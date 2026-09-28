//! Authenticated HTTP and WebSocket client for a single MicroVM endpoint.
//!
//! Each MicroVM gets a dedicated HTTPS endpoint; ingress requires an
//! `X-aws-proxy-auth` JWE plus an `X-aws-proxy-port` selector, and
//! WebSockets carry the same credentials as subprotocols (because
//! browsers cannot set headers). `MicrovmEndpoint` binds those contract
//! details to one verified-`RUNNING` MicroVM.

use std::sync::Arc;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use url::Url;

use crate::error::{Error, Result};
use crate::token::TokenVending;
use crate::types::{
    DEFAULT_APP_PORT, Microvm, MicrovmId, PortSpec, RunningVm, WS_AUTH_PROTOCOL_PREFIX,
    WS_BASE_PROTOCOL, WS_PORT_PROTOCOL_PREFIX,
};

/// Convenience alias for the stream returned by [`WsRequest::connect`].
pub type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Client bound to one MicroVM's dedicated endpoint.
///
/// Constructible only from a [`RunningVm`]: the type system guarantees
/// callers handed the endpoint a VM that was `RUNNING` when verified.
/// Subsequent liveness drift (auto-suspend, reapers) is surfaced as
/// request failures — see `SandboxPool` for the lifecycle-aware layer.
#[derive(Clone)]
pub struct MicrovmEndpoint {
    vm: Microvm,
    base: Url,
    /// `None` uses the process-wide default client (see
    /// [`default_client`]); `with_client` installs a custom one.
    http: Option<reqwest::Client>,
    tokens: Arc<TokenVending>,
    scope: Vec<PortSpec>,
    default_port: u16,
}

impl MicrovmEndpoint {
    /// Binds an endpoint client to a running MicroVM.
    ///
    /// Defaults: application port [`DEFAULT_APP_PORT`], token scope of
    /// exactly that port (least privilege — widen via [`Self::with_scope`]).
    pub fn new(vm: &RunningVm, tokens: Arc<TokenVending>) -> Result<Self> {
        Self::build(vm, tokens, false)
    }

    /// Like [`Self::new`] but also accepts `http://` endpoints — for the
    /// `kotatsu-dev` emulator and tests pointing at local upstreams.
    /// Production callers must use [`Self::new`].
    pub fn new_insecure(vm: &RunningVm, tokens: Arc<TokenVending>) -> Result<Self> {
        Self::build(vm, tokens, true)
    }

    fn build(vm: &RunningVm, tokens: Arc<TokenVending>, allow_http: bool) -> Result<Self> {
        let base: Url = vm
            .endpoint()
            .parse()
            .map_err(|e| Error::invalid(format!("bad endpoint url {:?}: {e}", vm.endpoint())))?;
        let secure = base.scheme() == "https" || (allow_http && base.scheme() == "http");
        if !secure {
            return Err(Error::invalid(format!(
                "endpoint url {:?} is not https",
                vm.endpoint()
            )));
        }
        Ok(Self {
            vm: vm.microvm().clone(),
            base,
            http: None,
            tokens,
            scope: vec![PortSpec::port(DEFAULT_APP_PORT)?],
            default_port: DEFAULT_APP_PORT,
        })
    }

    /// Uses a preconfigured `reqwest::Client` (timeouts, TLS roots…).
    ///
    /// Build it with `redirect(reqwest::redirect::Policy::none())`: a
    /// client that follows redirects resends `X-aws-proxy-auth` to any
    /// URL the VM answers with.
    #[must_use]
    pub fn with_client(mut self, http: reqwest::Client) -> Self {
        self.http = Some(http);
        self
    }

    fn client(&self) -> &reqwest::Client {
        self.http.as_ref().unwrap_or_else(|| default_client())
    }

    /// Changes the default application port and narrows the token scope
    /// to that port.
    pub fn with_port(mut self, port: u16) -> Result<Self> {
        self.scope = vec![PortSpec::port(port)?];
        self.default_port = port;
        Ok(self)
    }

    /// Widens the token scope (multiple ports / ranges). Every port a
    /// request may target must be covered, and the scope must be valid
    /// and non-empty.
    pub fn with_scope(mut self, scope: Vec<PortSpec>) -> Result<Self> {
        if scope.is_empty() {
            return Err(Error::invalid("token scope must not be empty"));
        }
        if let Some(bad) = scope.iter().find(|s| !s.is_valid()) {
            return Err(Error::invalid(format!("invalid port scope: {bad}")));
        }
        self.scope = scope;
        Ok(self)
    }

    /// The bound MicroVM description.
    pub fn vm(&self) -> &Microvm {
        &self.vm
    }

    /// The bound MicroVM identifier.
    pub fn microvm_id(&self) -> &MicrovmId {
        &self.vm.id
    }

    /// The endpoint base URL.
    pub fn url(&self) -> &Url {
        &self.base
    }

    /// Builds an authenticated request for `path` on `port`
    /// (default: the configured application port).
    ///
    /// `path` must be an origin-form path (`/...`); anything else is
    /// rejected before resolution so a network-path reference
    /// (`//host/x`) or absolute URI can never redirect the request — and
    /// the stamped `X-aws-proxy-auth` token — off this MicroVM's origin.
    ///
    /// Fetches a scoped token from the vending cache, then stamps the
    /// two contract headers. Returns the `RequestBuilder` so callers can
    /// attach bodies, headers and query parameters before `.send()`.
    pub async fn request(
        &self,
        method: http::Method,
        path: &str,
        port: Option<u16>,
    ) -> Result<reqwest::RequestBuilder> {
        let port = port.unwrap_or(self.default_port);
        let url = self.resolve_path(path)?;
        let token = self.token_for(port).await?;
        Ok(self
            .client()
            .request(method, url)
            .header(crate::AUTH_HEADER, token.header_value())
            .header(crate::PORT_HEADER, port.to_string()))
    }

    /// Authenticated GET.
    pub async fn get(&self, path: &str) -> Result<reqwest::RequestBuilder> {
        self.request(http::Method::GET, path, None).await
    }

    /// Authenticated POST.
    pub async fn post(&self, path: &str) -> Result<reqwest::RequestBuilder> {
        self.request(http::Method::POST, path, None).await
    }

    /// Authenticated PUT.
    pub async fn put(&self, path: &str) -> Result<reqwest::RequestBuilder> {
        self.request(http::Method::PUT, path, None).await
    }

    /// Authenticated DELETE.
    pub async fn delete(&self, path: &str) -> Result<reqwest::RequestBuilder> {
        self.request(http::Method::DELETE, path, None).await
    }

    /// Prepares a WebSocket handshake for `path` on `port`.
    ///
    /// The token travels in the `Sec-WebSocket-Protocol` header as three
    /// subprotocols — `lambda-microvms`,
    /// `lambda-microvms.authentication.<token>`,
    /// `lambda-microvms.port.<port>` — matching the documented contract.
    pub async fn websocket(&self, path: &str, port: Option<u16>) -> Result<WsRequest> {
        let port = port.unwrap_or(self.default_port);
        let mut url = self.resolve_path(path)?;
        let token = self.token_for(port).await?;
        let ws_scheme = if self.base.scheme() == "http" {
            "ws"
        } else {
            "wss"
        };
        url.set_scheme(ws_scheme)
            .map_err(|()| Error::invalid("endpoint url cannot become a ws scheme"))?;
        Ok(WsRequest {
            url,
            protocols: vec![
                WS_BASE_PROTOCOL.to_owned(),
                format!("{WS_AUTH_PROTOCOL_PREFIX}{}", token.header_value()),
                format!("{WS_PORT_PROTOCOL_PREFIX}{port}"),
            ],
        })
    }

    /// Resolves an origin-form `path` against the endpoint base.
    ///
    /// `Url::join` performs full RFC 3986 reference resolution, so a
    /// caller-supplied `//other.host/x` or `https://other/x` would flip
    /// the origin and carry the auth token elsewhere. Reject anything
    /// that is not a plain `/`-prefixed path, then assert post-join that
    /// the origin and scheme are unchanged as defense in depth.
    fn resolve_path(&self, path: &str) -> Result<Url> {
        if !path.starts_with('/') || path.starts_with("//") || path.contains('\\') {
            return Err(Error::invalid(format!(
                "path {path:?} must be an origin-form path (/...)"
            )));
        }
        let url = self
            .base
            .join(path)
            .map_err(|e| Error::invalid(format!("bad path {path:?}: {e}")))?;
        if url.origin() != self.base.origin() || url.scheme() != self.base.scheme() {
            return Err(Error::invalid(format!(
                "path {path:?} escapes the endpoint origin"
            )));
        }
        Ok(url)
    }

    /// A cached token that covers `port`, or an error if the configured
    /// scope does not include it.
    ///
    /// The scope check runs before minting so out-of-scope requests fail
    /// locally without burning a `create-microvm-auth-token` call.
    async fn token_for(&self, port: u16) -> Result<crate::types::AuthToken> {
        if port == 0 {
            return Err(Error::invalid("port 0 is not valid"));
        }
        if !self.scope.iter().any(|s| s.covers(port)) {
            return Err(Error::invalid(format!(
                "token scope {} does not cover port {port}",
                self.scope_display()
            )));
        }
        let token = self.tokens.token(self.microvm_id(), &self.scope).await?;
        debug_assert!(token.covers_port(port));
        Ok(token)
    }

    fn scope_display(&self) -> String {
        self.scope
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// A prepared WebSocket handshake.
///
/// The token value is embedded in [`WsRequest::subprotocols`], which is
/// exactly what browsers would send — but it is still a credential, so
/// `Debug` redacts the URL (which contains no secret anyway, since the
/// token lives only in the subprotocol list).
pub struct WsRequest {
    url: Url,
    protocols: Vec<String>,
}

impl WsRequest {
    /// The `wss://` URL to connect to.
    pub fn url(&self) -> &Url {
        &self.url
    }

    /// The three contract subprotocols, in order.
    pub fn subprotocols(&self) -> &[String] {
        &self.protocols
    }

    /// Performs the handshake via `tokio-tungstenite`.
    pub async fn connect(&self) -> Result<WsStream> {
        let mut req = self.url.as_str().into_client_request()?;
        req.headers_mut().insert(
            http::header::SEC_WEBSOCKET_PROTOCOL,
            http::HeaderValue::from_str(&self.protocols.join(", "))
                .map_err(|e| Error::invalid(format!("bad subprotocol header: {e}")))?,
        );
        let (stream, _resp) = tokio_tungstenite::connect_async_tls_with_config(
            req,
            None,
            false,
            Some(ws_tls_connector()?),
        )
        .await?;
        Ok(stream)
    }
}

impl std::fmt::Debug for WsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsRequest")
            .field("url", &self.url.as_str())
            .field("protocols", &"<redacted; see subprotocols()>")
            .finish()
    }
}

/// TLS connector for `wss://` handshakes.
///
/// This crate's dependency graph enables both of rustls' built-in
/// providers (`aws-lc-rs` through the AWS SDK, `ring` through reqwest),
/// so rustls cannot choose a process default and a handshake that
/// leaves the choice to it panics. Uses the process default when the
/// application installed one, else `ring`, with the webpki roots.
#[doc(hidden)]
pub fn ws_tls_connector() -> Result<tokio_tungstenite::Connector> {
    let provider = rustls::crypto::CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::ring::default_provider()));
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::Ws(Box::new(e)))?
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(tokio_tungstenite::Connector::Rustls(Arc::new(config)))
}

/// Process-wide default HTTP client, built once on first use.
///
/// Connect timeout only: no total timeout, because callers may
/// legitimately hold long-lived responses (SSE, streaming). Endpoints
/// that need different behavior install their own via `with_client`.
/// Redirects are returned to the caller, never followed: following one
/// would resend `X-aws-proxy-auth` to wherever the VM points.
fn default_client() -> &'static reqwest::Client {
    static CLIENT: std::sync::LazyLock<reqwest::Client> = std::sync::LazyLock::new(|| {
        reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("reqwest::Client with a connect timeout and no redirects cannot fail")
    });
    &CLIENT
}
