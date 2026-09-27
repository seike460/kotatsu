//! Gateway router: `/healthz`, `/metrics`, and the tenant proxy
//! `/t/{tenant}/{*path}` (HTTP + WebSocket upgrade).
//!
//! Notes: `/metrics` is unauthenticated on whatever `--listen` binds —
//! bind it privately or front it with your own auth layer. Request
//! trailers are not proxied (gRPC trailer passthrough is not supported
//! yet). `key` is a reserved query parameter for browser-WS auth and is
//! stripped before forwarding upstream. WS upstream handshakes carry
//! only the contract headers — client `Cookie`/`Origin`/app headers do
//! not cross (unlike the HTTP path, which also sets `x-forwarded-*`).

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::extract::connect_info::ConnectInfo;
use axum::extract::ws::{CloseFrame, Message as AxumMsg, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use kotatsu::{Error, Sandbox, SandboxPool, TenantKey};
use metrics_exporter_prometheus::PrometheusHandle;
use tokio_tungstenite::tungstenite::Message as TungMsg;

/// Shared state for all handlers.
#[derive(Clone)]
pub struct AppState {
    pub pool: Arc<SandboxPool>,
    pub metrics: PrometheusHandle,
    /// key → allowed tenants (`None` = any tenant). A key scoped to a
    /// tenant can only reach `/t/{that-tenant}` — a stolen key cannot
    /// pivot across tenant boundaries.
    pub api_keys: HashMap<String, Option<HashSet<String>>>,
    pub allow_unauthenticated: bool,
    /// Value written to `x-forwarded-proto` (e.g. `https` behind a
    /// TLS-terminating load balancer).
    pub forwarded_proto: String,
}

/// Builds the gateway router. The proxy handlers extract
/// `ConnectInfo<SocketAddr>` — serve with
/// `into_make_service_with_connect_info::<SocketAddr>()` or every
/// `/t/*` request fails extraction.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/metrics", get(metrics_handler))
        .route("/t/{tenant}", any(proxy_tenant))
        // Trailing slash with an empty wildcard doesn't match
        // `{*rest}` — `upstream_path` already maps it to "/".
        .route("/t/{tenant}/", any(proxy_tenant))
        .route("/t/{tenant}/{*rest}", any(proxy_rest))
        .with_state(state)
}

async fn metrics_handler(State(s): State<AppState>) -> Response {
    Response::builder()
        .header(http::header::CONTENT_TYPE, "text/plain; version=0.0.4")
        .body(Body::from(s.metrics.render()))
        .expect("metrics response")
}

/// Hop-by-hop + credential + identity headers that must never cross the
/// gateway boundary in either direction. `Connection`-nominated headers
/// are stripped additionally per request (RFC 9110 §7.6.1).
const STRIPPED: &[&str] = &[
    // RFC 2616 §13.5.1 hop-by-hop + RFC 9110 additions.
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "http2-settings",
    "expect",
    // Framing: bodies are re-streamed, a stale length desyncs the peer.
    "content-length",
    "host",
    // Credentials: client auth never reaches the VM; contract headers
    // must never be client-injected.
    "authorization",
    "x-aws-proxy-auth",
    "x-aws-proxy-port",
    // Identity: spoofable — the gateway sets truthful values itself.
    "forwarded",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-proto",
    "x-forwarded-port",
    "x-real-ip",
    // Same-origin poisoning: tenant apps must not set these on the
    // shared gateway host. `set-cookie` stays — sandbox apps need it.
    "alt-svc",
    "strict-transport-security",
    // WS handshake fields have no meaning on the plain-HTTP path.
    "sec-websocket-accept",
    "sec-websocket-extensions",
    "sec-websocket-key",
    "sec-websocket-protocol",
    "sec-websocket-version",
];

/// Adds every header nominated by `Connection` to a deny-check —
/// `Connection: x-foo` makes `x-foo` hop-by-hop for this message.
fn connection_nominated(headers: &HeaderMap) -> HashSet<String> {
    headers
        .get_all(http::header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|t| t.trim().to_ascii_lowercase())
        .filter(|t| !t.is_empty())
        .collect()
}

fn stripped(name: &str, nominated: &HashSet<String>) -> bool {
    // HeaderName::as_str() is already lowercase for both call sites.
    STRIPPED.contains(&name) || nominated.contains(name)
}

fn is_upgrade(headers: &HeaderMap) -> bool {
    headers
        .get(http::header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
}

/// Authenticates a request and returns the key's tenant scope:
/// `Some(None)` for a global key, `Some(Some(tenants))` for a
/// tenant-scoped key, `None` when authentication failed.
fn authenticate(
    s: &AppState,
    headers: &HeaderMap,
    query_key: Option<&str>,
) -> Option<Option<HashSet<String>>> {
    if s.allow_unauthenticated {
        return Some(None);
    }
    // RFC 6750: the scheme name is case-insensitive.
    if let Some(scope) = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            let (scheme, token) = v.split_once(' ')?;
            scheme.eq_ignore_ascii_case("bearer").then_some(token)
        })
        .and_then(|key| s.api_keys.get(key))
    {
        return Some(scope.clone());
    }
    // Browser WebSocket clients cannot set headers — accept `?key=` on
    // upgrade requests only (RFC 6750 §2.3 style).
    if is_upgrade(headers)
        && let Some(scope) = query_key.and_then(|key| s.api_keys.get(key))
    {
        return Some(scope.clone());
    }
    None
}

fn unauthorized() -> Response {
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header(http::header::WWW_AUTHENTICATE, "Bearer")
        .body(Body::empty())
        .expect("401")
}

fn forbidden() -> Response {
    Response::builder()
        .status(StatusCode::FORBIDDEN)
        .body(Body::empty())
        .expect("403")
}

fn err_response(e: &Error) -> Response {
    let status = match e {
        Error::InvalidInput(_) => StatusCode::BAD_REQUEST,
        Error::NotFound { .. } => StatusCode::NOT_FOUND,
        Error::PoolExhausted(_) => StatusCode::SERVICE_UNAVAILABLE,
        Error::WaitTimeout { .. } => StatusCode::GATEWAY_TIMEOUT,
        _ => StatusCode::BAD_GATEWAY,
    };
    let body = serde_json::json!({"error": e.to_string()}).to_string();
    Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .expect("error response")
}

/// `/t/{tenant}` — proxy to the VM's root path.
/// The query is read manually from `parts.uri` so non-UTF-8
/// (percent-encoded or raw byte) queries are proxied verbatim instead
/// of being rejected by `Query`'s UTF-8 decoding.
async fn proxy_tenant(
    s: State<AppState>,
    addr: ConnectInfo<SocketAddr>,
    Path(tenant): Path<String>,
    req: Request,
) -> Response {
    proxy_inner(s.0, addr, tenant, req).await
}

/// `/t/{tenant}/{*rest}` — proxy to a path on the tenant's VM.
async fn proxy_rest(
    s: State<AppState>,
    addr: ConnectInfo<SocketAddr>,
    Path((tenant, _rest)): Path<(String, String)>,
    req: Request,
) -> Response {
    proxy_inner(s.0, addr, tenant, req).await
}

/// The raw (still percent-encoded) upstream path: everything after the
/// `/t/<tenant>` prefix of `uri.path()`. Using the raw request target —
/// not the decoded `{*rest}` param — preserves `%2F`/`%3F`/`%23`
/// semantics the upstream may rely on.
fn upstream_path(uri_path: &str) -> &str {
    let after_prefix = uri_path.strip_prefix("/t/").unwrap_or("");
    match after_prefix.find('/') {
        Some(i) => &after_prefix[i..],
        None => "/",
    }
}

/// The decoded value of the `key` credential parameter, if present.
/// Parses the raw query byte-for-byte, so a malformed or non-UTF-8
/// sibling segment cannot hide a valid `key` — or vice versa.
/// On duplicate `key` params the first wins (fail-closed: an attacker
/// cannot smuggle a valid key behind a bad one).
fn query_key(raw: Option<&str>) -> Option<String> {
    url::form_urlencoded::parse(raw?.as_bytes())
        .find(|(k, _)| k == "key")
        .map(|(_, v)| v.into_owned())
}

/// The client query minus the reserved `key` credential parameter.
/// Non-`key` segments are passed through byte-for-byte (raw query
/// signatures and non-UTF-8 values survive); only the decoded *name*
/// decides whether a segment is dropped.
fn upstream_query(raw: Option<&str>) -> Option<String> {
    let q = raw?;
    let filtered = q
        .split('&')
        .filter(|seg| {
            let name = seg.split('=').next().unwrap_or_default();
            !url::form_urlencoded::parse(name.as_bytes())
                .next()
                .is_some_and(|(k, _)| k == "key")
        })
        .collect::<Vec<_>>()
        .join("&");
    (!filtered.is_empty()).then_some(filtered)
}

/// Clamps an upstream `Set-Cookie` to this tenant's path prefix and
/// strips `Domain`, returning `None` when the result cannot be
/// expressed safely. All tenants share the gateway host, so a VM's
/// `Path=/` cookie would otherwise be sent to every other tenant's VM.
/// The value is decoded lossily — an obs-text/UTF-8 cookie value must
/// not skip clamping entirely.
fn clamp_cookie_to_tenant(value: &http::HeaderValue, tenant: &str) -> Option<http::HeaderValue> {
    let text = String::from_utf8_lossy(value.as_bytes());
    let prefix = format!("/t/{tenant}");
    let prefix_dir = format!("{prefix}/");
    let mut segments: Vec<String> = Vec::new();
    let mut saw_path = false;
    for (i, seg) in text.split(';').enumerate() {
        let seg = seg.trim();
        // Segment 0 is the cookie's own name=value — never an attribute.
        let name = if i > 0 {
            seg.split_once('=')
                .map(|(n, _)| n.trim())
                .unwrap_or(seg)
                .to_ascii_lowercase()
        } else {
            String::new()
        };
        match name.as_str() {
            // A VM must never scope cookies to the shared host/domain.
            "domain" => {}
            "path" => {
                saw_path = true;
                let p = seg.split_once('=').map(|(_, v)| v.trim()).unwrap_or("");
                // Boundary-aware: "/t/u1evil" must NOT match tenant "u1".
                if p == prefix || p.starts_with(&prefix_dir) {
                    segments.push(seg.to_owned());
                } else {
                    let sub = p.trim_start_matches('/');
                    let clamped = if sub.is_empty() {
                        prefix.clone()
                    } else {
                        format!("{prefix_dir}{sub}")
                    };
                    segments.push(format!("Path={clamped}"));
                }
            }
            _ => segments.push(seg.to_owned()),
        }
    }
    if !saw_path {
        segments.push(format!("Path={prefix}"));
    }
    // If the rebuilt value cannot be represented, drop the cookie —
    // never pass an unclamped one through.
    http::HeaderValue::from_str(&segments.join("; ")).ok()
}

/// Shared proxy flow: authenticate, resolve the tenant's VM (holding
/// through suspend→resume inside `acquire`), then proxy HTTP or WS.
/// `WebSocketUpgrade` is constructed manually so the same route serves
/// both transports — `Option<WebSocketUpgrade>` is not an extractor.
async fn proxy_inner(
    s: AppState,
    addr: ConnectInfo<SocketAddr>,
    tenant: String,
    req: Request,
) -> Response {
    let start = Instant::now();
    let (mut parts, body) = req.into_parts();
    let headers = parts.headers.clone();

    let Some(scope) = authenticate(&s, &headers, query_key(parts.uri.query()).as_deref()) else {
        kotatsu::metrics::record_http_request(401, start.elapsed());
        return unauthorized();
    };

    let tenant = match TenantKey::new(&tenant) {
        Ok(t) => t,
        Err(e) => {
            kotatsu::metrics::record_http_request(400, start.elapsed());
            return err_response(&e);
        }
    };
    // Tenant authorization: a scoped key may only reach its own tenants.
    if scope.is_some_and(|tenants| !tenants.contains(tenant.as_str())) {
        kotatsu::metrics::record_http_request(403, start.elapsed());
        return forbidden();
    }

    // Rebuild the upstream target from the *raw* request target: path
    // stays percent-encoded; `key` never crosses to the VM.
    let mut path = upstream_path(parts.uri.path()).to_owned();
    if let Some(q) = upstream_query(parts.uri.query()) {
        path.push('?');
        path.push_str(&q);
    }

    if is_upgrade(&headers) {
        use axum::extract::FromRequestParts;
        return match WebSocketUpgrade::from_request_parts(&mut parts, &s).await {
            Ok(ws) => ws_proxy(s, ws, tenant, path, start).await,
            Err(rejection) => {
                let resp = rejection.into_response();
                kotatsu::metrics::record_http_request(resp.status().as_u16(), start.elapsed());
                resp
            }
        };
    }
    let method = parts.method.clone();
    let req = Request::from_parts(parts, body);
    http_proxy(
        s,
        HttpProxyArgs {
            method,
            tenant,
            path,
            headers,
            req,
            addr,
            start,
        },
    )
    .await
}

// `Box<Response>` keeps the `Err` variant small (clippy::result_large_err).
async fn acquire_or_err(s: &AppState, tenant: &TenantKey) -> Result<Sandbox, Box<Response>> {
    s.pool
        .acquire(tenant)
        .await
        .map_err(|e| Box::new(err_response(&e)))
}

/// Everything `http_proxy` needs to forward one request.
struct HttpProxyArgs {
    method: Method,
    tenant: TenantKey,
    path: String,
    headers: HeaderMap,
    req: Request,
    addr: ConnectInfo<SocketAddr>,
    start: Instant,
}

async fn http_proxy(s: AppState, a: HttpProxyArgs) -> Response {
    let HttpProxyArgs {
        method,
        tenant,
        path,
        headers,
        req,
        addr,
        start,
    } = a;
    let sandbox = match acquire_or_err(&s, &tenant).await {
        Ok(sb) => sb,
        Err(resp) => {
            kotatsu::metrics::record_http_request(resp.status().as_u16(), start.elapsed());
            return *resp;
        }
    };

    let mut builder = match sandbox.endpoint().request(method, &path, None).await {
        Ok(b) => b,
        Err(e) => {
            kotatsu::metrics::record_http_request(502, start.elapsed());
            return err_response(&e);
        }
    };

    let nominated = connection_nominated(&headers);
    for (name, value) in headers.iter() {
        if !stripped(name.as_str(), &nominated) {
            builder = builder.header(name.as_str(), value.as_bytes());
        }
    }
    // Truthful client identity (client-injected values were stripped).
    builder = builder
        .header("x-forwarded-for", addr.0.ip().to_string())
        .header("x-forwarded-proto", &s.forwarded_proto);

    let stream = http_body_util::BodyStream::new(req.into_body())
        .try_filter_map(|frame| async move { Ok(frame.into_data().ok()) });
    builder = builder.body(reqwest::Body::wrap_stream(stream));

    let upstream = match builder.send().await {
        Ok(r) => r,
        Err(e) => {
            let resp = err_response(&Error::Http(e));
            kotatsu::metrics::record_http_request(502, start.elapsed());
            return resp;
        }
    };

    let status = upstream.status();
    kotatsu::metrics::record_http_request(status.as_u16(), start.elapsed());

    let mut resp = Response::builder().status(status);
    let resp_headers = resp.headers_mut().expect("response headers");
    let resp_nominated = connection_nominated(upstream.headers());
    for (name, value) in upstream.headers() {
        if name == http::header::SET_COOKIE {
            // append, not insert: multi-valued headers (Set-Cookie!)
            // must not collapse to last-wins. Unclamped cookies are
            // dropped rather than passed through.
            if let Some(v) = clamp_cookie_to_tenant(value, tenant.as_str()) {
                resp_headers.append(name.clone(), v);
            }
        } else if !stripped(name.as_str(), &resp_nominated) {
            resp_headers.append(name.clone(), value.clone());
        }
    }
    resp.body(Body::from_stream(upstream.bytes_stream()))
        .expect("proxy response")
}

async fn ws_proxy(
    s: AppState,
    ws: WebSocketUpgrade,
    tenant: TenantKey,
    path: String,
    start: Instant,
) -> Response {
    let sandbox = match acquire_or_err(&s, &tenant).await {
        Ok(sb) => sb,
        Err(resp) => {
            kotatsu::metrics::record_http_request(resp.status().as_u16(), start.elapsed());
            return *resp;
        }
    };

    let upstream_req = match sandbox.endpoint().websocket(&path, None).await {
        Ok(w) => w,
        Err(e) => {
            kotatsu::metrics::record_http_request(502, start.elapsed());
            return err_response(&e);
        }
    };

    // Connect upstream BEFORE answering 101 — a dead VM endpoint yields
    // a real 502 instead of an instantly-dead WebSocket.
    let upstream_stream =
        match tokio::time::timeout(Duration::from_secs(10), upstream_req.connect()).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => {
                kotatsu::metrics::record_http_request(502, start.elapsed());
                return err_response(&e);
            }
            Err(_) => {
                kotatsu::metrics::record_http_request(504, start.elapsed());
                return Response::builder()
                    .status(StatusCode::GATEWAY_TIMEOUT)
                    .header(http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({"error": "upstream ws connect timed out"}).to_string(),
                    ))
                    .expect("504");
            }
        };

    kotatsu::metrics::record_http_request(101, start.elapsed());
    ws.on_upgrade(move |socket| async move {
        let _session = SessionGauge::new();
        if let Err(e) = pipe_ws(socket, upstream_stream).await {
            tracing::debug!(error = %e, "ws pipe ended");
        }
    })
}

/// ws_session gauge: the decrement runs even if the pipe task aborts.
struct SessionGauge;
impl SessionGauge {
    fn new() -> Self {
        kotatsu::metrics::ws_session(true);
        Self
    }
}
impl Drop for SessionGauge {
    fn drop(&mut self) {
        kotatsu::metrics::ws_session(false);
    }
}

// NOTE: mirrors `pipe_ws`/`to_tungstenite`/`to_axum` in
// `kotatsu-dev/src/proxy.rs`. Intentionally duplicated rather than
// shared: the gateway side enforces auth/tenant headers while the dev
// side trusts its peer, and extracting them into `kotatsu` would pull
// axum into the core crate. Keep behavior in sync when editing.

async fn pipe_ws(
    socket: WebSocket,
    upstream_stream: kotatsu::WsStream,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (mut vm_tx, mut vm_rx) = upstream_stream.split();
    let (mut cli_tx, mut cli_rx) = socket.split();

    let client_to_vm = async {
        while let Some(msg) = cli_rx.next().await {
            match msg {
                // Keepalive terminates at each hop: both tungstenite
                // endpoints already auto-answer pings, so forwarding
                // them doubles the pong traffic.
                Ok(AxumMsg::Ping(_) | AxumMsg::Pong(_)) => continue,
                Ok(m) => {
                    if vm_tx.send(to_tungstenite(m)).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        let _ = vm_tx.close().await;
    };
    let vm_to_client = async {
        while let Some(msg) = vm_rx.next().await {
            match msg {
                Ok(TungMsg::Ping(_) | TungMsg::Pong(_) | TungMsg::Frame(_)) => continue,
                Ok(m) => {
                    if cli_tx.send(to_axum(m)).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        let _ = cli_tx.close().await;
    };
    tokio::select! {
        _ = client_to_vm => {}
        _ = vm_to_client => {}
    }
    Ok(())
}

fn to_tungstenite(m: AxumMsg) -> TungMsg {
    match m {
        AxumMsg::Text(t) => TungMsg::Text(t.as_str().into()),
        AxumMsg::Binary(b) => TungMsg::Binary(b),
        AxumMsg::Ping(p) => TungMsg::Ping(p),
        AxumMsg::Pong(p) => TungMsg::Pong(p),
        AxumMsg::Close(c) => {
            TungMsg::Close(
                c.map(|f| tokio_tungstenite::tungstenite::protocol::CloseFrame {
                    code: f.code.into(),
                    reason: f.reason.as_str().into(),
                }),
            )
        }
    }
}

fn to_axum(m: TungMsg) -> AxumMsg {
    match m {
        TungMsg::Text(t) => AxumMsg::Text(t.as_str().into()),
        TungMsg::Binary(b) => AxumMsg::Binary(b),
        TungMsg::Ping(p) => AxumMsg::Ping(p),
        TungMsg::Pong(p) => AxumMsg::Pong(p),
        TungMsg::Close(c) => AxumMsg::Close(c.map(|f| CloseFrame {
            code: f.code.into(),
            reason: f.reason.as_str().into(),
        })),
        // Raw frames are filtered out by the read loop, so this arm is
        // unreachable — kept only to satisfy the exhaustive match.
        TungMsg::Frame(_) => AxumMsg::Binary(bytes::Bytes::new()),
    }
}
