//! The contract proxy: enforces `X-aws-proxy-auth`/`X-aws-proxy-port`
//! and the `lambda-microvms` subprotocols, then relays to the app.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::ws::{Message as AxumMsg, WebSocket, WebSocketUpgrade};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt, TryStreamExt};
use tokio_tungstenite::tungstenite::Message as TungMsg;

use crate::emulator::{DevState, Shared};

use kotatsu::{
    AUTH_HEADER, PORT_HEADER, WS_AUTH_PROTOCOL_PREFIX as WS_AUTH_PREFIX,
    WS_BASE_PROTOCOL as WS_BASE, WS_PORT_PROTOCOL_PREFIX as WS_PORT_PREFIX,
};

/// Hop-by-hop + contract headers never forwarded to the app.
const STRIPPED: &[&str] = &[
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
    "content-length",
    "host",
    // HeaderMap names are lowercase internally — strip literals must
    // be lowercase (AUTH_HEADER/PORT_HEADER are canonical-cased for
    // wire use, not for map lookup).
    "x-aws-proxy-auth",
    "x-aws-proxy-port",
    // The client's WS handshake is answered by the emulator; the app
    // gets a fresh one without contract fields.
    "sec-websocket-accept",
    "sec-websocket-extensions",
    "sec-websocket-key",
    "sec-websocket-protocol",
    "sec-websocket-version",
];

fn err(status: StatusCode, msg: &str) -> Response {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::json!({"error": msg}).to_string()))
        .expect("err")
}

/// Headers nominated by `Connection` are hop-by-hop (RFC 9110 §7.6.1).
fn connection_nominated(headers: &HeaderMap) -> std::collections::HashSet<String> {
    headers
        .get_all(http::header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|t| t.trim().to_ascii_lowercase())
        .filter(|t| !t.is_empty())
        .collect()
}

fn stripped(name: &str, nominated: &std::collections::HashSet<String>) -> bool {
    STRIPPED.contains(&name) || nominated.contains(name)
}

fn token_ok(s: &Shared, token: &str) -> bool {
    (s.cfg.accept_mock_tokens && token.starts_with("dev-token-"))
        || s.cfg.accepted_tokens.iter().any(|t| t == token)
}

/// Extracts (token, port) from headers — the plain-HTTP contract.
fn creds_from_headers(h: &HeaderMap) -> Result<(String, u16), Box<Response>> {
    let token = h
        .get(AUTH_HEADER)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Box::new(err(StatusCode::UNAUTHORIZED, "missing x-aws-proxy-auth")))?;
    let port: u16 = h
        .get(PORT_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| {
            Box::new(err(
                StatusCode::BAD_REQUEST,
                "missing/invalid x-aws-proxy-port",
            ))
        })?;
    Ok((token.to_owned(), port))
}

/// Extracts (token, port) from the WS subprotocol offer — the browser
/// contract: `lambda-microvms`, `lambda-microvms.authentication.<t>`,
/// `lambda-microvms.port.<p>`.
fn creds_from_protocols(h: &HeaderMap) -> Result<(String, u16), Box<Response>> {
    let offered: Vec<&str> = h
        .get_all(http::header::SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .collect();
    if !offered.contains(&WS_BASE) {
        return Err(Box::new(err(
            StatusCode::BAD_REQUEST,
            "missing lambda-microvms subprotocol",
        )));
    }
    let token = offered
        .iter()
        .find_map(|p| p.strip_prefix(WS_AUTH_PREFIX))
        .ok_or_else(|| {
            Box::new(err(
                StatusCode::UNAUTHORIZED,
                "missing lambda-microvms.authentication subprotocol",
            ))
        })?;
    let port: u16 = offered
        .iter()
        .find_map(|p| p.strip_prefix(WS_PORT_PREFIX))
        .and_then(|p| p.parse().ok())
        .ok_or_else(|| {
            Box::new(err(
                StatusCode::BAD_REQUEST,
                "missing/invalid lambda-microvms.port subprotocol",
            ))
        })?;
    Ok((token.to_owned(), port))
}

/// Gates on lifecycle state; auto-resumes on traffic when enabled.
async fn gate(s: &Shared) -> Result<(), Box<Response>> {
    match s.dev_state() {
        DevState::Running => Ok(()),
        DevState::Pending => Err(Box::new(err(
            StatusCode::SERVICE_UNAVAILABLE,
            "microvm is booting",
        ))),
        // AWS answers a request whose auto-resume fails with 502.
        DevState::Suspended if s.cfg.auto_resume => s.resume().await.map_err(|e| {
            Box::new(err(
                StatusCode::BAD_GATEWAY,
                &format!("auto-resume failed: {e}"),
            ))
        }),
        DevState::Suspended => Err(Box::new(err(
            StatusCode::SERVICE_UNAVAILABLE,
            "microvm suspended",
        ))),
        DevState::Terminated => Err(Box::new(err(StatusCode::GONE, "microvm terminated"))),
        DevState::Failed(e) => Err(Box::new(err(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("microvm failed: {e}"),
        ))),
    }
}

/// The catch-all handler: every non-`/_kotatsu/` request is a VM-traffic
/// request that must satisfy the contract.
pub(crate) async fn contract_proxy(State(s): State<Arc<Shared>>, req: Request) -> Response {
    let (mut parts, body) = req.into_parts();
    let is_ws = parts
        .headers
        .get(http::header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"));

    let (token, port) = match if is_ws {
        creds_from_protocols(&parts.headers)
    } else {
        creds_from_headers(&parts.headers)
    } {
        Ok(c) => c,
        Err(resp) => return *resp,
    };
    if !token_ok(&s, &token) {
        return err(StatusCode::FORBIDDEN, "bad x-aws-proxy-auth token");
    }
    if port != s.cfg.app_port {
        return err(
            StatusCode::BAD_REQUEST,
            &format!("port {port} not exposed by this microvm"),
        );
    }
    // Origin-form target only — `//host/x` must not become a network-
    // path reference off the app's origin (same rule as the client's
    // `MicrovmEndpoint::resolve_path`), and `OPTIONS *` has no target.
    // Validated BEFORE gate(): a malformed request must not wake a
    // suspended VM.
    let path_q = match parts.uri.path_and_query().map(|pq| pq.as_str()) {
        Some(pq) if pq.starts_with('/') && !pq.starts_with("//") => pq.to_owned(),
        _ => return err(StatusCode::BAD_REQUEST, "bad request target"),
    };
    if let Err(resp) = gate(&s).await {
        return *resp;
    }

    if is_ws {
        use axum::extract::FromRequestParts;
        return match WebSocketUpgrade::from_request_parts(&mut parts, &s).await {
            Ok(ws) => ws_proxy(s, ws, path_q).await,
            Err(rejection) => rejection.into_response(),
        };
    }
    http_proxy(s, parts, body, path_q).await
}

async fn http_proxy(
    s: Arc<Shared>,
    parts: http::request::Parts,
    body: Body,
    path_q: String,
) -> Response {
    let url = match s.app.join(&path_q) {
        Ok(u) if u.origin() == s.app.origin() => u,
        _ => return err(StatusCode::BAD_REQUEST, "path escapes the app origin"),
    };

    let mut builder = s.http.request(parts.method, url);
    let nominated = connection_nominated(&parts.headers);
    for (name, value) in parts.headers.iter() {
        if !stripped(name.as_str(), &nominated) {
            builder = builder.header(name.as_str(), value.as_bytes());
        }
    }
    let stream = http_body_util::BodyStream::new(body)
        .try_filter_map(|frame| async move { Ok(frame.into_data().ok()) });
    builder = builder.body(reqwest::Body::wrap_stream(stream));

    let upstream = match builder.send().await {
        Ok(r) => r,
        Err(e) => return err(StatusCode::BAD_GATEWAY, &format!("app connect failed: {e}")),
    };
    let mut resp = Response::builder().status(upstream.status());
    let out = resp.headers_mut().expect("headers");
    let resp_nominated = connection_nominated(upstream.headers());
    for (name, value) in upstream.headers() {
        if !stripped(name.as_str(), &resp_nominated) {
            out.append(name.clone(), value.clone());
        }
    }
    resp.body(Body::from_stream(upstream.bytes_stream()))
        .expect("resp")
}

/// Connects to the app BEFORE answering 101 — a dead app yields a real
/// 502 instead of an instantly-dead WebSocket (same rule as kotatsud).
async fn ws_proxy(s: Arc<Shared>, ws: WebSocketUpgrade, path_q: String) -> Response {
    let mut app_url = match s.app.join(&path_q) {
        Ok(u) if u.origin() == s.app.origin() => u,
        _ => return err(StatusCode::BAD_REQUEST, "path escapes the app origin"),
    };
    let scheme = if app_url.scheme() == "http" {
        "ws"
    } else {
        "wss"
    };
    if app_url.set_scheme(scheme).is_err() {
        return err(StatusCode::BAD_REQUEST, "app url cannot become ws");
    }
    let tls = match kotatsu::ws_tls_connector() {
        Ok(c) => c,
        Err(e) => {
            return err(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("app ws tls: {e}"),
            );
        }
    };
    let app_stream = match tokio::time::timeout(
        std::time::Duration::from_secs(10),
        tokio_tungstenite::connect_async_tls_with_config(app_url.as_str(), None, false, Some(tls)),
    )
    .await
    {
        Ok(Ok((stream, _))) => stream,
        Ok(Err(e)) => return err(StatusCode::BAD_GATEWAY, &format!("app ws connect: {e}")),
        Err(_) => return err(StatusCode::GATEWAY_TIMEOUT, "app ws connect timed out"),
    };
    ws.protocols([WS_BASE])
        .on_upgrade(move |socket| pipe_ws(socket, app_stream))
}

// NOTE: mirrors `pipe_ws`/`to_tungstenite`/`to_axum` in
// `kotatsud/src/gateway.rs`. Intentionally duplicated rather than
// shared: the dev proxy trusts its peer while the gateway enforces
// auth/tenant headers, and extracting them into `kotatsu` would pull
// axum into the core crate. Keep behavior in sync when editing.

async fn pipe_ws(socket: WebSocket, app_stream: kotatsu::WsStream) {
    let (mut app_tx, mut app_rx) = app_stream.split();
    let (mut cli_tx, mut cli_rx) = socket.split();

    let to_app = async {
        while let Some(msg) = cli_rx.next().await {
            match msg {
                Ok(AxumMsg::Ping(_) | AxumMsg::Pong(_)) => continue,
                Ok(m) => {
                    if app_tx.send(to_tungstenite(m)).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        let _ = app_tx.close().await;
    };
    let to_client = async {
        while let Some(msg) = app_rx.next().await {
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
        _ = to_app => {}
        _ = to_client => {}
    }
}

fn to_tungstenite(m: AxumMsg) -> TungMsg {
    match m {
        AxumMsg::Text(t) => TungMsg::Text(t.as_str().into()),
        AxumMsg::Binary(b) => TungMsg::Binary(b.to_vec().into()),
        AxumMsg::Close(c) => {
            TungMsg::Close(
                c.map(|f| tokio_tungstenite::tungstenite::protocol::CloseFrame {
                    code: f.code.into(),
                    reason: f.reason.as_str().into(),
                }),
            )
        }
        AxumMsg::Ping(p) => TungMsg::Ping(p.to_vec().into()),
        AxumMsg::Pong(p) => TungMsg::Pong(p.to_vec().into()),
    }
}

fn to_axum(m: TungMsg) -> AxumMsg {
    match m {
        TungMsg::Text(t) => AxumMsg::Text(t.as_str().into()),
        TungMsg::Binary(b) => AxumMsg::Binary(b.to_vec().into()),
        TungMsg::Close(c) => AxumMsg::Close(c.map(|f| axum::extract::ws::CloseFrame {
            code: f.code.into(),
            reason: f.reason.as_str().into(),
        })),
        TungMsg::Ping(p) => AxumMsg::Ping(p.to_vec().into()),
        TungMsg::Pong(p) => AxumMsg::Pong(p.to_vec().into()),
        TungMsg::Frame(_) => AxumMsg::Binary(Vec::new().into()),
    }
}
