//! End-to-end gateway tests: real axum servers on both sides — a fake
//! "VM upstream" echoing what it received, and the kotatsud router on
//! `MockControlPlane` with `endpoint_override`.

mod common;

use std::sync::Arc;

use axum::Router;
use axum::extract::{Request, WebSocketUpgrade};
use axum::response::Response;
use axum::routing::{any, get};
use common::{gateway_router, serve};
use futures_util::{SinkExt, StreamExt};
use kotatsu::mock::MockControlPlane;

/// Upstream "VM": echoes the auth/port headers and the path+body it saw.
fn upstream_app() -> Router {
    Router::new()
        .route(
            "/cookies",
            get(|| async {
                Response::builder()
                    .header("set-cookie", "a=1; Path=/")
                    .header("set-cookie", "b=2; Path=/api; Secure")
                    .header("set-cookie", "c=3; Domain=shared.example")
                    // Sibling-tenant escape attempt: must NOT survive.
                    .header("set-cookie", "d=4; Path=/t/u1evil")
                    // No Path at all — must gain the tenant prefix.
                    .header("set-cookie", "e=5")
                    .body(axum::body::Body::from("ok"))
                    .unwrap()
            }),
        )
        .route(
            "/ws",
            get(|ws: WebSocketUpgrade| async move {
                // The AWS contract requires the server to select a
                // subprotocol when the client offers them.
                ws.protocols(["lambda-microvms"])
                    .on_upgrade(|mut socket| async move {
                        while let Some(Ok(m)) = socket.recv().await {
                            if socket.send(m).await.is_err() {
                                return;
                            }
                        }
                    })
            }),
        )
        .route("/", get(|| async { "root" }))
        .route(
            "/{*p}",
            any(|req: Request| async move {
                let auth = req
                    .headers()
                    .get("x-aws-proxy-auth")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_owned();
                let port = req
                    .headers()
                    .get("x-aws-proxy-port")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_owned();
                let custom = req
                    .headers()
                    .get("x-custom")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_owned();
                let xff = req
                    .headers()
                    .get("x-forwarded-for")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_owned();
                let nominated = req
                    .headers()
                    .get("x-nominated")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_owned();
                let client_auth = req.headers().contains_key("authorization");
                let path_q = req
                    .uri()
                    .path_and_query()
                    .map(|pq| pq.to_string())
                    .unwrap_or_default();
                let body = axum::body::to_bytes(req.into_body(), usize::MAX)
                    .await
                    .unwrap();
                serde_json::json!({
                    "auth": auth, "port": port, "custom": custom,
                    "xff": xff, "nominated": nominated,
                    "client_auth": client_auth, "path": path_q,
                    "body": String::from_utf8_lossy(&body),
                })
                .to_string()
            }),
        )
}

fn gateway_app(upstream: &str) -> (Router, Arc<MockControlPlane>) {
    gateway_app_keys(upstream, [("k1".to_owned(), None)])
}

fn gateway_app_keys(
    upstream: &str,
    keys: impl IntoIterator<Item = (String, Option<std::collections::HashSet<String>>)>,
) -> (Router, Arc<MockControlPlane>) {
    let cp = Arc::new(MockControlPlane::new().endpoint_override(upstream));
    let router = gateway_router(cp.clone(), keys.into_iter().collect());
    (router, cp)
}

#[tokio::test]
async fn unauthenticated_requests_are_rejected() {
    let upstream = serve(upstream_app()).await;
    let (app, _cp) = gateway_app(&format!("http://{upstream}"));
    let gw = serve(app).await;
    let http = reqwest::Client::new();

    for req in [
        http.get(format!("http://{gw}/t/u1/x")),
        http.get(format!("http://{gw}/t/u1/x")).bearer_auth("wrong"),
    ] {
        let resp = req.send().await.unwrap();
        assert_eq!(resp.status(), 401);
        // RFC 6750 challenge.
        assert_eq!(
            resp.headers()["www-authenticate"].to_str().unwrap(),
            "Bearer"
        );
    }
}

/// Regression (C1): the `?key=` gateway credential must never reach the
/// VM, and `Connection`-nominated headers must be stripped (RFC 9110).
#[tokio::test]
async fn key_param_and_hop_by_hop_headers_do_not_leak() {
    let upstream = serve(upstream_app()).await;
    let (app, _cp) = gateway_app(&format!("http://{upstream}"));
    let gw = serve(app).await;
    let http = reqwest::Client::new();

    let resp = http
        .get(format!("http://{gw}/t/u1/x?key=k1&ok=1"))
        .bearer_auth("k1")
        .header("connection", "x-nominated")
        .header("x-nominated", "sneaky")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let path = body["path"].as_str().unwrap();
    assert_eq!(path, "/x?ok=1", "key must be stripped: {path}");
    assert_eq!(body["nominated"], "", "Connection-nominated header leaked");
    assert!(!body["xff"].as_str().unwrap().is_empty());
}

/// Regression (M6): multi-valued response headers must all reach the
/// client — Set-Cookie must not collapse to last-wins.
#[tokio::test]
async fn multi_value_response_headers_survive() {
    let upstream = serve(upstream_app()).await;
    let (app, _cp) = gateway_app(&format!("http://{upstream}"));
    let gw = serve(app).await;
    let http = reqwest::Client::new();

    let resp = http
        .get(format!("http://{gw}/t/u1/cookies"))
        .bearer_auth("k1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let cookies: Vec<_> = resp
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|v| v.to_str().unwrap().to_owned())
        .collect();
    assert_eq!(cookies.len(), 5, "set-cookie collapsed: {cookies:?}");
    for name in ["a=1", "b=2", "c=3", "d=4", "e=5"] {
        assert!(
            cookies.iter().any(|c| c.starts_with(name)),
            "{name} missing: {cookies:?}"
        );
    }
}

/// A VM's cookies must be scoped to its own tenant prefix — `Path=/`
/// on the shared gateway host would send them to every other tenant.
/// `Domain` is stripped so a VM cannot widen the scope.
#[tokio::test]
async fn set_cookie_is_clamped_to_tenant_path() {
    let upstream = serve(upstream_app()).await;
    let (app, _cp) = gateway_app(&format!("http://{upstream}"));
    let gw = serve(app).await;
    let http = reqwest::Client::new();

    let resp = http
        .get(format!("http://{gw}/t/u1/cookies"))
        .bearer_auth("k1")
        .send()
        .await
        .unwrap();
    let cookies: Vec<_> = resp
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|v| v.to_str().unwrap().to_owned())
        .collect();
    // Anchor per cookie — substring matches across entries must not
    // satisfy these assertions.
    let cookie = |name: &str| {
        cookies
            .iter()
            .find(|c| c.starts_with(name))
            .unwrap_or_else(|| panic!("{name} missing: {cookies:?}"))
    };
    assert!(cookie("a=1").contains("Path=/t/u1"), "a: {cookies:?}");
    assert!(cookie("b=2").contains("Path=/t/u1/api"), "b: {cookies:?}");
    assert!(cookie("b=2").contains("Secure"), "b lost attrs");
    // Domain never crosses.
    assert!(
        !cookies.iter().any(|c| c.to_lowercase().contains("domain=")),
        "Domain survived: {cookies:?}"
    );
    // `/t/u1evil` is a *different* tenant — prefix match must be
    // boundary-aware, so it lands back under u1's own tree.
    let d = cookie("d=4");
    assert!(
        d.contains("Path=/t/u1/") || d.contains("Path=/t/u1;") || d.ends_with("Path=/t/u1"),
        "sibling-tenant escape survived: {d}"
    );
    assert!(!d.contains("Path=/t/u1evil"), "escape kept verbatim: {d}");
    // No Path gains the tenant prefix.
    assert!(cookie("e=5").contains("Path=/t/u1"), "e: {cookies:?}");
}

/// A tenant-scoped key authenticates for its own tenant but is
/// forbidden (403) on every other — a leaked scoped key cannot pivot.
#[tokio::test]
async fn scoped_key_cannot_cross_tenants() {
    let upstream = serve(upstream_app()).await;
    let (app, _cp) = gateway_app_keys(
        &format!("http://{upstream}"),
        [
            ("admin".to_owned(), None),
            (
                "scoped-u1".to_owned(),
                Some(std::collections::HashSet::from(["u1".to_owned()])),
            ),
        ],
    );
    let gw = serve(app).await;
    let http = reqwest::Client::new();

    // Scoped key reaches its own tenant…
    let ok = http
        .get(format!("http://{gw}/t/u1/x"))
        .bearer_auth("scoped-u1")
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200);
    // …but not a sibling tenant, while a global key reaches both.
    let forbidden = http
        .get(format!("http://{gw}/t/u2/x"))
        .bearer_auth("scoped-u1")
        .send()
        .await
        .unwrap();
    assert_eq!(forbidden.status(), 403);
    let global = http
        .get(format!("http://{gw}/t/u2/x"))
        .bearer_auth("admin")
        .send()
        .await
        .unwrap();
    assert_eq!(global.status(), 200);
}

/// Regression (M4): encoded separators survive — `%2F` must not become
/// a real `/`, `%23` must not swallow the query into a fragment.
#[tokio::test]
async fn encoded_path_chars_are_preserved() {
    let upstream = serve(upstream_app()).await;
    let (app, _cp) = gateway_app(&format!("http://{upstream}"));
    let gw = serve(app).await;
    let http = reqwest::Client::new();

    let resp = http
        .get(format!("http://{gw}/t/u1/a%2Fb%23frag?q=v"))
        .bearer_auth("k1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let path = body["path"].as_str().unwrap();
    assert!(path.contains("q=v"), "query was lost to fragment: {path}");
    assert!(
        path.contains("a%2Fb") || path.contains("a/b"),
        "%2F mangled: {path}"
    );
}

/// Non-UTF-8 percent-encoded bytes in the query reach the VM verbatim —
/// the gateway reads the raw `uri.query()` instead of a decoded
/// `Query<HashMap>` extractor, which would 400 them upstream of auth.
#[tokio::test]
async fn non_utf8_query_is_forwarded_verbatim() {
    let upstream = serve(upstream_app()).await;
    let (app, _cp) = gateway_app(&format!("http://{upstream}"));
    let gw = serve(app).await;
    let http = reqwest::Client::new();

    let resp = http
        .get(format!("http://{gw}/t/u1/echo?raw=%FF%FE&ok=1"))
        .bearer_auth("k1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    let path = body["path"].as_str().unwrap();
    assert!(
        path.contains("raw=%FF%FE"),
        "non-UTF-8 query mangled: {path}"
    );
    assert!(path.contains("ok=1"), "sibling segment lost: {path}");
}

#[tokio::test]
async fn proxied_request_reaches_vm_with_contract_headers() {
    let upstream = serve(upstream_app()).await;
    let (app, _cp) = gateway_app(&format!("http://{upstream}"));
    let gw = serve(app).await;
    let http = reqwest::Client::new();

    let resp = http
        .post(format!("http://{gw}/t/u1/echo?x=1"))
        .bearer_auth("k1")
        .header("x-custom", "yes")
        .body("hello")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();

    // The VM received the contract headers minted by the pool.
    assert!(body["auth"].as_str().unwrap().starts_with("dev-token-"));
    assert_eq!(body["port"], "8080");
    assert_eq!(body["path"], "/echo?x=1");
    assert_eq!(body["body"], "hello");
    assert_eq!(body["custom"], "yes");
    // The client's own Authorization header was stripped upstream.
    assert_eq!(body["client_auth"], false);
}

/// `/t/{tenant}/` (trailing slash, empty wildcard) must reach the VM
/// root — axum's `{*rest}` doesn't match an empty remainder.
#[tokio::test]
async fn trailing_slash_proxies_to_root() {
    let upstream = serve(upstream_app()).await;
    let (app, _cp) = gateway_app(&format!("http://{upstream}"));
    let gw = serve(app).await;
    let http = reqwest::Client::new();

    let resp = http
        .get(format!("http://{gw}/t/u1/"))
        .bearer_auth("k1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    // Upstream's explicit `/` route — proves the empty wildcard proxied
    // to the VM root rather than hitting the gateway's 404.
    assert_eq!(resp.text().await.unwrap(), "root");
}

#[tokio::test]
async fn same_tenant_reuses_vm_and_cached_token() {
    let upstream = serve(upstream_app()).await;
    let (app, _cp) = gateway_app(&format!("http://{upstream}"));
    let gw = serve(app).await;
    let http = reqwest::Client::new();

    let auth_of = |resp: reqwest::Response| async move {
        let b: serde_json::Value = resp.json().await.unwrap();
        b["auth"].as_str().unwrap().to_owned()
    };
    let a1 = auth_of(
        http.get(format!("http://{gw}/t/shared/echo"))
            .bearer_auth("k1")
            .send()
            .await
            .unwrap(),
    )
    .await;
    let a2 = auth_of(
        http.get(format!("http://{gw}/t/shared/echo"))
            .bearer_auth("k1")
            .send()
            .await
            .unwrap(),
    )
    .await;
    // Same tenant → same VM → cached token reused.
    assert_eq!(a1, a2);
}

#[tokio::test]
async fn invalid_tenant_key_is_400() {
    let upstream = serve(upstream_app()).await;
    let (app, _cp) = gateway_app(&format!("http://{upstream}"));
    let gw = serve(app).await;
    let http = reqwest::Client::new();
    let resp = http
        .get(format!("http://{gw}/t/bad%20tenant/x"))
        .bearer_auth("k1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn metrics_and_healthz_work() {
    let upstream = serve(upstream_app()).await;
    let (app, _cp) = gateway_app(&format!("http://{upstream}"));
    let gw = serve(app).await;
    let http = reqwest::Client::new();

    let h = http
        .get(format!("http://{gw}/healthz"))
        .send()
        .await
        .unwrap();
    assert_eq!(h.status(), 200);

    let m = http
        .get(format!("http://{gw}/metrics"))
        .send()
        .await
        .unwrap();
    assert_eq!(m.status(), 200);
    assert_eq!(
        m.headers()["content-type"].to_str().unwrap(),
        "text/plain; version=0.0.4"
    );
}

/// WebSocket proxy: browser-style `?key=` auth (no header) plus an echo
/// upstream — proves the subprotocol handshake and frame relay work.
#[tokio::test]
async fn websocket_proxy_echoes_through() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("kotatsud=debug,kotatsu=debug")
        .try_init();
    let upstream = serve(upstream_app()).await;
    let (app, _cp) = gateway_app(&format!("http://{upstream}"));
    let gw = serve(app).await;

    let url = format!("ws://{gw}/t/u1/ws?key=k1");
    let (mut ws, resp) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(resp.status(), 101);

    ws.send(tokio_tungstenite::tungstenite::Message::Text("ping".into()))
        .await
        .unwrap();
    let msg = ws.next().await.unwrap().unwrap();
    assert!(
        matches!(msg, tokio_tungstenite::tungstenite::Message::Text(t) if t.as_str() == "ping")
    );

    // Unauthenticated WS must fail before upgrade.
    let err = tokio_tungstenite::connect_async(format!("ws://{gw}/t/u1/ws?key=bad"))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        tokio_tungstenite::tungstenite::Error::Http(_)
    ));
}
