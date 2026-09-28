//! End-to-end gateway tests: real axum servers on both sides — a fake
//! "VM upstream" echoing what it received, and the kotatsud router on
//! `MockControlPlane` with `endpoint_override`.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, WebSocketUpgrade};
use axum::response::Response;
use axum::routing::{any, get, post};
use common::{gateway_router, gateway_state, pool_config, serve};
use futures_util::{SinkExt, StreamExt};
use kotatsu::mock::{MockBehavior, MockControlPlane};

/// The header's value as text, or `""` when absent.
fn header(req: &Request, name: &str) -> String {
    req.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned()
}

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
            "/origin-wide",
            get(|| async {
                Response::builder()
                    .header("strict-transport-security", "max-age=31536000")
                    .header("alt-svc", "h3=\":443\"")
                    .header("service-worker-allowed", "/")
                    .header("clear-site-data", "\"cookies\", \"storage\"")
                    .header("nel", r#"{"report_to":"t","max_age":86400,"success_fraction":1.0}"#)
                    .header(
                        "report-to",
                        r#"{"group":"t","max_age":86400,"endpoints":[{"url":"https://collector.example/r"}]}"#,
                    )
                    .header("x-app", "kept")
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
                let mut echoed = serde_json::json!({
                    "auth": header(&req, "x-aws-proxy-auth"),
                    "port": header(&req, "x-aws-proxy-port"),
                    "custom": header(&req, "x-custom"),
                    "xff": header(&req, "x-forwarded-for"),
                    "nominated": header(&req, "x-nominated"),
                    "content_length": header(&req, "content-length"),
                    "transfer_encoding": header(&req, "transfer-encoding"),
                    "client_auth": req.headers().contains_key("authorization"),
                    "path": req
                        .uri()
                        .path_and_query()
                        .map(|pq| pq.to_string())
                        .unwrap_or_default(),
                });
                let body = axum::body::to_bytes(req.into_body(), usize::MAX)
                    .await
                    .unwrap();
                echoed["body"] = String::from_utf8_lossy(&body).into();
                echoed.to_string()
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

/// Upstream redirects go back to the client untouched. Following them
/// inside the gateway would let a VM make kotatsud fetch internal URLs
/// (SSRF) with X-aws-proxy-auth attached, and would swallow the
/// redirect's own Set-Cookie.
#[tokio::test]
async fn upstream_redirects_are_passed_through_not_followed() {
    let leak_hits = Arc::new(AtomicUsize::new(0));
    let hits = leak_hits.clone();
    let internal = serve(Router::new().fallback(move || {
        hits.fetch_add(1, Ordering::SeqCst);
        async { "internal" }
    }))
    .await;
    let target = format!("http://{internal}/latest/meta-data/");
    let (see_other, found) = (target.clone(), target.clone());
    let upstream = serve(
        Router::new()
            .route(
                "/see-other",
                get(move || async move {
                    Response::builder()
                        .status(303)
                        .header("location", see_other)
                        .body(axum::body::Body::empty())
                        .unwrap()
                }),
            )
            .route(
                "/login",
                post(move || async move {
                    Response::builder()
                        .status(302)
                        .header("location", found)
                        .header("set-cookie", "session=1; Path=/")
                        .body(axum::body::Body::empty())
                        .unwrap()
                }),
            ),
    )
    .await;
    let (app, _cp) = gateway_app(&format!("http://{upstream}"));
    let gw = serve(app).await;
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let resp = http
        .get(format!("http://{gw}/t/u1/see-other"))
        .bearer_auth("k1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 303);
    assert_eq!(resp.headers()["location"], target.as_str());

    let resp = http
        .post(format!("http://{gw}/t/u1/login"))
        .bearer_auth("k1")
        .body("user=a")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 302);
    assert_eq!(resp.headers()["location"], target.as_str());
    assert!(
        resp.headers()["set-cookie"]
            .to_str()
            .unwrap()
            .starts_with("session=1"),
        "{:?}",
        resp.headers()
    );

    assert_eq!(leak_hits.load(Ordering::SeqCst), 0);
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

/// All tenants share the gateway origin, so upstream headers that act
/// on the whole origin must not reach the browser. Other app headers
/// pass through.
#[tokio::test]
async fn origin_wide_response_headers_are_stripped() {
    let upstream = serve(upstream_app()).await;
    let (app, _cp) = gateway_app(&format!("http://{upstream}"));
    let gw = serve(app).await;

    let resp = reqwest::Client::new()
        .get(format!("http://{gw}/t/u1/origin-wide"))
        .bearer_auth("k1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    for name in [
        "strict-transport-security",
        "alt-svc",
        "service-worker-allowed",
        "clear-site-data",
        "nel",
        "report-to",
    ] {
        assert!(
            !resp.headers().contains_key(name),
            "{name} leaked: {:?}",
            resp.headers()
        );
    }
    assert_eq!(resp.headers()["x-app"], "kept");
}

/// A body framed by `Content-Length` keeps that framing upstream — an
/// upload is not turned into chunked, and a GET body is not dropped.
/// A chunked body stays chunked, and a bodiless GET gains no length.
#[tokio::test]
async fn request_body_framing_reaches_the_vm() {
    let upstream = serve(upstream_app()).await;
    let (app, _cp) = gateway_app(&format!("http://{upstream}"));
    let gw = serve(app).await;
    let http = reqwest::Client::new();
    let echo = |req: reqwest::RequestBuilder| async move {
        let resp = req.bearer_auth("k1").send().await.unwrap();
        assert_eq!(resp.status(), 200);
        resp.json::<serde_json::Value>().await.unwrap()
    };

    let post = echo(http.post(format!("http://{gw}/t/u1/echo")).body("hello")).await;
    assert_eq!(post["content_length"], "5", "{post}");
    assert_eq!(post["transfer_encoding"], "", "{post}");
    assert_eq!(post["body"], "hello");

    let get = echo(http.get(format!("http://{gw}/t/u1/echo")).body("q=1")).await;
    assert_eq!(get["content_length"], "3", "{get}");
    assert_eq!(get["body"], "q=1");

    let bodiless = echo(http.get(format!("http://{gw}/t/u1/echo"))).await;
    assert_eq!(bodiless["content_length"], "", "{bodiless}");
    assert_eq!(bodiless["transfer_encoding"], "", "{bodiless}");

    let chunks = futures_util::stream::iter([Ok::<_, std::io::Error>("part-1,"), Ok("part-2")]);
    let chunked = echo(
        http.post(format!("http://{gw}/t/u1/echo"))
            .body(reqwest::Body::wrap_stream(chunks)),
    )
    .await;
    assert_eq!(chunked["transfer_encoding"], "chunked", "{chunked}");
    assert_eq!(chunked["content_length"], "", "{chunked}");
    assert_eq!(chunked["body"], "part-1,part-2");
}

/// Sends `GET /t/{tenant}/x` with the test key and returns the status
/// and the raw body.
async fn get_status_and_body(gw: std::net::SocketAddr, tenant: &str) -> (u16, String) {
    let resp = reqwest::Client::new()
        .get(format!("http://{gw}/t/{tenant}/x"))
        .bearer_auth("k1")
        .send()
        .await
        .unwrap();
    (resp.status().as_u16(), resp.text().await.unwrap())
}

/// At `max_vms` a new tenant gets 503 with a fixed body.
#[tokio::test]
async fn pool_exhaustion_is_503_with_a_fixed_body() {
    let upstream = serve(upstream_app()).await;
    let cp = Arc::new(MockControlPlane::new().endpoint_override(&format!("http://{upstream}")));
    let mut cfg = pool_config();
    cfg.max_vms = 1;
    let keys = HashMap::from([("k1".to_owned(), None)]);
    let gw = serve(kotatsud::gateway::router(gateway_state(cp, keys, cfg))).await;

    assert_eq!(get_status_and_body(gw, "u1").await.0, 200);
    assert_eq!(
        get_status_and_body(gw, "u2").await,
        (503, r#"{"error":"no sandbox capacity"}"#.to_owned())
    );
}

/// A VM that does not boot within the wait budget gives 504. The body
/// is fixed — the waiter's error names the MicroVM.
#[tokio::test]
async fn wait_timeout_is_504_without_the_microvm_id() {
    let upstream = serve(upstream_app()).await;
    let cp = Arc::new(
        MockControlPlane::with_behavior(MockBehavior {
            boot_time: Duration::from_secs(60),
            ..Default::default()
        })
        .endpoint_override(&format!("http://{upstream}")),
    );
    let mut cfg = pool_config();
    cfg.wait.timeout = Duration::from_millis(200);
    let keys = HashMap::from([("k1".to_owned(), None)]);
    let gw = serve(kotatsud::gateway::router(gateway_state(cp, keys, cfg))).await;

    assert_eq!(
        get_status_and_body(gw, "u1").await,
        (
            504,
            r#"{"error":"timed out waiting for the sandbox"}"#.to_owned()
        )
    );
}

/// An unreachable VM endpoint gives 502. The body is fixed — the HTTP
/// client's error names the endpoint URL.
#[tokio::test]
async fn unreachable_vm_is_502_without_the_endpoint_url() {
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let (app, _cp) = gateway_app(&format!("http://{closed}"));
    let gw = serve(app).await;

    assert_eq!(
        get_status_and_body(gw, "u1").await,
        (502, r#"{"error":"upstream unavailable"}"#.to_owned())
    );
}
