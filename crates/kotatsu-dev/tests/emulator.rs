//! End-to-end emulator tests: a fake "app" records lifecycle hooks at
//! the real contract paths (`POST /aws/lambda-microvms/runtime/v1/*`)
//! and echoes traffic; the emulator enforces the AWS contract in front.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::{Request, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::{any, get, post};
use futures_util::{SinkExt, StreamExt};
use kotatsu::HOOK_PATH_PREFIX;
use kotatsu_dev::{DevState, Emulator, EmulatorConfig};
use parking_lot::Mutex;
use tokio_tungstenite::tungstenite::Message as TungMsg;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

fn hook(name: &str) -> String {
    format!("{HOOK_PATH_PREFIX}/{name}")
}

/// A fake app: records hook invocations, verifies the `/run` JSON body,
/// echoes requests, and serves an echo WS at `/ws` (no contract
/// subprotocols — that lives at the emulator edge).
#[derive(Clone, Default)]
struct Hooks {
    calls: Arc<Mutex<Vec<String>>>,
    run_body: Arc<Mutex<Option<serde_json::Value>>>,
}

fn hooks_app(hooks: Hooks) -> Router {
    let record = |name: &'static str| {
        let hooks = hooks.clone();
        move |body: Option<axum::Json<serde_json::Value>>| {
            let hooks = hooks.clone();
            async move {
                hooks.calls.lock().push(name.into());
                if let Some(b) = body {
                    hooks.run_body.lock().replace(b.0);
                }
                "ok"
            }
        }
    };
    Router::new()
        .route(&hook("ready"), post(|| async { "ready" }))
        .route(&hook("validate"), post(record("validate")))
        .route(&hook("run"), post(record("run")))
        .route(&hook("suspend"), post(record("suspend")))
        .route(&hook("resume"), post(record("resume")))
        .route(&hook("terminate"), post(record("terminate")))
        .route(
            "/ws",
            get(|ws: WebSocketUpgrade| async move {
                ws.on_upgrade(|mut socket| async move {
                    while let Some(Ok(m)) = socket.recv().await {
                        if socket.send(m).await.is_err() {
                            return;
                        }
                    }
                })
            }),
        )
        .fallback(any(|req: Request| async move {
            let leaked = req.headers().contains_key("x-aws-proxy-auth")
                || req.headers().contains_key("x-aws-proxy-port");
            let path_q = req
                .uri()
                .path_and_query()
                .map(|pq| pq.to_string())
                .unwrap_or_default();
            let body = axum::body::to_bytes(req.into_body(), usize::MAX)
                .await
                .unwrap();
            serde_json::json!({
                "path": path_q,
                "body": String::from_utf8_lossy(&body),
                "contract_headers_leaked": leaked,
            })
            .to_string()
        }))
}

async fn serve(app: Router) -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    a
}

async fn up(hooks: Hooks) -> (Emulator, String) {
    up_with(hooks_app(hooks), |_| {}).await
}

/// Boots an emulator in front of `app` (hooks it lacks answer 404, i.e.
/// "not implemented") after `tweak` adjusts the config.
async fn up_with(app: Router, tweak: impl FnOnce(&mut EmulatorConfig)) -> (Emulator, String) {
    let app = serve(app).await;
    let mut cfg = EmulatorConfig::new(format!("http://{app}"));
    cfg.app_port = 8080;
    cfg.ready_poll = Duration::from_millis(10);
    tweak(&mut cfg);
    let emu = Emulator::start(cfg).await.unwrap();
    assert_eq!(emu.wait_boot().await, DevState::Running);
    let ep = emu.endpoint().to_owned();
    (emu, ep)
}

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

fn authed(http: &reqwest::Client, url: &str) -> reqwest::RequestBuilder {
    http.get(url)
        .header("x-aws-proxy-auth", "dev-token-1")
        .header("x-aws-proxy-port", "8080")
}

fn authed_post(http: &reqwest::Client, url: &str) -> reqwest::RequestBuilder {
    http.post(url)
        .header("x-aws-proxy-auth", "dev-token-1")
        .header("x-aws-proxy-port", "8080")
}

#[tokio::test]
async fn contract_proxy_forwards_and_strips_headers() {
    let hooks = Hooks::default();
    let (_emu, ep) = up(hooks.clone()).await;
    let http = client();

    let resp = authed_post(&http, &format!("{ep}/work?q=1"))
        .body("data")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let b: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(b["path"], "/work?q=1");
    assert_eq!(b["body"], "data");
    assert_eq!(b["contract_headers_leaked"], false);

    // Boot hooks ran in order, /run carried the contract body.
    let calls = hooks.calls.lock().clone();
    assert_eq!(calls, vec!["validate", "run"]);
    let body = hooks.run_body.lock().clone().unwrap();
    assert_eq!(body["microvmId"], "microvm-dev000000001");
    assert!(body.get("runHookPayload").is_some());
}

#[tokio::test]
async fn contract_violations_are_rejected() {
    let hooks = Hooks::default();
    let (_emu, ep) = up(hooks).await;
    let http = client();

    // No auth header.
    let r = http
        .get(format!("{ep}/x"))
        .header("x-aws-proxy-port", "8080")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 401);

    // Bad token.
    let r = http
        .get(format!("{ep}/x"))
        .header("x-aws-proxy-auth", "wrong-token")
        .header("x-aws-proxy-port", "8080")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 403);

    // Wrong port.
    let r = http
        .get(format!("{ep}/x"))
        .header("x-aws-proxy-auth", "dev-token-1")
        .header("x-aws-proxy-port", "9999")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 400);
}

#[tokio::test]
async fn suspend_then_traffic_auto_resumes() {
    let hooks = Hooks::default();
    let (emu, ep) = up(hooks.clone()).await;
    let http = client();

    emu.suspend().await.unwrap();
    assert_eq!(emu.state(), DevState::Suspended);

    // Traffic drives /resume transparently, then serves.
    let resp = authed(&http, &format!("{ep}/after")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(emu.state(), DevState::Running);

    let calls = hooks.calls.lock().clone();
    assert_eq!(calls, vec!["validate", "run", "suspend", "resume"]);
}

#[tokio::test]
async fn terminate_blocks_traffic() {
    let hooks = Hooks::default();
    let (emu, ep) = up(hooks.clone()).await;
    let http = client();

    // Control API terminate.
    let r = http
        .post(format!("{ep}/_kotatsu/terminate"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(emu.state(), DevState::Terminated);

    let resp = authed(&http, &format!("{ep}/x")).send().await.unwrap();
    assert_eq!(resp.status(), 410);
    assert!(hooks.calls.lock().contains(&"terminate".to_string()));
}

#[tokio::test]
async fn websocket_contract_echoes() {
    let hooks = Hooks::default();
    let (_emu, ep) = up(hooks).await;
    let ws_ep = ep.replacen("http://", "ws://", 1);

    // Contract handshake: base + authentication + port subprotocols.
    let mut req = format!("{ws_ep}/ws").into_client_request().unwrap();
    req.headers_mut().insert(
        "sec-websocket-protocol",
        "lambda-microvms, lambda-microvms.authentication.dev-token-1, lambda-microvms.port.8080"
            .parse()
            .unwrap(),
    );
    let (mut ws, resp) = tokio_tungstenite::connect_async(req).await.unwrap();
    assert_eq!(resp.status(), 101);

    ws.send(TungMsg::Text("hello".into())).await.unwrap();
    let msg = ws.next().await.unwrap().unwrap();
    assert!(matches!(msg, TungMsg::Text(t) if t.as_str() == "hello"));

    // Missing subprotocols → handshake rejected.
    let bad = tokio_tungstenite::connect_async(format!("{ws_ep}/ws")).await;
    assert!(bad.is_err());
}

#[tokio::test]
async fn control_state_endpoint_reports_lifecycle() {
    let hooks = Hooks::default();
    let (emu, ep) = up(hooks).await;
    let http = client();

    let r: serde_json::Value = http
        .get(format!("{ep}/_kotatsu/state"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r, serde_json::json!({"state": "RUNNING"}));

    let r = http
        .post(format!("{ep}/_kotatsu/suspend"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(emu.state(), DevState::Suspended);

    let r = http
        .post(format!("{ep}/_kotatsu/resume"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(emu.state(), DevState::Running);
}

/// A request hitting the emulator while still Pending gets 503.
#[tokio::test]
async fn pending_state_returns_503() {
    // App whose /ready never succeeds (a real 5xx, not "unimplemented").
    let counter = Arc::new(AtomicU32::new(0));
    let c = counter.clone();
    let app = Router::new().route(
        &hook("ready"),
        post(move || {
            let c = c.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                axum::http::StatusCode::SERVICE_UNAVAILABLE
            }
        }),
    );
    let app_addr = serve(app).await;

    let mut cfg = EmulatorConfig::new(format!("http://{app_addr}"));
    cfg.ready_timeout = Duration::from_secs(5);
    cfg.ready_poll = Duration::from_millis(50);
    let emu = Emulator::start(cfg).await.unwrap();

    // Fire while boot is still polling /ready.
    tokio::time::sleep(Duration::from_millis(30)).await;
    if emu.state() == DevState::Pending {
        let resp = authed(&client(), &format!("{}/x", emu.endpoint()))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 503);
    } else {
        panic!("boot finished too fast: {:?}", emu.state());
    }
}

/// An app whose hooks all 404 must still boot — unimplemented hooks
/// (404/405) count as success on real AWS.
#[tokio::test]
async fn hookless_app_boots() {
    let app = Router::new().fallback(any(|req: Request| async move {
        if req.uri().path().starts_with(HOOK_PATH_PREFIX) {
            return axum::http::StatusCode::NOT_FOUND.into_response();
        }
        axum::response::Response::new(axum::body::Body::from("bare app"))
    }));
    let app_addr = serve(app).await;
    let mut cfg = EmulatorConfig::new(format!("http://{app_addr}"));
    cfg.ready_poll = Duration::from_millis(10);
    let emu = Emulator::start(cfg).await.unwrap();
    assert_eq!(emu.wait_boot().await, DevState::Running);

    // /run 404 was accepted; traffic flows.
    let resp = authed(&client(), emu.endpoint()).send().await.unwrap();
    assert_eq!(resp.status(), 200);
}

/// Generic HTTP servers answer unknown POSTs with 501 — that must count
/// as "hook not implemented" too (this was a real e2e failure).
#[tokio::test]
async fn hookless_501_app_boots() {
    let app = Router::new().fallback(any(|req: Request| async move {
        if req.uri().path().starts_with(HOOK_PATH_PREFIX) {
            return axum::http::StatusCode::NOT_IMPLEMENTED.into_response();
        }
        axum::response::Response::new(axum::body::Body::from("bare app"))
    }));
    let app_addr = serve(app).await;
    let mut cfg = EmulatorConfig::new(format!("http://{app_addr}"));
    cfg.ready_poll = Duration::from_millis(10);
    let emu = Emulator::start(cfg).await.unwrap();
    assert_eq!(emu.wait_boot().await, DevState::Running);
}

/// A terminate racing an in-flight boot must win — no resurrection.
/// Two layers cooperate: terminate() aborts the boot task first (this
/// test's path — the task dies at its /ready await), and the boot's
/// send_if_modified CAS refuses to leave Pending-exited states if a
/// late outcome ever lands mid-sync-code.
#[tokio::test]
async fn terminate_during_pending_cannot_resurrect() {
    let app = Router::new()
        .route(
            &hook("ready"),
            post(|| async {
                tokio::time::sleep(Duration::from_millis(500)).await;
                "late"
            }),
        )
        .route(&hook("terminate"), post(|| async { "ok" }));
    let app_addr = serve(app).await;
    let mut cfg = EmulatorConfig::new(format!("http://{app_addr}"));
    cfg.ready_poll = Duration::from_millis(5);
    cfg.hook_timeout = Duration::from_secs(5);
    let emu = Emulator::start(cfg).await.unwrap();

    // Terminate while the ready-poll is still sleeping on the app.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(emu.state(), DevState::Pending);
    let r = client()
        .post(format!("{}/_kotatsu/terminate", emu.endpoint()))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    // The late /ready success lands ~500ms — the CAS must refuse it.
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(emu.state(), DevState::Terminated);
}

/// The app's 3xx and its `Set-Cookie` reach the client unchanged: the
/// emulator must not follow `Location` itself (a POST 301/302 would
/// turn into a GET and drop the redirect's cookie).
#[tokio::test]
async fn app_redirects_reach_the_client_unfollowed() {
    let app = Router::new()
        .route(
            "/redirect/{code}",
            post(
                |axum::extract::Path(code): axum::extract::Path<u16>| async move {
                    (
                        axum::http::StatusCode::from_u16(code).unwrap(),
                        [("location", "/home"), ("set-cookie", "sid=1; Path=/")],
                    )
                },
            ),
        )
        .route("/home", any(|| async { "home" }));
    let (_emu, ep) = up_with(app, |_| {}).await;
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    for code in [301, 302, 303, 307, 308] {
        let resp = authed_post(&http, &format!("{ep}/redirect/{code}"))
            .body("user=a")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), code);
        assert_eq!(resp.headers()["location"], "/home", "{code}");
        assert_eq!(resp.headers()["set-cookie"], "sid=1; Path=/", "{code}");
    }
}

/// A `/terminate` hook failing mid-boot must not leave the emulator
/// `Pending` forever: the boot task is already aborted, so it settles
/// `Failed` and `wait_boot` returns.
#[tokio::test]
async fn terminate_hook_failure_during_boot_settles_failed() {
    let app = Router::new()
        .route(
            &hook("ready"),
            post(|| async { axum::http::StatusCode::SERVICE_UNAVAILABLE }),
        )
        .route(
            &hook("terminate"),
            post(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
        );
    let app_addr = serve(app).await;
    let mut cfg = EmulatorConfig::new(format!("http://{app_addr}"));
    cfg.ready_timeout = Duration::from_secs(30);
    let emu = Emulator::start(cfg).await.unwrap();
    assert_eq!(emu.state(), DevState::Pending);

    let r = client()
        .post(format!("{}/_kotatsu/terminate", emu.endpoint()))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 409);
    let settled = tokio::time::timeout(Duration::from_secs(5), emu.wait_boot())
        .await
        .expect("wait_boot must return");
    assert_eq!(
        settled,
        DevState::Failed("terminate hook returned 500 Internal Server Error".into())
    );
    let resp = authed(&client(), &format!("{}/x", emu.endpoint()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 500);
}

/// A failed `/suspend` hook leaves the emulator `Running` (409 names the
/// hook error); a failed auto-resume answers 502, as AWS does, and
/// leaves it `Suspended`.
#[tokio::test]
async fn failed_suspend_or_resume_hook_keeps_state() {
    let fail_suspend = Arc::new(AtomicBool::new(true));
    let f = fail_suspend.clone();
    let app = Router::new()
        .route(
            &hook("suspend"),
            post(move || {
                let f = f.clone();
                async move {
                    if f.load(Ordering::SeqCst) {
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR
                    } else {
                        axum::http::StatusCode::OK
                    }
                }
            }),
        )
        .route(
            &hook("resume"),
            post(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
        );
    let (emu, ep) = up_with(app, |_| {}).await;
    let http = client();

    let r = http
        .post(format!("{ep}/_kotatsu/suspend"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 409);
    assert_eq!(
        r.text().await.unwrap(),
        "suspend hook returned 500 Internal Server Error"
    );
    assert_eq!(emu.state(), DevState::Running);

    fail_suspend.store(false, Ordering::SeqCst);
    emu.suspend().await.unwrap();
    let resp = authed(&http, &format!("{ep}/x")).send().await.unwrap();
    assert_eq!(resp.status(), 502);
    assert_eq!(emu.state(), DevState::Suspended);
}

/// A boot hook failure settles `Failed`: the state API reports it as a
/// plain `"FAILED"` string plus `"error"`, and traffic gets 500.
#[tokio::test]
async fn failed_boot_reports_state_and_answers_500() {
    let app = Router::new().route(
        &hook("run"),
        post(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }),
    );
    let app_addr = serve(app).await;
    let emu = Emulator::start(EmulatorConfig::new(format!("http://{app_addr}")))
        .await
        .unwrap();
    let error = "run hook returned 500 Internal Server Error";
    assert_eq!(emu.wait_boot().await, DevState::Failed(error.into()));

    let r: serde_json::Value = client()
        .get(format!("{}/_kotatsu/state", emu.endpoint()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(r, serde_json::json!({"state": "FAILED", "error": error}));

    let resp = authed(&client(), &format!("{}/x", emu.endpoint()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 500);
}

/// Browsers attach `Origin` to every POST: a web page must not drive
/// the unauthenticated control API (CSRF).
#[tokio::test]
async fn control_api_refuses_browser_origin() {
    let hooks = Hooks::default();
    let (emu, ep) = up(hooks.clone()).await;

    for route in ["suspend", "resume", "terminate"] {
        let r = client()
            .post(format!("{ep}/_kotatsu/{route}"))
            .header("origin", "https://evil.example")
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 403, "{route}");
    }
    assert_eq!(emu.state(), DevState::Running);
    assert_eq!(hooks.calls.lock().clone(), vec!["validate", "run"]);
}

/// The app's WS handshake carries the client's other headers (Cookie
/// here) but not the contract subprotocols — AWS forwards the request
/// minus its MicroVM subprotocols.
#[tokio::test]
async fn websocket_forwards_client_headers_to_app() {
    let app = Router::new().route(
        "/ws",
        get(
            |headers: axum::http::HeaderMap, ws: WebSocketUpgrade| async move {
                let seen = serde_json::json!({
                    "cookie": headers.get("cookie").and_then(|v| v.to_str().ok()),
                    "protocol": headers
                        .get("sec-websocket-protocol")
                        .and_then(|v| v.to_str().ok()),
                })
                .to_string();
                ws.on_upgrade(move |mut socket| async move {
                    let _ = socket
                        .send(axum::extract::ws::Message::Text(seen.into()))
                        .await;
                })
            },
        ),
    );
    let (_emu, ep) = up_with(app, |_| {}).await;
    let ws_ep = ep.replacen("http://", "ws://", 1);

    let mut req = format!("{ws_ep}/ws").into_client_request().unwrap();
    req.headers_mut().insert(
        "sec-websocket-protocol",
        "lambda-microvms, lambda-microvms.authentication.dev-token-1, lambda-microvms.port.8080"
            .parse()
            .unwrap(),
    );
    req.headers_mut().insert("cookie", "sid=1".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();

    let msg = ws.next().await.unwrap().unwrap();
    let seen: serde_json::Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
    assert_eq!(
        seen,
        serde_json::json!({"cookie": "sid=1", "protocol": null})
    );
}

/// With `auto_resume` off, traffic to a suspended emulator gets 503 and
/// does not call `/resume`.
#[tokio::test]
async fn suspended_without_auto_resume_returns_503() {
    let hooks = Hooks::default();
    let (emu, ep) = up_with(hooks_app(hooks.clone()), |c| c.auto_resume = false).await;
    emu.suspend().await.unwrap();

    let resp = authed(&client(), &format!("{ep}/x")).send().await.unwrap();
    assert_eq!(resp.status(), 503);
    assert_eq!(emu.state(), DevState::Suspended);
    assert!(!hooks.calls.lock().contains(&"resume".to_string()));
}

/// `//host/x` must not become a network-path reference off the app's
/// origin, and a malformed request must not wake a suspended VM.
#[tokio::test]
async fn network_path_target_is_400_without_resuming() {
    let hooks = Hooks::default();
    let (emu, ep) = up(hooks.clone()).await;
    emu.suspend().await.unwrap();

    let resp = authed(&client(), &format!("{ep}//evil.example/x"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    assert_eq!(
        resp.text().await.unwrap(),
        r#"{"error":"bad request target"}"#
    );
    assert_eq!(emu.state(), DevState::Suspended);
    assert!(!hooks.calls.lock().contains(&"resume".to_string()));
}

/// Only exact method+path pairs are control routes: a wrong method is
/// 405 without a transition, and other `/_kotatsu/*` paths reach the app.
#[tokio::test]
async fn control_routes_match_exact_method_and_path() {
    let hooks = Hooks::default();
    let (emu, ep) = up(hooks.clone()).await;
    let http = client();

    let r = http
        .get(format!("{ep}/_kotatsu/terminate"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 405);
    assert_eq!(emu.state(), DevState::Running);
    assert!(!hooks.calls.lock().contains(&"terminate".to_string()));

    let r = authed(&http, &format!("{ep}/_kotatsu/other"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let b: serde_json::Value = r.json().await.unwrap();
    assert_eq!(b["path"], "/_kotatsu/other");
}
