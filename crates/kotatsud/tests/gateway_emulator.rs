//! Full-chain e2e: browser → kotatsud gateway → kotatsu-dev emulator → app.
//!
//! The gateway's `MockControlPlane` mints `dev-token-*` credentials and
//! resolves every VM to the emulator's endpoint (via `endpoint_override`).
//! The emulator validates the contract headers (`X-aws-proxy-auth`,
//! `X-aws-proxy-port`) exactly as the real service would, then proxies to
//! a real axum app — so this exercises auth, token mint, contract headers,
//! and proxying in one request.

mod common;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Request, WebSocketUpgrade};
use axum::routing::{any, get};
use common::{gateway_router, serve};
use futures_util::{SinkExt, StreamExt};
use kotatsu::mock::MockControlPlane;
use kotatsu_dev::{DevState, Emulator, EmulatorConfig};

/// The "app": reports the headers/path it observed; serves a WS echo.
fn app() -> Router {
    Router::new()
        .route(
            "/ws",
            get(|ws: WebSocketUpgrade| async move {
                // The contract subprotocols terminate at the emulator
                // edge — the app's own handshake is protocol-free.
                ws.on_upgrade(|mut socket| async move {
                    while let Some(Ok(m)) = socket.recv().await {
                        if socket.send(m).await.is_err() {
                            return;
                        }
                    }
                })
            }),
        )
        .route(
            "/{*p}",
            any(|req: Request| async move {
                // The emulator strips contract headers before the app —
                // report whatever leaks through (expect none).
                let leaked_auth = req.headers().contains_key("x-aws-proxy-auth");
                let leaked_port = req.headers().contains_key("x-aws-proxy-port");
                let path_q = req
                    .uri()
                    .path_and_query()
                    .map(|pq| pq.to_string())
                    .unwrap_or_default();
                let body = axum::body::to_bytes(req.into_body(), usize::MAX)
                    .await
                    .unwrap();
                serde_json::json!({
                    "leaked_auth": leaked_auth,
                    "leaked_port": leaked_port,
                    "path": path_q,
                    "body": String::from_utf8_lossy(&body),
                })
                .to_string()
            }),
        )
}

async fn chain() -> (SocketAddr, Emulator) {
    // App behind the emulator.
    let app_addr = serve(app()).await;
    let mut cfg = EmulatorConfig::new(format!("http://{app_addr}"));
    cfg.app_port = 8080;
    let emu = Emulator::start(cfg).await.unwrap();
    assert!(matches!(emu.wait_boot().await, DevState::Running));

    // Gateway pointed at the emulator instead of real VM endpoints.
    let cp = Arc::new(MockControlPlane::new().endpoint_override(emu.endpoint()));
    let gw = serve(gateway_router(
        cp,
        std::collections::HashMap::from([("k1".to_owned(), None)]),
    ))
    .await;
    (gw, emu)
}

#[tokio::test]
async fn http_request_flows_gateway_emulator_app() {
    let (gw, emu) = chain().await;
    let http = reqwest::Client::new();

    let resp = http
        .post(format!("http://{gw}/t/u1/echo?q=1"))
        .bearer_auth("k1")
        .body("hello chain")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["path"], "/echo?q=1");
    assert_eq!(body["body"], "hello chain");
    // Contract headers were validated by the emulator then stripped
    // before the app — nothing contract-internal reaches the app.
    assert_eq!(body["leaked_auth"], false);
    assert_eq!(body["leaked_port"], false);

    // A direct call to the emulator without contract creds still fails —
    // the chain did not weaken the emulator's own gate.
    let direct = http
        .get(format!("{}/echo", emu.endpoint()))
        .send()
        .await
        .unwrap();
    assert_eq!(direct.status(), 401);
}

#[tokio::test]
async fn ws_flows_gateway_emulator_app() {
    let (gw, _emu) = chain().await;
    let (mut ws, resp) = tokio_tungstenite::connect_async(format!("ws://{gw}/t/u1/ws?key=k1"))
        .await
        .unwrap();
    assert_eq!(resp.status(), 101);

    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        "through".into(),
    ))
    .await
    .unwrap();
    let msg = ws.next().await.unwrap().unwrap();
    assert!(
        matches!(msg, tokio_tungstenite::tungstenite::Message::Text(t) if t.as_str() == "through")
    );
}
