//! Shared fixtures for the kotatsud integration tests.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use kotatsu::ControlPlane;
use kotatsu::{MemoryStore, PoolConfig, RunRequest, SandboxPool, WaitPolicy};
use kotatsud::gateway::{self, AppState};
use metrics_exporter_prometheus::PrometheusBuilder;

/// Pool config for tests: no warm target, insecure endpoints allowed,
/// fast wait policy.
pub fn pool_config() -> PoolConfig {
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.app_port = 8080;
    cfg.token_scope = vec![kotatsu::PortSpec::port(8080).unwrap()];
    cfg.allow_insecure_endpoints = true;
    cfg.wait = WaitPolicy {
        timeout: Duration::from_secs(5),
        initial_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(20),
    };
    cfg
}

/// Gateway router over a `MemoryStore` pool on `cp`, with the given
/// `key → allowed tenants` map.
pub fn gateway_router(
    cp: Arc<dyn ControlPlane>,
    api_keys: HashMap<String, Option<HashSet<String>>>,
) -> Router {
    let pool = Arc::new(SandboxPool::new(cp, Arc::new(MemoryStore::new()), pool_config()).unwrap());
    let prom = PrometheusBuilder::new().build_recorder();
    gateway::router(AppState {
        pool,
        metrics: prom.handle(),
        api_keys,
        allow_unauthenticated: false,
        forwarded_proto: "http".into(),
    })
}

/// Spawns `app` on an ephemeral loopback port; returns its address.
pub async fn serve(app: Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap()
    });
    addr
}
