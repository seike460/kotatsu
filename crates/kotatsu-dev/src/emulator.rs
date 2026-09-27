//! The emulator: lifecycle state machine, hook driver, control API.
//!
//! Hook calls follow the real contract — `POST
//! /aws/lambda-microvms/runtime/v1/{hook}` — and a hook answering
//! 404/405/501 counts as "not implemented → success".

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{any, get, post};
use serde::Serialize;
use tokio::sync::watch;

/// Lifecycle states mirroring the service contract.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DevState {
    /// Booting: `/run` then `/ready` polling is in progress.
    Pending,
    /// Serving traffic.
    Running,
    /// Folded away; traffic triggers `/resume` when `auto_resume`.
    Suspended,
    /// Final state; every request gets 410.
    Terminated,
    /// A hook call failed or timed out.
    Failed(String),
}

impl std::fmt::Display for DevState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "PENDING"),
            Self::Running => write!(f, "RUNNING"),
            Self::Suspended => write!(f, "SUSPENDED"),
            Self::Terminated => write!(f, "TERMINATED"),
            Self::Failed(e) => write!(f, "FAILED({e})"),
        }
    }
}

/// App-side lifecycle hook paths under
/// [`kotatsu::HOOK_PATH_PREFIX`] — the platform always POSTs to these.
#[derive(Clone, Debug)]
pub struct HookPaths {
    /// Image-validation hook (driven once, before `run`).
    pub validate: String,
    /// Polled until 2xx while `Pending`.
    pub ready: String,
    /// Called once at boot with a JSON body.
    pub run: String,
    /// Called before entering `Suspended`.
    pub suspend: String,
    /// Called on `Suspended → Running`.
    pub resume: String,
    /// Called before entering `Terminated`.
    pub terminate: String,
}

impl Default for HookPaths {
    fn default() -> Self {
        let p = kotatsu::HOOK_PATH_PREFIX;
        Self {
            validate: format!("{p}/validate"),
            ready: format!("{p}/ready"),
            run: format!("{p}/run"),
            suspend: format!("{p}/suspend"),
            resume: format!("{p}/resume"),
            terminate: format!("{p}/terminate"),
        }
    }
}

/// Emulator configuration.
#[derive(Clone, Debug)]
pub struct EmulatorConfig {
    /// Base URL of the local app being emulated (`http://127.0.0.1:PORT`).
    pub app_url: String,
    /// The app's port — accepted value for `X-aws-proxy-port`.
    pub app_port: u16,
    /// Address the emulator (the fake VM endpoint) binds.
    pub listen: SocketAddr,
    /// Fake MicroVM id sent in the `/run` hook body.
    pub microvm_id: String,
    /// JSON string sent as `runHookPayload` in the `/run` hook body.
    pub run_hook_payload: String,
    /// Lifecycle hook paths on the app.
    pub hooks: HookPaths,
    /// How long `/ready` may take before boot `Failed`s.
    pub ready_timeout: Duration,
    /// `/ready` poll interval.
    pub ready_poll: Duration,
    /// Per-hook-call timeout (the platform gives hooks a bounded budget;
    /// a stalled app must not wedge the lifecycle state machine).
    pub hook_timeout: Duration,
    /// Additional accepted `X-aws-proxy-auth` values (direct use).
    pub accepted_tokens: Vec<String>,
    /// Accept `dev-token-*` — the tokens `MockControlPlane` mints, so a
    /// `kotatsud --mock` gateway can drive this emulator end to end.
    pub accept_mock_tokens: bool,
    /// Traffic to a `Suspended` emulator drives `/resume` first (the
    /// AWS `autoResume` contract).
    pub auto_resume: bool,
}

impl EmulatorConfig {
    /// Minimal config: app at `app_url`, ephemeral listener.
    pub fn new(app_url: impl Into<String>) -> Self {
        Self {
            app_url: app_url.into(),
            app_port: 8080,
            listen: "127.0.0.1:0".parse().expect("valid addr"),
            microvm_id: "microvm-dev000000001".into(),
            run_hook_payload: "{}".into(),
            hooks: HookPaths::default(),
            ready_timeout: Duration::from_secs(30),
            ready_poll: Duration::from_millis(50),
            hook_timeout: Duration::from_secs(30),
            accepted_tokens: vec![],
            accept_mock_tokens: true,
            auto_resume: true,
        }
    }
}

/// Shared state for the axum handlers.
pub(crate) struct Shared {
    pub cfg: EmulatorConfig,
    pub app: url::Url,
    /// Broadcast lifecycle state — `watch` gives atomic CAS via
    /// `send_if_modified` and a lost-notification-free `wait_for`.
    pub state_tx: watch::Sender<DevState>,
    pub http: reqwest::Client,
    /// Serializes lifecycle transitions (suspend/resume/terminate and
    /// auto-resume must not overlap).
    pub lifecycle: tokio::sync::Mutex<()>,
    /// Boot driver handle — aborted on terminate/drop.
    pub boot: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Shared {
    pub fn dev_state(&self) -> DevState {
        self.state_tx.borrow().clone()
    }

    pub fn set_state(&self, s: DevState) {
        // `send` drops the value when no Receiver exists; send_modify
        // stores unconditionally (subscribers still get notified).
        self.state_tx.send_modify(|cur| *cur = s);
    }

    /// POSTs a lifecycle hook to the app under `hook_timeout`. 2xx = ok;
    /// 404/405/501 = "hook not implemented" → ok (hooks are optional —
    /// generic servers answer POST with 501 rather than 404/405).
    pub async fn call_hook(
        &self,
        name: &str,
        path: &str,
        body: serde_json::Value,
    ) -> Result<(), String> {
        let url = self
            .app
            .join(path)
            .map_err(|e| format!("bad hook url {path:?}: {e}"))?;
        let resp = tokio::time::timeout(
            self.cfg.hook_timeout,
            self.http.post(url).json(&body).send(),
        )
        .await
        .map_err(|_| format!("{name} hook timed out"))?
        .map_err(|e| format!("{name} hook connect failed: {e}"))?;
        let s = resp.status();
        if s == StatusCode::NOT_FOUND
            || s == StatusCode::METHOD_NOT_ALLOWED
            || s == StatusCode::NOT_IMPLEMENTED
        {
            // "not implemented" statuses — distinguishable from a real
            // hook only in the debug log.
            tracing::debug!(hook = name, status = %s, "hook skipped (not implemented)");
            return Ok(());
        }
        if s.is_success() {
            Ok(())
        } else {
            Err(format!("{name} hook returned {}", resp.status()))
        }
    }

    /// Drives `Suspended → Running`.
    pub async fn resume(&self) -> Result<(), String> {
        let _g = self.lifecycle.lock().await;
        match self.dev_state() {
            DevState::Running => Ok(()),
            DevState::Suspended => {
                self.call_hook("resume", &self.cfg.hooks.resume, serde_json::json!({}))
                    .await?;
                self.set_state(DevState::Running);
                Ok(())
            }
            other => Err(format!("cannot resume from {other}")),
        }
    }

    /// Drives `Running → Suspended` (idempotent when already suspended).
    pub async fn suspend(&self) -> Result<(), String> {
        let _g = self.lifecycle.lock().await;
        match self.dev_state() {
            DevState::Running => {
                self.call_hook("suspend", &self.cfg.hooks.suspend, serde_json::json!({}))
                    .await?;
                self.set_state(DevState::Suspended);
                Ok(())
            }
            DevState::Suspended => Ok(()),
            other => Err(format!("cannot suspend from {other}")),
        }
    }

    /// Drives any live state to `Terminated` and aborts the boot task.
    pub async fn terminate(&self) -> Result<(), String> {
        let _g = self.lifecycle.lock().await;
        if let Some(boot) = self.boot.lock().take() {
            boot.abort();
        }
        match self.dev_state() {
            DevState::Terminated => Ok(()),
            _ => {
                self.call_hook(
                    "terminate",
                    &self.cfg.hooks.terminate,
                    serde_json::json!({}),
                )
                .await?;
                self.set_state(DevState::Terminated);
                Ok(())
            }
        }
    }
}

/// A running emulator — the local stand-in for one MicroVM endpoint.
pub struct Emulator {
    shared: Arc<Shared>,
    endpoint: String,
    server: tokio::task::JoinHandle<()>,
}

impl Emulator {
    /// Binds the listener, spawns the contract proxy, and boots the
    /// app (`/validate`, `/run`, then `/ready` polling → `Running`).
    pub async fn start(cfg: EmulatorConfig) -> anyhow::Result<Self> {
        let app: url::Url = cfg
            .app_url
            .parse()
            .map_err(|e| anyhow::anyhow!("bad app_url {:?}: {e}", cfg.app_url))?;
        if !matches!(app.scheme(), "http" | "https") {
            anyhow::bail!("app_url must be http(s), got {:?}", cfg.app_url);
        }
        let listener = tokio::net::TcpListener::bind(cfg.listen).await?;
        let endpoint = format!("http://{}", listener.local_addr()?);

        let (state_tx, _rx) = watch::channel(DevState::Pending);
        let shared = Arc::new(Shared {
            cfg,
            app,
            state_tx,
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .build()?,
            lifecycle: tokio::sync::Mutex::new(()),
            boot: parking_lot::Mutex::new(None),
        });

        let app_router = Router::new()
            .route("/_kotatsu/state", get(state_handler))
            .route("/_kotatsu/suspend", post(suspend_handler))
            .route("/_kotatsu/resume", post(resume_handler))
            .route("/_kotatsu/terminate", post(terminate_handler))
            .fallback(any(crate::proxy::contract_proxy))
            .with_state(shared.clone());

        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app_router).await;
        });

        // Boot driver: /validate, /run (JSON body), then /ready POST
        // polling → RUNNING. The transition is a CAS from Pending so a
        // terminate mid-boot cannot be resurrected by a late success.
        let boot = {
            let shared = shared.clone();
            tokio::spawn(async move {
                let boot = async {
                    shared
                        .call_hook(
                            "validate",
                            &shared.cfg.hooks.validate,
                            serde_json::json!({}),
                        )
                        .await?;
                    shared
                        .call_hook(
                            "run",
                            &shared.cfg.hooks.run,
                            serde_json::json!({
                                "microvmId": shared.cfg.microvm_id,
                                "runHookPayload": shared.cfg.run_hook_payload,
                            }),
                        )
                        .await?;
                    let deadline = tokio::time::Instant::now() + shared.cfg.ready_timeout;
                    loop {
                        if tokio::time::Instant::now() > deadline {
                            return Err("ready hook timed out".to_string());
                        }
                        let url = shared
                            .app
                            .join(&shared.cfg.hooks.ready)
                            .map_err(|e| format!("bad ready url: {e}"))?;
                        let probe = tokio::time::timeout(
                            shared.cfg.hook_timeout,
                            shared.http.post(url).send(),
                        )
                        .await;
                        match probe {
                            Ok(Ok(r)) if r.status().is_success() => return Ok(()),
                            Ok(Ok(r))
                                if r.status() == StatusCode::NOT_FOUND
                                    || r.status() == StatusCode::METHOD_NOT_ALLOWED
                                    || r.status() == StatusCode::NOT_IMPLEMENTED =>
                            {
                                // Hook not implemented → app is ready.
                                return Ok(());
                            }
                            Ok(Ok(_)) | Ok(Err(_)) => {
                                tokio::time::sleep(shared.cfg.ready_poll).await
                            }
                            Err(_) => return Err("ready hook timed out".to_string()),
                        }
                    }
                };
                let outcome = boot.await;
                // CAS: only Pending may become Running/Failed — a
                // concurrent terminate() must win.
                shared.state_tx.send_if_modified(|s| {
                    if !matches!(s, DevState::Pending) {
                        return false;
                    }
                    *s = match &outcome {
                        Ok(()) => DevState::Running,
                        Err(e) => DevState::Failed(e.clone()),
                    };
                    true
                });
            })
        };
        *shared.boot.lock() = Some(boot);

        Ok(Self {
            shared,
            endpoint,
            server,
        })
    }

    /// The fake VM endpoint URL (`http://127.0.0.1:PORT`) — feed this to
    /// `MockControlPlane::endpoint_override` or `--mock-endpoint`.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Current lifecycle state.
    pub fn state(&self) -> DevState {
        self.shared.dev_state()
    }

    /// Waits for boot to settle (Running or Failed).
    pub async fn wait_boot(&self) -> DevState {
        let mut rx = self.shared.state_tx.subscribe();
        let _ = rx.wait_for(|s| !matches!(s, DevState::Pending)).await;
        rx.borrow().clone()
    }

    /// Suspends via the app's `/suspend` hook (control-side call; the
    /// same transition the `/_kotatsu/suspend` route performs).
    pub async fn suspend(&self) -> anyhow::Result<()> {
        self.shared.suspend().await.map_err(anyhow::Error::msg)
    }

    /// Resumes via the app's `/resume` hook.
    pub async fn resume(&self) -> anyhow::Result<()> {
        self.shared.resume().await.map_err(anyhow::Error::msg)
    }

    /// Terminates via the app's `/terminate` hook, aborts the boot
    /// driver, and stops the server.
    pub async fn terminate(self) -> anyhow::Result<()> {
        self.shared.terminate().await.map_err(anyhow::Error::msg)?;
        self.server.abort();
        Ok(())
    }
}

impl Drop for Emulator {
    fn drop(&mut self) {
        if let Some(boot) = self.shared.boot.lock().take() {
            boot.abort();
        }
        self.server.abort();
    }
}

// -- control API (operator-facing, not part of the AWS contract) ------

#[derive(Serialize)]
struct StateBody {
    state: DevState,
}

async fn state_handler(State(s): State<Arc<Shared>>) -> axum::Json<StateBody> {
    axum::Json(StateBody {
        state: s.dev_state(),
    })
}

async fn suspend_handler(State(s): State<Arc<Shared>>) -> (StatusCode, String) {
    match s.suspend().await {
        Ok(()) => (StatusCode::OK, "suspended".into()),
        Err(e) => (StatusCode::CONFLICT, e),
    }
}

async fn resume_handler(State(s): State<Arc<Shared>>) -> (StatusCode, String) {
    match s.resume().await {
        Ok(()) => (StatusCode::OK, "running".into()),
        Err(e) => (StatusCode::CONFLICT, e),
    }
}

async fn terminate_handler(State(s): State<Arc<Shared>>) -> (StatusCode, String) {
    match s.terminate().await {
        Ok(()) => (StatusCode::OK, "terminated".into()),
        Err(e) => (StatusCode::CONFLICT, e),
    }
}
