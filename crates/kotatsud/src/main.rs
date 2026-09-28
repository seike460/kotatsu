//! kotatsud — session gateway daemon for AWS Lambda MicroVM sandboxes.
//!
//! One plain-HTTP endpoint in front of N MicroVMs: it authenticates
//! clients (Bearer API key), resolves tenant→MicroVM affinity through
//! `SandboxPool`, holds requests while a suspended VM resumes, injects
//! `X-aws-proxy-auth`/`X-aws-proxy-port`, and proxies HTTP and WebSocket
//! traffic. It does not terminate TLS — on a public bind, put a
//! TLS-terminating load balancer before it. `/metrics` exposes the
//! pool/gateway Prometheus series — it shares the `--listen` socket
//! unauthenticated; bind privately.
//!
//! `--mock` needs `--mock-endpoint` (e.g. a kotatsu-dev emulator or any
//! local upstream) for proxied requests to reach a real socket.

use kotatsud::gateway;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use kotatsu::{
    AwsControlPlane, IdlePolicyConfig, MemoryStore, PoolConfig, PortSpec, RunRequest, StateStore,
};

/// Session gateway for AWS Lambda MicroVMs.
#[derive(Parser)]
#[command(name = "kotatsud", version, about)]
struct Cli {
    /// TOML config file; CLI/env flags override it.
    #[arg(long, env = "KOTATSU_CONFIG")]
    config: Option<PathBuf>,

    /// Listen address for the gateway [default: 127.0.0.1:3000].
    #[arg(long, env = "KOTATSU_LISTEN")]
    listen: Option<SocketAddr>,

    /// AWS region for the lambda-microvms API.
    #[arg(long, env = "AWS_REGION")]
    region: Option<String>,

    /// MicroVM image ARN used for every pooled VM.
    #[arg(long, env = "KOTATSU_IMAGE")]
    image: Option<String>,

    /// Application port inside each MicroVM [default: 8080].
    #[arg(long, env = "KOTATSU_APP_PORT")]
    app_port: Option<u16>,

    /// Unassigned warm VMs kept ready [default: 4].
    #[arg(long, env = "KOTATSU_WARM_SIZE")]
    warm_size: Option<usize>,

    /// UTC warm-size schedule `HH:MM-HH:MM=N` (repeatable; `24:00`
    /// allowed as end; `22:00-06:00` wraps midnight). The first
    /// matching window overrides `--warm-size` each maintenance tick.
    #[arg(long, env = "KOTATSU_WARM_SCHEDULE", value_parser = parse_warm_window, value_delimiter = ',')]
    warm_schedule: Vec<kotatsu::WarmWindow>,

    /// Hard cap on pool-managed VMs [default: 100].
    #[arg(long, env = "KOTATSU_MAX_VMS")]
    max_vms: Option<usize>,

    /// Terminate VMs older than this (seconds; 0 = disable) [default: 0].
    #[arg(long, env = "KOTATSU_MAX_AGE_SECS")]
    max_age_secs: Option<u64>,

    /// Suspend a VM after this many idle seconds (60-28800; 0 = off: VMs
    /// run without an idle policy and are not auto-suspended) [default: 0].
    #[arg(long, env = "KOTATSU_IDLE_SUSPEND_SECS")]
    idle_suspend_secs: Option<u64>,

    /// Terminate a suspended VM after this many seconds (max 28800)
    /// [default: 28800].
    #[arg(long, env = "KOTATSU_SUSPENDED_TTL_SECS")]
    suspended_ttl_secs: Option<u64>,

    /// Max seconds a request waits for a suspended VM to resume
    /// [default: 120].
    #[arg(long, env = "KOTATSU_WAIT_TIMEOUT_SECS")]
    wait_timeout_secs: Option<u64>,

    /// Value for `x-forwarded-proto` upstream (set `https` behind a
    /// TLS-terminating load balancer) [default: http].
    #[arg(long, env = "KOTATSU_FORWARDED_PROTO")]
    forwarded_proto: Option<String>,

    /// With `--mock`: endpoint override for every mock VM — a local URL
    /// like `http://127.0.0.1:PORT` (enables insecure endpoints).
    #[arg(long, env = "KOTATSU_MOCK_ENDPOINT", requires = "mock")]
    mock_endpoint: Option<String>,

    /// Maintenance tick in seconds [default: 60].
    #[arg(long, env = "KOTATSU_MAINTENANCE_SECS")]
    maintenance_secs: Option<u64>,

    /// Reap live VMs that match the pool image but are tracked
    /// nowhere — the restart-recovery reconcile. Only safe when the
    /// image is dedicated to this pool: an externally-launched VM of
    /// the same image/version is treated as lost and terminated.
    /// Requires `--image` to be the image ARN, not an image ID.
    /// `--reap-lost-vms=false` (or `KOTATSU_REAP_LOST_VMS=false`)
    /// explicitly disables it, overriding a config-file `true`.
    #[arg(long, env = "KOTATSU_REAP_LOST_VMS", num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    reap_lost_vms: Option<bool>,

    /// SQLite file for tenant bindings. Default:
    /// `$XDG_DATA_HOME/kotatsu/bindings.db` (or `~/.local/share/…`);
    /// with `--mock`, in memory. In-memory state orphans running VMs on
    /// restart (bills to 8h).
    #[arg(long, env = "KOTATSU_STATE_DB")]
    state_db: Option<PathBuf>,

    /// Client API keys (repeat or comma-separate via env).
    /// Clients pass `Authorization: Bearer <key>` (or `?key=` for
    /// browser WebSockets). Global keys reach every tenant — prefer
    /// --tenant-key for untrusted callers. Prefer the env var or
    /// --config: other local users can read command-line arguments.
    #[arg(long = "api-key", env = "KOTATSU_API_KEYS", value_delimiter = ',')]
    api_keys: Vec<String>,

    /// Tenant-scoped API key: `TENANT=KEY` (repeatable). The key only
    /// authorizes requests under `/t/{TENANT}` — a leaked scoped key
    /// cannot pivot to other tenants. Prefer the env var or --config:
    /// other local users can read command-line arguments.
    #[arg(
        long = "tenant-key",
        env = "KOTATSU_TENANT_KEYS",
        value_delimiter = ','
    )]
    tenant_keys: Vec<String>,

    /// Allow unauthenticated clients (local development only).
    /// `--allow-unauthenticated=false` (or
    /// `KOTATSU_ALLOW_UNAUTHENTICATED=false`) overrides a config-file
    /// `true`.
    #[arg(long, env = "KOTATSU_ALLOW_UNAUTHENTICATED", num_args = 0..=1, require_equals = true, default_missing_value = "true")]
    allow_unauthenticated: Option<bool>,

    /// Use the mock control plane — no AWS calls, for local testing.
    #[arg(long, env = "KOTATSU_MOCK")]
    mock: bool,
}

/// Optional TOML file; every field is overridden by an explicit CLI/env
/// flag. CLI values win over file values, which win over built-in
/// defaults.
#[derive(serde::Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    listen: Option<SocketAddr>,
    region: Option<String>,
    image: Option<String>,
    app_port: Option<u16>,
    warm_size: Option<usize>,
    #[serde(default)]
    warm_schedule: Vec<String>,
    max_vms: Option<usize>,
    #[serde(default, with = "humantime_serde")]
    max_age: Option<Duration>,
    #[serde(default, with = "humantime_serde")]
    idle_suspend: Option<Duration>,
    #[serde(default, with = "humantime_serde")]
    suspended_ttl: Option<Duration>,
    #[serde(default, with = "humantime_serde")]
    wait_timeout: Option<Duration>,
    forwarded_proto: Option<String>,
    #[serde(default, with = "humantime_serde")]
    maintenance_interval: Option<Duration>,
    reap_lost_vms: Option<bool>,
    state_db: Option<PathBuf>,
    #[serde(default, deserialize_with = "secret_list")]
    api_keys: Vec<String>,
    #[serde(default, deserialize_with = "secret_list")]
    tenant_keys: Vec<String>,
    #[serde(default)]
    allow_unauthenticated: bool,
}

/// An array of secrets. serde's type errors quote the rejected value
/// (`invalid type: string "…"`), so they are replaced.
fn secret_list<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    <Vec<String> as serde::Deserialize>::deserialize(d)
        .map_err(|_| serde::de::Error::custom("expected an array of strings"))
}

/// Reads the `--config` file. Errors name the path and position but
/// never quote the file: it holds API keys, and startup errors end up
/// in collected logs.
fn load_config(path: &std::path::Path) -> anyhow::Result<FileConfig> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("cannot read config {}: {e}", path.display()))?;
    parse_config(&text).map_err(|e| anyhow::anyhow!("invalid config {}: {e}", path.display()))
}

/// Parses the config text. The error is the parser's message and its
/// position; `toml::de::Error`'s `Display` would quote the whole line.
fn parse_config(text: &str) -> Result<FileConfig, String> {
    toml::from_str(text).map_err(|e: toml::de::Error| {
        let msg = e.message().trim_end().replace('\n', "; ");
        let Some(before) = e.span().and_then(|s| text.get(..s.start)) else {
            return msg;
        };
        let line_start = before.rfind('\n').map_or(0, |i| i + 1);
        let line = before.matches('\n').count() + 1;
        let column = before[line_start..].chars().count() + 1;
        format!("line {line}, column {column}: {msg}")
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kotatsud=info,kotatsu=info".into()),
        )
        .init();

    let cli = Cli::parse();
    let file: FileConfig = match &cli.config {
        Some(p) => load_config(p)?,
        None => FileConfig::default(),
    };

    let cfg = Resolved::resolve(&cli, &file)?;
    cfg.validate()?;

    let prom = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .map_err(|e| anyhow::anyhow!("prometheus recorder: {e}"))?;

    // -- control plane --------------------------------------------------
    // `mock_endpoint_is_http` remembers the *parsed* scheme — a case-
    // insensitive `HTTP://` normalizes to http and must still enable
    // insecure endpoints.
    let mut mock_endpoint_is_http = false;
    if let Some(ep) = &cli.mock_endpoint {
        match url::Url::parse(ep) {
            Ok(u) if matches!(u.scheme(), "http" | "https") => {
                mock_endpoint_is_http = u.scheme() == "http";
            }
            _ => anyhow::bail!("--mock-endpoint must be an http(s) URL, got {ep:?}"),
        }
    }
    if let Err(e) = http::header::HeaderValue::from_str(&cfg.forwarded_proto) {
        anyhow::bail!("invalid --forwarded-proto value: {e}");
    }

    let cp: Arc<dyn kotatsu::ControlPlane> = if cli.mock {
        tracing::warn!("mock control plane enabled — no AWS calls will be made");
        let mut mock = kotatsu::mock::MockControlPlane::new();
        if let Some(ep) = &cli.mock_endpoint {
            mock = mock.endpoint_override(ep);
            tracing::info!(endpoint = %ep, "mock endpoint override set");
        } else {
            tracing::warn!("--mock without --mock-endpoint: proxied requests will fail to connect");
        }
        Arc::new(mock)
    } else {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(r) = &cfg.region {
            loader = loader.region(aws_config::Region::new(r.clone()));
        }
        let sdk = loader.load().await;
        Arc::new(AwsControlPlane::new(&sdk))
    };

    // -- state store ----------------------------------------------------
    let store: Arc<dyn StateStore> = match cfg.state_db_path() {
        Some(path) => {
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir).map_err(|e| {
                    anyhow::anyhow!(
                        "cannot create bindings dir {} ({e}) — pass --state-db explicitly",
                        dir.display()
                    )
                })?;
            }
            tracing::info!(path = %path.display(), "persistent bindings (SQLite)");
            Arc::new(kotatsu::SqliteStore::open(&path).await?)
        }
        None if cfg.mock => Arc::new(MemoryStore::new()),
        None => {
            tracing::warn!("in-memory bindings: a restart orphans running VMs — set --state-db");
            Arc::new(MemoryStore::new())
        }
    };

    // -- pool -----------------------------------------------------------
    if cfg.suspended_ttl.is_some() && cfg.idle_suspend.filter(|d| !d.is_zero()).is_none() {
        tracing::warn!("--suspended-ttl-secs has no effect without --idle-suspend-secs");
    }
    let mut run = RunRequest::new(cfg.image.clone().unwrap_or_else(|| "mock-image".into()));
    // `idle_suspend` of 0 means "feature off". `suspended_ttl` of 0 is
    // meaningful — AWS terminates the VM immediately on suspend — so it
    // is passed through (AWS accepts 0..=28800 for that field).
    if let Some(suspend) = cfg.idle_suspend.filter(|d| !d.is_zero()) {
        run.idle_policy = Some(IdlePolicyConfig {
            auto_resume_enabled: true,
            max_idle_duration_seconds: i32::try_from(suspend.as_secs()).unwrap_or(i32::MAX),
            suspended_duration_seconds: i32::try_from(
                cfg.suspended_ttl
                    .unwrap_or(Duration::from_secs(
                        u64::try_from(kotatsu::MAX_DURATION_SECONDS).unwrap_or(u64::MAX),
                    ))
                    .as_secs(),
            )
            .unwrap_or(i32::MAX),
        });
    }
    let mut pool_cfg = PoolConfig::new(run);
    pool_cfg.warm_size = cfg.warm_size;
    pool_cfg.warm_schedule = cfg.warm_schedule;
    pool_cfg.max_vms = cfg.max_vms;
    pool_cfg.app_port = cfg.app_port;
    pool_cfg.token_scope = vec![PortSpec::port(cfg.app_port)?];
    pool_cfg.max_age = cfg.max_age.filter(|d| !d.is_zero());
    if let Some(t) = cfg.wait_timeout {
        pool_cfg.wait.timeout = t;
    }
    pool_cfg.maintenance_interval = cfg.maintenance_interval;
    pool_cfg.reap_lost_vms = cfg.reap_lost_vms;
    // `http://` endpoints only when a mock endpoint override says so.
    pool_cfg.allow_insecure_endpoints = mock_endpoint_is_http;
    let pool = Arc::new(kotatsu::SandboxPool::new(cp, store, pool_cfg)?);

    let _maintenance = pool.spawn_maintenance();
    // `assigned` is store-derived and only refreshes via stats().
    {
        let pool = pool.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(15)).await;
                pool.stats().await;
            }
        });
    }

    // -- http server ----------------------------------------------------
    let app = gateway::router(gateway::AppState {
        pool,
        metrics: prom,
        api_keys: build_api_keys(&cfg.api_keys, &cfg.tenant_keys),
        allow_unauthenticated: cfg.allow_unauthenticated,
        forwarded_proto: cfg.forwarded_proto,
    });

    let listener = tokio::net::TcpListener::bind(cfg.listen).await?;
    tracing::info!(listen = %cfg.listen, "kotatsud serving");
    if !cfg.listen.ip().is_loopback() {
        tracing::warn!(
            "listening on a non-loopback address in plain HTTP — put a \
             TLS-terminating load balancer in front or API keys and \
             tenant traffic are visible on the wire"
        );
        if cfg.allow_unauthenticated {
            tracing::warn!(
                "--allow-unauthenticated on a public bind: every request \
                 (incl. mock-mode endpoints) is open to the network"
            );
        }
    }
    serve_until(listener, app, shutdown_signal(), SHUTDOWN_GRACE).await?;
    Ok(())
}

/// How long open connections get to finish after SIGTERM/SIGINT — less
/// than the 30 s that Kubernetes and ECS wait before SIGKILL.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(20);

/// Serves `app` until `signal`, then stops accepting connections and
/// gives open ones `grace` to finish before dropping them. A response
/// streamed from a VM (SSE, a long download) may never end, and axum's
/// graceful shutdown alone would wait for it forever.
async fn serve_until(
    listener: tokio::net::TcpListener,
    app: axum::Router,
    signal: impl Future<Output = ()> + Send + 'static,
    grace: Duration,
) -> std::io::Result<()> {
    let (stopping_tx, stopping) = tokio::sync::oneshot::channel();
    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        signal.await;
        let _ = stopping_tx.send(());
    });
    let expired = async {
        if stopping.await.is_ok() {
            tokio::time::sleep(grace).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    tokio::select! {
        res = server.into_future() => res,
        () = expired => {
            tracing::warn!(
                ?grace,
                "connections still open after the shutdown grace period — closing them"
            );
            Ok(())
        }
    }
}

/// Builds the gateway's key map: `key → Option<allowed tenants>`
/// (`None` = global). When a key is declared more than once, scopes
/// union — a global declaration stays global, scoped declarations
/// merge their tenant sets. A duplicate never silently narrows a grant.
fn build_api_keys(
    api_keys: &[String],
    tenant_keys: &[(String, String)],
) -> std::collections::HashMap<String, Option<std::collections::HashSet<String>>> {
    let mut map = std::collections::HashMap::new();
    for k in api_keys {
        map.insert(k.clone(), None);
    }
    for (t, k) in tenant_keys {
        match map.entry(k.clone()) {
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(Some(std::collections::HashSet::from([t.clone()])));
            }
            std::collections::hash_map::Entry::Occupied(mut e) => {
                if let Some(scope) = e.get_mut() {
                    scope.insert(t.clone());
                }
            }
        }
    }
    map
}

/// Parse `TENANT=KEY` for `--tenant-key`. The tenant half must be a
/// valid [`kotatsu::TenantKey`]; the key half must be non-empty.
/// Errors never echo the key half — it is a secret, and parse errors
/// end up in startup diagnostics and collected logs.
fn parse_tenant_key(s: &str) -> Result<(String, String), String> {
    let s = s.trim();
    let (tenant, key) = s
        .split_once('=')
        .ok_or_else(|| "tenant key must be TENANT=KEY".to_string())?;
    kotatsu::TenantKey::new(tenant)
        .map_err(|e| format!("invalid tenant {tenant:?} in tenant key: {e}"))?;
    let key = key.trim();
    if key.is_empty() {
        return Err(format!("tenant key for {tenant:?} has an empty key"));
    }
    Ok((tenant.to_owned(), key.to_owned()))
}

/// Parse `HH:MM-HH:MM=N` (UTC) into a [`kotatsu::WarmWindow`].
/// `24:00` is valid as the (exclusive) end only.
fn parse_warm_window(s: &str) -> Result<kotatsu::WarmWindow, String> {
    // Comma-delimited env values may carry a space after the comma.
    let s = s.trim();
    let (range, size) = s
        .split_once('=')
        .ok_or_else(|| format!("expected HH:MM-HH:MM=N, got {s:?}"))?;
    let (a, b) = range
        .split_once('-')
        .ok_or_else(|| format!("expected HH:MM-HH:MM range, got {range:?}"))?;
    let parse_t = |t: &str| -> Result<u16, String> {
        let (h, m) = t
            .split_once(':')
            .ok_or_else(|| format!("expected HH:MM, got {t:?}"))?;
        let h: u16 = h.parse().map_err(|_| format!("bad hour in {t:?}"))?;
        let m: u16 = m.parse().map_err(|_| format!("bad minute in {t:?}"))?;
        if h > 24 || m > 59 || (h == 24 && m > 0) {
            return Err(format!("time {t:?} out of range"));
        }
        Ok(h * 60 + m)
    };
    let (start, end) = (parse_t(a)?, parse_t(b)?);
    if start >= 1440 {
        return Err(format!("start {a:?} must be before 24:00"));
    }
    let size: usize = size.parse().map_err(|_| format!("bad size in {s:?}"))?;
    kotatsu::WarmWindow::new(start, end, size).map_err(|e| e.to_string())
}

/// SIGINT or SIGTERM — under systemd/K8s a rollout sends SIGTERM.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                tracing::warn!("SIGTERM handler unavailable ({e}); SIGINT still handled");
                std::future::pending::<()>().await
            }
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
    tracing::info!("shutdown requested — bindings persist; VMs keep running");
}

/// Default SQLite path — in-memory state on restart orphans every
/// running VM, which keeps billing up to the 8 h suspended cap.
fn default_state_db() -> Option<PathBuf> {
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".local/share")))?;
    Some(base.join("kotatsu").join("bindings.db"))
}

/// Effective configuration after CLI > file > default resolution.
struct Resolved {
    listen: SocketAddr,
    region: Option<String>,
    image: Option<String>,
    app_port: u16,
    warm_size: usize,
    warm_schedule: Vec<kotatsu::WarmWindow>,
    max_vms: usize,
    max_age: Option<Duration>,
    idle_suspend: Option<Duration>,
    suspended_ttl: Option<Duration>,
    wait_timeout: Option<Duration>,
    forwarded_proto: String,
    maintenance_interval: Duration,
    reap_lost_vms: bool,
    state_db: Option<PathBuf>,
    api_keys: Vec<String>,
    /// (tenant, key) pairs — the key only authorizes `/t/{tenant}`.
    tenant_keys: Vec<(String, String)>,
    allow_unauthenticated: bool,
    mock: bool,
}

impl Resolved {
    fn resolve(cli: &Cli, file: &FileConfig) -> anyhow::Result<Self> {
        let secs = |v: Option<u64>| v.map(Duration::from_secs);
        Ok(Self {
            listen: cli
                .listen
                .or(file.listen)
                .unwrap_or_else(|| "127.0.0.1:3000".parse().unwrap()),
            region: cli.region.clone().or_else(|| file.region.clone()),
            image: cli.image.clone().or_else(|| file.image.clone()),
            app_port: cli.app_port.or(file.app_port).unwrap_or(8080),
            warm_size: cli.warm_size.or(file.warm_size).unwrap_or(4),
            warm_schedule: if !cli.warm_schedule.is_empty() {
                cli.warm_schedule.clone()
            } else {
                file.warm_schedule
                    .iter()
                    .map(|s| parse_warm_window(s))
                    .collect::<Result<_, _>>()
                    .map_err(anyhow::Error::msg)?
            },
            max_vms: cli.max_vms.or(file.max_vms).unwrap_or(100),
            max_age: secs(cli.max_age_secs).or(file.max_age),
            idle_suspend: secs(cli.idle_suspend_secs).or(file.idle_suspend),
            suspended_ttl: secs(cli.suspended_ttl_secs).or(file.suspended_ttl),
            wait_timeout: secs(cli.wait_timeout_secs).or(file.wait_timeout),
            forwarded_proto: cli
                .forwarded_proto
                .clone()
                .or_else(|| file.forwarded_proto.clone())
                .unwrap_or_else(|| "http".into()),
            maintenance_interval: secs(cli.maintenance_secs)
                .or(file.maintenance_interval)
                .unwrap_or(Duration::from_secs(60)),
            // Tri-state — an explicit CLI/env `false` must be able to
            // retract a config-file `true` (CLI > file > default).
            reap_lost_vms: cli.reap_lost_vms.or(file.reap_lost_vms).unwrap_or(false),
            state_db: cli.state_db.clone().or_else(|| file.state_db.clone()),
            api_keys: if cli.api_keys.is_empty() {
                file.api_keys.iter().map(|k| k.trim().to_owned()).collect()
            } else {
                cli.api_keys.iter().map(|k| k.trim().to_owned()).collect()
            },
            tenant_keys: {
                let raw = if cli.tenant_keys.is_empty() {
                    &file.tenant_keys
                } else {
                    &cli.tenant_keys
                };
                raw.iter()
                    .map(|s| parse_tenant_key(s))
                    .collect::<Result<_, _>>()
                    .map_err(anyhow::Error::msg)?
            },
            allow_unauthenticated: cli
                .allow_unauthenticated
                .unwrap_or(file.allow_unauthenticated),
            mock: cli.mock,
        })
    }

    /// The SQLite file for bindings, `None` for in-memory state. Mock
    /// mode never falls back to the default file: the mock control
    /// plane reports every real MicroVM as gone, so its maintenance
    /// would release the bindings of a real deployment sharing it.
    fn state_db_path(&self) -> Option<PathBuf> {
        match &self.state_db {
            Some(path) => Some(path.clone()),
            None if self.mock => None,
            None => default_state_db(),
        }
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.image.is_none() && !self.mock {
            anyhow::bail!("--image (or KOTATSU_IMAGE) is required unless --mock is set");
        }
        if self.api_keys.is_empty() && self.tenant_keys.is_empty() && !self.allow_unauthenticated {
            anyhow::bail!(
                "no API keys configured — pass --api-key/--tenant-key/--config, or \
                 --allow-unauthenticated for local development"
            );
        }
        if self.api_keys.iter().any(|k| k.trim().is_empty()) {
            anyhow::bail!("empty API key is not allowed");
        }
        // Unauthenticated + public bind + real control plane = an open
        // relay to AWS billing. Loopback-only or mock-mode are the only
        // safe shapes for this flag.
        if self.allow_unauthenticated && !self.listen.ip().is_loopback() && !self.mock {
            anyhow::bail!(
                "refusing --allow-unauthenticated with a non-loopback --listen: \
                 all tenants would be reachable without credentials"
            );
        }
        if self.app_port == 0 {
            anyhow::bail!("--app-port must be non-zero");
        }
        let idle_max = u64::try_from(kotatsu::MAX_DURATION_SECONDS).unwrap();
        if let Some(d) = self.idle_suspend
            && !d.is_zero()
            && !(u64::try_from(kotatsu::MIN_IDLE_DURATION_SECONDS).unwrap()..=idle_max)
                .contains(&d.as_secs())
        {
            anyhow::bail!("--idle-suspend-secs must be 0 (off) or 60-28800 (AWS minimum is 60s)");
        }
        if let Some(d) = self.suspended_ttl
            && d.as_secs() > idle_max
        {
            anyhow::bail!("--suspended-ttl-secs must be 0-28800");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parses `args` alone: `KOTATSU_*` and `AWS_REGION` in the
    /// environment running the tests must not change the result.
    fn parse_cli(args: &[&str]) -> Cli {
        use clap::{CommandFactory, FromArgMatches};
        let matches = Cli::command()
            .mut_args(|a| a.env(None::<&'static str>))
            .try_get_matches_from(args)
            .unwrap();
        Cli::from_arg_matches(&matches).unwrap()
    }

    #[test]
    fn warm_window_parses_hhmm_range() {
        let w = parse_warm_window("09:00-18:00=8").unwrap();
        assert_eq!(
            w,
            kotatsu::WarmWindow {
                start_min: 540,
                end_min: 1080,
                size: 8
            }
        );
        // Wrap-midnight and 24:00 end both parse.
        assert_eq!(parse_warm_window("22:00-06:00=4").unwrap().end_min, 360);
        assert_eq!(parse_warm_window("09:00-24:00=8").unwrap().end_min, 1440);
        assert_eq!(parse_warm_window("00:00-00:00=2").unwrap().start_min, 0);
    }

    #[test]
    fn warm_window_rejects_malformed() {
        for bad in [
            "",
            "9-18=2",
            "09:00=2",
            "09:00-18:00",
            "09:00-18:00=x",
            "25:00-26:00=1",
            "09:60-18:00=1",
            "09:00-25:00=1",
            "09:00-18:00=",
            "=8",
            "24:00-06:00=1", // start must be < 24:00
        ] {
            assert!(parse_warm_window(bad).is_err(), "{bad:?} should fail");
        }
        // start=24:00 gets a clearer message than the raw minutes form.
        assert!(
            parse_warm_window("24:00-06:00=1")
                .unwrap_err()
                .contains("24:00")
        );
        // Comma-delimited env values may carry a space after the comma.
        assert!(parse_warm_window(" 09:00-18:00=8").is_ok());
    }

    #[test]
    fn tenant_key_parses_and_rejects() {
        assert_eq!(
            parse_tenant_key("alice=s3cret").unwrap(),
            ("alice".to_owned(), "s3cret".to_owned())
        );
        // Keys may contain '='; the split is on the first one.
        assert_eq!(parse_tenant_key("t1=k=v=2").unwrap().1, "k=v=2".to_owned());
        for bad in ["", "nokey", "t=", "bad tenant=k", "t =k"] {
            assert!(parse_tenant_key(bad).is_err(), "{bad:?} should fail");
        }
    }

    #[test]
    fn reap_lost_vms_tri_state_resolution() {
        // Bare flag → Some(true); explicit false is expressible; unset → None.
        let cli = parse_cli(&["kotatsud", "--reap-lost-vms"]);
        assert_eq!(cli.reap_lost_vms, Some(true));
        let cli = parse_cli(&["kotatsud", "--reap-lost-vms=false"]);
        assert_eq!(cli.reap_lost_vms, Some(false));
        let cli = parse_cli(&["kotatsud"]);
        assert_eq!(cli.reap_lost_vms, None);

        // CLI > file: an explicit false retracts a config-file true —
        // a plain OR-merge could never turn the opt-in back off.
        let file_true = FileConfig {
            reap_lost_vms: Some(true),
            ..Default::default()
        };
        let cli = parse_cli(&["kotatsud", "--reap-lost-vms=false"]);
        assert!(!Resolved::resolve(&cli, &file_true).unwrap().reap_lost_vms);
        let cli = parse_cli(&["kotatsud"]);
        assert!(Resolved::resolve(&cli, &file_true).unwrap().reap_lost_vms);
        // Default when nothing sets it stays off.
        let cli = parse_cli(&["kotatsud"]);
        assert!(
            !Resolved::resolve(&cli, &FileConfig::default())
                .unwrap()
                .reap_lost_vms
        );
    }

    #[test]
    fn allow_unauthenticated_flag_overrides_the_file() {
        let allow = |args: &[&str], file: bool| {
            let file = FileConfig {
                allow_unauthenticated: file,
                ..Default::default()
            };
            Resolved::resolve(&parse_cli(args), &file)
                .unwrap()
                .allow_unauthenticated
        };
        assert!(!allow(&["kotatsud", "--allow-unauthenticated=false"], true));
        assert!(allow(&["kotatsud"], true));
        assert!(allow(&["kotatsud", "--allow-unauthenticated"], false));
        assert!(!allow(&["kotatsud"], false));
    }

    #[test]
    fn help_states_the_defaults() {
        use clap::CommandFactory;
        let cfg = Resolved::resolve(&parse_cli(&["kotatsud"]), &FileConfig::default()).unwrap();
        let secs = |d: Option<Duration>| d.map_or(0, |d| d.as_secs()).to_string();
        let cmd = Cli::command();
        for (id, default) in [
            ("listen", cfg.listen.to_string()),
            ("app_port", cfg.app_port.to_string()),
            ("warm_size", cfg.warm_size.to_string()),
            ("max_vms", cfg.max_vms.to_string()),
            ("max_age_secs", secs(cfg.max_age)),
            ("idle_suspend_secs", secs(cfg.idle_suspend)),
            (
                "suspended_ttl_secs",
                kotatsu::MAX_DURATION_SECONDS.to_string(),
            ),
            (
                "wait_timeout_secs",
                kotatsu::WaitPolicy::default().timeout.as_secs().to_string(),
            ),
            ("forwarded_proto", cfg.forwarded_proto.clone()),
            ("maintenance_secs", secs(Some(cfg.maintenance_interval))),
        ] {
            let arg = cmd.get_arguments().find(|a| a.get_id() == id).unwrap();
            let help = arg.get_help().unwrap().to_string();
            assert!(
                help.contains(&format!("[default: {default}]")),
                "{id}: {help}"
            );
        }
        // Unset, these two fall back to the values checked above.
        assert_eq!(cfg.suspended_ttl, None);
        assert_eq!(cfg.wait_timeout, None);
    }

    #[tokio::test]
    async fn shutdown_closes_streams_after_the_grace_period() {
        let app = axum::Router::new().route(
            "/stream",
            axum::routing::get(|| async {
                axum::body::Body::from_stream(futures_util::stream::pending::<
                    Result<Vec<u8>, std::io::Error>,
                >())
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(serve_until(
            listener,
            app,
            async {
                let _ = stopped.await;
            },
            Duration::from_millis(100),
        ));
        let resp = reqwest::get(format!("http://{addr}/stream")).await.unwrap();
        assert_eq!(resp.status(), 200);

        stop.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), server)
            .await
            .expect("shutdown waited for the open stream")
            .unwrap()
            .unwrap();
        drop(resp);
    }

    #[test]
    fn mock_mode_never_defaults_to_the_bindings_file() {
        // The mock control plane reports real VMs as gone, so sharing
        // the default file would drop a real deployment's bindings.
        let state_db = |args: &[&str]| {
            let cli = parse_cli(args);
            Resolved::resolve(&cli, &FileConfig::default())
                .unwrap()
                .state_db_path()
        };
        assert_eq!(state_db(&["kotatsud", "--mock"]), None);
        assert_eq!(state_db(&["kotatsud"]), default_state_db());
        // An explicit path still wins in mock mode.
        assert_eq!(
            state_db(&["kotatsud", "--mock", "--state-db", "mock.db"]),
            Some(PathBuf::from("mock.db"))
        );
    }

    #[test]
    fn readme_config_example_is_accepted() {
        // Unknown keys fail startup, so a renamed field must not leave
        // the documented example behind.
        let toml = include_str!("../README.md")
            .split("```toml\n")
            .nth(1)
            .and_then(|rest| rest.split("```").next())
            .expect("README has a toml block");
        let file = parse_config(toml).unwrap();
        let cli = parse_cli(&["kotatsud"]);
        let cfg = Resolved::resolve(&cli, &file).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.idle_suspend, Some(Duration::from_secs(300)));
        assert_eq!(cfg.wait_timeout, Some(Duration::from_secs(30)));
        assert_eq!(cfg.warm_schedule.len(), 2);
        assert_eq!(cfg.tenant_keys.len(), 1);
    }

    #[test]
    fn tenant_key_errors_never_leak_the_key() {
        // The key half is a secret; parse errors surface in startup
        // diagnostics and collected logs, so it must not be echoed.
        for bad in ["bad tenant=s3cret", "t =s3cret", "s3cret-no-equals", "t="] {
            let err = parse_tenant_key(bad).unwrap_err();
            assert!(!err.contains("s3cret"), "key leaked in error: {err}");
        }
    }

    #[test]
    fn config_file_values_resolve() {
        let file = parse_config(
            r#"
            max_age = "1h"
            maintenance_interval = "30s"
            suspended_ttl = "0s"
            reap_lost_vms = true
            api_keys = [" k1 "]
            tenant_keys = ["alice=k2"]
            "#,
        )
        .unwrap();
        let cli = parse_cli(&["kotatsud"]);
        let cfg = Resolved::resolve(&cli, &file).unwrap();
        assert_eq!(cfg.max_age, Some(Duration::from_secs(3600)));
        assert_eq!(cfg.maintenance_interval, Duration::from_secs(30));
        assert_eq!(cfg.suspended_ttl, Some(Duration::ZERO));
        assert!(cfg.reap_lost_vms);
        assert_eq!(cfg.api_keys, ["k1"]);
        assert_eq!(cfg.tenant_keys, [("alice".to_owned(), "k2".to_owned())]);

        // A flag wins over the file, and `--api-key` replaces only the
        // `api_keys` list.
        let cli = parse_cli(&["kotatsud", "--api-key", "cli", "--maintenance-secs", "5"]);
        let cfg = Resolved::resolve(&cli, &file).unwrap();
        assert_eq!(cfg.api_keys, ["cli"]);
        assert_eq!(cfg.tenant_keys.len(), 1);
        assert_eq!(cfg.maintenance_interval, Duration::from_secs(5));
    }

    #[test]
    fn config_file_rejects_bad_entries() {
        let err = parse_config("warm_sise = 4").err().unwrap();
        assert!(err.contains("unknown field `warm_sise`"), "{err}");
        // Durations are strings; the flags take the whole seconds.
        assert!(parse_config("max_age = 3600").is_err());
        let cli = parse_cli(&["kotatsud"]);
        for bad in [
            r#"warm_schedule = ["9-18=2"]"#,
            r#"tenant_keys = ["bad tenant=s3cret"]"#,
        ] {
            let file = parse_config(bad).unwrap();
            let err = Resolved::resolve(&cli, &file).err().unwrap().to_string();
            assert!(!err.contains("s3cret"), "{bad:?} leaked: {err}");
        }
    }

    #[test]
    fn validate_rejects_each_invalid_setting() {
        let valid = || {
            let cli = parse_cli(&["kotatsud", "--image", "img", "--api-key", "k"]);
            Resolved::resolve(&cli, &FileConfig::default()).unwrap()
        };
        assert!(valid().validate().is_ok());

        type Change = fn(&mut Resolved);
        let rejected: &[(&str, Change)] = &[
            ("--image", |c| c.image = None),
            ("no API keys", |c| c.api_keys.clear()),
            ("empty API key", |c| c.api_keys.push(" ".into())),
            ("--app-port", |c| c.app_port = 0),
            ("--idle-suspend-secs", |c| {
                c.idle_suspend = Some(Duration::from_secs(59));
            }),
            ("--idle-suspend-secs", |c| {
                c.idle_suspend = Some(Duration::from_secs(28_801));
            }),
            ("--suspended-ttl-secs", |c| {
                c.suspended_ttl = Some(Duration::from_secs(28_801));
            }),
        ];
        for (i, (want, change)) in rejected.iter().enumerate() {
            let mut cfg = valid();
            change(&mut cfg);
            let err = cfg.validate().err().unwrap().to_string();
            assert!(err.contains(want), "case {i}: {err}");
        }

        let accepted: &[Change] = &[
            |c| {
                c.image = None;
                c.mock = true;
            },
            |c| {
                c.api_keys.clear();
                c.tenant_keys.push(("alice".into(), "k".into()));
            },
            // 0 turns idle suspend off; 60 and 28800 are the bounds.
            |c| c.idle_suspend = Some(Duration::ZERO),
            |c| c.idle_suspend = Some(Duration::from_secs(60)),
            |c| c.idle_suspend = Some(Duration::from_secs(28_800)),
            |c| c.suspended_ttl = Some(Duration::ZERO),
            |c| c.suspended_ttl = Some(Duration::from_secs(28_800)),
        ];
        for (i, change) in accepted.iter().enumerate() {
            let mut cfg = valid();
            change(&mut cfg);
            assert!(cfg.validate().is_ok(), "case {i}");
        }
    }

    #[test]
    fn config_errors_never_quote_the_file() {
        // The file holds API keys, and startup errors reach collected
        // logs: an error gives the position, never the line or value.
        for (bad, at) in [
            // Misspelled key (unknown field).
            ("api_key = \"s3cret\"", "line 1, column 1"),
            // A single string where an array is expected.
            ("api_keys = \"s3cret\"", "line 1, column 12"),
            (
                "listen = \"127.0.0.1:3000\"\ntenant_keys = \"alice=s3cret\"",
                "line 2, column 15",
            ),
            // A non-string element.
            ("api_keys = [\"s3cret\", 5]", "line 1, column 12"),
            // Syntax error: unterminated string.
            ("tenant_keys = [\"alice=s3cret]", "line 1, column"),
        ] {
            let err = parse_config(bad).err().unwrap();
            assert!(!err.contains("s3cret"), "{bad:?} leaked: {err}");
            assert!(err.starts_with(at), "{bad:?}: {err}");
        }
        let err = load_config(std::path::Path::new("/nonexistent/kotatsud.toml"))
            .err()
            .unwrap();
        assert!(err.to_string().contains("/nonexistent/kotatsud.toml"));
    }

    #[test]
    fn duplicate_keys_merge_scopes() {
        let keys = build_api_keys(
            &["global".to_owned()],
            &[
                ("u1".to_owned(), "scoped".to_owned()),
                ("u2".to_owned(), "scoped".to_owned()),
                // Same key declared global AND scoped: stays global.
                ("u1".to_owned(), "global".to_owned()),
            ],
        );
        assert_eq!(keys["global"], None);
        let scope = keys["scoped"].as_ref().unwrap();
        assert!(scope.contains("u1") && scope.contains("u2"));
    }

    #[test]
    fn unauthenticated_public_bind_is_refused() {
        let base = || Resolved {
            listen: "0.0.0.0:9000".parse().unwrap(),
            region: None,
            image: Some("img".into()),
            app_port: 8080,
            warm_size: 0,
            warm_schedule: vec![],
            max_vms: 10,
            max_age: None,
            idle_suspend: None,
            suspended_ttl: None,
            wait_timeout: None,
            forwarded_proto: "http".into(),
            maintenance_interval: Duration::from_secs(60),
            reap_lost_vms: false,
            state_db: None,
            api_keys: vec![],
            tenant_keys: vec![],
            allow_unauthenticated: true,
            mock: false,
        };
        // Public bind + no auth + real AWS = refused.
        assert!(base().validate().is_err());
        // Loopback stays allowed for local development.
        let mut loopback = base();
        loopback.listen = "127.0.0.1:3000".parse().unwrap();
        assert!(loopback.validate().is_ok());
        // Mock mode stays allowed for emulator runs.
        let mut mock = base();
        mock.mock = true;
        assert!(mock.validate().is_ok());
    }
}
