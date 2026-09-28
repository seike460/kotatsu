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

    /// Listen address for the gateway.
    #[arg(long, env = "KOTATSU_LISTEN")]
    listen: Option<SocketAddr>,

    /// AWS region for the lambda-microvms API.
    #[arg(long, env = "AWS_REGION")]
    region: Option<String>,

    /// MicroVM image ARN used for every pooled VM.
    #[arg(long, env = "KOTATSU_IMAGE")]
    image: Option<String>,

    /// Application port inside each MicroVM.
    #[arg(long, env = "KOTATSU_APP_PORT")]
    app_port: Option<u16>,

    /// Unassigned warm VMs kept ready.
    #[arg(long, env = "KOTATSU_WARM_SIZE")]
    warm_size: Option<usize>,

    /// UTC warm-size schedule `HH:MM-HH:MM=N` (repeatable; `24:00`
    /// allowed as end; `22:00-06:00` wraps midnight). The first
    /// matching window overrides `--warm-size` each maintenance tick.
    #[arg(long, env = "KOTATSU_WARM_SCHEDULE", value_parser = parse_warm_window, value_delimiter = ',')]
    warm_schedule: Vec<kotatsu::WarmWindow>,

    /// Hard cap on pool-managed VMs.
    #[arg(long, env = "KOTATSU_MAX_VMS")]
    max_vms: Option<usize>,

    /// Terminate VMs older than this (seconds; 0 = disable).
    #[arg(long, env = "KOTATSU_MAX_AGE_SECS")]
    max_age_secs: Option<u64>,

    /// Suspend a VM after this many idle seconds (0 = rely on the
    /// image's own idle policy).
    #[arg(long, env = "KOTATSU_IDLE_SUSPEND_SECS")]
    idle_suspend_secs: Option<u64>,

    /// Terminate a suspended VM after this many seconds (max 28800).
    #[arg(long, env = "KOTATSU_SUSPENDED_TTL_SECS")]
    suspended_ttl_secs: Option<u64>,

    /// Max seconds a request waits for a suspended VM to resume.
    #[arg(long, env = "KOTATSU_WAIT_TIMEOUT_SECS")]
    wait_timeout_secs: Option<u64>,

    /// Value for `x-forwarded-proto` upstream (set `https` behind a
    /// TLS-terminating load balancer).
    #[arg(long, env = "KOTATSU_FORWARDED_PROTO")]
    forwarded_proto: Option<String>,

    /// With `--mock`: endpoint override for every mock VM — a local URL
    /// like `http://127.0.0.1:PORT` (enables insecure endpoints).
    #[arg(long, env = "KOTATSU_MOCK_ENDPOINT", requires = "mock")]
    mock_endpoint: Option<String>,

    /// Maintenance tick in seconds.
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
    /// `$XDG_DATA_HOME/kotatsu/bindings.db` (or `~/.local/share/…`).
    /// In-memory state orphans running VMs on restart (bills to 8h).
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
    #[arg(long, env = "KOTATSU_ALLOW_UNAUTHENTICATED")]
    allow_unauthenticated: bool,

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
    #[serde(default)]
    api_keys: Vec<String>,
    #[serde(default)]
    tenant_keys: Vec<String>,
    #[serde(default)]
    allow_unauthenticated: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kotatsud=info,kotatsu=info,tower_http=info".into()),
        )
        .init();

    let cli = Cli::parse();
    let file: FileConfig = match &cli.config {
        Some(p) => toml::from_str(&std::fs::read_to_string(p)?)?,
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
    let store: Arc<dyn StateStore> = match cfg.state_db.clone().or_else(default_state_db) {
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
    if cli.mock_endpoint.is_some() {
        pool_cfg.allow_insecure_endpoints = mock_endpoint_is_http;
    }
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
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;
    Ok(())
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
            allow_unauthenticated: cli.allow_unauthenticated || file.allow_unauthenticated,
            mock: cli.mock,
        })
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
        use clap::Parser;
        // Bare flag → Some(true); explicit false is expressible; unset → None.
        let cli = Cli::try_parse_from(["kotatsud", "--reap-lost-vms"]).unwrap();
        assert_eq!(cli.reap_lost_vms, Some(true));
        let cli = Cli::try_parse_from(["kotatsud", "--reap-lost-vms=false"]).unwrap();
        assert_eq!(cli.reap_lost_vms, Some(false));
        let cli = Cli::try_parse_from(["kotatsud"]).unwrap();
        assert_eq!(cli.reap_lost_vms, None);

        // CLI > file: an explicit false retracts a config-file true —
        // a plain OR-merge could never turn the opt-in back off.
        let file_true = FileConfig {
            reap_lost_vms: Some(true),
            ..Default::default()
        };
        let cli = Cli::try_parse_from(["kotatsud", "--reap-lost-vms=false"]).unwrap();
        assert!(!Resolved::resolve(&cli, &file_true).unwrap().reap_lost_vms);
        let cli = Cli::try_parse_from(["kotatsud"]).unwrap();
        assert!(Resolved::resolve(&cli, &file_true).unwrap().reap_lost_vms);
        // Default when nothing sets it stays off.
        let cli = Cli::try_parse_from(["kotatsud"]).unwrap();
        assert!(
            !Resolved::resolve(&cli, &FileConfig::default())
                .unwrap()
                .reap_lost_vms
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
        let file: FileConfig = toml::from_str(toml).unwrap();
        let cli = Cli::try_parse_from(["kotatsud"]).unwrap();
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
