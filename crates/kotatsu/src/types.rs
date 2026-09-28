//! Core domain types shared across kotatsu.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::{Duration, Instant};

use crate::error::{Error, Result};

// ---- Service contract constants (docs.aws.amazon.com/lambda/latest/dg) ----

/// HTTP header carrying the JWE auth token to a MicroVM endpoint.
pub const AUTH_HEADER: &str = "X-aws-proxy-auth";
/// HTTP header selecting the target port inside a MicroVM.
pub const PORT_HEADER: &str = "X-aws-proxy-port";
/// Required base WebSocket subprotocol.
pub const WS_BASE_PROTOCOL: &str = "lambda-microvms";
/// WebSocket subprotocol prefix carrying the auth token.
pub const WS_AUTH_PROTOCOL_PREFIX: &str = "lambda-microvms.authentication.";
/// WebSocket subprotocol prefix carrying the target port.
pub const WS_PORT_PROTOCOL_PREFIX: &str = "lambda-microvms.port.";
/// Path prefix the platform uses when calling lifecycle hooks.
pub const HOOK_PATH_PREFIX: &str = "/aws/lambda-microvms/runtime/v1";
/// Default port traffic is routed to inside a MicroVM.
pub const DEFAULT_APP_PORT: u16 = 8080;
/// Maximum lifetime of a MicroVM, in seconds (8 hours).
pub const MAX_DURATION_SECONDS: i32 = 28_800;
/// AWS-enforced minimum for `idlePolicy.maxIdleDurationSeconds`.
pub const MIN_IDLE_DURATION_SECONDS: i32 = 60;
/// Maximum size of the `/run` hook payload, in bytes.
pub const MAX_RUN_HOOK_PAYLOAD_BYTES: usize = 16_384;
/// Maximum auth token TTL accepted by `create-microvm-auth-token`, in minutes.
pub const MAX_TOKEN_TTL_MINUTES: i32 = 60;

/// Reserved tenant-key prefix for sentinel bindings — durable store
/// records marking a VM whose ownership could not be resolved.
/// [`crate::SandboxPool::maintain`] destroys sentinel-bound VMs on
/// sight and clears the marker, so a VM survives tracking even across
/// a process restart. Real tenant keys must never use this prefix —
/// [`TenantKey::new`] rejects it.
pub(crate) const LOST_TENANT_PREFIX: &str = "~kotatsu-lost~";

/// An opaque, validated tenant identifier used for session affinity.
///
/// Tenants are the unit of routing: each tenant key maps to at most one
/// MicroVM at a time inside a `SandboxPool`.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct TenantKey(pub(crate) String);

/// Deserialization goes through [`TenantKey::new`] — the transparent
/// derive would otherwise admit keys that bypass charset, length, and
/// reserved-prefix validation.
impl<'de> Deserialize<'de> for TenantKey {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        Self::new(s).map_err(serde::de::Error::custom)
    }
}

impl TenantKey {
    /// Creates a tenant key. Keys must be 1–128 chars of
    /// `[A-Za-z0-9._~-]` so they are safe to embed in URLs, and must
    /// not collide with the pool's reserved sentinel namespace.
    pub fn new(key: impl Into<String>) -> Result<Self> {
        let key = key.into();
        if key.is_empty() || key.len() > 128 {
            return Err(Error::invalid("tenant key must be 1-128 characters"));
        }
        if !key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '~' | '-'))
        {
            return Err(Error::invalid("tenant key must match [A-Za-z0-9._~-]+"));
        }
        if key.starts_with(LOST_TENANT_PREFIX) {
            return Err(Error::invalid("tenant key uses a reserved prefix"));
        }
        Ok(Self(key))
    }

    /// The sentinel key for `id` — a store marker tracking a VM whose
    /// ownership could not be resolved. Internal only; the prefix is
    /// rejected by [`TenantKey::new`] so user tenants can never be
    /// mistaken for lost-VM markers (or vice versa).
    pub(crate) fn lost_vm(id: &MicrovmId) -> Self {
        Self(format!("{LOST_TENANT_PREFIX}{}", id.as_str()))
    }

    /// The raw string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TenantKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for TenantKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TenantKey({:?})", self.0)
    }
}

/// A MicroVM identifier (`microvm-...`), as returned by `run-microvm`.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MicrovmId(pub(crate) String);

impl MicrovmId {
    /// Wraps a raw id string; only an empty id is rejected.
    pub fn new(id: impl Into<String>) -> Result<Self> {
        let id = id.into();
        if id.is_empty() {
            return Err(Error::invalid("microvm id must not be empty"));
        }
        Ok(Self(id))
    }

    /// The raw id string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MicrovmId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for MicrovmId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MicrovmId({:?})", self.0)
    }
}

/// Lifecycle state of a MicroVM.
///
/// Mirrors the service-side state machine:
/// `PENDING → RUNNING → SUSPENDING → SUSPENDED → RUNNING → TERMINATING → TERMINATED`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum State {
    /// Being provisioned; snapshot is loading.
    Pending,
    /// Active and accepting traffic.
    Running,
    /// `/suspend` hook running; checkpointing.
    Suspending,
    /// State preserved; no compute charges.
    Suspended,
    /// `/terminate` hook running; releasing resources.
    Terminating,
    /// Terminal state.
    Terminated,
    /// A state this crate does not know (forward-compatible).
    Unknown(String),
}

impl State {
    /// Service-side state name.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Suspending => "SUSPENDING",
            Self::Suspended => "SUSPENDED",
            Self::Terminating => "TERMINATING",
            Self::Terminated => "TERMINATED",
            Self::Unknown(s) => s.as_str(),
        }
    }

    /// True while the MicroVM may still serve traffic (now or after resume).
    ///
    /// `Unknown` is deliberately **not** live: a future terminal-like state
    /// (e.g. a `FAILED` the SDK does not know yet) must not keep receiving
    /// tenant traffic. Anything not in this list is treated as gone.
    pub fn is_live(&self) -> bool {
        matches!(
            self,
            Self::Pending | Self::Running | Self::Suspending | Self::Suspended
        )
    }
}

impl fmt::Display for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<&aws_sdk_lambdamicrovms::types::MicrovmState> for State {
    fn from(s: &aws_sdk_lambdamicrovms::types::MicrovmState) -> Self {
        use aws_sdk_lambdamicrovms::types::MicrovmState as S;
        match s {
            S::Pending => Self::Pending,
            S::Running => Self::Running,
            S::Suspending => Self::Suspending,
            S::Suspended => Self::Suspended,
            S::Terminating => Self::Terminating,
            S::Terminated => Self::Terminated,
            other => Self::Unknown(other.as_str().to_owned()),
        }
    }
}

/// Which ports an auth token grants access to on a MicroVM.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PortSpec {
    /// A single port.
    Port(u16),
    /// An inclusive port range.
    Range {
        /// First port (inclusive).
        start: u16,
        /// Last port (inclusive).
        end: u16,
    },
    /// All ports.
    All,
}

impl PortSpec {
    /// A single port; returns `Err` for port 0.
    pub fn port(port: u16) -> Result<Self> {
        if port == 0 {
            return Err(Error::invalid("port 0 is not valid"));
        }
        Ok(Self::Port(port))
    }

    /// An inclusive range; returns `Err` for empty ranges.
    pub fn range(start: u16, end: u16) -> Result<Self> {
        if start == 0 || start > end {
            return Err(Error::invalid(format!("bad port range {start}-{end}")));
        }
        Ok(Self::Range { start, end })
    }

    /// True if the spec is internally consistent (port > 0, start ≤ end).
    pub fn is_valid(&self) -> bool {
        match self {
            Self::Port(p) => *p > 0,
            Self::Range { start, end } => *start > 0 && start <= end,
            Self::All => true,
        }
    }

    /// Parses `"8080"`, `"9000-9010"` or `"all"`.
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        if s.eq_ignore_ascii_case("all") || s == "*" {
            return Ok(Self::All);
        }
        if let Some((a, b)) = s.split_once('-') {
            let start: u16 = a
                .trim()
                .parse()
                .map_err(|_| Error::invalid(format!("bad port range {s:?}")))?;
            let end: u16 = b
                .trim()
                .parse()
                .map_err(|_| Error::invalid(format!("bad port range {s:?}")))?;
            return Self::range(start, end);
        }
        let port: u16 = s
            .parse()
            .map_err(|_| Error::invalid(format!("bad port {s:?}")))?;
        Self::port(port)
    }

    /// Converts to the SDK union type.
    pub(crate) fn to_sdk(&self) -> aws_sdk_lambdamicrovms::types::PortSpecification {
        use aws_sdk_lambdamicrovms::types::{PortRange, PortSpecification};
        match self {
            Self::Port(p) => PortSpecification::Port(i32::from(*p)),
            Self::Range { start, end } => PortSpecification::Range(
                PortRange::builder()
                    .start_port(i32::from(*start))
                    .end_port(i32::from(*end))
                    .build()
                    .expect("PortRange requires start_port and end_port"),
            ),
            Self::All => PortSpecification::AllPorts,
        }
    }

    /// True if this spec covers `port`.
    pub fn covers(&self, port: u16) -> bool {
        match self {
            Self::Port(p) => *p == port,
            Self::Range { start, end } => *start <= port && port <= *end,
            Self::All => true,
        }
    }
}

impl fmt::Display for PortSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Port(p) => write!(f, "{p}"),
            Self::Range { start, end } => write!(f, "{start}-{end}"),
            Self::All => write!(f, "all"),
        }
    }
}

/// Distinguishes data-plane port tokens from shell tokens.
///
/// Shell tokens (`create-microvm-shell-auth-token`) are not port-scoped and
/// must never be treated as application-traffic tokens.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TokenKind {
    /// Token minted by `create-microvm-auth-token` for application traffic.
    Port,
    /// Token minted by `create-microvm-shell-auth-token` for shell access.
    Shell,
}

/// A JWE auth token for talking to a MicroVM endpoint.
///
/// The token value is treated as a secret: it is never emitted through
/// [`fmt::Debug`] or tracing. AWS returns only the token string, so the
/// expiry is computed locally from the requested TTL.
#[derive(Clone)]
pub struct AuthToken {
    /// `X-aws-proxy-auth` header value.
    pub(crate) value: String,
    /// When the token was minted (monotonic).
    pub(crate) issued_at: Instant,
    /// Requested TTL.
    pub(crate) ttl: Duration,
    /// Port scope the token was minted with.
    pub(crate) scope: Vec<PortSpec>,
    /// Which API minted this token.
    pub(crate) kind: TokenKind,
}

impl AuthToken {
    /// The header value to send as `X-aws-proxy-auth`.
    pub fn header_value(&self) -> &str {
        &self.value
    }

    /// The TTL the token was minted with.
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// True if fewer than `margin` remains before expiry.
    pub fn nearly_expired(&self, margin: Duration) -> bool {
        self.issued_at.elapsed() + margin >= self.ttl
    }

    /// Which API minted this token.
    pub fn kind(&self) -> TokenKind {
        self.kind
    }

    /// True if the token's scope allows `port`.
    ///
    /// Always `false` for [`TokenKind::Shell`] tokens: they are not
    /// data-plane credentials and must not be forwarded as such.
    pub fn covers_port(&self, port: u16) -> bool {
        self.kind == TokenKind::Port && self.scope.iter().any(|s| s.covers(port))
    }

    /// The scope the token was minted with.
    pub fn scope(&self) -> &[PortSpec] {
        &self.scope
    }
}

impl fmt::Debug for AuthToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthToken")
            .field("value", &"<redacted>")
            .field("ttl", &self.ttl)
            .field("scope", &self.scope)
            .finish()
    }
}

/// A point-in-time description of a MicroVM.
#[derive(Clone, Debug)]
pub struct Microvm {
    /// Unique MicroVM id.
    pub id: MicrovmId,
    /// Current lifecycle state.
    pub state: State,
    /// Dedicated HTTPS endpoint URL.
    pub endpoint: String,
    /// Image ARN the MicroVM was run from.
    pub image_arn: String,
    /// Image version (`major.minor`).
    pub image_version: String,
    /// Execution role ARN, if set.
    pub execution_role_arn: Option<String>,
    /// Configured maximum lifetime.
    pub maximum_duration_seconds: i32,
    /// Start timestamp (epoch seconds).
    pub started_at_secs: Option<i64>,
    /// Termination timestamp (epoch seconds).
    pub terminated_at_secs: Option<i64>,
    /// Service-provided reason for the current state.
    pub state_reason: Option<String>,
    /// Ingress network connectors attached at run time.
    pub ingress_connectors: Vec<String>,
    /// Egress network connectors attached at run time.
    pub egress_connectors: Vec<String>,
}

impl Microvm {
    /// True while the MicroVM can still serve traffic (now or after resume).
    pub fn is_live(&self) -> bool {
        self.state.is_live()
    }
}

/// A [`Microvm`] verified to be in `RUNNING` state at construction time.
///
/// `MicrovmEndpoint` and `SandboxPool` hand these out instead of bare
/// `Microvm` values so "traffic only goes to running VMs" is enforced by
/// the type system rather than by convention. The guarantee decays over
/// time (the VM can suspend or terminate asynchronously), but callers
/// holding a `RunningVm` obtained it through `wait_until_running` or an
/// equivalent check and can treat staleness as an error path.
#[derive(Clone, Debug)]
pub struct RunningVm(Microvm);

impl RunningVm {
    /// Wraps `vm` iff its state is currently `RUNNING`.
    pub fn try_from_vm(vm: Microvm) -> Result<Self> {
        if vm.state != State::Running {
            return Err(Error::UnexpectedState {
                id: vm.id.to_string(),
                expected: "RUNNING".into(),
                got: vm.state.to_string(),
            });
        }
        Ok(Self(vm))
    }

    /// The wrapped MicroVM description.
    pub fn microvm(&self) -> &Microvm {
        &self.0
    }

    /// The MicroVM identifier.
    pub fn id(&self) -> &MicrovmId {
        &self.0.id
    }

    /// The MicroVM's dedicated HTTPS endpoint URL.
    pub fn endpoint(&self) -> &str {
        &self.0.endpoint
    }

    /// Releases the wrapped `Microvm` (e.g. for pool bookkeeping).
    pub fn into_microvm(self) -> Microvm {
        self.0
    }
}

/// Summary entry from `list-microvms` (no endpoint field).
#[derive(Clone, Debug)]
pub struct MicrovmSummary {
    /// MicroVM id.
    pub id: MicrovmId,
    /// Lifecycle state.
    pub state: State,
    /// Image ARN.
    pub image_arn: String,
    /// Image version.
    pub image_version: String,
    /// Start timestamp (epoch seconds).
    pub started_at_secs: Option<i64>,
}

/// Idle policy configuration for `run-microvm`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdlePolicyConfig {
    /// Resume automatically when traffic arrives while suspended.
    pub auto_resume_enabled: bool,
    /// Suspend after this many seconds without inbound traffic
    /// (60–28,800 per the `lambda-microvms` API contract).
    pub max_idle_duration_seconds: i32,
    /// Terminate after this many seconds in the suspended state
    /// (0–28,800 — `0` terminates immediately on suspend).
    pub suspended_duration_seconds: i32,
}

impl IdlePolicyConfig {
    /// Validate against the documented limits.
    pub fn validate(&self) -> Result<()> {
        if !(MIN_IDLE_DURATION_SECONDS..=MAX_DURATION_SECONDS)
            .contains(&self.max_idle_duration_seconds)
        {
            return Err(Error::invalid(format!(
                "max_idle_duration_seconds must be {MIN_IDLE_DURATION_SECONDS}-{MAX_DURATION_SECONDS}"
            )));
        }
        if !(0..=MAX_DURATION_SECONDS).contains(&self.suspended_duration_seconds) {
            return Err(Error::invalid(format!(
                "suspended_duration_seconds must be 0-{MAX_DURATION_SECONDS}"
            )));
        }
        Ok(())
    }

    pub(crate) fn to_sdk(&self) -> aws_sdk_lambdamicrovms::types::IdlePolicy {
        aws_sdk_lambdamicrovms::types::IdlePolicy::builder()
            .auto_resume_enabled(self.auto_resume_enabled)
            .max_idle_duration_seconds(self.max_idle_duration_seconds)
            .suspended_duration_seconds(self.suspended_duration_seconds)
            .build()
            .expect("IdlePolicy requires all fields")
    }
}

/// Parameters for `run-microvm`.
#[derive(Clone, Debug, Default)]
pub struct RunRequest {
    /// MicroVM image ARN (required).
    pub image_identifier: String,
    /// `major.minor` image version; latest ACTIVE when omitted.
    pub image_version: Option<String>,
    /// IAM execution role assumed inside the MicroVM.
    pub execution_role_arn: Option<String>,
    /// Auto suspend/resume behaviour.
    pub idle_policy: Option<IdlePolicyConfig>,
    /// Ingress connector ARNs.
    pub ingress_connectors: Vec<String>,
    /// Egress connector ARNs.
    pub egress_connectors: Vec<String>,
    /// Hard lifetime cap (1–28,800 s).
    pub maximum_duration_seconds: Option<i32>,
    /// Payload delivered to the `/run` hook (≤16 KiB).
    pub run_hook_payload: Option<String>,
    /// Idempotency token.
    pub client_token: Option<String>,
}

impl RunRequest {
    /// Starts a request for `image_identifier`.
    pub fn new(image_identifier: impl Into<String>) -> Self {
        Self {
            image_identifier: image_identifier.into(),
            ..Default::default()
        }
    }

    /// Validate against the documented service limits.
    pub fn validate(&self) -> Result<()> {
        if self.image_identifier.is_empty() {
            return Err(Error::invalid("image_identifier is required"));
        }
        if let Some(d) = self.maximum_duration_seconds
            && !(1..=MAX_DURATION_SECONDS).contains(&d)
        {
            return Err(Error::invalid(format!(
                "maximum_duration_seconds must be 1-{MAX_DURATION_SECONDS}"
            )));
        }
        if let Some(p) = &self.run_hook_payload
            && p.len() > MAX_RUN_HOOK_PAYLOAD_BYTES
        {
            return Err(Error::invalid(format!(
                "run_hook_payload exceeds {MAX_RUN_HOOK_PAYLOAD_BYTES} bytes"
            )));
        }
        if let Some(idle) = &self.idle_policy {
            idle.validate()?;
        }
        Ok(())
    }
}
