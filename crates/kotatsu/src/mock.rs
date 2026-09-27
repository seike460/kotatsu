//! In-memory [`ControlPlane`] implementation for tests and `kotatsu dev`.

use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use crate::types::{
    AuthToken, Microvm, MicrovmId, MicrovmSummary, PortSpec, RunRequest, State, TokenKind,
};

use crate::control_plane::ControlPlane;

/// Behavior knobs for the fake service.
///
/// Zero-duration knobs mean the corresponding transition completes
/// immediately (skipping the transitional state), matching tests that do not
/// care about `SUSPENDING`/`TERMINATING`/`PENDING`. Set them when callers
/// must observe the intermediate states the real service emits.
#[derive(Clone, Debug, Default)]
pub struct MockBehavior {
    /// How long a VM stays PENDING before becoming RUNNING.
    pub boot_time: Duration,
    /// How long resume takes from SUSPENDED.
    pub resume_time: Duration,
    /// How long suspend takes (RUNNING → SUSPENDING → SUSPENDED).
    pub suspend_time: Duration,
    /// How long terminate takes (* → TERMINATING → TERMINATED).
    pub terminate_time: Duration,
    /// If set, `run` fails with this message on every call while set.
    pub run_error: Option<String>,
    /// If set, `run` blocks on this notification before creating the VM —
    /// for cancellation-safety tests that need `run` genuinely in flight.
    pub run_gate: Option<Arc<tokio::sync::Notify>>,
    /// If set, `terminate` fails with this message on every call while set.
    pub terminate_error: Option<String>,
    /// If set, `get` blocks on this notification before responding —
    /// for tests that bound the wait budget against a stalled control
    /// plane. The gate is not consumed: every `get` waits on it.
    pub get_gate: Option<Arc<tokio::sync::Notify>>,
    /// If set, `resume` blocks on this notification before the lookup.
    pub resume_gate: Option<Arc<tokio::sync::Notify>>,
    /// If set, `resume` fails with this permanent (non-transient) error
    /// on every call while set.
    pub resume_error: Option<String>,
    /// If set, the first N `resume` calls fail with a `Conflict` —
    /// the real service's `ConflictException` while a transition is
    /// already in flight — before the real lookup runs.
    pub resume_transient_failures: Option<Arc<AtomicU32>>,
}

#[derive(Debug)]
struct MockVm {
    vm: Microvm,
    /// When the current pending transition completes.
    transition_at: Option<Instant>,
    /// Target state of the pending transition.
    transition_to: Option<State>,
}

impl MockVm {
    /// Clears any pending transition. Required whenever code sets `vm.state`
    /// directly — a stale transition would otherwise resurrect the VM later.
    fn clear_transition(&mut self) {
        self.transition_at = None;
        self.transition_to = None;
    }

    /// Arms a deferred transition to `to` after `after`, or applies it
    /// immediately when `after` is zero.
    fn transition(&mut self, to: State, after: Duration) {
        if after.is_zero() {
            self.clear_transition();
            self.vm.state = to;
        } else {
            self.transition_at = Some(Instant::now() + after);
            self.transition_to = Some(to);
        }
    }
}

/// In-memory `lambda-microvms` fake.
///
/// State transitions happen lazily: mutating calls arm a transition, and
/// `get`/`list` resolve it once the deadline passes — matching the real
/// service where `run-microvm` returns PENDING and `get-microvm` shows
/// progress.
///
/// Fabricated details not covered by the service contract: endpoint URLs are
/// built as `<id>.lambda-microvm.<region>.on.aws` (matching the documented
/// pattern) and `maximum_duration_seconds` defaults to 28,800. If the real
/// defaults differ, fix this fake — do not copy its assumptions into
/// production code.
#[derive(Clone)]
pub struct MockControlPlane {
    inner: Arc<Mutex<HashMap<String, MockVm>>>,
    counter: Arc<AtomicU64>,
    behavior: MockBehavior,
    region: String,
    endpoint_override: Option<String>,
}

impl MockControlPlane {
    /// A fake with default (instant) behavior.
    pub fn new() -> Self {
        Self::with_behavior(MockBehavior::default())
    }

    /// A fake with explicit timing behavior.
    pub fn with_behavior(behavior: MockBehavior) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            counter: Arc::new(AtomicU64::new(0)),
            behavior,
            region: "ap-northeast-1".into(),
            endpoint_override: None,
        }
    }

    /// Overrides the fake region used when fabricating endpoint URLs.
    pub fn region(mut self, region: &str) -> Self {
        self.region = region.to_owned();
        self
    }

    /// Uses `url` verbatim as every MicroVM's endpoint — lets gateway and
    /// endpoint tests point the pool at a real local upstream (with
    /// `PoolConfig::allow_insecure_endpoints` for `http://` URLs).
    pub fn endpoint_override(mut self, url: &str) -> Self {
        self.endpoint_override = Some(url.to_owned());
        self
    }

    /// Applies an armed transition whose deadline has passed.
    fn resolve(vm: &mut MockVm) {
        if let (Some(at), Some(to)) = (vm.transition_at, vm.transition_to.clone())
            && Instant::now() >= at
        {
            vm.vm.state = to;
            vm.clear_transition();
        }
    }

    fn lookup<'a>(
        guard: &'a mut HashMap<String, MockVm>,
        id: &MicrovmId,
    ) -> Result<&'a mut MockVm> {
        let entry = guard
            .get_mut(id.as_str())
            .ok_or_else(|| Error::NotFound { id: id.to_string() })?;
        Self::resolve(entry);
        Ok(entry)
    }
}

impl Default for MockControlPlane {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ControlPlane for MockControlPlane {
    async fn run(&self, req: &RunRequest) -> Result<Microvm> {
        req.validate()?;
        if let Some(msg) = &self.behavior.run_error {
            return Err(Error::Other(format!("run_microvm failed: {msg}")));
        }
        if let Some(gate) = &self.behavior.run_gate {
            gate.notified().await;
        }
        let n = self.counter.fetch_add(1, Ordering::SeqCst) + 1;
        let id = format!("microvm-{n:012}");
        let endpoint = self
            .endpoint_override
            .clone()
            .unwrap_or_else(|| format!("https://{id}.lambda-microvm.{}.on.aws", self.region));
        let mut entry = MockVm {
            vm: Microvm {
                id: MicrovmId(id.clone()),
                state: State::Pending,
                endpoint,
                image_arn: req.image_identifier.clone(),
                image_version: req
                    .image_version
                    .clone()
                    .unwrap_or_else(|| "1.0".to_string()),
                execution_role_arn: req.execution_role_arn.clone(),
                // Fabricated default; the service-side default is not in the
                // verified contract. See type docs above.
                maximum_duration_seconds: req
                    .maximum_duration_seconds
                    .unwrap_or(crate::MAX_DURATION_SECONDS),
                started_at_secs: Some(chrono::Utc::now().timestamp()),
                terminated_at_secs: None,
                state_reason: None,
                ingress_connectors: req.ingress_connectors.clone(),
                egress_connectors: req.egress_connectors.clone(),
            },
            transition_at: None,
            transition_to: None,
        };
        entry.transition(State::Running, self.behavior.boot_time);
        let vm = entry.vm.clone();
        self.inner.lock().insert(id, entry);
        Ok(vm)
    }

    async fn get(&self, id: &MicrovmId) -> Result<Microvm> {
        if let Some(gate) = &self.behavior.get_gate {
            gate.notified().await;
        }
        let mut guard = self.inner.lock();
        let entry = Self::lookup(&mut guard, id)?;
        Ok(entry.vm.clone())
    }

    async fn suspend(&self, id: &MicrovmId) -> Result<()> {
        let mut guard = self.inner.lock();
        let entry = Self::lookup(&mut guard, id)?;
        if entry.vm.state != State::Running {
            return Err(Error::UnexpectedState {
                id: id.to_string(),
                expected: "RUNNING".into(),
                got: entry.vm.state.to_string(),
            });
        }
        if self.behavior.suspend_time.is_zero() {
            entry.transition(State::Suspended, Duration::ZERO);
        } else {
            entry.vm.state = State::Suspending;
            entry.transition(State::Suspended, self.behavior.suspend_time);
        }
        Ok(())
    }

    async fn resume(&self, id: &MicrovmId) -> Result<()> {
        if let Some(gate) = &self.behavior.resume_gate {
            gate.notified().await;
        }
        if let Some(msg) = &self.behavior.resume_error {
            return Err(Error::Other(format!("resume_microvm failed: {msg}")));
        }
        if let Some(n) = &self.behavior.resume_transient_failures
            && n.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| x.checked_sub(1))
                .is_ok()
        {
            return Err(Error::Conflict {
                op: "resume_microvm",
                source: Box::new(std::io::Error::other(
                    "ConflictException: transition already in progress",
                )),
            });
        }
        let mut guard = self.inner.lock();
        let entry = Self::lookup(&mut guard, id)?;
        if entry.vm.state != State::Suspended {
            return Err(Error::UnexpectedState {
                id: id.to_string(),
                expected: "SUSPENDED".into(),
                got: entry.vm.state.to_string(),
            });
        }
        // Idempotent while a resume is already in flight: re-arming the
        // transition here would push the completion deadline out on every
        // duplicate call and livelock callers that re-issue `resume`.
        if entry.transition_to == Some(State::Running) {
            return Ok(());
        }
        entry.transition(State::Running, self.behavior.resume_time);
        Ok(())
    }

    async fn terminate(&self, id: &MicrovmId) -> Result<()> {
        if let Some(msg) = &self.behavior.terminate_error {
            return Err(Error::Other(msg.clone()));
        }
        let mut guard = self.inner.lock();
        let entry = Self::lookup(&mut guard, id)?;
        if !entry.vm.state.is_live() {
            return Err(Error::Terminated(id.to_string()));
        }
        entry.clear_transition();
        if self.behavior.terminate_time.is_zero() {
            entry.vm.state = State::Terminated;
            entry.vm.terminated_at_secs = Some(chrono::Utc::now().timestamp());
        } else {
            entry.vm.state = State::Terminating;
            entry.transition(State::Terminated, self.behavior.terminate_time);
        }
        Ok(())
    }

    async fn list(
        &self,
        image_identifier: Option<&str>,
        image_version: Option<&str>,
    ) -> Result<Vec<MicrovmSummary>> {
        let mut guard = self.inner.lock();
        let mut out = Vec::new();
        for entry in guard.values_mut() {
            Self::resolve(entry);
            let vm = &entry.vm;
            if let Some(img) = image_identifier
                && vm.image_arn != img
            {
                continue;
            }
            if let Some(ver) = image_version
                && vm.image_version != ver
            {
                continue;
            }
            out.push(MicrovmSummary {
                id: vm.id.clone(),
                state: vm.state.clone(),
                image_arn: vm.image_arn.clone(),
                image_version: vm.image_version.clone(),
                started_at_secs: vm.started_at_secs,
            });
        }
        out.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(out)
    }

    async fn mint_token(
        &self,
        id: &MicrovmId,
        scope: &[PortSpec],
        ttl_minutes: i32,
    ) -> Result<AuthToken> {
        // Parity with AwsControlPlane: the service rejects empty scopes,
        // non-positive TTLs, and tokens for non-live MicroVMs.
        if scope.is_empty() {
            return Err(Error::invalid("token scope must not be empty"));
        }
        if let Some(bad) = scope.iter().find(|s| !s.is_valid()) {
            return Err(Error::invalid(format!("invalid port scope: {bad}")));
        }
        let ttl_minutes = crate::control_plane::checked_ttl_minutes(ttl_minutes)?;
        let mut guard = self.inner.lock();
        let entry = Self::lookup(&mut guard, id)?;
        if !entry.vm.state.is_live() {
            return Err(Error::Terminated(id.to_string()));
        }
        Ok(AuthToken {
            value: format!("dev-token-{}", uuid::Uuid::new_v4()),
            issued_at: Instant::now(),
            ttl: Duration::from_secs(u64::from(ttl_minutes as u32) * 60),
            scope: scope.to_vec(),
            kind: TokenKind::Port,
        })
    }

    async fn mint_shell_token(&self, id: &MicrovmId, ttl_minutes: i32) -> Result<AuthToken> {
        let ttl_minutes = crate::control_plane::checked_ttl_minutes(ttl_minutes)?;
        let mut guard = self.inner.lock();
        let entry = Self::lookup(&mut guard, id)?;
        if !entry.vm.state.is_live() {
            return Err(Error::Terminated(id.to_string()));
        }
        Ok(AuthToken {
            value: format!("dev-shell-token-{}", uuid::Uuid::new_v4()),
            issued_at: Instant::now(),
            ttl: Duration::from_secs(u64::from(ttl_minutes as u32) * 60),
            scope: Vec::new(),
            kind: TokenKind::Shell,
        })
    }
}
