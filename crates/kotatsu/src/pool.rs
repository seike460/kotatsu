//! Warm pools and tenant→MicroVM affinity.
//!
//! `SandboxPool` is the heart of kotatsu: it keeps a configurable number
//! of unassigned MicroVMs warm, claims a VM per tenant atomically through
//! [`StateStore`], hands out [`Sandbox`] handles carrying an authenticated
//! [`MicrovmEndpoint`], and reaps dead/aged VMs on a maintenance tick.
//!
//! Capacity invariant: every pool-managed VM is counted exactly once — in
//! `warm` (unassigned), in `inflight` (reserved/popped mid-handoff, or a
//! detached reaper), or in the store (tenant-bound). Sentinel "lost-VM"
//! markers are not assignments: one whose reaper is alive counts via
//! `inflight`, one without (post-restart) counts via its marker — either
//! way exactly once. The `max_vms` cap applies to the sum, so the pool
//! can never run away on AWS spend even under concurrent acquires.
//!
//! Restart recovery: `maintain` reconciles live VMs that match the
//! pool's image but are tracked nowhere. That reconcile assumes a
//! *dedicated* image — see [`PoolConfig::reap_lost_vms`].

use parking_lot::Mutex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::control_plane::ControlPlane;
use crate::endpoint::MicrovmEndpoint;
use crate::error::{Error, Result};
use crate::state::{Binding, ClaimOutcome, StateStore};
use crate::token::TokenVending;
use crate::types::{Microvm, MicrovmId, PortSpec, RunRequest, RunningVm, TenantKey};
use crate::waiter::{WaitPolicy, wait_until_running};

/// Configuration for [`SandboxPool`].
#[derive(Clone, Debug)]
pub struct PoolConfig {
    /// Template for every VM the pool launches (image, idle policy,
    /// connectors…). `client_token` is **replaced** with a fresh value
    /// per launch — replaying one token would let AWS's idempotency
    /// collapse multiple pool runs into a single VM.
    pub run_request: RunRequest,
    /// Target number of unassigned warm VMs to keep ready.
    ///
    /// Overridden per UTC time-of-day when [`Self::warm_schedule`]
    /// contains a matching window.
    pub warm_size: usize,
    /// Optional UTC time-of-day sizing: the first matching
    /// [`WarmWindow`] replaces `warm_size` for that `maintain` tick,
    /// both topping up and terminating excess warm VMs.
    /// Empty = constant `warm_size`.
    pub warm_schedule: Vec<WarmWindow>,
    /// Hard cap on pool-managed VMs (warm + in-flight + assigned).
    pub max_vms: usize,
    /// Application port inside the VM that endpoint clients default to.
    pub app_port: u16,
    /// Token scope requested for handed-out endpoint clients. Must cover
    /// `app_port`. Defaults to just `app_port` — widen deliberately (e.g.
    /// [`PortSpec::All`]) only if tenants need other ports.
    pub token_scope: Vec<PortSpec>,
    /// Terminate VMs (warm or assigned) older than this. `None` leaves
    /// age enforcement to the VM's own `maximum_duration`.
    pub max_age: Option<Duration>,
    /// Waiter budget for acquire (boot + resume waits).
    pub wait: WaitPolicy,
    /// Reaper tick used by [`SandboxPool::spawn_maintenance`].
    pub maintenance_interval: Duration,
    /// Permit `http://` MicroVM endpoints — local development and tests
    /// only (`kotatsu-dev`, `MockControlPlane::endpoint_override`).
    /// Production pools must leave this `false`.
    pub allow_insecure_endpoints: bool,
    /// Reap live VMs that match this pool's image but are tracked
    /// nowhere — no binding, no `warm` slot, no in-flight handoff.
    /// This is the restart-recovery path for VMs whose bookkeeping was
    /// lost before a sentinel marker could persist.
    ///
    /// **Ownership assumption — off by default**: there is no VM-level
    /// tag in the `list-microvms` contract, so "same image + untracked"
    /// is the only available identity. Enable this only when the image
    /// is dedicated to this pool — with it on, an externally-launched
    /// VM of the same image/version is treated as a lost fleet member
    /// and terminated. With it off (the safe default), recovery still
    /// covers every VM that got a durable sentinel marker; only the
    /// never-pinned gap needs the reconcile.
    pub reap_lost_vms: bool,
}

impl PoolConfig {
    /// Sensible default: warm 1 VM, cap 50, port 8080, 5-minute
    /// maintenance tick.
    pub fn new(run_request: RunRequest) -> Self {
        Self {
            run_request,
            warm_size: 1,
            warm_schedule: Vec::new(),
            max_vms: 50,
            app_port: crate::DEFAULT_APP_PORT,
            token_scope: vec![PortSpec::Port(crate::DEFAULT_APP_PORT)],
            max_age: None,
            wait: WaitPolicy::default(),
            maintenance_interval: Duration::from_secs(300),
            allow_insecure_endpoints: false,
            reap_lost_vms: false,
        }
    }

    fn validate(&self) -> Result<()> {
        self.run_request.validate()?;
        if self.warm_size > self.max_vms {
            return Err(Error::invalid("warm_size must not exceed max_vms"));
        }
        if self.max_vms == 0 {
            return Err(Error::invalid("max_vms must be positive"));
        }
        for w in &self.warm_schedule {
            if w.start_min >= 1440 || w.end_min > 1440 {
                return Err(Error::invalid(format!(
                    "warm_schedule window must be within 00:00-24:00 UTC: {}-{}",
                    w.start_min, w.end_min
                )));
            }
            if w.size > self.max_vms {
                return Err(Error::invalid(format!(
                    "warm_schedule size {} exceeds max_vms {}",
                    w.size, self.max_vms
                )));
            }
        }
        if self.app_port == 0 {
            return Err(Error::invalid("app_port must be non-zero"));
        }
        if self.token_scope.is_empty()
            || self.token_scope.iter().any(|s| !s.is_valid())
            || !self.token_scope.iter().any(|s| s.covers(self.app_port))
        {
            return Err(Error::invalid(format!(
                "token_scope must be valid and cover app_port {}",
                self.app_port
            )));
        }
        self.wait.validate()?;
        if self.maintenance_interval.is_zero() {
            return Err(Error::invalid("maintenance_interval must be positive"));
        }
        Ok(())
    }

    /// Effective warm target for the instant `unix_secs` (UTC).
    /// First matching [`WarmWindow`] wins; otherwise `warm_size`.
    pub fn warm_target(&self, unix_secs: i64) -> usize {
        let m = (unix_secs.rem_euclid(86_400) / 60) as u16;
        self.warm_schedule
            .iter()
            .find(|w| w.contains(m))
            .map(|w| w.size)
            .unwrap_or(self.warm_size)
    }
}

/// A warm-set size override for a UTC time-of-day window.
///
/// `start_min`/`end_min` are minutes after 00:00 UTC (`end_min` is
/// exclusive and may be 1440 for end-of-day). A window with
/// `end_min <= start_min` wraps midnight (22:00→06:00); equal
/// bounds mean "all day".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WarmWindow {
    /// Window start, minutes after 00:00 UTC (0..1440).
    pub start_min: u16,
    /// Exclusive window end, minutes after 00:00 UTC (0..=1440).
    pub end_min: u16,
    /// Warm target inside the window.
    pub size: usize,
}

impl WarmWindow {
    /// Validated constructor (see [`PoolConfig::warm_schedule`]).
    pub fn new(start_min: u16, end_min: u16, size: usize) -> Result<Self> {
        if start_min >= 1440 || end_min > 1440 {
            return Err(Error::invalid(format!(
                "warm window {start_min}-{end_min} out of 00:00-24:00 UTC range"
            )));
        }
        Ok(Self {
            start_min,
            end_min,
            size,
        })
    }

    fn contains(&self, minute: u16) -> bool {
        if self.start_min < self.end_min {
            (self.start_min..self.end_min).contains(&minute)
        } else {
            minute >= self.start_min || minute < self.end_min
        }
    }
}

/// Outcome of one [`SandboxPool::maintain`] tick.
#[derive(Clone, Debug, Default)]
pub struct PoolReport {
    /// New VMs launched to top up the warm set.
    pub warmed: usize,
    /// VMs terminated for exceeding `max_age`.
    pub reaped: usize,
    /// Bindings dropped because their VM is gone.
    pub bindings_dropped: usize,
    /// Warm entries dropped because the VM vanished outside the pool.
    pub warm_dropped: usize,
    /// Warm VMs terminated to shrink to a smaller scheduled target
    /// ([`PoolConfig::warm_schedule`]).
    pub shrunk: usize,
    /// Warm entries dropped because a store binding owns the VM —
    /// a last-resort guard for a store that reports a failed
    /// claim/release while still applying the write.
    pub bound_dropped: usize,
}

/// Point-in-time pool counters.
#[derive(Clone, Debug)]
pub struct PoolStats {
    /// Unassigned warm VMs.
    pub warm: usize,
    /// VMs in the acquire handoff (popped or being launched), minus
    /// surplus slot appearances on VMs another registry already owns
    /// — parallel cleanups can hold several `pending` refs on one
    /// physical VM.
    pub inflight: usize,
    /// Tenant-bound VMs.
    pub assigned: usize,
    /// Sentinel-bound VMs whose ownership could not be resolved and
    /// which no live reaper, warm entry, or normal binding holds —
    /// e.g. a marker surviving a restart. A sentinel held anywhere
    /// else is counted there instead, so every VM is counted exactly
    /// once.
    pub lost: usize,
    /// Configured cap on `warm + inflight + assigned + lost`.
    pub max_vms: usize,
}

/// A MicroVM checked out to one tenant.
///
/// Dropping a `Sandbox` does **not** release anything: affinity is the
/// point, and the binding persists for the next acquire. End the
/// relationship explicitly with [`Sandbox::release`] (terminate the VM)
/// or [`Sandbox::suspend`] (keep the binding, resume on next acquire).
pub struct Sandbox {
    tenant: TenantKey,
    vm: RunningVm,
    endpoint: MicrovmEndpoint,
    cp: Arc<dyn ControlPlane>,
    store: Arc<dyn StateStore>,
    tokens: Arc<TokenVending>,
}

impl std::fmt::Debug for Sandbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sandbox")
            .field("tenant", &self.tenant)
            .field("microvm", self.vm.id())
            .finish()
    }
}

impl Sandbox {
    /// The owning tenant.
    pub fn tenant(&self) -> &TenantKey {
        &self.tenant
    }

    /// The checked-out VM (verified `RUNNING` at checkout).
    pub fn vm(&self) -> &RunningVm {
        &self.vm
    }

    /// Authenticated HTTP/WebSocket client bound to this VM.
    pub fn endpoint(&self) -> &MicrovmEndpoint {
        &self.endpoint
    }

    /// Terminates the VM and deletes the binding.
    ///
    /// The binding release is *scoped* to this VM's id: if the tenant
    /// somehow re-bound to another VM in the meantime, that newer
    /// binding is untouched. If `terminate` fails for a reason other
    /// than the VM already being gone, the binding is *kept* — the VM
    /// may still be running and billing, so it must stay tracked for
    /// the next acquire/`maintain` tick to retry.
    pub async fn release(self) -> Result<()> {
        // Counted at issue time so a cancelled in-flight call counts.
        crate::metrics::record_terminate();
        let res = self.cp.terminate(self.vm.id()).await;
        match res {
            Ok(()) | Err(Error::NotFound { .. }) | Err(Error::Terminated(_)) => {
                self.tokens.invalidate(self.vm.id());
                self.store
                    .release(&self.tenant, self.vm.id())
                    .await
                    .map(|_| ())
            }
            Err(e) => Err(e),
        }
    }

    /// Suspends the VM but keeps the tenant binding — the next
    /// [`SandboxPool::acquire`] for this tenant resumes it.
    ///
    /// Note the MicroVM's *own* `idle_policy` may also terminate a
    /// suspended VM eventually; suspension is a pause, not a freeze.
    pub async fn suspend(self) -> Result<()> {
        let res = self.cp.suspend(self.vm.id()).await;
        // Count completions — a failed suspend is not a suspended VM,
        // so it must not inflate the suspend counter.
        if res.is_ok() {
            crate::metrics::record_suspend();
        }
        res
    }
}

/// Binding-settle retries inside `acquire`: a racing `acquire` can steal
/// the claim between `get` and `claim`, so we loop a few times before
/// giving up instead of failing the first collision.
const ACQUIRE_ATTEMPTS: usize = 4;

/// Bookkeeping kept under one lock so capacity accounting stays atomic.
#[derive(Default)]
struct PoolInner {
    /// Unassigned VMs available for checkout.
    warm: Vec<Microvm>,
    /// VMs that are reserved or being launched but not yet in `warm` or
    /// bound in the store. Without this, a check-then-run race could
    /// exceed `max_vms`, and a popped warm VM would vanish from the
    /// accounting until its claim lands.
    inflight: usize,
    /// Do-not-reap registry: live VMs mid-handoff (popped from `warm`,
    /// freshly launched, or held by a detached reaper) that no binding
    /// or `warm` slot names right now. `maintain`'s lost-VM reconcile
    /// skips these, so a VM the pool still owns can never be reaped as
    /// "lost". Ref-counted — a VM can hold two registrations at once
    /// (e.g. a handoff slot plus a spawned reaper).
    pending: HashMap<MicrovmId, usize>,
    /// Untracked-VM suspects from the previous `maintain` tick. A VM
    /// must appear live and untracked on two consecutive ticks before
    /// the reconcile reaps it, so a just-launched VM can never be
    /// caught between `run` landing and its `pending` registration.
    lost_suspects: HashSet<MicrovmId>,
    /// Detached cleanup/reaper tasks. Aborted if the pool itself is
    /// dropped so an orphaned retry loop cannot outlive the pool that
    /// spawned it; finished results are drained at each spawn site.
    reapers: tokio::task::JoinSet<()>,
}

/// Removes one `pending` registration for `vm` (see [`PoolInner::pending`]).
fn pending_remove(g: &mut PoolInner, vm: &MicrovmId) {
    if let Some(n) = g.pending.get_mut(vm) {
        *n -= 1;
        if *n == 0 {
            g.pending.remove(vm);
        }
    }
}

/// Parks `vm` in `warm` unless an entry already tracks the same id.
/// Several paths can legitimately converge on one physical VM —
/// parallel wait-fail cleanups for a single binding, a sweep restore
/// racing a re-park — and a duplicate `warm` entry would hand that
/// one VM out to two tenants.
fn warm_push(g: &mut PoolInner, vm: Microvm) {
    if !g.warm.iter().any(|v| v.id == vm.id) {
        g.warm.push(vm);
    }
}

/// Surplus `warm`/`inflight` appearances for `stats`/`try_reserve`,
/// so `warm + inflight + assigned + lost` counts each physical VM
/// once. `pending` is refcounted: parallel cleanups can hold two
/// slots on one VM (k−1 surplus — still a single VM in flight), and a
/// slot on a VM a `warm` entry or normal binding already owns is
/// fully redundant (k surplus). A VM both `warm` and normally bound
/// belongs to the binding.
fn registry_overlap(g: &PoolInner, normal: &HashSet<&MicrovmId>) -> (usize, usize) {
    let inflight_extra: usize = g
        .pending
        .iter()
        .map(|(id, &k)| {
            let held = g.warm.iter().any(|v| &v.id == id) || normal.contains(id);
            if held { k } else { k - 1 }
        })
        .sum();
    let warm_extra = g.warm.iter().filter(|v| normal.contains(&v.id)).count();
    (warm_extra, inflight_extra)
}

/// Capacity reservation that also *owns* the in-handoff VM.
///
/// A popped or freshly-launched VM is untracked between materialization
/// and `store.claim` resolving — if the future is dropped in that window
/// (`?`, `tokio::time::timeout`, task abort), a bare `usize` counter
/// would keep the accounting honest but the real VM would leak and be
/// invisible to `maintain` forever. `Handoff` closes that hole: `Drop`
/// returns any unclaimed VM to `warm`, so nothing is ever "alive but
/// nowhere".
struct Handoff<'a> {
    inner: &'a Mutex<PoolInner>,
    /// The VM in transit; `None` until launched (or after `detach`).
    vm: Option<Microvm>,
    /// True once `store.claim` succeeded and the binding took over the
    /// accounting — Drop then skips both re-warming and the decrement.
    adopted: bool,
}

impl<'a> Handoff<'a> {
    /// The VM being handed over. Only absent between reservation and
    /// launch; populated before any claim attempt.
    fn vm(&self) -> &Microvm {
        self.vm.as_ref().expect("handoff without vm")
    }

    /// Marks the VM `id` as adopted by a store binding: the store now
    /// counts it, so the inflight slot and the do-not-reap registry
    /// entry are freed.
    fn adopt(&mut self, id: &MicrovmId) {
        if !self.adopted {
            self.adopted = true;
            let mut g = self.inner.lock();
            g.inflight -= 1;
            pending_remove(&mut g, id);
            crate::metrics::set_warm_inflight(g.warm.len(), g.inflight);
        }
    }

    /// Detaches the VM for explicit caller cleanup; Drop then only
    /// frees the (already-adopted) bookkeeping.
    fn detach(&mut self) -> Microvm {
        self.vm.take().expect("handoff without vm")
    }
}

impl Drop for Handoff<'_> {
    fn drop(&mut self) {
        let mut g = self.inner.lock();
        if !self.adopted {
            // Never claimed — the VM is unbound, so return it to warm
            // rather than leak it.
            if let Some(vm) = self.vm.take() {
                pending_remove(&mut g, &vm.id);
                warm_push(&mut g, vm);
            }
            g.inflight -= 1;
            crate::metrics::set_warm_inflight(g.warm.len(), g.inflight);
        }
    }
}

/// Resolves a dispatched `store.claim` even if the caller is cancelled
/// mid-await. The store's INSERT may still land after the await is
/// dropped (e.g. `SqliteStore` dispatches to a worker thread on first
/// poll), so ownership must follow the claim's *result* — decided by
/// whoever observes it — rather than assuming the cancel means "not
/// claimed".
///
/// The claim task also reports `bound`: whether the store provably
/// owns the VM (claimed, re-bound, or a verified landed write hiding
/// behind an error reply). On drop with an unresolved claim: takes
/// over the caller's `inflight` slot so the in-flight VM stays
/// counted, then a detached task parks the VM where the outcome says
/// it belongs — a bound VM must NOT re-warm, or a second tenant could
/// be handed a VM the store already bound to someone else; anything
/// unbound re-warms.
struct ClaimAdopt {
    inner: Arc<Mutex<PoolInner>>,
    join: Option<tokio::task::JoinHandle<(Result<ClaimOutcome>, bool, Microvm)>>,
    /// The VM the claim task owns — needed to free its `pending`
    /// registration if the task's result can never be recovered.
    vm_id: MicrovmId,
}

impl Drop for ClaimAdopt {
    fn drop(&mut self) {
        let Some(join) = self.join.take() else { return };
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            // No runtime left to resolve on — the spawned task keeps
            // running detached; nothing better exists at teardown.
            return;
        };
        self.inner.lock().inflight += 1;
        let inner = Arc::clone(&self.inner);
        let vm_id = self.vm_id.clone();
        handle.spawn(async move {
            let res = join.await;
            let mut g = inner.lock();
            g.inflight -= 1;
            match res {
                // Bound VMs belong to the store; only the provably
                // unbound return to warm.
                Ok((_, bound, vm)) => {
                    pending_remove(&mut g, &vm.id);
                    if !bound {
                        warm_push(&mut g, vm);
                    }
                }
                // Task panicked — the VM is unrecoverable; free its
                // `pending` entry so the reconcile can still reap it.
                Err(e) => {
                    pending_remove(&mut g, &vm_id);
                    tracing::warn!(error = %e, "claim task failed");
                }
            }
            crate::metrics::set_warm_inflight(g.warm.len(), g.inflight);
        });
    }
}

/// Builds the sentinel binding marking `vm_id` as a VM whose ownership
/// could not be resolved — the durable record `maintain` reaps. The
/// tenant key keeps the readable `{prefix}{vm}` shape, but identity is
/// the explicit `sentinel` flag — never the string.
fn sentinel_binding(vm_id: &MicrovmId) -> Binding {
    Binding {
        tenant: TenantKey::lost_vm(vm_id),
        microvm_id: vm_id.clone(),
        claimed_at_secs: chrono::Utc::now().timestamp(),
        sentinel: true,
    }
}

/// True when `b` is a sentinel marker — decided by the explicit flag
/// the store persists, not the tenant string. A historical tenant that
/// legitimately used the reserved prefix stays a normal binding.
fn is_sentinel(b: &Binding) -> bool {
    b.sentinel
}

/// True when a `claim` result proves the sentinel marker is persisted
/// for this VM — `Claimed`, or an earlier *sentinel* pin for the
/// *same* VM. A `HeldByOther` naming a different VM — or a row that
/// is not a marker at all — is not a pin: retrying preserves
/// the durable-tracking goal instead of trusting a foreign row.
fn sentinel_pinned(outcome: &Result<ClaimOutcome>, vm_id: &MicrovmId) -> bool {
    match outcome {
        Ok(ClaimOutcome::Claimed) => true,
        Ok(ClaimOutcome::HeldByOther(b)) => b.sentinel && b.microvm_id == *vm_id,
        Err(_) => false,
    }
}

/// True when a sentinel claim lost to a *normal* binding for this very
/// VM — the store provably owns it, so nothing is lost and no reaper
/// may touch it. Destroying such a VM would orphan the binding.
fn bound_to_other(outcome: &Result<ClaimOutcome>, vm_id: &MicrovmId) -> bool {
    matches!(outcome, Ok(ClaimOutcome::HeldByOther(b)) if !b.sentinel && b.microvm_id == *vm_id)
}

/// Destroys a VM whose store ownership could not be resolved. First it
/// pins a sentinel binding (`{LOST_TENANT_PREFIX}{vm}`) — once that
/// lands, the store itself durably tracks the VM and `maintain`
/// reaps it even if this task dies. `terminate` then retries with
/// capped exponential backoff until the VM is provably gone
/// (the pin retries on each failure while still unpinned); the marker
/// is released afterwards. Every issued terminate is counted.
async fn reap_lost(
    store: &Arc<dyn StateStore>,
    cp: &Arc<dyn ControlPlane>,
    tokens: &Arc<TokenVending>,
    vm_id: &MicrovmId,
) {
    let sentinel = sentinel_binding(vm_id);
    let outcome = store.claim(&sentinel).await;
    if bound_to_other(&outcome, vm_id) {
        // A normal binding owns the VM — it was never lost.
        return;
    }
    let mut pinned = sentinel_pinned(&outcome, vm_id);
    let mut backoff = Duration::from_millis(200);
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        crate::metrics::record_terminate();
        match cp.terminate(vm_id).await {
            Ok(()) | Err(Error::NotFound { .. }) | Err(Error::Terminated(_)) => break,
            Err(err) => {
                if !pinned {
                    let outcome = store.claim(&sentinel).await;
                    if bound_to_other(&outcome, vm_id) {
                        return;
                    }
                    pinned = sentinel_pinned(&outcome, vm_id);
                }
                // A permanently-failing terminate keeps this task (and
                // its inflight slot) forever — escalate periodically so
                // an operator notices the capacity drain instead of it
                // blending into the warn noise.
                if attempt.is_multiple_of(25) {
                    tracing::error!(microvm = %vm_id, attempts = attempt, "lost VM terminate still failing — check the AWS-side VM state");
                } else {
                    tracing::warn!(microvm = %vm_id, error = %err, attempt, "lost VM terminate failed; retrying");
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(60));
            }
        }
    }
    if pinned {
        // Clear the marker — best effort: a marker left behind records
        // a dead VM, which `maintain` also releases on sight.
        let _ = store.release(&sentinel.tenant, vm_id).await;
    }
    tokens.invalidate(vm_id);
}

/// Hands a VM whose store ownership could not be resolved to a detached
/// terminator. The VM may be bound to a tenant — so it must never
/// return to `warm` — and it may be live, so an `inflight` slot and a
/// `pending` registration are taken first to keep it counted exactly
/// once (and un-reapable) until termination is proven. A binding that
/// did survive ends up pointing at a dead VM and self-heals on the
/// next acquire/maintain tick.
fn spawn_terminator(
    inner: &Arc<Mutex<PoolInner>>,
    store: Arc<dyn StateStore>,
    cp: Arc<dyn ControlPlane>,
    tokens: Arc<TokenVending>,
    vm_id: MicrovmId,
) {
    {
        let mut g = inner.lock();
        g.inflight += 1;
        *g.pending.entry(vm_id.clone()).or_default() += 1;
        // Reap finished results so the set cannot grow unboundedly.
        while g.reapers.try_join_next().is_some() {}
        g.reapers.spawn({
            let inner = Arc::clone(inner);
            async move {
                reap_lost(&store, &cp, &tokens, &vm_id).await;
                let mut g = inner.lock();
                g.inflight -= 1;
                pending_remove(&mut g, &vm_id);
                crate::metrics::set_warm_inflight(g.warm.len(), g.inflight);
            }
        });
        crate::metrics::set_warm_inflight(g.warm.len(), g.inflight);
    }
}

/// Restores unprocessed + surviving warm VMs if [`SandboxPool::maintain`]
/// is aborted mid-sweep (e.g. the spawned task's handle is aborted while
/// iterating). `commit` drains the survivor list so the normal path only
/// re-extends once.
///
/// While the sweep runs, the taken VMs are counted as `inflight` so
/// `try_reserve` keeps seeing them — sweeping must not deflate the
/// `max_vms` accounting.
struct WarmSweep<'a> {
    inner: &'a Mutex<PoolInner>,
    pending: VecDeque<Microvm>,
    keep: Vec<Microvm>,
    /// The VM currently being checked/terminated — parked here so a task
    /// abort mid-await restores it via `Drop` instead of losing it.
    current: Option<Microvm>,
    /// How many VMs were taken out of `warm` (and moved into `inflight`).
    swept: usize,
    done: bool,
}

impl<'a> WarmSweep<'a> {
    fn new(inner: &'a Mutex<PoolInner>) -> Self {
        let mut g = inner.lock();
        let pending: VecDeque<Microvm> = std::mem::take(&mut g.warm).into();
        let swept = pending.len();
        g.inflight += swept;
        Self {
            inner,
            pending,
            keep: Vec::new(),
            current: None,
            swept,
            done: false,
        }
    }

    /// Returns survivors to `warm` and frees the `inflight` accounting —
    /// dropped (dead/reaped) VMs leave the accounting entirely, which is
    /// correct because they are no longer managed.
    fn restore(&mut self) {
        if self.done {
            return;
        }
        self.done = true;
        let mut g = self.inner.lock();
        for vm in self
            .keep
            .drain(..)
            .chain(self.pending.drain(..))
            .chain(self.current.take())
        {
            warm_push(&mut g, vm);
        }
        g.inflight -= self.swept;
        crate::metrics::set_warm_inflight(g.warm.len(), g.inflight);
    }

    /// Normal end; `Drop` sees `done` and stays out.
    fn commit(mut self) {
        self.restore();
    }
}

impl Drop for WarmSweep<'_> {
    fn drop(&mut self) {
        self.restore();
    }
}

/// RAII guard for a warm VM popped for scheduled scale-down
/// ([`PoolConfig::warm_schedule`]).
///
/// The pop marked the VM `inflight` — without this guard, a task abort
/// mid-`terminate` would leave `inflight` permanently inflated and the
/// still-running VM invisible to every later `maintain` tick ("alive
/// but nowhere"). `Drop` returns the VM to `warm` and frees the slot;
/// `commit` disarms the restore once terminate succeeded.
struct ShrinkPop<'a> {
    inner: &'a Mutex<PoolInner>,
    vm: Option<Microvm>,
}

impl<'a> ShrinkPop<'a> {
    fn new(inner: &'a Mutex<PoolInner>, vm: Microvm) -> Self {
        Self {
            inner,
            vm: Some(vm),
        }
    }

    /// Terminate succeeded: drop bookkeeping without returning the VM.
    fn commit(mut self) {
        self.vm = None;
    }
}

impl Drop for ShrinkPop<'_> {
    fn drop(&mut self) {
        let mut g = self.inner.lock();
        g.inflight -= 1;
        if let Some(vm) = self.vm.take() {
            warm_push(&mut g, vm);
        }
        crate::metrics::set_warm_inflight(g.warm.len(), g.inflight);
    }
}

/// Pool of warm MicroVMs with tenant affinity.
pub struct SandboxPool {
    cp: Arc<dyn ControlPlane>,
    store: Arc<dyn StateStore>,
    tokens: Arc<TokenVending>,
    cfg: PoolConfig,
    /// `Arc` so a cancelled launch's `Adopt` guard can still park its VM.
    inner: Arc<Mutex<PoolInner>>,
    /// Serializes capacity-critical sections: `[list+reserve]`,
    /// `[claim+adopt]`, and `[release→recount]` in wait-fail cleanup
    /// must each be atomic w.r.t. the others or a store write
    /// resolving in the gap between another acquirer's `list()` and
    /// counter update would leave a VM counted nowhere — or briefly
    /// nowhere, letting `try_reserve` overshoot `max_vms`.
    capacity: Arc<tokio::sync::Mutex<()>>,
    /// Serializes `maintain` ticks; a manual call plus the spawned task
    /// must not run two reconciles at once (double top-up, double reaps).
    maintaining: AtomicBool,
    /// Shared HTTP client for all endpoint connections — connection
    /// reuse across requests/VMs instead of a fresh pool per sandbox.
    http: reqwest::Client,
}

impl SandboxPool {
    /// Creates a pool. Call [`SandboxPool::maintain`] (or spawn the
    /// maintenance loop) to fill the warm set — construction itself
    /// launches nothing.
    pub fn new(
        cp: Arc<dyn ControlPlane>,
        store: Arc<dyn StateStore>,
        cfg: PoolConfig,
    ) -> Result<Self> {
        cfg.validate()?;
        Ok(Self {
            tokens: Arc::new(TokenVending::new(cp.clone())),
            cp,
            store,
            cfg,
            inner: Arc::new(Mutex::new(PoolInner::default())),
            capacity: Arc::new(tokio::sync::Mutex::new(())),
            maintaining: AtomicBool::new(false),
            http: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .build()?,
        })
    }

    /// The pool's token vending (shared with every endpoint it hands out).
    pub fn tokens(&self) -> &Arc<TokenVending> {
        &self.tokens
    }

    /// Aborts the detached reaper tasks — a retry loop must not outlive
    /// the pool that spawned it. VMs those reapers held are either
    /// sentinel-marked (durable) or rediscovered by the next pool's
    /// lost-VM reconcile, so aborting loses no tracking.
    fn abort_reapers(&self) {
        let mut g = self.inner.lock();
        g.reapers.abort_all();
    }

    /// Acquires a running MicroVM bound to `tenant`.
    ///
    /// Order: reuse the tenant's existing binding when its VM is still
    /// live (waiting through boot/resume as needed); otherwise claim a
    /// warm VM, or launch a fresh one under `max_vms`. Concurrent
    /// acquires for the same tenant converge via [`StateStore::claim`].
    pub async fn acquire(&self, tenant: &TenantKey) -> Result<Sandbox> {
        use crate::metrics::AcquireOutcome;
        let start = Instant::now();
        let res = self.acquire_inner(tenant).await;
        let outcome = match &res {
            Ok(_) => AcquireOutcome::Ok,
            Err(Error::PoolExhausted(_)) => AcquireOutcome::Exhausted,
            Err(_) => AcquireOutcome::Error,
        };
        crate::metrics::record_acquire(outcome, start.elapsed());
        res
    }

    async fn acquire_inner(&self, tenant: &TenantKey) -> Result<Sandbox> {
        for _ in 0..ACQUIRE_ATTEMPTS {
            if let Some(b) = self.store.get(tenant).await? {
                if let Some(sb) = self.bound_sandbox(tenant, &b).await? {
                    return Ok(sb);
                }
                continue;
            }

            let mut handoff = match self.pop_warm() {
                Some(h) => h,
                None => {
                    let mut h = self.try_reserve().await?;
                    // Launch failure propagates via `?` — the empty
                    // Handoff drops and frees the inflight slot. The
                    // VM arrives already pending-registered: `launch`
                    // registers inside the run task before it can be
                    // observed, so `maintain`'s reconcile always sees
                    // it as owned.
                    let vm = self.launch().await?;
                    h.vm = Some(vm);
                    h
                }
            };
            let vm_id = handoff.vm().id.clone();

            let binding = Binding {
                tenant: tenant.clone(),
                microvm_id: vm_id.clone(),
                claimed_at_secs: chrono::Utc::now().timestamp(),
                sentinel: false,
            };
            // `[claim+adopt]` under the capacity lock: a concurrent
            // `[list+reserve]` always sees this VM in inflight *or* in
            // the store — never nowhere. The claim runs on a spawned
            // task owning the VM: a cancel mid-await can leave the
            // store's INSERT applied, so `ClaimAdopt` decides ownership
            // from the resolved outcome instead of double-tracking.
            let claimed = {
                let _cap = self.capacity.lock().await;
                let vm = handoff.detach();
                let store = self.store.clone();
                let cp = self.cp.clone();
                let tokens = self.tokens.clone();
                let inner = Arc::clone(&self.inner);
                let join = tokio::spawn(async move {
                    let outcome = store.claim(&binding).await;
                    // `bound` = the store provably owns this VM. An
                    // errored claim may still have landed the INSERT
                    // (e.g. the connection died after commit), so on
                    // `Err` we verify with `get`. `Ok(None)` proves the
                    // VM is unbound. When even `get` fails the outcome
                    // is indeterminate — the VM must never re-warm (a
                    // landed claim may own it), but it may also be live
                    // and bound to nothing, so a detached terminator
                    // destroys it under an inflight slot rather than
                    // dropping it out of all accounting. A landed
                    // binding ends up pointing at a dead VM and
                    // self-heals.
                    let bound = match &outcome {
                        Ok(ClaimOutcome::Claimed) => true,
                        Ok(ClaimOutcome::HeldByOther(b)) => b.microvm_id == binding.microvm_id,
                        Err(_) => match store.get(&binding.tenant).await {
                            Ok(Some(b)) => b.microvm_id == binding.microvm_id,
                            Ok(None) => false,
                            Err(_) => {
                                spawn_terminator(
                                    &inner,
                                    store.clone(),
                                    cp.clone(),
                                    tokens.clone(),
                                    binding.microvm_id.clone(),
                                );
                                true
                            }
                        },
                    };
                    (outcome, bound, vm)
                });
                let mut guard = ClaimAdopt {
                    inner: Arc::clone(&self.inner),
                    join: Some(join),
                    vm_id: vm_id.clone(),
                };
                let res = guard.join.as_mut().expect("claim join").await;
                guard.join = None; // resolved — disarm the guard
                match res {
                    Ok((claim_res, bound, vm)) => {
                        if bound {
                            // The store owns this VM — `adopt` frees
                            // the inflight slot because the binding
                            // keeps the accounting, and the VM must
                            // never re-enter warm for another tenant.
                            handoff.adopt(&vm_id);
                        }
                        // Bound VMs we did not claim stay with the
                        // store (dropped here); our claimed VM — the
                        // wait path below needs it — and unbound ones
                        // (drop → warm) return to the handoff.
                        if !bound || matches!(claim_res, Ok(ClaimOutcome::Claimed)) {
                            handoff.vm = Some(vm);
                        }
                        claim_res
                    }
                    // Task panicked; the VM it owned is unrecoverable —
                    // free its `pending` entry so the reconcile can
                    // still reap a VM we can no longer reach.
                    Err(e) => {
                        pending_remove(&mut self.inner.lock(), &vm_id);
                        Err(Error::Other(format!("claim task failed: {e}")))
                    }
                }
            }?;
            match claimed {
                ClaimOutcome::Claimed => {
                    match wait_until_running(&*self.cp, &vm_id, &self.cfg.wait).await {
                        Ok(running) => {
                            let _vm = handoff.detach();
                            return self.sandbox(tenant.clone(), running);
                        }
                        Err(e) => {
                            let vm = handoff.detach();
                            self.wait_fail_cleanup(tenant, vm_id, vm);
                            return Err(e);
                        }
                    }
                }
                ClaimOutcome::HeldByOther(_) => {
                    // Another acquire won the tenant; the Handoff drop
                    // returns our unclaimed candidate to warm.
                }
            }
        }
        Err(Error::Other(format!(
            "pool acquire could not settle a binding after {ACQUIRE_ATTEMPTS} attempts"
        )))
    }

    /// One maintenance tick: reconcile bindings and warm VMs against the
    /// service-side list, reap `max_age` violations, then grow or shrink
    /// the warm set to the (possibly scheduled) target. Idempotent and
    /// self-serializing — a second concurrent call returns an empty
    /// report.
    pub async fn maintain(&self) -> Result<PoolReport> {
        self.maintain_at(chrono::Utc::now().timestamp()).await
    }

    /// [`SandboxPool::maintain`] evaluated as of `unix_secs` (UTC) —
    /// the timestamp feeds both `max_age` checks and
    /// [`PoolConfig::warm_schedule`] window selection. Exposed so
    /// schedules and ageing are deterministically testable.
    pub async fn maintain_at(&self, unix_secs: i64) -> Result<PoolReport> {
        // CAS, not load+store, so two racers can't both observe false.
        if self
            .maintaining
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(PoolReport::default());
        }
        // Reset via Drop so early returns and task aborts can't stick
        // the flag on forever.
        struct Guard<'a>(&'a AtomicBool);
        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::SeqCst);
            }
        }
        let _guard = Guard(&self.maintaining);
        self.maintain_inner(unix_secs).await
    }

    async fn maintain_inner(&self, now: i64) -> Result<PoolReport> {
        let summaries = self.cp.list(None, None).await?;
        let mut report = PoolReport::default();
        let bindings = self.store.list().await?;
        // VMs owned by a store binding must never sit in `warm`. The
        // claim/release paths resolve outcomes before re-warming, so
        // this is the last-resort guard for a store that reported a
        // failure while still applying its write. The binding owns it;
        // drop it from `warm` so no second tenant can be handed the
        // same VM.
        let bound: std::collections::HashSet<&MicrovmId> =
            bindings.iter().map(|b| &b.microvm_id).collect();
        // Ownership snapshots for marker-vs-VM coexistence checks —
        // shared with the lost-VM reconcile below.
        let normal_bound: std::collections::HashSet<&MicrovmId> = bindings
            .iter()
            .filter(|b| !b.sentinel)
            .map(|b| &b.microvm_id)
            .collect();
        let (warm_ids, pending_ids) = {
            let g = self.inner.lock();
            (
                g.warm.iter().map(|v| v.id.clone()).collect::<HashSet<_>>(),
                g.pending.keys().cloned().collect::<HashSet<_>>(),
            )
        };

        // -- bindings: drop dead VMs' records, reap aged ones --
        for b in &bindings {
            // Sentinel bindings record VMs whose ownership could not
            // be resolved — destroy on sight and clear the marker.
            // The row is the durable record, so reaping works even
            // after a restart that lost the in-flight reaper task.
            if is_sentinel(b) {
                // A reaper/cleanup slot holds the VM — the live task
                // owns the marker lifecycle (it may be mid-handoff
                // between pin and release), so the marker is never
                // cleared out from under it.
                if pending_ids.contains(&b.microvm_id) {
                    continue;
                }
                // Marker decisions serialize with cleanup handoffs and
                // claim adoptions on the capacity lock — and they must
                // consult *fresh* state, not our snapshots: a claim
                // landing or a handoff completing after `store.list`
                // still shows up here (every binding write resolves
                // under this same lock), so a just-adopted VM is never
                // mistaken for a lost one.
                {
                    let _cap = self.capacity.lock().await;
                    let owned = {
                        let g = self.inner.lock();
                        if g.pending.contains_key(&b.microvm_id) {
                            continue;
                        }
                        g.warm.iter().any(|v| v.id == b.microvm_id)
                    } || match self.store.list().await {
                        Ok(fresh) => fresh
                            .iter()
                            .any(|x| !x.sentinel && x.microvm_id == b.microvm_id),
                        Err(e) => {
                            crate::metrics::record_maintain(&report);
                            return Err(e);
                        }
                    };
                    // The VM is tracked elsewhere — the marker is
                    // stale. A normal binding or a warm slot already
                    // owns it (a bound+sentinel coexistence can be left
                    // over from a crash between pin and resolution), so
                    // release the marker instead of destroying an
                    // owned VM.
                    if owned {
                        if let Err(e) = self.store.release(&b.tenant, &b.microvm_id).await {
                            crate::metrics::record_maintain(&report);
                            return Err(e);
                        }
                        continue;
                    }
                }
                // Terminate is deliberately outside the lock — a handoff
                // starting in the gap re-pins via the existing marker or
                // terminates alongside us; either way the VM ends dead.
                crate::metrics::record_terminate();
                match self.cp.terminate(&b.microvm_id).await {
                    Ok(()) | Err(Error::NotFound { .. }) | Err(Error::Terminated(_)) => {
                        // Scoped release — a re-pin after our snapshot
                        // would be a *new* incident's marker, but it
                        // names the same VM anyway.
                        if let Err(e) = self.store.release(&b.tenant, &b.microvm_id).await {
                            crate::metrics::record_maintain(&report);
                            return Err(e);
                        }
                        self.tokens.invalidate(&b.microvm_id);
                        report.reaped += 1;
                    }
                    // Keep the marker — the next tick retries.
                    Err(e) => {
                        tracing::warn!(microvm = %b.microvm_id, error = %e, "lost VM terminate failed");
                    }
                }
                continue;
            }
            let summary = summaries.iter().find(|s| s.id == b.microvm_id);
            let live = match summary {
                Some(s) => s.state.is_live(),
                // Not in list-microvms is not proof of death — the list
                // can lag. Confirm with a direct get before tearing down
                // a live binding.
                None => match self.cp.get(&b.microvm_id).await {
                    Ok(v) => v.is_live(),
                    Err(Error::NotFound { .. }) => false,
                    Err(e) => {
                        tracing::warn!(microvm = %b.microvm_id, error = %e, "binding vm check failed");
                        true // don't reap on inconclusive errors
                    }
                },
            };
            if !live {
                // Scoped release: a re-bind created after our snapshot
                // must survive.
                if let Err(e) = self.store.release(&b.tenant, &b.microvm_id).await {
                    crate::metrics::record_maintain(&report);
                    return Err(e);
                }
                self.tokens.invalidate(&b.microvm_id);
                report.bindings_dropped += 1;
                continue;
            }
            if self.aged_out(summary.and_then(|s| s.started_at_secs), now) {
                crate::metrics::record_terminate();
                let res = self.cp.terminate(&b.microvm_id).await;
                match res {
                    Ok(()) | Err(Error::NotFound { .. }) | Err(Error::Terminated(_)) => {}
                    // Terminate failed — the VM may still be alive, so
                    // keep the binding; the next tick retries.
                    Err(e) => {
                        tracing::warn!(microvm = %b.microvm_id, error = %e, "aged binding terminate failed");
                        continue;
                    }
                }
                if let Err(e) = self.store.release(&b.tenant, &b.microvm_id).await {
                    crate::metrics::record_maintain(&report);
                    return Err(e);
                }
                self.tokens.invalidate(&b.microvm_id);
                report.reaped += 1;
            }
        }

        // -- lost VMs: live, pool-image, tracked nowhere --
        // A VM whose sentinel pin could not land before a restart (or
        // that was lost by any other bookkeeping failure) is only
        // visible in `list-microvms` — reap it before it bills
        // forever. Two consecutive sightings are required so a
        // just-launched VM can never be caught between `run` landing
        // and its `pending` registration; anything bound, warm, or
        // mid-handoff is skipped. `reap_lost_vms` documents the
        // dedicated-image ownership assumption this relies on.
        if self.cfg.reap_lost_vms {
            let suspects = std::mem::take(&mut self.inner.lock().lost_suspects);
            let image = &self.cfg.run_request.image_identifier;
            let version = self.cfg.run_request.image_version.as_deref();
            let mut next_suspects = HashSet::new();
            for s in summaries.iter().filter(|s| {
                s.state.is_live()
                    && s.image_arn == *image
                    && version.is_none_or(|v| s.image_version == v)
                    && !bound.contains(&s.id)
                    && !warm_ids.contains(&s.id)
                    && !pending_ids.contains(&s.id)
            }) {
                if !suspects.contains(&s.id) {
                    // First sighting — could still be mid-registration.
                    next_suspects.insert(s.id.clone());
                    continue;
                }
                // Second consecutive sighting: provably tracked
                // nowhere. Pin the marker (best effort — store trouble
                // may be why it is lost) and terminate once; a failure
                // keeps the suspect for next tick, and a landed marker
                // is retried by the sentinel branch anyway.
                let sentinel = sentinel_binding(&s.id);
                let outcome = self.store.claim(&sentinel).await;
                // A normal binding landed between our snapshots — the
                // VM is owned now; never terminate it.
                if bound_to_other(&outcome, &s.id) {
                    continue;
                }
                let pinned = sentinel_pinned(&outcome, &s.id);
                crate::metrics::record_terminate();
                match self.cp.terminate(&s.id).await {
                    Ok(()) | Err(Error::NotFound { .. }) | Err(Error::Terminated(_)) => {
                        if pinned {
                            let _ = self.store.release(&sentinel.tenant, &s.id).await;
                        }
                        self.tokens.invalidate(&s.id);
                        report.reaped += 1;
                    }
                    Err(e) => {
                        tracing::warn!(microvm = %s.id, error = %e, "lost VM terminate failed");
                        next_suspects.insert(s.id.clone());
                    }
                }
            }
            self.inner.lock().lost_suspects = next_suspects;
        }

        // -- warm set: drop vanished VMs, reap aged ones --
        // WarmSweep restores whatever we haven't processed if this task
        // is aborted mid-loop.
        let mut sweep = WarmSweep::new(self.inner.as_ref());
        while let Some(vm) = sweep.pending.pop_front() {
            // Park in `current` so an abort mid-await restores this VM.
            sweep.current = Some(vm);
            let id = sweep.current.as_ref().expect("sweep current").id.clone();
            if normal_bound.contains(&id) {
                // Bound to a tenant — the binding owns it; drop it from
                // `warm` so it can't be handed to another tenant. A
                // *sentinel* marker is not tenant ownership: it's a
                // destroy-pending record the marker branch resolves on
                // its own, so a marker-held warm VM stays in `warm` —
                // dropping it here after the marker was released would
                // leave the live VM tracked nowhere.
                sweep.current = None;
                report.bound_dropped += 1;
                continue;
            }
            let summary = summaries.iter().find(|s| s.id == id);
            match summary {
                Some(s) if !s.state.is_live() => {
                    sweep.current = None;
                    report.warm_dropped += 1;
                }
                Some(s) if self.aged_out(s.started_at_secs, now) => {
                    crate::metrics::record_terminate();
                    let res = self.cp.terminate(&id).await;
                    match res {
                        Ok(()) | Err(Error::NotFound { .. }) | Err(Error::Terminated(_)) => {
                            self.tokens.invalidate(&id);
                            sweep.current = None;
                            report.reaped += 1;
                        }
                        // Terminate failed — the VM may still be alive:
                        // keep it tracked so the next tick retries.
                        Err(e) => {
                            tracing::warn!(microvm = %id, error = %e, "aged warm terminate failed");
                            sweep
                                .keep
                                .push(sweep.current.take().expect("sweep current"));
                        }
                    }
                }
                // VMs absent from list-microvms may be eventual-consistency
                // casualties rather than truly gone; keep entries whose own
                // state lookup still works, drop the rest.
                None => match self.cp.get(&id).await {
                    Ok(fresh) if fresh.is_live() => {
                        sweep.current = None;
                        sweep.keep.push(fresh);
                    }
                    Ok(_) | Err(Error::NotFound { .. }) => {
                        sweep.current = None;
                        report.warm_dropped += 1;
                    }
                    Err(e) => {
                        tracing::warn!(microvm = %id, error = %e, "warm vm check failed");
                        sweep
                            .keep
                            .push(sweep.current.take().expect("sweep current"));
                    }
                },
                Some(_) => {
                    sweep
                        .keep
                        .push(sweep.current.take().expect("sweep current"));
                }
            }
        }
        sweep.commit();
        crate::metrics::record_maintain(&report);

        // -- apply the (possibly scheduled) warm target --
        let target = self.cfg.warm_target(now);

        // Shrink: terminate excess warm VMs rather than idling them —
        // warm VMs bill while RUNNING. `pop` removes the newest entry,
        // the same end `acquire` claims from, so either end is fair.
        // `ShrinkPop` keeps an abort mid-`terminate` from leaking the
        // inflight slot and the (still running) VM.
        loop {
            let pop = {
                let mut g = self.inner.lock();
                if g.warm.len() > target {
                    // Count the popped VM as inflight while terminate is
                    // in flight — warm+inflight+assigned must stay
                    // accurate for try_reserve's capacity check.
                    g.inflight += 1;
                    let pop = g
                        .warm
                        .pop()
                        .map(|vm| ShrinkPop::new(self.inner.as_ref(), vm));
                    crate::metrics::set_warm_inflight(g.warm.len(), g.inflight);
                    pop
                } else {
                    None
                }
            };
            let Some(pop) = pop else { break };
            let vm_id = pop.vm.as_ref().expect("shrink pop without vm").id.clone();
            // Issued unconditionally — failed terminates count too.
            crate::metrics::record_terminate();
            let res = self.cp.terminate(&vm_id).await;
            match res {
                // NotFound/Terminated mean the VM is already gone —
                // the shrink goal is met, not a failure to retry.
                Ok(()) | Err(Error::NotFound { .. }) | Err(Error::Terminated(_)) => {
                    crate::metrics::record_shrunk();
                    pop.commit();
                    self.tokens.invalidate(&vm_id);
                    report.shrunk += 1;
                }
                // Drop restores the VM to warm; the next tick's sweep
                // re-checks liveness and retries.
                Err(e) => {
                    tracing::warn!(microvm = %vm_id, error = %e, "shrink terminate failed");
                    break;
                }
            }
        }

        // -- top up the warm set --
        loop {
            // Re-read per iteration: racing pushes can fill the set
            // while we were launching.
            if self.inner.lock().warm.len() >= target {
                break;
            }
            match self.try_reserve().await {
                Err(Error::PoolExhausted(_)) => break,
                Err(e) => return Err(e),
                Ok(h) => {
                    let vm = self.launch().await?;
                    // Push before dropping the handoff: the VM is always
                    // counted (warm *or* inflight), never neither.
                    self.push_warm(vm);
                    drop(h);
                    crate::metrics::record_warmed();
                    report.warmed += 1;
                }
            }
        }
        Ok(report)
    }

    /// Spawns a background task calling [`SandboxPool::maintain`] every
    /// `maintenance_interval`. Abort the returned handle to stop it —
    /// aborting is safe mid-tick: state locks are never held across
    /// `.await`, swept warm VMs are restored by an internal guard, the
    /// `maintaining` flag resets via `Drop`, and an in-flight `run` is
    /// adopted into `warm` by a detached task once it resolves.
    pub fn spawn_maintenance(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let pool = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(pool.cfg.maintenance_interval).await;
                if let Err(e) = pool.maintain().await {
                    tracing::warn!(error = %e, "pool maintenance tick failed");
                }
            }
        })
    }

    /// Live detached cleanup/reaper tasks (finished results included
    /// until a spawn site drains them). Test/diagnostic support.
    #[doc(hidden)]
    pub fn detached_task_count(&self) -> usize {
        self.inner.lock().reapers.len()
    }

    /// Current pool counters.
    ///
    /// The store list and the in-memory sets are read a moment apart,
    /// so individual buckets may straddle a handoff — but the dedup
    /// rules below count every cross-registry appearance once, so the
    /// returned counters never name the same physical VM twice.
    pub async fn stats(&self) -> PoolStats {
        // Sentinels are not tenant bindings. One whose VM a live
        // reaper, a `warm` entry, or a normal binding already holds is
        // carried there; report in `lost` only the markers tracking
        // the VM nowhere else (post-restart or abandoned). `inflight`
        // likewise drops the pending slots whose VM already sits in
        // `warm` — a sibling cleanup parked it — so
        // `warm + inflight + assigned + lost` counts each VM once.
        let (warm, inflight, assigned, lost) = match self.store.list().await {
            Ok(l) => {
                let normal: HashSet<&MicrovmId> = l
                    .iter()
                    .filter(|b| !b.sentinel)
                    .map(|b| &b.microvm_id)
                    .collect();
                let g = self.inner.lock();
                let (assigned, lost) = l.iter().fold((0, 0), |(a, s), b| {
                    if is_sentinel(b) {
                        let held = g.pending.contains_key(&b.microvm_id)
                            || g.warm.iter().any(|v| v.id == b.microvm_id)
                            || normal.contains(&b.microvm_id);
                        (a, s + usize::from(!held))
                    } else {
                        (a + 1, s)
                    }
                });
                let (warm_extra, inflight_extra) = registry_overlap(&g, &normal);
                (
                    g.warm.len() - warm_extra,
                    g.inflight - inflight_extra,
                    assigned,
                    lost,
                )
            }
            Err(_) => {
                let g = self.inner.lock();
                let empty = HashSet::new();
                let (warm_extra, inflight_extra) = registry_overlap(&g, &empty);
                (g.warm.len() - warm_extra, g.inflight - inflight_extra, 0, 0)
            }
        };
        let stats = PoolStats {
            warm,
            inflight,
            assigned,
            lost,
            max_vms: self.cfg.max_vms,
        };
        crate::metrics::set_pool_stats(&stats);
        stats
    }

    /// Terminates every pool-managed VM and clears all bindings.
    ///
    /// For embedders performing a full teardown — e.g. a dev-mode
    /// process exiting or test cleanup. `kotatsud` deliberately does
    /// *not* call this on shutdown: bindings persist in the state store
    /// and the VMs keep running for the next start. A failed
    /// `terminate` leaves its VM tracked — warm VMs return to `warm`,
    /// bindings stay bound — so a later `drain`/`maintain` can retry
    /// rather than leaking a live VM nobody reaps.
    ///
    /// Not atomic: an `acquire` racing `drain` may land a new VM after
    /// the sweep, and a concurrent `maintain` sweep can restore VMs it
    /// took out before draining them. For shutdown semantics, abort the
    /// maintenance task and stop accepting acquires first.
    pub async fn drain(&self) -> Result<()> {
        let warm_vms: Vec<Microvm> = {
            let mut g = self.inner.lock();
            let taken = std::mem::take(&mut g.warm);
            crate::metrics::set_warm_inflight(0, g.inflight);
            taken
        };
        let mut leftover = Vec::new();
        for vm in warm_vms {
            crate::metrics::record_terminate();
            let res = self.cp.terminate(&vm.id).await;
            match res {
                Ok(()) | Err(Error::NotFound { .. }) | Err(Error::Terminated(_)) => {
                    self.tokens.invalidate(&vm.id);
                }
                Err(e) => {
                    tracing::warn!(microvm = %vm.id, error = %e, "drain: warm terminate failed");
                    leftover.push(vm);
                }
            }
        }
        if !leftover.is_empty() {
            let mut g = self.inner.lock();
            for vm in leftover {
                warm_push(&mut g, vm);
            }
            crate::metrics::set_warm_inflight(g.warm.len(), g.inflight);
        }
        for b in self.store.list().await? {
            crate::metrics::record_terminate();
            let res = self.cp.terminate(&b.microvm_id).await;
            match res {
                Ok(()) | Err(Error::NotFound { .. }) | Err(Error::Terminated(_)) => {
                    let _ = self.store.release(&b.tenant, &b.microvm_id).await;
                    self.tokens.invalidate(&b.microvm_id);
                }
                Err(e) => {
                    // Binding stays — the VM may still be running.
                    tracing::warn!(microvm = %b.microvm_id, error = %e, "drain: terminate failed");
                }
            }
        }
        Ok(())
    }

    /// Atomically checks capacity and takes one slot, inside the
    /// `capacity` lock so a racing `[claim+adopt]` can't slip a VM into
    /// the store between our `list()` and our counter update.
    async fn try_reserve(&self) -> Result<Handoff<'_>> {
        let _cap = self.capacity.lock().await;
        // Sentinel markers are not assignments. A sentinel whose VM a
        // live reaper, a `warm` entry, or a normal binding already
        // holds counts there; one held nowhere (post-restart, or a
        // reaper that gave up) counts via its marker — either way
        // exactly once. `registry_overlap` drops the surplus
        // appearances a VM picks up across the other registries.
        let bindings = self.store.list().await?;
        let normal: HashSet<&MicrovmId> = bindings
            .iter()
            .filter(|b| !b.sentinel)
            .map(|b| &b.microvm_id)
            .collect();
        let mut g = self.inner.lock();
        let (assigned, lost_unheld) = bindings.iter().fold((0, 0), |(a, l), b| {
            if is_sentinel(b) {
                let held = g.pending.contains_key(&b.microvm_id)
                    || g.warm.iter().any(|v| v.id == b.microvm_id)
                    || normal.contains(&b.microvm_id);
                (a, l + usize::from(!held))
            } else {
                (a + 1, l)
            }
        });
        let (warm_extra, inflight_extra) = registry_overlap(&g, &normal);
        let managed =
            g.warm.len() - warm_extra + g.inflight - inflight_extra + assigned + lost_unheld;
        if managed >= self.cfg.max_vms {
            return Err(Error::PoolExhausted(managed));
        }
        g.inflight += 1;
        crate::metrics::set_warm_inflight(g.warm.len(), g.inflight);
        drop(g);
        Ok(Handoff {
            inner: self.inner.as_ref(),
            vm: None,
            adopted: false,
        })
    }

    /// Pops one warm VM and immediately accounts it as `inflight`,
    /// registering it in `pending` so the lost-VM reconcile keeps off
    /// a live VM that is momentarily bound nowhere.
    fn pop_warm(&self) -> Option<Handoff<'_>> {
        let mut g = self.inner.lock();
        let popped = g.warm.pop();
        if let Some(vm) = &popped {
            g.inflight += 1;
            *g.pending.entry(vm.id.clone()).or_default() += 1;
        }
        crate::metrics::set_warm_inflight(g.warm.len(), g.inflight);
        popped.map(|vm| Handoff {
            inner: self.inner.as_ref(),
            vm: Some(vm),
            adopted: false,
        })
    }

    /// Parks a VM in `warm`, releasing the `pending` registration
    /// `launch` made for it — the warm slot takes over ownership.
    fn push_warm(&self, vm: Microvm) {
        let mut g = self.inner.lock();
        pending_remove(&mut g, &vm.id);
        warm_push(&mut g, vm);
        crate::metrics::set_warm_inflight(g.warm.len(), g.inflight);
    }

    /// Launches one VM from the template with a *fresh* client token —
    /// replaying the template's token would let AWS idempotency collapse
    /// separate launches into one VM.
    ///
    /// Cancel-safe: `run` is spawned rather than awaited inline. Once the
    /// AWS call is in flight the VM may already exist — if the caller is
    /// dropped (timeout, task abort) the `Adopt` guard parks the finished
    /// VM into `warm` so it stays tracked and sweepable instead of
    /// becoming an invisible billing VM.
    async fn launch(&self) -> Result<Microvm> {
        let mut req = self.cfg.run_request.clone();
        req.client_token = Some(format!("kotatsu-pool-{}", uuid::Uuid::new_v4()));
        let join = tokio::spawn({
            let cp = Arc::clone(&self.cp);
            let inner = Arc::clone(&self.inner);
            async move {
                let res = cp.run(&req).await;
                if let Ok(vm) = &res {
                    // Do-not-reap registration the instant `run`
                    // resolves — before the caller can even see the
                    // VM — so `maintain`'s lost-VM reconcile always
                    // finds it owned. Every consumer (handoff,
                    // `push_warm`, `Adopt` re-park) frees exactly one
                    // entry.
                    *inner.lock().pending.entry(vm.id.clone()).or_default() += 1;
                }
                res
            }
        });
        /// Re-parks a VM whose caller vanished mid-launch.
        struct Adopt {
            inner: Arc<Mutex<PoolInner>>,
            join: Option<tokio::task::JoinHandle<Result<Microvm>>>,
        }
        impl Drop for Adopt {
            fn drop(&mut self) {
                let Some(join) = self.join.take() else { return };
                let Ok(handle) = tokio::runtime::Handle::try_current() else {
                    // No runtime left to adopt on — the spawned task
                    // still runs the AWS call to completion detached.
                    return;
                };
                // Take over the caller's inflight slot so the in-flight
                // VM stays counted until it lands in `warm` — without
                // this the gap between `Handoff`'s release and the push
                // would let `try_reserve` overshoot `max_vms`.
                self.inner.lock().inflight += 1;
                let inner = Arc::clone(&self.inner);
                handle.spawn(async move {
                    let res = join.await;
                    crate::metrics::record_launch();
                    let mut g = inner.lock();
                    g.inflight -= 1;
                    if let Ok(Ok(vm)) = res {
                        pending_remove(&mut g, &vm.id);
                        warm_push(&mut g, vm);
                    }
                    crate::metrics::set_warm_inflight(g.warm.len(), g.inflight);
                });
            }
        }
        let mut guard = Adopt {
            inner: Arc::clone(&self.inner),
            join: Some(join),
        };
        let res = guard.join.as_mut().expect("launch join").await;
        guard.join = None; // resolved — disarm the adopt guard
        crate::metrics::record_launch();
        res.map_err(|e| Error::Other(format!("launch task failed: {e}")))?
    }

    fn aged_out(&self, started_at_secs: Option<i64>, now: i64) -> bool {
        match (self.cfg.max_age, started_at_secs) {
            (Some(max), Some(started)) => {
                now.saturating_sub(started) > i64::try_from(max.as_secs()).unwrap_or(i64::MAX)
            }
            _ => false,
        }
    }

    /// Cleanup after a failed wait on a freshly-claimed VM: release the
    /// tenant binding, then re-warm the VM if it proves unbound and
    /// live, or terminate it otherwise. The work runs detached so a
    /// caller cancelled mid-`store.release` cannot guess at the
    /// outcome — if the dispatched DELETE did not land, the binding
    /// still owns the VM and re-warming it would hand a bound VM to
    /// another tenant — and so a retrying terminator can never stall
    /// the acquire that spawned it.
    ///
    /// The release/verification runs inside the `capacity` lock, making
    /// the binding→inflight handoff atomic w.r.t. `try_reserve`: a
    /// racing acquirer sees the VM in `assigned` *or* `inflight` —
    /// never in neither — so `max_vms` can neither be undershot nor
    /// overshot. `warm` placement frees the slot in the same lock, and
    /// `Unknown` ownership holds a slot while a retrying terminate
    /// collapses the ambiguity — bound or not, the VM must never be
    /// re-warmed and never go uncounted.
    ///
    /// Once the binding releases (Unbound/Unknown), a sentinel marker
    /// pins durable tracking for the transit — a task aborted mid-flight
    /// (pool drop) leaves the marker, and the next `maintain` destroys
    /// the VM rather than leaking it untracked. The marker clears once
    /// the VM lands in `warm` or is provably gone.
    ///
    /// The task joins the pool's `reapers` set — dropped with the pool
    /// instead of retrying detached, and drained there once finished.
    fn wait_fail_cleanup(&self, tenant: &TenantKey, vm_id: MicrovmId, vm: Microvm) {
        let task = {
            let store = self.store.clone();
            let cp = self.cp.clone();
            let tokens = self.tokens.clone();
            let inner = Arc::clone(&self.inner);
            let capacity = Arc::clone(&self.capacity);
            let tenant = tenant.clone();
            let vm_id = vm_id.clone();
            async move {
                /// The VM's ownership after the release attempt.
                enum Ownership {
                    /// The store provably does not bind this VM.
                    Unbound,
                    /// The binding survived — it owns and tracks the VM.
                    Bound,
                    /// The release outcome is lost — the DELETE may or
                    /// may not have landed.
                    Unknown,
                }
                /// The inflight slot held while an unbound or
                /// indeterminate VM is in transit — Drop frees it (and
                /// the `pending` registration it owns) when the VM is
                /// terminated or parked back in warm.
                struct Slot {
                    inner: Arc<Mutex<PoolInner>>,
                    id: MicrovmId,
                    armed: bool,
                }
                impl Slot {
                    /// Takes the slot inside the `capacity` critical
                    /// section, so the handoff from `assigned` is
                    /// invisible to `try_reserve`. Registers `pending`
                    /// in the same lock — the moment the binding stops
                    /// tracking the VM, the do-not-reap registry takes
                    /// over so `maintain`'s reconcile cannot mistake
                    /// the in-transit VM for a lost one while `cp.get`
                    /// is still in flight.
                    fn new(inner: &Arc<Mutex<PoolInner>>, id: MicrovmId) -> Self {
                        let mut g = inner.lock();
                        g.inflight += 1;
                        *g.pending.entry(id.clone()).or_default() += 1;
                        Self {
                            inner: Arc::clone(inner),
                            id,
                            armed: true,
                        }
                    }
                    /// Lands the VM in `warm` and frees the slot in the
                    /// same lock — the VM is never counted twice nor
                    /// nowhere for even an instant.
                    fn place_warm(mut self, vm: Microvm) {
                        let mut g = self.inner.lock();
                        pending_remove(&mut g, &self.id);
                        warm_push(&mut g, vm);
                        g.inflight -= 1;
                        self.armed = false;
                        crate::metrics::set_warm_inflight(g.warm.len(), g.inflight);
                    }
                }
                impl Drop for Slot {
                    fn drop(&mut self) {
                        if self.armed {
                            let mut g = self.inner.lock();
                            pending_remove(&mut g, &self.id);
                            g.inflight -= 1;
                            crate::metrics::set_warm_inflight(g.warm.len(), g.inflight);
                        }
                    }
                }
                // The durable marker covering the release→placed
                // transit — if this task dies mid-flight (pool drop
                // aborts it), the marker keeps the VM owned and
                // `maintain` destroys it. Released once the VM lands
                // somewhere tracked.
                let marker = sentinel_binding(&vm_id);
                // `[release→recount]` under the capacity lock: the
                // binding's count must not lapse before the inflight
                // slot takes over — the gap would let `try_reserve`
                // overshoot `max_vms`.
                let (ownership, slot) = {
                    let _cap = capacity.lock().await;
                    // Pending BEFORE any store write: `maintain` leaves
                    // a pending-held marker to its owner, so nothing
                    // can clear the pin as "stale" mid-handoff.
                    let slot = Slot::new(&inner, vm_id.clone());
                    // Pin BEFORE the destructive release — and only
                    // release once the marker is PROVEN durable. If the
                    // pin can't be verified (store error, or the row is
                    // occupied by a non-sentinel binding), bail out:
                    // the tenant binding still tracks the VM, so
                    // aborting here leaks nothing.
                    if !sentinel_pinned(&store.claim(&marker).await, &vm_id) {
                        tracing::warn!(microvm = %vm_id, "sentinel pin unverifiable — binding kept, VM stays tracked");
                        return;
                    }
                    let ownership = match store.release(&tenant, &vm_id).await {
                        Ok(_) => Ownership::Unbound,
                        Err(err) => {
                            tracing::warn!(microvm = %vm_id, error = %err, "wait-fail binding release failed");
                            match store.get(&tenant).await {
                                Ok(Some(b)) if b.microvm_id == vm_id => Ownership::Bound,
                                // The DELETE landed despite the error
                                // (or the tenant re-bound elsewhere) —
                                // the VM is unbound.
                                Ok(_) => Ownership::Unbound,
                                Err(_) => Ownership::Unknown,
                            }
                        }
                    };
                    // Bound: the pin we just made is stale — the
                    // binding owns the VM. Drop it best-effort.
                    if matches!(ownership, Ownership::Bound) {
                        let _ = store.release(&marker.tenant, &vm_id).await;
                    }
                    (ownership, slot)
                };
                match ownership {
                    Ownership::Unbound => {
                        match cp.get(&vm_id).await {
                            Ok(fresh) if fresh.is_live() => {
                                // Warm owns the VM — the marker has
                                // done its job. Clear it *after* the
                                // placement lands: aborting between the
                                // two leaves the marker, and `maintain`
                                // destroys the VM rather than leaking
                                // it untracked.
                                slot.place_warm(fresh);
                                let _ = store.release(&marker.tenant, &vm_id).await;
                            }
                            _ => {
                                crate::metrics::record_terminate();
                                match cp.terminate(&vm_id).await {
                                    Ok(())
                                    | Err(Error::NotFound { .. })
                                    | Err(Error::Terminated(_)) => {
                                        tokens.invalidate(&vm_id);
                                        let _ = store.release(&marker.tenant, &vm_id).await;
                                    }
                                    // Terminate failed — park it in
                                    // warm only when it proves live (a
                                    // dead VM must never be re-warmed).
                                    Err(_) => match cp.get(&vm_id).await {
                                        Ok(fresh) if fresh.is_live() => {
                                            slot.place_warm(fresh);
                                            let _ = store.release(&marker.tenant, &vm_id).await;
                                        }
                                        // Confirmed dead — clear the
                                        // marker, drop the VM.
                                        Ok(_) => {
                                            let _ = store.release(&marker.tenant, &vm_id).await;
                                        }
                                        // Inconclusive — keep the
                                        // snapshot tracked in warm for
                                        // the sweep.
                                        Err(_) => {
                                            slot.place_warm(vm);
                                            let _ = store.release(&marker.tenant, &vm_id).await;
                                        }
                                    },
                                }
                            }
                        }
                    }
                    Ownership::Bound => {
                        // The binding owns and tracks the VM — never
                        // re-warm. Terminate so a boot-failed VM cannot
                        // wedge the tenant or bill idle; on failure the
                        // binding keeps it tracked for the next
                        // acquire/maintain tick.
                        crate::metrics::record_terminate();
                        match cp.terminate(&vm_id).await {
                            Ok(()) | Err(Error::NotFound { .. }) | Err(Error::Terminated(_)) => {
                                tokens.invalidate(&vm_id);
                            }
                            Err(err) => {
                                tracing::warn!(microvm = %vm_id, error = %err, "wait-fail bound VM terminate failed; binding keeps it tracked");
                            }
                        }
                    }
                    Ownership::Unknown => {
                        // Ownership is unresolvable — the VM may be
                        // bound and may be live. The slot keeps it
                        // counted (a transient over-count against a
                        // surviving binding is the safe direction)
                        // while `reap_lost` destroys the VM under the
                        // marker we already pinned; a landed binding
                        // then points at a dead VM and self-heals.
                        let _slot = slot;
                        reap_lost(&store, &cp, &tokens, &vm_id).await;
                    }
                }
            }
        };
        let mut g = self.inner.lock();
        // Reap finished results so the set cannot grow unboundedly.
        while g.reapers.try_join_next().is_some() {}
        g.reapers.spawn(task);
    }

    /// Resolves an existing binding to a Sandbox, cleaning up when the VM
    /// is gone. `Ok(None)` means "binding was stale; retry acquire".
    async fn bound_sandbox(
        &self,
        tenant: &TenantKey,
        binding: &Binding,
    ) -> Result<Option<Sandbox>> {
        match self.cp.get(&binding.microvm_id).await {
            Ok(vm) if vm.is_live() => {
                match wait_until_running(&*self.cp, &binding.microvm_id, &self.cfg.wait).await {
                    Ok(running) => Ok(Some(self.sandbox(tenant.clone(), running)?)),
                    Err(e) => {
                        // A bound VM that never becomes ready would
                        // wedge the tenant — every future acquire hits
                        // the same binding — and holds its capacity
                        // forever. Run the same release-or-track
                        // cleanup a fresh-claim failure gets.
                        self.wait_fail_cleanup(tenant, binding.microvm_id.clone(), vm);
                        Err(e)
                    }
                }
            }
            Ok(_) | Err(Error::NotFound { .. }) => {
                // Scoped release only fires if the binding is still the
                // one we observed — a fresher re-bind survives.
                self.store.release(tenant, &binding.microvm_id).await?;
                self.tokens.invalidate(&binding.microvm_id);
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    fn sandbox(&self, tenant: TenantKey, running: RunningVm) -> Result<Sandbox> {
        // `new` re-validates the endpoint URL; `with_port`+`with_scope`
        // were already validated by `PoolConfig::validate` — a failure
        // here means the service handed us a malformed endpoint, which
        // propagates as a normal error.
        let endpoint = if self.cfg.allow_insecure_endpoints {
            MicrovmEndpoint::new_insecure(&running, self.tokens.clone())?
        } else {
            MicrovmEndpoint::new(&running, self.tokens.clone())?
        }
        .with_port(self.cfg.app_port)?
        .with_scope(self.cfg.token_scope.clone())?
        .with_client(self.http.clone());
        Ok(Sandbox {
            tenant,
            vm: running,
            endpoint,
            cp: self.cp.clone(),
            store: self.store.clone(),
            tokens: self.tokens.clone(),
        })
    }
}

impl Drop for SandboxPool {
    fn drop(&mut self) {
        self.abort_reapers();
    }
}
