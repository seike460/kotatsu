//! Tests for `StateStore` (memory + SQLite) and `SandboxPool`,
//! all against `MockControlPlane`.

use kotatsu::mock::MockControlPlane;
use kotatsu::{
    Binding, ClaimOutcome, ControlPlane, Error, MemoryStore, PoolConfig, PortSpec, RunRequest,
    SandboxPool, State, StateStore, TenantKey, WaitPolicy, WarmWindow,
};
use std::sync::Arc;
use std::time::Duration;

/// Pool image for tests that enable the lost-VM reconcile, which
/// requires the image ARN (`PoolConfig::reap_lost_vms`).
const IMG_ARN: &str = "arn:aws:lambda:us-east-1:123456789012:microvm-image:img";

fn tenant(s: &str) -> TenantKey {
    TenantKey::new(s).unwrap()
}

fn binding(t: &str, vm: &str) -> Binding {
    Binding {
        tenant: tenant(t),
        microvm_id: kotatsu::MicrovmId::new(vm).unwrap(),
        claimed_at_secs: 1_700_000_000,
        sentinel: false,
    }
}

async fn assert_store_contract(store: &(impl StateStore + ?Sized)) {
    assert!(store.get(&tenant("a")).await.unwrap().is_none());

    let b = binding("a", "microvm-1");
    assert_eq!(store.claim(&b).await.unwrap(), ClaimOutcome::Claimed);
    assert_eq!(store.get(&tenant("a")).await.unwrap().unwrap(), b);

    // put-if-absent: a different VM must not steal the binding.
    let other = binding("a", "microvm-2");
    match store.claim(&other).await.unwrap() {
        ClaimOutcome::HeldByOther(existing) => assert_eq!(existing, b),
        ClaimOutcome::Claimed => panic!("claim overwrote an existing binding"),
    }
    assert_eq!(store.get(&tenant("a")).await.unwrap().unwrap(), b);

    // A different tenant claims independently.
    assert_eq!(
        store.claim(&binding("b", "microvm-3")).await.unwrap(),
        ClaimOutcome::Claimed
    );
    assert_eq!(store.list().await.unwrap().len(), 2);

    // Scoped release: wrong expected id must not delete the binding.
    let wrong_vm = kotatsu::MicrovmId::new("microvm-wrong").unwrap();
    assert!(!store.release(&tenant("a"), &wrong_vm).await.unwrap());
    assert!(store.get(&tenant("a")).await.unwrap().is_some());

    let right_vm = kotatsu::MicrovmId::new("microvm-1").unwrap();
    assert!(store.release(&tenant("a"), &right_vm).await.unwrap());
    assert!(!store.release(&tenant("a"), &right_vm).await.unwrap());
    assert!(store.get(&tenant("a")).await.unwrap().is_none());
    assert_eq!(store.list().await.unwrap().len(), 1);
}

#[tokio::test]
async fn memory_store_contract() {
    assert_store_contract(&MemoryStore::new()).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_store_contract() {
    let store = kotatsu::SqliteStore::open(":memory:").await.unwrap();
    assert_store_contract(&store).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_store_persists_across_connections() {
    let dir = std::env::temp_dir().join(format!("kotatsu-test-{}", uuid::Uuid::new_v4()));
    let path = dir.join("state.db");
    std::fs::create_dir_all(&dir).unwrap();
    {
        let store = kotatsu::SqliteStore::open(&path).await.unwrap();
        store.claim(&binding("persist", "microvm-9")).await.unwrap();
    }
    {
        let store = kotatsu::SqliteStore::open(&path).await.unwrap();
        let b = store.get(&tenant("persist")).await.unwrap().unwrap();
        assert_eq!(b.microvm_id.as_str(), "microvm-9");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

fn test_pool(cp: Arc<MockControlPlane>, warm_size: usize, max_vms: usize) -> SandboxPool {
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = warm_size;
    cfg.max_vms = max_vms;
    cfg.wait = WaitPolicy {
        timeout: Duration::from_secs(5),
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(50),
    };
    SandboxPool::new(cp, Arc::new(MemoryStore::new()), cfg).unwrap()
}

#[tokio::test]
async fn acquire_creates_vm_and_binding() {
    let cp = Arc::new(MockControlPlane::new());
    let pool = test_pool(cp.clone(), 0, 10);
    let sb = pool.acquire(&tenant("u1")).await.unwrap();
    assert_eq!(sb.vm().microvm().state, State::Running);
    assert_eq!(sb.endpoint().microvm_id(), sb.vm().id());
    assert!(cp.get(sb.vm().id()).await.unwrap().is_live());
}

#[tokio::test]
async fn reacquire_lands_on_same_vm() {
    let cp = Arc::new(MockControlPlane::new());
    let pool = test_pool(cp, 0, 10);
    let first = pool.acquire(&tenant("u1")).await.unwrap();
    let id = first.vm().id().clone();
    drop(first); // drop keeps the binding
    let second = pool.acquire(&tenant("u1")).await.unwrap();
    assert_eq!(second.vm().id(), &id);
}

#[tokio::test]
async fn maintain_warms_then_acquire_consumes() {
    let cp = Arc::new(MockControlPlane::new());
    let pool = test_pool(cp, 2, 10);
    let report = pool.maintain().await.unwrap();
    assert_eq!(report.warmed, 2);
    assert_eq!(pool.stats().await.warm, 2);

    let _ = pool.acquire(&tenant("u1")).await.unwrap();
    assert_eq!(pool.stats().await.warm, 1);

    // Next tick tops back up.
    let report = pool.maintain().await.unwrap();
    assert_eq!(report.warmed, 1);
    assert_eq!(pool.stats().await.warm, 2);
}

#[tokio::test]
async fn spawned_maintenance_ticks_before_the_first_interval() {
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 2;
    cfg.maintenance_interval = Duration::from_secs(3600);
    let pool = Arc::new(
        SandboxPool::new(
            Arc::new(MockControlPlane::new()),
            Arc::new(MemoryStore::new()),
            cfg,
        )
        .unwrap(),
    );
    let task = pool.spawn_maintenance();
    let warmed = tokio::time::timeout(Duration::from_secs(5), async {
        while pool.stats().await.warm < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    task.abort();
    assert!(warmed.is_ok(), "the warm set waited for the first interval");
}

#[tokio::test]
async fn suspend_then_reacquire_resumes() {
    let cp = Arc::new(MockControlPlane::new());
    let pool = test_pool(cp.clone(), 0, 10);
    let sb = pool.acquire(&tenant("u1")).await.unwrap();
    let id = sb.vm().id().clone();
    sb.suspend().await.unwrap();
    assert_eq!(cp.get(&id).await.unwrap().state, State::Suspended);

    let sb2 = pool.acquire(&tenant("u1")).await.unwrap();
    assert_eq!(sb2.vm().id(), &id);
    assert_eq!(cp.get(&id).await.unwrap().state, State::Running);
}

#[tokio::test]
async fn dead_binding_is_reaped_and_replaced() {
    let cp = Arc::new(MockControlPlane::new());
    let pool = test_pool(cp.clone(), 0, 10);
    let sb = pool.acquire(&tenant("u1")).await.unwrap();
    let old = sb.vm().id().clone();
    drop(sb);
    // VM dies outside the pool's control.
    cp.terminate(&old).await.unwrap();

    // Without maintain, acquire self-heals through the binding path.
    let sb = pool.acquire(&tenant("u1")).await.unwrap();
    assert_ne!(sb.vm().id(), &old);
}

#[tokio::test]
async fn maintain_drops_dead_bindings() {
    let cp = Arc::new(MockControlPlane::new());
    let pool = test_pool(cp.clone(), 0, 10);
    let sb = pool.acquire(&tenant("u1")).await.unwrap();
    let id = sb.vm().id().clone();
    drop(sb);
    cp.terminate(&id).await.unwrap();

    let report = pool.maintain().await.unwrap();
    assert_eq!(report.bindings_dropped, 1);
    assert_eq!(pool.stats().await.assigned, 0);
}

#[tokio::test]
async fn capacity_cap_returns_pool_exhausted() {
    let cp = Arc::new(MockControlPlane::new());
    let pool = test_pool(cp, 0, 1);
    let _ = pool.acquire(&tenant("u1")).await.unwrap();
    let err = pool.acquire(&tenant("u2")).await.unwrap_err();
    assert!(matches!(err, Error::PoolExhausted(_)), "got {err:?}");
}

#[tokio::test]
async fn max_age_reaps_bound_vms() {
    let cp = Arc::new(MockControlPlane::new());
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.max_age = Some(Duration::from_secs(1));
    let pool = SandboxPool::new(cp.clone(), Arc::new(MemoryStore::new()), cfg).unwrap();

    let sb = pool.acquire(&tenant("u1")).await.unwrap();
    let id = sb.vm().id().clone();
    drop(sb);

    // The mock reports `started_at_secs = now`, so with max_age=1s the
    // fresh VM is NOT reaped…
    let report = pool.maintain().await.unwrap();
    assert_eq!(report.reaped, 0);
    assert!(cp.get(&id).await.unwrap().is_live());

    // …but it IS reaped once the clock moves past max_age. maintain_at
    // makes that deterministic instead of sleeping.
    let future = chrono::Utc::now().timestamp() + 10;
    let report = pool.maintain_at(future).await.unwrap();
    assert_eq!(report.reaped, 1);
    assert_eq!(cp.get(&id).await.unwrap().state, State::Terminated);
}

#[test]
fn warm_window_validation_and_target() {
    assert!(WarmWindow::new(1440, 1500, 1).is_err());
    assert!(WarmWindow::new(0, 1441, 1).is_err());
    assert!(WarmWindow::new(0, 1440, 1).is_ok());

    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 1;
    cfg.warm_schedule = vec![
        WarmWindow::new(9 * 60, 18 * 60, 8).unwrap(), // 09:00–18:00 → 8
        WarmWindow::new(22 * 60, 6 * 60, 4).unwrap(), // 22:00–06:00 wraps → 4
    ];
    assert_eq!(cfg.warm_target(10 * 3600), 8); // 10:00 UTC
    assert_eq!(cfg.warm_target(18 * 3600), 1); // 18:00 is the exclusive end
    assert_eq!(cfg.warm_target(23 * 3600), 4); // inside wrap window
    assert_eq!(cfg.warm_target(3 * 3600), 4); // wrap, early morning
    assert_eq!(cfg.warm_target(20 * 3600), 1); // gap → warm_size fallback
    // Equal bounds = all day.
    cfg.warm_schedule = vec![WarmWindow::new(0, 0, 7).unwrap()];
    assert_eq!(cfg.warm_target(0), 7);
    assert_eq!(cfg.warm_target(12 * 3600), 7);
    // First match wins.
    cfg.warm_schedule = vec![
        WarmWindow::new(0, 1440, 3).unwrap(),
        WarmWindow::new(0, 0, 9).unwrap(),
    ];
    assert_eq!(cfg.warm_target(5 * 3600), 3);
    // A schedule entry exceeding max_vms is rejected at build time.
    cfg.max_vms = 2;
    cfg.warm_schedule = vec![WarmWindow::new(0, 1440, 5).unwrap()];
    assert!(
        SandboxPool::new(
            Arc::new(MockControlPlane::new()),
            Arc::new(MemoryStore::new()),
            cfg
        )
        .is_err()
    );
}

#[tokio::test]
async fn warm_schedule_scales_down_between_ticks() {
    let cp = Arc::new(MockControlPlane::new());
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.warm_schedule = vec![
        WarmWindow::new(60, 120, 2).unwrap(),  // 01:00–02:00 UTC → 2
        WarmWindow::new(180, 240, 0).unwrap(), // 03:00–04:00 UTC → 0
    ];
    cfg.wait = WaitPolicy {
        timeout: Duration::from_secs(5),
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(50),
    };
    let pool = SandboxPool::new(cp.clone(), Arc::new(MemoryStore::new()), cfg).unwrap();

    let report = pool.maintain_at(90 * 60).await.unwrap(); // 01:30
    assert_eq!(report.warmed, 2);
    assert_eq!(pool.stats().await.warm, 2);

    // 03:30 — target drops to 0; excess warm VMs are terminated, not idled.
    let report = pool.maintain_at(210 * 60).await.unwrap();
    assert_eq!(report.shrunk, 2);
    assert_eq!(pool.stats().await.warm, 0);
    assert!(
        cp.list(None, None)
            .await
            .unwrap()
            .iter()
            .all(|s| s.state == State::Terminated)
    );
}

#[tokio::test]
async fn shrink_failure_restores_vm_to_warm() {
    // Seed warm VMs via a schedule that first wants 2, then 0 — the
    // failing-terminate tick must push the VM back, not leak it.
    let cp = Arc::new(MockControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            terminate_error: Some("boom".into()),
            ..Default::default()
        },
    ));
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.warm_schedule = vec![
        WarmWindow::new(60, 120, 2).unwrap(),
        WarmWindow::new(180, 240, 0).unwrap(),
    ];
    cfg.wait = WaitPolicy {
        timeout: Duration::from_secs(5),
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(50),
    };
    let pool = SandboxPool::new(cp.clone(), Arc::new(MemoryStore::new()), cfg).unwrap();
    let report = pool.maintain_at(90 * 60).await.unwrap();
    assert_eq!(report.warmed, 2);

    let report = pool.maintain_at(210 * 60).await.unwrap();
    assert_eq!(report.shrunk, 0);
    // Both VMs return to warm; inflight is fully released (no leak).
    let stats = pool.stats().await;
    assert_eq!(stats.warm, 2);
    assert_eq!(stats.inflight, 0);
    // And the VMs are still live — terminate never landed.
    assert!(
        cp.list(None, None)
            .await
            .unwrap()
            .iter()
            .all(|s| s.state == State::Running)
    );
}

#[tokio::test]
async fn release_terminates_and_clears_binding() {
    let cp = Arc::new(MockControlPlane::new());
    let pool = test_pool(cp.clone(), 0, 10);
    let sb = pool.acquire(&tenant("u1")).await.unwrap();
    let id = sb.vm().id().clone();
    sb.release().await.unwrap();
    assert_eq!(cp.get(&id).await.unwrap().state, State::Terminated);
    // Re-acquire starts fresh on a different VM.
    let sb2 = pool.acquire(&tenant("u1")).await.unwrap();
    assert_ne!(sb2.vm().id().as_str(), id.as_str());
}

#[tokio::test]
async fn drain_clears_everything() {
    let cp = Arc::new(MockControlPlane::new());
    let pool = test_pool(cp.clone(), 2, 10);
    pool.maintain().await.unwrap();
    let sb = pool.acquire(&tenant("u1")).await.unwrap();
    let assigned_id = sb.vm().id().clone();
    drop(sb);

    pool.drain().await.unwrap();
    assert_eq!(pool.stats().await.warm, 0);
    assert_eq!(pool.stats().await.assigned, 0);
    assert_eq!(cp.get(&assigned_id).await.unwrap().state, State::Terminated);
}

#[tokio::test]
async fn concurrent_acquire_converges_on_one_vm() {
    let cp = Arc::new(MockControlPlane::new());
    let pool = Arc::new(test_pool(cp, 0, 10));
    let t = tenant("shared");
    let (a, b) = tokio::join!(pool.acquire(&t), pool.acquire(&t));
    let a = a.unwrap();
    let b = b.unwrap();
    assert_eq!(a.vm().id(), b.vm().id());
}

#[tokio::test]
async fn concurrent_acquires_never_exceed_cap() {
    // Regression for the check_capacity→run TOCTOU: N concurrent
    // first-time acquires against a small cap must not over-launch.
    let cp = Arc::new(MockControlPlane::new());
    let pool = Arc::new(test_pool(cp, 0, 2));
    let results = futures_util::future::join_all((0..4).map(|i| {
        let pool = pool.clone();
        async move { pool.acquire(&tenant(&format!("t{i}"))).await }
    }))
    .await;
    let ok = results.iter().filter(|r| r.is_ok()).count();
    let exhausted = results
        .iter()
        .filter(|r| matches!(r, Err(Error::PoolExhausted(_))))
        .count();
    assert_eq!(ok, 2, "expected exactly 2 VMs under cap, got {ok}");
    assert_eq!(exhausted, 2, "expected 2 PoolExhausted, got {exhausted}");
    // And the accounting agrees.
    let stats = pool.stats().await;
    assert_eq!(stats.assigned, 2);
    assert_eq!(stats.inflight, 0);
}

#[tokio::test]
async fn default_token_scope_is_app_port_only() {
    // Least privilege: the handed-out token must not default to AllPorts.
    let cfg = PoolConfig::new(RunRequest::new("img"));
    assert_eq!(cfg.token_scope, vec![PortSpec::Port(8080)]);
    assert!(
        !cfg.token_scope.contains(&PortSpec::All),
        "default scope must not be AllPorts"
    );
}

#[tokio::test]
async fn release_keeps_binding_when_terminate_fails() {
    let cp = Arc::new(MockControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            terminate_error: Some("boom".into()),
            ..Default::default()
        },
    ));
    let store = Arc::new(MemoryStore::new());
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.wait = WaitPolicy {
        timeout: Duration::from_secs(5),
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(50),
    };
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    let sb = pool.acquire(&tenant("u1")).await.unwrap();
    let id = sb.vm().id().clone();
    // terminate fails → release must fail AND keep the binding so the
    // still-running VM stays tracked.
    assert!(sb.release().await.is_err());
    assert!(store.get(&tenant("u1")).await.unwrap().is_some());
    assert_eq!(cp.get(&id).await.unwrap().state, State::Running);
}

#[tokio::test]
async fn drain_release_failure_is_best_effort() {
    // The VM is terminated but its binding release fails: drain still
    // returns Ok, and the binding left pointing at the dead VM is
    // dropped by the next maintain.
    let cp = Arc::new(MockControlPlane::new());
    let store = Arc::new(FlakyStore::new());
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();
    let sb = pool.acquire(&tenant("u1")).await.unwrap();
    let id = sb.vm().id().clone();
    drop(sb);

    store
        .fail_release
        .store(true, std::sync::atomic::Ordering::SeqCst);
    pool.drain().await.unwrap();
    assert_eq!(cp.get(&id).await.unwrap().state, State::Terminated);
    assert!(store.get(&tenant("u1")).await.unwrap().is_some());

    store
        .fail_release
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let report = pool.maintain().await.unwrap();
    assert_eq!(report.bindings_dropped, 1);
    assert!(store.get(&tenant("u1")).await.unwrap().is_none());
}

#[tokio::test]
async fn drain_keeps_failed_terminates_tracked() {
    let cp = Arc::new(MockControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            terminate_error: Some("boom".into()),
            ..Default::default()
        },
    ));
    let store = Arc::new(MemoryStore::new());
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 2;
    cfg.max_vms = 10;
    cfg.wait = WaitPolicy {
        timeout: Duration::from_secs(5),
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(50),
    };
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();
    pool.maintain().await.unwrap();
    let sb = pool.acquire(&tenant("u1")).await.unwrap();
    drop(sb);

    pool.drain().await.unwrap();
    // terminate failed for everything: the warm VM is back in `warm`
    // and the binding survives — nothing is left untracked.
    let stats = pool.stats().await;
    assert_eq!(stats.warm, 1);
    assert_eq!(stats.assigned, 1);
    assert!(
        cp.list(None, None)
            .await
            .unwrap()
            .iter()
            .all(|s| s.state == State::Running)
    );
}

#[tokio::test]
async fn aborted_launch_vm_is_adopted_into_warm() {
    // A `maintain` aborted while `run` is in flight must not leak the
    // VM: the launch task resolves into `warm` via the Adopt guard.
    let gate = Arc::new(tokio::sync::Notify::new());
    let cp = Arc::new(MockControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            run_gate: Some(gate.clone()),
            ..Default::default()
        },
    ));
    let pool = Arc::new(test_pool(cp.clone(), 1, 10));

    let task = tokio::spawn({
        let pool = pool.clone();
        async move { pool.maintain().await }
    });
    // Wait until the launch is actually in flight (inflight slot taken).
    tokio::time::timeout(Duration::from_secs(2), async {
        while pool.stats().await.inflight == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("launch never started");
    task.abort();
    gate.notify_one();

    // The detached Adopt task parks the finished VM into warm.
    tokio::time::timeout(Duration::from_secs(2), async {
        while pool.stats().await.warm == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("aborted launch VM was not adopted into warm");
    assert_eq!(pool.stats().await.warm, 1);
}

/// Store wrapper that blocks the *first* `claim`/`release` until
/// notified — lets tests cancel an acquire while a store call is
/// genuinely in flight. Subsequent calls pass through so later
/// assertions can still use the store.
struct GatedStore {
    inner: MemoryStore,
    claim_gate: tokio::sync::Notify,
    release_gate: tokio::sync::Notify,
    /// Fires once the first gated call is inside the gate — lets tests
    /// abort deterministically mid-await instead of racing the entry.
    claim_entered: tokio::sync::Notify,
    release_entered: tokio::sync::Notify,
    gate_claims: std::sync::atomic::AtomicBool,
    gate_releases: std::sync::atomic::AtomicBool,
}

impl GatedStore {
    fn claims() -> Self {
        Self {
            inner: MemoryStore::new(),
            claim_gate: tokio::sync::Notify::new(),
            release_gate: tokio::sync::Notify::new(),
            claim_entered: tokio::sync::Notify::new(),
            release_entered: tokio::sync::Notify::new(),
            gate_claims: std::sync::atomic::AtomicBool::new(true),
            gate_releases: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn releases() -> Self {
        Self {
            gate_claims: std::sync::atomic::AtomicBool::new(false),
            gate_releases: std::sync::atomic::AtomicBool::new(true),
            ..Self::claims()
        }
    }
}

#[async_trait::async_trait]
impl StateStore for GatedStore {
    async fn get(&self, t: &TenantKey) -> kotatsu::Result<Option<Binding>> {
        self.inner.get(t).await
    }
    async fn claim(&self, b: &Binding) -> kotatsu::Result<ClaimOutcome> {
        if self
            .gate_claims
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.claim_entered.notify_one();
            self.claim_gate.notified().await;
        }
        self.inner.claim(b).await
    }
    async fn release(&self, t: &TenantKey, e: &kotatsu::MicrovmId) -> kotatsu::Result<bool> {
        if self
            .gate_releases
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.release_entered.notify_one();
            self.release_gate.notified().await;
        }
        self.inner.release(t, e).await
    }
    async fn list(&self) -> kotatsu::Result<Vec<Binding>> {
        self.inner.list().await
    }
}

/// Store wrapper whose `claim`/`get`/`release` can each be made to
/// fail — covering indeterminate outcomes: a write may land or not
/// before its call reports `Err`, exactly like a connection lost
/// mid-flight. Wraps any `StateStore` — including `SqliteStore` for
/// durability-across-restart tests.
struct FlakyStore {
    inner: Arc<dyn StateStore>,
    /// `claim` calls still to fail — lets an early claim fail while
    /// later ones (e.g. a sentinel pin) succeed. `claim_lands` chooses
    /// whether failed claims apply the INSERT first (lost reply after
    /// commit) or not (failure before write).
    claim_failures: std::sync::atomic::AtomicUsize,
    claim_lands: std::sync::atomic::AtomicBool,
    /// `get` reports `Err` — a store outage during verification.
    fail_get: std::sync::atomic::AtomicBool,
    /// `get` calls that still succeed — lets the acquire's own lookup
    /// pass while later verification gets fail.
    get_ok_budget: std::sync::atomic::AtomicUsize,
    /// Total `get` entries — a task's acquire path always starts with
    /// `get`, so reaching `base+1` proves it is inside `acquire`.
    get_calls: std::sync::atomic::AtomicUsize,
    /// Total `list` entries — `try_reserve` lists inside the capacity
    /// lock, so a flat counter while a call holds the lock proves the
    /// racing task never got past it.
    list_calls: std::sync::atomic::AtomicUsize,
    /// `release` reports `Err`. `release_lands` chooses whether the
    /// DELETE still applies first (lost reply after commit) or not
    /// (failure before write — the binding survives).
    fail_release: std::sync::atomic::AtomicBool,
    release_lands: std::sync::atomic::AtomicBool,
    /// Parks inside `release` until `release_gate` fires — the test
    /// observes entry via `release_entered`. Consumed on first use.
    gate_release: std::sync::atomic::AtomicBool,
    release_entered: tokio::sync::Notify,
    release_gate: tokio::sync::Notify,
    /// Parks inside `claim` for a *sentinel* binding until
    /// `sentinel_claim_gate` fires — lets a test write a colliding row
    /// or arm a failure between pin entry and the store op. Normal
    /// claims pass through. Only the `sentinel_claim_ordinal`-th
    /// (1-based) sentinel claim parks; consumed on first use.
    gate_sentinel_claim: std::sync::atomic::AtomicBool,
    sentinel_claim_ordinal: std::sync::atomic::AtomicUsize,
    sentinel_claims_seen: std::sync::atomic::AtomicUsize,
    sentinel_claim_entered: tokio::sync::Notify,
    sentinel_claim_gate: tokio::sync::Notify,
    /// Parks inside `release` for a *sentinel-tenant* binding until
    /// `marker_release_gate` fires — lets a test interleave a cleanup
    /// between `maintain`'s stale-marker decision and its delete.
    /// Consumed on first use.
    gate_marker_release: std::sync::atomic::AtomicBool,
    marker_release_entered: tokio::sync::Notify,
    marker_release_gate: tokio::sync::Notify,
}

impl FlakyStore {
    fn new() -> Self {
        Self::wrapping(Arc::new(MemoryStore::new()))
    }

    fn wrapping(inner: Arc<dyn StateStore>) -> Self {
        Self {
            inner,
            claim_failures: std::sync::atomic::AtomicUsize::new(0),
            claim_lands: std::sync::atomic::AtomicBool::new(false),
            fail_get: std::sync::atomic::AtomicBool::new(false),
            get_ok_budget: std::sync::atomic::AtomicUsize::new(0),
            get_calls: std::sync::atomic::AtomicUsize::new(0),
            list_calls: std::sync::atomic::AtomicUsize::new(0),
            fail_release: std::sync::atomic::AtomicBool::new(false),
            release_lands: std::sync::atomic::AtomicBool::new(false),
            gate_release: std::sync::atomic::AtomicBool::new(false),
            release_entered: tokio::sync::Notify::new(),
            release_gate: tokio::sync::Notify::new(),
            gate_sentinel_claim: std::sync::atomic::AtomicBool::new(false),
            sentinel_claim_ordinal: std::sync::atomic::AtomicUsize::new(1),
            sentinel_claims_seen: std::sync::atomic::AtomicUsize::new(0),
            sentinel_claim_entered: tokio::sync::Notify::new(),
            sentinel_claim_gate: tokio::sync::Notify::new(),
            gate_marker_release: std::sync::atomic::AtomicBool::new(false),
            marker_release_entered: tokio::sync::Notify::new(),
            marker_release_gate: tokio::sync::Notify::new(),
        }
    }

    fn fail(&self) -> kotatsu::Error {
        kotatsu::Error::Store("simulated store failure".into())
    }
}

#[async_trait::async_trait]
impl StateStore for FlakyStore {
    async fn get(&self, t: &TenantKey) -> kotatsu::Result<Option<Binding>> {
        self.get_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.fail_get.load(std::sync::atomic::Ordering::SeqCst) {
            let spent = self
                .get_ok_budget
                .fetch_update(
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                    |n| n.checked_sub(1),
                )
                .is_err();
            if spent {
                return Err(self.fail());
            }
        }
        self.inner.get(t).await
    }
    async fn claim(&self, b: &Binding) -> kotatsu::Result<ClaimOutcome> {
        if b.sentinel {
            let n = self
                .sentinel_claims_seen
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            if n == self
                .sentinel_claim_ordinal
                .load(std::sync::atomic::Ordering::SeqCst)
                && self
                    .gate_sentinel_claim
                    .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                self.sentinel_claim_entered.notify_one();
                self.sentinel_claim_gate.notified().await;
            }
        }
        if self
            .claim_failures
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |n| n.checked_sub(1),
            )
            .is_ok()
        {
            if self.claim_lands.load(std::sync::atomic::Ordering::SeqCst) {
                let _ = self.inner.claim(b).await?;
            }
            return Err(self.fail());
        }
        self.inner.claim(b).await
    }
    async fn release(&self, t: &TenantKey, e: &kotatsu::MicrovmId) -> kotatsu::Result<bool> {
        if t.as_str().starts_with("~kotatsu-lost~")
            && self
                .gate_marker_release
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.marker_release_entered.notify_one();
            self.marker_release_gate.notified().await;
        }
        if self
            .gate_release
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.release_entered.notify_one();
            self.release_gate.notified().await;
        }
        if self.fail_release.load(std::sync::atomic::Ordering::SeqCst) {
            if self.release_lands.load(std::sync::atomic::Ordering::SeqCst) {
                let _ = self.inner.release(t, e).await?;
            }
            return Err(self.fail());
        }
        self.inner.release(t, e).await
    }
    async fn list(&self) -> kotatsu::Result<Vec<Binding>> {
        self.list_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.list().await
    }
}

/// Control-plane wrapper whose `terminate` can be toggled to fail and
/// whose `get` can be parked — for tests that need a mid-flight outage
/// or a deterministic interleave inside cleanup.
struct FlakyControlPlane {
    inner: MockControlPlane,
    /// `terminate` reports `Err` while set — a mid-flight outage the
    /// test can lift at will.
    fail_terminate: std::sync::atomic::AtomicBool,
    /// Total `terminate` calls — lets tests prove a failure *and* a
    /// retry happened before recovering the plane.
    terminate_calls: std::sync::atomic::AtomicUsize,
    /// `get` parks on `get_gate` while set; `get_entered` fires on
    /// arrival so the test can act during the park.
    gate_gets: std::sync::atomic::AtomicBool,
    get_entered: tokio::sync::Notify,
    get_gate: tokio::sync::Notify,
    /// `list` calls — `try_reserve` lists inside the capacity lock,
    /// so a test can prove a task is (not) past the lock.
    list_calls: std::sync::atomic::AtomicUsize,
}

impl FlakyControlPlane {
    fn new() -> Self {
        Self::with_behavior(kotatsu::mock::MockBehavior::default())
    }

    fn with_behavior(behavior: kotatsu::mock::MockBehavior) -> Self {
        Self {
            inner: MockControlPlane::with_behavior(behavior),
            fail_terminate: std::sync::atomic::AtomicBool::new(false),
            terminate_calls: std::sync::atomic::AtomicUsize::new(0),
            gate_gets: std::sync::atomic::AtomicBool::new(false),
            get_entered: tokio::sync::Notify::new(),
            get_gate: tokio::sync::Notify::new(),
            list_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

#[async_trait::async_trait]
impl ControlPlane for FlakyControlPlane {
    async fn run(&self, req: &RunRequest) -> kotatsu::Result<kotatsu::Microvm> {
        self.inner.run(req).await
    }
    async fn get(&self, id: &kotatsu::MicrovmId) -> kotatsu::Result<kotatsu::Microvm> {
        if self.gate_gets.load(std::sync::atomic::Ordering::SeqCst) {
            self.get_entered.notify_one();
            self.get_gate.notified().await;
        }
        self.inner.get(id).await
    }
    async fn suspend(&self, id: &kotatsu::MicrovmId) -> kotatsu::Result<()> {
        self.inner.suspend(id).await
    }
    async fn resume(&self, id: &kotatsu::MicrovmId) -> kotatsu::Result<()> {
        self.inner.resume(id).await
    }
    async fn terminate(&self, id: &kotatsu::MicrovmId) -> kotatsu::Result<()> {
        self.terminate_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self
            .fail_terminate
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(kotatsu::Error::Other("simulated terminate failure".into()));
        }
        self.inner.terminate(id).await
    }
    async fn list(
        &self,
        image_identifier: Option<&str>,
        image_version: Option<&str>,
    ) -> kotatsu::Result<Vec<kotatsu::MicrovmSummary>> {
        self.list_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.list(image_identifier, image_version).await
    }
    async fn mint_token(
        &self,
        id: &kotatsu::MicrovmId,
        scope: &[PortSpec],
        ttl_minutes: i32,
    ) -> kotatsu::Result<kotatsu::AuthToken> {
        self.inner.mint_token(id, scope, ttl_minutes).await
    }
    async fn mint_shell_token(
        &self,
        id: &kotatsu::MicrovmId,
        ttl_minutes: i32,
    ) -> kotatsu::Result<kotatsu::AuthToken> {
        self.inner.mint_shell_token(id, ttl_minutes).await
    }
}

#[tokio::test]
async fn claim_error_that_landed_keeps_vm_bound() {
    // The INSERT applied but the claim reported failure — the pool
    // must discover the binding and keep the VM out of `warm`, or a
    // second tenant could be handed a bound VM.
    let cp = Arc::new(MockControlPlane::new());
    let store = Arc::new(FlakyStore::new());
    store
        .claim_lands
        .store(true, std::sync::atomic::Ordering::SeqCst);
    store
        .claim_failures
        .store(usize::MAX, std::sync::atomic::Ordering::SeqCst);
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.wait = WaitPolicy {
        timeout: Duration::from_secs(5),
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(50),
    };
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    let t1 = tenant("u1");
    assert!(pool.acquire(&t1).await.is_err());
    let bound_id = store.get(&t1).await.unwrap().unwrap().microvm_id;
    let stats = pool.stats().await;
    assert_eq!(stats.warm, 0, "a landed claim must not re-warm the VM");
    assert_eq!(stats.inflight, 0);
    assert_eq!(stats.assigned, 1, "the binding owns the VM");

    // A second tenant's claim also "fails" — but lands its own
    // binding, never a share of u1's VM.
    let t2 = tenant("u2");
    assert!(pool.acquire(&t2).await.is_err());
    let u2_id = store.get(&t2).await.unwrap().unwrap().microvm_id;
    assert_ne!(u2_id.as_str(), bound_id.as_str());

    // Each tenant's next acquire lands on its own bound VM.
    let sb1 = pool.acquire(&t1).await.unwrap();
    assert_eq!(sb1.vm().id(), &bound_id);
    let sb2 = pool.acquire(&t2).await.unwrap();
    assert_eq!(sb2.vm().id(), &u2_id);
}

#[tokio::test]
async fn claim_error_that_never_landed_rewarms_vm() {
    // claim `Err` + `get` `Ok(None)` PROVES the VM is unbound — it
    // must return to warm, not be dropped as untracked.
    let cp = Arc::new(MockControlPlane::new());
    let store = Arc::new(FlakyStore::new());
    store
        .claim_failures
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    assert!(pool.acquire(&tenant("u1")).await.is_err());
    let stats = pool.stats().await;
    assert_eq!(stats.warm, 1, "an unbound VM must re-warm");
    assert_eq!(stats.inflight, 0);
    assert_eq!(stats.assigned, 0);

    // With the failure budget spent, the re-warmed VM is claimable.
    let vm_id = cp.list(None, None).await.unwrap()[0].id.clone();
    let sb = pool.acquire(&tenant("u2")).await.unwrap();
    assert_eq!(sb.vm().id(), &vm_id);
}

#[tokio::test]
async fn indeterminate_claim_outcome_terminates_vm() {
    // claim `Err` + `get` `Err` cannot prove ownership either way —
    // the VM is terminated so it can be neither shared nor leaked:
    // bound → dead binding self-heals; unbound → simply gone.
    let cp = Arc::new(MockControlPlane::new());
    let store = Arc::new(FlakyStore::new());
    for f in [&store.claim_lands, &store.fail_get] {
        f.store(true, std::sync::atomic::Ordering::SeqCst);
    }
    store
        .claim_failures
        .store(1, std::sync::atomic::Ordering::SeqCst);
    // The acquire's own `get` passes; the claim task's verification
    // `get` fails — the outcome is truly indeterminate.
    store
        .get_ok_budget
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    assert!(pool.acquire(&tenant("u1")).await.is_err());
    // The detached terminator destroys the VM under an inflight slot —
    // wait for the VM to die AND the slot to free, since `inflight`
    // alone can read zero before the terminator starts.
    let dead = cp.list(None, None).await.unwrap()[0].id.clone();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let dead_now = cp.get(&dead).await.map(|v| !v.is_live()).unwrap_or(false);
            if dead_now && pool.stats().await.inflight == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("terminator never settled");
    let stats = pool.stats().await;
    assert_eq!(stats.warm, 0);
    assert_eq!(stats.inflight, 0);
    assert_eq!(cp.get(&dead).await.unwrap().state, State::Terminated);

    // The store recovers; a new tenant gets a fresh VM.
    store
        .fail_get
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let sb = pool.acquire(&tenant("u2")).await.unwrap();
    assert_ne!(sb.vm().id(), &dead);
}

#[tokio::test]
async fn bound_vm_surviving_failed_release_stays_tracked() {
    // `store.release` fails but the binding survives, and `terminate`
    // also fails: the VM must remain bound and tracked — never warm,
    // never dropped.
    let cp = Arc::new(FlakyControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            boot_time: Duration::from_secs(3600), // wait fails → cleanup
            ..Default::default()
        },
    ));
    cp.fail_terminate
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let store = Arc::new(FlakyStore::new());
    store
        .fail_release
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.wait = WaitPolicy {
        timeout: Duration::from_millis(100),
        initial_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(10),
    };
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    let t1 = tenant("u1");
    assert!(pool.acquire(&t1).await.is_err());
    // The cleanup runs detached; its failed terminate is its last step.
    tokio::time::timeout(Duration::from_secs(2), async {
        while cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("cleanup never tried to terminate the bound VM");
    let bound = store.get(&t1).await.unwrap().unwrap();
    let stats = pool.stats().await;
    assert_eq!(stats.warm, 0, "a bound VM must never be warm");
    assert_eq!(stats.inflight, 0);
    assert_eq!(stats.assigned, 1, "the binding still tracks the VM");
    assert_eq!(stats.lost, 0);
    assert_eq!(
        cp.get(&bound.microvm_id).await.unwrap().state,
        State::Pending,
        "the VM stays live under its binding"
    );
}

#[tokio::test]
async fn release_error_that_landed_rewarms_vm() {
    // `release` reports `Err` but the DELETE landed — the verify `get`
    // returns `Ok(None)`, proving the VM unbound, so a live VM re-warms.
    let cp = Arc::new(MockControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            boot_time: Duration::from_secs(3600), // wait fails → cleanup
            ..Default::default()
        },
    ));
    let store = Arc::new(FlakyStore::new());
    for f in [&store.fail_release, &store.release_lands] {
        f.store(true, std::sync::atomic::Ordering::SeqCst);
    }
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.wait = WaitPolicy {
        timeout: Duration::from_millis(100),
        initial_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(10),
    };
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    let t1 = tenant("u1");
    assert!(pool.acquire(&t1).await.is_err());
    tokio::time::timeout(Duration::from_secs(2), async {
        while pool.stats().await.warm == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("live VM never re-warmed");
    assert!(store.get(&t1).await.unwrap().is_none());
    let stats = pool.stats().await;
    assert_eq!(stats.inflight, 0);
    // Still exactly one VM — cleanup neither leaked nor duplicated it.
    assert_eq!(cp.list(None, None).await.unwrap().len(), 1);
}

#[tokio::test]
async fn indeterminate_release_outcome_terminates_vm() {
    // `release` `Err` (the DELETE landed) + the verify `get` `Err` —
    // ownership is unresolvable, so a retrying terminate collapses it:
    // the VM dies and is never re-warmed.
    let cp = Arc::new(MockControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            boot_time: Duration::from_secs(3600),
            ..Default::default()
        },
    ));
    let store = Arc::new(FlakyStore::new());
    for f in [&store.fail_release, &store.release_lands, &store.fail_get] {
        f.store(true, std::sync::atomic::Ordering::SeqCst);
    }
    // The acquire's own `get` passes; the cleanup's verification
    // `get` fails — the outcome is truly indeterminate.
    store
        .get_ok_budget
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.wait = WaitPolicy {
        timeout: Duration::from_millis(100),
        initial_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(10),
    };
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    let t1 = tenant("u1");
    assert!(pool.acquire(&t1).await.is_err());
    // Wait until the retrying terminate lands AND the inflight slot
    // frees — inflight alone can read zero before cleanup starts.
    let dead = cp.list(None, None).await.unwrap()[0].id.clone();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let dead_now = cp.get(&dead).await.map(|v| !v.is_live()).unwrap_or(false);
            if dead_now && pool.stats().await.inflight == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("terminator never settled");
    let stats = pool.stats().await;
    assert_eq!(stats.warm, 0);
    assert_eq!(stats.assigned, 0);
    assert_eq!(cp.get(&dead).await.unwrap().state, State::Terminated);
}

#[tokio::test]
async fn reserve_waits_for_release_handoff() {
    // The binding→inflight handoff is atomic w.r.t. `try_reserve`:
    // while cleanup's `cp.get` is parked post-handoff the VM sits in
    // `inflight`, so a racing acquirer at `max_vms` must be rejected —
    // never allowed to overshoot into a launch.
    let cp = Arc::new(FlakyControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            boot_time: Duration::from_secs(3600),
            ..Default::default()
        },
    ));
    let store = Arc::new(FlakyStore::new());
    store
        .gate_release
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 1;
    cfg.wait = WaitPolicy {
        timeout: Duration::from_millis(100),
        initial_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(10),
    };
    let pool = Arc::new(SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap());

    let t1 = tenant("u1");
    assert!(pool.acquire(&t1).await.is_err());
    // Park inside `release` — the capacity lock is held there, so a
    // racing acquire cannot even reach `try_reserve`.
    tokio::time::timeout(Duration::from_secs(2), store.release_entered.notified())
        .await
        .expect("release never started");
    let u2 = tokio::spawn({
        let pool = pool.clone();
        async move { pool.acquire(&tenant("u2")).await }
    });
    // Deterministic reach proof: u2's acquire path is
    // `store.get` → `pop_warm` → `try_reserve`, and the only await
    // between the `get` entry and `try_reserve`'s `store.list` is the
    // capacity lock itself. Once u2's `get` has been entered it can
    // only sit in sync code or at the lock — so a flat `store.list`
    // counter while the release parks inside the lock *proves* u2 is
    // queued on it, not merely unscheduled.
    let get_base = store.get_calls.load(std::sync::atomic::Ordering::SeqCst);
    let list_base = store.list_calls.load(std::sync::atomic::Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(2), async {
        while store.get_calls.load(std::sync::atomic::Ordering::SeqCst) <= get_base {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("u2 never reached its tenant lookup");
    // Between `get` entry and `store.list` the only await is the
    // capacity lock — held by the parked release — so `store.list`
    // can never fire until we open the gate, whenever we look.
    assert_eq!(
        store.list_calls.load(std::sync::atomic::Ordering::SeqCst),
        list_base,
        "u2 reached `store.list` — it got past the capacity lock while the release was parked"
    );
    assert!(
        !u2.is_finished(),
        "reserve must wait out the parked release"
    );
    // Arm the `get` gate and let the handoff complete: inflight=1
    // while `cp.get` is parked — and u2's reserve must observe it.
    cp.gate_gets
        .store(true, std::sync::atomic::Ordering::SeqCst);
    store.release_gate.notify_one();
    tokio::time::timeout(Duration::from_secs(2), cp.get_entered.notified())
        .await
        .expect("cleanup never reached cp.get");
    let stats = pool.stats().await;
    assert_eq!(stats.inflight, 1, "the handoff must land in inflight");
    assert_eq!(stats.warm, 0);
    assert_eq!(stats.assigned, 0);

    // max_vms=1 is fully consumed by the in-flight VM — u2's own
    // `list` saw exactly one managed VM, the handoff itself.
    let err = u2
        .await
        .expect("u2 task panicked")
        .expect_err("u2 must be rejected");
    assert!(
        matches!(err, Error::PoolExhausted(1)),
        "reserve must see the inflight handoff, got {err:?}"
    );

    // Release the gate: the live VM parks in warm, freeing the slot.
    cp.get_gate.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        while pool.stats().await.warm == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("live VM never re-warmed");
    assert_eq!(pool.stats().await.inflight, 0);
}

#[tokio::test]
async fn indeterminate_claim_with_failed_terminate_stays_tracked() {
    // claim `Err` + `get` `Err` + `terminate` `Err`: the VM may be
    // live and bound to nothing — it must stay counted (inflight, and
    // a sentinel `assigned` once pinned) while a detached terminator
    // retries, never warm, never dropped.
    let cp = Arc::new(FlakyControlPlane::new());
    cp.fail_terminate
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let store = Arc::new(FlakyStore::new());
    store
        .claim_failures
        .store(1, std::sync::atomic::Ordering::SeqCst);
    store
        .fail_get
        .store(true, std::sync::atomic::Ordering::SeqCst);
    store
        .get_ok_budget
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    assert!(pool.acquire(&tenant("u1")).await.is_err());
    // Prove the terminator has genuinely failed at least once.
    tokio::time::timeout(Duration::from_secs(2), async {
        while cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("terminator never attempted");
    let stats = pool.stats().await;
    assert_eq!(stats.inflight, 1, "the possibly-live VM stays counted");
    assert_eq!(stats.warm, 0, "indeterminate VMs never re-warm");
    // The sentinel marker is not an assignment — one lost VM counts
    // once, via its reaper's inflight slot; `lost` only reports
    // markers with no live reaper, so it stays 0 while this one runs.
    assert_eq!(stats.assigned, 0, "sentinels are not tenant bindings");
    assert_eq!(stats.lost, 0, "a reaper-held marker counts via inflight");
    let lost = cp.list(None, None).await.unwrap()[0].id.clone();
    assert!(
        cp.get(&lost).await.unwrap().is_live(),
        "the VM is live but tracked — never shared"
    );

    // Prove it retries under the held slot before recovering.
    tokio::time::timeout(Duration::from_secs(2), async {
        while cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("terminator never retried");
    cp.fail_terminate
        .store(false, std::sync::atomic::Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let dead_now = cp.get(&lost).await.map(|v| !v.is_live()).unwrap_or(false);
            if dead_now && pool.stats().await.inflight == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("terminator never landed");
    assert_eq!(cp.get(&lost).await.unwrap().state, State::Terminated);
    assert_eq!(pool.stats().await.warm, 0);
}

#[test]
fn lost_tenant_prefix_is_reserved() {
    // The `~kotatsu-lost~` namespace belongs to the pool's internal
    // sentinels — a real tenant must never create a key inside it, or
    // `maintain` would destroy that tenant's VM as a "lost" one.
    for key in [
        "~kotatsu-lost~x",
        "~kotatsu-lost~microvm-000000000001",
        "~kotatsu-lost~",
    ] {
        assert!(
            kotatsu::TenantKey::new(key).is_err(),
            "reserved prefix must be rejected: {key}"
        );
    }
    // `~` itself stays legal outside the reserved prefix.
    assert!(kotatsu::TenantKey::new("tenant~1").is_ok());
    assert!(kotatsu::TenantKey::new("~tenant").is_ok());
}

#[cfg(feature = "sqlite")]
#[test]
fn lost_vm_is_destroyed_after_restart() {
    // An indeterminate VM gets pinned with a sentinel binding — a
    // durable record in the store. A *real* restart means the
    // in-flight reaper dies too, so this test uses two Tokio runtimes:
    // dropping the first kills every detached task on it, and only
    // `maintain` on the second runtime can be making `terminate` calls.
    let dir = std::env::temp_dir().join(format!("kotatsu-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("state.db");
    let cp = Arc::new(FlakyControlPlane::new());
    cp.fail_terminate
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let rt1 = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let vm_id = rt1.block_on(async {
        let store = Arc::new(FlakyStore::wrapping(Arc::new(
            kotatsu::SqliteStore::open(&path).await.unwrap(),
        )));
        // u1's claim fails (never lands) and the verification get
        // fails — but the later sentinel pin must succeed.
        store
            .claim_failures
            .store(1, std::sync::atomic::Ordering::SeqCst);
        store
            .fail_get
            .store(true, std::sync::atomic::Ordering::SeqCst);
        store
            .get_ok_budget
            .store(1, std::sync::atomic::Ordering::SeqCst);
        let mut cfg = PoolConfig::new(RunRequest::new("img"));
        cfg.warm_size = 0;
        cfg.max_vms = 10;
        let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();
        assert!(pool.acquire(&tenant("u1")).await.is_err());
        // The reaper has genuinely attempted terminate under its slot.
        tokio::time::timeout(Duration::from_secs(2), async {
            while cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("terminator never attempted");
        cp.list(None, None).await.unwrap()[0].id.clone()
    });
    drop(rt1); // the "crash": aborts every detached task, reaper included

    let rt2 = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt2.block_on(async {
        // The sentinel marker persisted in the store.
        let store2 = Arc::new(kotatsu::SqliteStore::open(&path).await.unwrap());
        let list = store2.list().await.unwrap();
        assert_eq!(list.len(), 1, "the sentinel marker must persist");
        assert!(
            list[0].sentinel,
            "the persisted row must carry the explicit sentinel flag"
        );
        assert!(
            list[0].tenant.as_str().starts_with("~kotatsu-lost~"),
            "marker tenant: {}",
            list[0].tenant
        );
        assert_eq!(list[0].microvm_id, vm_id);

        // A fresh pool on the same store: `maintain` must attempt the
        // sentinel terminate. The old reaper is dead, so any new
        // `terminate` call can only come from `maintain`.
        let mut cfg = PoolConfig::new(RunRequest::new("img"));
        cfg.warm_size = 0;
        cfg.max_vms = 10;
        let pool2 = SandboxPool::new(cp.clone(), store2.clone(), cfg).unwrap();
        let before = cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst);
        pool2.maintain().await.unwrap();
        assert!(
            cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst) > before,
            "maintain must attempt the sentinel terminate"
        );
        assert_eq!(
            store2.list().await.unwrap().len(),
            1,
            "a failed terminate keeps the marker for the next tick"
        );
        assert!(
            cp.get(&vm_id).await.unwrap().is_live(),
            "the VM is still live — tracked, not dropped"
        );

        // Recover the plane: maintain destroys the VM and the marker
        // clears.
        cp.fail_terminate
            .store(false, std::sync::atomic::Ordering::SeqCst);
        pool2.maintain().await.unwrap();
        assert_eq!(cp.get(&vm_id).await.unwrap().state, State::Terminated);
        assert!(
            store2.list().await.unwrap().is_empty(),
            "the marker must clear once the VM is destroyed"
        );
    });
    drop(rt2);
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(feature = "sqlite")]
#[test]
fn unpinned_lost_vm_is_destroyed_after_restart() {
    // Harder case: the sentinel pin itself never lands (store claim
    // keeps failing) *and* terminate keeps failing, so nothing durable
    // records the VM. After a restart the new pool's lost-VM reconcile
    // must still find it via `list-microvms` and destroy it.
    let dir = std::env::temp_dir().join(format!("kotatsu-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("state.db");
    let cp = Arc::new(FlakyControlPlane::new());
    cp.fail_terminate
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let rt1 = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let vm_id = rt1.block_on(async {
        let store = Arc::new(FlakyStore::wrapping(Arc::new(
            kotatsu::SqliteStore::open(&path).await.unwrap(),
        )));
        // Every claim fails — the reaper can never pin its marker.
        store
            .claim_failures
            .store(usize::MAX, std::sync::atomic::Ordering::SeqCst);
        store
            .fail_get
            .store(true, std::sync::atomic::Ordering::SeqCst);
        store
            .get_ok_budget
            .store(1, std::sync::atomic::Ordering::SeqCst);
        let mut cfg = PoolConfig::new(RunRequest::new(IMG_ARN));
        cfg.warm_size = 0;
        cfg.max_vms = 10;
        let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();
        assert!(pool.acquire(&tenant("u1")).await.is_err());
        // The reaper attempted at least one pin+terminate pair.
        tokio::time::timeout(Duration::from_secs(2), async {
            while cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("terminator never attempted");
        cp.list(None, None).await.unwrap()[0].id.clone()
    });
    drop(rt1); // crash: reaper dies, and nothing was ever persisted

    let rt2 = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt2.block_on(async {
        // Nothing durable: no marker, no binding — only `list` sees
        // the still-live VM.
        let store2 = Arc::new(kotatsu::SqliteStore::open(&path).await.unwrap());
        assert!(
            store2.list().await.unwrap().is_empty(),
            "this scenario must start with no durable tracking"
        );
        assert!(cp.get(&vm_id).await.unwrap().is_live());

        let mut cfg = PoolConfig::new(RunRequest::new(IMG_ARN));
        cfg.warm_size = 0;
        cfg.max_vms = 10;
        cfg.reap_lost_vms = true; // the reconcile this test exercises
        let pool2 = SandboxPool::new(cp.clone(), store2.clone(), cfg).unwrap();
        let before = cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst);
        // First sighting: suspect only — no terminate yet.
        pool2.maintain().await.unwrap();
        assert_eq!(
            cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst),
            before,
            "a first sighting must not reap — it could be mid-registration"
        );
        // Second sighting: provably lost — pin + terminate (fails).
        pool2.maintain().await.unwrap();
        assert!(
            cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst) > before,
            "the reconcile must attempt terminate on the second sighting"
        );
        assert!(
            cp.get(&vm_id).await.unwrap().is_live(),
            "failed terminate — still live, still suspect"
        );

        // Recover: the next ticks converge — suspect retry or the
        // sentinel marker finishes the VM.
        cp.fail_terminate
            .store(false, std::sync::atomic::Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                pool2.maintain().await.unwrap();
                if !cp.get(&vm_id).await.unwrap().is_live() {
                    break;
                }
            }
        })
        .await
        .expect("reconcile never destroyed the lost VM");
        assert_eq!(cp.get(&vm_id).await.unwrap().state, State::Terminated);
    });
    drop(rt2);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn reserve_waits_for_pending_release_then_succeeds() {
    // While a cleanup's release is parked, the VM is counted once —
    // its binding and the cleanup's inflight slot overlap. Another
    // tenant's reserve waits out the release under the capacity lock,
    // then gets the free slot at `max_vms=2`.
    let cp = Arc::new(MockControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            boot_time: Duration::from_millis(500),
            ..Default::default()
        },
    ));
    let store = Arc::new(GatedStore::releases());
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 2;
    cfg.wait = WaitPolicy {
        timeout: Duration::from_secs(5),
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(50),
    };
    let pool = Arc::new(SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap());

    let t1 = tenant("u1");
    let task = tokio::spawn({
        let pool = pool.clone();
        let t1 = t1.clone();
        async move { pool.acquire(&t1).await }
    });
    // Kill the VM mid-wait so cleanup reaches the gated release.
    let bound_id = loop {
        let vms = cp.list(None, None).await.unwrap();
        if let Some(vm) = vms.first() {
            break vm.id.clone();
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    cp.terminate(&bound_id).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), store.release_entered.notified())
        .await
        .expect("release never started");
    task.abort();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(store.get(&t1).await.unwrap().is_some());
    let stats = pool.stats().await;
    assert_eq!(
        stats.warm + stats.inflight + stats.assigned + stats.lost,
        1,
        "the parked VM must be counted once"
    );

    // managed = warm(0) + inflight(0) + assigned(1) < max_vms(2) — but
    // the capacity lock serializes the binding→inflight handoff, so
    // u2's reserve waits out the parked release rather than racing it.
    let u2 = tokio::spawn({
        let pool = pool.clone();
        async move { pool.acquire(&tenant("u2")).await }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!u2.is_finished(), "reserve must wait out the release");
    store.release_gate.notify_one();
    let sb = tokio::time::timeout(Duration::from_secs(2), u2)
        .await
        .expect("u2 acquire never settled")
        .expect("u2 task panicked")
        .expect("u2 acquire failed");
    assert_ne!(sb.vm().id().as_str(), bound_id.as_str());
}

#[tokio::test]
async fn cancelled_claim_keeps_vm_owned_by_binding() {
    // If the caller is cancelled while `claim` is in flight, the
    // dispatched INSERT may still land. The VM must then belong to the
    // binding — never to `warm`, or another tenant could share it.
    let cp = Arc::new(MockControlPlane::new());
    let store = Arc::new(GatedStore::claims());
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.wait = WaitPolicy {
        timeout: Duration::from_secs(5),
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(50),
    };
    let pool = Arc::new(SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap());

    let t1 = tenant("u1");
    let task = tokio::spawn({
        let pool = pool.clone();
        let t1 = t1.clone();
        async move { pool.acquire(&t1).await }
    });
    // Wait until the claim is genuinely inside the gate.
    tokio::time::timeout(Duration::from_secs(2), store.claim_entered.notified())
        .await
        .expect("claim never started");
    task.abort();
    store.claim_gate.notify_one();

    // The claim resolves on the detached task; the binding owns the VM.
    tokio::time::timeout(Duration::from_secs(2), async {
        while store.get(&t1).await.unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("binding never landed");
    tokio::time::sleep(Duration::from_millis(50)).await; // resolver parks
    assert_eq!(
        pool.stats().await.warm,
        0,
        "claimed VM must not return to warm"
    );
    let bound_vm = store.get(&t1).await.unwrap().unwrap().microvm_id;

    // A different tenant must get a *different* VM — no sharing.
    let sb = pool.acquire(&tenant("u2")).await.unwrap();
    assert_ne!(sb.vm().id().as_str(), bound_vm.as_str());
}

#[tokio::test]
async fn cancelled_release_keeps_vm_until_outcome_known() {
    // Cancel inside `store.release`: the dispatched DELETE may never
    // run, so the binding can still own the VM. The detached cleanup
    // task must hold it (inflight — never warm) until the release
    // resolves; only a proven-unbound VM may re-enter warm.
    let cp = Arc::new(MockControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            // Never boots — the wait fails, taking the release path.
            boot_time: Duration::from_secs(3600),
            ..Default::default()
        },
    ));
    let store = Arc::new(GatedStore::releases());
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.wait = WaitPolicy {
        timeout: Duration::from_millis(100),
        initial_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(10),
    };
    let pool = Arc::new(SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap());

    let t1 = tenant("u1");
    let task = tokio::spawn({
        let pool = pool.clone();
        let t1 = t1.clone();
        async move { pool.acquire(&t1).await }
    });
    // Wait until the release call is inside the gate — the wait has
    // already timed out and the binding exists by then.
    tokio::time::timeout(Duration::from_secs(2), store.release_entered.notified())
        .await
        .expect("release never started");
    task.abort();
    // The release gate never fires: the binding survives and still
    // owns the VM — a bound VM must never sit in warm for another
    // tenant to claim.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(store.get(&t1).await.unwrap().is_some());
    let stats = pool.stats().await;
    assert_eq!(stats.warm, 0, "bound VM must never sit in warm");
    assert_eq!(stats.assigned, 1, "the binding still owns the VM");

    // Once the release resolves the VM as unbound and live, it may
    // re-warm.
    store.release_gate.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        while pool.stats().await.warm == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("live VM never re-warmed");
    assert!(store.get(&t1).await.unwrap().is_none());
    assert_eq!(pool.stats().await.warm, 1, "unbound live VM re-warms");
}

#[tokio::test]
async fn cancelled_release_never_shares_vm_with_other_tenant() {
    // Acquire cancelled while `store.release` is in flight must not
    // let another tenant claim the still-bound VM — not even before
    // the next `maintain` tick.
    let cp = Arc::new(MockControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            boot_time: Duration::from_millis(500),
            ..Default::default()
        },
    ));
    let store = Arc::new(GatedStore::releases());
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.wait = WaitPolicy {
        timeout: Duration::from_secs(5),
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(50),
    };
    let pool = Arc::new(SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap());

    let t1 = tenant("u1");
    let task = tokio::spawn({
        let pool = pool.clone();
        let t1 = t1.clone();
        async move { pool.acquire(&t1).await }
    });
    // Kill the VM while the acquire is waiting on it, so the wait
    // fails and the release path runs — gated inside `store.release`.
    let bound_id = loop {
        let vms = cp.list(None, None).await.unwrap();
        if let Some(vm) = vms.first() {
            break vm.id.clone();
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    cp.terminate(&bound_id).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), store.release_entered.notified())
        .await
        .expect("release never started");
    task.abort();
    // While parked, the binding still owns the VM — unreachable by any
    // acquire (and reserves serialize behind the cleanup's release).
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(store.get(&t1).await.unwrap().is_some());
    assert_eq!(pool.stats().await.warm, 0, "bound VM must not be warm");

    // Let the release resolve; a different tenant must then get a
    // *different* VM — no sharing.
    store.release_gate.notify_one();
    let sb = pool.acquire(&tenant("u2")).await.unwrap();
    assert_ne!(sb.vm().id().as_str(), bound_id.as_str());
}

#[tokio::test]
async fn dead_vm_not_rewarmed_after_wait_failure() {
    // A VM that dies during the wait must not be pushed back to warm
    // when terminate also fails — a dead warm entry poisons the next
    // acquire (LIFO pop → immediate wait failure).
    let cp = Arc::new(MockControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            boot_time: Duration::from_secs(3600),
            ..Default::default()
        },
    ));
    let pool = Arc::new(test_pool(cp.clone(), 0, 10));
    // The VM never boots; terminating it mid-wait fails the wait at
    // once instead of spending test_pool's 5s budget.
    let acq = tokio::spawn({
        let pool = pool.clone();
        async move { pool.acquire(&tenant("u1")).await }
    });
    // Kill the VM while the acquire is waiting on it.
    let id = loop {
        let vms = cp.list(None, None).await.unwrap();
        if let Some(vm) = vms.first() {
            break vm.id.clone();
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    cp.terminate(&id).await.unwrap();
    let res = tokio::time::timeout(Duration::from_secs(10), acq)
        .await
        .expect("acquire hung")
        .unwrap();
    assert!(res.is_err(), "acquire of a dead VM must fail");
    let stats = pool.stats().await;
    assert_eq!(stats.warm, 0, "dead VM must not be re-warmed");
    assert_eq!(stats.inflight, 0);
}

#[tokio::test]
async fn invalid_pool_config_rejected() {
    let cp = Arc::new(MockControlPlane::new());
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 5;
    cfg.max_vms = 2;
    assert!(SandboxPool::new(cp.clone(), Arc::new(MemoryStore::new()), cfg).is_err());

    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.token_scope = vec![]; // empty scope
    assert!(SandboxPool::new(cp, Arc::new(MemoryStore::new()), cfg).is_err());
}

#[test]
fn reap_lost_vms_requires_image_arn() {
    // list-microvms reports image ARNs: with an image ID the reconcile
    // would silently match nothing, so the pool refuses to start.
    let cp: Arc<dyn ControlPlane> = Arc::new(MockControlPlane::new());
    let pool = |image: &str, reap: bool| {
        let mut cfg = PoolConfig::new(RunRequest::new(image));
        cfg.reap_lost_vms = reap;
        SandboxPool::new(cp.clone(), Arc::new(MemoryStore::new()), cfg)
    };
    assert!(matches!(pool("img", true), Err(Error::InvalidInput(_))));
    assert!(pool(IMG_ARN, true).is_ok());
    assert!(pool("img", false).is_ok());
}

#[tokio::test]
async fn reap_lost_vms_disabled_keeps_foreign_vm() {
    // `PoolConfig::new` must default `reap_lost_vms` off: an
    // externally-launched VM sharing the pool's image survives every
    // maintenance tick — the pool only destroys VMs it provably owns.
    // (No explicit assignment — the constructor default is the point.)
    let cp = Arc::new(FlakyControlPlane::new());
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    assert!(
        !cfg.reap_lost_vms,
        "reap_lost_vms must default off — same-image identity proves nothing"
    );
    let pool = SandboxPool::new(cp.clone(), Arc::new(MemoryStore::new()), cfg).unwrap();

    // A VM this pool never launched — same image, tracked nowhere.
    let foreign = cp.run(&RunRequest::new("img")).await.unwrap();
    let terminates = || cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(terminates(), 0);
    pool.maintain().await.unwrap();
    pool.maintain().await.unwrap();
    assert!(
        cp.get(&foreign.id).await.unwrap().is_live(),
        "with reconcile disabled a foreign same-image VM must survive"
    );
    assert_eq!(terminates(), 0, "no terminate may be issued at all");
}

#[tokio::test]
async fn untracked_foreign_image_vm_survives_reconcile() {
    // Even with the reconcile enabled, image scoping holds: a live VM
    // whose image is NOT this pool's is never a lost fleet member.
    let cp = Arc::new(FlakyControlPlane::new());
    let pool = SandboxPool::new(cp.clone(), Arc::new(MemoryStore::new()), {
        let mut cfg = PoolConfig::new(RunRequest::new(IMG_ARN));
        cfg.warm_size = 0;
        cfg.max_vms = 10;
        cfg.reap_lost_vms = true;
        cfg
    })
    .unwrap();
    let foreign = cp.run(&RunRequest::new("other-img")).await.unwrap();
    pool.maintain().await.unwrap();
    pool.maintain().await.unwrap();
    assert!(
        cp.get(&foreign.id).await.unwrap().is_live(),
        "a different-image VM must never be reconciled"
    );
    assert_eq!(
        cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

#[tokio::test]
async fn wait_fail_cleanup_vm_survives_maintain_ticks() {
    // While cleanup waits inside `cp.get` the VM is owned by its
    // `Slot` — pending-registered — so `maintain`'s lost-VM reconcile
    // must not classify or terminate it on either of two ticks.
    let cp = Arc::new(FlakyControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            boot_time: Duration::from_secs(3600),
            ..Default::default()
        },
    ));
    let store = Arc::new(FlakyStore::new());
    let mut cfg = PoolConfig::new(RunRequest::new(IMG_ARN));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.reap_lost_vms = true; // reconcile ON to prove pending protects
    cfg.wait = WaitPolicy {
        timeout: Duration::from_millis(100),
        initial_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(10),
    };
    let pool = Arc::new(SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap());

    // Park the cleanup's `release` first — arming `gate_gets` only
    // after that wait keeps `wait_until_running`'s own `cp.get` polls
    // ungated while guaranteeing the cleanup's `cp.get` parks.
    store
        .gate_release
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(pool.acquire(&tenant("u1")).await.is_err());
    tokio::time::timeout(Duration::from_secs(2), store.release_entered.notified())
        .await
        .expect("cleanup never entered the release gate");
    cp.gate_gets
        .store(true, std::sync::atomic::Ordering::SeqCst);
    store.release_gate.notify_one();
    // The release lands → Unbound → Slot::new takes pending+inflight →
    // `cp.get` parks at the gate with the VM cleanup-owned.
    tokio::time::timeout(Duration::from_secs(2), cp.get_entered.notified())
        .await
        .expect("cleanup never reached cp.get");
    // Disarm the gate flag so the test's own `cp.get` calls don't
    // park — the cleanup's parked get stays parked until notified.
    cp.gate_gets
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let vm_id = cp.list(None, None).await.unwrap()[0].id.clone();
    let rows = store.list().await.unwrap();
    assert!(
        rows.len() == 1 && rows[0].sentinel,
        "the tenant binding is gone — a durable sentinel + Slot track the VM now"
    );

    // Two maintenance ticks with the get still parked.
    pool.maintain().await.unwrap();
    pool.maintain().await.unwrap();
    assert!(
        cp.get(&vm_id).await.unwrap().is_live(),
        "a cleanup-owned VM must never be reconciled as lost"
    );
    assert_eq!(
        cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "maintain must not terminate the in-transit VM"
    );
    assert!(
        store.list().await.unwrap().iter().all(|b| b.sentinel),
        "the transit marker must survive — it protects the in-flight VM"
    );

    // Release the gate — the live VM re-warms normally, then the
    // cleanup releases the marker now that `warm` tracks the VM.
    cp.get_gate.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        while pool.stats().await.warm == 0 || !store.list().await.unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("live VM never re-warmed or marker never released");
}

#[tokio::test]
async fn restart_sentinel_holds_capacity() {
    // Post-restart shape: a sentinel marker in the store, no live
    // reaper (pending empty), terminate still failing. The marker
    // must keep the lost VM counted — `max_vms=1` leaves no room for
    // a launch, from `acquire` or the maintain top-up.
    let cp = Arc::new(FlakyControlPlane::new());
    cp.fail_terminate
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let store = Arc::new(FlakyStore::new());
    store
        .claim_failures
        .store(1, std::sync::atomic::Ordering::SeqCst);
    store
        .fail_get
        .store(true, std::sync::atomic::Ordering::SeqCst);
    store
        .get_ok_budget
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 1;
    cfg.max_vms = 1;
    let pool = Arc::new(SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap());

    assert!(pool.acquire(&tenant("u1")).await.is_err());
    // Wait until the reaper has pinned the sentinel marker.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if store
                .list()
                .await
                .unwrap()
                .iter()
                .any(|b| b.tenant.as_str().starts_with("~kotatsu-lost~"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("sentinel never pinned");
    let vm_id = cp.list(None, None).await.unwrap()[0].id.clone();

    // Crash simulation: drop the pool — the reaper dies with its
    // pending entry; only the durable marker remains.
    drop(pool);

    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 1;
    cfg.max_vms = 1;
    let pool2 = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();
    // Restart-time maintain: the sentinel terminate fails — the
    // marker stays, and the top-up must not launch a second VM.
    pool2.maintain().await.unwrap();
    let stats = pool2.stats().await;
    assert_eq!(stats.lost, 1, "the unheld marker keeps the VM counted");
    assert_eq!(stats.warm, 0);
    assert_eq!(stats.inflight, 0);
    // The store outage was only for the crash scenario — lift it so
    // u2's lookup can run.
    store
        .fail_get
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let err = pool2.acquire(&tenant("u2")).await.unwrap_err();
    assert!(
        matches!(err, Error::PoolExhausted(1)),
        "the marker must hold capacity, got {err:?}"
    );
    assert_eq!(
        cp.list(None, None).await.unwrap().len(),
        1,
        "no extra VM may be launched while the lost VM is tracked"
    );
    assert!(cp.get(&vm_id).await.unwrap().is_live());
}

#[tokio::test]
async fn detached_cleanup_aborts_with_pool() {
    // wait_fail_cleanup joins the pool's task set: dropping the pool
    // mid-park aborts it — the gated release never runs, so the
    // binding provably survives.
    let cp = Arc::new(FlakyControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            boot_time: Duration::from_secs(3600),
            ..Default::default()
        },
    ));
    let store = Arc::new(FlakyStore::new());
    store
        .gate_release
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.wait = WaitPolicy {
        timeout: Duration::from_millis(100),
        initial_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(10),
    };
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    assert!(pool.acquire(&tenant("u1")).await.is_err());
    tokio::time::timeout(Duration::from_secs(2), store.release_entered.notified())
        .await
        .expect("cleanup never entered the release gate");
    drop(pool); // aborts the parked cleanup mid-gate
    store.release_gate.notify_one();
    // Yield so any (hypothetically surviving) task could run the
    // release — nothing may consume the binding.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        store.get(&tenant("u1")).await.unwrap().is_some(),
        "aborted cleanup must not have completed the release"
    );
}

#[tokio::test]
async fn aborted_cleanup_leaves_sentinel_marker() {
    // The dangerous window: release has LANDED and the cleanup is
    // parked in `cp.get` when the pool drops. The abort must leave
    // the durable sentinel behind — the VM is live, unbound, and the
    // marker is the only tracking left. A fresh pool reaps it through
    // the sentinel path.
    let cp = Arc::new(FlakyControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            boot_time: Duration::from_secs(3600),
            ..Default::default()
        },
    ));
    let store = Arc::new(FlakyStore::new());
    store
        .gate_release
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.reap_lost_vms = false; // tracking must survive even with reconcile OFF
    cfg.wait = WaitPolicy {
        timeout: Duration::from_millis(100),
        initial_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(10),
    };
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    assert!(pool.acquire(&tenant("u1")).await.is_err());
    tokio::time::timeout(Duration::from_secs(2), store.release_entered.notified())
        .await
        .expect("cleanup never entered the release gate");
    // Release can land now — park the cleanup's subsequent cp.get.
    cp.gate_gets
        .store(true, std::sync::atomic::Ordering::SeqCst);
    store.release_gate.notify_one();
    tokio::time::timeout(Duration::from_secs(2), cp.get_entered.notified())
        .await
        .expect("cleanup never reached cp.get");
    // The release landed; the sentinel is pinned behind the parked get.
    let vm_id = cp.list(None, None).await.unwrap()[0].id.clone();

    drop(pool); // aborts the cleanup mid-get
    cp.gate_gets
        .store(false, std::sync::atomic::Ordering::SeqCst);
    cp.get_gate.notify_waiters();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Tracking survived the abort: exactly one row, the sentinel.
    let rows = store.list().await.unwrap();
    assert_eq!(rows.len(), 1, "the aborted cleanup must leave its marker");
    assert!(rows[0].sentinel, "the surviving row must be the sentinel");
    assert_eq!(rows[0].microvm_id, vm_id);
    assert!(cp.get(&vm_id).await.unwrap().is_live());

    // A fresh pool on the same store recovers through the sentinel
    // path — destroy on sight, then clear the marker.
    let mut cfg2 = PoolConfig::new(RunRequest::new("img"));
    cfg2.warm_size = 0;
    cfg2.max_vms = 10;
    let pool2 = SandboxPool::new(cp.clone(), store.clone(), cfg2).unwrap();
    pool2.maintain().await.unwrap();
    assert!(
        !cp.get(&vm_id).await.unwrap().is_live(),
        "the new pool must terminate the sentinel-tracked VM"
    );
    assert!(
        store.list().await.unwrap().is_empty(),
        "the marker is released once the VM is gone"
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sentinel_claim_on_legacy_row_keeps_normal_binding() {
    // A kind=0 row that exactly matches the marker shape
    // (`{prefix}{vm}` → vm) must win the claim race: the cleanup's
    // sentinel pin reports HeldByOther, the pin is judged NOT landed,
    // and the cleanup bails before touching the tenant binding — the
    // normal row is never marker-released, the VM never terminated.
    let dir = std::env::temp_dir().join(format!("kotatsu-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("state.db");

    let cp = Arc::new(FlakyControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            boot_time: Duration::from_secs(3600),
            ..Default::default()
        },
    ));
    let store = Arc::new(FlakyStore::wrapping(Arc::new(
        kotatsu::SqliteStore::open(&path).await.unwrap(),
    )));
    store
        .gate_sentinel_claim
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.wait = WaitPolicy {
        timeout: Duration::from_millis(100),
        initial_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(10),
    };
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    assert!(pool.acquire(&tenant("u1")).await.is_err());
    tokio::time::timeout(
        Duration::from_secs(2),
        store.sentinel_claim_entered.notified(),
    )
    .await
    .expect("cleanup never reached the sentinel claim");

    // While the pin is parked, land the colliding normal row for the
    // very VM the cleanup owns.
    let vm_id = cp.list(None, None).await.unwrap()[0].id.clone();
    let raw = tokio_rusqlite::Connection::open(&path).await.unwrap();
    let t = format!("~kotatsu-lost~{}", vm_id.as_str());
    let id = vm_id.as_str().to_owned();
    raw.call(move |c| {
        c.execute(
            "INSERT INTO bindings(tenant, microvm_id, claimed_at)
             VALUES (?1, ?2, 1700000000)",
            tokio_rusqlite::rusqlite::params![t, id],
        )
        .map(|_| ())
    })
    .await
    .unwrap();
    drop(raw);

    store.sentinel_claim_gate.notify_one();
    // The pin reports HeldByOther → not verified → cleanup bails:
    // wait for its slot to drop (inflight back to 0).
    tokio::time::timeout(Duration::from_secs(2), async {
        while pool.stats().await.inflight != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("cleanup never returned");

    let rows = store.list().await.unwrap();
    assert_eq!(
        rows.len(),
        2,
        "u1's binding + the legacy row must both survive"
    );
    assert!(
        rows.iter().all(|b| !b.sentinel),
        "no row may be marked sentinel — the pin never landed"
    );
    assert!(
        store.get(&tenant("u1")).await.unwrap().is_some(),
        "the tenant binding must be kept — the release never ran"
    );
    assert!(cp.get(&vm_id).await.unwrap().is_live());
    assert_eq!(
        cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "a bound VM must never be terminated on a failed pin"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn unverifiable_sentinel_pin_keeps_binding() {
    // The marker claim fails BEFORE writing (store error). The
    // cleanup must bail without releasing the tenant binding —
    // the binding is the VM's only durable tracking left.
    let cp = Arc::new(FlakyControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            boot_time: Duration::from_secs(3600),
            ..Default::default()
        },
    ));
    let store = Arc::new(FlakyStore::new());
    store
        .gate_sentinel_claim
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.wait = WaitPolicy {
        timeout: Duration::from_millis(100),
        initial_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(10),
    };
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    assert!(pool.acquire(&tenant("u1")).await.is_err());
    tokio::time::timeout(
        Duration::from_secs(2),
        store.sentinel_claim_entered.notified(),
    )
    .await
    .expect("cleanup never reached the sentinel claim");
    // Fail the claim before it can write, then let it through.
    store
        .claim_failures
        .store(1, std::sync::atomic::Ordering::SeqCst);
    store.sentinel_claim_gate.notify_one();

    tokio::time::timeout(Duration::from_secs(2), async {
        while pool.stats().await.inflight != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("cleanup never returned");

    assert!(
        store.get(&tenant("u1")).await.unwrap().is_some(),
        "release must not run while the marker is unproven"
    );
    assert!(
        store.list().await.unwrap().iter().all(|b| !b.sentinel),
        "no marker may exist — the claim failed before writing"
    );
    let vm_id = cp.list(None, None).await.unwrap()[0].id.clone();
    assert!(cp.get(&vm_id).await.unwrap().is_live());
    assert_eq!(
        cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

#[tokio::test]
async fn maintain_keeps_marker_during_release_handoff() {
    // The pin→release window: marker landed, tenant binding still
    // present, cleanup parked inside `release`. `maintain` must leave
    // the marker alone (pending wins over the stale-marker sweep) —
    // clearing it now + an abort after the release lands would leave
    // a live VM tracked nowhere.
    let cp = Arc::new(FlakyControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            boot_time: Duration::from_secs(3600),
            ..Default::default()
        },
    ));
    let store = Arc::new(FlakyStore::new());
    store
        .gate_release
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.wait = WaitPolicy {
        timeout: Duration::from_millis(100),
        initial_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(10),
    };
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    assert!(pool.acquire(&tenant("u1")).await.is_err());
    tokio::time::timeout(Duration::from_secs(2), store.release_entered.notified())
        .await
        .expect("cleanup never entered the release gate");
    // Marker pinned, binding not yet released, slot pending-held.
    pool.maintain().await.unwrap();
    let rows = store.list().await.unwrap();
    assert_eq!(
        rows.len(),
        2,
        "maintain must not sweep the marker mid-handoff"
    );
    assert_eq!(
        rows.iter().filter(|b| b.sentinel).count(),
        1,
        "the pinned marker must survive the tick"
    );
    assert_eq!(
        cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "maintain must not terminate a pending-held VM"
    );

    // Let the handoff finish — VM re-warms and the marker is released.
    store.release_gate.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !store.list().await.unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("cleanup never finished");
    assert_eq!(pool.stats().await.warm, 1);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn reap_lost_claim_on_normal_row_never_terminates() {
    // `reap_lost`'s marker pin colliding with a kind=0 row naming the
    // same VM must abort the reap entirely: the normal binding owns
    // the VM — it is not lost, so no terminate and no row delete.
    let dir = std::env::temp_dir().join(format!("kotatsu-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("state.db");

    let cp = Arc::new(FlakyControlPlane::new());
    let store = Arc::new(FlakyStore::wrapping(Arc::new(
        kotatsu::SqliteStore::open(&path).await.unwrap(),
    )));
    store
        .gate_sentinel_claim
        .store(true, std::sync::atomic::Ordering::SeqCst);
    // u1's claim fails and the verification get fails — the VM's
    // ownership is indeterminate, so a detached `reap_lost` runs.
    store
        .claim_failures
        .store(1, std::sync::atomic::Ordering::SeqCst);
    store
        .fail_get
        .store(true, std::sync::atomic::Ordering::SeqCst);
    store
        .get_ok_budget
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    assert!(pool.acquire(&tenant("u1")).await.is_err());
    tokio::time::timeout(
        Duration::from_secs(2),
        store.sentinel_claim_entered.notified(),
    )
    .await
    .expect("reaper never reached the sentinel claim");

    // Land the colliding normal row while the pin is parked.
    let vm_id = cp.list(None, None).await.unwrap()[0].id.clone();
    let raw = tokio_rusqlite::Connection::open(&path).await.unwrap();
    let t = format!("~kotatsu-lost~{}", vm_id.as_str());
    let id = vm_id.as_str().to_owned();
    raw.call(move |c| {
        c.execute(
            "INSERT INTO bindings(tenant, microvm_id, claimed_at)
             VALUES (?1, ?2, 1700000000)",
            tokio_rusqlite::rusqlite::params![t, id],
        )
        .map(|_| ())
    })
    .await
    .unwrap();
    drop(raw);

    store.sentinel_claim_gate.notify_one();
    // Give the reaper a beat to (wrongly, if unfixed) terminate.
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        cp.get(&vm_id).await.unwrap().is_live(),
        "a VM owned by a normal binding must never be reaped"
    );
    assert_eq!(
        cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "no terminate may be issued against an owned VM"
    );
    let rows = store.list().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(!rows[0].sentinel, "the normal row must stay a binding");
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn reap_lost_reclaim_on_normal_row_stops_retry() {
    // The retry-loop re-claim must carry the same guard: a failed
    // first pin + a failed terminate sends `reap_lost` back to
    // `claim`, and a normal row landed meanwhile must stop the reap.
    let dir = std::env::temp_dir().join(format!("kotatsu-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("state.db");

    let cp = Arc::new(FlakyControlPlane::new());
    let store = Arc::new(FlakyStore::wrapping(Arc::new(
        kotatsu::SqliteStore::open(&path).await.unwrap(),
    )));
    // Park on the SECOND sentinel claim — the first pin attempt must
    // fail (below) so `reap_lost` re-claims inside its retry loop.
    store
        .sentinel_claim_ordinal
        .store(2, std::sync::atomic::Ordering::SeqCst);
    store
        .gate_sentinel_claim
        .store(true, std::sync::atomic::Ordering::SeqCst);
    // u1's claim + the reaper's first pin both fail; terminate fails
    // too — the loop reaches its second claim.
    store
        .claim_failures
        .store(2, std::sync::atomic::Ordering::SeqCst);
    store
        .fail_get
        .store(true, std::sync::atomic::Ordering::SeqCst);
    store
        .get_ok_budget
        .store(1, std::sync::atomic::Ordering::SeqCst);
    cp.fail_terminate
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    assert!(pool.acquire(&tenant("u1")).await.is_err());
    tokio::time::timeout(
        Duration::from_secs(2),
        store.sentinel_claim_entered.notified(),
    )
    .await
    .expect("reaper never re-claimed the marker");

    // A normal binding landed on the VM while the loop was parked.
    let vm_id = cp.list(None, None).await.unwrap()[0].id.clone();
    let raw = tokio_rusqlite::Connection::open(&path).await.unwrap();
    let t = format!("~kotatsu-lost~{}", vm_id.as_str());
    let id = vm_id.as_str().to_owned();
    raw.call(move |c| {
        c.execute(
            "INSERT INTO bindings(tenant, microvm_id, claimed_at)
             VALUES (?1, ?2, 1700000000)",
            tokio_rusqlite::rusqlite::params![t, id],
        )
        .map(|_| ())
    })
    .await
    .unwrap();
    drop(raw);

    store.sentinel_claim_gate.notify_one();
    // An unguarded loop re-issues terminate after its ~200ms backoff —
    // give it room and bound the wait on the call count itself.
    tokio::time::timeout(Duration::from_secs(2), async {
        while cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect_err("a missing guard would retry terminate after the backoff");
    assert_eq!(
        cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the re-claim collision must stop the retry loop"
    );
    assert!(cp.get(&vm_id).await.unwrap().is_live());
    let rows = store.list().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(!rows[0].sentinel);
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn reconcile_claim_on_normal_row_skips_vm() {
    // Same collision on the reconcile path: the marker claim parks, a
    // kind=0 row for the suspect VM lands, and the claim's
    // HeldByOther must suppress the terminate.
    let dir = std::env::temp_dir().join(format!("kotatsu-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("state.db");

    let cp = Arc::new(FlakyControlPlane::new());
    let store = Arc::new(FlakyStore::wrapping(Arc::new(
        kotatsu::SqliteStore::open(&path).await.unwrap(),
    )));
    let mut cfg = PoolConfig::new(RunRequest::new(IMG_ARN));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.reap_lost_vms = true; // reconcile ON — it must still yield to bindings
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    // An untracked same-image VM — a lost-fleet suspect.
    let foreign = cp.run(&RunRequest::new(IMG_ARN)).await.unwrap();
    pool.maintain().await.unwrap(); // first sighting

    // Second tick: the pin parks at the gate; the row lands mid-flight.
    store
        .gate_sentinel_claim
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let m = {
        let pool = &pool;
        pool.maintain()
    };
    tokio::pin!(m);
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            _ = &mut m => panic!("maintain returned without reaching the claim"),
            _ = store.sentinel_claim_entered.notified() => {}
        }
    })
    .await
    .expect("reconcile never reached the sentinel claim");

    let raw = tokio_rusqlite::Connection::open(&path).await.unwrap();
    let t = format!("~kotatsu-lost~{}", foreign.id.as_str());
    let id = foreign.id.as_str().to_owned();
    raw.call(move |c| {
        c.execute(
            "INSERT INTO bindings(tenant, microvm_id, claimed_at)
             VALUES (?1, ?2, 1700000000)",
            tokio_rusqlite::rusqlite::params![t, id],
        )
        .map(|_| ())
    })
    .await
    .unwrap();
    drop(raw);

    store.sentinel_claim_gate.notify_one();
    m.await.unwrap();
    assert!(
        cp.get(&foreign.id).await.unwrap().is_live(),
        "reconcile must not terminate a VM that just got bound"
    );
    assert_eq!(
        cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    let rows = store.list().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert!(!rows[0].sentinel);
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn stale_marker_release_racing_cleanup_repins() {
    // Pre-existing marker + tenant binding: `maintain`'s stale sweep
    // can win the race and release the marker before the cleanup's
    // pin — the cleanup's own claim must re-pin it so the VM stays
    // durably tracked through an abort.
    let dir = std::env::temp_dir().join(format!("kotatsu-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("state.db");

    let cp = Arc::new(FlakyControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            boot_time: Duration::from_secs(3600),
            ..Default::default()
        },
    ));
    let store = Arc::new(FlakyStore::wrapping(Arc::new(
        kotatsu::SqliteStore::open(&path).await.unwrap(),
    )));
    // A live VM, bound to u1, with a leftover marker on it.
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    store
        .inner
        .claim(&Binding {
            tenant: tenant("u1"),
            microvm_id: vm.id.clone(),
            claimed_at_secs: 1_700_000_000,
            sentinel: false,
        })
        .await
        .unwrap();
    let raw = tokio_rusqlite::Connection::open(&path).await.unwrap();
    let t = format!("~kotatsu-lost~{}", vm.id.as_str());
    let id = vm.id.as_str().to_owned();
    raw.call(move |c| {
        c.execute(
            "INSERT INTO bindings(tenant, microvm_id, claimed_at, kind)
             VALUES (?1, ?2, 1700000000, 1)",
            tokio_rusqlite::rusqlite::params![t, id],
        )
        .map(|_| ())
    })
    .await
    .unwrap();
    drop(raw);

    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.wait = WaitPolicy {
        timeout: Duration::from_millis(100),
        initial_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(10),
    };
    let pool = Arc::new(SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap());

    // maintain snapshots pending={} (no cleanup yet) and parks inside
    // its marker release — holding the capacity lock.
    store
        .gate_marker_release
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let m = {
        let pool = pool.clone();
        tokio::spawn(async move { pool.maintain().await.unwrap() })
    };
    tokio::time::timeout(
        Duration::from_secs(2),
        store.marker_release_entered.notified(),
    )
    .await
    .expect("maintain never reached the marker release");

    // The cleanup queues behind maintain's capacity lock: u1's bound
    // VM fails its wait and spawns the handoff task. `cp.get` stays
    // ungated until the acquire path is done with it — only the
    // cleanup's get must park.
    assert!(pool.acquire(&tenant("u1")).await.is_err());
    cp.gate_gets
        .store(true, std::sync::atomic::Ordering::SeqCst);
    // Free maintain's release — the marker is deleted, cap released;
    // the cleanup then pins a FRESH marker before its cp.get park.
    store.marker_release_gate.notify_one();
    m.await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), cp.get_entered.notified())
        .await
        .expect("cleanup never reached cp.get");

    drop(pool); // abort the cleanup mid-get
    cp.gate_gets
        .store(false, std::sync::atomic::Ordering::SeqCst);
    cp.get_gate.notify_waiters();
    tokio::time::sleep(Duration::from_millis(100)).await;

    // The re-pinned marker survived: the VM is durably tracked.
    let rows = store.list().await.unwrap();
    assert_eq!(rows.len(), 1, "the cleanup must have re-pinned a marker");
    assert!(rows[0].sentinel);
    assert_eq!(rows[0].microvm_id, vm.id);
    assert!(cp.get(&vm.id).await.unwrap().is_live());

    let mut cfg2 = PoolConfig::new(RunRequest::new("img"));
    cfg2.warm_size = 0;
    cfg2.max_vms = 10;
    let pool2 = SandboxPool::new(cp.clone(), store.clone(), cfg2).unwrap();
    pool2.maintain().await.unwrap();
    assert!(!cp.get(&vm.id).await.unwrap().is_live());
    assert!(store.list().await.unwrap().is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn concurrent_cleanups_never_duplicate_warm() {
    // Two acquires for one tenant can both hit the same wedged
    // binding and spawn a cleanup each — the second release reports
    // Ok(false)→Unbound and both would park the same VM. `warm` must
    // stay deduplicated: one physical VM, one entry — never the same
    // VM handed to two tenants.
    let cp = Arc::new(FlakyControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            boot_time: Duration::from_millis(200),
            ..Default::default()
        },
    ));
    let store = Arc::new(FlakyStore::new());
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    store
        .inner
        .claim(&Binding {
            tenant: tenant("u1"),
            microvm_id: vm.id.clone(),
            claimed_at_secs: 1_700_000_000,
            sentinel: false,
        })
        .await
        .unwrap();
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.wait = WaitPolicy {
        timeout: Duration::from_millis(60),
        initial_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(10),
    };
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    // Both acquires bind to the same VM and both waits fail — two
    // cleanups race to park it.
    let u1 = tenant("u1");
    let (r1, r2) = tokio::join!(pool.acquire(&u1), pool.acquire(&u1));
    assert!(r1.is_err() && r2.is_err());
    tokio::time::timeout(Duration::from_secs(2), async {
        while !store.list().await.unwrap().is_empty() || pool.stats().await.inflight != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("cleanups never settled");
    assert_eq!(
        pool.stats().await.warm,
        1,
        "two cleanups must not park the same VM twice"
    );

    // u2 takes the VM; u3 must never be handed the same physical VM.
    tokio::time::sleep(Duration::from_millis(300)).await; // past boot_time
    let s2 = pool.acquire(&tenant("u2")).await.unwrap();
    assert_eq!(*s2.vm().id(), vm.id);
    // A fresh launch timing out is fine — only a *share* is a bug.
    if let Ok(s3) = pool.acquire(&tenant("u3")).await {
        assert_ne!(
            *s3.vm().id(),
            vm.id,
            "the same physical VM was lent to two tenants"
        );
    }
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn marker_warm_coexist_keeps_vm_tracked() {
    // marker + warm coexistence (a marker release that failed during
    // cleanup, or a crash between pin and resolution): `maintain`'s
    // marker branch releases the stale marker because `warm` owns the
    // VM — and the sweep must KEEP it. A sentinel is not tenant
    // ownership; dropping the warm entry after the marker release
    // leaves the live VM tracked nowhere.
    let dir = std::env::temp_dir().join(format!("kotatsu-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("state.db");

    let cp = Arc::new(FlakyControlPlane::new());
    let store = Arc::new(FlakyStore::wrapping(Arc::new(
        kotatsu::SqliteStore::open(&path).await.unwrap(),
    )));
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 1;
    cfg.max_vms = 10;
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();
    pool.maintain().await.unwrap(); // fills `warm` with one VM
    assert_eq!(pool.stats().await.warm, 1);

    let vm_id = cp.list(None, None).await.unwrap()[0].id.clone();
    let raw = tokio_rusqlite::Connection::open(&path).await.unwrap();
    let t = format!("~kotatsu-lost~{}", vm_id.as_str());
    let id = vm_id.as_str().to_owned();
    raw.call(move |c| {
        c.execute(
            "INSERT INTO bindings(tenant, microvm_id, claimed_at, kind)
             VALUES (?1, ?2, 1700000000, 1)",
            tokio_rusqlite::rusqlite::params![t, id],
        )
        .map(|_| ())
    })
    .await
    .unwrap();
    drop(raw);

    pool.maintain().await.unwrap();
    assert!(
        store.list().await.unwrap().is_empty(),
        "the stale marker is released"
    );
    assert_eq!(
        pool.stats().await.warm,
        1,
        "the warm VM must stay tracked — the marker was not ownership"
    );
    // Identity matters: a dropped-then-refilled warm slot also reports
    // 1 — prove it's *this* VM and that no second one leaked.
    let s = pool.acquire(&tenant("u1")).await.unwrap();
    assert_eq!(
        *s.vm().id(),
        vm_id,
        "the original warm VM must survive the tick, not be replaced"
    );
    assert_eq!(
        cp.list(None, None).await.unwrap().len(),
        1,
        "no leaked VM besides the tracked one"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn bound_vm_wait_fail_reruns_cleanup() {
    // A bound VM that never becomes ready wedges the tenant: every
    // acquire hits the same binding and fails. The bound path now
    // runs the same release-or-track cleanup — after the store
    // recovers, a later attempt clears the binding.
    let cp = Arc::new(FlakyControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            boot_time: Duration::from_secs(3600),
            ..Default::default()
        },
    ));
    let store = Arc::new(FlakyStore::new());
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    store
        .inner
        .claim(&Binding {
            tenant: tenant("u1"),
            microvm_id: vm.id.clone(),
            claimed_at_secs: 1_700_000_000,
            sentinel: false,
        })
        .await
        .unwrap();
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.wait = WaitPolicy {
        timeout: Duration::from_millis(100),
        initial_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(10),
    };
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    // First attempt: the store is sick — the pin fails, the cleanup
    // bails, and the binding stays tracked (no wedge, no leak).
    store
        .claim_failures
        .store(1, std::sync::atomic::Ordering::SeqCst);
    assert!(pool.acquire(&tenant("u1")).await.is_err());
    tokio::time::timeout(Duration::from_secs(2), async {
        while pool.stats().await.inflight != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("cleanup never returned");
    assert!(
        store.get(&tenant("u1")).await.unwrap().is_some(),
        "an unverifiable pin must keep the binding"
    );

    // Second attempt: the store recovered — this cleanup pins,
    // releases the binding, and re-warms the VM.
    assert!(pool.acquire(&tenant("u1")).await.is_err());
    tokio::time::timeout(Duration::from_secs(2), async {
        while store.get(&tenant("u1")).await.unwrap().is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the wedged binding was never cleared");
    assert!(
        cp.get(&vm.id).await.unwrap().is_live(),
        "a live bound VM re-warms — it is recycled, not killed"
    );
}

#[tokio::test]
async fn detached_reaper_aborts_with_pool() {
    // A retrying terminator inside the pool's task set stops when the
    // pool drops — `terminate` calls cease, they never resume.
    let cp = Arc::new(FlakyControlPlane::new());
    cp.fail_terminate
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let store = Arc::new(FlakyStore::new());
    store
        .claim_failures
        .store(1, std::sync::atomic::Ordering::SeqCst);
    store
        .fail_get
        .store(true, std::sync::atomic::Ordering::SeqCst);
    store
        .get_ok_budget
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    assert!(pool.acquire(&tenant("u1")).await.is_err());
    tokio::time::timeout(Duration::from_secs(2), async {
        while cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("reaper never retried");
    drop(pool);
    let calls = cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst);
    // Longer than one backoff step — an unaborted reaper would retry.
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        cp.terminate_calls.load(std::sync::atomic::Ordering::SeqCst),
        calls,
        "the reaper kept terminating after the pool was dropped"
    );
}

#[tokio::test]
async fn finished_detached_tasks_are_drained() {
    // Completed cleanup tasks must not accumulate in the pool's task
    // set — each new spawn drains finished results first, so the set
    // holds only genuinely-live work.
    let cp = Arc::new(FlakyControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            boot_time: Duration::from_secs(3600),
            ..Default::default()
        },
    ));
    let store = Arc::new(FlakyStore::new());
    store
        .gate_release
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.wait = WaitPolicy {
        timeout: Duration::from_millis(100),
        initial_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(10),
    };
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    // First wait-fail cleanup: completes and stays parked as a
    // finished JoinSet entry until a later spawn drains it. The
    // marker release is the task's last step — the store going empty
    // means the task is done (give it one yield to fully return).
    assert!(pool.acquire(&tenant("u1")).await.is_err());
    tokio::time::timeout(Duration::from_secs(2), store.release_entered.notified())
        .await
        .expect("cleanup never entered the release gate");
    store.release_gate.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !store.list().await.unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // Let the now-unblocked task return so its JoinSet entry
        // reports finished.
        tokio::time::sleep(Duration::from_millis(20)).await;
    })
    .await
    .expect("cleanup never finished");

    // Second cleanup: its spawn drains the finished entry — the set
    // then holds only the newly-started task itself.
    store
        .gate_release
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(pool.acquire(&tenant("u2")).await.is_err());
    tokio::time::timeout(Duration::from_secs(2), store.release_entered.notified())
        .await
        .expect("second cleanup never entered the release gate");
    assert_eq!(
        pool.detached_task_count(),
        1,
        "finished results must be drained — only the live task may remain"
    );
    store.release_gate.notify_one();
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn legacy_prefixed_tenant_is_not_a_sentinel() {
    // A row whose tenant string exactly matches the sentinel shape —
    // `{prefix}{microvm_id}` pointing at that very VM — but stored
    // WITHOUT the explicit kind flag (a row written before sentinels
    // existed, or by a tenant that happened onto the same string) is
    // a normal binding. Its VM is never terminated and the row is
    // never deleted.
    let dir = std::env::temp_dir().join(format!("kotatsu-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("state.db");

    let cp = Arc::new(FlakyControlPlane::new());
    let store = Arc::new(kotatsu::SqliteStore::open(&path).await.unwrap());
    // A live VM under the pool's image first — the row must be an
    // exact self-reference to it.
    let vm = cp.run(&RunRequest::new(IMG_ARN)).await.unwrap();

    // Write the row raw (TenantKey::new rejects the prefix) with no
    // `kind` value — the column defaults to a normal binding.
    let raw = tokio_rusqlite::Connection::open(&path).await.unwrap();
    let t = format!("~kotatsu-lost~{}", vm.id.as_str());
    let id = vm.id.as_str().to_owned();
    raw.call(move |c| {
        c.execute(
            "INSERT INTO bindings(tenant, microvm_id, claimed_at)
             VALUES (?1, ?2, 1700000000)",
            tokio_rusqlite::rusqlite::params![t, id],
        )
        .map(|_| ())
    })
    .await
    .unwrap();
    drop(raw);

    // The row reads back as an ordinary, non-sentinel binding.
    let listed = store.list().await.unwrap();
    assert_eq!(listed.len(), 1);
    assert!(
        !listed[0].sentinel,
        "a legacy row with no kind flag must not be a sentinel"
    );

    // The pool sits on the *same* sqlite store holding the legacy
    // row — a MemoryStore pool would see an untracked VM instead.
    let mut cfg = PoolConfig::new(RunRequest::new(IMG_ARN));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.reap_lost_vms = true; // reconcile ON: the strongest adversary
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    // Two ticks: the VM is bound to a normal (if oddly-named) tenant —
    // never sentinel-reaped.
    pool.maintain().await.unwrap();
    pool.maintain().await.unwrap();
    assert!(
        cp.get(&vm.id).await.unwrap().is_live(),
        "a legacy prefixed tenant must not be sentinel-reaped"
    );
    let stats = pool.stats().await;
    assert_eq!(stats.assigned, 1, "the legacy row is a normal binding");
    assert_eq!(stats.lost, 0);
    let still = store.list().await.unwrap();
    assert!(
        still.len() == 1 && !still[0].sentinel,
        "the binding must remain — it is not a marker to clear"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn reserved_prefix_rejected_on_deserialize() {
    // Deserialization must not bypass `TenantKey::new` — a crafted
    // payload could otherwise smuggle a sentinel-looking key in.
    for bad in [
        "\"~kotatsu-lost~x\"",
        "\"~kotatsu-lost~microvm-000000000001\"",
        "\"has space\"",
        "\"\"",
    ] {
        assert!(
            serde_json::from_str::<TenantKey>(bad).is_err(),
            "invalid key must fail deserialization: {bad}"
        );
    }
    assert_eq!(
        serde_json::from_str::<TenantKey>("\"tenant~1\"").unwrap(),
        tenant("tenant~1")
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn binding_plus_marker_on_one_vm_counts_once() {
    // A crash between the cleanup's pin and its tenant release can
    // leave a marker AND a normal binding on the same VM. The binding
    // already counts it via `assigned` — the marker must not add
    // `lost` on top, or `max_vms` exhausts early and hands out
    // PoolExhausted while capacity remains.
    let dir = std::env::temp_dir().join(format!("kotatsu-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("state.db");

    let cp = Arc::new(FlakyControlPlane::new());
    let store = Arc::new(FlakyStore::wrapping(Arc::new(
        kotatsu::SqliteStore::open(&path).await.unwrap(),
    )));
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    store
        .inner
        .claim(&Binding {
            tenant: tenant("u1"),
            microvm_id: vm.id.clone(),
            claimed_at_secs: 1_700_000_000,
            sentinel: false,
        })
        .await
        .unwrap();
    let raw = tokio_rusqlite::Connection::open(&path).await.unwrap();
    let t = format!("~kotatsu-lost~{}", vm.id.as_str());
    let id = vm.id.as_str().to_owned();
    raw.call(move |c| {
        c.execute(
            "INSERT INTO bindings(tenant, microvm_id, claimed_at, kind)
             VALUES (?1, ?2, 1700000000, 1)",
            tokio_rusqlite::rusqlite::params![t, id],
        )
        .map(|_| ())
    })
    .await
    .unwrap();
    drop(raw);

    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 2;
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    let stats = pool.stats().await;
    assert_eq!(stats.assigned, 1);
    assert_eq!(
        stats.lost, 0,
        "the marker's VM is held by the normal binding — not lost"
    );

    // max_vms=2 leaves exactly one slot: a second tenant can only
    // reserve if the bound+marked VM counted once, not twice.
    let s2 = pool.acquire(&tenant("u2")).await.unwrap();
    assert_ne!(*s2.vm().id(), vm.id);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn stats_counts_once_across_cleanup_handoff() {
    // Two cleanups converge on one wedged VM and stall mid-handoff —
    // one parked at its sentinel claim holding `capacity`, the other
    // at its marker release with the VM already in `warm`. `stats`
    // reads the store rows and the pool sets a moment apart, but the
    // dedup rules must still count that one physical VM exactly once
    // across `warm + inflight + assigned + lost`.
    let cp = Arc::new(FlakyControlPlane::with_behavior(
        kotatsu::mock::MockBehavior {
            boot_time: Duration::from_secs(3600),
            ..Default::default()
        },
    ));
    let store = Arc::new(FlakyStore::new());
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    store
        .inner
        .claim(&Binding {
            tenant: tenant("u1"),
            microvm_id: vm.id.clone(),
            claimed_at_secs: 1_700_000_000,
            sentinel: false,
        })
        .await
        .unwrap();
    let mut cfg = PoolConfig::new(RunRequest::new("img"));
    cfg.warm_size = 0;
    cfg.max_vms = 10;
    cfg.wait = WaitPolicy {
        timeout: Duration::from_millis(100),
        initial_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(10),
    };
    let pool = SandboxPool::new(cp.clone(), store.clone(), cfg).unwrap();

    // The second cleanup's sentinel claim parks *inside* the capacity
    // section, owning its slot; the first cleanup proceeds, parks the
    // VM in `warm`, and stalls at its marker release — slot held.
    store
        .gate_marker_release
        .store(true, std::sync::atomic::Ordering::SeqCst);
    store
        .sentinel_claim_ordinal
        .store(2, std::sync::atomic::Ordering::SeqCst);
    store
        .gate_sentinel_claim
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let u1 = tenant("u1");
    let (r1, r2) = tokio::join!(pool.acquire(&u1), pool.acquire(&u1));
    assert!(r1.is_err() && r2.is_err());
    tokio::time::timeout(
        Duration::from_secs(2),
        store.sentinel_claim_entered.notified(),
    )
    .await
    .expect("the second cleanup never reached its sentinel claim");
    tokio::time::timeout(
        Duration::from_secs(2),
        store.marker_release_entered.notified(),
    )
    .await
    .expect("the first cleanup never reached its marker release");

    // First observation — mid-handoff: the second cleanup's `Slot`
    // still owns pending+inflight while `warm` already holds the VM —
    // dedup must keep the count at one.
    let stats = pool.stats().await;
    assert_eq!(stats.warm, 1);
    assert_eq!(
        stats.warm + stats.inflight + stats.assigned + stats.lost,
        1,
        "one physical VM, one count across all buckets"
    );

    // Second observation — provably progressed: free the claim, gate
    // the cleanup's own `cp.get`, and wait for it to arrive *with its
    // slot still armed* — pending+inflight still overlap `warm`, so
    // the dedup is still load-bearing here.
    cp.gate_gets
        .store(true, std::sync::atomic::Ordering::SeqCst);
    store.sentinel_claim_gate.notify_one();
    tokio::time::timeout(Duration::from_secs(2), cp.get_entered.notified())
        .await
        .expect("the freed cleanup never reached its cp.get");
    let stats = pool.stats().await;
    assert_eq!(stats.warm, 1);
    assert_eq!(
        stats.warm + stats.inflight + stats.assigned + stats.lost,
        1,
        "one physical VM, one count across all buckets"
    );

    // Free everything: the get, then the first cleanup's release.
    cp.get_gate.notify_waiters();
    store.marker_release_gate.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !store.list().await.unwrap().is_empty() || pool.stats().await.inflight != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the parked cleanup never settled");
    assert_eq!(pool.stats().await.warm, 1);
}
