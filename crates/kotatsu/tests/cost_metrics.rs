//! Tests for `cost` (AWS-example reproduction) and `metrics`
//! (recorder-backed assertions + no-op safety).

use kotatsu::cost::{MicrovmSpec, PriceBook, Usage};
use kotatsu::mock::MockControlPlane;
use kotatsu::{
    ControlPlane, MemoryStore, PoolConfig, RunRequest, SandboxPool, TenantKey, WaitPolicy,
};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

// ---------- cost ----------

#[test]
fn spec_derives_envelope_from_memory_tier() {
    let s = MicrovmSpec::baseline(2).unwrap();
    assert_eq!(s.baseline_vcpu_dec(), dec!(1));
    assert_eq!(s.peak_gb(), 8);
    assert_eq!(s.peak_vcpu_dec(), dec!(4));

    let s = MicrovmSpec::baseline(1).unwrap();
    assert_eq!(s.baseline_vcpu_dec(), dec!(0.5));
    assert_eq!(s.peak_gb(), 4);
    assert_eq!(s.peak_vcpu_dec(), dec!(2));
}

#[test]
fn spec_rejects_non_tiers() {
    for gb in [0, 3, 6, 16] {
        assert!(MicrovmSpec::baseline(gb).is_err(), "gb={gb} must fail");
    }
    for gb in [1, 2, 4, 8] {
        assert!(MicrovmSpec::baseline(gb).is_ok(), "gb={gb} must pass");
    }
}

/// Reproduces AWS's published "Pricing Example 1" (sandboxed coding
/// environments) end-to-end:
///
/// - 100 developers, 20 days each, 2.5 h/day active
///   → 13.5M baseline-seconds + 4.5M peak-seconds
/// - 12,000 suspend/resume cycles, 2,000 launches
/// - 22,000 GB-hours suspended state (100 devs × 2 GB × 5.5 h × 20 days)
/// - 5 × 2 GB images
///
/// AWS totals: compute $1,103.38, snapshot I/O $134.60, storage $3.24,
/// grand total $1,241.22.
#[test]
fn estimate_matches_aws_pricing_example_1() {
    let spec = MicrovmSpec::baseline(2).unwrap();
    let usage = Usage {
        spec,
        baseline_seconds: 13_500_000,
        peak_seconds: 4_500_000,
        suspends: 12_000,
        resumes: 12_000,
        launches: 2_000,
        suspended_gb_hours: dec!(22_000),
        image_gb: 2,
        image_count: 5,
    };
    let b = PriceBook::us_east_1().estimate(&usage).unwrap();

    let near = |got: Decimal, want: Decimal| {
        assert!((got - want).abs() <= dec!(0.05), "want ≈{want}, got {got}");
    };
    near(b.compute, dec!(1103.38));
    near(b.snapshot_reads, dec!(43.40));
    near(b.snapshot_writes, dec!(91.20));
    near(b.snapshot_reads + b.snapshot_writes, dec!(134.60));
    near(b.snapshot_storage, dec!(3.24));
    near(b.total, dec!(1241.22));
}

#[test]
fn zero_usage_is_free() {
    let usage = Usage::new(MicrovmSpec::default_tier());
    let b = PriceBook::us_east_1().estimate(&usage).unwrap();
    assert_eq!(b.total, Decimal::ZERO);
}

/// Compute charges nothing while suspended — the example's whole point.
#[test]
fn suspended_only_incurs_no_compute() {
    let mut usage = Usage::new(MicrovmSpec::default_tier());
    usage.suspended_gb_hours = dec!(720); // 1 GB-month held suspended
    let b = PriceBook::us_east_1().estimate(&usage).unwrap();
    assert_eq!(b.compute, Decimal::ZERO);
    assert_eq!(b.snapshot_storage, dec!(0.08));
    assert_eq!(b.total, dec!(0.08));
}

#[test]
fn suspend_without_resume_still_bills_write() {
    let mut usage = Usage::new(MicrovmSpec::default_tier());
    usage.suspends = 1;
    usage.resumes = 0;
    let b = PriceBook::us_east_1().estimate(&usage).unwrap();
    assert_eq!(b.snapshot_writes, dec!(0.0076)); // 2 GB × $0.0038
    assert_eq!(b.snapshot_reads, Decimal::ZERO);
}

/// Launch reads the *image* snapshot (image_gb), not the memory tier.
#[test]
fn launch_reads_image_snapshot_not_baseline() {
    let mut usage = Usage::new(MicrovmSpec::baseline(4).unwrap());
    usage.launches = 100;
    usage.image_gb = 10; // image larger than memory tier
    let b = PriceBook::us_east_1().estimate(&usage).unwrap();
    assert_eq!(b.snapshot_reads, dec!(1.55)); // 100 × 10 GB × $0.00155
}

#[test]
fn negative_inputs_are_rejected() {
    let mut book = PriceBook::us_east_1();
    book.mem_gb_per_second = dec!(-1);
    assert!(
        book.estimate(&Usage::new(MicrovmSpec::default_tier()))
            .is_err()
    );

    let mut usage = Usage::new(MicrovmSpec::default_tier());
    usage.suspended_gb_hours = dec!(-5);
    assert!(PriceBook::us_east_1().estimate(&usage).is_err());
}

#[test]
fn custom_pricebook_overrides_default() {
    let mut book = PriceBook::us_east_1();
    book.mem_gb_per_second = Decimal::ZERO;
    book.vcpu_per_second = Decimal::ZERO;
    let mut usage = Usage::new(MicrovmSpec::default_tier());
    usage.baseline_seconds = 3_600;
    let b = book.estimate(&usage).unwrap();
    assert_eq!(b.compute, Decimal::ZERO);
}

// ---------- metrics ----------

/// Minimal recorder: `with_local_recorder` scopes it to one call so tests
/// stay isolated without installing a global recorder.
#[derive(Default)]
struct TestRecorder {
    counters: Arc<Mutex<HashMap<String, u64>>>,
    gauges: Arc<Mutex<HashMap<String, f64>>>,
    histograms: Arc<Mutex<HashMap<String, usize>>>,
}

fn key_id(key: &metrics::Key) -> String {
    let labels = key
        .labels()
        .map(|l| format!("{}={}", l.key(), l.value()))
        .collect::<Vec<_>>()
        .join(",");
    format!("{}{{{}}}", key.name(), labels)
}

struct CounterHandle {
    map: Arc<Mutex<HashMap<String, u64>>>,
    id: String,
}
impl metrics::CounterFn for CounterHandle {
    fn increment(&self, v: u64) {
        *self.map.lock().entry(self.id.clone()).or_default() += v;
    }
    fn absolute(&self, v: u64) {
        self.map.lock().insert(self.id.clone(), v);
    }
}

struct GaugeHandle {
    map: Arc<Mutex<HashMap<String, f64>>>,
    id: String,
}
impl metrics::GaugeFn for GaugeHandle {
    fn increment(&self, v: f64) {
        *self.map.lock().entry(self.id.clone()).or_default() += v;
    }
    fn decrement(&self, v: f64) {
        *self.map.lock().entry(self.id.clone()).or_default() -= v;
    }
    fn set(&self, v: f64) {
        self.map.lock().insert(self.id.clone(), v);
    }
}

struct HistogramHandle {
    map: Arc<Mutex<HashMap<String, usize>>>,
    id: String,
}
impl metrics::HistogramFn for HistogramHandle {
    fn record(&self, _v: f64) {
        *self.map.lock().entry(self.id.clone()).or_default() += 1;
    }
}

impl metrics::Recorder for TestRecorder {
    fn describe_counter(
        &self,
        _n: metrics::KeyName,
        _u: Option<metrics::Unit>,
        _d: metrics::SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _n: metrics::KeyName,
        _u: Option<metrics::Unit>,
        _d: metrics::SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _n: metrics::KeyName,
        _u: Option<metrics::Unit>,
        _d: metrics::SharedString,
    ) {
    }
    fn register_counter(&self, key: &metrics::Key, _m: &metrics::Metadata<'_>) -> metrics::Counter {
        metrics::Counter::from_arc(Arc::new(CounterHandle {
            map: self.counters.clone(),
            id: key_id(key),
        }))
    }
    fn register_gauge(&self, key: &metrics::Key, _m: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::from_arc(Arc::new(GaugeHandle {
            map: self.gauges.clone(),
            id: key_id(key),
        }))
    }
    fn register_histogram(
        &self,
        key: &metrics::Key,
        _m: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::from_arc(Arc::new(HistogramHandle {
            map: self.histograms.clone(),
            id: key_id(key),
        }))
    }
}

#[test]
fn metrics_calls_are_noop_safe_without_recorder() {
    kotatsu::metrics::record_launch();
    kotatsu::metrics::record_acquire(
        kotatsu::metrics::AcquireOutcome::Ok,
        Duration::from_millis(3),
    );
    kotatsu::metrics::record_token(true);
    kotatsu::metrics::record_token(false);
    kotatsu::metrics::ws_session(true);
    kotatsu::metrics::ws_session(false);
    kotatsu::metrics::record_http_request(200, Duration::from_millis(1));
}

#[test]
fn metrics_record_under_local_recorder() {
    let rec = TestRecorder::default();
    let counters = rec.counters.clone();
    let histograms = rec.histograms.clone();
    metrics::with_local_recorder(&rec, || {
        kotatsu::metrics::record_launch();
        kotatsu::metrics::record_launch();
        kotatsu::metrics::record_terminate();
        kotatsu::metrics::record_acquire(
            kotatsu::metrics::AcquireOutcome::Ok,
            Duration::from_millis(5),
        );
        kotatsu::metrics::record_token(true);
    });

    let counters = counters.lock();
    assert_eq!(
        counters.get("kotatsu_vm_launch_total{}"),
        Some(&2),
        "counters: {counters:?}"
    );
    assert_eq!(counters.get("kotatsu_vm_terminate_total{}"), Some(&1));
    assert_eq!(
        counters.get("kotatsu_pool_acquire_total{outcome=ok}"),
        Some(&1)
    );
    assert_eq!(counters.get("kotatsu_token_cache_hit_total{}"), Some(&1));
    let hist = histograms.lock();
    assert_eq!(hist.get("kotatsu_pool_acquire_seconds{}"), Some(&1));
}

/// Integration: a real acquire drives the instrumentation, and metrics
/// carry only low-cardinality labels (outcome — never tenant/VM ids).
/// `with_local_recorder` is sync-scoped, so we re-enter the runtime with
/// the documented `block_in_place` + `Handle::block_on` pattern.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pool_acquire_emits_metrics() {
    let rec = TestRecorder::default();
    let counters = rec.counters.clone();
    let gauges = rec.gauges.clone();
    metrics::with_local_recorder(&rec, || {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let cp = Arc::new(MockControlPlane::new());
                let mut cfg = PoolConfig::new(RunRequest::new("img"));
                cfg.warm_size = 0;
                cfg.wait = WaitPolicy {
                    timeout: Duration::from_secs(5),
                    initial_delay: Duration::from_millis(5),
                    max_delay: Duration::from_millis(20),
                };
                let pool = SandboxPool::new(cp, Arc::new(MemoryStore::new()), cfg).unwrap();
                pool.acquire(&TenantKey::new("u1").unwrap()).await.unwrap();
                pool.stats().await;
            })
        })
    });

    let counters = counters.lock();
    assert_eq!(counters.get("kotatsu_vm_launch_total{}"), Some(&1));
    assert_eq!(
        counters.get("kotatsu_pool_acquire_total{outcome=ok}"),
        Some(&1)
    );
    // No label may carry tenant or VM identity.
    for k in counters.keys() {
        assert!(
            !k.contains("u1") && !k.contains("microvm-"),
            "label leak: {k}"
        );
    }
    let gauges = gauges.lock();
    assert_eq!(gauges.get("kotatsu_pool_assigned{}"), Some(&1.0));
    // Gauges update at mutation points, not only via stats().
    assert_eq!(gauges.get("kotatsu_pool_inflight{}"), Some(&0.0));
    assert_eq!(gauges.get("kotatsu_pool_warm{}"), Some(&0.0));
}

/// Release and maintain paths also emit counters — a dead binding is
/// dropped by the sweep and the release's terminate is counted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn release_and_maintain_emit_counters() {
    let rec = TestRecorder::default();
    let counters = rec.counters.clone();
    metrics::with_local_recorder(&rec, || {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let cp = Arc::new(MockControlPlane::new());
                let mut cfg = PoolConfig::new(RunRequest::new("img"));
                cfg.warm_size = 0;
                let pool = SandboxPool::new(cp.clone(), Arc::new(MemoryStore::new()), cfg).unwrap();
                let sb = pool.acquire(&TenantKey::new("u1").unwrap()).await.unwrap();
                let id = sb.vm().id().clone();
                sb.release().await.unwrap();
                assert_eq!(cp.get(&id).await.unwrap().state, kotatsu::State::Terminated);

                // A fresh binding whose VM dies is dropped by maintain.
                let sb2 = pool.acquire(&TenantKey::new("u2").unwrap()).await.unwrap();
                let id2 = sb2.vm().id().clone();
                drop(sb2);
                cp.terminate(&id2).await.unwrap();
                let report = pool.maintain().await.unwrap();
                assert_eq!(report.bindings_dropped, 1);
            })
        })
    });

    let counters = counters.lock();
    assert!(
        counters.get("kotatsu_vm_terminate_total{}") >= Some(&1),
        "terminate counter missing: {counters:?}"
    );
    assert_eq!(
        counters.get("kotatsu_pool_bindings_dropped_total{}"),
        Some(&1)
    );
    assert_eq!(counters.get("kotatsu_vm_launch_total{}"), Some(&2));
}

/// Wraps `MockControlPlane` so `terminate` blocks until `gate` is
/// notified — lets a test drop the call while it is genuinely in
/// flight.
struct TermGateCp {
    inner: MockControlPlane,
    gate: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl ControlPlane for TermGateCp {
    async fn run(&self, req: &RunRequest) -> kotatsu::Result<kotatsu::Microvm> {
        self.inner.run(req).await
    }
    async fn get(&self, id: &kotatsu::MicrovmId) -> kotatsu::Result<kotatsu::Microvm> {
        self.inner.get(id).await
    }
    async fn suspend(&self, id: &kotatsu::MicrovmId) -> kotatsu::Result<()> {
        self.inner.suspend(id).await
    }
    async fn resume(&self, id: &kotatsu::MicrovmId) -> kotatsu::Result<()> {
        self.inner.resume(id).await
    }
    async fn terminate(&self, id: &kotatsu::MicrovmId) -> kotatsu::Result<()> {
        self.gate.notified().await;
        self.inner.terminate(id).await
    }
    async fn list(
        &self,
        image_identifier: Option<&str>,
        image_version: Option<&str>,
    ) -> kotatsu::Result<Vec<kotatsu::MicrovmSummary>> {
        self.inner.list(image_identifier, image_version).await
    }
    async fn mint_token(
        &self,
        id: &kotatsu::MicrovmId,
        scope: &[kotatsu::PortSpec],
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

/// `record_terminate` counts calls *issued*, not completed: a
/// `release` dropped while its terminate is in flight must still
/// record the increment.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminate_counts_when_issued_even_if_interrupted() {
    let rec = TestRecorder::default();
    let counters = rec.counters.clone();
    metrics::with_local_recorder(&rec, || {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let cp = Arc::new(TermGateCp {
                    inner: MockControlPlane::new(),
                    // The gate is never notified: terminate stays in
                    // flight so the future can be dropped mid-call.
                    gate: Arc::new(tokio::sync::Notify::new()),
                });
                let mut cfg = PoolConfig::new(RunRequest::new("img"));
                cfg.warm_size = 0;
                let pool = SandboxPool::new(cp, Arc::new(MemoryStore::new()), cfg).unwrap();
                let sb = pool.acquire(&TenantKey::new("u1").unwrap()).await.unwrap();
                tokio::select! {
                    _ = sb.release() => unreachable!("gated terminate must not complete"),
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                }
            })
        })
    });

    let counters = counters.lock();
    assert_eq!(
        counters.get("kotatsu_vm_terminate_total{}"),
        Some(&1),
        "issued-but-interrupted terminate was not counted: {counters:?}"
    );
}
