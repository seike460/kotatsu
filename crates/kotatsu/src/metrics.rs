//! Metric names and recording helpers.
//!
//! Everything routes through the `metrics` facade: without a recorder
//! installed these calls are no-ops, so the core crate costs nothing when
//! unused. `kotatsud` installs a Prometheus exporter at `/metrics` and
//! read these exact names — do not rename without updating both ends.
//!
//! `assigned` lags slightly: it is derived from the store and only
//! refreshed by [`crate::pool::SandboxPool::stats`], so the daemon should
//! poll `stats()` on a tick. `warm`/`inflight` update at every mutation.

use std::time::Duration;

/// Metric name constants (Prometheus-exportable, `snake_case`).
pub mod names {
    /// Pool acquires, labeled `outcome` = ok | exhausted | error.
    pub const ACQUIRE_TOTAL: &str = "kotatsu_pool_acquire_total";
    /// Acquire latency in seconds (wait + boot/resume included).
    pub const ACQUIRE_SECONDS: &str = "kotatsu_pool_acquire_seconds";
    /// MicroVM launches issued by the pool.
    pub const LAUNCH_TOTAL: &str = "kotatsu_vm_launch_total";
    /// `terminate-microvm` calls issued (by reaper, release, drain) —
    /// failures included, one increment per call attempted. Recorded
    /// before the call awaits, so a cancelled in-flight call counts.
    pub const TERMINATE_TOTAL: &str = "kotatsu_vm_terminate_total";
    /// Successful suspend calls (completions — unlike terminate,
    /// failed suspends are not counted).
    pub const SUSPEND_TOTAL: &str = "kotatsu_vm_suspend_total";
    /// Resume calls (programmatic resume via waiter).
    pub const RESUME_TOTAL: &str = "kotatsu_vm_resume_total";
    /// Current warm VM count.
    pub const POOL_WARM: &str = "kotatsu_pool_warm";
    /// Current in-flight (handoff) VM count.
    pub const POOL_INFLIGHT: &str = "kotatsu_pool_inflight";
    /// Current tenant-bound VM count.
    pub const POOL_ASSIGNED: &str = "kotatsu_pool_assigned";
    /// Bindings dropped per maintain tick.
    pub const BINDINGS_DROPPED: &str = "kotatsu_pool_bindings_dropped_total";
    /// VMs reaped for max_age.
    pub const REAPED_TOTAL: &str = "kotatsu_pool_reaped_total";
    /// Warm VMs launched to reach the (possibly scheduled) target.
    pub const WARMED_TOTAL: &str = "kotatsu_pool_warmed_total";
    /// Warm VMs terminated when a `warm_schedule` window lowered the
    /// target below the live warm count.
    pub const SHRUNK_TOTAL: &str = "kotatsu_pool_shrunk_total";
    /// Warm entries dropped because the VM vanished outside the pool.
    pub const WARM_DROPPED_TOTAL: &str = "kotatsu_pool_warm_dropped_total";
    /// Warm entries dropped because a store binding owns the VM.
    pub const BOUND_DROPPED_TOTAL: &str = "kotatsu_pool_bound_dropped_total";
    /// HTTP proxy requests, labeled `status_class` = 2xx…5xx | other.
    pub const GATEWAY_REQUESTS: &str = "kotatsu_gateway_requests_total";
    /// HTTP proxy latency in seconds.
    pub const GATEWAY_SECONDS: &str = "kotatsu_gateway_request_seconds";
    /// Active WebSocket sessions.
    pub const GATEWAY_WS_ACTIVE: &str = "kotatsu_gateway_ws_active";
    /// Token mint attempts issued (cache misses; failures included).
    pub const TOKEN_MINT_TOTAL: &str = "kotatsu_token_mint_total";
    /// Token cache hits.
    pub const TOKEN_CACHE_HIT: &str = "kotatsu_token_cache_hit_total";
}

/// Outcome of a pool acquire, as a low-cardinality metric label.
#[derive(Clone, Copy, Debug)]
pub enum AcquireOutcome {
    /// A running sandbox was handed out.
    Ok,
    /// The pool was full (`Error::PoolExhausted`) — expected backpressure,
    /// not a fault.
    Exhausted,
    /// Any other failure.
    Error,
}

impl AcquireOutcome {
    fn as_label(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Exhausted => "exhausted",
            Self::Error => "error",
        }
    }
}

/// Records a pool acquire outcome and duration.
pub fn record_acquire(outcome: AcquireOutcome, elapsed: Duration) {
    metrics::counter!(names::ACQUIRE_TOTAL, "outcome" => outcome.as_label()).increment(1);
    metrics::histogram!(names::ACQUIRE_SECONDS).record(elapsed.as_secs_f64());
}

/// Records a `run-microvm` issued by the pool.
pub fn record_launch() {
    metrics::counter!(names::LAUNCH_TOTAL).increment(1);
}

/// Records a `terminate-microvm` issued anywhere.
pub fn record_terminate() {
    metrics::counter!(names::TERMINATE_TOTAL).increment(1);
}

/// Records a `suspend-microvm`.
pub fn record_suspend() {
    metrics::counter!(names::SUSPEND_TOTAL).increment(1);
}

/// Records a `resume-microvm`.
pub fn record_resume() {
    metrics::counter!(names::RESUME_TOTAL).increment(1);
}

/// Sets pool gauges from a stats snapshot.
pub fn set_pool_stats(stats: &crate::pool::PoolStats) {
    metrics::gauge!(names::POOL_WARM).set(stats.warm as f64);
    metrics::gauge!(names::POOL_INFLIGHT).set(stats.inflight as f64);
    metrics::gauge!(names::POOL_ASSIGNED).set(stats.assigned as f64);
}

/// Records a maintenance tick's reaper activity.
pub fn record_maintain(report: &crate::pool::PoolReport) {
    metrics::counter!(names::BINDINGS_DROPPED).increment(report.bindings_dropped as u64);
    metrics::counter!(names::REAPED_TOTAL).increment(report.reaped as u64);
    metrics::counter!(names::WARM_DROPPED_TOTAL).increment(report.warm_dropped as u64);
    metrics::counter!(names::BOUND_DROPPED_TOTAL).increment(report.bound_dropped as u64);
}

/// Records a warm VM launched to reach the warm target.
pub fn record_warmed() {
    metrics::counter!(names::WARMED_TOTAL).increment(1);
}

/// Records a warm VM terminated by a `warm_schedule` scale-down.
pub fn record_shrunk() {
    metrics::counter!(names::SHRUNK_TOTAL).increment(1);
}

/// Sets the warm/inflight gauges from `PoolInner` mutation points.
pub(crate) fn set_warm_inflight(warm: usize, inflight: usize) {
    metrics::gauge!(names::POOL_WARM).set(warm as f64);
    metrics::gauge!(names::POOL_INFLIGHT).set(inflight as f64);
}

/// Records a proxied HTTP request. `status` is bucketed to a
/// `2xx`/`3xx`/`4xx`/`5xx`/`other` class to keep cardinality bounded.
pub fn record_http_request(status: u16, elapsed: Duration) {
    let class = match status {
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "other",
    };
    metrics::counter!(names::GATEWAY_REQUESTS, "status_class" => class).increment(1);
    metrics::histogram!(names::GATEWAY_SECONDS).record(elapsed.as_secs_f64());
}

/// Increments/decrements the active WebSocket gauge.
pub fn ws_session(open: bool) {
    metrics::gauge!(names::GATEWAY_WS_ACTIVE).increment(if open { 1.0 } else { -1.0 });
}

/// Records a token mint (cache miss) or hit.
pub fn record_token(hit: bool) {
    if hit {
        metrics::counter!(names::TOKEN_CACHE_HIT).increment(1);
    } else {
        metrics::counter!(names::TOKEN_MINT_TOTAL).increment(1);
    }
}
