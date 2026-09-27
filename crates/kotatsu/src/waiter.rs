//! Pollers that bridge the gap between API calls and observable state.
//!
//! `run-microvm` returns while the MicroVM is still `PENDING`, and
//! `resume-microvm` is asynchronous. Callers that need a usable MicroVM
//! should not hand-roll sleep loops; these waiters implement bounded
//! exponential backoff and translate "the VM went somewhere else" into
//! typed errors.

use std::time::{Duration, Instant};

use crate::control_plane::ControlPlane;
use crate::error::{Error, Result};
use crate::types::{Microvm, MicrovmId, RunningVm, State};

/// Budget and pacing for state waiters.
#[derive(Clone, Debug)]
pub struct WaitPolicy {
    /// Total time budget before giving up.
    pub timeout: Duration,
    /// First poll interval.
    pub initial_delay: Duration,
    /// Poll interval ceiling.
    pub max_delay: Duration,
}

impl Default for WaitPolicy {
    /// 100ms→2s exponential backoff for up to two minutes.
    ///
    /// Two minutes covers documented resume latencies (seconds) with
    /// generous headroom for snapshot restores; gateways should pick a
    /// tighter budget matching their upstream timeout.
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(120),
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(2),
        }
    }
}

impl WaitPolicy {
    /// Rejects degenerate policies that would spin without yielding:
    /// every field must be positive and `max_delay` must cover
    /// `initial_delay`. Both waiters call this first, so a hand-rolled
    /// policy can never degrade into a `sleep(0)` busy-poll against the
    /// AWS API.
    pub fn validate(&self) -> Result<()> {
        if self.timeout.is_zero() || self.initial_delay.is_zero() || self.max_delay.is_zero() {
            return Err(Error::invalid("WaitPolicy: all durations must be positive"));
        }
        if self.timeout > Duration::from_secs(31_536_000)
            || self.max_delay > Duration::from_secs(31_536_000)
        {
            // Bound absurd budgets: `Instant + Duration` panics on overflow.
            return Err(Error::invalid(
                "WaitPolicy: timeout/max_delay exceeds one year",
            ));
        }
        if self.max_delay < self.initial_delay {
            return Err(Error::invalid(
                "WaitPolicy: max_delay must be >= initial_delay",
            ));
        }
        Ok(())
    }
}

/// Polls `get-microvm` until the MicroVM reaches `want` or leaves the live
/// state set.
///
/// Returns [`Error::UnexpectedState`] when the VM transitions to a
/// non-live state while `want` is a live state (e.g. it terminated while
/// waiting for `SUSPENDED`). When `want` is itself non-live (e.g.
/// `TERMINATED` for a confirm-terminate flow), non-live intermediate
/// states such as `TERMINATING` keep polling instead of aborting.
/// Returns [`Error::WaitTimeout`] when the budget expires.
pub async fn wait_for_state<C: ControlPlane + ?Sized>(
    cp: &C,
    id: &MicrovmId,
    want: &State,
    policy: &WaitPolicy,
) -> Result<Microvm> {
    policy.validate()?;
    let deadline = Instant::now() + policy.timeout;
    let mut delay = policy.initial_delay;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(wait_timeout(id, want, policy));
        }
        // Bound the API call itself: a stalled control-plane connection
        // must not stretch the caller's budget past `policy.timeout`.
        match tokio::time::timeout(remaining, cp.get(id)).await {
            Ok(Ok(vm)) => {
                if vm.state == *want {
                    return Ok(vm);
                }
                if !vm.state.is_live() && want.is_live() {
                    return Err(Error::UnexpectedState {
                        id: id.to_string(),
                        expected: want.to_string(),
                        got: vm.state.to_string(),
                    });
                }
            }
            // A throttled or transport-failed `get` must not kill the
            // wait while budget remains.
            Ok(Err(e)) if e.is_transient() => {
                tracing::debug!(
                    microvm = %id,
                    error = %e,
                    "get-microvm failed transiently; will retry"
                );
            }
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(wait_timeout(id, want, policy)),
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(wait_timeout(id, want, policy));
        }
        tokio::time::sleep(delay.min(remaining)).await;
        delay = delay.saturating_mul(2).min(policy.max_delay);
    }
}

fn wait_timeout(id: &MicrovmId, want: &State, policy: &WaitPolicy) -> Error {
    Error::WaitTimeout {
        id: id.to_string(),
        state: want.to_string(),
        timeout: policy.timeout,
    }
}

/// Drives a MicroVM to `RUNNING`, resuming it when suspended.
///
/// The gateway's "hold the request through a resume" path uses this: a
/// `SUSPENDED` VM triggers `resume-microvm` (rate-limited to one call
/// per `max_delay`, since the transition itself is asynchronous) until
/// it comes up. A resume error is retried on the same cadence only when
/// it is transient (transport timeout, dispatch failure, 409 conflict,
/// 429/5xx) — [`Error::NotFound`] and permanent rejections such as
/// AccessDenied surface immediately instead of expiring as
/// [`Error::WaitTimeout`].
/// Both `get` and `resume` calls are individually bounded by the
/// remaining budget, so a stalled control plane cannot stretch the
/// wait past `policy.timeout`.
pub async fn wait_until_running<C: ControlPlane + ?Sized>(
    cp: &C,
    id: &MicrovmId,
    policy: &WaitPolicy,
) -> Result<RunningVm> {
    policy.validate()?;
    let deadline = Instant::now() + policy.timeout;
    let mut delay = policy.initial_delay;
    // Resume is async: after a successful call the VM can still read
    // SUSPENDED for a while. Rate-limit re-issues to one per max_delay so
    // a slow resume does not turn into a resume-microvm call every poll.
    let mut resume_after = Instant::now()
        .checked_sub(policy.max_delay)
        .unwrap_or_else(Instant::now);
    let want = State::Running;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(wait_timeout(id, &want, policy));
        }
        match tokio::time::timeout(remaining, cp.get(id)).await {
            Ok(Ok(vm)) => match vm.state {
                State::Running => return RunningVm::try_from_vm(vm),
                State::Suspended if Instant::now() >= resume_after => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(wait_timeout(id, &want, policy));
                    }
                    crate::metrics::record_resume();
                    match tokio::time::timeout(remaining, cp.resume(id)).await {
                        Err(_) => return Err(wait_timeout(id, &want, policy)),
                        Ok(Ok(())) => {}
                        Ok(Err(e @ Error::NotFound { .. })) => return Err(e),
                        Ok(Err(e)) if !e.is_transient() => return Err(e),
                        Ok(Err(e)) => {
                            tracing::debug!(
                                microvm = %id,
                                error = %e,
                                "resume-microvm rejected transiently; will retry"
                            );
                        }
                    }
                    resume_after = Instant::now() + policy.max_delay;
                }
                ref s if !s.is_live() => {
                    return Err(Error::UnexpectedState {
                        id: id.to_string(),
                        expected: "RUNNING".into(),
                        got: s.to_string(),
                    });
                }
                _ => {}
            },
            Ok(Err(e)) if e.is_transient() => {
                tracing::debug!(
                    microvm = %id,
                    error = %e,
                    "get-microvm failed transiently; will retry"
                );
            }
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(wait_timeout(id, &want, policy)),
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(wait_timeout(id, &want, policy));
        }
        tokio::time::sleep(delay.min(remaining)).await;
        delay = delay.saturating_mul(2).min(policy.max_delay);
    }
}
