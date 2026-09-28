//! Unit tests for core contract types and the mock control plane.

use kotatsu::mock::{MockBehavior, MockControlPlane};
use kotatsu::{
    ControlPlane, IdlePolicyConfig, MicrovmId, PortSpec, RunRequest, State, TenantKey, WaitPolicy,
    wait_for_state,
};
use std::time::Duration;

/// Mock transitions run on wall-clock time: a transitional state
/// asserted right after the call needs a window far wider than
/// scheduler jitter.
const TRANSITION: Duration = Duration::from_millis(500);

fn poll() -> WaitPolicy {
    WaitPolicy {
        timeout: Duration::from_secs(5),
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(50),
    }
}

#[test]
fn tenant_key_validation() {
    assert!(TenantKey::new("user-42").is_ok());
    assert!(TenantKey::new("tenant_1.sub~x").is_ok());
    assert!(TenantKey::new("").is_err());
    assert!(TenantKey::new("bad/key").is_err());
    assert!(TenantKey::new("bad key").is_err());
    assert!(TenantKey::new("x".repeat(129)).is_err());
}

#[test]
fn port_spec_parse() {
    assert_eq!(PortSpec::parse("8080").unwrap(), PortSpec::Port(8080));
    assert_eq!(
        PortSpec::parse("9000-9010").unwrap(),
        PortSpec::Range {
            start: 9000,
            end: 9010
        }
    );
    assert_eq!(PortSpec::parse("all").unwrap(), PortSpec::All);
    assert_eq!(PortSpec::parse("*").unwrap(), PortSpec::All);
    assert!(PortSpec::parse("0").is_err());
    assert!(PortSpec::parse("99999").is_err());
    assert!(PortSpec::parse("9010-9000").is_err());
    assert!(PortSpec::parse("abc").is_err());
}

#[test]
fn port_spec_covers() {
    assert!(PortSpec::Port(8080).covers(8080));
    assert!(!PortSpec::Port(8080).covers(8081));
    assert!(
        PortSpec::Range {
            start: 9000,
            end: 9010
        }
        .covers(9005)
    );
    assert!(
        !PortSpec::Range {
            start: 9000,
            end: 9010
        }
        .covers(9011)
    );
    assert!(PortSpec::All.covers(65535));
}

#[test]
fn run_request_validation() {
    assert!(RunRequest::new("").validate().is_err());
    let mut req = RunRequest::new("arn:aws:lambda:us-east-1:123456789012:microvm-image:x");
    assert!(req.validate().is_ok());
    req.maximum_duration_seconds = Some(28_800);
    assert!(req.validate().is_ok());
    req.maximum_duration_seconds = Some(28_801);
    assert!(req.validate().is_err());
    req.maximum_duration_seconds = None;
    req.run_hook_payload = Some("x".repeat(16_385));
    assert!(req.validate().is_err());
    req.run_hook_payload = None;
    req.idle_policy = Some(IdlePolicyConfig {
        auto_resume_enabled: true,
        max_idle_duration_seconds: 900,
        suspended_duration_seconds: 300,
    });
    assert!(req.validate().is_ok());
    req.idle_policy = Some(IdlePolicyConfig {
        auto_resume_enabled: true,
        max_idle_duration_seconds: 0,
        suspended_duration_seconds: 300,
    });
    assert!(req.validate().is_err());
}

#[test]
fn idle_policy_matches_documented_aws_bounds() {
    // AWS API: maxIdleDurationSeconds >= 60, suspendedDurationSeconds
    // >= 0 (0 = terminate immediately on suspend), both <= 28,800.
    fn valid(max_idle: i32, suspended: i32) -> bool {
        let mut req = RunRequest::new("img");
        req.idle_policy = Some(IdlePolicyConfig {
            auto_resume_enabled: true,
            max_idle_duration_seconds: max_idle,
            suspended_duration_seconds: suspended,
        });
        req.validate().is_ok()
    }
    for bad in [i32::MIN, -1, 0, 1, 59] {
        assert!(!valid(bad, 300), "max_idle {bad} must be rejected");
    }
    for good in [60, 900, 28_800] {
        assert!(valid(good, 300), "max_idle {good} must be accepted");
    }
    for bad in [i32::MIN, -1, 28_801, i32::MAX] {
        assert!(!valid(300, bad), "suspended {bad} must be rejected");
    }
    for good in [0, 300, 28_800] {
        assert!(valid(300, good), "suspended {good} must be accepted");
    }
}

#[test]
fn state_liveness() {
    assert!(State::Running.is_live());
    assert!(State::Suspended.is_live());
    assert!(State::Pending.is_live());
    assert!(State::Suspending.is_live());
    assert!(!State::Terminated.is_live());
    assert!(!State::Terminating.is_live());
    // An unknown (possibly terminal) state must never receive traffic.
    assert!(!State::Unknown("FAILED".into()).is_live());
}

#[tokio::test]
async fn mock_run_get_suspend_resume_terminate() {
    let cp = MockControlPlane::new();
    let req = RunRequest::new("arn:aws:lambda:ap-northeast-1:1:microvm-image:test");
    let vm = cp.run(&req).await.unwrap();
    assert_eq!(vm.state, State::Running);
    assert!(
        vm.endpoint
            .contains(".lambda-microvm.ap-northeast-1.on.aws")
    );

    let got = cp.get(&vm.id).await.unwrap();
    assert_eq!(got.id, vm.id);

    cp.suspend(&vm.id).await.unwrap();
    assert_eq!(cp.get(&vm.id).await.unwrap().state, State::Suspended);

    cp.resume(&vm.id).await.unwrap();
    assert_eq!(cp.get(&vm.id).await.unwrap().state, State::Running);

    cp.terminate(&vm.id).await.unwrap();
    assert_eq!(cp.get(&vm.id).await.unwrap().state, State::Terminated);
    assert!(cp.resume(&vm.id).await.is_err());
}

#[tokio::test]
async fn mock_transition_timing() {
    let cp = MockControlPlane::with_behavior(MockBehavior {
        boot_time: TRANSITION,
        ..Default::default()
    });
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    assert_eq!(vm.state, State::Pending);
    assert_eq!(cp.get(&vm.id).await.unwrap().state, State::Pending);
    let done = wait_for_state(&cp, &vm.id, &State::Running, &poll())
        .await
        .unwrap();
    assert_eq!(done.state, State::Running);
}

#[tokio::test]
async fn mock_list_filter() {
    let cp = MockControlPlane::new();
    cp.run(&RunRequest::new("img-a")).await.unwrap();
    cp.run(&RunRequest::new("img-b")).await.unwrap();
    let all = cp.list(None, None).await.unwrap();
    assert_eq!(all.len(), 2);
    let only_a = cp.list(Some("img-a"), None).await.unwrap();
    assert_eq!(only_a.len(), 1);
}

#[tokio::test]
async fn mock_mint_token() {
    let cp = MockControlPlane::new();
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    let token = cp
        .mint_token(&vm.id, &[PortSpec::Port(8080)], 30)
        .await
        .unwrap();
    assert!(token.header_value().starts_with("dev-token-"));
    assert!(token.covers_port(8080));
    assert!(!token.covers_port(9000));
    assert!(!token.nearly_expired(Duration::from_secs(60)));
    // Debug output must never contain the raw token.
    let dbg = format!("{token:?}");
    assert!(!dbg.contains(token.header_value()));
    assert!(dbg.contains("<redacted>"));

    let missing = MicrovmId::new("microvm-nope").unwrap();
    let err = cp
        .mint_token(&missing, &[PortSpec::All], 5)
        .await
        .unwrap_err();
    assert!(matches!(err, kotatsu::Error::NotFound { .. }));

    // Contract parity: empty scope / non-positive TTL / invalid spec rejected.
    assert!(
        cp.mint_token(&vm.id, &[], 5).await.is_err(),
        "empty scope must be rejected"
    );
    assert!(
        cp.mint_token(&vm.id, &[PortSpec::All], 0).await.is_err(),
        "ttl 0 must be rejected"
    );
    assert!(
        cp.mint_token(&vm.id, &[PortSpec::Port(0)], 5)
            .await
            .is_err(),
        "port 0 must be rejected"
    );
    // ttl > 60 clamps rather than fails.
    let clamped = cp.mint_token(&vm.id, &[PortSpec::All], 120).await.unwrap();
    assert_eq!(clamped.ttl(), Duration::from_secs(60 * 60));

    // Shell tokens never masquerade as data-plane tokens.
    let shell = cp.mint_shell_token(&vm.id, 10).await.unwrap();
    assert!(!shell.covers_port(8080));

    // No tokens for terminated MicroVMs.
    cp.terminate(&vm.id).await.unwrap();
    assert!(cp.mint_token(&vm.id, &[PortSpec::All], 5).await.is_err());
}

#[tokio::test]
async fn terminate_during_boot_does_not_resurrect() {
    let cp = MockControlPlane::with_behavior(MockBehavior {
        boot_time: Duration::from_millis(80),
        ..Default::default()
    });
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    assert_eq!(vm.state, State::Pending);
    cp.terminate(&vm.id).await.unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    assert_eq!(cp.get(&vm.id).await.unwrap().state, State::Terminated);
}

#[tokio::test]
async fn transitional_states_are_observable() {
    let cp = MockControlPlane::with_behavior(MockBehavior {
        suspend_time: TRANSITION,
        terminate_time: TRANSITION,
        ..Default::default()
    });
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    cp.suspend(&vm.id).await.unwrap();
    assert_eq!(cp.get(&vm.id).await.unwrap().state, State::Suspending);
    let done = wait_for_state(&cp, &vm.id, &State::Suspended, &poll())
        .await
        .unwrap();
    assert_eq!(done.state, State::Suspended);
    cp.resume(&vm.id).await.unwrap();
    cp.terminate(&vm.id).await.unwrap();
    assert_eq!(cp.get(&vm.id).await.unwrap().state, State::Terminating);
    let done = wait_for_state(&cp, &vm.id, &State::Terminated, &poll())
        .await
        .unwrap();
    assert_eq!(done.state, State::Terminated);
}

#[tokio::test]
async fn double_terminate_is_idempotent() {
    let cp = MockControlPlane::new();
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    cp.terminate(&vm.id).await.unwrap();
    cp.terminate(&vm.id).await.unwrap();
    assert_eq!(cp.get(&vm.id).await.unwrap().state, State::Terminated);
    assert!(
        cp.get(&MicrovmId::new("microvm-404").unwrap())
            .await
            .is_err()
    );
}
