//! Tests for `TokenVending`, `MicrovmEndpoint` and the waiters, all
//! against `MockControlPlane` (no AWS credentials required).

mod common;

use kotatsu::mock::{MockBehavior, MockControlPlane};
use kotatsu::{
    ControlPlane, Error, MicrovmEndpoint, PortSpec, RunRequest, RunningVm, State, TokenKind,
    TokenVending, WaitPolicy, wait_for_state, wait_until_running,
};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

fn policy() -> WaitPolicy {
    WaitPolicy {
        timeout: Duration::from_secs(5),
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(50),
    }
}

#[tokio::test]
async fn wait_until_running_immediate() {
    let cp = MockControlPlane::new();
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    let running = wait_until_running(&cp, &vm.id, &policy()).await.unwrap();
    assert_eq!(running.microvm().state, State::Running);
    assert_eq!(running.id(), &vm.id);
}

#[tokio::test]
async fn wait_until_running_through_boot() {
    let cp = MockControlPlane::with_behavior(MockBehavior {
        boot_time: Duration::from_millis(60),
        ..Default::default()
    });
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    let running = wait_until_running(&cp, &vm.id, &policy()).await.unwrap();
    assert_eq!(running.microvm().state, State::Running);
}

#[tokio::test]
async fn wait_until_running_resumes_suspended() {
    let cp = MockControlPlane::with_behavior(MockBehavior {
        resume_time: Duration::from_millis(40),
        ..Default::default()
    });
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    cp.suspend(&vm.id).await.unwrap();
    assert_eq!(cp.get(&vm.id).await.unwrap().state, State::Suspended);
    let running = wait_until_running(&cp, &vm.id, &policy()).await.unwrap();
    assert_eq!(running.microvm().state, State::Running);
}

#[tokio::test]
async fn wait_until_running_times_out() {
    let cp = MockControlPlane::with_behavior(MockBehavior {
        boot_time: Duration::from_secs(60),
        ..Default::default()
    });
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    let short = WaitPolicy {
        timeout: Duration::from_millis(80),
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(20),
    };
    let err = wait_until_running(&cp, &vm.id, &short).await.unwrap_err();
    assert!(matches!(err, Error::WaitTimeout { .. }), "got {err:?}");
}

#[tokio::test]
async fn wait_until_running_fails_on_terminated() {
    let cp = MockControlPlane::new();
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    cp.terminate(&vm.id).await.unwrap();
    let err = wait_until_running(&cp, &vm.id, &policy())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::UnexpectedState { .. }), "got {err:?}");
}

#[tokio::test]
async fn wait_for_state_suspended() {
    let cp = MockControlPlane::with_behavior(MockBehavior {
        suspend_time: Duration::from_millis(40),
        ..Default::default()
    });
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    cp.suspend(&vm.id).await.unwrap();
    let done = wait_for_state(&cp, &vm.id, &State::Suspended, &policy())
        .await
        .unwrap();
    assert_eq!(done.state, State::Suspended);
}

#[tokio::test]
async fn running_vm_rejects_non_running() {
    let cp = MockControlPlane::with_behavior(MockBehavior {
        boot_time: Duration::from_secs(60),
        ..Default::default()
    });
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    assert_eq!(vm.state, State::Pending);
    assert!(RunningVm::try_from_vm(vm).is_err());
}

#[tokio::test]
async fn vending_caches_tokens_per_scope() {
    let cp = MockControlPlane::new();
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    let vending = TokenVending::new(Arc::new(cp));

    let scope = vec![PortSpec::port(8080).unwrap()];
    let t1 = vending.token(&vm.id, &scope).await.unwrap();
    let t2 = vending.token(&vm.id, &scope).await.unwrap();
    assert_eq!(t1.header_value(), t2.header_value(), "cache miss");

    let other_scope = vec![PortSpec::port(9090).unwrap()];
    let t3 = vending.token(&vm.id, &other_scope).await.unwrap();
    assert_ne!(t1.header_value(), t3.header_value());

    vending.invalidate(&vm.id);
    let t4 = vending.token(&vm.id, &scope).await.unwrap();
    assert_ne!(t1.header_value(), t4.header_value(), "invalidate failed");
}

#[tokio::test]
async fn vending_shell_tokens_are_separate() {
    let cp = MockControlPlane::new();
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    let vending = TokenVending::new(Arc::new(cp));

    let data = vending.token(&vm.id, &[PortSpec::All]).await.unwrap();
    let shell = vending.shell_token(&vm.id).await.unwrap();
    assert_eq!(data.kind(), TokenKind::Port);
    assert_eq!(shell.kind(), TokenKind::Shell);
    assert!(!shell.covers_port(8080));
    // Cached: second call returns the same value.
    assert_eq!(
        shell.header_value(),
        vending.shell_token(&vm.id).await.unwrap().header_value()
    );
}

async fn running_endpoint(cp: Arc<MockControlPlane>) -> MicrovmEndpoint {
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    let running = wait_until_running(&*cp, &vm.id, &policy()).await.unwrap();
    let vending = TokenVending::new(cp);
    MicrovmEndpoint::new(&running, Arc::new(vending)).unwrap()
}

#[tokio::test]
async fn endpoint_stamps_contract_headers() {
    let cp = Arc::new(MockControlPlane::new());
    let ep = running_endpoint(cp).await;
    let req = ep.get("/health").await.unwrap().build().unwrap();

    assert_eq!(req.method(), http::Method::GET);
    assert!(req.url().path().ends_with("/health"));
    assert!(req.url().scheme() == "https");
    assert!(
        req.headers()
            .get(kotatsu::AUTH_HEADER)
            .is_some_and(|v| v.to_str().unwrap().starts_with("dev-token-"))
    );
    assert_eq!(req.headers().get(kotatsu::PORT_HEADER).unwrap(), "8080");
}

#[tokio::test]
async fn endpoint_rejects_port_outside_scope() {
    let cp = Arc::new(MockControlPlane::new());
    let ep = running_endpoint(cp).await;
    // Default scope is port 8080 only.
    let err = ep
        .request(http::Method::GET, "/x", Some(9999))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidInput(_)), "got {err:?}");
}

#[tokio::test]
async fn endpoint_widened_scope_covers_other_ports() {
    let cp = Arc::new(MockControlPlane::new());
    let ep = running_endpoint(cp)
        .await
        .with_scope(vec![PortSpec::range(9000, 9010).unwrap()])
        .unwrap();
    let req = ep
        .request(http::Method::GET, "/x", Some(9005))
        .await
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(req.headers().get(kotatsu::PORT_HEADER).unwrap(), "9005");
}

#[tokio::test]
async fn endpoint_rejects_non_https_url() {
    let cp = MockControlPlane::new();
    let mut vm = cp.run(&RunRequest::new("img")).await.unwrap();
    vm.endpoint = "http://insecure.example".into();
    let running = RunningVm::try_from_vm(vm).unwrap();
    let vending = TokenVending::new(Arc::new(cp));
    assert!(MicrovmEndpoint::new(&running, Arc::new(vending)).is_err());
}

#[tokio::test]
async fn websocket_request_uses_subprotocol_contract() {
    let cp = Arc::new(MockControlPlane::new());
    let ep = running_endpoint(cp).await;
    let ws = ep.websocket("/ws", None).await.unwrap();

    assert_eq!(ws.url().scheme(), "wss");
    let protos = ws.subprotocols();
    assert_eq!(protos.len(), 3);
    assert_eq!(protos[0], kotatsu::WS_BASE_PROTOCOL);
    assert!(protos[1].starts_with(kotatsu::WS_AUTH_PROTOCOL_PREFIX));
    assert_eq!(
        protos[2],
        format!("{}8080", kotatsu::WS_PORT_PROTOCOL_PREFIX)
    );

    // Debug must not leak the token embedded in the subprotocol list.
    let token = protos[1]
        .strip_prefix(kotatsu::WS_AUTH_PROTOCOL_PREFIX)
        .unwrap();
    assert!(!format!("{ws:?}").contains(token));
}

#[tokio::test]
async fn hostile_paths_cannot_escape_origin() {
    let cp = Arc::new(MockControlPlane::new());
    let ep = running_endpoint(cp).await;
    for bad in [
        "//evil.example/x",
        "/\\evil.example/y",
        "https://evil.example/z",
        "http://evil.example/z",
        "relative/path",
        "/back\\slash",
    ] {
        let err = ep.request(http::Method::GET, bad, None).await;
        assert!(
            matches!(err, Err(Error::InvalidInput(_))),
            "path {bad:?} accepted: {err:?}"
        );
    }
    // And a good path keeps the MicroVM origin.
    let req = ep.get("/ok?q=1").await.unwrap().build().unwrap();
    assert_eq!(req.url().host_str(), ep.url().host_str());
    assert_eq!(req.url().scheme(), "https");
}

#[tokio::test]
async fn endpoint_does_not_follow_upstream_redirects() {
    // A VM answering with a redirect must not make the client fetch
    // the target — that would carry X-aws-proxy-auth off the VM origin.
    let (leak, leak_hits) = common::canned_http(200, &[], "secret").await;
    let target = format!("http://{leak}/latest/meta-data/");
    for status in [301, 302, 303, 307, 308] {
        let (vm_addr, _) = common::canned_http(status, &[("location", &target)], "").await;
        let cp = Arc::new(MockControlPlane::new().endpoint_override(&format!("http://{vm_addr}")));
        let vm = cp.run(&RunRequest::new("img")).await.unwrap();
        let running = wait_until_running(&*cp, &vm.id, &policy()).await.unwrap();
        let ep = MicrovmEndpoint::new_insecure(&running, Arc::new(TokenVending::new(cp))).unwrap();

        let resp = ep.get("/start").await.unwrap().send().await.unwrap();
        assert_eq!(resp.status().as_u16(), status);
        assert_eq!(resp.headers()["location"], target.as_str());
    }
    assert_eq!(leak_hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn wss_connect_reaches_the_tls_handshake() {
    // Both rustls providers are compiled into this crate's dependency
    // graph, so a handshake that leaves the provider choice to rustls
    // panics instead of returning an error.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((sock, _)) = listener.accept().await {
            drop(sock);
        }
    });
    let cp = Arc::new(MockControlPlane::new().endpoint_override(&format!("https://{addr}")));
    let ep = running_endpoint(cp).await;
    let ws = ep.websocket("/ws", None).await.unwrap();
    assert_eq!(ws.url().scheme(), "wss");
    let err = ws.connect().await.unwrap_err();
    assert!(matches!(err, Error::Ws(_)), "got {err:?}");
}

#[tokio::test]
async fn wait_for_state_terminated_is_reachable() {
    let cp = MockControlPlane::with_behavior(MockBehavior {
        terminate_time: Duration::from_millis(60),
        ..Default::default()
    });
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    cp.terminate(&vm.id).await.unwrap();
    assert_eq!(cp.get(&vm.id).await.unwrap().state, State::Terminating);
    let done = wait_for_state(&cp, &vm.id, &State::Terminated, &policy())
        .await
        .unwrap();
    assert_eq!(done.state, State::Terminated);
}

#[tokio::test]
async fn resume_slower_than_poll_interval_completes() {
    // Regression for the resume re-arm livelock: resume_time larger than
    // the poll cadence must still finish.
    let cp = MockControlPlane::with_behavior(MockBehavior {
        resume_time: Duration::from_millis(200),
        ..Default::default()
    });
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    cp.suspend(&vm.id).await.unwrap();
    let fast_poll = WaitPolicy {
        timeout: Duration::from_secs(5),
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(25),
    };
    let running = wait_until_running(&cp, &vm.id, &fast_poll).await.unwrap();
    assert_eq!(running.microvm().state, State::Running);
}

#[tokio::test]
async fn duplicate_resume_is_idempotent_in_mock() {
    let cp = MockControlPlane::with_behavior(MockBehavior {
        resume_time: Duration::from_millis(400),
        ..Default::default()
    });
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    cp.suspend(&vm.id).await.unwrap();
    cp.resume(&vm.id).await.unwrap();
    // A second resume while the transition is armed must not re-arm it.
    // The check lands past the first deadline (100+320 > 400ms) but
    // before a re-armed one (320 < 400ms after the second call).
    tokio::time::sleep(Duration::from_millis(100)).await;
    cp.resume(&vm.id).await.unwrap();
    tokio::time::sleep(Duration::from_millis(320)).await;
    assert_eq!(cp.get(&vm.id).await.unwrap().state, State::Running);
}

#[tokio::test]
async fn wait_policy_rejects_degenerate_durations() {
    let cp = MockControlPlane::new();
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    let bad = WaitPolicy {
        timeout: Duration::from_secs(1),
        initial_delay: Duration::ZERO,
        max_delay: Duration::from_secs(1),
    };
    let err = wait_until_running(&cp, &vm.id, &bad).await.unwrap_err();
    assert!(matches!(err, Error::InvalidInput(_)), "got {err:?}");
}

#[tokio::test]
async fn vending_rejects_margin_ge_ttl() {
    let cp = Arc::new(MockControlPlane::new());
    let cfg = kotatsu::TokenVendingConfig {
        ttl_minutes: 1,
        refresh_margin: Duration::from_secs(60),
    };
    assert!(TokenVending::with_config(cp, cfg).is_err());
}

#[tokio::test]
async fn vending_rejects_empty_scope_locally() {
    let cp = Arc::new(MockControlPlane::new());
    let vending = TokenVending::new(cp);
    let vm_id = kotatsu::MicrovmId::new("microvm-x").unwrap();
    let err = vending.token(&vm_id, &[]).await.unwrap_err();
    assert!(matches!(err, Error::InvalidInput(_)), "got {err:?}");
}

#[tokio::test]
async fn wait_until_running_propagates_not_found() {
    let cp = MockControlPlane::new();
    let vm_id = kotatsu::MicrovmId::new("microvm-missing").unwrap();
    let err = wait_until_running(&cp, &vm_id, &policy())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::NotFound { .. }), "got {err:?}");
}

#[tokio::test]
async fn wait_for_state_bounds_a_stalled_get() {
    // The get call never returns; the waiter must still honour the
    // policy timeout instead of hanging on the control plane.
    let cp = MockControlPlane::with_behavior(MockBehavior {
        get_gate: Some(Arc::new(tokio::sync::Notify::new())),
        ..Default::default()
    });
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    let short = WaitPolicy {
        timeout: Duration::from_millis(80),
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(20),
    };
    let start = std::time::Instant::now();
    let err = wait_for_state(&cp, &vm.id, &State::Running, &short)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::WaitTimeout { .. }), "got {err:?}");
    assert!(start.elapsed() < Duration::from_secs(3), "get unbounded");
}

#[tokio::test]
async fn wait_until_running_bounds_a_stalled_resume() {
    let cp = MockControlPlane::with_behavior(MockBehavior {
        resume_gate: Some(Arc::new(tokio::sync::Notify::new())),
        ..Default::default()
    });
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    cp.suspend(&vm.id).await.unwrap();
    let short = WaitPolicy {
        timeout: Duration::from_millis(80),
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(20),
    };
    let start = std::time::Instant::now();
    let err = wait_until_running(&cp, &vm.id, &short).await.unwrap_err();
    assert!(matches!(err, Error::WaitTimeout { .. }), "got {err:?}");
    assert!(start.elapsed() < Duration::from_secs(3), "resume unbounded");
}

#[tokio::test]
async fn wait_until_running_returns_permanent_resume_error() {
    // AccessDenied-style rejections must surface immediately instead of
    // being retried into a misleading WaitTimeout.
    let cp = MockControlPlane::with_behavior(MockBehavior {
        resume_error: Some("AccessDeniedException".into()),
        ..Default::default()
    });
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    cp.suspend(&vm.id).await.unwrap();
    let short = WaitPolicy {
        timeout: Duration::from_secs(30),
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(20),
    };
    let start = std::time::Instant::now();
    let err = wait_until_running(&cp, &vm.id, &short).await.unwrap_err();
    assert!(matches!(err, Error::Other(_)), "got {err:?}");
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "permanent error retried"
    );
}

#[tokio::test]
async fn wait_until_running_retries_transient_resume_errors() {
    let cp = MockControlPlane::with_behavior(MockBehavior {
        resume_transient_failures: Some(Arc::new(std::sync::atomic::AtomicU32::new(2))),
        resume_time: Duration::from_millis(30),
        ..Default::default()
    });
    let vm = cp.run(&RunRequest::new("img")).await.unwrap();
    cp.suspend(&vm.id).await.unwrap();
    let running = wait_until_running(&cp, &vm.id, &policy()).await.unwrap();
    assert_eq!(running.microvm().state, State::Running);
}

#[test]
fn error_transience_classification() {
    assert!(Error::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, "x")).is_transient());
    assert!(Error::Store("db locked".into()).is_transient());
    // A 409 means "already transitioning" — waiters must keep polling.
    assert!(
        Error::Conflict {
            op: "resume_microvm",
            source: Box::new(std::io::Error::other("ConflictException")),
        }
        .is_transient()
    );
    assert!(!Error::NotFound { id: "x".into() }.is_transient());
    assert!(!Error::Terminated("x".into()).is_transient());
    assert!(!Error::InvalidInput("x".into()).is_transient());
    assert!(!Error::Token("x".into()).is_transient());
    assert!(!Error::Config("x".into()).is_transient());
    assert!(!Error::Other("x".into()).is_transient());
    assert!(!Error::PoolExhausted(3).is_transient());
}
