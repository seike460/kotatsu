//! `AwsControlPlane` against canned `lambda-microvms` responses from a
//! loopback server: the real SDK request/response path, no AWS account.

mod common;

use aws_sdk_lambdamicrovms::config::retry::RetryConfig;
use aws_sdk_lambdamicrovms::config::{BehaviorVersion, Credentials, Region};
use kotatsu::{AwsControlPlane, ControlPlane, Error, MicrovmId, State};

fn control_plane(endpoint: &str) -> AwsControlPlane {
    let conf = aws_sdk_lambdamicrovms::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new("AKIDTEST", "secret", None, None, "test"))
        .endpoint_url(endpoint)
        .retry_config(RetryConfig::disabled())
        .build();
    AwsControlPlane::from_conf(conf)
}

/// A control plane whose every call gets `status` with the service's
/// JSON error shape for `code`.
async fn failing(status: u16, code: &str, message: &str) -> AwsControlPlane {
    let body = format!(r#"{{"message":"{message}"}}"#);
    let (addr, _) = common::canned_http(
        status,
        &[
            ("content-type", "application/json"),
            ("x-amzn-errortype", code),
        ],
        &body,
    )
    .await;
    control_plane(&format!("http://{addr}"))
}

fn vm_id() -> MicrovmId {
    MicrovmId::new("microvm-0123456789abcdef").unwrap()
}

#[tokio::test]
async fn resource_not_found_maps_to_not_found() {
    let cp = failing(404, "ResourceNotFoundException", "no such microvm").await;
    for err in [
        cp.get(&vm_id()).await.unwrap_err(),
        cp.terminate(&vm_id()).await.unwrap_err(),
        cp.resume(&vm_id()).await.unwrap_err(),
    ] {
        match err {
            Error::NotFound { id } => assert_eq!(id, vm_id().as_str()),
            other => panic!("expected NotFound, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn conflict_maps_to_transient_conflict() {
    let cp = failing(409, "ConflictException", "microvm is suspending").await;
    let err = cp.resume(&vm_id()).await.unwrap_err();
    assert!(
        matches!(
            err,
            Error::Conflict {
                op: "resume_microvm",
                ..
            }
        ),
        "got {err:?}"
    );
    assert!(err.is_transient());
}

#[tokio::test]
async fn throttling_and_server_errors_are_transient() {
    for (status, code) in [
        (429, "ThrottlingException"),
        (500, "InternalServerException"),
        (503, "ServiceUnavailableException"),
    ] {
        let err = failing(status, code, "try again")
            .await
            .get(&vm_id())
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                Error::Aws {
                    op: "get_microvm",
                    transient: true,
                    ..
                }
            ),
            "{status}: got {err:?}"
        );
    }
}

#[tokio::test]
async fn access_denied_is_permanent_and_names_the_cause() {
    let msg = "User is not authorized to perform: lambda-microvms:ListMicrovms";
    let cp = failing(403, "AccessDeniedException", msg).await;
    let err = cp.list(None, None).await.unwrap_err();
    assert!(
        matches!(
            err,
            Error::Aws {
                op: "list_microvms",
                transient: false,
                ..
            }
        ),
        "got {err:?}"
    );
    // `%e` logging is all operators get; a bare "service error" hides
    // the IAM misconfiguration behind it.
    let shown = err.to_string();
    assert!(shown.contains("AccessDeniedException"), "{shown}");
    assert!(shown.contains(msg), "{shown}");
    assert_eq!(shown.matches(msg).count(), 1, "{shown}");
}

#[tokio::test]
async fn unreachable_endpoint_is_transient_and_names_the_cause() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let err = control_plane(&format!("http://{addr}"))
        .get(&vm_id())
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            Error::Aws {
                transient: true,
                ..
            }
        ),
        "got {err:?}"
    );
    let shown = err.to_string();
    assert!(shown.to_ascii_lowercase().contains("refused"), "{shown}");
}

#[tokio::test]
async fn get_maps_service_states() {
    for (wire, state) in [
        ("RUNNING", State::Running),
        ("SUSPENDED", State::Suspended),
        ("TERMINATED", State::Terminated),
        ("FAILED", State::Unknown("FAILED".into())),
    ] {
        let body = format!(
            r#"{{"microvmId":"{id}","state":"{wire}","endpoint":"https://{id}.lambda-microvm.us-east-1.on.aws","imageArn":"arn:aws:lambda:us-east-1:123456789012:microvm-image/img","imageVersion":"1","maximumDurationInSeconds":28800,"startedAt":1767225600}}"#,
            id = vm_id(),
        );
        let (addr, _) =
            common::canned_http(200, &[("content-type", "application/json")], &body).await;
        let vm = control_plane(&format!("http://{addr}"))
            .get(&vm_id())
            .await
            .unwrap();
        assert_eq!(vm.id, vm_id());
        assert_eq!(vm.state, state, "{wire}");
        assert_eq!(vm.started_at_secs, Some(1_767_225_600));
    }
}
