//! Control-plane abstraction over the `lambda-microvms` API.

use async_trait::async_trait;
use aws_sdk_lambdamicrovms::types::MicrovmItem;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::types::{
    AuthToken, Microvm, MicrovmId, MicrovmSummary, PortSpec, RunRequest, State, TokenKind,
};

/// The operations a MicroVM control plane must provide.
///
/// `AwsControlPlane` implements this against the real `lambda-microvms` API;
/// [`crate::mock::MockControlPlane`] implements it in-memory for tests.
#[async_trait]
pub trait ControlPlane: Send + Sync {
    /// Runs a new MicroVM from an image (`run-microvm`).
    async fn run(&self, req: &RunRequest) -> Result<Microvm>;
    /// Describes a MicroVM (`get-microvm`).
    async fn get(&self, id: &MicrovmId) -> Result<Microvm>;
    /// Suspends a running MicroVM (`suspend-microvm`).
    async fn suspend(&self, id: &MicrovmId) -> Result<()>;
    /// Resumes a suspended MicroVM (`resume-microvm`).
    async fn resume(&self, id: &MicrovmId) -> Result<()>;
    /// Terminates a MicroVM (`terminate-microvm`).
    async fn terminate(&self, id: &MicrovmId) -> Result<()>;
    /// Lists MicroVMs, optionally filtered by image (`list-microvms`).
    async fn list(
        &self,
        image_identifier: Option<&str>,
        image_version: Option<&str>,
    ) -> Result<Vec<MicrovmSummary>>;
    /// Mints a JWE auth token for traffic to a MicroVM (`create-microvm-auth-token`).
    ///
    /// Returns an [`AuthToken`] whose [`AuthToken::header_value`] is the
    /// `X-aws-proxy-auth` value. The caller decides the port scope;
    /// `ttl_minutes` is capped at [`crate::MAX_TOKEN_TTL_MINUTES`].
    async fn mint_token(
        &self,
        id: &MicrovmId,
        scope: &[PortSpec],
        ttl_minutes: i32,
    ) -> Result<AuthToken>;
    /// Mints a shell auth token (`create-microvm-shell-auth-token`).
    ///
    /// Only valid when the MicroVM was run with the `SHELL_INGRESS` connector.
    async fn mint_shell_token(&self, id: &MicrovmId, ttl_minutes: i32) -> Result<AuthToken>;
}

/// Control plane backed by `aws-sdk-lambdamicrovms`.
#[derive(Clone)]
pub struct AwsControlPlane {
    client: aws_sdk_lambdamicrovms::Client,
}

impl AwsControlPlane {
    /// Builds a client from a shared `aws-config` resolution.
    pub fn new(config: &aws_config::SdkConfig) -> Self {
        Self {
            client: aws_sdk_lambdamicrovms::Client::new(config),
        }
    }

    /// Builds a client from a service-specific config.
    pub fn from_conf(config: aws_sdk_lambdamicrovms::Config) -> Self {
        Self {
            client: aws_sdk_lambdamicrovms::Client::from_conf(config),
        }
    }

    /// Access to the raw SDK client for operations kotatsu does not wrap.
    pub fn raw(&self) -> &aws_sdk_lambdamicrovms::Client {
        &self.client
    }
}

fn auth_token_from(
    map: &std::collections::HashMap<String, String>,
    scope: Vec<PortSpec>,
    ttl: Duration,
    kind: TokenKind,
) -> Result<AuthToken> {
    let value = map
        .get(crate::AUTH_HEADER)
        .cloned()
        .ok_or_else(|| Error::Token("response lacked an X-aws-proxy-auth entry".into()))?;
    Ok(AuthToken {
        value,
        issued_at: std::time::Instant::now(),
        ttl,
        scope,
        kind,
    })
}

/// Fields shared by `RunMicrovmOutput` and `GetMicrovmOutput`.
struct MicrovmFields<'a> {
    microvm_id: &'a str,
    state: &'a aws_sdk_lambdamicrovms::types::MicrovmState,
    endpoint: &'a str,
    image_arn: &'a str,
    image_version: &'a str,
    execution_role_arn: Option<&'a str>,
    maximum_duration_in_seconds: i32,
    started_at: &'a aws_sdk_lambdamicrovms::primitives::DateTime,
    terminated_at: Option<&'a aws_sdk_lambdamicrovms::primitives::DateTime>,
    state_reason: Option<&'a str>,
    ingress: &'a [String],
    egress: &'a [String],
}

fn microvm_from(f: MicrovmFields<'_>) -> Microvm {
    Microvm {
        id: MicrovmId(f.microvm_id.to_owned()),
        state: State::from(f.state),
        endpoint: f.endpoint.to_owned(),
        image_arn: f.image_arn.to_owned(),
        image_version: f.image_version.to_owned(),
        execution_role_arn: f.execution_role_arn.map(str::to_owned),
        maximum_duration_seconds: f.maximum_duration_in_seconds,
        started_at_secs: Some(f.started_at.secs()),
        terminated_at_secs: f.terminated_at.map(|t| t.secs()),
        state_reason: f.state_reason.map(str::to_owned),
        ingress_connectors: f.ingress.to_vec(),
        egress_connectors: f.egress.to_vec(),
    }
}

fn summary_from_item(item: &MicrovmItem) -> MicrovmSummary {
    MicrovmSummary {
        id: MicrovmId(item.microvm_id().to_owned()),
        state: State::from(item.state()),
        image_arn: item.image_arn().to_owned(),
        image_version: item.image_version().to_owned(),
        started_at_secs: Some(item.started_at().secs()),
    }
}

#[async_trait]
impl ControlPlane for AwsControlPlane {
    async fn run(&self, req: &RunRequest) -> Result<Microvm> {
        req.validate()?;
        let mut b = self.client.run_microvm();
        b = b.set_image_identifier(Some(req.image_identifier.clone()));
        b = b.set_image_version(req.image_version.clone());
        b = b.set_execution_role_arn(req.execution_role_arn.clone());
        b = b.set_idle_policy(req.idle_policy.as_ref().map(|p| p.to_sdk()));
        if !req.ingress_connectors.is_empty() {
            b = b.set_ingress_network_connectors(Some(req.ingress_connectors.clone()));
        }
        if !req.egress_connectors.is_empty() {
            b = b.set_egress_network_connectors(Some(req.egress_connectors.clone()));
        }
        b = b.set_maximum_duration_in_seconds(req.maximum_duration_seconds);
        b = b.set_run_hook_payload(req.run_hook_payload.clone());
        b = b.set_client_token(req.client_token.clone());
        let out = b.send().await.map_err(|e| Error::aws("run_microvm", e))?;
        Ok(microvm_from(MicrovmFields {
            microvm_id: out.microvm_id(),
            state: out.state(),
            endpoint: out.endpoint(),
            image_arn: out.image_arn(),
            image_version: out.image_version(),
            execution_role_arn: out.execution_role_arn(),
            maximum_duration_in_seconds: out.maximum_duration_in_seconds(),
            started_at: out.started_at(),
            terminated_at: out.terminated_at(),
            state_reason: out.state_reason(),
            ingress: out.ingress_network_connectors(),
            egress: out.egress_network_connectors(),
        }))
    }

    async fn get(&self, id: &MicrovmId) -> Result<Microvm> {
        let out = self
            .client
            .get_microvm()
            .microvm_identifier(id.as_str())
            .send()
            .await
            .map_err(|e| svc_err("get_microvm", id, e))?;
        Ok(microvm_from(MicrovmFields {
            microvm_id: out.microvm_id(),
            state: out.state(),
            endpoint: out.endpoint(),
            image_arn: out.image_arn(),
            image_version: out.image_version(),
            execution_role_arn: out.execution_role_arn(),
            maximum_duration_in_seconds: out.maximum_duration_in_seconds(),
            started_at: out.started_at(),
            terminated_at: out.terminated_at(),
            state_reason: out.state_reason(),
            ingress: out.ingress_network_connectors(),
            egress: out.egress_network_connectors(),
        }))
    }

    async fn suspend(&self, id: &MicrovmId) -> Result<()> {
        self.client
            .suspend_microvm()
            .microvm_identifier(id.as_str())
            .send()
            .await
            .map_err(|e| svc_err("suspend_microvm", id, e))?;
        Ok(())
    }

    async fn resume(&self, id: &MicrovmId) -> Result<()> {
        self.client
            .resume_microvm()
            .microvm_identifier(id.as_str())
            .send()
            .await
            .map_err(|e| svc_err("resume_microvm", id, e))?;
        Ok(())
    }

    async fn terminate(&self, id: &MicrovmId) -> Result<()> {
        self.client
            .terminate_microvm()
            .microvm_identifier(id.as_str())
            .send()
            .await
            .map_err(|e| svc_err("terminate_microvm", id, e))?;
        Ok(())
    }

    async fn list(
        &self,
        image_identifier: Option<&str>,
        image_version: Option<&str>,
    ) -> Result<Vec<MicrovmSummary>> {
        let mut out = Vec::new();
        let mut stream = self
            .client
            .list_microvms()
            .set_image_identifier(image_identifier.map(str::to_owned))
            .set_image_version(image_version.map(str::to_owned))
            .into_paginator()
            .send();
        while let Some(page) = stream.next().await {
            let page = page.map_err(|e| Error::aws("list_microvms", e))?;
            for item in page.items() {
                out.push(summary_from_item(item));
            }
        }
        Ok(out)
    }

    async fn mint_token(
        &self,
        id: &MicrovmId,
        scope: &[PortSpec],
        ttl_minutes: i32,
    ) -> Result<AuthToken> {
        if scope.is_empty() {
            return Err(Error::invalid("token scope must not be empty"));
        }
        if let Some(bad) = scope.iter().find(|s| !s.is_valid()) {
            return Err(Error::invalid(format!("invalid port scope: {bad}")));
        }
        let ttl_minutes = checked_ttl_minutes(ttl_minutes)?;
        let out = self
            .client
            .create_microvm_auth_token()
            .microvm_identifier(id.as_str())
            .expiration_in_minutes(ttl_minutes)
            .set_allowed_ports(Some(scope.iter().map(PortSpec::to_sdk).collect()))
            .send()
            .await
            .map_err(|e| svc_err("create_microvm_auth_token", id, e))?;
        auth_token_from(
            out.auth_token(),
            scope.to_vec(),
            Duration::from_secs(u64::from(ttl_minutes as u32) * 60),
            TokenKind::Port,
        )
    }

    async fn mint_shell_token(&self, id: &MicrovmId, ttl_minutes: i32) -> Result<AuthToken> {
        let ttl_minutes = checked_ttl_minutes(ttl_minutes)?;
        let out = self
            .client
            .create_microvm_shell_auth_token()
            .microvm_identifier(id.as_str())
            .expiration_in_minutes(ttl_minutes)
            .send()
            .await
            .map_err(|e| svc_err("create_microvm_shell_auth_token", id, e))?;
        auth_token_from(
            out.auth_token(),
            Vec::new(),
            Duration::from_secs(u64::from(ttl_minutes as u32) * 60),
            TokenKind::Shell,
        )
    }
}

/// Validates a token TTL: must be positive; values above the service's
/// 60-minute cap are clamped rather than rejected. Shared by the AWS and
/// mock control planes so both enforce the same contract.
pub(crate) fn checked_ttl_minutes(ttl_minutes: i32) -> Result<i32> {
    if ttl_minutes <= 0 {
        return Err(Error::invalid("ttl_minutes must be positive"));
    }
    Ok(ttl_minutes.min(crate::MAX_TOKEN_TTL_MINUTES))
}

/// Maps `ResourceNotFoundException` service errors to [`Error::NotFound`]
/// so callers can distinguish "VM already gone" from other failures.
fn svc_err<E>(
    op: &'static str,
    id: &MicrovmId,
    e: aws_sdk_lambdamicrovms::error::SdkError<E>,
) -> Error
where
    E: aws_sdk_lambdamicrovms::error::ProvideErrorMetadata
        + std::error::Error
        + Send
        + Sync
        + 'static,
{
    use aws_sdk_lambdamicrovms::error::SdkError;
    if let SdkError::ServiceError(ctx) = &e
        && ctx.err().meta().code() == Some("ResourceNotFoundException")
    {
        return Error::NotFound { id: id.to_string() };
    }
    Error::aws(op, e)
}
