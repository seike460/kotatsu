//! kotatsu — CLI for the AWS Lambda MicroVMs fleet control plane.
//!
//! Subcommands:
//! - `kotatsu vm …` — direct control-plane ops (list/get/run/suspend/resume/terminate)
//! - `kotatsu token …` — mint `X-aws-proxy-auth` / shell tokens
//! - `kotatsu image …` — image lifecycle (create/list/versions/base/get/get-version/
//!   update/update-version/builds/build/delete-version/delete)
//! - `kotatsu tag …` — resource tagging by ARN (list/set/unset)
//! - `kotatsu dev` — run the local contract emulator (`kotatsu-dev`)
//! - `kotatsu cost` — offline cost estimate (no AWS calls)
//! - `kotatsu serve` — hand off to the `kotatsud` gateway daemon on PATH

use std::net::SocketAddr;

use anyhow::{Context, bail};
use clap::{Args, Parser, Subcommand};
use kotatsu::{
    AwsControlPlane, ControlPlane, IdlePolicyConfig, MicrovmId, PortSpec, RunRequest, State,
    WaitPolicy, wait_for_state, wait_until_running,
};
use kotatsu_dev::{Emulator, EmulatorConfig};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;

/// Fleet control plane for AWS Lambda MicroVMs.
#[derive(Parser)]
#[command(name = "kotatsu", version, about)]
struct Cli {
    /// AWS region override (AWS_REGION env works too).
    #[arg(long, global = true, env = "AWS_REGION")]
    region: Option<String>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Direct MicroVM operations against the AWS control plane.
    Vm(VmCmd),
    /// Mint auth tokens for a MicroVM endpoint.
    Token(TokenCmd),
    /// MicroVM image lifecycle (build is AWS-side and async).
    Image(ImageCmd),
    /// Resource tagging for MicroVMs and images (by ARN).
    Tag(TagCmd),
    /// Run the local contract emulator in front of a local app.
    Dev(DevCmd),
    /// Offline monthly cost estimate (official us-east-1 rates).
    Cost(CostArgs),
    /// Run the kotatsud session gateway (execs `kotatsud` from PATH).
    ///
    /// Args are forwarded verbatim; clap-owned flags like `--region` or
    /// `--help` must follow `--` to reach kotatsud unambiguously.
    Serve {
        /// Arguments forwarded verbatim to kotatsud.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
}

#[derive(Args)]
struct VmCmd {
    #[command(subcommand)]
    cmd: VmSub,
}

#[derive(Subcommand)]
enum VmSub {
    /// List MicroVMs (optionally filtered by image).
    List {
        /// Filter by image identifier or ARN.
        #[arg(long)]
        image: Option<String>,
        /// Filter by image version (major.minor).
        #[arg(long)]
        version: Option<String>,
    },
    /// Describe one MicroVM as JSON.
    Get {
        /// MicroVM ID.
        id: String,
    },
    /// Launch a MicroVM; optionally wait for RUNNING.
    Run {
        /// Image ARN or identifier (required).
        #[arg(long)]
        image: String,
        /// Image version (major.minor); latest when omitted.
        #[arg(long)]
        version: Option<String>,
        /// Execution role ARN the MicroVM runs as.
        #[arg(long)]
        execution_role_arn: Option<String>,
        /// Max lifetime in seconds (service-side cap).
        #[arg(long)]
        maximum_duration_seconds: Option<i32>,
        /// Auto-suspend after N idle seconds (sets idlePolicy).
        #[arg(long)]
        idle_suspend_seconds: Option<i32>,
        /// Terminate after N suspended seconds (requires --idle-suspend-seconds).
        #[arg(long, requires = "idle_suspend_seconds")]
        suspended_ttl_seconds: Option<i32>,
        /// Disable auto-resume (requires --idle-suspend-seconds).
        #[arg(long, requires = "idle_suspend_seconds")]
        no_auto_resume: bool,
        /// Ingress connector (repeatable); SHELL_* enables `token shell`.
        #[arg(long = "ingress-connector")]
        ingress_connectors: Vec<String>,
        /// Egress connector (repeatable).
        #[arg(long = "egress-connector")]
        egress_connectors: Vec<String>,
        /// Idempotency token for run; a fresh UUID is used when omitted.
        #[arg(long)]
        client_token: Option<String>,
        /// JSON payload for the /run hook (≤16 KiB).
        #[arg(long)]
        run_hook_payload: Option<String>,
        /// Block until RUNNING (default budget 120s).
        #[arg(long)]
        wait: bool,
    },
    /// Suspend a MicroVM.
    Suspend {
        /// MicroVM ID.
        id: String,
    },
    /// Resume a suspended MicroVM; optionally wait for RUNNING.
    Resume {
        /// MicroVM ID.
        id: String,
        /// Block until RUNNING (default budget 120s).
        #[arg(long)]
        wait: bool,
    },
    /// Terminate a MicroVM.
    Terminate {
        /// MicroVM ID.
        id: String,
    },
}

#[derive(Args)]
struct TokenCmd {
    #[command(subcommand)]
    cmd: TokenSub,
}

#[derive(Subcommand)]
enum TokenSub {
    /// Mint a scoped `X-aws-proxy-auth` token (create-microvm-auth-token).
    Mint {
        /// MicroVM ID.
        id: String,
        /// Port scope, comma-separated: `8080`, `9000-9010`, `all`.
        #[arg(long, value_delimiter = ',', default_value = "8080")]
        ports: Vec<String>,
        /// TTL in minutes (1-60; short-lived by default).
        #[arg(long, default_value = "15")]
        ttl_minutes: i32,
    },
    /// Mint a shell token (create-microvm-shell-auth-token).
    ///
    /// Requires the MicroVM to have been run with a SHELL_* ingress connector.
    Shell {
        /// MicroVM ID.
        id: String,
        /// TTL in minutes (1-60).
        #[arg(long, default_value = "15")]
        ttl_minutes: i32,
    },
}

#[derive(Args)]
struct ImageCmd {
    #[command(subcommand)]
    cmd: ImageSub,
}

#[derive(Subcommand)]
enum ImageSub {
    /// Register a new image version; builds asynchronously on AWS.
    Create {
        /// Image name.
        #[arg(long)]
        name: String,
        /// s3:// URI of the Dockerfile zip artifact.
        #[arg(long)]
        s3_uri: String,
        /// Managed base image ARN to build on (required by the service —
        /// find one with `image base`).
        #[arg(long)]
        base_image_arn: String,
        /// IAM role ARN the build runs as (required by the service).
        #[arg(long)]
        build_role_arn: String,
        /// Human-readable description.
        #[arg(long)]
        description: Option<String>,
    },
    /// List registered images.
    List {
        /// Filter by image name substring.
        #[arg(long)]
        name_filter: Option<String>,
    },
    /// List versions of an image.
    Versions {
        /// Image identifier or ARN.
        #[arg(long)]
        image: String,
    },
    /// List AWS-managed base images (find `image create --base-image-arn`
    /// here); with --image, list that base's versions.
    Base {
        /// Managed base image ARN/identifier for version listing.
        #[arg(long)]
        image: Option<String>,
    },
    /// List builds for an image version.
    Builds {
        /// Image identifier or ARN.
        #[arg(long)]
        image: String,
        /// Image version (major.minor) — required by the service.
        #[arg(long)]
        version: String,
    },
    /// Describe one image as JSON.
    Get {
        /// Image identifier or ARN.
        #[arg(long)]
        image: String,
    },
    /// Describe one image version as JSON.
    GetVersion {
        /// Image identifier or ARN.
        #[arg(long)]
        image: String,
        /// Image version (major.minor).
        #[arg(long)]
        version: String,
    },
    /// Describe one build.
    Build {
        /// Image identifier or ARN.
        #[arg(long)]
        image: String,
        /// Image version (major.minor).
        #[arg(long)]
        version: String,
        /// Build ID.
        #[arg(long)]
        build_id: String,
    },
    /// Build a new version of an existing image (update-microvm-image).
    Update {
        /// Image identifier or ARN.
        #[arg(long)]
        image: String,
        /// s3:// URI of the Dockerfile zip artifact.
        #[arg(long)]
        s3_uri: String,
        /// Managed base image ARN to build on (required by the service).
        #[arg(long)]
        base_image_arn: String,
        /// Pin a specific managed base image version.
        #[arg(long)]
        base_image_version: Option<String>,
        /// IAM role ARN the build runs as (required by the service).
        #[arg(long)]
        build_role_arn: String,
        /// Human-readable description.
        #[arg(long)]
        description: Option<String>,
    },
    /// Set an image version's status (ACTIVE | INACTIVE).
    UpdateVersion {
        /// Image identifier or ARN.
        #[arg(long)]
        image: String,
        /// Image version (major.minor).
        #[arg(long)]
        version: String,
        /// New status: ACTIVE or INACTIVE (required by the service).
        #[arg(long, value_parser = parse_image_version_status)]
        status: aws_sdk_lambdamicrovms::types::MicrovmImageVersionStatus,
    },
    /// Delete ONE image version (irreversible).
    DeleteVersion {
        /// Image identifier or ARN.
        #[arg(long)]
        image: String,
        /// Image version (major.minor).
        #[arg(long)]
        version: String,
    },
    /// Delete an image and ALL its versions (irreversible).
    Delete {
        /// Image identifier or ARN.
        #[arg(long)]
        image: String,
    },
}

/// Clap value parser: accept ACTIVE/INACTIVE (case-insensitive) and reject
/// anything else so typos can't become SDK `Unknown` variants.
fn parse_image_version_status(
    s: &str,
) -> Result<aws_sdk_lambdamicrovms::types::MicrovmImageVersionStatus, String> {
    match s.to_ascii_uppercase().as_str() {
        "ACTIVE" => Ok(aws_sdk_lambdamicrovms::types::MicrovmImageVersionStatus::Active),
        "INACTIVE" => Ok(aws_sdk_lambdamicrovms::types::MicrovmImageVersionStatus::Inactive),
        other => Err(format!("status must be ACTIVE or INACTIVE, got {other:?}")),
    }
}

#[derive(Args)]
struct TagCmd {
    #[command(subcommand)]
    cmd: TagSub,
}

#[derive(Subcommand)]
enum TagSub {
    /// List tags on a resource ARN.
    List {
        /// MicroVM or image ARN.
        #[arg(long)]
        resource: String,
    },
    /// Set tags (KEY=VALUE, repeatable) on a resource ARN.
    Set {
        /// MicroVM or image ARN.
        #[arg(long)]
        resource: String,
        /// Tag as KEY=VALUE (repeatable, at least one required).
        #[arg(long = "tag", value_name = "KEY=VALUE", required = true)]
        tags: Vec<String>,
    },
    /// Remove tag keys (repeatable) from a resource ARN.
    Unset {
        /// MicroVM or image ARN.
        #[arg(long)]
        resource: String,
        /// Tag key to remove (repeatable, at least one required).
        #[arg(long = "key", required = true)]
        keys: Vec<String>,
    },
}

#[derive(Args)]
struct DevCmd {
    /// Base URL of the local app to emulate in front of.
    #[arg(long)]
    app_url: String,
    /// The app's port — the only `X-aws-proxy-port` value accepted.
    #[arg(
        long,
        default_value_t = kotatsu::DEFAULT_APP_PORT,
        value_parser = clap::value_parser!(u16).range(1..)
    )]
    app_port: u16,
    /// Address the fake VM endpoint binds.
    #[arg(long, default_value = "127.0.0.1:0")]
    listen: SocketAddr,
    /// JSON payload sent as runHookPayload in the /run hook.
    #[arg(long)]
    run_hook_payload: Option<String>,
    /// Reject `dev-token-*` (only --token values are accepted).
    #[arg(long)]
    no_mock_tokens: bool,
    /// Extra accepted X-aws-proxy-auth values.
    #[arg(long = "token")]
    tokens: Vec<String>,
}

#[derive(Args)]
struct CostArgs {
    /// Baseline memory in GB (tiered: 1,2,4 → 1,2,4 vCPU).
    #[arg(long, default_value = "2")]
    baseline_gb: u32,
    /// Non-peak RUNNING seconds in the period.
    #[arg(long, default_value = "0")]
    baseline_seconds: u64,
    /// RUNNING seconds at the 4x peak envelope.
    #[arg(long, default_value = "0")]
    peak_seconds: u64,
    /// suspend-microvm events (snapshot write each).
    #[arg(long, default_value = "0")]
    suspends: u64,
    /// resume-microvm events (snapshot read each).
    #[arg(long, default_value = "0")]
    resumes: u64,
    /// Fresh launches (snapshot read of image_gb each).
    #[arg(long, default_value = "0")]
    launches: u64,
    /// GB-hours of suspended state retained.
    #[arg(long, default_value = "0")]
    suspended_gb_hours: f64,
    /// Per-image size in GB (also the per-launch read size).
    #[arg(long, default_value = "0")]
    image_gb: u32,
    /// Number of images stored for the period.
    #[arg(long, default_value = "1")]
    image_count: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kotatsu=info".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Vm(c) => vm(c, cli.region).await,
        Cmd::Token(c) => token(c, cli.region).await,
        Cmd::Image(c) => image(c, cli.region).await,
        Cmd::Tag(c) => tag(c, cli.region).await,
        Cmd::Dev(c) => dev(c).await,
        Cmd::Cost(c) => cost(c),
        Cmd::Serve { args } => serve(args, cli.region),
    }
}

async fn sdk_config(region: &Option<String>) -> aws_config::SdkConfig {
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
    if let Some(r) = region {
        loader = loader.region(aws_config::Region::new(r.clone()));
    }
    loader.load().await
}

async fn aws_cp(region: &Option<String>) -> anyhow::Result<AwsControlPlane> {
    Ok(AwsControlPlane::new(&sdk_config(region).await))
}

fn vm_id(id: &str) -> anyhow::Result<MicrovmId> {
    MicrovmId::new(id).map_err(Into::into)
}

async fn vm(c: VmCmd, region: Option<String>) -> anyhow::Result<()> {
    let cp = aws_cp(&region).await?;
    match c.cmd {
        VmSub::List { image, version } => {
            let vms = cp.list(image.as_deref(), version.as_deref()).await?;
            println!(
                "{:<38} {:<11} {:<10} STARTED_UNIX",
                "ID", "STATE", "VERSION"
            );
            for v in &vms {
                println!(
                    "{:<38} {:<11} {:<10} {}",
                    v.id,
                    v.state,
                    v.image_version,
                    v.started_at_secs
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| "-".into()),
                );
            }
        }
        VmSub::Get { id } => {
            let v = cp.get(&vm_id(&id)?).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "id": v.id.to_string(),
                    "state": v.state.to_string(),
                    "endpoint": v.endpoint,
                    "image_arn": v.image_arn,
                    "image_version": v.image_version,
                    "execution_role_arn": v.execution_role_arn,
                    "maximum_duration_seconds": v.maximum_duration_seconds,
                    "started_at_secs": v.started_at_secs,
                    "terminated_at_secs": v.terminated_at_secs,
                    "state_reason": v.state_reason,
                    "ingress_connectors": v.ingress_connectors,
                    "egress_connectors": v.egress_connectors,
                }))?
            );
        }
        VmSub::Run {
            image,
            version,
            execution_role_arn,
            maximum_duration_seconds,
            idle_suspend_seconds,
            suspended_ttl_seconds,
            no_auto_resume,
            ingress_connectors,
            egress_connectors,
            client_token,
            run_hook_payload,
            wait,
        } => {
            let mut req = RunRequest::new(image);
            req.image_version = version;
            req.execution_role_arn = execution_role_arn;
            req.maximum_duration_seconds = maximum_duration_seconds;
            req.run_hook_payload = run_hook_payload;
            req.ingress_connectors = ingress_connectors;
            req.egress_connectors = egress_connectors;
            req.client_token =
                Some(client_token.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()));
            if let Some(idle) = idle_suspend_seconds {
                req.idle_policy = Some(IdlePolicyConfig {
                    auto_resume_enabled: !no_auto_resume,
                    max_idle_duration_seconds: idle,
                    suspended_duration_seconds: suspended_ttl_seconds
                        .unwrap_or(kotatsu::MAX_DURATION_SECONDS),
                });
            }
            req.validate()?;
            let vm = cp.run(&req).await?;
            println!("{} {}", vm.id, vm.state);
            if wait {
                let running = wait_until_running(&cp, &vm.id, &WaitPolicy::default()).await?;
                println!("{} {}", running.id(), running.microvm().endpoint);
            }
        }
        VmSub::Suspend { id } => {
            cp.suspend(&vm_id(&id)?).await?;
            println!("suspend requested: {id}");
        }
        VmSub::Resume { id, wait } => {
            let id = vm_id(&id)?;
            cp.resume(&id).await?;
            println!("resume requested: {id}");
            if wait {
                // Poll only — resume was already issued above, so don't use
                // wait_until_running (it re-issues resume on every SUSPENDED).
                let vm = wait_for_state(&cp, &id, &State::Running, &WaitPolicy::default()).await?;
                println!("{} {}", vm.id, vm.endpoint);
            }
        }
        VmSub::Terminate { id } => {
            cp.terminate(&vm_id(&id)?).await?;
            println!("terminate requested: {id}");
        }
    }
    Ok(())
}

async fn image(c: ImageCmd, region: Option<String>) -> anyhow::Result<()> {
    // Image lifecycle ops live on the raw SDK — the ControlPlane trait is
    // the sandbox-lifecycle surface only.
    let sdk = aws_sdk_lambdamicrovms::Client::new(&sdk_config(&region).await);
    match c.cmd {
        ImageSub::Create {
            name,
            s3_uri,
            base_image_arn,
            build_role_arn,
            description,
        } => {
            if !s3_uri.starts_with("s3://") {
                bail!("--s3-uri must be an s3:// URI of the Dockerfile zip");
            }
            let mut b = sdk
                .create_microvm_image()
                .name(name)
                .base_image_arn(base_image_arn)
                .build_role_arn(build_role_arn)
                .code_artifact(aws_sdk_lambdamicrovms::types::CodeArtifact::Uri(s3_uri));
            if let Some(v) = description {
                b = b.description(v);
            }
            let out = b.send().await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "image_arn": out.image_arn,
                    "name": out.name,
                    "state": out.state.to_string(),
                    "image_version": out.image_version,
                    "latest_active_image_version": out.latest_active_image_version,
                }))?
            );
        }
        ImageSub::List { name_filter } => {
            let mut b = sdk.list_microvm_images();
            if let Some(f) = name_filter {
                b = b.name_filter(f);
            }
            let mut pages = b.into_paginator().send();
            println!("{:<40} {:<12} {:<14} ARN", "NAME", "STATE", "LATEST_ACTIVE");
            while let Some(page) = pages.try_next().await? {
                for it in &page.items {
                    println!(
                        "{:<40} {:<12} {:<14} {}",
                        it.name,
                        it.state,
                        it.latest_active_image_version.as_deref().unwrap_or("-"),
                        it.image_arn,
                    );
                }
            }
        }
        ImageSub::Versions { image } => {
            let mut pages = sdk
                .list_microvm_image_versions()
                .image_identifier(image)
                .into_paginator()
                .send();
            println!("{:<12} {:<12} {:<12} ARN", "VERSION", "STATE", "STATUS");
            while let Some(page) = pages.try_next().await? {
                for it in &page.items {
                    println!(
                        "{:<12} {:<12} {:<12} {}",
                        it.image_version, it.state, it.status, it.image_arn,
                    );
                }
            }
        }
        ImageSub::Base { image } => match image {
            None => {
                let mut pages = sdk.list_managed_microvm_images().into_paginator().send();
                println!("ARN");
                while let Some(page) = pages.try_next().await? {
                    for it in &page.items {
                        println!("{}", it.image_arn);
                    }
                }
            }
            Some(image) => {
                let mut pages = sdk
                    .list_managed_microvm_image_versions()
                    .image_identifier(image)
                    .into_paginator()
                    .send();
                println!("{:<12} {:<12} ARN", "VERSION", "STATUS");
                while let Some(page) = pages.try_next().await? {
                    for it in &page.items {
                        println!(
                            "{:<12} {:<12} {}",
                            it.image_version,
                            it.status
                                .as_ref()
                                .map(|s| s.to_string())
                                .unwrap_or_default(),
                            it.image_arn,
                        );
                    }
                }
            }
        },
        ImageSub::Builds { image, version } => {
            let mut pages = sdk
                .list_microvm_image_builds()
                .image_identifier(image)
                .image_version(version)
                .into_paginator()
                .send();
            println!(
                "{:<40} {:<10} {:<14} CREATED_UNIX",
                "BUILD_ID", "VERSION", "STATE"
            );
            while let Some(page) = pages.try_next().await? {
                for it in &page.items {
                    println!(
                        "{:<40} {:<10} {:<14} {}",
                        it.build_id,
                        it.image_version,
                        it.build_state,
                        it.created_at.secs(),
                    );
                }
            }
        }
        ImageSub::Build {
            image,
            version,
            build_id,
        } => {
            let out = sdk
                .get_microvm_image_build()
                .image_identifier(image)
                .image_version(version)
                .build_id(build_id)
                .send()
                .await?;
            let snap = out.snapshot_build.as_ref();
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "image_arn": out.image_arn,
                    "image_version": out.image_version,
                    "build_id": out.build_id,
                    "build_state": out.build_state.to_string(),
                    "state_reason": out.state_reason,
                    "architecture": out.architecture.to_string(),
                    "chipset": out.chipset.to_string(),
                    "chipset_generation": out.chipset_generation,
                    "created_at_secs": out.created_at.secs(),
                    "snapshot": snap.map(|s| serde_json::json!({
                        "memory_snapshot_bytes": s.memory_snapshot_size_in_bytes,
                        "code_install_bytes": s.code_install_size_in_bytes,
                        "disk_snapshot_bytes": s.disk_snapshot_size_in_bytes,
                    })),
                }))?
            );
        }
        ImageSub::Get { image } => {
            let out = sdk
                .get_microvm_image()
                .image_identifier(image)
                .send()
                .await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "image_arn": out.image_arn,
                    "name": out.name,
                    "state": out.state.to_string(),
                    "latest_active_image_version": out.latest_active_image_version,
                    "latest_failed_image_version": out.latest_failed_image_version,
                    "created_at_secs": out.created_at.secs(),
                    "updated_at_secs": out.updated_at.map(|t| t.secs()),
                    "tags": out.tags,
                }))?
            );
        }
        ImageSub::GetVersion { image, version } => {
            let out = sdk
                .get_microvm_image_version()
                .image_identifier(image)
                .image_version(version)
                .send()
                .await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "image_arn": out.image_arn,
                    "image_version": out.image_version,
                    "state": out.state.to_string(),
                    "status": out.status.to_string(),
                    "state_reason": out.state_reason,
                    "base_image_arn": out.base_image_arn,
                    "base_image_version": out.base_image_version,
                    "build_role_arn": out.build_role_arn,
                    "description": out.description,
                    "egress_network_connectors": out.egress_network_connectors,
                    "environment_variables": out.environment_variables,
                    "created_at_secs": out.created_at.secs(),
                    "updated_at_secs": out.updated_at.map(|t| t.secs()),
                    "tags": out.tags,
                }))?
            );
        }
        ImageSub::Update {
            image,
            s3_uri,
            base_image_arn,
            base_image_version,
            build_role_arn,
            description,
        } => {
            if !s3_uri.starts_with("s3://") {
                bail!("--s3-uri must be an s3:// URI of the Dockerfile zip");
            }
            let mut b = sdk
                .update_microvm_image()
                .image_identifier(image)
                .base_image_arn(base_image_arn)
                .build_role_arn(build_role_arn)
                .code_artifact(aws_sdk_lambdamicrovms::types::CodeArtifact::Uri(s3_uri));
            if let Some(v) = base_image_version {
                b = b.base_image_version(v);
            }
            if let Some(v) = description {
                b = b.description(v);
            }
            let out = b.send().await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "image_arn": out.image_arn,
                    "name": out.name,
                    "state": out.state.to_string(),
                    "image_version": out.image_version,
                }))?
            );
        }
        ImageSub::UpdateVersion {
            image,
            version,
            status,
        } => {
            let out = sdk
                .update_microvm_image_version()
                .image_identifier(image)
                .image_version(version)
                .status(status)
                .send()
                .await?;
            println!("{} -> {} ({})", out.image_arn, out.status, out.state);
        }
        ImageSub::DeleteVersion { image, version } => {
            let out = sdk
                .delete_microvm_image_version()
                .image_identifier(image)
                .image_version(version)
                .send()
                .await?;
            println!(
                "delete requested: {} v{} ({})",
                out.image_identifier, out.image_version, out.state
            );
        }
        ImageSub::Delete { image } => {
            let out = sdk
                .delete_microvm_image()
                .image_identifier(&image)
                .send()
                .await?;
            println!("delete requested: {} ({})", out.image_identifier, out.state);
        }
    }
    Ok(())
}

/// Split a `KEY=VALUE` tag argument, rejecting missing `=` or empty keys.
fn parse_tag_kv(t: &str) -> anyhow::Result<(&str, &str)> {
    let (k, v) = t
        .split_once('=')
        .ok_or_else(|| anyhow::anyhow!("--tag must be KEY=VALUE, got {t:?}"))?;
    if k.is_empty() {
        bail!("--tag key must not be empty: {t:?}");
    }
    Ok((k, v))
}

async fn tag(c: TagCmd, region: Option<String>) -> anyhow::Result<()> {
    let sdk = aws_sdk_lambdamicrovms::Client::new(&sdk_config(&region).await);
    match c.cmd {
        TagSub::List { resource } => {
            let out = sdk.list_tags().resource(resource).send().await?;
            let mut tags: Vec<_> = out.tags.unwrap_or_default().into_iter().collect();
            tags.sort();
            for (k, v) in tags {
                println!("{k}={v}");
            }
        }
        TagSub::Set { resource, tags } => {
            let mut map = std::collections::HashMap::new();
            for t in &tags {
                let (k, v) = parse_tag_kv(t)?;
                map.insert(k.to_owned(), v.to_owned());
            }
            let n = map.len();
            sdk.tag_resource()
                .resource(resource)
                .set_tags(Some(map))
                .send()
                .await?;
            println!("{n} tag(s) set");
        }
        TagSub::Unset { resource, keys } => {
            if keys.iter().any(|k| k.is_empty()) {
                bail!("--key must not be empty");
            }
            sdk.untag_resource()
                .resource(resource)
                .set_tag_keys(Some(keys.clone()))
                .send()
                .await?;
            println!("{} tag(s) removed", keys.len());
        }
    }
    Ok(())
}

fn check_ttl(ttl_minutes: i32) -> anyhow::Result<()> {
    if !(1..=kotatsu::MAX_TOKEN_TTL_MINUTES).contains(&ttl_minutes) {
        bail!("--ttl-minutes must be 1-{}", kotatsu::MAX_TOKEN_TTL_MINUTES);
    }
    Ok(())
}

async fn token(c: TokenCmd, region: Option<String>) -> anyhow::Result<()> {
    let cp = aws_cp(&region).await?;
    match c.cmd {
        TokenSub::Mint {
            id,
            ports,
            ttl_minutes,
        } => {
            let scope: Vec<PortSpec> = ports
                .iter()
                .map(|p| PortSpec::parse(p))
                .collect::<Result<_, _>>()
                .context("bad --ports")?;
            check_ttl(ttl_minutes)?;
            let t = cp.mint_token(&vm_id(&id)?, &scope, ttl_minutes).await?;
            // The user explicitly asked for a credential — print it.
            println!("{}", t.header_value());
        }
        TokenSub::Shell { id, ttl_minutes } => {
            check_ttl(ttl_minutes)?;
            let t = cp.mint_shell_token(&vm_id(&id)?, ttl_minutes).await?;
            println!("{}", t.header_value());
        }
    }
    Ok(())
}

async fn dev(c: DevCmd) -> anyhow::Result<()> {
    if c.no_mock_tokens && c.tokens.is_empty() {
        eprintln!("warning: --no-mock-tokens with no --token means no token is accepted");
    }
    let mut cfg = EmulatorConfig::new(c.app_url);
    cfg.app_port = c.app_port;
    cfg.listen = c.listen;
    cfg.accept_mock_tokens = !c.no_mock_tokens;
    cfg.accepted_tokens = c.tokens;
    if let Some(p) = c.run_hook_payload {
        if p.len() > kotatsu::MAX_RUN_HOOK_PAYLOAD_BYTES {
            bail!(
                "--run-hook-payload exceeds {} bytes",
                kotatsu::MAX_RUN_HOOK_PAYLOAD_BYTES
            );
        }
        cfg.run_hook_payload = p;
    }
    let emu = Emulator::start(cfg).await?;
    println!("emulator endpoint: {}", emu.endpoint());
    println!("control API:       {}/_kotatsu/state", emu.endpoint());
    println!(
        "point kotatsud at it: kotatsud --mock --mock-endpoint {} …",
        emu.endpoint()
    );
    // Surface boot failure instead of serving a broken emulator silently;
    // anything but Running (Failed, or Terminated via the control API
    // mid-boot) is a dead end.
    match emu.wait_boot().await {
        kotatsu_dev::DevState::Running => tracing::info!("emulator ready"),
        s => bail!("emulator boot failed: {s}"),
    }
    // Run until interrupted, then fire the app's /terminate hook.
    tokio::signal::ctrl_c().await?;
    eprintln!("shutting down — calling the app's /terminate hook");
    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), emu.terminate()).await;
    Ok(())
}

fn cost(c: CostArgs) -> anyhow::Result<()> {
    let spec = kotatsu::cost::MicrovmSpec::baseline(c.baseline_gb)?;
    let mut usage = kotatsu::cost::Usage::new(spec);
    usage.baseline_seconds = c.baseline_seconds;
    usage.peak_seconds = c.peak_seconds;
    usage.suspends = c.suspends;
    usage.resumes = c.resumes;
    usage.launches = c.launches;
    usage.image_gb = c.image_gb;
    usage.image_count = c.image_count;
    usage.suspended_gb_hours =
        Decimal::from_f64(c.suspended_gb_hours).context("invalid --suspended-gb-hours")?;
    let est = kotatsu::cost::PriceBook::us_east_1().estimate(&usage)?;
    println!("compute:          ${:.2}", est.compute);
    println!("snapshot reads:   ${:.2}", est.snapshot_reads);
    println!("snapshot writes:  ${:.2}", est.snapshot_writes);
    println!("snapshot storage: ${:.2}", est.snapshot_storage);
    println!(
        "total:            ${:.2}  (us-east-1, data transfer excluded)",
        est.total
    );
    Ok(())
}

fn serve(args: Vec<String>, region: Option<String>) -> anyhow::Result<()> {
    // The parent's --region is consumed by clap before it reaches `args`;
    // inject it back so `kotatsu --region X serve` and
    // `kotatsu serve -- --region X` behave the same.
    let mut forwarded = args;
    let user_set_region = forwarded
        .iter()
        .any(|a| a == "--region" || a.starts_with("--region="));
    if let Some(r) = region.filter(|_| !user_set_region) {
        forwarded.splice(0..0, ["--region".to_string(), r]);
    }
    let mut cmd = std::process::Command::new("kotatsud");
    cmd.args(&forwarded);

    #[cfg(unix)]
    {
        // True exec: PID, exit status and signals pass through unchanged,
        // and no orphaned kotatsud survives a signal to this process.
        use std::os::unix::process::CommandExt;
        Err(cmd.exec()).context(KOTATSUD_EXEC_FAILED)
    }
    #[cfg(not(unix))]
    {
        let status = cmd.status().context(KOTATSUD_EXEC_FAILED)?;
        std::process::exit(status.code().unwrap_or(1));
    }
}

/// Hint for a missing `kotatsud`; it has to work outside a clone of the
/// repository.
const KOTATSUD_EXEC_FAILED: &str = "failed to exec `kotatsud` — install it from the GitHub \
     Releases tarball or with `cargo install --locked --git https://github.com/seike460/kotatsu kotatsud`";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_vm_subcommands() {
        let cli = Cli::try_parse_from([
            "kotatsu",
            "vm",
            "list",
            "--image",
            "img-1",
            "--version",
            "7",
        ])
        .unwrap();
        match cli.cmd {
            Cmd::Vm(VmCmd {
                cmd: VmSub::List { image, version },
            }) => {
                assert_eq!(image.as_deref(), Some("img-1"));
                assert_eq!(version.as_deref(), Some("7"));
            }
            _ => panic!("wrong parse"),
        }
    }

    #[test]
    fn parses_run_flags() {
        let cli = Cli::try_parse_from([
            "kotatsu",
            "vm",
            "run",
            "--image",
            "arn:img",
            "--idle-suspend-seconds",
            "300",
            "--suspended-ttl-seconds",
            "3600",
            "--wait",
        ])
        .unwrap();
        match cli.cmd {
            Cmd::Vm(VmCmd {
                cmd:
                    VmSub::Run {
                        image,
                        idle_suspend_seconds,
                        suspended_ttl_seconds,
                        wait,
                        ..
                    },
            }) => {
                assert_eq!(image, "arn:img");
                assert_eq!(idle_suspend_seconds, Some(300));
                assert_eq!(suspended_ttl_seconds, Some(3600));
                assert!(wait);
            }
            _ => panic!("wrong parse"),
        }
    }

    #[test]
    fn parses_token_port_scope() {
        let cli = Cli::try_parse_from([
            "kotatsu",
            "token",
            "mint",
            "microvm-123",
            "--ports",
            "8080,9000-9010",
            "--ttl-minutes",
            "30",
        ])
        .unwrap();
        match cli.cmd {
            Cmd::Token(TokenCmd {
                cmd:
                    TokenSub::Mint {
                        id,
                        ports,
                        ttl_minutes,
                    },
            }) => {
                assert_eq!(id, "microvm-123");
                assert_eq!(ports, ["8080", "9000-9010"]);
                assert_eq!(ttl_minutes, 30);
            }
            _ => panic!("wrong parse"),
        }
    }

    #[test]
    fn port_specs_parse() {
        assert_eq!(PortSpec::parse("8080").unwrap(), PortSpec::Port(8080));
        assert_eq!(
            PortSpec::parse("9000-9010").unwrap(),
            PortSpec::Range {
                start: 9000,
                end: 9010
            }
        );
        assert_eq!(PortSpec::parse("all").unwrap(), PortSpec::All);
        assert!(PortSpec::parse("0").is_err());
        assert!(PortSpec::parse("9000-9000").is_ok());
        assert!(PortSpec::parse("9010-9000").is_err());
    }

    #[test]
    fn ttl_bounds() {
        assert!(check_ttl(1).is_ok());
        assert!(check_ttl(60).is_ok());
        assert!(check_ttl(0).is_err());
        assert!(check_ttl(61).is_err());
        assert!(check_ttl(-5).is_err());
    }

    #[test]
    fn parses_dev_flags() {
        let cli = Cli::try_parse_from([
            "kotatsu",
            "dev",
            "--app-url",
            "http://127.0.0.1:3000",
            "--app-port",
            "3000",
            "--token",
            "t1",
            "--token",
            "t2",
        ])
        .unwrap();
        match cli.cmd {
            Cmd::Dev(d) => {
                assert_eq!(d.app_url, "http://127.0.0.1:3000");
                assert_eq!(d.app_port, 3000);
                assert_eq!(d.tokens, ["t1", "t2"]);
            }
            _ => panic!("wrong parse"),
        }
    }

    #[test]
    fn parses_serve_passthrough() {
        let cli = Cli::try_parse_from([
            "kotatsu",
            "serve",
            "--",
            "--mock",
            "--listen",
            "127.0.0.1:9000",
        ])
        .unwrap();
        match cli.cmd {
            Cmd::Serve { args } => {
                assert_eq!(args, ["--mock", "--listen", "127.0.0.1:9000"]);
            }
            _ => panic!("wrong parse"),
        }
    }

    #[test]
    fn serve_hyphen_args_without_separator() {
        let cli = Cli::try_parse_from(["kotatsu", "serve", "--mock", "--listen", "x"]).unwrap();
        match cli.cmd {
            Cmd::Serve { args } => assert_eq!(args, ["--mock", "--listen", "x"]),
            _ => panic!("wrong parse"),
        }
    }

    #[test]
    fn suspended_ttl_requires_idle_suspend() {
        assert!(
            Cli::try_parse_from([
                "kotatsu",
                "vm",
                "run",
                "--image",
                "i",
                "--suspended-ttl-seconds",
                "60"
            ])
            .is_err()
        );
    }

    #[test]
    fn ports_all_parses() {
        let cli = Cli::try_parse_from(["kotatsu", "token", "mint", "m", "--ports", "all"]).unwrap();
        match cli.cmd {
            Cmd::Token(TokenCmd {
                cmd: TokenSub::Mint { ports, .. },
            }) => {
                assert_eq!(ports, ["all"]);
                assert_eq!(PortSpec::parse(&ports[0]).unwrap(), PortSpec::All);
            }
            _ => panic!("wrong parse"),
        }
    }

    #[test]
    fn parses_image_subcommands() {
        let cli = Cli::try_parse_from([
            "kotatsu",
            "image",
            "create",
            "--name",
            "sandbox",
            "--s3-uri",
            "s3://b/img.zip",
            "--base-image-arn",
            "arn:aws:lambda:us-east-1::microvm-image:base",
            "--build-role-arn",
            "arn:aws:iam::1:role/r",
        ])
        .unwrap();
        match cli.cmd {
            Cmd::Image(ImageCmd {
                cmd:
                    ImageSub::Create {
                        name,
                        s3_uri,
                        base_image_arn,
                        build_role_arn,
                        ..
                    },
            }) => {
                assert_eq!(name, "sandbox");
                assert_eq!(s3_uri, "s3://b/img.zip");
                assert_eq!(
                    base_image_arn,
                    "arn:aws:lambda:us-east-1::microvm-image:base"
                );
                assert_eq!(build_role_arn, "arn:aws:iam::1:role/r");
            }
            _ => panic!("wrong parse"),
        }
        // Required by the service — clap must refuse these omissions.
        assert!(
            Cli::try_parse_from([
                "kotatsu", "image", "create", "--name", "n", "--s3-uri", "s3://b/x"
            ])
            .is_err()
        );
        // builds --version is required by the service.
        assert!(Cli::try_parse_from(["kotatsu", "image", "builds", "--image", "img"]).is_err());
        assert!(
            Cli::try_parse_from([
                "kotatsu",
                "image",
                "builds",
                "--image",
                "img",
                "--version",
                "1.0"
            ])
            .is_ok()
        );
        assert!(Cli::try_parse_from(["kotatsu", "image", "delete", "--image", "img"]).is_ok());
        // list / versions / base / build coverage.
        assert!(Cli::try_parse_from(["kotatsu", "image", "list"]).is_ok());
        assert!(Cli::try_parse_from(["kotatsu", "image", "list", "--name-filter", "sand"]).is_ok());
        assert!(Cli::try_parse_from(["kotatsu", "image", "versions"]).is_err());
        assert!(Cli::try_parse_from(["kotatsu", "image", "versions", "--image", "img"]).is_ok());
        assert!(Cli::try_parse_from(["kotatsu", "image", "base"]).is_ok());
        assert!(Cli::try_parse_from(["kotatsu", "image", "base", "--image", "b"]).is_ok());
        // get / get-version / update / update-version / delete-version.
        assert!(Cli::try_parse_from(["kotatsu", "image", "get", "--image", "i"]).is_ok());
        assert!(Cli::try_parse_from(["kotatsu", "image", "get"]).is_err());
        assert!(Cli::try_parse_from(["kotatsu", "image", "get-version", "--image", "i"]).is_err());
        assert!(
            Cli::try_parse_from([
                "kotatsu",
                "image",
                "get-version",
                "--image",
                "i",
                "--version",
                "1.0"
            ])
            .is_ok()
        );
        // update: s3-uri / base-image-arn / build-role-arn are required.
        assert!(
            Cli::try_parse_from([
                "kotatsu", "image", "update", "--image", "i", "--s3-uri", "s3://b/z",
            ])
            .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "kotatsu",
                "image",
                "update",
                "--image",
                "i",
                "--s3-uri",
                "s3://b/z",
                "--base-image-arn",
                "arn:b",
                "--build-role-arn",
                "arn:r"
            ])
            .is_ok()
        );
        // update-version: status parser rejects unknown values.
        assert!(
            Cli::try_parse_from([
                "kotatsu",
                "image",
                "update-version",
                "--image",
                "i",
                "--version",
                "1.0",
                "--status",
                "BOGUS"
            ])
            .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "kotatsu",
                "image",
                "update-version",
                "--image",
                "i",
                "--version",
                "1.0",
                "--status",
                "inactive"
            ])
            .is_ok()
        );
        assert!(
            Cli::try_parse_from(["kotatsu", "image", "delete-version", "--image", "i"]).is_err()
        );
        assert!(
            Cli::try_parse_from([
                "kotatsu",
                "image",
                "delete-version",
                "--image",
                "i",
                "--version",
                "1.0"
            ])
            .is_ok()
        );
        assert!(
            Cli::try_parse_from([
                "kotatsu",
                "image",
                "build",
                "--image",
                "img",
                "--build-id",
                "b1"
            ])
            .is_err()
        );
        assert!(
            Cli::try_parse_from([
                "kotatsu",
                "image",
                "build",
                "--image",
                "img",
                "--version",
                "1.0",
                "--build-id",
                "b1"
            ])
            .is_ok()
        );
    }

    #[test]
    fn parses_tag_subcommands() {
        assert!(Cli::try_parse_from(["kotatsu", "tag", "list", "--resource", "arn:x"]).is_ok());
        assert!(Cli::try_parse_from(["kotatsu", "tag", "list"]).is_err());
        // set requires at least one --tag.
        assert!(Cli::try_parse_from(["kotatsu", "tag", "set", "--resource", "arn:x"]).is_err());
        assert!(
            Cli::try_parse_from([
                "kotatsu",
                "tag",
                "set",
                "--resource",
                "arn:x",
                "--tag",
                "env=prod",
                "--tag",
                "team=core"
            ])
            .is_ok()
        );
        // unset requires at least one --key.
        assert!(Cli::try_parse_from(["kotatsu", "tag", "unset", "--resource", "arn:x"]).is_err());
        assert!(
            Cli::try_parse_from([
                "kotatsu",
                "tag",
                "unset",
                "--resource",
                "arn:x",
                "--key",
                "env"
            ])
            .is_ok()
        );
    }

    #[test]
    fn tag_kv_validation() {
        assert_eq!(parse_tag_kv("k=v").unwrap(), ("k", "v"));
        assert_eq!(parse_tag_kv("k=a=b").unwrap(), ("k", "a=b")); // value may contain '='
        assert_eq!(parse_tag_kv("k=").unwrap(), ("k", "")); // empty value is valid
        assert!(parse_tag_kv("novalue").is_err());
        assert!(parse_tag_kv("=v").is_err());
    }

    #[test]
    fn cost_rejects_bad_gb_hours() {
        let c = CostArgs {
            baseline_gb: 2,
            baseline_seconds: 0,
            peak_seconds: 0,
            suspends: 0,
            resumes: 0,
            launches: 0,
            suspended_gb_hours: -1.0,
            image_gb: 0,
            image_count: 1,
        };
        assert!(cost(c).is_err());
    }

    #[test]
    fn empty_vm_id_rejected() {
        assert!(vm_id("").is_err());
        assert!(vm_id("microvm-abc").is_ok());
    }
}
