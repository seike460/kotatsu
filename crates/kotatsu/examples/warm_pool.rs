//! Warm-pool walkthrough on `MockControlPlane` — no AWS account needed.
//!
//! ```text
//! cargo run --example warm_pool -p kotatsu
//! ```
//!
//! Shows the sandbox lifecycle: `acquire` hands a warm VM to a tenant,
//! `release` terminates the VM and drops the binding, `suspend` pauses
//! while keeping the binding — and `maintain()` rewarms the pool after
//! either for the next tenant.

use std::sync::Arc;

use kotatsu::mock::MockControlPlane;
use kotatsu::{MemoryStore, PoolConfig, RunRequest, SandboxPool, TenantKey};

#[tokio::main]
async fn main() -> kotatsu::Result<()> {
    // Mock CP pretends to be AWS — VMs "run" instantly on `run-microvm`.
    let cp = Arc::new(MockControlPlane::new());

    let mut cfg = PoolConfig::new(RunRequest::new("my-sandbox-image"));
    cfg.warm_size = 2; // keep two VMs staged for instant handout
    cfg.max_vms = 8;

    let pool = Arc::new(SandboxPool::new(cp, Arc::new(MemoryStore::new()), cfg)?);
    // Warm the pool before traffic arrives (the gateway does this in a
    // spawned maintenance task).
    let report = pool.maintain().await?;
    println!("warmed {} VMs", report.warmed);

    // Tenant 1 acquires a warm VM — no cold-start wait.
    let sb = pool.acquire(&TenantKey::new("tenant-1")?).await?;
    println!("tenant-1 got {} @ {}", sb.vm().id(), sb.endpoint().url());
    // `sb.endpoint().get("/path")` would mint/refresh X-aws-proxy-auth and
    // produce a request builder with the contract headers set.
    sb.release().await?; // terminates the VM and drops the tenant binding

    // Tenant 2 suspends instead: the binding survives, so its next
    // `acquire` resumes the same VM rather than taking a warm one.
    let tenant2 = TenantKey::new("tenant-2")?;
    let sb = pool.acquire(&tenant2).await?;
    let suspended = sb.vm().id().clone();
    sb.suspend().await?;
    let sb = pool.acquire(&tenant2).await?;
    println!(
        "tenant-2 resumed {} (same VM: {})",
        sb.vm().id(),
        *sb.vm().id() == suspended
    );

    // Re-warm for the next tenant.
    pool.maintain().await?;
    let stats = pool.stats().await;
    println!(
        "pool: {} warm, {} assigned, {} inflight (max {})",
        stats.warm, stats.assigned, stats.inflight, stats.max_vms
    );

    pool.drain().await?; // terminate every managed VM (shutdown path)
    println!("drained");
    Ok(())
}
