# kotatsu(炬燵)

Sandbox fleet control plane for AWS Lambda MicroVMs — tenant affinity,
warm pools, scoped auth-token vending, and lifecycle management as a
Rust library.

```rust
let cp: Arc<dyn ControlPlane> = Arc::new(AwsControlPlane::new(&aws_config));
let mut cfg = PoolConfig::new(RunRequest::new(image_arn));
cfg.warm_size = 4;
let pool = SandboxPool::new(cp, Arc::new(MemoryStore::new()), cfg)?;
let sandbox = pool.acquire(&TenantKey::new("user-42")?).await?;
```

Full documentation, the `kotatsud` gateway daemon, and the `kotatsu`
CLI live in the [repository](https://github.com/seike460/kotatsu).

License: Apache-2.0 OR MIT.
