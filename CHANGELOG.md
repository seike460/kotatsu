# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Security

- The `SandboxPool` proxy client and the default `MicrovmEndpoint` client
  no longer follow HTTP redirects; a VM's 3xx response reaches the caller
  unchanged. Before, kotatsud followed a VM-supplied `Location` itself —
  fetching internal URLs (SSRF) with `X-aws-proxy-auth` attached — and a
  redirect's own `Set-Cookie` never reached the browser.
- kotatsud error responses no longer carry internal details. The JSON
  body is a fixed message per status (e.g. `upstream unavailable`), and
  the error itself — VM endpoint URL, MicroVM ID, AWS error message —
  is logged at `warn` with the tenant.
- kotatsud also strips `Service-Worker-Allowed`, `Clear-Site-Data`,
  `NEL` and `Report-To` from upstream responses, as it already did
  `Strict-Transport-Security` and `Alt-Svc`. All tenants share the
  gateway origin, so one tenant could otherwise register a service
  worker over every tenant's pages, clear their cookies and storage, or
  collect network error reports for the whole origin.

### Fixed

- `wss://` handshakes (`WsRequest::connect`, kotatsud WebSocket proxying
  to real MicroVM endpoints, `kotatsu dev` with an `https` `--app-url`)
  no longer panic with "Could not automatically determine the
  process-level CryptoProvider". They use the process-default rustls
  provider when one is installed, else `ring`.
- `Error::Aws` and `Error::Conflict` messages now include the AWS error
  code and message (or the transport cause) instead of only
  "service error" / "dispatch failure", so `%e` logs show why a call
  failed (e.g. `AccessDeniedException`).
- Docs: `PoolReport::reaped` and `kotatsu_pool_reaped_total` also count
  sentinel-marked and lost VMs, not only `max_age` expiries.
- `SandboxPool::stats` no longer sets the `kotatsu_pool_assigned` gauge
  to 0 when the state store cannot be listed. It logs the failure and
  leaves the pool gauges at their last values.
- `SandboxPool::drain` logs a failed binding release instead of
  discarding the error; its docs now state that it is best effort.
- Docs: `Binding::sentinel` is no longer hidden on docs.rs, and states
  that a custom `StateStore` must persist it and compare it in `claim`.

### Changed

- `MockControlPlane::terminate` is idempotent like `terminate-microvm`:
  terminating an already terminated MicroVM succeeds instead of
  returning `Error::Terminated`.
- `SandboxPool::new` rejects `reap_lost_vms = true` unless
  `run_request.image_identifier` is an image ARN (`arn:…`); for
  kotatsud, `--reap-lost-vms` needs an ARN `--image`. `list-microvms`
  reports image ARNs, so with an image ID the lost-VM reconcile
  silently matched nothing.
- `kotatsud::gateway` is hidden from the docs. The kotatsud library
  exists for its binary and tests and is not a stable API.

## [0.1.0] - 2026-09-28

### Added

- `kotatsu` core library: `ControlPlane` trait over `aws-sdk-lambdamicrovms`,
  `AwsControlPlane` and `MockControlPlane`, lifecycle states and waiters,
  `TokenVending` (scoped `X-aws-proxy-auth` mint with TTL bounds and cache),
  `MicrovmEndpoint` HTTP/WebSocket client with header/subprotocol contract,
  `SandboxPool` warm pool with tenant affinity, `maintain`/`maintain_at`
  reaper, UTC time-of-day warm sizing via `WarmWindow` (`warm_schedule`),
  `StateStore` (in-memory + SQLite), cost estimation (`PriceBook`,
  us-east-1 ARM rates), and Prometheus metrics instrumentation.
  Restart recovery uses durable sentinel bindings (`Binding::sentinel`,
  persisted as `kind` in SQLite); the lost-VM reconcile is opt-in via
  `PoolConfig::reap_lost_vms` (off by default — enable only with a
  dedicated image, since list APIs carry no owner tag; kotatsud's flag
  also accepts `--reap-lost-vms=false` to retract a config-file `true`).
  `PoolStats` reports `warm`/`assigned`/`inflight`/`lost`.
  `detached_task_count()`
  is a `#[doc(hidden)]` diagnostic for observing the reaper JoinSet.
- `kotatsud` session gateway daemon: tenant routing (`/t/{tenant}[/{*path}]`),
  Bearer API-key auth, HTTP + WebSocket proxying with header stripping,
  per-VM token vending, `/healthz` + `/metrics`, SQLite binding persistence,
  graceful SIGINT/SIGTERM, TOML config file, `--warm-schedule` UTC windows,
  `--reap-lost-vms` (`KOTATSU_REAP_LOST_VMS`) lost-VM reconcile opt-in,
  and a `--mock`/`--mock-endpoint` development mode.
- `kotatsu-dev` local contract emulator (`kotatsu dev`): lifecycle hooks
  (`validate/ready/run/suspend/resume/terminate`), auth/port proxy contract,
  WS subprotocols, PENDING/SUSPENDED transitions, `/_kotatsu/*` control API.
- `kotatsu` CLI: `vm` (list/get/run/suspend/resume/terminate), `token`
  (mint/shell), `image` (create/list/versions/base/get/get-version/update/
  update-version/builds/build/delete-version/delete), `tag`
  (list/set/unset), `dev`, `cost`, `serve`.
- `Dockerfile` (kotatsud + kotatsu), `LICENSE-MIT`/`LICENSE-APACHE`,
  CI (fmt/clippy/test) and tag-release binary workflow.
