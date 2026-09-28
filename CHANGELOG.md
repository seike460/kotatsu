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

### Fixed

- `wss://` handshakes (`WsRequest::connect`, kotatsud WebSocket proxying
  to real MicroVM endpoints, `kotatsu dev` with an `https` `--app-url`)
  no longer panic with "Could not automatically determine the
  process-level CryptoProvider". They use the process-default rustls
  provider when one is installed, else `ring`.

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
