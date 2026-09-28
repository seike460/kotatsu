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
- kotatsud config file errors no longer quote the file. They give the
  path, line and column and the parser's message. Before, a misspelled
  key or a wrong value type printed the whole line, and serde's type
  errors printed the value, so an API key could reach the startup
  error and the logs that collect it.

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
- `kotatsud --mock` keeps tenant bindings in memory unless `--state-db`
  is given. Before, it opened the default `bindings.db`, and when a
  real deployment on the same host used that file too, the first
  maintenance tick released all of its bindings: the mock control
  plane reports real MicroVMs as gone. The real VMs then ran untracked
  and kept billing.
- kotatsud forwards the request `Content-Length` when the client framed
  the body by length. Before, every upload reached the VM chunked and
  the body of a GET request was dropped.
- kotatsud answers 400 to a request target that starts with `//` or
  contains a backslash before it resolves the tenant's VM. Before, such
  a request launched or resumed the VM first and failed afterwards.
- Docs: kotatsud serves plain HTTP. Its crate README, module docs and
  CONCEPT.md no longer call it an HTTPS endpoint.
- Docs: the kotatsud crate README lists the config-file keys with their
  flags, value formats and defaults. It used to point to a reference
  that did not exist.
- Docs: README examples pass API keys through `KOTATSU_API_KEYS`
  instead of `--api-key` and recommend long random keys. The
  `--api-key` / `--tenant-key` help recommends the env var or
  `--config`, since other local users can read command-line arguments.
- Docs: README states that `__Host-` cookies do not survive the tenant
  path clamp.
- Docs: the `kotatsud --idle-suspend-secs` help no longer says that 0
  relies on the image's own idle policy. MicroVM images have no idle
  policy: with 0 (or unset), VMs run without one and are not
  auto-suspended.
- Docs: README's security model states that kotatsud forwards the
  client `Cookie` header to the tenant's VM on HTTP requests. A front
  proxy's session cookie must use a `Path` outside `/t/`, or the proxy
  must strip it.

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
