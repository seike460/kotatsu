# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] - 2026-09-29

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
  URL queries in that log line are replaced with `?REDACTED`: the
  proxied request carries the client's query verbatim, and the HTTP and
  WebSocket client errors repeat the URL.
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
- The `kotatsu dev` control routes (`POST /_kotatsu/suspend`,
  `/resume`, `/terminate`) refuse a request that carries `Origin` with
  403. Browsers send `Origin` with every POST, so any web page the
  developer opened could suspend or terminate the local emulator
  (CSRF). curl and other tools send no `Origin` and are not affected.
- `kotatsu dev` warns at startup when `--listen` is not a loopback
  address: the `/_kotatsu/*` control API is unauthenticated, and unless
  `--no-mock-tokens` is given, any `dev-token-*` value reaches the app.
  kotatsud already warned on such a bind.

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
- `SandboxPool::spawn_maintenance` runs its first tick at once, then
  every `maintenance_interval`. Before, it waited a full interval
  first, so kotatsud served its first 60 s with an empty warm pool
  (every acquire was a cold start) and restart recovery waited as long.
- kotatsud gives open connections 20 s after SIGTERM or SIGINT, then
  closes them and exits. Before, a response streamed from a VM (SSE, a
  long download) kept it running until the supervisor's SIGKILL.
- `kotatsud --allow-unauthenticated=false` (or
  `KOTATSU_ALLOW_UNAUTHENTICATED=false`) overrides
  `allow_unauthenticated = true` in the config file, as other flags do.
  Before, the flag and the file were OR-ed, so the file's `true` could
  not be turned off.
- Docs: `kotatsud --help` shows the default of each flag that has one,
  such as `--listen` (`127.0.0.1:3000`) and `--app-port` (`8080`).
- `kotatsu dev` relays the app's redirects unchanged. Before, the
  emulator followed a 303, or a 301/302 that answered a POST, itself:
  the client got the next page fetched as a GET, and the redirect's
  own `Set-Cookie` never reached it.
- `kotatsu dev` no longer hangs when the app's `/terminate` hook fails
  during boot. The emulator then stayed `PENDING`, answered every
  request 503 and never finished waiting for boot. It now ends
  `FAILED`.
- `kotatsu dev` answers 502 to a request whose auto-resume fails, as
  AWS does. It used to answer 503.
- `GET /_kotatsu/state` answers `{"state":"FAILED","error":"…"}` after
  a failed boot. Before, only this state was an object
  (`{"state":{"FAILED":"…"}}`) instead of a string.
- `kotatsu dev` passes the client's headers, such as `Cookie` and
  `Origin`, on the WebSocket handshake to the app, as AWS does. Before,
  the app's handshake carried none of them. The contract subprotocols
  are still removed.
- Docs: kotatsu-dev states what a failed hook does and the
  `/_kotatsu/state` response format, and `DevState::Pending` lists the
  `/validate` hook.
- `kotatsu dev` also shuts down on SIGTERM (`docker stop`, systemd, an
  IDE's stop button) and calls the app's `/terminate` hook, as it does
  on Ctrl-C. Before, SIGTERM ended it at once without the hook.
- `kotatsu dev` warns when the app's `/terminate` hook fails or does
  not finish within 10 s at shutdown. Before, the result was discarded.
- Docs: the `kotatsu cost --baseline-gb` help lists the 8 GB tier and
  gives one vCPU per 2 GB, as the estimate computes. It said 1/2/4 GB
  with one vCPU per GB.
- When `kotatsu serve` cannot find `kotatsud`, it points to the GitHub
  Releases tarball and `cargo install --git`. Before, it suggested
  `cargo install --path crates/kotatsud`, which works only inside a
  clone of the repository.
- Docs: the kotatsu-cli crate README installs both binaries (from
  crates.io or GitHub) and states that `serve` needs `kotatsud` on
  `PATH`. The 0.1.0 README suggested `cargo install kotatsu-cli` before
  the crates were published.
- The Linux release binaries run on glibc 2.28 and later (Amazon
  Linux 2023, Debian 10+, Ubuntu 20.04+, RHEL 8+). The v0.1.0 ones
  were linked against the build runner's glibc 2.39 and failed to
  start on an older glibc, such as Amazon Linux 2023's, with
  `version 'GLIBC_2.39' not found`.
- Docs: CONCEPT.md no longer says that `kotatsu dev` starts the app in
  a container: it proxies to an app already listening at `--app-url`.
  Its hook list now includes `/validate`, the roadmap lists the
  features shipped in v0.1 under v0.1 instead of v0.2, and the license
  section is no longer a draft.
- Docs: README's security model no longer says that the `Set-Cookie`
  path clamp keeps cookies from crossing tenants. The clamp stops a
  VM's `Path=/` cookie from reaching other tenants' VMs, but all
  tenants share one origin: a tenant's page can read and write another
  tenant's non-`HttpOnly` cookies from a same-origin iframe, and
  `localStorage` and IndexedDB are shared. Tenants that serve untrusted
  browser content need a host (origin) each.
- Docs: README and the kotatsud crate README state that WebSocket
  handshakes to the VM carry only the contract headers. The client's
  `Cookie`, `Origin`, subprotocols and other headers do not reach the
  VM, and `x-forwarded-*` is set on HTTP requests only.
- Docs: README's crate table names the CLI crate `kotatsu-cli` (its
  command is `kotatsu`) instead of a second `kotatsu`, and its status
  section points to CONCEPT.md §8 for what is not yet verified on AWS
  instead of calling §8 the roadmap.

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
- The `kotatsu` crate no longer turns on features it does not use:
  `behavior-version-latest` of `aws-config` and `serde` of `chrono` and
  `uuid`. A crate that relied on kotatsu for them, for example to call
  `aws_config::from_env()` without a deprecation warning or to
  serialize a `Uuid`, must enable them in its own `Cargo.toml`.
- The `kotatsu` crate moves to tokio-tungstenite 0.29 (from 0.26), the
  version axum 0.8 uses, so one WebSocket stack is built instead of two.
  `WsStream`, `ws_tls_connector()` and the `From` conversion of
  tungstenite's `Error` now name 0.29 types.
- The `sqlite` feature moves to tokio-rusqlite 0.8 (rusqlite 0.40,
  `libsqlite3-sys` 0.38). Only one `libsqlite3-sys` can link into a
  build, so an application that also uses `rusqlite` needs 0.40.
- The `Dockerfile` pins `rust:1-bookworm` and `debian:bookworm-slim` by
  digest, so a rebuild of the same source uses the same base images.
  Dependabot moves the pins.
- Release tarballs also contain `LICENSE-MIT`, `LICENSE-APACHE` and
  `README.md`. A tag with a pre-release suffix, such as `v0.2.0-rc.1`,
  is published as a GitHub pre-release.

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

[Unreleased]: https://github.com/seike460/kotatsu/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/seike460/kotatsu/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/seike460/kotatsu/releases/tag/v0.1.0
