//! kotatsu-dev — local emulation of the AWS Lambda MicroVMs endpoint contract.
//!
//! Building against real MicroVMs means an AWS-side async image build
//! per iteration; this crate reproduces the *contract* locally so hook
//! implementations and client code can iterate fast:
//!
//! - lifecycle hooks driven against a user-supplied local app: the
//!   platform POSTs `/aws/lambda-microvms/runtime/v1/{validate,ready,run,
//!   suspend,resume,terminate}` (a hook answering 404/405/501 counts as
//!   "not implemented → success"), with `Pending → Running` and
//!   `Suspended → Running` transitions
//! - the proxy contract enforced in front of the app:
//!   `X-aws-proxy-auth` / `X-aws-proxy-port` headers, and the
//!   `lambda-microvms` (+ `.authentication.*` / `.port.*`) WebSocket
//!   subprotocols
//! - `autoResume` semantics: traffic to a suspended emulator calls the
//!   `/resume` hook before serving
//!
//! A real VMM (Firecracker/libkrun) is intentionally out of scope — the
//! "app" is any local HTTP server already listening.
//!
//! Point `kotatsud --mock --mock-endpoint <emulator-url>` at an
//! [`Emulator`] for a full local pipeline, or embed it in tests.
//!
//! `/_kotatsu/{state,suspend,resume,terminate}` is a reserved,
//! unauthenticated operator API on the same listener. Only the exact
//! method+path pairs are intercepted: other `/_kotatsu/*` paths fall
//! through to the app, while a wrong method on a registered path is a
//! 405 before the app sees it.
//!
//! Emulated hook order at boot: `validate → run → ready-poll` — the
//! real platform drives `ready`/`validate` at image-build time and the
//! runtime hooks at boot; the emulator collapses both into boot.
//!
//! Hook failures: a boot hook that fails or times out leaves the
//! emulator `FAILED`, and every request then gets 500 (on AWS a failed
//! `/run` may send the VM straight to `TERMINATING`). A failed
//! `/suspend`, `/resume` or `/terminate` hook leaves the state as it
//! was, so the call can be retried; only a `/terminate` during boot
//! ends `FAILED`. A request whose auto-resume fails gets 502, as on AWS.

mod emulator;
mod proxy;

pub use emulator::{DevState, Emulator, EmulatorConfig, HookPaths};
