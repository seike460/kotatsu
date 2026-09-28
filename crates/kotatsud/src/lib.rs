//! kotatsud — session gateway daemon for AWS Lambda MicroVM sandboxes.
//!
//! See `src/main.rs` for the binary entrypoint. This library holds the
//! gateway router so the binary and the integration tests (against
//! `MockControlPlane`) share it. It is not a stable API: `gateway` is
//! hidden from the docs and outside semver.

#[doc(hidden)]
pub mod gateway;
