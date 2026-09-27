//! kotatsud — session gateway daemon for AWS Lambda MicroVM sandboxes.
//!
//! See `src/main.rs` for the binary entrypoint; this library exposes the
//! gateway router so tests can drive it against `MockControlPlane`.

pub mod gateway;
