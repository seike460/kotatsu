//! kotatsu — sandbox fleet control plane for AWS Lambda MicroVMs.
//!
//! Lambda MicroVMs gives each sandbox its own Firecracker-backed HTTPS
//! endpoint; everything between "N microVMs" and "one product" is on you.
//! kotatsu provides that middle layer:
//!
//! - `ControlPlane`: thin async wrapper over the `lambda-microvms` API.
//! - `TokenVending`: mints, scopes, caches and rotates `X-aws-proxy-auth` JWEs.
//! - `MicrovmEndpoint`: authenticated HTTP/WebSocket client for one MicroVM.
//! - `SandboxPool`: warm pools, tenant→VM affinity, age/zombie reapers
//!   (idle→suspend itself is the VM-side `IdlePolicy`).
//! - `StateStore`: binding persistence (memory + SQLite).
//!
//! All service contract constants (headers, hook paths, limits) live in this
//! crate's root so downstream code cannot drift from the AWS documentation.

mod control_plane;
pub mod cost;
mod endpoint;
mod error;
pub mod metrics;
pub mod mock;
mod pool;
mod state;
mod token;
mod types;
mod waiter;

pub use control_plane::{AwsControlPlane, ControlPlane};
pub use cost::{CostBreakdown, MicrovmSpec, PriceBook, Usage};
#[doc(hidden)]
pub use endpoint::ws_tls_connector;
pub use endpoint::{MicrovmEndpoint, WsRequest, WsStream};
pub use error::{Error, Result};
pub use pool::{PoolConfig, PoolReport, PoolStats, Sandbox, SandboxPool, WarmWindow};
#[cfg(feature = "sqlite")]
pub use state::SqliteStore;
pub use state::{Binding, ClaimOutcome, MemoryStore, StateStore};
pub use token::{TokenVending, TokenVendingConfig};
pub use types::*;
pub use waiter::{WaitPolicy, wait_for_state, wait_until_running};
