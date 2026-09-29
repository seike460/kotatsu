//! Persistence for tenant→MicroVM affinity bindings.
//!
//! The pool's hot state (which VMs exist, who is running) lives in AWS;
//! the one thing AWS does not track is *which tenant owns which VM*.
//! `StateStore` persists exactly that mapping. `MemoryStore` covers
//! single-process use; `SqliteStore` survives daemon restarts.

use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::HashMap;

#[cfg(feature = "sqlite")]
use tokio_rusqlite::rusqlite::{self, OptionalExtension};

use crate::error::Result;
use crate::types::{MicrovmId, TenantKey};

/// A durable tenant→MicroVM affinity record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Binding {
    /// Owning tenant.
    pub tenant: TenantKey,
    /// Bound MicroVM.
    pub microvm_id: MicrovmId,
    /// When the binding was claimed (epoch seconds).
    pub claimed_at_secs: i64,
    /// Internal lost-VM marker — a durable record the pool's reaper
    /// owns, not a tenant binding. Stored as an explicit flag (not
    /// inferred from the tenant string) so a tenant name can never be
    /// mistaken for — or forged into — a marker.
    ///
    /// A custom [`StateStore`] must persist and return this flag with
    /// the row, and compare it in `claim` (see [`ClaimOutcome`]); a
    /// store that drops it turns markers into tenant bindings, and
    /// lost VMs are no longer reaped after a restart.
    pub sentinel: bool,
}

/// Result of an atomic put-if-absent claim.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// The caller now holds the binding: either the INSERT landed, or
    /// the stored row is identical to what was claimed (same VM and
    /// same `sentinel` kind).
    Claimed,
    /// A different row already holds this tenant — a different VM, or
    /// a different `sentinel` kind for the same tenant key (a sentinel
    /// claim landing on a normal binding did NOT pin a marker).
    /// Carries the existing binding so the caller can use it without a
    /// second round trip.
    HeldByOther(Binding),
}

/// Atomic store for tenant bindings.
///
/// `claim` is the only insert and never overwrites: put-if-absent keeps
/// concurrent acquires of the same tenant from splitting onto two VMs.
/// `release` is the only delete, scoped to the expected VM.
#[async_trait]
pub trait StateStore: Send + Sync {
    /// Current binding for `tenant`, if any.
    async fn get(&self, tenant: &TenantKey) -> Result<Option<Binding>>;
    /// Atomically binds `tenant` to `microvm_id` unless a binding already
    /// exists. Never overwrites.
    async fn claim(&self, binding: &Binding) -> Result<ClaimOutcome>;
    /// Removes the binding for `tenant`, but only if it still points to
    /// `expected`. The scoped delete is load-bearing: without it, a stale
    /// reader that raced a fresh re-bind would delete a binding it never
    /// owned. `false` means either no binding or a different VM holds it.
    async fn release(&self, tenant: &TenantKey, expected: &MicrovmId) -> Result<bool>;
    /// All bindings — reapers sweep through this.
    async fn list(&self) -> Result<Vec<Binding>>;
}

/// In-memory [`StateStore`] for single-process use and tests.
#[derive(Default)]
pub struct MemoryStore {
    map: Mutex<HashMap<String, Binding>>,
}

impl MemoryStore {
    /// Empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StateStore for MemoryStore {
    async fn get(&self, tenant: &TenantKey) -> Result<Option<Binding>> {
        Ok(self.map.lock().get(tenant.as_str()).cloned())
    }

    async fn claim(&self, binding: &Binding) -> Result<ClaimOutcome> {
        let mut map = self.map.lock();
        if let Some(existing) = map.get(binding.tenant.as_str()) {
            // Same contract as `SqliteStore`: `Claimed` only when the
            // stored row is what we tried to write — a sentinel claim
            // landing on a normal row (different `kind`) did NOT pin
            // a marker and must surface `HeldByOther` instead.
            if existing.microvm_id == binding.microvm_id && existing.sentinel == binding.sentinel {
                return Ok(ClaimOutcome::Claimed);
            }
            return Ok(ClaimOutcome::HeldByOther(existing.clone()));
        }
        map.insert(binding.tenant.as_str().to_owned(), binding.clone());
        Ok(ClaimOutcome::Claimed)
    }

    async fn release(&self, tenant: &TenantKey, expected: &MicrovmId) -> Result<bool> {
        let mut map = self.map.lock();
        if map
            .get(tenant.as_str())
            .is_some_and(|b| b.microvm_id == *expected)
        {
            map.remove(tenant.as_str());
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn list(&self) -> Result<Vec<Binding>> {
        Ok(self.map.lock().values().cloned().collect())
    }
}

/// SQLite-backed [`StateStore`] (feature `sqlite`, on by default).
///
/// One table (`tenant PRIMARY KEY`) gives `claim` its atomicity through
/// `INSERT OR IGNORE` — the same connection serializes writers, and on
/// disk the uniqueness constraint holds across processes.
#[cfg(feature = "sqlite")]
pub struct SqliteStore {
    conn: tokio_rusqlite::Connection,
}

#[cfg(feature = "sqlite")]
impl SqliteStore {
    /// Opens (or creates) a store at `path`. `":memory:"` works for tests
    /// — the single shared connection keeps the database alive.
    pub async fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let conn = tokio_rusqlite::Connection::open(path.as_ref())
            .await
            .map_err(store_err)?;
        conn.call(|c| {
            c.execute_batch(
                "PRAGMA busy_timeout = 5000;
                 CREATE TABLE IF NOT EXISTS bindings (
                    tenant      TEXT PRIMARY KEY,
                    microvm_id  TEXT NOT NULL,
                    claimed_at  INTEGER NOT NULL
                );",
            )?;
            // Migrate pre-`kind` databases in place — the column
            // defaults to a normal binding, so no historical row can
            // ever be mistaken for a sentinel marker. Markers written
            // by older versions (a `{prefix}{vm}` tenant, kind=0)
            // degrade to ordinary bindings pointing at their VM:
            // they self-heal once the VM hits its max duration, and
            // never cause a destroy the VM doesn't deserve.
            let has_kind = c
                .prepare("PRAGMA table_info(bindings)")?
                .query_map([], |r| r.get::<_, String>(1))?
                .filter_map(std::result::Result::ok)
                .any(|n| n == "kind");
            if !has_kind {
                // Two openers can pass the column check before either
                // ALTER commits — a duplicate-column error means the
                // migration already landed, not a failure.
                match c.execute_batch(
                    "ALTER TABLE bindings ADD COLUMN kind INTEGER NOT NULL DEFAULT 0",
                ) {
                    Ok(()) => {}
                    Err(e) if e.to_string().contains("duplicate column") => {}
                    Err(e) => return Err(e),
                }
            }
            Ok::<(), tokio_rusqlite::rusqlite::Error>(())
        })
        .await
        .map_err(store_err)?;
        Ok(Self { conn })
    }
}

#[cfg(feature = "sqlite")]
fn store_err(e: impl std::fmt::Display) -> crate::error::Error {
    crate::error::Error::Store(e.to_string())
}

#[cfg(feature = "sqlite")]
fn row_to_binding(row: &rusqlite::Row<'_>) -> rusqlite::Result<Binding> {
    Ok(Binding {
        tenant: TenantKey(row.get(0)?),
        microvm_id: MicrovmId::new(row.get::<_, String>(1)?)
            .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))?,
        claimed_at_secs: row.get(2)?,
        sentinel: row.get::<_, i64>(3)? != 0,
    })
}

#[cfg(feature = "sqlite")]
#[async_trait]
impl StateStore for SqliteStore {
    async fn get(&self, tenant: &TenantKey) -> Result<Option<Binding>> {
        let key = tenant.as_str().to_owned();
        self.conn
            .call(move |c| {
                c.query_row(
                    "SELECT tenant, microvm_id, claimed_at, kind FROM bindings WHERE tenant = ?1",
                    rusqlite::params![key],
                    row_to_binding,
                )
                .optional()
            })
            .await
            .map_err(store_err)
    }

    async fn claim(&self, binding: &Binding) -> Result<ClaimOutcome> {
        let b = binding.clone();
        self.conn
            .call(move |c| {
                // Transaction wraps insert+select so a cross-connection
                // delete cannot slip between them on shared files.
                let tx = c.transaction()?;
                tx.execute(
                    "INSERT OR IGNORE INTO bindings(tenant, microvm_id, claimed_at, kind)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![
                        b.tenant.as_str(),
                        b.microvm_id.as_str(),
                        b.claimed_at_secs,
                        i64::from(b.sentinel),
                    ],
                )?;
                let existing: Binding = tx.query_row(
                    "SELECT tenant, microvm_id, claimed_at, kind FROM bindings WHERE tenant = ?1",
                    rusqlite::params![b.tenant.as_str()],
                    row_to_binding,
                )?;
                tx.commit()?;
                // `INSERT OR IGNORE` + PRIMARY KEY decide the winner:
                // whoever's row is stored held the claim. `kind` must
                // match too — a sentinel claim that lands on a normal
                // row (an exact `{prefix}{vm}` legacy tenant) did NOT
                // pin a marker; reporting `Claimed` would let the
                // caller clear that normal binding as if it were a
                // marker.
                let outcome =
                    if existing.microvm_id == b.microvm_id && existing.sentinel == b.sentinel {
                        ClaimOutcome::Claimed
                    } else {
                        ClaimOutcome::HeldByOther(existing)
                    };
                Ok::<_, rusqlite::Error>(outcome)
            })
            .await
            .map_err(store_err)
    }

    async fn release(&self, tenant: &TenantKey, expected: &MicrovmId) -> Result<bool> {
        let key = tenant.as_str().to_owned();
        let vm = expected.as_str().to_owned();
        self.conn
            .call(move |c| {
                c.execute(
                    "DELETE FROM bindings WHERE tenant = ?1 AND microvm_id = ?2",
                    rusqlite::params![key, vm],
                )
            })
            .await
            .map(|n| n > 0)
            .map_err(store_err)
    }

    async fn list(&self) -> Result<Vec<Binding>> {
        self.conn
            .call(|c| {
                let mut stmt =
                    c.prepare("SELECT tenant, microvm_id, claimed_at, kind FROM bindings")?;
                stmt.query_map([], row_to_binding)?
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .await
            .map_err(store_err)
    }
}
