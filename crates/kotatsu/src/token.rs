//! Auth-token minting, caching and rotation.
//!
//! `X-aws-proxy-auth` tokens are scoped to one MicroVM, a set of allowed
//! ports, and a TTL capped at 60 minutes. `TokenVending` owns the cache so
//! callers (endpoint clients, the gateway, the pool) never mint a fresh
//! JWE per request and never hold an expired one.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::control_plane::ControlPlane;
use crate::error::Result;
use crate::types::{AuthToken, MicrovmId, PortSpec, TokenKind};

/// Configuration for [`TokenVending`].
#[derive(Clone, Debug)]
pub struct TokenVendingConfig {
    /// TTL requested per token; the service clamps it at 60 minutes.
    pub ttl_minutes: i32,
    /// Tokens with less than this much life left are re-minted eagerly,
    /// so an in-flight request never carries an about-to-expire JWE.
    pub refresh_margin: Duration,
}

impl Default for TokenVendingConfig {
    fn default() -> Self {
        Self {
            ttl_minutes: 30,
            refresh_margin: Duration::from_secs(60),
        }
    }
}

/// Mints and caches MicroVM auth tokens.
///
/// Caching policy: a mint outside the lock inserts last-writer-wins, so a
/// burst of concurrent misses may mint a few redundant tokens — harmless,
/// since tokens are independent JWEs and AWS does not document a
/// per-MicroVM token quota. Correctness (never serve an expired token)
/// takes precedence over singleflight guarantees.
#[derive(Clone)]
pub struct TokenVending {
    cp: Arc<dyn ControlPlane>,
    cfg: TokenVendingConfig,
    cache: Arc<Mutex<HashMap<CacheKey, AuthToken>>>,
}

/// Structured cache key — avoids string-prefix matching on caller-controlled
/// ids.
#[derive(PartialEq, Eq, Hash)]
struct CacheKey {
    id: MicrovmId,
    kind: TokenKind,
    /// Scope specs rendered and sorted for a canonical representation.
    scope: String,
}

fn cache_key(id: &MicrovmId, kind: TokenKind, scope: &[PortSpec]) -> CacheKey {
    let mut parts: Vec<String> = scope.iter().map(ToString::to_string).collect();
    parts.sort();
    CacheKey {
        id: id.clone(),
        kind,
        scope: parts.join(","),
    }
}

impl TokenVending {
    /// Wraps a control plane with default configuration.
    pub fn new(cp: Arc<dyn ControlPlane>) -> Self {
        // Default config is valid by construction.
        Self::with_config(cp, TokenVendingConfig::default())
            .expect("default TokenVendingConfig is valid")
    }

    /// Wraps a control plane with explicit configuration.
    ///
    /// Fails when `refresh_margin` is not smaller than the requested TTL:
    /// every minted token would be born inside the margin, turning the
    /// cache into a mint-per-call passthrough.
    pub fn with_config(cp: Arc<dyn ControlPlane>, cfg: TokenVendingConfig) -> Result<Self> {
        if cfg.ttl_minutes <= 0 {
            return Err(crate::error::Error::invalid(
                "TokenVendingConfig: ttl_minutes must be positive",
            ));
        }
        // The service clamps the effective TTL at 60 minutes, so the
        // margin must be compared against the *effective* TTL — a 120min
        // request mints a 60min token.
        let ttl =
            Duration::from_secs(cfg.ttl_minutes.min(crate::MAX_TOKEN_TTL_MINUTES) as u64 * 60);
        if cfg.refresh_margin >= ttl {
            return Err(crate::error::Error::invalid(format!(
                "TokenVendingConfig: refresh_margin {:?} must be < ttl {ttl:?}",
                cfg.refresh_margin
            )));
        }
        Ok(Self {
            cp,
            cfg,
            cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Returns a fresh token covering `scope` for `id`.
    ///
    /// Cached hits are reused until `refresh_margin` before expiry.
    /// `scope` must be non-empty; the smallest practical scope is
    /// recommended because each distinct scope keeps its own cache entry.
    pub async fn token(&self, id: &MicrovmId, scope: &[PortSpec]) -> Result<AuthToken> {
        if scope.is_empty() {
            return Err(crate::error::Error::invalid(
                "token scope must not be empty",
            ));
        }
        let key = cache_key(id, TokenKind::Port, scope);
        let cached = self
            .cache
            .lock()
            .get(&key)
            .filter(|t| !t.nearly_expired(self.cfg.refresh_margin))
            .cloned();
        if let Some(t) = cached {
            crate::metrics::record_token(true);
            return Ok(t);
        }
        crate::metrics::record_token(false);
        let token = self.cp.mint_token(id, scope, self.cfg.ttl_minutes).await?;
        self.cache.lock().insert(key, token.clone());
        Ok(token)
    }

    /// Returns a fresh shell token for `id`.
    ///
    /// Shell tokens are not port-scoped and are cached separately from
    /// data-plane tokens.
    pub async fn shell_token(&self, id: &MicrovmId) -> Result<AuthToken> {
        let key = cache_key(id, TokenKind::Shell, &[]);
        let cached = self
            .cache
            .lock()
            .get(&key)
            .filter(|t| !t.nearly_expired(self.cfg.refresh_margin))
            .cloned();
        if let Some(t) = cached {
            crate::metrics::record_token(true);
            return Ok(t);
        }
        crate::metrics::record_token(false);
        let token = self.cp.mint_shell_token(id, self.cfg.ttl_minutes).await?;
        self.cache.lock().insert(key, token.clone());
        Ok(token)
    }

    /// Drops every cached token for `id` (all kinds and scopes).
    ///
    /// Call when the MicroVM is terminated. Re-warming an unbound VM
    /// does *not* invalidate: tokens are VM-scoped, so re-vending a
    /// cached token to whichever tenant claims that VM is still safe.
    pub fn invalidate(&self, id: &MicrovmId) {
        self.cache.lock().retain(|k, _| &k.id != id);
    }
}
