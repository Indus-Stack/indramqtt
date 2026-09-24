//! Node-level authorization cache configuration behind the management API.
//!
//! Covers the state behind `GET /authorization/node_cache` (read),
//! `PUT /authorization/node_cache` (validated full replace) and
//! `POST /authorization/node_cache/reset` (evict) over the real
//! [`broker_config::AuthzNodeCacheConf`]. Single-node, management-plane
//! only: handlers clone one small snapshot per request under no delivery
//! lock; the kernel boot path seeds the same instance from the config
//! registry (`crates/broker-node/src/main.rs`, boot seeding) so
//! management reads observe the persisted policy from the first
//! connection; publish and deliver never touch this store.
//!
//! Store bounds (both stated here and enforced below):
//! - exactly one small validated struct (flag, TTL cadences, entry and
//!   memory caps); reads clone it per request and never grow with
//!   connections, sessions or subscriptions;
//! - the entry cap stays finite (1..=[`broker_config::MAX_AUTHZ_NODE_CACHE_MAX`]);
//!   `0` (unlimited) is rejected instead of growing without limit;
//! - writes go validate-all-before-apply through the config registry
//!   (commit then atomic save); a failed validation or persist applies
//!   nothing new.
//!
//! Cached verdicts themselves live in the per-session decision caches
//! (`crates/broker-session/src/lib.rs`, `Session::record_authz_decision`,
//! driven by the publish and subscribe authorization events in
//! `crates/broker-node/src/main.rs`); the node reset evicts every entry
//! on every session on this node so subsequent decisions are recomputed.
//! Verdict contents are memory-only and do not survive a kernel restart
//! (sessions are rebuilt empty); only this configuration persists.
// TODO(parity): does the documented config accept an explicit
// `unlimited` entry cap, and where does the node cap/TTL get enforced on
// the authorize path? The rulebook does not decide the cap opt-in, and
// enforcement on publish/subscribe would need pipeline BENCH numbers the
// spec carries no BENCH line for; the current choice rejects `unlimited`
// caps as the conservative bounded answer while the per-client cap
// (`MAX_AUTHZ_DECISIONS_PER_CLIENT`) keeps entries bounded and expiry
// stays lazy (TTL bounds staleness once the authorize path consults it).

use broker_config::{AuthzNodeCacheConf, ConfigRegistry};
use std::sync::Arc;

/// Why [`AuthzNodeCache::replace`] refused an update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthzCacheUpdateError {
    /// The candidate failed validation (message names the field).
    Invalid(String),
    /// The registry commit or atomic save failed after validation.
    Persist(String),
}

/// Node authorization cache policy: one validated struct with a lock-free
/// read snapshot plus registry persistence.
///
/// Management writes validate the candidate before swapping it into view;
/// readers load the snapshot without taking a write lock. Publish and
/// deliver never touch either.
pub struct AuthzNodeCache {
    current: arc_swap::ArcSwap<AuthzNodeCacheConf>,
    registry: parking_lot::RwLock<Option<Arc<ConfigRegistry>>>,
    save_lock: parking_lot::Mutex<()>,
}

impl AuthzNodeCache {
    /// Default policy with no persistence hook.
    pub fn new() -> Self {
        Self {
            current: arc_swap::ArcSwap::from(Arc::new(AuthzNodeCacheConf::default())),
            registry: parking_lot::RwLock::new(None),
            save_lock: parking_lot::Mutex::new(()),
        }
    }

    /// Seed from a validated config root. Memory-only.
    pub fn from_conf(conf: &AuthzNodeCacheConf) -> Self {
        let store = Self::new();
        store.current.store(Arc::new(conf.clone()));
        store
    }

    /// Seed from the registry's current snapshot and persist every later
    /// mutation back through it. Kernel boot path; defaults yield today's
    /// behaviour (caching enabled, 50 000 entries, 60 s TTL).
    pub fn from_registry(registry: &Arc<ConfigRegistry>) -> Self {
        let store = Self::from_conf(&registry.snapshot().authz_node_cache);
        *store.registry.write() = Some(Arc::clone(registry));
        store
    }

    /// Attach the registry and replace the current contents with its
    /// snapshot, in place on the same instance.
    pub fn seed_from_registry(&self, registry: &Arc<ConfigRegistry>) {
        self.current
            .store(Arc::new(registry.snapshot().authz_node_cache.clone()));
        *self.registry.write() = Some(Arc::clone(registry));
    }

    /// Lock-free snapshot for readers: one `Arc` load, no write lock.
    /// Publish and deliver never call this.
    pub fn snapshot(&self) -> Arc<AuthzNodeCacheConf> {
        self.current.load_full()
    }

    /// Snapshot clone of the current policy.
    pub fn get(&self) -> AuthzNodeCacheConf {
        (*self.current.load_full()).clone()
    }

    /// Whether verdicts are cached on this node. Consulted on the
    /// management plane only (the authorize path keeps its current
    /// behaviour until a BENCH'd hot-path task wires the consult).
    pub fn is_enabled(&self) -> bool {
        self.current.load().enable
    }

    /// Current entry cap.
    pub fn max_count(&self) -> usize {
        self.current.load().max_count
    }

    /// Replace the stored policy with a full-replacement candidate.
    ///
    /// Validate-all-before-apply: the candidate is validated before it
    /// can become visible, so invalid state is never observable. On
    /// success the snapshot, the persisted registry and any future
    /// consults all move together; the in-memory swap precedes the
    /// atomic save (mirroring the sibling settings stores), so a save
    /// failure surfaces as [`AuthzCacheUpdateError::Persist`] while the
    /// validated value stays. Management-plane only: one short swap per
    /// request; publish and deliver never touch this store.
    pub fn replace(
        &self,
        next: AuthzNodeCacheConf,
    ) -> Result<AuthzNodeCacheConf, AuthzCacheUpdateError> {
        next.validate()
            .map_err(|e| AuthzCacheUpdateError::Invalid(e.to_string()))?;
        let _guard = self.save_lock.lock();
        self.current.store(Arc::new(next.clone()));
        if let Some(registry) = self.registry.read().clone() {
            registry
                .commit_authz_node_cache(next.clone())
                .map_err(|e| AuthzCacheUpdateError::Persist(e.to_string()))?;
            registry
                .save()
                .map_err(|e| AuthzCacheUpdateError::Persist(e.to_string()))?;
        }
        Ok(next)
    }

    /// Default entry cap stated next to the bound above.
    pub fn default_max() -> usize {
        broker_config::DEFAULT_AUTHZ_NODE_CACHE_MAX
    }

    /// Number of bytes in the largest accepted config document (8 KiB).
    /// Reason: the policy rides a per-request snapshot clone, so one
    /// write cannot balloon memory; larger documents are rejected
    /// instead of growing without limit.
    pub fn max_body_len() -> usize {
        8192
    }
}

impl Default for AuthzNodeCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_documented_policy() {
        let store = AuthzNodeCache::new();
        let got = store.get();
        assert!(got.enable);
        assert_eq!(got.cache_ttl, "60s");
        assert_eq!(got.cleanup_interval, "60s");
        assert_eq!(got.stat_update_interval, "5s");
        assert_eq!(got.max_count, broker_config::DEFAULT_AUTHZ_NODE_CACHE_MAX);
        assert_eq!(got.max_count, 50_000);
        assert_eq!(got.max_memory, "100MB");
        assert!(store.is_enabled());
        assert_eq!(store.max_count(), 50_000);
        got.validate().expect("documented defaults validate");
    }

    #[test]
    fn invalid_candidate_is_rejected_without_applying() {
        let store = AuthzNodeCache::new();
        let before = store.get();
        let mut bad = before.clone();
        bad.max_count = 0;
        assert!(matches!(
            store.replace(bad),
            Err(AuthzCacheUpdateError::Invalid(_))
        ));
        assert_eq!(store.get(), before);
        let mut bad_ttl = before.clone();
        bad_ttl.cache_ttl = "soon".to_string();
        assert!(matches!(
            store.replace(bad_ttl),
            Err(AuthzCacheUpdateError::Invalid(_))
        ));
        assert_eq!(store.get(), before);
        let mut bad_cap = before.clone();
        bad_cap.max_count = broker_config::MAX_AUTHZ_NODE_CACHE_MAX + 1;
        assert!(matches!(
            store.replace(bad_cap),
            Err(AuthzCacheUpdateError::Invalid(_))
        ));
        assert_eq!(store.get(), before);
    }

    #[test]
    fn policy_survives_registry_reload_while_entries_do_not() {
        let dir = std::env::temp_dir().join(format!(
            "authz-node-cache-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads"));
        let store = AuthzNodeCache::from_registry(&registry);
        let mut next = store.get();
        next.max_count = 5000;
        next.cache_ttl = "30s".to_string();
        store.replace(next.clone()).expect("persist policy");
        assert!(dir.join(broker_config::STATE_FILE_NAME).is_file());
        // Rebuild from the same data dir (simulating a kernel restart):
        // the policy persists; verdict entries (per-session, memory-only)
        // are rebuilt empty elsewhere.
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("data dir reloads"));
        let restarted = AuthzNodeCache::from_registry(&reloaded);
        assert_eq!(restarted.get(), next);
        std::fs::remove_dir_all(&dir).ok();
    }
}
