//! Global authorization settings behind the management API.
//!
//! Covers the state behind `GET /authorization/settings` (read) and
//! `PUT /authorization/settings` (validated replace with documented
//! defaults filled) over the real [`broker_config::AuthzSettingsConf`].
//! Single-node, management-plane only: handlers clone one small snapshot
//! per request; the kernel boot path seeds the same instance from the
//! config registry (`crates/broker-node/src/main.rs`, boot seeding, and
//! `crates/broker-api/src/lib.rs`, `ApiState::new`) so management reads
//! observe the persisted settings from the first connection. The
//! authorize path reads the `no_match`/`deny_action` values here
//! (`MemoryAuth` in `crate::lib`, one lock-free snapshot load per
//! unmatched or denied message, no clone). The `cache` half is enforced
//! on the publish, subscribe and will paths in
//! `crates/broker-node/src/main.rs` (enable, cap, TTL, excludes).
//! The `include_mountpoint` and `ignore_backend_failures` halves are
//! stored, validated and reported only.
//!
//! Store bounds (both stated here and enforced below):
//! - exactly one small validated struct (two decision strings, one cache
//!   half, two flags); reads clone it per request and never grow with
//!   connections, sessions or subscriptions;
//! - the decision-cache entry cap inside it stays finite
//!   (1..=[`broker_config::MAX_AUTHZ_SETTINGS_CACHE_MAX_SIZE`]); `0`
//!   (unlimited) is rejected instead of growing without limit;
//! - the topic-pattern exclusion list stays bounded (at most
//!   [`broker_config::MAX_AUTHZ_SETTINGS_EXCLUDES`] entries, default
//!   empty);
//! - writes go validate-all-before-apply through the config registry
//!   (commit then atomic save); a failed validation or persist applies
//!   nothing new.
//!
//! Decision evaluation reads the `no_match`/`deny_action` values on the
//! authorize path. The bench hook is
//! `crates/broker-node/tests/authz_decision_bench.rs`.
// TODO(parity): does the decision cache (`cache` half) share the
// node-cache entries or stay its own verdict table? The rulebook does
// not decide the cache wiring; the current choice keeps its own
// per-client verdict table.
// TODO(parity): does the documented PUT merge missing fields or reset
// them to defaults, and does an empty body reset everything or fail? The
// rulebook does not decide the merge semantics; the current choice fills
// missing fields with the documented defaults (full replace, mirroring
// the neighbouring settings routes) and rejects an empty body
// fail-closed, until the checker pins it.

use broker_config::{AuthzSettingsConf, ConfigRegistry};
use std::sync::Arc;

/// Why [`AuthzSettingsStore::replace`] refused an update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthzSettingsUpdateError {
    /// The candidate failed validation (message names the field).
    Invalid(String),
    /// The registry commit or atomic save failed after validation.
    Persist(String),
}

/// Global authorization settings: one validated struct with a lock-free
/// read snapshot plus registry persistence.
///
/// Management writes validate the candidate before swapping it into view;
/// readers load the snapshot without taking a write lock. The authorize
/// path never clones the snapshot; it reads the two bools below.
pub struct AuthzSettingsStore {
    current: arc_swap::ArcSwap<AuthzSettingsConf>,
    registry: parking_lot::RwLock<Option<Arc<ConfigRegistry>>>,
    save_lock: parking_lot::Mutex<()>,
}

impl AuthzSettingsStore {
    /// Default settings with no persistence hook.
    pub fn new() -> Self {
        Self {
            current: arc_swap::ArcSwap::from(Arc::new(AuthzSettingsConf::default())),
            registry: parking_lot::RwLock::new(None),
            save_lock: parking_lot::Mutex::new(()),
        }
    }

    /// Seed from a validated config root. Memory-only.
    pub fn from_conf(conf: &AuthzSettingsConf) -> Self {
        let store = Self::new();
        store.current.store(Arc::new(conf.clone()));
        store
    }

    /// Seed from the registry's current snapshot and persist every later
    /// mutation back through it. Kernel boot path; defaults yield today's
    /// behaviour (unmatched denied, denied ignored, backend outages fail
    /// closed).
    pub fn from_registry(registry: &Arc<ConfigRegistry>) -> Self {
        let store = Self::from_conf(&registry.snapshot().authz_settings);
        *store.registry.write() = Some(Arc::clone(registry));
        store
    }

    /// Attach the registry and replace the current contents with its
    /// snapshot, in place on the same instance.
    pub fn seed_from_registry(&self, registry: &Arc<ConfigRegistry>) {
        self.current
            .store(Arc::new(registry.snapshot().authz_settings.clone()));
        *self.registry.write() = Some(Arc::clone(registry));
    }

    /// Lock-free snapshot for management readers: one `Arc` load, no
    /// write lock. The authorize path never calls this; it reads the two
    /// bools below with no clone.
    pub fn snapshot(&self) -> Arc<AuthzSettingsConf> {
        self.current.load_full()
    }

    /// Snapshot clone of the current settings.
    pub fn get(&self) -> AuthzSettingsConf {
        (*self.current.load_full()).clone()
    }

    /// Decision when no rule matches (`allow` or `deny`). The authorize
    /// path reads this through [`AuthzSettingsStore::no_match_is_allow`];
    /// management reads use this clone.
    pub fn no_match(&self) -> String {
        self.current.load().no_match.clone()
    }

    /// Action on an explicit deny (`ignore` or `disconnect`). The publish
    /// and subscribe paths read this through
    /// [`AuthzSettingsStore::deny_action_is_disconnect`]; management
    /// reads use this clone.
    pub fn deny_action(&self) -> String {
        self.current.load().deny_action.clone()
    }

    /// True when no-match allows. One lock-free snapshot load with no
    /// clone and no reference-count write, so the authorize path calls
    /// it per unmatched message. Unlinked stores deny.
    pub fn no_match_is_allow(&self) -> bool {
        self.current.load().no_match == "allow"
    }

    /// True when a deny disconnects. One lock-free snapshot load with no
    /// clone and no reference-count write, so the publish and subscribe
    /// paths call it per denied message.
    pub fn deny_action_is_disconnect(&self) -> bool {
        self.current.load().deny_action == "disconnect"
    }

    /// Whether a backend outage is ignored (fail-open) or denies access
    /// (fail-closed, the default).
    pub fn ignore_backend_failures(&self) -> bool {
        self.current.load().ignore_backend_failures
    }

    /// Replace the stored settings with a candidate.
    ///
    /// Validate the candidate before it becomes visible.
    /// Commit and save before the in-memory swap.
    /// A failed write keeps the old decision (D2).
    /// Management-plane only.
    /// Publish and deliver never touch this store.
    pub fn replace(
        &self,
        next: AuthzSettingsConf,
    ) -> Result<AuthzSettingsConf, AuthzSettingsUpdateError> {
        next.validate()
            .map_err(|e| AuthzSettingsUpdateError::Invalid(e.to_string()))?;
        let _guard = self.save_lock.lock();
        if let Some(registry) = self.registry.read().clone() {
            let previous = registry.snapshot().authz_settings.clone();
            registry
                .commit_authz_settings(next.clone())
                .map_err(|e| AuthzSettingsUpdateError::Persist(e.to_string()))?;
            if let Err(error) = registry.save() {
                // A failed save must not leave the registry changed.
                // Restore the old snapshot so a later save keeps the old
                // decision (D2).
                if let Err(restore) = registry.commit_authz_settings(previous) {
                    tracing::error!(
                        error = %restore,
                        "cannot restore authorization settings after a failed save"
                    );
                }
                return Err(AuthzSettingsUpdateError::Persist(error.to_string()));
            }
        }
        self.current.store(Arc::new(next.clone()));
        Ok(next)
    }

    /// Number of bytes in the largest accepted settings document (8 KiB).
    /// Reason: settings ride the per-request snapshot clone, so one write
    /// cannot balloon memory; larger documents are rejected instead of
    /// growing without limit.
    pub fn max_body_len() -> usize {
        8192
    }
}

impl Default for AuthzSettingsStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_documented_struct() {
        let store = AuthzSettingsStore::new();
        let got = store.get();
        assert_eq!(got.no_match, "deny");
        assert_eq!(got.deny_action, "ignore");
        assert!(got.cache.enable);
        assert_eq!(
            got.cache.max_size,
            broker_config::DEFAULT_AUTHZ_SETTINGS_CACHE_MAX_SIZE
        );
        assert_eq!(got.cache.ttl, "1m");
        assert!(got.cache.excludes.is_empty());
        assert!(!got.include_mountpoint);
        assert!(!got.ignore_backend_failures);
        assert_eq!(store.no_match(), "deny");
        assert_eq!(store.deny_action(), "ignore");
        assert!(!store.ignore_backend_failures());
        got.validate().expect("documented defaults validate");
    }

    #[test]
    fn invalid_candidate_is_rejected_without_applying() {
        let store = AuthzSettingsStore::new();
        let before = store.get();
        for bad in [
            {
                let mut c = before.clone();
                c.no_match = "sometimes".to_string();
                c
            },
            {
                let mut c = before.clone();
                c.deny_action = "log".to_string();
                c
            },
            {
                let mut c = before.clone();
                c.cache.max_size = 0;
                c
            },
            {
                let mut c = before.clone();
                c.cache.ttl = "soon".to_string();
                c
            },
            {
                let mut c = before.clone();
                c.cache.excludes = vec![String::new()];
                c
            },
        ] {
            assert!(matches!(
                store.replace(bad),
                Err(AuthzSettingsUpdateError::Invalid(_))
            ));
            assert_eq!(store.get(), before);
        }
    }

    #[test]
    fn replace_moves_snapshot_and_reports_decisions() {
        let store = AuthzSettingsStore::new();
        let mut next = store.get();
        next.no_match = "allow".to_string();
        next.deny_action = "disconnect".to_string();
        next.ignore_backend_failures = true;
        next.cache.max_size = 64;
        next.cache.excludes = vec!["nocache/#".to_string()];
        store.replace(next.clone()).expect("valid replace");
        assert_eq!(store.get(), next);
        assert_eq!(store.no_match(), "allow");
        assert_eq!(store.deny_action(), "disconnect");
        assert!(store.ignore_backend_failures());
    }

    #[test]
    fn settings_survive_registry_reload() {
        let dir = std::env::temp_dir().join(format!(
            "authz-settings-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads"));
        let store = AuthzSettingsStore::from_registry(&registry);
        let mut next = store.get();
        next.no_match = "allow".to_string();
        next.cache.max_size = 64;
        store.replace(next.clone()).expect("persist settings");
        assert!(dir.join(broker_config::STATE_FILE_NAME).is_file());
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("data dir reloads"));
        let restarted = AuthzSettingsStore::from_registry(&reloaded);
        assert_eq!(restarted.get(), next);
        std::fs::remove_dir_all(&dir).ok();
    }
}
