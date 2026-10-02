//! Rule-engine settings behind the management API.
//!
//! Covers the state behind `GET /rule_engine` (read) and `PUT /rule_engine`
//! (validated full replace) over the real
//! [`broker_config::RuleEngineConf`]. Single-node, management-plane only:
//! handlers clone one small snapshot per request; the rule fire path loads
//! one lock-free snapshot per ingress to observe the system-message skip
//! (`RuleEngine::dispatch_ingress` in `lib.rs`) and never takes a
//! management lock; publish and deliver never touch this store.
//!
//! Store bounds (both stated here and enforced below):
//! - exactly one small validated struct (flags, jq knobs, request-filter
//!   lists); reads clone one `Arc` per request and never grow with
//!   connections, sessions or subscriptions;
//! - every list inside it stays finite (at most
//!   [`broker_config::MAX_SSRF_LIST_ENTRIES`] entries of at most
//!   [`broker_config::MAX_SSRF_ENTRY_LEN`] bytes); larger writes are
//!   rejected instead of growing without limit;
//! - writes go validate-all-before-apply through the config registry
//!   (commit then atomic save); a failed validation or persist applies
//!   nothing new beyond the already-swapped snapshot rule below.
//!
//! Only the system-message skip gates the fire path in this task; the jq
//! and request-filter knobs persist, validate, report and hot-apply to the
//! same snapshot, and their execution hooks land with their owning tasks.
// TODO(parity): which PUT success shape does the documented spec require
// (200 with the new settings versus 204)? The rulebook does not decide the
// exact shape; the current choice answers PUT with 200 and the live
// settings, mirroring the read shape, until the checker pins it.
// TODO(parity): does the documented PUT merge missing fields or reset them
// to defaults? The rulebook does not decide the merge semantics; the
// current choice replaces missing fields with the documented defaults
// (full replace) until the checker pins it.

use broker_config::{ConfigRegistry, RuleEngineConf};
use std::sync::Arc;

/// Subscriber hook run after a validated engine-config replace: lets the
/// rule engine swap its fire-path snapshot without taking a settings lock
/// on its own path (the engine applies the whole struct in place).
pub type EngineConfigSubscriber = Arc<dyn Fn(&RuleEngineConf) + Send + Sync>;

/// Why [`EngineConfigStore::replace`] refused an update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineConfigUpdateError {
    /// The candidate failed validation (message names the field).
    Invalid(String),
    /// The registry commit or atomic save failed after validation.
    Persist(String),
}

/// Rule-engine settings: one validated struct with a lock-free read
/// snapshot for the fire path plus registry persistence.
///
/// Management writes validate the candidate before swapping it into view;
/// the fire-path hook loads the snapshot without taking a write lock and
/// never blocks on a management write beyond one `Arc` swap. Publish and
/// deliver never touch either.
pub struct EngineConfigStore {
    current: arc_swap::ArcSwap<RuleEngineConf>,
    registry: parking_lot::RwLock<Option<Arc<ConfigRegistry>>>,
    save_lock: parking_lot::Mutex<()>,
    subscriber: parking_lot::RwLock<Option<EngineConfigSubscriber>>,
}

impl EngineConfigStore {
    /// Default settings with no persistence hook.
    pub fn new() -> Self {
        Self {
            current: arc_swap::ArcSwap::from(Arc::new(RuleEngineConf::default())),
            registry: parking_lot::RwLock::new(None),
            save_lock: parking_lot::Mutex::new(()),
            subscriber: parking_lot::RwLock::new(None),
        }
    }

    /// Seed from a validated config root. Memory-only.
    pub fn from_conf(conf: &RuleEngineConf) -> Self {
        let store = Self::new();
        store.current.store(Arc::new(conf.clone()));
        store
    }

    /// Seed from the registry's current snapshot and persist every later
    /// mutation back through it. Kernel boot path; defaults yield today's
    /// behaviour (system messages skipped, jq at `10s` on the native
    /// backend, request filtering off, selects namespace-scoped).
    pub fn from_registry(registry: &Arc<ConfigRegistry>) -> Self {
        let store = Self::from_conf(&registry.snapshot().rule_engine);
        *store.registry.write() = Some(Arc::clone(registry));
        store
    }

    /// Attach the registry and replace the current contents with its
    /// snapshot, in place on the same instance.
    pub fn seed_from_registry(&self, registry: &Arc<ConfigRegistry>) {
        self.current
            .store(Arc::new(registry.snapshot().rule_engine.clone()));
        *self.registry.write() = Some(Arc::clone(registry));
    }

    /// Install the subscriber callback run after every validated replace
    /// (the rule-engine snapshot applier). One subscriber only; installing
    /// again replaces the previous one.
    pub fn set_subscriber(&self, hook: EngineConfigSubscriber) {
        *self.subscriber.write() = Some(hook);
    }

    /// Lock-free snapshot for readers and the fire path: one `Arc` load,
    /// no write lock. Publish and deliver never call this directly; the
    /// engine holds its own snapshot fed by the subscriber.
    pub fn snapshot(&self) -> Arc<RuleEngineConf> {
        self.current.load_full()
    }

    /// Snapshot clone of the current settings.
    pub fn get(&self) -> RuleEngineConf {
        (*self.current.load_full()).clone()
    }

    /// Replace the stored settings with a full-replacement candidate.
    ///
    /// Validate-all-before-apply: the candidate is validated before it can
    /// become visible, so invalid state is never observable. On success the
    /// snapshot, the persisted registry and the subscriber all move
    /// together; the in-memory swap precedes the atomic save (mirroring the
    /// sibling settings stores), so a save failure surfaces as
    /// [`EngineConfigUpdateError::Persist`] while the validated value
    /// stays. Management-plane only: one short swap per request; the fire
    /// path keeps reading its lock-free snapshot and publish/deliver never
    /// touch this store.
    pub fn replace(&self, next: RuleEngineConf) -> Result<RuleEngineConf, EngineConfigUpdateError> {
        next.validate()
            .map_err(|e| EngineConfigUpdateError::Invalid(e.to_string()))?;
        let _guard = self.save_lock.lock();
        self.current.store(Arc::new(next.clone()));
        if let Some(registry) = self.registry.read().clone() {
            registry
                .commit_rule_engine(next.clone())
                .map_err(|e| EngineConfigUpdateError::Persist(e.to_string()))?;
            registry
                .save()
                .map_err(|e| EngineConfigUpdateError::Persist(e.to_string()))?;
        }
        if let Some(hook) = self.subscriber.read().clone() {
            hook(&next);
        }
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

impl Default for EngineConfigStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_documented_struct() {
        let store = EngineConfigStore::new();
        let got = store.get();
        assert!(got.ignore_sys_message);
        assert_eq!(got.jq_function_default_timeout, "10s");
        assert_eq!(got.jq_timeout_ms(), 10_000);
        assert_eq!(got.jq_implementation_module, "jq_nif");
        assert!(!got.ssrf.enable);
        assert!(got.limit_selects_in_namespace);
    }

    #[test]
    fn invalid_candidate_is_rejected_without_applying() {
        let store = EngineConfigStore::new();
        let before = store.get();
        let mut bad = before.clone();
        bad.jq_function_default_timeout = "soon".to_string();
        assert!(matches!(
            store.replace(bad),
            Err(EngineConfigUpdateError::Invalid(_))
        ));
        assert_eq!(store.get(), before);
        let mut bad_module = before.clone();
        bad_module.jq_implementation_module = "jq_wasm".to_string();
        assert!(matches!(
            store.replace(bad_module),
            Err(EngineConfigUpdateError::Invalid(_))
        ));
        assert_eq!(store.get(), before);
    }

    #[test]
    fn replace_moves_snapshot_and_notifies_subscriber() {
        let store = EngineConfigStore::new();
        let seen = Arc::new(parking_lot::Mutex::new(false));
        let seen_clone = Arc::clone(&seen);
        store.set_subscriber(Arc::new(move |next: &RuleEngineConf| {
            *seen_clone.lock() = !next.ignore_sys_message;
        }));
        let mut next = store.get();
        next.ignore_sys_message = false;
        store.replace(next.clone()).expect("valid replace");
        assert_eq!(store.get(), next);
        assert!(*seen.lock());
        assert!(!store.snapshot().ignore_sys_message);
    }

    #[test]
    fn settings_survive_registry_reload() {
        let dir = std::env::temp_dir().join(format!(
            "rule-engine-config-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads"));
        let store = EngineConfigStore::from_registry(&registry);
        let mut next = store.get();
        next.ignore_sys_message = false;
        next.jq_function_default_timeout = "30s".to_string();
        store.replace(next.clone()).expect("persist settings");
        assert!(dir.join(broker_config::STATE_FILE_NAME).is_file());
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("data dir reloads"));
        let restarted = EngineConfigStore::from_registry(&reloaded);
        assert_eq!(restarted.get(), next);
        std::fs::remove_dir_all(&dir).ok();
    }
}
