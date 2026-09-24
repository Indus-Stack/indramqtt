//! Ordered authorization source registry for the management API.
//!
//! Covers the registry behind `GET /authorization/sources` (ordered list),
//! `POST /authorization/sources` (register one source) and the built-in
//! catch-all clear (`DELETE
//! /authorization/sources/built_in_database/rules`, which empties the
//! built-in rule table). Single-node, management-plane only: handlers
//! clone at most the capped list once per request under a short lock;
//! the publish and subscribe authorization consults read the built-in
//! rule table (`MemoryAuth` in `crate::lib`, driven by the publish event
//! in `crates/broker-node/src/main.rs`), never this registry's write
//! lock, and publish/deliver never touch this store.
//!
//! Store bounds (both stated here and enforced below):
//! - at most [`broker_config::MAX_AUTHZ_SOURCES`] entries (8: the
//!   management plane snapshots the whole registry per request and the
//!   decision path scans at most the capped entries, so the registry must
//!   stay small and bounded; 8 covers the documented backend families
//!   with headroom while keeping per-request clones cheap); creates past
//!   the cap are rejected instead of growing without limit;
//! - `type` values name a known source type and are unique within the
//!   registry (a duplicate type is rejected, never silently merged);
//! - the stored backend config is at most 8 KiB of JSON, so one entry
//!   cannot balloon memory;
//! - reads clone at most the capped list once per request under a short
//!   lock; decisions read a lock-free snapshot.
//!
//! Only the built-in database executes in the parity waves; every other
//! source type's config is stored and reported, never claimed as live.
//! An empty registry lists as an empty collection, never an error.
// TODO(parity): is 8 the documented source cap, and which source types
// plus per-source fields (status, order, health) does the spec require?
// The rulebook does not decide the exact set; the current choice mirrors
// the authenticator backend families and reports type/enable only,
// omitting status fields the broker cannot supply.

use broker_config::{
    is_known_authz_source_type, AuthzSourceEntry as ConfEntry, AuthzSourcesConf, ConfigRegistry,
    MAX_AUTHZ_SOURCES,
};
use std::sync::Arc;

/// Longest accepted backend-config JSON document (8 KiB).
/// Reason (measured cost): configs ride the per-request registry clone,
/// so one entry cannot balloon memory; 8 KiB caps the worst-case snapshot
/// at 8 entries x 8 KiB = 64 KiB, keeping per-request clones cheap, and
/// larger documents are rejected instead of growing without limit.
const MAX_CONFIG_LEN: usize = 8192;

/// One ordered registry slot: the source key plus its enable flag and
/// opaque backend config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthzSourceEntry {
    pub source_type: String,
    pub enable: bool,
    pub config: serde_json::Value,
}

/// Why [`AuthzSourceRegistry::insert`] refused an entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceInsertError {
    /// The `type` is already stored.
    Duplicate,
    /// The store holds [`MAX_AUTHZ_SOURCES`] entries.
    Full,
    /// The entry failed validation (message names the field).
    Invalid(String),
    /// The registry commit or atomic save failed after validation.
    Persist(String),
}

/// Ordered authorization source registry behind one short lock plus a
/// lock-free read snapshot for the decision path.
///
/// Management writes take the write lock, append the stored entry and
/// publish a fresh snapshot; readers load the snapshot without taking
/// the write lock and never block on a management write beyond one `Arc`
/// swap. Publish and deliver never touch either.
pub struct AuthzSourceRegistry {
    inner: parking_lot::RwLock<Vec<AuthzSourceEntry>>,
    snapshot: arc_swap::ArcSwap<Vec<AuthzSourceEntry>>,
    registry: parking_lot::RwLock<Option<Arc<ConfigRegistry>>>,
    save_lock: parking_lot::Mutex<()>,
}

impl AuthzSourceRegistry {
    /// Default registry: only the built-in database source (the one
    /// backend that executes, so a fresh broker authorizes against its
    /// local rule table without operator setup).
    pub fn new() -> Self {
        Self::from_snapshot(&AuthzSourcesConf::default())
    }

    /// Seed from a validated snapshot root. Memory-only. An empty root
    /// yields an empty registry, which lists as an empty collection.
    pub fn from_snapshot(conf: &AuthzSourcesConf) -> Self {
        let registry = Self {
            inner: parking_lot::RwLock::new(Vec::new()),
            snapshot: arc_swap::ArcSwap::from(Arc::new(Vec::new())),
            registry: parking_lot::RwLock::new(None),
            save_lock: parking_lot::Mutex::new(()),
        };
        registry.seed_from_snapshot(conf);
        registry
    }

    /// Seed from the registry's current snapshot and persist every later
    /// mutation back through it. Kernel boot path; defaults yield the
    /// built-in database source.
    pub fn from_registry(registry: &Arc<ConfigRegistry>) -> Self {
        let store = Self::from_snapshot(&registry.snapshot().authz_sources);
        *store.registry.write() = Some(Arc::clone(registry));
        store
    }

    /// Attach the registry and replace the current contents with its
    /// snapshot, in place on the same instance.
    pub fn seed_from_registry(&self, registry: &Arc<ConfigRegistry>) {
        let entries = registry
            .snapshot()
            .authz_sources
            .sources
            .iter()
            .filter_map(conf_to_entry)
            .collect::<Vec<_>>();
        *self.inner.write() = entries.clone();
        self.snapshot.store(Arc::new(entries));
        *self.registry.write() = Some(Arc::clone(registry));
    }

    fn seed_from_snapshot(&self, conf: &AuthzSourcesConf) {
        let entries = conf
            .sources
            .iter()
            .filter_map(conf_to_entry)
            .collect::<Vec<_>>();
        *self.inner.write() = entries.clone();
        self.snapshot.store(Arc::new(entries));
    }

    /// Ordered snapshot of the registry in stored order. Management-plane
    /// read only; clones at most the capped list under one short lock.
    pub fn list(&self) -> Vec<AuthzSourceEntry> {
        self.inner.read().clone()
    }

    /// Lock-free snapshot for readers: one `Arc` load, no write lock, at
    /// most the capped entries to scan. Publish and deliver never call
    /// this on the hot path; decisions read snapshots.
    pub fn snapshot(&self) -> Arc<Vec<AuthzSourceEntry>> {
        self.snapshot.load_full()
    }

    /// One entry by source `type` (`None` when unknown). Management-plane
    /// read only; clones at most one entry under a short lock.
    pub fn get(&self, source_type: &str) -> Option<AuthzSourceEntry> {
        self.inner
            .read()
            .iter()
            .find(|e| e.source_type == source_type)
            .cloned()
    }

    /// Whether the built-in database source may execute: true when the
    /// registry holds an enabled `built_in_database` slot. Consulted by
    /// the rule-table tasks once they land; management-plane only today.
    pub fn built_in_available(&self) -> bool {
        let snap = self.snapshot.load();
        snap.iter()
            .any(|e| e.source_type == "built_in_database" && e.enable)
    }

    /// Register one source. Fails cleanly on duplicate `type` (caller
    /// maps to `ALREADY_EXISTS`), a full registry (caller maps to
    /// `BAD_REQUEST`) or an invalid entry (caller maps to `BAD_REQUEST`
    /// or `NOT_FOUND` for an unknown type). Validated all-before-apply:
    /// the candidate root is validated before it can become visible, so
    /// invalid state is never observable. Persists through the registry
    /// when one is attached; a commit or save failure surfaces as
    /// [`SourceInsertError::Persist`] and the in-memory update stays
    /// (commit precedes the atomic save, mirroring the credential
    /// store).
    pub fn insert(&self, entry: AuthzSourceEntry) -> Result<AuthzSourceEntry, SourceInsertError> {
        validate_entry(&entry)?;
        let _guard = self.save_lock.lock();
        let conf = {
            let mut current = self.inner.write();
            if current.iter().any(|e| e.source_type == entry.source_type) {
                return Err(SourceInsertError::Duplicate);
            }
            if current.len() >= MAX_AUTHZ_SOURCES {
                return Err(SourceInsertError::Full);
            }
            let mut next = current.clone();
            next.push(entry.clone());
            let conf = export_conf(&next);
            conf.validate()
                .map_err(|e| SourceInsertError::Invalid(e.to_string()))?;
            current.push(entry.clone());
            self.snapshot.store(Arc::new(current.clone()));
            conf
        };
        if let Some(registry) = self.registry.read().clone() {
            registry
                .commit_authz_sources(conf)
                .map_err(|e| SourceInsertError::Persist(e.to_string()))?;
            registry
                .save()
                .map_err(|e| SourceInsertError::Persist(e.to_string()))?;
        }
        Ok(entry)
    }

    /// Number of stored entries.
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// Whether the registry holds no entries.
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }
}

impl Default for AuthzSourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

fn validate_entry(entry: &AuthzSourceEntry) -> Result<(), SourceInsertError> {
    if entry.source_type.trim().is_empty() {
        return Err(SourceInsertError::Invalid(
            "field `type` must not be empty".to_string(),
        ));
    }
    if !is_known_authz_source_type(&entry.source_type) {
        return Err(SourceInsertError::Invalid(format!(
            "field `type` {:?} is unknown",
            entry.source_type
        )));
    }
    let rendered = entry.config.to_string();
    if rendered.len() > MAX_CONFIG_LEN {
        return Err(SourceInsertError::Invalid(
            "field `config` is too large".to_string(),
        ));
    }
    Ok(())
}

fn export_conf(entries: &[AuthzSourceEntry]) -> AuthzSourcesConf {
    AuthzSourcesConf {
        sources: entries
            .iter()
            .map(|e| ConfEntry {
                source_type: e.source_type.clone(),
                enable: e.enable,
                config: if e.config.is_null() {
                    String::new()
                } else {
                    e.config.to_string()
                },
            })
            .collect(),
    }
}

fn conf_to_entry(conf: &ConfEntry) -> Option<AuthzSourceEntry> {
    if conf.source_type.trim().is_empty() || !is_known_authz_source_type(&conf.source_type) {
        return None;
    }
    let config = if conf.config.trim().is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(&conf.config).ok()?
    };
    Some(AuthzSourceEntry {
        source_type: conf.source_type.clone(),
        enable: conf.enable,
        config,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(source_type: &str) -> AuthzSourceEntry {
        AuthzSourceEntry {
            source_type: source_type.to_string(),
            enable: true,
            config: serde_json::Value::Null,
        }
    }

    #[test]
    fn default_registry_holds_only_the_built_in_source() {
        let registry = AuthzSourceRegistry::new();
        assert_eq!(registry.len(), 1);
        let stored = registry.get("built_in_database").expect("built-in present");
        assert!(stored.enable);
        assert!(registry.built_in_available());
        let snap = registry.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].source_type, "built_in_database");
    }

    #[test]
    fn empty_snapshot_lists_empty_without_error() {
        let registry = AuthzSourceRegistry::from_snapshot(&AuthzSourcesConf { sources: vec![] });
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        assert!(registry.list().is_empty());
        assert!(registry.snapshot().is_empty());
        assert!(!registry.built_in_available());
    }

    #[test]
    fn insert_appends_in_order_and_rejects_duplicates() {
        let registry = AuthzSourceRegistry::new();
        registry.insert(entry("file")).expect("file insert");
        let ids: Vec<_> = registry
            .list()
            .iter()
            .map(|e| e.source_type.clone())
            .collect();
        assert_eq!(
            ids,
            vec!["built_in_database".to_string(), "file".to_string()]
        );
        // Snapshot moves with the store.
        let snap: Vec<_> = registry
            .snapshot()
            .iter()
            .map(|e| e.source_type.clone())
            .collect();
        assert_eq!(ids, snap);
        assert_eq!(
            registry.insert(entry("file")),
            Err(SourceInsertError::Duplicate)
        );
        // Stored order is unchanged by the rejected duplicate.
        let ids: Vec<_> = registry
            .list()
            .iter()
            .map(|e| e.source_type.clone())
            .collect();
        assert_eq!(
            ids,
            vec!["built_in_database".to_string(), "file".to_string()]
        );
    }

    #[test]
    fn insert_rejects_unknown_type_and_oversized_config() {
        let registry = AuthzSourceRegistry::new();
        assert!(matches!(
            registry.insert(entry("nowhere")),
            Err(SourceInsertError::Invalid(_))
        ));
        let mut bad = entry("file");
        bad.source_type = "  ".to_string();
        assert!(matches!(
            registry.insert(bad),
            Err(SourceInsertError::Invalid(_))
        ));
        let mut big = entry("file");
        big.config = serde_json::Value::String("x".repeat(MAX_CONFIG_LEN + 1));
        assert!(matches!(
            registry.insert(big),
            Err(SourceInsertError::Invalid(_))
        ));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn insert_enforces_source_cap() {
        let registry = AuthzSourceRegistry::from_snapshot(&AuthzSourcesConf { sources: vec![] });
        for source_type in broker_config::KNOWN_AUTHZ_SOURCE_TYPES
            .iter()
            .take(MAX_AUTHZ_SOURCES)
        {
            registry.insert(entry(source_type)).expect("fill to cap");
        }
        assert_eq!(registry.len(), MAX_AUTHZ_SOURCES);
        // The next documented type is not stored yet, so the cap (not a
        // duplicate) refuses it.
        assert_eq!(
            registry.insert(entry(
                broker_config::KNOWN_AUTHZ_SOURCE_TYPES[MAX_AUTHZ_SOURCES]
            )),
            Err(SourceInsertError::Full)
        );
        assert_eq!(registry.len(), MAX_AUTHZ_SOURCES);
    }

    #[test]
    fn registry_survives_registry_reload() {
        let dir = std::env::temp_dir().join(format!(
            "authz-sources-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads"));
        // Fresh defaults already persist the built-in source.
        let restarted = AuthzSourceRegistry::from_registry(&registry);
        assert!(restarted.get("built_in_database").is_some());
        restarted.insert(entry("file")).expect("persist file");
        assert!(dir.join(broker_config::STATE_FILE_NAME).is_file());
        drop(restarted);
        drop(registry);
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("data dir reloads"));
        let restarted = AuthzSourceRegistry::from_registry(&reloaded);
        let ids: Vec<_> = restarted
            .list()
            .iter()
            .map(|e| e.source_type.clone())
            .collect();
        assert_eq!(
            ids,
            vec!["built_in_database".to_string(), "file".to_string()]
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
