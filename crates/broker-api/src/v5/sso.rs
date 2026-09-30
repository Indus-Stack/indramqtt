//! Configured single-sign-on backends for the v5 management API.
//!
//! Covers `GET /sso` (ordered list, C05-11) on real state.
//! Single-node, management-plane only: handlers clone at most the capped
//! list once per request under a short lock; publish and deliver never
//! touch this store, and no identity-provider client runs here (listing
//! configured backends only; login flows, MFA, callbacks and token
//! exchange are sibling tasks).
//!
//! Store bounds (stated here and enforced below):
//! - at most [`MAX_SSO_BACKENDS`] backend records; inserts past the cap
//!   are rejected instead of growing the map without limit;
//! - backend names are capped at
//!   [`broker_config::MAX_SSO_BACKEND_NAME_LEN`] bytes and must be
//!   slash-free, so one record cannot balloon memory or smuggle a path;
//! - backend config documents are capped at [`MAX_SSO_CONFIG_LEN`] bytes
//!   of rendered JSON, so one record cannot balloon memory;
//! - the default store is empty; an empty set lists as an empty
//!   collection, never an error.
// TODO(parity): which per-backend fields does the documented shape
// require beyond backend/enable (display name, URLs, client ids, ...)?
// The rulebook does not decide the exact shape and the broker tracks only
// the registry slot (backend key, enable flag, opaque config), so the
// current choice renders backend/enable plus `config` when one is stored
// and omits fields the broker cannot supply (never a constant standing
// in for a measurement) until the checker pins them.
// TODO(parity): is the documented list a bare array or an envelope, and
// what are the exact per-backend field names (`backend` versus `name`
// versus `id`)? The rulebook does not decide the exact shape; the current
// choice answers a bare JSON array of backend rows (the route is not
// paged) until the checker pins it.
// TODO(parity): which backend names does the spec document (oidc, saml,
// oauth, ...)? The rulebook does not decide the exact vocabulary; the
// current choice stores any non-empty slash-free name within the length
// cap, without claiming any backend executes, until the checker pins it.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use broker_config::{ConfigRegistry, SsoBackendsConf};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::ApiState;

/// Upper bound for configured backends. Inserts past this size are
/// rejected instead of growing the map without limit. Reason: the
/// management plane snapshots the whole registry per list request and
/// each entry holds an opaque config document, so an unbounded map lets
/// one management batch exhaust the kernel; 16 covers the dashboard
/// backend families with headroom while keeping list reads cheap.
pub const MAX_SSO_BACKENDS: usize = broker_config::MAX_SSO_BACKENDS;
/// Longest accepted rendered backend config document (8 KiB of JSON).
/// Reason: the config renders in the detail row and is cloned per list
/// snapshot plus persisted per entry, so an unbounded document lets one
/// record bloat each list read; 8 KiB covers provider URLs, client ids
/// and field maps with headroom while keeping rows small.
const MAX_SSO_CONFIG_LEN: usize = 8192;

/// One configured backend: the registry key plus the enable flag and the
/// opaque backend config (Null when nothing is stored).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsoBackend {
    backend: String,
    enable: bool,
    config: serde_json::Value,
}

impl SsoBackend {
    /// List row: the stored slot. `config` renders only when one is
    /// stored; backends without stored config omit it rather than
    /// inventing an empty object.
    fn to_json(&self) -> serde_json::Value {
        let mut value = serde_json::json!({
            "backend": self.backend,
            "enable": self.enable,
        });
        if !self.config.is_null() {
            value["config"] = self.config.clone();
        }
        value
    }
}

/// In-memory backend registry keyed by backend name.
///
/// Single-node map behind one lock; every method finishes quickly.
/// Management writes take the write lock plus the registry save lock on
/// persist; reads take one short read lock and clone at most the capped
/// entries once per request. Publish and deliver never touch this store.
pub struct SsoStore {
    inner: RwLock<HashMap<String, SsoBackend>>,
    /// Kernel config registry receiving every mutation (`None` in unit
    /// tests and standalone state, which stay memory-only).
    registry: RwLock<Option<Arc<ConfigRegistry>>>,
    /// Serialises export-commit-save so concurrent mutations cannot
    /// interleave into a lost update on disk.
    save_lock: Mutex<()>,
}

impl SsoStore {
    /// Empty registry (today's empty behaviour: no backends).
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            registry: RwLock::new(None),
            save_lock: Mutex::new(()),
        }
    }

    /// Seed from a validated snapshot root. Memory-only.
    pub fn from_snapshot(conf: &SsoBackendsConf) -> Self {
        let store = Self::new();
        store.seed_from_snapshot(conf);
        store
    }

    /// Seed from the registry's current snapshot and persist every later
    /// mutation back through it. Kernel boot path; an empty snapshot
    /// yields today's empty behaviour.
    pub fn from_registry(registry: &Arc<ConfigRegistry>) -> Self {
        let store = Self::from_snapshot(&registry.snapshot().sso_backends);
        *store.registry.write().expect("sso store lock") = Some(Arc::clone(registry));
        store
    }

    /// Attach the registry and replace the current contents with its
    /// snapshot, in place on the same instance.
    pub fn seed_from_registry(&self, registry: &Arc<ConfigRegistry>) {
        self.seed_from_snapshot(&registry.snapshot().sso_backends);
        *self.registry.write().expect("sso store lock") = Some(Arc::clone(registry));
    }

    fn seed_from_snapshot(&self, conf: &SsoBackendsConf) {
        let mut map = self.inner.write().expect("sso store lock");
        map.clear();
        for entry in &conf.backends {
            let config = if entry.config.trim().is_empty() {
                serde_json::Value::Null
            } else {
                match serde_json::from_str(&entry.config) {
                    Ok(config) => config,
                    Err(_) => {
                        // Fail closed: an undecodable config can never
                        // drive a login flow, so the entry keeps its key
                        // with no config instead of a half-parsed value.
                        // The loss is visible as a missing `config` field
                        // in the list row instead of a silently wrong one.
                        tracing::warn!(
                            backend = entry.backend.as_str(),
                            "sso snapshot entry has an undecodable config; stored without config"
                        );
                        serde_json::Value::Null
                    }
                }
            };
            map.insert(
                entry.backend.clone(),
                SsoBackend {
                    backend: entry.backend.clone(),
                    enable: entry.enable,
                    config,
                },
            );
        }
    }

    /// Snapshot of live backends sorted by name for stable reads.
    pub fn list(&self) -> Vec<SsoBackend> {
        let map = self.inner.read().expect("sso store lock");
        let mut out: Vec<SsoBackend> = map.values().cloned().collect();
        out.sort_by(|a, b| a.backend.cmp(&b.backend));
        out
    }

    /// Read one backend by name. Returns a clone under a short read lock,
    /// or `None` when nothing is stored under the name.
    pub fn get(&self, backend: &str) -> Option<SsoBackend> {
        self.inner
            .read()
            .expect("sso store lock")
            .get(backend)
            .cloned()
    }

    /// Record one backend. Used by the broker-through test seed and the
    /// future per-backend detail task; the list route itself never writes.
    /// Fails cleanly on duplicate names (caller maps to `ALREADY_EXISTS`),
    /// a full registry (caller maps to `BAD_REQUEST`) or an invalid entry
    /// (caller maps to `BAD_REQUEST`). Persists through the registry when
    /// one is attached; a commit or save failure rolls back the in-memory
    /// insert and surfaces as [`SsoInsertError::Persist`].
    pub fn insert(&self, backend: SsoBackend) -> Result<SsoBackend, SsoInsertError> {
        validate_backend(&backend)?;
        {
            let mut map = self.inner.write().expect("sso store lock");
            if map.contains_key(&backend.backend) {
                return Err(SsoInsertError::Duplicate);
            }
            if map.len() >= MAX_SSO_BACKENDS {
                return Err(SsoInsertError::Full);
            }
            map.insert(backend.backend.clone(), backend.clone());
        }
        if let Err(reason) = self.persist() {
            let mut map = self.inner.write().expect("sso store lock");
            map.remove(&backend.backend);
            return Err(SsoInsertError::Persist(reason));
        }
        Ok(backend)
    }

    /// Export the current contents as a validated config root, sorted by
    /// backend name so the persisted file is deterministic.
    fn export_conf(&self) -> SsoBackendsConf {
        let map = self.inner.read().expect("sso store lock");
        let mut entries: Vec<broker_config::SsoBackendEntry> = map
            .values()
            .map(|backend| broker_config::SsoBackendEntry {
                backend: backend.backend.clone(),
                enable: backend.enable,
                config: if backend.config.is_null() {
                    String::new()
                } else {
                    backend.config.to_string()
                },
            })
            .collect();
        entries.sort_by(|a, b| a.backend.cmp(&b.backend));
        SsoBackendsConf { backends: entries }
    }

    /// Commit the exported root and atomically save it. A no-op without
    /// a registry; any commit or save failure surfaces as a message so
    /// callers answer 500 and the loss is never silent.
    fn persist(&self) -> Result<(), String> {
        let registry = self.registry.read().expect("sso store lock").clone();
        let Some(registry) = registry else {
            return Ok(());
        };
        let _guard = self.save_lock.lock().expect("sso save lock");
        let conf = self.export_conf();
        registry
            .commit_sso_backends(conf)
            .map_err(|err| format!("cannot persist sso backends: {err}"))?;
        registry
            .save()
            .map_err(|err| format!("cannot persist sso backends: {err}"))?;
        Ok(())
    }
}

impl Default for SsoStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Why [`SsoStore::insert`] refused a record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SsoInsertError {
    /// The backend name is already stored.
    Duplicate,
    /// The registry holds [`MAX_SSO_BACKENDS`] records.
    Full,
    /// The entry failed validation (empty name, slash, too long, or an
    /// oversized config document).
    Invalid(String),
    /// The in-memory insert applied but the registry commit or atomic
    /// save failed. Callers map this to HTTP 500; it is never silent.
    Persist(String),
}

/// Validate one backend record before it can become visible.
fn validate_backend(backend: &SsoBackend) -> Result<(), SsoInsertError> {
    if backend.backend.trim().is_empty() {
        return Err(SsoInsertError::Invalid(
            "field `backend` must not be empty".to_string(),
        ));
    }
    if backend.backend.len() > broker_config::MAX_SSO_BACKEND_NAME_LEN {
        return Err(SsoInsertError::Invalid(
            "field `backend` is too long".to_string(),
        ));
    }
    if backend.backend.contains('/') {
        return Err(SsoInsertError::Invalid(
            "field `backend` must not contain `/`".to_string(),
        ));
    }
    if backend.config.to_string().len() > MAX_SSO_CONFIG_LEN {
        return Err(SsoInsertError::Invalid(
            "field `config` is too large".to_string(),
        ));
    }
    Ok(())
}

/// Build one backend record from parts. The list route never calls this;
/// the broker-through seed and the future detail task share it so both
/// validate the same way.
pub fn sso_backend(backend: String, enable: bool, config: serde_json::Value) -> SsoBackend {
    SsoBackend {
        backend,
        enable,
        config,
    }
}

/// `GET /sso`: configured backends as a bare JSON array in backend-name
/// order. An empty registry reads as `[]`, never an error. The route is
/// not paged and takes no parameters; unknown query parameters are
/// ignored (the handler takes no query extractor, so Axum never parses
/// them).
///
/// Management-plane only: one short read lock per request cloning at most
/// the capped entries; publish and deliver never touch this store.
pub async fn list_sso(State(state): State<ApiState>) -> Response {
    let all = state.sso_backends.list();
    let data: Vec<serde_json::Value> = all.iter().map(SsoBackend::to_json).collect();
    (StatusCode::OK, Json(data)).into_response()
}

#[cfg(test)]
mod sso_store_tests {
    use super::*;

    fn empty_store() -> SsoStore {
        SsoStore::new()
    }

    #[test]
    fn empty_registry_lists_nothing() {
        let store = empty_store();
        assert!(store.list().is_empty());
        assert!(store.get("w232-missing").is_none());
    }

    #[test]
    fn insert_lists_sorted_with_config_shape() {
        let store = empty_store();
        store
            .insert(sso_backend(
                "w232-saml".to_string(),
                true,
                serde_json::Value::Null,
            ))
            .expect("first insert succeeds");
        store
            .insert(sso_backend(
                "w232-oidc".to_string(),
                false,
                serde_json::json!({"issuer": "https://idp.example.invalid"}),
            ))
            .expect("second insert succeeds");
        let listed = store.list();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].backend, "w232-oidc");
        assert_eq!(listed[1].backend, "w232-saml");
        // Rows render the stored slot; backends without config omit it
        // rather than inventing an empty object.
        let oidc = listed[0].to_json();
        assert_eq!(oidc["backend"], serde_json::json!("w232-oidc"));
        assert_eq!(oidc["enable"], serde_json::json!(false));
        assert_eq!(
            oidc["config"],
            serde_json::json!({"issuer": "https://idp.example.invalid"})
        );
        let saml = listed[1].to_json();
        assert_eq!(saml["backend"], serde_json::json!("w232-saml"));
        assert_eq!(saml["enable"], serde_json::json!(true));
        assert!(saml.get("config").is_none());
        // Detail read agrees with the list.
        assert_eq!(store.get("w232-oidc"), Some(listed[0].clone()));
    }

    #[test]
    fn duplicate_names_are_rejected() {
        let store = empty_store();
        store
            .insert(sso_backend(
                "w232-dup".to_string(),
                true,
                serde_json::Value::Null,
            ))
            .expect("first insert succeeds");
        assert_eq!(
            store.insert(sso_backend(
                "w232-dup".to_string(),
                true,
                serde_json::Value::Null
            )),
            Err(SsoInsertError::Duplicate)
        );
        assert_eq!(store.list().len(), 1);
    }

    #[test]
    fn invalid_entries_are_rejected() {
        let store = empty_store();
        for bad in [
            sso_backend(String::new(), true, serde_json::Value::Null),
            sso_backend("   ".to_string(), true, serde_json::Value::Null),
            sso_backend("has/slash".to_string(), true, serde_json::Value::Null),
            sso_backend(
                "x".repeat(broker_config::MAX_SSO_BACKEND_NAME_LEN + 1),
                true,
                serde_json::Value::Null,
            ),
        ] {
            assert!(
                matches!(store.insert(bad), Err(SsoInsertError::Invalid(_))),
                "bad entry must be rejected"
            );
        }
        assert!(store.list().is_empty());
    }

    #[test]
    fn full_registry_rejects_further_inserts() {
        let store = empty_store();
        {
            let mut map = store.inner.write().expect("test lock");
            for index in 0..MAX_SSO_BACKENDS {
                let name = format!("w232-fill-{index}");
                map.insert(
                    name.clone(),
                    SsoBackend {
                        backend: name,
                        enable: true,
                        config: serde_json::Value::Null,
                    },
                );
            }
        }
        assert_eq!(
            store.insert(sso_backend(
                "w232-one-more".to_string(),
                true,
                serde_json::Value::Null
            )),
            Err(SsoInsertError::Full)
        );
    }

    #[test]
    fn snapshot_round_trip_persists_across_restart() {
        let dir = std::env::temp_dir().join(format!(
            "indramqtt-sso-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).expect("create test dir");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("registry loads"));
        let store = SsoStore::from_registry(&registry);
        assert!(store.list().is_empty());
        store
            .insert(sso_backend(
                "w232-oidc".to_string(),
                true,
                serde_json::json!({"issuer": "https://idp.example.invalid"}),
            ))
            .expect("insert succeeds");
        // The persisted snapshot holds the entry.
        let snapshot = registry.snapshot();
        assert_eq!(snapshot.sso_backends.backends.len(), 1);
        assert_eq!(snapshot.sso_backends.backends[0].backend, "w232-oidc");
        // A restart re-seed observes the same entry.
        let restarted = SsoStore::from_registry(&Arc::new(
            ConfigRegistry::load(&dir).expect("registry reloads"),
        ));
        let listed = restarted.list();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].backend, "w232-oidc");
        assert!(restarted.get("w232-missing").is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
