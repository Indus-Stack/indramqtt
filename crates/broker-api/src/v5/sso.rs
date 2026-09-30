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
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use broker_config::{ConfigRegistry, SsoBackendsConf};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::ApiState;

/// Upper bound for configured backends. Inserts past this size are
/// rejected instead of growing the map without limit. Reason: the
/// management plane snapshots the whole registry per list request and
/// each entry holds an opaque config document, so an unbounded map lets
/// one management batch exhaust the kernel; 16 covers the dashboard
/// backend families with headroom while keeping list reads cheap.
pub const MAX_SSO_BACKENDS: usize = broker_config::MAX_SSO_BACKENDS;
/// Upper bound for pending single-sign-on login flows. Every login attempt
/// that reaches verification records one entry; entries expire after
/// [`PENDING_SSO_LOGIN_TTL`] and are swept on every attempt. Failed logins
/// never issue tokens and never complete, so without expiry the table would
/// grow with the attempt rate. Reason: each entry holds a flow id plus
/// backend/username (hundreds of bytes), so 1024 covers concurrent dashboard
/// login attempts while keeping management memory bounded regardless of
/// attempt rate. The default is finite: no opt-in to unlimited exists
/// anywhere on this path.
pub const MAX_PENDING_SSO_LOGINS: usize = 1024;
/// Lifetime of one pending login flow: 300 s (5 minutes). Reason: a login
/// flow completes via a dashboard redirect round-trip measured in seconds;
/// 300 s covers slow round-trips while the per-attempt expiry sweep keeps
/// stale flows from accumulating. Flows that never complete (every flow
/// today: verification waits on the provider clients) vanish after this.
// TODO(parity): is 300 s the documented flow/code lifetime, or does the
// spec expire login flows sooner? The rulebook does not decide the exact
// TTL; 300 s is the conservative choice until the checker pins it.
pub const PENDING_SSO_LOGIN_TTL: Duration = Duration::from_secs(300);
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

/// One initiated login flow awaiting verification. The flow never completes
/// in this task: verification needs a provider client that only the stored
/// backend config describes (no outbound client here), so every flow fails
/// closed and vanishes at [`PENDING_SSO_LOGIN_TTL`]. The table exists so an
/// initiated attempt is observable and bounded, never an unbounded session
/// table.
#[derive(Debug, Clone)]
struct PendingSsoLogin {
    flow_id: String,
    backend: String,
    username: String,
    created_at: Instant,
}

impl PendingSsoLogin {
    fn expired(&self, now: Instant) -> bool {
        now.duration_since(self.created_at) >= PENDING_SSO_LOGIN_TTL
    }
}

/// In-memory backend registry keyed by backend name.
///
/// Single-node map behind one lock; every method finishes quickly.
/// Management writes take the write lock plus the registry save lock on
/// persist; reads take one short read lock and clone at most the capped
/// entries once per request. Publish and deliver never touch this store.
/// The pending-flow table beside it takes one short mutex per login attempt
/// (sweep plus insert); publish and deliver never touch it either.
pub struct SsoStore {
    inner: RwLock<HashMap<String, SsoBackend>>,
    /// Initiated login flows, swept for expiry on every attempt and capped
    /// at [`MAX_PENDING_SSO_LOGINS`]. Management-plane only.
    pending: Mutex<HashMap<String, PendingSsoLogin>>,
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
            pending: Mutex::new(HashMap::new()),
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

    /// Record one initiated login flow and return its id. Sweeps expired
    /// flows first; when the table is still at [`MAX_PENDING_SSO_LOGINS`]
    /// evicts the oldest flow so one burst of attempts cannot grow the map
    /// without limit. Management-plane only: one short mutex per login
    /// attempt; publish and deliver never touch this table.
    // TODO(parity): when the table is still full after the expiry sweep,
    // does the documented behaviour evict the oldest flow or refuse the
    // login? The rulebook does not decide the full-map policy; the current
    // choice evicts the oldest flow so a live login attempt is always
    // recorded, until the checker pins it.
    pub fn record_login_attempt(&self, backend: &str, username: &str) -> String {
        let flow_id = format!("{:032x}", rand::random::<u128>());
        let mut pending = self.pending.lock().expect("sso pending lock");
        let now = Instant::now();
        pending.retain(|_, flow| !flow.expired(now));
        while pending.len() >= MAX_PENDING_SSO_LOGINS {
            let oldest = pending
                .iter()
                .min_by_key(|(_, flow)| flow.created_at)
                .map(|(id, _)| id.clone());
            match oldest {
                Some(id) => {
                    if let Some(evicted) = pending.remove(&id) {
                        tracing::debug!(
                            flow_id = evicted.flow_id.as_str(),
                            backend = evicted.backend.as_str(),
                            username = evicted.username.as_str(),
                            "sso pending login flow evicted: table full"
                        );
                    }
                }
                None => break,
            }
        }
        pending.insert(
            flow_id.clone(),
            PendingSsoLogin {
                flow_id: flow_id.clone(),
                backend: backend.to_string(),
                username: username.to_string(),
                created_at: now,
            },
        );
        flow_id
    }

    /// Live pending flows after sweeping expired ones. Test and operator
    /// hook; the login route itself never reads it.
    #[cfg(test)]
    fn pending_len(&self) -> usize {
        let mut pending = self.pending.lock().expect("sso pending lock");
        let now = Instant::now();
        pending.retain(|_, flow| !flow.expired(now));
        pending.len()
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

/// `POST /sso/login/{backend}`: initiate a login against one configured
/// backend.
///
/// The route is public (no bearer token: the dashboard login page calls it
/// before anyone holds a token) and not paged. Behaviour, in order:
/// - unknown backends fail with 404 in the documented `{code, message}`
///   shape and record nothing;
/// - bodies that are not a JSON object, or carry no non-empty `username`,
///   fail with 400 in the documented shape and record nothing;
/// - a backend stored with `enable` set to false fails closed with 401:
///   a disabled slot must never authenticate;
/// - otherwise the attempt is recorded in the bounded pending-flow table
///   and fails closed with 401: verification needs the provider the stored
///   config describes, and no provider client runs in this task, so no
///   credential can be verified and no token is ever issued here.
///
/// Fail-closed rows are logged without secrets (usernames only, never
/// bodies). Management-plane only: one short registry read plus one short
/// pending-table lock per call; publish and deliver never touch either.
// TODO(parity): is the documented miss code `BACKEND_NOT_FOUND` or the
// generic `NOT_FOUND`? The rulebook does not decide the exact code name;
// the current choice keeps `BACKEND_NOT_FOUND` (404, documented
// `{code, message}` shape) until the checker pins it.
// TODO(parity): what is the documented per-backend body (password,
// one-time code, redirect response, ...)? The rulebook does not decide the
// body contract; the current choice accepts any JSON object with a
// non-empty `username` and ignores the rest until the checker pins it.
// TODO(parity): what completes a flow (token fields, redirect fields,
// MFA steps)? Verification needs a provider client beyond the stored
// config, which is out of scope here, so every verified attempt currently
// fails closed with `BAD_USERNAME_OR_PWD` (401, documented shape, no
// token) until the provider and token-exchange tasks pin success.
pub async fn login(
    State(state): State<ApiState>,
    Path(backend): Path<String>,
    body: Bytes,
) -> Response {
    let Some(stored) = state.sso_backends.get(&backend) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "code": "BACKEND_NOT_FOUND",
                "message": "Backend not found",
            })),
        )
            .into_response();
    };
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "code": "BAD_REQUEST",
                    "message": format!("invalid login body: {error}"),
                })),
            )
                .into_response();
        }
    };
    let Some(username) = value
        .as_object()
        .and_then(|object| object.get("username"))
        .and_then(|name| name.as_str())
        .map(str::trim)
        .filter(|name| !name.is_empty())
    else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "BAD_REQUEST",
                "message": "field `username` must not be empty",
            })),
        )
            .into_response();
    };
    if !stored.enable {
        tracing::warn!(
            backend = backend.as_str(),
            username = username,
            "sso login refused: backend is disabled"
        );
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "code": "BAD_USERNAME_OR_PWD",
                "message": "Auth failed",
            })),
        )
            .into_response();
    }
    // Initiated and bounded: the flow is recorded, then verification fails
    // closed. No provider client runs here, so no credential can be
    // checked against anything; granting a token would grant access
    // without verification. The attempt is logged without secrets.
    state.sso_backends.record_login_attempt(&backend, username);
    tracing::warn!(
        backend = backend.as_str(),
        username = username,
        "sso login failed: no provider client for this backend"
    );
    (
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({
            "code": "BAD_USERNAME_OR_PWD",
            "message": "Auth failed",
        })),
    )
        .into_response()
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

#[cfg(test)]
mod sso_login_tests {
    use super::*;
    use broker_rules::{BackpressurePolicy, RuleEngine};

    fn standalone_state() -> ApiState {
        ApiState::standalone(Arc::new(RuleEngine::new(
            16,
            BackpressurePolicy::DropOldest,
        )))
    }

    fn seed_backend(state: &ApiState, name: &str, enable: bool) {
        state
            .sso_backends
            .insert(sso_backend(
                name.to_string(),
                enable,
                serde_json::json!({"issuer": "https://idp.example.invalid"}),
            ))
            .expect("seed backend inserts");
    }

    fn body(value: serde_json::Value) -> Bytes {
        Bytes::from(serde_json::to_vec(&value).expect("test body serialises"))
    }

    async fn parts(response: Response) -> (StatusCode, serde_json::Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("test body is small and readable");
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("test body is JSON");
        (status, json)
    }

    async fn attempt(
        state: &ApiState,
        backend: &str,
        raw: Bytes,
    ) -> (StatusCode, serde_json::Value) {
        parts(login(State(state.clone()), Path(backend.to_string()), raw).await).await
    }

    #[tokio::test]
    async fn unknown_backend_is_404_with_documented_shape_and_records_nothing() {
        // Before this task every login answered 404 with the router's
        // plain-text body; now unknown backends answer 404 in the
        // documented `{code, message}` shape.
        let state = standalone_state();
        let (status, err) = attempt(
            &state,
            "w233-no-such-backend",
            body(serde_json::json!({"username": "alice", "password": "wrong"})),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(err["code"], serde_json::json!("BACKEND_NOT_FOUND"));
        assert!(err["message"].is_string());
        assert_eq!(state.sso_backends.pending_len(), 0);
    }

    #[tokio::test]
    async fn configured_backend_with_bad_credentials_is_rejected_without_token() {
        // The registry decides: a stored backend with unverifiable
        // credentials fails closed (401, documented shape, no token) and
        // the attempt is recorded in the bounded pending table.
        let state = standalone_state();
        seed_backend(&state, "w233-ldap", true);
        let (status, err) = attempt(
            &state,
            "w233-ldap",
            body(serde_json::json!({"username": "alice", "password": "wrong"})),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(err["code"], serde_json::json!("BAD_USERNAME_OR_PWD"));
        assert!(err["message"].is_string());
        assert!(err.get("token").is_none(), "no token is issued on failure");
        assert_eq!(state.sso_backends.pending_len(), 1);
        // The list still serves the backend; login recorded nothing there.
        assert_eq!(state.sso_backends.list().len(), 1);
    }

    #[tokio::test]
    async fn malformed_bodies_are_400_and_record_nothing() {
        let state = standalone_state();
        seed_backend(&state, "w233-ldap", true);
        for raw in [
            Bytes::from_static(b""),
            Bytes::from_static(b"not json"),
            body(serde_json::json!([1, 2])),
            body(serde_json::json!(null)),
            body(serde_json::json!({})),
            body(serde_json::json!({"password": "x"})),
            body(serde_json::json!({"username": ""})),
            body(serde_json::json!({"username": "   "})),
            body(serde_json::json!({"username": 7})),
        ] {
            let (status, err) = attempt(&state, "w233-ldap", raw).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
            assert!(err["message"].is_string());
        }
        assert_eq!(state.sso_backends.pending_len(), 0);
        // Backend lookup runs first: garbage bodies against an unknown
        // backend still answer 404, never 400.
        let (status, err) = attempt(
            &state,
            "w233-no-such-backend",
            Bytes::from_static(b"not json"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(err["code"], serde_json::json!("BACKEND_NOT_FOUND"));
    }

    #[tokio::test]
    async fn disabled_backend_is_denied_without_recording() {
        // A stored but disabled slot fails closed: 401, no token, and no
        // pending flow (disabled backends never authenticate).
        let state = standalone_state();
        seed_backend(&state, "w233-off", false);
        let (status, err) = attempt(
            &state,
            "w233-off",
            body(serde_json::json!({"username": "alice", "password": "x"})),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(err["code"], serde_json::json!("BAD_USERNAME_OR_PWD"));
        assert!(err.get("token").is_none());
        assert_eq!(state.sso_backends.pending_len(), 0);
    }

    #[test]
    fn pending_table_is_bounded_regardless_of_attempt_rate() {
        let store = SsoStore::new();
        const { assert!(MAX_PENDING_SSO_LOGINS > 0) };
        assert!(!PENDING_SSO_LOGIN_TTL.is_zero());
        for index in 0..(MAX_PENDING_SSO_LOGINS + 10) {
            store.record_login_attempt("w233-ldap", &format!("user-{index}"));
        }
        assert_eq!(store.pending_len(), MAX_PENDING_SSO_LOGINS);
    }
}
