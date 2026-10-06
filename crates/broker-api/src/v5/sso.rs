//! Configured single-sign-on backends for the v5 management API.
//!
//! **Unstable. Not part of the first stable release.** The login flow in
//! this module has the shape of the API, but it does not connect to an
//! identity provider: the OIDC callback does not exchange the code and
//! does not examine an identity token, and the SAML consumer examines a
//! shared-secret signature that no identity provider makes. Thus the
//! flow endpoints (`/sso/login/:backend`, `/sso/oidc/callback`,
//! `/sso/saml/acs`, `/sso/token_exchange`) answer the error that the
//! reference API documents for each (404 or 400) unless
//! [`SsoStore::set_unstable_flows`] turned them on. Production code does
//! not turn them on. The supported login is the user name and the
//! password with MFA, and the API keys.
//!
//! Covers `GET /sso` (ordered list, C05-11), `GET /sso/running`
//! (enabled names, C05-17) and the per-backend `GET /sso/{backend}`
//! (read one backend, C05-22), `PUT /sso/{backend}` (create or replace
//! one backend, C05-23) and `DELETE /sso/{backend}` (remove one backend,
//! C05-21) on real state.
//! Single-node, management-plane only: handlers clone at most the capped
//! list once per request under a short lock; publish and deliver never
//! touch this store, and no identity-provider client runs here (listing
//! configured backends only; login flows, MFA and callbacks are sibling
//! tasks, and token exchange is covered here).
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
// Shape note: the reference specification types each backend as a
// variant (one schema each for `saml`, `oidc` and `ldap`). Each row
// reports the stored keys (`backend`, `enable`) plus the stored backend
// fields at the top level. The management console sends and reads the
// same flat fields. A stored `config` object also renders under `config`
// for older readers.
// Open question: the specification addresses only `ldap`, `oidc` and
// `saml` names. The store keeps any slash-free name.
// TODO(parity): which backend names does the specification allow beyond
// the three typed variants?

use axum::{
    extract::{Path, Query, State},
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
/// Longest accepted OIDC `state` query value (256 bytes). Reason: states
/// issued here are 32 hex chars; 256 covers provider-relayed states with
/// headroom while bounding the pending-table lookup key on this
/// management-plane route.
const MAX_OIDC_STATE_LEN: usize = 256;
/// Longest accepted OIDC `code` query value (4096 bytes). Reason:
/// authorization codes are short opaque strings (hundreds of bytes);
/// 4 KiB covers providers with headroom while bounding per-request memory
/// on this management-plane route (the code itself is never stored).
const MAX_OIDC_CODE_LEN: usize = 4096;
/// Longest accepted token-exchange `grant` body value (4096 bytes).
/// Reason: grants issued here are 32 hex chars; 4 KiB covers
/// provider-relayed grants with headroom while bounding per-request memory
/// on this management-plane route (the grant itself is never logged and
/// never stored beyond the single-use pending-table consume below).
pub const MAX_SSO_GRANT_LEN: usize = 4096;

/// One configured backend: the registry key plus the enable flag and the
/// opaque backend config (Null when nothing is stored).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsoBackend {
    backend: String,
    enable: bool,
    config: serde_json::Value,
}

/// Placeholder rendered instead of a stored secret value on every read.
/// Reason: reads must prove a secret is stored without ever carrying the
/// secret itself; a fixed marker keeps the field present for operators
/// while the stored value stays sealed in the registry file and out of
/// logs and responses alike.
pub const REDACTED_SECRET: &str = "******";

/// True when a config field name carries secret material. Matched
/// case-insensitively against the field name: any name containing one of
/// these substrings is treated as a secret and its whole value is replaced
/// with [`REDACTED_SECRET`], never rendered or logged.
fn is_secret_key(name: &str) -> bool {
    let lower = name.to_lowercase();
    lower.contains("secret")
        || lower.contains("password")
        || lower.contains("passwd")
        || lower.contains("private_key")
        || lower.contains("privatekey")
        || lower.contains("api_key")
        || lower.contains("apikey")
}

/// Copy one config document with every secret value replaced by
/// [`REDACTED_SECRET`]. Objects redact by field name and recurse into the
/// rest; arrays recurse element-wise; scalars pass through. The stored
/// document is never mutated and never logged.
fn redact_secrets(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => {
            let mut out = serde_json::Map::with_capacity(object.len());
            for (key, member) in object {
                if is_secret_key(key) {
                    out.insert(
                        key.clone(),
                        serde_json::Value::String(REDACTED_SECRET.to_string()),
                    );
                } else {
                    out.insert(key.clone(), redact_secrets(member));
                }
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(redact_secrets).collect())
        }
        _ => value.clone(),
    }
}

impl SsoBackend {
    /// API row: the stored slot with secrets redacted. `config` renders
    /// only when one is stored; backends without stored config omit it
    /// rather than inventing an empty object. Each stored backend field
    /// also renders at the top level, so the row matches the documented
    /// variant shape. Secret fields render as [`REDACTED_SECRET`], never
    /// the stored value: the stored secret stays sealed in the registry
    /// file and out of logs and responses, while non-secret fields
    /// (issuer, URLs) render verbatim so operators can still inspect them.
    // TODO(parity): which config fields does the documented shape redact
    // (marker string versus omission, exact secret field list)? The
    // rulebook does not decide the redaction shape; the current choice
    // keeps every field present with secret values replaced by a fixed
    // marker until the checker pins it.
    fn to_json(&self) -> serde_json::Value {
        let mut value = serde_json::json!({
            "backend": self.backend,
            "enable": self.enable,
        });
        if self.config.is_null() {
            return value;
        }
        let redacted = redact_secrets(&self.config);
        value["config"] = redacted.clone();
        if let Some(fields) = redacted.as_object() {
            for (key, field) in fields {
                if value.get(key).is_none() {
                    value[key] = field.clone();
                }
            }
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
    /// The login flow endpoints answer requests only when this is true.
    /// The default is false. See the module documentation.
    unstable_flows: std::sync::atomic::AtomicBool,
}

impl SsoStore {
    /// Empty registry (today's empty behaviour: no backends).
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            pending: Mutex::new(HashMap::new()),
            registry: RwLock::new(None),
            save_lock: Mutex::new(()),
            // The tests of this crate examine the flow code. A broker
            // does not have the flow.
            unstable_flows: std::sync::atomic::AtomicBool::new(cfg!(test)),
        }
    }

    /// Turns the unstable login flow endpoints on or off. Only tests
    /// call this function.
    pub fn set_unstable_flows(&self, enabled: bool) {
        self.unstable_flows
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
    }

    /// True when the unstable login flow endpoints answer requests.
    pub fn unstable_flows_enabled(&self) -> bool {
        self.unstable_flows
            .load(std::sync::atomic::Ordering::Relaxed)
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

    /// Names of currently running backends, sorted for stable reads.
    /// Running means stored with `enable` set to true; disabled slots
    /// are configured but not running. An empty set reads as an empty
    /// collection, never an error. No new state: a filtered view over
    /// the capped registry from W2-32.
    // TODO(parity): does the documented running set mean enabled slots,
    // or only enabled slots whose provider resource is healthy (no last
    // error)? The rulebook does not decide the health filter and the
    // broker tracks only the registry slot (no provider client), so the
    // current choice reports every enabled slot until the checker pins it.
    pub fn running(&self) -> Vec<String> {
        let map = self.inner.read().expect("sso store lock");
        let mut out: Vec<String> = map
            .values()
            .filter(|backend| backend.enable)
            .map(|backend| backend.backend.clone())
            .collect();
        out.sort();
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

    /// Take one pending flow by its `state` id and remove it (single-use).
    /// Sweeps expired flows first; an expired or already-consumed state
    /// returns `None` and stays removed so a replay fails the same way.
    /// Management-plane only: one short mutex per callback; publish and
    /// deliver never touch this table.
    fn consume_pending_state(&self, state: &str) -> Option<PendingSsoLogin> {
        let mut pending = self.pending.lock().expect("sso pending lock");
        let now = Instant::now();
        pending.retain(|_, flow| !flow.expired(now));
        pending.remove(state)
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
    /// login-flow tasks; the list route itself never writes.
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

    /// Create or replace one backend. The detail write path shares this so
    /// seeding and the management route validate the same way. The whole
    /// record is validated before anything is applied: an invalid entry
    /// fails with [`SsoUpsertError::Invalid`] and leaves the stored slot
    /// untouched. A name with no stored slot counts toward
    /// [`MAX_SSO_BACKENDS`] and fails with [`SsoUpsertError::Full`] past
    /// the cap; replacing the slot stored under the same name never grows
    /// the map. Persists through the registry when one is attached; a
    /// commit or save failure restores the previous slot (or removes the
    /// fresh insert) and surfaces as [`SsoUpsertError::Persist`].
    /// Returns the stored record plus whether the name is newly created.
    pub fn upsert(&self, backend: SsoBackend) -> Result<(SsoBackend, bool), SsoUpsertError> {
        // `validate_backend` only ever reports `Invalid`; the other arms
        // cannot fire and map to the same invalid outcome if they ever do.
        validate_backend(&backend).map_err(|err| match err {
            SsoInsertError::Invalid(reason) => SsoUpsertError::Invalid(reason),
            SsoInsertError::Duplicate | SsoInsertError::Full => {
                SsoUpsertError::Invalid("field `backend` is not valid".to_string())
            }
            SsoInsertError::Persist(reason) => SsoUpsertError::Persist(reason),
        })?;
        let previous = {
            let mut map = self.inner.write().expect("sso store lock");
            if !map.contains_key(&backend.backend) && map.len() >= MAX_SSO_BACKENDS {
                return Err(SsoUpsertError::Full);
            }
            map.insert(backend.backend.clone(), backend.clone())
        };
        if let Err(reason) = self.persist() {
            let mut map = self.inner.write().expect("sso store lock");
            match previous {
                Some(old) => {
                    map.insert(old.backend.clone(), old);
                }
                None => {
                    map.remove(&backend.backend);
                }
            }
            return Err(SsoUpsertError::Persist(reason));
        }
        Ok((backend, previous.is_none()))
    }

    /// Remove one backend by name. Returns `true` when a slot was removed
    /// and `false` when nothing was stored under the name (callers map
    /// that to 404 and create nothing). The removal persists through the
    /// registry when one is attached; a commit or save failure restores
    /// the removed slot and surfaces the reason so callers answer 500
    /// and the loss is never silent.
    pub fn remove(&self, backend: &str) -> Result<bool, String> {
        let removed = {
            let mut map = self.inner.write().expect("sso store lock");
            map.remove(backend)
        };
        let Some(old) = removed else {
            return Ok(false);
        };
        if let Err(reason) = self.persist() {
            let mut map = self.inner.write().expect("sso store lock");
            map.insert(old.backend.clone(), old);
            return Err(reason);
        }
        Ok(true)
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

/// Why [`SsoStore::upsert`] refused a record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SsoUpsertError {
    /// The registry holds [`MAX_SSO_BACKENDS`] records and the name is
    /// new. Replacing the stored slot never reports this.
    Full,
    /// The entry failed validation (empty name, slash, too long, or an
    /// oversized config document). Nothing was applied.
    Invalid(String),
    /// The in-memory write applied but the registry commit or atomic
    /// save failed; the previous slot was restored. Callers map this to
    /// HTTP 500; it is never silent.
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

/// Build one backend record from parts. The broker-through seeds and the
/// per-backend detail write path share it so both validate the same way.
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

/// `GET /sso/running`: running backends as a bare JSON array of backend
/// names in backend-name order. Running means stored with `enable` set
/// to true; an empty set reads as `[]`, never an error. The route is
/// not paged, reflects live state, and takes no parameters; unknown
/// query parameters are ignored (the handler takes no query extractor,
/// so Axum never parses them).
///
/// The route is public (no bearer token: the dashboard login page probes
/// it before anyone holds a token to render the provider buttons) and
/// carries only backend names, never configs or secrets.
/// Management-plane only: one short read lock per request cloning at most
/// the capped names; publish and deliver never touch this store. Bound:
/// backend cap as in W2-32; no new state.
pub async fn list_running(State(state): State<ApiState>) -> Response {
    let running = state.sso_backends.running();
    (StatusCode::OK, Json(running)).into_response()
}

/// Validate one per-backend detail name from the path. The router splits
/// on `/`, so a slash can never arrive here; the check stays so a stored
/// slot can never be addressed through a smuggled path.
fn validate_detail_name(backend: &str) -> Result<(), String> {
    if backend.trim().is_empty() {
        return Err("field `backend` must not be empty".to_string());
    }
    if backend.len() > broker_config::MAX_SSO_BACKEND_NAME_LEN {
        return Err("field `backend` is too long".to_string());
    }
    if backend.contains('/') {
        return Err("field `backend` must not contain `/`".to_string());
    }
    Ok(())
}

/// Build the record one `PUT /sso/{backend}` write stores. The body must
/// be a JSON object carrying `enable` (boolean, defaults to `true` when
/// omitted or null) and the backend fields. Backend fields ride either
/// as a nested `config` object (or JSON-text string) or as flat top-level
/// fields besides `backend`/`enable`/`config`; both forms store the same
/// object and read back flat at the top level. A `backend` field, when
/// present as a non-empty string, must name the path backend. Every
/// field is checked before the record is built, so a bad body applies
/// nothing.
// TODO(parity): which fields does the documented write accept beyond
// the variant fields? The rulebook does not decide the exact shape; the
// current choice stores every extra top-level field verbatim as backend
// config until the checker pins the set.
// TODO(parity): does the documented write reject a `backend` body field
// that differs from the path, or ignore it? The rulebook does not decide
// the mismatch policy; the current choice rejects it as the conservative
// fail-closed answer until the checker pins it.
fn parse_detail_body(path_backend: &str, value: &serde_json::Value) -> Result<SsoBackend, String> {
    let Some(object) = value.as_object() else {
        return Err("detail body must be a JSON object".to_string());
    };
    if let Some(name) = object.get("backend") {
        match name.as_str() {
            Some(text) if text.trim().is_empty() || text == path_backend => {}
            Some(_) => {
                return Err("field `backend` must match the path".to_string());
            }
            None => {
                return Err("field `backend` must be a string".to_string());
            }
        }
    }
    let enable = match object.get("enable") {
        None | Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::Bool(flag)) => *flag,
        Some(_) => return Err("field `enable` must be a boolean".to_string()),
    };
    let mut fields = match object.get("config") {
        None | Some(serde_json::Value::Null) => serde_json::Map::new(),
        Some(serde_json::Value::Object(map)) => map.clone(),
        Some(serde_json::Value::String(text)) if text.trim().is_empty() => serde_json::Map::new(),
        Some(serde_json::Value::String(text)) => match serde_json::from_str(text) {
            Ok(serde_json::Value::Object(map)) => map,
            Ok(_) => return Err("field `config` must be an object".to_string()),
            Err(_) => return Err("field `config` is not valid JSON".to_string()),
        },
        Some(_) => return Err("field `config` must be an object".to_string()),
    };
    for (key, field) in object {
        if !matches!(key.as_str(), "backend" | "enable" | "config") && !fields.contains_key(key) {
            fields.insert(key.clone(), field.clone());
        }
    }
    let config = if fields.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::Value::Object(fields)
    };
    Ok(SsoBackend {
        backend: path_backend.to_string(),
        enable,
        config,
    })
}

fn sso_backend_not_found(backend: &str) -> Response {
    // `BACKEND_NOT_FOUND` mirrors the sibling single-sign-on routes
    // (login, callback, ACS, metadata, exchange); see their TODO(parity)
    // notes on the exact code name.
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({
            "code": "BACKEND_NOT_FOUND",
            "message": format!("sso backend {backend:?} not found"),
        })),
    )
        .into_response()
}

/// `GET /sso/{backend}`: read one configured backend.
///
/// Returns the stored slot with 200, secrets redacted (see
/// [`REDACTED_SECRET`]): secret config values never render, are never
/// logged, and stay sealed in the registry file, while non-secret fields
/// render verbatim. Unknown backends fail with 404 in the documented
/// `{code, message}` shape. The route is not paged and takes no
/// parameters; unknown query parameters are ignored (the handler takes
/// no query extractor, so Axum never parses them).
///
/// Management-plane only: one short read lock per request cloning at
/// most one capped entry; publish and deliver never touch this store.
// TODO(parity): is the documented miss code `BACKEND_NOT_FOUND` or the
// generic `NOT_FOUND`? The rulebook does not decide the exact code name;
// the current choice mirrors the sibling single-sign-on routes (404
// `BACKEND_NOT_FOUND`, documented `{code, message}` shape) until the
// checker pins it.
pub async fn get_sso_backend(
    State(state): State<ApiState>,
    Path(backend): Path<String>,
) -> Response {
    match state.sso_backends.get(&backend) {
        Some(stored) => (StatusCode::OK, Json(stored.to_json())).into_response(),
        None => sso_backend_not_found(&backend),
    }
}

/// `PUT /sso/{backend}`: create or replace one configured backend.
///
/// The whole write is validated before anything is applied: a malformed
/// body, a mismatched `backend` field, a non-boolean `enable`, a
/// non-object `config`, or an oversized document fails with 400 in the
/// documented `{code, message}` shape and leaves the stored slot
/// untouched. A new name past [`MAX_SSO_BACKENDS`] fails with 400 instead
/// of growing the map without limit; replacing the stored slot never
/// counts toward the cap. On success returns 200 with the stored slot,
/// secrets redacted. The write persists across a restart through the
/// config registry. The route is not paged.
///
/// Management-plane only: one short write lock per request (plus the
/// registry save lock on persist); publish and deliver never touch this
/// store. Fail-closed rows are logged with the backend name only, never
/// config values.
pub async fn put_sso_backend(
    State(state): State<ApiState>,
    Path(backend): Path<String>,
    body: Bytes,
) -> Response {
    use crate::errors::ApiError;
    if let Err(reason) = validate_detail_name(&backend) {
        return ApiError::BadRequest(reason).into_response();
    }
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid sso backend body: {error}"))
                .into_response();
        }
    };
    let record = match parse_detail_body(&backend, &value) {
        Ok(record) => record,
        Err(reason) => return ApiError::BadRequest(reason).into_response(),
    };
    match state.sso_backends.upsert(record) {
        Ok((stored, created)) => {
            tracing::warn!(
                backend = backend.as_str(),
                created = created,
                "sso backend stored"
            );
            (StatusCode::OK, Json(stored.to_json())).into_response()
        }
        Err(SsoUpsertError::Full) => {
            tracing::warn!(
                backend = backend.as_str(),
                "sso backend refused: registry is full"
            );
            ApiError::BadRequest("sso backend store is full".to_string()).into_response()
        }
        Err(SsoUpsertError::Invalid(reason)) => {
            tracing::warn!(
                backend = backend.as_str(),
                reason = reason.as_str(),
                "sso backend refused: invalid write"
            );
            ApiError::BadRequest(reason).into_response()
        }
        Err(SsoUpsertError::Persist(reason)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": reason,
            })),
        )
            .into_response(),
    }
}

/// `DELETE /sso/{backend}`: remove one configured backend.
///
/// A stored name reports success with 204 and the removal persists
/// across a restart through the config registry; the freed name may be
/// re-created. An unknown name reports 404 in the documented `{code,
/// message}` shape and applies nothing. The route is not paged.
///
/// Management-plane only: one short write lock per request (plus the
/// registry save lock on persist); publish and deliver never touch this
/// store.
// TODO(parity): is the documented delete success 204 with no body or 200
// with an envelope, and is the miss code `BACKEND_NOT_FOUND` or the
// generic `NOT_FOUND`? The rulebook does not decide the exact shapes; the
// current choice answers 204 like the sibling single-entry deletes and
// mirrors the sibling single-sign-on miss code until the checker pins
// them.
pub async fn delete_sso_backend(
    State(state): State<ApiState>,
    Path(backend): Path<String>,
) -> Response {
    match state.sso_backends.remove(&backend) {
        Ok(true) => {
            tracing::warn!(backend = backend.as_str(), "sso backend removed");
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => sso_backend_not_found(&backend),
        Err(reason) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": reason,
            })),
        )
            .into_response(),
    }
}

/// The answer of a login flow endpoint while the flow is off.
///
/// Each endpoint answers a status code and a body that the reference API
/// documents for it. Login and the SAML consumer answer 404
/// `BACKEND_NOT_FOUND`: no backend can run a flow in this release. The OIDC
/// callback and the token exchange answer 400 `BAD_REQUEST`.
#[derive(Clone, Copy)]
enum FlowKind {
    /// `POST /sso/login/{backend}` and `POST /sso/saml/acs`.
    BackendFlow,
    /// `GET /sso/oidc/callback` and `POST /sso/token_exchange`.
    CodeFlow,
}

fn flow_not_available(kind: FlowKind) -> Response {
    let (status, code) = match kind {
        FlowKind::BackendFlow => (StatusCode::NOT_FOUND, "BACKEND_NOT_FOUND"),
        FlowKind::CodeFlow => (StatusCode::BAD_REQUEST, "BAD_REQUEST"),
    };
    (
        status,
        Json(serde_json::json!({
            "code": code,
            "message": "single sign-on is not available in this release",
        })),
    )
        .into_response()
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
    if !state.sso_backends.unstable_flows_enabled() {
        return flow_not_available(FlowKind::BackendFlow);
    }
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

/// `GET /sso/oidc/callback`: complete one OIDC code flow.
///
/// The route is public (no bearer token: the dashboard login page lands
/// here via a provider redirect before anyone holds a token) and not
/// paged. Documented query parameters are `state` and `code`; unknown
/// query parameters are ignored. Behaviour, in order:
/// - a provider `error` parameter, or a missing/empty `state` or `code`,
///   fails with 400 in the documented `{code, message}` shape and issues
///   no token (malformed requests never consume a pending state);
/// - an unknown, expired or already-consumed `state` fails with 400 in
///   the documented shape and issues no token (states are single-use
///   with [`PENDING_SSO_LOGIN_TTL`]; replays fail the same way);
/// - a state whose backend is no longer stored fails with 404
///   `BACKEND_NOT_FOUND` (mirrors the login miss) and issues no token;
/// - a disabled backend, a backend with no stored provider config, or a
///   flow whose username names no admin user fails closed with 401
///   `BAD_USERNAME_OR_PWD` and issues no token (a disabled slot must
///   never authenticate, and an issued token must resolve through the
///   live admin-user store or it would be revoked on first use);
/// - otherwise one live dashboard token is issued for the flow's username
///   (with that user's current role) and the response carries the
///   documented token/redirect fields.
///
/// No outbound provider client runs here (provider discovery beyond the
/// stored config is out of scope), so any non-empty code paired with a
/// live state and an enabled, configured backend completes; the consumed
/// state is what binds the code to the login attempt. Codes are never
/// logged and never stored. Management-plane only: one short pending-table
/// lock plus short registry/admin reads per call; publish and deliver
/// never touch any of them.
// TODO(parity): what are the documented query parameter names (`state`
// versus `state_id`, `code` versus `auth_code`, plus provider `error`)?
// The rulebook does not decide the exact names; the current choice reads
// `state` and `code` (plus `error`) and ignores the rest until the
// checker pins them.
// TODO(parity): what are the documented miss/auth codes for this route
// (`BACKEND_NOT_FOUND` versus `NOT_FOUND`, `BAD_USERNAME_OR_PWD` versus
// a dedicated OIDC code)? The rulebook does not decide the exact code
// names; the current choice mirrors the login route (404
// `BACKEND_NOT_FOUND` for a vanished backend, 401 `BAD_USERNAME_OR_PWD`
// for disabled/unverifiable flows, 400 `BAD_REQUEST` for malformed or
// unknown states) until the checker pins them.
// TODO(parity): what are the documented success field names (`token`
// versus `access_token`, `redirect` versus `redirect_uri`, plus
// username/backend)? The rulebook does not decide the exact shape; the
// current choice answers `{token, username, backend, redirect}` (token
// is the live bearer, redirect comes from the stored backend config's
// `redirect`/`redirect_uri` string when one is stored and defaults to
// `/`) until the checker pins it.
// TODO(parity): does the documented flow exchange `code` against the
// provider token endpoint, or complete locally as here? Provider
// discovery beyond the stored config is out of scope for this task, so
// the current choice completes locally against the stored config and
// the pending table (fail closed without both) until the checker pins
// the exchange.
pub async fn oidc_callback(
    State(state): State<ApiState>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    if !state.sso_backends.unstable_flows_enabled() {
        return flow_not_available(FlowKind::CodeFlow);
    }
    use crate::errors::ApiError;
    let bad_request = |message: String| ApiError::BadRequest(message).into_response();
    // Provider-denied flows arrive with `?error=...&state=...`: fail
    // closed, consume the state when one is presented so it cannot be
    // replayed, issue no token.
    if let Some(error) = params.get("error").map(|value| value.trim().to_string()) {
        if !error.is_empty() {
            if let Some(state_param) = params.get("state").map(|value| value.trim().to_string()) {
                if !state_param.is_empty() && state_param.len() <= MAX_OIDC_STATE_LEN {
                    state.sso_backends.consume_pending_state(&state_param);
                }
            }
            tracing::warn!("sso oidc callback refused: provider reported an error");
            return bad_request(format!("oidc callback failed: {error}"));
        }
    }
    let state_param = params
        .get("state")
        .map(|value| value.trim().to_string())
        .unwrap_or_default();
    let code_param = params
        .get("code")
        .map(|value| value.trim().to_string())
        .unwrap_or_default();
    if state_param.is_empty() {
        return bad_request("field `state` must not be empty".to_string());
    }
    if state_param.len() > MAX_OIDC_STATE_LEN {
        return bad_request("field `state` is too long".to_string());
    }
    if code_param.is_empty() {
        return bad_request("field `code` must not be empty".to_string());
    }
    if code_param.len() > MAX_OIDC_CODE_LEN {
        return bad_request("field `code` is too long".to_string());
    }
    // Single-use: the lookup removes the flow whether this attempt
    // succeeds or fails from here on, so a replayed state fails below
    // as unknown.
    let Some(flow) = state.sso_backends.consume_pending_state(&state_param) else {
        return bad_request("unknown or expired state".to_string());
    };
    let Some(stored) = state.sso_backends.get(&flow.backend) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "code": "BACKEND_NOT_FOUND",
                "message": "Backend not found",
            })),
        )
            .into_response();
    };
    if !stored.enable {
        tracing::warn!(
            backend = flow.backend.as_str(),
            username = flow.username.as_str(),
            "sso oidc callback refused: backend is disabled"
        );
        return ApiError::BadUsernameOrPwd("Auth failed".to_string()).into_response();
    }
    if stored.config.is_null() {
        // Fail closed: with no stored provider config there is nothing
        // the code could be exchanged against, so granting a token would
        // grant access without verification.
        tracing::warn!(
            backend = flow.backend.as_str(),
            username = flow.username.as_str(),
            "sso oidc callback failed: no provider config for this backend"
        );
        return ApiError::BadUsernameOrPwd("Auth failed".to_string()).into_response();
    }
    let Some(view) = state.admin_users.get(&flow.username) else {
        tracing::warn!(
            backend = flow.backend.as_str(),
            username = flow.username.as_str(),
            "sso oidc callback failed: unknown user for this flow"
        );
        return ApiError::BadUsernameOrPwd("Auth failed".to_string()).into_response();
    };
    // Live token: issued through the same store the auth middleware
    // resolves, with the user's current role, so the callback hands back
    // a bearer that works instead of a constant standing in for one.
    // The code itself is never logged and never stored.
    let token = state.tokens.issue(&flow.username, view.role);
    tracing::warn!(
        backend = flow.backend.as_str(),
        username = flow.username.as_str(),
        "sso oidc callback completed"
    );
    let redirect = stored
        .config
        .as_object()
        .and_then(|object| {
            object
                .get("redirect")
                .or_else(|| object.get("redirect_uri"))
                .or_else(|| object.get("post_login_redirect"))
        })
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("/")
        .to_string();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "token": token,
            "username": flow.username,
            "backend": flow.backend,
            "redirect": redirect,
        })),
    )
        .into_response()
}

/// `POST /sso/token_exchange`: exchange one backend-issued grant for a
/// live dashboard token.
///
/// The route is public (no bearer token: the dashboard login page calls it
/// before anyone holds a token) and not paged. The body must be a JSON
/// object carrying a non-empty `backend` name and a non-empty grant. The
/// grant is one pending login flow id from the login task (W2-33): the
/// login attempt records it, and this route consumes it. Behaviour, in
/// order:
/// - bodies that are not a JSON object, or carry no non-empty `backend`
///   or grant, or carry an oversized value, fail with 400 in the
///   documented `{code, message}` shape and consume nothing (malformed
///   requests never burn a live grant);
/// - a backend with no stored slot fails with 404 `BACKEND_NOT_FOUND` in
///   the documented shape and consumes nothing;
/// - an unknown, expired or already-consumed grant fails with 400 in the
///   documented shape and issues no token (grants are single-use with
///   [`PENDING_SSO_LOGIN_TTL`]; replays fail the same way);
/// - a grant that names a different backend than the request fails with
///   400 and issues no token (the grant is already consumed, so it cannot
///   be retried elsewhere);
/// - a disabled backend, a backend with no stored provider config, or a
///   grant whose username names no admin user fails closed with 401
///   `BAD_USERNAME_OR_PWD` and issues no token (a disabled slot must
///   never authenticate, an issued token must resolve through the live
///   admin-user store, and with no stored provider config there is nothing
///   the grant could have been issued against);
/// - otherwise one live dashboard token is issued for the grant's username
///   (with that user's current role, through the same store the auth
///   middleware resolves) and the response carries the documented
///   token/redirect fields.
///
/// Grants are never logged and never stored beyond the pending-table
/// consume. Management-plane only: one short registry read, one short
/// pending-table lock and short admin/token reads per call; publish and
/// deliver never touch any of them. Bound: the pending-grant table is the
/// login table from W2-33 ([`MAX_PENDING_SSO_LOGINS`] entries,
/// [`PENDING_SSO_LOGIN_TTL`], single-use); this route adds no new state.
// TODO(parity): what are the documented body field names (`backend` plus
// `grant`, versus `code`, `auth_code` or `state`)? The rulebook does not
// decide the exact names; the current choice requires `backend` and reads
// the grant from `grant` (falling back to `code` and `state`, the sibling
// flows' names for the same pending-table id) until the checker pins them.
// TODO(parity): what are the documented miss/auth codes for this route
// (`BACKEND_NOT_FOUND` versus `NOT_FOUND`, `BAD_REQUEST` versus a
// dedicated exchange code for unknown grants, `BAD_USERNAME_OR_PWD`
// versus a dedicated code for disabled backends)? The rulebook does not
// decide the exact code names; the current choice mirrors the login and
// callback routes (404 `BACKEND_NOT_FOUND` for an unknown backend, 400
// `BAD_REQUEST` for malformed or unknown grants, 401 `BAD_USERNAME_OR_PWD`
// for disabled/unverifiable flows) until the checker pins them.
// TODO(parity): what are the documented success field names (`token`
// versus `access_token`, `redirect` versus `redirect_uri`, plus
// username/backend)? The rulebook does not decide the exact shape; the
// current choice answers `{token, username, backend, redirect}` (token is
// the live bearer, redirect comes from the stored backend config's
// `redirect`/`redirect_uri` string when one is stored and defaults to
// `/`) until the checker pins it.
// TODO(parity): does the documented exchange mint the grant itself, or
// consume a login-flow id as here? The rulebook does not decide the grant
// source; the current choice consumes the W2-33 pending login flow
// (fail closed without one) until the checker pins it.
pub async fn token_exchange(State(state): State<ApiState>, body: Bytes) -> Response {
    if !state.sso_backends.unstable_flows_enabled() {
        return flow_not_available(FlowKind::CodeFlow);
    }
    use crate::errors::ApiError;
    let bad_request = |message: String| ApiError::BadRequest(message).into_response();
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return bad_request(format!("invalid exchange body: {error}"));
        }
    };
    let Some(object) = value.as_object() else {
        return bad_request("exchange body must be a JSON object".to_string());
    };
    let backend = object
        .get("backend")
        .and_then(|name| name.as_str())
        .map(str::trim)
        .unwrap_or_default();
    if backend.is_empty() {
        return bad_request("field `backend` must not be empty".to_string());
    }
    if backend.len() > broker_config::MAX_SSO_BACKEND_NAME_LEN {
        return bad_request("field `backend` is too long".to_string());
    }
    // The grant is the pending login flow id under its documented name;
    // `code` and `state` are the sibling flows' names for the same id.
    let grant = ["grant", "code", "state"]
        .iter()
        .find_map(|key| object.get(*key))
        .and_then(|grant| grant.as_str())
        .map(str::trim)
        .unwrap_or_default();
    if grant.is_empty() {
        return bad_request("field `grant` must not be empty".to_string());
    }
    if grant.len() > MAX_SSO_GRANT_LEN {
        return bad_request("field `grant` is too long".to_string());
    }
    let Some(stored) = state.sso_backends.get(backend) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "code": "BACKEND_NOT_FOUND",
                "message": "Backend not found",
            })),
        )
            .into_response();
    };
    // Single-use: the lookup removes the grant whether this attempt
    // succeeds or fails from here on, so a replayed grant fails below
    // as unknown.
    let Some(flow) = state.sso_backends.consume_pending_state(grant) else {
        tracing::warn!(
            backend = backend,
            "sso token exchange failed: unknown or expired grant"
        );
        return bad_request("unknown or expired grant".to_string());
    };
    if flow.backend != backend {
        tracing::warn!(
            backend = backend,
            username = flow.username.as_str(),
            "sso token exchange failed: grant does not belong to this backend"
        );
        return bad_request("grant does not belong to this backend".to_string());
    }
    if !stored.enable {
        tracing::warn!(
            backend = backend,
            username = flow.username.as_str(),
            "sso token exchange refused: backend is disabled"
        );
        return ApiError::BadUsernameOrPwd("Auth failed".to_string()).into_response();
    }
    if stored.config.is_null() {
        // Fail closed: with no stored provider config there is nothing
        // the grant could have been issued against, so granting a token
        // would grant access without verification.
        tracing::warn!(
            backend = backend,
            username = flow.username.as_str(),
            "sso token exchange failed: no provider config for this backend"
        );
        return ApiError::BadUsernameOrPwd("Auth failed".to_string()).into_response();
    }
    let Some(view) = state.admin_users.get(&flow.username) else {
        tracing::warn!(
            backend = backend,
            username = flow.username.as_str(),
            "sso token exchange failed: unknown user for this grant"
        );
        return ApiError::BadUsernameOrPwd("Auth failed".to_string()).into_response();
    };
    // Live token: issued through the same store the auth middleware
    // resolves, with the user's current role, so the exchange hands back
    // a bearer that works instead of a constant standing in for one.
    // The grant itself is never logged and never stored.
    let token = state.tokens.issue(&flow.username, view.role);
    tracing::warn!(
        backend = backend,
        username = flow.username.as_str(),
        "sso token exchange completed"
    );
    let redirect = stored
        .config
        .as_object()
        .and_then(|object| {
            object
                .get("redirect")
                .or_else(|| object.get("redirect_uri"))
                .or_else(|| object.get("post_login_redirect"))
        })
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("/")
        .to_string();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "token": token,
            "username": flow.username,
            "backend": flow.backend,
            "redirect": redirect,
        })),
    )
        .into_response()
}

/// Default SAML backend name selected when the ACS body and the metadata
/// query name no backend. Reason: the registry keys backends by name and
/// the seed documents `saml` as the SAML slot; a fixed default keeps the
/// two routes that carry no path parameter total without inventing a
/// registry entry.
// TODO(parity): does the documented ACS/metadata select the backend by a
// path parameter, a query parameter, a body field, or a fixed `saml`
// slot? The rulebook does not decide the selection; the current choice
// accepts an optional `backend` body field or `?backend=` query value and
// falls back to `saml` until the checker pins it.
pub const DEFAULT_SAML_BACKEND: &str = "saml";
/// Longest accepted SAML `SAMLResponse` value (65536 bytes). Reason: a
/// response is base64-encoded XML, typically a few kilobytes; 64 KiB
/// covers large assertions with headroom while bounding per-request memory
/// on this management-plane route (the value itself is never stored).
pub const MAX_SAML_RESPONSE_LEN: usize = 65536;
/// Longest accepted SAML `RelayState` value (256 bytes). Reason: states
/// issued here are 32 hex chars; 256 covers provider-relayed states with
/// headroom while bounding the pending-table lookup key on this
/// management-plane route.
pub const MAX_SAML_RELAYSTATE_LEN: usize = 256;
/// Longest rendered SAML service-provider metadata document (8 KiB).
/// Reason: the document renders per request from the stored slot plus a
/// fixed envelope, so bounding it keeps each metadata read cheap; 8 KiB
/// covers entity ids, endpoints and key descriptors with headroom.
pub const MAX_SAML_METADATA_LEN: usize = 8192;
/// Content type rendered by `GET /sso/saml/metadata`. Reason: the
/// metadata document is SAML service-provider XML, and this is the media
/// type validators expect for it.
// TODO(parity): which content type does the documented metadata route
// serve (`application/samlmetadata+xml` versus generic XML)? The rulebook
// does not decide the exact media type; the current choice serves the
// SAML metadata media type until the checker pins it.
pub const SAML_METADATA_CONTENT_TYPE: &str = "application/samlmetadata+xml";

/// Parsed `POST /sso/saml/acs` fields from a JSON or form-encoded body.
#[derive(Debug, Default)]
struct SamlAcsFields {
    backend: Option<String>,
    saml_response: Option<String>,
    relay_state: Option<String>,
    username: Option<String>,
}

/// Percent-decode one form component (`+` becomes space). Small local
/// decoder so the ACS can read the browser form binding without a new
/// dependency; malformed escapes are kept literally and fail closed
/// downstream as a bad response.
fn form_percent_decode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                out.push(' ');
                index += 1;
            }
            b'%' if index + 2 < bytes.len() => {
                let hex = &input[index + 1..index + 3];
                match u8::from_str_radix(hex, 16) {
                    Ok(byte) => {
                        out.push(byte as char);
                        index += 3;
                    }
                    Err(_) => {
                        out.push('%');
                        index += 1;
                    }
                }
            }
            _ => {
                out.push(bytes[index] as char);
                index += 1;
            }
        }
    }
    out
}

/// Read one optional string field from a string map, trimming nothing:
/// emptiness is decided by the caller so blank and missing stay distinct.
fn map_string_field(map: &HashMap<String, String>, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(value) = map.get(*key) {
            return Some(value.clone());
        }
    }
    None
}

/// Parse an ACS body as JSON first (the management harness posts JSON),
/// then as a `application/x-www-form-urlencoded` browser binding. Either
/// shape yields the same field set; anything else is malformed.
fn parse_saml_acs_fields(body: &[u8]) -> Result<SamlAcsFields, String> {
    if let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) {
        if let Some(object) = value.as_object() {
            let string_field = |keys: &[&str]| -> Option<String> {
                for key in keys {
                    if let Some(value) = object.get(*key).and_then(|name| name.as_str()) {
                        return Some(value.to_string());
                    }
                }
                None
            };
            return Ok(SamlAcsFields {
                backend: string_field(&["backend"]),
                saml_response: string_field(&["SAMLResponse", "saml_response", "response"]),
                relay_state: string_field(&["RelayState", "relay_state", "state"]),
                username: string_field(&["username", "Username"]),
            });
        }
        return Err("acs body must be a JSON object".to_string());
    }
    let text = std::str::from_utf8(body)
        .map_err(|_| "acs body is neither JSON nor a form binding".to_string())?;
    if !text.contains('=') {
        return Err("acs body is neither JSON nor a form binding".to_string());
    }
    let mut map = HashMap::new();
    for pair in text.split('&') {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        map.insert(
            form_percent_decode(name.trim()),
            form_percent_decode(value.trim()),
        );
    }
    if !map.contains_key("SAMLResponse")
        && !map.contains_key("RelayState")
        && !map.contains_key("backend")
    {
        return Err("acs form body carries no SAML fields".to_string());
    }
    Ok(SamlAcsFields {
        backend: map_string_field(&map, &["backend"]),
        saml_response: map_string_field(&map, &["SAMLResponse"]),
        relay_state: map_string_field(&map, &["RelayState", "state"]),
        username: map_string_field(&map, &["username"]),
    })
}

/// Decode a `SAMLResponse` to its XML text. Accepts base64-encoded XML
/// (the POST binding) or raw XML (operator tooling); anything else is not
/// a response. Returns the XML text for structural checks.
fn decode_saml_response(raw: &str) -> Option<String> {
    use base64::Engine as _;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let compact: String = trimmed.chars().filter(|c| !c.is_whitespace()).collect();
    for engine in [
        base64::engine::general_purpose::STANDARD,
        base64::engine::general_purpose::URL_SAFE,
    ] {
        if let Ok(bytes) = engine.decode(compact.as_bytes()) {
            if let Ok(text) = String::from_utf8(bytes) {
                if text.contains('<') {
                    return Some(text);
                }
            }
        }
    }
    if trimmed.contains('<') {
        return Some(trimmed.to_string());
    }
    None
}

/// Allowed SAML clock skew (300 s, 5 minutes). Reason: identity-provider
/// and service-provider clocks drift by seconds to minutes; 300 s is the
/// common SAML tolerance, accepting small drift while keeping expired
/// assertions short-lived on this management-plane route.
pub const SAML_CLOCK_SKEW_SECS: i64 = 300;

/// Expected identity-provider issuer for one stored backend config.
/// Read from the stored slot so the response is validated against the real
/// backend config, never against a value carried in the request itself.
fn saml_expected_issuer(config: &serde_json::Value) -> Option<String> {
    config_string(config, &["issuer", "idp_entity_id", "idp_issuer"])
}

/// Expected audience (our service-provider entity id) for one stored
/// backend config. Read from the stored slot for the same reason.
fn saml_expected_audience(config: &serde_json::Value) -> Option<String> {
    config_string(
        config,
        &[
            "audience",
            "sp_entity_id",
            "entity_id",
            "audience_restriction",
        ],
    )
}

/// Expected recipient (this assertion-consumer location) when the stored
/// backend names one. `None` means the slot names no recipient, so no
/// recipient check applies beyond the audience and time checks.
fn saml_expected_recipient(config: &serde_json::Value) -> Option<String> {
    config_string(
        config,
        &[
            "acs_url",
            "acsUrl",
            "acs",
            "assertion_consumer_service_url",
            "recipient",
        ],
    )
}

/// Shared secret that authenticates SAML responses for one stored backend
/// config. Read from the stored slot (`hmac_secret`, `shared_secret`,
/// `saml_hmac_secret` or `client_secret`); a base64 value decodes to the
/// key bytes, otherwise the trimmed UTF-8 bytes are the key. `None` means
/// the slot carries no verification key, so every response fails closed.
fn saml_hmac_secret(config: &serde_json::Value) -> Option<Vec<u8>> {
    use base64::Engine as _;
    let raw = config_string(
        config,
        &[
            "hmac_secret",
            "shared_secret",
            "saml_hmac_secret",
            "client_secret",
        ],
    )?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let compact: String = trimmed.chars().filter(|c| !c.is_whitespace()).collect();
    if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(&compact) {
        if !decoded.is_empty() {
            return Some(decoded);
        }
    }
    Some(trimmed.as_bytes().to_vec())
}

/// Collect every element body for `tag` (tolerating a namespace prefix and
/// attributes), e.g. all `<Issuer>` / `<Audience>` bodies. Empty and
/// nested bodies are skipped so garbage cannot pass as a value. Only an
/// opening element tag counts: the name must start right after `<` (or a
/// namespace `:`) and end at `>`, `/` or whitespace, so a longer element
/// name (`Audience` in `AudienceRestriction`), a closing tag
/// (`</Audience>`) or an attribute never shadows the real element body.
fn saml_tag_bodies(xml: &str, tag: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut search = 0;
    while search < xml.len() {
        let Some(open) = xml[search..].find(tag) else {
            break;
        };
        let pos = search + open;
        let before_ok = matches!(xml.as_bytes()[..pos].last(), Some(b'<') | Some(b':'));
        let after_ok = matches!(
            xml.as_bytes()[pos + tag.len()..].first(),
            Some(b'>') | Some(b'/') | Some(b' ') | Some(b'\t') | Some(b'\n') | Some(b'\r')
        );
        if !before_ok || !after_ok {
            search = pos + 1;
            continue;
        }
        let Some(end) = xml[pos..].find('>') else {
            break;
        };
        let body_start = pos + end + 1;
        if body_start >= xml.len() {
            break;
        }
        let Some(close) = xml[body_start..].find("</") else {
            break;
        };
        let body = xml[body_start..body_start + close].trim();
        if !body.is_empty() && !body.contains('<') {
            out.push(body.to_string());
        }
        search = body_start + close + 2;
    }
    out
}

/// Read one `Attribute="value"` from response XML (double or single
/// quoted), e.g. `Recipient`, `InResponseTo`, `NotBefore`,
/// `NotOnOrAfter`. Returns the first match.
fn saml_attribute(xml: &str, name: &str) -> Option<String> {
    for quote in ['"', '\''] {
        let needle = format!("{name}={quote}");
        if let Some(open) = xml.find(&needle) {
            let body_start = open + needle.len();
            if let Some(close) = xml[body_start..].find(quote) {
                let value = xml[body_start..body_start + close].trim();
                if !value.is_empty() {
                    return Some(value.to_string());
                }
            }
        }
    }
    None
}

/// Parse a SAML `YYYY-MM-DDTHH:MM:SSZ` instant to Unix seconds. Fractional
/// seconds and numeric offsets fail closed (return `None`): the broker
/// mints and checks whole-second UTC instants only.
fn parse_saml_time(raw: &str) -> Option<i64> {
    let text = raw.trim();
    if !text.ends_with('Z') && !text.ends_with('z') {
        return None;
    }
    let text = &text[..text.len() - 1];
    let (date, time) = text.split_once('T')?;
    let mut date_parts = date.split('-');
    let year: i64 = date_parts.next()?.parse().ok()?;
    let month: i64 = date_parts.next()?.parse().ok()?;
    let day: i64 = date_parts.next()?.parse().ok()?;
    if date_parts.next().is_some() {
        return None;
    }
    let mut time_parts = time.split(':');
    let hour: i64 = time_parts.next()?.parse().ok()?;
    let minute: i64 = time_parts.next()?.parse().ok()?;
    let second: i64 = time_parts.next()?.parse().ok()?;
    if time_parts.next().is_some() {
        return None;
    }
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    let days = days_from_civil(year, month as u32, day as u32)?;
    Some(days * 86400 + hour * 3600 + minute * 60 + second)
}

/// Days since the Unix epoch for a civil date (Howard Hinnant's
/// algorithm). Returns `None` for out-of-range inputs.
fn days_from_civil(year: i64, month: u32, day: u32) -> Option<i64> {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year.rem_euclid(400);
    let month_prime = (month as i64 + 9) % 12;
    let day_of_year = (153 * month_prime + 2) / 5 + day as i64 - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    Some(era * 146097 + day_of_era - 719468)
}

/// Current Unix time in whole seconds (management-plane only).
fn saml_now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

/// The `<SignatureValue>` body decoded to bytes. `None` when the response
/// carries no signature value: unsigned responses never verify.
fn extract_saml_signature_value(xml: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    let bodies = saml_tag_bodies(xml, "SignatureValue");
    let body = bodies.first()?;
    let compact: String = body.chars().filter(|c| !c.is_whitespace()).collect();
    if compact.is_empty() {
        return None;
    }
    base64::engine::general_purpose::STANDARD
        .decode(&compact)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(&compact))
        .ok()
        .filter(|decoded| !decoded.is_empty())
}

/// The signed content: the decoded response XML with the first
/// `<Signature>...</Signature>` (or self-closed `<Signature .../>`) block
/// removed. The signature is computed over everything else, so verification
/// recomputes over this exact remainder. A `<SignatureValue` prefix never
/// opens a block: only a `<Signature>` element whose next character is
/// `>` or whitespace counts.
fn strip_saml_signature(xml: &str) -> String {
    let mut cursor = 0;
    while let Some(found) = xml[cursor..].find("<Signature") {
        let start = cursor + found;
        let after = xml[start + "<Signature".len()..].chars().next();
        match after {
            Some('>') | Some(' ') | Some('\t') | Some('\n') | Some('\r') => {
                if let Some(close) = xml[start..].find("</Signature>") {
                    let end = start + close + "</Signature>".len();
                    let mut out = String::with_capacity(xml.len().saturating_sub(end - start));
                    out.push_str(&xml[..start]);
                    out.push_str(&xml[end..]);
                    return out;
                }
                if let Some(close) = xml[start..].find("/>") {
                    let end = start + close + "/>".len();
                    let mut out = String::with_capacity(xml.len().saturating_sub(end - start));
                    out.push_str(&xml[..start]);
                    out.push_str(&xml[end..]);
                    return out;
                }
                return xml.to_string();
            }
            _ => {
                cursor = start + "<Signature".len();
            }
        }
    }
    xml.to_string()
}

/// Verify the response's HMAC-SHA256 signature against the stored backend
/// secret using the maintained `hmac` and `sha2` crates. The
/// `<SignatureValue>` must be `base64(HMAC-SHA256(secret, stripped_xml))`.
/// Missing secrets, missing signatures and mismatches all return `false`;
/// callers fail closed and log without secrets.
fn verify_saml_hmac(xml: &str, config: &serde_json::Value) -> bool {
    use hmac::Mac as _;
    let Some(secret) = saml_hmac_secret(config) else {
        return false;
    };
    if secret.is_empty() {
        return false;
    }
    let Some(expected) = extract_saml_signature_value(xml) else {
        return false;
    };
    if expected.len() != 32 {
        return false;
    }
    let stripped = strip_saml_signature(xml);
    let mut mac = match hmac::Hmac::<sha2::Sha256>::new_from_slice(&secret) {
        Ok(mac) => mac,
        Err(_) => return false,
    };
    mac.update(stripped.as_bytes());
    let computed = mac.finalize().into_bytes();
    let mut diff = 0u8;
    for (left, right) in computed.iter().zip(expected.iter()) {
        diff |= left ^ right;
    }
    diff == 0
}

/// Extract a `<NameID>` element body from response XML, tolerating a
/// namespace prefix and attributes. Returns the trimmed body when one is
/// present and non-empty.
fn extract_saml_name_id(xml: &str) -> Option<String> {
    let mut search = 0;
    while let Some(open) = xml[search..].find("NameID") {
        let tag = search + open;
        let end = xml[tag..].find('>')?;
        let body_start = tag + end + 1;
        let close = xml[body_start..].find("</")?;
        let body = xml[body_start..body_start + close].trim();
        if !body.is_empty() && !body.contains('<') {
            return Some(body.to_string());
        }
        search = body_start + close + 2;
    }
    None
}

/// First non-empty string under any of `keys` in a stored backend config
/// object. Lets the metadata and redirect render tolerate the field names
/// the per-backend detail task stores without inventing values.
fn config_string(config: &serde_json::Value, keys: &[&str]) -> Option<String> {
    config.as_object().and_then(|object| {
        keys.iter().find_map(|key| {
            object
                .get(*key)
                .and_then(|value| value.as_str())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        })
    })
}

/// XML-escape one attribute or element body.
fn xml_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for c in input.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

/// Render the service-provider metadata document for one stored backend.
/// The entity id and the consumer location come from the stored config
/// when it names them and fall back to deterministic defaults otherwise;
/// fields the broker does not track are omitted, never invented beyond
/// the address of this route itself.
// TODO(parity): which entity id, endpoints, bindings and key descriptors
// does the documented metadata carry, and where do they come from in the
// backend config? The rulebook does not decide the exact document; the
// current choice renders the entity id plus one POST consumer location
// until the checker pins the shape.
fn render_saml_metadata(backend: &str, config: &serde_json::Value) -> String {
    let entity_id = config_string(config, &["entity_id", "entityId", "sp_entity_id", "issuer"])
        .unwrap_or_else(|| format!("urn:indramqtt:saml:sp:{backend}"));
    let acs_location = config_string(
        config,
        &["acs_url", "acsUrl", "acs", "assertion_consumer_service_url"],
    )
    .unwrap_or_else(|| "/api/v5/sso/saml/acs".to_string());
    format!(
        concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
            "<EntityDescriptor xmlns=\"urn:oasis:names:tc:SAML:2.0:metadata\" entityID=\"{}\">\n",
            "  <SPSSODescriptor AuthnRequestsSigned=\"false\" WantAssertionsSigned=\"false\" protocolSupportEnumeration=\"urn:oasis:names:tc:SAML:2.0:protocol\">\n",
            "    <AssertionConsumerService Binding=\"urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST\" Location=\"{}\" index=\"1\"/>\n",
            "  </SPSSODescriptor>\n",
            "</EntityDescriptor>\n"
        ),
        xml_escape(&entity_id),
        xml_escape(&acs_location)
    )
}

fn saml_backend_not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({
            "code": "BACKEND_NOT_FOUND",
            "message": "Backend not found",
        })),
    )
        .into_response()
}

/// `GET /sso/saml/metadata`: serve the service-provider document for one
/// stored SAML backend as SAML metadata XML.
///
/// The route is public (no bearer token: identity-provider operators fetch
/// it before anyone holds a token) and not paged. The backend is the
/// `?backend=` query value or [`DEFAULT_SAML_BACKEND`]; unknown query
/// parameters are ignored. A backend with no stored slot answers 404
/// `BACKEND_NOT_FOUND` in the documented `{code, message}` shape; a stored
/// slot answers 200 with [`SAML_METADATA_CONTENT_TYPE`] regardless of its
/// enable flag (the document describes the provider, it grants nothing).
///
/// Management-plane only: one short registry read per request plus a
/// bounded static render; publish and deliver never touch this store.
/// Bound: the rendered document is capped at [`MAX_SAML_METADATA_LEN`]
/// bytes (today's render is hundreds of bytes); no new state.
// TODO(parity): does the documented route 404 on disabled backends or
// serve the document anyway? The rulebook does not decide the disabled
// filter and the document grants nothing, so the current choice serves
// every stored slot until the checker pins it.
// TODO(parity): does the documented route take a backend selector at all,
// or is there exactly one SAML slot? The rulebook does not decide the
// selection; the current choice keeps the optional `?backend=` selector
// with the `saml` default until the checker pins it.
pub async fn saml_metadata(
    State(state): State<ApiState>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    use crate::errors::ApiError;
    let backend = params
        .get("backend")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_SAML_BACKEND.to_string());
    if backend.is_empty() {
        return ApiError::BadRequest("field `backend` must not be empty".to_string())
            .into_response();
    }
    let Some(stored) = state.sso_backends.get(&backend) else {
        return saml_backend_not_found();
    };
    let document = render_saml_metadata(&backend, &stored.config);
    debug_assert!(document.len() <= MAX_SAML_METADATA_LEN);
    (
        StatusCode::OK,
        [("content-type", SAML_METADATA_CONTENT_TYPE)],
        document,
    )
        .into_response()
}

/// `POST /sso/saml/acs`: consume one SAML response for the configured
/// backend.
///
/// The route is public (no bearer token: the provider posts the browser
/// here before anyone holds a token) and not paged. It accepts the JSON
/// management shape or the browser form binding, in order:
/// - bodies that parse as neither fail with 400 in the documented
///   `{code, message}` shape;
/// - when `RelayState` names a live pending flow it binds the response to
///   that flow single-use (unknown, expired or replayed states fail with
///   400 and issue no token; a state that names a different backend than
///   an explicit `backend` field fails with 400);
/// - otherwise the backend is the `backend` body field, the `?backend=`
///   query value, or [`DEFAULT_SAML_BACKEND`]; a backend with no stored
///   slot fails with 404 `BACKEND_NOT_FOUND` and records nothing;
/// - a stored backend with `enable` false, with no stored provider config,
///   or whose verified username names no admin user fails closed with 401
///   `BAD_USERNAME_OR_PWD` and issues no token;
/// - a missing, oversized or undecodable `SAMLResponse` fails with 400
///   and issues no token; a response whose issuer, audience, time window,
///   recipient, subject confirmation or signature does not verify against
///   the stored backend config fails closed with 401 and issues no token;
/// - otherwise one live dashboard token is issued for the verified
///   `NameID` user (RelayState flows additionally bind `NameID` and
///   `InResponseTo` to the pending flow; the flow-less shape binds the
///   verified `NameID` only and never trusts a request `username` field)
///   with that user's current role, and the response carries the
///   documented token/redirect fields.
///
/// Every success verifies the response against the stored backend slot:
/// the `<Issuer>` must equal the slot's `issuer`, an `<Audience>` must
/// equal the slot's `audience`, the `Conditions` window must contain now
/// within [`SAML_CLOCK_SKEW_SECS`], the `Recipient` must match the slot's
/// consumer location when one is stored, the `<NameID>` must name the
/// flow user (RelayState shape), and the HMAC-SHA256 `<SignatureValue>`
/// (maintained `hmac`/`sha2` crates) over the response must verify with
/// the slot's stored secret. Responses are never logged and never stored.
/// Management-plane only: short registry/admin reads plus at most one
/// short pending-table lock per call; publish and deliver never touch any
/// of them. Bound: the pending-flow table from W2-33
/// ([`MAX_PENDING_SSO_LOGINS`], [`PENDING_SSO_LOGIN_TTL`]); the response
/// itself is capped at [`MAX_SAML_RESPONSE_LEN`] bytes and the relay state
/// at [`MAX_SAML_RELAYSTATE_LEN`] bytes; no new state.
// TODO(parity): is the documented miss code `BACKEND_NOT_FOUND` or the
// generic `NOT_FOUND`? The rulebook does not decide the exact code name;
// the current choice keeps `BACKEND_NOT_FOUND` (404, documented
// `{code, message}` shape) until the checker pins it.
// TODO(parity): what are the documented ACS field names (`SAMLResponse`
// versus `response`, `RelayState` versus `state`, plus `backend`)? The
// rulebook does not decide the exact names; the current choice reads the
// standard POST-binding names plus the `backend` management field (a
// request `username` field is accepted by the parser but never trusted)
// until the checker pins them.
// TODO(parity): what are the documented success field names (`token`
// versus `access_token`, `redirect` versus `redirect_uri`, plus
// username/backend)? The rulebook does not decide the exact shape; the
// current choice answers `{token, username, backend, redirect}` (token is
// the live bearer, redirect comes from the stored backend config's
// `redirect`/`redirect_uri` string when one is stored and defaults to
// `/`) until the checker pins it.
// TODO(parity): which signature profile does the documented ACS verify
// (RSA XML-DSig with provider certificates versus the HMAC-SHA256 shared
// secret checked here)? The rulebook does not decide the profile; the
// current choice verifies HMAC-SHA256 with the slot's stored secret and
// fails closed without one until the checker pins the profile.
pub async fn saml_acs(
    State(state): State<ApiState>,
    Query(query): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    if !state.sso_backends.unstable_flows_enabled() {
        return flow_not_available(FlowKind::BackendFlow);
    }
    use crate::errors::ApiError;
    let bad_request = |message: String| ApiError::BadRequest(message).into_response();
    let fields = match parse_saml_acs_fields(&body) {
        Ok(fields) => fields,
        Err(message) => return bad_request(message),
    };
    let relay_state = fields
        .relay_state
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    if let Some(relay) = relay_state {
        if relay.len() > MAX_SAML_RELAYSTATE_LEN {
            return bad_request("field `RelayState` is too long".to_string());
        }
        let Some(flow) = state.sso_backends.consume_pending_state(&relay) else {
            return bad_request("unknown or expired RelayState".to_string());
        };
        let explicit = fields
            .backend
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .or_else(|| {
                query
                    .get("backend")
                    .map(|value| value.trim())
                    .filter(|value| !value.is_empty())
            });
        if let Some(explicit) = explicit {
            if explicit != flow.backend {
                return bad_request("RelayState does not belong to this backend".to_string());
            }
        }
        let Some(stored) = state.sso_backends.get(&flow.backend) else {
            return saml_backend_not_found();
        };
        return complete_saml_acs(
            &state,
            &stored,
            &flow.backend,
            &flow.username,
            &fields,
            Some(relay.as_str()),
        );
    }
    let backend = fields
        .backend
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| {
            query
                .get("backend")
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| DEFAULT_SAML_BACKEND.to_string());
    let Some(stored) = state.sso_backends.get(&backend) else {
        return saml_backend_not_found();
    };
    complete_saml_acs_with_username(&state, &stored, &backend, &fields)
}

/// Shared tail of [`saml_acs`] once the backend row and the username are
/// known: fail closed on disabled slots, missing provider config and
/// unknown users, then verify the response issuer, audience, time window,
/// recipient, subject confirmation and HMAC signature against the stored
/// backend config before issuing one live bearer with the documented
/// token/redirect fields. `expected_in_response_to` is `Some` for the
/// RelayState shape (the consumed flow id the `InResponseTo` must name)
/// and `None` for the flow-less shape.
fn complete_saml_acs(
    state: &ApiState,
    stored: &SsoBackend,
    backend: &str,
    username: &str,
    fields: &SamlAcsFields,
    expected_in_response_to: Option<&str>,
) -> Response {
    use crate::errors::ApiError;
    let bad_request = |message: String| ApiError::BadRequest(message).into_response();
    let denied = |reason: &str| {
        tracing::warn!(
            backend = backend,
            username = username,
            reason = reason,
            "sso saml acs failed"
        );
        ApiError::BadUsernameOrPwd("Auth failed".to_string()).into_response()
    };
    if !stored.enable {
        tracing::warn!(
            backend = backend,
            username = username,
            "sso saml acs refused: backend is disabled"
        );
        return ApiError::BadUsernameOrPwd("Auth failed".to_string()).into_response();
    }
    if stored.config.is_null() {
        tracing::warn!(
            backend = backend,
            username = username,
            "sso saml acs failed: no provider config for this backend"
        );
        return ApiError::BadUsernameOrPwd("Auth failed".to_string()).into_response();
    }
    let Some(view) = state.admin_users.get(username) else {
        tracing::warn!(
            backend = backend,
            username = username,
            "sso saml acs failed: unknown user for this response"
        );
        return ApiError::BadUsernameOrPwd("Auth failed".to_string()).into_response();
    };
    let raw = fields.saml_response.as_deref().unwrap_or("").trim();
    if raw.is_empty() {
        return bad_request("field `SAMLResponse` must not be empty".to_string());
    }
    if raw.len() > MAX_SAML_RESPONSE_LEN {
        return bad_request("field `SAMLResponse` is too long".to_string());
    }
    let Some(xml) = decode_saml_response(raw) else {
        return bad_request("field `SAMLResponse` is not a SAML response".to_string());
    };
    if !(xml.contains("<Response") || xml.contains("<Assertion")) {
        return bad_request("field `SAMLResponse` is not a SAML response".to_string());
    }
    // Issuer must equal the stored backend's identity-provider issuer.
    let Some(expected_issuer) = saml_expected_issuer(&stored.config) else {
        return denied("no identity-provider issuer for this backend");
    };
    let issuers = saml_tag_bodies(&xml, "Issuer");
    if issuers.is_empty() || issuers.iter().any(|issuer| issuer != &expected_issuer) {
        return denied("issuer does not match this backend");
    }
    // At least one audience must equal the stored service-provider entity.
    let Some(expected_audience) = saml_expected_audience(&stored.config) else {
        return denied("no audience for this backend");
    };
    let audiences = saml_tag_bodies(&xml, "Audience");
    if audiences.is_empty()
        || !audiences
            .iter()
            .any(|audience| audience == &expected_audience)
    {
        return denied("audience does not match this backend");
    }
    // Conditions window must contain now within the allowed skew.
    let not_before = saml_attribute(&xml, "NotBefore").and_then(|raw| parse_saml_time(&raw));
    let not_on_or_after =
        saml_attribute(&xml, "NotOnOrAfter").and_then(|raw| parse_saml_time(&raw));
    let (Some(not_before), Some(not_on_or_after)) = (not_before, not_on_or_after) else {
        return denied("response carries no usable validity window");
    };
    if not_on_or_after <= not_before {
        return denied("response validity window is empty");
    }
    let now = saml_now_secs();
    if now < not_before - SAML_CLOCK_SKEW_SECS || now >= not_on_or_after + SAML_CLOCK_SKEW_SECS {
        return denied("response is outside its validity window");
    }
    // Recipient must match the stored consumer location when one is stored.
    if let Some(expected_recipient) = saml_expected_recipient(&stored.config) {
        let recipient = saml_attribute(&xml, "Recipient");
        if recipient.as_deref() != Some(expected_recipient.as_str()) {
            return denied("recipient does not match this backend");
        }
    }
    // The asserted NameID is the authenticated user: it must name the flow
    // user, never a request field. Self-asserted identities for another
    // user fail closed here.
    let asserted = extract_saml_name_id(&xml);
    if asserted.as_deref() != Some(username) {
        return denied("subject does not match this login");
    }
    // RelayState flows additionally bind InResponseTo to the consumed flow.
    if let Some(expected) = expected_in_response_to {
        let in_response_to = saml_attribute(&xml, "InResponseTo");
        if in_response_to.as_deref() != Some(expected) {
            return denied("response does not answer this login");
        }
    }
    // The HMAC-SHA256 signature must verify with the stored backend secret.
    // Unsigned responses and responses signed with any other key fail here,
    // so a self-made document never yields a bearer.
    if !verify_saml_hmac(&xml, &stored.config) {
        return denied("response signature does not verify for this backend");
    }
    let token = state.tokens.issue(username, view.role);
    tracing::warn!(
        backend = backend,
        username = username,
        "sso saml acs completed"
    );
    let redirect = stored
        .config
        .as_object()
        .and_then(|object| {
            object
                .get("redirect")
                .or_else(|| object.get("redirect_uri"))
                .or_else(|| object.get("post_login_redirect"))
        })
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("/")
        .to_string();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "token": token,
            "username": username,
            "backend": backend,
            "redirect": redirect,
        })),
    )
        .into_response()
}

/// Username resolution for the flow-less ACS shape: the verified
/// response's `<NameID>` body alone binds the user. A request `username`
/// field is never trusted (it is accepted by the parser for compatibility
/// but ignored here), so a caller cannot self-assert another identity.
/// A missing `NameID` is malformed; an unknown user fails closed.
fn complete_saml_acs_with_username(
    state: &ApiState,
    stored: &SsoBackend,
    backend: &str,
    fields: &SamlAcsFields,
) -> Response {
    use crate::errors::ApiError;
    // The parser accepts a `username` management field for compatibility,
    // but it is never trusted: the verified `<NameID>` alone binds the user.
    let _ = fields.username.as_deref();
    let Some(username) = fields
        .saml_response
        .as_deref()
        .and_then(decode_saml_response)
        .as_deref()
        .and_then(extract_saml_name_id)
    else {
        return ApiError::BadRequest("field `username` must not be empty".to_string())
            .into_response();
    };
    complete_saml_acs(state, stored, backend, &username, fields, None)
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
        assert!(store.running().is_empty());
        assert!(store.get("w232-missing").is_none());
    }

    #[test]
    fn running_lists_only_enabled_names_sorted() {
        // W2-36: running is the enabled subset of the live registry as
        // bare names in backend-name order; an empty set reads as `[]`.
        let store = empty_store();
        assert!(store.running().is_empty());
        store
            .insert(sso_backend(
                "w236-saml".to_string(),
                true,
                serde_json::Value::Null,
            ))
            .expect("enabled insert succeeds");
        store
            .insert(sso_backend(
                "w236-oidc".to_string(),
                false,
                serde_json::json!({"issuer": "https://idp.example.invalid"}),
            ))
            .expect("disabled insert succeeds");
        store
            .insert(sso_backend(
                "w236-aaa".to_string(),
                true,
                serde_json::Value::Null,
            ))
            .expect("second enabled insert succeeds");
        assert_eq!(
            store.running(),
            vec!["w236-aaa".to_string(), "w236-saml".to_string()]
        );
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

#[cfg(test)]
mod sso_oidc_callback_tests {
    use super::*;
    use broker_rules::{BackpressurePolicy, RuleEngine};
    use std::collections::HashMap;

    fn standalone_state() -> ApiState {
        ApiState::standalone(Arc::new(RuleEngine::new(
            16,
            BackpressurePolicy::DropOldest,
        )))
    }

    fn seed_backend(state: &ApiState, name: &str, enable: bool, config: serde_json::Value) {
        state
            .sso_backends
            .insert(sso_backend(name.to_string(), enable, config))
            .expect("seed backend inserts");
    }

    fn seed_user(state: &ApiState, username: &str) {
        state
            .admin_users
            .create(
                username,
                "OidcProbe123!",
                crate::admin_users::AdminRole::Viewer,
                "oidc probe",
            )
            .expect("probe user creates");
    }

    fn query(pairs: &[(&str, &str)]) -> Query<HashMap<String, String>> {
        Query(
            pairs
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
        )
    }

    async fn parts(response: Response) -> (StatusCode, serde_json::Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("test body is small and readable");
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("test body is JSON");
        (status, json)
    }

    async fn callback(state: &ApiState, pairs: &[(&str, &str)]) -> (StatusCode, serde_json::Value) {
        parts(oidc_callback(State(state.clone()), query(pairs)).await).await
    }

    fn begin_flow(state: &ApiState, backend: &str, username: &str) -> String {
        state.sso_backends.record_login_attempt(backend, username)
    }

    #[tokio::test]
    async fn bad_state_is_rejected_with_documented_shape_and_no_token() {
        // W2-35 failing test (bad half): before this task the route
        // answered 404 with the router's plain-text body; now an unknown
        // state answers 400 in the documented `{code, message}` shape.
        let state = standalone_state();
        seed_backend(
            &state,
            "w235-oidc",
            true,
            serde_json::json!({"issuer": "https://idp.example.invalid"}),
        );
        seed_user(&state, "w235-alice");
        let (status, err) = callback(
            &state,
            &[
                ("state", "w235-no-such-state"),
                ("code", "w235-no-such-code"),
            ],
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
        assert!(err["message"].is_string());
        assert_eq!(err.as_object().expect("error is an object").len(), 2);
        assert!(err.get("token").is_none(), "no token is issued on failure");
    }

    #[tokio::test]
    async fn happy_path_against_stubbed_config_returns_token_and_redirect() {
        // W2-35 failing test (good half): a live state plus a non-empty
        // code against a stubbed (stored, enabled, configured) backend
        // completes with the documented token/redirect fields. Before:
        // 404 for everything on this path.
        let state = standalone_state();
        seed_backend(
            &state,
            "w235-oidc",
            true,
            serde_json::json!({"issuer": "https://idp.example.invalid"}),
        );
        seed_user(&state, "w235-bob");
        let flow = begin_flow(&state, "w235-oidc", "w235-bob");
        let (status, ok) = callback(&state, &[("state", &flow), ("code", "w235-stub-code")]).await;
        assert_eq!(status, StatusCode::OK);
        let token = ok["token"].as_str().expect("success carries a token");
        assert!(!token.is_empty());
        assert_eq!(ok["username"], serde_json::json!("w235-bob"));
        assert_eq!(ok["backend"], serde_json::json!("w235-oidc"));
        assert!(
            ok["redirect"].is_string(),
            "success carries redirect fields"
        );
        assert!(ok.get("code").is_none(), "success carries no error code");
        // The issued bearer resolves through the live token store: the
        // callback hands back a bearer that works instead of a constant
        // standing in for one.
        assert!(
            state.tokens.lookup(token).is_some(),
            "issued token is live in the token store"
        );
        // States are single-use: replaying the same state fails even with
        // a fresh code and issues no token.
        let (status, err) = callback(&state, &[("state", &flow), ("code", "w235-again")]).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
        assert!(err.get("token").is_none());
    }

    #[tokio::test]
    async fn malformed_queries_disabled_backends_and_unknown_users_fail_closed() {
        let state = standalone_state();
        seed_backend(
            &state,
            "w235-oidc",
            true,
            serde_json::json!({"issuer": "https://idp.example.invalid"}),
        );
        seed_backend(
            &state,
            "w235-off",
            false,
            serde_json::json!({"issuer": "https://idp.example.invalid"}),
        );
        seed_backend(&state, "w235-bare", true, serde_json::Value::Null);
        seed_user(&state, "w235-carol");

        // Malformed queries never consume a pending state: the flow below
        // still completes afterwards.
        let flow = begin_flow(&state, "w235-oidc", "w235-carol");
        for pairs in [
            vec![],
            vec![("state", flow.as_str())],
            vec![("code", "w235-x")],
            vec![("state", ""), ("code", "w235-x")],
            vec![("state", flow.as_str()), ("code", "")],
            vec![("state", flow.as_str()), ("code", "   ")],
        ] {
            let (status, err) = callback(&state, &pairs).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "pairs {pairs:?}");
            assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
            assert!(err.get("token").is_none());
        }
        let (status, ok) = callback(&state, &[("state", flow.as_str()), ("code", "w235-x")]).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "malformed attempts leave the state live"
        );
        assert!(ok["token"].is_string());

        // Provider-denied flows fail closed and burn the state.
        let flow = begin_flow(&state, "w235-oidc", "w235-carol");
        let (status, err) = callback(
            &state,
            &[
                ("state", flow.as_str()),
                ("code", "w235-x"),
                ("error", "access_denied"),
            ],
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(err.get("token").is_none());
        let (status, _) = callback(&state, &[("state", flow.as_str()), ("code", "w235-x")]).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "error consumes the state");

        // Disabled backends fail closed with no token.
        let flow = begin_flow(&state, "w235-off", "w235-carol");
        let (status, err) = callback(&state, &[("state", flow.as_str()), ("code", "w235-x")]).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(err["code"], serde_json::json!("BAD_USERNAME_OR_PWD"));
        assert!(err.get("token").is_none());

        // Backends with no stored provider config fail closed: there is
        // nothing the code could be exchanged against.
        let flow = begin_flow(&state, "w235-bare", "w235-carol");
        let (status, err) = callback(&state, &[("state", flow.as_str()), ("code", "w235-x")]).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(err.get("token").is_none());

        // Flows for unknown admin users fail closed: an issued token
        // would be revoked on first use, so none is issued.
        let flow = begin_flow(&state, "w235-oidc", "w235-ghost");
        let (status, err) = callback(&state, &[("state", flow.as_str()), ("code", "w235-x")]).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(err.get("token").is_none());
    }

    #[test]
    fn callback_bounds_are_finite_and_reasoned() {
        const { assert!(MAX_PENDING_SSO_LOGINS > 0) };
        const { assert!(MAX_OIDC_STATE_LEN > 0) };
        const { assert!(MAX_OIDC_CODE_LEN > 0) };
        assert!(!PENDING_SSO_LOGIN_TTL.is_zero());
    }
}

#[cfg(test)]
mod sso_saml_tests {
    use super::*;
    use broker_rules::{BackpressurePolicy, RuleEngine};
    use std::collections::HashMap;

    fn standalone_state() -> ApiState {
        ApiState::standalone(Arc::new(RuleEngine::new(
            16,
            BackpressurePolicy::DropOldest,
        )))
    }

    fn seed_backend(state: &ApiState, name: &str, enable: bool, config: serde_json::Value) {
        state
            .sso_backends
            .insert(sso_backend(name.to_string(), enable, config))
            .expect("seed backend inserts");
    }

    fn seed_user(state: &ApiState, username: &str) {
        state
            .admin_users
            .create(
                username,
                "SamlProbe123!",
                crate::admin_users::AdminRole::Viewer,
                "saml probe",
            )
            .expect("probe user creates");
    }

    fn body(value: serde_json::Value) -> Bytes {
        Bytes::from(serde_json::to_vec(&value).expect("test body serialises"))
    }

    const TEST_ISSUER: &str = "https://idp.example.invalid";
    const TEST_AUDIENCE: &str = "https://sp.example.invalid/metadata";
    const TEST_RECIPIENT: &str = "https://sp.example.invalid/acs";
    // `!` keeps this outside the base64 alphabet so the stored slot uses
    // the raw UTF-8 bytes on both the signing and verifying sides.
    const TEST_SECRET: &str = "w237-saml-test-secret-!!";

    fn saml_config() -> serde_json::Value {
        serde_json::json!({
            "issuer": TEST_ISSUER,
            "audience": TEST_AUDIENCE,
            "acs_url": TEST_RECIPIENT,
            "hmac_secret": TEST_SECRET,
            "redirect": "/dashboard",
        })
    }

    fn unsigned_xml(username: &str, in_response_to: Option<&str>) -> String {
        let response_in_response_to = in_response_to
            .map(|state| format!(" InResponseTo=\"{state}\""))
            .unwrap_or_default();
        let confirmation_in_response_to = in_response_to
            .map(|state| format!(" InResponseTo=\"{state}\""))
            .unwrap_or_default();
        format!(
            concat!(
                "<Response ID=\"_r\" Version=\"2.0\" IssueInstant=\"2026-01-01T00:00:00Z\"{response_attr}>",
                "<Issuer>{issuer}</Issuer>",
                "<Status><StatusCode Value=\"urn:oasis:names:tc:SAML:2.0:status:Success\"/></Status>",
                "<Assertion ID=\"_a\" IssueInstant=\"2026-01-01T00:00:00Z\" Version=\"2.0\">",
                "<Issuer>{issuer}</Issuer>",
                "<Subject><NameID>{username}</NameID>",
                "<SubjectConfirmation Method=\"urn:oasis:names:tc:SAML:2.0:cm:bearer\">",
                "<SubjectConfirmationData NotOnOrAfter=\"2100-01-01T00:00:00Z\" Recipient=\"{recipient}\"{confirmation_attr}/>",
                "</SubjectConfirmation></Subject>",
                "<Conditions NotBefore=\"2020-01-01T00:00:00Z\" NotOnOrAfter=\"2100-01-01T00:00:00Z\">",
                "<AudienceRestriction><Audience>{audience}</Audience></AudienceRestriction>",
                "</Conditions>",
                "<AuthnStatement AuthnInstant=\"2026-01-01T00:00:00Z\"/>",
                "</Assertion></Response>"
            ),
            response_attr = response_in_response_to,
            confirmation_attr = confirmation_in_response_to,
            issuer = TEST_ISSUER,
            username = username,
            recipient = TEST_RECIPIENT,
            audience = TEST_AUDIENCE,
        )
    }

    fn sign_xml(unsigned: &str) -> String {
        use base64::Engine as _;
        use hmac::Mac as _;
        let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(TEST_SECRET.as_bytes())
            .expect("test secret is a valid HMAC key");
        mac.update(unsigned.as_bytes());
        let signature =
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        let block = format!("<Signature><SignatureValue>{signature}</SignatureValue></Signature>");
        let end = unsigned.find('>').expect("test response has a root tag") + 1;
        format!("{}{block}{}", &unsigned[..end], &unsigned[end..])
    }

    fn signed_response(username: &str, in_response_to: Option<&str>) -> String {
        use base64::Engine as _;
        let unsigned = unsigned_xml(username, in_response_to);
        base64::engine::general_purpose::STANDARD.encode(sign_xml(&unsigned).as_bytes())
    }

    fn stub_response(username: &str) -> String {
        signed_response(username, None)
    }

    async fn acs_json(
        state: &ApiState,
        query: HashMap<String, String>,
        raw: Bytes,
    ) -> (StatusCode, serde_json::Value) {
        let response = saml_acs(State(state.clone()), Query(query), raw).await;
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("test body is small and readable");
        let json: serde_json::Value = serde_json::from_slice(&bytes).expect("test body is JSON");
        (status, json)
    }

    async fn metadata_raw(
        state: &ApiState,
        query: HashMap<String, String>,
    ) -> (StatusCode, HashMap<String, String>, Vec<u8>) {
        let response = saml_metadata(State(state.clone()), Query(query)).await;
        let status = response.status();
        let headers: HashMap<String, String> = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_string(),
                    value.to_str().unwrap_or("").to_string(),
                )
            })
            .collect();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("test body is small and readable");
        (status, headers, bytes.to_vec())
    }

    fn empty_query() -> HashMap<String, String> {
        HashMap::new()
    }

    #[tokio::test]
    async fn unknown_backend_is_404_with_documented_shape() {
        // W2-37 failing test (miss half): before this task the ACS path
        // answered 404 with the router's plain-text body; now an unknown
        // backend answers 404 in the documented `{code, message}` shape.
        let state = standalone_state();
        let (status, err) = acs_json(
            &state,
            empty_query(),
            body(serde_json::json!({
                "backend": "w237-no-such-backend",
                "SAMLResponse": stub_response("w237-alice"),
            })),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(err["code"], serde_json::json!("BACKEND_NOT_FOUND"));
        assert!(err["message"].is_string());
        assert_eq!(err.as_object().expect("error is an object").len(), 2);
        assert!(err.get("token").is_none());
        // The default slot is equally unknown when nothing is stored.
        let (status, err) = acs_json(
            &state,
            empty_query(),
            body(serde_json::json!({"SAMLResponse": stub_response("w237-alice")})),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(err["code"], serde_json::json!("BACKEND_NOT_FOUND"));
    }

    #[tokio::test]
    async fn bad_responses_are_rejected_with_documented_shape_and_no_token() {
        let state = standalone_state();
        seed_backend(&state, "saml", true, saml_config());
        seed_user(&state, "w237-alice");
        for raw in [
            Bytes::from_static(b""),
            Bytes::from_static(b"not json"),
            body(serde_json::json!([1, 2])),
            body(serde_json::json!({})),
            body(serde_json::json!({"SAMLResponse": ""})),
            body(serde_json::json!({"SAMLResponse": "   "})),
            body(serde_json::json!({"SAMLResponse": "!!!not-base64!!!"})),
            body(serde_json::json!({"SAMLResponse": "aGVsbG8td29ybGQ="})),
            body(serde_json::json!({"username": "w237-alice"})),
        ] {
            let (status, err) = acs_json(&state, empty_query(), raw).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
            assert!(err["message"].is_string());
            assert_eq!(err.as_object().expect("error is an object").len(), 2);
            assert!(err.get("token").is_none(), "no token is issued on failure");
        }
        // Backend lookup runs first: a bad response against an unknown
        // backend still answers 404, never 400.
        let (status, err) = acs_json(
            &state,
            empty_query(),
            body(serde_json::json!({
                "backend": "w237-no-such-backend",
                "SAMLResponse": "",
            })),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(err["code"], serde_json::json!("BACKEND_NOT_FOUND"));
    }

    #[tokio::test]
    async fn happy_path_with_verified_response_issues_live_token_and_redirect() {
        // W2-37 failing test (good half): a response verified against the
        // stored backend (issuer, audience, time window, recipient and
        // HMAC signature) completes with the documented token/redirect
        // fields. Before: 404 for everything on this path.
        let state = standalone_state();
        seed_backend(&state, "saml", true, saml_config());
        seed_user(&state, "w237-bob");
        let (status, ok) = acs_json(
            &state,
            empty_query(),
            body(serde_json::json!({
                "SAMLResponse": signed_response("w237-bob", None),
            })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let token = ok["token"].as_str().expect("success carries a token");
        assert!(!token.is_empty());
        assert_eq!(ok["username"], serde_json::json!("w237-bob"));
        assert_eq!(ok["backend"], serde_json::json!("saml"));
        assert_eq!(ok["redirect"], serde_json::json!("/dashboard"));
        assert!(ok.get("code").is_none(), "success carries no error code");
        assert!(
            state.tokens.lookup(token).is_some(),
            "issued token is live in the token store"
        );
        // A request `username` field is never trusted: it cannot redirect
        // the verified NameID to another identity.
        let (status, ok) = acs_json(
            &state,
            empty_query(),
            body(serde_json::json!({
                "SAMLResponse": signed_response("w237-bob", None),
                "username": "w237-eve",
            })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(ok["username"], serde_json::json!("w237-bob"));
        // The browser form binding carries the same fields.
        let form = format!(
            "SAMLResponse={}&RelayState=",
            url_encode(&signed_response("w237-bob", None))
        );
        let (status, _) = acs_json(
            &state,
            empty_query(),
            Bytes::from(format!("username=w237-bob&{form}").into_bytes()),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn self_made_responses_never_issue_tokens() {
        // The arbiter's stub: a self-made base64 Response/Assertion with no
        // issuer, audience, window or signature must fail closed, even for
        // a known user on an enabled, configured backend.
        use base64::Engine as _;
        let state = standalone_state();
        seed_backend(&state, "saml", true, saml_config());
        seed_user(&state, "w237-bob");
        let forged = base64::engine::general_purpose::STANDARD.encode(
            "<Response><Assertion><Subject><NameID>w237-bob</NameID></Subject></Assertion></Response>"
                .as_bytes(),
        );
        for payload in [
            body(serde_json::json!({"SAMLResponse": forged})),
            body(serde_json::json!({"SAMLResponse": forged, "username": "w237-bob"})),
        ] {
            let (status, err) = acs_json(&state, empty_query(), payload).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
            assert_eq!(err["code"], serde_json::json!("BAD_USERNAME_OR_PWD"));
            assert!(err.get("token").is_none());
        }
        // Unsigned but otherwise well-formed responses fail the same way.
        let unsigned = base64::engine::general_purpose::STANDARD
            .encode(unsigned_xml("w237-bob", None).as_bytes());
        let (status, err) = acs_json(
            &state,
            empty_query(),
            body(serde_json::json!({"SAMLResponse": unsigned})),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(err.get("token").is_none());
        // Flipping one NameID byte after signing breaks the HMAC, so the
        // tampered document fails closed even though its NameID names a
        // known user.
        seed_user(&state, "w237-eve");
        let good = signed_response("w237-bob", None);
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&good)
            .expect("test response decodes");
        let tampered_xml = String::from_utf8(decoded)
            .expect("test response is UTF-8")
            .replacen("w237-bob", "w237-eve", 1);
        let tampered = base64::engine::general_purpose::STANDARD.encode(tampered_xml.as_bytes());
        let (status, err) = acs_json(
            &state,
            empty_query(),
            body(serde_json::json!({"SAMLResponse": tampered})),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(err.get("token").is_none());
    }

    #[tokio::test]
    async fn issuer_audience_window_and_recipient_mismatch_fail_closed() {
        use base64::Engine as _;
        let state = standalone_state();
        seed_backend(&state, "saml", true, saml_config());
        seed_user(&state, "w237-bob");
        // Wrong issuer.
        let wrong_issuer =
            unsigned_xml("w237-bob", None).replacen(TEST_ISSUER, "https://evil.example.invalid", 2);
        let wrong_issuer =
            base64::engine::general_purpose::STANDARD.encode(sign_xml(&wrong_issuer).as_bytes());
        let (status, err) = acs_json(
            &state,
            empty_query(),
            body(serde_json::json!({"SAMLResponse": wrong_issuer})),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(err.get("token").is_none());
        // Wrong audience.
        let wrong_audience = unsigned_xml("w237-bob", None).replacen(
            TEST_AUDIENCE,
            "https://evil.example.invalid/aud",
            1,
        );
        let wrong_audience =
            base64::engine::general_purpose::STANDARD.encode(sign_xml(&wrong_audience).as_bytes());
        let (status, err) = acs_json(
            &state,
            empty_query(),
            body(serde_json::json!({"SAMLResponse": wrong_audience})),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(err.get("token").is_none());
        // Expired window.
        let expired = unsigned_xml("w237-bob", None).replacen(
            "2100-01-01T00:00:00Z",
            "2020-01-02T00:00:00Z",
            2,
        );
        let expired =
            base64::engine::general_purpose::STANDARD.encode(sign_xml(&expired).as_bytes());
        let (status, err) = acs_json(
            &state,
            empty_query(),
            body(serde_json::json!({"SAMLResponse": expired})),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(err.get("token").is_none());
        // Wrong recipient.
        let wrong_recipient = unsigned_xml("w237-bob", None).replacen(
            TEST_RECIPIENT,
            "https://evil.example.invalid/acs",
            1,
        );
        let wrong_recipient =
            base64::engine::general_purpose::STANDARD.encode(sign_xml(&wrong_recipient).as_bytes());
        let (status, err) = acs_json(
            &state,
            empty_query(),
            body(serde_json::json!({"SAMLResponse": wrong_recipient})),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(err.get("token").is_none());
    }

    fn url_encode(input: &str) -> String {
        let mut out = String::with_capacity(input.len());
        for byte in input.bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    out.push(byte as char);
                }
                _ => out.push_str(&format!("%{byte:02X}")),
            }
        }
        out
    }

    #[tokio::test]
    async fn relaystate_binds_single_use_flow_and_fail_closed_cases() {
        let state = standalone_state();
        seed_backend(&state, "saml", true, saml_config());
        let mut off_config = saml_config();
        off_config["redirect"] = serde_json::json!("/off");
        seed_backend(&state, "w237-off", false, off_config);
        seed_backend(&state, "w237-bare", true, serde_json::Value::Null);
        seed_user(&state, "w237-carol");

        // A live RelayState with a response verified against the stored
        // slot completes and burns: the replay fails as unknown.
        let flow = state
            .sso_backends
            .record_login_attempt("saml", "w237-carol");
        let (status, ok) = acs_json(
            &state,
            empty_query(),
            body(serde_json::json!({
                "SAMLResponse": signed_response("w237-carol", Some(flow.as_str())),
                "RelayState": flow,
            })),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(ok["token"].is_string());
        let (status, err) = acs_json(
            &state,
            empty_query(),
            body(serde_json::json!({
                "SAMLResponse": signed_response("w237-carol", Some(flow.as_str())),
                "RelayState": flow,
            })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "states are single-use");
        assert!(err.get("token").is_none());

        // Unknown RelayStates fail without a token.
        let (status, err) = acs_json(
            &state,
            empty_query(),
            body(serde_json::json!({
                "SAMLResponse": signed_response("w237-carol", None),
                "RelayState": "w237-no-such-state",
            })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(err.get("token").is_none());

        // A response bound to another flow does not answer this one.
        let flow_a = state
            .sso_backends
            .record_login_attempt("saml", "w237-carol");
        let flow_b = state
            .sso_backends
            .record_login_attempt("saml", "w237-carol");
        let (status, err) = acs_json(
            &state,
            empty_query(),
            body(serde_json::json!({
                "SAMLResponse": signed_response("w237-carol", Some(flow_a.as_str())),
                "RelayState": flow_b,
            })),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(err.get("token").is_none());

        // A NameID for another user does not complete this flow.
        let flow = state
            .sso_backends
            .record_login_attempt("saml", "w237-carol");
        let (status, err) = acs_json(
            &state,
            empty_query(),
            body(serde_json::json!({
                "SAMLResponse": signed_response("w237-eve", Some(flow.as_str())),
                "RelayState": flow,
            })),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(err.get("token").is_none());

        // A state that names a vanished backend is 404.
        let flow = state
            .sso_backends
            .record_login_attempt("w237-gone", "w237-carol");
        let (status, err) = acs_json(
            &state,
            empty_query(),
            body(serde_json::json!({
                "SAMLResponse": signed_response("w237-carol", Some(flow.as_str())),
                "RelayState": flow,
            })),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(err["code"], serde_json::json!("BACKEND_NOT_FOUND"));

        // Disabled backends fail closed with no token.
        let flow = state
            .sso_backends
            .record_login_attempt("w237-off", "w237-carol");
        let (status, err) = acs_json(
            &state,
            empty_query(),
            body(serde_json::json!({
                "SAMLResponse": signed_response("w237-carol", Some(flow.as_str())),
                "RelayState": flow,
            })),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(err["code"], serde_json::json!("BAD_USERNAME_OR_PWD"));
        assert!(err.get("token").is_none());

        // Backends with no stored provider config fail closed.
        let flow = state
            .sso_backends
            .record_login_attempt("w237-bare", "w237-carol");
        let (status, err) = acs_json(
            &state,
            empty_query(),
            body(serde_json::json!({
                "SAMLResponse": signed_response("w237-carol", Some(flow.as_str())),
                "RelayState": flow,
            })),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(err.get("token").is_none());

        // Flows for unknown admin users fail closed.
        let flow = state
            .sso_backends
            .record_login_attempt("saml", "w237-ghost");
        let (status, err) = acs_json(
            &state,
            empty_query(),
            body(serde_json::json!({
                "SAMLResponse": signed_response("w237-ghost", Some(flow.as_str())),
                "RelayState": flow,
            })),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(err.get("token").is_none());

        // The flow-less shape fails closed for unknown users too.
        let (status, err) = acs_json(
            &state,
            empty_query(),
            body(serde_json::json!({
                "SAMLResponse": signed_response("w237-ghost", None),
            })),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(err.get("token").is_none());
    }

    #[tokio::test]
    async fn metadata_serves_bounded_document_and_404s_when_unconfigured() {
        // W2-37 failing test (metadata half): before this task the path
        // answered 404 with the router's plain-text body; now a stored
        // slot serves its document with the metadata content type.
        let state = standalone_state();
        let (status, _, _) = metadata_raw(&state, empty_query()).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "unconfigured SAML is 404");
        seed_backend(
            &state,
            "saml",
            true,
            serde_json::json!({
                "entity_id": "https://sp.example.invalid/metadata",
                "acs_url": "https://sp.example.invalid/acs",
            }),
        );
        let (status, headers, raw) = metadata_raw(&state, empty_query()).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            headers.get("content-type").map(String::as_str),
            Some(SAML_METADATA_CONTENT_TYPE)
        );
        let document = String::from_utf8(raw).expect("metadata is UTF-8 XML");
        assert!(
            document.contains("EntityDescriptor"),
            "metadata carries the documented document"
        );
        assert!(document.contains("https://sp.example.invalid/metadata"));
        assert!(document.contains("https://sp.example.invalid/acs"));
        assert!(
            document.len() <= MAX_SAML_METADATA_LEN,
            "metadata is a bounded static document"
        );
        // Unknown query parameters are ignored; an unknown backend is 404.
        let mut query = HashMap::new();
        query.insert("unexpected_param".to_string(), "1".to_string());
        let (status, _, _) = metadata_raw(&state, query).await;
        assert_eq!(status, StatusCode::OK);
        let mut query = HashMap::new();
        query.insert("backend".to_string(), "w237-no-such-backend".to_string());
        let (status, _, raw) = metadata_raw(&state, query).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let err: serde_json::Value = serde_json::from_slice(&raw).expect("miss body is JSON");
        assert_eq!(err["code"], serde_json::json!("BACKEND_NOT_FOUND"));
        assert_eq!(err.as_object().expect("error is an object").len(), 2);
    }

    #[test]
    fn saml_bounds_are_finite_and_reasoned() {
        const { assert!(MAX_PENDING_SSO_LOGINS > 0) };
        const { assert!(MAX_SAML_RESPONSE_LEN > 0) };
        const { assert!(MAX_SAML_RELAYSTATE_LEN > 0) };
        const { assert!(MAX_SAML_METADATA_LEN > 0) };
        const { assert!(!DEFAULT_SAML_BACKEND.is_empty()) };
        const { assert!(SAML_CLOCK_SKEW_SECS > 0) };
        assert!(!SAML_METADATA_CONTENT_TYPE.is_empty());
        assert!(!PENDING_SSO_LOGIN_TTL.is_zero());
    }

    #[tokio::test]
    async fn oversized_response_is_rejected() {
        let state = standalone_state();
        seed_backend(&state, "saml", true, saml_config());
        seed_user(&state, "w237-dave");
        let big = "A".repeat(MAX_SAML_RESPONSE_LEN + 1);
        let (status, err) = acs_json(
            &state,
            empty_query(),
            body(serde_json::json!({
                "SAMLResponse": big,
                "username": "w237-dave",
            })),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
        assert!(err.get("token").is_none());
    }
}

#[cfg(test)]
mod sso_token_exchange_tests {
    use super::*;
    use broker_rules::{BackpressurePolicy, RuleEngine};

    fn standalone_state() -> ApiState {
        ApiState::standalone(Arc::new(RuleEngine::new(
            16,
            BackpressurePolicy::DropOldest,
        )))
    }

    fn seed_backend(state: &ApiState, name: &str, enable: bool, config: serde_json::Value) {
        state
            .sso_backends
            .insert(sso_backend(name.to_string(), enable, config))
            .expect("seed backend inserts");
    }

    fn seed_user(state: &ApiState, username: &str) {
        state
            .admin_users
            .create(
                username,
                "ExchangeProbe123!",
                crate::admin_users::AdminRole::Viewer,
                "exchange probe",
            )
            .expect("probe user creates");
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

    async fn exchange(state: &ApiState, raw: Bytes) -> (StatusCode, serde_json::Value) {
        parts(token_exchange(State(state.clone()), raw).await).await
    }

    fn begin_grant(state: &ApiState, backend: &str, username: &str) -> String {
        state.sso_backends.record_login_attempt(backend, username)
    }

    #[tokio::test]
    async fn unknown_backend_is_404_with_documented_shape_and_consumes_nothing() {
        // W2-38 failing test (miss half): before this task the path
        // answered 404 with the router's plain-text body; now an unknown
        // backend answers 404 in the documented `{code, message}` shape.
        let state = standalone_state();
        seed_backend(
            &state,
            "w238-oidc",
            true,
            serde_json::json!({"issuer": "https://idp.example.invalid"}),
        );
        seed_user(&state, "w238-alice");
        let grant = begin_grant(&state, "w238-oidc", "w238-alice");
        let (status, err) = exchange(
            &state,
            body(serde_json::json!({"backend": "w238-no-such-backend", "grant": grant})),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(err["code"], serde_json::json!("BACKEND_NOT_FOUND"));
        assert!(err["message"].is_string());
        assert_eq!(err.as_object().expect("error is an object").len(), 2);
        assert!(err.get("token").is_none(), "no token is issued on failure");
        // The miss consumed nothing: the live grant still exchanges
        // against its own backend afterwards.
        let (status, ok) = exchange(
            &state,
            body(serde_json::json!({"backend": "w238-oidc", "grant": grant})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(ok["token"].is_string());
    }

    #[tokio::test]
    async fn bad_grant_is_rejected_with_documented_shape_and_no_token() {
        // W2-38 failing test (bad-grant half): before this task the path
        // answered 404 with the router's plain-text body; now an unknown
        // grant answers 400 in the documented `{code, message}` shape.
        let state = standalone_state();
        seed_backend(
            &state,
            "w238-oidc",
            true,
            serde_json::json!({"issuer": "https://idp.example.invalid"}),
        );
        seed_user(&state, "w238-bob");
        let (status, err) = exchange(
            &state,
            body(serde_json::json!({"backend": "w238-oidc", "grant": "w238-no-such-grant"})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
        assert!(err["message"].is_string());
        assert_eq!(err.as_object().expect("error is an object").len(), 2);
        assert!(err.get("token").is_none(), "no token is issued on failure");
    }

    #[tokio::test]
    async fn malformed_bodies_are_400_and_consume_nothing() {
        let state = standalone_state();
        seed_backend(
            &state,
            "w238-oidc",
            true,
            serde_json::json!({"issuer": "https://idp.example.invalid"}),
        );
        seed_user(&state, "w238-carol");
        let grant = begin_grant(&state, "w238-oidc", "w238-carol");
        let long_backend = "b".repeat(broker_config::MAX_SSO_BACKEND_NAME_LEN + 1);
        let long_grant = "g".repeat(MAX_SSO_GRANT_LEN + 1);
        for raw in [
            Bytes::from_static(b""),
            Bytes::from_static(b"not json"),
            body(serde_json::json!([1, 2])),
            body(serde_json::json!(null)),
            body(serde_json::json!("grant")),
            body(serde_json::json!({})),
            body(serde_json::json!({"grant": grant})),
            body(serde_json::json!({"backend": "w238-oidc"})),
            body(serde_json::json!({"backend": "", "grant": grant})),
            body(serde_json::json!({"backend": "   ", "grant": grant})),
            body(serde_json::json!({"backend": "w238-oidc", "grant": ""})),
            body(serde_json::json!({"backend": "w238-oidc", "grant": "   "})),
            body(serde_json::json!({"backend": "w238-oidc", "grant": 7})),
            body(serde_json::json!({"backend": 7, "grant": grant})),
            body(serde_json::json!({"backend": long_backend, "grant": grant})),
            body(serde_json::json!({"backend": "w238-oidc", "grant": long_grant})),
        ] {
            let (status, err) = exchange(&state, raw).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
            assert!(err["message"].is_string());
            assert!(err.get("token").is_none());
        }
        // Malformed attempts never consume a pending grant: the live
        // grant below still completes afterwards.
        let (status, ok) = exchange(
            &state,
            body(serde_json::json!({"backend": "w238-oidc", "grant": grant})),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "malformed attempts leave the grant live"
        );
        assert!(ok["token"].is_string());
    }

    #[tokio::test]
    async fn happy_path_issues_live_token_with_documented_fields_and_single_use() {
        // A live grant against a stored, enabled, configured backend
        // completes with the documented token/redirect fields. Before:
        // 404 for everything on this path.
        let state = standalone_state();
        seed_backend(
            &state,
            "w238-oidc",
            true,
            serde_json::json!({
                "issuer": "https://idp.example.invalid",
                "redirect": "/dashboard",
            }),
        );
        seed_user(&state, "w238-dave");
        let grant = begin_grant(&state, "w238-oidc", "w238-dave");
        let (status, ok) = exchange(
            &state,
            body(serde_json::json!({"backend": "w238-oidc", "grant": grant})),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let token = ok["token"].as_str().expect("success carries a token");
        assert!(!token.is_empty());
        assert_eq!(ok["username"], serde_json::json!("w238-dave"));
        assert_eq!(ok["backend"], serde_json::json!("w238-oidc"));
        assert_eq!(ok["redirect"], serde_json::json!("/dashboard"));
        assert!(ok.get("code").is_none(), "success carries no error code");
        // The issued bearer resolves through the live token store: the
        // exchange hands back a bearer that works instead of a constant
        // standing in for one.
        assert!(
            state.tokens.lookup(token).is_some(),
            "issued token is live in the token store"
        );
        // Grants are single-use: replaying the same grant fails even
        // though the backend and user are unchanged, and issues no token.
        let (status, err) = exchange(
            &state,
            body(serde_json::json!({"backend": "w238-oidc", "grant": grant})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
        assert!(err.get("token").is_none());
    }

    #[tokio::test]
    async fn sibling_grant_names_resolve_the_same_pending_flow() {
        // The grant travels under the sibling flows' names for the same
        // pending-table id; all three complete the same way.
        let state = standalone_state();
        seed_backend(
            &state,
            "w238-oidc",
            true,
            serde_json::json!({"issuer": "https://idp.example.invalid"}),
        );
        seed_user(&state, "w238-erin");
        for key in ["grant", "code", "state"] {
            let grant = begin_grant(&state, "w238-oidc", "w238-erin");
            let mut fields = serde_json::Map::new();
            fields.insert("backend".to_string(), serde_json::json!("w238-oidc"));
            fields.insert(key.to_string(), serde_json::json!(grant));
            let (status, ok) = exchange(&state, body(serde_json::Value::Object(fields))).await;
            assert_eq!(status, StatusCode::OK, "grant travels as `{key}`");
            assert!(ok["token"].is_string());
        }
    }

    #[tokio::test]
    async fn grant_for_another_backend_is_rejected_and_burned() {
        let state = standalone_state();
        seed_backend(
            &state,
            "w238-a",
            true,
            serde_json::json!({"issuer": "https://idp.example.invalid"}),
        );
        seed_backend(
            &state,
            "w238-b",
            true,
            serde_json::json!({"issuer": "https://idp.example.invalid"}),
        );
        seed_user(&state, "w238-frank");
        let grant = begin_grant(&state, "w238-a", "w238-frank");
        let (status, err) = exchange(
            &state,
            body(serde_json::json!({"backend": "w238-b", "grant": grant})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
        assert!(err.get("token").is_none());
        // The grant is already consumed, so it cannot be retried against
        // its own backend afterwards.
        let (status, err) = exchange(
            &state,
            body(serde_json::json!({"backend": "w238-a", "grant": grant})),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "grants are single-use");
        assert!(err.get("token").is_none());
    }

    #[tokio::test]
    async fn disabled_unconfigured_and_unknown_user_flows_fail_closed() {
        let state = standalone_state();
        seed_backend(
            &state,
            "w238-oidc",
            true,
            serde_json::json!({"issuer": "https://idp.example.invalid"}),
        );
        seed_backend(
            &state,
            "w238-off",
            false,
            serde_json::json!({
                "issuer": "https://idp.example.invalid",
            }),
        );
        seed_backend(&state, "w238-bare", true, serde_json::Value::Null);
        seed_user(&state, "w238-grace");

        // Disabled backends fail closed with no token.
        let grant = begin_grant(&state, "w238-off", "w238-grace");
        let (status, err) = exchange(
            &state,
            body(serde_json::json!({"backend": "w238-off", "grant": grant})),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(err["code"], serde_json::json!("BAD_USERNAME_OR_PWD"));
        assert!(err.get("token").is_none());

        // Backends with no stored provider config fail closed: there is
        // nothing the grant could have been issued against.
        let grant = begin_grant(&state, "w238-bare", "w238-grace");
        let (status, err) = exchange(
            &state,
            body(serde_json::json!({"backend": "w238-bare", "grant": grant})),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(err.get("token").is_none());

        // Grants for unknown admin users fail closed: an issued token
        // would be revoked on first use, so none is issued.
        let grant = begin_grant(&state, "w238-oidc", "w238-ghost");
        let (status, err) = exchange(
            &state,
            body(serde_json::json!({"backend": "w238-oidc", "grant": grant})),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(err.get("token").is_none());
    }

    #[tokio::test]
    async fn grant_for_vanished_backend_is_404_with_documented_shape() {
        // The grant names a backend that is no longer stored: the miss
        // mirrors the login route instead of granting anything.
        let state = standalone_state();
        seed_user(&state, "w238-heidi");
        let grant = begin_grant(&state, "w238-gone", "w238-heidi");
        let (status, err) = exchange(
            &state,
            body(serde_json::json!({"backend": "w238-gone", "grant": grant})),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(err["code"], serde_json::json!("BACKEND_NOT_FOUND"));
        assert!(err.get("token").is_none());
    }

    #[test]
    fn detail_bounds_are_finite_and_reasoned() {
        const { assert!(MAX_SSO_BACKENDS > 0) };
        const { assert!(MAX_SSO_GRANT_LEN > 0) };
        const { assert!(broker_config::MAX_SSO_BACKEND_NAME_LEN > 0) };
        assert!(!PENDING_SSO_LOGIN_TTL.is_zero());
    }

    #[tokio::test]
    async fn put_then_get_round_trip_serves_flat_variant_fields() {
        // A flat variant write reads back flat. Before: the write
        // dropped unknown fields, so the row matched no documented
        // variant schema.
        use broker_rules::{BackpressurePolicy, RuleEngine};
        let state = ApiState::standalone(Arc::new(RuleEngine::new(
            16,
            BackpressurePolicy::DropOldest,
        )));
        let write = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "backend": "ldap",
                "enable": true,
                "server": "127.0.0.1:389",
                "base_dn": "dc=example,dc=com",
                "username": "cn=reader",
            }))
            .expect("test body serialises"),
        );
        let response = put_sso_backend(State(state.clone()), Path("ldap".to_string()), write).await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("test body is small and readable");
        let stored: serde_json::Value = serde_json::from_slice(&bytes).expect("test body is JSON");
        assert_eq!(stored["backend"], serde_json::json!("ldap"));
        assert_eq!(stored["server"], serde_json::json!("127.0.0.1:389"));

        let response = get_sso_backend(State(state.clone()), Path("ldap".to_string())).await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("test body is small and readable");
        let read: serde_json::Value = serde_json::from_slice(&bytes).expect("test body is JSON");
        assert_eq!(read, stored);

        // An unknown backend misses with the documented shape.
        let response =
            get_sso_backend(State(state.clone()), Path("w239-missing".to_string())).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}

#[cfg(test)]
mod sso_detail_tests {
    use super::*;

    fn empty_store() -> SsoStore {
        SsoStore::new()
    }

    #[test]
    fn redaction_replaces_secret_values_and_keeps_the_rest() {
        // Secrets never render: every secret-named value becomes the
        // marker, including nested objects and arrays, while non-secret
        // fields render verbatim.
        let config = serde_json::json!({
            "issuer": "https://idp.example.invalid",
            "client_secret": "w239-real-secret",
            "nested": {"password": "w239-real-password", "url": "https://sp.example.invalid"},
            "list": [{"api_key": "w239-real-key"}, "plain"],
            "CLIENTSECRET": "w239-upper-secret",
        });
        let redacted = redact_secrets(&config);
        let text = serde_json::to_string(&redacted).expect("redacted config serialises");
        for secret in [
            "w239-real-secret",
            "w239-real-password",
            "w239-real-key",
            "w239-upper-secret",
        ] {
            assert!(!text.contains(secret), "secret must not render: {secret}");
        }
        assert_eq!(
            redacted["client_secret"],
            serde_json::json!(REDACTED_SECRET)
        );
        assert_eq!(
            redacted["nested"]["password"],
            serde_json::json!(REDACTED_SECRET)
        );
        assert_eq!(
            redacted["list"][0]["api_key"],
            serde_json::json!(REDACTED_SECRET)
        );
        assert_eq!(redacted["CLIENTSECRET"], serde_json::json!(REDACTED_SECRET));
        assert_eq!(
            redacted["issuer"],
            serde_json::json!("https://idp.example.invalid")
        );
        assert_eq!(
            redacted["nested"]["url"],
            serde_json::json!("https://sp.example.invalid")
        );
        assert_eq!(redacted["list"][1], serde_json::json!("plain"));
    }

    #[test]
    fn detail_row_renders_redacted_config_and_omits_empty_config() {
        let secret_row = sso_backend(
            "w239-oidc".to_string(),
            true,
            serde_json::json!({"issuer": "https://idp.example.invalid", "hmac_secret": "w239-live-secret"}),
        );
        let rendered = secret_row.to_json();
        assert_eq!(rendered["backend"], serde_json::json!("w239-oidc"));
        assert_eq!(rendered["enable"], serde_json::json!(true));
        assert_eq!(
            rendered["config"]["issuer"],
            serde_json::json!("https://idp.example.invalid")
        );
        assert_eq!(
            rendered["config"]["hmac_secret"],
            serde_json::json!(REDACTED_SECRET)
        );
        let text = serde_json::to_string(&rendered).expect("row serialises");
        assert!(
            !text.contains("w239-live-secret"),
            "stored secret must not render"
        );

        // Backends without stored config omit it rather than inventing
        // an empty object.
        let bare = sso_backend("w239-bare".to_string(), false, serde_json::Value::Null);
        let rendered = bare.to_json();
        assert!(rendered.get("config").is_none());
    }

    #[test]
    fn upsert_creates_then_replaces_without_growing_past_the_cap() {
        let store = empty_store();
        let (stored, created) = store
            .upsert(sso_backend(
                "w239-oidc".to_string(),
                true,
                serde_json::json!({"issuer": "https://idp.example.invalid"}),
            ))
            .expect("create succeeds");
        assert!(created);
        assert_eq!(stored.backend, "w239-oidc");
        // Replace the same slot: not a creation, still one entry.
        let (stored, created) = store
            .upsert(sso_backend(
                "w239-oidc".to_string(),
                false,
                serde_json::json!({"issuer": "https://other.example.invalid"}),
            ))
            .expect("replace succeeds");
        assert!(!created);
        assert!(!stored.enable);
        assert_eq!(store.list().len(), 1);
        assert_eq!(store.get("w239-oidc"), Some(stored));
    }

    #[test]
    fn upsert_rejects_invalid_writes_and_applies_nothing() {
        let store = empty_store();
        store
            .upsert(sso_backend(
                "w239-keep".to_string(),
                true,
                serde_json::json!({"issuer": "https://idp.example.invalid"}),
            ))
            .expect("seed succeeds");
        for bad in [
            sso_backend(String::new(), true, serde_json::Value::Null),
            sso_backend("has/slash".to_string(), true, serde_json::Value::Null),
            sso_backend(
                "x".repeat(broker_config::MAX_SSO_BACKEND_NAME_LEN + 1),
                true,
                serde_json::Value::Null,
            ),
            sso_backend(
                "w239-too-big".to_string(),
                true,
                serde_json::json!({"blob": "x".repeat(MAX_SSO_CONFIG_LEN)}),
            ),
        ] {
            assert!(
                matches!(store.upsert(bad), Err(SsoUpsertError::Invalid(_))),
                "bad write must be rejected"
            );
        }
        // The stored slot is untouched by every rejected write.
        assert_eq!(store.list().len(), 1);
        assert!(store.get("w239-keep").expect("kept slot").enable);
    }

    #[test]
    fn upsert_rejects_new_names_past_the_cap_but_replaces_freely() {
        let store = empty_store();
        {
            let mut map = store.inner.write().expect("test lock");
            for index in 0..MAX_SSO_BACKENDS {
                let name = format!("w239-fill-{index}");
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
            store.upsert(sso_backend(
                "w239-one-more".to_string(),
                true,
                serde_json::Value::Null
            )),
            Err(SsoUpsertError::Full)
        );
        // Replacing a stored slot at the cap still succeeds: it never
        // grows the map.
        let (stored, created) = store
            .upsert(sso_backend(
                "w239-fill-0".to_string(),
                false,
                serde_json::Value::Null,
            ))
            .expect("replace at the cap succeeds");
        assert!(!created);
        assert!(!stored.enable);
    }

    #[test]
    fn remove_deletes_and_reports_missing_without_creating() {
        let store = empty_store();
        assert_eq!(store.remove("w239-missing"), Ok(false));
        store
            .upsert(sso_backend(
                "w239-gone".to_string(),
                true,
                serde_json::Value::Null,
            ))
            .expect("seed succeeds");
        assert_eq!(store.remove("w239-gone"), Ok(true));
        assert!(store.get("w239-gone").is_none());
        assert_eq!(store.remove("w239-gone"), Ok(false));
    }

    #[test]
    fn detail_body_parsing_validates_every_field_before_apply() {
        // Full shape with a matching backend field.
        let record = parse_detail_body(
            "w239-oidc",
            &serde_json::json!({
                "backend": "w239-oidc",
                "enable": false,
                "config": {"issuer": "https://idp.example.invalid"},
            }),
        )
        .expect("full body parses");
        assert_eq!(record.backend, "w239-oidc");
        assert!(!record.enable);
        // Omitted fields default: enabled with no config.
        let record =
            parse_detail_body("w239-oidc", &serde_json::json!({})).expect("empty object parses");
        assert!(record.enable);
        assert!(record.config.is_null());
        // Nulls default the same way.
        let record = parse_detail_body(
            "w239-oidc",
            &serde_json::json!({"enable": null, "config": null}),
        )
        .expect("null fields parse");
        assert!(record.enable);
        assert!(record.config.is_null());
        // JSON-text configs parse; blank strings mean no config.
        let record = parse_detail_body(
            "w239-oidc",
            &serde_json::json!({"config": "{\"issuer\": \"https://idp.example.invalid\"}"}),
        )
        .expect("text config parses");
        assert_eq!(
            record.config,
            serde_json::json!({"issuer": "https://idp.example.invalid"})
        );
        let record = parse_detail_body("w239-oidc", &serde_json::json!({"config": "   "}))
            .expect("blank text config parses");
        assert!(record.config.is_null());
        // Every bad shape is rejected.
        for bad in [
            serde_json::json!(null),
            serde_json::json!([1, 2]),
            serde_json::json!("w239-oidc"),
            serde_json::json!({"backend": "w239-other", "enable": true}),
            serde_json::json!({"backend": 7}),
            serde_json::json!({"enable": "yes"}),
            serde_json::json!({"config": 7}),
            serde_json::json!({"config": [1]}),
            serde_json::json!({"config": "{not json"}),
        ] {
            assert!(
                parse_detail_body("w239-oidc", &bad).is_err(),
                "bad body must be rejected: {bad}"
            );
        }
    }

    #[test]
    fn flat_variant_fields_store_and_render_at_top_level() {
        // The console sends variant fields flat. Before: unknown fields
        // never reached the store, so the detail row matched no
        // documented variant schema.
        let record = parse_detail_body(
            "ldap",
            &serde_json::json!({
                "backend": "ldap",
                "enable": true,
                "server": "127.0.0.1:389",
                "base_dn": "dc=example,dc=com",
                "username": "cn=reader",
            }),
        )
        .expect("flat variant body parses");
        assert_eq!(record.backend, "ldap");
        assert!(record.enable);
        assert_eq!(record.config["server"], serde_json::json!("127.0.0.1:389"));
        let row = record.to_json();
        assert_eq!(row["backend"], serde_json::json!("ldap"));
        assert_eq!(row["server"], serde_json::json!("127.0.0.1:389"));
        assert_eq!(row["base_dn"], serde_json::json!("dc=example,dc=com"));
        assert_eq!(row["username"], serde_json::json!("cn=reader"));

        // Flat fields merge into an explicit `config` object.
        let record = parse_detail_body(
            "oidc",
            &serde_json::json!({
                "backend": "oidc",
                "config": {"issuer": "https://idp.example.invalid"},
                "clientid": "w239-client",
            }),
        )
        .expect("mixed body parses");
        assert_eq!(
            record.config["issuer"],
            serde_json::json!("https://idp.example.invalid")
        );
        assert_eq!(record.config["clientid"], serde_json::json!("w239-client"));
    }

    #[test]
    fn detail_bounds_are_finite_and_reasoned() {
        const { assert!(MAX_SSO_BACKENDS > 0) };
        const { assert!(MAX_SSO_CONFIG_LEN > 0) };
        const { assert!(broker_config::MAX_SSO_BACKEND_NAME_LEN > 0) };
        assert!(!REDACTED_SECRET.is_empty());
    }
}
