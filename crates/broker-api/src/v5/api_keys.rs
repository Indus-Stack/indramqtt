//! Management API key records for the v5 management API.
//!
//! Covers `GET /api_key` (paged list), `POST /api_key` (mint one key),
//! `DELETE /api_key/{name}` (revoke one key), `GET /api_key/{name}` (one
//! key's metadata), `PUT /api_key/{name}` (change one key) and
//! `GET /api_key_scopes` (fixed scope catalogue) on real state. The
//! shapes follow the reference API. Single-node, management-plane only:
//! handlers clone at most the capped list once per request under a short
//! lock; publish and deliver never touch this store, and key checks on
//! the management plane use constant-time comparison off the hot path
//! (see [`ApiKeyStore::resolve`]).
//!
//! Record fields: `name`, `api_key` (the public identifier, the user name
//! in Basic authentication), `desc`, `enable`, `expired_at` (a time or
//! `infinity`), `expired` (derived), `created_at`, `role` and `scopes` (a
//! list or the text `unset`). `POST /api_key` also returns `api_secret`
//! one time. The input field `description` is an alias of `desc`.
//!
//! Authentication: a client sends `Authorization: Basic
//! base64(api_key:api_secret)`. As an extension, the client can send the
//! secret as `Authorization: Bearer <api_secret>`. The middleware
//! ([`crate::api_auth::require_api_auth`]) finds the key with
//! [`ApiKeyStore::resolve_basic`] or [`ApiKeyStore::resolve`] and applies
//! the rules of the key with [`key_decision`]:
//! - the broker refuses a key that is disabled or expired;
//! - a key with the scope text `unset` follows its role: scopes are the
//!   defaults of the role (all the catalogue scopes for `administrator`
//!   and `viewer`, only `publish` for `publisher`);
//! - a key with an explicit scope list can call only the routes of those
//!   scopes. An empty list denies each route;
//! - a `viewer` key can only read. The middleware applies the viewer
//!   rule of the users who logged in;
//! - the reference lists some operations for a bearer token only
//!   (`GET`/`POST /api_key`, `GET /api_key_scopes`, `GET`/`POST /users`,
//!   `GET /user_scopes`, `POST /logout`). A key cannot call them;
//! - the other operations on API keys, users and single sign-on accept a
//!   key. The scope catalogue has no scope for them, thus only an
//!   `administrator` key without an explicit scope list can call them;
//! - a key cannot call a path that [`access_for_path`] does not know.
//!
//! Store bounds (stated here and enforced below):
//! - at most [`MAX_API_KEYS`] key records; creates past the cap are
//!   rejected instead of growing the map without limit;
//! - names are capped at [`broker_config::MAX_API_KEY_NAME_LEN`] bytes,
//!   scopes at [`broker_config::MAX_API_KEY_SCOPE_LEN`] bytes each and at
//!   most [`broker_config::MAX_API_KEY_SCOPES`] per key, descriptions at
//!   1024 bytes, so one record cannot balloon memory;
//! - the default store is empty.
//!
//! Secret handling: minting generates 32 random bytes (256 bits of
//! entropy, so plain SHA-256 needs no stretching or salt) rendered as
//! URL-safe base64; only the hex SHA-256 hash persists (in memory and
//! through the config registry) and the plaintext appears only in the
//! create response, exactly once. Reads and lists never render secrets or
//! hashes, only metadata.
//!
//! Namespaces: the reference API can scope a key to a namespace. This
//! broker has no namespace model yet, thus a request that sets a
//! namespace gets `BAD_REQUEST`. The broker does not store a key that it
//! cannot limit.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use broker_config::{ApiKeysConf, ConfigRegistry};
use bytes::Bytes;
use rand::Rng;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::errors::ApiError;
use crate::ApiState;

/// Upper bound for stored API keys. Creates past this size are rejected
/// with `BAD_REQUEST` instead of growing the map without limit. Reason:
/// the management plane snapshots the whole list per list request and
/// each entry holds a hash plus metadata, so an unbounded map lets one
/// management batch exhaust the kernel; 1024 covers operator and
/// automation provisioning with headroom while keeping list reads paged
/// and per-request clones cheap.
pub const MAX_API_KEYS: usize = broker_config::MAX_API_KEYS;
/// Longest accepted key `desc` text (1024 bytes). Reason: the
/// description renders in every list row and is cloned per list snapshot
/// plus persisted per entry, so an unbounded text lets one record bloat
/// each list read; 1024 covers free-form operator notes with headroom
/// while keeping rows and per-request clones small.
const MAX_DESCRIPTION_LEN: usize = 1024;

/// The role of a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyRole {
    /// Can call each operation that its scopes allow.
    Administrator,
    /// Can read only.
    Viewer,
    /// Can publish only.
    Publisher,
}

impl KeyRole {
    /// The text of the role in the API.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Administrator => "administrator",
            Self::Viewer => "viewer",
            Self::Publisher => "publisher",
        }
    }

    /// Reads the text of a role. An empty text is `administrator`.
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "" | "administrator" => Some(Self::Administrator),
            "viewer" => Some(Self::Viewer),
            "publisher" => Some(Self::Publisher),
            _ => None,
        }
    }

    /// The scopes that a key with this role has when it holds no
    /// explicit scope list.
    fn default_scopes(self) -> Vec<&'static str> {
        match self {
            Self::Administrator | Self::Viewer => {
                API_KEY_SCOPES.iter().map(|entry| entry.name).collect()
            }
            Self::Publisher => vec!["publish"],
        }
    }
}

/// One stored API key: the name plus the secret hash and metadata.
///
/// The hash is the raw 32-byte SHA-256 of the minted secret; only its
/// hex form ever leaves this struct (registry persistence). Plaintext
/// secrets are never stored here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyRecord {
    name: String,
    /// Public identifier: the user name in Basic authentication.
    key_id: String,
    key_hash: [u8; 32],
    /// `None`: no explicit scope list, the role decides.
    scopes: Option<Vec<String>>,
    description: String,
    created_at: String,
    enable: bool,
    /// Expiry in epoch seconds. `None` never expires.
    expired_at: Option<u64>,
    role: KeyRole,
}

impl ApiKeyRecord {
    /// True when the expiry time is not after `now` (epoch seconds).
    fn is_expired(&self, now: u64) -> bool {
        self.expired_at.is_some_and(|at| now >= at)
    }

    /// True when the key can authenticate at `now`.
    fn is_usable(&self, now: u64) -> bool {
        self.enable && !self.is_expired(now)
    }

    /// Metadata row at the current time. The secret and its hash never
    /// render.
    pub fn to_json(&self) -> serde_json::Value {
        self.to_json_at(now_secs())
    }

    /// Metadata row at `now` (epoch seconds): everything that a read
    /// renders. The secret and its hash never render.
    fn to_json_at(&self, now: u64) -> serde_json::Value {
        let expired_at = match self.expired_at {
            Some(at) => serde_json::Value::String(epoch_to_rfc3339(at)),
            None => serde_json::Value::String("infinity".to_string()),
        };
        let scopes = match &self.scopes {
            Some(list) => serde_json::json!(list),
            None => serde_json::Value::String("unset".to_string()),
        };
        serde_json::json!({
            "name": self.name,
            "api_key": self.key_id,
            "desc": self.description,
            "enable": self.enable,
            "expired": self.is_expired(now),
            "expired_at": expired_at,
            "created_at": self.created_at,
            "role": self.role.as_str(),
            "scopes": scopes,
        })
    }
}

/// The fields of a new key.
#[derive(Debug, Clone)]
pub struct NewApiKey {
    pub name: String,
    pub description: String,
    pub enable: bool,
    /// Expiry in epoch seconds. `None` never expires.
    pub expired_at: Option<u64>,
    pub role: KeyRole,
    /// `None` is the text `unset`: the role decides.
    pub scopes: Option<Vec<String>>,
}

impl NewApiKey {
    /// A key with the defaults of the reference API.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: String::new(),
            enable: true,
            expired_at: None,
            role: KeyRole::Administrator,
            scopes: None,
        }
    }
}

/// The changes of an update. A `None` field keeps the stored value.
#[derive(Debug, Clone, Default)]
pub struct ApiKeyPatch {
    pub description: Option<String>,
    pub enable: Option<bool>,
    /// `Some(None)` sets `infinity`.
    pub expired_at: Option<Option<u64>>,
    pub role: Option<KeyRole>,
    /// `Some(None)` sets the text `unset`.
    pub scopes: Option<Option<Vec<String>>>,
}

/// In-memory API key directory keyed by name.
///
/// Single-node map behind one lock; every method finishes quickly.
/// Management writes take the write lock; the key check
/// ([`ApiKeyStore::resolve`]) takes a short read lock and never mutates,
/// so delivery never blocks on a management write. Publish and deliver
/// never touch this store.
pub struct ApiKeyStore {
    inner: RwLock<HashMap<String, ApiKeyRecord>>,
    /// Kernel config registry receiving every mutation (`None` in unit
    /// tests and standalone state, which stay memory-only).
    registry: RwLock<Option<Arc<ConfigRegistry>>>,
    /// Serialises export-commit-save so concurrent mutations cannot
    /// interleave into a lost update on disk.
    save_lock: Mutex<()>,
}

impl ApiKeyStore {
    /// Empty directory.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            registry: RwLock::new(None),
            save_lock: Mutex::new(()),
        }
    }

    fn read_map(&self) -> RwLockReadGuard<'_, HashMap<String, ApiKeyRecord>> {
        self.inner.read().unwrap_or_else(|p| p.into_inner())
    }

    fn write_map(&self) -> RwLockWriteGuard<'_, HashMap<String, ApiKeyRecord>> {
        self.inner.write().unwrap_or_else(|p| p.into_inner())
    }

    /// Seed from a validated snapshot root. Memory-only.
    pub fn from_snapshot(conf: &ApiKeysConf) -> Self {
        let store = Self::new();
        store.seed_from_snapshot(conf);
        store
    }

    /// Seed from the registry's current snapshot and persist every later
    /// mutation back through it. Kernel boot path; an empty snapshot
    /// yields today's empty behaviour.
    pub fn from_registry(registry: &Arc<ConfigRegistry>) -> Self {
        let store = Self::from_snapshot(&registry.snapshot().api_keys);
        *store.registry.write().unwrap_or_else(|p| p.into_inner()) = Some(Arc::clone(registry));
        store
    }

    /// Attach the registry and replace the current contents with its
    /// snapshot, in place on the same instance.
    pub fn seed_from_registry(&self, registry: &Arc<ConfigRegistry>) {
        self.seed_from_snapshot(&registry.snapshot().api_keys);
        *self.registry.write().unwrap_or_else(|p| p.into_inner()) = Some(Arc::clone(registry));
    }

    fn seed_from_snapshot(&self, conf: &ApiKeysConf) {
        let mut map = self.write_map();
        map.clear();
        for entry in &conf.keys {
            let Ok(hash) = parse_hex_hash(&entry.key_hash) else {
                // Fail closed: an undecodable hash can never verify, so
                // the entry is dropped instead of stored with a weak
                // placeholder. The loss is visible as a missing row in
                // the key list instead of a silently open key.
                tracing::warn!(
                    name = entry.name.as_str(),
                    "api-key snapshot entry has an undecodable key hash; entry dropped"
                );
                continue;
            };
            let Some(role) = KeyRole::parse(&entry.role) else {
                tracing::warn!(
                    name = entry.name.as_str(),
                    "api-key snapshot entry has an unknown role; entry dropped"
                );
                continue;
            };
            // A record from an older release has no explicit flag. An
            // empty list there meant "no scope policy"; a list with
            // entries was explicit.
            let scopes = if entry.scopes_set || !entry.scopes.is_empty() {
                Some(entry.scopes.clone())
            } else {
                None
            };
            // A record from an older release has no identifier: its
            // name is the identifier.
            let key_id = if entry.key_id.is_empty() {
                entry.name.clone()
            } else {
                entry.key_id.clone()
            };
            map.insert(
                entry.name.clone(),
                ApiKeyRecord {
                    name: entry.name.clone(),
                    key_id,
                    key_hash: hash,
                    scopes,
                    description: entry.description.clone(),
                    created_at: entry.created_at.clone(),
                    enable: entry.enable,
                    expired_at: entry.expired_at,
                    role,
                },
            );
        }
    }

    /// Snapshot of live records sorted by name for stable pages.
    pub fn list(&self) -> Vec<ApiKeyRecord> {
        let map = self.read_map();
        let mut out: Vec<ApiKeyRecord> = map.values().cloned().collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// Finds the key for a Basic request: the identifier and the secret
    /// must both match. Returns `None` when no key matches, when the key
    /// is disabled and when the key is expired. See
    /// [`ApiKeyStore::resolve`] for the timing rules.
    pub fn resolve_basic(&self, key_id: &str, secret: &str) -> Option<ResolvedApiKey> {
        if key_id.is_empty() || key_id.len() > broker_config::MAX_API_KEY_NAME_LEN {
            return None;
        }
        self.resolve_inner(Some(key_id), secret)
    }

    /// Finds the key that has this secret (the Bearer extension). Returns
    /// the name, the role and the scopes of the key, or `None` when no
    /// key has the secret, when the key is disabled and when the key is
    /// expired.
    ///
    /// The function hashes the candidate one time and compares the hash
    /// with each stored hash in constant time. It does not stop at the
    /// first match, thus the time does not show which key matched. The
    /// work is at most [`MAX_API_KEYS`] comparisons of 32 bytes under one
    /// short read lock, for each management request that carries a key.
    pub fn resolve(&self, secret: &str) -> Option<ResolvedApiKey> {
        self.resolve_inner(None, secret)
    }

    fn resolve_inner(&self, key_id: Option<&str>, secret: &str) -> Option<ResolvedApiKey> {
        if secret.is_empty() {
            return None;
        }
        let candidate = hash_secret(secret);
        let now = now_secs();
        let map = self.read_map();
        let mut found: Option<&ApiKeyRecord> = None;
        for record in map.values() {
            let hash_match = constant_time_eq(&candidate, &record.key_hash);
            let id_match = match key_id {
                Some(id) => constant_time_eq_bytes(id.as_bytes(), record.key_id.as_bytes()),
                None => true,
            };
            // `&` and not `&&`: both comparisons always run.
            if hash_match & id_match {
                found = Some(record);
            }
        }
        found
            .filter(|record| record.is_usable(now))
            .map(|record| ResolvedApiKey {
                name: record.name.clone(),
                grant: KeyGrant {
                    scopes: record.scopes.clone(),
                    role: record.role,
                },
            })
    }

    /// Mint one key. Fails cleanly when the name is already stored
    /// (caller maps to `BAD_REQUEST`) or the store is at [`MAX_API_KEYS`]
    /// (caller maps to `BAD_REQUEST`). Returns the metadata row plus the
    /// plaintext secret, which the caller renders exactly once and never
    /// stores. A scope list that equals the default of the role is stored
    /// as no list. Persists through the registry when one is attached; a
    /// commit or save failure rolls back the in-memory insert and
    /// surfaces as [`ApiKeyInsertError::Persist`] (commit precedes the
    /// atomic save, mirroring the credential stores).
    pub fn create(&self, new: NewApiKey) -> Result<(ApiKeyRecord, String), ApiKeyInsertError> {
        let mut secret_bytes = [0u8; 32];
        rand::rng().fill_bytes(&mut secret_bytes);
        let secret = URL_SAFE_NO_PAD.encode(secret_bytes);
        let scopes = normalize_scopes(new.role, new.scopes);
        let record = {
            let mut map = self.write_map();
            if map.contains_key(&new.name) {
                return Err(ApiKeyInsertError::Duplicate);
            }
            if map.len() >= MAX_API_KEYS {
                return Err(ApiKeyInsertError::Full);
            }
            let mut key_id = String::new();
            for _ in 0..8 {
                let mut id_bytes = [0u8; 8];
                rand::rng().fill_bytes(&mut id_bytes);
                let candidate = encode_hex(&id_bytes);
                if !map.values().any(|record| record.key_id == candidate) {
                    key_id = candidate;
                    break;
                }
            }
            if key_id.is_empty() {
                return Err(ApiKeyInsertError::Persist(
                    "cannot generate a unique key identifier".to_string(),
                ));
            }
            let record = ApiKeyRecord {
                name: new.name.clone(),
                key_id,
                key_hash: hash_secret(&secret),
                scopes,
                description: new.description,
                created_at: utc_rfc3339_now(),
                enable: new.enable,
                expired_at: new.expired_at,
                role: new.role,
            };
            map.insert(new.name, record.clone());
            record
        };
        if let Err(reason) = self.persist() {
            // The in-memory insert is rolled back (removed) so memory
            // stays consistent with what persisted; the failure is
            // never silent.
            self.write_map().remove(&record.name);
            return Err(ApiKeyInsertError::Persist(reason));
        }
        Ok((record, secret))
    }

    /// Revoke the key stored under `name`. Returns true when a record
    /// was removed, false when nothing was stored under the name.
    /// Persists through the registry when one is attached; a commit or
    /// save failure restores the removed record and surfaces as an error
    /// message so the loss is never silent.
    pub fn remove(&self, name: &str) -> Result<bool, String> {
        let removed = self.write_map().remove(name);
        let Some(record) = removed else {
            return Ok(false);
        };
        if let Err(reason) = self.persist() {
            self.write_map().insert(name.to_string(), record);
            return Err(reason);
        }
        Ok(true)
    }

    /// Read one key's metadata row. Returns a clone under a short read
    /// lock, or `None` when nothing is stored under `name`.
    pub fn get(&self, name: &str) -> Option<ApiKeyRecord> {
        self.read_map().get(name).cloned()
    }

    /// Change one key in place. `None` fields of the patch keep the
    /// stored value; `Some` fields must already be validated (same caps
    /// as minting) and replace verbatim. A scope list that equals the
    /// default of the final role is stored as no list. Unknown names fail
    /// with [`ApiKeyUpdateError::NotFound`] and create nothing; the
    /// secret hash, the identifier and the mint time never change here.
    /// Persists through the registry when one is attached; a commit or
    /// save failure restores the previous record and surfaces as
    /// [`ApiKeyUpdateError::Persist`] so the loss is never silent.
    pub fn update(
        &self,
        name: &str,
        patch: ApiKeyPatch,
    ) -> Result<ApiKeyRecord, ApiKeyUpdateError> {
        let previous = {
            let mut map = self.write_map();
            let Some(record) = map.get_mut(name) else {
                return Err(ApiKeyUpdateError::NotFound);
            };
            let previous = record.clone();
            if let Some(role) = patch.role {
                record.role = role;
            }
            if let Some(scopes) = patch.scopes {
                record.scopes = normalize_scopes(record.role, scopes);
            }
            if let Some(description) = patch.description {
                record.description = description;
            }
            if let Some(enable) = patch.enable {
                record.enable = enable;
            }
            if let Some(expired_at) = patch.expired_at {
                record.expired_at = expired_at;
            }
            previous
        };
        if let Err(reason) = self.persist() {
            // Restore the previous row so memory stays consistent with
            // what persisted; the failure is never silent.
            self.write_map().insert(name.to_string(), previous);
            return Err(ApiKeyUpdateError::Persist(reason));
        }
        self.get(name).ok_or(ApiKeyUpdateError::NotFound)
    }

    /// Export the current contents as a validated config root, sorted by
    /// name so the persisted file is deterministic.
    fn export_conf(&self) -> ApiKeysConf {
        let map = self.read_map();
        let mut entries: Vec<broker_config::ApiKeyEntry> = map
            .values()
            .map(|record| broker_config::ApiKeyEntry {
                name: record.name.clone(),
                key_hash: encode_hex(&record.key_hash),
                key_id: record.key_id.clone(),
                scopes: record.scopes.clone().unwrap_or_default(),
                scopes_set: record.scopes.is_some(),
                description: record.description.clone(),
                created_at: record.created_at.clone(),
                enable: record.enable,
                expired_at: record.expired_at,
                role: record.role.as_str().to_string(),
            })
            .collect();
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        ApiKeysConf { keys: entries }
    }

    /// Commit the exported root and atomically save it. A no-op without
    /// a registry; any commit or save failure surfaces as a message so
    /// handlers answer 500 and the loss is never silent.
    fn persist(&self) -> Result<(), String> {
        let registry = self
            .registry
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let Some(registry) = registry else {
            return Ok(());
        };
        let _guard = self.save_lock.lock().unwrap_or_else(|p| p.into_inner());
        let conf = self.export_conf();
        registry
            .commit_api_keys(conf)
            .map_err(|err| format!("cannot persist api keys: {err}"))?;
        registry
            .save()
            .map_err(|err| format!("cannot persist api keys: {err}"))?;
        Ok(())
    }
}

impl Default for ApiKeyStore {
    fn default() -> Self {
        Self::new()
    }
}

/// What the broker knows about a key after it found the key: the role and
/// the scopes. The middleware applies it to each request path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyGrant {
    /// `None`: no explicit list, the role decides. `Some(empty)` denies
    /// each scope.
    pub scopes: Option<Vec<String>>,
    pub role: KeyRole,
}

impl KeyGrant {
    /// True when the key has the scope, from its list or from the default
    /// of its role.
    fn has_scope(&self, needed: &str) -> bool {
        match &self.scopes {
            Some(list) => list.iter().any(|scope| scope == needed),
            None => self.role.default_scopes().contains(&needed),
        }
    }
}

/// The identity of a key that [`ApiKeyStore::resolve`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedApiKey {
    /// The name of the key.
    pub name: String,
    /// The role and the scopes of the key.
    pub grant: KeyGrant,
}

/// What a key needs to call a path of the management API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathAccess {
    /// The reference API has no authentication on the path.
    Anonymous,
    /// Each key can call the path, unless the key denies each scope. These
    /// paths give no control and show no configuration.
    AnyKey,
    /// The key must have this scope (from its list or from its role).
    Scope(&'static str),
    /// The scope catalogue has no scope for the path (users, single
    /// sign-on, one API key). Only an `administrator` key with no
    /// explicit scope list can call it.
    RoleOnly,
    /// The reference API lists the operation for a bearer token only. No
    /// key can call it.
    BearerOnly,
    /// No key can call the path. A path that is not in the table is in
    /// this group.
    Closed,
}

/// The first segments under `/api/v5` that belong to the public single
/// sign-on flow. A key cannot call them through the key table.
const SSO_FLOW_SEGMENTS: [&str; 6] = ["login", "mfa", "running", "oidc", "saml", "token_exchange"];

/// Gives the access rule of a request path. The rule comes from the
/// first segment after `/api/v5` or `/api/v1`, and for the operations
/// with a special rule in the reference API from the whole path.
///
/// The table must have an entry for each first segment that the router
/// has. A segment without an entry gets [`PathAccess::Closed`], thus a
/// new route is closed for keys until it has an entry here.
pub fn access_for_path(path: &str) -> PathAccess {
    let trimmed = path.trim();
    let native = trimmed.starts_with("/api/v1/");
    let group = crate::v5::audit::operation_type_for_path(trimmed);
    if native {
        return match group.as_str() {
            "nodes" => PathAccess::Scope("cluster_operations"),
            "clients" => PathAccess::Scope("connections"),
            "metrics" => PathAccess::Scope("monitoring"),
            "connectors" | "rules" => PathAccess::Scope("data_integration"),
            "auth" => PathAccess::Scope("access_control"),
            "config" => PathAccess::Scope("system"),
            _ => PathAccess::Closed,
        };
    }
    let without_prefix = trimmed.strip_prefix("/api/v5").unwrap_or(trimmed);
    let segments: Vec<&str> = without_prefix
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    match segments.as_slice() {
        // The reference lists these operations for a bearer token only.
        ["api_key"] | ["api_key_scopes"] | ["users"] | ["user_scopes"] | ["logout"] => {
            return PathAccess::BearerOnly;
        }
        // The reference lists these operations for Basic and bearer.
        ["api_key", _] | ["users", _] | ["users", _, "change_pwd"] | ["users", _, "mfa"] => {
            return PathAccess::RoleOnly;
        }
        ["sso"] => return PathAccess::RoleOnly,
        ["sso", backend] if !SSO_FLOW_SEGMENTS.contains(backend) => {
            return PathAccess::RoleOnly;
        }
        ["api_key", ..] | ["users", ..] | ["sso", ..] => return PathAccess::Closed,
        _ => {}
    }
    match group.as_str() {
        "status" | "error_codes" => PathAccess::Anonymous,
        "current_user" | "features" => PathAccess::AnyKey,
        "clients" | "clients_v2" | "subscriptions" | "topics" | "sessions_count" | "banned"
        | "queues" | "queue" | "streams" | "stream" => PathAccess::Scope("connections"),
        "publish" => PathAccess::Scope("publish"),
        "rules"
        | "rule_engine"
        | "rule_events"
        | "rule_test"
        | "ai"
        | "actions"
        | "actions_probe"
        | "actions_summary"
        | "action_types"
        | "connectors"
        | "connectors_probe"
        | "sources"
        | "sources_probe"
        | "sources_summary"
        | "source_types"
        | "schema_registry"
        | "schemas"
        | "message_transformations" => PathAccess::Scope("data_integration"),
        "authentication" | "authorization" => PathAccess::Scope("access_control"),
        "gateways" => PathAccess::Scope("gateways"),
        "metrics" | "monitor" | "monitor_current" | "stats" | "alarms" | "trace" | "tracing"
        | "slow_subscriptions" | "telemetry" => PathAccess::Scope("monitoring"),
        "nodes" | "load_rebalance" | "node_eviction" => PathAccess::Scope("cluster_operations"),
        "listeners" | "mqtt" | "data" => PathAccess::Scope("system"),
        "audit" => PathAccess::Scope("audit"),
        "license" => PathAccess::Scope("license"),
        _ => PathAccess::Closed,
    }
}

/// The answer of [`key_decision`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyDecision {
    /// The key can call the path.
    Allowed,
    /// The operation accepts only the token of a user who logged in.
    LoginOnly,
    /// The key has no right for the path (scope, role or unknown path).
    Forbidden,
}

/// Decides if a key with this grant can call the path. See
/// [`access_for_path`] and the module documentation. The viewer rule
/// (read only) is not here: the middleware applies it to the identity.
pub fn key_decision(grant: &KeyGrant, path: &str) -> KeyDecision {
    let denies_all = matches!(&grant.scopes, Some(list) if list.is_empty());
    match access_for_path(path) {
        PathAccess::Anonymous => KeyDecision::Allowed,
        PathAccess::AnyKey => {
            if denies_all {
                KeyDecision::Forbidden
            } else {
                KeyDecision::Allowed
            }
        }
        PathAccess::Scope(needed) => {
            if grant.role == KeyRole::Publisher && needed != "publish" {
                KeyDecision::Forbidden
            } else if grant.has_scope(needed) {
                KeyDecision::Allowed
            } else {
                KeyDecision::Forbidden
            }
        }
        PathAccess::RoleOnly => {
            if grant.scopes.is_none() && grant.role == KeyRole::Administrator {
                KeyDecision::Allowed
            } else {
                KeyDecision::Forbidden
            }
        }
        PathAccess::BearerOnly => KeyDecision::LoginOnly,
        PathAccess::Closed => KeyDecision::Forbidden,
    }
}

/// True when a key with this grant can call the path.
pub fn key_may_call(grant: &KeyGrant, path: &str) -> bool {
    key_decision(grant, path) == KeyDecision::Allowed
}

/// Why [`ApiKeyStore::create`] refused a mint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiKeyInsertError {
    /// The name is already stored.
    Duplicate,
    /// The store holds [`MAX_API_KEYS`] records.
    Full,
    /// The in-memory insert applied but the registry commit or atomic
    /// save failed. Handlers map this to HTTP 500; it is never silent.
    Persist(String),
}

/// Why [`ApiKeyStore::update`] refused a replace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiKeyUpdateError {
    /// Nothing is stored under the name; nothing was created.
    NotFound,
    /// The in-memory replace applied but the registry commit or atomic
    /// save failed (the previous row was restored). Handlers map this
    /// to HTTP 500; it is never silent.
    Persist(String),
}

/// Stores no list when the list equals the default scopes of the role
/// (the reference API says that such a list means "no explicit scope
/// policy"). Removes duplicate scopes and keeps the order.
fn normalize_scopes(role: KeyRole, scopes: Option<Vec<String>>) -> Option<Vec<String>> {
    let list = scopes?;
    let mut unique: Vec<String> = Vec::with_capacity(list.len());
    for scope in list {
        if !unique.contains(&scope) {
            unique.push(scope);
        }
    }
    let defaults = role.default_scopes();
    let same_as_default = unique.len() == defaults.len()
        && defaults
            .iter()
            .all(|default| unique.iter().any(|scope| scope.as_str() == *default));
    if same_as_default {
        None
    } else {
        Some(unique)
    }
}

/// SHA-256 of the secret string bytes. The secret is 32 fresh random
/// bytes per mint (256 bits of entropy), so no salt or stretching is
/// needed: only an exact guess verifies, and the hash never leaves the
/// store except as hex in the persisted snapshot.
fn hash_secret(secret: &str) -> [u8; 32] {
    Sha256::digest(secret.as_bytes()).into()
}

fn constant_time_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for i in 0..32 {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

/// Constant-time comparison of two byte strings of any length. The time
/// depends on the longer string, not on the position of a difference.
fn constant_time_eq_bytes(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() ^ b.len();
    for i in 0..a.len().max(b.len()) {
        let x = a.get(i).copied().unwrap_or(0);
        let y = b.get(i).copied().unwrap_or(0);
        diff |= usize::from(x ^ y);
    }
    diff == 0
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn parse_hex_hash(text: &str) -> Result<[u8; 32], ()> {
    if text.len() != 64 {
        return Err(());
    }
    let mut out = [0u8; 32];
    for (i, chunk) in text.as_bytes().chunks(2).enumerate() {
        let hi = hex_val(chunk[0]).ok_or(())?;
        let lo = hex_val(chunk[1]).ok_or(())?;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

fn hex_val(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Current time in epoch seconds.
fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

/// Current UTC time as `YYYY-MM-DDTHH:MM:SS+00:00` (always UTC).
fn utc_rfc3339_now() -> String {
    epoch_to_rfc3339(now_secs())
}

pub(crate) fn epoch_to_rfc3339(secs: u64) -> String {
    let (year, month, day, hour, min, sec) = civil_from_epoch(secs);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}+00:00")
}

fn civil_from_epoch(secs: u64) -> (i64, u32, u32, u32, u32, u32) {
    let days = (secs / 86_400) as i64;
    let second_of_day = (secs % 86_400) as u32;
    let (year, month, day) = civil_from_days(days);
    (
        year,
        month,
        day,
        second_of_day / 3600,
        (second_of_day % 3600) / 60,
        second_of_day % 60,
    )
}

// Days since 1970-01-01 to year/month/day (Howard Hinnant's algorithm,
// public-domain date arithmetic also used by the ban list's timestamps).
pub(crate) fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = (z - era * 146_097) as u64;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era as i64 + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_prime + 2) / 5 + 1) as u32;
    let month = (if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    }) as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

/// `GET /api_key`: a bare JSON array of the key records, sorted by name.
/// The spec declares one object for this body, but the management console
/// reads an array (`const data: APIKey[] = await http.get('/api_key')`),
/// thus the array is the shape. The store holds at most [`MAX_API_KEYS`]
/// records, so the array is bounded and has no paging. An empty store
/// reads as an empty array, never an error. Secrets and hashes are never
/// rendered. Query parameters are ignored.
pub async fn list_api_keys(State(state): State<ApiState>) -> Response {
    let now = now_secs();
    let data: Vec<serde_json::Value> = state
        .api_keys
        .list()
        .iter()
        .map(|record| record.to_json_at(now))
        .collect();
    (StatusCode::OK, Json(data)).into_response()
}

/// `POST /api_key`: mint one key. Answers 200 with the metadata plus the
/// secret under `api_secret`; the secret is rendered exactly once and
/// never stored in plaintext. Malformed bodies fail with `BAD_REQUEST`;
/// an existing name fails with `BAD_REQUEST` instead of rotating the
/// secret in place; a full store fails with `BAD_REQUEST`.
pub async fn create_api_key(State(state): State<ApiState>, body: Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid api-key body: {error}")).into_response();
        }
    };
    let new = match parse_create_request(&value) {
        Ok(parsed) => parsed,
        Err(message) => return ApiError::BadRequest(message).into_response(),
    };
    match state.api_keys.create(new) {
        Ok((record, secret)) => {
            let mut body = record.to_json();
            body["api_secret"] = serde_json::Value::String(secret);
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(ApiKeyInsertError::Duplicate) => {
            ApiError::BadRequest("api key already exists".to_string()).into_response()
        }
        Err(ApiKeyInsertError::Full) => {
            ApiError::BadRequest("api-key store is full".to_string()).into_response()
        }
        Err(ApiKeyInsertError::Persist(message)) => internal_error(message),
    }
}

fn internal_error(message: String) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({
            "code": "INTERNAL_ERROR",
            "message": message,
        })),
    )
        .into_response()
}

/// `DELETE /api_key/{name}`: revoke one key. A stored name reports
/// success with 204; an unknown name reports `NOT_FOUND` in the
/// documented `{code, message}` shape instead of success.
pub async fn delete_api_key(State(state): State<ApiState>, Path(name): Path<String>) -> Response {
    match state.api_keys.remove(&name) {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => ApiError::NotFound(format!("api key {name:?} not found")).into_response(),
        Err(message) => internal_error(message),
    }
}

/// `GET /api_key/{name}`: read one key's metadata.
///
/// Returns the stored metadata row without any secret material; the
/// secret and its hash never render. An unknown name reports
/// `NOT_FOUND` in the documented `{code, message}` shape. The route is
/// not paged.
/// Management-plane only: one short read lock per request; publish and
/// deliver never touch this store.
pub async fn get_api_key(State(state): State<ApiState>, Path(name): Path<String>) -> Response {
    match state.api_keys.get(&name) {
        Some(record) => (StatusCode::OK, Json(record.to_json())).into_response(),
        None => ApiError::NotFound(format!("api key {name:?} not found")).into_response(),
    }
}

/// `PUT /api_key/{name}`: change one key.
///
/// The body must be a JSON object. Each field that the body holds
/// replaces the stored value: `desc` (or its `description` alias),
/// `enable`, `expired_at`, `role` and `scopes`. A field that the body
/// omits, or that is `null`, keeps the stored value, with one exception:
/// an omitted `expired_at` sets `infinity`, because the console omits it
/// to say "never expires". The field `expired`
/// is read only and the broker ignores it. Every supplied field is
/// validated before anything is applied: bad values fail with
/// `BAD_REQUEST` and apply nothing. An unknown name fails with
/// `NOT_FOUND` and creates nothing. On success returns 200 with the
/// stored metadata row (never the secret). The secret hash, the
/// identifier and the mint time never change here. The write persists
/// across a restart through the config registry. The route is not paged.
/// Management-plane only: one short write lock per request (plus the
/// registry save lock on persist); publish and deliver never touch this
/// store. An update replaces in place and never grows the map toward
/// [`MAX_API_KEYS`].
pub async fn update_api_key(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid api-key body: {error}")).into_response();
        }
    };
    let patch = match parse_update_request(&value) {
        Ok(parsed) => parsed,
        Err(message) => return ApiError::BadRequest(message).into_response(),
    };
    match state.api_keys.update(&name, patch) {
        Ok(record) => (StatusCode::OK, Json(record.to_json())).into_response(),
        Err(ApiKeyUpdateError::NotFound) => {
            ApiError::NotFound(format!("api key {name:?} not found")).into_response()
        }
        Err(ApiKeyUpdateError::Persist(message)) => internal_error(message),
    }
}

/// One entry of the fixed API-key scope catalogue: the stable scope
/// name stored on key records plus a short operator-facing summary.
struct ScopeEntry {
    name: &'static str,
    desc: &'static str,
}

/// Fixed catalogue of assignable API-key scopes (C05-06).
///
/// Bound (stated here and enforced by construction): exactly ten
/// entries, no growth; the list lives in code, so there is no
/// persistence beyond it and no per-request growth. Each summary is a
/// short static string, so serving the catalogue allocates only the
/// small bounded response body once per request.
///
/// This is the single registry the key create/update validation shares
/// with the catalogue route: [`is_known_api_key_scope`] consults this
/// same list. A create or an update refuses a scope that is not in the
/// list, because [`key_may_call`] gives a key with scopes only the
/// routes of those scopes, and a name with a spelling error would give
/// a key that can call almost nothing.
// TODO(parity): what are the exact operator-facing summaries per
// scope? The rulebook does not decide the wording; the current choice
// describes each scope's area in our own words and keeps the stable
// scope names (protocol values) exact until the checker pins them.
const API_KEY_SCOPES: [ScopeEntry; 10] = [
    ScopeEntry {
        name: "connections",
        desc: "View and manage client connections, subscriptions and topics",
    },
    ScopeEntry {
        name: "publish",
        desc: "Publish MQTT messages through the management API",
    },
    ScopeEntry {
        name: "data_integration",
        desc: "Manage rules, connectors and data integrations",
    },
    ScopeEntry {
        name: "access_control",
        desc: "Manage client authentication and authorization settings",
    },
    ScopeEntry {
        name: "gateways",
        desc: "Manage protocol gateways and their clients",
    },
    ScopeEntry {
        name: "monitoring",
        desc: "Read metrics, alarms, traces and monitoring data",
    },
    ScopeEntry {
        name: "cluster_operations",
        desc: "Manage cluster membership and node operations",
    },
    ScopeEntry {
        name: "system",
        desc: "Manage core configuration, listeners and system settings",
    },
    ScopeEntry {
        name: "audit",
        desc: "Query the audit log",
    },
    ScopeEntry {
        name: "license",
        desc: "Read the licence document and manage the licence settings",
    },
];

/// Whether `name` is one of the assignable catalogue scopes.
///
/// Shared by the key create/update validation path and the catalogue
/// route so both read the same registry.
pub fn is_known_api_key_scope(name: &str) -> bool {
    API_KEY_SCOPES.iter().any(|entry| entry.name == name)
}

/// `GET /api_key_scopes`: fixed catalogue of assignable scopes
/// (C05-06).
///
/// Returns `{"scopes": [{"name", "desc"}]}` in catalogue order. The
/// route is not paged and takes no parameters; unknown query
/// parameters are ignored (the handler takes no extractor, so Axum
/// never parses them). Management-plane only: reads a code-defined
/// list, takes no lock, and publish/deliver never touch it.
pub async fn list_api_key_scopes() -> Response {
    let scopes: Vec<serde_json::Value> = API_KEY_SCOPES
        .iter()
        .map(|entry| {
            serde_json::json!({
                "name": entry.name,
                "desc": entry.desc,
            })
        })
        .collect();
    (
        StatusCode::OK,
        Json(serde_json::json!({ "scopes": scopes })),
    )
        .into_response()
}

/// Checks the name rule of the reference API: 1 to 256 characters,
/// `^[A-Za-z]+[A-Za-z0-9-_]*$`.
fn validate_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("field `name` must not be empty".to_string());
    }
    if name.len() > broker_config::MAX_API_KEY_NAME_LEN {
        return Err(format!(
            "field `name` exceeds {} characters",
            broker_config::MAX_API_KEY_NAME_LEN
        ));
    }
    let mut chars = name.chars();
    let first_is_letter = chars.next().is_some_and(|c| c.is_ascii_alphabetic());
    let rest_is_valid = chars.all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if !first_is_letter || !rest_is_valid {
        return Err(
            "field `name` must start with a letter and hold only letters, digits, `-` and `_`"
                .to_string(),
        );
    }
    Ok(())
}

/// Reads `expired_at`: epoch seconds as an integer, an ISO 8601 text
/// with a time zone, or the text `infinity` (no expiry, `None`).
fn parse_expired_at(value: &serde_json::Value) -> Result<Option<u64>, String> {
    match value {
        serde_json::Value::Number(number) => number
            .as_u64()
            .map(Some)
            .ok_or_else(|| "field `expired_at` must be a non-negative integer".to_string()),
        serde_json::Value::String(text) if text == "infinity" => Ok(None),
        serde_json::Value::String(text) => {
            let parsed = chrono::DateTime::parse_from_rfc3339(text).map_err(|_| {
                "field `expired_at` must be `infinity`, epoch seconds or an ISO 8601 time"
                    .to_string()
            })?;
            u64::try_from(parsed.timestamp())
                .map(Some)
                .map_err(|_| "field `expired_at` must not be before 1970".to_string())
        }
        _ => Err(
            "field `expired_at` must be `infinity`, epoch seconds or an ISO 8601 time".to_string(),
        ),
    }
}

/// Reads `scopes`: a list of catalogue scopes, or the text `unset`
/// (`None`, the role decides).
fn parse_scopes(value: &serde_json::Value) -> Result<Option<Vec<String>>, String> {
    match value {
        serde_json::Value::String(text) if text == "unset" => Ok(None),
        serde_json::Value::Array(items) => {
            if items.len() > broker_config::MAX_API_KEY_SCOPES {
                return Err(format!(
                    "field `scopes` holds {} entries, at most {}",
                    items.len(),
                    broker_config::MAX_API_KEY_SCOPES
                ));
            }
            let mut out = Vec::with_capacity(items.len());
            for (index, item) in items.iter().enumerate() {
                let scope = item
                    .as_str()
                    .ok_or_else(|| format!("field `scopes[{index}]` must be a string"))?;
                if scope.trim().is_empty() || scope.len() > broker_config::MAX_API_KEY_SCOPE_LEN {
                    return Err(format!("field `scopes[{index}]` is invalid"));
                }
                if !is_known_api_key_scope(scope) {
                    return Err(format!(
                        "field `scopes[{index}]` is not a known scope; see /api_key_scopes"
                    ));
                }
                out.push(scope.to_string());
            }
            Ok(Some(out))
        }
        _ => Err("field `scopes` must be an array of strings or the text `unset`".to_string()),
    }
}

/// Fields that a create body and an update body share.
#[derive(Default)]
struct SharedFields {
    description: Option<String>,
    enable: Option<bool>,
    expired_at: Option<Option<u64>>,
    role: Option<KeyRole>,
    scopes: Option<Option<Vec<String>>>,
}

/// Reads the shared fields of a body. A missing field and a `null` field
/// are both "not supplied". The field `expired` is read only and the
/// function ignores it. The function refuses a namespace: this broker
/// has no namespace model.
fn parse_shared_fields(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<SharedFields, String> {
    let supplied = |key: &str| obj.get(key).filter(|value| !value.is_null());
    let no_namespace = || {
        "namespaces are not supported on this node: omit `namespace` and use a role without \
         the `ns:` prefix"
            .to_string()
    };
    if let Some(namespace) = supplied("namespace") {
        match namespace.as_str() {
            Some("") => {}
            Some(_) => return Err(no_namespace()),
            None => return Err("field `namespace` must be a string".to_string()),
        }
    }
    let mut fields = SharedFields::default();
    // `desc` is the name in the reference API; `description` is an alias.
    let text = match (supplied("desc"), supplied("description")) {
        (Some(value), _) => Some(("desc", value)),
        (None, Some(value)) => Some(("description", value)),
        (None, None) => None,
    };
    if let Some((key, value)) = text {
        let text = value
            .as_str()
            .ok_or_else(|| format!("field `{key}` must be a string"))?;
        if text.len() > MAX_DESCRIPTION_LEN {
            return Err(format!("field `{key}` is too long"));
        }
        fields.description = Some(text.to_string());
    }
    if let Some(value) = supplied("enable") {
        fields.enable = Some(
            value
                .as_bool()
                .ok_or_else(|| "field `enable` must be a boolean".to_string())?,
        );
    }
    if let Some(value) = supplied("expired_at") {
        fields.expired_at = Some(parse_expired_at(value)?);
    }
    if let Some(value) = supplied("role") {
        let text = value
            .as_str()
            .ok_or_else(|| "field `role` must be a string".to_string())?;
        if text.starts_with("ns:") {
            return Err(no_namespace());
        }
        fields.role = Some(
            KeyRole::parse(text)
                .filter(|_| !text.is_empty())
                .ok_or_else(|| {
                    "field `role` must be `administrator`, `viewer` or `publisher`".to_string()
                })?,
        );
    }
    if let Some(value) = supplied("scopes") {
        fields.scopes = Some(parse_scopes(value)?);
    }
    Ok(fields)
}

/// Validate a create body into a [`NewApiKey`].
///
/// The body must be a JSON object with a `name` that follows the name
/// rule. Optional: `desc` (or its `description` alias), `enable`,
/// `expired_at`, `role` and `scopes`. The defaults are the defaults of
/// the reference API: enabled, no expiry, role `administrator`, scopes
/// `unset`. Extra fields are ignored so future record fields degrade to
/// the same mint instead of a 400.
fn parse_create_request(value: &serde_json::Value) -> Result<NewApiKey, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "api-key body must be a JSON object".to_string())?;
    let name = obj
        .get("name")
        .and_then(|value| value.as_str())
        .ok_or_else(|| "field `name` is required".to_string())?;
    validate_name(name)?;
    let fields = parse_shared_fields(obj)?;
    let mut new = NewApiKey::new(name);
    if let Some(description) = fields.description {
        new.description = description;
    }
    if let Some(enable) = fields.enable {
        new.enable = enable;
    }
    if let Some(expired_at) = fields.expired_at {
        new.expired_at = expired_at;
    }
    if let Some(role) = fields.role {
        new.role = role;
    }
    if let Some(scopes) = fields.scopes {
        new.scopes = scopes;
    }
    Ok(new)
}

/// Validate an update body into an [`ApiKeyPatch`].
///
/// The body must be a JSON object. Each supplied field must be valid
/// (see [`parse_shared_fields`]); a missing or `null` field keeps the
/// stored value, except `expired_at`, which becomes `infinity`. A `name` field, when present, is ignored: the path is
/// the authority and a body name can never rename or fork the record.
/// Any other extra fields are ignored. Every supplied field is checked
/// before returning, so a caller that applies the result changes nothing
/// on error.
fn parse_update_request(value: &serde_json::Value) -> Result<ApiKeyPatch, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "api-key body must be a JSON object".to_string())?;
    let fields = parse_shared_fields(obj)?;
    Ok(ApiKeyPatch {
        description: fields.description,
        enable: fields.enable,
        // The body is the whole form: an omitted `expired_at` is
        // `infinity`, the default of the reference API. The console omits
        // the field to say "never expires".
        expired_at: Some(fields.expired_at.unwrap_or(None)),
        role: fields.role,
        scopes: fields.scopes,
    })
}

#[cfg(test)]
mod api_key_store_tests {
    use super::*;
    use serde_json::json;

    fn empty_store() -> ApiKeyStore {
        ApiKeyStore::new()
    }

    fn mint(store: &ApiKeyStore, name: &str) -> (ApiKeyRecord, String) {
        store.create(NewApiKey::new(name)).expect("mint succeeds")
    }

    fn mint_with(
        store: &ApiKeyStore,
        name: &str,
        role: KeyRole,
        scopes: Option<Vec<&str>>,
    ) -> (ApiKeyRecord, String) {
        let mut new = NewApiKey::new(name);
        new.role = role;
        new.scopes = scopes.map(|list| list.into_iter().map(str::to_string).collect());
        store.create(new).expect("mint succeeds")
    }

    fn grant(role: KeyRole, scopes: Option<Vec<&str>>) -> KeyGrant {
        KeyGrant {
            scopes: scopes.map(|list| list.into_iter().map(str::to_string).collect()),
            role,
        }
    }

    #[test]
    fn mint_renders_the_reference_fields_and_resolves_both_schemes() {
        let store = empty_store();
        assert!(store.list().is_empty());
        let (record, secret) = mint(&store, "ops-key");
        assert!(!secret.is_empty());
        let row = record.to_json();
        assert_eq!(row["name"], json!("ops-key"));
        let key_id = row["api_key"].as_str().expect("identifier").to_string();
        assert_eq!(key_id.len(), 16);
        assert_eq!(row["desc"], json!(""));
        assert_eq!(row["enable"], json!(true));
        assert_eq!(row["expired"], json!(false));
        assert_eq!(row["expired_at"], json!("infinity"));
        assert_eq!(row["role"], json!("administrator"));
        assert_eq!(row["scopes"], json!("unset"));
        assert!(row["created_at"].as_str().is_some());
        // The row never carries secret material.
        assert!(row.get("api_secret").is_none());
        assert!(row.get("secret").is_none());
        assert!(row.get("key_hash").is_none());
        assert!(!row.to_string().contains(&secret));

        // Basic needs the identifier and the secret.
        let found = store
            .resolve_basic(&key_id, &secret)
            .expect("the identifier and the secret resolve");
        assert_eq!(found.name, "ops-key");
        assert!(store.resolve_basic(&key_id, "wrong-secret").is_none());
        assert!(store.resolve_basic("0000000000000000", &secret).is_none());
        assert!(store.resolve_basic("", &secret).is_none());
        // The name is not the identifier of a new key.
        assert!(store.resolve_basic("ops-key", &secret).is_none());
        // The Bearer extension needs only the secret.
        assert!(store.resolve(&secret).is_some());
        assert!(store.resolve("wrong-secret").is_none());
        assert!(store.resolve("").is_none());
    }

    #[test]
    fn duplicate_names_are_rejected_and_miss_removes_404() {
        let store = empty_store();
        let (_, first_secret) = mint(&store, "dup");
        let duplicate = store.create(NewApiKey::new("dup"));
        assert_eq!(duplicate.map(|_| ()), Err(ApiKeyInsertError::Duplicate));
        // The original secret still resolves: no silent rotation.
        assert!(store.resolve(&first_secret).is_some());
        assert!(!store.remove("missing").expect("remove reads"));
        assert!(store.remove("dup").expect("remove revokes"));
        assert!(store.resolve(&first_secret).is_none());
    }

    #[test]
    fn names_follow_the_reference_rule() {
        let longest = "a".repeat(256);
        let too_long = "a".repeat(257);
        for good in ["a", "Ref-API-KEY-1", "k_1", "Z9", longest.as_str()] {
            assert!(validate_name(good).is_ok(), "{good} must be accepted");
        }
        for bad in [
            "",
            "1abc",
            "-abc",
            "_abc",
            "has space",
            "has/slash",
            "dot.name",
            "naïve",
            too_long.as_str(),
        ] {
            assert!(validate_name(bad).is_err(), "{bad} must be refused");
        }
    }

    #[test]
    fn malformed_bodies_are_rejected() {
        for body in [
            json!({}),
            json!([]),
            json!({"name": ""}),
            json!({"name": "has/slash"}),
            json!({"name": "1starts-with-digit"}),
            json!({"name": "ok", "scopes": "all"}),
            json!({"name": "ok", "scopes": [""]}),
            json!({"name": "ok", "scopes": ["no-such-scope"]}),
            json!({"name": "ok", "enable": "yes"}),
            json!({"name": "ok", "desc": 5}),
            json!({"name": "ok", "role": "root"}),
            json!({"name": "ok", "role": ""}),
            json!({"name": "ok", "expired_at": "tomorrow"}),
            json!({"name": "ok", "expired_at": -5}),
            json!({"name": "ok", "expired_at": 1.5}),
        ] {
            assert!(
                parse_create_request(&body).is_err(),
                "body {body} must be rejected"
            );
        }
        let new = parse_create_request(&json!({"name": "ok"})).expect("defaults");
        assert!(new.enable);
        assert_eq!(new.expired_at, None);
        assert_eq!(new.role, KeyRole::Administrator);
        assert_eq!(new.scopes, None);
        assert_eq!(new.description, "");
    }

    #[test]
    fn create_body_fields_are_read() {
        let new = parse_create_request(&json!({
            "name": "full",
            "desc": "from desc",
            "description": "ignored when desc is present",
            "enable": false,
            "expired": true,
            "expired_at": "2030-01-02T03:04:05Z",
            "role": "viewer",
            "scopes": ["audit", "monitoring"],
        }))
        .expect("valid body");
        assert_eq!(new.description, "from desc");
        assert!(!new.enable);
        assert_eq!(new.expired_at, Some(1_893_553_445));
        assert_eq!(new.role, KeyRole::Viewer);
        assert_eq!(
            new.scopes,
            Some(vec!["audit".to_string(), "monitoring".to_string()])
        );
        // `description` is accepted as an alias.
        let alias =
            parse_create_request(&json!({"name": "alias", "description": "note"})).expect("alias");
        assert_eq!(alias.description, "note");
        // `unset` and null mean no explicit list.
        let unset = parse_create_request(&json!({"name": "u", "scopes": "unset"})).expect("unset");
        assert_eq!(unset.scopes, None);
        let null = parse_create_request(&json!({"name": "n", "scopes": null})).expect("null");
        assert_eq!(null.scopes, None);
    }

    #[test]
    fn expired_at_accepts_infinity_epoch_and_iso_time() {
        assert_eq!(parse_expired_at(&json!("infinity")), Ok(None));
        assert_eq!(
            parse_expired_at(&json!(1_640_995_200)),
            Ok(Some(1_640_995_200))
        );
        assert_eq!(
            parse_expired_at(&json!("2022-01-01T00:00:00.000Z")),
            Ok(Some(1_640_995_200))
        );
        assert_eq!(
            parse_expired_at(&json!("2022-01-01T02:00:00+02:00")),
            Ok(Some(1_640_995_200))
        );
        assert!(parse_expired_at(&json!("")).is_err());
        assert!(parse_expired_at(&json!(true)).is_err());
        assert!(parse_expired_at(&json!("1969-12-31T23:59:59Z")).is_err());
    }

    #[test]
    fn namespaces_are_refused_with_a_clear_message() {
        for body in [
            json!({"name": "ok", "namespace": "ns1"}),
            json!({"name": "ok", "role": "ns:ns1::administrator"}),
            json!({"name": "ok", "namespace": 5}),
        ] {
            assert!(
                parse_create_request(&body).is_err(),
                "body {body} must be refused"
            );
        }
        assert!(parse_update_request(&json!({"namespace": "ns1"})).is_err());
        // An empty namespace means no namespace.
        assert!(parse_create_request(&json!({"name": "ok", "namespace": ""})).is_ok());
    }

    #[test]
    fn full_store_rejects_further_mints() {
        let store = empty_store();
        {
            let mut map = store.write_map();
            for index in 0..MAX_API_KEYS {
                let name = format!("fill-{index}");
                map.insert(
                    name.clone(),
                    ApiKeyRecord {
                        key_id: format!("id-{index}"),
                        name,
                        key_hash: [7u8; 32],
                        scopes: None,
                        description: String::new(),
                        created_at: String::new(),
                        enable: true,
                        expired_at: None,
                        role: KeyRole::Administrator,
                    },
                );
            }
        }
        assert_eq!(
            store.create(NewApiKey::new("one-more")).map(|_| ()),
            Err(ApiKeyInsertError::Full)
        );
    }

    #[test]
    fn snapshot_round_trip_persists_hashes_never_plaintext() {
        let dir = std::env::temp_dir().join(format!(
            "indramqtt-apikeys-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).expect("create test dir");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("registry loads"));
        let store = ApiKeyStore::from_registry(&registry);
        let mut new = NewApiKey::new("persist");
        new.role = KeyRole::Viewer;
        new.scopes = Some(Vec::new());
        new.enable = false;
        new.expired_at = Some(4_102_444_800);
        new.description = "kept".to_string();
        let (record, secret) = store.create(new).expect("mint succeeds");
        let key_id = record.key_id.clone();
        // The persisted snapshot holds the hash, never the secret.
        let snapshot = registry.snapshot();
        assert_eq!(snapshot.api_keys.keys.len(), 1);
        assert_eq!(snapshot.api_keys.keys[0].name, "persist");
        assert!(!snapshot.api_keys.keys[0].key_hash.contains(&secret));
        assert_eq!(snapshot.api_keys.keys[0].key_id, key_id);
        // A restart re-seed observes every field.
        let restarted = ApiKeyStore::from_registry(&Arc::new(
            ConfigRegistry::load(&dir).expect("registry reloads"),
        ));
        let again = restarted.get("persist").expect("row survives");
        assert_eq!(again, record);
        assert_eq!(again.scopes, Some(Vec::new()), "an empty list survives");
        // The key is disabled, thus it does not resolve; enable it.
        assert!(restarted.resolve_basic(&key_id, &secret).is_none());
        restarted
            .update(
                "persist",
                ApiKeyPatch {
                    enable: Some(true),
                    ..ApiKeyPatch::default()
                },
            )
            .expect("enable");
        assert!(restarted.resolve_basic(&key_id, &secret).is_some());
        assert!(restarted.remove("persist").expect("remove revokes"));
        assert!(restarted.resolve(&secret).is_none());
        let reloaded = ConfigRegistry::load(&dir).expect("registry reloads again");
        assert!(reloaded.snapshot().api_keys.keys.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_record_from_an_older_release_loads_as_before() {
        // An older file has neither identifier, flag, role nor expiry.
        let hash = encode_hex(&hash_secret("old-secret"));
        let plain: broker_config::ApiKeyEntry =
            serde_json::from_value(json!({"name": "old", "key_hash": hash, "scopes": []}))
                .expect("old entry loads");
        let scoped: broker_config::ApiKeyEntry = serde_json::from_value(
            json!({"name": "old-scoped", "key_hash": encode_hex(&hash_secret("scoped-secret")),
                   "scopes": ["audit"], "description": "d"}),
        )
        .expect("old scoped entry loads");
        assert!(plain.enable);
        assert_eq!(plain.role, "administrator");
        let conf = ApiKeysConf {
            keys: vec![plain, scoped],
        };
        conf.validate().expect("old file validates");
        let store = ApiKeyStore::from_snapshot(&conf);

        let row = store.get("old").expect("row").to_json();
        assert_eq!(row["scopes"], json!("unset"));
        assert_eq!(row["enable"], json!(true));
        assert_eq!(row["role"], json!("administrator"));
        assert_eq!(row["expired_at"], json!("infinity"));
        // The name is the identifier of an old record.
        assert_eq!(row["api_key"], json!("old"));
        assert!(store.resolve_basic("old", "old-secret").is_some());
        assert!(store.resolve("old-secret").is_some());

        let scoped_row = store.get("old-scoped").expect("row").to_json();
        assert_eq!(scoped_row["scopes"], json!(["audit"]));
        assert_eq!(scoped_row["desc"], json!("d"));
        let found = store.resolve("scoped-secret").expect("resolves");
        assert_eq!(found.grant.scopes, Some(vec!["audit".to_string()]));
    }

    #[test]
    fn a_disabled_or_expired_key_does_not_resolve() {
        let store = empty_store();
        let (record, secret) = mint(&store, "gate");
        let key_id = record.key_id.clone();
        assert!(store.resolve(&secret).is_some());

        store
            .update(
                "gate",
                ApiKeyPatch {
                    enable: Some(false),
                    ..ApiKeyPatch::default()
                },
            )
            .expect("disable");
        assert!(store.resolve(&secret).is_none());
        assert!(store.resolve_basic(&key_id, &secret).is_none());

        let updated = store
            .update(
                "gate",
                ApiKeyPatch {
                    enable: Some(true),
                    expired_at: Some(Some(1_000)),
                    ..ApiKeyPatch::default()
                },
            )
            .expect("expire in the past");
        assert!(store.resolve(&secret).is_none());
        assert!(store.resolve_basic(&key_id, &secret).is_none());
        let row = updated.to_json();
        assert_eq!(row["expired"], json!(true));
        assert_eq!(row["expired_at"], json!("1970-01-01T00:16:40+00:00"));

        store
            .update(
                "gate",
                ApiKeyPatch {
                    expired_at: Some(None),
                    ..ApiKeyPatch::default()
                },
            )
            .expect("back to infinity");
        assert!(store.resolve(&secret).is_some());
        let future = now_secs() + 3_600;
        let row = store
            .update(
                "gate",
                ApiKeyPatch {
                    expired_at: Some(Some(future)),
                    ..ApiKeyPatch::default()
                },
            )
            .expect("expire in the future")
            .to_json();
        assert_eq!(row["expired"], json!(false));
        assert!(store.resolve(&secret).is_some());
    }

    #[test]
    fn scopes_unset_default_list_and_empty_list_differ() {
        let store = empty_store();
        let all: Vec<&str> = API_KEY_SCOPES.iter().map(|entry| entry.name).collect();
        // Omitted, `unset` and the role default all read back as `unset`.
        let (omitted, _) = mint_with(&store, "omitted", KeyRole::Administrator, None);
        assert_eq!(omitted.to_json()["scopes"], json!("unset"));
        let (same, _) = mint_with(&store, "same", KeyRole::Viewer, Some(all.clone()));
        assert_eq!(same.to_json()["scopes"], json!("unset"));
        let (publisher, _) = mint_with(&store, "pub", KeyRole::Publisher, Some(vec!["publish"]));
        assert_eq!(publisher.to_json()["scopes"], json!("unset"));
        // A different list stays explicit and keeps its order.
        let (some, _) = mint_with(
            &store,
            "some",
            KeyRole::Administrator,
            Some(vec!["monitoring", "audit", "monitoring"]),
        );
        assert_eq!(some.to_json()["scopes"], json!(["monitoring", "audit"]));
        // An empty list denies each scope.
        let (none, secret) = mint_with(&store, "none", KeyRole::Administrator, Some(Vec::new()));
        assert_eq!(none.to_json()["scopes"], json!([]));
        let found = store.resolve(&secret).expect("the key resolves");
        assert_eq!(found.grant.scopes, Some(Vec::new()));

        // An update sets, clears and normalises the list.
        let row = store
            .update(
                "some",
                ApiKeyPatch {
                    scopes: Some(None),
                    ..ApiKeyPatch::default()
                },
            )
            .expect("clear")
            .to_json();
        assert_eq!(row["scopes"], json!("unset"));
        let row = store
            .update(
                "some",
                ApiKeyPatch {
                    scopes: Some(Some(all.iter().map(|s| s.to_string()).collect())),
                    ..ApiKeyPatch::default()
                },
            )
            .expect("default list")
            .to_json();
        assert_eq!(row["scopes"], json!("unset"));
    }

    #[test]
    fn detail_update_changes_the_fields_and_keeps_the_secret() {
        let store = empty_store();
        assert_eq!(
            store
                .update(
                    "missing",
                    ApiKeyPatch {
                        description: Some("x".to_string()),
                        ..ApiKeyPatch::default()
                    }
                )
                .map(|_| ()),
            Err(ApiKeyUpdateError::NotFound)
        );
        assert!(store.get("missing").is_none());
        let mut new = NewApiKey::new("edit");
        new.description = "before".to_string();
        new.scopes = Some(vec!["audit".to_string()]);
        let (before, secret) = store.create(new).expect("mint");
        let updated = store
            .update(
                "edit",
                ApiKeyPatch {
                    description: Some("after".to_string()),
                    role: Some(KeyRole::Viewer),
                    scopes: Some(Some(vec!["connections".to_string(), "publish".to_string()])),
                    ..ApiKeyPatch::default()
                },
            )
            .expect("update succeeds");
        assert_eq!(updated.description, "after");
        assert_eq!(updated.role, KeyRole::Viewer);
        assert_eq!(
            updated.scopes,
            Some(vec!["connections".to_string(), "publish".to_string()])
        );
        // Secret, identifier and mint time never change.
        assert!(store.resolve_basic(&before.key_id, &secret).is_some());
        assert_eq!(updated.key_id, before.key_id);
        assert_eq!(updated.created_at, before.created_at);
        // An empty patch keeps every field.
        let kept = store
            .update("edit", ApiKeyPatch::default())
            .expect("empty update keeps");
        assert_eq!(kept, updated);
    }

    #[test]
    fn update_bodies_are_fully_validated() {
        for body in [
            json!([]),
            json!({"scopes": "all"}),
            json!({"scopes": [""]}),
            json!({"scopes": [42]}),
            json!({"scopes": ["nope"]}),
            json!({"desc": 42}),
            json!({"description": 42}),
            json!({"enable": "no"}),
            json!({"role": "king"}),
            json!({"expired_at": "soon"}),
        ] {
            assert!(
                parse_update_request(&body).is_err(),
                "body {body} must be rejected"
            );
        }
        // Omitted and null fields keep; read only and unknown fields degrade.
        let empty = parse_update_request(&json!({})).expect("empty");
        assert!(empty.description.is_none() && empty.enable.is_none());
        assert!(empty.role.is_none() && empty.scopes.is_none());
        // An omitted `expired_at` is `infinity` (the console omits it).
        assert_eq!(empty.expired_at, Some(None));
        let null = parse_update_request(&json!({"scopes": null, "enable": null})).expect("null");
        assert!(null.scopes.is_none() && null.enable.is_none());
        let patch = parse_update_request(&json!({
            "name": "ignored", "expired": true, "extra": 1,
            "scopes": ["monitoring"], "expired_at": "infinity",
        }))
        .expect("tolerated fields");
        assert_eq!(patch.expired_at, Some(None));
        assert_eq!(patch.scopes, Some(Some(vec!["monitoring".to_string()])));
        let unset = parse_update_request(&json!({"scopes": "unset"})).expect("unset");
        assert_eq!(unset.scopes, Some(None));
    }

    #[test]
    fn a_bad_update_body_applies_nothing() {
        let store = empty_store();
        let mut new = NewApiKey::new("keep");
        new.description = "before".to_string();
        new.scopes = Some(vec!["audit".to_string()]);
        store.create(new).expect("mint");
        assert!(parse_update_request(&json!({"scopes": [""], "desc": "after"})).is_err());
        let record = store.get("keep").expect("row survives");
        assert_eq!(record.scopes, Some(vec!["audit".to_string()]));
        assert_eq!(record.description, "before");
    }

    #[test]
    fn scope_catalogue_lists_the_documented_names_with_fields() {
        // Every entry carries a non-empty name and summary, names are
        // unique, and the registry predicate agrees with the served list.
        assert_eq!(API_KEY_SCOPES.len(), 10);
        let mut names: Vec<&str> = API_KEY_SCOPES.iter().map(|entry| entry.name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), 10);
        for entry in &API_KEY_SCOPES {
            assert!(!entry.name.is_empty());
            assert!(!entry.desc.is_empty());
            assert!(is_known_api_key_scope(entry.name));
        }
        for documented in [
            "connections",
            "publish",
            "data_integration",
            "access_control",
            "gateways",
            "monitoring",
            "cluster_operations",
            "system",
            "audit",
            "license",
        ] {
            assert!(
                is_known_api_key_scope(documented),
                "catalogue must contain {documented}"
            );
        }
        assert!(!is_known_api_key_scope("no-such-scope"));
    }

    #[test]
    fn a_write_refuses_a_scope_that_is_not_in_the_list() {
        assert!(
            parse_create_request(&json!({"name": "k", "scopes": ["monitoring", "audit"]})).is_ok()
        );
        assert!(parse_create_request(&json!({"name": "k", "scopes": ["all"]})).is_err());
        assert!(parse_update_request(&json!({"scopes": ["Monitoring"]})).is_err());
    }

    #[test]
    fn resolve_finds_the_key_of_a_secret_and_only_that_key() {
        let store = ApiKeyStore::new();
        let (_, first) = mint_with(&store, "first", KeyRole::Administrator, Some(vec!["audit"]));
        let (_, second) = mint(&store, "second");
        let found = store.resolve(&first).expect("the first secret resolves");
        assert_eq!(found.name, "first");
        assert_eq!(found.grant.scopes, Some(vec!["audit".to_string()]));
        assert_eq!(found.grant.role, KeyRole::Administrator);
        let found = store.resolve(&second).expect("the second secret resolves");
        assert_eq!(found.name, "second");
        assert_eq!(found.grant.scopes, None);
        assert!(store.resolve("").is_none());
        assert!(store.resolve("not-a-secret").is_none());
        // The name of a key is not a credential.
        assert!(store.resolve("first").is_none());
        // A removed key does not resolve.
        assert_eq!(store.remove("first"), Ok(true));
        assert!(store.resolve(&first).is_none());
        assert!(store.resolve(&second).is_some());
    }

    #[test]
    fn byte_comparison_handles_every_length() {
        assert!(constant_time_eq_bytes(b"", b""));
        assert!(constant_time_eq_bytes(b"abc", b"abc"));
        assert!(!constant_time_eq_bytes(b"abc", b"abd"));
        assert!(!constant_time_eq_bytes(b"abc", b"abcd"));
        assert!(!constant_time_eq_bytes(b"abc", b"ab"));
        assert!(!constant_time_eq_bytes(b"ab", b"ab\0"));
    }

    #[test]
    fn a_scope_opens_only_its_paths() {
        let audit = grant(KeyRole::Administrator, Some(vec!["audit"]));
        assert!(key_may_call(&audit, "/api/v5/audit"));
        assert!(!key_may_call(&audit, "/api/v5/clients"));
        assert!(!key_may_call(&audit, "/api/v5/rules/r1"));
        assert!(!key_may_call(&audit, "/api/v1/config/reload"));
        // Paths that each key can call.
        assert!(key_may_call(&audit, "/api/v5/current_user"));
        assert!(key_may_call(&audit, "/api/v5/status"));

        let unset = grant(KeyRole::Administrator, None);
        assert!(key_may_call(&unset, "/api/v5/clients/c1"));
        assert!(key_may_call(&unset, "/api/v5/data/export"));
        assert!(key_may_call(&unset, "/api/v1/config/reload"));
        assert!(key_may_call(&unset, "/schemas/s1"));

        assert_eq!(
            access_for_path("/api/v5/publish/bulk"),
            PathAccess::Scope("publish")
        );
        assert_eq!(
            access_for_path("/api/v1/auth/users"),
            PathAccess::Scope("access_control")
        );
    }

    #[test]
    fn an_empty_scope_list_denies_everything_except_anonymous_paths() {
        let deny = grant(KeyRole::Administrator, Some(Vec::new()));
        assert!(key_may_call(&deny, "/api/v5/status"));
        for path in [
            "/api/v5/stats",
            "/api/v5/clients",
            "/api/v5/current_user",
            "/api/v5/users/admin",
            "/api/v5/api_key/other",
        ] {
            assert!(!key_may_call(&deny, path), "{path} must be closed");
        }
    }

    #[test]
    fn the_operations_that_the_reference_lists_for_a_bearer_token_are_login_only() {
        let admin = grant(KeyRole::Administrator, None);
        for path in [
            "/api/v5/api_key",
            "/api/v5/api_key/",
            "/api/v5/api_key_scopes",
            "/api/v5/users",
            "/api/v5/user_scopes",
            "/api/v5/logout",
        ] {
            assert_eq!(
                key_decision(&admin, path),
                KeyDecision::LoginOnly,
                "{path} accepts a bearer token only"
            );
        }
    }

    #[test]
    fn the_operations_that_the_reference_lists_for_basic_accept_a_plain_admin_key() {
        let admin = grant(KeyRole::Administrator, None);
        for path in [
            "/api/v5/api_key/other",
            "/api/v5/users/admin",
            "/api/v5/users/admin/change_pwd",
            "/api/v5/users/admin/mfa",
            "/api/v5/sso",
            "/api/v5/sso/ldap",
        ] {
            assert_eq!(
                key_decision(&admin, path),
                KeyDecision::Allowed,
                "{path} accepts a key"
            );
        }
        // The catalogue has no scope for these paths: a key with a scope
        // list, a viewer key and a publisher key cannot call them.
        let listed = grant(
            KeyRole::Administrator,
            Some(vec!["access_control", "system"]),
        );
        let viewer = grant(KeyRole::Viewer, None);
        let publisher = grant(KeyRole::Publisher, None);
        for key in [&listed, &viewer, &publisher] {
            for path in [
                "/api/v5/users/admin",
                "/api/v5/api_key/other",
                "/api/v5/sso",
            ] {
                assert_eq!(key_decision(key, path), KeyDecision::Forbidden, "{path}");
            }
        }
    }

    #[test]
    fn paths_that_the_table_does_not_know_stay_closed_for_every_key() {
        let every: Vec<&str> = API_KEY_SCOPES.iter().map(|entry| entry.name).collect();
        for key in [
            grant(KeyRole::Administrator, None),
            grant(KeyRole::Administrator, Some(every)),
            grant(KeyRole::Viewer, None),
        ] {
            for path in [
                "/api/v1/licence/install",
                "/api/v5/no_such_group",
                "/api/v5/%75sers",
                "/api/v5/users/admin/unknown/deep",
                "/api/v5/api_key/a/b",
                "/api/v5/sso/mfa/setup",
                "/api/v5/sso/login/ldap",
                "/api/v5/sso/saml/acs",
                "/other",
            ] {
                assert_eq!(
                    key_decision(&key, path),
                    KeyDecision::Forbidden,
                    "{path} must be closed"
                );
            }
        }
    }

    #[test]
    fn a_publisher_key_can_only_publish() {
        let publisher = grant(KeyRole::Publisher, None);
        assert!(key_may_call(&publisher, "/api/v5/publish"));
        assert!(key_may_call(&publisher, "/api/v5/publish/bulk"));
        assert!(!key_may_call(&publisher, "/api/v5/clients"));
        assert!(!key_may_call(&publisher, "/api/v5/stats"));
        // An explicit list cannot widen the role.
        let widened = grant(KeyRole::Publisher, Some(vec!["publish", "monitoring"]));
        assert!(key_may_call(&widened, "/api/v5/publish"));
        assert!(!key_may_call(&widened, "/api/v5/stats"));
    }
}
