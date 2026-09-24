//! Management API key records for the v5 management API.
//!
//! Covers `GET /api_key` (paged list, C05-01), `POST /api_key`
//! (mint one key, C05-02), `DELETE /api_key/{name}` (revoke one key,
//! C05-03), `GET /api_key/{name}` (one key's metadata, C05-04),
//! `PUT /api_key/{name}` (replace one key's scopes/description, C05-05)
//! and `GET /api_key_scopes` (fixed scope catalogue, C05-06)
//! on real state. Single-node, management-plane only: handlers
//! clone at most the capped list once per request under a short lock;
//! publish and deliver never touch this store, and key checks on the
//! management plane use constant-time comparison off the hot path (see
//! [`ApiKeyStore::verify`]).
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
//! create response, exactly once. Listing never renders secrets or
//! hashes, only metadata.
// TODO(parity): which expiry/enabled fields do the single-key read
// render and the update accept? The rulebook does not decide the exact
// record shape and the broker tracks neither field, so the current
// choice omits fields the broker cannot supply (never a constant
// standing in for a measurement): the read renders
// name/scopes/description/created_at only, and the update replaces
// scopes/description only, ignoring any other supplied fields
// consistently with the create path's extra-fields-ignored rule.
// TODO(parity): which per-key fields plus list envelope does the spec
// require (bare list versus data/meta, expiry/enabled/status fields)?
// The rulebook does not decide the exact shape; the current choice
// reuses the shared pagination extractor (data plus meta) and reports
// name/scopes/description/created_at only, omitting fields the broker
// cannot supply.
// TODO(parity): what are the exact create request/response field names
// (`api_key` versus `key` versus `secret`), the create status (201
// versus 200) and the delete success shape (204 versus 200 envelope)?
// The rulebook does not decide the exact shapes; the current choice
// answers create with 201 carrying `api_key` once and delete with 204
// until the checker pins them.
// TODO(parity): which scope defaults and vocabulary enforcement does
// the spec require (default scope set for an omitted `scopes` field,
// rejection of unknown scope names on create/update)? The rulebook
// does not decide the exact set; the assignable vocabulary is pinned
// by [`API_KEY_SCOPES`] below (served verbatim by the catalogue
// route), while create/update still store the supplied scopes verbatim
// (empty when omitted) and enforcement stays with the middleware lane
// until the checker pins it.
// TODO(parity): which name charset and duplicate status (400 versus
// 409) does the documented `ALREADY_EXISTS` code carry? The rulebook
// does not decide the exact duplicate semantics; the current choice
// rejects non-empty slash-free names up to the length cap with
// `ALREADY_EXISTS` (400 in this tree's shared error mapping) until the
// checker pins it.

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
use std::sync::{Arc, Mutex, RwLock};

use crate::errors::ApiError;
use crate::pagination::{meta_page, paginate, PageParams};
use crate::ApiState;

/// Upper bound for stored API keys. Creates past this size are rejected
/// with `BAD_REQUEST` instead of growing the map without limit. Reason:
/// the management plane snapshots the whole list per list request and
/// each entry holds a hash plus metadata, so an unbounded map lets one
/// management batch exhaust the kernel; 1024 covers operator and
/// automation provisioning with headroom while keeping list reads paged
/// and per-request clones cheap.
pub const MAX_API_KEYS: usize = broker_config::MAX_API_KEYS;
/// Longest accepted key `description` text (1024 bytes). Reason: the
/// description renders in every list row and is cloned per list snapshot
/// plus persisted per entry, so an unbounded text lets one record bloat
/// each list read; 1024 covers free-form operator notes with headroom
/// while keeping rows and per-request clones small.
const MAX_DESCRIPTION_LEN: usize = 1024;

/// One stored API key: the name plus the secret hash and metadata.
///
/// The hash is the raw 32-byte SHA-256 of the minted secret; only its
/// hex form ever leaves this struct (registry persistence). Plaintext
/// secrets are never stored here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyRecord {
    name: String,
    key_hash: [u8; 32],
    scopes: Vec<String>,
    description: String,
    created_at: String,
}

impl ApiKeyRecord {
    /// Metadata row: everything the list renders. The secret and its
    /// hash never render.
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "scopes": self.scopes,
            "description": self.description,
            "created_at": self.created_at,
        })
    }
}

/// In-memory API key directory keyed by name.
///
/// Single-node map behind one lock; every method finishes quickly.
/// Management writes take the write lock; the future management-plane
/// key check ([`ApiKeyStore::verify`]) takes a short read lock and never
/// mutates, so delivery never blocks on a management write. Publish and
/// deliver never touch this store.
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
        *store.registry.write().expect("api-key store lock") = Some(Arc::clone(registry));
        store
    }

    /// Attach the registry and replace the current contents with its
    /// snapshot, in place on the same instance.
    pub fn seed_from_registry(&self, registry: &Arc<ConfigRegistry>) {
        self.seed_from_snapshot(&registry.snapshot().api_keys);
        *self.registry.write().expect("api-key store lock") = Some(Arc::clone(registry));
    }

    fn seed_from_snapshot(&self, conf: &ApiKeysConf) {
        let mut map = self.inner.write().expect("api-key store lock");
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
            map.insert(
                entry.name.clone(),
                ApiKeyRecord {
                    name: entry.name.clone(),
                    key_hash: hash,
                    scopes: entry.scopes.clone(),
                    description: entry.description.clone(),
                    created_at: entry.created_at.clone(),
                },
            );
        }
    }

    /// Snapshot of live records sorted by name for stable pages.
    pub fn list(&self) -> Vec<ApiKeyRecord> {
        let map = self.inner.read().expect("api-key store lock");
        let mut out: Vec<ApiKeyRecord> = map.values().cloned().collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// Whether `secret` is the minted secret for `name`.
    ///
    /// Hashes the candidate and compares in constant time so key checks
    /// do not leak prefix information through timing. Unknown names fail
    /// closed (a dummy constant-time comparison still runs so misses do
    /// not return faster than hits). Management-plane only: one short
    /// read lock per check; the publish and deliver paths never call
    /// this. The future Basic-enforcement middleware (owned elsewhere)
    /// consults this method off the hot path.
    pub fn verify(&self, name: &str, secret: &str) -> bool {
        let found: Option<[u8; 32]> = {
            let map = self.inner.read().expect("api-key store lock");
            map.get(name).map(|record| record.key_hash)
        };
        let candidate = hash_secret(secret);
        match found {
            Some(stored) => constant_time_eq(&candidate, &stored),
            None => {
                let dummy = [0u8; 32];
                let _ = constant_time_eq(&candidate, &dummy);
                false
            }
        }
    }

    /// Mint one key. Fails cleanly when `name` is already stored (caller
    /// maps to `ALREADY_EXISTS`) or the store is at [`MAX_API_KEYS`]
    /// (caller maps to `BAD_REQUEST`). Returns the metadata row plus the
    /// plaintext secret, which the caller renders exactly once and never
    /// stores. Persists through the registry when one is attached; a
    /// commit or save failure rolls back the in-memory insert and
    /// surfaces as [`ApiKeyInsertError::Persist`] (commit precedes the
    /// atomic save, mirroring the credential stores).
    pub fn create(
        &self,
        name: String,
        scopes: Vec<String>,
        description: String,
    ) -> Result<(ApiKeyRecord, String), ApiKeyInsertError> {
        let mut secret_bytes = [0u8; 32];
        rand::rng().fill_bytes(&mut secret_bytes);
        let secret = URL_SAFE_NO_PAD.encode(secret_bytes);
        let record = ApiKeyRecord {
            name: name.clone(),
            key_hash: hash_secret(&secret),
            scopes,
            description,
            created_at: utc_rfc3339_now(),
        };
        {
            let mut map = self.inner.write().expect("api-key store lock");
            if map.contains_key(&name) {
                return Err(ApiKeyInsertError::Duplicate);
            }
            if map.len() >= MAX_API_KEYS {
                return Err(ApiKeyInsertError::Full);
            }
            map.insert(name, record.clone());
        }
        if let Err(reason) = self.persist() {
            // The in-memory insert is rolled back (removed) so memory
            // stays consistent with what persisted; the failure is
            // never silent.
            let mut map = self.inner.write().expect("api-key store lock");
            map.remove(&record.name);
            return Err(ApiKeyInsertError::Persist(reason));
        }
        Ok((record, secret))
    }

    /// Revoke the key stored under `name`. Returns true when a record
    /// was removed, false when nothing was stored under the name.
    /// Persists through the registry when one is attached; a commit or
    /// save failure restores the removed record and surfaces as
    /// [`ApiKeyInsertError::Persist`] so the loss is never silent.
    pub fn remove(&self, name: &str) -> Result<bool, String> {
        let removed = self.inner.write().expect("api-key store lock").remove(name);
        let Some(record) = removed else {
            return Ok(false);
        };
        if let Err(reason) = self.persist() {
            self.inner
                .write()
                .expect("api-key store lock")
                .insert(name.to_string(), record);
            return Err(reason);
        }
        Ok(true)
    }

    /// Read one key's metadata row. Returns a clone under a short read
    /// lock, or `None` when nothing is stored under `name`.
    pub fn get(&self, name: &str) -> Option<ApiKeyRecord> {
        self.inner
            .read()
            .expect("api-key store lock")
            .get(name)
            .cloned()
    }

    /// Replace one key's scopes and description in place. `None` fields
    /// keep the stored value; `Some` fields must already be validated
    /// (same caps as minting) and replace verbatim. Unknown names fail
    /// with [`ApiKeyUpdateError::NotFound`] and create nothing; the
    /// secret hash and mint time are never rotated here. Persists
    /// through the registry when one is attached; a commit or save
    /// failure restores the previous record and surfaces as
    /// [`ApiKeyUpdateError::Persist`] so the loss is never silent.
    pub fn update(
        &self,
        name: &str,
        scopes: Option<Vec<String>>,
        description: Option<String>,
    ) -> Result<ApiKeyRecord, ApiKeyUpdateError> {
        let previous = {
            let mut map = self.inner.write().expect("api-key store lock");
            let Some(record) = map.get_mut(name) else {
                return Err(ApiKeyUpdateError::NotFound);
            };
            let previous = record.clone();
            if let Some(scopes) = scopes {
                record.scopes = scopes;
            }
            if let Some(description) = description {
                record.description = description;
            }
            previous
        };
        if let Err(reason) = self.persist() {
            // Restore the previous row so memory stays consistent with
            // what persisted; the failure is never silent.
            self.inner
                .write()
                .expect("api-key store lock")
                .insert(name.to_string(), previous);
            return Err(ApiKeyUpdateError::Persist(reason));
        }
        self.get(name).ok_or(ApiKeyUpdateError::NotFound)
    }

    /// Export the current contents as a validated config root, sorted by
    /// name so the persisted file is deterministic.
    fn export_conf(&self) -> ApiKeysConf {
        let map = self.inner.read().expect("api-key store lock");
        let mut entries: Vec<broker_config::ApiKeyEntry> = map
            .values()
            .map(|record| broker_config::ApiKeyEntry {
                name: record.name.clone(),
                key_hash: encode_hex_hash(&record.key_hash),
                scopes: record.scopes.clone(),
                description: record.description.clone(),
                created_at: record.created_at.clone(),
            })
            .collect();
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        ApiKeysConf { keys: entries }
    }

    /// Commit the exported root and atomically save it. A no-op without
    /// a registry; any commit or save failure surfaces as a message so
    /// handlers answer 500 and the loss is never silent.
    fn persist(&self) -> Result<(), String> {
        let registry = self.registry.read().expect("api-key store lock").clone();
        let Some(registry) = registry else {
            return Ok(());
        };
        let _guard = self.save_lock.lock().expect("api-key save lock");
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

fn encode_hex_hash(hash: &[u8; 32]) -> String {
    let mut out = String::with_capacity(64);
    for byte in hash {
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

/// Current UTC time as `YYYY-MM-DDTHH:MM:SS+00:00` (always UTC).
fn utc_rfc3339_now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    epoch_to_rfc3339(secs)
}

fn epoch_to_rfc3339(secs: u64) -> String {
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
fn civil_from_days(z: i64) -> (i64, u32, u32) {
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

/// `GET /api_key`: paged metadata page (`data` plus `meta`). An empty
/// store reads as an empty page, never an error. Secrets and hashes are
/// never rendered. Malformed `page`/`limit` fall back to defaults via
/// [`PageParams`].
pub async fn list_api_keys(State(state): State<ApiState>, params: PageParams) -> Response {
    let all = state.api_keys.list();
    let total = all.len();
    let page_items = paginate(&all, params.page, params.limit);
    let data: Vec<serde_json::Value> = page_items.iter().map(ApiKeyRecord::to_json).collect();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "data": data,
            "meta": meta_page(params.page, params.limit, total),
        })),
    )
        .into_response()
}

/// `POST /api_key`: mint one key. Returns the metadata plus the secret
/// under `api_key` with 201; the secret is rendered exactly once and
/// never stored in plaintext. Malformed bodies fail with `BAD_REQUEST`;
/// an existing name fails with `ALREADY_EXISTS` instead of rotating the
/// secret in place; a full store fails with `BAD_REQUEST`.
pub async fn create_api_key(State(state): State<ApiState>, body: Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid api-key body: {error}")).into_response();
        }
    };
    let (name, scopes, description) = match parse_create_request(&value) {
        Ok(parsed) => parsed,
        Err(message) => return ApiError::BadRequest(message).into_response(),
    };
    match state.api_keys.create(name, scopes, description) {
        Ok((record, secret)) => {
            let mut body = record.to_json();
            body["api_key"] = serde_json::Value::String(secret);
            (StatusCode::CREATED, Json(body)).into_response()
        }
        Err(ApiKeyInsertError::Duplicate) => {
            ApiError::AlreadyExists("api key already exists".to_string()).into_response()
        }
        Err(ApiKeyInsertError::Full) => {
            ApiError::BadRequest("api-key store is full".to_string()).into_response()
        }
        Err(ApiKeyInsertError::Persist(message)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": message,
            })),
        )
            .into_response(),
    }
}

/// `DELETE /api_key/{name}`: revoke one key. A stored name reports
/// success with 204; an unknown name reports `NOT_FOUND` in the
/// documented `{code, message}` shape instead of success.
pub async fn delete_api_key(State(state): State<ApiState>, Path(name): Path<String>) -> Response {
    match state.api_keys.remove(&name) {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => ApiError::NotFound(format!("api key {name:?} not found")).into_response(),
        Err(message) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": message,
            })),
        )
            .into_response(),
    }
}

/// `GET /api_key/{name}`: read one key's metadata (C05-04).
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

/// `PUT /api_key/{name}`: replace one key's scopes/description
/// (C05-05).
///
/// The body must be a JSON object; `scopes` (when present and non-null)
/// replaces the scope set verbatim under the same caps as minting, and
/// `description` (or its `desc` alias; `description` wins when both are
/// present, when present as a string) replaces the description verbatim.
/// Omitted fields keep the stored value; a `null` `scopes` keeps the
/// stored set. Every supplied field is validated before anything is
/// applied: bad values fail with `BAD_REQUEST` and apply nothing. An
/// unknown name fails with `NOT_FOUND` and creates nothing. On success
/// returns 200 with the stored metadata row (never the secret). The
/// secret hash and mint time are never rotated here. The write persists
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
    let (scopes, description) = match parse_update_request(&value) {
        Ok(parsed) => parsed,
        Err(message) => return ApiError::BadRequest(message).into_response(),
    };
    match state.api_keys.update(&name, scopes, description) {
        Ok(record) => (StatusCode::OK, Json(record.to_json())).into_response(),
        Err(ApiKeyUpdateError::NotFound) => {
            ApiError::NotFound(format!("api key {name:?} not found")).into_response()
        }
        Err(ApiKeyUpdateError::Persist(message)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": message,
            })),
        )
            .into_response(),
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
/// same list. Vocabulary enforcement itself (rejecting unknown names
/// on write) is owned elsewhere; the catalogue route serves the list
/// verbatim regardless of which scopes any stored key holds.
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
        desc: "Manage licence installation and status",
    },
];

/// Whether `name` is one of the assignable catalogue scopes.
///
/// Shared by the key create/update validation path and the catalogue
/// route so both read the same registry. Advisory for now: writers
/// still store scopes verbatim and enforcement stays with the
/// middleware lane (see the module TODO); the catalogue route serves
/// this list regardless.
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

/// Validate a create body into `(name, scopes, description)`.
///
/// The body must be a JSON object with a non-empty slash-free `name`
/// (at most [`broker_config::MAX_API_KEY_NAME_LEN`] bytes). Optional:
/// `scopes` (an array of non-empty strings within their caps) and
/// `description` (or its `desc` alias; `description` wins when both are
/// present). Extra fields are ignored so future record fields degrade
/// to the same mint instead of a 400.
fn parse_create_request(
    value: &serde_json::Value,
) -> Result<(String, Vec<String>, String), String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "api-key body must be a JSON object".to_string())?;
    let name = obj
        .get("name")
        .and_then(|value| value.as_str())
        .ok_or_else(|| "field `name` is required".to_string())?;
    if name.trim().is_empty() {
        return Err("field `name` must not be empty".to_string());
    }
    if name.len() > broker_config::MAX_API_KEY_NAME_LEN {
        return Err(format!(
            "field `name` exceeds {} bytes",
            broker_config::MAX_API_KEY_NAME_LEN
        ));
    }
    if name.contains('/') {
        return Err("field `name` must not contain `/`".to_string());
    }
    let scopes = match obj.get("scopes") {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(serde_json::Value::Array(items)) => {
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
                out.push(scope.to_string());
            }
            out
        }
        Some(_) => return Err("field `scopes` must be an array of strings".to_string()),
    };
    let description = match obj.get("description") {
        Some(serde_json::Value::String(text)) => text.clone(),
        Some(_) => return Err("field `description` must be a string".to_string()),
        None => match obj.get("desc") {
            Some(serde_json::Value::String(text)) => text.clone(),
            Some(_) => return Err("field `desc` must be a string".to_string()),
            None => String::new(),
        },
    };
    if description.len() > MAX_DESCRIPTION_LEN {
        return Err("field `description` is too long".to_string());
    }
    Ok((name.to_string(), scopes, description))
}

/// Validate an update body into `(scopes, description)` replaces.
///
/// The body must be a JSON object. `scopes` (when present and non-null)
/// must be an array of non-empty strings within the same caps as
/// minting; a `null` or absent `scopes` keeps the stored set.
/// `description` (or its `desc` alias; `description` wins when both are
/// present, when present as a string) must be a string within
/// [`MAX_DESCRIPTION_LEN`] bytes; an absent pair keeps the stored text.
/// A `name` field, when present, is ignored: the path is the authority
/// and a body name can never rename or fork the record. Any other extra
/// fields are ignored so future record fields degrade to the same
/// replace instead of a 400 (see the expiry TODO at the top of this
/// file: fields the broker does not track are omitted, never invented).
/// Every supplied field is checked before returning, so a caller that
/// applies the result changes nothing on error.
fn parse_update_request(
    value: &serde_json::Value,
) -> Result<(Option<Vec<String>>, Option<String>), String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "api-key body must be a JSON object".to_string())?;
    let scopes = match obj.get("scopes") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::Array(items)) => {
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
                out.push(scope.to_string());
            }
            Some(out)
        }
        Some(_) => return Err("field `scopes` must be an array of strings".to_string()),
    };
    let description = match obj.get("description") {
        Some(serde_json::Value::String(text)) => {
            if text.len() > MAX_DESCRIPTION_LEN {
                return Err("field `description` is too long".to_string());
            }
            Some(text.clone())
        }
        Some(_) => return Err("field `description` must be a string".to_string()),
        None => match obj.get("desc") {
            Some(serde_json::Value::String(text)) => {
                if text.len() > MAX_DESCRIPTION_LEN {
                    return Err("field `desc` is too long".to_string());
                }
                Some(text.clone())
            }
            Some(_) => return Err("field `desc` must be a string".to_string()),
            None => None,
        },
    };
    Ok((scopes, description))
}

#[cfg(test)]
mod api_key_store_tests {
    use super::*;

    fn empty_store() -> ApiKeyStore {
        ApiKeyStore::new()
    }

    #[test]
    fn mint_lists_without_secrets_and_verifies() {
        let store = empty_store();
        assert!(store.list().is_empty());
        let (record, secret) = store
            .create(
                "w225-key".to_string(),
                vec!["all".to_string()],
                "ops".to_string(),
            )
            .expect("mint succeeds");
        assert!(!secret.is_empty());
        assert!(store.verify("w225-key", &secret));
        assert!(!store.verify("w225-key", "wrong-secret"));
        assert!(!store.verify("unknown-key", &secret));
        // The metadata row never carries secret material.
        let row = record.to_json();
        assert_eq!(row["name"], serde_json::json!("w225-key"));
        assert!(row.get("api_key").is_none());
        assert!(row.get("secret").is_none());
        assert!(row.get("key_hash").is_none());
        // Listing holds the row without secrets.
        let listed = store.list();
        assert_eq!(listed.len(), 1);
        let listed_row = listed[0].to_json();
        assert!(listed_row.get("api_key").is_none());
        assert!(listed_row.get("key_hash").is_none());
    }

    #[test]
    fn duplicate_names_are_rejected_and_miss_removes_404() {
        let store = empty_store();
        let (_, first_secret) = store
            .create("w225-dup".to_string(), Vec::new(), String::new())
            .expect("first mint succeeds");
        let duplicate = store.create("w225-dup".to_string(), Vec::new(), String::new());
        assert_eq!(duplicate, Err(ApiKeyInsertError::Duplicate));
        // The original secret still verifies: no silent rotation.
        assert!(store.verify("w225-dup", &first_secret));
        assert!(!store.remove("w225-missing").expect("remove reads"));
        assert!(store.remove("w225-dup").expect("remove revokes"));
        assert!(!store.verify("w225-dup", &first_secret));
    }

    #[test]
    fn malformed_bodies_are_rejected() {
        for body in [
            serde_json::json!({}),
            serde_json::json!({"name": ""}),
            serde_json::json!({"name": "has/slash"}),
            serde_json::json!({"name": "ok", "scopes": "all"}),
            serde_json::json!({"name": "ok", "scopes": [""]}),
        ] {
            assert!(
                parse_create_request(&body).is_err(),
                "body {body} must be rejected"
            );
        }
        assert!(parse_create_request(&serde_json::json!({"name": "ok"})).is_ok());
    }

    #[test]
    fn full_store_rejects_further_mints() {
        let store = empty_store();
        {
            let mut map = store.inner.write().expect("test lock");
            for index in 0..MAX_API_KEYS {
                let name = format!("w225-fill-{index}");
                map.insert(
                    name.clone(),
                    ApiKeyRecord {
                        name,
                        key_hash: [7u8; 32],
                        scopes: Vec::new(),
                        description: String::new(),
                        created_at: String::new(),
                    },
                );
            }
        }
        assert_eq!(
            store.create("w225-one-more".to_string(), Vec::new(), String::new()),
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
        let (_, secret) = store
            .create(
                "w225-persist".to_string(),
                vec!["all".to_string()],
                String::new(),
            )
            .expect("mint succeeds");
        // The persisted snapshot holds the hash, never the secret.
        let snapshot = registry.snapshot();
        assert_eq!(snapshot.api_keys.keys.len(), 1);
        assert_eq!(snapshot.api_keys.keys[0].name, "w225-persist");
        assert!(!snapshot.api_keys.keys[0].key_hash.contains(&secret));
        // A restart re-seed verifies the same secret and revokes on delete.
        let restarted = ApiKeyStore::from_registry(&Arc::new(
            ConfigRegistry::load(&dir).expect("registry reloads"),
        ));
        assert!(restarted.verify("w225-persist", &secret));
        assert!(restarted.remove("w225-persist").expect("remove revokes"));
        assert!(!restarted.verify("w225-persist", &secret));
        let reloaded = ConfigRegistry::load(&dir).expect("registry reloads again");
        assert!(reloaded.snapshot().api_keys.keys.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn detail_get_returns_metadata_without_secrets_and_misses() {
        // W2-26 store read: hit renders metadata without secret
        // material, miss reads as absent.
        let store = empty_store();
        assert!(store.get("w226-missing").is_none());
        let (_, secret) = store
            .create(
                "w226-key".to_string(),
                vec!["all".to_string()],
                "ops".to_string(),
            )
            .expect("mint succeeds");
        let record = store.get("w226-key").expect("hit reads");
        assert_eq!(record.name, "w226-key");
        assert_eq!(record.scopes, vec!["all".to_string()]);
        let row = record.to_json();
        assert_eq!(row["name"], serde_json::json!("w226-key"));
        assert!(row.get("api_key").is_none());
        assert!(row.get("secret").is_none());
        assert!(row.get("key_hash").is_none());
        assert!(store.verify("w226-key", &secret));
    }

    #[test]
    fn detail_update_replaces_scopes_and_description() {
        // W2-26 store replace: scopes/description swap, the secret hash
        // and mint time never rotate, unknown names create nothing.
        let store = empty_store();
        assert_eq!(
            store.update("w226-missing", Some(vec!["all".to_string()]), None,),
            Err(ApiKeyUpdateError::NotFound)
        );
        assert!(store.get("w226-missing").is_none());
        let (before, secret) = store
            .create(
                "w226-key".to_string(),
                vec!["all".to_string()],
                "before".to_string(),
            )
            .expect("mint succeeds");
        let updated = store
            .update(
                "w226-key",
                Some(vec!["read".to_string(), "write".to_string()]),
                Some("after".to_string()),
            )
            .expect("update succeeds");
        assert_eq!(
            updated.scopes,
            vec!["read".to_string(), "write".to_string()]
        );
        assert_eq!(updated.description, "after");
        // Secret and mint time are untouched by the replace.
        assert!(store.verify("w226-key", &secret));
        assert_eq!(updated.created_at, before.created_at);
        let row = updated.to_json();
        assert!(row.get("api_key").is_none());
        assert!(row.get("key_hash").is_none());
        // `None` fields keep the stored value.
        let kept = store
            .update("w226-key", None, None)
            .expect("empty update keeps");
        assert_eq!(kept.scopes, updated.scopes);
        assert_eq!(kept.description, updated.description);
    }

    #[test]
    fn detail_update_bodies_are_fully_validated() {
        for body in [
            serde_json::json!([]),
            serde_json::json!({"scopes": "all"}),
            serde_json::json!({"scopes": [""]}),
            serde_json::json!({"scopes": [42]}),
            serde_json::json!({"description": 42}),
            serde_json::json!({"desc": 42}),
        ] {
            assert!(
                parse_update_request(&body).is_err(),
                "body {body} must be rejected"
            );
        }
        // Omitted fields keep; null scopes keeps; extra fields degrade.
        assert_eq!(
            parse_update_request(&serde_json::json!({})),
            Ok((None, None))
        );
        assert_eq!(
            parse_update_request(&serde_json::json!({"scopes": serde_json::Value::Null})),
            Ok((None, None))
        );
        assert!(parse_update_request(
            &serde_json::json!({"scopes": ["all"], "expiry": "tomorrow"})
        )
        .is_ok());
    }

    #[test]
    fn detail_update_rejects_bad_values_without_applying() {
        // Validate-all-before-apply at the store edge: a bad body never
        // reaches the map, so the stored row is untouched.
        let store = empty_store();
        store
            .create(
                "w226-key".to_string(),
                vec!["all".to_string()],
                "before".to_string(),
            )
            .expect("mint succeeds");
        assert!(parse_update_request(&serde_json::json!({"scopes": [""]})).is_err());
        let record = store.get("w226-key").expect("row survives");
        assert_eq!(record.scopes, vec!["all".to_string()]);
        assert_eq!(record.description, "before");
    }

    #[test]
    fn detail_update_persists_across_restart() {
        let dir = std::env::temp_dir().join(format!(
            "indramqtt-apikeys-w226-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).expect("create test dir");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("registry loads"));
        let store = ApiKeyStore::from_registry(&registry);
        let (_, secret) = store
            .create(
                "w226-persist".to_string(),
                vec!["all".to_string()],
                "before".to_string(),
            )
            .expect("mint succeeds");
        store
            .update(
                "w226-persist",
                Some(vec!["read".to_string()]),
                Some("after".to_string()),
            )
            .expect("update succeeds");
        // A restart re-seed observes the replaced row and the same
        // secret still verifies (updates never rotate secrets).
        let restarted = ApiKeyStore::from_registry(&Arc::new(
            ConfigRegistry::load(&dir).expect("registry reloads"),
        ));
        let record = restarted.get("w226-persist").expect("row survives");
        assert_eq!(record.scopes, vec!["read".to_string()]);
        assert_eq!(record.description, "after");
        assert!(restarted.verify("w226-persist", &secret));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn scope_catalogue_lists_the_documented_names_with_fields() {
        // W2-27 catalogue shape: every entry carries a non-empty name
        // and summary, names are unique, and the registry predicate
        // agrees with the served list.
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
}
