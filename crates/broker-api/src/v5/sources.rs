//! Source instance store for the v5 management API.
//!
//! Covers `GET /sources` (paged list, C07-12), `POST /sources`
//! (validated create, C07-13), `DELETE /sources/:id` (drop one source,
//! C07-14), `GET /sources/:id` (single read, C07-15),
//! `PUT /sources/:id` (validated replace, C07-16),
//! `POST /sources/:id/:operation` (C07-20, run one lifecycle
//! operation against one source), `PUT /sources/:id/enable/:enable`
//! (C07-17, flip one source's enable flag on the same runtime core) and `GET /sources_summary` (C07-22,
//! aggregated counts over the live store). Single-node,
//! management-plane only: the list clones at most the capped entries
//! per request under a short lock and the summary is computed on demand
//! from a bounded snapshot clone, so aggregation never runs on the
//! publish or deliver path and neither path references this store.
//!
//! Runtime core: the lifecycle state is the stored `enable` flag. `start`
//! sets it, `stop` clears it; later source tasks reuse [`SourceStore::get`]
//! and [`SourceStore::set_enabled`] rather than reimplementing the flip.
//! Ingress runs on its own runtime (a dedicated source task consulting a
//! snapshot off the message path), never inline on publish or deliver.
//! The registry itself persists across a restart via the config core
//! (`SourcesConf`, versioned file); every mutation exports the validated
//! store and saves atomically, and a commit or save failure answers 500
//! so the loss is never silent.
//!
//! Store bounds (both stated here and enforced below):
//! - at most [`MAX_SOURCES`] entries (512: caps management-plane memory
//!   for the ingress catalogue; creates past the cap are rejected with
//!   `BAD_REQUEST` instead of growing without limit);
//! - `id` is capped at [`MAX_SOURCE_ID_LEN`] chars (256: bounds map-key
//!   memory per entry; longer ids are rejected with `BAD_REQUEST`);
//! - the list is not paged, as the reference API documents; the default
//!   store is empty;
//! - every entry is three small fields (`id`, `type`, `enable`), so
//!   per-entry memory is constant and a list/summary snapshot clones at
//!   most the capped list once per request under a short lock.
// TODO(parity): which operation names does the documented spec serve
// beyond `start`/`stop` (for example `restart`)? The rulebook does not
// decide the exact verb set; the current choice serves exactly
// `start`/`stop` as enable flips (the only lifecycle the kernel tracks,
// mirroring the connector operation) and rejects everything else with
// `BAD_REQUEST` until the checker pins it.
// TODO(parity): are operation names case-sensitive (`START` versus
// `start`)? The rulebook does not decide; the current choice matches
// exactly `start`/`stop` in lower case and rejects every other spelling
// with `BAD_REQUEST` until the checker pins it.
// TODO(parity): for an unknown id with an unknown operation, is the
// answer 404 or 400? The rulebook does not decide the precedence; the
// current choice answers 404 first (existence before validation,
// mirroring the connector operation) and persists nothing either way.
// TODO(parity): does a valid operation answer 200 with the updated source
// or 204 with no body? The rulebook does not decide the exact shape; the
// current choice returns the updated source with 200 (mirroring the
// connector operation) so the resulting state is visible without a
// second read.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::errors::ApiError;
use crate::node_scope::resolve_node;
use crate::ApiState;

use crate::node_scope::local_node_name;

/// Upper bound for stored sources. Creates past this size are rejected
/// with `BAD_REQUEST` instead of growing the map without limit. Reason:
/// the list handler clones the whole snapshot once per request, so the
/// catalogue must stay small and bounded; 512 covers the documented
/// ingress catalogue with headroom while keeping per-request clones
/// cheap.
pub const MAX_SOURCES: usize = broker_config::MAX_SOURCES;
/// Longest accepted source `id`. Reason: bounds map-key memory per
/// entry so one create cannot balloon the store.
const MAX_SOURCE_ID_LEN: usize = broker_config::MAX_SOURCE_ID_LEN;

/// One stored source: operator-chosen bare id, the ingress kind it was
/// validated against, whether it is enabled (disabled sources stop
/// ingress; the flag is visible on the next summary read), the
/// documented `connector` reference, the documented `parameters` and
/// `resource_opts` objects, and all remaining fields stored opaquely.
#[derive(Debug, Clone, PartialEq)]
pub struct SourceEntry {
    pub id: String,
    pub name: String,
    pub source_type: String,
    pub enable: bool,
    pub connector: String,
    pub parameters: serde_json::Value,
    pub resource_opts: serde_json::Value,
    pub params: serde_json::Value,
}

impl SourceEntry {
    fn to_json(&self) -> serde_json::Value {
        let status = super::typed::typed_status(self.enable);
        let mut obj = serde_json::Map::new();
        obj.insert("id".to_string(), serde_json::Value::String(self.id.clone()));
        obj.insert(
            "name".to_string(),
            serde_json::Value::String(self.name.clone()),
        );
        obj.insert(
            "type".to_string(),
            serde_json::Value::String(self.source_type.clone()),
        );
        obj.insert("enable".to_string(), serde_json::Value::Bool(self.enable));
        obj.insert(
            "connector".to_string(),
            serde_json::Value::String(self.connector.clone()),
        );
        obj.insert("parameters".to_string(), self.parameters.clone());
        obj.insert("resource_opts".to_string(), self.resource_opts.clone());
        obj.insert(
            "status".to_string(),
            serde_json::Value::String(status.to_string()),
        );
        obj.insert(
            "node_status".to_string(),
            super::typed::typed_node_status(status),
        );
        if let Some(extra) = self.params.as_object() {
            for (key, value) in extra {
                if !obj.contains_key(key) {
                    obj.insert(key.clone(), value.clone());
                }
            }
        }
        serde_json::Value::Object(obj)
    }
}

/// Envelope keys live in first-class fields. All other keys stay in
/// `params` and read back unchanged.
const SOURCE_ENVELOPE_KEYS: &[&str] = &[
    "id",
    "name",
    "type",
    "enable",
    "connector",
    "parameters",
    "resource_opts",
    "status",
    "node_status",
    "status_reason",
];

/// Split a create or replace body into envelope fields plus extras.
/// Returns the error text for a 400 answer.
fn split_source_body(
    body: &serde_json::Value,
    path_bare: Option<&str>,
) -> Result<SourceEntry, String> {
    let Some(obj) = body.as_object() else {
        return Err("request body must be a JSON object".to_string());
    };
    let raw_id = obj
        .get("id")
        .or_else(|| obj.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let kind = obj
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if kind.is_empty() {
        return Err("missing required field: type".to_string());
    }
    if !is_known_source_type(&kind) {
        return Err(format!("unknown source type: {kind}"));
    }
    // The path id is authoritative. The caller resolves it to the
    // stored bare id first. A body id that names another source is
    // rejected. A type change through replace stays allowed.
    let mut id = if raw_id.is_empty() {
        String::new()
    } else {
        super::typed::bare_name(&raw_id, &kind)
    };
    if let Some(expected) = path_bare {
        if !id.is_empty() && id != expected {
            return Err(format!(
                "body id {raw_id:?} does not match path id {expected:?} (field `id`)"
            ));
        }
        id = expected.to_string();
    }
    if id.is_empty() {
        return Err("source id or name is required".to_string());
    }
    if id.len() > MAX_SOURCE_ID_LEN {
        return Err(format!(
            "source id holds {} chars, at most {MAX_SOURCE_ID_LEN} (field `id`)",
            id.len()
        ));
    }
    let enable = obj.get("enable").and_then(|v| v.as_bool()).unwrap_or(true);
    let connector = obj
        .get("connector")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let parameters = match obj.get("parameters") {
        None => serde_json::json!({}),
        Some(value) if value.is_object() => value.clone(),
        Some(_) => return Err("field `parameters` must be an object".to_string()),
    };
    let resource_opts = match obj.get("resource_opts") {
        None => serde_json::json!({}),
        Some(value) if value.is_object() => value.clone(),
        Some(_) => return Err("field `resource_opts` must be an object".to_string()),
    };
    let mut extras = serde_json::Map::new();
    for (key, value) in obj {
        if !SOURCE_ENVELOPE_KEYS.contains(&key.as_str()) {
            extras.insert(key.clone(), value.clone());
        }
    }
    Ok(SourceEntry {
        id: id.clone(),
        name: id,
        source_type: kind,
        enable,
        connector,
        parameters,
        resource_opts,
        params: serde_json::Value::Object(extras),
    })
}

/// Insert failure: duplicate id or a full store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceInsertError {
    Duplicate,
    Full,
}

/// Bounded in-memory source directory keyed by id, persisted through
/// the config core.
///
/// Single-node map behind one lock; every method finishes quickly.
/// Management writes take the write lock plus the registry save lock on
/// persist; the list read takes one short lock and clones at most
/// [`MAX_SOURCES`] small entries, so a management read never blocks
/// messaging (and publish/deliver never touch this store at all).
pub struct SourceStore {
    inner: RwLock<HashMap<String, SourceEntry>>,
    /// Kernel config registry receiving every mutation (`None` in unit
    /// tests and standalone state, which stay memory-only).
    registry: RwLock<Option<Arc<broker_config::ConfigRegistry>>>,
    /// Serialises export-commit-save so concurrent mutations cannot
    /// interleave into a lost update on disk.
    save_lock: Mutex<()>,
}

impl SourceStore {
    /// Empty directory.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            registry: RwLock::new(None),
            save_lock: Mutex::new(()),
        }
    }

    /// Seed from the registry's current snapshot and persist every later
    /// mutation back through it. Kernel boot path; an empty snapshot
    /// yields today's empty behaviour.
    pub fn from_registry(registry: &Arc<broker_config::ConfigRegistry>) -> Self {
        let store = Self::new();
        store.seed_from_snapshot(&registry.snapshot().sources);
        *store.registry.write().expect("source store lock") = Some(Arc::clone(registry));
        store
    }

    /// Attach the registry and replace the current contents with its
    /// snapshot, in place on the same instance. Kernel boot path: the
    /// shared instance `serve_api` hands to `ApiState` must observe the
    /// persisted sources from the first connection, so seeding runs on
    /// the shared instance itself. An empty snapshot is a no-op
    /// (today's empty behaviour).
    pub fn seed_from_registry(&self, registry: &Arc<broker_config::ConfigRegistry>) {
        self.seed_from_snapshot(&registry.snapshot().sources);
        *self.registry.write().expect("source store lock") = Some(Arc::clone(registry));
    }

    fn seed_from_snapshot(&self, conf: &broker_config::SourcesConf) {
        let mut map = self.inner.write().expect("source store lock");
        map.clear();
        for entry in &conf.sources {
            // Old files store an empty document. New files store the
            // envelope (`connector`, `parameters`, `resource_opts`)
            // plus flat extras. Missing envelope keys read as empty.
            let doc = if entry.config.trim().is_empty() {
                serde_json::json!({})
            } else {
                match serde_json::from_str(&entry.config) {
                    Ok(doc) => doc,
                    Err(_) => {
                        tracing::warn!(
                            source = entry.id.as_str(),
                            "source snapshot entry has an undecodable config; stored without params"
                        );
                        serde_json::json!({})
                    }
                }
            };
            let doc_obj = doc.as_object().cloned().unwrap_or_default();
            let connector = doc_obj
                .get("connector")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let parameters = match doc_obj.get("parameters") {
                Some(value) if value.is_object() => value.clone(),
                _ => serde_json::json!({}),
            };
            let resource_opts = match doc_obj.get("resource_opts") {
                Some(value) if value.is_object() => value.clone(),
                _ => serde_json::json!({}),
            };
            let mut extras = serde_json::Map::new();
            for (key, value) in &doc_obj {
                if !SOURCE_ENVELOPE_KEYS.contains(&key.as_str()) {
                    extras.insert(key.clone(), value.clone());
                }
            }
            let bare = super::typed::bare_name(&entry.id, &entry.source_type);
            map.insert(
                bare.clone(),
                SourceEntry {
                    id: bare.clone(),
                    name: bare,
                    source_type: entry.source_type.clone(),
                    enable: entry.enable,
                    connector,
                    parameters,
                    resource_opts,
                    params: serde_json::Value::Object(extras),
                },
            );
        }
    }

    /// Snapshot of live entries sorted by id for stable reads.
    pub fn snapshot(&self) -> Vec<SourceEntry> {
        let map = self.inner.read().expect("source store lock");
        let mut out: Vec<SourceEntry> = map.values().cloned().collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }

    /// Aggregated counts over the live snapshot: (total, enabled,
    /// disabled). Computed on demand from the snapshot, never stored,
    /// so the counts always track creates and deletes.
    pub fn summary_counts(&self) -> (usize, usize, usize) {
        let map = self.inner.read().expect("source store lock");
        let total = map.len();
        let enabled = map.values().filter(|entry| entry.enable).count();
        (total, enabled, total - enabled)
    }

    /// Insert one entry. Fails cleanly when the id already exists
    /// (caller maps to `ALREADY_EXISTS`) or the store is at
    /// [`MAX_SOURCES`] (caller maps to `BAD_REQUEST`).
    pub fn insert(&self, entry: SourceEntry) -> Result<SourceEntry, SourceInsertError> {
        let mut map = self.inner.write().expect("source store lock");
        if map.contains_key(&entry.id) {
            return Err(SourceInsertError::Duplicate);
        }
        if map.len() >= MAX_SOURCES {
            return Err(SourceInsertError::Full);
        }
        map.insert(entry.id.clone(), entry.clone());
        Ok(entry)
    }

    /// Remove one entry by id. Returns false when the id is unknown
    /// (caller maps to `NOT_FOUND`).
    pub fn remove(&self, id: &str) -> bool {
        self.inner
            .write()
            .expect("source store lock")
            .remove(id)
            .is_some()
    }

    /// Read one entry by id. Clones at most one small record under a
    /// short read lock; returns `None` when the id is unknown (caller
    /// maps to `NOT_FOUND`). Management-plane only; publish and deliver
    /// never call this.
    pub fn get(&self, id: &str) -> Option<SourceEntry> {
        self.inner
            .read()
            .expect("source store lock")
            .get(id)
            .cloned()
    }

    /// Resolve a path id to the stored bare id. Accepts the bare name
    /// and the `{type}:{name}` key. Returns `None` for unknown ids.
    pub fn resolve_id(&self, requested: &str) -> Option<String> {
        let map = self.inner.read().expect("source store lock");
        if map.contains_key(requested) {
            return Some(requested.to_string());
        }
        map.values()
            .find(|entry| super::typed::id_matches(&entry.id, &entry.source_type, requested))
            .map(|entry| entry.id.clone())
    }

    /// Read one entry by bare name or key. Returns `None` when unknown.
    pub fn get_any(&self, requested: &str) -> Option<SourceEntry> {
        let bare = self.resolve_id(requested)?;
        self.get(&bare)
    }

    /// True when the bare name or key names a stored entry.
    pub fn contains_any(&self, requested: &str) -> bool {
        self.resolve_id(requested).is_some()
    }

    /// Remove one entry by bare name or key. Returns false when unknown.
    pub fn remove_any(&self, requested: &str) -> bool {
        let Some(bare) = self.resolve_id(requested) else {
            return false;
        };
        self.remove(&bare)
    }

    /// Replace the entry stored under the resolved bare id. Returns
    /// false when unknown. Never grows the map.
    pub fn replace_any(&self, requested: &str, entry: SourceEntry) -> bool {
        let Some(bare) = self.resolve_id(requested) else {
            return false;
        };
        self.replace(&bare, entry)
    }

    /// Flip the enable flag of one entry in place (W3-23, C07-20
    /// runtime core). Returns the updated record, or `None` when the id
    /// is unknown (caller maps to `NOT_FOUND` and changes nothing).
    /// Touches only the single boolean; the id and kind are untouched,
    /// so the update can never trip the store cap or change validation.
    /// Later source tasks reuse this flip rather than reimplementing it.
    /// Management-plane only; publish and deliver never call this
    /// (ingress consults a snapshot off the message path).
    pub fn set_enabled(&self, id: &str, enable: bool) -> Option<SourceEntry> {
        let mut map = self.inner.write().expect("source store lock");
        let entry = map.get_mut(id)?;
        entry.enable = enable;
        Some(entry.clone())
    }

    /// Replace the entry stored under `id` with a validated snapshot.
    /// Returns false when the id is unknown (caller maps to `NOT_FOUND`
    /// and creates nothing). The replace swaps one validated entry in
    /// place and never grows the map toward [`MAX_SOURCES`], so an
    /// update can never trip the store cap. Management-plane only;
    /// publish and deliver never call this.
    pub fn replace(&self, id: &str, entry: SourceEntry) -> bool {
        let mut map = self.inner.write().expect("source store lock");
        if !map.contains_key(id) {
            return false;
        }
        map.insert(id.to_string(), entry);
        true
    }

    /// Export the store as a validated config root: entries sorted by
    /// id so the persisted file is deterministic. Each entry carries
    /// the id, kind, enable flag and the envelope (`connector`,
    /// `parameters`, `resource_opts`) plus the opaque extras as a JSON
    /// document. Nothing derived is stored: only inputs persist.
    fn export_conf(&self) -> broker_config::SourcesConf {
        let map = self.inner.read().expect("source store lock");
        let mut entries: Vec<broker_config::SourceEntry> = map
            .values()
            .map(|entry| {
                let mut doc = serde_json::Map::new();
                doc.insert(
                    "connector".to_string(),
                    serde_json::Value::String(entry.connector.clone()),
                );
                doc.insert("parameters".to_string(), entry.parameters.clone());
                doc.insert("resource_opts".to_string(), entry.resource_opts.clone());
                if let Some(extra) = entry.params.as_object() {
                    for (key, value) in extra {
                        if !doc.contains_key(key) {
                            doc.insert(key.clone(), value.clone());
                        }
                    }
                }
                broker_config::SourceEntry {
                    id: entry.id.clone(),
                    source_type: entry.source_type.clone(),
                    enable: entry.enable,
                    config: serde_json::to_string(&serde_json::Value::Object(doc))
                        .unwrap_or_else(|_| "{}".to_string()),
                }
            })
            .collect();
        entries.sort_by(|a, b| a.id.cmp(&b.id));
        broker_config::SourcesConf { sources: entries }
    }

    /// Commit the exported root and atomically save it. Any commit or
    /// save failure surfaces so callers answer 500 and the loss is
    /// never silent (the in-memory mutation stays: commit precedes the
    /// atomic save). Stores without a registry (unit tests, standalone
    /// state) are memory-only and persist as a no-op.
    fn persist(&self) -> Result<(), broker_config::ConfigError> {
        let registry = self.registry.read().expect("source store lock").clone();
        let Some(registry) = registry else {
            return Ok(());
        };
        let _guard = self.save_lock.lock().expect("source store lock");
        let conf = self.export_conf();
        registry.commit_sources(conf)?;
        registry.save()?;
        Ok(())
    }
}

impl Default for SourceStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Source kinds mirror the connector backend families (ingress versus
/// egress over the same backends), so validation reads the shared W3-25
/// source-type registry rather than a second list that could drift from
/// the runtimes (same set the catalogue serves and the stateless source
/// probe checks). A kind is listed only if its runtime exists; the `test`
/// helper has none and is rejected.
fn is_known_source_type(kind: &str) -> bool {
    broker_connectors::is_known_source_type(kind)
}

/// `GET /api/v5/source_types` (C07-11): names of supported source kinds.
///
/// Returns the bare list of documented names (not paged): an empty
/// build reads as `[]`, never an error. Unknown query parameters are
/// ignored (only the ignore sink is extracted). A name is listed only
/// if its runtime exists in this build (`azure_event_grid` has none
/// and stays out). Management-plane only: reads a static list, takes
/// no lock, and publish/deliver never touch it.
///
/// Store wiring (rulebook section 2): no store. The list is the
/// documented enum filtered to the runtimes, shared with the
/// [`is_known_source_type`] predicate above (and the source store
/// validation plus the stateless probe) so the served list and the
/// checks cannot drift. There is no broker event driving it beyond
/// serving the management read, and nothing outside
/// `crates/broker-api` writes or consults it, so
/// `tools/wiring_check.sh` needs no new entry (it only tracks
/// `Arc<crate::v5::...>` store fields on `ApiState`, of which this task
/// adds none).
///
/// Store bounds (stated here and enforced by construction): at most
/// four short strings; the list never grows at runtime.
///
/// Fire-path cost: none. The list is never consulted per message; the
/// publish path keeps paying what message ingress already pays (the
/// rule match in `dispatch_ingress`) with no new lock, allocation or
/// queue from this route.
pub async fn list_source_types(Query(_ignored): Query<HashMap<String, String>>) -> Response {
    (
        StatusCode::OK,
        Json(serde_json::json!(super::typed::SPEC_SOURCE_TYPES)),
    )
        .into_response()
}

/// Map a store persistence failure onto a 500: the in-memory mutation
/// applied but the commit or atomic save failed, so the loss must never
/// be silent.
fn source_persist_error(error: broker_config::ConfigError) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({
            "code": "INTERNAL_ERROR",
            "message": format!("cannot persist sources: {error}")
        })),
    )
        .into_response()
}

/// `GET /api/v5/sources`: every source as a bare JSON array, as the
/// reference API and the management console expect. An empty store reads
/// as `[]`, never an error. The reference API documents no paging, so the
/// route takes none. Unknown query parameters are ignored.
/// Management-plane only; publish and deliver never touch this store.
///
/// Store wiring (rulebook section 2): the backing store is
/// [`SourceStore`], shared with the kernel (`crates/broker-node/src/main.rs`,
/// `serve_api` share) so management reads observe the kernel's directory;
/// the driving broker events are source create/delete/operation writes
/// through this same management plane. Declared `wired` in
/// `tools/wiring.txt`.
///
/// Store bounds (stated here and enforced by construction): at most
/// [`MAX_SOURCES`] entries (512: caps management-plane memory; creates
/// past the cap are rejected); one bounded snapshot clone per request.
///
/// Fire-path cost: none. The list is never consulted per message; the
/// publish path keeps paying nothing for sources (no consult, no lock,
/// no allocation on either path).
pub async fn list_sources(State(state): State<ApiState>) -> Response {
    let entries = state.sources.snapshot();
    let rows: Vec<serde_json::Value> = entries.iter().map(SourceEntry::to_json).collect();
    (StatusCode::OK, Json(rows)).into_response()
}

/// `POST /api/v5/sources`: register one source after validating the
/// body against the named kind. A duplicate id is rejected cleanly,
/// never overwritten. Management-plane only; publish and deliver never
/// touch this store.
// TODO(parity): what per-kind config validation does create require
// beyond the kind being known? The rulebook does not decide the exact
// per-kind schemas; the current choice validates the kind against the
// W3-25 kind registry and keeps the shared `id`/`type`/`enable` envelope
// until the checker pins it.
pub async fn create_source(
    State(state): State<ApiState>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let entry = match split_source_body(&body, None) {
        Ok(entry) => entry,
        Err(reason) => return ApiError::BadRequest(reason).into_response(),
    };
    let id = entry.id.clone();
    let enable = entry.enable;
    match state.sources.insert(entry.clone()) {
        Ok(entry) => {
            // W3-29: register a zeroed per-source metric record and mirror
            // the flag into the engine's lock-free ingress snapshot so the
            // first publish after this create attributes through lock-free
            // atomics (management-plane swaps; the publish path only loads).
            state.engine.ensure_source_metrics(&id);
            state.engine.set_source_enabled(&id, enable);
            if let Err(error) = state.sources.persist() {
                return source_persist_error(error);
            }
            (axum::http::StatusCode::CREATED, Json(entry.to_json())).into_response()
        }
        Err(SourceInsertError::Duplicate) => {
            ApiError::AlreadyExists(format!("source {id:?} already exists")).into_response()
        }
        Err(SourceInsertError::Full) => ApiError::BadRequest(format!(
            "source store holds {MAX_SOURCES} entries, at most {MAX_SOURCES}"
        ))
        .into_response(),
    }
}

/// `DELETE /api/v5/sources/:id`: drop one source. An unknown id
/// returns 404 with the documented error code, not an empty body.
/// Management-plane only; publish and deliver never touch this store.
pub async fn delete_source(State(state): State<ApiState>, Path(id): Path<String>) -> Response {
    let Some(stored) = state.sources.get_any(&id) else {
        return ApiError::NotFound(format!("source {id:?} not found")).into_response();
    };
    let bare = stored.id.clone();
    if state.sources.remove(&bare) {
        // W3-29: drop the per-source metric record so the id reads as
        // unknown (404) until re-created, which starts again at zero; drop
        // the id from the ingress snapshot so a later re-create with the
        // same id starts enabled.
        state.engine.remove_source_metrics(&bare);
        state.engine.remove_source(&bare);
        if let Err(error) = state.sources.persist() {
            return source_persist_error(error);
        }
        axum::http::StatusCode::NO_CONTENT.into_response()
    } else {
        ApiError::NotFound(format!("source {id:?} not found")).into_response()
    }
}

/// `GET /api/v5/sources/:id`: read one source (C07-15).
///
/// A known id returns the documented source fields (`id`, `type`,
/// `enable`); an unknown id returns 404 with the documented
/// `{code, message}` shape, not an empty body. The pair is not paged;
/// unknown query parameters are ignored (only the ignore sink is
/// extracted). Management-plane only: one short read lock cloning at
/// most one small record per request, and publish and deliver never
/// touch this store.
///
/// Store wiring (rulebook section 2): the backing store is
/// [`SourceStore`], shared with the kernel (`crates/broker-node/src/main.rs`,
/// `serve_api` share) so management reads observe the kernel's directory;
/// the driving broker events are source create/replace/delete writes
/// through this same management plane. Declared `wired` in
/// `tools/wiring.txt`.
///
/// Store bounds (stated here and enforced by construction): a read never
/// grows the map, so the [`MAX_SOURCES`] cap from W3-26 applies unchanged
/// and no new accumulation is introduced. Config persists across restart
/// via the config core; the read itself persists nothing.
///
/// Fire-path cost: none. The read is never consulted per message; the
/// publish path keeps paying nothing for sources (no consult, no lock,
/// no allocation on either path).
pub async fn get_source(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Query(_ignored): Query<HashMap<String, String>>,
) -> Response {
    match state.sources.get_any(&id) {
        Some(entry) => (StatusCode::OK, Json(entry.to_json())).into_response(),
        None => ApiError::NotFound(format!("source {id:?} not found")).into_response(),
    }
}

/// `PUT /api/v5/sources/:id`: validated replace of one source (C07-16).
///
/// The body gets the same validation as create: it must be a JSON
/// object carrying a known `type` (unknown kind or a missing `type` is
/// rejected with the documented error shape), and the same id-length
/// bound applies to the path id. An unknown path id returns 404 and
/// creates nothing. On success the stored entry is swapped for the
/// validated snapshot and the new fields read back with 200.
///
/// The path id is authoritative: a body `id`/`name` that names a
/// different source is rejected instead of renaming or forking the
/// entry. An omitted `enable` defaults to `true`, mirroring create.
/// The pair is not paged; unknown query parameters are ignored (no
/// query is extracted). Management-plane only: one short write lock
/// swapping a single validated entry (plus the registry save lock on
/// persist); ingress consults a snapshot off the message path, and
/// publish and deliver never touch this store.
///
/// Store wiring (rulebook section 2): same [`SourceStore`] as the read
/// above (kernel-shared via `serve_api`); the driving broker event is
/// this management replace itself. Config persists across restart via
/// the config core (a commit or save failure answers 500 so the loss is
/// never silent); the replace itself never grows the map toward
/// [`MAX_SOURCES`], so no new accumulation is introduced.
///
/// Fire-path cost: none. The replace swaps a validated snapshot on the
/// management plane; publish and deliver keep paying nothing for
/// sources (no consult, no lock, no allocation on either path).
// TODO(parity): may an update rename the source (body id differing
// from the path id), or change only the config half? The rulebook does
// not decide the exact PUT field set; the current choice keeps the path
// id authoritative and replaces the whole entry in place until the
// checker pins it.
// TODO(parity): does an omitted `enable` keep the stored value or
// default to `true`? The rulebook does not decide; the current choice
// mirrors create (`true`) until the checker pins it.
// TODO(parity): for an unknown id with an invalid body, is the answer
// 404 or 400? The rulebook does not decide the precedence; the current
// choice answers 404 first (existence before validation, mirroring the
// connector update) and creates nothing either way.
pub async fn update_source(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(stored) = state.sources.get_any(&id) else {
        return ApiError::NotFound(format!("source {id:?} not found")).into_response();
    };
    let bare = stored.id.clone();
    let entry = match split_source_body(&body, Some(&bare)) {
        Ok(entry) => entry,
        Err(reason) => return ApiError::BadRequest(reason).into_response(),
    };
    if !state.sources.replace(&bare, entry.clone()) {
        return ApiError::NotFound(format!("source {id:?} not found")).into_response();
    }
    // W3-29: keep the metric record registered across replaces (counters
    // survive a config replace; only delete or reset zeroes them) and
    // mirror the flag into the ingress snapshot (same lock-free path as
    // the enable route below).
    state.engine.ensure_source_metrics(&bare);
    state.engine.set_source_enabled(&bare, entry.enable);
    if let Err(error) = state.sources.persist() {
        return source_persist_error(error);
    }
    (StatusCode::OK, Json(entry.to_json())).into_response()
}

/// `POST /api/v5/sources/:id/:operation`: run one named lifecycle
/// operation against one source (W3-23, C07-20).
///
/// Only the operations the runtime supports are served: `start`
/// (enable the source) and `stop` (disable it). Both flip the stored
/// enable flag in place via [`SourceStore::set_enabled`] and answer 204
/// with no body, as the reference API documents. Later source tasks
/// reuse the same flip (and the same `get` existence check) rather than
/// reimplementing the runtime.
///
/// An unknown source id returns 404 with the documented error code, not
/// an empty body, and changes nothing; an unknown operation name is
/// rejected with the documented error shape and changes nothing. The
/// pair is not paged; unknown query parameters are ignored (no query is
/// extracted). Management-plane only: one short store read plus one
/// short write lock cloning at most one small record per request;
/// ingress runs on its own runtime off the message path, and publish
/// and deliver never touch this store.
///
/// Lifecycle state persists with the source record (the enable flag in
/// the config core); transient run state is not kept, so nothing extra
/// needs clearing across a restart (boot rebuilds the directory from the
/// persisted flags).
///
/// Fire-path cost (stated for the delivery-path note): no new work on
/// publish or deliver. The operation itself runs entirely on the
/// management plane; publish and deliver keep paying nothing for
/// sources (no consult, no lock, no allocation on either path).
pub async fn run_source_operation(
    State(state): State<ApiState>,
    Path((id, operation)): Path<(String, String)>,
) -> Response {
    apply_source_operation(&state, &id, &operation)
}

/// Shared dispatch behind the source operation route: existence check
/// (unknown id is 404, changes nothing), operation validity check
/// (unknown name is `BAD_REQUEST`, changes nothing), then the enable
/// flip and the 204 answer with no body. Management-plane only; see
/// the caller for the locking and fire-path notes.
fn apply_source_operation(state: &ApiState, id: &str, operation: &str) -> Response {
    let Some(stored) = state.sources.get_any(id) else {
        return ApiError::NotFound(format!("source {id:?} not found")).into_response();
    };
    let bare = stored.id.clone();
    let enable = match operation {
        "start" => true,
        "stop" => false,
        _ => {
            return ApiError::BadRequest(format!(
                "unknown source operation {operation:?}: expected \"start\" or \"stop\" (field `operation`)"
            ))
            .into_response();
        }
    };
    if state.sources.set_enabled(&bare, enable).is_none() {
        return ApiError::NotFound(format!("source {id:?} not found")).into_response();
    }
    // Mirror into the ingress snapshot before persisting: the in-memory
    // flag is already flipped, so the next ingress must observe it even if
    // the disk save below fails (the store itself keeps the mutation on a
    // persist failure and answers 500, never silently).
    state.engine.set_source_enabled(&bare, enable);
    if let Err(error) = state.sources.persist() {
        return source_persist_error(error);
    }
    StatusCode::NO_CONTENT.into_response()
}

/// `POST /api/v5/nodes/:node/sources/:id/:operation`: run one named
/// lifecycle operation against one source on one named node (W3-24,
/// C07-10).
///
/// Single-node execution only: the `{node}` segment is checked with the
/// shared [`resolve_node`] helper against the configured
/// [`ApiState::node_id`] (falling back to the [`local_node_name`] name
/// the v5 rows render today); an unknown name returns the helper's 404
/// `NOT_FOUND` shape naming the node and touches no other state. A known
/// node dispatches to the same [`apply_source_operation`] path as the
/// global `POST /sources/:id/:operation` route (W3-23), so the reported
/// state, the 404 for an unknown id, and the rejection of an unknown
/// operation are identical on both routes.
///
/// The pair is not paged; unknown query parameters are ignored (no query
/// is extracted). Management-plane only: one short store read plus one
/// short write lock cloning at most one small record per request;
/// ingress runs on its own runtime off the message path, and publish
/// and deliver never touch this store.
///
/// Bound (stated here and enforced by construction): single-request bounded
/// work with no wait and no accumulated per-node state. The operation
/// completes synchronously under short in-memory locks with no remote call,
/// no retry, no timer and no broker-side timeout to tune (there is no wait
/// to bound: the handler never blocks, so a single request costs at most
/// one short read lock plus one short write lock cloning one small record);
/// node membership itself is owned elsewhere
/// (exactly one local record) and nothing here grows with connections,
/// sessions or subscriptions.
///
/// Fire-path cost: no new work on publish or deliver. The operation itself
/// runs entirely on the management plane; publish and deliver keep paying
/// nothing for sources (no consult, no lock, no allocation on either path).
// TODO(parity): for an unknown node with an unknown id or operation, is
// the answer the node 404 or the source/operation error? The rulebook does
// not decide the precedence; the current choice answers the node 404 first
// (node scope before source existence before operation validity, fail
// closed) and touches nothing else either way.
pub async fn run_node_source_operation(
    State(state): State<ApiState>,
    Path((node, id, operation)): Path<(String, String, String)>,
) -> Response {
    if resolve_node(&state.node_id, &node).is_err()
        && resolve_node(local_node_name(), &node).is_err()
    {
        // Reuse the helper's error so the unknown-node shape stays exactly
        // the documented `{code: NOT_FOUND, message}` body.
        return resolve_node(&state.node_id, &node)
            .expect_err("node already checked as unknown")
            .into_response();
    }
    apply_source_operation(&state, &id, &operation)
}

/// `PUT /api/v5/sources/:id/enable/:enable`: flip one source's enable
/// flag (W3-28, C07-17).
///
/// An unknown id returns 404 with the documented `{code, message}` shape
/// and persists nothing. A malformed enable value is rejected with the
/// documented error shape and persists nothing. On success the stored
/// flag is flipped through the same [`SourceStore::set_enabled`] core the
/// W3-23 operation route uses (the lifecycle state is the stored flag, so
/// the write drives that runtime: disabled sources stop ingress), the
/// mutation persists through the config core with the source record like
/// every other source write, and the answer is 204 with no body, as the
/// reference API documents.
///
/// The pair is not paged; unknown query parameters are ignored (no query
/// is extracted). Management-plane only: one short read lock for the
/// existence check plus one short write lock cloning at most one small
/// record per request (plus the registry save lock on persist); ingress
/// consults a snapshot off the message path, and publish and deliver never
/// touch this store.
///
/// Bound (stated here and enforced by construction): a single boolean
/// per source; no new list, no new accumulation. The flag persists across
/// restart with the source record via the config core.
///
/// Fire-path cost: none. The enable read stays off the message path;
/// publish and deliver keep paying nothing for sources (no consult, no
/// lock, no allocation on either path).
// TODO(parity): which spellings does the enable path accept (`true`/
// `false` only, or also `1`/`0`, `on`/`off`, mixed case)? The rulebook
// does not decide; the current choice accepts exactly `true`/`false`
// case-insensitively and rejects everything else with `BAD_REQUEST`
// (mirroring the action and connector enable routes) until the checker
// pins it.
// TODO(parity): for an unknown id with a malformed enable value, is the
// answer 404 or 400? The rulebook does not decide the precedence; the
// current choice answers 404 first (existence before validation,
// mirroring the source update and operation routes) and persists nothing
// either way.
pub async fn set_source_enabled(
    State(state): State<ApiState>,
    Path((id, enable_raw)): Path<(String, String)>,
) -> Response {
    let Some(stored) = state.sources.get_any(&id) else {
        return ApiError::NotFound(format!("source {id:?} not found")).into_response();
    };
    let bare = stored.id.clone();
    let enable = match enable_raw.trim().to_ascii_lowercase().as_str() {
        "true" => true,
        "false" => false,
        _ => {
            return ApiError::BadRequest(format!(
                "invalid enable value {enable_raw:?}: expected \"true\" or \"false\" (field `enable`)"
            ))
            .into_response();
        }
    };
    if state.sources.set_enabled(&bare, enable).is_none() {
        return ApiError::NotFound(format!("source {id:?} not found")).into_response();
    }
    // Mirror into the ingress snapshot before persisting: the in-memory
    // flag is already flipped, so the next ingress must observe it even if
    // the disk save below fails (the store itself keeps the mutation on a
    // persist failure and answers 500, never silently).
    state.engine.set_source_enabled(&bare, enable);
    if let Err(error) = state.sources.persist() {
        return source_persist_error(error);
    }
    StatusCode::NO_CONTENT.into_response()
}

/// `GET /api/v5/sources/:id/metrics`: live per-source counters (W3-29,
/// C07-18).
///
/// A known id returns the live counters attributed on the ingress path
/// (`received` while enabled, `dropped` while stopped) with zero defaults
/// at creation and after a restart. An unknown id returns 404 with the
/// documented `{code, message}` shape, not an empty body. The pair is not
/// paged; unknown query parameters are ignored (no query beyond the ignore
/// sink is extracted). Management-plane only: one short store read plus
/// lock-free atomic loads; the publish path bumps the same atomics and
/// never takes this store's lock.
///
/// The counter set is fixed per source (two atomics, no history), so a
/// read never grows anything; gauges (`inflight`, `queuing`), rates
/// (`rate`, `rate_max`, `rate_last5m`), `retried` and the `dropped.*`
/// breakdown are omitted because the broker does not track per-source
/// queue depth, retry state or drop causes, and a constant standing in for
/// a measurement would be worse than a missing field.
///
/// Fire-path cost (stated for the delivery-path note): the publish path
/// holds one `Arc` of the metric map plus one `Arc` of the disabled set
/// per ingress, then one `HashSet` lookup plus one relaxed `fetch_add`
/// per registered source — lock-free atomics only, no lock, no queue, no
/// per-fire allocation; text rendering stays here in the handler.
// TODO(parity): which counter names plus envelope shapes does the documented
// spec require beyond `received`/`dropped` (matched, success, failed, rates,
// node rows, ...)? The rulebook does not decide the exact shape; the current
// choice reports the honest live subset the ingress path can attribute
// (`id` plus `metrics {received, dropped}`) and omits the rest rather than
// inventing constants, until the checker pins it.
pub async fn get_source_metrics(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Query(_ignored): Query<HashMap<String, String>>,
) -> Response {
    let Some(stored) = state.sources.get_any(&id) else {
        return ApiError::NotFound(format!("source {id:?} not found")).into_response();
    };
    let bare = stored.id.clone();
    // The record is registered on create/boot, but a standalone engine or
    // a race keeps this fail-closed at zero rather than 500.
    let (received, dropped) = state
        .engine
        .source_metrics_snapshot_for(&bare)
        .unwrap_or((0, 0));
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "id": id,
            "metrics": {
                "received": received,
                "dropped": dropped,
            },
        })),
    )
        .into_response()
}

/// `PUT /api/v5/sources/:id/metrics/reset`: zero one source's counters
/// (W3-29, C07-19).
///
/// Resets the live counters to zero and reports success with 204 and no
/// body. Resetting an unknown id is 404 with the documented `{code,
/// message}` shape, not an empty body. Counters resume from zero on the
/// next ingress; the reset itself persists only as the zeroed values (the
/// map is memory-only and restarts at zero by design). The pair is not
/// paged; unknown query parameters are ignored. Management-plane only: one
/// short store read plus lock-free atomic stores; increments stay lock-free
/// atomics and publish/deliver never touch this store.
// TODO(parity): does the spec report a reset as an empty 204 or as a 200
// with a body? The rulebook does not decide the exact success shape; the
// current choice answers 204 with no body, mirroring the sibling rule and
// action resets, until the checker pins it.
pub async fn reset_source_metrics(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Query(_ignored): Query<HashMap<String, String>>,
) -> Response {
    let Some(stored) = state.sources.get_any(&id) else {
        return ApiError::NotFound(format!("source {id:?} not found")).into_response();
    };
    let bare = stored.id.clone();
    if !state.engine.reset_source_metrics(&bare) {
        // The source exists but its record is missing (standalone engine
        // or a boot race): register a zeroed record and report success so
        // the reset is still observable as zeroes on the next read.
        state.engine.ensure_source_metrics(&bare);
        state.engine.reset_source_metrics(&bare);
    }
    StatusCode::NO_CONTENT.into_response()
}

/// `GET /api/v5/sources_summary` (C07-22): a bare JSON array with one
/// summary row per source, as the reference API documents.
///
/// Each row carries `name`, `type` and `enable`, read from the live store.
/// The broker keeps no health state, no creation time and no rule links
/// for a source, so the rows leave out `status`, `created_at`,
/// `last_modified_at` and `rules` instead of inventing values. An empty
/// store reads as `[]`. The query parameters `ns` and `only_global` are
/// ignored. Management-plane only: one bounded snapshot clone per
/// request, and publish and deliver never touch this store.
pub async fn get_sources_summary(State(state): State<ApiState>) -> Response {
    let rows: Vec<serde_json::Value> = state
        .sources
        .snapshot()
        .iter()
        .map(|entry| {
            serde_json::json!({
                "name": entry.id,
                "type": entry.source_type,
                "enable": entry.enable,
            })
        })
        .collect();
    (StatusCode::OK, Json(rows)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, enable: bool) -> SourceEntry {
        SourceEntry {
            id: id.to_string(),
            name: id.to_string(),
            source_type: "mqtt".to_string(),
            enable,
            connector: String::new(),
            parameters: serde_json::json!({}),
            resource_opts: serde_json::json!({}),
            params: serde_json::json!({}),
        }
    }

    #[test]
    fn get_reads_one_entry_and_misses_unknown() {
        let store = SourceStore::new();
        assert!(store.get("missing").is_none());
        assert!(store.insert(entry("a", true)).is_ok());
        let found = store.get("a").expect("stored source reads back");
        assert_eq!(found.id, "a");
        assert!(found.enable);
        assert!(store.get("nope").is_none());
    }

    #[test]
    fn set_enabled_flips_only_the_flag_and_misses_unknown() {
        let store = SourceStore::new();
        assert!(store.set_enabled("missing", false).is_none());
        assert!(store.insert(entry("a", true)).is_ok());
        let stopped = store.set_enabled("a", false).expect("known id flips");
        assert!(!stopped.enable);
        assert_eq!(stopped.id, "a");
        assert_eq!(stopped.source_type, "mqtt");
        assert!(!store.get("a").expect("reads back").enable);
        let started = store.set_enabled("a", true).expect("known id flips");
        assert!(started.enable);
        assert!(store.get("a").expect("reads back").enable);
        assert_eq!(store.snapshot().len(), 1);
    }

    #[test]
    fn replace_swaps_in_place_and_misses_unknown() {
        let store = SourceStore::new();
        assert!(!store.replace("missing", entry("missing", true)));
        assert!(store.insert(entry("a", true)).is_ok());
        let mut updated = entry("a", false);
        updated.source_type = "http".to_string();
        assert!(store.replace("a", updated));
        let found = store.get("a").expect("replaced source reads back");
        assert_eq!(found.source_type, "http");
        assert!(!found.enable);
        assert_eq!(store.snapshot().len(), 1);
        assert!(store.replace_any("http:a", entry("a", true)));
    }

    #[test]
    fn key_form_resolves_to_the_bare_row() {
        let store = SourceStore::new();
        assert!(store.insert(entry("a", true)).is_ok());
        let found = store.get_any("mqtt:a").expect("key form reads back");
        assert_eq!(found.id, "a");
        assert!(store.contains_any("mqtt:a"));
        assert!(store.get_any("other:a").is_none());
        let rendered = found.to_json();
        assert_eq!(rendered["type"], serde_json::json!("mqtt"));
        assert_eq!(rendered["connector"], serde_json::json!(""));
        assert!(rendered["parameters"].is_object());
        assert_eq!(rendered["status"], serde_json::json!("connected"));
    }

    #[test]
    fn split_body_accepts_the_documented_envelope() {
        let entry = split_source_body(
            &serde_json::json!({
                "type": "kafka_consumer",
                "name": "in",
                "connector": "kafka_consumer:up",
                "parameters": {"topic": "events"},
            }),
            None,
        )
        .expect("documented body parses");
        assert_eq!(entry.id, "in");
        assert_eq!(entry.parameters["topic"], serde_json::json!("events"));
        assert!(split_source_body(&serde_json::json!({"name": "x"}), None).is_err());
        assert!(
            split_source_body(&serde_json::json!({"name": "x", "type": "nope"}), None).is_err()
        );
    }

    #[test]
    fn remove_misses_unknown_after_delete() {
        let store = SourceStore::new();
        assert!(store.insert(entry("a", true)).is_ok());
        assert!(store.remove("a"));
        assert!(!store.remove("a"));
        assert!(store.get("a").is_none());
    }

    #[test]
    fn export_round_trips_through_validated_config() {
        let store = SourceStore::new();
        assert!(store.insert(entry("src-1", true)).is_ok());
        let conf = store.export_conf();
        conf.validate().expect("exported sources must validate");
        assert_eq!(conf.sources.len(), 1);
        assert_eq!(conf.sources[0].id, "src-1");
        assert_eq!(conf.sources[0].source_type, "mqtt");
    }

    #[test]
    fn export_rejects_unknown_kind_at_commit() {
        let conf = broker_config::SourcesConf {
            sources: vec![broker_config::SourceEntry {
                id: "src-1".to_string(),
                source_type: "no_such_kind_xyz".to_string(),
                enable: true,
                config: "{}".to_string(),
            }],
        };
        assert!(conf.validate().is_err());
    }

    #[test]
    fn known_source_type_follows_the_w3_25_registry() {
        assert!(is_known_source_type("mqtt"));
        assert!(is_known_source_type("kafka"));
        assert!(!is_known_source_type("no_such_kind_xyz"));
        assert!(!is_known_source_type(""));
    }
}
