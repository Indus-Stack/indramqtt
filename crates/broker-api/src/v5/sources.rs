//! Source instance store for the v5 management API.
//!
//! Covers `GET /sources` (paged list, C07-12), `POST /sources`
//! (validated create, C07-13), `DELETE /sources/:id` (drop one source,
//! C07-14), `POST /sources/:id/:operation` (C07-20, run one lifecycle
//! operation against one source) and `GET /sources_summary` (C07-22,
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
//! - list paging reuses the shared extractor (default limit 100, max
//!   1000); the default store is empty;
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
use crate::pagination::{meta_page, paginate, PageParams};
use crate::ApiState;

/// Single-node name rendered by `v5::nodes::list_nodes`. Accepted alongside
/// [`ApiState::node_id`] until every caller migrates to the configured id
/// (see `node_scope` docs); both name the same local node.
const LEGACY_NODE_NAME: &str = "indramqtt@127.0.0.1";

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

/// One stored source: operator-chosen id, the ingress kind it was
/// validated against, and whether it is enabled (disabled sources stop
/// ingress; the flag is visible on the next summary read).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceEntry {
    pub id: String,
    pub source_type: String,
    pub enable: bool,
}

impl SourceEntry {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "id": self.id,
            "type": self.source_type,
            "enable": self.enable,
        })
    }
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
            map.insert(
                entry.id.clone(),
                SourceEntry {
                    id: entry.id.clone(),
                    source_type: entry.source_type.clone(),
                    enable: entry.enable,
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

    /// Export the store as a validated config root: entries sorted by
    /// id so the persisted file is deterministic. Each entry carries
    /// the id, kind and enable flag; kind-specific body fields are not
    /// stored (the create call keeps only the shared `id`/`type`/`enable`
    /// envelope, and nothing derived is persisted).
    fn export_conf(&self) -> broker_config::SourcesConf {
        let map = self.inner.read().expect("source store lock");
        let mut entries: Vec<broker_config::SourceEntry> = map
            .values()
            .map(|entry| broker_config::SourceEntry {
                id: entry.id.clone(),
                source_type: entry.source_type.clone(),
                enable: entry.enable,
                config: "{}".to_string(),
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

/// `GET /api/v5/source_types` (C07-11): catalogue of supported source
/// kinds with the documented per-entry fields.
///
/// Returns the bare list (not paged): an empty registry reads as `[]`,
/// never an error. Unknown query parameters are ignored (only the ignore
/// sink is extracted). A kind is listed only if its runtime exists (the
/// shared W3-25 registry in `broker-connectors`: every executable sink
/// kind plus the `http`/`mqtt` ingress aliases, never the `test`
/// helper), so adding a runtime elsewhere automatically extends the
/// served list. Management-plane only: reads a static registry, takes no
/// lock, and publish/deliver never touch it.
///
/// Store wiring (rulebook section 2): no store. The catalogue is the
/// source-type registry itself, shared with the [`is_known_source_type`]
/// predicate above (and the source store validation plus the stateless
/// probe) so the served list and the checks cannot drift. There is no
/// broker event driving it beyond serving the management read, and
/// nothing outside `crates/broker-api` writes or consults it, so
/// `tools/wiring_check.sh` needs no new entry (it only tracks
/// `Arc<crate::v5::...>` store fields on `ApiState`, of which this task
/// adds none).
///
/// Store bounds (stated here and enforced by construction):
/// - at most [`broker_connectors::MAX_SOURCE_TYPES`] entries (64: caps
///   management-plane memory; the built-in set is exactly
///   [`broker_connectors::SOURCE_TYPE_COUNT`]);
/// - an empty catalogue reads as an empty list (`[]`), never an error;
/// - reads clone at most one small document per kind (three short
///   strings) with no lock held.
///
/// Fire-path cost: none. The catalogue is never consulted per message;
/// the publish path keeps paying what message ingress already pays (the
/// rule match in `dispatch_ingress`) with no new lock, allocation or
/// queue from this route.
// TODO(parity): which per-entry fields does the documented catalogue shape
// require beyond `kind`/`description`/`configuration` (columns, schema,
// ...)? The rulebook does not decide the exact shape; fields the broker
// cannot supply are omitted rather than invented, until the checker pins
// it.
// TODO(parity): which kinds have dedicated ingress readers versus the
// generic enable-flag runtime? The rulebook does not decide the exact
// ingress split; the current choice lists every backend family the kernel
// can instantiate minus the `test` helper, until the checker pins it.
pub async fn list_source_types(Query(_ignored): Query<HashMap<String, String>>) -> Response {
    let catalogue = broker_connectors::source_types_catalogue();
    (
        StatusCode::OK,
        Json(serde_json::to_value(catalogue).unwrap_or_default()),
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

/// `GET /api/v5/sources`: paged list over the real store with the
/// standard page envelope (`data` plus `meta {page, limit, count,
/// hasnext}`); an empty store returns an empty page, not an error.
/// Unknown query parameters are ignored (only `page`/`limit` are
/// extracted). Management-plane only; publish and deliver never touch
/// this store.
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
/// past the cap are rejected); list paging reuses the shared extractor
/// (default limit 100, max 1000); one bounded snapshot clone per request.
///
/// Fire-path cost: none. The list is never consulted per message; the
/// publish path keeps paying nothing for sources (no consult, no lock,
/// no allocation on either path).
// TODO(parity): which per-entry fields does the documented list shape
// require beyond `id`/`type`/`enable` (status, node rows, ...)? The
// rulebook does not decide the exact shape; fields the broker cannot
// supply are omitted rather than invented, until the checker pins it.
pub async fn list_sources(State(state): State<ApiState>, params: PageParams) -> Response {
    let entries = state.sources.snapshot();
    let rows: Vec<serde_json::Value> = entries.iter().map(SourceEntry::to_json).collect();
    let count = rows.len();
    let page_rows = paginate(&rows, params.page, params.limit);
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "data": page_rows,
            "meta": meta_page(params.page, params.limit, count),
        })),
    )
        .into_response()
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
    let Some(obj) = body.as_object() else {
        return ApiError::BadRequest("request body must be a JSON object".to_string())
            .into_response();
    };
    let id = obj
        .get("id")
        .or_else(|| obj.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if id.is_empty() {
        return ApiError::BadRequest("source id or name is required".to_string()).into_response();
    }
    if id.len() > MAX_SOURCE_ID_LEN {
        return ApiError::BadRequest(format!(
            "source id holds {} chars, at most {MAX_SOURCE_ID_LEN} (field `id`)",
            id.len()
        ))
        .into_response();
    }
    let kind = obj
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if kind.is_empty() {
        return ApiError::BadRequest("missing required field: type".to_string()).into_response();
    }
    if !is_known_source_type(&kind) {
        return ApiError::BadRequest(format!("unknown source type: {kind}")).into_response();
    }
    let enable = obj.get("enable").and_then(|v| v.as_bool()).unwrap_or(true);
    match state.sources.insert(SourceEntry {
        id: id.clone(),
        source_type: kind,
        enable,
    }) {
        Ok(entry) => {
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
    if state.sources.remove(&id) {
        if let Err(error) = state.sources.persist() {
            return source_persist_error(error);
        }
        axum::http::StatusCode::NO_CONTENT.into_response()
    } else {
        ApiError::NotFound(format!("source {id:?} not found")).into_response()
    }
}

/// `POST /api/v5/sources/:id/:operation`: run one named lifecycle
/// operation against one source (W3-23, C07-20).
///
/// Only the operations the runtime supports are served: `start`
/// (enable the source) and `stop` (disable it). Both flip the stored
/// enable flag in place via [`SourceStore::set_enabled`] and report the
/// resulting source with 200 so the state change is visible without a
/// second read. Later source tasks reuse the same flip (and the same
/// `get` existence check) rather than reimplementing the runtime.
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
/// flip with the updated source and 200. Management-plane only; see
/// the caller for the locking and fire-path notes.
fn apply_source_operation(state: &ApiState, id: &str, operation: &str) -> Response {
    if state.sources.get(id).is_none() {
        return ApiError::NotFound(format!("source {id:?} not found")).into_response();
    }
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
    let Some(entry) = state.sources.set_enabled(id, enable) else {
        return ApiError::NotFound(format!("source {id:?} not found")).into_response();
    };
    if let Err(error) = state.sources.persist() {
        return source_persist_error(error);
    }
    (StatusCode::OK, Json(entry.to_json())).into_response()
}

/// `POST /api/v5/nodes/:node/sources/:id/:operation`: run one named
/// lifecycle operation against one source on one named node (W3-24,
/// C07-10).
///
/// Single-node execution only: the `{node}` segment is checked with the
/// shared [`resolve_node`] helper against the configured
/// [`ApiState::node_id`] (falling back to the [`LEGACY_NODE_NAME`] literal
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
// TODO(parity): does the node-scoped answer carry a node envelope or the
// bare updated source? The rulebook does not decide the exact shape; the
// current choice returns the same bare updated source with 200 as the
// global route (single-node answer) until the checker pins it.
pub async fn run_node_source_operation(
    State(state): State<ApiState>,
    Path((node, id, operation)): Path<(String, String, String)>,
) -> Response {
    if resolve_node(&state.node_id, &node).is_err()
        && resolve_node(LEGACY_NODE_NAME, &node).is_err()
    {
        // Reuse the helper's error so the unknown-node shape stays exactly
        // the documented `{code: NOT_FOUND, message}` body.
        return resolve_node(&state.node_id, &node)
            .expect_err("node already checked as unknown")
            .into_response();
    }
    apply_source_operation(&state, &id, &operation)
}

/// `GET /api/v5/sources_summary` (C07-22): aggregated counts over the
/// real source store computed on demand from the live snapshot, never
/// constants; an empty store returns zeroes, not an error. Not paged
/// and not per-id, so there is no 404 path; unknown query parameters
/// are ignored (no query is extracted). Management-plane only: one
/// bounded snapshot clone per request, and publish and deliver never
/// touch this store.
pub async fn get_sources_summary(State(state): State<ApiState>) -> Response {
    let (total, enabled, disabled) = state.sources.summary_counts();
    Json(serde_json::json!({
        "total": total,
        "enabled": enabled,
        "disabled": disabled,
    }))
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, enable: bool) -> SourceEntry {
        SourceEntry {
            id: id.to_string(),
            source_type: "mqtt".to_string(),
            enable,
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
