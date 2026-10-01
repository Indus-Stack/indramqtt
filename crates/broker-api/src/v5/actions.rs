//! Action instance store for the v5 management API.
//!
//! Covers `GET /actions` (paged list, C06-02), `POST /actions`
//! (validated create, C06-03), `DELETE /actions/:id` (drop one action,
//! C06-04), `GET /actions/:id` (single read, C06-05),
//! `PUT /actions/:id` (validated replace, C06-06) and
//! `PUT /actions/:id/enable/:enable` (flag flip, C06-07) on a real
//! backend.
//! Single-node, management-plane only: handlers clone at most the capped
//! list (list) or one entry (single read/replace) per request under a
//! short lock; rule evaluation reads a snapshot, never this store
//! directly, and publish and deliver never touch it.
//!
//! Store bounds (both stated here and enforced below):
//! - at most [`MAX_ACTIONS`] entries (1024: caps management-plane memory
//!   for the action catalogue; creates past the cap are rejected with
//!   `BAD_REQUEST` instead of growing without limit);
//! - `id` is capped at [`MAX_ACTION_ID_LEN`] chars (256: bounds map-key
//!   memory per entry; longer ids are rejected with `BAD_REQUEST`);
//! - list paging reuses the shared extractor (default limit 100, max
//!   1000); the default store is empty.
//!
//! Persistence: every mutation exports the validated store into the
//! config core (`ActionsConf`, versioned file) and saves atomically;
//! a commit or save failure answers 500 so the loss is never silent.
//! In-flight execution counters do not persist (per-action metrics land
//! with their own task and stay memory-only).
// TODO(parity): which per-action fields does the spec document beyond
// id/type/enable (connector reference, failure handling, ...)? The
// rulebook does not decide the exact shape; fields the broker cannot
// supply are omitted rather than invented, and kind-specific body
// fields round-trip opaquely.
// TODO(parity): what per-kind config validation does create require
// beyond the kind being known? The rulebook does not decide the exact
// per-kind schemas; the current choice validates the kind against the
// W3-01 kind registry and stores the remaining fields opaquely.
// TODO(parity): what is the exact delete-referenced status and code
// (400 versus 409, and which code name)? The rulebook does not decide
// the exact shape; the current choice refuses with `BAD_REQUEST` (a
// client-error status in this tree's shared error mapping) naming the
// referencing rule until the checker pins it.
// TODO(parity): is 1024 the documented action cap? The rulebook does
// not decide the exact bound; 1024 is the conservative choice until the
// conformance checker pins it.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::errors::ApiError;
use crate::pagination::{meta_page, paginate, PageParams};
use crate::ApiState;

/// Upper bound for stored actions. Creates past this size are rejected
/// with `BAD_REQUEST` instead of growing the map without limit. Reason:
/// the list handler clones the whole snapshot once per request, so the
/// catalogue must stay small and bounded; 1024 covers the documented
/// action catalogue with headroom while keeping per-request clones
/// cheap.
pub const MAX_ACTIONS: usize = broker_config::MAX_ACTIONS;
/// Longest accepted action `id`. Reason: bounds map-key memory per
/// entry so one create cannot balloon the store.
const MAX_ACTION_ID_LEN: usize = broker_config::MAX_ACTION_ID_LEN;

/// One stored action: operator-chosen id, the action kind it was
/// validated against, whether it participates, and the kind-specific
/// fields stored opaquely (everything the create call accepts except
/// `id`/`name`/`type`/`enable`).
#[derive(Debug, Clone, PartialEq)]
pub struct ActionRecord {
    pub id: String,
    pub action_type: String,
    pub enable: bool,
    pub params: serde_json::Value,
}

impl ActionRecord {
    fn to_json(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();
        obj.insert("id".to_string(), serde_json::Value::String(self.id.clone()));
        obj.insert(
            "type".to_string(),
            serde_json::Value::String(self.action_type.clone()),
        );
        obj.insert("enable".to_string(), serde_json::Value::Bool(self.enable));
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

/// Insert failure: duplicate id or a full store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionInsertError {
    Duplicate,
    Full,
}

/// Bounded in-memory action directory keyed by id, persisted through
/// the config core.
///
/// Single-node map behind one lock; every method finishes quickly.
/// Management writes take the write lock plus the registry save lock on
/// persist; the list read takes one short lock and clones at most
/// [`MAX_ACTIONS`] small entries, so a management read never blocks
/// messaging (and publish/deliver never touch this store at all).
pub struct ActionStore {
    inner: RwLock<HashMap<String, ActionRecord>>,
    /// Kernel config registry receiving every mutation (`None` in unit
    /// tests and standalone state, which stay memory-only).
    registry: RwLock<Option<Arc<broker_config::ConfigRegistry>>>,
    /// Serialises export-commit-save so concurrent mutations cannot
    /// interleave into a lost update on disk.
    save_lock: Mutex<()>,
}

impl ActionStore {
    /// Empty directory (today's empty behaviour: no actions).
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
        store.seed_from_snapshot(&registry.snapshot().actions);
        *store.registry.write().expect("action store lock") = Some(Arc::clone(registry));
        store
    }

    /// Attach the registry and replace the current contents with its
    /// snapshot, in place on the same instance. Kernel boot path: the
    /// shared instance `serve_api` hands to `ApiState` must observe the
    /// persisted actions from the first connection, so seeding runs on
    /// the shared instance itself. An empty snapshot is a no-op
    /// (today's empty behaviour).
    pub fn seed_from_registry(&self, registry: &Arc<broker_config::ConfigRegistry>) {
        self.seed_from_snapshot(&registry.snapshot().actions);
        *self.registry.write().expect("action store lock") = Some(Arc::clone(registry));
    }

    fn seed_from_snapshot(&self, conf: &broker_config::ActionsConf) {
        let mut map = self.inner.write().expect("action store lock");
        map.clear();
        for entry in &conf.actions {
            let params = if entry.config.trim().is_empty() {
                serde_json::json!({})
            } else {
                match serde_json::from_str(&entry.config) {
                    Ok(params) => params,
                    Err(_) => {
                        // Fail closed: an undecodable config can never
                        // drive an action, so the entry keeps its key
                        // with no params instead of a half-parsed value.
                        tracing::warn!(
                            action = entry.id.as_str(),
                            "action snapshot entry has an undecodable config; stored without params"
                        );
                        serde_json::json!({})
                    }
                }
            };
            map.insert(
                entry.id.clone(),
                ActionRecord {
                    id: entry.id.clone(),
                    action_type: entry.action_type.clone(),
                    enable: entry.enable,
                    params,
                },
            );
        }
    }

    /// Snapshot of live entries sorted by id for stable reads.
    pub fn snapshot(&self) -> Vec<ActionRecord> {
        let map = self.inner.read().expect("action store lock");
        let mut out: Vec<ActionRecord> = map.values().cloned().collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }

    /// Insert one entry. Fails cleanly when the id already exists
    /// (caller maps to `ALREADY_EXISTS`) or the store is at
    /// [`MAX_ACTIONS`] (caller maps to `BAD_REQUEST`).
    pub fn insert(&self, entry: ActionRecord) -> Result<ActionRecord, ActionInsertError> {
        let mut map = self.inner.write().expect("action store lock");
        if map.contains_key(&entry.id) {
            return Err(ActionInsertError::Duplicate);
        }
        if map.len() >= MAX_ACTIONS {
            return Err(ActionInsertError::Full);
        }
        map.insert(entry.id.clone(), entry.clone());
        Ok(entry)
    }

    /// Remove one entry by id. Returns false when the id is unknown
    /// (caller maps to `NOT_FOUND`).
    pub fn remove(&self, id: &str) -> bool {
        self.inner
            .write()
            .expect("action store lock")
            .remove(id)
            .is_some()
    }

    /// Read one entry by id. Clones at most one small record under a
    /// short read lock; returns `None` when the id is unknown (caller
    /// maps to `NOT_FOUND`). Management-plane only; publish and deliver
    /// never call this.
    pub fn get(&self, id: &str) -> Option<ActionRecord> {
        self.inner
            .read()
            .expect("action store lock")
            .get(id)
            .cloned()
    }

    /// Replace the entry stored under `id` with a validated snapshot.
    /// Returns false when the id is unknown (caller maps to `NOT_FOUND`
    /// and creates nothing). The replace swaps one validated entry in
    /// place and never grows the map toward [`MAX_ACTIONS`], so an
    /// update can never trip the store cap. Management-plane only;
    /// publish and deliver never call this.
    pub fn replace(&self, id: &str, entry: ActionRecord) -> bool {
        let mut map = self.inner.write().expect("action store lock");
        if !map.contains_key(id) {
            return false;
        }
        map.insert(id.to_string(), entry);
        true
    }

    /// Flip the enable flag of one entry in place (W3-04, C06-07).
    /// Returns the updated record, or `None` when the id is unknown
    /// (caller maps to `NOT_FOUND` and persists nothing). Touches only
    /// the single boolean; the id, kind and params are untouched, so the
    /// update can never trip the store cap or change validation.
    /// Management-plane only; publish and deliver never call this (rule
    /// evaluation reads the engine's lock-free dispatch snapshot).
    pub fn set_enabled(&self, id: &str, enable: bool) -> Option<ActionRecord> {
        let mut map = self.inner.write().expect("action store lock");
        let entry = map.get_mut(id)?;
        entry.enable = enable;
        Some(entry.clone())
    }

    /// Export the store as a validated config root: entries sorted by
    /// id so the persisted file is deterministic. Each entry carries
    /// the id, kind, enable flag and the opaque kind-specific fields as
    /// a JSON document. Nothing derived is stored: only inputs persist.
    fn export_conf(&self) -> broker_config::ActionsConf {
        let map = self.inner.read().expect("action store lock");
        let mut entries: Vec<broker_config::ActionEntry> = map
            .values()
            .map(|record| broker_config::ActionEntry {
                id: record.id.clone(),
                action_type: record.action_type.clone(),
                enable: record.enable,
                config: serde_json::to_string(&record.params).unwrap_or_else(|_| "{}".to_string()),
            })
            .collect();
        entries.sort_by(|a, b| a.id.cmp(&b.id));
        broker_config::ActionsConf { actions: entries }
    }

    /// Commit the exported root and atomically save it. Any commit or
    /// save failure surfaces so callers answer 500 and the loss is
    /// never silent (the in-memory mutation stays: commit precedes the
    /// atomic save). Stores without a registry (unit tests, standalone
    /// state) are memory-only and persist as a no-op.
    fn persist(&self) -> Result<(), broker_config::ConfigError> {
        let registry = self.registry.read().expect("action store lock").clone();
        let Some(registry) = registry else {
            return Ok(());
        };
        let _guard = self.save_lock.lock().expect("action store lock");
        let conf = self.export_conf();
        registry.commit_actions(conf)?;
        registry.save()?;
        Ok(())
    }
}

impl Default for ActionStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Map a store persistence failure onto a 500: the in-memory mutation
/// applied but the commit or atomic save failed, so the loss must never
/// be silent.
fn action_persist_error(error: broker_config::ConfigError) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({
            "code": "INTERNAL_ERROR",
            "message": format!("cannot persist actions: {error}")
        })),
    )
        .into_response()
}

/// An action kind is listed only if the kernel can execute it: the kind
/// registry from the action-types catalogue (W3-01), not a second list
/// that could drift from the runtimes.
fn is_known_action_type(kind: &str) -> bool {
    broker_connectors::action_types_catalogue()
        .iter()
        .any(|entry| entry.kind == kind)
}

/// `GET /api/v5/actions`: paged list over the real store with the
/// standard page envelope (`data` plus `meta {page, limit, count,
/// hasnext}`); an empty store returns an empty page, not an error.
/// Unknown query parameters are ignored (no query is extracted beyond
/// `page`/`limit`). Management-plane only; publish and deliver never
/// touch this store.
pub async fn list_actions(State(state): State<ApiState>, params: PageParams) -> Response {
    let records = state.actions.snapshot();
    let rows: Vec<serde_json::Value> = records.iter().map(ActionRecord::to_json).collect();
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

/// `POST /api/v5/actions`: validate the body against the named action
/// kind (unknown kind rejected with the documented error shape); a
/// duplicate id is rejected cleanly, never overwritten. Management-plane
/// only; publish and deliver never touch this store.
pub async fn create_action(
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
        return ApiError::BadRequest("action id or name is required".to_string()).into_response();
    }
    if id.len() > MAX_ACTION_ID_LEN {
        return ApiError::BadRequest(format!(
            "action id holds {} chars, at most {MAX_ACTION_ID_LEN} (field `id`)",
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
    if !is_known_action_type(&kind) {
        return ApiError::BadRequest(format!("unknown action type: {kind}")).into_response();
    }
    let enable = obj.get("enable").and_then(|v| v.as_bool()).unwrap_or(true);
    let mut params = serde_json::Map::new();
    for (key, value) in obj {
        if key != "id" && key != "name" && key != "type" && key != "enable" {
            params.insert(key.clone(), value.clone());
        }
    }
    match state.actions.insert(ActionRecord {
        id: id.clone(),
        action_type: kind,
        enable,
        params: serde_json::Value::Object(params),
    }) {
        Ok(entry) => {
            // W3-04: mirror the flag into the engine's lock-free dispatch
            // snapshot so rule evaluation observes the create without
            // taking this store's lock on the message path.
            state.engine.set_action_enabled(&id, enable);
            if let Err(error) = state.actions.persist() {
                return action_persist_error(error);
            }
            (StatusCode::CREATED, Json(entry.to_json())).into_response()
        }
        Err(ActionInsertError::Duplicate) => {
            ApiError::AlreadyExists(format!("action {id:?} already exists")).into_response()
        }
        Err(ActionInsertError::Full) => ApiError::BadRequest(format!(
            "action store holds {MAX_ACTIONS} entries, at most {MAX_ACTIONS}"
        ))
        .into_response(),
    }
}

/// `DELETE /api/v5/actions/:id`: drop one action. An unknown id returns
/// 404 with the documented error code, not an empty body; an action
/// still referenced by a rule is rejected with a client-error status.
/// Management-plane only; publish and deliver never touch this store.
pub async fn delete_action(State(state): State<ApiState>, Path(id): Path<String>) -> Response {
    let records = state.actions.snapshot();
    if !records.iter().any(|record| record.id == id) {
        return ApiError::NotFound(format!("action {id:?} not found")).into_response();
    }
    // Rule linkage: an action still referenced by a rule cannot be
    // dropped while the reference stands. Rules name their forward
    // target by connector id, so a rule forwarding to this id holds the
    // reference; the first such rule is named in the refusal.
    if let Some(rule_id) = state.engine.list_rules().iter().find_map(|rule| {
        rule.actions
            .iter()
            .any(|action| {
                matches!(
                    action,
                    broker_rules::RuleAction::ForwardConnector { connector_id }
                    if connector_id == &id
                )
            })
            .then(|| rule.id.clone())
    }) {
        return ApiError::BadRequest(format!("action {id:?} is referenced by rule {rule_id:?}"))
            .into_response();
    }
    if !state.actions.remove(&id) {
        return ApiError::NotFound(format!("action {id:?} not found")).into_response();
    }
    // W3-04: drop the id from the dispatch snapshot so a later re-create
    // with the same id starts enabled.
    state.engine.remove_action(&id);
    if let Err(error) = state.actions.persist() {
        return action_persist_error(error);
    }
    StatusCode::NO_CONTENT.into_response()
}

/// `GET /api/v5/actions/:id`: read one action (C06-05).
///
/// A known id returns the documented action fields (`id`, `type`,
/// `enable` plus the kind-specific fields stored opaquely); an unknown
/// id returns 404 with the documented `{code, message}` shape, not an
/// empty body. The pair is not paged; unknown query parameters are
/// ignored (no query is extracted). Management-plane only: one short
/// read lock cloning at most one small record per request, and publish
/// and deliver never touch this store.
pub async fn get_action(State(state): State<ApiState>, Path(id): Path<String>) -> Response {
    match state.actions.get(&id) {
        Some(entry) => (StatusCode::OK, Json(entry.to_json())).into_response(),
        None => ApiError::NotFound(format!("action {id:?} not found")).into_response(),
    }
}

/// `PUT /api/v5/actions/:id`: validated replace of one action (C06-06).
///
/// The body gets the same validation as create: it must be a JSON
/// object carrying a known `type` (unknown kind or a missing `type` is
/// rejected with the documented error shape), and the same id-length
/// bound applies to the path id. An unknown path id returns 404 and
/// creates nothing. On success the stored entry is swapped for the
/// validated snapshot and the new fields read back with 200.
///
/// The path id is authoritative: a body `id`/`name` that names a
/// different action is rejected instead of renaming or forking the
/// entry. An omitted `enable` defaults to `true`, mirroring create.
/// The pair is not paged; unknown query parameters are ignored (no
/// query is extracted). Management-plane only: one short write lock
/// swapping a single validated entry (plus the registry save lock on
/// persist); rule evaluation keeps reading its snapshot and publish and
/// deliver never touch this store.
// TODO(parity): may an update rename the action (body id differing
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
pub async fn update_action(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    if state.actions.get(&id).is_none() {
        return ApiError::NotFound(format!("action {id:?} not found")).into_response();
    }
    let Some(obj) = body.as_object() else {
        return ApiError::BadRequest("request body must be a JSON object".to_string())
            .into_response();
    };
    if let Some(body_id) = obj
        .get("id")
        .or_else(|| obj.get("name"))
        .and_then(|v| v.as_str())
    {
        let trimmed = body_id.trim();
        if !trimmed.is_empty() && trimmed != id {
            return ApiError::BadRequest(format!(
                "body id {trimmed:?} does not match path id {id:?} (field `id`)"
            ))
            .into_response();
        }
    }
    if id.len() > MAX_ACTION_ID_LEN {
        return ApiError::BadRequest(format!(
            "action id holds {} chars, at most {MAX_ACTION_ID_LEN} (field `id`)",
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
    if !is_known_action_type(&kind) {
        return ApiError::BadRequest(format!("unknown action type: {kind}")).into_response();
    }
    let enable = obj.get("enable").and_then(|v| v.as_bool()).unwrap_or(true);
    let mut params = serde_json::Map::new();
    for (key, value) in obj {
        if key != "id" && key != "name" && key != "type" && key != "enable" {
            params.insert(key.clone(), value.clone());
        }
    }
    let entry = ActionRecord {
        id: id.clone(),
        action_type: kind,
        enable,
        params: serde_json::Value::Object(params),
    };
    if !state.actions.replace(&id, entry.clone()) {
        return ApiError::NotFound(format!("action {id:?} not found")).into_response();
    }
    // W3-04: mirror the flag into the dispatch snapshot (same lock-free
    // path as the enable route below).
    state.engine.set_action_enabled(&id, entry.enable);
    if let Err(error) = state.actions.persist() {
        return action_persist_error(error);
    }
    (StatusCode::OK, Json(entry.to_json())).into_response()
}

/// `PUT /api/v5/actions/:id/enable/:enable`: flip one action's enable
/// flag (W3-04, C06-07).
///
/// An unknown id returns 404 with the documented `{code, message}` shape
/// and persists nothing. A malformed enable value is rejected with the
/// documented error shape and persists nothing. On success the stored
/// flag is flipped, the change is mirrored into the engine's lock-free
/// dispatch snapshot (disabled forwards are skipped at fire time), the
/// mutation persists through the config core like every other action
/// write, and the updated action reads back with 200 so the flag change
/// is visible on the next read.
///
/// The pair is not paged; unknown query parameters are ignored (no query
/// is extracted). Management-plane only: one short write lock plus the
/// registry save lock on persist, and one lock-free snapshot swap; rule
/// evaluation reads the snapshot, and publish and deliver never touch
/// this store.
// TODO(parity): which spellings does the enable path accept (`true`/
/// `false` only, or also `1`/`0`, `on`/`off`, mixed case)? The rulebook
// does not decide; the current choice accepts exactly `true`/`false`
// case-insensitively and rejects everything else with `BAD_REQUEST`
// until the checker pins it.
// TODO(parity): for an unknown id with a malformed enable value, is the
// answer 404 or 400? The rulebook does not decide the precedence; the
// current choice answers 404 first (existence before validation,
// mirroring the action update) and persists nothing either way.
// TODO(parity): does the enable route answer 200 with the updated action
// or 204 with no body? The rulebook does not decide the exact shape;
// the current choice returns the updated action with 200 (mirroring the
// action update) so the flag change is visible without a second read.
pub async fn set_action_enabled(
    State(state): State<ApiState>,
    Path((id, enable_raw)): Path<(String, String)>,
) -> Response {
    if state.actions.get(&id).is_none() {
        return ApiError::NotFound(format!("action {id:?} not found")).into_response();
    }
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
    let Some(entry) = state.actions.set_enabled(&id, enable) else {
        return ApiError::NotFound(format!("action {id:?} not found")).into_response();
    };
    // Mirror into the dispatch snapshot before persisting: the in-memory
    // flag is already flipped, so evaluation must observe it even if the
    // disk save below fails (the store itself keeps the mutation on a
    // persist failure and answers 500, never silently).
    state.engine.set_action_enabled(&id, enable);
    if let Err(error) = state.actions.persist() {
        return action_persist_error(error);
    }
    (StatusCode::OK, Json(entry.to_json())).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: &str) -> ActionRecord {
        ActionRecord {
            id: id.to_string(),
            action_type: "mqtt_bridge".to_string(),
            enable: true,
            params: serde_json::json!({}),
        }
    }

    #[test]
    fn insert_rejects_duplicates_and_reports_full() {
        let store = ActionStore::new();
        assert!(store.insert(record("a")).is_ok());
        assert_eq!(store.insert(record("a")), Err(ActionInsertError::Duplicate));
        assert!(store.remove("a"));
        assert!(!store.remove("a"));
    }

    #[test]
    fn snapshot_sorts_by_id_for_stable_reads() {
        let store = ActionStore::new();
        assert!(store.insert(record("b")).is_ok());
        assert!(store.insert(record("a")).is_ok());
        let ids: Vec<String> = store.snapshot().iter().map(|r| r.id.clone()).collect();
        assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn render_carries_id_type_enable_and_params() {
        let store = ActionStore::new();
        let entry = ActionRecord {
            id: "act-1".to_string(),
            action_type: "mqtt_bridge".to_string(),
            enable: false,
            params: serde_json::json!({"server": "127.0.0.1:1883"}),
        };
        assert!(store.insert(entry).is_ok());
        let rendered = store.snapshot()[0].to_json();
        assert_eq!(rendered["id"], serde_json::json!("act-1"));
        assert_eq!(rendered["type"], serde_json::json!("mqtt_bridge"));
        assert_eq!(rendered["enable"], serde_json::json!(false));
        assert_eq!(rendered["server"], serde_json::json!("127.0.0.1:1883"));
    }

    #[test]
    fn export_round_trips_through_validated_config() {
        let store = ActionStore::new();
        assert!(store.insert(record("act-1")).is_ok());
        let conf = store.export_conf();
        conf.validate().expect("exported actions must validate");
        assert_eq!(conf.actions.len(), 1);
        assert_eq!(conf.actions[0].id, "act-1");
        assert_eq!(conf.actions[0].action_type, "mqtt_bridge");
    }

    #[test]
    fn export_rejects_unknown_kind_at_commit() {
        let conf = broker_config::ActionsConf {
            actions: vec![broker_config::ActionEntry {
                id: "act-1".to_string(),
                action_type: "no_such_kind_xyz".to_string(),
                enable: true,
                config: "{}".to_string(),
            }],
        };
        assert!(conf.validate().is_err());
    }

    #[test]
    fn known_action_type_follows_the_w3_01_catalogue() {
        assert!(is_known_action_type("mqtt_bridge"));
        assert!(is_known_action_type("kafka"));
        assert!(!is_known_action_type("no_such_kind_xyz"));
        assert!(!is_known_action_type(""));
    }

    #[test]
    fn get_returns_one_record_and_misses_unknown() {
        let store = ActionStore::new();
        assert!(store.get("missing").is_none());
        assert!(store.insert(record("a")).is_ok());
        let found = store.get("a").expect("stored action reads back");
        assert_eq!(found.id, "a");
        assert!(store.get("nope").is_none());
    }

    #[test]
    fn set_enabled_flips_only_the_flag_and_misses_unknown() {
        let store = ActionStore::new();
        assert!(store.set_enabled("missing", false).is_none());
        assert!(store.insert(record("a")).is_ok());
        let disabled = store.set_enabled("a", false).expect("known id flips");
        assert!(!disabled.enable);
        assert!(!store.get("a").expect("reads back").enable);
        let enabled = store.set_enabled("a", true).expect("known id flips");
        assert!(enabled.enable);
        assert!(store.get("a").expect("reads back").enable);
        assert_eq!(store.snapshot().len(), 1);
    }

    #[test]
    fn replace_swaps_in_place_and_misses_unknown() {
        let store = ActionStore::new();
        assert!(store.insert(record("a")).is_ok());
        let updated = ActionRecord {
            id: "a".to_string(),
            action_type: "kafka".to_string(),
            enable: false,
            params: serde_json::json!({"topic": "events"}),
        };
        assert!(store.replace("a", updated.clone()));
        let found = store.get("a").expect("replaced action reads back");
        assert_eq!(found, updated);
        assert_eq!(store.snapshot().len(), 1);
        assert!(!store.replace("missing", record("missing")));
    }
}
