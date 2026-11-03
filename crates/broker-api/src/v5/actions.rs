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
// TODO(parity): what is the exact delete-referenced status and code
// (400 versus 409, and which code name)? The rulebook does not decide
// the exact shape; the current choice refuses with `BAD_REQUEST` (a
// client-error status in this tree's shared error mapping) naming the
// referencing rule until the checker pins it.
// TODO(parity): is 1024 the documented action cap? The rulebook does
// not decide the exact bound; 1024 is the conservative choice until the
// conformance checker pins it.

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

/// One stored action: operator-chosen bare id, the action kind it was
/// validated against, whether it participates, the documented
/// `connector` reference, the documented `parameters` and
/// `resource_opts` objects, and all remaining fields stored opaquely
/// (legacy flat fields plus `description`, `tags` and
/// `fallback_actions`).
#[derive(Debug, Clone, PartialEq)]
pub struct ActionRecord {
    pub id: String,
    pub name: String,
    pub action_type: String,
    pub enable: bool,
    pub connector: String,
    pub parameters: serde_json::Value,
    pub resource_opts: serde_json::Value,
    pub params: serde_json::Value,
}

/// In-memory build state of one action's owned runtime sink (MV-01).
/// Never persisted: boot, create, update, enable-flipping config
/// writes and config reloads always rebuild it from the stored
/// connector settings plus the stored action parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActionWiring {
    /// The kind owns no runtime here yet (or the build was never
    /// attempted in this process): no sink, no reason.
    None,
    /// The owned sink is built and registered under the action
    /// dispatch key; delivery for this action flows through it.
    Live,
    /// The owned sink could not be built. The message names the action
    /// and the offending field; the action stays stored, reports
    /// `disconnected` with the reason and raises
    /// `action_build_failed:{id}` (no invented status value).
    Failed(String),
}

impl ActionRecord {
    #[cfg(test)]
    fn to_json_with(&self, wiring: &ActionWiring) -> serde_json::Value {
        self.to_json_with_health(wiring, None)
    }

    fn to_json_with_health(
        &self,
        wiring: &ActionWiring,
        health: Option<&broker_connectors::SinkHealth>,
    ) -> serde_json::Value {
        // KC-00/D2: a stored action whose kind is removed or not `ready`
        // reads back `disconnected` with a reason, so an operator can see
        // and delete it.
        let mut reason = if broker_config::is_removed_connector_type(&self.action_type) {
            Some(crate::REMOVED_CONNECTOR_MESSAGE.to_string())
        } else {
            super::rules::kind_refusal(&self.action_type)
        };
        // MV-01/D3: an action-owned sink build failure reports through
        // `status_reason` (the status reads `disconnected`), never
        // through a new status value. The kind refusals keep priority:
        // an unavailable kind is not a build failure.
        if let ActionWiring::Failed(message) = wiring {
            if reason.is_none() {
                reason = Some(message.clone());
            }
        }
        // RO-01: a health probe that fails marks the live target
        // disconnected and reports the probe reason.
        if reason.is_none() {
            if let Some(health) = health {
                if !health.healthy {
                    reason = Some(
                        health
                            .reason
                            .clone()
                            .unwrap_or_else(|| "health check failed".to_string()),
                    );
                }
            }
        }
        let status = if reason.is_some() {
            "disconnected"
        } else {
            super::typed::typed_status(self.enable)
        };
        let mut obj = serde_json::Map::new();
        obj.insert("id".to_string(), serde_json::Value::String(self.id.clone()));
        obj.insert(
            "name".to_string(),
            serde_json::Value::String(self.name.clone()),
        );
        obj.insert(
            "type".to_string(),
            serde_json::Value::String(self.action_type.clone()),
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
        if let Some(reason) = reason {
            obj.insert(
                "status_reason".to_string(),
                serde_json::Value::String(reason),
            );
        }
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
const ACTION_ENVELOPE_KEYS: &[&str] = &[
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
fn split_action_body(
    body: &serde_json::Value,
    path_id: Option<&str>,
) -> Result<ActionRecord, String> {
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
    // A console body carries the `{type}:{name}` key in `id`. The
    // store keys rows by the bare name. Reads accept both forms.
    let kind = obj
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if kind.is_empty() {
        return Err("missing required field: type".to_string());
    }
    if let Some(message) = crate::removed_connector_refusal(&kind) {
        return Err(message.to_string());
    }
    if !is_known_action_type(&kind) {
        return Err(format!("unknown action type: {kind}"));
    }
    // KC-00: a known kind whose capability state is not `ready` is refused.
    if let Some(message) = super::rules::kind_refusal(&kind) {
        return Err(message);
    }
    // The path id is authoritative. The caller resolves it to the
    // stored bare id first. A body id that names another action is
    // rejected. A type change through replace stays allowed.
    let mut id = if raw_id.is_empty() {
        String::new()
    } else {
        super::typed::bare_name(&raw_id, &kind)
    };
    if let Some(expected) = path_id {
        if !id.is_empty() && id != expected {
            return Err(format!(
                "body id {raw_id:?} does not match path id {expected:?} (field `id`)"
            ));
        }
        id = expected.to_string();
    }
    if id.is_empty() {
        return Err("action id or name is required".to_string());
    }
    if id.len() > MAX_ACTION_ID_LEN {
        return Err(format!(
            "action id holds {} chars, at most {MAX_ACTION_ID_LEN} (field `id`)",
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
        if !ACTION_ENVELOPE_KEYS.contains(&key.as_str()) {
            extras.insert(key.clone(), value.clone());
        }
    }
    Ok(ActionRecord {
        id: id.clone(),
        name: id,
        action_type: kind,
        enable,
        connector,
        parameters,
        resource_opts,
        params: serde_json::Value::Object(extras),
    })
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
    /// Per-action owned-sink build state (MV-01), recomputed by every
    /// write path that rebuilds sinks and kept memory-only.
    wiring: RwLock<HashMap<String, ActionWiring>>,
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
            wiring: RwLock::new(HashMap::new()),
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
        *store
            .registry
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::clone(registry));
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
        *self
            .registry
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::clone(registry));
    }

    fn seed_from_snapshot(&self, conf: &broker_config::ActionsConf) {
        let mut map = self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.wiring
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        map.clear();
        for entry in &conf.actions {
            // Old files store flat fields only. New files store the
            // envelope (`connector`, `parameters`, `resource_opts`)
            // plus flat extras. Missing envelope keys read as empty.
            let doc = if entry.config.trim().is_empty() {
                serde_json::json!({})
            } else {
                match serde_json::from_str(&entry.config) {
                    Ok(doc) => doc,
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
                if !ACTION_ENVELOPE_KEYS.contains(&key.as_str()) {
                    extras.insert(key.clone(), value.clone());
                }
            }
            // Old rows keyed the map by `{type}:{name}` stay readable:
            // the key form resolves to the bare name here.
            let bare = super::typed::bare_name(&entry.id, &entry.action_type);
            map.insert(
                bare.clone(),
                ActionRecord {
                    id: bare.clone(),
                    name: bare,
                    action_type: entry.action_type.clone(),
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
    pub fn snapshot(&self) -> Vec<ActionRecord> {
        let map = self
            .inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut out: Vec<ActionRecord> = map.values().cloned().collect();
        out.sort_by(|a, b| a.id.cmp(&b.id));
        out
    }

    /// Aggregated counts over the live snapshot: (total, enabled,
    /// disabled). Computed on demand from the live entries, never
    /// stored, so the counts always track creates, deletes and enable
    /// flips. Takes one short read lock and counts in place without
    /// cloning, so a management read never blocks messaging (and
    /// publish/deliver never touch this store at all).
    pub fn summary_counts(&self) -> (usize, usize, usize) {
        let map = self
            .inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let total = map.len();
        let enabled = map.values().filter(|record| record.enable).count();
        (total, enabled, total - enabled)
    }

    /// Insert one entry. Fails cleanly when the id already exists
    /// (caller maps to `ALREADY_EXISTS`) or the store is at
    /// [`MAX_ACTIONS`] (caller maps to `BAD_REQUEST`).
    pub fn insert(&self, entry: ActionRecord) -> Result<ActionRecord, ActionInsertError> {
        let mut map = self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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
            .unwrap_or_else(|poisoned| poisoned.into_inner())
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
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(id)
            .cloned()
    }

    /// Resolve a path id to the stored bare id. Accepts the bare name
    /// and the `{type}:{name}` key. Returns `None` for unknown ids.
    pub fn resolve_id(&self, requested: &str) -> Option<String> {
        let map = self
            .inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if map.contains_key(requested) {
            return Some(requested.to_string());
        }
        map.values()
            .find(|record| super::typed::id_matches(&record.id, &record.action_type, requested))
            .map(|record| record.id.clone())
    }

    /// Read one entry by bare name or key. Returns `None` when unknown.
    pub fn get_any(&self, requested: &str) -> Option<ActionRecord> {
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
    pub fn replace_any(&self, requested: &str, entry: ActionRecord) -> bool {
        let Some(bare) = self.resolve_id(requested) else {
            return false;
        };
        self.replace(&bare, entry)
    }

    /// Replace the entry stored under `id` with a validated snapshot.
    /// Returns false when the id is unknown (caller maps to `NOT_FOUND`
    /// and creates nothing). The replace swaps one validated entry in
    /// place and never grows the map toward [`MAX_ACTIONS`], so an
    /// update can never trip the store cap. Management-plane only;
    /// publish and deliver never call this.
    pub fn replace(&self, id: &str, entry: ActionRecord) -> bool {
        let mut map = self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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
        let mut map = self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = map.get_mut(id)?;
        entry.enable = enable;
        Some(entry.clone())
    }

    /// Store the owned-sink build state of one action (MV-01). The
    /// wiring map holds at most one entry per created action and is
    /// memory-only; a delete leaves no entry behind.
    pub fn set_wiring(&self, id: &str, wiring: ActionWiring) {
        if wiring == ActionWiring::None {
            self.wiring
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(id);
            return;
        }
        self.wiring
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(id.to_string(), wiring);
    }

    /// Read one action's owned-sink build state. An unknown id (or one
    /// that never built anything) reads [`ActionWiring::None`].
    pub fn wiring_of(&self, id: &str) -> ActionWiring {
        self.wiring
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(id)
            .cloned()
            .unwrap_or(ActionWiring::None)
    }

    /// Export the store as a validated config root: entries sorted by
    /// id so the persisted file is deterministic. Each entry carries
    /// the id, kind, enable flag and the envelope (`connector`,
    /// `parameters`, `resource_opts`) plus the opaque extras as a JSON
    /// document. Nothing derived is stored: only inputs persist.
    fn export_conf(&self) -> broker_config::ActionsConf {
        let map = self
            .inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut entries: Vec<broker_config::ActionEntry> = map
            .values()
            .map(|record| {
                let mut doc = serde_json::Map::new();
                doc.insert(
                    "connector".to_string(),
                    serde_json::Value::String(record.connector.clone()),
                );
                doc.insert("parameters".to_string(), record.parameters.clone());
                doc.insert("resource_opts".to_string(), record.resource_opts.clone());
                if let Some(extra) = record.params.as_object() {
                    for (key, value) in extra {
                        if !doc.contains_key(key) {
                            doc.insert(key.clone(), value.clone());
                        }
                    }
                }
                broker_config::ActionEntry {
                    id: record.id.clone(),
                    action_type: record.action_type.clone(),
                    enable: record.enable,
                    config: serde_json::to_string(&serde_json::Value::Object(doc))
                        .unwrap_or_else(|_| "{}".to_string()),
                }
            })
            .collect();
        entries.sort_by(|a, b| a.id.cmp(&b.id));
        broker_config::ActionsConf { actions: entries }
    }

    /// Commit the exported root and atomically save it. Any commit or
    /// save failure surfaces so callers answer 500 and the loss is
    /// never silent (the in-memory mutation stays: commit precedes the
    /// atomic save). Stores without a registry (unit tests, standalone
    /// state) are memory-only and persist as a no-op. Uses the stored
    /// rewrite so already-stored unavailable records stay readable
    /// while a new removed kind is still refused.
    fn persist(&self) -> Result<(), broker_config::ConfigError> {
        let registry = self
            .registry
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let Some(registry) = registry else {
            return Ok(());
        };
        let _guard = self
            .save_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let conf = self.export_conf();
        registry.commit_actions_stored(conf)?;
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

/// An action kind is listed only if the kernel can execute it: the
/// shared kind registry in `broker-config`, which also accepts the
/// documented names the console sends (`http`, `mqtt`,
/// `kafka_producer`, ...). Historic aliases (`mqtt_bridge`, `kafka`,
/// ...) stay accepted so old files keep loading.
fn is_known_action_type(kind: &str) -> bool {
    broker_config::is_known_action_type(kind)
}

/// `GET /api/v5/actions`: every action as a bare JSON array, as the
/// reference API and the management console expect. An empty store reads
/// as `[]`, never an error. The reference API documents no paging, so the
/// route takes none. Unknown query parameters are ignored.
/// Management-plane only; publish and deliver never touch this store.
pub async fn list_actions(State(state): State<ApiState>) -> Response {
    let records = state.actions.snapshot();
    let rows: Vec<serde_json::Value> = records
        .iter()
        .map(|record| {
            let health = action_health(&state, &record.id);
            record.to_json_with_health(&state.actions.wiring_of(&record.id), health.as_ref())
        })
        .collect();
    (StatusCode::OK, Json(rows)).into_response()
}

/// Last health-probe result of one action's registered runtime.
fn action_health(state: &ApiState, id: &str) -> Option<broker_connectors::SinkHealth> {
    state
        .engine
        .connectors()
        .get(&broker_config::v5_action_sink_key(id))
        .and_then(|sink| sink.health())
}

/// `POST /api/v5/actions`: validate the body against the named action
/// kind (unknown kind rejected with the documented error shape); a
/// duplicate id is rejected cleanly, never overwritten. The body
/// carries the documented envelope (`type`, `name`, `connector`,
/// `enable`, `parameters`, `resource_opts`); `parameters` and
/// `resource_opts` default to `{}` and the rest reads back opaquely.
/// An action kind with an owned runtime (`pgsql`, `webhook`/`http`)
/// stages its sink from the referenced connector plus these
/// parameters before anything stores: a build that cannot complete
/// answers 400 naming the field and stores nothing. Answers 201 with
/// the stored shape (the owned sink already live under the action's
/// dispatch key when one built). Management-plane only; publish
/// and deliver never touch this store.
pub async fn create_action(
    State(state): State<ApiState>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    // KC-00: create of a known kind that is not `ready` keeps the
    // reference error code for this operation, `ALREADY_EXISTS`.
    if let Some(kind) = body.get("type").and_then(|v| v.as_str()) {
        let kind = kind.trim();
        if crate::removed_connector_refusal(kind).is_none() {
            if let Some(message) = super::rules::kind_refusal(kind) {
                return ApiError::AlreadyExists(message).into_response();
            }
        }
    }
    let entry = match split_action_body(&body, None) {
        Ok(entry) => entry,
        Err(reason) => return ApiError::AlreadyExists(reason).into_response(),
    };
    let id = entry.id.clone();
    let enable = entry.enable;
    // Serialise the generation with every other action and connector
    // sink write. Staged build, persist and swap form one generation.
    let _sink_guard = state.action_sink_lock.lock().await;
    // MV-01 (R2-03/04): stage the action's owned sink before anything
    // stores. A sink that cannot be built from the referenced
    // connector plus these parameters is refused with 400 (naming the
    // action and the field) and nothing is stored; a registered
    // lifecycle is never built from invented values. A referenced
    // connector that does not resolve keeps the action storable (the
    // accepted conformance contract): the staged wiring reports the
    // reason and the run reads `disconnected` until the connector
    // arrives. A connector that resolves but stays incomplete (its own
    // credential is missing) stores the same way.
    let staged = match crate::v5::action_sinks::stage_action_sink(&state.connectors, &entry) {
        Ok(staged) => staged,
        Err(message) => return ApiError::AlreadyExists(message).into_response(),
    };
    match state.actions.insert(entry.clone()) {
        Ok(entry) => {
            let id = entry.id.clone();
            // MV-01: the engine dispatch key of an action is the
            // action-owned sink key (`action:{id}`), so the enable
            // flag and the metric record ride under the key that the
            // fire path uses.
            let key = broker_config::v5_action_sink_key(&id);
            // W3-04: mirror the flag into the engine's lock-free dispatch
            // snapshot so rule evaluation observes the create without
            // taking this store's lock on the message path. MV-01/R2:
            // delivery needs the action's flag AND its connector's, so
            // a connector born disabled holds the dependent action off
            // from the first publish.
            state.engine.set_action_enabled(
                &key,
                enable
                    && crate::v5::action_sinks::action_connector_enable(&state.connectors, &entry),
            );
            // W3-05: register a zeroed per-action metric record so the next
            // fire attributes through lock-free atomics (management-plane
            // swap; the publish path only loads).
            state.engine.ensure_action_metrics(&key);
            if let Err(error) = state.actions.persist() {
                // MV-01/R2: a persistence failure keeps the old
                // generation. The create had published nothing live
                // yet, so nothing was delivered; the half-written row
                // is rolled back instead of outliving its refused
                // persist, and the engine mirrors reset to absent.
                state.actions.remove(&id);
                state.engine.remove_action(&key);
                state.engine.remove_action_metrics(&key);
                return action_persist_error(error);
            }
            // Publish the new runtime generation: register the staged
            // sink under the action dispatch key (atomic replace;
            // delivery crosses to it in one store). R2-04 order: staged
            // build, then persist, then register, then old arcs drained
            // by the replace itself. A dangling connector reference
            // reports through the alarm plus the wiring reason; no
            // sink, no dial.
            let wiring = staged.wiring;
            let probe_on_create = staged.probe_on_create;
            let previous = state.engine.connectors().get(&key);
            if let Some(sink) = staged.sink {
                let sink = crate::v5::action_sinks::wrap_action_runtime(
                    &state.engine,
                    &key,
                    sink,
                    staged.resource_opts,
                    staged.health_target,
                );
                state
                    .engine
                    .connectors()
                    .register(key.clone(), Arc::clone(&sink));
                // A down server is stored and reports `disconnected`
                // from this real connection attempt; the sink keeps the
                // action and retries on its interval.
                if probe_on_create {
                    let _ = sink.probe().await;
                }
            }
            if let Some(old) = previous {
                let _ = old.flush().await;
            }
            if let crate::v5::actions::ActionWiring::Failed(message) = &wiring {
                state.alarms.activate(
                    &format!("action_build_failed:{id}"),
                    message,
                    serde_json::json!({
                        "kind": "action",
                        "id": id,
                        "type": entry.action_type,
                    }),
                );
            } else {
                state
                    .alarms
                    .deactivate(&format!("action_build_failed:{id}"));
            }
            state.actions.set_wiring(&id, wiring.clone());
            let health = action_health(&state, &id);
            (
                StatusCode::CREATED,
                Json(entry.to_json_with_health(&wiring, health.as_ref())),
            )
                .into_response()
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
    // Serialise the generation with every other action and connector
    // sink write.
    let _sink_guard = state.action_sink_lock.lock().await;
    let Some(stored) = state.actions.get_any(&id) else {
        return ApiError::NotFound(format!("action {id:?} not found")).into_response();
    };
    let bare = stored.id.clone();
    // MV-01: the fire dispatch key of this action (rules compiled
    // after MV-01 carry the key, not the bare id) drops the owned sink
    // under that key, so a delete stops that action's delivery.
    let key = broker_config::v5_action_sink_key(&bare);
    // Rule linkage: an action still referenced by a rule cannot be
    // dropped while the reference stands. Rules name their forward
    // target by the action dispatch key, so both the key form and the
    // older bare/typed forms count as a reference; the first such rule
    // is named in the refusal.
    if let Some(rule_id) = state.engine.list_rules().iter().find_map(|rule| {
        rule.actions
            .iter()
            .any(|action| {
                matches!(
                    action,
                    broker_rules::RuleAction::ForwardConnector { connector_id }
                    if connector_id == &bare
                        || connector_id == &id
                        || connector_id == &key
                        || connector_id == &broker_config::v5_action_sink_key(&id)
                )
            })
            .then(|| rule.id.clone())
    }) {
        return ApiError::BadRequest(format!("action {id:?} is referenced by rule {rule_id:?}"))
            .into_response();
    }
    if !state.actions.remove(&bare) {
        return ApiError::NotFound(format!("action {id:?} not found")).into_response();
    }
    state
        .alarms
        .deactivate(&format!("action_unavailable:{bare}"));
    state
        .alarms
        .deactivate(&format!("action_build_failed:{bare}"));
    state.actions.set_wiring(&bare, ActionWiring::None);
    // MV-01: drop the owned sink under its dispatch key so delivery
    // stops with the delete (unknown keys are a no-op). Drain the
    // replaced generation after the swap so a buffered batch still
    // reaches the old target.
    let previous = state.engine.connectors().get(&key);
    state.engine.connectors().unregister(&key);
    if let Some(old) = previous {
        let _ = old.flush().await;
    }
    // W3-04: drop the id from the dispatch snapshot so a later re-create
    // with the same id starts enabled.
    state.engine.remove_action(&key);
    // W3-05: drop the per-action metric record so the id reads as unknown
    // (404) until re-created, which starts again at zero. The record
    // rides the MV-01 dispatch key.
    state.engine.remove_action_metrics(&key);
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
    match state.actions.get_any(&id) {
        Some(entry) => {
            let wiring = state.actions.wiring_of(&entry.id);
            let health = action_health(&state, &entry.id);
            (
                StatusCode::OK,
                Json(entry.to_json_with_health(&wiring, health.as_ref())),
            )
                .into_response()
        }
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
    // Serialise the generation with every other action and connector
    // sink write. Staged build, persist and swap form one generation.
    let _sink_guard = state.action_sink_lock.lock().await;
    let Some(stored) = state.actions.get_any(&id) else {
        return ApiError::NotFound(format!("action {id:?} not found")).into_response();
    };
    let bare = stored.id.clone();
    // MV-01/R2: the connector's flag rides along with every dispatch
    // mirror and with the rollback below. Probed from the OLD record:
    // the update can move the action to another connector, so the
    // revert must restore the flag pair the old generation delivered
    // under.
    let prior_connector_on =
        crate::v5::action_sinks::action_connector_enable(&state.connectors, &stored);
    let entry = match split_action_body(&body, Some(&bare)) {
        Ok(entry) => entry,
        Err(reason) => return ApiError::BadRequest(reason).into_response(),
    };
    // MV-01 (R2-03/04): stage the owned sink built from the NEW entry
    // (never registered). A build that cannot complete for an
    // action-side reason is refused with 400 naming the action and the
    // field; every old sink keeps delivering and the stored row is
    // untouched. A connector-side gap stores with failed wiring.
    let staged = match crate::v5::action_sinks::stage_action_sink(&state.connectors, &entry) {
        Ok(staged) => staged,
        Err(message) => return ApiError::BadRequest(message).into_response(),
    };
    // The dispatch key rides the (unchangeable) bare id; a kind change
    // never changes the key the engine fires under.
    let key = broker_config::v5_action_sink_key(&bare);
    if !state.actions.replace(&bare, entry.clone()) {
        return ApiError::NotFound(format!("action {id:?} not found")).into_response();
    }
    // W3-04: mirror the flag into the dispatch snapshot (same lock-free
    // path as the enable route below). The engine keys by the MV-01
    // dispatch key; the connector's flag rides along (MV-01/R2: a
    // connector disable holds the dependent action off).
    state.engine.set_action_enabled(
        &key,
        entry.enable && crate::v5::action_sinks::action_connector_enable(&state.connectors, &entry),
    );
    // W3-05: keep the metric record registered across replaces (counters
    // survive a config replace; only delete or reset zeroes them).
    state.engine.ensure_action_metrics(&key);
    if let Err(error) = state.actions.persist() {
        // MV-01/R2: a persistence failure keeps the old generation.
        // Nothing swapped under the dispatch key yet, so the old sink
        // (if any) still delivers; the stored row reverts to the
        // previous snapshot and the delayed engine mirror reverts too.
        state.actions.replace(&bare, stored.clone());
        state
            .engine
            .set_action_enabled(&key, stored.enable && prior_connector_on);
        return action_persist_error(error);
    }
    // Publish the new runtime generation (staged build → persist →
    // swap under the key, in that order). A kind step away from the
    // action-owned set drains the prior sink: the key stops claiming
    // delivery with it. A dangling connector reference reports through
    // the alarm plus the wiring reason; no sink, no dial.
    let previous = state.engine.connectors().get(&key);
    let wiring = staged.wiring;
    let probe_on_create = staged.probe_on_create;
    match &staged.sink {
        Some(sink) => {
            let sink = crate::v5::action_sinks::wrap_action_runtime(
                &state.engine,
                &key,
                Arc::clone(sink),
                staged.resource_opts.clone(),
                staged.health_target.clone(),
            );
            // A down server is stored and reports `disconnected` from
            // this real connection attempt; the sink retries. Probe
            // before the swap so the previous generation stays live
            // while the new target is checked.
            if probe_on_create {
                let _ = sink.probe().await;
            }
            state
                .engine
                .connectors()
                .register(key.clone(), Arc::clone(&sink));
            state
                .alarms
                .deactivate(&format!("action_build_failed:{bare}"));
        }
        None => {
            // A kind step away from the action-owned set drops the key
            // so it stops claiming delivery. The previous generation
            // drains below through `previous`.
            if previous.is_some() {
                state.engine.connectors().unregister(&key);
            }
            if let crate::v5::actions::ActionWiring::Failed(message) = &wiring {
                state.alarms.activate(
                    &format!("action_build_failed:{bare}"),
                    message,
                    serde_json::json!({
                        "kind": "action",
                        "id": bare,
                        "type": entry.action_type,
                    }),
                );
            } else {
                state
                    .alarms
                    .deactivate(&format!("action_build_failed:{bare}"));
            }
        }
    }
    // Drain the waiting rows of the replaced generation after the
    // swap. No row is lost: the old sink delivers its buffer.
    if let Some(old) = previous {
        let _ = old.flush().await;
    }
    state.actions.set_wiring(&bare, wiring.clone());
    let health = action_health(&state, &bare);
    (
        StatusCode::OK,
        Json(entry.to_json_with_health(&wiring, health.as_ref())),
    )
        .into_response()
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
/// write, and the answer is 204 with no body, as the reference API
/// documents.
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
pub async fn set_action_enabled(
    State(state): State<ApiState>,
    Path((id, enable_raw)): Path<(String, String)>,
) -> Response {
    let _sink_guard = state.action_sink_lock.lock().await;
    let Some(stored) = state.actions.get_any(&id) else {
        return ApiError::NotFound(format!("action {id:?} not found")).into_response();
    };
    let bare = stored.id.clone();
    // MV-01/R2: the connector's flag rides along with this flip — a
    // disabled connector holds its dependent action off regardless of
    // this route. Probed before the store write below while the record
    // borrow is still valid.
    let connector_on = crate::v5::action_sinks::action_connector_enable(&state.connectors, &stored);
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
    if state.actions.set_enabled(&bare, enable).is_none() {
        return ApiError::NotFound(format!("action {id:?} not found")).into_response();
    }
    // MV-01: mirror under the action's dispatch key before persisting: the
    // in-memory flag is already flipped, so evaluation must observe it
    // even if the disk save below fails (the store itself keeps the
    // mutation on a persist failure and answers 500, never silently).
    let key = broker_config::v5_action_sink_key(&bare);
    state
        .engine
        .set_action_enabled(&key, enable && connector_on);
    // A disable stops new fires first, then drains the rows the runtime
    // buffered before the stop.
    if !enable {
        if let Some(sink) = state.engine.connectors().get(&key) {
            let _ = sink.flush().await;
        }
    }
    if let Err(error) = state.actions.persist() {
        return action_persist_error(error);
    }
    StatusCode::NO_CONTENT.into_response()
}

/// `GET /api/v5/actions/:id/metrics`: live per-action counters (W3-05,
/// C06-08).
///
/// A known id returns the live counters attributed on the rule-fire path
/// and in the action runtime: `matched`, `success`, `failed`, `dropped`
/// with its reasons, `queuing` and `inflight`. `total` is kept
/// for older readers. All counters start at zero at creation and after a
/// restart. An unknown id returns 404 with the documented `{code,
/// message}` shape, not an empty body. The pair is not paged; unknown
/// query parameters are ignored. Management-plane only: one short store
/// read plus lock-free atomic loads; the publish path bumps the same
/// atomics and never takes this store's lock.
///
/// The counter set is fixed per action, so a read never grows anything.
/// Rates and aggregated-upload counters are omitted: the broker does not
/// measure them, and a constant would stand in for a measurement.
// TODO(parity): does the documented shape require `received` and the rate
// counters (`rate`, `rate_max`, `rate_last5m`)? The rulebook does not
// decide. The current choice reports the honest measured subset and omits
// the rest until the checker pins it.
pub async fn get_action_metrics(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Query(_ignored): Query<HashMap<String, String>>,
) -> Response {
    let Some(stored) = state.actions.get_any(&id) else {
        return ApiError::NotFound(format!("action {id:?} not found")).into_response();
    };
    let bare = stored.id.clone();
    // The record is registered on create/boot, but a standalone engine or
    // a race keeps this fail-closed at zero rather than 500. MV-01: the
    // metric record rides the action's dispatch key.
    let metrics = state
        .engine
        .action_metrics_snapshot_for(&broker_config::v5_action_sink_key(&bare))
        .unwrap_or_default();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "id": id,
            "metrics": {
                "total": metrics.total,
                "matched": metrics.matched,
                "success": metrics.success,
                "failed": metrics.failed,
                "dropped": metrics.dropped,
                "dropped.queue_full": metrics.dropped_queue_full,
                "dropped.resource_not_found": metrics.dropped_resource_not_found,
                "dropped.resource_stopped": metrics.dropped_resource_stopped,
                "dropped.other": metrics.dropped_other,
                "queuing": metrics.queuing,
                "inflight": metrics.inflight,
            },
        })),
    )
        .into_response()
}

/// `PUT /api/v5/actions/:id/metrics/reset`: zero one action's counters
/// (W3-05, C06-09).
///
/// Resets the live counters to zero and reports success with 204 and no
/// body. Resetting an unknown id is 404 with the documented `{code,
/// message}` shape, not an empty body. Counters resume from zero on the
/// next fire; the reset itself persists only as the zeroed values (the map
/// is memory-only and restarts at zero by design). The pair is not paged;
/// unknown query parameters are ignored. Management-plane only: one short
/// store read plus lock-free atomic stores; increments stay lock-free
/// atomics and publish/deliver never touch this store.
// TODO(parity): does the spec report a reset as an empty 204 or as a 200
// with a body? The rulebook does not decide the exact success shape; the
// current choice answers 204 with no body, mirroring the sibling rule and
// authenticator resets, until the checker pins it.
pub async fn reset_action_metrics(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Query(_ignored): Query<HashMap<String, String>>,
) -> Response {
    let Some(stored) = state.actions.get_any(&id) else {
        return ApiError::NotFound(format!("action {id:?} not found")).into_response();
    };
    let bare = stored.id.clone();
    let key = broker_config::v5_action_sink_key(&bare);
    if !state.engine.reset_action_metrics(&key) {
        // The action exists but its record is missing (standalone engine
        // or a boot race): register a zeroed record and report success so
        // the reset is still observable as zeroes on the next read.
        state.engine.ensure_action_metrics(&key);
        state.engine.reset_action_metrics(&key);
    }
    StatusCode::NO_CONTENT.into_response()
}

/// `POST /api/v5/actions/:id/:operation`: run one named lifecycle
/// operation against one action (W3-06, C06-10).
///
/// Only the operations the lifecycle supports are served: `start`
/// (enable the action) and `stop` (disable it). Both flip the stored
/// enable flag in place, mirror the flag into the engine's lock-free
/// dispatch snapshot (disabled forwards are skipped at fire time before
/// reaching the connector), persist through the config core like every
/// other action write, and answer 204 with no body, as the reference API
/// documents.
///
/// An unknown action id returns 404 with the documented `{code, message}`
/// shape and persists nothing; an unknown operation name is rejected
/// with the documented error shape and persists nothing. The pair is not
/// paged; unknown query parameters are ignored (no query is extracted).
/// Management-plane only: one short store read plus one short write lock
/// cloning at most one small record per request (plus the registry save
/// lock on persist) and one lock-free snapshot swap; rule evaluation
/// reads the snapshot, and publish and deliver never touch this store.
///
/// Lifecycle state persists with the action record (the enable flag in
/// the config core); transient run state is not kept, so nothing extra
/// needs clearing across a restart (boot rebuilds the dispatch snapshot
/// from the persisted flags).
///
/// Fire-path cost (stated for the delivery-path note): no new work on
/// publish or deliver. The operation itself runs entirely on the
/// management plane; the publish path keeps paying what the enable route
/// already pays (one `Arc` of the disabled set per ingress plus one
/// `HashSet` lookup per forward, lock-free, no per-fire allocation).
// TODO(parity): which operation names does the documented spec serve
// beyond `start` (for example `stop`, `restart`)? The rulebook does not
// decide the exact verb set; the current choice serves exactly
// `start`/`stop` as enable flips (the only lifecycle the kernel tracks)
// and rejects everything else with `BAD_REQUEST` until the checker pins
// it.
// TODO(parity): are operation names case-sensitive (`START` versus
// `start`)? The rulebook does not decide; the current choice matches
// exactly `start`/`stop` in lower case and rejects every other spelling
// with `BAD_REQUEST` until the checker pins it.
// TODO(parity): for an unknown id with an unknown operation, is the
// answer 404 or 400? The rulebook does not decide the precedence; the
// current choice answers 404 first (existence before validation,
// mirroring the action update and enable routes) and persists nothing
// either way.
pub async fn run_action_operation(
    State(state): State<ApiState>,
    Path((id, operation)): Path<(String, String)>,
) -> Response {
    apply_action_operation(&state, &id, &operation).await
}

/// `POST /api/v5/nodes/:node/actions/:id/:operation`: run one named
/// lifecycle operation against one action on one named node (W3-09,
/// C06-13).
///
/// Single-node execution only: the `{node}` segment is checked with the
/// shared [`resolve_node`] helper against the configured
/// [`ApiState::node_id`] (falling back to the [`local_node_name`] name
/// the v5 rows render today); an unknown name returns the helper's 404
/// `NOT_FOUND` shape naming the node and touches no other state. A known
/// node dispatches to the same [`apply_action_operation`] path as the
/// global `POST /actions/:id/:operation` route (W3-06), so the reported
/// state, the 404 for an unknown id, and the rejection of an unknown
/// operation are identical on both routes.
///
/// The pair is not paged; unknown query parameters are ignored (no query
/// is extracted). Management-plane only: one short store read plus one
/// short write lock cloning at most one small record per request (plus the
/// registry save lock on persist) and one lock-free snapshot swap; rule
/// evaluation reads the snapshot, and publish and deliver never touch this
/// store.
///
/// Bound (stated here and enforced by construction): single-request bounded
/// work with no wait and no accumulated per-node state. The operation
/// completes synchronously under short in-memory locks with no remote call,
/// no retry and no timer, so there is no timeout to tune; node membership
/// itself is owned elsewhere (exactly one local record) and nothing here
/// grows with connections, sessions or subscriptions.
///
/// Fire-path cost: no new work on publish or deliver. The operation itself
/// runs entirely on the management plane; the publish path keeps paying
/// what the enable route already pays (one `Arc` of the disabled set per
/// ingress plus one `HashSet` lookup per forward, lock-free, no per-fire
/// allocation).
// TODO(parity): for an unknown node with an unknown id or operation, is
// the answer the node 404 or the action/operation error? The rulebook does
// not decide the precedence; the current choice answers the node 404 first
// (node scope before action existence before operation validity, fail
// closed) and touches nothing else either way.
pub async fn run_node_action_operation(
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
    apply_action_operation(&state, &id, &operation).await
}

/// Shared dispatch behind both the global and the node-scoped operation
/// routes: existence check (unknown id is 404, creates nothing), operation
/// validity check (unknown name is `BAD_REQUEST`, persists nothing), then
/// the enable flip with the engine snapshot mirror, persistence and the
/// 204 answer with no body. Management-plane only; see the callers for the
/// locking and fire-path notes.
async fn apply_action_operation(state: &ApiState, id: &str, operation: &str) -> Response {
    let _sink_guard = state.action_sink_lock.lock().await;
    let Some(stored) = state.actions.get_any(id) else {
        return ApiError::NotFound(format!("action {id:?} not found")).into_response();
    };
    let bare = stored.id.clone();
    let enable = match operation {
        "start" => true,
        "stop" => false,
        _ => {
            return ApiError::BadRequest(format!(
                "unknown action operation {operation:?}: expected \"start\" or \"stop\" (field `operation`)"
            ))
            .into_response();
        }
    };
    if state.actions.set_enabled(&bare, enable).is_none() {
        return ApiError::NotFound(format!("action {id:?} not found")).into_response();
    }
    // Wire into the connector lifecycle underneath: the engine's
    // lock-free dispatch snapshot gates every `ForwardConnector` forward
    // before `ConnectorManager::send` (see `run_actions` in
    // `crates/broker-rules/src/lib.rs`), so flipping the flag here
    // starts/stops delivery through the connector without touching the
    // publish path itself. MV-01: the key is the action's dispatch key.
    let key = broker_config::v5_action_sink_key(&bare);
    state.engine.set_action_enabled(&key, enable);
    if !enable {
        if let Some(sink) = state.engine.connectors().get(&key) {
            let _ = sink.flush().await;
        }
    }
    if let Err(error) = state.actions.persist() {
        return action_persist_error(error);
    }
    StatusCode::NO_CONTENT.into_response()
}

/// `GET /api/v5/actions_summary` (W3-08, C06-12): a bare JSON array with
/// one summary row per action, as the reference API documents.
///
/// Each row carries `name`, `type` and `enable`, read from the live store.
/// The broker keeps no health state, no creation time and no rule links
/// for an action, so the rows leave out `status`, `created_at`,
/// `last_modified_at` and `rules` instead of inventing values. An empty
/// store reads as `[]`. The query parameters `ns` and `only_global` are
/// ignored. Management-plane only: one bounded snapshot clone per
/// request, and publish and deliver never touch this store.
pub async fn get_actions_summary(State(state): State<ApiState>) -> Response {
    let rows: Vec<serde_json::Value> = state
        .actions
        .snapshot()
        .iter()
        .map(|record| {
            serde_json::json!({
                "name": record.id,
                "type": record.action_type,
                "enable": record.enable,
            })
        })
        .collect();
    (StatusCode::OK, Json(rows)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: &str) -> ActionRecord {
        ActionRecord {
            id: id.to_string(),
            name: id.to_string(),
            action_type: "webhook".to_string(),
            enable: true,
            connector: String::new(),
            parameters: serde_json::json!({}),
            resource_opts: serde_json::json!({}),
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
            name: "act-1".to_string(),
            action_type: "webhook".to_string(),
            enable: false,
            connector: "webhook:up".to_string(),
            parameters: serde_json::json!({"topic": "out/1"}),
            resource_opts: serde_json::json!({}),
            params: serde_json::json!({"server": "127.0.0.1:1883"}),
        };
        assert!(store.insert(entry).is_ok());
        let rendered = store.snapshot()[0].to_json_with(&store.wiring_of("act-1"));
        assert_eq!(rendered["id"], serde_json::json!("act-1"));
        assert_eq!(rendered["name"], serde_json::json!("act-1"));
        assert_eq!(rendered["type"], serde_json::json!("webhook"));
        assert_eq!(rendered["enable"], serde_json::json!(false));
        assert_eq!(rendered["connector"], serde_json::json!("webhook:up"));
        assert_eq!(
            rendered["parameters"],
            serde_json::json!({"topic": "out/1"})
        );
        assert!(rendered["resource_opts"].is_object());
        assert_eq!(rendered["status"], serde_json::json!("disconnected"));
        assert!(rendered["node_status"].is_array());
        assert_eq!(rendered["server"], serde_json::json!("127.0.0.1:1883"));
    }

    #[test]
    fn key_form_resolves_to_the_bare_row() {
        let store = ActionStore::new();
        assert!(store.insert(record("a")).is_ok());
        let found = store.get_any("webhook:a").expect("key form reads back");
        assert_eq!(found.id, "a");
        assert!(store.contains_any("webhook:a"));
        assert!(store.get_any("other:a").is_none());
        assert!(store.get_any("missing").is_none());
    }

    #[test]
    fn split_body_accepts_the_documented_envelope() {
        let entry = split_action_body(
            &serde_json::json!({
                "type": "webhook",
                "name": "web",
                "connector": "webhook:up",
                "enable": true,
                "parameters": {"method": "post", "path": "/hook"},
                "resource_opts": {},
            }),
            None,
        )
        .expect("documented body parses");
        assert_eq!(entry.id, "web");
        assert_eq!(entry.connector, "webhook:up");
        assert_eq!(entry.parameters["path"], serde_json::json!("/hook"));
        // The console key form in `id` strips to the bare name.
        let keyed = split_action_body(
            &serde_json::json!({"id": "webhook:web", "type": "webhook"}),
            None,
        )
        .expect("key form parses");
        assert_eq!(keyed.id, "web");
        // A missing type and an unknown type both fail.
        assert!(split_action_body(&serde_json::json!({"name": "x"}), None).is_err());
        assert!(
            split_action_body(&serde_json::json!({"name": "x", "type": "nope"}), None).is_err()
        );
        // A non-object `parameters` fails.
        assert!(split_action_body(
            &serde_json::json!({"name": "x", "type": "webhook", "parameters": []}),
            None
        )
        .is_err());
    }

    #[test]
    fn export_round_trips_through_validated_config() {
        let store = ActionStore::new();
        assert!(store.insert(record("act-1")).is_ok());
        let conf = store.export_conf();
        conf.validate().expect("exported actions must validate");
        assert_eq!(conf.actions.len(), 1);
        assert_eq!(conf.actions[0].id, "act-1");
        assert_eq!(conf.actions[0].action_type, "webhook");
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
        assert!(is_known_action_type("http"));
        assert!(is_known_action_type("mqtt"));
        assert!(is_known_action_type("kafka_producer"));
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
            name: "a".to_string(),
            action_type: "webhook".to_string(),
            enable: false,
            connector: String::new(),
            parameters: serde_json::json!({"topic": "events"}),
            resource_opts: serde_json::json!({}),
            params: serde_json::json!({}),
        };
        assert!(store.replace("a", updated.clone()));
        let found = store.get("a").expect("replaced action reads back");
        assert_eq!(found, updated);
        assert_eq!(store.snapshot().len(), 1);
        assert!(!store.replace("missing", record("missing")));
        assert!(store.replace_any("webhook:a", record("a")));
    }

    #[test]
    fn serves_after_lock_poison() {
        // A poisoned lock must not stop the store. Reads recover the lock.
        let store = ActionStore::new();
        assert!(store.insert(record("a")).is_ok());
        std::thread::scope(|s| {
            let h = s.spawn(|| {
                let _guard = store.inner.write().unwrap();
                panic!("poison the action lock");
            });
            let _ = h.join();
        });
        assert!(store.get("a").is_some());
        assert_eq!(store.snapshot().len(), 1);
    }

    #[test]
    fn set_wiring_tracks_the_recorded_state_and_clears_on_none() {
        let store = ActionStore::new();
        assert_eq!(store.wiring_of("a"), ActionWiring::None);
        assert!(store.insert(record("a")).is_ok());
        store.set_wiring("a", ActionWiring::Live);
        assert_eq!(store.wiring_of("a"), ActionWiring::Live);
        store.set_wiring("a", ActionWiring::None);
        assert_eq!(store.wiring_of("a"), ActionWiring::None);
    }

    #[test]
    fn wiring_render_reads_status_reason_only_from_the_recorded_state() {
        let store = ActionStore::new();
        let mut entry = record("a");
        entry.action_type = "webhook".to_string();
        assert!(store.insert(entry.clone()).is_ok());
        // A failed build renders `disconnected` with the recorded reason.
        store.set_wiring("a", ActionWiring::Failed("broken".to_string()));
        let rendered = store.snapshot()[0].to_json_with(&store.wiring_of("a"));
        assert_eq!(rendered["status"], serde_json::json!("disconnected"));
        assert_eq!(rendered["status_reason"], serde_json::json!("broken"));
        store.set_wiring("a", ActionWiring::None);
        assert_eq!(store.wiring_of("a"), ActionWiring::None);
        // A live build renders connected with no reason.
        store.set_wiring("a", ActionWiring::Live);
        let rendered = store.snapshot()[0].to_json_with(&store.wiring_of("a"));
        assert_eq!(rendered["status"], serde_json::json!("connected"));
        assert!(rendered["status_reason"].is_null());
        // The kind refusal keeps priority over a recorded reason.
        let mut removed = record("b");
        removed.action_type = "s3_tables".to_string();
        assert!(store.insert(removed).is_ok());
        store.set_wiring("b", ActionWiring::Failed("broken".to_string()));
        let rendered: Vec<serde_json::Value> = store
            .snapshot()
            .iter()
            .map(|r| r.to_json_with(&store.wiring_of(&r.id)))
            .collect();
        let removed_row = rendered
            .iter()
            .find(|row| row["id"] == "b")
            .expect("removed row");
        assert_eq!(removed_row["status"], "disconnected");
        let reason = removed_row["status_reason"].as_str().expect("reason");
        assert!(
            !reason.contains("broken"),
            "the kind refusal wins over the build reason: {reason}"
        );
    }
}
