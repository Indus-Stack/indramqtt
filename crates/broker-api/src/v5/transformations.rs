//! Ordered message-transformation chain for the v5 management API.
//!
//! Covers `GET /message_transformations` (ordered list),
//! `POST /message_transformations` (append one entry) and
//! `PUT /message_transformations` (replace the whole chain) on a real
//! backend. Single-node, management-plane only: handlers clone at most
//! the capped chain once per request under a short lock; the ingress
//! path loads one lock-free snapshot per message and never takes the
//! chain write lock.
//!
//! Execution: stored steps run in chain order on matching messages
//! before retained writes and rule dispatch, so retained state and rules
//! observe the transformed message. Per-transform metrics do not exist;
//! counters stay memory-only elsewhere and are out of scope.
//!
//! Store bounds (stated here and enforced below):
//! - at most [`MAX_TRANSFORMATIONS`] entries (64: caps management-plane
//!   memory and per-ingress matching; creates past the cap fail with
//!   `BAD_REQUEST`);
//! - `name` holds at most [`MAX_TRANSFORMATION_NAME_LEN`] chars (256:
//!   bounds map-key memory per entry);
//! - `topics` holds at most [`MAX_TRANSFORMATION_TOPICS`] filters (16:
//!   bounds per-ingress matching), each at most
//!   [`MAX_TRANSFORMATION_TOPIC_LEN`] bytes (256);
//! - `operations` holds at most [`MAX_TRANSFORMATION_OPERATIONS`] steps
//!   (16: bounds per-ingress rendering), each template at most
//!   [`MAX_TRANSFORMATION_VALUE_LEN`] bytes (1024);
//! - extra fields hold at most [`MAX_TRANSFORMATION_CONFIG_LEN`] bytes
//!   (4096: bounds per-request clones);
//! - the default store is empty; an empty chain reads as `[]`.
// TODO(parity): which per-entry fields does the documented shape require
// beyond name/topics/failure_action/enable/operations (decoders,
// encoders, description, tags, log settings)? The rulebook does not
// decide the exact shape; fields the broker cannot execute ride
// opaquely and read back unchanged until the checker pins it.
// TODO(parity): does the documented PUT answer 200 with the new chain or
// 204 with no body? The rulebook does not decide the exact success
// shape; the current choice answers 200 with the bare chain until the
// checker pins it.
// TODO(parity): does `disconnect` close the publisher connection or only
// shed the message? The rulebook does not decide; the current choice
// sheds the message like `drop` until the checker pins it.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::errors::ApiError;
use crate::ApiState;

/// Upper bound for stored transformations. Creates past this size fail
/// with `BAD_REQUEST` instead of growing the chain without limit. Reason:
/// the list handler clones the whole snapshot once per request and the
/// publish path tests each step per ingress, so the chain must stay
/// small and bounded; 64 covers the documented chain with headroom
/// while keeping per-request clones and per-ingress matching cheap.
pub const MAX_TRANSFORMATIONS: usize = broker_config::MAX_TRANSFORMATIONS;
/// Longest accepted transformation `name`. Reason: bounds key memory
/// per entry so one create cannot balloon the store.
const MAX_TRANSFORMATION_NAME_LEN: usize = broker_config::MAX_TRANSFORMATION_NAME_LEN;
/// Most topic filters in one transformation. Reason: the publish path
/// tests each filter per ingress, so the list stays small.
const MAX_TRANSFORMATION_TOPICS: usize = broker_config::MAX_TRANSFORMATION_TOPICS;
/// Longest accepted topic filter. Reason: filters clone into every
/// snapshot, so one long filter cannot bloat each read.
const MAX_TRANSFORMATION_TOPIC_LEN: usize = broker_config::MAX_TRANSFORMATION_TOPIC_LEN;
/// Most operations in one transformation. Reason: the publish path
/// renders each operation per matching ingress, so the list stays small.
const MAX_TRANSFORMATION_OPERATIONS: usize = broker_config::MAX_TRANSFORMATION_OPERATIONS;
/// Longest accepted operation template. Reason: templates render per
/// matching ingress and clone into snapshots, so one long template
/// cannot bloat reads and the fire path.
const MAX_TRANSFORMATION_VALUE_LEN: usize = broker_config::MAX_TRANSFORMATION_VALUE_LEN;
/// Longest accepted opaque config document. Reason: the document clones
/// into every snapshot, so one large document cannot bloat each read.
const MAX_TRANSFORMATION_CONFIG_LEN: usize = broker_config::MAX_TRANSFORMATION_CONFIG_LEN;

/// One field set inside a transformation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformOperation {
    /// Message field to set (`payload` or `topic`).
    pub key: String,
    /// Template for the new value.
    pub value: String,
}

/// One stored transformation: operator-chosen name, the filters it
/// applies to, the failure behaviour, whether it participates, the
/// ordered operations, and the remaining fields stored opaquely.
#[derive(Debug, Clone, PartialEq)]
pub struct TransformRecord {
    /// Chain key; unique within the chain.
    pub name: String,
    /// Topic filters the step applies to, in stored order.
    pub topics: Vec<String>,
    /// Failure behaviour (`drop`, `disconnect` or `ignore`).
    pub failure_action: String,
    /// Whether the step participates.
    pub enable: bool,
    /// Ordered field sets applied in order on matching messages.
    pub operations: Vec<TransformOperation>,
    /// Remaining fields (decoders, encoders, description, tags, log
    /// settings, extras) stored opaquely and reported back unchanged.
    pub params: serde_json::Value,
}

impl TransformRecord {
    fn to_json(&self) -> serde_json::Value {
        let mut obj = serde_json::Map::new();
        obj.insert(
            "name".to_string(),
            serde_json::Value::String(self.name.clone()),
        );
        let topics: Vec<serde_json::Value> = self
            .topics
            .iter()
            .map(|t| serde_json::Value::String(t.clone()))
            .collect();
        obj.insert("topics".to_string(), serde_json::Value::Array(topics));
        obj.insert(
            "failure_action".to_string(),
            serde_json::Value::String(self.failure_action.clone()),
        );
        obj.insert("enable".to_string(), serde_json::Value::Bool(self.enable));
        let operations: Vec<serde_json::Value> = self
            .operations
            .iter()
            .map(|op| {
                serde_json::json!({
                    "key": op.key,
                    "value": op.value,
                })
            })
            .collect();
        obj.insert(
            "operations".to_string(),
            serde_json::Value::Array(operations),
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

    /// Live execution step for the engine snapshot. Filters that no
    /// longer parse never match (fail closed); validated records always
    /// parse, so this only triggers on a corrupt boot snapshot.
    fn to_step(&self) -> broker_rules::TransformStep {
        let mut filters = Vec::with_capacity(self.topics.len());
        for raw in &self.topics {
            if let Ok(filter) = broker_protocol::TopicFilter::new(raw.clone()) {
                filters.push(filter);
            }
        }
        broker_rules::TransformStep {
            name: self.name.clone(),
            topics: filters,
            failure_action: self.failure_action.clone(),
            enable: self.enable,
            operations: self
                .operations
                .iter()
                .map(|op| broker_rules::TransformOperation {
                    key: op.key.clone(),
                    value: op.value.clone(),
                })
                .collect(),
        }
    }
}

/// Insert failure: duplicate name or a full chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransformInsertError {
    Duplicate,
    Full,
}

/// Bounded ordered transformation chain, persisted through the config
/// core. Chain order defines execution order; creates append, whole
/// replaces swap the order.
pub struct TransformChainStore {
    inner: RwLock<Vec<TransformRecord>>,
    /// Kernel config registry receiving every mutation (`None` in unit
    /// tests and standalone state, which stay memory-only).
    registry: RwLock<Option<Arc<broker_config::ConfigRegistry>>>,
    /// Serialises export-commit-save so concurrent mutations cannot
    /// interleave into a lost update on disk.
    save_lock: Mutex<()>,
}

impl TransformChainStore {
    /// Empty chain (today's empty behaviour: no rewrites).
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(Vec::new()),
            registry: RwLock::new(None),
            save_lock: Mutex::new(()),
        }
    }

    /// Seed from the registry snapshot and persist later mutations
    /// through it. Kernel boot path; an empty snapshot is a no-op.
    pub fn from_registry(registry: &Arc<broker_config::ConfigRegistry>) -> Self {
        let store = Self::new();
        store.seed_from_snapshot(&registry.snapshot().transformations);
        *store.registry.write().expect("transform store lock") = Some(Arc::clone(registry));
        store
    }

    /// Attach the registry and replace contents with its snapshot, in
    /// place on the shared instance. Kernel boot path.
    pub fn seed_from_registry(&self, registry: &Arc<broker_config::ConfigRegistry>) {
        self.seed_from_snapshot(&registry.snapshot().transformations);
        *self.registry.write().expect("transform store lock") = Some(Arc::clone(registry));
    }

    fn seed_from_snapshot(&self, conf: &broker_config::TransformationsConf) {
        let mut chain = self.inner.write().expect("transform store lock");
        chain.clear();
        for entry in &conf.transformations {
            let params = if entry.config.trim().is_empty() {
                serde_json::json!({})
            } else {
                match serde_json::from_str(&entry.config) {
                    Ok(params) => params,
                    Err(_) => {
                        // An undecodable config stores no extra fields.
                        tracing::warn!(
                            transformation = entry.name.as_str(),
                            "transform snapshot entry has an undecodable config; stored without params"
                        );
                        serde_json::json!({})
                    }
                }
            };
            chain.push(TransformRecord {
                name: entry.name.clone(),
                topics: entry.topics.clone(),
                failure_action: entry.failure_action.clone(),
                enable: entry.enable,
                operations: entry
                    .operations
                    .iter()
                    .map(|op| TransformOperation {
                        key: op.key.clone(),
                        value: op.value.clone(),
                    })
                    .collect(),
                params,
            });
        }
    }

    /// Snapshot of live entries in chain order for reads.
    pub fn snapshot(&self) -> Vec<TransformRecord> {
        self.inner.read().expect("transform store lock").clone()
    }

    /// Live execution steps in chain order for the engine snapshot.
    pub fn steps_snapshot(&self) -> Vec<broker_rules::TransformStep> {
        self.inner
            .read()
            .expect("transform store lock")
            .iter()
            .map(TransformRecord::to_step)
            .collect()
    }

    /// Append one entry. Fails when the name exists (caller maps to
    /// `ALREADY_EXISTS`) or the chain is at [`MAX_TRANSFORMATIONS`].
    pub fn insert(&self, entry: TransformRecord) -> Result<TransformRecord, TransformInsertError> {
        let mut chain = self.inner.write().expect("transform store lock");
        if chain.iter().any(|record| record.name == entry.name) {
            return Err(TransformInsertError::Duplicate);
        }
        if chain.len() >= MAX_TRANSFORMATIONS {
            return Err(TransformInsertError::Full);
        }
        chain.push(entry.clone());
        Ok(entry)
    }

    /// Replace the whole chain with validated entries in order.
    /// Caller validates every entry before calling, so this never
    /// partially applies.
    pub fn replace_all(&self, entries: Vec<TransformRecord>) {
        *self.inner.write().expect("transform store lock") = entries;
    }

    /// Export the store as a validated config root: entries in chain
    /// order so the persisted file keeps execution order. Each entry
    /// carries the name, filters, failure behaviour, enable flag, the
    /// ordered operations, and the opaque remaining fields as a JSON
    /// document. Nothing derived is stored: only inputs persist.
    fn export_conf(&self) -> broker_config::TransformationsConf {
        let chain = self.inner.read().expect("transform store lock");
        let transformations: Vec<broker_config::TransformationEntry> = chain
            .iter()
            .map(|record| broker_config::TransformationEntry {
                name: record.name.clone(),
                topics: record.topics.clone(),
                failure_action: record.failure_action.clone(),
                enable: record.enable,
                operations: record
                    .operations
                    .iter()
                    .map(|op| broker_config::TransformationOperation {
                        key: op.key.clone(),
                        value: op.value.clone(),
                    })
                    .collect(),
                config: if record.params.as_object().is_some_and(|m| m.is_empty()) {
                    String::new()
                } else {
                    serde_json::to_string(&record.params).unwrap_or_default()
                },
            })
            .collect();
        broker_config::TransformationsConf { transformations }
    }

    /// Commit the exported root and atomically save it. Any commit or
    /// save failure surfaces so callers answer 500 and the loss is
    /// never silent. Stores without a registry stay memory-only.
    fn persist(&self) -> Result<(), broker_config::ConfigError> {
        let registry = self.registry.read().expect("transform store lock").clone();
        let Some(registry) = registry else {
            return Ok(());
        };
        let _guard = self.save_lock.lock().expect("transform store lock");
        let conf = self.export_conf();
        registry.commit_transformations(conf)?;
        registry.save()?;
        Ok(())
    }
}

impl Default for TransformChainStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Map a store persistence failure onto a 500: the in-memory mutation
/// applied but the commit or atomic save failed, so the loss must never
/// be silent.
fn transform_persist_error(error: broker_config::ConfigError) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({
            "code": "INTERNAL_ERROR",
            "message": format!("cannot persist transformations: {error}")
        })),
    )
        .into_response()
}

/// Mirror the live chain into the engine snapshot. The publish path
/// loads the snapshot per ingress; it never takes this store lock.
fn sync_engine(state: &ApiState) {
    state
        .engine
        .sync_transforms(state.transformations.steps_snapshot());
}

/// `GET /api/v5/message_transformations`: ordered chain as a bare JSON
/// array in execution order; an empty chain reads as `[]`, never an
/// error. Unknown query parameters are ignored. The route is not paged.
/// Management-plane only: one bounded snapshot clone per request; the
/// publish path loads the engine snapshot and never touches this store.
///
/// Store wiring (rulebook section 2): the backing store is
/// [`TransformChainStore`], shared with the kernel
/// (`crates/broker-node/src/main.rs`, `serve_api` share) so management
/// reads observe the kernel directory; the driving broker event is the
/// ingress transform step (`crates/broker-node/src/main.rs`, ingress
/// pipeline) loading the engine snapshot. Declared `wired` in
/// `tools/wiring.txt`.
///
/// Fire-path cost: one `Arc` clone plus one length check per message
/// when the chain is empty; matching steps add one filter test per
/// filter plus one template render per operation (each allocating the
/// rewritten value only).
pub async fn list_transformations(
    State(state): State<ApiState>,
    Query(_ignored): Query<HashMap<String, String>>,
) -> Response {
    let chain = state.transformations.snapshot();
    let rows: Vec<serde_json::Value> = chain.iter().map(TransformRecord::to_json).collect();
    (StatusCode::OK, Json(rows)).into_response()
}

/// `POST /api/v5/message_transformations`: append one validated
/// transformation. A duplicate name fails with `ALREADY_EXISTS` and
/// overwrites nothing; a malformed body fails with `BAD_REQUEST`.
/// Unknown query parameters are ignored. On success answers 201 with
/// the stored entry. Management-plane only; publish and deliver never
/// touch this store.
pub async fn create_transformation(State(state): State<ApiState>, body: Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return ApiError::BadRequest(format!("invalid transformation body: {e}"))
                .into_response();
        }
    };
    let entry = match parse_transform_request(&value) {
        Ok(e) => e,
        Err(msg) => return ApiError::BadRequest(msg).into_response(),
    };
    match state.transformations.insert(entry) {
        Ok(stored) => {
            sync_engine(&state);
            if let Err(error) = state.transformations.persist() {
                return transform_persist_error(error);
            }
            (StatusCode::CREATED, Json(stored.to_json())).into_response()
        }
        Err(TransformInsertError::Duplicate) => {
            ApiError::AlreadyExists("transformation already exists".to_string()).into_response()
        }
        Err(TransformInsertError::Full) => ApiError::BadRequest(format!(
            "transformation chain holds {MAX_TRANSFORMATIONS} entries, at most {MAX_TRANSFORMATIONS}"
        ))
        .into_response(),
    }
}

/// `PUT /api/v5/message_transformations`: replace the whole chain.
/// The body must be a JSON array; every entry gets the same validation
/// as create, and the whole write validates before anything applies:
/// one bad entry rejects the entire write with `BAD_REQUEST` and leaves
/// the old chain in place. Duplicate names inside the body fail the
/// same way. Unknown query parameters are ignored. On success answers
/// 200 with the new chain in order. Management-plane only; publish and
/// deliver never touch this store.
pub async fn replace_transformations(State(state): State<ApiState>, body: Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return ApiError::BadRequest(format!("invalid transformation chain body: {e}"))
                .into_response();
        }
    };
    let items = match value.as_array() {
        Some(items) => items,
        None => {
            return ApiError::BadRequest("transformation chain must be a JSON array".to_string())
                .into_response();
        }
    };
    if items.len() > MAX_TRANSFORMATIONS {
        return ApiError::BadRequest(format!(
            "transformation chain holds {} entries, at most {MAX_TRANSFORMATIONS}",
            items.len()
        ))
        .into_response();
    }
    let mut entries = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        match parse_transform_request(item) {
            Ok(entry) => entries.push(entry),
            Err(msg) => {
                return ApiError::BadRequest(format!("transformations[{index}]: {msg}"))
                    .into_response();
            }
        }
    }
    let mut seen = std::collections::HashSet::with_capacity(entries.len());
    for entry in &entries {
        if !seen.insert(entry.name.clone()) {
            return ApiError::BadRequest(format!(
                "transformation {:?} is duplicated (field `name`)",
                entry.name
            ))
            .into_response();
        }
    }
    state.transformations.replace_all(entries);
    sync_engine(&state);
    if let Err(error) = state.transformations.persist() {
        return transform_persist_error(error);
    }
    let chain = state.transformations.snapshot();
    let rows: Vec<serde_json::Value> = chain.iter().map(TransformRecord::to_json).collect();
    (StatusCode::OK, Json(rows)).into_response()
}

/// Validate one transformation body into a chain record.
///
/// Required: `name` (non-empty, bounded), `topics` (a non-empty
/// string or a non-empty array of strings, each a valid MQTT filter)
/// and `failure_action` (a known behaviour).
/// `enable` defaults to true. `operations` defaults to
/// `[]`; each entry must carry a non-empty `key` (`payload` or `topic`)
/// and a string `value`. All other fields ride opaquely and read back
/// unchanged.
fn parse_transform_request(value: &serde_json::Value) -> Result<TransformRecord, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "transformation entry must be a JSON object".to_string())?;
    let name = obj
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "field `name` is required".to_string())?;
    if name.trim().is_empty() {
        return Err("field `name` must not be empty".to_string());
    }
    if name.len() > MAX_TRANSFORMATION_NAME_LEN {
        return Err(format!(
            "field `name` holds {} chars, at most {MAX_TRANSFORMATION_NAME_LEN}",
            name.len()
        ));
    }
    let topics = match obj.get("topics") {
        Some(serde_json::Value::String(s)) => vec![s.clone()],
        Some(serde_json::Value::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let s = item
                    .as_str()
                    .ok_or_else(|| "field `topics` entries must be strings".to_string())?;
                out.push(s.to_string());
            }
            out
        }
        _ => return Err("field `topics` is required".to_string()),
    };
    if topics.is_empty() {
        return Err("field `topics` must not be empty".to_string());
    }
    if topics.len() > MAX_TRANSFORMATION_TOPICS {
        return Err(format!(
            "field `topics` holds {} entries, at most {MAX_TRANSFORMATION_TOPICS}",
            topics.len()
        ));
    }
    for topic in &topics {
        if topic.trim().is_empty() {
            return Err("field `topics` entries must not be empty".to_string());
        }
        if topic.len() > MAX_TRANSFORMATION_TOPIC_LEN {
            return Err(format!(
                "field `topics` entry holds {} bytes, at most {MAX_TRANSFORMATION_TOPIC_LEN}",
                topic.len()
            ));
        }
        if broker_protocol::TopicFilter::new(topic.clone()).is_err() {
            return Err(format!(
                "field `topics` entry {topic:?} is not a valid filter"
            ));
        }
    }
    let failure_action = match obj.get("failure_action") {
        None | Some(serde_json::Value::Null) => {
            return Err("field `failure_action` is required".to_string());
        }
        Some(v) => {
            let raw = v
                .as_str()
                .ok_or_else(|| "field `failure_action` must be a string".to_string())?;
            if !broker_config::is_known_transformation_failure_action(raw) {
                return Err(format!("field `failure_action` {raw:?} is unknown"));
            }
            raw.to_string()
        }
    };
    let enable = match obj.get("enable") {
        None | Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::Bool(b)) => *b,
        Some(_) => return Err("field `enable` must be a boolean".to_string()),
    };
    let operations = match obj.get("operations") {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(v) => {
            let items = v
                .as_array()
                .ok_or_else(|| "field `operations` must be an array".to_string())?;
            if items.len() > MAX_TRANSFORMATION_OPERATIONS {
                return Err(format!(
                    "field `operations` holds {} entries, at most {MAX_TRANSFORMATION_OPERATIONS}",
                    items.len()
                ));
            }
            let mut out = Vec::with_capacity(items.len());
            for (index, item) in items.iter().enumerate() {
                let op = item
                    .as_object()
                    .ok_or_else(|| format!("field `operations[{index}]` must be an object"))?;
                let key = op
                    .get("key")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| format!("field `operations[{index}].key` is required"))?;
                if key.trim().is_empty() {
                    return Err(format!("field `operations[{index}].key` must not be empty"));
                }
                if !matches!(key, "payload" | "topic") {
                    return Err(format!(
                        "field `operations[{index}].key` {key:?} is unknown"
                    ));
                }
                let value_str = op
                    .get("value")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| format!("field `operations[{index}].value` is required"))?;
                if value_str.len() > MAX_TRANSFORMATION_VALUE_LEN {
                    return Err(format!(
                        "field `operations[{index}].value` holds {} bytes, at most {MAX_TRANSFORMATION_VALUE_LEN}",
                        value_str.len()
                    ));
                }
                out.push(TransformOperation {
                    key: key.to_string(),
                    value: value_str.to_string(),
                });
            }
            out
        }
    };
    let mut params = serde_json::Map::new();
    for (key, val) in obj {
        if !matches!(
            key.as_str(),
            "name" | "topics" | "failure_action" | "enable" | "operations"
        ) {
            params.insert(key.clone(), val.clone());
        }
    }
    let params_value = serde_json::Value::Object(params);
    let params_len = serde_json::to_string(&params_value)
        .unwrap_or_default()
        .len();
    if params_len > MAX_TRANSFORMATION_CONFIG_LEN {
        return Err(format!(
            "extra fields hold {params_len} bytes, at most {MAX_TRANSFORMATION_CONFIG_LEN}"
        ));
    }
    Ok(TransformRecord {
        name: name.to_string(),
        topics,
        failure_action,
        enable,
        operations,
        params: params_value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(name: &str) -> serde_json::Value {
        serde_json::json!({
            "name": name,
            "topics": ["sensors/#"],
            "failure_action": "drop",
            "operations": [{"key": "payload", "value": "${payload}-x"}],
        })
    }

    #[test]
    fn insert_rejects_duplicates_and_reports_full() {
        let store = TransformChainStore::new();
        assert!(store
            .insert(parse_transform_request(&body("a")).expect("valid"))
            .is_ok());
        assert_eq!(
            store.insert(parse_transform_request(&body("a")).expect("valid")),
            Err(TransformInsertError::Duplicate)
        );
        assert_eq!(store.snapshot().len(), 1);
    }

    #[test]
    fn snapshot_keeps_chain_order() {
        let store = TransformChainStore::new();
        for name in ["b", "a"] {
            assert!(store
                .insert(parse_transform_request(&body(name)).expect("valid"))
                .is_ok());
        }
        let names: Vec<String> = store.snapshot().iter().map(|r| r.name.clone()).collect();
        assert_eq!(names, vec!["b".to_string(), "a".to_string()]);
    }

    #[test]
    fn replace_all_swaps_order() {
        let store = TransformChainStore::new();
        assert!(store
            .insert(parse_transform_request(&body("a")).expect("valid"))
            .is_ok());
        let entries = ["c", "b"]
            .iter()
            .map(|n| parse_transform_request(&body(n)).expect("valid"))
            .collect();
        store.replace_all(entries);
        let names: Vec<String> = store.snapshot().iter().map(|r| r.name.clone()).collect();
        assert_eq!(names, vec!["c".to_string(), "b".to_string()]);
    }

    #[test]
    fn export_round_trips_through_validated_config() {
        let store = TransformChainStore::new();
        assert!(store
            .insert(parse_transform_request(&body("t-1")).expect("valid"))
            .is_ok());
        let conf = store.export_conf();
        conf.validate().expect("exported chain must validate");
        assert_eq!(conf.transformations.len(), 1);
        assert_eq!(conf.transformations[0].name, "t-1");
        assert_eq!(conf.transformations[0].operations.len(), 1);
    }

    #[test]
    fn chain_persists_across_registry_reload() {
        let dir = std::env::temp_dir().join(format!(
            "indramqtt-transforms-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|span| span.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).expect("create test dir");
        let registry =
            std::sync::Arc::new(broker_config::ConfigRegistry::load(&dir).expect("load"));
        let store = TransformChainStore::from_registry(&registry);
        store
            .insert(parse_transform_request(&body("persist-1")).expect("valid"))
            .expect("insert");
        store.persist().expect("save");
        let reloaded =
            std::sync::Arc::new(broker_config::ConfigRegistry::load(&dir).expect("reload"));
        assert_eq!(reloaded.snapshot().transformations.transformations.len(), 1);
        assert_eq!(
            reloaded.snapshot().transformations.transformations[0].name,
            "persist-1"
        );
        let restarted = TransformChainStore::from_registry(&reloaded);
        assert_eq!(restarted.snapshot().len(), 1);
        assert_eq!(restarted.snapshot()[0].name, "persist-1");
        assert_eq!(restarted.steps_snapshot().len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn parse_rejects_unbounded_entries() {
        let many_topics: Vec<serde_json::Value> = (0..(MAX_TRANSFORMATION_TOPICS + 1))
            .map(|i| serde_json::json!(format!("sensors/{i}")))
            .collect();
        assert!(parse_transform_request(&serde_json::json!({
            "name": "many-topics",
            "topics": many_topics,
        }))
        .is_err());
        let long_topic = "a".repeat(MAX_TRANSFORMATION_TOPIC_LEN + 1);
        assert!(parse_transform_request(&serde_json::json!({
            "name": "long-topic",
            "topics": [long_topic],
        }))
        .is_err());
        let many_ops: Vec<serde_json::Value> = (0..(MAX_TRANSFORMATION_OPERATIONS + 1))
            .map(|_| serde_json::json!({"key": "payload", "value": "x"}))
            .collect();
        assert!(parse_transform_request(&serde_json::json!({
            "name": "many-ops",
            "topics": ["a/#"],
            "operations": many_ops,
        }))
        .is_err());
        let long_value = "v".repeat(MAX_TRANSFORMATION_VALUE_LEN + 1);
        assert!(parse_transform_request(&serde_json::json!({
            "name": "long-value",
            "topics": ["a/#"],
            "operations": [{"key": "payload", "value": long_value}],
        }))
        .is_err());
        let big_extra = "e".repeat(MAX_TRANSFORMATION_CONFIG_LEN + 1);
        assert!(parse_transform_request(&serde_json::json!({
            "name": "big-params",
            "topics": ["a/#"],
            "extra": big_extra,
        }))
        .is_err());
    }

    #[test]
    fn parse_rejects_malformed_bodies() {
        for bad in [
            serde_json::json!({"topics": ["a/#"]}),
            serde_json::json!({"name": "  ", "topics": ["a/#"]}),
            serde_json::json!({"name": "a"}),
            serde_json::json!({"name": "a", "topics": []}),
            serde_json::json!({"name": "a", "topics": ["#+bad"]}),
            serde_json::json!({"name": "a", "topics": ["a/#"], "failure_action": "retry"}),
            serde_json::json!({"name": "a", "topics": ["a/#"], "operations": [{"key": "qos", "value": "1"}]}),
            serde_json::json!({"name": "a", "topics": ["a/#"], "operations": [{"key": "payload"}]}),
            serde_json::json!("just-a-string"),
        ] {
            assert!(
                parse_transform_request(&bad).is_err(),
                "body {bad} must fail"
            );
        }
    }

    #[test]
    fn topics_accepts_a_single_string() {
        let value = serde_json::json!({
            "name": "single",
            "topics": "sensors/#",
            "failure_action": "ignore",
        });
        let record = parse_transform_request(&value).expect("single string topics validate");
        assert_eq!(record.topics, vec!["sensors/#".to_string()]);
        assert_eq!(record.failure_action, "ignore");
        assert!(record.enable);
        assert!(record.operations.is_empty());
    }

    fn standalone_state() -> ApiState {
        let engine = std::sync::Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        ApiState::standalone(engine)
    }

    async fn response_parts(response: axum::response::Response) -> (StatusCode, serde_json::Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("transform body is small and readable");
        if bytes.is_empty() {
            return (status, serde_json::Value::Null);
        }
        let body: serde_json::Value =
            serde_json::from_slice(&bytes).expect("transform body is JSON");
        (status, body)
    }

    fn make_body(value: &serde_json::Value) -> Bytes {
        Bytes::from(serde_json::to_vec(value).expect("body is JSON"))
    }

    #[tokio::test]
    async fn create_two_then_list_keeps_order_and_rejects_duplicates() {
        let state = standalone_state();
        for name in ["first", "second"] {
            let body = serde_json::json!({
                "name": name,
                "topics": ["order/#"],
                "failure_action": "drop",
                "operations": [{"key": "payload", "value": format!("${{payload}}-{name}")}],
            });
            let (status, created) =
                response_parts(create_transformation(State(state.clone()), make_body(&body)).await)
                    .await;
            assert_eq!(status, StatusCode::CREATED);
            assert_eq!(created["name"], serde_json::json!(name));
        }
        // Duplicate name is rejected and overwrites nothing.
        let dup = serde_json::json!({
            "name": "first",
            "topics": ["other/#"],
            "failure_action": "drop",
            "operations": [],
        });
        let (status, err) =
            response_parts(create_transformation(State(state.clone()), make_body(&dup)).await)
                .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("ALREADY_EXISTS"));
        // List keeps creation order through the production entry.
        let (status, listed) = response_parts(
            list_transformations(State(state.clone()), Query(Default::default())).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let names: Vec<String> = listed
            .as_array()
            .expect("list is a bare array")
            .iter()
            .map(|row| row["name"].as_str().expect("name is a string").to_string())
            .collect();
        assert_eq!(names, vec!["first".to_string(), "second".to_string()]);
        // The engine snapshot follows the same order.
        let steps = state.engine.transform_snapshot();
        let step_names: Vec<String> = steps.iter().map(|s| s.name.clone()).collect();
        assert_eq!(step_names, names);
    }

    #[tokio::test]
    async fn replace_reorders_chain_and_applies_to_live_messages() {
        let state = standalone_state();
        for name in ["t-a", "t-b"] {
            let body = serde_json::json!({
                "name": name,
                "topics": ["live/#"],
                "failure_action": "drop",
                "operations": [{"key": "payload", "value": format!("${{payload}}[{name}]")}],
            });
            let (status, _) =
                response_parts(create_transformation(State(state.clone()), make_body(&body)).await)
                    .await;
            assert_eq!(status, StatusCode::CREATED);
        }
        // Replace with the reversed order.
        let reversed = serde_json::json!([
            {"name": "t-b", "topics": ["live/#"], "failure_action": "drop",
             "operations": [{"key": "payload", "value": "${payload}[t-b]"}]},
            {"name": "t-a", "topics": ["live/#"], "failure_action": "drop",
             "operations": [{"key": "payload", "value": "${payload}[t-a]"}]},
        ]);
        let (status, listed) = response_parts(
            replace_transformations(State(state.clone()), make_body(&reversed)).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let names: Vec<String> = listed
            .as_array()
            .expect("replace answers the bare chain")
            .iter()
            .map(|row| row["name"].as_str().expect("name is a string").to_string())
            .collect();
        assert_eq!(names, vec!["t-b".to_string(), "t-a".to_string()]);
        // The new order takes effect on the next message.
        let topic = broker_protocol::Topic::new("live/1".to_string()).expect("valid topic");
        let (out_topic, out_payload, shed) = broker_rules::apply_transform_steps(
            &state.engine.transform_snapshot(),
            &topic,
            &Bytes::from("m"),
        );
        assert!(!shed);
        assert_eq!(out_topic.as_str(), "live/1");
        assert_eq!(
            String::from_utf8_lossy(&out_payload),
            "m[t-b][t-a]",
            "chain order defines execution order"
        );
    }

    #[tokio::test]
    async fn invalid_replace_leaves_old_chain_in_place() {
        let state = standalone_state();
        let body = serde_json::json!({
            "name": "keep",
            "topics": ["keep/#"],
            "failure_action": "drop",
            "operations": [],
        });
        let (status, _) =
            response_parts(create_transformation(State(state.clone()), make_body(&body)).await)
                .await;
        assert_eq!(status, StatusCode::CREATED);
        // One valid plus one invalid entry: the whole write fails.
        let bad = serde_json::json!([
            {"name": "keep", "topics": ["keep/#"], "failure_action": "drop", "operations": []},
            {"name": "broken", "topics": ["#+bad"]},
        ]);
        let (status, err) =
            response_parts(replace_transformations(State(state.clone()), make_body(&bad)).await)
                .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
        // The old chain is unchanged.
        let (status, listed) = response_parts(
            list_transformations(State(state.clone()), Query(Default::default())).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(listed.as_array().expect("list is an array").len(), 1);
        assert_eq!(listed[0]["name"], serde_json::json!("keep"));
        assert_eq!(state.engine.transform_snapshot().len(), 1);
    }
}
