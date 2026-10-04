//! Ordered message-transformation chain for the v5 management API.
//!
//! Covers `GET /message_transformations` (ordered list),
//! `POST /message_transformations` (append one entry),
//! `PUT /message_transformations` (replace the whole chain) and
//! `POST /message_transformations/reorder` (replace the chain order)
//! on a real backend. Single-node, management-plane only: handlers clone at most
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

/// Single-request body bound for the dry-run probe: 256 KiB. Reason: a
/// sample message plus a chain fit, while one request cannot bloat
/// the kernel.
pub const MAX_DRYRUN_BODY_BYTES: usize = 256 * 1024;

/// Longest accepted dry-run sample payload: 256 KiB. Reason: the
/// sample mirrors a real ingress payload, and the output derives
/// from it, so both stay bounded by one cap.
pub const MAX_DRYRUN_PAYLOAD_BYTES: usize = 256 * 1024;

/// Single-request evaluation bound for the dry-run probe: 5 s.
/// Reason: the probe runs off the message path, and a bounded
/// input evaluates in milliseconds, so the bound caps the worst
/// case well under management HTTP timeouts.
// TODO(parity): does the documented dry-run define its own timeout
// or payload cap? The rulebook does not decide the exact bound;
// the current choice states 5 s and 256 KiB until the checker
// pins it.
pub const DRYRUN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

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

/// Why `reorder` refused a new order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransformReorderError {
    /// Unknown and omitted names in one order.
    Invalid(String),
    /// Names with no stored entry.
    Unknown(Vec<String>),
    /// Stored names missing from the order.
    Incomplete(Vec<String>),
    /// One name listed twice.
    Duplicate(String),
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

    /// Reorder the stored chain to the supplied name order.
    ///
    /// Validates the full order before applying anything: an unknown
    /// name, a stored name missing from the order, or a duplicated
    /// name applies nothing and reports the cause. On success the
    /// stored order swaps at once; the caller then syncs the engine
    /// snapshot and persists, as the replace handler does. Entries
    /// stay untouched: nothing is created or deleted.
    pub fn reorder(&self, names: Vec<String>) -> Result<(), TransformReorderError> {
        let mut seen = std::collections::HashSet::with_capacity(names.len());
        for name in &names {
            if !seen.insert(name.clone()) {
                return Err(TransformReorderError::Duplicate(format!(
                    "order names transformation {name:?} more than once"
                )));
            }
        }
        let reordered = {
            let current = self.inner.read().expect("transform store lock");
            let unknown: Vec<String> = names
                .iter()
                .filter(|name| !current.iter().any(|record| &record.name == *name))
                .cloned()
                .collect();
            let missing: Vec<String> = current
                .iter()
                .filter(|record| !names.contains(&record.name))
                .map(|record| record.name.clone())
                .collect();
            if !unknown.is_empty() && !missing.is_empty() {
                return Err(TransformReorderError::Invalid(format!(
                    "unknown transformation name(s): {}; order omits stored name(s): {}",
                    unknown.join(", "),
                    missing.join(", "),
                )));
            }
            if !unknown.is_empty() {
                return Err(TransformReorderError::Unknown(unknown));
            }
            if !missing.is_empty() {
                return Err(TransformReorderError::Incomplete(missing));
            }
            let mut next = Vec::with_capacity(current.len());
            for name in &names {
                let record = current
                    .iter()
                    .find(|record| &record.name == name)
                    .cloned()
                    .expect("names validated against current chain");
                next.push(record);
            }
            next
        };
        *self.inner.write().expect("transform store lock") = reordered;
        Ok(())
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

/// `POST /api/v5/message_transformations/reorder`: replace the chain
/// order with the supplied name order.
///
/// The body must be a JSON object with an `order` array of names
/// (`{"order": ["b", "a"]}`). The full order gets validated before
/// anything applies: an unknown name, a stored name missing from the
/// order, a duplicated name, or a malformed body fails with
/// `BAD_REQUEST` and leaves the old order in place. On success answers
/// 204 with no body; the new order shows on the next list read and
/// drives execution order. Nothing is created or deleted.
/// Management-plane only; publish and deliver never touch this store.
///
/// Store wiring (rulebook section 2): the backing store is the same
/// [`TransformChainStore`] as the list/create/replace routes, shared
/// with the kernel (`crates/broker-node/src/main.rs`, `serve_api`
/// share); the driving broker event is the ingress transform step
/// (`crates/broker-node/src/main.rs`, ingress pipeline) loading the
/// engine snapshot. Declared `wired` in `tools/wiring.txt`.
///
/// Fire-path cost: none. The publish path keeps loading one `Arc` per
/// ingress with no new lock, allocation, or queue from this route.
/// This route takes the store write lock once, swaps the order, and
/// syncs the engine snapshot; order persists across a restart with
/// the chain through the same export path as replace.
///
/// Tenant behaviour: the reorder selects no session, so no path
/// leaves a tenant.
// TODO(parity): does the documented body also accept a bare array or
// per-entry objects (`[{"name": "b"}]`)? The rulebook does not decide
// the exact shape; the current choice accepts only `{"order": [...]}`
// with string entries until the checker pins it.
// TODO(parity): which detail keys do the documented errors carry
// beyond `code` and `message` (unknown-name, omitted-name and
// duplicate lists)? The rulebook does not decide the exact shape; the
// current choice reports the names inside `message` until the checker
// pins it.
pub async fn reorder_transformations(State(state): State<ApiState>, body: Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return ApiError::BadRequest(format!("invalid reorder body: {e}")).into_response();
        }
    };
    let names = match parse_reorder_request(&value) {
        Ok(names) => names,
        Err(msg) => return ApiError::BadRequest(msg).into_response(),
    };
    match state.transformations.reorder(names) {
        Ok(()) => {
            sync_engine(&state);
            if let Err(error) = state.transformations.persist() {
                return transform_persist_error(error);
            }
            StatusCode::NO_CONTENT.into_response()
        }
        Err(TransformReorderError::Invalid(msg)) => ApiError::BadRequest(msg).into_response(),
        Err(TransformReorderError::Unknown(unknown)) => ApiError::BadRequest(format!(
            "unknown transformation name(s): {}",
            unknown.join(", ")
        ))
        .into_response(),
        Err(TransformReorderError::Incomplete(missing)) => ApiError::BadRequest(format!(
            "order omits stored transformation name(s): {}",
            missing.join(", ")
        ))
        .into_response(),
        Err(TransformReorderError::Duplicate(msg)) => ApiError::BadRequest(msg).into_response(),
    }
}

/// Validate a reorder body into the ordered name list.
///
/// The body must be a JSON object carrying an `order` array of
/// non-empty string names. Extra top-level keys are ignored. The list
/// must hold at most [`MAX_TRANSFORMATIONS`] names: a longer list can
/// never match the capped chain, so it fails here instead of growing
/// the request without bound.
fn parse_reorder_request(value: &serde_json::Value) -> Result<Vec<String>, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "reorder body must be a JSON object".to_string())?;
    let items = obj
        .get("order")
        .ok_or_else(|| "field `order` is required".to_string())?;
    let items = items
        .as_array()
        .ok_or_else(|| "field `order` must be an array of names".to_string())?;
    if items.len() > MAX_TRANSFORMATIONS {
        return Err(format!(
            "field `order` holds {} entries, at most {MAX_TRANSFORMATIONS}",
            items.len()
        ));
    }
    let mut names = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let name = item
            .as_str()
            .ok_or_else(|| format!("field `order[{index}]` must be a string"))?;
        if name.trim().is_empty() {
            return Err(format!("field `order[{index}]` must not be empty"));
        }
        if name.len() > MAX_TRANSFORMATION_NAME_LEN {
            return Err(format!(
                "field `order[{index}]` holds {} chars, at most {MAX_TRANSFORMATION_NAME_LEN}",
                name.len()
            ));
        }
        names.push(name.to_string());
    }
    Ok(names)
}

/// `POST /api/v5/message_transformations/dryrun`: stateless probe.
///
/// Runs the supplied chain against one sample message through the
/// real executor ([`broker_rules::apply_transform_steps`]), the same
/// function ingress uses, and answers 200 with the transformed
/// output. A shed message still answers 200 with `dropped: true`
/// plus the reason. A malformed body answers 400 with the documented
/// `{code, message}` shape. The probe writes no store, publishes
/// nothing, writes no retained state, and never touches the stored
/// chain. Unknown ids do not apply: only the supplied chain runs.
/// The route takes no paging parameters; unknown query keys are
/// ignored. Management-plane only; publish and deliver never touch
/// this path.
///
/// Store wiring (rulebook section 2): no store. The handler adds no
/// `Arc<crate::v5::...>` field on `ApiState`, nothing outside
/// `crates/broker-api` writes or consults anything here, and no
/// broker event drives it beyond serving the probe, so
/// `tools/wiring_check.sh` needs no new entry. The backing executor
/// is the live transform step runner
/// (`crates/broker-rules/src/lib.rs`, `apply_transform_steps`), and
/// the driving broker event for the shared live chain stays the
/// ingress transform step (`crates/broker-node/src/main.rs`, ingress
/// pipeline) loading the engine snapshot; this probe never loads or
/// stores that snapshot.
///
/// Fire-path cost: none. The probe runs synchronously per request
/// off the message path; the publish path keeps loading one `Arc`
/// per ingress with no new lock, allocation, or queue from this
/// route. The probe holds no lock across evaluation and caches
/// nothing: every request re-parses and re-evaluates.
///
/// Tenant behaviour: the probe renders strings only and selects no
/// session, so no path leaves a tenant.
// TODO(parity): which exact request keys does the documented probe
// require (chain key, sample nesting)? The rulebook does not decide
// the exact shape; the current choice accepts `transformations` plus
// `chain`/`transformation`/`transforms` aliases and flat or nested
// topic/payload until the checker pins it.
// TODO(parity): which exact success keys does the documented probe
// require beyond the transformed output (`payload` versus `output`,
// `dropped` versus `shed`, reason key)? The rulebook does not decide
// the exact shape; the current choice serves `topic`, `payload`,
// `output` (mirrors `payload`), `dropped`, `shed` (mirrors
// `dropped`), and `reason` (null when kept) until the checker pins it.
// TODO(parity): which reason detail does a shed message require
// (per-step name versus generic notice)? The rulebook does not decide;
// the current choice reports a generic notice until the checker pins it.
pub async fn dryrun_transformations(
    State(_state): State<ApiState>,
    Query(_ignored): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    if body.len() > MAX_DRYRUN_BODY_BYTES {
        return ApiError::BadRequest(format!(
            "dry-run body is too large: {} bytes exceeds {MAX_DRYRUN_BODY_BYTES} bytes",
            body.len()
        ))
        .into_response();
    }
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return ApiError::BadRequest(format!("invalid dry-run body: {e}")).into_response();
        }
    };
    let obj = match value.as_object() {
        Some(o) => o,
        None => {
            return ApiError::BadRequest("dry-run body must be a JSON object".to_string())
                .into_response();
        }
    };
    let chain_items = match extract_dryrun_chain(obj) {
        Ok(items) => items,
        Err(msg) => return ApiError::BadRequest(msg).into_response(),
    };
    if chain_items.len() > MAX_TRANSFORMATIONS {
        return ApiError::BadRequest(format!(
            "dry-run chain holds {} entries, at most {MAX_TRANSFORMATIONS}",
            chain_items.len()
        ))
        .into_response();
    }
    let mut records = Vec::with_capacity(chain_items.len());
    for (index, item) in chain_items.iter().enumerate() {
        match parse_transform_request(item) {
            Ok(record) => records.push(record),
            Err(msg) => {
                return ApiError::BadRequest(format!("transformations[{index}]: {msg}"))
                    .into_response();
            }
        }
    }
    let topic_raw = match extract_dryrun_topic(obj) {
        Some(t) => t,
        None => {
            return ApiError::BadRequest("field `topic` is required".to_string()).into_response()
        }
    };
    let topic = match broker_protocol::Topic::new(topic_raw.clone()) {
        Ok(t) => t,
        Err(_) => {
            return ApiError::BadRequest(format!("field `topic` {topic_raw:?} is not valid"))
                .into_response();
        }
    };
    let payload = match extract_dryrun_payload(obj) {
        Some(p) => p,
        None => {
            return ApiError::BadRequest("field `payload` is required".to_string()).into_response();
        }
    };
    if payload.len() > MAX_DRYRUN_PAYLOAD_BYTES {
        return ApiError::BadRequest(format!(
            "dry-run payload holds {} bytes, at most {MAX_DRYRUN_PAYLOAD_BYTES}",
            payload.len()
        ))
        .into_response();
    }
    let steps: Vec<broker_rules::TransformStep> =
        records.iter().map(TransformRecord::to_step).collect();
    let evaluated = tokio::time::timeout(DRYRUN_TIMEOUT, async move {
        broker_rules::apply_transform_steps(&steps, &topic, &payload)
    })
    .await;
    let (out_topic, out_payload, shed) = match evaluated {
        Ok(output) => output,
        Err(_) => {
            return (
                StatusCode::REQUEST_TIMEOUT,
                Json(serde_json::json!({
                    "code": "TIMEOUT",
                    "message": format!(
                        "dry-run timed out after {}s",
                        DRYRUN_TIMEOUT.as_secs()
                    ),
                })),
            )
                .into_response();
        }
    };
    if out_payload.len() > MAX_DRYRUN_PAYLOAD_BYTES {
        return (
            StatusCode::OK,
            Json(serde_json::json!({
                "topic": out_topic.as_str(),
                "payload": "",
                "output": "",
                "dropped": true,
                "shed": true,
                "reason": format!(
                    "dry-run output holds {} bytes, at most {MAX_DRYRUN_PAYLOAD_BYTES}",
                    out_payload.len()
                ),
            })),
        )
            .into_response();
    }
    let payload_text = String::from_utf8_lossy(&out_payload).into_owned();
    let reason = if shed {
        serde_json::Value::String("message dropped by transformation chain".to_string())
    } else {
        serde_json::Value::Null
    };
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "topic": out_topic.as_str(),
            "payload": payload_text,
            "output": payload_text,
            "dropped": shed,
            "shed": shed,
            "reason": reason,
        })),
    )
        .into_response()
}

/// Pick the supplied chain from the accepted request keys.
fn extract_dryrun_chain(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<Vec<serde_json::Value>, String> {
    for key in ["transformations", "chain", "transforms"] {
        if let Some(items) = obj.get(key) {
            return items
                .as_array()
                .cloned()
                .ok_or_else(|| format!("field `{key}` must be an array of transformations"));
        }
    }
    if let Some(single) = obj.get("transformation") {
        if let Some(items) = single.as_array() {
            return Ok(items.clone());
        }
        if single.is_object() {
            return Ok(vec![single.clone()]);
        }
        return Err("field `transformation` must be an object or an array".to_string());
    }
    Err("field `transformations` is required".to_string())
}

/// Pick the sample topic from the flat or nested request shape.
fn extract_dryrun_topic(obj: &serde_json::Map<String, serde_json::Value>) -> Option<String> {
    if let Some(topic) = obj.get("topic").and_then(|v| v.as_str()) {
        if !topic.trim().is_empty() {
            return Some(topic.to_string());
        }
    }
    for nest in ["message", "sample", "context"] {
        if let Some(inner) = obj.get(nest).and_then(|v| v.as_object()) {
            if let Some(topic) = inner.get("topic").and_then(|v| v.as_str()) {
                if !topic.trim().is_empty() {
                    return Some(topic.to_string());
                }
            }
        }
    }
    None
}

/// Render the sample payload to bytes. Strings pass through as-is;
/// other JSON values serialise to their compact form.
fn dryrun_payload_bytes(value: &serde_json::Value) -> Option<Bytes> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(Bytes::from(s.clone().into_bytes())),
        other => Some(Bytes::from(
            serde_json::to_string(other)
                .unwrap_or_default()
                .into_bytes(),
        )),
    }
}

/// Pick the sample payload from the flat or nested request shape.
fn extract_dryrun_payload(obj: &serde_json::Map<String, serde_json::Value>) -> Option<Bytes> {
    if let Some(value) = obj.get("payload") {
        if let Some(bytes) = dryrun_payload_bytes(value) {
            return Some(bytes);
        }
    }
    for nest in ["message", "sample", "context"] {
        if let Some(inner) = obj.get(nest).and_then(|v| v.as_object()) {
            if let Some(value) = inner.get("payload") {
                if let Some(bytes) = dryrun_payload_bytes(value) {
                    return Some(bytes);
                }
            }
        }
    }
    None
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

    fn reorder_body(names: &[&str]) -> Bytes {
        let order: Vec<serde_json::Value> = names
            .iter()
            .map(|name| serde_json::Value::String(name.to_string()))
            .collect();
        Bytes::from(
            serde_json::to_vec(&serde_json::json!({ "order": order })).expect("order is JSON"),
        )
    }

    async fn listed_names(state: &ApiState) -> Vec<String> {
        let (status, listed) = response_parts(
            list_transformations(State(state.clone()), Query(Default::default())).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        listed
            .as_array()
            .expect("list is a bare array")
            .iter()
            .map(|row| row["name"].as_str().expect("name is a string").to_string())
            .collect()
    }

    async fn live_output(state: &ApiState, topic: &str, payload: &str) -> (String, bool) {
        let topic = broker_protocol::Topic::new(topic.to_string()).expect("valid topic");
        let (_, out_payload, shed) = broker_rules::apply_transform_steps(
            &state.engine.transform_snapshot(),
            &topic,
            &Bytes::from(payload.to_string().into_bytes()),
        );
        (String::from_utf8_lossy(&out_payload).into_owned(), shed)
    }

    #[tokio::test]
    async fn reorder_flips_list_and_execution_order() {
        let state = standalone_state();
        for name in ["re-a", "re-b"] {
            let body = serde_json::json!({
                "name": name,
                "topics": ["reorder/live/#"],
                "failure_action": "drop",
                "operations": [{"key": "payload", "value": format!("${{payload}}[{name}]")}],
            });
            let (status, _) =
                response_parts(create_transformation(State(state.clone()), make_body(&body)).await)
                    .await;
            assert_eq!(status, StatusCode::CREATED);
        }
        assert_eq!(
            listed_names(&state).await,
            vec!["re-a".to_string(), "re-b".to_string()]
        );
        // Reorder through the production entry: answers 204 with no body.
        let response =
            reorder_transformations(State(state.clone()), reorder_body(&["re-b", "re-a"])).await;
        let (status, empty) = response_parts(response).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert_eq!(empty, serde_json::Value::Null);
        // The new order shows on the next list read.
        assert_eq!(
            listed_names(&state).await,
            vec!["re-b".to_string(), "re-a".to_string()]
        );
        // The engine snapshot follows, so the next message runs flipped.
        let step_names: Vec<String> = state
            .engine
            .transform_snapshot()
            .iter()
            .map(|step| step.name.clone())
            .collect();
        assert_eq!(step_names, vec!["re-b".to_string(), "re-a".to_string()]);
        let (out, shed) = live_output(&state, "reorder/live/1", "m").await;
        assert!(!shed);
        assert_eq!(out, "m[re-b][re-a]", "chain order defines execution order");
    }

    #[tokio::test]
    async fn reorder_unknown_name_is_rejected_with_order_unchanged() {
        let state = standalone_state();
        for name in ["keep-a", "keep-b"] {
            let body = serde_json::json!({
                "name": name,
                "topics": ["reorder/keep/#"],
                "failure_action": "drop",
                "operations": [{"key": "payload", "value": format!("${{payload}}[{name}]")}],
            });
            let (status, _) =
                response_parts(create_transformation(State(state.clone()), make_body(&body)).await)
                    .await;
            assert_eq!(status, StatusCode::CREATED);
        }
        // One stored name plus one unknown name: the whole write fails.
        let (status, err) = response_parts(
            reorder_transformations(State(state.clone()), reorder_body(&["keep-b", "nowhere"]))
                .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
        assert!(err["message"].is_string());
        // The old order is unchanged on the list and on live messages.
        assert_eq!(
            listed_names(&state).await,
            vec!["keep-a".to_string(), "keep-b".to_string()]
        );
        let step_names: Vec<String> = state
            .engine
            .transform_snapshot()
            .iter()
            .map(|step| step.name.clone())
            .collect();
        assert_eq!(step_names, vec!["keep-a".to_string(), "keep-b".to_string()]);
        let (out, shed) = live_output(&state, "reorder/keep/1", "m").await;
        assert!(!shed);
        assert_eq!(out, "m[keep-a][keep-b]");
    }

    #[tokio::test]
    async fn reorder_malformed_and_partial_orders_are_rejected_without_change() {
        let state = standalone_state();
        for name in ["m-a", "m-b"] {
            let body = serde_json::json!({
                "name": name,
                "topics": ["reorder/malformed/#"],
                "failure_action": "drop",
                "operations": [],
            });
            let (status, _) =
                response_parts(create_transformation(State(state.clone()), make_body(&body)).await)
                    .await;
            assert_eq!(status, StatusCode::CREATED);
        }
        let long_name = "n".repeat(MAX_TRANSFORMATION_NAME_LEN + 1);
        let long_order: Vec<serde_json::Value> = (0..(MAX_TRANSFORMATIONS + 1))
            .map(|i| serde_json::json!(format!("m-{i}")))
            .collect();
        for bad in [
            serde_json::json!({"order": "m-a"}),
            serde_json::json!({"order": [{"name": "m-a"}]}),
            serde_json::json!({"order": ["m-a", 7]}),
            serde_json::json!({"order": ["m-a", "  "]}),
            serde_json::json!({"order": [long_name]}),
            serde_json::json!({"order": long_order}),
            serde_json::json!({}),
            serde_json::json!([]),
            serde_json::json!("m-a"),
            // Stored name omitted: partial order.
            serde_json::json!({"order": ["m-a"]}),
            // Empty order on a non-empty chain.
            serde_json::json!({"order": []}),
            // Duplicated name.
            serde_json::json!({"order": ["m-a", "m-a", "m-b"]}),
        ] {
            let (status, err) = response_parts(
                reorder_transformations(State(state.clone()), make_body(&bad)).await,
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "body {bad} must fail");
            assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
        }
        // Every rejected write left the old order in place.
        assert_eq!(
            listed_names(&state).await,
            vec!["m-a".to_string(), "m-b".to_string()]
        );
        assert_eq!(state.engine.transform_snapshot().len(), 2);
    }

    #[test]
    fn reorder_persists_across_registry_reload() {
        let dir = std::env::temp_dir().join(format!(
            "indramqtt-transform-reorder-{}-{}",
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
        for name in ["p-a", "p-b"] {
            let entry = parse_transform_request(&serde_json::json!({
                "name": name,
                "topics": ["persist/#"],
                "failure_action": "drop",
                "operations": [],
            }))
            .expect("valid");
            store.insert(entry).expect("insert");
        }
        store
            .reorder(vec!["p-b".to_string(), "p-a".to_string()])
            .expect("reorder applies");
        store.persist().expect("save");
        let reloaded =
            std::sync::Arc::new(broker_config::ConfigRegistry::load(&dir).expect("reload"));
        let names: Vec<String> = reloaded
            .snapshot()
            .transformations
            .transformations
            .iter()
            .map(|entry| entry.name.clone())
            .collect();
        assert_eq!(names, vec!["p-b".to_string(), "p-a".to_string()]);
        let restarted = TransformChainStore::from_registry(&reloaded);
        let live: Vec<String> = restarted
            .snapshot()
            .iter()
            .map(|record| record.name.clone())
            .collect();
        assert_eq!(live, names);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn dryrun_body(value: &serde_json::Value) -> Bytes {
        Bytes::from(serde_json::to_vec(value).expect("dry-run body is JSON"))
    }

    async fn dryrun_parts(
        state: &ApiState,
        value: &serde_json::Value,
    ) -> (StatusCode, serde_json::Value) {
        response_parts(
            dryrun_transformations(
                State(state.clone()),
                Query(Default::default()),
                dryrun_body(value),
            )
            .await,
        )
        .await
    }

    #[tokio::test]
    async fn dryrun_valid_chain_reports_output_and_keeps_stored_chain() {
        let state = standalone_state();
        let stored = serde_json::json!({
            "name": "stored",
            "topics": ["dry/#"],
            "failure_action": "drop",
            "operations": [{"key": "payload", "value": "${payload}-stored"}],
        });
        let (status, _) =
            response_parts(create_transformation(State(state.clone()), make_body(&stored)).await)
                .await;
        assert_eq!(status, StatusCode::CREATED);
        let before = state.transformations.snapshot();
        let engine_before = state.engine.transform_snapshot().len();
        let probe = serde_json::json!({
            "transformations": [{
                "name": "probe",
                "topics": ["dry/#"],
                "failure_action": "drop",
                "operations": [{"key": "payload", "value": "${payload}-probe"}],
            }],
            "topic": "dry/1",
            "payload": "m",
        });
        let (status, out) = dryrun_parts(&state, &probe).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(out["topic"], serde_json::json!("dry/1"));
        assert_eq!(out["payload"], serde_json::json!("m-probe"));
        assert_eq!(out["output"], out["payload"]);
        assert_eq!(out["dropped"], serde_json::json!(false));
        assert_eq!(out["shed"], out["dropped"]);
        // The stored chain is unchanged.
        let after: Vec<String> = state
            .transformations
            .snapshot()
            .iter()
            .map(|r| r.name.clone())
            .collect();
        let names: Vec<String> = before.iter().map(|r| r.name.clone()).collect();
        assert_eq!(after, names);
        assert_eq!(state.engine.transform_snapshot().len(), engine_before);
        // The live path still uses the stored chain.
        let topic = broker_protocol::Topic::new("dry/1".to_string()).expect("valid topic");
        let (_, live, shed) = broker_rules::apply_transform_steps(
            &state.engine.transform_snapshot(),
            &topic,
            &Bytes::from("m"),
        );
        assert!(!shed);
        assert_eq!(String::from_utf8_lossy(&live), "m-stored");
    }

    #[tokio::test]
    async fn dryrun_shed_reports_dropped_with_reason() {
        let state = standalone_state();
        let probe = serde_json::json!({
            "transformations": [{
                "name": "bad-topic",
                "topics": ["dry/#"],
                "failure_action": "drop",
                "operations": [{"key": "topic", "value": ""}],
            }],
            "topic": "dry/1",
            "payload": "m",
        });
        let (status, out) = dryrun_parts(&state, &probe).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(out["dropped"], serde_json::json!(true));
        assert_eq!(out["shed"], serde_json::json!(true));
        assert!(out["reason"].as_str().is_some_and(|r| !r.is_empty()));
        assert!(state.transformations.snapshot().is_empty());
    }

    #[tokio::test]
    async fn dryrun_malformed_body_is_rejected_with_error_shape() {
        let state = standalone_state();
        let stored = serde_json::json!({
            "name": "keep",
            "topics": ["keep/#"],
            "failure_action": "drop",
            "operations": [],
        });
        let (status, _) =
            response_parts(create_transformation(State(state.clone()), make_body(&stored)).await)
                .await;
        assert_eq!(status, StatusCode::CREATED);
        for bad in [
            serde_json::json!({"topic": "a/b", "payload": "m"}),
            serde_json::json!({
                "transformations": [{"name": "broken", "topics": ["#+bad"]}],
                "topic": "a/b",
                "payload": "m",
            }),
            serde_json::json!({
                "transformations": [],
                "payload": "m",
            }),
            serde_json::json!({
                "transformations": [],
                "topic": "a/b",
            }),
            serde_json::json!([]),
        ] {
            let (status, err) = dryrun_parts(&state, &bad).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
            assert!(err["message"].is_string());
        }
        // A rejected probe stores nothing new.
        assert_eq!(state.transformations.snapshot().len(), 1);
        assert_eq!(state.engine.transform_snapshot().len(), 1);
    }

    #[tokio::test]
    async fn dryrun_oversized_body_is_rejected_before_parsing() {
        let state = standalone_state();
        let big = "x".repeat(MAX_DRYRUN_BODY_BYTES + 1);
        let raw = Bytes::from(format!(
            "{{\"transformations\":[],\"topic\":\"t\",\"payload\":\"{big}\"}}"
        ));
        let (status, err) = response_parts(
            dryrun_transformations(State(state.clone()), Query(Default::default()), raw).await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
    }
}
