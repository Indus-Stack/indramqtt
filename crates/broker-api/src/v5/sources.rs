//! Source instance store for the v5 management API.
//!
//! Covers `POST /sources` (register one source), `DELETE /sources/:id`
//! (drop one source) and `GET /sources_summary` (C07-22, aggregated
//! counts over the live store). Single-node, management-plane only:
//! the summary is computed on demand from a bounded snapshot clone, so
//! aggregation never runs on the publish or deliver path and neither
//! path references this store.
//!
//! Store bounds (both stated here and enforced below):
//! - at most [`MAX_SOURCES`] entries (512: caps management-plane memory
//!   for the ingress catalogue; creates past the cap are rejected with
//!   `BAD_REQUEST` instead of growing without limit);
//! - `id` is capped at [`MAX_SOURCE_ID_LEN`] chars (256: bounds map-key
//!   memory per entry; longer ids are rejected with `BAD_REQUEST`);
//! - every entry is three small fields (`id`, `type`, `enable`), so
//!   per-entry memory is constant and a summary snapshot clones at most
//!   the capped list once per request under a short lock.

use axum::{
    extract::{Path, State},
    response::{IntoResponse, Response},
    Json,
};
use std::collections::HashMap;
use std::sync::RwLock;

use crate::errors::ApiError;
use crate::ApiState;

/// Upper bound for stored sources. Creates past this size are rejected
/// with `BAD_REQUEST` instead of growing the map without limit. Reason:
/// the summary handler clones the whole snapshot once per request, so
/// the catalogue must stay small and bounded; 512 covers the documented
/// ingress catalogue with headroom while keeping per-request clones
/// cheap.
pub const MAX_SOURCES: usize = 512;
/// Longest accepted source `id`. Reason: bounds map-key memory per
/// entry so one create cannot balloon the store.
const MAX_SOURCE_ID_LEN: usize = 256;

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

/// Bounded in-memory source directory keyed by id.
///
/// Single-node map behind one lock; every method finishes quickly.
/// Management writes take the write lock; the summary read takes one
/// short lock and clones at most [`MAX_SOURCES`] small entries, so a
/// management read never blocks messaging (and publish/deliver never
/// touch this store at all).
pub struct SourceStore {
    inner: RwLock<HashMap<String, SourceEntry>>,
}

impl SourceStore {
    /// Empty directory.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
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
}

impl Default for SourceStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Source kinds mirror the connector backend families (ingress versus
/// egress over the same backends), so validation reuses the connector
/// registry rather than a second list that could drift from the
/// runtimes (same set the stateless source probe checks).
fn is_known_source_type(kind: &str) -> bool {
    broker_config::is_known_connector_type(kind)
}

/// `POST /api/v5/sources`: register one source after validating the
/// body against the named kind. A duplicate id is rejected cleanly,
/// never overwritten. Management-plane only; publish and deliver never
/// touch this store.
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
        Ok(entry) => (axum::http::StatusCode::CREATED, Json(entry.to_json())).into_response(),
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
        axum::http::StatusCode::NO_CONTENT.into_response()
    } else {
        ApiError::NotFound(format!("source {id:?} not found")).into_response()
    }
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
