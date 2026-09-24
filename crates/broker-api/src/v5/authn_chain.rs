//! Ordered authenticator chain for the v5 management API.
//!
//! Covers `GET /authentication` (ordered list),
//! `POST /authentication` (append one entry) and
//! `PUT /authentication/order` (replace the whole order) on the real
//! chain store.
//! Single-node, management-plane only: handlers clone at most the capped
//! list once per request under a short lock; the CONNECT path reads one
//! lock-free snapshot per connect and never takes the chain write lock,
//! and publish/deliver never touch this store.
//!
//! Only the built-in database executes; every other backend's config is
//! stored and reported, never claimed as live.
// TODO(parity): which per-entry fields plus list envelope does the spec
// require (bare list versus data/meta, status fields)? The rulebook does
// not decide the exact shape; the current choice reuses the shared
// pagination extractor (data plus meta) and reports
// id/mechanism/backend/enable only, omitting status fields the broker
// cannot supply.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;

use crate::errors::ApiError;
use crate::pagination::{meta_page, paginate, PageParams};
use crate::ApiState;
use broker_auth::{AuthnEntry, ChainInsertError, ChainReorderError};

/// `GET /authentication`: ordered page (`data` plus `meta`). An empty
/// chain reads as an empty page, never an error. Malformed
/// `page`/`limit` fall back to defaults via [`PageParams`].
pub async fn list_authn_chain(State(state): State<ApiState>, params: PageParams) -> Response {
    let all = state.authn_chain.list();
    let total = all.len();
    let page_items = paginate(&all, params.page, params.limit);
    let data: Vec<serde_json::Value> = page_items.iter().map(render_entry).collect();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "data": data,
            "meta": meta_page(params.page, params.limit, total),
        })),
    )
        .into_response()
}

/// `POST /authentication`: append one authenticator. Returns the stored
/// entry with 201. Malformed bodies fail with `BAD_REQUEST`; an existing
/// `id` fails with `ALREADY_EXISTS` instead of being overwritten; a full
/// chain fails with `BAD_REQUEST`.
pub async fn create_authn_chain(State(state): State<ApiState>, body: Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return ApiError::BadRequest(format!("invalid authentication body: {e}"))
                .into_response();
        }
    };
    let entry = match parse_chain_request(&value) {
        Ok(e) => e,
        Err(msg) => return ApiError::BadRequest(msg).into_response(),
    };
    match state.authn_chain.insert(entry) {
        Ok(stored) => (StatusCode::CREATED, Json(render_entry(&stored))).into_response(),
        Err(ChainInsertError::Duplicate) => {
            ApiError::AlreadyExists("authenticator already exists".to_string()).into_response()
        }
        Err(ChainInsertError::Full) => {
            ApiError::BadRequest("authenticator chain is full".to_string()).into_response()
        }
        Err(ChainInsertError::Invalid(msg)) => ApiError::BadRequest(msg).into_response(),
        Err(ChainInsertError::Persist(msg)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot persist authenticator chain: {msg}"),
            })),
        )
            .into_response(),
    }
}

/// `PUT /authentication/order`: replace the whole chain order with the
/// supplied ordered id list (`[{"id": "..."}, ...]`).
///
/// Validates the full list before applying: a partial list (stored ids
/// omitted), an unknown id, a duplicate id or a malformed body applies
/// nothing and fails with `BAD_REQUEST` in the documented `{code,
/// message}` shape. On success returns 204 with no body. The order
/// persists across a restart through the config registry.
/// Management-plane only: one short write lock per request; the CONNECT
/// path keeps reading its lock-free snapshot and publish/deliver never
/// touch this store.
pub async fn replace_authn_order(State(state): State<ApiState>, body: Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return ApiError::BadRequest(format!("invalid authentication order body: {e}"))
                .into_response();
        }
    };
    let ids = match parse_order_request(&value) {
        Ok(ids) => ids,
        Err(msg) => return ApiError::BadRequest(msg).into_response(),
    };
    match state.authn_chain.reorder(ids) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(ChainReorderError::Invalid(msg)) => ApiError::BadRequest(msg).into_response(),
        Err(ChainReorderError::Unknown(unknown)) => ApiError::BadRequest(format!(
            "unknown authenticator id(s): {}",
            unknown.join(", ")
        ))
        .into_response(),
        Err(ChainReorderError::Incomplete(missing)) => ApiError::BadRequest(format!(
            "order omits stored authenticator id(s): {}",
            missing.join(", ")
        ))
        .into_response(),
        Err(ChainReorderError::Duplicate(msg)) => ApiError::BadRequest(msg).into_response(),
        Err(ChainReorderError::Persist(msg)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot persist authenticator chain: {msg}"),
            })),
        )
            .into_response(),
    }
}

/// Validate an order body into the ordered id list.
///
/// The body must be a JSON array with at least the stored entries
/// covered; every element must be an object carrying a non-empty string
/// `id`. Extra keys on elements are ignored so future fields degrade to
/// the same reorder instead of a 400.
fn parse_order_request(value: &serde_json::Value) -> Result<Vec<String>, String> {
    let items = value
        .as_array()
        .ok_or_else(|| "authentication order must be a JSON array".to_string())?;
    let mut ids = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let obj = item
            .as_object()
            .ok_or_else(|| format!("authentication order[{index}] must be an object"))?;
        let id = obj
            .get("id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("authentication order[{index}].id is required"))?;
        if id.trim().is_empty() {
            return Err(format!(
                "authentication order[{index}].id must not be empty"
            ));
        }
        ids.push(id.to_string());
    }
    Ok(ids)
}

fn render_entry(entry: &AuthnEntry) -> serde_json::Value {
    let mut value = serde_json::json!({
        "id": entry.id,
        "mechanism": entry.mechanism,
        "backend": entry.backend,
        "enable": entry.enable,
    });
    if !entry.config.is_null() {
        value["config"] = entry.config.clone();
    }
    value
}

/// Validate a create body into a chain entry.
///
/// Required: `mechanism` and `backend` (both non-empty, both naming a
/// known value). Optional: `id` (defaults to `{mechanism}:{backend}`; a
/// colon-form id must start with its mechanism) and `enable` (defaults
/// to true). Backend config rides either as a nested `config` object or
/// as extra top-level fields besides the four known ones; both forms
/// store the same object and read back under `config`.
fn parse_chain_request(value: &serde_json::Value) -> Result<AuthnEntry, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "authentication entry must be a JSON object".to_string())?;
    let mechanism = obj
        .get("mechanism")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "field `mechanism` is required".to_string())?;
    if mechanism.trim().is_empty() {
        return Err("field `mechanism` must not be empty".to_string());
    }
    if !broker_config::is_known_authn_mechanism(mechanism) {
        return Err(format!("field `mechanism` {mechanism:?} is unknown"));
    }
    let backend = obj
        .get("backend")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "field `backend` is required".to_string())?;
    if backend.trim().is_empty() {
        return Err("field `backend` must not be empty".to_string());
    }
    if !broker_config::is_known_authn_backend(backend) {
        return Err(format!("field `backend` {backend:?} is unknown"));
    }
    let id = match obj.get("id") {
        None | Some(serde_json::Value::Null) => format!("{mechanism}:{backend}"),
        Some(v) => {
            let raw = v
                .as_str()
                .ok_or_else(|| "field `id` must be a string".to_string())?;
            if raw.trim().is_empty() {
                return Err("field `id` must not be empty".to_string());
            }
            raw.to_string()
        }
    };
    if let Some((head, _)) = id.split_once(':') {
        if head != mechanism {
            return Err(format!(
                "field `id` {id:?} must start with its mechanism {mechanism:?}"
            ));
        }
    }
    let enable = match obj.get("enable") {
        None | Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::Bool(b)) => *b,
        Some(_) => return Err("field `enable` must be a boolean".to_string()),
    };
    let config = match obj.get("config") {
        None | Some(serde_json::Value::Null) => {
            let mut extra = serde_json::Map::new();
            for (key, val) in obj {
                if !matches!(
                    key.as_str(),
                    "id" | "mechanism" | "backend" | "enable" | "config"
                ) {
                    extra.insert(key.clone(), val.clone());
                }
            }
            if extra.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::Value::Object(extra)
            }
        }
        Some(v) => {
            if !v.is_object() {
                return Err("field `config` must be an object".to_string());
            }
            if v.as_object().is_some_and(|m| m.is_empty()) {
                serde_json::Value::Null
            } else {
                v.clone()
            }
        }
    };
    Ok(AuthnEntry {
        id,
        mechanism: mechanism.to_string(),
        backend: backend.to_string(),
        enable,
        config,
    })
}
