//! Ordered authenticator chain for the v5 management API.
//!
//! Covers `GET /authentication` (ordered list),
//! `POST /authentication` (append one entry),
//! `PUT /authentication/order` (replace the whole order), the per-id
//! `GET /authentication/{id}` (read one entry),
//! `PUT /authentication/{id}` (replace one entry),
//! `DELETE /authentication/{id}` (remove one entry) and
//! `PUT /authentication/{id}/position/{position}` (move one entry to a
//! zero-based position) on the real
//! chain store.
//! Single-node, management-plane only: handlers clone at most the capped
//! list once per request under a short lock; the CONNECT path reads one
//! lock-free snapshot per connect and never takes the chain write lock,
//! and publish/deliver never touch this store.
//!
//! Only the built-in database executes; every other backend's config is
//! stored and reported, never claimed as live.
//!
//! Shape note: the reference specification types each authenticator as a
//! variant (one schema per mechanism and backend). The row reports the
//! stored keys (`id`, `mechanism`, `backend`, `enable`) plus the stored
//! backend fields at the top level. The management console reads the same
//! flat fields. A stored `config` object also renders under `config` for
//! older readers.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;

use crate::errors::ApiError;
use crate::ApiState;
use broker_auth::{
    AuthnEntry, ChainInsertError, ChainMoveError, ChainRemoveError, ChainReorderError,
    ChainUpdateError,
};

/// `GET /authentication`: the ordered chain as a bare JSON array. An empty
/// chain reads as `[]`, never an error. The reference API documents no
/// paging, so the route takes none.
pub async fn list_authn_chain(State(state): State<ApiState>) -> Response {
    let data: Vec<serde_json::Value> = state.authn_chain.list().iter().map(render_entry).collect();
    (StatusCode::OK, Json(data)).into_response()
}

/// `POST /authentication`: append one authenticator. Returns the stored
/// entry with 200, as the reference API documents. Malformed bodies fail with `BAD_REQUEST`; an existing
/// `id` fails with `ALREADY_EXISTS` instead of being overwritten; a full
/// chain fails with `BAD_REQUEST`.
// TODO(parity): does the documented API reject a write that matches no
// variant (a `mysql` entry without `query`, a `redis` entry without
// `cmd`)? The current choice stores it leniently, so older minimal
// bodies keep working, until the checker pins the rejection set.
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
        Ok(stored) => (StatusCode::OK, Json(render_entry(&stored))).into_response(),
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

/// `GET /authentication/{id}`: one chain entry by id.
///
/// Returns the stored entry with 200. Unknown ids fail with `NOT_FOUND`
/// in the documented `{code, message}` shape. The route is not paged.
/// Management-plane only: clones at most one entry under a short lock;
/// the CONNECT path keeps reading its lock-free snapshot and
/// publish/deliver never touch this store.
pub async fn get_authn_entry(
    State(state): State<ApiState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
    match state.authn_chain.get(&id) {
        Some(entry) => (StatusCode::OK, Json(render_entry(&entry))).into_response(),
        None => ApiError::NotFound(format!("authenticator {id:?} not found")).into_response(),
    }
}

/// `PUT /authentication/{id}`: replace the entry stored under `id`.
///
/// The whole replacement is validated before anything is applied: one
/// bad field rejects the entire write with `BAD_REQUEST` and leaves the
/// stored entry untouched. Unknown ids fail with `NOT_FOUND`. A body
/// `id` that differs from the path id is rejected. Omitted
/// `mechanism`/`backend`/`enable`/`config` keep their stored values, so
/// a body carrying only the fields to change still replaces after full
/// validation. On success returns 204 with no body, as the reference API
/// documents. The write
/// persists across a restart through the config registry.
/// Management-plane only: one short write lock per request; the CONNECT
/// path keeps reading its lock-free snapshot and publish/deliver never
/// touch this store.
// TODO(parity): may the update rename the entry or change its
// mechanism/backend, or is only the config/enable pair mutable? The
// rulebook does not decide the exact PUT field set; the current choice
// replaces the whole entry in place (same `id`) after full validation
// until the checker pins the shape.
pub async fn put_authn_entry(
    State(state): State<ApiState>,
    axum::extract::Path(id): axum::extract::Path<String>,
    body: Bytes,
) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return ApiError::BadRequest(format!("invalid authentication body: {e}"))
                .into_response();
        }
    };
    let Some(existing) = state.authn_chain.get(&id) else {
        return ApiError::NotFound(format!("authenticator {id:?} not found")).into_response();
    };
    let entry = match parse_update_request(&value, &id, &existing) {
        Ok(e) => e,
        Err(msg) => return ApiError::BadRequest(msg).into_response(),
    };
    match state.authn_chain.update(&id, entry) {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(ChainUpdateError::NotFound) => {
            ApiError::NotFound(format!("authenticator {id:?} not found")).into_response()
        }
        Err(ChainUpdateError::Invalid(msg)) => ApiError::BadRequest(msg).into_response(),
        Err(ChainUpdateError::Persist(msg)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot persist authenticator chain: {msg}"),
            })),
        )
            .into_response(),
    }
}

/// `DELETE /authentication/{id}`: remove one chain entry.
///
/// Unknown ids fail with `NOT_FOUND` in the documented `{code,
/// message}` shape and apply nothing. Deleting the last remaining entry
/// is refused with `BAD_REQUEST` instead of leaving authentication open
/// (an empty chain preserves today's open behaviour). The freed `id`
/// may be re-created. On success returns 204 with no body. The removal
/// persists across a restart through the config registry.
/// Management-plane only: one short write lock per request; the CONNECT
/// path keeps reading its lock-free snapshot and publish/deliver never
/// touch this store.
// TODO(parity): is "last" the last entry of any kind, or the last
// enabled entry / last live backend? The rulebook does not decide the
// exact refusal set; the current choice refuses deleting the sole
// remaining entry (any kind) as the conservative fail-closed answer
// until the checker pins it.
pub async fn delete_authn_entry(
    State(state): State<ApiState>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
    match state.authn_chain.remove(&id) {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(ChainRemoveError::NotFound) => {
            ApiError::NotFound(format!("authenticator {id:?} not found")).into_response()
        }
        Err(ChainRemoveError::Last(msg)) => ApiError::BadRequest(msg).into_response(),
        Err(ChainRemoveError::Persist(msg)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot persist authenticator chain: {msg}"),
            })),
        )
            .into_response(),
    }
}

/// `PUT /authentication/{id}/position/{position}`: move one chain entry
/// to a zero-based position.
///
/// The `{position}` path segment must parse as a non-negative integer;
/// anything else fails with `BAD_REQUEST` and moves nothing. Positions
/// are zero-based indexes into the stored order (`0` is first): a
/// position at or past the chain length fails with `BAD_REQUEST` and
/// moves nothing. Unknown ids fail with `NOT_FOUND` in the documented
/// `{code, message}` shape and apply nothing. Moving to the entry's
/// current index is a no-op success. On success returns 204 with no body,
/// as the reference API documents. The new order persists across a
/// restart through the config registry.
/// Management-plane only: one short write lock per request; the CONNECT
/// path keeps reading its lock-free snapshot and publish/deliver never
/// touch this store. The chain stays bounded by the W2-02 cap (at most
/// 8 entries): a move never grows the list, it only reorders at most the
/// capped entries under one short lock.
// TODO(parity): is `{position}` a zero-based index or one of the
// `front`/`rear`/`before:<id>`/`after:<id>` keywords? The rulebook does
// not decide the exact position vocabulary; the current choice implements
// the task's stated zero-based index assumption until the checker pins it.
pub async fn move_authn_position(
    State(state): State<ApiState>,
    axum::extract::Path((id, raw_position)): axum::extract::Path<(String, String)>,
) -> Response {
    let position: usize = match raw_position.trim().parse() {
        Ok(position) => position,
        Err(_) => {
            return ApiError::BadRequest(
                "field `position` must be a non-negative integer".to_string(),
            )
            .into_response();
        }
    };
    match state.authn_chain.move_to_position(&id, position) {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(ChainMoveError::NotFound) => {
            ApiError::NotFound(format!("authenticator {id:?} not found")).into_response()
        }
        Err(ChainMoveError::OutOfRange(msg)) => ApiError::BadRequest(msg).into_response(),
        Err(ChainMoveError::Persist(msg)) => (
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

/// Render one stored entry as a typed variant row.
/// Report the stored keys. Copy each stored backend field to the top
/// level. Keep the stored object under `config` for older readers.
/// Add the documented `user_id_type` default (`username`) for the
/// built-in database when the stored entry lacks it.
fn render_entry(entry: &AuthnEntry) -> serde_json::Value {
    let mut value = serde_json::json!({
        "id": entry.id,
        "mechanism": entry.mechanism,
        "backend": entry.backend,
        "enable": entry.enable,
    });
    if let Some(fields) = entry.config.as_object() {
        for (key, field) in fields {
            if value.get(key).is_none() {
                value[key] = field.clone();
            }
        }
        if !entry.config.is_null() {
            value["config"] = entry.config.clone();
        }
    }
    if entry.mechanism == "password_based"
        && entry.backend == "built_in_database"
        && value.get("user_id_type").is_none()
    {
        value["user_id_type"] = serde_json::json!("username");
    }
    value
}

/// Validate a create body into a chain entry.
///
/// Required: `mechanism` (non-empty, naming a known value). `backend` is
/// required except for `jwt` and `cinfo`: the documented shapes carry no
/// backend, so a missing backend defaults to the mechanism name and reads
/// back as an extra field. Optional: `id` (defaults to
/// `{mechanism}:{backend}`; a colon-form id must start with its mechanism)
/// and `enable` (defaults to true). Backend config rides either as a
/// nested `config` object or as extra top-level fields besides the four
/// known ones; both forms store the same object and read back under
/// `config`.
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
    let backend = match obj.get("backend") {
        Some(value) if !value.is_null() => {
            let raw = value
                .as_str()
                .ok_or_else(|| "field `backend` must be a string".to_string())?;
            if raw.trim().is_empty() {
                return Err("field `backend` must not be empty".to_string());
            }
            if !broker_config::is_known_authn_backend(raw) {
                return Err(format!("field `backend` {raw:?} is unknown"));
            }
            raw.to_string()
        }
        _ if mechanism == "jwt" || mechanism == "cinfo" => mechanism.to_string(),
        _ => {
            return Err("field `backend` is required".to_string());
        }
    };
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

/// Validate an update body into a chain entry keeping `path_id`.
///
/// The body must be a JSON object. A body `id` that differs from the
/// path id is rejected. Omitted `mechanism`/`backend`/`enable` keep
/// their stored values; supplied values must be non-empty and name
/// known values. Backend config rides either as a nested `config`
/// object or as extra top-level fields; when neither is present the
/// stored config is kept.
fn parse_update_request(
    value: &serde_json::Value,
    path_id: &str,
    existing: &AuthnEntry,
) -> Result<AuthnEntry, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "authentication entry must be a JSON object".to_string())?;
    if let Some(v) = obj.get("id") {
        if v.is_null() {
            // Explicit null keeps the path id.
        } else {
            let raw = v
                .as_str()
                .ok_or_else(|| "field `id` must be a string".to_string())?;
            if raw != path_id {
                return Err("field `id` must match the path id".to_string());
            }
        }
    }
    let mechanism = match obj.get("mechanism") {
        None | Some(serde_json::Value::Null) => existing.mechanism.clone(),
        Some(v) => {
            let raw = v
                .as_str()
                .ok_or_else(|| "field `mechanism` must be a string".to_string())?;
            if raw.trim().is_empty() {
                return Err("field `mechanism` must not be empty".to_string());
            }
            if !broker_config::is_known_authn_mechanism(raw) {
                return Err(format!("field `mechanism` {raw:?} is unknown"));
            }
            raw.to_string()
        }
    };
    let backend = match obj.get("backend") {
        None | Some(serde_json::Value::Null) => existing.backend.clone(),
        Some(v) => {
            let raw = v
                .as_str()
                .ok_or_else(|| "field `backend` must be a string".to_string())?;
            if raw.trim().is_empty() {
                return Err("field `backend` must not be empty".to_string());
            }
            if !broker_config::is_known_authn_backend(raw) {
                return Err(format!("field `backend` {raw:?} is unknown"));
            }
            raw.to_string()
        }
    };
    if let Some((head, _)) = path_id.split_once(':') {
        if head != mechanism {
            return Err(format!(
                "field `id` {path_id:?} must start with its mechanism {mechanism:?}"
            ));
        }
    }
    let enable = match obj.get("enable") {
        None | Some(serde_json::Value::Null) => existing.enable,
        Some(serde_json::Value::Bool(b)) => *b,
        Some(_) => return Err("field `enable` must be a boolean".to_string()),
    };
    let config = match obj.get("config") {
        Some(v) if !v.is_null() => {
            if !v.is_object() {
                return Err("field `config` must be an object".to_string());
            }
            if v.as_object().is_some_and(|m| m.is_empty()) {
                serde_json::Value::Null
            } else {
                v.clone()
            }
        }
        _ => {
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
                existing.config.clone()
            } else {
                serde_json::Value::Object(extra)
            }
        }
    };
    Ok(AuthnEntry {
        id: path_id.to_string(),
        mechanism,
        backend,
        enable,
        config,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_rules::{BackpressurePolicy, RuleEngine};

    fn standalone_state() -> ApiState {
        ApiState::standalone(std::sync::Arc::new(RuleEngine::new(
            16,
            BackpressurePolicy::DropOldest,
        )))
    }

    fn entry(id: &str, mechanism: &str, backend: &str) -> AuthnEntry {
        AuthnEntry {
            id: id.to_string(),
            mechanism: mechanism.to_string(),
            backend: backend.to_string(),
            enable: true,
            config: serde_json::Value::Null,
        }
    }

    async fn parts(response: Response) -> (StatusCode, serde_json::Value, Vec<u8>) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("test body is small and readable");
        let raw = bytes.to_vec();
        let json: serde_json::Value = if raw.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&raw).expect("test body is JSON")
        };
        (status, json, raw)
    }

    #[tokio::test]
    async fn detail_renders_flat_variant_fields() {
        // The detail row carries the backend fields at the top level.
        // Before: only `id`/`mechanism`/`backend`/`enable` plus nested
        // `config`, so no documented variant schema matched.
        let state = standalone_state();
        state
            .authn_chain
            .insert(entry(
                "password_based:built_in_database",
                "password_based",
                "built_in_database",
            ))
            .expect("insert applies");
        let (status, body, _) = parts(
            get_authn_entry(
                State(state),
                axum::extract::Path("password_based:built_in_database".to_string()),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["mechanism"], serde_json::json!("password_based"));
        assert_eq!(body["backend"], serde_json::json!("built_in_database"));
        assert_eq!(body["user_id_type"], serde_json::json!("username"));
    }

    #[tokio::test]
    async fn order_accepts_the_documented_body() {
        // The documented body is an array of `{"id": ...}` objects.
        // Before: the route answered 400 for this body.
        let state = standalone_state();
        state
            .authn_chain
            .insert(entry(
                "password_based:built_in_database",
                "password_based",
                "built_in_database",
            ))
            .expect("insert applies");
        state
            .authn_chain
            .insert(entry("password_based:mysql", "password_based", "mysql"))
            .expect("insert applies");
        let body = Bytes::from_static(
            br#"[{"id": "password_based:mysql"}, {"id": "password_based:built_in_database"}]"#,
        );
        let (status, _, raw) = parts(replace_authn_order(State(state.clone()), body).await).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(raw.is_empty(), "order answers 204 with no body");
        let ids: Vec<String> = state
            .authn_chain
            .list()
            .iter()
            .map(|e| e.id.clone())
            .collect();
        assert_eq!(
            ids,
            vec![
                "password_based:mysql".to_string(),
                "password_based:built_in_database".to_string()
            ]
        );

        // An unknown id fails closed and changes nothing.
        let body = Bytes::from_static(br#"[{"id": "password_based:nowhere"}]"#);
        let (status, err, _) = parts(replace_authn_order(State(state.clone()), body).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
        let ids: Vec<String> = state
            .authn_chain
            .list()
            .iter()
            .map(|e| e.id.clone())
            .collect();
        assert_eq!(ids.len(), 2);
    }

    #[tokio::test]
    async fn create_round_trips_each_documented_variant() {
        // Each documented variant keeps its required fields through the
        // write and the read. The stored row reports them flat at the
        // top level, so the answer matches the documented variant.
        // Before: `jwt` and `cinfo` failed with 400, as the handler
        // required a backend the documented shapes do not carry.
        let state = standalone_state();
        let cases: Vec<serde_json::Value> = vec![
            serde_json::json!({
                "mechanism": "jwt",
                "algorithm": "hmac-based",
                "secret": "mysecret",
                "secret_base64_encoded": false,
                "use_jwks": false,
                "verify_claims": {"username": "${username}"},
            }),
            serde_json::json!({
                "mechanism": "password_based",
                "backend": "built_in_database",
                "password_hash_algorithm": {"name": "sha256", "salt_position": "suffix"},
                "user_id_type": "username",
            }),
            serde_json::json!({
                "mechanism": "password_based",
                "backend": "http",
                "method": "post",
                "url": "http://127.0.0.1:18083",
                "headers": {"content-type": "application/json"},
                "body": {"username": "${username}", "password": "${password}"},
            }),
            serde_json::json!({
                "mechanism": "password_based",
                "backend": "redis",
                "redis_type": "single",
                "server": "127.0.0.1:6379",
                "cmd": "HMGET ${username} password_hash salt",
            }),
            serde_json::json!({
                "mechanism": "password_based",
                "backend": "mongodb",
                "mongo_type": "single",
                "server": "127.0.0.1:27017",
                "database": "example",
                "collection": "users",
            }),
            serde_json::json!({
                "mechanism": "password_based",
                "backend": "ldap",
                "server": "127.0.0.1:389",
                "base_dn": "ou=users,dc=example,dc=org",
                "username": "cn=reader",
            }),
            serde_json::json!({
                "mechanism": "password_based",
                "backend": "mysql",
                "server": "127.0.0.1:3306",
                "database": "mqtt",
                "query": "SELECT password_hash FROM users WHERE username = ${username} LIMIT 1",
            }),
            serde_json::json!({
                "mechanism": "cinfo",
                "checks": [],
            }),
        ];
        // The chain holds at most eight entries. The remaining documented
        // shapes (`gssapi` on `kerberos`, `scram` on `http`) round-trip
        // through their own state in the test below.
        assert_eq!(cases.len(), 8);
        for sent in &cases {
            let raw = serde_json::to_vec(sent).expect("case serialises");
            let (status, stored, _) =
                parts(create_authn_chain(State(state.clone()), Bytes::from(raw)).await).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(stored["mechanism"], sent["mechanism"]);
            for (key, want) in sent.as_object().expect("case is an object") {
                assert_eq!(&stored[key], want, "variant field {key} round-trips",);
            }
        }
        // The backend-free shapes default the backend to the mechanism.
        // The read keeps the default as an extra field.
        let rows = state.authn_chain.list();
        let jwt = rows
            .iter()
            .find(|entry| entry.mechanism == "jwt")
            .expect("jwt stored");
        assert_eq!(jwt.backend, "jwt");
        let cinfo = rows
            .iter()
            .find(|entry| entry.mechanism == "cinfo")
            .expect("cinfo stored");
        assert_eq!(cinfo.backend, "cinfo");
        assert_eq!(rows.len(), cases.len());
    }

    #[tokio::test]
    async fn create_round_trips_scram_and_kerberos_variants() {
        // The `scram` and `gssapi` shapes keep their discriminator
        // fields. They run on their own state, as the chain holds at
        // most eight entries.
        let state = standalone_state();
        let cases: Vec<serde_json::Value> = vec![
            serde_json::json!({
                "mechanism": "scram",
                "backend": "built_in_database",
                "algorithm": "sha256",
                "iteration_count": 4096,
            }),
            serde_json::json!({
                "mechanism": "scram",
                "backend": "http",
                "method": "post",
                "url": "http://127.0.0.1:18083/scram",
            }),
            serde_json::json!({
                "mechanism": "gssapi",
                "backend": "kerberos",
                "principal": "mqtt@EXAMPLE.ORG",
            }),
        ];
        for sent in &cases {
            let raw = serde_json::to_vec(sent).expect("case serialises");
            let (status, stored, _) =
                parts(create_authn_chain(State(state.clone()), Bytes::from(raw)).await).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(stored["mechanism"], sent["mechanism"]);
            for (key, want) in sent.as_object().expect("case is an object") {
                assert_eq!(&stored[key], want, "variant field {key} round-trips",);
            }
        }
        assert_eq!(state.authn_chain.list().len(), cases.len());
    }
}
