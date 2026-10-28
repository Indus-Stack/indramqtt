//! Authorization source registry for the v5 management API.
//!
//! Covers `GET /authorization/sources` (ordered collection), `POST
//! /authorization/sources` (register one source), `GET
//! /authorization/sources/:type` (one source's config and counters),
//! `PUT /authorization/sources/:type` (replace one source) and `POST
//! /authorization/sources/:type/metrics/reset` (zero one source's
//! counters), plus `DELETE
//! /authorization/sources/built_in_database/rules` (clear every
//! built-in rule) over the real [`broker_auth::AuthzSourceRegistry`]
//! plus the live built-in rule table (`MemoryAuth` in
//! `crates/broker-auth/src/lib.rs`, consulted by the publish event in
//! `crates/broker-node/src/main.rs` and the console path in
//! `crates/broker-api/src/ws.rs`). Single-node: handlers clone at most
//! the capped list once per request under a short lock; the publish and
//! subscribe consults load the lock-free order snapshot for the stored
//! enable flag and order plus the built-in rule table, and attribute
//! each verdict to the built-in counters; publish/deliver never take
//! this registry's write lock.
//!
//! Only the built-in database source executes; every other source type's
//! config is stored and reported, never claimed as live (per-type query
//! clients are deferred follow-ups). An empty registry lists as an empty
//! collection, never an error. Error answers use the documented
//! `{code, message}` shape.
//!
//! Bound (stated here and enforced in `broker-auth`): at most
//! [`broker_config::MAX_AUTHZ_SOURCES`] sources (8: the management plane
//! snapshots the whole registry per request, so it must stay small and
//! bounded; 8 covers the documented backend families with headroom while
//! keeping per-request clones cheap). Default registry: only the
//! built-in database source.
//!
//! Shape note: the reference specification types each source as a variant
//! (one schema per backend). Each row reports the stored keys (`type`,
//! `enable`) plus the stored backend fields at the top level. The
//! management console reads the same flat fields. A stored `config`
//! object also renders under `config` for older readers.
// TODO(parity): does a successful clear report an empty 204 or a 200
// success envelope, and does creating with an unknown `type` fail with
// 404 or 400? The rulebook does not decide the exact shapes; the current
// choice answers clear with 204 (mirroring the sibling cache clears) and
// unknown types with NOT_FOUND (a type reference that names nothing
// stored or documented), until the checker pins it.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;

use crate::errors::ApiError;
use crate::pagination::PageParams;
use crate::ApiState;
use broker_auth::{AuthzSourceEntry, SourceInsertError, SourceUpdateError};

/// Render one stored source. Report the stored keys (`type`, `enable`).
/// Copy each stored backend field to the top level. Keep the stored
/// object under `config` for older readers. Omit health fields the
/// broker does not measure.
fn render_entry(entry: &AuthzSourceEntry) -> serde_json::Value {
    let mut value = serde_json::json!({
        "type": entry.source_type,
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
    value
}

/// `GET /authorization/sources`: ordered collection under `sources`.
/// An empty registry reads as an empty collection, never an error.
/// The reference specification documents `{"sources": [...]}`; the
/// broker answers exactly that shape in stored order.
pub async fn list_authz_sources(State(state): State<ApiState>, params: PageParams) -> Response {
    let _ = params;
    let all = state.authz_sources.list();
    let data: Vec<serde_json::Value> = all.iter().map(render_entry).collect();
    (StatusCode::OK, Json(serde_json::json!({ "sources": data }))).into_response()
}

/// Why a create body was refused.
enum CreateError {
    /// The body is malformed (message names the field).
    Invalid(String),
    /// The `type` names no documented source type.
    UnknownType(String),
}

/// `POST /authorization/sources`: register one source. Returns 204 with
/// no body, as the reference API documents. Malformed bodies fail with `BAD_REQUEST`; a
/// `type` that names no documented source fails with `NOT_FOUND`; an
/// already-stored `type` fails with `BAD_REQUEST` instead of merging;
/// a full registry fails with `BAD_REQUEST`.
// TODO(parity): does the documented API reject a write that matches no
// variant (a `redis` source without `cmd`, a `file` source without
// `rules`)? The current choice stores it leniently, so older minimal
// bodies keep working, until the checker pins the rejection set.
pub async fn create_authz_source(State(state): State<ApiState>, body: Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return ApiError::BadRequest(format!("invalid authorization source body: {e}"))
                .into_response();
        }
    };
    let entry = match parse_source_request(&value) {
        Ok(e) => e,
        Err(CreateError::Invalid(msg)) => return ApiError::BadRequest(msg).into_response(),
        Err(CreateError::UnknownType(source_type)) => {
            return ApiError::NotFound(format!("authorization source {source_type:?} not found"))
                .into_response();
        }
    };
    // An already-stored kind stays a duplicate rejection. This also
    // covers a source that an older version stored.
    if state.authz_sources.get(&entry.source_type).is_some() {
        return ApiError::BadRequest("authorization source already exists".to_string())
            .into_response();
    }
    // Only the built-in database executes in this build; every other
    // source kind is refused before anything is stored so a stored-only
    // source can never weaken access (D2).
    let kind = entry.source_type.clone();
    if kind != "built_in_database" {
        return ApiError::BadRequest(format!(
            "authorization source {kind} is not available in this broker"
        ))
        .into_response();
    }
    match state.authz_sources.insert(entry) {
        Ok(_) => {
            state.sessions.clear_all_authz_caches();
            StatusCode::NO_CONTENT.into_response()
        }
        Err(SourceInsertError::Duplicate) => {
            ApiError::BadRequest("authorization source already exists".to_string()).into_response()
        }
        Err(SourceInsertError::Full) => {
            ApiError::BadRequest("authorization source registry is full".to_string())
                .into_response()
        }
        Err(SourceInsertError::Invalid(msg)) => ApiError::BadRequest(msg).into_response(),
        Err(SourceInsertError::Persist(msg)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot persist authorization sources: {msg}"),
            })),
        )
            .into_response(),
    }
}

/// Validate a create body into a registry entry.
///
/// Required: `type` (non-empty, naming a documented source type).
/// Optional: `enable` (defaults to true) and `config` (an object;
/// missing or empty reads as no stored config). Extra top-level fields
/// besides the three known ones are stored as the backend config so
/// backend-specific fields degrade to the same create instead of a 400.
fn parse_source_request(value: &serde_json::Value) -> Result<AuthzSourceEntry, CreateError> {
    let obj = value.as_object().ok_or_else(|| {
        CreateError::Invalid("authorization source must be a JSON object".to_string())
    })?;
    let source_type = obj
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| CreateError::Invalid("field `type` is required".to_string()))?;
    if source_type.trim().is_empty() {
        return Err(CreateError::Invalid(
            "field `type` must not be empty".to_string(),
        ));
    }
    if !broker_config::is_known_authz_source_type(source_type) {
        return Err(CreateError::UnknownType(source_type.to_string()));
    }
    let enable = match obj.get("enable") {
        None | Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::Bool(b)) => *b,
        Some(_) => {
            return Err(CreateError::Invalid(
                "field `enable` must be a boolean".to_string(),
            ));
        }
    };
    let config = match obj.get("config") {
        None | Some(serde_json::Value::Null) => {
            let mut extra = serde_json::Map::new();
            for (key, val) in obj {
                if !matches!(key.as_str(), "type" | "enable" | "config") {
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
                return Err(CreateError::Invalid(
                    "field `config` must be an object".to_string(),
                ));
            }
            if v.as_object().is_some_and(|m| m.is_empty()) {
                serde_json::Value::Null
            } else {
                v.clone()
            }
        }
    };
    Ok(AuthzSourceEntry {
        source_type: source_type.to_string(),
        enable,
        config,
    })
}

/// `GET /authorization/sources/built_in_database/rules`: same built-in
/// table as the generic `GET /authorization/sources/:type/rules` with
/// `type = built_in_database`. Needed alongside the generic route because
/// the static catch-all path would otherwise shadow the generic one for
/// this type and answer 405 for non-DELETE methods.
pub async fn list_built_in_rules(State(state): State<ApiState>) -> Response {
    super::auth::list_authz_rules(State(state), Path("built_in_database".to_string())).await
}

/// `POST /authorization/sources/built_in_database/rules`: same built-in
/// table as the generic `POST /authorization/sources/:type/rules` with
/// `type = built_in_database`. Needed alongside the generic route because
/// the static catch-all path would otherwise shadow the generic one for
/// this type and answer 405 for creates.
pub async fn create_built_in_rule(
    State(state): State<ApiState>,
    Json(req): Json<super::auth::CreateAuthzRuleReq>,
) -> Response {
    super::auth::create_authz_rule(
        State(state),
        Path("built_in_database".to_string()),
        Json(req),
    )
    .await
}

/// `DELETE /authorization/sources/built_in_database/rules`: clear every
/// built-in rule.
///
/// Clearing an already-empty table still succeeds (204 with no body).
/// The removal persists across a restart through the config registry
/// (the same `MemoryAuth` commit path the rule-table consults).
/// Management-plane only: one short rule-table write lock per request
/// (plus the registry save lock on persist); the publish and subscribe
/// authorization consults keep reading under short read locks and never
/// take this lock beyond their check.
pub async fn clear_built_in_rules(State(state): State<ApiState>) -> Response {
    if let Err(error) = state.auth.clear_rules() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot clear built-in authorization rules: {error}"),
            })),
        )
            .into_response();
    }
    state.sessions.clear_all_authz_caches();
    StatusCode::NO_CONTENT.into_response()
}

/// Render one catch-all rule with the documented per-rule fields only:
/// `topic`, `permission` (`allow`/`deny`) and `action`
/// (`publish`/`subscribe`/`all`). Fields the broker does not measure
/// (qos, retain, patterns) are omitted, never invented; the table is
/// global, so no per-client key is rendered.
fn render_catch_all_rule(rule: &broker_auth::AclRule) -> serde_json::Value {
    serde_json::json!({
        "topic": rule.topic_pattern,
        "permission": if rule.allow { "allow" } else { "deny" },
        "action": match rule.action {
            broker_auth::AclAction::Publish => "publish",
            broker_auth::AclAction::Subscribe => "subscribe",
            broker_auth::AclAction::All => "all",
        },
    })
}

/// `GET /authorization/sources/built_in_database/rules/all`: read the
/// catch-all list.
///
/// Returns 200 with `{"rules": [...]}`; an empty table returns
/// `{"rules": []}`, never an error. The list is small and not paged:
/// at most [`broker_auth::MAX_AUTHZ_CATCH_ALL_RULES`] entries (128: the
/// management plane snapshots the whole list per request and the
/// publish/subscribe decision scans it linearly, so it must stay small
/// and bounded) are ever returned in stored order.
/// Management-plane only: clones at most the capped list once per
/// request under a short read lock; the publish event in
/// `crates/broker-node/src/main.rs` consults the same table
/// (`MemoryAuth::authorize_publish` in `crates/broker-auth/src/lib.rs`)
/// and publish/deliver never take this handler's lock.
// TODO(parity): is the documented envelope `{"rules": [...]}` with exactly
// topic/permission/action per rule, and is the list truly unpaged? The
// rulebook does not decide the exact shape; the current choice returns the
// modelled triple without invented qos/retain/pattern fields until the
// checker pins it.
pub async fn list_catch_all_rules(State(state): State<ApiState>) -> Response {
    let rules = state.auth.acl_rules();
    let rendered: Vec<serde_json::Value> = rules.iter().map(render_catch_all_rule).collect();
    (
        StatusCode::OK,
        Json(serde_json::json!({ "rules": rendered })),
    )
        .into_response()
}

/// `POST /authorization/sources/built_in_database/rules/all`: replace the
/// catch-all list.
///
/// The body must be an object carrying the replacement under `rules`
/// (`{"rules": [...]}`); anything else fails with `BAD_REQUEST` and
/// applies nothing. Every entry is validated before anything becomes
/// visible (validate-all-before-apply): each must carry a non-empty
/// `topic` that parses as an MQTT filter, a `permission` of
/// `allow`/`deny` and an `action` of `publish`/`subscribe`/`all`; a bad
/// entry fails the whole write with `BAD_REQUEST` and the stored table
/// is unchanged. A list longer than
/// [`broker_auth::MAX_AUTHZ_CATCH_ALL_RULES`] entries is rejected the
/// same way. On success the replacement persists across a restart
/// through the config registry and answers 204 with no body.
/// Management-plane only: one short rule-table write lock per request
/// (plus the registry save lock on persist); the publish and subscribe
/// authorization consults keep reading under short read locks.
// TODO(parity): which additional per-rule match fields (qos, retain,
// client/username patterns, zone/listener/ip) does the spec require, and
// is a bare array body accepted alongside the `{"rules": [...]}` object?
// The rulebook does not decide the exact set; the current choice requires
// the object shape, models the topic/permission/action triple (with an
// optional `clientid` defaulting to global `*`), and ignores extra match
// fields rather than inventing them.
pub async fn replace_catch_all_rules(State(state): State<ApiState>, body: Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return ApiError::BadRequest(format!("invalid catch-all rule body: {e}"))
                .into_response();
        }
    };
    let rules = match parse_catch_all_body(&value) {
        Ok(rules) => rules,
        Err(msg) => return ApiError::BadRequest(msg).into_response(),
    };
    if let Err(error) = state.auth.replace_rules(rules) {
        let message = error.to_string();
        if message.contains("at most")
            || message.contains("must not be empty")
            || message.contains("not a valid filter")
            || message.contains("unknown")
        {
            return ApiError::BadRequest(message).into_response();
        }
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot replace catch-all authorization rules: {message}"),
            })),
        )
            .into_response();
    }
    state.sessions.clear_all_authz_caches();
    StatusCode::NO_CONTENT.into_response()
}

/// Validate a catch-all replace body into ordered [`broker_auth::AclRule`]s.
///
/// Required shape: an object with a `rules` array. Each entry must be an
/// object with string `topic` (non-empty, valid MQTT filter), string
/// `permission` (`allow`/`deny`, case-insensitive) and string `action`
/// (`publish`/`subscribe`/`all`, case-insensitive). An optional string
/// `clientid` selects the client pattern and defaults to global `*`;
/// every other field is ignored so future match fields degrade to the
/// same replace instead of a 400.
fn parse_catch_all_body(value: &serde_json::Value) -> Result<Vec<broker_auth::AclRule>, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "catch-all body must be an object with a `rules` array".to_string())?;
    let items = obj
        .get("rules")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "catch-all body must carry an array under `rules`".to_string())?;
    if items.len() > broker_auth::MAX_AUTHZ_CATCH_ALL_RULES {
        return Err(format!(
            "catch-all holds {} entries, at most {} (field `rules`)",
            items.len(),
            broker_auth::MAX_AUTHZ_CATCH_ALL_RULES
        ));
    }
    let mut out = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let entry = item
            .as_object()
            .ok_or_else(|| format!("catch-all rules[{index}] must be an object (field `rules`)"))?;
        let topic = entry
            .get("topic")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("catch-all rules[{index}].topic is required (field `topic`)"))?;
        if topic.trim().is_empty() {
            return Err(format!(
                "catch-all rules[{index}].topic must not be empty (field `topic`)"
            ));
        }
        if broker_protocol::TopicFilter::new(topic.to_string()).is_err() {
            return Err(format!(
                "catch-all rules[{index}].topic {topic:?} is not a valid filter (field `topic`)"
            ));
        }
        let permission = entry
            .get("permission")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                format!("catch-all rules[{index}].permission is required (field `permission`)")
            })?;
        let allow = match permission.to_ascii_lowercase().as_str() {
            "allow" => true,
            "deny" => false,
            _ => {
                return Err(format!(
                    "catch-all rules[{index}].permission {permission:?} is unknown (field `permission`)"
                ));
            }
        };
        let action_raw = entry
            .get("action")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                format!("catch-all rules[{index}].action is required (field `action`)")
            })?;
        let action = broker_auth::AclAction::parse(action_raw).ok_or_else(|| {
            format!("catch-all rules[{index}].action {action_raw:?} is unknown (field `action`)")
        })?;
        let client_pattern = match entry.get("clientid").or(entry.get("username")) {
            None | Some(serde_json::Value::Null) => "*".to_string(),
            Some(serde_json::Value::String(s)) => {
                if s.trim().is_empty() {
                    return Err(format!(
                        "catch-all rules[{index}].clientid must not be empty (field `clientid`)"
                    ));
                }
                s.clone()
            }
            Some(_) => {
                return Err(format!(
                    "catch-all rules[{index}].clientid must be a string (field `clientid`)"
                ));
            }
        };
        out.push(broker_auth::AclRule::new(
            client_pattern,
            action,
            topic.to_string(),
            allow,
        ));
    }
    Ok(out)
}

/// `DELETE /authorization/sources/built_in_database/rules/all`: clear the
/// catch-all list.
///
/// Clearing an already-empty table still succeeds (204 with no body).
/// The removal persists across a restart through the config registry
/// (the same `MemoryAuth` commit path the publish/subscribe consults
/// read). Management-plane only: one short rule-table write lock per
/// request (plus the registry save lock on persist); the publish and
/// subscribe authorization consults keep reading under short read locks.
pub async fn clear_catch_all_rules(State(state): State<ApiState>) -> Response {
    if let Err(error) = state.auth.clear_rules() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot clear catch-all authorization rules: {error}"),
            })),
        )
            .into_response();
    }
    state.sessions.clear_all_authz_caches();
    StatusCode::NO_CONTENT.into_response()
}

/// `PUT /authorization/sources/order`: replace the whole source
/// evaluation order with the supplied ordered type list.
///
/// The body must be a JSON array; every element must be an object
/// carrying a non-empty string `type` naming a stored source. Extra keys
/// on elements are ignored so future fields degrade to the same reorder
/// instead of a 400. The full list is validated before anything becomes
/// visible (validate-all-before-apply): an unknown type, a duplicate
/// type, or a list omitting a stored type applies nothing and fails with
/// `BAD_REQUEST` in the documented `{code, message}` shape. On success
/// returns 204 with no body. The order persists across a restart through
/// the config registry and the publish/subscribe consults honour it from
/// the next verdict (stored-only sources are skipped in stored order;
/// a disabled or removed built-in source denies fail closed).
/// Management-plane only for the write: one short registry write lock
/// per request (plus the registry save lock on persist); the publish
/// and subscribe consults load the lock-free order snapshot and never
/// take this handler's lock.
// TODO(parity): is the documented order body a bare array of
// `{"type": ...}` objects (as implemented here, mirroring the sibling
// authenticator order), an array of bare type strings, or an object
// carrying the list? The rulebook does not decide the exact envelope;
// the current choice mirrors the sibling order until the checker pins it.
pub async fn replace_authz_order(State(state): State<ApiState>, body: Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return ApiError::BadRequest(format!("invalid authorization order body: {e}"))
                .into_response();
        }
    };
    let types = match parse_order_request(&value) {
        Ok(types) => types,
        Err(msg) => return ApiError::BadRequest(msg).into_response(),
    };
    match state.authz_sources.reorder(types) {
        Ok(()) => {
            state.sessions.clear_all_authz_caches();
            StatusCode::NO_CONTENT.into_response()
        }
        Err(broker_auth::SourceReorderError::Invalid(msg)) => {
            ApiError::BadRequest(msg).into_response()
        }
        Err(broker_auth::SourceReorderError::Unknown(unknown)) => ApiError::BadRequest(format!(
            "unknown authorization source type(s): {}",
            unknown.join(", ")
        ))
        .into_response(),
        Err(broker_auth::SourceReorderError::Incomplete(missing)) => ApiError::BadRequest(format!(
            "order omits stored authorization source type(s): {}",
            missing.join(", ")
        ))
        .into_response(),
        Err(broker_auth::SourceReorderError::Duplicate(msg)) => {
            ApiError::BadRequest(msg).into_response()
        }
        Err(broker_auth::SourceReorderError::Persist(msg)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot persist authorization sources: {msg}"),
            })),
        )
            .into_response(),
    }
}

/// Validate an order body into the ordered type list.
///
/// The body must be a JSON array; every element must be an object
/// carrying a non-empty string `type`. Extra keys on elements are
/// ignored so future fields degrade to the same reorder instead of a
/// 400.
fn parse_order_request(value: &serde_json::Value) -> Result<Vec<String>, String> {
    let items = value
        .as_array()
        .ok_or_else(|| "authorization order must be a JSON array".to_string())?;
    let mut types = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let obj = item
            .as_object()
            .ok_or_else(|| format!("authorization order[{index}] must be an object"))?;
        let source_type = obj
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("authorization order[{index}].type is required"))?;
        if source_type.trim().is_empty() {
            return Err(format!(
                "authorization order[{index}].type must not be empty"
            ));
        }
        types.push(source_type.to_string());
    }
    Ok(types)
}

/// `DELETE /authorization/sources/{type}`: remove one source.
///
/// Unknown types fail with `NOT_FOUND` in the documented `{code,
/// message}` shape and apply nothing. Deleting the last remaining
/// source is refused with `BAD_REQUEST` instead of leaving
/// authorization open. On success returns 204 with no body. The removal
/// persists across a restart through the config registry and the
/// publish/subscribe consults honour it from the next verdict.
/// Management-plane only for the write: one short registry write lock
/// per request (plus the registry save lock on persist); the publish
/// and subscribe consults load the lock-free order snapshot and never
/// take this handler's lock.
// TODO(parity): is "last" the last entry of any kind, or the last
// enabled entry / last live backend? The rulebook does not decide the
// exact refusal set; the current choice refuses deleting the sole
// remaining entry (any kind) as the conservative fail-closed answer
// until the checker pins it.
pub async fn delete_authz_source(
    State(state): State<ApiState>,
    Path(source_type): Path<String>,
) -> Response {
    match state.authz_sources.remove(&source_type) {
        Ok(_) => {
            state.sessions.clear_all_authz_caches();
            StatusCode::NO_CONTENT.into_response()
        }
        Err(broker_auth::SourceRemoveError::NotFound) => {
            ApiError::NotFound(format!("authorization source {source_type:?} not found"))
                .into_response()
        }
        Err(broker_auth::SourceRemoveError::Last(msg)) => ApiError::BadRequest(msg).into_response(),
        Err(broker_auth::SourceRemoveError::Persist(msg)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot persist authorization sources: {msg}"),
            })),
        )
            .into_response(),
    }
}

/// Render one stored source with its live counters. Report the stored
/// keys (`type`, `enable`) plus the stored backend fields at the top
/// level and the counter-only `metrics` (`allow`/`deny`/`total`,
/// memory-only, zeroed on restart). Keep the stored object under
/// `config` for older readers. Health the broker does not measure
/// (`status`, rates) is omitted, never invented.
fn render_detail(
    entry: &AuthzSourceEntry,
    counters: broker_auth::AuthzSourceCounterSnapshot,
) -> serde_json::Value {
    let mut value = serde_json::json!({
        "type": entry.source_type,
        "enable": entry.enable,
        "metrics": {
            "allow": counters.allow,
            "deny": counters.deny,
            "total": counters.total,
        },
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
    value
}

/// `GET /authorization/sources/{type}`: read one source's config and
/// counters by type.
///
/// Returns 200 with the stored `type`/`enable` (plus the backend
/// `config` object when one is stored) and the live counter-only
/// `metrics`. Health the broker does not measure is omitted, never
/// invented. Unknown types fail with `NOT_FOUND` in the documented
/// `{code, message}` shape. Management-plane read only: one short
/// registry read plus lock-free atomic loads; publish and deliver never
/// take this store's write lock.
pub async fn get_authz_source(
    State(state): State<ApiState>,
    Path(source_type): Path<String>,
) -> Response {
    let Some(entry) = state.authz_sources.get(&source_type) else {
        return ApiError::NotFound(format!("authorization source {source_type:?} not found"))
            .into_response();
    };
    let counters = state
        .authz_sources
        .counter_snapshot(&source_type)
        .unwrap_or(broker_auth::AuthzSourceCounterSnapshot {
            allow: 0,
            deny: 0,
            total: 0,
        });
    (StatusCode::OK, Json(render_detail(&entry, counters))).into_response()
}

/// `PUT /authorization/sources/{type}`: replace the source stored under
/// `type`.
///
/// The whole replacement is validated before anything is applied: one
/// bad field rejects the entire write with `BAD_REQUEST` and leaves the
/// stored entry untouched. An unknown type fails with `NOT_FOUND`; a
/// known type other than the built-in database fails with `BAD_REQUEST`.
/// A body `type` that differs from the path type is rejected. Omitted
/// `enable`/`config` keep their stored values, so a body carrying only
/// the fields to change still replaces after full validation. On
/// success returns 204 with no body, as the reference API documents.
/// The write persists across a restart through the config registry
/// (counters do not: they restart at zero) and the publish/subscribe
/// consults honour it from the next verdict.
/// Management-plane only for the write: one short write lock per
/// request (plus the registry save lock on persist); the publish and
/// subscribe consults load the lock-free order snapshot and never take
/// this handler's lock.
// TODO(parity): may the update rename the entry (change its `type`) or
// only replace the config/enable pair in place? The rulebook does not
// decide the exact PUT field set; the current choice replaces the whole
// entry in place (same `type`) after full validation until the checker
// pins the shape.
pub async fn put_authz_source(
    State(state): State<ApiState>,
    Path(source_type): Path<String>,
    body: Bytes,
) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return ApiError::BadRequest(format!("invalid authorization source body: {e}"))
                .into_response();
        }
    };
    // Unknown types fail with NOT_FOUND. Only the built-in
    // database executes in this build. Refuse every other known
    // type before anything is stored (D2).
    if !broker_config::is_known_authz_source_type(&source_type) {
        return ApiError::NotFound(format!("authorization source {source_type:?} not found"))
            .into_response();
    }
    if source_type != "built_in_database" {
        return ApiError::BadRequest(format!(
            "authorization source {source_type} is not available in this broker"
        ))
        .into_response();
    }
    let Some(existing) = state.authz_sources.get(&source_type) else {
        return ApiError::NotFound(format!("authorization source {source_type:?} not found"))
            .into_response();
    };
    let entry = match parse_update_request(&value, &source_type, &existing) {
        Ok(e) => e,
        Err(msg) => return ApiError::BadRequest(msg).into_response(),
    };
    match state.authz_sources.update(&source_type, entry) {
        Ok(_) => {
            state.sessions.clear_all_authz_caches();
            StatusCode::NO_CONTENT.into_response()
        }
        Err(SourceUpdateError::NotFound) => {
            ApiError::NotFound(format!("authorization source {source_type:?} not found"))
                .into_response()
        }
        Err(SourceUpdateError::Invalid(msg)) => ApiError::BadRequest(msg).into_response(),
        Err(SourceUpdateError::Persist(msg)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot persist authorization sources: {msg}"),
            })),
        )
            .into_response(),
    }
}

/// Validate an update body into a source entry keeping `path_type`.
///
/// The body must be a JSON object. A body `type` that differs from the
/// path type is rejected (explicit null keeps the path type). Omitted
/// `enable` keeps the stored value; a supplied value must be a boolean.
/// Backend config rides either as a nested `config` object (empty reads
/// as no stored config) or as extra top-level fields besides the three
/// known ones; when neither is present the stored config is kept.
fn parse_update_request(
    value: &serde_json::Value,
    path_type: &str,
    existing: &AuthzSourceEntry,
) -> Result<AuthzSourceEntry, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "authorization source must be a JSON object".to_string())?;
    if let Some(v) = obj.get("type") {
        if v.is_null() {
            // Explicit null keeps the path type.
        } else {
            let raw = v
                .as_str()
                .ok_or_else(|| "field `type` must be a string".to_string())?;
            if raw != path_type {
                return Err("field `type` must match the path type".to_string());
            }
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
                if !matches!(key.as_str(), "type" | "enable" | "config") {
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
    Ok(AuthzSourceEntry {
        source_type: path_type.to_string(),
        enable,
        config,
    })
}

/// `POST /authorization/sources/{type}/metrics/reset`: zero the live
/// counters for one source.
///
/// Resets the per-source counters to zero and reports success (204, no
/// body). Resetting an unknown type fails with `NOT_FOUND` in the
/// documented `{code, message}` shape, not an empty body. Counters
/// resume from zero on the next verdict; the reset itself does not
/// persist beyond the counter values. Management-plane only for the
/// reset: one short registry read plus lock-free atomic stores;
/// verdict increments stay lock-free atomics and publish and deliver
/// never take the registry write lock here. The
/// counter set is fixed (allow/deny/total per source type, at most the
/// source cap of records), so a reset never grows anything.
// TODO(parity): does the spec report success as an empty 204 or as a 200
// with a body? The rulebook does not decide the exact success shape; the
// current choice answers 204 with no body, mirroring the sibling rule,
// node-cache and authenticator resets, until the checker pins it.
pub async fn reset_authz_source_metrics(
    State(state): State<ApiState>,
    Path(source_type): Path<String>,
) -> Response {
    if !state.authz_sources.reset_counters(&source_type) {
        return ApiError::NotFound(format!("authorization source {source_type:?} not found"))
            .into_response();
    }
    StatusCode::NO_CONTENT.into_response()
}

/// `POST /authorization/sources/{type}/move`: reposition one source
/// within the evaluation order and report the new order.
///
/// The body must be a JSON object carrying the target under `position`
/// (a non-negative integer index, a numeric string, or one of the
/// `front`/`rear`/`before:<type>`/`after:<type>` keywords), or under
/// `before`/`after` (a stored source type naming the anchor). The move
/// is bounds-checked: an unknown path type fails with `NOT_FOUND` and
/// moves nothing, and a bad position (missing key, unknown anchor, or
/// an index at or past the registry length) fails with `BAD_REQUEST`
/// and moves nothing. Moving to the entry's current position is a
/// no-op success. On success returns 204 with no body, as the reference
/// API documents. The new order persists across a restart through the
/// config registry and the publish/subscribe consults honour it from
/// the next verdict.
/// Management-plane only for the write: one short registry write lock
/// per request (plus the registry save lock on persist); the publish
/// and subscribe consults load the lock-free order snapshot and never
/// take this handler's lock. A
/// move never grows the list: at most the source cap of entries is
/// reordered, and the counter set stays fixed.
// TODO(parity): is the documented move body `{"position": ...}` with a
// front/rear/before:/after: vocabulary, a `{"before": ...}` /
// `{"after": ...}` anchor pair, or a numeric index only? The rulebook
// does not decide the exact envelope; the current choice accepts all
// three shapes until the checker pins it.
pub async fn move_authz_source(
    State(state): State<ApiState>,
    Path(source_type): Path<String>,
    body: Bytes,
) -> Response {
    if state.authz_sources.get(&source_type).is_none() {
        return ApiError::NotFound(format!("authorization source {source_type:?} not found"))
            .into_response();
    }
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return ApiError::BadRequest(format!("invalid authorization move body: {e}"))
                .into_response();
        }
    };
    let order = state.authz_sources.list();
    let position = match resolve_move_target(&value, &source_type, &order) {
        Ok(position) => position,
        Err(msg) => return ApiError::BadRequest(msg).into_response(),
    };
    match state.authz_sources.relocate(&source_type, position) {
        Ok(_) => {
            state.sessions.clear_all_authz_caches();
            StatusCode::NO_CONTENT.into_response()
        }
        Err(broker_auth::SourceMoveError::NotFound) => {
            ApiError::NotFound(format!("authorization source {source_type:?} not found"))
                .into_response()
        }
        Err(broker_auth::SourceMoveError::OutOfRange(msg)) => {
            ApiError::BadRequest(msg).into_response()
        }
        Err(broker_auth::SourceMoveError::Persist(msg)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot persist authorization sources: {msg}"),
            })),
        )
            .into_response(),
    }
}

/// Resolve a move body into the zero-based target index for `moving`.
///
/// Accepted shapes (all equivalent): `{"position": <int>}`, `{"position":
/// "<int>" | "front" | "rear" | "before:<type>" | "after:<type>"}`,
/// `{"before": "<type>"}`, `{"after": "<type>"}`. Anchors must name a
/// stored source type (naming the moving entry itself is a no-op for
/// both `before` and `after`). Anything else is a
/// client error describing the bad position; nothing is applied by this
/// function itself.
fn resolve_move_target(
    value: &serde_json::Value,
    moving: &str,
    order: &[AuthzSourceEntry],
) -> Result<usize, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "authorization move body must be a JSON object".to_string())?;
    if let Some(before) = obj.get("before") {
        let anchor = before
            .as_str()
            .ok_or_else(|| "field `before` must be a source type string".to_string())?;
        if anchor.trim().is_empty() {
            return Err("field `before` must not be empty".to_string());
        }
        if anchor == moving {
            let pos = order
                .iter()
                .position(|e| e.source_type == moving)
                .ok_or_else(|| format!("authorization source {moving:?} not found"))?;
            return Ok(pos);
        }
        let anchor_pos = order
            .iter()
            .position(|e| e.source_type == anchor)
            .ok_or_else(|| format!("move anchor {anchor:?} is unknown (field `before`)"))?;
        let from = order
            .iter()
            .position(|e| e.source_type == moving)
            .ok_or_else(|| format!("authorization source {moving:?} not found"))?;
        // Removing the moving entry shifts a later anchor down by one.
        if from < anchor_pos {
            return Ok(anchor_pos - 1);
        }
        return Ok(anchor_pos);
    }
    if let Some(after) = obj.get("after") {
        let anchor = after
            .as_str()
            .ok_or_else(|| "field `after` must be a source type string".to_string())?;
        if anchor.trim().is_empty() {
            return Err("field `after` must not be empty".to_string());
        }
        if anchor == moving {
            let pos = order
                .iter()
                .position(|e| e.source_type == moving)
                .ok_or_else(|| format!("authorization source {moving:?} not found"))?;
            return Ok(pos);
        }
        let anchor_pos = order
            .iter()
            .position(|e| e.source_type == anchor)
            .ok_or_else(|| format!("move anchor {anchor:?} is unknown (field `after`)"))?;
        let from = order
            .iter()
            .position(|e| e.source_type == moving)
            .ok_or_else(|| format!("authorization source {moving:?} not found"))?;
        // Removing the moving entry shifts a later anchor down by one;
        // the slot after the anchor is the target in final coordinates.
        if from < anchor_pos {
            return Ok(anchor_pos);
        }
        return Ok(anchor_pos + 1);
    }
    let raw = obj
        .get("position")
        .ok_or_else(|| "authorization move body must carry `position`".to_string())?;
    if let Some(index) = raw.as_u64() {
        let position = index as usize;
        if position >= order.len() {
            return Err(format!(
                "position {position} is out of range for {} entries (valid 0..{})",
                order.len(),
                order.len().saturating_sub(1),
            ));
        }
        return Ok(position);
    }
    if let Some(text) = raw.as_str() {
        let trimmed = text.trim();
        if let Ok(index) = trimmed.parse::<usize>() {
            if index >= order.len() {
                return Err(format!(
                    "position {index} is out of range for {} entries (valid 0..{})",
                    order.len(),
                    order.len().saturating_sub(1),
                ));
            }
            return Ok(index);
        }
        match trimmed.to_ascii_lowercase().as_str() {
            "front" | "first" | "top" | "begin" => return Ok(0),
            "rear" | "last" | "end" | "bottom" => {
                if order.is_empty() {
                    return Err("authorization source registry is empty".to_string());
                }
                return Ok(order.len() - 1);
            }
            _ => {}
        }
        if let Some(anchor) = trimmed
            .strip_prefix("before:")
            .or_else(|| trimmed.strip_prefix("BEFORE:"))
        {
            let anchor = anchor.trim();
            if anchor.is_empty() {
                return Err("field `position` must not be empty".to_string());
            }
            return resolve_move_target(&serde_json::json!({"before": anchor}), moving, order);
        }
        if let Some(anchor) = trimmed
            .strip_prefix("after:")
            .or_else(|| trimmed.strip_prefix("AFTER:"))
        {
            let anchor = anchor.trim();
            if anchor.is_empty() {
                return Err("field `position` must not be empty".to_string());
            }
            return resolve_move_target(&serde_json::json!({"after": anchor}), moving, order);
        }
        return Err(format!(
            "field `position` {text:?} is unknown: expected an index or front/rear/before:<type>/after:<type>"
        ));
    }
    Err("field `position` must be an index or a position keyword".to_string())
}

/// `GET /authorization/sources/{type}/status`: live counters for one
/// source.
///
/// Returns 200 with the stored `type`/`enable`, the live counter-only
/// `metrics` (and its single-node `node_metrics` row) and the empty
/// `node_error` list. Health the broker does not measure (`status`,
/// `node_status`, rates, resource fields) is omitted, never invented:
/// no backend health probe runs here. Unknown types fail with
/// `NOT_FOUND` in the documented `{code, message}` shape. Unknown query
/// keys are ignored: the route is not paged, so extra keys never become
/// a 400. Status is live and does not persist (counters are memory-only
/// and restart at zero); no outbound connection is opened to decide it.
/// Management-plane read only: one short registry read plus lock-free
/// atomic loads; publish and deliver never take this store's write
/// lock. The counter set is fixed (allow/deny/total per source type,
/// at most the source cap of records).
// TODO(parity): which further status fields plus rate/resource shapes
// does the spec require? The rulebook does not decide the exact shape;
// the current choice reports the honest live subset the broker can
// supply (counter-only `metrics` plus single-node `node_metrics`,
// empty `node_error`) and omits rate, resource and status fields the
// broker does not track rather than standing a constant in for a
// measurement.
pub async fn get_authz_source_status(
    State(state): State<ApiState>,
    Path(source_type): Path<String>,
    Query(_ignored): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let Some(entry) = state.authz_sources.get(&source_type) else {
        return ApiError::NotFound(format!("authorization source {source_type:?} not found"))
            .into_response();
    };
    let counters = state
        .authz_sources
        .counter_snapshot(&source_type)
        .unwrap_or(broker_auth::AuthzSourceCounterSnapshot {
            allow: 0,
            deny: 0,
            total: 0,
        });
    let node = state.node_id.clone();
    let metrics = serde_json::json!({
        "allow": counters.allow,
        "deny": counters.deny,
        "total": counters.total,
    });
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "type": entry.source_type,
            "enable": entry.enable,
            "metrics": metrics,
            "node_metrics": [{"node": node, "metrics": metrics}],
            "node_error": [],
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_auth::{AclAction, AclRule, Authorizer};
    use broker_rules::{BackpressurePolicy, RuleEngine};

    fn standalone_state() -> ApiState {
        ApiState::standalone(std::sync::Arc::new(RuleEngine::new(
            16,
            BackpressurePolicy::DropOldest,
        )))
    }

    fn body(value: serde_json::Value) -> Bytes {
        Bytes::from(serde_json::to_vec(&value).expect("test body serialises"))
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

    fn params(page: u32, limit: u32) -> PageParams {
        PageParams { page, limit }
    }

    #[tokio::test]
    async fn list_empty_registry_create_list_duplicate_rejected() {
        // Before this task every route here was missing (bare 404).
        let state = standalone_state();

        // Default registry holds only the built-in database source.
        // The reference specification documents `{"sources": [...]}`.
        let (status, listed, _) =
            parts(list_authz_sources(State(state.clone()), params(1, 100)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            listed["sources"].as_array().expect("sources array").len(),
            1
        );
        assert_eq!(
            listed["sources"][0]["type"],
            serde_json::json!("built_in_database")
        );

        // A fresh source kind this build does not evaluate is refused
        // and stores nothing (AZ-02).
        let (status, refused, _) = parts(
            create_authz_source(
                State(state.clone()),
                body(serde_json::json!({"type": "file"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(refused["code"], serde_json::json!("BAD_REQUEST"));

        // An older stored source still reads back with its health never
        // claimed: type/enable only, no live status invented.
        state
            .authz_sources
            .insert(AuthzSourceEntry {
                source_type: "file".to_string(),
                enable: true,
                config: serde_json::Value::Null,
            })
            .expect("seed the stored source");

        // List now carries both entries in stored order.
        let (status, listed, _) =
            parts(list_authz_sources(State(state.clone()), params(1, 100)).await).await;
        assert_eq!(status, StatusCode::OK);
        let types: Vec<&str> = listed["sources"]
            .as_array()
            .expect("sources array")
            .iter()
            .map(|row| row["type"].as_str().expect("row has type"))
            .collect();
        assert_eq!(types, vec!["built_in_database", "file"]);

        // A duplicate type is rejected, never silently merged.
        let (status, duplicate, _) = parts(
            create_authz_source(
                State(state.clone()),
                body(serde_json::json!({"type": "file"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(duplicate["code"], serde_json::json!("BAD_REQUEST"));
        assert!(duplicate["message"].is_string());

        // The built-in entry itself cannot be re-registered either.
        let (status, duplicate, _) = parts(
            create_authz_source(
                State(state.clone()),
                body(serde_json::json!({"type": "built_in_database"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(duplicate["code"], serde_json::json!("BAD_REQUEST"));

        // Malformed bodies are client errors and apply nothing.
        for bad in [
            serde_json::json!({"enable": true}),
            serde_json::json!({"type": ""}),
            serde_json::json!({"type": "  "}),
            serde_json::json!({"type": 7}),
            serde_json::json!({"type": "file", "enable": "yes"}),
            serde_json::json!({"type": "file", "config": "many"}),
            serde_json::json!([]),
        ] {
            let (status, err, _) =
                parts(create_authz_source(State(state.clone()), body(bad.clone())).await).await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "body {bad} must be rejected"
            );
            assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
        }
        let (status, listed, _) =
            parts(list_authz_sources(State(state.clone()), params(1, 100)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            listed["sources"].as_array().expect("sources array").len(),
            2
        );

        // Unknown source types are 404 with the documented shape.
        let (status, unknown, _) = parts(
            create_authz_source(
                State(state.clone()),
                body(serde_json::json!({"type": "nowhere"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(unknown["code"], serde_json::json!("NOT_FOUND"));
        assert!(unknown["message"].is_string());
    }

    #[tokio::test]
    async fn catch_all_clear_empties_rules_and_reopens_publish_path() {
        // Goes through the broker, not only the store: a built-in deny
        // rule first refuses on the publish/subscribe authorization path
        // (`MemoryAuth::authorize_publish`, consulted by the publish event
        // in `crates/broker-node/src/main.rs`), and the catch-all delete
        // reopens it by emptying the same table the event consults.
        let state = standalone_state();
        state
            .auth
            .add_rule(AclRule::new(
                "w217-client",
                AclAction::Publish,
                "w217/denied",
                false,
            ))
            .expect("deny rule applies");
        state
            .auth
            .add_rule(AclRule::new(
                "w217-client",
                AclAction::Subscribe,
                "w217/denied",
                false,
            ))
            .expect("subscribe deny rule applies");
        let topic = broker_protocol::Topic::new("w217/denied").expect("topic parses");
        let filter = broker_protocol::TopicFilter::new("w217/denied").expect("filter parses");
        assert!(
            state
                .auth
                .authorize_publish("w217-client", &topic)
                .await
                .is_err(),
            "deny rule refuses on the publish path"
        );
        assert!(
            state
                .auth
                .authorize_subscribe("w217-client", &filter)
                .await
                .is_err(),
            "deny rule refuses on the subscribe path"
        );

        let response = clear_built_in_rules(State(state.clone())).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(
            state.auth.acl_rules().is_empty(),
            "catch-all empties the table"
        );
        state
            .auth
            .authorize_publish("w217-client", &topic)
            .await
            .expect("empty table allows on the publish path");
        state
            .auth
            .authorize_subscribe("w217-client", &filter)
            .await
            .expect("empty table allows on the subscribe path");

        // Clearing an already-empty table still succeeds.
        let again = clear_built_in_rules(State(state.clone())).await;
        assert_eq!(again.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn unknown_source_type_in_rule_helpers_is_404() {
        // Unknown source types referenced elsewhere return 404 with the
        // documented shape instead of serving the built-in table.
        let state = standalone_state();
        let response = crate::v5::auth::list_authz_rules(
            State(state.clone()),
            axum::extract::Path("nowhere".to_string()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let (_, shaped, _) = parts(response).await;
        assert_eq!(shaped["code"], serde_json::json!("NOT_FOUND"));
        assert!(shaped["message"].is_string());
    }

    #[tokio::test]
    async fn catch_all_replace_read_delete_round_trip_through_broker() {
        // The task's failing test: before this task the three `/all`
        // routes were missing (bare 404). Write two catch-all rules,
        // read expecting both; delete and read expecting empty. Goes
        // through the broker decision path, not only the store: the
        // written deny is observed on `authorize_publish` (consulted by
        // the publish event in `crates/broker-node/src/main.rs`), and
        // the delete reopens it.
        let state = standalone_state();

        // Empty reads as an empty collection, never an error; not paged.
        let (status, empty, _) = parts(list_catch_all_rules(State(state.clone())).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(empty["rules"], serde_json::json!([]));

        let replacement = serde_json::json!({
            "rules": [
                {"topic": "w218/alpha/#", "permission": "allow", "action": "publish"},
                {"topic": "w218/beta/#", "permission": "deny", "action": "subscribe"},
            ]
        });
        let (status, _, raw) =
            parts(replace_catch_all_rules(State(state.clone()), body(replacement)).await).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(raw.is_empty(), "replace answers 204 with no body");

        let (status, listed, _) = parts(list_catch_all_rules(State(state.clone())).await).await;
        assert_eq!(status, StatusCode::OK);
        let rules = listed["rules"].as_array().expect("rules array").clone();
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0]["topic"], serde_json::json!("w218/alpha/#"));
        assert_eq!(rules[0]["permission"], serde_json::json!("allow"));
        assert_eq!(rules[0]["action"], serde_json::json!("publish"));
        assert_eq!(rules[1]["topic"], serde_json::json!("w218/beta/#"));
        assert_eq!(rules[1]["permission"], serde_json::json!("deny"));
        assert_eq!(rules[1]["action"], serde_json::json!("subscribe"));

        // The stored deny is live on the publish/subscribe path.
        let deny_topic = broker_protocol::Topic::new("w218/alpha/data").expect("topic parses");
        state
            .auth
            .authorize_publish("any-client", &deny_topic)
            .await
            .expect("catch-all allow opens the publish path");

        // Delete clears the list and reports success; clearing empty
        // still succeeds.
        let response = clear_catch_all_rules(State(state.clone())).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let (status, cleared, _) = parts(list_catch_all_rules(State(state.clone())).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(cleared["rules"], serde_json::json!([]));
        let again = clear_catch_all_rules(State(state.clone())).await;
        assert_eq!(again.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn catch_all_replace_rejects_bad_and_applies_nothing() {
        // Validate-all-before-apply: bad rules are client errors and the
        // stored table is unchanged.
        let state = standalone_state();
        let seed = serde_json::json!({
            "rules": [
                {"topic": "w218/keep/#", "permission": "allow", "action": "all"},
            ]
        });
        let (status, _, _) =
            parts(replace_catch_all_rules(State(state.clone()), body(seed)).await).await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        let bad_bodies = [
            serde_json::json!({"rules": [
                {"topic": "w218/ok/#", "permission": "allow", "action": "publish"},
                {"topic": "", "permission": "allow", "action": "publish"},
            ]}),
            serde_json::json!({"rules": [
                {"topic": "w218/ok/#", "permission": "maybe", "action": "publish"},
            ]}),
            serde_json::json!({"rules": [
                {"topic": "w218/ok/#", "permission": "allow", "action": "forward"},
            ]}),
            serde_json::json!({"rules": [
                {"permission": "allow", "action": "publish"},
            ]}),
            serde_json::json!({"rules": [
                {"topic": "w218/bad/#/x", "permission": "allow", "action": "publish"},
            ]}),
            serde_json::json!({"nope": []}),
            serde_json::json!([]),
            serde_json::json!({"rules": "many"}),
        ];
        for bad in bad_bodies {
            let (status, err, _) =
                parts(replace_catch_all_rules(State(state.clone()), body(bad.clone())).await).await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "body {bad} must be rejected"
            );
            assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
        }
        // Over-cap is rejected the same way.
        let mut many = Vec::new();
        for i in 0..broker_auth::MAX_AUTHZ_CATCH_ALL_RULES + 1 {
            many.push(serde_json::json!({
                "topic": format!("w218/overflow/{i}/#"),
                "permission": "allow",
                "action": "publish",
            }));
        }
        let (status, err, _) = parts(
            replace_catch_all_rules(
                State(state.clone()),
                body(serde_json::json!({"rules": many})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));

        // The seeded table is unchanged by every rejected write.
        let (status, listed, _) = parts(list_catch_all_rules(State(state.clone())).await).await;
        assert_eq!(status, StatusCode::OK);
        let rules = listed["rules"].as_array().expect("rules array");
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0]["topic"], serde_json::json!("w218/keep/#"));
    }

    #[tokio::test]
    async fn catch_all_bound_and_persistence_verified() {
        // Bound: the catch-all cap is stated, finite, and enforced; the
        // default table is empty.
        assert_eq!(broker_auth::MAX_AUTHZ_CATCH_ALL_RULES, 128);
        let fresh = standalone_state();
        let (status, empty, _) = parts(list_catch_all_rules(State(fresh.clone())).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(empty["rules"], serde_json::json!([]));

        // Persistence: a replace through a registry-backed table survives
        // a reload of the same data dir.
        let dir = std::env::temp_dir().join(format!(
            "authz-catch-all-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let registry = std::sync::Arc::new(
            broker_config::ConfigRegistry::load(&dir).expect("fresh dir loads"),
        );
        let auth = std::sync::Arc::new(broker_auth::MemoryAuth::from_registry(&registry));
        auth.replace_rules(vec![broker_auth::AclRule::new(
            "*",
            broker_auth::AclAction::Publish,
            "w218/persist/#",
            true,
        )])
        .expect("registry-backed replace applies");
        assert!(dir.join(broker_config::STATE_FILE_NAME).is_file());
        drop(auth);
        drop(registry);
        let reloaded =
            std::sync::Arc::new(broker_config::ConfigRegistry::load(&dir).expect("dir reloads"));
        let restarted = broker_auth::MemoryAuth::from_registry(&reloaded);
        let rules = restarted.acl_rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].topic_pattern, "w218/persist/#");
        std::fs::remove_dir_all(&dir).ok();
    }

    fn order_body(types: &[&str]) -> Bytes {
        body(
            types
                .iter()
                .map(|t| serde_json::json!({"type": t}))
                .collect::<Vec<_>>()
                .into(),
        )
    }

    async fn put_order(
        state: &ApiState,
        payload: Bytes,
    ) -> (StatusCode, serde_json::Value, Vec<u8>) {
        parts(replace_authz_order(State(state.clone()), payload).await).await
    }

    async fn list_types(state: &ApiState) -> Vec<String> {
        let (_, listed, _) =
            parts(list_authz_sources(State(state.clone()), params(1, 100)).await).await;
        listed["sources"]
            .as_array()
            .expect("sources array")
            .iter()
            .map(|row| row["type"].as_str().expect("row has type").to_string())
            .collect()
    }

    #[tokio::test]
    async fn order_replace_round_trip_rejects_bad_without_change() {
        // The task's failing test: before this task the order route was
        // missing (bare 404). Register a second source, swap the whole
        // evaluation order and read it back; a bad order naming an
        // unknown type is rejected with the stored order unchanged.
        // The stored order gates the publish/subscribe consult from the
        // next verdict (a disabled or removed built-in source denies
        // fail closed).
        let state = standalone_state();
        assert_eq!(
            list_types(&state).await,
            vec!["built_in_database".to_string()]
        );

        state
            .authz_sources
            .insert(AuthzSourceEntry {
                source_type: "file".to_string(),
                enable: true,
                config: serde_json::Value::Null,
            })
            .expect("seed the stored source");

        let (status, _, raw) = put_order(&state, order_body(&["file", "built_in_database"])).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(raw.is_empty(), "reorder answers 204 with no body");
        assert_eq!(
            list_types(&state).await,
            vec!["file".to_string(), "built_in_database".to_string()]
        );

        // Every bad order is a client error and the stored order is
        // unchanged: unknown type, partial list, duplicate type, missing
        // key, empty type, wrong envelope.
        let bad_bodies = [
            order_body(&["built_in_database", "nowhere"]),
            order_body(&["nowhere"]),
            order_body(&["built_in_database"]),
            order_body(&["file", "file", "built_in_database"]),
            order_body(&["file", "built_in_database", "built_in_database"]),
            body(serde_json::json!([{"nope": "file"}, {"type": "built_in_database"}])),
            body(serde_json::json!([{"type": ""}, {"type": "built_in_database"}])),
            body(serde_json::json!({"type": "file"})),
            body(serde_json::json!([])),
        ];
        for bad in bad_bodies {
            let (status, err, _) = put_order(&state, bad.clone()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "order must be rejected");
            assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
            assert!(err["message"].is_string());
        }
        assert_eq!(
            list_types(&state).await,
            vec!["file".to_string(), "built_in_database".to_string()]
        );

        // Restoring the original order succeeds.
        let (status, _, _) = put_order(&state, order_body(&["built_in_database", "file"])).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert_eq!(
            list_types(&state).await,
            vec!["built_in_database".to_string(), "file".to_string()]
        );
    }

    #[tokio::test]
    async fn source_delete_hit_miss_refuses_last() {
        // The task's failing test: before this task the per-type delete
        // was missing (bare 404). Delete a second source and read the
        // list back; an unknown type is 404; deleting the last live
        // source is refused instead of leaving authorization open.
        let state = standalone_state();
        state
            .authz_sources
            .insert(AuthzSourceEntry {
                source_type: "file".to_string(),
                enable: true,
                config: serde_json::Value::Null,
            })
            .expect("seed the stored source");

        let response = delete_authz_source(State(state.clone()), Path("file".to_string())).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            list_types(&state).await,
            vec!["built_in_database".to_string()]
        );

        // Unknown types return 404 with the documented shape.
        let (status, missing, _) =
            parts(delete_authz_source(State(state.clone()), Path("nowhere".to_string())).await)
                .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], serde_json::json!("NOT_FOUND"));
        assert!(missing["message"].is_string());

        // Deleting the last live source is refused cleanly; the stored
        // source is unchanged.
        let (status, refused, _) = parts(
            delete_authz_source(State(state.clone()), Path("built_in_database".to_string())).await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(refused["code"], serde_json::json!("BAD_REQUEST"));
        assert!(refused["message"].is_string());
        assert_eq!(
            list_types(&state).await,
            vec!["built_in_database".to_string()]
        );
    }

    #[tokio::test]
    async fn source_detail_read_update_reset_round_trip_through_broker() {
        // The task's failing test: before this task the per-type read,
        // update and metrics reset were missing (bare 404). Create a
        // source, read it, update with a valid config and read back;
        // move counters then reset and verify zeroed; an unknown type
        // reads 404. Goes through the broker, not only the store: the
        // stored built-in source is observed on `authorize_publish`
        // (consulted by the publish event in
        // `crates/broker-node/src/main.rs`) while the detail read
        // reports the same stored source.
        let state = standalone_state();
        state
            .authz_sources
            .insert(AuthzSourceEntry {
                source_type: "file".to_string(),
                enable: true,
                config: serde_json::json!({"path": "/etc/authz/rules.conf"}),
            })
            .expect("seed the stored source");

        // Read hits with the stored config and live counters; health
        // the broker does not measure is omitted, never invented.
        let (status, detail, _) =
            parts(get_authz_source(State(state.clone()), Path("file".to_string())).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(detail["type"], serde_json::json!("file"));
        assert_eq!(detail["enable"], serde_json::json!(true));
        assert_eq!(
            detail["config"]["path"],
            serde_json::json!("/etc/authz/rules.conf")
        );
        assert!(detail.get("status").is_none());
        assert!(detail.get("node_status").is_none());
        assert_eq!(detail["metrics"]["allow"], serde_json::json!(0));
        assert_eq!(detail["metrics"]["deny"], serde_json::json!(0));
        assert_eq!(detail["metrics"]["total"], serde_json::json!(0));

        // The built-in entry reports its stored keys with live counters.
        let (status, builtin, _) = parts(
            get_authz_source(State(state.clone()), Path("built_in_database".to_string())).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(builtin["type"], serde_json::json!("built_in_database"));
        assert!(builtin.get("status").is_none());

        // The publish proof below covers the built-in table. Disable the
        // unsupported stored source so the chain has no enabled source
        // this build does not evaluate (D2).
        state
            .authz_sources
            .update(
                "file",
                AuthzSourceEntry {
                    source_type: "file".to_string(),
                    enable: false,
                    config: serde_json::json!({"path": "/etc/authz/rules.conf"}),
                },
            )
            .expect("disable the unsupported source for the built-in proof");

        // The stored built-in source is live on the publish path: the
        // empty table allows, and the verdict attributes to the
        // built-in counters (not only to explicit store records).
        let topic = broker_protocol::Topic::new("w223/data").expect("topic parses");
        state
            .auth
            .authorize_publish("any-client", &topic)
            .await
            .expect("empty table allows on the publish path");
        let broker_counted = state
            .authz_sources
            .counter_snapshot("built_in_database")
            .expect("broker verdict attributes to built-in counters");
        assert_eq!(broker_counted.allow, 1);
        assert_eq!(broker_counted.total, 1);

        // The stored order and enable flags gate the publish path: a
        // disabled built-in source denies fail closed and counts the
        // denial.
        state
            .authz_sources
            .update(
                "built_in_database",
                broker_auth::AuthzSourceEntry {
                    source_type: "built_in_database".to_string(),
                    enable: false,
                    config: serde_json::Value::Null,
                },
            )
            .expect("disable applies");
        assert!(
            state
                .auth
                .authorize_publish("any-client", &topic)
                .await
                .is_err(),
            "disabled built-in denies on the publish path"
        );
        let gated = state
            .authz_sources
            .counter_snapshot("built_in_database")
            .expect("gated denial attributes to built-in counters");
        assert_eq!(gated.deny, 1);
        assert_eq!(gated.total, 2);
        state
            .authz_sources
            .update(
                "built_in_database",
                broker_auth::AuthzSourceEntry {
                    source_type: "built_in_database".to_string(),
                    enable: true,
                    config: serde_json::Value::Null,
                },
            )
            .expect("re-enable applies");

        // Restore the stored source for the detail and metric checks.
        state
            .authz_sources
            .update(
                "file",
                AuthzSourceEntry {
                    source_type: "file".to_string(),
                    enable: true,
                    config: serde_json::json!({"path": "/etc/authz/rules.conf"}),
                },
            )
            .expect("re-enable the stored source");

        // A refused update of an unsupported kind keeps the stored source
        // unchanged (AZ-02).
        let (status, refused, _) = parts(
            put_authz_source(
                State(state.clone()),
                Path("file".to_string()),
                body(serde_json::json!({
                    "enable": false,
                    "config": {"path": "/etc/authz/other.conf"},
                })),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(refused["code"], serde_json::json!("BAD_REQUEST"));
        assert!(
            refused["message"]
                .as_str()
                .is_some_and(|message| message.contains("file")),
            "the refusal names the kind: {refused}"
        );
        let (status, reread, _) =
            parts(get_authz_source(State(state.clone()), Path("file".to_string())).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reread["enable"], serde_json::json!(true));
        assert_eq!(
            reread["config"]["path"],
            serde_json::json!("/etc/authz/rules.conf")
        );
        assert!(reread.get("status").is_none());

        // Move counters then reset and verify zeroed.
        state.authz_sources.record_allow("file");
        state.authz_sources.record_allow("file");
        state.authz_sources.record_deny("file");
        let (status, moved, _) =
            parts(get_authz_source(State(state.clone()), Path("file".to_string())).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(moved["metrics"]["allow"], serde_json::json!(2));
        assert_eq!(moved["metrics"]["deny"], serde_json::json!(1));
        assert_eq!(moved["metrics"]["total"], serde_json::json!(3));

        let response =
            reset_authz_source_metrics(State(state.clone()), Path("file".to_string())).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let (status, zeroed, _) =
            parts(get_authz_source(State(state.clone()), Path("file".to_string())).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(zeroed["metrics"]["allow"], serde_json::json!(0));
        assert_eq!(zeroed["metrics"]["deny"], serde_json::json!(0));
        assert_eq!(zeroed["metrics"]["total"], serde_json::json!(0));

        // Counters resume from zero on the next verdict.
        state.authz_sources.record_deny("file");
        let (status, resumed, _) =
            parts(get_authz_source(State(state.clone()), Path("file".to_string())).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(resumed["metrics"]["deny"], serde_json::json!(1));
        assert_eq!(resumed["metrics"]["total"], serde_json::json!(1));

        // Unknown types miss on all three routes with the documented
        // shape; a known unavailable kind is refused before storage
        // (AZ-02).
        let (status, missing, _) =
            parts(get_authz_source(State(state.clone()), Path("nowhere".to_string())).await).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], serde_json::json!("NOT_FOUND"));
        assert!(missing["message"].is_string());
        let (status, missing, _) = parts(
            put_authz_source(
                State(state.clone()),
                Path("nowhere".to_string()),
                body(serde_json::json!({"enable": true})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], serde_json::json!("NOT_FOUND"));
        assert!(missing["message"].is_string());
        let (status, missing, _) = parts(
            reset_authz_source_metrics(State(state.clone()), Path("nowhere".to_string())).await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], serde_json::json!("NOT_FOUND"));
        assert!(missing["message"].is_string());
    }

    #[tokio::test]
    async fn source_detail_update_rejects_bad_and_applies_nothing() {
        // Validate-all-before-apply: bad configs are client errors and
        // the stored entry is unchanged.
        let state = standalone_state();
        state
            .authz_sources
            .insert(AuthzSourceEntry {
                source_type: "file".to_string(),
                enable: true,
                config: serde_json::json!({"path": "/etc/authz/keep.conf"}),
            })
            .expect("seed the stored source");

        let bad_bodies = [
            // A body `type` that differs from the path type is rejected.
            serde_json::json!({"type": "http", "enable": true}),
            serde_json::json!({"type": 7}),
            serde_json::json!({"enable": "yes"}),
            serde_json::json!({"config": "many"}),
            serde_json::json!({"config": ["many"]}),
            serde_json::json!([]),
        ];
        for bad in bad_bodies {
            let (status, err, _) = parts(
                put_authz_source(
                    State(state.clone()),
                    Path("file".to_string()),
                    body(bad.clone()),
                )
                .await,
            )
            .await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "body {bad} must be rejected"
            );
            assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
            assert!(err["message"].is_string());
        }

        // The seeded entry is unchanged by every rejected write.
        let (status, kept, _) =
            parts(get_authz_source(State(state.clone()), Path("file".to_string())).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(kept["enable"], serde_json::json!(true));
        assert_eq!(
            kept["config"]["path"],
            serde_json::json!("/etc/authz/keep.conf")
        );

        // A body carrying only the stored `type` is refused for an
        // unavailable kind and keeps the stored values.
        let (status, refused, _) = parts(
            put_authz_source(
                State(state.clone()),
                Path("file".to_string()),
                body(serde_json::json!({"type": "file"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(refused["code"], serde_json::json!("BAD_REQUEST"));
        let (status, same, _) =
            parts(get_authz_source(State(state.clone()), Path("file".to_string())).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(same["enable"], serde_json::json!(true));
        assert_eq!(
            same["config"]["path"],
            serde_json::json!("/etc/authz/keep.conf")
        );
    }

    #[tokio::test]
    async fn source_detail_update_persists_config_but_not_counters() {
        // Bound: the source cap is stated, finite, and enforced; the
        // counter set is fixed (allow/deny/total per type, at most the
        // capped records).
        assert_eq!(broker_config::MAX_AUTHZ_SOURCES, 8);

        // Persistence: an update through a registry-backed registry
        // survives a reload of the same data dir, while counters restart
        // at zero.
        let dir = std::env::temp_dir().join(format!(
            "authz-source-detail-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let registry = std::sync::Arc::new(
            broker_config::ConfigRegistry::load(&dir).expect("fresh dir loads"),
        );
        let sources = broker_auth::AuthzSourceRegistry::from_registry(&registry);
        sources
            .insert(broker_auth::AuthzSourceEntry {
                source_type: "file".to_string(),
                enable: true,
                config: serde_json::json!({"path": "/etc/authz/before.conf"}),
            })
            .expect("persist file");
        sources
            .update(
                "file",
                broker_auth::AuthzSourceEntry {
                    source_type: "file".to_string(),
                    enable: false,
                    config: serde_json::json!({"path": "/etc/authz/after.conf"}),
                },
            )
            .expect("persist update applies");
        sources.record_allow("file");
        sources.record_deny("file");
        assert_eq!(
            sources
                .counter_snapshot("file")
                .expect("counters move before restart")
                .total,
            2
        );
        assert!(dir.join(broker_config::STATE_FILE_NAME).is_file());
        drop(sources);
        drop(registry);
        let reloaded =
            std::sync::Arc::new(broker_config::ConfigRegistry::load(&dir).expect("dir reloads"));
        let restarted = broker_auth::AuthzSourceRegistry::from_registry(&reloaded);
        let stored = restarted.get("file").expect("updated source persists");
        assert!(!stored.enable);
        assert_eq!(
            stored.config["path"],
            serde_json::json!("/etc/authz/after.conf")
        );
        // Counters do not persist: the restarted source reads at zero.
        let counters = restarted
            .counter_snapshot("file")
            .expect("counter record exists after restart");
        assert_eq!(counters.allow, 0);
        assert_eq!(counters.deny, 0);
        assert_eq!(counters.total, 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn order_and_delete_persist_across_restart() {
        // Bound: the source cap is stated, finite, and enforced; the
        // default registry holds only the built-in source.
        assert_eq!(broker_config::MAX_AUTHZ_SOURCES, 8);
        let fresh = standalone_state();
        assert_eq!(
            list_types(&fresh).await,
            vec!["built_in_database".to_string()]
        );

        // Persistence: a reorder plus a delete through a registry-backed
        // registry survive a reload of the same data dir in the new order.
        let dir = std::env::temp_dir().join(format!(
            "authz-order-delete-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let registry = std::sync::Arc::new(
            broker_config::ConfigRegistry::load(&dir).expect("fresh dir loads"),
        );
        let sources = broker_auth::AuthzSourceRegistry::from_registry(&registry);
        sources
            .insert(broker_auth::AuthzSourceEntry {
                source_type: "file".to_string(),
                enable: true,
                config: serde_json::Value::Null,
            })
            .expect("persist file");
        sources
            .reorder(vec!["file".to_string(), "built_in_database".to_string()])
            .expect("persist reorder applies");
        sources.remove("built_in_database").expect("delete applies");
        assert!(dir.join(broker_config::STATE_FILE_NAME).is_file());
        drop(sources);
        drop(registry);
        let reloaded =
            std::sync::Arc::new(broker_config::ConfigRegistry::load(&dir).expect("dir reloads"));
        let restarted = broker_auth::AuthzSourceRegistry::from_registry(&reloaded);
        let ids: Vec<_> = restarted
            .list()
            .iter()
            .map(|e| e.source_type.clone())
            .collect();
        assert_eq!(ids, vec!["file".to_string()]);
        std::fs::remove_dir_all(&dir).ok();
    }

    async fn move_call(
        state: &ApiState,
        source_type: &str,
        payload: serde_json::Value,
    ) -> (StatusCode, serde_json::Value, Vec<u8>) {
        parts(
            move_authz_source(
                State(state.clone()),
                Path(source_type.to_string()),
                body(payload),
            )
            .await,
        )
        .await
    }

    async fn status_call(
        state: &ApiState,
        source_type: &str,
    ) -> (StatusCode, serde_json::Value, Vec<u8>) {
        parts(
            get_authz_source_status(
                State(state.clone()),
                Path(source_type.to_string()),
                Query(Default::default()),
            )
            .await,
        )
        .await
    }

    #[tokio::test]
    async fn source_move_round_trip_and_status_shape_through_broker() {
        // The task's failing test: before this task both routes were
        // missing (bare 404). Create two sources, move the second before
        // the first and list order expecting the change; read status
        // expecting the documented fields. Goes through the broker, not
        // only the store: the stored built-in source is observed on
        // `authorize_publish` (consulted by the publish event in
        // `crates/broker-node/src/main.rs`) while the move reports and
        // the status reads the same stored sources.
        let state = standalone_state();
        for source_type in ["file", "http"] {
            state
                .authz_sources
                .insert(AuthzSourceEntry {
                    source_type: source_type.to_string(),
                    enable: true,
                    config: serde_json::Value::Null,
                })
                .expect("seed the stored source");
        }
        assert_eq!(
            list_types(&state).await,
            vec![
                "built_in_database".to_string(),
                "file".to_string(),
                "http".to_string(),
            ]
        );

        // Move the second stored source before the first via the anchor
        // shape. The answer is 204 with no body; the list shows the new
        // order.
        let (status, moved, raw) = move_call(
            &state,
            "file",
            serde_json::json!({"before": "built_in_database"}),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(moved.is_null() && raw.is_empty(), "move returns no body");
        let moved_types = list_types(&state).await;
        assert_eq!(
            moved_types,
            vec![
                "file".to_string(),
                "built_in_database".to_string(),
                "http".to_string(),
            ]
        );

        // The numeric-index shape moves back; the keyword shape reaches
        // the front of the list.
        let (status, _, _) = move_call(&state, "file", serde_json::json!({"position": 1})).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert_eq!(list_types(&state).await[1], "file");
        let (status, _, _) = move_call(
            &state,
            "built_in_database",
            serde_json::json!({"position": "front"}),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert_eq!(list_types(&state).await[0], "built_in_database");

        // Status carries the documented fields: identity plus live
        // counters. Health the broker does not measure is omitted.
        let (status, shaped, _) = status_call(&state, "file").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(shaped["type"], serde_json::json!("file"));
        assert_eq!(shaped["enable"], serde_json::json!(true));
        assert!(shaped.get("status").is_none());
        assert!(shaped.get("node_status").is_none());
        assert_eq!(shaped["metrics"]["allow"], serde_json::json!(0));
        assert_eq!(shaped["metrics"]["deny"], serde_json::json!(0));
        assert_eq!(shaped["metrics"]["total"], serde_json::json!(0));
        assert_eq!(
            shaped["node_metrics"][0]["metrics"]["total"],
            shaped["metrics"]["total"]
        );
        assert_eq!(shaped["node_error"], serde_json::json!([]));

        // The built-in entry reports its stored keys with live counters.
        let (status, builtin, _) = status_call(&state, "built_in_database").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(builtin["type"], serde_json::json!("built_in_database"));
        assert!(builtin.get("status").is_none());

        // Unknown query keys never become a 400 on the status read.
        let (status, _, _) = parts(
            get_authz_source_status(
                State(state.clone()),
                Path("file".to_string()),
                Query(
                    [("unknown_param".to_string(), "1".to_string())]
                        .into_iter()
                        .collect(),
                ),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        // The publish proof below covers the built-in table. Disable the
        // unsupported stored sources so the chain has no enabled source
        // this build does not evaluate (D2).
        for source_type in ["file", "http"] {
            state
                .authz_sources
                .update(
                    source_type,
                    AuthzSourceEntry {
                        source_type: source_type.to_string(),
                        enable: false,
                        config: serde_json::Value::Null,
                    },
                )
                .expect("disable the unsupported source for the built-in proof");
        }

        // The stored built-in source is live on the publish path: the
        // empty table allows, and the verdict attributes to the
        // built-in counters.
        let topic = broker_protocol::Topic::new("w224/data").expect("topic parses");
        state
            .auth
            .authorize_publish("any-client", &topic)
            .await
            .expect("empty table allows on the publish path");
        let broker_counted = state
            .authz_sources
            .counter_snapshot("built_in_database")
            .expect("broker verdict attributes to built-in counters");
        assert_eq!(broker_counted.total, 1);

        // Restore the stored sources for the counter checks.
        for source_type in ["file", "http"] {
            state
                .authz_sources
                .update(
                    source_type,
                    AuthzSourceEntry {
                        source_type: source_type.to_string(),
                        enable: true,
                        config: serde_json::Value::Null,
                    },
                )
                .expect("re-enable the stored source");
        }

        // Counters move on explicit records and read back on status.
        state.authz_sources.record_allow("file");
        state.authz_sources.record_deny("file");
        let (status, moved, _) = status_call(&state, "file").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(moved["metrics"]["allow"], serde_json::json!(1));
        assert_eq!(moved["metrics"]["deny"], serde_json::json!(1));
        assert_eq!(moved["metrics"]["total"], serde_json::json!(2));
    }

    #[tokio::test]
    async fn source_move_rejects_bad_without_change_and_unknown_is_404() {
        // Bounds-checked reorder: bad positions are client errors and
        // move nothing; unknown types are 404 with the documented shape.
        let state = standalone_state();
        state
            .authz_sources
            .insert(AuthzSourceEntry {
                source_type: "file".to_string(),
                enable: true,
                config: serde_json::Value::Null,
            })
            .expect("seed the stored source");
        let before = list_types(&state).await;

        let bad_bodies = [
            serde_json::json!({"position": 99}),
            serde_json::json!({"position": "99"}),
            serde_json::json!({"position": "middle"}),
            serde_json::json!({"position": "before:nowhere"}),
            serde_json::json!({"position": "after:nowhere"}),
            serde_json::json!({"before": "nowhere"}),
            serde_json::json!({"after": "nowhere"}),
            serde_json::json!({"before": ""}),
            serde_json::json!({"after": 7}),
            serde_json::json!({"position": true}),
            serde_json::json!({"elsewhere": "front"}),
            serde_json::json!([]),
        ];
        for bad in bad_bodies {
            let (status, err, _) = move_call(&state, "file", bad.clone()).await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "body {bad} must be rejected"
            );
            assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
            assert!(err["message"].is_string());
        }
        assert_eq!(list_types(&state).await, before);

        // Unknown types miss on both routes with the documented shape.
        let (status, missing, _) =
            move_call(&state, "nowhere", serde_json::json!({"position": "front"})).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], serde_json::json!("NOT_FOUND"));
        assert!(missing["message"].is_string());
        let (status, missing, _) = status_call(&state, "nowhere").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], serde_json::json!("NOT_FOUND"));
        assert!(missing["message"].is_string());
        assert_eq!(list_types(&state).await, before);
    }

    #[tokio::test]
    async fn source_move_persists_order_but_not_counters() {
        // Bound: the source cap is stated, finite, and enforced; the
        // counter set is fixed (allow/deny/total per type, at most the
        // capped records).
        assert_eq!(broker_config::MAX_AUTHZ_SOURCES, 8);

        // Persistence: a move through a registry-backed registry survives
        // a reload of the same data dir in the new order, while counters
        // restart at zero.
        let dir = std::env::temp_dir().join(format!(
            "authz-source-move-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let registry = std::sync::Arc::new(
            broker_config::ConfigRegistry::load(&dir).expect("fresh dir loads"),
        );
        let sources = broker_auth::AuthzSourceRegistry::from_registry(&registry);
        sources
            .insert(broker_auth::AuthzSourceEntry {
                source_type: "file".to_string(),
                enable: true,
                config: serde_json::Value::Null,
            })
            .expect("persist file");
        sources.relocate("file", 0).expect("persist move applies");
        sources.record_allow("file");
        assert_eq!(
            sources
                .counter_snapshot("file")
                .expect("counters move before restart")
                .total,
            1
        );
        assert!(dir.join(broker_config::STATE_FILE_NAME).is_file());
        drop(sources);
        drop(registry);
        let reloaded =
            std::sync::Arc::new(broker_config::ConfigRegistry::load(&dir).expect("dir reloads"));
        let restarted = broker_auth::AuthzSourceRegistry::from_registry(&reloaded);
        let ids: Vec<_> = restarted
            .list()
            .iter()
            .map(|e| e.source_type.clone())
            .collect();
        assert_eq!(
            ids,
            vec!["file".to_string(), "built_in_database".to_string()]
        );
        // Counters do not persist: the restarted source reads at zero.
        let counters = restarted
            .counter_snapshot("file")
            .expect("counter record exists after restart");
        assert_eq!(counters.allow, 0);
        assert_eq!(counters.deny, 0);
        assert_eq!(counters.total, 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn create_and_read_round_trip_each_documented_variant() {
        // A fresh kind this build does not evaluate is refused; an older
        // stored variant keeps its required fields through the read. The
        // row reports them flat at the top level, so the answer matches
        // the documented variant.
        let state = standalone_state();
        let cases: Vec<serde_json::Value> = vec![
            serde_json::json!({
                "type": "redis",
                "redis_type": "single",
                "server": "127.0.0.1:6379",
                "cmd": "HGETALL mqtt_acl:${username}",
            }),
            serde_json::json!({
                "type": "ldap",
                "server": "127.0.0.1:389",
                "base_dn": "ou=users,dc=example,dc=org",
                "username": "cn=reader",
            }),
            serde_json::json!({
                "type": "http",
                "method": "post",
                "url": "http://127.0.0.1:8080/acl",
            }),
            serde_json::json!({
                "type": "file",
                "rules": "{allow, all}.",
            }),
            serde_json::json!({
                "type": "mysql",
                "server": "127.0.0.1:3306",
                "database": "mqtt",
                "query": "SELECT action, permission, topic FROM acl WHERE username = ${username}",
            }),
            serde_json::json!({
                "type": "postgresql",
                "server": "127.0.0.1:5432",
                "database": "mqtt",
                "username": "root",
                "query": "SELECT action, permission, topic FROM acl WHERE username = ${username}",
            }),
        ];
        for sent in &cases {
            let source_type = sent["type"].as_str().expect("case names a type");
            // A fresh kind this build does not evaluate is refused and
            // stores nothing (AZ-02).
            let (status, refused, _) =
                parts(create_authz_source(State(state.clone()), body(sent.clone())).await).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "variant {source_type}");
            assert_eq!(refused["code"], serde_json::json!("BAD_REQUEST"));
            // An older stored variant still reads back its flat fields.
            let mut config = sent.as_object().expect("case is an object").clone();
            config.remove("type");
            config.remove("enable");
            config.remove("config");
            state
                .authz_sources
                .insert(AuthzSourceEntry {
                    source_type: source_type.to_string(),
                    enable: true,
                    config: serde_json::Value::Object(config),
                })
                .expect("seed the stored variant");
            let (status, read, _) =
                parts(get_authz_source(State(state.clone()), Path(source_type.to_string())).await)
                    .await;
            assert_eq!(status, StatusCode::OK);
            for (key, want) in sent.as_object().expect("case is an object") {
                assert_eq!(&read[key], want, "variant field {key} round-trips");
            }
        }
    }
}
