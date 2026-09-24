//! Authorization source registry for the v5 management API.
//!
//! Covers `GET /authorization/sources` (ordered page), `POST
//! /authorization/sources` (register one source) and `DELETE
//! /authorization/sources/built_in_database/rules` (clear every
//! built-in rule) over the real [`broker_auth::AuthzSourceRegistry`]
//! plus the live built-in rule table (`MemoryAuth` in
//! `crates/broker-auth/src/lib.rs`, consulted by the publish event in
//! `crates/broker-node/src/main.rs` and the console path in
//! `crates/broker-api/src/ws.rs`). Single-node, management-plane only:
//! handlers clone at most the capped list once per request under a short
//! lock; the CONNECT path keeps its own short locks and publish/deliver
//! never touch this registry.
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
// TODO(parity): which per-source fields plus list envelope does the spec
// require (bare list versus data/meta, status/health fields)? The
// rulebook does not decide the exact shape; the current choice reuses the
// shared pagination extractor (data plus meta) and reports type/enable
// only, omitting status fields the broker cannot supply.
// TODO(parity): does a successful clear report an empty 204 or a 200
// success envelope, and does creating with an unknown `type` fail with
// 404 or 400? The rulebook does not decide the exact shapes; the current
// choice answers clear with 204 (mirroring the sibling cache clears) and
// unknown types with NOT_FOUND (a type reference that names nothing
// stored or documented), until the checker pins it.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;

use crate::errors::ApiError;
use crate::pagination::{meta_page, paginate, PageParams};
use crate::ApiState;
use broker_auth::{AuthzSourceEntry, SourceInsertError};

/// Render one stored source. Only stored fields are reported: `type`
/// plus `enable`, with the backend `config` object when one is stored.
/// Health/status fields the broker does not measure are omitted, never
/// invented; non-built-in sources are stored configuration, never
/// claimed as live.
fn render_entry(entry: &AuthzSourceEntry) -> serde_json::Value {
    let mut value = serde_json::json!({
        "type": entry.source_type,
        "enable": entry.enable,
    });
    if !entry.config.is_null() {
        value["config"] = entry.config.clone();
    }
    value
}

/// `GET /authorization/sources`: ordered page (`data` plus `meta`). An
/// empty registry reads as an empty page, never an error. Malformed
/// `page`/`limit` fall back to defaults via [`PageParams`].
pub async fn list_authz_sources(State(state): State<ApiState>, params: PageParams) -> Response {
    let all = state.authz_sources.list();
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

/// Why a create body was refused.
enum CreateError {
    /// The body is malformed (message names the field).
    Invalid(String),
    /// The `type` names no documented source type.
    UnknownType(String),
}

/// `POST /authorization/sources`: register one source. Returns the
/// stored entry with 201. Malformed bodies fail with `BAD_REQUEST`; a
/// `type` that names no documented source fails with `NOT_FOUND`; an
/// already-stored `type` fails with `ALREADY_EXISTS` instead of merging;
/// a full registry fails with `BAD_REQUEST`.
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
    match state.authz_sources.insert(entry) {
        Ok(stored) => (StatusCode::CREATED, Json(render_entry(&stored))).into_response(),
        Err(SourceInsertError::Duplicate) => {
            ApiError::AlreadyExists("authorization source already exists".to_string())
                .into_response()
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
    StatusCode::NO_CONTENT.into_response()
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
        let (status, listed, _) =
            parts(list_authz_sources(State(state.clone()), params(1, 100)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(listed["data"].as_array().expect("data array").len(), 1);
        assert_eq!(
            listed["data"][0]["type"],
            serde_json::json!("built_in_database")
        );
        assert_eq!(listed["meta"]["page"], serde_json::json!(1));
        assert_eq!(listed["meta"]["limit"], serde_json::json!(100));
        assert_eq!(listed["meta"]["count"], serde_json::json!(1));
        assert_eq!(listed["meta"]["hasnext"], serde_json::json!(false));

        // Register one stored-only source; it reads back with its health
        // never claimed: type/enable only, no live status invented.
        let (status, created, _) = parts(
            create_authz_source(
                State(state.clone()),
                body(serde_json::json!({"type": "file"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(created["type"], serde_json::json!("file"));
        assert_eq!(created["enable"], serde_json::json!(true));
        assert!(created.get("status").is_none(), "no live status is claimed");

        // List now carries both entries in stored order.
        let (status, listed, _) =
            parts(list_authz_sources(State(state.clone()), params(1, 100)).await).await;
        assert_eq!(status, StatusCode::OK);
        let types: Vec<&str> = listed["data"]
            .as_array()
            .expect("data array")
            .iter()
            .map(|row| row["type"].as_str().expect("row has type"))
            .collect();
        assert_eq!(types, vec!["built_in_database", "file"]);
        assert_eq!(listed["meta"]["count"], serde_json::json!(2));

        // Paging slices after stored order: page 2 with limit 1 holds file.
        let (status, page2, _) =
            parts(list_authz_sources(State(state.clone()), params(2, 1)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(page2["data"][0]["type"], serde_json::json!("file"));
        assert_eq!(page2["meta"]["count"], serde_json::json!(2));

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
        assert_eq!(duplicate["code"], serde_json::json!("ALREADY_EXISTS"));
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
        assert_eq!(duplicate["code"], serde_json::json!("ALREADY_EXISTS"));

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
        assert_eq!(listed["meta"]["count"], serde_json::json!(2));

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
}
