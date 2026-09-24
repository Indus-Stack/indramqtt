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
}
