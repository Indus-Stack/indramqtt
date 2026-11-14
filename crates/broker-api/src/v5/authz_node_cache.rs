//! Node-level authorization cache configuration for the v5 API.
//!
//! Covers `GET /authorization/node_cache` (read), `PUT
//! /authorization/node_cache` (validated full replace) and `POST
//! /authorization/node_cache/reset` (evict) over the real
//! [`broker_auth::AuthzNodeCache`]. Single-node, management-plane only:
//! handlers clone one small snapshot per request under no delivery lock;
//! the kernel boot path seeds the same instance from the config registry
//! (`crates/broker-node/src/main.rs`, boot seeding) so reads observe the
//! persisted policy without a restart; publish and deliver never touch
//! this store.
//!
//! Neither route is paged. Error answers use the documented
//! `{code, message}` shape.
//!
//! Cached verdicts themselves live in the per-session decision caches
//! (`crates/broker-session/src/lib.rs`, `Session::record_authz_decision`,
//! driven by the publish and subscribe authorization events); the reset
//! evicts every entry on every session on this node so subsequent
//! decisions are recomputed. Evicting an already-empty cache still
//! succeeds. Only the configuration persists across a restart, via the
//! config registry.
// TODO(parity): does the documented PUT merge missing fields or reset
// them to defaults, and does the documented reset report anything beyond
// an empty success? The rulebook does not decide the merge or reset
// shape; the current choice replaces missing fields with defaults (full
// replace, mirroring the neighbouring settings route) and answers PUT
// and reset with 204 and no body.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use bytes::Bytes;

use crate::errors::ApiError;
use crate::ApiState;
use broker_auth::AuthzCacheUpdateError;
use broker_config::AuthzNodeCacheConf;

pub use broker_auth::AuthzNodeCache;

/// `GET /authorization/node_cache`: current node-cache policy.
///
/// One lock-free snapshot and one small JSON clone per request; no
/// delivery locks, no new buffering. Unknown query keys are ignored: the
/// handler takes no paging params so extra keys never become a 400.
pub async fn get_authz_node_cache(
    State(state): State<ApiState>,
    Query(_ignored): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let snapshot = state.authz_node_cache.snapshot();
    match serde_json::to_value(&*snapshot) {
        Ok(body) => (StatusCode::OK, axum::Json(body)).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot render authorization node cache: {error}"),
            })),
        )
            .into_response(),
    }
}

/// `PUT /authorization/node_cache`: validated full replace.
///
/// The whole body is validated before anything is applied: one bad field
/// rejects the entire write with the documented `BAD_REQUEST` shape and
/// leaves the stored policy untouched. Unknown fields are rejected
/// fail-closed; they never degrade to the known subset. Missing fields
/// reset to their documented defaults: the body replaces the stored
/// policy, it never merges into it. Unknown query parameters are
/// rejected fail-closed. On success returns 204 with no body and the
/// policy persists across a restart through the config registry.
/// Management-plane only: one snapshot swap per request; publish and
/// deliver never touch this store.
pub async fn put_authz_node_cache(
    State(state): State<ApiState>,
    Query(query): Query<std::collections::HashMap<String, String>>,
    body: Bytes,
) -> Response {
    if !query.is_empty() {
        return ApiError::BadRequest(
            "authorization node cache accepts no query parameters".to_string(),
        )
        .into_response();
    }
    if body.len() > AuthzNodeCache::max_body_len() {
        return ApiError::BadRequest("authorization node cache body is too large".to_string())
            .into_response();
    }
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid authorization node cache body: {error}"))
                .into_response();
        }
    };
    let obj = match value.as_object() {
        Some(map) => map.clone(),
        None => {
            return ApiError::BadRequest(
                "authorization node cache body must be a JSON object".to_string(),
            )
            .into_response();
        }
    };
    if obj.is_empty() {
        return ApiError::BadRequest("authorization node cache body must not be empty".to_string())
            .into_response();
    }
    // Full replace: unknown fields are rejected by `deny_unknown_fields`
    // on the config struct, missing fields take their documented
    // defaults. Nothing is applied until the candidate validates.
    let candidate: AuthzNodeCacheConf = match serde_json::from_value(serde_json::Value::Object(obj))
    {
        Ok(candidate) => candidate,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid authorization node cache body: {error}"))
                .into_response();
        }
    };
    match state.authz_node_cache.replace(candidate) {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(AuthzCacheUpdateError::Invalid(reason)) => ApiError::BadRequest(reason).into_response(),
        Err(AuthzCacheUpdateError::Persist(reason)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot persist authorization node cache: {reason}"),
            })),
        )
            .into_response(),
    }
}

/// `POST /authorization/node_cache/reset`: evict every cached
/// authorization decision on this node.
///
/// Clears the node verdicts by evicting every per-session decision entry
/// (`crates/broker-session/src/lib.rs`,
/// `SessionManager::clear_all_authz_caches`, whose entries the publish
/// and subscribe authorization events recorded), so subsequent decisions
/// are recomputed. Clearing an already-empty cache still succeeds (204
/// with no body). Unknown query keys are ignored. A background eviction
/// only: clones one `Arc` per session under a single short map read,
/// then clears each per-session cache under its own short lock; no work
/// on the publish or deliver path.
pub async fn reset_authz_node_cache(
    State(state): State<ApiState>,
    Query(_ignored): Query<std::collections::HashMap<String, String>>,
) -> Response {
    state.sessions.clear_all_authz_caches();
    StatusCode::NO_CONTENT.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

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
            .expect("node cache body is small and readable");
        if bytes.is_empty() {
            return (status, serde_json::Value::Null);
        }
        let body: serde_json::Value =
            serde_json::from_slice(&bytes).expect("node cache body is JSON");
        (status, body)
    }

    #[tokio::test]
    async fn node_cache_round_trip_read_write_read() {
        let state = standalone_state();
        let (status, before) = response_parts(
            get_authz_node_cache(State(state.clone()), Query(Default::default())).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(before["enable"], serde_json::json!(true));
        assert_eq!(before["cache_ttl"], serde_json::json!("60s"));
        assert_eq!(before["max_count"], serde_json::json!(50_000));
        assert_eq!(before["max_memory"], serde_json::json!("100MB"));

        // Full replace with every field set.
        let update = serde_json::json!({
            "enable": true,
            "cache_ttl": "30s",
            "cleanup_interval": "60s",
            "stat_update_interval": "5s",
            "max_count": 5000,
            "max_memory": "100MB",
        });
        let body = Bytes::from(serde_json::to_vec(&update).expect("update is JSON"));
        let (status, _) = response_parts(
            put_authz_node_cache(State(state.clone()), Query(Default::default()), body).await,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        let (status, reread) = response_parts(
            get_authz_node_cache(State(state.clone()), Query(Default::default())).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reread["cache_ttl"], serde_json::json!("30s"));
        assert_eq!(reread["max_count"], serde_json::json!(5000));
    }

    #[tokio::test]
    async fn invalid_update_is_rejected_without_applying() {
        let state = standalone_state();
        let (status, before) = response_parts(
            get_authz_node_cache(State(state.clone()), Query(Default::default())).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        for bad in [
            serde_json::json!({"enable": "yes"}),
            serde_json::json!({"cache_ttl": "soon"}),
            serde_json::json!({"cleanup_interval": "soon"}),
            serde_json::json!({"stat_update_interval": 5}),
            serde_json::json!({"max_count": 0}),
            serde_json::json!({"max_count": 2_000_000}),
            serde_json::json!({"max_count": "many"}),
            serde_json::json!({"max_memory": "huge"}),
            serde_json::json!({"enable": true, "bogus": 1}),
            serde_json::json!({}),
            serde_json::json!([]),
        ] {
            let body = Bytes::from(serde_json::to_vec(&bad).expect("bad body is JSON"));
            let (status, err) = response_parts(
                put_authz_node_cache(State(state.clone()), Query(Default::default()), body).await,
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

        // Unknown query parameters are rejected fail-closed.
        let mut query = std::collections::HashMap::new();
        query.insert("unknown_param".to_string(), "1".to_string());
        let body = Bytes::from(
            serde_json::to_vec(&serde_json::json!({"enable": true})).expect("body is JSON"),
        );
        let (status, err) =
            response_parts(put_authz_node_cache(State(state.clone()), Query(query), body).await)
                .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));

        let (status, reread) = response_parts(
            get_authz_node_cache(State(state.clone()), Query(Default::default())).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reread, before, "failed writes must not apply");
    }

    #[tokio::test]
    async fn reset_evicts_entries_and_succeeds_empty() {
        let state = standalone_state();
        // Populate through real session decisions (the same entries the
        // broker's publish/subscribe authorization events record).
        let first = state.sessions.get_or_create("authz-node-a", true).0;
        let second = state.sessions.get_or_create("authz-node-b", true).0;
        first.record_authz_decision("publish", "t/a", 0, false, true);
        second.record_authz_decision("subscribe", "t/b", 0, false, true);
        assert_eq!(first.authz_cache_snapshot().len(), 1);
        assert_eq!(second.authz_cache_snapshot().len(), 1);

        let response =
            reset_authz_node_cache(State(state.clone()), Query(Default::default())).await;
        let (status, body) = response_parts(response).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert_eq!(body, serde_json::Value::Null);
        assert!(first.authz_cache_snapshot().is_empty());
        assert!(second.authz_cache_snapshot().is_empty());

        // Resetting an already-empty cache still succeeds, and unknown
        // query keys are ignored rather than a 400.
        let mut query = std::collections::HashMap::new();
        query.insert("unknown_param".to_string(), "1".to_string());
        let response = reset_authz_node_cache(State(state.clone()), Query(query)).await;
        let (status, _) = response_parts(response).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }

    #[test]
    fn documented_defaults_match_registry_source() {
        let defaults = AuthzNodeCacheConf::default();
        assert!(defaults.enable);
        assert_eq!(defaults.cache_ttl, "60s");
        assert_eq!(defaults.cleanup_interval, "60s");
        assert_eq!(defaults.stat_update_interval, "5s");
        assert_eq!(
            defaults.max_count,
            broker_config::DEFAULT_AUTHZ_NODE_CACHE_MAX
        );
        assert_eq!(defaults.max_memory, "100MB");
        defaults.validate().expect("documented defaults validate");
    }
}
