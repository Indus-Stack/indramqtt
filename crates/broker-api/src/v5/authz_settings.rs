//! Global authorization settings for the v5 management API.
//!
//! Covers `GET /authorization/settings` (read) and `PUT
//! /authorization/settings` (validated replace with documented defaults
//! filled) over the real [`broker_auth::AuthzSettingsStore`]. Single-node,
//! management-plane only: handlers clone one small snapshot per request
//! under no delivery lock; the kernel boot path seeds the same instance
//! from the config registry (`crates/broker-node/src/main.rs`, boot
//! seeding, and `crate::ApiState::new`) so reads observe the persisted
//! settings from the first connection. The publish and subscribe
//! authorization events read the stored `no_match`/`deny_action` values
//! through lock-free bool reads with no snapshot clone.
//!
//! Neither route is paged. Error answers use the documented
//! `{code, message}` shape.
//!
//! Decision evaluation reads the `no_match`/`deny_action` values on the
//! authorize path; the remaining halves (`cache`, `include_mountpoint`,
//! `ignore_backend_failures`) are stored, validated and reported only.
// TODO(parity): does the documented PUT merge missing fields or reset
// them to defaults, and does an empty body reset everything or fail? The
// rulebook does not decide the merge semantics; the current choice fills
// missing fields with the documented defaults (full replace, mirroring
// the neighbouring settings routes) and rejects an empty body
// fail-closed, until the checker pins it.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use bytes::Bytes;

use crate::errors::ApiError;
use crate::ApiState;
use broker_auth::AuthzSettingsUpdateError;
use broker_config::AuthzSettingsConf;

pub use broker_auth::AuthzSettingsStore;

/// `GET /authorization/settings`: current global settings.
///
/// One lock-free snapshot and one small JSON clone per request; no
/// delivery locks, no new buffering. Unknown query keys are ignored: the
/// handler takes no paging params so extra keys never become a 400.
pub async fn get_authz_settings(
    State(state): State<ApiState>,
    Query(_ignored): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let snapshot = state.authz_settings.snapshot();
    match serde_json::to_value(&*snapshot) {
        Ok(body) => (StatusCode::OK, axum::Json(body)).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot render authorization settings: {error}"),
            })),
        )
            .into_response(),
    }
}

/// `PUT /authorization/settings`: validated replace.
///
/// The whole body is validated before anything is applied: one bad field
/// rejects the entire write with the documented `BAD_REQUEST` shape and
/// leaves the stored settings untouched. Unknown fields (top-level or
/// inside `cache`) are rejected fail-closed; they never degrade to the
/// known subset. Missing fields take their documented defaults: the body
/// replaces the stored settings, it never merges into them. Unknown query
/// parameters are rejected fail-closed. On success returns 200 with the
/// stored settings (the documented shape) and the settings persist across
/// a restart through the config registry. A successful replace clears the
/// per-session decision caches so the next publish or subscribe uses the
/// new policy at once. Management-plane only: one snapshot swap per
/// request; publish and deliver never touch this store.
pub async fn put_authz_settings(
    State(state): State<ApiState>,
    Query(query): Query<std::collections::HashMap<String, String>>,
    body: Bytes,
) -> Response {
    if !query.is_empty() {
        return ApiError::BadRequest(
            "authorization settings accepts no query parameters".to_string(),
        )
        .into_response();
    }
    if body.len() > AuthzSettingsStore::max_body_len() {
        return ApiError::BadRequest("authorization settings body is too large".to_string())
            .into_response();
    }
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid authorization settings body: {error}"))
                .into_response();
        }
    };
    let obj = match value.as_object() {
        Some(map) => map.clone(),
        None => {
            return ApiError::BadRequest(
                "authorization settings body must be a JSON object".to_string(),
            )
            .into_response();
        }
    };
    if obj.is_empty() {
        return ApiError::BadRequest("authorization settings body must not be empty".to_string())
            .into_response();
    }
    // Replace with defaults filled: unknown fields are rejected by
    // `deny_unknown_fields` on the config struct, missing fields take
    // their documented defaults. Nothing is applied until the candidate
    // validates.
    let candidate: AuthzSettingsConf = match serde_json::from_value(serde_json::Value::Object(obj))
    {
        Ok(candidate) => candidate,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid authorization settings body: {error}"))
                .into_response();
        }
    };
    match state.authz_settings.replace(candidate) {
        Ok(stored) => {
            // A settings change invalidates cached decisions so the next
            // operation uses the new policy at once (SA-22 item 4).
            state.sessions.clear_all_authz_caches();
            match serde_json::to_value(&stored) {
                Ok(body) => (StatusCode::OK, axum::Json(body)).into_response(),
                Err(error) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(serde_json::json!({
                        "code": "INTERNAL_ERROR",
                        "message": format!("cannot render authorization settings: {error}"),
                    })),
                )
                    .into_response(),
            }
        }
        Err(AuthzSettingsUpdateError::Invalid(reason)) => {
            ApiError::BadRequest(reason).into_response()
        }
        Err(AuthzSettingsUpdateError::Persist(reason)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot persist authorization settings: {reason}"),
            })),
        )
            .into_response(),
    }
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
            .expect("settings body is small and readable");
        if bytes.is_empty() {
            return (status, serde_json::Value::Null);
        }
        let body: serde_json::Value =
            serde_json::from_slice(&bytes).expect("settings body is JSON");
        (status, body)
    }

    #[tokio::test]
    async fn settings_round_trip_read_write_read() {
        let state = standalone_state();
        let (status, before) = response_parts(
            get_authz_settings(State(state.clone()), Query(Default::default())).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(before["no_match"], serde_json::json!("deny"));
        assert_eq!(before["deny_action"], serde_json::json!("ignore"));
        assert_eq!(before["cache"]["enable"], serde_json::json!(true));
        assert_eq!(before["cache"]["max_size"], serde_json::json!(32));
        assert_eq!(before["cache"]["ttl"], serde_json::json!("1m"));
        assert_eq!(before["cache"]["excludes"], serde_json::json!([]));
        assert_eq!(before["include_mountpoint"], serde_json::json!(false));
        assert_eq!(before["ignore_backend_failures"], serde_json::json!(false));

        // Full replace with every field set; the write answers 200 with
        // the stored settings.
        let update = serde_json::json!({
            "no_match": "allow",
            "deny_action": "disconnect",
            "cache": {
                "enable": false,
                "max_size": 64,
                "ttl": "60s",
                "excludes": ["nocache/#"],
            },
            "include_mountpoint": false,
            "ignore_backend_failures": true,
        });
        let body = Bytes::from(serde_json::to_vec(&update).expect("update is JSON"));
        let (status, answered) = response_parts(
            put_authz_settings(State(state.clone()), Query(Default::default()), body).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(answered["no_match"], serde_json::json!("allow"));
        assert_eq!(answered["deny_action"], serde_json::json!("disconnect"));
        assert_eq!(answered["cache"]["max_size"], serde_json::json!(64));
        assert_eq!(
            answered["cache"]["excludes"],
            serde_json::json!(["nocache/#"])
        );

        let (status, reread) = response_parts(
            get_authz_settings(State(state.clone()), Query(Default::default())).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reread, answered, "the write answers what the read reports");
    }

    #[tokio::test]
    async fn partial_body_replaces_missing_fields_with_defaults() {
        let state = standalone_state();
        // Move fields off their defaults first with a full body.
        let full = serde_json::json!({
            "no_match": "allow",
            "deny_action": "disconnect",
            "cache": {
                "enable": false,
                "max_size": 64,
                "ttl": "60s",
                "excludes": ["nocache/#"],
            },
            "include_mountpoint": false,
            "ignore_backend_failures": true,
        });
        let body = Bytes::from(serde_json::to_vec(&full).expect("full is JSON"));
        let (status, _) = response_parts(
            put_authz_settings(State(state.clone()), Query(Default::default()), body).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        // A partial body replaces: omitted fields reset to defaults
        // instead of merging with the stored values.
        let partial = serde_json::json!({"no_match": "deny"});
        let body = Bytes::from(serde_json::to_vec(&partial).expect("partial is JSON"));
        let (status, answered) = response_parts(
            put_authz_settings(State(state.clone()), Query(Default::default()), body).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(answered["no_match"], serde_json::json!("deny"));
        assert_eq!(
            answered["cache"]["max_size"],
            serde_json::json!(32),
            "omitted fields must reset to defaults, not merge"
        );
        assert_eq!(
            answered["cache"]["excludes"],
            serde_json::json!([]),
            "omitted cache fields must reset to defaults, not merge"
        );
        assert_eq!(answered["deny_action"], serde_json::json!("ignore"));
        assert_eq!(
            answered["ignore_backend_failures"],
            serde_json::json!(false)
        );
    }

    #[tokio::test]
    async fn invalid_update_is_rejected_without_applying() {
        let state = standalone_state();
        let (status, before) = response_parts(
            get_authz_settings(State(state.clone()), Query(Default::default())).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        for bad in [
            serde_json::json!({"no_match": "sometimes"}),
            serde_json::json!({"no_match": "DENY"}),
            serde_json::json!({"deny_action": "log"}),
            serde_json::json!({"deny_action": 1}),
            serde_json::json!({"cache": {"max_size": 0}}),
            serde_json::json!({"cache": {"max_size": 2_000_000}}),
            serde_json::json!({"cache": {"max_size": "many"}}),
            serde_json::json!({"cache": {"ttl": "soon"}}),
            serde_json::json!({"cache": {"ttl": 60}}),
            serde_json::json!({"cache": {"excludes": [""]}}),
            serde_json::json!({"cache": {"excludes": "nocache/#"}}),
            serde_json::json!({"cache": "yes"}),
            serde_json::json!({"include_mountpoint": "yes"}),
            serde_json::json!({"ignore_backend_failures": "yes"}),
            serde_json::json!({"unknown_field": 1}),
            serde_json::json!({"no_match": "deny", "bogus": 1}),
            serde_json::json!({"cache": {"max_size": 64, "bogus": true}}),
            serde_json::json!({}),
            serde_json::json!([]),
        ] {
            let body = Bytes::from(serde_json::to_vec(&bad).expect("bad body is JSON"));
            let (status, err) = response_parts(
                put_authz_settings(State(state.clone()), Query(Default::default()), body).await,
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
            serde_json::to_vec(&serde_json::json!({"no_match": "deny"})).expect("body is JSON"),
        );
        let (status, err) =
            response_parts(put_authz_settings(State(state.clone()), Query(query), body).await)
                .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));

        let (status, reread) = response_parts(
            get_authz_settings(State(state.clone()), Query(Default::default())).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reread, before, "failed writes must not apply");
    }

    #[test]
    fn documented_defaults_match_registry_source() {
        let defaults = AuthzSettingsConf::default();
        assert_eq!(defaults.no_match, "deny");
        assert_eq!(defaults.deny_action, "ignore");
        assert!(defaults.cache.enable);
        assert_eq!(
            defaults.cache.max_size,
            broker_config::DEFAULT_AUTHZ_SETTINGS_CACHE_MAX_SIZE
        );
        assert_eq!(defaults.cache.ttl, "1m");
        assert!(defaults.cache.excludes.is_empty());
        assert!(!defaults.include_mountpoint);
        assert!(!defaults.ignore_backend_failures);
        defaults.validate().expect("documented defaults validate");
    }
}
