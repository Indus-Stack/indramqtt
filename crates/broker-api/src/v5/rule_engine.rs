//! Rule-engine settings for the v5 management API.
//!
//! Covers `GET /rule_engine` (read the live settings) and
//! `PUT /rule_engine` (validated full replace) over the real
//! [`broker_rules::EngineConfigStore`]. Single-node, management-plane
//! only: handlers clone one small snapshot per request under no delivery
//! lock; the rule fire path loads one lock-free snapshot per ingress to
//! observe the system-message skip (`broker_rules::RuleEngine::
//! dispatch_ingress` in `crates/broker-rules/src/lib.rs`, driven by every
//! published message) and never takes a management lock; publish and
//! deliver never touch this store.
//!
//! Neither route is paged and neither takes an id. Error answers use the
//! documented `{code, message}` shape.
//!
//! Only the system-message skip gates the fire path in this task; the jq
//! and request-filter knobs persist, validate, report and hot-apply to the
//! same snapshot, and their execution hooks land with their owning tasks.
// TODO(parity): which PUT success shape does the documented spec require
// (200 with the new settings versus 204)? The rulebook does not decide the
// exact shape; the current choice answers PUT with 200 and the live
// settings, mirroring the read shape, until the checker pins it.
// TODO(parity): does the documented PUT merge missing fields or reset them
// to defaults? The rulebook does not decide the merge semantics; the
// current choice replaces missing fields with the documented defaults
// (full replace) until the checker pins it.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use bytes::Bytes;

use crate::errors::ApiError;
use crate::ApiState;
use broker_config::RuleEngineConf;

pub use broker_rules::{EngineConfigStore, EngineConfigSubscriber, EngineConfigUpdateError};

/// `GET /rule_engine`: the live engine settings.
///
/// One lock-free snapshot and one small JSON clone per request; no
/// delivery locks, no new buffering. Unknown query keys are ignored: the
/// handler takes no paging params so extra keys never become a 400. A
/// fresh node answers the documented defaults, never an error.
pub async fn get_rule_engine(
    State(state): State<ApiState>,
    Query(_ignored): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let snapshot = state.rule_engine_config.snapshot();
    match serde_json::to_value(&*snapshot) {
        Ok(body) => (StatusCode::OK, axum::Json(body)).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot render rule engine settings: {error}"),
            })),
        )
            .into_response(),
    }
}

/// `PUT /rule_engine`: validated full replace.
///
/// The whole body is validated before anything is applied: one bad field
/// rejects the entire write with the documented `BAD_REQUEST` shape and
/// leaves the stored settings untouched. Unknown fields are rejected
/// fail-closed; they never degrade to the known subset. Missing fields
/// reset to their documented defaults: the body replaces the stored
/// settings, it never merges into them. Unknown query parameters are
/// rejected fail-closed. On success answers 200 with the live settings;
/// the settings persist across a restart through the config registry and
/// the engine subscriber applies the new snapshot in place for the next
/// ingress. Management-plane only: one snapshot swap per request; the
/// fire path keeps reading its lock-free snapshot and publish/deliver
/// never touch this store.
pub async fn put_rule_engine(
    State(state): State<ApiState>,
    Query(query): Query<std::collections::HashMap<String, String>>,
    body: Bytes,
) -> Response {
    if !query.is_empty() {
        return ApiError::BadRequest(
            "rule engine settings accepts no query parameters".to_string(),
        )
        .into_response();
    }
    if body.len() > EngineConfigStore::max_body_len() {
        return ApiError::BadRequest("rule engine settings body is too large".to_string())
            .into_response();
    }
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid rule engine settings body: {error}"))
                .into_response();
        }
    };
    let obj = match value.as_object() {
        Some(map) => map.clone(),
        None => {
            return ApiError::BadRequest(
                "rule engine settings body must be a JSON object".to_string(),
            )
            .into_response();
        }
    };
    if obj.is_empty() {
        return ApiError::BadRequest("rule engine settings body must not be empty".to_string())
            .into_response();
    }
    // Full replace: unknown fields are rejected by `deny_unknown_fields`
    // on the config struct, missing fields take their documented
    // defaults. Nothing is applied until the candidate validates.
    let candidate: RuleEngineConf = match serde_json::from_value(serde_json::Value::Object(obj)) {
        Ok(candidate) => candidate,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid rule engine settings body: {error}"))
                .into_response();
        }
    };
    match state.rule_engine_config.replace(candidate) {
        Ok(next) => match serde_json::to_value(&next) {
            Ok(body) => (StatusCode::OK, axum::Json(body)).into_response(),
            Err(error) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(serde_json::json!({
                    "code": "INTERNAL_ERROR",
                    "message": format!("cannot render rule engine settings: {error}"),
                })),
            )
                .into_response(),
        },
        Err(EngineConfigUpdateError::Invalid(reason)) => {
            ApiError::BadRequest(reason).into_response()
        }
        Err(EngineConfigUpdateError::Persist(reason)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot persist rule engine settings: {reason}"),
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
    async fn settings_read_returns_documented_defaults() {
        let state = standalone_state();
        let (status, before) =
            response_parts(get_rule_engine(State(state.clone()), Query(Default::default())).await)
                .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(before["ignore_sys_message"], serde_json::json!(true));
        assert_eq!(
            before["jq_function_default_timeout"],
            serde_json::json!("10s")
        );
        assert_eq!(
            before["jq_implementation_module"],
            serde_json::json!("jq_nif")
        );
        assert_eq!(before["ssrf"]["enable"], serde_json::json!(false));
        assert_eq!(
            before["limit_selects_in_namespace"],
            serde_json::json!(true)
        );
    }

    #[tokio::test]
    async fn settings_round_trip_read_write_read() {
        let state = standalone_state();
        let (status, _) =
            response_parts(get_rule_engine(State(state.clone()), Query(Default::default())).await)
                .await;
        assert_eq!(status, StatusCode::OK);

        // Full replace with every field set.
        let update = serde_json::json!({
            "ignore_sys_message": false,
            "jq_function_default_timeout": "30s",
            "jq_implementation_module": "jq_port",
            "ssrf": {
                "enable": true,
                "allow_cidrs": ["10.0.0.0/8"],
                "deny_cidrs": ["192.168.0.0/16"],
                "deny_hosts": ["example.internal"],
            },
            "limit_selects_in_namespace": false,
        });
        let body = Bytes::from(serde_json::to_vec(&update).expect("update is JSON"));
        let (status, applied) = response_parts(
            put_rule_engine(State(state.clone()), Query(Default::default()), body).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(applied["ignore_sys_message"], serde_json::json!(false));
        assert_eq!(
            applied["jq_function_default_timeout"],
            serde_json::json!("30s")
        );
        assert_eq!(applied["ssrf"]["enable"], serde_json::json!(true));

        let (status, reread) =
            response_parts(get_rule_engine(State(state.clone()), Query(Default::default())).await)
                .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reread, applied);

        // The engine hot-applied the same snapshot for the fire path.
        assert!(!state.engine.engine_config_snapshot().ignore_sys_message);
    }

    #[tokio::test]
    async fn partial_body_replaces_missing_fields_with_defaults() {
        let state = standalone_state();
        // Move fields off their defaults first with a full body.
        let full = serde_json::json!({
            "ignore_sys_message": false,
            "jq_function_default_timeout": "30s",
            "jq_implementation_module": "jq_port",
            "ssrf": {
                "enable": true,
                "allow_cidrs": ["10.0.0.0/8"],
                "deny_cidrs": ["192.168.0.0/16"],
                "deny_hosts": ["example.internal"],
            },
            "limit_selects_in_namespace": false,
        });
        let body = Bytes::from(serde_json::to_vec(&full).expect("full is JSON"));
        let (status, _) = response_parts(
            put_rule_engine(State(state.clone()), Query(Default::default()), body).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        // A partial body replaces: omitted fields reset to defaults
        // instead of merging with the stored values.
        let partial = serde_json::json!({
            "ignore_sys_message": true,
        });
        let body = Bytes::from(serde_json::to_vec(&partial).expect("partial is JSON"));
        let (status, _) = response_parts(
            put_rule_engine(State(state.clone()), Query(Default::default()), body).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, reread) =
            response_parts(get_rule_engine(State(state.clone()), Query(Default::default())).await)
                .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reread["ignore_sys_message"], serde_json::json!(true));
        assert_eq!(
            reread["jq_function_default_timeout"],
            serde_json::json!("10s"),
            "omitted fields must reset to defaults, not merge"
        );
        assert_eq!(
            reread["limit_selects_in_namespace"],
            serde_json::json!(true)
        );
    }

    #[tokio::test]
    async fn invalid_update_is_rejected_without_applying() {
        let state = standalone_state();
        let (status, before) =
            response_parts(get_rule_engine(State(state.clone()), Query(Default::default())).await)
                .await;
        assert_eq!(status, StatusCode::OK);

        for bad in [
            serde_json::json!({"ignore_sys_message": "yes"}),
            serde_json::json!({"jq_function_default_timeout": "soon"}),
            serde_json::json!({"jq_function_default_timeout": "0s"}),
            serde_json::json!({"jq_function_default_timeout": 10}),
            serde_json::json!({"jq_implementation_module": "jq_wasm"}),
            serde_json::json!({"ssrf": {"enable": "yes"}}),
            serde_json::json!({"ssrf": {"allow_cidrs": ["not-a-cidr"]}}),
            serde_json::json!({"ssrf": {"deny_cidrs": ["10.0.0.0/33"]}}),
            serde_json::json!({"ssrf": "yes"}),
            serde_json::json!({"limit_selects_in_namespace": 1}),
            serde_json::json!({"unknown_field": 1}),
            serde_json::json!({"ssrf": {"enable": true, "bogus": true}}),
            serde_json::json!({}),
            serde_json::json!([]),
        ] {
            let body = Bytes::from(serde_json::to_vec(&bad).expect("bad body is JSON"));
            let (status, err) = response_parts(
                put_rule_engine(State(state.clone()), Query(Default::default()), body).await,
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
            serde_json::to_vec(&serde_json::json!({"ignore_sys_message": false}))
                .expect("body is JSON"),
        );
        let (status, err) =
            response_parts(put_rule_engine(State(state.clone()), Query(query), body).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));

        let (status, reread) =
            response_parts(get_rule_engine(State(state.clone()), Query(Default::default())).await)
                .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reread, before, "failed writes must not apply");
    }

    #[test]
    fn documented_defaults_match_registry_source() {
        let defaults = RuleEngineConf::default();
        assert!(defaults.ignore_sys_message);
        assert_eq!(defaults.jq_function_default_timeout, "10s");
        assert_eq!(defaults.jq_timeout_ms(), 10_000);
        assert_eq!(defaults.jq_implementation_module, "jq_nif");
        assert!(!defaults.ssrf.enable);
        assert!(defaults.limit_selects_in_namespace);
        defaults.validate().expect("documented defaults validate");
    }

    struct FnSink;

    #[async_trait::async_trait]
    impl broker_rules::BrokerSink for FnSink {
        async fn publish(
            &self,
            _topic: broker_protocol::Topic,
            _payload: Bytes,
            _qos: broker_protocol::QoS,
            _retain: bool,
        ) -> Result<(), broker_rules::RuleEngineError> {
            Ok(())
        }
    }

    /// The engine gate goes through the broker rule path: a published
    /// system message matches no rule while the flag holds and matches
    /// once the management plane flips it, with the hot-apply subscriber
    /// moving the fire-path snapshot (not the registry) per ingress.
    #[tokio::test]
    async fn system_message_skip_follows_hot_applied_snapshot() {
        let state = standalone_state();
        state
            .engine
            .create_rule(
                "sys-rule".to_string(),
                broker_protocol::TopicFilter::new("#").expect("wildcard filter"),
                None,
                true,
                vec![broker_rules::RuleAction::Log],
            )
            .expect("rule creates");
        let sink: std::sync::Arc<dyn broker_rules::BrokerSink> = std::sync::Arc::new(FnSink);
        let topic = broker_protocol::Topic::new("$SYS/broker/uptime").expect("system topic");
        let payload = Bytes::from_static(b"{}");

        // Defaults skip system topics on the fire path.
        let matched = state
            .engine
            .dispatch_ingress(&topic, &payload, broker_protocol::QoS::AtMostOnce, &sink)
            .await;
        assert_eq!(matched, 0);

        // Flip the flag through the management plane: validate, persist,
        // hot-apply.
        let body = Bytes::from(
            serde_json::to_vec(&serde_json::json!({"ignore_sys_message": false}))
                .expect("body is JSON"),
        );
        let (status, _) = response_parts(
            put_rule_engine(State(state.clone()), Query(Default::default()), body).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        let matched = state
            .engine
            .dispatch_ingress(&topic, &payload, broker_protocol::QoS::AtMostOnce, &sink)
            .await;
        assert_eq!(matched, 1);

        // Ordinary topics are unaffected by the flag either way.
        let plain = broker_protocol::Topic::new("sensors/1").expect("plain topic");
        let matched = state
            .engine
            .dispatch_ingress(&plain, &payload, broker_protocol::QoS::AtMostOnce, &sink)
            .await;
        assert_eq!(matched, 1);
    }
}
