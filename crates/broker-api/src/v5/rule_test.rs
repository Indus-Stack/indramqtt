//! Stateless rule test for the v5 management API.
//!
//! Covers `POST /rule_test` (C06-17): evaluates a rule body against a
//! supplied sample message without storing anything, reporting
//! match/no-match plus the projected output. Malformed rule bodies are
//! rejected with the documented `{code, message}` shape; the test leaves
//! no rule, action binding or retained state behind. The route is not
//! paged and takes no id.
//!
//! The evaluator reuses the real rule compiler through
//! [`broker_rules::try_evaluate`] (`crates/broker-rules/src/lib.rs`),
//! the same front end rule create and update accept, so the offline probe
//! and the live fire path cannot drift. Side-effect-free by construction:
//! the probe runs with `dry_run` set, so state-mutating UDFs fail closed
//! instead of touching node-global maps, and the handler writes to no
//! store, publishes nothing and fires no action.
//!
//! Store wiring (rulebook section 2): no store. There is no
//! `Arc<crate::v5::...>` field on `ApiState` for this task, nothing
//! outside `crates/broker-api` writes or consults anything here, and no
//! broker event drives it beyond serving the management read, so
//! `tools/wiring_check.sh` needs no new entry (it only tracks store
//! fields on `ApiState`, of which this task adds none).
//!
//! Request bounds (stated here and enforced below):
//! - bodies past [`MAX_RULE_TEST_BODY_BYTES`] bytes are rejected before
//!   parsing (256 KiB: a sample payload plus a rule body comfortably fit,
//!   while one management request cannot bloat the kernel; the protocol
//!   needs only small samples and large retained blobs are never valid
//!   test inputs).
//! - the projected output derives from the bounded input, so it is bounded
//!   by the same limit; no separate output cache is built.
//!
//! Single-request bound for `POST /api/v5/rule_test`: one synchronous
//! evaluation at most, capped by [`RULE_TEST_TIMEOUT`]. Reason: a probe is
//! an operator-driven management-plane evaluation, never on the
//! publish/deliver path; 5 s bounds the worst case per request (a bounded
//! 256 KiB input evaluates in milliseconds) while staying well under
//! management-plane HTTP timeouts. Results are never cached (default no
//! caching): every request re-parses and re-evaluates so a transient
//! failure cannot be served stale.
//!
//! Management-plane only: runs off the message path (no lock, no new
//! allocation on publish/deliver), performs single-request bounded work,
//! writes to no store, registers no runtime, and leaves nothing running
//! after return.
//!
//! Fire-path cost: none. The evaluator is never consulted per message;
//! the publish path keeps paying what message ingress already pays (the
//! rule match in `dispatch_ingress`) with no new lock, allocation or queue
//! from this route.
// TODO(parity): which exact field names does the documented `POST /rule_test`
// request require (`sql` versus `sql_query`, flat `topic`/`payload` versus
// nested `context`)? The rulebook does not decide the exact shape; the
// current choice accepts `sql`/`sql_query` plus flat and `context`-nested
// topic/payload aliases until the checker pins it.
// TODO(parity): which exact success shape does the documented probe require
// beyond match/no-match plus projected output (`projected` versus `output`
// key)? The rulebook does not decide the exact shape; fields the broker
// can supply are served under both keys (`output` mirrors `projected`)
// until the checker pins it.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use std::collections::HashMap;

use crate::ApiState;

/// Single-request body bound for `POST /api/v5/rule_test`: 256 KiB.
/// Reason: a rule body plus a sample message comfortably fit, while one
/// operator-driven management request cannot bloat the kernel; larger
/// retained blobs are never valid test inputs.
pub const MAX_RULE_TEST_BODY_BYTES: usize = 256 * 1024;

/// Single-request evaluation bound for `POST /api/v5/rule_test`: 5 s.
/// Reason: an operator-driven management-plane probe, never on the
/// publish/deliver path; a bounded 256 KiB input evaluates in
/// milliseconds, so 5 s caps the worst case per request well under
/// management-plane HTTP timeouts.
pub const RULE_TEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

fn bad_request(message: String) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({ "code": "BAD_REQUEST", "message": message })),
    )
        .into_response()
}

fn missing_field(field: &str) -> Response {
    bad_request(format!("missing required field: {field}"))
}

/// Answer 412 with the documented `NOT_MATCH` code. The rule parses
/// but the sample does not match it.
fn no_match() -> Response {
    (
        StatusCode::PRECONDITION_FAILED,
        Json(serde_json::json!({
            "code": "NOT_MATCH",
            "message": "Rule does not match the sample.",
        })),
    )
        .into_response()
}

fn as_non_empty_str(value: Option<&serde_json::Value>) -> Option<String> {
    value
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
}

/// Extract the rule SQL from the flat or nested request shape.
fn extract_sql(body: &serde_json::Value) -> Option<String> {
    if let Some(sql) = as_non_empty_str(body.get("sql")) {
        return Some(sql);
    }
    if let Some(sql) = as_non_empty_str(body.get("sql_query")) {
        return Some(sql);
    }
    match body.get("rule") {
        Some(serde_json::Value::String(s)) if !s.trim().is_empty() => {
            return Some(s.trim().to_string());
        }
        Some(serde_json::Value::Object(rule)) => {
            if let Some(sql) = as_non_empty_str(rule.get("sql")) {
                return Some(sql);
            }
            if let Some(sql) = as_non_empty_str(rule.get("sql_query")) {
                return Some(sql);
            }
        }
        _ => {}
    }
    None
}

/// Extract the sample topic from the flat or `context`-nested shape.
fn extract_topic(body: &serde_json::Value) -> Option<String> {
    if let Some(topic) = as_non_empty_str(body.get("topic")) {
        return Some(topic);
    }
    if let Some(context) = body.get("context").and_then(|v| v.as_object()) {
        if let Some(topic) = as_non_empty_str(context.get("topic")) {
            return Some(topic);
        }
    }
    None
}

/// Extract the optional topic filter (`topic_filter` or `from`).
fn extract_topic_filter(body: &serde_json::Value) -> Option<String> {
    if let Some(filter) = as_non_empty_str(body.get("topic_filter")) {
        return Some(filter);
    }
    match body.get("from") {
        Some(serde_json::Value::String(s)) if !s.trim().is_empty() => Some(s.trim().to_string()),
        Some(serde_json::Value::Array(items)) => {
            for item in items {
                if let Some(s) = item.as_str().map(str::trim).filter(|s| !s.is_empty()) {
                    return Some(s.to_string());
                }
            }
            None
        }
        _ => None,
    }
}

/// Normalise the sample payload: objects/arrays/numbers pass through, a
/// string carrying JSON is parsed, any other string stays a string value.
fn normalise_payload(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return serde_json::Value::String(s.clone());
            }
            match serde_json::from_str::<serde_json::Value>(trimmed) {
                Ok(parsed) => parsed,
                Err(_) => serde_json::Value::String(s.clone()),
            }
        }
        other => other.clone(),
    }
}

/// Extract the sample payload from the flat, `message`/`sample` alias, or
/// `context`-nested shape.
fn extract_payload(body: &serde_json::Value) -> Option<serde_json::Value> {
    for key in ["payload", "message", "sample"] {
        if let Some(value) = body.get(key) {
            if !value.is_null() {
                return Some(normalise_payload(value));
            }
        }
    }
    if let Some(context) = body.get("context").and_then(|v| v.as_object()) {
        for key in ["payload", "message"] {
            if let Some(value) = context.get(key) {
                if !value.is_null() {
                    return Some(normalise_payload(value));
                }
            }
        }
    }
    None
}

/// `POST /rule_test`: stateless offline evaluation reusing the real rule
/// compiler.
///
/// Parses the bounded body, evaluates the rule SQL plus optional filter
/// over the sample through [`broker_rules::try_evaluate`], and answers
/// 200 with `{matched, projected, output}` on a match (`output` mirrors
/// `projected` for shape compatibility). A sample that does not match
/// answers 412 with the documented `NOT_MATCH` code. Malformed rule
/// bodies, topics, filters and samples are 400 with the documented
/// `{code, message}` shape. Nothing is stored, published or fired, and
/// nothing is cached: every request re-parses and re-evaluates.
/// Management-plane only; publish and deliver never touch this path.
pub async fn test_rule_offline(body: Bytes) -> Response {
    if body.len() > MAX_RULE_TEST_BODY_BYTES {
        return bad_request(format!(
            "rule test body is too large: {} bytes exceeds {} bytes",
            body.len(),
            MAX_RULE_TEST_BODY_BYTES
        ));
    }
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(error) => return bad_request(format!("invalid rule test body: {error}")),
    };
    if !value.is_object() {
        return bad_request("rule test body must be a JSON object".to_string());
    }
    let Some(sql) = extract_sql(&value) else {
        return missing_field("sql");
    };
    let Some(topic) = extract_topic(&value) else {
        return missing_field("topic");
    };
    let Some(payload) = extract_payload(&value) else {
        return missing_field("payload");
    };
    let topic_filter = extract_topic_filter(&value);
    // The stated [`RULE_TEST_TIMEOUT`] bounds one synchronous in-memory
    // evaluation per request via `tokio::time::timeout`: a bounded 256 KiB
    // input completes in milliseconds, so the timeout caps the worst case
    // per request well under management-plane HTTP timeouts.
    let sql_owned = sql.clone();
    let filter_owned = topic_filter.clone();
    let topic_owned = topic.clone();
    let payload_owned = payload.clone();
    let evaluated = tokio::time::timeout(RULE_TEST_TIMEOUT, async move {
        broker_rules::try_evaluate(
            Some(sql_owned.as_str()),
            filter_owned.as_deref(),
            topic_owned.as_str(),
            &payload_owned,
        )
    })
    .await;
    let (matched, projected) = match evaluated {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => return bad_request(error),
        Err(_) => {
            return (
                StatusCode::REQUEST_TIMEOUT,
                Json(serde_json::json!({
                    "code": "TIMEOUT",
                    "message": format!(
                        "rule test timed out after {}s",
                        RULE_TEST_TIMEOUT.as_secs()
                    ),
                })),
            )
                .into_response();
        }
    };
    if !matched {
        return no_match();
    }
    {
        let output = projected.clone().unwrap_or(serde_json::Value::Null);
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "matched": matched,
                "projected": output,
                "output": output,
            })),
        )
            .into_response()
    }
}

/// `POST /rules/:id/test`: per-rule offline evaluation bound to the stored
/// compiled rule (C06-25).
///
/// Loads the stored rule by id and evaluates it against the supplied sample
/// message (`topic` plus `payload`, flat or `context`-nested, the same
/// aliases [`test_rule_offline`] accepts) through the same
/// [`broker_rules::try_evaluate`] front end, reusing the stored SQL and the
/// stored topic filter. A sample `sql` field, when present, is ignored: the
/// probe is bound to the stored compiled rule, never to a caller-supplied
/// program. A match reports 200 with `{matched, projected, output}`
/// (`output` mirrors `projected` for shape compatibility). A sample that
/// does not match answers 412 with the documented `NOT_MATCH` code.
/// An unknown id returns 404 with the documented `{code,
/// message}` shape, not an empty body; malformed samples are rejected with
/// 400 and the documented error shape. Nothing is stored, published, fired
/// or retained: the probe runs with `dry_run` set, writes to no store and
/// leaves nothing cached. Management-plane only; publish and deliver never
/// touch this path.
///
/// Bounds (stated here and enforced below): the same single-request
/// [`MAX_RULE_TEST_BODY_BYTES`] body cap and [`RULE_TEST_TIMEOUT`]
/// evaluation bound as [`test_rule_offline`], for the same reasons (a
/// sample payload comfortably fits while one management request cannot
/// bloat the kernel; a bounded input evaluates in milliseconds).
///
/// Fire-path cost: none. The evaluator is never consulted per message;
/// the publish path keeps paying what message ingress already pays (the
/// rule match in `dispatch_ingress`) with no new lock, allocation or queue
/// from this route. The stored-rule lookup clones at most one small rule
/// under one short read lock on the management plane.
// TODO(parity): which exact sample shape does the documented per-rule probe
// require (flat `topic`/`payload` versus nested `context`, sample `sql`
// overriding or ignored)? The rulebook does not decide the exact shape;
// the current choice accepts flat and `context`-nested topic/payload
// aliases and ignores a sample `sql` (bound to the stored rule) until the
// checker pins it.
// TODO(parity): which exact success shape does the documented per-rule
// probe require beyond match/no-match plus projected output (`projected`
// versus `output` key)? The rulebook does not decide the exact shape;
// fields the broker can supply are served under both keys (`output`
// mirrors `projected`) until the checker pins it.
// TODO(parity): for an unknown id with an invalid sample body, is the
// answer 404 or 400? The rulebook does not decide the precedence; the
// current choice answers 404 first (existence before validation, mirroring
// the rule update) and stores nothing either way.
pub async fn test_stored_rule(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Query(_ignored): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    let Some(rule) = state.engine.get_rule(&id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "code": "RULE_NOT_FOUND",
                "message": "Rule not found"
            })),
        )
            .into_response();
    };
    if body.len() > MAX_RULE_TEST_BODY_BYTES {
        return bad_request(format!(
            "rule test body is too large: {} bytes exceeds {} bytes",
            body.len(),
            MAX_RULE_TEST_BODY_BYTES
        ));
    }
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(error) => return bad_request(format!("invalid rule test body: {error}")),
    };
    if !value.is_object() {
        return bad_request("rule test body must be a JSON object".to_string());
    }
    // The documented flag defaults to true: stop each action after its
    // template is rendered. This probe never runs an action, so `false`
    // is refused instead of being accepted and ignored.
    match value.get("stop_action_after_template_rendering") {
        None | Some(serde_json::Value::Bool(true)) => {}
        Some(serde_json::Value::Bool(false)) => {
            return bad_request(
                "running the actions of a rule from a test is not available in this broker \
                 (field `stop_action_after_template_rendering`)"
                    .to_string(),
            );
        }
        Some(_) => {
            return bad_request(
                "field `stop_action_after_template_rendering` must be a boolean".to_string(),
            );
        }
    }
    let Some(topic) = extract_topic(&value) else {
        return missing_field("topic");
    };
    let Some(payload) = extract_payload(&value) else {
        return missing_field("payload");
    };
    // Bound to the stored compiled rule: the stored SQL text plus the
    // stored topic filter, never a caller-supplied program.
    let stored_sql = rule.sql_query.clone();
    let stored_filter = rule.topic_filter.as_str().to_string();
    // The stated [`RULE_TEST_TIMEOUT`] bounds one synchronous in-memory
    // evaluation per request via `tokio::time::timeout`, the same bound
    // as the stateless probe.
    let evaluated = tokio::time::timeout(RULE_TEST_TIMEOUT, async move {
        broker_rules::try_evaluate(
            stored_sql.as_deref(),
            Some(stored_filter.as_str()),
            topic.as_str(),
            &payload,
        )
    })
    .await;
    let (matched, projected) = match evaluated {
        Ok(Ok(output)) => output,
        Ok(Err(error)) => return bad_request(error),
        Err(_) => {
            return (
                StatusCode::REQUEST_TIMEOUT,
                Json(serde_json::json!({
                    "code": "TIMEOUT",
                    "message": format!(
                        "rule test timed out after {}s",
                        RULE_TEST_TIMEOUT.as_secs()
                    ),
                })),
            )
                .into_response();
        }
    };
    if !matched {
        return no_match();
    }
    {
        let output = projected.clone().unwrap_or(serde_json::Value::Null);
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "matched": matched,
                "projected": output,
                "output": output,
            })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn standalone_state() -> ApiState {
        let engine = Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        ApiState::standalone(engine)
    }

    async fn response_parts(response: Response) -> (StatusCode, serde_json::Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 2 * 1024 * 1024)
            .await
            .expect("rule-test body is bounded and readable");
        if bytes.is_empty() {
            return (status, serde_json::Value::Null);
        }
        let body: serde_json::Value =
            serde_json::from_slice(&bytes).expect("rule-test body is JSON");
        (status, body)
    }

    fn body_of(value: serde_json::Value) -> Bytes {
        Bytes::from(serde_json::to_vec(&value).expect("test body serialises"))
    }

    #[tokio::test]
    async fn matching_sample_reports_projected_output() {
        let (status, body) = response_parts(
            test_rule_offline(body_of(serde_json::json!({
                "sql": "SELECT temperature FROM \"sensors/+\" WHERE temperature > 0",
                "topic": "sensors/kitchen",
                "payload": {"temperature": 72.5, "secret": "x"},
            })))
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["matched"], serde_json::json!(true));
        assert_eq!(body["projected"], serde_json::json!({"temperature": 72.5}));
        assert_eq!(body["output"], body["projected"]);
    }

    #[tokio::test]
    async fn non_matching_sample_answers_not_match() {
        let (status, body) = response_parts(
            test_rule_offline(body_of(serde_json::json!({
                "sql": "SELECT * FROM \"sensors/+\" WHERE temperature > 100.0",
                "topic": "sensors/kitchen",
                "payload": {"temperature": 72.5},
            })))
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::PRECONDITION_FAILED);
        assert_eq!(body["code"], serde_json::json!("NOT_MATCH"));
        assert!(body["message"].is_string());
    }

    #[tokio::test]
    async fn malformed_rule_is_rejected_without_storing() {
        let state = standalone_state();
        let before = state.engine.list_rules().len();
        let (status, body) = response_parts(
            test_rule_offline(body_of(serde_json::json!({
                "sql": "SELECT WHERE WHERE",
                "topic": "a/b",
                "payload": {},
            })))
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
        assert!(body["message"].is_string());
        assert_eq!(
            state.engine.list_rules().len(),
            before,
            "offline eval must not store rules"
        );
    }

    #[tokio::test]
    async fn oversized_body_is_rejected_before_parsing() {
        let big = "x".repeat(MAX_RULE_TEST_BODY_BYTES + 1);
        let raw = Bytes::from(format!(
            "{{\"sql\":\"SELECT 1\",\"topic\":\"t\",\"payload\":\"{big}\"}}"
        ));
        let (status, body) = response_parts(test_rule_offline(raw).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
    }

    #[tokio::test]
    async fn missing_fields_are_rejected() {
        for bad in [
            serde_json::json!({"topic": "a/b", "payload": {}}),
            serde_json::json!({"sql": "SELECT * FROM \"a/b\"", "payload": {}}),
            serde_json::json!({"sql": "SELECT * FROM \"a/b\"", "topic": "a/b"}),
            serde_json::json!([]),
        ] {
            let (status, body) = response_parts(test_rule_offline(body_of(bad)).await).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
        }
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

    /// The probe goes through the broker rule path without touching live
    /// routing: the same SQL evaluated offline also matches through the
    /// engine's ingress dispatch, while the probe itself stores nothing.
    #[tokio::test]
    async fn offline_probe_shares_compiler_with_live_dispatch() {
        let state = standalone_state();
        let (status, body) = response_parts(
            test_rule_offline(body_of(serde_json::json!({
                "sql": "SELECT * FROM \"w312/#\"",
                "topic": "w312/probe",
                "payload": {"v": 1},
            })))
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["matched"], serde_json::json!(true));
        assert_eq!(state.engine.list_rules().len(), 0);

        state
            .engine
            .create_rule(
                "w312-live".to_string(),
                broker_protocol::TopicFilter::new("w312/#").expect("filter"),
                Some("SELECT * FROM \"w312/#\"".to_string()),
                true,
                vec![broker_rules::RuleAction::Log],
            )
            .expect("rule creates");
        let sink: Arc<dyn broker_rules::BrokerSink> = Arc::new(FnSink);
        let topic = broker_protocol::Topic::new("w312/probe").expect("topic");
        let matched = state
            .engine
            .dispatch_ingress(
                &topic,
                &Bytes::from_static(b"{\"v\": 1}"),
                broker_protocol::QoS::AtMostOnce,
                &sink,
            )
            .await;
        assert_eq!(matched, 1);
    }

    fn stored_state() -> ApiState {
        let state = standalone_state();
        state
            .engine
            .create_rule_with_id(
                "w316-rule".to_string(),
                "w316-rule".to_string(),
                broker_protocol::TopicFilter::new("sensors/+").expect("filter"),
                Some("SELECT temperature FROM \"sensors/+\" WHERE temperature > 100.0".to_string()),
                true,
                vec![],
            )
            .expect("rule creates");
        state
    }

    /// The probe never runs an action. The default (stop after the
    /// template) and an explicit `true` are served; `false` is refused.
    #[tokio::test]
    async fn stored_probe_refuses_to_run_actions() {
        let state = stored_state();
        for (flag, expected) in [
            (None, StatusCode::OK),
            (Some(serde_json::json!(true)), StatusCode::OK),
            (Some(serde_json::json!(false)), StatusCode::BAD_REQUEST),
            (Some(serde_json::json!("no")), StatusCode::BAD_REQUEST),
        ] {
            let mut body = serde_json::json!({
                "context": {"topic": "sensors/kitchen", "payload": "{\"temperature\":150.0}"},
            });
            if let Some(flag) = flag {
                body["stop_action_after_template_rendering"] = flag;
            }
            let (status, _) = response_parts(
                test_stored_rule(
                    State(state.clone()),
                    Path("w316-rule".to_string()),
                    Query(HashMap::new()),
                    body_of(body),
                )
                .await,
            )
            .await;
            assert_eq!(status, expected);
        }
    }

    /// Per-rule probe bound to the stored rule: match reports projected
    /// output, no-match answers 412, and the stored rule survives.
    #[tokio::test]
    async fn stored_probe_reports_match_and_no_match() {
        let state = stored_state();
        let (status, body) = response_parts(
            test_stored_rule(
                State(state.clone()),
                Path("w316-rule".to_string()),
                Query(HashMap::new()),
                body_of(serde_json::json!({
                    "topic": "sensors/kitchen",
                    "payload": {"temperature": 150.0, "secret": "x"},
                })),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["matched"], serde_json::json!(true));
        assert_eq!(body["projected"], serde_json::json!({"temperature": 150.0}));
        assert_eq!(body["output"], body["projected"]);

        let (status, body) = response_parts(
            test_stored_rule(
                State(state.clone()),
                Path("w316-rule".to_string()),
                Query(HashMap::new()),
                body_of(serde_json::json!({
                    "topic": "sensors/kitchen",
                    "payload": {"temperature": 72.5},
                })),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::PRECONDITION_FAILED);
        assert_eq!(body["code"], serde_json::json!("NOT_MATCH"));
        assert!(body["message"].is_string());
        assert!(state.engine.get_rule("w316-rule").is_some());
    }

    /// Unknown rule ids are 404 with the documented shape, even with a
    /// body that would otherwise be invalid (existence before validation).
    #[tokio::test]
    async fn stored_probe_unknown_id_is_404() {
        let state = stored_state();
        for raw in [
            body_of(serde_json::json!({
                "topic": "sensors/kitchen",
                "payload": {"temperature": 1.0},
            })),
            body_of(serde_json::json!({"topic": "sensors/kitchen"})),
        ] {
            let (status, body) = response_parts(
                test_stored_rule(
                    State(state.clone()),
                    Path("no-such-rule".to_string()),
                    Query(HashMap::new()),
                    raw,
                )
                .await,
            )
            .await;
            assert_eq!(status, StatusCode::NOT_FOUND);
            assert_eq!(body["code"], serde_json::json!("RULE_NOT_FOUND"));
            assert!(body["message"].is_string());
        }
    }

    /// Malformed samples on a known rule are 400 with the documented
    /// shape and store nothing new.
    #[tokio::test]
    async fn stored_probe_malformed_sample_is_rejected() {
        let state = stored_state();
        let before = state.engine.list_rules().len();
        for bad in [
            serde_json::json!({"payload": {"temperature": 1.0}}),
            serde_json::json!({"topic": "sensors/kitchen"}),
            serde_json::json!([]),
        ] {
            let (status, body) = response_parts(
                test_stored_rule(
                    State(state.clone()),
                    Path("w316-rule".to_string()),
                    Query(HashMap::new()),
                    body_of(bad),
                )
                .await,
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
        }
        assert_eq!(state.engine.list_rules().len(), before);
    }
}
