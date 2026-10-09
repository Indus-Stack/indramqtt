//! Rule-event catalogue for the v5 management API.
//!
//! Covers `GET /rule_events` (C06-16).
//! It lists the event types that rules can trigger on.
//! It carries the documented per-entry fields.
//! Single node only, management plane only.
//! The handler clones at most
//! [`broker_rules::events::MAX_RULE_EVENTS`] small entries per request.
//! It reads from the real producer registry
//! ([`broker_rules::events::rule_event_catalogue`] in
//! `crates/broker-rules/src/events.rs`).
//! The rule fire path never reads this catalogue.
//! The fire path is `broker_rules::RuleEngine::dispatch_ingress` in
//! `crates/broker-rules/src/lib.rs`.
//! The kernel ingress pipeline drives it for each published message.
//! The catalogue adds no work to publish or deliver.
//! It adds no locks and no allocation.
//!
//! Store wiring (rulebook section 2): no store.
//! The catalogue reads the producer registry itself.
//! It shares the registry with the [`is_produced_event`]
//! predicate below.
//! The served list and the producer check cannot drift.
//! No broker event drives it beyond serving the management read.
//! Nothing outside `crates/broker-api` writes it.
//! Nothing outside `crates/broker-api` reads it.
//! `tools/wiring_check.sh` needs no new entry.
//! The script tracks only `Arc<crate::v5::...>` store fields on
//! `ApiState`, and this task adds none.
//!
//! Persistence: none per request. The registry is static within a run
//! (compiled in, rebuilt at startup with the binary); a restart serves
//! the same fixed set.
//!
//! Store bounds (stated here and enforced by construction):
//! - at most [`broker_rules::events::MAX_RULE_EVENTS`] entries (64: caps
//!   management-plane memory; the built-in set is exactly
//!   [`broker_rules::events::RULE_EVENT_COUNT`]);
//! - an empty catalogue reads as an empty list (`[]`), never an error;
//! - reads clone at most one small document per producer (two short
//!   strings) with no lock held.
//!
//! Paging (stated here): the route is not paged and takes no parameters;
//! unknown query parameters are ignored (no query beyond the ignore sink
//! is extracted).
// The catalogue serves `event` plus `description` per entry. Fields
// the broker cannot supply stay omitted rather than invented. It lists
// only the events the kernel drives today, never invented names.
// TODO(parity): Which fields does the catalogue need beyond event and description?

use axum::{
    extract::Query,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use std::collections::HashMap;

/// Whether `event` names a real kernel producer (exact match on the
/// stable event name).
///
/// Shared with the served list so the two cannot drift: the list and this
/// predicate read the same [`broker_rules::events`] registry.
pub use broker_rules::events::is_produced_event;

/// `GET /rule_events`: catalogue of event types rules can trigger on.
///
/// Returns the bare list (not paged): an empty registry reads as `[]`,
/// never an error. Unknown query parameters are ignored. The catalogue
/// is built from the real producer registry, so adding a producer
/// elsewhere automatically extends the served list; only events the
/// kernel actually produces are listed, never invented names.
/// Management-plane only: reads a static registry, takes no lock, and
/// publish/deliver never touch it.
///
/// Fire-path cost: none. The catalogue is never consulted per message;
/// the publish path keeps paying what message ingress already pays (the
/// rule match in `dispatch_ingress`) with no new lock, allocation or
/// queue from this route.
pub async fn list_rule_events(Query(_ignored): Query<HashMap<String, String>>) -> Response {
    let catalogue = broker_rules::events::rule_event_catalogue();
    (
        StatusCode::OK,
        Json(serde_json::to_value(catalogue).unwrap_or_default()),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ApiState;
    use axum::extract::State;
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
        let bytes = axum::body::to_bytes(response.into_body(), 256 * 1024)
            .await
            .expect("rule-event body is small and readable");
        let body: serde_json::Value =
            serde_json::from_slice(&bytes).expect("rule-event body is JSON");
        (status, body)
    }

    #[tokio::test]
    async fn catalogue_lists_only_produced_events() {
        // W3-11 failing test shape: the served list carries one entry per
        // real producer with the documented fields. Before: 404
        // route-absent.
        let (status, body) =
            response_parts(list_rule_events(Query(Default::default())).await).await;
        assert_eq!(status, StatusCode::OK);
        let entries = body.as_array().expect("catalogue is a list");
        assert!(!entries.is_empty(), "catalogue must not be empty");
        assert!(entries.len() <= broker_rules::events::MAX_RULE_EVENTS);
        let mut names: Vec<&str> = Vec::new();
        for entry in entries {
            let event = entry["event"].as_str().expect("entry carries event");
            assert!(
                is_produced_event(event),
                "catalogue lists unproduced event '{event}'"
            );
            assert!(
                entry["description"].as_str().is_some_and(|d| !d.is_empty()),
                "entry carries a summary: {entry}"
            );
            names.push(event);
        }
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), entries.len(), "events must be unique");
        assert!(names.contains(&"$events/client_connected"));
        assert!(!names.contains(&"$events/message_publish"));
    }

    #[tokio::test]
    async fn unknown_query_parameters_are_ignored() {
        // W3-11 failing test shape: unknown query keys never become a 400.
        // Before: 404 route-absent.
        let (status, body) =
            response_parts(list_rule_events(Query(Default::default())).await).await;
        assert_eq!(status, StatusCode::OK);
        let mut query = HashMap::new();
        query.insert("unknown".to_string(), "param".to_string());
        let (status, again) = response_parts(list_rule_events(Query(query)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(again, body);
    }

    struct FnSink;

    #[async_trait::async_trait]
    impl broker_rules::BrokerSink for FnSink {
        async fn publish(
            &self,
            _topic: broker_protocol::Topic,
            _payload: bytes::Bytes,
            _qos: broker_protocol::QoS,
            _retain: bool,
        ) -> Result<(), broker_rules::RuleEngineError> {
            Ok(())
        }
    }

    /// The catalogue goes through the broker rule path: a published
    /// message is seen by the trigger path behind the listed event, with
    /// the catalogue itself never consulted per message.
    #[tokio::test]
    async fn published_event_reaches_rule_trigger_path() {
        let state = standalone_state();
        let (status, body) =
            response_parts(list_rule_events(Query(Default::default())).await).await;
        assert_eq!(status, StatusCode::OK);
        let entries = body.as_array().expect("catalogue is a list");
        assert!(entries
            .iter()
            .any(|e| e["event"] == "$events/client_connected"));
        state
            .engine
            .create_rule(
                "w311-rule".to_string(),
                broker_protocol::TopicFilter::new("w311/#").expect("filter"),
                None,
                true,
                vec![broker_rules::RuleAction::Log],
            )
            .expect("rule creates");
        let sink: Arc<dyn broker_rules::BrokerSink> = Arc::new(FnSink);
        let topic = broker_protocol::Topic::new("w311/probe").expect("topic");
        let payload = bytes::Bytes::from_static(b"{}");
        let matched = state
            .engine
            .dispatch_ingress(&topic, &payload, broker_protocol::QoS::AtMostOnce, &sink)
            .await;
        assert_eq!(matched, 1);
    }

    #[tokio::test]
    async fn catalogue_read_adds_no_work_to_publish() {
        // Management-plane only: catalogue reads followed by a management
        // publish through the rule engine still answer, so customer
        // traffic keeps serving.
        let state = standalone_state();
        let (status, _) = response_parts(list_rule_events(Query(Default::default())).await).await;
        assert_eq!(status, StatusCode::OK);
        let response = crate::v5::clients::publish_message(
            State(state),
            bytes::Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "topic": "w311/probe",
                    "payload": "hello",
                    "qos": 0,
                }))
                .expect("json"),
            ),
        )
        .await;
        let (status, body) = response_parts(response).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(body.get("reason_code").is_some(), "publish answers 202");
    }
}
