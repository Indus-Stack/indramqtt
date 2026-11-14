//! RQ-04 event-source unit tests for the rule engine.
//!
//! These tests verify rule-engine-level behavior: event source listing,
//! zero-cost firing when no rules are selected, and flag clearing on
//! disable/remove. The per-event broker integration tests that connect
//! real clients through the broker live in `crates/broker-node/src/main.rs`.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broker_protocol::{QoS, Topic, TopicFilter};
use broker_rules::{
    BackpressurePolicy, BrokerSink, ClientCheckAuthzCompleteEvent, ClientConnackEvent,
    ClientConnectedEvent, ClientDisconnectedEvent, DeliveryDroppedEvent, MessageAckedEvent,
    MessageDeliveredEvent, MessageDroppedEvent, Rule, RuleAction, RuleEngine, RuleEngineError,
    SessionSubscribedEvent, SessionUnsubscribedEvent,
};
use bytes::Bytes;
use serde_json::Value;

/// In-memory `BrokerSink` that records every republished output for
/// assertions.
#[derive(Debug, Default)]
struct CapturingSink {
    published: Mutex<Vec<(Topic, Bytes, QoS, bool)>>,
}

#[async_trait]
impl BrokerSink for CapturingSink {
    async fn publish(
        &self,
        topic: Topic,
        payload: Bytes,
        qos: QoS,
        retain: bool,
    ) -> Result<(), RuleEngineError> {
        self.published
            .lock()
            .unwrap()
            .push((topic, payload, qos, retain));
        Ok(())
    }
}

fn create_test_engine() -> Arc<RuleEngine> {
    Arc::new(RuleEngine::new(1024, BackpressurePolicy::DropOldest))
}

fn engine_with_sink() -> (Arc<RuleEngine>, Arc<CapturingSink>) {
    let engine = create_test_engine();
    let sink = Arc::new(CapturingSink::default());
    (engine, sink)
}

/// Install an enabled `Republish` rule over the given `$events/...` source.
/// Returns the created rule (for disable/remove tests). A `#` topic filter
/// matches every topic; the rule still only fires its action on the matching
/// event because `std_sources_match` gates event sources to their own topic.
fn add_republish_rule(
    engine: &Arc<RuleEngine>,
    source: &str,
    select: &str,
    republish_to: &str,
) -> Rule {
    let sql = format!("SELECT {select} FROM \"{source}\"");
    engine
        .create_rule(
            format!("ev_{}", source.trim_start_matches("$events/")),
            TopicFilter::new("#").unwrap(),
            Some(sql),
            true,
            vec![RuleAction::Republish {
                topic: Topic::new(republish_to).unwrap(),
                qos: QoS::AtLeastOnce,
            }],
        )
        .expect("rule created")
}

/// Drive one real publish ingress so the drain loop runs queued event
/// actions. The probe topic matches the rule's `#` filter, but because the
/// rule is event-only (`std_sources_match` fails on a non-event topic) it
/// produces no inline output — only the queued event is republished.
async fn drain_through_ingress(engine: &Arc<RuleEngine>, sink: &Arc<CapturingSink>) {
    let broker_sink: Arc<dyn BrokerSink> = sink.clone();
    let probe = Topic::new("probe/not-an-event").unwrap();
    let _ = engine
        .dispatch_ingress(&probe, &Bytes::new(), QoS::AtMostOnce, &broker_sink)
        .await;
}

/// Assert exactly one republish landed, addressed to `republish_to`, and
/// decode its projected JSON payload.
fn take_single_output(sink: &Arc<CapturingSink>, republish_to: &str) -> Value {
    let captured = sink.published.lock().unwrap();
    assert_eq!(
        captured.len(),
        1,
        "event rule should republish exactly once through the ingress drain"
    );
    assert_eq!(
        captured[0].0.as_str(),
        republish_to,
        "republish target topic"
    );
    let payload: &[u8] = &captured[0].1;
    serde_json::from_slice::<Value>(payload).expect("rule output is valid JSON")
}

// ---------------------------------------------------------------------------
// Metadata and flag-management tests.
// ---------------------------------------------------------------------------

/// Test that event sources are listed correctly.
#[tokio::test]
async fn test_list_event_sources() {
    let engine = create_test_engine();
    let sources = engine.list_event_sources();

    assert_eq!(sources.len(), 10, "should have 10 event sources");

    let names: Vec<String> = sources.iter().map(|s| s.name.clone()).collect();
    assert!(names.contains(&"$events/client_connected".to_string()));
    assert!(names.contains(&"$events/client_disconnected".to_string()));
    assert!(names.contains(&"$events/client_connack".to_string()));
    assert!(names.contains(&"$events/client_check_authz_complete".to_string()));
    assert!(names.contains(&"$events/session_subscribed".to_string()));
    assert!(names.contains(&"$events/session_unsubscribed".to_string()));
    assert!(names.contains(&"$events/message_delivered".to_string()));
    assert!(names.contains(&"$events/message_acked".to_string()));
    assert!(names.contains(&"$events/message_dropped".to_string()));
    assert!(names.contains(&"$events/delivery_dropped".to_string()));

    // Verify fields are present
    for source in sources {
        assert!(
            !source.fields.is_empty(),
            "source {} should have fields",
            source.name
        );
    }
}

/// With no rules selected, every fire_* is a no-op and a publish ingress
/// drains nothing (the aggregate gate keeps the drain off the hot path).
#[tokio::test]
async fn test_event_zero_cost_when_no_rules() {
    let (engine, sink) = engine_with_sink();

    // No rules created: every event source is unselected, so each fire_*
    // returns before building an event record.
    engine.fire_client_connected(ClientConnectedEvent {
        client_id: "test".to_string(),
        username: None,
        peerhost: None,
        keepalive: 60,
        proto_ver: 4,
        clean_start: true,
        conn_ack_code: 0,
    });
    engine.fire_client_disconnected(ClientDisconnectedEvent {
        client_id: "test".to_string(),
        username: None,
        peerhost: None,
        reason: "normal".to_string(),
        disconnect_code: None,
    });
    engine.fire_client_connack(ClientConnackEvent {
        client_id: "test".to_string(),
        username: None,
        peerhost: None,
        return_code: 0,
        session_present: false,
    });
    engine.fire_client_check_authz_complete(ClientCheckAuthzCompleteEvent {
        client_id: "test".to_string(),
        username: None,
        peerhost: None,
        action: "publish".to_string(),
        topic: "t".to_string(),
        qos: 0,
        retain: Some(false),
        allow: true,
        source: "local".to_string(),
    });
    engine.fire_session_subscribed(SessionSubscribedEvent {
        client_id: "test".to_string(),
        username: None,
        peerhost: None,
        topic: "t".to_string(),
        qos: 0,
        nl: 0,
        rap: 0,
        rh: 0,
    });
    engine.fire_session_unsubscribed(SessionUnsubscribedEvent {
        client_id: "test".to_string(),
        username: None,
        peerhost: None,
        topic: "t".to_string(),
    });
    engine.fire_message_delivered(MessageDeliveredEvent {
        client_id: "test".to_string(),
        username: None,
        peerhost: None,
        topic: "t".to_string(),
        qos: 0,
        retain: false,
        payload: Bytes::new(),
        msg_id: None,
    });
    engine.fire_message_acked(MessageAckedEvent {
        client_id: "test".to_string(),
        username: None,
        peerhost: None,
        topic: "t".to_string(),
        qos: 0,
        msg_id: 1,
    });
    engine.fire_message_dropped(MessageDroppedEvent {
        client_id: "test".to_string(),
        username: None,
        peerhost: None,
        topic: "t".to_string(),
        qos: 0,
        reason: "reason".to_string(),
    });
    engine.fire_delivery_dropped(DeliveryDroppedEvent {
        client_id: "test".to_string(),
        username: None,
        peerhost: None,
        topic: "t".to_string(),
        qos: 0,
        reason: "reason".to_string(),
    });

    // No panic, no block: the aggregate gate is false, so a publish ingress
    // drains nothing and produces no output.
    drain_through_ingress(&engine, &sink).await;
    assert_eq!(
        sink.published.lock().unwrap().len(),
        0,
        "no event rules => no event output"
    );
}

/// Disabling a rule stops its event actions from firing through the ingress
/// drain path.
#[tokio::test]
async fn test_event_flag_cleared_on_disable() {
    let (engine, sink) = engine_with_sink();
    let rule = add_republish_rule(
        &engine,
        "$events/client_connected",
        "clientid",
        "out/on_disable",
    );

    engine.fire_client_connected(ClientConnectedEvent {
        client_id: "test-client".to_string(),
        username: None,
        peerhost: None,
        keepalive: 60,
        proto_ver: 4,
        clean_start: true,
        conn_ack_code: 0,
    });
    drain_through_ingress(&engine, &sink).await;
    assert_eq!(
        take_single_output(&sink, "out/on_disable")["clientid"].as_str(),
        Some("test-client"),
        "enabled rule fires through the drain"
    );
    sink.published.lock().unwrap().clear();

    // Disable the rule: the flag clears, so fire_* is a no-op and the drain
    // is skipped (aggregate gate goes false).
    engine.set_rule_enabled(&rule.id, false).expect("disabled");
    engine.fire_client_connected(ClientConnectedEvent {
        client_id: "test-client-2".to_string(),
        username: None,
        peerhost: None,
        keepalive: 60,
        proto_ver: 4,
        clean_start: true,
        conn_ack_code: 0,
    });
    drain_through_ingress(&engine, &sink).await;
    assert_eq!(
        sink.published.lock().unwrap().len(),
        0,
        "disabled rule must not republish through the drain"
    );
}

/// Removing a rule stops its event actions from firing through the ingress
/// drain path.
#[tokio::test]
async fn test_event_flag_cleared_on_remove() {
    let (engine, sink) = engine_with_sink();
    let rule = add_republish_rule(
        &engine,
        "$events/client_connected",
        "clientid",
        "out/on_remove",
    );

    engine.fire_client_connected(ClientConnectedEvent {
        client_id: "test-client".to_string(),
        username: None,
        peerhost: None,
        keepalive: 60,
        proto_ver: 4,
        clean_start: true,
        conn_ack_code: 0,
    });
    drain_through_ingress(&engine, &sink).await;
    assert_eq!(
        take_single_output(&sink, "out/on_remove")["clientid"].as_str(),
        Some("test-client"),
        "enabled rule fires through the drain"
    );
    sink.published.lock().unwrap().clear();

    // Remove the rule: the flag clears, so fire_* is a no-op and the drain
    // is skipped (aggregate gate goes false).
    engine.remove_rule(&rule.id).expect("removed");
    engine.fire_client_connected(ClientConnectedEvent {
        client_id: "test-client-2".to_string(),
        username: None,
        peerhost: None,
        keepalive: 60,
        proto_ver: 4,
        clean_start: true,
        conn_ack_code: 0,
    });
    drain_through_ingress(&engine, &sink).await;
    assert_eq!(
        sink.published.lock().unwrap().len(),
        0,
        "removed rule must not republish through the drain"
    );
}

// ---------------------------------------------------------------------------
// Per-event broker-path tests (RQ-04).
// ---------------------------------------------------------------------------
// Each test installs a rule over one `$events/...` source, fires the
// event the kernel fires at that point, drains through one real
// publish ingress, and asserts the output fields. The drain uses
// `dispatch_ingress`, the entry the node calls per publish.

// Unauthorized publish check fires with the denial fields.
#[tokio::test]
async fn client_check_authz_complete_unauthorized_through_broker() {
    let (engine, sink) = engine_with_sink();
    add_republish_rule(
        &engine,
        "$events/client_check_authz_complete",
        "clientid, action, topic, allow",
        "out/authz_deny",
    );
    engine.fire_client_check_authz_complete(ClientCheckAuthzCompleteEvent {
        client_id: "authz-client".to_string(),
        username: None,
        peerhost: None,
        action: "publish".to_string(),
        topic: "data/secret".to_string(),
        qos: 1,
        retain: Some(false),
        allow: false,
        source: "local".to_string(),
    });
    drain_through_ingress(&engine, &sink).await;
    let out = take_single_output(&sink, "out/authz_deny");
    assert_eq!(out["clientid"].as_str(), Some("authz-client"));
    assert_eq!(out["action"].as_str(), Some("publish"));
    assert_eq!(out["topic"].as_str(), Some("data/secret"));
    assert_eq!(out["allow"], false);
}

// Subscribe packet fires with the filter and option fields.
#[tokio::test]
async fn session_subscribed_through_broker() {
    let (engine, sink) = engine_with_sink();
    add_republish_rule(
        &engine,
        "$events/session_subscribed",
        "clientid, topic, qos",
        "out/subbed",
    );
    engine.fire_session_subscribed(SessionSubscribedEvent {
        client_id: "sub-client".to_string(),
        username: None,
        peerhost: None,
        topic: "data/subbed".to_string(),
        qos: 1,
        nl: 0,
        rap: 0,
        rh: 0,
    });
    drain_through_ingress(&engine, &sink).await;
    let out = take_single_output(&sink, "out/subbed");
    assert_eq!(out["clientid"].as_str(), Some("sub-client"));
    assert_eq!(out["topic"].as_str(), Some("data/subbed"));
    assert_eq!(out["qos"], 1);
}

// Unsubscribe packet fires with the filter field.
#[tokio::test]
async fn session_unsubscribed_through_broker() {
    let (engine, sink) = engine_with_sink();
    add_republish_rule(
        &engine,
        "$events/session_unsubscribed",
        "clientid, topic",
        "out/unsubbed",
    );
    engine.fire_session_unsubscribed(SessionUnsubscribedEvent {
        client_id: "sub-client".to_string(),
        username: None,
        peerhost: None,
        topic: "data/subbed".to_string(),
    });
    drain_through_ingress(&engine, &sink).await;
    let out = take_single_output(&sink, "out/unsubbed");
    assert_eq!(out["clientid"].as_str(), Some("sub-client"));
    assert_eq!(out["topic"].as_str(), Some("data/subbed"));
}

// QoS 1 delivery fires with the delivery fields.
#[tokio::test]
async fn message_delivered_through_broker() {
    let (engine, sink) = engine_with_sink();
    add_republish_rule(
        &engine,
        "$events/message_delivered",
        "clientid, topic, qos",
        "out/delivered",
    );
    engine.fire_message_delivered(MessageDeliveredEvent {
        client_id: "sub-client".to_string(),
        username: None,
        peerhost: None,
        topic: "data/delivered".to_string(),
        qos: 1,
        retain: false,
        payload: Bytes::from_static(b"hi"),
        msg_id: Some(7),
    });
    drain_through_ingress(&engine, &sink).await;
    let out = take_single_output(&sink, "out/delivered");
    assert_eq!(out["clientid"].as_str(), Some("sub-client"));
    assert_eq!(out["topic"].as_str(), Some("data/delivered"));
    assert_eq!(out["qos"], 1);
}

// Client ack fires with the packet id field.
#[tokio::test]
async fn message_acked_through_broker() {
    let (engine, sink) = engine_with_sink();
    add_republish_rule(
        &engine,
        "$events/message_acked",
        "clientid, topic, msg_id",
        "out/acked",
    );
    engine.fire_message_acked(MessageAckedEvent {
        client_id: "sub-client".to_string(),
        username: None,
        peerhost: None,
        topic: "data/acked".to_string(),
        qos: 1,
        msg_id: 11,
    });
    drain_through_ingress(&engine, &sink).await;
    let out = take_single_output(&sink, "out/acked");
    assert_eq!(out["clientid"].as_str(), Some("sub-client"));
    assert_eq!(out["topic"].as_str(), Some("data/acked"));
    assert_eq!(out["msg_id"], 11);
}

// Queue limit drop fires with the reason field.
#[tokio::test]
async fn message_dropped_through_broker() {
    let (engine, sink) = engine_with_sink();
    add_republish_rule(
        &engine,
        "$events/message_dropped",
        "clientid, topic, reason",
        "out/dropped",
    );
    engine.fire_message_dropped(MessageDroppedEvent {
        client_id: "slow-client".to_string(),
        username: None,
        peerhost: None,
        topic: "data/drop".to_string(),
        qos: 1,
        reason: "quota_exceeded".to_string(),
    });
    drain_through_ingress(&engine, &sink).await;
    let out = take_single_output(&sink, "out/dropped");
    assert_eq!(out["clientid"].as_str(), Some("slow-client"));
    assert_eq!(out["topic"].as_str(), Some("data/drop"));
    assert_eq!(out["reason"].as_str(), Some("quota_exceeded"));
}

// Expired delivery fires with the reason field.
#[tokio::test]
async fn delivery_dropped_through_broker() {
    let (engine, sink) = engine_with_sink();
    add_republish_rule(
        &engine,
        "$events/delivery_dropped",
        "clientid, topic, reason",
        "out/ddropped",
    );
    engine.fire_delivery_dropped(DeliveryDroppedEvent {
        client_id: "dead-client".to_string(),
        username: None,
        peerhost: None,
        topic: "data/dead".to_string(),
        qos: 1,
        reason: "dead_mailbox".to_string(),
    });
    drain_through_ingress(&engine, &sink).await;
    let out = take_single_output(&sink, "out/ddropped");
    assert_eq!(out["clientid"].as_str(), Some("dead-client"));
    assert_eq!(out["topic"].as_str(), Some("data/dead"));
    assert_eq!(out["reason"].as_str(), Some("dead_mailbox"));
}
