//! RX-01 done-when tests: a plain rule, a tumbling-window rule and a
//! hopping-window rule, each publishing through the broker and asserting
//! the rule's output reaches a sink with exact counts.
//!
//! Every test drives [`broker_rules::RuleEngine::dispatch_ingress`], the
//! same entry the node calls per publish from
//! `ingress_pipeline_with_publisher` (`crates/broker-node/src/main.rs`)
//! and from the management publish path
//! (`crates/broker-api/src/v5/clients.rs`): connect/publish/deliver
//! through the broker, never `sink.send` directly and never the store
//! alone. Window rules additionally traverse the embedded runtime: one
//! [`rekuiper_core::StreamRecord`] conversion per ingress in
//! `dispatch_ingress`, the bounded [`rekuiper_core::StreamBus`] feed, the
//! per-rule task and the sink bridge to [`broker_rules::BrokerSink`].
//! At-least-once permits duplicates, never loss: the tests assert every
//! expected row is present with exact counts.
//!
//! The bounds test pins the spec table: ingress 1024, bus 4096 per
//! subscriber, sink 10 000 per rule, task feed 65 536.

use async_trait::async_trait;
use broker_protocol::{QoS, Topic, TopicFilter};
use broker_rules::{
    runtime as rule_runtime, BackpressurePolicy, BrokerSink, RuleAction, RuleEngine,
    RuleEngineError,
};
use bytes::Bytes;
use std::sync::{Arc, Mutex as StdMutex};

/// Recording sink: every republished row lands here with its topic, so the
/// tests assert exact delivery counts through the broker's sink interface.
#[derive(Debug, Default)]
struct RecordingSink {
    published: StdMutex<Vec<(String, serde_json::Value)>>,
}

#[async_trait]
impl BrokerSink for RecordingSink {
    async fn publish(
        &self,
        topic: Topic,
        payload: Bytes,
        _qos: QoS,
        _retain: bool,
    ) -> Result<(), RuleEngineError> {
        let value: serde_json::Value =
            serde_json::from_slice(&payload).unwrap_or(serde_json::Value::Null);
        self.published
            .lock()
            .unwrap()
            .push((topic.as_str().to_string(), value));
        Ok(())
    }
}

fn republish_action() -> Vec<RuleAction> {
    vec![RuleAction::Republish {
        topic: Topic::new("bench/out").expect("out topic parses"),
        qos: QoS::AtMostOnce,
    }]
}

async fn ingress(engine: &RuleEngine, sink: &Arc<dyn BrokerSink>, payload: &'static [u8]) -> usize {
    engine
        .dispatch_ingress(
            &Topic::new("sensors/kitchen").expect("ingress topic parses"),
            &Bytes::from_static(payload),
            QoS::AtMostOnce,
            sink,
        )
        .await
}

fn published_rows(sink: &RecordingSink) -> Vec<serde_json::Value> {
    sink.published
        .lock()
        .unwrap()
        .iter()
        .map(|(_, payload)| payload.clone())
        .collect()
}

async fn wait_rows(sink: &Arc<RecordingSink>, n: usize) {
    for _ in 0..60 {
        if sink.published.lock().unwrap().len() >= n {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!(
        "timed out waiting for {n} rows (got {})",
        sink.published.lock().unwrap().len()
    );
}

/// Done-when (plain): a plain rule publishes through the broker and every
/// evaluation reaches the sink with exact counts.
#[tokio::test]
async fn runtime_plain_rule_through_broker_exact_counts() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    engine
        .create_rule(
            "plain".to_string(),
            TopicFilter::new("sensors/#").expect("filter parses"),
            Some("SELECT payload.x FROM \"sensors/#\" WHERE payload.x > 10".to_string()),
            true,
            republish_action(),
        )
        .expect("rule creates");
    let sink = Arc::new(RecordingSink::default());
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();

    // Nested shape: `payload` resolves to the top-level `payload` key.
    for _ in 0..5 {
        ingress(&engine, &sink_obj, b"{\"payload\": {\"x\": 20}}").await;
    }
    // Rejected by WHERE: never reaches the sink.
    ingress(&engine, &sink_obj, b"{\"payload\": {\"x\": 1}}").await;

    assert_eq!(
        published_rows(&sink).len(),
        5,
        "every matching publish must reach the sink, exactly once each"
    );
    assert_eq!(engine.runtime_feed_dropped(), 0);
}

/// Done-when (tumbling): a tumbling-window rule publishes through the
/// broker's runtime and its aggregate reaches the sink exactly once; empty
/// windows dispatch nothing afterwards.
#[tokio::test]
async fn runtime_tumbling_window_through_broker_exact_counts() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    let rule = engine
        .create_rule(
            "tumbling".to_string(),
            TopicFilter::new("sensors/#").expect("filter parses"),
            Some(
                "SELECT avg(temperature) AS a, count(*) AS n FROM \"sensors/#\" GROUP BY TUMBLINGWINDOW(ms, 150)".to_string(),
            ),
            true,
            republish_action(),
        )
        .expect("rule creates");
    // Tier labels stay for display: the window clause selects the runtime.
    assert_eq!(engine.rule_task_count(), 1);
    assert_eq!(
        engine.rule_task_status(&rule.id).as_deref(),
        Some("running")
    );
    let sink = Arc::new(RecordingSink::default());
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();
    engine.set_broker_sink(sink_obj.clone());

    ingress(&engine, &sink_obj, br#"{ "temperature": 10.0 }"#).await;
    ingress(&engine, &sink_obj, br#"{ "temperature": 20.0 }"#).await;
    ingress(&engine, &sink_obj, br#"{ "temperature": 30.0 }"#).await;
    wait_rows(&sink, 1).await;
    // Empty windows dispatch nothing: still exactly one row later.
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    let rows = published_rows(&sink);
    assert_eq!(rows.len(), 1, "one closed window, one row, got {rows:?}");
    assert_eq!(rows[0]["a"], serde_json::json!(20.0));
    assert_eq!(rows[0]["n"], serde_json::json!(3));

    // Per-rule runtime counters move on the runtime path.
    let counters = engine
        .rule_task_counters(&rule.id)
        .expect("task counters exist");
    assert!(
        counters
            .source_in
            .load(std::sync::atomic::Ordering::Relaxed)
            >= 3
    );
    assert!(counters.sink_out.load(std::sync::atomic::Ordering::Relaxed) >= 1);
    assert_eq!(engine.runtime_feed_dropped(), 0);
}

/// Done-when (hopping): a hopping-window rule publishes through the
/// broker's runtime and every tick inside the span emits the same trailing
/// aggregate with exact counts.
#[tokio::test]
async fn runtime_hopping_window_through_broker_exact_counts() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    engine
        .create_rule(
            "hopping".to_string(),
            TopicFilter::new("sensors/#").expect("filter parses"),
            Some(
                "SELECT avg(temperature) AS a FROM \"sensors/#\" GROUP BY HOPPINGWINDOW(ss, 30, 1)"
                    .to_string(),
            ),
            true,
            republish_action(),
        )
        .expect("rule creates");
    let sink = Arc::new(RecordingSink::default());
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();
    engine.set_broker_sink(sink_obj.clone());

    ingress(&engine, &sink_obj, br#"{ "temperature": 10.0 }"#).await;
    ingress(&engine, &sink_obj, br#"{ "temperature": 30.0 }"#).await;
    // First tick inside the span emits the trailing aggregate. The next
    // tick is a full second away, so the count right after the wait is
    // exact; one more tick later the same window re-emits, still exact.
    wait_rows(&sink, 1).await;
    assert_eq!(
        published_rows(&sink).len(),
        1,
        "first tick emits exactly one row"
    );
    assert_eq!(published_rows(&sink)[0]["a"], serde_json::json!(20.0));
    tokio::time::sleep(std::time::Duration::from_millis(1_200)).await;
    let rows = published_rows(&sink);
    assert_eq!(rows.len(), 2, "second tick re-emits once, got {rows:?}");
    for row in &rows {
        assert_eq!(row["a"], serde_json::json!(20.0));
    }
    assert_eq!(engine.runtime_feed_dropped(), 0);
}

/// The runtime feed also admits whole batches with backpressure
/// ([`rekuiper_core::StreamSender::send_batch`]): a two-record batch for a
/// `COUNTWINDOW(2)` rule closes exactly one window through the broker.
#[tokio::test]
async fn runtime_bus_batch_admission_through_broker() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    engine
        .create_rule(
            "batched".to_string(),
            TopicFilter::new("sensors/#").expect("filter parses"),
            Some(
                "SELECT avg(temperature) AS a FROM \"sensors/#\" GROUP BY COUNTWINDOW(2)"
                    .to_string(),
            ),
            true,
            republish_action(),
        )
        .expect("rule creates");
    let sink = Arc::new(RecordingSink::default());
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();
    engine.set_broker_sink(sink_obj.clone());

    let records = [
        br#"{ "temperature": 10.0 }"#.as_slice(),
        br#"{ "temperature": 30.0 }"#.as_slice(),
    ]
    .iter()
    .map(|payload| {
        rule_runtime::ingress_to_stream_record(
            &Topic::new("sensors/kitchen").expect("ingress topic parses"),
            &Bytes::copy_from_slice(payload),
            QoS::AtMostOnce,
        )
    })
    .collect::<Vec<_>>();
    engine
        .ingress_sender()
        .send_batch(records)
        .await
        .expect("batch admits");

    wait_rows(&sink, 1).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let rows = published_rows(&sink);
    assert_eq!(rows.len(), 1, "one batch, one window, got {rows:?}");
    assert_eq!(rows[0]["a"], serde_json::json!(20.0));
}

/// Spec bounds table, pinned: every default finite with its reason in
/// [`broker_rules::runtime`].
#[test]
fn runtime_bounds_table_pinned() {
    assert_eq!(rule_runtime::RULE_INGRESS_QUEUE_CAPACITY, 1_024);
    assert_eq!(rule_runtime::STREAM_QUEUE_CAPACITY, 4_096);
    assert_eq!(rule_runtime::PER_RULE_SINK_QUEUE_DEPTH, 10_000);
    assert_eq!(rule_runtime::RULE_TASK_FEED_DEPTH, 65_536);
    assert_eq!(
        broker_rules::DEFAULT_WINDOW_CHANNEL_DEPTH,
        65_536,
        "feed depth alias must match the task feed bound"
    );
}
