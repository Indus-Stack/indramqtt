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
use broker_router::{Router, Subscription};
use broker_rules::{
    runtime as rule_runtime, BackpressurePolicy, BrokerSink, RuleAction, RuleEngine,
    RuleEngineError,
};
use broker_session::SessionManager;
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
            "default",
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

/// Sink for the broker path. It fans out each rule output through
/// the router to live sessions of the same tenant. It records each
/// delivery as `(tenant, client, topic)`.
struct BrokerPathSink {
    router: Arc<Router>,
    sessions: Arc<SessionManager>,
    delivered: StdMutex<Vec<(String, String, String)>>,
}

impl BrokerPathSink {
    fn with_parts(router: Arc<Router>, sessions: Arc<SessionManager>) -> Self {
        Self {
            router,
            sessions,
            delivered: StdMutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl BrokerSink for BrokerPathSink {
    async fn publish(
        &self,
        topic: Topic,
        payload: Bytes,
        qos: QoS,
        retain: bool,
    ) -> Result<(), RuleEngineError> {
        self.publish_in_tenant("default", topic, payload, qos, retain)
            .await
    }

    async fn publish_in_tenant(
        &self,
        tenant: &str,
        topic: Topic,
        _payload: Bytes,
        _qos: QoS,
        _retain: bool,
    ) -> Result<(), RuleEngineError> {
        let (matched, _) = self.router.matches_in_tenant_counted(&topic, None, tenant);
        for sub in matched.iter() {
            let session = self.sessions.get_in_tenant(tenant, sub.client_id.as_ref());
            if let Some(session) = session {
                let connected = *session.connected.read();
                let live = *session.conn_id.read();
                if connected && live == Some(sub.conn_id) {
                    self.delivered.lock().unwrap().push((
                        tenant.to_string(),
                        sub.client_id.to_string(),
                        topic.as_str().to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Connect one real client. It creates the session record and pins
/// the edge connection, like the kernel bind hook does.
fn connect_client(sessions: &SessionManager, tenant: &str, client_id: &str, conn_id: u64) {
    let (session, _) = sessions.get_or_create_in_tenant(tenant, client_id, true);
    sessions.bind_session(&session, conn_id);
    *session.keepalive_secs.write() = 60;
    assert!(*session.connected.read());
}

/// Subscribe one real client. It records the grant on the router
/// and on the session, like the kernel subscribe path does.
fn subscribe_client(
    router: &Router,
    sessions: &SessionManager,
    tenant: &str,
    client_id: &str,
    conn_id: u64,
    filter: &str,
) {
    router.subscribe(
        &TopicFilter::new(filter).expect("filter parses"),
        Subscription::new_in_tenant(client_id, conn_id, QoS::AtMostOnce, tenant),
    );
    sessions.add_subscription_in_tenant(
        tenant,
        client_id,
        TopicFilter::new(filter).expect("filter parses"),
        QoS::AtMostOnce,
    );
}

/// Make one republish action to a concrete topic.
fn republish_to(out: &str) -> Vec<RuleAction> {
    vec![RuleAction::Republish {
        topic: Topic::new(out).expect("out topic parses"),
        qos: QoS::AtMostOnce,
    }]
}

/// Make one random value. The broker cannot know it in advance.
fn random_secret(tag: &str) -> Bytes {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|span| span.as_nanos())
        .unwrap_or(0);
    Bytes::from(format!("tm04-{tag}-{nanos}"))
}

/// Rule of tenant A fires for a publish of tenant A only.
/// It delivers to the subscriber of tenant A only.
#[tokio::test]
async fn tm04_rule_in_tenant_a_fires_only_for_a_through_broker_path() {
    let router = Arc::new(Router::new());
    let sessions = Arc::new(SessionManager::new());
    connect_client(&sessions, "tm04-a", "tm04-a-sub", 4101);
    connect_client(&sessions, "tm04-b", "tm04-b-sub", 4102);
    subscribe_client(&router, &sessions, "tm04-a", "tm04-a-sub", 4101, "tm04/out");
    subscribe_client(&router, &sessions, "tm04-b", "tm04-b-sub", 4102, "tm04/out");
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    engine
        .create_rule_in_tenant(
            "tm04-a".to_string(),
            "tm04-a-rule".to_string(),
            TopicFilter::new("tm04/in").expect("filter parses"),
            None,
            true,
            republish_to("tm04/out"),
        )
        .expect("rule creates");
    let sink = Arc::new(BrokerPathSink::with_parts(router, sessions));
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();
    let topic = Topic::new("tm04/in").expect("topic parses");
    // Publish of tenant A fires once and delivers to A only.
    let fired = engine
        .dispatch_ingress_in_tenant(
            "tm04-a",
            &topic,
            &random_secret("a"),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;
    assert_eq!(fired, 1);
    // Publish of tenant B fires nothing and delivers nothing.
    let fired = engine
        .dispatch_ingress_in_tenant(
            "tm04-b",
            &topic,
            &random_secret("b"),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;
    assert_eq!(fired, 0);
    let rows = sink.delivered.lock().unwrap().clone();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "tm04-a");
    assert_eq!(rows[0].1, "tm04-a-sub");
    assert_eq!(rows[0].2, "tm04/out");
}

/// Rule output of tenant A reaches a subscriber of tenant A.
/// It never reaches a subscriber of tenant B.
#[tokio::test]
async fn tm04_rule_output_stays_in_own_tenant_through_broker_path() {
    let router = Arc::new(Router::new());
    let sessions = Arc::new(SessionManager::new());
    connect_client(&sessions, "tm04-a", "tm04-a-sub", 4201);
    connect_client(&sessions, "tm04-b", "tm04-b-sub", 4202);
    subscribe_client(
        &router,
        &sessions,
        "tm04-a",
        "tm04-a-sub",
        4201,
        "tm04/shared",
    );
    subscribe_client(
        &router,
        &sessions,
        "tm04-b",
        "tm04-b-sub",
        4202,
        "tm04/shared",
    );
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    for (tenant, name) in [("tm04-a", "rule-a"), ("tm04-b", "rule-b")] {
        engine
            .create_rule_in_tenant(
                tenant.to_string(),
                name.to_string(),
                TopicFilter::new("tm04/in").expect("filter parses"),
                None,
                true,
                republish_to("tm04/shared"),
            )
            .expect("rule creates");
    }
    let sink = Arc::new(BrokerPathSink::with_parts(router, sessions));
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();
    let topic = Topic::new("tm04/in").expect("topic parses");
    let fired = engine
        .dispatch_ingress_in_tenant(
            "tm04-a",
            &topic,
            &random_secret("a"),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;
    assert_eq!(fired, 1);
    let rows = sink.delivered.lock().unwrap().clone();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "tm04-a");
    assert_eq!(rows[0].1, "tm04-a-sub");
}

/// Rule from before tenancy serves the default tenant.
/// It fires for default ingress and never for other tenants.
#[tokio::test]
async fn tm04_legacy_rule_serves_default_tenant_through_broker_path() {
    let router = Arc::new(Router::new());
    let sessions = Arc::new(SessionManager::new());
    connect_client(&sessions, "default", "tm04-legacy-sub", 4301);
    subscribe_client(
        &router,
        &sessions,
        "default",
        "tm04-legacy-sub",
        4301,
        "tm04/legacy-out",
    );
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    let rule = engine
        .create_rule(
            "legacy".to_string(),
            TopicFilter::new("tm04/legacy-in").expect("filter parses"),
            None,
            true,
            republish_to("tm04/legacy-out"),
        )
        .expect("rule creates");
    assert_eq!(rule.tenant, "default");
    let sink = Arc::new(BrokerPathSink::with_parts(router, sessions));
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();
    let topic = Topic::new("tm04/legacy-in").expect("topic parses");
    let fired = engine
        .dispatch_ingress(&topic, &random_secret("d"), QoS::AtMostOnce, &sink_obj)
        .await;
    assert_eq!(fired, 1);
    let fired = engine
        .dispatch_ingress_in_tenant(
            "tm04-a",
            &topic,
            &random_secret("a"),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;
    assert_eq!(fired, 0);
    let rows = sink.delivered.lock().unwrap().clone();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "default");
    assert_eq!(rows[0].1, "tm04-legacy-sub");
}
