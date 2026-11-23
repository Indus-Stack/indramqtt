use super::*;

fn published_rows(sink: &RecordingSink) -> Vec<serde_json::Value> {
    sink.published
        .lock()
        .unwrap()
        .iter()
        .map(|(_, payload, _, _)| serde_json::from_slice(payload).expect("row is JSON"))
        .collect()
}

/// Build an engine with one rule; returns engine, sink handle, and
/// the stored rule (for tier assertions). Mirrors production wiring:
/// the same sink serves inline dispatch and window flushes.
fn window_rule(
    sql: Option<&str>,
    actions: Vec<RuleAction>,
) -> (RuleEngine, Arc<RecordingSink>, Arc<dyn BrokerSink>, Rule) {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    let rule = engine
        .create_rule(
            "window-probe".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            sql.map(str::to_string),
            true,
            actions,
        )
        .expect("rule creates");
    let sink = Arc::new(RecordingSink::default());
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();
    engine.set_broker_sink(sink_obj.clone());
    (engine, sink, sink_obj, rule)
}

fn republish_action() -> Vec<RuleAction> {
    vec![RuleAction::Republish {
        topic: Topic::new("alerts/agg").unwrap(),
        qos: QoS::AtMostOnce,
    }]
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
#[test]
fn test_window_rule_created_outside_runtime_defers_task() {
    // Plain #[test]: no Tokio runtime, so eager spawn is impossible.
    // Creation still succeeds; the task arrives on first ingress.
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    let rule = engine
        .create_rule(
            "w".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            Some(
                r#"SELECT avg(temperature) AS a FROM "sensors/+" GROUP BY TUMBLINGWINDOW(ss, 10)"#
                    .to_string(),
            ),
            true,
            vec![RuleAction::Log],
        )
        .expect("rule creates");
    assert_eq!(rule.tier, RuleTier::Enterprise);
    assert_eq!(engine.rule_task_count(), 0);
    assert!(engine.get_rule(&rule.id).is_some());
}

#[tokio::test]
async fn test_rule_task_lifecycle() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    assert_eq!(engine.rule_task_count(), 0);
    let rule = engine
        .create_rule(
            "w".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            Some(
                r#"SELECT avg(temperature) AS a FROM "sensors/+" GROUP BY TUMBLINGWINDOW(ss, 30)"#
                    .to_string(),
            ),
            true,
            vec![RuleAction::Log],
        )
        .expect("rule creates");
    assert_eq!(engine.rule_task_count(), 1);
    // Re-creating is per-rule; removing stops and aborts the task.
    assert!(engine
        .remove_rule(&rule.id)
        .expect("memory-only remove cannot fail"));
    assert_eq!(engine.rule_task_count(), 0);
    assert!(!engine
        .remove_rule(&rule.id)
        .expect("memory-only remove cannot fail"));
    assert!(!engine
        .remove_rule("rule-999")
        .expect("memory-only remove cannot fail"));
}

#[tokio::test]
async fn test_count_window_aggregates_on_threshold() {
    let (engine, sink, sink_obj, rule) = window_rule(
        Some(r#"SELECT avg(temperature) AS avg_temp FROM "sensors/+" GROUP BY COUNTWINDOW(2)"#),
        republish_action(),
    );
    assert_eq!(rule.tier, RuleTier::Enterprise);

    // Two records close one window: exactly one aggregate row.
    ingress(&engine, &sink_obj, br#"{ "temperature": 10.0 }"#).await;
    ingress(&engine, &sink_obj, br#"{ "temperature": 30.0 }"#).await;
    wait_rows(&sink, 1).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let rows = published_rows(&sink);
    assert_eq!(rows.len(), 1, "one row per closed window, got {rows:?}");
    assert_eq!(rows[0]["avg_temp"], serde_json::json!(20.0));
}

#[tokio::test]
async fn test_count_window_where_gate() {
    let (engine, sink, sink_obj, _) = window_rule(
        Some(
            r#"SELECT avg(temperature) AS avg_temp FROM "sensors/+" WHERE temperature > 50.0 GROUP BY COUNTWINDOW(2)"#,
        ),
        republish_action(),
    );

    // Below-threshold ingress never reaches the buffer...
    ingress(&engine, &sink_obj, br#"{ "temperature": 10.0 }"#).await;
    // ...so these two close the first window alone.
    ingress(&engine, &sink_obj, br#"{ "temperature": 60.0 }"#).await;
    ingress(&engine, &sink_obj, br#"{ "temperature": 70.0 }"#).await;
    wait_rows(&sink, 1).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let rows = published_rows(&sink);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["avg_temp"], serde_json::json!(65.0));
}

#[tokio::test]
async fn test_tumbling_window_aggregates_and_skips_empty() {
    let (engine, sink, sink_obj, _) = window_rule(
        Some(
            r#"SELECT avg(temperature) AS avg_temp, sum(temperature) AS total, count(*) AS n, min(temperature) AS lo, max(temperature) AS hi FROM "sensors/+" GROUP BY TUMBLINGWINDOW(ms, 150)"#,
        ),
        republish_action(),
    );

    ingress(&engine, &sink_obj, br#"{ "temperature": 10.0 }"#).await;
    ingress(&engine, &sink_obj, br#"{ "temperature": 20.0 }"#).await;
    ingress(&engine, &sink_obj, br#"{ "temperature": 30.0 }"#).await;
    wait_rows(&sink, 1).await;
    let rows = published_rows(&sink);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["avg_temp"], serde_json::json!(20.0));
    assert_eq!(rows[0]["total"], serde_json::json!(60.0));
    assert_eq!(rows[0]["n"], serde_json::json!(3));
    assert_eq!(rows[0]["lo"], serde_json::json!(10.0));
    assert_eq!(rows[0]["hi"], serde_json::json!(30.0));

    // Empty windows dispatch nothing: still exactly one row later.
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    assert_eq!(published_rows(&sink).len(), 1);
}

#[tokio::test]
async fn test_group_by_partitions_per_sensor() {
    let (engine, sink, sink_obj, _) = window_rule(
        Some(
            r#"SELECT sensor_id, avg(temperature) AS avg_temp FROM "sensors/+" GROUP BY sensor_id, COUNTWINDOW(3)"#,
        ),
        republish_action(),
    );

    ingress(
        &engine,
        &sink_obj,
        br#"{ "sensor_id": "a", "temperature": 10.0 }"#,
    )
    .await;
    ingress(
        &engine,
        &sink_obj,
        br#"{ "sensor_id": "b", "temperature": 30.0 }"#,
    )
    .await;
    ingress(
        &engine,
        &sink_obj,
        br#"{ "sensor_id": "a", "temperature": 20.0 }"#,
    )
    .await;
    wait_rows(&sink, 2).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let rows = published_rows(&sink);
    assert_eq!(rows.len(), 2, "one row per group, got {rows:?}");
    // First-seen group order is deterministic.
    assert_eq!(rows[0]["sensor_id"], serde_json::json!("a"));
    assert_eq!(rows[0]["avg_temp"], serde_json::json!(15.0));
    assert_eq!(rows[1]["sensor_id"], serde_json::json!("b"));
    assert_eq!(rows[1]["avg_temp"], serde_json::json!(30.0));
}

#[tokio::test]
async fn test_window_bounds_resolve_in_output() {
    let (engine, sink, sink_obj, _) = window_rule(
        Some(
            r#"SELECT avg(temperature) AS a, window_start() AS ws, window_end() AS we FROM "sensors/+" GROUP BY COUNTWINDOW(1)"#,
        ),
        republish_action(),
    );

    ingress(&engine, &sink_obj, br#"{ "temperature": 5.0 }"#).await;
    wait_rows(&sink, 1).await;
    let rows = published_rows(&sink);
    assert_eq!(rows.len(), 1);
    let ws = rows[0]["ws"].as_u64().expect("window_start resolves");
    let we = rows[0]["we"].as_u64().expect("window_end resolves");
    assert!(ws > 0 && we >= ws, "sane bounds: ws={ws} we={we}");
    assert!(
        we - ws < 60_000,
        "count-window bounds span the batch, not history"
    );
}

#[tokio::test]
async fn test_hopping_window_smoke() {
    let (engine, sink, sink_obj, _) = window_rule(
        Some(
            r#"SELECT avg(temperature) AS a FROM "sensors/+" GROUP BY HOPPINGWINDOW(ms, 300, 150)"#,
        ),
        republish_action(),
    );

    ingress(&engine, &sink_obj, br#"{ "temperature": 10.0 }"#).await;
    ingress(&engine, &sink_obj, br#"{ "temperature": 30.0 }"#).await;
    // Overlapping ticks eventually flush the trailing window.
    wait_rows(&sink, 1).await;
    let rows = published_rows(&sink);
    assert!(!rows.is_empty());
    assert!(rows[0]["a"].is_number());
}

#[tokio::test]
async fn test_sliding_window_aggregates_per_arrival() {
    let (engine, sink, sink_obj, _) = window_rule(
        Some(r#"SELECT avg(temperature) AS a FROM "sensors/+" GROUP BY SLIDINGWINDOW(ss, 60)"#),
        republish_action(),
    );

    // Every arrival re-aggregates the trailing window, in order.
    ingress(&engine, &sink_obj, br#"{ "temperature": 10.0 }"#).await;
    ingress(&engine, &sink_obj, br#"{ "temperature": 30.0 }"#).await;
    wait_rows(&sink, 2).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let rows = published_rows(&sink);
    assert_eq!(rows.len(), 2, "one row per arrival, got {rows:?}");
    assert_eq!(rows[0]["a"], serde_json::json!(10.0));
    assert_eq!(rows[1]["a"], serde_json::json!(20.0));
}

#[tokio::test]
async fn test_window_forward_connector_dispatch() {
    use broker_connectors::Sink as ConnectorSinkTrait;

    #[derive(Debug, Default)]
    struct ProbeConnector {
        events: StdMutex<Vec<serde_json::Value>>,
    }

    #[async_trait]
    impl ConnectorSinkTrait for ProbeConnector {
        async fn send(
            &self,
            _topic: &Topic,
            payload: &Bytes,
            _qos: QoS,
        ) -> Result<(), broker_connectors::ConnectorError> {
            self.events
                .lock()
                .unwrap()
                .push(serde_json::from_slice(payload).expect("row JSON"));
            Ok(())
        }

        fn kind(&self) -> &'static str {
            "probe"
        }
    }

    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    let probe = Arc::new(ProbeConnector::default());
    engine.connectors().register("probe", probe.clone());
    engine
        .create_rule(
            "w".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            Some(
                r#"SELECT avg(temperature) AS a FROM "sensors/+" GROUP BY COUNTWINDOW(2)"#
                    .to_string(),
            ),
            true,
            vec![RuleAction::ForwardConnector {
                connector_id: "probe".to_string(),
            }],
        )
        .expect("rule creates");
    let sink: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());
    let sink_obj = sink;

    ingress(&engine, &sink_obj, br#"{ "temperature": 8.0 }"#).await;
    ingress(&engine, &sink_obj, br#"{ "temperature": 12.0 }"#).await;
    for _ in 0..60 {
        if !probe.events.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let events = probe.events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["a"], serde_json::json!(10.0));
}

#[test]
fn test_window_channel_depth_configurable() {
    // INDRA-215: default is the high-scale 65,536; the constructor
    // and builder override with a floor of 1 and no ceiling.
    assert_eq!(DEFAULT_WINDOW_CHANNEL_DEPTH, 65_536);
    assert_eq!(
        RuleEngine::new(16, BackpressurePolicy::DropOldest).window_channel_depth(),
        65_536
    );
    assert_eq!(
        RuleEngine::new_with_window_depth(16, BackpressurePolicy::DropOldest, 10_000)
            .window_channel_depth(),
        10_000
    );
    assert_eq!(
        RuleEngine::new(16, BackpressurePolicy::DropOldest)
            .with_window_channel_depth(1_000_000)
            .window_channel_depth(),
        1_000_000
    );
    assert_eq!(
        RuleEngine::new(16, BackpressurePolicy::DropOldest)
            .with_window_channel_depth(0)
            .window_channel_depth(),
        1
    );
}

#[tokio::test]
async fn test_window_channel_depth_reaches_worker() {
    // The spawned task's feed bound equals the configured depth.
    async fn worker_capacity(depth: usize) -> usize {
        let engine = RuleEngine::new_with_window_depth(16, BackpressurePolicy::DropOldest, depth);
        let sink: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());
        engine
            .create_rule(
                "w".to_string(),
                TopicFilter::new("sensors/+").unwrap(),
                Some(
                    r#"SELECT avg(temperature) AS a FROM "sensors/+" GROUP BY COUNTWINDOW(2)"#
                        .to_string(),
                ),
                true,
                vec![],
            )
            .expect("rule creates");
        ingress(&engine, &sink, br#"{ "temperature": 1.0 }"#).await;
        let tasks = engine.rule_tasks.read();
        let task = tasks.get("rule-1").expect("task spawned");
        task.feed_tx.max_capacity()
    }
    assert_eq!(worker_capacity(65_536).await, 65_536);
    assert_eq!(worker_capacity(10_000).await, 10_000);
}

#[tokio::test]
async fn test_window_burst_twelve_k_without_drop() {
    // INDRA-215: 12,000 ingress events over COUNTWINDOW(100) flush
    // 120 windows with zero drops at the default depth.
    #[derive(Debug, Default)]
    struct BurstProbe {
        events: StdMutex<Vec<serde_json::Value>>,
    }
    #[async_trait::async_trait]
    impl broker_connectors::Sink for BurstProbe {
        async fn send(
            &self,
            _topic: &Topic,
            payload: &Bytes,
            _qos: QoS,
        ) -> Result<(), broker_connectors::ConnectorError> {
            self.events
                .lock()
                .unwrap()
                .push(serde_json::from_slice(payload).expect("row JSON"));
            Ok(())
        }

        fn kind(&self) -> &'static str {
            "burst-probe"
        }
    }

    let engine = RuleEngine::new(65_536, BackpressurePolicy::DropOldest);
    let probe = Arc::new(BurstProbe::default());
    engine.connectors().register("burst", probe.clone());
    engine
        .create_rule(
            "burst-rule".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            Some(
                r#"SELECT avg(temperature) AS a FROM "sensors/+" GROUP BY COUNTWINDOW(100)"#
                    .to_string(),
            ),
            true,
            vec![RuleAction::ForwardConnector {
                connector_id: "burst".to_string(),
            }],
        )
        .expect("rule creates");
    let sink: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());
    for _ in 0..12_000 {
        ingress(&engine, &sink, br#"{ "temperature": 20.0 }"#).await;
    }
    for _ in 0..400 {
        if probe.events.lock().unwrap().len() >= 120 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let events = probe.events.lock().unwrap();
    assert_eq!(events.len(), 120, "burst dropped window flushes");
    assert_eq!(events[0]["a"], serde_json::json!(20.0));
}

#[tokio::test]
async fn test_remove_rule_stops_window_delivery() {
    let (engine, sink, sink_obj, rule) = window_rule(
        Some(r#"SELECT avg(temperature) AS a FROM "sensors/+" GROUP BY TUMBLINGWINDOW(ss, 30)"#),
        republish_action(),
    );
    assert_eq!(engine.rule_task_count(), 1);

    // Records buffer, then the rule disappears before any close.
    for _ in 0..5 {
        ingress(&engine, &sink_obj, br#"{ "temperature": 1.0 }"#).await;
    }
    assert!(engine
        .remove_rule(&rule.id)
        .expect("memory-only remove cannot fail"));
    assert_eq!(engine.rule_task_count(), 0);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(
        published_rows(&sink).is_empty(),
        "aborted task must not deliver"
    );
}
