use super::*;

#[derive(Debug, Default)]
struct RecordingConnector {
    events: StdMutex<Vec<(String, Vec<u8>, u8)>>,
}

#[async_trait]
impl broker_connectors::Sink for RecordingConnector {
    async fn send(
        &self,
        topic: &Topic,
        payload: &Bytes,
        qos: QoS,
    ) -> Result<(), broker_connectors::ConnectorError> {
        self.events.lock().unwrap().push((
            topic.as_str().to_string(),
            payload.to_vec(),
            u8::from(qos),
        ));
        Ok(())
    }

    fn kind(&self) -> &'static str {
        "test"
    }
}

#[tokio::test]
async fn test_forward_connector_receives_projected_payload() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    let recorder: Arc<RecordingConnector> = Arc::new(RecordingConnector::default());
    engine.connectors().register("webhook", recorder.clone());

    engine
        .create_rule(
            "to-webhook".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            Some(r#"SELECT temperature FROM "sensors/+" WHERE temperature > 0"#.to_string()),
            true,
            vec![RuleAction::ForwardConnector {
                connector_id: "webhook".to_string(),
            }],
        )
        .expect("rule creates");
    let mqtt_sink = Arc::new(RecordingSink::default());
    let sink: Arc<dyn BrokerSink> = mqtt_sink.clone();

    engine
        .dispatch_ingress(
            &Topic::new("sensors/temperature").unwrap(),
            &Bytes::from_static(br#"{ "temperature": 72.5, "secret": "hide_me" }"#),
            QoS::AtMostOnce,
            &sink,
        )
        .await;

    // Connector got the projected payload (secret stripped), the MQTT
    // sink got nothing (no Republish action on this rule).
    let events = recorder.events.lock().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].0, "sensors/temperature");
    let body: serde_json::Value =
        serde_json::from_slice(&events[0].1).expect("connector payload is JSON");
    assert_eq!(body, serde_json::json!({ "temperature": 72.5 }));
    assert!(mqtt_sink.published.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_forward_connector_missing_id_is_tolerated() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    engine
        .create_rule(
            "to-nowhere".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            None,
            true,
            vec![
                RuleAction::ForwardConnector {
                    connector_id: "ghost".to_string(),
                },
                RuleAction::Log,
            ],
        )
        .expect("rule creates");
    let sink: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());

    // Unknown connector id: warned, skipped, remaining actions run.
    engine
        .dispatch_ingress(
            &Topic::new("sensors/temperature").unwrap(),
            &Bytes::from_static(b"21.5C"),
            QoS::AtMostOnce,
            &sink,
        )
        .await;
}

// ------------------------------------------------------------------
// INDRA-211/212/213: tiering, window workers, aggregations, flush.
// ------------------------------------------------------------------

#[test]
fn test_tier_classification() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    let mk = |sql: Option<&str>| {
        engine
            .create_rule(
                "t".to_string(),
                TopicFilter::new("sensors/+").unwrap(),
                sql.map(str::to_string),
                true,
                vec![RuleAction::Log],
            )
            .expect("rule creates")
            .tier
    };
    // No SQL and plain SELECT: Community.
    assert_eq!(mk(None), RuleTier::Community);
    assert_eq!(
        mk(Some(r#"SELECT temperature FROM "sensors/+""#)),
        RuleTier::Community
    );
    // Plain GROUP BY runs as an implicit session window: Enterprise.
    assert_eq!(
        mk(Some(
            r#"SELECT sensor_id, avg(temperature) AS a FROM "sensors/+" GROUP BY sensor_id"#
        )),
        RuleTier::Enterprise
    );
    // Any window clause makes the rule Enterprise — even disabled.
    assert_eq!(
        mk(Some(
            r#"SELECT avg(temperature) AS a FROM "sensors/+" GROUP BY TUMBLINGWINDOW(ss, 10)"#
        )),
        RuleTier::Enterprise
    );
    let disabled = engine
        .create_rule(
            "d".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            Some(
                r#"SELECT avg(temperature) AS a FROM "sensors/+" GROUP BY COUNTWINDOW(5)"#
                    .to_string(),
            ),
            false,
            vec![RuleAction::Log],
        )
        .expect("rule creates");
    assert_eq!(disabled.tier, RuleTier::Enterprise);
}

#[test]
fn test_try_evaluate_vectors() {
    let payload = serde_json::json!({ "temperature": 72.5, "secret": "x" });

    // SQL match with projection.
    let (matched, projected) = try_evaluate(
        Some(r#"SELECT temperature FROM "sensors/+" WHERE temperature > 0"#),
        Some("sensors/+"),
        "sensors/kitchen",
        &payload,
    )
    .expect("evaluates");
    assert!(matched);
    assert_eq!(projected, Some(serde_json::json!({ "temperature": 72.5 })));

    // Predicate false: no match, no projection.
    let (matched, projected) = try_evaluate(
        Some(r#"SELECT * FROM "sensors/+" WHERE temperature > 100.0"#),
        Some("sensors/+"),
        "sensors/kitchen",
        &payload,
    )
    .expect("evaluates");
    assert!(!matched);
    assert_eq!(projected, None);

    // Filter mismatch short-circuits before SQL.
    let (matched, projected) = try_evaluate(
        Some(r#"SELECT * FROM "sensors/+" WHERE temperature > 0"#),
        Some("factory/#"),
        "sensors/kitchen",
        &payload,
    )
    .expect("evaluates");
    assert!(!matched);
    assert_eq!(projected, None);

    // No SQL: raw passthrough on match.
    let (matched, projected) =
        try_evaluate(None, Some("sensors/+"), "sensors/kitchen", &payload).expect("evaluates");
    assert!(matched);
    assert_eq!(projected, Some(payload.clone()));

    // Non-object payloads never match SQL rules.
    let scalar = serde_json::json!(42);
    let (matched, _) = try_evaluate(
        Some(r#"SELECT * FROM "sensors/+" WHERE temperature > 0"#),
        None,
        "sensors/kitchen",
        &scalar,
    )
    .expect("evaluates");
    assert!(!matched);

    // Invalid inputs fail loudly.
    assert!(try_evaluate(Some("SELECT WHERE WHERE"), None, "t", &payload).is_err());
    assert!(try_evaluate(None, Some("a/#/b"), "t", &payload).is_err());
    assert!(try_evaluate(None, None, "", &payload).is_err());
}

#[test]
fn test_split_into_connector_vectors() {
    // No INTO: untouched, no binding.
    let (stripped, bound) =
        split_into_connector(r#"SELECT * FROM "sensors/+" WHERE temperature > 0"#).expect("parses");
    assert_eq!(
        stripped,
        r#"SELECT * FROM "sensors/+" WHERE temperature > 0"#
    );
    assert_eq!(bound, None);

    // Trailing INTO binds the connector and strips cleanly.
    let (stripped, bound) = split_into_connector(
        r#"SELECT temperature FROM "sensors/+" WHERE temperature > 0 INTO connector("kafka-sink-1")"#,
    )
    .expect("parses");
    assert_eq!(
        stripped,
        r#"SELECT temperature FROM "sensors/+" WHERE temperature > 0"#
    );
    assert_eq!(bound, Some("kafka-sink-1".to_string()));

    // Single quotes, extra whitespace, trailing semicolon tolerated.
    let (stripped, bound) =
        split_into_connector("SELECT a FROM s WHERE v > 1   INTO   connector( 'r1' ) ;")
            .expect("parses");
    assert_eq!(stripped, "SELECT a FROM s WHERE v > 1");
    assert_eq!(bound, Some("r1".to_string()));

    // INTO inside a string literal is data, not a clause.
    let (stripped, bound) =
        split_into_connector(r#"SELECT * FROM s WHERE note = 'INTO the void'"#).expect("parses");
    assert_eq!(bound, None);
    assert!(stripped.contains("INTO the void"));

    // Malformed INTO fails creation loudly.
    for bad in [
        r#"SELECT * FROM s INTO connector()"#,
        r#"SELECT * FROM s INTO connector("unclosed)"#,
        r#"SELECT * FROM s INTO connector"#,
        r#"SELECT * FROM s INTO topic("x")"#,
        r#"SELECT * FROM s INTO connector("x") TRAILING"#,
    ] {
        assert!(split_into_connector(bad).is_err(), "must reject: {bad}");
    }
}

// Needs the `kafka` heavy SDK (X1-10); runs on feature builds.
#[cfg(feature = "kafka")]
#[tokio::test]
async fn test_into_connector_desugars_to_forward_action() {
    use broker_connectors::{
        kafka::KafkaHeaderEncodeMode, KafkaSink, KafkaSinkConfig, MemoryKafkaTransport,
    };

    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    let transport = Arc::new(MemoryKafkaTransport::new());
    let kafka = Arc::new(
        KafkaSink::new(
            KafkaSinkConfig {
                bootstrap_servers: "unused:9092".to_string(),
                topic_template: "out-${topic}".to_string(),
                partition_key_field: Some("device_id".to_string()),
                key_template: None,
                partitions: 4,
                client_id: "test".to_string(),
                acks: "1".to_string(),
                batch_max_records: 100,
                batch_max_bytes: 1024 * 1024,
                random_partition: false,
                headers_template: None,
                extra_headers: Vec::new(),
                header_encode_mode: KafkaHeaderEncodeMode::None,
            },
            transport.clone(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("kafka-sink-1", kafka.clone());

    // INTO appends the ForwardConnector action; the original SQL is
    // preserved verbatim on the stored rule.
    let rule = engine
        .create_rule(
            "bridge".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            Some(
                r#"SELECT temperature, device_id FROM "sensors/+" WHERE temperature > 0 INTO connector("kafka-sink-1")"#
                    .to_string(),
            ),
            true,
            vec![],
        )
        .expect("rule creates");
    assert_eq!(
        rule.sql_query.as_deref(),
        Some(
            r#"SELECT temperature, device_id FROM "sensors/+" WHERE temperature > 0 INTO connector("kafka-sink-1")"#
        )
    );
    assert!(rule.actions.iter().any(|action| matches!(
        action,
        RuleAction::ForwardConnector { connector_id } if connector_id == "kafka-sink-1"
    )));

    // End to end: ingress MQTT bytes trigger SQL projection into the
    // mock Kafka buffer (no broker anywhere in the loop). Batching
    // holds records until the batch limit or an explicit flush.
    let sink: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());
    engine
        .dispatch_ingress(
            &Topic::new("sensors/kitchen").unwrap(),
            &Bytes::from_static(
                br#"{ "temperature": 72.5, "device_id": "d7", "secret": "hide_me" }"#,
            ),
            QoS::AtMostOnce,
            &sink,
        )
        .await;
    assert_eq!(kafka.buffered_records(), 1);
    kafka.flush().await.expect("flush");

    let records = transport.records_flat();
    assert_eq!(records.len(), 1);
    // Kafka topics cannot contain `/`, so `/` renders as `.`.
    assert_eq!(records[0].topic, "out-sensors.kitchen");
    assert_eq!(records[0].key, Some(Bytes::from_static(b"d7")));
    let body: serde_json::Value =
        serde_json::from_slice(&records[0].value).expect("projected JSON");
    assert_eq!(
        body,
        serde_json::json!({ "temperature": 72.5, "device_id": "d7" })
    );
    let header_map: std::collections::HashMap<&str, &Bytes> = records[0]
        .headers
        .iter()
        .map(|(k, v)| (k.as_str(), v))
        .collect();
    assert_eq!(
        header_map.get("mqtt.topic"),
        Some(&&Bytes::from_static(b"sensors/kitchen"))
    );

    // Below-threshold ingress fires nothing anywhere.
    engine
        .dispatch_ingress(
            &Topic::new("sensors/kitchen").unwrap(),
            &Bytes::from_static(br#"{ "temperature": -5.0, "device_id": "d7" }"#),
            QoS::AtMostOnce,
            &sink,
        )
        .await;
    assert_eq!(transport.records_flat().len(), 1);
}

#[tokio::test]
async fn test_into_malformed_rejected_at_creation() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    let err = engine
        .create_rule(
            "broken-into".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            Some(r#"SELECT * FROM "sensors/+" WHERE temperature > 0 INTO connector()"#.to_string()),
            true,
            vec![],
        )
        .expect_err("malformed INTO must fail creation");
    assert!(matches!(err, RuleEngineError::InvalidRule(_)));
    assert!(engine.list_rules().is_empty());
}

#[derive(Debug, Default)]
struct AlwaysFailConnector;

#[async_trait]
impl broker_connectors::Sink for AlwaysFailConnector {
    async fn send(
        &self,
        _topic: &Topic,
        _payload: &Bytes,
        _qos: QoS,
    ) -> Result<(), broker_connectors::ConnectorError> {
        Err(broker_connectors::ConnectorError::Dispatch(
            "target down".to_string(),
        ))
    }

    fn kind(&self) -> &'static str {
        "always-fail"
    }
}

fn failing_forward_engine(connector_id: &str) -> (RuleEngine, Arc<dyn BrokerSink>) {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    engine
        .connectors()
        .register(connector_id, Arc::new(AlwaysFailConnector));
    engine
        .create_rule(
            "forward-fail".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            None,
            true,
            vec![RuleAction::ForwardConnector {
                connector_id: connector_id.to_string(),
            }],
        )
        .expect("rule creates");
    let sink: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());
    (engine, sink)
}

#[tokio::test]
async fn test_forward_failures_counted_in_action_metrics() {
    let (engine, sink) = failing_forward_engine("down");
    let topic = Topic::new("sensors/temperature").unwrap();
    for _ in 0..50 {
        engine
            .dispatch_ingress(
                &topic,
                &Bytes::from_static(b"21.5C"),
                QoS::AtMostOnce,
                &sink,
            )
            .await;
    }
    let rule = engine.get_rule("rule-1").expect("rule stored");
    assert_eq!(rule.matched_cnt.load(Ordering::Relaxed), 50);
    assert_eq!(rule.passed_cnt.load(Ordering::Relaxed), 50);
    assert_eq!(rule.actions_total_cnt.load(Ordering::Relaxed), 50);
    assert_eq!(rule.actions_success_cnt.load(Ordering::Relaxed), 0);
    assert_eq!(rule.actions_failed_cnt.load(Ordering::Relaxed), 50);
}

#[test]
fn test_forward_failure_throttle_decisions() {
    let throttle = parking_lot::Mutex::new(HashMap::new());
    assert_eq!(
        forward_failure_should_log(&throttle, "rule-1", "down", 1_000),
        Some(0)
    );
    for now in 1_001..=1_049 {
        assert_eq!(
            forward_failure_should_log(&throttle, "rule-1", "down", now),
            None,
            "failures inside the window must only be counted"
        );
    }
    assert_eq!(
        forward_failure_should_log(
            &throttle,
            "rule-1",
            "down",
            1_000 + FORWARD_FAILURE_LOG_WINDOW_MS
        ),
        Some(49)
    );
    assert_eq!(
        forward_failure_should_log(
            &throttle,
            "rule-1",
            "down",
            1_000 + FORWARD_FAILURE_LOG_WINDOW_MS
        ),
        None
    );
    assert_eq!(
        forward_failure_should_log(&throttle, "rule-1", "other", 1_050),
        Some(0)
    );
}

#[tokio::test]
async fn test_forward_success_counted() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    let recorder: Arc<RecordingConnector> = Arc::new(RecordingConnector::default());
    engine.connectors().register("up", recorder);
    engine
        .create_rule(
            "forward-ok".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            None,
            true,
            vec![RuleAction::ForwardConnector {
                connector_id: "up".to_string(),
            }],
        )
        .expect("rule creates");
    let sink: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());
    let topic = Topic::new("sensors/temperature").unwrap();
    for _ in 0..5 {
        engine
            .dispatch_ingress(
                &topic,
                &Bytes::from_static(b"21.5C"),
                QoS::AtMostOnce,
                &sink,
            )
            .await;
    }
    let rule = engine.get_rule("rule-1").expect("rule stored");
    assert_eq!(rule.actions_total_cnt.load(Ordering::Relaxed), 5);
    assert_eq!(rule.actions_success_cnt.load(Ordering::Relaxed), 5);
    assert_eq!(rule.actions_failed_cnt.load(Ordering::Relaxed), 0);
}
