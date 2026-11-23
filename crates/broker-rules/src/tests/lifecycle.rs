use super::*;

#[tokio::test]
async fn test_rule_republish_executes_on_match() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    engine
        .create_rule(
            "republish-temp".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            None,
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("alerts/critical").unwrap(),
                qos: QoS::AtLeastOnce,
            }],
        )
        .expect("rule creates");
    let sink = Arc::new(RecordingSink::default());
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();

    engine
        .dispatch_ingress(
            &Topic::new("sensors/temperature").unwrap(),
            &Bytes::from_static(b"21.5C"),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;

    let published = sink
        .published
        .lock()
        .unwrap()
        .iter()
        .map(|(t, p, q, r)| (t.as_str().to_string(), p.clone(), *q, *r))
        .collect::<Vec<_>>();
    // Effective QoS is min(ingress 0, action 1) = 0; retain stays false.
    assert_eq!(
        published,
        vec![(
            "alerts/critical".to_string(),
            Bytes::from_static(b"21.5C"),
            QoS::AtMostOnce,
            false
        )]
    );
}

#[derive(Debug, Default)]
struct TenantSink {
    published: StdMutex<Vec<(String, Topic, Bytes)>>,
}

#[async_trait]
impl BrokerSink for TenantSink {
    async fn publish(
        &self,
        topic: Topic,
        payload: Bytes,
        _qos: QoS,
        _retain: bool,
    ) -> Result<(), RuleEngineError> {
        self.published
            .lock()
            .unwrap()
            .push(("default".to_string(), topic, payload));
        Ok(())
    }

    async fn publish_in_tenant(
        &self,
        tenant: &str,
        topic: Topic,
        payload: Bytes,
        _qos: QoS,
        _retain: bool,
    ) -> Result<(), RuleEngineError> {
        self.published
            .lock()
            .unwrap()
            .push((tenant.to_string(), topic, payload));
        Ok(())
    }
}

fn tenant_republish_rule(
    engine: &RuleEngine,
    tenant: &str,
    name: &str,
    filter: &str,
    out: &str,
) -> Rule {
    engine
        .create_rule_in_tenant(
            tenant.to_string(),
            name.to_string(),
            TopicFilter::new(filter).expect("filter parses"),
            None,
            true,
            vec![RuleAction::Republish {
                topic: Topic::new(out).expect("topic parses"),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("rule creates")
}

/// A rule of tenant A runs for a publish of tenant A only.
/// A publish of tenant B never runs it.
#[tokio::test]
async fn tenant_rule_fires_only_in_own_tenant() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    tenant_republish_rule(&engine, "tenant-a", "rule-a", "sensors/#", "out/a");
    let sink = Arc::new(TenantSink::default());
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();
    let topic = Topic::new("sensors/1").expect("topic parses");
    let payload = Bytes::from_static(b"hello");
    let fired = engine
        .dispatch_ingress_in_tenant("tenant-a", &topic, &payload, QoS::AtMostOnce, &sink_obj)
        .await;
    assert_eq!(fired, 1);
    let fired = engine
        .dispatch_ingress_in_tenant("tenant-b", &topic, &payload, QoS::AtMostOnce, &sink_obj)
        .await;
    assert_eq!(fired, 0);
    let rows = sink.published.lock().unwrap().clone();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "tenant-a");
    assert_eq!(rows[0].1.as_str(), "out/a");
}

/// Output of a rule of tenant A fans out in tenant A only.
/// Two rules on one filter each serve their own tenant.
#[tokio::test]
async fn tenant_rule_output_stays_in_own_tenant() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    tenant_republish_rule(&engine, "tenant-a", "rule-a", "sensors/#", "out/shared");
    tenant_republish_rule(&engine, "tenant-b", "rule-b", "sensors/#", "out/shared");
    let sink = Arc::new(TenantSink::default());
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();
    let topic = Topic::new("sensors/1").expect("topic parses");
    let payload = Bytes::from_static(b"data");
    let fired = engine
        .dispatch_ingress_in_tenant("tenant-a", &topic, &payload, QoS::AtMostOnce, &sink_obj)
        .await;
    assert_eq!(fired, 1);
    let rows = sink.published.lock().unwrap().clone();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "tenant-a");
}

/// The window transport carries the ingress tenant. The feeder
/// rebuilds it, so a task of another tenant drops the record.
#[test]
fn window_transport_carries_ingress_tenant() {
    let topic = Topic::new("sensors/1").expect("topic parses");
    let payload = Bytes::from_static(br#"{"temp": 21}"#);
    let record = runtime::ingress_to_stream_record("tenant-a", &topic, &payload, QoS::AtMostOnce);
    let feeder = runtime::split_stream_record(record.data).expect("record splits");
    assert_eq!(feeder.tenant, "tenant-a");
    assert_eq!(feeder.topic.as_str(), "sensors/1");
}

/// A rule stored before tenancy serves the default tenant.
/// It runs as before for default ingress, never for others.
#[tokio::test]
async fn pre_tenancy_rule_serves_default_tenant() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    let rule = engine
        .create_rule(
            "legacy".to_string(),
            TopicFilter::new("sensors/#").unwrap(),
            None,
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("out/legacy").unwrap(),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("rule creates");
    assert_eq!(rule.tenant, "default");
    let sink = Arc::new(TenantSink::default());
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();
    let topic = Topic::new("sensors/1").expect("topic parses");
    let payload = Bytes::from_static(b"data");
    let fired = engine
        .dispatch_ingress(&topic, &payload, QoS::AtMostOnce, &sink_obj)
        .await;
    assert_eq!(fired, 1);
    let fired = engine
        .dispatch_ingress_in_tenant("tenant-a", &topic, &payload, QoS::AtMostOnce, &sink_obj)
        .await;
    assert_eq!(fired, 0);
    let rows = sink.published.lock().unwrap().clone();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "default");
}

#[tokio::test]
async fn test_rule_skips_disabled_and_non_matching() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    engine
        .create_rule(
            "disabled".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            None,
            false,
            vec![RuleAction::Log],
        )
        .expect("rule creates");
    engine
        .create_rule(
            "other-branch".to_string(),
            TopicFilter::new("factory/#").unwrap(),
            None,
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("alerts/other").unwrap(),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("rule creates");
    let sink = Arc::new(RecordingSink::default());
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();

    engine
        .dispatch_ingress(
            &Topic::new("sensors/temperature").unwrap(),
            &Bytes::from_static(b"21.5C"),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;

    assert!(sink.published.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_rule_crud_lifecycle() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    let rule = engine
        .create_rule(
            "r1".to_string(),
            TopicFilter::new("a/#").unwrap(),
            Some("SELECT * FROM a/#".to_string()),
            true,
            vec![RuleAction::Log],
        )
        .expect("rule creates");
    assert_eq!(rule.id, "rule-1");
    assert!(engine.get_rule("rule-1").is_some());
    assert!(engine.get_rule("rule-999").is_none());
    assert_eq!(engine.list_rules().len(), 1);
    assert!(engine
        .remove_rule("rule-1")
        .expect("memory-only remove cannot fail"));
    assert!(!engine
        .remove_rule("rule-1")
        .expect("memory-only remove cannot fail"));
    assert!(engine.list_rules().is_empty());
}
