use super::*;

/// Unique scratch data dir under the OS temp dir (no dev-dependency).
fn unique_rules_data_dir(prefix: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "broker-rules-{prefix}-{}-{nanos}",
        std::process::id()
    ))
}

fn persisted_rule_snapshot(
    rule: &Rule,
) -> (
    String,
    String,
    String,
    serde_json::Value,
    String,
    Option<String>,
    bool,
) {
    (
        rule.id.clone(),
        rule.name.clone(),
        rule.description.clone(),
        rule.metadata.clone(),
        rule.topic_filter.as_str().to_string(),
        rule.sql_query.clone(),
        rule.enabled,
    )
}

fn persisted_actions_value(rule: &Rule) -> serde_json::Value {
    serde_json::to_value(&rule.actions).expect("actions serialise")
}

#[tokio::test]
async fn rules_survive_restart() {
    let dir = unique_rules_data_dir("restart");
    let registry =
        Arc::new(broker_config::ConfigRegistry::load(&dir).expect("fresh data dir loads"));
    let engine = RuleEngine::from_registry(&registry).expect("empty snapshot boots");
    // All three action variants; the republish rule stays disabled.
    engine
        .create_rule_with_id_in_tenant(
            "republish-rule".to_string(),
            "default".to_string(),
            "republish-rule".to_string(),
            "hot readings".to_string(),
            serde_json::json!({"owner": "rules"}),
            TopicFilter::new("sensors/+").unwrap(),
            Some(r#"SELECT temperature FROM "sensors/+" WHERE temperature > 0"#.to_string()),
            false,
            vec![RuleAction::Republish {
                topic: Topic::new("alerts/hot").unwrap(),
                qos: QoS::AtLeastOnce,
            }],
        )
        .expect("republish rule persists");
    engine
        .create_rule(
            "log-rule".to_string(),
            TopicFilter::new("logs/#").unwrap(),
            None,
            true,
            vec![RuleAction::Log],
        )
        .expect("log rule persists");
    engine
        .create_rule(
            "forward-rule".to_string(),
            TopicFilter::new("factory/#").unwrap(),
            Some(r#"SELECT * FROM "factory/#""#.to_string()),
            true,
            vec![RuleAction::ForwardConnector {
                connector_id: "webhook".to_string(),
            }],
        )
        .expect("forward rule persists");
    assert!(dir.join(broker_config::STATE_FILE_NAME).is_file());
    let before = engine.list_rules();
    assert_eq!(before.len(), 3);

    // Rebuild from the same data dir (simulating a kernel restart).
    let reloaded = Arc::new(broker_config::ConfigRegistry::load(&dir).expect("data dir reloads"));
    let restarted = RuleEngine::from_registry(&reloaded).expect("snapshot replays");
    let after = restarted.list_rules();
    assert_eq!(after.len(), 3);
    for (before_rule, after_rule) in before.iter().zip(after.iter()) {
        assert_eq!(
            persisted_rule_snapshot(before_rule),
            persisted_rule_snapshot(after_rule),
            "id/name/description/metadata/filter/sql/enable must round-trip"
        );
        assert_eq!(
            persisted_actions_value(before_rule),
            persisted_actions_value(after_rule),
            "full action list must round-trip without loss"
        );
        // Metric counters are runtime-only and reset to zero.
        assert_eq!(after_rule.matched_cnt.load(Ordering::Relaxed), 0);
    }
    let disabled = restarted
        .get_rule(&before[0].id)
        .expect("disabled rule reloads");
    assert!(!disabled.enabled, "disabled rule stays disabled");
    assert!(
        restarted
            .get_rule(&before[1].id)
            .expect("log rule reloads")
            .enabled
    );
    // The republish variant kept every field (topic + qos).
    let republished = restarted
        .get_rule(&before[0].id)
        .expect("republish rule reloads");
    assert!(matches!(
        &republished.actions[..],
        [RuleAction::Republish { topic, qos }]
            if topic.as_str() == "alerts/hot" && *qos == QoS::AtLeastOnce
    ));

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn rule_delete_survives_restart() {
    let dir = unique_rules_data_dir("delete");
    let registry =
        Arc::new(broker_config::ConfigRegistry::load(&dir).expect("fresh data dir loads"));
    let engine = RuleEngine::from_registry(&registry).expect("empty snapshot boots");
    let rule = engine
        .create_rule(
            "temp".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            None,
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("alerts/all").unwrap(),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("rule persists");
    let recorder = Arc::new(RecordingSink::default());
    let sink: Arc<dyn BrokerSink> = recorder.clone();
    engine
        .dispatch_ingress(
            &Topic::new("sensors/kitchen").unwrap(),
            &Bytes::from_static(b"21.5C"),
            QoS::AtMostOnce,
            &sink,
        )
        .await;
    assert_eq!(recorder.published.lock().unwrap().len(), 1);
    assert!(engine.remove_rule(&rule.id).expect("persist rule delete"));

    // Rebuild from the same data dir: the deleted rule stays gone and
    // routing no longer matches it.
    let reloaded = Arc::new(broker_config::ConfigRegistry::load(&dir).expect("data dir reloads"));
    let restarted = RuleEngine::from_registry(&reloaded).expect("snapshot replays");
    assert!(restarted.get_rule(&rule.id).is_none());
    assert!(restarted.list_rules().is_empty());
    let recorder_after = Arc::new(RecordingSink::default());
    let sink_after: Arc<dyn BrokerSink> = recorder_after.clone();
    let matched = restarted
        .dispatch_ingress(
            &Topic::new("sensors/kitchen").unwrap(),
            &Bytes::from_static(b"21.5C"),
            QoS::AtMostOnce,
            &sink_after,
        )
        .await;
    assert_eq!(matched, 0);
    assert!(recorder_after.published.lock().unwrap().is_empty());

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn invalid_stored_rule_fails_boot_loudly() {
    // An empty connector id passes TOML parsing but must fail boot
    // loudly (never skipped silently).
    let bad_connector = broker_config::RulesConf {
        rules: vec![broker_config::RuleEntry {
            id: "rule-1".to_string(),
            name: "bad".to_string(),
            description: String::new(),
            metadata: serde_json::json!({}),
            tenant: "default".to_string(),
            topic_filter: "sensors/#".to_string(),
            sql_query: None,
            enabled: true,
            actions: vec![broker_config::RuleActionEntry::ForwardConnector {
                connector_id: String::new(),
            }],
        }],
    };
    let err = match RuleEngine::from_snapshot(&bad_connector) {
        Ok(_) => panic!("empty connector must fail"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("connector_id"),
        "error must name the field, got: {err}"
    );
    // Invalid stored SQL fails boot loudly as well.
    let bad_sql = broker_config::RulesConf {
        rules: vec![broker_config::RuleEntry {
            id: "rule-1".to_string(),
            name: "bad".to_string(),
            description: String::new(),
            metadata: serde_json::json!({}),
            tenant: "default".to_string(),
            topic_filter: "sensors/#".to_string(),
            sql_query: Some("SELECT FROM WHERE".to_string()),
            enabled: true,
            actions: Vec::new(),
        }],
    };
    let err = match RuleEngine::from_snapshot(&bad_sql) {
        Ok(_) => panic!("invalid SQL must fail"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("invalid"),
        "error must fail loudly, got: {err}"
    );
}
