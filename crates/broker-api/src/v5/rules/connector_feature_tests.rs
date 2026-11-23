use super::*;
use broker_protocol::{QoS, Topic, TopicFilter};
use broker_rules::{BackpressurePolicy, BrokerSink, RuleEngine, RuleEngineError};
use bytes::Bytes;
use std::sync::{Arc, Mutex as StdMutex};

#[derive(Debug, Default)]
struct RecordingSink {
    published: StdMutex<Vec<(Topic, Bytes, QoS, bool)>>,
}

#[async_trait::async_trait]
impl BrokerSink for RecordingSink {
    async fn publish(
        &self,
        topic: Topic,
        payload: Bytes,
        qos: QoS,
        retain: bool,
    ) -> Result<(), RuleEngineError> {
        self.published
            .lock()
            // INVARIANT: Mutex lock on a test sink is never poisoned.
            .unwrap()
            .push((topic, payload, qos, retain));
        Ok(())
    }
}

async fn expect_disabled(kind: &str, feature: &str) {
    assert_eq!(
        disabled_connector_feature(kind),
        Some(feature),
        "{kind} must map to feature {feature} when disabled"
    );
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    let err = try_register_live_sink(
        &engine,
        kind,
        "feat-test",
        &serde_json::json!({}),
        std::path::Path::new("./data"),
    )
    .await
    .expect_err("disabled kind must fail registration");
    assert!(
        err.contains(feature),
        "closed error must name feature '{feature}': {err}"
    );
    assert!(
        engine.connectors().get("feat-test").is_none(),
        "no sink may register when feature '{feature}' is disabled"
    );
}

/// The compiled-in list comes from the same table as the lookup.
/// Each entry tracks its own feature, so no second list exists.
/// The test fails if the lookup ever disagrees with the table or
/// if two rows claim one alias.
#[tokio::test]
async fn test_compiled_features_match_table() {
    let compiled = compiled_connector_features();
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for gate in CONNECTOR_GATES {
        assert_eq!(
            compiled.contains(&gate.feature),
            gate.enabled,
            "feature `{}` must track its table row",
            gate.feature
        );
        for kind in gate.kinds {
            assert!(
                seen.insert(kind),
                "alias `{kind}` must appear in one table row only"
            );
            assert_eq!(
                disabled_connector_feature(kind).is_some(),
                !gate.enabled,
                "kind `{kind}` must track its table row"
            );
        }
    }
}

#[cfg(not(feature = "s3"))]
#[tokio::test]
async fn test_disabled_s3_names_feature() {
    expect_disabled("s3", "s3").await;
    expect_disabled("minio", "s3").await;
}

#[cfg(not(feature = "dynamodb"))]
#[tokio::test]
async fn test_disabled_dynamodb_names_feature() {
    expect_disabled("dynamodb", "dynamodb").await;
    expect_disabled("dynamo", "dynamodb").await;
}

#[cfg(not(feature = "bigquery"))]
#[tokio::test]
async fn test_disabled_bigquery_names_feature() {
    expect_disabled("bigquery", "bigquery").await;
}

#[cfg(not(feature = "aws_iot"))]
#[tokio::test]
async fn test_disabled_aws_iot_names_feature() {
    expect_disabled("aws_iot", "aws_iot").await;
    expect_disabled("aws_iot_core", "aws_iot").await;
}

#[cfg(not(feature = "azure_blob"))]
#[tokio::test]
async fn test_disabled_azure_blob_names_feature() {
    expect_disabled("azure_blob", "azure_blob").await;
    expect_disabled("azure_blob_storage", "azure_blob").await;
}

#[cfg(not(feature = "azure_eventhubs"))]
#[tokio::test]
async fn test_disabled_azure_eventhubs_names_feature() {
    expect_disabled("azure_eventhubs", "azure_eventhubs").await;
    expect_disabled("azure_event_hubs", "azure_eventhubs").await;
}

#[cfg(not(feature = "azure_iot"))]
#[tokio::test]
async fn test_disabled_azure_iot_names_feature() {
    expect_disabled("azure_iot", "azure_iot").await;
    expect_disabled("azure_iot_hub", "azure_iot").await;
}

#[cfg(not(feature = "gcp_iot"))]
#[tokio::test]
async fn test_disabled_gcp_iot_names_feature() {
    expect_disabled("gcp_iot", "gcp_iot").await;
    expect_disabled("gcp_iot_core", "gcp_iot").await;
}

#[cfg(not(feature = "kafka"))]
#[tokio::test]
async fn test_disabled_kafka_names_feature() {
    expect_disabled("kafka", "kafka").await;
}

#[cfg(not(feature = "rabbitmq"))]
#[tokio::test]
async fn test_disabled_rabbitmq_names_feature() {
    expect_disabled("rabbitmq", "rabbitmq").await;
    expect_disabled("amqp", "rabbitmq").await;
}

#[cfg(not(feature = "clickhouse"))]
#[tokio::test]
async fn test_disabled_clickhouse_names_feature() {
    expect_disabled("clickhouse", "clickhouse").await;
    expect_disabled("ch", "clickhouse").await;
}

#[cfg(feature = "clickhouse")]
#[tokio::test]
async fn test_ch_alias_registers_like_clickhouse() {
    assert_eq!(live_sink_kind("ch"), "clickhouse");
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    try_register_live_sink(
        &engine,
        "ch",
        "ch-test",
        &serde_json::json!({"url": "http://127.0.0.1:9", "database": "db", "table": "t"}),
        std::path::Path::new("./data"),
    )
    .await
    .expect("ch must register through the clickhouse arm");
    assert!(
        engine.connectors().get("ch-test").is_some(),
        "ch must leave a live sink"
    );
}

#[cfg(not(feature = "elasticsearch"))]
#[tokio::test]
async fn test_disabled_elasticsearch_names_feature() {
    expect_disabled("elasticsearch", "elasticsearch").await;
    expect_disabled("opensearch", "elasticsearch").await;
}

#[cfg(not(feature = "cassandra"))]
#[tokio::test]
async fn test_disabled_cassandra_names_feature() {
    expect_disabled("cassandra", "cassandra").await;
    expect_disabled("scylla", "cassandra").await;
}

#[cfg(not(feature = "confluent"))]
#[tokio::test]
async fn test_disabled_confluent_names_feature() {
    expect_disabled("confluent", "confluent").await;
}

#[cfg(not(feature = "couchbase"))]
#[tokio::test]
async fn test_disabled_couchbase_names_feature() {
    expect_disabled("couchbase", "couchbase").await;
}

#[cfg(not(feature = "mongodb"))]
#[tokio::test]
async fn test_disabled_mongodb_names_feature() {
    expect_disabled("mongodb", "mongodb").await;
    expect_disabled("mongo", "mongodb").await;
}

#[cfg(not(feature = "gcp_pubsub"))]
#[tokio::test]
async fn test_disabled_gcp_pubsub_names_feature() {
    expect_disabled("gcp_pubsub", "gcp_pubsub").await;
    expect_disabled("pubsub", "gcp_pubsub").await;
}

#[cfg(not(feature = "kinesis"))]
#[tokio::test]
async fn test_disabled_kinesis_names_feature() {
    expect_disabled("kinesis", "kinesis").await;
    expect_disabled("aws_kinesis", "kinesis").await;
}

/// Enabled evidence (dynamodb): a rule registered with the connector
/// manager delivers exactly one batch through the broker rule path.
#[cfg(feature = "dynamodb")]
#[tokio::test]
async fn test_enabled_dynamodb_through_rule_engine() {
    use broker_connectors::Sink as _;
    use broker_connectors_enterprise::dynamodb::{
        DynamoDbSink, DynamoDbSinkConfig, DynamoKeyConfig, MockDynamoDbTransport,
    };
    let engine = RuleEngine::new(65_536, BackpressurePolicy::DropOldest);
    let transport = Arc::new(MockDynamoDbTransport::new());
    let config = DynamoDbSinkConfig {
        table_name: "telemetry_table".to_string(),
        region: "us-east-1".to_string(),
        endpoint: None,
        access_key_id: "AKID".to_string(),
        secret_access_key: "secret".to_string(),
        session_token: None,
        partition_key: DynamoKeyConfig {
            name: "device_id".to_string(),
            template: "${client_id}".to_string(),
            key_type: "S".to_string(),
        },
        sort_key: None,
        ttl_attribute: None,
        ttl_secs: None,
        attributes_mapping: std::collections::HashMap::new(),
        undefined_vars_as_null: false,
        batch_size: Some(1),
        batch_bytes: Some(1_048_576),
        linger_ms: Some(10),
        max_retries: Some(3),
        initial_backoff_ms: Some(1),
        max_backoff_ms: Some(2),
        timeout_ms: None,
    };
    let sink = Arc::new(DynamoDbSink::new(config, transport.clone()).expect("valid sink"));
    assert_eq!(sink.kind(), "dynamodb");
    engine.connectors().register("dd-feat", sink);
    engine
            .create_rule(
                "dd-feat-rule".to_string(),
                TopicFilter::new("sensors/+").unwrap(),
                Some(
                    r#"SELECT client_id, temp FROM "sensors/+" WHERE temp > 20.0 INTO connector("dd-feat")"#.to_string(),
                ),
                true,
                vec![],
            )
            .expect("rule creates");
    let broker: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());
    engine
        .dispatch_ingress(
            &Topic::new("sensors/kitchen").unwrap(),
            &Bytes::from_static(br#"{ "client_id": "device-42", "temp": 22.5 }"#),
            QoS::AtMostOnce,
            &broker,
        )
        .await;
    let captured = transport.captured();
    assert_eq!(captured.len(), 1, "exactly one batch must deliver");
    assert_eq!(captured[0].table, "telemetry_table");
    assert_eq!(captured[0].items.len(), 1, "every key present, no loss");
}

/// Enabled evidence (s3): same rule-path shape over the mock object
/// transport; exactly one object lands with the projected JSON inside.
#[cfg(feature = "s3")]
#[tokio::test]
async fn test_enabled_s3_through_rule_engine() {
    use broker_connectors::s3::{MockS3Transport, S3Compression, S3Sink, S3SinkConfig};
    use broker_connectors::Sink as _;
    let engine = RuleEngine::new(65_536, BackpressurePolicy::DropOldest);
    let transport = Arc::new(MockS3Transport::new());
    let config = S3SinkConfig {
        endpoint: "http://127.0.0.1:9000".to_string(),
        bucket: "telemetry".to_string(),
        region: "us-east-1".to_string(),
        access_key_id: String::new(),
        secret_access_key: String::new(),
        key_template: "telemetry/${topic}_${seq}.ndjson".to_string(),
        compression: S3Compression::None,
        batch_size: 1,
        batch_bytes: 5 * 1024 * 1024,
        batch_timeout_ms: 10,
        timeout_ms: None,
    };
    let sink = Arc::new(S3Sink::new(config, transport.clone()).expect("valid sink"));
    assert_eq!(sink.kind(), "s3");
    engine.connectors().register("s3-feat", sink);
    engine
            .create_rule(
                "s3-feat-rule".to_string(),
                TopicFilter::new("sensors/+").unwrap(),
                Some(
                    r#"SELECT client_id, temp FROM "sensors/+" WHERE temp > 20.0 INTO connector("s3-feat")"#.to_string(),
                ),
                true,
                vec![],
            )
            .expect("rule creates");
    let broker: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());
    engine
        .dispatch_ingress(
            &Topic::new("sensors/kitchen").unwrap(),
            &Bytes::from_static(br#"{ "client_id": "device-42", "temp": 22.5 }"#),
            QoS::AtMostOnce,
            &broker,
        )
        .await;
    let puts = transport.puts();
    assert_eq!(puts.len(), 1, "exactly one object must land");
    assert_eq!(puts[0].bucket, "telemetry");
    let body = String::from_utf8(puts[0].body.clone()).expect("ndjson is UTF-8");
    assert!(
        body.contains("device-42"),
        "projected payload must travel untouched: {body}"
    );
}

#[tokio::test]
async fn disk_log_connector_applies_format_rotation_and_contains_paths() {
    use broker_connectors::Sink as _;
    let data = std::env::temp_dir().join(format!(
        "disk-log-connector-{}-{}",
        std::process::id(),
        now_nanos_for_test()
    ));
    let outside = data
        .join("..")
        .join(format!("outside-{}", now_nanos_for_test()));
    std::fs::create_dir_all(&data).expect("data dir");
    std::fs::create_dir_all(&outside).expect("outside dir");
    std::os::unix::fs::symlink(&outside, data.join("link")).expect("symlink");
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    let body = |directory: &str| {
        serde_json::json!({
            "directory": directory, "filename_prefix": "sel", "filename_extension": "ent",
            "format": "raw", "max_file_size_bytes": 16, "compression": "gzip",
            "max_backup_files": 4, "sync_mode": "every_batch"
        })
    };
    // A directory outside the data directory is refused with the field named.
    for bad in [
        outside.to_string_lossy().into_owned(),
        "../x".to_string(),
        "link".to_string(),
    ] {
        let error = register_live_sink(&engine, "disk_log", "d", &body(&bad), &data)
            .await
            .expect_err("outside directory is refused");
        assert!(error.contains("directory"), "{error}");
    }
    assert_eq!(std::fs::read_dir(&outside).expect("read").count(), 0);
    // A bad setting is refused with the field named.
    let mut bad_format = body("inside");
    bad_format["format"] = serde_json::json!("xml");
    let error = register_live_sink(&engine, "disk_log", "d", &bad_format, &data)
        .await
        .expect_err("unknown format is refused");
    assert!(error.contains("format"), "{error}");
    // A directory inside the data directory takes the format and the size limit.
    register_live_sink(&engine, "disk_log", "d", &body("inside"), &data)
        .await
        .expect("inside directory registers");
    let sink = engine.connectors().get("d").expect("sink registered");
    for value in 0..3 {
        let payload = Bytes::from(format!("{{\"v\":{value}}}"));
        sink.send(&Topic::new("t/x").unwrap(), &payload, QoS::AtMostOnce)
            .await
            .expect("send");
    }
    let active = std::fs::read(data.join("inside").join("sel.ent")).expect("active file");
    assert_eq!(
        active, b"{\"v\":2}\n",
        "raw format, one record after rotation"
    );
    let rotated = std::fs::read_dir(data.join("inside"))
        .expect("read dir")
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".gz"))
        .count();
    assert_eq!(rotated, 1, "the size limit rotated one gzip segment");
    let _ = std::fs::remove_dir_all(&data);
    let _ = std::fs::remove_dir_all(&outside);
}

fn now_nanos_for_test() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0)
}

#[test]
fn probe_health_check_timeout_defaults_to_60s() {
    let body = serde_json::json!({});
    assert_eq!(
        probe_health_check_timeout(&body),
        std::time::Duration::from_secs(60)
    );
}

#[test]
fn probe_health_check_timeout_reads_resource_opts() {
    let body = serde_json::json!({
        "resource_opts": {"health_check_timeout": "30s"}
    });
    assert_eq!(
        probe_health_check_timeout(&body),
        std::time::Duration::from_secs(30)
    );
}

#[test]
fn probe_health_check_timeout_refuses_bad_unit() {
    let body = serde_json::json!({
        "resource_opts": {"health_check_timeout": "30xyz"}
    });
    assert_eq!(
        probe_health_check_timeout(&body),
        std::time::Duration::from_secs(60)
    );
}

#[test]
fn probe_health_check_timeout_refuses_empty_string() {
    let body = serde_json::json!({
        "resource_opts": {"health_check_timeout": ""}
    });
    assert_eq!(
        probe_health_check_timeout(&body),
        std::time::Duration::from_secs(60)
    );
}

#[test]
fn probe_health_check_timeout_ms_and_m() {
    let body = serde_json::json!({
        "resource_opts": {"health_check_timeout": "500ms"}
    });
    assert_eq!(
        probe_health_check_timeout(&body),
        std::time::Duration::from_millis(500)
    );
    let body = serde_json::json!({
        "resource_opts": {"health_check_timeout": "2m"}
    });
    assert_eq!(
        probe_health_check_timeout(&body),
        std::time::Duration::from_secs(120)
    );
}
