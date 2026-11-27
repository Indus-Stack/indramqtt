use super::*;

#[test]
fn canonical_action_kind_maps_every_mv03b_alias() {
    assert_eq!(canonical_action_kind("aws_kinesis"), "kinesis");
    assert_eq!(canonical_action_kind("gcp"), "gcp_pubsub");
    assert_eq!(canonical_action_kind("pubsub"), "gcp_pubsub");
    assert_eq!(canonical_action_kind("gcp_pubsub_producer"), "gcp_pubsub");
    assert_eq!(canonical_action_kind("gcp_pubsub_consumer"), "gcp_pubsub");
    assert_eq!(canonical_action_kind("azure_event_hubs"), "azure_eventhubs");
    assert_eq!(canonical_action_kind("azure"), "azure_eventhubs");
    assert_eq!(canonical_action_kind("eventhubs"), "azure_eventhubs");
    assert_eq!(
        canonical_action_kind("azure_event_hub_producer"),
        "azure_eventhubs"
    );
    assert_eq!(canonical_action_kind("oci"), "oci_streaming");
    assert_eq!(canonical_action_kind("aws_iot_core"), "aws_iot");
    assert_eq!(canonical_action_kind("azure_iot_hub"), "azure_iot");
    assert_eq!(canonical_action_kind("azure_iothub"), "azure_iot");
    assert_eq!(canonical_action_kind("iothub"), "azure_iot");
    assert_eq!(canonical_action_kind("gcp_iot_core"), "gcp_iot");
    assert_eq!(canonical_action_kind("cloud_iot"), "gcp_iot");
}

#[test]
fn action_owned_kind_covers_the_mv03b_kinds() {
    for kind in [
        "kinesis",
        "aws_kinesis",
        "gcp_pubsub",
        "gcp",
        "pubsub",
        "gcp_pubsub_producer",
        "gcp_pubsub_consumer",
        "azure_eventhubs",
        "azure_event_hubs",
        "azure",
        "eventhubs",
        "azure_event_hub_producer",
        "aws_iot",
        "aws_iot_core",
        "azure_iot",
        "azure_iot_hub",
        "azure_iothub",
        "iothub",
        "oci_streaming",
        "oci",
    ] {
        assert!(action_owned_kind(kind), "{kind} owns a runtime");
    }
    assert!(!action_owned_kind("gcp_iot"));
    assert!(!action_owned_kind("gcp_iot_core"));
}

#[cfg(feature = "kinesis")]
#[tokio::test]
async fn kinesis_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let doc = serde_json::json!({
        "endpoint": "http://127.0.0.1:9",
        "region": "us-east-1",
        "aws_access_key_id": "AKID",
        "aws_secret_access_key": "secret",
    });
    let rec = mv02d_record("act", "aws_kinesis", "k:up", serde_json::json!({}));
    let error = build_kinesis_sink(&rec, &doc)
        .err()
        .expect("a missing stream refuses");
    assert!(error.contains("stream_name"), "{error}");
    let rec = mv02d_record(
        "act",
        "kinesis",
        "k:up",
        serde_json::json!({"stream_name": "telemetry", "partition_key": "${client_id}"}),
    );
    let sink = build_kinesis_sink(&rec, &doc).expect("a full kinesis pair builds");
    assert_eq!(sink.kind(), "kinesis");
}

#[cfg(feature = "gcp_pubsub")]
#[tokio::test]
async fn gcp_pubsub_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let doc = serde_json::json!({"project_id": "demo-proj"});
    let rec = mv02d_record("act", "gcp", "g:up", serde_json::json!({}));
    let error = build_gcp_pubsub_sink(&rec, &doc)
        .err()
        .expect("a missing topic refuses");
    assert!(error.contains("pubsub_topic"), "{error}");
    let rec = mv02d_record(
        "act",
        "gcp_pubsub_producer",
        "g:up",
        serde_json::json!({"pubsub_topic": "events"}),
    );
    let sink = build_gcp_pubsub_sink(&rec, &doc).expect("a full pubsub pair builds");
    assert_eq!(sink.kind(), "gcp_pubsub");
}

#[cfg(feature = "azure_eventhubs")]
#[tokio::test]
async fn azure_eventhubs_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let doc = serde_json::json!({
        "namespace": "ns-1",
        "shared_access_key_name": "send",
        "shared_access_key": "secret",
    });
    let rec = mv02d_record("act", "azure", "e:up", serde_json::json!({}));
    let error = build_azure_eventhubs_sink(&rec, &doc)
        .err()
        .expect("a missing hub refuses");
    assert!(error.contains("event_hub"), "{error}");
    let rec = mv02d_record(
        "act",
        "azure_event_hub_producer",
        "e:up",
        serde_json::json!({"event_hub": "hub-a"}),
    );
    let sink = build_azure_eventhubs_sink(&rec, &doc).expect("a full hub pair builds");
    assert_eq!(sink.kind(), "azure_eventhubs");
}

#[cfg(feature = "aws_iot")]
#[tokio::test]
async fn aws_iot_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let doc = serde_json::json!({"endpoint": "127.0.0.1:8883"});
    let rec = mv02d_record(
        "act",
        "aws_iot",
        "a:up",
        serde_json::json!({"remote_topic": "t/${client_id}"}),
    );
    let error = build_aws_iot_sink(&rec, &doc)
        .err()
        .expect("a missing cert refuses");
    assert!(error.contains("client_cert_pem"), "{error}");
}

#[cfg(feature = "azure_iot")]
#[tokio::test]
async fn azure_iot_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let doc = serde_json::json!({"device_id": "edge-1", "shared_access_key": "s"});
    let rec = mv02d_record("act", "azure_iot", "z:up", serde_json::json!({}));
    let error = build_azure_iot_sink(&rec, &doc)
        .err()
        .expect("a missing hub refuses");
    assert!(error.contains("iot_hub_name"), "{error}");
}

#[tokio::test]
async fn oci_streaming_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let doc = serde_json::json!({
        "endpoint": "https://cell-1.streaming.us-east-1.oraclecloud.com",
        "stream_pool_id": "ocid1.streampool.oc1.phx.aaa",
        "tenancy_ocid": "ocid1.tenancy.oc1..aaa",
        "user_ocid": "ocid1.user.oc1..aaa",
        "fingerprint": "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99",
        "private_key_pem": "k",
    });
    let rec = mv02d_record("act", "oci", "o:up", serde_json::json!({}));
    let error = build_oci_streaming_sink(&rec, &doc)
        .err()
        .expect("a missing stream refuses");
    assert!(error.contains("stream_id"), "{error}");
}

#[cfg(feature = "rabbitmq")]
#[tokio::test]
async fn rabbitmq_sink_builds_with_empty_exchange_for_default_exchange() {
    // Empty exchange selects the default exchange.
    // See #components/schemas/bridge_rabbitmq.action_parameters.
    let rec = mv02b_record(
        "act",
        "rabbitmq",
        "rabbitmq:up",
        serde_json::json!({"routing_key": "queue.${topic}"}),
    );
    let doc = serde_json::json!({
        "servers": "127.0.0.1:5672",
        "username": "u",
        "password": "p",
    });
    let config = rabbitmq_sink_config(&rec, &doc).expect("empty exchange builds a config");
    assert_eq!(config.exchange, "");
    assert_eq!(config.routing_key_template, "queue.${topic}");
    let sink = build_rabbitmq_sink(&rec, &doc).expect("empty exchange builds a sink");
    assert_eq!(sink.kind(), "rabbitmq");
    let bare = mv02b_record("act", "rabbitmq", "rabbitmq:up", serde_json::json!({}));
    let error = build_rabbitmq_sink(&bare, &doc)
        .err()
        .expect("a missing routing key refuses");
    assert!(error.contains("routing_key"), "{error}");
}

#[test]
fn canonical_action_kind_maps_every_mv03a_alias() {
    assert_eq!(canonical_action_kind("mqtt"), "mqtt_bridge");
    assert_eq!(canonical_action_kind("kafka_producer"), "kafka");
    assert_eq!(canonical_action_kind("kafka_consumer"), "kafka");
    assert_eq!(canonical_action_kind("confluent_producer"), "confluent");
    assert_eq!(canonical_action_kind("confluent_kafka"), "confluent");
    assert_eq!(canonical_action_kind("confluent_cloud"), "confluent");
    assert_eq!(canonical_action_kind("rabbit"), "rabbitmq");
    assert_eq!(canonical_action_kind("amqp"), "rabbitmq");
    for kind in [
        "kafka",
        "confluent",
        "rabbitmq",
        "pulsar",
        "rocketmq",
        "mqtt_bridge",
        "redis",
    ] {
        assert_eq!(canonical_action_kind(kind), kind);
    }
}

#[test]
fn canonical_action_kind_maps_every_remaining_alias() {
    assert_eq!(canonical_action_kind("sqlserver"), "mssql");
    assert_eq!(canonical_action_kind("mariadb"), "mysql");
    assert_eq!(canonical_action_kind("cockroach"), "cockroachdb");
    assert_eq!(canonical_action_kind("postgres"), "pgsql");
    assert_eq!(canonical_action_kind("postgresql"), "pgsql");
    assert_eq!(canonical_action_kind("google_alloydb"), "alloydb");
    assert_eq!(canonical_action_kind("alloy_db"), "alloydb");
    assert_eq!(canonical_action_kind("opcua"), "opc_ua");
    assert_eq!(canonical_action_kind("sparkplug"), "sparkplug_b");
    assert_eq!(canonical_action_kind("spb"), "sparkplug_b");
    assert_eq!(canonical_action_kind("rest"), "http");
    assert_eq!(canonical_action_kind("logger"), "console");
}

#[test]
fn action_owned_kind_covers_the_mv03a_kinds() {
    for kind in [
        "kafka",
        "kafka_producer",
        "kafka_consumer",
        "confluent",
        "confluent_producer",
        "confluent_kafka",
        "confluent_cloud",
        "rabbitmq",
        "rabbit",
        "amqp",
        "pulsar",
        "rocketmq",
        "mqtt_bridge",
        "mqtt",
        "redis",
    ] {
        assert!(action_owned_kind(kind), "{kind} owns a runtime");
    }
}

#[cfg(feature = "kafka")]
#[test]
fn kafka_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let rec = mv02b_record(
        "act",
        "kafka",
        "kafka:up",
        serde_json::json!({"topic": "events"}),
    );
    let error = build_kafka_sink(&rec, &serde_json::json!({}))
        .err()
        .expect("a missing bootstrap host refuses");
    assert!(error.contains("bootstrap_hosts"), "{error}");
    let sasl = mv02b_record(
        "act",
        "kafka",
        "kafka:up",
        serde_json::json!({"topic": "events"}),
    );
    let error = build_kafka_sink(
        &sasl,
        &serde_json::json!({
            "bootstrap_hosts": "h:9092",
            "authentication": {"mechanism": "plain", "username": "u", "password": "p"},
        }),
    )
    .err()
    .expect("a SASL connector refuses");
    assert!(error.contains("authentication"), "{error}");
    let cfg = kafka_sink_config(
        &mv02b_record(
            "act",
            "kafka",
            "kafka:up",
            serde_json::json!({
                "topic": "events",
                "message": {"key": "${payload.k}"},
                "partition_strategy": "key_dispatch",
                "required_acks": "leader_only",
            }),
        ),
        &serde_json::json!({"bootstrap_hosts": "h:9092"}),
    )
    .expect("a full kafka config builds");
    assert_eq!(cfg.bootstrap_servers, "h:9092");
    assert_eq!(cfg.topic_template, "events");
    assert_eq!(cfg.key_template.as_deref(), Some("${payload.k}"));
    assert!(!cfg.random_partition);
    assert_eq!(cfg.acks, "1");
    let sink = build_kafka_sink(
        &mv02b_record(
            "act",
            "kafka",
            "kafka:up",
            serde_json::json!({"topic": "events"}),
        ),
        &serde_json::json!({"bootstrap_hosts": "h:9092"}),
    )
    .expect("a full kafka pair builds");
    assert_eq!(sink.kind(), "kafka");
    // Defaults: random strategy spreads, acks wait for all replicas.
    let cfg = kafka_sink_config(
        &mv02b_record(
            "act",
            "kafka",
            "kafka:up",
            serde_json::json!({"topic": "events"}),
        ),
        &serde_json::json!({"bootstrap_hosts": "h:9092"}),
    )
    .expect("defaults build");
    assert!(cfg.random_partition);
    assert_eq!(cfg.acks, "all");
    assert_eq!(cfg.key_template.as_deref(), Some("${.clientid}"));
}

#[cfg(feature = "confluent")]
#[test]
fn confluent_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let rec = mv02b_record(
        "act",
        "confluent",
        "confluent:up",
        serde_json::json!({"topic": "events"}),
    );
    let error = build_confluent_sink(&rec, &serde_json::json!({"bootstrap_hosts": "h:9092"}))
        .err()
        .expect("a missing API key refuses");
    assert!(error.contains("api_key"), "{error}");
    let error = build_confluent_sink(
        &rec,
        &serde_json::json!({"bootstrap_hosts": "h:9092", "api_key": "k"}),
    )
    .err()
    .expect("a missing API secret refuses");
    assert!(error.contains("api_secret"), "{error}");
    // The reference `authentication` object supplies the same pair.
    let cfg = confluent_sink_config(
        &rec,
        &serde_json::json!({
            "bootstrap_hosts": "h:9092",
            "authentication": {"username": "k", "password": "s"},
            "ssl": {"enable": false},
        }),
    )
    .expect("a nested credentials pair builds");
    assert_eq!(cfg.api_key, "k");
    assert_eq!(cfg.api_secret, "s");
    assert_eq!(
        cfg.security_protocol,
        broker_connectors_enterprise::confluent::ConfluentSecurityProtocol::SaslPlaintext
    );
    assert_eq!(cfg.bootstrap_servers, vec!["h:9092".to_string()]);
    assert_eq!(
        cfg.partition_key_template.as_deref(),
        Some("${.clientid}"),
        "absent key defaults to the reference client id"
    );
    let sink = build_confluent_sink(
        &rec,
        &serde_json::json!({"bootstrap_hosts": "h:9092", "api_key": "k", "api_secret": "s"}),
    )
    .expect("a full confluent pair builds");
    assert_eq!(sink.kind(), "confluent");
}

#[cfg(feature = "rabbitmq")]
#[test]
fn rabbitmq_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let rec = mv02b_record(
        "act",
        "rabbitmq",
        "rabbitmq:up",
        serde_json::json!({"routing_key": "sensor.${topic}"}),
    );
    let error = build_rabbitmq_sink(&rec, &serde_json::json!({"servers": "h:5672"}))
        .err()
        .expect("a missing username refuses");
    assert!(error.contains("username"), "{error}");
    let error = build_rabbitmq_sink(&rec, &serde_json::json!({"username": "u", "password": "p"}))
        .err()
        .expect("a missing target refuses");
    assert!(error.contains("servers"), "{error}");
    let cfg = rabbitmq_sink_config(
        &rec,
        &serde_json::json!({"servers": "h:5672", "username": "u", "password": "p"}),
    )
    .expect("a full rabbitmq config builds");
    assert_eq!(cfg.endpoint, "amqp://u:p@h:5672/%2f");
    assert_eq!(cfg.exchange, "");
    assert_eq!(cfg.routing_key_template, "sensor.${topic}");
    assert_eq!(cfg.delivery_mode, 1);
    let sink = build_rabbitmq_sink(
        &mv02b_record(
            "act",
            "rabbitmq",
            "rabbitmq:up",
            serde_json::json!({"routing_key": "k", "exchange": "e", "delivery_mode": "persistent"}),
        ),
        &serde_json::json!({"servers": "h:5672", "username": "u", "password": "p"}),
    )
    .expect("a full rabbitmq pair builds");
    assert_eq!(sink.kind(), "rabbitmq");
}

#[test]
fn redis_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let rec = mv02b_record(
        "act",
        "redis",
        "redis:up",
        serde_json::json!({"command_template": ["HSET", "s:${topic}", "f", "${payload}"]}),
    );
    let error = build_redis_sink(&rec, &serde_json::json!({}))
        .err()
        .expect("a missing server refuses");
    assert!(error.contains("servers"), "{error}");
    let cfg = redis_sink_config(
        &rec,
        &serde_json::json!({"servers": "h:6380", "password": "pw", "database": 2}),
    )
    .expect("a full redis config builds");
    assert_eq!(cfg.endpoint, "redis://:pw@h:6380/2");
    match &cfg.command {
        broker_connectors::redis::RedisCommandKind::Raw { argv_templates } => {
            assert_eq!(argv_templates.len(), 4);
            assert_eq!(argv_templates[0], "HSET");
        }
        other => panic!("the action builds a raw command, got {other:?}"),
    }
    let sink = build_redis_sink(&rec, &serde_json::json!({"servers": "h:6379"}))
        .expect("a full redis pair builds");
    assert_eq!(sink.kind(), "redis");
}

#[test]
fn mqtt_bridge_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let rec = mv02b_record(
        "act",
        "mqtt_bridge",
        "mqtt_bridge:up",
        serde_json::json!({"topic": "remote/${topic}", "qos": 1, "retain": true}),
    );
    let error = build_mqtt_bridge_sink(&rec, &serde_json::json!({}))
        .err()
        .expect("a missing server refuses");
    assert!(error.contains("server"), "{error}");
    let cfg = mqtt_bridge_sink_config(
        &rec,
        &serde_json::json!({"server": "h:1883", "proto_ver": "v5"}),
    )
    .expect("a full bridge config builds");
    assert_eq!(cfg.broker_address, "h:1883");
    assert_eq!(cfg.topic_template.as_deref(), Some("remote/${topic}"));
    assert_eq!(cfg.qos_override, Some(1));
    assert_eq!(cfg.retain_override, Some(true));
    assert_eq!(cfg.protocol, broker_connectors::MqttBridgeProtocol::V50);
    assert_eq!(cfg.max_batch_size, Some(1));
    let sink = build_mqtt_bridge_sink(&rec, &serde_json::json!({"server": "h:1883"}))
        .expect("a full bridge pair builds");
    assert_eq!(sink.kind(), "mqtt_bridge");
}

#[test]
fn pulsar_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let rec = mv02b_record(
        "act",
        "pulsar",
        "pulsar:up",
        serde_json::json!({"pulsar_topic": "events", "message": {"key": "${client_id}"}}),
    );
    let error = build_pulsar_sink(&rec, &serde_json::json!({}))
        .err()
        .expect("a missing service URL refuses");
    assert!(error.contains("servers"), "{error}");
    let error = build_pulsar_sink(&rec, &serde_json::json!({"servers": "http://h:8080"}))
        .err()
        .expect("an admin URL refuses");
    assert!(error.contains("pulsar://"), "{error}");
    let error = build_pulsar_sink(
        &rec,
        &serde_json::json!({"servers": "pulsar://h:6650", "authentication": "token"}),
    )
    .err()
    .expect("a token auth refuses");
    assert!(error.contains("authentication"), "{error}");
    let cfg = pulsar_sink_config(
        &rec,
        &serde_json::json!({"servers": "pulsar://h:6650", "tenant": "t", "namespace": "n"}),
    )
    .expect("a full pulsar config builds");
    assert_eq!(cfg.service_url, "pulsar://h:6650");
    assert_eq!(cfg.tenant, "t");
    assert_eq!(cfg.topic, "events");
    assert_eq!(cfg.batch_size, Some(1));
    assert_eq!(cfg.max_retries, Some(3));
    let sink = build_pulsar_sink(&rec, &serde_json::json!({"servers": "pulsar://h:6650"}))
        .expect("a full pulsar pair builds");
    assert_eq!(sink.kind(), "pulsar");
}

#[test]
fn rocketmq_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let rec = mv02b_record(
        "act",
        "rocketmq",
        "rocketmq:up",
        serde_json::json!({"tag": "t", "key": "${client_id}"}),
    );
    let error = build_rocketmq_sink(&rec, &serde_json::json!({}))
        .err()
        .expect("a missing server refuses");
    assert!(error.contains("servers"), "{error}");
    let error = build_rocketmq_sink(
        &rec,
        &serde_json::json!({"servers": "h:9876", "access_key": "k"}),
    )
    .err()
    .expect("a half ACL pair refuses");
    assert!(error.contains("secret_key"), "{error}");
    let cfg = rocketmq_sink_config(
        &rec,
        &serde_json::json!({"servers": "h:9876", "access_key": "k", "secret_key": "s"}),
    )
    .expect("a full rocketmq config builds");
    assert_eq!(cfg.endpoints, vec!["h:9876".to_string()]);
    assert_eq!(cfg.topic, "TopicTest");
    assert_eq!(cfg.tag_template.as_deref(), Some("t"));
    let sink = build_rocketmq_sink(&rec, &serde_json::json!({"servers": "h:9876"}))
        .expect("a full rocketmq pair builds");
    assert_eq!(sink.kind(), "rocketmq");
}

#[tokio::test]
async fn mv03c_canonical_maps_every_alias() {
    assert_eq!(canonical_action_kind("opcua"), "opc_ua");
    assert_eq!(canonical_action_kind("sparkplug"), "sparkplug_b");
    assert_eq!(canonical_action_kind("spb"), "sparkplug_b");
    assert_eq!(canonical_action_kind("rest"), "http");
    assert_eq!(canonical_action_kind("http"), "http");
}

#[tokio::test]
async fn mv03c_owned_kind_covers_device_and_http() {
    assert!(action_owned_kind("http"));
    assert!(action_owned_kind("rest"));
    assert!(action_owned_kind("opc_ua"));
    assert!(action_owned_kind("opcua"));
    assert!(action_owned_kind("sparkplug_b"));
    assert!(action_owned_kind("sparkplug"));
    assert!(action_owned_kind("spb"));
}

#[tokio::test]
async fn mv03c_opc_ua_sink_uses_connector_and_action_fields() {
    let doc = serde_json::json!({"endpoint_url": "opc.tcp://127.0.0.1:4840"});
    let rec = record("act", "opc_ua", "opc_ua:up");
    let error = build_opc_ua_sink(&rec, &doc)
        .err()
        .expect("a missing node id refuses");
    assert!(error.contains("node_id"), "{error}");
    let mut full = record("act", "opc_ua", "opc_ua:up");
    full.parameters = serde_json::json!({"node_id": "ns=1;i=1001"});
    let sink = build_opc_ua_sink(&full, &doc).expect("a full pair builds");
    assert_eq!(sink.kind(), "opc_ua");
    let bad_doc = serde_json::json!({"server": "127.0.0.1:4840"});
    let error = build_opc_ua_sink(&full, &bad_doc)
        .err()
        .expect("a missing endpoint refuses");
    assert!(error.contains("endpoint_url"), "{error}");
}

#[tokio::test]
async fn mv03c_sparkplug_b_sink_uses_connector_and_action_fields() {
    let doc = serde_json::json!({"mqtt_url": "mqtt://127.0.0.1:1883"});
    let mut rec = record("act", "sparkplug_b", "sparkplug_b:up");
    rec.parameters = serde_json::json!({"topic_prefix": "spBv1.0/plant1"});
    let sink = build_sparkplug_b_sink(&rec, &doc).expect("a full pair builds");
    assert_eq!(sink.kind(), "sparkplug_b");
    let bare = record("act", "sparkplug_b", "sparkplug_b:up");
    let error = build_sparkplug_b_sink(&bare, &serde_json::json!({}))
        .err()
        .expect("a missing mqtt url refuses");
    assert!(error.contains("mqtt_url"), "{error}");
    let mut bad_prefix = record("act", "sparkplug_b", "sparkplug_b:up");
    bad_prefix.parameters = serde_json::json!({"topic_prefix": "spBv1.0/a/#/x"});
    let error = required_action_parameters(&bad_prefix).expect_err("a wildcard prefix refuses");
    assert!(error.contains("topic_prefix"), "{error}");
}
