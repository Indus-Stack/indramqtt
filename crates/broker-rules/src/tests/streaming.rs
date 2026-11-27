use super::*;

/// Sprint 17 e2e: one rule fans out to all five analytical sinks
/// (Kafka, Redis, MySQL, ClickHouse, InfluxDB) via distinct
/// connectors. TCP sinks use in-memory transports, HTTP sinks use
/// in-process axum fakes: no broker anywhere in the loop.
// Needs the `kafka` + `clickhouse` heavy SDKs (X1-10).
#[cfg(all(feature = "kafka", feature = "clickhouse"))]
#[tokio::test]
async fn test_rule_fans_out_to_five_analytical_sinks() {
    use axum::{extract::State, routing::post, Router};
    use broker_connectors::{
        kafka::KafkaHeaderEncodeMode, ClickHouseSink, ClickHouseSinkConfig, InfluxDbSink,
        InfluxDbSinkConfig, KafkaSink, KafkaSinkConfig, MemoryKafkaTransport, MemoryMySqlTransport,
        MemoryRedisTransport, MySqlSink, MySqlSinkConfig, RedisCommandKind, RedisSink,
        RedisSinkConfig,
    };
    use tokio::net::TcpListener;

    async fn serve_capture(route: &str, captured: Arc<parking_lot::Mutex<String>>) -> u16 {
        async fn handler(
            State(captured): State<Arc<parking_lot::Mutex<String>>>,
            body: String,
        ) -> axum::http::StatusCode {
            *captured.lock() = body;
            axum::http::StatusCode::OK
        }
        let app = Router::new()
            .route(route, post(handler))
            .with_state(captured);
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        port
    }

    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);

    // Kafka (memory transport).
    let kafka_transport = Arc::new(MemoryKafkaTransport::new());
    let kafka = Arc::new(
        KafkaSink::new(
            KafkaSinkConfig {
                bootstrap_servers: "unused:9092".to_string(),
                topic_template: "out-${topic}".to_string(),
                partition_key_field: None,
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
            kafka_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("kafka-an", kafka.clone());

    // Redis (memory transport, immediate dispatch).
    let redis_transport = Arc::new(MemoryRedisTransport::new());
    let redis = Arc::new(
        RedisSink::new(
            RedisSinkConfig {
                endpoint: "redis://unused:6379".to_string(),
                command: RedisCommandKind::Publish {
                    channel_template: "fanout".to_string(),
                },
            },
            redis_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("redis-an", redis.clone());

    // MySQL (memory transport).
    let mysql_transport = Arc::new(MemoryMySqlTransport::new());
    let mysql = Arc::new(
        MySqlSink::new(
            MySqlSinkConfig {
                connection_url: "mysql://u:p@unused:3306/db".to_string(),
                sql_template: "INSERT INTO mqtt_events (topic, qos, payload) VALUES (?, ?, ?)"
                    .to_string(),
                pool_size: 1,
                batch_size: 100,
                batch_timeout_ms: 50,
            },
            mysql_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("mysql-an", mysql.clone());

    // ClickHouse (mock driver transport, offline).
    let ch_transport = Arc::new(broker_connectors::MockClickHouseTransport::new(
        "indra",
        "mqtt_events",
        "JSONEachRow",
    ));
    let clickhouse = Arc::new(
        ClickHouseSink::new(
            ClickHouseSinkConfig {
                endpoint: "http://127.0.0.1:8123".to_string(),
                database: "indra".to_string(),
                table: "mqtt_events".to_string(),
                format: "JSONEachRow".to_string(),
                batch_size: 100,
                batch_timeout_ms: 100,
                username: "default".to_string(),
                password: String::new(),
                request_timeout_ms: None,
            },
            ch_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("ch-an", clickhouse.clone());

    // InfluxDB (axum fake).
    let influx_body = Arc::new(parking_lot::Mutex::new(String::new()));
    let influx_port = serve_capture("/api/v2/write", influx_body.clone()).await;
    let influx = Arc::new(
        InfluxDbSink::new(
            InfluxDbSinkConfig {
                endpoint: format!("http://127.0.0.1:{influx_port}"),
                bucket: "mqtt".to_string(),
                org: "indra".to_string(),
                token: "secret".to_string(),
                measurement_template: "mqtt_events".to_string(),
                precision: "ms".to_string(),
                batch_size: 100,
                batch_timeout_ms: 50,
                request_timeout_ms: None,
            },
            reqwest::Client::new(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("influx-an", influx.clone());

    // One rule, five ForwardConnector actions, SQL projection.
    engine
        .create_rule(
            "fanout-five".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            Some(
                r#"SELECT temperature FROM "sensors/+" WHERE temperature IS NOT NULL"#.to_string(),
            ),
            true,
            ["kafka-an", "redis-an", "mysql-an", "ch-an", "influx-an"]
                .into_iter()
                .map(|id| RuleAction::ForwardConnector {
                    connector_id: id.to_string(),
                })
                .collect(),
        )
        .expect("rule creates");

    let sink: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());
    engine
        .dispatch_ingress(
            &Topic::new("sensors/kitchen").unwrap(),
            &Bytes::from_static(br#"{ "temperature": 72.5, "secret": "hide_me" }"#),
            QoS::AtMostOnce,
            &sink,
        )
        .await;

    // Buffered sinks flush explicitly (Redis dispatches inline).
    kafka.flush().await.expect("kafka flush");
    mysql.flush().await.expect("mysql flush");
    clickhouse.flush().await.expect("clickhouse flush");
    influx.flush().await.expect("influx flush");

    let projected = serde_json::json!({ "temperature": 72.5 });

    let records = kafka_transport.records_flat();
    assert_eq!(records.len(), 1);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&records[0].value).unwrap(),
        projected
    );

    let commands = redis_transport.commands();
    assert_eq!(commands.len(), 1);
    let argv = &commands[0].argv;
    assert_eq!(argv[0], b"PUBLISH".to_vec());
    assert_eq!(argv[1], b"fanout".to_vec());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&argv[2]).unwrap(),
        projected
    );

    let batches = mysql_transport.batches();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].rows.len(), 1);
    assert_eq!(batches[0].rows[0][0], b"sensors/kitchen".to_vec());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&batches[0].rows[0][2]).unwrap(),
        projected
    );

    let ch_captured = ch_transport.captured();
    assert_eq!(ch_captured.len(), 1);
    assert_eq!(ch_captured[0].rows.len(), 1);
    assert_eq!(ch_captured[0].rows[0].topic, "sensors/kitchen");
    assert_eq!(ch_captured[0].rows[0].payload, projected.to_string());

    let influx_lines: Vec<String> = influx_body.lock().lines().map(str::to_string).collect();
    assert_eq!(influx_lines.len(), 1);
    assert!(
        influx_lines[0].starts_with("mqtt_events,topic=sensors/kitchen "),
        "unexpected line: {}",
        influx_lines[0]
    );
    assert!(influx_lines[0].contains("payload=\"{\\\"temperature\\\":72.5}\""));

    // Non-JSON ingress matches nothing: all five sinks stay silent.
    engine
        .dispatch_ingress(
            &Topic::new("sensors/kitchen").unwrap(),
            &Bytes::from_static(b"not json"),
            QoS::AtMostOnce,
            &sink,
        )
        .await;
    kafka.flush().await.expect("kafka flush");
    mysql.flush().await.expect("mysql flush");
    clickhouse.flush().await.expect("clickhouse flush");
    influx.flush().await.expect("influx flush");
    assert_eq!(kafka_transport.records_flat().len(), 1);
    assert_eq!(redis_transport.commands().len(), 1);
    assert_eq!(mysql_transport.batches().len(), 1);
    assert_eq!(ch_transport.captured().len(), 1);
    assert_eq!(influx_body.lock().lines().count(), 1);
}

/// Industrial e2e (INDRA-217): one Sparkplug telemetry ingress
/// routes through streaming SQL `INTO connector(...)` rules and
/// fans out simultaneously to the webhook, MQTT bridge, disk log
/// and Sparkplug B sinks. All transports are in-memory.
#[tokio::test]
async fn test_into_fans_out_to_industrial_sinks() {
    use broker_connectors::{
        DiskLogSink, DiskLogSinkConfig, HttpSink, HttpSinkConfig, MemoryDiskLogWriter,
        MemoryMqttBridgeTransport, MockHttpTransport, MqttBridgeSink, MqttBridgeSinkConfig,
    };
    use broker_connectors_enterprise::{
        MemorySparkplugTransport, SparkplugBSink, SparkplugSinkConfig,
    };

    let engine = RuleEngine::new(65_536, BackpressurePolicy::DropOldest);

    // Webhook (auto-flush every row).
    let hook_transport = Arc::new(MockHttpTransport::new());
    let hook = Arc::new(
        HttpSink::new(
            HttpSinkConfig {
                url: "https://hooks.example.com/ingest/${topic}".to_string(),
                method: broker_connectors::HttpMethod::Post,
                headers: HashMap::new(),
                auth: broker_connectors::HttpAuth::None,
                body_format: broker_connectors::HttpBodyFormat::RawJson,
                signature: None,
                batch_size: Some(1),
                batch_bytes: None,
                linger_ms: None,
                timeout_ms: Some(5_000),
                max_retries: Some(3),
                initial_backoff_ms: Some(1),
                max_backoff_ms: Some(2),
                buffer_capacity: Some(10_000),
            },
            hook_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine
        .connectors()
        .register("hook-industrial", hook.clone());

    // MQTT bridge (auto-flush every row).
    let bridge_transport = Arc::new(MemoryMqttBridgeTransport::new());
    let bridge = Arc::new(
        MqttBridgeSink::new(
            MqttBridgeSinkConfig {
                broker_address: "mqtt://upstream:1883".to_string(),
                client_id: "indra-bridge-test".to_string(),
                clean_start: true,
                username: None,
                password: None,
                keep_alive_secs: 60,
                topic_prefix: Some("upstream/".to_string()),
                topic_template: None,
                payload_template: None,
                qos_override: None,
                retain_override: None,
                max_inflight: Some(10_000),
                max_batch_size: Some(1),
                linger_ms: Some(10),
                protocol: broker_connectors::MqttBridgeProtocol::V311,
            },
            bridge_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine
        .connectors()
        .register("bridge-upstream", bridge.clone());

    // Disk audit log (write-through, in-memory segments).
    let disk_writer = Arc::new(
        MemoryDiskLogWriter::new(&DiskLogSinkConfig {
            directory: "memory://audit".to_string(),
            filename_prefix: "audit".to_string(),
            filename_extension: "log".to_string(),
            format: broker_connectors::DiskLogFormat::Ndjson,
            max_file_size_bytes: None,
            max_file_age_secs: None,
            compression: broker_connectors::DiskLogCompression::None,
            max_backup_files: None,
            max_retention_days: None,
            sync_mode: broker_connectors::DiskSyncMode::OsDefault,
            timeout_ms: None,
        })
        .expect("valid writer"),
    );
    let disk = Arc::new(
        DiskLogSink::new(
            DiskLogSinkConfig {
                directory: "memory://audit".to_string(),
                filename_prefix: "audit".to_string(),
                filename_extension: "log".to_string(),
                format: broker_connectors::DiskLogFormat::Ndjson,
                max_file_size_bytes: None,
                max_file_age_secs: None,
                compression: broker_connectors::DiskLogCompression::None,
                max_backup_files: None,
                max_retention_days: None,
                sync_mode: broker_connectors::DiskSyncMode::OsDefault,
                timeout_ms: None,
            },
            disk_writer.clone(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("disk-audit", disk.clone());

    // Sparkplug B frames (auto-flush every row).
    let spb_transport = Arc::new(MemorySparkplugTransport::new());
    let spb = Arc::new(
        SparkplugBSink::new(
            SparkplugSinkConfig {
                topic_prefix: Some("spBv1.0/plant1".to_string()),
                tier: "enterprise".to_string(),
                batch_size: Some(1),
                linger_ms: Some(50),
                timeout_ms: None,
                mqtt_url: None,
                client_id: None,
                max_aliases: Some(10_000),
            },
            spb_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("spb-metrics", spb.clone());

    // One INTO rule per sink over the same Sparkplug stream.
    for (id, connector) in [
        ("hook-rule", "hook-industrial"),
        ("bridge-rule", "bridge-upstream"),
        ("disk-rule", "disk-audit"),
        ("spb-rule", "spb-metrics"),
    ] {
        engine
            .create_rule(
                id.to_string(),
                TopicFilter::new("spBv1.0/#").unwrap(),
                Some(format!(
                    r#"SELECT metrics FROM "spBv1.0/#" WHERE metrics.Temperature > 80.0 INTO connector("{connector}")"#
                )),
                true,
                vec![],
            )
            .expect("rule creates");
    }

    let sink: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());
    engine
        .dispatch_ingress(
            &Topic::new("spBv1.0/plant1/DDATA/edge7/plc3").unwrap(),
            &Bytes::from_static(br#"{ "metrics": { "Temperature": 82.5 }, "seq": 14 }"#),
            QoS::AtMostOnce,
            &sink,
        )
        .await;

    let projected = serde_json::json!({"metrics": {"Temperature": 82.5}});

    // Webhook got the projected JSON at the templated URL.
    let captured = hook_transport.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(
        captured[0].url,
        "https://hooks.example.com/ingest/spBv1.0/plant1/DDATA/edge7/plc3"
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&captured[0].body).unwrap(),
        projected
    );

    // Bridge got one QoS 0 frame on the prefixed topic.
    let packets = bridge_transport.packets();
    assert_eq!(packets.len(), 1);
    assert_eq!(packets[0].topic, "upstream/spBv1.0/plant1/DDATA/edge7/plc3");
    let decoded = broker_connectors::decode_publish(&packets[0].bytes, false).unwrap();
    assert_eq!(decoded.qos, 0);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&decoded.payload).unwrap(),
        projected
    );

    // Disk got one ndjson audit line.
    let current = String::from_utf8(disk_writer.current_bytes()).unwrap();
    assert_eq!(current.lines().count(), 1);
    let row: serde_json::Value = serde_json::from_str(current.trim_end()).unwrap();
    assert_eq!(row["topic"], "spBv1.0/plant1/DDATA/edge7/plc3");
    assert_eq!(row["payload"], projected);

    // Sparkplug got one Protobuf frame decoding back to metrics.
    let frames = spb_transport.frames();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].topic, "spBv1.0/plant1/DDATA/edge7/plc3");
    let back = broker_connectors_enterprise::decode_payload(&frames[0].payload).unwrap();
    let metrics: HashMap<String, broker_connectors_enterprise::SpbValue> = back
        .metrics
        .into_iter()
        .map(|metric| (metric.name.clone().unwrap(), metric.value))
        .collect();
    assert_eq!(
        metrics["Temperature"],
        broker_connectors_enterprise::SpbValue::Double(82.5)
    );

    // Below-threshold telemetry fires nothing anywhere.
    engine
        .dispatch_ingress(
            &Topic::new("spBv1.0/plant1/DDATA/edge7/plc3").unwrap(),
            &Bytes::from_static(br#"{ "metrics": { "Temperature": 70.0 } }"#),
            QoS::AtMostOnce,
            &sink,
        )
        .await;
    assert_eq!(hook_transport.captured().len(), 1);
    assert_eq!(bridge_transport.packets().len(), 1);
    let current = String::from_utf8(disk_writer.current_bytes()).unwrap();
    assert_eq!(current.lines().count(), 1);
    assert_eq!(spb_transport.frames().len(), 1);
}

/// Multi-cloud e2e (INDRA-218): one ingress event routes through
/// streaming SQL `INTO connector(...)` rules and fans out
/// simultaneously to the Kinesis, GCP Pub/Sub, Azure Event Hubs
/// and Pulsar mocks. All transports are in-memory.
// Needs the `azure_eventhubs`, `gcp_pubsub` and `kinesis` heavy
// SDKs; runs on feature builds.
#[cfg(all(
    feature = "azure_eventhubs",
    feature = "gcp_pubsub",
    feature = "kinesis"
))]
#[tokio::test]
async fn test_into_fans_out_to_cloud_sinks() {
    use broker_connectors_enterprise::{
        AzureEventHubsSink, AzureEventHubsSinkConfig, GcpPubSubSink, GcpPubSubSinkConfig,
        KinesisSink, KinesisSinkConfig, MemoryPulsarTransport, MockAzureEventHubsTransport,
        MockGcpPubSubTransport, MockKinesisTransport, PulsarSink, PulsarSinkConfig,
    };

    let engine = RuleEngine::new(65_536, BackpressurePolicy::DropOldest);

    // Kinesis (auto-flush every row, clean mock).
    let kinesis_transport = Arc::new(MockKinesisTransport::new());
    let kinesis = Arc::new(
        KinesisSink::new(
            KinesisSinkConfig {
                stream_name: "telemetry-stream".to_string(),
                region: "us-east-1".to_string(),
                endpoint: None,
                access_key_id: "AKID".to_string(),
                secret_access_key: "secret".to_string(),
                session_token: None,
                partition_key_template: Some("${client_id}".to_string()),
                explicit_hash_key: None,
                batch_size: Some(1),
                batch_bytes: None,
                linger_ms: Some(20),
                max_retries: Some(5),
                initial_backoff_ms: Some(1),
                max_backoff_ms: Some(2),
                timeout_ms: None,
            },
            kinesis_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("aws-kinesis", kinesis.clone());

    // GCP Pub/Sub (auto-flush every row).
    let gcp_transport = Arc::new(MockGcpPubSubTransport::new());
    let gcp = Arc::new(
        GcpPubSubSink::new(
            GcpPubSubSinkConfig {
                project_id: "my-iot-project".to_string(),
                topic_id: "telemetry-events".to_string(),
                endpoint: None,
                auth: broker_connectors_enterprise::GcpAuth::None,
                ordering_key_template: Some("${client_id}".to_string()),
                attributes: HashMap::from([("source".to_string(), "indramqtt".to_string())]),
                batch_size: Some(1),
                batch_bytes: None,
                linger_ms: Some(10),
                max_retries: Some(3),
                initial_backoff_ms: Some(1),
                max_backoff_ms: Some(2),
                timeout_ms: None,
            },
            gcp_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("gcp-pubsub", gcp.clone());

    // Azure Event Hubs (auto-flush every row).
    let azure_transport = Arc::new(MockAzureEventHubsTransport::new());
    let azure = Arc::new(
        AzureEventHubsSink::new(
            AzureEventHubsSinkConfig {
                namespace: "my-eventhub-ns".to_string(),
                event_hub: "telemetry-hub".to_string(),
                endpoint: None,
                shared_access_key_name: "SendPolicy".to_string(),
                shared_access_key: "c2VjcmV0".to_string(),
                partition_key_template: Some("${client_id}".to_string()),
                user_properties: HashMap::new(),
                token_ttl_secs: 3_600,
                batch_size: Some(1),
                batch_bytes: None,
                linger_ms: Some(20),
                max_retries: Some(4),
                initial_backoff_ms: Some(1),
                max_backoff_ms: Some(2),
                timeout_ms: None,
            },
            azure_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine
        .connectors()
        .register("azure-eventhubs", azure.clone());

    // Pulsar (auto-flush every row).
    let pulsar_transport = Arc::new(MemoryPulsarTransport::new());
    let pulsar = Arc::new(
        PulsarSink::new(
            PulsarSinkConfig {
                service_url: "pulsar://unused:6650".to_string(),
                tenant: "public".to_string(),
                namespace: "default".to_string(),
                topic: "${topic}".to_string(),
                auth: broker_connectors_enterprise::PulsarAuth::None,
                partition_key_template: Some("${client_id}".to_string()),
                strategy: "key_dispatch".to_string(),
                properties: HashMap::new(),
                batch_size: Some(1),
                batch_bytes: None,
                linger_ms: Some(10),
                max_retries: Some(3),
                initial_backoff_ms: Some(1),
                max_backoff_ms: Some(2),
                timeout_ms: None,
            },
            pulsar_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("pulsar-sink", pulsar.clone());

    // One INTO rule per cloud.
    for (id, connector) in [
        ("kinesis-rule", "aws-kinesis"),
        ("gcp-rule", "gcp-pubsub"),
        ("azure-rule", "azure-eventhubs"),
        ("pulsar-rule", "pulsar-sink"),
    ] {
        engine
            .create_rule(
                id.to_string(),
                TopicFilter::new("sensors/+").unwrap(),
                Some(format!(
                    r#"SELECT client_id, temp FROM "sensors/+" WHERE temp > 20.0 INTO connector("{connector}")"#
                )),
                true,
                vec![],
            )
            .expect("rule creates");
    }

    let sink: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());
    engine
        .dispatch_ingress(
            &Topic::new("sensors/kitchen").unwrap(),
            &Bytes::from_static(br#"{ "client_id": "device-42", "temp": 22.5 }"#),
            QoS::AtMostOnce,
            &sink,
        )
        .await;

    let projected = serde_json::json!({"client_id": "device-42", "temp": 22.5});

    // Kinesis: one batch, keyed by client_id, base64 body.
    let captured = kinesis_transport.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].stream_name, "telemetry-stream");
    assert_eq!(captured[0].records.len(), 1);
    assert_eq!(captured[0].records[0].partition_key, "device-42");
    let payload = base64_decode(&captured[0].records[0].data_b64);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&payload).unwrap(),
        projected
    );

    // GCP: one message with ordering key + attributes.
    let captured = gcp_transport.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].project, "my-iot-project");
    assert_eq!(captured[0].messages.len(), 1);
    assert_eq!(
        captured[0].messages[0].ordering_key.as_deref(),
        Some("device-42")
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&base64_decode(
            &captured[0].messages[0].data_b64
        ))
        .unwrap(),
        projected
    );

    // Azure: one event with the partition key + SAS token.
    let captured = azure_transport.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].hub, "telemetry-hub");
    assert_eq!(captured[0].events.len(), 1);
    assert_eq!(
        captured[0].events[0].partition_key.as_deref(),
        Some("device-42")
    );
    assert!(captured[0]
        .sas_token
        .starts_with("SharedAccessSignature sr="));
    let payload = base64_decode(&captured[0].events[0].body_b64);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&payload).unwrap(),
        projected
    );

    // Pulsar: one message on the canonical topic path.
    let captured = pulsar_transport.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(
        captured[0].topic_path,
        "persistent://public/default/sensors.kitchen"
    );
    assert_eq!(captured[0].messages.len(), 1);
    assert_eq!(captured[0].messages[0].sequence_id, 0);
    assert_eq!(
        captured[0].messages[0].partition_key.as_deref(),
        Some("device-42")
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&captured[0].messages[0].payload).unwrap(),
        projected
    );

    // Below-threshold telemetry fires nothing anywhere.
    engine
        .dispatch_ingress(
            &Topic::new("sensors/kitchen").unwrap(),
            &Bytes::from_static(br#"{ "client_id": "device-42", "temp": 10.0 }"#),
            QoS::AtMostOnce,
            &sink,
        )
        .await;
    assert_eq!(kinesis_transport.captured().len(), 1);
    assert_eq!(gcp_transport.captured().len(), 1);
    assert_eq!(azure_transport.captured().len(), 1);
    assert_eq!(pulsar_transport.captured().len(), 1);
}

/// IoT + industrial e2e (INDRA-219/220): one ingress event fans
/// out through streaming SQL `INTO connector(...)` rules to the
/// OCI, AWS IoT, Azure IoT and GCP IoT mocks, while a raw scalar
/// ingress forwards (no SQL projection, bytes verbatim) into the
/// OPC-UA memory transport as a typed node write. All transports
/// are in-memory; the RSA key is a fixed test key (never deployed).
/// X1-09: needs the `aws_iot`, `azure_iot`, `gcp_iot` and `opc_ua` features.
#[cfg(all(
    feature = "aws_iot",
    feature = "azure_iot",
    feature = "gcp_iot",
    feature = "opc_ua"
))]
#[tokio::test]
async fn test_into_fans_out_to_iot_industrial_sinks() {
    use broker_connectors_enterprise::{
        AwsIotAuth, AwsIotConfig, AwsIotSink, AzureIotAuth, AzureIotConfig, AzureIotSink,
        BridgeTopicMapping, GcpIotAlgorithm, GcpIotConfig, GcpIotSink, MemoryOpcUaTransport,
        MockAwsIotTransport, MockAzureIotTransport, MockGcpIotTransport, MockOciStreamingTransport,
        NodeSubscriptionConfig, OciStreamingSink, OciStreamingSinkConfig, OpcUaAuth,
        OpcUaSecurityMode, OpcUaSecurityPolicy, OpcUaSink, OpcUaSinkConfig, OpcUaVariant,
    };
    let engine = RuleEngine::new(65_536, BackpressurePolicy::DropOldest);

    // Fixed test RSA key for the OCI signature + GCP JWT legs
    // (openssl-generated, in-memory, never deployed).
    let test_pem = "-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCgQlU8hDdoMjP5
QU2fhr0g+2n5HQSvgQaDKkpZFftqbrMixmFg3pGQN+GZp9vIscT+BlNrJueigXpn
pRbhI0RRj8EvVl+4Or+0hdzeLDmfGl/9SIhnyiRsJ7YDeO3uZq/Cff2zMeXqqbk4
RgKwbQksnJPOOYgVOfPrLjHJdEkNcSxT44tyOVrhBVISYm+zUw4By4GSQXp4RoTa
8UFX/gHoa+31um/9yZfDf9ekzelBys+4iSBeJ6imdStCjt8K+71yxewMSMD6HiCj
2GWvp+mPixqf4GcwaiqoFgJj+IKspc3eyozqJY/+610aaSw/ooO2AFXfErJlJEBP
H5hITBUlAgMBAAECggEAErJqe1r5k+B3i9cAlWIE4royTOwDxe4JsnfWoLod0PcF
U0NNzR1qYicC3Qhmbe2/i9t1FAU/9QeiHkF2f+G7cMCSy1EKbdX807TiZdFHD7bm
CAjUUTeWNEAVziXnrG6yhsBoPuXNaylOALC6U5cFAP1riR3RMJjISmHjURuOAlFI
W8tlKq77I5a4L93IW+2/elDPTjhUYsQnSEtJPWG/BizSVihHSiHh2lAN0JLZChWk
6J2e2FaZYC28Swu/V+GLXLg0Ai8GkSZNYTqOf8HqnkB7X3G7+OMnLPW+0I5GPOh+
9aX9WXhGAI6gxJf6UjcD5axHV+mdfgQnxBsJY4HxqQKBgQDfUbtYzfLBxnnxk0H7
N+lhdiANA/YXiR4OljziTSTjnnSDdabktr3ubIofpEwjvAqKIdn5ruyqdT/9GOZt
/7sai0aqyIzGlQiLhkxHvHBIxGajRBbPq2BhLqQYBeL0Xy4oMcD5NjGoUuhcck0b
bw6h935CIJYzJQtx+K/U4I2DwwKBgQC3timBQPbq+wJx4yuyf+VOy4r9qW0/4DUM
pw3qo1tqOk1hKN6pZazov0qfrGEKIOG4Ws0rLCKgwKwocdfFfzPwTgg8srl5r5XM
k5r97mHNYqSlboAE2YIM+CziUmVqklkMqQ4Hs38jk3tswt6yY+syrfZbp/Rhh/XK
pVv1itr89wKBgQDBi1VihsNw+7IuE2Eo9/E1faoTfa5oAXdiTwUfYJqrB2aVlH77
VAHSRJGFEODIS62av/Hpepg0t3+ovE7hYLTpMXIii8OuS/Xm7pLnzUJHXqhRsa5P
d4kFUOX4yAlFn8QiI9TKaBSrfIdTr+Bx+VNmPlhXuWRTmTSNJ2pEhgU//wKBgAt7
Vxy88rG8/mofyJtfYvWJwyYXcLyNRsODrVr82rnI6w0ngMMVl7j0O7W/EFGRvInJ
IwmPuJpTcG8WrmWpjZV3SwyAHxd74eDnWMiGHZa4k5HDVjz3Wyl0WVnLzIrcmrQv
3LCeh1Ox5AToKQL9O7XvKXaRCLUPykzgCN9PzmABAoGAUW/AIrYerL0oU0KBRZ5B
6NX+5++R6jugN/spvVs4OLwPM5a6ud6Bq/+BA3aT7NWdfypVgybotEytsb/y1oe2
XdFhno110rcMp6WoQHM4dCWw3cmRNbMv2aleY2FrTZlpxEXeA47iF5/if1VHldmR
Y7LzJJ6LCjfUFy8dMINZC7M=
-----END PRIVATE KEY-----
"
    .to_string();

    // OCI Streaming (auto-flush every row, clean mock).
    let oci_transport = Arc::new(MockOciStreamingTransport::new());
    let oci = Arc::new(
        OciStreamingSink::new(
            OciStreamingSinkConfig {
                endpoint: "https://cell-1.streaming.us-east-1.oci.oraclecloud.com".to_string(),
                stream_pool_id: "ocid1.streampool.oc1..testpool".to_string(),
                stream_id: "ocid1.stream.oc1..teststream".to_string(),
                tenancy_ocid: "ocid1.tenancy.oc1..test".to_string(),
                user_ocid: "ocid1.user.oc1..test".to_string(),
                fingerprint: "20:3b:97:13:aa:bb:cc:dd:ee:ff:00:11:22:33:44:55".to_string(),
                private_key_pem: test_pem.clone(),
                partition_key_template: "${client_id}".to_string(),
                batch_size: Some(1),
                buffer_capacity: None,
                batch_bytes: None,
                linger_ms: Some(10),
                max_retries: Some(5),
                initial_backoff_ms: Some(1),
                max_backoff_ms: Some(2),
                timeout_ms: None,
            },
            oci_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("oci-sink", oci.clone());

    // AWS IoT Core (SigV4 dummies; the mock never signs).
    let aws_transport = Arc::new(MockAwsIotTransport::new());
    let aws = Arc::new(
        AwsIotSink::new(
            AwsIotConfig {
                endpoint: "abc-ats.iot.us-east-1.amazonaws.com".to_string(),
                region: "us-east-1".to_string(),
                client_id: "e2e-bridge".to_string(),
                auth: AwsIotAuth::SigV4 {
                    access_key_id: "AKID".to_string(),
                    secret_access_key: "secret".to_string(),
                    session_token: None,
                },
                topic_mappings: vec![BridgeTopicMapping {
                    local_topic: "sensors/+".to_string(),
                    remote_topic: "indra/up".to_string(),
                    direction: broker_connectors_enterprise::BridgeDirection::LocalToRemote,
                }],
                shadow_sync: None,
                batch_size: Some(1),
                buffer_capacity: None,
                linger_ms: Some(10),
                max_retries: Some(5),
                timeout_ms: None,
                connect_timeout_ms: None,
                handshake_timeout_ms: None,
                ca_bundle_pem: None,
                alpn_protocols: None,
            },
            aws_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("aws-iot-sink", aws.clone());

    // Azure IoT Hub (SAS minted locally with HMAC, no network).
    let azure_transport = Arc::new(MockAzureIotTransport::new());
    let azure = Arc::new(
        AzureIotSink::new(
            AzureIotConfig {
                iot_hub_name: "e2e-hub".to_string(),
                device_id: "e2e-device".to_string(),
                module_id: None,
                auth: AzureIotAuth::SharedAccessKey {
                    key: "c2VjcmV0".to_string(),
                    key_name: Some("device".to_string()),
                },
                api_version: "2021-04-12".to_string(),
                direct_methods_enabled: false,
                twin_sync_enabled: false,
                batch_size: Some(1),
                buffer_capacity: None,
                linger_ms: Some(10),
                max_retries: Some(5),
                timeout_ms: None,
                connect_timeout_ms: None,
                handshake_timeout_ms: None,
                ca_bundle_pem: None,
                sas_ttl_secs: None,
            },
            azure_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine
        .connectors()
        .register("azure-iot-sink", azure.clone());

    // GCP IoT Core (RS256 JWT minted with the test key).
    let gcp_transport = Arc::new(MockGcpIotTransport::new());
    let gcp = Arc::new(
        GcpIotSink::new(
            GcpIotConfig {
                project_id: "e2e-project".to_string(),
                cloud_region: "us-central1".to_string(),
                registry_id: "e2e-registry".to_string(),
                device_id: "e2e-device".to_string(),
                private_key_pem: test_pem,
                algorithm: GcpIotAlgorithm::Rs256,
                token_lifetime_secs: 3_600,
                endpoint: "mqtt.googleapis.com:8883".to_string(),
                batch_size: Some(1),
                buffer_capacity: None,
                linger_ms: Some(10),
                max_retries: Some(5),
                timeout_ms: None,
                ca_bundle_pem: None,
            },
            gcp_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("gcp-iot-sink", gcp.clone());

    // OPC-UA (memory transport; the write leg matches a raw scalar
    // ingress against `write_topic_pattern`, no SQL projection).
    let opcua_transport = Arc::new(MemoryOpcUaTransport::new());
    let opcua = Arc::new(
        OpcUaSink::new(
            OpcUaSinkConfig {
                endpoint_url: "opc.tcp://127.0.0.1:4840".to_string(),
                security_policy: OpcUaSecurityPolicy::None,
                security_mode: OpcUaSecurityMode::None,
                auth: OpcUaAuth::Anonymous,
                node_subscriptions: vec![NodeSubscriptionConfig {
                    node_id: "ns=2;s=Setpoint".to_string(),
                    sampling_interval_ms: 1_000,
                    publish_topic_template: "opcua/setpoint".to_string(),
                    write_topic_pattern: Some("factory/setpoint/+".to_string()),
                }],
                buffer_capacity: None,
                batch_size: Some(1),
                linger_ms: Some(10),
                timeout_ms: None,
            },
            opcua_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("opcua-sink", opcua.clone());

    // One INTO rule per IoT cloud.
    for (id, connector) in [
        ("oci-rule", "oci-sink"),
        ("aws-rule", "aws-iot-sink"),
        ("azure-rule", "azure-iot-sink"),
        ("gcp-rule", "gcp-iot-sink"),
    ] {
        engine
            .create_rule(
                id.to_string(),
                TopicFilter::new("sensors/+").unwrap(),
                Some(format!(
                    r#"SELECT client_id, temp FROM "sensors/+" WHERE temp > 20.0 INTO connector("{connector}")"#
                )),
                true,
                vec![],
            )
            .expect("rule creates");
    }
    // OPC-UA: verbatim forward, no projection (scalars only).
    engine
        .create_rule(
            "opcua-rule".to_string(),
            TopicFilter::new("factory/setpoint/+").unwrap(),
            None,
            true,
            vec![RuleAction::ForwardConnector {
                connector_id: "opcua-sink".to_string(),
            }],
        )
        .expect("rule creates");

    let sink: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());
    engine
        .dispatch_ingress(
            &Topic::new("sensors/kitchen").unwrap(),
            &Bytes::from_static(br#"{ "client_id": "device-42", "temp": 22.5 }"#),
            QoS::AtMostOnce,
            &sink,
        )
        .await;

    let projected = serde_json::json!({"client_id": "device-42", "temp": 22.5});

    // OCI: one entry, partitioned by client_id, signed auth attached.
    let captured = oci_transport.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].messages.len(), 1);
    assert_eq!(
        base64_decode(&captured[0].messages[0].key_b64),
        b"device-42".to_vec()
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&base64_decode(
            &captured[0].messages[0].value_b64
        ))
        .unwrap(),
        projected
    );
    assert!(captured[0]
        .auth
        .authorization
        .starts_with("Signature version=\"1\""));

    // AWS IoT: one frame on the mapped remote topic.
    let captured = aws_transport.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].remote_topic, "indra/up");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&captured[0].payload).unwrap(),
        projected
    );

    // Azure IoT: one D2C publish with a SAS token.
    let captured = azure_transport.captured();
    assert_eq!(captured.len(), 1);
    assert!(captured[0].topic.contains("e2e-device"));
    assert!(captured[0]
        .sas_token
        .starts_with("SharedAccessSignature sr="));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&captured[0].body).unwrap(),
        projected
    );

    // GCP IoT: one telemetry publish with a JWT password.
    let captured = gcp_transport.captured();
    assert_eq!(captured.len(), 1);
    assert!(captured[0].topic.contains("e2e-device"));
    assert_eq!(captured[0].password_token.split('.').count(), 3);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&captured[0].body).unwrap(),
        projected
    );

    // OPC-UA: raw scalar ingress becomes a typed node write.
    engine
        .dispatch_ingress(
            &Topic::new("factory/setpoint/line1").unwrap(),
            &Bytes::from_static(b"22.5"),
            QoS::AtMostOnce,
            &sink,
        )
        .await;
    let writes = opcua_transport.writes();
    assert_eq!(writes.len(), 1);
    assert_eq!(writes[0].variant, OpcUaVariant::Double(22.5));
    assert!(format!("{:?}", writes[0].node_id).contains("Setpoint"));

    // Below-threshold telemetry fires nothing anywhere.
    engine
        .dispatch_ingress(
            &Topic::new("sensors/kitchen").unwrap(),
            &Bytes::from_static(br#"{ "client_id": "device-42", "temp": 10.0 }"#),
            QoS::AtMostOnce,
            &sink,
        )
        .await;
    assert_eq!(oci_transport.captured().len(), 1);
    assert_eq!(aws_transport.captured().len(), 1);
    assert_eq!(azure_transport.captured().len(), 1);
    assert_eq!(gcp_transport.captured().len(), 1);
    assert_eq!(opcua_transport.writes().len(), 1);
}

#[allow(dead_code)]
fn base64_decode(input: &str) -> Vec<u8> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(input)
        .unwrap()
}
