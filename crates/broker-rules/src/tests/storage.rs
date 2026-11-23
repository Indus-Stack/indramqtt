use super::*;

/// End to end across two database sinks: one ingress event fans out
/// through SQL projection into a mock Postgres batch buffer and a
/// mock Redis stream buffer, with secrets stripped by the SELECT.
#[tokio::test]
async fn test_sql_fans_out_to_postgres_and_redis() {
    use broker_connectors::{
        MemoryPgTransport, MemoryRedisTransport, PostgreSqlSink, PostgreSqlSinkConfig,
        RedisCommandKind, RedisSink, RedisSinkConfig,
    };

    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);

    let pg_transport = Arc::new(MemoryPgTransport::new());
    let pg = Arc::new(
        PostgreSqlSink::new(
            PostgreSqlSinkConfig {
                connection_url: "postgresql://u:p@db/db".to_string(),
                sql_template:
                    "INSERT INTO device_status (topic, qos, payload) VALUES ($1, $2, $3::jsonb)"
                        .to_string(),
                pool_size: 1,
                batch_size: 100,
                batch_timeout_ms: 50,
            },
            pg_transport.clone(),
        )
        .expect("valid pg sink"),
    );
    engine.connectors().register("pg-device-state", pg.clone());

    let redis_transport = Arc::new(MemoryRedisTransport::new());
    let redis = Arc::new(
        RedisSink::new(
            RedisSinkConfig {
                endpoint: "redis://127.0.0.1:6379".to_string(),
                command: RedisCommandKind::XAdd {
                    stream_template: "stream:${topic}".to_string(),
                    maxlen: Some(1000),
                },
            },
            redis_transport.clone(),
        )
        .expect("valid redis sink"),
    );
    engine.connectors().register("redis-device-state", redis);

    engine
        .create_rule(
            "persist-status".to_string(),
            TopicFilter::new("devices/+/status").unwrap(),
            Some(
                r#"SELECT status, battery FROM "devices/+/status" WHERE battery > 0 INTO connector("pg-device-state")"#
                    .to_string(),
            ),
            true,
            vec![],
        )
        .expect("pg rule creates");
    engine
        .create_rule(
            "stream-status".to_string(),
            TopicFilter::new("devices/+/status").unwrap(),
            Some(
                r#"SELECT status FROM "devices/+/status" INTO connector("redis-device-state")"#
                    .to_string(),
            ),
            true,
            vec![],
        )
        .expect("redis rule creates");

    let sink: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());
    engine
        .dispatch_ingress(
            &Topic::new("devices/thermostat/status").unwrap(),
            &Bytes::from_static(br#"{ "status": "online", "battery": 87, "secret": "hide_me" }"#),
            QoS::AtMostOnce,
            &sink,
        )
        .await;
    pg.flush().await.expect("pg flush");

    // Postgres received one projected row: secrets stripped, full
    // topic + qos bound as parameters, never interpolated.
    let batches = pg_transport.batches();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].rows.len(), 1);
    assert_eq!(batches[0].rows[0][0], b"devices/thermostat/status");
    assert_eq!(batches[0].rows[0][1], b"0");
    let stored: serde_json::Value =
        serde_json::from_slice(&batches[0].rows[0][2]).expect("stored JSON");
    assert_eq!(
        stored,
        serde_json::json!({ "status": "online", "battery": 87 })
    );
    assert!(!batches[0].sql.contains("thermostat"), "values stay bound");

    // Redis received one XADD with the projected payload.
    let commands = redis_transport.commands();
    assert_eq!(commands.len(), 1);
    let encoded = String::from_utf8_lossy(&commands[0].encode_resp()).to_string();
    assert!(
        encoded.contains("stream:devices/thermostat/status"),
        "stream key"
    );
    assert!(encoded.contains("MAXLEN"), "trim directive");
    assert!(
        encoded.contains(r#""status":"online""#),
        "projected payload"
    );
    assert!(!encoded.contains("hide_me"), "secrets never leave the rule");
}

/// Sprint 25 studio fanout (INDRA-224): one ingress event routes
/// through four streaming SQL `INTO connector(...)` rules and lands
/// simultaneously in the Azure Blob, Tablestore, Confluent and
/// RocketMQ mocks. All transports are in-memory.
// Needs the `azure_blob` and `confluent` heavy SDKs; runs on feature
// builds.
#[cfg(all(feature = "azure_blob", feature = "confluent"))]
#[tokio::test]
async fn test_into_fans_out_to_storage_messaging_sinks() {
    use broker_connectors_enterprise::{
        AttributeColumnMapping, AttributeColumnType, AzureBlobAuth, AzureBlobSink,
        AzureBlobSinkConfig, ConfluentKafkaConfig, ConfluentKafkaSink, ConfluentSecurityProtocol,
        MemoryConfluentTransport, MockAzureBlobTransport, MockRocketMqTransport,
        MockTablestoreTransport, PrimaryKeyMapping, PrimaryKeyType, RocketMqSink,
        RocketMqSinkConfig, SaslMechanism, TablestoreSink, TablestoreSinkConfig,
    };

    let engine = RuleEngine::new(65_536, BackpressurePolicy::DropOldest);

    // Azure Blob (auto-flush every row, SharedKey proof runs).
    let blob_transport = Arc::new(MockAzureBlobTransport::new());
    let blob = Arc::new(
        AzureBlobSink::new(
            AzureBlobSinkConfig {
                account_name: "mydeviceblobs".to_string(),
                container_name: "telemetry".to_string(),
                endpoint: None,
                auth: AzureBlobAuth::SharedKey {
                    account_key:
                        "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=".to_string(),
                },
                blob_path_template:
                    "telemetry/year=${date.year}/month=${date.month}/day=${date.day}/${batch_id}.json"
                        .to_string(),
                compression: broker_connectors_enterprise::AzureBlobCompression::None,
                max_records_per_blob: Some(1),
                max_bytes_per_blob: None,
                flush_interval_secs: 60,
                buffer_capacity: None,
                timeout_ms: None,
            },
            blob_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("blob_store", blob.clone());

    // Tablestore (auto-flush every row).
    let ots_transport = Arc::new(MockTablestoreTransport::new());
    let ots = Arc::new(
        TablestoreSink::new(
            TablestoreSinkConfig {
                endpoint: "https://test-instance.cn-hangzhou.ots.aliyuncs.com".to_string(),
                instance_name: "test-instance".to_string(),
                table_name: "telemetry".to_string(),
                access_key_id: "test-key-id".to_string(),
                access_key_secret: "test-secret".to_string(),
                primary_keys: vec![PrimaryKeyMapping {
                    name: "device_id".to_string(),
                    source: "${client_id}".to_string(),
                    data_type: PrimaryKeyType::String,
                }],
                attribute_columns: vec![AttributeColumnMapping {
                    name: "val".to_string(),
                    source: "${payload.sensor_val}".to_string(),
                    data_type: AttributeColumnType::Double,
                }],
                measurement: "telemetry".to_string(),
                batch_size: Some(1),
                buffer_capacity: None,
                linger_ms: Some(10),
                timeout_ms: None,
            },
            ots_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("ots_store", ots.clone());

    // Confluent Cloud (auto-flush every row, SASL dummies).
    let confluent_transport = Arc::new(MemoryConfluentTransport::new());
    let confluent = Arc::new(
        ConfluentKafkaSink::new(
            ConfluentKafkaConfig {
                bootstrap_servers: vec!["pkc-test.us-east-1.aws.confluent.cloud:9092".to_string()],
                api_key: "confluent-key".to_string(),
                api_secret: "confluent-secret".to_string(),
                auth_mechanism: SaslMechanism::Plain,
                security_protocol: ConfluentSecurityProtocol::SaslSsl,
                topic_template: "telemetry-${topic_segment_2}".to_string(),
                partition_key_template: Some("${client_id}".to_string()),
                schema_registry: None,
                partitions: 12,
                acks: "all".to_string(),
                random_partition: false,
                headers_template: None,
                extra_headers: Vec::new(),
                header_encode_mode:
                    broker_connectors_enterprise::confluent::ConfluentHeaderEncodeMode::None,
                batch_size: Some(1),
                buffer_capacity: None,
                timeout_ms: None,
            },
            confluent_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine
        .connectors()
        .register("cloud_kafka", confluent.clone());

    // RocketMQ (auto-flush every row, anonymous proxy).
    let rmq_transport = Arc::new(MockRocketMqTransport::new());
    let rmq = Arc::new(
        RocketMqSink::new(
            RocketMqSinkConfig {
                endpoints: vec!["127.0.0.1:8081".to_string()],
                topic: "rocket-telemetry".to_string(),
                tag_template: Some("${topic_segment_2}".to_string()),
                keys_template: Some("${client_id}-${message_id}".to_string()),
                strategy: "key_dispatch".to_string(),
                message_group_template: None,
                access_key: None,
                secret_key: None,
                security_token: None,
                batch_size: Some(1),
                buffer_capacity: None,
                linger_ms: Some(10),
                timeout_ms: None,
            },
            rmq_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("rocket_producer", rmq.clone());

    // One INTO rule per studio connector.
    for (id, connector) in [
        ("blob-rule", "blob_store"),
        ("ots-rule", "ots_store"),
        ("confluent-rule", "cloud_kafka"),
        ("rmq-rule", "rocket_producer"),
    ] {
        engine
            .create_rule(
                id.to_string(),
                TopicFilter::new("storage/stream/#").unwrap(),
                Some(format!(
                    r#"SELECT client_id, sensor_val, now_timestamp() AS ts FROM "storage/stream/#" WHERE sensor_val > 100.0 INTO connector("{connector}")"#
                )),
                true,
                vec![],
            )
            .expect("rule creates");
    }

    let sink: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());
    engine
        .dispatch_ingress(
            &Topic::new("storage/stream/line1").unwrap(),
            &Bytes::from_static(br#"{ "client_id": "sensor-42", "sensor_val": 150.0 }"#),
            QoS::AtMostOnce,
            &sink,
        )
        .await;

    // Azure Blob: one object with the projected row.
    let puts = blob_transport.puts();
    assert_eq!(puts.len(), 1);
    assert!(puts[0].path.ends_with("/0.json"), "got {:?}", puts[0].path);
    let row: serde_json::Value =
        serde_json::from_str(String::from_utf8(puts[0].body.clone()).unwrap().trim_end()).unwrap();
    assert_eq!(row["payload"]["client_id"], "sensor-42");
    assert_eq!(row["payload"]["sensor_val"], 150.0);
    assert!(row["payload"]["ts"].as_i64().unwrap_or(0) > 0);

    // Tablestore: one typed row (string PK, double attribute).
    let batches = ots_transport.captured();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].len(), 1);
    assert_eq!(
        batches[0][0].primary_keys[0].1,
        broker_connectors_enterprise::OtsValue::String("sensor-42".to_string())
    );
    assert_eq!(
        batches[0][0].attributes[0].1,
        broker_connectors_enterprise::OtsValue::Double(150.0)
    );

    // Confluent: one record on the templated topic, keyed by client.
    let records = confluent_transport.records_flat();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].topic, "telemetry-stream");
    assert_eq!(records[0].key, Some(Bytes::from_static(b"sensor-42")));
    let value: serde_json::Value = serde_json::from_slice(&records[0].value).unwrap();
    assert_eq!(value["sensor_val"], 150.0);

    // RocketMQ: one envelope with tag + business keys.
    let envelopes = rmq_transport.envelopes();
    assert_eq!(envelopes.len(), 1);
    assert_eq!(envelopes[0].topic, "rocket-telemetry");
    assert_eq!(envelopes[0].messages.len(), 1);
    assert_eq!(
        envelopes[0].messages[0].system_properties.tag.as_deref(),
        Some("stream")
    );
    assert_eq!(
        envelopes[0].messages[0].system_properties.keys,
        vec!["sensor-42-0".to_string()]
    );

    // Below-threshold telemetry fires nothing anywhere.
    engine
        .dispatch_ingress(
            &Topic::new("storage/stream/line1").unwrap(),
            &Bytes::from_static(br#"{ "client_id": "sensor-42", "sensor_val": 50.0 }"#),
            QoS::AtMostOnce,
            &sink,
        )
        .await;
    assert_eq!(blob_transport.puts().len(), 1);
    assert_eq!(ots_transport.captured().len(), 1);
    assert_eq!(confluent_transport.records_flat().len(), 1);
    assert_eq!(rmq_transport.envelopes().len(), 1);
}

/// Azure Blob REST write via the broker: one ingress publish
/// routes through `ForwardConnector` into `AzureBlobSink` on
/// `HttpAzureBlobTransport`, whose idempotent container create plus
/// block-blob upload land on a local HTTP fake. Every request pins
/// x-ms-version 2021-08-06.
/// X1-09: needs the `azure_blob` feature (absent by default).
#[cfg(feature = "azure_blob")]
#[tokio::test]
async fn test_azure_blob_rest_write_via_dispatch_against_loopback() {
    use broker_connectors::Sink as _;
    use broker_connectors_enterprise::{
        AzureBlobAuth, AzureBlobSink, AzureBlobSinkConfig, HttpAzureBlobTransport,
        AZURE_STORAGE_VERSION,
    };

    #[derive(Debug, Default)]
    struct Captured {
        inner: parking_lot::Mutex<Vec<CapturedPut>>,
    }
    #[derive(Debug)]
    struct CapturedPut {
        path: String,
        query: Option<String>,
        version: Option<String>,
        auth: Option<String>,
    }

    let captured = Arc::new(Captured::default());
    let app = axum::Router::new().fallback(axum::routing::put({
        let captured = captured.clone();
        move |uri: axum::http::Uri, headers: axum::http::HeaderMap, _body: Bytes| {
            let captured = captured.clone();
            async move {
                captured.inner.lock().push(CapturedPut {
                    path: uri.path().to_string(),
                    query: uri.query().map(str::to_string),
                    version: headers
                        .get("x-ms-version")
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string),
                    auth: headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string),
                });
                (
                    axum::http::StatusCode::CREATED,
                    [
                        ("x-ms-request-id", "00000000-0000-0000-0000-000000000000"),
                        ("x-ms-version", AZURE_STORAGE_VERSION),
                        ("ETag", "\"0x8DD000000000000\""),
                        ("Last-Modified", "Tue, 22 Sep 2026 20:00:00 GMT"),
                        ("Date", "Tue, 22 Sep 2026 20:00:00 GMT"),
                        ("x-ms-request-server-encrypted", "false"),
                    ],
                    "",
                )
            }
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let engine = RuleEngine::new(65_536, BackpressurePolicy::DropOldest);
    let config = AzureBlobSinkConfig {
        account_name: "mydeviceblobs".to_string(),
        container_name: "telemetry".to_string(),
        endpoint: Some(format!("http://127.0.0.1:{port}/devstoreaccount1")),
        auth: AzureBlobAuth::SharedKey {
            account_key: "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=".to_string(),
        },
        blob_path_template: "telemetry/${batch_id}.json".to_string(),
        compression: broker_connectors_enterprise::AzureBlobCompression::None,
        max_records_per_blob: Some(1),
        max_bytes_per_blob: None,
        flush_interval_secs: 60,
        buffer_capacity: None,
        timeout_ms: Some(5_000),
    };
    let transport = Arc::new(
        HttpAzureBlobTransport::new(&config, reqwest::Client::new())
            .expect("rest builds with loopback"),
    );
    let sink = Arc::new(AzureBlobSink::new(config, transport).expect("valid sink"));
    engine.connectors().register("rest_blob", sink.clone());
    engine
        .create_rule(
            "rest-blob-rule".to_string(),
            TopicFilter::new("sdk/blob/#").unwrap(),
            Some(r#"SELECT client_id FROM "sdk/blob/#" INTO connector("rest_blob")"#.to_string()),
            true,
            vec![],
        )
        .expect("rule creates");

    let broker_sink: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());
    engine
        .dispatch_ingress(
            &Topic::new("sdk/blob/line1").unwrap(),
            &Bytes::from_static(br#"{ "client_id": "sensor-42" }"#),
            QoS::AtMostOnce,
            &broker_sink,
        )
        .await;
    assert_eq!(sink.sent_records(), 1);
    assert_eq!(sink.kind(), "azure_blob");

    let puts = captured.inner.lock();
    // One flush = idempotent container create + one blob upload.
    assert_eq!(puts.len(), 2, "got {puts:?}");
    assert!(
        puts.iter().any(|p| p
            .query
            .as_deref()
            .unwrap_or("")
            .contains("restype=container")),
        "container create missing: {puts:?}"
    );
    for put in puts.iter() {
        assert_eq!(put.version.as_deref(), Some(AZURE_STORAGE_VERSION));
        assert!(put.path.contains("telemetry"), "got {put:?}");
        assert!(
            put.auth
                .as_deref()
                .unwrap_or("")
                .starts_with("SharedKey mydeviceblobs:"),
            "SharedKey missing: {put:?}"
        );
    }
    server.abort();
}

/// Azure Event Hubs REST write via the broker: one ingress publish
/// routes through `ForwardConnector` into `AzureEventHubsSink` on
/// `HttpAzureEventHubsTransport`, whose signed `POST {hub}/messages`
/// lands on a local HTTP fake. The SAS header carries the key name
/// (never a pre-formed token in config), so the sink renews it from
/// the stored key on every attempt.
/// X1-09: needs the `azure_eventhubs` feature (absent by default).
#[cfg(feature = "azure_eventhubs")]
#[tokio::test]
async fn test_azure_eventhubs_rest_write_via_dispatch_against_loopback() {
    use broker_connectors::Sink as _;
    use broker_connectors_enterprise::{
        AzureEventHubsSink, AzureEventHubsSinkConfig, HttpAzureEventHubsTransport,
    };

    #[derive(Debug, Default)]
    struct Captured {
        inner: parking_lot::Mutex<Vec<CapturedPost>>,
    }
    #[derive(Debug)]
    struct CapturedPost {
        path: String,
        auth: Option<String>,
        body: Vec<u8>,
    }

    let captured = Arc::new(Captured::default());
    let app = axum::Router::new().fallback(axum::routing::post({
        let captured = captured.clone();
        move |uri: axum::http::Uri, headers: axum::http::HeaderMap, body: Bytes| {
            let captured = captured.clone();
            async move {
                captured.inner.lock().push(CapturedPost {
                    path: uri.path().to_string(),
                    auth: headers
                        .get("authorization")
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string),
                    body: body.to_vec(),
                });
                axum::http::StatusCode::CREATED
            }
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let engine = RuleEngine::new(65_536, BackpressurePolicy::DropOldest);
    let config = AzureEventHubsSinkConfig {
        namespace: "my-eventhub-ns".to_string(),
        event_hub: "telemetry-hub".to_string(),
        endpoint: Some(format!("http://127.0.0.1:{port}")),
        shared_access_key_name: "SendPolicy".to_string(),
        shared_access_key: "dGVzdC1rZXktbWF0ZXJpYWwtMzItYnl0ZXMhIU9L".to_string(),
        partition_key_template: Some("${client_id}".to_string()),
        user_properties: std::collections::HashMap::new(),
        token_ttl_secs: 3_600,
        batch_size: Some(1),
        batch_bytes: Some(1_048_576),
        linger_ms: Some(20),
        max_retries: Some(2),
        initial_backoff_ms: Some(1),
        max_backoff_ms: Some(2),
        timeout_ms: Some(5_000),
    };
    let transport = Arc::new(
        HttpAzureEventHubsTransport::new(&config, reqwest::Client::new())
            .expect("rest builds offline"),
    );
    let sink = Arc::new(AzureEventHubsSink::new(config, transport).expect("valid sink"));
    engine.connectors().register("rest_eventhubs", sink.clone());
    engine
        .create_rule(
            "rest-eventhubs-rule".to_string(),
            TopicFilter::new("sdk/eh/#").unwrap(),
            Some(
                r#"SELECT client_id FROM "sdk/eh/#" INTO connector("rest_eventhubs")"#.to_string(),
            ),
            true,
            vec![],
        )
        .expect("rule creates");

    let broker_sink: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());
    engine
        .dispatch_ingress(
            &Topic::new("sdk/eh/line1").unwrap(),
            &Bytes::from_static(br#"{ "client_id": "sensor-42" }"#),
            QoS::AtMostOnce,
            &broker_sink,
        )
        .await;
    // The publish travelled the broker into the sink and flushed
    // (batch of 1): one signed POST landed on the fake.
    assert_eq!(sink.kind(), "azure_eventhubs");
    assert_eq!(sink.sent_records(), 1);
    assert_eq!(sink.buffered_rows(), 0);

    let posts = captured.inner.lock();
    assert_eq!(posts.len(), 1, "got {posts:?}");
    assert_eq!(posts[0].path, "/telemetry-hub/messages");
    let auth = posts[0].auth.as_deref().expect("SAS header");
    assert!(auth.starts_with("SharedAccessSignature sr="));
    assert!(auth.contains("&skn=SendPolicy"));
    let doc: serde_json::Value =
        serde_json::from_str(std::str::from_utf8(&posts[0].body).unwrap()).unwrap();
    assert_eq!(doc[0]["BrokerProperties"]["PartitionKey"], "sensor-42");
    server.abort();
}

/// Sprint 26 studio fanout (INDRA-225): one ingress event routes
/// through six streaming SQL `INTO connector(...)` rules and lands
/// simultaneously in the Oracle, CockroachDB, AlloyDB, OpenTSDB,
/// GreptimeDB, and Datalayers mocks. All transports are in-memory.
#[tokio::test]
async fn test_into_fans_out_to_databases_and_timeseries_sinks() {
    use broker_connectors::{
        GreptimeDbConfig, GreptimeDbSink, GreptimeFormat, GreptimePrecision,
        MockGreptimeDbTransport, MockOpenTsdbTransport, OpenTsdbCompression, OpenTsdbConfig,
        OpenTsdbProtocol, OpenTsdbSink,
    };
    use broker_connectors_enterprise::{
        AlloydbAuth, AlloydbColumnMapping, AlloydbConfig, AlloydbSink, CockroachDbConfig,
        CockroachDbSink, DatalayersConfig, DatalayersSink, MockAlloydbTransport,
        MockCockroachDbTransport, MockDatalayersTransport, MockOracleTransport, OracleSink,
        OracleSinkConfig,
    };

    let engine = RuleEngine::new(65_536, BackpressurePolicy::DropOldest);

    // 1. Oracle (MERGE INTO atomic upsert)
    let oracle_transport = Arc::new(MockOracleTransport::new());
    let oracle = Arc::new(
        OracleSink::new(
            OracleSinkConfig {
                url: "https://oracle-host:8080/ords/admin/_/sql".to_string(),
                schema: "ADMIN".to_string(),
                table: "telemetry".to_string(),
                username: "admin".to_string(),
                password: "secret".to_string(),
                custom_upsert: None,
                key_columns: vec!["device_id".to_string()],
                batch_size: Some(1),
                buffer_capacity: None,
                timeout_ms: None,
            },
            oracle_transport.clone(),
        )
        .expect("valid oracle sink"),
    );
    engine.connectors().register("oracle_sink", oracle.clone());

    // 2. CockroachDB (multi-row UPSERT)
    let cockroach_transport = Arc::new(MockCockroachDbTransport::new());
    let cockroach = Arc::new(
        CockroachDbSink::new(
            CockroachDbConfig {
                connection_string: "postgresql://root@127.0.0.1:26257/defaultdb".to_string(),
                table: "telemetry".to_string(),
                upsert_conflict_columns: vec!["device_id".to_string()],
                batch_size: Some(1),
                max_retry_attempts: 5,
                buffer_capacity: None,
                timeout_ms: None,
            },
            cockroach_transport.clone(),
        )
        .expect("valid cockroach sink"),
    );
    engine
        .connectors()
        .register("cockroach_sink", cockroach.clone());

    // 3. AlloyDB (accelerated multi-row INSERT)
    let alloydb_transport = Arc::new(MockAlloydbTransport::new());
    let alloydb = Arc::new(
        AlloydbSink::new(
            AlloydbConfig {
                host: "10.0.0.1".to_string(),
                port: 5432,
                database: "telemetry".to_string(),
                username: "postgres".to_string(),
                auth: AlloydbAuth::Password {
                    password: "secret".to_string(),
                },
                table: "readings".to_string(),
                column_mappings: vec![
                    AlloydbColumnMapping {
                        db_column: "device_id".to_string(),
                        source_field: "device_id".to_string(),
                        data_type: None,
                    },
                    AlloydbColumnMapping {
                        db_column: "temp".to_string(),
                        source_field: "temp".to_string(),
                        data_type: None,
                    },
                ],
                batch_size: Some(1),
                buffer_capacity: None,
                timeout_ms: None,
                tls: None,
                ca_bundle_pem: None,
                tls_ca_file: None,
            },
            alloydb_transport.clone(),
        )
        .expect("valid alloydb sink"),
    );
    engine
        .connectors()
        .register("alloydb_sink", alloydb.clone());

    // 4. OpenTSDB (HTTP PUT summary)
    let opentsdb_transport = Arc::new(MockOpenTsdbTransport::new());
    let mut tags = std::collections::HashMap::new();
    tags.insert("device".to_string(), "${payload.device_id}".to_string());
    let opentsdb = Arc::new(
        OpenTsdbSink::new(
            OpenTsdbConfig {
                endpoint: "http://127.0.0.1:4242".to_string(),
                protocol: OpenTsdbProtocol::Http,
                metric_template: "factory.temp".to_string(),
                tag_mappings: tags,
                value_field: "temp".to_string(),
                summary: true,
                compression: OpenTsdbCompression::None,
                batch_size: Some(1),
                buffer_capacity: None,
                timeout_ms: None,
            },
            opentsdb_transport.clone(),
        )
        .expect("valid opentsdb sink"),
    );
    engine
        .connectors()
        .register("opentsdb_sink", opentsdb.clone());

    // 5. GreptimeDB (SQL INSERT / Influx Line Protocol)
    let greptimedb_transport = Arc::new(MockGreptimeDbTransport::new());
    let greptimedb = Arc::new(
        GreptimeDbSink::new(
            GreptimeDbConfig {
                endpoint: "http://127.0.0.1:4000".to_string(),
                database: "public".to_string(),
                auth: None,
                format: GreptimeFormat::SqlInsert,
                table_template: "sensor_readings".to_string(),
                timestamp_precision: GreptimePrecision::Millisecond,
                batch_size: Some(1),
                buffer_capacity: None,
                timeout_ms: None,
            },
            greptimedb_transport.clone(),
        )
        .expect("valid greptimedb sink"),
    );
    engine
        .connectors()
        .register("greptimedb_sink", greptimedb.clone());

    // 6. Datalayers (industrial time-series write)
    let datalayers_transport = Arc::new(MockDatalayersTransport::new());
    let datalayers = Arc::new(
        DatalayersSink::new(
            DatalayersConfig {
                endpoint: "http://127.0.0.1:8360".to_string(),
                database: "telemetry".to_string(),
                table: "metrics".to_string(),
                auth_token: Some("secret-bearer-token".to_string()),
                username: None,
                password: None,
                timestamp_field: None,
                tag_columns: vec!["device_id".to_string()],
                field_columns: vec!["temp".to_string()],
                precision: None,
                batch_size: Some(1),
                buffer_capacity: None,
                timeout_ms: None,
            },
            datalayers_transport.clone(),
        )
        .expect("valid datalayers sink"),
    );
    engine
        .connectors()
        .register("datalayers_sink", datalayers.clone());

    // Six INTO rules routing to the six database & TS sinks
    for (id, connector) in [
        ("oracle-rule", "oracle_sink"),
        ("cockroach-rule", "cockroach_sink"),
        ("alloydb-rule", "alloydb_sink"),
        ("opentsdb-rule", "opentsdb_sink"),
        ("greptimedb-rule", "greptimedb_sink"),
        ("datalayers-rule", "datalayers_sink"),
    ] {
        engine
            .create_rule(
                id.to_string(),
                TopicFilter::new("factory/+/telemetry").unwrap(),
                Some(format!(
                    r#"SELECT device_id, temp FROM "factory/+/telemetry" WHERE temp > 50.0 INTO connector("{connector}")"#
                )),
                true,
                vec![],
            )
            .expect("rule creates");
    }

    let sink: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());

    // Ingress above threshold: temp = 68.5 > 50.0
    engine
        .dispatch_ingress(
            &Topic::new("factory/line1/telemetry").unwrap(),
            &Bytes::from_static(br#"{ "device_id": "sensor-01", "temp": 68.5 }"#),
            QoS::AtMostOnce,
            &sink,
        )
        .await;

    // Verify executions inside isolated block so all MutexGuards drop before next await
    {
        let ora_execs = oracle_transport.executions.lock();
        assert_eq!(ora_execs.len(), 1);
        assert!(ora_execs[0].statement.contains("MERGE INTO telemetry"));

        let crdb_execs = cockroach_transport.executions.lock();
        assert_eq!(crdb_execs.len(), 1);
        assert!(
            crdb_execs[0].query.contains("INTO telemetry")
                && crdb_execs[0]
                    .query
                    .contains("ON CONFLICT (device_id) DO UPDATE SET")
        );

        let alloy_execs = alloydb_transport.executions.lock();
        assert_eq!(alloy_execs.len(), 1);
        assert!(alloy_execs[0].query.contains("INSERT INTO readings"));

        let opentsdb_pts = opentsdb_transport.captured_points.lock();
        assert_eq!(opentsdb_pts.len(), 1);
        assert_eq!(opentsdb_pts[0].metric, "factory.temp");
        assert_eq!(opentsdb_pts[0].value, 68.5);

        let grep_sqls = greptimedb_transport.captured_sqls.lock();
        assert_eq!(grep_sqls.len(), 1);
        assert!(grep_sqls[0].contains("INSERT INTO sensor_readings"));

        let dl_reqs = datalayers_transport.captured_requests.lock();
        assert_eq!(dl_reqs.len(), 1);
        assert_eq!(dl_reqs[0].table, "metrics");
        assert_eq!(dl_reqs[0].records.len(), 1);
        assert_eq!(
            dl_reqs[0].records[0].tags.get("device_id").unwrap(),
            "sensor-01"
        );
    }

    // Ingress below threshold: temp = 32.0 <= 50.0 -> no actions should fire
    engine
        .dispatch_ingress(
            &Topic::new("factory/line1/telemetry").unwrap(),
            &Bytes::from_static(br#"{ "device_id": "sensor-01", "temp": 32.0 }"#),
            QoS::AtMostOnce,
            &sink,
        )
        .await;

    assert_eq!(oracle_transport.executions.lock().len(), 1);
    assert_eq!(cockroach_transport.executions.lock().len(), 1);
    assert_eq!(alloydb_transport.executions.lock().len(), 1);
    assert_eq!(opentsdb_transport.captured_points.lock().len(), 1);
    assert_eq!(greptimedb_transport.captured_sqls.lock().len(), 1);
    assert_eq!(datalayers_transport.captured_requests.lock().len(), 1);
}

// ------------------------------------------------------------------
// R22: forward failures counted in action metrics, throttled log.
// ------------------------------------------------------------------
