#[allow(unused_imports)]
use super::*;

/// Cassandra keyspace e2e (MN-05 item 7): one ingress event
/// routes through a streaming SQL `INTO connector(...)` rule into
/// a real `CassandraSink`. The sink qualifies the bare table
/// with the configured keyspace, so the statement that reaches
/// the transport carries `keyspace.table`. The transport is
/// in-memory.
// Needs the `cassandra` heavy SDK (X1-10).
#[cfg(feature = "cassandra")]
#[tokio::test]
async fn test_into_cassandra_carries_keyspace_table() {
    use broker_connectors_enterprise::{
        CassandraSink, CassandraSinkConfig, CqlConsistency, MockCassandraTransport,
    };

    let engine = RuleEngine::new(65_536, BackpressurePolicy::DropOldest);

    // Cassandra statements (auto-flush every row). The template
    // names the bare table; the sink qualifies it on the path.
    let transport = Arc::new(MockCassandraTransport::new());
    let sink = Arc::new(
        CassandraSink::new(
            CassandraSinkConfig {
                contact_points: vec!["10.0.0.1:9042".to_string()],
                keyspace: "telemetry".to_string(),
                table_template: "events".to_string(),
                auth: broker_connectors_enterprise::CassandraAuth::None,
                consistency: CqlConsistency::LocalQuorum,
                partition_key_template: "${client_id}".to_string(),
                cql_statement_template: "INSERT INTO events (device_id, bucket_hour, event_time, payload) VALUES (?, ?, ?, ?)".to_string(),
                ttl_secs: None,
                batch_size: Some(1),
                batch_bytes: None,
                linger_ms: Some(10),
                max_retries: Some(4),
                initial_backoff_ms: Some(1),
                max_backoff_ms: Some(2),
                timeout_ms: None,
            },
            transport.clone(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("cassandra-sink", sink.clone());

    engine
        .create_rule(
            "cassandra-rule".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            Some(
                r#"SELECT client_id, device_id, temp FROM "sensors/+" WHERE temp > 20.0 INTO connector("cassandra-sink")"#.to_string(),
            ),
            true,
            vec![],
        )
        .expect("rule creates");

    let sink: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());
    engine
        .dispatch_ingress(
            &Topic::new("sensors/kitchen").unwrap(),
            &Bytes::from_static(
                br#"{ "client_id": "device-42", "device_id": "device-42", "temp": 22.5 }"#,
            ),
            QoS::AtMostOnce,
            &sink,
        )
        .await;

    // The row lands with the qualified table on the deliver path.
    let captured = transport.captured();
    assert_eq!(captured.len(), 1, "one batch reaches the transport");
    assert_eq!(captured[0].keyspace, "telemetry");
    assert_eq!(captured[0].batch.len(), 1);
    assert_eq!(
        captured[0].batch[0].cql,
        "INSERT INTO telemetry.events (device_id, bucket_hour, event_time, payload) VALUES (?, ?, ?, ?)",
        "the CQL carries keyspace.table"
    );
    assert_eq!(captured[0].batch[0].values[0], b"device-42");
}

/// Multi-store e2e (INDRA-220): one ingress event routes through
/// streaming SQL `INTO connector(...)` rules and fans out
/// simultaneously to the MongoDB, MSSQL, Cassandra and Couchbase
/// mocks with zero drops. All transports are in-memory.
// Needs the `mongodb` + `cassandra` + `couchbase` heavy SDKs (X1-10).
#[cfg(all(feature = "mongodb", feature = "cassandra", feature = "couchbase"))]
#[tokio::test]
async fn test_into_fans_out_to_database_sinks() {
    use broker_connectors_enterprise::{
        CassandraSink, CassandraSinkConfig, CouchbaseSink, CouchbaseSinkConfig,
        MockCassandraTransport, MockCouchbaseTransport, MockMongoDbTransport, MockMssqlTransport,
        MongoDbSink, MongoDbSinkConfig, MssqlSink, MssqlSinkConfig,
    };

    let engine = RuleEngine::new(65_536, BackpressurePolicy::DropOldest);

    // MongoDB documents (auto-flush every row).
    let mongo_transport = Arc::new(MockMongoDbTransport::new());
    let mongo = Arc::new(
        MongoDbSink::new(
            MongoDbSinkConfig {
                connection_string: "mongodb://u:p@unused:27017".to_string(),
                database: "telemetry".to_string(),
                collection_template: "readings".to_string(),
                payload_template: None,
                operation: broker_connectors_enterprise::MongoOperation::InsertOne,
                batch_size: Some(1),
                batch_bytes: None,
                linger_ms: Some(20),
                max_retries: Some(4),
                initial_backoff_ms: Some(1),
                max_backoff_ms: Some(2),
                timeout_ms: None,
            },
            mongo_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("mongodb-sink", mongo.clone());

    // MSSQL rows (auto-flush every row).
    let mssql_transport = Arc::new(MockMssqlTransport::new());
    let mssql = Arc::new(
        MssqlSink::new(
            MssqlSinkConfig {
                host: "unused".to_string(),
                port: None,
                database: "telemetry".to_string(),
                table_template: "dbo.SensorEvents".to_string(),
                auth: broker_connectors_enterprise::MssqlAuth::Integrated,
                query_mode: broker_connectors_enterprise::MssqlQueryMode::InsertJson,
                trust_server_certificate: true,
                batch_size: Some(1),
                batch_bytes: None,
                linger_ms: Some(20),
                max_retries: Some(3),
                initial_backoff_ms: Some(1),
                max_backoff_ms: Some(2),
                timeout_ms: None,
            },
            mssql_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("mssql-sink", mssql.clone());

    // Cassandra statements (auto-flush every row).
    let cassandra_transport = Arc::new(MockCassandraTransport::new());
    let cassandra = Arc::new(
        CassandraSink::new(
            CassandraSinkConfig {
                contact_points: vec!["10.0.0.1:9042".to_string()],
                keyspace: "telemetry".to_string(),
                table_template: "events".to_string(),
                auth: broker_connectors_enterprise::CassandraAuth::None,
                consistency: broker_connectors_enterprise::CqlConsistency::LocalQuorum,
                partition_key_template: "${client_id}".to_string(),
                cql_statement_template: "INSERT INTO telemetry.events (device_id, bucket_hour, event_time, payload) VALUES (?, ?, ?, ?)".to_string(),
                ttl_secs: None,
                batch_size: Some(1),
                batch_bytes: None,
                linger_ms: Some(10),
                max_retries: Some(4),
                initial_backoff_ms: Some(1),
                max_backoff_ms: Some(2),
                timeout_ms: None,
            },
            cassandra_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine
        .connectors()
        .register("cassandra-sink", cassandra.clone());

    // Couchbase documents (auto-flush every row).
    let couchbase_transport = Arc::new(MockCouchbaseTransport::new());
    couchbase_transport.set_operation_tag(0);
    let couchbase = Arc::new(
        CouchbaseSink::new(
            CouchbaseSinkConfig {
                connection_string: "couchbase://unused".to_string(),
                bucket: "telemetry".to_string(),
                scope: None,
                collection: None,
                auth: broker_connectors_enterprise::CouchbaseAuth {
                    username: "Administrator".to_string(),
                    password: "secret".to_string(),
                },
                doc_id_template: "${client_id}::${timestamp}".to_string(),
                sql_template: None,
                operation: broker_connectors_enterprise::CouchbaseOperation::Upsert,
                expiry_secs: None,
                batch_size: Some(1),
                batch_bytes: None,
                linger_ms: Some(10),
                max_retries: Some(3),
                initial_backoff_ms: Some(1),
                max_backoff_ms: Some(2),
                timeout_ms: None,
            },
            couchbase_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine
        .connectors()
        .register("couchbase-sink", couchbase.clone());

    // One INTO rule per store.
    for (id, connector) in [
        ("mongo-rule", "mongodb-sink"),
        ("mssql-rule", "mssql-sink"),
        ("cassandra-rule", "cassandra-sink"),
        ("couchbase-rule", "couchbase-sink"),
    ] {
        engine
            .create_rule(
                id.to_string(),
                TopicFilter::new("sensors/+").unwrap(),
                Some(format!(
                    r#"SELECT client_id, device_id, temp FROM "sensors/+" WHERE temp > 20.0 INTO connector("{connector}")"#
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
            &Bytes::from_static(
                br#"{ "client_id": "device-42", "device_id": "device-42", "temp": 22.5 }"#,
            ),
            QoS::AtMostOnce,
            &sink,
        )
        .await;

    let projected =
        serde_json::json!({"client_id": "device-42", "device_id": "device-42", "temp": 22.5});

    // MongoDB: one document with _mqtt metadata in `readings`.
    let captured = mongo_transport.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].db, "telemetry");
    assert_eq!(captured[0].collection, "readings");
    assert_eq!(captured[0].docs.len(), 1);
    let document = &captured[0].docs[0].document;
    assert!(matches!(
        document.get("_id"),
        Some(broker_connectors_enterprise::BsonValue::ObjectId(_))
    ));
    assert_eq!(
        document.get("temp"),
        Some(&broker_connectors_enterprise::BsonValue::Double(22.5))
    );
    assert_eq!(
        document.get("_mqtt").and_then(|meta| match meta {
            broker_connectors_enterprise::BsonValue::Document(meta) => meta.get("topic").cloned(),
            _ => None,
        }),
        Some(broker_connectors_enterprise::BsonValue::String(
            "sensors/kitchen".to_string()
        ))
    );

    // MSSQL: one typed row in dbo.SensorEvents.
    let captured = mssql_transport.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].table, "dbo.SensorEvents");
    assert_eq!(captured[0].rows.len(), 1);
    assert_eq!(captured[0].rows[0].topic, "sensors/kitchen");
    assert_eq!(captured[0].rows[0].client_id, "device-42");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&captured[0].rows[0].payload_json).unwrap(),
        projected
    );

    // Cassandra: one bound statement keyed by client_id.
    let captured = cassandra_transport.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].keyspace, "telemetry");
    assert_eq!(captured[0].batch.len(), 1);
    assert_eq!(captured[0].batch[0].values[0], b"device-42");
    assert_eq!(
        captured[0].batch[0].partition_token,
        broker_connectors_enterprise::murmur3_token(b"device-42")
    );

    // Couchbase: one document under client_id::timestamp.
    let captured = couchbase_transport.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].bucket, "telemetry");
    assert_eq!(captured[0].scope, "_default");
    assert_eq!(captured[0].collection, "_default");
    assert_eq!(captured[0].items.len(), 1);
    assert!(captured[0].items[0].key.starts_with("device-42::"));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&captured[0].items[0].body).unwrap()["temp"],
        serde_json::json!(22.5)
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
    assert_eq!(mongo_transport.captured().len(), 1);
    assert_eq!(mssql_transport.captured().len(), 1);
    assert_eq!(cassandra_transport.captured().len(), 1);
    assert_eq!(couchbase_transport.captured().len(), 1);
}

/// Time-series e2e (INDRA-221): one ingress event routes through
/// streaming SQL `INTO connector(...)` rules and fans out
/// simultaneously to the TDengine, IoTDB, Timestream and DynamoDB
/// mocks with zero drops. All transports are in-memory.
/// X1-09: needs the `dynamodb` feature (absent by default).
#[cfg(feature = "dynamodb")]
#[tokio::test]
async fn test_into_fans_out_to_timeseries_sinks() {
    use broker_connectors_enterprise::{
        DynamoDbSink, DynamoDbSinkConfig, IotDbSink, IotDbSinkConfig, MockDynamoDbTransport,
        MockIotDbTransport, MockTdengineTransport, MockTimestreamTransport, TdengineSink,
        TdengineSinkConfig, TimestreamSink, TimestreamSinkConfig,
    };
    use std::collections::HashMap;

    let engine = RuleEngine::new(65_536, BackpressurePolicy::DropOldest);

    // TDengine super-table rows (auto-flush every row).
    let tdengine_transport = Arc::new(MockTdengineTransport::new());
    let tdengine = Arc::new(
        TdengineSink::new(
            TdengineSinkConfig {
                endpoint: "http://127.0.0.1:6041/rest/sql".to_string(),
                database: "power".to_string(),
                stable_name: "meters".to_string(),
                subtable_template: "d_${client_id}".to_string(),
                auth: broker_connectors_enterprise::TdengineAuth::Basic {
                    username: "root".to_string(),
                    password: "taosdata".to_string(),
                },
                tags_template: HashMap::new(),
                metrics_template: HashMap::from([
                    (
                        "temperature".to_string(),
                        "${payload.temperature}".to_string(),
                    ),
                    ("humidity".to_string(), "${payload.humidity}".to_string()),
                ]),
                batch_size: Some(1),
                batch_bytes: None,
                linger_ms: Some(20),
                buffer_capacity: None,
                max_retries: Some(4),
                initial_backoff_ms: Some(1),
                max_backoff_ms: Some(2),
                timeout_ms: None,
            },
            tdengine_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine
        .connectors()
        .register("tdengine-sink", tdengine.clone());

    // IoTDB tablet rows (auto-flush every row).
    let iotdb_transport = Arc::new(MockIotDbTransport::new());
    let iotdb = Arc::new(
        IotDbSink::new(
            IotDbSinkConfig {
                endpoint: "http://127.0.0.1:18080/rest/v2".to_string(),
                device_path_template: "root.factory.${payload.plant_id}.${client_id}".to_string(),
                auth: broker_connectors_enterprise::IotDbAuth {
                    username: "root".to_string(),
                    password: "root".to_string(),
                },
                is_aligned: false,
                measurements: vec!["temperature".to_string(), "humidity".to_string()],
                data_types: vec![
                    broker_connectors_enterprise::IotDbDataType::Double,
                    broker_connectors_enterprise::IotDbDataType::Double,
                ],
                batch_size: Some(1),
                batch_bytes: None,
                linger_ms: Some(10),
                max_retries: Some(3),
                initial_backoff_ms: Some(1),
                max_backoff_ms: Some(2),
                timeout_ms: None,
            },
            iotdb_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("iotdb-sink", iotdb.clone());

    // Timestream multi-measure records (auto-flush every row).
    let timestream_transport = Arc::new(MockTimestreamTransport::new());
    let timestream = Arc::new(
        TimestreamSink::new(
            TimestreamSinkConfig {
                database_name: "iot_database".to_string(),
                table_name: "telemetry".to_string(),
                region: "us-east-1".to_string(),
                endpoint: None,
                access_key_id: "AKID".to_string(),
                secret_access_key: "secret".to_string(),
                session_token: None,
                dimensions: HashMap::from([("device_id".to_string(), "${client_id}".to_string())]),
                time_unit: broker_connectors_enterprise::TimestreamTimeUnit::Milliseconds,
                measure_name_template: Some("sensor_metrics".to_string()),
                multi_measure_mappings: HashMap::from([
                    ("temperature".to_string(), "DOUBLE".to_string()),
                    ("humidity".to_string(), "DOUBLE".to_string()),
                ]),
                batch_size: Some(1),
                batch_bytes: None,
                linger_ms: Some(20),
                max_retries: Some(4),
                initial_backoff_ms: Some(1),
                max_backoff_ms: Some(2),
                timeout_ms: None,
            },
            timestream_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine
        .connectors()
        .register("timestream-sink", timestream.clone());

    // DynamoDB items (auto-flush every row).
    let dynamodb_transport = Arc::new(MockDynamoDbTransport::new());
    let dynamodb = Arc::new(
        DynamoDbSink::new(
            DynamoDbSinkConfig {
                table_name: "telemetry_table".to_string(),
                region: "us-east-1".to_string(),
                endpoint: None,
                access_key_id: "AKID".to_string(),
                secret_access_key: "secret".to_string(),
                session_token: None,
                partition_key: broker_connectors_enterprise::DynamoKeyConfig {
                    name: "device_id".to_string(),
                    template: "${client_id}".to_string(),
                    key_type: "S".to_string(),
                },
                sort_key: None,
                ttl_attribute: None,
                ttl_secs: None,
                attributes_mapping: HashMap::from([(
                    "temperature".to_string(),
                    "${payload.temperature}".to_string(),
                )]),
                undefined_vars_as_null: false,
                batch_size: Some(1),
                batch_bytes: None,
                linger_ms: Some(10),
                max_retries: Some(4),
                initial_backoff_ms: Some(1),
                max_backoff_ms: Some(2),
                timeout_ms: None,
            },
            dynamodb_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine
        .connectors()
        .register("dynamodb-sink", dynamodb.clone());

    // One INTO rule per store over the same telemetry stream.
    for (id, connector) in [
        ("tdengine-rule", "tdengine-sink"),
        ("iotdb-rule", "iotdb-sink"),
        ("timestream-rule", "timestream-sink"),
        ("dynamodb-rule", "dynamodb-sink"),
    ] {
        engine
            .create_rule(
                id.to_string(),
                TopicFilter::new("sensors/+").unwrap(),
                Some(format!(
                    r#"SELECT client_id, plant_id, temperature, humidity FROM "sensors/+" WHERE temperature > 20.0 INTO connector("{connector}")"#
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
            &Bytes::from_static(
                br#"{ "client_id": "sensor101", "plant_id": "plant1", "temperature": 24.5, "humidity": 61.2 }"#,
            ),
            QoS::AtMostOnce,
            &sink,
        )
        .await;

    // TDengine: one INSERT with the sub-table + measures.
    let captured = tdengine_transport.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].database, "power");
    assert!(captured[0]
        .sql
        .starts_with("INSERT INTO d_sensor101 USING meters TAGS ()"));
    assert!(captured[0].sql.contains("61.2, 24.5"));

    // IoTDB: one tablet on the hierarchical device path.
    let captured = iotdb_transport.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].tablet.device, "root.factory.plant1.sensor101");
    assert_eq!(captured[0].tablet.values.len(), 1);
    assert_eq!(captured[0].tablet.values[0][0], serde_json::json!(24.5));
    assert_eq!(captured[0].tablet.values[0][1], serde_json::json!(61.2));

    // Timestream: one multi-measure record with dimensions.
    let captured = timestream_transport.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].database, "iot_database");
    assert_eq!(captured[0].records.len(), 1);
    assert_eq!(captured[0].records[0].measure_name, "sensor_metrics");
    assert!(captured[0].records[0]
        .dimensions
        .iter()
        .any(|(name, value)| name == "device_id" && value == "sensor101"));
    assert!(captured[0].records[0]
        .measures
        .iter()
        .any(|(name, value, measure_type)| name == "temperature"
            && value == "24.5"
            && measure_type == "DOUBLE"));

    // DynamoDB: one item keyed by client_id with the unpacked doc.
    let captured = dynamodb_transport.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].table, "telemetry_table");
    assert_eq!(captured[0].items.len(), 1);
    let item: serde_json::Value = serde_json::from_str(&captured[0].items[0].body).unwrap();
    assert_eq!(item["device_id"], serde_json::json!({"S": "sensor101"}));
    assert_eq!(item["temperature"], serde_json::json!({"N": "24.5"}));

    // Below-threshold telemetry fires nothing anywhere.
    engine
        .dispatch_ingress(
            &Topic::new("sensors/kitchen").unwrap(),
            &Bytes::from_static(br#"{ "client_id": "sensor101", "temperature": 10.0 }"#),
            QoS::AtMostOnce,
            &sink,
        )
        .await;
    assert_eq!(tdengine_transport.captured().len(), 1);
    assert_eq!(iotdb_transport.captured().len(), 1);
    assert_eq!(timestream_transport.captured().len(), 1);
    assert_eq!(dynamodb_transport.captured().len(), 1);
}

/// Lakehouse e2e (INDRA-222): one ingress event routes through
/// streaming SQL `INTO connector(...)` rules and fans out
/// simultaneously to the Snowflake, Databricks, Doris, BigQuery
/// and Redshift mocks with zero drops. All transports in-memory.
// Needs the `bigquery` heavy SDK (with its `gcp_pubsub` auth stack);
// runs on feature builds.
#[cfg(all(feature = "bigquery", feature = "gcp_pubsub"))]
#[tokio::test]
async fn test_into_fans_out_to_lakehouse_sinks() {
    use broker_connectors_enterprise::{
        BigQuerySink, BigQuerySinkConfig, DatabricksSink, DatabricksSinkConfig, DorisSink,
        DorisSinkConfig, MockBigQueryTransport, MockDatabricksTransport, MockDorisTransport,
        MockRedshiftTransport, MockSnowflakeTransport, RedshiftSink, RedshiftSinkConfig,
        SnowflakeSink, SnowflakeSinkConfig,
    };
    use std::collections::HashMap;

    // Test-only RSA key (openssl-generated, never deployed).
    const SNOWFLAKE_TEST_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCwQ2w63oB3FtHg\n7xysQK8MuX9S0WkbAVlxWpLHDNIdRVxA9Ra2gFFpKy8jX45UMSow6Yny7IvYWFzZ\nL4y9yoFiqu+LxhlJHIO6JO8+ZmeBoNwuDiIzgesbZwjyQiQ2M7p/4c18a2ffGPWF\nBETT7uVwVKJ3hTp97RN7Mc1/eFMimuT/TC11I+sFCZUHgrbhEG3L5Gg3RJ2MKbcX\nGEIxjFDJdLJ9RK0BopD6lxR1a4zeYr+iF/m3+JeJPAaS15yMD+sB1g5C7XZ1OIsB\nNBBnHWHpNhYO2IrCc9lZeSSzSkbRC6k1oqvTurFRzHWZqBKQGYnH8BftubIPSTBg\nU/BM4rR3AgMBAAECggEAHSRwmwUZoVb1CWcPSw2Aw65RtkwoQA5Hjv3GIcHlZXCH\n0beT80Wg8C3zI7qTSik8zAx4weDJOFJXu5LohqKaJMmVRHtSx+s+fkLICX2d5GlH\nrhepIPH8gLHW4VL9MLb5wVYAhu8tI845Ha54gL/RUHK1z+QHqTVO0MIJs2cd+6zx\nKsAtnqEQJMFpl1D0y0uutuboK4soHJMyRyrHBNWdgfzmTrCsngzu2zVM4aZh/gQY\nHcQgJ1rK6Wnen/GGPrNluwWU+bfLdlWO2qiXXwGLfhyx2H6cuROGdoU607BFJNpM\nkAudvEuLa0fOi1ym6lJ5pcJ6pSLkbeveW6+thkO2fQKBgQDXc2GiKx15vQHmdDmZ\nUJEiPJ+hSry5fjaowzrfgqJHyeNfUjnM/E9WlNn2AuxKDWGc3UNEr6jB9V7leKev\nQaPB2LAgXt0YVHmyim51/gTDguE9TOTGWqL4npZG9Nqh8xMxWt08ULvknkOQQOso\nzCoZQYlG4BHegAG7n0/5IN7HdQKBgQDRb/VbJ9iE0wtY/A3e3eWPbGfTF7AZREUu\n/mt94tFEWDDvedX1EPi4DJgPMqQ4eHnBZb3+G7jPcRdm6/KQzR5QiRMHSylfIQRH\nLqqfHBzZDDSZINLW1FMReC9xGfkRoG0Tlt2iQzXOy90+uE/9k5BGSbQNakfVDXJs\n3JAHDMy6uwKBgQCaazxC+xv5MRq3jf3qgPBE1aaj9+kkGe4bLzJ3GC4vveeVXl3H\nKd/DcpR12sp4mPapc3zPMgeGXNNTLRMiba1tNl2mFdfppEJFUSqyrwnDB39gbEhc\nUoIUJ7YVzVEWWh4bdcCzhjnlNfm+3oitiQdzaqF1hwvHqX+Udi7fpEuIMQKBgQC5\nu0bkQu7Rw/MRQ93tIe19ho6AdkZV8eREq52Z8vbQXEFxbiOfBCD93zVObQOTjMu1\nBcw6uEzpsgol3OKtJSpYE2eLlU0oLriDg9AN8DlpBljy31f66iqMmH/CFl16E0II\nGEeOqXnjXYlkIMHXR/CvVJdXOkRfnWA3SFZ12hUJFwKBgD8JlGTyrVfNsNMOaTDV\nNopoYnUQ6ljFmJi6TGmnkliCRXPuqBl+2hVxiKeWI2MprJ5Ya8qLbL6M56uCwAD2\nqEhvjEuatma5rJyE5NULOjAXA5tLw9qM1M9j1FNOaXnFC9/Yii2a49R8zu05wRB2\nH+dMMSDXQ4EHHYcKIFJjDbxn\n-----END PRIVATE KEY-----\n";

    let engine = RuleEngine::new(65_536, BackpressurePolicy::DropOldest);

    // Snowflake streaming rows (auto-flush every row).
    let snowflake_transport = Arc::new(MockSnowflakeTransport::new());
    let snowflake = Arc::new(
        SnowflakeSink::new(
            SnowflakeSinkConfig {
                account: "xy12345.us-east-1".to_string(),
                user: "indra_loader".to_string(),
                database: "IOT".to_string(),
                schema: "PUBLIC".to_string(),
                table_template: "TELEMETRY_${topic}".to_string(),
                private_key_pem: SNOWFLAKE_TEST_KEY.to_string(),
                endpoint: None,
                role: None,
                channel: "INDRA_CHANNEL".to_string(),
                column_mappings: HashMap::from([
                    ("device_id".to_string(), "${client_id}".to_string()),
                    (
                        "temperature".to_string(),
                        "${payload.temperature}".to_string(),
                    ),
                ]),
                batch_size: Some(1),
                batch_bytes: None,
                linger_ms: Some(20),
                max_retries: Some(4),
                initial_backoff_ms: Some(1),
                max_backoff_ms: Some(2),
                timeout_ms: None,
            },
            snowflake_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine
        .connectors()
        .register("snowflake-sink", snowflake.clone());

    // Databricks lakehouse rows (auto-flush every row).
    let databricks_transport = Arc::new(MockDatabricksTransport::new());
    let databricks = Arc::new(
        DatabricksSink::new(
            DatabricksSinkConfig {
                host: "dbc-test.cloud.databricks.com".to_string(),
                token: "dapi-test".to_string(),
                catalog: "main".to_string(),
                schema: "default".to_string(),
                table_template: "sensor_readings".to_string(),
                http_path: None,
                partition_key_template: None,
                column_mappings: HashMap::from([
                    ("device_id".to_string(), "${client_id}".to_string()),
                    (
                        "temperature".to_string(),
                        "${payload.temperature}".to_string(),
                    ),
                ]),
                batch_size: Some(1),
                batch_bytes: None,
                linger_ms: Some(10),
                max_retries: Some(3),
                initial_backoff_ms: Some(1),
                max_backoff_ms: Some(2),
                timeout_ms: None,
            },
            databricks_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine
        .connectors()
        .register("databricks-sink", databricks.clone());

    // Doris stream load (auto-flush every row).
    let doris_transport = Arc::new(MockDorisTransport::new());
    let doris = Arc::new(
        DorisSink::new(
            DorisSinkConfig {
                fe_host: "127.0.0.1".to_string(),
                http_port: 8030,
                database: "telemetry".to_string(),
                table_template: "events".to_string(),
                auth: broker_connectors_enterprise::DorisAuth {
                    username: "root".to_string(),
                    password: String::new(),
                },
                format: broker_connectors_enterprise::DorisFormat::Json,
                jsonpaths: None,
                strip_outer_array: true,
                max_filter_ratio: Some(0.0),
                batch_size: Some(1),
                batch_bytes: None,
                linger_ms: Some(10),
                max_retries: Some(4),
                initial_backoff_ms: Some(1),
                max_backoff_ms: Some(2),
                timeout_ms: None,
            },
            doris_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine.connectors().register("doris-sink", doris.clone());

    // BigQuery streaming rows (auto-flush every row).
    let bigquery_transport = Arc::new(MockBigQueryTransport::new());
    let bigquery = Arc::new(
        BigQuerySink::new(
            BigQuerySinkConfig {
                project_id: "my-iot-project".to_string(),
                dataset_id: "telemetry".to_string(),
                table_template: "sensor_logs".to_string(),
                endpoint: None,
                auth: broker_connectors_enterprise::GcpAuth::None,
                ignore_unknown_values: true,
                skip_invalid_rows: false,
                template_suffix: None,
                batch_size: Some(1),
                batch_bytes: None,
                linger_ms: Some(20),
                max_retries: Some(4),
                initial_backoff_ms: Some(1),
                max_backoff_ms: Some(2),
                timeout_ms: None,
            },
            bigquery_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine
        .connectors()
        .register("bigquery-sink", bigquery.clone());

    // Redshift statements (auto-flush every row).
    let redshift_transport = Arc::new(MockRedshiftTransport::new());
    let redshift = Arc::new(
        RedshiftSink::new(
            RedshiftSinkConfig {
                database: "analytics".to_string(),
                table_template: "sensor_logs".to_string(),
                cluster_identifier: None,
                workgroup_name: Some("iot-workgroup".to_string()),
                region: "us-east-1".to_string(),
                endpoint: None,
                access_key_id: "AKID".to_string(),
                secret_access_key: "secret".to_string(),
                session_token: None,
                db_user: None,
                sql_template: None,
                batch_size: Some(1),
                batch_bytes: None,
                linger_ms: Some(20),
                max_retries: Some(4),
                initial_backoff_ms: Some(1),
                max_backoff_ms: Some(2),
                timeout_ms: None,
            },
            redshift_transport.clone(),
        )
        .expect("valid sink"),
    );
    engine
        .connectors()
        .register("redshift-sink", redshift.clone());

    // One INTO rule per lakehouse over the same telemetry stream.
    for (id, connector) in [
        ("snowflake-rule", "snowflake-sink"),
        ("databricks-rule", "databricks-sink"),
        ("doris-rule", "doris-sink"),
        ("bigquery-rule", "bigquery-sink"),
        ("redshift-rule", "redshift-sink"),
    ] {
        engine
            .create_rule(
                id.to_string(),
                TopicFilter::new("factory/+").unwrap(),
                Some(format!(
                    r#"SELECT client_id, temperature FROM "factory/+" WHERE temperature > 70.0 INTO connector("{connector}")"#
                )),
                true,
                vec![],
            )
            .expect("rule creates");
    }

    let sink: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());
    engine
        .dispatch_ingress(
            &Topic::new("factory/temp").unwrap(),
            &Bytes::from_static(
                br#"{ "client_id": "sensor-101", "temperature": 75.2, "status": "normal" }"#,
            ),
            QoS::AtMostOnce,
            &sink,
        )
        .await;

    let projected = serde_json::json!({"client_id": "sensor-101", "temperature": 75.2});

    // Snowflake: uppercased table with mapped columns + metadata.
    let captured = snowflake_transport.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].table, "TELEMETRY_FACTORY_TEMP");
    assert_eq!(captured[0].rows.len(), 1);
    let fields: HashMap<String, &serde_json::Value> = captured[0].rows[0]
        .fields
        .iter()
        .map(|(k, v)| (k.clone(), v))
        .collect();
    assert_eq!(fields["DEVICE_ID"], &serde_json::json!("sensor-101"));
    assert_eq!(fields["TEMPERATURE"], &serde_json::json!(75.2));

    // Databricks: qualified INSERT with typed params.
    let captured = databricks_transport.captured();
    assert_eq!(captured.len(), 1);
    assert!(captured[0]
        .statement
        .starts_with("INSERT INTO main.default.sensor_readings"));
    assert!(captured[0]
        .params
        .iter()
        .any(|param| param.value == "sensor-101"));

    // Doris: one JSON array load with the projected row.
    let captured = doris_transport.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].table, "events");
    let body: serde_json::Value = serde_json::from_slice(&captured[0].body).unwrap();
    assert_eq!(body.as_array().expect("array").len(), 1);
    assert_eq!(body[0]["temperature"], serde_json::json!(75.2));

    // BigQuery: one row with a UUID insert id.
    let captured = bigquery_transport.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].table, "sensor_logs");
    assert_eq!(captured[0].rows.len(), 1);
    assert_eq!(captured[0].rows[0].json, projected);
    assert_eq!(captured[0].rows[0].insert_id.len(), 36);

    // Redshift: one statement carrying the projected payload.
    let captured = redshift_transport.captured();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].workgroup_name.as_deref(), Some("iot-workgroup"));
    assert_eq!(captured[0].statements.len(), 1);
    assert!(captured[0].statements[0].contains("'sensor-101'"));

    // Below-threshold telemetry fires nothing anywhere.
    engine
        .dispatch_ingress(
            &Topic::new("factory/temp").unwrap(),
            &Bytes::from_static(br#"{ "client_id": "sensor-101", "temperature": 60.0 }"#),
            QoS::AtMostOnce,
            &sink,
        )
        .await;
    assert_eq!(snowflake_transport.captured().len(), 1);
    assert_eq!(databricks_transport.captured().len(), 1);
    assert_eq!(doris_transport.captured().len(), 1);
    assert_eq!(bigquery_transport.captured().len(), 1);
    assert_eq!(redshift_transport.captured().len(), 1);
}

/// B3-06 broker proof: 1000 rule-shaped rows stream via
/// `dispatch_ingress` through `BigQuerySink` on the maintained
/// `SdkBigQueryTransport` to a loopback `insertAll` fake. Row count is
/// asserted on the fake (not just the sink counters), plus selective
/// requeue of only failed rows through the same rule path.
// Needs the `bigquery` heavy SDK (with its `gcp_pubsub` auth stack);
// runs on feature builds.
#[cfg(all(feature = "bigquery", feature = "gcp_pubsub"))]
#[tokio::test]
async fn test_bigquery_sdk_sink_via_rule_against_fake() {
    use axum::{extract::State, http::StatusCode, routing::post, Router};
    use broker_connectors::Sink;
    use broker_connectors_enterprise::{BigQuerySink, BigQuerySinkConfig, SdkBigQueryTransport};

    #[derive(Default)]
    struct FakeBigQuery {
        bodies: parking_lot::Mutex<Vec<serde_json::Value>>,
        script: parking_lot::Mutex<std::collections::VecDeque<(u16, serde_json::Value)>>,
    }

    async fn serve_fake(fake: Arc<FakeBigQuery>) -> String {
        async fn handler(
            State(fake): State<Arc<FakeBigQuery>>,
            body: String,
        ) -> (StatusCode, String) {
            let parsed: serde_json::Value =
                serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
            fake.bodies.lock().push(parsed);
            let next = fake.script.lock().pop_front();
            match next {
                Some((status, value)) => (
                    StatusCode::from_u16(status).unwrap_or(StatusCode::OK),
                    value.to_string(),
                ),
                None => (StatusCode::OK, "{}".to_string()),
            }
        }
        let app = Router::new()
            .route("/*rest", post(handler))
            .with_state(fake);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake bigquery");
        let port = listener.local_addr().expect("fake addr").port();
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve fake");
        });
        format!("http://127.0.0.1:{port}")
    }

    let fake = Arc::new(FakeBigQuery::default());
    let endpoint = serve_fake(fake.clone()).await;
    let config = BigQuerySinkConfig {
        project_id: "my-iot-project".to_string(),
        dataset_id: "telemetry".to_string(),
        table_template: "sensor_logs".to_string(),
        endpoint: Some(endpoint),
        auth: broker_connectors_enterprise::GcpAuth::None,
        ignore_unknown_values: true,
        skip_invalid_rows: false,
        template_suffix: None,
        batch_size: Some(100),
        batch_bytes: Some(1_048_576),
        linger_ms: Some(20),
        max_retries: Some(4),
        initial_backoff_ms: Some(1),
        max_backoff_ms: Some(2),
        timeout_ms: Some(15_000),
    };
    let transport = Arc::new(
        SdkBigQueryTransport::new(&config, reqwest::Client::new()).expect("sdk transport"),
    );
    let bigquery = Arc::new(BigQuerySink::new(config, transport).expect("sink"));
    assert_eq!(bigquery.kind(), "bigquery");

    let engine = RuleEngine::new(65_536, BackpressurePolicy::DropOldest);
    engine
        .connectors()
        .register("bigquery-sdk-sink", bigquery.clone());
    engine
        .create_rule(
            "bq-sdk-rule".to_string(),
            TopicFilter::new("sensors/qual").unwrap(),
            Some(
                r#"SELECT device_id, temp, seq FROM "sensors/qual" INTO connector("bigquery-sdk-sink")"#
                    .to_string(),
            ),
            true,
            vec![],
        )
        .expect("rule creates");

    let probe: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());
    for seq in 0..1000 {
        let payload = Bytes::from(format!(
            r#"{{"device_id":"dev-{seq:04}","temp":{temp},"seq":{seq}}}"#,
            temp = 20.0 + f64::from(seq) * 0.01
        ));
        engine
            .dispatch_ingress(
                &Topic::new("sensors/qual").unwrap(),
                &payload,
                QoS::AtLeastOnce,
                &probe,
            )
            .await;
    }
    bigquery.flush().await.expect("flush");
    assert_eq!(bigquery.sent_records(), 1000);
    let total: usize = fake
        .bodies
        .lock()
        .iter()
        .map(|body| body["rows"].as_array().map_or(0, |rows| rows.len()))
        .sum();
    assert_eq!(total, 1000);
    let first = fake.bodies.lock()[0].clone();
    assert_eq!(
        first["rows"][0]["json"]["device_id"]
            .as_str()
            .expect("device"),
        "dev-0000"
    );

    // Partial-failure retry through the same rule path requeues only
    // the failed row.
    let partial_fake = Arc::new(FakeBigQuery {
        bodies: parking_lot::Mutex::new(Vec::new()),
        script: parking_lot::Mutex::new(
            vec![
                (
                    200,
                    serde_json::json!({
                        "insertErrors": [{"index": 1, "errors": [{"reason": "rateLimitExceeded"}]}]
                    }),
                ),
                (200, serde_json::json!({})),
            ]
            .into_iter()
            .collect(),
        ),
    });
    let partial_endpoint = serve_fake(partial_fake.clone()).await;
    let partial_config = BigQuerySinkConfig {
        project_id: "my-iot-project".to_string(),
        dataset_id: "telemetry".to_string(),
        table_template: "sensor_logs".to_string(),
        endpoint: Some(partial_endpoint),
        auth: broker_connectors_enterprise::GcpAuth::None,
        ignore_unknown_values: true,
        skip_invalid_rows: false,
        template_suffix: None,
        batch_size: Some(10),
        batch_bytes: Some(1_048_576),
        linger_ms: Some(20),
        max_retries: Some(4),
        initial_backoff_ms: Some(1),
        max_backoff_ms: Some(2),
        timeout_ms: Some(15_000),
    };
    let partial_transport = Arc::new(
        SdkBigQueryTransport::new(&partial_config, reqwest::Client::new())
            .expect("partial transport"),
    );
    let partial_sink =
        Arc::new(BigQuerySink::new(partial_config, partial_transport).expect("sink"));
    let partial_engine = RuleEngine::new(65_536, BackpressurePolicy::DropOldest);
    partial_engine
        .connectors()
        .register("bq-partial-sink", partial_sink.clone());
    partial_engine
        .create_rule(
            "bq-partial-rule".to_string(),
            TopicFilter::new("sensors/retry").unwrap(),
            Some(
                r#"SELECT temp FROM "sensors/retry" INTO connector("bq-partial-sink")"#.to_string(),
            ),
            true,
            vec![],
        )
        .expect("rule creates");
    for temp in [20.5, 21.5] {
        partial_engine
            .dispatch_ingress(
                &Topic::new("sensors/retry").unwrap(),
                &Bytes::from(format!(r#"{{"temp":{temp}}}"#)),
                QoS::AtMostOnce,
                &probe,
            )
            .await;
    }
    partial_sink.flush().await.expect("retry flush");
    assert_eq!(partial_fake.bodies.lock().len(), 2);
    let retry_bodies = partial_fake.bodies.lock().clone();
    assert_eq!(retry_bodies[1]["rows"].as_array().expect("rows").len(), 1);
    assert_eq!(
        retry_bodies[1]["rows"][0]["json"]["temp"],
        serde_json::json!(21.5)
    );
}
