use super::*;

#[cfg(all(
    feature = "kafka",
    feature = "rabbitmq",
    feature = "clickhouse",
    feature = "s3",
    feature = "elasticsearch",
    feature = "mongodb",
    feature = "cassandra",
    feature = "couchbase",
    feature = "confluent",
    feature = "gcp_pubsub",
    feature = "bigquery",
    feature = "kinesis",
    feature = "opc_ua"
))]
#[tokio::test]
async fn test_connectors_create_kafka_rabbitmq_logger() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Fixed test RSA key for the OCI + GCP-IoT creation legs below
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

    // Kafka sink: validated, lazily connected, listed with its kind.
    // X1-10: gated behind Cargo feature `kafka`.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "kafka-sink-1",
                       "kind": "kafka",
                       "config": {"bootstrap_servers": "127.0.0.1:9092",
                                  "topic_template": "out-${topic}",
                                  "partition_key_field": "device_id",
                                  "partitions": 4,
                                  "client_id": "indra",
                                  "acks": "all"}}),
            &token,
        )
        .await;
    if cfg!(feature = "kafka") {
        assert_eq!(status, 201);
        assert_eq!(created["id"], json!("kafka-sink-1"));
        assert_eq!(created["kind"], json!("kafka"));
    } else {
        assert_eq!(status, 400);
        assert!(
            created["error"].as_str().unwrap_or("").contains("kafka"),
            "disabled kafka must name the feature, got: {created}"
        );
    }

    // RabbitMQ sink: slash translation config accepted.
    // X1-10: gated behind Cargo feature `rabbitmq`.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "rabbit-sink-1",
                       "kind": "rabbitmq",
                       "config": {"endpoint": "amqp://127.0.0.1:5672/%2f",
                                  "exchange": "telemetry",
                                  "routing_key_template": "sensor.${topic}",
                                  "delivery_mode": 2}}),
            &token,
        )
        .await;
    if cfg!(feature = "rabbitmq") {
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("rabbitmq"));
    } else {
        assert_eq!(status, 400);
        assert!(
            created["error"].as_str().unwrap_or("").contains("rabbitmq"),
            "disabled rabbitmq must name the feature, got: {created}"
        );
    }

    // Logger sink needs no config at all.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "diag", "kind": "logger", "config": {}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("console"));

    // Postgres sink: validated without touching any database.
    let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "pg-sink-1",
                       "kind": "postgres",
                       "config": {"connection_url": "postgresql://u:p@127.0.0.1:5432/db",
                                  "sql_template": "INSERT INTO t (topic, qos, payload) VALUES ($1, $2, $3)",
                                  "pool_size": 2,
                                  "batch_size": 50,
                                  "batch_timeout_ms": 25}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("postgres"));

    // Redis sink: XADD stream config accepted (command is flat).
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "redis-sink-1",
                       "kind": "redis",
                       "config": {"endpoint": "redis://127.0.0.1:6379",
                                  "command": "xadd",
                                  "stream_template": "events",
                                  "maxlen": 1000}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("redis"));

    // MySQL sink: validated without touching any database.
    let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "mysql-sink-1",
                       "kind": "mysql",
                       "config": {"connection_url": "mysql://u:p@127.0.0.1:3306/db",
                                  "sql_template": "INSERT INTO t (topic, qos, payload) VALUES (?, ?, ?)",
                                  "pool_size": 2,
                                  "batch_size": 50,
                                  "batch_timeout_ms": 25}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("mysql"));

    // ClickHouse sink: validated without touching any server.
    // X1-10: gated behind Cargo feature `clickhouse`.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "ch-sink-1",
                       "kind": "clickhouse",
                       "config": {"endpoint": "http://127.0.0.1:8123",
                                  "database": "indra",
                                  "table": "mqtt_events",
                                  "format": "JSONEachRow",
                                  "batch_size": 100,
                                  "batch_timeout_ms": 50}}),
            &token,
        )
        .await;
    if cfg!(feature = "clickhouse") {
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("clickhouse"));
    } else {
        assert_eq!(status, 400);
        assert!(
            created["error"]
                .as_str()
                .unwrap_or("")
                .contains("clickhouse"),
            "disabled clickhouse must name the feature, got: {created}"
        );
    }

    // InfluxDB sink: validated without touching any server.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "influx-sink-1",
                       "kind": "influxdb",
                       "config": {"endpoint": "http://127.0.0.1:8086",
                                  "bucket": "mqtt",
                                  "org": "indra",
                                  "token": "secret",
                                  "measurement_template": "mqtt_events",
                                  "precision": "ms",
                                  "batch_size": 100,
                                  "batch_timeout_ms": 50}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("influxdb"));

    // S3 sink: validated without touching any object store.
    // X1-09: `s3` gated behind Cargo feature `s3`; disabled builds
    // fail closed naming the feature.
    let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "s3-sink-1",
                       "kind": "s3",
                       "config": {"endpoint": "http://127.0.0.1:9000",
                                  "bucket": "telemetry-cold-store",
                                  "region": "us-east-1",
                                  "key_template": "telemetry/year=${YYYY}/month=${MM}/day=${DD}/${topic}_${seq}.ndjson",
                                  "compression": "none",
                                  "batch_size": 100,
                                  "batch_bytes": 65536,
                                  "batch_timeout_ms": 1000}}),
                &token,
            )
            .await;
    if cfg!(feature = "s3") {
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("s3"));
    } else {
        assert_eq!(status, 400);
        assert!(
            created["error"].as_str().unwrap_or("").contains("s3"),
            "disabled s3 must name the feature, got: {created}"
        );
    }

    // Elasticsearch sink: validated without touching any cluster.
    // X1-10: gated behind Cargo feature `elasticsearch`.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "es-sink-1",
                       "kind": "elasticsearch",
                       "config": {"endpoint": "http://127.0.0.1:9200",
                                  "index_template": "iot-telemetry-${YYYY.MM.dd}",
                                  "auth": {"type": "none"},
                                  "batch_size": 100,
                                  "batch_timeout_ms": 50,
                                  "max_retries": 3}}),
            &token,
        )
        .await;
    if cfg!(feature = "elasticsearch") {
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("elasticsearch"));
    } else {
        assert_eq!(status, 400);
        assert!(
            created["error"]
                .as_str()
                .unwrap_or("")
                .contains("elasticsearch"),
            "disabled elasticsearch must name the feature, got: {created}"
        );
    }

    // TimescaleDB sink: validated without touching any database.
    let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "ts-sink-1",
                       "kind": "timescaledb",
                       "config": {"connection_url": "postgresql://u:p@127.0.0.1:5432/timeseries",
                                  "hypertable": "sensor_metrics",
                                  "time_column": "time",
                                  "sql_template": "INSERT INTO sensor_metrics (time, device_id, topic, metrics) VALUES ($1, $2, $3, $4::jsonb) ON CONFLICT (time, device_id) DO UPDATE SET metrics = EXCLUDED.metrics",
                                  "pool_size": 2,
                                  "batch_size": 50,
                                  "batch_timeout_ms": 25}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("timescaledb"));

    // Webhook sink: validated without sending any request.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "hook-1",
                       "kind": "webhook",
                       "config": {"url": "https://hooks.example.com/ingest/${topic}",
                                  "method": "post",
                                  "headers": {},
                                  "auth": {"type": "none"},
                                  "body_format": "rawjson",
                                  "batch_size": 50,
                                  "batch_bytes": 65536,
                                  "linger_ms": 25,
                                  "timeout_ms": 1000,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("webhook"));

    // MQTT bridge sink: validated without opening any socket.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "bridge-1",
                       "kind": "mqtt_bridge",
                       "config": {"broker_address": "mqtt://127.0.0.1:1883",
                                  "client_id": "indra-bridge-test",
                                  "clean_start": true,
                                  "keep_alive_secs": 60,
                                  "max_inflight": 1000,
                                  "max_batch_size": 50,
                                  "linger_ms": 10,
                                  "protocol": "v311"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("mqtt_bridge"));

    // Disk log sink: validated against a relative path inside the data directory.
    let disk_dir = format!("indra-test-disk-{}", std::process::id());
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "disk-1",
                       "kind": "disk_log",
                       "config": {"directory": disk_dir,
                                  "filename_prefix": "audit",
                                  "filename_extension": "log",
                                  "format": "ndjson",
                                  "compression": "none",
                                  "sync_mode": {"mode": "osdefault"}}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("disk_log"));

    // Sparkplug B sink: the driver connects at the first write, thus
    // the create step does not need a server.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "spb-1",
                       "kind": "sparkplug_b",
                       "config": {"topic_prefix": "spBv1.0/plant1",
                                  "mqtt_url": "mqtt://127.0.0.1:1883",
                                  "tier": "enterprise",
                                  "batch_size": 50,
                                  "linger_ms": 25}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("sparkplug_b"));

    // Kinesis sink: validated without touching AWS.
    // X1-10: gated behind Cargo feature `kinesis`.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "kinesis-1",
                       "kind": "kinesis",
                       "config": {"stream_name": "telemetry-stream",
                                  "region": "us-east-1",
                                  "access_key_id": "AKID",
                                  "secret_access_key": "secret",
                                  "partition_key_template": "${topic}",
                                  "batch_size": 100,
                                  "batch_bytes": 65536,
                                  "linger_ms": 20,
                                  "max_retries": 3,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
            &token,
        )
        .await;
    if cfg!(feature = "kinesis") {
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("kinesis"));
    } else {
        assert_eq!(status, 400);
        assert!(
            created["error"].as_str().unwrap_or("").contains("kinesis"),
            "disabled kinesis must name the feature, got: {created}"
        );
    }

    // GCP Pub/Sub sink: validated without touching Google.
    // X1-10: gated behind Cargo feature `gcp_pubsub`.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "gcp-1",
                       "kind": "gcp_pubsub",
                       "config": {"project_id": "my-iot-project",
                                  "topic_id": "telemetry-events",
                                  "auth": {"type": "none"},
                                  "batch_size": 100,
                                  "batch_bytes": 65536,
                                  "linger_ms": 10,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
            &token,
        )
        .await;
    if cfg!(feature = "gcp_pubsub") {
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("gcp_pubsub"));
    } else {
        assert_eq!(status, 400);
        assert!(
            created["error"]
                .as_str()
                .unwrap_or("")
                .contains("gcp_pubsub"),
            "disabled gcp_pubsub must name the feature, got: {created}"
        );
    }

    // Azure Event Hubs sink: validated without touching Azure.
    // X1-09: gated behind Cargo feature `azure_eventhubs`.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "azure-1",
                       "kind": "azure_eventhubs",
                       "config": {"namespace": "my-eventhub-ns",
                                  "event_hub": "telemetry-hub",
                                  "shared_access_key_name": "SendPolicy",
                                  "shared_access_key": "secret",
                                  "token_ttl_secs": 3600,
                                  "batch_size": 50,
                                  "batch_bytes": 65536,
                                  "linger_ms": 20,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
            &token,
        )
        .await;
    if cfg!(feature = "azure_eventhubs") {
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("azure_eventhubs"));
    } else {
        assert_eq!(status, 400);
        assert!(
            created["error"]
                .as_str()
                .unwrap_or("")
                .contains("azure_eventhubs"),
            "disabled azure_eventhubs must name the feature, got: {created}"
        );
    }

    // Pulsar sink: validated without opening any socket.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "pulsar-1",
                       "kind": "pulsar",
                       "config": {"service_url": "pulsar://127.0.0.1:6650",
                                  "tenant": "public",
                                  "namespace": "default",
                                  "topic": "iot-telemetry",
                                  "auth": {"type": "none"},
                                  "batch_size": 50,
                                  "batch_bytes": 65536,
                                  "linger_ms": 10,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("pulsar"));

    // MongoDB sink: validated without opening any socket.
    // X1-10: gated behind Cargo feature `mongodb`.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "mongo-1",
                       "kind": "mongodb",
                       "config": {"connection_string": "mongodb://u:p@127.0.0.1:27017",
                                  "database": "telemetry",
                                  "collection_template": "readings",
                                  "operation": {"type": "insert_one"},
                                  "batch_size": 50,
                                  "batch_bytes": 65536,
                                  "linger_ms": 20,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
            &token,
        )
        .await;
    if cfg!(feature = "mongodb") {
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("mongodb"));
    } else {
        assert_eq!(status, 400);
        assert!(
            created["error"].as_str().unwrap_or("").contains("mongodb"),
            "disabled mongodb must name the feature, got: {created}"
        );
    }

    // MSSQL sink: validated without opening any socket.
    let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "mssql-1",
                       "kind": "mssql",
                       "config": {"host": "127.0.0.1",
                                  "database": "telemetry",
                                  "table_template": "dbo.SensorEvents",
                                  "auth": {"type": "sql_password", "username": "sa", "password": "secret"},
                                  "query_mode": {"mode": "insertjson"},
                                  "trust_server_certificate": true,
                                  "batch_size": 50,
                                  "batch_bytes": 65536,
                                  "linger_ms": 20,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("mssql"));

    // Cassandra sink: validated without opening any socket.
    // X1-10: gated behind Cargo feature `cassandra`.
    let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "cassandra-1",
                       "kind": "cassandra",
                       "config": {"contact_points": ["10.0.0.1:9042"],
                                  "keyspace": "telemetry",
                                  "table_template": "events",
                                  "auth": {"type": "none"},
                                  "consistency": "localquorum",
                                  "partition_key_template": "${topic}",
                                  "cql_statement_template": "INSERT INTO telemetry.events (device_id, bucket_hour, event_time, payload) VALUES (?, ?, ?, ?)",
                                  "batch_size": 50,
                                  "batch_bytes": 65536,
                                  "linger_ms": 10,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
    if cfg!(feature = "cassandra") {
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("cassandra"));
    } else {
        assert_eq!(status, 400);
        assert!(
            created["error"]
                .as_str()
                .unwrap_or("")
                .contains("cassandra"),
            "disabled cassandra must name the feature, got: {created}"
        );
    }

    // Couchbase sink: validated without opening any socket.
    // X1-10: gated behind Cargo feature `couchbase`.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "couchbase-1",
                       "kind": "couchbase",
                       "config": {"connection_string": "couchbase://127.0.0.1",
                                  "bucket": "telemetry",
                                  "auth": {"username": "Administrator", "password": "secret"},
                                  "doc_id_template": "${client_id}::${timestamp}",
                                  "operation": "upsert",
                                  "batch_size": 50,
                                  "batch_bytes": 65536,
                                  "linger_ms": 10,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
            &token,
        )
        .await;
    if cfg!(feature = "couchbase") {
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("couchbase"));
    } else {
        assert_eq!(status, 400);
        assert!(
            created["error"]
                .as_str()
                .unwrap_or("")
                .contains("couchbase"),
            "disabled couchbase must name the feature, got: {created}"
        );
    }

    // TDengine sink: validated without touching any server.
    let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "tdengine-1",
                       "kind": "tdengine",
                       "config": {"endpoint": "http://127.0.0.1:6041/rest/sql",
                                  "database": "power",
                                  "stable_name": "meters",
                                  "subtable_template": "d_${client_id}",
                                  "auth": {"type": "basic", "username": "root", "password": "taosdata"},
                                  "tags_template": {},
                                  "metrics_template": {},
                                  "batch_size": 100,
                                  "batch_bytes": 65536,
                                  "linger_ms": 20,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("tdengine"));

    // IoTDB sink: validated without touching any server.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "iotdb-1",
                       "kind": "iotdb",
                       "config": {"endpoint": "http://127.0.0.1:18080/rest/v2",
                                  "device_path_template": "root.factory.plant1",
                                  "auth": {"username": "root", "password": "root"},
                                  "is_aligned": false,
                                  "measurements": ["temperature"],
                                  "data_types": ["DOUBLE"],
                                  "batch_size": 50,
                                  "batch_bytes": 65536,
                                  "linger_ms": 10,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("iotdb"));

    // Timestream sink: validated without touching AWS.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "timestream-1",
                       "kind": "timestream",
                       "config": {"database_name": "iot_database",
                                  "table_name": "telemetry",
                                  "region": "us-east-1",
                                  "access_key_id": "AKID",
                                  "secret_access_key": "secret",
                                  "dimensions": {},
                                  "time_unit": "milliseconds",
                                  "multi_measure_mappings": {"temperature": "DOUBLE"},
                                  "batch_size": 50,
                                  "batch_bytes": 65536,
                                  "linger_ms": 20,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("timestream"));

    // DynamoDB sink: validated without touching AWS.
    // X1-09: gated behind Cargo feature `dynamodb`.
    let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "dynamodb-1",
                       "kind": "dynamodb",
                       "config": {"table_name": "telemetry_table",
                                  "region": "us-east-1",
                                  "access_key_id": "AKID",
                                  "secret_access_key": "secret",
                                  "partition_key": {"name": "device_id", "template": "${client_id}", "key_type": "S"},
                                  "attributes_mapping": {},
                                  "batch_size": 25,
                                  "batch_bytes": 65536,
                                  "linger_ms": 10,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
    if cfg!(feature = "dynamodb") {
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("dynamodb"));
    } else {
        assert_eq!(status, 400);
        assert!(
            created["error"].as_str().unwrap_or("").contains("dynamodb"),
            "disabled dynamodb must name the feature, got: {created}"
        );
    }

    // Snowflake sink: key-pair validated without touching Snowflake.
    // Test-only RSA key (openssl-generated, never deployed).
    #[cfg(test)]
    const SNOWFLAKE_TEST_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCwQ2w63oB3FtHg\n7xysQK8MuX9S0WkbAVlxWpLHDNIdRVxA9Ra2gFFpKy8jX45UMSow6Yny7IvYWFzZ\nL4y9yoFiqu+LxhlJHIO6JO8+ZmeBoNwuDiIzgesbZwjyQiQ2M7p/4c18a2ffGPWF\nBETT7uVwVKJ3hTp97RN7Mc1/eFMimuT/TC11I+sFCZUHgrbhEG3L5Gg3RJ2MKbcX\nGEIxjFDJdLJ9RK0BopD6lxR1a4zeYr+iF/m3+JeJPAaS15yMD+sB1g5C7XZ1OIsB\nNBBnHWHpNhYO2IrCc9lZeSSzSkbRC6k1oqvTurFRzHWZqBKQGYnH8BftubIPSTBg\nU/BM4rR3AgMBAAECggEAHSRwmwUZoVb1CWcPSw2Aw65RtkwoQA5Hjv3GIcHlZXCH\n0beT80Wg8C3zI7qTSik8zAx4weDJOFJXu5LohqKaJMmVRHtSx+s+fkLICX2d5GlH\nrhepIPH8gLHW4VL9MLb5wVYAhu8tI845Ha54gL/RUHK1z+QHqTVO0MIJs2cd+6zx\nKsAtnqEQJMFpl1D0y0uutuboK4soHJMyRyrHBNWdgfzmTrCsngzu2zVM4aZh/gQY\nHcQgJ1rK6Wnen/GGPrNluwWU+bfLdlWO2qiXXwGLfhyx2H6cuROGdoU607BFJNpM\nkAudvEuLa0fOi1ym6lJ5pcJ6pSLkbeveW6+thkO2fQKBgQDXc2GiKx15vQHmdDmZ\nUJEiPJ+hSry5fjaowzrfgqJHyeNfUjnM/E9WlNn2AuxKDWGc3UNEr6jB9V7leKev\nQaPB2LAgXt0YVHmyim51/gTDguE9TOTGWqL4npZG9Nqh8xMxWt08ULvknkOQQOso\nzCoZQYlG4BHegAG7n0/5IN7HdQKBgQDRb/VbJ9iE0wtY/A3e3eWPbGfTF7AZREUu\n/mt94tFEWDDvedX1EPi4DJgPMqQ4eHnBZb3+G7jPcRdm6/KQzR5QiRMHSylfIQRH\nLqqfHBzZDDSZINLW1FMReC9xGfkRoG0Tlt2iQzXOy90+uE/9k5BGSbQNakfVDXJs\n3JAHDMy6uwKBgQCaazxC+xv5MRq3jf3qgPBE1aaj9+kkGe4bLzJ3GC4vveeVXl3H\nKd/DcpR12sp4mPapc3zPMgeGXNNTLRMiba1tNl2mFdfppEJFUSqyrwnDB39gbEhc\nUoIUJ7YVzVEWWh4bdcCzhjnlNfm+3oitiQdzaqF1hwvHqX+Udi7fpEuIMQKBgQC5\nu0bkQu7Rw/MRQ93tIe19ho6AdkZV8eREq52Z8vbQXEFxbiOfBCD93zVObQOTjMu1\nBcw6uEzpsgol3OKtJSpYE2eLlU0oLriDg9AN8DlpBljy31f66iqMmH/CFl16E0II\nGEeOqXnjXYlkIMHXR/CvVJdXOkRfnWA3SFZ12hUJFwKBgD8JlGTyrVfNsNMOaTDV\nNopoYnUQ6ljFmJi6TGmnkliCRXPuqBl+2hVxiKeWI2MprJ5Ya8qLbL6M56uCwAD2\nqEhvjEuatma5rJyE5NULOjAXA5tLw9qM1M9j1FNOaXnFC9/Yii2a49R8zu05wRB2\nH+dMMSDXQ4EHHYcKIFJjDbxn\n-----END PRIVATE KEY-----\n";
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "snowflake-1",
                       "kind": "snowflake",
                       "config": {"account": "xy12345.us-east-1",
                                  "user": "indra_loader",
                                  "database": "IOT",
                                  "schema": "PUBLIC",
                                  "table_template": "IOT_EVENTS",
                                  "private_key_pem": SNOWFLAKE_TEST_KEY,
                                  "channel": "INDRA_CHANNEL",
                                  "batch_size": 100,
                                  "batch_bytes": 65536,
                                  "linger_ms": 20,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("snowflake"));

    // Databricks sink: validated without touching any workspace.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "databricks-1",
                       "kind": "databricks",
                       "config": {"host": "dbc-a1b2c3d4-e5f6.cloud.databricks.com",
                                  "token": "dapi-test",
                                  "catalog": "main",
                                  "schema": "default",
                                  "table_template": "sensor_readings",
                                  "batch_size": 50,
                                  "batch_bytes": 65536,
                                  "linger_ms": 10,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("databricks"));

    // Doris sink: validated without touching any cluster.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "doris-1",
                       "kind": "doris",
                       "config": {"fe_host": "127.0.0.1",
                                  "http_port": 8030,
                                  "database": "telemetry",
                                  "table_template": "events",
                                  "auth": {"username": "root", "password": ""},
                                  "format": "json",
                                  "strip_outer_array": true,
                                  "batch_size": 100,
                                  "batch_bytes": 65536,
                                  "linger_ms": 10,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("doris"));

    // BigQuery sink: validated without touching Google.
    // X1-09: gated behind Cargo feature `bigquery`.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "bigquery-1",
                       "kind": "bigquery",
                       "config": {"project_id": "my-iot-project",
                                  "dataset_id": "telemetry",
                                  "table_template": "sensor_logs",
                                  "auth": {"type": "none"},
                                  "ignore_unknown_values": true,
                                  "skip_invalid_rows": false,
                                  "batch_size": 100,
                                  "batch_bytes": 65536,
                                  "linger_ms": 20,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
            &token,
        )
        .await;
    if cfg!(feature = "bigquery") {
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("bigquery"));
    } else {
        assert_eq!(status, 400);
        assert!(
            created["error"].as_str().unwrap_or("").contains("bigquery"),
            "disabled bigquery must name the feature, got: {created}"
        );
    }

    // Redshift sink: validated without touching AWS.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "redshift-1",
                       "kind": "redshift",
                       "config": {"database": "analytics",
                                  "table_template": "sensor_logs",
                                  "workgroup_name": "iot-workgroup",
                                  "region": "us-east-1",
                                  "access_key_id": "AKID",
                                  "secret_access_key": "secret",
                                  "batch_size": 50,
                                  "batch_bytes": 65536,
                                  "linger_ms": 20,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("redshift"));

    // OCI Streaming sink: Cavage-signed, validated without touching OCI.
    let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "oci-1",
                       "kind": "oci_streaming",
                       "config": {"endpoint": "https://cell-1.streaming.us-east-1.oci.oraclecloud.com",
                                  "stream_pool_id": "ocid1.streampool.oc1..testpool",
                                  "stream_id": "ocid1.stream.oc1..teststream",
                                  "tenancy_ocid": "ocid1.tenancy.oc1..test",
                                  "user_ocid": "ocid1.user.oc1..test",
                                  "fingerprint": "20:3b:97:13:aa:bb:cc:dd:ee:ff:00:11:22:33:44:55",
                                  "private_key_pem": test_pem.clone(),
                                  "partition_key_template": "${client_id}",
                                  "batch_size": 500,
                                  "batch_bytes": 65536,
                                  "linger_ms": 100,
                                  "max_retries": 5,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("oci_streaming"));

    // AWS IoT Core sink: mTLS-only build, validated without opening any socket.
    // SigV4/WebSocket is intentionally not built (no licence-compliant
    // WebSocket client); SigV4 configs are rejected at registration.
    // X1-09: gated behind Cargo feature `aws_iot`.
    let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "aws-iot-1",
                       "kind": "aws_iot",
                       "config": {"endpoint": "abc-ats.iot.us-east-1.amazonaws.com",
                                  "region": "us-east-1",
                                  "client_id": "indra-bridge-1",
                                  "auth": {"type": "mtls",
                                            "ca_cert_pem": include_str!("../../../broker-connectors-enterprise/testdata/ca-cert.pem"),
                                            "client_cert_pem": include_str!("../../../broker-connectors-enterprise/testdata/client-cert.pem"),
                                            "client_key_pem": include_str!("../../../broker-connectors-enterprise/testdata/client-key.pem")},
                                  "topic_mappings": [{"local_topic": "sensors/+",
                                                      "remote_topic": "indra/up",
                                                      "direction": "localtoremote"}],
                                  "batch_size": 200,
                                  "linger_ms": 50,
                                  "max_retries": 5}}),
                &token,
            )
            .await;
    if cfg!(feature = "aws_iot") {
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("aws_iot"));
    } else {
        assert_eq!(status, 400);
        assert!(
            created["error"].as_str().unwrap_or("").contains("aws_iot"),
            "disabled aws_iot must name the feature, got: {created}"
        );
    }

    // Azure IoT Hub sink: validated without opening any socket.
    // X1-09: gated behind Cargo feature `azure_iot`.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "azure-iot-1",
                       "kind": "azure_iot",
                       "config": {"iot_hub_name": "e2e-hub",
                                  "device_id": "e2e-device",
                                  "module_id": null,
                                  "auth": {"type": "sharedaccesskey",
                                            "key": "c2VjcmV0",
                                            "key_name": "device"},
                                  "api_version": "2021-04-12",
                                  "direct_methods_enabled": false,
                                  "twin_sync_enabled": false,
                                  "batch_size": 200,
                                  "linger_ms": 50,
                                  "max_retries": 5}}),
            &token,
        )
        .await;
    if cfg!(feature = "azure_iot") {
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("azure_iot"));
    } else {
        assert_eq!(status, 400);
        assert!(
            created["error"]
                .as_str()
                .unwrap_or("")
                .contains("azure_iot"),
            "disabled azure_iot must name the feature, got: {created}"
        );
    }

    // GCP IoT Core sink: validated without opening any socket.
    // X1-09: gated behind Cargo feature `gcp_iot`.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "gcp-iot-1",
                       "kind": "gcp_iot",
                       "config": {"project_id": "e2e-project",
                                  "cloud_region": "us-central1",
                                  "registry_id": "e2e-registry",
                                  "device_id": "e2e-device",
                                  "private_key_pem": test_pem,
                                  "algorithm": "RS256",
                                  "token_lifetime_secs": 3600,
                                  "endpoint": "mqtt.googleapis.com:8883",
                                  "batch_size": 200,
                                  "linger_ms": 50,
                                  "max_retries": 5}}),
            &token,
        )
        .await;
    if cfg!(feature = "gcp_iot") {
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("gcp_iot"));
    } else {
        assert_eq!(status, 400);
        assert!(
            created["error"].as_str().unwrap_or("").contains("gcp_iot"),
            "disabled gcp_iot must name the feature, got: {created}"
        );
    }

    // OPC-UA sink: validated without opening any socket.
    let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "opcua-1",
                       "kind": "opc_ua",
                       "config": {"endpoint_url": "opc.tcp://127.0.0.1:4840",
                                  "security_policy": "None",
                                  "security_mode": "none",
                                  "auth": {"type": "anonymous"},
                                  "node_subscriptions": [{"node_id": "ns=2;s=Temperature",
                                                          "sampling_interval_ms": 1000,
                                                          "publish_topic_template": "opcua/temperature"}],
                                  "batch_size": 200,
                                  "linger_ms": 50}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("opc_ua"));

    // Azure Blob sink: validated without touching Azure.
    // X1-09: gated behind Cargo feature `azure_blob`.
    let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "azblob-1",
                       "kind": "azure_blob",
                       "config": {"account_name": "mydeviceblobs",
                                  "container_name": "telemetry",
                                  "auth": {"type": "sharedkey",
                                            "account_key": "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="},
                                  "blob_path_template": "telemetry/year=${date.year}/${batch_id}.json",
                                  "compression": "none",
                                  "max_records_per_blob": 10000,
                                  "max_bytes_per_blob": 10485760,
                                  "flush_interval_secs": 60}}),
                &token,
            )
            .await;
    if cfg!(feature = "azure_blob") {
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("azure_blob"));
    } else {
        assert_eq!(status, 400);
        assert!(
            created["error"]
                .as_str()
                .unwrap_or("")
                .contains("azure_blob"),
            "disabled azure_blob must name the feature, got: {created}"
        );
    }

    // Tablestore sink: validated without touching Alibaba Cloud.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "ots-1",
                       "kind": "tablestore",
                       "config": {"endpoint": "https://test-instance.cn-hangzhou.ots.aliyuncs.com",
                                  "instance_name": "test-instance",
                                  "table_name": "telemetry",
                                  "measurement": "telemetry",
                                  "access_key_id": "test-key-id",
                                  "access_key_secret": "test-secret",
                                  "primary_keys": [{"name": "device_id",
                                                    "source": "${client_id}",
                                                    "data_type": "string"}],
                                  "attribute_columns": [{"name": "temperature",
                                                         "source": "${payload.temperature}",
                                                         "data_type": "double"}],
                                  "batch_size": 200}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("tablestore"));

    // Confluent Cloud sink: validated without opening any socket.
    // X1-10: gated behind Cargo feature `confluent`.
    let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "confluent-1",
                       "kind": "confluent",
                       "config": {"bootstrap_servers": ["pkc-test.us-east-1.aws.confluent.cloud:9092"],
                                  "api_key": "confluent-key",
                                  "api_secret": "confluent-secret",
                                  "auth_mechanism": "plain",
                                  "topic_template": "telemetry-${topic_segment_1}",
                                  "partition_key_template": "${client_id}",
                                  "partitions": 12,
                                  "batch_size": 500}}),
                &token,
            )
            .await;
    if cfg!(feature = "confluent") {
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("confluent"));
    } else {
        assert_eq!(status, 400);
        assert!(
            created["error"]
                .as_str()
                .unwrap_or("")
                .contains("confluent"),
            "disabled confluent must name the feature, got: {created}"
        );
    }

    // RocketMQ sink: validated without opening any socket.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "rmq-1",
                       "kind": "rocketmq",
                       "config": {"endpoints": ["127.0.0.1:8081"],
                                  "topic": "rocket-telemetry",
                                  "tag_template": "${topic_segment_2}",
                                  "access_key": "rocket-key",
                                  "secret_key": "rocket-secret",
                                  "batch_size": 128}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("rocketmq"));

    // Oracle sink: validated without touching ORDS.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "oracle-1",
                       "kind": "oracle",
                       "config": {"url": "https://oracle-host:8080/ords/admin/_/sql",
                                  "schema": "ADMIN",
                                  "table": "telemetry",
                                  "username": "admin",
                                  "password": "secret",
                                  "key_columns": ["device_id"],
                                  "batch_size": 500}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("oracle"));

    // CockroachDB sink: validated without touching CockroachDB.
    let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "cockroach-1",
                       "kind": "cockroachdb",
                       "config": {"connection_string": "postgresql://root@127.0.0.1:26257/defaultdb",
                                  "table": "telemetry",
                                  "upsert_conflict_columns": ["device_id"],
                                  "batch_size": 500,
                                  "max_retry_attempts": 5}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("cockroachdb"));

    // AlloyDB sink: validated without touching AlloyDB.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "alloydb-1",
                       "kind": "alloydb",
                       "config": {"host": "10.0.0.1",
                                  "port": 5432,
                                  "database": "telemetry",
                                  "username": "postgres",
                                  "auth": {"type": "password", "password": "secret"},
                                  "table": "readings",
                                  "batch_size": 1000}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("alloydb"));

    // OpenTSDB sink: validated without touching OpenTSDB.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "opentsdb-1",
                       "kind": "opentsdb",
                       "config": {"endpoint": "http://127.0.0.1:4242",
                                  "protocol": "http",
                                  "metric_template": "sensor.temp",
                                  "value_field": "value",
                                  "summary": true,
                                  "compression": "none",
                                  "batch_size": 1000}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("opentsdb"));

    // GreptimeDB sink: validated without touching GreptimeDB.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "greptimedb-1",
                       "kind": "greptimedb",
                       "config": {"endpoint": "http://127.0.0.1:4000",
                                  "database": "public",
                                  "format": "sql_insert",
                                  "table_template": "sensor_readings",
                                  "timestamp_precision": "millisecond",
                                  "batch_size": 1000}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("greptimedb"));

    // Datalayers sink: validated without touching Datalayers.
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "datalayers-1",
                       "kind": "datalayers",
                       "config": {"endpoint": "http://127.0.0.1:8360",
                                  "database": "telemetry",
                                  "table": "metrics",
                                  "auth_token": "secret-token",
                                  "batch_size": 500}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["kind"], json!("datalayers"));

    let (status, body) = server.get_auth("/api/v1/connectors", &token).await;
    assert_eq!(status, 200);
    // X1-09/X1-10: each gated kind is present only when its Cargo
    // feature is enabled; otherwise creation above failed closed and
    // the list omits them. Assert presence conditionally.
    {
        let list = body.as_array().expect("connector list");
        let ids: std::collections::HashSet<&str> =
            list.iter().filter_map(|e| e["id"].as_str()).collect();
        for always in [
            "alloydb-1",
            "bridge-1",
            "cockroach-1",
            "databricks-1",
            "datalayers-1",
            "diag",
            "disk-1",
            "doris-1",
            "greptimedb-1",
            "hook-1",
            "influx-sink-1",
            "iotdb-1",
            "mssql-1",
            "mysql-sink-1",
            "oci-1",
            "opcua-1",
            "opentsdb-1",
            "oracle-1",
            "ots-1",
            "pg-sink-1",
            "pulsar-1",
            "redis-sink-1",
            "redshift-1",
            "rmq-1",
            "snowflake-1",
            "spb-1",
            "tdengine-1",
            "timestream-1",
            "ts-sink-1",
        ] {
            assert!(ids.contains(always), "connector {always} must be listed");
        }
        for (id, enabled) in [
            ("s3-sink-1", cfg!(feature = "s3")),
            ("azure-1", cfg!(feature = "azure_eventhubs")),
            ("dynamodb-1", cfg!(feature = "dynamodb")),
            ("bigquery-1", cfg!(feature = "bigquery")),
            ("aws-iot-1", cfg!(feature = "aws_iot")),
            ("azure-iot-1", cfg!(feature = "azure_iot")),
            ("gcp-iot-1", cfg!(feature = "gcp_iot")),
            ("azblob-1", cfg!(feature = "azure_blob")),
            ("kafka-sink-1", cfg!(feature = "kafka")),
            ("rabbit-sink-1", cfg!(feature = "rabbitmq")),
            ("ch-sink-1", cfg!(feature = "clickhouse")),
            ("es-sink-1", cfg!(feature = "elasticsearch")),
            ("gcp-1", cfg!(feature = "gcp_pubsub")),
            ("mongo-1", cfg!(feature = "mongodb")),
            ("cassandra-1", cfg!(feature = "cassandra")),
            ("couchbase-1", cfg!(feature = "couchbase")),
            ("confluent-1", cfg!(feature = "confluent")),
            ("kinesis-1", cfg!(feature = "kinesis")),
        ] {
            assert_eq!(ids.contains(id), enabled, "gated connector {id} presence");
        }
        let expected = 29
            + (cfg!(feature = "s3") as usize)
            + (cfg!(feature = "azure_eventhubs") as usize)
            + (cfg!(feature = "dynamodb") as usize)
            + (cfg!(feature = "bigquery") as usize)
            + (cfg!(feature = "aws_iot") as usize)
            + (cfg!(feature = "azure_iot") as usize)
            + (cfg!(feature = "gcp_iot") as usize)
            + (cfg!(feature = "azure_blob") as usize)
            + (cfg!(feature = "kafka") as usize)
            + (cfg!(feature = "rabbitmq") as usize)
            + (cfg!(feature = "clickhouse") as usize)
            + (cfg!(feature = "elasticsearch") as usize)
            + (cfg!(feature = "gcp_pubsub") as usize)
            + (cfg!(feature = "mongodb") as usize)
            + (cfg!(feature = "cassandra") as usize)
            + (cfg!(feature = "couchbase") as usize)
            + (cfg!(feature = "confluent") as usize)
            + (cfg!(feature = "kinesis") as usize);
        assert_eq!(list.len(), expected, "connector count");
    }

    // Unknown kinds and invalid configs are 400s that store nothing.
    // Empty exchange is valid. It selects the default exchange.
    // See #components/schemas/bridge_rabbitmq.action_parameters.
    // Old code refused empty exchange with 400. New code accepts it.
    for payload in [
        json!({"id": "x", "kind": "pigeon", "config": {}}),
        json!({"id": "", "kind": "logger", "config": {}}),
        json!({"id": "bad-k", "kind": "kafka",
                   "config": {"bootstrap_servers": "", "topic_template": "t",
                              "client_id": "c", "acks": "all"}}),
        json!({"id": "bad-pg", "kind": "postgres",
                   "config": {"connection_url": "postgresql://h/db",
                              "sql_template": "INSERT INTO t VALUES ($9)"}}),
        json!({"id": "bad-redis", "kind": "redis",
                   "config": {"endpoint": "redis://h",
                              "command": {"command": "set", "key_template": ""}}}),
        json!({"id": "bad-mysql", "kind": "mysql",
                   "config": {"connection_url": "mysql://u:p@h/db",
                              "sql_template": "INSERT INTO t VALUES (?, ?)",
                              "pool_size": 1, "batch_size": 10, "batch_timeout_ms": 10}}),
        json!({"id": "bad-ch", "kind": "clickhouse",
                   "config": {"endpoint": "http://h:8123",
                              "database": "db; DROP TABLE x;",
                              "table": "t",
                              "format": "JSONEachRow",
                              "batch_size": 10, "batch_timeout_ms": 10}}),
        json!({"id": "bad-influx", "kind": "influxdb",
                   "config": {"endpoint": "http://h:8086",
                              "bucket": "b", "org": "o", "token": "",
                              "measurement_template": "m", "precision": "ms"}}),
        json!({"id": "bad-s3", "kind": "s3",
                   "config": {"endpoint": "http://h:9000",
                              "bucket": "UPPERCASE",
                              "key_template": "t/${topic}.ndjson"}}),
        json!({"id": "bad-es", "kind": "elasticsearch",
                   "config": {"endpoint": "http://h:9200",
                              "index_template": "-leading",
                              "auth": {"type": "none"}}}),
        json!({"id": "bad-ts", "kind": "timescaledb",
                   "config": {"connection_url": "postgresql://u:p@h/db",
                              "hypertable": "m",
                              "sql_template": "INSERT INTO m VALUES ($1, $2)"}}),
        json!({"id": "bad-hook", "kind": "webhook",
                   "config": {"url": "ftp://hooks.example.com/x"}}),
        json!({"id": "bad-bridge", "kind": "mqtt_bridge",
                   "config": {"broker_address": "mqtt://h:1883",
                              "client_id": "b",
                              "qos_override": 5}}),
        json!({"id": "bad-disk", "kind": "disk_log",
                   "config": {"directory": "",
                              "format": "ndjson"}}),
        json!({"id": "bad-spb", "kind": "sparkplug_b",
                   "config": {"tier": "community"}}),
        json!({"id": "bad-kinesis", "kind": "kinesis",
                   "config": {"stream_name": "",
                              "region": "us-east-1",
                              "access_key_id": "AKID",
                              "secret_access_key": "secret"}}),
        json!({"id": "bad-gcp", "kind": "gcp_pubsub",
                   "config": {"project_id": "UPPER",
                              "topic_id": "telemetry-events",
                              "auth": {"type": "none"}}}),
        json!({"id": "bad-azure", "kind": "azure_eventhubs",
                   "config": {"namespace": "my-eventhub-ns",
                              "event_hub": "telemetry-hub",
                              "shared_access_key_name": "",
                              "shared_access_key": "secret"}}),
        json!({"id": "bad-pulsar", "kind": "pulsar",
                   "config": {"service_url": "pulsar://127.0.0.1:6650",
                              "tenant": "public",
                              "namespace": "default",
                              "topic": "",
                              "auth": {"type": "none"}}}),
        json!({"id": "bad-mongo", "kind": "mongodb",
                   "config": {"connection_string": "mongodb://127.0.0.1:27017",
                              "database": "",
                              "collection_template": "readings",
                              "operation": {"type": "insert_one"}}}),
        json!({"id": "bad-mssql", "kind": "mssql",
                   "config": {"host": "",
                              "database": "telemetry",
                              "table_template": "dbo.T",
                              "auth": {"type": "integrated"},
                              "query_mode": {"mode": "insertjson"}}}),
        json!({"id": "bad-cassandra", "kind": "cassandra",
                   "config": {"contact_points": [],
                              "keyspace": "telemetry",
                              "table_template": "events",
                              "partition_key_template": "${topic}",
                              "cql_statement_template": "INSERT INTO t VALUES (?, ?, ?, ?)"}}),
        json!({"id": "bad-couchbase", "kind": "couchbase",
                   "config": {"connection_string": "couchbase://127.0.0.1",
                              "bucket": "telemetry",
                              "auth": {"username": "", "password": "secret"},
                              "doc_id_template": "k",
                              "operation": "upsert"}}),
        json!({"id": "bad-tdengine", "kind": "tdengine",
                   "config": {"endpoint": "http://127.0.0.1:6041/rest/sql",
                              "database": "has space",
                              "stable_name": "meters",
                              "subtable_template": "d_${client_id}"}}),
        json!({"id": "bad-iotdb", "kind": "iotdb",
                   "config": {"endpoint": "http://127.0.0.1:18080/rest/v2",
                              "device_path_template": "factory.plant1",
                              "auth": {"username": "root", "password": "root"},
                              "measurements": ["temperature"],
                              "data_types": ["DOUBLE"]}}),
        json!({"id": "bad-timestream", "kind": "timestream",
                   "config": {"database_name": "iot_database",
                              "table_name": "",
                              "region": "us-east-1",
                              "access_key_id": "AKID",
                              "secret_access_key": "secret"}}),
        json!({"id": "bad-dynamodb", "kind": "dynamodb",
                   "config": {"table_name": "telemetry_table",
                              "region": "us-east-1",
                              "access_key_id": "AKID",
                              "secret_access_key": "secret",
                              "partition_key": {"name": "", "template": "${client_id}", "key_type": "S"}}}),
        json!({"id": "bad-snowflake", "kind": "snowflake",
                   "config": {"account": "",
                              "user": "u",
                              "database": "IOT",
                              "schema": "PUBLIC",
                              "table_template": "T",
                              "private_key_pem": "not-a-key"}}),
        json!({"id": "bad-databricks", "kind": "databricks",
                   "config": {"host": "https://host/path",
                              "token": "tok",
                              "table_template": "t"}}),
        json!({"id": "bad-doris", "kind": "doris",
                   "config": {"fe_host": "127.0.0.1",
                              "database": "has space",
                              "table_template": "events",
                              "auth": {"username": "root", "password": ""}}}),
        json!({"id": "bad-bigquery", "kind": "bigquery",
                   "config": {"project_id": "my-iot-project",
                              "dataset_id": "telemetry",
                               "table_template": "9lives",
                               "auth": {"type": "none"}}}),
        json!({"id": "bad-redshift", "kind": "redshift",
                   "config": {"database": "analytics",
                              "table_template": "t",
                              "region": "us-east-1",
                               "access_key_id": "AKID",
                               "secret_access_key": "secret"}}),
        json!({"id": "bad-azblob", "kind": "azure_blob",
                   "config": {"account_name": "AB",
                              "container_name": "telemetry",
                              "auth": {"type": "sharedkey",
                                        "account_key": "a2V5"},
                              "blob_path_template": "t/${batch_id}.json"}}),
        json!({"id": "bad-ots", "kind": "tablestore",
                   "config": {"endpoint": "https://i.cn-hangzhou.ots.aliyuncs.com",
                              "instance_name": "i",
                              "table_name": "t",
                              "access_key_id": "k",
                              "access_key_secret": "s",
                              "primary_keys": []}}),
        json!({"id": "bad-confluent", "kind": "confluent",
                   "config": {"bootstrap_servers": [],
                              "api_key": "k",
                              "api_secret": "s",
                              "topic_template": "t"}}),
        json!({"id": "bad-rmq", "kind": "rocketmq",
                   "config": {"endpoints": ["127.0.0.1:8081"],
                              "topic": ""}}),
        json!({"id": "bad-ora", "kind": "oracle",
                   "config": {"url": "https://oracle-host:8080/ords/admin/_/sql",
                              "schema": "",
                              "table": "telemetry",
                              "username": "admin",
                              "password": "secret",
                              "key_columns": ["device_id"]}}),
        json!({"id": "bad-crdb", "kind": "cockroachdb",
                   "config": {"connection_string": "postgresql://root@127.0.0.1:26257/defaultdb",
                              "table": "",
                              "upsert_conflict_columns": ["device_id"]}}),
        json!({"id": "bad-alloy", "kind": "alloydb",
                   "config": {"host": "",
                              "database": "telemetry",
                              "username": "postgres",
                              "table": "readings"}}),
        json!({"id": "bad-opentsdb", "kind": "opentsdb",
                   "config": {"endpoint": "",
                              "metric_template": "sensor.temp",
                              "value_field": "value"}}),
        json!({"id": "bad-greptimedb", "kind": "greptimedb",
                   "config": {"endpoint": "http://127.0.0.1:4000",
                              "database": "",
                              "table_template": "sensor_readings"}}),
        json!({"id": "bad-datalayers", "kind": "datalayers",
                   "config": {"endpoint": "",
                              "database": "telemetry",
                              "table": "metrics"}}),
    ] {
        let (status, _) = server
            .post_auth("/api/v1/connectors", payload, &token)
            .await;
        assert_eq!(status, 400);
    }
    let (status, body) = server.get_auth("/api/v1/connectors", &token).await;
    assert_eq!(status, 200);
    // X1-09/X1-10: count follows the enabled gated features (29 always
    // plus each gated kind when its feature is on).
    let expected = 29
        + (cfg!(feature = "s3") as usize)
        + (cfg!(feature = "azure_eventhubs") as usize)
        + (cfg!(feature = "dynamodb") as usize)
        + (cfg!(feature = "bigquery") as usize)
        + (cfg!(feature = "aws_iot") as usize)
        + (cfg!(feature = "azure_iot") as usize)
        + (cfg!(feature = "gcp_iot") as usize)
        + (cfg!(feature = "azure_blob") as usize)
        + (cfg!(feature = "kafka") as usize)
        + (cfg!(feature = "rabbitmq") as usize)
        + (cfg!(feature = "clickhouse") as usize)
        + (cfg!(feature = "elasticsearch") as usize)
        + (cfg!(feature = "gcp_pubsub") as usize)
        + (cfg!(feature = "mongodb") as usize)
        + (cfg!(feature = "cassandra") as usize)
        + (cfg!(feature = "couchbase") as usize)
        + (cfg!(feature = "confluent") as usize)
        + (cfg!(feature = "kinesis") as usize);
    assert_eq!(body.as_array().expect("list").len(), expected);
}

#[tokio::test]
async fn mv02d_v1_tablestore_create_without_measurement_is_400() {
    // MV-02d: a v1 tablestore create without `measurement` is a 400
    // that names `measurement`. No default is invented.
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, body) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "ots-no-measurement",
                       "kind": "tablestore",
                       "config": {"endpoint": "https://test-instance.cn-hangzhou.ots.aliyuncs.com",
                                  "instance_name": "test-instance",
                                  "table_name": "telemetry",
                                  "access_key_id": "test-key-id",
                                  "access_key_secret": "test-secret",
                                  "primary_keys": [{"name": "device_id",
                                                    "source": "${client_id}",
                                                    "data_type": "string"}],
                                  "attribute_columns": [{"name": "temperature",
                                                         "source": "${payload.temperature}",
                                                         "data_type": "double"}],
                                  "batch_size": 200}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a missing measurement refuses: {body}");
    assert!(
        body["error"].as_str().unwrap_or("").contains("measurement"),
        "the 400 names measurement, got: {body}"
    );
}

#[tokio::test]
async fn mv02d_v1_tablestore_create_with_non_object_config_is_400() {
    // MV-02d: a v1 tablestore create with a non-object `config` is a
    // 400. The handler never indexes the client JSON first.
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, _) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "ots-bad-config",
                       "kind": "tablestore",
                       "config": "not-an-object"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a non-object config refuses");
}

#[tokio::test]
async fn s101_no_default_credentials_rejects_and_accepts() {
    // S1-01: a connector without its required credential is rejected
    // with the missing field named, and registers nothing.
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    for (name, body, field) in [
        (
            "s101-gcp-no-key",
            json!({"name": "s101-gcp-no-key", "type": "gcp_iot",
                       "project_id": "e2e-project", "cloud_region": "us-central1",
                       "registry_id": "e2e-registry", "device_id": "e2e-device"}),
            "private_key_pem",
        ),
        (
            "s101-snow-no-key",
            json!({"name": "s101-snow-no-key", "type": "snowflake",
                       "account": "xy12345.us-east-1", "user": "indra_loader",
                       "database": "IOT", "schema": "PUBLIC", "table": "TELEMETRY"}),
            "private_key_pem",
        ),
        (
            "s101-oci-no-key",
            json!({"name": "s101-oci-no-key", "type": "oci_streaming",
                       "stream_pool_id": "ocid1.streampool.oc1..testpool",
                       "stream_id": "ocid1.stream.oc1..teststream",
                       "tenancy_ocid": "ocid1.tenancy.oc1..test",
                       "user_ocid": "ocid1.user.oc1..test",
                       "fingerprint": "20:3b:97:13:aa:bb:cc:dd:ee:ff:00:11:22:33:44:55"}),
            "private_key_pem",
        ),
        (
            "s101-db-no-token",
            json!({"name": "s101-db-no-token", "type": "databricks",
                       "host": "my-workspace.cloud.databricks.com",
                       "catalog": "main", "schema": "default", "table": "events"}),
            "token",
        ),
        (
            "s101-ots-no-key",
            json!({"name": "s101-ots-no-key", "type": "tablestore",
                       "instance_name": "test-instance-1", "table_name": "sensor_data",
                       "access_key_id": "test-ak-1"}),
            "access_key_secret",
        ),
        (
            "s101-conf-no-key",
            json!({"name": "s101-conf-no-key", "type": "confluent",
                       "bootstrap_servers": "127.0.0.1:9092",
                       "topic": "telemetry-events"}),
            "api_key",
        ),
        (
            "s101-eh-no-key",
            json!({"name": "s101-eh-no-key", "type": "azure_eventhubs",
                       "namespace": "s101ns", "event_hub": "s101hub"}),
            "shared_access_key",
        ),
        (
            "s101-aiot-no-key",
            json!({"name": "s101-aiot-no-key", "type": "azure_iot",
                       "iot_hub_name": "127.0.0.1:8883",
                       "device_id": "s101-device"}),
            "shared_access_key",
        ),
    ] {
        let (status, body) = server.post_auth("/api/v5/connectors", body, &token).await;
        assert_eq!(status, 400, "missing credential for {name}: {body}");
        assert!(
            body["message"].as_str().unwrap_or("").contains(field),
            "missing {field} must be named for {name}, got: {body}"
        );
        assert!(
            state.engine.connectors().get(name).is_none(),
            "{name} registers no sink"
        );
    }

    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "s101-ready", "type": "pgsql"}),
            &token,
        )
        .await;
    assert_eq!(status, 201, "ready kind still stores: {body}");
    assert!(state.engine.connectors().get("s101-ready").is_some());
    let (status, _) = server
        .delete_auth("/api/v5/connectors/s101-ready", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn cf05_v5_connector_create_refuses_sink_build_failure() {
    // CF-05: the v5 create path refuses a kind that is not ready. The
    // v1 path is unchanged and still runs each sink builder and
    // refuses its bad config with the builder text. MV-03a sets the
    // message-broker kinds ready and MV-03c sets `http` ready, so a
    // pending kind and a removed kind prove the refusal here.
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    for (name, kind) in [
        ("cf05-timescaledb-pending", "timescaledb"),
        ("cf05-s3tables-removed", "s3_tables"),
    ] {
        let (status, body) = server
                .post_auth(
                    "/api/v5/connectors",
                    json!({"name": name, "type": kind, "endpoint": "http://127.0.0.1:9",
                           "account": "xy12345.us-east-1", "user": "indra_loader",
                           "database": "IOT", "schema": "PUBLIC", "table": "TELEMETRY",
                           "private_key_pem": "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----"}),
                    &token,
                )
                .await;
        assert_eq!(status, 400, "{kind} create must be refused: {body}");
        assert_eq!(body["code"], json!("ALREADY_EXISTS"), "{kind}");
        assert!(
            body["message"]
                .as_str()
                .unwrap_or("")
                .contains("not available in this broker"),
            "{kind} refusal names the kind: {body}"
        );
        assert!(state.engine.connectors().get(name).is_none());
    }

    // The native entry still runs each sink builder and refuses its
    // bad config with the builder text.
    for (id, kind, config, text) in [
        (
            "cf05-dl-conflict",
            "datalayers",
            json!({"endpoint": "http://127.0.0.1:8360",
                       "database": "factory_db", "table": "sensor_events",
                       "auth_token": "cf05-token",
                       "username": "cf05-user", "password": "cf05-pass"}),
            "auth modes conflict",
        ),
        (
            "cf05-dl-half",
            "datalayers",
            json!({"endpoint": "http://127.0.0.1:8360",
                       "database": "factory_db", "table": "sensor_events",
                       "username": "cf05-user"}),
            "both username and password",
        ),
        (
            "cf05-greptime-empty",
            "greptimedb",
            json!({"endpoint": "http://127.0.0.1:4000/v1", "database": "",
                       "table_template": "events"}),
            "database",
        ),
        (
            "cf05-opentsdb-empty",
            "opentsdb",
            json!({"endpoint": "http://127.0.0.1:4242",
                       "metric_template": "", "value_field": "value"}),
            "metric",
        ),
    ] {
        let (status, refused) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": id, "kind": kind, "config": config}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "{id} builder must refuse, got: {refused}");
        assert!(
            refused["error"].as_str().unwrap_or("").contains(text),
            "{id} refusal carries the sink text, got: {refused}"
        );
    }

    // A refused update keeps the stored ready record.
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "cf05-keep", "type": "webhook", "url": "http://127.0.0.1:9"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, body) = server
        .put_auth(
            "/api/v5/connectors/cf05-keep",
            json!({"name": "cf05-keep", "type": "datalayers",
                       "endpoint": "http://127.0.0.1:8360"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "pending replace must fail, got: {body}");
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, kept) = server
        .get_auth("/api/v5/connectors/cf05-keep", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(kept["type"], json!("webhook"));
    assert_eq!(kept["url"], json!("http://127.0.0.1:9"));
    let (status, _) = server
        .delete_auth("/api/v5/connectors/cf05-keep", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn cf05_v5_connector_valid_but_unreachable_stays_stored() {
    // CF-05: a valid sink whose server is down still stores. After
    // a restart the boot probe finds no target, so the entry reads
    // back disconnected instead of failing the create.
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("cf05-conn-down");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let (status, created) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "cf05-dl-down", "type": "pgsql",
                       "server": "127.0.0.1:9", "database": "factory_db",
                       "table": "sensor_events"}),
            &token,
        )
        .await;
    assert_eq!(
        status, 201,
        "valid sink stores even when down, got: {created}"
    );
    drop(server);
    drop(registry);

    // Simulate a kernel restart from the same dir.
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, state) = TestServer::start_with_registry(reloaded).await;
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    let token = body["token"].as_str().expect("admin token").to_string();
    let (status, read) = server
        .get_auth("/api/v5/connectors/cf05-dl-down", &token)
        .await;
    assert_eq!(status, 200, "stored entry survives the restart");
    assert_eq!(read["status"], json!("disconnected"));
    assert!(
        state.engine.connectors().get("cf05-dl-down").is_none(),
        "no live sink while the target stays down"
    );
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}
