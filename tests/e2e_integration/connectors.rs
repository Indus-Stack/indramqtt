use super::*;

/// Two actions on one connector each build their own sink and route to
/// their own parameter shape; a delete stops only the deleted action.
#[tokio::test]
async fn mv01_webhook_action_sinks_route_independently_and_stop_on_delete() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 2);
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-webhook",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    for (id, path) in [("mv01-a", "/a"), ("mv01-b", "/b")] {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/actions",
            serde_json::json!({
                "name": id,
                "type": "webhook",
                "connector": "mv01-webhook",
                "parameters": {"path": path},
            }),
        )
        .await;
        assert_eq!(status, 201, "action {id} create: {body}");
        assert_eq!(
            body["status"], "connected",
            "action {id} reads live: {body}"
        );
        assert!(body["status_reason"].is_null(), "action {id}: {body}");
    }
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-rule",
            "sql": "SELECT * FROM \"mv01/in/1\"",
            "actions": ["mv01-a", "mv01-b"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/1", 0, 0, false, br#"{"k":1}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    let mut paths: Vec<&str> = received.iter().map(|(path, _)| path.as_str()).collect();
    paths.sort_unstable();
    assert_eq!(
        paths,
        vec!["/a", "/b"],
        "both action sinks routed: {received:?}"
    );
    for (_, body) in &received {
        assert_eq!(body.as_slice(), &br#"{"k":1}"#[..]);
    }
    // A rule that still names the action holds the delete back.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/mv01-a",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 400, "referenced action delete: {body}");
    // Remove the action from the rule, then delete it.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/rules/mv01-rule",
        serde_json::json!({
            "sql": "SELECT * FROM \"mv01/in/1\"",
            "actions": ["mv01-b"],
        }),
    )
    .await;
    assert_eq!(status, 200, "rule update: {body}");
    // Deleting one action stops only that action's delivery.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/mv01-a",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "action delete: {body}");
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/1", 0, 0, false, br#"{"k":2}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 1, "only the surviving action delivers");
    let (path, body) = &received[0];
    assert_eq!(path, "/b");
    assert_eq!(body.as_slice(), &br#"{"k":2}"#[..]);
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-a",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 404, "deleted action reads absent: {body}");
    kernel.finish().await;
}

/// Each documented action alias routes to its canonical kind and the
/// action create answers 201.
#[tokio::test]
async fn alias_action_create_routes_to_canonical_kind() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let _receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 0);
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "alias-webhook",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");

    // `rest` is a documented alias of `http`/`webhook`.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "alias-rest",
            "type": "rest",
            "connector": "alias-webhook",
            "parameters": {"path": "/r"},
        }),
    )
    .await;
    assert_eq!(status, 201, "rest alias action create: {body}");

    // `logger` is a documented alias of `console`; no connector required.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "alias-logger",
            "type": "logger",
        }),
    )
    .await;
    assert_eq!(status, 201, "logger alias action create: {body}");

    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/alias-rest",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/alias-logger",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/connectors/alias-webhook",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    kernel.finish().await;
}

/// Every section-3 action alias is accepted through the HTTP API. Each
/// alias posts an action document with that `type` and succeeds; the
/// canonical kind owns the validation. Aliases of pending or refused
/// kinds are refused naming the canonical kind.
#[tokio::test]
async fn mn05_every_section3_alias_routes_through_http() {
    let kernel = spawn_kernel("").await;
    // (alias, canonical kind). The canonical kind owns the params.
    let aliases: &[(&str, &str)] = &[
        ("aws_timestream", "timestream"),
        ("opents", "opentsdb"),
        ("scylla", "cassandra"),
        ("scylladb", "cassandra"),
        ("cql", "cassandra"),
        ("greptime", "greptimedb"),
        ("ch", "clickhouse"),
        ("mongo", "mongodb"),
        ("documentdb", "mongodb"),
        ("couch", "couchbase"),
        ("cb", "couchbase"),
        ("elastic", "elasticsearch"),
        ("es", "elasticsearch"),
        ("opensearch", "elasticsearch"),
        ("sqlserver", "mssql"),
        ("mariadb", "mysql"),
        ("cockroach", "cockroachdb"),
        ("postgres", "pgsql"),
        ("postgresql", "pgsql"),
        ("google_alloydb", "alloydb"),
        ("alloy_db", "alloydb"),
        ("minio", "s3"),
        ("azure_blob_storage", "azure_blob"),
        ("dynamo", "dynamodb"),
        ("ddb", "dynamodb"),
        ("rabbit", "rabbitmq"),
        ("amqp", "rabbitmq"),
        ("bq", "bigquery"),
        ("gbq", "bigquery"),
        ("snowflake_streaming", "snowflake"),
        ("snowflake_aggregated", "snowflake"),
        ("ots", "tablestore"),
        ("delta_lake", "databricks"),
        ("opcua", "opc_ua"),
        ("sparkplug", "sparkplug_b"),
        ("spb", "sparkplug_b"),
        ("rest", "http"),
        ("logger", "console"),
        ("aws_kinesis", "kinesis"),
        ("gcp", "gcp_pubsub"),
        ("pubsub", "gcp_pubsub"),
        ("gcp_pubsub_producer", "gcp_pubsub"),
        ("gcp_pubsub_consumer", "gcp_pubsub"),
        ("azure_event_hubs", "azure_eventhubs"),
        ("azure", "azure_eventhubs"),
        ("eventhubs", "azure_eventhubs"),
        ("azure_event_hub_producer", "azure_eventhubs"),
        ("oci", "oci_streaming"),
        ("aws_iot_core", "aws_iot"),
        ("azure_iot_hub", "azure_iot"),
        ("azure_iothub", "azure_iot"),
        ("iothub", "azure_iot"),
        ("gcp_iot_core", "gcp_iot"),
        ("cloud_iot", "gcp_iot"),
        ("mqtt", "mqtt_bridge"),
        ("kafka_producer", "kafka"),
        ("kafka_consumer", "kafka"),
        ("confluent_producer", "confluent"),
        ("confluent_kafka", "confluent"),
        ("confluent_cloud", "confluent"),
    ];
    // Aliases whose canonical kind is pending or refused: the HTTP
    // refusal names the canonical kind.
    let pending_400: &[&str] = &[
        "scylla",
        "scylladb",
        "cql",
        "delta_lake",
        "gcp_iot_core",
        "cloud_iot",
        "aws_iot_core",
        "azure_iot_hub",
        "azure_iothub",
        "iothub",
        "oci",
    ];
    // Parameters that satisfy every canonical kind at once. The
    // connector dangles, so only the param gate runs; the sink never
    // builds and no server is needed.
    let full_params = || {
        serde_json::json!({
            "path": "/r",
            "sql": "SELECT 1",
            "bucket": "b", "key": "k",
            "container": "c", "blob": "bl",
            "table": "events", "hash_key": "id",
            "dataset": "d",
            "database": "db", "schema": "s", "pipe": "p",
            "table_name": "t", "measurement": "m",
            "fields": [{"column": "c", "value": "${payload.c}"}],
            "stream_name": "sn", "partition_key": "${client_id}",
            "pubsub_topic": "events", "topic": "events",
            "event_hub": "hub",
            "routing_key": "rk",
            "node_id": "ns=2;i=1",
            "topic_prefix": "spBv1.0/plant1",
            "action": "create", "index": "idx",
        })
    };
    for (alias, canonical) in aliases {
        let name = format!("mn05-alias-{alias}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/actions",
            serde_json::json!({
                "name": name,
                "type": alias,
                "connector": "mn05-no-such-connector",
                "parameters": full_params(),
            }),
        )
        .await;
        if pending_400.contains(alias) {
            assert_eq!(status, 400, "pending alias {alias} refuses: {body}");
            assert!(
                body["message"].as_str().unwrap_or("").contains(canonical),
                "pending alias {alias} refusal names {canonical}: {body}"
            );
        } else {
            assert_eq!(status, 201, "alias {alias} creates: {body}");
            assert_eq!(
                body["type"].as_str().unwrap_or(""),
                *alias,
                "alias {alias} echoes its type: {body}"
            );
            let (status, _) = mv00_api(
                &kernel,
                reqwest::Method::DELETE,
                &format!("/api/v5/actions/{name}"),
                serde_json::Value::Null,
            )
            .await;
            assert_eq!(status, 204);
        }
    }
    // Each canonical kind validates its own required field for its
    // aliases: without it the create is refused naming the field.
    // (alias, missing field).
    let negative: &[(&str, &str)] = &[
        ("couch", "sql"),
        ("elastic", "index"),
        ("sqlserver", "sql"),
        ("mariadb", "sql"),
        ("cockroach", "sql"),
        ("postgres", "sql"),
        ("google_alloydb", "sql"),
        ("minio", "bucket"),
        ("azure_blob_storage", "container"),
        ("dynamo", "table"),
        ("bq", "dataset"),
        ("snowflake_streaming", "database"),
        ("ots", "table_name"),
        ("opcua", "node_id"),
        ("aws_kinesis", "stream_name"),
        ("gcp", "pubsub_topic"),
        ("azure", "event_hub"),
        ("mqtt", "topic"),
        ("kafka_producer", "topic"),
        ("confluent_producer", "topic"),
        ("rabbit", "routing_key"),
    ];
    for (alias, field) in negative {
        let mut params = full_params();
        params
            .as_object_mut()
            .expect("params object")
            .remove(*field);
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/actions",
            serde_json::json!({
                "name": format!("mn05-alias-neg-{alias}"),
                "type": alias,
                "connector": "mn05-no-such-connector",
                "parameters": params,
            }),
        )
        .await;
        assert_eq!(status, 400, "alias {alias} without {field} refuses: {body}");
        assert!(
            body["message"].as_str().unwrap_or("").contains(field),
            "alias {alias} refusal names {field}: {body}"
        );
    }
    kernel.finish().await;
}

/// A `file:` or `env:` target on `POST /api/v5/connectors` resolves to its
/// real URL when the reference is readable, or answers 400 naming the field
/// when it is not.
#[tokio::test]
async fn file_env_connector_target_resolves_or_refusals() {
    // The spawned kernel inherits its environment at spawn, so set the
    // referenced variable before boot. The kernel then resolves it.
    std::env::set_var("MN05_CONNECTOR_URL", "http://127.0.0.1:9");
    let kernel = spawn_kernel("").await;

    // A readable file: URL resolves.
    let dir = std::env::temp_dir().join(format!("broker-mn05-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create dir");
    let path = dir.join("url");
    std::fs::write(&path, b"http://127.0.0.1:9\n").expect("write url");
    let file_ref = format!("file:{}", path.display());

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mn05-file-url",
            "type": "webhook",
            "url": file_ref,
        }),
    )
    .await;
    assert_eq!(status, 201, "file: connector create: {body}");

    // An unreadable file: URL is refused naming the field.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mn05-file-missing",
            "type": "webhook",
            "url": "file:/definitely/not/here-mn05",
        }),
    )
    .await;
    assert_eq!(status, 400, "missing file: connector create: {body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("url"),
        "refusal names url: {body}"
    );

    // An env: URL that is set resolves.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mn05-env-url",
            "type": "webhook",
            "url": "env:MN05_CONNECTOR_URL",
        }),
    )
    .await;
    assert_eq!(status, 201, "env: connector create: {body}");

    // An env: URL that is unset is refused naming the field.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mn05-env-missing",
            "type": "webhook",
            "url": "env:MN05_CONNECTOR_DEFINITELY_UNSET",
        }),
    )
    .await;
    assert_eq!(status, 400, "missing env: connector create: {body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("url"),
        "refusal names url: {body}"
    );

    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/connectors/mn05-file-url",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/connectors/mn05-env-url",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    std::fs::remove_dir_all(&dir).ok();
    std::env::remove_var("MN05_CONNECTOR_URL");
    kernel.finish().await;
}

/// A `file:` or `env:` target on `POST /api/v5/connectors_probe` resolves to
/// its real URL and is dialled (no `127.0.0.1:80` fallback). An unresolvable
/// reference is refused with 400 that names the field.
#[tokio::test]
async fn file_env_connector_probe_target_resolves() {
    // The spawned kernel inherits its environment at spawn, so set the
    // referenced variable before boot.
    std::env::set_var("MN05_PROBE_URL", "http://127.0.0.1:1");
    let kernel = spawn_kernel("").await;

    // A readable file: URL resolves and the probe dials it; nothing listens
    // on port 1 so the dial fails naming the resolved host and port.
    let dir = std::env::temp_dir().join(format!("broker-mn05p-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create dir");
    let path = dir.join("url");
    std::fs::write(&path, b"http://127.0.0.1:1\n").expect("write url");
    let file_ref = format!("file:{}", path.display());

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors_probe",
        serde_json::json!({
            "type": "webhook",
            "url": file_ref,
        }),
    )
    .await;
    assert_eq!(status, 400, "file: probe dials and fails: {body}");
    let message = body["message"].as_str().unwrap_or("");
    assert!(
        message.contains("127.0.0.1:1"),
        "file: probe dialled the resolved host and port: {body}"
    );
    assert!(
        !message.contains("127.0.0.1:80"),
        "file: probe never dialled a synthesised 127.0.0.1:80: {body}"
    );

    // An env: URL that is set resolves similarly.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors_probe",
        serde_json::json!({
            "type": "webhook",
            "url": "env:MN05_PROBE_URL",
        }),
    )
    .await;
    assert_eq!(status, 400, "env: probe dials and fails: {body}");
    let message = body["message"].as_str().unwrap_or("");
    assert!(
        message.contains("127.0.0.1:1"),
        "env: probe dialled the resolved host and port: {body}"
    );
    assert!(
        !message.contains("127.0.0.1:80"),
        "env: probe never dialled a synthesised 127.0.0.1:80: {body}"
    );

    // An unset env: reference resolves to nothing: 400 names the field.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors_probe",
        serde_json::json!({
            "type": "webhook",
            "url": "env:MN05_PROBE_DEFINITELY_UNSET",
        }),
    )
    .await;
    assert_eq!(status, 400, "unset env: probe refuses: {body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("url"),
        "unset env: refusal names url: {body}"
    );

    // An unreadable file: reference resolves to nothing: 400 names it.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors_probe",
        serde_json::json!({
            "type": "webhook",
            "url": "file:/definitely/not/here-mn05",
        }),
    )
    .await;
    assert_eq!(status, 400, "missing file: probe refuses: {body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("url"),
        "missing file: refusal names url: {body}"
    );
    std::env::remove_var("MN05_PROBE_URL");

    std::fs::remove_dir_all(&dir).ok();
    kernel.finish().await;
}

/// A bad service account key is refused through the management API. A
/// connector or action that carries `service_account_json` with bad JSON,
/// without `client_email`, or without `private_key` answers 400 that
/// names the field. A build without the cloud feature keeps the same
/// fail-closed refusal naming the feature.
#[tokio::test]
async fn mn05_bigquery_bad_key_refused_through_api() {
    let kernel = spawn_kernel("").await;

    // A valid attached-identity connector probes the build: 201 means
    // the cloud feature is on and the strict 400 checks run below.
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mn05-bq-probe",
            "type": "gcp_pubsub",
            "project_id": "demo",
            "authentication": {"type": "attached_service_account", "gcp_project_id": "p"},
        }),
    )
    .await;
    let strict = status == 201;
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/connectors/mn05-bq-probe",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);

    // Connector leg: each bad key shape is refused naming the field.
    for (leaf, raw) in [
        ("bad-json", "{not json".to_string()),
        (
            "missing-email",
            "{\"private_key\":\"pk\",\"project_id\":\"proj\"}".to_string(),
        ),
        (
            "missing-key",
            "{\"client_email\":\"a@b\",\"project_id\":\"proj\"}".to_string(),
        ),
    ] {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/connectors",
            serde_json::json!({
                "name": format!("mn05-bq-bad-{leaf}"),
                "type": "gcp_pubsub",
                "project_id": "demo",
                "authentication": {"type": "service_account_json", "service_account_json": raw},
            }),
        )
        .await;
        if strict {
            assert_eq!(status, 400, "bad {leaf} connector refuses: {body}");
            let message = body["message"].as_str().unwrap_or("");
            assert!(
                message.contains("authentication.service_account_json"),
                "bad {leaf} refusal names the key: {body}"
            );
            let want = match leaf {
                "missing-email" => "client_email",
                "missing-key" => "private_key",
                _ => "authentication.service_account_json",
            };
            assert!(
                message.contains(want),
                "bad {leaf} refusal names {want}: {body}"
            );
        } else {
            assert_ne!(
                status, 201,
                "bad {leaf} never stores without the feature: {body}"
            );
            assert!(
                body["message"]
                    .as_str()
                    .unwrap_or("")
                    .contains("gcp_pubsub"),
                "feature-off refusal names the kind: {body}"
            );
        }
    }

    // Action leg: a stored connector with a bad key fails the action.
    // Without the feature the connector itself is already refused.
    if strict {
        for (leaf, raw) in [
            ("bad-json", "{not json".to_string()),
            (
                "missing-email",
                "{\"private_key\":\"pk\",\"project_id\":\"proj\"}".to_string(),
            ),
            (
                "missing-key",
                "{\"client_email\":\"a@b\",\"project_id\":\"proj\"}".to_string(),
            ),
        ] {
            let connector = format!("mn05-bq-actconn-{leaf}");
            let (status, body) = mv00_api(
                &kernel,
                reqwest::Method::POST,
                "/api/v5/connectors",
                serde_json::json!({
                    "name": connector,
                    "type": "bigquery",
                    "project_id": "demo",
                    "authentication": {"type": "service_account_json", "service_account_json": raw},
                }),
            )
            .await;
            assert_eq!(
                status, 201,
                "bad {leaf} connector without dataset stores: {body}"
            );
            let (status, body) = mv00_api(
                &kernel,
                reqwest::Method::POST,
                "/api/v5/actions",
                serde_json::json!({
                    "name": format!("mn05-bq-act-{leaf}"),
                    "type": "bigquery",
                    "connector": connector,
                    "parameters": {"dataset": "d", "table": "events"},
                }),
            )
            .await;
            assert_eq!(status, 400, "bad {leaf} action refuses: {body}");
            let message = body["message"].as_str().unwrap_or("");
            assert!(
                message.contains("authentication.service_account_json"),
                "bad {leaf} action refusal names the key: {body}"
            );
            let want = match leaf {
                "missing-email" => "client_email",
                "missing-key" => "private_key",
                _ => "authentication.service_account_json",
            };
            assert!(
                message.contains(want),
                "bad {leaf} action refusal names {want}: {body}"
            );
            let (status, _) = mv00_api(
                &kernel,
                reqwest::Method::DELETE,
                &format!("/api/v5/connectors/{connector}"),
                serde_json::Value::Null,
            )
            .await;
            assert_eq!(status, 204);
        }
    }
    kernel.finish().await;
}

/// The probe dial uses the configured health timeout. Posting to the
/// connector and action probes with distinct
/// `resource_opts.health_check_timeout` values shows each value in the
/// reachability result; the documented 60 s default applies when the
/// field is absent (capped by the 5 s outer probe bound).
#[tokio::test]
async fn mn05_probe_uses_configured_health_timeout() {
    let kernel = spawn_kernel("").await;

    // A closed port fails fast and names the dialled target, proving
    // the probe entry accepts distinct timeout values.
    for (leg, timeout) in [("one", "1s"), ("two", "2s")] {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/connectors_probe",
            serde_json::json!({
                "type": "webhook",
                "url": "http://127.0.0.1:1",
                "resource_opts": {"health_check_timeout": timeout},
            }),
        )
        .await;
        assert_eq!(
            status, 400,
            "closed port with {timeout} refuses ({leg}): {body}"
        );
        assert!(
            body["message"]
                .as_str()
                .unwrap_or("")
                .contains("127.0.0.1:1"),
            "closed port with {timeout} dialled the target ({leg}): {body}"
        );
    }

    // An unroutable address hangs until the configured timeout, so the
    // timeout value is visible in the refusal message on both probes.
    for path in ["/api/v5/connectors_probe", "/api/v5/actions_probe"] {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            path,
            serde_json::json!({
                "type": "webhook",
                "url": "http://192.0.2.1:81",
                "resource_opts": {"health_check_timeout": "1s"},
            }),
        )
        .await;
        assert_eq!(status, 400, "{path} with 1s refuses: {body}");
        assert!(
            body["message"]
                .as_str()
                .unwrap_or("")
                .contains("timed out after 1s"),
            "{path} with 1s uses the 1s dial timeout: {body}"
        );
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            path,
            serde_json::json!({
                "type": "webhook",
                "url": "http://192.0.2.1:81",
                "resource_opts": {"health_check_timeout": "2s"},
            }),
        )
        .await;
        assert_eq!(status, 400, "{path} with 2s refuses: {body}");
        assert!(
            body["message"]
                .as_str()
                .unwrap_or("")
                .contains("timed out after 2s"),
            "{path} with 2s uses the 2s dial timeout: {body}"
        );
        // No field: the 60 s default applies, capped by the outer bound.
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            path,
            serde_json::json!({
                "type": "webhook",
                "url": "http://192.0.2.1:81",
            }),
        )
        .await;
        assert_eq!(status, 400, "{path} with default refuses: {body}");
        assert!(
            body["message"]
                .as_str()
                .unwrap_or("")
                .contains("timed out after 5s"),
            "{path} default timeout applies (outer 5s cap over 60s): {body}"
        );
    }
    kernel.finish().await;
}

/// An action whose connector does not resolve stores `disconnected` with
/// the named reason and starts delivering once the connector arrives —
/// creating the connector alone wires it.
#[tokio::test]
async fn mv01_http_action_dangling_connector_stores_disconnected_and_recovers() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-late",
            "type": "webhook",
            "connector": "webhook:mv01-late-link",
            "parameters": {"path": "/late"},
        }),
    )
    .await;
    assert_eq!(status, 201, "dangling action stores: {body}");
    assert_eq!(body["status"], "disconnected", "{body}");
    let reason = body["status_reason"].as_str().expect("status reason");
    assert!(
        reason.contains("mv01-late-link"),
        "reason names the missing connector: {reason}"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-late-link",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-late",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["status"], "connected",
        "the action wired with its connector: {body}"
    );
    assert!(body["status_reason"].is_null(), "{body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-late-rule",
            "sql": "SELECT * FROM \"mv01/in/late\"",
            "actions": ["mv01-late"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-late-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/late", 0, 0, false, br#"{"late":true}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    let (path, body) = &received[0];
    assert_eq!(path, "/late");
    assert_eq!(body.as_slice(), &br#"{"late":true}"#[..]);
    kernel.finish().await;
}

/// Disabling an action stops its delivery; re-enabling restores it.
#[tokio::test]
async fn mv01_webhook_action_disable_stops_delivery_and_toggle_restores() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-onoff",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-onoff-action",
            "type": "webhook",
            "connector": "mv01-onoff",
            "parameters": {"path": "/onoff"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-onoff-rule",
            "sql": "SELECT * FROM \"mv01/in/onoff\"",
            "actions": ["mv01-onoff-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-onoff-pub").await;
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/onoff", 0, 0, false, br#"{"e":1}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    let (path, body) = &received[0];
    assert_eq!(path, "/onoff");
    assert_eq!(body.as_slice(), &br#"{"e":1}"#[..]);
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-onoff-action/enable/false",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "disable: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-onoff-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "disconnected", "{body}");
    // A silenced receiver: nothing accepts while the action reads off.
    let watch = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/onoff", 0, 0, false, br#"{"e":2}"#),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(
        !watch.is_finished(),
        "a disabled action delivered: accept ended the promise"
    );
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-onoff-action/enable/true",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "re-enable");
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/onoff", 0, 0, false, br#"{"e":3}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, watch)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    let (path, body) = &received[0];
    assert_eq!(path, "/onoff");
    assert_eq!(body.as_slice(), &br#"{"e":3}"#[..]);
    kernel.finish().await;
}

/// A connector update that breaks a dependent action's sink build is
/// refused with the action and field named; the old sink keeps
/// delivering and the stored connector stays untouched.
#[tokio::test]
async fn mv01_connector_update_dependent_build_failure_refuses_and_keeps_delivery() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-break",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-break-action",
            "type": "webhook",
            "connector": "mv01-break",
            "parameters": {"path": "/break"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-break-rule",
            "sql": "SELECT * FROM \"mv01/in/break\"",
            "actions": ["mv01-break-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-break-pub").await;
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/break", 0, 0, false, br#"{"f":1}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received[0].1.as_slice(), &br#"{"f":1}"#[..]);
    // The URL only resolves through a file reference the environment
    // cannot load: the dependent build fails closed and the update is
    // refused with the action and field named.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/connectors/mv01-break",
        serde_json::json!({
            "name": "mv01-break",
            "type": "webhook",
            "url": "file:/nonexistent/indra-mv01-secret",
        }),
    )
    .await;
    assert_eq!(status, 400, "dependent build failure refuses: {body}");
    assert!(body["message"].is_string(), "{body}");
    let message = body["message"].as_str().expect("error message");
    assert!(
        message.contains("mv01-break-action") && message.contains("url"),
        "the refusal names the action and the field: {message}"
    );
    // The old generation kept delivering.
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/break", 0, 0, false, br#"{"f":2}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    let (path, body) = &received[0];
    assert_eq!(path, "/break");
    assert_eq!(body.as_slice(), &br#"{"f":2}"#[..]);
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-break-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["status"], "connected",
        "the refused update kept the wired action live: {body}"
    );
    kernel.finish().await;
}

/// A restart rebuilds the owned sinks from the stored connector settings
/// plus the stored action parameters and delivery resumes.
#[tokio::test]
async fn mv01_webhook_action_sink_survives_restart() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let mut kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-restart",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-restart-action",
            "type": "webhook",
            "connector": "mv01-restart",
            "parameters": {"path": "/after/restart"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-restart-rule",
            "sql": "SELECT * FROM \"mv01/in/restart\"",
            "actions": ["mv01-restart-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-restart-pub").await;
    let first = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/restart", 0, 0, false, br#"{"r":1}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, first)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    let (path, body) = &received[0];
    assert_eq!(path, "/after/restart");
    assert_eq!(body.as_slice(), &br#"{"r":1}"#[..]);
    mv00_restart(&mut kernel).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-restart-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "connected", "{body}");
    let mut publisher = mv00_connect(&kernel, "mv01-restart-pub2").await;
    let second = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/restart", 0, 0, false, br#"{"r":2}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, second)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    let (path, body) = &received[0];
    assert_eq!(path, "/after/restart");
    assert_eq!(body.as_slice(), &br#"{"r":2}"#[..]);
    kernel.finish().await;
}
