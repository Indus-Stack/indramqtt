use super::*;

/// RO-01: an action runtime applies its stored options through a real
/// publish, keeps its counters per action, resets every counter and
/// rebuilds the same runtime from the store after a restart.
#[tokio::test]
async fn ro01_action_runtime_applies_options_through_publish_and_restart() {
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
            "name": "ro01-restart",
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
            "name": "ro01-restart-action",
            "type": "webhook",
            "connector": "ro01-restart",
            "parameters": {"path": "/ro01"},
            "resource_opts": {
                "batch_size": 2,
                "batch_time": "5s",
                "inflight_window": 1,
                "worker_pool_size": 2,
                "max_buffer_bytes": "1MB",
                "request_ttl": "30s",
                "query_mode": "async",
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    assert_eq!(body["status"], "connected", "{body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-restart-rule",
            "sql": "SELECT * FROM \"ro01/in/restart\"",
            "actions": ["ro01-restart-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-restart-action/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["metrics"]["matched"], 0,
        "counters start at zero: {body}"
    );
    assert_eq!(body["metrics"]["success"], 0, "{body}");
    assert!(
        body["metrics"].get("retried").is_none(),
        "the invented retried counter is gone: {body}"
    );
    let mut publisher = mv00_connect(&kernel, "ro01-restart-pub").await;
    let mut first = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/restart", 0, 0, false, br#"{"r":1}"#),
    )
    .await;
    // The runtime holds one row until the batch is full or the batch time
    // elapses. A raw sink would deliver it now, so this assertion fails
    // when the runtime is absent.
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), &mut first)
            .await
            .is_err(),
        "one row waits in the bounded action buffer"
    );
    first.abort();
    let second = mv01_spawn_http_receiver(Arc::clone(&listener), 2);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/restart", 0, 0, false, br#"{"r":2}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, second)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 2, "the batch reached the target");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-restart-action/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["metrics"]["matched"], 2, "{body}");
    assert_eq!(body["metrics"]["success"], 2, "{body}");
    assert_eq!(body["metrics"]["failed"], 0, "{body}");
    assert_eq!(body["metrics"]["queuing"], 0, "{body}");
    assert_eq!(body["metrics"]["inflight"], 0, "{body}");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/ro01-restart-action/metrics/reset",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, 204);
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-restart-action/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    for counter in [
        "total",
        "matched",
        "success",
        "failed",
        "dropped",
        "dropped.queue_full",
        "dropped.resource_not_found",
        "dropped.resource_stopped",
        "dropped.other",
        "queuing",
        "inflight",
    ] {
        assert_eq!(
            body["metrics"][counter], 0,
            "reset zeroes {counter}: {body}"
        );
    }
    mv00_restart(&mut kernel).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-restart-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "connected", "{body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-restart-action/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["metrics"]["matched"], 0,
        "restart starts at zero: {body}"
    );
    let mut publisher = mv00_connect(&kernel, "ro01-restart-pub2").await;
    let mut first = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/restart", 0, 0, false, br#"{"r":3}"#),
    )
    .await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), &mut first)
            .await
            .is_err(),
        "the rebuilt runtime applies the stored batch size"
    );
    first.abort();
    let second = mv01_spawn_http_receiver(Arc::clone(&listener), 2);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/restart", 0, 0, false, br#"{"r":4}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, second)
        .await
        .expect("HTTP delivery after restart in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 2, "the rebuilt runtime delivered the batch");
    kernel.finish().await;
}

/// RO-01/N1: a connector update rebuilds the dependent action sink through
/// `wrap_action_runtime`, so the stored action options still apply.
#[tokio::test]
async fn ro01_connector_update_keeps_action_options() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let kernel = spawn_kernel("").await;
    let connector = serde_json::json!({
        "name": "ro01-update",
        "type": "webhook",
        "url": format!("http://{address}"),
    });
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        connector.clone(),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "ro01-update-action",
            "type": "webhook",
            "connector": "ro01-update",
            "parameters": {"path": "/ro01-update"},
            "resource_opts": {
                "batch_size": 2,
                "batch_time": "5s",
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-update-rule",
            "sql": "SELECT * FROM \"ro01/in/update\"",
            "actions": ["ro01-update-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/connectors/ro01-update",
        connector,
    )
    .await;
    assert_eq!(status, 200, "connector update: {body}");
    let mut publisher = mv00_connect(&kernel, "ro01-update-pub").await;
    let mut first = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/update", 0, 0, false, br#"{"u":1}"#),
    )
    .await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), &mut first)
            .await
            .is_err(),
        "the rebuilt sink still holds one row for the batch"
    );
    first.abort();
    let second = mv01_spawn_http_receiver(Arc::clone(&listener), 2);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/update", 0, 0, false, br#"{"u":2}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, second)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 2, "the rebuilt runtime delivered the batch");
    kernel.finish().await;
}

/// RO-01/N1/B13: a stored pgsql action with the legacy integer
/// `batch_timeout_ms` boots again and applies its batch size.
#[tokio::test]
async fn ro01_stored_pgsql_legacy_batch_timeout_boots() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pg fake binds");
    let address = listener.local_addr().expect("pg fake address");
    let captured = Arc::new(Mutex::new(Vec::<(String, Vec<Vec<Vec<u8>>>)>::new()));
    let fake = {
        let captured = Arc::clone(&captured);
        tokio::spawn(async move {
            let stream = mv01_pg_accept(&listener).await;
            mv01_pg_serve_one(stream, captured, "u", "pwd", "db", 2).await;
        })
    };
    let mut kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "ro01-legacy-pg",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "ro01-legacy-pg-action",
            "type": "pgsql",
            "connector": "ro01-legacy-pg",
            "parameters": {"sql": "RO01-LEGACY INSERT $1, $2, $3"},
            "resource_opts": {
                "batch_size": 2,
                "batch_timeout_ms": 5000,
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-legacy-rule",
            "sql": "SELECT * FROM \"ro01/in/legacy\"",
            "actions": ["ro01-legacy-pg-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    mv00_restart(&mut kernel).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-legacy-pg-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["status"], "connected",
        "the stored legacy action rebuilt at boot: {body}"
    );
    let mut publisher = mv00_connect(&kernel, "ro01-legacy-pub").await;
    for value in [1, 2] {
        let payload = format!("{{\"n\":{value}}}");
        send_mqtt(
            &mut publisher,
            &sy02_publish311("ro01/in/legacy", 0, 0, false, payload.as_bytes()),
        )
        .await;
    }
    fake.await.expect("pg fake task");
    let captured = std::sync::Mutex::into_inner(Arc::try_unwrap(captured).expect("sole owner"))
        .expect("capture lock");
    assert_eq!(captured.len(), 1, "one runtime batch reached the driver");
    assert_eq!(
        captured[0].1.len(),
        2,
        "the rebuilt runtime applied its stored batch size"
    );
    kernel.finish().await;
}

/// RO-01/P1: each applied resource option shows its effect through a real
/// publish. One kernel and one connector serve six sequential actions. Each
/// block would fail with a raw sink: the first rows would arrive at once,
/// the overflow would not count drops, and the expired row would deliver.
#[tokio::test]
async fn ro01_action_resource_opts_fields_apply_through_publish() {
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
            "name": "ro01-fields",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let mut publisher = mv00_connect(&kernel, "ro01-fields-pub").await;

    // `batch_time` flushes a partial batch after the wait.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "ro01-f-time",
            "type": "webhook",
            "connector": "ro01-fields",
            "parameters": {"path": "/f-time"},
            "resource_opts": {
                "batch_size": 100,
                "batch_time": "100ms",
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "time action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-f-time-rule",
            "sql": "SELECT * FROM \"ro01/in/f-time\"",
            "actions": ["ro01-f-time"],
        }),
    )
    .await;
    assert_eq!(status, 201, "time rule create: {body}");
    let mut hold = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/f-time", 0, 0, false, br#"{"n":1}"#),
    )
    .await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut hold)
            .await
            .is_err(),
        "one row waits for the batch time"
    );
    hold.abort();
    let delivered = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    let received = tokio::time::timeout(STEP, delivered)
        .await
        .expect("batch time delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 1, "the timer flushed the partial batch");
    assert_eq!(
        received[0].0, "/f-time",
        "the time action path: {received:?}"
    );
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/ro01-f-time-rule",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/ro01-f-time",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);

    // `max_buffer_bytes` drops the oldest rows and counts them.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "ro01-f-buf",
            "type": "webhook",
            "connector": "ro01-fields",
            "parameters": {"path": "/f-buf"},
            "resource_opts": {
                "batch_size": 100,
                "batch_time": "10s",
                "max_buffer_bytes": "20B",
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "buffer action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-f-buf-rule",
            "sql": "SELECT * FROM \"ro01/in/f-buf\"",
            "actions": ["ro01-f-buf"],
        }),
    )
    .await;
    assert_eq!(status, 201, "buffer rule create: {body}");
    let mut hold = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    for value in [1, 2, 3] {
        let payload = format!("{{\"n\":{value}}}");
        send_mqtt(
            &mut publisher,
            &sy02_publish311("ro01/in/f-buf", 0, 0, false, payload.as_bytes()),
        )
        .await;
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), &mut hold)
            .await
            .is_err(),
        "a full buffer holds rows until the batch is full"
    );
    hold.abort();
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-f-buf/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["metrics"]["matched"], 3, "{body}");
    assert!(
        body["metrics"]["dropped"].as_u64().unwrap_or(0) >= 1,
        "overflow drops are counted: {body}"
    );
    assert!(
        body["metrics"]["dropped.queue_full"].as_u64().unwrap_or(0) >= 1,
        "queue-full drops are counted: {body}"
    );
    let buffered = body["metrics"]["queuing"].as_u64().unwrap_or(0) as usize;
    let drained = mv01_spawn_http_receiver(Arc::clone(&listener), buffered);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/ro01-f-buf-rule",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/ro01-f-buf",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let drained = tokio::time::timeout(STEP, drained)
        .await
        .expect("delete drains the buffered rows in time")
        .expect("HTTP receiver task");
    assert_eq!(
        drained.len(),
        buffered,
        "delete delivers the buffered rows before the next block"
    );

    // `request_ttl` of zero expires every row before the send.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "ro01-f-ttl",
            "type": "webhook",
            "connector": "ro01-fields",
            "parameters": {"path": "/f-ttl"},
            "resource_opts": {
                "batch_size": 1,
                "batch_time": "0ms",
                "request_ttl": "0ms",
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "ttl action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-f-ttl-rule",
            "sql": "SELECT * FROM \"ro01/in/f-ttl\"",
            "actions": ["ro01-f-ttl"],
        }),
    )
    .await;
    assert_eq!(status, 201, "ttl rule create: {body}");
    let mut hold = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/f-ttl", 0, 0, false, br#"{"n":1}"#),
    )
    .await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(500), &mut hold)
            .await
            .is_err(),
        "an expired row never reaches the target"
    );
    hold.abort();
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-f-ttl/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["metrics"]["matched"], 1, "{body}");
    assert!(
        body["metrics"]["dropped"].as_u64().unwrap_or(0) >= 1,
        "expiry drops are counted: {body}"
    );
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/ro01-f-ttl-rule",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/ro01-f-ttl",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);

    // `inflight_window` of one still batches two rows before the send.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "ro01-f-inflight",
            "type": "webhook",
            "connector": "ro01-fields",
            "parameters": {"path": "/f-inflight"},
            "resource_opts": {
                "batch_size": 2,
                "batch_time": "5s",
                "inflight_window": 1,
                "worker_pool_size": 2,
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "inflight action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-f-inflight-rule",
            "sql": "SELECT * FROM \"ro01/in/f-inflight\"",
            "actions": ["ro01-f-inflight"],
        }),
    )
    .await;
    assert_eq!(status, 201, "inflight rule create: {body}");
    let mut hold = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/f-inflight", 0, 0, false, br#"{"n":1}"#),
    )
    .await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), &mut hold)
            .await
            .is_err(),
        "one row waits for the inflight batch"
    );
    hold.abort();
    let delivered = mv01_spawn_http_receiver(Arc::clone(&listener), 2);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/f-inflight", 0, 0, false, br#"{"n":2}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, delivered)
        .await
        .expect("inflight delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 2, "both rows reached the target");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-f-inflight/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["metrics"]["matched"], 2, "{body}");
    assert_eq!(body["metrics"]["success"], 2, "{body}");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/ro01-f-inflight-rule",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/ro01-f-inflight",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);

    // `query_mode` sync flushes one row before the publish returns.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "ro01-f-sync",
            "type": "webhook",
            "connector": "ro01-fields",
            "parameters": {"path": "/f-sync"},
            "resource_opts": {
                "batch_size": 100,
                "batch_time": "10s",
                "query_mode": "sync",
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "sync action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-f-sync-rule",
            "sql": "SELECT * FROM \"ro01/in/f-sync\"",
            "actions": ["ro01-f-sync"],
        }),
    )
    .await;
    assert_eq!(status, 201, "sync rule create: {body}");
    let delivered = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/f-sync", 0, 0, false, br#"{"n":1}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, delivered)
        .await
        .expect("sync delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 1, "sync flushes before it returns");
    assert_eq!(
        received[0].0, "/f-sync",
        "the sync action path: {received:?}"
    );
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/ro01-f-sync-rule",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/ro01-f-sync",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);

    // `worker_pool_size` of two still delivers a batch of four.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "ro01-f-worker",
            "type": "webhook",
            "connector": "ro01-fields",
            "parameters": {"path": "/f-worker"},
            "resource_opts": {
                "batch_size": 4,
                "batch_time": "5s",
                "worker_pool_size": 2,
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "worker action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-f-worker-rule",
            "sql": "SELECT * FROM \"ro01/in/f-worker\"",
            "actions": ["ro01-f-worker"],
        }),
    )
    .await;
    assert_eq!(status, 201, "worker rule create: {body}");
    let mut hold = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    for value in [1, 2, 3] {
        let payload = format!("{{\"n\":{value}}}");
        send_mqtt(
            &mut publisher,
            &sy02_publish311("ro01/in/f-worker", 0, 0, false, payload.as_bytes()),
        )
        .await;
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), &mut hold)
            .await
            .is_err(),
        "three rows wait for the worker batch"
    );
    hold.abort();
    let delivered = mv01_spawn_http_receiver(Arc::clone(&listener), 4);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/f-worker", 0, 0, false, br#"{"n":4}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, delivered)
        .await
        .expect("worker delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 4, "the worker batch reached the target");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-f-worker/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["metrics"]["matched"], 4, "{body}");
    assert_eq!(body["metrics"]["success"], 4, "{body}");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/ro01-f-worker-rule",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/ro01-f-worker",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    kernel.finish().await;
}

/// RO-01/A2-1/B2-1: `inflight_window` bounds the driver calls that run at
/// one time through a real publish. A window of two with one row per call
/// shows a peak of two concurrent HTTP requests. A sequential dispatch
/// shows one; an ignored window shows four.
#[tokio::test]
async fn ro01_inflight_window_bounds_concurrent_driver_calls_through_publish() {
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
            "name": "ro01-inflight-conn",
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
            "name": "ro01-inflight-action",
            "type": "webhook",
            "connector": "ro01-inflight-conn",
            "parameters": {"path": "/inflight"},
            "resource_opts": {
                "batch_size": 4,
                "batch_time": "5s",
                "inflight_window": 2,
                "worker_pool_size": 1,
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-inflight-rule",
            "sql": "SELECT * FROM \"ro01/in/inflight\"",
            "actions": ["ro01-inflight-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let receiver = mv01_spawn_http_receiver_active(
        Arc::clone(&listener),
        4,
        std::time::Duration::from_millis(500),
    );
    let mut publisher = mv00_connect(&kernel, "ro01-inflight-pub").await;
    for value in [1, 2, 3, 4] {
        let payload = format!("{{\"n\":{value}}}");
        send_mqtt(
            &mut publisher,
            &sy02_publish311("ro01/in/inflight", 0, 0, false, payload.as_bytes()),
        )
        .await;
    }
    let (received, peak) = tokio::time::timeout(STEP, receiver)
        .await
        .expect("inflight delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 4, "the batch reached the target");
    assert_eq!(
        peak, 2,
        "the inflight window bounds the concurrent driver calls"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-inflight-action/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["metrics"]["matched"], 4, "{body}");
    assert_eq!(body["metrics"]["success"], 4, "{body}");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/ro01-inflight-rule",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/ro01-inflight-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/connectors/ro01-inflight-conn",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    kernel.finish().await;
}

/// RO-01/B2-2: `worker_pool_size` caps one driver batch through a real
/// publish. Four rows with a worker size of two reach the driver as two
/// batches of two. An ignored worker size sends one batch of four.
#[tokio::test]
async fn ro01_worker_pool_size_caps_driver_batches_through_publish() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pg fake binds");
    let address = listener.local_addr().expect("pg fake address");
    let captured = Arc::new(Mutex::new(Vec::<(String, Vec<Vec<Vec<u8>>>)>::new()));
    let fake = {
        let captured = Arc::clone(&captured);
        tokio::spawn(async move {
            for _ in 0..2 {
                let stream = mv01_pg_accept(&listener).await;
                mv01_pg_serve_one(stream, Arc::clone(&captured), "u", "pwd", "db", 2).await;
            }
        })
    };
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "ro01-worker-pg",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "ro01-worker-action",
            "type": "pgsql",
            "connector": "ro01-worker-pg",
            "parameters": {"sql": "RO01-WORKER INSERT $1, $2, $3"},
            "resource_opts": {
                "batch_size": 4,
                "batch_time": "5s",
                "inflight_window": 1,
                "worker_pool_size": 2,
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-worker-rule",
            "sql": "SELECT * FROM \"ro01/in/worker\"",
            "actions": ["ro01-worker-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "ro01-worker-pub").await;
    for value in [1, 2, 3, 4] {
        let payload = format!("{{\"n\":{value}}}");
        send_mqtt(
            &mut publisher,
            &sy02_publish311("ro01/in/worker", 0, 0, false, payload.as_bytes()),
        )
        .await;
    }
    tokio::time::timeout(STEP, fake)
        .await
        .expect("both driver batches in time")
        .expect("pg fake task");
    let captured = std::sync::Mutex::into_inner(Arc::try_unwrap(captured).expect("sole owner"))
        .expect("capture lock");
    assert_eq!(
        captured.len(),
        2,
        "worker_pool_size two splits four rows into two driver batches"
    );
    assert!(
        captured.iter().all(|(_, rows)| rows.len() == 2),
        "each driver batch holds two rows"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-worker-action/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["metrics"]["matched"], 4, "{body}");
    assert_eq!(body["metrics"]["success"], 4, "{body}");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/ro01-worker-rule",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/ro01-worker-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/connectors/ro01-worker-pg",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    kernel.finish().await;
}

/// RO-01: each refused resource option answers 400 with the field name
/// and stores nothing through the management API.
#[tokio::test]
async fn ro01_action_resource_opts_refusals_through_the_api() {
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "ro01-refusal-hook",
            "type": "webhook",
            "url": "http://127.0.0.1:9",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let above_max = (tokio::sync::Semaphore::MAX_PERMITS as u64) + 1;
    let cases = [
        (
            "dispatch_strategy",
            serde_json::json!({"dispatch_strategy": "random"}),
        ),
        (
            "health_check_interval_jitter",
            serde_json::json!({"health_check_interval_jitter": "1s"}),
        ),
        ("no_such_option", serde_json::json!({"no_such_option": 1})),
        ("batch_time", serde_json::json!({"batch_time": "infinity"})),
        (
            "max_buffer_bytes",
            serde_json::json!({"max_buffer_bytes": "0"}),
        ),
        (
            "inflight_window",
            serde_json::json!({"inflight_window": above_max}),
        ),
    ];
    for (field, opts) in cases {
        let name = format!("ro01-refused-{field}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/actions",
            serde_json::json!({
                "name": name,
                "type": "webhook",
                "connector": "ro01-refusal-hook",
                "parameters": {"path": "/new"},
                "resource_opts": opts,
            }),
        )
        .await;
        assert_eq!(status, 400, "{field} is refused: {body}");
        assert!(
            body["message"].as_str().unwrap_or("").contains(field),
            "the refusal names {field}: {body}"
        );
        let (status, _) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/actions/{name}"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 404, "a refused create stores nothing: {field}");
    }
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/connectors/ro01-refusal-hook",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    kernel.finish().await;
}

// -- Fake PostgreSQL for the action-owned pgsql sink -----------------------

/// RO-01/B2-4: `inflight_window == 1` keeps publish order through dispatch.
/// Twenty QoS 1 messages published in order arrive at the HTTP receiver
/// in the same order; no reordering under a window of one.
#[tokio::test]
async fn ro01_inflight_window_one_keeps_publish_order() {
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
            "name": "ro01-order-conn",
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
            "name": "ro01-order-action",
            "type": "webhook",
            "connector": "ro01-order-conn",
            "parameters": {"path": "/order"},
            "resource_opts": {
                "inflight_window": 1,
                "worker_pool_size": 1,
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-order-rule",
            "sql": "SELECT * FROM \"ro01/in/order\"",
            "actions": ["ro01-order-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 20);
    let mut publisher = mv00_connect(&kernel, "ro01-order-pub").await;
    for i in 1..=20 {
        let payload = format!("{{\"n\":{i}}}");
        let publish = sy02_publish311("ro01/in/order", i, 1, false, payload.as_bytes());
        send_mqtt(&mut publisher, &publish).await;
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut publisher))
            .await
            .expect("PUBACK in time")
            .expect("PUBACK");
        assert_eq!(head, 0x40, "publisher sees PUBACK for message {i}");
        assert_eq!(
            body,
            vec![(i >> 8) as u8, (i & 0xff) as u8],
            "PUBACK mirrors packet id {i}"
        );
    }
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("ordered delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 20, "all 20 messages delivered");
    for (i, (path, body)) in received.iter().enumerate() {
        assert_eq!(path, "/order", "path for message {}", i + 1);
        let expected = format!("{{\"n\":{}}}", i + 1);
        assert_eq!(
            body,
            expected.as_bytes(),
            "message {} arrives in publish order",
            i + 1
        );
    }
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/ro01-order-rule",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/ro01-order-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/connectors/ro01-order-conn",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    kernel.finish().await;
}
