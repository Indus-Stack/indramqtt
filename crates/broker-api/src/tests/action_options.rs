use super::*;

#[tokio::test]
async fn action_resource_opts_apply_through_publish_and_metrics_report_them() {
    // RO-01: a console-style body with every documented default is
    // accepted. A non-default value that the action runtime cannot
    // apply is refused with the field named and stores nothing.
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (address, hits, receiver) = spawn_ro_counting_http_target().await;
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({
                "id": "ro-hook",
                "type": "webhook",
                "url": format!("http://{address}"),
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201, "webhook connector creates");
    let defaults = json!({
        "name": "ro-default-action",
        "type": "webhook",
        "connector": "ro-hook",
        "parameters": {"path": "/hook"},
        "resource_opts": {
            "batch_size": 1,
            "batch_time": "0ms",
            "dispatch_strategy": "per_clientid",
            "health_check_interval": "15s",
            "health_check_interval_jitter": "0ms",
            "health_check_timeout": "60s",
            "inflight_window": 100,
            "max_buffer_bytes": "256MB",
            "query_mode": "async",
            "request_ttl": "45s",
            "worker_pool_size": 16,
        },
    });
    let (status, body) = server.post_auth("/api/v5/actions", defaults, &token).await;
    assert_eq!(status, 201, "all-default resource_opts accepted: {body}");
    let (status, _) = server
        .delete_auth("/api/v5/actions/ro-default-action", &token)
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .post_auth(
            "/api/v5/actions",
            json!({
                "name": "ro-refused-action",
                "type": "webhook",
                "connector": "ro-hook",
                "parameters": {"path": "/hook"},
                "resource_opts": {"health_check_interval_jitter": "1s"},
            }),
            &token,
        )
        .await;
    assert_eq!(status, 400, "an unapplied setting is refused: {body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap_or("")
            .contains("health_check_interval_jitter"),
        "the refusal names the field: {body}"
    );
    let (status, _) = server
        .get_auth("/api/v5/actions/ro-refused-action", &token)
        .await;
    assert_eq!(status, 404, "a refused write stores nothing");
    // A real publish drives the batch and the counters.
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({
                "name": "ro-batch-action",
                "type": "webhook",
                "connector": "ro-hook",
                "parameters": {"path": "/hook"},
                "resource_opts": {
                    "batch_size": 2,
                    "batch_time": "1s",
                    "inflight_window": 3,
                    "worker_pool_size": 2,
                },
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, rule_body) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "name": "ro-batch-rule",
                "sql": "SELECT * FROM \"ro/in\"",
                "actions": ["ro-batch-action"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let rule_id = rule_body["id"].as_str().expect("rule id").to_string();
    let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
        std::sync::Arc::new(NoopBrokerSink);
    let topic = broker_protocol::Topic::new("ro/in").expect("topic");
    let payload = bytes::Bytes::from_static(b"{\"k\":1}");
    state
        .engine
        .dispatch_ingress(
            &topic,
            &payload,
            broker_protocol::QoS::AtMostOnce,
            &broker_sink,
        )
        .await;
    // One row stays in the bounded action buffer until the batch is
    // full or its time elapses. A raw sink delivers it now, so this
    // assertion fails when the runtime is absent.
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert_eq!(
        hits.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the runtime holds the first row in its batch buffer"
    );
    state
        .engine
        .dispatch_ingress(
            &topic,
            &payload,
            broker_protocol::QoS::AtMostOnce,
            &broker_sink,
        )
        .await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while hits.load(std::sync::atomic::Ordering::SeqCst) < 2 && std::time::Instant::now() < deadline
    {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert_eq!(
        hits.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "the batch reached the HTTP target"
    );
    let (status, body) = server
        .get_auth("/api/v5/actions/ro-batch-action/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["metrics"]["matched"], json!(2));
    assert_eq!(body["metrics"]["success"], json!(2));
    assert_eq!(body["metrics"]["failed"], json!(0));
    assert_eq!(body["metrics"]["queuing"], json!(0));
    assert_eq!(body["metrics"]["inflight"], json!(0));
    assert!(
        body["metrics"].get("retried").is_none(),
        "the invented retried counter is gone: {body}"
    );
    let (status, _) = server
        .send(
            "PUT",
            "/api/v5/actions/ro-batch-action/metrics/reset",
            Some(json!({})),
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/actions/ro-batch-action/metrics", &token)
        .await;
    assert_eq!(status, 200);
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
            body["metrics"][counter],
            json!(0),
            "reset zeroes {counter}: {body}"
        );
    }
    let (status, _) = server
        .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/ro-batch-action", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/connectors/ro-hook", &token)
        .await;
    assert_eq!(status, 204);
    receiver.abort();
}

/// Check one refused `resource_opts` field for RO-01.
/// Creates with the field, expects 400 naming it and nothing stored,
/// then updates the old action and expects the old action unchanged.
/// Keeps the loop body in one place so the test stays small.
#[cfg(test)]
async fn check_one_refused_resource_opt(
    server: &TestServer,
    token: &str,
    field: &str,
    opts: &Value,
) {
    let name = format!("ro-refused-{field}");
    let (status, body) = server
        .post_auth(
            "/api/v5/actions",
            json!({
                "name": name,
                "type": "webhook",
                "connector": "ro-refusal-hook",
                "parameters": {"path": "/new"},
                "resource_opts": opts,
            }),
            token,
        )
        .await;
    assert_eq!(status, 400, "{field} is refused: {body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains(field),
        "the refusal names {field}: {body}"
    );
    let (status, _) = server
        .get_auth(&format!("/api/v5/actions/{name}"), token)
        .await;
    assert_eq!(status, 404, "a refused create stores nothing: {field}");
    let (status, body) = server
        .put_auth(
            "/api/v5/actions/ro-refusal-action",
            json!({
                "name": "ro-refusal-action",
                "type": "webhook",
                "connector": "ro-refusal-hook",
                "parameters": {"path": "/new"},
                "resource_opts": opts,
            }),
            token,
        )
        .await;
    assert_eq!(status, 400, "a refused update names {field}: {body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains(field),
        "the update refusal names {field}: {body}"
    );
    let (status, body) = server
        .get_auth("/api/v5/actions/ro-refusal-action", token)
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["parameters"]["path"],
        json!("/old"),
        "the refused update keeps the old parameters: {body}"
    );
    assert_eq!(
        body["resource_opts"],
        json!({}),
        "the refused update keeps the old options: {body}"
    );
}

#[tokio::test]
async fn action_resource_opts_refusals_name_the_field_and_keep_the_old_action() {
    // RO-01: each refused resource option answers 400 with the field
    // name, stores nothing, and a refused update leaves the old
    // action and its options unchanged.
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "ro-refusal-hook", "type": "webhook", "url": "http://127.0.0.1:9"}),
            &token,
        )
        .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = server
        .post_auth(
            "/api/v5/actions",
            json!({
                "name": "ro-refusal-action",
                "type": "webhook",
                "connector": "ro-refusal-hook",
                "parameters": {"path": "/old"},
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201, "action create: {body}");
    let above_max = (tokio::sync::Semaphore::MAX_PERMITS as u64) + 1;
    // `health_check_timeout` is applied (default 60s), so a
    // non-default value parses and is not in the refused list.
    let bad_fields = [
        ("dispatch_strategy", json!({"dispatch_strategy": "random"})),
        (
            "health_check_interval_jitter",
            json!({"health_check_interval_jitter": "1s"}),
        ),
        ("no_such_option", json!({"no_such_option": 1})),
        ("batch_time", json!({"batch_time": "infinity"})),
        ("max_buffer_bytes", json!({"max_buffer_bytes": "0"})),
        ("inflight_window", json!({"inflight_window": above_max})),
    ];
    for (field, opts) in &bad_fields {
        check_one_refused_resource_opt(&server, &token, field, opts).await;
    }
    let (status, _) = server
        .delete_auth("/api/v5/actions/ro-refusal-action", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/connectors/ro-refusal-hook", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn action_health_probe_reports_a_closed_target() {
    // RO-01: a non-default health interval is applied. The probe marks
    // a closed target disconnected and names the probe failure.
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a port");
    let address = listener.local_addr().expect("closed address");
    drop(listener);
    let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "ro-health-hook", "type": "webhook", "url": format!("http://{address}")}),
                &token,
            )
            .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = server
        .post_auth(
            "/api/v5/actions",
            json!({
                "name": "ro-health-action",
                "type": "webhook",
                "connector": "ro-health-hook",
                "parameters": {"path": "/health"},
                "resource_opts": {"health_check_interval": "20ms"},
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201, "action create: {body}");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let (status, body) = server
            .get_auth("/api/v5/actions/ro-health-action", &token)
            .await;
        assert_eq!(status, 200, "{body}");
        if body["status"] == json!("disconnected") {
            assert!(
                body["status_reason"]
                    .as_str()
                    .is_some_and(|reason| !reason.is_empty()),
                "the probe names the failure: {body}"
            );
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the health probe marked the closed target: {body}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let (status, _) = server
        .delete_auth("/api/v5/actions/ro-health-action", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/connectors/ro-health-hook", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn action_batch_time_larger_than_request_ttl_is_refused() {
    // A batch time larger than the request TTL is refused. The
    // refused write stores nothing. An equal pair stays accepted.
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "sb25-refusal-hook", "type": "webhook", "url": "http://127.0.0.1:9"}),
            &token,
        )
        .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = server
        .post_auth(
            "/api/v5/actions",
            json!({
                "name": "sb25-refused-action",
                "type": "webhook",
                "connector": "sb25-refusal-hook",
                "parameters": {"path": "/refused"},
                "resource_opts": {"batch_time": "10s", "request_ttl": "5s"},
            }),
            &token,
        )
        .await;
    assert_eq!(
        status, 400,
        "a batch time larger than the TTL is refused: {body}"
    );
    assert!(
        body["message"]
            .as_str()
            .unwrap_or("")
            .contains("batch_time"),
        "the refusal names the field: {body}"
    );
    let (status, _) = server
        .get_auth("/api/v5/actions/sb25-refused-action", &token)
        .await;
    assert_eq!(status, 404, "a refused write stores nothing");
    let (status, body) = server
        .post_auth(
            "/api/v5/actions",
            json!({
                "name": "sb25-equal-action",
                "type": "webhook",
                "connector": "sb25-refusal-hook",
                "parameters": {"path": "/equal"},
                "resource_opts": {"batch_time": "5s", "request_ttl": "5s"},
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201, "an equal pair stays accepted: {body}");
    let (status, _) = server
        .delete_auth("/api/v5/actions/sb25-equal-action", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/connectors/sb25-refusal-hook", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn action_update_drains_waiting_rows_with_no_loss() {
    // One row waits in the bounded buffer. An action update drains it
    // to the old target with no loss.
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (address, hits, receiver) = spawn_ro_counting_http_target().await;
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({
                "name": "sb25-update-hook",
                "type": "webhook",
                "url": format!("http://{address}"),
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201, "connector create: {body}");
    let action = json!({
        "name": "sb25-update-action",
        "type": "webhook",
        "connector": "sb25-update-hook",
        "parameters": {"path": "/before"},
        "resource_opts": {
            "batch_size": 100,
            "batch_time": "10s",
            "request_ttl": "30s",
            "health_check_interval": "1h",
        },
    });
    let (status, body) = server
        .post_auth("/api/v5/actions", action.clone(), &token)
        .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "name": "sb25-update-rule",
                "sql": "SELECT * FROM \"sb25/in/update\"",
                "actions": ["sb25-update-action"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201, "rule create: {body}");
    let sink: Arc<dyn broker_rules::BrokerSink> = Arc::new(NoopBrokerSink);
    state
        .engine
        .dispatch_ingress(
            &Topic::new("sb25/in/update").expect("topic"),
            &bytes::Bytes::from_static(br#"{"n":1}"#),
            QoS::AtMostOnce,
            &sink,
        )
        .await;
    let (status, body) = server
        .get_auth("/api/v5/actions/sb25-update-action/metrics", &token)
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["metrics"]["queuing"], json!(1), "{body}");
    let mut updated = action.clone();
    updated["parameters"] = json!({"path": "/after"});
    let (status, body) = server
        .put_auth("/api/v5/actions/sb25-update-action", updated, &token)
        .await;
    assert_eq!(status, 200, "action update: {body}");
    let (status, body) = server
        .get_auth("/api/v5/actions/sb25-update-action/metrics", &token)
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["metrics"]["success"], json!(1), "{body}");
    assert_eq!(body["metrics"]["dropped"], json!(0), "{body}");
    assert_eq!(
        body["metrics"]["dropped.resource_stopped"],
        json!(0),
        "{body}"
    );
    assert_eq!(body["metrics"]["queuing"], json!(0), "{body}");
    assert_eq!(
        hits.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the waiting row reached the old target"
    );
    let (status, _) = server
        .delete_auth("/api/v5/rules/sb25-update-rule", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/sb25-update-action", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/connectors/sb25-update-hook", &token)
        .await;
    assert_eq!(status, 204);
    receiver.abort();
}

#[tokio::test]
async fn action_health_reason_hides_the_probe_target() {
    // The probe marks a closed target disconnected. The reason names
    // the failure without the resolved host and port.
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind a port");
    let address = listener.local_addr().expect("closed address");
    drop(listener);
    let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "sb25-health-hook", "type": "webhook", "url": format!("http://{address}")}),
                &token,
            )
            .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = server
        .post_auth(
            "/api/v5/actions",
            json!({
                "name": "sb25-health-action",
                "type": "webhook",
                "connector": "sb25-health-hook",
                "parameters": {"path": "/health"},
                "resource_opts": {"health_check_interval": "20ms"},
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201, "action create: {body}");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    loop {
        let (status, body) = server
            .get_auth("/api/v5/actions/sb25-health-action", &token)
            .await;
        assert_eq!(status, 200, "{body}");
        if body["status"] == json!("disconnected") {
            let reason = body["status_reason"].as_str().unwrap_or("");
            assert!(!reason.is_empty(), "the probe names the failure: {body}");
            assert!(
                !reason.contains(&address.to_string()),
                "the reason hides the target: {body}"
            );
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the health probe marked the closed target: {body}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let (status, _) = server
        .delete_auth("/api/v5/actions/sb25-health-action", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/connectors/sb25-health-hook", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn action_slow_target_runs_concurrent_driver_calls() {
    // Ten publishes with a 200 ms target each finish in less than 1 s
    // only when the driver calls run together.
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind slow target");
    let address = listener.local_addr().expect("slow target address");
    let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = std::sync::Arc::clone(&hits);
    let receiver = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            // One task per connection. A single accept loop would
            // serve the ten slow calls one after another and hide
            // broker concurrency. The sleep stays 200 ms per call.
            let counter = std::sync::Arc::clone(&counter);
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut chunk = [0u8; 2048];
                let body_start = loop {
                    let Ok(read) = socket.read(&mut chunk).await else {
                        break None;
                    };
                    if read == 0 {
                        break None;
                    }
                    request.extend_from_slice(&chunk[..read]);
                    if let Some(pos) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                        break Some(pos + 4);
                    }
                };
                if let Some(start) = body_start {
                    let headers = String::from_utf8_lossy(&request[..start]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .and_then(|value| value.trim().parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    while request.len() < start + length {
                        let Ok(read) = socket.read(&mut chunk).await else {
                            break;
                        };
                        if read == 0 {
                            break;
                        }
                        request.extend_from_slice(&chunk[..read]);
                    }
                    counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                let _ = socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .await;
            });
        }
    });
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({
                "name": "sb25-slow-hook",
                "type": "webhook",
                "url": format!("http://{address}"),
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = server
        .post_auth(
            "/api/v5/actions",
            json!({
                "name": "sb25-slow-action",
                "type": "webhook",
                "connector": "sb25-slow-hook",
                "parameters": {"path": "/slow"},
                "resource_opts": {
                    "batch_size": 1,
                    "batch_time": "0ms",
                    "inflight_window": 10,
                    "worker_pool_size": 1,
                    "health_check_interval": "1h",
                },
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "name": "sb25-slow-rule",
                "sql": "SELECT * FROM \"sb25/in/slow\"",
                "actions": ["sb25-slow-action"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201, "rule create: {body}");
    let start = std::time::Instant::now();
    let mut tasks = Vec::new();
    for index in 0..10 {
        let engine = std::sync::Arc::clone(&state.engine);
        let payload = bytes::Bytes::from(format!("{{\"n\":{index}}}"));
        tasks.push(tokio::spawn(async move {
            let sink: Arc<dyn broker_rules::BrokerSink> = Arc::new(NoopBrokerSink);
            engine
                .dispatch_ingress(
                    &Topic::new("sb25/in/slow").expect("topic"),
                    &payload,
                    QoS::AtMostOnce,
                    &sink,
                )
                .await
        }));
    }
    for task in tasks {
        task.await.expect("publish task runs");
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "ten slow calls ran together: {elapsed:?}"
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let (status, body) = server
            .get_auth("/api/v5/actions/sb25-slow-action/metrics", &token)
            .await;
        assert_eq!(status, 200, "{body}");
        if body["metrics"]["success"] == json!(10) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "all ten rows reached the target: {body}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(
        hits.load(std::sync::atomic::Ordering::SeqCst),
        10,
        "the target saw ten driver calls"
    );
    let (status, _) = server
        .delete_auth("/api/v5/rules/sb25-slow-rule", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/sb25-slow-action", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/connectors/sb25-slow-hook", &token)
        .await;
    assert_eq!(status, 204);
    receiver.abort();
}

#[tokio::test]
async fn action_health_probe_resolves_the_endpoint_alias() {
    // RO-01: the `endpoint` alias resolves like `url` before the
    // probe. A secret reference never becomes a fabricated target.
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the health target");
    let address = listener.local_addr().expect("health target address");
    let variable = format!("RO01_ENDPOINT_TARGET_{}", std::process::id());
    std::env::set_var(&variable, format!("http://{address}"));
    let reference = format!("env:{variable}");
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({
                "name": "ro-endpoint-hook",
                "type": "webhook",
                "endpoint": reference,
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = server
        .post_auth(
            "/api/v5/actions",
            json!({
                "name": "ro-endpoint-action",
                "type": "webhook",
                "connector": "ro-endpoint-hook",
                "parameters": {"path": "/endpoint"},
                "resource_opts": {"health_check_interval": "20ms"},
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201, "action create: {body}");
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    let (status, body) = server
        .get_auth("/api/v5/actions/ro-endpoint-action", &token)
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["status"],
        json!("connected"),
        "the endpoint alias resolves before the probe: {body}"
    );
    let (status, _) = server
        .delete_auth("/api/v5/actions/ro-endpoint-action", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/connectors/ro-endpoint-hook", &token)
        .await;
    assert_eq!(status, 204);
    std::env::remove_var(&variable);
}
