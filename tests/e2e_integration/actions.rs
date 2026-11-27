use super::*;

/// Waits until the action holds `rows` rows in its buffer. A QoS 0 publish
/// reaches the buffer after the PUBLISH is sent, so a test that changes the
/// action at once can race the rule engine.
async fn mv01_wait_queued(kernel: &Kernel, action: &str, rows: u64) {
    let deadline = tokio::time::Instant::now() + STEP;
    loop {
        let (status, body) = mv00_api(
            kernel,
            reqwest::Method::GET,
            &format!("/api/v5/actions/{action}/metrics"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "action metrics: {body}");
        if body["metrics"]["queuing"].as_u64() == Some(rows) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "action {action} did not buffer {rows} rows in time: {body}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// A buffered batch survives an update, a disable and a delete: each
/// swap drains the old runtime before it stops it.
#[tokio::test]
async fn mv01_action_buffer_drains_on_update_disable_and_delete() {
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
            "name": "mv01-drain",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let opts = serde_json::json!({
        "batch_size": 3,
        "batch_time": "10s",
        "health_check_interval": "1h",
    });
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-drain",
            "type": "webhook",
            "connector": "mv01-drain",
            "parameters": {"path": "/drain"},
            "resource_opts": opts,
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-drain-rule",
            "sql": "SELECT * FROM \"mv01/in/drain\"",
            "actions": ["mv01-drain"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-drain-pub").await;
    // One row waits in the old buffer; the update drains it to /drain.
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/drain", 0, 0, false, br#"{"d":1}"#),
    )
    .await;
    mv01_wait_queued(&kernel, "mv01-drain", 1).await;
    let first = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-drain",
        serde_json::json!({
            "name": "mv01-drain",
            "type": "webhook",
            "connector": "mv01-drain",
            "parameters": {"path": "/after"},
            "resource_opts": opts,
        }),
    )
    .await;
    assert_eq!(status, 200, "action update: {body}");
    let received = tokio::time::timeout(STEP, first)
        .await
        .expect("updated sink drained in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].0, "/drain");
    assert_eq!(received[0].1.as_slice(), &br#"{"d":1}"#[..]);
    // Two rows wait in the new buffer; the disable drains them.
    for n in [2, 3] {
        let payload = format!("{{\"d\":{n}}}");
        send_mqtt(
            &mut publisher,
            &sy02_publish311("mv01/in/drain", 0, 0, false, payload.as_bytes()),
        )
        .await;
    }
    mv01_wait_queued(&kernel, "mv01-drain", 2).await;
    let second = mv01_spawn_http_receiver(Arc::clone(&listener), 2);
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-drain/enable/false",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "disable: {body}");
    let received = tokio::time::timeout(STEP, second)
        .await
        .expect("disabled sink drained in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 2);
    assert!(received.iter().all(|(path, _)| path == "/after"));
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-drain/enable/true",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "enable: {body}");
    // Two rows wait again; the delete drains them after the rule detach.
    for n in [4, 5] {
        let payload = format!("{{\"d\":{n}}}");
        send_mqtt(
            &mut publisher,
            &sy02_publish311("mv01/in/drain", 0, 0, false, payload.as_bytes()),
        )
        .await;
    }
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/rules/mv01-drain-rule",
        serde_json::json!({
            "sql": "SELECT * FROM \"mv01/in/drain\"",
            "actions": [],
        }),
    )
    .await;
    assert_eq!(status, 200, "rule detach: {body}");
    let third = mv01_spawn_http_receiver(Arc::clone(&listener), 2);
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/mv01-drain",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "action delete: {body}");
    let received = tokio::time::timeout(STEP, third)
        .await
        .expect("deleted sink drained in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 2);
    assert!(received.iter().all(|(path, _)| path == "/after"));
    kernel.finish().await;
}

/// A connector disable stops every dependent action; enable restores
/// them. Deleting a connector that an action still uses is refused and
/// leaves both records in place.
#[tokio::test]
async fn mv01_connector_disable_mirrors_dependents_and_delete_guards_references() {
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
            "name": "mv01-cd",
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
            "name": "mv01-cd-action",
            "type": "webhook",
            "connector": "mv01-cd",
            "parameters": {"path": "/cd"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-cd-rule",
            "sql": "SELECT * FROM \"mv01/in/cd\"",
            "actions": ["mv01-cd-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-cd-pub").await;
    let first = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/cd", 0, 0, false, br#"{"d":1}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, first)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received[0].0, "/cd");
    assert_eq!(received[0].1.as_slice(), &br#"{"d":1}"#[..]);
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/connectors/mv01-cd/enable/false",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "connector disable: {body}");
    let watch = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/cd", 0, 0, false, br#"{"d":2}"#),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(
        !watch.is_finished(),
        "a disabled connector still delivered through its dependent action"
    );
    watch.abort();
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/connectors/mv01-cd/enable/true",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "connector enable: {body}");
    let second = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/cd", 0, 0, false, br#"{"d":3}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, second)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received[0].0, "/cd");
    assert_eq!(received[0].1.as_slice(), &br#"{"d":3}"#[..]);
    // The used connector cannot be deleted while the action points at it.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/connectors/mv01-cd",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 400, "used connector delete refuses: {body}");
    let message = body["message"].as_str().expect("error message");
    assert!(
        message.contains("mv01-cd-action") && message.contains("action"),
        "the refusal names the dependent action: {message}"
    );
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/connectors/mv01-cd",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "the refused delete keeps the connector");
    // Detach the rule, then the action and its connector can go.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/rules/mv01-cd-rule",
        serde_json::json!({
            "sql": "SELECT * FROM \"mv01/in/cd\"",
            "actions": [],
        }),
    )
    .await;
    assert_eq!(status, 200, "rule detach: {body}");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/mv01-cd-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "action delete after detach");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/connectors/mv01-cd",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "connector delete after the action is gone");
    kernel.finish().await;
}

/// SR-01 item 4: a connector start re-enables a dependent action only
/// when every referenced connector is enabled. The action names A for
/// delivery and B as a second reference. Stop A, start B, publish: no
/// delivery. Start A: delivery resumes.
#[tokio::test]
async fn sr01_connector_start_keeps_action_off_while_another_reference_is_stopped() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let kernel = spawn_kernel("").await;
    for (name, body) in [
        (
            "sr01-multi-a",
            serde_json::json!({
                "name": "sr01-multi-a",
                "type": "webhook",
                "url": format!("http://{address}"),
            }),
        ),
        (
            "sr01-multi-b",
            serde_json::json!({
                "name": "sr01-multi-b",
                "type": "console",
            }),
        ),
    ] {
        let (status, created) =
            mv00_api(&kernel, reqwest::Method::POST, "/api/v5/connectors", body).await;
        assert_eq!(status, 201, "connector {name}: {created}");
    }
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "sr01-multi-action",
            "type": "webhook",
            "connector": "sr01-multi-a",
            "connector_id": "sr01-multi-b",
            "parameters": {"path": "/multi"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "sr01-multi-rule",
            "sql": "SELECT * FROM \"sr01/multi/in\"",
            "actions": ["sr01-multi-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "sr01-multi-pub").await;

    let first = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("sr01/multi/in", 0, 0, false, br#"{"d":1}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, first)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received[0].0, "/multi");
    assert_eq!(received[0].1.as_slice(), &br#"{"d":1}"#[..]);

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors/sr01-multi-a/stop",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "connector A stop: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors/sr01-multi-b/start",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "connector B start: {body}");
    let watch = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("sr01/multi/in", 0, 0, false, br#"{"d":2}"#),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(
        !watch.is_finished(),
        "starting connector B resumed delivery through stopped connector A"
    );
    watch.abort();

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors/sr01-multi-a/start",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "connector A start: {body}");
    let second = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("sr01/multi/in", 0, 0, false, br#"{"d":3}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, second)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received[0].0, "/multi");
    assert_eq!(received[0].1.as_slice(), &br#"{"d":3}"#[..]);
    kernel.finish().await;
}

/// SR-01 item 4: an action start honours the connector state. Stop the
/// connector, start the action, publish: no delivery. Start the
/// connector: delivery resumes.
#[tokio::test]
async fn sr01_action_start_keeps_delivery_off_through_a_stopped_connector() {
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
            "name": "sr01-start-conn",
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
            "name": "sr01-start-action",
            "type": "webhook",
            "connector": "sr01-start-conn",
            "parameters": {"path": "/start"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "sr01-start-rule",
            "sql": "SELECT * FROM \"sr01/start/in\"",
            "actions": ["sr01-start-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "sr01-start-pub").await;

    let first = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("sr01/start/in", 0, 0, false, br#"{"d":1}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, first)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received[0].0, "/start");
    assert_eq!(received[0].1.as_slice(), &br#"{"d":1}"#[..]);

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors/sr01-start-conn/stop",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "connector stop: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions/sr01-start-action/start",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "action start: {body}");
    let watch = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("sr01/start/in", 0, 0, false, br#"{"d":2}"#),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(
        !watch.is_finished(),
        "the started action delivered through the stopped connector"
    );
    watch.abort();

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors/sr01-start-conn/start",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "connector start: {body}");
    let second = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("sr01/start/in", 0, 0, false, br#"{"d":3}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, second)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received[0].0, "/start");
    assert_eq!(received[0].1.as_slice(), &br#"{"d":3}"#[..]);
    kernel.finish().await;
}

/// Publishes continue across an action update: every message reaches
/// exactly one statement, and a later delete stops the action.
#[tokio::test]
async fn mv01_concurrent_publish_update_and_delete_keep_each_message_once() {
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
            "name": "mv01-conc",
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
            "name": "mv01-conc-action",
            "type": "webhook",
            "connector": "mv01-conc",
            "parameters": {"path": "/before"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-conc-rule",
            "sql": "SELECT * FROM \"mv01/in/conc\"",
            "actions": ["mv01-conc-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let payloads: Vec<String> = (0..16).map(|index| format!("{{\"n\":{index}}}")).collect();
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), payloads.len());
    let publisher = mv00_connect(&kernel, "mv01-conc-pub").await;
    let send_payloads = payloads.clone();
    let publisher_task = tokio::spawn(async move {
        let mut publisher = publisher;
        for payload in send_payloads {
            send_mqtt(
                &mut publisher,
                &sy02_publish311("mv01/in/conc", 0, 0, false, payload.as_bytes()),
            )
            .await;
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-conc-action",
        serde_json::json!({
            "name": "mv01-conc-action",
            "type": "webhook",
            "connector": "mv01-conc",
            "parameters": {"path": "/after"},
        }),
    )
    .await;
    assert_eq!(status, 200, "action update during publishes: {body}");
    publisher_task.await.expect("publisher task");
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(
        received.len(),
        payloads.len(),
        "every publish delivered once"
    );
    let mut got: Vec<String> = received
        .iter()
        .map(|(_, body)| String::from_utf8(body.clone()).expect("payload text"))
        .collect();
    let mut want = payloads.clone();
    got.sort();
    want.sort();
    assert_eq!(got, want, "no publish lost or duplicated across the swap");
    for (path, _) in &received {
        assert!(
            path.as_str() == "/before" || path.as_str() == "/after",
            "unexpected delivery path {path}"
        );
    }
    // Detach the rule, then delete the action; a later publish is silent.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/rules/mv01-conc-rule",
        serde_json::json!({
            "sql": "SELECT * FROM \"mv01/in/conc\"",
            "actions": [],
        }),
    )
    .await;
    assert_eq!(status, 200, "rule detach: {body}");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/mv01-conc-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "action delete");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-conc-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 404, "the deleted action reads absent");
    let watch = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    let mut publisher = mv00_connect(&kernel, "mv01-conc-pub2").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/conc", 0, 0, false, br#"{"n":"gone"}"#),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(!watch.is_finished(), "a deleted action still delivered");
    watch.abort();
    kernel.finish().await;
}

/// A persistence failure on an action update keeps the old sink and the
/// old stored parameters, and answers 500.
#[tokio::test]
async fn mv01_action_persist_failure_keeps_the_old_sink_delivering() {
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
            "name": "mv01-persist",
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
            "name": "mv01-persist-action",
            "type": "webhook",
            "connector": "mv01-persist",
            "parameters": {"path": "/persist"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-persist-rule",
            "sql": "SELECT * FROM \"mv01/in/persist\"",
            "actions": ["mv01-persist-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-persist-pub").await;
    let first = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/persist", 0, 0, false, br#"{"p":1}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, first)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received[0].1.as_slice(), &br#"{"p":1}"#[..]);
    // Block the atomic save: the state path becomes a directory, so the
    // temp-file rename fails for every later persist.
    let state_path = kernel.scratch.join("state.toml");
    std::fs::remove_file(&state_path).expect("state file exists after the writes");
    std::fs::create_dir(&state_path).expect("state path blocks the rename");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-persist-action",
        serde_json::json!({
            "name": "mv01-persist-action",
            "type": "webhook",
            "connector": "mv01-persist",
            "parameters": {"path": "/persist-new"},
        }),
    )
    .await;
    assert_eq!(status, 500, "the persistence failure answers 500: {body}");
    assert_eq!(body["code"], "INTERNAL_ERROR", "{body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-persist-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["parameters"]["path"], "/persist", "{body}");
    let second = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/persist", 0, 0, false, br#"{"p":2}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, second)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received[0].0, "/persist");
    assert_eq!(received[0].1.as_slice(), &br#"{"p":2}"#[..]);
    kernel.finish().await;
}

/// A persistence failure on connector create and update keeps the store
/// and every sink as they were.
#[tokio::test]
async fn mv01_connector_persist_failure_keeps_the_previous_generation() {
    let old_listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("old receiver binds"),
    );
    let old_address = old_listener.local_addr().expect("old receiver address");
    let new_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("new receiver binds");
    let new_address = new_listener.local_addr().expect("new receiver address");
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-conn-persist",
            "type": "webhook",
            "url": format!("http://{old_address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-conn-persist",
            "type": "webhook",
            "connector": "mv01-conn-persist",
            "parameters": {"path": "/keep"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-conn-persist-rule",
            "sql": "SELECT * FROM \"mv01/in/connpersist\"",
            "actions": ["mv01-conn-persist"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-conn-persist-pub").await;
    let first = mv01_spawn_http_receiver(Arc::clone(&old_listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/connpersist", 0, 0, false, br#"{"c":1}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, first)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received[0].1.as_slice(), &br#"{"c":1}"#[..]);
    // Block the atomic save for every later write.
    let state_path = kernel.scratch.join("state.toml");
    std::fs::remove_file(&state_path).expect("state file exists after the writes");
    std::fs::create_dir(&state_path).expect("state path blocks the rename");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/connectors/mv01-conn-persist",
        serde_json::json!({
            "name": "mv01-conn-persist",
            "type": "webhook",
            "url": format!("http://{new_address}"),
        }),
    )
    .await;
    assert_eq!(status, 500, "the update persist failure: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/connectors/mv01-conn-persist",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["url"],
        serde_json::json!(format!("http://{old_address}")),
        "the stored connector is unchanged: {body}"
    );
    let second = mv01_spawn_http_receiver(Arc::clone(&old_listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/connpersist", 0, 0, false, br#"{"c":2}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, second)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received[0].0, "/keep");
    assert_eq!(received[0].1.as_slice(), &br#"{"c":2}"#[..]);
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-conn-persist-new",
            "type": "webhook",
            "url": format!("http://{new_address}"),
        }),
    )
    .await;
    assert_eq!(status, 500, "the create persist failure: {body}");
    let (status, _body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/connectors/mv01-conn-persist-new",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 404, "the uncommitted connector is not stored");
    kernel.finish().await;
}

/// One rule mixes an owned `pgsql` action, an owned `webhook` action and
/// a kind with no owned runtime: both owned sinks deliver and the
/// refused kind counts one failed fire.
#[tokio::test]
async fn mv01_mixed_kind_rule_delivers_owned_actions_and_counts_refused_failure() {
    let pg_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pg fake binds");
    let pg_address = pg_listener.local_addr().expect("pg fake address");
    let captured: PgCaptured = Arc::new(Mutex::new(Vec::new()));
    let pg_captured = Arc::clone(&captured);
    let pg_fake = tokio::spawn(async move {
        let stream = mv01_pg_accept(&pg_listener).await;
        mv01_pg_serve_one(stream, pg_captured, "u", "pwd", "db", 1).await;
    });
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
            "name": "mv01-mixed-pg",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", pg_address.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 201, "pgsql connector: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-mixed-pg-act",
            "type": "pgsql",
            "connector": "mv01-mixed-pg",
            "parameters": {"sql": "MV01-MIXED INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 201, "pgsql action: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-mixed-wh",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "webhook connector: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-mixed-wh-act",
            "type": "webhook",
            "connector": "mv01-mixed-wh",
            "parameters": {"path": "/mixed"},
        }),
    )
    .await;
    assert_eq!(status, 201, "webhook action: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-mixed-refused",
            "type": "console",
        }),
    )
    .await;
    assert_eq!(status, 201, "unowned kind action stores: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-mixed-rule",
            "sql": "SELECT * FROM \"mv01/in/mixed\"",
            "actions": ["mv01-mixed-pg-act", "mv01-mixed-wh-act", "mv01-mixed-refused"],
        }),
    )
    .await;
    assert_eq!(status, 201, "mixed rule create: {body}");
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    let mut publisher = mv00_connect(&kernel, "mv01-mixed-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/mixed", 0, 0, false, br#"{"m":1}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received[0].0, "/mixed");
    assert_eq!(received[0].1.as_slice(), &br#"{"m":1}"#[..]);
    tokio::time::timeout(STEP, pg_fake)
        .await
        .expect("pgsql delivery in time")
        .expect("pg fake task");
    let captured = std::sync::Mutex::into_inner(Arc::try_unwrap(captured).expect("sole owner"))
        .expect("capture lock");
    assert_eq!(captured.len(), 1, "the pgsql action delivered once");
    assert_eq!(captured[0].1.len(), 1, "one engine row");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-mixed-refused/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "refused-kind metrics: {body}");
    assert_eq!(body["metrics"]["success"], serde_json::json!(0), "{body}");
    assert_eq!(body["metrics"]["failed"], serde_json::json!(1), "{body}");
    kernel.finish().await;
}

/// A stored action whose sink cannot build at boot reports the error state
/// and raises the build alarm through the production `/alarms` entry.
#[tokio::test]
async fn mv01_failed_boot_action_wiring_reports_error_and_alarm() {
    let mut kernel = spawn_kernel("").await;
    let secret = kernel.scratch.join("mv01-boot-url");
    std::fs::write(&secret, "http://127.0.0.1:9").expect("secret file");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-boot-link",
            "type": "webhook",
            "url": format!("file:{}", secret.display()),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-boot-fail",
            "type": "webhook",
            "connector": "mv01-boot-link",
            "parameters": {"path": "/boot"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    assert_eq!(
        body["status"], "disconnected",
        "the secret resolves to a down target and the probe reports it: {body}"
    );
    // The secret disappears before the restart, so the boot replay cannot
    // rebuild the action sink.
    std::fs::remove_file(&secret).expect("remove the secret");
    mv00_restart(&mut kernel).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-boot-fail",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "the failed action stays readable: {body}");
    assert_eq!(body["status"], "disconnected", "{body}");
    let reason = body["status_reason"].as_str().expect("status reason");
    assert!(
        reason.contains("url"),
        "the reason names the field: {reason}"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/alarms",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "alarms read: {body}");
    let alarm = body["data"]
        .as_array()
        .expect("alarm rows")
        .iter()
        .find(|row| row["name"] == "action_build_failed:mv01-boot-fail")
        .expect("the boot failure raised its alarm");
    assert!(
        alarm["message"]
            .as_str()
            .is_some_and(|text| text.contains("url")),
        "the alarm carries the build reason: {alarm}"
    );
    kernel.finish().await;
}

/// A valid webhook server that is down at write time does not refuse the
/// action; the stored action delivers once the server starts.
#[tokio::test]
async fn mv01_down_webhook_server_stores_the_action_and_delivers_when_up() {
    let reserve = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a port");
    let address = reserve.local_addr().expect("reserved address");
    let kernel = spawn_kernel("").await;
    drop(reserve);
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-down",
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
            "name": "mv01-down-action",
            "type": "webhook",
            "connector": "mv01-down",
            "parameters": {"path": "/down"},
        }),
    )
    .await;
    assert_eq!(status, 201, "a down target still stores: {body}");
    assert_eq!(
        body["status"], "disconnected",
        "a real connection attempt reports the down target: {body}"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-down-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["parameters"]["path"], "/down", "{body}");
    assert_eq!(
        body["status"], "disconnected",
        "the read keeps the real probe result: {body}"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-down-rule",
            "sql": "SELECT * FROM \"mv01/in/down\"",
            "actions": ["mv01-down-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-down-pub").await;
    // The server is down: the fire fails, but the broker keeps serving.
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/down", 0, 0, false, br#"{"down":1}"#),
    )
    .await;
    // Wait for that failed fire to finish before the server starts, so the
    // first message cannot arrive at the receiver below.
    let deadline = std::time::Instant::now() + STEP;
    loop {
        let (_, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            "/api/v5/actions/mv01-down-action/metrics",
            serde_json::Value::Null,
        )
        .await;
        if body["metrics"]["failed"].as_u64().unwrap_or(0) >= 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the down-target fire failed in time: {body}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let listener = Arc::new(
        tokio::net::TcpListener::bind(address)
            .await
            .expect("the server starts on the reserved port"),
    );
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/down", 0, 0, false, br#"{"down":2}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received[0].0, "/down");
    assert_eq!(received[0].1.as_slice(), &br#"{"down":2}"#[..]);
    kernel.finish().await;
}

/// A v5 action and a v1 connector may share a name; typed and bare v5
/// action ids resolve to the same sink, and both paths survive a restart.
#[tokio::test]
async fn mv01_v5_action_and_v1_connector_share_a_name_and_survive_restart() {
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
        "/api/v1/connectors",
        serde_json::json!({
            "id": "shared",
            "kind": "webhook",
            "config": {"url": format!("http://{address}/v1"), "batch_size": 1},
        }),
    )
    .await;
    assert_eq!(status, 201, "v1 connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-ns-link",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "v5 connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "shared",
            "type": "webhook",
            "connector": "mv01-ns-link",
            "parameters": {"path": "/v5"},
        }),
    )
    .await;
    assert_eq!(status, 201, "v5 action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v1/rules",
        serde_json::json!({
            "name": "mv01-ns-v1",
            "topic_filter": "mv01/ns/v1",
            "sql_query": "SELECT * FROM \"mv01/ns/v1\"",
            "actions": [{"type": "forwardconnector", "connector_id": "shared"}],
        }),
    )
    .await;
    assert_eq!(status, 201, "v1 rule create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-ns-typed",
            "sql": "SELECT * FROM \"mv01/ns/typed\"",
            "actions": ["webhook:shared"],
        }),
    )
    .await;
    assert_eq!(status, 201, "typed v5 rule create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-ns-bare",
            "sql": "SELECT * FROM \"mv01/ns/bare\"",
            "actions": ["shared"],
        }),
    )
    .await;
    assert_eq!(status, 201, "bare v5 rule create: {body}");
    let topics = ["mv01/ns/v1", "mv01/ns/typed", "mv01/ns/bare"];
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), topics.len());
    let mut publisher = mv00_connect(&kernel, "mv01-ns-pub").await;
    for topic in topics {
        send_mqtt(
            &mut publisher,
            &sy02_publish311(topic, 0, 0, false, topic.as_bytes()),
        )
        .await;
    }
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    let mut paths: Vec<&str> = received.iter().map(|(path, _)| path.as_str()).collect();
    paths.sort_unstable();
    assert_eq!(
        paths,
        vec!["/v1", "/v5", "/v5"],
        "same-name v1 connector and v5 action routed separately: {received:?}"
    );
    // The v1 forward on the same bare name does not inflate the v5 action
    // metrics: only the two v5 fires count.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/shared/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "action metrics: {body}");
    assert_eq!(body["metrics"]["matched"], serde_json::json!(2), "{body}");
    assert_eq!(body["metrics"]["success"], serde_json::json!(2), "{body}");
    // Restart: the v5 action replay rebuilds under its dispatch key. The
    // v1 path stays in its own memory-only name space, as it did before
    // MV-01, so it does not write a v5 connector row and does not resume.
    mv00_restart(&mut kernel).await;
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 2);
    let mut publisher = mv00_connect(&kernel, "mv01-ns-pub2").await;
    for topic in topics {
        send_mqtt(
            &mut publisher,
            &sy02_publish311(topic, 0, 0, false, format!("{topic}-restart").as_bytes()),
        )
        .await;
    }
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    let mut paths: Vec<&str> = received.iter().map(|(path, _)| path.as_str()).collect();
    paths.sort_unstable();
    assert_eq!(
        paths,
        vec!["/v5", "/v5"],
        "the v5 actions resumed and the v1 row stayed out of the v5 store: {received:?}"
    );
    // The v1 rule fired on the same bare name after the restart too. Its
    // forward must not credit the v5 action's metrics.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/shared/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "post-restart action metrics: {body}");
    assert_eq!(
        body["metrics"]["matched"],
        serde_json::json!(2),
        "only the two v5 fires count after restart: {body}"
    );
    assert_eq!(
        body["metrics"]["success"],
        serde_json::json!(2),
        "only the two v5 fires succeed after restart: {body}"
    );
    kernel.finish().await;
}

/// A webhook connector that is down reads `disconnected` from its own
/// probe at create and at read time, and `connected` after the update
/// points it at a live target and probes it again.
#[tokio::test]
async fn mv01_down_webhook_connector_reads_disconnected_from_probe() {
    let kernel = spawn_kernel("").await;
    // The down target uses the discard port. Nothing listens there,
    // so the probe always fails. A reserved-then-freed port can be
    // taken by a parallel kernel before the probe dials.
    let connector = serde_json::json!({
        "name": "mv01-probe-link",
        "type": "webhook",
        "url": "http://127.0.0.1:9",
    });
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        connector.clone(),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    assert_eq!(
        body["status"], "disconnected",
        "the create probe reports the down target: {body}"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/connectors/mv01-probe-link",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "connector read: {body}");
    assert_eq!(
        body["status"], "disconnected",
        "the read keeps the probe result: {body}"
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the live target binds");
    let live = listener.local_addr().expect("live target address");
    let connector = serde_json::json!({
        "name": "mv01-probe-link",
        "type": "webhook",
        "url": format!("http://{live}"),
    });
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/connectors/mv01-probe-link",
        connector,
    )
    .await;
    assert_eq!(status, 200, "connector update: {body}");
    assert_eq!(
        body["status"], "connected",
        "the update probe reports the live target: {body}"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/connectors/mv01-probe-link",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "connector read: {body}");
    assert_eq!(body["status"], "connected", "{body}");
    drop(listener);
    kernel.finish().await;
}

/// A not-ready action kind is refused at the KC-00 map with the create
/// code `ALREADY_EXISTS` and the update code `BAD_REQUEST`.
#[tokio::test]
async fn mv01_action_readiness_refusals_keep_the_documented_codes() {
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-mqtt-not-ready",
            "type": "mqtt",
            "connector": "mv01-missing-link",
        }),
    )
    .await;
    assert_eq!(status, 400, "a not-ready kind is refused: {body}");
    assert_eq!(body["code"], "ALREADY_EXISTS", "{body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-ready-link",
            "type": "webhook",
            "url": "http://127.0.0.1:9",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-ready-action",
            "type": "webhook",
            "connector": "mv01-ready-link",
            "parameters": {"path": "/ready"},
        }),
    )
    .await;
    assert_eq!(status, 201, "ready action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-ready-action",
        serde_json::json!({
            "name": "mv01-ready-action",
            "type": "mqtt",
            "connector": "mv01-ready-link",
        }),
    )
    .await;
    assert_eq!(status, 400, "the update refuses the not-ready kind: {body}");
    assert_eq!(body["code"], "BAD_REQUEST", "{body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-ready-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(
        status, 200,
        "the refused update keeps the old action: {body}"
    );
    assert_eq!(body["type"], "webhook", "{body}");
    kernel.finish().await;
}

/// An invalid header value is refused without echoing the value.
#[tokio::test]
async fn mv01_action_header_value_is_not_echoed_in_the_error() {
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-header-link",
            "type": "webhook",
            "url": "http://127.0.0.1:9",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-header-action",
            "type": "webhook",
            "connector": "mv01-header-link",
            "parameters": {
                "path": "/header",
                "headers": {"X-Secret": "super-secret\nvalue"},
            },
        }),
    )
    .await;
    assert_eq!(status, 400, "the invalid header value is refused: {body}");
    assert_eq!(body["code"], "ALREADY_EXISTS", "{body}");
    let message = body["message"].as_str().expect("error message");
    assert!(message.contains("X-Secret"), "the key is named: {message}");
    assert!(
        message.contains("parameters.headers"),
        "the field is named: {message}"
    );
    assert!(
        !message.contains("super-secret"),
        "the header value is not echoed: {message}"
    );
    kernel.finish().await;
}

/// The action request timeout comes from `resource_opts.request_ttl`, not
/// from the sink's 5 s default.
#[tokio::test]
async fn mv01_action_request_ttl_bounds_the_http_request() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let receiver = mv01_spawn_http_receiver_active(
        Arc::clone(&listener),
        1,
        std::time::Duration::from_secs(3),
    );
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-ttl-link",
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
            "name": "mv01-ttl-action",
            "type": "webhook",
            "connector": "mv01-ttl-link",
            "parameters": {"path": "/ttl"},
            "resource_opts": {
                "batch_size": 1,
                "batch_time": "0ms",
                "request_ttl": "1s",
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
            "id": "mv01-ttl-rule",
            "sql": "SELECT * FROM \"mv01/in/ttl\"",
            "actions": ["mv01-ttl-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-ttl-pub").await;
    let start = std::time::Instant::now();
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/ttl", 0, 0, false, br#"{"ttl":1}"#),
    )
    .await;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(2500);
    loop {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            "/api/v5/actions/mv01-ttl-action/metrics",
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "metrics: {body}");
        if body["metrics"]["failed"].as_u64().unwrap_or(0) >= 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the request timed out from request_ttl, not the 5 s default: {body}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(
        start.elapsed() < std::time::Duration::from_millis(2500),
        "the 1 s request TTL applies"
    );
    receiver.abort();
    kernel.finish().await;
}

/// A connector create must not rebuild an unrelated action from a stale
/// snapshot. Action updates and connector creates run together; after the
/// churn the sink that delivers matches the stored action.
#[tokio::test]
async fn mv01_connector_rebuild_does_not_overwrite_an_action_update() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let drain_listener = Arc::clone(&listener);
    let drain = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = drain_listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                use tokio::io::AsyncReadExt;
                let mut byte = [0_u8; 1];
                let _ = socket.read_exact(&mut byte).await;
            });
        }
    });
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-churn-link",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let action_body = |path: &str| {
        serde_json::json!({
            "name": "mv01-churn-action",
            "type": "webhook",
            "connector": "mv01-churn-link",
            "parameters": {"path": path},
        })
    };
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        action_body("/p0"),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-churn-rule",
            "sql": "SELECT * FROM \"mv01/in/churn\"",
            "actions": ["mv01-churn-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let updates = async {
        for index in 1..30 {
            let path = format!("/p{index}");
            let (status, body) = mv00_api(
                &kernel,
                reqwest::Method::PUT,
                "/api/v5/actions/mv01-churn-action",
                action_body(&path),
            )
            .await;
            assert_eq!(status, 200, "action update: {body}");
        }
    };
    let creates = async {
        for index in 0..15 {
            let name = format!("mv01-churn-{index}");
            let (status, body) = mv00_api(
                &kernel,
                reqwest::Method::POST,
                "/api/v5/connectors",
                serde_json::json!({
                    "name": name,
                    "type": "webhook",
                    "url": format!("http://{address}"),
                }),
            )
            .await;
            assert_eq!(status, 201, "unrelated connector create: {body}");
            let path = format!("/api/v5/connectors/{name}");
            let (status, _) = mv00_api(
                &kernel,
                reqwest::Method::DELETE,
                &path,
                serde_json::Value::Null,
            )
            .await;
            assert_eq!(status, 204, "unrelated connector delete");
        }
    };
    tokio::join!(updates, creates);
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-churn-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "action read: {body}");
    let stored_path = body["parameters"]["path"]
        .as_str()
        .expect("stored path")
        .to_string();
    drain.abort();
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    let mut publisher = mv00_connect(&kernel, "mv01-churn-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/churn", 0, 0, false, br#"{"churn":1}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 1, "one delivery");
    assert_eq!(
        received[0].0, stored_path,
        "the delivering sink matches the stored action"
    );
    kernel.finish().await;
}
