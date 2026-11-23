use super::*;

/// RO-01/B2-6: GreptimeDB defaults `server` to 127.0.0.1:4001 when absent.
/// The default is observed on the wire: a listener on 127.0.0.1:4001
/// receives the published row.
#[tokio::test]
async fn ro01_greptimedb_defaults_server() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:4001")
            .await
            .expect("GreptimeDB default port binds"),
    );
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "ro01-greptime-conn",
            "type": "greptimedb",
            "database": "demo",
            "table": "metrics",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/connectors/ro01-greptime-conn",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "connector readable: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "ro01-greptime-action",
            "type": "greptimedb",
            "connector": "ro01-greptime-conn",
            "parameters": {"table": "metrics"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-greptime-rule",
            "sql": "SELECT * FROM \"ro01/greptime\"",
            "actions": ["ro01-greptime-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    let mut publisher = mv00_connect(&kernel, "ro01-greptime-pub").await;
    let publish = sy02_publish311("ro01/greptime", 1, 1, false, b"{\"n\":1}");
    send_mqtt(&mut publisher, &publish).await;
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut publisher))
        .await
        .expect("PUBACK in time")
        .expect("PUBACK");
    assert_eq!(head, 0x40, "publisher sees PUBACK");
    assert_eq!(body, vec![0, 1], "PUBACK mirrors packet id 1");
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("default-endpoint delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 1, "one row reaches 127.0.0.1:4001");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/ro01-greptime-rule",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/ro01-greptime-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/connectors/ro01-greptime-conn",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    kernel.finish().await;
}

/// GreptimeDB appends port 8086 to a bare host: a connector with
/// `server` 127.0.0.1 delivers to 127.0.0.1:8086.
#[tokio::test]
async fn mn05_greptimedb_bare_host_gains_8086() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:8086")
            .await
            .expect("GreptimeDB bare-host port binds"),
    );
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mn05-greptime-bare-conn",
            "type": "greptimedb",
            "server": "127.0.0.1",
            "database": "demo",
            "table": "metrics",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mn05-greptime-bare-action",
            "type": "greptimedb",
            "connector": "mn05-greptime-bare-conn",
            "parameters": {"table": "metrics"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mn05-greptime-bare-rule",
            "sql": "SELECT * FROM \"mn05/greptime-bare\"",
            "actions": ["mn05-greptime-bare-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    let mut publisher = mv00_connect(&kernel, "mn05-greptime-bare-pub").await;
    let publish = sy02_publish311("mn05/greptime-bare", 1, 1, false, b"{\"n\":1}");
    send_mqtt(&mut publisher, &publish).await;
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut publisher))
        .await
        .expect("PUBACK in time")
        .expect("PUBACK");
    assert_eq!(head, 0x40, "publisher sees PUBACK");
    assert_eq!(body, vec![0, 1], "PUBACK mirrors packet id 1");
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("bare-host delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 1, "one row reaches 127.0.0.1:8086");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/mn05-greptime-bare-rule",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/mn05-greptime-bare-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/connectors/mn05-greptime-bare-conn",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    kernel.finish().await;
}

/// RO-01/B2-6: Datalayers defaults `server` to 127.0.0.1:8361 when absent.
/// The default is observed on the wire: a listener on 127.0.0.1:8361
/// receives the published row.
#[tokio::test]
async fn ro01_datalayers_defaults_server() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:8361")
            .await
            .expect("Datalayers default port binds"),
    );
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "ro01-datalayers-conn",
            "type": "datalayers",
            "database": "demo",
            "table": "metrics",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/connectors/ro01-datalayers-conn",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "connector readable: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "ro01-datalayers-action",
            "type": "datalayers",
            "connector": "ro01-datalayers-conn",
            "parameters": {"table": "metrics"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-datalayers-rule",
            "sql": "SELECT * FROM \"ro01/datalayers\"",
            "actions": ["ro01-datalayers-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    let mut publisher = mv00_connect(&kernel, "ro01-datalayers-pub").await;
    let publish = sy02_publish311("ro01/datalayers", 1, 1, false, b"{\"n\":1}");
    send_mqtt(&mut publisher, &publish).await;
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut publisher))
        .await
        .expect("PUBACK in time")
        .expect("PUBACK");
    assert_eq!(head, 0x40, "publisher sees PUBACK");
    assert_eq!(body, vec![0, 1], "PUBACK mirrors packet id 1");
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("default-endpoint delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 1, "one row reaches 127.0.0.1:8361");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/ro01-datalayers-rule",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/ro01-datalayers-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/connectors/ro01-datalayers-conn",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    kernel.finish().await;
}
