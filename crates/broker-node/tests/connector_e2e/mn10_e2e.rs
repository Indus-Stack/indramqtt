//! End-to-end tests for InfluxDB escapes and HTTP topic encoding (MN-10).
//!
//! Each test boots one real broker and one HTTP target.
//! It creates objects through the v5 HTTP management API.
//! It publishes messages through a real MQTT socket.
//! It verifies the escaped format at the target.

use std::time::Duration;

use super::e2e_support::{TestBroker, TestTarget};
use serde_json::json;

/// Bound for waiting for delivery to the test target.
/// Reason: 10 s covers socket publish, rule match, and HTTP post.
/// Source: pipeline timeout budget.
const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn mn10_influxdb_field_string_doubles_backslashes_through_broker() {
    let target = TestTarget::start_http().await;
    let broker = TestBroker::start("").await;

    let (status, body) = broker
        .api(
            reqwest::Method::POST,
            "/api/v5/connectors",
            json!({
                "type": "influxdb",
                "name": "mn10-esc",
                "server": target.address(),
                "bucket": "b",
                "org": "o",
                "token": "t",
                "measurement": "m",
            }),
        )
        .await;
    assert_eq!(status, 201, "connector creation failed: {body}");

    let (status, body) = broker
        .api(
            reqwest::Method::POST,
            "/api/v5/actions",
            json!({
                "type": "influxdb",
                "name": "mn10-esc-act",
                "connector": "mn10-esc",
                "parameters": {},
            }),
        )
        .await;
    assert_eq!(status, 201, "action creation failed: {body}");

    let (status, body) = broker
        .api(
            reqwest::Method::POST,
            "/api/v5/rules",
            json!({
                "id": "mn10-esc-rule",
                "name": "mn10-esc-rule",
                "sql": "SELECT * FROM \"mn10/esc\"",
                "actions": ["mn10-esc-act"],
            }),
        )
        .await;
    assert_eq!(status, 201, "rule creation failed: {body}");

    // The payload is the bytes {"msg":"line1\nline2"} with a backslash and n (not a line feed).
    broker
        .publish("mn10-esc-pub", "mn10/esc", br#"{"msg":"line1\nline2"}"#, 0)
        .await;

    target.wait_for_count(1, WAIT_TIMEOUT).await;
    let records = target.records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].method, "POST");
    assert_eq!(records[0].path, "/api/v2/write?org=o&bucket=b&precision=ms");

    let body = String::from_utf8(records[0].body.clone()).expect("valid utf8");
    assert_eq!(body.lines().count(), 1);
    assert!(
        body.starts_with(r#"m,topic=mn10/esc qos=0i,payload="{\"msg\":\"line1\\nline2\"}""#),
        "expected double backslash before n: {body}"
    );

    broker.finish().await;
}

#[tokio::test]
async fn mn10_influxdb_measurement_escapes_specials_through_broker() {
    let target = TestTarget::start_http().await;
    let broker = TestBroker::start("").await;

    let (status, body) = broker
        .api(
            reqwest::Method::POST,
            "/api/v5/connectors",
            json!({
                "type": "influxdb",
                "name": "mn10-meas-conn",
                "server": target.address(),
                "bucket": "b",
                "org": "o",
                "token": "t",
                "measurement": "m",
            }),
        )
        .await;
    assert_eq!(status, 201, "connector creation failed: {body}");

    let (status, body) = broker
        .api(
            reqwest::Method::POST,
            "/api/v5/actions",
            json!({
                "type": "influxdb",
                "name": "mn10-meas-act",
                "connector": "mn10-meas-conn",
                "parameters": {
                    "measurement": "a\nb\rc,d e",
                },
            }),
        )
        .await;
    assert_eq!(status, 201, "action creation failed: {body}");

    let (status, body) = broker
        .api(
            reqwest::Method::POST,
            "/api/v5/rules",
            json!({
                "id": "mn10-meas-rule",
                "name": "mn10-meas-rule",
                "sql": "SELECT * FROM \"mn10/meas\"",
                "actions": ["mn10-meas-act"],
            }),
        )
        .await;
    assert_eq!(status, 201, "rule creation failed: {body}");

    broker
        .publish("mn10-meas-pub", "mn10/meas", br#"{"v":1}"#, 0)
        .await;

    target.wait_for_count(1, WAIT_TIMEOUT).await;
    let records = target.records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].method, "POST");
    assert_eq!(records[0].path, "/api/v2/write?org=o&bucket=b&precision=ms");

    let body = String::from_utf8(records[0].body.clone()).expect("valid utf8");
    let lines: Vec<&str> = body.lines().collect();
    assert_eq!(lines.len(), 1, "body must have exactly one line: {body}");
    assert!(
        lines[0].starts_with("a\\nb\\rc\\,d\\ e,topic=mn10/meas "),
        "expected escaped measurement prefix: {body}"
    );

    broker.finish().await;
}

#[tokio::test]
async fn mn10_http_action_topic_percent_encodes_path_through_broker() {
    let target = TestTarget::start_http().await;
    let broker = TestBroker::start("").await;

    let (status, body) = broker
        .api(
            reqwest::Method::POST,
            "/api/v5/connectors",
            json!({
                "type": "http",
                "name": "mn10-http",
                "url": target.address(),
            }),
        )
        .await;
    assert_eq!(status, 201, "connector creation failed: {body}");

    let (status, body) = broker
        .api(
            reqwest::Method::POST,
            "/api/v5/actions",
            json!({
                "type": "http",
                "name": "mn10-http-act",
                "connector": "mn10-http",
                "parameters": {
                    "path": "/ingest/${topic}",
                    "method": "post",
                },
            }),
        )
        .await;
    assert_eq!(status, 201, "action creation failed: {body}");

    let (status, body) = broker
        .api(
            reqwest::Method::POST,
            "/api/v5/rules",
            json!({
                "id": "mn10-http-rule",
                "name": "mn10-http-rule",
                "sql": "SELECT * FROM \"mn10/#\"",
                "actions": ["mn10-http-act"],
            }),
        )
        .await;
    assert_eq!(status, 201, "rule creation failed: {body}");

    broker
        .publish("mn10-http-pub1", "mn10/with space", br#"{"v":1}"#, 0)
        .await;
    broker
        .publish("mn10-http-pub2", "mn10/http", br#"{"v":2}"#, 0)
        .await;

    target.wait_for_count(2, WAIT_TIMEOUT).await;
    let records = target.records();
    assert_eq!(records.len(), 2);

    let rec_space = records
        .iter()
        .find(|record| record.path.contains("with"))
        .expect("request with space in topic must arrive");
    assert_eq!(
        rec_space.path, "/ingest/mn10/with%20space",
        "topic byte not in allowed set must be percent-encoded"
    );

    let rec_http = records
        .iter()
        .find(|record| record.path == "/ingest/mn10/http")
        .expect("request with plain topic must arrive");
    assert_eq!(
        rec_http.path, "/ingest/mn10/http",
        "standard topic path must stay unencoded"
    );

    broker.finish().await;
}
