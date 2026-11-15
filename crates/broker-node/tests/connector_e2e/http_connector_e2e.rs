//! Tests for the HTTP webhook connector.
//!
//! Each test boots one real broker.
//! It creates the connector and rule.
//! It publishes on a real socket.
//! It reads the request at a receiver.

use std::path::Path;
use std::time::Duration;

use super::e2e_support::{TestBroker, TestTarget};
use serde_json::json;

fn webhook_config(url: &str) -> serde_json::Value {
    json!({"url": url, "batch_size": 1})
}

#[tokio::test]
async fn http_forward_raw_json_through_broker() {
    let target = TestTarget::start_http().await;
    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce01-http",
            "webhook",
            webhook_config(&format!("{}/hook", target.address())),
        )
        .await;
    broker
        .create_forward_rule("ce01-http-rule", "ce01/http/basic", "ce01-http")
        .await;

    broker
        .publish("ce01-http-pub", "ce01/http/basic", br#"{"v":41}"#, 0)
        .await;

    target.wait_for_count(1, Duration::from_secs(10)).await;
    let records = target.records();
    assert_eq!(records[0].method, "POST");
    assert_eq!(records[0].path, "/hook");
    let body: serde_json::Value = serde_json::from_slice(&records[0].body).expect("JSON body");
    assert_eq!(body, json!({"v": 41}));
    broker.finish().await;
}

#[tokio::test]
async fn http_methods_through_broker() {
    let target = TestTarget::start_http().await;
    let broker = TestBroker::start("").await;
    for (index, method) in ["post", "put", "patch"].iter().enumerate() {
        let topic = format!("ce01/http/method/{index}");
        let id = format!("ce01-http-method-{index}");
        broker
            .create_connector(
                &id,
                "webhook",
                json!({
                    "url": format!("{}/method/{index}", target.address()),
                    "method": method,
                    "batch_size": 1,
                }),
            )
            .await;
        broker
            .create_forward_rule(&format!("{id}-rule"), &topic, &id)
            .await;
        broker
            .publish(
                &format!("{id}-pub"),
                &topic,
                format!(r#"{{"n":{index}}}"#).as_bytes(),
                0,
            )
            .await;
    }
    target.wait_for_count(3, Duration::from_secs(10)).await;
    let records = target.records();
    for (index, expected) in ["POST", "PUT", "PATCH"].iter().enumerate() {
        let record = records
            .iter()
            .find(|record| record.path == format!("/method/{index}"))
            .expect("method request arrives");
        assert_eq!(&record.method, expected);
    }
    broker.finish().await;
}

#[tokio::test]
async fn http_headers_template_through_broker() {
    let target = TestTarget::start_http().await;
    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce01-http-headers",
            "webhook",
            json!({
                "url": format!("{}/headers", target.address()),
                "batch_size": 1,
                "headers": {"X-Topic": "${topic}", "X-Static": "yes"},
            }),
        )
        .await;
    broker
        .create_forward_rule(
            "ce01-http-headers-rule",
            "ce01/http/headers",
            "ce01-http-headers",
        )
        .await;
    broker
        .publish(
            "ce01-http-headers-pub",
            "ce01/http/headers",
            br#"{"v":1}"#,
            0,
        )
        .await;

    target.wait_for_count(1, Duration::from_secs(10)).await;
    let record = &target.records()[0];
    let header = |name: &str| {
        record
            .headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    };
    assert_eq!(header("X-Topic"), Some("ce01/http/headers"));
    assert_eq!(header("X-Static"), Some("yes"));
    broker.finish().await;
}

#[tokio::test]
async fn http_auth_modes_through_broker() {
    let target = TestTarget::start_http().await;
    let broker = TestBroker::start("").await;
    let cases = [
        (
            "none",
            json!({"type": "none"}),
            "authorization",
            None::<&str>,
        ),
        (
            "basic",
            json!({"type": "basic", "username": "qual", "password": "secret"}),
            "authorization",
            Some("Basic cXVhbDpzZWNyZXQ="),
        ),
        (
            "bearer",
            json!({"type": "bearer", "token": "token-123"}),
            "authorization",
            Some("Bearer token-123"),
        ),
        (
            "apikey",
            json!({"type": "apikey", "header_name": "X-Api-Key", "key": "key-123"}),
            "x-api-key",
            Some("key-123"),
        ),
    ];
    for (index, (name, auth, header_name, expected)) in cases.iter().enumerate() {
        let id = format!("ce01-http-auth-{name}");
        let topic = format!("ce01/http/auth/{index}");
        broker
            .create_connector(
                &id,
                "webhook",
                json!({
                    "url": format!("{}/auth/{index}", target.address()),
                    "batch_size": 1,
                    "auth": auth,
                }),
            )
            .await;
        broker
            .create_forward_rule(&format!("{id}-rule"), &topic, &id)
            .await;
        broker
            .publish(&format!("{id}-pub"), &topic, br#"{"v":1}"#, 0)
            .await;
        target
            .wait_for_count(index + 1, Duration::from_secs(10))
            .await;
        let record = target
            .records()
            .into_iter()
            .find(|record| record.path == format!("/auth/{index}"))
            .expect("auth request arrives");
        let value = record
            .headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(header_name))
            .map(|(_, value)| value.as_str());
        assert_eq!(value, *expected);
    }
    broker.finish().await;
}

#[tokio::test]
async fn http_hmac_sha256_hex_through_broker() {
    let target = TestTarget::start_http().await;
    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce01-http-hmac",
            "webhook",
            json!({
                "url": format!("{}/hmac", target.address()),
                "batch_size": 1,
                "signature": {
                    "header_name": "X-Signature-SHA256",
                    "algorithm": "sha256",
                    "secret": "s3cr3t",
                    "encoding": "hex",
                },
            }),
        )
        .await;
    broker
        .create_forward_rule("ce01-http-hmac-rule", "ce01/http/hmac", "ce01-http-hmac")
        .await;
    broker
        .publish("ce01-http-hmac-pub", "ce01/http/hmac", br#"{"v":7}"#, 0)
        .await;

    target.wait_for_count(1, Duration::from_secs(10)).await;
    let record = &target.records()[0];
    let signature = record
        .headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("X-Signature-SHA256"))
        .map(|(_, value)| value.as_str());
    assert_eq!(
        signature,
        Some("6e236275ab67f589d0e7e5c2f57f8d077390e38832f1677169260cc651a411a3")
    );
    broker.finish().await;
}

#[tokio::test]
async fn http_hmac_sha1_base64_through_broker() {
    let target = TestTarget::start_http().await;
    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce01-http-hmac-sha1",
            "webhook",
            json!({
                "url": format!("{}/hmac-sha1", target.address()),
                "batch_size": 1,
                "signature": {
                    "header_name": "X-Signature-SHA1",
                    "algorithm": "sha1",
                    "secret": "s3cr3t",
                    "encoding": "base64",
                },
            }),
        )
        .await;
    broker
        .create_forward_rule(
            "ce01-http-hmac-sha1-rule",
            "ce01/http/hmac-sha1",
            "ce01-http-hmac-sha1",
        )
        .await;
    broker
        .publish(
            "ce01-http-hmac-sha1-pub",
            "ce01/http/hmac-sha1",
            br#"{"v":8}"#,
            0,
        )
        .await;

    target.wait_for_count(1, Duration::from_secs(10)).await;
    let record = &target.records()[0];
    let signature = record
        .headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("X-Signature-SHA1"))
        .map(|(_, value)| value.as_str());
    assert_eq!(signature, Some("quy36/o0DGu6xAg+AU6iVy9ZCSc="));
    broker.finish().await;
}

#[tokio::test]
async fn http_batch_array_through_broker() {
    let target = TestTarget::start_http().await;
    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce01-http-batch",
            "webhook",
            json!({
                "url": format!("{}/batch", target.address()),
                "batch_size": 3,
                "linger_ms": 5000,
                "body_format": "jsonbatcharray",
            }),
        )
        .await;
    broker
        .create_forward_rule("ce01-http-batch-rule", "ce01/http/batch", "ce01-http-batch")
        .await;
    // Send all rows on one connection with QoS 1.
    // One connection keeps the order of the rows.
    let mut publisher = broker.connect("ce01-http-batch-pub").await;
    for value in 1..=3 {
        publisher
            .publish(
                "ce01/http/batch",
                format!(r#"{{"n":{value}}}"#).as_bytes(),
                1,
            )
            .await;
    }
    publisher.disconnect().await;

    target.expect_batch(3, Duration::from_secs(10)).await;
    let records = target.records();
    assert_eq!(records.len(), 1);
    let body: serde_json::Value = serde_json::from_slice(&records[0].body).expect("JSON body");
    assert_eq!(body, json!([{"n": 1}, {"n": 2}, {"n": 3}]));
    broker.finish().await;
}

#[tokio::test]
async fn http_form_urlencoded_through_broker() {
    let target = TestTarget::start_http().await;
    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce01-http-form",
            "webhook",
            json!({
                "url": format!("{}/form", target.address()),
                "batch_size": 1,
                "body_format": "formurlencoded",
            }),
        )
        .await;
    broker
        .create_forward_rule("ce01-http-form-rule", "ce01/http/form", "ce01-http-form")
        .await;
    broker
        .publish(
            "ce01-http-form-pub",
            "ce01/http/form",
            br#"{"message":"hello world"}"#,
            0,
        )
        .await;

    target.wait_for_count(1, Duration::from_secs(10)).await;
    let body = String::from_utf8(target.records()[0].body.clone()).expect("UTF-8 form");
    assert!(body.contains("topic=ce01%2Fhttp%2Fform"), "topic: {body}");
    assert!(body.contains("qos=0"), "qos: {body}");
    assert!(body.contains("payload="), "payload: {body}");
    assert!(body.contains("hello+world"), "payload text: {body}");
    broker.finish().await;
}

#[tokio::test]
async fn http_batch_bytes_through_broker() {
    let target = TestTarget::start_http().await;
    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce01-http-bytes",
            "webhook",
            json!({
                "url": format!("{}/bytes", target.address()),
                "batch_size": 100,
                "batch_bytes": 2000,
                "linger_ms": 5000,
                "body_format": "jsonbatcharray",
            }),
        )
        .await;
    broker
        .create_forward_rule("ce01-http-bytes-rule", "ce01/http/bytes", "ce01-http-bytes")
        .await;
    let pad = "a".repeat(800);
    for value in 1..=3 {
        broker
            .publish(
                "ce01-http-bytes-pub",
                "ce01/http/bytes",
                format!(r#"{{"n":{value},"pad":"{pad}"}}"#).as_bytes(),
                0,
            )
            .await;
    }

    target.expect_batch(3, Duration::from_secs(10)).await;
    let records = target.records();
    assert_eq!(records.len(), 1);
    let body: serde_json::Value = serde_json::from_slice(&records[0].body).expect("JSON body");
    assert_eq!(body.as_array().expect("array").len(), 3);
    broker.finish().await;
}

#[tokio::test]
async fn http_retry_after_429_through_broker() {
    let target = TestTarget::start_http().await;
    // Reason: 4 retries need max 5; default 3 fails.
    // Source: CE-01 P1 retry isolation.
    target.set_status_sequence(&[429, 429, 429, 429, 200]);
    let broker = TestBroker::start("").await;
    // Reason: 5 exceeds default 3 and proves retries.
    // Reason: 7 ms and 11 ms differ from 100/2000 defaults.
    // Reason: 5000 rows differ from 10000 default cap.
    // Source: CE-01 P1 non-default values.
    let config = json!({
        "url": format!("{}/retry", target.address()),
        "batch_size": 1,
        "max_retries": 5,
        "initial_backoff_ms": 7,
        "max_backoff_ms": 11,
        "buffer_capacity": 5000,
        "timeout_ms": 1000,
    });
    broker
        .create_connector("ce01-http-retry", "webhook", config)
        .await;
    let listed = broker.list_connectors().await;
    assert!(
        listed
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["id"] == "ce01-http-retry")),
        "connector record exists"
    );
    let rule = broker
        .create_forward_rule("ce01-http-retry-rule", "ce01/http/retry", "ce01-http-retry")
        .await;
    // Reason: start timer proves fast backoff.
    // Source: CE-01 Q1 timing isolation.
    let started = std::time::Instant::now();
    broker
        .publish("ce01-http-retry-pub", "ce01/http/retry", br#"{"v":9}"#, 0)
        .await;

    target.wait_for_count(1, Duration::from_secs(10)).await;
    assert_eq!(target.http_statuses(), vec![429, 429, 429, 429, 200]);
    let (total, success, failed) = broker.rule_actions(&rule).await;
    assert_eq!((total, success, failed), (1, 1, 0));
    // Reason: 2 s separates 7/11 ms backoff from 100/2000 ms defaults.
    // Source: backoff_delay at http.rs:791-793.
    assert!(
        started.elapsed() < Duration::from_millis(2000),
        "fast backoff stays under 2 s"
    );
    broker.finish().await;
}

#[tokio::test]
async fn http_initial_backoff_ms_through_broker() {
    let target = TestTarget::start_http().await;
    // Reason: 3 failures fit default 3 retries.
    // Source: CE-01 R1 retry count.
    target.set_status_sequence(&[429, 429, 429, 200]);
    let broker = TestBroker::start("").await;
    // Reason: 7 ms differs from 100 ms default.
    // Reason: 2000 ms equals default ceiling.
    // Source: backoff_delay at http.rs:791-797.
    let config = json!({
        "url": format!("{}/initial", target.address()),
        "batch_size": 1,
        "max_retries": 3,
        "initial_backoff_ms": 7,
        "max_backoff_ms": 2000,
    });
    broker
        .create_connector("ce01-http-initial", "webhook", config)
        .await;
    let rule = broker
        .create_forward_rule(
            "ce01-http-initial-rule",
            "ce01/http/initial",
            "ce01-http-initial",
        )
        .await;
    // Reason: start timer proves fast backoff.
    // Source: CE-01 R1 timing isolation.
    let started = std::time::Instant::now();
    broker
        .publish(
            "ce01-http-initial-pub",
            "ce01/http/initial",
            br#"{"v":9}"#,
            0,
        )
        .await;

    target.wait_for_count(1, Duration::from_secs(10)).await;
    assert_eq!(target.http_statuses(), vec![429, 429, 429, 200]);
    let (total, success, failed) = broker.rule_actions(&rule).await;
    assert_eq!((total, success, failed), (1, 1, 0));
    // Reason: 500 ms separates 7 ms backoff from 100 ms default.
    // Source: backoff_delay at http.rs:791-797.
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "fast initial backoff stays under 500 ms"
    );
    broker.finish().await;
}

#[tokio::test]
async fn http_max_backoff_ms_through_broker() {
    let target = TestTarget::start_http().await;
    // Reason: 3 failures fit default 3 retries.
    // Source: CE-01 R1 retry count.
    target.set_status_sequence(&[429, 429, 429, 200]);
    let broker = TestBroker::start("").await;
    // Reason: 100 ms equals default first delay.
    // Reason: 120 ms differs from 2000 ms default.
    // Source: backoff_delay at http.rs:791-797.
    let config = json!({
        "url": format!("{}/ceiling", target.address()),
        "batch_size": 1,
        "max_retries": 3,
        "initial_backoff_ms": 100,
        "max_backoff_ms": 120,
    });
    broker
        .create_connector("ce01-http-ceiling", "webhook", config)
        .await;
    let rule = broker
        .create_forward_rule(
            "ce01-http-ceiling-rule",
            "ce01/http/ceiling",
            "ce01-http-ceiling",
        )
        .await;
    // Reason: start timer proves capped backoff.
    // Source: CE-01 R1 timing isolation.
    let started = std::time::Instant::now();
    broker
        .publish(
            "ce01-http-ceiling-pub",
            "ce01/http/ceiling",
            br#"{"v":9}"#,
            0,
        )
        .await;

    target.wait_for_count(1, Duration::from_secs(10)).await;
    assert_eq!(target.http_statuses(), vec![429, 429, 429, 200]);
    let (total, success, failed) = broker.rule_actions(&rule).await;
    assert_eq!((total, success, failed), (1, 1, 0));
    // Reason: 1000 ms separates 120 ms ceiling from 2000 ms default.
    // Source: backoff_delay at http.rs:791-797.
    assert!(
        started.elapsed() < Duration::from_millis(1000),
        "capped backoff stays under 1 s"
    );
    broker.finish().await;
}

#[tokio::test]
async fn http_timeout_is_applied_through_broker() {
    let target = TestTarget::start_http().await;
    target.slow_server();
    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce01-http-timeout",
            "webhook",
            json!({
                "url": format!("{}/slow", target.address()),
                "batch_size": 1,
                "max_retries": 0,
                "timeout_ms": 100,
            }),
        )
        .await;
    let rule = broker
        .create_forward_rule(
            "ce01-http-timeout-rule",
            "ce01/http/timeout",
            "ce01-http-timeout",
        )
        .await;
    broker
        .publish(
            "ce01-http-timeout-pub",
            "ce01/http/timeout",
            br#"{"v":1}"#,
            0,
        )
        .await;

    let (_, _, failed) = broker
        .wait_for_action_failures(&rule, 1, Duration::from_secs(10))
        .await;
    assert!(failed >= 1, "timeout must fail the action");
    broker.finish().await;
}

#[tokio::test]
async fn http_retry_after_server_down_through_broker() {
    let target = TestTarget::start_http().await;
    target.set_fault("down");
    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce01-http-down",
            "webhook",
            json!({
                "url": format!("{}/down", target.address()),
                "batch_size": 1,
                "max_retries": 0,
                "initial_backoff_ms": 1,
                "max_backoff_ms": 2,
                "timeout_ms": 1000,
            }),
        )
        .await;
    let rule = broker
        .create_forward_rule("ce01-http-down-rule", "ce01/http/down", "ce01-http-down")
        .await;
    // Send both rows on one connection with QoS 1.
    // One connection keeps the order of the rows.
    let mut publisher = broker.connect("ce01-http-down-pub").await;
    publisher.publish("ce01/http/down", br#"{"v":1}"#, 1).await;
    broker
        .wait_for_action_failures(&rule, 1, Duration::from_secs(10))
        .await;

    target.stop_and_restart_server().await;
    tokio::time::sleep(Duration::from_millis(2100)).await;
    publisher.publish("ce01/http/down", br#"{"v":2}"#, 1).await;
    publisher.disconnect().await;
    target.wait_for_count(2, Duration::from_secs(10)).await;
    let bodies: Vec<serde_json::Value> = target
        .records()
        .iter()
        .map(|record| serde_json::from_slice(&record.body).expect("JSON body"))
        .collect();
    assert_eq!(bodies, vec![json!({"v": 1}), json!({"v": 2})]);
    broker.finish().await;
}

#[tokio::test]
async fn http_buffer_capacity_fails_closed_through_broker() {
    let target = TestTarget::start_http().await;
    let mut broker = TestBroker::start("").await;
    // Reason: 2 rows prove bound; default 10000 hides it.
    // Reason: 100 rows keep the batch open, so the bound shows before a send.
    // Reason: 60000 ms keeps a slow CI from flushing the partial batch.
    // Source: CE-01 Q2 buffer isolation.
    let config = json!({
        "url": format!("{}/buffer", target.address()),
        "batch_size": 100,
        "buffer_capacity": 2,
        "linger_ms": 60000,
        "max_retries": 0,
        "timeout_ms": 1000,
        "body_format": "jsonbatcharray",
    });
    broker
        .create_connector("ce01-http-buffer", "webhook", config)
        .await;
    let listed = broker.list_connectors().await;
    assert!(
        listed
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["id"] == "ce01-http-buffer")),
        "connector record exists"
    );
    let rule = broker
        .create_forward_rule(
            "ce01-http-buffer-rule",
            "ce01/http/buffer",
            "ce01-http-buffer",
        )
        .await;
    // Reason: the open batch fills the bound without a send or a backoff.
    // Source: CE-01 Q2 capacity error, not backoff error.
    // Publish all six rows on one connection with QoS 1.
    // One connection keeps the order of the rows.
    // Reason: 2 ms keeps the rows apart on loopback.
    // Source: task buffer case.
    const ROW_SPACING: Duration = Duration::from_millis(2);
    let mut publisher = broker.connect("ce01-http-buffer-pub").await;
    for value in 1..=6 {
        if value > 1 {
            tokio::time::sleep(ROW_SPACING).await;
        }
        publisher
            .publish(
                "ce01/http/buffer",
                format!(r#"{{"v":{value}}}"#).as_bytes(),
                1,
            )
            .await;
    }
    publisher.disconnect().await;
    let (_, _, failed) = broker
        .wait_for_action_failures(&rule, 4, Duration::from_secs(10))
        .await;
    assert_eq!(failed, 4, "one refused row while full");
    assert_eq!(target.count_in_server(), 0, "refused row never leaves");
    // Reason: an orderly stop drains the two buffered rows.
    // Source: CE-01 item 4 orderly stop.
    broker.stop_and_restart_orderly().await;
    target.wait_for_count(2, Duration::from_secs(10)).await;
    let records = target.records();
    assert_eq!(records.len(), 1, "one drained batch");
    let body: serde_json::Value = serde_json::from_slice(&records[0].body).expect("JSON body");
    assert_eq!(body, json!([{"v": 1}, {"v": 2}]));
    broker.finish().await;
}

#[tokio::test]
async fn http_orderly_stop_with_partial_batch_through_broker() {
    let target = TestTarget::start_http().await;
    let mut broker = TestBroker::start("").await;
    // Reason: 3 proves size trigger; default is 100.
    // Reason: 5000 ms stops stale flush during test.
    // Source: CE-01 item 4 partial batch.
    broker
        .create_connector(
            "ce01-http-stop",
            "webhook",
            json!({
                "url": format!("{}/stop", target.address()),
                "batch_size": 3,
                "linger_ms": 5000,
                "body_format": "jsonbatcharray",
            }),
        )
        .await;
    let rule = broker
        .create_forward_rule("ce01-http-stop-rule", "ce01/http/stop", "ce01-http-stop")
        .await;
    // Send both rows on one connection with QoS 1.
    // One connection keeps the order of the rows.
    let mut publisher = broker.connect("ce01-http-stop-pub").await;
    for value in 1..=2 {
        publisher
            .publish(
                "ce01/http/stop",
                format!(r#"{{"n":{value}}}"#).as_bytes(),
                1,
            )
            .await;
    }
    publisher.disconnect().await;
    broker
        .wait_for_action_success(&rule, 2, Duration::from_secs(10))
        .await;
    assert_eq!(target.count_in_server(), 0, "partial batch stays buffered");
    broker.stop_and_restart_orderly().await;

    target.expect_batch(2, Duration::from_secs(10)).await;
    let records = target.records();
    assert_eq!(records.len(), 1);
    let body: serde_json::Value = serde_json::from_slice(&records[0].body).expect("JSON body");
    assert_eq!(body, json!([{"n": 1}, {"n": 2}]));
    broker.finish().await;
}

#[tokio::test]
async fn http_tls_through_broker() {
    let target = TestTarget::start_https().await;
    let ca = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("k1_05")
        .join("ca-cert.pem");
    let broker =
        TestBroker::start_with_env("", &[("SSL_CERT_FILE", ca.display().to_string())]).await;
    broker
        .create_connector(
            "ce01-http-tls",
            "webhook",
            webhook_config(&format!("{}/tls", target.address())),
        )
        .await;
    broker
        .create_forward_rule("ce01-http-tls-rule", "ce01/http/tls", "ce01-http-tls")
        .await;
    broker
        .publish("ce01-http-tls-pub", "ce01/http/tls", br#"{"v":3}"#, 0)
        .await;

    target.wait_for_count(1, Duration::from_secs(10)).await;
    let body: serde_json::Value =
        serde_json::from_slice(&target.records()[0].body).expect("JSON body");
    assert_eq!(body, json!({"v": 3}));
    broker.finish().await;
}

// BUG(parity): with batch_size > 1 and no later publish, linger_ms does not flush the partial batch.
#[tokio::test]
#[ignore = "defect: no timer flush; a partial batch waits for the next publish"]
async fn http_partial_batch_flush_on_linger() {
    let target = TestTarget::start_http().await;
    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce01-http-linger",
            "webhook",
            json!({
                "url": format!("{}/linger", target.address()),
                "batch_size": 10,
                "linger_ms": 50,
                "body_format": "jsonbatcharray",
            }),
        )
        .await;
    broker
        .create_forward_rule(
            "ce01-http-linger-rule",
            "ce01/http/linger",
            "ce01-http-linger",
        )
        .await;
    broker
        .publish("ce01-http-linger-pub", "ce01/http/linger", br#"{"v":1}"#, 0)
        .await;

    target.wait_for_count(1, Duration::from_millis(500)).await;
    broker.finish().await;
}
