//! Tests for the remote MQTT bridge connector.
//!
//! Each test boots one real broker.
//! It creates the bridge and rule.
//! It publishes on a real socket.
//! It reads at a second test broker.

use std::time::Duration;

use super::e2e_support::{TestBroker, TestTarget};

use broker_connectors::MqttBridgeTransport as _;
use serde_json::json;

fn bridge_config(address: &str, client_id: &str) -> serde_json::Value {
    // Reason: false differs from default true.
    // Source: CE-01 P2 clean_start isolation.
    json!({
        "broker_address": address,
        "client_id": client_id,
        "clean_start": false,
        "keep_alive_secs": 30,
        "topic_template": "${topic}",
        "qos_override": 1,
        "retain_override": false,
        "max_inflight": 1000,
        "max_batch_size": 1,
        "linger_ms": 10,
        "protocol": "v311",
    })
}

#[tokio::test]
async fn mqtt_bridge_forward_through_broker() {
    let target = TestTarget::start_mqtt().await;
    let broker = TestBroker::start("").await;
    let config = bridge_config(&target.address(), "ce01-bridge");
    broker
        .create_connector("ce01-mqtt", "mqtt_bridge", config)
        .await;
    broker
        .create_forward_rule("ce01-mqtt-rule", "ce01/mqtt/basic", "ce01-mqtt")
        .await;
    broker
        .publish("ce01-mqtt-pub", "ce01/mqtt/basic", br#"{"v":1}"#, 0)
        .await;

    target.wait_for_count(1, Duration::from_secs(10)).await;
    let records = target.records();
    assert_eq!(records[0].topic, "ce01/mqtt/basic");
    assert_eq!(records[0].payload, br#"{"v":1}"#);
    assert_eq!(records[0].qos, 1);
    assert!(!records[0].retain);
    assert_eq!(target.client_ids(), vec!["ce01-bridge"]);
    assert_eq!(target.clean_starts(), vec![false]);
    assert_eq!(target.keepalives(), vec![30]);
    assert_eq!(target.protocol_levels(), vec![4]);
    broker.finish().await;
}

#[tokio::test]
async fn mqtt_bridge_auth_through_broker() {
    let target = TestTarget::start_mqtt_auth("qual-user", "qual-pass").await;
    let broker = TestBroker::start("").await;
    let mut config = bridge_config(&target.address(), "ce01-bridge-auth");
    config["username"] = json!("qual-user");
    config["password"] = json!("qual-pass");
    broker
        .create_connector("ce01-mqtt-auth", "mqtt_bridge", config)
        .await;
    broker
        .create_forward_rule("ce01-mqtt-auth-rule", "ce01/mqtt/auth", "ce01-mqtt-auth")
        .await;
    broker
        .publish("ce01-mqtt-auth-pub", "ce01/mqtt/auth", br#"{"v":2}"#, 0)
        .await;

    target.wait_for_count(1, Duration::from_secs(10)).await;
    assert_eq!(target.records()[0].payload, br#"{"v":2}"#);
    broker.finish().await;
}

#[tokio::test]
async fn mqtt_bridge_topic_mapping_through_broker() {
    let target = TestTarget::start_mqtt().await;
    let broker = TestBroker::start("").await;
    let mut config = bridge_config(&target.address(), "ce01-bridge-map");
    config["topic_prefix"] = json!("edge/site1");
    config["topic_template"] = json!("mapped/${topic}");
    config["qos_override"] = json!(0);
    config["retain_override"] = json!(true);
    broker
        .create_connector("ce01-mqtt-map", "mqtt_bridge", config)
        .await;
    broker
        .create_forward_rule("ce01-mqtt-map-rule", "ce01/mqtt/map", "ce01-mqtt-map")
        .await;
    broker
        .publish("ce01-mqtt-map-pub", "ce01/mqtt/map", br#"{"v":3}"#, 0)
        .await;

    target.wait_for_count(1, Duration::from_secs(10)).await;
    let record = &target.records()[0];
    assert_eq!(record.topic, "edge/site1/mapped/ce01/mqtt/map");
    assert_eq!(record.qos, 0);
    assert!(record.retain);
    broker.finish().await;
}

#[tokio::test]
async fn mqtt_bridge_payload_template_through_broker() {
    let target = TestTarget::start_mqtt().await;
    let broker = TestBroker::start("").await;
    let mut config = bridge_config(&target.address(), "ce01-bridge-body");
    config["payload_template"] = json!("${topic}:${payload}");
    broker
        .create_connector("ce01-mqtt-body", "mqtt_bridge", config)
        .await;
    broker
        .create_forward_rule("ce01-mqtt-body-rule", "ce01/mqtt/body", "ce01-mqtt-body")
        .await;
    broker
        .publish("ce01-mqtt-body-pub", "ce01/mqtt/body", br#"{"v":4}"#, 0)
        .await;

    target.wait_for_count(1, Duration::from_secs(10)).await;
    let records = target.records();
    assert_eq!(records[0].payload, br#"ce01/mqtt/body:{"v":4}"#);
    broker.finish().await;
}

#[tokio::test]
async fn mqtt_bridge_batching_through_broker() {
    let target = TestTarget::start_mqtt().await;
    let broker = TestBroker::start("").await;
    let mut config = bridge_config(&target.address(), "ce01-bridge-batch");
    config["qos_override"] = json!(0);
    // Reason: 3 differs from default 100 and proves size.
    // Reason: 5000 ms stops stale flush during test.
    // Source: CE-01 P2 single batch of 3.
    config["max_batch_size"] = json!(3);
    config["linger_ms"] = json!(5000);
    broker
        .create_connector("ce01-mqtt-batch", "mqtt_bridge", config)
        .await;
    broker
        .create_forward_rule("ce01-mqtt-batch-rule", "ce01/mqtt/batch", "ce01-mqtt-batch")
        .await;
    for value in 1..=2 {
        broker
            .publish(
                "ce01-mqtt-batch-pub",
                "ce01/mqtt/batch",
                format!(r#"{{"n":{value}}}"#).as_bytes(),
                0,
            )
            .await;
    }
    // Reason: 300 ms is under the 5000 ms linger.
    // Source: proves two rows do not flush alone.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(target.count_in_server(), 0);
    broker
        .publish("ce01-mqtt-batch-pub", "ce01/mqtt/batch", br#"{"n":3}"#, 0)
        .await;

    target.expect_mqtt_batch(3, Duration::from_secs(10)).await;
    let payloads: Vec<Vec<u8>> = target
        .records()
        .into_iter()
        .map(|record| record.payload)
        .collect();
    assert_eq!(
        payloads,
        vec![
            br#"{"n":1}"#.to_vec(),
            br#"{"n":2}"#.to_vec(),
            br#"{"n":3}"#.to_vec()
        ]
    );
    broker.finish().await;
}

#[tokio::test]
async fn mqtt_bridge_retry_after_server_down_through_broker() {
    let target = TestTarget::start_mqtt().await;
    target.set_fault("down");
    let broker = TestBroker::start("").await;
    let mut config = bridge_config(&target.address(), "ce01-bridge-down");
    config["qos_override"] = json!(0);
    broker
        .create_connector("ce01-mqtt-down", "mqtt_bridge", config)
        .await;
    let rule = broker
        .create_forward_rule("ce01-mqtt-down-rule", "ce01/mqtt/down", "ce01-mqtt-down")
        .await;
    broker
        .publish("ce01-mqtt-down-pub", "ce01/mqtt/down", br#"{"v":1}"#, 0)
        .await;
    broker
        .wait_for_action_failures(&rule, 1, Duration::from_secs(10))
        .await;

    target.set_fault("up");
    tokio::time::sleep(Duration::from_millis(2100)).await;
    broker
        .publish("ce01-mqtt-down-pub2", "ce01/mqtt/down", br#"{"v":2}"#, 0)
        .await;
    target.wait_for_count(2, Duration::from_secs(10)).await;
    let payloads: Vec<Vec<u8>> = target
        .records()
        .into_iter()
        .map(|record| record.payload)
        .collect();
    assert_eq!(
        payloads,
        vec![br#"{"v":1}"#.to_vec(), br#"{"v":2}"#.to_vec()]
    );
    broker.finish().await;
}

#[tokio::test]
async fn mqtt_bridge_inflight_cap_through_broker() {
    let target = TestTarget::start_mqtt().await;
    let broker = TestBroker::start("").await;
    let mut config = bridge_config(&target.address(), "ce01-bridge-inflight");
    config["qos_override"] = json!(0);
    // Reason: 2 rows prove bound; default 10000 hides it.
    // Reason: 100 rows keep the batch open, so the bound shows before a send.
    // Reason: 60000 ms keeps a slow CI from flushing the partial batch.
    // Source: CE-01 R2 inflight isolation.
    config["max_inflight"] = json!(2);
    config["max_batch_size"] = json!(100);
    config["linger_ms"] = json!(60000);
    broker
        .create_connector("ce01-mqtt-inflight", "mqtt_bridge", config)
        .await;
    let listed = broker.list_connectors().await;
    assert!(
        listed
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["id"] == "ce01-mqtt-inflight")),
        "connector record exists"
    );
    let rule = broker
        .create_forward_rule(
            "ce01-mqtt-inflight-rule",
            "ce01/mqtt/inflight",
            "ce01-mqtt-inflight",
        )
        .await;
    // Reason: the open batch fills the bound without a send or a backoff.
    // Source: CE-01 R2 inflight error, not backoff error.
    for value in 1..=6 {
        broker
            .publish(
                "ce01-mqtt-inflight-pub",
                "ce01/mqtt/inflight",
                format!(r#"{{"v":{value}}}"#).as_bytes(),
                0,
            )
            .await;
    }
    let (_, _, failed) = broker
        .wait_for_action_failures(&rule, 4, Duration::from_secs(10))
        .await;
    assert_eq!(failed, 4, "one refused row while full");
    assert_eq!(target.count_in_server(), 0, "refused rows never leave");
    broker.finish().await;
}

#[tokio::test]
async fn mqtt_bridge_v50_through_broker() {
    let target = TestTarget::start_mqtt().await;
    let broker = TestBroker::start("").await;
    let mut config = bridge_config(&target.address(), "ce01-bridge-v50");
    config["protocol"] = json!("v50");
    config["max_batch_size"] = json!(1);
    broker
        .create_connector("ce01-mqtt-v50", "mqtt_bridge", config)
        .await;
    broker
        .create_forward_rule("ce01-mqtt-v50-rule", "ce01/mqtt/v50", "ce01-mqtt-v50")
        .await;
    broker
        .publish("ce01-mqtt-v50-pub", "ce01/mqtt/v50", br#"{"v":5}"#, 0)
        .await;

    target.wait_for_count(1, Duration::from_secs(10)).await;
    let record = &target.records()[0];
    assert_eq!(record.payload, br#"{"v":5}"#);
    assert_eq!(record.qos, 1);
    assert_eq!(target.protocol_levels(), vec![5]);
    broker.finish().await;
}

#[tokio::test]
async fn mqtt_bridge_orderly_stop_with_partial_batch_through_broker() {
    let target = TestTarget::start_mqtt().await;
    let mut broker = TestBroker::start("").await;
    let mut config = bridge_config(&target.address(), "ce01-bridge-stop");
    config["qos_override"] = json!(0);
    // Reason: 3 proves size trigger; default is 100.
    // Reason: 5000 ms stops stale flush during test.
    // Source: CE-01 item 4 partial batch.
    config["max_batch_size"] = json!(3);
    config["linger_ms"] = json!(5000);
    broker
        .create_connector("ce01-mqtt-stop", "mqtt_bridge", config)
        .await;
    let rule = broker
        .create_forward_rule("ce01-mqtt-stop-rule", "ce01/mqtt/stop", "ce01-mqtt-stop")
        .await;
    for value in 1..=2 {
        broker
            .publish(
                "ce01-mqtt-stop-pub",
                "ce01/mqtt/stop",
                format!(r#"{{"n":{value}}}"#).as_bytes(),
                0,
            )
            .await;
    }
    broker
        .wait_for_action_success(&rule, 2, Duration::from_secs(10))
        .await;
    assert_eq!(target.count_in_server(), 0, "partial batch stays buffered");
    broker.stop_and_restart_orderly().await;

    target.expect_mqtt_batch(2, Duration::from_secs(10)).await;
    let payloads: Vec<Vec<u8>> = target
        .records()
        .into_iter()
        .map(|record| record.payload)
        .collect();
    assert_eq!(
        payloads,
        vec![br#"{"n":1}"#.to_vec(), br#"{"n":2}"#.to_vec()]
    );
    broker.finish().await;
}

#[tokio::test]
async fn mqtt_bridge_tls_fails_closed_through_broker() {
    let target = TestTarget::start_mqtt().await;
    // Reason: real port proves TLS refusal, not dial failure.
    // Source: CE-01 P3 mqtts refusal at mqtt_bridge.rs.
    let tls_address = target.local_address().replacen("mqtt://", "mqtts://", 1);
    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce01-mqtt-tls",
            "mqtt_bridge",
            json!({
                "broker_address": tls_address,
                "client_id": "ce01-bridge-tls",
                "max_batch_size": 1,
                "qos_override": 0,
            }),
        )
        .await;
    let rule = broker
        .create_forward_rule("ce01-mqtt-tls-rule", "ce01/mqtt/tls", "ce01-mqtt-tls")
        .await;
    broker
        .publish("ce01-mqtt-tls-pub", "ce01/mqtt/tls", br#"{"v":6}"#, 0)
        .await;

    let (_, _, failed) = broker
        .wait_for_action_failures(&rule, 1, Duration::from_secs(10))
        .await;
    assert_eq!(failed, 1);
    assert_eq!(target.count_in_server(), 0);
    // Reason: small bound keeps the probe fast.
    // Source: CE-01 P3 exact refusal text.
    let config = broker_connectors::MqttBridgeSinkConfig {
        broker_address: tls_address,
        client_id: "ce01-bridge-tls".to_string(),
        clean_start: true,
        username: None,
        password: None,
        keep_alive_secs: 30,
        topic_prefix: None,
        topic_template: None,
        payload_template: None,
        qos_override: Some(0),
        retain_override: None,
        max_inflight: Some(10),
        max_batch_size: Some(1),
        linger_ms: Some(10),
        protocol: broker_connectors::MqttBridgeProtocol::V311,
    };
    let transport =
        broker_connectors::RumqttcMqttBridgeTransport::new(&config).expect("transport builds");
    let error = transport
        .connect()
        .await
        .expect_err("mqtts must fail closed");
    let message = match error {
        broker_connectors::ConnectorError::Dispatch(message) => message,
        other => panic!("expected dispatch failure, got {other}"),
    };
    assert_eq!(
        message,
        "mqtts TLS transport not enabled in this build; use mqtt:// or terminate TLS in a sidecar proxy"
    );
    broker.finish().await;
}

// BUG(parity): with batch_size > 1 and no later publish, linger_ms does not flush the partial batch.
#[tokio::test]
#[ignore = "defect: no timer flush; a partial batch waits for the next publish"]
async fn mqtt_bridge_partial_batch_flush_on_linger() {
    let target = TestTarget::start_mqtt().await;
    let broker = TestBroker::start("").await;
    let mut config = bridge_config(&target.address(), "ce01-bridge-linger");
    config["qos_override"] = json!(0);
    config["max_batch_size"] = json!(10);
    config["linger_ms"] = json!(50);
    broker
        .create_connector("ce01-mqtt-linger", "mqtt_bridge", config)
        .await;
    broker
        .create_forward_rule(
            "ce01-mqtt-linger-rule",
            "ce01/mqtt/linger",
            "ce01-mqtt-linger",
        )
        .await;
    broker
        .publish("ce01-mqtt-linger-pub", "ce01/mqtt/linger", br#"{"v":1}"#, 0)
        .await;

    target.wait_for_count(1, Duration::from_millis(500)).await;
    broker.finish().await;
}
