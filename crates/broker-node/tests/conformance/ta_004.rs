//! Conformance tests for core MQTT channel packet handling.
//!
//! Proves that the kernel listener handles ingress and egress MQTT
//! packets over real TCP sockets.

use super::support::*;

/// Builds an MQTT 3.1.1 UNSUBSCRIBE packet.
fn unsubscribe311(packet_id: u16, filters: &[&str]) -> Vec<u8> {
    let mut rest = Vec::new();
    rest.extend_from_slice(&packet_id.to_be_bytes());
    for filter in filters {
        rest.extend_from_slice(&mqtt_str(filter));
    }
    let mut packet = vec![0xA2];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// Builds an MQTT 5 CONNECT requesting JWT authentication without auth data.
fn connect5_jwt_no_data(client_id: &str) -> Vec<u8> {
    let mut props = Vec::new();
    props.push(21);
    props.extend_from_slice(&mqtt_str("JWT"));
    connect5_with_props(client_id, true, 60, &props)
}

/// An incoming QoS 2 PUBLISH receives a PUBREC packet with the matching packet identifier.
/// When the broker does not return PUBREC on QoS 2 publish,
/// `assert_eq!(head, 0x50, "QoS 2 publish receives PUBREC")` is the assertion that fails.
#[tokio::test]
async fn ta_004_qos2_publish_receives_pubrec() {
    let kernel = spawn_kernel("ta-004-q2-rec", None).await;
    let mut sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sock, "ta-004-q2-cli", true, 60).await);

    send(
        &mut sock,
        &publish311("ta04/q2/in", 10, 2, false, b"q2-payload"),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x50, "QoS 2 publish receives PUBREC");
    assert_eq!(
        &body[..2],
        &[0x00, 10],
        "PUBREC packet identifier matches QoS 2 publish"
    );
    kernel.finish().await;
}

/// An incoming SUBSCRIBE packet is accepted and receives a SUBACK packet.
/// When the broker does not acknowledge subscriptions with SUBACK,
/// `assert_eq!(head, 0x90, "SUBSCRIBE receives SUBACK")` is the assertion that fails.
#[tokio::test]
async fn ta_004_subscribe_receives_suback() {
    let kernel = spawn_kernel("ta-004-sub-ack", None).await;
    let mut sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sock, "ta-004-sub-cli", true, 60).await);

    send(&mut sock, &subscribe(1, &[("ta04/sub/test", 0)])).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x90, "SUBSCRIBE receives SUBACK");
    assert_eq!(
        &body[..2],
        &[0x00, 0x01],
        "SUBACK packet identifier matches subscribe"
    );
    assert_eq!(body[2], 0x00, "SUBACK return code is QoS 0 success");
    kernel.finish().await;
}

/// An incoming UNSUBSCRIBE packet is accepted and receives an UNSUBACK packet.
/// When the broker does not acknowledge unsubscriptions with UNSUBACK,
/// `assert_eq!(head, 0xB0, "UNSUBSCRIBE receives UNSUBACK")` is the assertion that fails.
#[tokio::test]
async fn ta_004_unsubscribe_receives_unsuback() {
    let kernel = spawn_kernel("ta-004-unsub-ack", None).await;
    let mut sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sock, "ta-004-unsub-cli", true, 60).await);

    send(&mut sock, &subscribe(1, &[("ta04/unsub/test", 0)])).await;
    let (head, _) = recv(&mut sock).await;
    assert_eq!(head, 0x90);

    send(&mut sock, &unsubscribe311(2, &["ta04/unsub/test"])).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0xB0, "UNSUBSCRIBE receives UNSUBACK");
    assert_eq!(
        &body[..2],
        &[0x00, 0x02],
        "UNSUBACK packet identifier matches unsubscribe"
    );
    kernel.finish().await;
}

/// An unauthorized subscription closes the connection when deny action is disconnect.
/// When the broker keeps the connection open on an unauthorized subscription,
/// `assert_eq!(head, 0xE0, "denied subscribe with disconnect deny_action sends DISCONNECT")` is the assertion that fails.
#[tokio::test]
async fn ta_004_subscribe_denied_disconnect_action_closes() {
    let kernel = spawn_kernel("ta-004-sub-deny", None).await;
    let http = reqwest::Client::new();
    let res = http
        .put(format!(
            "http://127.0.0.1:{}/api/v5/authorization/settings",
            kernel.api_port
        ))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({
            "no_match": "deny",
            "deny_action": "disconnect",
        }))
        .send()
        .await
        .expect("update authorization settings");
    assert_eq!(res.status(), 200, "authorization settings updated");
    let res = http
        .post(format!(
            "http://127.0.0.1:{}/api/v5/authorization/sources/built_in_database/rules/all",
            kernel.api_port
        ))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({
            "rules": [
                {
                    "action": "subscribe",
                    "permission": "deny",
                    "topic": "ta04/denied/#"
                }
            ]
        }))
        .send()
        .await
        .expect("update authorization rules");
    assert_eq!(res.status(), 204, "authorization rules updated");
    let mut sock = dial(kernel.mqtt_port).await;
    send(&mut sock, &connect5("ta-004-sub-deny-cli", true, 60, 0)).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x20);
    let (_, reason, _) = parse_connack5(&body);
    assert_eq!(reason, 0);

    send(&mut sock, &subscribe_v5(1, &[("ta04/denied/topic", 0)], 0)).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x90, "SUBSCRIBE receives SUBACK");
    assert_eq!(&body[..2], &[0x00, 0x01]);
    assert_eq!(body[3], 0x87, "SUBACK reason code is 0x87 Not authorized");

    let (head, body) = recv(&mut sock).await;
    assert_eq!(
        head, 0xE0,
        "denied subscribe with disconnect deny_action sends DISCONNECT"
    );
    assert_eq!(body[0], 0x87, "reason code is 0x87 Not authorized");

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        closed.is_err() || matches!(&closed, Ok(Err(_))),
        "socket closes after DISCONNECT"
    );
    kernel.finish().await;
}

/// A failed CONNECT receives a CONNACK packet carrying an error reason code.
/// When the broker does not return an error CONNACK on failed authentication,
/// `assert_eq!(reason, 0x87, "CONNACK carries failure reason code 0x87")` is the assertion that fails.
#[tokio::test]
async fn ta_004_connack_failure_returns_error_code() {
    let kernel = spawn_kernel("ta-004-fail-conn", None).await;
    let mut sock = dial(kernel.mqtt_port).await;
    send(&mut sock, &connect5_jwt_no_data("ta-004-fail-cli")).await;

    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x20);
    let (_, reason, _) = parse_connack5(&body);
    assert_eq!(reason, 0x87, "CONNACK carries failure reason code 0x87");

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        closed.is_err() || matches!(&closed, Ok(Err(_))),
        "socket closes after CONNACK failure"
    );
    kernel.finish().await;
}

/// An outbound PUBACK packet carries the matching packet identifier.
/// When the broker does not return PUBACK for QoS 1 publish,
/// `assert_eq!(head, 0x40, "QoS 1 publish receives PUBACK")` is the assertion that fails.
#[tokio::test]
async fn ta_004_outbound_puback_matches_packet_id() {
    let kernel = spawn_kernel("ta-004-out-ack", None).await;
    let mut sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sock, "ta-004-puback-cli", true, 60).await);

    send(
        &mut sock,
        &publish311("ta04/puback/test", 42, 1, false, b"puback-payload"),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x40, "QoS 1 publish receives PUBACK");
    assert_eq!(
        &body[..2],
        &[0x00, 42],
        "PUBACK packet identifier matches publish"
    );
    kernel.finish().await;
}

/// An outbound PUBCOMP packet completes a QoS 2 release with the matching packet identifier.
/// When the broker does not return PUBCOMP on PUBREL,
/// `assert_eq!(head, 0x70, "broker sends outbound PUBCOMP")` is the assertion that fails.
#[tokio::test]
async fn ta_004_outbound_pubcomp_matches_packet_id() {
    let kernel = spawn_kernel("ta-004-out-comp", None).await;
    let mut sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sock, "ta-004-pubcomp-cli", true, 60).await);

    send(
        &mut sock,
        &publish311("ta04/pubcomp/test", 15, 2, false, b"comp-payload"),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x50);
    assert_eq!(&body[..2], &[0x00, 15]);

    send(&mut sock, &ack(0x62, 15)).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x70, "broker sends outbound PUBCOMP");
    assert_eq!(
        &body[..2],
        &[0x00, 15],
        "PUBCOMP packet identifier matches PUBREL"
    );
    kernel.finish().await;
}

/// An outbound PUBREC packet acknowledges a QoS 2 publish with the matching packet identifier.
/// When the broker does not return PUBREC on QoS 2 publish,
/// `assert_eq!(head, 0x50, "broker sends outbound PUBREC")` is the assertion that fails.
#[tokio::test]
async fn ta_004_outbound_pubrec_matches_packet_id() {
    let kernel = spawn_kernel("ta-004-out-rec", None).await;
    let mut sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sock, "ta-004-pubrec-cli", true, 60).await);

    send(
        &mut sock,
        &publish311("ta04/pubrec/test", 25, 2, false, b"rec-payload"),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x50, "broker sends outbound PUBREC");
    assert_eq!(
        &body[..2],
        &[0x00, 25],
        "PUBREC packet identifier matches publish"
    );
    kernel.finish().await;
}

/// An outbound DISCONNECT packet is sent to an MQTT 5 client on a protocol error.
/// When the broker does not send DISCONNECT on a protocol error,
/// `assert_eq!(head, 0xE0, "server sends outbound DISCONNECT on protocol error")` is the assertion that fails.
#[tokio::test]
async fn ta_004_outbound_disconnect_on_protocol_error() {
    let kernel = spawn_kernel("ta-004-out-disc", None).await;
    let mut sock = dial(kernel.mqtt_port).await;
    send(&mut sock, &connect5("ta-004-disc-cli", true, 60, 0)).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x20);
    let (_, reason, _) = parse_connack5(&body);
    assert_eq!(reason, 0);

    send(&mut sock, &connect5("ta-004-disc-cli-again", true, 60, 0)).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(
        head, 0xE0,
        "server sends outbound DISCONNECT on protocol error"
    );
    assert_eq!(body[0], 0x82, "reason code is 0x82 Protocol Error");

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        closed.is_err() || matches!(&closed, Ok(Err(_))),
        "socket closes after DISCONNECT"
    );
    kernel.finish().await;
}

/// Multiple outbound PUBLISH packets are delivered to a subscriber.
/// When the broker fails to deliver queued messages to a subscriber,
/// `assert_eq!(head & 0xF0, 0x30, "subscriber receives outbound PUBLISH delivery")` is the assertion that fails.
#[tokio::test]
async fn ta_004_outbound_publish_batch_delivery() {
    let kernel = spawn_kernel("ta-004-batch", None).await;
    let mut sub = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub, "ta-004-batch-sub", true, 60).await);
    send(&mut sub, &subscribe(1, &[("ta04/batch/#", 1)])).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90);

    let mut pub_sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut pub_sock, "ta-004-batch-pub", true, 60).await);

    send(
        &mut pub_sock,
        &publish311("ta04/batch/q0", 0, 0, false, b"batch-q0"),
    )
    .await;
    send(
        &mut pub_sock,
        &publish311("ta04/batch/q1", 1, 1, false, b"batch-q1"),
    )
    .await;
    let (head, _) = recv(&mut pub_sock).await;
    assert_eq!(head, 0x40);

    let (head, body) = recv(&mut sub).await;
    assert_eq!(
        head & 0xF0,
        0x30,
        "subscriber receives outbound PUBLISH delivery"
    );
    let first = parse_publish(head, &body);
    assert_eq!(first.topic, "ta04/batch/q0");
    assert_eq!(first.payload, b"batch-q0");

    let (head, body) = recv(&mut sub).await;
    assert_eq!(
        head & 0xF0,
        0x30,
        "subscriber receives second outbound PUBLISH delivery"
    );
    let second = parse_publish(head, &body);
    assert_eq!(second.topic, "ta04/batch/q1");
    assert_eq!(second.payload, b"batch-q1");
    send(&mut sub, &ack(0x40, second.packet_id)).await;
    kernel.finish().await;
}

/// An outbound QoS 1 PUBLISH packet is delivered to a subscriber.
/// When the broker fails to deliver a QoS 1 message to a subscriber,
/// `assert_eq!(head & 0xF0, 0x30, "subscriber receives QoS 1 outbound PUBLISH")` is the assertion that fails.
#[tokio::test]
async fn ta_004_outbound_publish_qos1_delivery() {
    let kernel = spawn_kernel("ta-004-q1-single", None).await;
    let mut sub = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub, "ta-004-q1-sub", true, 60).await);
    send(&mut sub, &subscribe(1, &[("ta04/q1/single", 1)])).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90);

    let mut pub_sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut pub_sock, "ta-004-q1-pub", true, 60).await);
    send(
        &mut pub_sock,
        &publish311("ta04/q1/single", 100, 1, false, b"q1-single-payload"),
    )
    .await;
    let (head, body) = recv(&mut pub_sock).await;
    assert_eq!(head, 0x40);
    assert_eq!(&body[..2], &[0x00, 100]);

    let (head, body) = recv(&mut sub).await;
    assert_eq!(
        head & 0xF0,
        0x30,
        "subscriber receives QoS 1 outbound PUBLISH"
    );
    let live = parse_publish(head, &body);
    assert_eq!(live.qos, 1);
    assert_eq!(live.payload, b"q1-single-payload");
    send(&mut sub, &ack(0x40, live.packet_id)).await;
    kernel.finish().await;
}
