//! Conformance tests for core MQTT channel packet handling.
//!
//! Proves that the kernel listener handles ingress and egress MQTT
//! packets over real TCP sockets.

use super::support::*;

/// Builds an MQTT 5 PUBLISH carrying a Subscription Identifier property.
fn publish5_sub_id(topic: &str, packet_id: u16, qos: u8, sub_id: u32, payload: &[u8]) -> Vec<u8> {
    let mut rest = mqtt_str(topic);
    if qos > 0 {
        rest.extend_from_slice(&packet_id.to_be_bytes());
    }
    let mut props = vec![11u8];
    props.extend_from_slice(&varint(sub_id));
    rest.extend_from_slice(&varint(props.len() as u32));
    rest.extend_from_slice(&props);
    rest.extend_from_slice(payload);
    let mut packet = vec![0x30 | (qos << 1)];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// A packet sent after DISCONNECT is dropped and the socket closes.
/// When the broker processes packets after DISCONNECT,
/// `assert!(quiet.is_err() || matches!(&quiet, Ok(Err(_))), "no PINGRESP sent after DISCONNECT")` is the assertion that fails.
#[tokio::test]
async fn ta_003_packet_when_disconnected_is_dropped() {
    let kernel = spawn_kernel("ta-003-pdisc", None).await;
    let mut sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sock, "ta-003-pdisc-cli", true, 60).await);

    send(&mut sock, &[0xE0, 0x00, 0xC0, 0x00]).await;
    let quiet = tokio::time::timeout(QUIET, read_mqtt_packet(&mut sock)).await;
    assert!(
        quiet.is_err() || matches!(&quiet, Ok(Err(_))),
        "no PINGRESP sent after DISCONNECT"
    );
    kernel.finish().await;
}

/// A duplicate PUBACK for an in-use or retired ID is ignored and keeps the connection open.
/// When the broker closes the connection on duplicate PUBACK,
/// `assert_eq!(head, 0xD0, "connection stays open after duplicate PUBACK")` is the assertion that fails.
#[tokio::test]
async fn ta_003_puback_id_in_use_keeps_connection() {
    let kernel = spawn_kernel("ta-003-ack-use", None).await;
    let topic = "ta03/ack/inuse";
    let mut sub = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub, "ta-003-ack-use-sub", false, 60).await);
    send(&mut sub, &subscribe(1, &[(topic, 1)])).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90);

    let mut pub_sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut pub_sock, "ta-003-ack-use-pub", true, 60).await);
    send(&mut pub_sock, &publish311(topic, 10, 1, false, b"ack-use")).await;
    let (head, _) = recv(&mut pub_sock).await;
    assert_eq!(head, 0x40);

    let (head, body) = recv(&mut sub).await;
    let live = parse_publish(head, &body);
    send(&mut sub, &ack(0x40, live.packet_id)).await;
    send(&mut sub, &ack(0x40, live.packet_id)).await;

    send(&mut sub, &[0xC0, 0x00]).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0xD0, "connection stays open after duplicate PUBACK");
    kernel.finish().await;
}

/// A PUBACK with an unknown packet ID is ignored and keeps the connection open.
/// When the broker disconnects on unknown PUBACK,
/// `assert_eq!(head, 0xD0, "connection stays open after unknown PUBACK")` is the assertion that fails.
#[tokio::test]
async fn ta_003_puback_id_not_found_keeps_connection() {
    let kernel = spawn_kernel("ta-003-ack-nf", None).await;
    let mut sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sock, "ta-003-ack-nf-cli", true, 60).await);

    send(&mut sock, &ack(0x40, 0x9999)).await;
    send(&mut sock, &[0xC0, 0x00]).await;
    let (head, _) = recv(&mut sock).await;
    assert_eq!(head, 0xD0, "connection stays open after unknown PUBACK");
    kernel.finish().await;
}

/// A valid PUBACK releases the inflight entry so it does not replay on reconnect.
/// When the inflight entry is not released,
/// `assert!(quiet.is_err(), "no replay after PUBACK released inflight")` is the assertion that fails.
#[tokio::test]
async fn ta_003_puback_ok_releases_inflight() {
    let kernel = spawn_kernel("ta-003-ack-ok", None).await;
    let topic = "ta03/ack/ok";
    let mut sub = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub, "ta-003-ack-ok-sub", false, 60).await);
    send(&mut sub, &subscribe(1, &[(topic, 1)])).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90);

    let mut pub_sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut pub_sock, "ta-003-ack-ok-pub", true, 60).await);
    send(&mut pub_sock, &publish311(topic, 1, 1, false, b"ack-ok")).await;
    let (head, _) = recv(&mut pub_sock).await;
    assert_eq!(head, 0x40);

    let (head, body) = recv(&mut sub).await;
    let live = parse_publish(head, &body);
    send(&mut sub, &ack(0x40, live.packet_id)).await;
    drop(sub);

    let mut resume = dial(kernel.mqtt_port).await;
    send(&mut resume, &connect311("ta-003-ack-ok-sub", false, 60)).await;
    let (head, body) = recv(&mut resume).await;
    assert_eq!(head, 0x20);
    assert_eq!(parse_connack311(&body), (true, 0));

    let quiet = tokio::time::timeout(QUIET, read_mqtt_packet(&mut resume)).await;
    assert!(quiet.is_err(), "no replay after PUBACK released inflight");
    kernel.finish().await;
}

/// A PUBCOMP with an unknown packet ID is ignored and keeps the connection open.
/// When the broker disconnects on unknown PUBCOMP,
/// `assert_eq!(head, 0xD0, "connection stays open after unknown PUBCOMP")` is the assertion that fails.
#[tokio::test]
async fn ta_003_pubcomp_not_found_error_keeps_connection() {
    let kernel = spawn_kernel("ta-003-comp-nf", None).await;
    let mut sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sock, "ta-003-comp-nf-cli", true, 60).await);

    send(&mut sock, &ack(0x70, 0x8888)).await;
    send(&mut sock, &[0xC0, 0x00]).await;
    let (head, _) = recv(&mut sock).await;
    assert_eq!(head, 0xD0, "connection stays open after unknown PUBCOMP");
    kernel.finish().await;
}

/// A PUBCOMP acknowledging a QoS 2 delivery releases the message from the session.
/// When the QoS 2 state is not completed,
/// `assert!(quiet.is_err(), "no replay after PUBCOMP completes QoS 2")` is the assertion that fails.
#[tokio::test]
async fn ta_003_pubcomp_ok_completes_handshake() {
    let kernel = spawn_kernel("ta-003-comp-ok", None).await;
    let topic = "ta03/comp/ok";
    let mut sub = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub, "ta-003-comp-ok-sub", false, 60).await);
    send(&mut sub, &subscribe(1, &[(topic, 2)])).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90);

    let mut pub_sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut pub_sock, "ta-003-comp-ok-pub", true, 60).await);
    send(&mut pub_sock, &publish311(topic, 7, 2, false, b"comp-ok")).await;
    let (head, _) = recv(&mut pub_sock).await;
    assert_eq!(head, 0x50);
    send(&mut pub_sock, &ack(0x62, 7)).await;
    let (head, _) = recv(&mut pub_sock).await;
    assert_eq!(head, 0x70);

    let (head, body) = recv(&mut sub).await;
    let live = parse_publish(head, &body);
    assert_eq!(live.qos, 2);
    send(&mut sub, &ack(0x50, live.packet_id)).await;
    let (head, body) = recv(&mut sub).await;
    assert_eq!(head, 0x62);
    send(&mut sub, &ack(0x70, u16::from_be_bytes([body[0], body[1]]))).await;
    drop(sub);

    let mut resume = dial(kernel.mqtt_port).await;
    send(&mut resume, &connect311("ta-003-comp-ok-sub", false, 60)).await;
    let (head, body) = recv(&mut resume).await;
    assert_eq!(head, 0x20);
    assert_eq!(parse_connack311(&body), (true, 0));

    let quiet = tokio::time::timeout(QUIET, read_mqtt_packet(&mut resume)).await;
    assert!(quiet.is_err(), "no replay after PUBCOMP completes QoS 2");
    kernel.finish().await;
}

/// An incoming MQTT 5 PUBLISH with a subscription identifier is rejected with DISCONNECT 0x82.
/// When the broker permits subscription identifiers on incoming publish packets,
/// `assert_eq!(head, 0xE0, "v5 publish with subscription identifier gets DISCONNECT")` is the assertion that fails.
#[tokio::test]
async fn ta_003_publish_with_subscription_identifier_rejected() {
    let kernel = spawn_kernel("ta-003-subid-rej", None).await;
    let mut sock = dial(kernel.mqtt_port).await;
    send(&mut sock, &connect5("ta-003-subid-cli", true, 60, 0)).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x20);
    let (_, reason, _) = parse_connack5(&body);
    assert_eq!(reason, 0);

    let bad = publish5_sub_id("ta03/subid/bad", 1, 1, 1, b"sub-id-bytes");
    send(&mut sock, &bad).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(
        head, 0xE0,
        "v5 publish with subscription identifier gets DISCONNECT"
    );
    assert_eq!(body, vec![0x82], "reason is protocol error 0x82");
    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        closed.is_err() || matches!(&closed, Ok(Err(_))),
        "socket closes after DISCONNECT"
    );
    kernel.finish().await;
}

/// An incoming PUBREC for an outbound QoS 2 message causes the broker to respond with PUBREL.
/// When the broker does not reply to PUBREC,
/// `assert_eq!(head, 0x62, "broker replies PUBREL to valid PUBREC")` is the assertion that fails.
#[tokio::test]
async fn ta_003_pubrec_ok_replies_pubrel() {
    let kernel = spawn_kernel("ta-003-rec-ok", None).await;
    let topic = "ta03/rec/ok";
    let mut sub = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub, "ta-003-rec-ok-sub", false, 60).await);
    send(&mut sub, &subscribe(1, &[(topic, 2)])).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90);

    let mut pub_sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut pub_sock, "ta-003-rec-ok-pub", true, 60).await);
    send(&mut pub_sock, &publish311(topic, 10, 2, false, b"rec-ok")).await;
    let (head, _) = recv(&mut pub_sock).await;
    assert_eq!(head, 0x50);
    send(&mut pub_sock, &ack(0x62, 10)).await;
    let (head, _) = recv(&mut pub_sock).await;
    assert_eq!(head, 0x70);

    let (head, body) = recv(&mut sub).await;
    let live = parse_publish(head, &body);
    send(&mut sub, &ack(0x50, live.packet_id)).await;
    let (head, body) = recv(&mut sub).await;
    assert_eq!(head, 0x62, "broker replies PUBREL to valid PUBREC");
    assert_eq!(&body[..2], &live.packet_id.to_be_bytes());
    kernel.finish().await;
}

/// An incoming PUBREL with an unknown packet ID responds with PUBCOMP.
/// When the broker does not reply to unknown PUBREL,
/// `assert_eq!(head, 0x70, "broker replies PUBCOMP to unknown PUBREL")` is the assertion that fails.
#[tokio::test]
async fn ta_003_pubrel_not_found_error_replies_pubcomp() {
    let kernel = spawn_kernel("ta-003-rel-nf", None).await;
    let mut sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sock, "ta-003-rel-nf-cli", true, 60).await);

    send(&mut sock, &ack(0x62, 1234)).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x70, "broker replies PUBCOMP to unknown PUBREL");
    assert_eq!(&body[..2], &[0x04, 0xD2]);
    kernel.finish().await;
}

/// An incoming PUBREL completes message release and replies with PUBCOMP.
/// When the broker does not complete message routing on PUBREL,
/// `assert_eq!(head, 0x70, "publisher receives PUBCOMP")` is the assertion that fails.
#[tokio::test]
async fn ta_003_pubrel_ok_routes_and_replies_pubcomp() {
    let kernel = spawn_kernel("ta-003-rel-ok", None).await;
    let topic = "ta03/rel/ok";
    let mut sub = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub, "ta-003-rel-ok-sub", false, 60).await);
    send(&mut sub, &subscribe(1, &[(topic, 2)])).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90);

    let mut pub_sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut pub_sock, "ta-003-rel-ok-pub", true, 60).await);
    send(&mut pub_sock, &publish311(topic, 2, 2, false, b"rel-ok")).await;
    let (head, body) = recv(&mut pub_sock).await;
    assert_eq!(head, 0x50);
    assert_eq!(&body[..2], &[0x00, 0x02]);

    send(&mut pub_sock, &ack(0x62, 2)).await;
    let (head, body) = recv(&mut pub_sock).await;
    assert_eq!(head, 0x70, "publisher receives PUBCOMP");
    assert_eq!(&body[..2], &[0x00, 0x02]);

    let (head, body) = recv(&mut sub).await;
    let got = parse_publish(head, &body);
    assert_eq!(got.payload, b"rel-ok");
    kernel.finish().await;
}

/// An incoming QoS 0 PUBLISH is accepted without sending an acknowledgment.
/// When the broker returns an unexpected packet on QoS 0,
/// `assert!(quiet.is_err(), "no acknowledgment packet sent for QoS 0")` is the assertion that fails.
#[tokio::test]
async fn ta_003_qos0_publish_accepted_without_ack() {
    let kernel = spawn_kernel("ta-003-q0-noack", None).await;
    let mut sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sock, "ta-003-q0-cli", true, 60).await);

    send(
        &mut sock,
        &publish311("ta03/q0/noack", 0, 0, false, b"q0-bytes"),
    )
    .await;
    let quiet = tokio::time::timeout(QUIET, read_mqtt_packet(&mut sock)).await;
    assert!(quiet.is_err(), "no acknowledgment packet sent for QoS 0");

    send(&mut sock, &[0xC0, 0x00]).await;
    let (head, _) = recv(&mut sock).await;
    assert_eq!(head, 0xD0, "connection stays open after QoS 0 publish");
    kernel.finish().await;
}

/// An incoming QoS 1 PUBLISH receives a PUBACK with the matching packet identifier.
/// When the broker does not send a PUBACK,
/// `assert_eq!(head, 0x40, "QoS 1 publish receives PUBACK")` is the assertion that fails.
#[tokio::test]
async fn ta_003_qos1_publish_receives_puback() {
    let kernel = spawn_kernel("ta-003-q1-ack", None).await;
    let mut sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sock, "ta-003-q1-cli", true, 60).await);

    send(
        &mut sock,
        &publish311("ta03/q1/ack", 42, 1, false, b"q1-bytes"),
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
