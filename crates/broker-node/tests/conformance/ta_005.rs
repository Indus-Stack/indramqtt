//! Conformance tests for core MQTT packet handling and sessions.
//!
//! Proves that the kernel listener handles ingress and egress MQTT
//! packets and session lifecycles over real TCP sockets.

use super::support::*;
use std::time::Duration;

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

/// An outbound PUBREL packet is formatted with the matching packet identifier in response to PUBREC.
/// When the broker does not reply to PUBREC with PUBREL,
/// `assert_eq!(head, 0x62, "broker sends outbound PUBREL")` is the assertion that fails.
#[tokio::test]
async fn ta_005_outbound_pubrel_matches_packet_id() {
    let kernel = spawn_kernel("ta-005-out-rel", None).await;
    let topic = "ta05/pubrel/test";
    let mut sub = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub, "ta-005-pubrel-sub", false, 60).await);
    send(&mut sub, &subscribe(1, &[(topic, 2)])).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90);

    let mut pub_sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut pub_sock, "ta-005-pubrel-pub", true, 60).await);
    send(
        &mut pub_sock,
        &publish311(topic, 10, 2, false, b"rel-payload"),
    )
    .await;
    let (head, _) = recv(&mut pub_sock).await;
    assert_eq!(head, 0x50);
    send(&mut pub_sock, &ack(0x62, 10)).await;
    let (head, _) = recv(&mut pub_sock).await;
    assert_eq!(head, 0x70);

    let (head, body) = recv(&mut sub).await;
    let live = parse_publish(head, &body);
    send(&mut sub, &ack(0x50, live.packet_id)).await;
    let (head, body) = recv(&mut sub).await;
    assert_eq!(head, 0x62, "broker sends outbound PUBREL");
    assert_eq!(
        &body[..2],
        &live.packet_id.to_be_bytes(),
        "PUBREL packet identifier matches PUBREC"
    );
    send(&mut sub, &ack(0x70, live.packet_id)).await;
    kernel.finish().await;
}

/// An outbound SUBACK packet is formatted with the matching packet identifier and granted QoS return codes.
/// When the broker does not return the granted QoS in SUBACK,
/// `assert_eq!(body[2], 0x02, "SUBACK returns granted QoS 2")` is the assertion that fails.
#[tokio::test]
async fn ta_005_outbound_suback_granted_qos() {
    let kernel = spawn_kernel("ta-005-out-suback", None).await;
    let mut sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sock, "ta-005-suback-cli", true, 60).await);

    send(&mut sock, &subscribe(42, &[("ta05/suback/qos2", 2)])).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x90, "SUBSCRIBE receives SUBACK");
    assert_eq!(
        &body[..2],
        &[0x00, 42],
        "SUBACK packet identifier matches SUBSCRIBE"
    );
    assert_eq!(body[2], 0x02, "SUBACK returns granted QoS 2");
    kernel.finish().await;
}

/// An outbound UNSUBACK packet acknowledges an unsubscription with the matching packet identifier.
/// When the broker does not return an UNSUBACK packet,
/// `assert_eq!(head, 0xB0, "UNSUBSCRIBE receives UNSUBACK")` is the assertion that fails.
#[tokio::test]
async fn ta_005_outbound_unsuback_matches_packet_id() {
    let kernel = spawn_kernel("ta-005-out-unsuback", None).await;
    let mut sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sock, "ta-005-unsuback-cli", true, 60).await);

    send(&mut sock, &subscribe(1, &[("ta05/unsuback/test", 1)])).await;
    let (head, _) = recv(&mut sock).await;
    assert_eq!(head, 0x90);

    send(&mut sock, &unsubscribe311(77, &["ta05/unsuback/test"])).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0xB0, "UNSUBSCRIBE receives UNSUBACK");
    assert_eq!(
        &body[..2],
        &[0x00, 77],
        "UNSUBACK packet identifier matches UNSUBSCRIBE"
    );
    kernel.finish().await;
}

/// An inactive session with a finite session expiry interval expires after the interval elapses.
/// When the session does not expire and resumes after the interval,
/// `assert!(!present, "reconnecting after session expiry interval finds no existing session")` is the assertion that fails.
#[tokio::test]
async fn ta_005_session_expiry_interval_expires_session() {
    let kernel = spawn_kernel("ta-005-exp-sess", None).await;
    let client_id = "ta-005-exp-sess-cli";
    // Property 17 is Session Expiry Interval in seconds (1 second: [17, 0, 0, 0, 1]).
    let props = [17, 0, 0, 0, 1];

    let mut sock = dial(kernel.mqtt_port).await;
    send(
        &mut sock,
        &connect5_with_props(client_id, false, 60, &props),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x20);
    let (present, reason, _) = parse_connack5(&body);
    assert_eq!(reason, 0);
    assert!(!present, "first connect has session present false");

    send(&mut sock, &subscribe_v5(1, &[("ta05/exp/test", 0)], 0)).await;
    let (head, _) = recv(&mut sock).await;
    assert_eq!(head, 0x90);

    // Clean disconnect closes the socket and records the end timestamp.
    send(&mut sock, &[0xE0, 0x00]).await;
    drop(sock);

    // Sleep longer than the 1-second session expiry interval.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Reconnecting after the interval elapses should start a new session.
    let mut sock2 = dial(kernel.mqtt_port).await;
    send(
        &mut sock2,
        &connect5_with_props(client_id, false, 60, &props),
    )
    .await;
    let (head, body) = recv(&mut sock2).await;
    assert_eq!(head & 0xF0, 0x20);
    let (present, reason, _) = parse_connack5(&body);
    assert_eq!(reason, 0);
    assert!(
        !present,
        "reconnecting after session expiry interval finds no existing session"
    );
    kernel.finish().await;
}

/// An outbound PUBLISH to a client with topic alias maximum 0 omits the Topic-Alias property.
/// When the broker assigns a topic alias to a client that negotiated maximum 0,
/// `assert_eq!(got.alias, 0, "outbound delivery has no topic alias when maximum is 0")` is the assertion that fails.
#[tokio::test]
async fn ta_005_packing_alias_disabled_omits_topic_alias() {
    let kernel = spawn_kernel("ta-005-alias-dis", None).await;
    let topic = "ta05/alias/disabled";
    let mut sub = dial(kernel.mqtt_port).await;
    // Connect with topic alias maximum 0.
    send(&mut sub, &connect5("ta-005-alias-dis-sub", true, 60, 0)).await;
    let (head, body) = recv(&mut sub).await;
    assert_eq!(head & 0xF0, 0x20);
    let (_, reason, _) = parse_connack5(&body);
    assert_eq!(reason, 0);

    send(&mut sub, &subscribe_v5(1, &[(topic, 0)], 0)).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90);

    let mut pub_sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut pub_sock, "ta-005-alias-dis-pub", true, 60).await);
    send(
        &mut pub_sock,
        &publish311(topic, 0, 0, false, b"no-alias-payload"),
    )
    .await;

    let (head, body) = recv(&mut sub).await;
    let got = parse_publish5(head, &body);
    assert_eq!(got.topic, topic);
    assert_eq!(
        got.alias, 0,
        "outbound delivery has no topic alias when maximum is 0"
    );
    assert_eq!(got.payload, b"no-alias-payload");
    kernel.finish().await;
}

/// An outbound PUBLISH to a client whose topic alias table is full delivers with full topic and no new alias.
/// When the broker attempts to assign an alias beyond the client negotiated maximum,
/// `assert_eq!(second.alias, 0, "second topic has no alias when table capacity is full")` is the assertion that fails.
#[tokio::test]
async fn ta_005_packing_alias_full_table_omits_alias() {
    let kernel = spawn_kernel("ta-005-alias-full", None).await;
    let mut sub = dial(kernel.mqtt_port).await;
    // Connect with topic alias maximum 1.
    send(&mut sub, &connect5("ta-005-alias-full-sub", true, 60, 1)).await;
    let (head, body) = recv(&mut sub).await;
    assert_eq!(head & 0xF0, 0x20);
    let (_, reason, _) = parse_connack5(&body);
    assert_eq!(reason, 0);

    send(&mut sub, &subscribe_v5(1, &[("ta05/full/#", 0)], 0)).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90);

    let mut pub_sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut pub_sock, "ta-005-alias-full-pub", true, 60).await);

    // First topic consumes the single available outbound alias slot (alias 1).
    send(
        &mut pub_sock,
        &publish311("ta05/full/first", 0, 0, false, b"first-payload"),
    )
    .await;
    let (head, body) = recv(&mut sub).await;
    let first = parse_publish5(head, &body);
    assert_eq!(first.topic, "ta05/full/first");
    assert_eq!(first.alias, 1, "first topic gets alias 1");

    // Second distinct topic cannot allocate an alias because the table capacity of 1 is full.
    send(
        &mut pub_sock,
        &publish311("ta05/full/second", 0, 0, false, b"second-payload"),
    )
    .await;
    let (head, body) = recv(&mut sub).await;
    let second = parse_publish5(head, &body);
    assert_eq!(second.topic, "ta05/full/second");
    assert_eq!(
        second.alias, 0,
        "second topic has no alias when table capacity is full"
    );
    assert_eq!(second.payload, b"second-payload");
    kernel.finish().await;
}
