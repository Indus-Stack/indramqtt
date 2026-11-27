//! Conformance tests for core MQTT packet handling and channel operations (part 7).
//!
//! Proves that the kernel listener handles connect and disconnect sequences,
//! packet size bounds, user property limits, initial packet validation, connect timeouts
//! and publish storms over real TCP sockets.

use super::support::*;
use broker_protocol::wire::MAX_USER_PROPS;
use std::time::Duration;

/// Maximum CONNECT packet size in bytes.
/// Matches MAX_CONNECT_PACKET_BYTES in crates/broker-node/src/native.rs:53.
const MAX_CONNECT_PACKET_BYTES: usize = 64 * 1024;

/// Length of client ID for oversized packet test.
/// Variable header (10 bytes) + 2 bytes prefix + client ID exceeds MAX_CONNECT_PACKET_BYTES.
const OVERSIZED_CLIENT_ID_LEN: usize = MAX_CONNECT_PACKET_BYTES - 6;

/// Connect timeout wait bound in seconds.
/// Exceeds the 10-second CONNECT_TIMEOUT in crates/broker-node/src/native.rs:46.
const CONNECT_TIMEOUT_WAIT: Duration = Duration::from_secs(15);

/// Number of QoS 0 messages sent in publish storm test.
/// Generates a burst of QoS 0 traffic within the per-client backlog.
const STORM_QOS0_COUNT: usize = 50;

/// Frame receive timeout for draining messages.
/// Bounded wait per packet while reading storm deliveries.
const STORM_DRAIN_TIMEOUT: Duration = Duration::from_millis(500);

/// Standard test connection keep alive in seconds.
const KEEP_ALIVE_SECS: u16 = 60;

/// A client connecting after sending DISCONNECT and CONNECT on one TCP connection resumes the durable session when Clean Session is false.
/// When the broker does not resume the durable session on reconnect,
/// `assert!(present, "resumed session reports session present true")` is the assertion that fails.
#[tokio::test]
async fn ta_007_connect_after_disconnect_keeps_session() {
    let kernel = spawn_kernel("ta-007-disc-keeps", None).await;
    let client_id = "ta-007-disc-keeps-id";

    let mut sock1 = dial(kernel.mqtt_port).await;
    send(&mut sock1, &connect311(client_id, false, KEEP_ALIVE_SECS)).await;
    let (head, body) = recv(&mut sock1).await;
    assert_eq!(head, 0x20);
    let (present, code) = parse_connack311(&body);
    assert_eq!(code, 0);
    assert!(!present, "first connect has session present false");

    let mut payload = vec![0xE0, 0x00];
    payload.extend_from_slice(&connect311(client_id, false, KEEP_ALIVE_SECS));
    send(&mut sock1, &payload).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock1)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "socket closes after DISCONNECT"
    );

    let mut sock2 = dial(kernel.mqtt_port).await;
    send(&mut sock2, &connect311(client_id, false, KEEP_ALIVE_SECS)).await;
    let (head, body) = recv(&mut sock2).await;
    assert_eq!(head, 0x20);
    let (present, code) = parse_connack311(&body);
    assert_eq!(code, 0);
    assert!(present, "resumed session reports session present true");

    kernel.finish().await;
}

/// A client connecting after sending DISCONNECT and CONNECT on one TCP connection starts a fresh session when Clean Session is true.
/// When the broker erroneously keeps the durable session on a clean start reconnect,
/// `assert!(!present, "clean start after disconnect reports session present false")` is the assertion that fails.
#[tokio::test]
async fn ta_007_connect_after_disconnect_then_clean_start() {
    let kernel = spawn_kernel("ta-007-disc-clean", None).await;
    let client_id = "ta-007-disc-clean-id";

    let mut sock1 = dial(kernel.mqtt_port).await;
    send(&mut sock1, &connect311(client_id, false, KEEP_ALIVE_SECS)).await;
    let (head, body) = recv(&mut sock1).await;
    assert_eq!(head, 0x20);
    let (present, code) = parse_connack311(&body);
    assert_eq!(code, 0);
    assert!(!present);

    let mut payload = vec![0xE0, 0x00];
    payload.extend_from_slice(&connect311(client_id, false, KEEP_ALIVE_SECS));
    send(&mut sock1, &payload).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock1)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "socket closes after DISCONNECT"
    );

    let mut sock2 = dial(kernel.mqtt_port).await;
    send(&mut sock2, &connect311(client_id, true, KEEP_ALIVE_SECS)).await;
    let (head, body) = recv(&mut sock2).await;
    assert_eq!(head, 0x20);
    let (present, code) = parse_connack311(&body);
    assert_eq!(code, 0);
    assert!(
        !present,
        "clean start after disconnect reports session present false"
    );

    kernel.finish().await;
}

/// An incoming CONNECT packet exceeding the maximum connect packet size is refused and closes the socket.
/// When the broker accepts or fails to close on an oversized CONNECT packet,
/// `assert!(matches!(closed, Ok(Err(_))), "oversized CONNECT packet closes socket")` is the assertion that fails.
#[tokio::test]
async fn ta_007_connect_packet_too_large() {
    let kernel = spawn_kernel("ta-007-pkt-large", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    // Send a valid CONNECT packet whose total length exceeds MAX_CONNECT_PACKET_BYTES.
    let client_id = vec![b'a'; OVERSIZED_CLIENT_ID_LEN];
    let mut body = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3C];
    body.extend_from_slice(&(OVERSIZED_CLIENT_ID_LEN as u16).to_be_bytes());
    body.extend_from_slice(&client_id);

    assert!(body.len() > MAX_CONNECT_PACKET_BYTES);
    let mut oversized = vec![0x10];
    oversized.extend_from_slice(&rl(body.len()));
    oversized.extend_from_slice(&body);
    send(&mut sock, &oversized).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "oversized CONNECT packet closes socket"
    );

    kernel.finish().await;
}

/// An MQTT 5 CONNECT packet carrying more user properties than the supported limit is refused and closes the socket.
/// When the broker accepts a CONNECT packet exceeding the maximum user properties limit,
/// `assert!(matches!(closed, Ok(Err(_))), "CONNECT with too many user properties closes socket")` is the assertion that fails.
#[tokio::test]
async fn ta_007_connect_too_many_user_properties() {
    let kernel = spawn_kernel("ta-007-props-limit", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    // Build user properties exceeding MAX_USER_PROPS.
    let mut props = Vec::new();
    for i in 0..=((MAX_USER_PROPS) as u8) {
        props.push(38);
        props.extend_from_slice(&mqtt_str("k"));
        props.extend_from_slice(&mqtt_str(&format!("v{i}")));
    }
    let mut props_section = varint(props.len() as u32);
    props_section.extend_from_slice(&props);

    let mut var_header = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02];
    var_header.extend_from_slice(&KEEP_ALIVE_SECS.to_be_bytes());
    var_header.extend_from_slice(&props_section);

    let mut body = var_header;
    body.extend_from_slice(&mqtt_str("ta-007-props-client"));

    let mut packet = vec![0x10];
    packet.extend_from_slice(&rl(body.len()));
    packet.extend_from_slice(&body);

    send(&mut sock, &packet).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "CONNECT with too many user properties closes socket"
    );

    kernel.finish().await;
}

/// A DISCONNECT sent on a connection with Clean Session false preserves the session for a subsequent reconnect.
/// When the broker discards the durable session on DISCONNECT,
/// `assert!(present, "reconnecting after disconnect reports session present true")` is the assertion that fails.
#[tokio::test]
async fn ta_007_disconnect_keeps_session() {
    let kernel = spawn_kernel("ta-007-disc-session", None).await;
    let client_id = "ta-007-disc-session-id";

    let mut sock0 = dial(kernel.mqtt_port).await;
    send(&mut sock0, &connect311(client_id, false, KEEP_ALIVE_SECS)).await;
    let (head, body) = recv(&mut sock0).await;
    assert_eq!(head, 0x20);
    let (present, code) = parse_connack311(&body);
    assert_eq!(code, 0);
    assert!(!present, "first connect has session present false");

    send(&mut sock0, &[0xE0, 0x00]).await;
    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock0)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "socket closes after clean DISCONNECT"
    );

    let mut sock1 = dial(kernel.mqtt_port).await;
    send(&mut sock1, &connect311(client_id, false, KEEP_ALIVE_SECS)).await;
    let (head, body) = recv(&mut sock1).await;
    assert_eq!(head, 0x20);
    let (present, code) = parse_connack311(&body);
    assert_eq!(code, 0);
    assert!(
        present,
        "reconnecting after disconnect reports session present true"
    );

    kernel.finish().await;
}

/// An incoming non-CONNECT packet received as the first packet on a new connection is rejected and closes the socket.
/// When the broker fails to close the connection on a non-CONNECT initial packet,
/// `assert!(matches!(closed, Ok(Err(_))), "non-CONNECT first packet closes socket")` is the assertion that fails.
#[tokio::test]
async fn ta_007_first_packet_not_connect() {
    let kernel = spawn_kernel("ta-007-first-not-conn", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    send(&mut sock, &[0xC0, 0x00]).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "non-CONNECT first packet closes socket"
    );

    kernel.finish().await;
}

/// A subscriber connection survives a QoS 0 publish storm with bounded memory.
/// When the subscriber connection is dropped or terminated during a publish storm,
/// `assert_eq!(head, 0xD0, "subscriber connection survives publish storm and answers PINGREQ")` is the assertion that fails.
#[tokio::test]
async fn ta_007_congestion_qos0_publish_storm() {
    let kernel = spawn_kernel("ta-007-storm", None).await;

    let mut sub = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub, "ta-007-storm-sub", true, KEEP_ALIVE_SECS).await);
    send(
        &mut sub,
        &subscribe(1, &[("ta07/storm/qos1", 1), ("ta07/storm/qos0", 0)]),
    )
    .await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90, "SUBSCRIBE receives SUBACK");

    let mut pub_sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut pub_sock, "ta-007-storm-pub", true, KEEP_ALIVE_SECS).await);

    // Send a burst of QoS 0 publishes followed by a QoS 1 publish.
    for _ in 0..STORM_QOS0_COUNT {
        send(
            &mut pub_sock,
            &publish311("ta07/storm/qos0", 0, 0, false, b"storm-qos0"),
        )
        .await;
    }
    send(
        &mut pub_sock,
        &publish311("ta07/storm/qos1", 101, 1, false, b"storm-qos1"),
    )
    .await;
    let (puback_head, _) = recv(&mut pub_sock).await;
    assert_eq!(puback_head, 0x40, "PUBACK received for QoS 1 publish");

    let mut qos1_received = false;
    let deadline = tokio::time::Instant::now() + STEP;
    while tokio::time::Instant::now() < deadline {
        let Ok(Ok((head, body))) =
            tokio::time::timeout(STORM_DRAIN_TIMEOUT, read_mqtt_packet(&mut sub)).await
        else {
            break;
        };
        if head & 0xF0 == 0x30 {
            let qos = (head >> 1) & 0x03;
            if qos == 1 {
                qos1_received = true;
                if body.len() >= 4 {
                    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
                    if body.len() >= 2 + topic_len + 2 {
                        let pid_bytes = &body[2 + topic_len..2 + topic_len + 2];
                        let puback = vec![0x40, 0x02, pid_bytes[0], pid_bytes[1]];
                        send(&mut sub, &puback).await;
                    }
                }
            }
        }
    }
    assert!(
        qos1_received,
        "subscriber receives QoS 1 message despite QoS 0 burst"
    );

    // Verify subscriber connection survived and is responsive.
    send(&mut sub, &[0xC0, 0x00]).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(
        head, 0xD0,
        "subscriber connection survives publish storm and answers PINGREQ"
    );

    kernel.finish().await;
}

/// An incomplete CONNECT packet that is not completed within the connect timeout is dropped and the socket closes.
/// When the broker fails to close the incomplete connection after the timeout,
/// `assert!(matches!(closed, Ok(Err(_))), "incomplete CONNECT socket closes after timeout")` is the assertion that fails.
#[tokio::test]
async fn ta_007_connect_idle_timeout() {
    let kernel = spawn_kernel("ta-007-idle-timeout", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    // Send incomplete CONNECT packet: fixed header and 2 bytes of variable header.
    send(&mut sock, &[0x10, 0x0A, 0x00, 0x04]).await;

    let closed = tokio::time::timeout(CONNECT_TIMEOUT_WAIT, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "incomplete CONNECT socket closes after timeout"
    );

    kernel.finish().await;
}

/// A newly connected socket that sends no packets within the connect timeout is closed by the broker.
/// When the broker does not close a silent connection after the connect timeout,
/// `assert!(matches!(closed, Ok(Err(_))), "silent socket closes after connect timeout")` is the assertion that fails.
#[tokio::test]
async fn ta_007_connect_silent_idle_timeout() {
    let kernel = spawn_kernel("ta-007-silent-timeout", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    let closed = tokio::time::timeout(CONNECT_TIMEOUT_WAIT, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "silent socket closes after connect timeout"
    );

    kernel.finish().await;
}
