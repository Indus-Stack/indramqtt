//! Conformance tests for core MQTT frame parsing and packet validation (part 11).
//!
//! Proves that the kernel listener validates MQTT frame structures, property constraints,
//! and protocol bounds over real TCP sockets. Verifies duplicate property handling,
//! sticky frame reassembly, packet size boundaries, and malformed header rejections.

use super::support::*;
use std::time::Duration;

/// Standard test keepalive in seconds.
/// Sockets stay open during test execution.
const KEEP_ALIVE_SECS: u16 = 60;

/// Pause duration between fragmented packet chunks.
/// Allows the kernel listener to receive partial data before the next chunk arrives.
const CHUNK_DELAY: Duration = Duration::from_millis(50);

/// Builds an MQTT 5 AUTH packet.
fn auth5(reason: u8, method: &str, data: &[u8]) -> Vec<u8> {
    let mut props = Vec::new();
    props.push(21);
    props.extend_from_slice(&mqtt_str(method));
    props.push(22);
    props.extend_from_slice(&(data.len() as u16).to_be_bytes());
    props.extend_from_slice(data);

    let mut rest = vec![reason];
    rest.extend_from_slice(&varint(props.len() as u32));
    rest.extend_from_slice(&props);

    let mut packet = vec![0xF0];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// An MQTT 5 CONNECT packet with duplicate non-repeatable properties closes the socket.
/// When the broker accepts duplicate Session Expiry Interval properties,
/// `assert!(matches!(closed, Ok(Err(_))), "CONNECT with duplicate non-repeatable property closes socket")` is the assertion that fails.
#[tokio::test]
async fn ta_011_parse_duplicate_connect_property_strict() {
    let kernel = spawn_kernel("ta-011-dup-conn-prop", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    // Property ID 17 (Session Expiry Interval) is non-repeatable. Include it twice.
    let mut props = Vec::new();
    props.push(17);
    props.extend_from_slice(&(1u32).to_be_bytes());
    props.push(17);
    props.extend_from_slice(&(2u32).to_be_bytes());

    let packet = connect5_with_props("ta011-dup-prop-cli", true, KEEP_ALIVE_SECS, &props);
    send(&mut sock, &packet).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "CONNECT with duplicate non-repeatable property closes socket"
    );

    kernel.finish().await;
}

/// A packet with a malformed variable byte integer exceeding four continuation bytes closes the socket.
/// When the broker waits indefinitely or accepts a five-byte continuation variable integer,
/// `assert!(matches!(closed, Ok(Err(_))), "malformed variable byte integer closes socket")` is the assertion that fails.
#[tokio::test]
async fn ta_011_parse_frame_malformed_variable_byte_integer() {
    let kernel = spawn_kernel("ta-011-malformed-vbi", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    // Fixed header 0x30 followed by five bytes with continuation bit set.
    let packet = [0x30, 0x80, 0x80, 0x80, 0x80, 0x80];
    send(&mut sock, &packet).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "malformed variable byte integer closes socket"
    );

    kernel.finish().await;
}

/// An incoming packet whose length exceeds the configured maximum packet size receives DISCONNECT 0x95 and closes the socket.
/// When the broker fails to reject an oversize packet with DISCONNECT 0x95,
/// `assert_eq!(body[0], 0x95, "oversize packet receives DISCONNECT 0x95 Packet too large")` is the assertion that fails.
#[tokio::test]
async fn ta_011_parse_frame_too_large() {
    let kernel = boot_kernel(
        "ta-011-oversize",
        None,
        "\n[session]\nmax_packet_size = 256\n",
    )
    .await;
    let mut sock = dial(kernel.mqtt_port).await;

    send(
        &mut sock,
        &connect5("ta011-oversize-cli", true, KEEP_ALIVE_SECS, 0),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x20);
    assert_eq!(parse_connack5(&body).1, 0);

    // Send a 512-byte PUBLISH exceeding the 256-byte session limit.
    let large_pub = publish5("ta011/oversize", 0, 0, false, None, &vec![0x42; 480]);
    send(&mut sock, &large_pub).await;

    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0xE0, "oversize packet receives DISCONNECT");
    assert_eq!(
        body[0], 0x95,
        "oversize packet receives DISCONNECT 0x95 Packet too large"
    );

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        closed.is_err() || matches!(&closed, Ok(Err(_))),
        "socket closes after DISCONNECT"
    );

    kernel.finish().await;
}

/// A CONNECT packet with zero remaining length closes the socket.
/// When the broker waits for variable header bytes on zero remaining length,
/// `assert!(matches!(closed, Ok(Err(_))), "CONNECT packet with zero remaining length closes socket")` is the assertion that fails.
#[tokio::test]
async fn ta_011_parse_invalid_remaining_len() {
    let kernel = spawn_kernel("ta-011-zero-rl", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    let packet = [0x10, 0x00];
    send(&mut sock, &packet).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "CONNECT packet with zero remaining length closes socket"
    );

    kernel.finish().await;
}

/// A CONNECT packet carrying a property section with an unknown property identifier closes the socket.
/// When the broker accepts or fails to close on an unknown property identifier,
/// `assert!(matches!(closed, Ok(Err(_))), "CONNECT with unknown property identifier closes socket")` is the assertion that fails.
#[tokio::test]
async fn ta_011_parse_malformed_properties() {
    let kernel = spawn_kernel("ta-011-malformed-props", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    // Property identifier 0xFF is not defined by MQTT 5.0.
    let mut props = Vec::new();
    props.extend_from_slice(&[0xFF, 0x01, 0x00]);

    let packet = connect5_with_props("ta011-unk-prop-cli", true, KEEP_ALIVE_SECS, &props);
    send(&mut sock, &packet).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "CONNECT with unknown property identifier closes socket"
    );

    kernel.finish().await;
}

/// A CONNECT packet carrying a malformed UTF-8 protocol name closes the socket.
/// When the broker accepts an invalid UTF-8 protocol name,
/// `assert!(matches!(closed, Ok(Err(_))), "CONNECT with malformed protocol name closes socket")` is the assertion that fails.
#[tokio::test]
async fn ta_011_parse_malformed_utf8_string() {
    let kernel = spawn_kernel("ta-011-malformed-utf8", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    // Protocol name length 4 with malformed control bytes 0x00, 0x01, 0x1F, 0x7F instead of "MQTT".
    let mut rest = vec![0x00, 0x04, 0x00, 0x01, 0x1F, 0x7F, 0x04, 0x02, 0x00, 0x3C];
    rest.extend_from_slice(&mqtt_str("ta011-cli"));

    let mut packet = vec![0x10];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);

    send(&mut sock, &packet).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "CONNECT with malformed protocol name closes socket"
    );

    kernel.finish().await;
}

/// A non-CONNECT packet header received before CONNECT closes the connection immediately.
/// When the broker ignores or buffers a non-CONNECT packet header before CONNECT,
/// `assert!(matches!(closed, Ok(Err(_))), "non-CONNECT packet before CONNECT closes socket")` is the assertion that fails.
#[tokio::test]
async fn ta_011_parse_non_connect_before_connect() {
    let kernel = spawn_kernel("ta-011-pre-conn", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    // Non-CONNECT PUBLISH fixed header with maximum remaining length 268_435_455.
    let packet = [0x30, 0xFF, 0xFF, 0xFF, 0x7F];
    send(&mut sock, &packet).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "non-CONNECT packet before CONNECT closes socket"
    );

    kernel.finish().await;
}

/// An MQTT 5 CONNECT packet carrying multiple valid User-Property pairs interleaved with other properties is accepted.
/// When the broker rejects repeated user properties in CONNECT,
/// `assert_eq!(reason, 0, "CONNECT with repeated user properties is accepted")` is the assertion that fails.
#[tokio::test]
async fn ta_011_parse_repeated_user_property_strict() {
    let kernel = spawn_kernel("ta-011-repeated-up", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    let mut props = Vec::new();
    props.push(38);
    props.extend_from_slice(&mqtt_str("k"));
    props.extend_from_slice(&mqtt_str("1"));
    props.push(17);
    props.extend_from_slice(&(7u32).to_be_bytes());
    props.push(38);
    props.extend_from_slice(&mqtt_str("k"));
    props.extend_from_slice(&mqtt_str("2"));

    let packet = connect5_with_props("ta011-rep-up-cli", true, KEEP_ALIVE_SECS, &props);
    send(&mut sock, &packet).await;

    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x20);
    let (_, reason, _) = parse_connack5(&body);
    assert_eq!(
        reason, 0,
        "CONNECT with repeated user properties is accepted"
    );

    kernel.finish().await;
}

/// An MQIsdp 3.1 CONNECT packet with reserved connect flag bit 0 set closes the socket.
/// When the broker accepts an MQIsdp CONNECT packet with reserved flag bit set,
/// `assert!(matches!(closed, Ok(Err(_))), "MQIsdp CONNECT with reserved flag set closes socket")` is the assertion that fails.
#[tokio::test]
async fn ta_011_reserved_connect_flag() {
    let kernel = spawn_kernel("ta-011-res-flag-v3", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    // MQIsdp header with protocol level 3 and connect flag 0x01 (reserved bit 0 set).
    let mut rest = vec![0x00, 0x06, b'M', b'Q', b'I', b's', b'd', b'p', 0x03, 0x01];
    rest.extend_from_slice(&(60u16).to_be_bytes());
    rest.extend_from_slice(&mqtt_str("ta011-cli"));

    let mut packet = vec![0x10];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);

    send(&mut sock, &packet).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "MQIsdp CONNECT with reserved flag set closes socket"
    );

    kernel.finish().await;
}

/// Multiple MQTT packets coalesced across read buffer boundaries are correctly split and processed.
/// When the broker fails to process pipelined sticky packets,
/// `assert_eq!(head2, 0x40, "second packet in sticky frame is acknowledged")` is the assertion that fails.
#[tokio::test]
async fn ta_011_parse_sticky_frames() {
    let kernel = spawn_kernel("ta-011-sticky", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    send(
        &mut sock,
        &connect311("ta011-sticky-cli", true, KEEP_ALIVE_SECS),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x20);
    assert_eq!(parse_connack311(&body).1, 0);

    let pub1 = publish311("ta011/sticky", 1, 1, false, b"sticky-first");
    let pub2 = publish311("ta011/sticky", 2, 1, false, b"sticky-second");

    // Split first packet so trailing two bytes are sent with second packet.
    let cut = pub1.len() - 2;
    send(&mut sock, &pub1[..cut]).await;
    tokio::time::sleep(CHUNK_DELAY).await;

    let mut combined = pub1[cut..].to_vec();
    combined.extend_from_slice(&pub2);
    send(&mut sock, &combined).await;

    let (head1, body1) = recv(&mut sock).await;
    assert_eq!(head1, 0x40);
    assert_eq!(&body1[..2], &[0x00, 0x01]);

    let (head2, body2) = recv(&mut sock).await;
    assert_eq!(head2, 0x40, "second packet in sticky frame is acknowledged");
    assert_eq!(&body2[..2], &[0x00, 0x02]);

    kernel.finish().await;
}

/// A PUBLISH packet carrying a topic name with an invalid null character is rejected and closes the socket.
/// When the broker accepts a topic name containing a null character,
/// `assert!(matches!(closed, Ok(Err(_))), "PUBLISH with null character in topic closes socket")` is the assertion that fails.
#[tokio::test]
async fn ta_011_parse_utf8_topic_byte_positions() {
    let kernel = spawn_kernel("ta-011-topic-null", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    send(
        &mut sock,
        &connect311("ta011-null-cli", true, KEEP_ALIVE_SECS),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x20);
    assert_eq!(parse_connack311(&body).1, 0);

    // Topic with embedded null character (U+0000).
    let packet = publish311("ta011/topic\0bad", 1, 1, false, b"null-topic");
    send(&mut sock, &packet).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "PUBLISH with null character in topic closes socket"
    );

    kernel.finish().await;
}

/// An incoming PUBLISH packet whose encoded size matches the maximum packet size limit exactly is accepted.
/// When the broker rejects a packet whose size equals the maximum packet size,
/// `assert_eq!(head, 0x40, "packet exactly at maximum packet size limit is accepted")` is the assertion that fails.
#[tokio::test]
async fn ta_011_serialize_frame_at_max_size() {
    let kernel = boot_kernel(
        "ta-011-exact-size",
        None,
        "\n[session]\nmax_packet_size = 256\n",
    )
    .await;
    let mut sock = dial(kernel.mqtt_port).await;

    send(
        &mut sock,
        &connect5("ta011-exact-cli", true, KEEP_ALIVE_SECS, 0),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x20);
    assert_eq!(parse_connack5(&body).1, 0);

    // Construct a PUBLISH packet whose encoded wire size is exactly 256 bytes.
    // Fixed header 1 byte (0x32) + 2-byte varint (253) + 253 bytes body = 256 bytes.
    // Body: topic "ta011/exact" (13 bytes) + packet id (2 bytes) + props len (1 byte) + 237 bytes payload = 253.
    let topic = "ta011/exact";
    let payload = vec![0x58u8; 237];
    let pub_packet = publish5(topic, 1, 1, false, None, &payload);
    assert_eq!(
        pub_packet.len(),
        256,
        "PUBLISH wire size is exactly 256 bytes"
    );

    send(&mut sock, &pub_packet).await;

    let (head, body) = recv(&mut sock).await;
    assert_eq!(
        head, 0x40,
        "packet exactly at maximum packet size limit is accepted"
    );
    assert_eq!(&body[..2], &[0x00, 0x01]);

    kernel.finish().await;
}

/// An unexpected MQTT 5 AUTH packet sent on a connection without enhanced authentication receives DISCONNECT 0x82.
/// When the broker fails to reject an unexpected AUTH packet with DISCONNECT 0x82,
/// `assert_eq!(body[0], 0x82, "unexpected AUTH packet receives DISCONNECT 0x82 protocol error")` is the assertion that fails.
#[tokio::test]
async fn ta_011_serialize_parse_auth_v5() {
    let kernel = spawn_kernel("ta-011-auth-v5", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    send(
        &mut sock,
        &connect5("ta011-auth-cli", true, KEEP_ALIVE_SECS, 0),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x20);
    assert_eq!(parse_connack5(&body).1, 0);

    send(&mut sock, &auth5(0x19, "SCRAM-SHA-256", b"client-data")).await;

    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0xE0, "unexpected AUTH receives DISCONNECT");
    assert_eq!(
        body[0], 0x82,
        "unexpected AUTH packet receives DISCONNECT 0x82 protocol error"
    );

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        closed.is_err() || matches!(&closed, Ok(Err(_))),
        "socket closes after DISCONNECT"
    );

    kernel.finish().await;
}
