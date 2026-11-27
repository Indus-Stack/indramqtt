//! Conformance tests for core MQTT frame parsing and connection handling (part 10).
//!
//! Proves that the kernel listener enforces MQTT packet structure and connection rules.
//! Verifies malformed CONNECT packet rejections, continuation parsing across chunks,
//! and packet handling after connection over real sockets.

use super::support::*;
use std::time::Duration;

/// Standard test keepalive in seconds.
/// Sockets stay open during test execution.
const KEEP_ALIVE_SECS: u16 = 60;

/// Connect packet limit in bytes.
/// Matches MAX_CONNECT_PACKET_BYTES in the native listener.
const MAX_CONNECT_PACKET_BYTES: usize = 64 * 1024;

/// Pause duration between fragmented packet chunks.
/// Allows kernel to receive partial data before next chunk arrives.
const CHUNK_DELAY: Duration = Duration::from_millis(50);

/// A CONNECT packet with client identifier length exceeding the remaining buffer closes the socket.
/// When the broker accepts or fails to close on a truncated client identifier,
/// `assert!(matches!(closed, Ok(Err(_))), "CONNECT with client ID length exceeding packet closes socket")` is the assertion that fails.
#[tokio::test]
async fn ta_010_invalid_clientid() {
    let kernel = spawn_kernel("ta-010-inv-cid", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    // Fixed header length 15. MQIsdp (8 bytes) + level 3 (1 byte) + flags (1 byte) + keepalive (2 bytes) = 12 bytes.
    // Client ID length claims 256 bytes (0x0100), but only 1 byte (0x00) follows.
    let packet = [
        0x10, 0x0F, 0x00, 0x06, b'M', b'Q', b'I', b's', b'd', b'p', 0x03, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00,
    ];
    send(&mut sock, &packet).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "CONNECT with client ID length exceeding packet closes socket"
    );

    kernel.finish().await;
}

/// An MQTT 3.1.1 CONNECT packet with Password Flag set but Username Flag unset closes the socket.
/// When the broker accepts a password without username in MQTT 3.1.1,
/// `assert!(matches!(closed, Ok(Err(_))), "CONNECT with password but no username in 3.1.1 closes socket")` is the assertion that fails.
#[tokio::test]
async fn ta_010_invalid_password_flag() {
    let kernel = spawn_kernel("ta-010-inv-pwd-flag", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    // Connect flags 0x42: Password Flag set, Username Flag unset, Clean Session set.
    let flags = 0x42u8;
    let mut rest = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, flags, 0x00, 0x3C];
    rest.extend_from_slice(&mqtt_str("ta010-cli"));
    rest.extend_from_slice(&(1u16).to_be_bytes());
    rest.push(b'p');

    let mut packet = vec![0x10];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);

    send(&mut sock, &packet).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "CONNECT with password but no username in 3.1.1 closes socket"
    );

    kernel.finish().await;
}

/// A CONNECT packet with invalid Will QoS flags closes the socket.
/// When the broker accepts Will QoS 1 without Will Flag,
/// `assert!(matches!(closed, Ok(Err(_))), "CONNECT with invalid Will QoS flags closes socket")` is the assertion that fails.
#[tokio::test]
async fn ta_010_invalid_will_qos() {
    let kernel = spawn_kernel("ta-010-inv-will-qos", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    // Connect flags 0x0A: Clean Session set, Will Flag unset, Will QoS = 1.
    let flags = 0x0Au8;
    let mut rest = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, flags, 0x00, 0x3C];
    rest.extend_from_slice(&mqtt_str("ta010-cli"));

    let mut packet = vec![0x10];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);

    send(&mut sock, &packet).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "CONNECT with invalid Will QoS flags closes socket"
    );

    kernel.finish().await;
}

/// A CONNECT packet with Will Retain set without Will Flag closes the socket.
/// When the broker accepts Will Retain without Will Flag,
/// `assert!(matches!(closed, Ok(Err(_))), "CONNECT with Will Retain set without Will Flag closes socket")` is the assertion that fails.
#[tokio::test]
async fn ta_010_invalid_will_retain() {
    let kernel = spawn_kernel("ta-010-inv-will-ret", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    // Connect flags 0x22: Clean Session set, Will Retain set, Will Flag unset.
    let flags = 0x22u8;
    let mut rest = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, flags, 0x00, 0x3C];
    rest.extend_from_slice(&mqtt_str("ta010-cli"));

    let mut packet = vec![0x10];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);

    send(&mut sock, &packet).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "CONNECT with Will Retain set without Will Flag closes socket"
    );

    kernel.finish().await;
}

/// A CONNECT packet with unexpected trailing bytes after client payload closes the socket.
/// When the broker ignores trailing bytes after client payload,
/// `assert!(matches!(closed, Ok(Err(_))), "CONNECT packet with unexpected trailing bytes closes socket")` is the assertion that fails.
#[tokio::test]
async fn ta_010_malformed_connect_data() {
    let kernel = spawn_kernel("ta-010-malformed-data", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    let mut rest = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3C];
    rest.extend_from_slice(&mqtt_str("ta010-cli"));
    // Extra trailing bytes after complete payload fields.
    rest.extend_from_slice(&[0x00, 0x01, 0x02]);

    let mut packet = vec![0x10];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);

    send(&mut sock, &packet).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "CONNECT packet with unexpected trailing bytes closes socket"
    );

    kernel.finish().await;
}

/// A CONNECT packet truncated before the protocol level byte closes the socket.
/// When the broker waits indefinitely or accepts a header truncated before protocol level,
/// `assert!(matches!(closed, Ok(Err(_))), "CONNECT packet truncated before protocol version closes socket")` is the assertion that fails.
#[tokio::test]
async fn ta_010_malformed_connect_data_proto_ver() {
    let kernel = spawn_kernel("ta-010-malformed-proto", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    // Announced remaining length is 6 bytes. Holds only protocol name length and name.
    let packet = [0x10, 0x06, 0x00, 0x04, b'M', b'Q', b'T', b'T'];
    send(&mut sock, &packet).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "CONNECT packet truncated before protocol version closes socket"
    );

    kernel.finish().await;
}

/// A CONNECT packet with remaining length shorter than the variable header closes the socket.
/// When the broker fails to reject a truncated variable header,
/// `assert!(matches!(closed, Ok(Err(_))), "CONNECT packet with remaining length smaller than variable header closes socket")` is the assertion that fails.
#[tokio::test]
async fn ta_010_malformed_connect_header() {
    let kernel = spawn_kernel("ta-010-malformed-hdr", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    // Remaining length 8: holds protocol name, level and flags, but cuts off keepalive.
    let packet = [0x10, 0x08, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02];
    send(&mut sock, &packet).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "CONNECT packet with remaining length smaller than variable header closes socket"
    );

    kernel.finish().await;
}

/// Once CONNECT is parsed, subsequent packets parse and process as usual.
/// When the broker fails to parse or respond to a PUBLISH after connection,
/// `assert_eq!(head, 0x40, "PUBLISH after CONNECT receives PUBACK")` is the assertion that fails.
#[tokio::test]
async fn ta_010_parse_after_connect() {
    let kernel = spawn_kernel("ta-010-parse-after", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    assert!(!connect_client(&mut sock, "ta010-after-cli", true, KEEP_ALIVE_SECS).await);

    send(
        &mut sock,
        &publish311("ta010/parse/after", 1, 1, false, b"after-conn"),
    )
    .await;

    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x40, "PUBLISH after CONNECT receives PUBACK");
    assert_eq!(body, vec![0x00, 0x01], "packet id 1 acknowledged");

    kernel.finish().await;
}

/// A complete non-CONNECT packet received before CONNECT closes the connection.
/// When the broker ignores or fails to close on a complete non-CONNECT packet before CONNECT,
/// `assert!(matches!(closed, Ok(Err(_))), "complete non-CONNECT packet before CONNECT closes socket")` is the assertion that fails.
#[tokio::test]
async fn ta_010_parse_complete_non_connect_before_connect() {
    let kernel = spawn_kernel("ta-010-complete-pre-conn", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    // Send complete PINGREQ packet before CONNECT.
    send(&mut sock, &[0xC0, 0x00]).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "complete non-CONNECT packet before CONNECT closes socket"
    );

    kernel.finish().await;
}

/// A CONNECT packet announcing a remaining length exceeding the connect packet limit closes the socket immediately.
/// When the broker waits for body bytes on an announced length exceeding the connect limit,
/// `assert!(matches!(closed, Ok(Err(_))), "CONNECT packet announcing length exceeding connect limit closes socket immediately")` is the assertion that fails.
#[tokio::test]
async fn ta_010_parse_connect_packet_too_large() {
    let kernel = spawn_kernel("ta-010-conn-too-large", None).await;
    let mut first = dial(kernel.mqtt_port).await;

    // Announced remaining length 70_000 exceeds MAX_CONNECT_PACKET_BYTES (65_536).
    // Send only the fixed header without transmitting any body bytes.
    let announced_len = MAX_CONNECT_PACKET_BYTES + 4096;
    let mut header = vec![0x10];
    header.extend_from_slice(&rl(announced_len));

    send(&mut first, &header).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut first)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "CONNECT packet announcing length exceeding connect limit closes socket immediately"
    );

    // Subsequent valid connection within the limit succeeds.
    let mut second = dial(kernel.mqtt_port).await;
    assert!(
        !connect_client(&mut second, "ta010-valid-subseq", true, KEEP_ALIVE_SECS).await,
        "subsequent connection within limit connects successfully"
    );

    kernel.finish().await;
}

/// A CONNECT packet with the reserved connect flag set closes the connection.
/// When the broker accepts or fails to close on a set reserved connect flag,
/// `assert!(matches!(closed, Ok(Err(_))), "CONNECT with reserved flag set closes socket")` is the assertion that fails.
#[tokio::test]
async fn ta_010_parse_error() {
    let kernel = spawn_kernel("ta-010-parse-err", None).await;
    let mut first = dial(kernel.mqtt_port).await;

    // MQTT 5 CONNECT with reserved flag bit 0 set: flags byte is 0x01.
    let packet = [
        0x10, 0x0A, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x01, 0x00, 0x3C,
    ];
    send(&mut first, &packet).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut first)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "CONNECT with reserved flag set closes socket"
    );

    // Subsequent valid connection succeeds.
    let mut second = dial(kernel.mqtt_port).await;
    assert!(
        !connect_client(&mut second, "ta010-valid-after-err", true, KEEP_ALIVE_SECS).await,
        "subsequent connection connects successfully"
    );

    kernel.finish().await;
}

/// An MQTT 5 PUBLISH packet with truncated property length mid-property receives DISCONNECT 0x82.
/// When the broker fails to reject a PUBLISH with property length cutting off a user property,
/// `assert_eq!(head, 0xE0, "malformed v5 PUBLISH receives DISCONNECT")` is the assertion that fails.
#[tokio::test]
async fn ta_010_parse_bad_v5_publish_packet() {
    let kernel = spawn_kernel("ta-010-bad-pub-v5", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    send(
        &mut sock,
        &connect5("ta010-bad-pub-cli", true, KEEP_ALIVE_SECS, 0),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x20);
    assert_eq!(parse_connack5(&body).1, 0);

    // Topic "test/1" (8 bytes).
    // Property length 9 bytes: user property ID (1 byte) + key len 6 (2 bytes) + "sample" (6 bytes).
    // The announced 9 bytes end before the value length or value bytes.
    let mut rest = mqtt_str("test/1");
    rest.push(0x09);
    rest.push(38);
    rest.extend_from_slice(&(6u16).to_be_bytes());
    rest.extend_from_slice(b"sample");
    rest.extend_from_slice(b"extra payload data after property");

    let mut packet = vec![0x30];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);

    send(&mut sock, &packet).await;

    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0xE0, "malformed v5 PUBLISH receives DISCONNECT");
    assert_eq!(body[0], 0x82, "reason code is 0x82 protocol error");

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        closed.is_err() || matches!(&closed, Ok(Err(_))),
        "socket closes after DISCONNECT"
    );

    kernel.finish().await;
}

/// A CONNECT packet received in separate chunks is assembled and accepted.
/// When the broker fails to reassemble a fragmented CONNECT packet across socket reads,
/// `assert_eq!(code, 0, "fragmented CONNECT packet is assembled and accepted")` is the assertion that fails.
#[tokio::test]
async fn ta_010_parse_cont() {
    let kernel = spawn_kernel("ta-010-parse-cont", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    let packet = connect311("ta010-cont-cli", true, KEEP_ALIVE_SECS);
    assert!(packet.len() > 2);

    // Split packet into fixed header, remaining length, and body chunks.
    let chunk1 = &packet[..1];
    let chunk2 = &packet[1..2];
    let chunk3 = &packet[2..];

    send(&mut sock, chunk1).await;
    tokio::time::sleep(CHUNK_DELAY).await;

    send(&mut sock, chunk2).await;
    tokio::time::sleep(CHUNK_DELAY).await;

    send(&mut sock, chunk3).await;

    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x20);
    let (present, code) = parse_connack311(&body);
    assert!(!present);
    assert_eq!(
        code, 0,
        "fragmented CONNECT packet is assembled and accepted"
    );

    kernel.finish().await;
}
