//! Conformance tests for core MQTT packet handling and channel operations (part 9).
//!
//! Proves that the kernel listener rejects non-UTF-8 topic filters, refuses oversized
//! initial CONNECT packets, rejects non-CONNECT packets announcing large bodies before
//! buffering, closes WebSocket connections on premature PINGREQ, and enforces client
//! Maximum-Packet-Size on delivery over real sockets.

use super::support::*;

/// Standard test keepalive in seconds.
/// Sockets stay open during test execution.
const KEEP_ALIVE_SECS: u16 = 60;

/// Connect packet limit in bytes.
/// Matches MAX_CONNECT_PACKET_BYTES in the native listener.
const MAX_CONNECT_PACKET_BYTES: usize = 64 * 1024;

/// Payload size for oversized credentials in bytes.
/// Two fields of this size exceed the connect limit.
const CREDENTIAL_CHUNK_BYTES: usize = 35_000;

/// Large publish payload size in bytes.
/// Exceeds the client maximum packet size limit.
const LARGE_PAYLOAD_BYTES: usize = 2048;

/// Small publish payload in bytes.
/// Fits within the client maximum packet size limit.
const SMALL_PAYLOAD: &[u8] = b"small";

/// Packet identifier for subscription operations.
const PACKET_ID_SUB: u16 = 1;

/// Connects a WebSocket client with the mqtt subprotocol.
/// Returns the WebSocket stream.
async fn ws_dial(
    ws_port: u16,
) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    use tokio_tungstenite::tungstenite::client::ClientRequestBuilder;
    let uri: http::Uri = format!("ws://127.0.0.1:{ws_port}/mqtt")
        .parse()
        .expect("uri parses");
    let builder = ClientRequestBuilder::new(uri).with_sub_protocol("mqtt");
    let (stream, response) = tokio_tungstenite::connect_async(builder)
        .await
        .expect("websocket connects");
    let selected = response
        .headers()
        .get("Sec-WebSocket-Protocol")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    assert_eq!(selected, "mqtt", "server selects mqtt subprotocol");
    stream
}

/// A SUBSCRIBE packet with a non-UTF-8 topic filter closes the connection.
/// When the broker does not close the socket on an invalid topic filter,
/// `assert!(matches!(closed, Ok(Err(_))), "SUBSCRIBE with non-UTF-8 topic filter closes socket")` is the assertion that fails.
#[tokio::test]
async fn ta_009_sub_non_utf8_topic() {
    let kernel = spawn_kernel("ta-009-sub-utf8", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    assert!(!connect_client(&mut sock, "ta009-sub-utf8-client", true, KEEP_ALIVE_SECS).await);

    // Non-UTF-8 topic filter: 0x80 is an invalid UTF-8 leading byte.
    let non_utf8_topic = [
        0x80u8, 0x0A, 0x0A, 0x0C, 0xB2, 0x9F, 0xA2, 0x2F, 0x73, 0x01, 0x01, 0x01, 0x01,
    ];
    let topic_len = (non_utf8_topic.len() as u16).to_be_bytes();
    let qos = 1u8;

    let mut body = PACKET_ID_SUB.to_be_bytes().to_vec();
    body.extend_from_slice(&topic_len);
    body.extend_from_slice(&non_utf8_topic);
    body.push(qos);

    let mut packet = vec![0x82];
    packet.extend_from_slice(&rl(body.len()));
    packet.extend_from_slice(&body);

    send(&mut sock, &packet).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "SUBSCRIBE with non-UTF-8 topic filter closes socket"
    );

    kernel.finish().await;
}

/// An oversized CONNECT packet is refused and subsequent valid connections succeed.
/// When the broker accepts or fails to close on an oversized CONNECT packet,
/// `assert!(matches!(closed, Ok(Err(_))), "oversized CONNECT packet closes socket")` is the assertion that fails.
#[tokio::test]
async fn ta_009_transport_refuses_oversized_connect() {
    let kernel = spawn_kernel("ta-009-oversized-conn", None).await;
    let mut first = dial(kernel.mqtt_port).await;

    // Send an initial CONNECT packet exceeding MAX_CONNECT_PACKET_BYTES.
    let cred = vec![b'x'; CREDENTIAL_CHUNK_BYTES];
    let client_id = "ta009-oversized-cred-id";
    let mut body = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0xC2, 0x00, 0x3C];
    body.extend_from_slice(&mqtt_str(client_id));
    body.extend_from_slice(&(CREDENTIAL_CHUNK_BYTES as u16).to_be_bytes());
    body.extend_from_slice(&cred);
    body.extend_from_slice(&(CREDENTIAL_CHUNK_BYTES as u16).to_be_bytes());
    body.extend_from_slice(&cred);

    assert!(body.len() > MAX_CONNECT_PACKET_BYTES);

    let mut packet = vec![0x10];
    packet.extend_from_slice(&rl(body.len()));
    packet.extend_from_slice(&body);

    send(&mut first, &packet).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut first)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "oversized CONNECT packet closes socket"
    );

    // Subsequent client connecting within the limit succeeds.
    let mut second = dial(kernel.mqtt_port).await;
    assert!(
        !connect_client(&mut second, "ta009-subsequent-valid", true, KEEP_ALIVE_SECS).await,
        "subsequent connection within limit connects successfully"
    );

    kernel.finish().await;
}

/// A non-CONNECT first packet is rejected from its fixed header before the body is buffered.
/// When the broker buffers or waits for the body of a non-CONNECT packet,
/// `assert!(matches!(closed, Ok(Err(_))), "non-CONNECT packet with large announced length closes socket immediately")` is the assertion that fails.
#[tokio::test]
async fn ta_009_parse_incoming_before_connect() {
    let kernel = spawn_kernel("ta-009-pre-conn-hdr", None).await;
    let mut sock = dial(kernel.mqtt_port).await;

    // PUBLISH fixed header announcing 268,435,455 bytes body before CONNECT.
    // Only the fixed header is sent; no body is transmitted.
    let header = [0x30, 0xFF, 0xFF, 0xFF, 0x7F];
    send(&mut sock, &header).await;

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "non-CONNECT packet with large announced length closes socket immediately"
    );

    kernel.finish().await;
}

/// A PINGREQ received on a WebSocket connection before CONNECT closes the connection.
/// When the broker accepts or fails to close on a WebSocket PINGREQ before CONNECT,
/// `assert!(matches!(closed, Ok(None) | Ok(Some(Err(_))) | Ok(Some(Ok(tokio_tungstenite::tungstenite::protocol::Message::Close(_))))), "PINGREQ before CONNECT closes WebSocket connection")` is the assertion that fails.
#[tokio::test]
async fn ta_009_ws_pingreq_before_connected() {
    let kernel = spawn_kernel("ta-009-ws-pingreq", None).await;
    let mut ws = ws_dial(kernel.ws_port).await;

    use futures::SinkExt as _;
    use futures::StreamExt as _;

    // Send PINGREQ frame over WebSocket before sending CONNECT.
    ws.send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
        vec![0xC0, 0x00],
    ))
    .await
    .expect("send PINGREQ frame");

    let closed = tokio::time::timeout(STEP, ws.next()).await;
    assert!(
        matches!(
            closed,
            Ok(None)
                | Ok(Some(Err(_)))
                | Ok(Some(Ok(
                    tokio_tungstenite::tungstenite::protocol::Message::Close(_)
                )))
        ),
        "PINGREQ before CONNECT closes WebSocket connection"
    );

    kernel.finish().await;
}

/// A client specifying Maximum-Packet-Size does not receive publishes exceeding that size.
/// When the broker delivers publishes exceeding the client Maximum-Packet-Size,
/// `assert!(quiet.is_err(), "client with Maximum-Packet-Size 1024 does not receive 2048-byte publish")` is the assertion that fails.
#[tokio::test]
async fn ta_009_client_max_packet_size() {
    let kernel = spawn_kernel("ta-009-max-pkt-size", None).await;
    let topic = "ta009/test/max_packet_size";

    // Client with 1024 bytes Maximum-Packet-Size (property 39).
    let max_pkt_props = vec![39, 0, 0, 4, 0];
    let mut small = dial(kernel.mqtt_port).await;
    send(
        &mut small,
        &connect5_with_props("ta009-maxpkt-small", true, KEEP_ALIVE_SECS, &max_pkt_props),
    )
    .await;
    let (head, body) = recv(&mut small).await;
    assert_eq!(head, 0x20);
    assert_eq!(parse_connack5(&body).1, 0, "small client connects");

    send(&mut small, &subscribe_v5(PACKET_ID_SUB, &[(topic, 0)], 0)).await;
    let (head, _) = recv(&mut small).await;
    assert_eq!(head, 0x90);

    // Standard client with default packet size limit.
    let mut standard = dial(kernel.mqtt_port).await;
    send(
        &mut standard,
        &connect5("ta009-maxpkt-std", true, KEEP_ALIVE_SECS, 0),
    )
    .await;
    let (head, body) = recv(&mut standard).await;
    assert_eq!(head, 0x20);
    assert_eq!(parse_connack5(&body).1, 0, "standard client connects");

    send(
        &mut standard,
        &subscribe_v5(PACKET_ID_SUB, &[(topic, 0)], 0),
    )
    .await;
    let (head, _) = recv(&mut standard).await;
    assert_eq!(head, 0x90);

    // Publisher client sending publishes.
    let mut publisher = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut publisher, "ta009-maxpkt-pub", true, KEEP_ALIVE_SECS).await);

    // Publish large 2048-byte message. Exceeds small client limit.
    let large_payload = vec![b'x'; LARGE_PAYLOAD_BYTES];
    send(
        &mut publisher,
        &publish311(topic, 0, 0, false, &large_payload),
    )
    .await;

    // Standard client receives the large message.
    let (head, body) = recv(&mut standard).await;
    let pubmsg = parse_publish5(head, &body);
    assert_eq!(pubmsg.topic, topic);
    assert_eq!(pubmsg.payload, large_payload);

    // Small client does not receive the large message.
    let quiet = tokio::time::timeout(QUIET, read_mqtt_packet(&mut small)).await;
    assert!(
        quiet.is_err(),
        "client with Maximum-Packet-Size 1024 does not receive 2048-byte publish"
    );

    // Publish small 5-byte message. Both clients receive it.
    send(
        &mut publisher,
        &publish311(topic, 0, 0, false, SMALL_PAYLOAD),
    )
    .await;

    let (head, body) = recv(&mut small).await;
    let pubmsg = parse_publish5(head, &body);
    assert_eq!(pubmsg.topic, topic);
    assert_eq!(pubmsg.payload, SMALL_PAYLOAD);

    let (head, body) = recv(&mut standard).await;
    let pubmsg = parse_publish5(head, &body);
    assert_eq!(pubmsg.topic, topic);
    assert_eq!(pubmsg.payload, SMALL_PAYLOAD);

    kernel.finish().await;
}
