//! Hardening tests for the `/ws/mqtt` management WebSocket bridge (BH-02).

use std::sync::Arc;

use broker_api::ApiState;
use broker_protocol::{QoS, TopicFilter};
use broker_router::{Subscription, MAX_SUBSCRIPTIONS};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Raw WebSocket test client over TCP.
#[derive(Debug)]
struct TestWsClient {
    stream: tokio::net::TcpStream,
    buf: Vec<u8>,
}

impl TestWsClient {
    async fn connect_with_headers(port: u16, extra_headers: &str) -> Result<Self, String> {
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .map_err(|e| e.to_string())?;
        let req = format!(
            "GET /ws/mqtt HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\n\
             Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
             Sec-WebSocket-Version: 13\r\n{extra_headers}\r\n"
        );
        stream
            .write_all(req.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        let mut buf = Vec::new();
        loop {
            let mut chunk = [0u8; 1024];
            let n = stream.read(&mut chunk).await.map_err(|e| e.to_string())?;
            if n == 0 {
                return Err("connection closed during upgrade".to_string());
            }
            buf.extend_from_slice(&chunk[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&buf[..pos + 4]);
                if !head.starts_with("HTTP/1.1 101") {
                    return Err(head.to_string());
                }
                let rest = buf[pos + 4..].to_vec();
                return Ok(Self { stream, buf: rest });
            }
        }
    }

    async fn connect(port: u16) -> Self {
        Self::connect_with_headers(port, "")
            .await
            .expect("standard ws upgrade must succeed")
    }

    async fn send_bin(&mut self, payload: &[u8]) {
        let mut frame = vec![0x82u8];
        if payload.len() < 126 {
            frame.push(payload.len() as u8 | 0x80);
        } else if payload.len() < 65536 {
            frame.push(126 | 0x80);
            frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        } else {
            frame.push(127 | 0x80);
            frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        }
        let mask = [0x22u8, 0x44, 0x66, 0x88];
        frame.extend_from_slice(&mask);
        for (i, byte) in payload.iter().enumerate() {
            frame.push(byte ^ mask[i % 4]);
        }
        self.stream
            .write_all(&frame)
            .await
            .expect("write binary frame");
    }

    async fn recv_msg(&mut self) -> Option<Vec<u8>> {
        loop {
            if let Some((payload, consumed)) = parse_ws_frame(&self.buf) {
                self.buf.drain(..consumed);
                return Some(payload);
            }
            let mut chunk = [0u8; 4096];
            match self
                .stream
                .read(&mut chunk)
                .await
                .expect("read frame chunk")
            {
                0 => return None,
                n => self.buf.extend_from_slice(&chunk[..n]),
            }
        }
    }
}

fn parse_ws_frame(buf: &[u8]) -> Option<(Vec<u8>, usize)> {
    if buf.len() < 2 || buf[1] & 0x80 != 0 {
        return None;
    }
    let opcode = buf[0] & 0x0F;
    let mut header = 2usize;
    let mut length = (buf[1] & 0x7F) as usize;
    if length == 126 {
        if buf.len() < 4 {
            return None;
        }
        length = u16::from_be_bytes([buf[2], buf[3]]) as usize;
        header = 4;
    } else if length == 127 {
        if buf.len() < 10 {
            return None;
        }
        length = u64::from_be_bytes(buf[2..10].try_into().ok()?) as usize;
        header = 10;
    }
    if buf.len() < header + length {
        return None;
    }
    if opcode == 0x8 {
        return Some((Vec::new(), header + length));
    }
    if opcode != 0x1 && opcode != 0x2 {
        return None;
    }
    Some((buf[header..header + length].to_vec(), header + length))
}

fn encode_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn encode_remaining(mut n: usize, out: &mut Vec<u8>) {
    loop {
        let mut byte = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if n == 0 {
            break;
        }
    }
}

fn build_connect(client_id: &str, username: Option<&str>, password: Option<&[u8]>) -> Vec<u8> {
    let mut body = vec![0, 4, b'M', b'Q', b'T', b'T', 4];
    let mut flags = 0x02u8;
    if username.is_some() {
        flags |= 0x80;
    }
    if password.is_some() {
        flags |= 0x40;
    }
    body.push(flags);
    body.extend_from_slice(&60u16.to_be_bytes());
    encode_str(&mut body, client_id);
    if let Some(user) = username {
        encode_str(&mut body, user);
    }
    if let Some(pass) = password {
        body.extend_from_slice(&(pass.len() as u16).to_be_bytes());
        body.extend_from_slice(pass);
    }
    let mut packet = vec![0x10];
    encode_remaining(body.len(), &mut packet);
    packet.extend_from_slice(&body);
    packet
}

fn build_subscribe(packet_id: u16, filters: &[(&str, u8)]) -> Vec<u8> {
    let mut body = packet_id.to_be_bytes().to_vec();
    for (filter, qos) in filters {
        encode_str(&mut body, filter);
        body.push(*qos);
    }
    let mut packet = vec![0x82];
    encode_remaining(body.len(), &mut packet);
    packet.extend_from_slice(&body);
    packet
}

fn build_publish(topic: &str, packet_id: u16, qos: u8, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    encode_str(&mut body, topic);
    if qos > 0 {
        body.extend_from_slice(&packet_id.to_be_bytes());
    }
    body.extend_from_slice(payload);
    let mut packet = vec![0x30 | (qos << 1)];
    encode_remaining(body.len(), &mut packet);
    packet.extend_from_slice(&body);
    packet
}

async fn start_test_server() -> (u16, ApiState) {
    let engine = Arc::new(broker_rules::RuleEngine::new(
        16,
        broker_rules::BackpressurePolicy::DropOldest,
    ));
    let state = ApiState::standalone(engine);
    let app = broker_api::router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let port = listener.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve test server");
    });
    (port, state)
}

#[tokio::test]
async fn test_ws_unauthenticated_client_exceeding_pre_auth_limit_is_disconnected() {
    let (port, _state) = start_test_server().await;
    let mut client = TestWsClient::connect(port).await;

    // Send 65,537 bytes of valid partial CONNECT packet data (exceeds PRE_AUTH_MAX_BYTES 65,536).
    // Remaining length 100,000 = [0xA0, 0x8D, 0x06]. Fixed header = 0x10.
    let mut oversized = vec![0x10];
    encode_remaining(100_000, &mut oversized);
    oversized.resize(65_537, 0);
    client.send_bin(&oversized).await;

    // Server must disconnect unauthenticated client.
    let response = client.recv_msg().await;
    assert!(
        response.is_none(),
        "unauthenticated client exceeding 64 KiB buffer must be disconnected"
    );
}

#[tokio::test]
async fn test_ws_post_auth_packet_limit_tracks_session_max_packet_size() {
    let (port, state) = start_test_server().await;
    // Set session max_packet_size to 512 bytes.
    state.sessions.set_max_packet_size(512);

    let mut client = TestWsClient::connect(port).await;
    client
        .send_bin(&build_connect("ws-post-auth-limit", None, None))
        .await;
    let connack = client.recv_msg().await.expect("connack packet");
    assert_eq!(connack[0] >> 4, 2, "packet type must be CONNACK");
    assert_eq!(connack[3], 0, "CONNACK return code must be 0");

    // Send PUBLISH exceeding the 512 byte limit.
    let big_payload = vec![b'x'; 600];
    let big_publish = build_publish("test/topic", 0, 0, &big_payload);
    client.send_bin(&big_publish).await;

    // Server closes connection exceeding post-auth max packet limit.
    let next = client.recv_msg().await;
    assert!(
        next.is_none(),
        "post-auth client sending packet exceeding session max packet size must be disconnected"
    );
}

#[tokio::test]
async fn test_ws_origin_validation_cross_origin_mismatch_returns_403() {
    let (port, _state) = start_test_server().await;
    let err = TestWsClient::connect_with_headers(port, "Origin: http://evil.com\r\n")
        .await
        .expect_err("mismatched cross-origin upgrade must fail");
    assert!(
        err.starts_with("HTTP/1.1 403 Forbidden"),
        "expected 403 Forbidden, got: {err}"
    );
}

#[tokio::test]
async fn test_ws_origin_validation_allowed_origins_and_absence_control() {
    let (port, _state) = start_test_server().await;

    // 1. Matching Host header succeeds.
    let matching_origin = format!("Origin: http://127.0.0.1:{port}\r\n");
    let mut client = TestWsClient::connect_with_headers(port, &matching_origin)
        .await
        .expect("matching Host header must succeed");
    client
        .send_bin(&build_connect("ws-origin-match", None, None))
        .await;
    let connack = client.recv_msg().await.expect("connack packet");
    assert_eq!(connack[3], 0);

    // 2. Absent Origin header succeeds by default.
    let mut client_no_origin = TestWsClient::connect_with_headers(port, "")
        .await
        .expect("absent Origin header must succeed by default");
    client_no_origin
        .send_bin(&build_connect("ws-origin-absent", None, None))
        .await;
    let connack2 = client_no_origin.recv_msg().await.expect("connack packet");
    assert_eq!(connack2[3], 0);

    // 3. Untrusted cross-origin host receives 403 Forbidden.
    let untrusted_err =
        TestWsClient::connect_with_headers(port, "Origin: http://untrusted.internal:8080\r\n")
            .await
            .expect_err("untrusted origin must receive 403");
    assert!(
        untrusted_err.starts_with("HTTP/1.1 403 Forbidden"),
        "expected 403 Forbidden, got: {untrusted_err}"
    );

    // 4. Mismatched origin port receives 403 Forbidden.
    let mismatched_port = format!("Origin: http://127.0.0.1:{}\r\n", port + 1);
    let port_err = TestWsClient::connect_with_headers(port, &mismatched_port)
        .await
        .expect_err("mismatched origin port must receive 403");
    assert!(
        port_err.starts_with("HTTP/1.1 403 Forbidden"),
        "expected 403 Forbidden when origin port differs, got: {port_err}"
    );
}

#[tokio::test]
async fn test_ws_fragmented_remaining_length_header_assembles_cleanly() {
    let (port, _state) = start_test_server().await;
    let mut client = TestWsClient::connect(port).await;

    // Build a CONNECT packet with 2-byte Remaining Length.
    // Length 200 = [0xC8, 0x01].
    // Fixed header = 0x10.
    let mut connect_body = vec![0, 4, b'M', b'Q', b'T', b'T', 4, 0x02, 0, 60];
    encode_str(&mut connect_body, "frag-client");
    // Pad client body to exactly 200 bytes
    let pad_len = 200usize.saturating_sub(connect_body.len());
    connect_body.extend(std::iter::repeat_n(b'a', pad_len));
    assert_eq!(connect_body.len(), 200);

    // Frame 1: 0x10 followed by only the FIRST byte of the 2-byte remaining length (0xC8).
    let frame1 = vec![0x10, 0xC8];
    client.send_bin(&frame1).await;

    // Small delay to verify connection does not prematurely disconnect.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // Frame 2: 2nd byte of remaining length (0x01) followed by the 200-byte body.
    let mut frame2 = vec![0x01];
    frame2.extend_from_slice(&connect_body);
    client.send_bin(&frame2).await;

    // Server must assemble packet cleanly and reply with CONNACK 0x00.
    let connack = client.recv_msg().await.expect("connack message");
    assert_eq!(connack[0] >> 4, 2, "packet type must be CONNACK");
    assert_eq!(connack[3], 0x00, "handshake must succeed with CONNACK 0");
}

#[tokio::test]
async fn test_ws_malformed_remaining_length_exceeding_four_continuation_bytes_disconnects() {
    let (port, _state) = start_test_server().await;
    let mut client = TestWsClient::connect(port).await;

    // Send 5 continuation bytes for remaining length (>4 is malformed in MQTT).
    let malformed_packet = vec![0x10, 0x80, 0x80, 0x80, 0x80, 0x80];
    client.send_bin(&malformed_packet).await;

    // Server must disconnect client due to malformed header.
    let res = client.recv_msg().await;
    assert!(
        res.is_none(),
        "malformed header with >4 continuation bytes must cause disconnection"
    );
}

#[tokio::test]
async fn test_ws_mqtt311_error_codes_connack_and_suback() {
    let (port, state) = start_test_server().await;
    state
        .auth
        .add_user("alice", b"correct_pass")
        .expect("user persists");

    // 1. Bad credentials receives MQTT 3.1.1 CONNACK 0x04.
    let mut bad_auth_client = TestWsClient::connect(port).await;
    bad_auth_client
        .send_bin(&build_connect(
            "ws-bad-auth",
            Some("alice"),
            Some(b"wrong_pass"),
        ))
        .await;
    let connack_bad = bad_auth_client.recv_msg().await.expect("connack packet");
    assert_eq!(connack_bad[0] >> 4, 2, "packet type must be CONNACK");
    assert_eq!(
        connack_bad[3], 0x04,
        "bad credential CONNECT must receive CONNACK 0x04"
    );

    // 2. Anonymous refusal receives MQTT 3.1.1 CONNACK 0x05.
    let mut anon_client = TestWsClient::connect(port).await;
    anon_client
        .send_bin(&build_connect("ws-anon-refusal", None, None))
        .await;
    let connack_anon = anon_client.recv_msg().await.expect("connack packet");
    assert_eq!(connack_anon[0] >> 4, 2, "packet type must be CONNACK");
    assert_eq!(
        connack_anon[3], 0x05,
        "anonymous refusal must receive CONNACK 0x05"
    );

    // 3. Subscribe quota refusal receives MQTT 3.1.1 SUBACK 0x80.
    // Fill router to MAX_SUBSCRIPTIONS.
    for i in 0..MAX_SUBSCRIPTIONS {
        let a = i / 10_000;
        let b = (i / 100) % 100;
        let c = i % 100;
        let filter = TopicFilter::new(format!("quota/test/{a}/{b:02}/{c:02}")).unwrap();
        state.router.subscribe(
            &filter,
            Subscription::new(format!("sub-{i}"), i as u64, QoS::AtMostOnce),
        );
    }

    let mut sub_client = TestWsClient::connect(port).await;
    sub_client
        .send_bin(&build_connect(
            "ws-quota-client",
            Some("alice"),
            Some(b"correct_pass"),
        ))
        .await;
    let connack_ok = sub_client.recv_msg().await.expect("connack packet");
    assert_eq!(connack_ok[3], 0x00, "login must succeed");

    sub_client
        .send_bin(&build_subscribe(42, &[("quota/overflow", 0)]))
        .await;
    let suback = sub_client.recv_msg().await.expect("suback packet");
    assert_eq!(suback[0] >> 4, 9, "packet type must be SUBACK");
    assert_eq!(
        suback[4], 0x80,
        "subscribe quota refusal must receive SUBACK 0x80"
    );
}
