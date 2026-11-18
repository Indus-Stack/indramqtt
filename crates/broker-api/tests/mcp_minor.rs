//! Minor test proving body_base64 execution and validation over MCP.

use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use broker_api::v5::api_keys::{KeyRole, NewApiKey};
use broker_api::ApiState;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Minimal WebSocket client over TCP for MQTT frames.
struct WsClient {
    stream: tokio::net::TcpStream,
    buf: Vec<u8>,
}

impl WsClient {
    async fn connect(port: u16) -> Self {
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connect test tcp stream");
        let req = "GET /ws/mqtt HTTP/1.1\r\nHost: test\r\nUpgrade: websocket\r\n\
                   Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                   Sec-WebSocket-Version: 13\r\n\r\n";
        stream
            .write_all(req.as_bytes())
            .await
            .expect("write ws upgrade");
        let mut buf = Vec::new();
        loop {
            let mut chunk = [0u8; 512];
            let n = stream.read(&mut chunk).await.expect("read upgrade chunk");
            assert!(n > 0, "upgrade ended early");
            buf.extend_from_slice(&chunk[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&buf[..pos + 4]);
                assert!(head.starts_with("HTTP/1.1 101"), "upgrade must answer 101");
                let rest = buf[pos + 4..].to_vec();
                return Self { stream, buf: rest };
            }
        }
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
        let mask = [0x11u8, 0x22, 0x33, 0x44];
        frame.extend_from_slice(&mask);
        for (i, byte) in payload.iter().enumerate() {
            frame.push(byte ^ mask[i % 4]);
        }
        self.stream.write_all(&frame).await.expect("write frame");
    }

    async fn recv_msg(&mut self) -> Option<Vec<u8>> {
        loop {
            if let Some((payload, consumed)) = parse_ws_frame(&self.buf) {
                self.buf.drain(..consumed);
                return Some(payload);
            }
            let mut chunk = [0u8; 4096];
            match self.stream.read(&mut chunk).await.expect("read chunk") {
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

fn mqtt_remaining(mut n: usize) -> Vec<u8> {
    let mut out = Vec::new();
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
    out
}

fn mqtt_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn mqtt_connect(client_id: &str) -> Vec<u8> {
    let mut body = vec![0, 4, b'M', b'Q', b'T', b'T', 4, 0x02, 0, 60];
    mqtt_str(&mut body, client_id);
    let mut packet = vec![0x10];
    packet.extend_from_slice(&mqtt_remaining(body.len()));
    packet.extend_from_slice(&body);
    packet
}

fn mqtt_subscribe(pid: u16, subs: &[(&str, u8)]) -> Vec<u8> {
    let mut body = pid.to_be_bytes().to_vec();
    for (filter, qos) in subs {
        mqtt_str(&mut body, filter);
        body.push(*qos);
    }
    let mut packet = vec![0x82];
    packet.extend_from_slice(&mqtt_remaining(body.len()));
    packet.extend_from_slice(&body);
    packet
}

fn mqtt_packet_type(packet: &[u8]) -> u8 {
    packet[0] >> 4
}

async fn start_server() -> (String, String, ApiState) {
    let engine = Arc::new(broker_rules::RuleEngine::new(
        16,
        broker_rules::BackpressurePolicy::DropOldest,
    ));
    let state = ApiState::standalone(engine);
    let mut admin_new = NewApiKey::new("mcp-minor-admin");
    admin_new.role = KeyRole::Administrator;
    let (_, admin_secret) = state.api_keys.create(admin_new).expect("mint admin key");
    let app = broker_api::router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let port = listener.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve test app");
    });
    (format!("http://127.0.0.1:{port}"), admin_secret, state)
}

#[tokio::test]
async fn mn01_mcp_body_base64_publish_and_error() {
    let (base, admin_secret, _state) = start_server().await;
    let url_parsed = reqwest::Url::parse(&base).expect("valid base url");
    let port = url_parsed.port().expect("valid port");

    // Connect a subscriber client over a real MQTT socket.
    let mut sub = WsClient::connect(port).await;
    sub.send_bin(&mqtt_connect("mn01-subscriber")).await;
    let connack = tokio::time::timeout(Duration::from_secs(5), sub.recv_msg())
        .await
        .expect("connack timeout")
        .expect("connack message");
    assert_eq!(mqtt_packet_type(&connack), 2, "packet is CONNACK");
    assert_eq!(connack[3], 0, "CONNACK return code is 0");

    // Subscribe to topic mn01/b64.
    sub.send_bin(&mqtt_subscribe(101, &[("mn01/b64", 0)])).await;
    let suback = tokio::time::timeout(Duration::from_secs(5), sub.recv_msg())
        .await
        .expect("suback timeout")
        .expect("suback message");
    assert_eq!(mqtt_packet_type(&suback), 9, "packet is SUBACK");

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("client builds");

    // Call the MCP tool of POST /publish with body_base64.
    let publish_body = json!({
        "topic": "mn01/b64",
        "payload": "hello"
    });
    let body_base64 = base64::engine::general_purpose::STANDARD.encode(publish_body.to_string());

    let resp = client
        .post(format!("{base}/mcp"))
        .bearer_auth(&admin_secret)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "post_api_v5_publish",
                "arguments": {
                    "body_base64": body_base64,
                }
            }
        }))
        .send()
        .await
        .expect("mcp call succeeds");

    assert_eq!(resp.status().as_u16(), 200);
    let body: Value = resp.json().await.expect("body is JSON");
    assert!(body.get("error").is_none(), "no JSON-RPC error: {body}");
    let is_error = body["result"]
        .get("isError")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    assert!(!is_error, "success result must not have isError: {body}");

    // Assert that the subscriber receives payload hello on mn01/b64.
    let delivery = tokio::time::timeout(Duration::from_secs(5), sub.recv_msg())
        .await
        .expect("delivery timeout")
        .expect("delivery packet");
    assert_eq!(mqtt_packet_type(&delivery), 3, "packet is PUBLISH");
    assert!(
        delivery
            .windows(b"mn01/b64".len())
            .any(|w| w == b"mn01/b64"),
        "delivery topic is mn01/b64: {delivery:?}"
    );
    assert!(
        delivery.ends_with(b"hello"),
        "subscriber receives payload hello"
    );

    // Call the MCP tool with body_base64 that is not base64.
    let resp2 = client
        .post(format!("{base}/mcp"))
        .bearer_auth(&admin_secret)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "post_api_v5_publish",
                "arguments": {
                    "body_base64": "not base64!",
                }
            }
        }))
        .send()
        .await
        .expect("mcp call succeeds");

    let body2: Value = resp2.json().await.expect("body is JSON");
    assert_eq!(body2["result"]["isError"], json!(true));
    assert_eq!(
        body2["result"]["content"][0]["text"],
        json!("body_base64 is not base64")
    );
}
