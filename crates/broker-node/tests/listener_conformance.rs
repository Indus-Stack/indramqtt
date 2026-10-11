//! Socket conformance suite for the kernel MQTT listener.
//!
//! Each test boots one kernel with `native = true` on ephemeral ports
//! and speaks MQTT over real TCP and WebSocket sockets. No test uses
//! the frame harness or the store directly. The suite covers CONNECT
//! levels, timeouts, limits, QoS replay, will, takeover, retained,
//! malformed input, aliases, receive maximum, packet size, subscription
//! options, shared subscriptions, ban and kick, empty client ids and
//! the WebSocket transport.

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Bound for one socket step. A missing packet fails instead of hanging.
const STEP: Duration = Duration::from_secs(10);
/// Bound for one kernel boot.
const BOOT: Duration = Duration::from_secs(60);
/// Quiet wait. No packet in this time means no delivery.
const QUIET: Duration = Duration::from_millis(1200);
/// API key for the management API. Long enough for the minimum length.
const API_KEY: &str = "lc-conformance-test-key-0001";

/// One booted kernel for a test.
struct Kernel {
    child: tokio::process::Child,
    mqtt_port: u16,
    ws_port: u16,
    api_port: u16,
    scratch: std::path::PathBuf,
    config_dir: std::path::PathBuf,
}

impl Kernel {
    /// Stops the kernel and removes its temp dirs.
    async fn finish(mut self) {
        let _ = self.child.kill().await;
        let _ = std::fs::remove_dir_all(&self.scratch);
        let _ = std::fs::remove_dir_all(&self.config_dir);
    }
}

/// Locates the kernel binary under test.
fn kernel_binary() -> std::path::PathBuf {
    if let Some(built) = option_env!("CARGO_BIN_EXE_indramqtt") {
        return std::path::PathBuf::from(built);
    }
    let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for candidate in [
        manifest.join("../../target/debug/indramqtt"),
        manifest.join("../../target/ci-check/debug/indramqtt"),
    ] {
        if candidate.is_file() {
            return candidate;
        }
    }
    panic!("kernel binary not found: build the workspace first");
}

/// Takes one free loopback port.
fn ephemeral_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("probe binds")
        .local_addr()
        .expect("probe addr")
        .port()
}

/// Unique tag for temp dirs of one test.
fn tag(name: &str) -> String {
    format!(
        "lc-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    )
}

/// Boots one kernel with the kernel listener on ephemeral ports.
/// `tcp_max` sets `listeners.tcp.max_connections` when present.
async fn spawn_kernel(name: &str, tcp_max: Option<u32>) -> Kernel {
    let tag = tag(name);
    let scratch = std::env::temp_dir().join(format!("indramqtt-{tag}-data"));
    let config_dir = std::env::temp_dir().join(format!("indramqtt-{tag}-cfg"));
    std::fs::create_dir_all(&config_dir).expect("create temp config dir");
    let bl_port = ephemeral_port();
    let mqtt_port = ephemeral_port();
    let ws_port = ephemeral_port();
    let api_port = ephemeral_port();
    let max_line = tcp_max.map_or(String::new(), |m| format!("max_connections = {m}\n"));
    std::fs::write(
        config_dir.join("indra.toml"),
        format!(
            "[node]\n\
             id = \"lc-{tag}\"\n\
             data_dir = \"{}\"\n\
             brokerlink_bind = \"127.0.0.1:{bl_port}\"\n\
             \n\
             [listeners.tcp]\n\
             bind = \"127.0.0.1:{mqtt_port}\"\n\
             native = true\n\
             {max_line}\
             [listeners.ws]\n\
             bind = \"127.0.0.1:{ws_port}\"\n\
             native = true\n\
             \n\
             [listeners.api]\n\
             bind = \"127.0.0.1:{api_port}\"\n\
             \n\
             [logging]\n\
             level = \"warn\"\n",
            scratch.display(),
        ),
    )
    .expect("write indra.toml");
    let mut child = tokio::process::Command::new(kernel_binary())
        .arg("--config-dir")
        .arg(&config_dir)
        .arg("--allow-anonymous")
        .env("INDRA_API_KEYS", API_KEY)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn real kernel");
    let http = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{api_port}");
    let deadline = std::time::Instant::now() + BOOT;
    loop {
        let up = tokio::time::timeout(
            Duration::from_secs(2),
            http.get(format!("{base}/healthz")).send(),
        )
        .await;
        if let Ok(Ok(res)) = up {
            if let Ok(text) = res.text().await {
                if text == "OK"
                    && tokio::net::TcpStream::connect(("127.0.0.1", mqtt_port))
                        .await
                        .is_ok()
                    && tokio::net::TcpStream::connect(("127.0.0.1", ws_port))
                        .await
                        .is_ok()
                {
                    break;
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill().await;
            panic!("kernel never opened API {api_port} and MQTT {mqtt_port}");
        }
        if let Ok(Some(status)) = child.try_wait() {
            panic!("kernel exited during boot: {status}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Kernel {
        child,
        mqtt_port,
        ws_port,
        api_port,
        scratch,
        config_dir,
    }
}

/// Opens one TCP connection to the kernel listener.
async fn dial(port: u16) -> tokio::net::TcpStream {
    tokio::time::timeout(STEP, tokio::net::TcpStream::connect(("127.0.0.1", port)))
        .await
        .expect("dial in time")
        .expect("dial")
}

/// Sends bytes with a bound.
async fn send(sock: &mut tokio::net::TcpStream, bytes: &[u8]) {
    tokio::time::timeout(STEP, sock.write_all(bytes))
        .await
        .expect("send in time")
        .expect("send");
}

/// Reads one MQTT packet: the first byte plus the body.
async fn recv(sock: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    tokio::time::timeout(STEP, read_mqtt_packet(sock))
        .await
        .expect("packet in time")
        .expect("packet")
}

/// Reads one MQTT packet from the socket.
async fn read_mqtt_packet(sock: &mut tokio::net::TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut head = [0u8; 1];
    sock.read_exact(&mut head).await?;
    let mut multiplier = 1usize;
    let mut remaining = 0usize;
    loop {
        let mut byte = [0u8; 1];
        sock.read_exact(&mut byte).await?;
        remaining += ((byte[0] & 0x7F) as usize) * multiplier;
        if byte[0] & 0x80 == 0 {
            break;
        }
        multiplier *= 128;
        if multiplier > 128 * 128 * 128 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "remaining length overflow",
            ));
        }
    }
    let mut body = vec![0u8; remaining];
    sock.read_exact(&mut body).await?;
    Ok((head[0], body))
}

/// Encodes a remaining length.
fn rl(mut len: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            break;
        }
    }
    out
}

/// Encodes an MQTT string.
fn mqtt_str(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + s.len());
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
    out
}

/// Encodes a variable byte integer.
fn varint(mut value: u32) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
    out
}

/// Builds an MQTT 3.1.1 CONNECT.
fn connect311(client_id: &str, clean: bool, keepalive: u16) -> Vec<u8> {
    let mut rest = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04];
    rest.push(if clean { 0x02 } else { 0x00 });
    rest.extend_from_slice(&keepalive.to_be_bytes());
    rest.extend_from_slice(&mqtt_str(client_id));
    let mut packet = vec![0x10];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// Builds an MQTT 3.1 CONNECT with the old protocol name.
fn connect31(client_id: &str, keepalive: u16) -> Vec<u8> {
    let mut rest = vec![0x00, 0x06, b'M', b'Q', b'I', b's', b'd', b'p', 0x03, 0x02];
    rest.extend_from_slice(&keepalive.to_be_bytes());
    rest.extend_from_slice(&mqtt_str(client_id));
    let mut packet = vec![0x10];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// Builds an MQTT 3.1.1 CONNECT with a last will.
fn connect311_will(
    client_id: &str,
    keepalive: u16,
    will_topic: &str,
    will_payload: &[u8],
) -> Vec<u8> {
    let mut rest = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x06];
    rest.extend_from_slice(&keepalive.to_be_bytes());
    rest.extend_from_slice(&mqtt_str(client_id));
    rest.extend_from_slice(&mqtt_str(will_topic));
    rest.extend_from_slice(&(will_payload.len() as u16).to_be_bytes());
    rest.extend_from_slice(will_payload);
    let mut packet = vec![0x10];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// Builds an MQTT 5 CONNECT. `props` carries the raw property section.
fn connect5_with_props(client_id: &str, clean: bool, keepalive: u16, props: &[u8]) -> Vec<u8> {
    let mut rest = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05];
    rest.push(if clean { 0x02 } else { 0x00 });
    rest.extend_from_slice(&keepalive.to_be_bytes());
    rest.extend_from_slice(&varint(props.len() as u32));
    rest.extend_from_slice(props);
    rest.extend_from_slice(&mqtt_str(client_id));
    let mut packet = vec![0x10];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// Builds an MQTT 5 CONNECT with a last will and an alias maximum.
fn connect5(client_id: &str, clean: bool, keepalive: u16, alias_max: u16) -> Vec<u8> {
    let mut props = Vec::new();
    if alias_max != 0 {
        props.push(34);
        props.extend_from_slice(&alias_max.to_be_bytes());
    }
    connect5_with_props(client_id, clean, keepalive, &props)
}

/// Builds an MQTT 5 CONNECT with a receive maximum.
fn connect5_recvmax(client_id: &str, keepalive: u16, max: u16) -> Vec<u8> {
    let props = vec![33, (max >> 8) as u8, (max & 0xFF) as u8];
    connect5_with_props(client_id, true, keepalive, &props)
}

/// Builds a classic SUBSCRIBE.
fn subscribe(packet_id: u16, subs: &[(&str, u8)]) -> Vec<u8> {
    let mut rest = Vec::new();
    rest.extend_from_slice(&packet_id.to_be_bytes());
    for (filter, qos) in subs {
        rest.extend_from_slice(&mqtt_str(filter));
        rest.push(*qos);
    }
    let mut packet = vec![0x82];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// Builds an MQTT 5 SUBSCRIBE with a subscription identifier.
fn subscribe_v5(packet_id: u16, subs: &[(&str, u8)], sub_id: u32) -> Vec<u8> {
    let mut props = Vec::new();
    if sub_id != 0 {
        props.push(11u8);
        props.extend_from_slice(&varint(sub_id));
    }
    let mut rest = Vec::new();
    rest.extend_from_slice(&packet_id.to_be_bytes());
    rest.extend_from_slice(&varint(props.len() as u32));
    rest.extend_from_slice(&props);
    for (filter, opts) in subs {
        rest.extend_from_slice(&mqtt_str(filter));
        rest.push(*opts);
    }
    let mut packet = vec![0x82];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// Builds an MQTT 3.1.1 PUBLISH.
fn publish311(topic: &str, packet_id: u16, qos: u8, retain: bool, payload: &[u8]) -> Vec<u8> {
    let mut rest = mqtt_str(topic);
    if qos > 0 {
        rest.extend_from_slice(&packet_id.to_be_bytes());
    }
    rest.extend_from_slice(payload);
    let mut packet = vec![0x30 | (qos << 1) | u8::from(retain)];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// Builds an MQTT 5 PUBLISH. `alias` sets the topic alias property.
fn publish5(
    topic: &str,
    packet_id: u16,
    qos: u8,
    retain: bool,
    alias: Option<u16>,
    payload: &[u8],
) -> Vec<u8> {
    let mut rest = mqtt_str(topic);
    if qos > 0 {
        rest.extend_from_slice(&packet_id.to_be_bytes());
    }
    match alias {
        Some(value) => {
            rest.push(0x03);
            rest.push(35);
            rest.extend_from_slice(&value.to_be_bytes());
        }
        None => rest.push(0x00),
    }
    rest.extend_from_slice(payload);
    let mut packet = vec![0x30 | (qos << 1) | u8::from(retain)];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// Builds PUBACK, PUBREC, PUBREL or PUBCOMP with one packet id.
fn ack(kind: u8, packet_id: u16) -> Vec<u8> {
    vec![kind, 0x02, (packet_id >> 8) as u8, (packet_id & 0xFF) as u8]
}

/// Parses a 3.1.1 CONNACK body into presence plus return code.
fn parse_connack311(body: &[u8]) -> (bool, u8) {
    assert_eq!(body.len(), 2);
    (body[0] & 0x01 != 0, body[1])
}

/// Parses an MQTT 5 CONNACK body into presence, reason and alias maximum.
fn parse_connack5(body: &[u8]) -> (bool, u8, u16) {
    assert!(body.len() >= 3);
    let present = body[0] & 0x01 != 0;
    let reason = body[1];
    let (prop_len, used) = decode_varint(&body[2..]).expect("property length");
    let mut props = &body[2 + used..][..prop_len];
    let mut alias_max = 0u16;
    while !props.is_empty() {
        let (id, used) = decode_varint(props).expect("property id");
        props = &props[used..];
        match id {
            34 => {
                alias_max = u16::from_be_bytes([props[0], props[1]]);
                props = &props[2..];
            }
            17 | 39 => props = &props[4..],
            33 => props = &props[2..],
            18 | 28 | 31 => {
                let len = u16::from_be_bytes([props[0], props[1]]) as usize;
                props = &props[2 + len..];
            }
            38 => {
                for _ in 0..2 {
                    let len = u16::from_be_bytes([props[0], props[1]]) as usize;
                    props = &props[2 + len..];
                }
            }
            _ => panic!("unexpected CONNACK property {id}"),
        }
    }
    (present, reason, alias_max)
}

/// Reports whether an MQTT 5 CONNACK carries property `prop_id`.
fn connack5_has(body: &[u8], prop_id: usize) -> bool {
    let (prop_len, used) = decode_varint(&body[2..]).expect("property length");
    let mut props = &body[2 + used..][..prop_len];
    while !props.is_empty() {
        let (id, used) = decode_varint(props).expect("property id");
        props = &props[used..];
        if id == prop_id {
            return true;
        }
        match id {
            17 | 39 => props = &props[4..],
            33 | 34 => props = &props[2..],
            18 | 28 | 31 => {
                let len = u16::from_be_bytes([props[0], props[1]]) as usize;
                props = &props[2 + len..];
            }
            38 => {
                for _ in 0..2 {
                    let len = u16::from_be_bytes([props[0], props[1]]) as usize;
                    props = &props[2 + len..];
                }
            }
            _ => panic!("unexpected CONNACK property {id}"),
        }
    }
    false
}

/// Decodes one variable byte integer into value plus bytes used.
fn decode_varint(mut bytes: &[u8]) -> Option<(usize, usize)> {
    let mut value = 0usize;
    let mut used = 0usize;
    loop {
        let byte = *bytes.first()?;
        bytes = &bytes[1..];
        used += 1;
        value += ((byte & 0x7F) as usize) * 128usize.pow(used as u32 - 1);
        if byte & 0x80 == 0 {
            return Some((value, used));
        }
        if used == 4 {
            return None;
        }
    }
}

/// One classic PUBLISH delivery.
struct Publish {
    topic: String,
    packet_id: u16,
    qos: u8,
    retain: bool,
    dup: bool,
    payload: Vec<u8>,
}

/// Parses a classic PUBLISH delivery.
fn parse_publish(header: u8, body: &[u8]) -> Publish {
    let dup = header & 0x08 != 0;
    let qos = (header >> 1) & 0x03;
    let retain = header & 0x01 != 0;
    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    let topic = std::str::from_utf8(&body[2..2 + topic_len])
        .expect("topic")
        .to_string();
    let mut rest = &body[2 + topic_len..];
    let packet_id = if qos > 0 {
        let id = u16::from_be_bytes([rest[0], rest[1]]);
        rest = &rest[2..];
        id
    } else {
        0
    };
    Publish {
        topic,
        packet_id,
        qos,
        retain,
        dup,
        payload: rest.to_vec(),
    }
}

/// One MQTT 5 PUBLISH delivery.
struct Publish5 {
    topic: String,
    packet_id: u16,
    dup: bool,
    sub_id: u32,
    alias: u16,
    payload: Vec<u8>,
}

/// Parses an MQTT 5 PUBLISH delivery.
fn parse_publish5(header: u8, body: &[u8]) -> Publish5 {
    let dup = header & 0x08 != 0;
    let qos = (header >> 1) & 0x03;
    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    let topic = std::str::from_utf8(&body[2..2 + topic_len])
        .expect("topic")
        .to_string();
    let mut rest = &body[2 + topic_len..];
    let packet_id = if qos > 0 {
        let id = u16::from_be_bytes([rest[0], rest[1]]);
        rest = &rest[2..];
        id
    } else {
        0
    };
    let (prop_len, used) = decode_varint(rest).expect("property length");
    rest = &rest[used..];
    let mut props = &rest[..prop_len];
    let mut sub_id = 0u32;
    let mut alias = 0u16;
    while !props.is_empty() {
        let (id, used) = decode_varint(props).expect("property id");
        props = &props[used..];
        match id {
            11 => {
                let (v, used) = decode_varint(props).expect("sub id");
                sub_id = v as u32;
                props = &props[used..];
            }
            35 => {
                alias = u16::from_be_bytes([props[0], props[1]]);
                props = &props[2..];
            }
            1 => props = &props[1..],
            2 => props = &props[4..],
            38 => {
                for _ in 0..2 {
                    let len = u16::from_be_bytes([props[0], props[1]]) as usize;
                    props = &props[2 + len..];
                }
            }
            _ => panic!("unexpected PUBLISH property {id}"),
        }
    }
    Publish5 {
        topic,
        packet_id,
        dup,
        sub_id,
        alias,
        payload: rest[prop_len..].to_vec(),
    }
}

/// Connects one 3.1.1 client and checks its CONNACK. Returns presence.
async fn connect_client(
    sock: &mut tokio::net::TcpStream,
    client_id: &str,
    clean: bool,
    keepalive: u16,
) -> bool {
    send(sock, &connect311(client_id, clean, keepalive)).await;
    let (head, body) = recv(sock).await;
    assert_eq!(head, 0x20);
    let (present, code) = parse_connack311(&body);
    assert_eq!(code, 0, "CONNECT of {client_id} is accepted");
    present
}

/// Completes a WebSocket upgrade. Returns the status line.
async fn ws_handshake_raw(
    sock: &mut tokio::net::TcpStream,
    host: &str,
    port: u16,
    path: &str,
    subprotocol: Option<&str>,
) -> String {
    let mut request = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n"
    );
    if let Some(proto) = subprotocol {
        request.push_str(&format!("Sec-WebSocket-Protocol: {proto}\r\n"));
    }
    request.push_str("\r\n");
    tokio::time::timeout(STEP, sock.write_all(request.as_bytes()))
        .await
        .expect("handshake in time")
        .expect("handshake send");
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        tokio::time::timeout(STEP, sock.read_exact(&mut byte))
            .await
            .expect("handshake in time")
            .expect("handshake read");
        head.push(byte[0]);
        if head.len() >= 4 && head[head.len() - 4..] == *b"\r\n\r\n" {
            break;
        }
        assert!(head.len() <= 8192, "handshake head stays bounded");
    }
    String::from_utf8_lossy(&head)
        .lines()
        .next()
        .unwrap_or_default()
        .to_string()
}

/// Sends one masked client WebSocket frame.
async fn ws_send(sock: &mut tokio::net::TcpStream, opcode: u8, payload: &[u8]) {
    const MASK: [u8; 4] = [0x12, 0x34, 0x56, 0x78];
    let mut frame = vec![0x80 | opcode];
    if payload.len() < 126 {
        frame.push(0x80 | payload.len() as u8);
    } else {
        frame.push(0x80 | 126);
        frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    }
    frame.extend_from_slice(&MASK);
    for (i, byte) in payload.iter().enumerate() {
        frame.push(byte ^ MASK[i % 4]);
    }
    send(sock, &frame).await;
}

/// Reads one server WebSocket frame into opcode plus payload.
async fn ws_recv(sock: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    loop {
        let mut head = [0u8; 2];
        tokio::time::timeout(STEP, sock.read_exact(&mut head))
            .await
            .expect("frame in time")
            .expect("frame head");
        let opcode = head[0] & 0x0F;
        let mut length = (head[1] & 0x7F) as usize;
        if length == 126 {
            let mut ext = [0u8; 2];
            tokio::time::timeout(STEP, sock.read_exact(&mut ext))
                .await
                .expect("frame in time")
                .expect("frame length");
            length = u16::from_be_bytes(ext) as usize;
        }
        assert!(length <= 16_777_216, "frame stays bounded");
        let mut payload = vec![0u8; length];
        tokio::time::timeout(STEP, sock.read_exact(&mut payload))
            .await
            .expect("frame in time")
            .expect("frame payload");
        match opcode {
            0x2 | 0x8 | 0xA => return (opcode, payload),
            0x9 => {
                ws_send(sock, 0xA, &payload).await;
                continue;
            }
            _ => panic!("unexpected WS opcode {opcode}"),
        }
    }
}

/// CONNECT at levels 3, 4 and 5 is accepted through the socket.
#[tokio::test]
async fn lc_connect_levels_3_4_5_accept() {
    let kernel = spawn_kernel("levels", None).await;
    let mut third = dial(kernel.mqtt_port).await;
    send(&mut third, &connect31("lc-level3", 60)).await;
    let (head, body) = recv(&mut third).await;
    assert_eq!(head, 0x20);
    assert_eq!(parse_connack311(&body), (false, 0));

    let mut fourth = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut fourth, "lc-level4", true, 60).await);

    let mut fifth = dial(kernel.mqtt_port).await;
    send(&mut fifth, &connect5("lc-level5", true, 60, 0)).await;
    let (head, body) = recv(&mut fifth).await;
    assert_eq!(head & 0xF0, 0x20);
    let (present, reason, _) = parse_connack5(&body);
    assert!(!present);
    assert_eq!(reason, 0);
    kernel.finish().await;
}

/// An empty client id connects. The CONNACK carries no assigned id.
/// TODO(parity): the rulebook does not decide whether an empty id gets
/// an assigned identifier; today every empty id shares one session.
#[tokio::test]
async fn lc_empty_client_id_documents_shared_session() {
    let kernel = spawn_kernel("empty-id", None).await;
    let mut first = dial(kernel.mqtt_port).await;
    send(&mut first, &connect311("", true, 60)).await;
    let (head, body) = recv(&mut first).await;
    assert_eq!(head, 0x20);
    assert_eq!(parse_connack311(&body), (false, 0));

    let mut second = dial(kernel.mqtt_port).await;
    send(&mut second, &connect311("", true, 60)).await;
    let (head, body) = recv(&mut second).await;
    assert_eq!(head, 0x20);
    assert_eq!(parse_connack311(&body), (false, 0));

    let mut fifth = dial(kernel.mqtt_port).await;
    send(&mut fifth, &connect5("", true, 60, 0)).await;
    let (head, body) = recv(&mut fifth).await;
    assert_eq!(head & 0xF0, 0x20);
    let (_, reason, _) = parse_connack5(&body);
    assert_eq!(reason, 0);
    assert!(!connack5_has(&body, 18), "no assigned client id is sent");
    kernel.finish().await;
}

/// An idle client closes after 1.5 times its keepalive. A client with
/// keepalive 0 stays open and answers PINGREQ.
#[tokio::test]
async fn lc_keepalive_timeout_closes_idle_client() {
    let kernel = spawn_kernel("keepalive", None).await;
    let mut idle = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut idle, "lc-keep-idle", true, 2).await);
    let closed = tokio::time::timeout(Duration::from_secs(10), read_mqtt_packet(&mut idle)).await;
    assert!(
        closed.is_err() || matches!(&closed, Ok(Err(_))),
        "idle socket closes"
    );

    let mut steady = dial(kernel.mqtt_port).await;
    send(&mut steady, &connect311("lc-keep-steady", true, 0)).await;
    let (head, _) = recv(&mut steady).await;
    assert_eq!(head, 0x20);
    tokio::time::sleep(Duration::from_secs(4)).await;
    send(&mut steady, &[0xC0, 0x00]).await;
    let (head, body) = recv(&mut steady).await;
    assert_eq!(head, 0xD0);
    assert!(body.is_empty());
    kernel.finish().await;
}

/// A socket with no CONNECT closes after the 10 s CONNECT timeout.
#[tokio::test]
async fn lc_connect_timeout_closes_silent_socket() {
    let kernel = spawn_kernel("conn-timeout", None).await;
    let mut silent = dial(kernel.mqtt_port).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut live = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut live, "lc-conntime-live", true, 60).await);
    let closed = tokio::time::timeout(Duration::from_secs(15), read_mqtt_packet(&mut silent)).await;
    assert!(
        closed.is_err() || matches!(&closed, Ok(Err(_))),
        "silent socket closes"
    );
    kernel.finish().await;
}

/// The listener refuses connections beyond `max_connections`.
#[tokio::test]
async fn lc_max_connections_refuses_beyond_limit() {
    let kernel = spawn_kernel("max-conn", Some(2)).await;
    // The readiness probe drops at once; still, it frees its slot
    // asynchronously, so the test waits before it fills the limit.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut first = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut first, "lc-max-1", true, 60).await);
    let mut second = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut second, "lc-max-2", true, 60).await);
    let mut third = dial(kernel.mqtt_port).await;
    let _ = tokio::time::timeout(STEP, third.write_all(&connect311("lc-max-3", true, 60))).await;
    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut third)).await;
    assert!(
        closed.is_err() || matches!(&closed, Ok(Err(_))),
        "third socket closes"
    );
    kernel.finish().await;
}

/// QoS 1 delivers both ways. An unacked downlink replays with DUP set
/// after a reconnect of the durable session.
#[tokio::test]
async fn lc_qos1_dup_replays_after_reconnect() {
    let kernel = spawn_kernel("qos1", None).await;
    let topic = "lc/qos1/replay";
    let mut sub = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub, "lc-qos1-sub", false, 60).await);
    send(&mut sub, &subscribe(1, &[(topic, 1)])).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90);

    let mut publisher = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut publisher, "lc-qos1-pub", true, 60).await);
    send(
        &mut publisher,
        &publish311(topic, 7, 1, false, b"qos1-live"),
    )
    .await;
    let (head, body) = recv(&mut publisher).await;
    assert_eq!(head, 0x40);
    assert_eq!(body, vec![0x00, 0x07]);

    let (head, body) = recv(&mut sub).await;
    let live = parse_publish(head, &body);
    assert_eq!(live.qos, 1);
    assert!(!live.dup);
    assert_eq!(live.payload, b"qos1-live");
    drop(sub);

    let mut resume = dial(kernel.mqtt_port).await;
    send(&mut resume, &connect311("lc-qos1-sub", false, 60)).await;
    let (head, body) = recv(&mut resume).await;
    assert_eq!(head, 0x20);
    assert_eq!(parse_connack311(&body), (true, 0));
    let (head, body) = recv(&mut resume).await;
    let replay = parse_publish(head, &body);
    assert!(replay.dup, "replay sets DUP");
    assert_eq!(replay.packet_id, live.packet_id);
    assert_eq!(replay.payload, b"qos1-live");
    send(&mut resume, &ack(0x40, replay.packet_id)).await;
    kernel.finish().await;
}

/// QoS 2 completes the full handshake. An unacked downlink replays with
/// DUP set after a reconnect of the durable session.
#[tokio::test]
async fn lc_qos2_handshake_completes_and_replays() {
    let kernel = spawn_kernel("qos2", None).await;
    let topic = "lc/qos2/flow";
    let mut sub = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub, "lc-qos2-sub", false, 60).await);
    send(&mut sub, &subscribe(1, &[(topic, 2)])).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90);

    let mut publisher = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut publisher, "lc-qos2-pub", true, 60).await);
    send(
        &mut publisher,
        &publish311(topic, 7, 2, false, b"qos2-live"),
    )
    .await;
    let (head, body) = recv(&mut publisher).await;
    assert_eq!(head, 0x50);
    assert_eq!(body, vec![0x00, 0x07]);
    send(&mut publisher, &ack(0x62, 7)).await;
    let (head, body) = recv(&mut publisher).await;
    assert_eq!(head, 0x70);
    assert_eq!(body, vec![0x00, 0x07]);

    let (head, body) = recv(&mut sub).await;
    let live = parse_publish(head, &body);
    assert_eq!(live.qos, 2);
    assert!(!live.dup);
    send(&mut sub, &ack(0x50, live.packet_id)).await;
    let (head, body) = recv(&mut sub).await;
    assert_eq!(head, 0x62);
    send(&mut sub, &ack(0x70, u16::from_be_bytes([body[0], body[1]]))).await;

    send(
        &mut publisher,
        &publish311(topic, 8, 2, false, b"qos2-drop"),
    )
    .await;
    let (head, _) = recv(&mut publisher).await;
    assert_eq!(head, 0x50);
    send(&mut publisher, &ack(0x62, 8)).await;
    let (head, _) = recv(&mut publisher).await;
    assert_eq!(head, 0x70);
    let (head, body) = recv(&mut sub).await;
    let pending = parse_publish(head, &body);
    assert_eq!(pending.payload, b"qos2-drop");
    drop(sub);

    let mut resume = dial(kernel.mqtt_port).await;
    send(&mut resume, &connect311("lc-qos2-sub", false, 60)).await;
    let (head, body) = recv(&mut resume).await;
    assert_eq!(head, 0x20);
    assert_eq!(parse_connack311(&body), (true, 0));
    let (head, body) = recv(&mut resume).await;
    let replay = parse_publish(head, &body);
    assert!(replay.dup, "replay sets DUP");
    assert_eq!(replay.packet_id, pending.packet_id);
    send(&mut resume, &ack(0x50, replay.packet_id)).await;
    let (head, _) = recv(&mut resume).await;
    assert_eq!(head, 0x62);
    kernel.finish().await;
}

/// The will fires on an abnormal close and never after DISCONNECT.
#[tokio::test]
async fn lc_will_fires_on_drop_not_on_disconnect() {
    let kernel = spawn_kernel("will", None).await;
    let topic = "lc/will/fire";
    let mut watcher = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut watcher, "lc-will-watcher", true, 60).await);
    send(&mut watcher, &subscribe(1, &[(topic, 0)])).await;
    let (head, _) = recv(&mut watcher).await;
    assert_eq!(head, 0x90);

    let mut abrupt = dial(kernel.mqtt_port).await;
    send(
        &mut abrupt,
        &connect311_will("lc-will-abrupt", 60, topic, b"will-bytes"),
    )
    .await;
    let (head, _) = recv(&mut abrupt).await;
    assert_eq!(head, 0x20);
    drop(abrupt);
    let (head, body) = recv(&mut watcher).await;
    let fired = parse_publish(head, &body);
    assert_eq!(fired.payload, b"will-bytes");

    let mut clean = dial(kernel.mqtt_port).await;
    send(
        &mut clean,
        &connect311_will("lc-will-clean", 60, topic, b"no-will"),
    )
    .await;
    let (head, _) = recv(&mut clean).await;
    assert_eq!(head, 0x20);
    send(&mut clean, &[0xE0, 0x00]).await;
    let quiet = tokio::time::timeout(QUIET, read_mqtt_packet(&mut watcher)).await;
    assert!(quiet.is_err(), "no will arrives after DISCONNECT");
    kernel.finish().await;
}

/// A second CONNECT with the same client id resumes the durable session.
/// TODO(parity): the rulebook does not decide whether the old socket gets
/// DISCONNECT 0x8E; today only the new side is observed here.
#[tokio::test]
async fn lc_takeover_resumes_session() {
    let kernel = spawn_kernel("takeover", None).await;
    let topic = "lc/takeover/live";
    let mut first = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut first, "lc-takeover-id", false, 60).await);
    let mut second = dial(kernel.mqtt_port).await;
    send(&mut second, &connect311("lc-takeover-id", false, 60)).await;
    let (head, body) = recv(&mut second).await;
    assert_eq!(head, 0x20);
    assert_eq!(parse_connack311(&body), (true, 0));
    send(&mut second, &subscribe(1, &[(topic, 0)])).await;
    let (head, _) = recv(&mut second).await;
    assert_eq!(head, 0x90);

    let mut publisher = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut publisher, "lc-takeover-pub", true, 60).await);
    send(
        &mut publisher,
        &publish311(topic, 0, 0, false, b"takeover-live"),
    )
    .await;
    let (head, body) = recv(&mut second).await;
    let got = parse_publish(head, &body);
    assert_eq!(got.payload, b"takeover-live");
    kernel.finish().await;
}

/// A retained publish reaches a subscriber that arrives later.
#[tokio::test]
async fn lc_retained_delivers_to_late_subscriber() {
    let kernel = spawn_kernel("retained", None).await;
    let topic = "lc/retained/late";
    let mut publisher = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut publisher, "lc-ret-pub", true, 60).await);
    send(
        &mut publisher,
        &publish311(topic, 0, 0, true, b"retained-bytes"),
    )
    .await;

    let mut sub = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub, "lc-ret-sub", true, 60).await);
    send(&mut sub, &subscribe(1, &[(topic, 0)])).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90);
    let (head, body) = recv(&mut sub).await;
    let got = parse_publish(head, &body);
    assert!(got.retain);
    assert_eq!(got.payload, b"retained-bytes");
    kernel.finish().await;
}

/// A malformed packet closes only its own connection.
#[tokio::test]
async fn lc_malformed_packet_closes_only_that_client() {
    let kernel = spawn_kernel("malformed", None).await;
    let mut good = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut good, "lc-mal-good", true, 60).await);

    let mut bad_flags = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut bad_flags, "lc-mal-flags", true, 60).await);
    let mut rest = vec![0x00, 0x07];
    rest.extend_from_slice(&[0x00, 0x01, b'a', 0x00]);
    let mut packet = vec![0x80];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);
    send(&mut bad_flags, &packet).await;
    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut bad_flags)).await;
    assert!(
        closed.is_err() || matches!(&closed, Ok(Err(_))),
        "bad flags close"
    );

    let mut twice = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut twice, "lc-mal-twice", true, 60).await);
    send(&mut twice, &connect311("lc-mal-twice-2", true, 60)).await;
    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut twice)).await;
    assert!(
        closed.is_err() || matches!(&closed, Ok(Err(_))),
        "second CONNECT closes"
    );

    let mut qos3 = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut qos3, "lc-mal-qos3", true, 60).await);
    send(&mut qos3, &publish311("lc/mal/qos3", 0, 3, false, b"x")).await;
    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut qos3)).await;
    assert!(
        closed.is_err() || matches!(&closed, Ok(Err(_))),
        "QoS 3 closes"
    );

    send(&mut good, &subscribe(1, &[("lc/mal/alive", 0)])).await;
    let (head, _) = recv(&mut good).await;
    assert_eq!(head, 0x90);
    kernel.finish().await;
}

/// Topic aliases work in both directions. The client registers an alias
/// and then publishes by reference; the server assigns an alias on a
/// delivery to a client that sent an alias maximum.
#[tokio::test]
async fn lc_topic_alias_both_directions() {
    let kernel = spawn_kernel("alias", None).await;
    let topic = "lc/alias/roundtrip";
    let mut sub = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub, "lc-alias-sub", true, 60).await);
    send(&mut sub, &subscribe(1, &[(topic, 0)])).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90);

    let mut publisher = dial(kernel.mqtt_port).await;
    send(&mut publisher, &connect5("lc-alias-pub", true, 60, 10)).await;
    let (head, body) = recv(&mut publisher).await;
    assert_eq!(head & 0xF0, 0x20);
    let (_, reason, _) = parse_connack5(&body);
    assert_eq!(reason, 0);
    send(
        &mut publisher,
        &publish5(topic, 0, 0, false, Some(1), b"first"),
    )
    .await;
    send(
        &mut publisher,
        &publish5("", 0, 0, false, Some(1), b"second"),
    )
    .await;
    for want in [b"first".as_slice(), b"second".as_slice()] {
        let (head, body) = recv(&mut sub).await;
        let got = parse_publish(head, &body);
        assert_eq!(got.topic, topic);
        assert_eq!(got.payload, want);
    }

    let mut v5sub = dial(kernel.mqtt_port).await;
    send(&mut v5sub, &connect5("lc-alias-v5sub", true, 60, 10)).await;
    let (head, body) = recv(&mut v5sub).await;
    assert_eq!(head & 0xF0, 0x20);
    let (_, reason, alias_max) = parse_connack5(&body);
    assert_eq!(reason, 0);
    assert!(alias_max > 0, "CONNACK carries the server alias bound");
    send(&mut v5sub, &subscribe_v5(1, &[(topic, 0)], 0)).await;
    let (head, _) = recv(&mut v5sub).await;
    assert_eq!(head, 0x90);
    send(
        &mut publisher,
        &publish5(topic, 0, 0, false, None, b"third"),
    )
    .await;
    let (head, body) = recv(&mut v5sub).await;
    let got = parse_publish5(head, &body);
    assert_eq!(got.topic, topic);
    assert!(got.alias != 0, "server assigns an alias");
    assert_eq!(got.payload, b"third");
    kernel.finish().await;
}

/// The client receive maximum sheds QoS 1 downlinks above the window.
#[tokio::test]
async fn lc_receive_maximum_sheds_above_window() {
    let kernel = spawn_kernel("recvmax", None).await;
    let topic = "lc/recvmax/pace";
    let mut sub = dial(kernel.mqtt_port).await;
    send(&mut sub, &connect5_recvmax("lc-recvmax-sub", 60, 1)).await;
    let (head, body) = recv(&mut sub).await;
    assert_eq!(head & 0xF0, 0x20);
    let (_, reason, _) = parse_connack5(&body);
    assert_eq!(reason, 0);
    send(&mut sub, &subscribe_v5(1, &[(topic, 1)], 0)).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90);

    let mut publisher = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut publisher, "lc-recvmax-pub", true, 60).await);
    for (id, payload) in [(11u16, "pace-1"), (12, "pace-2")] {
        send(
            &mut publisher,
            &publish311(topic, id, 1, false, payload.as_bytes()),
        )
        .await;
        let (head, _) = recv(&mut publisher).await;
        assert_eq!(head, 0x40);
    }
    let (head, body) = recv(&mut sub).await;
    let first = parse_publish5(head, &body);
    assert!(!first.dup);
    assert_eq!(first.payload, b"pace-1");
    let quiet = tokio::time::timeout(QUIET, read_mqtt_packet(&mut sub)).await;
    assert!(
        quiet.is_err(),
        "second downlink sheds above receive-maximum 1"
    );
    send(&mut sub, &ack(0x40, first.packet_id)).await;
    let quiet = tokio::time::timeout(QUIET, read_mqtt_packet(&mut sub)).await;
    assert!(quiet.is_err(), "shed frame never arrives after the ack");
    kernel.finish().await;
}

/// An oversize publish closes the connection. The CONNACK advertises a
/// maximum packet size.
/// TODO(parity): the rulebook does not decide the cap value or the v5
/// 0x95 reply; the setting of step P2 is still missing, so this test
/// documents the fixed 1 MiB cap only.
#[tokio::test]
async fn lc_max_packet_size_documents_fixed_cap() {
    let kernel = spawn_kernel("max-packet", None).await;
    let mut sub = dial(kernel.mqtt_port).await;
    send(&mut sub, &connect5("lc-maxpkt-sub", true, 60, 0)).await;
    let (head, body) = recv(&mut sub).await;
    assert_eq!(head & 0xF0, 0x20);
    assert!(connack5_has(&body, 39), "CONNACK advertises a maximum size");

    let mut big = dial(kernel.mqtt_port).await;
    send(&mut big, &connect5("lc-maxpkt-big", true, 60, 0)).await;
    let (head, _) = recv(&mut big).await;
    assert_eq!(head & 0xF0, 0x20);
    let payload = vec![0x41u8; 1024 * 1024 + 64];
    // A broken pipe already proves the close.
    // Reason: the broker may close while the large write is in flight.
    let oversize = publish5("lc/maxpkt/big", 0, 0, false, None, &payload);
    let _ = tokio::time::timeout(STEP, big.write_all(&oversize)).await;
    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut big)).await;
    assert!(
        closed.is_err() || matches!(&closed, Ok(Err(_))),
        "oversize publish closes"
    );

    let mut live = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut live, "lc-maxpkt-live", true, 60).await);
    kernel.finish().await;
}

/// A subscription identifier rides the delivery. No-local hides a
/// publish from its own publisher only.
#[tokio::test]
async fn lc_subscription_id_and_no_local_options() {
    let kernel = spawn_kernel("subopts", None).await;
    let topic = "lc/subopts/props";
    let mut sub = dial(kernel.mqtt_port).await;
    send(&mut sub, &connect5("lc-subopts-sub", true, 60, 0)).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head & 0xF0, 0x20);
    send(&mut sub, &subscribe_v5(7, &[(topic, 0)], 42)).await;
    let (head, body) = recv(&mut sub).await;
    assert_eq!(head, 0x90);
    assert_eq!(&body[..2], &[0x00, 0x07]);

    let mut publisher = dial(kernel.mqtt_port).await;
    send(&mut publisher, &connect5("lc-subopts-pub", true, 60, 0)).await;
    let (head, _) = recv(&mut publisher).await;
    assert_eq!(head & 0xF0, 0x20);
    send(
        &mut publisher,
        &publish5(topic, 0, 0, false, None, b"with-id"),
    )
    .await;
    let (head, body) = recv(&mut sub).await;
    let got = parse_publish5(head, &body);
    assert_eq!(got.sub_id, 42);
    assert_eq!(got.payload, b"with-id");

    let topic2 = "lc/subopts/nolocal";
    let mut other = dial(kernel.mqtt_port).await;
    send(&mut other, &connect5("lc-subopts-other", true, 60, 0)).await;
    let (head, _) = recv(&mut other).await;
    assert_eq!(head & 0xF0, 0x20);
    send(&mut other, &subscribe_v5(1, &[(topic2, 0)], 0)).await;
    let (head, _) = recv(&mut other).await;
    assert_eq!(head, 0x90);
    send(&mut sub, &subscribe_v5(8, &[(topic2, 0x04)], 0)).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90);
    send(&mut sub, &publish5(topic2, 0, 0, false, None, b"nl-bytes")).await;
    let (head, body) = recv(&mut other).await;
    let got = parse_publish5(head, &body);
    assert_eq!(got.payload, b"nl-bytes");
    let quiet = tokio::time::timeout(QUIET, read_mqtt_packet(&mut sub)).await;
    assert!(quiet.is_err(), "no-local hides the copy from the publisher");
    kernel.finish().await;
}

/// A shared subscription delivers publishes to its member.
#[tokio::test]
async fn lc_shared_subscription_delivers() {
    let kernel = spawn_kernel("shared", None).await;
    let topic = "lc/shared/deliver";
    let mut member = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut member, "lc-shared-member", true, 60).await);
    let filter = format!("$share/lcgroup/{topic}");
    send(&mut member, &subscribe(1, &[(filter.as_str(), 0)])).await;
    let (head, body) = recv(&mut member).await;
    assert_eq!(head, 0x90);
    assert_eq!(&body[2..], &[0x00]);

    let mut publisher = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut publisher, "lc-shared-pub", true, 60).await);
    send(
        &mut publisher,
        &publish311(topic, 0, 0, false, b"shared-bytes"),
    )
    .await;
    let (head, body) = recv(&mut member).await;
    let got = parse_publish(head, &body);
    assert_eq!(got.topic, topic);
    assert_eq!(got.payload, b"shared-bytes");
    kernel.finish().await;
}

/// Kick closes the socket through the management API. A ban refuses a
/// new CONNECT and its removal admits it again.
#[tokio::test]
async fn lc_ban_and_kick_through_api() {
    let kernel = spawn_kernel("bankick", None).await;
    let http = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{}", kernel.api_port);

    let mut kicked = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut kicked, "lc-kick-me", true, 60).await);
    let res = tokio::time::timeout(
        STEP,
        http.delete(format!("{base}/api/v5/clients/lc-kick-me"))
            .bearer_auth(API_KEY)
            .send(),
    )
    .await
    .expect("kick in time")
    .expect("kick send");
    assert_eq!(res.status().as_u16(), 204);
    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut kicked)).await;
    assert!(
        closed.is_err() || matches!(&closed, Ok(Err(_))),
        "kick closes the socket"
    );

    let res = tokio::time::timeout(
        STEP,
        http.post(format!("{base}/api/v5/banned"))
            .bearer_auth(API_KEY)
            .json(&serde_json::json!({"as": "clientid", "who": "lc-banned-id"}))
            .send(),
    )
    .await
    .expect("ban in time")
    .expect("ban send");
    assert_eq!(res.status().as_u16(), 200);
    let mut banned = dial(kernel.mqtt_port).await;
    send(&mut banned, &connect311("lc-banned-id", true, 60)).await;
    let (head, body) = recv(&mut banned).await;
    assert_eq!(head, 0x20);
    assert_ne!(body[1], 0, "banned CONNECT is refused");
    let res = tokio::time::timeout(
        STEP,
        http.delete(format!("{base}/api/v5/banned/clientid/lc-banned-id"))
            .bearer_auth(API_KEY)
            .send(),
    )
    .await
    .expect("unban in time")
    .expect("unban send");
    assert_eq!(res.status().as_u16(), 204);
    let mut admitted = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut admitted, "lc-banned-id", true, 60).await);
    kernel.finish().await;
}

/// MQTT over WebSocket exchanges publishes with TCP. A wrong path gets
/// 404 and a missing subprotocol gets 400.
#[tokio::test]
async fn lc_websocket_serves_mqtt_and_refusals() {
    let kernel = spawn_kernel("ws-basic", None).await;
    let mut ws = dial(kernel.ws_port).await;
    let status =
        ws_handshake_raw(&mut ws, "127.0.0.1", kernel.ws_port, "/mqtt", Some("mqtt")).await;
    assert!(
        status.starts_with("HTTP/1.1 101"),
        "upgrade accepted, got: {status}"
    );
    ws_send(&mut ws, 0x2, &connect311("lc-ws-client", true, 60)).await;
    let (opcode, payload) = ws_recv(&mut ws).await;
    assert_eq!(opcode, 0x2);
    assert_eq!(payload[0], 0x20);

    let mut tcp = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut tcp, "lc-ws-tcp", true, 60).await);
    send(&mut tcp, &subscribe(1, &[("lc/ws/xchange", 0)])).await;
    let (head, _) = recv(&mut tcp).await;
    assert_eq!(head, 0x90);
    ws_send(
        &mut ws,
        0x2,
        &publish311("lc/ws/xchange", 0, 0, false, b"ws-to-tcp"),
    )
    .await;
    let (head, body) = recv(&mut tcp).await;
    let got = parse_publish(head, &body);
    assert_eq!(got.payload, b"ws-to-tcp");

    let mut bad_path = dial(kernel.ws_port).await;
    let status = ws_handshake_raw(
        &mut bad_path,
        "127.0.0.1",
        kernel.ws_port,
        "/bad",
        Some("mqtt"),
    )
    .await;
    assert!(
        status.starts_with("HTTP/1.1 404"),
        "wrong path gets 404, got: {status}"
    );
    let mut bad_proto = dial(kernel.ws_port).await;
    let status = ws_handshake_raw(&mut bad_proto, "127.0.0.1", kernel.ws_port, "/mqtt", None).await;
    assert!(
        status.starts_with("HTTP/1.1 400"),
        "missing subprotocol gets 400, got: {status}"
    );
    kernel.finish().await;
}

/// A WebSocket client answers ping with pong. An idle WebSocket client
/// closes after 1.5 times its keepalive.
#[tokio::test]
async fn lc_websocket_keepalive_and_ping() {
    let kernel = spawn_kernel("ws-keep", None).await;
    let mut ws = dial(kernel.ws_port).await;
    let status =
        ws_handshake_raw(&mut ws, "127.0.0.1", kernel.ws_port, "/mqtt", Some("mqtt")).await;
    assert!(status.starts_with("HTTP/1.1 101"));
    ws_send(&mut ws, 0x2, &connect311("lc-ws-ping", true, 60)).await;
    let (opcode, _) = ws_recv(&mut ws).await;
    assert_eq!(opcode, 0x2);
    ws_send(&mut ws, 0x9, b"ping1").await;
    let (opcode, payload) = ws_recv(&mut ws).await;
    assert_eq!(opcode, 0xA);
    assert_eq!(payload, b"ping1");

    let mut idle = dial(kernel.ws_port).await;
    let status = ws_handshake_raw(
        &mut idle,
        "127.0.0.1",
        kernel.ws_port,
        "/mqtt",
        Some("mqtt"),
    )
    .await;
    assert!(status.starts_with("HTTP/1.1 101"));
    ws_send(&mut idle, 0x2, &connect311("lc-ws-idle", true, 2)).await;
    let (opcode, _) = ws_recv(&mut idle).await;
    assert_eq!(opcode, 0x2);
    let closed = tokio::time::timeout(Duration::from_secs(10), ws_recv(&mut idle)).await;
    assert!(
        closed.is_err() || matches!(&closed, Ok((0x8, _))),
        "idle WS socket closes"
    );
    kernel.finish().await;
}
