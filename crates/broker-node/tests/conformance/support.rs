#![allow(dead_code)]

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Bound for one socket step. A missing packet fails instead of hanging.
pub const STEP: Duration = Duration::from_secs(10);
/// Bound for one kernel boot.
pub const BOOT: Duration = Duration::from_secs(60);
/// Quiet wait. No packet in this time means no delivery.
pub const QUIET: Duration = Duration::from_millis(1200);
/// API key for the management API. Long enough for the minimum length.
pub const API_KEY: &str = "lc-conformance-test-key-0001";

/// One booted kernel for a test.
pub struct Kernel {
    pub child: tokio::process::Child,
    pub mqtt_port: u16,
    pub ws_port: u16,
    pub api_port: u16,
    pub scratch: std::path::PathBuf,
    pub config_dir: std::path::PathBuf,
}

impl Kernel {
    /// Stops the kernel and removes its temp dirs.
    pub async fn finish(mut self) {
        let _ = self.child.kill().await;
        let _ = std::fs::remove_dir_all(&self.scratch);
        let _ = std::fs::remove_dir_all(&self.config_dir);
    }
}

/// Locates the kernel binary under test.
pub fn kernel_binary() -> std::path::PathBuf {
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
pub fn ephemeral_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("probe binds")
        .local_addr()
        .expect("probe addr")
        .port()
}

/// Unique tag for temp dirs of one test.
pub fn tag(name: &str) -> String {
    format!(
        "lc-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    )
}

/// Takes four free loopback ports. All four differ.
/// Parallel tests probe at once. Distinct ports stop self collision.
pub fn distinct_ports() -> (u16, u16, u16, u16) {
    loop {
        let ports = (
            ephemeral_port(),
            ephemeral_port(),
            ephemeral_port(),
            ephemeral_port(),
        );
        let (a, b, c, d) = ports;
        if a != b && a != c && a != d && b != c && b != d && c != d {
            return ports;
        }
    }
}

/// Boots one kernel with the kernel listener on ephemeral ports.
/// `tcp_max` sets `listeners.tcp.max_connections` when present.
pub async fn spawn_kernel(name: &str, tcp_max: Option<u32>) -> Kernel {
    boot_kernel(name, tcp_max, "").await
}

/// Boots one kernel with extra lines appended to `indra.toml`.
/// The boot path matches `spawn_kernel`; `extra_toml` carries whole
/// sections such as `[session]`. Each test boots a real kernel and
/// speaks MQTT over real TCP. No test touches the store directly.
pub async fn boot_kernel(name: &str, tcp_max: Option<u32>, extra_toml: &str) -> Kernel {
    // Retry boot when ports collide. A probe frees its port.
    // Another test sometimes binds it first. Fresh ports fix it.
    for attempt in 1..=3 {
        let run_tag = format!("{}-{attempt}", tag(name));
        let scratch = std::env::temp_dir().join(format!("indramqtt-{run_tag}-data"));
        let config_dir = std::env::temp_dir().join(format!("indramqtt-{run_tag}-cfg"));
        std::fs::create_dir_all(&config_dir).expect("create temp config dir");
        let (bl_port, mqtt_port, ws_port, api_port) = distinct_ports();
        let max_line = tcp_max.map_or(String::new(), |m| format!("max_connections = {m}\n"));
        std::fs::write(
            config_dir.join("indra.toml"),
            format!(
                "[node]\n\
                 id = \"lc-{run_tag}\"\n\
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
                 level = \"warn\"\n\
                 {extra_toml}",
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
                        return Kernel {
                            child,
                            mqtt_port,
                            ws_port,
                            api_port,
                            scratch,
                            config_dir,
                        };
                    }
                }
            }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill().await;
                panic!("kernel never opened API {api_port} and MQTT {mqtt_port}");
            }
            if let Ok(Some(status)) = child.try_wait() {
                // Clean stale dirs. Next attempt uses fresh ports.
                let _ = child.kill().await;
                let _ = std::fs::remove_dir_all(&scratch);
                let _ = std::fs::remove_dir_all(&config_dir);
                if attempt >= 3 {
                    panic!("kernel exited during boot: {status}");
                }
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    panic!("kernel never started after retries");
}

/// Opens one TCP connection to the kernel listener.
pub async fn dial(port: u16) -> tokio::net::TcpStream {
    tokio::time::timeout(STEP, tokio::net::TcpStream::connect(("127.0.0.1", port)))
        .await
        .expect("dial in time")
        .expect("dial")
}

/// Sends bytes with a bound.
pub async fn send(sock: &mut tokio::net::TcpStream, bytes: &[u8]) {
    tokio::time::timeout(STEP, sock.write_all(bytes))
        .await
        .expect("send in time")
        .expect("send");
}

/// Reads one MQTT packet: the first byte plus the body.
pub async fn recv(sock: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    tokio::time::timeout(STEP, read_mqtt_packet(sock))
        .await
        .expect("packet in time")
        .expect("packet")
}

/// Reads one MQTT packet from the socket.
pub async fn read_mqtt_packet(sock: &mut tokio::net::TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
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
pub fn rl(mut len: usize) -> Vec<u8> {
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
pub fn mqtt_str(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + s.len());
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
    out
}

/// Encodes a variable byte integer.
pub fn varint(mut value: u32) -> Vec<u8> {
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
pub fn connect311(client_id: &str, clean: bool, keepalive: u16) -> Vec<u8> {
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
pub fn connect31(client_id: &str, keepalive: u16) -> Vec<u8> {
    let mut rest = vec![0x00, 0x06, b'M', b'Q', b'I', b's', b'd', b'p', 0x03, 0x02];
    rest.extend_from_slice(&keepalive.to_be_bytes());
    rest.extend_from_slice(&mqtt_str(client_id));
    let mut packet = vec![0x10];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// Builds an MQTT 3.1.1 CONNECT with a last will.
pub fn connect311_will(
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
pub fn connect5_with_props(client_id: &str, clean: bool, keepalive: u16, props: &[u8]) -> Vec<u8> {
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
pub fn connect5(client_id: &str, clean: bool, keepalive: u16, alias_max: u16) -> Vec<u8> {
    let mut props = Vec::new();
    if alias_max != 0 {
        props.push(34);
        props.extend_from_slice(&alias_max.to_be_bytes());
    }
    connect5_with_props(client_id, clean, keepalive, &props)
}

/// Builds an MQTT 5 CONNECT with a receive maximum.
pub fn connect5_recvmax(client_id: &str, keepalive: u16, max: u16) -> Vec<u8> {
    let props = vec![33, (max >> 8) as u8, (max & 0xFF) as u8];
    connect5_with_props(client_id, true, keepalive, &props)
}

/// Builds a classic SUBSCRIBE.
pub fn subscribe(packet_id: u16, subs: &[(&str, u8)]) -> Vec<u8> {
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
pub fn subscribe_v5(packet_id: u16, subs: &[(&str, u8)], sub_id: u32) -> Vec<u8> {
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
pub fn publish311(topic: &str, packet_id: u16, qos: u8, retain: bool, payload: &[u8]) -> Vec<u8> {
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
pub fn publish5(
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

/// Builds an MQTT 5 UNSUBSCRIBE.
pub fn unsubscribe_v5(packet_id: u16, filters: &[&str]) -> Vec<u8> {
    let mut rest = Vec::new();
    rest.extend_from_slice(&packet_id.to_be_bytes());
    rest.push(0x00);
    for filter in filters {
        rest.extend_from_slice(&mqtt_str(filter));
    }
    let mut packet = vec![0xA2];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// Builds PUBACK, PUBREC, PUBREL or PUBCOMP with one packet id.
pub fn ack(kind: u8, packet_id: u16) -> Vec<u8> {
    vec![kind, 0x02, (packet_id >> 8) as u8, (packet_id & 0xFF) as u8]
}

/// Parses a 3.1.1 CONNACK body into presence plus return code.
pub fn parse_connack311(body: &[u8]) -> (bool, u8) {
    assert_eq!(body.len(), 2);
    (body[0] & 0x01 != 0, body[1])
}

/// Parses an MQTT 5 CONNACK body into presence, reason and alias maximum.
pub fn parse_connack5(body: &[u8]) -> (bool, u8, u16) {
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
pub fn connack5_has(body: &[u8], prop_id: usize) -> bool {
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

/// Gets the Maximum Packet Size of a CONNACK.
/// Returns None when the property is absent.
pub fn connack5_max_packet(body: &[u8]) -> Option<u32> {
    let (prop_len, used) = decode_varint(&body[2..]).expect("property length");
    let mut props = &body[2 + used..][..prop_len];
    while !props.is_empty() {
        let (id, used) = decode_varint(props).expect("property id");
        props = &props[used..];
        if id == 39 {
            let value = u32::from_be_bytes([props[0], props[1], props[2], props[3]]);
            return Some(value);
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
    None
}

/// Decodes one variable byte integer into value plus bytes used.
pub fn decode_varint(mut bytes: &[u8]) -> Option<(usize, usize)> {
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
pub struct Publish {
    pub topic: String,
    pub packet_id: u16,
    pub qos: u8,
    pub retain: bool,
    pub dup: bool,
    pub payload: Vec<u8>,
}

/// Parses a classic PUBLISH delivery.
pub fn parse_publish(header: u8, body: &[u8]) -> Publish {
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
pub struct Publish5 {
    pub topic: String,
    pub packet_id: u16,
    pub dup: bool,
    pub sub_id: u32,
    pub alias: u16,
    pub payload: Vec<u8>,
}

/// Parses an MQTT 5 PUBLISH delivery.
pub fn parse_publish5(header: u8, body: &[u8]) -> Publish5 {
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
pub async fn connect_client(
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
