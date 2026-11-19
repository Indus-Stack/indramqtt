use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::jwks_test_support;

/// Bound for one socket step. Slow CI runner margin for single TCP round trip.
pub const STEP: Duration = Duration::from_secs(10);
/// Bound for one kernel boot. Kernel bootstrap margin on cold runner.
pub const BOOT: Duration = Duration::from_secs(60);
/// Timeout for an individual HTTP health-check probe during boot.
pub const HEALTH_POLL_TIMEOUT: Duration = Duration::from_secs(2);
/// Sleep interval between health-check polls while waiting for boot.
pub const BOOT_POLL_INTERVAL: Duration = Duration::from_millis(200);
/// API key for the management API.
pub const API_KEY: &str = "pv14-reauth-test-key-0001";
/// SCRAM password of the default administrator.
pub const SCRAM_USER: &str = "admin";
pub const SCRAM_PASSWORD: &str = "public";

pub struct Kernel {
    pub child: tokio::process::Child,
    pub mqtt_port: u16,
    pub api_port: u16,
    pub scratch: std::path::PathBuf,
    pub config_dir: std::path::PathBuf,
}

impl Kernel {
    pub async fn finish(mut self) {
        let _ = self.child.kill().await;
        let _ = std::fs::remove_dir_all(&self.scratch);
        let _ = std::fs::remove_dir_all(&self.config_dir);
    }
}

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

pub fn ephemeral_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("probe binds")
        .local_addr()
        .expect("probe addr")
        .port()
}

pub fn tag(name: &str) -> String {
    format!(
        "pv14-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    )
}

pub async fn boot_kernel(name: &str, extra_toml: &str) -> Kernel {
    for attempt in 1..=3 {
        let run_tag = format!("{}-{attempt}", tag(name));
        let scratch = std::env::temp_dir().join(format!("indramqtt-{run_tag}-data"));
        let config_dir = std::env::temp_dir().join(format!("indramqtt-{run_tag}-cfg"));
        std::fs::create_dir_all(&config_dir).expect("create temp config dir");
        let bl_port = ephemeral_port();
        let mqtt_port = ephemeral_port();
        let api_port = ephemeral_port();
        std::fs::write(
            config_dir.join("indra.toml"),
            format!(
                "[node]\n\
                 id = \"pv14-{run_tag}\"\n\
                 data_dir = \"{}\"\n\
                 brokerlink_bind = \"127.0.0.1:{bl_port}\"\n\
                 \n\
                 [listeners.tcp]\n\
                 bind = \"127.0.0.1:{mqtt_port}\"\n\
                 native = true\n\
                 \n\
                 [listeners.tls]\n\
                 enabled = false\n\
                 \n\
                 [listeners.ws]\n\
                 enabled = false\n\
                 \n\
                 [listeners.wss]\n\
                 enabled = false\n\
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
                HEALTH_POLL_TIMEOUT,
                http.get(format!("{base}/healthz")).send(),
            )
            .await;
            if let Ok(Ok(res)) = up {
                if let Ok(text) = res.text().await {
                    if text == "OK"
                        && tokio::net::TcpStream::connect(("127.0.0.1", mqtt_port))
                            .await
                            .is_ok()
                    {
                        return Kernel {
                            child,
                            mqtt_port,
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
                let _ = child.kill().await;
                let _ = std::fs::remove_dir_all(&scratch);
                let _ = std::fs::remove_dir_all(&config_dir);
                if attempt >= 3 {
                    panic!("kernel exited during boot: {status}");
                }
                break;
            }
            tokio::time::sleep(BOOT_POLL_INTERVAL).await;
        }
    }
    panic!("kernel never started after retries");
}

pub async fn dial(port: u16) -> tokio::net::TcpStream {
    tokio::time::timeout(STEP, tokio::net::TcpStream::connect(("127.0.0.1", port)))
        .await
        .expect("dial in time")
        .expect("dial")
}

pub async fn send(sock: &mut tokio::net::TcpStream, bytes: &[u8]) {
    tokio::time::timeout(STEP, sock.write_all(bytes))
        .await
        .expect("send in time")
        .expect("send");
}

pub async fn recv(sock: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    tokio::time::timeout(STEP, read_packet(sock))
        .await
        .expect("packet in time")
        .expect("packet")
}

pub async fn read_packet(sock: &mut tokio::net::TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
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

pub fn mqtt_str(value: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + value.len());
    out.extend_from_slice(&(value.len() as u16).to_be_bytes());
    out.extend_from_slice(value.as_bytes());
    out
}

pub fn binary(value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + value.len());
    out.extend_from_slice(&(value.len() as u16).to_be_bytes());
    out.extend_from_slice(value);
    out
}

pub fn auth_method_prop(method: &str) -> Vec<u8> {
    let mut props = vec![21];
    props.extend_from_slice(&mqtt_str(method));
    props
}

pub fn connect5_full(
    client_id: &str,
    clean_start: bool,
    method: Option<&str>,
    auth_data: Option<&[u8]>,
    session_expiry: Option<u32>,
    will: Option<(&str, &[u8], u32)>,
) -> Vec<u8> {
    let mut props = Vec::new();
    if let Some(method) = method {
        props.extend_from_slice(&auth_method_prop(method));
    }
    if let Some(data) = auth_data {
        props.push(22);
        props.extend_from_slice(&binary(data));
    }
    if let Some(exp) = session_expiry {
        props.push(17);
        props.extend_from_slice(&exp.to_be_bytes());
    }
    let mut flags = if clean_start { 0x02u8 } else { 0x00u8 };
    if will.is_some() {
        flags |= 0x04;
    }
    let mut rest = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, flags];
    rest.extend_from_slice(&60u16.to_be_bytes());
    rest.extend_from_slice(&varint(props.len() as u32));
    rest.extend_from_slice(&props);
    rest.extend_from_slice(&mqtt_str(client_id));
    if let Some((will_topic, will_payload, will_delay)) = will {
        let mut will_props = Vec::new();
        if will_delay > 0 {
            will_props.push(24);
            will_props.extend_from_slice(&will_delay.to_be_bytes());
        }
        rest.extend_from_slice(&varint(will_props.len() as u32));
        rest.extend_from_slice(&will_props);
        rest.extend_from_slice(&mqtt_str(will_topic));
        rest.extend_from_slice(&binary(will_payload));
    }
    let mut packet = vec![0x10];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

pub fn connect5_with_credentials(
    client_id: &str,
    clean_start: bool,
    username: Option<&str>,
    password: Option<&[u8]>,
) -> Vec<u8> {
    let mut flags = if clean_start { 0x02u8 } else { 0x00u8 };
    if username.is_some() {
        flags |= 0x80;
    }
    if password.is_some() {
        flags |= 0x40;
    }
    let mut rest = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, flags];
    rest.extend_from_slice(&60u16.to_be_bytes());
    rest.push(0);
    rest.extend_from_slice(&mqtt_str(client_id));
    if let Some(user) = username {
        rest.extend_from_slice(&mqtt_str(user));
    }
    if let Some(pwd) = password {
        rest.extend_from_slice(&binary(pwd));
    }
    let mut packet = vec![0x10];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

pub fn auth_packet(reason: u8, method: Option<&str>, data: Option<&[u8]>) -> Vec<u8> {
    let mut props = Vec::new();
    if let Some(method) = method {
        props.extend_from_slice(&auth_method_prop(method));
    }
    if let Some(data) = data {
        props.push(22);
        props.extend_from_slice(&binary(data));
    }
    let mut rest = vec![reason];
    rest.extend_from_slice(&varint(props.len() as u32));
    rest.extend_from_slice(&props);
    let mut packet = vec![0xF0];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

pub fn subscribe_v5(packet_id: u16, filter: &str) -> Vec<u8> {
    let mut rest = packet_id.to_be_bytes().to_vec();
    rest.push(0);
    rest.extend_from_slice(&mqtt_str(filter));
    rest.push(0);
    let mut packet = vec![0x82];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

pub fn parse_auth(body: &[u8]) -> (u8, Option<String>, Option<Vec<u8>>) {
    if body.is_empty() {
        return (0, None, None);
    }
    let reason = body[0];
    if body.len() < 2 {
        return (reason, None, None);
    }
    let mut index = 1usize;
    let mut multiplier = 1usize;
    let mut length = 0usize;
    loop {
        let byte = body[index];
        index += 1;
        length += ((byte & 0x7F) as usize) * multiplier;
        if byte & 0x80 == 0 {
            break;
        }
        multiplier *= 128;
    }
    let end = index + length;
    let mut method = None;
    let mut data = None;
    while index < end {
        let id = body[index];
        index += 1;
        match id {
            21 => {
                let len = u16::from_be_bytes([body[index], body[index + 1]]) as usize;
                index += 2;
                method = Some(String::from_utf8(body[index..index + len].to_vec()).expect("UTF-8"));
                index += len;
            }
            22 => {
                let len = u16::from_be_bytes([body[index], body[index + 1]]) as usize;
                index += 2;
                data = Some(body[index..index + len].to_vec());
                index += len;
            }
            _ => panic!("unexpected AUTH property {id}"),
        }
    }
    (reason, method, data)
}

pub fn parse_publish_payload(head: u8, body: &[u8]) -> Vec<u8> {
    assert_eq!(head & 0xF0, 0x30, "packet is PUBLISH");
    let qos = (head >> 1) & 0x03;
    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    let mut rest = &body[2 + topic_len..];
    if qos > 0 {
        rest = &rest[2..];
    }
    let mut multiplier = 1usize;
    let mut prop_len = 0usize;
    let mut offset = 0usize;
    loop {
        let byte = rest[offset];
        offset += 1;
        prop_len += ((byte & 0x7F) as usize) * multiplier;
        if byte & 0x80 == 0 {
            break;
        }
        multiplier *= 128;
    }
    rest[offset + prop_len..].to_vec()
}

pub async fn expect_disconnect(sock: &mut tokio::net::TcpStream, reason: u8) {
    let (head, body) = recv(sock).await;
    assert_eq!(head & 0xF0, 0xE0, "kernel sends DISCONNECT");
    assert_eq!(body.first().copied().unwrap_or(0), reason, "reason code");
    let mut byte = [0u8; 1];
    let read = tokio::time::timeout(STEP, sock.read(&mut byte))
        .await
        .expect("close in time")
        .expect("close read");
    assert_eq!(read, 0, "kernel closes the socket");
}

pub fn scram_param(message: &str, name: &str) -> String {
    message
        .split(',')
        .find_map(|part| {
            let (key, value) = part.split_once('=')?;
            (key == name).then(|| value.to_string())
        })
        .unwrap_or_default()
}

pub fn scram_b64_decode(text: &str) -> Vec<u8> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(text)
        .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(text))
        .expect("base64")
}

pub fn scram_b64_encode(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

pub fn scram_hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, key);
    let tag = ring::hmac::sign(&key, data);
    let mut out = [0u8; 32];
    out.copy_from_slice(tag.as_ref());
    out
}

pub fn scram_client_final(client_first: &str, server_first: &str, password: &str) -> String {
    use std::num::NonZeroU32;
    let client_nonce = scram_param(client_first, "r");
    let salt = scram_b64_decode(&scram_param(server_first, "s"));
    let iterations: u32 = scram_param(server_first, "i").parse().expect("iterations");
    let combined = scram_param(server_first, "r");
    let mut salted = [0u8; 32];
    ring::pbkdf2::derive(
        ring::pbkdf2::PBKDF2_HMAC_SHA256,
        NonZeroU32::new(iterations).expect("nonzero"),
        &salt,
        password.as_bytes(),
        &mut salted,
    );
    let client_key = scram_hmac(&salted, b"Client Key");
    let stored_key = ring::digest::digest(&ring::digest::SHA256, &client_key);
    let bare_first = match client_first.find("n=") {
        Some(index) => &client_first[index..],
        None => client_first,
    };
    let without_proof = format!("c=biws,r={combined}");
    let auth_message = format!("{bare_first},{server_first},{without_proof}");
    let client_signature = scram_hmac(stored_key.as_ref(), auth_message.as_bytes());
    let mut proof = [0u8; 32];
    for index in 0..32 {
        proof[index] = client_key[index] ^ client_signature[index];
    }
    assert!(
        combined.starts_with(&client_nonce),
        "server combines both nonces"
    );
    format!("{without_proof},p={}", scram_b64_encode(&proof))
}

pub fn mint_custom_jwt(
    signing: &jwks_test_support::TestSigningKey,
    issuer: &str,
    audience: &str,
    exp_offset_secs: i64,
    claims_extra: serde_json::Value,
) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock reads")
        .as_secs() as i64;
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
    header.kid = Some(signing.kid.clone());
    let key = jsonwebtoken::EncodingKey::from_rsa_der(&signing.private_der);
    let mut claims = serde_json::json!({
        "sub": "device-1",
        "iss": issuer,
        "aud": audience,
        "iat": now,
        "exp": now + exp_offset_secs,
    });
    if let (Some(map), Some(extra)) = (claims.as_object_mut(), claims_extra.as_object()) {
        for (k, v) in extra {
            map.insert(k.clone(), v.clone());
        }
    }
    jsonwebtoken::encode(&header, &claims, &key).expect("token mints")
}

pub async fn connect_scram_ok(
    port: u16,
    client_id: &str,
    password: &str,
    will: Option<(&str, &[u8], u32)>,
) -> tokio::net::TcpStream {
    let mut sock = dial(port).await;
    let client_nonce = "rOprNGfwEbeRWgbNEkqO";
    let client_first = format!("n={SCRAM_USER},r={client_nonce}");
    send(
        &mut sock,
        &connect5_full(
            client_id,
            true,
            Some("SCRAM-SHA-256"),
            Some(client_first.as_bytes()),
            None,
            will,
        ),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0xF0, "initial SCRAM sends AUTH continue");
    let (reason, method, data) = parse_auth(&body);
    assert_eq!(reason, 0x18, "server-first uses continue");
    assert_eq!(method.as_deref(), Some("SCRAM-SHA-256"));
    let server_first = String::from_utf8(data.expect("server-first data")).expect("UTF-8");

    let client_final = scram_client_final(&client_first, &server_first, password);
    send(
        &mut sock,
        &auth_packet(0x18, Some("SCRAM-SHA-256"), Some(client_final.as_bytes())),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0xF0, "server sends AUTH success");
    let (reason, _method, data) = parse_auth(&body);
    assert_eq!(reason, 0x00, "SCRAM initial auth succeeds");
    let server_final = String::from_utf8(data.expect("server-final data")).expect("UTF-8");
    assert!(server_final.starts_with("v="));

    let (c_head, c_body) = recv(&mut sock).await;
    assert_eq!(c_head & 0xF0, 0x20, "server sends CONNACK");
    assert_eq!(
        c_body.get(1).copied().unwrap_or(0xFF),
        0x00,
        "CONNACK success"
    );
    sock
}

pub async fn connect_jwt_ok(
    port: u16,
    client_id: &str,
    token: &str,
    will: Option<(&str, &[u8], u32)>,
) -> tokio::net::TcpStream {
    let mut sock = dial(port).await;
    send(
        &mut sock,
        &connect5_full(
            client_id,
            true,
            Some("JWT"),
            Some(token.as_bytes()),
            None,
            will,
        ),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x20, "kernel answers with CONNACK");
    assert_eq!(
        body.get(1).copied().unwrap_or(0xFF),
        0x00,
        "CONNECT is accepted"
    );
    sock
}
