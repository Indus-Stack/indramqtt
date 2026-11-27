use super::*;

#[derive(Debug, Default)]
pub(crate) struct TestBrokerSink {
    pub(crate) published: Mutex<Vec<(Topic, Bytes, QoS, bool)>>,
}

#[async_trait::async_trait]
impl BrokerSink for TestBrokerSink {
    async fn publish(
        &self,
        topic: Topic,
        payload: Bytes,
        qos: QoS,
        retain: bool,
    ) -> Result<(), broker_rules::RuleEngineError> {
        self.published
            .lock()
            .unwrap()
            .push((topic, payload, qos, retain));
        Ok(())
    }
}

/// Minimal MQTT 3.1.1 CONNECT packet for `client_id` (clean session,
/// keepalive 60): fixed header plus the `MQTT`/4 variable header and
/// the id payload. Enough for the kernel listener to accept the client.
pub(crate) fn mqtt_connect_packet(client_id: &str) -> Vec<u8> {
    let mut rest = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3C];
    rest.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    rest.extend_from_slice(client_id.as_bytes());
    let mut packet = vec![0x10];
    let mut len = rest.len();
    loop {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        packet.push(byte);
        if len == 0 {
            break;
        }
    }
    packet.extend_from_slice(&rest);
    packet
}

/// Read one MQTT control packet: returns the fixed-header first byte
/// and the remaining body. Only used with an explicit caller timeout.
pub(crate) async fn read_mqtt_packet(
    sock: &mut tokio::net::TcpStream,
) -> std::io::Result<(u8, Vec<u8>)> {
    read_mqtt_packet_from(sock).await
}

/// Read one MQTT control packet from any async reader.
pub(crate) async fn read_mqtt_packet_from<R>(reader: &mut R) -> std::io::Result<(u8, Vec<u8>)>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;
    let mut head = [0u8; 1];
    reader.read_exact(&mut head).await?;
    let mut multiplier = 1usize;
    let mut remaining = 0usize;
    loop {
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte).await?;
        remaining += ((byte[0] & 0x7F) as usize) * multiplier;
        if byte[0] & 0x80 == 0 {
            break;
        }
        multiplier *= 128;
        if multiplier > 128 * 128 * 128 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "MQTT remaining length overflow",
            ));
        }
    }
    let mut body = vec![0u8; remaining];
    reader.read_exact(&mut body).await?;
    Ok((head[0], body))
}

/// Dials the kernel MQTT listener with a bound.
pub(crate) async fn dial_mqtt(port: u16) -> tokio::net::TcpStream {
    tokio::time::timeout(STEP, tokio::net::TcpStream::connect(("127.0.0.1", port)))
        .await
        .expect("dial in time")
        .expect("dial")
}

/// Sends bytes with a bound.
pub(crate) async fn send_mqtt(sock: &mut tokio::net::TcpStream, bytes: &[u8]) {
    use tokio::io::AsyncWriteExt;
    tokio::time::timeout(STEP, sock.write_all(bytes))
        .await
        .expect("send in time")
        .expect("send");
}

/// Reads one MQTT packet with a bound.
pub(crate) async fn recv_mqtt(sock: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    tokio::time::timeout(STEP, read_mqtt_packet(sock))
        .await
        .expect("packet in time")
        .expect("packet")
}

/// Open one MQTT 5 client on the booted kernel and require an accepted
/// CONNECT.
pub(crate) async fn sr01_connect5(
    port: u16,
    client_id: &str,
    creds: Option<(&str, &[u8])>,
) -> tokio::net::TcpStream {
    let mut sock = dial_mqtt(port).await;
    send_mqtt(
        &mut sock,
        &sy02_connect5(client_id, true, 60, creds, None, 0),
    )
    .await;
    let (head, body) = recv_mqtt(&mut sock).await;
    assert_eq!(head & 0xF0, 0x20, "kernel answers with CONNACK");
    let (_, reason, _) = sy02_parse_connack5(&body);
    assert_eq!(reason, 0, "CONNECT is accepted for {client_id}");
    sock
}

/// Send one v5 SUBSCRIBE and return the first granted code.
pub(crate) async fn sr01_subscribe_code(
    sock: &mut tokio::net::TcpStream,
    packet_id: u16,
    filter: &str,
) -> u8 {
    send_mqtt(sock, &sy02_subscribe_v5(packet_id, &[(filter, 0)], 0)).await;
    let (head, body) = recv_mqtt(sock).await;
    assert_eq!(head & 0xF0, 0x90, "kernel answers with SUBACK");
    let (_, codes) = sy02_parse_suback5(&body);
    assert_eq!(codes.len(), 1, "one filter gets one code");
    codes[0]
}

/// Locate the real kernel binary for the spawned-kernel tests.
/// `CARGO_BIN_EXE_indramqtt` is set when Cargo builds the test target;
/// the probe fallback covers binaries built by an earlier explicit
/// `cargo build -p broker-node` under either target directory.
pub(crate) fn shard_kernel_binary() -> std::path::PathBuf {
    if let Some(built) = option_env!("CARGO_BIN_EXE_indramqtt") {
        return std::path::PathBuf::from(built);
    }
    let exe = if cfg!(windows) {
        "indramqtt.exe"
    } else {
        "indramqtt"
    };
    let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for candidate in [
        manifest.join(format!("../target/debug/{exe}")),
        manifest.join(format!("../target/ci-check/debug/{exe}")),
    ] {
        if candidate.is_file() {
            return candidate;
        }
    }
    panic!(
        "kernel binary not found: run `cargo build -p broker-node` first \
         (CARGO_BIN_EXE_indramqtt unset and no binary under target/debug or target/ci-check/debug)"
    );
}

/// Encode an MQTT remaining length (variable-byte integer).
pub(crate) fn sy02_mqtt_rl(mut len: usize) -> Vec<u8> {
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

/// Encode an MQTT UTF-8 string (`len:16be | bytes`).
pub(crate) fn sy02_mqtt_str(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + s.len());
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
    out
}

/// Minimal MQTT 3.1.1 CONNECT with optional credentials.
pub(crate) fn sy02_connect311(
    client_id: &str,
    clean_start: bool,
    keepalive: u16,
    creds: Option<(&str, &[u8])>,
) -> Vec<u8> {
    let mut rest = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04];
    let mut flags = 0u8;
    if clean_start {
        flags |= 0x02;
    }
    if let Some((_, pass)) = creds {
        flags |= 0x80;
        if !pass.is_empty() {
            flags |= 0x40;
        }
    }
    rest.push(flags);
    rest.extend_from_slice(&keepalive.to_be_bytes());
    rest.extend_from_slice(&sy02_mqtt_str(client_id));
    if let Some((user, pass)) = creds {
        rest.extend_from_slice(&sy02_mqtt_str(user));
        rest.extend_from_slice(&(pass.len() as u16).to_be_bytes());
        rest.extend_from_slice(pass);
    }
    let mut packet = vec![0x10];
    packet.extend_from_slice(&sy02_mqtt_rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// MQTT 5 CONNECT with optional credentials, an optional last will and
/// the client's Topic Alias Maximum (property 34, the kernel outbound
/// bound; 0 sends an empty property section).
pub(crate) fn sy02_connect5(
    client_id: &str,
    clean_start: bool,
    keepalive: u16,
    creds: Option<(&str, &[u8])>,
    will: Option<(&str, &[u8])>,
    alias_max: u16,
) -> Vec<u8> {
    let mut rest = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05];
    let mut flags = 0u8;
    if creds.is_some() {
        // The username and password fields follow below. An empty
        // password still travels as a zero-length field with its flag
        // set, so the packet stays well formed.
        flags |= 0xC0;
    }
    if will.is_some() {
        flags |= 0x04;
    }
    if clean_start {
        flags |= 0x02;
    }
    rest.push(flags);
    rest.extend_from_slice(&keepalive.to_be_bytes());
    let mut props: Vec<u8> = Vec::new();
    if !clean_start {
        // MQTT 5: a session stays after the disconnect only if the client
        // sends a Session Expiry Interval (property 17). A client that
        // resumes its session asks for one hour.
        props.push(17);
        props.extend_from_slice(&3600u32.to_be_bytes());
    }
    if alias_max != 0 {
        props.push(34);
        props.extend_from_slice(&alias_max.to_be_bytes());
    }
    rest.push(props.len() as u8);
    rest.extend_from_slice(&props);
    rest.extend_from_slice(&sy02_mqtt_str(client_id));
    if will.is_some() {
        // Empty will-property section, then topic and payload.
        rest.push(0x00);
    }
    if let Some((will_topic, will_payload)) = will {
        rest.extend_from_slice(&sy02_mqtt_str(will_topic));
        rest.extend_from_slice(&(will_payload.len() as u16).to_be_bytes());
        rest.extend_from_slice(will_payload);
    }
    if let Some((user, pass)) = creds {
        rest.extend_from_slice(&sy02_mqtt_str(user));
        rest.extend_from_slice(&(pass.len() as u16).to_be_bytes());
        rest.extend_from_slice(pass);
    }
    let mut packet = vec![0x10];
    packet.extend_from_slice(&sy02_mqtt_rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// Classic SUBSCRIBE (`packet id | filter | qos ...`): the shape the
/// kernel decodes on every session, 3.1.1 and 5 alike.
pub(crate) fn sy02_subscribe(packet_id: u16, subs: &[(&str, u8)]) -> Vec<u8> {
    let mut rest = Vec::new();
    rest.extend_from_slice(&packet_id.to_be_bytes());
    for (filter, qos) in subs {
        rest.extend_from_slice(&sy02_mqtt_str(filter));
        rest.push(*qos);
    }
    let mut packet = vec![0x82];
    packet.extend_from_slice(&sy02_mqtt_rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// MQTT 3.1.1 PUBLISH (client to kernel).
pub(crate) fn sy02_publish311(
    topic: &str,
    packet_id: u16,
    qos: u8,
    retain: bool,
    payload: &[u8],
) -> Vec<u8> {
    let mut rest = sy02_mqtt_str(topic);
    if qos > 0 {
        rest.extend_from_slice(&packet_id.to_be_bytes());
    }
    rest.extend_from_slice(payload);
    let mut flags = qos << 1;
    if retain {
        flags |= 0x01;
    }
    let mut packet = vec![0x30 | flags];
    packet.extend_from_slice(&sy02_mqtt_rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// MQTT 5 PUBLISH (client to kernel): `topic | packet id | properties |
/// payload`. `Some(alias)` registers the alias (property 35);
/// `None` sends an empty property section.
pub(crate) fn sy02_publish5(
    topic: &str,
    packet_id: u16,
    qos: u8,
    retain: bool,
    alias: Option<u16>,
    payload: &[u8],
) -> Vec<u8> {
    let mut rest = sy02_mqtt_str(topic);
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
    let mut flags = qos << 1;
    if retain {
        flags |= 0x01;
    }
    let mut packet = vec![0x30 | flags];
    packet.extend_from_slice(&sy02_mqtt_rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// Two-byte PUBACK (packet id only): the shape clients send on every
/// session, 3.1.1 and 5 alike.
pub(crate) fn sy02_puback(packet_id: u16) -> Vec<u8> {
    vec![0x40, 0x02, (packet_id >> 8) as u8, (packet_id & 0xff) as u8]
}

/// Encode a variable-byte integer (MQTT property lengths and ids).
pub(crate) fn sy02_encode_varint(mut value: u32) -> Vec<u8> {
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

/// MQTT 5 SUBSCRIBE with per-filter options plus the packet-wide
/// subscription identifier (X1-03): `packet id | props | filters`.
/// `sub_id` 0 sends an empty property section.
pub(crate) fn sy02_subscribe_v5(packet_id: u16, subs: &[(&str, u8)], sub_id: u32) -> Vec<u8> {
    let mut props = Vec::new();
    if sub_id != 0 {
        props.push(11u8);
        props.extend_from_slice(&sy02_encode_varint(sub_id));
    }
    let mut rest = Vec::new();
    rest.extend_from_slice(&packet_id.to_be_bytes());
    rest.extend_from_slice(&sy02_encode_varint(props.len() as u32));
    rest.extend_from_slice(&props);
    for (filter, opts) in subs {
        rest.extend_from_slice(&sy02_mqtt_str(filter));
        rest.push(*opts);
    }
    let mut packet = vec![0x82];
    packet.extend_from_slice(&sy02_mqtt_rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// MQTT 5 PUBLISH with forwarded properties (X1-03): payload format,
/// message expiry and user properties ride the property section.
/// `format`/`expiry` `None` omits the property; empty `users` omits it.
pub(crate) fn sy02_publish5_with_props(
    topic: &str,
    qos: u8,
    retain: bool,
    format: Option<u8>,
    expiry: Option<u32>,
    users: &[(&str, &str)],
    payload: &[u8],
) -> Vec<u8> {
    let mut props = Vec::new();
    if let Some(f) = format {
        props.push(1u8);
        props.push(f);
    }
    if let Some(e) = expiry {
        props.push(2u8);
        props.extend_from_slice(&e.to_be_bytes());
    }
    for (k, v) in users {
        props.push(38u8);
        props.extend_from_slice(&sy02_mqtt_str(k));
        props.extend_from_slice(&sy02_mqtt_str(v));
    }
    let mut rest = sy02_mqtt_str(topic);
    // QoS 0 in this test: no packet id.
    assert_eq!(qos, 0, "X1-03 test uses QoS 0");
    rest.extend_from_slice(&sy02_encode_varint(props.len() as u32));
    rest.extend_from_slice(&props);
    rest.extend_from_slice(payload);
    let mut flags = qos << 1;
    if retain {
        flags |= 0x01;
    }
    let mut packet = vec![0x30 | flags];
    packet.extend_from_slice(&sy02_mqtt_rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// MQTT 5 DISCONNECT with a reason code (X1-03): `reason | props(empty)`.
pub(crate) fn sy02_disconnect_v5(reason: u8) -> Vec<u8> {
    let body = vec![reason, 0x00];
    let mut packet = vec![0xE0];
    packet.extend_from_slice(&sy02_mqtt_rl(body.len()));
    packet.extend_from_slice(&body);
    packet
}

/// Parse an MQTT 5 SUBACK body into `(packet_id, granted codes)`,
/// skipping the property section.
pub(crate) fn sy02_parse_suback5(body: &[u8]) -> (u16, Vec<u8>) {
    assert!(body.len() >= 3, "v5 SUBACK carries packet id and props");
    let packet_id = u16::from_be_bytes([body[0], body[1]]);
    let (prop_len, used) = sy02_varint(&body[2..]).expect("SUBACK property length");
    assert!(body.len() >= 2 + used + prop_len, "SUBACK props fit");
    (packet_id, body[2 + used + prop_len..].to_vec())
}

/// One MQTT 5 PUBLISH as the kernel delivers it, with forwarded props.
pub(crate) struct Sy02Publish5 {
    pub(crate) topic: String,
    pub(crate) qos: u8,
    pub(crate) sub_id: u32,
    pub(crate) format: u8,
    pub(crate) expiry: u32,
    pub(crate) users: Vec<(String, String)>,
    pub(crate) payload: Vec<u8>,
}

/// Parse an MQTT 5 PUBLISH body (QoS 0): topic, property section
/// (subscription id 11, format 1, expiry 2, user 38, alias 35 skipped)
/// and payload.
pub(crate) fn sy02_parse_publish_v5(header: u8, body: &[u8]) -> Sy02Publish5 {
    let qos = (header >> 1) & 0x03;
    assert_eq!(qos, 0, "X1-03 test uses QoS 0");
    assert!(body.len() >= 2, "PUBLISH carries a topic");
    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    let topic = std::str::from_utf8(&body[2..2 + topic_len])
        .expect("topic UTF-8")
        .to_string();
    let mut rest = &body[2 + topic_len..];
    let (prop_len, used) = sy02_varint(rest).expect("PUBLISH property length");
    rest = &rest[used..];
    assert!(rest.len() >= prop_len, "PUBLISH props fit");
    let mut props = &rest[..prop_len];
    let mut sub_id = 0u32;
    let mut format = 0u8;
    let mut expiry = 0u32;
    let mut users = Vec::new();
    while !props.is_empty() {
        let (id, used) = sy02_varint(props).expect("property id");
        props = &props[used..];
        match id {
            11 => {
                let (v, used) = sy02_varint(props).expect("sub id varint");
                sub_id = v as u32;
                props = &props[used..];
            }
            1 => {
                assert!(!props.is_empty(), "format carries a byte");
                format = props[0];
                props = &props[1..];
            }
            2 => {
                assert!(props.len() >= 4, "expiry carries a u32");
                expiry = u32::from_be_bytes([props[0], props[1], props[2], props[3]]);
                props = &props[4..];
            }
            35 => {
                assert!(props.len() >= 2, "alias carries a u16");
                props = &props[2..];
            }
            38 => {
                assert!(props.len() >= 2, "user key length");
                let k_len = u16::from_be_bytes([props[0], props[1]]) as usize;
                assert!(props.len() >= 2 + k_len + 2, "user key fits");
                let k = std::str::from_utf8(&props[2..2 + k_len])
                    .expect("user key UTF-8")
                    .to_string();
                let v_len = u16::from_be_bytes([props[2 + k_len], props[2 + k_len + 1]]) as usize;
                assert!(props.len() >= 2 + k_len + 2 + v_len, "user value fits");
                let v = std::str::from_utf8(&props[2 + k_len + 2..2 + k_len + 2 + v_len])
                    .expect("user value UTF-8")
                    .to_string();
                users.push((k, v));
                props = &props[2 + k_len + 2 + v_len..];
            }
            _ => panic!("unexpected v5 PUBLISH property {id}"),
        }
    }
    Sy02Publish5 {
        topic,
        qos,
        sub_id,
        format,
        expiry,
        users,
        payload: rest[prop_len..].to_vec(),
    }
}

/// Decode one variable-byte integer, returning `(value, bytes used)`.
pub(crate) fn sy02_varint(mut bytes: &[u8]) -> Option<(usize, usize)> {
    let mut value = 0usize;
    let mut used = 0usize;
    loop {
        let byte = *bytes.first()?;
        bytes = &bytes[1..];
        used += 1;
        value += ((byte & 0x7F) as usize) * (128usize.pow(used as u32 - 1));
        if byte & 0x80 == 0 {
            return Some((value, used));
        }
        if used == 4 {
            return None;
        }
    }
}

/// Parse a 3.1.1 CONNACK body into `(session_present, return_code)`.
pub(crate) fn sy02_parse_connack311(body: &[u8]) -> (bool, u8) {
    assert_eq!(body.len(), 2, "3.1.1 CONNACK body is two bytes");
    (body[0] & 0x01 != 0, body[1])
}

/// Parse an MQTT 5 CONNACK body into `(session_present, reason, alias_max)`.
/// Scans the property section for the Topic Alias Maximum (34) while
/// accepting every other property the kernel may advertise on the live
/// handshake path (X1-02): session expiry (17, u32), assigned client
/// id (18, UTF-8), server receive maximum (33, u16), server maximum
/// packet size (39, u32), reason string (31, UTF-8) and user
/// properties (38, UTF-8 pair). A strict match on 34 alone rejects the
/// negotiated limits the kernel advertises on every v5 CONNACK, so the
/// parser records the alias maximum and skips the rest instead.
pub(crate) fn sy02_parse_connack5(body: &[u8]) -> (bool, u8, u16) {
    assert!(
        body.len() >= 3,
        "MQTT 5 CONNACK carries flags, reason and properties"
    );
    let present = body[0] & 0x01 != 0;
    let reason = body[1];
    let (prop_len, used) = sy02_varint(&body[2..]).expect("CONNACK property length");
    let mut props = &body[2 + used..];
    assert!(props.len() >= prop_len, "CONNACK properties fit the body");
    props = &props[..prop_len];
    // Skip one length-prefixed UTF-8 string, returning the remainder.
    fn skip_utf8<'a>(buf: &'a [u8], what: &str) -> &'a [u8] {
        assert!(buf.len() >= 2, "{what} carries a length");
        let len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
        assert!(buf.len() >= 2 + len, "{what} fits the body");
        &buf[2 + len..]
    }
    let mut alias_max = 0u16;
    while !props.is_empty() {
        let (id, used) = sy02_varint(props).expect("CONNACK property id");
        props = &props[used..];
        match id {
            17 => {
                assert!(props.len() >= 4, "session-expiry property carries a u32");
                props = &props[4..];
            }
            18 => {
                props = skip_utf8(props, "assigned-client-id property");
            }
            33 => {
                assert!(props.len() >= 2, "receive-maximum property carries a u16");
                props = &props[2..];
            }
            34 => {
                assert!(props.len() >= 2, "alias-maximum property carries a u16");
                alias_max = u16::from_be_bytes([props[0], props[1]]);
                props = &props[2..];
            }
            28 | 31 => {
                props = skip_utf8(props, "reason-string property");
            }
            38 => {
                props = skip_utf8(props, "user-property key");
                props = skip_utf8(props, "user-property value");
            }
            39 => {
                assert!(
                    props.len() >= 4,
                    "maximum-packet-size property carries a u32"
                );
                props = &props[4..];
            }
            _ => panic!("unexpected CONNACK property {id}"),
        }
    }
    (present, reason, alias_max)
}

/// Parse a classic SUBACK body into `(packet_id, granted codes)`.
pub(crate) fn sy02_parse_suback(body: &[u8]) -> (u16, Vec<u8>) {
    assert!(body.len() >= 3, "SUBACK carries a packet id plus codes");
    (u16::from_be_bytes([body[0], body[1]]), body[2..].to_vec())
}

/// One PUBLISH as the kernel delivers it to a subscribing client.
pub(crate) struct Sy02Publish {
    pub(crate) topic: String,
    pub(crate) packet_id: u16,
    pub(crate) qos: u8,
    pub(crate) retain: bool,
    pub(crate) dup: bool,
    pub(crate) alias: u16,
    pub(crate) payload: Vec<u8>,
}

/// Parse a classic-shape PUBLISH (no property section): live retained
/// replays, offline replays and every delivery toward a client that
/// negotiated no outbound alias.
pub(crate) fn sy02_parse_publish_classic(header: u8, body: &[u8]) -> Sy02Publish {
    let dup = header & 0x08 != 0;
    let qos = (header >> 1) & 0x03;
    let retain = header & 0x01 != 0;
    assert!(body.len() >= 2, "PUBLISH carries a topic");
    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    assert!(body.len() >= 2 + topic_len, "PUBLISH topic fits the body");
    let topic = std::str::from_utf8(&body[2..2 + topic_len])
        .expect("PUBLISH topic UTF-8")
        .to_string();
    let mut rest = &body[2 + topic_len..];
    let packet_id = if qos > 0 {
        assert!(rest.len() >= 2, "PUBLISH carries a packet id");
        let id = u16::from_be_bytes([rest[0], rest[1]]);
        rest = &rest[2..];
        id
    } else {
        0
    };
    Sy02Publish {
        topic,
        packet_id,
        qos,
        retain,
        dup,
        alias: 0,
        payload: rest.to_vec(),
    }
}

/// Parse an MQTT 5 PUBLISH: the property length is always there, and
/// the alias property is there when the kernel assigned an alias
/// (`alias` is 0 when it is absent).
pub(crate) fn sy02_parse_publish_alias(header: u8, body: &[u8]) -> Sy02Publish {
    let dup = header & 0x08 != 0;
    let qos = (header >> 1) & 0x03;
    let retain = header & 0x01 != 0;
    assert!(body.len() >= 3, "PUBLISH carries a topic and properties");
    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    assert!(body.len() > 2 + topic_len, "PUBLISH topic fits the body");
    let topic = std::str::from_utf8(&body[2..2 + topic_len])
        .expect("PUBLISH topic UTF-8")
        .to_string();
    let mut rest = &body[2 + topic_len..];
    let packet_id = if qos > 0 {
        assert!(rest.len() >= 2, "PUBLISH carries a packet id");
        let id = u16::from_be_bytes([rest[0], rest[1]]);
        rest = &rest[2..];
        id
    } else {
        0
    };
    let (prop_len, used) = sy02_varint(rest).expect("PUBLISH property length");
    rest = &rest[used..];
    assert!(rest.len() >= prop_len, "PUBLISH properties fit the body");
    let mut props = &rest[..prop_len];
    let mut alias = 0u16;
    while !props.is_empty() {
        let (id, used) = sy02_varint(props).expect("PUBLISH property id");
        props = &props[used..];
        match id {
            35 => {
                assert!(props.len() >= 2, "alias property carries a u16");
                alias = u16::from_be_bytes([props[0], props[1]]);
                props = &props[2..];
            }
            _ => panic!("unexpected PUBLISH property {id}"),
        }
    }
    Sy02Publish {
        topic,
        packet_id,
        qos,
        retain,
        dup,
        alias,
        payload: rest[prop_len..].to_vec(),
    }
}

/// Send a management request to a real broker and retain its JSON order.
pub(crate) async fn mv00_api(
    kernel: &Kernel,
    method: reqwest::Method,
    path: &str,
    body: serde_json::Value,
) -> (u16, serde_json::Value) {
    let response = tokio::time::timeout(
        STEP,
        reqwest::Client::new()
            .request(
                method,
                format!("http://127.0.0.1:{}{path}", kernel.api_port),
            )
            .bearer_auth(API_KEY)
            .json(&body)
            .send(),
    )
    .await
    .expect("management request in time")
    .expect("management request");
    let status = response.status().as_u16();
    let text = response.text().await.expect("management response body");
    let value = if text.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(&text).expect("management JSON")
    };
    (status, value)
}

pub(crate) async fn mv00_connect(kernel: &Kernel, id: &str) -> tokio::net::TcpStream {
    let mut socket = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(&mut socket, &mqtt_connect_packet(id)).await;
    let (head, body) = recv_mqtt(&mut socket).await;
    assert_eq!(head, 0x20);
    assert_eq!(sy02_parse_connack311(&body), (false, 0));
    socket
}

pub(crate) async fn mv00_subscribe(socket: &mut tokio::net::TcpStream) {
    send_mqtt(socket, &sy02_subscribe(1, &[("mv00/out/#", 0)])).await;
    let (head, body) = recv_mqtt(socket).await;
    assert_eq!(head, 0x90);
    assert_eq!(sy02_parse_suback(&body), (1, vec![0]));
}

pub(crate) fn mv00_rule(id: &str, sql: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "name": id,
        "sql": sql,
        "actions": [{"function": "republish", "args": {"topic": format!("mv00/out/{id}")}}],
    })
}

pub(crate) async fn mv00_publish_outputs(
    publisher: &mut tokio::net::TcpStream,
    subscriber: &mut tokio::net::TcpStream,
    id: &str,
    input: &str,
    count: usize,
) -> Vec<Vec<u8>> {
    let mut messages = Vec::new();
    send_mqtt(
        publisher,
        &sy02_publish311(&format!("mv00/in/{id}"), 0, 0, false, input.as_bytes()),
    )
    .await;
    for _ in 0..count {
        let (head, body) = recv_mqtt(subscriber).await;
        assert_eq!(head & 0xf0, 0x30, "rule output is PUBLISH");
        let message = sy02_parse_publish_classic(head, &body);
        assert_eq!(message.topic, format!("mv00/out/{id}"));
        messages.push(message.payload);
    }
    messages
}

pub(crate) async fn mv00_assert_publish(
    publisher: &mut tokio::net::TcpStream,
    subscriber: &mut tokio::net::TcpStream,
    id: &str,
    input: &str,
    expected: &[&str],
) {
    let messages = mv00_publish_outputs(publisher, subscriber, id, input, expected.len()).await;
    let want: Vec<&[u8]> = expected.iter().map(|row| row.as_bytes()).collect();
    assert_eq!(messages, want, "rule {id} output bytes");
}

/// Reboot from the same durable state and the same listener configuration.
pub(crate) async fn mv00_restart(kernel: &mut Kernel) {
    kernel
        .child
        .kill()
        .await
        .expect("stop broker before restart");
    // Wait for the old broker to exit so its listeners release
    // before the new broker binds the same ports. Reason: an
    // immediate rebind can fail with EADDRINUSE under parallel load.
    let _ = kernel.child.wait().await;
    let mut attempt = 0;
    loop {
        attempt += 1;
        kernel.child = tokio::process::Command::new(shard_kernel_binary())
            .arg("--config-dir")
            .arg(&kernel.config_dir)
            .arg("--allow-anonymous")
            .env("INDRA_API_KEYS", API_KEY)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("restart broker");
        let deadline = std::time::Instant::now() + BOOT;
        let mut exited = false;
        while std::time::Instant::now() < deadline {
            if let Ok(Ok(response)) = tokio::time::timeout(
                STEP,
                reqwest::Client::new()
                    .get(format!("http://127.0.0.1:{}/healthz", kernel.api_port))
                    .send(),
            )
            .await
            {
                if response.text().await.expect("health body") == "OK"
                    && tokio::net::TcpStream::connect(("127.0.0.1", kernel.mqtt_port))
                        .await
                        .is_ok()
                {
                    return;
                }
            }
            if let Ok(Some(_)) = kernel.child.try_wait() {
                exited = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        if exited && attempt < 3 {
            let _ = kernel.child.wait().await;
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            continue;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "restart opens listeners in time"
        );
        assert!(
            kernel.child.try_wait().expect("restart status").is_none(),
            "restarted broker stays running"
        );
        break;
    }
}

/// Accept `expected` HTTP requests on the shared listener and return each
/// (request path, request body) pair. One serve per POST.
pub(crate) fn mv01_spawn_http_receiver(
    listener: Arc<tokio::net::TcpListener>,
    expected: usize,
) -> tokio::task::JoinHandle<Vec<(String, Vec<u8>)>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    tokio::spawn(async move {
        let mut recorded = Vec::with_capacity(expected);
        for _ in 0..expected {
            // A health probe opens and closes a bare TCP connection. Skip
            // it; only a real HTTP request is a delivery.
            let (mut socket, _) = loop {
                let (socket, _) = listener.accept().await.expect("HTTP request accepted");
                let mut probe = [0_u8; 1];
                match socket.peek(&mut probe).await {
                    Ok(0) | Err(_) => continue,
                    Ok(_) => break (socket, ()),
                }
            };
            let mut request = Vec::new();
            loop {
                let mut byte = [0_u8; 1];
                socket
                    .read_exact(&mut byte)
                    .await
                    .expect("HTTP header byte");
                request.push(byte[0]);
                assert!(request.len() <= 16384, "bounded HTTP header");
                if request.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            let headers = String::from_utf8(request).expect("HTTP headers");
            let length: usize = headers
                .lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().expect("HTTP length"))
                })
                .expect("HTTP content length");
            assert!(length <= 4096, "bounded body");
            let mut body = vec![0_u8; length];
            socket.read_exact(&mut body).await.expect("HTTP body");
            let path = headers
                .lines()
                .next()
                .expect("request line")
                .split_whitespace()
                .nth(1)
                .expect("request path")
                .to_string();
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .await
                .expect("HTTP response");
            recorded.push((path, body));
        }
        recorded
    })
}

/// Requests and peak active count from the concurrent HTTP receiver.
pub(crate) type Mv01ActiveCapture = (Vec<(String, Vec<u8>)>, usize);

/// Serve HTTP requests and record the peak number of requests handled at
/// one time. Each response waits `hold`, so a concurrent client shows a
/// peak above one. Returns the requests and the peak.
pub(crate) fn mv01_spawn_http_receiver_active(
    listener: Arc<tokio::net::TcpListener>,
    expected: usize,
    hold: std::time::Duration,
) -> tokio::task::JoinHandle<Mv01ActiveCapture> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    tokio::spawn(async move {
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let recorded = Arc::new(Mutex::new(Vec::with_capacity(expected)));
        let mut handlers = Vec::with_capacity(expected);
        for _ in 0..expected {
            // A health probe opens and closes a bare TCP connection. Skip
            // it; only a real HTTP request is a delivery.
            let (mut socket, _) = loop {
                let (socket, _) = listener.accept().await.expect("HTTP request accepted");
                let mut probe = [0_u8; 1];
                match socket.peek(&mut probe).await {
                    Ok(0) | Err(_) => continue,
                    Ok(_) => break (socket, ()),
                }
            };
            let current = active.fetch_add(1, Ordering::SeqCst) + 1;
            max_active.fetch_max(current, Ordering::SeqCst);
            let active = Arc::clone(&active);
            let recorded = Arc::clone(&recorded);
            handlers.push(tokio::spawn(async move {
                let mut request = Vec::new();
                loop {
                    let mut byte = [0_u8; 1];
                    socket
                        .read_exact(&mut byte)
                        .await
                        .expect("HTTP header byte");
                    request.push(byte[0]);
                    assert!(request.len() <= 16384, "bounded HTTP header");
                    if request.ends_with(b"\r\n\r\n") {
                        break;
                    }
                }
                let headers = String::from_utf8(request).expect("HTTP headers");
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().expect("HTTP length"))
                    })
                    .expect("HTTP content length");
                assert!(length <= 4096, "bounded body");
                let mut body = vec![0_u8; length];
                socket.read_exact(&mut body).await.expect("HTTP body");
                let path = headers
                    .lines()
                    .next()
                    .expect("request line")
                    .split_whitespace()
                    .nth(1)
                    .expect("request path")
                    .to_string();
                tokio::time::sleep(hold).await;
                socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    )
                    .await
                    .expect("HTTP response");
                active.fetch_sub(1, Ordering::SeqCst);
                recorded
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push((path, body));
            }));
        }
        for handler in handlers {
            handler.await.expect("HTTP handler task");
        }
        let recorded = Arc::try_unwrap(recorded)
            .expect("sole owner")
            .into_inner()
            .expect("capture lock");
        (recorded, max_active.load(Ordering::SeqCst))
    })
}

/// Read one PostgreSQL message (tag plus body) from the fake server stream.
pub(crate) async fn mv01_pg_read_msg(stream: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    use tokio::io::AsyncReadExt;
    let mut header = [0_u8; 5];
    stream.read_exact(&mut header).await.expect("pg head");
    let len = i32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
    let mut body = vec![0_u8; len - 4];
    stream.read_exact(&mut body).await.expect("pg body");
    (header[0], body)
}

/// Write one PostgreSQL message to the fake server stream.
pub(crate) async fn mv01_pg_write_msg(stream: &mut tokio::net::TcpStream, tag: u8, body: &[u8]) {
    use tokio::io::AsyncWriteExt;
    let mut frame = vec![tag];
    frame.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    frame.extend_from_slice(body);
    stream.write_all(&frame).await.expect("pg write");
}

/// Read the startup parameter block and return the (key, value) pairs.
pub(crate) async fn mv01_pg_read_startup(
    stream: &mut tokio::net::TcpStream,
) -> Vec<(String, String)> {
    use tokio::io::AsyncReadExt;
    let mut len_buf = [0_u8; 4];
    stream.read_exact(&mut len_buf).await.expect("startup len");
    let len = i32::from_be_bytes(len_buf) as usize;
    let mut body = vec![0_u8; len - 4];
    stream.read_exact(&mut body).await.expect("startup body");
    mv01_pg_startup_params(&body)
}

/// Parse one startup body (protocol version plus parameters).
pub(crate) fn mv01_pg_startup_params(body: &[u8]) -> Vec<(String, String)> {
    assert_eq!(
        &body[..4],
        &196608_i32.to_be_bytes(),
        "startup protocol 3.0"
    );
    let mut params = Vec::new();
    let mut parts = body[4..].split(|b| *b == 0);
    while let (Some(key), Some(value)) = (parts.next(), parts.next()) {
        if key.is_empty() {
            break;
        }
        params.push((
            String::from_utf8_lossy(key).into_owned(),
            String::from_utf8_lossy(value).into_owned(),
        ));
    }
    params
}

pub(crate) async fn mv01_pg_ready(stream: &mut tokio::net::TcpStream) {
    mv01_pg_write_msg(stream, b'Z', b"I").await;
}

/// One captured engine row per statement: the bound statement plus the
/// rows the sink flushed (each row: topic, QoS, payload).
pub(crate) type PgCaptured = Arc<Mutex<Vec<(String, Vec<Vec<Vec<u8>>>)>>>;

/// Accept one real protocol connection. A reachability probe connects
/// and closes without a byte; skip it so the fake serves the driver.
pub(crate) async fn mv01_pg_accept(listener: &tokio::net::TcpListener) -> tokio::net::TcpStream {
    loop {
        let (stream, _) = listener.accept().await.expect("pg backend accepted");
        let mut probe = [0_u8; 8];
        match stream.peek(&mut probe).await {
            Ok(0) | Err(_) => continue,
            Ok(_) => return stream,
        }
    }
}

/// One fake backend. It checks the password, then speaks the extended
/// query protocol that the maintained driver sends: Parse, Describe,
/// Sync for a prepared statement, then Bind, Execute, Sync for each row.
/// Every row of one connection is captured under its statement.
pub(crate) async fn mv01_pg_serve_one(
    mut stream: tokio::net::TcpStream,
    captured: PgCaptured,
    expected_user: &str,
    expected_password: &str,
    expected_database: &str,
    expected_rows: usize,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut probe = [0_u8; 8];
    if stream.read_exact(&mut probe).await.is_err() {
        return;
    }
    // A real server accepts both an SSLRequest and a direct plaintext
    // startup message. The maintained driver sends a startup message
    // directly when TLS is off.
    let params = if probe[4..8] == 80877103_i32.to_be_bytes() {
        stream.write_all(b"N").await.expect("ssl deny");
        mv01_pg_read_startup(&mut stream).await
    } else {
        let len = i32::from_be_bytes([probe[0], probe[1], probe[2], probe[3]]) as usize;
        let mut body = probe[4..8].to_vec();
        let rest = len.saturating_sub(8);
        let mut tail = vec![0_u8; rest];
        stream.read_exact(&mut tail).await.expect("startup body");
        body.extend_from_slice(&tail);
        mv01_pg_startup_params(&body)
    };
    // The connection settings come from the connector document alone.
    assert!(
        params
            .iter()
            .any(|(k, v)| k == "user" && v == expected_user),
        "the fake checks the user: {params:?}"
    );
    assert!(
        params
            .iter()
            .any(|(k, v)| k == "database" && v == expected_database),
        "the fake checks the database: {params:?}"
    );
    // Cleartext password authentication: the driver sends the password as
    // a PasswordMessage. The fake verifies it before it answers OK.
    mv01_pg_write_msg(&mut stream, b'R', &3_i32.to_be_bytes()).await;
    let (tag, body) = mv01_pg_read_msg(&mut stream).await;
    assert_eq!(tag, b'p', "the driver answers the password request");
    let sent_password = body
        .strip_suffix(&[0])
        .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
        .unwrap_or_default();
    assert_eq!(
        sent_password, expected_password,
        "the fake checks the password"
    );
    mv01_pg_write_msg(&mut stream, b'R', &0_i32.to_be_bytes()).await;
    mv01_pg_ready(&mut stream).await;
    let mut statement = String::new();
    let mut rows = Vec::new();
    loop {
        let mut header = [0_u8; 5];
        if stream.read_exact(&mut header).await.is_err() {
            break;
        }
        let len = i32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
        let mut body = vec![0_u8; len - 4];
        stream.read_exact(&mut body).await.expect("pg body");
        match header[0] {
            b'P' => {
                let mut cursor = &body[..];
                let take_cstr = |cur: &mut &[u8]| {
                    let end = cur.iter().position(|&b| b == 0).expect("cstr");
                    let text = String::from_utf8_lossy(&cur[..end]).into_owned();
                    *cur = &cur[end + 1..];
                    text
                };
                let name = take_cstr(&mut cursor);
                statement = format!("{name}|{}", take_cstr(&mut cursor));
                mv01_pg_write_msg(&mut stream, b'1', &[]).await;
            }
            b'D' => {
                if body.first() == Some(&b'S') {
                    // ParameterDescription: three text parameters (oid 25).
                    let mut types = Vec::new();
                    types.extend_from_slice(&3_u16.to_be_bytes());
                    types.extend_from_slice(&25_i32.to_be_bytes());
                    types.extend_from_slice(&25_i32.to_be_bytes());
                    types.extend_from_slice(&25_i32.to_be_bytes());
                    mv01_pg_write_msg(&mut stream, b't', &types).await;
                }
                // An INSERT returns no rows; NoData ends the description.
                mv01_pg_write_msg(&mut stream, b'n', &[]).await;
            }
            b'S' => {
                mv01_pg_ready(&mut stream).await;
                if !rows.is_empty() && rows.len() >= expected_rows {
                    break;
                }
            }
            b'B' => {
                let mut cursor = &body[..];
                let skip_cstr = |cursor: &mut &[u8]| {
                    let end = cursor.iter().position(|&b| b == 0).expect("cstr");
                    *cursor = &cursor[end + 1..];
                };
                skip_cstr(&mut cursor);
                skip_cstr(&mut cursor);
                let formats = u16::from_be_bytes([cursor[0], cursor[1]]) as usize;
                cursor = &cursor[2 + formats * 2..];
                let count = u16::from_be_bytes([cursor[0], cursor[1]]) as usize;
                cursor = &cursor[2..];
                let mut row = Vec::new();
                for _ in 0..count {
                    let len = i32::from_be_bytes([cursor[0], cursor[1], cursor[2], cursor[3]]);
                    cursor = &cursor[4..];
                    row.push(cursor[..len as usize].to_vec());
                    cursor = &cursor[len as usize..];
                }
                rows.push(row);
                mv01_pg_write_msg(&mut stream, b'2', &[]).await;
            }
            b'E' => mv01_pg_write_msg(&mut stream, b'C', b"INSERT 0 1\0").await,
            b'X' => break,
            _ => {}
        }
    }
    captured
        .lock()
        .expect("capture lock")
        .push((statement, rows));
}

/// Loopback MQTT broker for the source ingress tests. It accepts a
/// CONNECT, answers a SUBSCRIBE, and forwards a test message to each
/// connection that holds the matching subscription. Drop stops the
/// accept task and every connection task.
pub(crate) struct FakeMqttSource {
    pub(crate) port: u16,
    pub(crate) active: Arc<AtomicUsize>,
    pub(crate) subscriptions: Arc<Mutex<Vec<String>>>,
    pub(crate) written: Arc<AtomicUsize>,
    pub(crate) broadcasts: tokio::sync::broadcast::Sender<(String, Vec<Vec<u8>>)>,
    pub(crate) connections: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    pub(crate) task: tokio::task::JoinHandle<()>,
}

impl FakeMqttSource {
    pub(crate) async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake source broker");
        let port = listener.local_addr().expect("fake address").port();
        let active = Arc::new(AtomicUsize::new(0));
        let subscriptions = Arc::new(Mutex::new(Vec::new()));
        let written = Arc::new(AtomicUsize::new(0));
        let connections: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>> =
            Arc::new(Mutex::new(Vec::new()));
        let (broadcasts, _) = tokio::sync::broadcast::channel::<(String, Vec<Vec<u8>>)>(64);
        let task = {
            let active = Arc::clone(&active);
            let subscriptions = Arc::clone(&subscriptions);
            let written = Arc::clone(&written);
            let connections = Arc::clone(&connections);
            let broadcasts = broadcasts.clone();
            tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        break;
                    };
                    active.fetch_add(1, Ordering::SeqCst);
                    let active = Arc::clone(&active);
                    let subscriptions = Arc::clone(&subscriptions);
                    let written = Arc::clone(&written);
                    let mut receiver = broadcasts.subscribe();
                    let handle = tokio::spawn(async move {
                        use tokio::io::AsyncWriteExt;
                        let (mut reader, mut writer) = stream.into_split();
                        let mut subscription: Option<(String, u8)> = None;
                        let mut next_packet_id: u16 = 1;
                        loop {
                            tokio::select! {
                                packet = read_mqtt_packet_from(&mut reader) => {
                                    let Ok((head, body)) = packet else {
                                        break;
                                    };
                                    match head >> 4 {
                                        1 => {
                                            if writer.write_all(&[0x20, 0x02, 0x00, 0x00]).await.is_err() {
                                                break;
                                            }
                                        }
                                        8 => {
                                            if body.len() < 4 {
                                                break;
                                            }
                                            let len = u16::from_be_bytes([body[2], body[3]]) as usize;
                                            if body.len() < 5 + len {
                                                break;
                                            }
                                            let topic = String::from_utf8_lossy(&body[4..4 + len]).to_string();
                                            let qos = body[4 + len] & 0x03;
                                            subscription = Some((topic.clone(), qos));
                                            subscriptions.lock().unwrap().push(topic);
                                            let ack = [0x90, 0x03, body[0], body[1], qos];
                                            if writer.write_all(&ack).await.is_err() {
                                                break;
                                            }
                                        }
                                        12 if writer.write_all(&[0xD0, 0x00]).await.is_err() => break,
                                        _ => {}
                                    }
                                }
                                received = receiver.recv() => {
                                    if let Ok((topic, payloads)) = received {
                                        if let Some((subscribed, sub_qos)) = subscription.as_ref() {
                                            if subscribed == &topic {
                                                let mut batch = Vec::new();
                                                for payload in &payloads {
                                                    let qos = (*sub_qos).min(1);
                                                    let packet = sy02_publish311(
                                                        &topic,
                                                        next_packet_id,
                                                        qos,
                                                        false,
                                                        payload,
                                                    );
                                                    next_packet_id = next_packet_id.wrapping_add(1).max(1);
                                                    batch.extend_from_slice(&packet);
                                                }
                                                if writer.write_all(&batch).await.is_err() {
                                                    break;
                                                }
                                                written.fetch_add(payloads.len(), Ordering::SeqCst);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        active.fetch_sub(1, Ordering::SeqCst);
                    });
                    connections.lock().unwrap().push(handle);
                }
            })
        };
        Self {
            port,
            active,
            subscriptions,
            broadcasts,
            written,
            connections,
            task,
        }
    }

    pub(crate) fn publish(&self, topic: &str, payload: &[u8]) {
        let _ = self
            .broadcasts
            .send((topic.to_string(), vec![payload.to_vec()]));
    }

    pub(crate) fn publish_burst(&self, topic: &str, payloads: &[Vec<u8>]) {
        let _ = self.broadcasts.send((topic.to_string(), payloads.to_vec()));
    }

    pub(crate) fn active(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    pub(crate) fn written(&self) -> usize {
        self.written.load(Ordering::SeqCst)
    }

    pub(crate) fn subscription_count(&self) -> usize {
        self.subscriptions.lock().unwrap().len()
    }
}

impl Drop for FakeMqttSource {
    fn drop(&mut self) {
        self.task.abort();
        for handle in self.connections.lock().unwrap().drain(..) {
            handle.abort();
        }
    }
}

/// Loopback endpoint that closes each accepted connection. The source
/// driver then fails and retries after the configured interval, so the
/// accept count shows which interval the runtime applied.
pub(crate) struct ClosingMqttSource {
    pub(crate) port: u16,
    pub(crate) accepts: Arc<AtomicUsize>,
    pub(crate) task: tokio::task::JoinHandle<()>,
}

impl ClosingMqttSource {
    pub(crate) async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind closing source broker");
        let port = listener.local_addr().expect("closing address").port();
        let accepts = Arc::new(AtomicUsize::new(0));
        let task = {
            let accepts = Arc::clone(&accepts);
            tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        break;
                    };
                    accepts.fetch_add(1, Ordering::SeqCst);
                    drop(stream);
                }
            })
        };
        Self {
            port,
            accepts,
            task,
        }
    }

    pub(crate) fn accepts(&self) -> usize {
        self.accepts.load(Ordering::SeqCst)
    }
}

impl Drop for ClosingMqttSource {
    fn drop(&mut self) {
        self.task.abort();
    }
}
