use super::*;

/// MN-12: the subscription-table quota answers per protocol version.
///
/// A version-4 SUBACK allows only 0x00-0x02 and 0x80, so a version-4
/// client refused for quota gets 0x80. A version-5 client gets 0x97.
/// One kernel serves a version-4 fill plus a version-5 probe.
#[tokio::test]
async fn mn12_subscription_quota_codes_by_protocol_version() {
    tokio::time::timeout(std::time::Duration::from_secs(120), async {
        let kernel = spawn_kernel("").await;
        let mut v4 = dial_mqtt(kernel.mqtt_port).await;
        send_mqtt(&mut v4, &mqtt_connect_packet("mn12-v4-fill")).await;
        let (head, body) = recv_mqtt(&mut v4).await;
        assert_eq!(head, 0x20, "kernel answers v4 with CONNACK");
        assert_eq!(body, vec![0x00, 0x00], "v4 CONNECT is accepted");
        for batch in 0..100u32 {
            let owned: Vec<String> = (0..1000u32).map(|n| format!("mn12/{batch}/{n}")).collect();
            let refs: Vec<(&str, u8)> = owned.iter().map(|f| (f.as_str(), 0u8)).collect();
            let packet_id = batch as u16 + 1;
            send_mqtt(&mut v4, &sy02_subscribe(packet_id, &refs)).await;
            let (head, body) = recv_mqtt(&mut v4).await;
            assert_eq!(head & 0xF0, 0x90, "kernel answers v4 with SUBACK");
            let (got_id, codes) = sy02_parse_suback(&body);
            assert_eq!(got_id, packet_id, "v4 SUBACK mirrors packet id");
            assert_eq!(codes.len(), 1000, "each SUBACK holds 1000 codes");
            assert!(
                codes.iter().all(|c| *c == 0),
                "fill batch {batch} grants every filter"
            );
        }
        send_mqtt(&mut v4, &sy02_subscribe(101, &[("mn12/over", 0)])).await;
        let (head, body) = recv_mqtt(&mut v4).await;
        assert_eq!(head & 0xF0, 0x90, "kernel answers v4 with SUBACK");
        let (_, codes) = sy02_parse_suback(&body);
        assert_eq!(codes, vec![0x80u8], "version-4 quota refusal answers 0x80");
        let mut v5 = sr01_connect5(kernel.mqtt_port, "mn12-v5-probe", None).await;
        let code = sr01_subscribe_code(&mut v5, 1, "mn12/over5").await;
        assert_eq!(code, 0x97, "version-5 quota refusal answers 0x97");
        kernel.finish().await;
    })
    .await
    .expect("mn12 quota flow completes in time");
}

/// RFC 6455 example key: the server cannot echo the right accept value
/// without computing it, so asserting the known answer verifies the
/// handshake.
const SY02_WS_KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";
const SY02_WS_ACCEPT: &str = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

/// Complete a WebSocket upgrade for MQTT against the kernel and verify
/// the 101 answer (accept key plus `mqtt` subprotocol).
async fn sy02_ws_handshake(
    sock: &mut tokio::net::TcpStream,
    host: &str,
    port: u16,
) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let request = format!(
        "GET /mqtt HTTP/1.1\r\nHost: {host}:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {SY02_WS_KEY}\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: mqtt\r\n\r\n"
    );
    sock.write_all(request.as_bytes()).await?;
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        sock.read_exact(&mut byte).await?;
        head.push(byte[0]);
        if head.len() >= 4 && head[head.len() - 4..] == *b"\r\n\r\n" {
            break;
        }
        if head.len() > 8192 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "WS handshake head too long",
            ));
        }
    }
    let head_text = String::from_utf8_lossy(&head);
    let mut lines = head_text.lines();
    let status = lines.next().unwrap_or_default();
    assert!(
        status.starts_with("HTTP/1.1 101"),
        "WS upgrade accepted, got: {status}"
    );
    let mut accept_ok = false;
    let mut proto_ok = false;
    for line in lines {
        let line = line.trim();
        if let Some((name, value)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("sec-websocket-accept") {
                accept_ok = value.trim() == SY02_WS_ACCEPT;
            } else if name.trim().eq_ignore_ascii_case("sec-websocket-protocol") {
                proto_ok = value.split(',').any(|token| token.trim() == "mqtt");
            }
        }
    }
    assert!(accept_ok, "WS accept key matches the offered key");
    assert!(proto_ok, "WS subprotocol negotiates mqtt");
    Ok(())
}

/// Send one masked client-to-server binary frame.
async fn sy02_ws_send(sock: &mut tokio::net::TcpStream, payload: &[u8]) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    // Fixed mask: RFC-compliant (present), deterministic for the harness.
    const MASK: [u8; 4] = [0x12, 0x34, 0x56, 0x78];
    let mut frame = vec![0x82];
    if payload.len() < 126 {
        frame.push(0x80 | payload.len() as u8);
    } else if payload.len() < 65536 {
        frame.push(0x80 | 126);
        frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    } else {
        frame.push(0x80 | 127);
        frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    }
    frame.extend_from_slice(&MASK);
    for (index, byte) in payload.iter().enumerate() {
        frame.push(byte ^ MASK[index % 4]);
    }
    sock.write_all(&frame).await
}

/// What one server-to-client frame read yields.
enum Sy02WsMsg {
    Binary(Vec<u8>),
    Closed,
}

/// Read server frames until a binary message or a close arrives,
/// answering pings on the way. Only used with a caller timeout.
async fn sy02_ws_recv(sock: &mut tokio::net::TcpStream) -> std::io::Result<Sy02WsMsg> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    loop {
        let mut head = [0u8; 2];
        sock.read_exact(&mut head).await?;
        let opcode = head[0] & 0x0F;
        let masked = head[1] & 0x80 != 0;
        assert!(!masked, "server-to-client frames are unmasked");
        let mut length = (head[1] & 0x7F) as u64;
        if length == 126 {
            let mut ext = [0u8; 2];
            sock.read_exact(&mut ext).await?;
            length = u16::from_be_bytes(ext) as u64;
        } else if length == 127 {
            let mut ext = [0u8; 8];
            sock.read_exact(&mut ext).await?;
            length = u64::from_be_bytes(ext);
        }
        assert!(length <= 16_777_216, "WS frame bounded");
        let mut payload = vec![0u8; length as usize];
        sock.read_exact(&mut payload).await?;
        match opcode {
            0x2 => return Ok(Sy02WsMsg::Binary(payload)),
            0x8 => return Ok(Sy02WsMsg::Closed),
            0x9 => {
                // Answer pings so an idle peer is never dropped: one
                // masked pong carrying the ping payload.
                const MASK: [u8; 4] = [0x12, 0x34, 0x56, 0x78];
                let mut frame = vec![0x8A];
                if payload.len() < 126 {
                    frame.push(0x80 | payload.len() as u8);
                } else {
                    frame.push(0x80 | 126);
                    frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
                }
                frame.extend_from_slice(&MASK);
                for (index, byte) in payload.iter().enumerate() {
                    frame.push(byte ^ MASK[index % 4]);
                }
                sock.write_all(&frame).await?;
                continue;
            }
            0xA => continue,
            other => panic!("unexpected WS opcode {other}"),
        }
    }
}

/// A last will fires on ungraceful close on TCP and on WebSocket.
///
/// A 3.1.1 subscriber watches the will topic through the kernel
/// listener. A version-5 TCP publisher connects with a will and drops
/// without DISCONNECT, and the subscriber gets the will. A version-5
/// WebSocket publisher repeats the same over the WebSocket listener,
/// and the same subscriber gets the second will.
#[tokio::test]
async fn sy02_ws_mqtt5_will_fires_to_subscriber() {
    let kernel = spawn_kernel("").await;

    let mut sub = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(&mut sub, &sy02_connect311("sy02-ws-sub", true, 60, None)).await;
    let (head, body) = recv_mqtt(&mut sub).await;
    assert_eq!(head, 0x20, "kernel answers with CONNACK");
    assert_eq!(sy02_parse_connack311(&body), (false, 0));
    send_mqtt(&mut sub, &sy02_subscribe(1, &[("sy02/ws/will", 0)])).await;
    let (head, body) = recv_mqtt(&mut sub).await;
    assert_eq!(head, 0x90, "kernel answers with SUBACK");
    assert_eq!(sy02_parse_suback(&body), (1, vec![0]));

    // TCP leg: a version-5 publisher connects with a will and drops the
    // socket with no DISCONNECT. The kernel fires the stored will once.
    let mut tcp_pub = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(
        &mut tcp_pub,
        &sy02_connect5(
            "sy02-ws-pub",
            true,
            60,
            None,
            Some(("sy02/ws/will", b"ws-will-bytes")),
            10,
        ),
    )
    .await;
    let (head, body) = recv_mqtt(&mut tcp_pub).await;
    assert_eq!(head & 0xF0, 0x20, "TCP publisher gets CONNACK");
    let (_, reason, alias_max) = sy02_parse_connack5(&body);
    assert_eq!(reason, 0, "will-carrying CONNECT is accepted");
    assert_eq!(alias_max, 10, "CONNACK carries the alias maximum");
    drop(tcp_pub);
    let (head, body) = recv_mqtt(&mut sub).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let first = sy02_parse_publish_classic(head, &body);
    assert_eq!(first.topic, "sy02/ws/will");
    assert_eq!(first.payload, b"ws-will-bytes");

    // WebSocket leg: the same handshake over the WebSocket listener.
    // The publisher completes the upgrade, connects with a will, and
    // drops the socket with no DISCONNECT and no close frame.
    let mut ws = tokio::time::timeout(
        STEP,
        tokio::net::TcpStream::connect(("127.0.0.1", kernel.ws_port)),
    )
    .await
    .expect("WS dial in time")
    .expect("WS dial");
    tokio::time::timeout(
        STEP,
        sy02_ws_handshake(&mut ws, "127.0.0.1", kernel.ws_port),
    )
    .await
    .expect("WS handshake in time")
    .expect("WS handshake");
    let connect = sy02_connect5(
        "sy02-ws5-pub",
        true,
        60,
        None,
        Some(("sy02/ws/will", b"ws5-will-bytes")),
        10,
    );
    tokio::time::timeout(STEP, sy02_ws_send(&mut ws, &connect))
        .await
        .expect("WS CONNECT in time")
        .expect("WS CONNECT send");
    let connack = tokio::time::timeout(STEP, sy02_ws_recv(&mut ws))
        .await
        .expect("WS CONNACK in time")
        .expect("WS CONNACK");
    let (present, reason, alias_max) = match connack {
        Sy02WsMsg::Binary(frame) => {
            assert_eq!(frame[0] & 0xF0, 0x20, "socket answers with CONNACK");
            let body = &frame[2..];
            sy02_parse_connack5(body)
        }
        Sy02WsMsg::Closed => panic!("socket closed on CONNECT"),
    };
    assert!(!present, "first WS bind reports session_present=false");
    assert_eq!(reason, 0, "will-carrying WS CONNECT is accepted");
    assert_eq!(alias_max, 10, "WS CONNACK carries the alias maximum");
    drop(ws);
    let (head, body) = recv_mqtt(&mut sub).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let second = sy02_parse_publish_classic(head, &body);
    assert_eq!(second.topic, "sy02/ws/will");
    assert_eq!(second.payload, b"ws5-will-bytes");

    kernel.finish().await;
}

/// X1-03: end-to-end through the kernel listener with real MQTT clients.
///
/// A version-5 subscriber connects with properties, subscribes with
/// options plus a subscription identifier, and receives a version-5
/// publish carrying the same user properties (plus the identifier) on
/// delivery; the publisher disconnects with a reason code. The same
/// scenario over version 4 still passes unchanged (proves 3.1.1 is
/// untouched). Goes through the broker (listener codec, subscribe event,
/// publish event, delivery to socket), never the store.
#[tokio::test]
async fn x103_v5_subscriptions_reasons_e2e() {
    use tokio::io::AsyncWriteExt;
    // The kernel serves the same MQTT bytes on its own listener.
    let kernel = spawn_kernel("").await;
    async fn dial(host: &str, port: u16) -> tokio::net::TcpStream {
        tokio::time::timeout(STEP, tokio::net::TcpStream::connect((host, port)))
            .await
            .expect("dial in time")
            .expect("dial")
    }
    async fn recv(sock: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
        tokio::time::timeout(STEP, read_mqtt_packet(sock))
            .await
            .expect("packet in time")
            .expect("packet")
    }
    async fn send(sock: &mut tokio::net::TcpStream, bytes: &[u8]) {
        tokio::time::timeout(STEP, sock.write_all(bytes))
            .await
            .expect("send in time")
            .expect("send");
    }
    // Version-5 leg: CONNECT with properties (alias maximum), SUBSCRIBE
    // with options plus identifier, PUBLISH with user properties,
    // DISCONNECT with a reason code.
    let mut sub = dial("127.0.0.1", kernel.mqtt_port).await;
    send(
        &mut sub,
        &sy02_connect5("x103-v5-sub", true, 60, None, None, 10),
    )
    .await;
    let (head, body) = recv(&mut sub).await;
    assert_eq!(head & 0xF0, 0x20, "kernel answers with CONNACK");
    let (_, reason, _) = sy02_parse_connack5(&body);
    assert_eq!(reason, 0, "v5 CONNECT accepted");
    // Options: QoS 0, no-local 0, rap 0, retain-handling 0.
    send(&mut sub, &sy02_subscribe_v5(7, &[("x103/v5/props", 0)], 42)).await;
    let (head, body) = recv(&mut sub).await;
    assert_eq!(head & 0xF0, 0x90, "kernel answers with SUBACK");
    let (packet_id, codes) = sy02_parse_suback5(&body);
    assert_eq!(packet_id, 7);
    assert_eq!(codes, vec![0u8], "v5 subscribe granted QoS 0");
    let mut publisher = dial("127.0.0.1", kernel.mqtt_port).await;
    send(
        &mut publisher,
        &sy02_connect5("x103-v5-pub", true, 60, None, None, 10),
    )
    .await;
    let (head, body) = recv(&mut publisher).await;
    assert_eq!(head & 0xF0, 0x20, "publisher CONNACK");
    let (_, reason, _) = sy02_parse_connack5(&body);
    assert_eq!(reason, 0);
    send(
        &mut publisher,
        &sy02_publish5_with_props(
            "x103/v5/props",
            0,
            false,
            Some(1),
            None,
            &[("k", "v")],
            b"hello-v5",
        ),
    )
    .await;
    let (head, body) = recv(&mut sub).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let got = sy02_parse_publish_v5(head, &body);
    assert_eq!(got.topic, "x103/v5/props");
    assert_eq!(got.payload, b"hello-v5");
    assert_eq!(got.qos, 0, "delivery keeps QoS 0");
    assert_eq!(got.sub_id, 42, "delivery carries the subscription id");
    assert_eq!(got.format, 1, "delivery forwards payload format");
    assert_eq!(got.expiry, 0, "delivery forwards message expiry");
    assert_eq!(
        got.users,
        vec![("k".to_string(), "v".to_string())],
        "delivery forwards user properties"
    );
    // DISCONNECT with reason 0 succeeds (clean close, no will).
    send(&mut publisher, &sy02_disconnect_v5(0)).await;
    drop(publisher);
    // A malformed option fails only its own filter: one good filter
    // plus one with reserved bits set. The good filter grants QoS 0,
    // the bad one answers 0x8F, and the good filter still delivers.
    send(
        &mut sub,
        &sy02_subscribe_v5(8, &[("x103/v5/mixed", 0), ("x103/v5/bad", 0xC1)], 0),
    )
    .await;
    let (head, body) = recv(&mut sub).await;
    assert_eq!(head & 0xF0, 0x90, "kernel answers mixed SUBACK");
    let (packet_id, codes) = sy02_parse_suback5(&body);
    assert_eq!(packet_id, 8);
    assert_eq!(
        codes,
        vec![0u8, 0x8Fu8],
        "good filter grants, bad filter fails alone"
    );
    // Version-4 leg unchanged: classic CONNECT/SUBSCRIBE/PUBLISH.
    let mut sub4 = dial("127.0.0.1", kernel.mqtt_port).await;
    send(&mut sub4, &sy02_connect311("x103-v4-sub", true, 60, None)).await;
    let (head, body) = recv(&mut sub4).await;
    assert_eq!(head, 0x20, "v4 CONNACK");
    assert_eq!(sy02_parse_connack311(&body), (false, 0));
    send(&mut sub4, &sy02_subscribe(1, &[("x103/v4/plain", 0)])).await;
    let (head, body) = recv(&mut sub4).await;
    assert_eq!(head & 0xF0, 0x90, "v4 SUBACK");
    assert_eq!(sy02_parse_suback(&body), (1, vec![0u8]));
    let mut pub4 = dial("127.0.0.1", kernel.mqtt_port).await;
    send(&mut pub4, &sy02_connect311("x103-v4-pub", true, 60, None)).await;
    let (head, body) = recv(&mut pub4).await;
    assert_eq!(head, 0x20);
    assert_eq!(sy02_parse_connack311(&body), (false, 0));
    send(
        &mut pub4,
        &sy02_publish311("x103/v4/plain", 0, 0, false, b"hello-v4"),
    )
    .await;
    let (head, body) = recv(&mut sub4).await;
    assert_eq!(head & 0xF0, 0x30, "v4 downlink is PUBLISH");
    let got4 = sy02_parse_publish_classic(head, &body);
    assert_eq!(got4.topic, "x103/v4/plain");
    assert_eq!(got4.payload, b"hello-v4");
    kernel.finish().await;
}

/// SY-02: the QoS 1 inflight window set through `indra.toml` and its
/// `INDRA_` variable is enforced on real MQTT connections.
///
/// The config file sets `session.max_qos1_inflight = 2` /
/// `session.max_qos1_spill = 2`; the environment overrides both to 1.
/// The kernel boots against that config dir with the environment set, so
/// the resolved window is 1 and spill is 1, and the kernel listener
/// serves real MQTT clients. A durable subscriber receives three
/// live QoS 1 downlinks without acking; it then drops and reconnects.
/// Only the two tracked downlinks (window oldest-first, then spill)
/// replay with DUP set — the third, delivered live once but past
/// window+spill, stays untracked and never replays. That replay count is
/// the enforcement observed on real connections, and it matches the env
/// override (1+1), not the file (2+2). Acking the replays then releases
/// the hold: a fourth publish is tracked again and replays alone after
/// the next drop, proving the broker holds the excess only until acks
/// arrive.
#[tokio::test]
async fn sy02_qos1_window_from_toml_and_env_enforced() {
    // A probe dir proves the file layer alone resolves to 2/2 through
    // the real loader. The booted kernel below carries the same section.
    let tag = format!(
        "sy02-qos1-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let probe_dir = std::env::temp_dir().join(format!("indramqtt-{tag}-probe"));
    std::fs::create_dir_all(&probe_dir).expect("create probe config dir");
    std::fs::write(
        probe_dir.join("indra.toml"),
        "[session]\nmax_qos1_inflight = 2\nmax_qos1_spill = 2\n",
    )
    .expect("write indra.toml");

    // The file layer alone resolves to 2/2 through the real loader.
    {
        use broker_config::layers::{load_layered_with_env, CliOverrides};
        let layered =
            load_layered_with_env(&probe_dir, &[], &CliOverrides::default()).expect("load file");
        assert_eq!(layered.config().session.max_qos1_inflight, 2);
        assert_eq!(layered.config().session.max_qos1_spill, 2);
    }
    let _ = std::fs::remove_dir_all(&probe_dir);

    // The environment overrides the file for the booted kernel below.
    let prev_window = std::env::var("INDRA_SESSION__MAX_QOS1_INFLIGHT").ok();
    let prev_spill = std::env::var("INDRA_SESSION__MAX_QOS1_SPILL").ok();
    std::env::set_var("INDRA_SESSION__MAX_QOS1_INFLIGHT", "1");
    std::env::set_var("INDRA_SESSION__MAX_QOS1_SPILL", "1");

    // The kernel boots with the same session section in its config dir.
    // The wait for `/healthz` inside completes the boot, so the restore
    // below cannot race the kernel read of the environment.
    let kernel = spawn_kernel("[session]\nmax_qos1_inflight = 2\nmax_qos1_spill = 2\n").await;

    std::env::remove_var("INDRA_SESSION__MAX_QOS1_INFLIGHT");
    std::env::remove_var("INDRA_SESSION__MAX_QOS1_SPILL");
    if let Some(v) = prev_window {
        std::env::set_var("INDRA_SESSION__MAX_QOS1_INFLIGHT", v);
    }
    if let Some(v) = prev_spill {
        std::env::set_var("INDRA_SESSION__MAX_QOS1_SPILL", v);
    }

    use tokio::io::AsyncWriteExt;

    // Durable subscriber over real MQTT: CONNECT (clean_start=false),
    // then a QoS 1 subscribe.
    let mut sub = tokio::time::timeout(
        STEP,
        tokio::net::TcpStream::connect(("127.0.0.1", kernel.mqtt_port)),
    )
    .await
    .expect("subscriber dial in time")
    .expect("subscriber dial");
    let connect = sy02_connect311("sy02-qos1-sub", false, 60, None);
    tokio::time::timeout(STEP, sub.write_all(&connect))
        .await
        .expect("CONNECT in time")
        .expect("CONNECT send");
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut sub))
        .await
        .expect("CONNACK in time")
        .expect("CONNACK");
    assert_eq!(head, 0x20, "kernel answers with CONNACK");
    assert_eq!(
        sy02_parse_connack311(&body),
        (false, 0),
        "first durable CONNECT accepted with session_present=false"
    );
    let subscribe = sy02_subscribe(1, &[("sy02/qos1", 1)]);
    tokio::time::timeout(STEP, sub.write_all(&subscribe))
        .await
        .expect("SUBSCRIBE in time")
        .expect("SUBSCRIBE send");
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut sub))
        .await
        .expect("SUBACK in time")
        .expect("SUBACK");
    assert_eq!(head, 0x90, "kernel answers with SUBACK");
    assert_eq!(
        sy02_parse_suback(&body),
        (1, vec![1]),
        "QoS 1 subscription granted"
    );

    // Publisher over real MQTT.
    let mut publisher = tokio::time::timeout(
        STEP,
        tokio::net::TcpStream::connect(("127.0.0.1", kernel.mqtt_port)),
    )
    .await
    .expect("publisher dial in time")
    .expect("publisher dial");
    let connect = sy02_connect311("sy02-qos1-pub", true, 60, None);
    tokio::time::timeout(STEP, publisher.write_all(&connect))
        .await
        .expect("publisher CONNECT in time")
        .expect("publisher CONNECT send");
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut publisher))
        .await
        .expect("publisher CONNACK in time")
        .expect("publisher CONNACK");
    assert_eq!(head, 0x20, "kernel answers the publisher with CONNACK");
    assert_eq!(sy02_parse_connack311(&body), (false, 0));

    // Three QoS 1 publishes; the subscriber receives all three live (live
    // delivery always goes out once) and acks nothing. The publisher
    // drains its own PUBACKs to stay in sync.
    for (seq, payload) in [(1u16, "qos1-1"), (2, "qos1-2"), (3, "qos1-3")] {
        let publish = sy02_publish311("sy02/qos1", seq, 1, false, payload.as_bytes());
        tokio::time::timeout(STEP, publisher.write_all(&publish))
            .await
            .expect("publish in time")
            .expect("publish send");
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut publisher))
            .await
            .expect("PUBACK in time")
            .expect("PUBACK");
        assert_eq!(head, 0x40, "publisher sees PUBACK");
        assert_eq!(
            body,
            vec![(seq >> 8) as u8, (seq & 0xff) as u8],
            "PUBACK mirrors its packet id"
        );
    }
    let mut live = Vec::new();
    for _ in 0..3 {
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut sub))
            .await
            .expect("live downlink in time")
            .expect("live downlink");
        assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
        let downlink = sy02_parse_publish_classic(head, &body);
        assert_eq!(downlink.qos, 1, "downlink keeps QoS 1");
        assert!(!downlink.dup, "live delivery carries no DUP");
        live.push(downlink.payload);
    }
    assert_eq!(
        live,
        vec![b"qos1-1".to_vec(), b"qos1-2".to_vec(), b"qos1-3".to_vec()],
        "all three publish live exactly once"
    );

    // Ungraceful drop of the subscriber socket, then reconnect the
    // durable session on a fresh socket.
    drop(sub);
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let mut re = tokio::time::timeout(
        STEP,
        tokio::net::TcpStream::connect(("127.0.0.1", kernel.mqtt_port)),
    )
    .await
    .expect("reconnect in time")
    .expect("reconnect");
    let connect = sy02_connect311("sy02-qos1-sub", false, 60, None);
    tokio::time::timeout(STEP, re.write_all(&connect))
        .await
        .expect("reconnect CONNECT in time")
        .expect("reconnect CONNECT send");
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut re))
        .await
        .expect("reconnect CONNACK in time")
        .expect("reconnect CONNACK");
    assert_eq!(head, 0x20, "kernel answers the reconnect with CONNACK");
    assert_eq!(
        sy02_parse_connack311(&body),
        (true, 0),
        "resumed session reports session_present=true"
    );

    // Exactly window+spill (1+1 from the env override) replay with DUP,
    // oldest-first; the third live-only delivery never replays.
    let mut replayed = Vec::new();
    let mut replay_ids = Vec::new();
    for _ in 0..2 {
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut re))
            .await
            .expect("replay in time")
            .expect("replay");
        assert_eq!(head & 0xF0, 0x30, "replay is PUBLISH");
        let downlink = sy02_parse_publish_classic(head, &body);
        assert!(downlink.dup, "replayed downlink carries DUP");
        replay_ids.push(downlink.packet_id);
        replayed.push(downlink.payload);
    }
    assert_eq!(
        replayed,
        vec![b"qos1-1".to_vec(), b"qos1-2".to_vec()],
        "window oldest-first, then spill; live-only third never replays"
    );
    let nothing =
        tokio::time::timeout(std::time::Duration::from_secs(2), read_mqtt_packet(&mut re)).await;
    assert!(
        nothing.is_err(),
        "no third replay: window+spill bound enforced"
    );

    // Acking the replays releases the hold: a fourth publish is tracked
    // again and replays alone after the next drop, proving the broker
    // holds the excess only until acks arrive.
    for packet_id in &replay_ids {
        tokio::time::timeout(STEP, re.write_all(&sy02_puback(*packet_id)))
            .await
            .expect("PUBACK in time")
            .expect("PUBACK send");
    }
    // Let the PUBACKs land before the next publish races them.
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let publish = sy02_publish311("sy02/qos1", 4, 1, false, b"qos1-4");
    tokio::time::timeout(STEP, publisher.write_all(&publish))
        .await
        .expect("fourth publish in time")
        .expect("fourth publish send");
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut publisher))
        .await
        .expect("fourth PUBACK in time")
        .expect("fourth PUBACK");
    assert_eq!(head, 0x40, "publisher sees the fourth PUBACK");
    assert_eq!(body, vec![0x00, 0x04]);
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut re))
        .await
        .expect("fourth live downlink in time")
        .expect("fourth live downlink");
    assert_eq!(head & 0xF0, 0x30, "fourth downlink is PUBLISH");
    let downlink = sy02_parse_publish_classic(head, &body);
    assert!(!downlink.dup, "live delivery carries no DUP");
    assert_eq!(downlink.payload, b"qos1-4");
    drop(re);
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let mut re2 = tokio::time::timeout(
        STEP,
        tokio::net::TcpStream::connect(("127.0.0.1", kernel.mqtt_port)),
    )
    .await
    .expect("second reconnect in time")
    .expect("second reconnect");
    let connect = sy02_connect311("sy02-qos1-sub", false, 60, None);
    tokio::time::timeout(STEP, re2.write_all(&connect))
        .await
        .expect("second reconnect CONNECT in time")
        .expect("second reconnect CONNECT send");
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut re2))
        .await
        .expect("second reconnect CONNACK in time")
        .expect("second reconnect CONNACK");
    assert_eq!(head, 0x20, "kernel answers with CONNACK");
    assert_eq!(
        sy02_parse_connack311(&body),
        (true, 0),
        "resumed session reports session_present=true"
    );
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut re2))
        .await
        .expect("fourth replay in time")
        .expect("fourth replay");
    assert_eq!(head & 0xF0, 0x30, "replay is PUBLISH");
    let downlink = sy02_parse_publish_classic(head, &body);
    assert!(downlink.dup, "replayed downlink carries DUP");
    assert_eq!(
        downlink.payload, b"qos1-4",
        "only the acked-after publish replays: acks released the hold"
    );
    let nothing = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_mqtt_packet(&mut re2),
    )
    .await;
    assert!(
        nothing.is_err(),
        "no second replay: acked entries stay released"
    );

    kernel.finish().await;
}
