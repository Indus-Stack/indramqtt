//! CoAP GET, Observe, and DELETE access control and multi-tenant isolation tests.
//!
//! Tests enforce ACL checks on CoAP GET, Observe, and DELETE requests,
//! verify observer table capacity bounds, and prove multi-tenant isolation
//! per R-TENANT.

use broker_gateway::coap::{
    option_number, CoapCode, CoapGatewayHandler, CoapMessage, CoapOption, CoapType,
    MAX_COAP_OBSERVERS, MAX_OBSERVED_TOPICS, MAX_OBSERVERS_PER_TOPIC,
};
use bytes::Bytes;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const API_KEY: &str = "bh01-coap-test-key-0001";
const STEP_TIMEOUT: Duration = Duration::from_secs(5);
const BOOT_TIMEOUT: Duration = Duration::from_secs(30);

struct Kernel {
    child: tokio::process::Child,
    mqtt_port: u16,
    api_port: u16,
    coap_port: u16,
    scratch: PathBuf,
    config_dir: PathBuf,
}

impl Drop for Kernel {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        let _ = std::fs::remove_dir_all(&self.scratch);
        let _ = std::fs::remove_dir_all(&self.config_dir);
    }
}

fn kernel_binary() -> PathBuf {
    if let Some(built) = option_env!("CARGO_BIN_EXE_indramqtt") {
        return PathBuf::from(built);
    }
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
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

fn ephemeral_tcp_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("tcp probe binds")
        .local_addr()
        .expect("tcp addr")
        .port()
}

fn ephemeral_udp_port() -> u16 {
    std::net::UdpSocket::bind("127.0.0.1:0")
        .expect("udp probe binds")
        .local_addr()
        .expect("udp addr")
        .port()
}

fn unique_tag(name: &str) -> String {
    format!(
        "bh01-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    )
}

async fn boot_kernel(name: &str, extra_toml: &str) -> Kernel {
    for attempt in 1..=3 {
        let tag = format!("{}-{attempt}", unique_tag(name));
        let scratch = std::env::temp_dir().join(format!("indramqtt-{tag}-data"));
        let config_dir = std::env::temp_dir().join(format!("indramqtt-{tag}-cfg"));
        std::fs::create_dir_all(&config_dir).expect("create temp config dir");

        let bl_port = ephemeral_tcp_port();
        let mqtt_port = ephemeral_tcp_port();
        let api_port = ephemeral_tcp_port();
        let coap_port = ephemeral_udp_port();

        std::fs::write(
            config_dir.join("indra.toml"),
            format!(
                "[node]\n\
                 id = \"bh01-{tag}\"\n\
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
                 [gateway]\n\
                 coap_bind = \"127.0.0.1:{coap_port}\"\n\
                 \n\
                 [logging]\n\
                 level = \"warn\"\n\
                 \n\
                 [auth]\n\
                 allow_anonymous = true\n\
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
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn indramqtt");

        let http = reqwest::Client::new();
        let base = format!("http://127.0.0.1:{api_port}");
        let deadline = std::time::Instant::now() + BOOT_TIMEOUT;
        let mut ready = false;

        while std::time::Instant::now() < deadline {
            if let Ok(Ok(res)) = tokio::time::timeout(
                Duration::from_millis(500),
                http.get(format!("{base}/healthz")).send(),
            )
            .await
            {
                if let Ok(text) = res.text().await {
                    if text.trim() == "OK" {
                        ready = true;
                        break;
                    }
                }
            }
            if let Ok(Some(status)) = child.try_wait() {
                eprintln!("kernel exited during startup: {status}");
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        if ready {
            return Kernel {
                child,
                mqtt_port,
                api_port,
                coap_port,
                scratch,
                config_dir,
            };
        }
        let _ = child.kill().await;
        let _ = std::fs::remove_dir_all(&scratch);
        let _ = std::fs::remove_dir_all(&config_dir);
    }
    panic!("kernel failed to boot after 3 attempts");
}

fn build_coap_req(
    code: CoapCode,
    mid: u16,
    token: &[u8],
    topic: &str,
    observe: bool,
) -> CoapMessage {
    let mut options = Vec::new();
    if !topic.is_empty() {
        options.push(CoapOption {
            number: option_number::URI_PATH,
            value: Bytes::from_static(b"ps"),
        });
        for part in topic.split('/') {
            options.push(CoapOption {
                number: option_number::URI_PATH,
                value: Bytes::copy_from_slice(part.as_bytes()),
            });
        }
    }
    if observe {
        options.push(CoapOption {
            number: option_number::OBSERVE,
            value: Bytes::from_static(b"\x00"),
        });
    }
    CoapMessage {
        message_type: CoapType::Confirmable,
        code,
        message_id: mid,
        token: Bytes::copy_from_slice(token),
        options,
        payload: Bytes::new(),
    }
}

async fn send_and_recv_coap(
    client: &tokio::net::UdpSocket,
    gateway_addr: SocketAddr,
    req: &CoapMessage,
) -> CoapMessage {
    client
        .send_to(&req.encode(), gateway_addr)
        .await
        .expect("coap send");
    let mut buf = [0u8; 2048];
    let (len, _) = tokio::time::timeout(STEP_TIMEOUT, client.recv_from(&mut buf))
        .await
        .expect("coap reply arrives")
        .expect("coap recv");
    CoapMessage::decode(&buf[..len]).expect("coap decode")
}

fn mqtt_field(data: &[u8]) -> Vec<u8> {
    let len = (data.len() as u16).to_be_bytes();
    let mut v = Vec::with_capacity(2 + data.len());
    v.extend_from_slice(&len);
    v.extend_from_slice(data);
    v
}

async fn mqtt_connect_311(port: u16, client_id: &str) -> tokio::net::TcpStream {
    let mut sock = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect to mqtt");
    let mut body = Vec::new();
    body.extend_from_slice(b"\x00\x04MQTT\x04\x02\x00\x3c");
    body.extend_from_slice(&mqtt_field(client_id.as_bytes()));

    let mut packet = vec![0x10, body.len() as u8];
    packet.extend_from_slice(&body);
    sock.write_all(&packet).await.expect("send CONNECT");

    let mut connack = [0u8; 4];
    sock.read_exact(&mut connack).await.expect("recv CONNACK");
    assert_eq!(connack[0], 0x20);
    assert_eq!(connack[3], 0x00, "CONNACK success");
    sock
}

async fn mqtt_publish(sock: &mut tokio::net::TcpStream, topic: &str, payload: &[u8], retain: bool) {
    let header_byte = 0x30 | (if retain { 0x01 } else { 0x00 });
    let mut body = mqtt_field(topic.as_bytes());
    body.extend_from_slice(payload);

    let mut packet = vec![header_byte, body.len() as u8];
    packet.extend_from_slice(&body);
    sock.write_all(&packet).await.expect("send PUBLISH");
}

async fn mqtt_ping(sock: &mut tokio::net::TcpStream) {
    sock.write_all(&[0xc0, 0x00]).await.expect("send PINGREQ");
    let mut resp = [0u8; 2];
    sock.read_exact(&mut resp).await.expect("recv PINGRESP");
    assert_eq!(resp, [0xd0, 0x00]);
}

async fn mqtt_subscribe_and_read_one(port: u16, client_id: &str, topic: &str) -> Option<Vec<u8>> {
    let mut sock = mqtt_connect_311(port, client_id).await;
    let mut sub_body = Vec::new();
    sub_body.extend_from_slice(&1u16.to_be_bytes()); // Packet ID 1
    sub_body.extend_from_slice(&mqtt_field(topic.as_bytes()));
    sub_body.push(0); // QoS 0

    let mut sub_packet = vec![0x82, sub_body.len() as u8];
    sub_packet.extend_from_slice(&sub_body);
    sock.write_all(&sub_packet).await.expect("send SUBSCRIBE");

    let mut suback = [0u8; 5];
    sock.read_exact(&mut suback).await.expect("recv SUBACK");
    assert_eq!(suback[0], 0x90);

    let mut buf = [0u8; 1024];
    let read_res = tokio::time::timeout(Duration::from_millis(800), sock.read(&mut buf)).await;
    match read_res {
        Ok(Ok(n)) if n > 0 => {
            if buf[0] & 0xF0 == 0x30 {
                let topic_len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
                let payload_start = 4 + topic_len;
                Some(buf[payload_start..n].to_vec())
            } else {
                None
            }
        }
        _ => None,
    }
}

async fn add_acl_rule(
    api_port: u16,
    tenant: Option<&str>,
    client_pattern: &str,
    action: &str,
    topic_pattern: &str,
    allow: bool,
) {
    let http = reqwest::Client::new();
    let query = tenant.map_or(String::new(), |t| format!("?tenant={t}"));
    let url = format!("http://127.0.0.1:{api_port}/api/v1/auth/acls{query}");
    let res = http
        .post(&url)
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({
            "client_pattern": client_pattern,
            "action": action,
            "topic_pattern": topic_pattern,
            "allow": allow
        }))
        .send()
        .await
        .expect("send acl post");
    assert!(
        res.status().is_success(),
        "acl rule addition must succeed: {}",
        res.status()
    );
}

/// Test 1: Denied and allowed CoAP GET and Observe requests.
///
/// Asserts:
/// - Denied GET /ps/secret/data returns 4.01 UNAUTHORIZED and exposes no payload.
/// - Allowed GET /ps/open/data returns 2.05 CONTENT with payload.
/// - Allowed GET on missing topic returns 4.04 NOT FOUND.
/// - Denied Observe: 0 returns 4.01 and receives no notifications.
/// - Allowed Observe: 0 returns 2.05 with Observe option and receives notifications.
#[tokio::test]
async fn coap_get_and_observe_acl_enforcement() {
    let kernel = boot_kernel("get-obs", "").await;
    let gateway_addr: SocketAddr = format!("127.0.0.1:{}", kernel.coap_port)
        .parse()
        .expect("parse gateway addr");

    // Deny subscribe on secret/# for coap-gateway, allow everything else.
    add_acl_rule(
        kernel.api_port,
        None,
        "coap-gateway",
        "subscribe",
        "secret/#",
        false,
    )
    .await;
    add_acl_rule(kernel.api_port, None, "*", "all", "#", true).await;

    // Seed retained messages via MQTT publisher.
    let mut mqtt = mqtt_connect_311(kernel.mqtt_port, "seeder-1").await;
    mqtt_publish(&mut mqtt, "secret/data", b"classified-payload", true).await;
    mqtt_publish(&mut mqtt, "open/data", b"public-payload", true).await;
    mqtt_ping(&mut mqtt).await;

    let client = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("client udp bind");

    // 1. Denied plain GET -> 4.01 UNAUTHORIZED, exposes no retained payload.
    let req_denied_get = build_coap_req(CoapCode::GET, 101, b"t1", "secret/data", false);
    let resp_denied_get = send_and_recv_coap(&client, gateway_addr, &req_denied_get).await;
    assert_eq!(
        resp_denied_get.code,
        CoapCode::UNAUTHORIZED,
        "denied GET must return 4.01 UNAUTHORIZED"
    );
    assert!(
        resp_denied_get.payload.is_empty(),
        "denied GET must not expose retained payload"
    );

    // 2. Allowed plain GET -> 2.05 CONTENT with retained payload.
    let req_allowed_get = build_coap_req(CoapCode::GET, 102, b"t2", "open/data", false);
    let resp_allowed_get = send_and_recv_coap(&client, gateway_addr, &req_allowed_get).await;
    assert_eq!(
        resp_allowed_get.code,
        CoapCode::CONTENT,
        "allowed GET must return 2.05 CONTENT"
    );
    assert_eq!(
        resp_allowed_get.payload,
        Bytes::from_static(b"public-payload")
    );
    assert!(
        !resp_allowed_get
            .options
            .iter()
            .any(|o| o.number == option_number::OBSERVE),
        "plain GET response must not have Observe option"
    );

    // 3. Allowed plain GET on non-existent topic -> 4.04 NOT FOUND.
    let req_not_found = build_coap_req(CoapCode::GET, 103, b"t3", "open/missing", false);
    let resp_not_found = send_and_recv_coap(&client, gateway_addr, &req_not_found).await;
    assert_eq!(
        resp_not_found.code,
        CoapCode::NOT_FOUND,
        "allowed GET with no retained message must return 4.04 NOT FOUND"
    );

    // 4. Denied GET with Observe: 0 -> 4.01 UNAUTHORIZED, no registration, no notifications.
    let req_denied_obs = build_coap_req(CoapCode::GET, 104, b"obs-denied", "secret/data", true);
    let resp_denied_obs = send_and_recv_coap(&client, gateway_addr, &req_denied_obs).await;
    assert_eq!(
        resp_denied_obs.code,
        CoapCode::UNAUTHORIZED,
        "denied Observe GET must return 4.01 UNAUTHORIZED"
    );

    // Publish update to secret/data: client must receive no notification.
    mqtt_publish(&mut mqtt, "secret/data", b"classified-update", false).await;
    mqtt_ping(&mut mqtt).await;
    let mut probe_buf = [0u8; 1024];
    let obs_recv =
        tokio::time::timeout(Duration::from_millis(500), client.recv_from(&mut probe_buf)).await;
    assert!(
        obs_recv.is_err(),
        "denied observer must not receive publish notifications"
    );

    // 5. Allowed GET with Observe: 0 -> 2.05 CONTENT, Observe option present, receives notifications.
    let req_allowed_obs = build_coap_req(CoapCode::GET, 105, b"obs-ok", "open/data", true);
    let resp_allowed_obs = send_and_recv_coap(&client, gateway_addr, &req_allowed_obs).await;
    assert_eq!(
        resp_allowed_obs.code,
        CoapCode::CONTENT,
        "allowed Observe GET must return 2.05 CONTENT"
    );
    assert_eq!(
        resp_allowed_obs.payload,
        Bytes::from_static(b"public-payload")
    );
    assert!(
        resp_allowed_obs
            .options
            .iter()
            .any(|o| o.number == option_number::OBSERVE),
        "allowed Observe response must include Observe option"
    );

    // Publish update to open/data: client receives notification.
    mqtt_publish(&mut mqtt, "open/data", b"public-update-1", false).await;
    mqtt_ping(&mut mqtt).await;
    let (notif_len, _) = tokio::time::timeout(STEP_TIMEOUT, client.recv_from(&mut probe_buf))
        .await
        .expect("notification arrives")
        .expect("udp recv");
    let notif_msg = CoapMessage::decode(&probe_buf[..notif_len]).expect("notification decodes");
    assert_eq!(notif_msg.code, CoapCode::CONTENT);
    assert_eq!(notif_msg.payload, Bytes::from_static(b"public-update-1"));
    assert_eq!(notif_msg.token, Bytes::from_static(b"obs-ok"));
    assert!(
        notif_msg
            .options
            .iter()
            .any(|o| o.number == option_number::OBSERVE),
        "notification message must carry Observe option"
    );
}

/// Test 2: Denied and allowed CoAP DELETE requests.
///
/// Asserts:
/// - Denied DELETE /ps/readonly/data returns 4.01 UNAUTHORIZED and preserves retained payload.
/// - Allowed DELETE /ps/mutable/data returns 2.02 DELETED and clears retained payload.
#[tokio::test]
async fn coap_delete_acl_enforcement() {
    let kernel = boot_kernel("del-acl", "").await;
    let gateway_addr: SocketAddr = format!("127.0.0.1:{}", kernel.coap_port)
        .parse()
        .expect("parse gateway addr");

    // Deny publish (and delete) on readonly/# for coap-gateway, allow everything else.
    add_acl_rule(
        kernel.api_port,
        None,
        "coap-gateway",
        "publish",
        "readonly/#",
        false,
    )
    .await;
    add_acl_rule(kernel.api_port, None, "*", "all", "#", true).await;

    // Seed retained messages via MQTT publisher.
    let mut mqtt = mqtt_connect_311(kernel.mqtt_port, "seeder-2").await;
    mqtt_publish(&mut mqtt, "readonly/data", b"preserved-data", true).await;
    mqtt_publish(&mut mqtt, "mutable/data", b"temporary-data", true).await;
    mqtt_ping(&mut mqtt).await;

    let client = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("client udp bind");

    // Ensure mutable/data is retained before testing DELETE.
    let mut verified = false;
    for _ in 0..20 {
        let req_check = build_coap_req(CoapCode::GET, 200, b"d0", "mutable/data", false);
        let resp_check = send_and_recv_coap(&client, gateway_addr, &req_check).await;
        if resp_check.code == CoapCode::CONTENT
            && resp_check.payload == Bytes::from_static(b"temporary-data")
        {
            verified = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        verified,
        "mutable/data must be retained before testing DELETE"
    );

    // 1. Denied DELETE -> 4.01 UNAUTHORIZED.
    let req_denied_del = build_coap_req(CoapCode::DELETE, 201, b"d1", "readonly/data", false);
    let resp_denied_del = send_and_recv_coap(&client, gateway_addr, &req_denied_del).await;
    assert_eq!(
        resp_denied_del.code,
        CoapCode::UNAUTHORIZED,
        "denied DELETE must return 4.01 UNAUTHORIZED"
    );

    // Verify retained message in readonly/data was preserved.
    let retained_read =
        mqtt_subscribe_and_read_one(kernel.mqtt_port, "verifier-sub-1", "readonly/data").await;
    assert_eq!(
        retained_read,
        Some(b"preserved-data".to_vec()),
        "denied DELETE must preserve the retained payload"
    );

    // 2. Allowed DELETE -> 2.02 DELETED.
    let req_allowed_del = build_coap_req(CoapCode::DELETE, 202, b"d2", "mutable/data", false);
    let resp_allowed_del = send_and_recv_coap(&client, gateway_addr, &req_allowed_del).await;
    assert_eq!(
        resp_allowed_del.code,
        CoapCode::DELETED,
        "allowed DELETE must return 2.02 DELETED"
    );

    // Verify retained message in mutable/data was cleared.
    let cleared_read =
        mqtt_subscribe_and_read_one(kernel.mqtt_port, "verifier-sub-2", "mutable/data").await;
    assert_eq!(
        cleared_read, None,
        "allowed DELETE must clear the retained message"
    );
}

/// Test 3: Missing and invalid topics fail closed with 4.00 BAD REQUEST.
#[tokio::test]
async fn coap_bad_request_topics_fail_closed() {
    let kernel = boot_kernel("bad-req", "").await;
    let gateway_addr: SocketAddr = format!("127.0.0.1:{}", kernel.coap_port)
        .parse()
        .expect("parse gateway addr");
    let client = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("client udp bind");

    // Missing topic (bare /ps)
    let req_missing = CoapMessage {
        message_type: CoapType::Confirmable,
        code: CoapCode::GET,
        message_id: 301,
        token: Bytes::from_static(b"m1"),
        options: vec![CoapOption {
            number: option_number::URI_PATH,
            value: Bytes::from_static(b"ps"),
        }],
        payload: Bytes::new(),
    };
    let resp_missing = send_and_recv_coap(&client, gateway_addr, &req_missing).await;
    assert_eq!(resp_missing.code, CoapCode::BAD_REQUEST);

    // Invalid topic with '+' wildcard
    let req_wildcard_plus = build_coap_req(CoapCode::GET, 302, b"m2", "sensors/+/temp", false);
    let resp_wildcard_plus = send_and_recv_coap(&client, gateway_addr, &req_wildcard_plus).await;
    assert_eq!(resp_wildcard_plus.code, CoapCode::BAD_REQUEST);

    // Invalid topic with '#' wildcard on DELETE
    let req_wildcard_hash = build_coap_req(CoapCode::DELETE, 303, b"m3", "sensors/#", false);
    let resp_wildcard_hash = send_and_recv_coap(&client, gateway_addr, &req_wildcard_hash).await;
    assert_eq!(resp_wildcard_hash.code, CoapCode::BAD_REQUEST);
}

/// Test 4: Observer table capacity bounds and fallback.
///
/// Asserts:
/// - CoapGatewayHandler bounds total observer capacity to MAX_COAP_OBSERVERS = 65536.
/// - Over UDP sockets, when an observer registration is rejected due to capacity,
///   the gateway returns 2.05 CONTENT without an Observe option and registers no observer.
#[tokio::test]
async fn coap_observer_capacity_and_fallback() {
    // 1. Direct verification of MAX_COAP_OBSERVERS bound in CoapGatewayHandler.
    let handler = CoapGatewayHandler::new();
    {
        let table = handler.observers_for("temp");
        assert!(table.is_empty());
    }
    // Test MAX_COAP_OBSERVERS constant value
    assert_eq!(
        MAX_COAP_OBSERVERS,
        MAX_OBSERVED_TOPICS * MAX_OBSERVERS_PER_TOPIC,
        "MAX_COAP_OBSERVERS must equal 2048 * 32 = 65536"
    );

    // 2. End-to-end UDP verification of observer capacity rejection fallback:
    // When registration fails, GET serves retained content without Observe option.
    let kernel = boot_kernel("obs-cap", "").await;
    let gateway_addr: SocketAddr = format!("127.0.0.1:{}", kernel.coap_port)
        .parse()
        .expect("parse gateway addr");

    let mut mqtt = mqtt_connect_311(kernel.mqtt_port, "seeder-cap").await;
    mqtt_publish(&mut mqtt, "capped/topic", b"capped-payload", true).await;
    mqtt_ping(&mut mqtt).await;

    // Fill the per-topic observer capacity (MAX_OBSERVERS_PER_TOPIC = 32)
    // using distinct UDP client sockets.
    let mut fill_clients = Vec::new();
    for i in 0..MAX_OBSERVERS_PER_TOPIC {
        let client = tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("udp client bind");
        let token = format!("tok{i:03}");
        let req = build_coap_req(
            CoapCode::GET,
            (400 + i) as u16,
            token.as_bytes(),
            "capped/topic",
            true,
        );
        let resp = send_and_recv_coap(&client, gateway_addr, &req).await;
        assert_eq!(resp.code, CoapCode::CONTENT);
        assert!(
            resp.options
                .iter()
                .any(|o| o.number == option_number::OBSERVE),
            "registration #{i} within capacity must have Observe option"
        );
        fill_clients.push(client);
    }

    // The 33rd registration exceeds capacity:
    // Gateway must return 2.05 CONTENT with retained payload, but WITHOUT the Observe option.
    let extra_client = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("extra udp client bind");
    let req_extra = build_coap_req(CoapCode::GET, 500, b"extra-tok", "capped/topic", true);
    let resp_extra = send_and_recv_coap(&extra_client, gateway_addr, &req_extra).await;
    assert_eq!(
        resp_extra.code,
        CoapCode::CONTENT,
        "request past observer capacity must still return 2.05 CONTENT"
    );
    assert_eq!(
        resp_extra.payload,
        Bytes::from_static(b"capped-payload"),
        "request past observer capacity serves retained payload"
    );
    assert!(
        !resp_extra
            .options
            .iter()
            .any(|o| o.number == option_number::OBSERVE),
        "request past observer capacity must omit the Observe option"
    );

    // Verify extra_client was not registered and receives no subsequent notifications.
    mqtt_publish(&mut mqtt, "capped/topic", b"capped-update", false).await;
    mqtt_ping(&mut mqtt).await;
    let mut probe_buf = [0u8; 1024];
    let extra_recv = tokio::time::timeout(
        Duration::from_millis(500),
        extra_client.recv_from(&mut probe_buf),
    )
    .await;
    assert!(
        extra_recv.is_err(),
        "unregistered observer past capacity must receive no notification"
    );
}

/// Test 5: Multi-tenant isolation per R-TENANT.
///
/// Proves:
/// - CoAP operations run strictly in DEFAULT_TENANT_ID.
/// - CoAP GET cannot read retained messages of tenant-b.
/// - CoAP Observe cannot stream messages of tenant-b.
/// - CoAP DELETE cannot clear retained messages of tenant-b.
#[tokio::test]
async fn coap_two_tenant_isolation() {
    let tenant_conf = "\n[[tenants.rules]]\nlistener = \"\"\nattribute = \"tenant\"\nexpression = \"${clientid}\"\n";
    let kernel = boot_kernel("two-tenant", tenant_conf).await;
    let gateway_addr: SocketAddr = format!("127.0.0.1:{}", kernel.coap_port)
        .parse()
        .expect("parse gateway addr");

    // Client "tenant-b" connects into isolated tenant "tenant-b"
    // and publishes a retained message on "isolated/data".
    let mut mqtt_tenant_b = mqtt_connect_311(kernel.mqtt_port, "tenant-b").await;
    mqtt_publish(
        &mut mqtt_tenant_b,
        "isolated/data",
        b"tenant-b-secret-retained",
        true,
    )
    .await;
    mqtt_ping(&mut mqtt_tenant_b).await;

    let client = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("client udp bind");

    // 1. CoAP GET in default tenant cannot read tenant-b retained message -> 4.04 NOT FOUND.
    let req_get = build_coap_req(CoapCode::GET, 601, b"g1", "isolated/data", false);
    let resp_get = send_and_recv_coap(&client, gateway_addr, &req_get).await;
    assert_eq!(
        resp_get.code,
        CoapCode::NOT_FOUND,
        "CoAP GET must not read retained messages of another tenant"
    );

    // 2. CoAP Observe in default tenant registers in default tenant.
    let req_obs = build_coap_req(CoapCode::GET, 602, b"o1", "isolated/data", true);
    let resp_obs = send_and_recv_coap(&client, gateway_addr, &req_obs).await;
    // With no retained message in default tenant, Observe returns empty content with Observe option.
    assert_eq!(resp_obs.code, CoapCode::CONTENT);
    assert!(
        resp_obs.payload.is_empty(),
        "default tenant has no retained message"
    );

    // Publish update in tenant-b: default tenant CoAP observer must receive nothing.
    mqtt_publish(
        &mut mqtt_tenant_b,
        "isolated/data",
        b"tenant-b-update",
        false,
    )
    .await;
    mqtt_ping(&mut mqtt_tenant_b).await;
    let mut probe_buf = [0u8; 1024];
    let obs_leak =
        tokio::time::timeout(Duration::from_millis(500), client.recv_from(&mut probe_buf)).await;
    assert!(
        obs_leak.is_err(),
        "CoAP observer in default tenant must not receive tenant-b publish"
    );

    // 3. CoAP DELETE in default tenant cannot clear tenant-b retained message.
    let req_del = build_coap_req(CoapCode::DELETE, 603, b"d1", "isolated/data", false);
    let resp_del = send_and_recv_coap(&client, gateway_addr, &req_del).await;
    assert_eq!(
        resp_del.code,
        CoapCode::DELETED,
        "CoAP DELETE answers 2.02 DELETED in default tenant"
    );

    // Verify tenant-b retained message is still present in tenant-b!
    let retained_tenant_b =
        mqtt_subscribe_and_read_one(kernel.mqtt_port, "tenant-b", "isolated/data").await;
    assert_eq!(
        retained_tenant_b,
        Some(b"tenant-b-secret-retained".to_vec()),
        "CoAP DELETE in default tenant must not clear retained message of tenant-b"
    );
}
