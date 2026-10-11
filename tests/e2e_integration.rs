//! End-to-end integration and smoke suite for IndraMQTT core services.
//!
//! Validates the end-to-end messaging pipeline without requiring external network daemons:
//! 1. Multi-topic Radix Trie router registration and matching.
//! 2. Streaming SQL evaluation and rule action dispatch.
//! 3. Session state retention, QoS 1 in-flight tracking, and offline queueing.
//! 4. API kick of a connected client delivers `ConnClose` through
//!    the kernel listener and the client observes TCP close.
//!
//! Each test that needs a broker boots one real kernel with
//! [`spawn_kernel`]. The helper writes a config dir with ephemeral ports
//! for the TCP, WebSocket and API listeners, waits for `/healthz` and
//! the MQTT port, and stops the child on drop. The kernel serves every
//! enabled listener, so the configs set no `native` key.

use broker_protocol::{QoS, Topic, TopicFilter};
use broker_router::{Router, Subscription};
use broker_rules::{BackpressurePolicy, BrokerSink, RuleAction, RuleEngine};
use broker_session::{QueuedMessage, SessionManager};
use bytes::Bytes;
use std::sync::{Arc, Mutex};

/// Bound for one socket step. A missing packet fails instead of hanging.
const STEP: std::time::Duration = std::time::Duration::from_secs(10);
/// Bound for one kernel boot. A cold debug boot needs the full minute.
const BOOT: std::time::Duration = std::time::Duration::from_secs(60);
/// API key for the management API. It clears the 16-char minimum.
const API_KEY: &str = "p6-e2e-test-key-0001";

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

/// Takes one free loopback port.
fn ephemeral_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("probe binds")
        .local_addr()
        .expect("probe addr")
        .port()
}

/// Boots one real kernel with the kernel listener on ephemeral ports.
///
/// `extra_toml` appends to `indra.toml` (tenant rules, session limits).
/// TCP, WebSocket and API get explicit loopback ports. TLS and secure
/// WebSocket stay disabled. The link bind gets an explicit loopback port
/// so parallel tests never share the default port. The call waits for
/// `/healthz` plus the MQTT and WebSocket ports. The child dies with the
/// test (`kill_on_drop`).
async fn spawn_kernel(extra_toml: &str) -> Kernel {
    let tag = format!(
        "p6-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let scratch = std::env::temp_dir().join(format!("indramqtt-{tag}-data"));
    let config_dir = std::env::temp_dir().join(format!("indramqtt-{tag}-cfg"));
    std::fs::create_dir_all(&config_dir).expect("create temp config dir");
    let bl_port = ephemeral_port();
    let mqtt_port = ephemeral_port();
    let ws_port = ephemeral_port();
    let api_port = ephemeral_port();
    std::fs::write(
        config_dir.join("indra.toml"),
        format!(
            "[node]\n\
             id = \"p6-e2e\"\n\
             data_dir = \"{}\"\n\
             brokerlink_bind = \"127.0.0.1:{bl_port}\"\n\
             \n\
             [listeners.tcp]\n\
              bind = \"127.0.0.1:{mqtt_port}\"\n\
              \n\
              [listeners.tls]\n\
              enabled = false\n\
              \n\
              [listeners.ws]\n\
              bind = \"127.0.0.1:{ws_port}\"\n\
              \n\
              [listeners.wss]\n\
              enabled = false\n\
              \n\
              [listeners.api]\n\
              bind = \"127.0.0.1:{api_port}\"\n\
              \n\
              [logging]\n\
              level = \"warn\"\n\
              \n\
              {extra_toml}",
            scratch.display(),
        ),
    )
    .expect("write indra.toml");
    let mut child = tokio::process::Command::new(shard_kernel_binary())
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
            std::time::Duration::from_secs(2),
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
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
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

#[derive(Debug, Default)]
struct TestBrokerSink {
    published: Mutex<Vec<(Topic, Bytes, QoS, bool)>>,
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

#[tokio::test]
async fn test_e2e_router_and_rules_pipeline() {
    let router = Router::new();
    let engine = RuleEngine::new(1024, BackpressurePolicy::Block);
    let sink = Arc::new(TestBrokerSink::default());
    let broker_sink: Arc<dyn BrokerSink> = sink.clone();

    // 1. Install subscriptions
    let filter = TopicFilter::new("industrial/+/telemetry").unwrap();
    router.subscribe(
        &filter,
        Subscription {
            client_id: "scada-gateway-1".into(),
            conn_id: 101,
            qos: QoS::AtLeastOnce,
            group: None,
            tenant: broker_router::DEFAULT_TENANT_ID.into(),
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
            subscription_id: 0,
        },
    );

    // 2. Install streaming rule with projection
    engine
        .create_rule(
            "temp_alarm".to_string(),
            TopicFilter::new("industrial/+/telemetry").unwrap(),
            Some(
                r#"SELECT machine_id, temperature FROM "industrial/+/telemetry" WHERE temperature > 85.0"#
                    .to_string(),
            ),
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("alarms/high_temperature").unwrap(),
                qos: QoS::AtLeastOnce,
            }],
        )
        .expect("rule created");

    // 3. Ingest matching event below threshold (router matches, but rule filter suppresses action)
    let normal_topic = Topic::new("industrial/line1/telemetry").unwrap();
    let normal_payload = Bytes::from_static(br#"{"machine_id": "M1", "temperature": 72.0}"#);

    assert_eq!(router.matches(&normal_topic).len(), 1);
    engine
        .dispatch_ingress(
            &normal_topic,
            &normal_payload,
            QoS::AtLeastOnce,
            &broker_sink,
        )
        .await;
    assert_eq!(sink.published.lock().unwrap().len(), 0);

    // 4. Ingest matching event above threshold (triggers rule republish action)
    let alarm_topic = Topic::new("industrial/line2/telemetry").unwrap();
    let alarm_payload = Bytes::from_static(br#"{"machine_id": "M2", "temperature": 94.5}"#);

    assert_eq!(router.matches(&alarm_topic).len(), 1);
    engine
        .dispatch_ingress(&alarm_topic, &alarm_payload, QoS::AtLeastOnce, &broker_sink)
        .await;

    let published = sink.published.lock().unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].0.as_str(), "alarms/high_temperature");
    let projected_json: serde_json::Value =
        serde_json::from_slice(&published[0].1).expect("valid projected JSON");
    assert_eq!(projected_json["machine_id"], "M2");
    assert_eq!(projected_json["temperature"], 94.5);
}

#[test]
fn test_e2e_session_lifecycle_and_offline_queue() {
    let session_mgr = SessionManager::new_with_limits(Some(50));
    let client_id = "edge-station-42";

    // Create durable session
    let (session, present) = session_mgr.get_or_create(client_id, false);
    assert!(!present);
    assert_eq!(session.client_id, client_id);

    // Queue offline messages
    for i in 1..=20 {
        let topic = Topic::new(format!("downstream/job-{i}")).unwrap();
        let payload = Bytes::from(format!("payload-{i}"));
        session.push_offline(QueuedMessage {
            topic,
            qos: QoS::AtLeastOnce,
            retain: false,
            payload,
            publish_at_ms: None,
        });
    }
    assert_eq!(session.offline_len(), 20);

    // Reconnecting retrieves the same session with all 20 queued messages
    let (resumed, present2) = session_mgr.get_or_create(client_id, false);
    assert!(present2);
    assert_eq!(resumed.id, session.id);
    assert_eq!(resumed.offline_len(), 20);

    // Drain offline messages on reconnection
    let drained = resumed.drain_offline();
    assert_eq!(drained.len(), 20);
    assert_eq!(resumed.offline_len(), 0);
}

/// Minimal MQTT 3.1.1 CONNECT packet for `client_id` (clean session,
/// keepalive 60): fixed header plus the `MQTT`/4 variable header and
/// the id payload. Enough for the kernel listener to accept the client.
fn mqtt_connect_packet(client_id: &str) -> Vec<u8> {
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
async fn read_mqtt_packet(sock: &mut tokio::net::TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    use tokio::io::AsyncReadExt;
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
                "MQTT remaining length overflow",
            ));
        }
    }
    let mut body = vec![0u8; remaining];
    sock.read_exact(&mut body).await?;
    Ok((head[0], body))
}

/// Dials the kernel MQTT listener with a bound.
async fn dial_mqtt(port: u16) -> tokio::net::TcpStream {
    tokio::time::timeout(STEP, tokio::net::TcpStream::connect(("127.0.0.1", port)))
        .await
        .expect("dial in time")
        .expect("dial")
}

/// Sends bytes with a bound.
async fn send_mqtt(sock: &mut tokio::net::TcpStream, bytes: &[u8]) {
    use tokio::io::AsyncWriteExt;
    tokio::time::timeout(STEP, sock.write_all(bytes))
        .await
        .expect("send in time")
        .expect("send");
}

/// Reads one MQTT packet with a bound.
async fn recv_mqtt(sock: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    tokio::time::timeout(STEP, read_mqtt_packet(sock))
        .await
        .expect("packet in time")
        .expect("packet")
}

/// Kick closes the socket of a live client through the kernel listener.
///
/// A real 3.1.1 client connects to the booted kernel. The test kicks it
/// with `DELETE /api/v5/clients/{id}` and bearer auth. The client then
/// observes TCP close. The kick sends `ConnClose` with empty metadata,
/// so a version-5 client gets no DISCONNECT either. A regressed
/// unregister-first order drops the close and the final read times out
/// instead of passing.
#[tokio::test]
async fn kicked_mqtt_client_sees_disconnect() {
    use tokio::io::AsyncReadExt;

    const CLIENT_ID: &str = "kicked-client";

    let kernel = spawn_kernel("").await;
    let mut sock = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(&mut sock, &mqtt_connect_packet(CLIENT_ID)).await;
    let (head, body) = recv_mqtt(&mut sock).await;
    assert_eq!(head, 0x20, "kernel answers with CONNACK");
    assert_eq!(body, vec![0x00, 0x00], "CONNECT is accepted");

    let http = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{}", kernel.api_port);
    let kick = tokio::time::timeout(
        STEP,
        http.delete(format!("{base}/api/v5/clients/{CLIENT_ID}"))
            .bearer_auth(API_KEY)
            .send(),
    )
    .await
    .expect("kick in time")
    .expect("kick send");
    assert_eq!(kick.status(), 204, "kick removes the live client");

    let mut probe = [0u8; 1];
    let closed = tokio::time::timeout(STEP, sock.read(&mut probe))
        .await
        .expect("close in time")
        .expect("close read");
    assert_eq!(closed, 0, "kicked socket is closed");

    let again = tokio::time::timeout(
        STEP,
        http.delete(format!("{base}/api/v5/clients/{CLIENT_ID}"))
            .bearer_auth(API_KEY)
            .send(),
    )
    .await
    .expect("second kick in time")
    .expect("second kick send");
    assert_eq!(again.status(), 404, "kicked client is gone");

    kernel.finish().await;
}

/// Backup file download through the broker HTTP API (C18-05).
///
/// Upload one file with random bytes through POST /data/files. Fetch it
/// through GET /data/files/{filename}. Check the bytes match. Check an
/// unknown name returns 404 NOT_FOUND. Check a bad name returns 400.
#[tokio::test]
async fn backup_file_download_through_broker_api() {
    use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
    use broker_api::ApiState;
    use broker_auth::MemoryAuth;
    use broker_config::ConfigRegistry;
    use broker_observability::Metrics;
    use broker_router::{ConnTable, Router};
    use broker_rules::{BackpressurePolicy, RuleEngine};
    use broker_session::SessionManager;
    use rand::Rng;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpListener;

    const TIMEOUT: Duration = Duration::from_secs(5);

    let engine = Arc::new(RuleEngine::new(16, BackpressurePolicy::DropOldest));
    let sessions = Arc::new(SessionManager::new());
    let sub_router = Arc::new(Router::new());
    let metrics = Arc::new(Metrics::new());
    let auth = Arc::new(MemoryAuth::new());
    let conns = Arc::new(ConnTable::default());
    let scratch = std::env::temp_dir().join(format!(
        "indramqtt-backup-e2e-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let config = Arc::new(ConfigRegistry::load(&scratch).expect("validated defaults load"));
    let (edge_tx, _edge_rx) = tokio::sync::mpsc::unbounded_channel::<brokerlink::BrokerFrame>();
    let api_state = ApiState::new(
        engine,
        sessions,
        sub_router,
        metrics,
        auth,
        conns,
        config,
        "indra-node-1".to_string(),
        edge_tx,
    );

    let api_listener = tokio::time::timeout(TIMEOUT, TcpListener::bind("127.0.0.1:0"))
        .await
        .expect("bind API listener in time")
        .expect("bind API listener");
    let api_port = api_listener.local_addr().expect("API addr").port();
    tokio::spawn(async move { broker_api::serve(api_listener, api_state).await });
    let base = format!("http://127.0.0.1:{api_port}");

    // Log in. Use the default admin credential first.
    let http = reqwest::Client::new();
    let login = tokio::time::timeout(
        TIMEOUT,
        http.post(format!("{base}/api/v5/login"))
            .json(&serde_json::json!({"username": "admin", "password": "public"}))
            .send(),
    )
    .await
    .expect("login request in time")
    .expect("login request");
    assert_eq!(login.status(), 200);
    let login_body: serde_json::Value = tokio::time::timeout(TIMEOUT, login.json())
        .await
        .expect("login body in time")
        .expect("login body");
    let fresh = login_body["token"]
        .as_str()
        .expect("login token")
        .to_string();
    let changed = tokio::time::timeout(
        TIMEOUT,
        http.put(format!("{base}/api/v5/users/admin/change_pwd"))
            .bearer_auth(&fresh)
            .json(&serde_json::json!({"old_pwd": "public", "new_pwd": "Backup-e2e-1!"}))
            .send(),
    )
    .await
    .expect("change password request in time")
    .expect("change password request");
    assert_eq!(changed.status(), 204);
    let login = tokio::time::timeout(
        TIMEOUT,
        http.post(format!("{base}/api/v5/login"))
            .json(&serde_json::json!({"username": "admin", "password": "Backup-e2e-1!"}))
            .send(),
    )
    .await
    .expect("second login request in time")
    .expect("second login request");
    assert_eq!(login.status(), 200);
    let login_body: serde_json::Value = tokio::time::timeout(TIMEOUT, login.json())
        .await
        .expect("login body in time")
        .expect("login body");
    let token = login_body["token"]
        .as_str()
        .expect("login token")
        .to_string();

    // Make random bytes. The broker cannot know them in advance.
    let mut rng = rand::rng();
    let mut test_content = vec![0u8; 8192];
    rng.fill_bytes(&mut test_content);
    let content = BASE64_STANDARD.encode(&test_content);
    let filename = "e2e-dl-hit.tar.gz";
    let upload = tokio::time::timeout(
        TIMEOUT,
        http.post(format!("{base}/api/v5/data/files"))
            .bearer_auth(&token)
            .json(&serde_json::json!({"filename": filename, "content": content}))
            .send(),
    )
    .await
    .expect("upload request in time")
    .expect("upload request");
    assert_eq!(upload.status(), 204);

    // Fetch the file. Check status, type, name and bytes.
    let download = tokio::time::timeout(
        TIMEOUT,
        http.get(format!("{base}/api/v5/data/files/{filename}"))
            .bearer_auth(&token)
            .send(),
    )
    .await
    .expect("download request in time")
    .expect("download request");
    assert_eq!(download.status(), 200);
    assert_eq!(
        download
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("application/octet-stream")
    );
    assert!(download
        .headers()
        .get("content-disposition")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains(filename))
        .unwrap_or(false));
    let bytes = tokio::time::timeout(TIMEOUT, download.bytes())
        .await
        .expect("download body in time")
        .expect("response body readable");
    assert_eq!(bytes.as_ref(), test_content.as_slice());

    // Fetch an unknown name. Check 404 NOT_FOUND.
    let missing = tokio::time::timeout(
        TIMEOUT,
        http.get(format!("{base}/api/v5/data/files/e2e-dl-missing.tar.gz"))
            .bearer_auth(&token)
            .send(),
    )
    .await
    .expect("missing request in time")
    .expect("missing request");
    assert_eq!(missing.status(), 404);
    let body: serde_json::Value = missing.json().await.expect("error body");
    assert_eq!(body["code"], "NOT_FOUND");

    // Fetch a bad name. Check 400 BAD_REQUEST.
    let bad = tokio::time::timeout(
        TIMEOUT,
        http.get(format!("{base}/api/v5/data/files/evil..traversal.tar.gz"))
            .bearer_auth(&token)
            .send(),
    )
    .await
    .expect("bad-name request in time")
    .expect("bad-name request");
    assert_eq!(bad.status(), 400);
    let body: serde_json::Value = bad.json().await.expect("error body");
    assert_eq!(body["code"], "BAD_REQUEST");

    // Fetch with an unknown node. Check 404 NOT_FOUND.
    let bad_node = tokio::time::timeout(
        TIMEOUT,
        http.get(format!(
            "{base}/api/v5/data/files/{filename}?node=no-such-node"
        ))
        .bearer_auth(&token)
        .send(),
    )
    .await
    .expect("node request in time")
    .expect("node request");
    assert_eq!(bad_node.status(), 404);
    let body: serde_json::Value = bad_node.json().await.expect("error body");
    assert_eq!(body["code"], "NOT_FOUND");

    // Remove the file. Keep the scratch dir clean.
    let delete = tokio::time::timeout(
        TIMEOUT,
        http.delete(format!("{base}/api/v5/data/files/{filename}"))
            .bearer_auth(&token)
            .send(),
    )
    .await
    .expect("delete request in time")
    .expect("delete request");
    assert_eq!(delete.status(), 204);
    let _ = std::fs::remove_dir_all(&scratch);
}

/// Locate the real kernel binary for the spawned-kernel tests.
/// `CARGO_BIN_EXE_indramqtt` is set when Cargo builds the test target;
/// the probe fallback covers binaries built by an earlier explicit
/// `cargo build -p broker-node` under either target directory.
fn shard_kernel_binary() -> std::path::PathBuf {
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

/// Topic aliases cross the broker on real MQTT 5 sockets.
///
/// A 3.1.1 subscriber and a version-5 publisher use the kernel listener.
/// The publisher registers alias 3 with a full topic, then publishes by
/// reference with an empty topic. Both downlinks carry the full topic.
/// A version-5 subscriber then proves the server-to-client direction:
/// the kernel assigns an outbound alias to a plain publish.
#[tokio::test]
async fn sharded_topic_alias_negotiation_and_delivery() {
    let kernel = spawn_kernel("").await;

    let mut sub = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(&mut sub, &sy02_connect311("alias-e2e-2", true, 60, None)).await;
    let (head, body) = recv_mqtt(&mut sub).await;
    assert_eq!(head, 0x20, "kernel answers with CONNACK");
    assert_eq!(sy02_parse_connack311(&body), (false, 0));
    send_mqtt(&mut sub, &sy02_subscribe(1, &[("alias/e2e", 0)])).await;
    let (head, body) = recv_mqtt(&mut sub).await;
    assert_eq!(head & 0xF0, 0x90, "kernel answers with SUBACK");
    assert_eq!(sy02_parse_suback(&body), (1, vec![0u8]));

    let mut publisher = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(
        &mut publisher,
        &sy02_connect5("alias-e2e-1", true, 60, None, None, 10),
    )
    .await;
    let (head, body) = recv_mqtt(&mut publisher).await;
    assert_eq!(head & 0xF0, 0x20, "publisher gets CONNACK");
    let (_, reason, alias_max) = sy02_parse_connack5(&body);
    assert_eq!(reason, 0, "alias-carrying CONNECT is accepted");
    assert_eq!(alias_max, 10, "CONNACK carries the alias maximum");

    // First publish carries topic plus alias 3. It registers the mapping
    // and delivers the full topic.
    send_mqtt(
        &mut publisher,
        &sy02_publish5("alias/e2e", 0, 0, false, Some(3), b"21.5"),
    )
    .await;
    let (head, body) = recv_mqtt(&mut sub).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let first = sy02_parse_publish_classic(head, &body);
    assert_eq!(first.topic, "alias/e2e");
    assert_eq!(first.payload, b"21.5");

    // Second publish carries the empty topic plus alias 3. The broker
    // resolves the alias and the subscriber still sees the topic.
    send_mqtt(
        &mut publisher,
        &sy02_publish5("", 0, 0, false, Some(3), b"22.5"),
    )
    .await;
    let (head, body) = recv_mqtt(&mut sub).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let second = sy02_parse_publish_classic(head, &body);
    assert_eq!(second.topic, "alias/e2e");
    assert_eq!(second.payload, b"22.5");

    // Server-to-client direction: a version-5 subscriber gets an
    // outbound alias on a plain publish.
    let mut v5sub = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(
        &mut v5sub,
        &sy02_connect5("alias-e2e-3", true, 60, None, None, 10),
    )
    .await;
    let (head, body) = recv_mqtt(&mut v5sub).await;
    assert_eq!(head & 0xF0, 0x20, "version-5 subscriber gets CONNACK");
    let (_, reason, alias_max) = sy02_parse_connack5(&body);
    assert_eq!(reason, 0);
    assert_eq!(alias_max, 10, "CONNACK carries the alias maximum");
    send_mqtt(&mut v5sub, &sy02_subscribe_v5(1, &[("alias/e2e", 0)], 0)).await;
    let (head, _) = recv_mqtt(&mut v5sub).await;
    assert_eq!(head & 0xF0, 0x90, "kernel answers with SUBACK");
    send_mqtt(
        &mut publisher,
        &sy02_publish5("alias/e2e", 0, 0, false, None, b"23.5"),
    )
    .await;
    let (head, body) = recv_mqtt(&mut v5sub).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let third = sy02_parse_publish_alias(head, &body);
    assert_eq!(third.topic, "alias/e2e");
    assert_eq!(third.payload, b"23.5");
    assert_ne!(third.alias, 0, "server assigns an outbound alias");

    kernel.finish().await;
}

/// Concurrent publishers share one kernel without loss or duplication.
///
/// 200 publishers publish one QoS 1 message each through the kernel
/// listener. A durable subscriber receives every payload exactly once.
/// The subscriber then drops, one message publishes while it is gone,
/// and the reconnect replays exactly that message.
#[tokio::test]
async fn sharded_transports_do_not_serialize() {
    use std::collections::HashSet;

    const PUBLISHERS: u32 = 200;

    let kernel = spawn_kernel("").await;

    let mut sub = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(&mut sub, &sy02_connect311("shard-sub", false, 60, None)).await;
    let (head, body) = recv_mqtt(&mut sub).await;
    assert_eq!(head, 0x20, "kernel answers with CONNACK");
    assert_eq!(
        sy02_parse_connack311(&body),
        (false, 0),
        "first durable CONNECT reports session_present=false"
    );
    send_mqtt(&mut sub, &sy02_subscribe(1, &[("shard/topic", 1)])).await;
    let (head, body) = recv_mqtt(&mut sub).await;
    assert_eq!(head, 0x90, "kernel answers with SUBACK");
    assert_eq!(sy02_parse_suback(&body), (1, vec![1]));

    // 200 publishers publish concurrently, one QoS 1 message each.
    let mut tasks = Vec::new();
    for i in 0..PUBLISHERS {
        let port = kernel.mqtt_port;
        tasks.push(tokio::spawn(async move {
            let mut sock = dial_mqtt(port).await;
            let id = format!("shard-pub-{i}");
            send_mqtt(&mut sock, &sy02_connect311(&id, true, 60, None)).await;
            let (head, _) = recv_mqtt(&mut sock).await;
            assert_eq!(head, 0x20, "publisher gets CONNACK");
            let payload = format!("burst-{i}");
            send_mqtt(
                &mut sock,
                &sy02_publish311("shard/topic", 1, 1, false, payload.as_bytes()),
            )
            .await;
            let (head, body) = recv_mqtt(&mut sock).await;
            assert_eq!(head, 0x40, "publisher sees PUBACK");
            assert_eq!(body, vec![0x00, 0x01], "PUBACK mirrors its packet id");
        }));
    }
    for task in tasks {
        tokio::time::timeout(BOOT, task)
            .await
            .expect("publisher joins in time")
            .expect("publisher passes");
    }

    // The subscriber receives every payload exactly once and acks each
    // delivery, so nothing stays inflight at the drop below.
    let mut seen = HashSet::new();
    for _ in 0..PUBLISHERS {
        let (head, body) = recv_mqtt(&mut sub).await;
        assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
        let downlink = sy02_parse_publish_classic(head, &body);
        assert_eq!(downlink.qos, 1, "downlink keeps QoS 1");
        assert!(seen.insert(downlink.payload.clone()), "no duped delivery");
        send_mqtt(&mut sub, &sy02_puback(downlink.packet_id)).await;
    }
    assert_eq!(seen.len(), PUBLISHERS as usize, "every publish arrives");
    for i in 0..PUBLISHERS {
        assert!(
            seen.contains(&format!("burst-{i}").into_bytes()),
            "burst {i} arrives"
        );
    }

    // Ungraceful drop of the subscriber socket. One publish while
    // detached queues instead of routing to the dead socket.
    drop(sub);
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let mut detached = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(
        &mut detached,
        &sy02_connect311("shard-detached-pub", true, 60, None),
    )
    .await;
    let (head, _) = recv_mqtt(&mut detached).await;
    assert_eq!(head, 0x20, "detached publisher gets CONNACK");
    send_mqtt(
        &mut detached,
        &sy02_publish311("shard/topic", 9, 1, false, b"replay-me"),
    )
    .await;
    let (head, body) = recv_mqtt(&mut detached).await;
    assert_eq!(head, 0x40, "detached publisher sees PUBACK");
    assert_eq!(body, vec![0x00, 0x09], "PUBACK mirrors its packet id");

    // Reconnect resumes the durable session and replays the queued
    // message. A dropped first bind that never detached would route the
    // publish into the dead mailbox instead of the queue, and this read
    // would time out.
    let mut re = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(&mut re, &sy02_connect311("shard-sub", false, 60, None)).await;
    let (head, body) = recv_mqtt(&mut re).await;
    assert_eq!(head, 0x20, "kernel answers the reconnect with CONNACK");
    assert_eq!(
        sy02_parse_connack311(&body),
        (true, 0),
        "resumed session reports session_present=true"
    );
    let (head, body) = recv_mqtt(&mut re).await;
    assert_eq!(head & 0xF0, 0x30, "replay is PUBLISH");
    let replay = sy02_parse_publish_classic(head, &body);
    assert_eq!(replay.topic, "shard/topic");
    assert_eq!(replay.payload, b"replay-me");
    let quiet =
        tokio::time::timeout(std::time::Duration::from_secs(2), read_mqtt_packet(&mut re)).await;
    assert!(quiet.is_err(), "exactly one message replays");

    kernel.finish().await;
}

/// Encode an MQTT remaining length (variable-byte integer).
fn sy02_mqtt_rl(mut len: usize) -> Vec<u8> {
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
fn sy02_mqtt_str(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + s.len());
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
    out
}

/// Minimal MQTT 3.1.1 CONNECT with optional credentials.
fn sy02_connect311(
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
fn sy02_connect5(
    client_id: &str,
    clean_start: bool,
    keepalive: u16,
    creds: Option<(&str, &[u8])>,
    will: Option<(&str, &[u8])>,
    alias_max: u16,
) -> Vec<u8> {
    let mut rest = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05];
    let mut flags = 0u8;
    if let Some((_, pass)) = creds {
        flags |= 0x80;
        if !pass.is_empty() {
            flags |= 0x40;
        }
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
fn sy02_subscribe(packet_id: u16, subs: &[(&str, u8)]) -> Vec<u8> {
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
fn sy02_publish311(topic: &str, packet_id: u16, qos: u8, retain: bool, payload: &[u8]) -> Vec<u8> {
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
fn sy02_publish5(
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
fn sy02_puback(packet_id: u16) -> Vec<u8> {
    vec![0x40, 0x02, (packet_id >> 8) as u8, (packet_id & 0xff) as u8]
}

/// Encode a variable-byte integer (MQTT property lengths and ids).
fn sy02_encode_varint(mut value: u32) -> Vec<u8> {
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
fn sy02_subscribe_v5(packet_id: u16, subs: &[(&str, u8)], sub_id: u32) -> Vec<u8> {
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
fn sy02_publish5_with_props(
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
fn sy02_disconnect_v5(reason: u8) -> Vec<u8> {
    let body = vec![reason, 0x00];
    let mut packet = vec![0xE0];
    packet.extend_from_slice(&sy02_mqtt_rl(body.len()));
    packet.extend_from_slice(&body);
    packet
}

/// Parse an MQTT 5 SUBACK body into `(packet_id, granted codes)`,
/// skipping the property section.
fn sy02_parse_suback5(body: &[u8]) -> (u16, Vec<u8>) {
    assert!(body.len() >= 3, "v5 SUBACK carries packet id and props");
    let packet_id = u16::from_be_bytes([body[0], body[1]]);
    let (prop_len, used) = sy02_varint(&body[2..]).expect("SUBACK property length");
    assert!(body.len() >= 2 + used + prop_len, "SUBACK props fit");
    (packet_id, body[2 + used + prop_len..].to_vec())
}

/// One MQTT 5 PUBLISH as the kernel delivers it, with forwarded props.
struct Sy02Publish5 {
    topic: String,
    qos: u8,
    sub_id: u32,
    format: u8,
    expiry: u32,
    users: Vec<(String, String)>,
    payload: Vec<u8>,
}

/// Parse an MQTT 5 PUBLISH body (QoS 0): topic, property section
/// (subscription id 11, format 1, expiry 2, user 38, alias 35 skipped)
/// and payload.
fn sy02_parse_publish_v5(header: u8, body: &[u8]) -> Sy02Publish5 {
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
fn sy02_varint(mut bytes: &[u8]) -> Option<(usize, usize)> {
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
fn sy02_parse_connack311(body: &[u8]) -> (bool, u8) {
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
fn sy02_parse_connack5(body: &[u8]) -> (bool, u8, u16) {
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
fn sy02_parse_suback(body: &[u8]) -> (u16, Vec<u8>) {
    assert!(body.len() >= 3, "SUBACK carries a packet id plus codes");
    (u16::from_be_bytes([body[0], body[1]]), body[2..].to_vec())
}

/// One PUBLISH as the kernel delivers it to a subscribing client.
struct Sy02Publish {
    topic: String,
    packet_id: u16,
    qos: u8,
    retain: bool,
    dup: bool,
    alias: u16,
    payload: Vec<u8>,
}

/// Parse a classic-shape PUBLISH (no property section): live retained
/// replays, offline replays and every delivery toward a client that
/// negotiated no outbound alias.
fn sy02_parse_publish_classic(header: u8, body: &[u8]) -> Sy02Publish {
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
fn sy02_parse_publish_alias(header: u8, body: &[u8]) -> Sy02Publish {
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

/// SY-02: two tenants cannot see each other's messages while aliases,
/// retained messages and offline queues work in each, over real MQTT.
///
/// The kernel boots with one tenant rule (`t-${username}`) installed from
/// `indra.toml` via `TenantsConf -> TenantRegistry::replace`, and the
/// kernel listener serves real MQTT 5 clients, so CONNECTs carrying
/// username `user-a` land in one tenant and `user-b` in another.
/// In each tenant: the subscriber connects durably with an alias maximum,
/// a topic+alias publish registers the inbound alias and arrives with the
/// full topic plus an outbound alias, an alias-by-reference publish
/// resolves to the same topic, a retained publish is visible only inside
/// its own tenant, then the subscriber drops and a publish while detached
/// replays on reconnect (with the same username, so the same tenant).
/// Every client receives exactly its own tenant's payloads — the spec's
/// isolation, observed over real connections through the broker, never
/// the store.
#[tokio::test]
async fn sy02_tenants_isolated_with_aliases_and_offline() {
    // The kernel boots with one tenant rule in its config dir. Real
    // MQTT 5 clients use the kernel listener, so CONNECTs that carry
    // username `user-a` land in one tenant and `user-b` in another.
    let kernel = spawn_kernel("[[tenants.rules]]\nexpression = \"t-${username}\"\n").await;

    // One socket per client so every downlink is attributable.
    async fn dial(host: &str, port: u16) -> tokio::net::TcpStream {
        tokio::time::timeout(STEP, tokio::net::TcpStream::connect((host, port)))
            .await
            .expect("dial in time")
            .expect("dial")
    }
    // CONNECT with the tenant username over MQTT 5 (alias maximum 10),
    // returning session-present. The CONNECT is accepted (rc 0) with the
    // kernel alias bound every time.
    async fn connect_tenant(
        sock: &mut tokio::net::TcpStream,
        client_id: &str,
        clean_start: bool,
        username: &str,
    ) -> bool {
        use tokio::io::AsyncWriteExt;
        let connect = sy02_connect5(
            client_id,
            clean_start,
            60,
            Some((username, b"pw")),
            None,
            10,
        );
        tokio::time::timeout(STEP, sock.write_all(&connect))
            .await
            .expect("CONNECT in time")
            .expect("CONNECT send");
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(sock))
            .await
            .expect("CONNACK in time")
            .expect("CONNACK");
        assert_eq!(head, 0x20, "kernel answers {client_id} with CONNACK");
        let (present, reason, alias_max) = sy02_parse_connack5(&body);
        assert_eq!(reason, 0, "CONNECT of {client_id} must be accepted");
        assert_eq!(alias_max, 10, "CONNACK carries the kernel alias maximum");
        present
    }
    async fn subscribe_qos1(sock: &mut tokio::net::TcpStream, client_id: &str, filter: &str) {
        use tokio::io::AsyncWriteExt;
        // Speak MQTT 5 on a v5 socket. Options hold QoS 1
        // with retain-as-published set, so retained replay
        // keeps the retain flag like the old v4 path.
        let subscribe = sy02_subscribe_v5(1, &[(filter, 9)], 0);
        tokio::time::timeout(STEP, sock.write_all(&subscribe))
            .await
            .expect("SUBSCRIBE in time")
            .expect("SUBSCRIBE send");
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(sock))
            .await
            .expect("SUBACK in time")
            .expect("SUBACK");
        assert_eq!(head & 0xF0, 0x90, "kernel answers with SUBACK");
        assert_eq!(
            sy02_parse_suback5(&body),
            (1, vec![1]),
            "QoS 1 subscription of {client_id} to {filter} must be granted"
        );
    }
    async fn recv_packet(sock: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
        tokio::time::timeout(STEP, read_mqtt_packet(sock))
            .await
            .expect("downlink in time")
            .expect("downlink")
    }
    async fn publish_qos1(
        sock: &mut tokio::net::TcpStream,
        topic: &str,
        packet_id: u16,
        retain: bool,
        alias: Option<u16>,
        payload: &[u8],
    ) {
        use tokio::io::AsyncWriteExt;
        let publish = sy02_publish5(topic, packet_id, 1, retain, alias, payload);
        tokio::time::timeout(STEP, sock.write_all(&publish))
            .await
            .expect("publish in time")
            .expect("publish send");
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(sock))
            .await
            .expect("PUBACK in time")
            .expect("PUBACK");
        assert_eq!(head, 0x40, "publisher sees PUBACK");
        assert_eq!(
            body,
            vec![(packet_id >> 8) as u8, (packet_id & 0xff) as u8],
            "PUBACK mirrors its packet id"
        );
    }
    async fn ack(sock: &mut tokio::net::TcpStream, packet_id: u16) {
        use tokio::io::AsyncWriteExt;
        tokio::time::timeout(STEP, sock.write_all(&sy02_puback(packet_id)))
            .await
            .expect("PUBACK in time")
            .expect("PUBACK send");
    }
    async fn expect_silence(sock: &mut tokio::net::TcpStream, what: &str) {
        let extra =
            tokio::time::timeout(std::time::Duration::from_secs(2), read_mqtt_packet(sock)).await;
        assert!(extra.is_err(), "{what}");
    }

    let mut a_sub = dial("127.0.0.1", kernel.mqtt_port).await;
    let mut a_pub = dial("127.0.0.1", kernel.mqtt_port).await;
    let mut b_sub = dial("127.0.0.1", kernel.mqtt_port).await;
    let mut b_pub = dial("127.0.0.1", kernel.mqtt_port).await;

    assert!(
        !connect_tenant(&mut a_sub, "sy02-a-sub", false, "user-a").await,
        "tenant-a first CONNECT reports session_present=false"
    );
    assert!(
        !connect_tenant(&mut b_sub, "sy02-b-sub", false, "user-b").await,
        "tenant-b first CONNECT reports session_present=false"
    );
    subscribe_qos1(&mut a_sub, "sy02-a-sub", "sy02/iso").await;
    subscribe_qos1(&mut b_sub, "sy02-b-sub", "sy02/iso").await;
    connect_tenant(&mut a_pub, "sy02-a-pub", true, "user-a").await;
    connect_tenant(&mut b_pub, "sy02-b-pub", true, "user-b").await;

    // Alias leg per tenant: topic+alias registers, alias-by-reference
    // resolves, downlink carries the full topic plus an outbound alias.
    publish_qos1(&mut a_pub, "sy02/iso", 1, false, Some(3), b"a-1").await;
    publish_qos1(&mut b_pub, "sy02/iso", 1, false, Some(5), b"b-1").await;
    let (head, body) = recv_packet(&mut a_sub).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let a1 = sy02_parse_publish_alias(head, &body);
    assert_eq!(a1.topic, "sy02/iso");
    assert_eq!(a1.payload, b"a-1");
    assert!(!a1.dup, "live delivery carries no DUP");
    assert_ne!(a1.alias, 0, "downlink carries an outbound alias");
    let (head, body) = recv_packet(&mut b_sub).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let b1 = sy02_parse_publish_alias(head, &body);
    assert_eq!(b1.topic, "sy02/iso");
    assert_eq!(b1.payload, b"b-1");
    assert!(!b1.dup, "live delivery carries no DUP");
    assert_ne!(b1.alias, 0, "downlink carries an outbound alias");
    ack(&mut a_sub, a1.packet_id).await;
    ack(&mut b_sub, b1.packet_id).await;

    // Alias-by-reference in each tenant resolves to the same topic.
    publish_qos1(&mut a_pub, "", 2, false, Some(3), b"a-2").await;
    publish_qos1(&mut b_pub, "", 2, false, Some(5), b"b-2").await;
    let (head, body) = recv_packet(&mut a_sub).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let a2 = sy02_parse_publish_alias(head, &body);
    assert_eq!(a2.topic, "sy02/iso", "alias resolves in tenant-a");
    assert_eq!(a2.payload, b"a-2");
    assert_ne!(a2.alias, 0, "downlink carries an outbound alias");
    let (head, body) = recv_packet(&mut b_sub).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let b2 = sy02_parse_publish_alias(head, &body);
    assert_eq!(b2.topic, "sy02/iso", "alias resolves in tenant-b");
    assert_eq!(b2.payload, b"b-2");
    assert_ne!(b2.alias, 0, "downlink carries an outbound alias");
    ack(&mut a_sub, a2.packet_id).await;
    ack(&mut b_sub, b2.packet_id).await;

    // Retained leg: the same topic string holds one retained message
    // per tenant, and each tenant's new subscriber sees only its own.
    publish_qos1(&mut a_pub, "sy02/retained", 3, true, None, b"a-ret").await;
    publish_qos1(&mut b_pub, "sy02/retained", 3, true, None, b"b-ret").await;
    let mut a_ret = dial("127.0.0.1", kernel.mqtt_port).await;
    assert!(
        !connect_tenant(&mut a_ret, "sy02-a-ret", true, "user-a").await,
        "tenant-a retained reader connects clean"
    );
    subscribe_qos1(&mut a_ret, "sy02-a-ret", "sy02/retained").await;
    let (head, body) = recv_packet(&mut a_ret).await;
    assert_eq!(head & 0xF0, 0x30, "retained downlink is PUBLISH");
    let retained_a = sy02_parse_publish_alias(head, &body);
    assert_eq!(retained_a.topic, "sy02/retained");
    assert_eq!(
        retained_a.payload, b"a-ret",
        "tenant-a sees only its own retained message"
    );
    assert!(retained_a.retain, "retained replay carries retain");
    expect_silence(&mut a_ret, "tenant-a holds only its own retained message").await;
    let mut b_ret = dial("127.0.0.1", kernel.mqtt_port).await;
    assert!(
        !connect_tenant(&mut b_ret, "sy02-b-ret", true, "user-b").await,
        "tenant-b retained reader connects clean"
    );
    subscribe_qos1(&mut b_ret, "sy02-b-ret", "sy02/retained").await;
    let (head, body) = recv_packet(&mut b_ret).await;
    assert_eq!(head & 0xF0, 0x30, "retained downlink is PUBLISH");
    let retained_b = sy02_parse_publish_alias(head, &body);
    assert_eq!(retained_b.topic, "sy02/retained");
    assert_eq!(
        retained_b.payload, b"b-ret",
        "tenant-b sees only its own retained message"
    );
    assert!(retained_b.retain, "retained replay carries retain");
    expect_silence(&mut b_ret, "tenant-b holds only its own retained message").await;

    // Offline leg: both subscribers drop; one publish per tenant while
    // detached; each reconnect (same username, so the same tenant)
    // replays exactly its own tenant's message.
    drop(a_sub);
    drop(b_sub);
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    publish_qos1(&mut a_pub, "sy02/iso", 4, false, None, b"a-off").await;
    publish_qos1(&mut b_pub, "sy02/iso", 4, false, None, b"b-off").await;
    let mut a_re = dial("127.0.0.1", kernel.mqtt_port).await;
    let mut b_re = dial("127.0.0.1", kernel.mqtt_port).await;
    assert!(
        connect_tenant(&mut a_re, "sy02-a-sub", false, "user-a").await,
        "tenant-a resume reports session_present=true"
    );
    assert!(
        connect_tenant(&mut b_re, "sy02-b-sub", false, "user-b").await,
        "tenant-b resume reports session_present=true"
    );
    let (head, body) = recv_packet(&mut a_re).await;
    assert_eq!(head & 0xF0, 0x30, "replay is PUBLISH");
    let off_a = sy02_parse_publish_alias(head, &body);
    assert_eq!(off_a.topic, "sy02/iso");
    assert_eq!(off_a.payload, b"a-off");
    let (head, body) = recv_packet(&mut b_re).await;
    assert_eq!(head & 0xF0, 0x30, "replay is PUBLISH");
    let off_b = sy02_parse_publish_alias(head, &body);
    assert_eq!(off_b.topic, "sy02/iso");
    assert_eq!(off_b.payload, b"b-off");
    // Nothing crosses tenants: each reconnect holds exactly one frame.
    expect_silence(&mut a_re, "tenant-a holds only its own replay").await;
    expect_silence(&mut b_re, "tenant-b holds only its own replay").await;

    kernel.finish().await;
}

/// MT-07: the same client id in two tenants holds two connections.
///
/// The kernel boots with one tenant rule from `indra.toml`. Two real
/// MQTT clients connect with the same client id but different
/// usernames. Both CONNACKs succeed and both sockets stay open. A
/// publish in each tenant reaches only its own tenant. The test uses
/// the file entry and client sockets only.
#[tokio::test]
async fn mt07_same_client_id_in_two_tenants_stays_connected() {
    use tokio::io::AsyncWriteExt;
    // The kernel boots with one tenant rule in its config dir. Real
    // MQTT clients use the kernel listener, so each username lands in
    // its own tenant.
    let kernel = spawn_kernel("[[tenants.rules]]\nexpression = \"t-${username}\"\n").await;

    // Two sockets share one client id. Each carries its own username,
    // so each lands in its own tenant.
    let mut a_sock = tokio::time::timeout(
        STEP,
        tokio::net::TcpStream::connect(("127.0.0.1", kernel.mqtt_port)),
    )
    .await
    .expect("dial A in time")
    .expect("dial A");
    let mut b_sock = tokio::time::timeout(
        STEP,
        tokio::net::TcpStream::connect(("127.0.0.1", kernel.mqtt_port)),
    )
    .await
    .expect("dial B in time")
    .expect("dial B");
    for (sock, user) in [(&mut a_sock, "alpha"), (&mut b_sock, "beta")] {
        let connect = sy02_connect311("mt07-dup", true, 60, Some((user, b"pw")));
        tokio::time::timeout(STEP, sock.write_all(&connect))
            .await
            .expect("CONNECT in time")
            .expect("CONNECT send");
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(sock))
            .await
            .expect("CONNACK in time")
            .expect("CONNACK");
        assert_eq!(head, 0x20, "kernel answers {user} with CONNACK");
        assert_eq!(
            sy02_parse_connack311(&body),
            (false, 0),
            "first CONNECT of {user} is accepted"
        );
    }
    // Both tenants subscribe to the same topic string.
    for sock in [&mut a_sock, &mut b_sock] {
        let subscribe = sy02_subscribe(1, &[("mt07/dup", 1)]);
        tokio::time::timeout(STEP, sock.write_all(&subscribe))
            .await
            .expect("SUBSCRIBE in time")
            .expect("SUBSCRIBE send");
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(sock))
            .await
            .expect("SUBACK in time")
            .expect("SUBACK");
        assert_eq!(head, 0x90, "kernel answers with SUBACK");
        assert_eq!(sy02_parse_suback(&body), (1, vec![1]));
    }
    // One publisher per tenant.
    let mut a_pub = tokio::time::timeout(
        STEP,
        tokio::net::TcpStream::connect(("127.0.0.1", kernel.mqtt_port)),
    )
    .await
    .expect("dial A pub in time")
    .expect("dial A pub");
    let mut b_pub = tokio::time::timeout(
        STEP,
        tokio::net::TcpStream::connect(("127.0.0.1", kernel.mqtt_port)),
    )
    .await
    .expect("dial B pub in time")
    .expect("dial B pub");
    for (sock, id, user) in [
        (&mut a_pub, "mt07-a-pub", "alpha"),
        (&mut b_pub, "mt07-b-pub", "beta"),
    ] {
        let connect = sy02_connect311(id, true, 60, Some((user, b"pw")));
        tokio::time::timeout(STEP, sock.write_all(&connect))
            .await
            .expect("pub CONNECT in time")
            .expect("pub CONNECT send");
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(sock))
            .await
            .expect("pub CONNACK in time")
            .expect("pub CONNACK");
        assert_eq!(head, 0x20, "kernel answers the publisher with CONNACK");
        assert_eq!(sy02_parse_connack311(&body), (false, 0));
    }
    // A publish in tenant A reaches A only. B stays silent.
    let publish = sy02_publish311("mt07/dup", 1, 1, false, b"from-a");
    tokio::time::timeout(STEP, a_pub.write_all(&publish))
        .await
        .expect("publish in time")
        .expect("publish send");
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut a_pub))
        .await
        .expect("PUBACK in time")
        .expect("PUBACK");
    assert_eq!(head, 0x40, "publisher sees PUBACK");
    assert_eq!(body, vec![0, 1], "PUBACK mirrors its packet id");
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut a_sock))
        .await
        .expect("A downlink in time")
        .expect("A downlink");
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let downlink = sy02_parse_publish_classic(head, &body);
    assert_eq!(downlink.topic, "mt07/dup");
    assert_eq!(downlink.payload, b"from-a");
    tokio::time::timeout(STEP, a_sock.write_all(&sy02_puback(downlink.packet_id)))
        .await
        .expect("PUBACK in time")
        .expect("PUBACK send");
    let silent = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_mqtt_packet(&mut b_sock),
    )
    .await;
    assert!(silent.is_err(), "B holds none of A's publish");
    // A publish in tenant B reaches B only. A stays silent.
    let publish = sy02_publish311("mt07/dup", 2, 1, false, b"from-b");
    tokio::time::timeout(STEP, b_pub.write_all(&publish))
        .await
        .expect("publish in time")
        .expect("publish send");
    let (head, _) = tokio::time::timeout(STEP, read_mqtt_packet(&mut b_pub))
        .await
        .expect("PUBACK in time")
        .expect("PUBACK");
    assert_eq!(head, 0x40, "publisher sees PUBACK");
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut b_sock))
        .await
        .expect("B downlink in time")
        .expect("B downlink");
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let downlink = sy02_parse_publish_classic(head, &body);
    assert_eq!(downlink.topic, "mt07/dup");
    assert_eq!(downlink.payload, b"from-b");
    tokio::time::timeout(STEP, b_sock.write_all(&sy02_puback(downlink.packet_id)))
        .await
        .expect("PUBACK in time")
        .expect("PUBACK send");
    let silent = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_mqtt_packet(&mut a_sock),
    )
    .await;
    assert!(silent.is_err(), "A holds none of B's publish");

    kernel.finish().await;
}

/// TM-09: tenant in client and subscription reads, through real
/// sockets and the real API.
///
/// The kernel boots from `indra.toml` with one tenant rule and its own
/// MQTT listener, so no edge is needed. Two tenants connect the same
/// client id. Each tenant publishes a random value the broker cannot
/// know. Delivery stays in-tenant. The API then shows each tenant its
/// own rows only, and the default-tenant reads keep their shapes.
#[tokio::test]
async fn tm09_tenant_client_reads_through_sockets_and_api() {
    use tokio::io::AsyncWriteExt;

    let tag = format!(
        "tm09-reads-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let scratch = std::env::temp_dir().join(format!("indramqtt-{tag}"));
    let config_dir = std::env::temp_dir().join(format!("indramqtt-{tag}-cfg"));
    std::fs::create_dir_all(&config_dir).expect("create temp config dir");

    let bl_port = ephemeral_port();
    let mqtt_port = ephemeral_port();
    let api_port = ephemeral_port();
    std::fs::write(
        config_dir.join("indra.toml"),
        format!(
            "[node]\n\
             id = \"tm09-e2e\"\n\
             data_dir = \"{}\"\n\
             brokerlink_bind = \"127.0.0.1:{bl_port}\"\n\
             \n\
             [listeners.tcp]\n\
              bind = \"127.0.0.1:{mqtt_port}\"\n\
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
              \n\
              [[tenants.rules]]\n\
              expression = \"t-${{username}}\"\n",
            scratch.display(),
        ),
    )
    .expect("write indra.toml");

    // Operator key: 21 chars, above the 16-char minimum. Bearer auth
    // opens the management API with no user setup.
    let api_key = "tm09-e2e-test-key-0001";
    let mut child = tokio::process::Command::new(shard_kernel_binary())
        .arg("--config-dir")
        .arg(&config_dir)
        .arg("--allow-anonymous")
        .env("INDRA_API_KEYS", api_key)
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
            std::time::Duration::from_secs(2),
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
                    break;
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill().await;
            panic!("tm09 kernel never opened API {api_port} and MQTT {mqtt_port}");
        }
        if let Ok(Some(status)) = child.try_wait() {
            panic!("tm09 kernel exited during boot: {status}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    async fn dial(port: u16) -> tokio::net::TcpStream {
        tokio::time::timeout(STEP, tokio::net::TcpStream::connect(("127.0.0.1", port)))
            .await
            .expect("dial in time")
            .expect("dial")
    }
    async fn connect_user(sock: &mut tokio::net::TcpStream, client_id: &str, username: &str) {
        tokio::time::timeout(
            STEP,
            sock.write_all(&sy02_connect311(
                client_id,
                true,
                60,
                Some((username, b"pw")),
            )),
        )
        .await
        .expect("CONNECT in time")
        .expect("CONNECT send");
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(sock))
            .await
            .expect("CONNACK in time")
            .expect("CONNACK");
        assert_eq!(head, 0x20, "kernel answers {client_id} with CONNACK");
        assert_eq!(body, vec![0x00, 0x00], "CONNECT of {client_id} is accepted");
    }
    async fn subscribe(sock: &mut tokio::net::TcpStream, filter: &str) {
        tokio::time::timeout(STEP, sock.write_all(&sy02_subscribe(1, &[(filter, 0)])))
            .await
            .expect("SUBSCRIBE in time")
            .expect("SUBSCRIBE send");
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(sock))
            .await
            .expect("SUBACK in time")
            .expect("SUBACK");
        assert_eq!(head & 0xF0, 0x90, "kernel answers with SUBACK");
        assert_eq!(body, vec![0x00, 0x01, 0x00], "subscribe is granted");
    }

    // Same client id in two tenants. Both connects succeed.
    let mut a_sub = dial(mqtt_port).await;
    let mut b_sub = dial(mqtt_port).await;
    let mut a_pub = dial(mqtt_port).await;
    let mut b_pub = dial(mqtt_port).await;
    connect_user(&mut a_sub, "tm09-dup", "tm09-a").await;
    connect_user(&mut b_sub, "tm09-dup", "tm09-b").await;
    subscribe(&mut a_sub, "tm09/check").await;
    subscribe(&mut b_sub, "tm09/check").await;
    connect_user(&mut a_pub, "tm09-a-pub", "tm09-a").await;
    connect_user(&mut b_pub, "tm09-b-pub", "tm09-b").await;

    // Random values the broker cannot know. Delivery stays in-tenant.
    let secret_a = format!("tm09-{}-a-{}", std::process::id(), rand::random::<u64>());
    let secret_b = format!("tm09-{}-b-{}", std::process::id(), rand::random::<u64>());
    tokio::time::timeout(
        STEP,
        a_pub.write_all(&sy02_publish311(
            "tm09/check",
            0,
            0,
            false,
            secret_a.as_bytes(),
        )),
    )
    .await
    .expect("publish in time")
    .expect("publish send");
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut a_sub))
        .await
        .expect("downlink in time")
        .expect("downlink");
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let own = sy02_parse_publish_classic(head, &body);
    assert_eq!(own.topic, "tm09/check");
    assert_eq!(own.payload, secret_a.as_bytes());
    let silent = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_mqtt_packet(&mut b_sub),
    )
    .await;
    assert!(silent.is_err(), "tenant B holds none of tenant A publish");

    tokio::time::timeout(
        STEP,
        b_pub.write_all(&sy02_publish311(
            "tm09/check",
            0,
            0,
            false,
            secret_b.as_bytes(),
        )),
    )
    .await
    .expect("publish in time")
    .expect("publish send");
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut b_sub))
        .await
        .expect("downlink in time")
        .expect("downlink");
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let own = sy02_parse_publish_classic(head, &body);
    assert_eq!(own.payload, secret_b.as_bytes());
    let silent = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_mqtt_packet(&mut a_sub),
    )
    .await;
    assert!(silent.is_err(), "tenant A holds none of tenant B publish");

    // Reads through the API. Each tenant filter shows its rows only.
    async fn api_get(
        http: &reqwest::Client,
        base: &str,
        key: &str,
        path: &str,
    ) -> (u16, serde_json::Value) {
        let res = tokio::time::timeout(
            STEP,
            http.get(format!("{base}{path}")).bearer_auth(key).send(),
        )
        .await
        .expect("API in time")
        .expect("API send");
        let status = res.status().as_u16();
        let body: serde_json::Value = tokio::time::timeout(STEP, res.json())
            .await
            .expect("body in time")
            .expect("body is JSON");
        (status, body)
    }
    for tenant in ["t-tm09-a", "t-tm09-b"] {
        let (status, body) = api_get(
            &http,
            &base,
            api_key,
            &format!("/api/v5/clients?tenant={tenant}"),
        )
        .await;
        assert_eq!(status, 200, "client list of {tenant}");
        let data = body["data"].as_array().expect("client list has data");
        assert!(!data.is_empty(), "{tenant} keeps its rows");
        for row in data {
            assert_eq!(row["mountpoint"], serde_json::json!(tenant));
        }
        assert!(
            data.iter().any(|row| row["clientid"] == "tm09-dup"),
            "{tenant} shows its namesake"
        );
    }
    let (status, body) = api_get(&http, &base, api_key, "/api/v5/clients").await;
    assert_eq!(status, 200);
    let data = body["data"].as_array().expect("client list has data");
    let mut mounts: Vec<&str> = data
        .iter()
        .filter(|row| row["clientid"] == "tm09-dup")
        .map(|row| row["mountpoint"].as_str().expect("row carries mountpoint"))
        .collect();
    mounts.sort_unstable();
    assert_eq!(mounts, vec!["t-tm09-a", "t-tm09-b"]);

    // The bare detail read keeps the default tenant, so it misses
    // here instead of leaking another tenant session.
    let (status, _) = api_get(&http, &base, api_key, "/api/v5/clients/tm09-dup").await;
    assert_eq!(status, 404);
    let (status, body) = api_get(
        &http,
        &base,
        api_key,
        "/api/v5/clients/tm09-dup?tenant=t-tm09-a",
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["mountpoint"], serde_json::json!("t-tm09-a"));

    // Subscription reads scope the same way with the current shape.
    let (status, body) = api_get(
        &http,
        &base,
        api_key,
        "/api/v5/subscriptions?tenant=t-tm09-a",
    )
    .await;
    assert_eq!(status, 200);
    let rows = body.as_array().expect("subscription list is an array");
    assert!(!rows.is_empty(), "tenant A keeps its subscriptions");
    for row in rows {
        assert!(row.get("tenant").is_none());
    }
    assert!(
        rows.iter().any(|row| row["topic"] == "tm09/check"),
        "tenant A shows its subscription"
    );
    let (status, _) = api_get(&http, &base, api_key, "/api/v5/topics").await;
    assert_eq!(status, 200);
    let (status, _) = api_get(&http, &base, api_key, "/api/v5/sessions_count").await;
    assert_eq!(status, 200);

    let _ = child.kill().await;
    let _ = std::fs::remove_dir_all(&scratch);
    let _ = std::fs::remove_dir_all(&config_dir);
}

/// Built-in rule actions of the reference API, through real sockets and
/// the real management API.
///
/// The kernel boots from `indra.toml` with its own MQTT listener, so no
/// edge is needed. Each rule is created with `POST /api/v5/rules`. Real
/// MQTT clients publish, and other clients receive on the republish
/// topic. Every value is random, so the broker cannot know it.
#[tokio::test]
async fn rule_builtin_actions_republish_and_console_through_the_broker() {
    use tokio::io::AsyncWriteExt;

    const QUIET: std::time::Duration = std::time::Duration::from_millis(1500);
    let tag = format!(
        "rb-actions-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let scratch = std::env::temp_dir().join(format!("indramqtt-{tag}"));
    let config_dir = std::env::temp_dir().join(format!("indramqtt-{tag}-cfg"));
    let kernel_log = std::env::temp_dir().join(format!("indramqtt-{tag}.log"));
    std::fs::create_dir_all(&config_dir).expect("create temp config dir");

    let bl_port = ephemeral_port();
    let mqtt_port = ephemeral_port();
    let api_port = ephemeral_port();
    std::fs::write(
        config_dir.join("indra.toml"),
        format!(
            "[node]\n\
             id = \"rb-e2e\"\n\
             data_dir = \"{}\"\n\
             brokerlink_bind = \"127.0.0.1:{bl_port}\"\n\
             \n\
             [listeners.tcp]\n\
              bind = \"127.0.0.1:{mqtt_port}\"\n\
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
              level = \"info\"\n\
              \n\
              [[tenants.rules]]\n\
              expression = \"t-${{username}}\"\n",
            scratch.display(),
        ),
    )
    .expect("write indra.toml");

    let api_key = "rb-e2e-test-key-0001";
    let log_file = std::fs::File::create(&kernel_log).expect("create kernel log");
    let mut child = tokio::process::Command::new(shard_kernel_binary())
        .arg("--config-dir")
        .arg(&config_dir)
        .arg("--allow-anonymous")
        .env("INDRA_API_KEYS", api_key)
        .env("NO_COLOR", "1")
        .stdout(log_file.try_clone().expect("clone log handle"))
        .stderr(log_file)
        .kill_on_drop(true)
        .spawn()
        .expect("spawn real kernel");
    let http = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{api_port}");
    let deadline = std::time::Instant::now() + BOOT;
    loop {
        let up = tokio::time::timeout(
            std::time::Duration::from_secs(2),
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
                    break;
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill().await;
            panic!("rule actions kernel never opened API {api_port} and MQTT {mqtt_port}");
        }
        if let Ok(Some(status)) = child.try_wait() {
            panic!("rule actions kernel exited during boot: {status}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    async fn api(
        http: &reqwest::Client,
        base: &str,
        key: &str,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> (u16, serde_json::Value) {
        let mut request = http
            .request(method, format!("{base}{path}"))
            .bearer_auth(key);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let res = tokio::time::timeout(STEP, request.send())
            .await
            .expect("API in time")
            .expect("API send");
        let status = res.status().as_u16();
        let text = res.text().await.expect("API body");
        let value = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
        (status, value)
    }
    async fn dial(port: u16) -> tokio::net::TcpStream {
        tokio::time::timeout(STEP, tokio::net::TcpStream::connect(("127.0.0.1", port)))
            .await
            .expect("dial in time")
            .expect("dial")
    }
    async fn connect(port: u16, client_id: &str, username: Option<&str>) -> tokio::net::TcpStream {
        let mut sock = dial(port).await;
        let creds = username.map(|name| (name, &b"pw"[..]));
        tokio::time::timeout(
            STEP,
            sock.write_all(&sy02_connect311(client_id, true, 60, creds)),
        )
        .await
        .expect("CONNECT in time")
        .expect("CONNECT send");
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock))
            .await
            .expect("CONNACK in time")
            .expect("CONNACK");
        assert_eq!(head, 0x20, "kernel answers {client_id} with CONNACK");
        assert_eq!(body, vec![0x00, 0x00], "CONNECT of {client_id} is accepted");
        sock
    }
    async fn subscribe(sock: &mut tokio::net::TcpStream, filter: &str, qos: u8) {
        tokio::time::timeout(STEP, sock.write_all(&sy02_subscribe(1, &[(filter, qos)])))
            .await
            .expect("SUBSCRIBE in time")
            .expect("SUBSCRIBE send");
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(sock))
            .await
            .expect("SUBACK in time")
            .expect("SUBACK");
        assert_eq!(head & 0xF0, 0x90, "kernel answers with SUBACK");
        assert_eq!(body, vec![0x00, 0x01, qos], "subscribe to {filter} granted");
    }
    async fn publish(sock: &mut tokio::net::TcpStream, topic: &str, qos: u8, payload: &[u8]) {
        tokio::time::timeout(
            STEP,
            sock.write_all(&sy02_publish311(topic, 7, qos, false, payload)),
        )
        .await
        .expect("publish in time")
        .expect("publish send");
        if qos > 0 {
            let (head, _) = tokio::time::timeout(STEP, read_mqtt_packet(sock))
                .await
                .expect("PUBACK in time")
                .expect("PUBACK");
            assert_eq!(head, 0x40, "publisher sees PUBACK");
        }
    }
    /// The next PUBLISH on the socket, or `None` when nothing comes in
    /// the given time.
    async fn next_publish(
        sock: &mut tokio::net::TcpStream,
        wait: std::time::Duration,
    ) -> Option<Sy02Publish> {
        let (head, body) = tokio::time::timeout(wait, read_mqtt_packet(sock))
            .await
            .ok()?
            .expect("downlink");
        assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
        Some(sy02_parse_publish_classic(head, &body))
    }
    let nonce = || format!("{:016x}", rand::random::<u64>());

    // Two tenants own rules; clients with these usernames land there.
    for ns in ["t-rb-a", "t-rb-b"] {
        let (status, body) = api(
            &http,
            &base,
            api_key,
            reqwest::Method::POST,
            &format!("/api/v5/mt/ns/{ns}"),
            None,
        )
        .await;
        assert!(status < 300, "namespace {ns} answered {status}: {body}");
    }
    let make_rule = |name: &str, sql: &str, actions: serde_json::Value| serde_json::json!({"id": name, "name": name, "sql": sql, "actions": actions});

    // 1. A fixed topic and the default `${payload}`. The republish topic
    // matches the filter of the rule itself: a loop would deliver more
    // than one copy.
    let (status, body) = api(
        &http,
        &base,
        api_key,
        reqwest::Method::POST,
        "/api/v5/rules",
        Some(make_rule(
            "rb-fixed",
            "SELECT * FROM \"rb/one/#\"",
            serde_json::json!([
                {"function": "republish", "args": {"topic": "rb/one/out", "payload": "${payload}"}}
            ]),
        )),
    )
    .await;
    assert_eq!(status, 201, "create answered {body}");
    assert_eq!(body["actions"][0]["function"], "republish");
    let mut sub_one = connect(mqtt_port, "rb-sub-one", None).await;
    subscribe(&mut sub_one, "rb/one/out", 0).await;
    let mut pubr = connect(mqtt_port, "rb-pub", None).await;
    let value = nonce();
    let message = format!("{{\"v\":\"{value}\"}}");
    publish(&mut pubr, "rb/one/in", 0, message.as_bytes()).await;
    let got = next_publish(&mut sub_one, STEP).await.expect("republish");
    assert_eq!(got.topic, "rb/one/out");
    assert_eq!(got.payload, message.as_bytes());
    assert!(
        next_publish(&mut sub_one, QUIET).await.is_none(),
        "one input gives one output: a republish does not start its own rule again"
    );

    // 2. A payload template with selected fields and a topic template.
    let (status, body) = api(
        &http,
        &base,
        api_key,
        reqwest::Method::POST,
        "/api/v5/rules",
        Some(make_rule(
            "rb-template",
            "SELECT temp, dev FROM \"rb/two/+\"",
            serde_json::json!([
                {"function": "republish",
                 "args": {"topic": "rb/two/out/${dev}", "payload": "temp=${temp};dev=${dev}"}}
            ]),
        )),
    )
    .await;
    assert_eq!(status, 201, "create answered {body}");
    let mut sub_two = connect(mqtt_port, "rb-sub-two", None).await;
    subscribe(&mut sub_two, "rb/two/out/#", 0).await;
    let dev = nonce();
    let temp = rand::random::<u32>();
    let message = format!("{{\"temp\":{temp},\"dev\":\"{dev}\",\"noise\":\"x\"}}");
    publish(&mut pubr, "rb/two/in", 0, message.as_bytes()).await;
    let got = next_publish(&mut sub_two, STEP).await.expect("template");
    assert_eq!(got.topic, format!("rb/two/out/{dev}"));
    assert_eq!(got.payload, format!("temp={temp};dev={dev}").as_bytes());

    // 3. A WHERE clause filters.
    let (status, body) = api(
        &http,
        &base,
        api_key,
        reqwest::Method::POST,
        "/api/v5/rules",
        Some(make_rule(
            "rb-where",
            "SELECT * FROM \"rb/three\" WHERE n > 5",
            serde_json::json!([{"function": "republish", "args": {"topic": "rb/three/out"}}]),
        )),
    )
    .await;
    assert_eq!(status, 201, "create answered {body}");
    let mut sub_three = connect(mqtt_port, "rb-sub-three", None).await;
    subscribe(&mut sub_three, "rb/three/out", 0).await;
    publish(&mut pubr, "rb/three", 0, br#"{"n":1,"w":"low"}"#).await;
    assert!(
        next_publish(&mut sub_three, QUIET).await.is_none(),
        "WHERE n > 5 drops n = 1"
    );
    let high = format!("{{\"n\":9,\"w\":\"{}\"}}", nonce());
    publish(&mut pubr, "rb/three", 0, high.as_bytes()).await;
    let got = next_publish(&mut sub_three, STEP).await.expect("where");
    assert_eq!(got.payload, high.as_bytes());

    // 4. QoS: the default follows the input message, a number is fixed,
    // and a template reads a field.
    let (status, body) = api(
        &http,
        &base,
        api_key,
        reqwest::Method::POST,
        "/api/v5/rules",
        Some(make_rule(
            "rb-qos",
            "SELECT * FROM \"rb/four\"",
            serde_json::json!([
                {"function": "republish", "args": {"topic": "rb/four/follow"}},
                {"function": "republish", "args": {"topic": "rb/four/fixed", "qos": 1}},
                {"function": "republish", "args": {"topic": "rb/four/field", "qos": "${lvl}"}}
            ]),
        )),
    )
    .await;
    assert_eq!(status, 201, "create answered {body}");
    let mut sub_four = connect(mqtt_port, "rb-sub-four", None).await;
    subscribe(&mut sub_four, "rb/four/+", 2).await;
    for (input_qos, expect) in [(1u8, [1u8, 1, 2]), (0u8, [0u8, 1, 2])] {
        let message = format!("{{\"lvl\":2,\"id\":\"{}\"}}", nonce());
        publish(&mut pubr, "rb/four", input_qos, message.as_bytes()).await;
        let mut seen = std::collections::HashMap::new();
        for _ in 0..3 {
            let got = next_publish(&mut sub_four, STEP).await.expect("qos output");
            if got.qos == 1 {
                tokio::time::timeout(STEP, sub_four.write_all(&sy02_puback(got.packet_id)))
                    .await
                    .expect("PUBACK in time")
                    .expect("PUBACK send");
            }
            seen.insert(got.topic.clone(), got.qos);
        }
        assert_eq!(
            seen.len(),
            3,
            "three outputs for input QoS {input_qos}: {seen:?}"
        );
        assert_eq!(
            seen["rb/four/follow"], expect[0],
            "follows input QoS {input_qos}"
        );
        assert_eq!(seen["rb/four/fixed"], expect[1], "fixed QoS 1");
        assert_eq!(seen["rb/four/field"], expect[2], "QoS from the field `lvl`");
    }

    // 5. A retained republish is kept for later subscribers.
    let (status, body) = api(
        &http,
        &base,
        api_key,
        reqwest::Method::POST,
        "/api/v5/rules",
        Some(make_rule(
            "rb-retain",
            "SELECT * FROM \"rb/five\"",
            serde_json::json!([
                {"function": "republish", "args": {"topic": "rb/five/kept", "retain": true}}
            ]),
        )),
    )
    .await;
    assert_eq!(status, 201, "create answered {body}");
    let kept = format!("{{\"keep\":\"{}\"}}", nonce());
    publish(&mut pubr, "rb/five", 0, kept.as_bytes()).await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let mut late = connect(mqtt_port, "rb-late", None).await;
    subscribe(&mut late, "rb/five/kept", 0).await;
    let got = next_publish(&mut late, STEP)
        .await
        .expect("retained replay");
    assert_eq!(got.payload, kept.as_bytes());
    assert!(got.retain, "the replay carries the retain flag");

    // 6. A rule of tenant A republishes inside tenant A only.
    let (status, body) = api(
        &http,
        &base,
        api_key,
        reqwest::Method::POST,
        "/api/v5/rules",
        Some(serde_json::json!({
            "name": "rb-tenant-a",
            "tenant": "t-rb-a",
            "sql": "SELECT * FROM \"rb/six\"",
            "actions": [{"function": "republish", "args": {"topic": "rb/six/out"}}]
        })),
    )
    .await;
    assert_eq!(status, 201, "create answered {body}");
    let mut a_sub = connect(mqtt_port, "rb-a-sub", Some("rb-a")).await;
    let mut a_pub = connect(mqtt_port, "rb-a-pub", Some("rb-a")).await;
    let mut b_sub = connect(mqtt_port, "rb-b-sub", Some("rb-b")).await;
    let mut b_pub = connect(mqtt_port, "rb-b-pub", Some("rb-b")).await;
    let mut default_sub = connect(mqtt_port, "rb-d-sub", None).await;
    for sock in [&mut a_sub, &mut b_sub, &mut default_sub] {
        subscribe(sock, "rb/six/out", 0).await;
    }
    let secret = nonce();
    let message = format!("{{\"s\":\"{secret}\"}}");
    publish(&mut a_pub, "rb/six", 0, message.as_bytes()).await;
    let got = next_publish(&mut a_sub, STEP)
        .await
        .expect("tenant A output");
    assert_eq!(got.payload, message.as_bytes());
    assert!(
        next_publish(&mut b_sub, QUIET).await.is_none(),
        "tenant B holds none of the output of a rule of tenant A"
    );
    assert!(
        next_publish(&mut default_sub, std::time::Duration::from_millis(300))
            .await
            .is_none(),
        "the default tenant holds none of the output of a rule of tenant A"
    );
    // A publish of tenant B on the same topic does not fire the rule.
    publish(&mut b_pub, "rb/six", 0, message.as_bytes()).await;
    assert!(
        next_publish(&mut a_sub, QUIET).await.is_none(),
        "a publish of tenant B does not fire a rule of tenant A"
    );
    assert!(
        next_publish(&mut b_sub, std::time::Duration::from_millis(300))
            .await
            .is_none(),
        "a publish of tenant B gives no output in tenant B"
    );

    // 7. MQTT 5 properties: user properties from a result field and the
    // message expiry from the arguments.
    let (status, body) = api(
        &http,
        &base,
        api_key,
        reqwest::Method::POST,
        "/api/v5/rules",
        Some(make_rule(
            "rb-props",
            "SELECT * FROM \"rb/seven\"",
            serde_json::json!([
                {"function": "republish",
                 "args": {"topic": "rb/seven/out", "user_properties": "${props}",
                          "mqtt_properties": {"Message-Expiry-Interval": "120"}}}
            ]),
        )),
    )
    .await;
    assert_eq!(status, 201, "create answered {body}");
    let mut v5_sub = dial(mqtt_port).await;
    tokio::time::timeout(
        STEP,
        v5_sub.write_all(&sy02_connect5("rb-v5-sub", true, 60, None, None, 0)),
    )
    .await
    .expect("CONNECT in time")
    .expect("CONNECT send");
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut v5_sub))
        .await
        .expect("CONNACK in time")
        .expect("CONNACK");
    assert_eq!(head, 0x20);
    assert_eq!(body[1], 0, "v5 CONNECT accepted");
    tokio::time::timeout(
        STEP,
        v5_sub.write_all(&sy02_subscribe_v5(1, &[("rb/seven/out", 0)], 0)),
    )
    .await
    .expect("SUBSCRIBE in time")
    .expect("SUBSCRIBE send");
    let (head, _) = tokio::time::timeout(STEP, read_mqtt_packet(&mut v5_sub))
        .await
        .expect("SUBACK in time")
        .expect("SUBACK");
    assert_eq!(head & 0xF0, 0x90);
    let prop_value = nonce();
    let message = format!("{{\"props\":{{\"trace\":\"{prop_value}\"}},\"body\":1}}");
    publish(&mut pubr, "rb/seven", 0, message.as_bytes()).await;
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut v5_sub))
        .await
        .expect("v5 downlink in time")
        .expect("v5 downlink");
    let parsed = sy02_parse_publish_v5(head, &body);
    assert_eq!(parsed.topic, "rb/seven/out");
    assert_eq!(parsed.expiry, 120);
    assert_eq!(parsed.users, vec![("trace".to_string(), prop_value)]);

    // 8. The console function writes the result to the log, on one line,
    // with a size limit.
    let (status, body) = api(
        &http,
        &base,
        api_key,
        reqwest::Method::POST,
        "/api/v5/rules",
        Some(make_rule(
            "rb-console",
            "SELECT * FROM \"rb/eight\"",
            serde_json::json!([{"function": "console"}]),
        )),
    )
    .await;
    assert_eq!(status, 201, "create answered {body}");
    let seen_value = nonce();
    publish(
        &mut pubr,
        "rb/eight",
        0,
        format!("{{\"seen\":\"{seen_value}\"}}").as_bytes(),
    )
    .await;
    let big_mark = nonce();
    let big = format!(
        "{{\"mark\":\"{big_mark}\",\"pad\":\"{}\"}}",
        "x".repeat(4000)
    );
    publish(&mut pubr, "rb/eight", 0, big.as_bytes()).await;
    let mut console_lines = Vec::new();
    for _ in 0..50 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let text = std::fs::read_to_string(&kernel_log).unwrap_or_default();
        console_lines = text
            .lines()
            .filter(|line| line.contains("rule console"))
            .map(str::to_string)
            .collect();
        if console_lines.iter().any(|l| l.contains(&seen_value))
            && console_lines.iter().any(|l| l.contains(&big_mark))
        {
            break;
        }
    }
    assert!(
        console_lines.iter().any(|l| l.contains(&seen_value)),
        "the console line holds the result: {console_lines:?}"
    );
    let big_line = console_lines
        .iter()
        .find(|l| l.contains(&big_mark))
        .expect("console line of the big result");
    assert!(
        !big_line.contains(&"x".repeat(2000)),
        "the console line is cut at the size limit"
    );
    assert!(big_line.contains("..."), "a cut line ends with dots");

    // 9. A user function is refused with 400 and nothing is stored.
    let (status, body) = api(
        &http,
        &base,
        api_key,
        reqwest::Method::POST,
        "/api/v5/rules",
        Some(make_rule(
            "rb-user-fn",
            "SELECT * FROM \"rb/nine\"",
            serde_json::json!([{"function": "my_module:my_function", "args": {}}]),
        )),
    )
    .await;
    assert_eq!(status, 400, "user function answered {body}");
    assert_eq!(body["code"], "BAD_REQUEST");
    let (status, _) = api(
        &http,
        &base,
        api_key,
        reqwest::Method::GET,
        "/api/v5/rules/rb-user-fn",
        None,
    )
    .await;
    assert_eq!(status, 404, "the refused rule was not stored");

    let _ = child.kill().await;
    let _ = std::fs::remove_dir_all(&scratch);
    let _ = std::fs::remove_dir_all(&config_dir);
    let _ = std::fs::remove_file(&kernel_log);
}
