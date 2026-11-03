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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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

/// Counter for kernel boot tags. Parallel boots never share dirs.
static KERNEL_TAG_SLOT: AtomicU64 = AtomicU64::new(0);

/// Boots one real kernel with the kernel listener on ephemeral ports.
///
/// `extra_toml` appends to `indra.toml` (tenant rules, session limits).
/// TCP, WebSocket and API get explicit loopback ports. TLS and secure
/// WebSocket stay disabled. The link bind gets an explicit loopback port
/// so parallel tests never share the default port. The call waits for
/// `/healthz` plus the MQTT and WebSocket ports. The child dies with the
/// test (`kill_on_drop`).
async fn spawn_kernel(extra_toml: &str) -> Kernel {
    // Boot up to three times. A parallel boot sometimes takes a probed
    // port first, and the loser exits during boot. Each attempt uses
    // fresh ports and fresh dirs. A real boot defect fails each attempt
    // and still panics.
    let mut attempt = 0;
    loop {
        attempt += 1;
        match try_spawn_kernel(extra_toml).await {
            Ok(kernel) => return kernel,
            Err(boot) if boot.exited && attempt < 3 => continue,
            Err(boot) => panic!("{}", boot.message),
        }
    }
}

/// One kernel boot failure. `exited` marks an early child exit.
struct BootFailure {
    exited: bool,
    message: String,
}

/// Boots one kernel once. Returns the kernel on success. Returns the
/// failure on a timeout or an early child exit. The caller retries an
/// early exit and panics otherwise.
async fn try_spawn_kernel(extra_toml: &str) -> Result<Kernel, BootFailure> {
    let slot = KERNEL_TAG_SLOT.fetch_add(1, Ordering::SeqCst);
    let tag = format!(
        "p6-{}-{:?}-{}-{slot}",
        std::process::id(),
        std::thread::current().id(),
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
            let _ = std::fs::remove_dir_all(&scratch);
            let _ = std::fs::remove_dir_all(&config_dir);
            return Err(BootFailure {
                exited: false,
                message: format!("kernel never opened API {api_port} and MQTT {mqtt_port}"),
            });
        }
        if let Ok(Some(status)) = child.try_wait() {
            let _ = std::fs::remove_dir_all(&scratch);
            let _ = std::fs::remove_dir_all(&config_dir);
            return Err(BootFailure {
                exited: true,
                message: format!("kernel exited during boot: {status}"),
            });
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    Ok(Kernel {
        child,
        mqtt_port,
        ws_port,
        api_port,
        scratch,
        config_dir,
    })
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

/// Send a management request to a real broker and retain its JSON order.
async fn mv00_api(
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

async fn mv00_connect(kernel: &Kernel, id: &str) -> tokio::net::TcpStream {
    let mut socket = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(&mut socket, &mqtt_connect_packet(id)).await;
    let (head, body) = recv_mqtt(&mut socket).await;
    assert_eq!(head, 0x20);
    assert_eq!(sy02_parse_connack311(&body), (false, 0));
    socket
}

async fn mv00_subscribe(socket: &mut tokio::net::TcpStream) {
    send_mqtt(socket, &sy02_subscribe(1, &[("mv00/out/#", 0)])).await;
    let (head, body) = recv_mqtt(socket).await;
    assert_eq!(head, 0x90);
    assert_eq!(sy02_parse_suback(&body), (1, vec![0]));
}

fn mv00_rule(id: &str, sql: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "name": id,
        "sql": sql,
        "actions": [{"function": "republish", "args": {"topic": format!("mv00/out/{id}")}}],
    })
}

async fn mv00_publish_outputs(
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

async fn mv00_assert_publish(
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

/// A real MQTT input and the HTTP tester use the same ordered result.
#[tokio::test]
async fn mv00_projection_order_through_mqtt_and_http() {
    let kernel = spawn_kernel("").await;
    let mut subscriber = mv00_connect(&kernel, "mv00-sub").await;
    mv00_subscribe(&mut subscriber).await;
    let mut publisher = mv00_connect(&kernel, "mv00-pub").await;
    let cases: &[(&str, &str, &str, &[&str])] = &[
        (
            "explicit",
            "SELECT b, a, c",
            r#"{"c":3,"a":1,"b":2}"#,
            &[r#"{"b":2,"a":1,"c":3}"#],
        ),
        (
            "wildcard",
            "SELECT *",
            r#"{"c":3,"a":1,"b":2}"#,
            &[r#"{"c":3,"a":1,"b":2}"#],
        ),
        (
            "mixed",
            "SELECT c AS first, *, a AS last",
            r#"{"c":3,"a":1,"b":2}"#,
            &[r#"{"first":3,"c":3,"a":1,"b":2,"last":1}"#],
        ),
        (
            "mixed-duplicate",
            "SELECT 99 AS a, *",
            r#"{"b":2,"a":1,"c":3}"#,
            &[r#"{"a":1,"b":2,"c":3}"#],
        ),
        (
            "alias",
            "SELECT b AS x, a",
            r#"{"a":1,"b":2}"#,
            &[r#"{"x":2,"a":1}"#],
        ),
        (
            "duplicate",
            "SELECT b AS x, a AS y, c AS x",
            r#"{"a":1,"b":2,"c":3}"#,
            &[r#"{"x":3,"y":1}"#],
        ),
        (
            "nested",
            "SELECT obj, b",
            r#"{"b":3,"obj":{"z":1,"a":2}}"#,
            &[r#"{"obj":{"z":1,"a":2},"b":3}"#],
        ),
        (
            "shadow",
            "SELECT *",
            r#"{"topic":"payload-topic","qos":2,"b":3}"#,
            &[r#"{"topic":"payload-topic","qos":2,"b":3}"#],
        ),
        (
            "shadow-payload",
            "SELECT payload AS shadow, b",
            r#"{"payload":{"z":1,"a":2},"b":3}"#,
            &[r#"{"shadow":{"z":1,"a":2},"b":3}"#],
        ),
        (
            "metadata",
            "SELECT qos AS level, b, topic AS origin",
            r#"{"b":2,"qos":0}"#,
            &[r#"{"level":0,"b":2,"origin":"mv00/in/metadata"}"#],
        ),
        (
            "missing",
            "SELECT missing, b",
            r#"{"b":2}"#,
            &[r#"{"missing":null,"b":2}"#],
        ),
        (
            "expression",
            "SELECT b + 1, abs(a), c + 2",
            r#"{"b":2,"a":-1.5,"c":4}"#,
            &[r#"{"kuiper_field_0":3,"abs":1.5,"kuiper_field_1":6}"#],
        ),
        (
            "indexed",
            "SELECT p[1], q[1:2]",
            r#"{"p":[7,8],"q":[4,5,6]}"#,
            &[r#"{"kuiper_field_0":7,"kuiper_field_1":[5]}"#],
        ),
        (
            "foreach-indexed",
            "FOREACH payload.items DO item.b[1], item.a[1:2]",
            r#"{"items":[{"b":[7,8],"a":[4,5,6]}]}"#,
            &[r#"{"b":7,"a":[5]}"#],
        ),
        (
            "foreach",
            "FOREACH payload.items",
            r#"{"items":[{"b":2,"a":1},{"b":4,"a":3}]}"#,
            &[r#"{"item":{"b":2,"a":1}}"#, r#"{"item":{"b":4,"a":3}}"#],
        ),
        (
            "foreach-do",
            "FOREACH payload.items DO item.b AS z, item.a, item.b + 1",
            r#"{"items":[{"b":2,"a":1},{"b":4,"a":3}]}"#,
            &[r#"{"z":2,"a":1,"col_2":3}"#, r#"{"z":4,"a":3,"col_2":5}"#],
        ),
        (
            "foreach-duplicate",
            "FOREACH payload.items DO item.b AS x, item.a AS y, item.a AS x",
            r#"{"items":[{"b":2,"a":1}]}"#,
            &[r#"{"x":1,"y":1}"#],
        ),
        (
            "dotted-missing",
            "SELECT b AS z, payload.a, missing",
            r#"{"a":1,"b":2}"#,
            &[r#"{"z":2,"a":1,"missing":null}"#],
        ),
        (
            "foreach-guards",
            "FOREACH payload.items DO item.b AS z, item.missing INCASE item.b > 1",
            r#"{"items":[{"b":0},{"b":2},{"b":3}]}"#,
            &[r#"{"z":2,"missing":null}"#, r#"{"z":3,"missing":null}"#],
        ),
    ];
    let mut failures = Vec::new();
    for (id, fields, input, expected) in cases {
        let sql = format!("{fields} FROM \"mv00/in/{id}\"");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/rules",
            mv00_rule(id, &sql),
        )
        .await;
        assert_eq!(status, 201, "create {id}: {body}");
        let repeats = if *id == "explicit" { 50 } else { 1 };
        for _ in 0..repeats {
            let messages =
                mv00_publish_outputs(&mut publisher, &mut subscriber, id, input, expected.len())
                    .await;
            let want: Vec<&[u8]> = expected.iter().map(|row| row.as_bytes()).collect();
            if messages != want {
                failures.push(format!("MQTT {id}: {messages:?} != {want:?}"));
            }
        }
        let (status, body) = mv00_api(&kernel, reqwest::Method::POST, "/api/v5/rule_test", serde_json::json!({
            "sql": sql,
            "context": {"event_type": "message_publish", "topic": format!("mv00/in/{id}"), "payload": input},
        })).await;
        assert_eq!(status, 200, "test {id}: {body}");
        let want = if fields.starts_with("FOREACH") {
            format!("[{}]", expected.join(","))
        } else {
            expected[0].to_string()
        };
        let projected_bytes = body["projected"].to_string();
        if projected_bytes != want {
            failures.push(format!("HTTP {id}: {} != {want}", body["projected"]));
        }
        assert_eq!(
            body["output"].to_string(),
            want,
            "HTTP {id} output alias bytes"
        );
        let (status, rpc) = mv00_api(&kernel, reqwest::Method::POST, "/mcp", serde_json::json!({
            "jsonrpc": "2.0", "id": id, "method": "tools/call",
            "params": {"name": "post_api_v5_rule_test", "arguments": {"body": {
                "sql": sql,
                "context": {"event_type": "message_publish", "topic": format!("mv00/in/{id}"), "payload": input},
            }}},
        })).await;
        assert_eq!(status, 200, "MCP {id}: {rpc}");
        assert_ne!(rpc["result"]["isError"], true, "MCP {id}: {rpc}");
        let text = rpc["result"]["content"][0]["text"]
            .as_str()
            .expect("MCP result text");
        let forwarded: serde_json::Value = serde_json::from_str(text).expect("MCP forwarded JSON");
        let forwarded_bytes = forwarded["projected"].to_string();
        if forwarded_bytes != want {
            failures.push(format!("MCP {id}: {} != {want}", forwarded["projected"]));
        }
    }
    kernel.finish().await;
    assert!(
        failures.is_empty(),
        "projection mismatches:\n{}",
        failures.join("\n")
    );
}

/// Window aliases keep their first position and use the last value.
/// Regression for upstream issue #47: retain the last-value contract.
#[tokio::test]
async fn mv00_window_w4_last_duplicate_value_through_mqtt() {
    mv00_window_case(
        "w4",
        "SELECT sum(v) AS x, v AS x, count(*) AS n",
        r#"{"x":1,"n":2}"#,
    )
    .await;
}

/// A wildcard after an aggregate replaces its value at its first position.
/// Regression for upstream issue #47: retain the last-value contract.
#[tokio::test]
async fn mv00_window_w5_later_wildcard_value_through_mqtt() {
    mv00_window_case("w5", "SELECT sum(v) AS v, *", r#"{"v":1}"#).await;
}

async fn mv00_window_case(id: &str, fields: &str, expected: &str) {
    mv00_window_inputs_case(id, fields, &[r#"{"v":1}"#, r#"{"v":3}"#], expected).await;
}

async fn mv00_window_inputs_case(id: &str, fields: &str, inputs: &[&str], expected: &str) {
    let sql = format!("{fields} FROM \"mv00/in/{id}\" GROUP BY COUNTWINDOW(2)");
    mv00_window_sql_case(id, &sql, inputs, expected).await;
}

async fn mv00_window_sql_case(id: &str, sql: &str, inputs: &[&str], expected: &str) {
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(id, sql),
    )
    .await;
    assert_eq!(status, 201, "window create: {body}");
    let mut subscriber = mv00_connect(&kernel, "mv00-window-sub").await;
    mv00_subscribe(&mut subscriber).await;
    let mut publisher = mv00_connect(&kernel, "mv00-window-pub").await;
    for input in inputs {
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&format!("mv00/in/{id}"), 0, 0, false, input.as_bytes()),
        )
        .await;
    }
    let (head, body) = recv_mqtt(&mut subscriber).await;
    assert_eq!(head & 0xf0, 0x30);
    let message = sy02_parse_publish_classic(head, &body);
    assert_eq!(message.topic, format!("mv00/out/{id}"));
    let (status, body) = mv00_api(&kernel, reqwest::Method::POST, "/api/v5/rule_test", serde_json::json!({
        "sql": sql,
        "context": {"event_type": "message_publish", "topic": format!("mv00/in/{id}"), "payload": format!("[{}]", inputs.join(","))},
    })).await;
    assert_eq!(status, 200, "window HTTP: {body}");
    assert_eq!(
        (message.payload, body["projected"].to_string()),
        (expected.as_bytes().to_vec(), format!("[{expected}]")),
        "window {id}: MQTT bytes and HTTP result",
    );
    kernel.finish().await;
}

/// Reboot from the same durable state and the same listener configuration.
async fn mv00_restart(kernel: &mut Kernel) {
    kernel
        .child
        .kill()
        .await
        .expect("stop broker before restart");
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
    loop {
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
        assert!(
            std::time::Instant::now() < deadline,
            "restart opens listeners in time"
        );
        assert!(
            kernel.child.try_wait().expect("restart status").is_none(),
            "restarted broker stays running"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// An accepted update changes the order; a refused update and restart keep it.
#[tokio::test]
async fn mv00_update_refusal_and_restart_keep_projection_order() {
    let mut kernel = spawn_kernel("").await;
    let id = "update";
    let sql = "SELECT b, a, c FROM \"mv00/in/update\"";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(id, sql),
    )
    .await;
    assert_eq!(status, 201, "create: {body}");
    let mut subscriber = mv00_connect(&kernel, "mv00-update-sub").await;
    mv00_subscribe(&mut subscriber).await;
    let mut publisher = mv00_connect(&kernel, "mv00-update-pub").await;
    let input = r#"{"a":1,"b":2,"c":3}"#;
    mv00_assert_publish(
        &mut publisher,
        &mut subscriber,
        id,
        input,
        &[r#"{"b":2,"a":1,"c":3}"#],
    )
    .await;
    let sql = "SELECT c, b, a FROM \"mv00/in/update\"";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/rules/update",
        mv00_rule(id, sql),
    )
    .await;
    assert_eq!(status, 200, "update: {body}");
    let expected = &[r#"{"c":3,"b":2,"a":1}"#];
    mv00_assert_publish(&mut publisher, &mut subscriber, id, input, expected).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/rules/update",
        mv00_rule(id, "SELECT WHERE WHERE"),
    )
    .await;
    assert_eq!(status, 400, "invalid update: {body}");
    assert_eq!(body["code"], "BAD_REQUEST");
    mv00_assert_publish(&mut publisher, &mut subscriber, id, input, expected).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/rules/update",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        body["sql"], sql,
        "refused SQL did not replace durable or live state"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/rules/absent",
        mv00_rule("absent", sql),
    )
    .await;
    assert_eq!(status, 404, "unknown update: {body}");
    drop(publisher);
    drop(subscriber);
    mv00_restart(&mut kernel).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/rules/update",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["sql"], sql, "accepted order persisted through restart");
    let mut subscriber = mv00_connect(&kernel, "mv00-restart-sub").await;
    mv00_subscribe(&mut subscriber).await;
    let mut publisher = mv00_connect(&kernel, "mv00-restart-pub").await;
    mv00_assert_publish(&mut publisher, &mut subscriber, id, input, expected).await;
    kernel.finish().await;
}

/// Ordered aggregate fields and wildcard inputs pass through the real window bus.
#[tokio::test]
async fn mv00_window_projection_order_through_mqtt_and_http() {
    mv00_window_case(
        "window-explicit",
        "SELECT sum(v) AS total, count(*) AS n",
        r#"{"total":4,"n":2}"#,
    )
    .await;
}

#[tokio::test]
async fn mv00_window_fraction_keeps_numeric_value_and_field_order() {
    mv00_window_inputs_case(
        "window-fraction",
        "SELECT sum(v) AS total, count(*) AS n",
        &[r#"{"v":1.5}"#, r#"{"v":3}"#],
        r#"{"total":4.5,"n":2}"#,
    )
    .await;
}

#[tokio::test]
async fn mv00_window_wildcard_keeps_input_order_without_metadata() {
    mv00_window_case(
        "window-wildcard",
        "SELECT *, sum(v) AS total",
        r#"{"v":1,"total":4}"#,
    )
    .await;
}

#[tokio::test]
async fn mv00_window_expressions_keep_names_and_order() {
    mv00_window_case(
        "window-expression",
        "SELECT sum(v), count(*), sum(v) + 1",
        r#"{"sum":4,"count":2,"kuiper_field_0":5}"#,
    )
    .await;
}

/// Time-window metadata stays in its SELECT position on each supported path.
#[tokio::test]
async fn mv00_window_bounds_keep_select_positions_through_mqtt() {
    for (id, window) in [
        ("count-bounds", "COUNTWINDOW(1)"),
        ("tumbling-bounds", "TUMBLINGWINDOW(ms, 50)"),
        ("hopping-bounds", "HOPPINGWINDOW(ms, 1000, 50)"),
        ("sliding-bounds", "SLIDINGWINDOW(ss, 1)"),
    ] {
        let kernel = spawn_kernel("").await;
        let sql = format!("SELECT window_end() AS end, sum(v) AS total, window_start() AS start FROM \"mv00/in/{id}\" GROUP BY {window}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/rules",
            mv00_rule(id, &sql),
        )
        .await;
        assert_eq!(status, 201, "window bounds create: {body}");
        let mut subscriber = mv00_connect(&kernel, "mv00-bounds-sub").await;
        mv00_subscribe(&mut subscriber).await;
        let mut publisher = mv00_connect(&kernel, "mv00-bounds-pub").await;
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&format!("mv00/in/{id}"), 0, 0, false, br#"{"v":1}"#),
        )
        .await;
        let (head, body) = recv_mqtt(&mut subscriber).await;
        assert_eq!(head & 0xf0, 0x30);
        let message = sy02_parse_publish_classic(head, &body);
        let output: serde_json::Value =
            serde_json::from_slice(&message.payload).expect("window JSON");
        let keys: Vec<&str> = output
            .as_object()
            .expect("window object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, ["end", "total", "start"], "{id} SELECT order");
        let start = output["start"].as_u64().expect("real window start");
        let end = output["end"].as_u64().expect("real window end");
        assert!(start > 0 && end >= start, "{id} real bounds");
        assert_eq!(output["total"].as_f64(), Some(1.0));
        kernel.finish().await;
    }
}

/// Window rules emit for plain payloads, as stateless rules do.
#[tokio::test]
async fn sb23_window_plain_payload_emits_through_mqtt() {
    let id = "sb23-plain";
    let kernel = spawn_kernel("").await;
    let sql = format!("SELECT * FROM \"mv00/in/{id}\" GROUP BY COUNTWINDOW(1)");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(id, &sql),
    )
    .await;
    assert_eq!(status, 201, "window create: {body}");
    let mut subscriber = mv00_connect(&kernel, "sb23-plain-sub").await;
    mv00_subscribe(&mut subscriber).await;
    let mut publisher = mv00_connect(&kernel, "sb23-plain-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311(&format!("mv00/in/{id}"), 0, 0, false, b"hello"),
    )
    .await;
    let (head, body) = tokio::time::timeout(STEP, recv_mqtt(&mut subscriber))
        .await
        .expect("window output in time");
    assert_eq!(head & 0xf0, 0x30);
    let message = sy02_parse_publish_classic(head, &body);
    assert_eq!(message.topic, format!("mv00/out/{id}"));
    let output: serde_json::Value = serde_json::from_slice(&message.payload).expect("window JSON");
    assert_eq!(output, serde_json::json!({}), "plain yields empty row");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        &format!("/api/v5/rules/{id}/metrics"),
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "metrics read: {body}");
    assert_eq!(body["metrics"]["matched"], 1, "{body}");
    assert_eq!(body["metrics"]["passed"], 1, "{body}");
    assert_eq!(body["metrics"]["actions.total"], 1, "{body}");
    kernel.finish().await;
}

/// SB-27 plain payloads agree inline and in windows through sockets.
#[tokio::test]
async fn sb27_inline_and_window_plain_payload_agree_through_mqtt() {
    let kernel = spawn_kernel("").await;
    let inline_id = "sb27-inline";
    let inline_sql = format!("SELECT * FROM \"mv00/in/{inline_id}\"");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(inline_id, &inline_sql),
    )
    .await;
    assert_eq!(status, 201, "inline create: {body}");
    let window_id = "sb27-window";
    let window_sql = format!("SELECT * FROM \"mv00/in/{window_id}\" GROUP BY COUNTWINDOW(1)");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(window_id, &window_sql),
    )
    .await;
    assert_eq!(status, 201, "window create: {body}");
    let mut subscriber = mv00_connect(&kernel, "sb27-agree-sub").await;
    mv00_subscribe(&mut subscriber).await;
    let mut publisher = mv00_connect(&kernel, "sb27-agree-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311(&format!("mv00/in/{inline_id}"), 0, 0, false, b"hello"),
    )
    .await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311(&format!("mv00/in/{window_id}"), 0, 0, false, b"hello"),
    )
    .await;
    let mut got = Vec::new();
    for _ in 0..2 {
        let (head, body) = tokio::time::timeout(STEP, recv_mqtt(&mut subscriber))
            .await
            .expect("rule output in time");
        assert_eq!(head & 0xf0, 0x30);
        let message = sy02_parse_publish_classic(head, &body);
        let output: serde_json::Value =
            serde_json::from_slice(&message.payload).expect("rule JSON");
        assert_eq!(output, serde_json::json!({}), "plain yields empty row");
        got.push(message.topic);
    }
    got.sort();
    assert_eq!(
        got,
        vec![
            format!("mv00/out/{inline_id}"),
            format!("mv00/out/{window_id}")
        ]
    );
    for id in [inline_id, window_id] {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/rules/{id}/metrics"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "metrics read: {body}");
        assert_eq!(body["metrics"]["matched"], 1, "{body}");
        assert_eq!(body["metrics"]["passed"], 1, "{body}");
        assert_eq!(body["metrics"]["failed"], 0, "{body}");
    }
    let object_id = "sb27-object";
    let object_sql = format!("SELECT * FROM \"mv00/in/{object_id}\" GROUP BY COUNTWINDOW(1)");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(object_id, &object_sql),
    )
    .await;
    assert_eq!(status, 201, "object window create: {body}");
    // A JSON object payload uses the same rule path.
    // It delivers the object in windows.
    send_mqtt(
        &mut publisher,
        &sy02_publish311(&format!("mv00/in/{object_id}"), 0, 0, false, br#"{"v":42}"#),
    )
    .await;
    let (head, body) = tokio::time::timeout(STEP, recv_mqtt(&mut subscriber))
        .await
        .expect("object window output in time");
    assert_eq!(head & 0xf0, 0x30);
    let message = sy02_parse_publish_classic(head, &body);
    assert_eq!(message.topic, format!("mv00/out/{object_id}"));
    let output: serde_json::Value =
        serde_json::from_slice(&message.payload).expect("object window JSON");
    assert_eq!(
        output,
        serde_json::json!({"v": 42}),
        "object yields its fields"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        &format!("/api/v5/rules/{object_id}/metrics"),
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "object metrics read: {body}");
    assert_eq!(body["metrics"]["matched"], 1, "{body}");
    assert_eq!(body["metrics"]["passed"], 1, "{body}");
    assert_eq!(body["metrics"]["failed"], 0, "{body}");
    // A filtered input counts failed once. The rule stays stored.
    let filtered_id = "sb27-filtered";
    let filtered_sql = format!("SELECT * FROM \"mv00/in/{filtered_id}\" WHERE v = 1");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(filtered_id, &filtered_sql),
    )
    .await;
    assert_eq!(status, 201, "filtered create: {body}");
    send_mqtt(
        &mut publisher,
        &sy02_publish311(
            &format!("mv00/in/{filtered_id}"),
            0,
            0,
            false,
            br#"{"v":2}"#,
        ),
    )
    .await;
    for _ in 0..100 {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/rules/{filtered_id}/metrics"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "filtered metrics read: {body}");
        if body["metrics"]["failed"].as_u64().unwrap_or(0) == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        &format!("/api/v5/rules/{filtered_id}/metrics"),
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "filtered metrics read: {body}");
    assert_eq!(body["metrics"]["matched"], 1, "{body}");
    assert_eq!(body["metrics"]["passed"], 0, "{body}");
    assert_eq!(body["metrics"]["failed"], 1, "{body}");
    kernel.finish().await;
}

/// Group rows keep first-seen order and each group's payload metadata keys.
#[tokio::test]
async fn mv00_group_windows_keep_order_and_payload_shadows() {
    let mut failures = Vec::new();
    for (id, fields, expected) in [
        (
            "groups",
            "SELECT sum(v) AS total, count(*) AS n, sensor_id",
            vec![
                r#"{"total":4,"n":2,"sensor_id":"z"}"#,
                r#"{"total":2,"n":1,"sensor_id":"a"}"#,
            ],
        ),
        (
            "group-shadows",
            "SELECT *, sum(v) AS total",
            vec![
                r#"{"v":1,"sensor_id":"z","topic":"payload-topic","total":4}"#,
                r#"{"v":2,"sensor_id":"a","total":2}"#,
            ],
        ),
    ] {
        let kernel = spawn_kernel("").await;
        let sql = format!("{fields} FROM \"mv00/in/{id}\" GROUP BY sensor_id, COUNTWINDOW(3)");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/rules",
            mv00_rule(id, &sql),
        )
        .await;
        assert_eq!(status, 201, "group create: {body}");
        let mut subscriber = mv00_connect(&kernel, "mv00-group-sub").await;
        mv00_subscribe(&mut subscriber).await;
        let mut publisher = mv00_connect(&kernel, "mv00-group-pub").await;
        let inputs = [
            r#"{"v":1,"sensor_id":"z","topic":"payload-topic"}"#,
            r#"{"v":2,"sensor_id":"a"}"#,
            r#"{"v":3,"sensor_id":"z"}"#,
        ];
        let mut messages = Vec::new();
        for input in inputs {
            send_mqtt(
                &mut publisher,
                &sy02_publish311(&format!("mv00/in/{id}"), 0, 0, false, input.as_bytes()),
            )
            .await;
        }
        for _ in &expected {
            let (head, body) = recv_mqtt(&mut subscriber).await;
            assert_eq!(head & 0xf0, 0x30);
            let message = sy02_parse_publish_classic(head, &body);
            assert_eq!(message.topic, format!("mv00/out/{id}"));
            messages.push(String::from_utf8(message.payload).expect("group JSON bytes"));
        }
        if messages != expected {
            failures.push(format!("MQTT {id}: {messages:?} != {expected:?}"));
        }
        let (status, body) = mv00_api(&kernel, reqwest::Method::POST, "/api/v5/rule_test", serde_json::json!({
            "sql": sql, "context": {"event_type": "message_publish", "topic": format!("mv00/in/{id}"), "payload": format!("[{}]", inputs.join(","))},
        })).await;
        assert_eq!(status, 200, "group HTTP: {body}");
        let want = format!("[{}]", expected.join(","));
        let projected_bytes = body["projected"].to_string();
        if projected_bytes != want {
            failures.push(format!("HTTP {id}: {} != {want}", body["projected"]));
        }
        kernel.finish().await;
    }
    assert!(
        failures.is_empty(),
        "group mismatches:\n{}",
        failures.join("\n")
    );
}

/// Non-array input and a false guard produce no FOREACH output.
#[tokio::test]
async fn mv00_foreach_no_match_and_refused_update_keep_state() {
    let kernel = spawn_kernel("").await;
    let id = "foreach-negative";
    let sql = "FOREACH payload.items DO item.b AS z, item.a FROM \"mv00/in/foreach-negative\" WHERE allow = true";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(id, sql),
    )
    .await;
    assert_eq!(status, 201, "FOREACH create: {body}");
    let mut subscriber = mv00_connect(&kernel, "mv00-negative-sub").await;
    mv00_subscribe(&mut subscriber).await;
    let mut publisher = mv00_connect(&kernel, "mv00-negative-pub").await;
    for input in [
        r#"{"allow":false,"items":[{"b":2,"a":1}]}"#,
        r#"{"allow":true,"items":2}"#,
    ] {
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&format!("mv00/in/{id}"), 0, 0, false, input.as_bytes()),
        )
        .await;
        let mut head = [0_u8; 1];
        use tokio::io::AsyncReadExt;
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(200),
                subscriber.read_exact(&mut head)
            )
            .await
            .is_err(),
            "no output for unmatched FOREACH"
        );
        let (status, body) = mv00_api(&kernel, reqwest::Method::POST, "/api/v5/rule_test", serde_json::json!({
            "sql": sql, "context": {"event_type": "message_publish", "topic": format!("mv00/in/{id}"), "payload": input},
        })).await;
        assert_eq!(status, 412, "unmatched HTTP: {body}");
        assert_eq!(body["code"], "NOT_MATCH");
    }
    for invalid in [
        "FOREACH payload.items DO * FROM \"mv00/in/foreach-negative\"",
        "FOREACH payload.items FROM \"mv00/in/foreach-negative\" GROUP BY COUNTWINDOW(2)",
    ] {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::PUT,
            "/api/v5/rules/foreach-negative",
            mv00_rule(id, invalid),
        )
        .await;
        assert_eq!(status, 400, "refused FOREACH update: {body}");
        assert_eq!(body["code"], "BAD_REQUEST");
        mv00_assert_publish(
            &mut publisher,
            &mut subscriber,
            id,
            r#"{"allow":true,"items":[{"b":2,"a":1}]}"#,
            &[r#"{"z":2,"a":1}"#],
        )
        .await;
    }
    kernel.finish().await;
}

/// The connector receives the projected bytes after real MQTT ingress.
#[tokio::test]
async fn mv00_webhook_keeps_projection_bytes_through_mqtt() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("HTTP receiver binds");
    let address = listener.local_addr().expect("HTTP receiver address");
    let receiver = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("HTTP request accepted");
        let mut request = Vec::new();
        let header_end = loop {
            let mut byte = [0_u8; 1];
            socket
                .read_exact(&mut byte)
                .await
                .expect("HTTP header byte");
            request.push(byte[0]);
            assert!(request.len() <= 16384, "bounded HTTP header");
            if request.ends_with(b"\r\n\r\n") {
                break request.len();
            }
        };
        let headers = String::from_utf8(request).expect("HTTP headers");
        let length: usize = headers
            .lines()
            .find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse().expect("HTTP length"))
            })
            .expect("HTTP content length");
        assert!(header_end > 0 && length <= 4096, "bounded body");
        let mut body = vec![0_u8; length];
        socket.read_exact(&mut body).await.expect("HTTP body");
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .await
            .expect("HTTP response");
        body
    });
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(&kernel, reqwest::Method::POST, "/api/v1/connectors", serde_json::json!({
        "id": "mv00-hook", "kind": "webhook", "config": {"url": format!("http://{address}/order"), "batch_size": 1},
    })).await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v1/rules",
        serde_json::json!({
            "name": "mv00-hook-rule", "topic_filter": "mv00/hook/in",
            "sql_query": "SELECT b, a, obj FROM \"mv00/hook/in\"",
            "actions": [{"type": "forwardconnector", "connector_id": "mv00-hook"}],
        }),
    )
    .await;
    assert_eq!(status, 201, "forward rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv00-hook-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311(
            "mv00/hook/in",
            0,
            0,
            false,
            br#"{"a":1,"b":2,"obj":{"z":3,"a":4}}"#,
        ),
    )
    .await;
    let bytes = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(bytes, br#"{"b":2,"a":1,"obj":{"z":3,"a":4}}"#);
    kernel.finish().await;
}

/// The legacy parser also keeps wildcard order and hides injected bounds.
#[tokio::test]
async fn mv00_legacy_window_wildcard_keeps_payload_order() {
    mv00_window_sql_case("legacy-window", "SELECT *, sum(v) AS total FROM \"mv00/in/legacy-window\" GROUP BY COUNTWINDOW(2) ORDER BY v", &[r#"{"v":1,"b":2,"a":3}"#, r#"{"v":3,"b":4,"a":5}"#], r#"{"v":1,"b":2,"a":3,"total":4}"#).await;
}

/// A duplicate returns the reference code and keeps the live and stored rule.
#[tokio::test]
async fn mv00_api_error_duplicate_create_keeps_live_and_stored_rule() {
    let mut kernel = spawn_kernel("").await;
    let id = "duplicate-error";
    let sql = "SELECT b FROM \"mv00/in/duplicate-error\"";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(id, sql),
    )
    .await;
    assert_eq!(status, 201, "create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(id, "SELECT a FROM \"mv00/in/duplicate-error\""),
    )
    .await;
    assert_eq!(status, 400, "duplicate: {body}");
    assert!(body["message"].is_string());
    let duplicate_code = body["code"].as_str().expect("error code").to_string();
    for phase in 0..2 {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            "/api/v5/rules/duplicate-error",
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["sql"], sql, "duplicate kept the original SQL");
        let mut subscriber = mv00_connect(&kernel, "mv00-duplicate-sub").await;
        mv00_subscribe(&mut subscriber).await;
        let mut publisher = mv00_connect(&kernel, "mv00-duplicate-pub").await;
        mv00_assert_publish(
            &mut publisher,
            &mut subscriber,
            id,
            r#"{"a":1,"b":2}"#,
            &[r#"{"b":2}"#],
        )
        .await;
        drop(publisher);
        drop(subscriber);
        if phase == 0 {
            mv00_restart(&mut kernel).await;
        }
    }
    kernel.finish().await;
    assert_eq!(duplicate_code, "BAD_REQUEST");
}

/// An absent stored-rule probe returns the reference code and creates no state.
#[tokio::test]
async fn mv00_api_error_missing_probe_keeps_store_empty() {
    let mut kernel = spawn_kernel("").await;
    let mut codes = Vec::new();
    for phase in 0..2 {
        for sample in [
            serde_json::json!({"context": {
                "event_type": "message_publish",
                "topic": "mv00/in/missing",
                "payload": r#"{"b":2}"#,
            }}),
            serde_json::Value::Null,
        ] {
            let (status, body) = mv00_api(
                &kernel,
                reqwest::Method::POST,
                "/api/v5/rules/no-such-rule/test",
                sample,
            )
            .await;
            assert_eq!(status, 404, "absent rule: {body}");
            assert!(body["message"].is_string());
            codes.push(body["code"].as_str().expect("error code").to_string());
        }
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            "/api/v5/rules",
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["data"], serde_json::json!([]));
        if phase == 0 {
            mv00_restart(&mut kernel).await;
        }
    }
    kernel.finish().await;
    assert!(
        codes.iter().all(|code| code == "RULE_NOT_FOUND"),
        "absent-rule probe codes: {codes:?}"
    );
}

// ---------------------------------------------------------------------------
// MV-01/SB-03: action-owned runtime sinks. One action is one dispatch key
// (`action:{id}`); the sink builds from the referenced connector's settings
// plus the action's own parameters, and the fire path crosses to it.
// ---------------------------------------------------------------------------

/// Accept `expected` HTTP requests on the shared listener and return each
/// (request path, request body) pair. One serve per POST.
fn mv01_spawn_http_receiver(
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
type Mv01ActiveCapture = (Vec<(String, Vec<u8>)>, usize);

/// Serve HTTP requests and record the peak number of requests handled at
/// one time. Each response waits `hold`, so a concurrent client shows a
/// peak above one. Returns the requests and the peak.
fn mv01_spawn_http_receiver_active(
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

/// Two actions on one connector each build their own sink and route to
/// their own parameter shape; a delete stops only the deleted action.
#[tokio::test]
async fn mv01_webhook_action_sinks_route_independently_and_stop_on_delete() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 2);
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-webhook",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    for (id, path) in [("mv01-a", "/a"), ("mv01-b", "/b")] {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/actions",
            serde_json::json!({
                "name": id,
                "type": "webhook",
                "connector": "mv01-webhook",
                "parameters": {"path": path},
            }),
        )
        .await;
        assert_eq!(status, 201, "action {id} create: {body}");
        assert_eq!(
            body["status"], "connected",
            "action {id} reads live: {body}"
        );
        assert!(body["status_reason"].is_null(), "action {id}: {body}");
    }
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-rule",
            "sql": "SELECT * FROM \"mv01/in/1\"",
            "actions": ["mv01-a", "mv01-b"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/1", 0, 0, false, br#"{"k":1}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    let mut paths: Vec<&str> = received.iter().map(|(path, _)| path.as_str()).collect();
    paths.sort_unstable();
    assert_eq!(
        paths,
        vec!["/a", "/b"],
        "both action sinks routed: {received:?}"
    );
    for (_, body) in &received {
        assert_eq!(body.as_slice(), &br#"{"k":1}"#[..]);
    }
    // A rule that still names the action holds the delete back.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/mv01-a",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 400, "referenced action delete: {body}");
    // Remove the action from the rule, then delete it.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/rules/mv01-rule",
        serde_json::json!({
            "sql": "SELECT * FROM \"mv01/in/1\"",
            "actions": ["mv01-b"],
        }),
    )
    .await;
    assert_eq!(status, 200, "rule update: {body}");
    // Deleting one action stops only that action's delivery.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/mv01-a",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "action delete: {body}");
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/1", 0, 0, false, br#"{"k":2}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 1, "only the surviving action delivers");
    let (path, body) = &received[0];
    assert_eq!(path, "/b");
    assert_eq!(body.as_slice(), &br#"{"k":2}"#[..]);
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-a",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 404, "deleted action reads absent: {body}");
    kernel.finish().await;
}

/// An action whose connector does not resolve stores `disconnected` with
/// the named reason and starts delivering once the connector arrives —
/// creating the connector alone wires it.
#[tokio::test]
async fn mv01_http_action_dangling_connector_stores_disconnected_and_recovers() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-late",
            "type": "webhook",
            "connector": "webhook:mv01-late-link",
            "parameters": {"path": "/late"},
        }),
    )
    .await;
    assert_eq!(status, 201, "dangling action stores: {body}");
    assert_eq!(body["status"], "disconnected", "{body}");
    let reason = body["status_reason"].as_str().expect("status reason");
    assert!(
        reason.contains("mv01-late-link"),
        "reason names the missing connector: {reason}"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-late-link",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-late",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["status"], "connected",
        "the action wired with its connector: {body}"
    );
    assert!(body["status_reason"].is_null(), "{body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-late-rule",
            "sql": "SELECT * FROM \"mv01/in/late\"",
            "actions": ["mv01-late"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-late-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/late", 0, 0, false, br#"{"late":true}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    let (path, body) = &received[0];
    assert_eq!(path, "/late");
    assert_eq!(body.as_slice(), &br#"{"late":true}"#[..]);
    kernel.finish().await;
}

/// Disabling an action stops its delivery; re-enabling restores it.
#[tokio::test]
async fn mv01_webhook_action_disable_stops_delivery_and_toggle_restores() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-onoff",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-onoff-action",
            "type": "webhook",
            "connector": "mv01-onoff",
            "parameters": {"path": "/onoff"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-onoff-rule",
            "sql": "SELECT * FROM \"mv01/in/onoff\"",
            "actions": ["mv01-onoff-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-onoff-pub").await;
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/onoff", 0, 0, false, br#"{"e":1}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    let (path, body) = &received[0];
    assert_eq!(path, "/onoff");
    assert_eq!(body.as_slice(), &br#"{"e":1}"#[..]);
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-onoff-action/enable/false",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "disable: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-onoff-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "disconnected", "{body}");
    // A silenced receiver: nothing accepts while the action reads off.
    let watch = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/onoff", 0, 0, false, br#"{"e":2}"#),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(
        !watch.is_finished(),
        "a disabled action delivered: accept ended the promise"
    );
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-onoff-action/enable/true",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "re-enable");
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/onoff", 0, 0, false, br#"{"e":3}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, watch)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    let (path, body) = &received[0];
    assert_eq!(path, "/onoff");
    assert_eq!(body.as_slice(), &br#"{"e":3}"#[..]);
    kernel.finish().await;
}

/// A connector update that breaks a dependent action's sink build is
/// refused with the action and field named; the old sink keeps
/// delivering and the stored connector stays untouched.
#[tokio::test]
async fn mv01_connector_update_dependent_build_failure_refuses_and_keeps_delivery() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-break",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-break-action",
            "type": "webhook",
            "connector": "mv01-break",
            "parameters": {"path": "/break"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-break-rule",
            "sql": "SELECT * FROM \"mv01/in/break\"",
            "actions": ["mv01-break-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-break-pub").await;
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/break", 0, 0, false, br#"{"f":1}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received[0].1.as_slice(), &br#"{"f":1}"#[..]);
    // The URL only resolves through a file reference the environment
    // cannot load: the dependent build fails closed and the update is
    // refused with the action and field named.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/connectors/mv01-break",
        serde_json::json!({
            "name": "mv01-break",
            "type": "webhook",
            "url": "file:/nonexistent/indra-mv01-secret",
        }),
    )
    .await;
    assert_eq!(status, 400, "dependent build failure refuses: {body}");
    assert!(body["message"].is_string(), "{body}");
    let message = body["message"].as_str().expect("error message");
    assert!(
        message.contains("mv01-break-action") && message.contains("url"),
        "the refusal names the action and the field: {message}"
    );
    // The old generation kept delivering.
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/break", 0, 0, false, br#"{"f":2}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    let (path, body) = &received[0];
    assert_eq!(path, "/break");
    assert_eq!(body.as_slice(), &br#"{"f":2}"#[..]);
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-break-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["status"], "connected",
        "the refused update kept the wired action live: {body}"
    );
    kernel.finish().await;
}

/// A restart rebuilds the owned sinks from the stored connector settings
/// plus the stored action parameters and delivery resumes.
#[tokio::test]
async fn mv01_webhook_action_sink_survives_restart() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let mut kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-restart",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-restart-action",
            "type": "webhook",
            "connector": "mv01-restart",
            "parameters": {"path": "/after/restart"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-restart-rule",
            "sql": "SELECT * FROM \"mv01/in/restart\"",
            "actions": ["mv01-restart-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-restart-pub").await;
    let first = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/restart", 0, 0, false, br#"{"r":1}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, first)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    let (path, body) = &received[0];
    assert_eq!(path, "/after/restart");
    assert_eq!(body.as_slice(), &br#"{"r":1}"#[..]);
    mv00_restart(&mut kernel).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-restart-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "connected", "{body}");
    let mut publisher = mv00_connect(&kernel, "mv01-restart-pub2").await;
    let second = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/restart", 0, 0, false, br#"{"r":2}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, second)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    let (path, body) = &received[0];
    assert_eq!(path, "/after/restart");
    assert_eq!(body.as_slice(), &br#"{"r":2}"#[..]);
    kernel.finish().await;
}

/// RO-01: an action runtime applies its stored options through a real
/// publish, keeps its counters per action, resets every counter and
/// rebuilds the same runtime from the store after a restart.
#[tokio::test]
async fn ro01_action_runtime_applies_options_through_publish_and_restart() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let mut kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "ro01-restart",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "ro01-restart-action",
            "type": "webhook",
            "connector": "ro01-restart",
            "parameters": {"path": "/ro01"},
            "resource_opts": {
                "batch_size": 2,
                "batch_time": "5s",
                "inflight_window": 1,
                "worker_pool_size": 2,
                "max_buffer_bytes": "1MB",
                "request_ttl": "30s",
                "query_mode": "async",
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    assert_eq!(body["status"], "connected", "{body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-restart-rule",
            "sql": "SELECT * FROM \"ro01/in/restart\"",
            "actions": ["ro01-restart-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-restart-action/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["metrics"]["matched"], 0,
        "counters start at zero: {body}"
    );
    assert_eq!(body["metrics"]["success"], 0, "{body}");
    assert!(
        body["metrics"].get("retried").is_none(),
        "the invented retried counter is gone: {body}"
    );
    let mut publisher = mv00_connect(&kernel, "ro01-restart-pub").await;
    let mut first = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/restart", 0, 0, false, br#"{"r":1}"#),
    )
    .await;
    // The runtime holds one row until the batch is full or the batch time
    // elapses. A raw sink would deliver it now, so this assertion fails
    // when the runtime is absent.
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), &mut first)
            .await
            .is_err(),
        "one row waits in the bounded action buffer"
    );
    first.abort();
    let second = mv01_spawn_http_receiver(Arc::clone(&listener), 2);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/restart", 0, 0, false, br#"{"r":2}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, second)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 2, "the batch reached the target");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-restart-action/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["metrics"]["matched"], 2, "{body}");
    assert_eq!(body["metrics"]["success"], 2, "{body}");
    assert_eq!(body["metrics"]["failed"], 0, "{body}");
    assert_eq!(body["metrics"]["queuing"], 0, "{body}");
    assert_eq!(body["metrics"]["inflight"], 0, "{body}");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/ro01-restart-action/metrics/reset",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, 204);
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-restart-action/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    for counter in [
        "total",
        "matched",
        "success",
        "failed",
        "dropped",
        "dropped.queue_full",
        "dropped.resource_not_found",
        "dropped.resource_stopped",
        "dropped.other",
        "queuing",
        "inflight",
    ] {
        assert_eq!(
            body["metrics"][counter], 0,
            "reset zeroes {counter}: {body}"
        );
    }
    mv00_restart(&mut kernel).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-restart-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "connected", "{body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-restart-action/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["metrics"]["matched"], 0,
        "restart starts at zero: {body}"
    );
    let mut publisher = mv00_connect(&kernel, "ro01-restart-pub2").await;
    let mut first = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/restart", 0, 0, false, br#"{"r":3}"#),
    )
    .await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), &mut first)
            .await
            .is_err(),
        "the rebuilt runtime applies the stored batch size"
    );
    first.abort();
    let second = mv01_spawn_http_receiver(Arc::clone(&listener), 2);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/restart", 0, 0, false, br#"{"r":4}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, second)
        .await
        .expect("HTTP delivery after restart in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 2, "the rebuilt runtime delivered the batch");
    kernel.finish().await;
}

/// RO-01/N1: a connector update rebuilds the dependent action sink through
/// `wrap_action_runtime`, so the stored action options still apply.
#[tokio::test]
async fn ro01_connector_update_keeps_action_options() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let kernel = spawn_kernel("").await;
    let connector = serde_json::json!({
        "name": "ro01-update",
        "type": "webhook",
        "url": format!("http://{address}"),
    });
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        connector.clone(),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "ro01-update-action",
            "type": "webhook",
            "connector": "ro01-update",
            "parameters": {"path": "/ro01-update"},
            "resource_opts": {
                "batch_size": 2,
                "batch_time": "5s",
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-update-rule",
            "sql": "SELECT * FROM \"ro01/in/update\"",
            "actions": ["ro01-update-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/connectors/ro01-update",
        connector,
    )
    .await;
    assert_eq!(status, 200, "connector update: {body}");
    let mut publisher = mv00_connect(&kernel, "ro01-update-pub").await;
    let mut first = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/update", 0, 0, false, br#"{"u":1}"#),
    )
    .await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), &mut first)
            .await
            .is_err(),
        "the rebuilt sink still holds one row for the batch"
    );
    first.abort();
    let second = mv01_spawn_http_receiver(Arc::clone(&listener), 2);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/update", 0, 0, false, br#"{"u":2}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, second)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 2, "the rebuilt runtime delivered the batch");
    kernel.finish().await;
}

/// RO-01/N1/B13: a stored pgsql action with the legacy integer
/// `batch_timeout_ms` boots again and applies its batch size.
#[tokio::test]
async fn ro01_stored_pgsql_legacy_batch_timeout_boots() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pg fake binds");
    let address = listener.local_addr().expect("pg fake address");
    let captured = Arc::new(Mutex::new(Vec::<(String, Vec<Vec<Vec<u8>>>)>::new()));
    let fake = {
        let captured = Arc::clone(&captured);
        tokio::spawn(async move {
            let stream = mv01_pg_accept(&listener).await;
            mv01_pg_serve_one(stream, captured, "u", "pwd", "db", 2).await;
        })
    };
    let mut kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "ro01-legacy-pg",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "ro01-legacy-pg-action",
            "type": "pgsql",
            "connector": "ro01-legacy-pg",
            "parameters": {"sql": "RO01-LEGACY INSERT $1, $2, $3"},
            "resource_opts": {
                "batch_size": 2,
                "batch_timeout_ms": 5000,
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-legacy-rule",
            "sql": "SELECT * FROM \"ro01/in/legacy\"",
            "actions": ["ro01-legacy-pg-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    mv00_restart(&mut kernel).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-legacy-pg-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["status"], "connected",
        "the stored legacy action rebuilt at boot: {body}"
    );
    let mut publisher = mv00_connect(&kernel, "ro01-legacy-pub").await;
    for value in [1, 2] {
        let payload = format!("{{\"n\":{value}}}");
        send_mqtt(
            &mut publisher,
            &sy02_publish311("ro01/in/legacy", 0, 0, false, payload.as_bytes()),
        )
        .await;
    }
    fake.await.expect("pg fake task");
    let captured = std::sync::Mutex::into_inner(Arc::try_unwrap(captured).expect("sole owner"))
        .expect("capture lock");
    assert_eq!(captured.len(), 1, "one runtime batch reached the driver");
    assert_eq!(
        captured[0].1.len(),
        2,
        "the rebuilt runtime applied its stored batch size"
    );
    kernel.finish().await;
}

/// RO-01/P1: each applied resource option shows its effect through a real
/// publish. One kernel and one connector serve six sequential actions. Each
/// block would fail with a raw sink: the first rows would arrive at once,
/// the overflow would not count drops, and the expired row would deliver.
#[tokio::test]
async fn ro01_action_resource_opts_fields_apply_through_publish() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "ro01-fields",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let mut publisher = mv00_connect(&kernel, "ro01-fields-pub").await;

    // `batch_time` flushes a partial batch after the wait.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "ro01-f-time",
            "type": "webhook",
            "connector": "ro01-fields",
            "parameters": {"path": "/f-time"},
            "resource_opts": {
                "batch_size": 100,
                "batch_time": "100ms",
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "time action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-f-time-rule",
            "sql": "SELECT * FROM \"ro01/in/f-time\"",
            "actions": ["ro01-f-time"],
        }),
    )
    .await;
    assert_eq!(status, 201, "time rule create: {body}");
    let mut hold = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/f-time", 0, 0, false, br#"{"n":1}"#),
    )
    .await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut hold)
            .await
            .is_err(),
        "one row waits for the batch time"
    );
    hold.abort();
    let delivered = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    let received = tokio::time::timeout(STEP, delivered)
        .await
        .expect("batch time delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 1, "the timer flushed the partial batch");
    assert_eq!(
        received[0].0, "/f-time",
        "the time action path: {received:?}"
    );
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/ro01-f-time-rule",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/ro01-f-time",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);

    // `max_buffer_bytes` drops the oldest rows and counts them.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "ro01-f-buf",
            "type": "webhook",
            "connector": "ro01-fields",
            "parameters": {"path": "/f-buf"},
            "resource_opts": {
                "batch_size": 100,
                "batch_time": "10s",
                "max_buffer_bytes": "20B",
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "buffer action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-f-buf-rule",
            "sql": "SELECT * FROM \"ro01/in/f-buf\"",
            "actions": ["ro01-f-buf"],
        }),
    )
    .await;
    assert_eq!(status, 201, "buffer rule create: {body}");
    let mut hold = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    for value in [1, 2, 3] {
        let payload = format!("{{\"n\":{value}}}");
        send_mqtt(
            &mut publisher,
            &sy02_publish311("ro01/in/f-buf", 0, 0, false, payload.as_bytes()),
        )
        .await;
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), &mut hold)
            .await
            .is_err(),
        "a full buffer holds rows until the batch is full"
    );
    hold.abort();
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-f-buf/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["metrics"]["matched"], 3, "{body}");
    assert!(
        body["metrics"]["dropped"].as_u64().unwrap_or(0) >= 1,
        "overflow drops are counted: {body}"
    );
    assert!(
        body["metrics"]["dropped.queue_full"].as_u64().unwrap_or(0) >= 1,
        "queue-full drops are counted: {body}"
    );
    let buffered = body["metrics"]["queuing"].as_u64().unwrap_or(0) as usize;
    let drained = mv01_spawn_http_receiver(Arc::clone(&listener), buffered);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/ro01-f-buf-rule",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/ro01-f-buf",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let drained = tokio::time::timeout(STEP, drained)
        .await
        .expect("delete drains the buffered rows in time")
        .expect("HTTP receiver task");
    assert_eq!(
        drained.len(),
        buffered,
        "delete delivers the buffered rows before the next block"
    );

    // `request_ttl` of zero expires every row before the send.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "ro01-f-ttl",
            "type": "webhook",
            "connector": "ro01-fields",
            "parameters": {"path": "/f-ttl"},
            "resource_opts": {
                "batch_size": 1,
                "batch_time": "0ms",
                "request_ttl": "0ms",
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "ttl action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-f-ttl-rule",
            "sql": "SELECT * FROM \"ro01/in/f-ttl\"",
            "actions": ["ro01-f-ttl"],
        }),
    )
    .await;
    assert_eq!(status, 201, "ttl rule create: {body}");
    let mut hold = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/f-ttl", 0, 0, false, br#"{"n":1}"#),
    )
    .await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(500), &mut hold)
            .await
            .is_err(),
        "an expired row never reaches the target"
    );
    hold.abort();
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-f-ttl/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["metrics"]["matched"], 1, "{body}");
    assert!(
        body["metrics"]["dropped"].as_u64().unwrap_or(0) >= 1,
        "expiry drops are counted: {body}"
    );
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/ro01-f-ttl-rule",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/ro01-f-ttl",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);

    // `inflight_window` of one still batches two rows before the send.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "ro01-f-inflight",
            "type": "webhook",
            "connector": "ro01-fields",
            "parameters": {"path": "/f-inflight"},
            "resource_opts": {
                "batch_size": 2,
                "batch_time": "5s",
                "inflight_window": 1,
                "worker_pool_size": 2,
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "inflight action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-f-inflight-rule",
            "sql": "SELECT * FROM \"ro01/in/f-inflight\"",
            "actions": ["ro01-f-inflight"],
        }),
    )
    .await;
    assert_eq!(status, 201, "inflight rule create: {body}");
    let mut hold = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/f-inflight", 0, 0, false, br#"{"n":1}"#),
    )
    .await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), &mut hold)
            .await
            .is_err(),
        "one row waits for the inflight batch"
    );
    hold.abort();
    let delivered = mv01_spawn_http_receiver(Arc::clone(&listener), 2);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/f-inflight", 0, 0, false, br#"{"n":2}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, delivered)
        .await
        .expect("inflight delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 2, "both rows reached the target");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-f-inflight/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["metrics"]["matched"], 2, "{body}");
    assert_eq!(body["metrics"]["success"], 2, "{body}");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/ro01-f-inflight-rule",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/ro01-f-inflight",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);

    // `query_mode` sync flushes one row before the publish returns.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "ro01-f-sync",
            "type": "webhook",
            "connector": "ro01-fields",
            "parameters": {"path": "/f-sync"},
            "resource_opts": {
                "batch_size": 100,
                "batch_time": "10s",
                "query_mode": "sync",
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "sync action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-f-sync-rule",
            "sql": "SELECT * FROM \"ro01/in/f-sync\"",
            "actions": ["ro01-f-sync"],
        }),
    )
    .await;
    assert_eq!(status, 201, "sync rule create: {body}");
    let delivered = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/f-sync", 0, 0, false, br#"{"n":1}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, delivered)
        .await
        .expect("sync delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 1, "sync flushes before it returns");
    assert_eq!(
        received[0].0, "/f-sync",
        "the sync action path: {received:?}"
    );
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/ro01-f-sync-rule",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/ro01-f-sync",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);

    // `worker_pool_size` of two still delivers a batch of four.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "ro01-f-worker",
            "type": "webhook",
            "connector": "ro01-fields",
            "parameters": {"path": "/f-worker"},
            "resource_opts": {
                "batch_size": 4,
                "batch_time": "5s",
                "worker_pool_size": 2,
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "worker action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-f-worker-rule",
            "sql": "SELECT * FROM \"ro01/in/f-worker\"",
            "actions": ["ro01-f-worker"],
        }),
    )
    .await;
    assert_eq!(status, 201, "worker rule create: {body}");
    let mut hold = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    for value in [1, 2, 3] {
        let payload = format!("{{\"n\":{value}}}");
        send_mqtt(
            &mut publisher,
            &sy02_publish311("ro01/in/f-worker", 0, 0, false, payload.as_bytes()),
        )
        .await;
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(300), &mut hold)
            .await
            .is_err(),
        "three rows wait for the worker batch"
    );
    hold.abort();
    let delivered = mv01_spawn_http_receiver(Arc::clone(&listener), 4);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("ro01/in/f-worker", 0, 0, false, br#"{"n":4}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, delivered)
        .await
        .expect("worker delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 4, "the worker batch reached the target");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-f-worker/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["metrics"]["matched"], 4, "{body}");
    assert_eq!(body["metrics"]["success"], 4, "{body}");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/ro01-f-worker-rule",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/ro01-f-worker",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    kernel.finish().await;
}

/// RO-01/A2-1/B2-1: `inflight_window` bounds the driver calls that run at
/// one time through a real publish. A window of two with one row per call
/// shows a peak of two concurrent HTTP requests. A sequential dispatch
/// shows one; an ignored window shows four.
#[tokio::test]
async fn ro01_inflight_window_bounds_concurrent_driver_calls_through_publish() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "ro01-inflight-conn",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "ro01-inflight-action",
            "type": "webhook",
            "connector": "ro01-inflight-conn",
            "parameters": {"path": "/inflight"},
            "resource_opts": {
                "batch_size": 4,
                "batch_time": "5s",
                "inflight_window": 2,
                "worker_pool_size": 1,
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-inflight-rule",
            "sql": "SELECT * FROM \"ro01/in/inflight\"",
            "actions": ["ro01-inflight-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let receiver = mv01_spawn_http_receiver_active(
        Arc::clone(&listener),
        4,
        std::time::Duration::from_millis(500),
    );
    let mut publisher = mv00_connect(&kernel, "ro01-inflight-pub").await;
    for value in [1, 2, 3, 4] {
        let payload = format!("{{\"n\":{value}}}");
        send_mqtt(
            &mut publisher,
            &sy02_publish311("ro01/in/inflight", 0, 0, false, payload.as_bytes()),
        )
        .await;
    }
    let (received, peak) = tokio::time::timeout(STEP, receiver)
        .await
        .expect("inflight delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 4, "the batch reached the target");
    assert_eq!(
        peak, 2,
        "the inflight window bounds the concurrent driver calls"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-inflight-action/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["metrics"]["matched"], 4, "{body}");
    assert_eq!(body["metrics"]["success"], 4, "{body}");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/ro01-inflight-rule",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/ro01-inflight-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/connectors/ro01-inflight-conn",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    kernel.finish().await;
}

/// RO-01/B2-2: `worker_pool_size` caps one driver batch through a real
/// publish. Four rows with a worker size of two reach the driver as two
/// batches of two. An ignored worker size sends one batch of four.
#[tokio::test]
async fn ro01_worker_pool_size_caps_driver_batches_through_publish() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pg fake binds");
    let address = listener.local_addr().expect("pg fake address");
    let captured = Arc::new(Mutex::new(Vec::<(String, Vec<Vec<Vec<u8>>>)>::new()));
    let fake = {
        let captured = Arc::clone(&captured);
        tokio::spawn(async move {
            for _ in 0..2 {
                let stream = mv01_pg_accept(&listener).await;
                mv01_pg_serve_one(stream, Arc::clone(&captured), "u", "pwd", "db", 2).await;
            }
        })
    };
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "ro01-worker-pg",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "ro01-worker-action",
            "type": "pgsql",
            "connector": "ro01-worker-pg",
            "parameters": {"sql": "RO01-WORKER INSERT $1, $2, $3"},
            "resource_opts": {
                "batch_size": 4,
                "batch_time": "5s",
                "inflight_window": 1,
                "worker_pool_size": 2,
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "ro01-worker-rule",
            "sql": "SELECT * FROM \"ro01/in/worker\"",
            "actions": ["ro01-worker-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "ro01-worker-pub").await;
    for value in [1, 2, 3, 4] {
        let payload = format!("{{\"n\":{value}}}");
        send_mqtt(
            &mut publisher,
            &sy02_publish311("ro01/in/worker", 0, 0, false, payload.as_bytes()),
        )
        .await;
    }
    tokio::time::timeout(STEP, fake)
        .await
        .expect("both driver batches in time")
        .expect("pg fake task");
    let captured = std::sync::Mutex::into_inner(Arc::try_unwrap(captured).expect("sole owner"))
        .expect("capture lock");
    assert_eq!(
        captured.len(),
        2,
        "worker_pool_size two splits four rows into two driver batches"
    );
    assert!(
        captured.iter().all(|(_, rows)| rows.len() == 2),
        "each driver batch holds two rows"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/ro01-worker-action/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["metrics"]["matched"], 4, "{body}");
    assert_eq!(body["metrics"]["success"], 4, "{body}");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/ro01-worker-rule",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/ro01-worker-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/connectors/ro01-worker-pg",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    kernel.finish().await;
}

/// RO-01: each refused resource option answers 400 with the field name
/// and stores nothing through the management API.
#[tokio::test]
async fn ro01_action_resource_opts_refusals_through_the_api() {
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "ro01-refusal-hook",
            "type": "webhook",
            "url": "http://127.0.0.1:9",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let above_max = (tokio::sync::Semaphore::MAX_PERMITS as u64) + 1;
    let cases = [
        (
            "dispatch_strategy",
            serde_json::json!({"dispatch_strategy": "random"}),
        ),
        (
            "health_check_interval_jitter",
            serde_json::json!({"health_check_interval_jitter": "1s"}),
        ),
        (
            "health_check_timeout",
            serde_json::json!({"health_check_timeout": "30s"}),
        ),
        ("no_such_option", serde_json::json!({"no_such_option": 1})),
        ("batch_time", serde_json::json!({"batch_time": "infinity"})),
        (
            "max_buffer_bytes",
            serde_json::json!({"max_buffer_bytes": "0"}),
        ),
        (
            "inflight_window",
            serde_json::json!({"inflight_window": above_max}),
        ),
    ];
    for (field, opts) in cases {
        let name = format!("ro01-refused-{field}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/actions",
            serde_json::json!({
                "name": name,
                "type": "webhook",
                "connector": "ro01-refusal-hook",
                "parameters": {"path": "/new"},
                "resource_opts": opts,
            }),
        )
        .await;
        assert_eq!(status, 400, "{field} is refused: {body}");
        assert!(
            body["message"].as_str().unwrap_or("").contains(field),
            "the refusal names {field}: {body}"
        );
        let (status, _) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/actions/{name}"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 404, "a refused create stores nothing: {field}");
    }
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/connectors/ro01-refusal-hook",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    kernel.finish().await;
}

// -- Fake PostgreSQL for the action-owned pgsql sink -----------------------

/// Read one PostgreSQL message (tag plus body) from the fake server stream.
async fn mv01_pg_read_msg(stream: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    use tokio::io::AsyncReadExt;
    let mut header = [0_u8; 5];
    stream.read_exact(&mut header).await.expect("pg head");
    let len = i32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
    let mut body = vec![0_u8; len - 4];
    stream.read_exact(&mut body).await.expect("pg body");
    (header[0], body)
}

/// Write one PostgreSQL message to the fake server stream.
async fn mv01_pg_write_msg(stream: &mut tokio::net::TcpStream, tag: u8, body: &[u8]) {
    use tokio::io::AsyncWriteExt;
    let mut frame = vec![tag];
    frame.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    frame.extend_from_slice(body);
    stream.write_all(&frame).await.expect("pg write");
}

/// Read the startup parameter block and return the (key, value) pairs.
async fn mv01_pg_read_startup(stream: &mut tokio::net::TcpStream) -> Vec<(String, String)> {
    use tokio::io::AsyncReadExt;
    let mut len_buf = [0_u8; 4];
    stream.read_exact(&mut len_buf).await.expect("startup len");
    let len = i32::from_be_bytes(len_buf) as usize;
    let mut body = vec![0_u8; len - 4];
    stream.read_exact(&mut body).await.expect("startup body");
    mv01_pg_startup_params(&body)
}

/// Parse one startup body (protocol version plus parameters).
fn mv01_pg_startup_params(body: &[u8]) -> Vec<(String, String)> {
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

async fn mv01_pg_ready(stream: &mut tokio::net::TcpStream) {
    mv01_pg_write_msg(stream, b'Z', b"I").await;
}

/// One captured engine row per statement: the bound statement plus the
/// rows the sink flushed (each row: topic, QoS, payload).
type PgCaptured = Arc<Mutex<Vec<(String, Vec<Vec<Vec<u8>>>)>>>;

/// Accept one real protocol connection. A reachability probe connects
/// and closes without a byte; skip it so the fake serves the driver.
async fn mv01_pg_accept(listener: &tokio::net::TcpListener) -> tokio::net::TcpStream {
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
async fn mv01_pg_serve_one(
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

/// Two `pgsql` actions on one connector own separate statement shapes: each
/// delivers the engine row (topic, QoS, payload) under its own Parameters,
/// and the per-action counters attribute the fires.
#[tokio::test]
async fn mv01_pgsql_action_delivers_engine_rows_to_connector_settings() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pg fake binds");
    let address = listener.local_addr().expect("pg fake address");
    let captured = Arc::new(Mutex::new(Vec::<(String, Vec<Vec<Vec<u8>>>)>::new()));
    let fake = {
        let captured = Arc::clone(&captured);
        tokio::spawn(async move {
            for _ in 0..2 {
                let stream = mv01_pg_accept(&listener).await;
                mv01_pg_serve_one(stream, Arc::clone(&captured), "u", "pwd", "db", 1).await;
            }
        })
    };
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-pg",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    for (suffix, sentinel) in [("a", "MV01-A-SQL"), ("b", "MV01-B-SQL")] {
        let id = format!("mv01-pg-{suffix}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/actions",
            serde_json::json!({
                "name": id,
                "type": "pgsql",
                "connector": "mv01-pg",
                "parameters": {"sql": format!("{sentinel} INSERT $1, $2, $3")},
            }),
        )
        .await;
        assert_eq!(status, 201, "action {id} create: {body}");
        assert_eq!(body["status"], "connected", "action {id}: {body}");
        let rule_id = format!("mv01-pg-rule-{suffix}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/rules",
            serde_json::json!({
                "id": rule_id,
                "sql": format!("SELECT * FROM \"mv01/in/{suffix}\""),
                "actions": [id],
            }),
        )
        .await;
        assert_eq!(status, 201, "rule {rule_id} create: {body}");
    }
    // One publish per action fires its own rule; each action's pool dials
    // the fake backend and flushes its own statement with the engine row.
    for (suffix, payload) in [("a", &b"{\"v\":1}"[..]), ("b", &b"{\"v\":2}"[..])] {
        let mut publisher = mv00_connect(&kernel, &format!("mv01-pg-pub-{suffix}")).await;
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&format!("mv01/in/{suffix}"), 0, 0, false, payload),
        )
        .await;
        drop(publisher);
    }
    // The sink flusher outside this test drives both pools; bound the wait.
    let deadline = std::time::Instant::now() + STEP;
    loop {
        let arrived = captured.lock().expect("capture lock").len() == 2;
        assert!(
            std::time::Instant::now() < deadline || arrived,
            "both pgsql actions flushed in time"
        );
        if arrived {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    fake.await.expect("pg fake task");
    let captured = std::sync::Mutex::into_inner(Arc::try_unwrap(captured).expect("sole owner"))
        .expect("capture lock");
    // Per-action counters attributed one success each.
    for suffix in ["a", "b"] {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/actions/mv01-pg-{suffix}/metrics"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "action {suffix} metrics: {body}");
        assert_eq!(body["metrics"]["success"], serde_json::json!(1), "{body}");
        assert_eq!(body["metrics"]["failed"], serde_json::json!(0), "{body}");
    }
    // Both actions delivered under their own statements with the engine
    // row (topic, QoS, payload) bound by the sink.
    assert_eq!(captured.len(), 2);
    for (statement, rows) in &captured {
        assert!(rows.len() == 1, "one row per statement: {rows:?}");
        let row = &rows[0];
        assert_eq!(row.len(), 3, "topic, qos, payload bound: {row:?}");
        assert_eq!(row[1], &b"0"[..]);
        let (topic, payload) = if statement.contains("MV01-A-SQL") {
            ("mv01/in/a", "{\"v\":1}".as_bytes())
        } else if statement.contains("MV01-B-SQL") {
            ("mv01/in/b", "{\"v\":2}".as_bytes())
        } else {
            unreachable!("each connection binds its action's statement: {statement:?}");
        };
        assert_eq!(row[0], topic.as_bytes());
        assert_eq!(row[2], payload);
    }
    kernel.finish().await;
}

/// An action update swaps the statement live (new publishes use it); a
/// refused update (missing `sql`) answers 400 naming the field and the
/// old statement keeps delivering.
#[tokio::test]
async fn mv01_pgsql_action_update_swaps_statement_and_refusal_keeps_old() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pg fake binds");
    let address = listener.local_addr().expect("pg fake address");
    let captured = Arc::new(Mutex::new(Vec::<(String, Vec<Vec<Vec<u8>>>)>::new()));
    let fake = {
        let captured = Arc::clone(&captured);
        tokio::spawn(async move {
            for _ in 0..3 {
                let stream = mv01_pg_accept(&listener).await;
                mv01_pg_serve_one(stream, Arc::clone(&captured), "u", "pwd", "db", 1).await;
            }
        })
    };
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-pg-up",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-pg-up",
            "type": "pgsql",
            "connector": "mv01-pg-up",
            "parameters": {"sql": "MV01-PGUP-V1 INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-pg-up-rule",
            "sql": "SELECT * FROM \"mv01/in/up\"",
            "actions": ["mv01-pg-up"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-pg-up-pub").await;
    // The first publish flushes the created statement.
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/up", 0, 0, false, br#"{"u":1}"#),
    )
    .await;
    // An update that drops `sql` is refused with the field named; the
    // stored action and the delivering statement are untouched.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-pg-up",
        serde_json::json!({
            "name": "mv01-pg-up",
            "type": "pgsql",
            "connector": "mv01-pg-up",
            "parameters": {},
        }),
    )
    .await;
    assert_eq!(status, 400, "missing sql refuses: {body}");
    let message = body["message"].as_str().expect("error message");
    assert!(
        message.contains("mv01-pg-up") && message.contains("parameters.sql"),
        "the refusal names the action and the field: {message}"
    );
    // The old generation kept delivering (a fresh backend connection).
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/up", 0, 0, false, br#"{"u":2}"#),
    )
    .await;
    // Wait for the refused-update publish to flush on the old sink before
    // the replacement swaps it. A publish still in the broker task would
    // otherwise land on the new statement.
    mv01_wait_pg_batches(&captured, 2).await;
    // A full replacement swaps the statement in for the next publish.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-pg-up",
        serde_json::json!({
            "name": "mv01-pg-up",
            "type": "pgsql",
            "connector": "mv01-pg-up",
            "parameters": {"sql": "MV01-PGUP-V2 INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 200, "action update: {body}");
    assert_eq!(body["status"], "connected", "{body}");
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/up", 0, 0, false, br#"{"u":3}"#),
    )
    .await;
    // Bound the wait for all three flushes.
    let deadline = std::time::Instant::now() + STEP;
    loop {
        let arrived = captured.lock().expect("capture lock").len() == 3;
        assert!(
            std::time::Instant::now() < deadline || arrived,
            "all three statements flushed in time"
        );
        if arrived {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    fake.await.expect("pg fake task");
    let captured = std::sync::Mutex::into_inner(Arc::try_unwrap(captured).expect("sole owner"))
        .expect("capture lock");
    // Flush order follows connection order: created, refused-update
    // still old, updated new. The prepared statement carries the
    // driver's generated name; the sql follows after the separator.
    let sql_of = |index: usize| -> String {
        captured[index]
            .0
            .split_once('|')
            .expect("statement capture")
            .1
            .to_string()
    };
    assert_eq!(sql_of(0), "MV01-PGUP-V1 INSERT $1, $2, $3");
    assert_eq!(sql_of(1), "MV01-PGUP-V1 INSERT $1, $2, $3");
    assert_eq!(sql_of(2), "MV01-PGUP-V2 INSERT $1, $2, $3");
    // Every flush carries the engine row (topic, QoS, payload) bound by
    // the action to its own publish.
    let payloads: [&[u8]; 3] = [br#"{"u":1}"#, br#"{"u":2}"#, br#"{"u":3}"#];
    for (index, expected_payload) in payloads.iter().enumerate() {
        let rows = &captured[index].1;
        assert!(rows.len() == 1, "one row per flush: {rows:?}");
        let row = &rows[0];
        assert_eq!(row.len(), 3, "topic, qos, payload bound: {row:?}");
        assert_eq!(row[0], &b"mv01/in/up"[..]);
        assert_eq!(row[1], &b"0"[..]);
        assert_eq!(row[2], *expected_payload);
    }
    kernel.finish().await;
}

/// Wait for one more captured PostgreSQL batch. The fake flushes one
/// batch per connection, so the count is the number of completed sends.
async fn mv01_wait_pg_batches(captured: &PgCaptured, count: usize) {
    let deadline = std::time::Instant::now() + STEP;
    loop {
        if captured.lock().expect("capture lock").len() >= count {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the pgsql batches arrived in time: wanted {count}, got {}",
            captured.lock().expect("capture lock").len()
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// A pgsql action stops on disable, resumes on enable and stops on delete.
#[tokio::test]
async fn mv01_pgsql_action_disable_enable_delete() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pg fake binds");
    let address = listener.local_addr().expect("pg fake address");
    let captured: PgCaptured = Arc::new(Mutex::new(Vec::new()));
    let fake_captured = Arc::clone(&captured);
    let fake = tokio::spawn(async move {
        loop {
            let stream = mv01_pg_accept(&listener).await;
            mv01_pg_serve_one(stream, Arc::clone(&fake_captured), "u", "pwd", "db", 1).await;
        }
    });
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-pg-life",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-pg-life",
            "type": "pgsql",
            "connector": "mv01-pg-life",
            "parameters": {"sql": "MV01-LIFE INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-pg-life-rule",
            "sql": "SELECT * FROM \"mv01/in/life\"",
            "actions": ["mv01-pg-life"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-pg-life-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/life", 0, 0, false, br#"{"n":1}"#),
    )
    .await;
    mv01_wait_pg_batches(&captured, 1).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-pg-life/enable/false",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "disable: {body}");
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/life", 0, 0, false, br#"{"n":2}"#),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    assert_eq!(
        captured.lock().expect("capture lock").len(),
        1,
        "a disabled action delivers nothing"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-pg-life/enable/true",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "enable: {body}");
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/life", 0, 0, false, br#"{"n":3}"#),
    )
    .await;
    mv01_wait_pg_batches(&captured, 2).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/mv01-pg-life-rule",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "rule delete: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/mv01-pg-life",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "action delete: {body}");
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/life", 0, 0, false, br#"{"n":4}"#),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    assert_eq!(
        captured.lock().expect("capture lock").len(),
        2,
        "a deleted action delivers nothing"
    );
    fake.abort();
    kernel.finish().await;
}

/// A pgsql action stores while its server is down, reports `disconnected`
/// from the real attempt, and delivers after the server starts.
#[tokio::test]
async fn mv01_pgsql_down_server_reports_disconnected_and_delivers_when_up() {
    let reserve = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a port");
    let address = reserve.local_addr().expect("reserved address");
    drop(reserve);
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-pg-down",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-pg-down",
            "type": "pgsql",
            "connector": "mv01-pg-down",
            "parameters": {"sql": "MV01-DOWN INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 201, "a down target still stores: {body}");
    assert_eq!(body["status"], "disconnected", "{body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-pg-down-rule",
            "sql": "SELECT * FROM \"mv01/in/pgdown\"",
            "actions": ["mv01-pg-down"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("the server starts on the reserved port");
    let captured: PgCaptured = Arc::new(Mutex::new(Vec::new()));
    let fake_captured = Arc::clone(&captured);
    let fake = tokio::spawn(async move {
        loop {
            let stream = mv01_pg_accept(&listener).await;
            mv01_pg_serve_one(stream, Arc::clone(&fake_captured), "u", "pwd", "db", 1).await;
        }
    });
    let mut publisher = mv00_connect(&kernel, "mv01-pg-down-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/pgdown", 0, 0, false, br#"{"up":1}"#),
    )
    .await;
    mv01_wait_pg_batches(&captured, 1).await;
    fake.abort();
    kernel.finish().await;
}

/// A pgsql action delivers after a broker restart from the stored
/// connector and action.
#[tokio::test]
async fn mv01_pgsql_action_survives_restart() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pg fake binds");
    let address = listener.local_addr().expect("pg fake address");
    let captured: PgCaptured = Arc::new(Mutex::new(Vec::new()));
    let fake_captured = Arc::clone(&captured);
    let fake = tokio::spawn(async move {
        loop {
            let stream = mv01_pg_accept(&listener).await;
            mv01_pg_serve_one(stream, Arc::clone(&fake_captured), "u", "pwd", "db", 1).await;
        }
    });
    let mut kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-pg-restart",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-pg-restart",
            "type": "pgsql",
            "connector": "mv01-pg-restart",
            "parameters": {"sql": "MV01-RESTART INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-pg-restart-rule",
            "sql": "SELECT * FROM \"mv01/in/pgrestart\"",
            "actions": ["mv01-pg-restart"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-pg-restart-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/pgrestart", 0, 0, false, br#"{"r":1}"#),
    )
    .await;
    mv01_wait_pg_batches(&captured, 1).await;
    mv00_restart(&mut kernel).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-pg-restart",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "connected", "the replay rebuilt it: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-pg-restart-pub2").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/pgrestart", 0, 0, false, br#"{"r":2}"#),
    )
    .await;
    mv01_wait_pg_batches(&captured, 2).await;
    {
        let captured = captured.lock().expect("capture lock");
        assert!(captured[0].0.contains("MV01-RESTART"));
        assert!(captured[1].0.contains("MV01-RESTART"));
    }
    fake.abort();
    kernel.finish().await;
}

/// A connector update refuses when one dependent pgsql action cannot use
/// the new settings; the old statement keeps delivering and the refused
/// attempt leaks no connection or task.
#[tokio::test]
async fn mv01_pgsql_refused_connector_update_keeps_old_and_leaks_nothing() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pg fake binds");
    let address = listener.local_addr().expect("pg fake address");
    let captured: PgCaptured = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(AtomicUsize::new(0));
    let fake_captured = Arc::clone(&captured);
    let fake_accepted = Arc::clone(&accepted);
    let fake = tokio::spawn(async move {
        loop {
            let stream = mv01_pg_accept(&listener).await;
            fake_accepted.fetch_add(1, Ordering::SeqCst);
            mv01_pg_serve_one(stream, Arc::clone(&fake_captured), "u", "pwd", "db", 1).await;
        }
    });
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-pg-refuse",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-pg-refuse",
            "type": "pgsql",
            "connector": "mv01-pg-refuse",
            "parameters": {"sql": "MV01-REFUSE INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-pg-refuse-rule",
            "sql": "SELECT * FROM \"mv01/in/pgrefuse\"",
            "actions": ["mv01-pg-refuse"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-pg-refuse-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/pgrefuse", 0, 0, false, br#"{"v":1}"#),
    )
    .await;
    mv01_wait_pg_batches(&captured, 1).await;
    let accepted_before = accepted.load(Ordering::SeqCst);
    let tasks_before = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    // The new body drops `database`, so the dependent action cannot build.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/connectors/mv01-pg-refuse",
        serde_json::json!({
            "name": "mv01-pg-refuse",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "username": "u",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 400, "the dependent build refuses: {body}");
    let message = body["message"].as_str().expect("refusal message");
    assert!(
        message.contains("mv01-pg-refuse") && message.contains("database"),
        "the refusal names the action and the field: {message}"
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        accepted_before,
        "the refused update opens no connection"
    );
    let deadline = std::time::Instant::now() + STEP;
    let tasks_after = loop {
        let alive = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        if alive <= tasks_before || std::time::Instant::now() >= deadline {
            break alive;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };
    assert!(
        tasks_after <= tasks_before,
        "the refused update leaks no task: before {tasks_before}, after {tasks_after}"
    );
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/pgrefuse", 0, 0, false, br#"{"v":2}"#),
    )
    .await;
    mv01_wait_pg_batches(&captured, 2).await;
    {
        let captured = captured.lock().expect("capture lock");
        assert!(captured[1].0.contains("MV01-REFUSE"));
    }
    fake.abort();
    kernel.finish().await;
}

/// A successful connector update keeps two dependent actions on their own
/// statements.
#[tokio::test]
async fn mv01_pgsql_connector_update_keeps_two_action_statements() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pg fake binds");
    let address = listener.local_addr().expect("pg fake address");
    let captured: PgCaptured = Arc::new(Mutex::new(Vec::new()));
    let fake_captured = Arc::clone(&captured);
    let fake = tokio::spawn(async move {
        loop {
            let stream = mv01_pg_accept(&listener).await;
            mv01_pg_serve_one(stream, Arc::clone(&fake_captured), "u", "pwd", "db", 1).await;
        }
    });
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-pg-two",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
            "pool_size": 2,
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    for (id, sql) in [
        ("mv01-pg-two-a", "MV01-TWO-A INSERT $1, $2, $3"),
        ("mv01-pg-two-b", "MV01-TWO-B INSERT $1, $2, $3"),
    ] {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/actions",
            serde_json::json!({
                "name": id,
                "type": "pgsql",
                "connector": "mv01-pg-two",
                "parameters": {"sql": sql},
            }),
        )
        .await;
        assert_eq!(status, 201, "action {id}: {body}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/rules",
            serde_json::json!({
                "id": format!("{id}-rule"),
                "sql": format!("SELECT * FROM \"mv01/in/two/{id}\""),
                "actions": [id],
            }),
        )
        .await;
        assert_eq!(status, 201, "rule {id}: {body}");
    }
    let mut publisher = mv00_connect(&kernel, "mv01-pg-two-pub").await;
    for id in ["mv01-pg-two-a", "mv01-pg-two-b"] {
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&format!("mv01/in/two/{id}"), 0, 0, false, id.as_bytes()),
        )
        .await;
    }
    mv01_wait_pg_batches(&captured, 2).await;
    // A second server observes the rebuild: after the update every row
    // must reach the new target. A no-op connector update fails here.
    let listener2 = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("second pg fake binds");
    let address2 = listener2.local_addr().expect("second pg fake address");
    let captured2: PgCaptured = Arc::new(Mutex::new(Vec::new()));
    let fake2_captured = Arc::clone(&captured2);
    let fake2 = tokio::spawn(async move {
        loop {
            let stream = mv01_pg_accept(&listener2).await;
            mv01_pg_serve_one(stream, Arc::clone(&fake2_captured), "u", "pwd", "db", 1).await;
        }
    });
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/connectors/mv01-pg-two",
        serde_json::json!({
            "name": "mv01-pg-two",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address2.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
            "pool_size": 4,
        }),
    )
    .await;
    assert_eq!(status, 200, "connector update: {body}");
    for id in ["mv01-pg-two-a", "mv01-pg-two-b"] {
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&format!("mv01/in/two/{id}"), 0, 0, false, id.as_bytes()),
        )
        .await;
    }
    mv01_wait_pg_batches(&captured2, 2).await;
    {
        let captured2 = captured2.lock().expect("capture lock");
        let statements: Vec<&str> = captured2
            .iter()
            .map(|(statement, _)| {
                statement
                    .split_once('|')
                    .map(|(_, sql)| sql)
                    .unwrap_or(statement.as_str())
            })
            .collect();
        assert_eq!(
            statements
                .iter()
                .filter(|sql| sql.contains("MV01-TWO-A"))
                .count(),
            1,
            "action A rebuilt onto the new server: {statements:?}"
        );
        assert_eq!(
            statements
                .iter()
                .filter(|sql| sql.contains("MV01-TWO-B"))
                .count(),
            1,
            "action B rebuilt onto the new server: {statements:?}"
        );
    }
    fake.abort();
    fake2.abort();
    kernel.finish().await;
}

/// A stored pgsql action whose password reference disappears reports the
/// error state and raises the build alarm at boot.
#[tokio::test]
async fn mv01_pgsql_failed_boot_wiring_reports_error_and_alarm() {
    let mut kernel = spawn_kernel("").await;
    let secret = kernel.scratch.join("mv01-pg-password");
    std::fs::write(&secret, "pwd").expect("secret file");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-pg-boot",
            "type": "postgres",
            "server": "127.0.0.1:9",
            "database": "db",
            "username": "u",
            "password": format!("file:{}", secret.display()),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-pg-boot",
            "type": "pgsql",
            "connector": "mv01-pg-boot",
            "parameters": {"sql": "MV01-BOOT INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    std::fs::remove_file(&secret).expect("remove the secret");
    mv00_restart(&mut kernel).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-pg-boot",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "the failed action stays readable: {body}");
    assert_eq!(body["status"], "disconnected", "{body}");
    let reason = body["status_reason"].as_str().expect("status reason");
    assert!(
        reason.contains("password"),
        "the reason names the field: {reason}"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/alarms",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "alarms read: {body}");
    let alarm = body["data"]
        .as_array()
        .expect("alarm rows")
        .iter()
        .find(|row| row["name"] == "action_build_failed:mv01-pg-boot")
        .expect("the boot failure raised its alarm");
    assert!(
        alarm["message"]
            .as_str()
            .is_some_and(|text| text.contains("password")),
        "the alarm carries the build reason: {alarm}"
    );
    kernel.finish().await;
}

/// A password and a database that contain URL delimiters reach the
/// driver as values, not as a hand-joined URL.
#[tokio::test]
async fn mv01_pgsql_special_password_and_database_reach_driver() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pg fake binds");
    let address = listener.local_addr().expect("pg fake address");
    let captured: PgCaptured = Arc::new(Mutex::new(Vec::new()));
    let fake_captured = Arc::clone(&captured);
    let fake = tokio::spawn(async move {
        loop {
            let stream = mv01_pg_accept(&listener).await;
            mv01_pg_serve_one(
                stream,
                Arc::clone(&fake_captured),
                "u",
                "p@ss/w?rd#1",
                "db@x/y?z#w",
                1,
            )
            .await;
        }
    });
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-pg-special",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db@x/y?z#w",
            "username": "u",
            "password": "p@ss/w?rd#1",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-pg-special",
            "type": "pgsql",
            "connector": "mv01-pg-special",
            "parameters": {"sql": "MV01-SPECIAL INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-pg-special-rule",
            "sql": "SELECT * FROM \"mv01/in/special\"",
            "actions": ["mv01-pg-special"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-pg-special-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/special", 0, 0, false, br#"{"s":1}"#),
    )
    .await;
    mv01_wait_pg_batches(&captured, 1).await;
    fake.abort();
    kernel.finish().await;
}

/// A buffered batch survives an update, a disable and a delete: each
/// swap drains the old runtime before it stops it.
#[tokio::test]
async fn mv01_action_buffer_drains_on_update_disable_and_delete() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-drain",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let opts = serde_json::json!({
        "batch_size": 3,
        "batch_time": "10s",
        "health_check_interval": "1h",
    });
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-drain",
            "type": "webhook",
            "connector": "mv01-drain",
            "parameters": {"path": "/drain"},
            "resource_opts": opts,
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-drain-rule",
            "sql": "SELECT * FROM \"mv01/in/drain\"",
            "actions": ["mv01-drain"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-drain-pub").await;
    // One row waits in the old buffer; the update drains it to /drain.
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/drain", 0, 0, false, br#"{"d":1}"#),
    )
    .await;
    let first = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-drain",
        serde_json::json!({
            "name": "mv01-drain",
            "type": "webhook",
            "connector": "mv01-drain",
            "parameters": {"path": "/after"},
            "resource_opts": opts,
        }),
    )
    .await;
    assert_eq!(status, 200, "action update: {body}");
    let received = tokio::time::timeout(STEP, first)
        .await
        .expect("updated sink drained in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].0, "/drain");
    assert_eq!(received[0].1.as_slice(), &br#"{"d":1}"#[..]);
    // Two rows wait in the new buffer; the disable drains them.
    for n in [2, 3] {
        let payload = format!("{{\"d\":{n}}}");
        send_mqtt(
            &mut publisher,
            &sy02_publish311("mv01/in/drain", 0, 0, false, payload.as_bytes()),
        )
        .await;
    }
    let second = mv01_spawn_http_receiver(Arc::clone(&listener), 2);
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-drain/enable/false",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "disable: {body}");
    let received = tokio::time::timeout(STEP, second)
        .await
        .expect("disabled sink drained in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 2);
    assert!(received.iter().all(|(path, _)| path == "/after"));
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-drain/enable/true",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "enable: {body}");
    // Two rows wait again; the delete drains them after the rule detach.
    for n in [4, 5] {
        let payload = format!("{{\"d\":{n}}}");
        send_mqtt(
            &mut publisher,
            &sy02_publish311("mv01/in/drain", 0, 0, false, payload.as_bytes()),
        )
        .await;
    }
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/rules/mv01-drain-rule",
        serde_json::json!({
            "sql": "SELECT * FROM \"mv01/in/drain\"",
            "actions": [],
        }),
    )
    .await;
    assert_eq!(status, 200, "rule detach: {body}");
    let third = mv01_spawn_http_receiver(Arc::clone(&listener), 2);
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/mv01-drain",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "action delete: {body}");
    let received = tokio::time::timeout(STEP, third)
        .await
        .expect("deleted sink drained in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 2);
    assert!(received.iter().all(|(path, _)| path == "/after"));
    kernel.finish().await;
}

/// A connector disable stops every dependent action; enable restores
/// them. Deleting a connector that an action still uses is refused and
/// leaves both records in place.
#[tokio::test]
async fn mv01_connector_disable_mirrors_dependents_and_delete_guards_references() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-cd",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-cd-action",
            "type": "webhook",
            "connector": "mv01-cd",
            "parameters": {"path": "/cd"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-cd-rule",
            "sql": "SELECT * FROM \"mv01/in/cd\"",
            "actions": ["mv01-cd-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-cd-pub").await;
    let first = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/cd", 0, 0, false, br#"{"d":1}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, first)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received[0].0, "/cd");
    assert_eq!(received[0].1.as_slice(), &br#"{"d":1}"#[..]);
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/connectors/mv01-cd/enable/false",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "connector disable: {body}");
    let watch = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/cd", 0, 0, false, br#"{"d":2}"#),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(
        !watch.is_finished(),
        "a disabled connector still delivered through its dependent action"
    );
    watch.abort();
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/connectors/mv01-cd/enable/true",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "connector enable: {body}");
    let second = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/cd", 0, 0, false, br#"{"d":3}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, second)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received[0].0, "/cd");
    assert_eq!(received[0].1.as_slice(), &br#"{"d":3}"#[..]);
    // The used connector cannot be deleted while the action points at it.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/connectors/mv01-cd",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 400, "used connector delete refuses: {body}");
    let message = body["message"].as_str().expect("error message");
    assert!(
        message.contains("mv01-cd-action") && message.contains("action"),
        "the refusal names the dependent action: {message}"
    );
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/connectors/mv01-cd",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "the refused delete keeps the connector");
    // Detach the rule, then the action and its connector can go.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/rules/mv01-cd-rule",
        serde_json::json!({
            "sql": "SELECT * FROM \"mv01/in/cd\"",
            "actions": [],
        }),
    )
    .await;
    assert_eq!(status, 200, "rule detach: {body}");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/mv01-cd-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "action delete after detach");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/connectors/mv01-cd",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "connector delete after the action is gone");
    kernel.finish().await;
}

/// Publishes continue across an action update: every message reaches
/// exactly one statement, and a later delete stops the action.
#[tokio::test]
async fn mv01_concurrent_publish_update_and_delete_keep_each_message_once() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-conc",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-conc-action",
            "type": "webhook",
            "connector": "mv01-conc",
            "parameters": {"path": "/before"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-conc-rule",
            "sql": "SELECT * FROM \"mv01/in/conc\"",
            "actions": ["mv01-conc-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let payloads: Vec<String> = (0..16).map(|index| format!("{{\"n\":{index}}}")).collect();
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), payloads.len());
    let publisher = mv00_connect(&kernel, "mv01-conc-pub").await;
    let send_payloads = payloads.clone();
    let publisher_task = tokio::spawn(async move {
        let mut publisher = publisher;
        for payload in send_payloads {
            send_mqtt(
                &mut publisher,
                &sy02_publish311("mv01/in/conc", 0, 0, false, payload.as_bytes()),
            )
            .await;
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-conc-action",
        serde_json::json!({
            "name": "mv01-conc-action",
            "type": "webhook",
            "connector": "mv01-conc",
            "parameters": {"path": "/after"},
        }),
    )
    .await;
    assert_eq!(status, 200, "action update during publishes: {body}");
    publisher_task.await.expect("publisher task");
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(
        received.len(),
        payloads.len(),
        "every publish delivered once"
    );
    let mut got: Vec<String> = received
        .iter()
        .map(|(_, body)| String::from_utf8(body.clone()).expect("payload text"))
        .collect();
    let mut want = payloads.clone();
    got.sort();
    want.sort();
    assert_eq!(got, want, "no publish lost or duplicated across the swap");
    for (path, _) in &received {
        assert!(
            path.as_str() == "/before" || path.as_str() == "/after",
            "unexpected delivery path {path}"
        );
    }
    // Detach the rule, then delete the action; a later publish is silent.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/rules/mv01-conc-rule",
        serde_json::json!({
            "sql": "SELECT * FROM \"mv01/in/conc\"",
            "actions": [],
        }),
    )
    .await;
    assert_eq!(status, 200, "rule detach: {body}");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/mv01-conc-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "action delete");
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-conc-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 404, "the deleted action reads absent");
    let watch = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    let mut publisher = mv00_connect(&kernel, "mv01-conc-pub2").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/conc", 0, 0, false, br#"{"n":"gone"}"#),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(!watch.is_finished(), "a deleted action still delivered");
    watch.abort();
    kernel.finish().await;
}

/// A persistence failure on an action update keeps the old sink and the
/// old stored parameters, and answers 500.
#[tokio::test]
async fn mv01_action_persist_failure_keeps_the_old_sink_delivering() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-persist",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-persist-action",
            "type": "webhook",
            "connector": "mv01-persist",
            "parameters": {"path": "/persist"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-persist-rule",
            "sql": "SELECT * FROM \"mv01/in/persist\"",
            "actions": ["mv01-persist-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-persist-pub").await;
    let first = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/persist", 0, 0, false, br#"{"p":1}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, first)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received[0].1.as_slice(), &br#"{"p":1}"#[..]);
    // Block the atomic save: the state path becomes a directory, so the
    // temp-file rename fails for every later persist.
    let state_path = kernel.scratch.join("state.toml");
    std::fs::remove_file(&state_path).expect("state file exists after the writes");
    std::fs::create_dir(&state_path).expect("state path blocks the rename");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-persist-action",
        serde_json::json!({
            "name": "mv01-persist-action",
            "type": "webhook",
            "connector": "mv01-persist",
            "parameters": {"path": "/persist-new"},
        }),
    )
    .await;
    assert_eq!(status, 500, "the persistence failure answers 500: {body}");
    assert_eq!(body["code"], "INTERNAL_ERROR", "{body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-persist-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["parameters"]["path"], "/persist", "{body}");
    let second = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/persist", 0, 0, false, br#"{"p":2}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, second)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received[0].0, "/persist");
    assert_eq!(received[0].1.as_slice(), &br#"{"p":2}"#[..]);
    kernel.finish().await;
}

/// A persistence failure on connector create and update keeps the store
/// and every sink as they were.
#[tokio::test]
async fn mv01_connector_persist_failure_keeps_the_previous_generation() {
    let old_listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("old receiver binds"),
    );
    let old_address = old_listener.local_addr().expect("old receiver address");
    let new_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("new receiver binds");
    let new_address = new_listener.local_addr().expect("new receiver address");
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-conn-persist",
            "type": "webhook",
            "url": format!("http://{old_address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-conn-persist",
            "type": "webhook",
            "connector": "mv01-conn-persist",
            "parameters": {"path": "/keep"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-conn-persist-rule",
            "sql": "SELECT * FROM \"mv01/in/connpersist\"",
            "actions": ["mv01-conn-persist"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-conn-persist-pub").await;
    let first = mv01_spawn_http_receiver(Arc::clone(&old_listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/connpersist", 0, 0, false, br#"{"c":1}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, first)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received[0].1.as_slice(), &br#"{"c":1}"#[..]);
    // Block the atomic save for every later write.
    let state_path = kernel.scratch.join("state.toml");
    std::fs::remove_file(&state_path).expect("state file exists after the writes");
    std::fs::create_dir(&state_path).expect("state path blocks the rename");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/connectors/mv01-conn-persist",
        serde_json::json!({
            "name": "mv01-conn-persist",
            "type": "webhook",
            "url": format!("http://{new_address}"),
        }),
    )
    .await;
    assert_eq!(status, 500, "the update persist failure: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/connectors/mv01-conn-persist",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["url"],
        serde_json::json!(format!("http://{old_address}")),
        "the stored connector is unchanged: {body}"
    );
    let second = mv01_spawn_http_receiver(Arc::clone(&old_listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/connpersist", 0, 0, false, br#"{"c":2}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, second)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received[0].0, "/keep");
    assert_eq!(received[0].1.as_slice(), &br#"{"c":2}"#[..]);
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-conn-persist-new",
            "type": "webhook",
            "url": format!("http://{new_address}"),
        }),
    )
    .await;
    assert_eq!(status, 500, "the create persist failure: {body}");
    let (status, _body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/connectors/mv01-conn-persist-new",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 404, "the uncommitted connector is not stored");
    kernel.finish().await;
}

/// One rule mixes an owned `pgsql` action, an owned `webhook` action and
/// a kind with no owned runtime: both owned sinks deliver and the
/// refused kind counts one failed fire.
#[tokio::test]
async fn mv01_mixed_kind_rule_delivers_owned_actions_and_counts_refused_failure() {
    let pg_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pg fake binds");
    let pg_address = pg_listener.local_addr().expect("pg fake address");
    let captured: PgCaptured = Arc::new(Mutex::new(Vec::new()));
    let pg_captured = Arc::clone(&captured);
    let pg_fake = tokio::spawn(async move {
        let stream = mv01_pg_accept(&pg_listener).await;
        mv01_pg_serve_one(stream, pg_captured, "u", "pwd", "db", 1).await;
    });
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-mixed-pg",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", pg_address.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 201, "pgsql connector: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-mixed-pg-act",
            "type": "pgsql",
            "connector": "mv01-mixed-pg",
            "parameters": {"sql": "MV01-MIXED INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 201, "pgsql action: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-mixed-wh",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "webhook connector: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-mixed-wh-act",
            "type": "webhook",
            "connector": "mv01-mixed-wh",
            "parameters": {"path": "/mixed"},
        }),
    )
    .await;
    assert_eq!(status, 201, "webhook action: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-mixed-refused",
            "type": "console",
        }),
    )
    .await;
    assert_eq!(status, 201, "unowned kind action stores: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-mixed-rule",
            "sql": "SELECT * FROM \"mv01/in/mixed\"",
            "actions": ["mv01-mixed-pg-act", "mv01-mixed-wh-act", "mv01-mixed-refused"],
        }),
    )
    .await;
    assert_eq!(status, 201, "mixed rule create: {body}");
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    let mut publisher = mv00_connect(&kernel, "mv01-mixed-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/mixed", 0, 0, false, br#"{"m":1}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received[0].0, "/mixed");
    assert_eq!(received[0].1.as_slice(), &br#"{"m":1}"#[..]);
    tokio::time::timeout(STEP, pg_fake)
        .await
        .expect("pgsql delivery in time")
        .expect("pg fake task");
    let captured = std::sync::Mutex::into_inner(Arc::try_unwrap(captured).expect("sole owner"))
        .expect("capture lock");
    assert_eq!(captured.len(), 1, "the pgsql action delivered once");
    assert_eq!(captured[0].1.len(), 1, "one engine row");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-mixed-refused/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "refused-kind metrics: {body}");
    assert_eq!(body["metrics"]["success"], serde_json::json!(0), "{body}");
    assert_eq!(body["metrics"]["failed"], serde_json::json!(1), "{body}");
    kernel.finish().await;
}

/// A stored action whose sink cannot build at boot reports the error state
/// and raises the build alarm through the production `/alarms` entry.
#[tokio::test]
async fn mv01_failed_boot_action_wiring_reports_error_and_alarm() {
    let mut kernel = spawn_kernel("").await;
    let secret = kernel.scratch.join("mv01-boot-url");
    std::fs::write(&secret, "http://127.0.0.1:9").expect("secret file");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-boot-link",
            "type": "webhook",
            "url": format!("file:{}", secret.display()),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-boot-fail",
            "type": "webhook",
            "connector": "mv01-boot-link",
            "parameters": {"path": "/boot"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    assert_eq!(
        body["status"], "disconnected",
        "the secret resolves to a down target and the probe reports it: {body}"
    );
    // The secret disappears before the restart, so the boot replay cannot
    // rebuild the action sink.
    std::fs::remove_file(&secret).expect("remove the secret");
    mv00_restart(&mut kernel).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-boot-fail",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "the failed action stays readable: {body}");
    assert_eq!(body["status"], "disconnected", "{body}");
    let reason = body["status_reason"].as_str().expect("status reason");
    assert!(
        reason.contains("url"),
        "the reason names the field: {reason}"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/alarms",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "alarms read: {body}");
    let alarm = body["data"]
        .as_array()
        .expect("alarm rows")
        .iter()
        .find(|row| row["name"] == "action_build_failed:mv01-boot-fail")
        .expect("the boot failure raised its alarm");
    assert!(
        alarm["message"]
            .as_str()
            .is_some_and(|text| text.contains("url")),
        "the alarm carries the build reason: {alarm}"
    );
    kernel.finish().await;
}

/// A valid webhook server that is down at write time does not refuse the
/// action; the stored action delivers once the server starts.
#[tokio::test]
async fn mv01_down_webhook_server_stores_the_action_and_delivers_when_up() {
    let reserve = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a port");
    let address = reserve.local_addr().expect("reserved address");
    drop(reserve);
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-down",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-down-action",
            "type": "webhook",
            "connector": "mv01-down",
            "parameters": {"path": "/down"},
        }),
    )
    .await;
    assert_eq!(status, 201, "a down target still stores: {body}");
    assert_eq!(
        body["status"], "disconnected",
        "a real connection attempt reports the down target: {body}"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-down-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["parameters"]["path"], "/down", "{body}");
    assert_eq!(
        body["status"], "disconnected",
        "the read keeps the real probe result: {body}"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-down-rule",
            "sql": "SELECT * FROM \"mv01/in/down\"",
            "actions": ["mv01-down-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-down-pub").await;
    // The server is down: the fire fails, but the broker keeps serving.
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/down", 0, 0, false, br#"{"down":1}"#),
    )
    .await;
    // Wait for that failed fire to finish before the server starts, so the
    // first message cannot arrive at the receiver below.
    let deadline = std::time::Instant::now() + STEP;
    loop {
        let (_, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            "/api/v5/actions/mv01-down-action/metrics",
            serde_json::Value::Null,
        )
        .await;
        if body["metrics"]["failed"].as_u64().unwrap_or(0) >= 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the down-target fire failed in time: {body}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let listener = Arc::new(
        tokio::net::TcpListener::bind(address)
            .await
            .expect("the server starts on the reserved port"),
    );
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/down", 0, 0, false, br#"{"down":2}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received[0].0, "/down");
    assert_eq!(received[0].1.as_slice(), &br#"{"down":2}"#[..]);
    kernel.finish().await;
}

/// A v5 action and a v1 connector may share a name; typed and bare v5
/// action ids resolve to the same sink, and both paths survive a restart.
#[tokio::test]
async fn mv01_v5_action_and_v1_connector_share_a_name_and_survive_restart() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let mut kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v1/connectors",
        serde_json::json!({
            "id": "shared",
            "kind": "webhook",
            "config": {"url": format!("http://{address}/v1"), "batch_size": 1},
        }),
    )
    .await;
    assert_eq!(status, 201, "v1 connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-ns-link",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "v5 connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "shared",
            "type": "webhook",
            "connector": "mv01-ns-link",
            "parameters": {"path": "/v5"},
        }),
    )
    .await;
    assert_eq!(status, 201, "v5 action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v1/rules",
        serde_json::json!({
            "name": "mv01-ns-v1",
            "topic_filter": "mv01/ns/v1",
            "sql_query": "SELECT * FROM \"mv01/ns/v1\"",
            "actions": [{"type": "forwardconnector", "connector_id": "shared"}],
        }),
    )
    .await;
    assert_eq!(status, 201, "v1 rule create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-ns-typed",
            "sql": "SELECT * FROM \"mv01/ns/typed\"",
            "actions": ["webhook:shared"],
        }),
    )
    .await;
    assert_eq!(status, 201, "typed v5 rule create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-ns-bare",
            "sql": "SELECT * FROM \"mv01/ns/bare\"",
            "actions": ["shared"],
        }),
    )
    .await;
    assert_eq!(status, 201, "bare v5 rule create: {body}");
    let topics = ["mv01/ns/v1", "mv01/ns/typed", "mv01/ns/bare"];
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), topics.len());
    let mut publisher = mv00_connect(&kernel, "mv01-ns-pub").await;
    for topic in topics {
        send_mqtt(
            &mut publisher,
            &sy02_publish311(topic, 0, 0, false, topic.as_bytes()),
        )
        .await;
    }
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    let mut paths: Vec<&str> = received.iter().map(|(path, _)| path.as_str()).collect();
    paths.sort_unstable();
    assert_eq!(
        paths,
        vec!["/v1", "/v5", "/v5"],
        "same-name v1 connector and v5 action routed separately: {received:?}"
    );
    // The v1 forward on the same bare name does not inflate the v5 action
    // metrics: only the two v5 fires count.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/shared/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "action metrics: {body}");
    assert_eq!(body["metrics"]["matched"], serde_json::json!(2), "{body}");
    assert_eq!(body["metrics"]["success"], serde_json::json!(2), "{body}");
    // Restart: the v5 action replay rebuilds under its dispatch key. The
    // v1 path stays in its own memory-only name space, as it did before
    // MV-01, so it does not write a v5 connector row and does not resume.
    mv00_restart(&mut kernel).await;
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 2);
    let mut publisher = mv00_connect(&kernel, "mv01-ns-pub2").await;
    for topic in topics {
        send_mqtt(
            &mut publisher,
            &sy02_publish311(topic, 0, 0, false, format!("{topic}-restart").as_bytes()),
        )
        .await;
    }
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    let mut paths: Vec<&str> = received.iter().map(|(path, _)| path.as_str()).collect();
    paths.sort_unstable();
    assert_eq!(
        paths,
        vec!["/v5", "/v5"],
        "the v5 actions resumed and the v1 row stayed out of the v5 store: {received:?}"
    );
    // The v1 rule fired on the same bare name after the restart too. Its
    // forward must not credit the v5 action's metrics.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/shared/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "post-restart action metrics: {body}");
    assert_eq!(
        body["metrics"]["matched"],
        serde_json::json!(2),
        "only the two v5 fires count after restart: {body}"
    );
    assert_eq!(
        body["metrics"]["success"],
        serde_json::json!(2),
        "only the two v5 fires succeed after restart: {body}"
    );
    kernel.finish().await;
}

/// A webhook connector that is down reads `disconnected` from its own
/// probe at create and at read time, and `connected` after the target
/// comes up and the update probes it again.
#[tokio::test]
async fn mv01_down_webhook_connector_reads_disconnected_from_probe() {
    let reserve = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a port");
    let address = reserve.local_addr().expect("reserved address");
    drop(reserve);
    let kernel = spawn_kernel("").await;
    let connector = serde_json::json!({
        "name": "mv01-probe-link",
        "type": "webhook",
        "url": format!("http://{address}"),
    });
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        connector.clone(),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    assert_eq!(
        body["status"], "disconnected",
        "the create probe reports the down target: {body}"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/connectors/mv01-probe-link",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "connector read: {body}");
    assert_eq!(
        body["status"], "disconnected",
        "the read keeps the probe result: {body}"
    );
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("the target starts on the reserved port");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/connectors/mv01-probe-link",
        connector,
    )
    .await;
    assert_eq!(status, 200, "connector update: {body}");
    assert_eq!(
        body["status"], "connected",
        "the update probe reports the live target: {body}"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/connectors/mv01-probe-link",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "connector read: {body}");
    assert_eq!(body["status"], "connected", "{body}");
    drop(listener);
    kernel.finish().await;
}

/// A not-ready action kind is refused at the KC-00 map with the create
/// code `ALREADY_EXISTS` and the update code `BAD_REQUEST`.
#[tokio::test]
async fn mv01_action_readiness_refusals_keep_the_documented_codes() {
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-http-not-ready",
            "type": "http",
            "connector": "mv01-missing-link",
        }),
    )
    .await;
    assert_eq!(status, 400, "a not-ready kind is refused: {body}");
    assert_eq!(body["code"], "ALREADY_EXISTS", "{body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-ready-link",
            "type": "webhook",
            "url": "http://127.0.0.1:9",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-ready-action",
            "type": "webhook",
            "connector": "mv01-ready-link",
            "parameters": {"path": "/ready"},
        }),
    )
    .await;
    assert_eq!(status, 201, "ready action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-ready-action",
        serde_json::json!({
            "name": "mv01-ready-action",
            "type": "http",
            "connector": "mv01-ready-link",
        }),
    )
    .await;
    assert_eq!(status, 400, "the update refuses the not-ready kind: {body}");
    assert_eq!(body["code"], "BAD_REQUEST", "{body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-ready-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(
        status, 200,
        "the refused update keeps the old action: {body}"
    );
    assert_eq!(body["type"], "webhook", "{body}");
    kernel.finish().await;
}

/// An invalid header value is refused without echoing the value.
#[tokio::test]
async fn mv01_action_header_value_is_not_echoed_in_the_error() {
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-header-link",
            "type": "webhook",
            "url": "http://127.0.0.1:9",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-header-action",
            "type": "webhook",
            "connector": "mv01-header-link",
            "parameters": {
                "path": "/header",
                "headers": {"X-Secret": "super-secret\nvalue"},
            },
        }),
    )
    .await;
    assert_eq!(status, 400, "the invalid header value is refused: {body}");
    assert_eq!(body["code"], "ALREADY_EXISTS", "{body}");
    let message = body["message"].as_str().expect("error message");
    assert!(message.contains("X-Secret"), "the key is named: {message}");
    assert!(
        message.contains("parameters.headers"),
        "the field is named: {message}"
    );
    assert!(
        !message.contains("super-secret"),
        "the header value is not echoed: {message}"
    );
    kernel.finish().await;
}

/// The action request timeout comes from `resource_opts.request_ttl`, not
/// from the sink's 5 s default.
#[tokio::test]
async fn mv01_action_request_ttl_bounds_the_http_request() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let receiver = mv01_spawn_http_receiver_active(
        Arc::clone(&listener),
        1,
        std::time::Duration::from_secs(3),
    );
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-ttl-link",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-ttl-action",
            "type": "webhook",
            "connector": "mv01-ttl-link",
            "parameters": {"path": "/ttl"},
            "resource_opts": {
                "batch_size": 1,
                "batch_time": "0ms",
                "request_ttl": "1s",
                "health_check_interval": "1h",
            },
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-ttl-rule",
            "sql": "SELECT * FROM \"mv01/in/ttl\"",
            "actions": ["mv01-ttl-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-ttl-pub").await;
    let start = std::time::Instant::now();
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/ttl", 0, 0, false, br#"{"ttl":1}"#),
    )
    .await;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(2500);
    loop {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            "/api/v5/actions/mv01-ttl-action/metrics",
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "metrics: {body}");
        if body["metrics"]["failed"].as_u64().unwrap_or(0) >= 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the request timed out from request_ttl, not the 5 s default: {body}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(
        start.elapsed() < std::time::Duration::from_millis(2500),
        "the 1 s request TTL applies"
    );
    receiver.abort();
    kernel.finish().await;
}

/// The pgsql transport applies `ssl.verify = verify_peer` and refuses
/// `verify_none` instead of accepting a value it cannot apply.
#[tokio::test]
async fn mv01_pgsql_tls_verify_is_applied_or_refused() {
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-pg-tls",
            "type": "postgres",
            "server": "127.0.0.1:9",
            "database": "db",
            "username": "u",
            "password": "pwd",
            "ssl": {"enable": true, "verify": "verify_none"},
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-pg-tls",
            "type": "pgsql",
            "connector": "mv01-pg-tls",
            "parameters": {"sql": "MV01-TLS INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 400, "verify_none is refused: {body}");
    assert_eq!(body["code"], "ALREADY_EXISTS", "{body}");
    let message = body["message"].as_str().expect("error message");
    assert!(
        message.contains("ssl.verify") && message.contains("verify_none"),
        "the refusal names the setting: {message}"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/connectors/mv01-pg-tls",
        serde_json::json!({
            "name": "mv01-pg-tls",
            "type": "postgres",
            "server": "127.0.0.1:9",
            "database": "db",
            "username": "u",
            "password": "pwd",
            "ssl": {"enable": true, "verify": "verify_peer"},
        }),
    )
    .await;
    assert_eq!(status, 200, "connector update: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-pg-tls",
            "type": "pgsql",
            "connector": "mv01-pg-tls",
            "parameters": {"sql": "MV01-TLS INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 201, "verify_peer builds: {body}");
    assert_eq!(body["status"], "disconnected", "{body}");
    kernel.finish().await;
}

/// A `file:` password reference resolves before the driver dials, and the
/// fake server checks the real password.
#[tokio::test]
async fn mv01_pgsql_secret_password_reaches_driver() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pg fake binds");
    let address = listener.local_addr().expect("pg fake address");
    let captured: PgCaptured = Arc::new(Mutex::new(Vec::new()));
    let fake_captured = Arc::clone(&captured);
    let fake = tokio::spawn(async move {
        loop {
            let stream = mv01_pg_accept(&listener).await;
            mv01_pg_serve_one(
                stream,
                Arc::clone(&fake_captured),
                "u",
                "pwd-from-file",
                "db",
                1,
            )
            .await;
        }
    });
    let kernel = spawn_kernel("").await;
    let secret = kernel.scratch.join("mv01-pg-password");
    std::fs::write(&secret, "pwd-from-file").expect("secret file");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-pg-secret",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "u",
            "password": format!("file:{}", secret.display()),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-pg-secret",
            "type": "pgsql",
            "connector": "mv01-pg-secret",
            "parameters": {"sql": "MV01-SECRET INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-pg-secret-rule",
            "sql": "SELECT * FROM \"mv01/in/secret\"",
            "actions": ["mv01-pg-secret"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-pg-secret-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/secret", 0, 0, false, br#"{"secret":1}"#),
    )
    .await;
    mv01_wait_pg_batches(&captured, 1).await;
    fake.abort();
    kernel.finish().await;
}

/// A connector create must not rebuild an unrelated action from a stale
/// snapshot. Action updates and connector creates run together; after the
/// churn the sink that delivers matches the stored action.
#[tokio::test]
async fn mv01_connector_rebuild_does_not_overwrite_an_action_update() {
    let listener = Arc::new(
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP receiver binds"),
    );
    let address = listener.local_addr().expect("HTTP receiver address");
    let drain_listener = Arc::clone(&listener);
    let drain = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = drain_listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                use tokio::io::AsyncReadExt;
                let mut byte = [0_u8; 1];
                let _ = socket.read_exact(&mut byte).await;
            });
        }
    });
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-churn-link",
            "type": "webhook",
            "url": format!("http://{address}"),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let action_body = |path: &str| {
        serde_json::json!({
            "name": "mv01-churn-action",
            "type": "webhook",
            "connector": "mv01-churn-link",
            "parameters": {"path": path},
        })
    };
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        action_body("/p0"),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-churn-rule",
            "sql": "SELECT * FROM \"mv01/in/churn\"",
            "actions": ["mv01-churn-action"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let updates = async {
        for index in 1..30 {
            let path = format!("/p{index}");
            let (status, body) = mv00_api(
                &kernel,
                reqwest::Method::PUT,
                "/api/v5/actions/mv01-churn-action",
                action_body(&path),
            )
            .await;
            assert_eq!(status, 200, "action update: {body}");
        }
    };
    let creates = async {
        for index in 0..15 {
            let name = format!("mv01-churn-{index}");
            let (status, body) = mv00_api(
                &kernel,
                reqwest::Method::POST,
                "/api/v5/connectors",
                serde_json::json!({
                    "name": name,
                    "type": "webhook",
                    "url": format!("http://{address}"),
                }),
            )
            .await;
            assert_eq!(status, 201, "unrelated connector create: {body}");
            let path = format!("/api/v5/connectors/{name}");
            let (status, _) = mv00_api(
                &kernel,
                reqwest::Method::DELETE,
                &path,
                serde_json::Value::Null,
            )
            .await;
            assert_eq!(status, 204, "unrelated connector delete");
        }
    };
    tokio::join!(updates, creates);
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-churn-action",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "action read: {body}");
    let stored_path = body["parameters"]["path"]
        .as_str()
        .expect("stored path")
        .to_string();
    drain.abort();
    let receiver = mv01_spawn_http_receiver(Arc::clone(&listener), 1);
    let mut publisher = mv00_connect(&kernel, "mv01-churn-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/churn", 0, 0, false, br#"{"churn":1}"#),
    )
    .await;
    let received = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(received.len(), 1, "one delivery");
    assert_eq!(
        received[0].0, stored_path,
        "the delivering sink matches the stored action"
    );
    kernel.finish().await;
}

/// MV-02a is accepted: the six relational kinds are `ready`, so the
/// probe no longer refuses them as pending. A bare probe carries no
/// target, and a ready non-document kind reads 204.
#[tokio::test]
async fn mv02a_ready_kinds_probe_without_target_reads_no_content() {
    let kinds = [
        "mysql",
        "alloydb",
        "cockroachdb",
        "mssql",
        "oracle",
        "redshift",
    ];
    for kind in kinds {
        let kernel = spawn_kernel("").await;
        let (status, _) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/connectors_probe",
            serde_json::json!({ "type": kind }),
        )
        .await;
        assert_eq!(status, 204, "{kind} bare probe reads 204, got {status}");
        kernel.finish().await;
    }
}

/// A ready kind dials its target: a dead target fails the probe with
/// 400 TEST_FAILED. A pending kind would fail with the refusal text
/// instead of a dial error.
#[tokio::test]
async fn mv02a_ready_kinds_probe_dead_target_fails() {
    let kinds = [
        "mysql",
        "alloydb",
        "cockroachdb",
        "mssql",
        "oracle",
        "redshift",
    ];
    for kind in kinds {
        let kernel = spawn_kernel("").await;
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/connectors_probe",
            serde_json::json!({ "type": kind, "server": "127.0.0.1:9" }),
        )
        .await;
        assert_eq!(status, 400, "{kind} dead-target probe: {body}");
        assert_eq!(body["code"], "TEST_FAILED", "{kind} probe code: {body}");
        assert!(
            !body["message"]
                .as_str()
                .unwrap_or("")
                .contains("not available in this broker"),
            "{kind} probe dialled instead of refusing: {body}"
        );
        kernel.finish().await;
    }
}

/// MV-02a is accepted: the six relational kinds are `ready`, so a
/// complete connector create stores (201) and reads back. The sink
/// registers lazily, so a dead server still stores.
#[tokio::test]
async fn mv02a_ready_kinds_create_stores() {
    let kinds = [
        ("mysql", "mysql"),
        ("alloydb", "alloydb"),
        ("cockroachdb", "cockroachdb"),
        ("mssql", "sqlserver"),
        ("oracle", "oracle"),
        ("redshift", "redshift"),
    ];
    for (kind, connector_type) in kinds {
        let kernel = spawn_kernel("").await;
        let name = format!("mv02a-up-{kind}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/connectors",
            serde_json::json!({
                "name": name,
                "type": connector_type,
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
                "password": "pwd",
            }),
        )
        .await;
        assert_eq!(status, 201, "{kind} connector create: {body}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/connectors/{name}"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "{kind} connector read: {body}");
        assert_eq!(
            body["type"],
            serde_json::json!(connector_type),
            "{kind} type round-trips: {body}"
        );
        kernel.finish().await;
    }
}

/// MV-02a is accepted: the six relational kinds are `ready`, so an
/// action on a stored connector stages its owned sink and stores
/// (201). Each statement uses the placeholders of its driver.
#[tokio::test]
async fn mv02a_ready_kinds_action_create_stores() {
    let kinds = [
        (
            "mysql",
            "mysql",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
                "password": "pwd",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES (?, ?, ?)",
        ),
        (
            "alloydb",
            "alloydb",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
                "password": "pwd",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($1, $2, $3)",
        ),
        (
            "cockroachdb",
            "cockroachdb",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
                "password": "pwd",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($1, $2, $3)",
        ),
        (
            "mssql",
            "mssql",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($2, $4, $5)",
        ),
        (
            "oracle",
            "oracle",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "username": "u",
                "service_name": "XE",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES (:1, :2, :3)",
        ),
        (
            "redshift",
            "redshift",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($1, $2, $3)",
        ),
    ];
    for (kind, action_type, fields, sql) in kinds {
        let kernel = spawn_kernel("").await;
        let connector = format!("mv02a-up-{kind}");
        let mut connector_body = serde_json::json!({
            "name": connector,
            "type": action_type,
        });
        for (field, value) in fields.as_object().expect("connector fields") {
            connector_body[field] = value.clone();
        }
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/connectors",
            connector_body,
        )
        .await;
        assert_eq!(status, 201, "{kind} connector create: {body}");
        let name = format!("mv02a-act-{kind}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/actions",
            serde_json::json!({
                "name": name,
                "type": action_type,
                "connector": connector,
                "parameters": {"sql": sql},
            }),
        )
        .await;
        assert_eq!(status, 201, "{kind} action create: {body}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/actions/{name}"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "{kind} action read: {body}");
        assert_eq!(
            body["type"],
            serde_json::json!(action_type),
            "{kind} type round-trips: {body}"
        );
        kernel.finish().await;
    }
}

/// MV-02a is accepted: the kinds are `ready`, so option validation
/// runs in the action builder. Each combination below reaches the
/// builder and fails with the field named. The connector create
/// stores (201); the action create answers 400 and stores nothing.
#[tokio::test]
async fn mv02a_ready_kinds_action_builders_refuse_bad_options() {
    let cases = [
        (
            "redshift",
            "redshift",
            serde_json::json!({
                "server": "127.0.0.1:5439",
                "database": "db",
                "username": "u",
                "disable_prepared_statements": true,
            }),
            "INSERT INTO t VALUES ($1, $2, $3)",
            "disable_prepared_statements",
        ),
        (
            "cockroachdb",
            "cockroachdb",
            serde_json::json!({
                "server": "127.0.0.1:26257",
                "database": "db",
                "username": "u",
                "application_name": "app",
            }),
            "INSERT INTO t VALUES ($1, $2, $3)",
            "application_name",
        ),
        (
            "mysql",
            "mysql",
            serde_json::json!({
                "server": "127.0.0.1:3306",
                "database": "db",
                "ssl": { "enable": true },
            }),
            "INSERT INTO t VALUES (?, ?, ?)",
            "ssl.enable",
        ),
        (
            "oracle",
            "oracle",
            serde_json::json!({
                "server": "127.0.0.1:1521",
                "username": "u",
            }),
            "INSERT INTO t VALUES (:1)",
            "service_name",
        ),
        (
            "mssql",
            "mssql",
            serde_json::json!({
                "server": "127.0.0.1:1433",
                "database": "db",
                "driver": "FreeTDS",
            }),
            "INSERT INTO t VALUES ($2)",
            "driver",
        ),
    ];
    for (kind, action_type, fields, sql, field) in cases {
        let kernel = spawn_kernel("").await;
        let connector = format!("mv02a-opt-conn-{kind}");
        let mut connector_body = serde_json::json!({
            "name": connector,
            "type": action_type,
        });
        for (key, value) in fields.as_object().expect("connector fields") {
            connector_body[key] = value.clone();
        }
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/connectors",
            connector_body,
        )
        .await;
        assert_eq!(status, 201, "{kind} connector create: {body}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/actions",
            serde_json::json!({
                "name": format!("mv02a-opt-act-{kind}"),
                "type": action_type,
                "connector": connector,
                "parameters": {"sql": sql},
            }),
        )
        .await;
        assert_eq!(status, 400, "{kind} bad option refused: {body}");
        assert_eq!(
            body["code"], "ALREADY_EXISTS",
            "{kind} refusal code: {body}"
        );
        assert!(
            body["message"].as_str().unwrap_or("").contains(field),
            "{kind} refusal names {field}: {body}"
        );
        kernel.finish().await;
    }
}

/// MV-02a N1: the pg-family kinds deliver the engine row through the broker.
/// Each action fires on its own publish. The fake backend captures the
/// statement and the bound row. A quote in the payload stays bound.
#[tokio::test]
async fn mv02a_pg_family_delivers_bound_rows_through_broker() {
    let kinds = [
        ("alloydb", "alloydb"),
        ("cockroachdb", "cockroachdb"),
        ("redshift", "redshift"),
    ];
    for (kind, action_type) in kinds {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("pg fake binds");
        let address = listener.local_addr().expect("pg fake address");
        let captured: PgCaptured = Arc::new(Mutex::new(Vec::new()));
        let fake = {
            let captured = Arc::clone(&captured);
            tokio::spawn(async move {
                let stream = mv01_pg_accept(&listener).await;
                mv01_pg_serve_one(stream, Arc::clone(&captured), "u", "pwd", "db", 1).await;
            })
        };
        let kernel = spawn_kernel("").await;
        let connector = format!("mv02a-pg-{kind}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/connectors",
            serde_json::json!({
                "name": connector,
                "type": action_type,
                "server": format!("127.0.0.1:{}", address.port()),
                "database": "db",
                "username": "u",
                "password": "pwd",
            }),
        )
        .await;
        assert_eq!(status, 201, "{kind} connector create: {body}");
        let action = format!("mv02a-pg-act-{kind}");
        let sentinel = format!("MV02A-{kind}-SQL");
        let sql = format!("{sentinel} INSERT INTO t(topic, qos, payload) VALUES ($1, $2, $3)");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/actions",
            serde_json::json!({
                "name": action,
                "type": action_type,
                "connector": connector,
                "parameters": {"sql": sql},
            }),
        )
        .await;
        assert_eq!(status, 201, "{kind} action create: {body}");
        let rule = format!("mv02a-pg-rule-{kind}");
        let topic = format!("mv02a/in/pg-{kind}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/rules",
            serde_json::json!({
                "id": rule,
                "sql": format!("SELECT * FROM \"{topic}\""),
                "actions": [action.clone()],
            }),
        )
        .await;
        assert_eq!(status, 201, "{kind} rule create: {body}");
        let mut publisher = mv00_connect(&kernel, &format!("mv02a-pg-pub-{kind}")).await;
        let payload = br#"{"v":"a\"b"}"#;
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&topic, 0, 0, false, payload),
        )
        .await;
        drop(publisher);
        let deadline = std::time::Instant::now() + STEP;
        loop {
            let done = captured.lock().expect("capture lock").len() == 1;
            assert!(
                std::time::Instant::now() < deadline || done,
                "{kind} pg fake got one batch"
            );
            if done {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        fake.await.expect("pg fake task");
        let captured = std::sync::Mutex::into_inner(Arc::try_unwrap(captured).expect("sole owner"))
            .expect("capture lock");
        assert_eq!(captured.len(), 1, "{kind} one batch");
        let (statement, rows) = &captured[0];
        assert!(
            statement.contains(&sentinel),
            "{kind} statement carries sentinel: {statement}"
        );
        assert!(
            statement.contains("$1") && statement.contains("$3"),
            "{kind} markers stay: {statement}"
        );
        assert_eq!(rows.len(), 1, "{kind} one row");
        assert_eq!(rows[0].len(), 3, "{kind} topic, qos, payload");
        assert_eq!(rows[0][0], topic.as_bytes(), "{kind} topic bound");
        assert_eq!(rows[0][1], b"0", "{kind} qos bound");
        assert_eq!(rows[0][2], payload, "{kind} quote stays bound");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/actions/{action}/metrics"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "{kind} metrics: {body}");
        assert_eq!(
            body["metrics"]["success"],
            serde_json::json!(1),
            "{kind} success: {body}"
        );
        assert_eq!(
            body["metrics"]["failed"],
            serde_json::json!(0),
            "{kind} failed: {body}"
        );
        kernel.finish().await;
    }
}

/// Reads one MySQL packet (seq plus body) from the fake server stream.
/// Returns `None` for a reachability probe that connects and closes
/// without a byte, so the fake skips it and serves the driver.
async fn mv02a_mysql_read_packet(stream: &mut tokio::net::TcpStream) -> Option<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut header = [0_u8; 4];
    if stream.read_exact(&mut header).await.is_err() {
        return None;
    }
    let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
    let mut body = vec![0_u8; 1 + len];
    body[0] = header[3];
    if stream.read_exact(&mut body[1..]).await.is_err() {
        return None;
    }
    Some(body)
}

/// Writes one MySQL packet with the given seq to the fake server stream.
/// Returns false when the peer closed, so the caller skips it.
async fn mv02a_mysql_write_packet(
    stream: &mut tokio::net::TcpStream,
    seq: u8,
    body: &[u8],
) -> bool {
    use tokio::io::AsyncWriteExt;
    let len = body.len() as u32;
    let mut frame = vec![
        (len & 0xFF) as u8,
        ((len >> 8) & 0xFF) as u8,
        ((len >> 16) & 0xFF) as u8,
        seq,
    ];
    frame.extend_from_slice(body);
    stream.write_all(&frame).await.is_ok()
}

/// Decodes one length-encoded integer from the fake server cursor.
fn mv02a_mysql_decode_lenenc(cursor: &[u8]) -> (usize, &[u8]) {
    match cursor[0] {
        0xFC => (
            u16::from_le_bytes([cursor[1], cursor[2]]) as usize,
            &cursor[3..],
        ),
        0xFD => (
            u32::from_le_bytes([cursor[1], cursor[2], cursor[3], 0]) as usize,
            &cursor[4..],
        ),
        0xFE => (
            u64::from_le_bytes([
                cursor[1], cursor[2], cursor[3], cursor[4], cursor[5], cursor[6], cursor[7],
                cursor[8],
            ]) as usize,
            &cursor[9..],
        ),
        byte => (byte as usize, &cursor[1..]),
    }
}

/// Writes one MySQL column definition with catalog `def`.
/// It matches the real wire the driver parses.
async fn mv02a_mysql_write_column_def(stream: &mut tokio::net::TcpStream, seq: u8, name: &[u8]) {
    let mut def = vec![0x03, b'd', b'e', b'f'];
    def.push(0);
    def.push(0);
    def.push(0);
    def.push(name.len() as u8);
    def.extend_from_slice(name);
    def.push(0);
    def.push(0x0C);
    def.extend_from_slice(&33_u16.to_le_bytes());
    def.extend_from_slice(&1024_u32.to_le_bytes());
    def.push(0xFD);
    def.extend_from_slice(&0_u16.to_le_bytes());
    def.push(0);
    def.extend_from_slice(&[0, 0]);
    mv02a_mysql_write_packet(stream, seq, &def).await;
}

/// Answers one driver settings probe (`COM_QUERY`, for example
/// `SELECT @@socket`). Each selected column gets one NULL value.
/// The driver only reads the values by position.
async fn mv02a_mysql_answer_settings_query(stream: &mut tokio::net::TcpStream, sql: &[u8]) {
    let columns = sql.iter().filter(|&&b| b == b',').count() + 1;
    let columns = columns.min(8) as u8;
    mv02a_mysql_write_packet(stream, 1, &[columns]).await;
    for index in 0..columns {
        mv02a_mysql_write_column_def(stream, 2 + index, b"").await;
    }
    mv02a_mysql_write_packet(stream, 2 + columns, &[0xFE, 0, 0, 2, 0]).await;
    mv02a_mysql_write_packet(stream, 3 + columns, &vec![0xFB; columns as usize]).await;
    mv02a_mysql_write_packet(stream, 4 + columns, &[0xFE, 0, 0, 2, 0]).await;
}

/// One fake MySQL backend: greeting, accept-any handshake, driver
/// settings probe, one prepare capture, one execute capture. It
/// proves delivery, not auth.
async fn mv02a_mysql_serve_one(
    mut stream: tokio::net::TcpStream,
    captured_sql: Arc<Mutex<String>>,
    captured_rows: Arc<Mutex<Vec<Vec<Vec<u8>>>>>,
) {
    let seed_part1 = b"12345678";
    let seed_part2 = b"ABCDEFGHJKLM";
    let mut greeting = vec![10_u8];
    greeting.extend_from_slice(b"8.0.36-test\0");
    greeting.extend_from_slice(&42_u32.to_le_bytes());
    greeting.extend_from_slice(seed_part1);
    greeting.push(0);
    // No SSL bit: the fake speaks plaintext only, so it clears
    // CLIENT_SSL (0x0800) from the low caps. It advertises
    // CLIENT_PROTOCOL_41 and CLIENT_SECURE_CONNECTION, and
    // CLIENT_PLUGIN_AUTH in the high caps for mysql_native_password.
    greeting.extend_from_slice(&0xF7FF_u16.to_le_bytes());
    greeting.push(33);
    greeting.extend_from_slice(&0x0002_u16.to_le_bytes());
    greeting.extend_from_slice(&0x0008_u16.to_le_bytes());
    greeting.push(21);
    greeting.extend_from_slice(&[0_u8; 10]);
    greeting.extend_from_slice(seed_part2);
    greeting.push(0);
    greeting.extend_from_slice(b"mysql_native_password\0");
    if !mv02a_mysql_write_packet(&mut stream, 0, &greeting).await {
        return;
    }
    let Some(_handshake) = mv02a_mysql_read_packet(&mut stream).await else {
        return;
    };
    if !mv02a_mysql_write_packet(&mut stream, 2, &[0x00, 0, 0, 0, 0, 0, 0]).await {
        return;
    }
    let request = loop {
        let Some(request) = mv02a_mysql_read_packet(&mut stream).await else {
            return;
        };
        if request[1] == 0x03 {
            mv02a_mysql_answer_settings_query(&mut stream, &request[2..]).await;
            continue;
        }
        break request;
    };
    assert_eq!(request[1], 0x16, "prepare parses the action statement");
    captured_sql
        .lock()
        .expect("sql lock")
        .push_str(std::str::from_utf8(&request[2..]).expect("sql"));
    let mut prepare_ok = vec![0x00];
    prepare_ok.extend_from_slice(&7_u32.to_le_bytes());
    prepare_ok.extend_from_slice(&0_u16.to_le_bytes());
    prepare_ok.extend_from_slice(&3_u16.to_le_bytes());
    prepare_ok.extend_from_slice(&0_u16.to_le_bytes());
    prepare_ok.extend_from_slice(&[0_u8; 3]);
    mv02a_mysql_write_packet(&mut stream, 1, &prepare_ok).await;
    for seq in [2_u8, 3, 4] {
        mv02a_mysql_write_column_def(&mut stream, seq, b"").await;
    }
    mv02a_mysql_write_packet(&mut stream, 5, &[0xFE, 0, 0, 0, 0]).await;
    let Some(request) = mv02a_mysql_read_packet(&mut stream).await else {
        return;
    };
    assert_eq!(request[1], 0x17, "execute binds one engine row");
    let stmt_id = u32::from_le_bytes([request[2], request[3], request[4], request[5]]);
    assert_eq!(stmt_id, 7, "execute uses the prepared statement");
    let mut cursor = &request[6 + 1 + 4 + 1 + 1..];
    cursor = &cursor[6..];
    let mut row = Vec::new();
    for _ in 0..3 {
        let (len, rest) = mv02a_mysql_decode_lenenc(cursor);
        row.push(rest[..len].to_vec());
        cursor = &rest[len..];
    }
    captured_rows.lock().expect("rows lock").push(row);
    mv02a_mysql_write_packet(&mut stream, 1, &[0x00, 1, 0, 0, 0, 0, 0]).await;
}

/// MV-02a N1: MySQL delivers the engine row through the broker. The fake
/// backend captures the statement and the bound row. A quote stays bound.
#[tokio::test]
async fn mv02a_mysql_delivers_bound_rows_through_broker() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("mysql fake binds");
    let address = listener.local_addr().expect("mysql fake address");
    let captured_sql = Arc::new(Mutex::new(String::new()));
    let captured_rows: Arc<Mutex<Vec<Vec<Vec<u8>>>>> = Arc::new(Mutex::new(Vec::new()));
    let fake = {
        let captured_sql = Arc::clone(&captured_sql);
        let captured_rows = Arc::clone(&captured_rows);
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.expect("mysql backend accepted");
                mv02a_mysql_serve_one(
                    stream,
                    Arc::clone(&captured_sql),
                    Arc::clone(&captured_rows),
                )
                .await;
                let done = !captured_sql.lock().expect("sql lock").is_empty()
                    && !captured_rows.lock().expect("rows lock").is_empty();
                if done {
                    break;
                }
            }
        })
    };
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv02a-my",
            "type": "mysql",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let sentinel = "MV02A-MYSQL-SQL";
    let sql = format!("{sentinel} INSERT INTO t(topic, qos, payload) VALUES (?, ?, ?)");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv02a-my-act",
            "type": "mysql",
            "connector": "mv02a-my",
            "parameters": {"sql": sql},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let topic = "mv02a/in/mysql";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv02a-my-rule",
            "sql": format!("SELECT * FROM \"{topic}\""),
            "actions": ["mv02a-my-act"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv02a-my-pub").await;
    let payload = br#"{"v":"a\"b"}"#;
    send_mqtt(
        &mut publisher,
        &sy02_publish311(topic, 0, 0, false, payload),
    )
    .await;
    drop(publisher);
    let deadline = std::time::Instant::now() + STEP;
    loop {
        let done = captured_rows.lock().expect("rows lock").len() == 1;
        assert!(
            std::time::Instant::now() < deadline || done,
            "mysql fake got one row"
        );
        if done {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    fake.await.expect("mysql fake task");
    let statement = captured_sql.lock().expect("sql lock").clone();
    assert!(
        statement.contains(sentinel),
        "statement carries sentinel: {statement}"
    );
    assert!(statement.contains("?, ?, ?"), "markers stay: {statement}");
    let rows = captured_rows.lock().expect("rows lock").clone();
    assert_eq!(rows.len(), 1, "one row");
    assert_eq!(rows[0].len(), 3, "topic, qos, payload");
    assert_eq!(rows[0][0], topic.as_bytes(), "topic bound");
    assert_eq!(rows[0][1], b"0", "qos bound");
    assert_eq!(rows[0][2], payload, "quote stays bound");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv02a-my-act/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "metrics: {body}");
    assert_eq!(
        body["metrics"]["success"],
        serde_json::json!(1),
        "success: {body}"
    );
    assert_eq!(
        body["metrics"]["failed"],
        serde_json::json!(0),
        "failed: {body}"
    );
    kernel.finish().await;
}

/// Encodes text as UCS-2 (UTF-16LE) for the MSSQL fake assertions.
fn mv02a_ucs2(text: &str) -> Vec<u8> {
    let mut out = Vec::new();
    for unit in text.encode_utf16() {
        out.extend_from_slice(&unit.to_le_bytes());
    }
    out
}

/// Reads one TDS packet body from the fake MSSQL server stream.
async fn mv02a_mssql_read_packet(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
    use tokio::io::AsyncReadExt;
    let mut header = [0_u8; 8];
    stream.read_exact(&mut header).await.expect("tds head");
    let length = u16::from_be_bytes([header[2], header[3]]) as usize;
    let mut body = vec![0_u8; length - 8];
    stream.read_exact(&mut body).await.expect("tds body");
    body
}

/// Writes one TDS reply packet from the fake MSSQL server stream.
async fn mv02a_mssql_write_reply(stream: &mut tokio::net::TcpStream, payload: &[u8]) {
    use tokio::io::AsyncWriteExt;
    let mut packet = vec![0x04_u8, 0x01];
    packet.extend_from_slice(&((8 + payload.len()) as u16).to_be_bytes());
    packet.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
    packet.extend_from_slice(payload);
    stream.write_all(&packet).await.expect("tds reply");
}

/// One fake MSSQL backend: prelogin, login, one RPC capture. It proves
/// delivery, not auth. The RPC bytes carry the bound values.
async fn mv02a_mssql_serve_one(mut stream: tokio::net::TcpStream, captured: Arc<Mutex<Vec<u8>>>) {
    use tokio::io::AsyncWriteExt;
    let request = mv02a_mssql_read_packet(&mut stream).await;
    assert!(!request.is_empty(), "prelogin arrives");
    // Full prelogin reply: VERSION, ENCRYPTION (NOT_SUP, plaintext
    // only), INSTOPT (empty default instance), THREADID and MARS,
    // so a maintained driver accepts it. The delivery and auth
    // checks below are unchanged.
    let mut reply = Vec::new();
    let mut offset: u16 = 5 * 5 + 1;
    for (id, data_len) in [
        (0x00_u8, 6_usize),
        (0x01, 1),
        (0x02, 1),
        (0x03, 4),
        (0x04, 1),
    ] {
        reply.push(id);
        reply.extend_from_slice(&offset.to_be_bytes());
        reply.extend_from_slice(&(data_len as u16).to_be_bytes());
        offset += data_len as u16;
    }
    reply.push(0xFF);
    reply.extend_from_slice(&[0x0F, 0x00, 0x00, 0x00, 0x00, 0x00]);
    reply.push(0x02);
    reply.push(0x00);
    reply.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
    reply.push(0x00);
    mv02a_mssql_write_reply(&mut stream, &reply).await;
    let login = mv02a_mssql_read_packet(&mut stream).await;
    assert!(login.len() > 64, "login arrives");
    let needle = mv02a_ucs2("sa");
    assert!(
        login.windows(needle.len()).any(|w| w == needle.as_slice()),
        "login carries the user"
    );
    let mut loginack = vec![0xAD_u8];
    // Wire-valid LOGINACK the driver parses: interface, TDS
    // version, program name, server version.
    let mut login_body = vec![0x01_u8];
    login_body.extend_from_slice(&0x74000004_u32.to_be_bytes());
    login_body.push(0x05);
    for unit in "mssql".encode_utf16() {
        login_body.extend_from_slice(&unit.to_le_bytes());
    }
    login_body.extend_from_slice(&0_u32.to_le_bytes());
    loginack.extend_from_slice(&(login_body.len() as u16).to_le_bytes());
    loginack.extend_from_slice(&login_body);
    let mut done = vec![0xFD_u8];
    done.extend_from_slice(&0_u16.to_le_bytes());
    done.extend_from_slice(&0_u16.to_le_bytes());
    done.extend_from_slice(&0_u64.to_le_bytes());
    let mut payload = loginack;
    payload.extend_from_slice(&done);
    mv02a_mssql_write_reply(&mut stream, &payload).await;
    let batch = mv02a_mssql_read_packet(&mut stream).await;
    assert!(batch.len() > 16, "rpc arrives");
    *captured.lock().expect("capture lock") = batch;
    let mut done = vec![0xFD_u8];
    done.extend_from_slice(&0_u16.to_le_bytes());
    done.extend_from_slice(&0_u16.to_le_bytes());
    done.extend_from_slice(&1_u64.to_le_bytes());
    mv02a_mssql_write_reply(&mut stream, &done).await;
    let _ = stream.shutdown().await;
}

/// MV-02a N1: MSSQL delivers the engine row through the broker. The fake
/// backend captures the RPC. Topic and payload ride as bound params.
#[tokio::test]
async fn mv02a_mssql_delivers_bound_rows_through_broker() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("mssql fake binds");
    let address = listener.local_addr().expect("mssql fake address");
    let captured: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let fake = {
        let captured = Arc::clone(&captured);
        tokio::spawn(async move {
            let stream = mv01_pg_accept(&listener).await;
            mv02a_mssql_serve_one(stream, Arc::clone(&captured)).await;
        })
    };
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv02a-ms",
            "type": "mssql",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "sa",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let sentinel = "MV02A-MSSQL-SQL";
    let sql = format!("{sentinel} INSERT INTO t(topic, qos, payload) VALUES ($2, $4, $5)");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv02a-ms-act",
            "type": "mssql",
            "connector": "mv02a-ms",
            "parameters": {"sql": sql},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let topic = "mv02a/in/mssql";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv02a-ms-rule",
            "sql": format!("SELECT * FROM \"{topic}\""),
            "actions": ["mv02a-ms-act"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv02a-ms-pub").await;
    let payload = br#"{"v":"a\"b"}"#;
    send_mqtt(
        &mut publisher,
        &sy02_publish311(topic, 0, 0, false, payload),
    )
    .await;
    drop(publisher);
    let deadline = std::time::Instant::now() + STEP;
    loop {
        let done = !captured.lock().expect("capture lock").is_empty();
        assert!(
            std::time::Instant::now() < deadline || done,
            "mssql fake got one rpc"
        );
        if done {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    fake.await.expect("mssql fake task");
    let batch = captured.lock().expect("capture lock").clone();
    let topic_ucs2 = mv02a_ucs2(topic);
    assert!(
        batch
            .windows(topic_ucs2.len())
            .any(|w| w == topic_ucs2.as_slice()),
        "rpc carries the topic bound"
    );
    let payload_text = String::from_utf8_lossy(payload).into_owned();
    let payload_ucs2 = mv02a_ucs2(&payload_text);
    assert!(
        batch
            .windows(payload_ucs2.len())
            .any(|w| w == payload_ucs2.as_slice()),
        "rpc carries the payload with the quote bound"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv02a-ms-act/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "metrics: {body}");
    assert_eq!(
        body["metrics"]["success"],
        serde_json::json!(1),
        "success: {body}"
    );
    assert_eq!(
        body["metrics"]["failed"],
        serde_json::json!(0),
        "failed: {body}"
    );
    kernel.finish().await;
}

/// MV-02a N2: an Oracle action against a dead server counts one failed
/// forward. The broker keeps serving. The payload carries a quote.
#[tokio::test]
async fn mv02a_oracle_down_server_counts_failure_and_keeps_state() {
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv02a-ora-down",
            "type": "oracle",
            "server": "127.0.0.1:1",
            "username": "u",
            "password": "pwd",
            "service_name": "XE",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let sql = "INSERT INTO t(topic, qos, payload) VALUES (:1, :2, :3)";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv02a-ora-down-act",
            "type": "oracle",
            "connector": "mv02a-ora-down",
            "parameters": {"sql": sql},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv02a-ora-down-act",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "action read: {body}");
    assert_eq!(body["parameters"]["sql"], serde_json::json!(sql), "{body}");
    let topic = "mv02a/in/ora-down";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv02a-ora-down-rule",
            "sql": format!("SELECT * FROM \"{topic}\""),
            "actions": ["mv02a-ora-down-act"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv02a-ora-down-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311(topic, 0, 0, false, br#"{"v":"a\"b"}"#),
    )
    .await;
    let deadline = std::time::Instant::now() + STEP;
    loop {
        let (_, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            "/api/v5/actions/mv02a-ora-down-act/metrics",
            serde_json::Value::Null,
        )
        .await;
        if body["metrics"]["failed"].as_u64().unwrap_or(0) >= 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the dead-target fire failed in time: {body}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv02a-ora-down-act/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "metrics: {body}");
    assert_eq!(
        body["metrics"]["success"],
        serde_json::json!(0),
        "success stays zero: {body}"
    );
    assert_eq!(
        body["metrics"]["failed"],
        serde_json::json!(1),
        "one fire counts one failed: {body}"
    );
    kernel.finish().await;
}

/// MV-02a N2: every relational kind counts a dead-server fire as failed.
/// The broker keeps serving. This proves the Limits claim per kind.
#[tokio::test]
async fn mv02a_relational_dead_server_counts_failed_for_all_kinds() {
    let kinds: &[(&str, &str, serde_json::Value, &str)] = &[
        (
            "mysql",
            "mysql",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
                "password": "pwd",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES (?, ?, ?)",
        ),
        (
            "alloydb",
            "alloydb",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
                "password": "pwd",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($1, $2, $3)",
        ),
        (
            "cockroachdb",
            "cockroachdb",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
                "password": "pwd",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($1, $2, $3)",
        ),
        (
            "mssql",
            "mssql",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($2, $4, $5)",
        ),
        (
            "oracle",
            "oracle",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "username": "u",
                "service_name": "XE",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES (:1, :2, :3)",
        ),
        (
            "redshift",
            "redshift",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($1, $2, $3)",
        ),
    ];
    for (kind, action_type, fields, sql) in kinds {
        let kernel = spawn_kernel("").await;
        let connector = format!("mv02a-dead-{kind}");
        let mut connector_body = serde_json::json!({
            "name": connector,
            "type": *action_type,
        });
        for (key, value) in fields.as_object().expect("connector fields") {
            connector_body[key] = value.clone();
        }
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/connectors",
            connector_body,
        )
        .await;
        assert_eq!(status, 201, "{kind} connector create: {body}");
        let action = format!("mv02a-dead-act-{kind}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/actions",
            serde_json::json!({
                "name": action,
                "type": *action_type,
                "connector": connector,
                "parameters": {"sql": *sql},
            }),
        )
        .await;
        assert_eq!(status, 201, "{kind} action create: {body}");
        let topic = format!("mv02a/in/dead-{kind}");
        let rule = format!("mv02a-dead-rule-{kind}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/rules",
            serde_json::json!({
                "id": rule,
                "sql": format!("SELECT * FROM \"{topic}\""),
                "actions": [action.clone()],
            }),
        )
        .await;
        assert_eq!(status, 201, "{kind} rule create: {body}");
        let mut publisher = mv00_connect(&kernel, &format!("mv02a-dead-pub-{kind}")).await;
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&topic, 0, 0, false, br#"{"v":"a\"b"}"#),
        )
        .await;
        let deadline = std::time::Instant::now() + STEP;
        loop {
            let (_, body) = mv00_api(
                &kernel,
                reqwest::Method::GET,
                &format!("/api/v5/actions/{action}/metrics"),
                serde_json::Value::Null,
            )
            .await;
            if body["metrics"]["failed"].as_u64().unwrap_or(0) >= 1 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{kind} dead-target fire failed in time: {body}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/actions/{action}/metrics"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "{kind} metrics: {body}");
        assert_eq!(
            body["metrics"]["failed"],
            serde_json::json!(1),
            "{kind} one fire counts one failed: {body}"
        );
        kernel.finish().await;
    }
}

/// MV-02a N3: an accepted action update swaps the statement, a refused
/// update keeps the old delivery, and a restart keeps the state. This
/// runs for all six kinds against a dead server through the broker.
#[tokio::test]
async fn mv02a_relational_update_and_restart_for_all_kinds() {
    let kinds: &[(&str, &str, serde_json::Value, &str, &str)] = &[
        (
            "mysql",
            "mysql",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
                "password": "pwd",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES (?, ?, ?)",
            "INSERT INTO t2(topic, qos, payload) VALUES (?, ?, ?)",
        ),
        (
            "alloydb",
            "alloydb",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
                "password": "pwd",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($1, $2, $3)",
            "INSERT INTO t2(topic, qos, payload) VALUES ($1, $2, $3)",
        ),
        (
            "cockroachdb",
            "cockroachdb",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
                "password": "pwd",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($1, $2, $3)",
            "INSERT INTO t2(topic, qos, payload) VALUES ($1, $2, $3)",
        ),
        (
            "mssql",
            "mssql",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($2, $4, $5)",
            "INSERT INTO t2(topic, qos, payload) VALUES ($2, $4, $5)",
        ),
        (
            "oracle",
            "oracle",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "username": "u",
                "service_name": "XE",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES (:1, :2, :3)",
            "INSERT INTO t2(topic, qos, payload) VALUES (:1, :2, :3)",
        ),
        (
            "redshift",
            "redshift",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($1, $2, $3)",
            "INSERT INTO t2(topic, qos, payload) VALUES ($1, $2, $3)",
        ),
    ];
    for (kind, action_type, fields, sql_v1, sql_v2) in kinds {
        let mut kernel = spawn_kernel("").await;
        let connector = format!("mv02a-upd-{kind}");
        let mut connector_body = serde_json::json!({
            "name": connector,
            "type": *action_type,
        });
        for (key, value) in fields.as_object().expect("connector fields") {
            connector_body[key] = value.clone();
        }
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/connectors",
            connector_body,
        )
        .await;
        assert_eq!(status, 201, "{kind} connector create: {body}");
        let action = format!("mv02a-upd-act-{kind}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/actions",
            serde_json::json!({
                "name": action,
                "type": *action_type,
                "connector": connector,
                "parameters": {"sql": *sql_v1},
            }),
        )
        .await;
        assert_eq!(status, 201, "{kind} action create: {body}");
        let topic = format!("mv02a/in/upd-{kind}");
        let rule = format!("mv02a-upd-rule-{kind}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/rules",
            serde_json::json!({
                "id": rule,
                "sql": format!("SELECT * FROM \"{topic}\""),
                "actions": [action.clone()],
            }),
        )
        .await;
        assert_eq!(status, 201, "{kind} rule create: {body}");
        let mut publisher = mv00_connect(&kernel, &format!("mv02a-upd-pub-{kind}")).await;
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&topic, 0, 0, false, br#"{"u":1}"#),
        )
        .await;
        let deadline = std::time::Instant::now() + STEP;
        loop {
            let (_, body) = mv00_api(
                &kernel,
                reqwest::Method::GET,
                &format!("/api/v5/actions/{action}/metrics"),
                serde_json::Value::Null,
            )
            .await;
            if body["metrics"]["failed"].as_u64().unwrap_or(0) >= 1 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{kind} first fire failed in time: {body}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::PUT,
            &format!("/api/v5/actions/{action}"),
            serde_json::json!({
                "name": action,
                "type": *action_type,
                "connector": connector,
                "parameters": {"sql": *sql_v2},
            }),
        )
        .await;
        assert_eq!(status, 200, "{kind} accepted update: {body}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/actions/{action}"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "{kind} action read: {body}");
        assert_eq!(
            body["parameters"]["sql"],
            serde_json::json!(*sql_v2),
            "{body}"
        );
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&topic, 0, 0, false, br#"{"u":2}"#),
        )
        .await;
        let deadline = std::time::Instant::now() + STEP;
        loop {
            let (_, body) = mv00_api(
                &kernel,
                reqwest::Method::GET,
                &format!("/api/v5/actions/{action}/metrics"),
                serde_json::Value::Null,
            )
            .await;
            if body["metrics"]["failed"].as_u64().unwrap_or(0) >= 2 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{kind} updated fire failed in time: {body}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::PUT,
            &format!("/api/v5/actions/{action}"),
            serde_json::json!({
                "name": action,
                "type": *action_type,
                "connector": connector,
                "parameters": {},
            }),
        )
        .await;
        assert_eq!(status, 400, "{kind} missing sql refuses: {body}");
        assert!(
            body["message"].as_str().unwrap_or("").contains(&action)
                && body["message"]
                    .as_str()
                    .unwrap_or("")
                    .contains("parameters.sql"),
            "{kind} refusal names the action and the field: {body}"
        );
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&topic, 0, 0, false, br#"{"u":3}"#),
        )
        .await;
        let deadline = std::time::Instant::now() + STEP;
        loop {
            let (_, body) = mv00_api(
                &kernel,
                reqwest::Method::GET,
                &format!("/api/v5/actions/{action}/metrics"),
                serde_json::Value::Null,
            )
            .await;
            if body["metrics"]["failed"].as_u64().unwrap_or(0) >= 3 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{kind} refused-update fire kept delivery: {body}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        mv00_restart(&mut kernel).await;
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/actions/{action}"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "{kind} action survives restart: {body}");
        assert_eq!(
            body["parameters"]["sql"],
            serde_json::json!(*sql_v2),
            "{body}"
        );
        let mut publisher = mv00_connect(&kernel, &format!("mv02a-upd-pub2-{kind}")).await;
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&topic, 0, 0, false, br#"{"u":4}"#),
        )
        .await;
        let deadline = std::time::Instant::now() + STEP;
        loop {
            let (_, body) = mv00_api(
                &kernel,
                reqwest::Method::GET,
                &format!("/api/v5/actions/{action}/metrics"),
                serde_json::Value::Null,
            )
            .await;
            if body["metrics"]["failed"].as_u64().unwrap_or(0) >= 1 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{kind} post-restart fire failed in time: {body}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        kernel.finish().await;
    }
}
