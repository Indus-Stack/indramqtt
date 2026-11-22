use super::*;
#[cfg(test)]
pub(crate) use crate::jwks_test_support::{
    TestJwksServer as JwksTestServer, TestSigningKey as JwksSigningKey,
};
#[cfg(test)]
pub(crate) use brokerlink::FramedTransport;

/// Unique scratch data dir under the OS temp dir (no dev-dependency).
#[cfg(test)]
pub(crate) fn unique_data_dir() -> std::path::PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let slot = COUNTER.fetch_add(1, Ordering::SeqCst);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "indramqtt-w019-datadir-{}-{nanos}-{slot}",
        std::process::id()
    ))
}

/// Parses a command line as `main` does. Resolves it with the files
/// in `config_dir`.
#[cfg(test)]
pub(crate) fn resolve_boot(config_dir: &std::path::Path, flags: &[&str]) -> Args {
    let mut argv = vec![
        "indramqtt".to_string(),
        "--config-dir".to_string(),
        config_dir.to_str().expect("temp path is UTF-8").to_string(),
    ];
    argv.extend(flags.iter().map(|flag| flag.to_string()));
    let matches = <Args as clap::CommandFactory>::command().get_matches_from(argv);
    let mut args = <Args as clap::FromArgMatches>::from_arg_matches(&matches).expect("flags parse");
    let layered = load_startup_config(&args, &matches).expect("config resolves");
    apply_resolved_config(&mut args, &layered).expect("resolved config applies");
    args
}

/// Encode `BindConnection` metadata (test mirror of the BEAM
/// `indra_brokerlink:encode_bind_meta/3` contract).
#[cfg(test)]
pub(crate) fn encode_bind_meta(client_id: &str, clean_start: bool, keepalive: u16) -> Bytes {
    let id = client_id.as_bytes();
    let mut meta = Vec::with_capacity(2 + id.len() + 1 + 2);
    meta.extend_from_slice(&(id.len() as u16).to_be_bytes());
    meta.extend_from_slice(id);
    meta.push(u8::from(clean_start));
    meta.extend_from_slice(&keepalive.to_be_bytes());
    Bytes::from(meta)
}

/// Decode `SessionBinding` metadata into `(session_id, present, rc)`.
/// Accepts 10-byte (pre-alias), 12-byte (B4-05 with the trailing
/// Topic Alias Maximum) and X1-02 extended
/// (`Head12 | V5Payload | V5Len:16be`) encodings; the head always
/// parses the same way.
#[cfg(test)]
pub(crate) fn decode_session_binding_meta(meta: &[u8]) -> (u64, bool, u8) {
    let (session_id, present, rc, _, _) =
        crate::decode_session_binding_full(meta).expect("SessionBinding meta must decode");
    (session_id, present, rc)
}

#[cfg(test)]
pub(crate) fn bind_frame(conn_id: u64, seq: u64, meta: Bytes) -> BrokerFrame {
    BrokerFrame::new(OpCode::BindConnection, conn_id, seq, meta, Bytes::new())
        .expect("valid bind frame")
}

/// Encode `BindConnection` metadata with credentials (test mirror of
/// the BEAM `indra_brokerlink:encode_bind_meta/4` contract).
#[cfg(test)]
pub(crate) fn encode_bind_meta_creds(
    client_id: &str,
    clean_start: bool,
    keepalive: u16,
    username: &str,
    password: &[u8],
) -> Bytes {
    let mut meta = encode_bind_meta(client_id, clean_start, keepalive).to_vec();
    meta.extend_from_slice(&(username.len() as u16).to_be_bytes());
    meta.extend_from_slice(username.as_bytes());
    meta.extend_from_slice(&(password.len() as u16).to_be_bytes());
    meta.extend_from_slice(password);
    Bytes::from(meta)
}

/// Encode `SubscribeMeta` (test mirror of the BEAM contract).
#[cfg(test)]
pub(crate) fn encode_subscribe_meta(packet_id: u16, client_id: &str, subs: &[(&str, u8)]) -> Bytes {
    let id = client_id.as_bytes();
    let mut meta = Vec::new();
    meta.extend_from_slice(&packet_id.to_be_bytes());
    meta.extend_from_slice(&(id.len() as u16).to_be_bytes());
    meta.extend_from_slice(id);
    meta.extend_from_slice(&(subs.len() as u16).to_be_bytes());
    for (filter, qos) in subs {
        meta.extend_from_slice(&(filter.len() as u16).to_be_bytes());
        meta.extend_from_slice(filter.as_bytes());
        meta.push(*qos);
    }
    Bytes::from(meta)
}

/// Encode `PublishMeta` (test mirror of the BEAM contract).
/// Pre-alias encoding (no trailing alias section, decodes as alias
/// 0); kept to prove old-length frames still route.
#[cfg(test)]
pub(crate) fn encode_publish_meta(
    topic: &str,
    packet_id: u16,
    qos: u8,
    retain: bool,
    payload: &[u8],
) -> (Bytes, Bytes) {
    let mut meta = Vec::new();
    meta.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    meta.extend_from_slice(topic.as_bytes());
    meta.extend_from_slice(&packet_id.to_be_bytes());
    meta.push(qos);
    meta.push(u8::from(retain));
    meta.push(0u8);
    (Bytes::from(meta), Bytes::from(payload.to_vec()))
}

#[cfg(test)]
pub(crate) fn decode_publish_meta_parts(meta: &[u8]) -> (String, u16, u8, bool) {
    let topic_len = u16::from_be_bytes([meta[0], meta[1]]) as usize;
    let topic = if topic_len == 0 {
        String::new()
    } else {
        std::str::from_utf8(&meta[2..2 + topic_len])
            .unwrap()
            .to_string()
    };
    let base = 2 + topic_len;
    let packet_id = u16::from_be_bytes([meta[base], meta[base + 1]]);
    (topic, packet_id, meta[base + 2], meta[base + 3] != 0)
}

#[cfg(test)]
pub(crate) fn puback_frame(conn_id: u64, seq: u64, packet_id: u16) -> BrokerFrame {
    let mut meta = Vec::with_capacity(3);
    meta.extend_from_slice(&packet_id.to_be_bytes());
    meta.push(0u8);
    BrokerFrame::new(
        OpCode::PubAckIn,
        conn_id,
        seq,
        Bytes::from(meta),
        Bytes::new(),
    )
    .expect("valid puback frame")
}

#[cfg(test)]
pub(crate) fn subscribe_frame(conn_id: u64, seq: u64, meta: Bytes) -> BrokerFrame {
    BrokerFrame::new(OpCode::SubscribeIn, conn_id, seq, meta, Bytes::new())
        .expect("valid subscribe frame")
}

#[cfg(test)]
pub(crate) fn publish_frame(conn_id: u64, seq: u64, meta: Bytes, payload: Bytes) -> BrokerFrame {
    BrokerFrame::new(OpCode::PublishIn, conn_id, seq, meta, payload).expect("valid publish frame")
}

/// Binds a publisher client on `conn_id` through the bind entry. A
/// PUBLISH from a connection with no session routes nothing, so a test
/// that publishes from a bare connection id binds one first.
#[cfg(test)]
pub(crate) fn bind_test_publisher(shared: &Shared, conn_id: u64) {
    let client_id = format!("test-publisher-{conn_id}");
    let bind = bind_frame(conn_id, 1, encode_bind_meta(&client_id, true, 60));
    reply_for_frame(&bind, &shared.sessions).expect("bind replies");
}

#[cfg(test)]
pub(crate) fn test_shared() -> Shared {
    let mut shared = Shared::new();
    shared.allow_anonymous = true;
    shared
}

#[cfg(test)]
pub(crate) fn unbind_frame(conn_id: u64, seq: u64, client_id: &str) -> BrokerFrame {
    let mut meta = Vec::with_capacity(2 + client_id.len());
    meta.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    meta.extend_from_slice(client_id.as_bytes());
    BrokerFrame::new(
        OpCode::UnbindConnection,
        conn_id,
        seq,
        Bytes::from(meta),
        Bytes::new(),
    )
    .expect("valid unbind frame")
}

#[cfg(test)]
pub(crate) fn disconnect_frame(conn_id: u64, seq: u64, client_id: &str) -> BrokerFrame {
    let mut meta = Vec::with_capacity(2 + client_id.len());
    meta.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    meta.extend_from_slice(client_id.as_bytes());
    BrokerFrame::new(
        OpCode::DisconnectIn,
        conn_id,
        seq,
        Bytes::from(meta),
        Bytes::new(),
    )
    .expect("valid disconnect frame")
}

/// Encode `BindConnection` metadata with an F1-01 last will (test
/// mirror of the edge `encode_bind_meta/7` contract without
/// credentials, peer or alias sections): will-bit head, then
/// `WillQos:8 | WillRetain:8 | TopicLen:16be | Topic |
/// PayloadLen:32be | Payload`.
#[cfg(test)]
pub(crate) fn encode_bind_meta_with_will(
    client_id: &str,
    clean_start: bool,
    keepalive: u16,
    topic: &str,
    payload: &[u8],
    qos: u8,
    retain: bool,
) -> Bytes {
    let id = client_id.as_bytes();
    let mut meta = Vec::new();
    meta.extend_from_slice(&(id.len() as u16).to_be_bytes());
    meta.extend_from_slice(id);
    meta.push(u8::from(clean_start) | 0x02);
    meta.extend_from_slice(&keepalive.to_be_bytes());
    meta.push(qos);
    meta.push(u8::from(retain));
    meta.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    meta.extend_from_slice(topic.as_bytes());
    meta.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    meta.extend_from_slice(payload);
    Bytes::from(meta)
}

#[cfg(test)]
pub(crate) async fn bind_client(
    addr: std::net::SocketAddr,
    conn_id: u64,
    client_id: &str,
    clean_start: bool,
) -> FramedTransport<tokio::net::TcpStream> {
    let io = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect client");
    let client = FramedTransport::new(io);
    client
        .send(bind_frame(
            conn_id,
            1,
            encode_bind_meta(client_id, clean_start, 60),
        ))
        .await
        .expect("bind");
    let binding = client.recv().await.expect("recv binding");
    assert_eq!(binding.header.opcode, OpCode::SessionBinding);
    client
}

/// JWKS-enabled broker state for the CONNECT tests: endpoint plus
/// issuer/audience pinned, short timeouts so outage legs stay fast.
#[cfg(test)]
pub(crate) fn jwks_test_shared(url: String) -> Shared {
    let mut shared = test_shared();
    let config = JwksConfig {
        jwks_url: url,
        issuer: "https://issuer.example".to_string(),
        audience: "indra-mqtt".to_string(),
        refresh_period_secs: 60,
        fetch_timeout_ms: 3_000,
        refresh_timeout_ms: 3_000,
        cache_max_keys: 32,
        cache_ttl_secs: 60,
        max_document_bytes: broker_auth::JWKS_DEFAULT_DOCUMENT_CAP,
        clock_skew_secs: 60,
        // Loopback fixture only: production endpoints keep TLS on.
        tls_verify: false,
        ca_cert_path: None,
    };
    shared.jwks = Some(Arc::new(JwksAuthenticator::new(config)));
    shared
}

/// Test-owned webhook verdict endpoint on loopback (B5-04). It
/// verifies the credential it receives: auth requests are allowed
/// only when the username matches `expected_user` and the base64
/// password decodes to `expected_pass`, so an allow verdict proves
/// the broker POSTed the real credential. Publish requests follow
/// the `publish_allow` flag (flipped mid-test for TTL); `fail`
/// answers 500 to prove the breaker path. Every request bumps
/// `hits` so tests assert exact endpoint contact: a cache hit must
/// not contact it, a TTL expiry must re-ask exactly once, and an
/// open circuit must not contact it at all.
#[cfg(test)]
pub(crate) struct WebhookTestServer {
    pub(crate) expected_user: String,
    pub(crate) expected_pass: Vec<u8>,
    pub(crate) publish_allow: std::sync::Mutex<bool>,
    pub(crate) fail: std::sync::Mutex<bool>,
    pub(crate) hits: AtomicU64,
}

#[cfg(test)]
impl WebhookTestServer {
    pub(crate) fn new(user: &str, pass: &[u8]) -> Self {
        Self {
            expected_user: user.to_string(),
            expected_pass: pass.to_vec(),
            publish_allow: std::sync::Mutex::new(true),
            fail: std::sync::Mutex::new(false),
            hits: AtomicU64::new(0),
        }
    }
}

#[cfg(test)]
pub(crate) async fn webhook_test_handler(
    axum::extract::State(server): axum::extract::State<Arc<WebhookTestServer>>,
    body: axum::body::Bytes,
) -> (axum::http::StatusCode, String) {
    use base64::Engine as _;
    server.hits.fetch_add(1, Ordering::Relaxed);
    if *server.fail.lock().unwrap() {
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({"error": "fault"}).to_string(),
        );
    }
    let parsed: serde_json::Value =
        serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    let allow = match parsed.get("kind").and_then(|value| value.as_str()) {
        Some("auth") => {
            let user_ok = parsed.get("username").and_then(|value| value.as_str())
                == Some(server.expected_user.as_str());
            let pass_ok = parsed
                .get("password_b64")
                .and_then(|value| value.as_str())
                .and_then(|encoded| {
                    base64::engine::general_purpose::STANDARD
                        .decode(encoded)
                        .ok()
                })
                == Some(server.expected_pass.clone());
            user_ok && pass_ok
        }
        Some("publish") => *server.publish_allow.lock().unwrap(),
        _ => false,
    };
    (
        axum::http::StatusCode::OK,
        serde_json::json!({"allow": allow}).to_string(),
    )
}

#[cfg(test)]
pub(crate) async fn start_webhook_test_server(
    server: Arc<WebhookTestServer>,
) -> (tokio::task::JoinHandle<()>, String) {
    let app = axum::Router::new()
        .route("/", axum::routing::post(webhook_test_handler))
        .with_state(server);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let url = format!("http://{}", listener.local_addr().expect("local addr"));
    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve webhook verdicts");
    });
    (handle, url)
}

#[cfg(test)]
pub(crate) fn webhook_test_config(url: String) -> WebhookConfig {
    WebhookConfig {
        endpoint_url: url,
        pool_size: 4,
        request_timeout_ms: 2_000,
        breaker_failure_threshold: 5,
        breaker_reset_timeout_ms: 30_000,
        cache_max_entries: 128,
        cache_ttl_secs: 60,
    }
}

/// Test mirror of the edge peer-address section: append
/// `PeerLen:16be | PeerIp` to an encoded bind.
#[cfg(test)]
pub(crate) fn encode_bind_meta_peer(base: Bytes, peer: &str) -> Bytes {
    let mut meta = base.to_vec();
    meta.extend_from_slice(&(peer.len() as u16).to_be_bytes());
    meta.extend_from_slice(peer.as_bytes());
    Bytes::from(meta)
}

/// Test mirror of the B4-05 trailing alias section: append
/// `ClientAliasMax:16be` to any encoded bind (mirrors the edge
/// `encode_bind_meta/6` tail, which always trails last).
#[cfg(test)]
pub(crate) fn encode_bind_meta_alias(base: Bytes, alias_max: u16) -> Bytes {
    let mut meta = base.to_vec();
    meta.extend_from_slice(&protocol_v5::encode_alias_maximum(alias_max));
    Bytes::from(meta)
}
