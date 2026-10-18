//! Messaging tools of the agent endpoint (`POST /mcp`, MC-04).
//!
//! Each test boots one real kernel and works through the production
//! entries only: MQTT 3.1.1 over real TCP sockets plus the management
//! API and `/mcp` over HTTP. No test touches a store directly.
//!
//! Covered: the publish operation tool (`post_api_v5_publish`, same
//! handler and same result shape as `POST /api/v5/publish`);
//! `subscribe_collect` makes a temporary internal subscription,
//! returns topic, QoS, retain flag and payload, and removes the
//! subscription on every exit path; limits are enforced; keys need
//! publish rights; read-only mode hides both tools.

use std::time::Duration;

/// Bound for one socket or HTTP step. A missing answer fails.
const STEP: Duration = Duration::from_secs(10);
/// Bound for one kernel boot. A cold debug boot needs the full minute.
const BOOT: Duration = Duration::from_secs(60);
/// Operator key for the management API. It clears the 16-char minimum.
const API_KEY: &str = "mc04-e2e-test-key-0001";

/// One booted kernel for a test.
struct Kernel {
    child: tokio::process::Child,
    mqtt_port: u16,
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

/// Boots one real kernel on ephemeral ports. `extra_toml` appends to
/// `indra.toml` (the `[mcp]` section of the read-only and disabled
/// tests). Anonymous MQTT is allowed; the operator key guards the API.
/// Each listener uses an ephemeral loopback port, so parallel tests
/// never share a port.
async fn spawn_kernel(name: &str, extra_toml: &str) -> Kernel {
    let tag = format!(
        "mc04-{name}-{}-{}",
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
             id = \"mc04-{tag}\"\n\
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
        api_port,
        scratch,
        config_dir,
    }
}

/// Locates the real kernel binary for the spawned-kernel tests.
fn kernel_binary() -> std::path::PathBuf {
    if let Some(built) = option_env!("CARGO_BIN_EXE_indramqtt") {
        return std::path::PathBuf::from(built);
    }
    let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for candidate in [
        manifest.join("../target/debug/indramqtt"),
        manifest.join("../target/ci-check/debug/indramqtt"),
    ] {
        if candidate.is_file() {
            return candidate;
        }
    }
    panic!("kernel binary not found: build the workspace first");
}

/// MQTT variable-length remaining length.
fn mqtt_rl(mut len: usize) -> Vec<u8> {
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

/// MQTT UTF-8 string (`len:16be | bytes`).
fn mqtt_str(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + s.len());
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
    out
}

/// Anonymous MQTT 3.1.1 CONNECT (clean session, keepalive 60).
fn mqtt_connect(client_id: &str) -> Vec<u8> {
    let mut rest = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3C];
    rest.extend_from_slice(&mqtt_str(client_id));
    let mut packet = vec![0x10];
    packet.extend_from_slice(&mqtt_rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// MQTT 3.1.1 SUBSCRIBE for one filter at QoS 0.
fn mqtt_subscribe(packet_id: u16, filter: &str) -> Vec<u8> {
    let mut rest = Vec::new();
    rest.extend_from_slice(&packet_id.to_be_bytes());
    rest.extend_from_slice(&mqtt_str(filter));
    rest.push(0);
    let mut packet = vec![0x82];
    packet.extend_from_slice(&mqtt_rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// MQTT 3.1.1 PUBLISH (client to kernel) at QoS 0.
fn mqtt_publish(topic: &str, retain: bool, payload: &[u8]) -> Vec<u8> {
    let mut rest = mqtt_str(topic);
    rest.extend_from_slice(payload);
    let mut flags = 0u8;
    if retain {
        flags |= 0x01;
    }
    let mut packet = vec![0x30 | flags];
    packet.extend_from_slice(&mqtt_rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// Reads one MQTT control packet: the fixed-header first byte plus the
/// body. Only used with an explicit caller timeout.
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

/// Parses a server-to-client PUBLISH body (topic plus payload at QoS 0).
fn parse_downlink(body: &[u8]) -> (String, Vec<u8>) {
    assert!(body.len() >= 2, "PUBLISH carries a topic");
    let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
    assert!(body.len() >= 2 + topic_len, "PUBLISH topic fits");
    let topic = std::str::from_utf8(&body[2..2 + topic_len])
        .expect("topic UTF-8")
        .to_string();
    (topic, body[2 + topic_len..].to_vec())
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

/// Connects one anonymous client. Panics unless CONNACK accepts it.
async fn connect_client(sock: &mut tokio::net::TcpStream, client_id: &str) {
    send_mqtt(sock, &mqtt_connect(client_id)).await;
    let (head, body) = recv_mqtt(sock).await;
    assert_eq!(head, 0x20, "kernel answers with CONNACK");
    assert_eq!(body, vec![0x00, 0x00], "CONNECT is accepted");
}

/// Subscribes one filter at QoS 0. Panics unless SUBACK grants it.
async fn subscribe_filter(sock: &mut tokio::net::TcpStream, filter: &str) {
    send_mqtt(sock, &mqtt_subscribe(1, filter)).await;
    let (head, body) = recv_mqtt(sock).await;
    assert_eq!(head & 0xF0, 0x90, "kernel answers with SUBACK");
    assert_eq!(body, vec![0x00, 0x01, 0x00], "subscribe is granted");
}

/// Calls `POST /mcp` with a bearer key and a JSON-RPC body.
async fn mcp_call(
    http: &reqwest::Client,
    base: &str,
    key: &str,
    body: &serde_json::Value,
) -> (u16, serde_json::Value) {
    mcp_call_query(http, base, key, "", body).await
}

/// Calls `POST /mcp` with a query string (the `toolsets` selector).
async fn mcp_call_query(
    http: &reqwest::Client,
    base: &str,
    key: &str,
    query: &str,
    body: &serde_json::Value,
) -> (u16, serde_json::Value) {
    let res = tokio::time::timeout(
        STEP,
        http.post(format!("{base}/mcp{query}"))
            .bearer_auth(key)
            .json(body)
            .send(),
    )
    .await
    .expect("MCP in time")
    .expect("MCP send");
    let status = res.status().as_u16();
    let body: serde_json::Value = tokio::time::timeout(STEP, res.json())
        .await
        .expect("body in time")
        .expect("body is JSON");
    (status, body)
}

/// Reads the text of a successful `tools/call` result.
fn call_text(body: &serde_json::Value) -> &str {
    body["result"]["content"][0]["text"]
        .as_str()
        .expect("text content")
}

/// Starts one `subscribe_collect` call in the background. The call
/// blocks until its limit or its time bound, so the test publishes
/// while it runs.
fn spawn_collect(
    http: reqwest::Client,
    base: String,
    key: String,
    id: u64,
    args: serde_json::Value,
) -> tokio::task::JoinHandle<(u16, serde_json::Value)> {
    tokio::spawn(async move {
        mcp_call(
            &http,
            &base,
            &key,
            &serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "tools/call",
                "params": {"name": "subscribe_collect", "arguments": args}
            }),
        )
        .await
    })
}

/// Publishes through `POST /api/v5/publish` with a bearer key.
async fn http_publish(
    http: &reqwest::Client,
    base: &str,
    key: &str,
    topic: &str,
    payload: &str,
) -> (u16, serde_json::Value) {
    let res = tokio::time::timeout(
        STEP,
        http.post(format!("{base}/api/v5/publish"))
            .bearer_auth(key)
            .json(&serde_json::json!({"topic": topic, "payload": payload}))
            .send(),
    )
    .await
    .expect("publish in time")
    .expect("publish send");
    let status = res.status().as_u16();
    let body: serde_json::Value = tokio::time::timeout(STEP, res.json())
        .await
        .expect("body in time")
        .expect("body is JSON");
    (status, body)
}

/// A socket publish arrives in `subscribe_collect`, and the temporary
/// subscription is gone afterwards.
///
/// The test also proves the subscription exists while the collect
/// runs: a management publish to the quiet topic reports 503 (the
/// temporary subscription matches, but no mailbox delivery happens)
/// instead of the baseline 202. After the collect it reports 202
/// again, so no row lingers.
#[tokio::test]
async fn mc04_socket_publish_arrives_in_collect_and_leaves_no_row() {
    let kernel = spawn_kernel("roundtrip", "").await;
    let http = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{}", kernel.api_port);

    let (status, listed) = mcp_call(
        &http,
        &base,
        API_KEY,
        &serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
    )
    .await;
    assert_eq!(status, 200);
    let names: Vec<&str> = listed["result"]["tools"]
        .as_array()
        .expect("tools list")
        .iter()
        .map(|tool| tool["name"].as_str().expect("tool name"))
        .collect();
    assert!(
        names.contains(&"post_api_v5_publish"),
        "core set holds publish"
    );
    assert!(names.contains(&"subscribe_collect"));

    let list_body = serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"});
    let (status, all) = mcp_call_query(&http, &base, API_KEY, "?toolsets=all", &list_body).await;
    assert_eq!(status, 200);
    let all_names: Vec<&str> = all["result"]["tools"]
        .as_array()
        .expect("tools list")
        .iter()
        .map(|tool| tool["name"].as_str().expect("tool name"))
        .collect();
    assert!(all_names.contains(&"post_api_v5_publish"));
    assert!(all_names.contains(&"subscribe_collect"));
    let (status, none) = mcp_call_query(&http, &base, API_KEY, "?toolsets=nope", &list_body).await;
    assert_eq!(status, 200);
    assert_eq!(
        none["result"]["tools"],
        serde_json::json!([]),
        "unknown set keeps nothing"
    );

    let (status, _) = http_publish(&http, &base, API_KEY, "mc04/rt/probe", "x").await;
    assert_eq!(status, 202, "quiet topic matches nothing yet");

    let pending = spawn_collect(
        http.clone(),
        base.clone(),
        API_KEY.to_string(),
        2,
        serde_json::json!({
            "topic_filter": "mc04/rt/#",
            "max_messages": 2,
            "timeout_seconds": 8,
            "qos": 0
        }),
    );
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (status, busy) = http_publish(&http, &base, API_KEY, "mc04/rt/probe", "x").await;
    assert_eq!(status, 503, "running collect matches, delivery fails");
    assert_eq!(busy["reason_code"], serde_json::json!(131));

    let mut publisher = dial_mqtt(kernel.mqtt_port).await;
    connect_client(&mut publisher, "mc04-rt-pub").await;
    let secret = format!("mc04-{}-rt", std::process::id());
    send_mqtt(
        &mut publisher,
        &mqtt_publish("mc04/rt/one", false, secret.as_bytes()),
    )
    .await;

    let (status, done) = tokio::time::timeout(STEP, pending)
        .await
        .expect("collect ends in time")
        .expect("collect task ends");
    assert_eq!(status, 200);
    let result: serde_json::Value = serde_json::from_str(call_text(&done)).expect("result is JSON");
    let messages = result["messages"].as_array().expect("messages list");
    // The management probe arrives first (it feeds the collect while
    // matching only the temporary subscription), then the socket
    // publish. The limit ends the collect at once.
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0]["topic"], serde_json::json!("mc04/rt/probe"));
    assert_eq!(messages[0]["payload"].as_str(), Some("x"));
    assert_eq!(messages[1]["topic"], serde_json::json!("mc04/rt/one"));
    assert_eq!(messages[1]["qos"], serde_json::json!(0));
    assert_eq!(messages[1]["retain"], serde_json::json!(false));
    assert_eq!(messages[1]["payload"].as_str(), Some(secret.as_str()));
    assert_eq!(messages[1]["payload_encoding"], serde_json::json!("plain"));
    assert_eq!(messages[1]["truncated"], serde_json::json!(false));

    let (status, _) = http_publish(&http, &base, API_KEY, "mc04/rt/probe", "x").await;
    assert_eq!(status, 202, "collect exit removes the subscription");

    send_mqtt(&mut publisher, &mqtt_publish("mc04/rt/ret", true, b"stale")).await;
    let pending = spawn_collect(
        http.clone(),
        base.clone(),
        API_KEY.to_string(),
        3,
        serde_json::json!({
            "topic_filter": "mc04/rt/ret",
            "max_messages": 1,
            "timeout_seconds": 5,
            "qos": 0
        }),
    );
    let (status, done) = tokio::time::timeout(STEP, pending)
        .await
        .expect("collect ends in time")
        .expect("collect task ends");
    assert_eq!(status, 200);
    let result: serde_json::Value = serde_json::from_str(call_text(&done)).expect("result is JSON");
    let messages = result["messages"].as_array().expect("messages list");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["payload"].as_str(), Some("stale"));
    assert_eq!(messages[0]["retain"], serde_json::json!(true));
    kernel.finish().await;
}

/// The publish operation tool runs the `POST /api/v5/publish` handler:
/// same result shape, and a socket subscriber receives the message.
#[tokio::test]
async fn mc04_publish_tool_matches_http_and_reaches_subscribers() {
    let kernel = spawn_kernel("tool", "").await;
    let http = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{}", kernel.api_port);

    let mut subscriber = dial_mqtt(kernel.mqtt_port).await;
    connect_client(&mut subscriber, "mc04-tool-sub").await;
    subscribe_filter(&mut subscriber, "mc04/tool/#").await;

    let secret = format!("mc04-{}-tool", std::process::id());
    let (status, tool_result) = mcp_call(
        &http,
        &base,
        API_KEY,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "post_api_v5_publish",
                "arguments": {"body": {"topic": "mc04/tool/one", "payload": secret}}
            }
        }),
    )
    .await;
    assert_eq!(status, 200);
    assert!(tool_result.get("error").is_none(), "tool call succeeds");
    let tool_body: serde_json::Value =
        serde_json::from_str(call_text(&tool_result)).expect("tool text is JSON");

    let (http_status, http_body) =
        http_publish(&http, &base, API_KEY, "mc04/tool/one", &secret).await;
    assert_eq!(http_status, 200);
    assert_eq!(
        tool_body
            .as_object()
            .expect("tool body is an object")
            .keys()
            .collect::<Vec<_>>(),
        http_body
            .as_object()
            .expect("HTTP body is an object")
            .keys()
            .collect::<Vec<_>>(),
        "tool and HTTP share the result shape"
    );
    assert!(tool_body.get("id").is_some());

    let (head, body) = recv_mqtt(&mut subscriber).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let (topic, payload) = parse_downlink(&body);
    assert_eq!(topic, "mc04/tool/one");
    assert_eq!(
        payload,
        secret.as_bytes(),
        "tool publish reaches subscribers"
    );
    kernel.finish().await;
}

/// A collect on a quiet topic returns empty at the time bound, and the
/// temporary subscription is gone afterwards.
#[tokio::test]
async fn mc04_subscription_removed_after_timeout() {
    let kernel = spawn_kernel("timeout", "").await;
    let http = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{}", kernel.api_port);

    let (status, done) = mcp_call(
        &http,
        &base,
        API_KEY,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "subscribe_collect",
                "arguments": {
                    "topic_filter": "mc04/quiet/#",
                    "max_messages": 10,
                    "timeout_seconds": 1,
                    "qos": 0
                }
            }
        }),
    )
    .await;
    assert_eq!(status, 200);
    let result: serde_json::Value = serde_json::from_str(call_text(&done)).expect("result is JSON");
    assert_eq!(result["messages"], serde_json::json!([]));

    let (status, _) = http_publish(&http, &base, API_KEY, "mc04/quiet/one", "x").await;
    assert_eq!(status, 202, "timed-out collect leaves no subscription");
    kernel.finish().await;
}

/// A client disconnect during a collect still ends with no temporary
/// subscription: the collect returns the later publish, and a later
/// management publish matches nothing.
#[tokio::test]
async fn mc04_subscription_removed_after_client_disconnect() {
    let kernel = spawn_kernel("disconnect", "").await;
    let http = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{}", kernel.api_port);

    let pending = spawn_collect(
        http.clone(),
        base.clone(),
        API_KEY.to_string(),
        1,
        serde_json::json!({
            "topic_filter": "mc04/disc/#",
            "max_messages": 1,
            "timeout_seconds": 6,
            "qos": 0
        }),
    );
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut leaving = dial_mqtt(kernel.mqtt_port).await;
    connect_client(&mut leaving, "mc04-disc-leaver").await;
    subscribe_filter(&mut leaving, "mc04/disc/#").await;
    drop(leaving);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut publisher = dial_mqtt(kernel.mqtt_port).await;
    connect_client(&mut publisher, "mc04-disc-pub").await;
    let secret = format!("mc04-{}-disc", std::process::id());
    send_mqtt(
        &mut publisher,
        &mqtt_publish("mc04/disc/one", false, secret.as_bytes()),
    )
    .await;

    let (status, done) = tokio::time::timeout(STEP, pending)
        .await
        .expect("collect ends in time")
        .expect("collect task ends");
    assert_eq!(status, 200);
    let result: serde_json::Value = serde_json::from_str(call_text(&done)).expect("result is JSON");
    let messages = result["messages"].as_array().expect("messages list");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["payload"].as_str(), Some(secret.as_str()));

    let (status, _) = http_publish(&http, &base, API_KEY, "mc04/disc/probe", "x").await;
    assert_eq!(status, 202, "collect exit removes the subscription");
    kernel.finish().await;
}

/// Limits hold: out-of-range arguments fail, payloads past 64 KiB
/// arrive cut with a flag, binary payloads arrive as base64.
#[tokio::test]
async fn mc04_collect_enforces_limits() {
    let kernel = spawn_kernel("limits", "").await;
    let http = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{}", kernel.api_port);

    for (label, args) in [
        (
            "too many",
            serde_json::json!({"topic_filter": "mc04/lim/#", "qos": 0, "max_messages": 101}),
        ),
        (
            "no minimum",
            serde_json::json!({"topic_filter": "mc04/lim/#", "qos": 0, "max_messages": 0}),
        ),
        (
            "no time",
            serde_json::json!({"topic_filter": "mc04/lim/#", "qos": 0, "timeout_seconds": 0}),
        ),
        (
            "too long",
            serde_json::json!({"topic_filter": "mc04/lim/#", "qos": 0, "timeout_seconds": 31}),
        ),
        (
            "bad qos",
            serde_json::json!({"topic_filter": "mc04/lim/#", "qos": 2}),
        ),
        (
            "missing qos",
            serde_json::json!({"topic_filter": "mc04/lim/#"}),
        ),
        (
            "bad filter",
            serde_json::json!({"topic_filter": "mc04/#/bad", "qos": 0}),
        ),
    ] {
        let (status, body) = mcp_call(
            &http,
            &base,
            API_KEY,
            &serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "subscribe_collect", "arguments": args}
            }),
        )
        .await;
        assert_eq!(status, 200, "{label}");
        assert_eq!(
            body["error"]["code"],
            serde_json::json!(-32602),
            "{label} fails closed"
        );
    }

    let mut publisher = dial_mqtt(kernel.mqtt_port).await;
    connect_client(&mut publisher, "mc04-lim-pub").await;
    let big = vec![b'A'; 70_000];
    let pending = spawn_collect(
        http.clone(),
        base.clone(),
        API_KEY.to_string(),
        2,
        serde_json::json!({
            "topic_filter": "mc04/big/#",
            "max_messages": 2,
            "timeout_seconds": 8,
            "qos": 0
        }),
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    send_mqtt(&mut publisher, &mqtt_publish("mc04/big/blob", false, &big)).await;
    send_mqtt(
        &mut publisher,
        &mqtt_publish("mc04/big/bin", false, &[0xff, 0xfe, 0x00]),
    )
    .await;
    let (status, done) = tokio::time::timeout(STEP, pending)
        .await
        .expect("collect ends in time")
        .expect("collect task ends");
    assert_eq!(status, 200);
    let result: serde_json::Value = serde_json::from_str(call_text(&done)).expect("result is JSON");
    let messages = result["messages"].as_array().expect("messages list");
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0]["payload"].as_str().expect("text").len(), 65_536);
    assert_eq!(messages[0]["truncated"], serde_json::json!(true));
    assert_eq!(messages[0]["payload_encoding"], serde_json::json!("plain"));
    assert_eq!(messages[1]["payload_encoding"], serde_json::json!("base64"));
    assert_eq!(messages[1]["truncated"], serde_json::json!(false));
    kernel.finish().await;
}

/// Mints one API key with a role through the management API.
async fn mint_key(
    http: &reqwest::Client,
    base: &str,
    name: &str,
    body: &serde_json::Value,
) -> (String, String) {
    let res = tokio::time::timeout(
        STEP,
        http.post(format!("{base}/api/v5/api_key"))
            .bearer_auth(API_KEY)
            .json(body)
            .send(),
    )
    .await
    .expect("mint in time")
    .expect("mint send");
    assert_eq!(res.status(), 200, "key {name} mints");
    let minted: serde_json::Value = tokio::time::timeout(STEP, res.json())
        .await
        .expect("body in time")
        .expect("body is JSON");
    (
        minted["api_key"].as_str().expect("key id").to_string(),
        minted["api_secret"].as_str().expect("secret").to_string(),
    )
}

/// Keys need publish rights for both tools; read-only mode hides both.
///
/// A viewer key sees neither tool and cannot call them. A key without
/// the publish scope sees neither either. A publisher key sees both. A
/// kernel started with `[mcp] read_only = true` hides both tools from
/// every key, and a kernel with `[mcp] enabled = false` answers 404.
#[tokio::test]
async fn mc04_tool_permissions_and_read_only() {
    let kernel = spawn_kernel("perms", "").await;
    let http = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{}", kernel.api_port);

    let (_, viewer_secret) = mint_key(
        &http,
        &base,
        "mc04-viewer",
        &serde_json::json!({"name": "mc04-viewer", "role": "viewer"}),
    )
    .await;
    let (_, publisher_secret) = mint_key(
        &http,
        &base,
        "mc04-publisher",
        &serde_json::json!({"name": "mc04-publisher", "role": "publisher"}),
    )
    .await;
    let (_, scoped_secret) = mint_key(
        &http,
        &base,
        "mc04-scoped",
        &serde_json::json!({
            "name": "mc04-scoped",
            "role": "administrator",
            "scopes": ["monitoring"]
        }),
    )
    .await;

    async fn tool_names(http: &reqwest::Client, base: &str, key: &str) -> Vec<String> {
        let (status, listed) = mcp_call(
            http,
            base,
            key,
            &serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
        )
        .await;
        assert_eq!(status, 200);
        listed["result"]["tools"]
            .as_array()
            .expect("tools list")
            .iter()
            .map(|tool| tool["name"].as_str().expect("tool name").to_string())
            .collect()
    }

    let names = tool_names(&http, &base, &viewer_secret).await;
    assert!(
        !names.contains(&"post_api_v5_publish".to_string()),
        "viewer hides publish"
    );
    assert!(!names.contains(&"subscribe_collect".to_string()));
    let (status, refused) = mcp_call(
        &http,
        &base,
        &viewer_secret,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "subscribe_collect",
                "arguments": {"topic_filter": "mc04/perm/#", "qos": 0}
            }
        }),
    )
    .await;
    assert_eq!(status, 200);
    assert!(refused.get("error").is_some(), "viewer cannot collect");

    let names = tool_names(&http, &base, &scoped_secret).await;
    assert!(
        !names.contains(&"post_api_v5_publish".to_string()),
        "scope gates publish"
    );
    assert!(
        !names.contains(&"subscribe_collect".to_string()),
        "scope gates the collect tool"
    );

    let names = tool_names(&http, &base, &publisher_secret).await;
    assert!(
        names.contains(&"post_api_v5_publish".to_string()),
        "publisher keeps publish"
    );
    assert!(names.contains(&"subscribe_collect".to_string()));
    kernel.finish().await;

    let read_only = spawn_kernel("readonly", "[mcp]\nread_only = true\n").await;
    let ro_base = format!("http://127.0.0.1:{}", read_only.api_port);
    let names = tool_names(&http, &ro_base, API_KEY).await;
    assert!(
        !names.contains(&"post_api_v5_publish".to_string()),
        "read-only mode hides publish"
    );
    assert!(
        !names.contains(&"subscribe_collect".to_string()),
        "read-only mode hides the collect tool"
    );
    let (status, refused) = mcp_call(
        &http,
        &ro_base,
        API_KEY,
        &serde_json::json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "post_api_v5_publish",
                       "arguments": {"body": {"topic": "mc04/ro/x"}}}
        }),
    )
    .await;
    assert_eq!(status, 200);
    let refused_read_only = refused.get("error").is_some()
        || refused
            .get("result")
            .and_then(|result| result.get("isError"))
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
    assert!(refused_read_only, "read-only mode refuses publish");
    read_only.finish().await;

    let disabled = spawn_kernel("disabled", "[mcp]\nenabled = false\n").await;
    let dis_base = format!("http://127.0.0.1:{}", disabled.api_port);
    let res = tokio::time::timeout(
        STEP,
        http.post(format!("{dis_base}/mcp"))
            .bearer_auth(API_KEY)
            .json(&serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "ping"}))
            .send(),
    )
    .await
    .expect("MCP in time")
    .expect("MCP send");
    assert_eq!(res.status(), 404, "disabled endpoint answers 404");
    disabled.finish().await;
}
