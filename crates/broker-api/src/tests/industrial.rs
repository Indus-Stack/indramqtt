use super::*;

/// Next MQTT client number for through-broker publishes.
#[cfg(test)]
static MV03C_MQTT_CLIENT_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Publish one JSON event through a real MQTT client socket. The
/// socket connects to the broker listener and sends QoS 0 on the
/// topic. The call returns after the send; the caller polls the
/// target fake or the action metrics for the delivery.
#[cfg(test)]
async fn mv03c_publish_mqtt(port: u16, topic: &str, payload: &str) {
    let id = MV03C_MQTT_CLIENT_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let client_id = format!("mv03c-pub-{id}");
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect mqtt listener");
    stream
        .write_all(&mqtt_connect(&client_id, None, None))
        .await
        .expect("write CONNECT");
    let mut connack = [0u8; 4];
    stream
        .read_exact(&mut connack)
        .await
        .expect("connack answers");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0, "anonymous connect succeeds");
    stream
        .write_all(&mqtt_publish(topic, 0, 0, payload.as_bytes()))
        .await
        .expect("write PUBLISH");
    let _ = stream.flush().await;
    let _ = stream.write_all(&[0xE0, 0x00]).await;
    let _ = stream.shutdown().await;
}

#[tokio::test]
async fn mv03c_http_delivers_updates_and_counts_a_dead_server_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv03c-http");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02bFake::start(200, "{}").await;
    let port = fake.port;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{port}");
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "http", "name": "mv03c-http", "url": endpoint}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "http", "name": "mv03c-http-act",
                       "connector": "mv03c-http",
                       "parameters": {"path": "/ingest/${topic}", "method": "post",
                                      "headers": {"X-Source": "mv03c"}}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv03c-http-rule", "name": "mv03c-http-rule",
                       "sql": "SELECT * FROM \"mv03c/http\"", "actions": ["mv03c-http-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    mv03c_publish_mqtt(server.mqtt_port, "mv03c/http", "{\"v\":21}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].method, "POST");
    assert!(
        got[0].path.contains("/ingest/mv03c/http"),
        "the action path joins the event topic: {}",
        got[0].path
    );
    assert!(
        Mv02bFake::body_text(&got[0]).contains("\"v\":21"),
        "the exact value 21 travels: {}",
        Mv02bFake::body_text(&got[0])
    );
    mv02b_wait_success(&server, &token, "mv03c-http-act", 1).await;
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv03c-http-act",
            json!({"type": "http", "name": "mv03c-http-act",
                       "connector": "mv03c-http",
                       "parameters": {"path": "/updated/${topic}", "method": "post"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv03c_publish_mqtt(server.mqtt_port, "mv03c/http", "{\"v\":22}").await;
    let got = fake.wait_for(2).await;
    assert!(
        got[1].path.contains("/updated/mv03c/http"),
        "the updated path travels: {}",
        got[1].path
    );
    mv02b_wait_success(&server, &token, "mv03c-http-act", 2).await;
    let (status, body) = server
        .put_auth(
            "/api/v5/actions/mv03c-http-act",
            json!({"type": "http", "name": "mv03c-http-act",
                       "connector": "mv03c-http",
                       "parameters": {"path": "/bad", "headers": {"X-Bad": 1}}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a non-string header refuses: {body}");
    mv03c_publish_mqtt(server.mqtt_port, "mv03c/http", "{\"v\":23}").await;
    let got = fake.wait_for(3).await;
    assert!(
        got[2].path.contains("/updated/mv03c/http"),
        "a refused update keeps the old path: {}",
        got[2].path
    );
    drop(fake);
    mv03c_publish_mqtt(server.mqtt_port, "mv03c/http", "{\"v\":24}").await;
    let mut failed = 0;
    for _ in 0..100 {
        let (status, metrics) = server
            .get_auth("/api/v5/actions/mv03c-http-act/metrics", &token)
            .await;
        assert_eq!(status, 200);
        failed = metrics["metrics"]["failed"].as_u64().unwrap_or(0);
        if failed >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(failed >= 1, "a dead HTTP target counts one failed forward");
    drop(server);
    drop(registry);
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    let token = body["token"].as_str().expect("admin token").to_string();
    let (status, _) = server
        .get_auth("/api/v5/connectors/mv03c-http", &token)
        .await;
    assert_eq!(status, 200, "stored entry survives the restart");
    let fake = Mv02bFake::start_on(port, 200, "{}").await;
    mv03c_publish_mqtt(server.mqtt_port, "mv03c/http", "{\"v\":25}").await;
    // Metrics are memory-only, so the restarted broker starts at
    // zero and this first delivery reads one success.
    mv02b_wait_success(&server, &token, "mv03c-http-act", 1).await;
    let got = fake.wait_for(1).await;
    assert!(Mv02bFake::body_text(&got[0]).contains("\"v\":25"));
    std::fs::remove_dir_all(&dir).ok();
}

#[cfg(test)]
fn mv03c_opc_write_u8(out: &mut Vec<u8>, v: u8) {
    out.push(v);
}

#[cfg(test)]
fn mv03c_opc_write_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

#[cfg(test)]
fn mv03c_opc_write_i32(out: &mut Vec<u8>, v: i32) {
    out.extend_from_slice(&v.to_le_bytes());
}

#[cfg(test)]
fn mv03c_opc_write_i64(out: &mut Vec<u8>, v: i64) {
    out.extend_from_slice(&v.to_le_bytes());
}

#[cfg(test)]
fn mv03c_opc_write_f64(out: &mut Vec<u8>, v: f64) {
    out.extend_from_slice(&v.to_le_bytes());
}

// Write a UA string. Null encodes as -1.
#[cfg(test)]
fn mv03c_opc_write_uastring(out: &mut Vec<u8>, s: Option<&str>) {
    match s {
        None => mv03c_opc_write_i32(out, -1),
        Some(text) => {
            mv03c_opc_write_i32(out, text.len() as i32);
            out.extend_from_slice(text.as_bytes());
        }
    }
}

// Write a byte string. Null encodes as -1.
#[cfg(test)]
fn mv03c_opc_write_bytestring(out: &mut Vec<u8>, b: Option<&[u8]>) {
    match b {
        None => mv03c_opc_write_i32(out, -1),
        Some(bytes) => {
            mv03c_opc_write_i32(out, bytes.len() as i32);
            out.extend_from_slice(bytes);
        }
    }
}

// Write a two-byte node id.
#[cfg(test)]
fn mv03c_opc_write_nodeid_two(out: &mut Vec<u8>, id: u8) {
    mv03c_opc_write_u8(out, 0x00);
    mv03c_opc_write_u8(out, id);
}

// Write a four-byte numeric node id.
#[cfg(test)]
fn mv03c_opc_write_nodeid_four(out: &mut Vec<u8>, ns: u8, id: u16) {
    mv03c_opc_write_u8(out, 0x01);
    mv03c_opc_write_u8(out, ns);
    out.extend_from_slice(&id.to_le_bytes());
}

// Map wall time to OPC-UA ticks. The offset counts 100ns ticks.
#[cfg(test)]
fn mv03c_opc_now_ticks() -> i64 {
    // Offset from 1601 to 1970 in 100ns ticks. Reason: OPC-UA epoch.
    #[cfg(test)]
    const OFFSET: i64 = 116444736000000000;
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    millis * 10000 + OFFSET
}

// Write a good response header. It echoes the request handle.
#[cfg(test)]
fn mv03c_opc_write_response_header(out: &mut Vec<u8>, request_handle: u32, ticks: i64) {
    mv03c_opc_write_i64(out, ticks);
    mv03c_opc_write_u32(out, request_handle);
    mv03c_opc_write_u32(out, 0);
    mv03c_opc_write_u8(out, 0x00);
    mv03c_opc_write_i32(out, -1);
    mv03c_opc_write_nodeid_two(out, 0);
    mv03c_opc_write_u8(out, 0x00);
}

// Read a little-endian u32. Return none when short.
#[cfg(test)]
fn mv03c_opc_read_u32(buf: &[u8], off: usize) -> Option<u32> {
    let bytes: [u8; 4] = buf.get(off..off + 4)?.try_into().ok()?;
    Some(u32::from_le_bytes(bytes))
}

// Read a little-endian i32. Return none when short.
#[cfg(test)]
fn mv03c_opc_read_i32(buf: &[u8], off: usize) -> Option<i32> {
    let bytes: [u8; 4] = buf.get(off..off + 4)?.try_into().ok()?;
    Some(i32::from_le_bytes(bytes))
}

// Parse a UA string. Return the total bytes and the body.
#[cfg(test)]
fn mv03c_opc_parse_uastring(buf: &[u8], off: usize) -> Option<(usize, Option<Vec<u8>>)> {
    let len = mv03c_opc_read_i32(buf, off)?;
    if len == -1 {
        return Some((4, None));
    }
    if len < 0 {
        return None;
    }
    let len = len as usize;
    let body = buf.get(off + 4..off + 4 + len)?.to_vec();
    Some((4 + len, Some(body)))
}

// Parse a node id. Return the bytes used and the display text.
#[cfg(test)]
fn mv03c_opc_parse_nodeid(buf: &[u8], off: usize) -> Option<(usize, String)> {
    let enc = *buf.get(off)?;
    match enc {
        0x00 => {
            let id = *buf.get(off + 1)?;
            Some((2, format!("ns=0;i={id}")))
        }
        0x01 => {
            let ns = *buf.get(off + 1)?;
            let raw: [u8; 2] = buf.get(off + 2..off + 4)?.try_into().ok()?;
            let id = u16::from_le_bytes(raw);
            Some((4, format!("ns={ns};i={id}")))
        }
        0x02 => {
            let ns_raw: [u8; 2] = buf.get(off + 1..off + 3)?.try_into().ok()?;
            let ns = u16::from_le_bytes(ns_raw);
            let id = mv03c_opc_read_u32(buf, off + 3)?;
            Some((7, format!("ns={ns};i={id}")))
        }
        0x03 => {
            let ns_raw: [u8; 2] = buf.get(off + 1..off + 3)?.try_into().ok()?;
            let ns = u16::from_le_bytes(ns_raw);
            let (used, body) = mv03c_opc_parse_uastring(buf, off + 3)?;
            let text = body
                .map(|b| String::from_utf8_lossy(&b).to_string())
                .unwrap_or_default();
            Some((1 + 2 + used, format!("ns={ns};s={text}")))
        }
        0x04 => {
            let ns_raw: [u8; 2] = buf.get(off + 1..off + 3)?.try_into().ok()?;
            let ns = u16::from_le_bytes(ns_raw);
            buf.get(off + 3..off + 19)?;
            Some((19, format!("ns={ns};g=opaque")))
        }
        0x05 => {
            let ns_raw: [u8; 2] = buf.get(off + 1..off + 3)?.try_into().ok()?;
            let ns = u16::from_le_bytes(ns_raw);
            let (used, _) = mv03c_opc_parse_uastring(buf, off + 3)?;
            Some((1 + 2 + used, format!("ns={ns};b=opaque")))
        }
        _ => None,
    }
}

// Read the request handle from a request header at the offset.
#[cfg(test)]
fn mv03c_opc_parse_request_handle(buf: &[u8], off: usize) -> Option<u32> {
    let (used, _) = mv03c_opc_parse_nodeid(buf, off)?;
    mv03c_opc_read_u32(buf, off + used + 8)
}

// Measure a request header. Return the total bytes used.
#[cfg(test)]
fn mv03c_opc_request_header_len(buf: &[u8], off: usize) -> Option<usize> {
    let (mut used, _) = mv03c_opc_parse_nodeid(buf, off)?;
    used += 8;
    used += 4;
    used += 4;
    let (audit_used, _) = mv03c_opc_parse_uastring(buf, off + used)?;
    used += audit_used;
    used += 4;
    let (node_used, _) = mv03c_opc_parse_nodeid(buf, off + used)?;
    used += node_used;
    let mask = *buf.get(off + used)?;
    used += 1;
    if mask != 0x00 {
        return None;
    }
    Some(used)
}

// Write the minimal application description. All names stay null.
#[cfg(test)]
fn mv03c_opc_write_app_description(out: &mut Vec<u8>) {
    mv03c_opc_write_uastring(out, None);
    mv03c_opc_write_uastring(out, None);
    mv03c_opc_write_u8(out, 0x00);
    mv03c_opc_write_u32(out, 0);
    mv03c_opc_write_uastring(out, None);
    mv03c_opc_write_uastring(out, None);
    mv03c_opc_write_i32(out, -1);
}

// Write one endpoint with the None policy and mode.
#[cfg(test)]
fn mv03c_opc_write_endpoint(out: &mut Vec<u8>, endpoint_url: &str) {
    // None policy URI. Reason: OPC-UA Part 7 policy id.
    #[cfg(test)]
    const NONE_POLICY: &str = "http://opcfoundation.org/UA/SecurityPolicy#None";
    // Binary transport URI. Reason: OPC-UA Part 6 transport id.
    #[cfg(test)]
    const BINARY_TRANSPORT: &str =
        "http://opcfoundation.org/UA-Profile/Transport/uatcp-uasc-uabinary";
    mv03c_opc_write_uastring(out, Some(endpoint_url));
    mv03c_opc_write_app_description(out);
    mv03c_opc_write_bytestring(out, None);
    // MessageSecurityMode::None is 1. Reason: OPC-UA Part 6 enum.
    mv03c_opc_write_u32(out, 1);
    mv03c_opc_write_uastring(out, Some(NONE_POLICY));
    // One anonymous user token policy. Reason: the driver refuses
    // to activate a session without a matching policy.
    mv03c_opc_write_i32(out, 1);
    mv03c_opc_write_uastring(out, None);
    mv03c_opc_write_i32(out, 0);
    mv03c_opc_write_uastring(out, None);
    mv03c_opc_write_uastring(out, None);
    mv03c_opc_write_uastring(out, None);
    mv03c_opc_write_uastring(out, Some(BINARY_TRANSPORT));
    mv03c_opc_write_u8(out, 0);
}

// Write the asymmetric header for the None policy. No cert travels.
#[cfg(test)]
fn mv03c_opc_write_asymmetric_none(out: &mut Vec<u8>) {
    #[cfg(test)]
    const NONE_POLICY: &str = "http://opcfoundation.org/UA/SecurityPolicy#None";
    mv03c_opc_write_uastring(out, Some(NONE_POLICY));
    mv03c_opc_write_bytestring(out, None);
    mv03c_opc_write_bytestring(out, None);
}

// Build the OPN inner body. Caller adds the frame header.
#[cfg(test)]
fn mv03c_opc_build_open_response(request_handle: u32) -> Vec<u8> {
    let mut body = Vec::new();
    mv03c_opc_write_response_header(&mut body, request_handle, mv03c_opc_now_ticks());
    mv03c_opc_write_u32(&mut body, 0);
    mv03c_opc_write_u32(&mut body, 1);
    mv03c_opc_write_u32(&mut body, 1);
    mv03c_opc_write_i64(&mut body, mv03c_opc_now_ticks());
    // Lifetime 1 hour. Reason: driver default channel lifetime.
    #[cfg(test)]
    const LIFETIME: u32 = 3600000;
    mv03c_opc_write_u32(&mut body, LIFETIME);
    mv03c_opc_write_bytestring(&mut body, None);
    body
}

// Build a GetEndpoints response with one None endpoint.
#[cfg(test)]
fn mv03c_opc_build_endpoints_response(request_handle: u32, endpoint_url: &str) -> Vec<u8> {
    let mut inner = Vec::new();
    mv03c_opc_write_response_header(&mut inner, request_handle, mv03c_opc_now_ticks());
    mv03c_opc_write_i32(&mut inner, 1);
    mv03c_opc_write_endpoint(&mut inner, endpoint_url);
    inner
}

// Build a CreateSession response with fixed ids.
#[cfg(test)]
fn mv03c_opc_build_create_response(request_handle: u32) -> Vec<u8> {
    let mut inner = Vec::new();
    mv03c_opc_write_response_header(&mut inner, request_handle, mv03c_opc_now_ticks());
    mv03c_opc_write_nodeid_four(&mut inner, 1, 9999);
    mv03c_opc_write_nodeid_four(&mut inner, 1, 8888);
    mv03c_opc_write_f64(&mut inner, 60000.0);
    mv03c_opc_write_bytestring(&mut inner, None);
    mv03c_opc_write_bytestring(&mut inner, None);
    mv03c_opc_write_i32(&mut inner, -1);
    mv03c_opc_write_i32(&mut inner, -1);
    mv03c_opc_write_uastring(&mut inner, None);
    mv03c_opc_write_bytestring(&mut inner, None);
    mv03c_opc_write_u32(&mut inner, 4194304);
    inner
}

// Build an ActivateSession response with one good result.
#[cfg(test)]
fn mv03c_opc_build_activate_response(request_handle: u32) -> Vec<u8> {
    let mut inner = Vec::new();
    mv03c_opc_write_response_header(&mut inner, request_handle, mv03c_opc_now_ticks());
    mv03c_opc_write_bytestring(&mut inner, None);
    mv03c_opc_write_i32(&mut inner, 1);
    mv03c_opc_write_u32(&mut inner, 0);
    mv03c_opc_write_i32(&mut inner, -1);
    inner
}

// Build a Write response with one good result.
#[cfg(test)]
fn mv03c_opc_build_write_response(request_handle: u32) -> Vec<u8> {
    let mut inner = Vec::new();
    mv03c_opc_write_response_header(&mut inner, request_handle, mv03c_opc_now_ticks());
    mv03c_opc_write_i32(&mut inner, 1);
    mv03c_opc_write_u32(&mut inner, 0);
    mv03c_opc_write_i32(&mut inner, -1);
    inner
}

// Build a Read response with one zero Int32 value. Reason: the
// driver reads ServerState, an Int32 enum, for its keepalive.
#[cfg(test)]
fn mv03c_opc_build_read_response(request_handle: u32) -> Vec<u8> {
    let mut inner = Vec::new();
    mv03c_opc_write_response_header(&mut inner, request_handle, mv03c_opc_now_ticks());
    mv03c_opc_write_i32(&mut inner, 1);
    mv03c_opc_write_u8(&mut inner, 0x01);
    mv03c_opc_write_u8(&mut inner, 0x06);
    mv03c_opc_write_u32(&mut inner, 0);
    mv03c_opc_write_i32(&mut inner, -1);
    inner
}

// Build a CloseSession response with a good header.
#[cfg(test)]
fn mv03c_opc_build_close_response(request_handle: u32) -> Vec<u8> {
    let mut inner = Vec::new();
    mv03c_opc_write_response_header(&mut inner, request_handle, mv03c_opc_now_ticks());
    inner
}

// Send one MSG response. It echoes the client request id.
#[cfg(test)]
async fn mv03c_opc_send_msg(
    stream: &mut tokio::net::TcpStream,
    channel_id: u32,
    token_id: u32,
    server_seq: &mut u32,
    request_id: u32,
    type_id: u16,
    inner: &[u8],
) -> bool {
    use tokio::io::AsyncWriteExt;
    let mut body = Vec::new();
    mv03c_opc_write_u32(&mut body, token_id);
    mv03c_opc_write_u32(&mut body, *server_seq);
    mv03c_opc_write_u32(&mut body, request_id);
    mv03c_opc_write_nodeid_four(&mut body, 0, type_id);
    body.extend_from_slice(inner);
    *server_seq = server_seq.wrapping_add(1);
    let total = (12 + body.len()) as u32;
    let mut frame = Vec::with_capacity(12 + body.len());
    frame.extend_from_slice(b"MSG");
    frame.push(b'F');
    frame.extend_from_slice(&total.to_le_bytes());
    frame.extend_from_slice(&channel_id.to_le_bytes());
    frame.extend_from_slice(&body);
    stream.write_all(&frame).await.is_ok()
}

// Handle one driver TCP connection. It answers discovery and session.
#[cfg(test)]
async fn mv03c_opc_handle_one_driver_connection(
    stream: &mut tokio::net::TcpStream,
    received: &std::sync::Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    endpoint_url: &str,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut header = [0u8; 8];
    if stream.read_exact(&mut header).await.is_err() {
        return;
    }
    if &header[..3] != b"HEL" {
        return;
    }
    let length = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;
    if !(8..=65536).contains(&length) {
        return;
    }
    let mut body = vec![0u8; length - 8];
    if stream.read_exact(&mut body).await.is_err() {
        return;
    }
    // The ACK body is exactly the five negotiated u32 values. The
    // HEL body continues with the endpoint URL, which must not
    // travel in the ACK.
    let ack_body: &[u8] = if body.len() >= 20 {
        &body[..20]
    } else {
        &body[..]
    };
    let mut ack = b"ACKF".to_vec();
    let total = (8 + ack_body.len()) as u32;
    ack.extend_from_slice(&total.to_le_bytes());
    ack.extend_from_slice(ack_body);
    if stream.write_all(&ack).await.is_err() {
        return;
    }
    let mut server_seq: u32 = 1;
    loop {
        let mut head = [0u8; 8];
        if stream.read_exact(&mut head).await.is_err() {
            return;
        }
        let chunk_len = u32::from_le_bytes([head[4], head[5], head[6], head[7]]) as usize;
        if !(12..=65536).contains(&chunk_len) {
            return;
        }
        let mut rest = vec![0u8; chunk_len - 8];
        if stream.read_exact(&mut rest).await.is_err() {
            return;
        }
        if &head[..3] == b"CLO" {
            return;
        }
        if &head[..3] != b"OPN" && &head[..3] != b"MSG" {
            return;
        }
        let is_open = &head[..3] == b"OPN";
        if rest.len() < 4 {
            return;
        }
        let channel_id = u32::from_le_bytes([rest[0], rest[1], rest[2], rest[3]]);
        let _ = channel_id;
        if is_open {
            let mut off = 4;
            let (used, _) = match mv03c_opc_parse_uastring(&rest, off) {
                Some(v) => v,
                None => return,
            };
            off += used;
            let (used, _) = match mv03c_opc_parse_uastring(&rest, off) {
                Some(v) => v,
                None => return,
            };
            off += used;
            let (used, _) = match mv03c_opc_parse_uastring(&rest, off) {
                Some(v) => v,
                None => return,
            };
            off += used;
            let seq = match mv03c_opc_read_u32(&rest, off) {
                Some(v) => v,
                None => return,
            };
            let _ = seq;
            let request_id = match mv03c_opc_read_u32(&rest, off + 4) {
                Some(v) => v,
                None => return,
            };
            let type_off = off + 8;
            let (type_used, type_text) = match mv03c_opc_parse_nodeid(&rest, type_off) {
                Some(v) => v,
                None => return,
            };
            let _ = type_used;
            let _ = type_text;
            let handle = mv03c_opc_parse_request_handle(&rest, type_off + type_used).unwrap_or(1);
            let inner = mv03c_opc_build_open_response(handle);
            let mut frame_body = Vec::new();
            mv03c_opc_write_asymmetric_none(&mut frame_body);
            mv03c_opc_write_u32(&mut frame_body, server_seq);
            mv03c_opc_write_u32(&mut frame_body, request_id);
            mv03c_opc_write_nodeid_four(&mut frame_body, 0, 449);
            frame_body.extend_from_slice(&inner);
            server_seq = server_seq.wrapping_add(1);
            let total = (12 + frame_body.len()) as u32;
            let mut frame = Vec::with_capacity(12 + frame_body.len());
            frame.extend_from_slice(b"OPN");
            frame.push(b'F');
            frame.extend_from_slice(&total.to_le_bytes());
            frame.extend_from_slice(&1u32.to_le_bytes());
            frame.extend_from_slice(&frame_body);
            if stream.write_all(&frame).await.is_err() {
                return;
            }
            continue;
        }
        if rest.len() < 16 {
            return;
        }
        let token_id = mv03c_opc_read_u32(&rest, 4).unwrap_or(1);
        let request_id = mv03c_opc_read_u32(&rest, 12).unwrap_or(1);
        let (type_used, type_text) = match mv03c_opc_parse_nodeid(&rest, 16) {
            Some(v) => v,
            None => return,
        };
        let req_off = 16 + type_used;
        let handle = mv03c_opc_parse_request_handle(&rest, req_off).unwrap_or(1);
        if type_text == "ns=0;i=428" {
            let inner = mv03c_opc_build_endpoints_response(handle, endpoint_url);
            if !mv03c_opc_send_msg(
                stream,
                1,
                token_id,
                &mut server_seq,
                request_id,
                431,
                &inner,
            )
            .await
            {
                return;
            }
            continue;
        }
        if type_text == "ns=0;i=461" {
            let inner = mv03c_opc_build_create_response(handle);
            if !mv03c_opc_send_msg(
                stream,
                1,
                token_id,
                &mut server_seq,
                request_id,
                464,
                &inner,
            )
            .await
            {
                return;
            }
            continue;
        }
        if type_text == "ns=0;i=467" {
            let inner = mv03c_opc_build_activate_response(handle);
            if !mv03c_opc_send_msg(
                stream,
                1,
                token_id,
                &mut server_seq,
                request_id,
                470,
                &inner,
            )
            .await
            {
                return;
            }
            continue;
        }
        if type_text == "ns=0;i=673" {
            let header_len = match mv03c_opc_request_header_len(&rest, req_off) {
                Some(v) => v,
                None => return,
            };
            let mut woff = req_off + header_len;
            let count = mv03c_opc_read_i32(&rest, woff).unwrap_or(0);
            woff += 4;
            if count >= 1 {
                if let Some((used, display)) = mv03c_opc_parse_nodeid(&rest, woff) {
                    let _ = used;
                    received
                        .lock()
                        .expect("fake lock")
                        .push(display.into_bytes());
                }
            }
            let inner = mv03c_opc_build_write_response(handle);
            if !mv03c_opc_send_msg(
                stream,
                1,
                token_id,
                &mut server_seq,
                request_id,
                676,
                &inner,
            )
            .await
            {
                return;
            }
            continue;
        }
        if type_text == "ns=0;i=631" {
            let inner = mv03c_opc_build_read_response(handle);
            if !mv03c_opc_send_msg(
                stream,
                1,
                token_id,
                &mut server_seq,
                request_id,
                634,
                &inner,
            )
            .await
            {
                return;
            }
            continue;
        }
        if type_text == "ns=0;i=473" {
            let inner = mv03c_opc_build_close_response(handle);
            if !mv03c_opc_send_msg(
                stream,
                1,
                token_id,
                &mut server_seq,
                request_id,
                476,
                &inner,
            )
            .await
            {
                return;
            }
            continue;
        }
        return;
    }
}

// Run the driver fake. It serves each connection in its own task,
// so a new action generation can connect while an old one is open.
#[cfg(test)]
async fn mv03c_opc_run_driver_fake(
    listener: tokio::net::TcpListener,
    received: std::sync::Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    endpoint_url: String,
    connections: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
) {
    loop {
        let accepted = listener.accept().await;
        let Ok((mut stream, _)) = accepted else {
            break;
        };
        let received = std::sync::Arc::clone(&received);
        let endpoint_url = endpoint_url.clone();
        let handle = tokio::spawn(async move {
            mv03c_opc_handle_one_driver_connection(&mut stream, &received, &endpoint_url).await;
        });
        connections.lock().expect("fake lock").push(handle);
    }
}

#[tokio::test]
async fn mv03c_opc_ua_delivers_updates_and_counts_a_dead_server_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv03c-opcua");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind opc fake");
    let port = listener.local_addr().expect("fake addr").port();
    let received: std::sync::Arc<std::sync::Mutex<Vec<Vec<u8>>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let connections: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let endpoint_for_fake = format!("opc.tcp://127.0.0.1:{port}");
    let task = tokio::spawn(mv03c_opc_run_driver_fake(
        listener,
        std::sync::Arc::clone(&received),
        endpoint_for_fake,
        std::sync::Arc::clone(&connections),
    ));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("opc.tcp://127.0.0.1:{port}");
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "opc_ua", "name": "mv03c-opc", "endpoint_url": endpoint}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "opc_ua", "name": "mv03c-opc-act",
                       "connector": "mv03c-opc",
                       "parameters": {"node_id": "ns=1;i=1001"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv03c-opc-rule", "name": "mv03c-opc-rule",
                       "sql": "SELECT * FROM \"mv03c/opc\"", "actions": ["mv03c-opc-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    mv03c_publish_mqtt(server.mqtt_port, "mv03c/opc", "{\"v\":21}").await;
    for _ in 0..100 {
        if !received.lock().expect("fake lock").is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    {
        let got = received.lock().expect("fake lock").clone();
        assert_eq!(got.len(), 1, "one OPN write reaches the fake");
        assert!(
            got[0].windows(11).any(|w| w == b"ns=1;i=1001"),
            "the exact node id travels on the wire"
        );
    }
    mv02b_wait_success(&server, &token, "mv03c-opc-act", 1).await;
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv03c-opc-act",
            json!({"type": "opc_ua", "name": "mv03c-opc-act",
                       "connector": "mv03c-opc",
                       "parameters": {"node_id": "ns=1;i=1002"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv03c_publish_mqtt(server.mqtt_port, "mv03c/opc", "{\"v\":22}").await;
    for _ in 0..100 {
        if received.lock().expect("fake lock").len() >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    {
        let got = received.lock().expect("fake lock").clone();
        assert!(
            got[1].windows(11).any(|w| w == b"ns=1;i=1002"),
            "the updated node id travels on the wire"
        );
    }
    let (status, body) = server
        .put_auth(
            "/api/v5/actions/mv03c-opc-act",
            json!({"type": "opc_ua", "name": "mv03c-opc-act",
                       "connector": "mv03c-opc",
                       "parameters": {}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a missing node id refuses: {body}");
    task.abort();
    let _ = task.await;
    for handle in connections.lock().expect("fake lock").drain(..) {
        handle.abort();
    }
    // Let the driver observe the closed sockets before the next send.
    // Reason: a closed socket becomes visible at the next poll.
    tokio::time::sleep(std::time::Duration::from_millis(DEAD_SOCKET_SETTLE_MS)).await;
    mv03c_publish_mqtt(server.mqtt_port, "mv03c/opc", "{\"v\":23}").await;
    let mut failed = 0;
    for _ in 0..100 {
        let (status, metrics) = server
            .get_auth("/api/v5/actions/mv03c-opc-act/metrics", &token)
            .await;
        assert_eq!(status, 200);
        failed = metrics["metrics"]["failed"].as_u64().unwrap_or(0);
        if failed >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(failed >= 1, "a dead OPC server counts one failed forward");
    // The first failure sets a 2 s backoff window. Wait past it so the
    // next event retries the restored row instead of meeting the cap.
    #[cfg(test)]
    const OPC_UA_FIRST_BACKOFF_MS: u64 = 2_100;
    tokio::time::sleep(std::time::Duration::from_millis(OPC_UA_FIRST_BACKOFF_MS)).await;
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .expect("rebind opc fake");
    let endpoint_for_fake = format!("opc.tcp://127.0.0.1:{port}");
    let task = tokio::spawn(mv03c_opc_run_driver_fake(
        listener,
        std::sync::Arc::clone(&received),
        endpoint_for_fake,
        std::sync::Arc::clone(&connections),
    ));
    mv03c_publish_mqtt(server.mqtt_port, "mv03c/opc", "{\"v\":24}").await;
    for _ in 0..100 {
        if received.lock().expect("fake lock").len() >= 3 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    {
        let got = received.lock().expect("fake lock").clone();
        assert!(
            got.len() >= 3,
            "one OPN write reaches the fake after rebind"
        );
        assert!(
            got[2].windows(11).any(|w| w == b"ns=1;i=1002"),
            "delivery resumes after the server returns"
        );
    }
    mv02b_wait_success(&server, &token, "mv03c-opc-act", 3).await;
    task.abort();
    let _ = task.await;
    for handle in connections.lock().expect("fake lock").drain(..) {
        handle.abort();
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv03c_sparkplug_b_delivers_updates_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    #[cfg(test)]
    async fn read_packet(stream: &mut tokio::net::TcpStream) -> Option<(u8, Vec<u8>)> {
        let mut first = [0u8; 1];
        if stream.read_exact(&mut first).await.is_err() {
            return None;
        }
        let mut multiplier = 1usize;
        let mut length = 0usize;
        loop {
            let mut byte = [0u8; 1];
            if stream.read_exact(&mut byte).await.is_err() {
                return None;
            }
            length += ((byte[0] & 0x7F) as usize) * multiplier;
            multiplier *= 128;
            if byte[0] & 0x80 == 0 {
                break;
            }
            if multiplier > 128 * 128 * 128 {
                return None;
            }
        }
        let mut rest = vec![0u8; length];
        if stream.read_exact(&mut rest).await.is_err() {
            return None;
        }
        Some((first[0], rest))
    }
    let dir = unique_data_dir("mv03c-spb");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind sparkplug fake");
    let port = listener.local_addr().expect("fake addr").port();
    #[cfg(test)]
    type Mv03cSparkplugReceived = std::sync::Arc<std::sync::Mutex<Vec<(String, Vec<u8>)>>>;
    let received: Mv03cSparkplugReceived = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let task_received = std::sync::Arc::clone(&received);
    // Track per-connection tasks. Abort kills them with the accept
    // loop, so the dead-server leg observes a closed port.
    let task_conns: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let task_conns_accept = std::sync::Arc::clone(&task_conns);
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let task_received = std::sync::Arc::clone(&task_received);
            let handle = tokio::spawn(async move {
                let Some((kind, rest)) = read_packet(&mut stream).await else {
                    return;
                };
                assert_eq!(kind & 0xF0, 0x10, "first packet is CONNECT");
                assert!(
                    rest.windows(9).any(|w| w == b"mv03c-spb"),
                    "the CONNECT carries the client id"
                );
                let _ = stream.write_all(&[0x20, 0x02, 0x00, 0x00]).await;
                loop {
                    let Some((kind, rest)) = read_packet(&mut stream).await else {
                        return;
                    };
                    match kind & 0xF0 {
                        0x30 => {
                            if rest.len() < 2 {
                                return;
                            }
                            let topic_len = u16::from_be_bytes([rest[0], rest[1]]) as usize;
                            if rest.len() < 2 + topic_len + 2 {
                                return;
                            }
                            let topic =
                                String::from_utf8_lossy(&rest[2..2 + topic_len]).to_string();
                            let packet_id = [rest[2 + topic_len], rest[3 + topic_len]];
                            let payload = rest[4 + topic_len..].to_vec();
                            task_received
                                .lock()
                                .expect("fake lock")
                                .push((topic, payload));
                            let _ = stream
                                .write_all(&[0x40, 0x02, packet_id[0], packet_id[1]])
                                .await;
                        }
                        0xC0 => {
                            let _ = stream.write_all(&[0xD0, 0x00]).await;
                        }
                        _ => return,
                    }
                }
            });
            task_conns_accept.lock().expect("fake lock").push(handle);
        }
    });
    let (server, state1) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let mqtt_url = format!("mqtt://127.0.0.1:{port}");
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "sparkplug_b", "name": "mv03c-spb",
                       "mqtt_url": mqtt_url, "client_id": "mv03c-spb"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "sparkplug_b", "name": "mv03c-spb-act",
                       "connector": "mv03c-spb",
                       "parameters": {"topic_prefix": "spBv1.0/plant1"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv03c-spb-rule", "name": "mv03c-spb-rule",
                       "sql": "SELECT * FROM \"spBv1.0/plant1/NDATA/edge1\"",
                       "actions": ["mv03c-spb-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv03c-spb-birth", "name": "mv03c-spb-birth",
                       "sql": "SELECT * FROM \"spBv1.0/plant1/NBIRTH/edge1\"",
                       "actions": ["mv03c-spb-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv03c-spb-death", "name": "mv03c-spb-death",
                       "sql": "SELECT * FROM \"spBv1.0/plant1/NDEATH/edge1\"",
                       "actions": ["mv03c-spb-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    for topic in [
        "spBv1.0/plant1/NDATA/edge1",
        "spBv1.0/plant1/NBIRTH/edge1",
        "spBv1.0/plant1/NDEATH/edge1",
    ] {
        mv03c_publish_mqtt(server.mqtt_port, topic, "{\"metrics\":{\"temp\":21}}").await;
    }
    for _ in 0..100 {
        if received.lock().expect("fake lock").len() >= 3 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    {
        let got = received.lock().expect("fake lock").clone();
        assert_eq!(got.len(), 3, "birth, data and death reach the fake");
        assert_eq!(got[0].0, "spBv1.0/plant1/NDATA/edge1");
        let decoded = broker_connectors_enterprise::sparkplug_b::decode_payload(&got[0].1)
            .expect("the wire payload decodes");
        let metric = decoded
            .metrics
            .iter()
            .find(|m| m.name.as_deref() == Some("temp"))
            .expect("the temp metric travels");
        assert_eq!(
            format!("{:?}", metric.value),
            format!(
                "{:?}",
                broker_connectors_enterprise::sparkplug_b::SpbValue::Int(21)
            ),
            "the exact value 21 travels"
        );
        assert_eq!(got[1].0, "spBv1.0/plant1/NBIRTH/edge1");
        assert_eq!(got[2].0, "spBv1.0/plant1/NDEATH/edge1");
    }
    mv02b_wait_success(&server, &token, "mv03c-spb-act", 3).await;
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv03c-spb-act",
            json!({"type": "sparkplug_b", "name": "mv03c-spb-act",
                       "connector": "mv03c-spb",
                       "parameters": {"topic_prefix": "spBv1.0/plant1/NDATA"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv03c_publish_mqtt(
        server.mqtt_port,
        "spBv1.0/plant1/NDATA/edge1",
        "{\"metrics\":{\"temp\":22}}",
    )
    .await;
    for _ in 0..100 {
        if received.lock().expect("fake lock").len() >= 4 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    {
        let got = received.lock().expect("fake lock").clone();
        assert_eq!(got[3].0, "spBv1.0/plant1/NDATA/edge1");
        let decoded = broker_connectors_enterprise::sparkplug_b::decode_payload(&got[3].1)
            .expect("the updated wire payload decodes");
        assert!(
            decoded
                .metrics
                .iter()
                .any(|m| m.name.as_deref() == Some("temp")),
            "the updated temp metric travels"
        );
    }
    mv02b_wait_success(&server, &token, "mv03c-spb-act", 4).await;
    let (status, body) = server
        .put_auth(
            "/api/v5/actions/mv03c-spb-act",
            json!({"type": "sparkplug_b", "name": "mv03c-spb-act",
                       "connector": "mv03c-spb",
                       "parameters": {"topic_prefix": "spBv1.0/a/#/x"}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a wildcard prefix refuses: {body}");
    mv03c_publish_mqtt(
        server.mqtt_port,
        "spBv1.0/plant1/NDATA/edge1",
        "{\"metrics\":{\"temp\":23}}",
    )
    .await;
    for _ in 0..100 {
        if received.lock().expect("fake lock").len() >= 5 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(
        received.lock().expect("fake lock").len(),
        5,
        "a refused update keeps the old prefix"
    );
    mv02b_wait_success(&server, &token, "mv03c-spb-act", 5).await;
    task.abort();
    for handle in task_conns.lock().expect("fake lock").drain(..) {
        handle.abort();
    }
    // Let the driver's event loop observe the closed socket before
    // the next send, so the dead-target failure is deterministic.
    // Reason: a closed socket becomes visible at the next poll.
    tokio::time::sleep(std::time::Duration::from_millis(DEAD_SOCKET_SETTLE_MS)).await;
    mv03c_publish_mqtt(
        server.mqtt_port,
        "spBv1.0/plant1/NDATA/edge1",
        "{\"metrics\":{\"temp\":24}}",
    )
    .await;
    let mut failed = 0;
    for _ in 0..100 {
        let (status, metrics) = server
            .get_auth("/api/v5/actions/mv03c-spb-act/metrics", &token)
            .await;
        assert_eq!(status, 200);
        failed = metrics["metrics"]["failed"].as_u64().unwrap_or(0);
        if failed >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(failed >= 1, "a dead MQTT target counts one failed forward");
    // Stop the old server and its connector pump before the rebind,
    // so the restarted broker is the only client on the port.
    drop(server);
    drop(state1);
    drop(registry);
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    let token = body["token"].as_str().expect("admin token").to_string();
    let (status, _) = server
        .get_auth("/api/v5/connectors/mv03c-spb", &token)
        .await;
    assert_eq!(status, 200, "stored entry survives the restart");
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .expect("rebind sparkplug fake");
    let task_received = std::sync::Arc::clone(&received);
    let connect_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let task_connect_count = std::sync::Arc::clone(&connect_count);
    let restarted_conns: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let restarted_conns_accept = std::sync::Arc::clone(&restarted_conns);
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let task_received = std::sync::Arc::clone(&task_received);
            let task_connect_count = std::sync::Arc::clone(&task_connect_count);
            let handle = tokio::spawn(async move {
                let Some((kind, rest)) = read_packet(&mut stream).await else {
                    return;
                };
                if kind & 0xF0 == 0x10 {
                    task_connect_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                assert_eq!(kind & 0xF0, 0x10, "first packet is CONNECT");
                assert!(
                    rest.windows(9).any(|w| w == b"mv03c-spb"),
                    "the CONNECT carries the client id"
                );
                let _ = stream.write_all(&[0x20, 0x02, 0x00, 0x00]).await;
                loop {
                    let Some((kind, rest)) = read_packet(&mut stream).await else {
                        return;
                    };
                    match kind & 0xF0 {
                        0x30 => {
                            if rest.len() < 2 {
                                return;
                            }
                            let topic_len = u16::from_be_bytes([rest[0], rest[1]]) as usize;
                            if rest.len() < 2 + topic_len + 2 {
                                return;
                            }
                            let topic =
                                String::from_utf8_lossy(&rest[2..2 + topic_len]).to_string();
                            let packet_id = [rest[2 + topic_len], rest[3 + topic_len]];
                            let payload = rest[4 + topic_len..].to_vec();
                            task_received
                                .lock()
                                .expect("fake lock")
                                .push((topic, payload));
                            let _ = stream
                                .write_all(&[0x40, 0x02, packet_id[0], packet_id[1]])
                                .await;
                        }
                        0xC0 => {
                            let _ = stream.write_all(&[0xD0, 0x00]).await;
                        }
                        _ => return,
                    }
                }
            });
            restarted_conns_accept
                .lock()
                .expect("fake lock")
                .push(handle);
        }
    });
    mv03c_publish_mqtt(
        server.mqtt_port,
        "spBv1.0/plant1/NDATA/edge1",
        "{\"metrics\":{\"temp\":25}}",
    )
    .await;
    for _ in 0..100 {
        if received.lock().expect("fake lock").len() >= 6 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    {
        let got = received.lock().expect("fake lock").clone();
        assert!(got.len() >= 6, "delivery resumes after restart");
        assert_eq!(got[5].0, "spBv1.0/plant1/NDATA/edge1");
    }
    // Metrics are memory-only, so the restarted broker starts at
    // zero and this first delivery reads one success.
    mv02b_wait_success(&server, &token, "mv03c-spb-act", 1).await;
    // Five pump intervals of 100 ms to confirm no second connection arrives.
    const PUMP_SETTLE_MS: u64 = 500;
    tokio::time::sleep(std::time::Duration::from_millis(PUMP_SETTLE_MS)).await;
    assert_eq!(
        connect_count.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "only the restarted broker connects after the restart"
    );
    task.abort();
    for handle in restarted_conns.lock().expect("fake lock").drain(..) {
        handle.abort();
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv03c_console_logger_delivers_and_test_kind_follows_d9_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv03c-local");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "console", "name": "mv03c-console"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "logger", "name": "mv03c-logger"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "console", "name": "mv03c-console-act",
                       "connector": "mv03c-console"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv03c-console-rule", "name": "mv03c-console-rule",
                       "sql": "SELECT * FROM \"mv03c/local\"", "actions": ["mv03c-console-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    mv03c_publish_mqtt(server.mqtt_port, "mv03c/local", "{\"v\":21}").await;
    mv02b_wait_success(&server, &token, "mv03c-console-act", 1).await;
    let sink = state
        .engine
        .connectors()
        .get("mv03c-console")
        .expect("console stays registered");
    assert_eq!(
        sink.local_deliveries(),
        Some(1),
        "console captures one delivery"
    );
    let preview = sink
        .local_last_preview()
        .expect("console keeps captured output");
    assert!(
        preview.contains("21"),
        "console captures the delivery value, got {preview:?}"
    );
    let (status, _body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "test", "name": "mv03c-test"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "the test kind refuses in a release build");
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv03c_console_action_update_follows_the_new_connector_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv03c-consupd");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    for name in ["mv03c-cons-upd-a", "mv03c-cons-upd-b"] {
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"type": "console", "name": name}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
    }
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "console", "name": "mv03c-cons-upd-act",
                       "connector": "mv03c-cons-upd-a"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv03c-cons-upd-rule", "name": "mv03c-cons-upd-rule",
                       "sql": "SELECT * FROM \"mv03c/consupd\"",
                       "actions": ["mv03c-cons-upd-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    mv03c_publish_mqtt(server.mqtt_port, "mv03c/consupd", "{\"v\":21}").await;
    mv02b_wait_success(&server, &token, "mv03c-cons-upd-act", 1).await;
    let sink = state
        .engine
        .connectors()
        .get("mv03c-cons-upd-a")
        .expect("console stays registered under the old bare name");
    assert_eq!(sink.local_deliveries(), Some(1));
    assert!(
        sink.local_last_preview()
            .expect("console keeps captured output")
            .contains("21"),
        "console captures the delivery value"
    );
    let (status, _) = server
        .get_auth("/api/v5/connectors/mv03c-cons-upd-a", &token)
        .await;
    assert_eq!(status, 200);
    // Move the action to the second connector.
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv03c-cons-upd-act",
            json!({"type": "console", "name": "mv03c-cons-upd-act",
                       "connector": "mv03c-cons-upd-b"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv03c_publish_mqtt(server.mqtt_port, "mv03c/consupd", "{\"v\":22}").await;
    mv02b_wait_success(&server, &token, "mv03c-cons-upd-act", 2).await;
    let sink = state
        .engine
        .connectors()
        .get("mv03c-cons-upd-b")
        .expect("console follows the new bare name");
    assert_eq!(sink.local_deliveries(), Some(1));
    assert!(
        sink.local_last_preview()
            .expect("console keeps captured output")
            .contains("22"),
        "the connector read shows the new capture"
    );
    let (status, _) = server
        .get_auth("/api/v5/connectors/mv03c-cons-upd-b", &token)
        .await;
    assert_eq!(status, 200);
    assert!(
        state.engine.connectors().get("mv03c-cons-upd-a").is_none(),
        "no stale capture stays under the old name"
    );
    // The same bare name still console keeps one registration.
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv03c-cons-upd-act",
            json!({"type": "console", "name": "mv03c-cons-upd-act",
                       "connector": "mv03c-cons-upd-b"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv03c_publish_mqtt(server.mqtt_port, "mv03c/consupd", "{\"v\":23}").await;
    mv02b_wait_success(&server, &token, "mv03c-cons-upd-act", 3).await;
    let sink = state
        .engine
        .connectors()
        .get("mv03c-cons-upd-b")
        .expect("console keeps the same bare name");
    assert_eq!(sink.local_deliveries(), Some(1));
    assert!(sink
        .local_last_preview()
        .expect("console keeps captured output")
        .contains("23"));
    // An empty bare name registers nothing and removes the old one.
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv03c-cons-upd-act",
            json!({"type": "console", "name": "mv03c-cons-upd-act",
                       "connector": ""}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv03c_publish_mqtt(server.mqtt_port, "mv03c/consupd", "{\"v\":24}").await;
    assert!(
        state.engine.connectors().get("mv03c-cons-upd-b").is_none(),
        "the empty bare name removes the old registration"
    );
    let mut failed = 0;
    for _ in 0..100 {
        let (status, metrics) = server
            .get_auth("/api/v5/actions/mv03c-cons-upd-act/metrics", &token)
            .await;
        assert_eq!(status, 200);
        failed = metrics["metrics"]["failed"].as_u64().unwrap_or(0);
        if failed >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(
        failed >= 1,
        "a console action without a connector counts one failure"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv03c_console_action_delete_removes_the_bare_capture_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv03c-consdel");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "console", "name": "mv03c-cons-del"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "console", "name": "mv03c-cons-del-act",
                       "connector": "mv03c-cons-del"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv03c-cons-del-rule", "name": "mv03c-cons-del-rule",
                       "sql": "SELECT * FROM \"mv03c/consdel\"",
                       "actions": ["mv03c-cons-del-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    mv03c_publish_mqtt(server.mqtt_port, "mv03c/consdel", "{\"v\":21}").await;
    mv02b_wait_success(&server, &token, "mv03c-cons-del-act", 1).await;
    assert!(
        state.engine.connectors().get("mv03c-cons-del").is_some(),
        "console stays registered under the bare name"
    );
    // The delete refuses while a rule still references the action.
    let (status, _) = server
        .delete_auth("/api/v5/rules/mv03c-cons-del-rule", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/mv03c-cons-del-act", &token)
        .await;
    assert_eq!(status, 204);
    mv03c_publish_mqtt(server.mqtt_port, "mv03c/consdel", "{\"v\":22}").await;
    assert!(
        state.engine.connectors().get("mv03c-cons-del").is_none(),
        "no capture stays after the delete"
    );
    let (status, _) = server
        .get_auth("/api/v5/connectors/mv03c-cons-del", &token)
        .await;
    assert_eq!(status, 200, "the connector record stays readable");
    let (status, _) = server
        .get_auth("/api/v5/actions/mv03c-cons-del-act", &token)
        .await;
    assert_eq!(status, 404, "the action is gone");
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv03c_connector_shapes_refuse_missing_fields_and_accept_aliases() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    for (kind, body, field) in [
        ("http", json!({"name": "x"}), "url"),
        ("opc_ua", json!({"name": "x"}), "endpoint_url"),
        ("sparkplug_b", json!({"name": "x"}), "mqtt_url"),
    ] {
        let (status, resp) = server
            .post_auth(
                "/api/v5/connectors",
                serde_json::Value::Object(
                    json!({"type": kind, "name": format!("mv03c-flat-{kind}")})
                        .as_object()
                        .unwrap()
                        .clone()
                        .into_iter()
                        .chain(body.as_object().unwrap().clone())
                        .collect::<serde_json::Map<String, serde_json::Value>>(),
                ),
                &token,
            )
            .await;
        assert_eq!(status, 400, "a missing {field} refuses for {kind}: {resp}");
        assert!(resp.to_string().contains(field), "{resp}");
    }
    for (alias, canonical) in [
        ("rest", "http"),
        ("opcua", "opc_ua"),
        ("sparkplug", "sparkplug_b"),
        ("spb", "sparkplug_b"),
    ] {
        assert_eq!(
            crate::v5::action_sinks::canonical_action_kind(alias),
            canonical,
            "alias {alias} maps to {canonical}"
        );
    }
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "opc_ua", "name": "mv03c-opc-missing",
                       "connector": "no-such", "parameters": {}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a missing node id refuses");
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors_probe",
            json!({"type": "opc_ua", "name": "p"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a probe without a target refuses");
    std::fs::remove_dir_all(unique_data_dir("mv03c-shapes")).ok();
}

#[tokio::test]
async fn mv02b_connector_shapes_refuse_missing_fields_and_accept_aliases() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Flat bodies with a missing required field refuse naming it.
    for (kind, body, field) in [
        ("clickhouse", json!({"database": "db", "table": "t"}), "url"),
        ("tdengine", json!({"database": "db"}), "server"),
        (
            "greptimedb",
            json!({"server": "127.0.0.1:4001"}),
            "database",
        ),
        (
            "iotdb",
            json!({"endpoint": "http://127.0.0.1:6667", "username": "root"}),
            "password",
        ),
        ("opentsdb", json!({"metric": "m", "value": "v"}), "server"),
        ("doris", json!({"database": "db", "table": "t"}), "server"),
        (
            "datalayers",
            json!({"server": "127.0.0.1:8361"}),
            "database",
        ),
        (
            "influxdb",
            json!({"server": "127.0.0.1:8086", "org": "o", "token": "t", "measurement": "m"}),
            "bucket",
        ),
        (
            "timestream",
            json!({"table": "m", "region": "r", "access_key_id": "k",
                       "secret_access_key": "s", "measures": {"f": "DOUBLE"}}),
            "database",
        ),
        (
            "cassandra",
            json!({"servers": "127.0.0.1:9", "table": "t"}),
            "keyspace",
        ),
    ] {
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                Value::Object(
                    json!({"type": kind, "name": format!("mv02b-flat-{kind}")})
                        .as_object()
                        .unwrap()
                        .clone()
                        .into_iter()
                        .chain(body.as_object().unwrap().clone())
                        .collect::<serde_json::Map<String, Value>>(),
                ),
                &token,
            )
            .await;
        if kind == "cassandra" && !cfg!(feature = "cassandra") {
            assert_eq!(status, 400, "cassandra without the feature refuses");
            continue;
        }
        assert_eq!(status, 400, "{kind} refuses without {field}: {body}");
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(
            body["message"].as_str().unwrap_or("").contains(field),
            "{kind} names {field}: {body}"
        );
    }

    // Every documented alias creates.
    for (kind, body) in [
        (
            "ch",
            json!({"url": "http://127.0.0.1:9", "database": "db", "table": "t"}),
        ),
        (
            "greptime",
            json!({"server": "127.0.0.1:4001", "database": "db", "table": "t"}),
        ),
        (
            "opents",
            json!({"server": "http://127.0.0.1:9", "metric": "m", "value": "v"}),
        ),
        (
            "aws_timestream",
            json!({"database": "db", "table": "m", "region": "r",
                       "access_key_id": "k", "secret_access_key": "s",
                       "measures": {"f": "DOUBLE"}, "endpoint": "http://127.0.0.1:9"}),
        ),
        (
            "scylla",
            json!({"servers": "127.0.0.1:9", "keyspace": "ks", "table": "t"}),
        ),
        (
            "scylladb",
            json!({"servers": "127.0.0.1:9", "keyspace": "ks", "table": "t"}),
        ),
        (
            "cql",
            json!({"servers": "127.0.0.1:9", "keyspace": "ks", "table": "t"}),
        ),
    ] {
        let mut full = body.as_object().unwrap().clone();
        full.insert("type".to_string(), json!(kind));
        full.insert("name".to_string(), json!(format!("mv02b-alias-{kind}")));
        let (status, created) = server
            .post_auth("/api/v5/connectors", Value::Object(full), &token)
            .await;
        // Scylla aliases stay pending (SB-27). They never create.
        if kind == "scylla" || kind == "scylladb" || kind == "cql" {
            assert_eq!(status, 400, "{kind} pending refuses");
            continue;
        }
        if kind == "ch" && !cfg!(feature = "clickhouse") {
            assert_eq!(status, 400, "{kind} without the feature refuses");
            continue;
        }
        assert_eq!(status, 201, "{kind} alias creates: {created}");
    }

    // Probes against a closed port fail closed with TEST_FAILED.
    for (kind, body) in [
        (
            "clickhouse",
            json!({"url": "http://127.0.0.1:9", "database": "db", "table": "t"}),
        ),
        (
            "tdengine",
            json!({"server": "127.0.0.1:9", "database": "db"}),
        ),
        (
            "iotdb",
            json!({"endpoint": "http://127.0.0.1:9",
                             "username": "u", "password": "p"}),
        ),
        (
            "timestream",
            json!({"database": "db", "table": "m", "region": "r",
                                  "access_key_id": "k", "secret_access_key": "s",
                                  "measures": {"f": "DOUBLE"},
                                  "endpoint": "http://127.0.0.1:9"}),
        ),
    ] {
        let mut full = body.as_object().unwrap().clone();
        full.insert("type".to_string(), json!(kind));
        let (status, probed) = server
            .post_auth("/api/v5/connectors_probe", Value::Object(full), &token)
            .await;
        assert_eq!(status, 400, "{kind} probe fails closed: {probed}");
        assert_eq!(probed["code"], json!("TEST_FAILED"));
    }
}
