//! Shared harness for connector tests.
//!
//! One kernel boots on free ports.
//! A test creates one connector and rule.
//! It publishes on a real socket.
//! It reads the result from a target.
//! A fault file controls target state.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Bound for one step in a test.
/// Reason: 10 s covers publish plus retry.
/// Source: pipeline timeout budget.
pub const STEP: Duration = Duration::from_secs(10);
/// Bound for a cold kernel boot.
/// Reason: 60 s covers slow CI boot.
/// Source: pipeline timeout budget.
pub const BOOT: Duration = Duration::from_secs(60);
/// Management key used by the test broker.
pub const API_KEY: &str = "ce01-e2e-test-key-0001";
/// Fault-file variable of the qualification runner.
pub const QUAL_FAULT_FILE: &str = "QUAL_FAULT_FILE";
/// Runner address for the MQTT target.
/// Reason: the runner owns the server in qual.
/// Source: DX2-027 QUAL-ENV MQTT_BRIDGE_URL.
pub const QUAL_MQTT_URL: &str = "MQTT_BRIDGE_URL";
/// Runner address for the HTTP target.
/// Reason: the runner owns the server in qual.
/// Source: CE-01 item 2 runner address.
pub const QUAL_HTTP_URL: &str = "QUAL_HTTP_URL";

/// Slot for harness tags.
/// Reason: parallel boots need unique dirs.
/// Source: pipeline runs tests in threads.
static TAG_SLOT: AtomicU64 = AtomicU64::new(0);

/// Takes one unique tag for dirs and files.
/// Reason: parallel tests share pid and time.
/// Source: e2e_integration tag slot pattern.
fn unique_tag(prefix: &str) -> String {
    let slot = TAG_SLOT.fetch_add(1, Ordering::SeqCst);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_nanos())
        .unwrap_or(0);
    format!(
        "{prefix}-{}-{:?}-{nanos}-{slot}",
        std::process::id(),
        std::thread::current().id()
    )
}

/// One message or request that the target received.
#[derive(Clone, Debug)]
pub struct Received {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
}

/// Target family for one test.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TargetKind {
    Http,
    Https,
    Mqtt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    Up,
    Down,
    Slow,
    Status(u16),
}

struct TargetShared {
    kind: TargetKind,
    fault_file: PathBuf,
    records: Mutex<Vec<Received>>,
    client_ids: Mutex<Vec<String>>,
    clean_starts: Mutex<Vec<bool>>,
    keepalives: Mutex<Vec<u16>>,
    protocol_levels: Mutex<Vec<u8>>,
    statuses: Mutex<VecDeque<u16>>,
    http_statuses: Mutex<Vec<u16>>,
    expected_credentials: Option<(String, String)>,
    tls: Option<tokio_rustls::TlsAcceptor>,
    shutdown: AtomicBool,
}

/// A real target that a connector can reach.
pub struct TestTarget {
    kind: TargetKind,
    port: u16,
    fault_file: PathBuf,
    shared: Arc<TargetShared>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl TestTarget {
    /// Starts a plain HTTP receiver.
    pub async fn start_http() -> Self {
        Self::start(TargetKind::Http, None).await
    }

    /// Starts an HTTPS receiver with the test certificate.
    pub async fn start_https() -> Self {
        Self::start(TargetKind::Https, None).await
    }

    /// Starts a small MQTT 3.1.1/5.0 broker.
    pub async fn start_mqtt() -> Self {
        Self::start(TargetKind::Mqtt, None).await
    }

    /// Starts a small MQTT broker that checks one username and password.
    pub async fn start_mqtt_auth(username: &str, password: &str) -> Self {
        Self::start(
            TargetKind::Mqtt,
            Some((username.to_string(), password.to_string())),
        )
        .await
    }

    async fn start(kind: TargetKind, expected_credentials: Option<(String, String)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("target binds loopback");
        let port = listener.local_addr().expect("target address").port();
        let tag = unique_tag("ce01-target");
        let fault_file = std::env::var_os(QUAL_FAULT_FILE)
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join(format!("{tag}-fault")));
        std::fs::write(&fault_file, "up").expect("fault file writes");
        let tls = if kind == TargetKind::Https {
            Some(tls_acceptor())
        } else {
            None
        };
        let shared = Arc::new(TargetShared {
            kind,
            fault_file: fault_file.clone(),
            records: Mutex::new(Vec::new()),
            client_ids: Mutex::new(Vec::new()),
            clean_starts: Mutex::new(Vec::new()),
            keepalives: Mutex::new(Vec::new()),
            protocol_levels: Mutex::new(Vec::new()),
            statuses: Mutex::new(VecDeque::new()),
            http_statuses: Mutex::new(Vec::new()),
            expected_credentials,
            tls,
            shutdown: AtomicBool::new(false),
        });
        let loop_shared = shared.clone();
        let task = tokio::spawn(async move {
            loop {
                if loop_shared.shutdown.load(Ordering::SeqCst) {
                    break;
                }
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let connection_shared = loop_shared.clone();
                        tokio::spawn(async move {
                            handle_connection(connection_shared, stream).await;
                        });
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            kind,
            port,
            fault_file,
            shared,
            task: Mutex::new(Some(task)),
        }
    }

    /// Returns the URL or address for the connector.
    /// Reason: prefers the runner address in qual.
    /// Source: CE-01 item 2 QUAL_* address.
    pub fn address(&self) -> String {
        if self.kind == TargetKind::Mqtt {
            if let Ok(value) = std::env::var(QUAL_MQTT_URL) {
                let value = value.trim().to_string();
                if !value.is_empty() {
                    return value;
                }
            }
        } else if let Ok(value) = std::env::var(QUAL_HTTP_URL) {
            let value = value.trim().to_string();
            if !value.is_empty() {
                return value;
            }
        }
        match self.kind {
            TargetKind::Http => format!("http://127.0.0.1:{}", self.port),
            TargetKind::Https => format!("https://127.0.0.1:{}", self.port),
            TargetKind::Mqtt => format!("mqtt://127.0.0.1:{}", self.port),
        }
    }

    /// Returns the loopback URL even in qual.
    /// Reason: TLS tests need a local address.
    /// Source: CE-01 item 4 local target.
    pub fn local_address(&self) -> String {
        match self.kind {
            TargetKind::Http => format!("http://127.0.0.1:{}", self.port),
            TargetKind::Https => format!("https://127.0.0.1:{}", self.port),
            TargetKind::Mqtt => format!("mqtt://127.0.0.1:{}", self.port),
        }
    }

    /// Writes one fault directive: `up`, `down`, `slow`, `status:NNN`.
    pub fn set_fault(&self, value: &str) {
        self.shared.statuses.lock().expect("status lock").clear();
        std::fs::write(&self.fault_file, value).expect("fault file writes");
    }

    /// Writes a status sequence such as `[429, 200]`.
    pub fn set_status_sequence(&self, statuses: &[u16]) {
        let text = format!(
            "statuses:{}",
            statuses
                .iter()
                .map(u16::to_string)
                .collect::<Vec<_>>()
                .join(",")
        );
        let mut queue = self.shared.statuses.lock().expect("status lock");
        queue.clear();
        queue.extend(statuses.iter().copied());
        std::fs::write(&self.fault_file, text).expect("fault file writes");
    }

    /// Stops the target and starts it again.
    /// Reason: fault file drives the target per ticket.
    /// Source: CE-01 item 2 fault file.
    pub async fn stop_and_restart_server(&self) {
        self.set_fault("down");
        // Reason: 300 ms lets inflight sends fail.
        // Source: broker backoff is 2 s.
        tokio::time::sleep(Duration::from_millis(300)).await;
        self.set_fault("up");
        // Reason: 100 ms lets the listener accept again.
        // Source: loopback accept is instant.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    /// Makes the next target step slow.
    pub fn slow_server(&self) {
        self.set_fault("slow");
    }

    /// Returns every delivered record.
    pub fn records(&self) -> Vec<Received> {
        self.shared.records.lock().expect("records lock").clone()
    }

    /// Returns the number of delivered records.
    pub fn count_in_server(&self) -> usize {
        record_count(&self.records())
    }

    /// Waits for at least `expected` records.
    /// Reason: polls until the broker delivers.
    /// Source: pipeline timeout budget.
    pub async fn wait_for_count(&self, expected: usize, timeout: Duration) -> usize {
        let deadline = Instant::now() + timeout;
        loop {
            let count = self.count_in_server();
            if count >= expected {
                return count;
            }
            if Instant::now() >= deadline {
                panic!("target received {count} records, expected {expected}");
            }
            // Reason: 20 ms polls fast without load.
            // Source: loopback delivery is instant.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Waits for one batch with exactly `size_limit` docs.
    /// Reason: proves size trigger, not single sends.
    /// Source: CE-01 item 4 batching.
    pub async fn expect_batch(&self, size_limit: usize, time_limit: Duration) -> usize {
        let deadline = Instant::now() + time_limit;
        loop {
            let records = self.records();
            if self.kind == TargetKind::Mqtt {
                let count = record_count(&records);
                if count >= size_limit {
                    return count;
                }
            } else if records.iter().any(|record| documents(record) == size_limit) {
                return size_limit;
            }
            if Instant::now() >= deadline {
                panic!("no batch of {size_limit} records in {time_limit:?}");
            }
            // Reason: 20 ms polls fast without load.
            // Source: loopback delivery is instant.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Waits for one MQTT flush with exact count.
    /// Reason: cumulative count hides single sends.
    /// Source: CE-01 P2 single batch of 3.
    pub async fn expect_mqtt_batch(&self, size_limit: usize, time_limit: Duration) -> usize {
        let deadline = Instant::now() + time_limit;
        loop {
            let count = record_count(&self.records());
            if count == size_limit {
                return count;
            }
            if count > size_limit || Instant::now() >= deadline {
                panic!("mqtt target holds {count}, expected one batch of {size_limit}");
            }
            // Reason: 20 ms polls fast without load.
            // Source: loopback delivery is instant.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Returns the client ids that the MQTT target saw.
    pub fn client_ids(&self) -> Vec<String> {
        self.shared.client_ids.lock().expect("client lock").clone()
    }

    /// Returns the clean-start flags that the MQTT target saw.
    pub fn clean_starts(&self) -> Vec<bool> {
        self.shared.clean_starts.lock().expect("clean lock").clone()
    }

    /// Returns the keep-alive values that the MQTT target saw.
    pub fn keepalives(&self) -> Vec<u16> {
        self.shared.keepalives.lock().expect("keep lock").clone()
    }

    /// Returns the CONNECT levels that the target saw.
    /// Reason: proves V311 (4) and V50 (5).
    /// Source: CE-01 P3 protocol level.
    pub fn protocol_levels(&self) -> Vec<u8> {
        self.shared
            .protocol_levels
            .lock()
            .expect("level lock")
            .clone()
    }

    /// Returns every HTTP status that the receiver sent.
    pub fn http_statuses(&self) -> Vec<u16> {
        self.shared
            .http_statuses
            .lock()
            .expect("status log")
            .clone()
    }
}

impl Drop for TestTarget {
    fn drop(&mut self) {
        self.shared.shutdown.store(true, Ordering::SeqCst);
        if let Some(task) = self.task.lock().expect("task lock").take() {
            task.abort();
        }
        let _ = std::fs::remove_file(&self.fault_file);
    }
}

fn record_count(records: &[Received]) -> usize {
    records.iter().map(documents).sum()
}

fn documents(record: &Received) -> usize {
    if record.topic.is_empty() {
        if let Ok(Value::Array(rows)) = serde_json::from_slice::<Value>(&record.body) {
            return rows.len();
        }
        1
    } else {
        1
    }
}

fn read_fault(shared: &TargetShared) -> Fault {
    let text = std::fs::read_to_string(&shared.fault_file).unwrap_or_default();
    let text = text.trim();
    if text == "down" {
        return Fault::Down;
    }
    if text == "slow" {
        return Fault::Slow;
    }
    if text.starts_with("statuses:") {
        let mut queue = shared.statuses.lock().expect("status lock");
        return Fault::Status(queue.pop_front().unwrap_or(200));
    }
    if let Some(rest) = text.strip_prefix("status:") {
        return Fault::Status(rest.trim().parse().unwrap_or(200));
    }
    Fault::Up
}

fn tls_acceptor() -> tokio_rustls::TlsAcceptor {
    use rustls::pki_types::pem::PemObject as _;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    let base = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("k1_05");
    let certs: Vec<CertificateDer<'static>> =
        CertificateDer::pem_file_iter(base.join("server-cert.pem"))
            .expect("cert file reads")
            .collect::<Result<Vec<_>, _>>()
            .expect("cert parses");
    let key: PrivateKeyDer<'static> = PrivateKeyDer::pem_file_iter(base.join("server-key.pem"))
        .expect("key file reads")
        .next()
        .transpose()
        .expect("key parses")
        .expect("key exists");
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("TLS versions build")
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("TLS config builds");
    tokio_rustls::TlsAcceptor::from(Arc::new(config))
}

async fn handle_connection(shared: Arc<TargetShared>, stream: TcpStream) {
    let text = std::fs::read_to_string(&shared.fault_file).unwrap_or_default();
    let text = text.trim();
    if text == "down" {
        return;
    }
    if text == "slow" {
        // Reason: 500 ms exceeds client timeout in tests.
        // Source: timeout test uses 100 ms.
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    match shared.kind {
        TargetKind::Http => serve_http(stream, &shared).await,
        TargetKind::Https => {
            let Some(acceptor) = shared.tls.clone() else {
                return;
            };
            let Ok(stream) = acceptor.accept(stream).await else {
                return;
            };
            serve_http(stream, &shared).await;
        }
        TargetKind::Mqtt => serve_mqtt(stream, &shared).await,
    }
}

async fn serve_http<S>(mut stream: S, shared: &TargetShared)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut raw = Vec::new();
    let header_end = loop {
        let mut byte = [0u8; 1];
        match tokio::time::timeout(STEP, stream.read_exact(&mut byte)).await {
            Ok(Ok(_)) => raw.push(byte[0]),
            _ => return,
        }
        // Reason: 64 KiB caps a hostile header.
        // Source: common HTTP server header cap.
        if raw.len() > 65_536 {
            return;
        }
        if raw.ends_with(b"\r\n\r\n") {
            break raw.len();
        }
    };
    let head = String::from_utf8_lossy(&raw[..header_end]).to_string();
    let mut lines = head.lines();
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();
    let mut headers = Vec::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            headers.push((name.trim().to_string(), value.trim().to_string()));
        }
    }
    let length = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = vec![0u8; length];
    if length > 0
        && !matches!(
            tokio::time::timeout(STEP, stream.read_exact(&mut body)).await,
            Ok(Ok(_))
        )
    {
        return;
    }
    let fault = read_fault(shared);
    let status = match fault {
        Fault::Down => return,
        Fault::Slow => {
            // Reason: 500 ms exceeds client timeout in tests.
            // Source: timeout test uses 100 ms.
            tokio::time::sleep(Duration::from_millis(500)).await;
            200
        }
        Fault::Status(code) => code,
        Fault::Up => 200,
    };
    shared
        .http_statuses
        .lock()
        .expect("status log")
        .push(status);
    if (200..300).contains(&status) {
        shared.records.lock().expect("records lock").push(Received {
            method,
            path,
            headers,
            body,
            topic: String::new(),
            payload: Vec::new(),
            qos: 0,
            retain: false,
        });
    }
    let reply = format!("HTTP/1.1 {status} OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");
    let _ = stream.write_all(reply.as_bytes()).await;
    let _ = stream.shutdown().await;
}

async fn read_packet<R>(stream: &mut R) -> std::io::Result<(u8, Vec<u8>)>
where
    R: AsyncRead + Unpin,
{
    let mut head = [0u8; 1];
    stream.read_exact(&mut head).await?;
    let mut multiplier = 1usize;
    let mut remaining = 0usize;
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).await?;
        remaining += ((byte[0] & 0x7f) as usize) * multiplier;
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
    stream.read_exact(&mut body).await?;
    Ok((head[0], body))
}

fn take_u16(body: &[u8], at: &mut usize) -> Option<u16> {
    if *at + 2 > body.len() {
        return None;
    }
    let value = u16::from_be_bytes([body[*at], body[*at + 1]]);
    *at += 2;
    Some(value)
}

fn take_string(body: &[u8], at: &mut usize) -> Option<Vec<u8>> {
    let length = take_u16(body, at)? as usize;
    if *at + length > body.len() {
        return None;
    }
    let value = body[*at..*at + length].to_vec();
    *at += length;
    Some(value)
}

fn take_varint(body: &[u8], at: &mut usize) -> Option<usize> {
    let mut value = 0usize;
    let mut multiplier = 1usize;
    loop {
        if *at >= body.len() {
            return None;
        }
        let byte = body[*at];
        *at += 1;
        value += ((byte & 0x7f) as usize) * multiplier;
        if byte & 0x80 == 0 {
            return Some(value);
        }
        multiplier *= 128;
    }
}

async fn serve_mqtt(mut stream: TcpStream, shared: &TargetShared) {
    let (head, body) = match tokio::time::timeout(STEP, read_packet(&mut stream)).await {
        Ok(Ok(packet)) => packet,
        _ => return,
    };
    if head >> 4 != 1 {
        return;
    }
    let mut at = 0usize;
    let Some(protocol) = take_string(&body, &mut at) else {
        return;
    };
    if protocol != b"MQTT" || at >= body.len() {
        return;
    }
    let level = body[at];
    at += 1;
    if at >= body.len() {
        return;
    }
    let flags = body[at];
    at += 1;
    let Some(keepalive) = take_u16(&body, &mut at) else {
        return;
    };
    if level == 5 {
        let Some(length) = take_varint(&body, &mut at) else {
            return;
        };
        at += length;
    }
    let Some(client_id) = take_string(&body, &mut at) else {
        return;
    };
    if flags & 0x04 != 0 {
        if level == 5 {
            let Some(length) = take_varint(&body, &mut at) else {
                return;
            };
            at += length;
        }
        if take_string(&body, &mut at).is_none() || take_string(&body, &mut at).is_none() {
            return;
        }
    }
    let username = if flags & 0x80 != 0 {
        take_string(&body, &mut at).map(|value| String::from_utf8_lossy(&value).into_owned())
    } else {
        None
    };
    let password = if flags & 0x40 != 0 {
        take_string(&body, &mut at).map(|value| String::from_utf8_lossy(&value).into_owned())
    } else {
        None
    };
    let accepted = match &shared.expected_credentials {
        Some((expected_user, expected_password)) => {
            username.as_deref() == Some(expected_user.as_str())
                && password.as_deref() == Some(expected_password.as_str())
        }
        None => true,
    };
    if !accepted {
        let code = 0x04u8;
        let reply = if level == 5 {
            vec![0x20, 0x03, 0x00, code, 0x00]
        } else {
            vec![0x20, 0x02, 0x00, code]
        };
        let _ = stream.write_all(&reply).await;
        return;
    }
    shared
        .client_ids
        .lock()
        .expect("client lock")
        .push(String::from_utf8_lossy(&client_id).into_owned());
    shared
        .clean_starts
        .lock()
        .expect("clean lock")
        .push(flags & 0x02 != 0);
    shared.keepalives.lock().expect("keep lock").push(keepalive);
    shared
        .protocol_levels
        .lock()
        .expect("level lock")
        .push(level);
    let connack = if level == 5 {
        vec![0x20, 0x03, 0x00, 0x00, 0x00]
    } else {
        vec![0x20, 0x02, 0x00, 0x00]
    };
    let _ = stream.write_all(&connack).await;
    loop {
        if matches!(read_fault(shared), Fault::Down) {
            return;
        }
        // Reason: 30 s covers linger plus retry.
        // Source: pipeline timeout budget.
        let (head, body) =
            match tokio::time::timeout(Duration::from_secs(30), read_packet(&mut stream)).await {
                Ok(Ok(packet)) => packet,
                _ => return,
            };
        let packet_type = head >> 4;
        if matches!(read_fault(shared), Fault::Down) {
            return;
        }
        if packet_type == 3 {
            let qos = (head >> 1) & 0x03;
            let retain = head & 0x01 != 0;
            let mut at = 0usize;
            let Some(topic) = take_string(&body, &mut at) else {
                return;
            };
            let packet_id = if qos > 0 {
                take_u16(&body, &mut at)
            } else {
                Some(0)
            };
            let Some(packet_id) = packet_id else {
                return;
            };
            if level == 5 {
                let Some(length) = take_varint(&body, &mut at) else {
                    return;
                };
                at += length;
            }
            let payload = body[at..].to_vec();
            shared.records.lock().expect("records lock").push(Received {
                method: String::new(),
                path: String::new(),
                headers: Vec::new(),
                body: Vec::new(),
                topic: String::from_utf8_lossy(&topic).into_owned(),
                payload,
                qos,
                retain,
            });
            if qos == 1 {
                let ack = vec![0x40, 0x02, (packet_id >> 8) as u8, packet_id as u8];
                let _ = stream.write_all(&ack).await;
            } else if qos == 2 {
                let ack = vec![0x50, 0x02, (packet_id >> 8) as u8, packet_id as u8];
                let _ = stream.write_all(&ack).await;
            }
        } else if packet_type == 12 {
            let _ = stream.write_all(&[0xd0, 0x00]).await;
        } else if packet_type == 14 {
            return;
        }
    }
}

/// One booted kernel.
pub struct TestBroker {
    child: tokio::process::Child,
    pub mqtt_port: u16,
    pub api_port: u16,
    scratch: PathBuf,
    config_dir: PathBuf,
    envs: Vec<(String, String)>,
    http: reqwest::Client,
    api_key: String,
}

impl TestBroker {
    /// Boots a kernel with default extra configuration.
    pub async fn start(extra_toml: &str) -> Self {
        Self::start_with_env(extra_toml, &[]).await
    }

    /// Boots a kernel with extra environment variables.
    pub async fn start_with_env(extra_toml: &str, envs: &[(&str, String)]) -> Self {
        let mut attempt = 0;
        loop {
            attempt += 1;
            match Self::try_start(extra_toml, envs).await {
                Ok(broker) => return broker,
                Err(error) if error.0 && attempt < 3 => continue,
                Err(error) => panic!("{}", error.1),
            }
        }
    }

    async fn try_start(extra_toml: &str, envs: &[(&str, String)]) -> Result<Self, (bool, String)> {
        let tag = unique_tag("ce01");
        let scratch = std::env::temp_dir().join(format!("{tag}-data"));
        let config_dir = std::env::temp_dir().join(format!("{tag}-cfg"));
        std::fs::create_dir_all(&config_dir).map_err(|e| (false, e.to_string()))?;
        let bl_port = free_port();
        let mqtt_port = free_port();
        let api_port = free_port();
        std::fs::write(
            config_dir.join("indra.toml"),
            format!(
                "[node]\n\
                 id = \"ce01-e2e\"\n\
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
                 {extra_toml}",
                scratch.display(),
            ),
        )
        .map_err(|e| (false, e.to_string()))?;
        let mut child = kernel_command(&config_dir, envs)
            .spawn()
            .map_err(|e| (false, e.to_string()))?;
        if let Err(error) = Self::wait_until_ready(&mut child, mqtt_port, api_port).await {
            let _ = child.kill().await;
            let _ = std::fs::remove_dir_all(&scratch);
            let _ = std::fs::remove_dir_all(&config_dir);
            return Err(error);
        }
        Ok(Self {
            child,
            mqtt_port,
            api_port,
            scratch,
            config_dir,
            envs: envs
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect(),
            http: reqwest::Client::new(),
            api_key: API_KEY.to_string(),
        })
    }

    async fn wait_until_ready(
        child: &mut tokio::process::Child,
        mqtt_port: u16,
        api_port: u16,
    ) -> Result<(), (bool, String)> {
        let http = reqwest::Client::new();
        let base = format!("http://127.0.0.1:{api_port}");
        let deadline = Instant::now() + BOOT;
        loop {
            // Reason: 2 s bounds one health probe.
            // Source: pipeline timeout budget.
            let up = tokio::time::timeout(
                Duration::from_secs(2),
                http.get(format!("{base}/healthz")).send(),
            )
            .await;
            if let Ok(Ok(response)) = up {
                if let Ok(text) = response.text().await {
                    if text == "OK" && TcpStream::connect(("127.0.0.1", mqtt_port)).await.is_ok() {
                        return Ok(());
                    }
                }
            }
            if Instant::now() >= deadline {
                return Err((
                    false,
                    format!("kernel never opened API {api_port} and MQTT {mqtt_port}"),
                ));
            }
            if let Ok(Some(status)) = child.try_wait() {
                return Err((true, format!("kernel exited during boot: {status}")));
            }
            // Reason: 200 ms polls boot without load.
            // Source: pipeline timeout budget.
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Sends SIGTERM and waits for the kernel to stop in order.
    /// Reason: an orderly stop drains the connector buffers.
    /// Source: CE-01 item 4 orderly stop.
    pub async fn stop_orderly(&mut self) {
        #[cfg(unix)]
        {
            let mut sent = false;
            if let Some(pid) = self.child.id() {
                sent = std::process::Command::new("kill")
                    .arg("-TERM")
                    .arg(pid.to_string())
                    .status()
                    .is_ok_and(|status| status.success());
            }
            if !sent {
                let _ = self.child.start_kill();
            }
        }
        #[cfg(not(unix))]
        {
            let _ = self.child.start_kill();
        }
        let deadline = Instant::now() + STEP;
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Ok(None) | Err(_) => {
                    let _ = self.child.kill().await;
                    let _ = self.child.wait().await;
                    return;
                }
            }
        }
    }

    /// Starts the kernel again on the same ports and data directory.
    /// Reason: an orderly stop must keep the data directory.
    /// Source: CE-01 item 4 orderly stop.
    pub async fn start_again(&mut self) {
        let mut child = kernel_command(&self.config_dir, &self.envs)
            .spawn()
            .expect("kernel restarts");
        if let Err((_, message)) =
            Self::wait_until_ready(&mut child, self.mqtt_port, self.api_port).await
        {
            let _ = child.kill().await;
            panic!("{message}");
        }
        self.child = child;
    }

    /// Stops the kernel in order, then starts it again.
    /// Reason: proves buffered rows survive an orderly stop.
    /// Source: CE-01 item 4 orderly stop.
    pub async fn stop_and_restart_orderly(&mut self) {
        self.stop_orderly().await;
        self.start_again().await;
    }

    /// Returns the management API base URL.
    pub fn base(&self) -> String {
        format!("http://127.0.0.1:{}", self.api_port)
    }

    /// Sends one management request.
    pub async fn api(&self, method: reqwest::Method, path: &str, body: Value) -> (u16, Value) {
        let mut request = self
            .http
            .request(method, format!("{}{path}", self.base()))
            .bearer_auth(&self.api_key);
        if !body.is_null() {
            request = request.json(&body);
        }
        let response = tokio::time::timeout(STEP, request.send())
            .await
            .expect("management request in time")
            .expect("management request");
        let status = response.status().as_u16();
        let text = response.text().await.expect("management body");
        let value = if text.trim().is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text).unwrap_or(Value::String(text))
        };
        (status, value)
    }

    /// Creates a connector through `/api/v1/connectors`.
    pub async fn create_connector(&self, id: &str, kind: &str, config: Value) -> Value {
        let (status, body) = self
            .api(
                reqwest::Method::POST,
                "/api/v1/connectors",
                serde_json::json!({"id": id, "kind": kind, "config": config}),
            )
            .await;
        assert_eq!(status, 201, "connector {id} create: {body}");
        body
    }

    /// Creates a forwarding rule and returns its id.
    pub async fn create_forward_rule(
        &self,
        name: &str,
        topic_filter: &str,
        connector_id: &str,
    ) -> String {
        let sql = format!("SELECT * FROM \"{}\"", topic_filter.replace('"', "\\\""));
        let (status, body) = self
            .api(
                reqwest::Method::POST,
                "/api/v1/rules",
                serde_json::json!({
                    "name": name,
                    "topic_filter": topic_filter,
                    "sql_query": sql,
                    "actions": [{"type": "forwardconnector", "connector_id": connector_id}],
                }),
            )
            .await;
        assert_eq!(status, 201, "rule {name} create: {body}");
        body["id"].as_str().expect("rule id").to_string()
    }

    /// Reads one rule's live action counters.
    pub async fn rule_actions(&self, id: &str) -> (u64, u64, u64) {
        let (status, body) = self
            .api(
                reqwest::Method::GET,
                &format!("/api/v5/rules/{id}/metrics"),
                Value::Null,
            )
            .await;
        assert_eq!(status, 200, "rule metrics {id}: {body}");
        let metrics = &body["metrics"];
        (
            metrics["actions.total"].as_u64().unwrap_or(0),
            metrics["actions.success"].as_u64().unwrap_or(0),
            metrics["actions.failed"].as_u64().unwrap_or(0),
        )
    }

    /// Waits for at least `expected` failed actions.
    /// Reason: polls until the broker counts errors.
    /// Source: pipeline timeout budget.
    pub async fn wait_for_action_failures(
        &self,
        id: &str,
        expected: u64,
        timeout: Duration,
    ) -> (u64, u64, u64) {
        let deadline = Instant::now() + timeout;
        loop {
            let counters = self.rule_actions(id).await;
            if counters.2 >= expected {
                return counters;
            }
            if Instant::now() >= deadline {
                panic!("rule {id} has {counters:?}, expected {expected} failures");
            }
            // Reason: 50 ms polls fast without load.
            // Source: loopback delivery is instant.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Waits for at least `expected` successful actions.
    /// Reason: proves rows entered the connector before a stop.
    /// Source: CE-01 item 4 partial batch.
    pub async fn wait_for_action_success(
        &self,
        id: &str,
        expected: u64,
        timeout: Duration,
    ) -> (u64, u64, u64) {
        let deadline = Instant::now() + timeout;
        loop {
            let counters = self.rule_actions(id).await;
            if counters.1 >= expected {
                return counters;
            }
            if Instant::now() >= deadline {
                panic!("rule {id} has {counters:?}, expected {expected} successes");
            }
            // Reason: 50 ms polls fast without load.
            // Source: loopback delivery is instant.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Lists connectors through the management API.
    /// Reason: proves the record exists after create.
    /// Source: CE-01 item 2 management API.
    pub async fn list_connectors(&self) -> Value {
        let (status, body) = self
            .api(reqwest::Method::GET, "/api/v1/connectors", Value::Null)
            .await;
        assert_eq!(status, 200, "list connectors: {body}");
        body
    }

    /// Connects one MQTT 3.1.1 client.
    pub async fn connect(&self, client_id: &str) -> MqttClient {
        MqttClient::connect(self.mqtt_port, client_id).await
    }

    /// Publishes one message on a fresh connection.
    pub async fn publish(&self, client_id: &str, topic: &str, payload: &[u8], qos: u8) {
        let mut client = self.connect(client_id).await;
        client.publish(topic, payload, qos).await;
        client.disconnect().await;
    }

    /// Stops the kernel and removes its temp dirs.
    pub async fn finish(mut self) {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
        let _ = std::fs::remove_dir_all(&self.scratch);
        let _ = std::fs::remove_dir_all(&self.config_dir);
    }
}

/// One real MQTT client socket.
pub struct MqttClient {
    stream: TcpStream,
    packet_id: u16,
}

impl MqttClient {
    pub async fn connect(port: u16, client_id: &str) -> Self {
        let mut stream = tokio::time::timeout(STEP, TcpStream::connect(("127.0.0.1", port)))
            .await
            .expect("MQTT dial in time")
            .expect("MQTT dial");
        let packet = connect_packet(client_id);
        stream.write_all(&packet).await.expect("CONNECT write");
        let (head, body) = tokio::time::timeout(STEP, read_packet(&mut stream))
            .await
            .expect("CONNACK in time")
            .expect("CONNACK read");
        assert_eq!(head & 0xF0, 0x20, "server answers CONNACK");
        assert_eq!(body.get(1).copied().unwrap_or(1), 0, "CONNECT accepted");
        Self {
            stream,
            packet_id: 1,
        }
    }

    pub async fn publish(&mut self, topic: &str, payload: &[u8], qos: u8) {
        let id = self.packet_id;
        self.packet_id = self.packet_id.wrapping_add(1).max(1);
        let packet = publish_packet(topic, id, qos, payload);
        self.stream.write_all(&packet).await.expect("PUBLISH write");
        if qos > 0 {
            let (head, _) = tokio::time::timeout(STEP, read_packet(&mut self.stream))
                .await
                .expect("PUBACK in time")
                .expect("PUBACK read");
            assert_eq!(head & 0xF0, 0x40, "publisher sees PUBACK");
        }
    }

    pub async fn disconnect(mut self) {
        let _ = self.stream.write_all(&[0xE0, 0x00]).await;
        let _ = self.stream.shutdown().await;
    }
}

fn kernel_command<S: AsRef<str>>(
    config_dir: &Path,
    envs: &[(S, String)],
) -> tokio::process::Command {
    let mut command = tokio::process::Command::new(kernel_binary());
    command
        .arg("--config-dir")
        .arg(config_dir)
        .arg("--allow-anonymous")
        .env("INDRA_API_KEYS", API_KEY)
        .env("NO_COLOR", "1")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    for (name, value) in envs {
        command.env(name.as_ref(), value);
    }
    command
}

fn kernel_binary() -> PathBuf {
    if let Some(built) = option_env!("CARGO_BIN_EXE_indramqtt") {
        return PathBuf::from(built);
    }
    let exe = if cfg!(windows) {
        "indramqtt.exe"
    } else {
        "indramqtt"
    };
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for candidate in [
        manifest.join(format!("../../target/debug/{exe}")),
        manifest.join(format!("../../target/ci-check/debug/{exe}")),
    ] {
        if candidate.is_file() {
            return candidate;
        }
    }
    panic!("kernel binary not found: build the workspace first");
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("probe binds")
        .local_addr()
        .expect("probe address")
        .port()
}

fn mqtt_remaining_length(mut value: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            return out;
        }
    }
}

fn mqtt_string(value: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len() + 2);
    out.extend_from_slice(&(value.len() as u16).to_be_bytes());
    out.extend_from_slice(value.as_bytes());
    out
}

fn connect_packet(client_id: &str) -> Vec<u8> {
    let mut rest = mqtt_string("MQTT");
    rest.push(4);
    rest.push(0x02);
    rest.extend_from_slice(&60u16.to_be_bytes());
    rest.extend_from_slice(&mqtt_string(client_id));
    let mut packet = vec![0x10];
    packet.extend_from_slice(&mqtt_remaining_length(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

fn publish_packet(topic: &str, packet_id: u16, qos: u8, payload: &[u8]) -> Vec<u8> {
    let mut rest = mqtt_string(topic);
    if qos > 0 {
        rest.extend_from_slice(&packet_id.to_be_bytes());
    }
    rest.extend_from_slice(payload);
    let mut packet = vec![0x30 | (qos << 1)];
    packet.extend_from_slice(&mqtt_remaining_length(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}
