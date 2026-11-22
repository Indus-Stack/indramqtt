#[cfg(feature = "mongodb")]
use super::document::{mv02c_bson_docs, mv02c_bson_text};
use super::*;

/// Serialises the connector tests that share the process-global v5
/// connector store. Parallel creates/persists export the whole global
/// into each test's own data dir, so one test's snapshot can resurrect
/// another test's deleted entry on restart (e.g. w023-del-conn reads
/// back 200 instead of 404). Holding this across the whole test keeps
/// the create/delete/persist/seed sequence atomic. No production code
/// depends on it; it only orders tests.
#[cfg(test)]
pub(crate) static CONNECTOR_TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Settle delay before a dead-target leg. Reason: a closed socket
/// becomes visible at the next poll.
#[cfg(test)]
pub(crate) const DEAD_SOCKET_SETTLE_MS: u64 = 300;

/// In-process HTTP round-trip over loopback TCP: no extra crates, no
/// daemons. The server is spawned per test and aborted at the end.
#[cfg(test)]
pub(crate) struct TestServer {
    pub(crate) port: u16,
    pub(crate) task: tokio::task::JoinHandle<()>,
    pub(crate) mqtt_task: tokio::task::JoinHandle<()>,
    pub(crate) mqtt_port: u16,
}

#[cfg(test)]
pub(crate) struct TestState {
    pub(crate) engine: Arc<RuleEngine>,
    pub(crate) sessions: Arc<SessionManager>,
    pub(crate) router: Arc<SubscriptionRouter>,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) auth: Arc<MemoryAuth>,
    pub(crate) authz_sources: Arc<broker_auth::AuthzSourceRegistry>,
    pub(crate) admin_users: Arc<crate::admin_users::AdminUsers>,
    pub(crate) tokens: Arc<crate::v5::auth::ApiTokens>,
    pub(crate) alarms: Arc<crate::v5::alarms::AlarmStore>,
    pub(crate) license: Arc<crate::v5::license::LicenseState>,
    pub(crate) mcp: Arc<crate::mcp::McpStore>,
    pub(crate) config: Arc<ConfigRegistry>,
    pub(crate) edge_rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<BrokerFrame>>>,
}

#[cfg(test)]
impl TestServer {
    #[cfg(test)]
    pub(crate) async fn start() -> (Self, TestState) {
        Self::start_with_registry(defaults_registry()).await
    }

    /// Start serving `ApiState` built on `registry`: pass a registry
    /// loaded from a temp `--data-dir` to simulate kernel restarts.
    #[cfg(test)]
    pub(crate) async fn start_with_registry(registry: Arc<ConfigRegistry>) -> (Self, TestState) {
        let engine = Arc::new(RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        let sessions = Arc::new(SessionManager::new());
        let sub_router = Arc::new(SubscriptionRouter::new());
        let metrics = Arc::new(Metrics::new());
        let auth = Arc::new(MemoryAuth::new());
        let conns = Arc::new(broker_router::ConnTable::default());
        let (edge_tx, edge_rx) = tokio::sync::mpsc::unbounded_channel::<BrokerFrame>();
        let api_state = ApiState::new(
            engine.clone(),
            sessions.clone(),
            sub_router.clone(),
            metrics.clone(),
            auth.clone(),
            conns.clone(),
            registry,
            "indra-node-1".to_string(),
            edge_tx,
        );
        let state = TestState {
            engine: engine.clone(),
            sessions: sessions.clone(),
            router: sub_router.clone(),
            metrics: metrics.clone(),
            auth: auth.clone(),
            authz_sources: api_state.authz_sources.clone(),
            admin_users: api_state.admin_users.clone(),
            tokens: api_state.tokens.clone(),
            alarms: api_state.alarms.clone(),
            license: api_state.license.clone(),
            mcp: api_state.mcp.clone(),
            config: Arc::clone(&api_state.config),
            edge_rx: Arc::new(tokio::sync::Mutex::new(edge_rx)),
        };
        let app = router(api_state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let port = listener.local_addr().expect("local addr").port();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve test app");
        });
        let (mqtt_port, mqtt_task) = mv03c_start_mqtt_ingress(engine.clone()).await;
        (
            Self {
                port,
                task,
                mqtt_task,
                mqtt_port,
            },
            state,
        )
    }

    #[cfg(test)]
    pub(crate) async fn request(&self, raw: &str) -> (u16, Value) {
        let (status, text) = self.request_raw(raw).await;
        let body: Value = if text.trim().is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text).expect("response body is JSON")
        };
        (status, body)
    }

    #[cfg(test)]
    pub(crate) async fn request_raw(&self, raw: &str) -> (u16, String) {
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", self.port))
            .await
            .expect("connect test server");
        // The server answers 413 and closes the connection during a large upload.
        // A broken pipe means the answer waits in the read buffer.
        if let Err(error) = stream.write_all(raw.as_bytes()).await {
            assert!(
                error.kind() == std::io::ErrorKind::BrokenPipe
                    || error.kind() == std::io::ErrorKind::ConnectionReset,
                "write request: {error}"
            );
        }
        // HTTP/1.0 + Connection: close => the server closes when done.
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.expect("read response");
        let text = String::from_utf8(buf).expect("response is UTF-8");
        let (head, body) = text.split_once("\r\n\r\n").expect("header/body split");
        let status: u16 = head.lines().next().expect("status line")[9..12]
            .parse()
            .expect("status code");
        (status, body.to_string())
    }

    #[cfg(test)]
    pub(crate) async fn send(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
        token: Option<&str>,
    ) -> (u16, Value) {
        let authorization = token.map(|token| format!("Bearer {token}"));
        self.send_with_authorization(method, path, body, authorization.as_deref())
            .await
    }

    /// Send a request with a complete `Authorization` header value,
    /// for example `Basic ...`.
    #[cfg(test)]
    pub(crate) async fn send_with_authorization(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
        authorization: Option<&str>,
    ) -> (u16, Value) {
        let mut head = format!("{method} {path} HTTP/1.0\r\nHost: test\r\n");
        if let Some(authorization) = authorization {
            head.push_str(&format!("Authorization: {authorization}\r\n"));
        }
        if let Some(body) = body {
            let raw_body = serde_json::to_string(&body).expect("encode body");
            head.push_str(&format!(
                    "Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{raw_body}",
                    raw_body.len()
                ));
        } else {
            head.push_str("Connection: close\r\n\r\n");
        }
        self.request(&head).await
    }

    #[cfg(test)]
    pub(crate) async fn post(&self, path: &str, body: Value) -> (u16, Value) {
        self.send("POST", path, Some(body), None).await
    }

    /// `GET` with the Basic credentials of an API key.
    #[cfg(test)]
    pub(crate) async fn get_basic(&self, path: &str, key_id: &str, secret: &str) -> (u16, Value) {
        let authorization = basic_authorization(key_id, secret);
        self.send_with_authorization("GET", path, None, Some(&authorization))
            .await
    }

    /// `POST` with the Basic credentials of an API key.
    #[cfg(test)]
    pub(crate) async fn post_basic(
        &self,
        path: &str,
        body: Value,
        key_id: &str,
        secret: &str,
    ) -> (u16, Value) {
        let authorization = basic_authorization(key_id, secret);
        self.send_with_authorization("POST", path, Some(body), Some(&authorization))
            .await
    }

    #[cfg(test)]
    pub(crate) async fn post_auth(&self, path: &str, body: Value, token: &str) -> (u16, Value) {
        self.send("POST", path, Some(body), Some(token)).await
    }

    #[cfg(test)]
    pub(crate) async fn get(&self, path: &str) -> (u16, Value) {
        self.send("GET", path, None, None).await
    }

    #[cfg(test)]
    pub(crate) async fn get_auth(&self, path: &str, token: &str) -> (u16, Value) {
        self.send("GET", path, None, Some(token)).await
    }

    #[cfg(test)]
    pub(crate) async fn put_auth(&self, path: &str, body: Value, token: &str) -> (u16, Value) {
        self.send("PUT", path, Some(body), Some(token)).await
    }

    #[cfg(test)]
    pub(crate) async fn delete_auth(&self, path: &str, token: &str) -> (u16, Value) {
        self.send("DELETE", path, None, Some(token)).await
    }

    /// Log in with the default admin and return its bearer token.
    /// The token still carries `must_change_password`; it only opens
    /// the login, logout, current_user and own-password-change routes.
    #[cfg(test)]
    pub(crate) async fn admin_token(&self) -> String {
        let (status, body) = self
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "public"}),
            )
            .await;
        assert_eq!(status, 200);
        body["token"].as_str().expect("login token").to_string()
    }

    /// Log in as `admin`, clear the default-password flag, log in
    /// again and return a fully-privileged bearer token for tests
    /// that call authenticated routes.
    #[cfg(test)]
    pub(crate) async fn login_as_admin(&self) -> String {
        let fresh = self.admin_token().await;
        let (status, _) = self
            .put_auth(
                "/api/v5/users/admin/change_pwd",
                json!({"old_pwd": "public", "new_pwd": "Adm1n-test-pass!"}),
                &fresh,
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = self
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["must_change_password"], json!(false));
        body["token"].as_str().expect("login token").to_string()
    }
}

#[cfg(test)]
pub(crate) struct NoopBrokerSink;
#[async_trait::async_trait]
#[cfg(test)]
impl broker_rules::BrokerSink for NoopBrokerSink {
    #[cfg(test)]
    async fn publish(
        &self,
        _topic: broker_protocol::Topic,
        _payload: bytes::Bytes,
        _qos: broker_protocol::QoS,
        _retain: bool,
    ) -> Result<(), broker_rules::RuleEngineError> {
        Ok(())
    }
}

/// One captured request of the MV-02b loopback fake.
#[derive(Clone)]
#[cfg(test)]
pub(crate) struct Mv02bCaptured {
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) query: String,
    pub(crate) content_type: Option<String>,
    pub(crate) authorization: Option<String>,
    pub(crate) body: Vec<u8>,
}

/// Build the `_bulk` response that a real search node returns for
/// the NDJSON request body: one `items` entry per action line.
#[cfg(test)]
pub(crate) fn mv02b_bulk_reply(body: &[u8]) -> Vec<u8> {
    let text = String::from_utf8_lossy(body);
    let mut items = Vec::new();
    let mut source_line = false;
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        if source_line {
            source_line = false;
            continue;
        }
        let Ok(action) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let Some((name, _meta)) = action.as_object().and_then(|map| map.iter().next()) else {
            continue;
        };
        if !matches!(name.as_str(), "create" | "index" | "update" | "delete") {
            continue;
        }
        let status = if name == "create" || name == "index" {
            201
        } else {
            200
        };
        let mut item = serde_json::Map::new();
        item.insert(name.clone(), json!({"status": status}));
        items.push(serde_json::Value::Object(item));
        source_line = name != "delete";
    }
    serde_json::to_vec(&json!({"took": 1, "errors": false, "items": items}))
        .expect("bulk reply encodes")
}

/// Minimal HTTP responder for the MV-02b delivery tests: one TCP
/// listener that answers every request with a fixed status and body
/// and records what arrived. Dropping the fake closes the port, so
/// the same test proves delivery, then the down-server case, then
/// delivery again after a restart.
#[cfg(test)]
pub(crate) struct Mv02bFake {
    pub(crate) port: u16,
    pub(crate) received: std::sync::Arc<std::sync::Mutex<Vec<Mv02bCaptured>>>,
    pub(crate) _task: tokio::task::JoinHandle<()>,
}

#[cfg(test)]
impl Drop for Mv02bFake {
    #[cfg(test)]
    fn drop(&mut self) {
        self._task.abort();
    }
}

#[cfg(test)]
impl Mv02bFake {
    #[cfg(test)]
    pub(crate) async fn start(status: u16, body: &'static str) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake");
        Self::serve(listener, status, body).await
    }

    /// Bind the same port again after a drop, so the delivers-after-start
    /// step dials the target the stored connector names.
    #[cfg(test)]
    pub(crate) async fn start_on(port: u16, status: u16, body: &'static str) -> Self {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("rebind fake");
        Self::serve(listener, status, body).await
    }

    #[cfg(test)]
    pub(crate) async fn serve(
        listener: tokio::net::TcpListener,
        status: u16,
        body: &'static str,
    ) -> Self {
        let port = listener.local_addr().expect("fake addr").port();
        let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let task_received = std::sync::Arc::clone(&received);
        let reply = body.as_bytes().to_vec();
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let received = std::sync::Arc::clone(&task_received);
                let mut reply = reply.clone();
                tokio::spawn(async move {
                    let mut stream = stream;
                    let mut head = Vec::new();
                    let mut buf = [0u8; 4096];
                    loop {
                        match stream.read(&mut buf).await {
                            Ok(0) => return,
                            Ok(n) => {
                                head.extend_from_slice(&buf[..n]);
                                if head.windows(4).any(|w| w == b"\r\n\r\n") {
                                    break;
                                }
                                if head.len() > 65536 {
                                    return;
                                }
                            }
                            Err(_) => return,
                        }
                    }
                    let head_end = head
                        .windows(4)
                        .position(|w| w == b"\r\n\r\n")
                        .map(|p| p + 4)
                        .unwrap_or(head.len());
                    let head_text = String::from_utf8_lossy(&head[..head_end]).to_string();
                    let mut lines = head_text.lines();
                    let request_line = lines.next().unwrap_or("").to_string();
                    let mut content_length = 0usize;
                    let mut content_type = None;
                    let mut authorization = None;
                    for line in lines {
                        if let Some((name, value)) = line.split_once(':') {
                            if name.trim().eq_ignore_ascii_case("content-length") {
                                content_length = value.trim().parse().unwrap_or(0);
                            }
                            if name.trim().eq_ignore_ascii_case("content-type") {
                                content_type = Some(value.trim().to_string());
                            }
                            if name.trim().eq_ignore_ascii_case("authorization") {
                                authorization = Some(value.trim().to_string());
                            }
                        }
                    }
                    let mut body_data = head[head_end..].to_vec();
                    while body_data.len() < content_length {
                        match stream.read(&mut buf).await {
                            Ok(0) => break,
                            Ok(n) => body_data.extend_from_slice(&buf[..n]),
                            Err(_) => break,
                        }
                    }
                    body_data.truncate(content_length);
                    let mut parts = request_line.split_whitespace();
                    let method = parts.next().unwrap_or("").to_string();
                    let target = parts.next().unwrap_or("/").to_string();
                    let (path, query) = match target.split_once('?') {
                        Some((p, q)) => (p.to_string(), q.to_string()),
                        None => (target, String::new()),
                    };
                    let body_data = body_data;
                    if path == "/_bulk" && reply.as_slice() == b"{}" {
                        reply = mv02b_bulk_reply(&body_data);
                    }
                    received.lock().expect("fake lock").push(Mv02bCaptured {
                        method,
                        path,
                        query,
                        content_type,
                        authorization,
                        body: body_data,
                    });
                    let header = format!(
                            "HTTP/1.1 {status} MV02B\r\nX-Elastic-Product: Elasticsearch\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            reply.len()
                        );
                    let mut out = header.into_bytes();
                    out.extend_from_slice(&reply);
                    let _ = stream.write_all(&out).await;
                });
            }
        });
        Self {
            port,
            received,
            _task: task,
        }
    }

    #[cfg(test)]
    pub(crate) fn requests(&self) -> Vec<Mv02bCaptured> {
        self.received.lock().expect("fake lock").clone()
    }

    /// Poll until at least `count` requests arrived (delivery runs
    /// through the action runtime, so the POST lands after the
    /// publish answers). Panics with what did arrive.
    #[cfg(test)]
    pub(crate) async fn wait_for(&self, count: usize) -> Vec<Mv02bCaptured> {
        for _ in 0..100 {
            let got = self.requests();
            if got.len() >= count {
                return got;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        panic!(
            "fake got {} requests, wanted at least {count}",
            self.requests().len()
        );
    }

    #[cfg(test)]
    pub(crate) fn body_text(request: &Mv02bCaptured) -> String {
        String::from_utf8_lossy(&request.body).to_string()
    }
}

/// One command captured from the MongoDB loopback fake.
#[cfg(feature = "mongodb")]
#[derive(Clone, Debug)]
#[cfg(test)]
pub(crate) struct Mv02cMongoCall {
    pub(crate) command: String,
    pub(crate) db: String,
    pub(crate) collection: String,
    pub(crate) documents: Vec<broker_connectors_enterprise::BsonDocument>,
}

/// Loopback MongoDB wire server. It answers HELLO, PING and insert
/// commands and records each insert's database, collection and
/// documents. Dropping it closes the port for the down-server step.
#[cfg(feature = "mongodb")]
#[cfg(test)]
pub(crate) struct Mv02cMongoFake {
    pub(crate) port: u16,
    pub(crate) calls: std::sync::Arc<std::sync::Mutex<Vec<Mv02cMongoCall>>>,
    pub(crate) connections: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    pub(crate) _task: tokio::task::JoinHandle<()>,
}

#[cfg(feature = "mongodb")]
#[cfg(test)]
impl Drop for Mv02cMongoFake {
    #[cfg(test)]
    fn drop(&mut self) {
        self._task.abort();
        for handle in self
            .connections
            .lock()
            .expect("mongo fake connections")
            .drain(..)
        {
            handle.abort();
        }
    }
}

#[cfg(feature = "mongodb")]
#[cfg(test)]
impl Mv02cMongoFake {
    #[cfg(test)]
    pub(crate) async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mongo fake");
        Self::serve(listener).await
    }

    #[cfg(test)]
    pub(crate) async fn start_on(port: u16) -> Self {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("rebind mongo fake");
        Self::serve(listener).await
    }

    #[cfg(test)]
    pub(crate) async fn serve(listener: tokio::net::TcpListener) -> Self {
        let port = listener.local_addr().expect("mongo fake addr").port();
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let connections = std::sync::Arc::new(std::sync::Mutex::new(Vec::<
            tokio::task::JoinHandle<()>,
        >::new()));
        let task_calls = std::sync::Arc::clone(&calls);
        let task_connections = std::sync::Arc::clone(&connections);
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let calls = std::sync::Arc::clone(&task_calls);
                let handle = tokio::spawn(async move {
                    loop {
                        let mut head = [0u8; 4];
                        if stream.read_exact(&mut head).await.is_err() {
                            break;
                        }
                        let total = i32::from_le_bytes(head) as usize;
                        if !(16..=48 * 1024 * 1024).contains(&total) {
                            break;
                        }
                        let mut rest = vec![0u8; total - 4];
                        if stream.read_exact(&mut rest).await.is_err() {
                            break;
                        }
                        let mut frame = head.to_vec();
                        frame.extend_from_slice(&rest);
                        let request_id =
                            i32::from_le_bytes([frame[4], frame[5], frame[6], frame[7]]);
                        let opcode =
                            i32::from_le_bytes([frame[12], frame[13], frame[14], frame[15]]);
                        let (body, sequences, reply_opcode) = if opcode == 2013 {
                            let Ok((body, sequences)) = mv02c_parse_mongo_sections(&frame) else {
                                break;
                            };
                            (body, sequences, 2013)
                        } else if opcode == 2004 {
                            let Ok(body) = mv02c_parse_mongo_query(&frame) else {
                                break;
                            };
                            (body, Vec::new(), 1)
                        } else {
                            break;
                        };
                        let command = body
                            .fields
                            .first()
                            .map(|(name, _)| name.clone())
                            .unwrap_or_default();
                        let db = body
                            .get("$db")
                            .and_then(mv02c_bson_text)
                            .unwrap_or_default();
                        let reply = match command.as_str() {
                            "hello" | "isMaster" | "ismaster" => mv02c_mongo_hello(),
                            "insert" => {
                                let collection = body
                                    .get("insert")
                                    .and_then(mv02c_bson_text)
                                    .unwrap_or_default();
                                let documents = sequences
                                    .iter()
                                    .find(|(name, _)| name == "documents")
                                    .map(|(_, docs)| docs.clone())
                                    .or_else(|| body.get("documents").and_then(mv02c_bson_docs))
                                    .unwrap_or_default();
                                let count = documents.len() as i32;
                                calls.lock().expect("mongo fake lock").push(Mv02cMongoCall {
                                    command: "insert".to_string(),
                                    db,
                                    collection,
                                    documents,
                                });
                                mv02c_mongo_ok_with_count(count)
                            }
                            "update" | "delete" => {
                                let collection = body
                                    .get(command.as_str())
                                    .and_then(mv02c_bson_text)
                                    .unwrap_or_default();
                                calls.lock().expect("mongo fake lock").push(Mv02cMongoCall {
                                    command: command.clone(),
                                    db,
                                    collection,
                                    documents: Vec::new(),
                                });
                                mv02c_mongo_ok_with_count(1)
                            }
                            _ => mv02c_mongo_ok(),
                        };
                        let out = if reply_opcode == 1 {
                            mv02c_encode_mongo_reply(&reply, request_id)
                        } else {
                            let mut out = broker_connectors_enterprise::encode_op_msg(&reply, 0);
                            out[8..12].copy_from_slice(&request_id.to_le_bytes());
                            out
                        };
                        if stream.write_all(&out).await.is_err() {
                            break;
                        }
                    }
                });
                task_connections
                    .lock()
                    .expect("mongo fake connections")
                    .push(handle);
            }
        });
        Self {
            port,
            calls,
            connections,
            _task: task,
        }
    }

    #[cfg(test)]
    pub(crate) async fn wait_for(&self, count: usize) -> Vec<Mv02cMongoCall> {
        for _ in 0..300 {
            let got = self.calls.lock().expect("mongo fake lock").clone();
            if got.len() >= count {
                return got;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        panic!(
            "mongo fake got {} insert calls, wanted at least {count}",
            self.calls.lock().expect("mongo fake lock").len()
        );
    }
}

#[cfg(feature = "mongodb")]
#[cfg(test)]
pub(crate) type Mv02cMongoSections = (
    broker_connectors_enterprise::BsonDocument,
    Vec<(String, Vec<broker_connectors_enterprise::BsonDocument>)>,
);

#[cfg(feature = "mongodb")]
#[cfg(test)]
pub(crate) fn mv02c_parse_mongo_sections(frame: &[u8]) -> Result<Mv02cMongoSections, ()> {
    use broker_connectors_enterprise::BsonDocument;
    if frame.len() < 21 {
        return Err(());
    }
    let mut body = BsonDocument::new();
    let mut sequences = Vec::new();
    let mut at = 20usize;
    while at < frame.len() {
        let kind = frame[at];
        at += 1;
        match kind {
            0 => {
                if frame.len() < at + 4 {
                    return Err(());
                }
                let len =
                    i32::from_le_bytes([frame[at], frame[at + 1], frame[at + 2], frame[at + 3]])
                        as usize;
                if len < 5 || frame.len() < at + len {
                    return Err(());
                }
                body = BsonDocument::decode(&frame[at..at + len]).map_err(|_| ())?;
                at += len;
            }
            1 => {
                if frame.len() < at + 4 {
                    return Err(());
                }
                let size =
                    i32::from_le_bytes([frame[at], frame[at + 1], frame[at + 2], frame[at + 3]])
                        as usize;
                let end = at + size;
                if size < 5 || frame.len() < end {
                    return Err(());
                }
                at += 4;
                let Some(name_end) = frame[at..end].iter().position(|byte| *byte == 0) else {
                    return Err(());
                };
                let name = String::from_utf8_lossy(&frame[at..at + name_end]).to_string();
                at += name_end + 1;
                let mut docs = Vec::new();
                while at < end {
                    let len = i32::from_le_bytes([
                        frame[at],
                        frame[at + 1],
                        frame[at + 2],
                        frame[at + 3],
                    ]) as usize;
                    if len < 5 || end < at + len {
                        return Err(());
                    }
                    docs.push(BsonDocument::decode(&frame[at..at + len]).map_err(|_| ())?);
                    at += len;
                }
                sequences.push((name, docs));
            }
            _ => return Err(()),
        }
    }
    Ok((body, sequences))
}

#[cfg(feature = "mongodb")]
#[cfg(test)]
pub(crate) fn mv02c_parse_mongo_query(
    frame: &[u8],
) -> Result<broker_connectors_enterprise::BsonDocument, ()> {
    use broker_connectors_enterprise::BsonDocument;
    let mut at = 20usize;
    let Some(name_end) = frame
        .get(at..)
        .and_then(|rest| rest.iter().position(|b| *b == 0))
    else {
        return Err(());
    };
    at += name_end + 1;
    at += 8;
    if frame.len() < at + 5 {
        return Err(());
    }
    BsonDocument::decode(&frame[at..]).map_err(|_| ())
}

#[cfg(feature = "mongodb")]
#[cfg(test)]
pub(crate) fn mv02c_encode_mongo_reply(
    reply: &broker_connectors_enterprise::BsonDocument,
    request_id: i32,
) -> Vec<u8> {
    let document = reply.encode();
    let total = 16 + 4 + 8 + 4 + 4 + document.len();
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&(total as i32).to_le_bytes());
    out.extend_from_slice(&0i32.to_le_bytes());
    out.extend_from_slice(&request_id.to_le_bytes());
    out.extend_from_slice(&1i32.to_le_bytes());
    out.extend_from_slice(&0i32.to_le_bytes());
    out.extend_from_slice(&0i64.to_le_bytes());
    out.extend_from_slice(&0i32.to_le_bytes());
    out.extend_from_slice(&1i32.to_le_bytes());
    out.extend_from_slice(&document);
    out
}

#[cfg(feature = "mongodb")]
#[cfg(test)]
pub(crate) fn mv02c_mongo_ok() -> broker_connectors_enterprise::BsonDocument {
    use broker_connectors_enterprise::{BsonDocument, BsonValue};
    let mut doc = BsonDocument::new();
    doc.fields.push(("ok".to_string(), BsonValue::Double(1.0)));
    doc
}

#[cfg(feature = "mongodb")]
#[cfg(test)]
pub(crate) fn mv02c_mongo_ok_with_count(count: i32) -> broker_connectors_enterprise::BsonDocument {
    use broker_connectors_enterprise::BsonValue;
    let mut doc = mv02c_mongo_ok();
    doc.fields.push(("n".to_string(), BsonValue::Int32(count)));
    doc
}

#[cfg(feature = "mongodb")]
#[cfg(test)]
pub(crate) fn mv02c_mongo_hello() -> broker_connectors_enterprise::BsonDocument {
    use broker_connectors_enterprise::{BsonDocument, BsonValue};
    let mut doc = BsonDocument::new();
    doc.fields.push(("ok".to_string(), BsonValue::Double(1.0)));
    for key in ["ismaster", "isWritablePrimary"] {
        doc.fields.push((key.to_string(), BsonValue::Bool(true)));
    }
    doc.fields
        .push(("maxWireVersion".to_string(), BsonValue::Int32(21)));
    doc.fields
        .push(("minWireVersion".to_string(), BsonValue::Int32(0)));
    doc.fields.push((
        "maxBsonObjectSize".to_string(),
        BsonValue::Int32(16_777_216),
    ));
    doc.fields.push((
        "maxMessageSizeBytes".to_string(),
        BsonValue::Int32(48_000_000),
    ));
    doc.fields
        .push(("maxWriteBatchSize".to_string(), BsonValue::Int32(100_000)));
    doc.fields.push((
        "logicalSessionTimeoutMinutes".to_string(),
        BsonValue::Int32(30),
    ));
    doc.fields
        .push(("connectionId".to_string(), BsonValue::Int32(1)));
    doc.fields
        .push(("readOnly".to_string(), BsonValue::Bool(false)));
    doc.fields
        .push(("compression".to_string(), BsonValue::Array(Vec::new())));
    doc
}

/// Loopback Couchbase server for the query path. It answers the
/// memdx bootstrap (HELLO, SASL PLAIN and SCRAM-SHA512, cluster
/// config, select bucket) and the `query/service` HTTP request on the
/// same port.
#[cfg(feature = "couchbase")]
#[cfg(test)]
pub(crate) struct Mv02cCouchFake {
    pub(crate) port: u16,
    pub(crate) queries: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    pub(crate) connections: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    pub(crate) _task: tokio::task::JoinHandle<()>,
}

#[cfg(feature = "couchbase")]
#[cfg(test)]
impl Drop for Mv02cCouchFake {
    #[cfg(test)]
    fn drop(&mut self) {
        self._task.abort();
        for handle in self
            .connections
            .lock()
            .expect("couch fake connections")
            .drain(..)
        {
            handle.abort();
        }
    }
}

#[cfg(feature = "couchbase")]
#[cfg(test)]
impl Mv02cCouchFake {
    #[cfg(test)]
    pub(crate) async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind couch fake");
        Self::serve(listener).await
    }

    #[cfg(test)]
    pub(crate) async fn start_on(port: u16) -> Self {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("rebind couch fake");
        Self::serve(listener).await
    }

    #[cfg(test)]
    pub(crate) async fn serve(listener: tokio::net::TcpListener) -> Self {
        let port = listener.local_addr().expect("couch fake addr").port();
        let queries = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let connections = std::sync::Arc::new(std::sync::Mutex::new(Vec::<
            tokio::task::JoinHandle<()>,
        >::new()));
        let task_queries = std::sync::Arc::clone(&queries);
        let task_connections = std::sync::Arc::clone(&connections);
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let queries = std::sync::Arc::clone(&task_queries);
                let config = mv02c_couch_config(port);
                let handle = tokio::spawn(async move {
                    let mut first = [0u8; 1];
                    if stream.peek(&mut first).await.is_err() {
                        return;
                    }
                    if first[0] == 0x80 {
                        let mut scram: Option<(String, String)> = None;
                        loop {
                            let mut header = [0u8; 24];
                            if stream.read_exact(&mut header).await.is_err() {
                                break;
                            }
                            let total =
                                u32::from_be_bytes([header[8], header[9], header[10], header[11]])
                                    as usize;
                            let opaque = u32::from_be_bytes([
                                header[12], header[13], header[14], header[15],
                            ]);
                            let mut body = vec![0u8; total];
                            if stream.read_exact(&mut body).await.is_err() {
                                break;
                            }
                            let opcode = header[1];
                            let key_len = u16::from_be_bytes([header[2], header[3]]) as usize;
                            let ext_len = header[4] as usize;
                            let key_end = ext_len.saturating_add(key_len).min(body.len());
                            let key = body.get(ext_len..key_end).unwrap_or(&[]);
                            let value = body.get(key_end..).unwrap_or(&[]);
                            let (status, reply) = match opcode {
                                0x1f => (0u16, Vec::new()),
                                0x20 => {
                                    // The driver reads this list as
                                    // space-separated text.
                                    (0, b"PLAIN SCRAM-SHA512".to_vec())
                                }
                                0x21 => {
                                    // The fake verifies the PLAIN token
                                    // the probe sends, and starts SCRAM
                                    // for the driver. A wrong user or
                                    // password gets AUTH_ERROR.
                                    if key == b"PLAIN".as_slice() {
                                        if value == b"\0u\0p".as_slice() {
                                            (0, Vec::new())
                                        } else {
                                            (0x20, Vec::new())
                                        }
                                    } else if key.starts_with(b"SCRAM-SHA".as_slice()) {
                                        match mv02c_scram_server_first(value) {
                                            Some((bare, server_first, _)) => {
                                                scram = Some((bare, server_first.clone()));
                                                (0x21, server_first.into_bytes())
                                            }
                                            None => (0x20, Vec::new()),
                                        }
                                    } else {
                                        (0x20, Vec::new())
                                    }
                                }
                                0x22 => {
                                    let ok = scram.take().is_some_and(|(bare, first)| {
                                        std::str::from_utf8(value).ok().is_some_and(|text| {
                                            mv02c_scram_proof_ok(&bare, &first, text)
                                        })
                                    });
                                    if ok {
                                        (0, Vec::new())
                                    } else {
                                        (0x20, Vec::new())
                                    }
                                }
                                0x89 => (0, Vec::new()),
                                0xb5 => (0, config.as_bytes().to_vec()),
                                _ => (0, Vec::new()),
                            };
                            let out = mv02c_couch_response(opcode, opaque, status, &reply);
                            if stream.write_all(&out).await.is_err() {
                                break;
                            }
                        }
                    } else {
                        mv02c_couch_http(&mut stream, &queries, port).await;
                    }
                });
                task_connections
                    .lock()
                    .expect("couch fake connections")
                    .push(handle);
            }
        });
        Self {
            port,
            queries,
            connections,
            _task: task,
        }
    }

    #[cfg(test)]
    pub(crate) async fn wait_for(&self, count: usize) -> Vec<String> {
        for _ in 0..300 {
            let got = self.queries.lock().expect("couch fake lock").clone();
            if got.len() >= count {
                return got;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        panic!(
            "couch fake got {} queries, wanted at least {count}",
            self.queries.lock().expect("couch fake lock").len()
        );
    }
}

#[cfg(feature = "couchbase")]
#[cfg(test)]
pub(crate) fn mv02c_couch_response(opcode: u8, opaque: u32, status: u16, value: &[u8]) -> Vec<u8> {
    let mut out = vec![0x81, opcode];
    out.extend_from_slice(&0u16.to_be_bytes());
    out.push(0);
    out.push(0);
    out.extend_from_slice(&status.to_be_bytes());
    out.extend_from_slice(&(value.len() as u32).to_be_bytes());
    out.extend_from_slice(&opaque.to_be_bytes());
    out.extend_from_slice(&0u64.to_be_bytes());
    out.extend_from_slice(value);
    out
}

/// First SCRAM-SHA512 server message for the fake user `u`. Returns
/// the client-first bare text, the server-first text and the nonce.
#[cfg(feature = "couchbase")]
#[cfg(test)]
pub(crate) fn mv02c_scram_server_first(client_first: &[u8]) -> Option<(String, String, String)> {
    use base64::Engine as _;
    let text = std::str::from_utf8(client_first).ok()?;
    let bare = text.strip_prefix("n,,")?;
    let mut user = None;
    let mut nonce = None;
    for part in bare.split(',') {
        if let Some(value) = part.strip_prefix("n=") {
            user = Some(value);
        }
        if let Some(value) = part.strip_prefix("r=") {
            nonce = Some(value);
        }
    }
    if user != Some("u") {
        return None;
    }
    let nonce = nonce?.to_string();
    let server_nonce = format!("{nonce}server");
    let salt = base64::engine::general_purpose::STANDARD.encode(b"mv02c-scram-salt");
    let server_first = format!("r={server_nonce},s={salt},i=4096");
    Some((bare.to_string(), server_first, server_nonce))
}

/// True when the client-final SCRAM proof matches password `p`.
#[cfg(feature = "couchbase")]
#[cfg(test)]
pub(crate) fn mv02c_scram_proof_ok(
    client_first_bare: &str,
    server_first: &str,
    client_final: &str,
) -> bool {
    use base64::Engine as _;
    use hmac::{Hmac, Mac};
    use sha2::{Digest, Sha512};
    let Some((without_proof, proof)) = client_final.rsplit_once(",p=") else {
        return false;
    };
    let Ok(proof) = base64::engine::general_purpose::STANDARD.decode(proof) else {
        return false;
    };
    let Some(salt) = server_first
        .split(',')
        .find_map(|field| field.strip_prefix("s="))
        .and_then(|text| base64::engine::general_purpose::STANDARD.decode(text).ok())
    else {
        return false;
    };
    let iterations = server_first
        .split(',')
        .find_map(|field| field.strip_prefix("i="))
        .and_then(|text| text.parse::<u32>().ok())
        .unwrap_or(4096);
    let hmac = |key: &[u8], data: &[u8]| {
        let mut mac = Hmac::<Sha512>::new_from_slice(key).expect("HMAC accepts any key length");
        mac.update(data);
        mac.finalize().into_bytes().to_vec()
    };
    let mut block = salt;
    block.extend_from_slice(&1u32.to_be_bytes());
    let mut u = hmac(b"p".as_slice(), &block);
    let mut salted = u.clone();
    for _ in 1..iterations {
        u = hmac(b"p".as_slice(), &u);
        for (left, right) in salted.iter_mut().zip(u.iter()) {
            *left ^= right;
        }
    }
    let client_key = hmac(&salted, b"Client Key".as_slice());
    let stored_key = Sha512::digest(&client_key).to_vec();
    let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
    let client_signature = hmac(&stored_key, auth_message.as_bytes());
    if proof.len() != client_signature.len() {
        return false;
    }
    let recovered: Vec<u8> = proof
        .iter()
        .zip(client_signature.iter())
        .map(|(left, right)| left ^ right)
        .collect();
    Sha512::digest(&recovered).to_vec() == stored_key
}

#[cfg(feature = "couchbase")]
#[cfg(test)]
pub(crate) fn mv02c_couch_config(port: u16) -> String {
    serde_json::json!({
        "rev": 1,
        "revEpoch": 0,
        "clusterName": "mv02c-fake",
        "clusterUUID": "00000000000000000000000000000000",
        "nodeLocator": "vbucket",
        "nodes": [{
            "hostname": "127.0.0.1",
            "ports": {
                "direct": port,
                "http": port,
                "n1ql": port,
                "ssl": 0
            }
        }],
        "nodesExt": [{
            "hostname": "127.0.0.1",
            "services": {
                "kv": port,
                "kvSSL": 0,
                "mgmt": port,
                "mgmtSSL": 0,
                "n1ql": port,
                "n1qlSSL": 0,
                "capi": port,
                "capiSSL": 0,
                "fts": 0,
                "ftsSSL": 0,
                "indexAdmin": 0,
                "indexScan": 0,
                "indexHttp": 0,
                "indexHttps": 0,
                "cbas": 0,
                "cbasSSL": 0,
                "eventingAdminPort": 0,
                "eventingSSL": 0,
                "backupAPI": 0,
                "backupAPIHTTPS": 0
            }
        }],
        "bucketCapabilitiesVer": "",
        "bucketCapabilities": [
            "cbhello", "touch", "couchapi", "ccc", "xattr", "dcp", "cboy",
            "collections", "durableWrite", "tombstonedUserXAttrs", "subdoc",
            "subdoc.ReplicaRead", "subdoc.BinaryNoDocid", "subdoc.CreateAsDeleted",
            "subdoc.ReplaceBodyWithXattr", "subdoc.ReviveDocument"
        ],
        "clusterCapabilitiesVer": [1, 0],
        "clusterCapabilities": {}
    })
    .to_string()
}

#[cfg(feature = "couchbase")]
#[cfg(test)]
pub(crate) async fn mv02c_couch_http(
    stream: &mut tokio::net::TcpStream,
    queries: &std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    port: u16,
) {
    let mut head = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match stream.read(&mut buf).await {
            Ok(0) => return,
            Ok(n) => {
                head.extend_from_slice(&buf[..n]);
                if head.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
                if head.len() > 65536 {
                    return;
                }
            }
            Err(_) => return,
        }
    }
    let head_end = head
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|at| at + 4)
        .unwrap_or(head.len());
    let head_text = String::from_utf8_lossy(&head[..head_end]).to_string();
    let request_path = head_text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/")
        .to_string();
    let mut content_length = 0usize;
    for line in head_text.lines() {
        let lower = line.to_ascii_lowercase();
        if let Some(value) = lower.strip_prefix("content-length:") {
            content_length = value.trim().parse().unwrap_or(0);
        }
    }
    let mut body = head[head_end..].to_vec();
    while body.len() < content_length {
        match stream.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => body.extend_from_slice(&buf[..n]),
            Err(_) => break,
        }
    }
    body.truncate(content_length);
    if !body.is_empty() {
        queries
            .lock()
            .expect("couch fake lock")
            .push(String::from_utf8_lossy(&body).to_string());
    }
    let reply = if request_path.contains("query") {
        serde_json::json!({
            "requestID": "mv02c-fake",
            "signature": null,
            "results": [],
            "status": "success",
            "metrics": {
                "elapsedTime": "1ms",
                "executionTime": "1ms",
                "resultCount": 0,
                "resultSize": 0
            },
            "errors": []
        })
        .to_string()
    } else if request_path == "/pools" || request_path.starts_with("/pools?") {
        serde_json::json!({
            "implementationVersion": "7.0.0",
            "pools": [{"name": "default", "uri": "/pools/default"}]
        })
        .to_string()
    } else if request_path.contains("pools/default") {
        mv02c_couch_config(port)
    } else {
        "{}".to_string()
    };
    let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            reply.len()
        );
    let mut out = header.into_bytes();
    out.extend_from_slice(reply.as_bytes());
    let _ = stream.write_all(&out).await;
}

/// Poll one action's metrics until `success` reaches `want`.
#[cfg(test)]
pub(crate) async fn mv02b_wait_success(server: &TestServer, token: &str, action: &str, want: u64) {
    for _ in 0..300 {
        let (status, metrics) = server
            .get_auth(&format!("/api/v5/actions/{action}/metrics"), token)
            .await;
        assert_eq!(status, 200);
        if metrics["metrics"]["success"].as_u64().unwrap_or(0) >= want {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("action {action} never reached success {want}");
}

/// Publish one JSON event through the management publish entry,
/// which drives the stored rule path.
#[cfg(test)]
pub(crate) async fn mv02b_publish(server: &TestServer, token: &str, topic: &str, payload: &str) {
    mv02b_publish_qos(server, token, topic, payload, 0).await;
}

/// Publish one JSON event with an explicit QoS through the management
/// publish entry, which drives the stored rule path.
#[cfg(test)]
pub(crate) async fn mv02b_publish_qos(
    server: &TestServer,
    token: &str,
    topic: &str,
    payload: &str,
    qos: u8,
) {
    let (status, _) = server
        .post_auth(
            "/api/v5/publish",
            json!({"topic": topic, "payload": payload, "qos": qos}),
            token,
        )
        .await;
    assert_eq!(status, 202, "publish drives the rule path");
}

/// Read one MQTT variable-byte integer from the ingress socket.
#[cfg(test)]
pub(crate) async fn mv03c_mqtt_read_varint(stream: &mut tokio::net::TcpStream) -> Option<usize> {
    let mut value = 0usize;
    let mut shift = 0u32;
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).await.ok()?;
        value |= ((byte[0] & 0x7F) as usize) << shift;
        if byte[0] & 0x80 == 0 {
            return Some(value);
        }
        shift += 7;
        if shift > 21 {
            return None;
        }
    }
}

/// Serve one raw MQTT ingress connection. Each QoS 0 PUBLISH
/// drives the stored rule path through `dispatch_ingress`.
#[cfg(test)]
pub(crate) async fn mv03c_handle_mqtt_conn(
    mut stream: tokio::net::TcpStream,
    engine: Arc<RuleEngine>,
) {
    let mut fixed = [0u8; 1];
    if stream.read_exact(&mut fixed).await.is_err() {
        return;
    }
    if fixed[0] >> 4 != 1 {
        return;
    }
    let Some(len) = mv03c_mqtt_read_varint(&mut stream).await else {
        return;
    };
    let mut body = vec![0u8; len];
    if stream.read_exact(&mut body).await.is_err() {
        return;
    }
    if stream.write_all(&[0x20, 0x02, 0x00, 0x00]).await.is_err() {
        return;
    }
    loop {
        let mut head = [0u8; 1];
        if stream.read_exact(&mut head).await.is_err() {
            return;
        }
        match head[0] >> 4 {
            3 => {
                let Some(len) = mv03c_mqtt_read_varint(&mut stream).await else {
                    return;
                };
                let mut rest = vec![0u8; len];
                if stream.read_exact(&mut rest).await.is_err() {
                    return;
                }
                if rest.len() < 2 {
                    continue;
                }
                let topic_len = u16::from_be_bytes([rest[0], rest[1]]) as usize;
                if rest.len() < 2 + topic_len {
                    continue;
                }
                let topic = String::from_utf8_lossy(&rest[2..2 + topic_len]).to_string();
                let qos = (head[0] & 0x06) >> 1;
                let (packet_id, payload) = if qos == 0 {
                    (0u16, rest[2 + topic_len..].to_vec())
                } else {
                    if rest.len() < 2 + topic_len + 2 {
                        continue;
                    }
                    let pid = u16::from_be_bytes([rest[2 + topic_len], rest[3 + topic_len]]);
                    (pid, rest[4 + topic_len..].to_vec())
                };
                if let Ok(parsed) = Topic::new(topic) {
                    let sink: Arc<dyn broker_rules::BrokerSink> = Arc::new(NoopBrokerSink);
                    let _ = engine
                        .dispatch_ingress(
                            &parsed,
                            &bytes::Bytes::from(payload),
                            QoS::AtMostOnce,
                            &sink,
                        )
                        .await;
                }
                if qos == 1
                    && stream
                        .write_all(&[0x40, 0x02, (packet_id >> 8) as u8, (packet_id & 0xFF) as u8])
                        .await
                        .is_err()
                {
                    return;
                }
            }
            12 => {
                if stream.write_all(&[0xD0, 0x00]).await.is_err() {
                    return;
                }
            }
            14 => return,
            _ => return,
        }
    }
}

/// Start a raw MQTT listener for through-broker tests. Each
/// PUBLISH on it runs the rule engine. Returns the port and task handle.
#[cfg(test)]
pub(crate) async fn mv03c_start_mqtt_ingress(
    engine: Arc<RuleEngine>,
) -> (u16, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mqtt ingress");
    let port = listener.local_addr().expect("mqtt addr").port();
    let handle = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let engine = Arc::clone(&engine);
            tokio::spawn(mv03c_handle_mqtt_conn(stream, engine));
        }
    });
    (port, handle)
}

/// Poll one action's metrics until `failed` reaches `want`.
/// Exact equality: a duplicate failure count fails instead of passing.
#[cfg(test)]
pub(crate) async fn mv03a_wait_failed(server: &TestServer, token: &str, action: &str, want: u64) {
    for _ in 0..400 {
        let (status, metrics) = server
            .get_auth(&format!("/api/v5/actions/{action}/metrics"), token)
            .await;
        assert_eq!(status, 200);
        if metrics["metrics"]["failed"].as_u64().unwrap_or(0) == want {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("action {action} never reached failed {want}");
}

/// The `Authorization: Basic` value for an API key.
#[cfg(test)]
pub(crate) fn basic_authorization(key_id: &str, secret: &str) -> String {
    use base64::Engine;
    let encoded = base64::engine::general_purpose::STANDARD.encode(format!("{key_id}:{secret}"));
    format!("Basic {encoded}")
}

/// Makes a key through the API and returns the identifier and the
/// secret.
#[cfg(test)]
pub(crate) async fn make_key(server: &TestServer, token: &str, body: Value) -> (String, String) {
    let (status, created) = server.post_auth("/api/v5/api_key", body, token).await;
    assert_eq!(status, 200, "create: {created}");
    (
        created["api_key"].as_str().expect("identifier").to_string(),
        created["api_secret"].as_str().expect("secret").to_string(),
    )
}

#[cfg(test)]
impl Drop for TestServer {
    #[cfg(test)]
    fn drop(&mut self) {
        self.task.abort();
        self.mqtt_task.abort();
    }
}

#[cfg(test)]
pub(crate) fn read_backup_file(path: &std::path::Path) -> Vec<(String, String)> {
    let bytes = std::fs::read(path).expect("backup file reads");
    let gz = flate2::read::GzDecoder::new(bytes.as_slice());
    let mut archive = tar::Archive::new(gz);
    let mut out = Vec::new();
    for entry in archive.entries().expect("backup lists") {
        let mut entry = entry.expect("backup entry reads");
        let name = entry
            .path()
            .expect("entry path")
            .to_string_lossy()
            .into_owned();
        let mut text = String::new();
        std::io::Read::read_to_string(&mut entry, &mut text).expect("entry text reads");
        out.push((name, text));
    }
    out
}

/// Unique scratch data dir under the OS temp dir (no dev-dependency).
#[cfg(test)]
pub(crate) fn unique_data_dir(prefix: &str) -> std::path::PathBuf {
    #[cfg(test)]
    pub(crate) static COUNTER: AtomicU64 = AtomicU64::new(0);
    let slot = COUNTER.fetch_add(1, Ordering::SeqCst);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "indramqtt-w020-datadir-{prefix}-{}-{nanos}-{slot}",
        std::process::id()
    ))
}

/// Minimal WebSocket client over raw TCP (no extra deps): HTTP
/// upgrade with the RFC 6455 example key, masked binary sends, and
/// server frame parsing.
#[cfg(test)]
pub(crate) struct WsClient {
    pub(crate) stream: tokio::net::TcpStream,
    pub(crate) buf: Vec<u8>,
}

#[cfg(test)]
impl WsClient {
    #[cfg(test)]
    pub(crate) async fn connect(port: u16) -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connect ws");
        // Fixed RFC example key -> fixed accept digest.
        let req = "GET /ws/mqtt HTTP/1.1\r\nHost: test\r\nUpgrade: websocket\r\n\
                       Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                       Sec-WebSocket-Version: 13\r\n\r\n";
        stream
            .write_all(req.as_bytes())
            .await
            .expect("write upgrade");
        let mut buf = Vec::new();
        loop {
            let mut chunk = [0u8; 512];
            let n = stream.read(&mut chunk).await.expect("read upgrade");
            assert!(n > 0, "upgrade response ended early");
            buf.extend_from_slice(&chunk[..n]);
            if let Some(pos) = find_crlf2(&buf) {
                let head = String::from_utf8_lossy(&buf[..pos + 4]).to_string();
                assert!(
                    head.starts_with("HTTP/1.1 101"),
                    "expected 101, got: {head}"
                );
                assert!(head.contains("s3pPLMBiTxaQ9kYGzzhZRbK+xOo="));
                let rest = buf[pos + 4..].to_vec();
                return Self { stream, buf: rest };
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn send_bin(&mut self, payload: &[u8]) {
        use tokio::io::AsyncWriteExt;
        let mut frame = vec![0x82u8];
        if payload.len() < 126 {
            frame.push(payload.len() as u8 | 0x80);
        } else if payload.len() < 65536 {
            frame.push(126 | 0x80);
            frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        } else {
            frame.push(127 | 0x80);
            frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        }
        let mask = [0x11u8, 0x22, 0x33, 0x44];
        frame.extend_from_slice(&mask);
        for (i, byte) in payload.iter().enumerate() {
            frame.push(byte ^ mask[i % 4]);
        }
        self.stream.write_all(&frame).await.expect("write frame");
    }

    /// Next server message payload (`None` on clean EOF).
    #[cfg(test)]
    pub(crate) async fn recv_msg(&mut self) -> Option<Vec<u8>> {
        use tokio::io::AsyncReadExt;
        loop {
            if let Some((payload, consumed)) = parse_ws_frame(&self.buf) {
                self.buf.drain(..consumed);
                return Some(payload);
            }
            let mut chunk = [0u8; 4096];
            match self.stream.read(&mut chunk).await.expect("read frame") {
                0 => return None,
                n => self.buf.extend_from_slice(&chunk[..n]),
            }
        }
    }
}

#[cfg(test)]
pub(crate) fn find_crlf2(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

/// Parse one server frame (unmasked): `(payload, consumed)` or `None`
/// when incomplete. Close frames surface as empty payloads.
#[cfg(test)]
pub(crate) fn parse_ws_frame(buf: &[u8]) -> Option<(Vec<u8>, usize)> {
    if buf.len() < 2 || buf[1] & 0x80 != 0 {
        return None;
    }
    let opcode = buf[0] & 0x0F;
    let mut header = 2usize;
    let mut length = (buf[1] & 0x7F) as usize;
    if length == 126 {
        if buf.len() < 4 {
            return None;
        }
        length = u16::from_be_bytes([buf[2], buf[3]]) as usize;
        header = 4;
    } else if length == 127 {
        if buf.len() < 10 {
            return None;
        }
        length = u64::from_be_bytes(buf[2..10].try_into().ok()?) as usize;
        header = 10;
    }
    if buf.len() < header + length {
        return None;
    }
    if opcode == 0x8 {
        return Some((Vec::new(), header + length));
    }
    if opcode != 0x1 && opcode != 0x2 {
        return None;
    }
    Some((buf[header..header + length].to_vec(), header + length))
}

// -- MQTT packet builders (mirror of the dashboard console) --

#[cfg(test)]
pub(crate) fn mqtt_remaining(mut n: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = (n % 128) as u8;
        n /= 128;
        if n > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if n == 0 {
            break;
        }
    }
    out
}

#[cfg(test)]
pub(crate) fn mqtt_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

#[cfg(test)]
pub(crate) fn mqtt_connect(
    client_id: &str,
    username: Option<&str>,
    password: Option<&[u8]>,
) -> Vec<u8> {
    let mut body = vec![0, 4, b'M', b'Q', b'T', b'T', 4];
    let mut flags = 0x02u8;
    if username.is_some() {
        flags |= 0x80;
    }
    if password.is_some() {
        flags |= 0x40;
    }
    body.push(flags);
    body.extend_from_slice(&60u16.to_be_bytes());
    mqtt_str(&mut body, client_id);
    if let Some(user) = username {
        mqtt_str(&mut body, user);
    }
    if let Some(pass) = password {
        body.extend_from_slice(&(pass.len() as u16).to_be_bytes());
        body.extend_from_slice(pass);
    }
    let mut packet = vec![0x10];
    packet.extend_from_slice(&mqtt_remaining(body.len()));
    packet.extend_from_slice(&body);
    packet
}

#[cfg(test)]
pub(crate) fn mqtt_subscribe(pid: u16, subs: &[(&str, u8)]) -> Vec<u8> {
    let mut body = pid.to_be_bytes().to_vec();
    for (filter, qos) in subs {
        mqtt_str(&mut body, filter);
        body.push(*qos);
    }
    let mut packet = vec![0x82];
    packet.extend_from_slice(&mqtt_remaining(body.len()));
    packet.extend_from_slice(&body);
    packet
}

#[cfg(test)]
pub(crate) fn mqtt_publish(topic: &str, pid: u16, qos: u8, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    mqtt_str(&mut body, topic);
    if qos > 0 {
        body.extend_from_slice(&pid.to_be_bytes());
    }
    body.extend_from_slice(payload);
    let mut packet = vec![0x30 | (qos << 1)];
    packet.extend_from_slice(&mqtt_remaining(body.len()));
    packet.extend_from_slice(&body);
    packet
}

#[cfg(test)]
pub(crate) fn mqtt_packet_type(packet: &[u8]) -> u8 {
    packet[0] >> 4
}

/// Spawn a counting fake HTTP target for RO-01 batch tests.
/// Counts one hit per request with a body and answers 200.
/// Keeps the loop in one place so each batch test stays small.
#[cfg(test)]
pub(crate) async fn spawn_ro_counting_http_target() -> (
    std::net::SocketAddr,
    std::sync::Arc<std::sync::atomic::AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake HTTP target");
    let address = listener.local_addr().expect("fake HTTP address");
    let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = std::sync::Arc::clone(&hits);
    let receiver = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let mut request = Vec::new();
            let mut chunk = [0u8; 2048];
            let body_start = loop {
                let Ok(read) = socket.read(&mut chunk).await else {
                    break None;
                };
                if read == 0 {
                    break None;
                }
                request.extend_from_slice(&chunk[..read]);
                if let Some(pos) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                    break Some(pos + 4);
                }
            };
            if let Some(start) = body_start {
                let headers = String::from_utf8_lossy(&request[..start]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .and_then(|value| value.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                while request.len() < start + length {
                    let Ok(read) = socket.read(&mut chunk).await else {
                        break;
                    };
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&chunk[..read]);
                }
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            let _ = socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .await;
        }
    });
    (address, hits, receiver)
}

/// Build one minimal backup file in memory for import tests: META
/// plus the given parts under one base dir, gzipped and tarred.
#[cfg(test)]
pub(crate) fn build_test_backup(base: &str, roots: &[&str], parts: &[(&str, Value)]) -> Vec<u8> {
    build_test_backup_with_tables(base, roots, &[], parts)
}

/// Build one backup file with explicit tables. Roots map to
/// `snapshot/<root>.json` parts; tables map to `tables/<table>.json`.
/// Callers pass full relative paths in `parts`.
#[cfg(test)]
pub(crate) fn build_test_backup_with_tables(
    base: &str,
    roots: &[&str],
    tables: &[&str],
    parts: &[(&str, Value)],
) -> Vec<u8> {
    let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let mut tar = tar::Builder::new(encoder);
    let meta = json!({
        "format": "indra-backup-1",
        "version": "test",
        "exported_at": "2026-01-01T00:00:00+00:00",
        "roots": roots,
        "tables": tables,
    });
    let mut append = |path: String, value: &Value| {
        let bytes = serde_json::to_vec(value).expect("part is JSON");
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        tar.append_data(&mut header, path, bytes.as_slice())
            .expect("entry appends");
    };
    append(format!("{base}/META.json"), &meta);
    for (path, value) in parts {
        append(format!("{base}/{path}"), value);
    }
    let encoder = tar.into_inner().expect("tar closes");
    encoder.finish().expect("gzip closes")
}

/// Send one JSON-RPC message to `/mcp` with an `Authorization` value.
#[cfg(test)]
pub(crate) async fn mcp_rpc(
    server: &TestServer,
    authorization: &str,
    payload: Value,
) -> (u16, Value) {
    server
        .send_with_authorization("POST", "/mcp", Some(payload), Some(authorization))
        .await
}
