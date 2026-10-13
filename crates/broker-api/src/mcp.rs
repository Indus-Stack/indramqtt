//! Built-in MCP endpoint on the management listener.
//!
//! Serves `POST /mcp` with the Streamable HTTP transport and JSON
//! responses. The handler speaks the JSON-RPC 2.0 subset that the task
//! needs: `initialize`, `ping`, `tools/list` and `tools/call`. An
//! unknown method answers `-32601`. The endpoint carries one built-in
//! tool: `broker_status`.
//!
//! Authentication uses the same API key path as the management API
//! ([`crate::api_auth`]): Basic (`api_key:api_secret`) or Bearer
//! (`api_secret`, operator key). A dashboard session token never
//! authenticates here. A missing or wrong credential answers HTTP 401
//! with a plain error object, never a JSON-RPC body.
//!
//! Configuration comes from `[mcp]` (`enabled`, `read_only`). The
//! kernel shares one [`McpStore`] into `ApiState`, so reads observe
//! the boot values without a restart. A disabled path answers 404.
//! Read-only mode still serves `broker_status` and refuses every
//! other tool call.
//!
//! Store bounds (stated here and enforced below):
//! - the store holds two atomic flags, no map and no queue;
//! - one request body is at most [`MAX_MCP_BODY_BYTES`] bytes;
//! - per-request work is one parse plus one small JSON build;
//! - publish and deliver never touch this store.

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::admin_users::AdminRole;
use crate::v5::api_keys::{KeyDecision, KeyGrant};
use crate::ApiState;

/// Protocol version the server declares in `initialize`.
/// Reason: the reference specification defines no MCP surface, so the
/// server declares its own version and the negotiation answers it.
pub const MCP_PROTOCOL_VERSION: &str = "2025-06-18";

/// Largest accepted `POST /mcp` body in bytes.
/// Reason: a JSON-RPC control request is small; 64 KiB covers tool
/// arguments while one request stays bounded.
pub const MAX_MCP_BODY_BYTES: usize = 64 * 1024;

/// JSON-RPC parse error.
const PARSE_ERROR: i64 = -32700;
/// JSON-RPC invalid request.
const INVALID_REQUEST: i64 = -32600;
/// JSON-RPC unknown method.
const METHOD_NOT_FOUND: i64 = -32601;
/// JSON-RPC bad params or unknown tool.
const INVALID_PARAMS: i64 = -32602;

/// Name of the one built-in tool of this task.
const BROKER_STATUS_TOOL: &str = "broker_status";

/// Live MCP settings shared between the kernel and the handler.
///
/// The kernel builds one instance from the boot configuration and
/// shares it into `ApiState`, so the handler reads the file values.
/// Two atomics keep reads lock-free on the management plane.
pub struct McpStore {
    enabled: AtomicBool,
    read_only: AtomicBool,
}

impl McpStore {
    /// Store with the schema defaults: on, read-write.
    pub fn new() -> Self {
        Self {
            enabled: AtomicBool::new(true),
            read_only: AtomicBool::new(false),
        }
    }

    /// Store seeded from the boot configuration.
    pub fn from_config(conf: &broker_config::schema::McpConf) -> Self {
        Self {
            enabled: AtomicBool::new(conf.enabled),
            read_only: AtomicBool::new(conf.read_only),
        }
    }

    /// True while the path serves requests.
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// True while mutating tool calls are refused.
    pub fn is_read_only(&self) -> bool {
        self.read_only.load(Ordering::Relaxed)
    }

    /// Replace the enabled flag. Tests use this; the kernel sets it once
    /// at boot from the file configuration.
    pub fn set_enabled(&self, value: bool) {
        self.enabled.store(value, Ordering::Relaxed);
    }

    /// Replace the read-only flag. Tests use this; the kernel sets it
    /// once at boot from the file configuration.
    pub fn set_read_only(&self, value: bool) {
        self.read_only.store(value, Ordering::Relaxed);
    }
}

impl Default for McpStore {
    fn default() -> Self {
        Self::new()
    }
}

/// True for tools that read only. Only `broker_status` exists, and it
/// reads only, so read-only mode keeps serving it.
fn is_read_only_tool(name: &str) -> bool {
    name == BROKER_STATUS_TOOL
}

/// Identity that may call the endpoint: an operator key or a scoped
/// API key with its grant and role.
enum McpIdentity {
    Operator,
    Key { grant: KeyGrant, role: AdminRole },
}

/// Resolve an API key through the management key path and refuse the
/// dashboard session token. Returns the identity or `None` when the
/// request carries no usable API key.
fn resolve_mcp_identity(state: &ApiState, headers: &HeaderMap) -> Option<McpIdentity> {
    match crate::api_auth::parse_credentials(headers)? {
        crate::api_auth::Credentials::Basic { username, secret } => state
            .api_keys
            .resolve_basic(&username, &secret)
            .map(|key| McpIdentity::Key {
                grant: key.grant.clone(),
                role: key_role(&key.grant),
            }),
        crate::api_auth::Credentials::Bearer(token) => {
            if crate::v5::auth::authenticated_user(state, headers).is_some() {
                return None;
            }
            if let Some(key) = state.api_keys.resolve(&token) {
                return Some(McpIdentity::Key {
                    grant: key.grant.clone(),
                    role: key_role(&key.grant),
                });
            }
            if state.operator_api_keys.resolve(&token).is_some() {
                return Some(McpIdentity::Operator);
            }
            None
        }
    }
}

/// Map a key role to the administrator or viewer role of the users.
fn key_role(grant: &KeyGrant) -> AdminRole {
    match grant.role {
        crate::v5::api_keys::KeyRole::Viewer => AdminRole::Viewer,
        crate::v5::api_keys::KeyRole::Administrator | crate::v5::api_keys::KeyRole::Publisher => {
            AdminRole::Administrator
        }
    }
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({ "code": "UNAUTHORIZED", "message": "missing or invalid credentials" })),
    )
        .into_response()
}

fn forbidden(message: &str) -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(serde_json::json!({ "code": "FORBIDDEN", "message": message })),
    )
        .into_response()
}

fn disabled() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({ "code": "NOT_FOUND", "message": "mcp endpoint is disabled" })),
    )
        .into_response()
}

/// Build one JSON-RPC success envelope with the request id preserved.
fn success(id: &serde_json::Value, result: serde_json::Value) -> serde_json::Value {
    serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

/// Build one JSON-RPC error envelope with the request id preserved.
fn failure(id: &serde_json::Value, code: i64, message: &str) -> serde_json::Value {
    serde_json::json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

/// Read the broker status from the live node: build version, clock
/// uptime and local node name. No constant stands in for a value.
fn broker_status_value() -> serde_json::Value {
    serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_seconds": crate::v5::nodes::uptime_ms() / 1000,
        "node": crate::node_scope::local_node_name(),
    })
}

/// Describe the one built-in tool for `tools/list`.
fn broker_status_tool() -> serde_json::Value {
    serde_json::json!({
        "name": BROKER_STATUS_TOOL,
        "description": "Return the broker version, uptime and node name.",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "additionalProperties": false,
        },
    })
}

/// Answer one `tools/call` params object. Returns the result value or
/// the JSON-RPC error value. Read-only mode refuses every tool except
/// `broker_status` before the tool lookup runs.
fn call_tool(
    state: &ApiState,
    identity: &McpIdentity,
    id: &serde_json::Value,
    params: &serde_json::Value,
) -> serde_json::Value {
    let name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
    if name.is_empty() {
        return failure(id, INVALID_PARAMS, "tools/call needs a tool name");
    }
    if state.mcp.is_read_only() && !is_read_only_tool(name) {
        return failure(id, INVALID_REQUEST, "read-only mode refuses mutating tools");
    }
    if matches!(
        identity,
        McpIdentity::Key {
            role: AdminRole::Viewer,
            ..
        }
    ) && !is_read_only_tool(name)
    {
        return failure(id, INVALID_REQUEST, "viewer keys call read-only tools only");
    }
    if name != BROKER_STATUS_TOOL {
        return failure(id, INVALID_PARAMS, "unknown tool");
    }
    let text = match serde_json::to_string(&broker_status_value()) {
        Ok(text) => text,
        Err(_) => "{}".to_string(),
    };
    success(
        id,
        serde_json::json!({
            "content": [{ "type": "text", "text": text }],
            "isError": false,
        }),
    )
}

/// Answer one JSON-RPC request object. Returns `None` for a
/// notification (no id), which gets no response body.
fn dispatch_one(
    state: &ApiState,
    identity: &McpIdentity,
    request: &serde_json::Value,
) -> Option<serde_json::Value> {
    let id = request.get("id")?.clone();
    if request.get("jsonrpc").and_then(|v| v.as_str()) != Some("2.0") {
        return Some(failure(
            &id,
            INVALID_REQUEST,
            "request must be JSON-RPC 2.0",
        ));
    }
    let method = request.get("method").and_then(|v| v.as_str()).unwrap_or("");
    let params = request
        .get("params")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let result = match method {
        "initialize" => {
            let _client_version = params
                .get("protocolVersion")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            success(
                &id,
                serde_json::json!({
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": { "tools": {} },
                    "serverInfo": {
                        "name": "indramqtt",
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                }),
            )
        }
        "ping" => success(&id, serde_json::json!({})),
        "tools/list" => success(&id, serde_json::json!({ "tools": [broker_status_tool()] })),
        "tools/call" => call_tool(state, identity, &id, &params),
        _ => failure(&id, METHOD_NOT_FOUND, "unknown method"),
    };
    Some(result)
}

/// `POST /mcp`: Streamable HTTP transport with JSON responses.
///
/// The route lives outside the management auth layer on purpose: the
/// handler runs its own API key check and refuses dashboard tokens.
/// A disabled path answers 404 before authentication runs.
pub async fn post_mcp(State(state): State<ApiState>, headers: HeaderMap, body: Bytes) -> Response {
    if !state.mcp.is_enabled() {
        return disabled();
    }
    let Some(identity) = resolve_mcp_identity(&state, &headers) else {
        return unauthorized();
    };
    if let McpIdentity::Key { grant, .. } = &identity {
        match crate::v5::api_keys::key_decision(grant, "/mcp") {
            KeyDecision::Allowed => {}
            KeyDecision::LoginOnly => {
                return forbidden("this operation needs an API key");
            }
            KeyDecision::Forbidden => {
                return forbidden("this API key has no right to call /mcp");
            }
        }
    }
    if body.len() > MAX_MCP_BODY_BYTES {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(serde_json::json!({ "code": "PAYLOAD_TOO_LARGE", "message": "mcp request is too large" })),
        )
            .into_response();
    }
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": serde_json::Value::Null,
                    "error": { "code": PARSE_ERROR, "message": "body is not JSON" },
                })),
            )
                .into_response();
        }
    };
    if let Some(requests) = value.as_array() {
        if requests.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                Json(failure(
                    &serde_json::Value::Null,
                    INVALID_REQUEST,
                    "batch must not be empty",
                )),
            )
                .into_response();
        }
        let mut answers = Vec::new();
        for request in requests {
            if let Some(answer) = dispatch_one(&state, &identity, request) {
                answers.push(answer);
            }
        }
        if answers.is_empty() {
            return StatusCode::ACCEPTED.into_response();
        }
        return (StatusCode::OK, Json(serde_json::Value::Array(answers))).into_response();
    }
    let Some(object) = value.as_object() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(failure(
                &serde_json::Value::Null,
                INVALID_REQUEST,
                "body must be a JSON-RPC object",
            )),
        )
            .into_response();
    };
    let request = serde_json::Value::Object(object.clone());
    match dispatch_one(&state, &identity, &request) {
        Some(answer) => (StatusCode::OK, Json(answer)).into_response(),
        None => StatusCode::ACCEPTED.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct Harness {
        port: u16,
        task: tokio::task::JoinHandle<()>,
        state: ApiState,
        key_id: String,
        secret: String,
    }

    impl Harness {
        async fn start() -> Self {
            Self::start_with_mcp(None).await
        }

        async fn start_with_mcp(mcp: Option<McpStore>) -> Self {
            let engine = Arc::new(broker_rules::RuleEngine::new(
                16,
                broker_rules::BackpressurePolicy::DropOldest,
            ));
            let state = ApiState::standalone(engine);
            if let Some(mcp) = mcp {
                state.mcp.set_enabled(mcp.is_enabled());
                state.mcp.set_read_only(mcp.is_read_only());
            }
            let (record, secret) = state
                .api_keys
                .create(crate::v5::api_keys::NewApiKey::new("mcp-test"))
                .expect("mint succeeds");
            let key_id = record
                .to_json()
                .get("api_key")
                .and_then(|v| v.as_str())
                .expect("identifier renders")
                .to_string();
            let app = crate::router(state.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind test server");
            let port = listener.local_addr().expect("local addr").port();
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.expect("serve test app");
            });
            Self {
                port,
                task,
                state,
                key_id,
                secret,
            }
        }

        fn bearer(&self) -> String {
            format!("Bearer {}", self.secret)
        }

        fn basic(&self) -> String {
            use base64::Engine;
            let raw = format!("{}:{}", self.key_id, self.secret);
            let encoded = base64::engine::general_purpose::STANDARD.encode(raw);
            format!("Basic {encoded}")
        }

        async fn call(
            &self,
            auth: Option<&str>,
            body: &serde_json::Value,
        ) -> (u16, serde_json::Value) {
            let raw = serde_json::to_string(body).expect("encode body");
            let mut head = format!(
                "POST /mcp HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
                raw.len()
            );
            if let Some(auth) = auth {
                head.push_str(&format!("Authorization: {auth}\r\n"));
            }
            head.push_str(&format!("\r\n{raw}"));
            let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", self.port))
                .await
                .expect("connect test server");
            stream.write_all(head.as_bytes()).await.expect("write");
            let mut buf = Vec::new();
            stream.read_to_end(&mut buf).await.expect("read");
            let text = String::from_utf8(buf).expect("UTF-8");
            let (head, body) = text.split_once("\r\n\r\n").expect("header/body split");
            let status: u16 = head.lines().next().expect("status")[9..12]
                .parse()
                .expect("code");
            let body: serde_json::Value = if body.trim().is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::from_str(body).expect("JSON body")
            };
            (status, body)
        }

        async fn rpc(
            &self,
            auth: Option<&str>,
            id: i64,
            method: &str,
            params: serde_json::Value,
        ) -> (u16, serde_json::Value) {
            self.call(
                auth,
                &serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": method,
                    "params": params,
                }),
            )
            .await
        }
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn unique_dir(prefix: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!("mcp-{prefix}-{}-{nanos}", std::process::id()))
    }

    async fn harness_from_toml(text: &str) -> (Harness, std::path::PathBuf) {
        let dir = unique_dir("file");
        std::fs::create_dir_all(&dir).expect("create config dir");
        std::fs::write(dir.join("indra.toml"), text).expect("write config");
        // Reason: the build host exports `INDRA_BUILD_*` compile facts;
        // an empty env keeps the file load free of ambient variables.
        let layered = broker_config::load_layered_with_env(
            &dir,
            &[],
            &broker_config::CliOverrides::default(),
        )
        .expect("file loads");
        let store = McpStore::from_config(&layered.config().mcp);
        let harness = Harness::start_with_mcp(Some(store)).await;
        (harness, dir)
    }

    #[tokio::test]
    async fn initialize_lists_and_calls_broker_status_over_http() {
        let harness = Harness::start().await;
        let auth = harness.bearer();
        let (status, body) = harness
            .rpc(
                Some(&auth),
                1,
                "initialize",
                serde_json::json!({
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": { "name": "test", "version": "0" },
                }),
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(
            body["result"]["protocolVersion"],
            serde_json::json!(MCP_PROTOCOL_VERSION)
        );
        assert_eq!(
            body["result"]["serverInfo"]["name"],
            serde_json::json!("indramqtt")
        );

        let (status, body) = harness
            .rpc(Some(&auth), 2, "tools/list", serde_json::json!({}))
            .await;
        assert_eq!(status, 200);
        let tools = body["result"]["tools"].as_array().expect("tools list");
        assert!(tools
            .iter()
            .any(|t| t["name"] == serde_json::json!("broker_status")));

        let (status, body) = harness
            .rpc(
                Some(&auth),
                3,
                "tools/call",
                serde_json::json!({ "name": "broker_status", "arguments": {} }),
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["error"], serde_json::Value::Null);
        let content = body["result"]["content"].as_array().expect("content");
        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], serde_json::json!("text"));
        let text = content[0]["text"].as_str().expect("text");
        let status_value: serde_json::Value = serde_json::from_str(text).expect("status JSON");
        assert_eq!(
            status_value["version"],
            serde_json::json!(env!("CARGO_PKG_VERSION"))
        );
        assert!(status_value["uptime_seconds"].is_u64());
        assert_eq!(
            status_value["node"],
            serde_json::json!(crate::node_scope::local_node_name())
        );
    }

    #[tokio::test]
    async fn ping_answers_an_empty_result() {
        let harness = Harness::start().await;
        let auth = harness.bearer();
        let (status, body) = harness
            .rpc(Some(&auth), 1, "ping", serde_json::json!({}))
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["result"], serde_json::json!({}));
    }

    #[tokio::test]
    async fn basic_key_authenticates_the_same_path() {
        let harness = Harness::start().await;
        let auth = harness.basic();
        let (status, body) = harness
            .rpc(Some(&auth), 1, "ping", serde_json::json!({}))
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["result"], serde_json::json!({}));
        let (status, _) = harness
            .rpc(
                Some(&auth),
                2,
                "tools/call",
                serde_json::json!({ "name": "broker_status" }),
            )
            .await;
        assert_eq!(status, 200);
    }

    #[tokio::test]
    async fn missing_and_wrong_keys_get_401_without_rpc_body() {
        let harness = Harness::start().await;
        let payload = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "ping", "params": {},
        });
        let (status, body) = harness.call(None, &payload).await;
        assert_eq!(status, 401);
        assert!(body.get("jsonrpc").is_none());
        assert!(body.get("result").is_none());
        assert_eq!(body["code"], serde_json::json!("UNAUTHORIZED"));

        let (status, body) = harness.call(Some("Bearer wrong-secret"), &payload).await;
        assert_eq!(status, 401);
        assert!(body.get("jsonrpc").is_none());
        assert_eq!(body["code"], serde_json::json!("UNAUTHORIZED"));
    }

    #[tokio::test]
    async fn dashboard_token_is_refused() {
        let harness = Harness::start().await;
        let token = harness
            .state
            .tokens
            .issue("admin", crate::admin_users::AdminRole::Administrator);
        let auth = format!("Bearer {token}");
        let (status, body) = harness
            .rpc(Some(&auth), 1, "ping", serde_json::json!({}))
            .await;
        assert_eq!(status, 401);
        assert!(body.get("jsonrpc").is_none());
    }

    #[tokio::test]
    async fn disabled_path_answers_404() {
        let store = McpStore::new();
        store.set_enabled(false);
        let harness = Harness::start_with_mcp(Some(store)).await;
        let auth = harness.bearer();
        let (status, _) = harness
            .rpc(Some(&auth), 1, "ping", serde_json::json!({}))
            .await;
        assert_eq!(status, 404);
    }

    #[tokio::test]
    async fn file_with_disabled_mcp_answers_404_over_http() {
        let (harness, dir) = harness_from_toml("[mcp]\nenabled = false\n").await;
        let auth = harness.bearer();
        let (status, _) = harness
            .rpc(Some(&auth), 1, "ping", serde_json::json!({}))
            .await;
        assert_eq!(status, 404);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn file_with_read_only_mcp_keeps_status_over_http() {
        let (harness, dir) = harness_from_toml("[mcp]\nread_only = true\n").await;
        let auth = harness.bearer();
        let (status, body) = harness
            .rpc(
                Some(&auth),
                1,
                "tools/call",
                serde_json::json!({ "name": "broker_status" }),
            )
            .await;
        assert_eq!(status, 200);
        assert!(body["result"]["content"].is_array());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn read_only_blocks_mutating_calls_but_keeps_status() {
        let store = McpStore::new();
        store.set_read_only(true);
        let harness = Harness::start_with_mcp(Some(store)).await;
        let auth = harness.bearer();
        let (status, body) = harness
            .rpc(
                Some(&auth),
                1,
                "tools/call",
                serde_json::json!({ "name": "broker_status" }),
            )
            .await;
        assert_eq!(status, 200);
        assert!(body["result"]["content"].is_array());

        let (status, body) = harness
            .rpc(
                Some(&auth),
                2,
                "tools/call",
                serde_json::json!({ "name": "broker_restart" }),
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["error"]["code"], serde_json::json!(INVALID_REQUEST));

        let open = Harness::start().await;
        let open_auth = open.bearer();
        let (status, body) = open
            .rpc(
                Some(&open_auth),
                2,
                "tools/call",
                serde_json::json!({ "name": "broker_restart" }),
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["error"]["code"], serde_json::json!(INVALID_PARAMS));
    }

    #[tokio::test]
    async fn unknown_method_gets_method_not_found() {
        let harness = Harness::start().await;
        let auth = harness.bearer();
        let (status, body) = harness
            .rpc(Some(&auth), 1, "nope/method", serde_json::json!({}))
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["error"]["code"], serde_json::json!(METHOD_NOT_FOUND));
    }
}
