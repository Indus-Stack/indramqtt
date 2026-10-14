//! MCP server on the management listener (`POST /mcp`).
//!
//! The server speaks the Streamable HTTP transport of the Model Context
//! Protocol with JSON responses: `initialize`, `ping`, `tools/list`
//! and `tools/call`. It authenticates API keys through the same code
//! path as the management API (`api_auth`) and refuses dashboard login
//! tokens: an agent authenticates with a key. A key needs the
//! `monitoring` scope for `/mcp` itself. Each tool call then re-enters
//! the live axum router in process with the same `Authorization`
//! header. Per-operation scopes and the viewer rule apply exactly as
//! over HTTP. One request body holds one JSON-RPC object or one batch
//! array. Management-plane only; publish and deliver never touch this
//! module.

pub mod catalog;
pub mod route_scan;

use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::api_auth::{parse_credentials, Credentials};
use crate::v5::api_keys::{KeyDecision, KeyGrant};
use crate::ApiState;

use catalog::{build_catalog, list_entry_for, tool_by_name, ToolDef};

/// Largest `/mcp` request body in bytes (8 MiB).
/// Reason: one operator call stays far below the disk-backed store
/// caps, while base64 file arguments still fit. Larger payloads use
/// the HTTP route directly.
pub const MCP_MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

/// Largest tool response body kept in memory in bytes (8 MiB).
/// Reason: a tool answer stays small enough for an agent context;
/// larger files stay available over the HTTP route directly.
pub const MCP_MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// Protocol versions this server speaks, newest first.
/// Reason: `initialize` names the newest version the server speaks;
/// a client that asks for an older known version gets that version
/// back, so older clients keep working.
const PROTOCOL_VERSIONS: [&str; 3] = ["2025-06-18", "2025-03-26", "2024-11-05"];

/// Server name in `initialize`.
const SERVER_NAME: &str = "indramqtt-mcp";

/// Runtime switch for `POST /mcp`.
///
/// Both flags default to the schema defaults: enabled, writes allowed.
/// The kernel shares one instance into `ApiState` from the boot
/// configuration, and each test builds its own through `new`, so tests
/// stay isolated. Boot drives it from `mcp.enabled` and `mcp.read_only`
/// through [`McpStore::apply`] or [`McpStore::from_config`].
pub struct McpStore {
    enabled: AtomicBool,
    read_only: AtomicBool,
}

impl McpStore {
    /// New switch: enabled, writes allowed.
    /// Reason: the defaults keep the endpoint open.
    pub fn new() -> Self {
        Self {
            enabled: AtomicBool::new(true),
            read_only: AtomicBool::new(false),
        }
    }

    /// Switch seeded from the boot configuration.
    /// Reason: the handler reads the file values without a restart.
    pub fn from_config(conf: &broker_config::schema::McpConf) -> Self {
        Self {
            enabled: AtomicBool::new(conf.enabled),
            read_only: AtomicBool::new(conf.read_only),
        }
    }

    /// Current enabled value. One relaxed load per `/mcp` request.
    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// True while the path serves requests. Alias of [`McpStore::enabled`].
    pub fn is_enabled(&self) -> bool {
        self.enabled()
    }

    /// Current read-only value. One relaxed load per tool call.
    pub fn read_only(&self) -> bool {
        self.read_only.load(Ordering::Relaxed)
    }

    /// True while mutating tool calls are refused. Alias of [`McpStore::read_only`].
    pub fn is_read_only(&self) -> bool {
        self.read_only()
    }

    /// Apply the resolved `mcp` section. Boot calls this once with the
    /// layered configuration; a disabled endpoint answers 404 and a
    /// read-only endpoint refuses every tool that writes.
    pub fn apply(&self, enabled: bool, read_only: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
        self.read_only.store(read_only, Ordering::Relaxed);
    }

    /// Replace the enabled flag. Tests use this; boot uses `apply` or
    /// `from_config` with the file configuration.
    pub fn set_enabled(&self, value: bool) {
        self.enabled.store(value, Ordering::Relaxed);
    }

    /// Replace the read-only flag. Tests use this; boot uses `apply`
    /// or `from_config` with the file configuration.
    pub fn set_read_only(&self, value: bool) {
        self.read_only.store(value, Ordering::Relaxed);
    }
}

impl Default for McpStore {
    fn default() -> Self {
        Self::new()
    }
}

/// One JSON-RPC 2.0 message. Fields stay optional so a malformed
/// message gives `-32600`, never a 500.
#[derive(Debug, Deserialize)]
struct RpcCall {
    #[serde(default)]
    jsonrpc: Option<String>,
    #[serde(default)]
    method: Option<String>,
    #[serde(default)]
    params: Option<Value>,
    #[serde(default)]
    id: Option<Value>,
}

/// `POST /mcp`: the MCP endpoint.
///
/// Disabled answers 404 before authentication runs. Missing or wrong
/// credentials answer 401 with the management error shape, never a
/// JSON-RPC body. A key without the `monitoring` scope answers 403.
/// Oversize bodies answer 413. Anything else answers 200 with a
/// JSON-RPC response. A notification answers 202 with an empty body.
pub async fn post_mcp(State(state): State<ApiState>, headers: HeaderMap, body: Bytes) -> Response {
    if !state.mcp.enabled() {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "code": "NOT_FOUND",
                "message": "the MCP endpoint is disabled",
            })),
        )
            .into_response();
    }
    if body.len() > MCP_MAX_BODY_BYTES {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(serde_json::json!({
                "code": "PAYLOAD_TOO_LARGE",
                "message": "the MCP body passes the 8 MiB limit",
            })),
        )
            .into_response();
    }
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    match resolve_mcp_identity(&state, &headers) {
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({
                    "code": "UNAUTHORIZED",
                    "message": "missing or invalid API key",
                })),
            )
                .into_response();
        }
        Some(McpIdentity::Key { grant }) => match crate::v5::api_keys::key_decision(&grant, "/mcp")
        {
            KeyDecision::Allowed => {}
            KeyDecision::LoginOnly => {
                return (
                    StatusCode::FORBIDDEN,
                    Json(serde_json::json!({
                        "code": "FORBIDDEN",
                        "message": "this operation needs an API key",
                    })),
                )
                    .into_response();
            }
            KeyDecision::Forbidden => {
                return (
                    StatusCode::FORBIDDEN,
                    Json(serde_json::json!({
                        "code": "FORBIDDEN",
                        "message": "this API key has no right to call /mcp",
                    })),
                )
                    .into_response();
            }
        },
        Some(McpIdentity::Operator) => {}
    }
    let value: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => {
            return rpc_status(
                StatusCode::BAD_REQUEST,
                Value::Null,
                rpc_error(-32700, "the body is not JSON"),
            )
        }
    };
    if let Some(requests) = value.as_array() {
        if requests.is_empty() {
            return rpc_status(
                StatusCode::BAD_REQUEST,
                Value::Null,
                rpc_error(-32600, "batch must not be empty"),
            );
        }
        let mut answers = Vec::new();
        for request in requests {
            let call: RpcCall = match serde_json::from_value(request.clone()) {
                Ok(call) => call,
                Err(_) => {
                    return rpc_status(
                        StatusCode::BAD_REQUEST,
                        Value::Null,
                        rpc_error(-32600, "the body is not a JSON-RPC call"),
                    )
                }
            };
            if let Some(answer) = serve_call_value(&state, authorization.clone(), call).await {
                answers.push(answer);
            }
        }
        if answers.is_empty() {
            return StatusCode::ACCEPTED.into_response();
        }
        return (StatusCode::OK, Json(Value::Array(answers))).into_response();
    }
    let call: RpcCall = match &value {
        Value::Object(_) => match serde_json::from_value(value.clone()) {
            Ok(call) => call,
            Err(_) => {
                return rpc_status(
                    StatusCode::BAD_REQUEST,
                    Value::Null,
                    rpc_error(-32600, "the body is not a JSON-RPC call"),
                )
            }
        },
        _ => {
            return rpc_status(
                StatusCode::BAD_REQUEST,
                Value::Null,
                rpc_error(-32600, "the body is not a JSON-RPC call"),
            )
        }
    };
    serve_call(&state, authorization, call).await
}

/// Identity that may call the endpoint: an operator key or a scoped
/// API key with its grant.
enum McpIdentity {
    Operator,
    Key { grant: KeyGrant },
}

/// Resolve an API key through the management key path and refuse the
/// dashboard session token. Returns the identity or `None` when the
/// request carries no usable API key.
fn resolve_mcp_identity(state: &ApiState, headers: &HeaderMap) -> Option<McpIdentity> {
    match parse_credentials(headers)? {
        Credentials::Bearer(token) => {
            if crate::v5::auth::authenticated_user(state, headers).is_some() {
                return None;
            }
            if let Some(key) = state.api_keys.resolve(&token) {
                return Some(McpIdentity::Key {
                    grant: key.grant.clone(),
                });
            }
            if state.operator_api_keys.resolve(&token).is_some() {
                return Some(McpIdentity::Operator);
            }
            None
        }
        Credentials::Basic { username, secret } => state
            .api_keys
            .resolve_basic(&username, &secret)
            .map(|key| McpIdentity::Key {
                grant: key.grant.clone(),
            }),
    }
}

/// Answer one parsed call. Notifications (no id, or a `notifications/`
/// method) run without an answer body.
async fn serve_call(state: &ApiState, authorization: Option<String>, call: RpcCall) -> Response {
    match serve_call_value(state, authorization, call).await {
        Some(answer) => (StatusCode::OK, Json(answer)).into_response(),
        None => StatusCode::ACCEPTED.into_response(),
    }
}

/// Answer one parsed call as a value. Returns `None` for a
/// notification (no id, or a `notifications/` method), which gets no
/// response body. A malformed envelope still answers, so the caller
/// always has a body for requests with an id.
async fn serve_call_value(
    state: &ApiState,
    authorization: Option<String>,
    call: RpcCall,
) -> Option<Value> {
    if call.jsonrpc.as_deref() != Some("2.0") {
        return Some(with_envelope(
            &call.id,
            rpc_error(-32600, "jsonrpc must be 2.0"),
        ));
    }
    let method = call.method.as_deref().unwrap_or_default().to_string();
    if method.starts_with("notifications/") {
        return None;
    }
    // Any other notification: accept, answer nothing.
    call.id.as_ref()?;
    let answer = match method.as_str() {
        "initialize" => {
            let asked = call
                .params
                .as_ref()
                .and_then(|params| params.get("protocolVersion"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            let version = negotiate_version(asked);
            with_envelope(
                &call.id,
                serde_json::json!({"result": {
                    "protocolVersion": version,
                    "capabilities": {"tools": {}},
                    "serverInfo": {
                        "name": SERVER_NAME,
                        "version": env!("CARGO_PKG_VERSION"),
                    },
                }}),
            )
        }
        "ping" => with_envelope(&call.id, serde_json::json!({"result": {}})),
        "tools/list" => {
            let tools: Vec<Value> = build_catalog().iter().map(list_entry_for).collect();
            with_envelope(&call.id, serde_json::json!({"result": {"tools": tools}}))
        }
        "tools/call" => {
            let result = call_tool(state, authorization, call.params).await;
            with_envelope(&call.id, result)
        }
        _ => with_envelope(&call.id, rpc_error(-32601, "unknown method")),
    };
    Some(answer)
}

/// Pick the protocol version for `initialize`: the asked version when
/// known, else the newest this server speaks.
fn negotiate_version(asked: &str) -> &'static str {
    PROTOCOL_VERSIONS
        .into_iter()
        .find(|version| *version == asked)
        .unwrap_or(PROTOCOL_VERSIONS[0])
}

/// Wrap a result or error body with `jsonrpc` and `id` as a value.
/// Shared by single answers and batch answers.
fn with_envelope(id: &Option<Value>, mut body: Value) -> Value {
    if let Some(object) = body.as_object_mut() {
        object.insert("jsonrpc".to_string(), Value::String("2.0".to_string()));
        object.insert("id".to_string(), id.clone().unwrap_or(Value::Null));
    }
    body
}

/// Same, with a non-200 HTTP status (malformed envelopes only).
fn rpc_status(status: StatusCode, id: Value, mut body: Value) -> Response {
    if let Some(object) = body.as_object_mut() {
        object.insert("jsonrpc".to_string(), Value::String("2.0".to_string()));
        object.insert("id".to_string(), id);
    }
    (status, Json(body)).into_response()
}

/// One JSON-RPC error body.
fn rpc_error(code: i32, message: &str) -> Value {
    serde_json::json!({"error": {"code": code, "message": message}})
}

/// Run one `tools/call`: find the tool, build the HTTP request from
/// the arguments, run the live router in process, and shape the
/// answer. Returns a `result` body with `content` (and `isError` for
/// HTTP failures) or an `error` body for bad tool use.
async fn call_tool(
    state: &ApiState,
    authorization: Option<String>,
    params: Option<Value>,
) -> Value {
    let params = params.unwrap_or(Value::Null);
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let arguments = params.get("arguments").cloned().unwrap_or(Value::Null);
    if name.is_empty() || (!arguments.is_object() && !arguments.is_null()) {
        return rpc_error(-32602, "tools/call needs a name and an arguments object");
    }
    let arguments = if arguments.is_null() {
        serde_json::Map::new()
    } else {
        arguments.as_object().cloned().unwrap_or_default()
    };
    let catalog = build_catalog();
    let tool = match tool_by_name(&catalog, name) {
        Some(tool) => tool.clone(),
        None => {
            if is_excluded_tool_name(name) {
                return rpc_error(-32602, "this operation has no tool");
            }
            return rpc_error(-32602, "unknown tool");
        }
    };
    if state.mcp.read_only() && !tool.read_only {
        return serde_json::json!({"result": {
            "content": [{"type": "text", "text": "FORBIDDEN: the MCP endpoint is read-only"}],
            "isError": true,
        }});
    }
    let request = match tool_request(&tool, &arguments) {
        Ok(request) => request,
        Err(message) => return rpc_error(-32602, &message),
    };
    let mut request = request;
    if let Some(authorization) = authorization {
        if let Ok(value) = authorization.parse() {
            request.headers_mut().insert("authorization", value);
        }
    }
    use tower::ServiceExt as _;
    // The outer handler already caps the MCP body at 8 MiB.
    // Disable the default 2 MiB cap for the inner dispatch,
    // so a body up to 8 MiB reaches the handler.
    let app = crate::router(state.clone()).layer(axum::extract::DefaultBodyLimit::disable());
    let response = match app.oneshot(request).await {
        Ok(response) => response,
        Err(never) => match never {},
    };
    tool_result(response).await
}

/// True when `name` is the tool name of an excluded operation.
/// Used only for the error text of `tools/call`.
fn is_excluded_tool_name(name: &str) -> bool {
    catalog::EXCLUSIONS
        .iter()
        .any(|item| route_scan::tool_name_for(item.method, item.path) == name)
}

/// Build the inner HTTP request for a tool call. Path segments fill
/// from same-named arguments with percent-encoding; `query` becomes
/// the query string; `body` becomes the JSON body and `body_base64`
/// the raw bytes.
fn tool_request(
    tool: &ToolDef,
    arguments: &serde_json::Map<String, Value>,
) -> Result<axum::http::Request<Body>, String> {
    let mut segments: Vec<String> = Vec::new();
    for segment in tool.path.split('/') {
        if let Some(param) = segment.strip_prefix(':') {
            let value = arguments
                .get(param)
                .ok_or_else(|| format!("missing path parameter: {param}"))?;
            segments.push(encode_part(&scalar_text(value, param)?));
        } else if let Some(param) = segment.strip_prefix('*') {
            let value = arguments
                .get(param)
                .ok_or_else(|| format!("missing path parameter: {param}"))?;
            let text = scalar_text(value, param)?;
            let encoded = text
                .split('/')
                .map(encode_part)
                .collect::<Vec<_>>()
                .join("/");
            segments.push(encoded);
        } else {
            segments.push(segment.to_string());
        }
    }
    let mut uri = segments.join("/");
    if let Some(query) = arguments.get("query") {
        let query = query
            .as_object()
            .ok_or_else(|| "query must be an object".to_string())?;
        if !query.is_empty() {
            uri.push('?');
            uri.push_str(&encode_query(query)?);
        }
    }
    let has_body = arguments.contains_key("body");
    let has_base64 = arguments.contains_key("body_base64");
    if has_body && has_base64 {
        return Err("use either body or body_base64, never both".to_string());
    }
    let method: Method = tool
        .method
        .parse()
        .map_err(|_| format!("the tool has a bad method: {}", tool.method))?;
    let mut builder = axum::http::Request::builder().method(method).uri(uri);
    let body = if has_body {
        let bytes = serde_json::to_vec(&arguments["body"])
            .map_err(|_| "the body is not JSON".to_string())?;
        builder = builder.header("content-type", "application/json");
        Body::from(bytes)
    } else if has_base64 {
        let text = arguments["body_base64"]
            .as_str()
            .ok_or_else(|| "body_base64 must be a string".to_string())?;
        use base64::Engine as _;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(text.trim())
            .map_err(|_| "body_base64 is not base64".to_string())?;
        if bytes.len() > MCP_MAX_BODY_BYTES {
            return Err("body_base64 passes the 8 MiB limit".to_string());
        }
        builder = builder.header("content-type", "application/octet-stream");
        Body::from(bytes)
    } else {
        Body::empty()
    };
    builder
        .body(body)
        .map_err(|_| "the tool call is not a request".to_string())
}

/// Plain text of a scalar argument for paths and query keys.
fn scalar_text(value: &Value, name: &str) -> Result<String, String> {
    match value {
        Value::String(text) => Ok(text.clone()),
        Value::Number(number) => Ok(number.to_string()),
        Value::Bool(flag) => Ok(flag.to_string()),
        _ => Err(format!("{name} must be a string, number or boolean")),
    }
}

/// Percent-encode a query object to `k=v&k=v`.
fn encode_query(query: &serde_json::Map<String, Value>) -> Result<String, String> {
    let mut parts = Vec::new();
    for (key, value) in query {
        parts.push(format!(
            "{}={}",
            encode_part(key),
            encode_part(&scalar_text(value, key)?)
        ));
    }
    Ok(parts.join("&"))
}

/// Percent-encode one query key or value. Keeps unreserved bytes,
/// encodes the rest as uppercase hex UTF-8.
fn encode_part(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(*byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Shape one router response as a `tools/call` result.
///
/// A JSON body returns as text. A text body returns as text. Any
/// other body (file downloads) returns base64 inside a small JSON
/// envelope with its content type, capped at 8 MiB. An HTTP status of
/// 400 or more returns `isError: true` with the code and message of
/// the API.
async fn tool_result(response: axum::http::Response<Body>) -> Value {
    let (parts, body) = response.into_parts();
    let status = parts.status.as_u16();
    let content_type = parts
        .headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let bytes = match axum::body::to_bytes(body, MCP_MAX_RESPONSE_BYTES + 1).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return serde_json::json!({"result": {
                "content": [{"type": "text", "text": "LIMIT_EXCEEDED: the answer passes the 8 MiB limit"}],
                "isError": true,
            }})
        }
    };
    if bytes.len() > MCP_MAX_RESPONSE_BYTES {
        return serde_json::json!({"result": {
            "content": [{"type": "text", "text": "LIMIT_EXCEEDED: the answer passes the 8 MiB limit"}],
            "isError": true,
        }});
    }
    if status >= 400 {
        return serde_json::json!({"result": {
            "content": [{"type": "text", "text": error_text(status, &content_type, &bytes)}],
            "isError": true,
        }});
    }
    let text = if content_type.starts_with("application/json") || bytes.is_empty() {
        String::from_utf8_lossy(&bytes).into_owned()
    } else if content_type.starts_with("text/") {
        match String::from_utf8(bytes.to_vec()) {
            Ok(text) => text,
            Err(_) => base64_envelope(&content_type, &bytes),
        }
    } else {
        base64_envelope(&content_type, &bytes)
    };
    serde_json::json!({"result": {"content": [{"type": "text", "text": text}]}})
}

/// `CODE: message` of an API failure, from its JSON body when present.
fn error_text(status: u16, content_type: &str, bytes: &[u8]) -> String {
    if content_type.starts_with("application/json") {
        if let Ok(value) = serde_json::from_slice::<Value>(bytes) {
            let code = value
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let message = value
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if !code.is_empty() || !message.is_empty() {
                return format!("{code}: {message}");
            }
        }
    }
    let reason = axum::http::StatusCode::from_u16(status)
        .map(|status| {
            status
                .canonical_reason()
                .map_or(String::new(), str::to_string)
        })
        .unwrap_or_default();
    let detail = String::from_utf8_lossy(bytes);
    let detail = detail.trim();
    if detail.is_empty() {
        reason
    } else {
        format!("{reason}: {detail}")
    }
}

/// Base64 envelope for a binary answer (file downloads).
fn base64_envelope(content_type: &str, bytes: &[u8]) -> String {
    use base64::Engine as _;
    serde_json::json!({
        "content_base64": base64::engine::general_purpose::STANDARD.encode(bytes),
        "content_type": content_type,
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_negotiation_prefers_known_asked() {
        assert_eq!(negotiate_version("2025-03-26"), "2025-03-26");
        assert_eq!(negotiate_version("9.9.9"), PROTOCOL_VERSIONS[0]);
        assert_eq!(negotiate_version(""), PROTOCOL_VERSIONS[0]);
    }

    #[test]
    fn query_encoding_keeps_unreserved_bytes() {
        let query: serde_json::Map<String, Value> =
            serde_json::from_value(serde_json::json!({"topic": "a/b"})).expect("map");
        assert_eq!(encode_query(&query).expect("encode"), "topic=a%2Fb");
    }

    #[test]
    fn destructive_marks_delete_and_stop_kick_purge_reset() {
        assert!(catalog::is_destructive("DELETE", "/api/v5/banned"));
        assert!(catalog::is_destructive(
            "POST",
            "/api/v5/clients/kickout/bulk"
        ));
        assert!(catalog::is_destructive(
            "POST",
            "/api/v5/listeners/:id/stop"
        ));
        assert!(catalog::is_destructive(
            "PUT",
            "/api/v5/rules/:id/metrics/reset"
        ));
        assert!(!catalog::is_destructive("GET", "/api/v5/metrics"));
        assert!(!catalog::is_destructive("POST", "/api/v5/publish"));
    }

    #[test]
    fn mcp_store_aliases_match_primary_flags() {
        let store = McpStore::default();
        assert!(store.is_enabled());
        assert_eq!(store.is_enabled(), store.enabled());
        assert_eq!(store.is_read_only(), store.read_only());
        store.set_enabled(false);
        assert!(!store.is_enabled());
        store.set_read_only(true);
        assert!(store.is_read_only());
        store.apply(true, false);
        assert!(store.is_enabled());
        assert!(!store.is_read_only());
        let conf = broker_config::schema::McpConf {
            enabled: false,
            read_only: true,
        };
        let seeded = McpStore::from_config(&conf);
        assert!(!seeded.is_enabled());
        assert!(seeded.is_read_only());
    }

    #[test]
    fn tool_request_encodes_path_parameters() {
        let tool = ToolDef {
            name: "delete_api_v5_rules_by_id".to_string(),
            method: "DELETE",
            path: "/api/v5/rules/:id",
            summary: "DELETE /api/v5/rules/:id",
            read_only: false,
            destructive: true,
        };
        let args: serde_json::Map<String, Value> =
            serde_json::from_value(serde_json::json!({"id": "a/b"})).expect("map");
        let request = tool_request(&tool, &args).expect("request builds");
        assert_eq!(request.uri().to_string(), "/api/v5/rules/a%2Fb");
        let args: serde_json::Map<String, Value> =
            serde_json::from_value(serde_json::json!({"id": "a?b=c"})).expect("map");
        let request = tool_request(&tool, &args).expect("request builds");
        assert_eq!(request.uri().to_string(), "/api/v5/rules/a%3Fb%3Dc");
        let args: serde_json::Map<String, Value> =
            serde_json::from_value(serde_json::json!({"id": "a#b"})).expect("map");
        let request = tool_request(&tool, &args).expect("request builds");
        assert_eq!(request.uri().to_string(), "/api/v5/rules/a%23b");
        let wild = ToolDef {
            name: "get_assets_all_path".to_string(),
            method: "GET",
            path: "/assets/*path",
            summary: "GET /assets/*path",
            read_only: true,
            destructive: false,
        };
        let args: serde_json::Map<String, Value> =
            serde_json::from_value(serde_json::json!({"path": "a/b"})).expect("map");
        let request = tool_request(&wild, &args).expect("request builds");
        assert_eq!(request.uri().to_string(), "/assets/a/b");
    }
}
