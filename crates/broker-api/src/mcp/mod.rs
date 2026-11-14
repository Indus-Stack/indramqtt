//! MCP server on the management listener (`POST /mcp`).
//!
//! The server speaks the Streamable HTTP transport of the Model Context
//! Protocol with JSON responses: `initialize`, `ping`, `tools/list`
//! and `tools/call`. It authenticates API keys through the same code
//! path as the management API (`api_auth`) and refuses dashboard login
//! tokens: an agent authenticates with a key. A key reaches `/mcp`
//! itself with the `monitoring` scope or the `publish` scope. Each tool call then re-enters
//! the live axum router in process with the same `Authorization`
//! header. The route table is built one time and shared; each call
//! sets the live state, so authentication runs per request. Per-operation scopes and the viewer rule apply exactly as
//! over HTTP. One request body holds one JSON-RPC object or one batch
//! array. Tools form groups selected with `toolsets=a,b` on `/mcp`:
//! no parameter selects the `core` set, `all` selects every tool, and
//! `list_toolsets` is always present. Management-plane only; publish
//! and deliver never touch this module.

pub mod catalog;
pub mod collect;
pub mod route_scan;

use crate::admin_users::AdminRole;
use crate::api_auth::{parse_credentials, Credentials};
use crate::v5::api_keys::{key_decision, KeyDecision, KeyGrant, KeyRole};
use crate::ApiState;
use axum::{
    body::Body,
    extract::{Query, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use broker_protocol::{QoS, TopicFilter};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};

use catalog::{catalog, list_entry_for, tool_by_name, ToolDef};

pub use collect::CollectRegistry;

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

/// Query string of `POST /mcp`: the client selects tool groups with
/// `toolsets=a,b`. No parameter selects the `core` set; `all` selects
/// every tool.
#[derive(Debug, Deserialize, Default)]
pub struct ToolsetQuery {
    #[serde(default)]
    toolsets: Option<String>,
}

/// The caller behind one `/mcp` request.
#[derive(Debug, Clone)]
struct Caller {
    /// Audit name: `api-key:<name>` for key-store keys.
    username: String,
    /// True when the caller holds the viewer role.
    viewer: bool,
    /// Grant of a key-store key. `None` for operator keys, which
    /// keep full access like over HTTP.
    grant: Option<KeyGrant>,
}

/// Resolve the caller behind the headers: a key-store key (Basic or
/// Bearer secret) or an operator key from the environment. Dashboard
/// login tokens resolve to nothing: agents use keys.
fn resolve_caller(state: &ApiState, headers: &HeaderMap) -> Option<Caller> {
    match parse_credentials(headers)? {
        Credentials::Bearer(token) => {
            if let Some(key) = state.api_keys.resolve(&token) {
                return Some(key_caller(key.name, key.grant));
            }
            state.operator_api_keys.resolve(&token).map(|info| Caller {
                username: info.username,
                viewer: info.role == AdminRole::Viewer,
                grant: None,
            })
        }
        Credentials::Basic { username, secret } => state
            .api_keys
            .resolve_basic(&username, &secret)
            .map(|key| key_caller(key.name, key.grant)),
    }
}

/// Caller record for a key-store key.
fn key_caller(name: String, grant: KeyGrant) -> Caller {
    Caller {
        username: format!("api-key:{name}"),
        viewer: grant.role == KeyRole::Viewer,
        grant: Some(grant),
    }
}

/// True when the caller may see and call the tool.
/// Read-only mode and the viewer role keep read tools only.
/// A key-store key keeps the paths its scopes allow and the
/// bearer-only paths (exactly as over HTTP). The router refuses
/// a bearer-only call with FORBIDDEN.
/// `list_toolsets` is always allowed: every key sees it.
fn caller_may_call(state: &ApiState, caller: &Caller, tool: &ToolDef) -> bool {
    if tool.name == catalog::LIST_TOOLSETS_TOOL {
        return true;
    }
    if (state.mcp.read_only() || caller.viewer) && !tool.read_only {
        return false;
    }
    match &caller.grant {
        Some(grant) => {
            let decision = key_decision(grant, tool.path);
            decision == KeyDecision::Allowed || decision == KeyDecision::LoginOnly
        }
        None => true,
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
/// JSON-RPC body. A key with neither the `monitoring` scope nor the
/// `publish` scope answers 403.
/// Oversize bodies answer 413. Anything else answers 200 with a
/// JSON-RPC response. A notification answers 202 with an empty body.
pub async fn post_mcp(
    State(state): State<ApiState>,
    Query(query): Query<ToolsetQuery>,
    headers: HeaderMap,
    body: Result<Bytes, axum::extract::rejection::BytesRejection>,
) -> Response {
    // The route caps the body at the limit while it is read, so a larger
    // body never stays in memory. The cut read arrives here as a
    // rejection and gets the management error shape.
    let body = match body {
        Ok(body) => body,
        Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(serde_json::json!({
                    "code": "PAYLOAD_TOO_LARGE",
                    "message": "the MCP body passes the 8 MiB limit",
                })),
            )
                .into_response();
        }
        Err(rejection) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "code": "BAD_REQUEST",
                    "message": format!("the MCP body cannot be read: {rejection}"),
                })),
            )
                .into_response();
        }
    };
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
    // Dashboard session tokens never open this path: agents use keys.
    if crate::v5::auth::authenticated_user(&state, &headers).is_some() {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "code": "UNAUTHORIZED",
                "message": "missing or invalid API key",
            })),
        )
            .into_response();
    }
    let Some(caller) = resolve_caller(&state, &headers) else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({
                "code": "UNAUTHORIZED",
                "message": "missing or invalid API key",
            })),
        )
            .into_response();
    };
    let authorization = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    // A key reaches `/mcp` itself with the `monitoring` scope or
    // with the `publish` scope. The messaging tools (`publish` plus
    // `subscribe_collect`) need publish rights only, so a publish-only
    // key must get past this gate. Per-tool scopes apply again inside
    // each call, so no tool escapes its own scope here.
    if let Some(grant) = caller.grant.clone() {
        let monitoring = key_decision(&grant, "/mcp");
        let publishing = key_decision(&grant, "/api/v5/publish");
        if monitoring != KeyDecision::Allowed && publishing != KeyDecision::Allowed {
            match monitoring {
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
            }
        }
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
            if let Some(answer) =
                serve_call_value(&state, &caller, authorization.clone(), &query, call).await
            {
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
    serve_call(&state, &caller, authorization, query, call).await
}

/// Answer one parsed call. Notifications (no id, or a `notifications/`
/// method) run without an answer body.
async fn serve_call(
    state: &ApiState,
    caller: &Caller,
    authorization: Option<String>,
    query: ToolsetQuery,
    call: RpcCall,
) -> Response {
    match serve_call_value(state, caller, authorization, &query, call).await {
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
    caller: &Caller,
    authorization: Option<String>,
    query: &ToolsetQuery,
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
            let tools = visible_tools(state, caller, query);
            let mut entries: Vec<Value> = tools
                .iter()
                .filter(|tool| tool.name != SUBSCRIBE_COLLECT_TOOL)
                .map(tool_list_entry)
                .collect();
            if tools.iter().any(|tool| tool.name == SUBSCRIBE_COLLECT_TOOL) {
                entries.push(subscribe_collect_entry());
            }
            with_envelope(&call.id, serde_json::json!({"result": {"tools": entries}}))
        }
        "tools/call" => {
            let started = std::time::Instant::now();
            let (status, result, tool) = call_tool(state, caller, authorization, call.params).await;
            audit_tool_call(state, &caller.username, &tool, status, started.elapsed());
            with_envelope(&call.id, result)
        }
        _ => with_envelope(&call.id, rpc_error(-32601, "unknown method")),
    };
    Some(answer)
}

/// Tools the caller may see for the selected toolsets.
/// Unknown names select nothing and give an empty list.
/// `list_toolsets` is present for each known selection.
fn visible_tools(state: &ApiState, caller: &Caller, query: &ToolsetQuery) -> Vec<ToolDef> {
    let selected = select_toolsets(query);
    if selected.is_empty() {
        return Vec::new();
    }
    let mut out = vec![list_toolsets_def()];
    out.extend(
        catalog()
            .iter()
            .filter(|&tool| selected.contains(&tool.toolset))
            .filter(|&tool| caller_may_call(state, caller, tool))
            .cloned(),
    );
    let collect = subscribe_collect_def();
    if selected.contains(&collect.toolset) && caller_may_call(state, caller, &collect) {
        out.push(collect);
    }
    out
}

/// Toolsets named by the `toolsets` query value: `None` selects the
/// `core` set, `all` selects every toolset, else the named sets.
/// Unknown names select nothing.
fn select_toolsets(query: &ToolsetQuery) -> Vec<&'static str> {
    match query.toolsets.as_deref() {
        None => catalog::CORE_TOOLSETS.to_vec(),
        Some(raw) => {
            let names: Vec<String> = raw
                .split(',')
                .map(|name| name.trim().to_string())
                .filter(|name| !name.is_empty())
                .collect();
            if names.iter().any(|name| name == "all") {
                return catalog::TOOLSETS.iter().map(|set| set.name).collect();
            }
            catalog::TOOLSETS
                .iter()
                .map(|set| set.name)
                .filter(|known| names.iter().any(|name| name == known))
                .collect()
        }
    }
}

/// One `tools/list` entry: a catalog tool, or the built-in
/// `list_toolsets` entry.
fn tool_list_entry(tool: &ToolDef) -> Value {
    if tool.name == catalog::LIST_TOOLSETS_TOOL {
        return serde_json::json!({
            "name": catalog::LIST_TOOLSETS_TOOL,
            "description": "List the MCP toolsets with their tool counts.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "required": [],
                "additionalProperties": false,
            },
            "annotations": {
                "readOnlyHint": true,
                "destructiveHint": false,
            },
        });
    }
    list_entry_for(tool)
}

/// Name of the temporary-subscription collect tool (MC-04). It has
/// no HTTP route: it registers a temporary internal subscription,
/// drains matching publishes until its limit or its time bound, then
/// removes the subscription on every exit path.
pub const SUBSCRIBE_COLLECT_TOOL: &str = "subscribe_collect";

/// Short description of the collect tool.
const SUBSCRIBE_COLLECT_SUMMARY: &str = "Subscribe briefly and collect matching messages.";

/// Input schema of the collect tool. Hand-written: no HTTP route
/// defines it. Limits come from the task: 1 to 100 messages, 1 to 30
/// seconds, QoS 0 or 1.
fn subscribe_collect_schema() -> Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "topic_filter": {"type": "string"},
            "max_messages": {"type": "integer", "minimum": 1, "maximum": 100, "default": 10},
            "timeout_seconds": {"type": "integer", "minimum": 1, "maximum": 30, "default": 5},
            "qos": {"type": "integer", "enum": [0, 1]}
        },
        "required": ["topic_filter", "qos"]
    })
}

/// Definition of the collect tool for permission checks. It rides the
/// `publish` toolset (core) and needs the publish scope, exactly like
/// the publish operation. The HTTP subscribe operation is a POST (a
/// write for viewers), so read-only mode and the viewer role hide it.
/// Keys carry no tenant here, so the collect observes the default
/// tenant only, like every management publish.
fn subscribe_collect_def() -> ToolDef {
    ToolDef {
        name: SUBSCRIBE_COLLECT_TOOL.to_string(),
        method: "POST",
        path: "/api/v5/publish",
        summary: SUBSCRIBE_COLLECT_SUMMARY,
        read_only: false,
        destructive: false,
        toolset: "publish",
    }
}

/// One `tools/list` entry for the collect tool.
fn subscribe_collect_entry() -> Value {
    serde_json::json!({
        "name": SUBSCRIBE_COLLECT_TOOL,
        "description": SUBSCRIBE_COLLECT_SUMMARY,
        "inputSchema": subscribe_collect_schema(),
        "annotations": {
            "readOnlyHint": false,
            "destructiveHint": false,
        },
    })
}

/// The pseudo-tool behind `list_toolsets`. It is always present and
/// carries no HTTP operation.
fn list_toolsets_def() -> ToolDef {
    ToolDef {
        name: catalog::LIST_TOOLSETS_TOOL.to_string(),
        method: "GET",
        path: "/api/v5/status",
        summary: "List the MCP toolsets with their tool counts.",
        read_only: true,
        destructive: false,
        toolset: "status",
    }
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

/// Run one `tools/call`: find the tool, check the permission, build
/// the HTTP request from the arguments, run the live router in
/// process, and shape the answer. Returns the HTTP status for the
/// audit record, the `result`/`error` body and the tool name.
/// The audit record carries the tool name and the result only: tool
/// arguments may carry secrets and are never logged.
///
/// The inner dispatch reuses the one-time route table from
/// [`inner_routes`]: the table is built once per process, and each
/// call sets the live state. Authentication and permission checks run
/// per request against the current stores. A key change, a scope
/// change or a configuration change applies to the next call.
async fn call_tool(
    state: &ApiState,
    caller: &Caller,
    authorization: Option<String>,
    params: Option<Value>,
) -> (u16, Value, String) {
    let params = params.unwrap_or(Value::Null);
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let arguments = params.get("arguments").cloned().unwrap_or(Value::Null);
    if name.is_empty() || (!arguments.is_object() && !arguments.is_null()) {
        return (
            400,
            rpc_error(-32602, "tools/call needs a name and an arguments object"),
            name.to_string(),
        );
    }
    let arguments = if arguments.is_null() {
        serde_json::Map::new()
    } else {
        arguments.as_object().cloned().unwrap_or_default()
    };
    if name == catalog::LIST_TOOLSETS_TOOL {
        return (200, list_toolsets_result(state, caller), name.to_string());
    }
    if name == catalog::BROKER_STATUS_TOOL {
        let status_tool = tool_by_name(catalog(), catalog::BROKER_STATUS_TOOL)
            .cloned()
            .expect("status tool");
        if !caller_may_call(state, caller, &status_tool) {
            return (
                403,
                rpc_error(-32602, "this key may not call this tool"),
                name.to_string(),
            );
        }
        return (200, broker_status_result(), name.to_string());
    }
    if name == SUBSCRIBE_COLLECT_TOOL {
        let def = subscribe_collect_def();
        if state.mcp.read_only() && !def.read_only {
            return (
                403,
                serde_json::json!({"result": {
                    "content": [{"type": "text", "text": "FORBIDDEN: the MCP endpoint is read-only"}],
                    "isError": true,
                }}),
                name.to_string(),
            );
        }
        if !caller_may_call(state, caller, &def) {
            return (
                403,
                rpc_error(-32602, "this key may not call this tool"),
                name.to_string(),
            );
        }
        let (status, result) = call_collect(state, &arguments).await;
        return (status, result, name.to_string());
    }
    let tool = match tool_by_name(catalog(), name) {
        Some(tool) => tool.clone(),
        None => {
            if is_excluded_tool_name(name) {
                return (
                    400,
                    rpc_error(-32602, "this operation has no tool"),
                    name.to_string(),
                );
            }
            return (404, rpc_error(-32602, "unknown tool"), name.to_string());
        }
    };
    if state.mcp.read_only() && !tool.read_only {
        return (
            403,
            serde_json::json!({"result": {
                "content": [{"type": "text", "text": "FORBIDDEN: the MCP endpoint is read-only"}],
                "isError": true,
            }}),
            name.to_string(),
        );
    }
    let login_only = caller
        .grant
        .as_ref()
        .is_some_and(|grant| key_decision(grant, tool.path) == KeyDecision::LoginOnly);
    if !caller_may_call(state, caller, &tool) && !login_only {
        return (
            403,
            rpc_error(-32602, "this key may not call this tool"),
            name.to_string(),
        );
    }
    let request = match tool_request(&tool, &arguments) {
        Ok(request) => request,
        Err(message) => return (400, rpc_error(-32602, &message), name.to_string()),
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
    // so a body up to 8 MiB reaches the handler. The table is built
    // one time and shared; each call sets the live state, so
    // authentication runs per request against the current stores.
    let app = inner_routes(state).layer(axum::extract::DefaultBodyLimit::disable());
    let response = match app.oneshot(request).await {
        Ok(response) => response,
        Err(never) => match never {},
    };
    let status = response.status().as_u16();
    let result = tool_result(response).await;
    (status, result, name.to_string())
}

/// Inner dispatch table for `tools/call`: the live management
/// route table, built one time and shared by every call.
///
/// The table holds no state. Each call sets the live `ApiState` with
/// `with_state`, so authentication runs per request against the
/// current stores and concurrent calls share the table. No cycle
/// exists: the table never points back at the state or the outer
/// router. Tests read the build count through
/// [`inner_route_builds`].
fn inner_routes(state: &ApiState) -> axum::Router {
    crate::router_routes()
        .layer(axum::Extension(state.clone()))
        .with_state(state.clone())
}

/// Count of inner route table builds. Tests read this; production
/// ignores it. The table behind [`inner_routes`] is shared, so the
/// count stays at one after the first call.
#[cfg(test)]
pub(crate) fn inner_route_builds() -> u64 {
    crate::router_route_builds()
}

/// Run the `subscribe_collect` tool: register a temporary internal
/// subscription, collect until the limit or the time bound, then
/// remove the subscription. The guard removes it on every exit path.
/// Keys carry no tenant here, so the collect observes the default
/// tenant only, like every management publish.
async fn call_collect(
    state: &ApiState,
    arguments: &serde_json::Map<String, Value>,
) -> (u16, Value) {
    let filter_text = match arg_string(arguments, "topic_filter", true) {
        Ok(value) => value.unwrap_or_default(),
        Err(message) => return (400, rpc_error(-32602, &invalid_params(&message))),
    };
    if broker_router::strip_delayed_prefix(&filter_text).is_some() {
        return (
            400,
            rpc_error(
                -32602,
                "invalid params: delayed topics cannot be subscribed",
            ),
        );
    }
    let filter = match TopicFilter::new(filter_text.clone()) {
        Ok(filter) => filter,
        Err(error) => {
            return (
                400,
                rpc_error(
                    -32602,
                    &invalid_params(&format!("invalid topic filter `{filter_text}`: {error}")),
                ),
            );
        }
    };
    let max_messages = match arg_bounded(arguments, "max_messages", 10, 1, 100) {
        Ok(value) => value as usize,
        Err(message) => return (400, rpc_error(-32602, &invalid_params(&message))),
    };
    let timeout_secs = match arg_bounded(arguments, "timeout_seconds", 5, 1, 30) {
        Ok(value) => value,
        Err(message) => return (400, rpc_error(-32602, &invalid_params(&message))),
    };
    let qos_raw = match arguments.get("qos") {
        Some(Value::Number(n)) => match n.as_u64() {
            Some(0) => 0u8,
            Some(1) => 1u8,
            _ => {
                return (
                    400,
                    rpc_error(-32602, "invalid params: field `qos` must be 0 or 1"),
                );
            }
        },
        Some(_) => {
            return (
                400,
                rpc_error(-32602, "invalid params: field `qos` must be 0 or 1"),
            );
        }
        None => {
            return (
                400,
                rpc_error(-32602, "invalid params: field `qos` is required"),
            );
        }
    };
    // TODO(parity): should a future per-key tenant scope the collect?
    // No key record holds one today; the conservative choice keeps
    // default-only reads.
    let tenant = broker_session::tenant::DEFAULT_TENANT_ID;
    let handle = match state.collectors.register(filter.clone(), qos_raw, tenant) {
        Ok(handle) => handle,
        Err(collect::CollectError::Full) => {
            return (
                429,
                serde_json::json!({"result": {
                    "content": [{"type": "text", "text": "TOO_MANY_REQUESTS: too many concurrent collects"}],
                    "isError": true,
                }}),
            );
        }
    };
    let qos = if qos_raw == 1 {
        QoS::AtLeastOnce
    } else {
        QoS::AtMostOnce
    };
    if !state.router.subscribe(
        &filter,
        broker_router::Subscription::new_in_tenant(handle.client_id.as_str(), 0, qos, tenant),
    ) {
        state.collectors.unregister(handle.id);
        return (
            503,
            serde_json::json!({"result": {
                "content": [{"type": "text", "text": "TOO_MANY_REQUESTS: subscription table full"}],
                "isError": true,
            }}),
        );
    }
    let collect::CollectHandle {
        id: collect_id,
        client_id,
        filter,
        tenant,
        mut rx,
    } = handle;
    let guard = collect::CollectGuard {
        registry: state.collectors.clone(),
        router: state.router.clone(),
        id: collect_id,
        client_id,
        filter,
        tenant,
    };
    let mut messages = Vec::new();
    // Retained replay: a new subscription observes stored retained
    // messages first. Bounded by the collect limit. A failed lookup
    // collects nothing; live messages still flow. A replayed message
    // always carries the retain flag, like the kernel subscribe path.
    if let Ok(stored) = state
        .retained
        .find_matching_in_tenant(&guard.filter, &guard.tenant)
        .await
    {
        for one in stored.into_iter().take(max_messages) {
            let (bytes, truncated) = collect::truncate_payload(&one.payload);
            messages.push(collect::render_collected(&collect::CollectedMessage {
                topic: one.topic.as_str().to_string(),
                qos: std::cmp::min(u8::from(one.qos), qos_raw),
                retain: true,
                payload: bytes,
                truncated,
            }));
        }
    }
    let deadline = std::time::Duration::from_secs(timeout_secs);
    let _ = tokio::time::timeout(deadline, async {
        while messages.len() < max_messages {
            match rx.recv().await {
                Some(next) => messages.push(collect::render_collected(&next)),
                None => break,
            }
        }
    })
    .await;
    drop(guard);
    (
        200,
        serde_json::json!({"result": {
            "content": [{"type": "text", "text": serde_json::json!({"messages": messages}).to_string()}],
        }}),
    )
}

/// Read one required string field. Fails closed on other types.
fn arg_string(
    args: &serde_json::Map<String, Value>,
    field: &str,
    required: bool,
) -> Result<Option<String>, String> {
    match args.get(field) {
        None => {
            if required {
                return Err(format!("field `{field}` is required"));
            }
            Ok(None)
        }
        Some(Value::String(text)) => Ok(Some(text.clone())),
        Some(_) => Err(format!("field `{field}` must be a string")),
    }
}

/// Read one bounded integer field with a default. Fails closed on
/// other types and out-of-range values.
fn arg_bounded(
    args: &serde_json::Map<String, Value>,
    field: &str,
    default: u64,
    min: u64,
    max: u64,
) -> Result<u64, String> {
    match args.get(field) {
        None => Ok(default),
        Some(Value::Number(n)) => {
            let value = n
                .as_u64()
                .ok_or_else(|| format!("field `{field}` must be an integer {min}..={max}"))?;
            if value < min || value > max {
                return Err(format!("field `{field}` must be an integer {min}..={max}"));
            }
            Ok(value)
        }
        Some(_) => Err(format!("field `{field}` must be an integer {min}..={max}")),
    }
}

/// Prefix one validation detail for JSON-RPC invalid params.
fn invalid_params(message: &str) -> String {
    format!("invalid params: {message}")
}

/// One audit record per `tools/call`, through the existing audit log.
/// The record carries the key name, the tool name and the result
/// code. It never carries tool arguments: arguments may hold secrets.
fn audit_tool_call(
    state: &ApiState,
    source: &str,
    tool: &str,
    status: u16,
    elapsed: std::time::Duration,
) {
    state.audit.record(crate::v5::audit::AuditRecordInput {
        created_at_us: crate::v5::audit::now_micros(),
        node: state.node_id.clone(),
        from: "mcp".to_string(),
        source: source.to_string(),
        source_ip: String::new(),
        operation_id: format!("mcp:{tool}"),
        operation_type: "mcp".to_string(),
        operation_result: crate::v5::audit::outcome_for_status(status).to_string(),
        http_method: "post".to_string(),
        http_status_code: status,
        duration_ms: elapsed.as_millis().min(u128::from(u64::MAX)) as u64,
        tenant: broker_session::tenant::DEFAULT_TENANT_ID.to_string(),
    });
}

/// The built-in status answer: version, uptime and node name.
fn broker_status_result() -> Value {
    serde_json::json!({"result": {
        "content": [{"type": "text", "text": serde_json::json!({
            "version": env!("CARGO_PKG_VERSION"),
            "uptime_ms": crate::v5::nodes::uptime_ms(),
            "node": crate::node_scope::local_node_name(),
        }).to_string()}],
    }})
}

/// The built-in toolset listing: groups with tool counts for the
/// caller permission: a key sees only the tools it may call.
fn list_toolsets_result(state: &ApiState, caller: &Caller) -> Value {
    let shared = catalog();
    let collect = subscribe_collect_def();
    let mut sets: Vec<Value> = Vec::new();
    for set in catalog::TOOLSETS {
        let mut tools: Vec<&ToolDef> = shared
            .iter()
            .filter(|tool| tool.toolset == set.name)
            .filter(|tool| caller_may_call(state, caller, tool))
            .collect();
        if set.name == collect.toolset && caller_may_call(state, caller, &collect) {
            tools.push(&collect);
        }
        if tools.is_empty() {
            continue;
        }
        sets.push(catalog::toolset_entry_for(
            set.name,
            set.description,
            &tools,
        ));
    }
    serde_json::json!({"result": {
        "content": [{"type": "text", "text": serde_json::json!({"toolsets": sets}).to_string()}],
    }})
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
            toolset: "rules",
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
            toolset: "configuration",
        };
        let args: serde_json::Map<String, Value> =
            serde_json::from_value(serde_json::json!({"path": "a/b"})).expect("map");
        let request = tool_request(&wild, &args).expect("request builds");
        assert_eq!(request.uri().to_string(), "/assets/a/b");
    }
    #[test]
    fn toolset_selection_defaults_to_core() {
        let query = ToolsetQuery { toolsets: None };
        assert_eq!(select_toolsets(&query), catalog::CORE_TOOLSETS.to_vec());
    }

    #[test]
    fn toolset_selection_names_sets_and_all() {
        let named = ToolsetQuery {
            toolsets: Some("rules,connectors".to_string()),
        };
        assert_eq!(select_toolsets(&named), vec!["rules", "connectors"]);
        let all = ToolsetQuery {
            toolsets: Some("all".to_string()),
        };
        assert_eq!(select_toolsets(&all).len(), catalog::TOOLSETS.len());
        let unknown = ToolsetQuery {
            toolsets: Some("nope".to_string()),
        };
        assert!(select_toolsets(&unknown).is_empty());
    }

    #[test]
    fn toolset_mapping_covers_core_groups() {
        assert_eq!(catalog::toolset_for("/api/v5/status"), "status");
        assert_eq!(catalog::toolset_for("/api/v5/clients"), "clients");
        assert_eq!(
            catalog::toolset_for("/api/v5/subscriptions"),
            "subscriptions"
        );
        assert_eq!(catalog::toolset_for("/api/v5/topics"), "topics");
        assert_eq!(catalog::toolset_for("/api/v5/publish"), "publish");
        assert_eq!(catalog::toolset_for("/api/v5/metrics"), "metrics");
        assert_eq!(catalog::toolset_for("/api/v5/rules"), "rules");
        assert_eq!(catalog::toolset_for("/api/v5/connectors"), "connectors");
        assert_eq!(
            catalog::toolset_for("/api/v5/authentication"),
            "authentication"
        );
        assert_eq!(
            catalog::toolset_for("/api/v5/authorization/sources"),
            "authorization"
        );
        assert_eq!(catalog::toolset_for("/api/v5/mt/ns_list"), "tenants");
        assert_eq!(catalog::toolset_for("/api/v5/listeners"), "listeners");
        assert_eq!(catalog::toolset_for("/api/v5/configs"), "configuration");
    }

    /// Standalone state with one administrator key. Proofs below read
    /// through a real loopback socket; setup writes (keys) mirror boot.
    fn keyed_state() -> (ApiState, String) {
        let engine = std::sync::Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        let state = ApiState::standalone(engine);
        let mut new = crate::v5::api_keys::NewApiKey::new("mcp-admin");
        new.role = crate::v5::api_keys::KeyRole::Administrator;
        let secret = state.api_keys.create(new).expect("key mints").1;
        (state, secret)
    }

    /// Socket client for the handler tests: the real production
    /// router on a real loopback socket. Every proof below reads
    /// through HTTP.
    #[derive(Clone, Copy)]
    struct McpClient {
        port: u16,
    }

    impl McpClient {
        /// POST one path with a JSON body through the socket.
        async fn post(
            &self,
            path: &str,
            token: Option<&str>,
            body: &serde_json::Value,
        ) -> (StatusCode, serde_json::Value) {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let raw = serde_json::to_vec(body).expect("test body serialises");
            let mut head = format!(
                "POST {path} HTTP/1.1\r\nHost: test\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
                raw.len()
            );
            if let Some(token) = token {
                head.push_str(&format!("Authorization: Bearer {token}\r\n"));
            }
            head.push_str("Connection: close\r\n\r\n");
            let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", self.port))
                .await
                .expect("connect test server");
            stream.write_all(head.as_bytes()).await.expect("write head");
            stream.write_all(&raw).await.expect("write body");
            let mut buf = Vec::new();
            stream.read_to_end(&mut buf).await.expect("read response");
            let text = String::from_utf8(buf).expect("response is UTF-8");
            let (head, body) = text.split_once("\r\n\r\n").expect("header/body split");
            let status: u16 = head.lines().next().expect("status line")[9..12]
                .parse()
                .expect("status code");
            let json = if body.trim().is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::from_str(body).expect("response body is JSON")
            };
            (
                StatusCode::from_u16(status).expect("test status is valid"),
                json,
            )
        }
    }

    /// One served test router plus its client. Dropped at test end.
    struct McpServer {
        client: McpClient,
        task: tokio::task::JoinHandle<()>,
    }

    impl McpServer {
        async fn start(state: ApiState) -> Self {
            let app = crate::router(state);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind test server");
            let port = listener.local_addr().expect("local addr").port();
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.expect("serve test app");
            });
            Self {
                client: McpClient { port },
                task,
            }
        }
    }

    impl Drop for McpServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    /// Mint one API key in the store. Setup write; proofs use HTTP.
    fn mint_key(state: &ApiState, name: &str, role: crate::v5::api_keys::KeyRole) -> String {
        let mut new = crate::v5::api_keys::NewApiKey::new(name);
        new.role = role;
        state.api_keys.create(new).expect("key mints").1
    }

    /// Names of the listed tools for one credential through the socket.
    async fn listed_names(client: &McpClient, token: &str) -> Vec<String> {
        let (status, body) = client
            .post(
                "/mcp",
                Some(token),
                &serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        body["result"]["tools"]
            .as_array()
            .expect("tools list")
            .iter()
            .map(|tool| tool["name"].as_str().expect("tool name").to_string())
            .collect()
    }

    /// A body over the 8 MiB limit ends while it is read, before the key
    /// check: the caller gets 413 and the broker does not hold the body.
    /// A body at the limit with no key reaches the key check (401).
    #[tokio::test]
    async fn oversized_body_without_key_is_refused_while_read() {
        use tower::ServiceExt as _;
        let (state, _token) = keyed_state();
        let over = vec![b' '; MCP_MAX_BODY_BYTES + 1];
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("content-type", "application/json")
            .body(Body::from(over))
            .expect("request builds");
        let response = crate::router(state.clone())
            .oneshot(request)
            .await
            .expect("router answers");
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

        let at_limit = vec![b' '; MCP_MAX_BODY_BYTES];
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("content-type", "application/json")
            .body(Body::from(at_limit))
            .expect("request builds");
        let response = crate::router(state)
            .oneshot(request)
            .await
            .expect("router answers");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn default_list_holds_the_messaging_tools() {
        let (state, token) = keyed_state();
        let server = McpServer::start(state).await;
        let names = listed_names(&server.client, &token).await;
        assert!(
            names.contains(&"post_api_v5_publish".to_string()),
            "core set holds the publish operation"
        );
        assert!(names.contains(&SUBSCRIBE_COLLECT_TOOL.to_string()));
        let (status, body) = server
            .client
            .post(
                "/mcp",
                Some(&token),
                &serde_json::json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list"}),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        let tools = body["result"]["tools"].as_array().expect("tools list");
        let collect = tools
            .iter()
            .find(|tool| tool["name"] == SUBSCRIBE_COLLECT_TOOL)
            .expect("collect entry");
        assert_eq!(
            collect["annotations"]["readOnlyHint"],
            serde_json::json!(false)
        );
        assert!(collect["inputSchema"]["properties"]
            .as_object()
            .expect("schema properties")
            .contains_key("topic_filter"));
    }

    #[tokio::test]
    async fn collect_validates_its_limits_through_the_socket() {
        let (state, token) = keyed_state();
        let server = McpServer::start(state).await;
        for args in [
            serde_json::json!({"topic_filter": "a/b", "qos": 0, "max_messages": 101}),
            serde_json::json!({"topic_filter": "a/b", "qos": 0, "timeout_seconds": 0}),
            serde_json::json!({"topic_filter": "a/b", "qos": 0, "timeout_seconds": 31}),
            serde_json::json!({"topic_filter": "a/b", "qos": 2}),
            serde_json::json!({"topic_filter": "a/b"}),
            serde_json::json!({"topic_filter": "bad/#/x", "qos": 0}),
        ] {
            let (status, body) = server
                .client
                .post(
                    "/mcp",
                    Some(&token),
                    &serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": 5,
                        "method": "tools/call",
                        "params": {"name": SUBSCRIBE_COLLECT_TOOL, "arguments": args}
                    }),
                )
                .await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(
                body["error"]["code"],
                serde_json::json!(-32602),
                "args {args} fail"
            );
        }
    }

    /// Publish one message through the HTTP route on the same server.
    async fn http_publish(client: &McpClient, token: &str, topic: &str, payload: &str) {
        let (status, _) = client
            .post(
                "/api/v5/publish",
                Some(token),
                &serde_json::json!({"topic": topic, "payload": payload}),
            )
            .await;
        assert!(
            status == StatusCode::OK
                || status == StatusCode::ACCEPTED
                || status == StatusCode::SERVICE_UNAVAILABLE,
            "publish answers, got {status}"
        );
    }

    #[tokio::test]
    async fn collect_returns_published_messages_through_the_socket() {
        let (state, token) = keyed_state();
        let server = McpServer::start(state.clone()).await;
        // A publish before the collect registers never arrives.
        // Collects observe publishes inside their own window.
        http_publish(&server.client, &token, "mcp/self/one", "hello").await;
        let client = server.client;
        let call_token = token.clone();
        let pending = tokio::spawn(async move {
            client
                .post(
                    "/mcp",
                    Some(&call_token),
                    &serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": 6,
                        "method": "tools/call",
                        "params": {
                            "name": SUBSCRIBE_COLLECT_TOOL,
                            "arguments": {
                                "topic_filter": "mcp/self/#",
                                "max_messages": 1,
                                "timeout_seconds": 5,
                                "qos": 0
                            }
                        }
                    }),
                )
                .await
        });
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        http_publish(&server.client, &token, "mcp/self/two", "world").await;
        let (status, body) = pending.await.expect("collect ends");
        assert_eq!(status, StatusCode::OK);
        let text = body["result"]["content"][0]["text"]
            .as_str()
            .expect("text content");
        let result: serde_json::Value = serde_json::from_str(text).expect("result is JSON");
        let messages = result["messages"].as_array().expect("messages list");
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["topic"], serde_json::json!("mcp/self/two"));
        assert_eq!(messages[0]["payload"], serde_json::json!("world"));
        assert_eq!(messages[0]["payload_encoding"], serde_json::json!("plain"));
        assert_eq!(messages[0]["truncated"], serde_json::json!(false));
        assert_eq!(state.collectors.count(), 0, "exit removes the collect");
    }

    #[tokio::test]
    async fn collect_replays_retained_first_through_the_socket() {
        let (state, token) = keyed_state();
        let server = McpServer::start(state.clone()).await;
        // A retained publish before the collect replays first, with
        // the retain flag set like the kernel subscribe path.
        let (status, _) = server
            .client
            .post(
                "/api/v5/publish",
                Some(&token),
                &serde_json::json!({
                    "topic": "mcp/ret/old",
                    "payload": "stale",
                    "retain": true
                }),
            )
            .await;
        assert_eq!(status, StatusCode::ACCEPTED, "no subscriber yet");
        let (status, body) = server
            .client
            .post(
                "/mcp",
                Some(&token),
                &serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 7,
                    "method": "tools/call",
                    "params": {
                        "name": SUBSCRIBE_COLLECT_TOOL,
                        "arguments": {
                            "topic_filter": "mcp/ret/#",
                            "max_messages": 10,
                            "timeout_seconds": 1,
                            "qos": 0
                        }
                    }
                }),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        let text = body["result"]["content"][0]["text"]
            .as_str()
            .expect("text content");
        let result: serde_json::Value = serde_json::from_str(text).expect("result is JSON");
        let messages = result["messages"].as_array().expect("messages list");
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0]["topic"], serde_json::json!("mcp/ret/old"));
        assert_eq!(messages[0]["payload"], serde_json::json!("stale"));
        assert_eq!(messages[0]["retain"], serde_json::json!(true));
        assert_eq!(state.collectors.count(), 0, "exit removes the collect");
    }

    #[tokio::test]
    async fn keys_need_publish_rights_through_the_socket() {
        let (state, _) = keyed_state();
        let publish_secret = mint_key(&state, "mcp-pub", crate::v5::api_keys::KeyRole::Publisher);
        let viewer_secret = mint_key(&state, "mcp-view", crate::v5::api_keys::KeyRole::Viewer);
        let server = McpServer::start(state).await;

        let names = listed_names(&server.client, &publish_secret).await;
        assert!(names.contains(&"post_api_v5_publish".to_string()));
        assert!(names.contains(&SUBSCRIBE_COLLECT_TOOL.to_string()));

        let names = listed_names(&server.client, &viewer_secret).await;
        assert!(
            !names.contains(&SUBSCRIBE_COLLECT_TOOL.to_string()),
            "viewer hides the collect tool"
        );
        let (status, refused) = server
            .client
            .post(
                "/mcp",
                Some(&viewer_secret),
                &serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {
                        "name": SUBSCRIBE_COLLECT_TOOL,
                        "arguments": {"topic_filter": "mcp/keys/#", "qos": 0}
                    }
                }),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(refused.get("error").is_some(), "viewer cannot collect");
    }

    #[tokio::test]
    async fn read_only_and_disabled_modes_answer_through_the_socket() {
        let (state, token) = keyed_state();
        state.mcp.apply(true, true);
        let server = McpServer::start(state).await;
        let names = listed_names(&server.client, &token).await;
        assert!(
            !names.contains(&SUBSCRIBE_COLLECT_TOOL.to_string()),
            "read-only mode hides the collect tool"
        );
        let (status, refused) = server
            .client
            .post(
                "/mcp",
                Some(&token),
                &serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "tools/call",
                    "params": {"name": SUBSCRIBE_COLLECT_TOOL,
                               "arguments": {"topic_filter": "mcp/ro/#", "qos": 0}}
                }),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        let refused_read_only = refused.get("error").is_some()
            || refused
                .get("result")
                .and_then(|result| result.get("isError"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
        assert!(refused_read_only, "read-only mode refuses");

        let (disabled, _) = keyed_state();
        disabled.mcp.apply(false, false);
        let gone = McpServer::start(disabled).await;
        let (status, _) = gone
            .client
            .post(
                "/mcp",
                None,
                &serde_json::json!({"jsonrpc": "2.0", "id": 4, "method": "ping"}),
            )
            .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "disabled answers 404");
    }

    /// One `tools/call` of the status tool through the socket.
    async fn call_status_tool(client: &McpClient, token: &str, id: i64) -> (StatusCode, Value) {
        client
            .post(
                "/mcp",
                Some(token),
                &serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": "tools/call",
                    "params": {"name": catalog::BROKER_STATUS_TOOL, "arguments": {}}
                }),
            )
            .await
    }

    /// The shared route table and the shared catalog are built one
    /// time: 32 concurrent calls add at most one build each, and every
    /// call answers. Concurrent calls share the table.
    #[tokio::test]
    async fn shared_table_builds_once_for_many_concurrent_calls() {
        let (state, token) = keyed_state();
        let server = McpServer::start(state).await;
        let routes_before = inner_route_builds();
        let catalog_before = catalog::catalog_builds();
        let mut tasks = Vec::new();
        for id in 0..32i64 {
            let client = server.client;
            let call_token = token.clone();
            tasks.push(tokio::spawn(async move {
                if id % 2 == 0 {
                    client
                        .post(
                            "/mcp",
                            Some(&call_token),
                            &serde_json::json!({
                                "jsonrpc": "2.0", "id": id, "method": "tools/list"
                            }),
                        )
                        .await
                } else {
                    call_status_tool(&client, &call_token, id).await
                }
            }));
        }
        for task in tasks {
            let (status, _) = task.await.expect("call ends");
            assert_eq!(status, StatusCode::OK);
        }
        assert!(
            inner_route_builds() - routes_before <= 1,
            "one route table build at most"
        );
        assert!(
            catalog::catalog_builds() - catalog_before <= 1,
            "one catalog build at most"
        );
    }

    /// The shared table reads live state on each call. A removed key
    /// fails on the next call, and a new key passes. A read-only switch
    /// refuses writes on the next call. It allows them again after the
    /// switch is cleared. The table is built once, not per request.
    #[tokio::test]
    async fn shared_table_sees_key_and_mode_changes_on_next_call() {
        let (state, token) = keyed_state();
        let server = McpServer::start(state.clone()).await;
        let (status, _) = call_status_tool(&server.client, &token, 1).await;
        assert_eq!(status, StatusCode::OK);
        state.api_keys.remove("mcp-admin").expect("key removes");
        let (status, _) = call_status_tool(&server.client, &token, 2).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "removed key fails");
        let fresh = mint_key(
            &state,
            "mcp-admin-2",
            crate::v5::api_keys::KeyRole::Administrator,
        );
        let (status, body) = call_status_tool(&server.client, &fresh, 3).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.get("error").is_none(), "{body}");
        state.mcp.set_read_only(true);
        let (status, refused) = server
            .client
            .post(
                "/mcp",
                Some(&fresh),
                &serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 4,
                    "method": "tools/call",
                    "params": {"name": "post_api_v5_publish",
                               "arguments": {"body": {"topic": "mcp/ro", "payload": "x"}}}
                }),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(refused["result"]["isError"], serde_json::json!(true));
        state.mcp.set_read_only(false);
        let (status, body) = server
            .client
            .post(
                "/mcp",
                Some(&fresh),
                &serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 5,
                    "method": "tools/call",
                    "params": {"name": "post_api_v5_publish",
                               "arguments": {"body": {"topic": "mcp/ro", "payload": "x"}}}
                }),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.get("error").is_none(), "{body}");
    }

    /// `tools/list` with `toolsets=all` names each catalog tool exactly
    /// once, plus each custom tool once. Each catalog description names
    /// its route. The `gateways` toolset holds the honest markers: the
    /// router exposes the gateway list, detail and enable routes with
    /// empty or refused answers.
    #[tokio::test]
    async fn all_list_names_each_catalog_tool_once() {
        let (state, token) = keyed_state();
        let server = McpServer::start(state).await;
        let (status, body) = server
            .client
            .post(
                "/mcp?toolsets=all",
                Some(&token),
                &serde_json::json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        let tools = body["result"]["tools"].as_array().expect("tools list");
        let names: Vec<String> = tools
            .iter()
            .map(|tool| tool["name"].as_str().expect("tool name").to_string())
            .collect();
        for tool in catalog() {
            let count = names.iter().filter(|name| **name == tool.name).count();
            assert_eq!(count, 1, "tool {} is listed once", tool.name);
            let entry = tools
                .iter()
                .find(|item| item["name"] == tool.name)
                .expect("listed tool has an entry");
            let description = entry["description"].as_str().expect("description");
            let route = format!("{} {}", tool.method, tool.path);
            assert!(
                description.contains(&route),
                "tool {} names its route {route}",
                tool.name
            );
        }
        for custom in [
            catalog::BROKER_STATUS_TOOL,
            catalog::LIST_TOOLSETS_TOOL,
            SUBSCRIBE_COLLECT_TOOL,
        ] {
            let count = names.iter().filter(|name| **name == *custom).count();
            assert_eq!(count, 1, "custom tool {custom} is listed once");
        }
        assert!(
            catalog().iter().any(|tool| tool.toolset == "gateways"),
            "gateway honest markers are tools now"
        );
        let (_, body) = server
            .client
            .post(
                "/mcp",
                Some(&token),
                &serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {"name": catalog::LIST_TOOLSETS_TOOL, "arguments": {}}
                }),
            )
            .await;
        let text = body["result"]["content"][0]["text"]
            .as_str()
            .expect("text content");
        assert!(text.contains("\"gateways\""), "gateway set is listed");
    }

    /// Every generated forwarding tool answers through the endpoint:
    /// each call reaches its live handler, never `unknown tool` and
    /// never `this operation has no tool`. Path fields take a dummy
    /// value, `query` and `body` take empty objects. A handler that
    /// cannot serve the dummy input answers with its own error, which
    /// still proves the dispatch.
    #[tokio::test]
    async fn every_forwarding_tool_answers_through_the_endpoint() {
        let (state, token) = keyed_state();
        let server = McpServer::start(state).await;
        for tool in catalog() {
            let mut arguments = serde_json::Map::new();
            for param in route_scan::path_params(tool.path) {
                arguments.insert(param, serde_json::json!("test"));
            }
            arguments.insert("query".to_string(), serde_json::json!({}));
            if tool.method != "GET" && tool.method != "DELETE" {
                arguments.insert("body".to_string(), serde_json::json!({}));
            }
            let payload = serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": tool.name, "arguments": arguments}
            });
            let answer = tokio::time::timeout(
                std::time::Duration::from_secs(20),
                server
                    .client
                    .post("/mcp?toolsets=all", Some(&token), &payload),
            )
            .await
            .unwrap_or_else(|_| panic!("tool {} answers in time", tool.name));
            let (status, body) = answer;
            assert_eq!(status, StatusCode::OK, "tool {}", tool.name);
            let text = body.to_string();
            assert!(
                !text.contains("unknown tool"),
                "tool {} dispatches",
                tool.name
            );
            assert!(
                !text.contains("this operation has no tool"),
                "tool {} dispatches",
                tool.name
            );
        }
    }
}
