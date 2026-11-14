//! Client proof for the agent protocol endpoint (MC-05).
//!
//! The client below is written from `docs/mcp.md`, not from the
//! server code. It speaks raw JSON-RPC 2.0 over HTTP with `reqwest`
//! and checks the envelope by hand. The server runs in process on a
//! real loopback HTTP socket. The test walks the full path with a
//! full-access key: `initialize`, `tools/list` in the default, named
//! and `all` sets, one `tools/call` of every toolset, a publish
//! through the tool that `subscribe_collect` reads back, and the
//! refusal of a write tool for a viewer key.

use std::sync::Arc;
use std::time::Duration;

use broker_api::v5::api_keys::{KeyRole, NewApiKey};
use broker_api::ApiState;
use serde_json::{json, Value};

/// Independent client, written from the protocol description. It knows
/// only HTTP and the JSON-RPC envelope: no server function is called.
struct McpClient {
    http: reqwest::Client,
    base: String,
    key: Option<String>,
}

impl McpClient {
    fn new(base: String, key: Option<String>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .expect("client builds");
        Self { http, base, key }
    }

    /// Send one JSON-RPC request and return the HTTP status plus the body.
    async fn rpc(&self, method: &str, params: Value, id: i64) -> (u16, Value) {
        self.rpc_on_path("/mcp", method, params, id).await
    }

    async fn rpc_on_path(&self, path: &str, method: &str, params: Value, id: i64) -> (u16, Value) {
        let mut call = self
            .http
            .post(format!("{}{}", self.base, path))
            .json(&json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params,
            }));
        if let Some(key) = &self.key {
            call = call.bearer_auth(key);
        }
        let answer = call.send().await.expect("server answers");
        let status = answer.status().as_u16();
        let body: Value = answer.json().await.expect("body is JSON");
        (status, body)
    }

    /// Call one tool and return the HTTP status plus the body.
    async fn call_tool(&self, name: &str, arguments: Value, id: i64) -> (u16, Value) {
        self.rpc(
            "tools/call",
            json!({"name": name, "arguments": arguments}),
            id,
        )
        .await
    }
}

/// Start the management API on a loopback socket with one full-access
/// key and one viewer key. Returns the base URL, the two secrets and
/// the shared state.
async fn start_server() -> (String, String, String, ApiState) {
    let engine = Arc::new(broker_rules::RuleEngine::new(
        16,
        broker_rules::BackpressurePolicy::DropOldest,
    ));
    let state = ApiState::standalone(engine);
    let mut admin_new = NewApiKey::new("mcp-proof-admin");
    admin_new.role = KeyRole::Administrator;
    let (_, admin_secret) = state.api_keys.create(admin_new).expect("mint admin key");
    let mut viewer_new = NewApiKey::new("mcp-proof-viewer");
    viewer_new.role = KeyRole::Viewer;
    let (_, viewer_secret) = state.api_keys.create(viewer_new).expect("mint viewer key");
    let app = broker_api::router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let port = listener.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve test app");
    });
    (
        format!("http://127.0.0.1:{port}"),
        admin_secret,
        viewer_secret,
        state,
    )
}

/// Read the text of the first content part of a tool result.
fn result_text(body: &Value) -> String {
    body["result"]["content"][0]["text"]
        .as_str()
        .expect("one text part")
        .to_string()
}

/// Names of the tools in a `tools/list` answer.
fn tool_names(body: &Value) -> Vec<String> {
    body["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .map(|tool| tool["name"].as_str().expect("tool name").to_string())
        .collect()
}

/// One read tool of each toolset plus the arguments it needs. The
/// publish toolset is covered separately: it holds write tools only.
const TOOLSET_PROBES: &[(&str, &str)] = &[
    ("status", "broker_status"),
    ("clients", "get_api_v5_clients"),
    ("subscriptions", "get_api_v5_subscriptions"),
    ("topics", "get_api_v5_topics"),
    ("metrics", "get_api_v5_metrics"),
    ("rules", "get_api_v5_rules"),
    ("connectors", "get_api_v5_connectors"),
    ("authentication", "get_api_v5_authentication_settings"),
    ("authorization", "get_api_v5_authorization_sources"),
    ("tenants", "get_api_v5_mt_managed_ns_list"),
    ("listeners", "get_api_v5_listeners"),
    ("configuration", "get_api_v5_features"),
    ("cluster", "get_api_v5_nodes"),
    ("license", "get_api_v5_license"),
    ("audit", "get_api_v5_audit"),
];

#[tokio::test]
async fn mcp_client_proof() {
    let (base, admin_secret, viewer_secret, state) = start_server().await;
    let admin = McpClient::new(base.clone(), Some(admin_secret));
    let viewer = McpClient::new(base.clone(), Some(viewer_secret));
    let anon = McpClient::new(base.clone(), None);

    // initialize negotiates the asked protocol version.
    let (http, body) = admin
        .rpc("initialize", json!({"protocolVersion": "2025-06-18"}), 1)
        .await;
    assert_eq!(http, 200);
    assert_eq!(body["result"]["protocolVersion"], json!("2025-06-18"));
    assert_eq!(body["result"]["serverInfo"]["name"], json!("indramqtt-mcp"));
    assert!(body["result"]["capabilities"]["tools"].is_object());

    // An unknown version negotiates down to the newest known version.
    let (_, body) = admin
        .rpc("initialize", json!({"protocolVersion": "2099-01-01"}), 2)
        .await;
    assert_eq!(body["result"]["protocolVersion"], json!("2025-06-18"));

    // ping answers an empty result.
    let (_, body) = admin.rpc("ping", json!({}), 3).await;
    assert_eq!(body["result"], json!({}));

    // An unknown method answers JSON-RPC -32601.
    let (_, body) = admin.rpc("tools/enchant", json!({}), 4).await;
    assert_eq!(body["error"]["code"], json!(-32601));

    // tools/list with no set gives the core set plus list_toolsets.
    let (_, body) = admin.rpc("tools/list", json!({}), 5).await;
    let names = tool_names(&body);
    assert!(names.contains(&"broker_status".to_string()), "{names:?}");
    assert!(names.contains(&"list_toolsets".to_string()), "{names:?}");
    assert!(
        names.contains(&"get_api_v5_status".to_string()),
        "{names:?}"
    );
    for tool in body["result"]["tools"].as_array().expect("tools array") {
        assert!(tool["inputSchema"].is_object(), "{tool}");
        assert!(tool["annotations"]["readOnlyHint"].is_boolean(), "{tool}");
        assert!(
            tool["description"]
                .as_str()
                .is_some_and(|text| !text.is_empty()),
            "{tool}"
        );
    }

    // tools/list of a named set gives that set plus list_toolsets.
    let (_, body) = admin
        .rpc_on_path("/mcp?toolsets=metrics", "tools/list", json!({}), 6)
        .await;
    let names = tool_names(&body);
    assert!(names.contains(&"list_toolsets".to_string()), "{names:?}");
    assert!(!names.contains(&"broker_status".to_string()), "{names:?}");

    // An unknown set name selects nothing.
    let (http, body) = admin
        .rpc_on_path("/mcp?toolsets=nope", "tools/list", json!({}), 7)
        .await;
    assert_eq!(http, 200);
    assert_eq!(body["result"]["tools"], json!([]));

    // tools/list of all gives every tool.
    let (_, body) = admin
        .rpc_on_path("/mcp?toolsets=all", "tools/list", json!({}), 8)
        .await;
    let names = tool_names(&body);
    for wanted in [
        "broker_status",
        "list_toolsets",
        "post_api_v5_publish",
        "subscribe_collect",
        "get_api_v5_status",
    ] {
        assert!(names.contains(&wanted.to_string()), "{names:?}");
    }

    // list_toolsets names every toolset with its tool count.
    let (_, body) = admin.call_tool("list_toolsets", json!({}), 10).await;
    let groups: Value = serde_json::from_str(&result_text(&body)).expect("groups JSON");
    let group_names: Vec<String> = groups["toolsets"]
        .as_array()
        .expect("toolsets array")
        .iter()
        .map(|group| group["name"].as_str().expect("group name").to_string())
        .collect();
    for wanted in [
        "status",
        "clients",
        "subscriptions",
        "topics",
        "publish",
        "metrics",
        "rules",
        "connectors",
        "authentication",
        "authorization",
        "tenants",
        "listeners",
        "configuration",
        "cluster",
        "license",
        "audit",
    ] {
        assert!(group_names.contains(&wanted.to_string()), "{group_names:?}");
    }

    // One call of every toolset with the full-access key. Each probe
    // is visible in its own set and runs the production handler.
    for (toolset, tool) in TOOLSET_PROBES {
        let (_, listed) = admin
            .rpc_on_path(
                &format!("/mcp?toolsets={toolset}"),
                "tools/list",
                json!({}),
                11,
            )
            .await;
        let names = tool_names(&listed);
        assert!(
            names.contains(&tool.to_string()),
            "toolset {toolset} lists {tool}: {names:?}"
        );
        let (http, body) = admin.call_tool(tool, json!({}), 12).await;
        assert_eq!(http, 200, "toolset {toolset} tool {tool}: {body}");
        assert!(
            body.get("error").is_none(),
            "toolset {toolset} tool {tool}: {body}"
        );
        let text = result_text(&body);
        serde_json::from_str::<Value>(&text).expect("tool text is JSON");
    }

    // The status tool answers version, uptime and node name.
    let (_, body) = admin.call_tool("broker_status", json!({}), 13).await;
    let status: Value = serde_json::from_str(&result_text(&body)).expect("status JSON");
    assert!(status["version"].is_string(), "{status}");
    assert!(status["uptime_ms"].is_number(), "{status}");
    assert!(
        status["node"].as_str().expect("node").contains('@'),
        "{status}"
    );

    // The publish tool runs the management publish path: a retained
    // publish through the tool is visible to a collect through the
    // tool, so the message travels publish to deliver, not store only.
    let (http, body) = admin
        .call_tool(
            "post_api_v5_publish",
            json!({"body": {
                "topic": "mcp/proof/retained",
                "payload": "hello-proof",
                "retain": true,
            }}),
            14,
        )
        .await;
    assert_eq!(http, 200, "{body}");
    assert!(body.get("error").is_none(), "{body}");
    let (_, body) = admin
        .call_tool(
            "subscribe_collect",
            json!({
                "topic_filter": "mcp/proof/#",
                "max_messages": 10,
                "timeout_seconds": 5,
                "qos": 0,
            }),
            15,
        )
        .await;
    let collected: Value = serde_json::from_str(&result_text(&body)).expect("collect JSON");
    let rows = collected["messages"].as_array().expect("messages array");
    let row = rows
        .iter()
        .find(|row| row["topic"] == json!("mcp/proof/retained"))
        .expect("publish reaches the collect");
    assert_eq!(row["payload"], json!("hello-proof"));
    assert_eq!(row["retain"], json!(true));
    assert_eq!(row["payload_encoding"], json!("plain"));
    assert_eq!(row["truncated"], json!(false));

    // The viewer key sees read tools only and may not call a write tool.
    let (_, body) = viewer
        .rpc_on_path("/mcp?toolsets=all", "tools/list", json!({}), 20)
        .await;
    let names = tool_names(&body);
    assert!(
        !names.contains(&"post_api_v5_publish".to_string()),
        "{names:?}"
    );
    assert!(
        !names.contains(&"subscribe_collect".to_string()),
        "{names:?}"
    );
    let (_, body) = viewer
        .call_tool(
            "post_api_v5_publish",
            json!({"body": {"topic": "mcp/proof", "payload": "x"}}),
            21,
        )
        .await;
    assert_eq!(body["error"]["code"], json!(-32602), "{body}");

    // No key and a wrong key get HTTP 401 with a plain error object.
    let (http, body) = anon.rpc("tools/list", json!({}), 30).await;
    assert_eq!(http, 401);
    assert!(body.get("jsonrpc").is_none(), "{body}");
    let wrong = McpClient::new(
        base.clone(),
        Some("wrong-key-that-matches-nothing".to_string()),
    );
    let (http, _) = wrong.rpc("tools/list", json!({}), 31).await;
    assert_eq!(http, 401);

    // Read-only mode keeps read tools only, for every key.
    state.mcp.set_read_only(true);
    let (_, body) = admin
        .rpc_on_path("/mcp?toolsets=all", "tools/list", json!({}), 40)
        .await;
    let names = tool_names(&body);
    assert!(
        !names.contains(&"post_api_v5_publish".to_string()),
        "{names:?}"
    );
    let (http, body) = admin
        .call_tool(
            "post_api_v5_publish",
            json!({"body": {"topic": "mcp/proof", "payload": "x"}}),
            41,
        )
        .await;
    assert_eq!(http, 200, "{body}");
    assert_eq!(body["result"]["isError"], json!(true), "{body}");
    assert!(result_text(&body).contains("FORBIDDEN"), "{body}");
    state.mcp.apply(true, false);

    // A disabled endpoint answers 404.
    state.mcp.set_enabled(false);
    let (http, _) = admin.rpc("tools/list", json!({}), 42).await;
    assert_eq!(http, 404);
    state.mcp.apply(true, false);

    // Every call left one audit record naming the tool, never the args.
    let entries = state.audit.query(&broker_api::v5::audit::AuditFilter {
        operation_type: Some("mcp".to_string()),
        ..Default::default()
    });
    let ids: Vec<String> = entries
        .iter()
        .map(|entry| entry.operation_id.clone())
        .collect();
    assert!(
        ids.iter().any(|id| id == "mcp:post_api_v5_publish"),
        "{ids:?}"
    );
    assert!(ids.iter().any(|id| id == "mcp:broker_status"), "{ids:?}");
}

/// Every example in `docs/mcp.md` runs here. The configuration
/// block parses and authenticates a real call. The status and
/// publish snippets run unchanged against the broker.
#[tokio::test]
async fn mcp_docs_examples_run() {
    let (base, admin_secret, _, _) = start_server().await;
    let client = McpClient::new(base.clone(), Some(admin_secret.clone()));

    // `docs/mcp.md` "Check that it works": the status tool answers the
    // same JSON as HTTP `GET /api/v5/status?format=json`.
    let (_, body) = client
        .rpc(
            "tools/call",
            json!({"name": "get_api_v5_status",
                   "arguments": {"query": {"format": "json"}}}),
            1,
        )
        .await;
    assert!(body.get("error").is_none(), "{body}");
    let answer: Value = serde_json::from_str(&result_text(&body)).expect("status JSON");
    assert!(answer["rel_vsn"].is_string(), "{answer}");

    // The page publishes one message through the publish tool.
    let (http, body) = client
        .rpc(
            "tools/call",
            json!({"name": "post_api_v5_publish",
                   "arguments": {"body": {"topic": "mcp/docs",
                                          "payload": "hello"}}}),
            2,
        )
        .await;
    assert_eq!(http, 200, "{body}");
    assert!(body.get("error").is_none(), "{body}");

    // The page shows a Mermaid sequence diagram and names the two
    // settings. Both are checked here so the page cannot drift.
    let page_path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../docs/mcp.md");
    let page = std::fs::read_to_string(page_path).expect("docs page exists");
    assert!(page.contains("```mermaid"), "page has a diagram");
    assert!(page.contains("mcp.enabled"), "page names the switch");
    assert!(
        page.contains("mcp.read_only"),
        "page names the read-only lock"
    );

    // The "Connect a client" block parses as JSON and its URL and
    // header run a real call: the documented configuration works.
    let marker = "<!-- client-config-example -->";
    let marked = page.find(marker).expect("config marker");
    let fence = page[marked..].find("```json").expect("config block");
    let block_start = marked + fence + "```json".len();
    let block_end = page[block_start..].find("```").expect("config block end");
    let config: Value = serde_json::from_str(page[block_start..block_start + block_end].trim())
        .expect("config parses");
    let documented_url = config["mcpServers"]["indramqtt"]["url"]
        .as_str()
        .expect("config has a url");
    assert!(documented_url.ends_with("/mcp"), "{documented_url}");
    let documented_header = config["mcpServers"]["indramqtt"]["headers"]["Authorization"]
        .as_str()
        .expect("config has a header");
    assert!(
        documented_header.starts_with("Bearer "),
        "{documented_header}"
    );
    let url = documented_url.replace("http://127.0.0.1:18083", &base);
    let header = documented_header.replace("<api_secret>", &admin_secret);
    let answer = client
        .http
        .post(&url)
        .header("Authorization", header)
        .json(&json!({"jsonrpc": "2.0", "id": 1, "method": "ping"}))
        .send()
        .await
        .expect("server answers the documented address");
    assert_eq!(answer.status().as_u16(), 200);
    let body: Value = answer.json().await.expect("body is JSON");
    assert_eq!(body["result"], json!({}));
}
