use super::*;

/// Send one JSON-RPC message to `/mcp` with a query string, for
/// example `toolsets=rules`.
#[cfg(test)]
async fn mcp_rpc_with_query(
    server: &TestServer,
    query: &str,
    authorization: &str,
    payload: Value,
) -> (u16, Value) {
    server
        .send_with_authorization(
            "POST",
            &format!("/mcp?{query}"),
            Some(payload),
            Some(authorization),
        )
        .await
}

/// Names of the tools in a `tools/list` answer.
#[cfg(test)]
fn mcp_tool_names(body: &Value) -> Vec<String> {
    body["result"]["tools"]
        .as_array()
        .expect("tools is a list")
        .iter()
        .map(|tool| tool["name"].as_str().expect("tool name").to_string())
        .collect()
}

/// The MCP tool catalog covers every router operation: each row of
/// the build-time route table is a tool or an exclusion, each
/// exclusion names a real row, and tool names are unique snake
/// case. Prints the tool and exclusion counts.
#[test]
fn mcp_tool_catalog_covers_every_route() {
    use crate::mcp::catalog;

    let catalog = catalog::build_catalog();
    let mut tools = 0usize;
    let mut excluded = 0usize;
    for row in catalog::MCP_ROUTE_TABLE {
        match catalog::find_exclusion(row.method, row.path) {
            Some(_) => excluded += 1,
            None => {
                let tool = catalog::tool_by_name(&catalog, row.tool)
                    .unwrap_or_else(|| panic!("no tool for {} {}", row.method, row.path));
                assert_eq!(tool.method, row.method);
                assert_eq!(tool.path, row.path);
                tools += 1;
            }
        }
    }
    // No stale exclusions: each names a real router row.
    for exclusion in catalog::EXCLUSIONS {
        assert!(
            catalog::MCP_ROUTE_TABLE
                .iter()
                .any(|row| row.method == exclusion.method && row.path == exclusion.path),
            "stale exclusion {} {}",
            exclusion.method,
            exclusion.path
        );
    }
    // Tool names are unique stable snake case.
    let mut names: Vec<&str> = catalog.iter().map(|tool| tool.name.as_str()).collect();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), catalog.len(), "tool names are unique");
    for tool in &catalog {
        assert!(
            tool.name
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
            "snake-case tool name: {}",
            tool.name
        );
        // Every tool belongs to a known toolset.
        assert!(
            catalog::TOOLSETS.iter().any(|set| set.name == tool.toolset),
            "tool {} has a known toolset",
            tool.name
        );
        // Path parameters are required schema fields.
        let schema = catalog::input_schema_for(tool);
        let required = schema["required"].as_array().expect("required is a list");
        for param in crate::mcp::route_scan::path_params(tool.path) {
            assert!(
                required.iter().any(|name| name == &json!(param)),
                "tool {} requires {param}",
                tool.name
            );
        }
    }
    // Annotations follow the method and the verb.
    let api_status = catalog::tool_by_name(&catalog, "get_api_v5_status").expect("status tool");
    assert!(api_status.read_only);
    assert!(!api_status.destructive);
    assert_eq!(api_status.method, "GET");
    assert_eq!(api_status.path, "/api/v5/status");
    // The built-in status tool proves the path end to end.
    let broker = catalog::tool_by_name(&catalog, "broker_status").expect("status tool");
    assert!(broker.read_only);
    assert!(!broker.destructive);
    // `/status` and `/api/v5/status` are two routes with two tools.
    let alias = catalog::tool_by_name(&catalog, "get_status").expect("alias tool");
    assert!(alias.read_only);
    assert!(!alias.destructive);
    assert_eq!(alias.method, "GET");
    assert_eq!(alias.path, "/status");
    for (method, path) in [("GET", "/status"), ("GET", "/api/v5/status")] {
        assert!(
            catalog::MCP_ROUTE_TABLE
                .iter()
                .any(|row| row.method == method && row.path == path),
            "route row missing: {method} {path}",
        );
        assert!(
            catalog::find_exclusion(method, path).is_none(),
            "status route is a tool, not an exclusion: {method} {path}",
        );
    }
    let metrics = catalog::tool_by_name(&catalog, "get_api_v5_metrics").expect("metrics tool");
    assert!(metrics.read_only);
    let kick =
        catalog::tool_by_name(&catalog, "post_api_v5_clients_kickout_bulk").expect("kick tool");
    assert!(kick.destructive);
    println!("MCP_TOOLS {tools} MCP_EXCLUSIONS {excluded}");
    // Every catalog entry covers a route row plus the built-in
    // status tool: no other extra tool hides beside the table.
    assert_eq!(catalog.len(), tools + 1);
    // The table is never empty: an empty scan must fail, not pass.
    assert!(!catalog::MCP_ROUTE_TABLE.is_empty());
    // Known operations survive any scanner change.
    for name in [
        "get_status",
        "get_api_v5_status",
        "broker_status",
        "get_api_v5_metrics",
        "post_api_v5_publish",
        "delete_api_v5_rules_by_id",
        "get_api_v1_nodes",
        "post_api_v5_rules",
    ] {
        assert!(
            catalog::tool_by_name(&catalog, name).is_some(),
            "known tool missing: {name}",
        );
    }
}

/// `initialize`, `tools/list` and `tools/call get_api_v5_status`
/// work over a real socket. Missing, wrong and dashboard credentials
/// are refused with 401 and no JSON-RPC body.
#[tokio::test]
async fn mcp_initialize_lists_and_calls_over_a_real_socket() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (key_id, secret) = make_key(&server, &token, json!({"name": "mcp-agent"})).await;
    let bearer = format!("Bearer {secret}");
    let basic = basic_authorization(&key_id, &secret);

    // No key: 401 with the management error shape.
    let (status, body) = server
        .send_with_authorization(
            "POST",
            "/mcp",
            Some(json!({"jsonrpc": "2.0", "method": "ping", "id": 1})),
            None,
        )
        .await;
    assert_eq!(status, 401);
    assert_eq!(body["code"], json!("UNAUTHORIZED"));

    // Wrong key: 401.
    let (status, _) = server
        .send_with_authorization(
            "POST",
            "/mcp",
            Some(json!({"jsonrpc": "2.0", "method": "ping", "id": 1})),
            Some("Bearer wrong-secret"),
        )
        .await;
    assert_eq!(status, 401);

    // Dashboard login token: 401. Agents use API keys.
    let (status, _) = mcp_rpc(
        &server,
        &format!("Bearer {token}"),
        json!({"jsonrpc": "2.0", "method": "ping", "id": 1}),
    )
    .await;
    assert_eq!(status, 401);

    // Unknown method: JSON-RPC -32601.
    let (status, body) = mcp_rpc(
        &server,
        &bearer,
        json!({"jsonrpc": "2.0", "method": "nope", "id": 2}),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["error"]["code"], json!(-32601));

    // Initialize negotiates the asked version.
    let (status, body) = mcp_rpc(
        &server,
        &bearer,
        json!({"jsonrpc": "2.0", "method": "initialize",
                "params": {"protocolVersion": "2025-03-26"}, "id": 3}),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["result"]["protocolVersion"], json!("2025-03-26"));
    assert_eq!(body["result"]["serverInfo"]["name"], json!("indramqtt-mcp"));

    // tools/list holds the status tools with annotations.
    let (status, body) = mcp_rpc(
        &server,
        &bearer,
        json!({"jsonrpc": "2.0", "method": "tools/list", "id": 4}),
    )
    .await;
    assert_eq!(status, 200);
    let tools = body["result"]["tools"].as_array().expect("tools is a list");
    let status_tool = tools
        .iter()
        .find(|tool| tool["name"] == json!("get_api_v5_status"))
        .expect("get_api_v5_status is listed");
    assert_eq!(status_tool["annotations"]["readOnlyHint"], json!(true));
    assert!(
        tools.iter().any(|tool| tool["name"] == json!("get_status")),
        "alias tool is listed",
    );
    assert!(
        tools
            .iter()
            .any(|tool| tool["name"] == json!("broker_status")),
        "built-in status tool is listed",
    );
    assert!(
        tools
            .iter()
            .any(|tool| tool["name"] == json!("list_toolsets")),
        "toolset listing is listed",
    );
    assert!(
        tools
            .iter()
            .any(|tool| tool["name"] == json!("get_api_v5_metrics")),
        "catalog tools are listed",
    );

    // tools/call get_api_v5_status answers exactly as HTTP.
    let (http_status, http_body) = server.get_auth("/api/v5/status?format=json", &token).await;
    assert_eq!(http_status, 200);
    let (status, body) = mcp_rpc(
        &server,
        &bearer,
        json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "get_api_v5_status",
                    "arguments": {"query": {"format": "json"}}}, "id": 5}),
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body["result"].get("isError").is_none(),
        "status succeeds: {body}"
    );
    let text = body["result"]["content"][0]["text"]
        .as_str()
        .expect("text content");
    let answer: Value = serde_json::from_str(text).expect("JSON text");
    assert_eq!(answer, http_body);
    // The built-in status tool answers version, uptime and node.
    let (status, body) = mcp_rpc(
        &server,
        &bearer,
        json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "broker_status", "arguments": {}}, "id": 6}),
    )
    .await;
    assert_eq!(status, 200);
    let text = body["result"]["content"][0]["text"].as_str().expect("text");
    let answer: Value = serde_json::from_str(text).expect("JSON text");
    assert!(answer["version"].is_string());
    assert!(answer["uptime_ms"].is_number());
    assert!(answer["node"].is_string());

    // Basic key credentials work too.
    let (status, _) = mcp_rpc(
        &server,
        &basic,
        json!({"jsonrpc": "2.0", "method": "ping", "id": 6}),
    )
    .await;
    assert_eq!(status, 200);

    // A notification answers 202 with no body.
    let (status, _) = mcp_rpc(
        &server,
        &bearer,
        json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
    )
    .await;
    assert_eq!(status, 202);
}

/// A batch array answers one JSON-RPC array with one entry per
/// request that has an id. An empty batch gets 400. Proves the
/// batch path over a real socket.
#[tokio::test]
async fn mcp_batch_array_answers_each_request() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (_key_id, secret) = make_key(&server, &token, json!({"name": "mcp-batch"})).await;
    let bearer = format!("Bearer {secret}");
    let (status, body) = mcp_rpc(
        &server,
        &bearer,
        json!([
            {"jsonrpc": "2.0", "method": "ping", "id": 1},
            {"jsonrpc": "2.0", "method": "notifications/initialized"},
            {"jsonrpc": "2.0", "method": "ping", "id": 2},
        ]),
    )
    .await;
    assert_eq!(status, 200);
    let answers = body.as_array().expect("batch answer is a list");
    assert_eq!(answers.len(), 2);
    assert_eq!(answers[0]["id"], json!(1));
    assert_eq!(answers[1]["id"], json!(2));
    let (status, _) = mcp_rpc(&server, &bearer, json!([])).await;
    assert_eq!(status, 400);
}

/// A key without the `monitoring` scope gets 403 on `/mcp`.
/// Proves the entry scope gate over a real socket: the same key
/// still calls the paths its own scope allows over HTTP.
#[tokio::test]
async fn mcp_key_without_monitoring_scope_gets_403() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (_key_id, secret) = make_key(
        &server,
        &token,
        json!({"name": "mcp-audit", "scopes": ["audit"]}),
    )
    .await;
    let (status, body) = mcp_rpc(
        &server,
        &format!("Bearer {secret}"),
        json!({"jsonrpc": "2.0", "method": "ping", "id": 1}),
    )
    .await;
    assert_eq!(status, 403);
    assert_eq!(body["code"], json!("FORBIDDEN"));
    // The same key still calls its own scope over HTTP.
    let (status, _) = server.get_auth("/api/v5/audit", &secret).await;
    assert_eq!(status, 200);
}

/// One GET tool, one POST tool with a body and one DELETE tool
/// answer through `/mcp` exactly as through HTTP.
#[tokio::test]
async fn mcp_tools_match_http_for_get_post_delete() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (key_id, secret) = make_key(&server, &token, json!({"name": "mcp-parity"})).await;
    let bearer = format!("Bearer {secret}");

    // GET: stable settings read, identical bodies.
    let (http_status, http_body) = server
        .get_basic("/api/v5/license/setting", &key_id, &secret)
        .await;
    assert_eq!(http_status, 200);
    let (status, body) = mcp_rpc(
        &server,
        &bearer,
        json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "get_api_v5_license_setting", "arguments": {}}, "id": 1}),
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body["result"].get("isError").is_none(),
        "GET succeeds: {body}"
    );
    let text = body["result"]["content"][0]["text"].as_str().expect("text");
    let mcp_body: Value = serde_json::from_str(text).expect("JSON text");
    assert_eq!(mcp_body, http_body);

    // POST with a body: stateless rule test, identical bodies.
    let probe = json!({
        "sql": "SELECT temperature FROM \"sensors/+\" WHERE temperature > 0",
        "topic": "sensors/kitchen",
        "payload": {"temperature": 72.5},
    });
    let (http_status, http_body) = server
        .post_auth("/api/v5/rule_test", probe.clone(), &token)
        .await;
    assert_eq!(http_status, 200);
    let (status, body) = mcp_rpc(
            &server,
            &bearer,
            json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "post_api_v5_rule_test", "arguments": {"body": probe}}, "id": 2}),
        )
        .await;
    assert_eq!(status, 200);
    let text = body["result"]["content"][0]["text"].as_str().expect("text");
    let mcp_body: Value = serde_json::from_str(text).expect("JSON text");
    assert_eq!(mcp_body, http_body);
    assert_eq!(mcp_body["matched"], json!(true));

    // DELETE of a missing rule: 404 on both sides.
    let (http_status, http_body) = server
        .delete_auth("/api/v5/rules/no-such-rule-mcp", &token)
        .await;
    assert_eq!(http_status, 404);
    let (status, body) = mcp_rpc(
        &server,
        &bearer,
        json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "delete_api_v5_rules_by_id",
                    "arguments": {"id": "no-such-rule-mcp"}}, "id": 3}),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["result"]["isError"], json!(true));
    let text = body["result"]["content"][0]["text"].as_str().expect("text");
    let code = http_body["code"].as_str().unwrap_or_default();
    assert!(
        !code.is_empty() && text.contains(code),
        "MCP error carries the API code: {text}",
    );

    // Real round trip: create over HTTP, delete over MCP, gone over HTTP.
    let (status, created) = server
        .post_auth(
            "/api/v5/rules",
            json!({"name": "mcp-del", "sql": "SELECT * FROM \"mc02/del\"",
                    "actions": [{"function": "console"}]}),
            &token,
        )
        .await;
    assert_eq!(status, 201, "create: {created}");
    let rule_id = created["id"].as_str().expect("rule id").to_string();
    let (status, body) = mcp_rpc(
        &server,
        &bearer,
        json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "delete_api_v5_rules_by_id",
                    "arguments": {"id": rule_id}}, "id": 4}),
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body["result"].get("isError").is_none(),
        "delete succeeds: {body}"
    );
    let (status, _) = server
        .get_auth(&format!("/api/v5/rules/{rule_id}"), &token)
        .await;
    assert_eq!(status, 404);
}

/// A publish through `/mcp` fires a rule through the broker: the
/// rule metrics move. Proves the tools drive broker state, not a
/// store.
#[tokio::test]
async fn mcp_publish_through_broker_fires_a_rule() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (_key_id, secret) = make_key(&server, &token, json!({"name": "mcp-broker"})).await;
    let bearer = format!("Bearer {secret}");

    // Create the rule through MCP.
    let (status, body) = mcp_rpc(
        &server,
        &bearer,
        json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "post_api_v5_rules", "arguments": {"body": {
                    "name": "mcp-fire",
                    "sql": "SELECT * FROM \"mc02/fire\"",
                    "actions": [{"function": "console"}],
                }}}, "id": 1}),
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body["result"].get("isError").is_none(),
        "create succeeds: {body}"
    );
    let text = body["result"]["content"][0]["text"].as_str().expect("text");
    let created: Value = serde_json::from_str(text).expect("JSON text");
    let rule_id = created["id"].as_str().expect("rule id").to_string();

    // Publish through MCP: the management publish path dispatches
    // ingress through the rule engine.
    let (status, body) = mcp_rpc(
        &server,
        &bearer,
        json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "post_api_v5_publish", "arguments": {"body": {
                    "topic": "mc02/fire", "payload": "{\"k\": 1}", "qos": 0,
                }}}, "id": 2}),
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body["result"].get("isError").is_none(),
        "publish succeeds: {body}"
    );

    // The rule saw the publish through the broker.
    let (status, metrics) = server
        .get_auth(&format!("/api/v5/rules/{rule_id}/metrics"), &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(metrics["metrics"]["matched"], json!(1));
}

/// `mcp.enabled = false` in `indra.toml` disables `POST /mcp`
/// through the socket. The test resolves the file with the
/// production layer loader and applies it with the same call boot
/// uses. `INDRA_MCP__ENABLED=true` wins over the file.
#[tokio::test]
async fn mcp_enabled_from_indra_toml_answers_404_through_socket() {
    // A file value of false disables the endpoint.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir().join(format!(
        "indramqtt-mcp-enabled-{nanos}-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create config dir");
    std::fs::write(dir.join("indra.toml"), "[mcp]\nenabled = false\n").expect("write indra.toml");
    let layered = broker_config::layers::load_layered_with_env(
        &dir,
        &[],
        &broker_config::layers::CliOverrides::default(),
    )
    .expect("layers load");
    assert!(!layered.config().mcp.enabled);
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (_key_id, secret) = make_key(&server, &token, json!({"name": "mcp-file"})).await;
    // Boot applies the resolved section with this call.
    let mcp = layered.config().mcp.clone();
    state.mcp.apply(mcp.enabled, mcp.read_only);
    let (status, body) = mcp_rpc(
        &server,
        &format!("Bearer {secret}"),
        json!({"jsonrpc": "2.0", "method": "ping", "id": 1}),
    )
    .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    // An environment value of true enables the endpoint again.
    let layered_env = broker_config::layers::load_layered_with_env(
        &dir,
        &[("INDRA_MCP__ENABLED".to_string(), "true".to_string())],
        &broker_config::layers::CliOverrides::default(),
    )
    .expect("env layers load");
    assert!(layered_env.config().mcp.enabled);
    let mcp = layered_env.config().mcp.clone();
    state.mcp.apply(mcp.enabled, mcp.read_only);
    let (status, _) = mcp_rpc(
        &server,
        &format!("Bearer {secret}"),
        json!({"jsonrpc": "2.0", "method": "ping", "id": 2}),
    )
    .await;
    assert_eq!(status, 200);
    std::fs::remove_dir_all(&dir).ok();
}

/// `mcp.read_only = true` in `indra.toml` keeps reads and refuses
/// writes through the socket. `INDRA_MCP__READ_ONLY=false` lifts it.
#[tokio::test]
async fn mcp_read_only_from_indra_toml_refuses_writes_through_socket() {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir().join(format!(
        "indramqtt-mcp-readonly-{nanos}-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create config dir");
    std::fs::write(dir.join("indra.toml"), "[mcp]\nread_only = true\n").expect("write indra.toml");
    let layered = broker_config::layers::load_layered_with_env(
        &dir,
        &[],
        &broker_config::layers::CliOverrides::default(),
    )
    .expect("layers load");
    assert!(layered.config().mcp.read_only);
    // The schema default allows writes.
    assert!(!broker_config::schema::McpConf::default().read_only);
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (_key_id, secret) = make_key(&server, &token, json!({"name": "mcp-ro"})).await;
    let bearer = format!("Bearer {secret}");
    let mcp = layered.config().mcp.clone();
    state.mcp.apply(mcp.enabled, mcp.read_only);
    // A GET tool still succeeds.
    let (status, body) = mcp_rpc(
        &server,
        &bearer,
        json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "get_api_v5_license_setting",
                    "arguments": {}}, "id": 1}),
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body["result"].get("isError").is_none(),
        "read succeeds: {body}"
    );
    // A POST tool is refused with the API forbidden code.
    let (status, body) = mcp_rpc(
        &server,
        &bearer,
        json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "post_api_v5_rule_test", "arguments": {"body": {
                    "sql": "SELECT * FROM \"mc02/ro\"",
                    "topic": "mc02/ro",
                    "payload": {"k": 1},
                }}}, "id": 2}),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["result"]["isError"], json!(true));
    let text = body["result"]["content"][0]["text"].as_str().expect("text");
    assert!(
        text.contains("FORBIDDEN"),
        "refusal carries the API code: {text}",
    );
    // A DELETE tool is refused too.
    let (status, body) = mcp_rpc(
        &server,
        &bearer,
        json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "delete_api_v5_rules_by_id",
                    "arguments": {"id": "no-such-rule-mcp-ro"}}, "id": 3}),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["result"]["isError"], json!(true));
    // An environment value of false lifts the restriction.
    let layered_env = broker_config::layers::load_layered_with_env(
        &dir,
        &[("INDRA_MCP__READ_ONLY".to_string(), "false".to_string())],
        &broker_config::layers::CliOverrides::default(),
    )
    .expect("env layers load");
    assert!(!layered_env.config().mcp.read_only);
    let mcp = layered_env.config().mcp.clone();
    state.mcp.apply(mcp.enabled, mcp.read_only);
    let (status, body) = mcp_rpc(
        &server,
        &bearer,
        json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "post_api_v5_rule_test", "arguments": {"body": {
                    "sql": "SELECT * FROM \"mc02/ro\"",
                    "topic": "mc02/ro",
                    "payload": {"k": 1},
                }}}, "id": 4}),
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        body["result"].get("isError").is_none(),
        "write succeeds again: {body}"
    );
    std::fs::remove_dir_all(&dir).ok();
}
/// Default, named and full toolset selection through `/mcp`.
#[tokio::test]
async fn mcp_toolsets_default_named_and_all() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (_key_id, secret) = make_key(&server, &token, json!({"name": "mcp-sets"})).await;
    let bearer = format!("Bearer {secret}");
    let list = json!({"jsonrpc": "2.0", "method": "tools/list", "id": 1});

    // No parameter: the core set only.
    let (status, body) = mcp_rpc(&server, &bearer, list.clone()).await;
    assert_eq!(status, 200);
    let names = mcp_tool_names(&body);
    assert!(names.contains(&"broker_status".to_string()));
    assert!(names.contains(&"list_toolsets".to_string()));
    assert!(names.contains(&"get_api_v5_metrics".to_string()));
    assert!(
        !names.contains(&"post_api_v5_rules".to_string()),
        "default set holds no rules tool: {names:?}"
    );

    // A named set: rules tools, no client tools.
    let (status, body) = mcp_rpc_with_query(&server, "toolsets=rules", &bearer, list.clone()).await;
    assert_eq!(status, 200);
    let names = mcp_tool_names(&body);
    assert!(names.contains(&"list_toolsets".to_string()));
    assert!(names.contains(&"post_api_v5_rules".to_string()));
    assert!(
        !names.contains(&"get_api_v5_clients".to_string()),
        "rules set holds no client tool: {names:?}"
    );

    // `all`: both groups at once.
    let (status, body) = mcp_rpc_with_query(&server, "toolsets=all", &bearer, list.clone()).await;
    assert_eq!(status, 200);
    let names = mcp_tool_names(&body);
    assert!(names.contains(&"post_api_v5_rules".to_string()));
    assert!(names.contains(&"get_api_v5_clients".to_string()));
    assert!(names.contains(&"list_toolsets".to_string()));
}

/// `list_toolsets` names every group with its tool count.
#[tokio::test]
async fn mcp_list_toolsets_names_groups_and_counts() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (_key_id, secret) = make_key(&server, &token, json!({"name": "mcp-groups"})).await;
    let (status, body) = mcp_rpc(
        &server,
        &format!("Bearer {secret}"),
        json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "list_toolsets", "arguments": {}}, "id": 1}),
    )
    .await;
    assert_eq!(status, 200);
    let text = body["result"]["content"][0]["text"].as_str().expect("text");
    let answer: Value = serde_json::from_str(text).expect("JSON text");
    let sets = answer["toolsets"].as_array().expect("toolsets is a list");
    let names: Vec<&str> = sets
        .iter()
        .map(|set| set["name"].as_str().expect("set name"))
        .collect();
    for expected in ["status", "clients", "rules", "tenants"] {
        assert!(names.contains(&expected), "group listed: {expected}");
    }
    for set in sets {
        assert!(set["tool_count"].as_u64().expect("count") > 0);
        assert!(!set["tools"].as_array().expect("tools").is_empty());
    }
}

/// A viewer key sees read tools only and cannot call a write tool.
#[tokio::test]
async fn mcp_viewer_key_sees_and_calls_read_only() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (_key_id, secret) = make_key(
        &server,
        &token,
        json!({"name": "mcp-viewer", "role": "viewer"}),
    )
    .await;
    let bearer = format!("Bearer {secret}");

    // The list holds reads and hides writes.
    let (status, body) = mcp_rpc_with_query(
        &server,
        "toolsets=all",
        &bearer,
        json!({"jsonrpc": "2.0", "method": "tools/list", "id": 1}),
    )
    .await;
    assert_eq!(status, 200);
    let names = mcp_tool_names(&body);
    assert!(names.contains(&"broker_status".to_string()));
    assert!(names.contains(&"list_toolsets".to_string()));
    assert!(
        !names.contains(&"post_api_v5_publish".to_string()),
        "viewer sees no write tool: {names:?}"
    );

    // A write call is refused with a JSON-RPC error.
    let (status, body) = mcp_rpc_with_query(
        &server,
        "toolsets=all",
        &bearer,
        json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "post_api_v5_publish", "arguments": {"body": {
                    "topic": "mc03/denied", "payload": "{}",
                }}}, "id": 2}),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["error"]["code"], json!(-32602));

    // A read call still works.
    let (status, body) = mcp_rpc_with_query(
        &server,
        "toolsets=all",
        &bearer,
        json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "broker_status", "arguments": {}}, "id": 3}),
    )
    .await;
    assert_eq!(status, 200);
    assert!(body["result"]["content"][0]["text"].is_string());
}

/// Read-only mode lists read tools only, for every key.
#[tokio::test]
async fn mcp_read_only_mode_hides_and_refuses_writes() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (_key_id, secret) = make_key(&server, &token, json!({"name": "mcp-ro"})).await;
    let bearer = format!("Bearer {secret}");
    state.mcp.set_read_only(true);

    // The list holds reads and hides writes.
    let (status, body) = mcp_rpc_with_query(
        &server,
        "toolsets=all",
        &bearer,
        json!({"jsonrpc": "2.0", "method": "tools/list", "id": 1}),
    )
    .await;
    assert_eq!(status, 200);
    let names = mcp_tool_names(&body);
    assert!(names.contains(&"broker_status".to_string()));
    assert!(
        !names.contains(&"post_api_v5_publish".to_string()),
        "read-only mode lists no write tool: {names:?}"
    );

    // A write call is refused even with a full-access key.
    let (status, body) = mcp_rpc_with_query(
        &server,
        "toolsets=all",
        &bearer,
        json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "post_api_v5_publish", "arguments": {"body": {
                    "topic": "mc03/readonly", "payload": "{}",
                }}}, "id": 2}),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["result"]["isError"], json!(true));
    let text = body["result"]["content"][0]["text"].as_str().expect("text");
    assert!(
        text.contains("FORBIDDEN"),
        "refusal carries the API code: {text}"
    );
    state.mcp.set_read_only(false);
}

/// the tool name and the result code.
#[tokio::test]
async fn mcp_call_writes_one_audit_record() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (_key_id, secret) = make_key(&server, &token, json!({"name": "mcp-audit"})).await;
    let bearer = format!("Bearer {secret}");

    let (status, _) = mcp_rpc(
        &server,
        &bearer,
        json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "broker_status", "arguments": {}}, "id": 1}),
    )
    .await;
    assert_eq!(status, 200);

    // The audit log holds the call record.
    let (status, page) = server
        .send_with_authorization(
            "GET",
            "/api/v5/audit?from=mcp&operation_id=mcp:broker_status",
            None,
            Some(&bearer),
        )
        .await;
    assert_eq!(status, 200);
    let rows = page[0]["data"].as_array().expect("audit data");
    let record = rows
        .iter()
        .find(|row| {
            row["source"] == json!("api-key:mcp-audit")
                && row["operation_id"] == json!("mcp:broker_status")
        })
        .expect("audit record exists after the call");
    assert_eq!(record["operation_result"], json!("success"));
    assert_eq!(record["from"], json!("mcp"));
}

/// A disabled endpoint answers 404, even with a valid key.
#[tokio::test]
async fn mcp_disabled_path_answers_404() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (_key_id, secret) = make_key(&server, &token, json!({"name": "mcp-off"})).await;
    state.mcp.set_enabled(false);
    let (status, body) = mcp_rpc(
        &server,
        &format!("Bearer {secret}"),
        json!({"jsonrpc": "2.0", "method": "ping", "id": 1}),
    )
    .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    state.mcp.set_enabled(true);
}

/// `mcp.enabled` in `indra.toml` drives `POST /mcp` through the socket.
#[tokio::test]
async fn mcp_enabled_applies_from_indra_toml() {
    // A file value of false disables the endpoint.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir().join(format!(
        "indramqtt-mcp-enabled-{nanos}-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create config dir");
    std::fs::write(dir.join("indra.toml"), "[mcp]\nenabled = false\n").expect("write indra.toml");
    let layered = broker_config::layers::load_layered_with_env(
        &dir,
        &[],
        &broker_config::layers::CliOverrides::default(),
    )
    .expect("layers load");
    assert!(!layered.config().mcp.enabled);
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (_key_id, secret) = make_key(&server, &token, json!({"name": "mcp-file"})).await;
    // Boot applies the resolved value to the switch.
    state.mcp.set_enabled(layered.config().mcp.enabled);
    let (status, _) = mcp_rpc(
        &server,
        &format!("Bearer {secret}"),
        json!({"jsonrpc": "2.0", "method": "ping", "id": 1}),
    )
    .await;
    assert_eq!(status, 404);
    // An environment value of true enables the endpoint again.
    let layered_env = broker_config::layers::load_layered_with_env(
        &dir,
        &[("INDRA_MCP__ENABLED".to_string(), "true".to_string())],
        &broker_config::layers::CliOverrides::default(),
    )
    .expect("env layers load");
    assert!(layered_env.config().mcp.enabled);
    state.mcp.set_enabled(layered_env.config().mcp.enabled);
    let (status, _) = mcp_rpc(
        &server,
        &format!("Bearer {secret}"),
        json!({"jsonrpc": "2.0", "method": "ping", "id": 2}),
    )
    .await;
    assert_eq!(status, 200);
    std::fs::remove_dir_all(&dir).ok();
}

/// `mcp.read_only` in `indra.toml` drives `/mcp` through the socket.
#[tokio::test]
async fn mcp_read_only_applies_from_indra_toml() {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir().join(format!(
        "indramqtt-mcp-readonly-{nanos}-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create config dir");
    std::fs::write(dir.join("indra.toml"), "[mcp]\nread_only = true\n").expect("write indra.toml");
    let layered = broker_config::layers::load_layered_with_env(
        &dir,
        &[],
        &broker_config::layers::CliOverrides::default(),
    )
    .expect("layers load");
    assert!(layered.config().mcp.read_only);
    // The environment wins over the file.
    let layered_env = broker_config::layers::load_layered_with_env(
        &dir,
        &[("INDRA_MCP__READ_ONLY".to_string(), "false".to_string())],
        &broker_config::layers::CliOverrides::default(),
    )
    .expect("env layers load");
    assert!(!layered_env.config().mcp.read_only);
    std::fs::remove_dir_all(&dir).ok();
}

/// A 3 MiB call reaches the handler. A call just over 8 MiB gets 413.
/// Both go through a socket.
#[tokio::test]
async fn mcp_body_cap_is_eight_mebibytes() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (_key_id, secret) = make_key(&server, &token, json!({"name": "mcp-limits"})).await;
    let bearer = format!("Bearer {secret}");

    // A 3 MiB envelope passes the transport and gets a method answer.
    let big_pad = "p".repeat(3 * 1024 * 1024);
    let (status, body) = mcp_rpc(
        &server,
        &bearer,
        json!({"jsonrpc": "2.0", "method": "ping",
                "params": {"pad": big_pad}, "id": 1}),
    )
    .await;
    assert_eq!(status, 200);
    assert!(body["result"].is_object(), "ping answers: {body}");

    // A body exactly one byte over 8 MiB is refused with 413. The
    // fixed overhead keeps the serialized body at the boundary so
    // the socket write finishes before the server refuses it.
    let empty = json!({"jsonrpc": "2.0", "method": "ping",
            "params": {"pad": ""}, "id": 2});
    let overhead = serde_json::to_vec(&empty).expect("encode body").len();
    let huge_pad = "p".repeat(crate::mcp::MCP_MAX_BODY_BYTES + 1 - overhead);
    let (status, body) = mcp_rpc(
        &server,
        &bearer,
        json!({"jsonrpc": "2.0", "method": "ping",
                "params": {"pad": huge_pad}, "id": 2}),
    )
    .await;
    assert_eq!(status, 413);
    assert_eq!(body["code"], json!("PAYLOAD_TOO_LARGE"));
}
