//! MCP tool descriptions integration test (MX-01).
//!
//! Verifies that `tools/list` over `POST /mcp` provides clean descriptions
//! without garbled doc-comment text for status tools.

use std::sync::Arc;
use std::time::Duration;

use broker_api::v5::api_keys::{KeyRole, NewApiKey};
use broker_api::ApiState;
use serde_json::{json, Value};

/// Start the management API on a loopback TCP socket with an Administrator key.
async fn start_server() -> (String, String) {
    let engine = Arc::new(broker_rules::RuleEngine::new(
        16,
        broker_rules::BackpressurePolicy::DropOldest,
    ));
    let state = ApiState::standalone(engine);
    let mut admin_new = NewApiKey::new("mcp-descriptions-admin");
    admin_new.role = KeyRole::Administrator;
    let (_, admin_secret) = state.api_keys.create(admin_new).expect("mint admin key");

    let app = broker_api::router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let port = listener.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve test app");
    });

    (format!("http://127.0.0.1:{port}"), admin_secret)
}

#[tokio::test]
async fn test_tools_list_status_descriptions() {
    let (base, admin_key) = start_server().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .expect("build client");

    let response = client
        .post(format!("{base}/mcp"))
        .bearer_auth(admin_key)
        .json(&json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/list",
            "params": {}
        }))
        .send()
        .await
        .expect("send tools/list request");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: Value = response.json().await.expect("parse response body");
    let tools = body["result"]["tools"]
        .as_array()
        .expect("tools array in result");

    let api_v5_status = tools
        .iter()
        .find(|tool| tool["name"] == "get_api_v5_status")
        .expect("find get_api_v5_status tool");
    let status_tool = tools
        .iter()
        .find(|tool| tool["name"] == "get_status")
        .expect("find get_status tool");

    let api_v5_desc = api_v5_status["description"]
        .as_str()
        .expect("description must be a string");
    let status_desc = status_tool["description"]
        .as_str()
        .expect("description must be a string");

    // Exact expected descriptions
    assert_eq!(api_v5_desc, "Node status read. (GET /api/v5/status)");
    assert_eq!(status_desc, "Node status read. (GET /status)");

    // Neither description may contain garbled comment fragments
    assert!(
        !api_v5_desc.contains("as the ("),
        "get_api_v5_status description contains 'as the (': {api_v5_desc}"
    );
    assert!(
        !status_desc.contains("as the ("),
        "get_status description contains 'as the (': {status_desc}"
    );
}
