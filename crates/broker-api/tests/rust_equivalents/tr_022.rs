//! Rust equivalents for bridge summary rows, part 22.
//!
//! Holds the test that proves our code keeps the list summary
//! behaviour of the bridge suites through the operator path.

use std::sync::Arc;
use std::time::Duration;

use broker_api::ApiState;
use reqwest::StatusCode;
use serde_json::json;

/// Start the management API on a loopback socket.
///
/// Set up the server with `ApiState::standalone`.
/// Bind a loopback listener, and serve requests with Axum.
/// Return the base URL and the state.
async fn start_server() -> (String, ApiState) {
    let engine = Arc::new(broker_rules::RuleEngine::new(
        16,
        broker_rules::BackpressurePolicy::DropOldest,
    ));
    let state = ApiState::standalone(engine);
    let app = broker_api::router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let port = listener.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve test app");
    });
    (format!("http://127.0.0.1:{port}"), state)
}

/// Log in with the default administrator and return a bearer token.
///
/// Change the default password first, then log in with the new password.
async fn login_admin(client: &reqwest::Client, base_url: &str) -> String {
    let fresh_res = client
        .post(format!("{base_url}/api/v5/login"))
        .json(&json!({"username": "admin", "password": "public"}))
        .send()
        .await
        .expect("initial admin login");
    assert_eq!(fresh_res.status(), StatusCode::OK);
    let fresh_body: serde_json::Value = fresh_res.json().await.expect("fresh json");
    let fresh_token = fresh_body["token"].as_str().expect("fresh token");

    let change_pwd_res = client
        .put(format!("{base_url}/api/v5/users/admin/change_pwd"))
        .bearer_auth(fresh_token)
        .json(&json!({"old_pwd": "public", "new_pwd": "Adm1n-test-pass!"}))
        .send()
        .await
        .expect("change admin password");
    assert_eq!(change_pwd_res.status(), StatusCode::NO_CONTENT);

    let login_res = client
        .post(format!("{base_url}/api/v5/login"))
        .json(&json!({"username": "admin", "password": "Adm1n-test-pass!"}))
        .send()
        .await
        .expect("login with new admin password");
    assert_eq!(login_res.status(), StatusCode::OK);
    let login_body: serde_json::Value = login_res.json().await.expect("login json");
    login_body["token"]
        .as_str()
        .expect("admin token")
        .to_string()
}

/// Prove that the action summary lists one enabled action.
///
/// Create one console action without the enable flag.
/// Read the summary and check the live fields of the row.
/// Removing the summary status makes the status assertion fail.
#[tokio::test]
async fn tr_022_summary_lists_enabled_action() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let create_res = client
        .post(format!("{base_url}/api/v5/actions"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "name": "tr022-summary-action",
            "type": "console",
            "server": "127.0.0.1:1883",
        }))
        .send()
        .await
        .expect("create action without enable");
    assert_eq!(create_res.status(), StatusCode::CREATED);
    let created: serde_json::Value = create_res.json().await.expect("create json");
    assert_eq!(created["enable"], json!(true));

    let summary_res = client
        .get(format!("{base_url}/api/v5/actions_summary"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("read actions summary");
    assert_eq!(summary_res.status(), StatusCode::OK);
    let rows: serde_json::Value = summary_res.json().await.expect("summary json");
    let row = rows
        .as_array()
        .expect("summary is a list")
        .iter()
        .find(|entry| entry["name"] == json!("tr022-summary-action"))
        .expect("created action present");
    assert_eq!(row["enable"], json!(true));
    assert_eq!(row["type"], json!("console"));
    assert_eq!(row["status"], json!("connected"));
    assert_eq!(row["node_status"][0]["status"], json!("connected"));
    assert_eq!(row["rules"], json!([]));
}
