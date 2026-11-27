//! Rust equivalents for bridge action rows, part 21.
//!
//! Holds the tests that prove our code keeps the behaviours of the
//! reference bridge suites that have an operator path.

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

/// Prove that create without `enable` stores `true`.
///
/// Create one action and one source without the `enable` field.
/// Check the create answers and the later reads hold `true`.
/// Removing the default makes the `enable` assertions fail.
#[tokio::test]
async fn tr_021_raw_config_enable_defaults_true() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let action_res = client
        .post(format!("{base_url}/api/v5/actions"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "name": "tr021-raw-action",
            "type": "console",
            "server": "127.0.0.1:1883",
        }))
        .send()
        .await
        .expect("create action without enable");
    assert_eq!(action_res.status(), StatusCode::CREATED);
    let action_body: serde_json::Value = action_res.json().await.expect("action json");
    assert_eq!(action_body["enable"], json!(true));

    let action_get = client
        .get(format!("{base_url}/api/v5/actions/tr021-raw-action"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("read action");
    assert_eq!(action_get.status(), StatusCode::OK);
    let action_read: serde_json::Value = action_get.json().await.expect("read json");
    assert_eq!(action_read["enable"], json!(true));

    let source_res = client
        .post(format!("{base_url}/api/v5/sources"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "name": "tr021-raw-source",
            "type": "mqtt",
            "connector": "mqtt:up",
            "parameters": {"topic": "in/tr021/raw"},
        }))
        .send()
        .await
        .expect("create source without enable");
    assert_eq!(source_res.status(), StatusCode::CREATED);
    let source_body: serde_json::Value = source_res.json().await.expect("source json");
    assert_eq!(source_body["enable"], json!(true));

    let source_get = client
        .get(format!("{base_url}/api/v5/sources/tr021-raw-source"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("read source");
    assert_eq!(source_get.status(), StatusCode::OK);
    let source_read: serde_json::Value = source_get.json().await.expect("source read json");
    assert_eq!(source_read["enable"], json!(true));
}
