use std::sync::Arc;
use std::time::Duration;

use broker_api::v5::api_keys::{access_for_path, PathAccess};
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

/// Log in with the default administrator, change the password, and return a token.
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

/// Prove that mapped, public, and unmapped paths classify distinctly.
///
/// Mapped paths carry a scope. Public paths need no credentials.
/// Unknown paths stay closed.
#[test]
fn tr_015_classify_handler() {
    assert_eq!(
        access_for_path("/api/v5/clients"),
        PathAccess::Scope("connections")
    );
    assert_eq!(access_for_path("/api/v5/status"), PathAccess::Anonymous);
    assert_eq!(
        access_for_path("/api/v5/no_such_path_xyz"),
        PathAccess::Closed
    );
}

/// Prove that handler paths resolve to their required scopes.
///
/// Client paths need the connections scope. Publish paths need
/// the publish scope. Unknown handlers stay closed.
#[test]
fn tr_015_handler_scopes() {
    assert_eq!(
        access_for_path("/api/v5/clients"),
        PathAccess::Scope("connections")
    );
    assert_eq!(
        access_for_path("/api/v5/publish"),
        PathAccess::Scope("publish")
    );
    assert_eq!(
        access_for_path("/api/v5/no_such_api_module_xyz"),
        PathAccess::Closed
    );
}

/// Prove that API keys cannot manage API keys.
///
/// An API key request to `/api/v5/api_key` fails with 403 login-only.
/// A scoped API key request to `/api/v5/api_key/some-key` fails with 403 forbidden.
#[tokio::test]
async fn tr_015_handler_scopes_login_only() {
    let (base_url, _state) = start_server().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let create_res = client
        .post(format!("{base_url}/api/v5/api_key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "name": "tr015-scoped-key",
            "role": "viewer",
            "scopes": ["connections", "publish"]
        }))
        .send()
        .await
        .expect("mint api key");
    assert_eq!(create_res.status(), StatusCode::OK);
    let key_body: serde_json::Value = create_res.json().await.expect("key json");
    let key_id = key_body["api_key"].as_str().expect("api_key");
    let secret = key_body["api_secret"].as_str().expect("api_secret");

    let login_only_res = client
        .post(format!("{base_url}/api/v5/api_key"))
        .basic_auth(key_id, Some(secret))
        .json(&json!({
            "name": "tr015-nested-key",
            "role": "viewer"
        }))
        .send()
        .await
        .expect("call api_key collection with key");
    assert_eq!(login_only_res.status(), StatusCode::FORBIDDEN);
    let login_only_body: serde_json::Value = login_only_res.json().await.expect("login_only json");
    assert_eq!(login_only_body["code"], "FORBIDDEN");
    assert_eq!(
        login_only_body["message"],
        "this operation needs the token of a user who logged in"
    );

    let role_only_res = client
        .get(format!("{base_url}/api/v5/api_key/some-key"))
        .basic_auth(key_id, Some(secret))
        .send()
        .await
        .expect("call single api_key with scoped key");
    assert_eq!(role_only_res.status(), StatusCode::FORBIDDEN);
    let role_only_body: serde_json::Value = role_only_res.json().await.expect("role_only json");
    assert_eq!(role_only_body["code"], "FORBIDDEN");
    assert_eq!(
        role_only_body["message"],
        "this API key does not have the right to call this path"
    );
}

/// Prove that public paths declare no scope.
///
/// Status paths need no credentials and no scope.
#[test]
fn tr_015_public_handlers_are_unscoped() {
    assert_eq!(access_for_path("/api/v5/status"), PathAccess::Anonymous);
    assert_ne!(
        access_for_path("/api/v5/status"),
        PathAccess::Scope("connections")
    );
}

/// Prove that scope lists accept known scopes and reject unknown scopes.
#[tokio::test]
async fn tr_015_validate_scopes() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let ok_res = client
        .post(format!("{base_url}/api/v5/api_key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "name": "tr015-valid-scopes",
            "role": "viewer",
            "scopes": ["connections", "publish"]
        }))
        .send()
        .await
        .expect("create key with valid scopes");
    assert_eq!(ok_res.status(), StatusCode::OK);

    let bad_res = client
        .post(format!("{base_url}/api/v5/api_key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "name": "tr015-unknown-scope",
            "role": "viewer",
            "scopes": ["nonexistent_scope_xyz"]
        }))
        .send()
        .await
        .expect("create key with unknown scope");
    assert_eq!(bad_res.status(), StatusCode::BAD_REQUEST);
}

/// Prove that malformed scope values fail with HTTP 400.
#[tokio::test]
async fn tr_015_validate_scopes_bad_input() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let str_res = client
        .post(format!("{base_url}/api/v5/api_key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "name": "tr015-bad-str",
            "role": "viewer",
            "scopes": "not-a-list"
        }))
        .send()
        .await
        .expect("create key with string scopes");
    assert_eq!(str_res.status(), StatusCode::BAD_REQUEST);

    let num_res = client
        .post(format!("{base_url}/api/v5/api_key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "name": "tr015-bad-num",
            "role": "viewer",
            "scopes": 42
        }))
        .send()
        .await
        .expect("create key with numeric scopes");
    assert_eq!(num_res.status(), StatusCode::BAD_REQUEST);
}
