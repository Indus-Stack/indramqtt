use std::sync::Arc;
use std::time::Duration;

use broker_api::admin_users::AdminRole;
use broker_api::ApiState;
use reqwest::StatusCode;
use serde_json::json;

/// Start the management API on a loopback socket.
///
/// Sets up the server with `ApiState::standalone`, binds a loopback
/// listener, and serves requests with Axum. Returns the base URL and
/// the state.
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

/// Catalog of all 14 administrator dashboard scopes.
fn full_admin_scopes() -> Vec<&'static str> {
    vec![
        "connections",
        "publish",
        "data_integration",
        "access_control",
        "gateways",
        "monitoring",
        "cluster_operations",
        "system",
        "audit",
        "license",
        "user_management",
        "mfa_management",
        "sso_management",
        "api_key_management",
    ]
}

/// Catalog of common viewer dashboard scopes.
fn viewer_scopes() -> Vec<&'static str> {
    vec![
        "connections",
        "publish",
        "data_integration",
        "access_control",
        "gateways",
        "monitoring",
        "cluster_operations",
        "system",
        "audit",
        "license",
    ]
}

/// Log in with the default administrator, clear the password flag, and return a bearer token.
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

/// Prove that sending a shuffled full scope catalog to the default administrator normalizes to unset.
#[tokio::test]
async fn tr_009_default_admin_put_full_list_shuffled() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let mut shuffled = full_admin_scopes();
    shuffled.reverse();

    let put_payload = json!({
        "role": "administrator",
        "description": "shuffled scope test",
        "scopes": shuffled
    });
    let put_res = client
        .put(format!("{base_url}/api/v5/users/admin"))
        .bearer_auth(&admin_token)
        .json(&put_payload)
        .send()
        .await
        .expect("update admin with shuffled scope list");
    assert_eq!(put_res.status(), StatusCode::OK);
    let body: serde_json::Value = put_res.json().await.expect("parse put response");
    assert_eq!(body["description"], "shuffled scope test");
    assert_eq!(body["scopes"], "unset");
}

/// Prove that updating default administrator with the unset sentinel preserves the unset state.
#[tokio::test]
async fn tr_009_default_admin_put_unset_sentinel() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let put_payload = json!({
        "role": "administrator",
        "description": "unset sentinel test",
        "scopes": "unset"
    });
    let put_res = client
        .put(format!("{base_url}/api/v5/users/admin"))
        .bearer_auth(&admin_token)
        .json(&put_payload)
        .send()
        .await
        .expect("update admin with unset sentinel");
    assert_eq!(put_res.status(), StatusCode::OK);
    let body: serde_json::Value = put_res.json().await.expect("parse put response");
    assert_eq!(body["description"], "unset sentinel test");
    assert_eq!(body["scopes"], "unset");
}

/// Prove that user records without explicit scopes render the unset sentinel on read.
#[tokio::test]
async fn tr_009_legacy_record_shows_unset_sentinel() {
    let (base_url, state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    // Simulate a legacy record without explicit scopes in the user store.
    state
        .admin_users
        .create_with_scopes(
            "legacy_user",
            "Legacy-pwd-1!",
            AdminRole::Administrator,
            "legacy record",
            None,
        )
        .expect("insert legacy record");

    let get_res = client
        .get(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("list users");
    assert_eq!(get_res.status(), StatusCode::OK);
    let users: serde_json::Value = get_res.json().await.expect("parse users json");
    let users_arr = users.as_array().expect("users array");
    let legacy_entry = users_arr
        .iter()
        .find(|u| u["username"] == "legacy_user")
        .expect("legacy user present");
    assert_eq!(legacy_entry["scopes"], "unset");
}

/// Prove that creating a user with the unset sentinel stores unset scopes.
#[tokio::test]
async fn tr_009_post_create_unset_sentinel() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let payload = json!({
        "username": "created_unset",
        "password": "User-pass-1!",
        "role": "administrator",
        "description": "created with unset",
        "scopes": "unset"
    });
    let create_res = client
        .post(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .json(&payload)
        .send()
        .await
        .expect("create user with unset sentinel");
    assert_eq!(create_res.status(), StatusCode::OK);
    let create_body: serde_json::Value = create_res.json().await.expect("parse create response");
    assert_eq!(create_body["username"], "created_unset");
    assert_eq!(create_body["scopes"], "unset");

    let get_res = client
        .get(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("list users");
    assert_eq!(get_res.status(), StatusCode::OK);
    let users: serde_json::Value = get_res.json().await.expect("parse users json");
    let entry = users
        .as_array()
        .expect("users array")
        .iter()
        .find(|u| u["username"] == "created_unset")
        .expect("created user found");
    assert_eq!(entry["scopes"], "unset");
}

/// Prove that omitting the scopes field on user creation persists role-default scopes.
#[tokio::test]
async fn tr_009_post_users_materialises_default_scopes() {
    let (base_url, state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let payload = json!({
        "username": "mat_admin",
        "password": "User-pass-1!",
        "role": "administrator",
        "description": "materialise admin test"
    });
    let create_res = client
        .post(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .json(&payload)
        .send()
        .await
        .expect("create admin without explicit scopes");
    assert_eq!(create_res.status(), StatusCode::OK);

    let stored = state
        .admin_users
        .get("mat_admin")
        .expect("stored user present");
    let expected_scopes: Vec<String> = full_admin_scopes().into_iter().map(String::from).collect();
    assert_eq!(stored.scopes.expect("scopes persisted"), expected_scopes);
}

/// Prove that creating a user with explicit scopes returns the scopes in the response.
#[tokio::test]
async fn tr_009_post_users_response_includes_scopes() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let assigned_scopes = vec!["user_management", "api_key_management"];
    let payload = json!({
        "username": "scoped_user",
        "password": "User-pass-1!",
        "role": "administrator",
        "description": "explicit scopes user",
        "scopes": assigned_scopes
    });
    let create_res = client
        .post(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .json(&payload)
        .send()
        .await
        .expect("create user with explicit scopes");
    assert_eq!(create_res.status(), StatusCode::OK);
    let create_body: serde_json::Value = create_res.json().await.expect("parse response json");
    assert_eq!(create_body["username"], "scoped_user");
    assert_eq!(create_body["role"], "administrator");
    assert_eq!(
        create_body["scopes"],
        json!(["user_management", "api_key_management"])
    );
}

/// Prove that creating an administrator without scopes returns materialised defaults in response.
#[tokio::test]
async fn tr_009_post_users_response_role_default_scopes_when_not_set() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let payload = json!({
        "username": "admin_no_scopes",
        "password": "User-pass-1!",
        "role": "administrator",
        "description": "no scopes test"
    });
    let create_res = client
        .post(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .json(&payload)
        .send()
        .await
        .expect("create admin without scopes");
    assert_eq!(create_res.status(), StatusCode::OK);
    let create_body: serde_json::Value = create_res.json().await.expect("parse response json");
    assert_eq!(create_body["username"], "admin_no_scopes");
    assert_eq!(create_body["role"], "administrator");
    let expected_scopes: Vec<String> = full_admin_scopes().into_iter().map(String::from).collect();
    assert_eq!(create_body["scopes"], json!(expected_scopes));
}

/// Prove that creating a viewer without scopes returns only common scopes without login-only scopes.
#[tokio::test]
async fn tr_009_post_users_response_viewer_default_scopes() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let payload = json!({
        "username": "viewer_no_scopes",
        "password": "User-pass-1!",
        "role": "viewer",
        "description": "viewer default scopes test"
    });
    let create_res = client
        .post(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .json(&payload)
        .send()
        .await
        .expect("create viewer without scopes");
    assert_eq!(create_res.status(), StatusCode::OK);
    let create_body: serde_json::Value = create_res.json().await.expect("parse response json");
    assert_eq!(create_body["username"], "viewer_no_scopes");
    assert_eq!(create_body["role"], "viewer");
    let expected_scopes: Vec<String> = viewer_scopes().into_iter().map(String::from).collect();
    assert_eq!(create_body["scopes"], json!(expected_scopes));

    // Viewer defaults must not contain login-only scopes.
    let returned = create_body["scopes"].as_array().expect("scopes array");
    assert!(!returned.contains(&json!("user_management")));
    assert!(!returned.contains(&json!("mfa_management")));
    assert!(!returned.contains(&json!("sso_management")));
    assert!(!returned.contains(&json!("api_key_management")));
}

/// Prove that user modification response reflects updated scope assignments.
#[tokio::test]
async fn tr_009_put_users_response_includes_updated_scopes() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let create_payload = json!({
        "username": "u_update",
        "password": "User-pass-1!",
        "role": "administrator",
        "description": "initial user"
    });
    let create_res = client
        .post(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .json(&create_payload)
        .send()
        .await
        .expect("create user");
    assert_eq!(create_res.status(), StatusCode::OK);

    let put_payload = json!({
        "role": "administrator",
        "description": "updated description",
        "scopes": ["mfa_management"]
    });
    let put_res = client
        .put(format!("{base_url}/api/v5/users/u_update"))
        .bearer_auth(&admin_token)
        .json(&put_payload)
        .send()
        .await
        .expect("update user scopes");
    assert_eq!(put_res.status(), StatusCode::OK);
    let body: serde_json::Value = put_res.json().await.expect("parse put response");
    assert_eq!(body["description"], "updated description");
    assert_eq!(body["scopes"], json!(["mfa_management"]));
}

/// Prove that updating a regular user with unset clears previously-explicit scopes.
#[tokio::test]
async fn tr_009_regular_user_put_unset_clears_explicit() {
    let (base_url, state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let create_payload = json!({
        "username": "reguser2",
        "password": "User-pass-1!",
        "role": "administrator",
        "description": "explicit user",
        "scopes": ["mfa_management"]
    });
    let create_res = client
        .post(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .json(&create_payload)
        .send()
        .await
        .expect("create user with explicit scopes");
    assert_eq!(create_res.status(), StatusCode::OK);

    let put_payload = json!({
        "role": "administrator",
        "description": "cleared scopes",
        "scopes": "unset"
    });
    let put_res = client
        .put(format!("{base_url}/api/v5/users/reguser2"))
        .bearer_auth(&admin_token)
        .json(&put_payload)
        .send()
        .await
        .expect("update user with unset sentinel");
    assert_eq!(put_res.status(), StatusCode::OK);
    let body: serde_json::Value = put_res.json().await.expect("parse put response");
    assert_eq!(body["scopes"], "unset");

    let stored = state
        .admin_users
        .get("reguser2")
        .expect("stored user present");
    assert_eq!(stored.scopes, None);
}

/// Prove that round-tripping the expanded scope catalog on a regular user preserves unset state.
#[tokio::test]
async fn tr_009_regular_user_round_trip_full_list_stays_unset() {
    let (base_url, state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let create_payload = json!({
        "username": "reguser_unset",
        "password": "User-pass-1!",
        "role": "administrator",
        "description": "unset user",
        "scopes": "unset"
    });
    let create_res = client
        .post(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .json(&create_payload)
        .send()
        .await
        .expect("create user with unset sentinel");
    assert_eq!(create_res.status(), StatusCode::OK);

    let put_payload = json!({
        "role": "administrator",
        "description": "round trip full list",
        "scopes": full_admin_scopes()
    });
    let put_res = client
        .put(format!("{base_url}/api/v5/users/reguser_unset"))
        .bearer_auth(&admin_token)
        .json(&put_payload)
        .send()
        .await
        .expect("update user with full list");
    assert_eq!(put_res.status(), StatusCode::OK);
    let body: serde_json::Value = put_res.json().await.expect("parse put response");
    assert_eq!(body["scopes"], "unset");

    let stored = state
        .admin_users
        .get("reguser_unset")
        .expect("stored user present");
    assert_eq!(stored.scopes, None);
}

/// Prove that role demotion succeeds when persisted scopes are compatible with the new role.
#[tokio::test]
async fn tr_009_role_demotion_with_compatible_persisted_scopes_succeeds() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let create_payload = json!({
        "username": "demote_ok",
        "password": "User-pass-1!",
        "role": "administrator",
        "description": "compatible scopes target",
        "scopes": ["connections"]
    });
    let create_res = client
        .post(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .json(&create_payload)
        .send()
        .await
        .expect("create user with compatible scope");
    assert_eq!(create_res.status(), StatusCode::OK);

    let put_payload = json!({
        "role": "viewer",
        "description": "demoted user"
    });
    let put_res = client
        .put(format!("{base_url}/api/v5/users/demote_ok"))
        .bearer_auth(&admin_token)
        .json(&put_payload)
        .send()
        .await
        .expect("demote user to viewer");
    assert_eq!(put_res.status(), StatusCode::OK);
    let body: serde_json::Value = put_res.json().await.expect("parse put response");
    assert_eq!(body["role"], "viewer");
    assert_eq!(body["scopes"], json!(["connections"]));
}

/// Prove that role demotion fails when persisted scopes contain administrator-only scopes.
#[tokio::test]
async fn tr_009_role_demotion_with_persisted_admin_scopes_is_rejected() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let create_payload = json!({
        "username": "demote_bad",
        "password": "User-pass-1!",
        "role": "administrator",
        "description": "incompatible scopes target",
        "scopes": ["user_management"]
    });
    let create_res = client
        .post(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .json(&create_payload)
        .send()
        .await
        .expect("create user with admin-only scope");
    assert_eq!(create_res.status(), StatusCode::OK);

    let put_payload = json!({
        "role": "viewer",
        "description": "demote attempt"
    });
    let put_res = client
        .put(format!("{base_url}/api/v5/users/demote_bad"))
        .bearer_auth(&admin_token)
        .json(&put_payload)
        .send()
        .await
        .expect("attempt demotion with admin-only scope");
    assert_eq!(put_res.status(), StatusCode::BAD_REQUEST);

    // Verify stored state is unchanged.
    let get_res = client
        .get(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("list users");
    assert_eq!(get_res.status(), StatusCode::OK);
    let users: serde_json::Value = get_res.json().await.expect("parse users json");
    let user_entry = users
        .as_array()
        .expect("users array")
        .iter()
        .find(|u| u["username"] == "demote_bad")
        .expect("demote target present");
    assert_eq!(user_entry["role"], "administrator");
    assert_eq!(user_entry["scopes"], json!(["user_management"]));
}

/// Prove that user creation with an unknown scope name returns HTTP 400 Bad Request.
#[tokio::test]
async fn tr_009_unknown_scope_returns_400() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let payload = json!({
        "username": "bad_scope_user",
        "password": "User-pass-1!",
        "role": "administrator",
        "description": "invalid scope",
        "scopes": ["bogus_scope_name"]
    });
    let res = client
        .post(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .json(&payload)
        .send()
        .await
        .expect("create user with unknown scope");
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

/// Prove that omitted scopes and explicit empty scopes succeed on user creation.
#[tokio::test]
async fn tr_009_user_non_explicit_lists_pass() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    // Omitted scopes field succeeds.
    let omitted_payload = json!({
        "username": "u_omitted",
        "password": "User-pass-1!",
        "role": "administrator",
        "description": "omitted scopes"
    });
    let res_omitted = client
        .post(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .json(&omitted_payload)
        .send()
        .await
        .expect("create user with omitted scopes");
    assert_eq!(res_omitted.status(), StatusCode::OK);

    // Explicit empty list succeeds.
    let empty_payload = json!({
        "username": "u_empty",
        "password": "User-pass-1!",
        "role": "administrator",
        "description": "empty scopes",
        "scopes": []
    });
    let res_empty = client
        .post(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .json(&empty_payload)
        .send()
        .await
        .expect("create user with empty scopes");
    assert_eq!(res_empty.status(), StatusCode::OK);
    let body_empty: serde_json::Value = res_empty.json().await.expect("parse empty response");
    assert_eq!(body_empty["scopes"], json!([]));
}
