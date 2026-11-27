use std::sync::Arc;
use std::time::Duration;

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

/// Prove that administrator accounts can hold multiple privilege login scopes.
#[tokio::test]
async fn tr_008_admin_can_hold_all_4_new_scopes() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let priv_login_scopes = vec![
        "user_management",
        "mfa_management",
        "sso_management",
        "api_key_management",
    ];
    let payload = json!({
        "username": "admin_priv_scopes",
        "password": "Admin-pass-1!",
        "role": "administrator",
        "description": "privileged scopes admin",
        "scopes": priv_login_scopes
    });

    let create_res = client
        .post(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .json(&payload)
        .send()
        .await
        .expect("create admin with privilege login scopes");
    assert_eq!(create_res.status(), StatusCode::OK);
    let create_body: serde_json::Value = create_res.json().await.expect("parse response json");
    assert_eq!(create_body["username"], "admin_priv_scopes");
    assert_eq!(create_body["role"], "administrator");
    assert_eq!(
        create_body["scopes"],
        json!([
            "user_management",
            "mfa_management",
            "sso_management",
            "api_key_management"
        ])
    );

    let mfa_mixed_payload = json!({
        "username": "admin_mfa_mixed",
        "password": "Admin-pass-2!",
        "role": "administrator",
        "description": "mfa and generic scope admin",
        "scopes": ["mfa_management", "connections"]
    });

    let create_mfa_res = client
        .post(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .json(&mfa_mixed_payload)
        .send()
        .await
        .expect("create admin with mfa management and connections scope");
    assert_eq!(create_mfa_res.status(), StatusCode::OK);
    let create_mfa_body: serde_json::Value = create_mfa_res
        .json()
        .await
        .expect("parse mfa response json");
    assert_eq!(create_mfa_body["username"], "admin_mfa_mixed");
    assert_eq!(
        create_mfa_body["scopes"],
        json!(["mfa_management", "connections"])
    );
}

/// Prove that the bootstrapped default administrator account has unset scopes.
#[tokio::test]
async fn tr_008_default_admin_bootstrap_unset_scopes() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let res = client
        .get(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("list users");
    assert_eq!(res.status(), StatusCode::OK);
    let users: serde_json::Value = res.json().await.expect("parse users json");
    let users_arr = users.as_array().expect("users array");
    let admin_user = users_arr
        .iter()
        .find(|u| u["username"] == "admin")
        .expect("default admin present");
    assert_eq!(admin_user["scopes"], "unset");
    assert_eq!(admin_user["role"], "administrator");
}

/// Prove that updating default administrator description preserves unset scopes.
#[tokio::test]
async fn tr_008_default_admin_description_can_be_updated() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let update_payload = json!({
        "role": "administrator",
        "description": "updated desc"
    });
    let put_res = client
        .put(format!("{base_url}/api/v5/users/admin"))
        .bearer_auth(&admin_token)
        .json(&update_payload)
        .send()
        .await
        .expect("update admin description");
    assert_eq!(put_res.status(), StatusCode::OK);
    let body: serde_json::Value = put_res.json().await.expect("parse put response");
    assert_eq!(body["description"], "updated desc");
    assert_eq!(body["scopes"], "unset");
}

/// Prove that editing the default admin with the full catalog normalizes scopes to unset.
#[tokio::test]
async fn tr_008_default_admin_note_only_edit_full_list() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let full_list = vec![
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
    ];

    let put_payload = json!({
        "role": "administrator",
        "description": "edited note",
        "scopes": full_list
    });
    let put_res = client
        .put(format!("{base_url}/api/v5/users/admin"))
        .bearer_auth(&admin_token)
        .json(&put_payload)
        .send()
        .await
        .expect("update admin with full scope list");
    assert_eq!(put_res.status(), StatusCode::OK);
    let body: serde_json::Value = put_res.json().await.expect("parse put response");
    assert_eq!(body["description"], "edited note");
    assert_eq!(body["scopes"], "unset");
}
