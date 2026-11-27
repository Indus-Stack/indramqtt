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

/// Prove that users without multi-factor authentication log in normally.
#[tokio::test]
async fn tr_004_login_no_mfa_setting_no_mfa_token() {
    let (base_url, _state) = start_server().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let user_payload = json!({
        "username": "tr004_viewer",
        "password": "Viewer-password-1",
        "role": "viewer"
    });
    let create_res = client
        .post(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .json(&user_payload)
        .send()
        .await
        .expect("create user");
    assert_eq!(create_res.status(), StatusCode::OK);

    let res_no_token = client
        .post(format!("{base_url}/api/v5/login"))
        .json(&json!({
            "username": "tr004_viewer",
            "password": "Viewer-password-1"
        }))
        .send()
        .await
        .expect("send login without mfa token");
    assert_eq!(res_no_token.status(), StatusCode::OK);
    let body_no_token: serde_json::Value = res_no_token.json().await.expect("parse login json");
    assert!(body_no_token["token"]
        .as_str()
        .is_some_and(|t| !t.is_empty()));

    let res_with_token = client
        .post(format!("{base_url}/api/v5/login"))
        .json(&json!({
            "username": "tr004_viewer",
            "password": "Viewer-password-1",
            "mfa_token": "123456"
        }))
        .send()
        .await
        .expect("send login with extra mfa token");
    assert_eq!(res_with_token.status(), StatusCode::OK);
    let body_with_token: serde_json::Value = res_with_token.json().await.expect("parse login json");
    assert!(body_with_token["token"]
        .as_str()
        .is_some_and(|t| !t.is_empty()));

    let res_bad_pwd = client
        .post(format!("{base_url}/api/v5/login"))
        .json(&json!({
            "username": "tr004_viewer",
            "password": "wrong-password",
            "mfa_token": "123456"
        }))
        .send()
        .await
        .expect("send login with wrong password");
    assert_eq!(res_bad_pwd.status(), StatusCode::UNAUTHORIZED);
    let body_bad_pwd: serde_json::Value = res_bad_pwd.json().await.expect("parse login error json");
    assert_eq!(body_bad_pwd["code"], "BAD_USERNAME_OR_PWD");
}
