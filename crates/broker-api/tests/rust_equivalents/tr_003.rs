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

/// Prove that users with the viewer role cannot access administrator routes.
#[tokio::test]
async fn tr_003_viewer_denied_on_admin_routes() {
    let (base_url, _state) = start_server().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let viewer_payload = json!({
        "username": "tr003_viewer",
        "password": "Viewer-password-1",
        "role": "viewer"
    });
    let create_viewer = client
        .post(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .json(&viewer_payload)
        .send()
        .await
        .expect("create viewer user");
    assert_eq!(create_viewer.status(), StatusCode::OK);

    let other_payload = json!({
        "username": "tr003_other",
        "password": "Other-password-1",
        "role": "viewer"
    });
    let create_other = client
        .post(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .json(&other_payload)
        .send()
        .await
        .expect("create other user");
    assert_eq!(create_other.status(), StatusCode::OK);

    let login_res = client
        .post(format!("{base_url}/api/v5/login"))
        .json(&json!({
            "username": "tr003_viewer",
            "password": "Viewer-password-1"
        }))
        .send()
        .await
        .expect("viewer login");
    assert_eq!(login_res.status(), StatusCode::OK);
    let login_body: serde_json::Value = login_res.json().await.expect("parse login json");
    let viewer_token = login_body["token"].as_str().expect("viewer token");

    for target in ["tr003_other", "tr003_viewer"] {
        let post_mfa = client
            .post(format!("{base_url}/api/v5/users/{target}/mfa"))
            .bearer_auth(viewer_token)
            .json(&json!({"mechanism": "totp"}))
            .send()
            .await
            .expect("post mfa");
        assert_eq!(post_mfa.status(), StatusCode::FORBIDDEN);
        let err: serde_json::Value = post_mfa.json().await.expect("parse post mfa err");
        assert_eq!(err["code"], "FORBIDDEN");

        let delete_mfa = client
            .delete(format!("{base_url}/api/v5/users/{target}/mfa"))
            .bearer_auth(viewer_token)
            .send()
            .await
            .expect("delete mfa");
        assert_eq!(delete_mfa.status(), StatusCode::FORBIDDEN);
        let err: serde_json::Value = delete_mfa.json().await.expect("parse delete mfa err");
        assert_eq!(err["code"], "FORBIDDEN");

        let put_user = client
            .put(format!("{base_url}/api/v5/users/{target}"))
            .bearer_auth(viewer_token)
            .json(&json!({"description": "updated description"}))
            .send()
            .await
            .expect("put user");
        assert_eq!(put_user.status(), StatusCode::FORBIDDEN);
        let err: serde_json::Value = put_user.json().await.expect("parse put user err");
        assert_eq!(err["code"], "FORBIDDEN");

        let del_user = client
            .delete(format!("{base_url}/api/v5/users/{target}"))
            .bearer_auth(viewer_token)
            .send()
            .await
            .expect("delete user");
        assert_eq!(del_user.status(), StatusCode::FORBIDDEN);
        let err: serde_json::Value = del_user.json().await.expect("parse delete user err");
        assert_eq!(err["code"], "FORBIDDEN");
    }
}

/// Prove that a successful login resets the consecutive failure count.
#[tokio::test]
async fn tr_003_cancel_lock_with_successful_login() {
    let (base_url, _state) = start_server().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");

    for _ in 0..4 {
        let res = client
            .post(format!("{base_url}/api/v5/login"))
            .json(&json!({"username": "admin", "password": "wrong-password"}))
            .send()
            .await
            .expect("send failed login");
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        let body: serde_json::Value = res.json().await.expect("parse json");
        assert_eq!(body["code"], "BAD_USERNAME_OR_PWD");
    }

    let success_res = client
        .post(format!("{base_url}/api/v5/login"))
        .json(&json!({"username": "admin", "password": "public"}))
        .send()
        .await
        .expect("send successful login");
    assert_eq!(success_res.status(), StatusCode::OK);
    let success_body: serde_json::Value = success_res.json().await.expect("parse json");
    assert!(success_body["token"]
        .as_str()
        .is_some_and(|t| !t.is_empty()));

    for _ in 0..4 {
        let res = client
            .post(format!("{base_url}/api/v5/login"))
            .json(&json!({"username": "admin", "password": "wrong-password"}))
            .send()
            .await
            .expect("send subsequent failed login");
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        let body: serde_json::Value = res.json().await.expect("parse json");
        assert_eq!(body["code"], "BAD_USERNAME_OR_PWD");
    }
}

/// Prove that login attempts for unknown usernames create no lockout records.
#[tokio::test]
async fn tr_003_no_record_for_invalid_username() {
    let (base_url, _state) = start_server().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");

    for _ in 0..7 {
        let res = client
            .post(format!("{base_url}/api/v5/login"))
            .json(&json!({"username": "no-such-user", "password": "wrong-password"}))
            .send()
            .await
            .expect("send invalid user login");
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        let body: serde_json::Value = res.json().await.expect("parse json");
        assert_eq!(body["code"], "BAD_USERNAME_OR_PWD");
    }
}
