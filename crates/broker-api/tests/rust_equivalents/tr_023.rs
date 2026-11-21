//! Rust equivalents for dashboard user checks, part 23.
//!
//! Holds the tests that prove our code keeps the user lookup and
//! cookie behaviours of the dashboard suites through the operator path.

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

/// Prove that a session cookie does not authenticate a users list read.
///
/// Send the valid token only as a cookie header, with no bearer header.
/// The middleware reads no cookie header for any name, so the route
/// answers 401. The same token as a bearer header answers 200.
/// Accepting cookie credentials makes the 401 assertion fail.
#[tokio::test]
async fn tr_023_cookie_does_not_authenticate_users_list() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let cookie_res = client
        .get(format!("{base_url}/api/v5/users"))
        .header("cookie", format!("session={admin_token}"))
        .send()
        .await
        .expect("cookie-only users read");
    assert_eq!(cookie_res.status(), StatusCode::UNAUTHORIZED);
    let cookie_body: serde_json::Value = cookie_res.json().await.expect("error json");
    assert_eq!(cookie_body["code"], json!("UNAUTHORIZED"));

    let bearer_res = client
        .get(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("bearer users read");
    assert_eq!(bearer_res.status(), StatusCode::OK);
}

/// Prove that a user lookup returns the record and misses empty.
///
/// Create one viewer, read it back, and read an unknown name.
/// The read carries the username and the description. The unknown
/// name reads as empty. Returning empty for the known user makes
/// the username assertion fail.
#[test]
fn tr_023_lookup_user_reads_record_and_misses_empty() {
    use broker_api::admin_users::{AdminRole, AdminUsers};

    let users = AdminUsers::with_default_admin();
    users
        .create(
            "tr023-lookup",
            "Lookup-pass-1",
            AdminRole::Viewer,
            "lookup description",
        )
        .expect("create lookup user");

    let view = users.get("tr023-lookup").expect("known user reads back");
    assert_eq!(view.username, "tr023-lookup");
    assert_eq!(view.role, AdminRole::Viewer);
    assert_eq!(view.description, "lookup description");

    assert!(
        users.get("tr023-no-such-user").is_none(),
        "unknown user reads as empty"
    );
}
