//! Rust equivalents for monitor history and user checks, part 24.
//!
//! Holds the tests that prove our code keeps the empty read, the clear
//! and the role refusal through the operator path.

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

/// Prove that an empty history reads as an empty list.
///
/// Read the history with no window and with a wide window.
/// Both answers are 200 with an empty array. A stored point
/// makes the length assertion fail.
#[tokio::test]
async fn tr_024_empty_history_reads_as_empty_list() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let plain_res = client
        .get(format!("{base_url}/api/v5/monitor"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("read empty history");
    assert_eq!(plain_res.status(), StatusCode::OK);
    let plain_body: serde_json::Value = plain_res.json().await.expect("history json");
    assert_eq!(plain_body, json!([]));

    let window_res = client
        .get(format!("{base_url}/api/v5/monitor?latest=20000"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("read empty history with window");
    assert_eq!(window_res.status(), StatusCode::OK);
    let window_body: serde_json::Value = window_res.json().await.expect("window json");
    assert_eq!(window_body, json!([]));
}

/// Prove that a clear drops every stored history point.
///
/// Record two current points and read two back in order.
/// Delete the history, then read with a plain GET.
/// A clear that keeps a point makes the final assertion fail.
#[tokio::test]
async fn tr_024_clear_drops_every_stored_point() {
    let (base_url, state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .expect("now after epoch");
    let first_stamp = now_secs.saturating_sub(1);
    let second_stamp = now_secs;
    state
        .monitor
        .record(broker_api::v5::monitor::MonitorSample::from_live(
            first_stamp,
            broker_api::v5::monitor::LiveCounters {
                connections: 3,
                ..Default::default()
            },
        ));
    state
        .monitor
        .record(broker_api::v5::monitor::MonitorSample::from_live(
            second_stamp,
            broker_api::v5::monitor::LiveCounters {
                connections: 5,
                ..Default::default()
            },
        ));

    let filled_res = client
        .get(format!("{base_url}/api/v5/monitor"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("read filled history");
    assert_eq!(filled_res.status(), StatusCode::OK);
    let filled_body: serde_json::Value = filled_res.json().await.expect("filled json");
    assert_eq!(filled_body.as_array().expect("array").len(), 2);
    assert_eq!(filled_body[0]["time_stamp"], json!(first_stamp));
    assert_eq!(filled_body[1]["time_stamp"], json!(second_stamp));

    let clear_res = client
        .delete(format!("{base_url}/api/v5/monitor"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("clear history");
    assert_eq!(clear_res.status(), StatusCode::NO_CONTENT);

    let empty_res = client
        .get(format!("{base_url}/api/v5/monitor"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("read cleared history");
    assert_eq!(empty_res.status(), StatusCode::OK);
    let empty_body: serde_json::Value = empty_res.json().await.expect("empty json");
    assert_eq!(empty_body, json!([]));
}

/// Prove that a user create with an unknown role fails.
///
/// Send a role outside the catalogue. The route answers 400 with
/// code BAD_REQUEST and creates nothing. Acceptance of the role
/// makes the status assertion fail.
#[tokio::test]
async fn tr_024_unknown_role_fails_with_bad_request() {
    let (base_url, state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let bad_res = client
        .post(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "username": "tr024-bad-role",
            "password": "Bad-role-pass-1",
            "role": "bad_role",
        }))
        .send()
        .await
        .expect("create user with unknown role");
    assert_eq!(bad_res.status(), StatusCode::BAD_REQUEST);
    let bad_body: serde_json::Value = bad_res.json().await.expect("error json");
    assert_eq!(bad_body["code"], json!("BAD_REQUEST"));

    assert!(
        state.admin_users.get("tr024-bad-role").is_none(),
        "unknown role creates no user"
    );
}
