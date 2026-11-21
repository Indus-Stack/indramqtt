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

/// Prove create, duplicate refusal, and delete of one ban.
///
/// Create one client id ban through the API.
/// Send the same create again and check refusal.
/// Delete the ban and check the list is empty.
#[tokio::test]
async fn tr_020_banned_add_delete() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let create_res = client
        .post(format!("{base_url}/api/v5/banned"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "as": "clientid",
            "who": "tr-020-banned-id",
            "by": "test",
            "reason": "test",
            "until": "infinity",
        }))
        .send()
        .await
        .expect("create ban");
    assert_eq!(create_res.status(), StatusCode::OK);
    let created: serde_json::Value = create_res.json().await.expect("create json");
    assert_eq!(created["data"][0]["who"], json!("tr-020-banned-id"));
    assert_eq!(created["data"][0]["as"], json!("clientid"));
    assert_eq!(created["data"][0]["until"], json!("infinity"));

    let list_res = client
        .get(format!("{base_url}/api/v5/banned"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("list bans");
    assert_eq!(list_res.status(), StatusCode::OK);
    let list_body: serde_json::Value = list_res.json().await.expect("list json");
    assert_eq!(list_body["data"].as_array().expect("data array").len(), 1);
    assert_eq!(list_body["meta"]["count"], json!(1));
    assert_eq!(list_body["data"][0]["who"], json!("tr-020-banned-id"));
    assert_eq!(list_body["data"][0]["until"], json!("infinity"));

    let dup_res = client
        .post(format!("{base_url}/api/v5/banned"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "as": "clientid",
            "who": "tr-020-banned-id",
            "by": "test",
            "reason": "test",
            "until": "infinity",
        }))
        .send()
        .await
        .expect("duplicate ban");
    assert_eq!(dup_res.status(), StatusCode::BAD_REQUEST);
    let dup_body: serde_json::Value = dup_res.json().await.expect("duplicate json");
    assert_eq!(dup_body["code"], json!("ALREADY_EXISTS"));

    let del_res = client
        .delete(format!(
            "{base_url}/api/v5/banned/clientid/tr-020-banned-id"
        ))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("delete ban");
    assert_eq!(del_res.status(), StatusCode::NO_CONTENT);

    let empty_res = client
        .get(format!("{base_url}/api/v5/banned"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("list bans after delete");
    assert_eq!(empty_res.status(), StatusCode::OK);
    let empty_body: serde_json::Value = empty_res.json().await.expect("empty json");
    assert_eq!(empty_body["data"].as_array().expect("data array").len(), 0);
    assert_eq!(empty_body["meta"]["count"], json!(0));
}

/// Prove an expired ban stays absent while a live ban remains.
///
/// Create one live ban and one expired ban through the API.
/// Check the list holds only the live ban.
/// Delete the live ban and check the expired ban reads as missing.
#[tokio::test]
async fn tr_020_banned_unused() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let live_res = client
        .post(format!("{base_url}/api/v5/banned"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "as": "clientid",
            "who": "tr-020-live-id",
            "by": "test",
            "reason": "test",
            "until": "infinity",
        }))
        .send()
        .await
        .expect("create live ban");
    assert_eq!(live_res.status(), StatusCode::OK);

    let expired_res = client
        .post(format!("{base_url}/api/v5/banned"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "as": "clientid",
            "who": "tr-020-expired-id",
            "by": "test",
            "reason": "test",
            "until": "1970-01-01T00:00:00Z",
        }))
        .send()
        .await
        .expect("create expired ban");
    assert_eq!(expired_res.status(), StatusCode::OK);

    let list_res = client
        .get(format!("{base_url}/api/v5/banned"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("list bans");
    assert_eq!(list_res.status(), StatusCode::OK);
    let list_body: serde_json::Value = list_res.json().await.expect("list json");
    assert_eq!(list_body["data"].as_array().expect("data array").len(), 1);
    assert_eq!(list_body["meta"]["count"], json!(1));
    assert_eq!(list_body["data"][0]["who"], json!("tr-020-live-id"));
    assert_eq!(list_body["data"][0]["until"], json!("infinity"));

    let del_live = client
        .delete(format!("{base_url}/api/v5/banned/clientid/tr-020-live-id"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("delete live ban");
    assert_eq!(del_live.status(), StatusCode::NO_CONTENT);

    let del_expired = client
        .delete(format!(
            "{base_url}/api/v5/banned/clientid/tr-020-expired-id"
        ))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("delete expired ban");
    assert_eq!(del_expired.status(), StatusCode::NOT_FOUND);
    let del_body: serde_json::Value = del_expired.json().await.expect("missing json");
    assert_eq!(del_body["code"], json!("NOT_FOUND"));

    let empty_res = client
        .get(format!("{base_url}/api/v5/banned"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("list bans after deletes");
    assert_eq!(empty_res.status(), StatusCode::OK);
    let empty_body: serde_json::Value = empty_res.json().await.expect("empty json");
    assert_eq!(empty_body["data"].as_array().expect("data array").len(), 0);
    assert_eq!(empty_body["meta"]["count"], json!(0));
}
