use std::sync::Arc;
use std::time::Duration;

use broker_api::v5::audit::{AuditFilter, AuditLog, AuditRecordInput, AUDIT_FROM};
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

/// Prove that an odd audit input never crashes the recorder.
///
/// Record one entry with an empty origin and an oversized actor.
/// Verify the ring holds one row with the default origin.
#[test]
fn tr_018_audit_record_keeps_odd_input_without_crash() {
    let log = AuditLog::new();
    log.record(AuditRecordInput {
        created_at_us: 1,
        node: String::new(),
        from: String::new(),
        source: "s".repeat(10_000),
        source_ip: String::new(),
        operation_id: "/api/v5/tr018-probe".to_string(),
        operation_type: "probe".to_string(),
        operation_result: "success".to_string(),
        http_method: "post".to_string(),
        http_status_code: 200,
        duration_ms: 0,
        tenant: String::new(),
    });
    assert_eq!(log.len(), 1);
    let rows = log.query(&AuditFilter {
        operation_id: Some("/api/v5/tr018-probe".to_string()),
        ..Default::default()
    });
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].from, AUDIT_FROM);
    assert!(rows[0].source.len() < 10_000);
}

/// Prove that user routes refuse an unknown authenticator.
///
/// Read and create users under an id the chain never held.
/// Verify both answer 404 with NOT_FOUND.
#[tokio::test]
async fn tr_018_authenticator_users_missing_chain_answers_not_found() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let get_res = client
        .get(format!(
            "{base_url}/api/v5/authentication/password_based:nowhere/users"
        ))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("list users of unknown authenticator");
    assert_eq!(get_res.status(), StatusCode::NOT_FOUND);
    let get_body: serde_json::Value = get_res.json().await.expect("error json");
    assert_eq!(get_body["code"], "NOT_FOUND");

    let post_res = client
        .post(format!(
            "{base_url}/api/v5/authentication/password_based:nowhere/users"
        ))
        .bearer_auth(&admin_token)
        .json(&json!({"user_id": "tr018-ghost", "password": "Ghost-pass-1!"}))
        .send()
        .await
        .expect("create user under unknown authenticator");
    assert_eq!(post_res.status(), StatusCode::NOT_FOUND);
    let post_body: serde_json::Value = post_res.json().await.expect("error json");
    assert_eq!(post_body["code"], "NOT_FOUND");
}

/// Prove that a duplicate id rejects the whole order replace.
///
/// Create two authenticators through the management API.
/// Send an order holding one duplicate id.
/// Verify the API answers 400 and the stored order stays.
#[tokio::test]
async fn tr_018_authenticator_order_duplicate_rejects_without_apply() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    for backend in ["built_in_database", "mysql"] {
        let create_res = client
            .post(format!("{base_url}/api/v5/authentication"))
            .bearer_auth(&admin_token)
            .json(&json!({"mechanism": "password_based", "backend": backend}))
            .send()
            .await
            .expect("create authenticator");
        assert_eq!(create_res.status(), StatusCode::OK);
    }

    let put_res = client
        .put(format!("{base_url}/api/v5/authentication/order"))
        .bearer_auth(&admin_token)
        .json(&json!([
            {"id": "password_based:mysql"},
            {"id": "password_based:built_in_database"},
            {"id": "password_based:mysql"},
        ]))
        .send()
        .await
        .expect("reorder with duplicate id");
    assert_eq!(put_res.status(), StatusCode::BAD_REQUEST);
    let put_body: serde_json::Value = put_res.json().await.expect("error json");
    assert_eq!(put_body["code"], "BAD_REQUEST");

    let list_res = client
        .get(format!("{base_url}/api/v5/authentication"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("list chain after rejected reorder");
    assert_eq!(list_res.status(), StatusCode::OK);
    let list_body: serde_json::Value = list_res.json().await.expect("list json");
    let ids: Vec<&str> = list_body
        .as_array()
        .expect("chain is a list")
        .iter()
        .map(|entry| entry["id"].as_str().expect("entry id"))
        .collect();
    assert_eq!(
        ids,
        vec!["password_based:built_in_database", "password_based:mysql"]
    );
}
