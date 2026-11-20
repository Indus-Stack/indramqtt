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

/// Prove publisher key role authorization on publish and client bans endpoints.
#[tokio::test]
async fn tr_011_authorize_publisher() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let create_res = client
        .post(format!("{base_url}/api/v5/api_key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "name": "pub-auth-key",
            "role": "publisher"
        }))
        .send()
        .await
        .expect("create publisher key");
    assert_eq!(create_res.status(), StatusCode::OK);
    let key_data: serde_json::Value = create_res.json().await.expect("key json");
    let key_id = key_data["api_key"].as_str().expect("key_id");
    let secret = key_data["api_secret"].as_str().expect("secret");

    let ban_get = client
        .get(format!("{base_url}/api/v5/banned"))
        .basic_auth(key_id, Some(secret))
        .send()
        .await
        .expect("get banned");
    assert_eq!(ban_get.status(), StatusCode::FORBIDDEN);

    let ban_del = client
        .delete(format!("{base_url}/api/v5/banned/clientid/any-client"))
        .basic_auth(key_id, Some(secret))
        .send()
        .await
        .expect("delete banned");
    assert_eq!(ban_del.status(), StatusCode::FORBIDDEN);

    let pub_res = client
        .post(format!("{base_url}/api/v5/publish"))
        .basic_auth(key_id, Some(secret))
        .json(&json!({
            "topic": "t/tr011_publisher",
            "payload": "hello"
        }))
        .send()
        .await
        .expect("publish message");
    assert_eq!(pub_res.status(), StatusCode::ACCEPTED);
}

/// Prove viewer key role authorization on client ban reading and deletion.
#[tokio::test]
async fn tr_011_authorize_viewer() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let create_res = client
        .post(format!("{base_url}/api/v5/api_key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "name": "view-auth-key",
            "role": "viewer"
        }))
        .send()
        .await
        .expect("create viewer key");
    assert_eq!(create_res.status(), StatusCode::OK);
    let key_data: serde_json::Value = create_res.json().await.expect("key json");
    let key_id = key_data["api_key"].as_str().expect("key_id");
    let secret = key_data["api_secret"].as_str().expect("secret");

    let ban_get = client
        .get(format!("{base_url}/api/v5/banned"))
        .basic_auth(key_id, Some(secret))
        .send()
        .await
        .expect("get banned");
    assert_eq!(ban_get.status(), StatusCode::OK);

    let ban_del = client
        .delete(format!("{base_url}/api/v5/banned/clientid/any-client"))
        .basic_auth(key_id, Some(secret))
        .send()
        .await
        .expect("delete banned");
    assert_eq!(ban_del.status(), StatusCode::FORBIDDEN);
}

/// Prove API key creation with metadata fields and read-back roundtrip.
#[tokio::test]
async fn tr_011_create_api_key_metadata() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let res = client
        .post(format!("{base_url}/api/v5/api_key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "name": "metadata-test-key",
            "role": "viewer",
            "desc": "test description"
        }))
        .send()
        .await
        .expect("create key");
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.expect("json body");
    assert!(body["api_key"].is_string());
    assert!(body["api_secret"].is_string());
    assert!(body["created_at"].is_string());
    assert_eq!(body["desc"], "test description");
    assert_eq!(body["enable"], json!(true));
    assert_eq!(body["name"], "metadata-test-key");
    assert_eq!(body["role"], "viewer");

    let get_res = client
        .get(format!("{base_url}/api/v5/api_key/metadata-test-key"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("read key");
    assert_eq!(get_res.status(), StatusCode::OK);
    let read_body: serde_json::Value = get_res.json().await.expect("read json");
    assert_eq!(read_body["name"], "metadata-test-key");
    assert_eq!(read_body["desc"], "test description");
    assert_eq!(read_body["role"], "viewer");
}

/// Prove publisher key creation succeeds with explicit publish scope.
#[tokio::test]
async fn tr_011_publisher_only_publish_scope() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let res = client
        .post(format!("{base_url}/api/v5/api_key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "name": "pub-only-publish",
            "role": "publisher",
            "scopes": ["publish"]
        }))
        .send()
        .await
        .expect("create publisher with publish scope");
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.expect("json body");
    assert_eq!(body["name"], "pub-only-publish");
}

/// Prove publisher API key creation rejects login-only scope with 400.
#[tokio::test]
async fn tr_011_publisher_rejects_login_only_scope() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let res = client
        .post(format!("{base_url}/api/v5/api_key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "name": "pub-login-only-reject",
            "role": "publisher",
            "scopes": ["user_management"]
        }))
        .send()
        .await
        .expect("create publisher with login only scope");
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

/// Prove updating role to publisher succeeds with compatible persisted scopes.
#[tokio::test]
async fn tr_011_publisher_role_change_compatible_persisted_scopes() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let create_res = client
        .post(format!("{base_url}/api/v5/api_key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "name": "pub-compat-key",
            "role": "administrator",
            "scopes": ["publish"]
        }))
        .send()
        .await
        .expect("create admin key with publish scope");
    assert_eq!(create_res.status(), StatusCode::OK);

    let put_res = client
        .put(format!("{base_url}/api/v5/api_key/pub-compat-key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "role": "publisher"
        }))
        .send()
        .await
        .expect("update key role to publisher");
    assert_eq!(put_res.status(), StatusCode::OK);
    let body: serde_json::Value = put_res.json().await.expect("json body");
    assert_eq!(body["role"], "publisher");
    assert_eq!(body["scopes"], json!(["publish"]));
}

/// Prove creating publisher API key succeeds with empty scopes list.
#[tokio::test]
async fn tr_011_publisher_with_empty_scopes() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let res = client
        .post(format!("{base_url}/api/v5/api_key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "name": "pub-empty-scopes",
            "role": "publisher",
            "scopes": []
        }))
        .send()
        .await
        .expect("create publisher with empty scopes");
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.expect("json body");
    assert_eq!(body["name"], "pub-empty-scopes");
}

/// Prove creating publisher API key succeeds with omitted scopes.
#[tokio::test]
async fn tr_011_publisher_with_undefined_scopes() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let res = client
        .post(format!("{base_url}/api/v5/api_key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "name": "pub-undefined-scopes",
            "role": "publisher"
        }))
        .send()
        .await
        .expect("create publisher without scopes");
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.expect("json body");
    assert_eq!(body["name"], "pub-undefined-scopes");
}

/// Prove creating API key with unset sentinel for scopes.
#[tokio::test]
async fn tr_011_scopes_create_unset_sentinel() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let res = client
        .post(format!("{base_url}/api/v5/api_key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "name": "scopes-create-unset",
            "scopes": "unset"
        }))
        .send()
        .await
        .expect("create key with unset scopes");
    assert_eq!(res.status(), StatusCode::OK);

    let get_res = client
        .get(format!("{base_url}/api/v5/api_key/scopes-create-unset"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("read key");
    assert_eq!(get_res.status(), StatusCode::OK);
    let body: serde_json::Value = get_res.json().await.expect("json body");
    assert_eq!(body["scopes"], "unset");
}

/// Prove updating API key with an explicit scope list.
#[tokio::test]
async fn tr_011_scopes_update_explicit_list() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let create_res = client
        .post(format!("{base_url}/api/v5/api_key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "name": "scopes-explicit-key"
        }))
        .send()
        .await
        .expect("create key");
    assert_eq!(create_res.status(), StatusCode::OK);

    let put_res = client
        .put(format!("{base_url}/api/v5/api_key/scopes-explicit-key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "scopes": ["connections", "monitoring"]
        }))
        .send()
        .await
        .expect("update scopes");
    assert_eq!(put_res.status(), StatusCode::OK);

    let get_res = client
        .get(format!("{base_url}/api/v5/api_key/scopes-explicit-key"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("read key");
    assert_eq!(get_res.status(), StatusCode::OK);
    let body: serde_json::Value = get_res.json().await.expect("json body");
    assert_eq!(body["scopes"], json!(["connections", "monitoring"]));
}

/// Prove round-trip scope updates with default lists and unset sentinels.
#[tokio::test]
async fn tr_011_scopes_update_unchanged_roundtrip() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let create_res = client
        .post(format!("{base_url}/api/v5/api_key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "name": "scopes-roundtrip-key"
        }))
        .send()
        .await
        .expect("create key");
    assert_eq!(create_res.status(), StatusCode::OK);

    let catalogue_res = client
        .get(format!("{base_url}/api/v5/api_key_scopes"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("get catalogue");
    assert_eq!(catalogue_res.status(), StatusCode::OK);
    let cat_body: serde_json::Value = catalogue_res.json().await.expect("cat json");
    let default_scopes: Vec<String> = cat_body["scopes"]
        .as_array()
        .expect("scopes array")
        .iter()
        .map(|entry| entry["name"].as_str().expect("name").to_string())
        .collect();

    // 1. Update with the full default list: stored as unset.
    let put1 = client
        .put(format!("{base_url}/api/v5/api_key/scopes-roundtrip-key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "scopes": default_scopes
        }))
        .send()
        .await
        .expect("update with full default list");
    assert_eq!(put1.status(), StatusCode::OK);
    let body1: serde_json::Value = put1.json().await.expect("json body");
    assert_eq!(body1["scopes"], "unset");

    // 2. Round-trip once more with the unset sentinel.
    let put2 = client
        .put(format!("{base_url}/api/v5/api_key/scopes-roundtrip-key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "scopes": "unset"
        }))
        .send()
        .await
        .expect("update with unset sentinel");
    assert_eq!(put2.status(), StatusCode::OK);
    let body2: serde_json::Value = put2.json().await.expect("json body");
    assert_eq!(body2["scopes"], "unset");

    // 3. Reversed default list is also order-insensitive and stores as unset.
    let mut reversed = default_scopes.clone();
    reversed.reverse();
    let put3 = client
        .put(format!("{base_url}/api/v5/api_key/scopes-roundtrip-key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "scopes": reversed
        }))
        .send()
        .await
        .expect("update with reversed default list");
    assert_eq!(put3.status(), StatusCode::OK);
    let body3: serde_json::Value = put3.json().await.expect("json body");
    assert_eq!(body3["scopes"], "unset");

    // 4. From unset state, an explicit non-default list takes effect.
    let put4 = client
        .put(format!("{base_url}/api/v5/api_key/scopes-roundtrip-key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "scopes": ["connections", "monitoring"]
        }))
        .send()
        .await
        .expect("update with explicit non-default scopes");
    assert_eq!(put4.status(), StatusCode::OK);
    let body4: serde_json::Value = put4.json().await.expect("json body");
    assert_eq!(body4["scopes"], json!(["connections", "monitoring"]));
}

/// Prove updating API key description, status, and role.
#[tokio::test]
async fn tr_011_update_key_metadata_and_role() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let create_res = client
        .post(format!("{base_url}/api/v5/api_key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "name": "update-meta-key",
            "role": "viewer"
        }))
        .send()
        .await
        .expect("create viewer key");
    assert_eq!(create_res.status(), StatusCode::OK);

    let put_res = client
        .put(format!("{base_url}/api/v5/api_key/update-meta-key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "desc": "NoteVersion1",
            "enable": false,
            "role": "administrator"
        }))
        .send()
        .await
        .expect("update key");
    assert_eq!(put_res.status(), StatusCode::OK);
    let put_body: serde_json::Value = put_res.json().await.expect("put json");
    assert_eq!(put_body["role"], "administrator");
    assert_eq!(put_body["desc"], "NoteVersion1");
    assert_eq!(put_body["enable"], json!(false));

    let get_res = client
        .get(format!("{base_url}/api/v5/api_key/update-meta-key"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("read updated key");
    assert_eq!(get_res.status(), StatusCode::OK);
    let get_body: serde_json::Value = get_res.json().await.expect("get json");
    assert_eq!(get_body["name"], "update-meta-key");
    assert_eq!(get_body["role"], "administrator");
}

/// Prove viewer API key can hold publish scope alongside monitoring scopes.
#[tokio::test]
async fn tr_011_viewer_role_with_publish_scope_allowed() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let res = client
        .post(format!("{base_url}/api/v5/api_key"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "name": "viewer-with-pub",
            "role": "viewer",
            "scopes": ["publish", "monitoring"]
        }))
        .send()
        .await
        .expect("create viewer with publish scope");
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.expect("json body");
    assert_eq!(body["name"], "viewer-with-pub");
}
