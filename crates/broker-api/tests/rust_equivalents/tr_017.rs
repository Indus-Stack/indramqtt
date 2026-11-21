use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
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

/// Prove that client detail reports connected true for a live session.
///
/// Create a session through the session manager.
/// Read the client through the management API.
/// Verify the client id and the connected flag.
#[tokio::test]
async fn tr_017_client_connected_true() {
    let (base_url, state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    state.sessions.get_or_create("tr017-live-client", true);

    let res = client
        .get(format!("{base_url}/api/v5/clients/tr017-live-client"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("get client details");
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.expect("json body");
    assert_eq!(body["clientid"], "tr017-live-client");
    assert_eq!(body["connected"], true);
}

/// Prove that client detail reports session fields for a live session.
///
/// Create a session through the session manager.
/// Read the client through the management API.
/// Verify the expiry interval and the connected flag.
#[tokio::test]
async fn tr_017_client_session_detail_reports_expiry() {
    let (base_url, state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let (session, _) = state.sessions.get_or_create("tr017-session-client", true);
    session.set_session_expiry(123546);

    let res = client
        .get(format!("{base_url}/api/v5/clients/tr017-session-client"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("get client details");
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.expect("json body");
    assert_eq!(body["clientid"], "tr017-session-client");
    assert_eq!(body["connected"], true);
    assert_eq!(body["expiry_interval"], 123546);
}

/// Prove that the publish API accepts payload, QoS and retain flag.
///
/// Register a session and subscribe it through the management API.
/// Publish a retained message through the management API.
/// Verify the stored QoS and payload bytes.
#[tokio::test]
async fn tr_017_publish_with_payload_qos_retain() {
    let (base_url, state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    // Durable session: detached publishes queue offline, so the
    // publish answers 200 instead of 503 with no live connection.
    state.sessions.get_or_create("tr017-pub-client", false);
    let sub_res = client
        .post(format!(
            "{base_url}/api/v5/clients/tr017-pub-client/subscribe"
        ))
        .bearer_auth(&admin_token)
        .json(&json!({"topic": "tr017-pub", "qos": 1}))
        .send()
        .await
        .expect("subscribe client");
    assert_eq!(sub_res.status(), StatusCode::OK);

    let pub_res = client
        .post(format!("{base_url}/api/v5/publish"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "topic": "tr017-pub",
            "payload": "hello-tr017",
            "qos": 1,
            "retain": true,
        }))
        .send()
        .await
        .expect("publish message");
    assert_eq!(pub_res.status(), StatusCode::OK);

    let get_res = client
        .get(format!("{base_url}/api/v5/mqtt/retainer/message/tr017-pub"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("read retained row");
    assert_eq!(get_res.status(), StatusCode::OK);
    let get_body: serde_json::Value = get_res.json().await.expect("retained json");
    assert_eq!(get_body["topic"], "tr017-pub");
    assert_eq!(get_body["qos"], 1);
    let raw = base64::engine::general_purpose::STANDARD
        .decode(get_body["payload"].as_str().expect("payload is string"))
        .expect("payload is base64");
    assert_eq!(raw, b"hello-tr017");
}

/// Prove that a retained row is replaced on a second publish.
///
/// Publish two payloads to one topic through the management API.
/// Read the row through the management API.
/// Verify the second payload wins.
#[tokio::test]
async fn tr_017_retained_add_or_replace_updates_row() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    for payload in ["tr017-first", "tr017-second"] {
        let pub_res = client
            .post(format!("{base_url}/api/v5/publish"))
            .bearer_auth(&admin_token)
            .json(&json!({
                "topic": "tr017-replace",
                "payload": payload,
                "qos": 1,
                "retain": true,
            }))
            .send()
            .await
            .expect("publish retained");
        assert_eq!(pub_res.status(), StatusCode::ACCEPTED);
    }

    let get_res = client
        .get(format!(
            "{base_url}/api/v5/mqtt/retainer/message/tr017-replace"
        ))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("read replaced row");
    assert_eq!(get_res.status(), StatusCode::OK);
    let get_body: serde_json::Value = get_res.json().await.expect("retained json");
    assert_eq!(get_body["topic"], "tr017-replace");
    let raw = base64::engine::general_purpose::STANDARD
        .decode(get_body["payload"].as_str().expect("payload is string"))
        .expect("payload is base64");
    assert_eq!(raw, b"tr017-second");
}

/// Prove that clearing removes every retained row.
///
/// Publish two retained rows through the management API.
/// Clear all rows through the management API.
/// Verify both rows are gone and the list is empty.
#[tokio::test]
async fn tr_017_retained_clear_removes_all_rows() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    for topic in ["tr017-clear-a", "tr017-clear-b"] {
        let pub_res = client
            .post(format!("{base_url}/api/v5/publish"))
            .bearer_auth(&admin_token)
            .json(&json!({
                "topic": topic,
                "payload": "clear-me",
                "qos": 0,
                "retain": true,
            }))
            .send()
            .await
            .expect("publish retained");
        assert_eq!(pub_res.status(), StatusCode::ACCEPTED);
    }

    let clear_res = client
        .delete(format!("{base_url}/api/v5/mqtt/retainer/messages"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("clear retained rows");
    assert_eq!(clear_res.status(), StatusCode::NO_CONTENT);

    for topic in ["tr017-clear-a", "tr017-clear-b"] {
        let get_res = client
            .get(format!("{base_url}/api/v5/mqtt/retainer/message/{topic}"))
            .bearer_auth(&admin_token)
            .send()
            .await
            .expect("read cleared row");
        assert_eq!(get_res.status(), StatusCode::NOT_FOUND);
    }

    let list_res = client
        .get(format!("{base_url}/api/v5/mqtt/retainer/messages"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("list retained rows");
    assert_eq!(list_res.status(), StatusCode::OK);
    let list_body: serde_json::Value = list_res.json().await.expect("list json");
    assert_eq!(list_body["meta"]["count"], 0);
}

/// Prove that a retained read returns the stored fields.
///
/// Publish a retained row through the management API.
/// Read the row through the management API.
/// Verify topic, QoS, message id and payload bytes.
#[tokio::test]
async fn tr_017_retained_get_returns_stored_fields() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let pub_res = client
        .post(format!("{base_url}/api/v5/publish"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "topic": "tr017-fields",
            "payload": "field-bytes",
            "qos": 1,
            "retain": true,
        }))
        .send()
        .await
        .expect("publish retained");
    assert_eq!(pub_res.status(), StatusCode::ACCEPTED);

    let get_res = client
        .get(format!(
            "{base_url}/api/v5/mqtt/retainer/message/tr017-fields"
        ))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("read retained row");
    assert_eq!(get_res.status(), StatusCode::OK);
    let get_body: serde_json::Value = get_res.json().await.expect("retained json");
    assert_eq!(get_body["topic"], "tr017-fields");
    assert_eq!(get_body["qos"], 1);
    let first_msgid = get_body["msgid"].as_str().expect("msgid is string");
    assert_eq!(first_msgid.len(), 32);

    let again_res = client
        .get(format!(
            "{base_url}/api/v5/mqtt/retainer/message/tr017-fields"
        ))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("read retained row again");
    assert_eq!(again_res.status(), StatusCode::OK);
    let again_body: serde_json::Value = again_res.json().await.expect("retained json");
    assert_eq!(
        again_body["msgid"].as_str().expect("msgid is string"),
        first_msgid
    );
    let raw = base64::engine::general_purpose::STANDARD
        .decode(get_body["payload"].as_str().expect("payload is string"))
        .expect("payload is base64");
    assert_eq!(raw, b"field-bytes");
}

/// Prove that a single delete removes only its own row.
///
/// Publish two retained rows through the management API.
/// Delete one row through the management API.
/// Verify the other row still reads back.
#[tokio::test]
async fn tr_017_retained_remove_deletes_single_row() {
    let (base_url, _state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    for topic in ["tr017-drop", "tr017-keep"] {
        let pub_res = client
            .post(format!("{base_url}/api/v5/publish"))
            .bearer_auth(&admin_token)
            .json(&json!({
                "topic": topic,
                "payload": "row-payload",
                "qos": 0,
                "retain": true,
            }))
            .send()
            .await
            .expect("publish retained");
        assert_eq!(pub_res.status(), StatusCode::ACCEPTED);
    }

    let del_res = client
        .delete(format!(
            "{base_url}/api/v5/mqtt/retainer/message/tr017-drop"
        ))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("delete retained row");
    assert_eq!(del_res.status(), StatusCode::NO_CONTENT);

    let gone_res = client
        .get(format!(
            "{base_url}/api/v5/mqtt/retainer/message/tr017-drop"
        ))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("read deleted row");
    assert_eq!(gone_res.status(), StatusCode::NOT_FOUND);

    let kept_res = client
        .get(format!(
            "{base_url}/api/v5/mqtt/retainer/message/tr017-keep"
        ))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("read kept row");
    assert_eq!(kept_res.status(), StatusCode::OK);
    let kept_body: serde_json::Value = kept_res.json().await.expect("retained json");
    assert_eq!(kept_body["topic"], "tr017-keep");
}

/// Prove that bulk subscribe installs two valid rows.
///
/// Create a session through the session manager.
/// Send two valid entries through the management API.
/// Verify both rows read back with exact topics and QoS.
#[tokio::test]
async fn tr_017_bulk_subscribe_installs_two_rows() {
    let (base_url, state) = start_server().await;
    // Five seconds covers loopback latency plus test runner margin.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    state.sessions.get_or_create("tr017-bulk-client", true);

    let bulk_res = client
        .post(format!(
            "{base_url}/api/v5/clients/tr017-bulk-client/subscribe/bulk"
        ))
        .bearer_auth(&admin_token)
        .json(&json!([
            {"topic": "tr017/bulk/first", "qos": 0},
            {"topic": "tr017/bulk/second", "qos": 1},
        ]))
        .send()
        .await
        .expect("bulk subscribe");
    assert_eq!(bulk_res.status(), StatusCode::OK);
    let bulk_body: serde_json::Value = bulk_res.json().await.expect("bulk json");
    let results = bulk_body.as_array().expect("bulk result is a list");
    assert_eq!(results.len(), 2);

    let list_res = client
        .get(format!(
            "{base_url}/api/v5/clients/tr017-bulk-client/subscriptions"
        ))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("list subscriptions");
    assert_eq!(list_res.status(), StatusCode::OK);
    let list_body: serde_json::Value = list_res.json().await.expect("list json");
    let subs = list_body.as_array().expect("subscriptions list");
    assert!(
        subs.iter()
            .any(|s| s["topic"] == json!("tr017/bulk/first") && s["qos"] == json!(0)),
        "first bulk row lands: {subs:?}"
    );
    assert!(
        subs.iter()
            .any(|s| s["topic"] == json!("tr017/bulk/second") && s["qos"] == json!(1)),
        "second bulk row lands: {subs:?}"
    );
}
