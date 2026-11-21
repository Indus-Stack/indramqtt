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

/// Prove that a failed restore preserves existing records.
///
/// Create a rule through the management API.
/// Attempt to import a corrupt backup archive.
/// Verify that the import fails with HTTP 400.
/// Verify that the rule still exists.
#[tokio::test]
async fn tr_016_restore_failure_keeps_existing_records() {
    let (base_url, _state) = start_server().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let create_res = client
        .post(format!("{base_url}/api/v5/rules"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "name": "tr016-keep-rule",
            "sql": "SELECT * FROM \"tr016/#\"",
            "actions": []
        }))
        .send()
        .await
        .expect("create rule");
    assert_eq!(create_res.status(), StatusCode::CREATED);
    let rule_obj: serde_json::Value = create_res.json().await.expect("rule json");
    let rule_id = rule_obj["id"].as_str().expect("rule id");

    let garbage = base64::engine::general_purpose::STANDARD.encode(b"not gzip data");
    let upload_res = client
        .post(format!("{base_url}/api/v5/data/files"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "filename": "tr016-corrupt.tar.gz",
            "content": garbage
        }))
        .send()
        .await
        .expect("upload corrupt archive");
    assert_eq!(upload_res.status(), StatusCode::NO_CONTENT);

    let import_res = client
        .post(format!("{base_url}/api/v5/data/import"))
        .bearer_auth(&admin_token)
        .json(&json!({"filename": "tr016-corrupt.tar.gz"}))
        .send()
        .await
        .expect("import corrupt archive");
    assert_eq!(import_res.status(), StatusCode::BAD_REQUEST);

    let check_res = client
        .get(format!("{base_url}/api/v5/rules/{rule_id}"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("check rule");
    assert_eq!(check_res.status(), StatusCode::OK);
    let check_body: serde_json::Value = check_res.json().await.expect("rule json");
    assert_eq!(check_body["name"], "tr016-keep-rule");
}

/// Prove that backup archives with symlinks fail with HTTP 400.
///
/// Construct a tar archive holding a symlink entry.
/// Upload the archive through the files API.
/// Import the archive and verify refusal.
#[tokio::test]
async fn tr_016_symlink_member_rejected() {
    let (base_url, _state) = start_server().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let mut tar_builder = tar::Builder::new(encoder);

    let meta_bytes = serde_json::to_vec(&json!({
        "format": "indra-backup-1",
        "version": "test",
        "exported_at": "2026-01-01T00:00:00+00:00",
        "roots": ["rules"],
        "tables": []
    }))
    .expect("meta bytes");
    let mut meta_header = tar::Header::new_gnu();
    meta_header.set_size(meta_bytes.len() as u64);
    meta_header.set_mode(0o644);
    tar_builder
        .append_data(&mut meta_header, "backup-1-0/META.json", &meta_bytes[..])
        .expect("append meta");

    let mut link_header = tar::Header::new_gnu();
    link_header.set_entry_type(tar::EntryType::Symlink);
    link_header.set_link_name("/etc/passwd").expect("link name");
    link_header.set_size(0);
    link_header.set_mode(0o777);
    tar_builder
        .append_data(&mut link_header, "backup-1-0/symlink_entry", &[][..])
        .expect("append symlink");

    let encoder = tar_builder.into_inner().expect("tar inner");
    let gz_bytes = encoder.finish().expect("finish gzip");
    let payload = base64::engine::general_purpose::STANDARD.encode(gz_bytes);

    let upload_res = client
        .post(format!("{base_url}/api/v5/data/files"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "filename": "tr016-symlink.tar.gz",
            "content": payload
        }))
        .send()
        .await
        .expect("upload symlink archive");
    assert_eq!(upload_res.status(), StatusCode::NO_CONTENT);

    let import_res = client
        .post(format!("{base_url}/api/v5/data/import"))
        .bearer_auth(&admin_token)
        .json(&json!({"filename": "tr016-symlink.tar.gz"}))
        .send()
        .await
        .expect("import symlink archive");
    assert_eq!(import_res.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = import_res.json().await.expect("json");
    assert_eq!(body["code"], "BAD_REQUEST");
}

/// Prove that backup archives with traversal paths fail with HTTP 400.
///
/// Construct a tar archive holding an unexpected entry.
/// Upload the archive through the files API.
/// Import the archive and verify refusal.
#[tokio::test]
async fn tr_016_tar_outside_backup_dir() {
    let (base_url, _state) = start_server().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let mut tar_builder = tar::Builder::new(encoder);

    let meta_bytes = serde_json::to_vec(&json!({
        "format": "indra-backup-1",
        "version": "test",
        "exported_at": "2026-01-01T00:00:00+00:00",
        "roots": ["rules"],
        "tables": []
    }))
    .expect("meta bytes");
    let mut meta_header = tar::Header::new_gnu();
    meta_header.set_size(meta_bytes.len() as u64);
    meta_header.set_mode(0o644);
    tar_builder
        .append_data(&mut meta_header, "backup-1-0/META.json", &meta_bytes[..])
        .expect("append meta");

    let evil_content = b"{}";
    let mut evil_header = tar::Header::new_gnu();
    evil_header.set_size(evil_content.len() as u64);
    evil_header.set_mode(0o644);
    tar_builder
        .append_data(&mut evil_header, "outside/evil.json", &evil_content[..])
        .expect("append evil part");

    let encoder = tar_builder.into_inner().expect("tar inner");
    let gz_bytes = encoder.finish().expect("finish gzip");
    let payload = base64::engine::general_purpose::STANDARD.encode(gz_bytes);

    let upload_res = client
        .post(format!("{base_url}/api/v5/data/files"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "filename": "tr016-outside.tar.gz",
            "content": payload
        }))
        .send()
        .await
        .expect("upload outside archive");
    assert_eq!(upload_res.status(), StatusCode::NO_CONTENT);

    let import_res = client
        .post(format!("{base_url}/api/v5/data/import"))
        .bearer_auth(&admin_token)
        .json(&json!({"filename": "tr016-outside.tar.gz"}))
        .send()
        .await
        .expect("import outside archive");
    assert_eq!(import_res.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = import_res.json().await.expect("json");
    assert_eq!(body["code"], "BAD_REQUEST");
}

/// Prove that backup uploads with unsafe filenames fail with HTTP 400.
///
/// Attempt to upload a backup archive named `...tar.gz`.
/// Verify that the API rejects the request with HTTP 400.
#[tokio::test]
async fn tr_016_unsafe_backup_name() {
    let (base_url, _state) = start_server().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let upload_res = client
        .post(format!("{base_url}/api/v5/data/files"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "filename": "...tar.gz",
            "content": "aGVsbG8="
        }))
        .send()
        .await
        .expect("upload unsafe file name");
    assert_eq!(upload_res.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = upload_res.json().await.expect("json");
    assert_eq!(body["code"], "BAD_REQUEST");
}

/// Prove that creating a user with a namespaced role fails with HTTP 400.
///
/// Send a request with a namespaced administrator role.
/// Verify that the API rejects the creation with HTTP 400.
#[tokio::test]
async fn tr_016_namespaced_user_api_checks() {
    let (base_url, _state) = start_server().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let res = client
        .post(format!("{base_url}/api/v5/users"))
        .bearer_auth(&admin_token)
        .json(&json!({
            "username": "tr016-ns-user",
            "password": "SuperSecurePassword123!",
            "role": "ns:ns1::administrator"
        }))
        .send()
        .await
        .expect("create namespaced user");
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = res.json().await.expect("json");
    assert_eq!(body["code"], "BAD_REQUEST");
}

/// Prove that Prometheus auth metrics return HTTP 200 and text exposition.
///
/// Query the auth metrics endpoint.
/// Verify status 200 and Prometheus text content type.
#[tokio::test]
async fn tr_016_prometheus_auth_api() {
    let (base_url, _state) = start_server().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let res = client
        .get(format!("{base_url}/api/v5/prometheus/auth"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("get prometheus auth");
    assert_eq!(res.status(), StatusCode::OK);
    let content_type = res
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .expect("content type header")
        .to_str()
        .expect("content type string");
    assert_eq!(content_type, "text/plain; version=0.0.4");
    let text = res.text().await.expect("text body");
    assert!(text.contains("indramqtt_"));
}

/// Prove that aggregated Prometheus auth metrics return HTTP 200.
///
/// Query the endpoint with the aggregation parameter.
/// Verify status 200 and Prometheus text content type.
#[tokio::test]
async fn tr_016_prometheus_auth_api_aggregated() {
    let (base_url, _state) = start_server().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let res = client
        .get(format!(
            "{base_url}/api/v5/prometheus/auth?mode=all_nodes_aggregated"
        ))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("get aggregated prometheus auth");
    assert_eq!(res.status(), StatusCode::OK);
    let content_type = res
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .expect("content type header")
        .to_str()
        .expect("content type string");
    assert_eq!(content_type, "text/plain; version=0.0.4");
    let text = res.text().await.expect("text body");
    assert!(text.contains("indramqtt_"));
}

/// Prove that disconnected sessions report connected false.
///
/// Insert a session marked as disconnected.
/// Query client details through the management API.
/// Verify that the response reports connected false.
#[tokio::test]
async fn tr_016_client_connected_false_success() {
    let (base_url, state) = start_server().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let (session, _) = state
        .sessions
        .get_or_create("tr016-disconnected-client", false);
    *session.connected.write() = false;

    let res = client
        .get(format!(
            "{base_url}/api/v5/clients/tr016-disconnected-client"
        ))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("get client details");
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.expect("json body");
    assert_eq!(body["clientid"], "tr016-disconnected-client");
    assert_eq!(body["connected"], false);
}

/// Prove that querying a missing client returns HTTP 404.
///
/// Query client details for a client identifier that does not exist.
/// Verify that the API returns HTTP 404 with CLIENTID_NOT_FOUND.
#[tokio::test]
async fn tr_016_client_connected_null_success() {
    let (base_url, _state) = start_server().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");
    let admin_token = login_admin(&client, &base_url).await;

    let res = client
        .get(format!("{base_url}/api/v5/clients/tr016-missing-client"))
        .bearer_auth(&admin_token)
        .send()
        .await
        .expect("get missing client details");
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
    let body: serde_json::Value = res.json().await.expect("json body");
    assert_eq!(body["code"], "CLIENTID_NOT_FOUND");
}
