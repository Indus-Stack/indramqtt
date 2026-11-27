/// Backup file download through the broker HTTP API (C18-05).
///
/// Upload one file with random bytes through POST /data/files. Fetch it
/// through GET /data/files/{filename}. Check the bytes match. Check an
/// unknown name returns 404 NOT_FOUND. Check a bad name returns 400.
#[tokio::test]
async fn backup_file_download_through_broker_api() {
    use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
    use broker_api::ApiState;
    use broker_auth::MemoryAuth;
    use broker_config::ConfigRegistry;
    use broker_observability::Metrics;
    use broker_router::{ConnTable, Router};
    use broker_rules::{BackpressurePolicy, RuleEngine};
    use broker_session::SessionManager;
    use rand::Rng;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::net::TcpListener;

    // Bound for one management request in this test. Ten seconds
    // matches STEP above; a cold debug boot plus parallel hashing
    // and fsync completes within ten seconds under load.
    const TIMEOUT: Duration = Duration::from_secs(10);

    let engine = Arc::new(RuleEngine::new(16, BackpressurePolicy::DropOldest));
    let sessions = Arc::new(SessionManager::new());
    let sub_router = Arc::new(Router::new());
    let metrics = Arc::new(Metrics::new());
    let auth = Arc::new(MemoryAuth::new());
    let conns = Arc::new(ConnTable::default());
    let scratch = std::env::temp_dir().join(format!(
        "indramqtt-backup-e2e-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let config = Arc::new(ConfigRegistry::load(&scratch).expect("validated defaults load"));
    let (edge_tx, _edge_rx) = tokio::sync::mpsc::unbounded_channel::<brokerlink::BrokerFrame>();
    let api_state = ApiState::new(
        engine,
        sessions,
        sub_router,
        metrics,
        auth,
        conns,
        config,
        "indra-node-1".to_string(),
        edge_tx,
    );

    let api_listener = tokio::time::timeout(TIMEOUT, TcpListener::bind("127.0.0.1:0"))
        .await
        .expect("bind API listener in time")
        .expect("bind API listener");
    let api_port = api_listener.local_addr().expect("API addr").port();
    tokio::spawn(async move { broker_api::serve(api_listener, api_state).await });
    let base = format!("http://127.0.0.1:{api_port}");

    // Log in. Use the default admin credential first.
    let http = reqwest::Client::new();
    let login = tokio::time::timeout(
        TIMEOUT,
        http.post(format!("{base}/api/v5/login"))
            .json(&serde_json::json!({"username": "admin", "password": "public"}))
            .send(),
    )
    .await
    .expect("login request in time")
    .expect("login request");
    assert_eq!(login.status(), 200);
    let login_body: serde_json::Value = tokio::time::timeout(TIMEOUT, login.json())
        .await
        .expect("login body in time")
        .expect("login body");
    let fresh = login_body["token"]
        .as_str()
        .expect("login token")
        .to_string();
    let changed = tokio::time::timeout(
        TIMEOUT,
        http.put(format!("{base}/api/v5/users/admin/change_pwd"))
            .bearer_auth(&fresh)
            .json(&serde_json::json!({"old_pwd": "public", "new_pwd": "Backup-e2e-1!"}))
            .send(),
    )
    .await
    .expect("change password request in time")
    .expect("change password request");
    assert_eq!(changed.status(), 204);
    let login = tokio::time::timeout(
        TIMEOUT,
        http.post(format!("{base}/api/v5/login"))
            .json(&serde_json::json!({"username": "admin", "password": "Backup-e2e-1!"}))
            .send(),
    )
    .await
    .expect("second login request in time")
    .expect("second login request");
    assert_eq!(login.status(), 200);
    let login_body: serde_json::Value = tokio::time::timeout(TIMEOUT, login.json())
        .await
        .expect("login body in time")
        .expect("login body");
    let token = login_body["token"]
        .as_str()
        .expect("login token")
        .to_string();

    // Make random bytes. The broker cannot know them in advance.
    let mut rng = rand::rng();
    let mut test_content = vec![0u8; 8192];
    rng.fill_bytes(&mut test_content);
    let content = BASE64_STANDARD.encode(&test_content);
    let filename = "e2e-dl-hit.tar.gz";
    let upload = tokio::time::timeout(
        TIMEOUT,
        http.post(format!("{base}/api/v5/data/files"))
            .bearer_auth(&token)
            .json(&serde_json::json!({"filename": filename, "content": content}))
            .send(),
    )
    .await
    .expect("upload request in time")
    .expect("upload request");
    assert_eq!(upload.status(), 204);

    // Fetch the file. Check status, type, name and bytes.
    let download = tokio::time::timeout(
        TIMEOUT,
        http.get(format!("{base}/api/v5/data/files/{filename}"))
            .bearer_auth(&token)
            .send(),
    )
    .await
    .expect("download request in time")
    .expect("download request");
    assert_eq!(download.status(), 200);
    assert_eq!(
        download
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("application/octet-stream")
    );
    assert!(download
        .headers()
        .get("content-disposition")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains(filename))
        .unwrap_or(false));
    let bytes = tokio::time::timeout(TIMEOUT, download.bytes())
        .await
        .expect("download body in time")
        .expect("response body readable");
    assert_eq!(bytes.as_ref(), test_content.as_slice());

    // Fetch an unknown name. Check 404 NOT_FOUND.
    let missing = tokio::time::timeout(
        TIMEOUT,
        http.get(format!("{base}/api/v5/data/files/e2e-dl-missing.tar.gz"))
            .bearer_auth(&token)
            .send(),
    )
    .await
    .expect("missing request in time")
    .expect("missing request");
    assert_eq!(missing.status(), 404);
    let body: serde_json::Value = missing.json().await.expect("error body");
    assert_eq!(body["code"], "NOT_FOUND");

    // Fetch a bad name. Check 400 BAD_REQUEST.
    let bad = tokio::time::timeout(
        TIMEOUT,
        http.get(format!("{base}/api/v5/data/files/evil..traversal.tar.gz"))
            .bearer_auth(&token)
            .send(),
    )
    .await
    .expect("bad-name request in time")
    .expect("bad-name request");
    assert_eq!(bad.status(), 400);
    let body: serde_json::Value = bad.json().await.expect("error body");
    assert_eq!(body["code"], "BAD_REQUEST");

    // Fetch with an unknown node. Check 404 NOT_FOUND.
    let bad_node = tokio::time::timeout(
        TIMEOUT,
        http.get(format!(
            "{base}/api/v5/data/files/{filename}?node=no-such-node"
        ))
        .bearer_auth(&token)
        .send(),
    )
    .await
    .expect("node request in time")
    .expect("node request");
    assert_eq!(bad_node.status(), 404);
    let body: serde_json::Value = bad_node.json().await.expect("error body");
    assert_eq!(body["code"], "NOT_FOUND");

    // Remove the file. Keep the scratch dir clean.
    let delete = tokio::time::timeout(
        TIMEOUT,
        http.delete(format!("{base}/api/v5/data/files/{filename}"))
            .bearer_auth(&token)
            .send(),
    )
    .await
    .expect("delete request in time")
    .expect("delete request");
    assert_eq!(delete.status(), 204);
    let _ = std::fs::remove_dir_all(&scratch);
}
