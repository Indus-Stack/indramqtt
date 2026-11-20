use super::*;

#[tokio::test]
async fn mn08_v1_disk_log_directory_stays_inside_data_dir() {
    // MN-08: an outside `directory` fails. A relative path works.
    // A refused repeat keeps the old sink.
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mn08-disk");
    std::fs::create_dir_all(&dir).expect("data dir");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    let outside = std::env::temp_dir().join(format!("mn08-outside-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&outside).expect("outside dir");
    std::os::unix::fs::symlink(&outside, dir.join("mn08-link")).expect("symlink");
    let outside_text = outside.to_string_lossy().into_owned();
    for (id, directory) in [
        ("mn08-abs", outside_text.clone()),
        ("mn08-dotdot", "../mn08-escape".to_string()),
        ("mn08-link", "mn08-link".to_string()),
    ] {
        let (status, body) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": id, "kind": "disk_log",
                       "config": {"directory": directory}}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "outside directory refuses for {id}: {body}");
        let message = body["error"].as_str().unwrap_or("");
        assert!(message.contains("directory"), "names directory: {body}");
        assert!(message.contains(id), "names the id: {body}");
        assert!(
            message.contains("inside the data directory"),
            "states the rule: {body}"
        );
        let (status, list) = server.get_auth("/api/v1/connectors", &token).await;
        assert_eq!(status, 200);
        assert!(
            !list
                .as_array()
                .expect("list")
                .iter()
                .any(|row| row["id"] == json!(id)),
            "refused id stores nothing: {list}"
        );
    }
    assert_eq!(
        std::fs::read_dir(&outside).expect("read").count(),
        0,
        "no file appears outside"
    );
    assert!(!dir.parent().unwrap_or(&dir).join("mn08-escape").exists());
    let (status, created) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "mn08-keep", "kind": "disk_log",
                   "config": {"directory": "mn08-inside"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201, "relative directory passes: {created}");
    assert_eq!(created["kind"], json!("disk_log"));
    let (status, _) = server
        .post_auth(
            "/api/v1/rules",
            json!({"name": "mn08-rule", "topic_filter": "mn08/disk",
                   "actions": [{"type": "forwardconnector",
                                "connector_id": "mn08-keep"}]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/publish",
            json!({"topic": "mn08/disk", "payload": "{\"v\":1}", "qos": 0}),
            &token,
        )
        .await;
    assert_eq!(status, 202);
    let active = dir.join("mn08-inside").join("indra.log");
    let mut found = false;
    for _ in 0..50 {
        if std::fs::read_to_string(&active)
            .unwrap_or_default()
            .contains("\"v\":1")
        {
            found = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(found, "publish writes inside the data directory");
    let (status, body) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "mn08-keep", "kind": "disk_log",
                   "config": {"directory": outside_text}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "refused repeat fails: {body}");
    let (status, list) = server.get_auth("/api/v1/connectors", &token).await;
    assert_eq!(status, 200);
    assert!(
        list.as_array()
            .expect("list")
            .iter()
            .any(|row| row["id"] == json!("mn08-keep")),
        "old entry stays: {list}"
    );
    let (status, _) = server
        .post_auth(
            "/api/v5/publish",
            json!({"topic": "mn08/disk", "payload": "{\"v\":2}", "qos": 0}),
            &token,
        )
        .await;
    assert_eq!(status, 202);
    let mut found = false;
    for _ in 0..50 {
        if std::fs::read_to_string(&active)
            .unwrap_or_default()
            .contains("\"v\":2")
        {
            found = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(found, "old sink still writes after refused repeat");
    assert_eq!(
        std::fs::read_dir(&outside).expect("read").count(),
        0,
        "repeat stores nothing outside"
    );
    std::fs::remove_dir_all(&dir).ok();
    std::fs::remove_dir_all(&outside).ok();
}
