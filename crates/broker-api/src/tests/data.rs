use super::*;

#[tokio::test]
async fn data_export_round_trip_serves_live_tables() {
    // Export through the broker: seed live state through the API,
    // export it, then read the file back from the backup dir.
    let dir = unique_data_dir("data-export");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;

    let (status, _) = server
        .post_auth(
            "/api/v5/banned",
            json!({"as": "clientid", "who": "w5-26-live", "reason": "export check"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = server
        .post_auth(
            "/api/v1/auth/users",
            json!({"username": "w5-26-sensor", "password": "Sup3r-secret-pw!"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    let (status, body) = server
        .post_auth(
            "/api/v5/data/export",
            json!({"root_keys": ["rules"], "table_sets": ["banned", "builtin_authn"]}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let filename = body["filename"].as_str().expect("export names its file");
    assert!(filename.ends_with(".tar.gz"));

    // The file reference resolves in the on-disk backup dir.
    let path = dir.join("backup").join(filename);
    assert!(path.is_file());
    let parts = read_backup_file(&path);
    assert!(!parts.is_empty());

    // Live rows cross the export: the ban and the user appear.
    let banned = parts
        .iter()
        .find(|(name, _)| name.ends_with("/tables/banned.json"))
        .expect("banned table ships");
    assert!(banned.1.contains("w5-26-live"));
    let authn = parts
        .iter()
        .find(|(name, _)| name.ends_with("/tables/builtin_authn.json"))
        .expect("authn table ships");
    assert!(authn.1.contains("w5-26-sensor"));

    // Secrets stay out: no password hash ships in any part.
    for (_, text) in &parts {
        assert!(!text.contains("password_hash"));
        assert!(!text.contains("Sup3r-secret-pw!"));
    }
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

/// TM-01: two tenants share one username with different passwords.
/// Real API calls create the users and rules per tenant; the CONNECT
/// and publish checks run through the broker auth store; a reload
/// keeps both tenants; a restart on the same data dir keeps both;
/// the export file carries both tenants without secrets.
#[tokio::test]
async fn tm01_two_tenants_persist_across_reload_restart_and_export() {
    let dir = unique_data_dir("tm01-storage");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    state
        .auth
        .set_policy(broker_auth::PasswordHashPolicy::for_tests());
    let token = server.login_as_admin().await;
    // Same username, different passwords, disjoint rules per tenant.
    for (tenant, password, topic) in [
        ("tenant-a", "pw-for-a-7x", "t/a/#"),
        ("tenant-b", "pw-for-b-9q", "t/b/#"),
    ] {
        let (status, _) = server
            .post_auth(
                &format!("/api/v1/auth/users?tenant={tenant}"),
                json!({"username": "sam", "password": password}),
                &token,
            )
            .await;
        assert_eq!(status, 201, "user creates in {tenant}");
        for action in ["publish", "subscribe"] {
            let (status, _) = server
                .post_auth(
                    &format!("/api/v1/auth/acls?tenant={tenant}"),
                    json!({
                        "client_pattern": "*",
                        "action": action,
                        "topic_pattern": topic,
                        "allow": true,
                    }),
                    &token,
                )
                .await;
            assert_eq!(status, 201, "{action} rule creates in {tenant}");
        }
    }
    // Default tenant rows work through the same routes with no change.
    let (status, _) = server
        .post_auth(
            "/api/v1/auth/users",
            json!({"username": "base", "password": "base-pw-3z"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v1/auth/acls",
            json!({
                "client_pattern": "base",
                "action": "all",
                "topic_pattern": "t/#",
                "allow": true,
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    // A bad tenant id fails with 400 and applies nothing.
    let long = "t".repeat(129);
    let (status, _) = server
        .get_auth(&format!("/api/v1/auth/users?tenant={long}"), &token)
        .await;
    assert_eq!(status, 400);
    // Broker-path checks: the same calls the kernel makes per CONNECT
    // and per message. Each password works only in its own tenant.
    assert!(state
        .auth
        .authenticate_in_tenant("tenant-a", "c", Some("sam"), Some(b"pw-for-a-7x"))
        .await
        .is_ok());
    assert!(state
        .auth
        .authenticate_in_tenant("tenant-b", "c", Some("sam"), Some(b"pw-for-b-9q"))
        .await
        .is_ok());
    assert!(state
        .auth
        .authenticate_in_tenant("tenant-a", "c", Some("sam"), Some(b"pw-for-b-9q"))
        .await
        .is_err());
    assert!(state
        .auth
        .authenticate_in_tenant("tenant-b", "c", Some("sam"), Some(b"pw-for-a-7x"))
        .await
        .is_err());
    // A publish allowed in A is refused in B and vice versa.
    let topic_a = Topic::new("t/a/1").expect("topic");
    let topic_b = Topic::new("t/b/1").expect("topic");
    assert!(state
        .auth
        .authorize_publish_in_tenant("tenant-a", "sam", &topic_a)
        .await
        .is_ok());
    assert!(state
        .auth
        .authorize_publish_in_tenant("tenant-b", "sam", &topic_a)
        .await
        .is_err());
    assert!(state
        .auth
        .authorize_publish_in_tenant("tenant-b", "sam", &topic_b)
        .await
        .is_ok());
    assert!(state
        .auth
        .authorize_publish_in_tenant("tenant-a", "sam", &topic_b)
        .await
        .is_err());
    // Delivery isolation: one subscription per tenant on the same
    // filter; a publish in A matches only the subscriber in A.
    let filter = TopicFilter::new("t/#").expect("filter");
    state.router.subscribe(
        &filter,
        broker_router::Subscription::new_in_tenant("sub-a", 7101, QoS::AtMostOnce, "tenant-a"),
    );
    state.router.subscribe(
        &filter,
        broker_router::Subscription::new_in_tenant("sub-b", 7102, QoS::AtMostOnce, "tenant-b"),
    );
    let (matched_a, _) = state
        .router
        .matches_in_tenant_counted(&topic_a, None, "tenant-a");
    assert!(matched_a.iter().any(|s| s.client_id.as_ref() == "sub-a"));
    assert!(!matched_a.iter().any(|s| s.client_id.as_ref() == "sub-b"));
    let (matched_b, _) = state
        .router
        .matches_in_tenant_counted(&topic_b, None, "tenant-b");
    assert!(matched_b.iter().any(|s| s.client_id.as_ref() == "sub-b"));
    assert!(!matched_b.iter().any(|s| s.client_id.as_ref() == "sub-a"));
    // Reload keeps both tenants.
    let candidate = registry.snapshot().as_ref().clone();
    let (status, body) = server
        .post_auth(
            "/api/v1/config/reload",
            json!({"snapshot": candidate, "actor": "tm01", "summary": "keep both tenants"}),
            &token,
        )
        .await;
    assert_eq!(status, 200, "reload keeps both tenants: {body}");
    assert!(state
        .auth
        .authenticate_in_tenant("tenant-a", "c", Some("sam"), Some(b"pw-for-a-7x"))
        .await
        .is_ok());
    assert!(state
        .auth
        .authenticate_in_tenant("tenant-b", "c", Some("sam"), Some(b"pw-for-b-9q"))
        .await
        .is_ok());
    // The state file carries both tenants; plaintext never persists.
    let text = std::fs::read_to_string(dir.join("state.toml")).expect("state file written");
    assert!(text.contains("tenant-a") && text.contains("tenant-b"));
    assert!(!text.contains("pw-for-a-7x") && !text.contains("pw-for-b-9q"));
    // The export file carries both tenants without secrets.
    let (status, body) = server
        .post_auth(
            "/api/v5/data/export",
            json!({"root_keys": ["mqtt_users"], "table_sets": ["builtin_authn", "builtin_authz"]}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let filename = body["filename"].as_str().expect("export names its file");
    let parts = read_backup_file(&dir.join("backup").join(filename));
    let snap = parts
        .iter()
        .find(|(name, _)| name.ends_with("/snapshot/mqtt_users.json"))
        .expect("mqtt_users root ships");
    assert!(snap.1.contains("tenant-a") && snap.1.contains("tenant-b"));
    let authn = parts
        .iter()
        .find(|(name, _)| name.ends_with("/tables/builtin_authn.json"))
        .expect("authn table ships");
    assert!(authn.1.contains("tenant-a") && authn.1.contains("tenant-b"));
    for (_, text) in &parts {
        assert!(!text.contains("password_hash"));
        assert!(!text.contains("pw-for-a-7x"));
        assert!(!text.contains("pw-for-b-9q"));
    }
    // Restart on the same data dir keeps both tenants.
    drop(server);
    drop(state);
    drop(registry);
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("registry reloads after a restart"));
    let (server, state) = TestServer::start_with_registry(registry).await;
    assert!(state
        .auth
        .authenticate_in_tenant("tenant-a", "c", Some("sam"), Some(b"pw-for-a-7x"))
        .await
        .is_ok());
    assert!(state
        .auth
        .authenticate_in_tenant("tenant-b", "c", Some("sam"), Some(b"pw-for-b-9q"))
        .await
        .is_ok());
    use broker_auth::Authenticator as _;
    assert!(state
        .auth
        .authenticate("device-1", Some("base"), Some(b"base-pw-3z"))
        .await
        .is_ok());
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn data_export_rejects_unknown_and_malformed() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    for bad in [
        json!({"root_keys": [], "table_sets": ["no-such-table"]}),
        json!({"root_keys": ["no-such-root"], "table_sets": []}),
        json!("malformed"),
        json!([123]),
    ] {
        let (status, body) = server.post_auth("/api/v5/data/export", bad, &token).await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(body["message"].is_string());
        assert_eq!(body.as_object().expect("error shape").len(), 2);
    }

    // Broken JSON bytes are client errors with the same shape.
    // Send the body as an argument so braces need no escape.
    let raw = format!(
            "POST /api/v5/data/export HTTP/1.0\r\nHost: test\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: 7\r\nConnection: close\r\n\r\n{}",
            "{{oops!"
        );
    let (status, text) = server.request_raw(&raw).await;
    assert_eq!(status, 400);
    let body: Value = serde_json::from_str(&text).expect("error body is JSON");
    assert_eq!(body["code"], json!("BAD_REQUEST"));
}

#[tokio::test]
async fn data_export_answers_documented_fields() {
    // The export answers the documented file fields through the API.
    // Before: only `filename`. Now: `created_at`, `filename`,
    // `node` and the true file `size`.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, body) = server
        .post_auth(
            "/api/v5/data/export",
            json!({"root_keys": ["rules"], "table_sets": ["banned"]}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let filename = body["filename"].as_str().expect("export names its file");
    assert!(filename.ends_with(".tar.gz"));
    assert!(body["created_at"].is_string(), "export stamps its time");
    assert!(body["node"].is_string(), "export names its node");
    assert!(
        body["size"].as_u64().is_some_and(|size| size > 0),
        "export reports its true size"
    );
}

#[tokio::test]
async fn data_export_files_survive_restart() {
    // Export files persist across a restart: export, restart on the
    // same data dir, and the file is still there.
    let dir = unique_data_dir("data-export-restart");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let (status, body) = server
        .post_auth(
            "/api/v5/data/export",
            json!({"root_keys": ["rules"], "table_sets": ["banned"]}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let filename = body["filename"]
        .as_str()
        .expect("export names its file")
        .to_string();
    drop(server);
    drop(registry);

    assert!(dir.join("backup").join(&filename).is_file());
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;
    assert!(dir.join("backup").join(&filename).is_file());
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    let token = body["token"].as_str().expect("admin token").to_string();
    let (status, _) = server
        .post_auth(
            "/api/v5/data/export",
            json!({"root_keys": ["rules"], "table_sets": ["banned"]}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn data_export_enforces_bounds_through_api() {
    // Fill the backup dir past every bound. Export once through the
    // API. Observe that the store meets every bound.
    use std::time::{Duration, SystemTime};
    let dir = unique_data_dir("data-export-bounds");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let backup = dir.join("backup");
    std::fs::create_dir_all(&backup).expect("backup dir forms");
    let write_dummy = |name: &str, size: u64, age_secs: u64| {
        let path = backup.join(name);
        std::fs::write(&path, b"dummy").expect("dummy writes");
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("dummy opens");
        file.set_len(size).expect("size extends");
        let mtime = SystemTime::now()
            .checked_sub(Duration::from_secs(age_secs))
            .expect("age precedes now");
        file.set_modified(mtime).expect("mtime applies");
    };
    for index in 0..17 {
        let age = 3_600 - index as u64;
        write_dummy(&format!("backup-fill-{index:03}.tar.gz"), 8, age);
    }
    write_dummy("backup-aged-000.tar.gz", 8, 8 * 86_400);
    write_dummy("backup-aged-001.tar.gz", 8, 8 * 86_400);
    let big = 140 * 1024 * 1024;
    write_dummy("backup-big-000.tar.gz", big, 20);
    write_dummy("backup-big-001.tar.gz", big, 10);
    let (status, body) = server
        .post_auth(
            "/api/v5/data/export",
            json!({"root_keys": ["rules"], "table_sets": ["banned"]}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let filename = body["filename"]
        .as_str()
        .expect("export names file")
        .to_string();
    let mut names = Vec::new();
    let mut total: u64 = 0;
    for entry in std::fs::read_dir(&backup).expect("backup lists") {
        let entry = entry.expect("entry reads");
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.ends_with(".tar.gz") {
            continue;
        }
        total += entry.metadata().expect("size reads").len();
        names.push(name);
    }
    assert!(names.len() <= 16);
    assert!(total <= 256 * 1024 * 1024);
    assert!(!names.contains(&"backup-aged-000.tar.gz".to_string()));
    assert!(!names.contains(&"backup-aged-001.tar.gz".to_string()));
    assert!(names.contains(&filename));
    assert!(backup.join(&filename).is_file());
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn data_export_keeps_ban_store_usable() {
    // Create one ban through the API. Export it through the API.
    // Observe that the ban remains listed and exported.
    let dir = unique_data_dir("data-export-bans");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let (status, _) = server
        .post_auth(
            "/api/v5/banned",
            json!({"as": "clientid", "who": "w5-26-probe", "reason": "export check"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, body) = server
        .post_auth(
            "/api/v5/data/export",
            json!({"root_keys": ["rules"], "table_sets": ["banned"]}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let filename = body["filename"]
        .as_str()
        .expect("export names file")
        .to_string();
    let (status, body) = server.get_auth("/api/v5/banned", &token).await;
    assert_eq!(status, 200);
    let data = body["data"].as_array().expect("ban list reads");
    assert!(data
        .iter()
        .any(|entry| entry["who"] == json!("w5-26-probe")));
    let path = dir.join("backup").join(&filename);
    assert!(path.is_file());
    let parts = read_backup_file(&path);
    let banned = parts
        .iter()
        .find(|(name, _)| name.ends_with("/tables/banned.json"))
        .expect("banned table ships");
    assert!(banned.1.contains("w5-26-probe"));
    let (status, _) = server
        .post_auth(
            "/api/v5/data/export",
            json!({"root_keys": ["rules"], "table_sets": ["banned"]}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, body) = server.get_auth("/api/v5/banned", &token).await;
    assert_eq!(status, 200);
    let data = body["data"].as_array().expect("ban list reads");
    assert!(data
        .iter()
        .any(|entry| entry["who"] == json!("w5-26-probe")));
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn data_files_round_trip_through_api() {
    // Upload one file through the API. List it with paging data.
    // Upload it again for a conflict. Delete it. Delete it again
    // for a 404. An empty store reads as an empty page.
    use base64::Engine as _;
    let dir = unique_data_dir("data-files-roundtrip");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let (status, body) = server.get_auth("/api/v5/data/files", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["data"], json!([]));
    assert_eq!(body["meta"]["count"], json!(0));
    assert_eq!(body["meta"]["page"], json!(1));
    assert_eq!(body["meta"]["limit"], json!(100));
    let content = base64::engine::general_purpose::STANDARD.encode(b"w5-27 round-trip bytes");
    let (status, _) = server
        .post_auth(
            "/api/v5/data/files",
            json!({"filename": "w5-27-roundtrip.tar.gz", "content": content}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    assert!(dir.join("backup").join("w5-27-roundtrip.tar.gz").is_file());
    let (status, body) = server.get_auth("/api/v5/data/files", &token).await;
    assert_eq!(status, 200);
    let data = body["data"].as_array().expect("file list reads");
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["filename"], json!("w5-27-roundtrip.tar.gz"));
    assert!(data[0]["size"].as_u64().expect("size reads") > 0);
    assert!(data[0]["created_at"].as_str().is_some());
    assert!(data[0]["node"].as_str().is_some());
    assert_eq!(body["meta"]["count"], json!(1));
    let (status, body) = server
        .get_auth("/api/v5/data/files?page=1&limit=1", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["data"].as_array().expect("page reads").len(), 1);
    assert_eq!(body["meta"]["page"], json!(1));
    assert_eq!(body["meta"]["limit"], json!(1));
    let (status, body) = server
        .post_auth(
            "/api/v5/data/files",
            json!({"filename": "w5-27-roundtrip.tar.gz", "content": content}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("ALREADY_EXISTS"));
    let (status, _) = server
        .delete_auth("/api/v5/data/files/w5-27-roundtrip.tar.gz", &token)
        .await;
    assert_eq!(status, 204);
    assert!(!dir.join("backup").join("w5-27-roundtrip.tar.gz").is_file());
    let (status, body) = server.get_auth("/api/v5/data/files", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["data"], json!([]));
    let (status, body) = server
        .delete_auth("/api/v5/data/files/w5-27-roundtrip.tar.gz", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn data_files_reject_malformed_uploads() {
    // Bad names and bad bytes fail as client errors.
    use base64::Engine as _;
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let good = base64::engine::general_purpose::STANDARD.encode(b"w5-27 malformed probe");
    for bad in [
        json!({"filename": "", "content": good}),
        json!({"filename": "../escape.tar.gz", "content": good}),
        json!({"filename": "bad.txt", "content": good}),
        json!({"filename": "w5-27-bad.tar.gz", "content": ""}),
        json!({"filename": "w5-27-bad.tar.gz", "content": "!!!not-base64!!!"}),
        json!({"filename": "w5-27-bad.tar.gz"}),
        json!({"content": good}),
    ] {
        let (status, body) = server.post_auth("/api/v5/data/files", bad, &token).await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
    }
}

#[tokio::test]
async fn data_files_paging_honours_limit() {
    // Three uploads list as pages of two plus one.
    use base64::Engine as _;
    let dir = unique_data_dir("data-files-paging");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    for name in ["w5-27-p0.tar.gz", "w5-27-p1.tar.gz", "w5-27-p2.tar.gz"] {
        let content = base64::engine::general_purpose::STANDARD.encode(format!("bytes for {name}"));
        let (status, _) = server
            .post_auth(
                "/api/v5/data/files",
                json!({"filename": name, "content": content}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
    }
    let (status, body) = server
        .get_auth("/api/v5/data/files?page=1&limit=2", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["data"].as_array().expect("page reads").len(), 2);
    assert_eq!(body["meta"]["count"], json!(3));
    let (status, body) = server
        .get_auth("/api/v5/data/files?page=2&limit=2", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["data"].as_array().expect("page reads").len(), 1);
    assert_eq!(body["meta"]["count"], json!(3));
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn data_files_survive_restart() {
    // Upload one file. Start the broker again on the same data
    // dir. The file lists again.
    use base64::Engine as _;
    let dir = unique_data_dir("data-files-restart");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let content = base64::engine::general_purpose::STANDARD.encode(b"w5-27 restart bytes");
    let (status, _) = server
        .post_auth(
            "/api/v5/data/files",
            json!({"filename": "w5-27-restart.tar.gz", "content": content}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    drop(server);
    drop(registry);
    assert!(dir.join("backup").join("w5-27-restart.tar.gz").is_file());
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;
    // The admin password persisted, so log in with the changed password.
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    let token = body["token"].as_str().expect("admin token").to_string();
    let (status, body) = server.get_auth("/api/v5/data/files", &token).await;
    assert_eq!(status, 200);
    let data = body["data"].as_array().expect("file list reads");
    assert!(data
        .iter()
        .any(|entry| entry["filename"] == json!("w5-27-restart.tar.gz")));
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}
