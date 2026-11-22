use super::*;

#[tokio::test]
async fn backup_import_round_trip_through_broker() {
    // W5-29 broker-through: create users and rules, export, change
    // them, import, and read them again. Every step goes through the
    // served management API on `ApiState` (the same state the kernel
    // shares in `crates/broker-node/src/main.rs`, `serve_api`).
    // Management-plane only: the import takes short management locks
    // and never the publish or deliver path.
    let dir = unique_data_dir("data-import-roundtrip");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;

    // One MQTT user, one rule, one ban and one ACL entry.
    let (status, _) = server
        .post_auth(
            "/api/v5/authentication",
            json!({"mechanism": "password_based", "backend": "built_in_database"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = server
        .post_auth(
            "/api/v5/authentication/password_based:built_in_database/users",
            json!({"user_id": "rt-user", "password": "RtPass123!"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, created) = server
            .post_auth(
                "/api/v5/rules",
                json!({"name": "rt-rule", "sql": "SELECT * FROM \"rt/#\"", "enable": true, "actions": []}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    let rule_id = created["id"].as_str().expect("rule id").to_string();
    let (status, _) = server
        .post_auth(
            "/api/v5/banned",
            json!({"as": "clientid", "who": "rt-banned", "reason": "round trip"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = server
        .post_auth(
            "/api/v5/authorization/sources/built_in_database/rules/all",
            json!({"rules": [{"topic": "rt/#", "permission": "allow", "action": "publish"}]}),
            &token,
        )
        .await;
    assert_eq!(status, 204);

    // Export the live state through the API.
    let (status, body) = server
        .post_auth("/api/v5/data/export", json!({}), &token)
        .await;
    assert_eq!(status, 200);
    let filename = body["filename"]
        .as_str()
        .expect("export names file")
        .to_string();

    // Change everything the file carries in full: drop the rule, the
    // ban and the ACL; recreate the user with other quotas.
    let (status, _) = server
        .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/banned/clientid/rt-banned", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth(
            "/api/v5/authorization/sources/built_in_database/rules/all",
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth(
            "/api/v5/authentication/password_based:built_in_database/users/rt-user",
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
            .post_auth(
                "/api/v1/auth/users",
                json!({"username": "rt-user", "password": "RtPass123!", "quotas": {"max_connections": 3}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .get_auth(&format!("/api/v5/rules/{rule_id}"), &token)
        .await;
    assert_eq!(status, 404);

    // Import the exported file through the API.
    let (status, _) = server
        .post_auth(
            "/api/v5/data/import",
            json!({"filename": filename.as_str()}),
            &token,
        )
        .await;
    assert_eq!(status, 204);

    // The rule is back with its SQL; the ban is back; the ACL is back.
    let (status, fetched) = server
        .get_auth(&format!("/api/v5/rules/{rule_id}"), &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(fetched["sql"], json!("SELECT * FROM \"rt/#\""));
    let (status, body) = server.get_auth("/api/v5/banned", &token).await;
    assert_eq!(status, 200);
    let bans = body["data"].as_array().expect("ban list reads");
    assert!(bans.iter().any(|entry| entry["who"] == json!("rt-banned")));
    let (status, body) = server
        .get_auth(
            "/api/v5/authorization/sources/built_in_database/rules/all",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let acls = body["rules"].as_array().expect("acl list reads");
    assert_eq!(acls.len(), 1);
    assert_eq!(acls[0]["topic"], json!("rt/#"));
    // The user is back with the exported quotas (the file carries no
    // password, so the live verifier stays).
    let (status, body) = server.get_auth("/api/v1/auth/users", &token).await;
    assert_eq!(status, 200);
    let users = body.as_array().expect("user list reads");
    let user = users
        .iter()
        .find(|entry| entry["username"] == json!("rt-user"))
        .expect("user restored");
    assert!(user["quotas"]["max_connections"].is_null());

    // The restore persists: a registry reload from the same data dir
    // still holds the rule, the user and the ACL.
    drop(server);
    drop(registry);
    let reloaded = ConfigRegistry::load(&dir).expect("reload data dir");
    let snapshot = reloaded.snapshot();
    assert!(snapshot.rules.rules.iter().any(|rule| rule.id == rule_id));
    assert!(snapshot
        .mqtt_users
        .users
        .iter()
        .any(|user| user.username == "rt-user"));
    assert_eq!(snapshot.mqtt_users.acls.len(), 1);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn backup_import_unknown_file_is_404() {
    // An unknown filename answers 404 with the documented shape.
    // Broker-through: the request goes through the served API.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, body) = server
        .post_auth(
            "/api/v5/data/import",
            json!({"filename": "missing-file.tar.gz"}),
            &token,
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    assert!(body["message"].as_str().is_some_and(|m| !m.is_empty()));
    assert_eq!(body.as_object().expect("error shape").len(), 2);

    // A missing filename is a client error with the same shape.
    let (status, body) = server
        .post_auth("/api/v5/data/import", json!({}), &token)
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
}

#[tokio::test]
async fn backup_import_corrupt_file_keeps_prior_state() {
    // A corrupt file is refused whole: 400 plus the state before the
    // import stays. Broker-through: upload and import go through the
    // served API; the rule read proves the state is intact.
    use base64::Engine as _;
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, created) = server
        .post_auth(
            "/api/v5/rules",
            json!({"name": "keep-me", "sql": "SELECT * FROM \"keep/#\"", "actions": []}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let rule_id = created["id"].as_str().expect("rule id").to_string();
    let garbage = base64::engine::general_purpose::STANDARD.encode(b"not a gzip file");
    let (status, _) = server
        .post_auth(
            "/api/v5/data/files",
            json!({"filename": "conf-corrupt.tar.gz", "content": garbage}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .post_auth(
            "/api/v5/data/import",
            json!({"filename": "conf-corrupt.tar.gz"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    assert!(body["message"].as_str().is_some_and(|m| !m.is_empty()));
    assert_eq!(body.as_object().expect("error shape").len(), 2);
    let (status, fetched) = server
        .get_auth(&format!("/api/v5/rules/{rule_id}"), &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(fetched["sql"], json!("SELECT * FROM \"keep/#\""));
    let (status, _) = server
        .delete_auth("/api/v5/data/files/conf-corrupt.tar.gz", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn backup_import_without_admin_is_refused() {
    // An import without any administrator is refused and changes
    // nothing. Broker-through: the file is built in the test, stored
    // through the served upload API and imported through the API.
    use base64::Engine as _;
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, _) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "v-user", "password": "V-us3r-pass!", "role": "viewer"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    // One admin-less file: only the viewer, no administrator.
    let file = build_test_backup(
        "noadmin-0",
        &["admin_users"],
        &[(
            "snapshot/admin_users.json",
            json!({"users": [{"username": "v-user", "role": "viewer", "description": "", "must_change_password": false}]}),
        )],
    );
    let content = base64::engine::general_purpose::STANDARD.encode(&file);
    let (status, _) = server
        .post_auth(
            "/api/v5/data/files",
            json!({"filename": "conf-noadmin.tar.gz", "content": content}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .post_auth(
            "/api/v5/data/import",
            json!({"filename": "conf-noadmin.tar.gz"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("VALIDATION_FAILED"));
    assert!(body["message"].as_str().is_some_and(|m| !m.is_empty()));
    assert_eq!(body.as_object().expect("error shape").len(), 2);
    // Both users are still there, including the administrator.
    let (status, body) = server.get_auth("/api/v5/users", &token).await;
    assert_eq!(status, 200);
    let users = body.as_array().expect("user list reads");
    assert!(users
        .iter()
        .any(|entry| entry["username"] == json!("admin")));
    assert!(users
        .iter()
        .any(|entry| entry["username"] == json!("v-user")));
    let (status, _) = server
        .delete_auth("/api/v5/data/files/conf-noadmin.tar.gz", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn backup_import_rights_change_needs_full_administrator() {
    // SR-01 item 2: a system-scoped administrator cannot raise its
    // own rights through an import. The same archive applies for a
    // full administrator. Broker-through: user create, login, upload
    // and import all use the served API.
    use base64::Engine as _;
    let dir = unique_data_dir("sr01-import-rights");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(registry).await;
    let admin = server.login_as_admin().await;
    let (status, body) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "sr01-scoped", "password": "Scoped-pass-1a",
                       "role": "administrator", "scopes": ["system"]}),
            &admin,
        )
        .await;
    assert_eq!(status, 200, "scoped user create: {body}");
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "sr01-scoped", "password": "Scoped-pass-1a"}),
        )
        .await;
    assert_eq!(status, 200, "scoped login: {body}");
    let scoped = body["token"].as_str().expect("scoped token").to_string();

    let file = build_test_backup(
        "sr01-rights-0",
        &["admin_users"],
        &[(
            "snapshot/admin_users.json",
            json!({"users": [
                {"username": "admin", "role": "administrator", "description": "",
                 "must_change_password": false, "scopes": null, "scopes_set": false},
                {"username": "sr01-scoped", "role": "administrator", "description": "",
                 "must_change_password": false, "scopes": null, "scopes_set": false},
            ]}),
        )],
    );
    let content = base64::engine::general_purpose::STANDARD.encode(&file);
    let (status, _) = server
        .post_auth(
            "/api/v5/data/files",
            json!({"filename": "conf-sr01-rights.tar.gz", "content": content}),
            &admin,
        )
        .await;
    assert_eq!(status, 204);

    // The scoped administrator may not widen its own row.
    let (status, body) = server
        .post_auth(
            "/api/v5/data/import",
            json!({"filename": "conf-sr01-rights.tar.gz"}),
            &scoped,
        )
        .await;
    assert_eq!(status, 403, "scoped import must be refused: {body}");
    assert_eq!(body["code"], json!("FORBIDDEN"));
    assert_eq!(body.as_object().expect("error shape").len(), 2);
    let (status, users) = server.get_auth("/api/v5/users", &admin).await;
    assert_eq!(status, 200);
    let row = users
        .as_array()
        .expect("user list")
        .iter()
        .find(|entry| entry["username"] == json!("sr01-scoped"))
        .expect("scoped user stays");
    assert_eq!(row["scopes"], json!(["system"]), "the old scopes stay");

    // A full administrator applies the same archive.
    let (status, body) = server
        .post_auth(
            "/api/v5/data/import",
            json!({"filename": "conf-sr01-rights.tar.gz"}),
            &admin,
        )
        .await;
    assert_eq!(status, 204, "full administrator import: {body}");
    let (status, users) = server.get_auth("/api/v5/users", &admin).await;
    assert_eq!(status, 200);
    let row = users
        .as_array()
        .expect("user list")
        .iter()
        .find(|entry| entry["username"] == json!("sr01-scoped"))
        .expect("scoped user stays");
    assert_eq!(
        row["scopes"],
        json!("unset"),
        "the full administrator import applies the new default"
    );
    let (status, _) = server
        .delete_auth("/api/v5/data/files/conf-sr01-rights.tar.gz", &admin)
        .await;
    assert_eq!(status, 204);
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn backup_export_and_old_import_keep_dashboard_scopes() {
    // SR-01 item 3: the export writes the explicit scopes and the
    // flag, so a round trip keeps a restricted administrator
    // restricted. An archive from an older release carries no scope
    // field and keeps the stored scopes. Broker-through: every step
    // uses the served API or the export file it wrote.
    let dir = unique_data_dir("sr01-export-scopes");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(registry).await;
    let admin = server.login_as_admin().await;
    let (status, body) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "sr01-restricted", "password": "Restricted-pass-1a",
                       "role": "administrator", "scopes": ["system"]}),
            &admin,
        )
        .await;
    assert_eq!(status, 200, "restricted user create: {body}");

    let (status, body) = server
        .post_auth("/api/v5/data/export", json!({}), &admin)
        .await;
    assert_eq!(status, 200);
    let filename = body["filename"].as_str().expect("export names file");
    let path = dir.join("backup").join(filename);
    let parts = read_backup_file(&path);
    let admin_part = parts
        .iter()
        .find(|(name, _)| name.ends_with("/snapshot/admin_users.json"))
        .expect("admin users part ships");
    let exported: Value = serde_json::from_str(&admin_part.1).expect("admin part is JSON");
    let row = exported["users"]
        .as_array()
        .expect("users array")
        .iter()
        .find(|entry| entry["username"] == json!("sr01-restricted"))
        .expect("restricted user ships");
    assert_eq!(row["scopes"], json!(["system"]), "the export keeps scopes");
    assert_eq!(row["scopes_set"], json!(true), "the export names the list");

    // Import the unchanged archive: the restriction stays.
    let (status, body) = server
        .post_auth("/api/v5/data/import", json!({"filename": filename}), &admin)
        .await;
    assert_eq!(status, 204, "round-trip import: {body}");
    let (status, users) = server.get_auth("/api/v5/users", &admin).await;
    assert_eq!(status, 200);
    let row = users
        .as_array()
        .expect("user list")
        .iter()
        .find(|entry| entry["username"] == json!("sr01-restricted"))
        .expect("restricted user stays");
    assert_eq!(
        row["scopes"],
        json!(["system"]),
        "the round trip keeps the restriction"
    );

    // An archive with no scope field keeps the stored scopes.
    let old = build_test_backup(
        "sr01-old-scopes-0",
        &["admin_users"],
        &[(
            "snapshot/admin_users.json",
            json!({"users": [
                {"username": "admin", "role": "administrator", "description": "",
                 "must_change_password": false},
                {"username": "sr01-restricted", "role": "administrator", "description": "",
                 "must_change_password": false},
            ]}),
        )],
    );
    use base64::Engine as _;
    let content = base64::engine::general_purpose::STANDARD.encode(&old);
    let (status, _) = server
        .post_auth(
            "/api/v5/data/files",
            json!({"filename": "conf-sr01-old.tar.gz", "content": content}),
            &admin,
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .post_auth(
            "/api/v5/data/import",
            json!({"filename": "conf-sr01-old.tar.gz"}),
            &admin,
        )
        .await;
    assert_eq!(status, 204, "old archive import: {body}");
    let (status, users) = server.get_auth("/api/v5/users", &admin).await;
    assert_eq!(status, 200);
    let row = users
        .as_array()
        .expect("user list")
        .iter()
        .find(|entry| entry["username"] == json!("sr01-restricted"))
        .expect("restricted user stays");
    assert_eq!(
        row["scopes"],
        json!(["system"]),
        "an old archive never widens stored scopes"
    );
    let (status, _) = server
        .delete_auth("/api/v5/data/files/conf-sr01-old.tar.gz", &admin)
        .await;
    assert_eq!(status, 204);
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn backup_import_unknown_root_is_refused_through_broker() {
    // An unknown root is refused whole through the API.
    // Broker-through: upload and import go through the served API.
    // The prior rule stays, so validation precedes any apply.
    use base64::Engine as _;
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, created) = server
            .post_auth(
                "/api/v5/rules",
                json!({"name": "keep-unknown", "sql": "SELECT * FROM \"keep/unknown/#\"", "actions": []}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    let rule_id = created["id"].as_str().expect("rule id").to_string();
    let file = build_test_backup(
        "unknown-root-0",
        &["no-such-root"],
        &[("snapshot/no-such-root.json", json!({}))],
    );
    let content = base64::engine::general_purpose::STANDARD.encode(&file);
    let (status, _) = server
        .post_auth(
            "/api/v5/data/files",
            json!({"filename": "conf-unknown-root.tar.gz", "content": content}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .post_auth(
            "/api/v5/data/import",
            json!({"filename": "conf-unknown-root.tar.gz"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert!(body["message"].as_str().is_some_and(|m| !m.is_empty()));
    assert_eq!(body.as_object().expect("error shape").len(), 2);
    let (status, fetched) = server
        .get_auth(&format!("/api/v5/rules/{rule_id}"), &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(fetched["sql"], json!("SELECT * FROM \"keep/unknown/#\""));
    let (status, _) = server
        .delete_auth("/api/v5/data/files/conf-unknown-root.tar.gz", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn backup_import_missing_part_is_refused_through_broker() {
    // A listed part without bytes is refused whole through the API.
    // Broker-through: upload and import go through the served API.
    // The prior rule stays, so validation precedes any apply.
    use base64::Engine as _;
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, created) = server
            .post_auth(
                "/api/v5/rules",
                json!({"name": "keep-missing", "sql": "SELECT * FROM \"keep/missing/#\"", "actions": []}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    let rule_id = created["id"].as_str().expect("rule id").to_string();
    let file = build_test_backup("missing-part-0", &["rules"], &[]);
    let content = base64::engine::general_purpose::STANDARD.encode(&file);
    let (status, _) = server
        .post_auth(
            "/api/v5/data/files",
            json!({"filename": "conf-missing-part.tar.gz", "content": content}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .post_auth(
            "/api/v5/data/import",
            json!({"filename": "conf-missing-part.tar.gz"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert!(body["message"].as_str().is_some_and(|m| !m.is_empty()));
    assert_eq!(body.as_object().expect("error shape").len(), 2);
    let (status, fetched) = server
        .get_auth(&format!("/api/v5/rules/{rule_id}"), &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(fetched["sql"], json!("SELECT * FROM \"keep/missing/#\""));
    let (status, _) = server
        .delete_auth("/api/v5/data/files/conf-missing-part.tar.gz", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn backup_import_not_ready_is_refused() {
    // The import refuses while the node is not ready.
    // The handler is the production entry for the route.
    // A ready node passes the gate and reads the backup dir.
    let engine = std::sync::Arc::new(broker_rules::RuleEngine::new(
        16,
        broker_rules::BackpressurePolicy::DropOldest,
    ));
    let state = ApiState::standalone(engine);
    state.readiness.mark_not_ready();
    assert!(!state.readiness.is_ready());
    let body = bytes::Bytes::from(
        serde_json::to_vec(&json!({"filename": "any.tar.gz"})).expect("body is JSON"),
    );
    let response =
        crate::v5::backup::import_data(axum::extract::State(state.clone()), None, body).await;
    assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
    let raw = axum::body::to_bytes(response.into_body(), 16 * 1024)
        .await
        .expect("error body reads");
    let parsed: Value = serde_json::from_slice(&raw).expect("error body is JSON");
    assert_eq!(parsed["code"], json!("STORE_NOT_READY"));
    assert!(parsed["message"].as_str().is_some_and(|m| !m.is_empty()));
    assert_eq!(parsed.as_object().expect("error shape").len(), 2);
    state.readiness.mark_ready();
    assert!(state.readiness.is_ready());
    let body = bytes::Bytes::from(
        serde_json::to_vec(&json!({"filename": "missing-file.tar.gz"})).expect("body is JSON"),
    );
    let response = crate::v5::backup::import_data(axum::extract::State(state), None, body).await;
    assert_eq!(response.status(), axum::http::StatusCode::NOT_FOUND);
    let raw = axum::body::to_bytes(response.into_body(), 16 * 1024)
        .await
        .expect("error body reads");
    let parsed: Value = serde_json::from_slice(&raw).expect("error body is JSON");
    assert_eq!(parsed["code"], json!("NOT_FOUND"));
}

#[tokio::test]
async fn backup_import_retained_round_trip_through_broker() {
    // A retained table restores through the API and replaces live rows.
    // Broker-through: publish, upload and import go through the API.
    // The retained read proves the restore; the old row is gone.
    use base64::Engine as _;
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, _) = server
        .post_auth(
            "/api/v5/publish",
            json!({"topic": "rt-import-old", "payload": "old", "qos": 0, "retain": true}),
            &token,
        )
        .await;
    assert_eq!(status, 202);
    let payload_new = base64::engine::general_purpose::STANDARD.encode(b"new-retained");
    let file = build_test_backup_with_tables(
        "retained-rt-0",
        &[],
        &["retained"],
        &[(
            "tables/retained.json",
            json!([{"topic": "rt-import-new", "qos": 1, "payload": payload_new}]),
        )],
    );
    let content = base64::engine::general_purpose::STANDARD.encode(&file);
    let (status, _) = server
        .post_auth(
            "/api/v5/data/files",
            json!({"filename": "conf-retained-rt.tar.gz", "content": content}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .post_auth(
            "/api/v5/data/import",
            json!({"filename": "conf-retained-rt.tar.gz"}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, fetched) = server
        .get_auth("/api/v5/mqtt/retainer/message/rt-import-new", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(fetched["topic"], json!("rt-import-new"));
    assert_eq!(fetched["qos"], json!(1));
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(fetched["payload"].as_str().expect("payload is string"))
        .expect("payload is base64");
    assert_eq!(decoded, b"new-retained");
    let (status, _) = server
        .get_auth("/api/v5/mqtt/retainer/message/rt-import-old", &token)
        .await;
    assert_eq!(status, 404);
    let (status, _) = server
        .delete_auth("/api/v5/data/files/conf-retained-rt.tar.gz", &token)
        .await;
    assert_eq!(status, 204);
}
