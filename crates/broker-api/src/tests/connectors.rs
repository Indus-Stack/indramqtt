use super::*;

#[tokio::test]
async fn mv02b_tenant_gate_keeps_connector_forwards_in_default_tenant() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv02b-tenant");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02bFake::start(204, "").await;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{}", fake.port);

    // A second namespace owns the tenant-B rules below.
    let (status, _) = server
        .post_auth("/api/v5/mt/ns/mv02b-tenant", json!({}), &token)
        .await;
    assert_eq!(status, 204);

    // One connector and action per kind. Two kinds ride Cargo
    // features: without the feature the create refuses closed,
    // so the tenant gate is proved for them in the full build.
    let closed = "http://127.0.0.1:9".to_string();
    let bodies: Vec<(&str, serde_json::Value)> = vec![
        (
            "clickhouse",
            json!({"url": closed, "database": "db", "table": "events"}),
        ),
        (
            "tdengine",
            json!({"server": "127.0.0.1:9", "database": "db", "stable": "s",
                                "subtable": "d", "metrics": {"f": "x"}, "tags": {"t": "y"}}),
        ),
        (
            "greptimedb",
            json!({"server": "127.0.0.1:4001", "database": "db", "table": "events"}),
        ),
        (
            "iotdb",
            json!({"endpoint": closed, "username": "u", "password": "p",
                             "device": "root.d", "measurements": ["m"], "types": ["float"]}),
        ),
        (
            "opentsdb",
            json!({"server": closed, "metric": "m", "value": "v"}),
        ),
        (
            "doris",
            json!({"server": "127.0.0.1:9", "database": "db", "table": "events"}),
        ),
        (
            "datalayers",
            json!({"server": "127.0.0.1:8361", "database": "db", "table": "events"}),
        ),
        (
            "influxdb",
            json!({"server": endpoint, "bucket": "b", "org": "o",
                                "token": "t", "measurement": "m"}),
        ),
        (
            "timestream",
            json!({"database": "db", "table": "m", "region": "r",
                                 "access_key_id": "k", "secret_access_key": "s",
                                 "measures": {"f": "DOUBLE"}, "endpoint": closed}),
        ),
        (
            "cassandra",
            json!({"servers": "127.0.0.1:9", "keyspace": "ks", "table": "events"}),
        ),
    ];
    for (kind, mut connector) in bodies {
        // Cassandra is pending (SB-27). It never creates.
        if kind == "cassandra" {
            continue;
        }
        if kind == "clickhouse" && !cfg!(feature = "clickhouse") {
            continue;
        }
        let conn = format!("mv02b-ten-{kind}");
        let act = format!("mv02b-ten-{kind}-act");
        let rule = format!("mv02b-ten-{kind}-rule");
        connector["name"] = json!(conn);
        connector["type"] = json!(kind);
        let (status, created) = server
            .post_auth("/api/v5/connectors", connector, &token)
            .await;
        assert_eq!(status, 201, "{kind} connector creates: {created}");
        let (status, created) = server
            .post_auth(
                "/api/v5/actions",
                json!({"type": kind, "name": act, "connector": conn, "parameters": {}}),
                &token,
            )
            .await;
        assert_eq!(status, 201, "{kind} action creates: {created}");
        // A rule outside the default tenant must not forward to
        // the connector action: the path carries no tenant label.
        let (status, body) = server
            .post_auth(
                "/api/v5/rules",
                json!({"id": rule, "name": rule, "sql": "SELECT * FROM \"mv02b/ten\"",
                           "tenant": "mv02b-tenant", "actions": [act]}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "{kind} tenant forward refuses: {body}");
        assert!(
            body["message"].as_str().unwrap_or("").contains("tenant"),
            "{kind} names tenant: {body}"
        );
        // Nothing is stored for the refused rule.
        let (status, _) = server
            .get_auth(&format!("/api/v5/rules/{rule}"), &token)
            .await;
        assert_eq!(status, 404, "{kind} refused rule stores nothing");
    }

    // The default tenant keeps working: the rule creates, the
    // publish delivers through the broker to the fake.
    let (status, _) = server
            .post_auth(
                "/api/v5/rules",
                json!({"id": "mv02b-ten-rule", "name": "mv02b-ten-rule",
                       "sql": "SELECT * FROM \"mv02b/ten\"", "actions": ["mv02b-ten-influxdb-act"]}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    mv02b_publish(&server, &token, "mv02b/ten", "{\"temp\": 21.5}").await;
    let got = fake.wait_for(1).await;
    assert!(
        Mv02bFake::body_text(&got[0]).starts_with("m,topic=mv02b/ten "),
        "line: {}",
        Mv02bFake::body_text(&got[0])
    );
    mv02b_wait_success(&server, &token, "mv02b-ten-influxdb-act", 1).await;

    // A move outside the default tenant refuses and keeps the old rule.
    let (status, body) = server
        .put_auth(
            "/api/v5/rules/mv02b-ten-rule",
            json!({"id": "mv02b-ten-rule", "name": "mv02b-ten-rule",
                       "sql": "SELECT * FROM \"mv02b/ten\"", "tenant": "mv02b-tenant",
                       "actions": ["mv02b-ten-influxdb-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "move outside default refuses: {body}");
    let (status, fetched) = server
        .get_auth("/api/v5/rules/mv02b-ten-rule", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(fetched["tenant"], json!("default"));
    mv02b_publish(&server, &token, "mv02b/ten", "{\"temp\": 22.5}").await;
    let got = fake.wait_for(2).await;
    assert!(
        Mv02bFake::body_text(&got[1]).starts_with("m,topic=mv02b/ten "),
        "line: {}",
        Mv02bFake::body_text(&got[1])
    );
    mv02b_wait_success(&server, &token, "mv02b-ten-influxdb-act", 2).await;
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv02b_tenant_gate_covers_sql_into_connector_and_update() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, _) = server
        .post_auth("/api/v5/mt/ns/mv02b-tenant", json!({}), &token)
        .await;
    assert_eq!(status, 204);

    let sql = "SELECT * FROM \"mv02b/sql\" INTO connector(\"mv02b-shared\")";
    let (status, body) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02b-sql-nondefault", "name": "mv02b-sql-nondefault",
                       "sql": sql, "tenant": "mv02b-tenant", "actions": []}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "SQL forward outside default refuses: {body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("tenant"),
        "{body}"
    );
    let (status, _) = server
        .get_auth("/api/v5/rules/mv02b-sql-nondefault", &token)
        .await;
    assert_eq!(status, 404, "the refused SQL rule stores nothing");

    // The default tenant keeps the path: create, then move it.
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02b-sql-default", "name": "mv02b-sql-default",
                       "sql": "SELECT * FROM \"mv02b/sql\"", "actions": []}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, body) = server
        .put_auth(
            "/api/v5/rules/mv02b-sql-default",
            json!({"id": "mv02b-sql-default", "name": "mv02b-sql-default",
                       "sql": sql, "tenant": "mv02b-tenant", "actions": []}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "SQL move outside default refuses: {body}");
    let (status, fetched) = server
        .get_auth("/api/v5/rules/mv02b-sql-default", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(fetched["tenant"], json!("default"));
    assert_eq!(fetched["sql"], json!("SELECT * FROM \"mv02b/sql\""));
}

#[tokio::test]
async fn mv02b_tenant_gate_covers_v1_create() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, _) = server
        .post_auth("/api/v5/mt/ns/mv02b-tenant", json!({}), &token)
        .await;
    assert_eq!(status, 204);

    let forward = json!({"name": "mv02b-v1-nondefault",
            "topic_filter": "mv02b/v1",
            "sql_query": "SELECT * FROM \"mv02b/v1\"",
            "actions": [{"type": "forwardconnector", "connector_id": "mv02b-shared"}],
            "tenant": "mv02b-tenant"});
    let (status, body) = server
        .post_auth("/api/v1/rules", forward.clone(), &token)
        .await;
    assert_eq!(status, 400, "v1 forward outside default refuses: {body}");
    assert!(
        body["error"].as_str().unwrap_or("").contains("tenant"),
        "{body}"
    );
    let (status, list) = server.get_auth("/api/v5/rules", &token).await;
    assert_eq!(status, 200);
    assert!(
        !list["data"]
            .as_array()
            .expect("rule list")
            .iter()
            .any(|rule| rule["name"] == json!("mv02b-v1-nondefault")),
        "the refused v1 rule stores nothing"
    );

    let mut allowed = forward;
    allowed["tenant"] = json!("default");
    let (status, created) = server.post_auth("/api/v1/rules", allowed, &token).await;
    assert_eq!(status, 201, "the default tenant keeps the v1 path");
    let id = created["id"].as_str().expect("v1 rule id");
    let (status, _) = server
        .delete_auth(&format!("/api/v5/rules/{id}"), &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn mv02b_tenant_gate_covers_config_reload() {
    let dir = unique_data_dir("mv02b-tenant-reload");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let (status, _) = server
        .post_auth("/api/v5/mt/ns/mv02b-tenant", json!({}), &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02b-reload-keep", "name": "mv02b-reload-keep",
                       "sql": "SELECT * FROM \"mv02b/reload\"", "actions": []}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    let mut candidate = state.config.snapshot().as_ref().clone();
    candidate.rules.rules.push(broker_config::RuleEntry {
        id: "mv02b-reload-bad".to_string(),
        name: "mv02b-reload-bad".to_string(),
        description: String::new(),
        metadata: serde_json::json!({}),
        tenant: "mv02b-tenant".to_string(),
        topic_filter: "mv02b/reload".to_string(),
        sql_query: Some(
            "SELECT * FROM \"mv02b/reload\" INTO connector(\"mv02b-shared\")".to_string(),
        ),
        enabled: true,
        actions: Vec::new(),
    });
    let (status, body) = server
        .post_auth(
            "/api/v1/config/reload",
            json!({"snapshot": candidate, "actor": "sb07", "summary": "tenant gate"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "reload with a tenant forward refuses: {body}");
    assert!(
        body["error"].as_str().unwrap_or("").contains("tenant"),
        "{body}"
    );

    // The refused reload keeps the live rule and stores no bad one.
    let (status, _) = server
        .get_auth("/api/v5/rules/mv02b-reload-keep", &token)
        .await;
    assert_eq!(status, 200, "the refused reload keeps the live rules");
    let (status, _) = server
        .get_auth("/api/v5/rules/mv02b-reload-bad", &token)
        .await;
    assert_eq!(status, 404, "the refused reload stores nothing");
    drop(server);
    drop(registry);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv02b_tenant_gate_covers_backup_restore() {
    use base64::Engine as _;
    let dir = unique_data_dir("mv02b-tenant-restore");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let (status, _) = server
        .post_auth("/api/v5/mt/ns/mv02b-tenant", json!({}), &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02b-restore-keep", "name": "mv02b-restore-keep",
                       "sql": "SELECT * FROM \"mv02b/restore\"", "actions": []}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    let file = build_test_backup(
        "mv02b-tenant-restore",
        &["rules"],
        &[(
            "snapshot/rules.json",
            json!({"rules": [{
                "id": "mv02b-restore-bad",
                "name": "mv02b-restore-bad",
                "tenant": "mv02b-tenant",
                "topic_filter": "mv02b/restore",
                "sql_query": "SELECT * FROM \"mv02b/restore\"",
                "enabled": true,
                "actions": [{"type": "forwardconnector", "connector_id": "mv02b-shared"}]
            }]}),
        )],
    );
    let content = base64::engine::general_purpose::STANDARD.encode(&file);
    let (status, _) = server
        .post_auth(
            "/api/v5/data/files",
            json!({"filename": "mv02b-tenant-restore.tar.gz", "content": content}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .post_auth(
            "/api/v5/data/import",
            json!({"filename": "mv02b-tenant-restore.tar.gz"}),
            &token,
        )
        .await;
    assert_eq!(status, 500, "restore with a tenant forward refuses: {body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("tenant"),
        "{body}"
    );

    // The refused restore keeps the live rule and stores no bad one.
    let (status, _) = server
        .get_auth("/api/v5/rules/mv02b-restore-keep", &token)
        .await;
    assert_eq!(status, 200, "the refused restore keeps the live rules");
    let (status, _) = server
        .get_auth("/api/v5/rules/mv02b-restore-bad", &token)
        .await;
    assert_eq!(status, 404, "the refused restore stores nothing");
    drop(server);
    drop(registry);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn every_connector_kind_masks_secrets_on_read_and_keeps_them_on_marker_put() {
    #[cfg(test)]
    const SECRET: &str = "known-secret-value-7f3a91";
    let secret_fields = [
        "password",
        "token",
        "secret_key",
        "secret_access_key",
        "access_key_secret",
        "session_token",
        "private_key",
        "service_account_json",
    ];
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let mut created = Vec::new();
    let mut unreachable_kinds = Vec::new();
    for row in broker_config::capability::KIND_ROWS {
        if !row.connector || !broker_config::capability::row_is_ready(row) {
            continue;
        }
        let name = format!("mask-{}", row.name.replace('_', "-"));
        // Start with every secret field. Follow the refusals: drop a
        // field the kind refuses, add a field the kind requires.
        let mut body = json!({"type": row.name, "name": name});
        for field in secret_fields {
            body[field] = json!(SECRET);
        }
        // Required fields that a generic body cannot guess.
        let extra = match row.name {
            "bigquery" => {
                json!({"authentication": {"private_key": SECRET, "client_email": "a@b.c"}})
            }
            "gcp_pubsub" => json!({"service_account_json": null}),
            "datalayers" => {
                json!({"username": "u", "database": "d", "table": "t", "token": null})
            }
            "disk_log" => {
                json!({"filepath": "mask/x.log", "max_file_size": "1mb", "max_file_number": 2})
            }
            "greptimedb" => json!({"username": "u", "database": "d", "table": "t"}),
            "iotdb" => {
                json!({"username": "u", "measurements": ["m"], "types": ["double"], "device": "root.d"})
            }
            "sparkplug_b" => json!({"server": "mqtt://127.0.0.1:1"}),
            "tdengine" => {
                json!({"database": "d", "password": null, "metrics": {"v": "DOUBLE"}, "tags": {"t": "x"}})
            }
            "timestream" => json!({"measures": {"v": "DOUBLE"}}),
            _ => json!({}),
        };
        for (key, value) in extra.as_object().unwrap() {
            if value.is_null() {
                body.as_object_mut().unwrap().remove(key);
            } else {
                body[key] = value.clone();
            }
        }
        let mut ok = false;
        let mut last_message = String::new();
        for _ in 0..24 {
            let (status, answer) = server
                .post_auth("/api/v5/connectors", body.clone(), &token)
                .await;
            if status == 201 {
                ok = true;
                break;
            }
            let message = answer["message"].as_str().unwrap_or("").to_string();
            last_message = message.clone();
            let named = message
                .split("field `")
                .nth(1)
                .and_then(|rest| rest.split('`').next())
                .map(str::to_string)
                .or_else(|| {
                    message
                        .split("missing required field: ")
                        .nth(1)
                        .and_then(|rest| rest.split_whitespace().next())
                        .map(str::to_string)
                });
            let Some(field) = named else { break };
            if secret_fields.contains(&field.as_str()) && body.get(&field).is_some() {
                body.as_object_mut().unwrap().remove(&field);
            } else if body.get(&field).is_none() {
                let value = if field.contains("url") || field.contains("endpoint") {
                    "http://127.0.0.1:1"
                } else if field.contains("server") || field.contains("host") {
                    "127.0.0.1:1"
                } else {
                    "value"
                };
                body[field] = json!(value);
            } else {
                break;
            }
        }
        if ok {
            created.push((name, body));
        } else {
            unreachable_kinds.push((row.name, last_message));
        }
    }
    assert!(
        created.len() >= 20,
        "too few kinds could be created: {} (not created: {unreachable_kinds:?})",
        created.len()
    );
    let (status, listed) = server.get_auth("/api/v5/connectors", &token).await;
    assert_eq!(status, 200);
    assert!(
        !listed.to_string().contains(SECRET),
        "the list shows a secret"
    );
    let mut with_secret = 0;
    for (name, body) in &created {
        let path = format!("/api/v5/connectors/{name}");
        let (status, shown) = server.get_auth(&path, &token).await;
        assert_eq!(status, 200, "{name}");
        assert!(!shown.to_string().contains(SECRET), "{name} shows a secret");
        let stored_before = state
            .config
            .snapshot()
            .connectors
            .connectors
            .iter()
            .find(|entry| entry.id == *name)
            .map(|entry| serde_json::to_string(entry).unwrap())
            .expect("stored connector");
        let has_secret = stored_before.contains(SECRET);
        if has_secret {
            with_secret += 1;
        }
        // Send the read answer back: the mask keeps the stored secret.
        let mut put = shown.clone();
        for key in ["status", "node_status", "status_reason"] {
            put.as_object_mut().unwrap().remove(key);
        }
        if has_secret {
            assert!(
                put.to_string().contains("******"),
                "{name} shows no mask: {put}"
            );
        }
        let (status, answer) = server.put_auth(&path, put, &token).await;
        assert_eq!(status, 200, "{name}: {answer}");
        assert!(!answer.to_string().contains(SECRET), "{name} answer");
        let stored_after = state
            .config
            .snapshot()
            .connectors
            .connectors
            .iter()
            .find(|entry| entry.id == *name)
            .map(|entry| serde_json::to_string(entry).unwrap())
            .expect("stored connector");
        assert_eq!(
            stored_after.contains(SECRET),
            has_secret,
            "{name} lost its secret on the marker PUT"
        );
        assert!(!body.is_null());
    }
    assert!(with_secret >= 15, "only {with_secret} kinds kept a secret");
    // Only a kind that this build does not compile in may be missing.
    for (kind, message) in &unreachable_kinds {
        assert!(
            message.contains("cargo feature"),
            "{kind} could not be created: {message}"
        );
    }
    let (_, snapshot) = server.get_auth("/api/v5/configs", &token).await;
    assert!(
        !snapshot.to_string().contains(SECRET),
        "configs show a secret"
    );
    println!(
        "masked kinds: {} created, {} with a stored secret, not created: {:?}",
        created.len(),
        with_secret,
        unreachable_kinds
    );
}

#[tokio::test]
async fn raw_secrets_in_config_history_need_the_full_administrator_scope() {
    #[cfg(test)]
    const SECRET: &str = "known-history-secret-2e8f";
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, answer) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "mqtt", "name": "hist-secret", "server": "127.0.0.1:1",
                       "password": SECRET}),
            &token,
        )
        .await;
    assert_eq!(status, 201, "{answer}");
    // A key with one scope only (system) is not a full administrator.
    let (_, limited) = make_key(
        &server,
        &token,
        json!({"name": "hist-lim", "scopes": ["system"]}),
    )
    .await;
    let (_, versions) = server.get_auth("/api/v1/config/history", &limited).await;
    let id = versions["versions"][0]["id"]
        .as_u64()
        .expect("a version exists");
    let path = format!("/api/v1/config/history/{id}");
    // Masked answer: allowed for both.
    for who in [&token, &limited] {
        let (status, masked) = server.get_auth(&path, who).await;
        assert_eq!(status, 200);
        let text = masked.to_string();
        let at = text.find(SECRET).unwrap_or(0);
        assert!(
            !text.contains(SECRET),
            "masked answer shows a secret: {}",
            &text[at.saturating_sub(200)..(at + 100).min(text.len())]
        );
    }
    // Raw answer: the full administrator only.
    let (status, raw) = server
        .get_auth(&format!("{path}?secrets=raw"), &token)
        .await;
    assert_eq!(status, 200);
    assert!(
        raw.to_string().contains(SECRET),
        "the raw snapshot lacks the secret"
    );
    let (status, refused) = server
        .get_auth(&format!("{path}?secrets=raw"), &limited)
        .await;
    assert_eq!(status, 403, "{refused}");
    assert_eq!(refused["code"], json!("FORBIDDEN"));
    assert!(!refused.to_string().contains(SECRET));
    // A key with no scope list and the administrator role is a full administrator.
    let (_, full) = make_key(&server, &token, json!({"name": "hist-full"})).await;
    let (status, raw) = server.get_auth(&format!("{path}?secrets=raw"), &full).await;
    assert_eq!(status, 200);
    assert!(raw.to_string().contains(SECRET));
}

#[cfg(feature = "kafka")]
#[tokio::test]
async fn kafka_consumer_connector_keeps_the_type_that_was_sent() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    for (name, kind, expected) in [
        ("keep-consumer", "kafka_consumer", "kafka_consumer"),
        ("keep-producer", "kafka_producer", "kafka_producer"),
        ("keep-kafka", "kafka", "kafka_producer"),
    ] {
        let (status, created) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": name, "type": kind, "bootstrap_hosts": "127.0.0.1:9092"}),
                &token,
            )
            .await;
        assert_eq!(status, 201, "{kind}: {created}");
        assert_eq!(created["type"], json!(expected), "{kind} create");
        let (status, shown) = server
            .get_auth(&format!("/api/v5/connectors/{name}"), &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(shown["type"], json!(expected), "{kind} read");
    }
    let (_, listed) = server.get_auth("/api/v5/connectors", &token).await;
    let types: Vec<&str> = listed
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|row| row["type"].as_str())
        .collect();
    assert!(types.contains(&"kafka_consumer"), "{types:?}");
}

#[tokio::test]
async fn connectors_survive_restart() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("w023-conn-survive");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;

    // No `server`/`url` target: no probe runs, status is `connected`
    // and the create path registers a live `pgsql` sink.
    let (status, created) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "w023-http-conn", "type": "pgsql"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert!(
        state.engine.connectors().get("w023-http-conn").is_some(),
        "create must register a live sink"
    );
    drop(server);
    drop(registry);

    // Simulate a kernel restart: rebuild state from the same dir.
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, state) = TestServer::start_with_registry(reloaded).await;
    // The admin password persisted too, so the rebooted node logs in
    // with the changed password (`login_as_admin` only fits fresh
    // servers still on the default password).
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    let token = body["token"].as_str().expect("admin token").to_string();

    // The identical stored entry is back ...
    let (status, body) = server
        .get_auth("/api/v5/connectors/w023-http-conn", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(
        body, created,
        "restored connector must return the identical stored entry"
    );
    // ... and the restored connector actually connects, not just lists:
    // the live sink is re-registered through the same path create uses.
    assert!(
        state.engine.connectors().get("w023-http-conn").is_some(),
        "restored connector must re-register its live sink"
    );
    let (status, body) = server.get_auth("/api/v1/connectors", &token).await;
    assert_eq!(status, 200);
    assert!(
        body.as_array()
            .expect("connectors array")
            .iter()
            .any(|c| c["id"] == json!("w023-http-conn")),
        "restored connector must be live, got: {body}"
    );
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn connector_delete_survives_restart() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("w023-conn-delete");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;

    for name in ["w023-del-conn", "w023-kept-conn"] {
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": name, "type": "pgsql"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
    }
    let (status, _) = server
        .delete_auth("/api/v5/connectors/w023-del-conn", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .get_auth("/api/v5/connectors/w023-del-conn", &token)
        .await;
    assert_eq!(status, 404);
    // The delete must reach the snapshot on disk: the kept connector
    // is stored, the deleted one is gone.
    let snapshot = ConfigRegistry::load(&dir)
        .expect("reload data dir")
        .snapshot();
    let ids: Vec<&str> = snapshot
        .connectors
        .connectors
        .iter()
        .map(|c| c.id.as_str())
        .collect();
    assert!(
        ids.contains(&"w023-kept-conn"),
        "kept connector must be persisted, got: {ids:?}"
    );
    assert!(
        !ids.contains(&"w023-del-conn"),
        "deleted connector must stay deleted, got: {ids:?}"
    );
    drop(server);
    drop(registry);

    // Simulate a kernel restart: rebuild state from the same dir.
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, state) = TestServer::start_with_registry(reloaded).await;
    // The admin password persisted too, so the rebooted node logs in
    // with the changed password.
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    let token = body["token"].as_str().expect("admin token").to_string();

    // The deleted connector stays deleted, and no sink comes back,
    // while the kept connector is back with its live sink.
    let (status, body) = server
        .get_auth("/api/v5/connectors/w023-del-conn", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    assert!(
        state.engine.connectors().get("w023-del-conn").is_none(),
        "deleted connector must not re-register any sink"
    );
    let (status, body) = server
        .get_auth("/api/v5/connectors/w023-kept-conn", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["id"], json!("w023-kept-conn"));
    assert!(
        state.engine.connectors().get("w023-kept-conn").is_some(),
        "kept connector must re-register its live sink"
    );
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn connector_duplicate_create_rejected_without_poisoning() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    // TK-03: a duplicate create must be rejected with the documented
    // code before the store is mutated, so later
    // POST/PUT/DELETE keep working (no poisoned persistence).
    let dir = unique_data_dir("tk03-conn-dup");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;

    // No `server`/`url` target: no probe runs, status is `connected`.
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "tk03-dup-conn", "type": "pgsql"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    // Duplicate by `name`: the documented 400, not a 500.
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "tk03-dup-conn", "type": "pgsql"}),
            &token,
        )
        .await;
    assert_eq!(
        status, 400,
        "duplicate create must be rejected, got: {body}"
    );
    assert_eq!(body["code"], json!("ALREADY_EXISTS"));

    // Duplicate by `id`: the same connector under the other key.
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"id": "tk03-dup-conn", "type": "pgsql"}),
            &token,
        )
        .await;
    assert_eq!(
        status, 400,
        "duplicate create by id must be rejected, got: {body}"
    );
    assert_eq!(body["code"], json!("ALREADY_EXISTS"));

    // The store is not poisoned: later POST/PUT/DELETE work normally.
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "tk03-other-conn", "type": "pgsql"}),
            &token,
        )
        .await;
    assert_eq!(
        status, 201,
        "create after a rejected duplicate must succeed"
    );
    let (status, _) = server
        .put_auth(
            "/api/v5/connectors/tk03-dup-conn",
            json!({"type": "pgsql", "enable": true}),
            &token,
        )
        .await;
    assert_eq!(status, 200, "update of the existing id must keep working");
    let (status, _) = server
        .delete_auth("/api/v5/connectors/tk03-other-conn", &token)
        .await;
    assert_eq!(
        status, 204,
        "delete after a rejected duplicate must succeed"
    );
    let (status, body) = server
        .get_auth("/api/v5/connectors/tk03-dup-conn", &token)
        .await;
    assert_eq!(
        status, 200,
        "original connector must still be readable: {body}"
    );
    assert_eq!(body["id"], json!("tk03-dup-conn"));
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn w317_connectors_list_create_delete_round_trip() {
    // W3-17 (C07-01/C07-02/C07-03) failing test: create a connector,
    // list and expect it with paging metadata and live fields;
    // duplicate create expects rejection; unknown kind expects
    // rejection; delete then second delete expects 404 with exact-id
    // semantics. Before: dump shape (bare array), fuzzy suffix match,
    // missing kind validation. Broker-through: every step goes through
    // the HTTP router on the live engine; create registers the live
    // sink (asserted via the manager, driven by the create event) and
    // one publish goes through `dispatch_ingress` (the broker's rule
    // path), never `sink.send` directly. Management-plane only
    // otherwise; publish and deliver never touch the connector map
    // (dispatch reads the manager directly).
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Scrub leftovers from earlier runs sharing the process-global
    // store so paging assertions stay deterministic. The fire rule
    // goes first: a connector still referenced by a rule cannot be
    // deleted.
    let _ = server
        .delete_auth("/api/v5/rules/w317-fire-rule", &token)
        .await;
    for stale in [
        "w317-list-conn",
        "w317-exact",
        "w317-guarded",
        "w317-guard-action",
        "w317-fire-conn",
        "w317-fire-rule",
    ] {
        let _ = server
            .delete_auth(&format!("/api/v5/rules/{stale}"), &token)
            .await;
        let _ = server
            .delete_auth(&format!("/api/v5/actions/{stale}"), &token)
            .await;
        let _ = server
            .delete_auth(&format!("/api/v5/connectors/{stale}"), &token)
            .await;
    }

    // Create: validated write with live fields, not a raw dump.
    let (status, created) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "w317-list-conn", "type": "pgsql"}),
            &token,
        )
        .await;
    assert_eq!(status, 201, "create must succeed, got: {created}");
    assert_eq!(created["id"], json!("w317-list-conn"));
    assert_eq!(created["type"], json!("pgsql"));
    assert!(created.get("status").and_then(|v| v.as_str()).is_some());
    assert!(created.get("node_status").is_some());
    // Live health linkage: create registered the sink through the
    // same path boot replay uses, driven by the create event
    // (`crates/broker-api/src/v5/rules.rs`, `register_live_sink`;
    // consulted on the fire path at
    // `crates/broker-rules/src/lib.rs`, `run_actions` via
    // `ConnectorManager::send`).
    assert!(
        state.engine.connectors().get("w317-list-conn").is_some(),
        "create must register a live sink"
    );

    // List: paged collection with live health, not a raw dump.
    let (status, body) = server.get_auth("/api/v5/connectors", &token).await;
    assert_eq!(status, 200);
    let data = body.as_array().expect("data is a list");
    assert!(
        data.iter().any(|c| c["id"] == json!("w317-list-conn")),
        "list must contain the created connector, got: {body}"
    );
    let row = data
        .iter()
        .find(|c| c["id"] == json!("w317-list-conn"))
        .expect("created row present");
    assert!(row.get("status").and_then(|v| v.as_str()).is_some());
    assert!(row.get("node_status").is_some());

    // Unknown query parameters are ignored, never an error.
    let (status, _) = server
        .get_auth("/api/v5/connectors?unknown=param", &token)
        .await;
    assert_eq!(status, 200);

    // Duplicate create is rejected cleanly, never overwritten.
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "w317-list-conn", "type": "pgsql"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "duplicate must be rejected, got: {body}");
    assert_eq!(body["code"], json!("ALREADY_EXISTS"));
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"id": "w317-list-conn", "type": "pgsql"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "duplicate by id must be rejected, got: {body}");
    assert_eq!(body["code"], json!("ALREADY_EXISTS"));

    // Unknown kind and malformed bodies are rejected with the
    // documented error shape.
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "w317-bad-kind", "type": "no_such_kind_xyz"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "unknown kind must be rejected, got: {body}");
    assert_eq!(body["code"], json!("ALREADY_EXISTS"));
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "w317-no-type"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    let (status, _) = server
        .post_auth("/api/v5/connectors", json!({"type": "pgsql"}), &token)
        .await;
    assert_eq!(status, 400);

    // Exact-id semantics: the legacy `prefix:id` suffix form no longer
    // matches. Before, `x:w317-exact` deleted `w317-exact`.
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "w317-exact", "type": "pgsql"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .delete_auth("/api/v5/connectors/x:w317-exact", &token)
        .await;
    assert_eq!(status, 404, "fuzzy delete must miss");
    let (status, _) = server
        .delete_auth("/api/v5/connectors/w317-exact", &token)
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .delete_auth("/api/v5/connectors/w317-exact", &token)
        .await;
    assert_eq!(status, 404, "second delete must miss, got: {body}");
    assert_eq!(body["code"], json!("NOT_FOUND"));

    // Delete of an unknown id is 404 with the documented code.
    let (status, body) = server
        .delete_auth("/api/v5/connectors/w317-no-such", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));

    // Delete of a connector still referenced by an action is rejected.
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "w317-guarded", "type": "pgsql"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "w317-guard-action", "type": "console",
                       "server": "127.0.0.1:1883", "connector": "w317-guarded"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, body) = server
        .delete_auth("/api/v5/connectors/w317-guarded", &token)
        .await;
    assert_eq!(
        status, 400,
        "referenced delete must be rejected, got: {body}"
    );
    let (status, _) = server
        .delete_auth("/api/v5/actions/w317-guard-action", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/connectors/w317-guarded", &token)
        .await;
    assert_eq!(status, 204);

    // Broker rule path: a rule forwarding through the live manager
    // fires on publish, never `sink.send` directly.
    #[cfg(test)]
    struct NoopBrokerSink;
    #[async_trait::async_trait]
    #[cfg(test)]
    impl broker_rules::BrokerSink for NoopBrokerSink {
        #[cfg(test)]
        async fn publish(
            &self,
            _topic: broker_protocol::Topic,
            _payload: bytes::Bytes,
            _qos: broker_protocol::QoS,
            _retain: bool,
        ) -> Result<(), broker_rules::RuleEngineError> {
            Ok(())
        }
    }
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "w317-fire-conn", "type": "pgsql"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "w317-fire-conn", "type": "console",
                       "server": "127.0.0.1:1883"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "w317-fire-rule", "name": "w317-fire-rule",
                       "sql": "SELECT * FROM \"w317/fire\"",
                       "actions": ["w317-fire-conn"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let broker_sink: Arc<dyn broker_rules::BrokerSink> = Arc::new(NoopBrokerSink);
    let topic = broker_protocol::Topic::new("w317/fire").expect("valid topic");
    let payload = bytes::Bytes::from_static(b"{\"k\":1}");
    let matched = state
        .engine
        .dispatch_ingress(
            &topic,
            &payload,
            broker_protocol::QoS::AtMostOnce,
            &broker_sink,
        )
        .await;
    assert_eq!(matched, 1, "rule must fire on its topic");
    let rule = state
        .engine
        .get_rule("w317-fire-rule")
        .expect("rule present");
    assert_eq!(
        rule.actions_total_cnt.load(Ordering::Relaxed),
        1,
        "exactly one forward must attribute"
    );

    // Cleanup so the shared global store stays tidy. The rule goes
    // first: a connector still referenced by a rule cannot be
    // deleted.
    let _ = server
        .delete_auth("/api/v5/rules/w317-fire-rule", &token)
        .await;
    for stale in [
        "w317-list-conn",
        "w317-guarded",
        "w317-guard-action",
        "w317-fire-conn",
    ] {
        let _ = server
            .delete_auth(&format!("/api/v5/actions/{stale}"), &token)
            .await;
        let _ = server
            .delete_auth(&format!("/api/v5/connectors/{stale}"), &token)
            .await;
    }
}

#[tokio::test]
async fn w318_connector_detail_read_update_round_trip() {
    // W3-18 (C07-04/C07-05) failing test: create a connector, read it
    // back expecting documented fields, update the config and read
    // again expecting new values; near-miss id expects 404 (no fuzzy
    // match); unknown id expects 404; invalid bodies are rejected.
    // Before: fuzzy suffix match (`x:id` read `id`) and update without
    // create-like validation. Broker-through: every step goes through
    // the HTTP router on the live engine; create/update register the
    // live sink (asserted via the manager, driven by the create/update
    // event) and reads render live health from the manager, never a
    // write-time snapshot. Management-plane only; publish and deliver
    // never touch the connector map (dispatch reads the manager
    // directly, never this map).
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Scrub leftovers from earlier runs sharing the process-global
    // store so exact-match assertions stay deterministic.
    for stale in ["w318-detail-conn"] {
        let _ = server
            .delete_auth(&format!("/api/v5/connectors/{stale}"), &token)
            .await;
    }

    // Create: validated write with documented fields and live health.
    let (status, created) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "w318-detail-conn", "type": "webhook",
                       "url": "http://127.0.0.1:9"}),
            &token,
        )
        .await;
    assert_eq!(status, 201, "create must succeed, got: {created}");
    assert_eq!(created["id"], json!("w318-detail-conn"));
    assert_eq!(created["type"], json!("webhook"));
    assert!(created.get("status").and_then(|v| v.as_str()).is_some());
    assert!(created.get("node_status").is_some());
    // Live linkage: create registered the sink through the same path
    // boot replay uses (`crates/broker-api/src/v5/rules.rs`,
    // `register_live_sink`; the sink lives in
    // `crates/broker-connectors/src/lib.rs`, `ConnectorManager::register`
    // at line 543, consulted on the fire path at
    // `crates/broker-connectors/src/lib.rs`, `ConnectorManager::send`
    // at line 583 via `dispatch_ingress`).
    assert!(
        state.engine.connectors().get("w318-detail-conn").is_some(),
        "create must register a live sink"
    );

    // Read hit: documented fields with live health.
    let (status, body) = server
        .get_auth("/api/v5/connectors/w318-detail-conn", &token)
        .await;
    assert_eq!(status, 200, "read hit must succeed, got: {body}");
    assert_eq!(body["id"], json!("w318-detail-conn"));
    assert_eq!(body["type"], json!("webhook"));
    assert_eq!(body["url"], json!("http://127.0.0.1:9"));
    assert!(body.get("status").and_then(|v| v.as_str()).is_some());
    assert!(body.get("node_status").is_some());

    // Unknown query parameters are ignored, never an error.
    let (status, _) = server
        .get_auth("/api/v5/connectors/w318-detail-conn?unknown=param", &token)
        .await;
    assert_eq!(status, 200);

    // Near-miss ids never fuzzy-match: the legacy `prefix:id` suffix
    // form is a 404 with the documented error code. Note: a literal
    // trailing space cannot travel in an HTTP request-target (hyper
    // rejects it with 400 before routing), so the trailing-space
    // near-miss is sent percent-encoded (`%20`); axum decodes it to
    // `"w318-detail-conn "` before the exact-match lookup.
    for near_miss in [
        "x:w318-detail-conn",
        "W318-DETAIL-CONN",
        "w318-detail-conn%20",
    ] {
        let (status, body) = server
            .get_auth(&format!("/api/v5/connectors/{near_miss}"), &token)
            .await;
        assert_eq!(
            status, 404,
            "near-miss read {near_miss:?} must miss, got: {body}"
        );
        assert_eq!(body["code"], json!("NOT_FOUND"));
    }

    // Unknown id is 404 with the documented error code.
    let (status, body) = server
        .get_auth("/api/v5/connectors/w318-no-such", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));

    // Update round trip: validated replace, new values read back.
    let (status, updated) = server
        .put_auth(
            "/api/v5/connectors/w318-detail-conn",
            json!({"name": "w318-detail-conn", "type": "webhook",
                       "url": "http://127.0.0.1:10",
                       "description": "updated by w318"}),
            &token,
        )
        .await;
    assert_eq!(status, 200, "update must succeed, got: {updated}");
    assert_eq!(updated["id"], json!("w318-detail-conn"));
    assert_eq!(updated["url"], json!("http://127.0.0.1:10"));
    let (status, body) = server
        .get_auth("/api/v5/connectors/w318-detail-conn", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["url"], json!("http://127.0.0.1:10"));
    assert_eq!(body["description"], json!("updated by w318"));

    // Update of an unknown id is 404 and creates nothing.
    let (status, body) = server
        .put_auth(
            "/api/v5/connectors/w318-no-such",
            json!({"name": "w318-no-such", "type": "webhook",
                       "url": "http://127.0.0.1:9"}),
            &token,
        )
        .await;
    assert_eq!(status, 404, "unknown update must miss, got: {body}");
    assert_eq!(body["code"], json!("NOT_FOUND"));
    let (status, _) = server
        .get_auth("/api/v5/connectors/w318-no-such", &token)
        .await;
    assert_eq!(status, 404, "failed update must create nothing");

    // Invalid bodies are rejected with the documented error shape and
    // leave the stored entry untouched.
    for (label, bad) in [
        (
            "unknown kind",
            json!({"name": "w318-detail-conn", "type": "no_such_kind_xyz"}),
        ),
        ("missing type", json!({"name": "w318-detail-conn"})),
        (
            "body id mismatch",
            json!({"id": "w318-other", "name": "w318-other", "type": "webhook"}),
        ),
    ] {
        let (status, body) = server
            .put_auth("/api/v5/connectors/w318-detail-conn", bad, &token)
            .await;
        assert_eq!(
            status, 400,
            "invalid update ({label}) must be rejected, got: {body}"
        );
        assert_eq!(body["code"], json!("BAD_REQUEST"));
    }
    let (status, body) = server
        .get_auth("/api/v5/connectors/w318-detail-conn", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(
        body["url"],
        json!("http://127.0.0.1:10"),
        "rejected updates must not mutate the entry"
    );

    // Cleanup so the shared global store stays tidy.
    let _ = server
        .delete_auth("/api/v5/connectors/w318-detail-conn", &token)
        .await;
}

#[tokio::test]
async fn test_connectors_endpoint_lists_registered() {
    use broker_connectors::Sink;

    #[cfg(test)]
    struct NullSink;
    #[async_trait::async_trait]
    #[cfg(test)]
    impl Sink for NullSink {
        #[cfg(test)]
        async fn send(
            &self,
            _topic: &Topic,
            _payload: &bytes::Bytes,
            _qos: QoS,
        ) -> Result<(), broker_connectors::ConnectorError> {
            Ok(())
        }

        #[cfg(test)]
        fn kind(&self) -> &'static str {
            "test"
        }
    }

    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, body) = server.get_auth("/api/v1/connectors", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));

    state
        .engine
        .connectors()
        .register("webhook-1", Arc::new(NullSink));
    let (status, body) = server.get_auth("/api/v1/connectors", &token).await;
    assert_eq!(status, 200);
    assert_eq!(
        body,
        json!([{"id": "webhook-1", "kind": "test", "tier": "community"}])
    );
}

// Full-matrix creation test (X1-10): exercises every connector kind,
// including the feature-gated heavy SDKs, so it runs only when all
// gating features are enabled (default and per-feature builds skip
// it; the full-feature build runs it).
#[tokio::test]
async fn connector_unknown_is_404() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, body) = server
        .get_auth("/api/v5/connectors/w0-13-no-such-connector", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
}

#[tokio::test]
async fn ap05_connectors_typed_round_trip() {
    // AP-05/KC-00: ready kinds (webhook and pgsql) round trip through
    // both id forms. Pending timescaledb and removed s3_tables are refused and store
    // nothing. MV-03a sets mqtt and kafka ready, so they no longer
    // prove the pending refusal here.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let cases = vec![
        (
            "ap05-conn-webhook",
            json!({"type": "webhook", "name": "ap05-conn-webhook",
                       "url": "http://127.0.0.1:9"}),
            "webhook:ap05-conn-webhook",
        ),
        (
            "ap05-conn-pgsql",
            json!({"type": "pgsql", "name": "ap05-conn-pgsql"}),
            "pgsql:ap05-conn-pgsql",
        ),
    ];
    for (bare, body, key) in cases {
        let (status, created) = server.post_auth("/api/v5/connectors", body, &token).await;
        assert_eq!(status, 201, "create for {bare}");
        assert_eq!(created["name"], json!(bare));
        assert!(created["status"].is_string());
        assert!(created["node_status"].is_array());
        for path in [
            format!("/api/v5/connectors/{bare}"),
            format!("/api/v5/connectors/{key}"),
        ] {
            let (status, read) = server.get_auth(&path, &token).await;
            assert_eq!(status, 200, "read through {path}");
            assert_eq!(read["name"], json!(bare));
        }
        let (status, _) = server
                .put_auth(
                    &format!("/api/v5/connectors/{key}"),
                    json!({"type": created["type"].clone(), "name": bare, "description": "ap05", "url": "http://127.0.0.1:9"}),
                    &token,
                )
                .await;
        assert_eq!(status, 200, "replace through {key}");
        let (status, _) = server
            .delete_auth(&format!("/api/v5/connectors/{bare}"), &token)
            .await;
        assert_eq!(status, 204);
    }
    for (name, kind) in [
        ("ap05-conn-ts", "timescaledb"),
        ("ap05-conn-s3t", "s3_tables"),
    ] {
        let (status, refused) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"type": kind, "name": name}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "{kind} connector must be refused");
        assert!(
            refused.to_string().contains("not available in this broker"),
            "the refusal names the kind: {refused}"
        );
        let (status, _) = server
            .get_auth(&format!("/api/v5/connectors/{name}"), &token)
            .await;
        assert_eq!(status, 404, "{kind} connector stores nothing");
    }
    let (status, bad) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "no_such_kind_xyz", "name": "ap05-bad"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(bad["code"], json!("ALREADY_EXISTS"));
}
