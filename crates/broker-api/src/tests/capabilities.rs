use super::*;

#[tokio::test]
async fn removed_s3_tables_kind_is_refused_everywhere_and_stores_nothing() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let message = "connector type s3_tables is not available in this broker";

    for (index, kind) in ["s3_tables", "s3tables", "iceberg"].iter().enumerate() {
        let id = format!("removed-{index}");
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"id": id, "type": kind, "config": {}}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "{kind} connector create");
        assert_eq!(
            body["code"],
            json!("ALREADY_EXISTS"),
            "{kind} connector create"
        );
        assert_eq!(body["message"], json!(message), "{kind} connector create");

        let (status, body) = server
            .post_auth(
                "/api/v5/actions",
                json!({"id": id, "type": kind, "enable": true}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "{kind} action create");
        assert_eq!(
            body["code"],
            json!("ALREADY_EXISTS"),
            "{kind} action create"
        );
        assert_eq!(body["message"], json!(message), "{kind} action create");

        let (status, body) = server
            .post_auth(
                "/api/v5/sources",
                json!({"id": id, "type": kind, "enable": true}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "{kind} source create");
        assert_eq!(
            body["code"],
            json!("ALREADY_EXISTS"),
            "{kind} source create"
        );
        assert_eq!(body["message"], json!(message), "{kind} source create");

        let (status, body) = server
            .post_auth(
                "/api/v5/connectors_probe",
                json!({"type": kind, "config": {}}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "{kind} connector probe");
        assert_eq!(body["code"], json!("TEST_FAILED"), "{kind} connector probe");
        assert_eq!(body["message"], json!(message), "{kind} connector probe");

        let (status, body) = server
            .post_auth(
                "/api/v5/actions_probe",
                json!({"type": kind, "config": {}}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "{kind} action probe");
        assert_eq!(body["code"], json!("TEST_FAILED"), "{kind} action probe");
        assert_eq!(body["message"], json!(message), "{kind} action probe");

        let (status, body) = server
            .post_auth(
                "/api/v5/sources_probe",
                json!({"type": kind, "config": {}}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "{kind} source probe");
        assert_eq!(body["code"], json!("TEST_FAILED"), "{kind} source probe");
        assert_eq!(body["message"], json!(message), "{kind} source probe");

        let (status, body) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": id, "kind": kind, "config": {}}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "{kind} v1 connector create");
        assert_eq!(body["error"], json!(message), "{kind} v1 connector create");
    }

    let (status, connectors) = server.get_auth("/api/v5/connectors", &token).await;
    assert_eq!(status, 200);
    assert_eq!(connectors, json!([]));
    let (status, actions) = server.get_auth("/api/v5/actions", &token).await;
    assert_eq!(status, 200);
    assert_eq!(actions, json!([]));
    let (status, sources) = server.get_auth("/api/v5/sources", &token).await;
    assert_eq!(status, 200);
    assert_eq!(sources, json!([]));
    let (status, schema) = server.get_auth("/schemas/connectors", &token).await;
    assert_eq!(status, 200);
    let schema: serde_json::Value = serde_json::from_str(
        schema
            .as_str()
            .expect("connector schema body is a JSON string"),
    )
    .expect("connector schema string holds JSON");
    assert!(
        schema.get("connector_s3tables.post_connector").is_none(),
        "removed connector schema is not served"
    );
    let (status, _) = server
        .get_auth("/schemas/connector_s3tables.post_connector", &token)
        .await;
    assert_eq!(
        status, 404,
        "direct request for a removed schema is unknown"
    );
    let (status, action_types) = server.get_auth("/api/v5/action_types", &token).await;
    assert_eq!(status, 200);
    let action_types = action_types.as_array().expect("action type list");
    assert!(!action_types
        .iter()
        .any(|kind| kind.as_str() == Some("s3tables")));
    assert!(!action_types
        .iter()
        .any(|kind| kind.as_str() == Some("s3_tables")));
    let (status, source_types) = server.get_auth("/api/v5/source_types", &token).await;
    assert_eq!(status, 200);
    let source_types = source_types.as_array().expect("source type list");
    assert!(!source_types
        .iter()
        .any(|kind| kind.as_str() == Some("s3tables")));
    assert!(!source_types
        .iter()
        .any(|kind| kind.as_str() == Some("s3_tables")));
}

#[tokio::test]
async fn removed_update_is_refused_and_keeps_the_old_connector_and_action() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let message = "connector type s3_tables is not available in this broker";

    for (index, kind) in ["s3_tables", "s3tables", "iceberg"].iter().enumerate() {
        let connector_id = format!("keep-conn-{index}");
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"id": connector_id, "type": "console"}),
                &token,
            )
            .await;
        assert_eq!(status, 201, "{kind} connector seed");
        let (status, body) = server
            .put_auth(
                &format!("/api/v5/connectors/{connector_id}"),
                json!({"id": connector_id, "type": kind}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "{kind} connector update");
        assert_eq!(
            body["code"],
            json!("BAD_REQUEST"),
            "{kind} connector update"
        );
        assert_eq!(body["message"], json!(message), "{kind} connector update");
        let (status, stored) = server
            .get_auth(&format!("/api/v5/connectors/{connector_id}"), &token)
            .await;
        assert_eq!(status, 200, "{kind} connector still readable");
        assert_eq!(
            stored["type"],
            json!("console"),
            "{kind} connector unchanged"
        );

        let action_id = format!("keep-act-{index}");
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"id": action_id, "type": "console"}),
                &token,
            )
            .await;
        assert_eq!(status, 201, "{kind} action seed");
        let (status, body) = server
            .put_auth(
                &format!("/api/v5/actions/{action_id}"),
                json!({"id": action_id, "type": kind}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "{kind} action update");
        assert_eq!(body["code"], json!("BAD_REQUEST"), "{kind} action update");
        assert_eq!(body["message"], json!(message), "{kind} action update");
        let (status, stored) = server
            .get_auth(&format!("/api/v5/actions/{action_id}"), &token)
            .await;
        assert_eq!(status, 200, "{kind} action still readable");
        assert_eq!(stored["type"], json!("console"), "{kind} action unchanged");

        let (status, _) = server
            .delete_auth(&format!("/api/v5/connectors/{connector_id}"), &token)
            .await;
        assert_eq!(status, 204, "{kind} connector cleanup");
        let (status, _) = server
            .delete_auth(&format!("/api/v5/actions/{action_id}"), &token)
            .await;
        assert_eq!(status, 204, "{kind} action cleanup");
    }
}

#[tokio::test]
async fn strict_registry_commit_refuses_removed_kind_through_reload() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // A reload that carries a removed connector is refused.
    let mut candidate = state.config.snapshot().as_ref().clone();
    candidate
        .connectors
        .connectors
        .push(broker_config::ConnectorEntry {
            id: "reload-s3t".to_string(),
            connector_type: "s3_tables".to_string(),
            enable: true,
            config: "{}".to_string(),
        });
    let (status, body) = server
        .post_auth(
            "/api/v1/config/reload",
            json!({"snapshot": candidate, "actor": "sb22", "summary": "refuse s3"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "reload with removed connector: {body}");
    let error = body["error"].as_str().unwrap_or("");
    assert!(error.contains("s3_tables"), "error names kind: {error}");
    // A reload that carries a removed action is refused.
    let mut candidate = state.config.snapshot().as_ref().clone();
    candidate.actions.actions.push(broker_config::ActionEntry {
        id: "reload-s3t".to_string(),
        action_type: "s3tables".to_string(),
        enable: true,
        config: "{}".to_string(),
    });
    let (status, body) = server
        .post_auth(
            "/api/v1/config/reload",
            json!({"snapshot": candidate, "actor": "sb22", "summary": "refuse s3"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "reload with removed action: {body}");
    // A reload that carries a removed source is refused.
    let mut candidate = state.config.snapshot().as_ref().clone();
    candidate.sources.sources.push(broker_config::SourceEntry {
        id: "reload-s3t".to_string(),
        source_type: "s3_tables".to_string(),
        enable: true,
        config: "{}".to_string(),
    });
    let (status, body) = server
        .post_auth(
            "/api/v1/config/reload",
            json!({"snapshot": candidate, "actor": "sb22", "summary": "refuse s3"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "reload with removed source: {body}");
    // The generic commit for each root is also strict.
    let bad_connectors = broker_config::ConnectorsConf {
        connectors: vec![broker_config::ConnectorEntry {
            id: "commit-s3t".to_string(),
            connector_type: "s3_tables".to_string(),
            enable: true,
            config: "{}".to_string(),
        }],
    };
    let err = state
        .config
        .commit(broker_config::ConfigRoot::Connectors(bad_connectors))
        .expect_err("commit must refuse removed connector");
    assert!(err.to_string().contains("s3_tables"), "got: {err}");
    let bad_actions = broker_config::ActionsConf {
        actions: vec![broker_config::ActionEntry {
            id: "commit-s3t".to_string(),
            action_type: "s3_tables".to_string(),
            enable: true,
            config: "{}".to_string(),
        }],
    };
    let err = state
        .config
        .commit(broker_config::ConfigRoot::Actions(bad_actions))
        .expect_err("commit must refuse removed action");
    assert!(err.to_string().contains("s3_tables"), "got: {err}");
    // Nothing is stored.
    let (status, connectors) = server.get_auth("/api/v5/connectors", &token).await;
    assert_eq!(status, 200);
    assert_eq!(connectors, json!([]));
    let (status, actions) = server.get_auth("/api/v5/actions", &token).await;
    assert_eq!(status, 200);
    assert_eq!(actions, json!([]));
    let (status, sources) = server.get_auth("/api/v5/sources", &token).await;
    assert_eq!(status, 200);
    assert_eq!(sources, json!([]));
}

#[tokio::test]
async fn pending_kind_is_refused_everywhere_and_updates_keep_old_records() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let message = "connector type timescaledb is not available in this broker";

    // Create of a pending kind stores nothing.
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"id": "pending-conn", "type": "timescaledb"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "pending connector create");
    assert_eq!(body["code"], json!("ALREADY_EXISTS"));
    assert_eq!(body["message"], json!(message));

    // Seed one record of each ready kind.
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"id": "pending-keep-conn", "type": "webhook",
                       "url": "http://127.0.0.1:9"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"id": "pending-keep-action", "type": "console"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
            .post_auth(
                "/api/v5/sources",
                json!({"id": "pending-keep-source", "type": "mqtt", "connector": "mqtt:up", "parameters": {"topic": "in/pending-keep-source"}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);

    // Update of each pending kind answers 400 BAD_REQUEST and keeps the
    // old record.
    let (status, body) = server
        .put_auth(
            "/api/v5/connectors/pending-keep-conn",
            json!({"type": "timescaledb"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "pending connector update");
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    assert_eq!(body["message"], json!(message));
    let (status, body) = server
        .get_auth("/api/v5/connectors/pending-keep-conn", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["type"], json!("webhook"));
    assert_eq!(body["url"], json!("http://127.0.0.1:9"));

    let (status, body) = server
        .put_auth(
            "/api/v5/actions/pending-keep-action",
            json!({"type": "timescaledb"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "pending action update");
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    assert_eq!(body["message"], json!(message));
    let (status, body) = server
        .get_auth("/api/v5/actions/pending-keep-action", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["type"], json!("console"));

    let (status, body) = server
        .put_auth(
            "/api/v5/sources/pending-keep-source",
            json!({"type": "timescaledb"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "pending source update");
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    assert_eq!(body["message"], json!(message));
    let (status, body) = server
        .get_auth("/api/v5/sources/pending-keep-source", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["type"], json!("mqtt"));

    // Create of a pending action and source also stores nothing.
    for (path, id) in [
        ("/api/v5/actions", "pending-action"),
        ("/api/v5/sources", "pending-source"),
    ] {
        let (status, body) = server
            .post_auth(path, json!({"id": id, "type": "timescaledb"}), &token)
            .await;
        assert_eq!(status, 400, "{path} create");
        assert_eq!(body["code"], json!("ALREADY_EXISTS"), "{path}");
        assert_eq!(body["message"], json!(message), "{path}");
    }

    // Probes of the pending kind answer 400 TEST_FAILED before a dial.
    for path in [
        "/api/v5/connectors_probe",
        "/api/v5/actions_probe",
        "/api/v5/sources_probe",
    ] {
        let (status, body) = server
            .post_auth(path, json!({"type": "timescaledb"}), &token)
            .await;
        assert_eq!(status, 400, "{path}");
        assert_eq!(body["code"], json!("TEST_FAILED"), "{path}");
        assert_eq!(body["message"], json!(message), "{path}");
    }

    let (status, connectors) = server.get_auth("/api/v5/connectors", &token).await;
    assert_eq!(status, 200);
    for row in connectors.as_array().expect("connector list") {
        assert_ne!(
            row["type"],
            json!("timescaledb"),
            "pending connector not stored"
        );
    }
    let (status, actions) = server.get_auth("/api/v5/actions", &token).await;
    assert_eq!(status, 200);
    for row in actions.as_array().expect("action list") {
        assert_ne!(
            row["type"],
            json!("timescaledb"),
            "pending action not stored"
        );
    }
    let (status, sources) = server.get_auth("/api/v5/sources", &token).await;
    assert_eq!(status, 200);
    for row in sources.as_array().expect("source list") {
        assert_ne!(
            row["type"],
            json!("timescaledb"),
            "pending source not stored"
        );
    }

    let _ = server
        .delete_auth("/api/v5/sources/pending-keep-source", &token)
        .await;
    let _ = server
        .delete_auth("/api/v5/actions/pending-keep-action", &token)
        .await;
    let _ = server
        .delete_auth("/api/v5/connectors/pending-keep-conn", &token)
        .await;
}

/// SB-27 refuses a state window at create with 400 and a clear message.
#[tokio::test]
async fn sb27_state_window_is_refused_at_create_through_the_api() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, body) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "id": "sb27-state",
                "name": "sb27-state",
                "sql": "SELECT * FROM \"sb27/state\" GROUP BY STATEWINDOW(a = 1, a = 5)",
                "actions": [],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 400, "state window refuses: {body}");
    let message = body["message"].as_str().unwrap_or("");
    assert!(message.contains("state window"), "{body}");
    assert!(
        state.engine.get_rule("sb27-state").is_none(),
        "nothing is stored"
    );
}

#[tokio::test]
async fn test_kind_is_available_only_when_the_test_switch_is_on() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    broker_config::set_test_kinds_enabled(false);
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "test-kind-off", "type": "test"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "release semantics refuse the test kind");
    assert_eq!(body["code"], json!("ALREADY_EXISTS"));

    broker_config::set_test_kinds_enabled(true);
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "test-kind-on", "type": "test"}),
            &token,
        )
        .await;
    assert_eq!(status, 201, "a test build admits the test kind: {body}");
    let (status, _) = server
        .delete_auth("/api/v5/connectors/test-kind-on", &token)
        .await;
    assert_eq!(status, 204);
    broker_config::set_test_kinds_enabled(false);
}

#[tokio::test]
async fn schema_and_type_listings_hide_every_non_ready_kind() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    for path in ["/schemas/connectors", "/schemas/actions"] {
        let (status, schema) = server.get_auth(path, &token).await;
        assert_eq!(status, 200, "{path}");
        let schema: serde_json::Value =
            serde_json::from_str(schema.as_str().expect("schema body is a JSON string"))
                .expect("schema string holds JSON");
        let rows = schema.as_object().expect("schema object");
        assert!(!rows.is_empty(), "{path} lists at least one ready kind");
        let mut kinds = Vec::new();
        for value in rows.values() {
            let kind = value
                .get("properties")
                .and_then(|properties| properties.get("type"))
                .and_then(|type_prop| type_prop.get("default"))
                .and_then(|default| default.as_str())
                .expect("schema names its kind");
            assert!(
                broker_config::is_kind_ready(kind),
                "{path} lists non-ready kind {kind}"
            );
            kinds.push(kind.to_string());
        }
        assert!(
            kinds.iter().any(|kind| kind.as_str() == "pgsql"),
            "{path} lists the ready pgsql kind: {kinds:?}"
        );
    }

    let (status, action_types) = server.get_auth("/api/v5/action_types", &token).await;
    assert_eq!(status, 200);
    let action_kinds: Vec<&str> = action_types
        .as_array()
        .expect("action type list")
        .iter()
        .map(|kind| kind.as_str().expect("action type is a string"))
        .collect();
    for kind in &action_kinds {
        assert!(
            broker_config::is_kind_ready(kind),
            "action_types lists non-ready kind {kind}"
        );
    }
    assert!(
        action_kinds.contains(&"pgsql"),
        "action_types lists the documented ready kind pgsql: {action_kinds:?}"
    );

    let (status, source_types) = server.get_auth("/api/v5/source_types", &token).await;
    assert_eq!(status, 200);
    let source_kinds: Vec<&str> = source_types
        .as_array()
        .expect("source type list")
        .iter()
        .map(|kind| kind.as_str().expect("source type is a string"))
        .collect();
    for kind in &source_kinds {
        assert!(
            broker_config::is_kind_ready(kind),
            "source_types lists non-ready kind {kind}"
        );
    }
    // MV-08 lists only the source kinds with a working ingress
    // runtime. The MQTT subscriber runs here. The other reference
    // kinds stay out until their driver consumer lands; a create of
    // one answers 400 instead of claiming a source that receives
    // nothing.
    assert!(
        source_kinds.contains(&"mqtt"),
        "source_types lists the ready ingress kind mqtt: {source_kinds:?}"
    );
    for unsupported in ["kafka_consumer", "gcp_pubsub_consumer", "rabbitmq"] {
        assert!(
            !source_kinds.contains(&unsupported),
            "source_types lists no kind without ingress {unsupported}: {source_kinds:?}"
        );
    }
    for pending in ["timescaledb", "oci_streaming"] {
        assert!(
            !source_kinds.contains(&pending),
            "source_types lists no pending kind {pending}: {source_kinds:?}"
        );
    }
}

#[tokio::test]
async fn sb15_connector_schemas_match_ready_capability_kinds() {
    // MV-06b broker-through test: the connector schema listing names
    // exactly ready kinds. Each entry resolves to a ready capability
    // row. Pending kinds stay hidden. Ready kinds keep their form.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, schema) = server.get_auth("/schemas/connectors", &token).await;
    assert_eq!(status, 200);
    let schema: serde_json::Value =
        serde_json::from_str(schema.as_str().expect("schema body is a JSON string"))
            .expect("schema string holds JSON");
    let rows = schema.as_object().expect("schema object");
    let mut kinds = Vec::new();
    for value in rows.values() {
        let kind = value
            .get("properties")
            .and_then(|properties| properties.get("type"))
            .and_then(|type_prop| type_prop.get("default"))
            .and_then(|default| default.as_str())
            .expect("schema names its kind");
        assert!(
            broker_config::is_kind_ready(kind),
            "connectors schema lists non-ready kind {kind}"
        );
        kinds.push(kind.to_string());
    }
    // Derive the expected set from the capability map.
    // Each ready connector row needs one served form.
    // Each pending or refused row needs none.
    for row in broker_config::KIND_ROWS {
        let resolves = |served: &str| {
            broker_config::kind_row(served).is_some_and(|found| found.name == row.name)
        };
        if row.connector && broker_config::row_is_ready(row) {
            assert!(
                kinds.iter().any(|kind| resolves(kind)),
                "connectors schema lists ready kind {}: {kinds:?}",
                row.name
            );
        } else if !broker_config::row_is_ready(row) {
            assert!(
                !kinds.iter().any(|kind| resolves(kind)),
                "connectors schema hides non-ready kind {}: {kinds:?}",
                row.name
            );
        }
    }
}

#[tokio::test]
async fn legacy_s3_tables_state_boots_reports_and_deletes_in_reference_order() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let dir =
        std::env::temp_dir().join(format!("indra-mv04-legacy-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create data dir");
    let legacy = r#"
[[connectors.connectors]]
id = "legacy-s3t"
connector_type = "s3_tables"
enable = true
config = '{"table_bucket_arn":"arn:aws:s3tables:us-east-1:123456789012:bucket/telemetry"}'

[[connectors.connectors]]
id = "legacy-good"
connector_type = "console"
enable = true
config = '{}'

[[actions.actions]]
id = "legacy-s3t"
action_type = "s3tables"
enable = true
config = '{"connector":"legacy-s3t"}'

[[actions.actions]]
id = "legacy-good"
action_type = "console"
enable = true
config = '{"connector":"legacy-good"}'

[[rules.rules]]
id = "legacy-rule"
name = "legacy rule"
tenant = "default"
topic_filter = "legacy/#"
sql_query = 'SELECT * FROM "legacy/+" INTO connector("legacy-s3t")'
enabled = true

[[rules.rules.actions]]
#[cfg(test)]
type = "forwardconnector"
connector_id = "legacy-good"
"#;
    std::fs::write(dir.join("state.toml"), legacy).expect("write legacy state");

    let registry =
        Arc::new(ConfigRegistry::load_for_boot(&dir).expect("legacy state loads for boot"));
    let (server, state) = TestServer::start_with_registry(registry).await;
    let token = server.login_as_admin().await;
    let reason = "connector type s3_tables is not available in this broker";

    let (status, connectors) = server.get_auth("/api/v5/connectors", &token).await;
    assert_eq!(status, 200);
    let s3 = connectors
        .as_array()
        .expect("connector list")
        .iter()
        .find(|row| row["id"] == json!("legacy-s3t"))
        .expect("legacy connector listed");
    assert_eq!(s3["status"], json!("disconnected"));
    assert_eq!(s3["status_reason"], json!(reason));
    assert!(
        connectors
            .as_array()
            .expect("connector list")
            .iter()
            .any(|row| row["id"] == json!("legacy-good")),
        "working sibling connector stays listed"
    );

    let (status, actions) = server.get_auth("/api/v5/actions", &token).await;
    assert_eq!(status, 200);
    let action = actions
        .as_array()
        .expect("action list")
        .iter()
        .find(|row| row["id"] == json!("legacy-s3t"))
        .expect("legacy action listed");
    assert_eq!(action["status"], json!("disconnected"));
    assert_eq!(action["status_reason"], json!(reason));

    let (status, rule) = server.get_auth("/api/v5/rules/legacy-rule", &token).await;
    assert_eq!(status, 200);
    assert_eq!(rule["enable"], json!(true));

    let (status, alarms) = server.get_auth("/api/v5/alarms", &token).await;
    assert_eq!(status, 200);
    let names: Vec<&str> = alarms["data"]
        .as_array()
        .expect("alarm rows")
        .iter()
        .filter_map(|row| row["name"].as_str())
        .collect();
    assert!(
        names.contains(&"connector_unavailable:legacy-s3t"),
        "{names:?}"
    );
    assert!(
        names.contains(&"action_unavailable:legacy-s3t"),
        "{names:?}"
    );

    let sink: Arc<dyn broker_rules::BrokerSink> = Arc::new(NoopBrokerSink);
    state
        .engine
        .dispatch_ingress(
            &Topic::new("legacy/device").expect("topic"),
            &bytes::Bytes::from_static(br#"{"v":1}"#),
            QoS::AtMostOnce,
            &sink,
        )
        .await;

    let (status, metrics) = server
        .get_auth("/api/v5/actions/legacy-s3t/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(metrics["metrics"]["total"], json!(1));
    assert_eq!(metrics["metrics"]["failed"], json!(1));
    let (status, metrics) = server
        .get_auth("/api/v5/actions/legacy-good/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(metrics["metrics"]["total"], json!(1));
    assert_eq!(metrics["metrics"]["success"], json!(1));
    let (status, metrics) = server
        .get_auth("/api/v5/rules/legacy-rule/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(metrics["metrics"]["actions.failed"], json!(1));
    assert_eq!(metrics["metrics"]["actions.success"], json!(1));

    let (status, _) = server
        .delete_auth("/api/v5/connectors/legacy-s3t", &token)
        .await;
    assert_eq!(status, 400, "connector is still referenced by the rule");
    let (status, _) = server
        .delete_auth("/api/v5/actions/legacy-s3t", &token)
        .await;
    assert_eq!(status, 400, "action is still referenced by the rule");
    let (status, _) = server
        .delete_auth("/api/v5/rules/legacy-rule", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/legacy-s3t", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/connectors/legacy-s3t", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/legacy-good", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/connectors/legacy-good", &token)
        .await;
    assert_eq!(status, 204);

    let (status, alarms) = server.get_auth("/api/v5/alarms", &token).await;
    assert_eq!(status, 200);
    let names: Vec<&str> = alarms["data"]
        .as_array()
        .expect("alarm rows")
        .iter()
        .filter_map(|row| row["name"].as_str())
        .collect();
    assert!(
        !names.contains(&"connector_unavailable:legacy-s3t"),
        "{names:?}"
    );
    assert!(
        !names.contains(&"action_unavailable:legacy-s3t"),
        "{names:?}"
    );

    let reloaded = ConfigRegistry::load_for_boot(&dir).expect("rewritten state still boots");
    assert!(reloaded.snapshot().connectors.connectors.is_empty());
    assert!(reloaded.snapshot().actions.actions.is_empty());
    assert!(reloaded.snapshot().rules.rules.is_empty());
}

#[tokio::test]
async fn pending_kind_state_boots_reports_and_counts_failure() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let dir =
        std::env::temp_dir().join(format!("indra-kc00-pending-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create data dir");
    let pending = r#"
[[connectors.connectors]]
id = "pending-timescaledb"
connector_type = "timescaledb"
enable = true
config = '{}'

[[connectors.connectors]]
id = "pending-good"
connector_type = "console"
enable = true
config = '{}'

[[actions.actions]]
id = "pending-timescaledb"
action_type = "timescaledb"
enable = true
config = '{"connector":"pending-timescaledb"}'

[[actions.actions]]
id = "pending-good"
action_type = "console"
enable = true
config = '{"connector":"pending-good"}'

[[rules.rules]]
id = "pending-rule"
name = "pending rule"
tenant = "default"
topic_filter = "pending/#"
sql_query = 'SELECT * FROM "pending/+" INTO connector("pending-timescaledb")'
enabled = true

[[rules.rules.actions]]
#[cfg(test)]
type = "forwardconnector"
connector_id = "pending-good"
"#;
    std::fs::write(dir.join("state.toml"), pending).expect("write pending state");

    let registry =
        Arc::new(ConfigRegistry::load_for_boot(&dir).expect("pending state loads for boot"));
    let (server, _state) = TestServer::start_with_registry(registry).await;
    let token = server.login_as_admin().await;
    let reason = "connector type timescaledb is not available in this broker";

    let (status, connectors) = server.get_auth("/api/v5/connectors", &token).await;
    assert_eq!(status, 200);
    let pending_connector = connectors
        .as_array()
        .expect("connector list")
        .iter()
        .find(|row| row["id"] == json!("pending-timescaledb"))
        .expect("pending connector kept and listed");
    assert_eq!(pending_connector["status"], json!("disconnected"));
    assert_eq!(pending_connector["status_reason"], json!(reason));
    assert!(
        connectors
            .as_array()
            .expect("connector list")
            .iter()
            .any(|row| row["id"] == json!("pending-good")),
        "working sibling connector stays listed"
    );

    let (status, actions) = server.get_auth("/api/v5/actions", &token).await;
    assert_eq!(status, 200);
    let pending_action = actions
        .as_array()
        .expect("action list")
        .iter()
        .find(|row| row["id"] == json!("pending-timescaledb"))
        .expect("pending action kept and listed");
    assert_eq!(pending_action["status"], json!("disconnected"));
    assert_eq!(pending_action["status_reason"], json!(reason));

    let (status, rule) = server.get_auth("/api/v5/rules/pending-rule", &token).await;
    assert_eq!(status, 200);
    assert_eq!(rule["enable"], json!(true));

    let (status, alarms) = server.get_auth("/api/v5/alarms", &token).await;
    assert_eq!(status, 200);
    let names: Vec<&str> = alarms["data"]
        .as_array()
        .expect("alarm rows")
        .iter()
        .filter_map(|row| row["name"].as_str())
        .collect();
    assert!(
        names.contains(&"connector_unavailable:pending-timescaledb"),
        "{names:?}"
    );
    assert!(
        names.contains(&"action_unavailable:pending-timescaledb"),
        "{names:?}"
    );

    // The management publish entry drives the stored rule.
    let (status, _) = server
        .post_auth(
            "/api/v5/publish",
            json!({"topic": "pending/device", "payload": "{\"v\":1}", "qos": 1}),
            &token,
        )
        .await;
    assert_eq!(status, 202, "no subscriber: accepted for the rule path");

    let (status, metrics) = server
        .get_auth("/api/v5/actions/pending-timescaledb/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(metrics["metrics"]["total"], json!(1));
    assert_eq!(metrics["metrics"]["failed"], json!(1));
    let (status, metrics) = server
        .get_auth("/api/v5/actions/pending-good/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(metrics["metrics"]["total"], json!(1));
    assert_eq!(metrics["metrics"]["success"], json!(1));
    let (status, metrics) = server
        .get_auth("/api/v5/rules/pending-rule/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(metrics["metrics"]["actions.failed"], json!(1));
    assert_eq!(metrics["metrics"]["actions.success"], json!(1));

    std::fs::remove_dir_all(&dir).ok();
}
