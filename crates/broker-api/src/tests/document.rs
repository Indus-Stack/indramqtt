use super::*;

#[cfg(feature = "mongodb")]
#[cfg(test)]
pub(super) fn mv02c_bson_text(value: &broker_connectors_enterprise::BsonValue) -> Option<String> {
    use broker_connectors_enterprise::BsonValue;
    match value {
        BsonValue::String(text) => Some(text.clone()),
        _ => None,
    }
}

#[cfg(feature = "mongodb")]
#[cfg(test)]
pub(super) fn mv02c_bson_docs(
    value: &broker_connectors_enterprise::BsonValue,
) -> Option<Vec<broker_connectors_enterprise::BsonDocument>> {
    use broker_connectors_enterprise::BsonValue;
    match value {
        BsonValue::Array(items) => Some(
            items
                .iter()
                .filter_map(|item| match item {
                    BsonValue::Document(doc) => Some(doc.clone()),
                    _ => None,
                })
                .collect(),
        ),
        _ => None,
    }
}

#[cfg(feature = "elasticsearch")]
#[tokio::test]
async fn mv02c_elasticsearch_delivers_action_target_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv02c-elasticsearch");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02bFake::start(200, "{}").await;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{}", fake.port);

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "elasticsearch", "name": "mv02c-es", "server": endpoint}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "elasticsearch", "name": "mv02c-es-act",
                       "connector": "mv02c-es",
                       "parameters": {"action": "create", "index": "events",
                                      "id": "id-${seq}",
                                      "doc": "{\"topic\":\"${topic}\",\"payload\":${payload}}"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02c-es-rule", "name": "mv02c-es-rule",
                       "sql": "SELECT * FROM \"mv02c/es\"", "actions": ["mv02c-es-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    mv02b_publish(&server, &token, "mv02c/es", "{\"v\":1}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].method, "POST");
    assert_eq!(got[0].path, "/_bulk");
    let body_text = Mv02bFake::body_text(&got[0]);
    let lines: Vec<&str> = body_text.lines().filter(|line| !line.is_empty()).collect();
    assert_eq!(lines.len(), 2, "{lines:?}");
    let action: serde_json::Value = serde_json::from_str(lines[0]).expect("action line");
    let source: serde_json::Value = serde_json::from_str(lines[1]).expect("source line");
    assert_eq!(action["index"]["_index"], json!("events"));
    assert_eq!(action["index"]["_id"], json!("id-0"));
    assert_eq!(source, json!({"topic": "mv02c/es", "payload": {"v": 1}}));
    mv02b_wait_success(&server, &token, "mv02c-es-act", 1).await;

    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02c-es-act",
            json!({"type": "elasticsearch", "name": "mv02c-es-act",
                       "connector": "mv02c-es",
                       "parameters": {"action": "create", "index": "events2",
                                      "id": "id-${seq}",
                                      "doc": "{\"topic\":\"${topic}\"}"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv02c/es", "{\"v\":2}").await;
    let got = fake.wait_for(2).await;
    let action: serde_json::Value =
        serde_json::from_str(Mv02bFake::body_text(&got[1]).lines().next().unwrap_or(""))
            .expect("action line");
    assert_eq!(action["index"]["_index"], json!("events2"));
    mv02b_wait_success(&server, &token, "mv02c-es-act", 2).await;

    let (status, body) = server
        .put_auth(
            "/api/v5/actions/mv02c-es-act",
            json!({"type": "elasticsearch", "name": "mv02c-es-act",
                       "connector": "mv02c-es",
                       "parameters": {"action": "update", "index": "events2"}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "an update without an id refuses: {body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("id"),
        "{body}"
    );
    mv02b_publish(&server, &token, "mv02c/es", "{\"v\":3}").await;
    let got = fake.wait_for(3).await;
    let action: serde_json::Value =
        serde_json::from_str(Mv02bFake::body_text(&got[2]).lines().next().unwrap_or(""))
            .expect("action line");
    assert_eq!(action["index"]["_index"], json!("events2"));
    mv02b_wait_success(&server, &token, "mv02c-es-act", 3).await;

    // Reference `overwrite` false writes `create` (fail on conflict).
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02c-es-act",
            json!({"type": "elasticsearch", "name": "mv02c-es-act",
                       "connector": "mv02c-es",
                       "parameters": {"action": "create", "index": "events2",
                                      "id": "id-${seq}",
                                      "doc": "{\"topic\":\"${topic}\"}",
                                      "overwrite": false}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv02c/es", "{\"v\":33}").await;
    let got = fake.wait_for(4).await;
    let action: serde_json::Value =
        serde_json::from_str(Mv02bFake::body_text(&got[3]).lines().next().unwrap_or(""))
            .expect("action line");
    assert_eq!(action["create"]["_index"], json!("events2"));
    mv02b_wait_success(&server, &token, "mv02c-es-act", 4).await;

    let down_port = fake.port;
    drop(fake);
    mv02b_publish(&server, &token, "mv02c/es", "{\"v\":4}").await;
    for _ in 0..100 {
        let (status, metrics) = server
            .get_auth("/api/v5/actions/mv02c-es-act/metrics", &token)
            .await;
        assert_eq!(status, 200);
        if metrics["metrics"]["failed"].as_u64().unwrap_or(0) >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let (status, metrics) = server
        .get_auth("/api/v5/actions/mv02c-es-act/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(metrics["metrics"]["failed"], json!(1));
    // The server returns on the same port: delivery resumes.
    let fake = Mv02bFake::start_on(down_port, 200, "{}").await;
    mv02b_publish(&server, &token, "mv02c/es", "{\"v\":5}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got.len(), 1);
    mv02b_wait_success(&server, &token, "mv02c-es-act", 5).await;
    drop(server);
    drop(registry);
    std::fs::remove_dir_all(&dir).ok();
}

/// Poll one action's metrics until `failed` reaches `want`.
/// Exact equality: a duplicate failure count fails instead of passing.
#[cfg(test)]
async fn mv02c_wait_failed(server: &TestServer, token: &str, action: &str, want: u64) {
    for _ in 0..300 {
        let (status, metrics) = server
            .get_auth(&format!("/api/v5/actions/{action}/metrics"), token)
            .await;
        assert_eq!(status, 200);
        if metrics["metrics"]["failed"].as_u64().unwrap_or(0) == want {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("action {action} never reached failed {want}");
}

#[cfg(feature = "mongodb")]
#[tokio::test]
async fn mv02c_mongodb_wiring_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv02c-mongodb");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02cMongoFake::start().await;
    let port = fake.port;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let target = format!("127.0.0.1:{port}");

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "mongodb", "name": "mv02c-mongo",
                       "server": target, "database": "db", "timeout_ms": 1_000}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "mongodb", "name": "mv02c-mongo-act",
                       "connector": "mv02c-mongo",
                       "parameters": {"collection": "events",
                                      "payload_template": "{\"v\":${payload.v}}"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02c-mongo-rule", "name": "mv02c-mongo-rule",
                       "sql": "SELECT * FROM \"mv02c/mongo\"", "actions": ["mv02c-mongo-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    // The loopback server receives the exact database, collection
    // and document value the action names.
    mv02b_publish(&server, &token, "mv02c/mongo", "{\"v\":1}").await;
    let calls = fake.wait_for(1).await;
    assert_eq!(calls[0].command, "insert");
    assert_eq!(calls[0].db, "db");
    assert_eq!(calls[0].collection, "events");
    assert_eq!(
        calls[0].documents[0].get("v"),
        Some(&broker_connectors_enterprise::BsonValue::Int32(1)),
        "{:?}",
        calls[0].documents[0]
    );
    mv02b_wait_success(&server, &token, "mv02c-mongo-act", 1).await;

    // An update moves the collection on the wire.
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02c-mongo-act",
            json!({"type": "mongodb", "name": "mv02c-mongo-act",
                       "connector": "mv02c-mongo",
                       "parameters": {"collection": "events2",
                                      "payload_template": "{\"v\":${payload.v}}"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv02c/mongo", "{\"v\":2}").await;
    let calls = fake.wait_for(2).await;
    assert_eq!(calls[1].collection, "events2");
    assert_eq!(
        calls[1].documents[0].get("v"),
        Some(&broker_connectors_enterprise::BsonValue::Int32(2)),
        "{:?}",
        calls[1].documents[0]
    );
    mv02b_wait_success(&server, &token, "mv02c-mongo-act", 2).await;

    // A refused update keeps the old collection delivering.
    let (status, body) = server
        .put_auth(
            "/api/v5/actions/mv02c-mongo-act",
            json!({"type": "mongodb", "name": "mv02c-mongo-act",
                       "connector": "mv02c-mongo",
                       "parameters": {"collection": "events3",
                                      "payload_template": "not json ${topic}"}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "the refused update answers 400: {body}");
    mv02b_publish(&server, &token, "mv02c/mongo", "{\"v\":5}").await;
    let calls = fake.wait_for(3).await;
    assert_eq!(calls[2].db, "db");
    assert_eq!(calls[2].collection, "events2");
    assert_eq!(
        calls[2].documents[0].get("v"),
        Some(&broker_connectors_enterprise::BsonValue::Int32(5)),
        "{:?}",
        calls[2].documents[0]
    );
    mv02b_wait_success(&server, &token, "mv02c-mongo-act", 3).await;

    // The server dies: the action counts one failed forward.
    drop(fake);
    mv02b_publish(&server, &token, "mv02c/mongo", "{\"v\":3}").await;
    mv02c_wait_failed(&server, &token, "mv02c-mongo-act", 1).await;
    let (status, read) = server
        .get_auth("/api/v5/connectors/mv02c-mongo", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(read["status"], json!("disconnected"));

    // The server returns on the same port: delivery resumes with the
    // stored connector and the updated collection.
    let fake = Mv02cMongoFake::start_on(port).await;
    mv02b_publish(&server, &token, "mv02c/mongo", "{\"v\":4}").await;
    let calls = fake.wait_for(1).await;
    assert_eq!(calls[0].db, "db");
    assert_eq!(calls[0].collection, "events2");
    assert_eq!(
        calls[0].documents[0].get("v"),
        Some(&broker_connectors_enterprise::BsonValue::Int32(4)),
        "{:?}",
        calls[0].documents[0]
    );
    mv02b_wait_success(&server, &token, "mv02c-mongo-act", 4).await;

    // Two actions on one connector with different collections both
    // deliver through the same stored connection.
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "mongodb", "name": "mv02c-mongo-act-b",
                       "connector": "mv02c-mongo",
                       "parameters": {"collection": "events-b",
                                      "payload_template": "{\"v\":${payload.v}}"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, body) = server
        .put_auth(
            "/api/v5/rules/mv02c-mongo-rule",
            json!({"id": "mv02c-mongo-rule", "name": "mv02c-mongo-rule",
                       "sql": "SELECT * FROM \"mv02c/mongo\"",
                       "actions": ["mv02c-mongo-act", "mv02c-mongo-act-b"]}),
            &token,
        )
        .await;
    assert_eq!(status, 200, "the rule keeps both actions: {body}");
    mv02b_publish(&server, &token, "mv02c/mongo", "{\"v\":6}").await;
    let calls = fake.wait_for(3).await;
    let mut collections = vec![calls[1].collection.clone(), calls[2].collection.clone()];
    collections.sort();
    assert_eq!(
        collections,
        vec!["events-b".to_string(), "events2".to_string()]
    );
    mv02b_wait_success(&server, &token, "mv02c-mongo-act", 5).await;
    mv02b_wait_success(&server, &token, "mv02c-mongo-act-b", 1).await;

    drop(server);
    drop(registry);
    std::fs::remove_dir_all(&dir).ok();
}

#[cfg(feature = "couchbase")]
#[tokio::test]
async fn mv02c_couchbase_wiring_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv02c-couchbase");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02cCouchFake::start().await;
    let port = fake.port;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let target = format!("127.0.0.1:{port}");

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "couchbase", "name": "mv02c-cb",
                       "server": target, "username": "u", "password": "p",
                       "timeout_ms": 5_000}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"type": "couchbase", "name": "mv02c-cb-act",
                       "connector": "mv02c-cb",
                       "parameters": {"sql": "UPSERT INTO `b` (KEY, VALUE) VALUES ($client_id, {\"v\":$payload_v})"}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02c-cb-rule", "name": "mv02c-cb-rule",
                       "sql": "SELECT * FROM \"mv02c/cb\"", "actions": ["mv02c-cb-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    // The loopback query service receives the exact statement and
    // bound value for the event.
    mv02b_publish(
        &server,
        &token,
        "mv02c/cb",
        "{\"client_id\":\"edge-7\",\"v\":21}",
    )
    .await;
    let queries = fake.wait_for(1).await;
    let query: serde_json::Value =
        serde_json::from_str(&queries[0]).expect("couch query body is JSON");
    assert_eq!(
        query["statement"],
        json!("UPSERT INTO `b` (KEY, VALUE) VALUES ($client_id, {\"v\":$payload_v})")
    );
    assert_eq!(query["$client_id"], json!("edge-7"), "{query}");
    assert_eq!(query["$payload_v"], json!("21"), "{query}");
    mv02b_wait_success(&server, &token, "mv02c-cb-act", 1).await;

    // An update moves the statement on the wire.
    let (status, _) = server
            .put_auth(
                "/api/v5/actions/mv02c-cb-act",
                json!({"type": "couchbase", "name": "mv02c-cb-act",
                       "connector": "mv02c-cb",
                       "parameters": {"sql": "UPSERT INTO `b2` (KEY, VALUE) VALUES ($client_id, {\"v\":$payload_v})"}}),
                &token,
            )
            .await;
    assert_eq!(status, 200);
    mv02b_publish(
        &server,
        &token,
        "mv02c/cb",
        "{\"client_id\":\"edge-8\",\"v\":22}",
    )
    .await;
    let queries = fake.wait_for(2).await;
    let query: serde_json::Value =
        serde_json::from_str(&queries[1]).expect("couch query body is JSON");
    assert_eq!(
        query["statement"],
        json!("UPSERT INTO `b2` (KEY, VALUE) VALUES ($client_id, {\"v\":$payload_v})")
    );
    assert_eq!(query["$client_id"], json!("edge-8"), "{query}");
    assert_eq!(query["$payload_v"], json!("22"), "{query}");
    mv02b_wait_success(&server, &token, "mv02c-cb-act", 2).await;

    // A refused update keeps the old statement delivering.
    let (status, body) = server
        .put_auth(
            "/api/v5/actions/mv02c-cb-act",
            json!({"type": "couchbase", "name": "mv02c-cb-act",
                       "connector": "mv02c-cb", "parameters": {}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "an update without sql refuses: {body}");
    mv02b_publish(
        &server,
        &token,
        "mv02c/cb",
        "{\"client_id\":\"edge-11\",\"v\":25}",
    )
    .await;
    let queries = fake.wait_for(3).await;
    let query: serde_json::Value =
        serde_json::from_str(&queries[2]).expect("couch query body is JSON");
    assert_eq!(
        query["statement"],
        json!("UPSERT INTO `b2` (KEY, VALUE) VALUES ($client_id, {\"v\":$payload_v})")
    );
    assert_eq!(query["$client_id"], json!("edge-11"), "{query}");
    assert_eq!(query["$payload_v"], json!("25"), "{query}");
    mv02b_wait_success(&server, &token, "mv02c-cb-act", 3).await;

    // The server dies: the action counts one failed forward.
    drop(fake);
    mv02b_publish(
        &server,
        &token,
        "mv02c/cb",
        "{\"client_id\":\"edge-9\",\"v\":23}",
    )
    .await;
    mv02c_wait_failed(&server, &token, "mv02c-cb-act", 1).await;
    let (status, read) = server.get_auth("/api/v5/connectors/mv02c-cb", &token).await;
    assert_eq!(status, 200);
    assert_eq!(read["status"], json!("disconnected"));

    // The server returns on the same port: delivery resumes with the
    // stored connector and the updated statement.
    let fake = Mv02cCouchFake::start_on(port).await;
    mv02b_publish(
        &server,
        &token,
        "mv02c/cb",
        "{\"client_id\":\"edge-10\",\"v\":24}",
    )
    .await;
    let queries = fake.wait_for(1).await;
    let query: serde_json::Value =
        serde_json::from_str(&queries[0]).expect("couch query body is JSON");
    assert_eq!(
        query["statement"],
        json!("UPSERT INTO `b2` (KEY, VALUE) VALUES ($client_id, {\"v\":$payload_v})")
    );
    assert_eq!(query["$client_id"], json!("edge-10"), "{query}");
    assert_eq!(query["$payload_v"], json!("24"), "{query}");
    mv02b_wait_success(&server, &token, "mv02c-cb-act", 4).await;

    // Two actions on one connector with different statements both
    // deliver through the same stored connection.
    let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"type": "couchbase", "name": "mv02c-cb-act-b",
                       "connector": "mv02c-cb",
                       "parameters": {"sql": "UPSERT INTO `b3` (KEY, VALUE) VALUES ($client_id, {\"v\":$payload_v})"}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    let (status, body) = server
        .put_auth(
            "/api/v5/rules/mv02c-cb-rule",
            json!({"id": "mv02c-cb-rule", "name": "mv02c-cb-rule",
                       "sql": "SELECT * FROM \"mv02c/cb\"",
                       "actions": ["mv02c-cb-act", "mv02c-cb-act-b"]}),
            &token,
        )
        .await;
    assert_eq!(status, 200, "the rule keeps both actions: {body}");
    mv02b_publish(
        &server,
        &token,
        "mv02c/cb",
        "{\"client_id\":\"edge-12\",\"v\":26}",
    )
    .await;
    let queries = fake.wait_for(3).await;
    let mut statements = Vec::new();
    for raw in queries.iter().skip(1).take(2) {
        let query: serde_json::Value = serde_json::from_str(raw).expect("couch query body is JSON");
        assert_eq!(query["$client_id"], json!("edge-12"), "{query}");
        assert_eq!(query["$payload_v"], json!("26"), "{query}");
        statements.push(query["statement"].as_str().unwrap_or("").to_string());
    }
    statements.sort();
    let mut expected = vec![
        "UPSERT INTO `b2` (KEY, VALUE) VALUES ($client_id, {\"v\":$payload_v})".to_string(),
        "UPSERT INTO `b3` (KEY, VALUE) VALUES ($client_id, {\"v\":$payload_v})".to_string(),
    ];
    expected.sort();
    assert_eq!(statements, expected);
    mv02b_wait_success(&server, &token, "mv02c-cb-act", 5).await;
    mv02b_wait_success(&server, &token, "mv02c-cb-act-b", 1).await;

    drop(server);
    drop(registry);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv02a_oracle_delivers_bound_rows_through_broker() {
    // Oracle has no loopback TNS fake. The driver records each
    // statement with its binds before the dial. This test reads
    // that record after driving the full broker path.
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv02a-oracle");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    broker_connectors_enterprise::oracle::clear_oracle_driver_attempts();

    // A dead port fails the dial fast. The attempt record stays.
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "oracle", "name": "mv02a-ora",
                       "server": "127.0.0.1:1", "username": "u", "password": "pwd",
                       "service_name": "XE"}),
            &token,
        )
        .await;
    assert_eq!(status, 201, "connector create: {body}");
    let sql = "MV02A-ORACLE-SQL INSERT INTO t(topic, qos, payload) VALUES (:1, :2, :3)";
    let (status, body) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "oracle", "name": "mv02a-ora-act",
                       "connector": "mv02a-ora",
                       "parameters": {"sql": sql}}),
            &token,
        )
        .await;
    assert_eq!(status, 201, "action create: {body}");
    // The stored statement echoes exactly. No value is formatted in.
    let (status, read) = server
        .get_auth("/api/v5/actions/mv02a-ora-act", &token)
        .await;
    assert_eq!(status, 200, "{read}");
    assert_eq!(read["parameters"]["sql"], json!(sql), "{read}");
    let (status, body) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02a-ora-rule", "name": "mv02a-ora-rule",
                       "sql": "SELECT * FROM \"mv02a/ora\"", "actions": ["mv02a-ora-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201, "rule create: {body}");

    // One publish drives the stored rule path with a quote payload.
    // The management publish entry is a production entry.
    let payload = "{\"v\":\"a\\\"b\"}";
    mv02b_publish(&server, &token, "mv02a/ora", payload).await;
    mv02c_wait_failed(&server, &token, "mv02a-ora-act", 1).await;

    // The dead dial counts one failed forward, none succeeds.
    let (status, metrics) = server
        .get_auth("/api/v5/actions/mv02a-ora-act/metrics", &token)
        .await;
    assert_eq!(status, 200, "{metrics}");
    assert_eq!(metrics["metrics"]["failed"], json!(1), "{metrics}");
    assert_eq!(metrics["metrics"]["success"], json!(0), "{metrics}");

    // The driver got the bound row: topic, qos, payload with quote.
    // The quote never enters the statement text.
    let attempts = broker_connectors_enterprise::oracle::oracle_driver_attempts();
    assert!(!attempts.is_empty(), "the rule path reached the driver");
    for attempt in &attempts {
        assert_eq!(attempt.statement, sql, "exact :1 :2 :3 statement");
        assert_eq!(attempt.binds.len(), 3, "topic, qos, payload");
        assert_eq!(
            attempt.binds[0].value,
            broker_connectors_enterprise::oracle::OracleValue::String("mv02a/ora".to_string())
        );
        assert_eq!(
            attempt.binds[1].value,
            broker_connectors_enterprise::oracle::OracleValue::Integer(0)
        );
        assert_eq!(
            attempt.binds[2].value,
            broker_connectors_enterprise::oracle::OracleValue::String(payload.to_string())
        );
        assert!(
            !attempt.statement.contains("a\"b"),
            "quote stays bound: {}",
            attempt.statement
        );
    }

    let (status, _) = server
        .delete_auth("/api/v5/rules/mv02a-ora-rule", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/mv02a-ora-act", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/connectors/mv02a-ora", &token)
        .await;
    assert_eq!(status, 204);
    drop(server);
    drop(registry);
    std::fs::remove_dir_all(&dir).ok();
}

#[cfg(feature = "elasticsearch")]
#[tokio::test]
async fn mv02c_two_actions_on_one_connector_keep_delivering() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv02c-two-actions");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02bFake::start(200, "{}").await;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{}", fake.port);

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "elasticsearch", "name": "mv02c-es2", "server": endpoint}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    for (name, index) in [("mv02c-es2-a", "events-a"), ("mv02c-es2-b", "events-b")] {
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"type": "elasticsearch", "name": name,
                           "connector": "mv02c-es2",
                           "parameters": {"action": "create", "index": index,
                                          "id": "id-${seq}",
                                          "doc": "{\"topic\":\"${topic}\"}"}}),
                &token,
            )
            .await;
        assert_eq!(status, 201, "action {name} creates");
    }
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02c-es2-rule", "name": "mv02c-es2-rule",
                       "sql": "SELECT * FROM \"mv02c/es2\"",
                       "actions": ["mv02c-es2-a", "mv02c-es2-b"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    mv02b_publish(&server, &token, "mv02c/es2", "{\"v\":1}").await;
    let got = fake.wait_for(2).await;
    let mut indexes: Vec<String> = got
        .iter()
        .take(2)
        .map(|request| {
            let action: serde_json::Value =
                serde_json::from_str(Mv02bFake::body_text(request).lines().next().unwrap_or(""))
                    .expect("action line");
            let key = if action.get("index").is_some() {
                "index"
            } else {
                "create"
            };
            action[key]["_index"].as_str().unwrap_or("").to_string()
        })
        .collect();
    indexes.sort();
    assert_eq!(
        indexes,
        vec!["events-a".to_string(), "events-b".to_string()]
    );
    mv02b_wait_success(&server, &token, "mv02c-es2-a", 1).await;
    mv02b_wait_success(&server, &token, "mv02c-es2-b", 1).await;

    // A failed update keeps both actions delivering on their targets.
    let (status, body) = server
        .put_auth(
            "/api/v5/actions/mv02c-es2-a",
            json!({"type": "elasticsearch", "name": "mv02c-es2-a",
                       "connector": "mv02c-es2",
                       "parameters": {"action": "update", "index": "events-a"}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "an update without an id refuses: {body}");
    mv02b_publish(&server, &token, "mv02c/es2", "{\"v\":2}").await;
    let got = fake.wait_for(4).await;
    let mut indexes: Vec<String> = got
        .iter()
        .skip(2)
        .take(2)
        .map(|request| {
            let action: serde_json::Value =
                serde_json::from_str(Mv02bFake::body_text(request).lines().next().unwrap_or(""))
                    .expect("action line");
            let key = if action.get("index").is_some() {
                "index"
            } else {
                "create"
            };
            action[key]["_index"].as_str().unwrap_or("").to_string()
        })
        .collect();
    indexes.sort();
    assert_eq!(
        indexes,
        vec!["events-a".to_string(), "events-b".to_string()]
    );
    mv02b_wait_success(&server, &token, "mv02c-es2-a", 2).await;
    mv02b_wait_success(&server, &token, "mv02c-es2-b", 2).await;

    drop(server);
    drop(registry);
    std::fs::remove_dir_all(&dir).ok();
}
