use super::*;

#[tokio::test]
async fn removed_schema_registry_routes_return_404() {
    // W3-37 owns `GET+POST /schema_registry` and
    // `DELETE /schema_registry/:name`. MV-15 owns the validation
    // list, create, replace and detail reads as honest markers; the
    // per-validation metrics sub-route stays removed.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, body) = server.get_auth("/api/v5/schema_validations", &token).await;
    assert_eq!(
        status, 200,
        "honest route GET /schema_validations must be 200"
    );
    assert_eq!(body, json!([]));
    for path in ["/api/v5/schema_validations/validation/x/metrics"] {
        let (status, _) = server.get_auth(path, &token).await;
        assert_eq!(status, 404, "removed route {path} must be 404");
    }
}

#[tokio::test]
async fn schema_registry_list_create_delete_round_trip() {
    // W3-37 broker test. Create one schema. List shows it.
    // Duplicate create fails. Delete works. Second delete is 404.
    // Each step uses the HTTP router. No step touches the store directly.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // Empty registry returns an empty page.
    let (status, body) = server.get_auth("/api/v5/schema_registry", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
    assert_eq!(body.as_array().map(Vec::len), Some(0));
    // Unknown query keys are ignored.
    let (status, _) = server
        .get_auth("/api/v5/schema_registry?unknown=param", &token)
        .await;
    assert_eq!(status, 200);
    // Bad kind fails with the documented shape.
    let (status, body) = server
        .post_auth(
            "/api/v5/schema_registry",
            json!({
                "name": "bad-kind",
                "type": "nope",
                "schema": {"type": "object"},
            }),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    // Bad shape fails with the documented shape.
    let (status, body) = server
        .post_auth(
            "/api/v5/schema_registry",
            json!({
                "name": "bad-shape",
                "type": "json",
                "schema": {"foo": 1},
            }),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    // Create one schema.
    let (status, body) = server
        .post_auth(
            "/api/v5/schema_registry",
            json!({
                "name": "w3-37-schema",
                "type": "json",
                "description": "temp reading",
                "schema": {"type": "object", "properties": {"temp": {"type": "number"}}},
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(body["name"], json!("w3-37-schema"));
    assert_eq!(body["type"], json!("json"));
    // List shows it with paging metadata.
    let (status, body) = server.get_auth("/api/v5/schema_registry", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    let names: Vec<String> = body
        .as_array()
        .expect("list data is an array")
        .iter()
        .filter_map(|row| row.get("name").and_then(|v| v.as_str()).map(str::to_string))
        .collect();
    assert!(names.contains(&"w3-37-schema".to_string()));
    // Duplicate create fails cleanly.
    let (status, body) = server
        .post_auth(
            "/api/v5/schema_registry",
            json!({
                "name": "w3-37-schema",
                "type": "json",
                "schema": {"type": "object"},
            }),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("ALREADY_EXISTS"));
    // Delete works. Second delete is 404.
    let (status, _) = server
        .delete_auth("/api/v5/schema_registry/w3-37-schema", &token)
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .delete_auth("/api/v5/schema_registry/w3-37-schema", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    // Store is empty again.
    let (status, body) = server.get_auth("/api/v5/schema_registry", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(0));
}

#[tokio::test]
async fn schema_registry_survives_restart() {
    // W3-37 persistence test. Register through the broker.
    // Reload the same dir. Read the schema back from the list.
    let dir = unique_data_dir("w3-37-schemas");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let (status, _) = server
        .post_auth(
            "/api/v5/schema_registry",
            json!({
                "name": "restart-schema",
                "type": "avro",
                "schema": {
                    "type": "record",
                    "name": "Reading",
                    "fields": [{"name": "id", "type": "string"}],
                },
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    drop(server);
    drop(registry);
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    let token = body["token"].as_str().expect("admin token").to_string();
    let (status, body) = server.get_auth("/api/v5/schema_registry", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    let names: Vec<String> = body
        .as_array()
        .expect("list data is an array")
        .iter()
        .filter_map(|row| row.get("name").and_then(|v| v.as_str()).map(str::to_string))
        .collect();
    assert!(names.contains(&"restart-schema".to_string()));
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn ap05_schema_registry_source_field_and_single_read() {
    // AP-05: the registry accepts the documented `source` text and
    // serves the single read. Old `schema` bodies still pass.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, created) = server
        .post_auth(
            "/api/v5/schema_registry",
            json!({
                "name": "ap05-schema",
                "type": "json",
                "source": "{\"type\": \"object\"}",
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["source"], json!("{\"type\":\"object\"}"));
    let (status, read) = server
        .get_auth("/api/v5/schema_registry/ap05-schema", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(read, created);
    let (status, missing) = server
        .get_auth("/api/v5/schema_registry/no-such", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(missing["code"], json!("NOT_FOUND"));
    let (status, _) = server
        .delete_auth("/api/v5/schema_registry/ap05-schema", &token)
        .await;
    assert_eq!(status, 204);
}
