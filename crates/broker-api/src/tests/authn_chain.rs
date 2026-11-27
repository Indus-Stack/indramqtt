use super::*;

#[tokio::test]
async fn authn_backends_mask_secrets_on_read_and_keep_them_on_marker_put() {
    #[cfg(test)]
    const SECRET: &str = "known-authn-secret-91ab";
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let bodies = [
        json!({"mechanism": "password_based", "backend": "mysql", "server": "127.0.0.1:3306",
                   "database": "mqtt", "username": "u", "password": SECRET,
                   "query": "SELECT password_hash FROM users WHERE username = ${username} LIMIT 1"}),
        json!({"mechanism": "password_based", "backend": "postgresql", "server": "127.0.0.1:5432",
                   "database": "mqtt", "username": "u", "password": SECRET,
                   "query": "SELECT password_hash FROM users WHERE username = $1 LIMIT 1"}),
        json!({"mechanism": "password_based", "backend": "mongodb", "mongo_type": "single",
                   "server": "127.0.0.1:27017", "database": "example", "collection": "users",
                   "username": "u", "password": SECRET}),
        json!({"mechanism": "password_based", "backend": "redis", "redis_type": "single",
                   "server": "127.0.0.1:6379", "cmd": "HMGET ${username} password_hash salt",
                   "password": SECRET}),
        json!({"mechanism": "password_based", "backend": "http", "method": "post",
                   "url": "http://127.0.0.1:18083", "headers": {"content-type": "application/json"},
                   "body": {"username": "${username}", "password": "${password}"},
                   "password": SECRET}),
        json!({"mechanism": "jwt", "algorithm": "hmac-based", "secret": SECRET,
                   "secret_base64_encoded": false, "use_jwks": false, "verify_claims": {}}),
    ];
    let raw_chain = || serde_json::to_string(&state.config.snapshot().authn_chain).unwrap();
    for body in bodies {
        let (status, created) = server
            .post_auth("/api/v5/authentication", body.clone(), &token)
            .await;
        assert_eq!(status, 200, "{body}: {created}");
        assert!(
            !created.to_string().contains(SECRET),
            "create shows a secret"
        );
        let id = created["id"].as_str().unwrap().to_string();
        let path = format!("/api/v5/authentication/{id}");
        let (status, shown) = server.get_auth(&path, &token).await;
        assert_eq!(status, 200);
        assert!(
            !shown.to_string().contains(SECRET),
            "{id} single shows a secret"
        );
        assert!(shown.to_string().contains("******"), "{id} shows no mask");
        let (_, listed) = server.get_auth("/api/v5/authentication", &token).await;
        assert!(!listed.to_string().contains(SECRET), "list shows a secret");
        let before = raw_chain();
        assert!(before.contains(SECRET), "{id} did not store the secret");
        // Send the read answer back: the mask keeps the stored secret.
        let (status, answer) = server.put_auth(&path, shown.clone(), &token).await;
        assert_eq!(status, 204, "{id}: {answer}");
        let after = raw_chain();
        assert_eq!(
            after.matches(SECRET).count(),
            before.matches(SECRET).count(),
            "{id} lost its secret on the marker PUT"
        );
        assert!(!after.contains("******"), "{id} stored the mask");
    }
    let (_, snapshot) = server.get_auth("/api/v5/configs", &token).await;
    assert!(!snapshot.to_string().contains(SECRET));
}

#[tokio::test]
async fn authn_chain_list_create_round_trip() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Empty chain reads as an empty collection, not an error.
    let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
    assert_eq!(body.as_array().map(Vec::len), Some(0));

    // Create one built-in entry.
    let (status, created) = server
        .post_auth(
            "/api/v5/authentication",
            json!({"mechanism": "password_based", "backend": "built_in_database"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(created["id"], json!("password_based:built_in_database"));
    assert_eq!(created["mechanism"], json!("password_based"));
    assert_eq!(created["backend"], json!("built_in_database"));
    assert_eq!(created["enable"], json!(true));
    // No invented fields: the documented slot fields plus the
    // documented built-in default.
    let obj = created.as_object().expect("entry is an object");
    for key in obj.keys() {
        assert!(
            [
                "id",
                "mechanism",
                "backend",
                "enable",
                "config",
                "user_id_type"
            ]
            .contains(&key.as_str()),
            "unexpected field {key} in chain entry"
        );
    }
    assert_eq!(created["user_id_type"], json!("username"));

    // List again: the entry is present in order.
    let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
    assert_eq!(status, 200);
    let data = body.as_array().expect("data is a list");
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["id"], json!("password_based:built_in_database"));
    assert_eq!(body.as_array().map(Vec::len), Some(1));

    // Re-creating the same id is a clean duplicate rejection, never
    // a silent overwrite.
    let (status, body) = server
        .post_auth(
            "/api/v5/authentication",
            json!({"mechanism": "password_based", "backend": "built_in_database"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("ALREADY_EXISTS"));
    assert!(body["message"].is_string());

    // Malformed bodies are client errors with the documented shape.
    let (status, body) = server
        .post_auth(
            "/api/v5/authentication",
            json!({"mechanism": "password_based"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    assert!(body["message"].is_string());

    // Unknown backends are rejected, never stored.
    let (status, body) = server
        .post_auth(
            "/api/v5/authentication",
            json!({"mechanism": "password_based", "backend": "nope"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));

    // Second entry keeps insertion order.
    let (status, _) = server
        .post_auth(
            "/api/v5/authentication",
            json!({"mechanism": "password_based", "backend": "mysql"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
    assert_eq!(status, 200);
    let data = body.as_array().expect("data is a list");
    assert_eq!(data.len(), 2);
    assert_eq!(data[1]["id"], json!("password_based:mysql"));
}

#[tokio::test]
async fn authn_chain_drives_connect_and_survives_restart() {
    // Broker consult through the console CONNECT path: an empty chain
    // preserves today's open behaviour, while a chain with no live
    // backend fails closed without creating a session.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let mut anon = WsClient::connect(server.port).await;
    anon.send_bin(&mqtt_connect("w2-02-open", None, None)).await;
    let connack = anon.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    let (status, _) = server
        .post_auth(
            "/api/v5/authentication",
            json!({"mechanism": "password_based", "backend": "mysql"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let mut refused = WsClient::connect(server.port).await;
    refused
        .send_bin(&mqtt_connect("w2-02-closed", None, None))
        .await;
    let connack = refused.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0x04);

    // Restart persistence through the config registry: the chain
    // survives a rebuild from the same data dir.
    let dir = unique_data_dir("authn-chain");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let (status, _) = server
        .post_auth(
            "/api/v5/authentication",
            json!({"mechanism": "password_based", "backend": "built_in_database"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    drop(server);
    drop(registry);
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;
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
    let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
    assert_eq!(status, 200);
    let data = body.as_array().expect("data is a list");
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["id"], json!("password_based:built_in_database"));
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn authn_chain_order_replace_round_trip_and_rejects_unknown() {
    // Order-replace through the broker: create two entries, set an
    // explicit order, list expecting the new order; an unknown id
    // and a partial list are rejected with no change applied.
    // Management-plane only; publish and deliver never touch this
    // store, and the CONNECT path keeps reading its lock-free
    // snapshot.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
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
            "/api/v5/authentication",
            json!({"mechanism": "password_based", "backend": "mysql"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);

    // Explicit order applies: mysql first.
    let (status, _) = server
        .put_auth(
            "/api/v5/authentication/order",
            json!([{"id": "password_based:mysql"}, {"id": "password_based:built_in_database"}]),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
    assert_eq!(status, 200);
    let data = body.as_array().expect("data is a list");
    assert_eq!(data.len(), 2);
    assert_eq!(data[0]["id"], json!("password_based:mysql"));
    assert_eq!(data[1]["id"], json!("password_based:built_in_database"));

    // Unknown ids are rejected with the documented shape and apply
    // nothing.
    let (status, body) = server
        .put_auth(
            "/api/v5/authentication/order",
            json!([
                {"id": "password_based:mysql"},
                {"id": "password_based:built_in_database"},
                {"id": "password_based:nowhere"},
            ]),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    assert!(body["message"].is_string());
    let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
    assert_eq!(status, 200);
    let data = body.as_array().expect("data is a list");
    assert_eq!(data[0]["id"], json!("password_based:mysql"));
    assert_eq!(data[1]["id"], json!("password_based:built_in_database"));

    // Partial lists are rejected with no change applied.
    let (status, body) = server
        .put_auth(
            "/api/v5/authentication/order",
            json!([{"id": "password_based:mysql"}]),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
    assert_eq!(status, 200);
    let data = body.as_array().expect("data is a list");
    assert_eq!(data.len(), 2);
    assert_eq!(data[0]["id"], json!("password_based:mysql"));

    // Malformed bodies are client errors with the documented shape.
    let (status, body) = server
        .put_auth(
            "/api/v5/authentication/order",
            json!([{"not-id": "password_based:mysql"}]),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));

    // Order persists across a restart through the config registry.
    let dir = unique_data_dir("authn-order");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
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
            "/api/v5/authentication",
            json!({"mechanism": "password_based", "backend": "mysql"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = server
        .put_auth(
            "/api/v5/authentication/order",
            json!([{"id": "password_based:mysql"}, {"id": "password_based:built_in_database"}]),
            &token,
        )
        .await;
    assert_eq!(status, 204);
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
    let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
    assert_eq!(status, 200);
    let data = body.as_array().expect("data is a list");
    assert_eq!(data.len(), 2);
    assert_eq!(data[0]["id"], json!("password_based:mysql"));
    assert_eq!(data[1]["id"], json!("password_based:built_in_database"));
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn authn_entry_read_update_delete_round_trip() {
    // Per-id read/update/delete through the broker management plane
    // on the real chain store: create two entries, read one back,
    // update with a valid body and read again, delete then read 404,
    // delete unknown 404, invalid updates rejected without applying,
    // deleting the last entry refused, and delete-then-recreate of
    // the same id works. Management-plane only; the CONNECT path
    // keeps reading its lock-free snapshot and publish/deliver never
    // touch this store.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
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
            "/api/v5/authentication",
            json!({"mechanism": "password_based", "backend": "mysql"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);

    // Read one entry by id; the shape carries only the stored slot
    // fields, never invented measurements.
    let (status, body) = server
        .get_auth("/api/v5/authentication/password_based:mysql", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["id"], json!("password_based:mysql"));
    assert_eq!(body["mechanism"], json!("password_based"));
    assert_eq!(body["backend"], json!("mysql"));
    assert_eq!(body["enable"], json!(true));
    let obj = body.as_object().expect("entry is an object");
    for key in obj.keys() {
        assert!(
            ["id", "mechanism", "backend", "enable", "config"].contains(&key.as_str()),
            "unexpected field {key} in chain entry"
        );
    }

    // Unknown ids miss with the documented shape.
    let (status, body) = server
        .get_auth("/api/v5/authentication/password_based:nowhere", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    assert!(body["message"].is_string());

    // Valid update replaces and reads back.
    let (status, _) = server
        .put_auth(
            "/api/v5/authentication/password_based:mysql",
            json!({"enable": false, "config": {"pool_size": 4}}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/authentication/password_based:mysql", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["enable"], json!(false));
    assert_eq!(body["config"], json!({"pool_size": 4}));

    // Invalid updates are client errors and apply nothing.
    for bad in [
        json!({"backend": "nope"}),
        json!({"mechanism": "nope"}),
        json!({"enable": "yes"}),
        json!({"config": "yes"}),
        json!({"id": "password_based:other"}),
        json!([1, 2]),
    ] {
        let (status, body) = server
            .put_auth("/api/v5/authentication/password_based:mysql", bad, &token)
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(body["message"].is_string());
    }
    // Unknown ids fail updates with 404 even for valid bodies.
    let (status, body) = server
        .put_auth(
            "/api/v5/authentication/password_based:nowhere",
            json!({"enable": false}),
            &token,
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    let (status, body) = server
        .get_auth("/api/v5/authentication/password_based:mysql", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["backend"], json!("mysql"));
    assert_eq!(body["enable"], json!(false));

    // Delete removes and reports; the deleted id reads 404 after.
    let (status, _) = server
        .delete_auth("/api/v5/authentication/password_based:mysql", &token)
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/authentication/password_based:mysql", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));

    // Deleting an unknown id is 404.
    let (status, body) = server
        .delete_auth("/api/v5/authentication/password_based:nowhere", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));

    // Deleting then re-creating the same id works (no tombstone).
    let (status, recreated) = server
        .post_auth(
            "/api/v5/authentication",
            json!({"mechanism": "password_based", "backend": "mysql"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(recreated["id"], json!("password_based:mysql"));

    // Deleting the last remaining entry is refused instead of
    // leaving authentication open.
    let (status, _) = server
        .delete_auth(
            "/api/v5/authentication/password_based:built_in_database",
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .delete_auth("/api/v5/authentication/password_based:mysql", &token)
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    assert!(body["message"].is_string());
    let (status, body) = server
        .get_auth("/api/v5/authentication/password_based:mysql", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["id"], json!("password_based:mysql"));
}

#[tokio::test]
async fn authn_entry_update_delete_persist_across_restart() {
    // Per-id update/delete persist through the config registry: a
    // validated update and a removal survive a rebuild from the same
    // data dir. Management-plane only; publish and deliver never
    // touch this store.
    let dir = unique_data_dir("authn-entry");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
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
            "/api/v5/authentication",
            json!({"mechanism": "password_based", "backend": "mysql"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = server
        .put_auth(
            "/api/v5/authentication/password_based:mysql",
            json!({"enable": false}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/authentication/password_based:mysql", &token)
        .await;
    assert_eq!(status, 204);
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
    let (status, body) = server
        .get_auth(
            "/api/v5/authentication/password_based:built_in_database",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["id"], json!("password_based:built_in_database"));
    let (status, _) = server
        .get_auth("/api/v5/authentication/password_based:mysql", &token)
        .await;
    assert_eq!(status, 404);
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn authn_position_move_round_trip_rejects_out_of_range_and_persists() {
    // Position-move through the broker management plane on the real
    // chain store: create three entries, move the last to position 0,
    // then list expecting the new order; an out-of-range position is
    // rejected with the documented shape and moves nothing. Unknown
    // ids miss with 404, malformed positions fail with 400, and the
    // new order survives a restart. Management-plane only: one short
    // write lock per move; the CONNECT path keeps reading its
    // lock-free snapshot and publish/deliver never touch this store.
    // Before this task the route itself was missing (bare 404).
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    for backend in ["built_in_database", "mysql", "redis"] {
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": backend}),
                &token,
            )
            .await;
        assert_eq!(status, 200, "create {backend}");
    }

    // Move the last entry to the front. The answer is 204 with no
    // body, as the reference API documents.
    let (status, body) = server
        .send(
            "PUT",
            "/api/v5/authentication/password_based:redis/position/0",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    assert!(body.is_null(), "move returns no body, got: {body}");
    let reported: Vec<String> = vec![
        "password_based:redis".to_string(),
        "password_based:built_in_database".to_string(),
        "password_based:mysql".to_string(),
    ];
    // List shows the new order.
    let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
    assert_eq!(status, 200);
    let listed: Vec<String> = body
        .as_array()
        .expect("chain list is paged")
        .iter()
        .map(|entry| entry["id"].as_str().expect("id").to_string())
        .collect();
    assert_eq!(listed, reported);

    // An out-of-range position is a client error and moves nothing.
    let (status, body) = server
        .send(
            "PUT",
            "/api/v5/authentication/password_based:redis/position/99",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    assert!(body["message"].is_string());
    let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
    assert_eq!(status, 200);
    let unchanged: Vec<String> = body
        .as_array()
        .expect("chain list")
        .iter()
        .map(|entry| entry["id"].as_str().expect("id").to_string())
        .collect();
    assert_eq!(unchanged, reported);

    // Unknown ids miss with the documented shape and move nothing.
    let (status, body) = server
        .send(
            "PUT",
            "/api/v5/authentication/password_based:nowhere/position/0",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    assert!(body["message"].is_string());

    // Malformed positions are client errors and move nothing.
    for bad in ["front", "-1", "1.5"] {
        let (status, body) = server
            .send(
                "PUT",
                &format!("/api/v5/authentication/password_based:redis/position/{bad}"),
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 400, "position {bad} must be rejected");
        assert_eq!(body["code"], json!("BAD_REQUEST"));
    }
    let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
    assert_eq!(status, 200);
    let unchanged: Vec<String> = body
        .as_array()
        .expect("chain list")
        .iter()
        .map(|entry| entry["id"].as_str().expect("id").to_string())
        .collect();
    assert_eq!(unchanged, reported);

    // Persistence through the config registry: the moved order
    // survives a rebuild from the same data dir.
    let dir = unique_data_dir("authn-position");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    for backend in ["built_in_database", "mysql", "redis"] {
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": backend}),
                &token,
            )
            .await;
        assert_eq!(status, 200, "persist case create {backend}");
    }
    let (status, _) = server
        .send(
            "PUT",
            "/api/v5/authentication/password_based:redis/position/0",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
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
    let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
    assert_eq!(status, 200);
    let listed: Vec<String> = body
        .as_array()
        .expect("reloaded chain list")
        .iter()
        .map(|entry| entry["id"].as_str().expect("id").to_string())
        .collect();
    assert_eq!(
        listed,
        vec![
            "password_based:redis".to_string(),
            "password_based:built_in_database".to_string(),
            "password_based:mysql".to_string(),
        ]
    );
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn authn_status_reports_live_counters_through_connect() {
    // Status over the real chain entry plus live CONNECT verdicts:
    // create one built-in entry, read the documented fields at zero,
    // move success/failed through real console CONNECTs through the
    // broker, and read the moved counters back. A subscribe/publish/
    // deliver flow proves status assembly never touches the message
    // path. Management-plane only for reads; the CONNECT path
    // attributes one verdict per connect and publish/deliver never
    // touch these counters.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Unknown ids miss with the documented shape, not an empty body.
    // Before this task the route itself was missing (bare 404).
    let (status, body) = server
        .get_auth(
            "/api/v5/authentication/password_based:nowhere/status",
            &token,
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    assert!(body["message"].is_string());

    // One built-in entry: a fresh authenticator reads zeros live.
    let (status, _) = server
        .post_auth(
            "/api/v5/authentication",
            json!({"mechanism": "password_based", "backend": "built_in_database"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, body) = server
        .get_auth(
            "/api/v5/authentication/password_based:built_in_database/status",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["id"], json!("password_based:built_in_database"));
    assert_eq!(body["enable"], json!(true));
    assert!(body.get("status").is_none());
    assert!(body.get("node_status").is_none());
    assert_eq!(body["metrics"]["success"], json!(0));
    assert_eq!(body["metrics"]["failed"], json!(0));
    assert_eq!(body["metrics"]["total"], json!(0));
    assert!(body["metrics"]["nomatch"].is_number());
    assert!(body["node_metrics"].is_object());
    assert!(body["node_resource_metrics"].is_object());
    assert!(body["node_error"].is_object());
    assert_eq!(
        body["node_metrics"]["metrics"]["total"],
        body["metrics"]["total"]
    );
    assert_eq!(
        body["node_resource_metrics"]["metrics"]["matched"],
        body["metrics"]["total"]
    );
    assert!(body["node_error"]["node"].is_string());

    // Create a user through the broker, then CONNECT through the
    // broker: success moves the live counters.
    let (status, _) = server
        .post_auth(
            "/api/v5/authentication/password_based:built_in_database/users",
            json!({"user_id": "w2-08-user", "password": "W2-08-pass!"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let mut good = WsClient::connect(server.port).await;
    good.send_bin(&mqtt_connect(
        "w2-08-good",
        Some("w2-08-user"),
        Some(b"W2-08-pass!"),
    ))
    .await;
    let connack = good.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    let (status, body) = server
        .get_auth(
            "/api/v5/authentication/password_based:built_in_database/status",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["metrics"]["success"], json!(1));
    assert_eq!(body["metrics"]["total"], json!(1));

    // A bad password moves failed through the same broker path.
    let mut bad = WsClient::connect(server.port).await;
    bad.send_bin(&mqtt_connect(
        "w2-08-bad",
        Some("w2-08-user"),
        Some(b"wrong-pass"),
    ))
    .await;
    let connack = bad.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0x04);
    let (status, body) = server
        .get_auth(
            "/api/v5/authentication/password_based:built_in_database/status",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["metrics"]["success"], json!(1));
    assert_eq!(body["metrics"]["failed"], json!(1));
    assert_eq!(body["metrics"]["total"], json!(2));

    // Unknown query keys degrade to the same read, not a 400: the
    // route is not paged.
    let (status, body) = server
        .get_auth(
            "/api/v5/authentication/password_based:built_in_database/status?unknown_param=1",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["metrics"]["total"], json!(2));

    // The message path still flows while status reads run: status
    // assembly never touches publish or deliver.
    let mut sub = WsClient::connect(server.port).await;
    sub.send_bin(&mqtt_connect(
        "w2-08-sub",
        Some("w2-08-user"),
        Some(b"W2-08-pass!"),
    ))
    .await;
    let connack = sub.recv_msg().await.expect("connack");
    assert_eq!(connack[3], 0);
    sub.send_bin(&mqtt_subscribe(7, &[("w2-08/t", 1)])).await;
    let suback = sub.recv_msg().await.expect("suback");
    assert_eq!(mqtt_packet_type(&suback), 9);
    let mut publ = WsClient::connect(server.port).await;
    publ.send_bin(&mqtt_connect(
        "w2-08-pub",
        Some("w2-08-user"),
        Some(b"W2-08-pass!"),
    ))
    .await;
    let connack = publ.recv_msg().await.expect("connack");
    assert_eq!(connack[3], 0);
    publ.send_bin(&mqtt_publish("w2-08/t", 42, 1, b"hi-w2-08"))
        .await;
    let puback = publ.recv_msg().await.expect("puback");
    assert_eq!(mqtt_packet_type(&puback), 4);
    let delivery = sub.recv_msg().await.expect("delivery");
    assert_eq!(mqtt_packet_type(&delivery), 3);
    assert!(delivery.ends_with(b"hi-w2-08"));
    // Two more successful CONNECTs moved the counters with the flow.
    let (status, body) = server
        .get_auth(
            "/api/v5/authentication/password_based:built_in_database/status",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["metrics"]["success"], json!(3));
    assert_eq!(body["metrics"]["failed"], json!(1));
    assert_eq!(body["metrics"]["total"], json!(4));
}

#[tokio::test]
async fn authn_metrics_reset_zeroes_live_counters_through_connect() {
    // Reset over the real chain entry plus live CONNECT verdicts:
    // create one built-in entry and a user, move success/failed
    // through real console CONNECTs through the broker, reset, then
    // read zeroed counters back through status. Counters resume from
    // zero on the next authentication. Unknown ids miss with the
    // documented shape, not an empty body. Management-plane only for
    // the reset; publish and deliver never touch these counters.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Before this task the route itself was missing (bare 404 with
    // no JSON verdict).
    let (status, body) = server
        .send(
            "POST",
            "/api/v5/authentication/password_based:nowhere/metrics/reset",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    assert!(body["message"].is_string());

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
            json!({"user_id": "w2-09-user", "password": "W2-09-pass!"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    // One success and one failure through the broker move the live
    // counters.
    let mut good = WsClient::connect(server.port).await;
    good.send_bin(&mqtt_connect(
        "w2-09-good",
        Some("w2-09-user"),
        Some(b"W2-09-pass!"),
    ))
    .await;
    let connack = good.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    let mut bad = WsClient::connect(server.port).await;
    bad.send_bin(&mqtt_connect(
        "w2-09-bad",
        Some("w2-09-user"),
        Some(b"wrong-pass"),
    ))
    .await;
    let connack = bad.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0x04);
    let (status, body) = server
        .get_auth(
            "/api/v5/authentication/password_based:built_in_database/status",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["metrics"]["success"], json!(1));
    assert_eq!(body["metrics"]["failed"], json!(1));
    assert_eq!(body["metrics"]["total"], json!(2));

    // Reset zeroes every counter; the next status read reports zeros.
    let (status, _) = server
        .send(
            "POST",
            "/api/v5/authentication/password_based:built_in_database/metrics/reset",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth(
            "/api/v5/authentication/password_based:built_in_database/status",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["metrics"]["success"], json!(0));
    assert_eq!(body["metrics"]["failed"], json!(0));
    assert_eq!(body["metrics"]["nomatch"], json!(0));
    assert_eq!(body["metrics"]["total"], json!(0));

    // Counters resume from zero: one more success through the broker
    // counts up from zero, and a message still flows while the reset
    // path stays off publish and deliver.
    let mut again = WsClient::connect(server.port).await;
    again
        .send_bin(&mqtt_connect(
            "w2-09-again",
            Some("w2-09-user"),
            Some(b"W2-09-pass!"),
        ))
        .await;
    let connack = again.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    again.send_bin(&mqtt_subscribe(7, &[("w2-09/t", 1)])).await;
    let suback = again.recv_msg().await.expect("suback");
    assert_eq!(mqtt_packet_type(&suback), 9);
    again
        .send_bin(&mqtt_publish("w2-09/t", 42, 1, b"hi-w2-09"))
        .await;
    let puback = again.recv_msg().await.expect("puback");
    assert_eq!(mqtt_packet_type(&puback), 4);
    let (status, body) = server
        .get_auth(
            "/api/v5/authentication/password_based:built_in_database/status",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["metrics"]["success"], json!(1));
    assert_eq!(body["metrics"]["failed"], json!(0));
    assert_eq!(body["metrics"]["total"], json!(1));
}

#[tokio::test]
async fn authn_node_cache_status_reset_round_trip() {
    // Status over real state plus a reset that actually evicts:
    // read the documented fields, populate the cache through
    // authentications through the broker, reset, then read an empty
    // cache. Management-plane only; publish and deliver never touch
    // the store.
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    state
        .auth
        .add_user("w2-03-user", b"W2-03-pass!")
        .expect("test user persists");

    // Empty cache reads as size zero, never an error.
    let (status, body) = server
        .get_auth("/api/v5/authentication/node_cache/status", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["enabled"], json!(true));
    assert_eq!(body["size"], json!(0));
    assert_eq!(body["count"], json!(0));
    assert!(body["max_size"].is_number());
    assert!(body["max_count"].is_number());

    // Populate through a real credentialed CONNECT through the
    // broker (console path authenticates against the same store).
    let mut client = WsClient::connect(server.port).await;
    client
        .send_bin(&mqtt_connect(
            "w2-03-cache-1",
            Some("w2-03-user"),
            Some(b"W2-03-pass!"),
        ))
        .await;
    let connack = client.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    let (status, body) = server
        .get_auth("/api/v5/authentication/node_cache/status", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["enabled"], json!(true));
    assert_eq!(body["size"], json!(1));
    assert_eq!(body["count"], json!(1));

    // Unknown query keys are ignored, not a 400.
    let (status, body) = server
        .get_auth(
            "/api/v5/authentication/node_cache/status?unknown_param=1",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["size"], json!(1));

    // Reset evicts; the next read is empty. Resetting again still
    // succeeds (204).
    let (status, _) = server
        .post_auth("/api/v5/authentication/node_cache/reset", json!({}), &token)
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/authentication/node_cache/status", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["size"], json!(0));
    assert_eq!(body["count"], json!(0));
    let (status, _) = server
        .post_auth("/api/v5/authentication/node_cache/reset", json!({}), &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn authn_settings_replace_round_trip_drives_connect_and_survives_restart() {
    // Settings round trip through the broker: GET defaults, PUT a
    // full valid update, GET expecting the update; a console CONNECT
    // still succeeds (the settings consult never breaks the open
    // broker) and a subscribe/publish/deliver flow proves publish
    // and deliver never touch this store. An invalid PUT is rejected
    // with no change; the node-cache subscriber keeps the status cap
    // in sync. Settings persist across a restart via the registry.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Defaults read back with the documented fields.
    let (status, body) = server
        .get_auth("/api/v5/authentication/settings", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["ignore_backend_failures"], json!(false));
    assert_eq!(body["node_cache"]["enable"], json!(true));
    assert_eq!(body["node_cache"]["max_count"], json!(10000));
    assert_eq!(body["builtin_record_count_refresh_interval"], json!("1h"));

    // Full valid replace through the broker.
    let (status, _) = server
        .put_auth(
            "/api/v5/authentication/settings",
            json!({
                "ignore_backend_failures": true,
                "node_cache": {
                    "enable": true,
                    "cache_ttl": "30s",
                    "cleanup_interval": "1m",
                    "stat_update_interval": "5s",
                    "max_count": 5000,
                    "max_memory": "100MB",
                },
                "builtin_record_count_refresh_interval": "30m",
            }),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/authentication/settings", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["ignore_backend_failures"], json!(true));
    assert_eq!(body["node_cache"]["max_count"], json!(5000));
    assert_eq!(body["node_cache"]["cache_ttl"], json!("30s"));
    assert_eq!(body["builtin_record_count_refresh_interval"], json!("30m"));

    // The subscriber keeps the node-cache status cap in sync with
    // the settings' cache half without a restart.
    let (status, body) = server
        .get_auth("/api/v5/authentication/node_cache/status", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["max_count"], json!(5000));

    // The CONNECT consult never breaks the open broker: a console
    // CONNECT still succeeds, and a subscribe/publish/deliver flow
    // proves publish and deliver never touch this store.
    let mut sub = WsClient::connect(server.port).await;
    sub.send_bin(&mqtt_connect("w2-05-sub", None, None)).await;
    let connack = sub.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    sub.send_bin(&mqtt_subscribe(7, &[("w2-05/t", 1)])).await;
    let suback = sub.recv_msg().await.expect("suback");
    assert_eq!(mqtt_packet_type(&suback), 9);
    let mut publ = WsClient::connect(server.port).await;
    publ.send_bin(&mqtt_connect("w2-05-pub", None, None)).await;
    let connack = publ.recv_msg().await.expect("connack");
    assert_eq!(connack[3], 0);
    publ.send_bin(&mqtt_publish("w2-05/t", 42, 1, b"hi-w2-05"))
        .await;
    let puback = publ.recv_msg().await.expect("puback");
    assert_eq!(mqtt_packet_type(&puback), 4);
    let delivery = sub.recv_msg().await.expect("delivery");
    assert_eq!(mqtt_packet_type(&delivery), 3);
    assert!(delivery.ends_with(b"hi-w2-05"));

    // Unknown fields are rejected fail-closed with no change applied.
    let (status, body) = server
        .put_auth(
            "/api/v5/authentication/settings",
            json!({"ignore_backend_failures": true, "bogus": 1}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, body) = server
        .put_auth(
            "/api/v5/authentication/settings",
            json!({"node_cache": {"max_count": 0}}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, body) = server
        .get_auth("/api/v5/authentication/settings", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["node_cache"]["max_count"], json!(5000));

    // Partial bodies replace: missing fields reset to defaults.
    let (status, _) = server
        .put_auth(
            "/api/v5/authentication/settings",
            json!({"ignore_backend_failures": false}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/authentication/settings", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["ignore_backend_failures"], json!(false));
    assert_eq!(body["node_cache"]["max_count"], json!(10000));

    // Persistence across a restart through the config registry.
    let dir = unique_data_dir("authn-settings");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let (status, _) = server
        .put_auth(
            "/api/v5/authentication/settings",
            json!({
                "ignore_backend_failures": true,
                "node_cache": {
                    "enable": true,
                    "cache_ttl": "30s",
                    "cleanup_interval": "1m",
                    "stat_update_interval": "5s",
                    "max_count": 5000,
                    "max_memory": "100MB",
                },
                "builtin_record_count_refresh_interval": "30m",
            }),
            &token,
        )
        .await;
    assert_eq!(status, 204);
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
    let (status, body) = server
        .get_auth("/api/v5/authentication/settings", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["ignore_backend_failures"], json!(true));
    assert_eq!(body["node_cache"]["max_count"], json!(5000));
    assert_eq!(body["builtin_record_count_refresh_interval"], json!("30m"));
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn authn_users_list_has_no_invented_fields() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // W2-11: the user routes address one authenticator by id, so the
    // entry must exist first (unknown ids are 404); same setup as the
    // import test below.
    let (status, _) = server
        .post_auth(
            "/api/v5/authentication",
            json!({"mechanism": "password_based", "backend": "built_in_database"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, created) = server
        .post_auth(
            "/api/v5/authentication/password_based:built_in_database/users",
            json!({"user_id": "mqtt-alice", "password": "s3cret"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created, json!({"user_id": "mqtt-alice"}));
    let (status, body) = server
        .get_auth(
            "/api/v5/authentication/password_based:built_in_database/users",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let data = body["data"].as_array().expect("data is a list");
    assert_eq!(data.len(), 1);
    assert_eq!(data[0], json!({"user_id": "mqtt-alice"}));
}

#[tokio::test]
async fn authn_import_users_batch_round_trip_rejects_bad_and_duplicate() {
    // W2-07 failing test: import two users to a fresh authenticator,
    // list expecting both; re-import one duplicate expecting clean
    // rejection with the store intact. Before: 404 (no route).
    // Management-plane only: the import locks only the user map
    // (`crates/broker-auth/src/lib.rs`, `MemoryAuth::import_users`);
    // the broker event driving it is the management POST, and CONNECT
    // consults the same map
    // (`crates/broker-node/src/main.rs`, CONNECT handling).
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, _) = server
        .post_auth(
            "/api/v5/authentication",
            json!({"mechanism": "password_based", "backend": "built_in_database"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);

    let (status, body) = server
        .post_auth(
            "/api/v5/authentication/password_based:built_in_database/import_users",
            json!([
                {"user_id": "w2-07-a", "password": "Imp0rt-a!"},
                {"user_id": "w2-07-b", "password": "Imp0rt-b!"},
            ]),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["total"], json!(2));
    assert_eq!(body["success"], json!(2));

    let (status, body) = server
        .get_auth(
            "/api/v5/authentication/password_based:built_in_database/users",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let data = body["data"].as_array().expect("data is a list");
    assert!(data.contains(&json!({"user_id": "w2-07-a"})));
    assert!(data.contains(&json!({"user_id": "w2-07-b"})));

    // The imported credentials authenticate through the broker state:
    // `MemoryAuth::authenticate` accepts the new password and refuses
    // a wrong one (the same call the kernel CONNECT path makes).
    assert!(state
        .auth
        .authenticate("w2-07-c1", Some("w2-07-a"), Some(b"Imp0rt-a!"))
        .await
        .is_ok());
    assert!(state
        .auth
        .authenticate("w2-07-c1", Some("w2-07-a"), Some(b"wrong"))
        .await
        .is_err());

    // Re-import with one duplicate: clean rejection, store intact.
    let (status, body) = server
        .post_auth(
            "/api/v5/authentication/password_based:built_in_database/import_users",
            json!([
                {"user_id": "w2-07-b", "password": "Imp0rt-b!"},
                {"user_id": "w2-07-c", "password": "Imp0rt-c!"},
            ]),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    assert!(body["message"].is_string());
    let (status, body) = server
        .get_auth(
            "/api/v5/authentication/password_based:built_in_database/users",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let data = body["data"].as_array().expect("data is a list");
    assert_eq!(data.len(), 2);
    assert!(!data.contains(&json!({"user_id": "w2-07-c"})));

    // Duplicate within the batch is rejected the same way.
    let (status, body) = server
        .post_auth(
            "/api/v5/authentication/password_based:built_in_database/import_users",
            json!([
                {"user_id": "w2-07-d", "password": "Imp0rt-d!"},
                {"user_id": "w2-07-d", "password": "Imp0rt-d!"},
            ]),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));

    // Malformed batches are client errors with the documented shape
    // and apply nothing.
    for bad in [
        json!([]),
        json!([{"user_id": "w2-07-e"}]),
        json!([{"user_id": "", "password": "Imp0rt-e!"}]),
        json!([{"user_id": "w2-07-e", "password": ""}]),
        json!({"not-users": []}),
    ] {
        let (status, body) = server
            .post_auth(
                "/api/v5/authentication/password_based:built_in_database/import_users",
                bad,
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(body["message"].is_string());
    }
    let (status, body) = server
        .get_auth(
            "/api/v5/authentication/password_based:built_in_database/users",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["data"].as_array().expect("data").len(), 2);

    // Unknown authenticator ids are 404 with the documented code,
    // not an empty body.
    let (status, body) = server
        .post_auth(
            "/api/v5/authentication/password_based:nowhere/import_users",
            json!([{"user_id": "w2-07-x", "password": "Imp0rt-x!"}]),
            &token,
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    assert!(body["message"].is_string());

    // Non-built-in backends cannot be imported into the built-in map.
    let (status, _) = server
        .post_auth(
            "/api/v5/authentication",
            json!({"mechanism": "password_based", "backend": "mysql"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, body) = server
        .post_auth(
            "/api/v5/authentication/password_based:mysql/import_users",
            json!([{"user_id": "w2-07-y", "password": "Imp0rt-y!"}]),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
}

#[tokio::test]
async fn authn_imported_users_connect_and_persist() {
    // Imported users persist across a restart and authenticate a real
    // broker CONNECT through the console path (connect, not only the
    // store): the imported credential gets CONNACK 0 over the live
    // WebSocket console, a wrong password does not.
    let dir = unique_data_dir("authn-import");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
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
            "/api/v5/authentication/password_based:built_in_database/import_users",
            json!([
                {"user_id": "w2-07-persist", "password": "Persist1!"},
            ]),
            &token,
        )
        .await;
    assert_eq!(status, 200);

    let mut ok = WsClient::connect(server.port).await;
    ok.send_bin(&mqtt_connect(
        "w2-07-persist-ok",
        Some("w2-07-persist"),
        Some(b"Persist1!"),
    ))
    .await;
    let connack = ok.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    drop(ok);

    let mut bad = WsClient::connect(server.port).await;
    bad.send_bin(&mqtt_connect(
        "w2-07-persist-bad",
        Some("w2-07-persist"),
        Some(b"wrong"),
    ))
    .await;
    let connack = bad.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_ne!(connack[3], 0);
    drop(bad);
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
    let (status, body) = server
        .get_auth(
            "/api/v5/authentication/password_based:built_in_database/users",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let data = body["data"].as_array().expect("data is a list");
    assert!(data.contains(&json!({"user_id": "w2-07-persist"})));
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}
