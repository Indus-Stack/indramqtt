use super::*;

/// The secret of a key from `POST /api_key` works as the password of
/// Basic authentication (user name: the `api_key` identifier) and as
/// a bearer credential. The scopes of the key limit the paths, the
/// operations that the reference lists for a bearer token only refuse
/// a key, and a removed, disabled or expired key does not work.
#[tokio::test]
async fn an_api_key_authenticates_with_its_scopes() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (scoped_id, scoped) = make_key(
        &server,
        &token,
        json!({"name": "scoped", "scopes": ["monitoring"]}),
    )
    .await;
    let (full_id, full) = make_key(&server, &token, json!({"name": "full"})).await;

    // The scoped key can read its group and nothing else, with Basic
    // and with Bearer.
    let (status, _) = server.get_auth("/api/v5/stats", &scoped).await;
    assert_eq!(status, 200);
    let (status, _) = server.get_basic("/api/v5/stats", &scoped_id, &scoped).await;
    assert_eq!(status, 200);
    let (status, body) = server.get_auth("/api/v5/clients", &scoped).await;
    assert_eq!(status, 403, "another scope: {body}");
    assert_eq!(body["code"], json!("FORBIDDEN"));
    let (status, _) = server
        .get_basic("/api/v5/clients", &scoped_id, &scoped)
        .await;
    assert_eq!(status, 403);

    // The key with no scope list can call each group.
    let (status, _) = server.get_basic("/api/v5/stats", &full_id, &full).await;
    assert_eq!(status, 200);
    let (status, _) = server.get_basic("/api/v5/clients", &full_id, &full).await;
    assert_eq!(status, 200);

    // Basic needs the identifier and the secret together.
    let (status, _) = server
        .get_basic("/api/v5/stats", &full_id, "wrong-secret")
        .await;
    assert_eq!(status, 401);
    let (status, _) = server.get_basic("/api/v5/stats", &scoped_id, &full).await;
    assert_eq!(status, 401, "the secret of another key");
    let (status, _) = server.get_basic("/api/v5/stats", "full", &full).await;
    assert_eq!(status, 401, "the name is not the identifier");
    let (status, _) = server.get_auth("/api/v5/stats", "scoped").await;
    assert_eq!(status, 401, "the name is not a secret");

    // The reference lists these operations for a bearer token only:
    // no key can call them, whatever the scheme.
    for (key_id, secret) in [(&scoped_id, &scoped), (&full_id, &full)] {
        for path in [
            "/api/v5/api_key",
            "/api/v5/api_key_scopes",
            "/api/v5/users",
            "/api/v5/user_scopes",
        ] {
            let (status, body) = server.get_basic(path, key_id, secret).await;
            assert_eq!(status, 403, "GET {path} with Basic: {body}");
            assert_eq!(body["code"], json!("FORBIDDEN"));
            let (status, _) = server.get_auth(path, secret).await;
            assert_eq!(status, 403, "GET {path} with Bearer");
        }
        let (status, _) = server
            .post_basic("/api/v5/api_key", json!({"name": "more"}), key_id, secret)
            .await;
        assert_eq!(status, 403);
        let (status, _) = server
            .post_basic("/api/v5/users", json!({}), key_id, secret)
            .await;
        assert_eq!(status, 403);
        // Logout documents 401.
        let (status, _) = server
            .post_basic("/api/v5/logout", json!({}), key_id, secret)
            .await;
        assert_eq!(status, 401);
    }
    let (status, list) = server.get_auth("/api/v5/api_key", &token).await;
    assert_eq!(status, 200);
    assert_eq!(
        list.as_array().map(Vec::len),
        Some(2),
        "no key made a key: {list}"
    );

    // The operations that the reference lists for Basic accept a key
    // without a scope list. The catalogue has no scope for them, thus
    // a key with a scope list cannot call them.
    let (status, body) = server
        .get_basic("/api/v5/api_key/scoped", &full_id, &full)
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["name"], json!("scoped"));
    assert!(body.get("api_secret").is_none());
    let (status, _) = server.get_basic("/api/v5/sso", &full_id, &full).await;
    assert_eq!(status, 200);
    let (status, _) = server.get_basic("/api/v5/sso", &scoped_id, &scoped).await;
    assert_eq!(status, 403);
    let (status, _) = server
        .get_basic("/api/v5/api_key/full", &scoped_id, &scoped)
        .await;
    assert_eq!(status, 403);

    // A disabled key does not work, and works again when enabled.
    let (status, body) = server
        .put_auth("/api/v5/api_key/scoped", json!({"enable": false}), &token)
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["enable"], json!(false));
    let (status, _) = server.get_auth("/api/v5/stats", &scoped).await;
    assert_eq!(status, 401);
    let (status, _) = server.get_basic("/api/v5/stats", &scoped_id, &scoped).await;
    assert_eq!(status, 401);
    let (status, _) = server
        .put_auth("/api/v5/api_key/scoped", json!({"enable": true}), &token)
        .await;
    assert_eq!(status, 200);
    let (status, _) = server.get_auth("/api/v5/stats", &scoped).await;
    assert_eq!(status, 200);

    // An expired key does not work, and the read shows `expired`.
    let (status, body) = server
        .put_auth(
            "/api/v5/api_key/scoped",
            json!({"expired_at": "2020-01-01T00:00:00Z"}),
            &token,
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["expired"], json!(true));
    let (status, _) = server.get_auth("/api/v5/stats", &scoped).await;
    assert_eq!(status, 401);
    let (status, _) = server
        .put_auth(
            "/api/v5/api_key/scoped",
            json!({"expired_at": "infinity"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, body) = server.get_auth("/api/v5/api_key/scoped", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["expired"], json!(false));
    assert_eq!(body["expired_at"], json!("infinity"));
    let (status, _) = server.get_auth("/api/v5/stats", &scoped).await;
    assert_eq!(status, 200);

    // A key with the role `viewer` can read and cannot write.
    let (viewer_id, viewer) = make_key(
        &server,
        &token,
        json!({"name": "watcher", "role": "viewer"}),
    )
    .await;
    let (status, _) = server.get_basic("/api/v5/stats", &viewer_id, &viewer).await;
    assert_eq!(status, 200);
    let (status, body) = server
        .post_basic("/api/v5/publish", json!({}), &viewer_id, &viewer)
        .await;
    assert_eq!(status, 403, "a viewer key cannot write: {body}");

    // A key with an empty scope list can call nothing.
    let (nothing_id, nothing) =
        make_key(&server, &token, json!({"name": "nothing", "scopes": []})).await;
    let (status, _) = server
        .get_basic("/api/v5/stats", &nothing_id, &nothing)
        .await;
    assert_eq!(status, 403);

    // A removed key does not work.
    let (status, _) = server.delete_auth("/api/v5/api_key/scoped", &token).await;
    assert_eq!(status, 204);
    let (status, _) = server.get_auth("/api/v5/stats", &scoped).await;
    assert_eq!(status, 401);
    let (status, _) = server.get_basic("/api/v5/stats", &scoped_id, &scoped).await;
    assert_eq!(status, 401);
    let (status, _) = server.get_auth("/api/v5/stats", &full).await;
    assert_eq!(status, 200);
}

#[tokio::test]
async fn api_keys_create_list_revoke_and_miss_404() {
    // W2-25 failing test: mint a key expecting the secret returned
    // once, list expecting metadata without secrets, delete it
    // expecting revocation, and delete unknown expecting 404.
    // Before: all three routes 404.
    // Broker-through: every step goes through the served management
    // API on `ApiState` (the same state the kernel shares in
    // `crates/broker-node/src/main.rs`, `serve_api`), not only
    // through the store; key verification itself is covered by the
    // store unit test and the management-plane verify off the hot
    // path, never by publish or deliver.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Empty store reads as an empty array, never an error.
    let (status, body) = server.get_auth("/api/v5/api_key", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));

    // Mint one key: the secret is returned exactly once.
    let (status, created) = server
        .post_auth(
            "/api/v5/api_key",
            json!({"name": "w225-broker-key", "scopes": ["monitoring"]}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(created["name"], json!("w225-broker-key"));
    let secret = created["api_secret"]
        .as_str()
        .expect("secret returned once");
    assert!(!secret.is_empty());
    let key_id = created["api_key"].as_str().expect("identifier");
    assert_eq!(key_id.len(), 16);
    for field in [
        "desc",
        "enable",
        "expired",
        "expired_at",
        "created_at",
        "role",
        "scopes",
    ] {
        assert!(created.get(field).is_some(), "create renders {field}");
    }
    assert_eq!(created["enable"], json!(true));
    assert_eq!(created["expired"], json!(false));
    assert_eq!(created["expired_at"], json!("infinity"));
    assert_eq!(created["role"], json!("administrator"));
    assert_eq!(created["scopes"], json!(["monitoring"]));
    assert!(created.get("namespace").is_none());

    // Malformed bodies are client errors.
    let (status, _) = server
        .post_auth("/api/v5/api_key", json!({"scopes": ["monitoring"]}), &token)
        .await;
    assert_eq!(status, 400);

    // Duplicate names are rejected cleanly, never silently rotated.
    let (status, duplicate) = server
        .post_auth(
            "/api/v5/api_key",
            json!({"name": "w225-broker-key"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(duplicate["code"], json!("BAD_REQUEST"));

    // The name rule of the reference: a letter first, then letters,
    // digits, `-` and `_`.
    for bad_name in ["1key", "-key", "has space", "has/slash"] {
        let (status, body) = server
            .post_auth("/api/v5/api_key", json!({"name": bad_name}), &token)
            .await;
        assert_eq!(status, 400, "name {bad_name}: {body}");
        assert_eq!(body["code"], json!("BAD_REQUEST"));
    }
    // A namespace is not available on this node.
    let (status, body) = server
        .post_auth(
            "/api/v5/api_key",
            json!({"name": "namespaced", "namespace": "ns1"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["code"], json!("BAD_REQUEST"));

    // Listing carries metadata without secrets.
    let (status, listed) = server.get_auth("/api/v5/api_key", &token).await;
    assert_eq!(status, 200);
    let data = listed.as_array().expect("the body is a bare array");
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["name"], json!("w225-broker-key"));
    assert_eq!(data[0]["api_key"], json!(key_id));
    assert!(data[0].get("api_secret").is_none());
    assert!(data[0].get("secret").is_none());
    assert!(data[0].get("key_hash").is_none());

    // Revoke the key and confirm unknown names 404 with the
    // documented shape.
    let (status, _) = server
        .delete_auth("/api/v5/api_key/w225-broker-key", &token)
        .await;
    assert_eq!(status, 204);
    let (status, missing) = server
        .delete_auth("/api/v5/api_key/w225-broker-key", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(missing["code"], json!("NOT_FOUND"));

    // Empty again after revocation.
    let (status, body) = server.get_auth("/api/v5/api_key", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
}

/// The management console creates, lists and edits keys with these
/// value forms: `scopes: "unset"`, an ISO time with milliseconds for
/// `expired_at` (or no `expired_at` for "never"), and it sends the
/// listed values back on an update. Each value must round-trip.
#[tokio::test]
async fn the_console_forms_of_an_api_key_round_trip() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, created) = server
        .post_auth(
            "/api/v5/api_key",
            json!({
                "name": "console_key",
                "desc": "from the console",
                "enable": true,
                "role": "viewer",
                "scopes": "unset",
                "expired_at": "2099-01-02T23:59:59.000Z",
            }),
            &token,
        )
        .await;
    assert_eq!(status, 200, "{created}");
    assert_eq!(created["scopes"], json!("unset"));
    assert_eq!(created["role"], json!("viewer"));
    assert_eq!(created["desc"], json!("from the console"));

    // The list is a bare array. The console sends a listed row back.
    let (status, listed) = server.get_auth("/api/v5/api_key", &token).await;
    assert_eq!(status, 200);
    let row = listed.as_array().expect("array")[0].clone();
    assert_eq!(row["expired"], json!(false));
    let (status, body) = server
        .put_auth(
            "/api/v5/api_key/console_key",
            json!({
                "enable": false,
                "desc": row["desc"],
                "role": row["role"],
                "scopes": row["scopes"],
                "expired_at": row["expired_at"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["enable"], json!(false));
    assert_eq!(body["expired_at"], row["expired_at"]);
    assert_eq!(body["scopes"], json!("unset"));

    // The console omits `expired_at` to say "never expires".
    let (status, body) = server
        .put_auth(
            "/api/v5/api_key/console_key",
            json!({"enable": true, "desc": "", "role": "administrator", "scopes": ["audit"]}),
            &token,
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["expired_at"], json!("infinity"));
    assert_eq!(body["role"], json!("administrator"));
    assert_eq!(body["scopes"], json!(["audit"]));
}

#[tokio::test]
async fn api_key_detail_read_update_round_trip_and_miss_404() {
    // W2-26 failing test: mint a key, read it expecting metadata
    // without the secret, update scopes/description and read back,
    // read/update unknown expecting 404, invalid updates rejected
    // without applying. Before: both detail routes 404.
    // Broker-through: every step goes through the served management
    // API on `ApiState` (the same state the kernel shares in
    // `crates/broker-node/src/main.rs`, `serve_api`), not only
    // through the store; the store unit tests cover hashing and the
    // management-plane verify off the hot path, never publish or
    // deliver.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Mint one key through the broker.
    let (status, _) = server
        .post_auth(
            "/api/v5/api_key",
            json!({"name": "w226-broker-key", "scopes": ["monitoring"], "desc": "before"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);

    // Read it back: metadata only, never the secret.
    let (status, body) = server
        .get_auth("/api/v5/api_key/w226-broker-key", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["name"], json!("w226-broker-key"));
    assert_eq!(body["scopes"], json!(["monitoring"]));
    assert_eq!(body["desc"], json!("before"));
    assert!(body["api_key"].is_string());
    assert!(body.get("api_secret").is_none());
    assert!(body.get("secret").is_none());
    assert!(body.get("key_hash").is_none());

    // Unknown names 404 with the documented shape and create nothing.
    let (status, missing) = server
        .get_auth("/api/v5/api_key/w226-no-such-key", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(missing["code"], json!("NOT_FOUND"));
    let (status, missing) = server
        .put_auth(
            "/api/v5/api_key/w226-no-such-key",
            json!({"scopes": ["connections"]}),
            &token,
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(missing["code"], json!("NOT_FOUND"));
    let (status, _) = server
        .get_auth("/api/v5/api_key/w226-no-such-key", &token)
        .await;
    assert_eq!(status, 404);

    // Update scopes/description and read back the replace.
    let (status, updated) = server
        .put_auth(
            "/api/v5/api_key/w226-broker-key",
            json!({"scopes": ["connections", "publish"], "desc": "after"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(updated["name"], json!("w226-broker-key"));
    assert_eq!(updated["scopes"], json!(["connections", "publish"]));
    assert_eq!(updated["desc"], json!("after"));
    assert!(updated["api_key"].is_string());
    assert!(updated.get("api_secret").is_none());
    assert!(updated.get("key_hash").is_none());
    let (status, reread) = server
        .get_auth("/api/v5/api_key/w226-broker-key", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(reread["scopes"], json!(["connections", "publish"]));
    assert_eq!(reread["desc"], json!("after"));

    // Bad values are client errors and apply nothing.
    let (status, _) = server
        .put_auth(
            "/api/v5/api_key/w226-broker-key",
            json!({"scopes": [""]}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    let (status, reread) = server
        .get_auth("/api/v5/api_key/w226-broker-key", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(reread["scopes"], json!(["connections", "publish"]));

    // Revocation still removes the detail row.
    let (status, _) = server
        .delete_auth("/api/v5/api_key/w226-broker-key", &token)
        .await;
    assert_eq!(status, 204);
    let (status, missing) = server
        .get_auth("/api/v5/api_key/w226-broker-key", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(missing["code"], json!("NOT_FOUND"));
}

#[tokio::test]
async fn api_key_detail_update_survives_restart() {
    // Persistence through the broker: PUT through the management
    // API, reload the same data dir in a fresh server, and read the
    // replaced row back. Secrets stay hashed at rest, so the update
    // never needs the secret.
    let dir = unique_data_dir("apikey-detail");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let (status, _) = server
        .post_auth(
            "/api/v5/api_key",
            json!({"name": "w226-persist", "scopes": ["monitoring"]}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = server
        .put_auth(
            "/api/v5/api_key/w226-persist",
            json!({"scopes": ["connections"], "desc": "kept"}),
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
    let (status, body) = server
        .get_auth("/api/v5/api_key/w226-persist", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["scopes"], json!(["connections"]));
    assert_eq!(body["desc"], json!("kept"));
    assert!(body["api_key"].is_string());
    assert!(body.get("api_secret").is_none());
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn api_key_scopes_catalogue_is_stable_and_unpaged() {
    // W2-27 failing test: fetch the scope catalogue expecting the
    // documented names present and stable across two reads, with
    // per-scope fields on each entry. Before: 404 (route removed).
    // Broker-through: both reads go through the served management
    // API on `ApiState` (the same state the kernel shares in
    // `crates/broker-node/src/main.rs`, `serve_api`), not only
    // through the store; the catalogue itself is code-defined
    // (`crates/broker-api/src/v5/api_keys.rs`, `API_KEY_SCOPES`),
    // shared with the key create/update validation registry
    // (`is_known_api_key_scope`), with no broker event driving it
    // beyond serving the management read. Management-plane only;
    // publish and deliver never touch it.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, first) = server.get_auth("/api/v5/api_key_scopes", &token).await;
    assert_eq!(status, 200);
    let scopes = first["scopes"].as_array().expect("scopes is a list");
    assert_eq!(scopes.len(), 10);
    let names: Vec<&str> = scopes
        .iter()
        .map(|entry| entry["name"].as_str().expect("entry has a name"))
        .collect();
    for entry in scopes {
        assert!(entry["name"].is_string(), "entry carries a name");
        assert!(entry["desc"].is_string(), "entry carries a summary");
    }
    for documented in [
        "connections",
        "publish",
        "data_integration",
        "access_control",
        "gateways",
        "monitoring",
        "cluster_operations",
        "system",
        "audit",
        "license",
    ] {
        assert!(
            names.contains(&documented),
            "catalogue must contain {documented}"
        );
    }

    // Stable across two reads.
    let (status, second) = server.get_auth("/api/v5/api_key_scopes", &token).await;
    assert_eq!(status, 200);
    assert_eq!(first, second);

    // Not paged and takes no parameters: unknown query parameters
    // are ignored, not errors.
    let (status, tolerated) = server
        .send(
            "GET",
            "/api/v5/api_key_scopes?unexpected_param=1&page=2&limit=1",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(tolerated, first);
}
