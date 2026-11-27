use super::*;

#[tokio::test]
async fn sso_list_empty_then_configured_and_survives_restart() {
    // W2-32 failing test: list when empty expecting an empty
    // collection, configure one backend then list expecting it.
    // Before: 404 (route removed).
    // Broker-through: every read goes through the served management
    // API on `ApiState` (the same state the kernel shares in
    // `crates/broker-node/src/main.rs`, `serve_api`), not only
    // through the store; the registry seed is the kernel boot path
    // (`Shared.sso_backends`, `seed_from_registry`), and the
    // per-backend detail write path belongs to a sibling task, so
    // the seed here commits the registry root directly. Store unit
    // tests cover insert validation and the management-plane read off
    // the hot path; publish and deliver never touch this store.
    let dir = unique_data_dir("sso-list");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;

    // Empty set reads as an empty collection, never an error.
    let (status, body) = server.get_auth("/api/v5/sso", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));

    // The route is not paged and takes no parameters: unknown query
    // parameters are ignored, not errors.
    let (status, tolerated) = server
        .send(
            "GET",
            "/api/v5/sso?unexpected_param=1&page=2&limit=1",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(tolerated, json!([]));

    // Configure one backend through the registry root, persist, and
    // restart: the list must show it with its stored fields.
    registry
        .commit_sso_backends(broker_config::SsoBackendsConf {
            backends: vec![broker_config::SsoBackendEntry {
                backend: "w232-oidc".to_string(),
                enable: true,
                config: serde_json::json!({"issuer": "https://idp.example.invalid"}).to_string(),
            }],
        })
        .expect("commit sso backend");
    registry.save().expect("save sso backend");
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
    let (status, body) = server.get_auth("/api/v5/sso", &token).await;
    assert_eq!(status, 200);
    let data = body.as_array().expect("sso list is a collection");
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["backend"], json!("w232-oidc"));
    assert_eq!(data[0]["enable"], json!(true));
    assert_eq!(
        data[0]["config"]["issuer"],
        json!("https://idp.example.invalid")
    );

    // Unknown query parameters stay ignored on the configured read.
    let (status, tolerated) = server
        .send("GET", "/api/v5/sso?unexpected_param=1", None, Some(&token))
        .await;
    assert_eq!(status, 200);
    assert_eq!(tolerated, body);
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn sso_running_empty_then_enabled_only_and_survives_restart() {
    // W2-36 failing test: list running with none enabled expecting
    // empty, enable one backend then list expecting it.
    // Before: 404 (no route for this path).
    // Broker-through: every read goes through the served management
    // API on `ApiState` (the same state the kernel shares in
    // `crates/broker-node/src/main.rs`, `serve_api`), not only
    // through the store; the registry seed is the kernel boot path
    // (`Shared.sso_backends`, `seed_from_registry`). Store unit
    // tests cover the enabled filter; publish and deliver never
    // touch this store.
    let dir = unique_data_dir("sso-running");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;

    // Empty set reads as an empty collection, never an error. The
    // route is public (login page probes it pre-auth), so both the
    // anonymous and the authenticated reads answer 200 here.
    let (status, body) = server.get("/api/v5/sso/running").await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
    let token = server.login_as_admin().await;
    let (status, body) = server.get_auth("/api/v5/sso/running", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));

    // The route is not paged and takes no parameters: unknown query
    // parameters are ignored, not errors.
    let (status, tolerated) = server
        .send(
            "GET",
            "/api/v5/sso/running?unexpected_param=1&page=2&limit=1",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(tolerated, json!([]));

    // Enable one backend and leave one disabled through the registry
    // root, persist, and restart: running must list only the enabled
    // name while the full list still shows both slots.
    registry
        .commit_sso_backends(broker_config::SsoBackendsConf {
            backends: vec![
                broker_config::SsoBackendEntry {
                    backend: "w236-oidc".to_string(),
                    enable: true,
                    config: serde_json::json!({"issuer": "https://idp.example.invalid"})
                        .to_string(),
                },
                broker_config::SsoBackendEntry {
                    backend: "w236-off".to_string(),
                    enable: false,
                    config: String::new(),
                },
            ],
        })
        .expect("commit sso backends");
    registry.save().expect("save sso backends");
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
    let (status, body) = server.get_auth("/api/v5/sso/running", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!(["w236-oidc"]));
    // Anonymous probe agrees with the authenticated read.
    let (status, anon) = server.get("/api/v5/sso/running").await;
    assert_eq!(status, 200);
    assert_eq!(anon, body);
    // The full list still carries both slots; running is the enabled
    // subset of that live state.
    let (status, full) = server.get_auth("/api/v5/sso", &token).await;
    assert_eq!(status, 200);
    assert_eq!(full.as_array().expect("sso list is a collection").len(), 2);
    // Unknown query parameters stay ignored on the configured read.
    let (status, tolerated) = server
        .send(
            "GET",
            "/api/v5/sso/running?unexpected_param=1",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(tolerated, body);
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn sso_backend_detail_write_read_update_delete_through_router() {
    // W2-39 failing test: write one backend config, read it expecting
    // redacted secrets; update and read back; delete then read
    // expecting 404. Before: all 404 (no route for this path).
    // Broker-through: every call goes through the served management
    // API on `ApiState` (the same state the kernel shares in
    // `crates/broker-node/src/main.rs`, `serve_api`), not only
    // through the store; the registry seed is the kernel boot path
    // (`Shared.sso_backends`, `seed_from_registry`). Store unit tests
    // cover insert validation and the management-plane read off the
    // hot path; publish and deliver never touch this store.
    let dir = unique_data_dir("sso-detail");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;

    // Unknown backends read as 404 in the documented shape.
    let (status, err) = server
        .get_auth("/api/v5/sso/w239-no-such-backend", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(err["code"], json!("BACKEND_NOT_FOUND"));
    assert!(err["message"].is_string());
    assert_eq!(err.as_object().expect("error is an object").len(), 2);

    // Unknown backends delete as 404 and create nothing.
    let (status, err) = server
        .delete_auth("/api/v5/sso/w239-no-such-backend", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(err["code"], json!("BACKEND_NOT_FOUND"));

    // Write one backend carrying a secret; the write answers 200
    // with the stored slot, secrets already redacted.
    let (status, written) = server
        .put_auth(
            "/api/v5/sso/w239-oidc",
            json!({
                "enable": true,
                "config": {
                    "issuer": "https://idp.example.invalid",
                    "client_secret": "w239-live-secret",
                },
            }),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(written["backend"], json!("w239-oidc"));
    assert_eq!(written["enable"], json!(true));
    assert_eq!(
        written["config"]["issuer"],
        json!("https://idp.example.invalid")
    );
    assert_eq!(written["config"]["client_secret"], json!("******"));
    let text = serde_json::to_string(&written).expect("write row serialises");
    assert!(
        !text.contains("w239-live-secret"),
        "stored secret must not render"
    );

    // Read it back: the secret stays redacted on every read.
    let (status, body) = server.get_auth("/api/v5/sso/w239-oidc", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, written);
    let text = serde_json::to_string(&body).expect("detail row serialises");
    assert!(
        !text.contains("w239-live-secret"),
        "stored secret must not render"
    );

    // The list carries the same redacted row: secrets never render
    // on any read.
    let (status, list) = server.get_auth("/api/v5/sso", &token).await;
    assert_eq!(status, 200);
    let rows = list.as_array().expect("sso list is a collection");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], written);

    // Update and read back: the replacement is stored whole.
    let (status, updated) = server
        .put_auth(
            "/api/v5/sso/w239-oidc",
            json!({
                "enable": false,
                "config": {"issuer": "https://other.example.invalid"},
            }),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(updated["enable"], json!(false));
    assert_eq!(
        updated["config"]["issuer"],
        json!("https://other.example.invalid")
    );
    let (status, body) = server.get_auth("/api/v5/sso/w239-oidc", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, updated);

    // Invalid writes are client errors and apply nothing.
    for bad in [
        json!({"enable": "yes"}),
        json!({"enable": true, "config": 7}),
        json!({"backend": "w239-other", "enable": true}),
    ] {
        let (status, err) = server.put_auth("/api/v5/sso/w239-oidc", bad, &token).await;
        assert_eq!(status, 400);
        assert_eq!(err["code"], json!("BAD_REQUEST"));
        assert!(err["message"].is_string());
    }
    let (status, intact) = server.get_auth("/api/v5/sso/w239-oidc", &token).await;
    assert_eq!(status, 200);
    assert_eq!(intact, updated);

    // Delete then read expecting 404.
    let (status, _) = server.delete_auth("/api/v5/sso/w239-oidc", &token).await;
    assert_eq!(status, 204);
    let (status, err) = server.get_auth("/api/v5/sso/w239-oidc", &token).await;
    assert_eq!(status, 404);
    assert_eq!(err["code"], json!("BACKEND_NOT_FOUND"));

    // Configs persist across a restart: write again, reload the data
    // dir, and read the same redacted slot without re-creating it.
    let (status, _) = server
        .put_auth(
            "/api/v5/sso/w239-oidc",
            json!({
                "enable": true,
                "config": {
                    "issuer": "https://idp.example.invalid",
                    "client_secret": "w239-restart-secret",
                },
            }),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    drop(server);
    drop(registry);
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;
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
    let (status, body) = server.get_auth("/api/v5/sso/w239-oidc", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["backend"], json!("w239-oidc"));
    assert_eq!(body["enable"], json!(true));
    assert_eq!(body["config"]["client_secret"], json!("******"));
    let text = serde_json::to_string(&body).expect("restarted row serialises");
    assert!(
        !text.contains("w239-restart-secret"),
        "stored secret must not render"
    );
    // The route is not paged: unknown query parameters are ignored.
    let (status, tolerated) = server
        .send(
            "GET",
            "/api/v5/sso/w239-oidc?unexpected_param=1",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(tolerated, body);
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn sso_token_exchange_bad_grant_and_unknown_backend_through_router() {
    // W2-38 failing test through the served management API: before
    // this task the path answered 404 with the router's plain-text
    // body; now unknown backends answer 404 and malformed bodies
    // answer 400, both in the documented `{code, message}` shape.
    // Broker-through: every call goes through the served management
    // API on `ApiState` (the same state the kernel shares in
    // `crates/broker-node/src/main.rs`, `serve_api`), not only
    // through the store. The route is public (pre-auth: the dashboard
    // login page calls it before anyone holds a token), so no bearer
    // is sent. Handler unit tests seed the registry for the success
    // half; this test pins the wiring plus the failure shapes.
    // Management-plane only; publish and deliver never touch the
    // backend registry or the pending table.
    let (server, _state) = TestServer::start().await;

    // Unknown backends fail with 404 and issue no token.
    let (status, err) = server
        .post(
            "/api/v5/sso/token_exchange",
            json!({"backend": "w238-no-such-backend", "grant": "w238-no-such-grant"}),
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(err["code"], json!("BACKEND_NOT_FOUND"));
    assert!(err["message"].is_string());
    assert_eq!(err.as_object().expect("error is an object").len(), 2);
    assert!(err.get("token").is_none());

    // Malformed bodies fail with 400 and issue no token.
    let (status, err) = server.post("/api/v5/sso/token_exchange", json!({})).await;
    assert_eq!(status, 400);
    assert_eq!(err["code"], json!("BAD_REQUEST"));
    assert!(err["message"].is_string());
    assert_eq!(err.as_object().expect("error is an object").len(), 2);
    assert!(err.get("token").is_none());

    drop(server);
}
