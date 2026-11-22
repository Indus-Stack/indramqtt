use super::*;

#[tokio::test]
async fn client_authz_cache_read_clear_round_trip() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Connect through the broker: the console CONNECT creates the
    // session, so the cache starts life empty. An empty cache reads
    // as an empty list.
    let mut subscriber = WsClient::connect(server.port).await;
    subscriber
        .send_bin(&mqtt_connect("w2-01-cache-1", None, None))
        .await;
    let connack = subscriber.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    let (status, body) = server
        .get_auth("/api/v5/clients/w2-01-cache-1/authorization/cache", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));

    // Populate through normal authorized activity through the broker:
    // a console SUBSCRIBE (real subscribe authorization) plus a
    // console PUBLISH from a second connection (real publish
    // authorization) that delivers to the subscriber.
    subscriber
        .send_bin(&mqtt_subscribe(7, &[("conf/authz/1", 0)]))
        .await;
    let suback = subscriber.recv_msg().await.expect("suback");
    assert_eq!(mqtt_packet_type(&suback), 9);
    assert_eq!(&suback[4..], &[0]);
    let mut publisher = WsClient::connect(server.port).await;
    publisher
        .send_bin(&mqtt_connect("w2-01-cache-2", None, None))
        .await;
    let connack = publisher.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    publisher
        .send_bin(&mqtt_publish("conf/authz/1", 0, 0, b"hi"))
        .await;
    // Deliver proves the publish rode the broker route path.
    let delivery = subscriber.recv_msg().await.expect("delivery");
    assert_eq!(mqtt_packet_type(&delivery), 3);

    // Read-hit shape: each decision present with per-entry fields.
    let (status, body) = server
        .get_auth("/api/v5/clients/w2-01-cache-1/authorization/cache", &token)
        .await;
    assert_eq!(status, 200);
    let entries = body.as_array().expect("cache is a bare list");
    assert_eq!(entries.len(), 1);
    let subscribe = entries
        .iter()
        .find(|e| e["topic"] == json!("conf/authz/1"))
        .expect("subscribe decision present");
    assert_eq!(subscribe["access"], json!("subscribe"));
    assert_eq!(subscribe["result"], json!("allow"));
    assert!(subscribe["updated_time"].is_number());
    let (status, body) = server
        .get_auth("/api/v5/clients/w2-01-cache-2/authorization/cache", &token)
        .await;
    assert_eq!(status, 200);
    let entries = body.as_array().expect("cache is a bare list");
    assert_eq!(entries.len(), 1);
    let publish = entries
        .iter()
        .find(|e| e["topic"] == json!("conf/authz/1"))
        .expect("publish decision present");
    assert_eq!(publish["access"], json!("publish"));
    assert_eq!(publish["result"], json!("allow"));
    assert!(publish["updated_time"].is_number());

    // A denial spells `deny`: a denied subscribe through the broker
    // records the exact value, so a wrong spelling fails this test.
    state
        .auth
        .add_rule(AclRule::new(
            "w2-01-cache-deny",
            AclAction::Subscribe,
            "conf/authz/denied",
            false,
        ))
        .expect("deny rule installs");
    let mut denied = WsClient::connect(server.port).await;
    denied
        .send_bin(&mqtt_connect("w2-01-cache-deny", None, None))
        .await;
    let connack = denied.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    denied
        .send_bin(&mqtt_subscribe(8, &[("conf/authz/denied", 0)]))
        .await;
    let suback = denied.recv_msg().await.expect("suback");
    assert_eq!(mqtt_packet_type(&suback), 9);
    assert_eq!(&suback[4..], &[0x80]);
    let (status, body) = server
        .get_auth(
            "/api/v5/clients/w2-01-cache-deny/authorization/cache",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let entries = body.as_array().expect("cache is a bare list");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["access"], json!("subscribe"));
    assert_eq!(entries[0]["topic"], json!("conf/authz/denied"));
    assert_eq!(entries[0]["result"], json!("deny"));

    // Unknown query keys are ignored, not a 400.
    let (status, body) = server
        .get_auth(
            "/api/v5/clients/w2-01-cache-1/authorization/cache?unknown_param=1",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().expect("list").len(), 1);

    // Clear evicts; the next read is empty. Clearing again still
    // succeeds (204).
    let (status, _) = server
        .delete_auth("/api/v5/clients/w2-01-cache-1/authorization/cache", &token)
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/clients/w2-01-cache-1/authorization/cache", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
    let (status, _) = server
        .delete_auth("/api/v5/clients/w2-01-cache-1/authorization/cache", &token)
        .await;
    assert_eq!(status, 204);

    // Unknown client ids are 404 with the documented error shape.
    let (status, body) = server
        .get_auth("/api/v5/clients/no-such-client/authorization/cache", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("CLIENTID_NOT_FOUND"));
    assert!(body["message"].is_string());
    let (status, body) = server
        .delete_auth("/api/v5/clients/no-such-client/authorization/cache", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("CLIENTID_NOT_FOUND"));
    assert!(body["message"].is_string());
}

#[tokio::test]
async fn client_authz_cache_ttl_filter_round_trip() {
    // TTL filter through the production router: one fresh decision
    // reads as a list with one entry, one expired decision reads as empty.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Connect and subscribe through the broker.
    let mut subscriber = WsClient::connect(server.port).await;
    subscriber
        .send_bin(&mqtt_connect("w2-01-cache-ttl", None, None))
        .await;
    let connack = subscriber.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    subscriber
        .send_bin(&mqtt_subscribe(7, &[("conf/authz/ttl", 0)]))
        .await;
    let suback = subscriber.recv_msg().await.expect("suback");
    assert_eq!(mqtt_packet_type(&suback), 9);
    assert_eq!(&suback[4..], &[0]);

    // Fresh entry reads as a list with one entry.
    let (status, body) = server
        .get_auth(
            "/api/v5/clients/w2-01-cache-ttl/authorization/cache",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let entries = body.as_array().expect("cache is a bare list");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["topic"], json!("conf/authz/ttl"));
    assert_eq!(entries[0]["access"], json!("subscribe"));
    assert_eq!(entries[0]["result"], json!("allow"));
    assert!(entries[0]["updated_time"].is_number());

    // Short TTL expires the entry. Wait past the TTL.
    // Short TTL keeps the wait small. Long wait avoids timing luck.
    let (status, _) = server
        .put_auth(
            "/api/v5/authorization/settings",
            json!({
                "no_match": "deny",
                "deny_action": "ignore",
                "cache": {
                    "enable": true,
                    "max_size": 32,
                    "ttl": "1ms",
                    "excludes": [],
                },
                "include_mountpoint": false,
                "ignore_backend_failures": false,
            }),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let (status, body) = server
        .get_auth(
            "/api/v5/clients/w2-01-cache-ttl/authorization/cache",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
}

#[tokio::test]
async fn client_authz_cache_latest_decision_round_trip() {
    // Latest decision through the production router: two fresh
    // subscribes, the read returns both entries with the latest last.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let mut subscriber = WsClient::connect(server.port).await;
    subscriber
        .send_bin(&mqtt_connect("w2-01-cache-latest", None, None))
        .await;
    let connack = subscriber.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    subscriber
        .send_bin(&mqtt_subscribe(7, &[("conf/authz/latest-a", 0)]))
        .await;
    let suback = subscriber.recv_msg().await.expect("suback");
    assert_eq!(mqtt_packet_type(&suback), 9);
    assert_eq!(&suback[4..], &[0]);
    subscriber
        .send_bin(&mqtt_subscribe(8, &[("conf/authz/latest-b", 0)]))
        .await;
    let suback = subscriber.recv_msg().await.expect("suback");
    assert_eq!(mqtt_packet_type(&suback), 9);
    assert_eq!(&suback[4..], &[0]);
    let (status, body) = server
        .get_auth(
            "/api/v5/clients/w2-01-cache-latest/authorization/cache",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let entries = body.as_array().expect("cache is a bare list");
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[1]["topic"], json!("conf/authz/latest-b"));
    assert_eq!(entries[1]["access"], json!("subscribe"));
    assert_eq!(entries[1]["result"], json!("allow"));
    assert!(entries[1]["updated_time"].is_number());
}

#[tokio::test]
async fn authz_global_cache_clear_round_trip() {
    // Global decision-cache clear (W2-13, C04-01) through the broker:
    // authorized activity populates the per-session decision caches
    // (consulted at `crates/broker-session/src/lib.rs`
    // `Session::record_authz_decision`, driven by the publish and
    // subscribe authorization events), the global clear evicts every
    // entry cluster-wide on this node, and the next authorized
    // decision is recomputed (the cache repopulates). Clearing an
    // already-empty cache still succeeds. Management-plane only;
    // publish and deliver never take the clear locks beyond the
    // short per-session evictions.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Empty global clear still succeeds (204) before any activity.
    let (status, _) = server
        .delete_auth("/api/v5/authorization/cache", &token)
        .await;
    assert_eq!(status, 204);

    // Populate through normal authorized activity through the broker:
    // a console SUBSCRIBE (real subscribe authorization) plus a
    // console PUBLISH from a second connection (real publish
    // authorization) that delivers to the subscriber.
    let mut subscriber = WsClient::connect(server.port).await;
    subscriber
        .send_bin(&mqtt_connect("w2-13-cache-1", None, None))
        .await;
    let connack = subscriber.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    subscriber
        .send_bin(&mqtt_subscribe(7, &[("w2-13/topic", 0)]))
        .await;
    let suback = subscriber.recv_msg().await.expect("suback");
    assert_eq!(mqtt_packet_type(&suback), 9);
    assert_eq!(&suback[4..], &[0]);
    let mut publisher = WsClient::connect(server.port).await;
    publisher
        .send_bin(&mqtt_connect("w2-13-cache-2", None, None))
        .await;
    let connack = publisher.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    publisher
        .send_bin(&mqtt_publish("w2-13/topic", 0, 0, b"hi"))
        .await;
    // Deliver proves the publish rode the broker route path.
    let delivery = subscriber.recv_msg().await.expect("delivery");
    assert_eq!(mqtt_packet_type(&delivery), 3);

    // Both per-client caches hold their decision.
    let (status, body) = server
        .get_auth("/api/v5/clients/w2-13-cache-1/authorization/cache", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().expect("cache is a list").len(), 1);
    let (status, body) = server
        .get_auth("/api/v5/clients/w2-13-cache-2/authorization/cache", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().expect("cache is a list").len(), 1);

    // Global clear evicts every entry on this node.
    let (status, _) = server
        .delete_auth("/api/v5/authorization/cache", &token)
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/clients/w2-13-cache-1/authorization/cache", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
    let (status, body) = server
        .get_auth("/api/v5/clients/w2-13-cache-2/authorization/cache", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));

    // The next authorized decision is recomputed (cache miss
    // counted as a fresh entry on the next publish through the
    // broker).
    publisher
        .send_bin(&mqtt_publish("w2-13/topic", 0, 0, b"again"))
        .await;
    let delivery = subscriber.recv_msg().await.expect("delivery");
    assert_eq!(mqtt_packet_type(&delivery), 3);
    let (status, body) = server
        .get_auth("/api/v5/clients/w2-13-cache-2/authorization/cache", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().expect("cache is a list").len(), 1);

    // Unknown query keys are ignored, not a 400.
    let (status, _) = server
        .send(
            "DELETE",
            "/api/v5/authorization/cache?unknown_param=1",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn authz_node_cache_config_reset_round_trip() {
    // Node-cache config read/write plus reset (W2-14, C04-02..C04-04)
    // through the broker: GET defaults, PUT a valid update and read
    // it back, PUT invalid expecting rejection with no change, then
    // authorized activity populates the node-cached decisions
    // (per-session entries recorded by the publish and subscribe
    // authorization events at `crates/broker-session/src/lib.rs`
    // `Session::record_authz_decision`, driven from
    // `crates/broker-node/src/main.rs`), the node reset evicts them,
    // and the next authorized decision is recomputed. Management-plane
    // only; publish and deliver never touch the policy store.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Defaults read back with the documented fields.
    let (status, body) = server
        .get_auth("/api/v5/authorization/node_cache", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["enable"], json!(true));
    assert_eq!(body["cache_ttl"], json!("60s"));
    assert_eq!(body["cleanup_interval"], json!("60s"));
    assert_eq!(body["stat_update_interval"], json!("5s"));
    assert_eq!(body["max_count"], json!(50_000));
    assert_eq!(body["max_memory"], json!("100MB"));

    // Unknown query keys are ignored on the read, not a 400.
    let (status, body) = server
        .get_auth("/api/v5/authorization/node_cache?unknown_param=1", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["max_count"], json!(50_000));

    // Full valid replace through the broker.
    let (status, _) = server
        .put_auth(
            "/api/v5/authorization/node_cache",
            json!({
                "enable": true,
                "cache_ttl": "30s",
                "cleanup_interval": "60s",
                "stat_update_interval": "5s",
                "max_count": 5000,
                "max_memory": "100MB",
            }),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/authorization/node_cache", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["cache_ttl"], json!("30s"));
    assert_eq!(body["max_count"], json!(5000));

    // Invalid writes are rejected with the documented shape and apply
    // nothing.
    for bad in [
        json!({"enable": "yes"}),
        json!({"cache_ttl": "soon"}),
        json!({"max_count": 0}),
        json!({"max_count": 2_000_000}),
        json!({"max_count": "many"}),
        json!({"max_memory": "huge"}),
        json!({"enable": true, "bogus": 1}),
    ] {
        let (status, body) = server
            .put_auth("/api/v5/authorization/node_cache", bad, &token)
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(body["message"].is_string());
    }
    let (status, body) = server
        .get_auth("/api/v5/authorization/node_cache", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["cache_ttl"], json!("30s"));
    assert_eq!(body["max_count"], json!(5000));

    // Resetting empty still succeeds (204).
    let (status, _) = server
        .post_auth("/api/v5/authorization/node_cache/reset", json!({}), &token)
        .await;
    assert_eq!(status, 204);

    // Populate through normal authorized activity through the broker:
    // a console SUBSCRIBE (real subscribe authorization) plus a
    // console PUBLISH from a second connection (real publish
    // authorization) that delivers to the subscriber.
    let mut subscriber = WsClient::connect(server.port).await;
    subscriber
        .send_bin(&mqtt_connect("w2-14-cache-1", None, None))
        .await;
    let connack = subscriber.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    subscriber
        .send_bin(&mqtt_subscribe(7, &[("w2-14/topic", 0)]))
        .await;
    let suback = subscriber.recv_msg().await.expect("suback");
    assert_eq!(mqtt_packet_type(&suback), 9);
    assert_eq!(&suback[4..], &[0]);
    let mut publisher = WsClient::connect(server.port).await;
    publisher
        .send_bin(&mqtt_connect("w2-14-cache-2", None, None))
        .await;
    let connack = publisher.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    publisher
        .send_bin(&mqtt_publish("w2-14/topic", 0, 0, b"hi"))
        .await;
    // Deliver proves the publish rode the broker route path.
    let delivery = subscriber.recv_msg().await.expect("delivery");
    assert_eq!(mqtt_packet_type(&delivery), 3);

    // Both per-client caches hold their node-cached decision.
    let (status, body) = server
        .get_auth("/api/v5/clients/w2-14-cache-1/authorization/cache", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().expect("cache is a list").len(), 1);
    let (status, body) = server
        .get_auth("/api/v5/clients/w2-14-cache-2/authorization/cache", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().expect("cache is a list").len(), 1);

    // Node reset evicts every entry on this node.
    let (status, _) = server
        .post_auth("/api/v5/authorization/node_cache/reset", json!({}), &token)
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/clients/w2-14-cache-1/authorization/cache", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
    let (status, body) = server
        .get_auth("/api/v5/clients/w2-14-cache-2/authorization/cache", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));

    // The next authorized decision is recomputed (the cache
    // repopulates on the next publish through the broker).
    publisher
        .send_bin(&mqtt_publish("w2-14/topic", 0, 0, b"again"))
        .await;
    let delivery = subscriber.recv_msg().await.expect("delivery");
    assert_eq!(mqtt_packet_type(&delivery), 3);
    let (status, body) = server
        .get_auth("/api/v5/clients/w2-14-cache-2/authorization/cache", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().expect("cache is a list").len(), 1);

    // Resetting again still succeeds, and unknown query keys are
    // ignored, not a 400.
    let (status, _) = server
        .post_auth("/api/v5/authorization/node_cache/reset", json!({}), &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .send(
            "POST",
            "/api/v5/authorization/node_cache/reset?unknown_param=1",
            Some(json!({})),
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn authz_node_cache_policy_survives_restart() {
    // Policy persistence across a restart through the config
    // registry: PUT through the broker, reload the same data dir in a
    // fresh server, and read the policy back. Cached verdict entries
    // are memory-only and do not cross the restart.
    let dir = unique_data_dir("authz-node-cache");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let (status, _) = server
        .put_auth(
            "/api/v5/authorization/node_cache",
            json!({
                "enable": true,
                "cache_ttl": "30s",
                "cleanup_interval": "60s",
                "stat_update_interval": "5s",
                "max_count": 5000,
                "max_memory": "100MB",
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
        .get_auth("/api/v5/authorization/node_cache", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["cache_ttl"], json!("30s"));
    assert_eq!(body["max_count"], json!(5000));
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn authz_node_cache_status_reports_live_counters() {
    // Node-cache status (W2-15, C04-05) through the broker: the status
    // reads the live policy (enabled flag, entry cap from the W2-14
    // config) plus the live entry count and cumulative hit/miss
    // counters rolled up across the per-session decision caches
    // (`crates/broker-session/src/lib.rs`,
    // `SessionManager::authz_node_stats`, driven by the publish and
    // subscribe authorization events). Before this task the route
    // itself was missing (bare 404). Management-plane only; publish
    // and deliver never touch the status read beyond the
    // already-held per-session write lock with relaxed atomic adds.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Empty node reads as zero entries and zero counters with the
    // live bounds, never an error.
    let (status, body) = server
        .get_auth("/api/v5/authorization/node_cache/status", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["enabled"], json!(true));
    assert_eq!(body["metrics"]["hits"]["value"], json!(0));
    assert_eq!(body["metrics"]["misses"]["value"], json!(0));
    assert_eq!(body["metrics"]["inserts"]["value"], json!(0));
    assert_eq!(body["metrics"]["count"], json!(0));
    assert_eq!(body["metrics"]["max_count"], json!(50_000));
    assert_eq!(body["node_metrics"].as_array().expect("rows").len(), 1);
    assert_eq!(
        body["node_metrics"][0]["metrics"], body["metrics"],
        "node row mirrors the aggregate"
    );

    // Populate through normal authorized activity through the broker:
    // a console SUBSCRIBE (real subscribe authorization) plus a
    // console PUBLISH from a second connection (real publish
    // authorization) that delivers to the subscriber.
    let mut subscriber = WsClient::connect(server.port).await;
    subscriber
        .send_bin(&mqtt_connect("w2-15-cache-1", None, None))
        .await;
    let connack = subscriber.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    subscriber
        .send_bin(&mqtt_subscribe(7, &[("w2-15/topic", 0)]))
        .await;
    let suback = subscriber.recv_msg().await.expect("suback");
    assert_eq!(mqtt_packet_type(&suback), 9);
    assert_eq!(&suback[4..], &[0]);
    let mut publisher = WsClient::connect(server.port).await;
    publisher
        .send_bin(&mqtt_connect("w2-15-cache-2", None, None))
        .await;
    let connack = publisher.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    publisher
        .send_bin(&mqtt_publish("w2-15/topic", 0, 0, b"hi"))
        .await;
    // Deliver proves the publish rode the broker route path.
    let delivery = subscriber.recv_msg().await.expect("delivery");
    assert_eq!(mqtt_packet_type(&delivery), 3);

    // Two new decisions moved the miss/insert counters and the entry
    // count; every counter field is numeric.
    let (status, body) = server
        .get_auth("/api/v5/authorization/node_cache/status", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["enabled"], json!(true));
    assert_eq!(body["metrics"]["misses"]["value"], json!(2));
    assert_eq!(body["metrics"]["inserts"]["value"], json!(2));
    assert_eq!(body["metrics"]["hits"]["value"], json!(0));
    assert_eq!(body["metrics"]["count"], json!(2));
    assert_eq!(body["metrics"]["max_count"], json!(50_000));

    // Repeating the same authorized publish refreshes the cached
    // decision in place: one hit, entries unchanged.
    publisher
        .send_bin(&mqtt_publish("w2-15/topic", 0, 0, b"again"))
        .await;
    let delivery = subscriber.recv_msg().await.expect("delivery");
    assert_eq!(mqtt_packet_type(&delivery), 3);
    let (status, body) = server
        .get_auth("/api/v5/authorization/node_cache/status", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["metrics"]["hits"]["value"], json!(1));
    assert_eq!(body["metrics"]["misses"]["value"], json!(2));
    assert_eq!(body["metrics"]["inserts"]["value"], json!(2));
    assert_eq!(body["metrics"]["count"], json!(2));

    // Unknown query keys are ignored, not a 400.
    let (status, body) = server
        .get_auth(
            "/api/v5/authorization/node_cache/status?unknown_param=1",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["metrics"]["count"], json!(2));
}

#[tokio::test]
async fn banned_create_list_clear_round_trip() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Empty store reads as an empty page, not an error.
    let (status, body) = server.get_auth("/api/v5/banned", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["data"], json!([]));
    assert_eq!(body["meta"]["count"], json!(0));
    assert_eq!(body["meta"]["hasnext"], json!(false));

    // Create one entry.
    let (status, created) = server
        .post_auth(
            "/api/v5/banned",
            json!({"as": "clientid", "who": "w1-01-c1", "reason": "round trip"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(created["data"][0]["as"], json!("clientid"));
    assert_eq!(created["data"][0]["who"], json!("w1-01-c1"));

    // Listing now carries the entry plus paging metadata.
    let (status, body) = server.get_auth("/api/v5/banned", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["meta"]["count"], json!(1));
    assert_eq!(body["data"][0]["who"], json!("w1-01-c1"));
    assert_eq!(body["data"][0]["as"], json!("clientid"));

    // Unknown query keys do not break the list call.
    let (status, body) = server
        .get_auth("/api/v5/banned?unknown_param=1", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["meta"]["count"], json!(1));

    // Clearing drops everything; the next list is empty again.
    let (status, _) = server
        .delete_auth("/api/v5/banned?unknown_param=1", &token)
        .await;
    assert_eq!(status, 204);
    let (status, body) = server.get_auth("/api/v5/banned", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["data"], json!([]));
    assert_eq!(body["meta"]["count"], json!(0));
}

#[tokio::test]
async fn banned_malformed_create_is_bad_request() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    for bad in [
        json!({}),
        json!({"as": "clientid"}),
        json!({"who": "w1-01-x"}),
        json!({"as": "not-a-kind", "who": "w1-01-x"}),
        json!({"as": "clientid", "who": ""}),
        json!({"as": "peerhost", "who": "not-an-ip"}),
    ] {
        let (status, body) = server.post_auth("/api/v5/banned", bad, &token).await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(body["message"].is_string());
    }

    // Nothing above may have stored anything.
    let (status, body) = server.get_auth("/api/v5/banned", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["data"], json!([]));
}

// Read one export file back into (path, text) pairs.
#[tokio::test]
async fn banned_duplicate_create_reports_conflict() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, _) = server
        .post_auth(
            "/api/v5/banned",
            json!({"as": "username", "who": "w1-01-dup"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, body) = server
        .post_auth(
            "/api/v5/banned",
            json!({"as": "username", "who": "w1-01-dup"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("ALREADY_EXISTS"));
    assert!(body["message"].is_string());
}

#[tokio::test]
async fn banned_delete_one_present_absent_and_bad_kind() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Create one entry through the store.
    let (status, _) = server
        .post_auth(
            "/api/v5/banned",
            json!({"as": "clientid", "who": "w1-02-del", "reason": "delete one"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);

    // Deleting it by kind/value reports no-content success.
    let (status, _) = server
        .delete_auth("/api/v5/banned/clientid/w1-02-del", &token)
        .await;
    assert_eq!(status, 204);

    // It is gone from the list afterwards.
    let (status, body) = server.get_auth("/api/v5/banned", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["data"], json!([]));

    // Deleting it again reports not-found, not success.
    let (status, body) = server
        .delete_auth("/api/v5/banned/clientid/w1-02-del", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    assert!(body["message"].is_string());

    // An unknown kind is a client error with the documented shape.
    let (status, body) = server
        .delete_auth("/api/v5/banned/not-a-kind/w1-02-del", &token)
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    assert!(body["message"].is_string());
}

#[tokio::test]
async fn authz_sources_mask_secrets_on_read_and_keep_them_on_marker_put() {
    #[cfg(test)]
    const SECRET: &str = "known-authz-secret-4d70";
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // Only the built-in database source can be written through the API
    // in this build; it carries the secret in its config.
    let (status, answer) = server
        .put_auth(
            "/api/v5/authorization/sources/built_in_database",
            json!({"type": "built_in_database", "enable": true,
                       "config": {"password": SECRET, "token": SECRET}}),
            &token,
        )
        .await;
    assert_eq!(status, 204, "{answer}");
    // A password source from an older file is read through the same API.
    state
        .authz_sources
        .insert(broker_auth::AuthzSourceEntry {
            source_type: "mysql".to_string(),
            enable: false,
            config: json!({"server": "127.0.0.1:3306", "database": "mqtt",
                               "username": "u", "password": SECRET,
                               "query": "SELECT 1"}),
        })
        .expect("seed mysql source");
    let (status, listed) = server
        .get_auth("/api/v5/authorization/sources", &token)
        .await;
    assert_eq!(status, 200);
    assert!(!listed.to_string().contains(SECRET), "list shows a secret");
    assert!(listed.to_string().contains("******"));
    for kind in ["built_in_database", "mysql"] {
        let path = format!("/api/v5/authorization/sources/{kind}");
        let (status, shown) = server.get_auth(&path, &token).await;
        assert_eq!(status, 200, "{kind}");
        assert!(!shown.to_string().contains(SECRET), "{kind} shows a secret");
    }
    let raw = || serde_json::to_string(&state.config.snapshot().authz_sources).unwrap();
    let before = raw();
    let (_, shown) = server
        .get_auth("/api/v5/authorization/sources/built_in_database", &token)
        .await;
    let mut put = shown.clone();
    put.as_object_mut().unwrap().remove("metrics");
    let (status, answer) = server
        .put_auth(
            "/api/v5/authorization/sources/built_in_database",
            put,
            &token,
        )
        .await;
    assert_eq!(status, 204, "{answer}");
    let after = raw();
    assert_eq!(
        after.matches(SECRET).count(),
        before.matches(SECRET).count()
    );
    assert!(!after.contains("******"), "the mask was stored");
}

#[tokio::test]
async fn removed_authchain_authz_routes_return_404() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // `GET /authorization/sources` is intentionally absent here: it
    // was re-added on the real source registry by W2-17 (C04-08), so
    // it answers 200 and is covered by the registry round-trip tests.
    for (method, path) in [("GET", "/api/v5/authentication/x")] {
        let (status, _) = server
                .request_raw(&format!(
                    "{method} {path} HTTP/1.0\r\nHost: test\r\nAuthorization: Bearer {token}\r\nConnection: close\r\n\r\n"
                ))
                .await;
        assert_eq!(status, 404, "{method} {path} must be gone");
    }
}

#[tokio::test]
async fn authz_sources_list_create_clear_round_trip() {
    // Source registry plus list/create routes and the built-in
    // catch-all clear (W2-17, C04-08..C04-10) through the broker:
    // default list holds only the built-in source, an unavailable
    // fresh kind is refused, an older stored source still lists,
    // duplicates are rejected, and the catch-all empties the
    // built-in rule table. Before this task every route here was
    // missing (bare 404).
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Default registry: only the built-in database source, empty
    // collection shape (sources list), never an error.
    let (status, body) = server
        .get_auth("/api/v5/authorization/sources", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(
        body["sources"],
        json!([{"type": "built_in_database", "enable": true}])
    );

    // A source kind this build does not evaluate is refused before
    // anything is stored (AZ-02).
    let (status, refused) = server
        .post_auth(
            "/api/v5/authorization/sources",
            json!({"type": "file", "config": {"path": "/etc/authz/rules.conf"}}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(refused["code"], json!("BAD_REQUEST"));

    // An older stored source still lists and reports without a live
    // claim; seed it directly to model the recovered state.
    state
        .authz_sources
        .insert(broker_auth::AuthzSourceEntry {
            source_type: "file".to_string(),
            enable: true,
            config: json!({"path": "/etc/authz/rules.conf"}),
        })
        .expect("seed the stored source");

    // List again: the entry is present in stored order.
    let (status, body) = server
        .get_auth("/api/v5/authorization/sources", &token)
        .await;
    assert_eq!(status, 200);
    let types: Vec<_> = body["sources"]
        .as_array()
        .expect("data is a list")
        .iter()
        .map(|row| row["type"].clone())
        .collect();
    assert_eq!(types, vec![json!("built_in_database"), json!("file")]);
    assert_eq!(body["sources"].as_array().expect("sources list").len(), 2);

    // Re-creating the same type is a clean duplicate rejection, never
    // a silent merge.
    let (status, body) = server
        .post_auth(
            "/api/v5/authorization/sources",
            json!({"type": "file"}),
            &token,
        )
        .await;
    assert_eq!(
        status, 400,
        "duplicate source must be rejected, got: {body}"
    );
    assert_eq!(body["code"], json!("ALREADY_EXISTS"));

    // Unknown source types are 404 with the documented shape.
    let (status, body) = server
        .post_auth(
            "/api/v5/authorization/sources",
            json!({"type": "nowhere"}),
            &token,
        )
        .await;
    assert_eq!(status, 404, "unknown source type must be 404, got: {body}");
    assert_eq!(body["code"], json!("NOT_FOUND"));

    // The catch-all clear reports success even with rules present.
    let (status, _) = server
        .post_auth(
            "/api/v5/authorization/sources/built_in_database/rules",
            json!({"action": "publish", "topic": "w217/#", "permission": "deny"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .delete_auth(
            "/api/v5/authorization/sources/built_in_database/rules",
            &token,
        )
        .await;
    assert_eq!(status, 204);
    // Clearing again still succeeds.
    let (status, _) = server
        .delete_auth(
            "/api/v5/authorization/sources/built_in_database/rules",
            &token,
        )
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn authz_sources_survive_restart() {
    // Registry persistence across a restart through the config
    // registry: register through the broker, reload the same data
    // dir in a fresh server, and read the sources back.
    let dir = unique_data_dir("authz-sources");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    state
        .authz_sources
        .insert(broker_auth::AuthzSourceEntry {
            source_type: "redis".to_string(),
            enable: true,
            config: Value::Null,
        })
        .expect("seed the stored source");
    // Side effect: login_as_admin persists the admin password change
    // through the registry so the reload below can authenticate.
    server.login_as_admin().await;
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
        .get_auth("/api/v5/authorization/sources", &token)
        .await;
    assert_eq!(status, 200);
    let types: Vec<_> = body["sources"]
        .as_array()
        .expect("data is a list")
        .iter()
        .map(|row| row["type"].clone())
        .collect();
    assert_eq!(types, vec![json!("built_in_database"), json!("redis")]);
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn authz_user_replace_order_delete_round_trip() {
    // Per-user replace plus order-replace plus per-type delete on real
    // state (W2-22, C04-23..C04-25) through the broker: seed a user
    // set, replace it and read back; submit a bad order naming an
    // unknown type expecting rejection; delete a second source then
    // delete unknown expecting 404. Before this task all three routes
    // were missing (bare 404). The written allow is live on the
    // publish path (`MemoryAuth::authorize_publish` in
    // `crates/broker-auth/src/lib.rs`, consulted by the publish event
    // in `crates/broker-node/src/main.rs`).
    use broker_auth::Authorizer;
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Seed one user rule, then replace the whole set and read back.
    let (status, _) = server
            .post_auth(
                "/api/v5/authorization/sources/built_in_database/rules/users",
                json!({"username": "w222-http", "topic": "w222/http/old/#", "permission": "allow", "action": "publish"}),
                &token,
            )
            .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .put_auth(
            "/api/v5/authorization/sources/built_in_database/rules/users/w222-http",
            json!({"rules": [
                {"topic": "w222/http/a/#", "permission": "allow", "action": "publish"},
                {"topic": "w222/http/b/#", "permission": "deny", "action": "subscribe"},
            ]}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth(
            "/api/v5/authorization/sources/built_in_database/rules/users/w222-http",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let rules = body["rules"].as_array().expect("rules array");
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0]["username"], json!("w222-http"));
    assert_eq!(rules[0]["topic"], json!("w222/http/a/#"));
    assert_eq!(rules[1]["permission"], json!("deny"));

    // The stored allow is live on the publish path for that user.
    let topic = broker_protocol::Topic::new("w222/http/a/data").expect("topic parses");
    state
        .auth
        .authorize_publish("w222-http", &topic)
        .await
        .expect("user allow opens the publish path for its user");

    // A bad replace applies nothing: the stored set is unchanged.
    let (status, body) = server
        .put_auth(
            "/api/v5/authorization/sources/built_in_database/rules/users/w222-http",
            json!({"rules": [
                {"topic": "w222/http/ok/#", "permission": "allow", "action": "publish"},
                {"topic": "", "permission": "allow", "action": "publish"},
            ]}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, body) = server
        .get_auth(
            "/api/v5/authorization/sources/built_in_database/rules/users/w222-http",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["rules"].as_array().expect("rules array").len(), 2);

    // An older stored source is seeded directly, then a bad order
    // naming an unknown type is rejected and the stored order is
    // unchanged.
    state
        .authz_sources
        .insert(broker_auth::AuthzSourceEntry {
            source_type: "file".to_string(),
            enable: true,
            config: Value::Null,
        })
        .expect("seed the stored source");
    let (status, body) = server
        .put_auth(
            "/api/v5/authorization/sources/order",
            json!([{"type": "built_in_database"}, {"type": "nowhere"}]),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, body) = server
        .get_auth("/api/v5/authorization/sources", &token)
        .await;
    assert_eq!(status, 200);
    let types: Vec<_> = body["sources"]
        .as_array()
        .expect("data is a list")
        .iter()
        .map(|row| row["type"].clone())
        .collect();
    assert_eq!(types, vec![json!("built_in_database"), json!("file")]);

    // A valid order swaps the evaluation order and reads back swapped.
    let (status, _) = server
        .put_auth(
            "/api/v5/authorization/sources/order",
            json!([{"type": "file"}, {"type": "built_in_database"}]),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/authorization/sources", &token)
        .await;
    assert_eq!(status, 200);
    let types: Vec<_> = body["sources"]
        .as_array()
        .expect("data is a list")
        .iter()
        .map(|row| row["type"].clone())
        .collect();
    assert_eq!(types, vec![json!("file"), json!("built_in_database")]);

    // Delete the second source, then an unknown type is 404.
    let (status, _) = server
        .delete_auth("/api/v5/authorization/sources/file", &token)
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/authorization/sources", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["sources"].as_array().expect("sources list").len(), 1);
    let (status, body) = server
        .delete_auth("/api/v5/authorization/sources/nowhere", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));

    // Deleting the last live source is refused instead of leaving
    // authorization open.
    let (status, body) = server
        .delete_auth("/api/v5/authorization/sources/built_in_database", &token)
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
}

#[tokio::test]
async fn mv09b_client_rules_http_round_trip_order_replace_refused() {
    // Per-client rules through the production router: create, list
    // with paging, read, replace, delete, order, merge and refusal.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Empty index reads as an empty page, never an error.
    let (status, body) = server
        .get_auth(
            "/api/v5/authorization/sources/built_in_database/rules/clients",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["data"], json!([]));
    assert_eq!(body["meta"]["count"], json!(0));

    // Create one rule. Success is 204 with no body.
    let (status, _) = server
            .post_auth(
                "/api/v5/authorization/sources/built_in_database/rules/clients",
                json!({"clientid": "mv09b-c1", "topic": "mv09b/c1/#", "permission": "allow", "action": "publish"}),
                &token,
            )
            .await;
    assert_eq!(status, 204);

    // Duplicate scope is rejected, never merged.
    let (status, body) = server
            .post_auth(
                "/api/v5/authorization/sources/built_in_database/rules/clients",
                json!({"clientid": "mv09b-c1", "topic": "mv09b/c1/#", "permission": "deny", "action": "publish"}),
                &token,
            )
            .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("ALREADY_EXISTS"));

    // Malformed bodies are rejected and store nothing new.
    for bad in [
        json!({"topic": "mv09b/x/#", "permission": "allow", "action": "publish"}),
        json!({"clientid": "", "topic": "mv09b/x/#", "permission": "allow", "action": "publish"}),
        json!({"clientid": "*", "topic": "mv09b/x/#", "permission": "allow", "action": "publish"}),
        json!({"clientid": "mv09b-x", "topic": "", "permission": "allow", "action": "publish"}),
        json!({"clientid": "mv09b-x", "topic": "mv09b/x/#", "permission": "maybe", "action": "publish"}),
        json!({"clientid": "mv09b-x", "topic": "mv09b/x/#", "permission": "allow", "action": "forward"}),
    ] {
        let (status, body) = server
            .post_auth(
                "/api/v5/authorization/sources/built_in_database/rules/clients",
                bad,
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
    }

    // Seed two more clients so paging has three rows.
    for (id, topic) in [("mv09b-c2", "mv09b/c2/#"), ("mv09b-c3", "mv09b/c3/#")] {
        let (status, _) = server
            .post_auth(
                "/api/v5/authorization/sources/built_in_database/rules/clients",
                json!({"clientid": id, "topic": topic, "permission": "allow", "action": "publish"}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
    }
    let (status, body) = server
        .get_auth(
            "/api/v5/authorization/sources/built_in_database/rules/clients?page=2&limit=1",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["data"].as_array().expect("data array").len(), 1);
    assert_eq!(body["meta"]["count"], json!(3));

    // Detail read carries the key plus the rule set.
    let (status, body) = server
        .get_auth(
            "/api/v5/authorization/sources/built_in_database/rules/clients/mv09b-c1",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["clientid"], json!("mv09b-c1"));
    assert_eq!(body["rules"].as_array().expect("rules array").len(), 1);

    // Unknown ids read as 404 with the documented shape.
    let (status, body) = server
        .get_auth(
            "/api/v5/authorization/sources/built_in_database/rules/clients/mv09b-missing",
            &token,
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));

    // Replace sets order: first match wins. Broad allow first opens
    // the narrow topic.
    let (status, _) = server
        .put_auth(
            "/api/v5/authorization/sources/built_in_database/rules/clients/mv09b-ord",
            json!({"rules": [
                {"topic": "mv09b/order/#", "permission": "allow", "action": "publish"},
                {"topic": "mv09b/order/secret/#", "permission": "deny", "action": "publish"},
            ]}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    // Prove at socket: first-match allow admits the publish.
    let mut client = WsClient::connect(server.port).await;
    client
        .send_bin(&mqtt_connect("mv09b-ord", None, None))
        .await;
    let connack = client.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    client
        .send_bin(&mqtt_publish("mv09b/order/secret/x", 1, 1, b"x"))
        .await;
    let puback = client.recv_msg().await.expect("puback");
    assert_eq!(mqtt_packet_type(&puback), 4);
    let (status, body) = server
        .get_auth("/api/v5/clients/mv09b-ord/authorization/cache", &token)
        .await;
    assert_eq!(status, 200);
    let entries = body.as_array().expect("cache is a bare list");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["access"], json!("publish"));
    assert_eq!(entries[0]["topic"], json!("mv09b/order/secret/x"));
    assert_eq!(entries[0]["result"], json!("allow"));

    // Reversed order denies the same probe.
    let (status, _) = server
        .put_auth(
            "/api/v5/authorization/sources/built_in_database/rules/clients/mv09b-ord",
            json!({"rules": [
                {"topic": "mv09b/order/secret/#", "permission": "deny", "action": "publish"},
                {"topic": "mv09b/order/#", "permission": "allow", "action": "publish"},
            ]}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    // Prove at socket: reversed order, first match denies.
    let mut client = WsClient::connect(server.port).await;
    client
        .send_bin(&mqtt_connect("mv09b-ord", None, None))
        .await;
    let connack = client.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    client
        .send_bin(&mqtt_publish("mv09b/order/secret/x", 2, 1, b"x"))
        .await;
    let puback = client.recv_msg().await.expect("puback");
    assert_eq!(mqtt_packet_type(&puback), 4);
    let (status, body) = server
        .get_auth("/api/v5/clients/mv09b-ord/authorization/cache", &token)
        .await;
    assert_eq!(status, 200);
    let entries = body.as_array().expect("cache is a bare list");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["access"], json!("publish"));
    assert_eq!(entries[0]["topic"], json!("mv09b/order/secret/x"));
    assert_eq!(entries[0]["result"], json!("deny"));

    // Replace drops omitted rules, it never merges.
    let (status, _) = server
        .put_auth(
            "/api/v5/authorization/sources/built_in_database/rules/clients/mv09b-ord",
            json!({"rules": [
                {"topic": "mv09b/order/kept/#", "permission": "allow", "action": "publish"},
            ]}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth(
            "/api/v5/authorization/sources/built_in_database/rules/clients/mv09b-ord",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["rules"].as_array().expect("rules array").len(), 1);
    assert_eq!(body["rules"][0]["topic"], json!("mv09b/order/kept/#"));

    // A refused replace keeps the active policy.
    let (status, body) = server
        .put_auth(
            "/api/v5/authorization/sources/built_in_database/rules/clients/mv09b-ord",
            json!({"rules": [
                {"topic": "mv09b/order/ok/#", "permission": "allow", "action": "publish"},
                {"topic": "", "permission": "allow", "action": "publish"},
            ]}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, body) = server
        .get_auth(
            "/api/v5/authorization/sources/built_in_database/rules/clients/mv09b-ord",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["rules"].as_array().expect("rules array").len(), 1);
    // Prove at socket: refused replace keeps the stored allow policy.
    let mut client = WsClient::connect(server.port).await;
    client
        .send_bin(&mqtt_connect("mv09b-ord", None, None))
        .await;
    let connack = client.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    client
        .send_bin(&mqtt_publish("mv09b/order/kept/x", 3, 1, b"x"))
        .await;
    let puback = client.recv_msg().await.expect("puback");
    assert_eq!(mqtt_packet_type(&puback), 4);
    let (status, body) = server
        .get_auth("/api/v5/clients/mv09b-ord/authorization/cache", &token)
        .await;
    assert_eq!(status, 200);
    let entries = body.as_array().expect("cache is a bare list");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["access"], json!("publish"));
    assert_eq!(entries[0]["topic"], json!("mv09b/order/kept/x"));
    assert_eq!(entries[0]["result"], json!("allow"));

    // Duplicate scope inside one batch is rejected and keeps policy.
    let (status, _) = server
        .put_auth(
            "/api/v5/authorization/sources/built_in_database/rules/clients/mv09b-ord",
            json!({"rules": [
                {"topic": "mv09b/order/dup/#", "permission": "allow", "action": "publish"},
                {"topic": "mv09b/order/dup/#", "permission": "deny", "action": "publish"},
            ]}),
            &token,
        )
        .await;
    assert_eq!(status, 400);

    // Delete removes every rule for the id. A second delete is 404.
    let (status, _) = server
        .delete_auth(
            "/api/v5/authorization/sources/built_in_database/rules/clients/mv09b-ord",
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .delete_auth(
            "/api/v5/authorization/sources/built_in_database/rules/clients/mv09b-ord",
            &token,
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
}

#[tokio::test]
async fn mv09b_changed_client_rule_applies_without_reconnect() {
    // A changed client rule steers the next operation on a connected
    // client. The client stays connected throughout: HTTP changes the
    // rule, the next subscribe on the same connection follows it.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let mut client = WsClient::connect(server.port).await;
    client
        .send_bin(&mqtt_connect("mv09b-noreconn", None, None))
        .await;
    let connack = client.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);

    // Empty table is open, so the first subscribe succeeds.
    client
        .send_bin(&mqtt_subscribe(7, &[("mv09b/noreconn/#", 0)]))
        .await;
    let suback = client.recv_msg().await.expect("suback");
    assert_eq!(mqtt_packet_type(&suback), 9);
    assert_eq!(&suback[4..], &[0]);

    // Deny the same filter over HTTP. The next subscribe on the same
    // connection is refused without a reconnect.
    let (status, _) = server
            .post_auth(
                "/api/v5/authorization/sources/built_in_database/rules/clients",
                json!({"clientid": "mv09b-noreconn", "topic": "mv09b/noreconn/#", "permission": "deny", "action": "subscribe"}),
                &token,
            )
            .await;
    assert_eq!(status, 204);
    client
        .send_bin(&mqtt_subscribe(8, &[("mv09b/noreconn/#", 0)]))
        .await;
    let suback = client.recv_msg().await.expect("suback");
    assert_eq!(mqtt_packet_type(&suback), 9);
    assert_eq!(&suback[4..], &[0x80]);

    // Delete the rule over HTTP. The next subscribe on the same
    // connection succeeds again, still without a reconnect.
    let (status, _) = server
        .delete_auth(
            "/api/v5/authorization/sources/built_in_database/rules/clients/mv09b-noreconn",
            &token,
        )
        .await;
    assert_eq!(status, 204);
    client
        .send_bin(&mqtt_subscribe(9, &[("mv09b/noreconn/#", 0)]))
        .await;
    let suback = client.recv_msg().await.expect("suback");
    assert_eq!(mqtt_packet_type(&suback), 9);
    assert_eq!(&suback[4..], &[0]);
}

#[tokio::test]
async fn authz_catch_all_rules_round_trip() {
    // Built-in catch-all table plus the three `/all` routes on real
    // state (W2-18, C04-11..C04-13) through the broker: write two
    // catch-all rules, read expecting both; delete and read expecting
    // empty. Before this task all three routes were missing (bare
    // 404). The written allow is live on the publish path
    // (`MemoryAuth::authorize_publish` in
    // `crates/broker-auth/src/lib.rs`, consulted by the publish event
    // in `crates/broker-node/src/main.rs`).
    use broker_auth::Authorizer;
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Empty reads as an empty collection, never an error; not paged.
    let (status, body) = server
        .get_auth(
            "/api/v5/authorization/sources/built_in_database/rules/all",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["rules"], json!([]));

    // Replace with two rules; success is 204 with no body.
    let (status, _) = server
        .post_auth(
            "/api/v5/authorization/sources/built_in_database/rules/all",
            json!({"rules": [
                {"topic": "w218/http/alpha/#", "permission": "allow", "action": "publish"},
                {"topic": "w218/http/beta/#", "permission": "deny", "action": "subscribe"},
            ]}),
            &token,
        )
        .await;
    assert_eq!(status, 204);

    // Read back: both entries with the documented per-rule fields.
    let (status, body) = server
        .get_auth(
            "/api/v5/authorization/sources/built_in_database/rules/all",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let rules = body["rules"].as_array().expect("rules array").clone();
    assert_eq!(rules.len(), 2);
    assert_eq!(rules[0]["topic"], json!("w218/http/alpha/#"));
    assert_eq!(rules[0]["permission"], json!("allow"));
    assert_eq!(rules[0]["action"], json!("publish"));
    assert_eq!(rules[1]["topic"], json!("w218/http/beta/#"));
    assert_eq!(rules[1]["permission"], json!("deny"));
    assert_eq!(rules[1]["action"], json!("subscribe"));

    // The stored allow is live on the broker publish path.
    let topic = broker_protocol::Topic::new("w218/http/alpha/data").expect("topic parses");
    state
        .auth
        .authorize_publish("any-client", &topic)
        .await
        .expect("catch-all allow opens the publish path");

    // An invalid replace is a client error and applies nothing.
    let (status, body) = server
        .post_auth(
            "/api/v5/authorization/sources/built_in_database/rules/all",
            json!({"rules": [
                {"topic": "w218/http/ok/#", "permission": "allow", "action": "publish"},
                {"topic": "", "permission": "allow", "action": "publish"},
            ]}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, body) = server
        .get_auth(
            "/api/v5/authorization/sources/built_in_database/rules/all",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["rules"].as_array().expect("rules array").len(), 2);

    // Delete clears the list and reports success; clearing empty
    // still succeeds.
    let (status, _) = server
        .delete_auth(
            "/api/v5/authorization/sources/built_in_database/rules/all",
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth(
            "/api/v5/authorization/sources/built_in_database/rules/all",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["rules"], json!([]));
    let (status, _) = server
        .delete_auth(
            "/api/v5/authorization/sources/built_in_database/rules/all",
            &token,
        )
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn authz_settings_round_trip_rejection_and_restart() {
    // Settings round trip through the broker (W2-16, C04-06/07):
    // GET defaults, PUT a full valid update answering 200 with the
    // stored settings, GET expecting the update; a
    // subscribe/publish/deliver flow proves publish and deliver never
    // touch this store. Invalid PUTs are rejected with no change;
    // partial bodies reset missing fields to defaults. Settings
    // persist across a restart via the registry. Before this task
    // both routes were 404.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Defaults read back with the documented fields.
    let (status, body) = server
        .get_auth("/api/v5/authorization/settings", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["no_match"], json!("deny"));
    assert_eq!(body["deny_action"], json!("ignore"));
    assert_eq!(body["cache"]["enable"], json!(true));
    assert_eq!(body["cache"]["max_size"], json!(32));
    assert_eq!(body["cache"]["ttl"], json!("1m"));
    assert_eq!(body["cache"]["excludes"], json!([]));
    assert_eq!(body["include_mountpoint"], json!(false));
    assert_eq!(body["ignore_backend_failures"], json!(false));

    // Full valid replace through the broker: the write answers 200
    // with the stored settings.
    let (status, answered) = server
        .put_auth(
            "/api/v5/authorization/settings",
            json!({
                "no_match": "allow",
                "deny_action": "disconnect",
                "cache": {
                    "enable": false,
                    "max_size": 64,
                    "ttl": "60s",
                    "excludes": ["nocache/#"],
                },
                "include_mountpoint": false,
                "ignore_backend_failures": true,
            }),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(answered["no_match"], json!("allow"));
    assert_eq!(answered["deny_action"], json!("disconnect"));
    assert_eq!(answered["cache"]["max_size"], json!(64));
    assert_eq!(answered["cache"]["excludes"], json!(["nocache/#"]));
    let (status, body) = server
        .get_auth("/api/v5/authorization/settings", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body, answered, "the write answers what the read reports");

    // A subscribe/publish/deliver flow still works: publish and
    // deliver never touch this store.
    let mut sub = WsClient::connect(server.port).await;
    sub.send_bin(&mqtt_connect("w2-16-sub", None, None)).await;
    let connack = sub.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    sub.send_bin(&mqtt_subscribe(7, &[("w2-16/t", 1)])).await;
    let suback = sub.recv_msg().await.expect("suback");
    assert_eq!(mqtt_packet_type(&suback), 9);
    let mut publ = WsClient::connect(server.port).await;
    publ.send_bin(&mqtt_connect("w2-16-pub", None, None)).await;
    let connack = publ.recv_msg().await.expect("connack");
    assert_eq!(connack[3], 0);
    publ.send_bin(&mqtt_publish("w2-16/t", 42, 1, b"hi-w2-16"))
        .await;
    let puback = publ.recv_msg().await.expect("puback");
    assert_eq!(mqtt_packet_type(&puback), 4);
    let delivery = sub.recv_msg().await.expect("delivery");
    assert_eq!(mqtt_packet_type(&delivery), 3);
    assert!(delivery.ends_with(b"hi-w2-16"));

    // Unknown and out-of-range fields are rejected fail-closed with
    // no change applied.
    for bad in [
        json!({"no_match": "sometimes"}),
        json!({"deny_action": "log"}),
        json!({"cache": {"max_size": 0}}),
        json!({"cache": {"max_size": 2_000_000}}),
        json!({"cache": {"ttl": "soon"}}),
        json!({"cache": {"excludes": [""]}}),
        json!({"no_match": "deny", "bogus": 1}),
        json!({"cache": {"max_size": 64, "bogus": true}}),
    ] {
        let (status, body) = server
            .put_auth("/api/v5/authorization/settings", bad, &token)
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(body["message"].is_string());
    }
    let (status, body) = server
        .get_auth("/api/v5/authorization/settings", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body, answered, "failed writes must not apply");

    // Partial bodies replace: missing fields reset to defaults.
    let (status, answered) = server
        .put_auth(
            "/api/v5/authorization/settings",
            json!({"no_match": "deny"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(answered["no_match"], json!("deny"));
    assert_eq!(answered["deny_action"], json!("ignore"));
    assert_eq!(answered["cache"]["max_size"], json!(32));
    assert_eq!(answered["cache"]["excludes"], json!([]));

    // Persistence across a restart through the config registry.
    let dir = unique_data_dir("authz-settings");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let (status, _) = server
        .put_auth(
            "/api/v5/authorization/settings",
            json!({
                "no_match": "allow",
                "deny_action": "disconnect",
                "cache": {
                    "enable": false,
                    "max_size": 64,
                    "ttl": "60s",
                    "excludes": ["nocache/#"],
                },
                "include_mountpoint": false,
                "ignore_backend_failures": true,
            }),
            &token,
        )
        .await;
    assert_eq!(status, 200);
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
        .get_auth("/api/v5/authorization/settings", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["no_match"], json!("allow"));
    assert_eq!(body["deny_action"], json!("disconnect"));
    assert_eq!(body["cache"]["max_size"], json!(64));
    assert_eq!(body["cache"]["excludes"], json!(["nocache/#"]));
    assert_eq!(body["ignore_backend_failures"], json!(true));
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}
