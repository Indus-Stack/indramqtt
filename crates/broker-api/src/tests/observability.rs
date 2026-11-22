use super::*;

#[test]
fn standalone_needs_no_filesystem() {
    let engine = Arc::new(RuleEngine::new(
        16,
        broker_rules::BackpressurePolicy::DropOldest,
    ));
    let state = ApiState::standalone(engine);
    assert_eq!(
        *state.config.snapshot(),
        broker_config::FullSnapshot::default(),
        "standalone exposes validated defaults"
    );
    assert_eq!(state.node_id, "indra-node-1");
    assert!(
        !state.config.dir().exists(),
        "standalone must not create any directory, found: {}",
        state.config.dir().display()
    );
}

#[tokio::test]
async fn test_api_router_health() {
    let (server, _state) = TestServer::start().await;
    let (status, text) = server
        .request_raw("GET /healthz HTTP/1.0\r\nHost: test\r\nConnection: close\r\n\r\n")
        .await;
    assert_eq!(status, 200);
    assert_eq!(text, "OK");
}

/// Empty states of the console pages (AP-01, AP2-02): the reads answer
/// the documented shape with an empty or disabled state, and the writes
/// refuse with the documented 400 code. Nothing is stored.
#[tokio::test]
async fn test_console_empty_states_are_honest() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // AI lists read empty; the Flow Designer loads both together.
    let (status, body) = server.get_auth("/api/v5/ai/providers", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
    let (status, body) = server
        .get_auth("/api/v5/ai/completion_profiles", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
    let (status, body) = server
        .get_auth("/api/v5/ai/providers/no-such-provider/models", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
    let (status, body) = server
        .post_auth(
            "/api/v5/ai/providers",
            json!({"name": "x", "type": "openai", "api_key": "k"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("INVALID_CREDENTIAL"));
    let (status, body) = server
        .post_auth(
            "/api/v5/ai/completion_profiles",
            json!({"name": "x", "provider_name": "y", "type": "openai"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("INVALID_COMPLETION_PROFILE"));
    let (status, body) = server
        .post_auth(
            "/api/v5/ai/models",
            json!({"type": "openai", "api_key": "k"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("INVALID_CREDENTIAL"));
    let (status, body) = server
        .get_auth("/api/v5/ai/providers/no-such-provider", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));

    // Queue and stream configs read disabled; lists read empty.
    let (status, body) = server.get_auth("/api/v5/queues/config", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["enable"], json!(false));
    assert!(body["auto_create"]["lastvalue"].get("limits").is_some());
    assert!(body["auto_create"]["regular"].get("limits").is_some());
    let (status, body) = server.get_auth("/api/v5/streams/config", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["enable"], json!(false));
    assert!(body["auto_create"]["lastvalue"].get("limits").is_some());
    assert!(body["auto_create"]["regular"].get("limits").is_some());
    let (status, body) = server.get_auth("/api/v5/queues", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
    let (status, body) = server.get_auth("/api/v5/streams", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
    let (status, body) = server
        .put_auth("/api/v5/queues/config", json!({"enable": true}), &token)
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, body) = server
        .post_auth("/api/v5/queues", json!({"name": "q1"}), &token)
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("ALREADY_EXISTS"));
    let (status, body) = server
        .post_auth("/api/v5/streams", json!({"name": "s1"}), &token)
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("ALREADY_EXISTS"));

    // Telemetry reads disabled; data misses while disabled.
    let (status, body) = server.get_auth("/api/v5/telemetry/status", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!({"enable": false}));
    let (status, body) = server.get_auth("/api/v5/telemetry/data", &token).await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));

    // Rebalance and eviction read idle; writes refuse.
    let (status, body) = server
        .get_auth("/api/v5/load_rebalance/status", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body, json!({"status": "disabled"}));
    let (status, body) = server
        .get_auth("/api/v5/load_rebalance/global_status", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["rebalances"], json!([]));
    let (status, body) = server
        .get("/api/v5/load_rebalance/availability_check")
        .await;
    assert_eq!(status, 200);
    assert_eq!(body, json!({}));
    let (status, body) = server
        .get_auth("/api/v5/node_eviction/status", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body, json!({"status": "disabled"}));
    let (status, body) = server
        .send(
            "POST",
            "/api/v5/load_rebalance/no-such-node/start",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
}

/// Honest markers of deferred functions (MV-15): gateways, schema
/// validations, OpenTelemetry export and cluster links. Reads answer
/// the documented shape with a true empty or disabled state. Writes
/// refuse with the documented 400 code. Nothing is stored, so a
/// second read shows the same empty state.
#[tokio::test]
async fn test_deferred_features_are_honest() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Gateways read empty; the console gateway page loads this list.
    let (status, body) = server.get_auth("/api/v5/gateways", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
    let (status, body) = server.get_auth("/api/v5/gateways/stomp", &token).await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    let (status, body) = server
        .put_auth("/api/v5/gateways/stomp", json!({"enable": true}), &token)
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    assert!(body["message"]
        .as_str()
        .is_some_and(|m| m.contains("not available")));
    let (status, body) = server
        .put_auth("/api/v5/gateways/stomp/enable/true", json!({}), &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    // A refused write stores nothing: the list still reads empty.
    let (status, body) = server.get_auth("/api/v5/gateways", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));

    // Schema validations read empty; writes refuse with the
    // documented codes and store nothing.
    let (status, body) = server.get_auth("/api/v5/schema_validations", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
    let (status, body) = server
            .post_auth(
                "/api/v5/schema_validations",
                json!({"name": "mv15-v1", "strategy": "all_pass", "failure_action": "drop", "checks": [], "topics": "mv15/t"}),
                &token,
            )
            .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("ALREADY_EXISTS"));
    let (status, body) = server
            .put_auth(
                "/api/v5/schema_validations",
                json!({"name": "mv15-v1", "strategy": "all_pass", "failure_action": "drop", "checks": [], "topics": "mv15/t"}),
                &token,
            )
            .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, body) = server
        .get_auth("/api/v5/schema_validations/validation/mv15-v1", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    let (status, _) = server
        .delete_auth("/api/v5/schema_validations/validation/mv15-v1", &token)
        .await;
    assert_eq!(status, 404);
    let (status, body) = server.get_auth("/api/v5/schema_validations", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));

    // OpenTelemetry reads disabled; the write refuses and stores
    // nothing.
    let (status, body) = server.get_auth("/api/v5/opentelemetry", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["type"], json!("generic"));
    assert_eq!(body["logs"]["enable"], json!(false));
    assert_eq!(body["metrics"]["enable"], json!(false));
    assert_eq!(body["traces"]["enable"], json!(false));
    let (status, refused) = server
        .put_auth(
            "/api/v5/opentelemetry",
            json!({"metrics": {"enable": true}}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(refused["code"], json!("BAD_REQUEST"));
    // A refused write stores nothing: the read still shows disabled.
    let (status, reread) = server.get_auth("/api/v5/opentelemetry", &token).await;
    assert_eq!(status, 200);
    assert_eq!(reread, body);

    // Cluster links read empty; the write refuses and stores nothing.
    let (status, body) = server.get_auth("/api/v5/cluster/links", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
    let (status, body) = server
        .post_auth(
            "/api/v5/cluster/links",
            json!({"name": "mv15-link", "server": "127.0.0.1:1883"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, body) = server.get_auth("/api/v5/cluster/links", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
}

#[tokio::test]
async fn test_nodes_reports_status_version_connections() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    state.metrics.set_active_connections(3);

    let (status, body) = server.get_auth("/api/v1/nodes", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["status"], json!("running"));
    assert_eq!(body["version"], json!(env!("CARGO_PKG_VERSION")));
    assert_eq!(body["connections"], json!(3));
    assert!(body["node_id"].is_string());
}

#[tokio::test]
async fn sessions_count_counts_sessions_as_a_json_string() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Zero sessions read as the string "0", not an error.
    let (status, body) = server.get_auth("/api/v5/sessions_count", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!("0"));

    // Two sessions read as the string "2".
    for (id, conn) in [("w1-13-a", 13_101u64), ("w1-13-b", 13_102u64)] {
        let (session, _) = state.sessions.get_or_create(id, true);
        state.sessions.bind_session(&session, conn);
    }
    let (status, body) = server.get_auth("/api/v5/sessions_count", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!("2"));

    // A detached session has not expired, so it stays in the count.
    state.sessions.unbind_connection("w1-13-a", 13_101);
    let (status, body) = server.get_auth("/api/v5/sessions_count", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!(state.sessions.session_count().to_string()));

    // `since` is a number of seconds. The broker keeps no expired
    // sessions, so the count does not change.
    let (status, body) = server
        .get_auth("/api/v5/sessions_count?since=1705391625", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body, json!(state.sessions.session_count().to_string()));
    let (status, body) = server
        .get_auth("/api/v5/sessions_count?since=yesterday", &token)
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));

    // Unknown parameters are ignored, not rejected.
    let (status, body) = server
        .get_auth("/api/v5/sessions_count?unknown_param=1", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body, json!(state.sessions.session_count().to_string()));
}

#[tokio::test]
async fn metrics_snapshot_is_flat_numeric_and_monotonic() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Documented counter names grouped as bytes, packets, messages,
    // delivery, client and session. Every value must stay numeric.
    let expected = [
        "bytes.received",
        "bytes.sent",
        "packets.received",
        "packets.sent",
        "packets.connect.received",
        "packets.connack.sent",
        "packets.connack.error",
        "packets.connack.auth_error",
        "packets.publish.received",
        "packets.publish.sent",
        "packets.subscribe.received",
        "packets.suback.sent",
        "packets.pingreq.received",
        "packets.pingresp.sent",
        "messages.received",
        "messages.sent",
        "messages.dropped",
        "messages.delivered",
        "messages.qos0.received",
        "messages.qos1.received",
        "messages.qos2.received",
        "delivery.dropped",
        "delivery.dropped.no_subscribers",
        "delivery.dropped.queue_full",
        "client.connect",
        "client.connack",
        "client.connected",
        "client.subscribe",
    ];

    // First read on a quiet node: flat object, no paging envelope.
    let (status, first) = server.get_auth("/api/v5/metrics", &token).await;
    assert_eq!(status, 200);
    let first_map = first.as_object().expect("metrics is a flat object");
    assert!(
        first.get("data").is_none(),
        "metrics is not paged: {first:?}"
    );
    assert!(
        first.get("meta").is_none(),
        "metrics has no meta: {first:?}"
    );
    for name in expected {
        let value = first_map
            .get(name)
            .unwrap_or_else(|| panic!("missing {name}"));
        assert!(value.is_number(), "{name} is numeric: {value:?}");
    }

    // Traffic between reads through the real counters: one connect,
    // one publish ingress and its fan-out, plus measured frame bytes.
    state.metrics.inc_connect_received();
    state.metrics.inc_connack_sent();
    state.metrics.inc_publish_received();
    state.metrics.inc_messages_received();
    state.metrics.inc_qos1_received();
    state.metrics.inc_bytes_received_by(128);
    state.metrics.inc_messages_forwarded();
    state.metrics.inc_publish_sent();
    state.metrics.inc_delivered();
    state.metrics.inc_bytes_sent_by(64);
    state.metrics.inc_subscribe_received();
    state.metrics.inc_suback_sent();
    state.metrics.inc_pingreq_received();
    state.metrics.inc_pingresp_sent();

    // Second read: every documented counter is non-decreasing.
    let (status, second) = server.get_auth("/api/v5/metrics", &token).await;
    assert_eq!(status, 200);
    let second_map = second.as_object().expect("metrics stays a flat object");
    for name in expected {
        let before = first_map[name]
            .as_u64()
            .unwrap_or_else(|| panic!("{name} numeric"));
        let after = second_map[name]
            .as_u64()
            .unwrap_or_else(|| panic!("{name} numeric"));
        assert!(after >= before, "{name} moves forward: {before} -> {after}");
    }
    assert!(second_map["messages.received"].as_u64().unwrap() >= 1);
    assert!(second_map["packets.publish.received"].as_u64().unwrap() >= 1);
    assert!(second_map["bytes.received"].as_u64().unwrap() >= 128);

    // Unknown parameters are ignored, not rejected.
    let (status, third) = server
        .get_auth("/api/v5/metrics?unknown_param=1", &token)
        .await;
    assert_eq!(status, 200);
    let third_map = third.as_object().expect("metrics with query stays flat");
    for name in expected {
        assert!(third_map.get(name).is_some(), "unknown query keeps {name}");
    }
}

#[tokio::test]
async fn topics_list_and_detail_round_trip() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Empty index reads as an empty page, not an error.
    let (status, body) = server.get_auth("/api/v5/topics", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["data"], json!([]));
    assert_eq!(body["meta"]["count"], json!(0));
    assert_eq!(body["meta"]["page"], json!(1));
    assert_eq!(body["meta"]["hasnext"], json!(false));

    // Subscribing alone never creates a topic row.
    state.sessions.get_or_create("w1-15-sub", true);
    let (status, _) = server
        .post_auth(
            "/api/v5/clients/w1-15-sub/subscribe",
            json!({"topic": "w1-15/filter", "qos": 0}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, body) = server.get_auth("/api/v5/topics", &token).await;
    assert_eq!(status, 200);
    assert_eq!(
        body["data"],
        json!([]),
        "subscribe must not index: {body:?}"
    );

    // Publish two concrete topics through the management publish path.
    for topic in ["w1-15/alpha", "w1-15/beta"] {
        let (status, _) = server
            .post_auth(
                "/api/v5/publish",
                json!({"topic": topic, "payload": "hi", "qos": 0}),
                &token,
            )
            .await;
        assert_eq!(status, 202);
    }

    // List shows both entries with the documented per-entry fields.
    let (status, body) = server.get_auth("/api/v5/topics", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["meta"]["count"], json!(2));
    assert_eq!(body["meta"]["hasnext"], json!(false));
    let data = body["data"].as_array().expect("topics list has data");
    assert_eq!(data.len(), 2, "both topics listed: {data:?}");
    // Deterministic order: sorted by topic.
    assert_eq!(data[0]["topic"], json!("w1-15/alpha"));
    assert_eq!(data[1]["topic"], json!("w1-15/beta"));
    for entry in data {
        assert_eq!(entry["node"], json!(crate::node_scope::local_node_name()));
        assert!(entry["topic"].is_string());
    }

    // W0 paging is honoured: one row per page.
    let (status, first) = server
        .get_auth("/api/v5/topics?page=1&limit=1", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(first["meta"]["count"], json!(2));
    assert_eq!(first["meta"]["hasnext"], json!(true));
    assert_eq!(first["data"].as_array().unwrap().len(), 1);
    assert_eq!(first["data"][0]["topic"], json!("w1-15/alpha"));

    let (status, second) = server
        .get_auth("/api/v5/topics?page=2&limit=1", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(second["data"].as_array().unwrap().len(), 1);
    assert_eq!(second["data"][0]["topic"], json!("w1-15/beta"));

    // Unknown query keys do not break the list call.
    let (status, body) = server
        .get_auth("/api/v5/topics?unknown_param=1", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["meta"]["count"], json!(2));

    // Detail hit returns the documented record (slashed names arrive
    // percent-encoded and match exactly after decoding).
    let (status, body) = server
        .get_auth("/api/v5/topics/w1-15%2Falpha", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    assert_eq!(body[0]["topic"], json!("w1-15/alpha"));
    assert_eq!(body[0]["node"], json!(crate::node_scope::local_node_name()));

    // Detail miss returns the documented not-found shape; prefix and
    // filter forms never match.
    for missing in ["w1-15%2Fmissing", "w1-15%2Falp", "w1-15%2Falpha%2Fx"] {
        let (status, body) = server
            .get_auth(&format!("/api/v5/topics/{missing}"), &token)
            .await;
        assert_eq!(status, 404, "unknown topic must 404: {missing}");
        assert_eq!(body["code"], json!("TOPIC_NOT_FOUND"));
        assert!(body["message"].is_string());
    }
}

#[tokio::test]
async fn alarms_raise_list_clear_round_trip() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Empty store reads as an empty page, not an error.
    let (status, body) = server.get_auth("/api/v5/alarms", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["data"], json!([]));
    assert_eq!(body["meta"]["count"], json!(0));
    assert_eq!(body["meta"]["page"], json!(1));
    assert_eq!(body["meta"]["hasnext"], json!(false));

    // No deactivated history yet either.
    let (status, body) = server
        .get_auth("/api/v5/alarms?activated=false", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["data"], json!([]));
    assert_eq!(body["meta"]["count"], json!(0));

    // Raise two test alarms directly on the store (alarms have no
    // creation endpoint; the kernel raises them internally).
    state
        .alarms
        .activate("w1-16-b-alarm", "second alarm", serde_json::json!({}));
    state.alarms.activate(
        "w1-16-a-alarm",
        "first alarm",
        serde_json::json!({"high_watermark": 70}),
    );

    // Listing carries both entries with the documented per-alarm fields.
    let (status, body) = server.get_auth("/api/v5/alarms", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["meta"]["count"], json!(2));
    assert_eq!(body["meta"]["hasnext"], json!(false));
    let data = body["data"].as_array().expect("alarms list has data");
    assert_eq!(data.len(), 2, "both alarms listed: {data:?}");
    // Deterministic order: sorted by name.
    assert_eq!(data[0]["name"], json!("w1-16-a-alarm"));
    assert_eq!(data[1]["name"], json!("w1-16-b-alarm"));
    for entry in data {
        assert_eq!(entry["node"], json!(crate::node_scope::local_node_name()));
        assert!(entry["name"].is_string());
        assert!(entry["message"].is_string());
        assert!(entry["details"].is_object());
        assert!(entry["duration"].is_number());
        assert!(entry["activate_at"].is_string());
        assert!(entry["activate_at"]
            .as_str()
            .is_some_and(|s| s.contains('T')));
        assert!(entry.get("deactivate_at").is_none());
    }
    assert_eq!(data[0]["message"], json!("first alarm"));
    assert_eq!(data[0]["details"], json!({"high_watermark": 70}));

    // W0 paging is honoured: one row per page.
    let (status, first) = server
        .get_auth("/api/v5/alarms?page=1&limit=1", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(first["meta"]["count"], json!(2));
    assert_eq!(first["meta"]["hasnext"], json!(true));
    assert_eq!(first["data"].as_array().unwrap().len(), 1);
    assert_eq!(first["data"][0]["name"], json!("w1-16-a-alarm"));

    let (status, second) = server
        .get_auth("/api/v5/alarms?page=2&limit=1", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(second["data"].as_array().unwrap().len(), 1);
    assert_eq!(second["data"][0]["name"], json!("w1-16-b-alarm"));

    // Unknown query keys do not break the list call.
    let (status, body) = server
        .get_auth("/api/v5/alarms?unknown_param=1", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["meta"]["count"], json!(2));

    // Clearing deactivates everything; the next list is empty again
    // while the deactivated history keeps what was cleared.
    let (status, _) = server
        .delete_auth("/api/v5/alarms?unknown_param=1", &token)
        .await;
    assert_eq!(status, 204);
    let (status, body) = server.get_auth("/api/v5/alarms", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["data"], json!([]));
    assert_eq!(body["meta"]["count"], json!(0));
    let (status, body) = server
        .get_auth("/api/v5/alarms?activated=false", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["meta"]["count"], json!(2));
    let history = body["data"].as_array().expect("cleared alarms kept");
    assert_eq!(history.len(), 2);
    assert_eq!(history[0]["name"], json!("w1-16-a-alarm"));
    assert_ne!(history[0]["deactivate_at"], json!("infinity"));

    // Clearing an empty store still reports success.
    let (status, _) = server.delete_auth("/api/v5/alarms", &token).await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn alarms_force_deactivate_round_trip() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // With one active alarm, force-deactivate reports success and the
    // follow-up list reads empty.
    state.alarms.activate(
        "w1-17-alarm",
        "force me",
        serde_json::json!({"high_watermark": 70}),
    );
    let (status, body) = server.get_auth("/api/v5/alarms", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["meta"]["count"], json!(1));

    let (status, _) = server
        .post_auth(
            "/api/v5/alarms/force_deactivate",
            json!({"name": "w1-17-alarm"}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server.get_auth("/api/v5/alarms", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["data"], json!([]));
    assert_eq!(body["meta"]["count"], json!(0));

    // An empty body fails with bad request.
    let (status, body) = server
        .post_auth("/api/v5/alarms/force_deactivate", json!({}), &token)
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));

    // A named body deactivates just that alarm; the other stays listed.
    state
        .alarms
        .activate("w1-17-a-alarm", "first alarm", serde_json::json!({}));
    state
        .alarms
        .activate("w1-17-b-alarm", "second alarm", serde_json::json!({}));
    let (status, _) = server
        .post_auth(
            "/api/v5/alarms/force_deactivate",
            json!({"name": "w1-17-a-alarm"}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server.get_auth("/api/v5/alarms", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["meta"]["count"], json!(1));
    assert_eq!(body["data"][0]["name"], json!("w1-17-b-alarm"));

    // Deactivating an unknown name fails and changes nothing.
    let (status, body) = server
        .post_auth(
            "/api/v5/alarms/force_deactivate",
            json!({"name": "w1-17-missing"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, body) = server.get_auth("/api/v5/alarms", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["meta"]["count"], json!(1));
    let (status, _) = server
        .post_auth(
            "/api/v5/alarms/force_deactivate",
            json!({"name": "w1-17-b-alarm"}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server.get_auth("/api/v5/alarms", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["data"], json!([]));
}

#[tokio::test]
async fn test_metrics_exposes_prometheus_counters() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    state.metrics.inc_messages_received();
    state.metrics.inc_messages_forwarded_by(2);
    state.metrics.inc_rules_executed();
    state.metrics.set_active_connections(1);

    let (status, text) = server
            .request_raw(&format!(
                "GET /api/v1/metrics HTTP/1.0\r\nHost: test\r\nAuthorization: Bearer {token}\r\nConnection: close\r\n\r\n"
            ))
            .await;
    assert_eq!(status, 200);
    assert!(text.contains("indramqtt_messages_received_total 1\n"));
    assert!(text.contains("indramqtt_messages_forwarded_total 2\n"));
    assert!(text.contains("indramqtt_rules_executed_total 1\n"));
    assert!(text.contains("indramqtt_connections_active 1\n"));
}

#[tokio::test]
async fn test_dashboard_routes() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (server, _state) = TestServer::start().await;

    // GET / redirects to the console.
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", server.port))
        .await
        .expect("connect");
    stream
        .write_all(b"GET / HTTP/1.0\r\nHost: test\r\nConnection: close\r\n\r\n")
        .await
        .expect("write");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read");
    let text = String::from_utf8(buf).expect("UTF-8");
    // Hyper echoes the HTTP/1.0 request version in the status line.
    assert!(text.contains(" 303 "), "root must redirect, got: {text}");
    assert!(
        text.to_ascii_lowercase().contains("location: /dashboard"),
        "redirect must target /dashboard, got: {text}"
    );

    // GET /dashboard serves the SPA shell.
    let (status, html) = server
        .request_raw("GET /dashboard HTTP/1.0\r\nHost: test\r\nConnection: close\r\n\r\n")
        .await;
    assert_eq!(status, 200);
    for marker in [
        "<title>IndraMQTT Console</title>",
        "Community Edition (MIT)",
        "id=\"metrics\"",
        "id=\"clients\"",
        "id=\"sql-studio\"",
        "id=\"connectors\"",
        "id=\"auth\"",
        "id=\"console\"",
        "rule-tpl-math",
        "rule-tpl-tumbling",
        "value=\"mysql\"",
        "value=\"clickhouse\"",
        "value=\"influxdb\"",
        "value=\"s3\"",
        "value=\"elasticsearch\"",
        "value=\"timescaledb\"",
        "conn-mysql-url",
        "conn-ch-endpoint",
        "conn-influx-endpoint",
        "conn-s3-bucket",
        "conn-es-index",
        "conn-ts-hypertable",
        "value=\"webhook\"",
        "value=\"mqtt_bridge\"",
        "value=\"disk_log\"",
        "value=\"sparkplug_b",
        "value=\"kinesis\"",
        "value=\"gcp_pubsub\"",
        "value=\"azure_eventhubs\"",
        "value=\"pulsar\"",
        "value=\"mongodb\"",
        "value=\"mssql\"",
        "value=\"cassandra\"",
        "value=\"couchbase\"",
        "value=\"tdengine\"",
        "value=\"iotdb\"",
        "value=\"timestream\"",
        "value=\"dynamodb\"",
        "value=\"snowflake\"",
        "value=\"databricks\"",
        "value=\"doris\"",
        "value=\"bigquery\"",
        "value=\"redshift\"",
        "value=\"azure_blob\"",
        "value=\"tablestore\"",
        "value=\"confluent\"",
        "value=\"rocketmq\"",
        "value=\"oracle\"",
        "value=\"cockroachdb\"",
        "value=\"alloydb\"",
        "value=\"opentsdb\"",
        "value=\"greptimedb\"",
        "value=\"datalayers\"",
        "conn-azblob-account",
        "conn-ots-endpoint",
        "conn-cfl-servers",
        "conn-rmq-endpoints",
        "conn-ora-url",
        "conn-crdb-url",
        "conn-alloy-host",
        "conn-otsdb-endpoint",
        "conn-grep-endpoint",
        "conn-dl-endpoint",
        "conn-hook-url",
        "conn-bridge-address",
        "conn-disk-dir",
        "conn-spb-prefix",
        "conn-kinesis-stream",
        "conn-gcp-project",
        "conn-azure-ns",
        "conn-pulsar-url",
        "conn-mongo-url",
        "conn-mssql-host",
        "conn-cass-contact",
        "conn-couch-url",
        "conn-td-endpoint",
        "conn-iotdb-device",
        "conn-ts-database",
        "conn-ddb-table",
        ".badge.community",
        ".badge.enterprise",
        "/ws/mqtt",
    ] {
        assert!(html.contains(marker), "dashboard missing {marker}");
    }
}

#[tokio::test]
async fn test_swagger_and_openapi_routes() {
    let (server, _state) = TestServer::start().await;

    // Test /swagger serves HTML
    let (status, html) = server
        .request_raw("GET /swagger HTTP/1.0\r\nHost: test\r\nConnection: close\r\n\r\n")
        .await;
    assert_eq!(status, 200);
    assert!(html.contains("SwaggerUIBundle"));
    assert!(html.contains("/api-docs/openapi.json"));

    // Test /api-docs/openapi.json serves valid OpenAPI 3.0 specification
    let (status, spec) = server.get("/api-docs/openapi.json").await;
    assert_eq!(status, 200);
    assert_eq!(spec["openapi"], "3.0.3");
    assert_eq!(spec["info"]["title"], "IndraMQTT Management API");
    assert!(spec["paths"]["/api/v1/nodes"]["get"].is_object());
    assert!(spec["paths"]["/api/v1/clients"]["get"].is_object());
    assert!(spec["paths"]["/api/v1/rules"]["get"].is_object());
    assert!(spec["paths"]["/api/v1/connectors"]["get"].is_object());
}

#[tokio::test]
async fn test_modern_ui_routes() {
    let (server, _state) = TestServer::start().await;

    // Test /ui serves the modern React SPA (or fallback)
    let (status, html) = server
        .request_raw("GET /ui HTTP/1.0\r\nHost: test\r\nConnection: close\r\n\r\n")
        .await;
    assert_eq!(status, 200);
    assert!(html.len() > 100);
}

#[tokio::test]
async fn removed_gateway_routes_return_404() {
    // MV-15 owns `GET /gateways`, `GET+PUT /gateways/{name}` and
    // `PUT /gateways/{name}/enable/{enable}` as honest markers: the
    // list reads `[]`, an unknown name misses with 404, and writes
    // refuse. The deeper per-gateway sub-resources stay removed.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, body) = server.get_auth("/api/v5/gateways", &token).await;
    assert_eq!(status, 200, "honest route GET /gateways must be 200");
    assert_eq!(body, json!([]));
    for path in ["/api/v5/gateways/stomp/clients/c1", "/api/v5/gateway"] {
        let (status, _) = server
                .request_raw(&format!(
                    "GET {path} HTTP/1.0\r\nHost: test\r\nAuthorization: Bearer {token}\r\nConnection: close\r\n\r\n"
                ))
                .await;
        assert_eq!(status, 404, "removed route {path} must be 404");
    }
}

#[tokio::test]
async fn listeners_routes_answer_documented_shapes() {
    // AP-02 restores the per-listener detail and lifecycle routes on
    // the live listener store; list coverage lives with the store
    // tests in `v5::listeners`.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, body) = server.get_auth("/api/v5/listeners", &token).await;
    assert_eq!(status, 200, "kept route GET /api/v5/listeners must be 200");
    assert!(body.is_array(), "listeners list is an array: {body}");
    let (status, body) = server
        .get_auth("/api/v5/listeners/tcp:default", &token)
        .await;
    assert_eq!(status, 200, "GET /api/v5/listeners/tcp:default must be 200");
    assert_eq!(body["id"], serde_json::json!("tcp:default"));
    let (status, body) = server.get_auth("/api/v5/listeners_status", &token).await;
    assert_eq!(status, 200, "GET /api/v5/listeners_status must be 200");
    assert!(body.is_array(), "listeners status is an array: {body}");
    let (status, body) = server
        .get_auth("/api/v5/listeners/tcp:missing", &token)
        .await;
    assert_eq!(status, 404, "unknown listener must be 404");
    assert_eq!(body["code"], serde_json::json!("BAD_LISTENER_ID"));
    let (status, body) = server
        .post_auth(
            "/api/v5/listeners/tcp:missing/stop",
            serde_json::json!({}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "stop of unknown listener must be 400");
    assert_eq!(body["code"], serde_json::json!("BAD_LISTENER_ID"));
}

#[tokio::test]
async fn removed_trace_slowsub_topicmetrics_routes_return_404() {
    // W1-30 restores `GET`/`DELETE /api/v5/slow_subscriptions` on the
    // real recorder, W1-31 restores `GET`/`PUT
    // /api/v5/slow_subscriptions/settings` on the real thresholds and
    // W1-33 restores `GET`/`POST`/`DELETE /api/v5/trace` on the real
    // registry, so only the per-session trace reads and topic-metrics
    // routes stay removed here; trace create-list-clear coverage lives
    // with the registry tests.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    for path in ["/api/v5/trace/x/log", "/api/v5/mqtt/topic_metrics"] {
        let (status, _) = server.get_auth(path, &token).await;
        assert_eq!(status, 404, "removed route {path} must be 404");
    }
    let (status, _) = server.get_auth("/api/v5/trace", &token).await;
    assert_eq!(status, 200, "kept route GET /api/v5/trace must be 200");
    let (status, _) = server.delete_auth("/api/v5/trace", &token).await;
    assert_eq!(status, 204, "kept route DELETE /api/v5/trace must be 204");
    let (status, _) = server.get_auth("/api/v5/slow_subscriptions", &token).await;
    assert_eq!(
        status, 200,
        "kept route GET /api/v5/slow_subscriptions must be 200"
    );
    let (status, _) = server
        .delete_auth("/api/v5/slow_subscriptions", &token)
        .await;
    assert_eq!(
        status, 204,
        "kept route DELETE /api/v5/slow_subscriptions must be 204"
    );
    let (status, _) = server
        .get_auth("/api/v5/slow_subscriptions/settings", &token)
        .await;
    assert_eq!(
        status, 200,
        "kept route GET /api/v5/slow_subscriptions/settings must be 200"
    );
    let (status, _) = server
        .put_auth(
            "/api/v5/slow_subscriptions/settings",
            json!({"threshold": "1s"}),
            &token,
        )
        .await;
    assert_eq!(
        status, 200,
        "kept route PUT /api/v5/slow_subscriptions/settings must be 200"
    );
}

#[tokio::test]
async fn trace_stop_and_log_detail_round_trip_through_api() {
    // Stop and detail read go through the HTTP router. Create one
    // session, read its file facts, stop it, and read the facts
    // again. A missing session reads as 404 on both routes.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, _) = server.delete_auth("/api/v5/trace", &token).await;
    assert_eq!(status, 204);
    let (status, created) = server
        .post_auth(
            "/api/v5/trace",
            json!({"name": "http-trace-1", "type": "topic", "topic": "http/trace/#"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert!(created["log_size"].is_array());
    let (status, detail) = server
        .get_auth("/api/v5/trace/http-trace-1/log_detail", &token)
        .await;
    assert_eq!(status, 200);
    assert!(detail.as_array().is_some_and(|rows| rows.len() == 1));
    assert!(detail[0]["node"].is_string());
    assert!(detail[0]["size"].as_u64().is_some());
    assert!(detail[0]["mtime"].as_u64().is_some());
    let (status, stopped) = server
        .put_auth("/api/v5/trace/http-trace-1/stop", json!(null), &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(stopped["status"], json!("stopped"));
    assert!(stopped["log_size"].is_array());
    let (status, _) = server
        .get_auth("/api/v5/trace/http-trace-1/log_detail", &token)
        .await;
    assert_eq!(status, 200);
    let (status, _) = server
        .get_auth("/api/v5/trace/no-such-trace/log_detail", &token)
        .await;
    assert_eq!(status, 404);
    let (status, _) = server
        .put_auth("/api/v5/trace/no-such-trace/stop", json!(null), &token)
        .await;
    assert_eq!(status, 404);
    let (status, _) = server.delete_auth("/api/v5/trace", &token).await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn removed_unsupported_retained_routes_return_404() {
    // AP-04 restores `GET /api/v5/mqtt/delayed/messages` on the real
    // scheduler, so it leaves the removed list here: it answers 200
    // with the documented page. The remaining paths stay removed.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    for path in [
        "/api/v5/mqtt/retained",
        "/api/v5/configs/a2a_registry",
        "/api/v5/indra/extra_features",
    ] {
        let (status, _) = server.get_auth(path, &token).await;
        assert_eq!(status, 404, "removed route {path} must be 404");
    }
    let (status, body) = server
        .get_auth("/api/v5/mqtt/delayed/messages", &token)
        .await;
    assert_eq!(status, 200, "restored delayed list must be 200");
    assert!(body.get("data").and_then(|v| v.as_array()).is_some());
    assert_eq!(body["meta"]["count"], serde_json::json!(0));
    let (status, _) = server.delete_auth("/api/v5/mqtt/retained", &token).await;
    assert_eq!(
        status, 404,
        "removed route DELETE /api/v5/mqtt/retained must be 404"
    );
}

#[tokio::test]
async fn removed_system_fake_routes_return_404() {
    // AP-03 restores `GET /api/v5/configs` on the real registry.
    // It answers 200 with each routed root. Management-plane only.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, body) = server.get_auth("/api/v5/configs", &token).await;
    assert_eq!(status, 200, "restored route /api/v5/configs must be 200");
    for root in [
        "broker",
        "log",
        "sysmon",
        "sys_topics",
        "alarm",
        "dashboard",
        "global_zone",
    ] {
        assert!(body.get(root).is_some(), "root {root} is present");
    }
}

#[tokio::test]
async fn configs_dashboard_reports_default_password() {
    // AP2-05 failing test: `GET /configs/dashboard` carries the
    // documented `default_password`. Before: the root read as `{}`.
    // Broker-through: the read goes through the served management
    // API on `ApiState`. The value is the true initial admin
    // password, never a stand-in.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, body) = server.get_auth("/api/v5/configs/dashboard", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["default_password"], json!("public"));
}

#[tokio::test]
async fn mqtt_group_delayed_rewrite_topicmetrics_over_http() {
    // AP-04 failing test: the `/mqtt` group answers the documented
    // shapes through the served management API. Before: delayed
    // routes 404, rewrite routes 404, topic-metrics v2 routes 404.
    // Broker-through: delayed reads run on the shared scheduler,
    // rewrite reads and writes run on the shared router, and the
    // honest-empty collection routes store nothing.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Delayed switch plus cap: read defaults, write, read back.
    let (status, before) = server.get_auth("/api/v5/mqtt/delayed", &token).await;
    assert_eq!(status, 200);
    assert_eq!(before["enable"], json!(true));
    assert!(before["max_delayed_messages"].as_u64().is_some());
    let (status, after) = server
        .put_auth(
            "/api/v5/mqtt/delayed",
            json!({"enable": false, "max_delayed_messages": 7}),
            &token,
        )
        .await;
    assert_eq!(status, 200, "delayed update: {after}");
    assert_eq!(after["enable"], json!(false));
    assert_eq!(after["max_delayed_messages"], json!(7));
    let (status, reread) = server.get_auth("/api/v5/mqtt/delayed", &token).await;
    assert_eq!(status, 200);
    assert_eq!(reread, after);
    // Bad switch and cap values fail without applying.
    for bad in [
        json!({"enable": "yes"}),
        json!({"max_delayed_messages": -1}),
        json!({"max_delayed_messages": 1_000_001}),
        json!("enable"),
    ] {
        let (status, err) = server.put_auth("/api/v5/mqtt/delayed", bad, &token).await;
        assert_eq!(status, 400, "bad delayed write must fail: {err}");
        assert_eq!(err["code"], json!("BAD_REQUEST"));
    }
    let (status, kept) = server.get_auth("/api/v5/mqtt/delayed", &token).await;
    assert_eq!(status, 200);
    assert_eq!(kept, after, "failed writes must not apply");

    // Empty delayed list reads as an empty page with the meta.
    let (status, empty) = server
        .get_auth("/api/v5/mqtt/delayed/messages", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(empty["data"], json!([]));
    assert_eq!(empty["meta"]["count"], json!(0));
    assert!(empty["meta"]["page"].as_u64().is_some());
    assert!(empty["meta"]["limit"].as_u64().is_some());

    // Detail and removal misses carry the documented codes.
    let (status, err) = server
        .get_auth("/api/v5/mqtt/delayed/messages/no-such-node/1", &token)
        .await;
    assert_eq!(status, 400);
    assert_eq!(err["code"], json!("INVALID_NODE"));
    let (status, err) = server
        .delete_auth("/api/v5/mqtt/delayed/messages/no-such-node/1", &token)
        .await;
    assert_eq!(status, 400);
    assert_eq!(err["code"], json!("INVALID_NODE"));

    // Rewrite list starts empty, replaces, and rejects bad rules.
    let (status, before) = server.get_auth("/api/v5/mqtt/topic_rewrite", &token).await;
    assert_eq!(status, 200);
    assert_eq!(before, json!([]));
    let (status, after) = server
        .put_auth(
            "/api/v5/mqtt/topic_rewrite",
            json!([{
                "action": "publish",
                "source_topic": "conf/http-rw/#",
                "dest_topic": "rewritten/$1",
                "re": "^conf/http-rw/(.*)$",
            }]),
            &token,
        )
        .await;
    assert_eq!(status, 200, "rewrite update: {after}");
    assert_eq!(after[0]["action"], json!("publish"));
    assert_eq!(after[0]["source_topic"], json!("conf/http-rw/#"));
    let (status, err) = server
        .put_auth(
            "/api/v5/mqtt/topic_rewrite",
            json!([{"action": "bogus"}]),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(err["code"], json!("BAD_REQUEST"));
    let many: Vec<Value> = (0..65)
        .map(|i| {
            json!({
                "action": "publish",
                "source_topic": format!("conf/http-rw/{i}/#"),
                "dest_topic": "rewritten/$1",
                "re": format!("^conf/http-rw/{i}/(.*)$"),
            })
        })
        .collect();
    let (status, err) = server
        .put_auth("/api/v5/mqtt/topic_rewrite", json!(many), &token)
        .await;
    assert_eq!(status, 413);
    assert_eq!(err["code"], json!("EXCEED_LIMIT"));

    // Topic-metric collections stay honestly empty: the list reads
    // empty, single reads miss, creates and resets fail, and the
    // bulk delete reports success.
    let (status, body) = server.get_auth("/api/v5/mqtt/topic_metrics2", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
    let (status, err) = server
        .post_auth(
            "/api/v5/mqtt/topic_metrics2",
            json!({"name": "alpha", "topic_filter": "alpha/#"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(err["code"], json!("BAD_NAME"));
    let (status, err) = server
        .get_auth("/api/v5/mqtt/topic_metrics2/alpha", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(err["code"], json!("NAME_NOT_FOUND"));
    let (status, err) = server
        .delete_auth("/api/v5/mqtt/topic_metrics2/alpha", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(err["code"], json!("NAME_NOT_FOUND"));
    let (status, err) = server
        .put_auth(
            "/api/v5/mqtt/topic_metrics2/alpha/reset",
            json!(null),
            &token,
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(err["code"], json!("NAME_NOT_FOUND"));
    let (status, _) = server
        .delete_auth("/api/v5/mqtt/topic_metrics2", &token)
        .await;
    assert_eq!(status, 204);

    // The legacy v1 paths carry no operation in the specification,
    // so they keep no route and answer 404.
    let (status, _) = server.get_auth("/api/v5/mqtt/topic_metrics", &token).await;
    assert_eq!(status, 404, "legacy v1 topic_metrics must stay 404");
}

#[tokio::test]
async fn removed_cluster_inflight_routes_return_404() {
    // AP-03 restores `GET /api/v5/cluster` on the real registry.
    // It answers 200 with the local view. Management-plane only.
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, body) = server.get_auth("/api/v5/cluster", &token).await;
    assert_eq!(
        status, 200,
        "restored route GET /api/v5/cluster must be 200"
    );
    assert!(body["name"].is_string(), "cluster view carries a name");
    assert!(body["nodes"].is_array(), "cluster view carries nodes");
    assert!(body["self"].is_string(), "cluster view carries self");
    assert!(body["description"].is_string(), "cluster view carries text");
    for path in ["/api/v5/clients/any/inflight_messages"] {
        let (status, _) = server.get_auth(path, &token).await;
        assert_eq!(status, 404, "removed route GET {path} must be 404");
    }
    let (status, _) = server.get_auth("/api/v5/nodes", &token).await;
    assert_eq!(status, 200, "kept route GET /api/v5/nodes must be 200");
    state.sessions.get_or_create("mqueue-kept-1", true);
    let (status, _) = server
        .get_auth("/api/v5/clients/mqueue-kept-1/mqueue_messages", &token)
        .await;
    assert_eq!(
        status, 200,
        "kept route GET /api/v5/clients/:clientid/mqueue_messages must be 200"
    );
}

#[tokio::test]
async fn removed_monitoring_fake_routes_return_404() {
    // W1-16 restores `/api/v5/alarms` on the real store, W1-17
    // restores `POST /api/v5/alarms/force_deactivate`, W1-18
    // restores `GET /api/v5/metrics` on the real counters and W1-19
    // restores `GET`/`DELETE /api/v5/monitor` on the history store,
    // so no monitoring route stays removed here; metrics, alarms and
    // monitor coverage lives with their store tests below.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    for path in [
        "/api/v5/metrics",
        "/api/v5/monitor",
        "/api/v5/monitor_current",
        "/api/v5/stats",
    ] {
        let (status, _) = server.get_auth(path, &token).await;
        assert_eq!(status, 200, "kept route GET {path} must be 200");
    }
    let (status, _) = server.delete_auth("/api/v5/monitor", &token).await;
    assert_eq!(status, 204, "kept route DELETE /api/v5/monitor must be 204");
}

#[tokio::test]
async fn ui_schema_unknown_is_404() {
    // Named dashboard objects (C05-10, W2-31): known names return the
    // documented JSON string that holds the schema value; unknown
    // names are 404 with `{code, message}`. Goes through the broker
    // API server (authenticated request), not only the store.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, body) = server.get_auth("/api/v5/schemas/sources", &token).await;
    assert_eq!(status, 404, "sources alias must be 404");
    assert_eq!(body["code"], json!("NOT_FOUND"));
    assert!(body["message"].is_string());
    for name in ["actions", "connectors", "hotconf"] {
        let (status, body) = server
            .get_auth(&format!("/api/v5/schemas/{name}"), &token)
            .await;
        assert_eq!(status, 200, "served schema {name} must be 200");
        assert!(
            body.is_string(),
            "served schema {name} must be a JSON string"
        );
        let schema: serde_json::Value =
            serde_json::from_str(body.as_str().expect("schema body is a JSON string"))
                .expect("schema string holds JSON");
        assert!(schema.is_object(), "schema value for {name} is an object");
    }
    // Hot-config alias resolves to the same canonical value.
    let (status, alias) = server.get_auth("/api/v5/schemas/hot-config", &token).await;
    assert_eq!(status, 200);
    let (status, canonical) = server.get_auth("/api/v5/schemas/hotconf", &token).await;
    assert_eq!(status, 200);
    assert_eq!(alias, canonical, "alias must serve the canonical schema");
    let (status, body) = server.get_auth("/api/v5/schemas", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!(["hotconf", "actions", "connectors"]));
}

#[tokio::test]
async fn node_loads_serialise_as_numbers() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, body) = server.get_auth("/api/v5/nodes", &token).await;
    assert_eq!(status, 200);
    let first = &body[0];
    for field in ["load1", "load5", "load15"] {
        assert!(
            first[field].is_number(),
            "{field} must be a JSON number, got: {}",
            first[field]
        );
    }
    let node = first["node"].as_str().expect("node name").to_string();
    let (status, single) = server
        .get_auth(&format!("/api/v5/nodes/{node}"), &token)
        .await;
    assert_eq!(status, 200);
    for field in ["load1", "load5", "load15"] {
        assert!(
            single[field].is_number(),
            "{field} must be a JSON number, got: {}",
            single[field]
        );
    }
}

#[tokio::test]
async fn audit_log_lists_management_operations_with_paging_and_time_filter() {
    // W2-28 failing test: perform two management operations, query
    // the audit log expecting both entries with paging metadata;
    // query with a time filter excluding them expecting an empty
    // page. Before: 404 (no route).
    // Broker-through: every step goes through the served management
    // API on `ApiState` (the same state the kernel shares in
    // `crates/broker-node/src/main.rs`, `serve_api`), not only
    // through the store. The entries are recorded by the management
    // auth middleware (`crates/broker-api/src/api_auth.rs`,
    // `require_api_auth`) on each authenticated request; publish and
    // deliver never touch the ring.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Two management operations through the broker.
    let (status, _) = server
        .post_auth(
            "/api/v5/api_key",
            json!({"name": "w228-audit-a", "scopes": ["monitoring"]}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = server
        .post_auth(
            "/api/v5/api_key",
            json!({"name": "w228-audit-b", "scopes": ["monitoring"]}),
            &token,
        )
        .await;
    assert_eq!(status, 200);

    // Query the log: both entries read back with paging metadata.
    let (status, body) = server.get_auth("/api/v5/audit", &token).await;
    assert_eq!(status, 200);
    let data = body[0]["data"].as_array().expect("data is a list");
    assert!(
        data.len() >= 2,
        "both management operations are audited: {body:?}"
    );
    assert!(body[0]["meta"].is_object(), "page envelope carries meta");
    assert_eq!(
        body[0]["meta"]["page"],
        json!(1),
        "default page reports: {body:?}"
    );
    assert_eq!(
        body[0]["meta"]["limit"],
        json!(100),
        "default limit reports: {body:?}"
    );
    let ids: Vec<&str> = data
        .iter()
        .filter_map(|entry| entry["operation_id"].as_str())
        .collect();
    assert!(
        ids.iter().any(|id| id.contains("api_key")),
        "audit rows name the operated resource: {ids:?}"
    );
    for entry in data {
        assert!(entry["created_at"].is_string(), "row has a timestamp");
        assert_eq!(entry["from"], json!("rest_api"));
        assert_eq!(entry["operation_result"], json!("success"));
        assert!(entry["http_status_code"].is_number());
    }

    // Paged read keeps the envelope.
    let (status, paged) = server
        .get_auth("/api/v5/audit?page=1&limit=1", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(paged[0]["meta"]["page"], json!(1));
    assert_eq!(paged[0]["meta"]["limit"], json!(1));
    assert_eq!(paged[0]["data"].as_array().expect("paged data").len(), 1);

    // Unknown query parameters are ignored, not errors.
    let (status, _) = server
        .get_auth("/api/v5/audit?unknown_param=1", &token)
        .await;
    assert_eq!(status, 200);

    // A time window excluding both entries reads as an empty page,
    // not an error.
    let (status, empty) = server
        .get_auth("/api/v5/audit?gte_created_at=4102444800000000", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(empty[0]["data"], json!([]));
    assert_eq!(empty[0]["meta"]["count"], json!(0));

    // The outcome filter honours success.
    let (status, filtered) = server
        .get_auth("/api/v5/audit?operation_result=success", &token)
        .await;
    assert_eq!(status, 200);
    assert!(
        filtered[0]["meta"]["count"].as_u64().expect("count") >= 2,
        "success filter keeps both entries: {filtered:?}"
    );

    let (status, _) = server
        .delete_auth("/api/v5/api_key/w228-audit-a", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/api_key/w228-audit-b", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn audit_log_ignores_reads() {
    // AP2-05 failing test: a read through the API makes no audit
    // record. GET /clients then GET /audit shows no entry for the
    // read. Before: it had one.
    // Broker-through: both steps go through the served management
    // API on `ApiState`, not only through the store. The entries
    // are recorded by the management auth middleware
    // (`crates/broker-api/src/api_auth.rs`, `require_api_auth`).
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, _) = server.get_auth("/api/v5/clients", &token).await;
    assert_eq!(status, 200);
    let (status, body) = server.get_auth("/api/v5/audit", &token).await;
    assert_eq!(status, 200);
    let data = body[0]["data"].as_array().expect("data is a list");
    assert!(
        data.iter()
            .all(|entry| entry["operation_id"] != json!("/api/v5/clients")),
        "reads leave no audit record: {body:?}"
    );
    assert!(
        data.iter()
            .all(|entry| entry["http_method"] != json!("get")),
        "audit holds only documented methods: {body:?}"
    );
}
