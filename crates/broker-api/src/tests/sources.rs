use super::*;

#[tokio::test]
async fn source_metrics_move_on_real_ingress_and_reset_zeroes() {
    // W3-29 (C07-18/C07-19) broker-through test: create a source via
    // the W3-26 route, read zeroed metrics, drive real ingress through
    // `dispatch_ingress` (the broker's rule path, also used by
    // `crates/broker-node` ingress), read attributed counts, stop the
    // source and observe shed counts, reset and expect zeroes; unknown
    // ids are 404. Before: both routes were 404.
    // The per-source counters live in `broker-rules`
    // (`RuleEngine::source_metrics`, bumped in `attribute_source_ingress`
    // on every `dispatch_ingress`), driven by the publish event.
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

    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // Unknown ids miss with the documented shape, not an empty body.
    let (status, body) = server
        .get_auth("/api/v5/sources/no-such-source/metrics", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    let (status, body) = server
        .send(
            "PUT",
            "/api/v5/sources/no-such-source/metrics/reset",
            Some(json!({})),
            Some(&token),
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    // Create one source via the W3-26 route; metrics start at zero.
    let (status, _) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "w3-29-source", "type": "mqtt", "connector": "mqtt:up", "parameters": {"topic": "in/w3-29-source"}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    let (status, body) = server
        .get_auth("/api/v5/sources/w3-29-source/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["id"], json!("w3-29-source"));
    assert_eq!(body["metrics"]["received"], json!(0));
    assert_eq!(body["metrics"]["dropped"], json!(0));
    // Unknown query parameters are ignored on both routes.
    let (status, _) = server
        .get_auth("/api/v5/sources/w3-29-source/metrics?unknown=param", &token)
        .await;
    assert_eq!(status, 200);
    // Drive real ingress through the source: one publish through
    // `dispatch_ingress` (the broker's rule path), never constants.
    let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
        std::sync::Arc::new(NoopBrokerSink);
    let topic = broker_protocol::Topic::new("w3/29/ingress").expect("valid topic");
    let payload = bytes::Bytes::from_static(b"{\"k\":1}");
    state
        .engine
        .dispatch_ingress(
            &topic,
            &payload,
            broker_protocol::QoS::AtMostOnce,
            &broker_sink,
        )
        .await;
    // Counters moved only through the real ingress, never constants.
    let (status, body) = server
        .get_auth("/api/v5/sources/w3-29-source/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["metrics"]["received"], json!(1));
    assert_eq!(body["metrics"]["dropped"], json!(0));
    // Stopped sources shed into `dropped`: flip via the W3-28 enable
    // route (which mirrors into the engine snapshot), publish again,
    // and expect the drop counter to move while `received` holds.
    let (status, _) = server
        .send(
            "PUT",
            "/api/v5/sources/w3-29-source/enable/false",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    state
        .engine
        .dispatch_ingress(
            &topic,
            &payload,
            broker_protocol::QoS::AtMostOnce,
            &broker_sink,
        )
        .await;
    let (status, body) = server
        .get_auth("/api/v5/sources/w3-29-source/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["metrics"]["received"], json!(1));
    assert_eq!(body["metrics"]["dropped"], json!(1));
    // Reset zeroes and reports success; the next read is zero.
    let (status, _) = server
        .send(
            "PUT",
            "/api/v5/sources/w3-29-source/metrics/reset",
            Some(json!({})),
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/sources/w3-29-source/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["metrics"]["received"], json!(0));
    assert_eq!(body["metrics"]["dropped"], json!(0));
    // Cleanup: delete the source (which drops its metric record, so a
    // later re-create starts at zero).
    let (status, _) = server
        .delete_auth("/api/v5/sources/w3-29-source", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn removed_sources_rulenotest_routes_return_404() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // W3-31 restores `POST /api/v5/sources`, `DELETE
    // /api/v5/sources/:id` and `GET /api/v5/sources_summary` on the
    // real source directory, so `/api/v5/sources` is no longer a
    // removed route (GET on the POST-only path is 405); W3-11
    // restores `GET /api/v5/rule_events` on the real producer
    // registry, so no GET path stays removed here; W3-12 restores
    // `POST /api/v5/rule_test` on the real offline evaluator, so it
    // is no longer a removed route either; W3-16 restores `POST
    // /api/v5/rules/:id/test` on the stored-rule evaluator, so it is
    // no longer a removed route either.
    for path in ["/api/v5/rule_events_missing"] {
        let (status, _) = server.get_auth(path, &token).await;
        assert_eq!(status, 404, "removed route {path} must be 404");
    }
}

#[tokio::test]
async fn rule_test_offline_match_no_match_side_effect_free() {
    // W3-12 failing test: POST /rule_test evaluates a rule body
    // against a sample through the served management API on `ApiState`
    // (the same state the kernel shares in
    // `crates/broker-node/src/main.rs`, `serve_api`), reusing the real
    // rule compiler (`broker_rules::try_evaluate` in
    // `crates/broker-rules/src/lib.rs`). Before: route returned 404.
    // Broker-through: management POST plus a live ingress dispatch
    // through the engine, then a rules list proving nothing was stored.
    // Management-plane only; publish and deliver never touch this path.
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Matching sample reports matched output.
    let (status, body) = server
        .post_auth(
            "/api/v5/rule_test",
            json!({
                "sql": "SELECT temperature FROM \"sensors/+\" WHERE temperature > 0",
                "topic": "sensors/kitchen",
                "payload": {"temperature": 72.5, "secret": "x"},
            }),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["matched"], json!(true));
    assert_eq!(body["projected"], json!({"temperature": 72.5}));

    // Non-matching sample answers 412 with the documented code.
    let (status, body) = server
        .post_auth(
            "/api/v5/rule_test",
            json!({
                "sql": "SELECT * FROM \"sensors/+\" WHERE temperature > 100.0",
                "topic": "sensors/kitchen",
                "payload": {"temperature": 72.5},
            }),
            &token,
        )
        .await;
    assert_eq!(status, 412);
    assert_eq!(body["code"], json!("NOT_MATCH"));

    // Malformed rule bodies are rejected with the documented shape.
    let (status, body) = server
        .post_auth(
            "/api/v5/rule_test",
            json!({"sql": "SELECT WHERE WHERE", "topic": "a/b", "payload": {}}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));

    // Side-effect-free: no rule, action binding or retained state left
    // behind, and live routing still works through the broker.
    #[cfg(test)]
    struct W312Sink;
    #[async_trait::async_trait]
    #[cfg(test)]
    impl broker_rules::BrokerSink for W312Sink {
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
    let sink: std::sync::Arc<dyn broker_rules::BrokerSink> = std::sync::Arc::new(W312Sink);
    let topic = broker_protocol::Topic::new("sensors/kitchen").expect("topic");
    let payload = bytes::Bytes::from_static(b"{\"temperature\": 72.5}");
    let matched = state
        .engine
        .dispatch_ingress(&topic, &payload, broker_protocol::QoS::AtMostOnce, &sink)
        .await;
    assert_eq!(
        matched, 0,
        "no stored rule so live dispatch matches nothing"
    );
    let (status, body) = server.get_auth("/api/v5/rules", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["data"], json!([]));
    assert_eq!(state.engine.list_rules().len(), 0);
}

#[tokio::test]
async fn rule_test_one_match_no_match_side_effect_free() {
    // W3-16 failing test: POST /rules/:id/test evaluates the stored
    // rule against a sample through the served management API on
    // `ApiState` (the same state the kernel shares in
    // `crates/broker-node/src/main.rs`, `serve_api`), reusing the real
    // rule compiler (`broker_rules::try_evaluate` in
    // `crates/broker-rules/src/lib.rs`) bound to the stored SQL and
    // topic filter. Before: route returned 404.
    // Broker-through: management POSTs plus a live ingress dispatch
    // through the engine, then a metrics read proving the probe fired
    // no action and bumped no live counter.
    // Management-plane only; publish and deliver never touch this path.
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Create the stored rule the probe binds to.
    let (status, _) = server
        .send(
            "POST",
            "/api/v5/rules",
            Some(json!({
                "id": "w3-16-rule",
                "sql": "SELECT temperature FROM \"sensors/+\" WHERE temperature > 100.0",
                "actions": [],
            })),
            Some(&token),
        )
        .await;
    assert_eq!(status, 201);

    // Matching sample reports matched output.
    let (status, body) = server
        .post_auth(
            "/api/v5/rules/w3-16-rule/test",
            json!({
                "topic": "sensors/kitchen",
                "payload": {"temperature": 150.0, "secret": "x"},
            }),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["matched"], json!(true));
    assert_eq!(body["projected"], json!({"temperature": 150.0}));
    assert_eq!(body["output"], body["projected"]);

    // Non-matching sample answers 412 with the documented code.
    let (status, body) = server
        .post_auth(
            "/api/v5/rules/w3-16-rule/test",
            json!({
                "topic": "sensors/kitchen",
                "payload": {"temperature": 72.5},
            }),
            &token,
        )
        .await;
    assert_eq!(status, 412);
    assert_eq!(body["code"], json!("NOT_MATCH"));

    // A sample with no topic is a normal no-match.
    let (status, body) = server
        .post_auth(
            "/api/v5/rules/w3-16-rule/test",
            json!({"payload": {"temperature": 1.0}}),
            &token,
        )
        .await;
    assert_eq!(status, 412);
    assert_eq!(body["code"], json!("NOT_MATCH"));

    // Unknown rule id is 404 with the documented error code.
    let (status, body) = server
        .post_auth(
            "/api/v5/rules/no-such-rule/test",
            json!({
                "topic": "sensors/kitchen",
                "payload": {"temperature": 1.0},
            }),
            &token,
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("RULE_NOT_FOUND"));

    // Side-effect-free: the stored rule survives, live counters stayed
    // at zero (the probe bumps nothing), and live routing still works
    // through the broker.
    #[cfg(test)]
    struct W316Sink;
    #[async_trait::async_trait]
    #[cfg(test)]
    impl broker_rules::BrokerSink for W316Sink {
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
    let sink: std::sync::Arc<dyn broker_rules::BrokerSink> = std::sync::Arc::new(W316Sink);
    let topic = broker_protocol::Topic::new("sensors/kitchen").expect("topic");
    let payload = bytes::Bytes::from_static(b"{\"temperature\": 150.0}");
    let matched = state
        .engine
        .dispatch_ingress(&topic, &payload, broker_protocol::QoS::AtMostOnce, &sink)
        .await;
    assert_eq!(matched, 1, "stored rule still fires on live dispatch");
    let (status, body) = server
        .get_auth("/api/v5/rules/w3-16-rule/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(
        body["metrics"]["matched"],
        json!(1),
        "only the live dispatch bumped counters; offline probes bump nothing"
    );
    let (status, _) = server.delete_auth("/api/v5/rules/w3-16-rule", &token).await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn rule_events_catalogue_lists_only_produced_events() {
    // W3-11 failing test: GET /rule_events returns the catalogue of
    // event types rules can trigger on, built from the real producer
    // registry; every listed event has a real producer, and a
    // published message is seen by the rule trigger path behind the
    // listed event. Before: route returned 404.
    // Broker-through: the request goes through the served management
    // API on `ApiState` (the same state the kernel shares in
    // `crates/broker-node/src/main.rs`, `serve_api`); the catalogue
    // itself is owned by `broker-rules`
    // (`rule_event_catalogue` in `crates/broker-rules/src/events.rs`),
    // read only on this management GET and never consulted on publish
    // or deliver.
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, body) = server.get_auth("/api/v5/rule_events", &token).await;
    assert_eq!(status, 200);
    let entries = body.as_array().expect("catalogue is a list");
    assert!(!entries.is_empty(), "catalogue must not be empty");
    assert!(entries.len() <= broker_rules::events::MAX_RULE_EVENTS);
    for entry in entries {
        let event = entry["event"].as_str().expect("entry carries event");
        assert!(
            crate::v5::rule_events::is_produced_event(event),
            "catalogue lists unproduced event '{event}'"
        );
        assert!(
            entry["description"].as_str().is_some_and(|d| !d.is_empty()),
            "entry carries a summary: {entry}"
        );
    }
    let names: Vec<&str> = entries.iter().filter_map(|e| e["event"].as_str()).collect();
    assert!(
        names.contains(&"$events/client_connected"),
        "catalogue lists the connect producer"
    );
    assert!(
        !names.contains(&"$events/message_publish"),
        "catalogue lists no unproduced event"
    );

    // Unknown query parameters are ignored, never an error.
    let (status, again) = server
        .get_auth("/api/v5/rule_events?unknown=param", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(again, body);

    // Publish an event through the broker rule path: a rule over the
    // published topic fires, so the listed producer is real.
    state
        .engine
        .create_rule(
            "w311-broker-rule".to_string(),
            broker_protocol::TopicFilter::new("w311/#").expect("filter"),
            None,
            true,
            vec![broker_rules::RuleAction::Log],
        )
        .expect("rule creates");
    #[cfg(test)]
    struct FnSink;
    #[async_trait::async_trait]
    #[cfg(test)]
    impl broker_rules::BrokerSink for FnSink {
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
    let sink: std::sync::Arc<dyn broker_rules::BrokerSink> = std::sync::Arc::new(FnSink);
    let topic = broker_protocol::Topic::new("w311/probe").expect("topic");
    let payload = bytes::Bytes::from_static(b"{}");
    let matched = state
        .engine
        .dispatch_ingress(&topic, &payload, broker_protocol::QoS::AtMostOnce, &sink)
        .await;
    assert_eq!(matched, 1);
}

#[tokio::test]
async fn sources_probe_is_stateless_with_documented_shapes() {
    // W3-30 (C07-21) broker-through test: a valid probe succeeds without
    // creating a source, a bad config fails with the documented error
    // shape, and an unknown kind is rejected. Management-plane only;
    // publish and deliver never touch the probe path.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // Valid probe: known kind, nothing to dial, so reachable by
    // validation alone. Must not create a source: the summary still
    // reports zeroes (W3-31 restores the source directory, so the
    // absence check reads the live summary, not a 404).
    let (status, _) = server
        .post_auth("/api/v5/sources_probe", json!({"type": "mqtt"}), &token)
        .await;
    assert_eq!(status, 204);
    let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
    assert_eq!(status, 200);
    assert_eq!(
        body.as_array().map(Vec::len),
        Some(0),
        "probe must not create a source"
    );
    // Bad config: known kind with a closed port reports the failure.
    let (status, body) = server
        .post_auth(
            "/api/v5/sources_probe",
            json!({"type": "webhook", "url": "http://127.0.0.1:9"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "bad probe config must be 400");
    assert_eq!(body["code"], json!("TEST_FAILED"));
    // Unknown kind is rejected with the documented error shape.
    let (status, body) = server
        .post_auth(
            "/api/v5/sources_probe",
            json!({"type": "no_such_source_kind_xyz"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "unknown source kind must be 400");
    assert_eq!(body["code"], json!("TEST_FAILED"));
    // Malformed body is rejected with the documented error shape.
    let (status, body) = server
        .post_auth("/api/v5/sources_probe", json!({}), &token)
        .await;
    assert_eq!(status, 400, "malformed probe body must be 400");
    assert_eq!(body["code"], json!("TEST_FAILED"));
}

#[tokio::test]
async fn sources_summary_tracks_create_delete() {
    // W3-31 (C07-22) broker-through test: create two sources of
    // different states, read the summary expecting matching counts;
    // delete both and expect zeroes. Every assertion goes through
    // the HTTP router against the shared store, never at the store
    // directly. Management-plane only; publish and deliver never
    // touch the source directory.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // Empty store returns zeroes, not an error.
    let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
    // Two sources of different states: one enabled, one disabled.
    let (status, _) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "sum-src-a", "type": "mqtt", "connector": "mqtt:up", "parameters": {"topic": "in/sum-src-a"}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    let (status, _) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "sum-src-b", "type": "mqtt", "enable": false, "connector": "mqtt:up", "parameters": {"topic": "in/sum-src-b"}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    // Counts match the live snapshot; unknown query parameters are
    // ignored.
    let (status, body) = server
        .get_auth("/api/v5/sources_summary?unknown=param", &token)
        .await;
    assert_eq!(status, 200);
    let rows = body.as_array().expect("summary is a list");
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.iter().filter(|r| r["enable"] == json!(true)).count(),
        1
    );
    assert_eq!(
        rows.iter().filter(|r| r["enable"] == json!(false)).count(),
        1
    );
    assert!(rows
        .iter()
        .all(|r| r["name"].is_string() && r["type"].is_string()));
    // Delete of an unknown id is 404 with the documented shape.
    let (status, body) = server
        .delete_auth("/api/v5/sources/no-such-source", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    // Delete both; the summary returns to zeroes.
    let (status, _) = server
        .delete_auth("/api/v5/sources/sum-src-a", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/sources/sum-src-b", &token)
        .await;
    assert_eq!(status, 204);
    let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
}

#[tokio::test]
async fn sources_list_create_delete_round_trip() {
    // W3-26 (C07-12..C07-14) broker-through test: create a source,
    // list and expect it present with paging metadata; create the
    // same id again and expect rejection; delete it and expect a
    // second delete to be 404. Every assertion goes through the HTTP
    // router against the shared store, never at the store directly.
    // Management-plane only; publish and deliver never touch the
    // source directory.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // Empty store returns an empty page, not an error.
    let (status, body) = server.get_auth("/api/v5/sources", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
    assert_eq!(body.as_array().map(Vec::len), Some(0));
    // Unknown query parameters are ignored.
    let (status, _) = server
        .get_auth("/api/v5/sources?unknown=param", &token)
        .await;
    assert_eq!(status, 200);
    // Unknown source kind is rejected with the documented shape.
    let (status, body) = server
        .post_auth(
            "/api/v5/sources",
            json!({"name": "bad-kind-source", "type": "no_such_kind_xyz"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "unknown source kind must be 400");
    assert_eq!(body["code"], json!("ALREADY_EXISTS"));
    // Create one source.
    let (status, body) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "w3-26-source", "type": "mqtt", "connector": "mqtt:up", "parameters": {"topic": "in/w3-26-source"}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    assert_eq!(body["id"], json!("w3-26-source"));
    assert_eq!(body["type"], json!("mqtt"));
    // List shows it with paging metadata.
    let (status, body) = server.get_auth("/api/v5/sources", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    let ids: Vec<String> = body
        .as_array()
        .expect("list data is an array")
        .iter()
        .filter_map(|row| row.get("id").and_then(|v| v.as_str()).map(str::to_string))
        .collect();
    assert!(ids.contains(&"w3-26-source".to_string()));
    // Duplicate create is rejected cleanly, never overwritten.
    let (status, body) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "w3-26-source", "type": "mqtt", "connector": "mqtt:up", "parameters": {"topic": "in/w3-26-source"}}),
                &token,
            )
            .await;
    assert_eq!(status, 400, "duplicate source id must be rejected");
    assert_eq!(body["code"], json!("ALREADY_EXISTS"));
    // Delete hit, then miss.
    let (status, _) = server
        .delete_auth("/api/v5/sources/w3-26-source", &token)
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .delete_auth("/api/v5/sources/w3-26-source", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    // Store is empty again.
    let (status, body) = server.get_auth("/api/v5/sources", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(0));
}

#[tokio::test]
async fn sources_single_read_and_replace_round_trip() {
    // W3-27 (C07-15/C07-16) broker-through test: create a source via
    // the W3-26 route, read it back and expect the documented
    // fields; replace its config and read again expecting the new
    // values; read an unknown id expecting 404. Every assertion goes
    // through the HTTP router against the shared store (the same
    // state the kernel shares in `crates/broker-node/src/main.rs`,
    // `serve_api`), never at the store directly. Management-plane
    // only; publish and deliver never touch the source directory.
    // Before: both single routes returned 404.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // Unknown read is 404 with the documented shape, not an empty body.
    let (status, body) = server
        .get_auth("/api/v5/sources/no-such-source", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    // Create one source via the W3-26 route.
    let (status, _) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "w3-27-source", "type": "mqtt", "connector": "mqtt:up", "parameters": {"topic": "in/w3-27-source"}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    // Read hit carries the documented fields.
    let (status, body) = server
        .get_auth("/api/v5/sources/w3-27-source", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["id"], json!("w3-27-source"));
    assert_eq!(body["type"], json!("mqtt"));
    assert_eq!(body["enable"], json!(true));
    // Unknown query parameters are ignored on the single read.
    let (status, _) = server
        .get_auth("/api/v5/sources/w3-27-source?unknown=param", &token)
        .await;
    assert_eq!(status, 200);
    // Replace the stored config with the same validation as create.
    let (status, body) = server
            .put_auth(
                "/api/v5/sources/w3-27-source",
                json!({"type": "mqtt", "enable": false, "connector": "mqtt:up", "parameters": {"topic": "in/w3-27-source"}}),
                &token,
            )
            .await;
    assert_eq!(status, 200);
    assert_eq!(body["id"], json!("w3-27-source"));
    assert_eq!(body["type"], json!("mqtt"));
    assert_eq!(body["enable"], json!(false));
    // Read again sees the new values.
    let (status, body) = server
        .get_auth("/api/v5/sources/w3-27-source", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["type"], json!("mqtt"));
    assert_eq!(body["enable"], json!(false));
    // Update of an unknown id is 404 and creates nothing.
    let (status, body) = server
        .put_auth(
            "/api/v5/sources/no-such-source",
            json!({"type": "mqtt"}),
            &token,
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    let (status, _) = server
        .get_auth("/api/v5/sources/no-such-source", &token)
        .await;
    assert_eq!(status, 404);
    // Unknown kind and malformed bodies are rejected, leaving the
    // stored entry untouched.
    let (status, body) = server
        .put_auth(
            "/api/v5/sources/w3-27-source",
            json!({"type": "no_such_kind_xyz"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, body) = server
        .put_auth(
            "/api/v5/sources/w3-27-source",
            json!({"enable": true}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, body) = server
        .get_auth("/api/v5/sources/w3-27-source", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["type"], json!("mqtt"));
    // Mismatched body id is rejected instead of renaming.
    let (status, body) = server
        .put_auth(
            "/api/v5/sources/w3-27-source",
            json!({"id": "other-source", "type": "mqtt"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    // Cleanup so the id does not leak into sibling tests sharing
    // nothing but the name space of fresh servers.
    let (status, _) = server
        .delete_auth("/api/v5/sources/w3-27-source", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn sources_survive_restart() {
    // W3-26 persistence across a restart through the config core:
    // register through the broker, reload the same data dir in a
    // fresh server, and read the source back from the list.
    let dir = unique_data_dir("w3-26-sources");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let (status, _) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "restart-src", "type": "mqtt", "connector": "mqtt:up", "parameters": {"topic": "in/restart-src"}}),
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
    let (status, body) = server.get_auth("/api/v5/sources", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    let ids: Vec<String> = body
        .as_array()
        .expect("list data is an array")
        .iter()
        .filter_map(|row| row.get("id").and_then(|v| v.as_str()).map(str::to_string))
        .collect();
    assert!(ids.contains(&"restart-src".to_string()));
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn source_operation_start_stop_round_trip() {
    // W3-23 (C07-20) broker-through test: create a source via the
    // `POST /sources` route, stop it through the operation route and
    // read back disabled from the reported state plus the summary,
    // start it and read back enabled; an unknown operation is
    // rejected and an unknown id is 404, both with the documented
    // shape and a non-empty body. Before: the operation route
    // returned 404. Every assertion goes through the HTTP router
    // against the shared store (the same state the kernel shares in
    // `crates/broker-node/src/main.rs`, `serve_api`), never at the
    // store directly. Management-plane only; a live ingress dispatch
    // through the engine still matches nothing afterwards, so
    // publish and deliver never touch the source directory.
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // Unknown id is 404 with the documented shape, not an empty body.
    let (status, body) = server
        .send(
            "POST",
            "/api/v5/sources/no-such-source/start",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    // Unknown id with an unknown operation is still 404 (existence
    // before validation) and changes nothing.
    let (status, body) = server
        .send(
            "POST",
            "/api/v5/sources/no-such-source/frobnicate",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    // Create one source (defaults to enabled).
    let (status, _) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "w3-23-src", "type": "mqtt", "connector": "mqtt:up", "parameters": {"topic": "in/w3-23-src"}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    // Unknown operation is rejected with the documented shape and
    // changes nothing (the stored flag stays enabled).
    let (status, body) = server
        .send(
            "POST",
            "/api/v5/sources/w3-23-src/frobnicate",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 400, "unknown operation must be rejected");
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    assert_eq!(
        body.as_array()
            .map(|rows| rows.iter().filter(|r| r["enable"] == json!(true)).count()),
        Some(1)
    );
    // Stop: the reported runtime state changes to disabled.
    let (status, _) = server
        .send("POST", "/api/v5/sources/w3-23-src/stop", None, Some(&token))
        .await;
    assert_eq!(status, 204);
    let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    assert_eq!(
        body.as_array()
            .map(|rows| rows.iter().filter(|r| r["enable"] == json!(true)).count()),
        Some(0)
    );
    // Start: the reported runtime state changes back to enabled.
    let (status, _) = server
        .send(
            "POST",
            "/api/v5/sources/w3-23-src/start",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    assert_eq!(
        body.as_array()
            .map(|rows| rows.iter().filter(|r| r["enable"] == json!(true)).count()),
        Some(1)
    );
    // The message path is untouched: live ingress dispatch still
    // works and the source directory reports the same counts after.
    #[cfg(test)]
    struct W323Sink;
    #[async_trait::async_trait]
    #[cfg(test)]
    impl broker_rules::BrokerSink for W323Sink {
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
    let sink: std::sync::Arc<dyn broker_rules::BrokerSink> = std::sync::Arc::new(W323Sink);
    let topic = broker_protocol::Topic::new("w323/probe").expect("topic");
    let payload = bytes::Bytes::from_static(b"{}");
    let matched = state
        .engine
        .dispatch_ingress(&topic, &payload, broker_protocol::QoS::AtMostOnce, &sink)
        .await;
    assert_eq!(
        matched, 0,
        "no stored rule so live dispatch matches nothing"
    );
    let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    let (status, _) = server
        .delete_auth("/api/v5/sources/w3-23-src", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn source_enable_flag_round_trip_and_runtime_effect() {
    // W3-28 (C07-17) broker-through test: create a source via the
    // `POST /sources` route, disable it through the enable route and
    // read back disabled from the reported state plus the single read
    // and the summary (disabled sources stop ingress on the shared
    // W3-23 runtime core), re-enable and read back enabled with
    // ingress resumed; unknown ids are 404 and malformed flags are
    // 400, both with the documented shape and a non-empty body.
    // Before: the enable route returned 404. Every assertion goes
    // through the HTTP router against the shared store (the same state
    // the kernel shares in `crates/broker-node/src/main.rs`,
    // `serve_api`), never at the store directly. Management-plane
    // only; publish and deliver never touch the source directory.
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // Unknown id is 404 with the documented shape, not an empty body.
    let (status, body) = server
        .send(
            "PUT",
            "/api/v5/sources/no-such-source/enable/false",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    // Unknown id with a malformed flag is still 404 (existence before
    // validation) and changes nothing.
    let (status, body) = server
        .send(
            "PUT",
            "/api/v5/sources/no-such-source/enable/maybe",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    // Create one source (defaults to enabled).
    let (status, _) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "w3-28-src", "type": "mqtt", "connector": "mqtt:up", "parameters": {"topic": "in/w3-28-src"}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    // Malformed flag is rejected with the documented shape and changes
    // nothing (the stored flag stays enabled).
    for bad in ["maybe", "1", "0", "on", "TRUEE", "enabled"] {
        let (status, body) = server
            .send(
                "PUT",
                &format!("/api/v5/sources/w3-28-src/enable/{bad}"),
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 400, "flag {bad:?} must be rejected");
        assert_eq!(body["code"], json!("BAD_REQUEST"));
    }
    let (status, body) = server.get_auth("/api/v5/sources/w3-28-src", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["enable"], json!(true));
    // Disable: the reported flag changes and reads back disabled on
    // the next single read (ingress stopped on the W3-23 runtime).
    let (status, _) = server
        .send(
            "PUT",
            "/api/v5/sources/w3-28-src/enable/false",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server.get_auth("/api/v5/sources/w3-28-src", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["enable"], json!(false));
    let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    assert_eq!(
        body.as_array()
            .map(|rows| rows.iter().filter(|r| r["enable"] == json!(true)).count()),
        Some(0)
    );
    // The W3-23 operation route observes the same core: starting the
    // disabled source re-enables it (ingress resumed).
    let (status, _) = server
        .send(
            "POST",
            "/api/v5/sources/w3-28-src/start",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    // Re-disable through the enable route, then re-enable through it:
    // the flag change is visible on the next read each way.
    let (status, _) = server
        .send(
            "PUT",
            "/api/v5/sources/w3-28-src/enable/false",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .send(
            "PUT",
            "/api/v5/sources/w3-28-src/enable/true",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server.get_auth("/api/v5/sources/w3-28-src", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["enable"], json!(true));
    // The message path is untouched: live ingress dispatch still
    // works and the source directory reports the same counts after.
    #[cfg(test)]
    struct W328Sink;
    #[async_trait::async_trait]
    #[cfg(test)]
    impl broker_rules::BrokerSink for W328Sink {
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
    let sink: std::sync::Arc<dyn broker_rules::BrokerSink> = std::sync::Arc::new(W328Sink);
    let topic = broker_protocol::Topic::new("w328/probe").expect("topic");
    let payload = bytes::Bytes::from_static(b"{}");
    let matched = state
        .engine
        .dispatch_ingress(&topic, &payload, broker_protocol::QoS::AtMostOnce, &sink)
        .await;
    assert_eq!(
        matched, 0,
        "no stored rule so live dispatch matches nothing"
    );
    let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    assert_eq!(
        body.as_array()
            .map(|rows| rows.iter().filter(|r| r["enable"] == json!(true)).count()),
        Some(1)
    );
    let (status, _) = server
        .delete_auth("/api/v5/sources/w3-28-src", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn node_source_operation_single_node_round_trip() {
    // W3-24 (C07-10) broker-through test: create a source via the
    // `POST /sources` route, stop it through the node-scoped operation
    // route and read back disabled from the reported state plus the
    // summary, start it and read back enabled; an unknown node is
    // NOT_FOUND, an unknown id is 404 and an unknown operation is
    // rejected, all with the documented shape and a non-empty body.
    // Before: the node-scoped route returned 404. Every assertion goes
    // through the HTTP router against the shared store (the same state
    // the kernel shares in `crates/broker-node/src/main.rs`,
    // `serve_api`), never at the store directly. Management-plane
    // only; a live ingress dispatch through the engine still matches
    // nothing afterwards, so publish and deliver never touch the
    // source directory.
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let local = "indra-node-1";
    let legacy = crate::node_scope::local_node_name();
    // Unknown node is NOT_FOUND with a non-empty body naming the node,
    // even when the id and operation are also unknown (node scope is
    // checked first, fail closed).
    for unknown in ["no-such-node-xyz", ""] {
        let path = format!("/api/v5/nodes/{unknown}/sources/no-such-source/start");
        let (status, body) = server.send("POST", &path, None, Some(&token)).await;
        assert_eq!(status, 404, "unknown node {unknown:?} must be 404");
        assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
        assert!(
            body["message"].is_string() && !body["message"].as_str().expect("message").is_empty(),
            "unknown-node body must not be empty: {body}"
        );
    }
    // Unknown id on the known node is 404 with the documented shape,
    // not an empty body.
    let (status, body) = server
        .send(
            "POST",
            &format!("/api/v5/nodes/{local}/sources/no-such-source/start"),
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    assert!(!body.to_string().is_empty());
    // Create one source via the W3-26 route (defaults to enabled).
    let (status, _) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "w3-24-src", "type": "mqtt", "connector": "mqtt:up", "parameters": {"topic": "in/w3-24-src"}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    // Unknown operation on the known node is rejected with the
    // documented shape and persists nothing (the flag stays enabled).
    let (status, body) = server
        .send(
            "POST",
            &format!("/api/v5/nodes/{local}/sources/w3-24-src/frobnicate"),
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 400, "unknown operation must be rejected");
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    assert!(!body.to_string().is_empty());
    let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    assert_eq!(
        body.as_array()
            .map(|rows| rows.iter().filter(|r| r["enable"] == json!(true)).count()),
        Some(1)
    ); // Stop through the node route: the reported state flips to
       // disabled. The legacy node name names the same local node.
    let (status, _) = server
        .send(
            "POST",
            &format!("/api/v5/nodes/{local}/sources/w3-24-src/stop"),
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
    assert_eq!(status, 200);
    assert_eq!(
        body.as_array()
            .map(|rows| rows.iter().filter(|r| r["enable"] == json!(true)).count()),
        Some(0)
    );
    let (status, _) = server
        .send(
            "POST",
            &format!("/api/v5/nodes/{legacy}/sources/w3-24-src/start"),
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204, "legacy node name must resolve");
    // The message path is untouched: live ingress dispatch still works
    // and the source directory reports the same counts after.
    #[cfg(test)]
    struct W324Sink;
    #[async_trait::async_trait]
    #[cfg(test)]
    impl broker_rules::BrokerSink for W324Sink {
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
    let sink: std::sync::Arc<dyn broker_rules::BrokerSink> = std::sync::Arc::new(W324Sink);
    let topic = broker_protocol::Topic::new("w324/probe").expect("topic");
    let payload = bytes::Bytes::from_static(b"{}");
    let matched = state
        .engine
        .dispatch_ingress(&topic, &payload, broker_protocol::QoS::AtMostOnce, &sink)
        .await;
    assert_eq!(
        matched, 0,
        "no stored rule so live dispatch matches nothing"
    );
    let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    let (status, _) = server
        .delete_auth("/api/v5/sources/w3-24-src", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn source_types_lists_documented_names() {
    // AP-05/KC-00: GET /source_types returns a bare string array of
    // the kinds that are ready in this build. It never lists a
    // pending or refused kind.
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, body) = server.get_auth("/api/v5/source_types", &token).await;
    assert_eq!(status, 200);
    let entries = body.as_array().expect("type list is an array");
    let kinds: std::collections::HashSet<&str> =
        entries.iter().filter_map(|e| e.as_str()).collect();
    assert_eq!(kinds.len(), entries.len(), "names must be unique strings");
    // MV-03a sets the message-broker source kinds ready. The list
    // holds only documented names.
    let documented: std::collections::HashSet<&str> = [
        "mqtt",
        "kafka_consumer",
        "gcp_pubsub_consumer",
        "rabbitmq",
        "azure_event_grid",
    ]
    .into_iter()
    .collect();
    for name in &kinds {
        assert!(
            documented.contains(name),
            "listed kind {name} is documented"
        );
    }
    assert!(
        kinds.contains(&"mqtt"),
        "type list holds the documented ingress kind mqtt"
    );
    for unsupported in ["kafka_consumer", "gcp_pubsub_consumer", "rabbitmq"] {
        assert!(
            !kinds.contains(unsupported),
            "type list has no ingress-less kind {unsupported}"
        );
    }
    for pending in ["timescaledb", "oci_streaming"] {
        assert!(
            !kinds.contains(pending),
            "type list has no pending '{pending}'"
        );
    }
    for name in &kinds {
        assert!(
            broker_config::is_kind_ready(name),
            "listed kind {name} is ready"
        );
    }

    let (status, again) = server
        .get_auth("/api/v5/source_types?unknown=param", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(again, body);

    // A ready local source kind creates and deletes through the
    // broker API.
    let (status, _) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "w325-probe-src", "type": "mqtt", "connector": "mqtt:up", "parameters": {"topic": "in/w325-probe-src"}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .delete_auth("/api/v5/sources/w325-probe-src", &token)
        .await;
    assert_eq!(status, 204);

    // The catalogue is never consulted per message: a published
    // message still reaches the rule trigger path after the reads.
    state
        .engine
        .create_rule(
            "w325-rule".to_string(),
            broker_protocol::TopicFilter::new("w325/#").expect("filter"),
            None,
            true,
            vec![broker_rules::RuleAction::Log],
        )
        .expect("rule creates");
    #[cfg(test)]
    struct W325Sink;
    #[async_trait::async_trait]
    #[cfg(test)]
    impl broker_rules::BrokerSink for W325Sink {
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
    let sink: std::sync::Arc<dyn broker_rules::BrokerSink> = std::sync::Arc::new(W325Sink);
    let topic = broker_protocol::Topic::new("w325/probe").expect("topic");
    let payload = bytes::Bytes::from_static(b"{}");
    let matched = state
        .engine
        .dispatch_ingress(&topic, &payload, broker_protocol::QoS::AtMostOnce, &sink)
        .await;
    assert_eq!(matched, 1);
}

#[tokio::test]
async fn ap05_sources_typed_round_trip() {
    // AP-05/KC-00: the ready MQTT ingress kind round trips. Pending
    // timescaledb and oci_streaming are refused and store nothing.
    // MV-08 keeps only the MQTT subscriber as a source with a runtime.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let cases = [
        (
            "ap05-src-mqtt-a",
            json!({"type": "mqtt", "name": "ap05-src-mqtt-a",
                       "connector": "mqtt:ap05-up",
                       "parameters": {"topic": "in/ap05"}}),
        ),
        (
            "ap05-src-mqtt-b",
            json!({"type": "mqtt", "name": "ap05-src-mqtt-b",
                       "connector": "mqtt:ap05-up",
                       "parameters": {"topic": "in/ap05"}}),
        ),
    ];
    for (bare, body) in cases {
        let (status, created) = server.post_auth("/api/v5/sources", body, &token).await;
        assert_eq!(status, 201, "create for {bare}");
        assert_eq!(created["name"], json!(bare));
        assert!(created["parameters"].is_object());
        assert_eq!(created["status"], json!("disconnected"));
        assert_eq!(created["status_reason"], json!("source is starting"));
        let (status, _) = server
            .get_auth(&format!("/api/v5/sources/{bare}"), &token)
            .await;
        assert_eq!(status, 200);
        let (status, _) = server
            .delete_auth(&format!("/api/v5/sources/{bare}"), &token)
            .await;
        assert_eq!(status, 204);
    }
    for (name, kind) in [
        ("ap05-src-ts", "timescaledb"),
        ("ap05-src-oci", "oci_streaming"),
    ] {
        let (status, refused) = server
            .post_auth(
                "/api/v5/sources",
                json!({"type": kind, "name": name}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "{kind} source must be refused");
        assert!(
            refused.to_string().contains("not available in this broker"),
            "the refusal names the kind: {refused}"
        );
        let (status, _) = server
            .get_auth(&format!("/api/v5/sources/{name}"), &token)
            .await;
        assert_eq!(status, 404, "{kind} source stores nothing");
    }
}
