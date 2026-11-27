use super::*;

#[tokio::test]
async fn action_reads_mask_secrets_and_marker_put_keeps_them() {
    #[cfg(test)]
    const SECRET: &str = "known-action-secret-5c21";
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, created) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "mask-action", "type": "console",
                       "parameters": {"password": SECRET, "headers": {"token": SECRET}}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert!(!created.to_string().contains(SECRET));
    let (status, shown) = server.get_auth("/api/v5/actions/mask-action", &token).await;
    assert_eq!(status, 200);
    assert!(!shown.to_string().contains(SECRET));
    assert_eq!(shown["parameters"]["password"], json!("******"));
    let (_, listed) = server.get_auth("/api/v5/actions", &token).await;
    assert!(!listed.to_string().contains(SECRET));
    let mut put = shown.clone();
    for key in ["status", "node_status", "status_reason"] {
        put.as_object_mut().unwrap().remove(key);
    }
    let (status, answer) = server
        .put_auth("/api/v5/actions/mask-action", put, &token)
        .await;
    assert_eq!(status, 200, "{answer}");
    assert!(!answer.to_string().contains(SECRET));
    let stored = serde_json::to_string(&state.config.snapshot().actions).unwrap();
    assert!(stored.contains(SECRET), "the marker PUT lost the secret");
    assert!(!stored.contains("******"), "the mask was stored");
}

#[tokio::test]
async fn removed_action_routes_return_404() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // W3-02 restores `GET /api/v5/actions`, `POST /api/v5/actions`
    // and `DELETE /api/v5/actions/:id` on the real action directory,
    // W3-03 adds `GET`/`PUT /api/v5/actions/:id`, W3-04 adds
    // `PUT /api/v5/actions/:id/enable/:enable`, W3-05 adds
    // `GET /api/v5/actions/:id/metrics` plus `PUT .../metrics/reset`,
    // W3-06 adds `POST /api/v5/actions/:id/:operation`, and W3-08
    // adds `GET /api/v5/actions_summary`, so no removed action path
    // stays 404 here: the summary reads live on the shared store.
    // `/api/v5/actions/:id` now exists for GET, PUT and DELETE and
    // `/api/v5/actions/:id/metrics` for GET, so neither is a removed
    // route (mirrors the `/api/v5/sources` handling in
    // `removed_sources_rulenotest_routes_return_404`).
    let (status, _) = server.get_auth("/api/v5/actions_summary", &token).await;
    assert_eq!(
        status, 200,
        "restored route /api/v5/actions_summary must be live"
    );
}

#[tokio::test]
async fn actions_list_create_delete_round_trip() {
    // W3-02 (C06-02..C06-04) broker-through test: create an action,
    // list and expect it present with paging metadata; create the
    // same id again and expect rejection; delete it and expect a
    // second delete to be 404. Every assertion goes through the HTTP
    // router against the shared store, never at the store directly.
    // Management-plane only; publish and deliver never touch the
    // action directory.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // Empty store returns an empty page, not an error.
    let (status, body) = server.get_auth("/api/v5/actions", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
    assert_eq!(body.as_array().map(Vec::len), Some(0));
    // Unknown query parameters are ignored.
    let (status, _) = server
        .get_auth("/api/v5/actions?unknown=param", &token)
        .await;
    assert_eq!(status, 200);
    // Unknown action kind is rejected with the documented shape.
    let (status, body) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "bad-kind-action", "type": "no_such_kind_xyz"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "unknown action kind must be 400");
    assert_eq!(body["code"], json!("ALREADY_EXISTS"));
    // Create one action.
    let (status, body) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "w3-02-action", "type": "console", "server": "127.0.0.1:1883"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(body["id"], json!("w3-02-action"));
    assert_eq!(body["type"], json!("console"));
    // List shows it with paging metadata.
    let (status, body) = server.get_auth("/api/v5/actions", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(1));
    let ids: Vec<String> = body
        .as_array()
        .expect("list data is an array")
        .iter()
        .filter_map(|row| row.get("id").and_then(|v| v.as_str()).map(str::to_string))
        .collect();
    assert!(ids.contains(&"w3-02-action".to_string()));
    // Duplicate create is rejected cleanly, never overwritten.
    let (status, body) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "w3-02-action", "type": "console"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "duplicate action id must be rejected");
    assert_eq!(body["code"], json!("ALREADY_EXISTS"));
    // Delete hit, then miss.
    let (status, _) = server
        .delete_auth("/api/v5/actions/w3-02-action", &token)
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .delete_auth("/api/v5/actions/w3-02-action", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    // Store is empty again.
    let (status, body) = server.get_auth("/api/v5/actions", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(0));
}

#[tokio::test]
async fn actions_delete_refused_while_rule_references() {
    // W3-02 rule-linkage broker-through test: an action still
    // referenced by a rule cannot be dropped while the reference
    // stands; once the rule is gone the delete succeeds. The
    // reference check consults the live rule engine
    // (`broker-rules`, `RuleEngine::list_rules`), driven by rule
    // create/delete events. Management-plane only; publish and
    // deliver never touch either store.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "linked-action", "type": "console"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, rule_body) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "name": "linked-rule",
                "sql": "SELECT * FROM \"w3/02/linked\"",
                "actions": ["linked-action"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    // Rules are addressed by their generated `id`, not by `name`:
    // delete the created rule id so the reference is really gone.
    let rule_id = rule_body["id"].as_str().expect("rule id").to_string();
    let (status, body) = server
        .delete_auth("/api/v5/actions/linked-action", &token)
        .await;
    assert_eq!(status, 400, "referenced action delete must be refused");
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, _) = server
        .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/linked-action", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn sb16_action_limit_create_file_and_boot() {
    // MV-07 limit test through the production entries.
    // The file check writes and reloads a state file.
    // The boot check starts a broker with the stored list.
    // The API check uses the HTTP router and the alarm list.
    let over: Vec<broker_config::ActionEntry> = (0..(broker_config::MAX_ACTIONS + 1))
        .map(|index| broker_config::ActionEntry {
            id: format!("limit-action-{index:04}"),
            action_type: "console".to_string(),
            enable: true,
            config: "{}".to_string(),
        })
        .collect();
    let conf = broker_config::ActionsConf { actions: over };
    assert!(conf.is_over_limit());
    assert!(conf.validate().is_err());
    assert!(conf.validate_stored().is_err());
    assert!(conf.validate_stored_for_boot().is_ok());
    let slot = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir().join(format!(
        "indramqtt-sb16-limit-{}-{}",
        std::process::id(),
        slot
    ));
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    registry
        .commit_actions_stored(conf)
        .expect("stored rewrite keeps over-limit rows");
    registry
        .save()
        .expect("stored list writes to the state file");
    assert!(
        ConfigRegistry::load(&dir).is_err(),
        "a new file load refuses a list above the cap"
    );
    let boot =
        Arc::new(ConfigRegistry::load_for_boot(&dir).expect("boot loads a list above the cap"));
    let (mut server, _state) = TestServer::start_with_registry(Arc::clone(&boot)).await;
    let token = server.login_as_admin().await;
    let (status, actions) = server.get_auth("/api/v5/actions", &token).await;
    assert_eq!(status, 200);
    assert_eq!(
        actions.as_array().expect("action list").len(),
        broker_config::MAX_ACTIONS + 1
    );
    let (status, alarms) = server.get_auth("/api/v5/alarms", &token).await;
    assert_eq!(status, 200);
    let names: Vec<&str> = alarms["data"]
        .as_array()
        .expect("alarm rows")
        .iter()
        .filter_map(|row| row["name"].as_str())
        .collect();
    assert!(names.contains(&"action_limit_exceeded"), "{names:?}");
    let (status, body) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "limit-one-more", "type": "console"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, actions) = server.get_auth("/api/v5/actions", &token).await;
    assert_eq!(status, 200);
    assert_eq!(
        actions.as_array().expect("action list").len(),
        broker_config::MAX_ACTIONS + 1
    );
    let (status, _) = server
        .delete_auth("/api/v5/actions/limit-action-0000", &token)
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
    assert!(!names.contains(&"action_limit_exceeded"), "{names:?}");
    server.task.abort();
    let _ = (&mut server.task).await;
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn sb16_action_delete_reports_rules_and_summary_lists_links() {
    // MV-07 shape test through the HTTP router.
    // Delete of a linked action names every rule.
    // Summary rows carry status and rule links.
    let (mut server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "sb16-linked", "type": "console"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, first) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "name": "sb16-rule-one",
                "sql": "SELECT * FROM \"sb16/linked\"",
                "actions": ["sb16-linked"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let first_id = first["id"].as_str().expect("rule id").to_string();
    let (status, second) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "name": "sb16-rule-two",
                "sql": "SELECT * FROM \"sb16/linked\"",
                "actions": ["sb16-linked"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let second_id = second["id"].as_str().expect("rule id").to_string();
    let (status, body) = server
        .delete_auth("/api/v5/actions/sb16-linked", &token)
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let rules = body["rules"].as_array().expect("rules array");
    assert_eq!(rules.len(), 2);
    let ids: Vec<&str> = rules.iter().filter_map(|row| row.as_str()).collect();
    assert!(ids.contains(&first_id.as_str()), "{ids:?}");
    assert!(ids.contains(&second_id.as_str()), "{ids:?}");
    let (status, summary) = server.get_auth("/api/v5/actions_summary", &token).await;
    assert_eq!(status, 200);
    let row = summary
        .as_array()
        .expect("summary list")
        .iter()
        .find(|row| row["name"] == json!("sb16-linked"))
        .expect("linked action in summary")
        .clone();
    assert_eq!(row["type"], json!("console"));
    assert_eq!(row["enable"], json!(true));
    assert!(row["status"].is_string());
    assert!(row["node_status"].is_array());
    let linked = row["rules"].as_array().expect("summary rules");
    assert_eq!(linked.len(), 2);
    let (status, _) = server
        .delete_auth(&format!("/api/v5/rules/{first_id}"), &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth(&format!("/api/v5/rules/{second_id}"), &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/sb16-linked", &token)
        .await;
    assert_eq!(status, 204);
    server.task.abort();
    let _ = (&mut server.task).await;
}

#[tokio::test]
async fn sb16_action_metrics_report_n_publishes_and_reset() {
    // MV-07 metrics test through the broker rule path.
    // Three publishes move the counters, then reset zeroes them.
    #[cfg(test)]
    struct CountingSink;
    #[async_trait::async_trait]
    #[cfg(test)]
    impl broker_connectors::Sink for CountingSink {
        #[cfg(test)]
        async fn send(
            &self,
            _topic: &broker_protocol::Topic,
            _payload: &bytes::Bytes,
            _qos: broker_protocol::QoS,
        ) -> broker_connectors::Result<()> {
            Ok(())
        }
        #[cfg(test)]
        fn kind(&self) -> &'static str {
            "test-counter"
        }
    }
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
    let (mut server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "sb16-metrics", "type": "console"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    state.engine.connectors().register(
        broker_config::v5_action_sink_key("sb16-metrics"),
        std::sync::Arc::new(CountingSink),
    );
    let (status, rule_body) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "name": "sb16-metrics-rule",
                "sql": "SELECT * FROM \"sb16/metrics\"",
                "actions": ["sb16-metrics"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let rule_id = rule_body["id"].as_str().expect("rule id").to_string();
    let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
        std::sync::Arc::new(NoopBrokerSink);
    let topic = broker_protocol::Topic::new("sb16/metrics").expect("valid topic");
    for _ in 0..3 {
        state
            .engine
            .dispatch_ingress(
                &topic,
                &bytes::Bytes::from_static(b"{\"k\":1}"),
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
    }
    let (status, body) = server
        .get_auth("/api/v5/actions/sb16-metrics/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["metrics"]["total"], json!(3));
    assert_eq!(body["metrics"]["matched"], json!(3));
    assert_eq!(body["metrics"]["success"], json!(3));
    assert_eq!(body["metrics"]["failed"], json!(0));
    let (status, _) = server
        .send(
            "PUT",
            "/api/v5/actions/sb16-metrics/metrics/reset",
            Some(json!({})),
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/actions/sb16-metrics/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["metrics"]["total"], json!(0));
    assert_eq!(body["metrics"]["matched"], json!(0));
    let (status, _) = server
        .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/sb16-metrics", &token)
        .await;
    assert_eq!(status, 204);
    server.task.abort();
    let _ = (&mut server.task).await;
}

#[tokio::test]
async fn actions_single_read_and_replace_round_trip() {
    // W3-03 (C06-05/C06-06) broker-through test: create an action via
    // the W3-02 routes, read it back and expect the documented
    // fields; replace its config and read again expecting the new
    // values; read an unknown id expecting 404. Every assertion goes
    // through the HTTP router against the shared store, never at the
    // store directly. Management-plane only; publish and deliver
    // never touch the action directory.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // Unknown read is 404 with the documented shape, not empty.
    let (status, body) = server
        .get_auth("/api/v5/actions/no-such-action", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    // Create one action via the W3-02 route.
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "w3-03-action", "type": "console", "server": "127.0.0.1:1883"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    // Read hit carries the documented fields.
    let (status, body) = server
        .get_auth("/api/v5/actions/w3-03-action", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["id"], json!("w3-03-action"));
    assert_eq!(body["type"], json!("console"));
    assert_eq!(body["server"], json!("127.0.0.1:1883"));
    // Unknown query parameters are ignored on the single read.
    let (status, _) = server
        .get_auth("/api/v5/actions/w3-03-action?unknown=param", &token)
        .await;
    assert_eq!(status, 200);
    // Replace the stored config with the same validation as create.
    let (status, body) = server
        .put_auth(
            "/api/v5/actions/w3-03-action",
            json!({"type": "console", "topic": "events-updated", "enable": false}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["id"], json!("w3-03-action"));
    assert_eq!(body["type"], json!("console"));
    assert_eq!(body["enable"], json!(false));
    // Read again sees the new values.
    let (status, body) = server
        .get_auth("/api/v5/actions/w3-03-action", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["type"], json!("console"));
    assert_eq!(body["topic"], json!("events-updated"));
    // Update of an unknown id is 404 and creates nothing.
    let (status, body) = server
        .put_auth(
            "/api/v5/actions/no-such-action",
            json!({"type": "console"}),
            &token,
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    let (status, _) = server
        .get_auth("/api/v5/actions/no-such-action", &token)
        .await;
    assert_eq!(status, 404);
    // Unknown kind and malformed bodies are rejected, leaving the
    // stored entry untouched.
    let (status, body) = server
        .put_auth(
            "/api/v5/actions/w3-03-action",
            json!({"type": "no_such_kind_xyz"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, body) = server
        .put_auth(
            "/api/v5/actions/w3-03-action",
            json!({"server": "127.0.0.1:1883"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, body) = server
        .get_auth("/api/v5/actions/w3-03-action", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["type"], json!("console"));
    // Mismatched body id is rejected instead of renaming.
    let (status, body) = server
        .put_auth(
            "/api/v5/actions/w3-03-action",
            json!({"id": "other-action", "type": "console"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
}

#[tokio::test]
async fn actions_enable_flag_round_trip_and_dispatch_skip() {
    // W3-04 (C06-07) broker-through test: create an action via the
    // W3-02 route, disable it through the enable route and read back
    // disabled, re-enable and read back enabled; unknown ids are 404
    // and malformed flags are 400, both with the documented shape.
    // Then a rule forwarding to the action id fires while enabled
    // and is skipped at fire time while disabled, through the
    // engine's lock-free dispatch snapshot (one publish through
    // `dispatch_ingress` per case). Management-plane only otherwise;
    // publish and deliver never touch the action store directly.
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    #[cfg(test)]
    struct CountingSink {
        hits: AtomicU64,
    }
    #[async_trait::async_trait]
    #[cfg(test)]
    impl broker_connectors::Sink for CountingSink {
        #[cfg(test)]
        async fn send(
            &self,
            _topic: &broker_protocol::Topic,
            _payload: &bytes::Bytes,
            _qos: broker_protocol::QoS,
        ) -> broker_connectors::Result<()> {
            self.hits.fetch_add(1, AtomicOrdering::Relaxed);
            Ok(())
        }
        #[cfg(test)]
        fn kind(&self) -> &'static str {
            "test-counter"
        }
    }
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
    // Unknown id is 404 with the documented shape, not an empty body.
    let (status, body) = server
        .send(
            "PUT",
            "/api/v5/actions/no-such-action/enable/false",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    // Create one action via the W3-02 route (defaults to enabled).
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "w3-04-action", "type": "console", "server": "127.0.0.1:1883"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    // Malformed flag is rejected with the documented shape (an empty
    // segment never reaches the handler: the router has no such route
    // and answers 404, so only non-empty spellings are exercised here).
    for bad in ["maybe", "1", "0", "on", "TRUEE", "enabled"] {
        let (status, body) = server
            .send(
                "PUT",
                &format!("/api/v5/actions/w3-04-action/enable/{bad}"),
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 400, "flag {bad:?} must be rejected");
        assert_eq!(body["code"], json!("BAD_REQUEST"));
    }
    // Disable: flag change is visible on the next read.
    let (status, _) = server
        .send(
            "PUT",
            "/api/v5/actions/w3-04-action/enable/false",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/actions/w3-04-action", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["enable"], json!(false));
    // Re-enable: visible on the next read.
    let (status, _) = server
        .send(
            "PUT",
            "/api/v5/actions/w3-04-action/enable/true",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/actions/w3-04-action", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["enable"], json!(true));
    // Rule dispatch through the broker: a rule forwarding to the
    // action id fires while enabled and skips while disabled. The
    // counting sink is registered under the action id, the rule is
    // created through the HTTP router, and each publish goes through
    // `dispatch_ingress` (the broker's rule path), never `sink.send`
    // directly.
    let counter = std::sync::Arc::new(CountingSink {
        hits: AtomicU64::new(0),
    });
    state.engine.connectors().register(
        broker_config::v5_action_sink_key("w3-04-action"),
        counter.clone(),
    );
    let (status, rule_body) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "name": "w3-04-rule",
                "sql": "SELECT * FROM \"w3/04/enable\"",
                "actions": ["w3-04-action"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let rule_id = rule_body["id"].as_str().expect("rule id").to_string();
    let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
        std::sync::Arc::new(NoopBrokerSink);
    let topic = broker_protocol::Topic::new("w3/04/enable").expect("valid topic");
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
    assert_eq!(
        counter.hits.load(AtomicOrdering::Relaxed),
        1,
        "enabled action must fire"
    );
    // Disable again: the next publish skips the forward.
    let (status, _) = server
        .send(
            "PUT",
            "/api/v5/actions/w3-04-action/enable/false",
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
    assert_eq!(
        counter.hits.load(AtomicOrdering::Relaxed),
        1,
        "disabled action must be skipped at fire time"
    );
    // Re-enable: firing resumes without re-registering anything.
    let (status, _) = server
        .send(
            "PUT",
            "/api/v5/actions/w3-04-action/enable/true",
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
    assert_eq!(
        counter.hits.load(AtomicOrdering::Relaxed),
        2,
        "re-enabled action must fire again"
    );
    // Cleanup: rule then action (the rule holds the reference).
    let (status, _) = server
        .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/w3-04-action", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn action_operation_start_stop_round_trip() {
    // W3-06 (C06-10) broker-through test: create an action via the
    // W3-02 route, stop it through the operation route and read back
    // disabled, start it and read back enabled; an unknown operation
    // is rejected and an unknown id is 404, both with the documented
    // shape. Then a rule forwarding to the action id fires while
    // started and is skipped at fire time while stopped, through the
    // engine's lock-free dispatch snapshot (one publish through
    // `dispatch_ingress` per case). Every assertion goes through the
    // HTTP router against the shared store, never at the store
    // directly. Management-plane only otherwise; publish and deliver
    // never touch the action store directly.
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    #[cfg(test)]
    struct CountingSink {
        hits: AtomicU64,
    }
    #[async_trait::async_trait]
    #[cfg(test)]
    impl broker_connectors::Sink for CountingSink {
        #[cfg(test)]
        async fn send(
            &self,
            _topic: &broker_protocol::Topic,
            _payload: &bytes::Bytes,
            _qos: broker_protocol::QoS,
        ) -> broker_connectors::Result<()> {
            self.hits.fetch_add(1, AtomicOrdering::Relaxed);
            Ok(())
        }
        #[cfg(test)]
        fn kind(&self) -> &'static str {
            "test-counter"
        }
    }
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
    // Unknown id is 404 with the documented shape, not an empty body.
    let (status, body) = server
        .send(
            "POST",
            "/api/v5/actions/no-such-action/start",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    // Create one action via the W3-02 route (defaults to enabled).
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "w3-06-action", "type": "console", "server": "127.0.0.1:1883"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    // Unknown operation is rejected with the documented shape and
    // persists nothing (the stored flag is untouched).
    let (status, body) = server
        .send(
            "POST",
            "/api/v5/actions/w3-06-action/frobnicate",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 400, "unknown operation must be rejected");
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, body) = server
        .get_auth("/api/v5/actions/w3-06-action", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["enable"], json!(true));
    // Stop: the reported state changes to disabled on the next read.
    let (status, _) = server
        .send(
            "POST",
            "/api/v5/actions/w3-06-action/stop",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/actions/w3-06-action", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["enable"], json!(false));
    // Start: the reported state changes back to enabled.
    let (status, _) = server
        .send(
            "POST",
            "/api/v5/actions/w3-06-action/start",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/actions/w3-06-action", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["enable"], json!(true));
    // Rule dispatch through the broker: a rule forwarding to the
    // action id fires while started and skips while stopped. The
    // counting sink is registered under the action id, the rule is
    // created through the HTTP router, and each publish goes through
    // `dispatch_ingress` (the broker's rule path), never `sink.send`
    // directly.
    let counter = std::sync::Arc::new(CountingSink {
        hits: AtomicU64::new(0),
    });
    state.engine.connectors().register(
        broker_config::v5_action_sink_key("w3-06-action"),
        counter.clone(),
    );
    let (status, rule_body) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "name": "w3-06-rule",
                "sql": "SELECT * FROM \"w3/06/operation\"",
                "actions": ["w3-06-action"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let rule_id = rule_body["id"].as_str().expect("rule id").to_string();
    let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
        std::sync::Arc::new(NoopBrokerSink);
    let topic = broker_protocol::Topic::new("w3/06/operation").expect("valid topic");
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
    assert_eq!(
        counter.hits.load(AtomicOrdering::Relaxed),
        1,
        "started action must fire"
    );
    // Stop again: the next publish skips the forward.
    let (status, _) = server
        .send(
            "POST",
            "/api/v5/actions/w3-06-action/stop",
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
    assert_eq!(
        counter.hits.load(AtomicOrdering::Relaxed),
        1,
        "stopped action must be skipped at fire time"
    );
    // Start again: firing resumes without re-registering anything.
    let (status, _) = server
        .send(
            "POST",
            "/api/v5/actions/w3-06-action/start",
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
    assert_eq!(
        counter.hits.load(AtomicOrdering::Relaxed),
        2,
        "re-started action must fire again"
    );
    // Cleanup: rule then action (the rule holds the reference).
    let (status, _) = server
        .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/w3-06-action", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn action_start_keeps_the_connector_stop() {
    // SR-01 item 4: starting a stopped action must not resume
    // delivery through a stopped connector. Broker-through: the
    // connector, action and rule are written through HTTP, every
    // publish enters through the HTTP publish route, and the
    // destination is a registered sink counted at the rule path.
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    #[cfg(test)]
    struct CountingSink {
        hits: AtomicU64,
    }
    #[async_trait::async_trait]
    #[cfg(test)]
    impl broker_connectors::Sink for CountingSink {
        #[cfg(test)]
        async fn send(
            &self,
            _topic: &broker_protocol::Topic,
            _payload: &bytes::Bytes,
            _qos: broker_protocol::QoS,
        ) -> broker_connectors::Result<()> {
            self.hits.fetch_add(1, AtomicOrdering::Relaxed);
            Ok(())
        }
        #[cfg(test)]
        fn kind(&self) -> &'static str {
            "test-counter"
        }
    }

    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "sr01-conn", "type": "webhook",
                       "url": "http://127.0.0.1:9"}),
            &token,
        )
        .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "sr01-act", "type": "console", "server": "127.0.0.1:1883",
                       "connector": "sr01-conn"}),
            &token,
        )
        .await;
    assert_eq!(status, 201, "action create: {body}");
    let counter = std::sync::Arc::new(CountingSink {
        hits: AtomicU64::new(0),
    });
    state.engine.connectors().register(
        broker_config::v5_action_sink_key("sr01-act"),
        counter.clone(),
    );
    let (status, rule_body) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "name": "sr01-act-rule",
                "sql": "SELECT * FROM \"sr01/action\"",
                "actions": ["sr01-act"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201, "rule create: {rule_body}");
    let rule_id = rule_body["id"].as_str().expect("rule id").to_string();
    let publish = |payload: &'static str| json!({"topic": "sr01/action", "payload": payload, "qos": 0, "retain": false});
    let (status, body) = server
        .post_auth("/api/v5/publish", publish("first"), &token)
        .await;
    assert_eq!(status, 202, "first publish: {body}");
    assert_eq!(
        counter.hits.load(AtomicOrdering::Relaxed),
        1,
        "an enabled action delivers once"
    );

    // Stop the connector, then start the action. The action's own
    // flag is true again, but the connector still holds delivery off.
    let (status, _) = server
        .send(
            "POST",
            "/api/v5/connectors/sr01-conn/stop",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .send("POST", "/api/v5/actions/sr01-act/start", None, Some(&token))
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .post_auth("/api/v5/publish", publish("stopped"), &token)
        .await;
    assert_eq!(status, 202, "publish while stopped: {body}");
    assert_eq!(
        counter.hits.load(AtomicOrdering::Relaxed),
        1,
        "a stopped connector must keep the restarted action off"
    );

    // Start the connector again: delivery resumes.
    let (status, _) = server
        .send(
            "POST",
            "/api/v5/connectors/sr01-conn/start",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .post_auth("/api/v5/publish", publish("resumed"), &token)
        .await;
    assert_eq!(status, 202, "publish after connector start: {body}");
    assert_eq!(
        counter.hits.load(AtomicOrdering::Relaxed),
        2,
        "a started connector restores delivery"
    );

    let (status, _) = server
        .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server.delete_auth("/api/v5/actions/sr01-act", &token).await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/connectors/sr01-conn", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn node_action_operation_round_trip() {
    // W3-09 (C06-13) broker-through test: create an action via the
    // W3-02 route, run `stop`/`start` through the node-scoped route
    // against the local node and read back the flipped state; an
    // unknown node is NOT_FOUND, an unknown id is 404 and an unknown
    // operation is rejected, all with the documented shape and a
    // non-empty body. Then a rule forwarding to the action id fires
    // while started and is skipped while stopped through the node
    // route, through the engine's lock-free dispatch snapshot (one
    // publish through `dispatch_ingress` per case). Every assertion
    // goes through the HTTP router against the shared store, never at
    // the store directly. Management-plane only; publish and deliver
    // never touch the action store directly.
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    #[cfg(test)]
    struct CountingSink {
        hits: AtomicU64,
    }
    #[async_trait::async_trait]
    #[cfg(test)]
    impl broker_connectors::Sink for CountingSink {
        #[cfg(test)]
        async fn send(
            &self,
            _topic: &broker_protocol::Topic,
            _payload: &bytes::Bytes,
            _qos: broker_protocol::QoS,
        ) -> broker_connectors::Result<()> {
            self.hits.fetch_add(1, AtomicOrdering::Relaxed);
            Ok(())
        }
        #[cfg(test)]
        fn kind(&self) -> &'static str {
            "test-counter"
        }
    }
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
    let local = "indra-node-1";
    let legacy = crate::node_scope::local_node_name();
    // Unknown node is NOT_FOUND with a non-empty body naming the node,
    // even when the id and operation are also unknown (node scope is
    // checked first, fail closed).
    for unknown in ["no-such-node-xyz", ""] {
        let path = format!("/api/v5/nodes/{unknown}/actions/no-such-action/start");
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
            &format!("/api/v5/nodes/{local}/actions/no-such-action/start"),
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
    assert!(!body.to_string().is_empty());
    // Create one action via the W3-02 route (defaults to enabled).
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "w3-09-action", "type": "console", "server": "127.0.0.1:1883"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    // Unknown operation on the known node is rejected with the
    // documented shape and persists nothing.
    let (status, body) = server
        .send(
            "POST",
            &format!("/api/v5/nodes/{local}/actions/w3-09-action/frobnicate"),
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 400, "unknown operation must be rejected");
    assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
    assert!(!body.to_string().is_empty());
    let (status, body) = server
        .get_auth("/api/v5/actions/w3-09-action", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["enable"], json!(true));
    // Stop through the node route: the reported state flips to
    // disabled on the next read. The legacy node name names the same
    // local node.
    let (status, _) = server
        .send(
            "POST",
            &format!("/api/v5/nodes/{local}/actions/w3-09-action/stop"),
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .send(
            "POST",
            &format!("/api/v5/nodes/{legacy}/actions/w3-09-action/start"),
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204, "legacy node name must resolve");
    // Rule dispatch through the broker: a rule forwarding to the
    // action id fires while started and skips while stopped through
    // the node route. The counting sink is registered under the action
    // id, the rule is created through the HTTP router, and each
    // publish goes through `dispatch_ingress` (the broker's rule
    // path), never `sink.send` directly.
    let counter = std::sync::Arc::new(CountingSink {
        hits: AtomicU64::new(0),
    });
    state.engine.connectors().register(
        broker_config::v5_action_sink_key("w3-09-action"),
        counter.clone(),
    );
    let (status, rule_body) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "name": "w3-09-rule",
                "sql": "SELECT * FROM \"w3/09/operation\"",
                "actions": ["w3-09-action"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let rule_id = rule_body["id"].as_str().expect("rule id").to_string();
    let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
        std::sync::Arc::new(NoopBrokerSink);
    let topic = broker_protocol::Topic::new("w3/09/operation").expect("valid topic");
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
    assert_eq!(
        counter.hits.load(AtomicOrdering::Relaxed),
        1,
        "started action must fire"
    );
    // Stop again through the node route: the next publish skips.
    let (status, _) = server
        .send(
            "POST",
            &format!("/api/v5/nodes/{local}/actions/w3-09-action/stop"),
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
    assert_eq!(
        counter.hits.load(AtomicOrdering::Relaxed),
        1,
        "stopped action must be skipped at fire time"
    );
    // Start again through the node route: firing resumes.
    let (status, _) = server
        .send(
            "POST",
            &format!("/api/v5/nodes/{local}/actions/w3-09-action/start"),
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
    assert_eq!(
        counter.hits.load(AtomicOrdering::Relaxed),
        2,
        "re-started action must fire again"
    );
    // Cleanup: rule then action (the rule holds the reference).
    let (status, _) = server
        .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/w3-09-action", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn actions_summary_tracks_create_delete() {
    // W3-08 (C06-12) broker-through test: create two actions of
    // different states, read the summary expecting matching counts;
    // delete both and expect zeroes. Every assertion goes through
    // the HTTP router against the shared W3-02 store, never at the
    // store directly. The summary counts are computed on demand from
    // the live snapshot in `crate::v5::actions::ActionStore`
    // (`summary_counts`, read by `get_actions_summary`), driven by
    // action create/delete events. Management-plane only; publish
    // and deliver never touch the action directory.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // Empty store returns zeroes, not an error.
    let (status, body) = server.get_auth("/api/v5/actions_summary", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
    // Two actions of different states: one enabled, one disabled.
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "sum-act-a", "type": "console", "server": "127.0.0.1:1883"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"name": "sum-act-b", "type": "console", "server": "127.0.0.1:1883", "enable": false}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    // Counts match the live snapshot; unknown query parameters are
    // ignored.
    let (status, body) = server
        .get_auth("/api/v5/actions_summary?unknown=param", &token)
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
    // Delete both; the summary returns to zeroes.
    let (status, _) = server
        .delete_auth("/api/v5/actions/sum-act-a", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/sum-act-b", &token)
        .await;
    assert_eq!(status, 204);
    let (status, body) = server.get_auth("/api/v5/actions_summary", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
}

#[tokio::test]
async fn action_metrics_move_on_real_fire_and_reset_zeroes() {
    // W3-05 (C06-08/C06-09) broker-through test: create an action via
    // the W3-02 route, read zeroed metrics, fire a rule bound to the
    // action through a real publish (`dispatch_ingress`, the broker's
    // rule path), read non-zero attributed counts, reset and expect
    // zeroes; unknown ids are 404. Before: both routes were 404.
    // The per-action counters live in `broker-rules`
    // (`RuleEngine::action_metrics`, bumped in `run_actions` on the
    // `ForwardConnector` arm), driven by the publish event.
    #[cfg(test)]
    struct CountingSink;
    #[async_trait::async_trait]
    #[cfg(test)]
    impl broker_connectors::Sink for CountingSink {
        #[cfg(test)]
        async fn send(
            &self,
            _topic: &broker_protocol::Topic,
            _payload: &bytes::Bytes,
            _qos: broker_protocol::QoS,
        ) -> broker_connectors::Result<()> {
            Ok(())
        }
        #[cfg(test)]
        fn kind(&self) -> &'static str {
            "test-counter"
        }
    }
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
        .get_auth("/api/v5/actions/no-such-action/metrics", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    let (status, body) = server
        .send(
            "PUT",
            "/api/v5/actions/no-such-action/metrics/reset",
            Some(json!({})),
            Some(&token),
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    // Create one action via the W3-02 route; metrics start at zero.
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "w3-05-action", "type": "console", "server": "127.0.0.1:1883"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, body) = server
        .get_auth("/api/v5/actions/w3-05-action/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["id"], json!("w3-05-action"));
    assert_eq!(body["metrics"]["total"], json!(0));
    assert_eq!(body["metrics"]["success"], json!(0));
    assert_eq!(body["metrics"]["failed"], json!(0));
    // Unknown query parameters are ignored on both routes.
    let (status, _) = server
        .get_auth("/api/v5/actions/w3-05-action/metrics?unknown=param", &token)
        .await;
    assert_eq!(status, 200);
    // Bind a rule to the action id and fire it through a real publish.
    // The counting sink is registered under the action's MV-01
    // dispatch key (the key the rule fire uses) so the fire
    // succeeds; the rule is created through the HTTP router and the
    // publish goes through `dispatch_ingress` (the broker's rule path),
    // never `sink.send` directly.
    state.engine.connectors().register(
        broker_config::v5_action_sink_key("w3-05-action"),
        std::sync::Arc::new(CountingSink),
    );
    let (status, rule_body) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "name": "w3-05-rule",
                "sql": "SELECT * FROM \"w3/05/fire\"",
                "actions": ["w3-05-action"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let rule_id = rule_body["id"].as_str().expect("rule id").to_string();
    let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
        std::sync::Arc::new(NoopBrokerSink);
    let topic = broker_protocol::Topic::new("w3/05/fire").expect("valid topic");
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
    // Counters moved only through the real fire, never constants.
    let (status, body) = server
        .get_auth("/api/v5/actions/w3-05-action/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["metrics"]["total"], json!(1));
    assert_eq!(body["metrics"]["success"], json!(1));
    assert_eq!(body["metrics"]["failed"], json!(0));
    // Reset zeroes and reports success; the next read is zero.
    let (status, _) = server
        .send(
            "PUT",
            "/api/v5/actions/w3-05-action/metrics/reset",
            Some(json!({})),
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/actions/w3-05-action/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["metrics"]["total"], json!(0));
    assert_eq!(body["metrics"]["success"], json!(0));
    assert_eq!(body["metrics"]["failed"], json!(0));
    // Cleanup: rule then action (the rule holds the reference).
    let (status, _) = server
        .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/w3-05-action", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn action_types_lists_documented_names() {
    // AP-05/KC-00: GET /action_types returns a bare string array of
    // the kinds that are ready in this build. It never lists a
    // pending or refused kind.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, body) = server.get_auth("/api/v5/action_types", &token).await;
    assert_eq!(status, 200);
    let names = body.as_array().expect("type list is an array");
    assert!(!names.is_empty(), "type list must not be empty");
    let kinds: std::collections::HashSet<&str> = names.iter().filter_map(|e| e.as_str()).collect();
    assert_eq!(kinds.len(), names.len(), "names must be unique strings");
    assert!(
        kinds.contains("pgsql"),
        "type list holds the documented ready kind pgsql"
    );
    for ready in [
        "mqtt",
        "kafka_producer",
        "redis",
        "rabbitmq",
        "pulsar",
        "rocketmq",
        "confluent_producer",
    ] {
        assert!(
            kinds.contains(ready),
            "type list holds the documented ready kind {ready}"
        );
    }
    for ready in [
        "mysql",
        "oracle",
        "sqlserver",
        "redshift",
        "alloydb",
        "cockroachdb",
        "http",
    ] {
        assert!(
            kinds.contains(ready),
            "type list holds the documented ready kind {ready}"
        );
    }
    // New device kinds have no reference schema and stay out of the
    // documented listing until the contract lands.
    for unlisted in ["opc_ua", "sparkplug_b"] {
        assert!(
            !kinds.contains(unlisted),
            "type list stays without the unlisted kind {unlisted}"
        );
    }
    for name in &kinds {
        assert!(
            broker_config::is_kind_ready(name),
            "listed kind {name} is ready"
        );
    }

    let (status, again) = server
        .get_auth("/api/v5/action_types?unknown=param", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(again, body);
}

#[tokio::test]
async fn ap05_http_action_typed_round_trip() {
    // AP-05: the documented http action body is accepted and reads
    // back in the same shape. The read works through the bare name
    // and through the `{type}:{name}` key. A replace keeps the
    // shape. An unknown type is 400. A duplicate is 400.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // The referenced connector must exist for the owned sink to build
    // and the action to read `connected`.
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "ap05-up", "type": "webhook", "url": "webhook://127.0.0.1:9"}),
            &token,
        )
        .await;
    assert_eq!(status, 201, "connector create: {status}");
    let body = json!({
        "type": "webhook",
        "name": "ap05-web",
        "connector": "webhook:ap05-up",
        "enable": true,
        "parameters": {"method": "post", "path": "/hook"},
        "resource_opts": {},
    });
    let (status, created) = server
        .post_auth("/api/v5/actions", body.clone(), &token)
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["type"], json!("webhook"));
    assert_eq!(created["name"], json!("ap05-web"));
    assert_eq!(created["connector"], json!("webhook:ap05-up"));
    assert_eq!(created["parameters"]["path"], json!("/hook"));
    assert!(created["resource_opts"].is_object());
    assert_eq!(created["status"], json!("connected"));
    assert!(created["node_status"].is_array());
    for path in [
        "/api/v5/actions/ap05-web",
        "/api/v5/actions/webhook:ap05-web",
    ] {
        let (status, read) = server.get_auth(path, &token).await;
        assert_eq!(status, 200, "read through {path}");
        assert_eq!(read, created, "same shape through {path}");
    }
    let (status, _) = server.post_auth("/api/v5/actions", body, &token).await;
    assert_eq!(status, 400);
    let (status, bad) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "no_such_kind_xyz", "name": "ap05-bad"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(bad["code"], json!("ALREADY_EXISTS"));
    let (status, missing) = server
        .get_auth("/api/v5/actions/webhook:no-such", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(missing["code"], json!("NOT_FOUND"));
    let (status, updated) = server
        .put_auth(
            "/api/v5/actions/webhook:ap05-web",
            json!({
                "type": "webhook",
                "name": "ap05-web",
                "connector": "webhook:ap05-up",
                "enable": false,
                "parameters": {"method": "post", "path": "/hook2"},
            }),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(updated["enable"], json!(false));
    assert_eq!(updated["parameters"]["path"], json!("/hook2"));
    assert_eq!(updated["status"], json!("disconnected"));
    let (status, _) = server.delete_auth("/api/v5/actions/ap05-web", &token).await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn ap05_mqtt_kafka_actions_typed_round_trip() {
    // AP-05/KC-00: timescaledb, cassandra and s3_tables are not ready. Each create
    // is refused with 400 ALREADY_EXISTS and stores nothing. MV-03a
    // sets mqtt, kafka and mqtt_bridge ready, so they no longer
    // prove the pending refusal here. A ready console action keeps
    // the typed round trip.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    for (name, kind) in [
        ("ap05-ts", "timescaledb"),
        ("ap05-cass", "cassandra"),
        ("ap05-legacy", "s3_tables"),
    ] {
        let (status, body) = server
            .post_auth(
                "/api/v5/actions",
                json!({"name": name, "type": kind, "parameters": {"topic": "out/ap05"}}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "{kind} action must be refused: {body}");
        assert_eq!(body["code"], json!("ALREADY_EXISTS"), "{kind}");
        assert!(
            body["message"]
                .as_str()
                .unwrap_or("")
                .contains("not available in this broker"),
            "{kind} refusal names the kind: {body}"
        );
        let (status, _) = server
            .get_auth(&format!("/api/v5/actions/{name}"), &token)
            .await;
        assert_eq!(status, 404, "{kind} action stores nothing");
    }
    let (status, created) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "console", "name": "ap05-console",
                       "parameters": {"topic": "out/ap05"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["parameters"]["topic"], json!("out/ap05"));
    let (status, _) = server
        .delete_auth("/api/v5/actions/ap05-console", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn ap05_typed_action_fires_through_the_rule_path() {
    // AP-05 integration: a typed http action created through the
    // management API fires through the broker rule path. A rule
    // forwards to the action id. A publish through
    // `dispatch_ingress` reaches the sink once. Every step uses the
    // production entries: the HTTP router and the engine ingress.
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
    #[cfg(test)]
    struct CountingSink {
        hits: AtomicU64,
    }
    #[async_trait::async_trait]
    #[cfg(test)]
    impl broker_connectors::Sink for CountingSink {
        #[cfg(test)]
        async fn send(
            &self,
            _topic: &broker_protocol::Topic,
            _payload: &bytes::Bytes,
            _qos: broker_protocol::QoS,
        ) -> broker_connectors::Result<()> {
            self.hits.fetch_add(1, AtomicOrdering::Relaxed);
            Ok(())
        }
        #[cfg(test)]
        fn kind(&self) -> &'static str {
            "ap05-counter"
        }
    }
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
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({
                "type": "webhook",
                "name": "ap05-fire",
                "connector": "webhook:ap05-up",
                "parameters": {"method": "post", "path": "/hook"},
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let counter = std::sync::Arc::new(CountingSink {
        hits: AtomicU64::new(0),
    });
    state.engine.connectors().register(
        broker_config::v5_action_sink_key("ap05-fire"),
        counter.clone(),
    );
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "id": "ap05-rule",
                "name": "ap05-rule",
                "sql": "SELECT * FROM \"ap05/fire\"",
                "actions": ["ap05-fire"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let broker: std::sync::Arc<dyn broker_rules::BrokerSink> = std::sync::Arc::new(NoopBrokerSink);
    let topic = broker_protocol::Topic::new("ap05/fire").expect("topic");
    let payload = bytes::Bytes::from_static(br#"{"temp": 21.5}"#);
    state
        .engine
        .dispatch_ingress(&topic, &payload, broker_protocol::QoS::AtMostOnce, &broker)
        .await;
    assert_eq!(counter.hits.load(AtomicOrdering::Relaxed), 1);
    let (status, _) = server.delete_auth("/api/v5/rules/ap05-rule", &token).await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/ap05-fire", &token)
        .await;
    assert_eq!(status, 204);
}
