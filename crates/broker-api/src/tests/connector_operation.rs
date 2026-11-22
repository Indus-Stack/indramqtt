use super::*;

#[tokio::test]
async fn removed_connector_operation_routes_return_404() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, _) = server
        .send(
            "PUT",
            "/api/v5/connectors/x/enable/true",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 404);
    let (status, _) = server
        .send("POST", "/api/v5/connectors/x/start", None, Some(&token))
        .await;
    assert_eq!(status, 404);
}

#[tokio::test]
async fn connector_enable_flag_round_trip_and_dispatch_skip() {
    // W3-19 (C07-06) broker-through test: create a connector via the
    // W3-17 route, disable it through the enable route and read back
    // disabled, re-enable and read back enabled; unknown ids are 404
    // and malformed flags are 400, both with the documented shape.
    // Then a rule forwarding to the shared id fires while enabled
    // and is skipped at fire time while disabled, through the
    // engine's lock-free dispatch snapshot (one publish through
    // `dispatch_ingress` per case). Management-plane only otherwise;
    // publish and deliver never touch the connector store directly.
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
            "/api/v5/connectors/no-such-connector/enable/false",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    // Create one connector via the W3-17 route (defaults to enabled).
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "w3-19-conn", "type": "webhook", "url": "http://127.0.0.1:9"}),
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
                &format!("/api/v5/connectors/w3-19-conn/enable/{bad}"),
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
            "/api/v5/connectors/w3-19-conn/enable/false",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/connectors/w3-19-conn", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["enable"], json!(false));
    // Re-enable: visible on the next read.
    let (status, _) = server
        .send(
            "PUT",
            "/api/v5/connectors/w3-19-conn/enable/true",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/connectors/w3-19-conn", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["enable"], json!(true));
    // Rule dispatch through the broker: the connector id shares the
    // engine's lock-free dispatch snapshot with action ids, so a rule
    // forwarding to this id fires while enabled and skips while
    // disabled. The counting sink is registered under the shared id
    // (an action with the same id exists so the rule validates), the
    // rule is created through the HTTP router, and each publish goes
    // through `dispatch_ingress` (the broker's rule path), never
    // `sink.send` directly.
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "w3-19-conn", "type": "console", "server": "127.0.0.1:1883"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let counter = std::sync::Arc::new(CountingSink {
        hits: AtomicU64::new(0),
    });
    state.engine.connectors().register(
        broker_config::v5_action_sink_key("w3-19-conn"),
        counter.clone(),
    );
    let (status, rule_body) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "name": "w3-19-rule",
                "sql": "SELECT * FROM \"w3/19/enable\"",
                "actions": ["w3-19-conn"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let rule_id = rule_body["id"].as_str().expect("rule id").to_string();
    let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
        std::sync::Arc::new(NoopBrokerSink);
    let topic = broker_protocol::Topic::new("w3/19/enable").expect("valid topic");
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
        "enabled connector must fire"
    );
    // Disable again: the next publish skips the forward.
    let (status, _) = server
        .send(
            "PUT",
            "/api/v5/connectors/w3-19-conn/enable/false",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    assert!(!state.engine.is_action_enabled("w3-19-conn"));
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
        "disabled connector must be skipped at fire time"
    );
    // Re-enable: firing resumes without re-registering anything.
    let (status, _) = server
        .send(
            "PUT",
            "/api/v5/connectors/w3-19-conn/enable/true",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    assert!(state.engine.is_action_enabled("w3-19-conn"));
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
        "re-enabled connector must fire again"
    );
    // Cleanup: rule, then action, then connector (the rule holds the
    // reference the connector delete guard checks).
    let (status, _) = server
        .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/w3-19-conn", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/connectors/w3-19-conn", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn connector_store_is_per_state_not_process_global() {
    // AP-10 broker-through test: two servers in one process hold
    // separate connector lists. A connector made on the first is
    // absent on the second. Before the move it was present: every
    // state shared one process static.
    let (server_a, _) = TestServer::start().await;
    let (server_b, _) = TestServer::start().await;
    let token_a = server_a.login_as_admin().await;
    let token_b = server_b.login_as_admin().await;
    let (status, _) = server_a
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "ap-10-isolated", "type": "webhook", "url": "http://127.0.0.1:9"}),
            &token_a,
        )
        .await;
    assert_eq!(status, 201);
    let (status, body) = server_a
        .get_auth("/api/v5/connectors/ap-10-isolated", &token_a)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["id"], json!("ap-10-isolated"));
    let (status, body) = server_b
        .get_auth("/api/v5/connectors/ap-10-isolated", &token_b)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    let (status, body) = server_b.get_auth("/api/v5/connectors", &token_b).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
    let (status, _) = server_a
        .delete_auth("/api/v5/connectors/ap-10-isolated", &token_a)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn connector_operation_start_stop_round_trip() {
    // W3-20 (C07-07) broker-through test: create a connector via the
    // W3-17 route, stop it through the operation route and read back
    // disabled, start it and read back enabled; an unknown operation
    // is rejected and an unknown id is 404, both with the documented
    // shape and a non-empty body. Then a rule forwarding to the
    // connector id fires while started and is skipped at fire time
    // while stopped, through the engine's lock-free dispatch snapshot
    // (one publish through `dispatch_ingress` per case, the broker's
    // rule path also used by `crates/broker-node` ingress). Every
    // assertion goes through the HTTP router against the shared store,
    // never at the store directly. Management-plane only otherwise;
    // publish and deliver never touch the connector store directly.
    // The connector id shares the dispatch snapshot with action ids
    // (`crates/broker-rules/src/lib.rs`, `RuleEngine::set_action_enabled`
    // at line 1717 consulted on the publish event by
    // `dispatch_ingress`/fire path), driven by the publish event.
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
            "/api/v5/connectors/no-such-connector/start",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    // Unknown id with an unknown operation is still 404 (existence
    // before validation) and persists nothing.
    let (status, body) = server
        .send(
            "POST",
            "/api/v5/connectors/no-such-connector/frobnicate",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    // Create one connector via the W3-17 route (defaults to enabled).
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "w3-20-conn", "type": "webhook", "url": "http://127.0.0.1:9"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    // Unknown operation is rejected with the documented shape and
    // persists nothing (the stored flag is untouched).
    let (status, body) = server
        .send(
            "POST",
            "/api/v5/connectors/w3-20-conn/frobnicate",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 400, "unknown operation must be rejected");
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, body) = server
        .get_auth("/api/v5/connectors/w3-20-conn", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["enable"], json!(true));
    // Stop: the reported state changes to disabled on the next read.
    let (status, _) = server
        .send(
            "POST",
            "/api/v5/connectors/w3-20-conn/stop",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/connectors/w3-20-conn", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["enable"], json!(false));
    // Start: the reported state changes back to enabled.
    let (status, _) = server
        .send(
            "POST",
            "/api/v5/connectors/w3-20-conn/start",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/connectors/w3-20-conn", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["enable"], json!(true));
    // Rule dispatch through the broker: the connector id shares the
    // engine's lock-free dispatch snapshot with action ids, so a rule
    // forwarding to this id fires while started and skips while
    // stopped. An action with the same id exists so the rule validates
    // (the W3-02 linkage), the counting sink is registered under the
    // shared id, the rule is created through the HTTP router, and each
    // publish goes through `dispatch_ingress` (the broker's rule path),
    // never `sink.send` directly.
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "w3-20-conn", "type": "console", "server": "127.0.0.1:1883"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let counter = std::sync::Arc::new(CountingSink {
        hits: AtomicU64::new(0),
    });
    state.engine.connectors().register(
        broker_config::v5_action_sink_key("w3-20-conn"),
        counter.clone(),
    );
    let (status, rule_body) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "name": "w3-20-rule",
                "sql": "SELECT * FROM \"w3/20/operation\"",
                "actions": ["w3-20-conn"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let rule_id = rule_body["id"].as_str().expect("rule id").to_string();
    let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
        std::sync::Arc::new(NoopBrokerSink);
    let topic = broker_protocol::Topic::new("w3/20/operation").expect("valid topic");
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
        "started connector must fire"
    );
    // Stop again: the next publish skips the forward.
    let (status, _) = server
        .send(
            "POST",
            "/api/v5/connectors/w3-20-conn/stop",
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
        "stopped connector must be skipped at fire time"
    );
    // Start again: firing resumes without re-registering anything.
    let (status, _) = server
        .send(
            "POST",
            "/api/v5/connectors/w3-20-conn/start",
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
        "re-started connector must fire again"
    );
    // Cleanup: rule, then action, then connector (the rule holds the
    // reference the connector delete guard checks).
    let (status, _) = server
        .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/w3-20-conn", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/connectors/w3-20-conn", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn sb15_connector_restart_keeps_settings_and_gates_delivery() {
    // MV-06b broker-through test: `restart` enables the connector,
    // keeps every stored setting and gates delivery. Each step goes
    // through the HTTP router. Publishes go through
    // `dispatch_ingress` (the broker rule path).
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
    struct ProbeFlipSink {
        healthy: std::sync::atomic::AtomicBool,
        reason: std::sync::Mutex<Option<String>>,
    }
    #[cfg(test)]
    impl ProbeFlipSink {
        #[cfg(test)]
        fn new() -> Self {
            Self {
                healthy: std::sync::atomic::AtomicBool::new(true),
                reason: std::sync::Mutex::new(None),
            }
        }
    }
    #[async_trait::async_trait]
    #[cfg(test)]
    impl broker_connectors::Sink for ProbeFlipSink {
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
        async fn probe(&self) -> std::result::Result<(), String> {
            // Flip the health on each call.
            let was = self.healthy.load(AtomicOrdering::Relaxed);
            self.healthy.store(!was, AtomicOrdering::Relaxed);
            if was {
                let text = "sb15 restart probe flipped to down: connection refused".to_string();
                *self.reason.lock().expect("probe lock") = Some(text.clone());
                Err(text)
            } else {
                *self.reason.lock().expect("probe lock") = None;
                Ok(())
            }
        }
        #[cfg(test)]
        fn health(&self) -> Option<broker_connectors::SinkHealth> {
            Some(broker_connectors::SinkHealth {
                healthy: self.healthy.load(AtomicOrdering::Relaxed),
                reason: self.reason.lock().expect("health lock").clone(),
            })
        }
        #[cfg(test)]
        fn kind(&self) -> &'static str {
            "webhook"
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
            "/api/v5/connectors",
            json!({"name": "sb15-restart", "type": "webhook",
                       "url": "http://127.0.0.1:9",
                       "description": "sb15 settings"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    // Register a probe fixture under the connector id.
    // Its `probe` flips `health`, so only a real
    // restart changes the next read.
    let probe_sink = std::sync::Arc::new(ProbeFlipSink::new());
    state
        .engine
        .connectors()
        .register("sb15-restart", probe_sink.clone());
    // Unknown operation is refused and keeps the stored state.
    let (status, body) = server
        .send(
            "POST",
            "/api/v5/connectors/sb15-restart/frobnicate",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, body) = server
        .send(
            "POST",
            "/api/v5/connectors/no-such-sb15/restart",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    // Stop disables; restart enables and keeps the settings.
    let (status, _) = server
        .send(
            "POST",
            "/api/v5/connectors/sb15-restart/stop",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/connectors/sb15-restart", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["enable"], json!(false));
    assert_eq!(body["status"], json!("connected"));
    let (status, _) = server
        .send(
            "POST",
            "/api/v5/connectors/sb15-restart/restart",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/connectors/sb15-restart", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["enable"], json!(true));
    assert_eq!(body["url"], json!("http://127.0.0.1:9"));
    assert_eq!(body["description"], json!("sb15 settings"));
    assert_eq!(body["status"], json!("disconnected"));
    assert!(
        body["status_reason"]
            .as_str()
            .is_some_and(|reason| !reason.is_empty()),
        "restart must run a fresh probe: {body}"
    );
    // Restart while enabled keeps the connector enabled.
    let (status, _) = server
        .send(
            "POST",
            "/api/v5/connectors/sb15-restart/restart",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth("/api/v5/connectors/sb15-restart", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["enable"], json!(true));
    // Delivery follows the lifecycle through the rule path.
    // The action id differs from the connector id.
    // Its `connector` field drives the dependent path.
    let connector_id = "sb15-restart";
    let action_id = "sb15-restart-act";
    assert_ne!(connector_id, action_id);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "sb15-restart-act", "type": "console", "server": "127.0.0.1:1883",
                       "connector": "sb15-restart"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let counter = std::sync::Arc::new(CountingSink {
        hits: AtomicU64::new(0),
    });
    state.engine.connectors().register(
        broker_config::v5_action_sink_key("sb15-restart-act"),
        counter.clone(),
    );
    let (status, rule_body) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "name": "sb15-restart-rule",
                "sql": "SELECT * FROM \"sb15/restart\"",
                "actions": ["sb15-restart-act"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let rule_id = rule_body["id"].as_str().expect("rule id").to_string();
    let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
        std::sync::Arc::new(NoopBrokerSink);
    let topic = broker_protocol::Topic::new("sb15/restart").expect("valid topic");
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
    assert_eq!(counter.hits.load(AtomicOrdering::Relaxed), 1);
    let (status, _) = server
        .send(
            "POST",
            "/api/v5/connectors/sb15-restart/stop",
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
        "stopped connector must be skipped at fire time"
    );
    let (status, _) = server
        .send(
            "POST",
            "/api/v5/connectors/sb15-restart/restart",
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
        "restarted connector must fire again"
    );
    let (status, _) = server
        .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/sb15-restart-act", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/connectors/sb15-restart", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn sb15_connector_health_reports_sink_state_per_action() {
    // MV-06b broker-through test: `status` and `status_reason` come
    // from the sink state. A connector with no actions reads fine. A
    // valid offline server reads `disconnected` with a reason. Two
    // actions report their own health. A failed update keeps the old
    // settings. Each step goes through the HTTP router.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, created) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "sb15-lonely", "type": "webhook",
                       "url": "http://127.0.0.1:9"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["status"], json!("disconnected"));
    let (status, body) = server
        .get_auth("/api/v5/connectors/sb15-lonely", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["status"], json!("disconnected"));
    assert!(
        body["status_reason"]
            .as_str()
            .is_some_and(|reason| !reason.is_empty()),
        "offline server must name a reason: {body}"
    );
    assert_eq!(body["node_status"][0]["status"], json!("disconnected"));
    assert_eq!(body["enable"], json!(true));
    let (status, list) = server.get_auth("/api/v5/connectors", &token).await;
    assert_eq!(status, 200);
    assert!(
        list.as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["name"] == json!("sb15-lonely"))),
        "connector with no actions still lists"
    );
    // Two actions on one connector report their own health.
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "sb15-act-up", "type": "console", "server": "127.0.0.1:1883",
                       "connector": "sb15-lonely"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "sb15-act-down", "type": "http",
                       "connector": "sb15-lonely",
                       "parameters": {"path": "/ingest/${topic}", "method": "post"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, up) = server.get_auth("/api/v5/actions/sb15-act-up", &token).await;
    assert_eq!(status, 200);
    assert_eq!(up["status"], json!("connected"));
    let (status, down) = server
        .get_auth("/api/v5/actions/sb15-act-down", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(down["status"], json!("disconnected"));
    assert!(
        down["status_reason"]
            .as_str()
            .is_some_and(|reason| !reason.is_empty()),
        "down action must name a reason: {down}"
    );
    // A failed update keeps the old settings in place.
    let (status, _) = server
        .put_auth(
            "/api/v5/connectors/sb15-lonely",
            json!({"name": "sb15-lonely", "type": "webhook"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    let (status, body) = server
        .get_auth("/api/v5/connectors/sb15-lonely", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["url"], json!("http://127.0.0.1:9"));
    assert_eq!(body["enable"], json!(true));
    let (status, _) = server
        .delete_auth("/api/v5/actions/sb15-act-up", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/sb15-act-down", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/connectors/sb15-lonely", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn node_connector_operation_round_trip() {
    // W3-22 (C07-09) broker-through test: create a connector via the
    // W3-17 route, run `stop`/`start` through the node-scoped route
    // against the local node and read back the flipped state; an
    // unknown node is NOT_FOUND, an unknown id is 404 and an unknown
    // operation is rejected, all with the documented shape and a
    // non-empty body. Then a rule forwarding to the connector id fires
    // while started and is skipped while stopped through the node
    // route, through the engine's lock-free dispatch snapshot (one
    // publish through `dispatch_ingress` per case, the broker's rule
    // path). Every assertion goes through the HTTP router against the
    // shared store, never at the store directly. Management-plane only;
    // publish and deliver never touch the connector store directly.
    // The connector id shares the dispatch snapshot with action ids
    // (`crates/broker-rules/src/lib.rs`, `RuleEngine::set_action_enabled`
    // consulted on the publish event by `dispatch_ingress`/fire path),
    // driven by the publish event.
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
        let path = format!("/api/v5/nodes/{unknown}/connectors/no-such-connector/start");
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
            &format!("/api/v5/nodes/{local}/connectors/no-such-connector/start"),
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    assert!(!body.to_string().is_empty());
    // Create one connector via the W3-17 route (defaults to enabled).
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"name": "w3-22-conn", "type": "webhook", "url": "http://127.0.0.1:9"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    // Unknown operation on the known node is rejected with the
    // documented shape and persists nothing.
    let (status, body) = server
        .send(
            "POST",
            &format!("/api/v5/nodes/{local}/connectors/w3-22-conn/frobnicate"),
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 400, "unknown operation must be rejected");
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    assert!(!body.to_string().is_empty());
    let (status, body) = server
        .get_auth("/api/v5/connectors/w3-22-conn", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["enable"], json!(true));
    // Stop through the node route: the reported state flips to
    // disabled on the next read. The legacy node name names the same
    // local node.
    let (status, _) = server
        .send(
            "POST",
            &format!("/api/v5/nodes/{local}/connectors/w3-22-conn/stop"),
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .send(
            "POST",
            &format!("/api/v5/nodes/{legacy}/connectors/w3-22-conn/start"),
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 204, "legacy node name must resolve");
    // Rule dispatch through the broker: the connector id shares the
    // engine's lock-free dispatch snapshot with action ids, so a rule
    // forwarding to this id fires while started and skips while
    // stopped. An action with the same id exists so the rule validates
    // (the W3-02 linkage), the counting sink is registered under the
    // shared id, the rule is created through the HTTP router, and each
    // publish goes through `dispatch_ingress` (the broker's rule path),
    // never `sink.send` directly.
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "w3-22-conn", "type": "console", "server": "127.0.0.1:1883"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let counter = std::sync::Arc::new(CountingSink {
        hits: AtomicU64::new(0),
    });
    state.engine.connectors().register(
        broker_config::v5_action_sink_key("w3-22-conn"),
        counter.clone(),
    );
    let (status, rule_body) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "name": "w3-22-rule",
                "sql": "SELECT * FROM \"w3/22/operation\"",
                "actions": ["w3-22-conn"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let rule_id = rule_body["id"].as_str().expect("rule id").to_string();
    let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
        std::sync::Arc::new(NoopBrokerSink);
    let topic = broker_protocol::Topic::new("w3/22/operation").expect("valid topic");
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
        "started connector must fire"
    );
    // Stop again through the node route: the next publish skips.
    let (status, _) = server
        .send(
            "POST",
            &format!("/api/v5/nodes/{local}/connectors/w3-22-conn/stop"),
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
        "stopped connector must be skipped at fire time"
    );
    // Start again through the node route: firing resumes.
    let (status, _) = server
        .send(
            "POST",
            &format!("/api/v5/nodes/{local}/connectors/w3-22-conn/start"),
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
        "re-started connector must fire again"
    );
    // Cleanup: rule, then action, then connector (the rule holds the
    // reference the connector delete guard checks).
    let (status, _) = server
        .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/w3-22-conn", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/connectors/w3-22-conn", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn connector_update_drains_waiting_rows_with_no_loss() {
    // One row waits in the action buffer. A connector update drains it
    // with no loss before the runtime swap.
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (address, hits, receiver) = spawn_ro_counting_http_target().await;
    let connector = json!({
        "name": "sb25-connector-hook",
        "type": "webhook",
        "url": format!("http://{address}"),
    });
    let (status, body) = server
        .post_auth("/api/v5/connectors", connector.clone(), &token)
        .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = server
        .post_auth(
            "/api/v5/actions",
            json!({
                "name": "sb25-connector-action",
                "type": "webhook",
                "connector": "sb25-connector-hook",
                "parameters": {"path": "/conn"},
                "resource_opts": {
                    "batch_size": 100,
                    "batch_time": "10s",
                    "request_ttl": "30s",
                    "health_check_interval": "1h",
                },
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "name": "sb25-connector-rule",
                "sql": "SELECT * FROM \"sb25/in/connector\"",
                "actions": ["sb25-connector-action"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201, "rule create: {body}");
    let sink: Arc<dyn broker_rules::BrokerSink> = Arc::new(NoopBrokerSink);
    state
        .engine
        .dispatch_ingress(
            &Topic::new("sb25/in/connector").expect("topic"),
            &bytes::Bytes::from_static(br#"{"n":1}"#),
            QoS::AtMostOnce,
            &sink,
        )
        .await;
    let (status, body) = server
        .put_auth("/api/v5/connectors/sb25-connector-hook", connector, &token)
        .await;
    assert_eq!(status, 200, "connector update: {body}");
    let (status, body) = server
        .get_auth("/api/v5/actions/sb25-connector-action/metrics", &token)
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["metrics"]["success"], json!(1), "{body}");
    assert_eq!(body["metrics"]["dropped"], json!(0), "{body}");
    assert_eq!(
        body["metrics"]["dropped.resource_stopped"],
        json!(0),
        "{body}"
    );
    assert_eq!(body["metrics"]["queuing"], json!(0), "{body}");
    assert_eq!(
        hits.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the waiting row reached the target"
    );
    let (status, _) = server
        .delete_auth("/api/v5/rules/sb25-connector-rule", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/sb25-connector-action", &token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/connectors/sb25-connector-hook", &token)
        .await;
    assert_eq!(status, 204);
    receiver.abort();
}
