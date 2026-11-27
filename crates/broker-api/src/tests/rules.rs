use super::*;

#[tokio::test]
async fn test_rules_crud_lifecycle() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Empty at first.
    let (status, body) = server.get_auth("/api/v1/rules", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));

    // Create.
    let (status, created) = server
        .post_auth(
            "/api/v1/rules",
            json!({
                "name": "republish-temp",
                "topic_filter": "sensors/+",
                "enabled": true,
                "actions": [{"type": "republish", "topic": "alerts/critical", "qos": 1}]
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["name"], json!("republish-temp"));
    assert_eq!(created["topic_filter"], json!("sensors/+"));
    assert_eq!(created["actions"][0]["type"], json!("republish"));
    assert_eq!(created["actions"][0]["qos"], json!(1));
    let id = created["id"]
        .as_str()
        .expect("created rule has id")
        .to_string();

    // List shows it.
    let (status, body) = server.get_auth("/api/v1/rules", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().expect("list").len(), 1);

    // Fetch it directly.
    let (status, fetched) = server
        .get_auth(&format!("/api/v1/rules/{id}"), &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(fetched, created);

    // Unknown id is a 404.
    let (status, body) = server.get_auth("/api/v1/rules/rule-999", &token).await;
    assert_eq!(status, 404);
    assert!(body["error"].as_str().unwrap().contains("rule-999"));

    // Delete it.
    let (status, _) = server
        .delete_auth(&format!("/api/v1/rules/{id}"), &token)
        .await;
    assert_eq!(status, 204);

    // Gone afterwards; second delete is a 404.
    let (status, _) = server
        .get_auth(&format!("/api/v1/rules/{id}"), &token)
        .await;
    assert_eq!(status, 404);
    let (status, _) = server
        .delete_auth(&format!("/api/v1/rules/{id}"), &token)
        .await;
    assert_eq!(status, 404);
}

/// Record the tenant of each rule output through the broker sink.
#[cfg(test)]
struct TenantProbe {
    recorded: std::sync::Mutex<Vec<(String, String)>>,
}

#[async_trait::async_trait]
#[cfg(test)]
impl broker_rules::BrokerSink for TenantProbe {
    #[cfg(test)]
    async fn publish(
        &self,
        topic: Topic,
        _payload: bytes::Bytes,
        _qos: QoS,
        _retain: bool,
    ) -> Result<(), broker_rules::RuleEngineError> {
        self.recorded
            .lock()
            .unwrap()
            .push(("default".to_string(), topic.as_str().to_string()));
        Ok(())
    }

    #[cfg(test)]
    async fn publish_in_tenant(
        &self,
        tenant: &str,
        topic: Topic,
        _payload: bytes::Bytes,
        _qos: QoS,
        _retain: bool,
    ) -> Result<(), broker_rules::RuleEngineError> {
        self.recorded
            .lock()
            .unwrap()
            .push((tenant.to_string(), topic.as_str().to_string()));
        Ok(())
    }
}

/// Unknown rule tenants fail closed through the v1 API: an unknown
/// tenant and a malformed tenant both answer 400 and store nothing.
#[tokio::test]
async fn test_rules_tenant_unknown_rejected_through_api() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    for tenant in ["tm04-ghost", "bad\u{7}tenant"] {
        let (status, body) = server
            .post_auth(
                "/api/v1/rules",
                json!({
                    "name": "tm04-bad",
                    "topic_filter": "tm04/#",
                    "tenant": tenant,
                    "actions": []
                }),
                &token,
            )
            .await;
        assert_eq!(status, 400, "tenant {tenant:?} must fail");
        assert!(
            body["error"].as_str().unwrap().contains("tenant"),
            "error names the field: {body}"
        );
    }
    let (status, body) = server.get_auth("/api/v1/rules", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]), "nothing stored");
    assert!(state.engine.list_rules().is_empty());
}

/// One rule of tenant A is created through the v1 API. It reads
/// back with its tenant. It fires only for a publish of tenant A
/// through the broker dispatch. Its output carries tenant A only.
#[tokio::test]
async fn test_rules_tenant_round_trip_through_api_and_broker() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // Two tenants through the production namespace entry.
    for ns in ["tm04-a", "tm04-b"] {
        let (status, _) = server
            .post_auth(&format!("/api/v5/mt/ns/{ns}"), json!({}), &token)
            .await;
        assert_eq!(status, 204, "namespace {ns} creates");
    }
    // Random value the broker cannot know.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|span| span.as_nanos())
        .unwrap_or(0);
    let secret = format!("tm04-secret-{nanos}");
    // Create the rule of tenant A with a republish action.
    let (status, created) = server
        .post_auth(
            "/api/v1/rules",
            json!({
                "name": "tm04-a-rule",
                "topic_filter": "tm04/in",
                "tenant": "tm04-a",
                "actions": [{"type": "republish", "topic": "tm04/out", "qos": 0}]
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201, "rule creates: {created}");
    assert_eq!(created["tenant"], json!("tm04-a"));
    let id = created["id"]
        .as_str()
        .expect("created rule has id")
        .to_string();
    // The read shows the same tenant.
    let (status, fetched) = server
        .get_auth(&format!("/api/v1/rules/{id}"), &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(fetched["tenant"], json!("tm04-a"));
    // No tenant in the body means the default tenant, as before.
    let (status, created) = server
        .post_auth(
            "/api/v1/rules",
            json!({
                "name": "tm04-plain",
                "topic_filter": "tm04/in",
                "actions": []
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["tenant"], json!("default"));
    // Through the broker: a publish of A fires only the A rule,
    // and the output carries tenant A. A publish of B fires
    // nothing: no rule of B matches.
    let probe = Arc::new(TenantProbe {
        recorded: std::sync::Mutex::new(Vec::new()),
    });
    let probe_obj: Arc<dyn broker_rules::BrokerSink> = probe.clone();
    let topic = Topic::new("tm04/in").expect("topic parses");
    let payload = bytes::Bytes::from(secret.clone());
    let fired = state
        .engine
        .dispatch_ingress_in_tenant("tm04-a", &topic, &payload, QoS::AtMostOnce, &probe_obj)
        .await;
    assert_eq!(fired, 1, "only the A rule fires for an A publish");
    let fired = state
        .engine
        .dispatch_ingress_in_tenant("tm04-b", &topic, &payload, QoS::AtMostOnce, &probe_obj)
        .await;
    assert_eq!(fired, 0, "no rule fires for a B publish");
    let recorded = probe.recorded.lock().unwrap().clone();
    assert_eq!(recorded.len(), 1, "one output, from the A publish");
    assert_eq!(recorded[0], ("tm04-a".to_string(), "tm04/out".to_string()));
    // Cleanup through the API.
    for rule_id in [id, created["id"].as_str().unwrap().to_string()] {
        let (status, _) = server
            .delete_auth(&format!("/api/v1/rules/{rule_id}"), &token)
            .await;
        assert_eq!(status, 204);
    }
}

/// The v5 rule API carries the tenant: create validates it, the
/// read shows it, update moves it, and dispatch honours it.
#[tokio::test]
async fn test_v5_rules_tenant_create_update_dispatch_through_api() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    for ns in ["tm04-v5a", "tm04-v5b"] {
        let (status, _) = server
            .post_auth(&format!("/api/v5/mt/ns/{ns}"), json!({}), &token)
            .await;
        assert_eq!(status, 204, "namespace {ns} creates");
    }
    // Unknown tenant fails with the documented shape.
    let (status, body) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "name": "tm04-v5-bad",
                "sql": "SELECT * FROM \"tm04v5/in\"",
                "tenant": "tm04-ghost",
                "actions": []
            }),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    // Create in the first tenant.
    let (status, created) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "name": "tm04-v5-rule",
                "sql": "SELECT * FROM \"tm04v5/in\"",
                "tenant": "tm04-v5a",
                "actions": []
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201, "rule creates: {created}");
    assert_eq!(created["tenant"], json!("tm04-v5a"));
    let id = created["id"]
        .as_str()
        .expect("created rule has id")
        .to_string();
    // Update moves the rule to the second tenant.
    let (status, updated) = server
        .put_auth(
            &format!("/api/v5/rules/{id}"),
            json!({
                "name": "tm04-v5-rule",
                "sql": "SELECT * FROM \"tm04v5/in\"",
                "tenant": "tm04-v5b",
                "actions": []
            }),
            &token,
        )
        .await;
    assert_eq!(status, 200, "update moves tenant: {updated}");
    assert_eq!(updated["tenant"], json!("tm04-v5b"));
    // Update to an unknown tenant fails and keeps the old rule.
    let (status, body) = server
        .put_auth(
            &format!("/api/v5/rules/{id}"),
            json!({
                "name": "tm04-v5-rule",
                "sql": "SELECT * FROM \"tm04v5/in\"",
                "tenant": "tm04-ghost",
                "actions": []
            }),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, fetched) = server
        .get_auth(&format!("/api/v5/rules/{id}"), &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(fetched["tenant"], json!("tm04-v5b"));
    // Dispatch honours the moved tenant.
    let probe = Arc::new(TenantProbe {
        recorded: std::sync::Mutex::new(Vec::new()),
    });
    let probe_obj: Arc<dyn broker_rules::BrokerSink> = probe.clone();
    let topic = Topic::new("tm04v5/in").expect("topic parses");
    let payload = bytes::Bytes::from_static(br#"{"temp": 21}"#);
    let fired = state
        .engine
        .dispatch_ingress_in_tenant("tm04-v5a", &topic, &payload, QoS::AtMostOnce, &probe_obj)
        .await;
    assert_eq!(fired, 0, "the moved rule no longer fires in A");
    let fired = state
        .engine
        .dispatch_ingress_in_tenant("tm04-v5b", &topic, &payload, QoS::AtMostOnce, &probe_obj)
        .await;
    assert_eq!(fired, 1, "the moved rule fires in B");
}

#[tokio::test]
async fn test_rules_create_rejects_invalid_input() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Bad topic filter.
    let (status, body) = server
        .post_auth(
            "/api/v1/rules",
            json!({"name": "bad", "topic_filter": "sport/#/bogus", "actions": []}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert!(body["error"].as_str().unwrap().contains("topic_filter"));

    // Bad republish QoS.
    let (status, body) = server
        .post_auth(
            "/api/v1/rules",
            json!({"name": "bad", "topic_filter": "a/#",
                       "actions": [{"type": "republish", "topic": "b", "qos": 7}]}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert!(body["error"].as_str().unwrap().contains("qos"));

    // Wildcard republish target is not a concrete topic.
    let (status, body) = server
        .post_auth(
            "/api/v1/rules",
            json!({"name": "bad", "topic_filter": "a/#",
                       "actions": [{"type": "republish", "topic": "b/#", "qos": 0}]}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert!(body["error"].as_str().unwrap().contains("topic"));

    // Empty name.
    let (status, _) = server
        .post_auth(
            "/api/v1/rules",
            json!({"name": "  ", "topic_filter": "a/#", "actions": []}),
            &token,
        )
        .await;
    assert_eq!(status, 400);

    // Nothing was stored by the failed creates.
    let (status, body) = server.get_auth("/api/v1/rules", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
}

#[tokio::test]
async fn test_rules_create_with_sql_query() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Valid streaming SQL is accepted and echoed back verbatim.
    let (status, created) = server
        .post_auth(
            "/api/v1/rules",
            json!({"name": "hot-temp",
                       "topic_filter": "sensors/+",
                       "sql_query": "SELECT * FROM \"sensors/+\" WHERE temperature > 50.0",
                       "enabled": true,
                       "actions": [{"type": "republish", "topic": "alerts/hot", "qos": 0}]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(
        created["sql_query"],
        json!("SELECT * FROM \"sensors/+\" WHERE temperature > 50.0")
    );
    let id = created["id"]
        .as_str()
        .expect("created rule has id")
        .to_string();
    let (status, _) = server
        .get_auth(&format!("/api/v1/rules/{id}"), &token)
        .await;
    assert_eq!(status, 200);

    // Broken SQL is a 400 and stores nothing.
    let (status, body) = server
        .post_auth(
            "/api/v1/rules",
            json!({"name": "broken",
                       "topic_filter": "sensors/+",
                       "sql_query": "SELECT WHERE WHERE",
                       "actions": []}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert!(body["error"].as_str().unwrap().contains("sql_query"));

    let (status, body) = server.get_auth("/api/v1/rules", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().expect("list").len(), 1);
}

#[tokio::test]
async fn test_rules_create_with_forward_connector() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, created) = server
        .post_auth(
            "/api/v1/rules",
            json!({"name": "to-webhook",
                       "topic_filter": "sensors/+",
                       "sql_query": "SELECT temperature FROM \"sensors/+\" WHERE temperature > 0",
                       "actions": [{"type": "forwardconnector", "connector_id": "webhook-1"}]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["actions"][0]["type"], json!("forwardconnector"));
    assert_eq!(created["actions"][0]["connector_id"], json!("webhook-1"));

    // Empty connector id is rejected.
    let (status, _) = server
        .post_auth(
            "/api/v1/rules",
            json!({"name": "bad",
                       "topic_filter": "sensors/+",
                       "actions": [{"type": "forwardconnector", "connector_id": "  "}]}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
}

#[tokio::test]
async fn rules_list_echoes_stored_actions_only() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, created) = server
        .post_auth(
            "/api/v5/rules",
            json!({"name": "no-actions",
                       "sql": "SELECT * FROM \"t/#\"",
                       "enable": true,
                       "actions": []}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["actions"], json!([]));
    assert_eq!(created["description"], json!(""));
    assert!(created.get("created_at").is_none());

    let (status, body) = server.get_auth("/api/v5/rules", &token).await;
    assert_eq!(status, 200);
    let text = serde_json::to_string(&body).expect("encode list body");
    assert!(
        !text.contains("kafka:kafka-prod"),
        "list must not inject fallback actions, got: {text}"
    );
    let data = body["data"].as_array().expect("data is a list");
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["actions"], json!([]));
    assert_eq!(data[0]["description"], json!(""));
    assert!(data[0].get("created_at").is_none());
}

#[tokio::test]
async fn rules_builtin_functions_round_trip_in_documented_shape() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // The example of the specification: a republish and a console.
    let body = json!({
        "name": "builtin-fn",
        "sql": "SELECT * FROM \"test/topic\" WHERE payload.x = 1",
        "actions": [
            {"function": "republish", "args": {"topic": "t/1", "payload": "${payload}"}},
            {"function": "console"}
        ]
    });
    let (status, created) = server.post_auth("/api/v5/rules", body, &token).await;
    assert_eq!(status, 201, "create answered {created}");
    let actions = created["actions"].as_array().expect("actions list");
    assert_eq!(actions.len(), 2);
    assert_eq!(actions[0]["function"], json!("republish"));
    assert_eq!(actions[0]["args"]["topic"], json!("t/1"));
    assert_eq!(actions[0]["args"]["payload"], json!("${payload}"));
    // Defaults are filled in, as the specification documents them.
    assert_eq!(actions[0]["args"]["qos"], json!("${qos}"));
    assert_eq!(actions[0]["args"]["retain"], json!("${retain}"));
    assert_eq!(actions[0]["args"]["direct_dispatch"], json!(false));
    assert_eq!(actions[1], json!({"function": "console"}));

    // The read route returns the same items.
    let rule_id = created["id"].as_str().expect("rule id").to_string();
    let (status, fetched) = server
        .get_auth(&format!("/api/v5/rules/{rule_id}"), &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(fetched["actions"], created["actions"]);

    // A full update replaces them; fixed QoS and retain keep their types.
    let (status, updated) = server
        .put_auth(
            &format!("/api/v5/rules/{rule_id}"),
            json!({
                "sql": "SELECT * FROM \"test/topic\"",
                "actions": [{"function": "republish",
                             "args": {"topic": "out/${topic}", "qos": 1, "retain": true}}]
            }),
            &token,
        )
        .await;
    assert_eq!(status, 200, "update answered {updated}");
    assert_eq!(updated["actions"][0]["args"]["qos"], json!(1));
    assert_eq!(updated["actions"][0]["args"]["retain"], json!(true));
    assert_eq!(
        updated["actions"][0]["args"]["topic"],
        json!("out/${topic}")
    );
}

#[tokio::test]
async fn rules_builtin_functions_refuse_what_the_broker_cannot_do() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let sql = "SELECT * FROM \"t/#\"";

    // A user function needs user code, which this broker cannot load.
    let (status, body) = server
        .post_auth(
            "/api/v5/rules",
            json!({"name": "user-fn", "sql": sql,
                       "actions": [{"function": "my_module:my_function", "args": {}}]}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("not available in this broker"),
        "message was {body}"
    );

    // Arguments that break the documented types or ranges.
    for (name, args) in [
        ("no-topic", json!({})),
        ("wildcard", json!({"topic": "a/#"})),
        ("bad-qos", json!({"topic": "a", "qos": 3})),
        ("bad-retain", json!({"topic": "a", "retain": "sometimes"})),
        ("open-template", json!({"topic": "a/${b"})),
        ("unknown-arg", json!({"topic": "a", "color": "red"})),
        (
            "content-type",
            json!({"topic": "a", "mqtt_properties": {"Content-Type": "text/plain"}}),
        ),
    ] {
        let (status, body) = server
            .post_auth(
                "/api/v5/rules",
                json!({"name": name, "sql": sql,
                           "actions": [{"function": "republish", "args": args}]}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "{name} answered {body}");
        assert_eq!(body["code"], json!("BAD_REQUEST"));
    }
    // Nothing was stored.
    let (_, list) = server.get_auth("/api/v5/rules", &token).await;
    assert_eq!(list["data"].as_array().map(Vec::len), Some(0));

    // The empty forms that the management console sends are accepted.
    let (status, body) = server
        .post_auth(
            "/api/v5/rules",
            json!({"name": "console-defaults", "sql": sql,
                       "actions": [{"function": "republish",
                                    "args": {"topic": "a", "qos": 0, "retain": false,
                                             "payload": "${payload}", "mqtt_properties": {},
                                             "user_properties": "", "direct_dispatch": false}}]}),
            &token,
        )
        .await;
    assert_eq!(status, 201, "console defaults answered {body}");
}

#[tokio::test]
async fn rules_create_rejects_bad_filter() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Spec-literal invalid filter: `FROM "t/["` must be 400 (previously
    // silently coerced to `t/#`).
    let (status, body) = server
        .post_auth(
            "/api/v5/rules",
            json!({"name": "bad-filter",
                       "sql": "SELECT * FROM \"t/[\"",
                       "actions": []}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));

    // A misplaced `#` (mid-filter) is likewise rejected, not coerced.
    let (status, body) = server
        .post_auth(
            "/api/v5/rules",
            json!({"name": "bad-filter-hash",
                       "sql": "SELECT * FROM \"t/#/bogus\"",
                       "actions": []}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));

    // A create with only `sql` succeeds: the broker assigns an id
    // and `GET /rules/{id}` finds the rule.
    let (status, created) = server
        .post_auth(
            "/api/v5/rules",
            json!({"sql": "SELECT * FROM \"t/#\"", "actions": []}),
            &token,
        )
        .await;
    assert_eq!(status, 201, "sql-only create answered {created}");
    let id = created["id"].as_str().expect("created rule has id");
    assert_eq!(id.len(), 8, "assigned id has 8 chars, got {id:?}");
    assert!(
        id.chars().all(|c| c.is_ascii_hexdigit()),
        "assigned id is hex, got {id:?}"
    );
    let (status, fetched) = server
        .get_auth(&format!("/api/v5/rules/{id}"), &token)
        .await;
    assert_eq!(status, 200, "assigned id reads back");
    assert_eq!(fetched["id"], json!(id));
    assert_eq!(fetched["sql"], json!("SELECT * FROM \"t/#\""));
    let (status, _) = server
        .delete_auth(&format!("/api/v5/rules/{id}"), &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn rules_update_unknown_is_404() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, body) = server
        .put_auth("/api/v5/rules/nope", json!({"enable": false}), &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
}

#[tokio::test]
async fn rules_read_and_full_update_round_trip() {
    // W3-14 (C06-21/C06-22) failing test: create a rule, read it back
    // expecting the full fields, replace the whole body and read again
    // expecting the new values (not just an enable flip); an unknown id
    // reads and updates as 404, and a malformed update keeps the old
    // rule. Before: update applied only the enable flag. Broker-through:
    // every step goes through the HTTP router on the live engine, and
    // one publish goes through `dispatch_ingress` (the broker's rule
    // path) before and after the replace. Management-plane only
    // otherwise; publish and deliver never touch the rule map directly
    // (dispatch reads a snapshot).
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

    // Unknown read is 404 with the documented code, not an empty body.
    let (status, body) = server.get_auth("/api/v5/rules/w3-14-nope", &token).await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));

    // Unknown update creates nothing.
    let (status, body) = server
        .put_auth(
            "/api/v5/rules/w3-14-nope",
            json!({"sql": "SELECT * FROM \"w3/14/missing\"", "actions": []}),
            &token,
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));

    // Create the action first, then the rule referencing it.
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "w3-14-action", "type": "console", "server": "127.0.0.1:1883"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, created) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "id": "w3-14-rule",
                "name": "w3-14-rule",
                "sql": "SELECT * FROM \"w3/14/before\"",
                "actions": ["w3-14-action"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    // Read back expects the full documented fields.
    let (status, fetched) = server.get_auth("/api/v5/rules/w3-14-rule", &token).await;
    assert_eq!(status, 200);
    assert_eq!(fetched["id"], json!("w3-14-rule"));
    assert_eq!(fetched["name"], json!("w3-14-rule"));
    assert_eq!(fetched["sql"], json!("SELECT * FROM \"w3/14/before\""));
    assert_eq!(fetched["from"], json!(["w3/14/before"]));
    assert_eq!(fetched["enable"], json!(true));

    // Unknown query parameters are ignored, never an error.
    let (status, _) = server
        .get_auth("/api/v5/rules/w3-14-rule?unknown=param", &token)
        .await;
    assert_eq!(status, 200);
    let (status, _) = server
        .put_auth(
            "/api/v5/rules/w3-14-rule?unknown=param",
            json!({
                "id": "w3-14-rule",
                "name": "w3-14-rule-v2",
                "sql": "SELECT * FROM \"w3/14/after\"",
                "enable": false,
                "actions": [],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 200);

    // Full replace round trip: the new values read back, not just the
    // enable flag.
    let (status, fetched) = server.get_auth("/api/v5/rules/w3-14-rule", &token).await;
    assert_eq!(status, 200);
    assert_eq!(fetched["name"], json!("w3-14-rule-v2"));
    assert_eq!(fetched["sql"], json!("SELECT * FROM \"w3/14/after\""));
    assert_eq!(fetched["from"], json!(["w3/14/after"]));
    assert_eq!(fetched["enable"], json!(false));
    assert_eq!(fetched["actions"], json!([]));
    assert_eq!(created["id"], json!("w3-14-rule"));

    // Broker rule path: the replaced rule fires through
    // `dispatch_ingress` on its new topic, never `sink.send` directly.
    let counter = std::sync::Arc::new(CountingSink {
        hits: AtomicU64::new(0),
    });
    state.engine.connectors().register(
        broker_config::v5_action_sink_key("w3-14-action"),
        counter.clone(),
    );
    // Re-enable with the action attached so the fire is observable.
    let (status, _) = server
        .put_auth(
            "/api/v5/rules/w3-14-rule",
            json!({
                "id": "w3-14-rule",
                "name": "w3-14-rule-v3",
                "sql": "SELECT * FROM \"w3/14/after\"",
                "enable": true,
                "actions": ["w3-14-action"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
        std::sync::Arc::new(NoopBrokerSink);
    let topic = broker_protocol::Topic::new("w3/14/after").expect("valid topic");
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
        "replaced rule must fire through dispatch on its new topic"
    );

    // Malformed update is rejected and the old rule is kept.
    let (status, body) = server
        .put_auth(
            "/api/v5/rules/w3-14-rule",
            json!({"sql": "SELECT WHERE WHERE", "actions": []}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, fetched) = server.get_auth("/api/v5/rules/w3-14-rule", &token).await;
    assert_eq!(status, 200);
    assert_eq!(fetched["sql"], json!("SELECT * FROM \"w3/14/after\""));

    // Cleanup: the rule, then the action it referenced.
    let (status, _) = server.delete_auth("/api/v5/rules/w3-14-rule", &token).await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/actions/w3-14-action", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn rules_list_create_delete_round_trip() {
    // W3-13 (C06-18/C06-19/C06-20) failing test: create a rule, list
    // and expect it with paging metadata; duplicate create expects
    // rejection; delete then second delete expects 404. Before:
    // paging missing (bare envelope) and duplicate ids accepted.
    // Broker-through: the rule is created through the HTTP router on
    // the live engine, one publish goes through `dispatch_ingress`
    // (the broker's rule path) into a sink registered under the
    // referenced action id, and the delete goes through the router
    // too. Management-plane only otherwise; publish and deliver never
    // touch the rule map directly (dispatch reads a snapshot).
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

    // Empty engine returns an empty page, not an error.
    let (status, body) = server.get_auth("/api/v5/rules", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["data"], json!([]));
    assert_eq!(body["meta"]["page"], json!(1));
    assert_eq!(body["meta"]["limit"], json!(100));
    assert_eq!(body["meta"]["count"], json!(0));
    assert_eq!(body["meta"]["hasnext"], json!(false));

    // Unknown query parameters are ignored, never an error.
    let (status, _) = server.get_auth("/api/v5/rules?unknown=param", &token).await;
    assert_eq!(status, 200);

    // The forward target must name a known action (W3-02 linkage):
    // an unknown action id is rejected before anything is stored.
    let (status, body) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "id": "w3-13-rule",
                "sql": "SELECT * FROM \"w3/13/rules\"",
                "actions": ["w3-13-action"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));

    // Create the action first, then the rule referencing it.
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"name": "w3-13-action", "type": "console", "server": "127.0.0.1:1883"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, created) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "id": "w3-13-rule",
                "sql": "SELECT * FROM \"w3/13/rules\"",
                "actions": ["w3-13-action"],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["id"], json!("w3-13-rule"));

    // Malformed SQL is compiled before storing and rejected.
    let (status, body) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "id": "w3-13-bad-sql",
                "sql": "SELECT WHERE WHERE",
                "actions": [],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));

    // List carries the rule with the standard page envelope.
    let (status, body) = server.get_auth("/api/v5/rules", &token).await;
    assert_eq!(status, 200);
    let data = body["data"].as_array().expect("data is a list");
    assert_eq!(data.len(), 1);
    assert_eq!(data[0]["id"], json!("w3-13-rule"));
    assert_eq!(body["meta"]["count"], json!(1));
    assert_eq!(body["meta"]["hasnext"], json!(false));

    // Paging slices after filtering: page 2 of a 1-row store is empty.
    let (status, body) = server
        .get_auth("/api/v5/rules?page=2&limit=1", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["data"], json!([]));
    assert_eq!(body["meta"]["page"], json!(2));
    assert_eq!(body["meta"]["limit"], json!(1));
    assert_eq!(body["meta"]["count"], json!(1));
    assert_eq!(body["meta"]["hasnext"], json!(false));

    // Duplicate id is rejected cleanly, never overwritten.
    let (status, body) = server
        .post_auth(
            "/api/v5/rules",
            json!({
                "id": "w3-13-rule",
                "sql": "SELECT * FROM \"w3/13/rules\"",
                "actions": [],
            }),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));

    // Broker rule path: one publish through `dispatch_ingress` fires
    // the stored rule into the registered sink, never `sink.send`
    // directly.
    let counter = std::sync::Arc::new(CountingSink {
        hits: AtomicU64::new(0),
    });
    state.engine.connectors().register(
        broker_config::v5_action_sink_key("w3-13-action"),
        counter.clone(),
    );
    let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
        std::sync::Arc::new(NoopBrokerSink);
    let topic = broker_protocol::Topic::new("w3/13/rules").expect("valid topic");
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
        "stored rule must fire through dispatch"
    );

    // Delete hits with 204; a second delete is 404, not an empty body.
    let (status, _) = server.delete_auth("/api/v5/rules/w3-13-rule", &token).await;
    assert_eq!(status, 204);
    let (status, body) = server.get_auth("/api/v5/rules", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["data"], json!([]));
    let (status, body) = server.delete_auth("/api/v5/rules/w3-13-rule", &token).await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));

    // Cleanup: the action the rule referenced.
    let (status, _) = server
        .delete_auth("/api/v5/actions/w3-13-action", &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn rule_metrics_unknown_is_404_and_honest() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, body) = server
        .get_auth("/api/v5/rules/w0-13-no-such-rule/metrics", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));

    let (status, body) = server
        .send(
            "PUT",
            "/api/v5/rules/w0-13-no-such-rule/metrics/reset",
            Some(json!({})),
            Some(&token),
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));

    let (status, created) = server
        .post_auth(
            "/api/v5/rules",
            json!({"name": "w0-13-metrics",
                       "sql": "SELECT * FROM \"t/w013\"",
                       "enable": true,
                       "actions": []}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let rule_id = created["id"].as_str().expect("rule id").to_string();

    let (status, body) = server
        .get_auth(&format!("/api/v5/rules/{rule_id}/metrics"), &token)
        .await;
    assert_eq!(status, 200);
    let metrics = body["metrics"].as_object().expect("metrics object");
    let mut keys: Vec<&str> = metrics.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec![
            "actions.failed",
            "actions.success",
            "actions.total",
            "failed",
            "failed.no_result",
            "matched",
            "passed"
        ]
    );
    assert_eq!(body["node_metrics"].as_array().expect("node rows").len(), 1);
    assert!(metrics.get("rate").is_none());
    assert!(metrics.get("rate_max").is_none());
    assert!(metrics.get("rate_last5m").is_none());
}

#[tokio::test]
async fn rule_metrics_move_on_real_traffic_and_reset_zeroes() {
    // W3-15 (C06-23/C06-24) broker-through test: create a stored rule
    // via the HTTP router, publish a matching and a non-matching
    // message through the real evaluation path (`dispatch_ingress`,
    // the broker's rule path also used by `crates/broker-node` ingress
    // and the management publish path), read metrics and expect the
    // documented counters to move; reset and expect zeroes; unknown id
    // is 404. Before: constant metrics or missing 404.
    // The per-rule counters live in `broker-rules`
    // (`Rule::{matched,passed,failed}_cnt`, bumped in
    // `RuleEngine::dispatch_ingress` on the publish event), driven by
    // the publish event.
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
        .get_auth("/api/v5/rules/no-such-rule-metrics/metrics", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    let (status, body) = server
        .send(
            "PUT",
            "/api/v5/rules/no-such-rule-metrics/metrics/reset",
            Some(json!({})),
            Some(&token),
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("NOT_FOUND"));
    // Stored rule with a WHERE gate so matching and non-matching
    // traffic diverge; empty actions keep the fire side-effect-free
    // while `passed`/`failed` still move.
    let (status, created) = server
        .post_auth(
            "/api/v5/rules",
            json!({"name": "w3-15-metrics",
                       "sql": "SELECT * FROM \"w3/15/metrics\" WHERE k > 10",
                       "enable": true,
                       "actions": []}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let rule_id = created["id"].as_str().expect("rule id").to_string();
    // Fresh rule reads zeroes; unknown query keys are ignored.
    let (status, body) = server
        .get_auth(
            &format!("/api/v5/rules/{rule_id}/metrics?unknown=param"),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["metrics"]["matched"], json!(0));
    assert_eq!(body["metrics"]["passed"], json!(0));
    assert_eq!(body["metrics"]["failed"], json!(0));
    // One matching and two non-matching publishes through the real
    // evaluation path (WHERE-true, WHERE-false, non-JSON payload),
    // never constants and never `sink.send` directly.
    let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
        std::sync::Arc::new(NoopBrokerSink);
    let topic = broker_protocol::Topic::new("w3/15/metrics").expect("valid topic");
    state
        .engine
        .dispatch_ingress(
            &topic,
            &bytes::Bytes::from_static(b"{\"k\":20}"),
            broker_protocol::QoS::AtMostOnce,
            &broker_sink,
        )
        .await;
    state
        .engine
        .dispatch_ingress(
            &topic,
            &bytes::Bytes::from_static(b"{\"k\":1}"),
            broker_protocol::QoS::AtMostOnce,
            &broker_sink,
        )
        .await;
    state
        .engine
        .dispatch_ingress(
            &topic,
            &bytes::Bytes::from_static(b"not json"),
            broker_protocol::QoS::AtMostOnce,
            &broker_sink,
        )
        .await;
    // Counters moved only through real evaluation.
    let (status, body) = server
        .get_auth(&format!("/api/v5/rules/{rule_id}/metrics"), &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["metrics"]["matched"], json!(3));
    assert_eq!(body["metrics"]["passed"], json!(1));
    assert_eq!(body["metrics"]["failed"], json!(2));
    // Reset zeroes the counters and reports success; the next read is
    // zero. Reset is not paged; unknown query keys are ignored.
    let (status, _) = server
        .send(
            "PUT",
            &format!("/api/v5/rules/{rule_id}/metrics/reset?unknown=param"),
            Some(json!({})),
            Some(&token),
        )
        .await;
    assert_eq!(status, 204);
    let (status, body) = server
        .get_auth(&format!("/api/v5/rules/{rule_id}/metrics"), &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["metrics"]["matched"], json!(0));
    assert_eq!(body["metrics"]["passed"], json!(0));
    assert_eq!(body["metrics"]["failed"], json!(0));
    let (status, _) = server
        .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn test_rules_test_endpoint() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // SQL match with projection.
    let (status, body) = server
        .post_auth(
            "/api/v1/rules/test",
            json!({"sql_query": "SELECT temperature FROM \"sensors/+\" WHERE temperature > 0",
                       "topic_filter": "sensors/+",
                       "topic": "sensors/kitchen",
                       "payload": {"temperature": 72.5, "secret": "x"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["matched"], json!(true));
    assert_eq!(body["projected"], json!({"temperature": 72.5}));

    // Predicate false.
    let (status, body) = server
        .post_auth(
            "/api/v1/rules/test",
            json!({"sql_query": "SELECT * FROM \"sensors/+\" WHERE temperature > 100.0",
                       "topic": "sensors/kitchen",
                       "payload": {"temperature": 72.5}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["matched"], json!(false));

    // No SQL: passthrough.
    let (status, body) = server
        .post_auth(
            "/api/v1/rules/test",
            json!({"topic": "a/b", "payload": {"v": 1}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["matched"], json!(true));
    assert_eq!(body["projected"], json!({"v": 1}));

    // Broken SQL is a 400.
    let (status, _) = server
        .post_auth(
            "/api/v1/rules/test",
            json!({"sql_query": "SELECT WHERE WHERE",
                       "topic": "a/b",
                       "payload": {}}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
}

#[tokio::test]
async fn test_rules_test_endpoint_batch() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Array payloads aggregate as one batch: one row per group.
    let (status, body) = server
            .post_auth(
                "/api/v1/rules/test",
                json!({"sql_query": "SELECT sensor_id, avg(temperature) AS avg_temp FROM \"sensors/+\" GROUP BY sensor_id",
                       "topic": "sensors/kitchen",
                       "payload": [{"sensor_id": "a", "temperature": 10.0},
                                   {"sensor_id": "b", "temperature": 30.0},
                                   {"sensor_id": "a", "temperature": 20.0}]}),
                &token,
            )
            .await;
    assert_eq!(status, 200);
    assert_eq!(body["matched"], json!(true));
    let rows = body["projected"].as_array().expect("projected rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["sensor_id"], json!("a"));
    assert_eq!(rows[0]["avg_temp"], json!(15.0));
    assert_eq!(rows[1]["sensor_id"], json!("b"));
    assert_eq!(rows[1]["avg_temp"], json!(30.0));

    // Empty batches match nothing.
    let (status, body) = server
        .post_auth(
            "/api/v1/rules/test",
            json!({"sql_query": "SELECT avg(temperature) AS a FROM \"sensors/+\"",
                       "topic": "sensors/kitchen",
                       "payload": []}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["matched"], json!(false));
    assert_eq!(body["projected"], serde_json::Value::Null);
}

#[tokio::test]
async fn test_rules_functions_catalog() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, body) = server.get_auth("/api/v1/rules/functions", &token).await;
    assert_eq!(status, 200);
    let catalog = body.as_array().expect("function array");
    // The engine revision in the lock file lists 219 functions.
    assert_eq!(catalog.len(), 219);
    let sin = catalog
        .iter()
        .find(|entry| entry["name"] == json!("sin"))
        .expect("sin in catalog");
    assert_eq!(sin["category"], json!("math"));
    assert_eq!(sin["aggregate"], json!(false));
    let avg = catalog
        .iter()
        .find(|entry| entry["name"] == json!("avg"))
        .expect("avg in catalog");
    assert_eq!(avg["aggregate"], json!(true));
}
