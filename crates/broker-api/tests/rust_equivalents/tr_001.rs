use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use broker_api::v5::api_keys::{KeyRole, NewApiKey};
use broker_api::ApiState;
use broker_connectors::Sink;
use reqwest::StatusCode;
use serde_json::json;

/// Start the management API on a loopback socket.
///
/// Sets up the server with `ApiState::standalone`, binds a loopback
/// listener, and serves requests with Axum. Returns the base URL and
/// the state.
async fn start_server() -> (String, ApiState) {
    let engine = Arc::new(broker_rules::RuleEngine::new(
        16,
        broker_rules::BackpressurePolicy::DropOldest,
    ));
    let state = ApiState::standalone(engine);
    let app = broker_api::router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let port = listener.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve test app");
    });
    (format!("http://127.0.0.1:{port}"), state)
}

/// Mint an administrator API key for test requests.
///
/// Returns the secret key string for Bearer authentication.
fn mint_admin_key(state: &ApiState) -> String {
    let mut admin_new = NewApiKey::new("test-admin-key");
    admin_new.role = KeyRole::Administrator;
    let (_, admin_secret) = state.api_keys.create(admin_new).expect("mint admin key");
    admin_secret
}

/// Prove that an action can be created and deleted through the management API.
#[tokio::test]
async fn tr_001_create_remove() {
    let (base_url, state) = start_server().await;
    let token = mint_admin_key(&state);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("build client");

    let action_payload = json!({
        "name": "tr-001-action",
        "type": "console"
    });

    let create_res = client
        .post(format!("{base_url}/api/v5/actions"))
        .bearer_auth(&token)
        .json(&action_payload)
        .send()
        .await
        .expect("send create action");

    assert_eq!(create_res.status(), StatusCode::CREATED);
    let created: serde_json::Value = create_res.json().await.expect("parse json");
    assert_eq!(created["id"], "tr-001-action");
    assert_eq!(created["type"], "console");

    let del_res = client
        .delete(format!("{base_url}/api/v5/actions/tr-001-action"))
        .bearer_auth(&token)
        .send()
        .await
        .expect("send delete action");

    assert_eq!(del_res.status(), StatusCode::NO_CONTENT);

    let second_del = client
        .delete(format!("{base_url}/api/v5/actions/tr-001-action"))
        .bearer_auth(&token)
        .send()
        .await
        .expect("send second delete action");

    assert_eq!(second_del.status(), StatusCode::NOT_FOUND);
    let err_body: serde_json::Value = second_del.json().await.expect("parse json");
    assert_eq!(err_body["code"], "NOT_FOUND");
}

/// Prove that probing an action sink invokes its probe and records error status.
#[tokio::test]
async fn tr_001_manual_health_check_error() {
    struct TestProbeSink {
        probed: AtomicBool,
    }

    #[async_trait::async_trait]
    impl broker_connectors::Sink for TestProbeSink {
        async fn send(
            &self,
            _topic: &broker_protocol::Topic,
            _payload: &bytes::Bytes,
            _qos: broker_protocol::QoS,
        ) -> broker_connectors::Result<()> {
            Ok(())
        }

        async fn probe(&self) -> Result<(), String> {
            self.probed.store(true, Ordering::Relaxed);
            Err("target connection refused".to_string())
        }

        fn kind(&self) -> &'static str {
            "console"
        }

        fn health(&self) -> Option<broker_connectors::SinkHealth> {
            if self.probed.load(Ordering::Relaxed) {
                Some(broker_connectors::SinkHealth {
                    healthy: false,
                    reason: Some("target connection refused".to_string()),
                })
            } else {
                Some(broker_connectors::SinkHealth {
                    healthy: true,
                    reason: None,
                })
            }
        }
    }

    let engine = broker_rules::RuleEngine::new(16, broker_rules::BackpressurePolicy::DropOldest);
    let actions = broker_api::v5::actions::ActionStore::new();
    let action = broker_api::v5::actions::ActionRecord {
        id: "tr001_probe_err".to_string(),
        name: "tr001_probe_err".to_string(),
        action_type: "console".to_string(),
        enable: true,
        connector: "".to_string(),
        parameters: serde_json::Value::Null,
        resource_opts: serde_json::Value::Null,
        params: serde_json::Value::Null,
    };
    actions.insert(action).expect("insert action");
    let key = broker_config::v5_action_sink_key("tr001_probe_err");
    let sink = Arc::new(TestProbeSink {
        probed: AtomicBool::new(false),
    });
    engine.connectors().register(&key, sink.clone());

    let initial_health = sink.health().expect("initial health");
    assert!(initial_health.healthy);

    broker_api::v5::action_sinks::probe_action_sinks(&engine, &actions, |_| true).await;

    let health = sink.health().expect("health reported");
    assert!(!health.healthy);
    assert_eq!(health.reason.as_deref(), Some("target connection refused"));
}

/// Prove that deleting a connector referenced by an action is rejected.
#[tokio::test]
async fn tr_001_remove_single_connector_being_referenced_with_active_channels() {
    let (base_url, state) = start_server().await;
    let token = mint_admin_key(&state);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("build client");

    let connector_payload = json!({
        "name": "tr-001-conn",
        "type": "pgsql"
    });

    let conn_res = client
        .post(format!("{base_url}/api/v5/connectors"))
        .bearer_auth(&token)
        .json(&connector_payload)
        .send()
        .await
        .expect("send create connector");

    assert_eq!(conn_res.status(), StatusCode::CREATED);

    let action_payload = json!({
        "name": "tr-001-act",
        "type": "console",
        "server": "127.0.0.1:1883",
        "connector": "tr-001-conn"
    });

    let act_res = client
        .post(format!("{base_url}/api/v5/actions"))
        .bearer_auth(&token)
        .json(&action_payload)
        .send()
        .await
        .expect("send create action");

    assert_eq!(act_res.status(), StatusCode::CREATED);

    let del_res = client
        .delete(format!("{base_url}/api/v5/connectors/tr-001-conn"))
        .bearer_auth(&token)
        .send()
        .await
        .expect("send delete connector");

    assert_eq!(del_res.status(), StatusCode::BAD_REQUEST);
    let err_body: serde_json::Value = del_res.json().await.expect("parse json");
    assert_eq!(err_body["code"], "BAD_REQUEST");
    let msg = err_body["message"].as_str().unwrap_or("");
    assert!(msg.contains("referenced by action"));

    let del_act = client
        .delete(format!("{base_url}/api/v5/actions/tr-001-act"))
        .bearer_auth(&token)
        .send()
        .await
        .expect("delete action");
    assert_eq!(del_act.status(), StatusCode::NO_CONTENT);

    let del_conn_after = client
        .delete(format!("{base_url}/api/v5/connectors/tr-001-conn"))
        .bearer_auth(&token)
        .send()
        .await
        .expect("delete connector after action");
    assert_eq!(del_conn_after.status(), StatusCode::NO_CONTENT);
}
