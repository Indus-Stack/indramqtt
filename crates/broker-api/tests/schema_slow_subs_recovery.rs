//! Integration tests for schema registry and slow subscription lock recovery.
//!
//! Verifies that poisoned locks recover when accessed via HTTP API routes
//! or direct store methods, preventing denial of service.

use std::sync::Arc;
use std::time::Duration;

use broker_api::v5::api_keys::{KeyRole, NewApiKey};
use broker_api::v5::schema_registry::SchemaRegistryStore;
use broker_api::ApiState;
use serde_json::json;

async fn start_server() -> (String, String, ApiState) {
    let engine = Arc::new(broker_rules::RuleEngine::new(
        16,
        broker_rules::BackpressurePolicy::DropOldest,
    ));
    let state = ApiState::standalone(engine);
    state.schema_registry.seed_from_registry(&state.config);

    let mut admin_new = NewApiKey::new("test-admin");
    admin_new.role = KeyRole::Administrator;
    let (_, admin_secret) = state.api_keys.create(admin_new).expect("mint admin key");

    let app = broker_api::router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let port = listener.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve test app");
    });

    (format!("http://127.0.0.1:{port}"), admin_secret, state)
}

#[tokio::test]
async fn schema_registry_recovers_after_lock_poisoning_http_flow() {
    let (base, admin_secret, state) = start_server().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("client builds");

    // Poison the schema registry inner write lock.
    let store = Arc::clone(&state.schema_registry);
    let poison_result = tokio::task::spawn_blocking(move || {
        store.poison_for_test();
    })
    .await;
    assert!(poison_result.is_err(), "the poison thread must panic");

    // 1. GET /api/v5/schema_registry recovers the poisoned lock.
    let resp = client
        .get(format!("{base}/api/v5/schema_registry"))
        .bearer_auth(&admin_secret)
        .send()
        .await
        .expect("send get schema registry");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let schemas: serde_json::Value = resp.json().await.expect("json schema list");
    assert!(schemas.is_array(), "response must be schema array");

    // 2. POST /api/v5/schema_registry recovers the lock and persists schema.
    let new_schema = json!({
        "name": "test_schema_json",
        "type": "json",
        "description": "sensor payload schema",
        "source": "{\"type\": \"object\", \"properties\": {\"temperature\": {\"type\": \"number\"}}}"
    });
    let resp = client
        .post(format!("{base}/api/v5/schema_registry"))
        .bearer_auth(&admin_secret)
        .json(&new_schema)
        .send()
        .await
        .expect("send post schema registry");
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let created: serde_json::Value = resp.json().await.expect("json created schema");
    assert_eq!(created["name"], json!("test_schema_json"));

    // Verify persistence to configuration registry.
    assert!(
        state
            .config
            .snapshot()
            .schemas
            .schemas
            .iter()
            .any(|s| s.name == "test_schema_json"),
        "created schema must persist to config registry"
    );

    // 3. GET /api/v5/schema_registry/{name} recovers the lock.
    let resp = client
        .get(format!("{base}/api/v5/schema_registry/test_schema_json"))
        .bearer_auth(&admin_secret)
        .send()
        .await
        .expect("send get schema by name");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let fetched: serde_json::Value = resp.json().await.expect("json fetched schema");
    assert_eq!(fetched["name"], json!("test_schema_json"));

    // 4. DELETE /api/v5/schema_registry/{name} recovers the lock and deletes schema.
    let resp = client
        .delete(format!("{base}/api/v5/schema_registry/test_schema_json"))
        .bearer_auth(&admin_secret)
        .send()
        .await
        .expect("send delete schema");
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);

    // Verify schema is removed from config storage as well.
    assert!(
        !state
            .config
            .snapshot()
            .schemas
            .schemas
            .iter()
            .any(|s| s.name == "test_schema_json"),
        "deleted schema must be removed from config registry"
    );
}

#[tokio::test]
async fn schema_registry_seeding_recovers_after_lock_poisoning() {
    let (base, admin_secret, state) = start_server().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("client builds");

    // Poison the schema registry inner write lock.
    let store = Arc::clone(&state.schema_registry);
    let poison_result = tokio::task::spawn_blocking(move || {
        store.poison_for_test();
    })
    .await;
    assert!(poison_result.is_err(), "the poison thread must panic");

    // Commit a schema directly to config registry.
    let entry = broker_config::SchemaEntry {
        name: "seed_schema_item".to_string(),
        schema_type: "json".to_string(),
        description: "seed item description".to_string(),
        schema: "{\"type\": \"object\"}".to_string(),
    };
    let conf = broker_config::SchemasConf {
        schemas: vec![entry],
    };
    state
        .config
        .commit_schemas(conf)
        .expect("commit schemas to config");

    // Seed schemas via seed_from_registry on the poisoned store.
    state.schema_registry.seed_from_registry(&state.config);

    // Verify GET /api/v5/schema_registry reflects the seeded schema.
    let resp = client
        .get(format!("{base}/api/v5/schema_registry"))
        .bearer_auth(&admin_secret)
        .send()
        .await
        .expect("send get schemas");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let schemas: serde_json::Value = resp.json().await.expect("json schema list");
    let names: Vec<&str> = schemas
        .as_array()
        .expect("array")
        .iter()
        .filter_map(|s| s["name"].as_str())
        .collect();
    assert!(
        names.contains(&"seed_schema_item"),
        "seeded schema must appear in list"
    );

    // Test from_registry constructor after configuration seeding.
    let fresh_store = SchemaRegistryStore::from_registry(&state.config);
    let snapshot = fresh_store.snapshot();
    assert!(
        snapshot.iter().any(|s| s.name == "seed_schema_item"),
        "from_registry must seed the schema snapshot"
    );
}

#[tokio::test]
async fn slow_subscriptions_store_recovers_after_lock_poisoning() {
    let (base, admin_secret, state) = start_server().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("client builds");

    // Poison the slow subscriptions store write lock.
    let store = Arc::clone(&state.slow_subs);
    let poison_result = tokio::task::spawn_blocking(move || {
        store.poison_for_test();
    })
    .await;
    assert!(poison_result.is_err(), "the poison thread must panic");

    // 1. GET /api/v5/slow_subscriptions recovers poisoned lock.
    let resp = client
        .get(format!("{base}/api/v5/slow_subscriptions"))
        .bearer_auth(&admin_secret)
        .send()
        .await
        .expect("send get slow subs");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("json slow subs page");
    assert!(body["data"].is_array(), "data field must be an array");

    // 2. record after lock poisoning records slow delivery.
    state
        .slow_subs
        .record("client-slow-1", "topic/alpha", 750, 1_700_000_100_000);

    let resp = client
        .get(format!("{base}/api/v5/slow_subscriptions"))
        .bearer_auth(&admin_secret)
        .send()
        .await
        .expect("send get slow subs");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("json slow subs page");
    let rows = body["data"].as_array().expect("data array");
    assert!(
        rows.iter()
            .any(|r| r["clientid"] == "client-slow-1" && r["timespan"] == 750),
        "recorded slow delivery must be present"
    );

    // 3. remove_expired after lock poisoning drops expired entries.
    let store = Arc::clone(&state.slow_subs);
    let poison_result = tokio::task::spawn_blocking(move || {
        store.poison_for_test();
    })
    .await;
    assert!(poison_result.is_err(), "poison thread must panic");

    // Entry at timestamp 1_700_000_100_000 with now_ms 1_700_000_200_000 is 100_000 ms old.
    // Expire window 50_000 ms drops it.
    state.slow_subs.remove_expired(1_700_000_200_000, 50_000);

    let resp = client
        .get(format!("{base}/api/v5/slow_subscriptions"))
        .bearer_auth(&admin_secret)
        .send()
        .await
        .expect("send get slow subs");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("json slow subs page");
    let rows = body["data"].as_array().expect("data array");
    assert!(
        rows.is_empty(),
        "expired record must be dropped by remove_expired"
    );

    // 4. truncate_to after lock poisoning caps retained entries.
    state
        .slow_subs
        .record("client-slow-a", "topic/a", 200, 1_700_000_000_000);
    state
        .slow_subs
        .record("client-slow-b", "topic/b", 400, 1_700_000_000_000);
    state
        .slow_subs
        .record("client-slow-c", "topic/c", 600, 1_700_000_000_000);

    let store = Arc::clone(&state.slow_subs);
    let poison_result = tokio::task::spawn_blocking(move || {
        store.poison_for_test();
    })
    .await;
    assert!(poison_result.is_err(), "poison thread must panic");

    state.slow_subs.truncate_to(2);

    let resp = client
        .get(format!("{base}/api/v5/slow_subscriptions"))
        .bearer_auth(&admin_secret)
        .send()
        .await
        .expect("send get slow subs");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("json slow subs page");
    let rows = body["data"].as_array().expect("data array");
    assert_eq!(rows.len(), 2, "truncate_to must cap retained entries to 2");

    // 5. DELETE /api/v5/slow_subscriptions recovers the lock and clears entries.
    let resp = client
        .delete(format!("{base}/api/v5/slow_subscriptions"))
        .bearer_auth(&admin_secret)
        .send()
        .await
        .expect("send delete slow subs");
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);

    let resp = client
        .get(format!("{base}/api/v5/slow_subscriptions"))
        .bearer_auth(&admin_secret)
        .send()
        .await
        .expect("send get slow subs");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("json slow subs page");
    assert_eq!(
        body["data"].as_array().expect("data array").len(),
        0,
        "store must be empty after clear"
    );
}

#[tokio::test]
async fn slow_subscriptions_settings_recovers_after_lock_poisoning() {
    let (base, admin_secret, state) = start_server().await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("client builds");

    // Poison the slow subscriptions settings write lock.
    let store = Arc::clone(&state.slow_subs_settings);
    let poison_result = tokio::task::spawn_blocking(move || {
        store.poison_for_test();
    })
    .await;
    assert!(poison_result.is_err(), "the poison thread must panic");

    // 1. GET /api/v5/slow_subscriptions/settings recovers poisoned lock.
    let resp = client
        .get(format!("{base}/api/v5/slow_subscriptions/settings"))
        .bearer_auth(&admin_secret)
        .send()
        .await
        .expect("send get slow subs settings");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let settings: serde_json::Value = resp.json().await.expect("json settings");
    assert_eq!(settings["enable"], json!(true));

    // 2. PUT /api/v5/slow_subscriptions/settings replaces settings through recovered lock.
    let update = json!({
        "enable": true,
        "threshold": "2s",
        "top_k_num": 50,
        "expire_interval": "10m",
        "stats_type": "whole"
    });
    let resp = client
        .put(format!("{base}/api/v5/slow_subscriptions/settings"))
        .bearer_auth(&admin_secret)
        .json(&update)
        .send()
        .await
        .expect("send put slow subs settings");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let updated: serde_json::Value = resp.json().await.expect("json updated settings");
    assert_eq!(updated["threshold"], json!("2s"));
    assert_eq!(updated["top_k_num"], json!(50));

    // 3. set_enabled after lock poisoning mutates enable flag.
    let store = Arc::clone(&state.slow_subs_settings);
    let poison_result = tokio::task::spawn_blocking(move || {
        store.poison_for_test();
    })
    .await;
    assert!(poison_result.is_err(), "the poison thread must panic");

    state.slow_subs_settings.set_enabled(false);

    let resp = client
        .get(format!("{base}/api/v5/slow_subscriptions/settings"))
        .bearer_auth(&admin_secret)
        .send()
        .await
        .expect("send get slow subs settings");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let settings: serde_json::Value = resp.json().await.expect("json settings");
    assert_eq!(settings["enable"], json!(false));

    // 4. set_threshold after lock poisoning mutates threshold.
    let store = Arc::clone(&state.slow_subs_settings);
    let poison_result = tokio::task::spawn_blocking(move || {
        store.poison_for_test();
    })
    .await;
    assert!(poison_result.is_err(), "the poison thread must panic");

    assert!(state.slow_subs_settings.set_threshold("1500ms"));

    let resp = client
        .get(format!("{base}/api/v5/slow_subscriptions/settings"))
        .bearer_auth(&admin_secret)
        .send()
        .await
        .expect("send get slow subs settings");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let settings: serde_json::Value = resp.json().await.expect("json settings");
    assert_eq!(settings["threshold"], json!("1500ms"));
}
