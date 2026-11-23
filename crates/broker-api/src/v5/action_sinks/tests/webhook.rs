use super::*;

fn webhook_record(id: &str, connector: &str, parameters: serde_json::Value) -> ActionRecord {
    ActionRecord {
        parameters,
        ..record(id, "webhook", connector)
    }
}

#[tokio::test]
async fn stage_non_owned_kind_wires_nothing() {
    let connectors = ConnectorStore::new();
    let stage = stage_action_sink(&connectors, &record("act", "no-such-kind", "no-such:up"))
        .expect("staging a kind with no runtime stages nothing");
    assert!(stage.sink.is_none());
    assert_eq!(stage.wiring, ActionWiring::None);
}

#[tokio::test]
async fn stage_missing_connector_field_refuses_the_write() {
    let connectors = ConnectorStore::new();
    let error = stage_action_sink(&connectors, &record("act", "webhook", ""))
        .err()
        .expect("an owned kind without a connector is refused");
    assert!(
        error.contains("act") && error.contains("connector"),
        "{error}"
    );
}

#[tokio::test]
async fn stage_dangling_connector_stores_disconnected_without_dialling() {
    let connectors = ConnectorStore::new();
    let stage = stage_action_sink(&connectors, &record("act", "webhook", "webhook:gone-away"))
        .expect("a dangling reference is not a build failure");
    assert!(stage.sink.is_none());
    let ActionWiring::Failed(reason) = stage.wiring else {
        panic!("a dangling reference reports failed wiring");
    };
    assert!(reason.contains("gone-away"), "{reason}");
    assert!(reason.contains("connector"), "{reason}");
}

#[tokio::test]
async fn stage_resolved_connector_builds_the_sink_live() {
    let connectors = ConnectorStore::new();
    connectors.upsert(serde_json::json!({
        "id": "webhook:up",
        "name": "up",
        "type": "webhook",
        "url": "http://127.0.0.1:9",
    }));
    let stage = stage_action_sink(
        &connectors,
        &webhook_record("act", "webhook:up", serde_json::json!({"path": "/p"})),
    )
    .expect("a resolvable webhook connector builds");
    assert_eq!(stage.wiring, ActionWiring::Live);
    let sink = stage.sink.expect("live wiring carries the sink");
    assert_eq!(sink.kind(), "webhook");
}

#[tokio::test]
async fn targets_connector_matches_every_reference_field() {
    let connectors = ConnectorStore::new();
    assert!(!action_targets_connector(
        &connectors,
        &record("act", "http", "webhook:up"),
        "up"
    ));
    connectors.upsert(serde_json::json!({
        "id": "webhook:up",
        "name": "up",
        "type": "webhook",
        "url": "http://127.0.0.1:9",
    }));
    assert!(action_targets_connector(
        &connectors,
        &record("act", "http", "webhook:up"),
        "up"
    ));
    assert!(action_targets_connector(
        &connectors,
        &record("act", "http", "up"),
        "up"
    ));
    // The legacy flat extras count as references too.
    let mut legacy = record("act", "http", "");
    legacy.params = serde_json::json!({"connector_id": "webhook:up"});
    assert!(action_targets_connector(&connectors, &legacy, "up"));
}

#[tokio::test]
async fn webhook_sink_joins_the_action_path_and_refuses_other_methods() {
    let connectors = ConnectorStore::new();
    connectors.upsert(serde_json::json!({
        "id": "webhook:up",
        "name": "up",
        "type": "webhook",
        "url": "http://127.0.0.1:9/base",
    }));
    // An explicit non-post method refuses with the shape named.
    let error = build_action_sink(
        &webhook_record(
            "act",
            "webhook:up",
            serde_json::json!({"path": "/p", "method": "get"}),
        ),
        &connectors.get("up").expect("connector stored"),
        &ActionResourceOpts::default(),
    )
    .err()
    .expect("a non-post method refuses");
    assert!(error.contains("act") && error.contains("get"), "{error}");
    let sink = build_action_sink(
        &webhook_record("act", "webhook:up", serde_json::json!({"path": "/p"})),
        &connectors.get("up").expect("connector stored"),
        &ActionResourceOpts::default(),
    )
    .expect("a plain path builds")
    .expect("an owned kind builds a sink");
    assert_eq!(sink.kind(), "webhook");
    // A POST renders the joined path through the real connector builder.
    let rendered = stage_action_sink(
        &connectors,
        &webhook_record("act", "webhook:up", serde_json::json!({"path": "/p"})),
    )
    .expect("stage builds");
    assert!(rendered.sink.is_some());
}

#[tokio::test]
async fn webhook_sink_fails_closed_on_the_invalid_headers() {
    let connectors = ConnectorStore::new();
    connectors.upsert(serde_json::json!({
        "id": "webhook:up",
        "name": "up",
        "type": "webhook",
        "url": "http://127.0.0.1:9",
    }));
    let doc = connectors.get("up").expect("connector stored");
    let error = build_action_sink(
        &webhook_record(
            "act",
            "webhook:up",
            serde_json::json!({"headers": {"good header": 1}}),
        ),
        &doc,
        &ActionResourceOpts::default(),
    )
    .err()
    .expect("a non-string header value refuses");
    assert!(error.contains("headers"), "{error}");
    let error = build_action_sink(
        &webhook_record(
            "act",
            "webhook:up",
            serde_json::json!({"headers": {"bad name": "x"}}),
        ),
        &doc,
        &ActionResourceOpts::default(),
    )
    .err()
    .expect("an invalid header name refuses");
    assert!(error.contains("header name"), "{error}");
    // A legacy flat `headers` extra covers the same field too.
    let mut legacy = record("act", "http", "webhook:up");
    legacy.params = serde_json::json!({"headers": {"X-Mv01": "v"}, "path": "/legacy"});
    assert!(
        build_action_sink(&legacy, &doc, &ActionResourceOpts::default()).is_ok(),
        "string headers build"
    );
}
