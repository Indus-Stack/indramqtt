use super::*;

#[test]
fn canonical_action_kind_maps_every_mv02c_alias() {
    assert_eq!(canonical_action_kind("mongo"), "mongodb");
    assert_eq!(canonical_action_kind("documentdb"), "mongodb");
    assert_eq!(canonical_action_kind("couch"), "couchbase");
    assert_eq!(canonical_action_kind("cb"), "couchbase");
    assert_eq!(canonical_action_kind("elastic"), "elasticsearch");
    assert_eq!(canonical_action_kind("es"), "elasticsearch");
    assert_eq!(canonical_action_kind("opensearch"), "elasticsearch");
}

#[test]
fn action_owned_kind_covers_the_mv02c_kinds() {
    for kind in [
        "mongodb",
        "mongo",
        "documentdb",
        "couchbase",
        "couch",
        "cb",
        "elasticsearch",
        "elastic",
        "es",
        "opensearch",
    ] {
        assert!(action_owned_kind(kind), "{kind} owns a runtime");
    }
}

#[cfg(feature = "mongodb")]
#[test]
fn mongodb_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let rec = mv02b_record(
        "act",
        "mongodb",
        "mongodb:up",
        serde_json::json!({"collection": "events"}),
    );
    let error = build_mongodb_sink(&rec, &serde_json::json!({"database": "db"}))
        .err()
        .expect("a missing server refuses");
    assert!(error.contains("server"), "{error}");
    let error = build_mongodb_sink(&rec, &serde_json::json!({"server": "h:27017"}))
        .err()
        .expect("a missing database refuses");
    assert!(error.contains("database"), "{error}");
    let error = build_mongodb_sink(
        &rec,
        &serde_json::json!({"server": "h:27017", "database": "db", "username": "u"}),
    )
    .err()
    .expect("a half credential pair refuses");
    assert!(error.contains("together"), "{error}");
    let sink = build_mongodb_sink(
        &rec,
        &serde_json::json!({"server": "h:27017", "database": "db"}),
    )
    .expect("a full mongodb pair builds");
    assert_eq!(sink.kind(), "mongodb");
    let cfg = mongodb_sink_config(
        &rec,
        &serde_json::json!({"server": "h:27017", "database": "db"}),
    )
    .expect("a full mongodb config builds");
    assert_eq!(cfg.collection_template, "events");
    assert_eq!(cfg.max_retries, Some(4));
    let bare = mv02b_record("act", "mongo", "mongodb:up", serde_json::json!({}));
    let sink = build_mongodb_sink(
        &bare,
        &serde_json::json!({"server": "h:27017", "database": "db"}),
    )
    .expect("the documented collection default builds");
    assert_eq!(sink.kind(), "mongodb");
    let cfg = mongodb_sink_config(
        &bare,
        &serde_json::json!({"server": "h:27017", "database": "db"}),
    )
    .expect("the documented collection default config builds");
    assert_eq!(cfg.collection_template, "mqtt");
    let bad = mv02b_record(
        "act",
        "documentdb",
        "mongodb:up",
        serde_json::json!({"payload_template": "not json ${topic}"}),
    );
    let error = build_mongodb_sink(
        &bad,
        &serde_json::json!({"server": "h:27017", "database": "db"}),
    )
    .err()
    .expect("a non-JSON document template refuses");
    assert!(error.contains("payload_template"), "{error}");
}

#[cfg(feature = "couchbase")]
#[test]
fn couchbase_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let rec = mv02b_record(
        "act",
        "couchbase",
        "couchbase:up",
        serde_json::json!({"sql": "UPSERT INTO default:b.s.c (KEY, VALUE) VALUES (\"${client_id}\", ${payload})"}),
    );
    let error = build_couchbase_sink(&rec, &serde_json::json!({"username": "u", "password": "p"}))
        .err()
        .expect("a missing server refuses");
    assert!(error.contains("server"), "{error}");
    let error = build_couchbase_sink(&rec, &serde_json::json!({"server": "h:11210"}))
        .err()
        .expect("a missing username refuses");
    assert!(error.contains("username"), "{error}");
    let no_sql = mv02b_record("act", "couch", "couchbase:up", serde_json::json!({}));
    let error = build_couchbase_sink(
        &no_sql,
        &serde_json::json!({"server": "h:11210", "username": "u"}),
    )
    .err()
    .expect("a missing sql refuses");
    assert!(error.contains("sql"), "{error}");
    let sink = build_couchbase_sink(
        &rec,
        &serde_json::json!({"server": "h:11210", "username": "u", "password": "p"}),
    )
    .expect("a full couchbase pair builds");
    assert_eq!(sink.kind(), "couchbase");
    let cfg = couchbase_sink_config(
        &rec,
        &serde_json::json!({"server": "h:11210", "username": "u", "password": "p"}),
    )
    .expect("a full couchbase config builds");
    assert_eq!(cfg.max_retries, Some(3));
}

#[cfg(feature = "elasticsearch")]
#[test]
fn elasticsearch_sink_uses_the_action_target_and_refuses_the_missing_fields() {
    let rec = mv02b_record(
        "act",
        "elasticsearch",
        "elasticsearch:up",
        serde_json::json!({"index": "events", "id": "${topic}"}),
    );
    let error = build_elasticsearch_sink(&rec, &serde_json::json!({"server": "127.0.0.1:9200"}))
        .err()
        .expect("a missing action refuses");
    assert!(error.contains("action"), "{error}");
    let no_index = mv02b_record(
        "act",
        "es",
        "elasticsearch:up",
        serde_json::json!({"action": "create"}),
    );
    let error =
        build_elasticsearch_sink(&no_index, &serde_json::json!({"server": "127.0.0.1:9200"}))
            .err()
            .expect("a missing index refuses");
    assert!(error.contains("index"), "{error}");
    let no_id = mv02b_record(
        "act",
        "opensearch",
        "elasticsearch:up",
        serde_json::json!({"action": "update", "index": "events"}),
    );
    let error = build_elasticsearch_sink(&no_id, &serde_json::json!({"server": "127.0.0.1:9200"}))
        .err()
        .expect("an update without an id refuses");
    assert!(error.contains("id"), "{error}");
    let sink = build_elasticsearch_sink(
        &mv02b_record(
            "act",
            "elasticsearch",
            "elasticsearch:up",
            serde_json::json!({"action": "create", "index": "events", "id": "${topic}"}),
        ),
        &serde_json::json!({"server": "127.0.0.1:9200"}),
    )
    .expect("a full elasticsearch pair builds");
    assert_eq!(sink.kind(), "elasticsearch");
    let cfg = elasticsearch_sink_config(
        &mv02b_record(
            "act",
            "elasticsearch",
            "elasticsearch:up",
            serde_json::json!({"action": "create", "index": "events", "id": "${topic}"}),
        ),
        &serde_json::json!({"server": "127.0.0.1:9200"}),
    )
    .expect("a full elasticsearch config builds");
    assert!(cfg.overwrite);
    assert_eq!(
        cfg.operation,
        broker_connectors::elasticsearch::ElasticsearchOperation::Index
    );
    assert_eq!(cfg.max_retries, 2);
    let bad = mv02b_record(
        "act",
        "elastic",
        "elasticsearch:up",
        serde_json::json!({"action": "create", "index": "events", "doc": "not json ${topic}"}),
    );
    let error = build_elasticsearch_sink(&bad, &serde_json::json!({"server": "127.0.0.1:9200"}))
        .err()
        .expect("a non-JSON document template refuses");
    assert!(error.contains("doc_template"), "{error}");
}
