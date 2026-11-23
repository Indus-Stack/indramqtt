use super::*;

#[test]
fn canonical_action_kind_maps_every_mv02b_alias() {
    assert_eq!(canonical_action_kind("aws_timestream"), "timestream");
    assert_eq!(canonical_action_kind("opents"), "opentsdb");
    assert_eq!(canonical_action_kind("scylla"), "cassandra");
    assert_eq!(canonical_action_kind("scylladb"), "cassandra");
    assert_eq!(canonical_action_kind("cql"), "cassandra");
    assert_eq!(canonical_action_kind("greptime"), "greptimedb");
    assert_eq!(canonical_action_kind("ch"), "clickhouse");
    assert_eq!(canonical_action_kind("timestream"), "timestream");
    assert_eq!(canonical_action_kind("mqtt_bridge"), "mqtt_bridge");
}

#[test]
fn action_owned_kind_covers_the_mv02b_kinds() {
    for kind in [
        "pgsql",
        "webhook",
        "http",
        "clickhouse",
        "tdengine",
        "greptimedb",
        "iotdb",
        "opentsdb",
        "doris",
        "datalayers",
        "influxdb",
        "timestream",
        "cassandra",
        "aws_timestream",
        "opents",
        "scylla",
        "scylladb",
        "cql",
        "greptime",
        "ch",
    ] {
        assert!(action_owned_kind(kind), "{kind} owns a runtime");
    }
    // MV-03a owns these runtimes now; they are covered by
    // `action_owned_kind_covers_the_mv03a_kinds`. Timescaledb stays
    // pending, so it still owns no runtime.
    assert!(!action_owned_kind("timescaledb"));
}

/// Read the shape error of present-but-invalid action parameters.
/// Presence is checked against the merged view (action parameter,
/// else connector document) by the builders below.
fn invalid_action_shape(
    action_type: &str,
    connector: &str,
    parameters: serde_json::Value,
) -> String {
    let rec = mv02b_record("act", action_type, connector, parameters);
    required_action_parameters(&rec).expect_err("an invalid action shape refuses")
}

#[cfg(feature = "clickhouse")]
#[tokio::test]
async fn clickhouse_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let rec = mv02b_record(
        "act",
        "clickhouse",
        "clickhouse:up",
        serde_json::json!({"table": "events"}),
    );
    let error = build_clickhouse_sink(&rec, &serde_json::json!({"database": "db"}))
        .err()
        .expect("a missing url refuses");
    assert!(error.contains("url"), "{error}");
    // A table missing in both the action and the connector refuses.
    let bare = mv02b_record("act", "clickhouse", "clickhouse:up", serde_json::json!({}));
    let error = build_clickhouse_sink(
        &bare,
        &serde_json::json!({"url": "http://127.0.0.1:8123", "database": "db"}),
    )
    .err()
    .expect("a table missing in both refuses");
    assert!(error.contains("table"), "{error}");
    // The connector table covers a bare action (merged view).
    let sink = build_clickhouse_sink(
        &bare,
        &serde_json::json!({"url": "http://127.0.0.1:8123", "database": "db", "table": "events"}),
    )
    .expect("a connector table covers a bare action");
    assert_eq!(sink.kind(), "clickhouse");
    let sink = build_clickhouse_sink(
        &rec,
        &serde_json::json!({"url": "http://127.0.0.1:8123", "database": "db"}),
    )
    .expect("a full clickhouse pair builds");
    assert_eq!(sink.kind(), "clickhouse");
}

#[tokio::test]
async fn influxdb_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let rec = mv02b_record(
        "act",
        "influxdb",
        "influxdb:up",
        serde_json::json!({"measurement": "m"}),
    );
    let error = build_influxdb_sink(&rec, &serde_json::json!({"bucket": "b", "org": "o"}))
        .err()
        .expect("a missing token refuses");
    assert!(error.contains("token"), "{error}");
    let bare = mv02b_record("act", "influxdb", "influxdb:up", serde_json::json!({}));
    let error = build_influxdb_sink(
        &bare,
        &serde_json::json!({"server": "127.0.0.1:8086", "bucket": "b", "org": "o", "token": "t"}),
    )
    .err()
    .expect("a measurement missing in both refuses");
    assert!(error.contains("measurement"), "{error}");
    let error = invalid_action_shape(
        "influxdb",
        "influxdb:up",
        serde_json::json!({"measurement": "m", "precision": "fortnight"}),
    );
    assert!(error.contains("precision"), "{error}");
    for precision in ["s", "ms", "us", "ns"] {
        let rec = mv02b_record(
            "act",
            "influxdb",
            "influxdb:up",
            serde_json::json!({"measurement": "m", "precision": precision}),
        );
        let sink = build_influxdb_sink(
            &rec,
            &serde_json::json!({"server": "127.0.0.1:8086", "bucket": "b", "org": "o", "token": "t"}),
        )
        .expect("every precision builds");
        assert_eq!(sink.kind(), "influxdb");
    }
}

#[tokio::test]
async fn greptimedb_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let rec = mv02b_record(
        "act",
        "greptimedb",
        "greptimedb:up",
        serde_json::json!({"table": "events"}),
    );
    let error = build_greptimedb_sink(&rec, &serde_json::json!({"server": "127.0.0.1:4001"}))
        .err()
        .expect("a missing database refuses");
    assert!(error.contains("database"), "{error}");
    let bare = mv02b_record("act", "greptimedb", "greptimedb:up", serde_json::json!({}));
    let error = build_greptimedb_sink(
        &bare,
        &serde_json::json!({"server": "127.0.0.1:4001", "database": "db"}),
    )
    .err()
    .expect("a table missing in both refuses");
    assert!(error.contains("table"), "{error}");
    let error = invalid_action_shape(
        "greptimedb",
        "greptimedb:up",
        serde_json::json!({"table": "t", "precision": "fortnight"}),
    );
    assert!(error.contains("precision"), "{error}");
    let error = build_greptimedb_sink(
        &rec,
        &serde_json::json!({"server": "127.0.0.1:4001", "database": "db", "username": "u"}),
    )
    .err()
    .expect("a half credential pair refuses");
    assert!(error.contains("username"), "{error}");
    for (precision, format) in [
        ("s", "sql"),
        ("ms", "influx"),
        ("us", "sql"),
        ("ns", "influx"),
    ] {
        let rec = mv02b_record(
            "act",
            "greptime",
            "greptimedb:up",
            serde_json::json!({"table": "events", "precision": precision, "format": format}),
        );
        let sink = build_greptimedb_sink(
            &rec,
            &serde_json::json!({"server": "127.0.0.1:4001", "dbname": "public"}),
        )
        .expect("every precision and format builds");
        assert_eq!(sink.kind(), "greptimedb");
    }

    // Default server: no server field → 127.0.0.1:4001.
    let rec = mv02b_record(
        "act",
        "greptimedb",
        "greptimedb:up",
        serde_json::json!({"table": "events"}),
    );
    let sink = build_greptimedb_sink(&rec, &serde_json::json!({"database": "db"}))
        .expect("default server builds");
    assert_eq!(sink.kind(), "greptimedb");
}

#[test]
fn greptimedb_endpoint_applies_the_reference_default() {
    // No server field: the reference default applies.
    assert_eq!(
        greptimedb_endpoint(None),
        "http://127.0.0.1:4001",
        "absent server uses 127.0.0.1:4001"
    );
    assert_eq!(
        greptimedb_endpoint(Some("")),
        "http://127.0.0.1:4001",
        "blank server uses 127.0.0.1:4001"
    );
    // A bare host gains the documented 8086 port.
    assert_eq!(
        greptimedb_endpoint(Some("example.com")),
        "http://example.com:8086",
        "bare host gains port 8086"
    );
    // A host with a port keeps it.
    assert_eq!(
        greptimedb_endpoint(Some("example.com:4001")),
        "http://example.com:4001",
        "host with port keeps its port"
    );
    // A full URL stays unchanged.
    assert_eq!(
        greptimedb_endpoint(Some("https://example.com:7443/db")),
        "https://example.com:7443/db",
        "full URL stays unchanged"
    );
}

#[test]
fn datalayers_endpoint_applies_the_reference_default() {
    // No server field: the reference default applies.
    assert_eq!(
        datalayers_endpoint(None),
        "http://127.0.0.1:8361",
        "absent server uses 127.0.0.1:8361"
    );
    assert_eq!(
        datalayers_endpoint(Some("")),
        "http://127.0.0.1:8361",
        "blank server uses 127.0.0.1:8361"
    );
    // A host with a port keeps it.
    assert_eq!(
        datalayers_endpoint(Some("example.com:8361")),
        "http://example.com:8361",
        "host with port keeps its port"
    );
}

#[tokio::test]
async fn opentsdb_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let rec = mv02b_record(
        "act",
        "opentsdb",
        "opentsdb:up",
        serde_json::json!({"metric": "m", "value": "${payload.temp}"}),
    );
    let error = build_opentsdb_sink(&rec, &serde_json::json!({}))
        .err()
        .expect("a missing server refuses");
    assert!(error.contains("server"), "{error}");
    let bare = mv02b_record("act", "opentsdb", "opentsdb:up", serde_json::json!({}));
    let error = build_opentsdb_sink(
        &bare,
        &serde_json::json!({"server": "http://127.0.0.1:4242"}),
    )
    .err()
    .expect("a metric missing in both refuses");
    assert!(error.contains("metric"), "{error}");
    // The connector metric and value cover a bare action.
    let sink = build_opentsdb_sink(
        &bare,
        &serde_json::json!({"server": "http://127.0.0.1:4242", "metric": "m", "value": "temperature"}),
    )
    .expect("connector fields cover a bare action");
    assert_eq!(sink.kind(), "opentsdb");
    for endpoint in ["http://127.0.0.1:4242", "telnet://127.0.0.1:4242"] {
        let sink = build_opentsdb_sink(&rec, &serde_json::json!({"server": endpoint}))
            .expect("both wire schemes build");
        assert_eq!(sink.kind(), "opentsdb");
    }
}

#[tokio::test]
async fn tdengine_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let rec = mv02b_record(
        "act",
        "tdengine",
        "tdengine:up",
        serde_json::json!({
            "stable": "meters",
            "subtable": "d_${client_id}",
            "metrics": {"temp": "${payload.temperature}"},
            "tags": {"location": "room1"},
        }),
    );
    let error = build_tdengine_sink(&rec, &serde_json::json!({"database": "power"}))
        .err()
        .expect("a missing server refuses");
    assert!(error.contains("server"), "{error}");
    let bare = mv02b_record("act", "tdengine", "tdengine:up", serde_json::json!({}));
    let error = build_tdengine_sink(
        &bare,
        &serde_json::json!({"server": "127.0.0.1:6041", "database": "power"}),
    )
    .err()
    .expect("a stable missing in both refuses");
    assert!(error.contains("stable"), "{error}");
    let error = invalid_action_shape(
        "tdengine",
        "tdengine:up",
        serde_json::json!({"stable": "s", "subtable": "d", "metrics": {"m": "1"}, "tags": {}}),
    );
    assert!(error.contains("tags"), "{error}");
    let sink = build_tdengine_sink(
        &rec,
        &serde_json::json!({"server": "127.0.0.1:6041", "database": "power"}),
    )
    .expect("a full tdengine pair builds");
    assert_eq!(sink.kind(), "tdengine");
}

#[tokio::test]
async fn mv02b_connector_level_settings_apply_and_refuse() {
    // IoTDB: a bare host gains `/rest/v2`; a carried path stays.
    assert_eq!(
        iotdb_endpoint("127.0.0.1:6667"),
        "http://127.0.0.1:6667/rest/v2"
    );
    assert_eq!(iotdb_endpoint("http://h:1/rest/v2"), "http://h:1/rest/v2");
    assert_eq!(iotdb_endpoint("http://h:1/rest/v2/"), "http://h:1/rest/v2");
    // TDengine: a half Basic pair refuses; a token builds.
    let rec = mv02b_record("act", "tdengine", "tdengine:up", serde_json::json!({}));
    let base = serde_json::json!({"server": "127.0.0.1:6041", "database": "power",
        "stable": "s", "subtable": "d",
        "metrics": {"m": "1"}, "tags": {"t": "2"}});
    let mut half = base.clone();
    half["username"] = serde_json::json!("u");
    let error = build_tdengine_sink(&rec, &half)
        .err()
        .expect("a half credential pair refuses");
    assert!(error.contains("together"), "{error}");
    let mut both = base.clone();
    both["username"] = serde_json::json!("u");
    both["password"] = serde_json::json!("p");
    both["token"] = serde_json::json!("tok");
    let error = build_tdengine_sink(&rec, &both)
        .err()
        .expect("a token mixed with Basic refuses");
    assert!(error.contains("token"), "{error}");
    let mut token = base.clone();
    token["token"] = serde_json::json!("tok");
    let sink = build_tdengine_sink(&rec, &token).expect("a token builds");
    assert_eq!(sink.kind(), "tdengine");
    // GreptimeDB: the connector precision and format apply; an
    // unknown connector value refuses.
    let grec = mv02b_record("act", "greptimedb", "greptimedb:up", serde_json::json!({}));
    let error = build_greptimedb_sink(
        &grec,
        &serde_json::json!({"server": "127.0.0.1:4001", "database": "db",
            "table": "t", "precision": "fortnight"}),
    )
    .err()
    .expect("an unknown connector precision refuses");
    assert!(error.contains("precision"), "{error}");
    let sink = build_greptimedb_sink(
        &grec,
        &serde_json::json!({"server": "127.0.0.1:4001", "database": "db",
            "table": "t", "precision": "s", "format": "influx"}),
    )
    .expect("a connector precision and format builds");
    assert_eq!(sink.kind(), "greptimedb");
    // Timestream: the connector precision applies; an unknown
    // connector value refuses.
    let trec = mv02b_record("act", "timestream", "timestream:up", serde_json::json!({}));
    let error = build_timestream_sink(
        &trec,
        &serde_json::json!({"database": "db", "table": "m", "region": "r",
            "access_key_id": "k", "secret_access_key": "s",
            "measures": {"f": "DOUBLE"}, "precision": "s"}),
    )
    .err()
    .expect("an unknown connector precision refuses");
    assert!(error.contains("precision"), "{error}");
    let sink = build_timestream_sink(
        &trec,
        &serde_json::json!({"database": "db", "table": "m", "region": "r",
            "access_key_id": "k", "secret_access_key": "s",
            "measures": {"f": "DOUBLE"}, "precision": "us"}),
    )
    .expect("a connector precision builds");
    assert_eq!(sink.kind(), "timestream");
}

#[tokio::test]
async fn iotdb_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let rec = mv02b_record(
        "act",
        "iotdb",
        "iotdb:up",
        serde_json::json!({"device": "root.factory.d1", "measurements": ["temperature"], "types": ["float"]}),
    );
    let error = build_iotdb_sink(
        &rec,
        &serde_json::json!({"endpoint": "http://127.0.0.1:6667", "username": "root"}),
    )
    .err()
    .expect("a missing password refuses");
    assert!(error.contains("password"), "{error}");
    let bare = mv02b_record("act", "iotdb", "iotdb:up", serde_json::json!({}));
    let error = build_iotdb_sink(
        &bare,
        &serde_json::json!({"endpoint": "http://127.0.0.1:6667", "username": "root", "password": "root"}),
    )
    .err()
    .expect("a device missing in both refuses");
    assert!(error.contains("device"), "{error}");
    let error = invalid_action_shape(
        "iotdb",
        "iotdb:up",
        serde_json::json!({"device": "root.d", "measurements": ["a", "b"], "types": ["float"]}),
    );
    assert!(error.contains("same count"), "{error}");
    let error = invalid_action_shape(
        "iotdb",
        "iotdb:up",
        serde_json::json!({"device": "root.d", "measurements": ["a"], "types": ["furlong"]}),
    );
    assert!(error.contains("types"), "{error}");
    let sink = build_iotdb_sink(
        &rec,
        &serde_json::json!({"endpoint": "http://127.0.0.1:6667", "username": "root", "password": "root"}),
    )
    .expect("a full iotdb pair builds");
    assert_eq!(sink.kind(), "iotdb");
}

#[tokio::test]
async fn doris_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let rec = mv02b_record(
        "act",
        "doris",
        "doris:up",
        serde_json::json!({"table": "events"}),
    );
    let error = build_doris_sink(&rec, &serde_json::json!({"database": "db"}))
        .err()
        .expect("a missing server refuses");
    assert!(error.contains("server"), "{error}");
    let bare = mv02b_record("act", "doris", "doris:up", serde_json::json!({}));
    let error = build_doris_sink(
        &bare,
        &serde_json::json!({"server": "127.0.0.1", "database": "db"}),
    )
    .err()
    .expect("a table missing in both refuses");
    assert!(error.contains("table"), "{error}");
    let error = invalid_action_shape(
        "doris",
        "doris:up",
        serde_json::json!({"table": "t", "format": "csv"}),
    );
    assert!(error.contains("format"), "{error}");
    let sink = build_doris_sink(
        &rec,
        &serde_json::json!({"server": "127.0.0.1", "database": "db"}),
    )
    .expect("a full doris pair builds");
    assert_eq!(sink.kind(), "doris");
}

#[tokio::test]
async fn datalayers_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let rec = mv02b_record(
        "act",
        "datalayers",
        "datalayers:up",
        serde_json::json!({"table": "events"}),
    );
    let error = build_datalayers_sink(&rec, &serde_json::json!({"server": "127.0.0.1:8360"}))
        .err()
        .expect("a missing database refuses");
    assert!(error.contains("database"), "{error}");
    let bare = mv02b_record("act", "datalayers", "datalayers:up", serde_json::json!({}));
    let error = build_datalayers_sink(
        &bare,
        &serde_json::json!({"server": "127.0.0.1:8360", "database": "db"}),
    )
    .err()
    .expect("a table missing in both refuses");
    assert!(error.contains("table"), "{error}");
    let error = build_datalayers_sink(
        &rec,
        &serde_json::json!({"server": "127.0.0.1:8360", "database": "db", "auth_token": "t", "username": "u"}),
    )
    .err()
    .expect("conflicting auth modes refuse");
    assert!(error.contains("auth"), "{error}");
    let sink = build_datalayers_sink(
        &rec,
        &serde_json::json!({"server": "127.0.0.1:8360", "database": "db"}),
    )
    .expect("a full datalayers pair builds");
    assert_eq!(sink.kind(), "datalayers");
}

#[tokio::test]
async fn datalayers_sink_defaults_server() {
    let rec = mv02b_record(
        "act",
        "datalayers",
        "datalayers:up",
        serde_json::json!({"table": "events"}),
    );
    // No server field: the default 127.0.0.1:8361 is used.
    let sink = build_datalayers_sink(&rec, &serde_json::json!({"database": "db"}))
        .expect("default server builds");
    assert_eq!(sink.kind(), "datalayers");
}

#[tokio::test]
async fn timestream_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let rec = mv02b_record(
        "act",
        "timestream",
        "timestream:up",
        serde_json::json!({"table": "metrics", "measures": {"temperature": "DOUBLE"}}),
    );
    let error = build_timestream_sink(
        &rec,
        &serde_json::json!({"database": "db", "region": "us-east-1", "access_key_id": "k"}),
    )
    .err()
    .expect("a missing secret refuses");
    assert!(error.contains("secret_access_key"), "{error}");
    let bare = mv02b_record("act", "timestream", "timestream:up", serde_json::json!({}));
    let full_connector = serde_json::json!({"database": "db", "region": "us-east-1", "access_key_id": "k", "secret_access_key": "s"});
    let error = build_timestream_sink(&bare, &full_connector)
        .err()
        .expect("a table missing in both refuses");
    assert!(error.contains("table"), "{error}");
    let error = build_timestream_sink(
        &mv02b_record(
            "act",
            "timestream",
            "timestream:up",
            serde_json::json!({"table": "t"}),
        ),
        &full_connector,
    )
    .err()
    .expect("measures missing in both refuse");
    assert!(error.contains("measures"), "{error}");
    let error = invalid_action_shape(
        "timestream",
        "timestream:up",
        serde_json::json!({"table": "t", "measures": {"m": "DOUBLE"}, "precision": "s"}),
    );
    assert!(error.contains("precision"), "{error}");
    let sink = build_timestream_sink(
        &rec,
        &serde_json::json!({"database": "db", "region": "us-east-1", "access_key_id": "k", "secret_access_key": "s"}),
    )
    .expect("a full timestream pair builds");
    assert_eq!(sink.kind(), "timestream");
}

#[cfg(feature = "cassandra")]
#[tokio::test]
async fn cassandra_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let rec = mv02b_record(
        "act",
        "cassandra",
        "cassandra:up",
        serde_json::json!({"table": "events"}),
    );
    let error = build_cassandra_sink(&rec, &serde_json::json!({"keyspace": "ks"}))
        .err()
        .expect("missing servers refuse");
    assert!(error.contains("servers"), "{error}");
    let bare = mv02b_record("act", "scylla", "cassandra:up", serde_json::json!({}));
    let error = build_cassandra_sink(
        &bare,
        &serde_json::json!({"servers": "127.0.0.1:9042", "keyspace": "telemetry"}),
    )
    .err()
    .expect("a table missing in both refuses");
    assert!(error.contains("table"), "{error}");
    let invalid = mv02b_record(
        "act",
        "cql",
        "cassandra:up",
        serde_json::json!({"table": "has space"}),
    );
    let error = build_cassandra_sink(
        &invalid,
        &serde_json::json!({"servers": "127.0.0.1:9042", "keyspace": "telemetry"}),
    )
    .err()
    .expect("a non-identifier table refuses");
    assert!(error.contains("table"), "{error}");
    let sink = build_cassandra_sink(
        &rec,
        &serde_json::json!({"servers": "127.0.0.1:9042", "keyspace": "telemetry"}),
    )
    .expect("a full cassandra pair builds");
    assert_eq!(sink.kind(), "cassandra");
}

#[tokio::test]
async fn mv02b_stage_builds_the_sink_live_through_the_connector_store() {
    let connectors = ConnectorStore::new();
    connectors.upsert(serde_json::json!({
        "id": "influxdb:up",
        "name": "up",
        "type": "influxdb",
        "server": "127.0.0.1:8086",
        "bucket": "b",
        "org": "o",
        "token": "t",
    }));
    let stage = stage_action_sink(
        &connectors,
        &mv02b_record(
            "act",
            "influxdb",
            "influxdb:up",
            serde_json::json!({"measurement": "m"}),
        ),
    )
    .expect("a resolvable influxdb connector stages");
    assert_eq!(stage.wiring, ActionWiring::Live);
    let sink = stage.sink.expect("live wiring carries the sink");
    assert_eq!(sink.kind(), "influxdb");
}
