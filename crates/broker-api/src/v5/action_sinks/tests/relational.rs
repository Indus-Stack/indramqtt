use super::*;

#[tokio::test]
async fn pgsql_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
    let loud = record("act", "pgsql", "postgres:up");
    // A missing connector field refuses with the field named.
    let error = build_pgsql_sink(
        &loud,
        &serde_json::json!({"username": "u"}),
        &ActionResourceOpts::default(),
    )
    .err()
    .expect("a missing server refuses");
    assert!(error.contains("server"), "{error}");
    // A missing action sql refuses with the action parameter named.
    let error = build_pgsql_sink(
        &loud,
        &serde_json::json!({
            "server": "127.0.0.1:9", "database": "db", "username": "u",
        }),
        &ActionResourceOpts::default(),
    )
    .err()
    .expect("a missing sql refuses");
    assert!(error.contains("sql"), "{error}");
    // A legacy flat sql extra covers it as well.
    let mut legacy = record("act", "pgsql", "postgres:up");
    legacy.params = serde_json::json!({"sql": "INSERT $1"});
    let sink = build_pgsql_sink(
        &legacy,
        &serde_json::json!({"server": "127.0.0.1:9", "database": "db", "username": "u"}),
        &ActionResourceOpts::default(),
    )
    .expect("a valid connector plus legacy sql builds");
    assert_eq!(sink.kind(), "postgres");
}

fn mysql_record(id: &str, connector: &str, sql: &str) -> ActionRecord {
    ActionRecord {
        parameters: serde_json::json!({"sql": sql}),
        ..record(id, "mysql", connector)
    }
}

fn mysql_doc(server: &str) -> serde_json::Value {
    serde_json::json!({"server": server, "database": "db", "username": "u"})
}

#[tokio::test]
async fn mysql_sink_builds_from_reference_fields_and_refuses_gaps() {
    // The documented three-marker statement builds.
    let sink = build_mysql_sink(
        &mysql_record(
            "act",
            "mysql:up",
            "INSERT INTO t(topic, qos, payload) VALUES (?, ?, ?)",
        ),
        &mysql_doc("127.0.0.1:3306"),
    )
    .expect("a complete mysql action builds");
    assert_eq!(sink.kind(), "mysql");
    // A missing server refuses with the field named.
    let error = build_mysql_sink(
        &mysql_record("act", "mysql:up", "INSERT INTO t VALUES (?, ?, ?)"),
        &serde_json::json!({"database": "db", "username": "u"}),
    )
    .err()
    .expect("a missing server refuses");
    assert!(error.contains("server"), "{error}");
    // A missing database refuses.
    let error = build_mysql_sink(
        &mysql_record("act", "mysql:up", "INSERT INTO t VALUES (?, ?, ?)"),
        &serde_json::json!({"server": "127.0.0.1:3306", "username": "u"}),
    )
    .err()
    .expect("a missing database refuses");
    assert!(error.contains("database"), "{error}");
    // A missing statement refuses with the action parameter named.
    let error = build_mysql_sink(
        &record("act", "mysql", "mysql:up"),
        &mysql_doc("127.0.0.1:3306"),
    )
    .err()
    .expect("a missing sql refuses");
    assert!(error.contains("parameters.sql"), "{error}");
    // A statement without exactly three markers refuses.
    let error = build_mysql_sink(
        &mysql_record("act", "mysql:up", "INSERT INTO t VALUES (?, ?)"),
        &mysql_doc("127.0.0.1:3306"),
    )
    .err()
    .expect("a two-marker statement refuses");
    assert!(error.contains('?'), "{error}");
    // TLS has no driver knob here: enabling it refuses instead of
    // running plaintext against a TLS-only server.
    let error = build_mysql_sink(
        &mysql_record("act", "mysql:up", "INSERT INTO t VALUES (?, ?, ?)"),
        &serde_json::json!({"server": "127.0.0.1:3306", "database": "db", "ssl": {"enable": true}}),
    )
    .err()
    .expect("an enabled ssl refuses");
    assert!(error.contains("ssl.enable"), "{error}");
    // A zero pool refuses.
    let error = build_mysql_sink(
        &mysql_record("act", "mysql:up", "INSERT INTO t VALUES (?, ?, ?)"),
        &serde_json::json!({"server": "127.0.0.1:3306", "database": "db", "pool_size": 0}),
    )
    .err()
    .expect("a zero pool refuses");
    assert!(error.contains("pool_size"), "{error}");
}

#[tokio::test]
async fn mysql_alias_selects_the_mysql_runtime() {
    assert_eq!(canonical_action_kind("mariadb"), "mysql");
    assert!(action_owned_kind("mariadb"));
    let sink = build_action_sink(
        &record("act", "mariadb", "mysql:up"),
        &serde_json::json!({}),
        &ActionResourceOpts::default(),
    );
    // An empty connector document is not a build failure path here:
    // the missing server refuses with the field named.
    let error = sink.err().expect("a missing server refuses");
    assert!(error.contains("server"), "{error}");
    // Absent or false renders the fixed row; true refuses named.
    let mut null_opt = record("act", "mysql", "mysql:up");
    null_opt.parameters = serde_json::json!({"sql": "INSERT INTO t VALUES (?, ?, ?)", "undefined_vars_as_null": true});
    let error = refuse_undefined_vars_as_null(&null_opt).expect_err("true refuses");
    assert!(error.contains("undefined_vars_as_null"), "{error}");
    null_opt.parameters = serde_json::json!({"sql": "INSERT INTO t VALUES (?, ?, ?)", "undefined_vars_as_null": false});
    assert!(refuse_undefined_vars_as_null(&null_opt).is_ok());
    null_opt.parameters = serde_json::json!({"sql": "INSERT INTO t VALUES (?, ?, ?)"});
    assert!(refuse_undefined_vars_as_null(&null_opt).is_ok());
}

#[test]
fn connector_pool_size_defaults_and_bounds() {
    let doc = serde_json::json!({});
    assert_eq!(
        connector_pool_size("act", "conn", &doc).expect("absent pools to 8"),
        8
    );
    assert_eq!(
        connector_pool_size("act", "conn", &serde_json::json!({"pool_size": 3}))
            .expect("an explicit pool applies"),
        3
    );
    assert!(connector_pool_size("act", "conn", &serde_json::json!({"pool_size": 0})).is_err());
    assert!(connector_pool_size("act", "conn", &serde_json::json!({"pool_size": 1025})).is_err());
    assert!(connector_pool_size("act", "conn", &serde_json::json!({"pool_size": "many"})).is_err());
}

#[test]
fn host_port_split_keeps_protocol_defaults() {
    let (host, port) =
        parse_host_port("act", "conn", "server", "db-host", 5432).expect("bare host");
    assert_eq!((host.as_str(), port), ("db-host", 5432));
    let (host, port) =
        parse_host_port("act", "conn", "server", "db-host:6432", 5432).expect("explicit port");
    assert_eq!((host.as_str(), port), ("db-host", 6432));
    let (host, port) =
        parse_host_port("act", "conn", "server", "mysql://db-host/db", 3306).expect("scheme");
    assert_eq!((host.as_str(), port), ("db-host", 3306));
    let (host, port) =
        parse_host_port("act", "conn", "server", "[::1]:7432", 5432).expect("bracketed ipv6");
    assert_eq!((host.as_str(), port), ("::1", 7432));
    assert!(parse_host_port("act", "conn", "server", "", 5432).is_err());
    assert!(parse_host_port("act", "conn", "server", "u@db-host", 5432).is_err());
    assert!(parse_host_port("act", "conn", "server", "db-host:0", 5432).is_err());
    assert!(parse_host_port("act", "conn", "server", "db-host:notaport", 5432).is_err());
}

#[test]
fn pg_tls_maps_the_reference_ssl_object() {
    let plain = serde_json::json!({});
    assert!(!pg_family_tls("act", "conn", &plain).expect("absent ssl is plaintext"));
    let enabled = serde_json::json!({"ssl": {"enable": true}});
    assert!(pg_family_tls("act", "conn", &enabled).expect("enabled ssl is TLS"));
    let peer = serde_json::json!({"ssl": {"enable": true, "verify": "verify_peer"}});
    assert!(pg_family_tls("act", "conn", &peer).expect("verify_peer is TLS"));
    let none = serde_json::json!({"ssl": {"enable": true, "verify": "verify_none"}});
    assert!(pg_family_tls("act", "conn", &none).is_err());
    let custom = serde_json::json!({"ssl": {"enable": true, "cacertfile": "/ca.pem"}});
    assert!(pg_family_tls("act", "conn", &custom).is_err());
    let disabled = serde_json::json!({"ssl": {"enable": false}});
    assert!(!pg_family_tls("act", "conn", &disabled).expect("disabled ssl is plaintext"));
}

fn pg_record(kind: &str, id: &str, connector: &str, sql: &str) -> ActionRecord {
    ActionRecord {
        parameters: serde_json::json!({"sql": sql}),
        ..record(id, kind, connector)
    }
}

fn pg_doc(server: &str) -> serde_json::Value {
    serde_json::json!({"server": server, "database": "db", "username": "u"})
}

#[tokio::test]
async fn pg_family_sink_builds_per_kind_and_refuses_gaps() {
    for kind in ["alloydb", "cockroachdb", "redshift"] {
        // The default port per kind (CockroachDB 26257, Redshift
        // 5439, AlloyDB 5432) is proved by the e2e tests, which run
        // the server without a port and observe the dial.
        let sink = build_pg_family_sink(
            kind,
            &pg_record(
                kind,
                "act",
                "conn:up",
                "INSERT INTO t(topic, qos, payload) VALUES ($1, $2, $3)",
            ),
            &pg_doc("127.0.0.1"),
        )
        .expect("a complete pg-family action builds");
        assert_eq!(sink.kind(), kind);
        // A missing username refuses: the reference names no default.
        let error = build_pg_family_sink(
            kind,
            &pg_record(kind, "act", "conn:up", "INSERT INTO t VALUES ($1, $2, $3)"),
            &serde_json::json!({"server": "127.0.0.1", "database": "db"}),
        )
        .err()
        .expect("a missing username refuses");
        assert!(error.contains("username"), "{error}");
        // A marker outside $1..$3 refuses.
        let error = build_pg_family_sink(
            kind,
            &pg_record(kind, "act", "conn:up", "INSERT INTO t VALUES ($1, $2, $4)"),
            &pg_doc("127.0.0.1"),
        )
        .err()
        .expect("a $4 marker refuses");
        assert!(error.contains("$4"), "{error}");
        // A prepared-statement opt-out refuses: every send prepares.
        let error = build_pg_family_sink(
        kind,
        &pg_record(kind, "act", "conn:up", "INSERT INTO t VALUES ($1, $2, $3)"),
        &serde_json::json!({"server": "127.0.0.1", "database": "db", "username": "u", "disable_prepared_statements": true}),
    )
    .err()
    .expect("disabling prepared statements refuses");
        assert!(error.contains("disable_prepared_statements"), "{error}");
        // An explicit application name refuses: never stamped.
        let error = build_pg_family_sink(
        kind,
        &pg_record(kind, "act", "conn:up", "INSERT INTO t VALUES ($1, $2, $3)"),
        &serde_json::json!({"server": "127.0.0.1", "database": "db", "username": "u", "application_name": "app"}),
    )
    .err()
    .expect("an application name refuses");
        assert!(error.contains("application_name"), "{error}");
    }
    // The cockroach alias selects the cockroachdb runtime.
    assert_eq!(canonical_action_kind("cockroach"), "cockroachdb");
    assert!(action_owned_kind("cockroach"));
}

#[tokio::test]
async fn mssql_sink_builds_custom_upsert_and_refuses_gaps() {
    let doc = serde_json::json!({"server": "127.0.0.1:1433", "database": "db"});
    let sink = build_mssql_sink(
        &pg_record(
            "mssql",
            "act",
            "mssql:up",
            "INSERT INTO t(topic, qos, payload) VALUES ($2, $4, $5)",
        ),
        &doc,
    )
    .expect("a complete mssql action builds");
    assert_eq!(sink.kind(), "mssql");
    // A missing database refuses.
    let error = build_mssql_sink(
        &pg_record("mssql", "act", "mssql:up", "INSERT INTO t VALUES ($2)"),
        &serde_json::json!({"server": "127.0.0.1:1433"}),
    )
    .err()
    .expect("a missing database refuses");
    assert!(error.contains("database"), "{error}");
    // A non-default driver refuses: only the default maps here.
    let error = build_mssql_sink(
        &pg_record("mssql", "act", "mssql:up", "INSERT INTO t VALUES ($2)"),
        &serde_json::json!({"server": "127.0.0.1:1433", "database": "db", "driver": "FreeTDS"}),
    )
    .err()
    .expect("a foreign driver refuses");
    assert!(error.contains("driver"), "{error}");
    // The sqlserver alias selects the mssql runtime.
    assert_eq!(canonical_action_kind("sqlserver"), "mssql");
    assert!(action_owned_kind("sqlserver"));
    // The postgres alias selects the pgsql runtime (MV-02a binding).
    assert_eq!(canonical_action_kind("postgres"), "pgsql");
    assert_eq!(canonical_action_kind("postgresql"), "pgsql");
    assert!(action_owned_kind("postgres"));
    // An action filed as postgres builds through the pgsql builder.
    let pg_doc = serde_json::json!({"server": "127.0.0.1:5432", "database": "db", "username": "u"});
    let pg_sink = build_action_sink(
        &pg_record(
            "postgres",
            "act",
            "pg:up",
            "INSERT INTO t VALUES ($1, $2, $3)",
        ),
        &pg_doc,
        &ActionResourceOpts::default(),
    )
    .expect("a postgres action builds")
    .expect("an owned kind builds a sink");
    assert_eq!(pg_sink.kind(), "postgres");
}

#[tokio::test]
async fn oracle_sink_builds_from_service_name_and_refuses_gaps() {
    let doc =
        serde_json::json!({"server": "127.0.0.1:1521", "username": "u", "service_name": "XE"});
    let sink = build_oracle_sink(
        &pg_record(
            "oracle",
            "act",
            "oracle:up",
            "INSERT INTO t(topic, qos, payload) VALUES (:1, :2, :3)",
        ),
        &doc,
    )
    .expect("a complete oracle action builds");
    assert_eq!(sink.kind(), "oracle");
    // A missing service refuses naming both fields.
    let error = build_oracle_sink(
        &pg_record("oracle", "act", "oracle:up", "INSERT INTO t VALUES (:1)"),
        &serde_json::json!({"server": "127.0.0.1:1521", "username": "u"}),
    )
    .err()
    .expect("a missing service refuses");
    assert!(error.contains("service_name"), "{error}");
    // A sysdba role refuses: the driver has no role knob.
    let error = build_oracle_sink(
        &pg_record("oracle", "act", "oracle:up", "INSERT INTO t VALUES (:1)"),
        &serde_json::json!({"server": "127.0.0.1", "username": "u", "sid": "XE", "role": "sysdba"}),
    )
    .err()
    .expect("a sysdba role refuses");
    assert!(error.contains("role"), "{error}");
    // A marker outside :1..:3 refuses.
    let error = build_oracle_sink(
        &pg_record(
            "oracle",
            "act",
            "oracle:up",
            "INSERT INTO t VALUES (:1, :4)",
        ),
        &doc,
    )
    .err()
    .expect("a :4 marker refuses");
    assert!(error.contains(":4"), "{error}");
}

#[tokio::test]
async fn oracle_sink_builds_from_sid_alone() {
    // A bare `sid` builds: the transport dials by identifier.
    let doc = serde_json::json!({"server": "127.0.0.1:1521", "username": "u", "sid": "XE"});
    let sink = build_oracle_sink(
        &pg_record(
            "oracle",
            "act",
            "oracle:up",
            "INSERT INTO t(topic, qos, payload) VALUES (:1, :2, :3)",
        ),
        &doc,
    )
    .expect("a sid-only oracle action builds");
    assert_eq!(sink.kind(), "oracle");
    // A service name wins when both are set.
    let doc = serde_json::json!({"server": "127.0.0.1:1521", "username": "u", "service_name": "XE", "sid": "ORCL"});
    let sink = build_oracle_sink(
        &pg_record(
            "oracle",
            "act",
            "oracle:up",
            "INSERT INTO t(topic, qos, payload) VALUES (:1, :2, :3)",
        ),
        &doc,
    )
    .expect("a service-name oracle action builds");
    assert_eq!(sink.kind(), "oracle");
}

#[tokio::test]
async fn relational_staging_wires_live_without_dialling() {
    let connectors = ConnectorStore::new();
    connectors.upsert(serde_json::json!({
        "id": "mysql:up",
        "name": "up",
        "type": "mysql",
        "server": "127.0.0.1:9",
        "database": "db",
    }));
    let stage = stage_action_sink(
        &connectors,
        &mysql_record(
            "act",
            "mysql:up",
            "INSERT INTO t(topic, qos, payload) VALUES (?, ?, ?)",
        ),
    )
    .expect("a resolvable mysql connector stages");
    assert_eq!(stage.wiring, ActionWiring::Live);
    assert!(stage.sink.is_some());
}
