//! Coverage for the DOC-04 guides (rules, connectors, tenancy).
//!
//! This test pins the documentation against the live code inventories:
//! every connector kind the kernel can execute names a row in the
//! connectors overview, and every built-in SQL function names an entry
//! in the functions page. It reads files only and starts no broker.

use std::path::PathBuf;

/// Read one docs page relative to `docs/`.
fn read_page(name: &str) -> String {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let page = root.join("../../docs").join(name);
    std::fs::read_to_string(&page)
        .unwrap_or_else(|error| panic!("read {}: {error}", page.display()))
}

/// Every connector kind the kernel executes holds a row in the overview.
#[test]
fn connector_table_covers_catalogue() {
    let page = read_page("connectors/index.md");
    let catalogue = broker_connectors::action_types_catalogue();
    assert!(!catalogue.is_empty(), "catalogue must not be empty");
    let mut missing = Vec::new();
    for entry in &catalogue {
        if !page.contains(&format!("`{}`", entry.kind)) {
            missing.push(entry.kind);
        }
    }
    assert!(
        missing.is_empty(),
        "connectors/index.md names no row for: {missing:?}"
    );
    println!("DOC04_CONNECTORS kinds={}", catalogue.len());
}

/// Every built-in SQL function holds an entry in the functions page.
#[test]
fn function_tables_cover_catalogue() {
    let page = read_page("rules/functions.md");
    let catalogue = broker_rules::builtin_function_metadata();
    assert!(!catalogue.is_empty(), "catalogue must not be empty");
    let mut missing = Vec::new();
    for entry in catalogue {
        if !page.contains(&format!("`{}`", entry.name)) {
            missing.push(entry.name);
        }
    }
    assert!(
        missing.is_empty(),
        "rules/functions.md names no entry for: {missing:?}"
    );
    println!("DOC04_FUNCTIONS catalogue={}", catalogue.len());
}

/// Every broker UDF the engine registers holds an entry on the page.
/// The list mirrors the registration in `broker-rules/src/stdlib.rs`.
#[test]
fn function_page_covers_broker_udfs() {
    let page = read_page("rules/functions.md");
    let names = [
        "msgid",
        "qos",
        "flags",
        "flag",
        "topic",
        "clientid",
        "clientip",
        "peerhost",
        "username",
        "payload",
        "contains_topic",
        "contains_topic_match",
        "topic_match",
        "null",
        "coalesce_ne",
        "div",
        "fmod",
        "acosh",
        "asinh",
        "atanh",
        "bitsl",
        "bitsr",
        "bitsize",
        "bytesize",
        "subbits",
        "str",
        "str_utf8",
        "str_utf16_le",
        "bool",
        "int",
        "float",
        "float2str",
        "map",
        "bin2hexstr",
        "hexstr2bin",
        "sqlserver_bin2hexstr",
        "is_null",
        "is_null_var",
        "is_not_null",
        "is_not_null_var",
        "is_str",
        "is_bool",
        "is_int",
        "is_float",
        "is_num",
        "is_map",
        "is_array",
        "is_empty",
        "strlen",
        "substr",
        "split",
        "concat",
        "tokens",
        "sprintf_s",
        "pad",
        "replace",
        "regex_match",
        "regex_replace",
        "regex_extract",
        "ascii",
        "find",
        "join_to_string",
        "map_to_redis_hset_args",
        "join_to_sql_values_string",
        "jq",
        "unescape",
        "rm_prefix",
        "rtrim",
        "map_new",
        "map_get",
        "map_put",
        "map_keys",
        "map_values",
        "map_to_entries",
        "map_size",
        "mget",
        "mput",
        "nth",
        "length",
        "sublist",
        "first",
        "last",
        "contains",
        "sha",
        "hash",
        "hash_to_range",
        "map_to_range",
        "zip",
        "unzip",
        "gzip",
        "gunzip",
        "zip_compress",
        "zip_uncompress",
        "lz4_compress",
        "lz4_uncompress",
        "base64_encode",
        "base64_decode",
        "json_encode",
        "json_decode",
        "term_encode",
        "term_decode",
        "now_rfc3339",
        "unix_ts_to_rfc3339",
        "rfc3339_to_unix_ts",
        "now_timestamp",
        "format_date",
        "date_to_unix_ts",
        "timezone_to_second",
        "timezone_to_offset_seconds",
        "mongo_date",
        "getenv",
        "random",
        "uuid_v4",
        "uuid_v4_no_hyphen",
        "proc_dict_get",
        "proc_dict_put",
        "proc_dict_del",
        "kv_store_get",
        "kv_store_put",
        "kv_store_del",
        "std_substr",
        "std_split",
        "std_concat",
        "std_length",
        "std_base64_encode",
        "std_base64_decode",
        "std_rtrim",
    ];
    let mut missing = Vec::new();
    for name in names {
        if !page.contains(&format!("`{name}`")) {
            missing.push(name);
        }
    }
    assert!(
        missing.is_empty(),
        "rules/functions.md names no entry for: {missing:?}"
    );
    println!("DOC04_UDFS names={}", names.len());
}

/// The SQL page names every clause, event source, and window form.
#[test]
fn sql_page_covers_clauses_sources_windows() {
    let page = read_page("rules/sql.md");
    let mut missing = Vec::new();
    for item in [
        "SELECT",
        "FROM",
        "WHERE",
        "FOREACH",
        "INCASE",
        "CASE",
        "GROUP BY",
        "$events/client_connected",
        "$events/client_disconnected",
        "$events/client_connack",
        "$events/client_check_authz_complete",
        "$events/session_subscribed",
        "$events/session_unsubscribed",
        "$events/message_delivered",
        "$events/message_acked",
        "$events/message_dropped",
        "$events/delivery_dropped",
        "TUMBLINGWINDOW",
        "HOPPINGWINDOW",
        "SLIDINGWINDOW",
        "COUNTWINDOW",
    ] {
        if !page.contains(item) {
            missing.push(item);
        }
    }
    assert!(missing.is_empty(), "rules/sql.md misses: {missing:?}");
}

/// The actions page names every built-in action.
#[test]
fn actions_page_covers_action_kinds() {
    let page = read_page("rules/actions.md");
    for item in [
        "republish",
        "console",
        "log",
        "forwardconnector",
        "${payload}",
        "direct_dispatch",
    ] {
        assert!(page.contains(item), "rules/actions.md misses `{item}`");
    }
}

/// Each top-ten connector holds its own page naming its kind.
#[test]
fn top_ten_connector_pages_exist() {
    for (file, kind) in [
        ("connectors/kafka.md", "kafka"),
        ("connectors/pgsql.md", "postgres"),
        ("connectors/mysql.md", "mysql"),
        ("connectors/mongodb.md", "mongodb"),
        ("connectors/redis.md", "redis"),
        ("connectors/influxdb.md", "influxdb"),
        ("connectors/clickhouse.md", "clickhouse"),
        ("connectors/http.md", "webhook"),
        ("connectors/mqtt-bridge.md", "mqtt_bridge"),
        ("connectors/s3.md", "s3"),
    ] {
        let page = read_page(file);
        assert!(
            page.contains(&format!("`{kind}`")),
            "{file} names no `{kind}`"
        );
    }
}

/// The tenancy pages name the model, the routes, and the bounds.
#[test]
fn tenancy_pages_cover_model_and_bounds() {
    let model = read_page("multi-tenancy/index.md");
    let quotas = read_page("multi-tenancy/quotas.md");
    for item in [
        "tenants.rules",
        "default",
        "t-${username}",
        "/api/v5/mt/ns/",
        "/api/v5/mt/ns_list",
    ] {
        assert!(
            model.contains(item),
            "multi-tenancy/index.md misses `{item}`"
        );
    }
    for item in [
        "quotas.max_connections_per_tenant",
        "quotas.max_sessions_per_tenant",
        "quotas.max_retained_per_tenant",
        "32",
        "1024",
        "kick_all_clients",
        "?tenant=",
    ] {
        assert!(
            quotas.contains(item) || model.contains(item),
            "tenancy pages miss `{item}`"
        );
    }
}
