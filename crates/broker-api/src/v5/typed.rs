//! Typed bodies for actions, connectors and sources.
//!
//! The console sends one body shape per type. Each shape carries
//! `type`, `name`, `connector`, `enable`, `parameters` and
//! `resource_opts`. Reads add `status` and `node_status`.
//! The path id has the form `{type}:{name}`.
//! Reads accept the bare name too. Old files stay readable.

/// Action type names from the documented enum in documented order.
/// The list holds each listed type that this build runs.
/// New kinds with no reference schema stay out until the contract lands.
pub const SPEC_ACTION_TYPES: &[&str] = &[
    "timescale",
    "opents",
    "mongodb",
    "gcp_pubsub_producer",
    "oracle",
    "pulsar",
    "tdengine",
    "azure_blob_storage",
    "snowflake_streaming",
    "elasticsearch",
    "influxdb",
    "pgsql",
    "cassandra",
    "cockroachdb",
    "kinesis",
    "iotdb",
    "mysql",
    "rabbitmq",
    "greptimedb",
    "confluent_producer",
    "disk_log",
    "datalayers",
    "sqlserver",
    "http",
    "kafka_producer",
    "redis",
    "redshift",
    "clickhouse",
    "mqtt",
    "tablestore",
    "alloydb",
    "aws_timestream",
    "bigquery",
    "snowflake",
    "rocketmq",
    "s3",
    "doris",
    "couchbase",
    "dynamo",
    "azure_event_hub_producer",
    "opc_ua",
    "sparkplug_b",
];

/// Source type names from the documented enum, in documented order,
/// limited to the types this build has. `azure_event_grid` has no
/// runtime here and stays out.
pub const SPEC_SOURCE_TYPES: &[&str] =
    &["mqtt", "kafka_consumer", "gcp_pubsub_consumer", "rabbitmq"];

/// Build the path id from a type and a bare name.
#[must_use]
pub fn bridge_key(action_type: &str, name: &str) -> String {
    format!("{action_type}:{name}")
}

/// Strip the `{type}:` prefix from a raw id. The store keys rows by
/// the bare name. Reads accept both forms.
#[must_use]
pub fn bare_name(raw: &str, action_type: &str) -> String {
    let trimmed = raw.trim();
    let prefix = format!("{action_type}:");
    if let Some(rest) = trimmed.strip_prefix(prefix.as_str()) {
        let rest = rest.trim();
        if !rest.is_empty() {
            return rest.to_string();
        }
    }
    trimmed.to_string()
}

/// True when `requested` names the stored row. Accepts the bare name
/// and the `{type}:{name}` key. Rejects all other forms.
#[must_use]
pub fn id_matches(stored_id: &str, stored_type: &str, requested: &str) -> bool {
    if requested == stored_id {
        return true;
    }
    requested == bridge_key(stored_type, stored_id)
}

/// Status for an action or a source. The kernel tracks only the
/// enable flag. An enabled row reads `connected`. A disabled row
/// reads `disconnected`. No probe runs here.
#[must_use]
pub fn typed_status(enable: bool) -> &'static str {
    if enable {
        "connected"
    } else {
        "disconnected"
    }
}

/// One node-status row for the local node.
#[must_use]
pub fn typed_node_status(status: &str) -> serde_json::Value {
    serde_json::json!([{ "node": crate::node_scope::local_node_name(), "status": status }])
}
