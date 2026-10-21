//! Kind capability map (KC-00).
//!
//! One row per canonical connector, action and source kind, with every
//! alias, the owning ticket, the required Cargo feature, the accepted
//! capabilities (connector create, action create, source create, probe,
//! SQL `INTO`) and the readiness state. The API, the schema listings,
//! the file loader, rule SQL `INTO`, boot replay and the documents read
//! this table; there is no second kind list.
//!
//! A kind whose state is not `ready` must be refused at create, update
//! and probe with `connector type <kind> is not available in this
//! broker`, must be refused by a strict config file load, and must be
//! tolerated by the stored-state path so an operator can still read and
//! delete an old record. Only `ready` kinds may deliver.

use std::fmt::Write as _;

/// Readiness state of one kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KindState {
    /// Implemented, no invented target value; may deliver.
    Ready,
    /// Local helper available only in a test build (decision D9).
    TestOnly,
    /// Refused until the named family ticket sets it `ready`.
    Pending(&'static str),
    /// Refused under the named decision; not kept for compatibility.
    Refused(&'static str),
}

impl KindState {
    /// Human-readable name used in documents and tests.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            KindState::Ready => "ready",
            KindState::TestOnly => "test-only",
            KindState::Pending(_) => "pending",
            KindState::Refused(_) => "refused",
        }
    }

    /// The owning ticket or decision, when the state names one.
    #[must_use]
    pub fn owner(self) -> Option<&'static str> {
        match self {
            KindState::Ready | KindState::TestOnly => None,
            KindState::Pending(ticket) | KindState::Refused(ticket) => Some(ticket),
        }
    }
}

/// One canonical kind and every name that selects it.
#[derive(Debug, Clone, Copy)]
pub struct KindRow {
    /// Canonical name (the row key).
    pub name: &'static str,
    /// Aliases accepted for the same runtime.
    pub aliases: &'static [&'static str],
    /// Family the kind belongs to (documentation grouping).
    pub family: &'static str,
    /// Owning ticket or decision.
    pub owner: &'static str,
    /// Readiness state.
    pub state: KindState,
    /// Accepted as a connector create kind.
    pub connector: bool,
    /// Accepted as an action create kind.
    pub action: bool,
    /// Accepted as a source create kind.
    pub source: bool,
    /// Exposed through the connector/action probe endpoints.
    pub probe: bool,
    /// Usable as an inline SQL `INTO connector("id")` target.
    pub sql_into: bool,
    /// Cargo feature that compiles the runtime in, when gated.
    pub feature: Option<&'static str>,
    /// Present in the default build.
    pub default_build: bool,
    /// Present in the `full`/all-features build.
    pub full_build: bool,
}

/// The one capability table. Every accepted name appears here exactly
/// once, as a `name` or in one row's `aliases`.
pub static KIND_ROWS: &[KindRow] = &[
    KindRow {
        name: "alloydb",
        aliases: &[],
        family: "relational",
        owner: "MV-02a",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "aws_iot",
        aliases: &["aws_iot_core"],
        family: "cloud-iot",
        owner: "MV-03b",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: Some("aws_iot"),
        default_build: false,
        full_build: true,
    },
    KindRow {
        name: "azure_blob",
        aliases: &["azure_blob_storage", "azureblob", "azblob"],
        family: "object-store",
        owner: "MV-02d",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: Some("azure_blob"),
        default_build: false,
        full_build: true,
    },
    KindRow {
        name: "azure_eventhubs",
        aliases: &[
            "azure_event_hubs",
            "azure",
            "eventhubs",
            "azure_event_hub_producer",
        ],
        family: "cloud-messaging",
        owner: "MV-03b",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: Some("azure_eventhubs"),
        default_build: false,
        full_build: true,
    },
    KindRow {
        name: "azure_iot",
        aliases: &["azure_iot_hub", "azure_iothub", "iothub"],
        family: "cloud-iot",
        owner: "MV-03b",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: Some("azure_iot"),
        default_build: false,
        full_build: true,
    },
    KindRow {
        name: "bigquery",
        aliases: &["bq", "gbq"],
        family: "object-store",
        owner: "MV-02d",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: Some("bigquery"),
        default_build: false,
        full_build: true,
    },
    KindRow {
        name: "cassandra",
        aliases: &["scylla", "scylladb", "cql"],
        family: "time-series",
        owner: "MV-02b",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: Some("cassandra"),
        default_build: false,
        full_build: true,
    },
    KindRow {
        name: "clickhouse",
        aliases: &["ch"],
        family: "time-series",
        owner: "MV-02b",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: Some("clickhouse"),
        default_build: false,
        full_build: true,
    },
    KindRow {
        name: "cockroachdb",
        aliases: &[],
        family: "relational",
        owner: "MV-02a",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "confluent",
        aliases: &["confluent_kafka", "confluent_cloud", "confluent_producer"],
        family: "message-broker",
        owner: "MV-03a",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: Some("confluent"),
        default_build: false,
        full_build: true,
    },
    KindRow {
        name: "console",
        aliases: &["logger"],
        family: "local",
        owner: "KC-00/D9",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "couchbase",
        aliases: &["couch", "cb"],
        family: "document-store",
        owner: "MV-02c",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: Some("couchbase"),
        default_build: false,
        full_build: true,
    },
    KindRow {
        name: "databricks",
        aliases: &["delta_lake"],
        family: "object-store",
        owner: "MV-02d",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "datalayers",
        aliases: &[],
        family: "time-series",
        owner: "MV-02b",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "disk_log",
        aliases: &[],
        family: "object-store",
        owner: "MV-02d",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "doris",
        aliases: &[],
        family: "time-series",
        owner: "MV-02b",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "dynamodb",
        aliases: &["dynamo", "ddb"],
        family: "object-store",
        owner: "MV-02d",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: Some("dynamodb"),
        default_build: false,
        full_build: true,
    },
    KindRow {
        name: "elasticsearch",
        aliases: &["elastic", "es", "opensearch"],
        family: "document-store",
        owner: "MV-02c",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: Some("elasticsearch"),
        default_build: false,
        full_build: true,
    },
    KindRow {
        name: "gcp_iot",
        aliases: &["gcp_iot_core", "cloud_iot"],
        family: "cloud-iot",
        owner: "MV-03b",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: Some("gcp_iot"),
        default_build: false,
        full_build: true,
    },
    KindRow {
        name: "gcp_pubsub",
        aliases: &[
            "gcp",
            "pubsub",
            "gcp_pubsub_consumer",
            "gcp_pubsub_producer",
        ],
        family: "cloud-messaging",
        owner: "MV-03b",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: Some("gcp_pubsub"),
        default_build: false,
        full_build: true,
    },
    KindRow {
        name: "greptimedb",
        aliases: &[],
        family: "time-series",
        owner: "MV-02b",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "http",
        aliases: &["rest"],
        family: "http",
        owner: "MV-03c",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "influxdb",
        aliases: &[],
        family: "time-series",
        owner: "MV-02b",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "iotdb",
        aliases: &[],
        family: "time-series",
        owner: "MV-02b",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "kafka",
        aliases: &["kafka_producer", "kafka_consumer"],
        family: "message-broker",
        owner: "MV-03a",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: Some("kafka"),
        default_build: false,
        full_build: true,
    },
    KindRow {
        name: "kinesis",
        aliases: &["aws_kinesis"],
        family: "cloud-messaging",
        owner: "MV-03b",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: Some("kinesis"),
        default_build: false,
        full_build: true,
    },
    KindRow {
        name: "mongodb",
        aliases: &["mongo", "documentdb"],
        family: "document-store",
        owner: "MV-02c",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: Some("mongodb"),
        default_build: false,
        full_build: true,
    },
    KindRow {
        name: "mqtt_bridge",
        aliases: &["mqtt", "mqtt-bridge", "bridge"],
        family: "message-broker",
        owner: "MV-03a",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "mssql",
        aliases: &["sqlserver"],
        family: "relational",
        owner: "MV-02a",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "mysql",
        aliases: &["mariadb"],
        family: "relational",
        owner: "MV-02a",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "oci_streaming",
        aliases: &["oci"],
        family: "cloud-messaging",
        owner: "MV-03b",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "opc_ua",
        aliases: &["opcua"],
        family: "device",
        owner: "MV-03c",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "opentsdb",
        aliases: &["opents"],
        family: "time-series",
        owner: "MV-02b",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "oracle",
        aliases: &[],
        family: "relational",
        owner: "MV-02a",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "pgsql",
        aliases: &["postgres", "postgresql"],
        family: "relational",
        owner: "MV-01",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "pulsar",
        aliases: &[],
        family: "message-broker",
        owner: "MV-03a",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "rabbitmq",
        aliases: &["rabbit", "amqp"],
        family: "message-broker",
        owner: "MV-03a",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: Some("rabbitmq"),
        default_build: false,
        full_build: true,
    },
    KindRow {
        name: "redis",
        aliases: &[],
        family: "message-broker",
        owner: "MV-03a",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "redshift",
        aliases: &[],
        family: "relational",
        owner: "MV-02a",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "rocketmq",
        aliases: &[],
        family: "message-broker",
        owner: "MV-03a",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "s3",
        aliases: &["minio"],
        family: "object-store",
        owner: "MV-02d",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: Some("s3"),
        default_build: false,
        full_build: true,
    },
    KindRow {
        name: "snowflake",
        aliases: &["snowflake_streaming", "snowflake_aggregated"],
        family: "object-store",
        owner: "MV-02d",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "sparkplug_b",
        aliases: &["sparkplug", "spb"],
        family: "device",
        owner: "MV-03c",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "tablestore",
        aliases: &["ots"],
        family: "object-store",
        owner: "MV-02d",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "tdengine",
        aliases: &[],
        family: "time-series",
        owner: "MV-02b",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "test",
        aliases: &[],
        family: "local",
        owner: "MV-03c",
        state: KindState::TestOnly,
        connector: true,
        action: false,
        source: false,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "timescaledb",
        aliases: &["timescale"],
        family: "time-series",
        owner: "MV-02b",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "timestream",
        aliases: &["aws_timestream"],
        family: "time-series",
        owner: "MV-02b",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "webhook",
        aliases: &[],
        family: "http",
        owner: "MV-01",
        state: KindState::Ready,
        connector: true,
        action: true,
        source: true,
        probe: true,
        sql_into: true,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "azure_event_grid",
        aliases: &[],
        family: "cloud-messaging",
        owner: "D3",
        state: KindState::Refused("D3"),
        connector: false,
        action: true,
        source: true,
        probe: true,
        sql_into: false,
        feature: None,
        default_build: true,
        full_build: true,
    },
    KindRow {
        name: "s3_tables",
        aliases: &["s3tables", "iceberg"],
        family: "object-store",
        owner: "MV-04",
        state: KindState::Refused("MV-04"),
        connector: false,
        action: false,
        source: false,
        probe: false,
        sql_into: false,
        feature: None,
        default_build: true,
        full_build: true,
    },
];

/// Find the row that owns `name` (canonical name or alias).
#[must_use]
pub fn kind_row(name: &str) -> Option<&'static KindRow> {
    KIND_ROWS
        .iter()
        .find(|row| row.name == name || row.aliases.contains(&name))
}

/// Readiness state of `name`, when the map owns it.
#[must_use]
pub fn kind_state(name: &str) -> Option<KindState> {
    kind_row(name).map(|row| row.state)
}

/// True when `name` is `ready` in a release build. `test` is never
/// ready here: the API admits it only under `cfg(test)` (decision D9).
#[must_use]
pub fn is_kind_ready(name: &str) -> bool {
    kind_row(name).is_some_and(|row| row.state == KindState::Ready)
}

/// True when the map owns `name` and it is not `ready`.
#[must_use]
pub fn is_kind_refused(name: &str) -> bool {
    kind_row(name).is_some_and(|row| row.state != KindState::Ready)
}

/// Cargo feature required by `name`, when gated.
#[must_use]
pub fn kind_feature(name: &str) -> Option<&'static str> {
    kind_row(name).and_then(|row| row.feature)
}

/// Names of every ready kind with the given capability, in table order.
/// Used by the schema listings so they can never show a non-ready kind.
#[must_use]
pub fn ready_names(pick: fn(&KindRow) -> bool) -> Vec<&'static str> {
    KIND_ROWS
        .iter()
        .filter(|row| row.state == KindState::Ready && pick(row))
        .map(|row| row.name)
        .collect()
}

/// Render the `docs/reference/connector-kinds.md` table from the map, so
/// the document is generated rather than hand-maintained.
#[must_use]
pub fn render_markdown() -> String {
    let mut out = String::new();
    let _ = writeln!(out, "# Connector, action and source kinds");
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "Generated from the KC-00 capability map (`broker_config::capability`)."
    );
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "| kind | aliases | family | owner | state | connector | action | source | probe | SQL INTO | feature |"
    );
    let _ = writeln!(
        out,
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
    );
    for row in KIND_ROWS {
        let state = match row.state {
            KindState::Ready => "ready".to_string(),
            KindState::TestOnly => "test-only".to_string(),
            KindState::Pending(ticket) => format!("pending {ticket}"),
            KindState::Refused(decision) => format!("refused {decision}"),
        };
        let yes = |v: bool| if v { "yes" } else { "no" };
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
            row.name,
            row.aliases.join(", "),
            row.family,
            row.owner,
            state,
            yes(row.connector),
            yes(row.action),
            yes(row.source),
            yes(row.probe),
            yes(row.sql_into),
            row.feature.unwrap_or("-"),
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KNOWN_ACTION_TYPES, KNOWN_CONNECTOR_TYPES, KNOWN_SOURCE_TYPES};
    use std::collections::HashSet;

    /// KC-00: every name the broker accepts today resolves to exactly one
    /// row. The test fails if a new kind is added to a known list without a
    /// capability row, and if one alias is claimed by two rows.
    #[test]
    fn every_accepted_name_has_one_row() {
        let mut owners: HashSet<&'static str> = HashSet::new();
        for row in KIND_ROWS {
            assert!(
                owners.insert(row.name),
                "canonical {} is duplicated",
                row.name
            );
            for alias in row.aliases {
                assert!(
                    owners.insert(alias),
                    "alias {alias} is claimed by more than one row"
                );
            }
        }
        for name in KNOWN_CONNECTOR_TYPES
            .iter()
            .chain(KNOWN_ACTION_TYPES.iter())
            .chain(KNOWN_SOURCE_TYPES.iter())
        {
            assert!(
                kind_row(name).is_some(),
                "accepted name {name} has no capability row"
            );
        }
    }

    /// The map's readiness is the single source the refusal paths read.
    #[test]
    fn readiness_matches_the_decision() {
        assert!(is_kind_ready("http"));
        assert!(is_kind_ready("console"));
        assert!(is_kind_ready("logger"));
        assert!(is_kind_ready("kafka"));
        assert!(is_kind_ready("pgsql"));
        assert!(is_kind_ready("webhook"));
        assert!(!is_kind_ready("test"));
        assert_eq!(kind_state("s3_tables"), Some(KindState::Refused("MV-04")));
        assert_eq!(kind_state("postgres"), Some(KindState::Ready));
        assert_eq!(kind_state("snowflake"), Some(KindState::Ready));
        assert_eq!(kind_state("tablestore"), Some(KindState::Ready));
        assert_eq!(kind_state("databricks"), Some(KindState::Ready));
        assert_eq!(kind_state("no_such_kind"), None);
    }

    /// The reference document is checked against the map: every canonical
    /// kind and every alias must be listed, and every state named.
    #[test]
    fn reference_document_lists_every_kind() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../docs/reference/connector-kinds.md"
        );
        let on_disk = std::fs::read_to_string(path)
            .expect("docs/reference/connector-kinds.md is generated from the map");
        for row in KIND_ROWS {
            assert!(
                on_disk.contains(row.name),
                "docs must list canonical kind {}",
                row.name
            );
            for alias in row.aliases {
                assert!(on_disk.contains(alias), "docs must list alias {alias}");
            }
        }
        assert!(on_disk.contains("| ready |"), "docs must name ready states");
        assert!(on_disk.contains("refused D3"), "docs must name refusals");
        assert!(on_disk.contains("refused MV-04"), "docs must name refusals");
    }
}
