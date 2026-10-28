//! Kind capability map (KC-00).
//!
//! One row per canonical connector, action and source kind, with every
//! alias, the owning ticket, the required Cargo feature, the accepted
//! capabilities (connector create, action create, source create, probe,
//! SQL `INTO`) and the readiness state. The API, the schema listings,
//! the file loader, boot replay and the documents read this table; there
//! is no second kind list.
//!
//! A kind whose state is not `ready` must be refused at create, update
//! and probe with `connector type <kind> is not available in this
//! broker`, must be refused by a strict config file load, and must be
//! tolerated by the stored-state path so an operator can still read and
//! delete an old record. Only `ready` kinds may deliver through the v5
//! path. The v1 path and SQL `INTO` keep their old delivery.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, Ordering};

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
        state: KindState::Pending("MV-02a"),
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
        state: KindState::Pending("MV-03b"),
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
        state: KindState::Pending("MV-02d"),
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
        state: KindState::Pending("MV-03b"),
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
        state: KindState::Pending("MV-03b"),
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
        state: KindState::Pending("MV-02d"),
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
        aliases: &["cockroach"],
        family: "relational",
        owner: "MV-02a",
        state: KindState::Pending("MV-02a"),
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
        state: KindState::Pending("MV-03a"),
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
        state: KindState::Pending("MV-02c"),
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
        state: KindState::Pending("MV-02d"),
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
        aliases: &["disk", "disklog"],
        family: "object-store",
        owner: "MV-02d",
        state: KindState::Pending("MV-02d"),
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
        state: KindState::Pending("MV-02d"),
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
        state: KindState::Pending("MV-02c"),
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
        state: KindState::Pending("MV-03b"),
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
        state: KindState::Pending("MV-03b"),
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
        aliases: &["greptime"],
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
        state: KindState::Pending("MV-03c"),
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
        state: KindState::Pending("MV-03a"),
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
        state: KindState::Pending("MV-03b"),
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
        state: KindState::Pending("MV-02c"),
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
        state: KindState::Pending("MV-03a"),
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
        state: KindState::Pending("MV-02a"),
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
        state: KindState::Pending("MV-02a"),
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
        state: KindState::Pending("MV-03b"),
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
        state: KindState::Pending("MV-03c"),
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
        state: KindState::Pending("MV-02a"),
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
        state: KindState::Pending("MV-03a"),
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
        state: KindState::Pending("MV-03a"),
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
        state: KindState::Pending("MV-03a"),
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
        state: KindState::Pending("MV-02a"),
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
        state: KindState::Pending("MV-03a"),
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
        state: KindState::Pending("MV-02d"),
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
        state: KindState::Pending("MV-02d"),
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
        state: KindState::Pending("MV-03c"),
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
        state: KindState::Pending("MV-02d"),
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
        owner: "MV-02a",
        state: KindState::Pending("MV-02a"),
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
        action: false,
        source: false,
        probe: false,
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

static TEST_KINDS_ENABLED: AtomicBool = AtomicBool::new(false);

/// Family tickets whose kinds the plan accepts as `ready` at the close of
/// KC-00. Add a ticket only after its family contract is accepted. The
/// readiness test derives the expected state from this list.
pub const ACCEPTED_FAMILY_TICKETS: &[&str] = &["MV-01", "KC-00/D9", "MV-02b"];

/// Enable the test-only `test` kind for this process. Test builds call
/// this; a release build does not.
#[doc(hidden)]
pub fn set_test_kinds_enabled(enabled: bool) {
    TEST_KINDS_ENABLED.store(enabled, Ordering::Relaxed);
}

/// True when a test build enabled the `test` kind.
#[must_use]
pub fn test_kinds_enabled() -> bool {
    TEST_KINDS_ENABLED.load(Ordering::Relaxed)
}

/// True when the row may deliver in this build. `test` is ready only when
/// a test build enabled it (decision D9).
#[must_use]
pub fn row_is_ready(row: &KindRow) -> bool {
    match row.state {
        KindState::Ready => true,
        KindState::TestOnly => test_kinds_enabled(),
        KindState::Pending(_) | KindState::Refused(_) => false,
    }
}

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

/// True when `name` is `ready` in this build. `test` is ready only when a
/// test build enabled it (decision D9).
#[must_use]
pub fn is_kind_ready(name: &str) -> bool {
    kind_row(name).is_some_and(row_is_ready)
}

/// True when the map owns `name` and it is not `ready` in this build.
#[must_use]
pub fn is_kind_refused(name: &str) -> bool {
    kind_row(name).is_some_and(|row| !row_is_ready(row))
}

/// Cargo feature required by `name`, when gated.
#[must_use]
pub fn kind_feature(name: &str) -> Option<&'static str> {
    kind_row(name).and_then(|row| row.feature)
}

/// Render `docs/reference/connector-kinds.md` from the map, so the
/// document is generated rather than hand-maintained.
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
    let _ = writeln!(out, "```mermaid");
    let _ = writeln!(out, "flowchart LR");
    let _ = writeln!(
        out,
        "    Request[\"v5 create / update / probe\"] --> Map[\"KIND_ROWS capability map\"]"
    );
    let _ = writeln!(out, "    File[\"indra.toml / state.toml\"] --> Map");
    let _ = writeln!(out, "    Map --> Ready[\"ready: runtime path runs\"]");
    let _ = writeln!(out, "    Map --> Pending[\"pending ticket: refused\"]");
    let _ = writeln!(out, "    Map --> Refused[\"refused decision: removed\"]");
    let _ = writeln!(
        out,
        "    Ready --> Deliver[\"connector / action / source delivers\"]"
    );
    let _ = writeln!(out, "    Pending --> BadRequest[\"400 refusal\"]");
    let _ = writeln!(out, "    Refused --> BadRequest");
    let _ = writeln!(out, "```");
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
    let _ = writeln!(out);
    let _ = writeln!(out, "## States");
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "- `ready`: implemented with no invented target value and proved by tests. Only ready kinds may deliver through the v5 path. The v1 path and SQL `INTO` keep their old delivery."
    );
    let _ = writeln!(
        out,
        "- `pending <ticket>`: refused until the named family ticket sets the kind `ready`."
    );
    let _ = writeln!(
        out,
        "- `refused <decision>`: not kept for compatibility; D3 closed services and the MV-04 S3 Tables removal."
    );
    let _ = writeln!(
        out,
        "- `test-only`: the local `test` helper, present only in a test build (decision D9)."
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "## Behaviour of a kind that is not ready");
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "Connector, action and source create answer `400` for a kind that is not ready. The code is `ALREADY_EXISTS` when the connector credential fields pass. A missing credential answers `400 BAD_REQUEST` and names the field. Update answers `400 BAD_REQUEST`. Probe answers `400 TEST_FAILED`. A non-ready refusal carries `connector type <kind> is not available in this broker`. Nothing is stored. A refused update keeps the old record. A strict config file load fails and names the file, key and kind. A stored record from an earlier version is kept and visible. It reports `disconnected` with a `status_reason`. It raises `connector_unavailable:<id>` or `action_unavailable:<id>`. A rule that uses it stays enabled and counts the forward as failed. The schema, action-type and source-type listings show only ready kinds."
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KNOWN_ACTION_TYPES, KNOWN_CONNECTOR_TYPES, KNOWN_SOURCE_TYPES};
    use std::collections::HashSet;

    /// Names the v5 API accepts outside the three `KNOWN_*` registries.
    /// The list is copied from `live_sink_kind`, `typed.rs` and the kind
    /// matches in `v5/rules.rs` and `lib.rs`. Add a name here when one of
    /// those paths starts to accept it.
    const BROKER_API_ACCEPTED_NAMES: &[&str] = &[
        // live_sink_kind
        "mqtt",
        "kafka_producer",
        "kafka_consumer",
        "confluent_producer",
        "gcp_pubsub_producer",
        "gcp_pubsub_consumer",
        "azure_event_hub_producer",
        "aws_timestream",
        "opents",
        "snowflake_streaming",
        "postgres",
        "sqlserver",
        "dynamo",
        // typed.rs SPEC_ACTION_TYPES and SPEC_SOURCE_TYPES
        "timescale",
        "mongodb",
        "oracle",
        "pulsar",
        "tdengine",
        "azure_blob_storage",
        "elasticsearch",
        "influxdb",
        "cassandra",
        "cockroachdb",
        "kinesis",
        "iotdb",
        "mysql",
        "rabbitmq",
        "greptimedb",
        "disk_log",
        "datalayers",
        "http",
        "redis",
        "redshift",
        "clickhouse",
        "tablestore",
        "alloydb",
        "bigquery",
        "snowflake",
        "rocketmq",
        "s3",
        "doris",
        "couchbase",
        // rules.rs and lib.rs kind matches
        "amqp",
        "azblob",
        "azureblob",
        "bq",
        "cb",
        "ch",
        "confluent_cloud",
        "confluent_kafka",
        "couch",
        "cql",
        "documentdb",
        "elastic",
        "es",
        "gbq",
        "gcp",
        "pubsub",
        "rabbit",
        "bridge",
        "cockroach",
        "disk",
        "disklog",
        "greptime",
        "minio",
        "mongo",
        "scylla",
        "scylladb",
        "sparkplug",
        "opcua",
        "opensearch",
        "aws_iot_core",
        "azure",
        "azure_event_hubs",
        "eventhubs",
        "azure_iot_hub",
        "azure_iothub",
        "iothub",
        "gcp_iot_core",
        "cloud_iot",
        "delta_lake",
        "ots",
        "oci",
        "aws_kinesis",
        "ddb",
        "pgsql",
        "webhook",
        "console",
        "logger",
    ];

    /// KC-00: every name in the known lists and every name the v5 mapping
    /// functions and kind matches accept resolves to exactly one row. The
    /// test fails if a listed name has no row, and if one alias is claimed
    /// by two rows.
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
            .chain(BROKER_API_ACCEPTED_NAMES.iter())
            .chain(KIND_ROWS.iter().flat_map(|row| row.aliases.iter()))
        {
            assert!(
                kind_row(name).is_some(),
                "accepted name {name} has no capability row"
            );
        }
    }

    /// Every row's state follows from its owner ticket and the list of
    /// accepted family tickets. A kind of a family ticket that is not
    /// accepted can never read `ready`.
    #[test]
    fn readiness_matches_the_decision() {
        for row in KIND_ROWS {
            match row.state {
                KindState::Ready => assert!(
                    ACCEPTED_FAMILY_TICKETS.contains(&row.owner),
                    "{} is ready but its family ticket {} is not accepted",
                    row.name,
                    row.owner
                ),
                KindState::Pending(ticket) => {
                    assert_eq!(ticket, row.owner, "{} pending owner", row.name);
                    assert!(
                        !ACCEPTED_FAMILY_TICKETS.contains(&ticket),
                        "{} is pending under accepted ticket {}",
                        row.name,
                        ticket
                    );
                }
                KindState::Refused(_) | KindState::TestOnly => {}
            }
        }
        assert!(is_kind_ready("pgsql"));
        assert!(is_kind_ready("webhook"));
        assert!(is_kind_ready("console"));
        assert!(is_kind_ready("logger"));
        assert!(is_kind_ready("clickhouse"));
        assert!(is_kind_ready("tdengine"));
        assert!(is_kind_ready("greptimedb"));
        assert!(is_kind_ready("iotdb"));
        assert!(is_kind_ready("opentsdb"));
        assert!(is_kind_ready("doris"));
        assert!(is_kind_ready("datalayers"));
        assert!(is_kind_ready("influxdb"));
        assert!(is_kind_ready("timestream"));
        assert!(is_kind_ready("cassandra"));
        assert!(!is_kind_ready("http"));
        assert!(!is_kind_ready("kafka"));
        assert!(!is_kind_ready("mysql"));
        assert_eq!(kind_state("postgres"), Some(KindState::Ready));
        assert_eq!(kind_state("snowflake"), Some(KindState::Pending("MV-02d")));
        assert_eq!(kind_state("tablestore"), Some(KindState::Pending("MV-02d")));
        assert_eq!(kind_state("databricks"), Some(KindState::Pending("MV-02d")));
        assert_eq!(kind_state("s3_tables"), Some(KindState::Refused("MV-04")));
        assert_eq!(
            kind_state("azure_event_grid"),
            Some(KindState::Refused("D3"))
        );
        assert_eq!(kind_state("no_such_kind"), None);
    }

    /// A refused kind has no capability that could let it deliver.
    #[test]
    fn refused_kinds_have_no_capabilities() {
        for row in KIND_ROWS {
            if matches!(row.state, KindState::Refused(_)) {
                assert!(!row.connector, "{} connector", row.name);
                assert!(!row.action, "{} action", row.name);
                assert!(!row.source, "{} source", row.name);
                assert!(!row.probe, "{} probe", row.name);
                assert!(!row.sql_into, "{} SQL INTO", row.name);
            }
        }
    }

    /// D9: the `test` kind is ready only when a test build sets the
    /// runtime switch; a release build keeps it refused.
    #[test]
    fn test_kind_uses_a_runtime_switch() {
        assert!(!test_kinds_enabled());
        assert!(!is_kind_ready("test"));
        assert!(is_kind_refused("test"));
        set_test_kinds_enabled(true);
        assert!(is_kind_ready("test"));
        assert!(!is_kind_refused("test"));
        set_test_kinds_enabled(false);
        assert!(!is_kind_ready("test"));
    }

    /// The reference document equals the generator output, byte for byte.
    #[test]
    fn reference_document_equals_generated_markdown() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../docs/reference/connector-kinds.md"
        );
        let on_disk = std::fs::read_to_string(path)
            .expect("docs/reference/connector-kinds.md is generated from the map");
        assert_eq!(on_disk, render_markdown());
    }
}
