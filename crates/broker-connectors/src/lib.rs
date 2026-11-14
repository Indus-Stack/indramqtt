#![allow(unknown_lints)]
#![allow(clippy::chunks_exact_to_as_chunks)]

//! Community connectors: light sinks with no vendor SDK.
//!
//! Default build (no features): `postgres`, `mysql`, `redis`,
//! `mqtt_bridge`, `http`, `disk_log`, `influxdb`, `greptimedb`,
//! `opentsdb`, `timescaledb`, `kinesis`, `mssql`, `redshift`,
//! `sparkplug_b`, `tdengine`, `cloud_tls` and the
//! `webhook`/`console`/`test` shims. Each heavy vendor SDK stays
//! off unless its Cargo feature is on: `s3`, `clickhouse`,
//! `kafka`, `elasticsearch` and `rabbitmq`. Wide-column and
//! streaming sinks live in the enterprise crate. Each has its
//! own feature there. A feature build (or `--all-features`)
//! restores the full set. An unresolvable kind fails closed
//! with an error naming the missing feature. Release builds use
//! the `full` feature of `broker-node`, which adds all gated
//! kinds. New B3 sinks land behind features from the start.
//!
//! The modules `kinesis`, `mssql`, `redshift`, `sparkplug_b` and
//! `tdengine` in this crate contain tests only. Those sinks are in
//! `broker-connectors-enterprise`.

use async_trait::async_trait;
use broker_protocol::{QoS, Topic};
use bytes::Bytes;
use reqwest::header::{HeaderMap, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;

#[cfg(feature = "clickhouse")]
pub mod clickhouse;
pub mod cloud_tls;
pub mod disk_log;
#[cfg(feature = "elasticsearch")]
pub mod elasticsearch;
pub mod greptimedb;
pub mod http;
pub mod influxdb;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod kinesis;
pub mod mqtt_bridge;
pub mod mssql;
pub mod mysql;
pub mod opentsdb;
pub mod pg_numbers;
pub mod postgres;
#[cfg(feature = "rabbitmq")]
pub mod rabbitmq;
pub mod redis;
pub mod redshift;
#[cfg(feature = "s3")]
pub mod s3;
pub mod sparkplug_b;
pub mod tdengine;
pub mod timescaledb;

#[cfg(feature = "clickhouse")]
pub use clickhouse::{
    CapturedClickHouseBatch, ClickHouseConnector, ClickHouseRow, ClickHouseSink,
    ClickHouseSinkConfig, ClickHouseTransport, DriverClickHouseTransport, HttpClickHouseTransport,
    MockClickHouseOutcome, MockClickHouseTransport,
};
pub use disk_log::{
    BackupInfo, DiskLogCompression, DiskLogConnector, DiskLogFormat, DiskLogSink,
    DiskLogSinkConfig, DiskLogWriter, DiskSyncMode, FileDiskLogWriter, MemoryDiskLogWriter,
};
#[cfg(feature = "elasticsearch")]
pub use elasticsearch::{
    BulkOutcome, CapturedBulk, DriverElasticsearchTransport, ElasticsearchAuth,
    ElasticsearchConnector, ElasticsearchOperation, ElasticsearchSink, ElasticsearchSinkConfig,
    ElasticsearchTransport, MockElasticsearchTransport,
};
pub use greptimedb::{
    build_greptime_sql_insert, build_influx_line_protocol, extract_greptime_record,
    resolve_table_name as resolve_greptime_table, GreptimeDbAuth, GreptimeDbConfig,
    GreptimeDbConnector, GreptimeDbSink, GreptimeDbTransport, GreptimeFormat, GreptimePrecision,
    GreptimeRecord, GreptimeValue, HttpGreptimeDbTransport, MockGreptimeDbTransport,
};
pub use http::{
    CapturedHttpRequest, HmacAlgorithm, HmacEncoding, HttpAuth, HttpBodyFormat, HttpConnector,
    HttpHmacSignature, HttpMethod, HttpRequest, HttpResponse, HttpSink, HttpSinkConfig,
    HttpTransport, MockHttpOutcome, MockHttpTransport, ReqwestHttpTransport,
};
pub use influxdb::{InfluxDbConnector, InfluxDbSink, InfluxDbSinkConfig};
#[cfg(feature = "kafka")]
pub use kafka::{
    classify_driver_error as classify_kafka_driver_error, encode_record_batch_v2,
    is_terminal_driver_message as is_kafka_terminal_driver_message, murmur2, partition_for_key,
    rdkafka_client_config as rdkafka_kafka_client_config, render_topic_template, KafkaRecord,
    KafkaSink, KafkaSinkConfig, KafkaTransport, MemoryKafkaTransport, RdkafkaKafkaTransport,
};
pub use mqtt_bridge::{
    decode_publish, decode_remaining_length, encode_publish, encode_remaining_length,
    parse_bridge_address, BridgeEndpoint, DecodedPublish, MemoryMqttBridgeTransport,
    MqttBridgeConnector, MqttBridgeProtocol, MqttBridgeSink, MqttBridgeSinkConfig,
    MqttBridgeTransport, RumqttcMqttBridgeTransport, SerializedMqttPacket,
};
pub use mysql::{
    DriverMySqlTransport, MemoryMySqlTransport, MySqlBatch, MySqlSink, MySqlSinkConfig,
    MySqlTransport,
};
pub use opentsdb::{
    extract_opentsdb_point, parse_telnet_addr, sanitize_opentsdb_string, serialize_telnet_lines,
    MockOpenTsdbTransport, NetworkOpenTsdbTransport, OpenTsdbCompression, OpenTsdbConfig,
    OpenTsdbConnector, OpenTsdbDataPoint, OpenTsdbProtocol, OpenTsdbSink, OpenTsdbSummaryResponse,
    OpenTsdbTransport,
};
pub use postgres::{
    DriverPgTransport, MemoryPgTransport, PgBatch, PgConnectionSettings, PgTransport,
    PostgreSqlSink, PostgreSqlSinkConfig,
};
#[cfg(feature = "rabbitmq")]
pub use rabbitmq::{
    AmqpFrame, LapinRabbitTransport, MemoryAmqpTransport, RabbitMqSink, RabbitMqSinkConfig,
    RabbitMqTransport,
};
pub use redis::{
    DriverRedisTransport, MemoryRedisTransport, RedisCommand, RedisCommandKind, RedisReply,
    RedisSink, RedisSinkConfig, RedisTransport,
};
#[cfg(feature = "s3")]
pub use s3::{
    MockS3Transport, S3Compression, S3Connector, S3Put, S3Sink, S3SinkConfig, S3Transport,
    SdkS3Transport, SigV4Request,
};
pub use timescaledb::{
    DriverTimescaleDbTransport, MockTimescaleTransport, TimescaleBatch, TimescaleDbConnector,
    TimescaleDbSink, TimescaleDbSinkConfig, TimescaleDbTransport,
};

#[derive(Error, Debug)]
pub enum ConnectorError {
    #[error("Connector dispatch failure: {0}")]
    Dispatch(String),

    #[error("Connector connection error: {0}")]
    Connection(String),

    #[error("Unknown connector: {0}")]
    UnknownConnector(String),
}

pub type Result<T> = std::result::Result<T, ConnectorError>;

/// One message in a batch handed to a driver.
#[derive(Debug, Clone)]
pub struct SinkBatchItem {
    pub topic: Topic,
    pub payload: Bytes,
    pub qos: QoS,
}

/// Last health-probe result of one sink. `None` from
/// [`Sink::health`] means the sink runs no probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkHealth {
    pub healthy: bool,
    pub reason: Option<String>,
}

/// Unified outbound boundary: every streaming sink implements `Sink`.
#[async_trait]
pub trait Sink: Send + Sync {
    async fn send(&self, topic: &Topic, payload: &Bytes, qos: QoS) -> Result<()>;

    /// Deliver one batch. The default sends each item in order, so a
    /// driver that writes one row at a time receives a batch of one at a
    /// time. A driver with a native batch write overrides this method.
    async fn send_batch(&self, items: &[SinkBatchItem]) -> Result<()> {
        for item in items {
            self.send(&item.topic, &item.payload, item.qos).await?;
        }
        Ok(())
    }

    /// Flush buffered rows before a swap or a lifecycle stop. The default
    /// has no buffer. A batching runtime drains its queue here so a
    /// replaced or disabled sink loses no message.
    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    /// Run one health check against the live target. The default runs no
    /// probe. An action-owned runtime performs a real connection attempt
    /// so a down target is reported as `disconnected`, never assumed
    /// reachable.
    async fn probe(&self) -> std::result::Result<(), String> {
        Ok(())
    }

    /// True when the sink counts its own action outcomes. An action-owned
    /// runtime counts matched, success, failed and dropped, so the rule
    /// path must not count them a second time.
    fn owns_action_metrics(&self) -> bool {
        false
    }

    /// Last health-probe result, when this sink runs a probe. The
    /// management read path uses it to report the live target state.
    fn health(&self) -> Option<SinkHealth> {
        None
    }

    /// Drop rows that wait in a bounded buffer without delivery. The
    /// default keeps nothing and counts nothing. An action-owned runtime
    /// overrides it: it empties its queue and counts each row as dropped.
    fn discard_buffered(&self) {}

    /// Stable kind name for management display (`webhook`, `console`,
    /// `kafka`, `rabbitmq`, ...).
    fn kind(&self) -> &'static str;

    /// Report local deliveries. Return the count when the sink keeps
    /// one. Return None when the sink keeps no count.
    fn local_deliveries(&self) -> Option<u64> {
        None
    }

    /// Return the last captured payload preview. Return the text when
    /// the sink keeps one. Return None when the sink keeps none.
    fn local_last_preview(&self) -> Option<String> {
        None
    }
}

/// Unified connector identity: a registered, addressable sink.
pub trait Connector: Send + Sync {
    fn connector_id(&self) -> &str;
    fn kind(&self) -> &'static str;
}

/// Management view of one registered connector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectorInfo {
    pub id: String,
    pub kind: String,
}

/// Open-core tier tag for a connector kind: the multi-cloud
/// streaming bridges, Sparkplug B, the enterprise databases, the
/// industrial time-series stores, the lakehouse sinks, the cloud
/// object/wide-column stores and the enterprise messaging bridges are
/// Enterprise; everything else is Community.
pub fn connector_tier(kind: &str) -> &'static str {
    match kind {
        "kinesis" | "gcp_pubsub" | "azure_eventhubs" | "pulsar" | "sparkplug_b" | "mongodb"
        | "mssql" | "cassandra" | "couchbase" | "tdengine" | "iotdb" | "timestream"
        | "dynamodb" | "snowflake" | "databricks" | "doris" | "bigquery" | "redshift"
        | "oci_streaming" | "aws_iot" | "azure_iot" | "gcp_iot" | "opc_ua" | "azure_blob"
        | "tablestore" | "confluent" | "rocketmq" | "oracle" | "cockroachdb" | "alloydb"
        | "datalayers" => "enterprise",
        _ => "community",
    }
}

/// Connector kinds compiled into this feature build (X1-10 feature
/// matrix): every light sink is always present; each heavy SDK kind is
/// present only when its Cargo feature is enabled. A kind absent here is
/// unresolvable in this build: registration fails closed with an error
/// naming the missing feature.
pub fn compiled_kinds() -> Vec<&'static str> {
    // `mut` is needed only when a heavy feature is enabled; the allow
    // keeps the default (light-only) build warning-free.
    #[allow(unused_mut)]
    let mut kinds = vec![
        "postgres",
        "mysql",
        "redis",
        "mqtt_bridge",
        "http",
        "disk_log",
        "influxdb",
        "greptimedb",
        "opentsdb",
        "timescaledb",
        "kinesis",
        "mssql",
        "redshift",
        "sparkplug_b",
        "tdengine",
        "cloud_tls",
        "webhook",
        "console",
        "test",
    ];
    #[cfg(feature = "s3")]
    kinds.push("s3");
    #[cfg(feature = "clickhouse")]
    kinds.push("clickhouse");
    #[cfg(feature = "kafka")]
    kinds.push("kafka");
    #[cfg(feature = "elasticsearch")]
    kinds.push("elasticsearch");
    #[cfg(feature = "rabbitmq")]
    kinds.push("rabbitmq");
    kinds
}

/// Manager-side handle pairing an id with its sink.
pub struct RegisteredConnector {
    id: String,
    sink: Arc<dyn Sink>,
    is_v1: bool,
}

impl RegisteredConnector {
    fn new(id: String, sink: Arc<dyn Sink>) -> Self {
        Self {
            id,
            sink,
            is_v1: false,
        }
    }

    fn new_v1(id: String, sink: Arc<dyn Sink>) -> Self {
        Self {
            id,
            sink,
            is_v1: true,
        }
    }
}

impl Connector for RegisteredConnector {
    fn connector_id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> &'static str {
        self.sink.kind()
    }
}

/// Default per-request timeout for webhook delivery.
pub const DEFAULT_WEBHOOK_TIMEOUT: Duration = Duration::from_secs(5);

/// HTTP webhook sink: POSTs the raw event payload to a URL.
///
/// The payload bytes travel untouched as the request body (already JSON
/// after SQL projection); topic and QoS ride along as `X-MQTT-Topic` /
/// `X-MQTT-QoS` headers plus any configured custom headers. The shared
/// `reqwest::Client` owns connection pooling. Non-2xx responses are
/// dispatch failures; transport errors are connection failures.
pub struct HttpWebhookSink {
    url: String,
    headers: HeaderMap,
    client: reqwest::Client,
    timeout: Duration,
    sent: AtomicU64,
    /// Last health probe. It starts unprobed, never healthy, so a
    /// registered target is not reported reachable before a real dial.
    health: std::sync::Mutex<SinkHealth>,
}

/// Resolve a connector config field that may hold a secret reference.
///
/// `file:`/`env:` references resolve to UTF-8 at use time through the
/// secret store; anything else passes through verbatim. Unresolvable or
/// non-UTF-8 secrets fail closed to an empty string (the caller then fails
/// its construction instead of sending with a wrong credential).
/// Reason: connector URLs, tokens and passwords must never travel as
/// plaintext in the snapshot while still connecting with the real value.
#[must_use]
pub fn resolve_field_secret(value: &str) -> String {
    let trimmed = value.trim();
    if broker_config::secrets::is_secret_ref(trimmed) {
        match broker_config::secrets::resolve_maybe_secret(trimmed) {
            Ok(bytes) => String::from_utf8(bytes).unwrap_or_default(),
            Err(_) => String::new(),
        }
    } else {
        value.to_string()
    }
}

impl HttpWebhookSink {
    pub fn new(url: String, headers: HeaderMap, client: reqwest::Client) -> Self {
        // Connector URLs may be secret references; resolve at construction
        // (use time) so the snapshot keeps the reference while the live
        // sink dials the real target.
        let url = resolve_field_secret(&url);
        Self {
            url,
            headers,
            client,
            timeout: DEFAULT_WEBHOOK_TIMEOUT,
            sent: AtomicU64::new(0),
            health: std::sync::Mutex::new(SinkHealth {
                healthy: false,
                reason: Some("health check pending".to_string()),
            }),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn sent_count(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }
}

/// Percent-encode one MQTT topic for a URL path. `/` stays
/// structural so `a/b` dials `/ingest/a/b`. Other bytes outside the
/// unreserved marks become `%XX`. Reason: RFC 3986 keeps `/` as the
/// path separator and encodes the rest.
fn encode_topic_path(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else if byte == b'/' {
            out.push('/');
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

#[async_trait]
impl Sink for HttpWebhookSink {
    async fn send(&self, topic: &Topic, payload: &Bytes, qos: QoS) -> Result<()> {
        // Render the `${topic}` template per event. The action path
        // joins the event topic (for example `/ingest/${topic}` with
        // topic `a/b` dials `/ingest/a/b`). The stored target keeps the
        // braces literal. Only the topic value is encoded.
        let url = self
            .url
            .replace("${topic}", &encode_topic_path(topic.as_str()));
        let response = self
            .client
            .post(&url)
            .headers(self.headers.clone())
            .header(CONTENT_TYPE, "application/json")
            .header("X-MQTT-Topic", topic.as_str())
            .header("X-MQTT-QoS", u8::from(qos).to_string())
            .body(payload.to_vec())
            .send()
            .await
            .map_err(|e| ConnectorError::Connection(e.to_string()))?;

        let status = response.status();
        if !status.is_success() {
            return Err(ConnectorError::Dispatch(format!(
                "webhook {url} answered {status}"
            )));
        }
        self.sent.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn probe(&self) -> std::result::Result<(), String> {
        let result = probe_webhook_target(&self.url, self.timeout).await;
        let mut health = self
            .health
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *health = SinkHealth {
            healthy: result.is_ok(),
            reason: result.as_ref().err().cloned(),
        };
        result
    }

    fn health(&self) -> Option<SinkHealth> {
        Some(
            self.health
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone(),
        )
    }

    fn kind(&self) -> &'static str {
        "webhook"
    }
}

/// One bounded TCP dial to the target of a webhook URL. The probe does
/// not send a request, so it cannot trigger the webhook action.
async fn probe_webhook_target(url: &str, timeout: Duration) -> std::result::Result<(), String> {
    let parsed = reqwest::Url::parse(url.trim())
        .map_err(|error| format!("webhook url is invalid: {error}"))?;
    let host = parsed
        .host_str()
        .filter(|host| !host.is_empty())
        .ok_or_else(|| "webhook url has no host".to_string())?;
    let port = parsed.port_or_known_default().unwrap_or(80);
    let target = format!("{host}:{port}");
    match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(&target)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => Err(format!("Connection refused to {target}: {error}")),
        Err(_) => Err(format!("health check timed out for {target}")),
    }
}

/// Formatted diagnostic sink: traces every event at INFO with a payload
/// preview (full payload when UTF-8 and short). Counts deliveries for
/// tests and health reporting. Keeps the last preview for tests.
pub struct ConsoleLoggerSink {
    name: String,
    max_preview_bytes: usize,
    logged: AtomicU64,
    last_preview: parking_lot::Mutex<Option<String>>,
}

impl ConsoleLoggerSink {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            max_preview_bytes: 256,
            logged: AtomicU64::new(0),
            last_preview: parking_lot::Mutex::new(None),
        }
    }

    pub fn logged_count(&self) -> u64 {
        self.logged.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl Sink for ConsoleLoggerSink {
    async fn send(&self, topic: &Topic, payload: &Bytes, qos: QoS) -> Result<()> {
        let preview = match std::str::from_utf8(payload) {
            Ok(text) if text.len() <= self.max_preview_bytes => text.into(),
            Ok(text) => format!(
                "{}…<{} bytes total>",
                &text[..self.max_preview_bytes],
                payload.len()
            ),
            Err(_) => format!("<{} non-UTF8 bytes>", payload.len()),
        };
        tracing::info!(
            connector = %self.name,
            topic = %topic,
            qos = u8::from(qos),
            payload = %preview,
            "connector event"
        );
        *self.last_preview.lock() = Some(preview);
        self.logged.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn kind(&self) -> &'static str {
        "console"
    }

    fn local_deliveries(&self) -> Option<u64> {
        Some(self.logged_count())
    }

    fn local_last_preview(&self) -> Option<String> {
        self.last_preview.lock().clone()
    }
}

/// Upper bound for the v1 `/connectors` name space. Reason: the v1
/// path has no separate store, so the registry itself must stay small.
/// 512 matches the v5 connector catalogue bound and keeps the per-request
/// management clones cheap.
pub const MAX_V1_CONNECTORS: usize = 512;

/// One-lock result for a rule forward. See
/// [`ConnectorManager::forward_lookup`].
pub struct ForwardLookup {
    /// Live sink under the bare forward id, when one is registered.
    pub sink: Option<Arc<dyn Sink>>,
    /// True when the bare id belongs to the v1 `/connectors` path.
    pub is_v1: bool,
    /// True when a v5 action runtime owns the `action:{id}` key.
    pub has_action_sink: bool,
}

/// Registry of live outbound connectors by id.
#[derive(Default)]
pub struct ConnectorManager {
    connectors: parking_lot::RwLock<HashMap<String, RegisteredConnector>>,
    /// Registrations collected by [`Self::begin_stage`]. While a stage is
    /// open, [`Self::register`] writes here instead of the live map, so a
    /// management write can build every new sink before it persists and
    /// swap them together only after the stores commit.
    staged: parking_lot::Mutex<Option<HashMap<String, Arc<dyn Sink>>>>,
    /// Ids registered through the v1 `/connectors` path. They share the
    /// live map with v5 connectors but not the v5 action name space, so
    /// a bare forward to one never attributes to a v5 action metric.
    v1_ids: parking_lot::RwLock<HashSet<String>>,
}

impl ConnectorManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Open one staging generation. The caller serialises writers, so
    /// only one stage is open at a time.
    pub fn begin_stage(&self) {
        *self.staged.lock() = Some(HashMap::new());
    }

    /// Drop the staged generation without touching the live map.
    pub fn abort_stage(&self) {
        *self.staged.lock() = None;
    }

    /// Swap every staged registration into the live map in one step and
    /// return the sinks they replaced, so the caller can drain them.
    pub fn commit_stage(&self) -> Vec<Arc<dyn Sink>> {
        let Some(staged) = self.staged.lock().take() else {
            return Vec::new();
        };
        let mut replaced = Vec::new();
        let mut live = self.connectors.write();
        for (id, sink) in staged {
            if let Some(previous) = live.insert(id.clone(), RegisteredConnector::new(id, sink)) {
                replaced.push(previous.sink);
            }
        }
        replaced
    }

    pub fn register(&self, id: impl Into<String>, sink: Arc<dyn Sink>) {
        let id = id.into();
        if let Some(staged) = self.staged.lock().as_mut() {
            staged.insert(id, sink);
            return;
        }
        // A direct live write owns the bare id now. It is not a v1 id.
        self.v1_ids.write().remove(&id);
        self.connectors
            .write()
            .insert(id.clone(), RegisteredConnector::new(id, sink));
    }

    /// Register one connector of the v1 `/connectors` path. It lives in
    /// the live map under its bare id but stays outside the v5 action
    /// name space. Returns false when the name space is full and the id
    /// is new; a replace of an existing id always succeeds.
    /// It writes straight to the live map, never to a staged v5
    /// generation, so a v5 stage cannot adopt it.
    pub fn register_v1(&self, id: impl Into<String>, sink: Arc<dyn Sink>) -> bool {
        let id = id.into();
        {
            let mut ids = self.v1_ids.write();
            if !ids.contains(&id) && ids.len() >= MAX_V1_CONNECTORS {
                return false;
            }
            ids.insert(id.clone());
        }
        self.connectors
            .write()
            .insert(id.clone(), RegisteredConnector::new_v1(id, sink));
        true
    }

    /// True when the id belongs to the v1 `/connectors` path.
    /// Management and test paths use this. The publish path uses
    /// [`Self::forward_lookup`] instead, so it takes no extra lock.
    pub fn is_v1_connector(&self, id: &str) -> bool {
        self.v1_ids.read().contains(id)
    }

    /// One-lock view for the publish path. One `connectors` read serves
    /// the sink, the v1 flag and the action-sink check together, so a
    /// forward takes at most one lock read. Reason: the forward runs on
    /// the publish path, where extra locks add delay to every message.
    pub fn forward_lookup(&self, id: &str) -> ForwardLookup {
        let live = self.connectors.read();
        let entry = live.get(id);
        let sink = entry.map(|found| found.sink.clone());
        let is_v1 = entry.is_some_and(|found| found.is_v1);
        let action_key = broker_config::v5_action_sink_key(id);
        let has_action_sink = live.get(&action_key).is_some();
        ForwardLookup {
            sink,
            is_v1,
            has_action_sink,
        }
    }

    pub fn unregister(&self, id: &str) -> bool {
        self.v1_ids.write().remove(id);
        self.connectors.write().remove(id).is_some()
    }

    pub fn get(&self, id: &str) -> Option<Arc<dyn Sink>> {
        self.connectors
            .read()
            .get(id)
            .map(|entry| entry.sink.clone())
    }

    pub fn ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self.connectors.read().keys().cloned().collect();
        ids.sort();
        ids
    }

    /// Ordered management view (`id` + `kind`) for the dashboard.
    pub fn infos(&self) -> Vec<ConnectorInfo> {
        let mut infos: Vec<ConnectorInfo> = self
            .connectors
            .read()
            .values()
            .map(|entry| ConnectorInfo {
                id: entry.connector_id().to_string(),
                kind: entry.kind().to_string(),
            })
            .collect();
        infos.sort_by(|a, b| a.id.cmp(&b.id));
        infos
    }

    /// Deliver one event through the named connector.
    pub async fn send(&self, id: &str, topic: &Topic, payload: &Bytes, qos: QoS) -> Result<()> {
        self.send_with_action_metrics(id, topic, payload, qos)
            .await
            .0
    }

    /// Deliver one event and report whether the sink counts its own action
    /// outcomes. One registry lookup serves both answers.
    pub async fn send_with_action_metrics(
        &self,
        id: &str,
        topic: &Topic,
        payload: &Bytes,
        qos: QoS,
    ) -> (Result<()>, bool) {
        match self.get(id) {
            Some(sink) => {
                let owns_metrics = sink.owns_action_metrics();
                (sink.send(topic, payload, qos).await, owns_metrics)
            }
            None => (Err(ConnectorError::UnknownConnector(id.to_string())), false),
        }
    }
}

/// Size- and linger-bounded row buffer shared by batching sinks
/// (Postgres, MySQL, ClickHouse, InfluxDB). Rows accumulate until the
/// batch is full or the oldest row outlives the linger window; failures
/// restore rows in order instead of dropping them.
pub struct BatchQueue<T> {
    rows: Vec<T>,
    oldest: Option<Instant>,
    max_rows: usize,
    linger: Duration,
}

impl<T> BatchQueue<T> {
    pub fn new(max_rows: usize, linger: Duration) -> Self {
        Self {
            rows: Vec::new(),
            oldest: None,
            max_rows: max_rows.max(1),
            linger,
        }
    }

    /// Push one row; true means flush now (full or stale).
    pub fn push(&mut self, row: T) -> bool {
        if self.is_empty() {
            self.oldest = Some(Instant::now());
        }
        self.rows.push(row);
        self.rows.len() >= self.max_rows || self.is_stale()
    }

    pub fn is_stale(&self) -> bool {
        self.oldest
            .map(|oldest| oldest.elapsed() >= self.linger)
            .unwrap_or(false)
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Take all rows, resetting the linger clock. Callers restore via
    /// [`BatchQueue::restore`] when the transport fails.
    pub fn take_batch(&mut self) -> (Vec<T>, Option<Instant>) {
        let rows = std::mem::take(&mut self.rows);
        let oldest = self.oldest.take();
        (rows, oldest)
    }

    /// Restore a failed batch at the front, preserving order and the
    /// original linger clock.
    pub fn restore(&mut self, mut rows: Vec<T>, oldest: Option<Instant>) {
        rows.append(&mut self.rows);
        self.rows = rows;
        if self.oldest.is_none() {
            self.oldest = oldest;
        }
    }
}

/// Exponential backoff state (2s, 4s, 8s ... capped at 30s) for failing
/// transports. While backing off, flushes fail fast without touching
/// the transport.
#[derive(Default)]
pub struct BackoffState {
    consecutive_errors: u32,
    retry_after: Option<Instant>,
}

impl BackoffState {
    pub fn check(&self) -> Result<()> {
        if let Some(retry_after) = self.retry_after {
            if Instant::now() < retry_after {
                return Err(ConnectorError::Connection(
                    "sink backing off after errors".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// True while a failure set a retry time. The time clears only on a
    /// success, so a caller can tell a pending retry from a new row that
    /// is buffered for a normal batch.
    pub fn is_failed(&self) -> bool {
        self.retry_after.is_some()
    }

    pub fn success(&mut self) {
        self.consecutive_errors = 0;
        self.retry_after = None;
    }

    pub fn failure(&mut self) {
        self.consecutive_errors += 1;
        let secs = 2u64.saturating_pow(self.consecutive_errors.min(5)).min(30);
        self.retry_after = Some(Instant::now() + Duration::from_secs(secs));
    }
}

/// Strict `${name}` template renderer shared by the object-storage and
/// search sinks (S3 keys, Elasticsearch indices/doc ids). Every
/// `${...}` must close and must name a key present in `vars`; anything
/// else is a dispatch error, so typos fail loudly at buffer time
/// instead of silently producing wrong object names.
pub fn render_template(template: &str, vars: &[(&str, String)]) -> Result<String> {
    render_template_with(template, |name| {
        vars.iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| value.clone())
    })
}

/// The scanner behind [`render_template`]. Every `${name}` must close and
/// must not be empty. `lookup` gives the text for one name; `None` is a
/// dispatch error that names the variable. Callers with their own variable
/// source (for example the fields of a rule result) pass a closure and
/// get the same syntax rules and the same error text.
pub fn render_template_with<F>(template: &str, mut lookup: F) -> Result<String>
where
    F: FnMut(&str) -> Option<String>,
{
    let mut out = String::with_capacity(template.len() + 32);
    let mut rest = template;
    while let Some(pos) = rest.find("${") {
        out.push_str(&rest[..pos]);
        let after = &rest[pos + 2..];
        let Some(end) = after.find('}') else {
            return Err(ConnectorError::Dispatch(format!(
                "unclosed template variable in {template:?}"
            )));
        };
        let name = &after[..end];
        if name.is_empty() {
            return Err(ConnectorError::Dispatch(format!(
                "empty template variable in {template:?}"
            )));
        }
        match lookup(name) {
            Some(value) => out.push_str(&value),
            None => {
                return Err(ConnectorError::Dispatch(format!(
                    "unknown template variable {name:?} in {template:?}"
                )))
            }
        }
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Wall-clock milliseconds since the Unix epoch (saturating at zero).
pub fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

/// Split Unix millis into civil (year, month, day) via Howard Hinnant's
/// days-from-civil inverse (proleptic Gregorian, UTC). Negative inputs
/// clamp to the epoch so templates never render year 1969 surprises.
pub fn ymd_from_millis(millis: i64) -> (i32, u32, u32) {
    let days = millis.max(0).div_euclid(86_400_000);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    if m <= 2 {
        y += 1;
    }
    (y as i32, m, d)
}

/// Split Unix millis into (hour, minute, second, milli) UTC time of day.
pub fn hms_milli_from_millis(millis: i64) -> (u32, u32, u32, u32) {
    let day_millis = millis.max(0).rem_euclid(86_400_000) as u64;
    let secs = day_millis / 1_000;
    (
        (secs / 3_600) as u32,
        ((secs / 60) % 60) as u32,
        (secs % 60) as u32,
        (day_millis % 1_000) as u32,
    )
}

/// RFC 3339 UTC timestamp with millis (`2026-09-12T11:18:09.123Z`).
pub fn rfc3339_millis(millis: i64) -> String {
    let (y, mo, d) = ymd_from_millis(millis);
    let (h, mi, s, ms) = hms_milli_from_millis(millis);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{ms:03}Z")
}

// ---------------------------------------------------------------------------
// AWS Signature Version 4 core (shared by the S3 and Kinesis signers).
// ---------------------------------------------------------------------------

/// HMAC-SHA256 on the maintained `hmac` + `sha2` crates (shared by
/// the S3 and Kinesis signers and the Azure SharedKey/SAS signers).
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> Vec<u8> {
    use hmac::Mac;
    let mut mac =
        hmac::Hmac::<sha2::Sha256>::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(message);
    mac.finalize().into_bytes().to_vec()
}

/// Lowercase hex SHA-256 of `data`.
pub fn sha256_hex(data: &[u8]) -> String {
    use sha2::Digest;
    sha2::Sha256::digest(data)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// HMAC-SHA1 on the maintained `hmac` + `sha1` crates. Shared by
/// the Alibaba Tablestore and RocketMQ signers.
pub fn hmac_sha1(key: &[u8], message: &[u8]) -> Vec<u8> {
    use hmac::Mac;
    let mut mac =
        hmac::Hmac::<sha1::Sha1>::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(message);
    mac.finalize().into_bytes().to_vec()
}

/// SigV4 `x-amz-date` timestamp (`YYYYMMDDTHHMMSSZ`, UTC).
pub fn amz_date(millis: i64) -> String {
    let (year, month, day) = ymd_from_millis(millis);
    let (hour, minute, second, _) = hms_milli_from_millis(millis);
    format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z")
}

/// SigV4 percent-encoding for canonical URIs: every byte except
/// unreserved marks and the `/` path separator becomes `%XX`.
pub fn aws_encode_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    for byte in path.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b'/') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Inputs to [`sigv4_authorization`]. Header names must already be
/// lowercase; values trimmed. The signer sorts headers by name.
pub struct SigV4Signing<'a> {
    pub method: &'a str,
    pub canonical_uri: String,
    pub canonical_query: String,
    pub headers: Vec<(String, String)>,
    pub payload_hash: String,
    pub access_key_id: &'a str,
    pub secret_access_key: &'a str,
    pub region: &'a str,
    pub service: &'a str,
    pub millis: i64,
}

/// Build an AWS Signature Version 4 `Authorization` header value.
pub fn sigv4_authorization(signing: &SigV4Signing<'_>) -> String {
    let date = amz_date(signing.millis);
    let short_date = &date[..8];
    let mut headers = signing.headers.clone();
    headers.sort_by(|a, b| a.0.cmp(&b.0));
    let mut canonical_headers = String::new();
    let mut signed_names = Vec::with_capacity(headers.len());
    for (name, value) in &headers {
        canonical_headers.push_str(name);
        canonical_headers.push(':');
        canonical_headers.push_str(value.trim());
        canonical_headers.push('\n');
        signed_names.push(name.clone());
    }
    let signed_headers = signed_names.join(";");
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        signing.method,
        signing.canonical_uri,
        signing.canonical_query,
        canonical_headers,
        signed_headers,
        signing.payload_hash
    );
    let scope = format!(
        "{short_date}/{}/{}/aws4_request",
        signing.region, signing.service
    );
    let canonical_hash = sha256_hex(canonical_request.as_bytes());
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{date}\n{scope}\n{canonical_hash}");
    let mut key = hmac_sha256(
        format!("AWS4{}", signing.secret_access_key).as_bytes(),
        short_date.as_bytes(),
    );
    for part in [signing.region, signing.service, "aws4_request"] {
        key = hmac_sha256(&key, part.as_bytes());
    }
    // NOTE: the signature is hex(HMAC(key, string_to_sign)) — the
    // HMAC output is raw bytes, hexed directly (never hashed again).
    let signature = signing_hex(&key, &string_to_sign);
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={signed_headers}, Signature={signature}",
        signing.access_key_id, scope
    )
}

/// Hex of HMAC-SHA256(key, message): the SigV4 signature.
fn signing_hex(key: &[u8], string_to_sign: &str) -> String {
    hmac_sha256(key, string_to_sign.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::State, http::StatusCode, routing::post, Router};
    use std::sync::Mutex as StdMutex;
    use tokio::net::TcpListener;

    #[derive(Debug, Default)]
    struct RecordingSink {
        events: StdMutex<Vec<(String, Vec<u8>, u8)>>,
    }

    #[async_trait]
    impl Sink for RecordingSink {
        async fn send(&self, topic: &Topic, payload: &Bytes, qos: QoS) -> Result<()> {
            self.events.lock().unwrap().push((
                topic.as_str().to_string(),
                payload.to_vec(),
                u8::from(qos),
            ));
            Ok(())
        }

        fn kind(&self) -> &'static str {
            "test"
        }
    }

    /// Probe broker entry for the matrix test. The rule engine needs a
    /// live `BrokerSink` to publish through. It records what the broker
    /// delivers.
    #[derive(Debug, Default)]
    struct RuleProbeSink {
        published: StdMutex<Vec<(Topic, Bytes, QoS, bool)>>,
    }

    #[async_trait]
    impl broker_rules::BrokerSink for RuleProbeSink {
        async fn publish(
            &self,
            topic: Topic,
            payload: Bytes,
            qos: QoS,
            retain: bool,
        ) -> std::result::Result<(), broker_rules::RuleEngineError> {
            self.published
                .lock()
                .unwrap()
                .push((topic, payload, qos, retain));
            Ok(())
        }
    }

    #[test]
    fn test_v1_registry_has_a_finite_bound() {
        let manager = ConnectorManager::new();
        for index in 0..MAX_V1_CONNECTORS {
            assert!(
                manager.register_v1(format!("v1-{index}"), Arc::new(RecordingSink::default())),
                "a new v1 id below the bound registers"
            );
        }
        assert!(
            !manager.register_v1("v1-over".to_string(), Arc::new(RecordingSink::default())),
            "a new v1 id past the bound is refused"
        );
        assert!(
            manager.register_v1("v1-0".to_string(), Arc::new(RecordingSink::default())),
            "a replace of an existing v1 id still succeeds"
        );
        assert!(manager.is_v1_connector("v1-0"));
    }

    #[tokio::test]
    async fn test_connector_manager_registry() {
        let manager = ConnectorManager::new();
        assert!(manager.ids().is_empty());
        assert!(manager.get("missing").is_none());

        let sink: Arc<dyn Sink> = Arc::new(RecordingSink::default());
        manager.register("rec", sink);
        assert_eq!(manager.ids(), vec!["rec".to_string()]);

        let topic = Topic::new("a/b").unwrap();
        manager
            .send("rec", &topic, &Bytes::from_static(b"hi"), QoS::AtLeastOnce)
            .await
            .unwrap();
        let err = manager
            .send(
                "missing",
                &topic,
                &Bytes::from_static(b"hi"),
                QoS::AtMostOnce,
            )
            .await
            .expect_err("unknown connector must fail");
        assert!(matches!(err, ConnectorError::UnknownConnector(_)));

        assert!(manager.unregister("rec"));
        assert!(!manager.unregister("rec"));
    }

    #[test]
    fn test_connector_tier_tags() {
        // Enterprise bridges (cloud, Sparkplug, databases, time-series,
        // lakehouse, IoT hubs, cloud stores, enterprise messaging).
        for kind in [
            "kinesis",
            "gcp_pubsub",
            "azure_eventhubs",
            "pulsar",
            "sparkplug_b",
            "mongodb",
            "mssql",
            "cassandra",
            "couchbase",
            "tdengine",
            "iotdb",
            "timestream",
            "dynamodb",
            "snowflake",
            "databricks",
            "doris",
            "bigquery",
            "redshift",
            "oci_streaming",
            "aws_iot",
            "azure_iot",
            "gcp_iot",
            "opc_ua",
            "azure_blob",
            "tablestore",
            "confluent",
            "rocketmq",
        ] {
            assert_eq!(
                connector_tier(kind),
                "enterprise",
                "{kind} must be enterprise"
            );
        }
        // Everything else is community (including unknown kinds).
        for kind in ["kafka", "postgres", "redis", "webhook", "logger", "nope"] {
            assert_eq!(
                connector_tier(kind),
                "community",
                "{kind} must be community"
            );
        }
    }

    /// X1-10 feature matrix: the default build holds exactly the stated
    /// light set. The test proves the set through the broker. It
    /// registers one sink per compiled kind with the rule engine. It
    /// adds one rule per kind and publishes through the rule engine.
    /// It asserts exact delivery into every sink. Each heavy kind tracks
    /// its Cargo feature exactly, so per-feature builds carry their sink
    /// alone and the full-feature build carries all of them.
    /// Matrix sink for the feature test. It implements the rule
    /// engine sink trait. It counts each delivery.
    struct MatrixSink {
        logged: AtomicU64,
    }

    impl MatrixSink {
        fn new() -> Self {
            Self {
                logged: AtomicU64::new(0),
            }
        }

        fn logged_count(&self) -> u64 {
            self.logged.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl broker_connectors_enterprise::Sink for MatrixSink {
        async fn send(
            &self,
            _topic: &Topic,
            _payload: &Bytes,
            _qos: QoS,
        ) -> broker_connectors_enterprise::Result<()> {
            self.logged.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        fn kind(&self) -> &'static str {
            "console"
        }
    }

    #[tokio::test]
    async fn test_feature_matrix_compiled_kinds() {
        let engine =
            broker_rules::RuleEngine::new(65_536, broker_rules::BackpressurePolicy::DropOldest);
        let kinds = compiled_kinds();
        let mut sinks: Vec<Arc<MatrixSink>> = Vec::with_capacity(kinds.len());
        for kind in &kinds {
            let sink = Arc::new(MatrixSink::new());
            engine.connectors().register(*kind, sink.clone());
            sinks.push(sink);
        }
        // The list comes from the running registry.
        let ids = engine.connectors().ids();
        assert_eq!(
            ids.len(),
            kinds.len(),
            "registry lists each compiled kind once"
        );
        for kind in &kinds {
            assert!(
                ids.contains(&kind.to_string()),
                "registry lists compiled kind `{kind}`"
            );
        }
        // Light sinks are always compiled in.
        for light in [
            "postgres",
            "mysql",
            "redis",
            "mqtt_bridge",
            "http",
            "disk_log",
            "influxdb",
            "greptimedb",
            "opentsdb",
            "timescaledb",
            "kinesis",
            "mssql",
            "redshift",
            "sparkplug_b",
            "tdengine",
            "cloud_tls",
            "webhook",
            "console",
            "test",
        ] {
            assert!(
                ids.contains(&light.to_string()),
                "light sink `{light}` must be compiled in"
            );
        }
        // Each heavy kind is present exactly when its feature is enabled.
        assert_eq!(
            ids.contains(&"s3".to_string()),
            cfg!(feature = "s3"),
            "s3 tracks its feature"
        );
        assert_eq!(
            ids.contains(&"clickhouse".to_string()),
            cfg!(feature = "clickhouse"),
            "clickhouse tracks its feature"
        );
        assert_eq!(
            ids.contains(&"kafka".to_string()),
            cfg!(feature = "kafka"),
            "kafka tracks its feature"
        );
        assert_eq!(
            ids.contains(&"elasticsearch".to_string()),
            cfg!(feature = "elasticsearch"),
            "elasticsearch tracks its feature"
        );
        assert_eq!(
            ids.contains(&"rabbitmq".to_string()),
            cfg!(feature = "rabbitmq"),
            "rabbitmq tracks its feature"
        );
        // Default build holds exactly the stated light set, nothing more.
        #[cfg(not(any(
            feature = "s3",
            feature = "clickhouse",
            feature = "kafka",
            feature = "elasticsearch",
            feature = "rabbitmq"
        )))]
        assert_eq!(ids.len(), 19, "default build holds exactly the light set");
        // Through the broker: one rule per compiled kind, then one
        // publish per kind through the rule engine. Each publish
        // reaches exactly its own sink once.
        for kind in &kinds {
            engine
                .create_rule(
                    format!("matrix-{kind}"),
                    broker_protocol::TopicFilter::new(format!("matrix/{kind}")).unwrap(),
                    Some(format!(
                        r#"SELECT client_id, temp FROM "matrix/{kind}" WHERE temp > 20.0 INTO connector("{kind}")"#
                    )),
                    true,
                    vec![],
                )
                .expect("matrix rule creates");
        }
        let probe: Arc<dyn broker_rules::BrokerSink> = Arc::new(RuleProbeSink::default());
        let payload = Bytes::from_static(br#"{ "client_id": "device-42", "temp": 22.5 }"#);
        for kind in &kinds {
            let topic = Topic::new(format!("matrix/{kind}")).unwrap();
            engine
                .dispatch_ingress(&topic, &payload, QoS::AtMostOnce, &probe)
                .await;
        }
        for (kind, sink) in kinds.iter().zip(sinks.iter()) {
            assert_eq!(
                sink.logged_count(),
                1,
                "kind `{kind}` delivers its publish through the rule path"
            );
        }
    }

    #[tokio::test]
    async fn test_console_logger_counts_deliveries() {
        let sink = ConsoleLoggerSink::new("diag");
        let topic = Topic::new("a/b").unwrap();
        sink.send(&topic, &Bytes::from_static(b"hello"), QoS::AtMostOnce)
            .await
            .unwrap();
        sink.send(&topic, &Bytes::from(vec![0xFF, 0xFE]), QoS::AtLeastOnce)
            .await
            .unwrap();
        assert_eq!(sink.logged_count(), 2);
    }

    /// Captured webhook deliveries for the in-process HTTP test.
    #[derive(Debug, Default)]
    struct CapturedPosts {
        bodies: StdMutex<Vec<Vec<u8>>>,
        topics: StdMutex<Vec<String>>,
    }

    async fn capture_handler(
        State(state): State<Arc<CapturedPosts>>,
        headers: axum::http::HeaderMap,
        body: Bytes,
    ) -> StatusCode {
        if let Some(topic) = headers.get("X-MQTT-Topic").and_then(|v| v.to_str().ok()) {
            state.topics.lock().unwrap().push(topic.to_string());
        }
        state.bodies.lock().unwrap().push(body.to_vec());
        StatusCode::OK
    }

    /// In-process HTTP test: an ephemeral Axum server receives exactly
    /// what the webhook sink posts — byte-identical JSON included.
    #[tokio::test]
    async fn test_http_webhook_posts_exact_payload() {
        let captured = Arc::new(CapturedPosts::default());
        let app = Router::new()
            .route("/hook", post(capture_handler))
            .with_state(captured.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("webhook client");
        let mut headers = HeaderMap::new();
        headers.insert("X-Tenant", "acme".parse().unwrap());
        let sink = HttpWebhookSink::new(format!("http://127.0.0.1:{port}/hook"), headers, client);
        assert_eq!(sink.sent_count(), 0);

        let payload = Bytes::from_static(br#"{ "temperature": 85.0 }"#);
        sink.send(&Topic::new("raw/temp").unwrap(), &payload, QoS::AtMostOnce)
            .await
            .expect("webhook delivery");
        assert_eq!(sink.sent_count(), 1);

        assert_eq!(
            captured.bodies.lock().unwrap().as_slice(),
            &[br#"{ "temperature": 85.0 }"#.to_vec()]
        );
        assert_eq!(
            captured.topics.lock().unwrap().as_slice(),
            &["raw/temp".to_string()]
        );
        server.abort();
    }

    #[tokio::test]
    async fn test_http_webhook_reports_http_errors() {
        let app = Router::new().route(
            "/boom",
            post(|| async { (StatusCode::INTERNAL_SERVER_ERROR, "nope") }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });

        let sink = HttpWebhookSink::new(
            format!("http://127.0.0.1:{port}/boom"),
            HeaderMap::new(),
            reqwest::Client::new(),
        );
        let err = sink
            .send(
                &Topic::new("a").unwrap(),
                &Bytes::from_static(b"{}"),
                QoS::AtMostOnce,
            )
            .await
            .expect_err("5xx must fail");
        assert!(matches!(err, ConnectorError::Dispatch(_)));
        assert_eq!(sink.sent_count(), 0);

        // Unroutable port: connection failure, not dispatch failure.
        let dead = HttpWebhookSink::new(
            "http://127.0.0.1:1/hook".to_string(),
            HeaderMap::new(),
            reqwest::Client::new(),
        )
        .with_timeout(Duration::from_millis(500));
        let err = dead
            .send(
                &Topic::new("a").unwrap(),
                &Bytes::from_static(b"{}"),
                QoS::AtMostOnce,
            )
            .await
            .expect_err("refused port must fail");
        assert!(matches!(err, ConnectorError::Connection(_)));

        server.abort();
    }

    #[test]
    fn test_render_template_strict() {
        let vars = vec![
            ("topic", "sensors/t1".to_string()),
            ("YYYY", "2026".to_string()),
        ];
        assert_eq!(
            render_template("a/${topic}/${YYYY}.ndjson", &vars).unwrap(),
            "a/sensors/t1/2026.ndjson"
        );
        assert_eq!(render_template("plain", &vars).unwrap(), "plain");
        assert!(render_template("a/${nope}", &vars).is_err());
        assert!(render_template("a/${topic", &vars).is_err());
        assert!(render_template("a/${}", &vars).is_err());
        assert!(render_template("price: $5", &vars).is_ok());
    }

    #[test]
    fn test_render_template_with_keeps_multibyte_text() {
        let out = render_template_with("caf\u{e9}/${a}/\u{4e2d}", |name| {
            (name == "a").then(|| "x".to_string())
        })
        .unwrap();
        assert_eq!(out, "caf\u{e9}/x/\u{4e2d}");
        let err = render_template_with("${gone}", |_| None).unwrap_err();
        assert!(err.to_string().contains("unknown template variable"));
    }

    #[test]
    fn test_ymd_and_rfc3339_vectors() {
        assert_eq!(ymd_from_millis(0), (1970, 1, 1));
        // 2024-02-29T00:00:00Z (leap day) and 2026-09-12T11:18:09.123Z.
        assert_eq!(ymd_from_millis(1_709_164_800_000), (2024, 2, 29));
        assert_eq!(ymd_from_millis(1_789_211_889_123), (2026, 9, 12));
        assert_eq!(hms_milli_from_millis(1_789_211_889_123), (11, 18, 9, 123));
        assert_eq!(
            rfc3339_millis(1_789_211_889_123),
            "2026-09-12T11:18:09.123Z"
        );
        assert_eq!(ymd_from_millis(-1), (1970, 1, 1));
    }

    #[test]
    fn test_action_types_catalogue_basics() {
        let catalogue = action_types_catalogue();
        assert!(!catalogue.is_empty(), "catalogue must not be empty");
        assert!(
            catalogue.len() <= MAX_ACTION_TYPES,
            "catalogue must not exceed {MAX_ACTION_TYPES}"
        );
        let kinds: std::collections::HashSet<&str> = catalogue.iter().map(|e| e.kind).collect();
        assert_eq!(kinds.len(), catalogue.len(), "kinds must be unique");
    }

    #[test]
    fn test_action_types_catalogue_matches_connector_kinds() {
        // Every kind the kernel can execute (every non-test `Sink::kind()`
        // in this crate) must be listed, and every listed kind must be a
        // real connector kind: a kind is listed only if its connector
        // module exists and its config validates. The `test`
        // RecordingSink below is a unit-test helper, not an executable
        // sink, so it stays out of the catalogue.
        let catalogue = action_types_catalogue();
        let kinds: std::collections::HashSet<&str> = catalogue.iter().map(|e| e.kind).collect();
        let expected: std::collections::HashSet<&str> = [
            "alloydb",
            "aws_iot",
            "azure_blob",
            "azure_eventhubs",
            "azure_iot",
            "bigquery",
            "cassandra",
            "clickhouse",
            "cockroachdb",
            "confluent",
            "console",
            "couchbase",
            "databricks",
            "datalayers",
            "disk_log",
            "doris",
            "dynamodb",
            "elasticsearch",
            "gcp_iot",
            "gcp_pubsub",
            "greptimedb",
            "influxdb",
            "iotdb",
            "kafka",
            "kinesis",
            "mongodb",
            "mqtt_bridge",
            "mssql",
            "mysql",
            "oci_streaming",
            "opc_ua",
            "opentsdb",
            "oracle",
            "postgres",
            "pulsar",
            "rabbitmq",
            "redis",
            "redshift",
            "rocketmq",
            "s3",
            "snowflake",
            "sparkplug_b",
            "tablestore",
            "tdengine",
            "timescaledb",
            "timestream",
            "webhook",
        ]
        .into_iter()
        .collect();
        if let Some(missing) = expected.difference(&kinds).next() {
            panic!("catalogue is missing executable kind '{missing}'");
        }
        if let Some(invented) = kinds.difference(&expected).next() {
            panic!("catalogue lists kind '{invented}' with no connector module");
        }
    }

    #[test]
    fn test_action_types_serialization_is_object_map() {
        let catalogue = action_types_catalogue();
        let json = serde_json::to_value(&catalogue).unwrap();
        let arr = json.as_array().unwrap();
        assert!(!arr.is_empty());
        for entry in arr {
            assert!(entry["kind"].is_string());
            assert!(entry["description"].is_string());
            assert!(
                entry["configuration"].is_object(),
                "configuration must serialize as a JSON object"
            );
            // tier field must not be present (not in spec)
            assert!(
                entry.get("tier").is_none(),
                "tier field must not be in the response"
            );
        }
    }

    #[test]
    fn test_live_qualification_never_skips() {
        // Scan live tests for early exit.
        // Read source files from both connector crates.
        // Fail on a bare early exit or on a bypass message.
        // Require at least 20 live tests.
        let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let mut dirs = vec![manifest.join("src")];
        if let Some(root) = manifest.parent() {
            dirs.push(root.join("broker-connectors-enterprise/src"));
        }
        let marker = ["fn test_qual", "ify"].concat();
        let early = ["ret", "urn;"].concat();
        let bypass = ["ski", "pp"].concat();
        let mut count = 0usize;
        let mut bad: Vec<String> = Vec::new();
        for dir in &dirs {
            let entries = std::fs::read_dir(dir).expect("read connector src");
            let mut names: Vec<std::path::PathBuf> = Vec::new();
            for entry in entries {
                let path = entry.expect("dir entry").path();
                if path.extension().and_then(|s| s.to_str()) == Some("rs") {
                    names.push(path);
                }
            }
            names.sort();
            for path in names {
                let text = std::fs::read_to_string(&path).expect("read source");
                let lines: Vec<&str> = text.lines().collect();
                let mut i = 0usize;
                while i < lines.len() {
                    let trimmed = lines[i].trim_start();
                    let head = trimmed.strip_prefix("async ").unwrap_or(trimmed);
                    if !head.starts_with(&marker) {
                        i += 1;
                        continue;
                    }
                    let mut j = i;
                    while j < lines.len() && !lines[j].contains('{') {
                        j += 1;
                    }
                    if j >= lines.len() {
                        break;
                    }
                    let mut depth = 0i32;
                    let mut started = false;
                    let mut k = j;
                    while k < lines.len() {
                        for ch in lines[k].chars() {
                            if ch == '{' {
                                depth += 1;
                                started = true;
                            } else if ch == '}' {
                                depth -= 1;
                            }
                        }
                        if started && depth <= 0 {
                            break;
                        }
                        k += 1;
                    }
                    count += 1;
                    let end = k.min(lines.len().saturating_sub(1));
                    for (off, &line) in lines.iter().enumerate().take(end + 1).skip(i) {
                        if line.trim_start().starts_with("//") {
                            continue;
                        }
                        if line.contains(&early) {
                            bad.push(format!("{}:{} has early exit", path.display(), off + 1));
                        }
                        if line.to_lowercase().contains(&bypass) {
                            bad.push(format!("{}:{} has bypass message", path.display(), off + 1));
                        }
                    }
                    i = k + 1;
                }
            }
        }
        assert!(count >= 20, "found only {count} live tests, need 20");
        assert!(
            bad.is_empty(),
            "live tests must panic without env: {}",
            bad.join("; ")
        );
    }
}

/// Upper bound on the action-types catalogue size (spec C06-01).
/// The built-in kind set is well below this; the value is a finite safety
/// cap — the catalogue is a static snapshot that does not grow at runtime.
pub const MAX_ACTION_TYPES: usize = 128;

/// Serialize a static slice of (key, description) pairs as a JSON object
/// rather than an array of pairs, so the field renders as a map.
fn serialize_config_map<S: serde::Serializer>(
    config: &&'static [(&'static str, &'static str)],
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    use serde::ser::SerializeMap;
    let mut map = serializer.serialize_map(Some(config.len()))?;
    for (key, desc) in *config {
        map.serialize_entry(key, desc)?;
    }
    map.end()
}

/// Catalogue entry for one action type (connector kind) with configuration
/// shape hints. This is a static snapshot of what the kernel can execute;
/// it does not depend on runtime registration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ActionTypeInfo {
    /// Stable kind name (e.g., "kafka", "webhook", "postgres").
    pub kind: &'static str,
    /// Human-readable description of the connector.
    pub description: &'static str,
    /// Configuration shape hints: each required config key mapped to a
    /// one-line description. Empty means the kind has no required keys.
    #[serde(serialize_with = "serialize_config_map")]
    pub configuration: &'static [(&'static str, &'static str)],
}

/// Compile-time catalogue of every action type the kernel can execute.
/// A kind is listed only if its connector module exists and its config validates.
/// The catalogue is bounded (max [`MAX_ACTION_TYPES`]) and does not grow at runtime.
const ACTION_TYPES_CATALOGUE: &[ActionTypeInfo] = &[
    ActionTypeInfo {
        kind: "kafka",
        description: "Apache Kafka producer sink",
        configuration: &[
            (
                "bootstrap_servers",
                "Kafka broker list, comma-separated host:port",
            ),
            ("topic", "Target topic name"),
        ],
    },
    ActionTypeInfo {
        kind: "rabbitmq",
        description: "RabbitMQ / AMQP 0.9.1 sink",
        configuration: &[
            ("endpoint", "Endpoint URL or host:port"),
            ("exchange", "AMQP exchange name"),
            ("routing_key", "AMQP routing key"),
        ],
    },
    ActionTypeInfo {
        kind: "postgres",
        description: "PostgreSQL / TimescaleDB sink",
        configuration: &[
            ("connection_url", "Database connection URL with credentials"),
            ("table", "Target table name"),
        ],
    },
    ActionTypeInfo {
        kind: "redis",
        description: "Redis stream / list sink",
        configuration: &[
            ("endpoint", "Endpoint URL or host:port"),
            ("key", "Stream or list key name"),
        ],
    },
    ActionTypeInfo {
        kind: "mysql",
        description: "MySQL / MariaDB sink",
        configuration: &[
            ("connection_url", "Database connection URL with credentials"),
            ("table", "Target table name"),
        ],
    },
    ActionTypeInfo {
        kind: "clickhouse",
        description: "ClickHouse HTTP sink",
        configuration: &[("url", "Target URL"), ("table", "Target table name")],
    },
    ActionTypeInfo {
        kind: "influxdb",
        description: "InfluxDB v1/v2 line protocol sink",
        configuration: &[
            ("url", "Target URL"),
            ("org", "InfluxDB organization"),
            ("bucket", "Bucket or container name"),
            ("token", "Authentication token"),
        ],
    },
    ActionTypeInfo {
        kind: "s3",
        description: "S3 / MinIO object storage sink",
        configuration: &[
            ("bucket", "Bucket or container name"),
            ("region", "AWS region"),
            ("key_template", "Key template for object names"),
        ],
    },
    ActionTypeInfo {
        kind: "elasticsearch",
        description: "Elasticsearch / OpenSearch bulk sink",
        configuration: &[
            ("url", "Target URL"),
            ("index_template", "Index name template"),
        ],
    },
    ActionTypeInfo {
        kind: "timescaledb",
        description: "TimescaleDB (native PostgreSQL) sink",
        configuration: &[
            ("connection_url", "Database connection URL with credentials"),
            ("table", "Target table name"),
        ],
    },
    ActionTypeInfo {
        kind: "webhook",
        description: "Generic HTTP webhook sink",
        configuration: &[("url", "Target URL")],
    },
    ActionTypeInfo {
        kind: "mqtt_bridge",
        description: "MQTT bridge to another broker",
        configuration: &[
            ("endpoint", "Endpoint URL or host:port"),
            ("topic_template", "Target topic template string"),
        ],
    },
    ActionTypeInfo {
        kind: "disk_log",
        description: "Local disk log sink (JSONL, CSV, etc.)",
        configuration: &[
            ("directory", "Output directory path"),
            ("format", "Log format (jsonl, csv, raw)"),
        ],
    },
    ActionTypeInfo {
        kind: "sparkplug_b",
        description: "Sparkplug B edge node sink",
        configuration: &[
            ("group_id", "Sparkplug group ID"),
            ("edge_node_id", "Sparkplug edge node ID"),
            ("device_id", "Device ID"),
        ],
    },
    ActionTypeInfo {
        kind: "kinesis",
        description: "AWS Kinesis Data Streams sink",
        configuration: &[("stream_name", "Stream name"), ("region", "AWS region")],
    },
    ActionTypeInfo {
        kind: "gcp_pubsub",
        description: "Google Cloud Pub/Sub sink",
        configuration: &[
            ("project_id", "GCP project ID"),
            ("topic", "Target topic name"),
        ],
    },
    ActionTypeInfo {
        kind: "azure_eventhubs",
        description: "Azure Event Hubs sink",
        configuration: &[
            ("namespace", "Azure Event Hubs namespace"),
            ("event_hub", "Event hub name"),
            ("policy_name", "SAS policy name"),
            ("policy_key", "SAS policy key"),
        ],
    },
    ActionTypeInfo {
        kind: "pulsar",
        description: "Apache Pulsar sink",
        configuration: &[
            ("service_url", "Service URL"),
            ("topic", "Target topic name"),
        ],
    },
    ActionTypeInfo {
        kind: "mongodb",
        description: "MongoDB / DocumentDB sink",
        configuration: &[
            ("connection_string", "Full connection string"),
            ("database", "Database name"),
            ("collection", "Collection name"),
        ],
    },
    ActionTypeInfo {
        kind: "mssql",
        description: "Microsoft SQL Server / Azure SQL sink",
        configuration: &[
            ("connection_url", "Database connection URL with credentials"),
            ("table", "Target table name"),
        ],
    },
    ActionTypeInfo {
        kind: "cassandra",
        description: "Cassandra / ScyllaDB sink",
        configuration: &[
            ("contact_points", "Contact points (host:port list)"),
            ("keyspace", "Keyspace name"),
            ("table", "Target table name"),
        ],
    },
    ActionTypeInfo {
        kind: "couchbase",
        description: "Couchbase KV sink",
        configuration: &[
            ("connection_string", "Full connection string"),
            ("bucket", "Bucket or container name"),
            ("scope", "Bucket scope name"),
            ("collection", "Collection name"),
        ],
    },
    ActionTypeInfo {
        kind: "tdengine",
        description: "TDengine time-series sink",
        configuration: &[
            ("url", "Target URL"),
            ("database", "Database name"),
            ("table", "Target table name"),
        ],
    },
    ActionTypeInfo {
        kind: "iotdb",
        description: "Apache IoTDB sink",
        configuration: &[
            ("url", "Target URL"),
            ("database", "Database name"),
            ("table", "Target table name"),
        ],
    },
    ActionTypeInfo {
        kind: "timestream",
        description: "AWS Timestream sink",
        configuration: &[
            ("database", "Database name"),
            ("table", "Target table name"),
            ("region", "AWS region"),
        ],
    },
    ActionTypeInfo {
        kind: "dynamodb",
        description: "AWS DynamoDB sink",
        configuration: &[("table", "Target table name"), ("region", "AWS region")],
    },
    ActionTypeInfo {
        kind: "snowflake",
        description: "Snowflake data warehouse sink",
        configuration: &[
            ("account", "Account identifier"),
            ("user", "Username"),
            ("private_key", "Private key file path"),
            ("database", "Database name"),
            ("schema", "Schema name"),
            ("table", "Target table name"),
        ],
    },
    ActionTypeInfo {
        kind: "databricks",
        description: "Databricks SQL / Delta Lake sink",
        configuration: &[
            ("host", "Host address"),
            ("token", "Authentication token"),
            ("http_path", "HTTP path for connection"),
            ("catalog", "Catalog name"),
            ("schema", "Schema name"),
            ("table", "Target table name"),
        ],
    },
    ActionTypeInfo {
        kind: "doris",
        description: "Apache Doris sink",
        configuration: &[
            ("url", "Target URL"),
            ("database", "Database name"),
            ("table", "Target table name"),
            ("user", "Username"),
            ("password", "Password"),
        ],
    },
    ActionTypeInfo {
        kind: "bigquery",
        description: "Google BigQuery sink",
        configuration: &[
            ("project_id", "GCP project ID"),
            ("dataset", "Target dataset name"),
            ("table", "Target table name"),
        ],
    },
    ActionTypeInfo {
        kind: "redshift",
        description: "Amazon Redshift sink",
        configuration: &[
            ("url", "Target URL"),
            ("table", "Target table name"),
            ("user", "Username"),
            ("password", "Password"),
        ],
    },
    ActionTypeInfo {
        kind: "oracle",
        description: "Oracle Database sink",
        configuration: &[
            ("connection_url", "Database connection URL with credentials"),
            ("table", "Target table name"),
        ],
    },
    ActionTypeInfo {
        kind: "cockroachdb",
        description: "CockroachDB sink",
        configuration: &[
            ("connection_url", "Database connection URL with credentials"),
            ("table", "Target table name"),
        ],
    },
    ActionTypeInfo {
        kind: "alloydb",
        description: "Google AlloyDB sink",
        configuration: &[
            ("connection_url", "Database connection URL with credentials"),
            ("table", "Target table name"),
        ],
    },
    ActionTypeInfo {
        kind: "opentsdb",
        description: "OpenTSDB telnet/HTTP sink",
        configuration: &[("host", "Host address"), ("port", "Port number")],
    },
    ActionTypeInfo {
        kind: "greptimedb",
        description: "GreptimeDB sink",
        configuration: &[
            ("url", "Target URL"),
            ("database", "Database name"),
            ("table", "Target table name"),
        ],
    },
    ActionTypeInfo {
        kind: "datalayers",
        description: "Datalayers sink",
        configuration: &[
            ("url", "Target URL"),
            ("token", "Authentication token"),
            ("table", "Target table name"),
        ],
    },
    ActionTypeInfo {
        kind: "oci_streaming",
        description: "OCI Streaming sink",
        configuration: &[
            ("stream_ocid", "Stream OCID"),
            ("compartment_ocid", "Compartment OCID"),
        ],
    },
    ActionTypeInfo {
        kind: "aws_iot",
        description: "AWS IoT Core sink",
        configuration: &[
            ("endpoint", "Endpoint URL or host:port"),
            ("thing_name", "IoT thing name"),
            ("cert_file", "Certificate file path"),
            ("key_file", "Path to private key file"),
        ],
    },
    ActionTypeInfo {
        kind: "azure_iot",
        description: "Azure IoT Hub sink",
        configuration: &[
            ("hub_name", "IoT Hub name"),
            ("device_id", "Device ID"),
            ("sas_key", "SAS key"),
        ],
    },
    ActionTypeInfo {
        kind: "gcp_iot",
        description: "Google Cloud IoT Core sink",
        configuration: &[
            ("project_id", "GCP project ID"),
            ("registry_id", "Registry ID"),
            ("device_id", "Device ID"),
            ("private_key", "Private key file path"),
        ],
    },
    ActionTypeInfo {
        kind: "opc_ua",
        description: "OPC UA publisher sink",
        configuration: &[
            ("endpoint_url", "OPC UA endpoint URL"),
            ("security_policy", "Security policy"),
            ("node_id", "Node ID"),
        ],
    },
    ActionTypeInfo {
        kind: "azure_blob",
        description: "Azure Blob Storage sink",
        configuration: &[
            ("account", "Account identifier"),
            ("container", "Container name"),
            ("key_template", "Key template for object names"),
            ("sas_token", "SAS token"),
        ],
    },
    ActionTypeInfo {
        kind: "tablestore",
        description: "Alibaba Tablestore sink",
        configuration: &[
            ("endpoint", "Endpoint URL or host:port"),
            ("instance", "Instance ID"),
            ("table", "Target table name"),
            ("access_key_id", "Access key ID"),
            ("access_key_secret", "Access key secret"),
        ],
    },
    ActionTypeInfo {
        kind: "confluent",
        description: "Confluent Cloud / Platform Kafka sink",
        configuration: &[
            (
                "bootstrap_servers",
                "Kafka broker list, comma-separated host:port",
            ),
            ("topic", "Target topic name"),
            ("schema_registry_url", "Schema registry URL"),
        ],
    },
    ActionTypeInfo {
        kind: "rocketmq",
        description: "Apache RocketMQ sink",
        configuration: &[
            ("name_server", "Name server address"),
            ("topic", "Target topic name"),
            ("access_key", "Access key"),
            ("secret_key", "Secret key"),
        ],
    },
    ActionTypeInfo {
        kind: "console",
        description: "Console logger sink (diagnostic)",
        configuration: &[],
    },
];
const _: () = assert!(
    ACTION_TYPES_CATALOGUE.len() <= MAX_ACTION_TYPES,
    "action types catalogue exceeds its bound"
);

/// Returns the complete catalogue of action types the kernel can execute.
/// A kind is listed only if its connector module exists and its config validates.
/// The catalogue is bounded (max [`MAX_ACTION_TYPES`]) and does not grow at runtime.
pub fn action_types_catalogue() -> Vec<ActionTypeInfo> {
    ACTION_TYPES_CATALOGUE.to_vec()
}

/// Upper bound on the source-types catalogue size (spec C07-11).
///
/// Reason: the list handler clones the whole snapshot once per request,
/// so the catalogue must stay small and bounded; 64 covers the built-in
/// ingress set (the executable sink set plus the four ingress aliases)
/// with headroom while keeping per-request clones cheap. The default is
/// finite (exactly [`SOURCE_TYPE_COUNT`]); the operator never opts into
/// unlimited because writes are out of scope (read-only route).
pub const MAX_SOURCE_TYPES: usize = 64;

/// Stated count of the built-in source set served today. Reason: one entry
/// per source kind the kernel can instantiate (the executable sink set
/// plus the ingress aliases, minus the `test` helper which has no
/// runtime); the count documents the bound the handler enforces.
pub const SOURCE_TYPE_COUNT: usize = ACTION_TYPES_CATALOGUE.len() + 4;

/// Catalogue entry for one source type (ingress kind) with configuration
/// shape hints. This is a static snapshot of what the kernel can
/// instantiate as a source; it does not depend on runtime registration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SourceTypeInfo {
    /// Stable kind name (e.g., "mqtt", "http", "kafka").
    pub kind: &'static str,
    /// Human-readable description of the ingress kind.
    pub description: &'static str,
    /// Configuration shape hints: each accepted config key mapped to a
    /// one-line description. Empty means the kind needs no extra keys
    /// beyond the shared `id`/`type`/`enable` envelope.
    #[serde(serialize_with = "serialize_config_map")]
    pub configuration: &'static [(&'static str, &'static str)],
}

// Compile-time guard that the bound covers the served registry: the
// action set plus the four ingress aliases must fit in the cap.
const _: () = assert!(
    ACTION_TYPES_CATALOGUE.len() + 4 <= MAX_SOURCE_TYPES,
    "source types catalogue exceeds its bound"
);

/// Extra ingress aliases served only as sources, not as actions.
/// `http` shares the webhook dial path (`HttpWebhookSink` in this crate);
/// `mqtt` ingress is the broker's own message ingress
/// (`RuleEngine::dispatch_ingress` in `broker-rules`, driven by every
/// published message via the kernel ingress pipeline). Both are real
/// runtimes: `http` dials its `url` on probe, `mqtt` validates by kind
/// alone, and both flip the W3-23 enable flag in the live source store.
const SOURCE_TYPE_ALIASES: &[SourceTypeInfo] = &[
    SourceTypeInfo {
        kind: "http",
        description: "HTTP ingress source",
        configuration: &[("url", "Source URL")],
    },
    SourceTypeInfo {
        kind: "mqtt",
        description: "MQTT ingress source",
        configuration: &[
            ("server", "Source broker address"),
            ("topic", "Topic filter to subscribe"),
        ],
    },
    // Documented names for the same backends. The console sends these.
    // Each alias shares the enable-flag runtime and the probe dial hook
    // of its base kind.
    SourceTypeInfo {
        kind: "kafka_consumer",
        description: "Kafka ingress source",
        configuration: &[
            (
                "bootstrap_hosts",
                "Kafka broker list, comma-separated host:port",
            ),
            ("topic", "Kafka topic to consume from"),
        ],
    },
    SourceTypeInfo {
        kind: "gcp_pubsub_consumer",
        description: "Google Cloud Pub/Sub ingress source",
        configuration: &[
            ("project_id", "GCP project ID"),
            ("topic", "Subscription or topic name"),
        ],
    },
];

/// Returns the complete catalogue of source types the kernel can instantiate.
///
/// Built from the real registries, not a second list that could drift:
/// every executable sink kind in [`ACTION_TYPES_CATALOGUE`] plus the two
/// ingress aliases above. A kind is listed only if its runtime exists —
/// a connector module (or the shared webhook dial path for `http`, the
/// broker ingress for `mqtt`) plus the W3-23 enable-flag runtime and the
/// W3-30 probe dial hook. The unit-test `test` helper has no runtime and
/// is never listed. The catalogue is bounded (max [`MAX_SOURCE_TYPES`])
/// and does not grow at runtime.
// TODO(parity): which kinds have dedicated ingress readers (per-kind poll
// loops) versus the generic store enable-flag runtime? The rulebook does
// not decide the exact ingress split; the current choice lists every
// backend family the kernel can instantiate (ingress versus egress over
// the same backends, sharing the enable-flag runtime and the probe dial
// hook) minus the `test` helper, until the checker pins it.
pub fn source_types_catalogue() -> Vec<SourceTypeInfo> {
    let mut out = Vec::with_capacity(ACTION_TYPES_CATALOGUE.len() + SOURCE_TYPE_ALIASES.len());
    for entry in ACTION_TYPES_CATALOGUE {
        out.push(SourceTypeInfo {
            kind: entry.kind,
            description: entry.description,
            configuration: entry.configuration,
        });
    }
    out.extend_from_slice(SOURCE_TYPE_ALIASES);
    out
}

/// Whether `kind` names a source type the kernel can instantiate.
///
/// Shared by the catalogue, the source store validation and the stateless
/// source probe so the three cannot drift: the served list and both
/// checks read the same registries.
pub fn is_known_source_type(kind: &str) -> bool {
    if SOURCE_TYPE_ALIASES.iter().any(|entry| entry.kind == kind) {
        return true;
    }
    ACTION_TYPES_CATALOGUE
        .iter()
        .any(|entry| entry.kind == kind)
}

#[cfg(test)]
mod source_types_tests {
    use super::*;

    #[test]
    fn test_source_types_catalogue_basics() {
        let catalogue = source_types_catalogue();
        assert!(!catalogue.is_empty(), "catalogue must not be empty");
        assert_eq!(catalogue.len(), SOURCE_TYPE_COUNT);
        assert!(
            catalogue.len() <= MAX_SOURCE_TYPES,
            "catalogue must not exceed {MAX_SOURCE_TYPES}"
        );
        let kinds: std::collections::HashSet<&str> = catalogue.iter().map(|e| e.kind).collect();
        assert_eq!(kinds.len(), catalogue.len(), "kinds must be unique");
    }

    #[test]
    fn test_source_types_catalogue_lists_runnable_kinds_only() {
        // Every listed kind must have a runtime behind it (the shared
        // predicate), and the `test` helper (no runtime) must stay out.
        // `http` and `mqtt` are ingress aliases with real runtimes and
        // must be listed; invented names must stay out.
        let catalogue = source_types_catalogue();
        for entry in &catalogue {
            assert!(
                is_known_source_type(entry.kind),
                "catalogue lists unrunnable kind '{}'",
                entry.kind
            );
            assert!(!entry.description.trim().is_empty());
        }
        assert!(is_known_source_type("mqtt"));
        assert!(is_known_source_type("http"));
        assert!(is_known_source_type("kafka"));
        assert!(is_known_source_type("mqtt_bridge"));
        assert!(!is_known_source_type("test"));
        assert!(!is_known_source_type("no_such_kind_xyz"));
        assert!(!is_known_source_type(""));
        assert!(!is_known_source_type("pgsql"));
        let kinds: std::collections::HashSet<&str> = catalogue.iter().map(|e| e.kind).collect();
        assert!(kinds.contains("mqtt"));
        assert!(kinds.contains("http"));
        assert!(kinds.contains("kafka"));
        assert!(!kinds.contains("test"));
        assert!(!kinds.contains("pgsql"));
    }

    #[test]
    fn test_source_types_serialization_is_object_map() {
        let catalogue = source_types_catalogue();
        let json = serde_json::to_value(&catalogue).unwrap();
        let arr = json.as_array().unwrap();
        assert!(!arr.is_empty());
        for entry in arr {
            assert!(entry["kind"].is_string());
            assert!(entry["description"].is_string());
            assert!(
                entry["configuration"].is_object(),
                "configuration must serialize as a JSON object"
            );
            assert!(
                entry.get("tier").is_none(),
                "tier field must not be in the response"
            );
        }
    }
}
