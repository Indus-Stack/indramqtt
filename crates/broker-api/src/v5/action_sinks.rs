//! Action-owned runtime sinks (MV-01, ticket SB-03).
//!
//! A v5 action owns its data path: the sink is built once per action
//! from the referenced connector document (connection settings only)
//! plus the action's own parameters, and registers in the connector
//! manager under the reserved action dispatch key
//! ([`broker_config::v5_action_sink_key`]). One action is one key, so
//! two actions on one connector with different statements route
//! independently and stop sharing one sink.
//!
//! Bound (stated here and by construction): `pgsql`, the
//! `webhook`/`http` pair, the ten MV-02b time-series kinds and the
//! MV-02d object, file and cloud kinds build their sinks here. The
//! MV-02d kinds are `s3`, `azure_blob`, `dynamodb`, `bigquery`,
//! `disk_log`, `snowflake` and `tablestore`. `databricks`/`delta_lake`
//! has no reference schema and stays refused. Every other action kind
//! owns no runtime yet. It stores opaquely and registers nothing. It
//! reads `disconnected` with a reason. A fire counts one `failed`
//! delivery against its per-action metrics. The kind families accept
//! the same surface once they plot their data paths (KC-00).
//!
//! A build never opens a connection. The transports are lazy (the
//! first send dials), the `webhook` client is constructed per rule
//! body. The health worker dials the target on its configured
//! interval. Persistence failures keep the old generation on every
//! write path that uses this module.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use broker_connectors::{Sink, SinkBatchItem, SinkHealth};
use broker_protocol::{QoS, Topic};
use broker_rules::ActionMetrics;
use bytes::Bytes;

use super::actions::{ActionRecord, ActionStore, ActionWiring};
use super::rules::ConnectorStore;

/// Default batch size from the reference action resource options. Reason:
/// one row per send is the documented no-batching default.
const DEFAULT_BATCH_SIZE: usize = 1;
/// Default batch wait from the reference action resource options. Reason:
/// a zero wait means that time alone never forms a batch.
const DEFAULT_BATCH_TIME: Duration = Duration::from_millis(0);
/// Default inflight window from the reference action resource options.
/// Reason: the reference caps concurrent queries per action at 100.
const DEFAULT_INFLIGHT_WINDOW: usize = 100;
/// Default buffer bound from the reference action resource options. Reason:
/// the reference bounds one buffer worker at 256 MB.
const DEFAULT_MAX_BUFFER_BYTES: usize = 256 * 1024 * 1024;
/// Default request time to live from the reference action resource options.
/// Reason: a request that waits 45 s is stale in the reference.
const DEFAULT_REQUEST_TTL: Duration = Duration::from_secs(45);
/// Default worker pool size from the reference action resource options.
/// Reason: the reference starts 16 buffer workers per action.
const DEFAULT_WORKER_POOL_SIZE: usize = 16;
/// Default health check interval from the reference action resource
/// options. Reason: the reference checks an action target every 15 s.
const DEFAULT_HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(15);
/// Default health check timeout from the reference action resource options.
/// Reason: a check that takes more than 60 s marks the target disconnected.
const DEFAULT_HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(60);
/// Kinesis batch ceiling from the driver default. Reason: one
/// PutRecords call caps at 4 MiB in the driver.
#[cfg(feature = "kinesis")]
const MV03B_KINESIS_BATCH_BYTES: usize = 4_194_304;
/// Kinesis rows per send from the driver default. Reason: one row per
/// send keeps the runtime in charge of batching (RO-01).
#[cfg(feature = "kinesis")]
const MV03B_KINESIS_BATCH_SIZE: usize = 1;
/// Kinesis linger from the driver default. Reason: zero wait means the
/// runtime forms batches, never the driver (RO-01).
#[cfg(feature = "kinesis")]
const MV03B_KINESIS_LINGER_MS: u64 = 0;
/// Kinesis send retries from the driver default. Reason: three tries
/// ride out a short flap without a long stall.
#[cfg(feature = "kinesis")]
const MV03B_KINESIS_MAX_RETRIES: usize = 3;
/// Kinesis retry start from the driver default. Reason: 100 ms spaces
/// quick retries without blocking the queue.
#[cfg(feature = "kinesis")]
const MV03B_KINESIS_BACKOFF_FIRST_MS: u64 = 100;
/// Kinesis retry cap from the driver default. Reason: 2 s caps one send
/// so a dead endpoint fails fast to the runtime backoff.
#[cfg(feature = "kinesis")]
const MV03B_KINESIS_BACKOFF_MAX_MS: u64 = 2_000;
/// Pub/Sub batch ceiling under the vendor cap. Reason: one
/// Publish call caps at 10 MiB; 8 MB leaves headroom.
#[cfg(feature = "gcp_pubsub")]
const MV03B_GCP_BATCH_BYTES: usize = 8_000_000;
/// Event Hubs token life from the driver default. Reason: the
/// driver renews the SAS token each hour.
#[cfg(feature = "azure_eventhubs")]
const MV03B_EVENTHUBS_TOKEN_TTL_SECS: u64 = 3_600;
/// Event Hubs batch ceiling under the vendor cap. Reason: one
/// batch caps at 1 MiB; 1 MB stays under it.
#[cfg(feature = "azure_eventhubs")]
const MV03B_EVENTHUBS_BATCH_BYTES: usize = 1_000_000;
/// IoT Hub SAS life from the driver default. Reason: the driver
/// renews the SAS token each hour.
#[cfg(feature = "azure_iot")]
const MV03B_AZURE_IOT_SAS_TTL_SECS: u64 = 3_600;
/// OCI batch ceiling from the driver default. Reason: one flush
/// stays under the service body ceiling.
const MV03B_OCI_BATCH_BYTES: usize = 4_000_000;
/// GCP rows per send from the driver default. Reason: one row per
/// send keeps the runtime in charge of batching (RO-01).
#[cfg(feature = "gcp_pubsub")]
const MV03B_GCP_BATCH_SIZE: usize = 1;
/// GCP linger from the driver default. Reason: zero wait means the
/// runtime forms batches, never the driver (RO-01).
#[cfg(feature = "gcp_pubsub")]
const MV03B_GCP_LINGER_MS: u64 = 0;
/// GCP send retries from the driver default. Reason: three tries
/// ride out a short flap without a long stall.
#[cfg(feature = "gcp_pubsub")]
const MV03B_GCP_MAX_RETRIES: usize = 3;
/// GCP retry start from the driver default. Reason: 100 ms spaces
/// quick retries without blocking the queue.
#[cfg(feature = "gcp_pubsub")]
const MV03B_GCP_BACKOFF_FIRST_MS: u64 = 100;
/// GCP retry cap from the driver default. Reason: 2 s caps one send
/// so a dead endpoint fails fast to the runtime backoff.
#[cfg(feature = "gcp_pubsub")]
const MV03B_GCP_BACKOFF_MAX_MS: u64 = 2_000;
/// Event Hubs rows per send from the driver default. Reason: one row
/// per send keeps the runtime in charge of batching (RO-01).
#[cfg(feature = "azure_eventhubs")]
const MV03B_EVENTHUBS_BATCH_SIZE: usize = 1;
/// Event Hubs linger from the driver default. Reason: zero wait
/// means the runtime forms batches, never the driver (RO-01).
#[cfg(feature = "azure_eventhubs")]
const MV03B_EVENTHUBS_LINGER_MS: u64 = 0;
/// Event Hubs send retries from the driver default. Reason: three
/// tries ride out a short flap without a long stall.
#[cfg(feature = "azure_eventhubs")]
const MV03B_EVENTHUBS_MAX_RETRIES: usize = 3;
/// Event Hubs retry start from the driver default. Reason: 100 ms
/// spaces quick retries without blocking the queue.
#[cfg(feature = "azure_eventhubs")]
const MV03B_EVENTHUBS_BACKOFF_FIRST_MS: u64 = 100;
/// Event Hubs retry cap from the driver default. Reason: 2 s caps one
/// send so a dead endpoint fails fast to the runtime backoff.
#[cfg(feature = "azure_eventhubs")]
const MV03B_EVENTHUBS_BACKOFF_MAX_MS: u64 = 2_000;
/// AWS IoT rows per send from the driver default. Reason: one row per
/// send keeps the runtime in charge of batching (RO-01).
#[cfg(feature = "aws_iot")]
const MV03B_AWS_IOT_BATCH_SIZE: usize = 1;
/// AWS IoT buffer capacity from the driver default. Reason: a finite
/// buffer bounds memory while backpressure builds.
#[cfg(feature = "aws_iot")]
const MV03B_AWS_IOT_BUFFER_CAPACITY: usize = 10_000;
/// AWS IoT linger from the driver default. Reason: zero wait means
/// the runtime forms batches, never the driver (RO-01).
#[cfg(feature = "aws_iot")]
const MV03B_AWS_IOT_LINGER_MS: u64 = 0;
/// AWS IoT send retries from the driver default. Reason: three tries
/// ride out a short flap without a long stall.
#[cfg(feature = "aws_iot")]
const MV03B_AWS_IOT_MAX_RETRIES: usize = 3;
/// Azure IoT rows per send from the driver default. Reason: one row
/// per send keeps the runtime in charge of batching (RO-01).
#[cfg(feature = "azure_iot")]
const MV03B_AZURE_IOT_BATCH_SIZE: usize = 1;
/// Azure IoT buffer capacity from the driver default. Reason: a
/// finite buffer bounds memory while backpressure builds.
#[cfg(feature = "azure_iot")]
const MV03B_AZURE_IOT_BUFFER_CAPACITY: usize = 10_000;
/// Azure IoT linger from the driver default. Reason: zero wait means
/// the runtime forms batches, never the driver (RO-01).
#[cfg(feature = "azure_iot")]
const MV03B_AZURE_IOT_LINGER_MS: u64 = 0;
/// Azure IoT send retries from the driver default. Reason: three
/// tries ride out a short flap without a long stall.
#[cfg(feature = "azure_iot")]
const MV03B_AZURE_IOT_MAX_RETRIES: usize = 3;
/// OCI rows per send from the driver default. Reason: one row per
/// send keeps the runtime in charge of batching (RO-01).
const MV03B_OCI_BATCH_SIZE: usize = 1;
/// OCI buffer capacity from the driver default. Reason: a finite
/// buffer bounds memory while backpressure builds.
const MV03B_OCI_BUFFER_CAPACITY: usize = 10_000;
/// OCI linger from the driver default. Reason: zero wait means the
/// runtime forms batches, never the driver (RO-01).
const MV03B_OCI_LINGER_MS: u64 = 0;
/// OCI send retries from the driver default. Reason: three tries ride
/// out a short flap without a long stall.
const MV03B_OCI_MAX_RETRIES: usize = 3;
/// OCI retry start from the driver default. Reason: 100 ms spaces
/// quick retries without blocking the queue.
const MV03B_OCI_BACKOFF_FIRST_MS: u64 = 100;
/// OCI retry cap from the driver default. Reason: 2 s caps one send
/// so a dead endpoint fails fast to the runtime backoff.
const MV03B_OCI_BACKOFF_MAX_MS: u64 = 2_000;

/// Message-broker partition count for hashing. Reason: the loopback
/// fake reports twelve partitions in Metadata, and the driver hashes
/// with this count when broker Metadata is unavailable.
#[cfg(any(feature = "kafka", feature = "confluent"))]
const MV03A_DEFAULT_PARTITIONS: u32 = 12;
/// Message-broker batch ceiling. Reason: one Kafka batch caps at
/// 1 MiB in the reference producer buffer.
#[cfg(any(feature = "kafka", feature = "confluent", feature = "rabbitmq"))]
const MV03A_BATCH_MAX_BYTES: usize = 1_048_576;
/// Message-broker rows per send. Reason: one row per send keeps the
/// runtime in charge of batching (RO-01).
const MV03A_BATCH_SIZE_ONE: usize = 1;
/// Message-broker batch bytes for Pulsar. Reason: the Pulsar driver
/// defaults one batch to 2 MiB.
const MV03A_PULSAR_BATCH_BYTES: usize = 2_097_152;
/// Message-broker linger. Reason: a 10 ms linger lets one row per
/// send wait for the runtime batch timer (RO-01).
const MV03A_LINGER_MS: u64 = 10;
/// Message-broker send retries. Reason: three tries ride out a short
/// flap without a long stall.
const MV03A_MAX_RETRIES: usize = 3;
/// Message-broker retry start. Reason: 100 ms spaces quick retries
/// without blocking the queue.
const MV03A_BACKOFF_FIRST_MS: u64 = 100;
/// Message-broker retry cap. Reason: 2 s caps one send so a dead
/// endpoint fails fast to the runtime backoff.
const MV03A_BACKOFF_MAX_MS: u64 = 2_000;
/// Message-broker inflight bound. Reason: 1000 rows cap the sink
/// queue while the runtime drains it (RO-01).
const MV03A_MAX_INFLIGHT: usize = 1_000;
/// Message-broker proxy buffer rows. Reason: 1024 rows cap the memory
/// the proxy client holds while the runtime owns batching (RO-01).
const MV03A_PROXY_BUFFER_ROWS: usize = 1_024;
/// MQTT bridge keep-alive. Reason: the reference connector_mqtt
/// defaults keepalive to 160 s.
const MV03A_MQTT_KEEPALIVE_SECS: u16 = 160;

/// Dispatch strategy of the reference action resource options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActionDispatchStrategy {
    /// Preserve the order of one action queue when the inflight window is
    /// one. This is the default.
    PerClientId,
}

/// Query mode of the reference action resource options.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActionQueryMode {
    /// A partial batch waits for the batch timer.
    Async,
    /// A send flushes the partial batch before it returns.
    Sync,
}

/// Parsed and validated `resource_opts` of one action. The action runtime
/// owns these values; the management store keeps the raw object so a read
/// returns exactly what the operator wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ActionResourceOpts {
    pub batch_size: usize,
    pub batch_time: Duration,
    pub inflight_window: usize,
    pub max_buffer_bytes: usize,
    pub request_ttl: Option<Duration>,
    pub worker_pool_size: usize,
    pub query_mode: ActionQueryMode,
    pub dispatch_strategy: ActionDispatchStrategy,
    pub health_check_interval: Duration,
    pub health_check_interval_jitter: Duration,
    pub health_check_timeout: Option<Duration>,
}

impl Default for ActionResourceOpts {
    fn default() -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            batch_time: DEFAULT_BATCH_TIME,
            inflight_window: DEFAULT_INFLIGHT_WINDOW,
            max_buffer_bytes: DEFAULT_MAX_BUFFER_BYTES,
            request_ttl: Some(DEFAULT_REQUEST_TTL),
            worker_pool_size: DEFAULT_WORKER_POOL_SIZE,
            query_mode: ActionQueryMode::Async,
            dispatch_strategy: ActionDispatchStrategy::PerClientId,
            health_check_interval: DEFAULT_HEALTH_CHECK_INTERVAL,
            health_check_interval_jitter: Duration::ZERO,
            health_check_timeout: Some(DEFAULT_HEALTH_CHECK_TIMEOUT),
        }
    }
}

impl ActionResourceOpts {
    /// Parse one action's raw `resource_opts` object. An absent key takes
    /// its documented default. A non-default value that this runtime cannot
    /// apply is refused with the field named, and no value is invented.
    pub(crate) fn parse(id: &str, raw: &serde_json::Value) -> Result<Self, String> {
        let Some(obj) = raw.as_object() else {
            return Err(format!(
                "action {id:?} field `resource_opts` must be an object"
            ));
        };
        let mut opts = Self::default();
        // Stored pgsql actions written before RO-01 use `batch_timeout_ms`
        // as an integer. Keep that field: it maps to the runtime batch
        // wait, so an old row still boots and applies its old value. A
        // malformed value is refused even when `batch_time` is present.
        if let Some(value) = obj.get("batch_timeout_ms") {
            let legacy = parse_millis(id, "batch_timeout_ms", value)?;
            if !obj.contains_key("batch_time") {
                opts.batch_time = legacy;
            }
        }
        for (key, value) in obj {
            match key.as_str() {
                "batch_size" => opts.batch_size = parse_usize(id, key, value, 1, usize::MAX)?,
                "batch_time" => {
                    opts.batch_time = parse_duration(id, key, value)?.ok_or_else(|| {
                        format!(
                            "action {id:?} field `resource_opts.batch_time` value \"infinity\" is not supported"
                        )
                    })?;
                }
                "batch_timeout_ms" => {}
                "inflight_window" => {
                    opts.inflight_window =
                        parse_usize(id, key, value, 1, tokio::sync::Semaphore::MAX_PERMITS)?
                }
                "max_buffer_bytes" => {
                    let bytes = parse_bytes(id, key, value)?;
                    if bytes == 0 {
                        return Err(format!(
                            "action {id:?} field `resource_opts.max_buffer_bytes` must be at least 1 byte"
                        ));
                    }
                    opts.max_buffer_bytes = bytes;
                }
                "request_ttl" => opts.request_ttl = parse_duration(id, key, value)?,
                "worker_pool_size" => opts.worker_pool_size = parse_usize(id, key, value, 1, 1024)?,
                "query_mode" => {
                    let mode = parse_string(id, key, value)?;
                    opts.query_mode = match mode.as_str() {
                        "async" => ActionQueryMode::Async,
                        "sync" => ActionQueryMode::Sync,
                        other => {
                            return Err(format!(
                                "action {id:?} field `resource_opts.query_mode` value {other:?} must be `sync` or `async`"
                            ));
                        }
                    };
                }
                "dispatch_strategy" => {
                    let strategy = parse_string(id, key, value)?;
                    opts.dispatch_strategy = match strategy.as_str() {
                        "per_clientid" => ActionDispatchStrategy::PerClientId,
                        other => {
                            return Err(format!(
                                "action {id:?} field `resource_opts.dispatch_strategy` value {other:?} is not supported: this action runtime dispatches one ordered queue"
                            ));
                        }
                    };
                }
                "health_check_interval" => {
                    let interval = parse_duration(id, key, value)?.ok_or_else(|| {
                        format!(
                            "action {id:?} field `resource_opts.health_check_interval` value \"infinity\" is not supported"
                        )
                    })?;
                    if interval.is_zero() {
                        return Err(format!(
                            "action {id:?} field `resource_opts.health_check_interval` must be at least 1ns"
                        ));
                    }
                    opts.health_check_interval = interval;
                }
                "health_check_interval_jitter" => {
                    opts.health_check_interval_jitter =
                        parse_duration(id, key, value)?.unwrap_or(Duration::MAX);
                }
                "health_check_timeout" => {
                    opts.health_check_timeout = parse_duration(id, key, value)?
                }
                // TODO(parity): the reference may add action resource
                // fields after 6.3.0. The rulebook does not decide. Refuse
                // an unknown field instead of ignoring it.
                other => {
                    return Err(format!(
                        "action {id:?} field `resource_opts.{other}` is not supported"
                    ));
                }
            }
        }
        if opts.health_check_interval_jitter != Duration::ZERO {
            // TODO(parity): the probe uses the documented timeout and no
            // jitter. The rulebook does not decide a per-connector jitter.
            // Refuse non-default jitter instead of ignoring it.
            return Err(format!(
                "action {id:?} field `resource_opts.health_check_interval_jitter` is not supported: this action runtime uses the documented default for that field"
            ));
        }
        if let Some(ttl) = opts.request_ttl {
            // Reason: a batch wait longer than the request TTL would let a
            // low-rate batch expire before its first send. Refuse the pair
            // instead of capping it silently.
            if opts.batch_time > ttl {
                return Err(format!(
                    "action {id:?} field `resource_opts.batch_time` value {:?} exceeds `request_ttl` {:?}: lower `batch_time` or raise `request_ttl`",
                    opts.batch_time, ttl
                ));
            }
        }
        Ok(opts)
    }

    /// Name of the first value that differs from its documented default.
    /// `None` means every value is a default.
    fn first_non_default_field(&self) -> Option<&'static str> {
        if self.batch_size != DEFAULT_BATCH_SIZE {
            Some("batch_size")
        } else if self.batch_time != DEFAULT_BATCH_TIME {
            Some("batch_time")
        } else if self.inflight_window != DEFAULT_INFLIGHT_WINDOW {
            Some("inflight_window")
        } else if self.max_buffer_bytes != DEFAULT_MAX_BUFFER_BYTES {
            Some("max_buffer_bytes")
        } else if self.request_ttl != Some(DEFAULT_REQUEST_TTL) {
            Some("request_ttl")
        } else if self.worker_pool_size != DEFAULT_WORKER_POOL_SIZE {
            Some("worker_pool_size")
        } else if self.query_mode != ActionQueryMode::Async {
            Some("query_mode")
        } else if self.dispatch_strategy != ActionDispatchStrategy::PerClientId {
            Some("dispatch_strategy")
        } else if self.health_check_interval != DEFAULT_HEALTH_CHECK_INTERVAL {
            Some("health_check_interval")
        } else if self.health_check_interval_jitter != Duration::ZERO {
            Some("health_check_interval_jitter")
        } else {
            None
        }
    }
}

fn parse_string(id: &str, key: &str, value: &serde_json::Value) -> Result<String, String> {
    value
        .as_str()
        .map(|text| text.trim().to_string())
        .ok_or_else(|| format!("action {id:?} field `resource_opts.{key}` must be a string"))
}

fn parse_usize(
    id: &str,
    key: &str,
    value: &serde_json::Value,
    minimum: usize,
    maximum: usize,
) -> Result<usize, String> {
    let Some(number) = value.as_u64() else {
        return Err(format!(
            "action {id:?} field `resource_opts.{key}` must be an integer"
        ));
    };
    let number = usize::try_from(number).unwrap_or(usize::MAX);
    if number < minimum || number > maximum {
        return Err(format!(
            "action {id:?} field `resource_opts.{key}` must be between {minimum} and {maximum}"
        ));
    }
    Ok(number)
}

/// Parse the legacy integer millisecond field `batch_timeout_ms`.
fn parse_millis(id: &str, key: &str, value: &serde_json::Value) -> Result<Duration, String> {
    let Some(number) = value.as_u64() else {
        return Err(format!(
            "action {id:?} field `resource_opts.{key}` must be an integer"
        ));
    };
    Ok(Duration::from_millis(number))
}

/// Parse a duration string such as `0ms`, `45s` or `infinity`. `None`
/// means the value is `infinity`.
fn parse_duration(
    id: &str,
    key: &str,
    value: &serde_json::Value,
) -> Result<Option<Duration>, String> {
    let Some(text) = value.as_str() else {
        return Err(format!(
            "action {id:?} field `resource_opts.{key}` must be a duration string"
        ));
    };
    let text = text.trim().to_ascii_lowercase();
    if text == "infinity" {
        return Ok(None);
    }
    let split = text
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .ok_or_else(|| duration_error(id, key, value))?;
    let (number, unit) = text.split_at(split);
    let number: f64 = number.parse().map_err(|_| duration_error(id, key, value))?;
    if !number.is_finite() || number < 0.0 {
        return Err(duration_error(id, key, value));
    }
    let nanos = match unit.trim() {
        "ns" => number,
        "us" | "µs" => number * 1_000.0,
        "ms" => number * 1_000_000.0,
        "s" => number * 1_000_000_000.0,
        "m" => number * 60.0 * 1_000_000_000.0,
        "h" => number * 3600.0 * 1_000_000_000.0,
        _ => return Err(duration_error(id, key, value)),
    };
    if nanos > u64::MAX as f64 {
        return Err(duration_error(id, key, value));
    }
    Ok(Some(Duration::from_nanos(nanos as u64)))
}

fn duration_error(id: &str, key: &str, value: &serde_json::Value) -> String {
    format!("action {id:?} field `resource_opts.{key}` value {value} is not a valid duration")
}

/// Parse a byte-size string such as `256MB`, `32MB` or `1GB`. The suffix
/// is binary (1 MB is 1024 * 1024 bytes), as the reference uses for
/// buffer bounds.
fn parse_bytes(id: &str, key: &str, value: &serde_json::Value) -> Result<usize, String> {
    let Some(text) = value.as_str() else {
        return Err(format!(
            "action {id:?} field `resource_opts.{key}` must be a byte-size string"
        ));
    };
    let text = text.trim().to_ascii_lowercase();
    let split = text
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .ok_or_else(|| byte_error(id, key, value))?;
    let (number, unit) = text.split_at(split);
    let number: f64 = number.parse().map_err(|_| byte_error(id, key, value))?;
    if !number.is_finite() || number < 0.0 {
        return Err(byte_error(id, key, value));
    }
    let multiplier: f64 = match unit.trim() {
        "b" | "" => 1.0,
        "kb" => 1024.0,
        "mb" => 1024.0 * 1024.0,
        "gb" => 1024.0 * 1024.0 * 1024.0,
        "kib" => 1024.0,
        "mib" => 1024.0 * 1024.0,
        "gib" => 1024.0 * 1024.0 * 1024.0,
        _ => return Err(byte_error(id, key, value)),
    };
    let bytes = number * multiplier;
    if bytes > usize::MAX as f64 {
        return Err(byte_error(id, key, value));
    }
    Ok(bytes as usize)
}

fn byte_error(id: &str, key: &str, value: &serde_json::Value) -> String {
    format!("action {id:?} field `resource_opts.{key}` value {value} is not a valid byte size")
}

/// Canonical runtime kind of one action type: the documented aliases
/// map to the base kind that executes them (the KC-00 map owns each
/// alias). Unknown names pass through unchanged; the caller rejects
/// them before this runs.
//
// TODO(parity): should the statement parameters (sql, write_syntax,
// cql) execute, or stay stored opaquely? The drivers take templates
// only. The current choice stores them and renders the fixed row
// shape until the checker pins it.
pub(crate) fn canonical_action_kind(kind: &str) -> &str {
    match kind {
        "aws_timestream" => "timestream",
        "opents" => "opentsdb",
        "scylla" | "scylladb" | "cql" => "cassandra",
        "greptime" => "greptimedb",
        "ch" => "clickhouse",
        "mongo" | "documentdb" => "mongodb",
        "couch" | "cb" => "couchbase",
        "elastic" | "es" | "opensearch" => "elasticsearch",
        "sqlserver" => "mssql",
        "mariadb" => "mysql",
        "cockroach" => "cockroachdb",
        "postgres" | "postgresql" => "pgsql",
        "google_alloydb" | "alloy_db" => "alloydb",
        "minio" => "s3",
        "azure_blob_storage" => "azure_blob",
        "dynamo" | "ddb" => "dynamodb",
        "rabbit" | "amqp" => "rabbitmq",
        "bq" | "gbq" => "bigquery",
        "snowflake_streaming" | "snowflake_aggregated" => "snowflake",
        "ots" => "tablestore",
        "delta_lake" => "databricks",
        "opcua" => "opc_ua",
        "sparkplug" | "spb" => "sparkplug_b",
        "rest" => "http",
        "logger" => "console",
        "aws_kinesis" => "kinesis",
        "gcp" | "pubsub" | "gcp_pubsub_producer" | "gcp_pubsub_consumer" => "gcp_pubsub",
        "azure_event_hubs" | "azure" | "eventhubs" | "azure_event_hub_producer" => {
            "azure_eventhubs"
        }
        "oci" => "oci_streaming",
        "aws_iot_core" => "aws_iot",
        "azure_iot_hub" | "azure_iothub" | "iothub" => "azure_iot",
        "gcp_iot_core" | "cloud_iot" => "gcp_iot",
        "mqtt" => "mqtt_bridge",
        "kafka_producer" | "kafka_consumer" => "kafka",
        "confluent_producer" | "confluent_kafka" | "confluent_cloud" => "confluent",
        _ => kind,
    }
}

/// True when the action kind builds an owned sink here. The
/// documented `http` action kind and the historic `webhook` base kind
/// share one builder; `pgsql` covers `postgres`-filed rows; each
/// MV-02b alias (for example `aws_timestream`) runs on the builder of
/// its canonical kind. MV-02a adds the relational family: `mysql`,
/// `mssql`, `oracle` and the PostgreSQL-protocol kinds `alloydb`,
/// `cockroachdb`, `redshift`.
pub(crate) fn action_owned_kind(kind: &str) -> bool {
    matches!(
        canonical_action_kind(kind),
        "pgsql"
            | "webhook"
            | "http"
            | "clickhouse"
            | "tdengine"
            | "greptimedb"
            | "iotdb"
            | "opentsdb"
            | "doris"
            | "datalayers"
            | "influxdb"
            | "timestream"
            | "cassandra"
            | "mysql"
            | "mssql"
            | "oracle"
            | "alloydb"
            | "cockroachdb"
            | "redshift"
            | "mongodb"
            | "couchbase"
            | "elasticsearch"
            | "s3"
            | "azure_blob"
            | "dynamodb"
            | "bigquery"
            | "disk_log"
            | "snowflake"
            | "tablestore"
            | "rabbitmq"
            | "opc_ua"
            | "sparkplug_b"
            | "console"
            | "kinesis"
            | "gcp_pubsub"
            | "azure_eventhubs"
            | "aws_iot"
            | "azure_iot"
            | "oci_streaming"
            | "kafka"
            | "confluent"
            | "pulsar"
            | "rocketmq"
            | "mqtt_bridge"
            | "redis"
    )
}

/// Every connector reference an action record carries: the documented
/// `connector` field plus the legacy flat extras
/// (`connector`/`connector_id` in `params`).
fn action_connector_references(record: &ActionRecord) -> Vec<String> {
    let mut references = Vec::new();
    for raw in [
        record.connector.as_str(),
        record
            .params
            .get("connector")
            .and_then(|v| v.as_str())
            .unwrap_or(""),
        record
            .params
            .get("connector_id")
            .and_then(|v| v.as_str())
            .unwrap_or(""),
    ] {
        let trimmed = raw.trim();
        if !trimmed.is_empty() && !references.contains(&trimmed.to_string()) {
            references.push(trimmed.to_string());
        }
    }
    references
}

/// True when the record targets the connector stored under `bare`:
/// every reference field resolves through the same matching the
/// connector read paths use (bare id or `{type}:{name}` key), so an
/// action can never hide a live reference in one of the legacy fields.
pub(crate) fn action_targets_connector(
    connectors: &ConnectorStore,
    record: &ActionRecord,
    bare: &str,
) -> bool {
    action_connector_references(record).iter().any(|reference| {
        super::rules::resolve_action_connector(connectors, reference)
            .map(|(resolved, _)| resolved == bare)
            .unwrap_or(false)
    })
}

/// One message waiting in an action buffer.
struct QueuedMessage {
    topic: Topic,
    payload: Bytes,
    qos: QoS,
    enqueued: Instant,
}

/// Bounded FIFO buffer of one action runtime.
#[derive(Default)]
struct RuntimeBuffer {
    rows: VecDeque<QueuedMessage>,
    bytes: usize,
}

/// Per-row byte overhead counted in `max_buffer_bytes`. Reason: each queued
/// row holds a topic string, a payload handle, a timestamp and a queue node.
/// 64 bytes bounds those handles on 64-bit builds.
const ROW_OVERHEAD_BYTES: usize = 64;

impl RuntimeBuffer {
    fn take(&mut self, count: usize) -> Vec<QueuedMessage> {
        let count = count.min(self.rows.len());
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(row) = self.rows.pop_front() {
                self.bytes = self
                    .bytes
                    .saturating_sub(row_footprint(&row.topic, &row.payload));
                out.push(row);
            }
        }
        out
    }

    /// Row count waiting in the buffer.
    fn len(&self) -> usize {
        self.rows.len()
    }

    /// True when no row waits in the buffer.
    fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// Byte footprint of one queued row: topic bytes plus payload bytes plus
/// the per-row overhead. The bound counts this total.
fn row_footprint(topic: &Topic, payload: &Bytes) -> usize {
    topic
        .as_str()
        .len()
        .saturating_add(payload.len())
        .saturating_add(ROW_OVERHEAD_BYTES)
}

/// Decrement a gauge without wrapping. A reset can set the gauge to zero
/// while rows are still queued or in flight.
fn sub_saturating(counter: &std::sync::atomic::AtomicU64, count: u64) {
    let _ = counter.try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_sub(count))
    });
}

/// Live health state of one action runtime target. The health worker
/// writes it; the management read path reads it.
#[derive(Debug)]
struct ActionHealth {
    healthy: AtomicBool,
    reason: Mutex<Option<String>>,
}

impl ActionHealth {
    fn new() -> Self {
        // A sink starts unprobed, never healthy. The create and update
        // paths probe before they answer; boot and reload probe before
        // service starts. Until then the read path reports the pending
        // state instead of an assumed reachable target.
        Self {
            healthy: AtomicBool::new(false),
            reason: Mutex::new(Some("health check pending".to_string())),
        }
    }

    fn record(&self, result: Result<(), String>) {
        match result {
            Ok(()) => {
                self.healthy.store(true, Ordering::Relaxed);
                *self
                    .reason
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
            }
            Err(reason) => {
                self.healthy.store(false, Ordering::Relaxed);
                *self
                    .reason
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(reason);
            }
        }
    }

    fn snapshot(&self) -> SinkHealth {
        SinkHealth {
            healthy: self.healthy.load(Ordering::Relaxed),
            reason: self
                .reason
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone(),
        }
    }
}

/// One action's runtime state: the built connector sink, the parsed
/// resource options, the counter record and the bounded queue.
struct ActionRuntimeState {
    inner: Arc<dyn Sink>,
    opts: ActionResourceOpts,
    metrics: Arc<ActionMetrics>,
    buffer: Mutex<RuntimeBuffer>,
    inflight: Arc<tokio::sync::Semaphore>,
    health: ActionHealth,
    health_target: Option<String>,
    health_timeout: Option<Duration>,
}

/// Action-owned runtime sink. It applies `resource_opts` for one action:
/// size and time batching, a bounded buffer, request time to live, an
/// inflight window and worker-row chunks. The driver receives a batch; a
/// driver that writes one row at a time receives batches of one.
pub(crate) struct ActionRuntimeSink {
    state: Arc<ActionRuntimeState>,
    wake: tokio::sync::mpsc::Sender<()>,
}

impl ActionRuntimeSink {
    pub(crate) fn new(
        inner: Arc<dyn Sink>,
        opts: ActionResourceOpts,
        metrics: Arc<ActionMetrics>,
        health_target: Option<String>,
    ) -> Self {
        let batch_time = opts.batch_time;
        let health_interval = opts.health_check_interval;
        let health_timeout = opts.health_check_timeout;
        let state = Arc::new(ActionRuntimeState {
            inner,
            inflight: Arc::new(tokio::sync::Semaphore::new(opts.inflight_window)),
            opts,
            metrics,
            buffer: Mutex::new(RuntimeBuffer::default()),
            health: ActionHealth::new(),
            health_target: health_target.clone(),
            health_timeout,
        });
        let (wake, receiver) = tokio::sync::mpsc::channel(1);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            if !batch_time.is_zero() {
                let weak: Weak<ActionRuntimeState> = Arc::downgrade(&state);
                handle.spawn(run_flush_worker(weak, receiver, batch_time));
            }
            if let Some(target) = health_target {
                let weak: Weak<ActionRuntimeState> = Arc::downgrade(&state);
                handle.spawn(run_health_worker(
                    weak,
                    target,
                    health_interval,
                    health_timeout,
                ));
            }
        }
        Self { state, wake }
    }
}

/// Probe the action target once per interval, after the first interval.
/// The default interval is 15 s and the default timeout is 60 s. The
/// dial uses the configured timeout, so a non-default timeout takes
/// effect on the wait.
async fn run_health_worker(
    state: Weak<ActionRuntimeState>,
    target: String,
    interval: Duration,
    timeout: Option<Duration>,
) {
    let tick = if interval.is_zero() {
        Duration::from_secs(1)
    } else {
        interval
    };
    loop {
        tokio::time::sleep(tick).await;
        let Some(current) = state.upgrade() else {
            break;
        };
        current.health.record(probe_target(&target, timeout).await);
    }
}

/// Dial target of one action health probe. A bare host reads port 80.
/// A bare `https://` host reads port 443, the scheme default port. A
/// value that already carries a port dials as given.
fn probe_dial_target(target: &str) -> String {
    let trimmed = target.trim();
    if let Some(rest) = trimmed.strip_prefix("https://") {
        let host = rest.split('/').next().unwrap_or(rest);
        if host.contains(':') {
            host.to_string()
        } else {
            format!("{host}:443")
        }
    } else if let Some(rest) = trimmed.strip_prefix("http://") {
        let host = rest.split('/').next().unwrap_or(rest);
        if host.contains(':') {
            host.to_string()
        } else {
            format!("{host}:80")
        }
    } else if trimmed.contains(':') {
        trimmed.to_string()
    } else {
        format!("{trimmed}:80")
    }
}

async fn probe_target(target: &str, timeout: Option<Duration>) -> Result<(), String> {
    // The reason carries no host. The action read path already names the
    // action and its target, so a repeated host adds no value.
    let dial = probe_dial_target(target);
    match timeout {
        Some(limit) => {
            match tokio::time::timeout(limit, tokio::net::TcpStream::connect(&dial)).await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(e)) => Err(format!("connection refused: {e}")),
                Err(_) => Err("health check timed out".to_string()),
            }
        }
        None => match tokio::net::TcpStream::connect(&dial).await {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("connection refused: {e}")),
        },
    }
}

async fn run_flush_worker(
    state: Weak<ActionRuntimeState>,
    mut receiver: tokio::sync::mpsc::Receiver<()>,
    batch_time: Duration,
) {
    let mut ticker = tokio::time::interval(batch_time);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let Some(state) = state.upgrade() else { break };
                let _ = state.flush_pending(false).await;
            }
            message = receiver.recv() => {
                if message.is_none() {
                    break;
                }
                let Some(state) = state.upgrade() else { break };
                let _ = state.flush_pending(false).await;
            }
        }
    }
}

impl ActionRuntimeState {
    /// Move ready rows out of the buffer and send them. `force_all` sends a
    /// partial batch (the sync mode and the zero-time mode use it); without
    /// it only a full or stale batch moves.
    async fn flush_pending(self: &Arc<Self>, force_all: bool) -> broker_connectors::Result<()> {
        let mut first_error = None;
        loop {
            let batch = {
                let mut buffer = self
                    .buffer
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if buffer.rows.is_empty() {
                    break;
                }
                let now = Instant::now();
                let stale = force_all
                    || self.opts.batch_time.is_zero()
                    || buffer.rows.front().is_some_and(|row| {
                        now.duration_since(row.enqueued) >= self.opts.batch_time
                    });
                let full = buffer.rows.len() >= self.opts.batch_size;
                // `batch_size` applies only when `batch_time` is more than
                // zero. A zero batch time sends each row at once.
                let take = if stale || full {
                    if self.opts.batch_time.is_zero() {
                        buffer.rows.len().min(1)
                    } else {
                        buffer.rows.len().min(self.opts.batch_size)
                    }
                } else {
                    0
                };
                if take == 0 {
                    break;
                }
                buffer.take(take)
            };
            if batch.is_empty() {
                break;
            }
            if let Err(error) = self.dispatch_batch(batch).await {
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
            if !force_all {
                let buffer = self
                    .buffer
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let stale = self.opts.batch_time.is_zero()
                    || buffer
                        .rows
                        .front()
                        .is_some_and(|row| row.enqueued.elapsed() >= self.opts.batch_time);
                if buffer.rows.len() < self.opts.batch_size && !stale {
                    break;
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Remove waiting rows and count them as stopped drops. The replace
    /// paths call it before they swap the sink, so no row vanishes
    /// without a counter.
    fn discard_buffered(&self) {
        let count = self.take_buffered();
        if count == 0 {
            return;
        }
        self.metrics.dropped.fetch_add(count, Ordering::Relaxed);
        self.metrics
            .dropped_resource_stopped
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Empty the buffer and return the dropped row count. The caller
    /// counts the rows. The queue gauge drops by the same count.
    fn take_buffered(&self) -> u64 {
        let mut buffer = self
            .buffer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if buffer.is_empty() {
            return 0;
        }
        let count = buffer.len() as u64;
        buffer.rows.clear();
        buffer.bytes = 0;
        sub_saturating(&self.metrics.queuing, count);
        count
    }

    async fn dispatch_batch(
        self: &Arc<Self>,
        mut batch: Vec<QueuedMessage>,
    ) -> broker_connectors::Result<()> {
        // One permit per driver call spans `send_batch`. Up to
        // `inflight_window` calls run at the same time. A window
        // of one keeps enqueue order. A larger window sends in parallel.
        //
        // One chunk is one driver call. `worker_pool_size` bounds the rows
        // in one call; `inflight_window` bounds the calls in flight.
        let chunk_size = self.opts.worker_pool_size.max(1);
        let mut tasks = tokio::task::JoinSet::new();
        while !batch.is_empty() {
            let take = batch.len().min(chunk_size);
            let chunk: Vec<QueuedMessage> = batch.drain(0..take).collect();
            let mut live = Vec::with_capacity(chunk.len());
            for row in chunk {
                sub_saturating(&self.metrics.queuing, 1);
                let expired_now = self
                    .opts
                    .request_ttl
                    .is_some_and(|ttl| row.enqueued.elapsed() >= ttl);
                if expired_now {
                    self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                    self.metrics.dropped_other.fetch_add(1, Ordering::Relaxed);
                } else {
                    live.push(row);
                }
            }
            if live.is_empty() {
                continue;
            }
            // One owned permit per driver call bounds the queries in
            // flight to the inflight window. With a window of one, each
            // call finishes before the next starts, so one action queue
            // keeps its order.
            let permit = match Arc::clone(&self.inflight).acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => {
                    let remaining = live.len() + batch.len();
                    for _ in 0..remaining {
                        sub_saturating(&self.metrics.queuing, 1);
                        self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                        self.metrics.dropped_other.fetch_add(1, Ordering::Relaxed);
                    }
                    return Err(broker_connectors::ConnectorError::Dispatch(
                        "action inflight window is closed".to_string(),
                    ));
                }
            };
            // A row can expire while it waits for the inflight window.
            // Count and drop it here, after the wait, before the driver
            // receives it.
            if let Some(ttl) = self.opts.request_ttl {
                live.retain(|row| {
                    if row.enqueued.elapsed() >= ttl {
                        self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                        self.metrics.dropped_other.fetch_add(1, Ordering::Relaxed);
                        false
                    } else {
                        true
                    }
                });
                if live.is_empty() {
                    drop(permit);
                    continue;
                }
            }
            let live: Vec<SinkBatchItem> = live
                .into_iter()
                .map(|row| SinkBatchItem {
                    topic: row.topic,
                    payload: row.payload,
                    qos: row.qos,
                })
                .collect();
            let state = Arc::clone(self);
            tasks.spawn(async move {
                let _permit = permit;
                let rows = live.len() as u64;
                state.metrics.inflight.fetch_add(rows, Ordering::Relaxed);
                let outcome = state.inner.send_batch(&live).await;
                sub_saturating(&state.metrics.inflight, rows);
                match &outcome {
                    Ok(()) => {
                        state.metrics.success.fetch_add(rows, Ordering::Relaxed);
                    }
                    Err(_) => {
                        state.metrics.failed.fetch_add(rows, Ordering::Relaxed);
                    }
                }
                outcome.err()
            });
        }
        let mut first_error = None;
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Some(error)) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
                Ok(None) => {}
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(broker_connectors::ConnectorError::Dispatch(format!(
                            "action dispatch task failed: {error}"
                        )));
                    }
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl Drop for ActionRuntimeState {
    fn drop(&mut self) {
        // Backstop: rows that still wait at drop count as dropped. An
        // explicit discard before a swap empties the queue first, so this
        // adds nothing on the replace paths.
        let mut buffer = self
            .buffer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if buffer.is_empty() {
            return;
        }
        let count = buffer.len() as u64;
        buffer.rows.clear();
        buffer.bytes = 0;
        drop(buffer);
        sub_saturating(&self.metrics.queuing, count);
        self.metrics.dropped.fetch_add(count, Ordering::Relaxed);
        self.metrics
            .dropped_other
            .fetch_add(count, Ordering::Relaxed);
    }
}

#[async_trait::async_trait]
impl Sink for ActionRuntimeSink {
    async fn flush(&self) -> broker_connectors::Result<()> {
        self.state.flush_pending(true).await
    }

    async fn probe(&self) -> std::result::Result<(), String> {
        let Some(target) = self.state.health_target.as_deref() else {
            // A kind without a network target has nothing to dial and is
            // not a down target.
            self.state.health.record(Ok(()));
            return Ok(());
        };
        let result = probe_target(target, self.state.health_timeout).await;
        self.state.health.record(result.clone());
        result
    }

    async fn send(
        &self,
        topic: &Topic,
        payload: &Bytes,
        qos: QoS,
    ) -> broker_connectors::Result<()> {
        let now = Instant::now();
        let size = row_footprint(topic, payload);
        let mut overflowed = false;
        let mut expired = false;
        if size > self.state.opts.max_buffer_bytes {
            // A row larger than the bound alone never fits. Refuse it and
            // keep the buffer as it was.
            overflowed = true;
        } else if self
            .state
            .opts
            .request_ttl
            .is_some_and(|ttl| now.elapsed() >= ttl)
        {
            // The TTL is checked at entry too. Only a zero TTL trips here;
            // a positive TTL expires while the row waits.
            expired = true;
        } else {
            let mut buffer = self
                .state
                .buffer
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            while buffer.bytes.saturating_add(size) > self.state.opts.max_buffer_bytes {
                let Some(row) = buffer.rows.pop_front() else {
                    break;
                };
                buffer.bytes = buffer
                    .bytes
                    .saturating_sub(row_footprint(&row.topic, &row.payload));
                sub_saturating(&self.state.metrics.queuing, 1);
                self.state.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                self.state
                    .metrics
                    .dropped_queue_full
                    .fetch_add(1, Ordering::Relaxed);
            }
            buffer.bytes = buffer.bytes.saturating_add(size);
            buffer.rows.push_back(QueuedMessage {
                topic: topic.clone(),
                payload: payload.clone(),
                qos,
                enqueued: now,
            });
            self.state.metrics.queuing.fetch_add(1, Ordering::Relaxed);
        }
        self.state.metrics.matched.fetch_add(1, Ordering::Relaxed);
        self.state.metrics.total.fetch_add(1, Ordering::Relaxed);
        if overflowed {
            self.state.metrics.dropped.fetch_add(1, Ordering::Relaxed);
            self.state
                .metrics
                .dropped_queue_full
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        if expired {
            self.state.metrics.dropped.fetch_add(1, Ordering::Relaxed);
            self.state
                .metrics
                .dropped_other
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        let should_flush = {
            let buffer = self
                .state
                .buffer
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.state.opts.batch_time.is_zero() || buffer.rows.len() >= self.state.opts.batch_size
        };
        if should_flush || self.state.opts.query_mode == ActionQueryMode::Sync {
            self.state.flush_pending(true).await?;
        } else {
            let _ = self.wake.try_send(());
        }
        Ok(())
    }

    fn owns_action_metrics(&self) -> bool {
        true
    }

    fn discard_buffered(&self) {
        self.state.discard_buffered();
    }

    fn health(&self) -> Option<SinkHealth> {
        Some(self.state.health.snapshot())
    }

    fn kind(&self) -> &'static str {
        self.state.inner.kind()
    }

    fn local_deliveries(&self) -> Option<u64> {
        self.state.inner.local_deliveries()
    }

    fn local_last_preview(&self) -> Option<String> {
        self.state.inner.local_last_preview()
    }
}

/// Wrap one built action sink in its resource-option runtime. The metric
/// record is created if the caller has not created it yet. The health
/// target is the connector's network target; a kind without one runs no
/// health probe.
pub(crate) fn wrap_action_runtime(
    engine: &broker_rules::RuleEngine,
    key: &str,
    sink: Arc<dyn Sink>,
    opts: ActionResourceOpts,
    health_target: Option<String>,
) -> Arc<dyn Sink> {
    engine.ensure_action_metrics(key);
    let metrics = engine
        .action_metrics_record(key)
        .unwrap_or_else(|| Arc::new(ActionMetrics::new()));
    Arc::new(ActionRuntimeSink::new(sink, opts, metrics, health_target))
}

/// One staged action-sink build: the sink (when this kind owns one
/// and the build succeeded) plus the wiring state the caller reports.
/// `Live` wiring always comes with a built `sink`.
pub(crate) struct ActionSinkStage {
    /// The built sink (`None` when the kind owns no runtime or its
    /// target connector is missing).
    pub sink: Option<Arc<dyn broker_connectors::Sink>>,
    /// The wiring state to report.
    pub wiring: ActionWiring,
    /// Parsed resource options for the action runtime.
    pub resource_opts: ActionResourceOpts,
    /// Network target for the periodic health probe.
    pub health_target: Option<String>,
    /// Run one probe before the write answers. The check runs after the
    /// secret resolver, so a target behind a secret reference is dialled
    /// too.
    pub probe_on_create: bool,
}

/// Resolve the connector an action references and stage its owned
/// sink (built, never registered).
///
/// - A kind with no owned runtime stages nothing with
///   [`ActionWiring::None`].
/// - A dangling connector reference is not a build failure: the
///   accepted conformance contract keeps such an action storable
///   (KC-00, `POST /actions` case with an unknown connector), so it
///   stages nothing and reports [`ActionWiring::Failed`] with the
///   reason. Nothing is dialled, no value invented.
/// - Everything else (missing field, bad parameter) is a build failure:
///   the caller refuses with a message that names the action and field.
pub(crate) fn stage_action_sink(
    connectors: &ConnectorStore,
    record: &ActionRecord,
) -> Result<ActionSinkStage, String> {
    if let Some(message) = super::rules::kind_refusal(&record.action_type) {
        return Err(message);
    }
    let resource_opts = ActionResourceOpts::parse(&record.id, &record.resource_opts)?;
    if !action_owned_kind(&record.action_type) {
        // This kind owns no action runtime yet, so only the documented
        // defaults are safe. A non-default value is refused instead of
        // stored as an ignored setting.
        if let Some(field) = resource_opts.first_non_default_field() {
            return Err(format!(
                "action {:?} field `resource_opts.{field}` is not supported for kind {:?}: this kind has no action runtime yet",
                record.id, record.action_type
            ));
        }
        return Ok(ActionSinkStage {
            sink: None,
            wiring: ActionWiring::None,
            resource_opts,
            health_target: None,
            probe_on_create: false,
        });
    }
    if canonical_action_kind(&record.action_type) == "console"
        && action_connector_references(record).is_empty()
    {
        // A console action without a connector has no captured-output
        // target. Keep the row storable with no runtime; the fire path
        // counts its failure. A non-default resource option needs a
        // runtime, so refuse it as for a kind without one.
        if let Some(field) = resource_opts.first_non_default_field() {
            return Err(format!(
                "action {:?} field `resource_opts.{field}` is not supported without a connector",
                record.id
            ));
        }
        return Ok(ActionSinkStage {
            sink: None,
            wiring: ActionWiring::None,
            resource_opts,
            health_target: None,
            probe_on_create: false,
        });
    }
    let reference = action_connector_references(record)
        .into_iter()
        .next()
        .ok_or_else(|| {
            format!(
                "action {:?} is missing required field: connector",
                record.id
            )
        })?;
    let Some((_, doc)) = super::rules::resolve_action_connector(connectors, &reference) else {
        // A dangling connector does not accept an action that is
        // missing its own required field.
        required_action_parameters(record)?;
        return Ok(ActionSinkStage {
            sink: None,
            wiring: ActionWiring::Failed(format!(
                "connector {reference:?} of the action not found (field `connector`)"
            )),
            resource_opts,
            health_target: None,
            probe_on_create: false,
        });
    };
    required_action_parameters(record)?;
    let sink = build_action_sink_with_dir(record, &doc, &resource_opts, &connectors.data_dir())?;
    let has_sink = sink.is_some();
    // A console sink logs locally and dials no target. It keeps no
    // health target, so it never reports the connector target state.
    // Other kinds probe the connector target.
    let health_target = if has_sink && canonical_action_kind(&record.action_type) != "console" {
        super::rules::extract_target_host_port(&doc)
    } else {
        None
    };
    Ok(ActionSinkStage {
        wiring: if has_sink {
            ActionWiring::Live
        } else {
            ActionWiring::None
        },
        sink,
        resource_opts,
        health_target,
        // Every built sink probes before the write answers, including a
        // target that arrives through a secret reference. The resolver
        // runs first, so the probe dials the real target.
        probe_on_create: has_sink,
    })
}

/// Build (never register) one action's owned sink from the referenced
/// connector document plus this action's own parameters. `Ok(None)`
/// marks a kind with no action-owned runtime. Every refusal names the
/// action and the offending field, so a refused create or update can
/// carry the reason straight to the operator.
#[cfg(test)]
pub(crate) fn build_action_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
    opts: &ActionResourceOpts,
) -> Result<Option<Arc<dyn broker_connectors::Sink>>, String> {
    build_action_sink_with_dir(record, connector_doc, opts, std::path::Path::new("./data"))
}

/// Build one action sink inside `data_dir`. Only `disk_log` uses the
/// directory. Other kinds ignore it.
pub(crate) fn build_action_sink_with_dir(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
    opts: &ActionResourceOpts,
    data_dir: &std::path::Path,
) -> Result<Option<Arc<dyn broker_connectors::Sink>>, String> {
    match canonical_action_kind(record.action_type.as_str()) {
        "pgsql" => build_pgsql_sink(record, connector_doc, opts).map(Some),
        "mysql" => build_mysql_sink(record, connector_doc).map(Some),
        "mssql" => build_mssql_sink(record, connector_doc).map(Some),
        "oracle" => build_oracle_sink(record, connector_doc).map(Some),
        "alloydb" => build_pg_family_sink("alloydb", record, connector_doc).map(Some),
        "cockroachdb" => build_pg_family_sink("cockroachdb", record, connector_doc).map(Some),
        "redshift" => build_pg_family_sink("redshift", record, connector_doc).map(Some),
        "webhook" | "http" => build_webhook_sink(record, connector_doc, opts).map(Some),
        "clickhouse" => build_clickhouse_sink(record, connector_doc).map(Some),
        "influxdb" => build_influxdb_sink(record, connector_doc).map(Some),
        "greptimedb" => build_greptimedb_sink(record, connector_doc).map(Some),
        "opentsdb" => build_opentsdb_sink(record, connector_doc).map(Some),
        "tdengine" => build_tdengine_sink(record, connector_doc).map(Some),
        "iotdb" => build_iotdb_sink(record, connector_doc).map(Some),
        "doris" => build_doris_sink(record, connector_doc).map(Some),
        "datalayers" => build_datalayers_sink(record, connector_doc).map(Some),
        "timestream" => build_timestream_sink(record, connector_doc).map(Some),
        "cassandra" => build_cassandra_sink(record, connector_doc).map(Some),
        "mongodb" => build_mongodb_sink(record, connector_doc).map(Some),
        "couchbase" => build_couchbase_sink(record, connector_doc).map(Some),
        "elasticsearch" => build_elasticsearch_sink(record, connector_doc).map(Some),
        "s3" => build_s3_object_sink(record, connector_doc, opts).map(Some),
        "azure_blob" => build_azure_blob_sink(record, connector_doc, opts).map(Some),
        "dynamodb" => build_dynamodb_action_sink(record, connector_doc, opts).map(Some),
        "rabbitmq" => build_rabbitmq_sink(record, connector_doc).map(Some),
        "bigquery" => build_bigquery_sink(record, connector_doc, opts).map(Some),
        "disk_log" => build_disk_log_action_sink_with_dir(record, connector_doc, data_dir, opts)
            .map(|sink| Some(sink as Arc<dyn broker_connectors::Sink>)),
        "snowflake" => build_snowflake_sink(record, connector_doc, opts).map(Some),
        "tablestore" => build_tablestore_sink(record, connector_doc, opts).map(Some),
        "opc_ua" => build_opc_ua_sink(record, connector_doc).map(Some),
        "sparkplug_b" => build_sparkplug_b_sink(record, connector_doc).map(Some),
        "console" => build_console_sink(record).map(Some),
        "kinesis" => build_kinesis_sink(record, connector_doc).map(Some),
        "gcp_pubsub" => build_gcp_pubsub_sink(record, connector_doc).map(Some),
        "azure_eventhubs" => build_azure_eventhubs_sink(record, connector_doc).map(Some),
        "aws_iot" => build_aws_iot_sink(record, connector_doc).map(Some),
        "azure_iot" => build_azure_iot_sink(record, connector_doc).map(Some),
        "oci_streaming" => build_oci_streaming_sink(record, connector_doc).map(Some),
        "kafka" => build_kafka_sink(record, connector_doc).map(Some),
        "confluent" => build_confluent_sink(record, connector_doc).map(Some),
        "pulsar" => build_pulsar_sink(record, connector_doc).map(Some),
        "rocketmq" => build_rocketmq_sink(record, connector_doc).map(Some),
        "mqtt_bridge" => build_mqtt_bridge_sink(record, connector_doc).map(Some),
        "redis" => build_redis_sink(record, connector_doc).map(Some),
        // Other kinds own no runtime yet: no sink, no registration.
        // Delivery stays refused-by-default (the fire counts one
        // failure) until the kind plots its own data path (KC-00).
        _ => Ok(None),
    }
}

/// True when every connector the record references participates
/// (`enable` absent reads enabled; a dangling reference reads enabled —
/// its delivery cannot resolve either way). Delivery requires the
/// action's own enable flag AND this: a connector disable stops every
/// dependent action, a connector enable restores them (R2).
pub(crate) fn action_connector_enable(connectors: &ConnectorStore, record: &ActionRecord) -> bool {
    action_connector_references(record).iter().all(|reference| {
        super::rules::resolve_action_connector(connectors, reference)
            .is_none_or(|(_, doc)| doc.get("enable").and_then(|v| v.as_bool()).unwrap_or(true))
    })
}

/// First present non-empty string among the alternative keys of one
/// document (connector bodies accept historic aliases).
fn first_string<'a>(
    doc: &'a serde_json::Value,
    keys: &[&str],
) -> Option<std::borrow::Cow<'a, str>> {
    keys.iter().find_map(|key| {
        doc.get(key)
            .and_then(|v| v.as_str().map(str::trim))
            .filter(|s| !s.is_empty())
            .map(std::borrow::Cow::Borrowed)
    })
}

/// Read one JSON parameter of the action record: the documented
/// `parameters` object first, the legacy flat extras (`params`) as
/// fallback. `None` when absent from both (an empty value reads as
/// unconfigured, never as a target).
fn param_value<'a>(record: &'a ActionRecord, key: &str) -> Option<&'a serde_json::Value> {
    record
        .parameters
        .get(key)
        .filter(|value| !value.is_null())
        .or_else(|| record.params.get(key))
        .filter(|value| !value.is_null())
}

/// Read one string parameter of the action record. `None` when absent
/// or blank (an empty value reads as unconfigured, never as a target).
fn param_string(record: &ActionRecord, key: &str) -> Option<String> {
    param_value(record, key)
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Read one string-list parameter of the action record: each entry is
/// trimmed, and blank entries are dropped. `None` when absent or blank.
fn param_string_list(record: &ActionRecord, key: &str) -> Vec<String> {
    param_value(record, key)
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// True when the value is a JSON object whose entries are all strings.
fn is_string_map(value: &serde_json::Value) -> bool {
    value
        .as_object()
        .is_some_and(|map| map.values().all(|entry| entry.is_string()))
}

/// True when the value is a non-empty JSON object of strings.
fn is_non_empty_string_map(value: &serde_json::Value) -> bool {
    value
        .as_object()
        .is_some_and(|map| !map.is_empty() && map.values().all(|entry| entry.is_string()))
}

/// True when the value is a non-empty JSON array of strings.
fn is_non_empty_string_list(value: &serde_json::Value) -> bool {
    value
        .as_array()
        .is_some_and(|items| !items.is_empty() && items.iter().all(|item| item.is_string()))
}

/// Read one string field of the reference `message` object of the
/// action record. `None` when the object or the field is absent.
fn param_message_field(record: &ActionRecord, key: &str) -> Option<String> {
    for source in [&record.parameters, &record.params] {
        if let Some(text) = source
            .get("message")
            .and_then(|message| message.get(key))
            .filter(|value| !value.is_null())
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            return Some(text.to_string());
        }
    }
    None
}

/// Read one string-map parameter into owned pairs. Non-string entries
/// are dropped; the caller validates emptiness first.
fn param_string_map(record: &ActionRecord, key: &str) -> std::collections::HashMap<String, String> {
    param_value(record, key)
        .and_then(|v| v.as_object())
        .map(|map| {
            map.iter()
                .filter_map(|(name, entry)| {
                    entry.as_str().map(|text| (name.clone(), text.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Check the action-owned value shapes before the sink is built.
/// Presence is checked against the merged view (the action parameter,
/// else the connector document).
pub(crate) fn required_action_parameters(record: &ActionRecord) -> Result<(), String> {
    if matches!(
        canonical_action_kind(record.action_type.as_str()),
        "pgsql" | "mysql" | "mssql" | "oracle" | "alloydb" | "cockroachdb" | "redshift"
    ) && param_string(record, "sql").is_none()
    {
        return Err(format!(
            "action {:?} is missing required field: sql (field `parameters.sql`)",
            record.id
        ));
    }
    let bad = |field: &str, detail: String| {
        format!("action {:?} field `parameters.{field}` {detail}", record.id)
    };
    match canonical_action_kind(record.action_type.as_str()) {
        "influxdb" | "greptimedb" => {
            if let Some(precision) = param_string(record, "precision") {
                if !["s", "ms", "us", "ns"].contains(&precision.as_str()) {
                    return Err(bad(
                        "precision",
                        format!("must be one of s, ms, us, ns: {precision:?}"),
                    ));
                }
            }
            if canonical_action_kind(record.action_type.as_str()) == "greptimedb" {
                if let Some(format) = param_string(record, "format") {
                    if !["sql", "influx"].contains(&format.as_str()) {
                        return Err(bad(
                            "format",
                            format!("must be one of sql, influx: {format:?}"),
                        ));
                    }
                }
            }
        }
        "tdengine" => {
            for field in ["metrics", "tags"] {
                if let Some(value) = param_value(record, field) {
                    if !is_non_empty_string_map(value) {
                        return Err(bad(
                            field,
                            "must be a non-empty object of string templates".to_string(),
                        ));
                    }
                }
            }
        }
        "iotdb" => {
            for field in ["measurements", "types"] {
                if let Some(value) = param_value(record, field) {
                    if !is_non_empty_string_list(value) && !value.is_null() {
                        return Err(bad(
                            field,
                            "must be a non-empty list of strings".to_string(),
                        ));
                    }
                }
            }
            let measurements = param_string_list(record, "measurements");
            let types = param_string_list(record, "types");
            if !measurements.is_empty() || !types.is_empty() {
                if measurements.len() != types.len() {
                    return Err(bad(
                        "types",
                        "must hold the same count as `parameters.measurements`".to_string(),
                    ));
                }
                for data_type in &types {
                    if parse_iotdb_data_type(data_type).is_none() {
                        return Err(bad(
                            "types",
                            format!("entry {data_type:?} must be one of boolean, int32, int64, float, double, text"),
                        ));
                    }
                }
            }
        }
        "doris" => {
            if let Some(format) = param_string(record, "format") {
                if format.to_lowercase() != "json" {
                    return Err(bad(
                        "format",
                        "must be `json`: the driver writes JSON stream loads only".to_string(),
                    ));
                }
            }
        }
        "timestream" => {
            if let Some(precision) = param_string(record, "precision") {
                if !["ms", "us", "ns"].contains(&precision.as_str()) {
                    return Err(bad(
                        "precision",
                        format!("must be one of ms, us, ns: the driver has no second resolution ({precision:?})"),
                    ));
                }
            }
            if let Some(measures) = param_value(record, "measures") {
                if !is_non_empty_string_map(measures) && !measures.is_null() {
                    return Err(bad(
                        "measures",
                        "must be a non-empty object of field to measure type".to_string(),
                    ));
                }
            }
            if let Some(dimensions) = param_value(record, "dimensions") {
                if !is_string_map(dimensions) && !dimensions.is_null() {
                    return Err(bad(
                        "dimensions",
                        "must be an object of string templates".to_string(),
                    ));
                }
            }
        }
        "opentsdb" => {
            if let Some(tags) = param_value(record, "tags") {
                if !is_string_map(tags) && !tags.is_null() {
                    return Err(bad(
                        "tags",
                        "must be an object of string templates".to_string(),
                    ));
                }
            }
        }
        "couchbase" => {
            if param_string(record, "sql").is_none() {
                return Err(format!(
                    "action {:?} is missing required field: sql (field `parameters.sql`)",
                    record.id
                ));
            }
        }
        "s3" => {
            for field in ["bucket", "key"] {
                if param_string(record, field).is_none() {
                    return Err(format!(
                        "action {:?} is missing required field: {field} (field `parameters.{field}`)",
                        record.id
                    ));
                }
            }
            refuse_object_upload_overrides(record)?;
        }
        "azure_blob" => {
            for field in ["container", "blob"] {
                if param_string(record, field).is_none() {
                    return Err(format!(
                        "action {:?} is missing required field: {field} (field `parameters.{field}`)",
                        record.id
                    ));
                }
            }
            refuse_object_upload_overrides(record)?;
        }
        "dynamodb" => {
            for field in ["table", "hash_key"] {
                if param_string(record, field).is_none() {
                    return Err(format!(
                        "action {:?} is missing required field: {field} (field `parameters.{field}`)",
                        record.id
                    ));
                }
            }
            if let Some(template) = param_string(record, "template") {
                if !template.trim().is_empty() {
                    serde_json::from_str::<serde_json::Value>(&template).map_err(|error| {
                        bad(
                            "template",
                            format!("must be valid JSON with placeholders: {error}"),
                        )
                    })?;
                }
            }
        }
        "bigquery" => {
            for field in ["dataset", "table"] {
                if param_string(record, field).is_none() {
                    return Err(format!(
                        "action {:?} is missing required field: {field} (field `parameters.{field}`)",
                        record.id
                    ));
                }
            }
        }
        "snowflake" => {
            for field in ["database", "schema", "pipe"] {
                if param_string(record, field).is_none() {
                    return Err(format!(
                        "action {:?} is missing required field: {field} (field `parameters.{field}`)",
                        record.id
                    ));
                }
            }
        }
        "kinesis" => {
            for field in ["stream_name", "partition_key"] {
                if param_string(record, field).is_none() {
                    return Err(format!(
                        "action {:?} is missing required field: {field} (field `parameters.{field}`)",
                        record.id
                    ));
                }
            }
        }
        "gcp_pubsub" => {
            let field = "pubsub_topic";
            if param_string(record, field).is_none() {
                return Err(format!(
                    "action {:?} is missing required field: {field} (field `parameters.{field}`)",
                    record.id
                ));
            }
        }
        "azure_eventhubs" => {
            let field = "event_hub";
            if param_string(record, field).is_none() && param_string(record, "hub").is_none() {
                return Err(format!(
                    "action {:?} is missing required field: {field} (field `parameters.{field}`)",
                    record.id
                ));
            }
        }
        "aws_iot" => {
            let field = "remote_topic";
            if param_string(record, field).is_none() && param_string(record, "topic").is_none() {
                return Err(format!(
                    "action {:?} is missing required field: {field} (field `parameters.{field}`)",
                    record.id
                ));
            }
        }
        "azure_iot" => {
            // The hub topic is fixed by the device; the action needs no
            // target field.
        }
        "oci_streaming" => {
            let field = "stream_id";
            if param_string(record, field).is_none() {
                return Err(format!(
                    "action {:?} is missing required field: {field} (field `parameters.{field}`)",
                    record.id
                ));
            }
        }
        "tablestore" => {
            for field in ["table_name", "measurement"] {
                if param_string(record, field).is_none() {
                    return Err(format!(
                        "action {:?} is missing required field: {field} (field `parameters.{field}`)",
                        record.id
                    ));
                }
            }
            if let Some(fields) = param_value(record, "fields") {
                if !fields.as_array().is_some_and(|items| !items.is_empty()) {
                    return Err(bad(
                        "fields",
                        "must be a non-empty list of {column, value} objects".to_string(),
                    ));
                }
            } else {
                return Err(format!(
                    "action {:?} is missing required field: fields (field `parameters.fields`)",
                    record.id
                ));
            }
        }
        "elasticsearch" => {
            let action = param_string(record, "action").ok_or_else(|| {
                format!(
                    "action {:?} is missing required field: action (field `parameters.action`)",
                    record.id
                )
            })?;
            if !["create", "update", "delete"].contains(&action.as_str()) {
                return Err(bad(
                    "action",
                    format!("must be one of create, update, delete: {action:?}"),
                ));
            }
            if param_string(record, "index").is_none() {
                return Err(format!(
                    "action {:?} is missing required field: index (field `parameters.index`)",
                    record.id
                ));
            }
            if action != "create" && param_string(record, "id").is_none() {
                return Err(format!(
                    "action {:?} is missing required field: id (field `parameters.id`)",
                    record.id
                ));
            }
        }
        "disk_log" => {
            if let Some(template) = param_string(record, "template") {
                if template != "${.}" {
                    return Err(bad(
                        "template",
                        "only the documented default `${.}` is supported: this driver writes the projected event as one record"
                            .to_string(),
                    ));
                }
            }
            if let Some(mode) = param_string(record, "write_mode") {
                if !["sync", "async"].contains(&mode.as_str()) {
                    return Err(bad(
                        "write_mode",
                        format!("must be `sync` or `async`: {mode:?}"),
                    ));
                }
            }
        }
        "opc_ua" => {
            if param_string(record, "node_id").is_none() {
                return Err(format!(
                    "action {:?} is missing required field: node_id (field `parameters.node_id`)",
                    record.id
                ));
            }
        }
        "sparkplug_b" => {
            if let Some(prefix) = param_string(record, "topic_prefix") {
                if prefix.contains('+') || prefix.contains('#') {
                    return Err(bad(
                        "topic_prefix",
                        "must be concrete: no `+` or `#`".to_string(),
                    ));
                }
                let parts: Vec<&str> = prefix.split('/').collect();
                if parts.first() != Some(&"spBv1.0") || parts.iter().any(|part| part.is_empty()) {
                    return Err(bad(
                        "topic_prefix",
                        "must start with `spBv1.0/`".to_string(),
                    ));
                }
            }
        }
        "kafka" | "confluent" => {
            if param_string(record, "topic").is_none() {
                return Err(format!(
                    "action {:?} is missing required field: topic (field `parameters.topic`)",
                    record.id
                ));
            }
            if let Some(strategy) = param_string(record, "partition_strategy") {
                if !["random", "key_dispatch"].contains(&strategy.as_str()) {
                    return Err(bad(
                        "partition_strategy",
                        format!("must be one of random, key_dispatch: {strategy:?}"),
                    ));
                }
            }
            if let Some(acks) = param_string(record, "required_acks") {
                if !["all_isr", "leader_only", "none"].contains(&acks.as_str()) {
                    return Err(bad(
                        "required_acks",
                        format!("must be one of all_isr, leader_only, none: {acks:?}"),
                    ));
                }
            }
            if let Some(mode) = param_string(record, "kafka_header_value_encode_mode") {
                if !["none", "json"].contains(&mode.as_str()) {
                    return Err(bad(
                        "kafka_header_value_encode_mode",
                        format!("must be one of none, json: {mode:?}"),
                    ));
                }
            }
            if param_value(record, "query_mode").is_some_and(|v| !v.is_null()) {
                return Err(bad(
                    "query_mode",
                    "is not supported for this kind: omit it, use `resource_opts.query_mode`"
                        .to_string(),
                ));
            }
            if let Some(message) = param_value(record, "message") {
                if !message.is_null() && !message.is_object() {
                    return Err(bad("message", "must be an object".to_string()));
                }
                if let Some(value) = message.get("value").filter(|v| !v.is_null()) {
                    let text = value.as_str().unwrap_or("");
                    if !["${.}", "${payload}"].contains(&text) {
                        return Err(bad(
                            "message.value",
                            "only the whole-message value is supported: omit it or use ${.}"
                                .to_string(),
                        ));
                    }
                }
                if let Some(stamp) = message.get("timestamp").filter(|v| !v.is_null()) {
                    let text = stamp.as_str().unwrap_or("");
                    if text != "${.timestamp}" {
                        return Err(bad(
                            "message.timestamp",
                            "only the event timestamp is supported: omit it or use ${.timestamp}"
                                .to_string(),
                        ));
                    }
                }
                if canonical_action_kind(record.action_type.as_str()) == "confluent"
                    && message.get("timestamp").is_some_and(|v| !v.is_null())
                {
                    return Err(bad(
                        "message.timestamp",
                        "the confluent message carries no timestamp: omit it".to_string(),
                    ));
                }
            }
            // Producer buffer and socket tuning belongs to the action
            // runtime (`resource_opts`, RO-01). The driver reads none
            // of these fields, so a present value is refused instead
            // of stored as an ignored setting.
            for field in [
                "buffer",
                "max_batch_age",
                "max_batch_bytes",
                "max_inflight",
                "max_linger_bytes",
                "max_linger_time",
                "max_retries",
                "partition_count_refresh_interval",
                "partitions_limit",
                "reconnect_delay",
                "send_buffer",
                "sync_query_timeout",
            ] {
                if param_value(record, field).is_some_and(|v| !v.is_null()) {
                    return Err(bad(
                        field,
                        "is not supported for this kind: batching and tuning belong to `resource_opts`"
                            .to_string(),
                    ));
                }
            }
            if let Some(compression) = param_string(record, "compression") {
                if compression != "no_compression" {
                    return Err(bad(
                        "compression",
                        "only no_compression is supported: the driver sends uncompressed"
                            .to_string(),
                    ));
                }
            }
        }
        "rabbitmq" => {
            if param_string(record, "routing_key").is_none() {
                return Err(format!(
                    "action {:?} is missing required field: routing_key (field `parameters.routing_key`)",
                    record.id
                ));
            }
            if let Some(mode) = param_string(record, "delivery_mode") {
                if !["non_persistent", "persistent"].contains(&mode.as_str()) {
                    return Err(bad(
                        "delivery_mode",
                        format!("must be one of non_persistent, persistent: {mode:?}"),
                    ));
                }
            }
            if let Some(headers) = param_value(record, "headers_template") {
                let items = headers.as_array().ok_or_else(|| {
                    bad(
                        "headers_template",
                        "must be an array of {key, value} templates".to_string(),
                    )
                })?;
                for item in items {
                    let ok = item.as_object().is_some_and(|map| {
                        map.get("key")
                            .and_then(|v| v.as_str())
                            .is_some_and(|s| !s.trim().is_empty())
                    }) && item.get("value").and_then(|v| v.as_str()).is_some();
                    if !ok {
                        return Err(bad(
                            "headers_template",
                            "each entry needs a non-empty `key` and a string `value` template"
                                .to_string(),
                        ));
                    }
                }
            }
            if let Some(props) = param_value(record, "properties_template") {
                let empty = props.as_array().is_some_and(|items| items.is_empty());
                if !empty {
                    return Err(bad(
                        "properties_template",
                        "only the empty list is supported: the broker sets content type, delivery mode and timestamp"
                            .to_string(),
                    ));
                }
            }
            // Confirmation tuning belongs to the driver call, which
            // always awaits the broker ack. A present value is refused
            // instead of stored as an ignored setting.
            for field in [
                "publish_confirmation_timeout",
                "wait_for_publish_confirmations",
            ] {
                if param_value(record, field).is_some_and(|v| !v.is_null()) {
                    return Err(bad(
                        field,
                        "is not supported for this kind: delivery always awaits the broker ack"
                            .to_string(),
                    ));
                }
            }
        }
        "redis" => match param_value(record, "command_template") {
            Some(value) => {
                let items = value.as_array().ok_or_else(|| {
                    bad(
                        "command_template",
                        "must be a non-empty array of strings".to_string(),
                    )
                })?;
                if items.is_empty()
                    || items
                        .iter()
                        .any(|entry| entry.as_str().is_none_or(|text| text.trim().is_empty()))
                {
                    return Err(bad(
                        "command_template",
                        "must be a non-empty array of non-blank strings".to_string(),
                    ));
                }
            }
            None => {
                return Err(format!(
                        "action {:?} is missing required field: command_template (field `parameters.command_template`)",
                        record.id
                    ));
            }
        },
        "mqtt_bridge" => {
            if param_string(record, "topic").is_none() {
                return Err(format!(
                    "action {:?} is missing required field: topic (field `parameters.topic`)",
                    record.id
                ));
            }
            if let Some(qos) = param_value(record, "qos").filter(|v| !v.is_null()) {
                let ok = qos.as_u64().is_some_and(|n| n <= 2)
                    || qos
                        .as_str()
                        .is_some_and(|text| ["0", "1", "2"].contains(&text.trim()));
                if !ok {
                    return Err(bad("qos", "must be one of 0, 1, 2".to_string()));
                }
            }
            if let Some(retain) = param_value(record, "retain").filter(|v| !v.is_null()) {
                if !retain.is_boolean() {
                    return Err(bad("retain", "must be a boolean".to_string()));
                }
            }
        }
        "pulsar" => {
            if param_string(record, "pulsar_topic").is_none() {
                return Err(format!(
                    "action {:?} is missing required field: pulsar_topic (field `parameters.pulsar_topic`)",
                    record.id
                ));
            }
            if let Some(strategy) = param_string(record, "strategy") {
                if !["random", "roundrobin", "key_dispatch"].contains(&strategy.as_str()) {
                    return Err(bad(
                        "strategy",
                        format!("must be one of random, roundrobin, key_dispatch: {strategy:?}"),
                    ));
                }
            }
            if let Some(message) = param_value(record, "message") {
                if !message.is_null() && !message.is_object() {
                    return Err(bad("message", "must be an object".to_string()));
                }
                if let Some(value) = message.get("value").filter(|v| !v.is_null()) {
                    let text = value.as_str().unwrap_or("");
                    if !["${.}", "${payload}"].contains(&text) {
                        return Err(bad(
                            "message.value",
                            "only the whole-message value is supported: omit it or use ${.}"
                                .to_string(),
                        ));
                    }
                }
            }
            // Producer buffer and socket tuning belongs to the action
            // runtime (`resource_opts`, RO-01). The driver reads none
            // of these fields, so a present value is refused instead
            // of stored as an ignored setting.
            for field in [
                "batch_size",
                "buffer",
                "max_batch_bytes",
                "max_inflight",
                "retention_period",
                "send_buffer",
                "sync_timeout",
            ] {
                if param_value(record, field).is_some_and(|v| !v.is_null()) {
                    return Err(bad(
                        field,
                        "is not supported for this kind: batching and tuning belong to `resource_opts`"
                            .to_string(),
                    ));
                }
            }
            if let Some(compression) = param_string(record, "compression") {
                if compression != "no_compression" {
                    return Err(bad(
                        "compression",
                        "only no_compression is supported: the driver sends uncompressed"
                            .to_string(),
                    ));
                }
            }
        }
        "rocketmq" => {
            if let Some(strategy) = param_string(record, "strategy") {
                if !["roundrobin", "key_dispatch"].contains(&strategy.as_str()) {
                    return Err(bad(
                        "strategy",
                        format!("must be one of roundrobin, key_dispatch: {strategy:?}"),
                    ));
                }
            }
            // A custom row template needs rule-output fields the sink
            // never sees. The empty template stores the whole message,
            // which is what the driver sends.
            // TODO(parity): MV-03a: execute a non-empty `template`, or
            // keep the refusal as documented behavior.
            if param_string(record, "template").is_some() {
                return Err(bad(
                    "template",
                    "only the empty template is supported: the driver stores the whole message"
                        .to_string(),
                ));
            }
            // Socket tuning belongs to the action runtime path, not the
            // driver call. A present value is refused instead of stored
            // as an ignored setting.
            for field in ["refresh_interval", "send_buffer", "sync_timeout"] {
                if param_value(record, field).is_some_and(|v| !v.is_null()) {
                    return Err(bad(
                        field,
                        "is not supported for this kind: tuning belongs to the runtime path"
                            .to_string(),
                    ));
                }
            }
        }
        _ => {}
    }
    Ok(())
}

/// The object-store driver joins the action `key` into the connector
/// endpoint URL. The reference also allows custom content, headers and
/// an ACL. The current driver writes the projected JSON body with the
/// driver content type, so a non-default value is refused instead of
/// accepted and ignored.
fn refuse_object_upload_overrides(record: &ActionRecord) -> Result<(), String> {
    if let Some(content) = param_string(record, "content") {
        if content != "${.}" {
            return Err(format!(
                "action {:?} field `parameters.content` value {content:?} is not supported: the driver writes the projected event body",
                record.id
            ));
        }
    }
    if let Some(headers) = param_value(record, "headers") {
        if !headers.as_object().is_some_and(|map| map.is_empty()) {
            return Err(format!(
                "action {:?} field `parameters.headers` is not supported: the driver writes the projected event body with the driver content type",
                record.id
            ));
        }
    }
    if let Some(acl) = param_string(record, "acl") {
        return Err(format!(
            "action {:?} field `parameters.acl` value {acl:?} is not supported: the driver writes a private object",
            record.id
        ));
    }
    Ok(())
}

/// Strip a connection target down to `host:port` the way the
/// connector builders do (`scheme://host:port/path` → `host:port`).
fn clean_server(server: &str) -> String {
    let server = server.split("://").last().unwrap_or(server);
    let server = server.split('/').next().unwrap_or(server);
    server.trim().trim_end_matches(':').to_string()
}

/// Build the action's `pgsql` sink: the connection string comes from
/// the connector (server, database, username, password — password
/// optional), the statement and the pool shape from this action.
///
/// Values and defaults:
/// - Connection fields: the transport receives `host`, `port`, `user`,
///   `password` and `database` as values through
///   [`broker_connectors::postgres::PgConnectionSettings`], so a
///   delimiter in a value cannot change the target. No invented
///   fallback: a missing `server`, `database` or `username` fails the
///   build (fail closed, S1-01) and the write that introduced it is
///   refused with the field named.
/// - `connection_url`: a display-only string without the password. The
///   transport never parses it.
/// - `sql_template`: this action's `parameters.sql`.
/// - `pool_size`: the connector setting, with the reference schema
///   default `8`; it bounds the action's concurrent batch connections.
/// - Batching belongs to the action runtime, not this driver. The inner
///   sink keeps one row per send with no linger, so the runtime owns the
///   batch size and the batch timer.
pub(crate) fn build_pgsql_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
    _opts: &ActionResourceOpts,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    let connector = record.connector.trim().to_string();
    let named = |field: &str| {
        format!(
            "connector {connector:?} is missing required field: {field} (action {:?})",
            record.id
        )
    };
    let raw_server = first_string(connector_doc, &["server", "host", "servers"])
        .ok_or_else(|| named("server"))?;
    let database = first_string(connector_doc, &["database"]).ok_or_else(|| named("database"))?;
    let username =
        first_string(connector_doc, &["username", "user"]).ok_or_else(|| named("username"))?;
    // A connector may store a secret reference instead of the password,
    // as the webhook connector does. Resolve it at build time so the
    // store keeps the reference and an unresolvable reference fails
    // closed instead of dialling with a placeholder.
    let raw_password = first_string(connector_doc, &["password"]);
    let password = raw_password
        .as_deref()
        .map(broker_connectors::resolve_field_secret)
        .unwrap_or_default();
    if raw_password
        .as_deref()
        .is_some_and(broker_config::secrets::is_secret_ref)
        && password.is_empty()
    {
        return Err(format!(
            "connector {connector:?} password reference is not resolvable (field `password`, action {:?})",
            record.id
        ));
    }
    let server = clean_server(&raw_server);
    if server.is_empty() {
        return Err(named("server"));
    }
    let (host, port) = match server.rsplit_once(':') {
        Some((host, port)) if port.parse::<u16>().is_ok() => (
            host.trim().trim_matches(&['[', ']'][..]).to_string(),
            port.parse::<u16>().map_err(|_| named("server"))?,
        ),
        _ => (server, 5432),
    };
    if host.is_empty() {
        return Err(named("server"));
    }
    let use_tls = pgsql_tls_enabled(connector_doc, &connector, &record.id)?;
    let sql = param_string(record, "sql").ok_or_else(|| {
        format!(
            "action {:?} is missing required field: sql (field `parameters.sql`)",
            record.id
        )
    })?;
    let pool_size = connector_doc
        .get("pool_size")
        .and_then(serde_json::Value::as_u64)
        .map(|v| v as usize)
        .unwrap_or(8);
    // One row and no linger: the action runtime groups rows and calls
    // `send_batch`, so this driver must not batch a second time.
    let batch_size = 1;
    let batch_timeout_ms = 0;
    let settings = broker_connectors::postgres::PgConnectionSettings {
        host,
        port,
        user: username.to_string(),
        password,
        database: database.to_string(),
        use_tls,
    };
    let transport =
        broker_connectors::postgres::DriverPgTransport::from_settings(settings, pool_size)
            .map_err(|e| {
                format!(
                    "action {:?} cannot build its pgsql transport: {e}",
                    record.id
                )
            })?;
    // The display form leaves out the password; the transport above owns
    // the connection fields and never parses this string.
    let display_url = format!(
        "postgres://{}@{}:{}/{}",
        username,
        connector_doc
            .get("server")
            .and_then(|v| v.as_str())
            .unwrap_or(""),
        port,
        database
    );
    let config = broker_connectors::postgres::PostgreSqlSinkConfig {
        connection_url: display_url,
        sql_template: sql,
        pool_size,
        batch_size,
        batch_timeout_ms,
    };
    let sink = broker_connectors::postgres::PostgreSqlSink::new(config, Arc::new(transport))
        .map_err(|e| format!("action {:?} cannot build its pgsql sink: {e}", record.id))?;
    Ok(Arc::new(sink))
}

/// Split a `Host[:Port]` connector value into its parts. A
/// `scheme://` prefix and any `/path` or `?query` suffix are stripped;
/// a `@` userinfo prefix is refused (credentials travel in their own
/// fields, never inside the host value). A missing port takes the
/// caller default (the protocol default the reference documents).
/// An empty host or a bad port fails closed naming the field.
fn parse_host_port(
    action_id: &str,
    connector: &str,
    field: &str,
    raw: &str,
    default_port: u16,
) -> Result<(String, u16), String> {
    let named = || {
        format!("connector {connector:?} is missing required field: {field} (action {action_id:?})")
    };
    let mut clean = raw.trim();
    if clean.is_empty() {
        return Err(named());
    }
    if let Some((_, rest)) = clean.split_once("://") {
        clean = rest;
    }
    if clean.contains('@') {
        return Err(format!(
            "connector {connector:?} field `{field}` must be `Host[:Port]`, got userinfo (action {action_id:?})"
        ));
    }
    clean = clean.split('/').next().unwrap_or(clean);
    clean = clean.split('?').next().unwrap_or(clean);
    // Bracketed IPv6 (`[::1]` or `[::1]:5432`).
    if let Some(rest) = clean.strip_prefix('[') {
        let Some(end) = rest.find(']') else {
            return Err(format!(
                "connector {connector:?} field `{field}` has an unbalanced bracket (action {action_id:?})"
            ));
        };
        let host = rest[..end].trim().to_string();
        if host.is_empty() {
            return Err(named());
        }
        let port = rest[end + 1..]
            .strip_prefix(':')
            .filter(|s| !s.is_empty())
            .map(|s| {
                s.parse::<u16>().map_err(|_| {
                    format!(
                        "connector {connector:?} field `{field}` has a bad port (action {action_id:?})"
                    )
                })
            })
            .transpose()?
            .unwrap_or(default_port);
        if port == 0 {
            return Err(format!(
                "connector {connector:?} field `{field}` has a bad port (action {action_id:?})"
            ));
        }
        return Ok((host, port));
    }
    match clean.rsplit_once(':') {
        Some((host, port_text)) if !port_text.is_empty() => {
            let host = host.trim();
            if host.is_empty() {
                return Err(named());
            }
            // A second colon without brackets is an unbracketed IPv6
            // literal: refuse it instead of guessing the port.
            if host.contains(':') {
                return Err(format!(
                    "connector {connector:?} field `{field}` must bracket IPv6 literals like `[::1]:{default_port}` (action {action_id:?})"
                ));
            }
            let port = port_text.trim().parse::<u16>().map_err(|_| {
                format!(
                    "connector {connector:?} field `{field}` has a bad port (action {action_id:?})"
                )
            })?;
            if port == 0 {
                return Err(format!(
                    "connector {connector:?} field `{field}` has a bad port (action {action_id:?})"
                ));
            }
            Ok((host.to_string(), port))
        }
        _ => {
            let host = clean.trim().trim_end_matches(':');
            if host.is_empty() || host.contains(':') {
                return Err(named());
            }
            Ok((host.to_string(), default_port))
        }
    }
}

/// Resolve the connector password: empty reads as unconfigured; a
/// `file:`/`env:` reference resolves at build time and fails closed
/// when unresolvable (never an empty credential standing in for one).
fn connector_password(
    action_id: &str,
    connector: &str,
    doc: &serde_json::Value,
) -> Result<String, String> {
    let raw = first_string(doc, &["password"])
        .map(|s| s.into_owned())
        .unwrap_or_default();
    if raw.trim().is_empty() {
        return Ok(String::new());
    }
    if broker_config::secrets::is_secret_ref(raw.trim()) {
        let resolved = broker_connectors::resolve_field_secret(&raw);
        if resolved.trim().is_empty() {
            return Err(format!(
                "connector {connector:?} password reference is not resolvable (field `password`, action {action_id:?})"
            ));
        }
        return Ok(resolved);
    }
    Ok(raw)
}

/// Connector pool size: the reference default is 8. Zero and values
/// above 1024 are refused: one connector keeps at most 1024 server
/// sessions, and the action runtime already caps concurrent driver
/// calls at its own inflight window.
fn connector_pool_size(
    action_id: &str,
    connector: &str,
    doc: &serde_json::Value,
) -> Result<usize, String> {
    match doc.get("pool_size") {
        None | Some(serde_json::Value::Null) => Ok(8),
        Some(value) => {
            let Some(number) = value.as_u64() else {
                return Err(format!(
                    "connector {connector:?} field `pool_size` must be an integer (action {action_id:?})"
                ));
            };
            let size = usize::try_from(number).unwrap_or(usize::MAX);
            if size == 0 || size > 1024 {
                return Err(format!(
                    "connector {connector:?} field `pool_size` must be between 1 and 1024 (action {action_id:?})"
                ));
            }
            Ok(size)
        }
    }
}

/// The action statement with positional markers bound by the driver.
/// Values travel out-of-band, so a quote in a value never escapes
/// into the statement text.
fn action_sql(record: &ActionRecord) -> Result<String, String> {
    param_string(record, "sql").ok_or_else(|| {
        format!(
            "action {:?} is missing required field: sql (field `parameters.sql`)",
            record.id
        )
    })
}

/// Build the action's `mysql` sink: the maintained `mysql_async`
/// driver dials `server`/`database` with `username` (reference
/// default `root`) and the resolved password; the action's
/// `parameters.sql` carries exactly three `?` markers bound to
/// (topic, QoS, payload). `ssl.enable` is refused: the driver
/// transport of this runtime has no TLS knob. Pool and batch
/// shape follow the `pgsql` builder: `pool_size` bounds server
/// sessions, one row per send with no linger (the action runtime
/// owns the batch size and the batch timer).
pub(crate) fn build_mysql_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    let connector = record.connector.trim().to_string();
    let named = |field: &str| {
        format!(
            "connector {connector:?} is missing required field: {field} (action {:?})",
            record.id
        )
    };
    if connector_doc
        .get("ssl")
        .and_then(|v| v.as_object())
        .and_then(|ssl| ssl.get("enable"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        // TODO(parity): the reference documents `ssl` for MySQL. The
        // rulebook does not decide whether the action runtime must
        // offer TLS; the current choice refuses it instead of
        // silently running plaintext against a TLS-only server.
        return Err(format!(
            "connector {connector:?} field `ssl.enable` is not supported for kind `mysql` (action {:?})",
            record.id
        ));
    }
    let raw_server =
        first_string(connector_doc, &["server", "host"]).ok_or_else(|| named("server"))?;
    let (host, port) = parse_host_port(&record.id, &connector, "server", &raw_server, 3306)?;
    let database = first_string(connector_doc, &["database"]).ok_or_else(|| named("database"))?;
    let username = first_string(connector_doc, &["username", "user"])
        .map(|s| s.into_owned())
        .unwrap_or_else(|| "root".to_string());
    let password = connector_password(&record.id, &connector, connector_doc)?;
    let pool_size = connector_pool_size(&record.id, &connector, connector_doc)?;
    let sql = action_sql(record)?;
    refuse_undefined_vars_as_null(record)?;
    // Display URL only: the credential travels as a driver value
    // through `from_parts`, never inside this string. It names the
    // target for diagnostics without carrying the secret.
    let display_url = format!("mysql://{username}@{host}:{port}/{database}");
    let config = broker_connectors::MySqlSinkConfig {
        connection_url: display_url,
        sql_template: sql,
        pool_size,
        batch_size: 1,
        batch_timeout_ms: 0,
    };
    config
        .validate()
        .map_err(|e| format!("action {:?} cannot build its mysql sink: {e}", record.id))?;
    let transport = broker_connectors::DriverMySqlTransport::from_parts(
        &host, port, &username, &password, &database, pool_size,
    )
    .map_err(|e| {
        format!(
            "action {:?} cannot build its mysql transport: {e}",
            record.id
        )
    })?;
    let sink = broker_connectors::MySqlSink::new(config, Arc::new(transport))
        .map_err(|e| format!("action {:?} cannot build its mysql sink: {e}", record.id))?;
    Ok(Arc::new(sink))
}

/// Read one non-default PostgreSQL-protocol connector option. The
/// reference documents the field but this runtime cannot apply it, so
/// a non-default value is refused with the field named instead of
/// being accepted and ignored.
fn refuse_pg_connector_override(
    action_id: &str,
    connector: &str,
    doc: &serde_json::Value,
    field: &str,
    is_default: impl Fn(&serde_json::Value) -> bool,
) -> Result<(), String> {
    if let Some(value) = doc.get(field) {
        if value.is_null() || is_default(value) {
            return Ok(());
        }
        return Err(format!(
            "connector {connector:?} field `{field}` is not supported (action {action_id:?})"
        ));
    }
    Ok(())
}

/// Check the `undefined_vars_as_null` action option of the relational
/// family. Absent or `false` keeps the fixed engine row. The row binds
/// topic, QoS and payload. These values are always defined. `true` asks
/// for missing variables as SQL NULL. These statements carry no
/// variables. The runtime refuses `true` and names the field.
fn refuse_undefined_vars_as_null(record: &ActionRecord) -> Result<(), String> {
    match record.parameters.get("undefined_vars_as_null") {
        None | Some(serde_json::Value::Null) => Ok(()),
        Some(serde_json::Value::Bool(false)) => Ok(()),
        Some(serde_json::Value::Bool(true)) => Err(format!(
            "action {:?} field `parameters.undefined_vars_as_null` value true is not supported: these statements bind (topic, qos, payload) positionally and carry no variables to render as null",
            record.id
        )),
        Some(_) => Err(format!(
            "action {:?} field `parameters.undefined_vars_as_null` must be a boolean",
            record.id
        )),
    }
}

/// Split the reference `ssl` object of a PostgreSQL-protocol
/// connector into the TLS choice. Absent or disabled reads as
/// plaintext; enabled reads as TLS through the system trust store.
/// An explicit `verify_none`, a custom CA, client certificate, key
/// or any other TLS knob is refused: the driver transport verifies
/// with the system roots only.
fn pg_family_tls(
    action_id: &str,
    connector: &str,
    doc: &serde_json::Value,
) -> Result<bool, String> {
    let Some(ssl) = doc.get("ssl") else {
        return Ok(false);
    };
    if ssl.is_null() {
        return Ok(false);
    }
    let Some(obj) = ssl.as_object() else {
        return Err(format!(
            "connector {connector:?} field `ssl` must be an object (action {action_id:?})"
        ));
    };
    if !obj.get("enable").and_then(|v| v.as_bool()).unwrap_or(false) {
        return Ok(false);
    }
    match obj.get("verify").and_then(|v| v.as_str()).map(str::trim) {
        None | Some("") | Some("verify_peer") => {}
        Some("verify_none") => {
            return Err(format!(
                "connector {connector:?} field `ssl.verify` value \"verify_none\" is not supported: this action runtime always verifies the server chain (action {action_id:?})"
            ));
        }
        Some(other) => {
            return Err(format!(
                "connector {connector:?} field `ssl.verify` value {other:?} is not supported (action {action_id:?})"
            ));
        }
    }
    for key in obj.keys() {
        if !matches!(key.as_str(), "enable" | "verify") {
            return Err(format!(
                "connector {connector:?} field `ssl.{key}` is not supported: this action runtime verifies with the system roots only (action {action_id:?})"
            ));
        }
    }
    Ok(true)
}

/// Map the connector `ssl` object onto the PostgreSQL transport. TLS is
/// on only when `ssl.enable` is true. The transport verifies the server
/// certificate against the system roots; a client certificate and a
/// custom CA file are refused here because this transport does not load
/// them, so the broker never accepts a setting it does not apply.
fn pgsql_tls_enabled(
    connector_doc: &serde_json::Value,
    connector: &str,
    action_id: &str,
) -> Result<bool, String> {
    let Some(ssl) = connector_doc.get("ssl") else {
        return Ok(false);
    };
    if !ssl.is_object() {
        return Err(format!(
            "connector {connector:?} field `ssl` must be an object (action {action_id:?})"
        ));
    }
    let enabled = ssl.get("enable").and_then(|v| v.as_bool()).unwrap_or(false);
    if !enabled {
        return Ok(false);
    }
    // The transport always verifies the server certificate against the
    // system trust store. Accept only the value that it applies and
    // refuse `verify_none` instead of accepting it and ignoring it.
    if let Some(verify) = ssl.get("verify").and_then(|v| v.as_str()) {
        match verify {
            "verify_peer" => {}
            "verify_none" => {
                return Err(format!(
                    "connector {connector:?} field `ssl.verify` value \"verify_none\" is not supported: this transport always verifies the server certificate (action {action_id:?})"
                ));
            }
            other => {
                return Err(format!(
                    "connector {connector:?} field `ssl.verify` value {other:?} is not supported (action {action_id:?})"
                ));
            }
        }
    }
    for (field, value) in [
        ("cacertfile", ssl.get("cacertfile")),
        ("certfile", ssl.get("certfile")),
        ("keyfile", ssl.get("keyfile")),
    ] {
        if value
            .and_then(|v| v.as_str())
            .is_some_and(|v| !v.trim().is_empty())
        {
            return Err(format!(
                "connector {connector:?} field `ssl.{field}` is not supported by this transport (action {action_id:?})"
            ));
        }
    }
    if let Some(name) = ssl
        .get("server_name_indication")
        .and_then(|v| v.as_str())
        .filter(|v| !v.is_empty() && *v != "disable")
    {
        return Err(format!(
            "connector {connector:?} field `ssl.server_name_indication` value {name:?} is not supported by this transport (action {action_id:?})"
        ));
    }
    Ok(true)
}

/// Build one PostgreSQL-protocol action sink (`alloydb`,
/// `cockroachdb`, `redshift`): the maintained `tokio-postgres` driver
/// dials `server`/`database` with `username` and the resolved
/// password; the action's `parameters.sql` carries `$1` (topic),
/// `$2` (QoS) and `$3` (payload) bound out-of-band. Differences from
/// the `pgsql` builder are exactly the documented ones: the default
/// port (CockroachDB 26257, Redshift 5439, AlloyDB 5432), the
/// reference `ssl` object (enabled maps to system-root TLS), and the
/// refused PostgreSQL options (`disable_prepared_statements: true`
/// has no effect here because every send prepares; an explicit
/// `application_name` is metadata this runtime never stamps).
/// Pool and batch shape follow the `pgsql` builder.
pub(crate) fn build_pg_family_sink(
    kind: &'static str,
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    let connector = record.connector.trim().to_string();
    let named = |field: &str| {
        format!(
            "connector {connector:?} is missing required field: {field} (action {:?})",
            record.id
        )
    };
    let default_port = match kind {
        "cockroachdb" => 26257,
        "redshift" => 5439,
        _ => 5432,
    };
    let raw_server = first_string(connector_doc, &["server", "host", "servers"])
        .ok_or_else(|| named("server"))?;
    let (host, port) =
        parse_host_port(&record.id, &connector, "server", &raw_server, default_port)?;
    let database = first_string(connector_doc, &["database"]).ok_or_else(|| named("database"))?;
    let username =
        first_string(connector_doc, &["username", "user"]).ok_or_else(|| named("username"))?;
    let password = connector_password(&record.id, &connector, connector_doc)?;
    let pool_size = connector_pool_size(&record.id, &connector, connector_doc)?;
    let use_tls = pg_family_tls(&record.id, &connector, connector_doc)?;
    refuse_undefined_vars_as_null(record)?;
    refuse_pg_connector_override(
        &record.id,
        &connector,
        connector_doc,
        "disable_prepared_statements",
        |v| v.as_bool().is_some_and(|b| !b),
    )?;
    refuse_pg_connector_override(
        &record.id,
        &connector,
        connector_doc,
        "application_name",
        // The name is metadata for the server's activity view. This
        // runtime never stamps it, so any explicit value is refused
        // instead of being accepted and ignored.
        |_| false,
    )?;
    let sql = action_sql(record)?;
    let display_url = if use_tls {
        format!("postgres://{username}@{host}:{port}/{database}?sslmode=require")
    } else {
        format!("postgres://{username}@{host}:{port}/{database}")
    };
    let config = broker_connectors::postgres::PostgreSqlSinkConfig {
        connection_url: display_url,
        sql_template: sql,
        pool_size,
        batch_size: 1,
        batch_timeout_ms: 0,
    };
    config
        .validate()
        .map_err(|e| format!("action {:?} cannot build its {kind} sink: {e}", record.id))?;
    let transport = broker_connectors::postgres::DriverPgTransport::from_parts(
        &host, port, &username, &password, &database, pool_size, use_tls,
    )
    .map_err(|e| {
        format!(
            "action {:?} cannot build its {kind} transport: {e}",
            record.id
        )
    })?;
    let sink = broker_connectors::postgres::PostgreSqlSink::new(config, Arc::new(transport))
        .map_err(|e| format!("action {:?} cannot build its {kind} sink: {e}", record.id))?;
    Ok(Arc::new(sink.with_kind(kind)))
}

/// Build the action's `mssql` sink: the maintained `tiberius` driver
/// dials `server` (default port 1433) with `database`, `username`
/// (reference default `sa`) and the resolved password; the action's
/// `parameters.sql` runs as a custom upsert with `$1` (event time),
/// `$2` (topic), `$3` (client id from the JSON payload, else empty),
/// `$4` (QoS) and `$5` (payload JSON) bound through
/// `sp_executesql`. The reference `driver` field accepts only the
/// default `ms-sql`: no other driver maps to this transport. The SSL
/// choice follows the enterprise default (encrypt, trust the chain)
/// unless the connector carries `ssl: {enable: true}`, which verifies
/// it. The table travels inside the SQL text, so the sink config
/// carries the action id only as the in-memory batch group, never as
/// a wire value. Pool and batch shape follow the `pgsql` builder.
pub(crate) fn build_mssql_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::mssql as enterprise_mssql;
    let connector = record.connector.trim().to_string();
    let named = |field: &str| {
        format!(
            "connector {connector:?} is missing required field: {field} (action {:?})",
            record.id
        )
    };
    let raw_server =
        first_string(connector_doc, &["server", "host"]).ok_or_else(|| named("server"))?;
    let (host, port) = parse_host_port(&record.id, &connector, "server", &raw_server, 1433)?;
    let database = first_string(connector_doc, &["database"]).ok_or_else(|| named("database"))?;
    let username = first_string(connector_doc, &["username", "user"])
        .map(|s| s.into_owned())
        .unwrap_or_else(|| "sa".to_string());
    let password = connector_password(&record.id, &connector, connector_doc)?;
    if let Some(driver) = connector_doc
        .get("driver")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        if !driver.eq_ignore_ascii_case("ms-sql") {
            return Err(format!(
                "connector {connector:?} field `driver` value {driver:?} is not supported: this action runtime dials with the default driver only (action {:?})",
                record.id
            ));
        }
    }
    let trust_server_certificate = !connector_doc
        .get("ssl")
        .and_then(|v| v.as_object())
        .and_then(|ssl| ssl.get("enable"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let pool_size = connector_pool_size(&record.id, &connector, connector_doc)?;
    let sql = action_sql(record)?;
    refuse_undefined_vars_as_null(record)?;
    let config = enterprise_mssql::MssqlSinkConfig {
        host,
        port: Some(port),
        database: database.to_string(),
        table_template: record.id.clone(),
        auth: enterprise_mssql::MssqlAuth::SqlPassword { username, password },
        query_mode: enterprise_mssql::MssqlQueryMode::CustomUpsert { sql_template: sql },
        trust_server_certificate,
        batch_size: Some(1),
        batch_bytes: None,
        linger_ms: Some(0),
        max_retries: Some(3),
        initial_backoff_ms: None,
        max_backoff_ms: None,
        timeout_ms: None,
    };
    config
        .validate()
        .map_err(|e| format!("action {:?} cannot build its mssql sink: {e}", record.id))?;
    let transport =
        enterprise_mssql::PooledMssqlTransport::new(&config, pool_size).map_err(|e| {
            format!(
                "action {:?} cannot build its mssql transport: {e}",
                record.id
            )
        })?;
    let sink = enterprise_mssql::MssqlSink::new(config, Arc::new(transport))
        .map_err(|e| format!("action {:?} cannot build its mssql sink: {e}", record.id))?;
    Ok(Arc::new(sink))
}

/// Build the action's `oracle` sink: the maintained `oracle_rs`
/// driver dials `server` (default port 1521, the reference default)
/// with `username`, the resolved password and `service_name` (or
/// `sid` when no service name is set; one of the two is required).
/// The `sysdba` role is refused (the driver transport has no role
/// knob). The action's `parameters.sql` carries `:1` (topic), `:2`
/// (QoS) and `:3` (payload) bound out-of-band. One pooled session
/// per `pool_size` entry dials lazily on first use; batches rotate
/// across entries. Failures map through the typed MV-13b
/// classifier. Pool and batch shape follow the `pgsql` builder: one
/// row per send at the driver, the action runtime owns batching.
pub(crate) fn build_oracle_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::oracle as enterprise_oracle;
    let connector = record.connector.trim().to_string();
    let named = |field: &str| {
        format!(
            "connector {connector:?} is missing required field: {field} (action {:?})",
            record.id
        )
    };
    let raw_server =
        first_string(connector_doc, &["server", "host"]).ok_or_else(|| named("server"))?;
    let (host, port) = parse_host_port(&record.id, &connector, "server", &raw_server, 1521)?;
    let username =
        first_string(connector_doc, &["username", "user"]).ok_or_else(|| named("username"))?;
    let password = connector_password(&record.id, &connector, connector_doc)?;
    // A service name wins when set; a bare `sid` dials by identifier.
    // One of the two is required.
    let service_name = first_string(connector_doc, &["service_name"])
        .map(|s| s.into_owned())
        .filter(|s| !s.trim().is_empty());
    let sid = first_string(connector_doc, &["sid"])
        .map(|s| s.into_owned())
        .filter(|s| !s.trim().is_empty());
    let (service, uses_sid) = match (service_name, sid) {
        (Some(name), _) => (name, false),
        (None, Some(sid)) => (sid, true),
        (None, None) => {
            return Err(format!(
                "connector {connector:?} is missing required field: service_name or sid (action {:?})",
                record.id
            ));
        }
    };
    if let Some(role) = connector_doc
        .get("role")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        if !role.eq_ignore_ascii_case("normal") {
            return Err(format!(
                "connector {connector:?} field `role` value {role:?} is not supported: this action runtime connects with the default role only (action {:?})",
                record.id
            ));
        }
    }
    let pool_size = connector_pool_size(&record.id, &connector, connector_doc)?;
    let sql = action_sql(record)?;
    refuse_undefined_vars_as_null(record)?;
    // Bound one dial attempt: the driver opens one TNS session per pool
    // entry on first use. Ten seconds caps a hung dial on the
    // management-plane action path without stalling the rule engine.
    let timeout = std::time::Duration::from_secs(10);
    let mut pool: Vec<Arc<dyn enterprise_oracle::OracleTransport>> = Vec::with_capacity(pool_size);
    for _ in 0..pool_size {
        let transport = if uses_sid {
            enterprise_oracle::DriverOracleTransport::with_sid(
                &host, port, &service, &username, &password, timeout,
            )
        } else {
            enterprise_oracle::DriverOracleTransport::new(
                &host, port, &service, &username, &password, timeout,
            )
        }
        .map_err(|e| {
            format!(
                "action {:?} cannot build its oracle transport: {e}",
                record.id
            )
        })?;
        pool.push(Arc::new(transport));
    }
    let sink = enterprise_oracle::OracleActionSink::new(sql, pool)
        .map_err(|e| format!("action {:?} cannot build its oracle sink: {e}", record.id))?;
    Ok(Arc::new(sink))
}

/// Build the action's `webhook`/`http` sink: the base URL comes from
/// the connector document, this action's `parameters` adds the path,
/// method and headers.
///
/// Values and defaults:
/// - URL: the connector's `url` (alias `endpoint`), required; a
///   missing or unresolvable (`file:`/`env:`) reference fails closed.
/// - `path`: optional URL path joined to the connector base (a
///   leading slash is added when absent).
/// - `method`: optional, `post` only (absent means `post`); the
///   action delivers with HTTP POST (the sink shape this runtime
///   implements).
/// - `headers`: optional object of string entries added to the POST.
pub(crate) fn build_webhook_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
    opts: &ActionResourceOpts,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    let connector = record.connector.trim().to_string();
    let named = |field: &str| {
        format!(
            "connector {connector:?} is missing required field: {field} (action {:?})",
            record.id
        )
    };
    let raw_url = first_string(connector_doc, &["url", "endpoint"]).ok_or_else(|| named("url"))?;
    // Connector URLs may be secret references; resolve at use time so
    // the store keeps the reference. An unresolvable reference fails
    // closed instead of dialling a redacted placeholder.
    let url = broker_connectors::resolve_field_secret(&raw_url);
    if url.trim().is_empty() {
        return Err(format!(
            "connector {connector:?} url reference is not resolvable (field `url`, action {:?})",
            record.id
        ));
    }
    // TODO(parity): does the reference `http` action schema document
    // methods beyond `post` (for example `get`)? The rulebook does not
    // decide; the current choice serves exactly `post` (the delivery
    // shape this runtime implements) and refuses every other value with
    // `BAD_REQUEST` until the checker pins it.
    if let Some(method) = param_string(record, "method") {
        if !method.eq_ignore_ascii_case("post") {
            return Err(format!(
                "action {:?} parameter method {method:?} unsupported: this action delivers with `post`",
                record.id
            ));
        }
    }
    let mut target = url.trim().trim_end_matches('/').to_string();
    if let Some(path) = param_string(record, "path") {
        if !path.starts_with('/') {
            target.push('/');
        }
        target.push_str(&path);
    }
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(value) = param_value(record, "headers") {
        let Some(map) = value.as_object() else {
            return Err(format!(
                "action {:?} field headers must be an object (field `parameters.headers`)",
                record.id
            ));
        };
        for (key, entry) in map {
            let Some(text) = entry.as_str() else {
                return Err(format!(
                    "action {:?} field headers.{key} must be a string",
                    record.id
                ));
            };
            let name = reqwest::header::HeaderName::from_bytes(key.as_bytes())
                .map_err(|_| format!("action {:?} header name {key:?} is invalid", record.id))?;
            // Do not echo the value: a header often carries a secret.
            let value = reqwest::header::HeaderValue::from_str(text).map_err(|_| {
                format!(
                    "action {:?} header value for {key:?} is invalid (field `parameters.headers`)",
                    record.id
                )
            })?;
            headers.insert(name, value);
        }
    }
    let mut client = reqwest::Client::builder();
    if let Some(ttl) = opts.request_ttl {
        client = client.timeout(ttl);
    }
    let client = client.build().map_err(|_| {
        format!(
            "action {:?} cannot build its webhook client (field `resource_opts.request_ttl`)",
            record.id
        )
    })?;
    Ok(Arc::new(broker_connectors::HttpWebhookSink::new(
        target, headers, client,
    )))
}

/// Build the action's `opc_ua` sink: the endpoint comes from the
/// connector, the node id comes from this action.
///
/// Values and defaults:
/// - Connector `endpoint_url` (aliases `server`, `url`, `endpoint`),
///   required; must start with `opc.tcp://`.
/// - Action `parameters.node_id`, required; must parse as a node id.
/// - Security stays at the documented default (anonymous, no policy,
///   no mode). A non-default value is refused with the field named.
/// - Sampling interval 1000 ms. Reason: OPC UA Part 4 sets
///   1000 ms as the default monitored-item sampling interval.
///   The server revises the rate.
/// - Publish topic template `opcua/${node.sanitized_id}`. Reason: the
///   rule engine routes by topic. The template carries the sanitized
///   node id.
/// - Write pattern `#`. Reason: the action target is the node id, so
///   every event topic routes to that node.
/// - One row per send with no linger. Reason: the action runtime owns
///   batching (RO-01).
/// - Buffer holds one row. Reason: one row bounds memory. The runtime
///   owns batching (RO-01).
pub(crate) fn build_opc_ua_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::opc_ua as enterprise_opc_ua;
    let connector = record.connector.trim().to_string();
    let endpoint_url = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["endpoint_url", "server", "url", "endpoint"],
        "endpoint_url",
    )?;
    if !endpoint_url.starts_with("opc.tcp://") {
        return Err(format!(
            "connector {connector:?} field `endpoint_url` must start with `opc.tcp://` (action {:?})",
            record.id
        ));
    }
    for key in ["security_policy", "security_mode", "auth"] {
        if let Some(value) = connector_doc.get(key) {
            if !value.is_null() {
                let text = value.as_str().unwrap_or("").trim().to_lowercase();
                let is_default = match key {
                    "security_policy" => text.is_empty() || text == "none",
                    "security_mode" => text.is_empty() || text == "none",
                    "auth" => value.as_object().is_none_or(|map| map.is_empty()),
                    _ => false,
                };
                if !is_default {
                    return Err(format!(
                        "connector {connector:?} field `{key}` is not supported: this action runtime uses anonymous with no policy or mode (action {:?})",
                        record.id
                    ));
                }
            }
        }
    }
    let node_id = param_string(record, "node_id").ok_or_else(|| {
        format!(
            "action {:?} is missing required field: node_id (field `parameters.node_id`)",
            record.id
        )
    })?;
    enterprise_opc_ua::OpcUaNodeId::parse(&node_id).map_err(|error| {
        format!(
            "action {:?} field `parameters.node_id` is invalid: {error}",
            record.id
        )
    })?;
    // Sampling interval 1000 ms. Reason: OPC UA Part 4 default.
    // The server revises the rate.
    const SAMPLING_INTERVAL_MS: u64 = 1000;
    let config = enterprise_opc_ua::OpcUaSinkConfig {
        endpoint_url,
        security_policy: enterprise_opc_ua::OpcUaSecurityPolicy::None,
        security_mode: enterprise_opc_ua::OpcUaSecurityMode::None,
        auth: enterprise_opc_ua::OpcUaAuth::Anonymous,
        node_subscriptions: vec![enterprise_opc_ua::NodeSubscriptionConfig {
            node_id: node_id.clone(),
            sampling_interval_ms: SAMPLING_INTERVAL_MS,
            publish_topic_template: "opcua/${node.sanitized_id}".to_string(),
            write_topic_pattern: Some("#".to_string()),
        }],
        buffer_capacity: Some(1),
        batch_size: Some(1),
        linger_ms: Some(0),
        timeout_ms: connector_timeout_ms(connector_doc),
    };
    let transport = enterprise_opc_ua::DriverOpcUaTransport::new(&config).map_err(|error| {
        format!(
            "action {:?} cannot build its opc_ua transport: {error}",
            record.id
        )
    })?;
    let sink = enterprise_opc_ua::OpcUaSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its opc_ua sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

/// Build the action's `sparkplug_b` sink: the MQTT server comes from
/// the connector, the namespace guard comes from this action.
///
/// Values and defaults:
/// - Connector `mqtt_url` (aliases `broker_address`, `server`,
///   `endpoint`, `url`), required; plain `mqtt://` only.
/// - Action `parameters.topic_prefix`, optional; when set it must
///   start with `spBv1.0/` and hold no wildcard.
/// - Client id from the connector `client_id`, else derived per
///   process by the driver. An empty value is refused.
/// - Tier is always `enterprise`. Reason: the driver requires it.
/// - One row per send with no linger. Reason: the action runtime owns
///   batching (RO-01).
/// - Alias ceiling 10_000. Reason: the driver default bounds the
///   edge alias table near 1 MiB.
pub(crate) fn build_sparkplug_b_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::sparkplug_b as enterprise_spb;
    let connector = record.connector.trim().to_string();
    let mqtt_url = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["mqtt_url", "broker_address", "server", "endpoint", "url"],
        "mqtt_url",
    )?;
    if let Some(client_id) = connector_doc.get("client_id").and_then(|v| v.as_str()) {
        if client_id.trim().is_empty() {
            return Err(format!(
                "connector {connector:?} field `client_id` must not be empty (action {:?})",
                record.id
            ));
        }
    }
    let topic_prefix = param_string(record, "topic_prefix");
    let client_id = connector_doc
        .get("client_id")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    // Alias ceiling 10_000. Reason: the driver default.
    const MAX_ALIASES: usize = 10_000;
    let config = enterprise_spb::SparkplugSinkConfig {
        topic_prefix: topic_prefix.clone(),
        tier: enterprise_spb::SPARKPLUG_TIER.to_string(),
        batch_size: Some(1),
        linger_ms: Some(0),
        timeout_ms: connector_timeout_ms(connector_doc),
        mqtt_url: Some(mqtt_url.clone()),
        client_id,
        max_aliases: Some(MAX_ALIASES),
    };
    // TODO(parity): which action field selects the Sparkplug group and
    // edge node? The rulebook does not decide. The current choice
    // guards on the optional `topic_prefix` only and forwards the
    // event topic unchanged.
    let transport =
        enterprise_spb::RumqttcSparkplugTransport::new(&mqtt_url, config.client_id.as_deref())
            .map_err(|error| {
                format!(
                    "action {:?} cannot build its sparkplug_b transport: {error}",
                    record.id
                )
            })?;
    let sink =
        enterprise_spb::SparkplugBSink::new(config, Arc::new(transport)).map_err(|error| {
            format!(
                "action {:?} cannot build its sparkplug_b sink: {error}",
                record.id
            )
        })?;
    Ok(Arc::new(sink))
}

/// Build the action's `console` sink. The sink keeps the captured
/// output for tests. It uses the action id as the sink name.
/// Reason: one action owns one sink, and the name shows the owner.
pub(crate) fn build_console_sink(
    record: &ActionRecord,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    Ok(Arc::new(broker_connectors::ConsoleLoggerSink::new(
        record.id.clone(),
    )))
}

/// Read one required connector field through its accepted key aliases.
/// A missing or blank value fails with the connector and field named.
fn connector_field(
    connector_doc: &serde_json::Value,
    connector: &str,
    action_id: &str,
    keys: &[&str],
    field: &str,
) -> Result<String, String> {
    first_string(connector_doc, keys)
        .map(|value| value.into_owned())
        .ok_or_else(|| {
            format!(
                "connector {connector:?} is missing required field: {field} (action {action_id:?})"
            )
        })
}

/// Read one required document-store connector field through its
/// accepted key aliases, top-level or inside the reference
/// `parameters` object.
#[cfg(any(feature = "mongodb", feature = "couchbase", feature = "elasticsearch"))]
fn document_connector_field(
    connector_doc: &serde_json::Value,
    connector: &str,
    action_id: &str,
    keys: &[&str],
    field: &str,
) -> Result<String, String> {
    first_string_nested(connector_doc, keys)
        .map(|value| value.into_owned())
        .ok_or_else(|| {
            format!(
                "connector {connector:?} is missing required field: {field} (action {action_id:?})"
            )
        })
}

/// Read one string from the connector document, then from its
/// reference-shaped `parameters` object.
#[cfg(any(feature = "mongodb", feature = "couchbase", feature = "elasticsearch"))]
fn first_string_nested<'a>(
    doc: &'a serde_json::Value,
    keys: &[&str],
) -> Option<std::borrow::Cow<'a, str>> {
    first_string(doc, keys).or_else(|| {
        doc.get("parameters")
            .and_then(|parameters| first_string(parameters, keys))
    })
}

/// Read one string from the connector document, top-level or inside
/// the reference `parameters` object. Message-broker connectors
/// accept both shapes.
fn nested_field<'a>(
    doc: &'a serde_json::Value,
    keys: &[&str],
) -> Option<std::borrow::Cow<'a, str>> {
    first_string(doc, keys).or_else(|| {
        doc.get("parameters")
            .and_then(|parameters| first_string(parameters, keys))
    })
}

/// Read one required connector field through its accepted key
/// aliases, top-level or inside `parameters`. A missing value fails
/// with the connector and the field named.
fn required_nested_field(
    connector_doc: &serde_json::Value,
    connector: &str,
    action_id: &str,
    keys: &[&str],
    field: &str,
) -> Result<String, String> {
    nested_field(connector_doc, keys)
        .map(|value| value.into_owned())
        .ok_or_else(|| {
            format!(
                "connector {connector:?} is missing required field: {field} (action {action_id:?})"
            )
        })
}

/// Normalize one HTTP(S) endpoint: a bare `host:port` reads as `http`.
/// Reason: the reference documents bare `Host[:Port]` entries, while
/// every driver below needs a URL scheme to dial.
fn http_endpoint(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    }
}

/// Endpoint for one GreptimeDB target. Reason: the reference default
/// is `127.0.0.1:4001`; a bare host keeps the documented 8086 port.
pub(crate) fn greptimedb_endpoint(raw_server: Option<&str>) -> String {
    let raw = raw_server
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("127.0.0.1:4001");
    if raw.contains("://") {
        raw.to_string()
    } else if raw.contains(':') {
        format!("http://{raw}")
    } else {
        format!("http://{raw}:8086")
    }
}

/// Endpoint for one Datalayers target. Reason: the reference default
/// is `127.0.0.1:8361`; any other value already carries its port.
pub(crate) fn datalayers_endpoint(raw_server: Option<&str>) -> String {
    let raw = raw_server
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("127.0.0.1:8361");
    http_endpoint(raw)
}

/// IoTDB REST endpoint: the driver posts `{endpoint}/insertTablet`
/// and its contract names `http://host:port/rest/v2`. A bare
/// `host:port` gains the `/rest/v2` base path; an endpoint that
/// already carries a path is kept (trailing slashes trimmed).
pub(crate) fn iotdb_endpoint(raw: &str) -> String {
    let base = http_endpoint(raw).trim_end_matches('/').to_string();
    let path = base
        .split("://")
        .nth(1)
        .unwrap_or("")
        .split('/')
        .nth(1)
        .unwrap_or("");
    if path.is_empty() {
        format!("{base}/rest/v2")
    } else {
        base
    }
}

/// Read the shared request-timeout knob of one connector document.
/// Reads `timeout_ms`, `timeout`, `request_timeout_ms` and the reference
/// `connect_timeout` (milliseconds or a duration string such as `15s`),
/// top-level or inside `parameters`. `None` keeps the driver default.
fn connector_timeout_ms(connector_doc: &serde_json::Value) -> Option<u64> {
    let params = connector_doc.get("parameters");
    for key in ["timeout_ms", "timeout", "request_timeout_ms"] {
        if let Some(value) = connector_doc
            .get(key)
            .or_else(|| params.and_then(|p| p.get(key)))
            .and_then(|value| value.as_u64())
        {
            return Some(value);
        }
    }
    if let Some(value) = connector_doc
        .get("connect_timeout")
        .or_else(|| params.and_then(|p| p.get("connect_timeout")))
    {
        if let Some(millis) = value.as_u64() {
            return Some(millis);
        }
        if let Some(text) = value.as_str() {
            if let Some(millis) = duration_str_to_ms(text) {
                return Some(millis);
            }
        }
    }
    None
}

/// Parse a duration string such as `15s`, `10s` or `100ms` to millis.
/// `None` when the text is not a duration.
fn duration_str_to_ms(text: &str) -> Option<u64> {
    let text = text.trim().to_ascii_lowercase();
    let split = text.find(|c: char| !c.is_ascii_digit() && c != '.')?;
    let (number, unit) = text.split_at(split);
    let number: f64 = number.parse().ok()?;
    if !number.is_finite() || number < 0.0 {
        return None;
    }
    let millis = match unit.trim() {
        "ms" => number,
        "s" => number * 1_000.0,
        "m" => number * 60.0 * 1_000.0,
        "h" => number * 3600.0 * 1_000.0,
        _ => return None,
    };
    if millis > u64::MAX as f64 {
        return None;
    }
    Some(millis as u64)
}

/// Refuse the pooled-connection and TLS fields of one document-store
/// connector. The timeout knob above is the applied one. The official
/// drivers own their pools and TLS, so an explicit pool, pipelining or
/// `ssl` value fails closed with 400 instead of being ignored.
pub(crate) fn validate_connector_io_bounds(
    connector_doc: &serde_json::Value,
    connector: &str,
    context: &str,
) -> Result<(), String> {
    let refused = |field: &str, why: &str| {
        format!("connector {connector:?} field `{field}` {why} ({context})")
    };
    let params = connector_doc.get("parameters");
    for key in ["pool_size", "pipelining", "enable_pipelining"] {
        let present = connector_doc
            .get(key)
            .or_else(|| params.and_then(|p| p.get(key)));
        if let Some(value) = present {
            if !value.is_null() {
                return Err(refused(key, "is not supported; remove it"));
            }
        }
    }
    if let Some(value) = connector_doc
        .get("max_inactive")
        .or_else(|| params.and_then(|p| p.get("max_inactive")))
    {
        if !value.is_null() {
            return Err(refused("max_inactive", "is not supported; remove it"));
        }
    }
    if let Some(value) = connector_doc
        .get("ssl")
        .or_else(|| params.and_then(|p| p.get("ssl")))
    {
        if !value.is_null() {
            return Err(refused("ssl", "is not supported; remove it"));
        }
    }
    Ok(())
}

/// Read one target field from the merged view: the action parameter
/// first, else the connector document. The connector carries the full
/// target (so the legacy connector-level path and SQL `INTO` keep
/// working); the action overrides it per target. A value missing in
/// both fails with the action and the connector field named.
fn merged_field(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
    action_key: &str,
    connector_keys: &[&str],
    field: &str,
) -> Result<String, String> {
    if let Some(value) = param_string(record, action_key) {
        return Ok(value);
    }
    first_string(connector_doc, connector_keys)
        .map(|value| value.into_owned())
        .ok_or_else(|| {
            let connector = record.connector.trim();
            format!(
                "action {:?} is missing required field: {field} (fields `parameters.{action_key}`, connector `{connector}`)",
                record.id
            )
        })
}

/// Read one target map from the merged view: the action parameter
/// first, else the connector document. Non-string entries are
/// dropped; the caller checks emptiness.
fn merged_map(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
    action_key: &str,
    connector_keys: &[&str],
) -> std::collections::HashMap<String, String> {
    let action_map = param_string_map(record, action_key);
    if !action_map.is_empty() {
        return action_map;
    }
    for key in connector_keys {
        if let Some(object) = connector_doc.get(key).and_then(|value| value.as_object()) {
            let map: std::collections::HashMap<String, String> = object
                .iter()
                .filter_map(|(name, entry)| {
                    entry.as_str().map(|text| (name.clone(), text.to_string()))
                })
                .collect();
            if !map.is_empty() {
                return map;
            }
        }
    }
    std::collections::HashMap::new()
}

/// Read one target list from the merged view: the action parameter
/// first, else the connector document.
fn merged_list(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
    action_key: &str,
    connector_keys: &[&str],
) -> Vec<String> {
    let action_list = param_string_list(record, action_key);
    if !action_list.is_empty() {
        return action_list;
    }
    for key in connector_keys {
        if let Some(items) = connector_doc.get(key).and_then(|value| value.as_array()) {
            let list: Vec<String> = items
                .iter()
                .filter_map(|item| item.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect();
            if !list.is_empty() {
                return list;
            }
        }
    }
    Vec::new()
}

/// Parse one IoTDB column type name (reference `data_type` values).
fn parse_iotdb_data_type(name: &str) -> Option<broker_connectors_enterprise::iotdb::IotDbDataType> {
    use broker_connectors_enterprise::iotdb::IotDbDataType;
    match name.trim().to_lowercase().as_str() {
        "boolean" => Some(IotDbDataType::Boolean),
        "int32" => Some(IotDbDataType::Int32),
        "int64" => Some(IotDbDataType::Int64),
        "float" => Some(IotDbDataType::Float),
        "double" => Some(IotDbDataType::Double),
        "text" => Some(IotDbDataType::Text),
        _ => None,
    }
}

/// Build the action's `clickhouse` sink: the endpoint and database come
/// from the connector; the action may override the target table.
///
/// Values and defaults:
/// - `url`: the connector's `url` (aliases `endpoint`, `server`),
///   required; a bare `host:port` reads as `http`.
/// - `database`: the connector's `database`, required.
/// - `username`: the connector's `username` (alias `user`); default
///   `default`, the ClickHouse server-side default user.
/// - `password`: the connector's `password`; default empty.
/// - `table`: the connector's target table, required; the action
///   parameter `table` overrides it per target.
/// - `format`: the insert format; default `JSONEachRow`, the sink
///   default row shape.
/// - The action-owned path posts `JSONEachRow` rows over HTTP: that
///   wire shape is proved by an in-repo loopback test on every run.
///   The legacy connector-level path keeps the native row driver,
///   whose shape the ignored qualification run proves.
/// - Batching belongs to the action runtime, not this driver. The
///   inner sink keeps one row per send with a short linger, so the
///   runtime owns the batch size and the batch timer.
#[cfg(feature = "clickhouse")]
pub(crate) fn build_clickhouse_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    let connector = record.connector.trim().to_string();
    let raw_url = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["url", "endpoint", "server"],
        "url",
    )?;
    let database = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["database"],
        "database",
    )?;
    let username = first_string(connector_doc, &["username", "user"])
        .map(|value| value.into_owned())
        .unwrap_or_else(|| "default".to_string());
    // A `file:` or `env:` password is resolved here; its text is never sent.
    let password = connector_password(&record.id, &connector, connector_doc)?;
    let table = merged_field(
        record,
        connector_doc,
        "table",
        &["table", "table_template"],
        "table",
    )?;
    let format = param_string(record, "format").unwrap_or_else(|| "JSONEachRow".to_string());
    let config = broker_connectors::clickhouse::ClickHouseSinkConfig {
        endpoint: http_endpoint(&raw_url),
        database,
        table,
        format,
        batch_size: 1,
        batch_timeout_ms: 10,
        username,
        password,
        request_timeout_ms: connector_timeout_ms(connector_doc),
    };
    let transport: std::sync::Arc<dyn broker_connectors::ClickHouseTransport> = std::sync::Arc::new(
        broker_connectors::clickhouse::HttpClickHouseTransport::new(
            &config,
            reqwest::Client::new(),
        )
        .map_err(|error| {
            format!(
                "action {:?} cannot build its clickhouse transport: {error}",
                record.id
            )
        })?,
    );
    let sink =
        broker_connectors::clickhouse::ClickHouseSink::new(config, transport).map_err(|error| {
            format!(
                "action {:?} cannot build its clickhouse sink: {error}",
                record.id
            )
        })?;
    Ok(Arc::new(sink))
}

/// The default build compiles the ClickHouse driver out: the kind is
/// known but the runtime is absent, so the build fails closed naming
/// the missing feature.
#[cfg(not(feature = "clickhouse"))]
pub(crate) fn build_clickhouse_sink(
    record: &ActionRecord,
    _connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    Err(format!(
        "action {:?} needs cargo feature `clickhouse` (disabled in this build)",
        record.id
    ))
}

/// Build the action's `influxdb` sink: the server, org, bucket and token
/// come from the connector, the measurement from this action.
///
/// Values and defaults:
/// - `server`: the connector's `server` (aliases `url`, `endpoint`);
///   default `127.0.0.1:8086`, the reference connector default.
/// - `bucket`, `org`, `token`: the connector's InfluxDB v2 identity,
///   all required. Only the v2 shape is supported; a v1 or v3 body
///   misses one of these fields and is refused naming it.
/// - `measurement`: the connector's measurement template, required;
///   the action parameter `measurement` overrides it per target.
/// - `parameters.precision`: one of `s`, `ms`, `us`, `ns`; default
///   `ms`, the reference action default.
/// - Batching belongs to the action runtime, not this driver. The
///   inner sink keeps one row per send with a short linger, so the
///   runtime owns the batch size and the batch timer.
pub(crate) fn build_influxdb_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    let connector = record.connector.trim().to_string();
    let raw_server = first_string(connector_doc, &["server", "url", "endpoint"])
        .map(|value| value.into_owned())
        .unwrap_or_else(|| "127.0.0.1:8086".to_string());
    // The reference connector keeps the bucket, org and token inside its
    // `parameters` object; the native form names them at the top level.
    let bucket =
        required_nested_field(connector_doc, &connector, &record.id, &["bucket"], "bucket")?;
    let org = required_nested_field(connector_doc, &connector, &record.id, &["org"], "org")?;
    let token = required_nested_field(connector_doc, &connector, &record.id, &["token"], "token")?;
    let measurement = match param_string(record, "measurement") {
        Some(value) => value,
        None => required_nested_field(
            connector_doc,
            &connector,
            &record.id,
            &["measurement", "measurement_template"],
            "measurement",
        )?,
    };
    let precision = param_string(record, "precision").unwrap_or_else(|| "ms".to_string());
    let config = broker_connectors::InfluxDbSinkConfig {
        endpoint: http_endpoint(&raw_server),
        bucket,
        org,
        token,
        measurement_template: measurement,
        precision,
        batch_size: 1,
        batch_timeout_ms: 10,
        request_timeout_ms: connector_timeout_ms(connector_doc),
    };
    let sink =
        broker_connectors::InfluxDbSink::new(config, reqwest::Client::new()).map_err(|error| {
            format!(
                "action {:?} cannot build its influxdb sink: {error}",
                record.id
            )
        })?;
    Ok(Arc::new(sink))
}

/// Build the action's `greptimedb` sink: the server, database and
/// credentials come from the connector; the action may override the
/// table.
///
/// Values and defaults:
/// - `server`: the connector's `server` (aliases `url`, `endpoint`,
///   `host`); default `127.0.0.1:4001`, the reference connector
///   default. A bare host without a port dials `:8086`, the
///   reference description default port.
/// - `database`: the connector's `database` (alias `dbname`),
///   required.
/// - `username`/`password`: the connector's pair; both or neither (a
///   half pair is refused fail closed, it cannot authenticate).
/// - `table`: the connector's table template, required; the action
///   parameter `table` overrides it per target.
/// - `precision`: one of `s`, `ms`, `us`, `ns`; default `ms`, the
///   reference action default. The action parameter wins; else the
///   connector value applies.
/// - `format`: `sql` or `influx`; default `sql`, the sink default
///   write shape. The action parameter wins; else the connector
///   value applies.
/// - `collision_seq_column`: optional SQL column name used to
///   disambiguate same-timestamp collisions when writing to a
///   deduplicating GreptimeDB table; omitted (`None`) for
///   `append_mode` tables.
/// - Batching belongs to the action runtime, not this driver. The
///   inner sink keeps one row per send with a short linger, so the
///   runtime owns the batch size and the batch timer.
pub(crate) fn build_greptimedb_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors::greptimedb::{
        GreptimeDbConfig, GreptimeDbSink, GreptimeFormat, GreptimePrecision,
        HttpGreptimeDbTransport,
    };
    let connector = record.connector.trim().to_string();
    let raw_server = first_string(connector_doc, &["server", "url", "endpoint", "host"])
        .map(|value| value.into_owned());
    let endpoint = greptimedb_endpoint(raw_server.as_deref());
    let database = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["database", "dbname"],
        "database",
    )?;
    let username =
        first_string(connector_doc, &["username", "user"]).map(|value| value.into_owned());
    let password = first_string(connector_doc, &["password"]).map(|value| value.into_owned());
    let auth = match (username, password) {
        (Some(username), Some(password)) => {
            Some(broker_connectors::greptimedb::GreptimeDbAuth { username, password })
        }
        (None, None) => None,
        _ => {
            return Err(format!(
                "connector {connector:?} fields `username` and `password` must be set together (action {:?})",
                record.id
            ));
        }
    };
    let table = merged_field(
        record,
        connector_doc,
        "table",
        &["table", "table_template"],
        "table",
    )?;
    let precision = match param_string(record, "precision")
        .or_else(|| first_string(connector_doc, &["precision"]).map(|value| value.into_owned()))
        .unwrap_or_else(|| "ms".to_string())
        .as_str()
    {
        "ns" => GreptimePrecision::Nanosecond,
        "us" => GreptimePrecision::Microsecond,
        "ms" => GreptimePrecision::Millisecond,
        "s" => GreptimePrecision::Second,
        other => {
            return Err(format!(
                "action {:?} field `parameters.precision` must be one of s, ms, us, ns: {other:?}",
                record.id
            ));
        }
    };
    let format = match param_string(record, "format")
        .or_else(|| first_string(connector_doc, &["format"]).map(|value| value.into_owned()))
        .unwrap_or_else(|| "sql".to_string())
        .as_str()
    {
        "sql" => GreptimeFormat::SqlInsert,
        "influx" => GreptimeFormat::InfluxLineProtocol,
        other => {
            return Err(format!(
                "action {:?} field `parameters.format` must be one of sql, influx: {other:?}",
                record.id
            ));
        }
    };
    let collision_seq_column = param_string(record, "collision_seq_column")
        .or_else(|| {
            first_string(connector_doc, &["collision_seq_column"]).map(|value| value.into_owned())
        })
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let config = GreptimeDbConfig {
        endpoint,
        database,
        auth,
        format,
        table_template: table,
        timestamp_precision: precision,
        batch_size: Some(1),
        buffer_capacity: None,
        timeout_ms: connector_timeout_ms(connector_doc),
        collision_seq_column,
    };
    let transport = Arc::new(HttpGreptimeDbTransport::new(&config));
    let sink = GreptimeDbSink::new(config, transport).map_err(|error| {
        format!(
            "action {:?} cannot build its greptimedb sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

/// Build the action's `opentsdb` sink: the endpoint comes from the
/// connector; the action may override the metric and value field.
///
/// Values and defaults:
/// - `server`: the connector's `server` (aliases `url`, `endpoint`),
///   required; a `telnet://` scheme selects the telnet wire, `http`
///   selects the HTTP wire.
/// - `metric`: the connector's metric template, required; the action
///   parameter `metric` overrides it per target.
/// - `value`: the connector's value field, required; the action
///   parameter `value` overrides it per target.
/// - `parameters.tags`: this action's tag templates; default empty,
///   and every point still carries the `topic` tag.
/// - `summary`: ask for summary counters; default `true`, the
///   reference default. The action parameter wins; else the
///   connector value applies.
/// - Batching belongs to the action runtime, not this driver. The
///   inner sink keeps one row per send with a short linger, so the
///   runtime owns the batch size and the batch timer.
pub(crate) fn build_opentsdb_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors::opentsdb::{
        NetworkOpenTsdbTransport, OpenTsdbCompression, OpenTsdbConfig, OpenTsdbProtocol,
        OpenTsdbSink,
    };
    let connector = record.connector.trim().to_string();
    let raw_endpoint = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["server", "endpoint", "url"],
        "server",
    )?;
    let protocol = if raw_endpoint.trim_start().starts_with("telnet://") {
        OpenTsdbProtocol::Telnet
    } else {
        OpenTsdbProtocol::Http
    };
    let endpoint = match protocol {
        OpenTsdbProtocol::Telnet => raw_endpoint.trim().to_string(),
        OpenTsdbProtocol::Http => http_endpoint(&raw_endpoint),
    };
    let metric = merged_field(
        record,
        connector_doc,
        "metric",
        &["metric", "metric_template"],
        "metric",
    )?;
    let value = merged_field(
        record,
        connector_doc,
        "value",
        &["value", "value_field"],
        "value",
    )?;
    let tags = merged_map(record, connector_doc, "tags", &["tags", "tag_mappings"]);
    let summary = param_value(record, "summary")
        .and_then(|value| value.as_bool())
        .or_else(|| {
            connector_doc
                .get("summary")
                .and_then(|value| value.as_bool())
        })
        .unwrap_or(true);
    let config = OpenTsdbConfig {
        endpoint,
        protocol,
        metric_template: metric,
        tag_mappings: tags,
        value_field: value,
        summary,
        compression: OpenTsdbCompression::None,
        batch_size: Some(1),
        buffer_capacity: None,
        timeout_ms: connector_timeout_ms(connector_doc),
    };
    let transport = Arc::new(NetworkOpenTsdbTransport::new(&config));
    let sink = OpenTsdbSink::new(config, transport).map_err(|error| {
        format!(
            "action {:?} cannot build its opentsdb sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

/// Build the action's `tdengine` sink: the endpoint and database come
/// from the connector; the action may override the super-table,
/// sub-table and row shape.
///
/// Values and defaults:
/// - `server`: the connector's `server` (aliases `url`, `endpoint`),
///   required; a bare `host:port` reads as `http`.
/// - `database`: the connector's `database`, required.
/// - `username`/`password`: default `root`/`taosdata`, the server-side
///   default credentials.
/// - `stable`: the connector's super-table, required; the action
///   parameter `stable` overrides it per target.
/// - `subtable`: the connector's sub-table template, required; the
///   action parameter `subtable` overrides it per target.
/// - `metrics`: the metric column templates (non-empty object),
///   required in the merged view; the action parameter `metrics`
///   overrides the connector value.
/// - `tags`: the tag column templates (non-empty object), required
///   in the merged view. The driver only writes super-table rows,
///   which need at least one tag column.
/// - Batching belongs to the action runtime, not this driver. The
///   inner sink keeps one row per send with a short linger, so the
///   runtime owns the batch size and the batch timer.
pub(crate) fn build_tdengine_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::tdengine::{
        DriverTdengineTransport, HttpTdengineTransport, TdengineAuth, TdengineSink,
        TdengineSinkConfig,
    };
    let connector = record.connector.trim().to_string();
    let raw_server = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["server", "url", "endpoint"],
        "server",
    )?;
    let database = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["database"],
        "database",
    )?;
    let username =
        first_string(connector_doc, &["username", "user"]).map(|value| value.into_owned());
    let password = first_string(connector_doc, &["password"]).map(|value| value.into_owned());
    let token =
        first_string(connector_doc, &["token", "auth_token"]).map(|value| value.into_owned());
    // A half Basic pair cannot authenticate: refuse it. A token
    // selects token auth and never mixes with Basic. Absent
    // credentials read the server-side defaults.
    let auth = match (username, password, token) {
        (Some(username), Some(password), None) => TdengineAuth::Basic { username, password },
        (None, None, Some(token)) => TdengineAuth::Token { token },
        (None, None, None) => TdengineAuth::Basic {
            username: "root".to_string(),
            password: "taosdata".to_string(),
        },
        _ => {
            return Err(format!(
                "connector {connector:?} fields `username` and `password` must be set together, and `token` never mixes with them (action {:?})",
                record.id
            ));
        }
    };
    let stable = merged_field(
        record,
        connector_doc,
        "stable",
        &["stable", "stable_name", "table"],
        "stable",
    )?;
    let subtable = merged_field(
        record,
        connector_doc,
        "subtable",
        &["subtable", "subtable_template"],
        "subtable",
    )?;
    let metrics = merged_map(
        record,
        connector_doc,
        "metrics",
        &["metrics", "metrics_template"],
    );
    if metrics.is_empty() {
        return Err(format!(
            "action {:?} is missing required field: metrics (fields `parameters.metrics`, connector `metrics`)",
            record.id
        ));
    }
    let tags = merged_map(record, connector_doc, "tags", &["tags", "tags_template"]);
    if tags.is_empty() {
        return Err(format!(
            "action {:?} is missing required field: tags (fields `parameters.tags`, connector `tags`)",
            record.id
        ));
    }
    let config = TdengineSinkConfig {
        endpoint: http_endpoint(&raw_server),
        database,
        stable_name: stable,
        subtable_template: subtable,
        auth,
        tags_template: tags,
        metrics_template: metrics,
        batch_size: Some(1),
        batch_bytes: Some(1_048_576),
        linger_ms: Some(10),
        buffer_capacity: None,
        max_retries: Some(4),
        initial_backoff_ms: Some(100),
        max_backoff_ms: Some(2_000),
        timeout_ms: connector_timeout_ms(connector_doc),
    };
    // Same production split as the connector path: Basic credentials
    // over plaintext HTTP ride the maintained driver, anything else
    // stays on the REST transport.
    if config.use_driver() {
        let transport = DriverTdengineTransport::new(&config).map_err(|error| {
            format!(
                "action {:?} cannot build its tdengine transport: {error}",
                record.id
            )
        })?;
        let sink = TdengineSink::new(config, Arc::new(transport)).map_err(|error| {
            format!(
                "action {:?} cannot build its tdengine sink: {error}",
                record.id
            )
        })?;
        return Ok(Arc::new(sink));
    }
    let client = reqwest::Client::builder()
        .timeout(config.timeout())
        .build()
        .unwrap_or_default();
    let transport = HttpTdengineTransport::new(&config, client).map_err(|error| {
        format!(
            "action {:?} cannot build its tdengine transport: {error}",
            record.id
        )
    })?;
    let sink = TdengineSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its tdengine sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

/// Build the action's `iotdb` sink: the endpoint and credentials come
/// from the connector; the action may override the device path and
/// tablet columns.
///
/// Values and defaults:
/// - `endpoint`: the connector's `endpoint` (aliases `server`, `url`,
///   `base_url`), required; a bare `host:port` reads as `http` and
///   gains the `/rest/v2` base path.
/// - `username`/`password`: the connector's pair, both required (the
///   tablet call authenticates every request).
/// - `device`: the connector's device path template, required; the
///   action parameter `device` overrides it per target.
/// - `measurements`: the connector's column names (non-empty list),
///   required in the merged view.
/// - `types`: the connector's column types, one per measurement
///   (`boolean`, `int32`, `int64`, `float`, `double`, `text`),
///   required in the merged view.
/// - `is_aligned`: the action parameter, else the connector value;
///   default false, the reference default.
/// - Batching belongs to the action runtime, which hands each batch
///   to the sink in one `send_batch` call. The sink writes one
///   `insertTablet` request per device for that batch.
pub(crate) fn build_iotdb_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::iotdb::{
        HttpIotDbTransport, IotDbAuth, IotDbDataType, IotDbSink, IotDbSinkConfig,
    };
    let connector = record.connector.trim().to_string();
    let raw_endpoint = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["endpoint", "server", "url", "base_url"],
        "endpoint",
    )?;
    let username = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["username", "user"],
        "username",
    )?;
    let password = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["password"],
        "password",
    )?;
    let device = merged_field(
        record,
        connector_doc,
        "device",
        &["device", "device_path", "device_path_template"],
        "device",
    )?;
    let measurements = merged_list(record, connector_doc, "measurements", &["measurements"]);
    if measurements.is_empty() {
        return Err(format!(
            "action {:?} is missing required field: measurements (fields `parameters.measurements`, connector `measurements`)",
            record.id
        ));
    }
    let type_names = merged_list(record, connector_doc, "types", &["types", "data_types"]);
    let mut data_types: Vec<IotDbDataType> = Vec::with_capacity(type_names.len());
    for name in &type_names {
        data_types.push(parse_iotdb_data_type(name).ok_or_else(|| {
            format!(
                "action {:?} field `parameters.types` entry {name:?} must be one of boolean, int32, int64, float, double, text",
                record.id
            )
        })?);
    }
    if measurements.len() != data_types.len() {
        return Err(format!(
            "action {:?} fields `parameters.measurements` and `parameters.types` must hold the same count",
            record.id
        ));
    }
    let is_aligned = match param_value(record, "is_aligned").or_else(|| {
        connector_doc
            .get("is_aligned")
            .filter(|value| !value.is_null())
    }) {
        None => false,
        Some(value) => value.as_bool().ok_or_else(|| {
            format!(
                "action {:?} field `parameters.is_aligned` must be a boolean",
                record.id
            )
        })?,
    };
    let config = IotDbSinkConfig {
        endpoint: iotdb_endpoint(&raw_endpoint),
        device_path_template: device,
        auth: IotDbAuth { username, password },
        is_aligned,
        measurements,
        data_types,
        batch_size: Some(1),
        batch_bytes: Some(2_097_152),
        linger_ms: Some(10),
        max_retries: Some(3),
        initial_backoff_ms: Some(100),
        max_backoff_ms: Some(2_000),
        timeout_ms: connector_timeout_ms(connector_doc),
    };
    let client = reqwest::Client::builder()
        .timeout(config.timeout())
        .build()
        .unwrap_or_default();
    let transport = HttpIotDbTransport::new(&config, client).map_err(|error| {
        format!(
            "action {:?} cannot build its iotdb transport: {error}",
            record.id
        )
    })?;
    let sink = IotDbSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its iotdb sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

/// Build the action's `doris` sink: the front-end, database and
/// credentials come from the connector; the action may override the
/// target table.
///
/// Values and defaults:
/// - `server`: the connector's `server` (aliases `fe_host`, `host`,
///   `endpoint`), required.
/// - `database`: the connector's `database`, required.
/// - `username`: default `root`, the reference connector default.
/// - `password`: the connector's `password`; default empty.
/// - `table`: the connector's table, required; the action parameter
///   `table` overrides it per target.
/// - The driver writes JSON stream loads only; an explicit
///   non-`json` `parameters.format` is refused.
/// - Batching belongs to the action runtime, not this driver. The
///   inner sink keeps one row per send with a short linger, so the
///   runtime owns the batch size and the batch timer.
pub(crate) fn build_doris_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::doris::{
        DorisAuth, DorisFormat, DorisSink, DorisSinkConfig, HttpDorisTransport,
    };
    let connector = record.connector.trim().to_string();
    let raw_server = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["server", "fe_host", "host", "endpoint"],
        "server",
    )?;
    let no_scheme = raw_server
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let fe_host = no_scheme
        .split([':', '/'])
        .next()
        .unwrap_or("")
        .trim()
        .to_string();
    if fe_host.is_empty() {
        return Err(format!(
            "connector {connector:?} is missing required field: server (action {:?})",
            record.id
        ));
    }
    // The port rides in `server` (`host:port`); an explicit
    // `http_port` (alias `port`) wins, else the server port
    // applies, else the Doris front-end default HTTP port.
    let server_port = no_scheme
        .split('/')
        .next()
        .unwrap_or("")
        .split(':')
        .nth(1)
        .and_then(|port| port.trim().parse::<u16>().ok());
    let http_port = connector_doc
        .get("http_port")
        .or_else(|| connector_doc.get("port"))
        .and_then(|value| value.as_u64())
        .map(|port| port as u16)
        .or(server_port)
        .unwrap_or(8030);
    // Default `8030`: the Doris front-end default HTTP port.
    let database = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["database"],
        "database",
    )?;
    let username = first_string(connector_doc, &["username", "user"])
        .map(|value| value.into_owned())
        .unwrap_or_else(|| "root".to_string());
    // A `file:` or `env:` password is resolved here; its text is never sent.
    let password = connector_password(&record.id, &connector, connector_doc)?;
    let table = merged_field(
        record,
        connector_doc,
        "table",
        &["table", "table_template"],
        "table",
    )?;
    if let Some(format) = param_string(record, "format") {
        if format.to_lowercase() != "json" {
            return Err(format!(
                "action {:?} field `parameters.format` must be `json`: the driver writes JSON stream loads only",
                record.id
            ));
        }
    }
    let config = DorisSinkConfig {
        fe_host,
        http_port,
        database,
        table_template: table,
        auth: DorisAuth { username, password },
        format: DorisFormat::Json,
        jsonpaths: None,
        strip_outer_array: true,
        max_filter_ratio: None,
        batch_size: Some(1),
        batch_bytes: Some(4_194_304),
        linger_ms: Some(10),
        max_retries: Some(3),
        initial_backoff_ms: Some(100),
        max_backoff_ms: Some(2_000),
        timeout_ms: connector_timeout_ms(connector_doc),
    };
    // No-redirect client: the transport replays auth and the label
    // itself on the FE-to-BE hop (a stock client strips the
    // authorization header when the back-end is another origin).
    let client = reqwest::Client::builder()
        .timeout(config.timeout())
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap_or_default();
    let transport = HttpDorisTransport::new(&config, client).map_err(|error| {
        format!(
            "action {:?} cannot build its doris transport: {error}",
            record.id
        )
    })?;
    let sink = DorisSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its doris sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

/// The time field, tag columns and field columns of a datalayers connector.
type DatalayersColumns = (Option<String>, Vec<String>, Vec<String>);

/// Read the datalayers column selection settings of a connector
/// document: the time field, the tag columns and the field columns.
/// A value of the wrong type is refused with the field named.
pub(crate) fn datalayers_column_settings(
    doc: &serde_json::Value,
    connector: &str,
) -> Result<DatalayersColumns, String> {
    let timestamp_field = match doc.get("timestamp_field") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(text)) => {
            Some(text.trim().to_string()).filter(|text| !text.is_empty())
        }
        Some(_) => {
            return Err(format!(
                "connector {connector:?} field `timestamp_field` must be a string"
            ))
        }
    };
    let columns = |key: &str| -> Result<Vec<String>, String> {
        match doc.get(key) {
            None | Some(serde_json::Value::Null) => Ok(Vec::new()),
            Some(serde_json::Value::Array(items)) => items
                .iter()
                .map(|item| {
                    item.as_str()
                        .map(str::to_string)
                        .filter(|text| !text.trim().is_empty())
                        .ok_or_else(|| {
                            format!(
                                "connector {connector:?} field `{key}` must hold non-empty strings"
                            )
                        })
                })
                .collect(),
            Some(_) => Err(format!(
                "connector {connector:?} field `{key}` must be a list of strings"
            )),
        }
    };
    Ok((
        timestamp_field,
        columns("tag_columns")?,
        columns("field_columns")?,
    ))
}

/// Check a datalayers write precision: `ns`, `us`, `ms` or `s`.
pub(crate) fn datalayers_precision(
    value: Option<String>,
    field: &str,
) -> Result<Option<String>, String> {
    match value.as_deref() {
        None => Ok(None),
        Some("ns" | "us" | "ms" | "s") => Ok(value),
        Some(other) => Err(format!("{field} must be one of ns, us, ms, s: {other:?}")),
    }
}

/// Build the action's `datalayers` sink: the server, database and
/// credentials come from the connector; the action may override the
/// table.
///
/// Values and defaults:
/// - `server`: the connector's `server` (aliases `url`, `endpoint`);
///   default `127.0.0.1:8361`, the reference connector default.
/// - `database`: the connector's `database`, required.
/// - `auth_token` or `username`/`password`: the connector's
///   credential, at most one mode (both modes, or a half Basic pair,
///   are refused by the sink).
/// - `table`: the connector's table, required; the action parameter
///   `table` overrides it per target.
/// - Stamps are always microseconds (the wire requirement); tag
///   columns default to the `topic` tag and every payload field
///   becomes a measured field.
/// - `precision`: one of `ns`, `us`, `ms`, `s`; default `us`, the sink
///   wire default. The action parameter wins; else the connector value
///   applies.
/// - `write_syntax` is a line-protocol template that this sink does not
///   render, so the action refuses it.
/// - `timestamp_field`, `tag_columns` and `field_columns` come from the
///   connector and select the time and the columns.
/// - Batching belongs to the action runtime, not this driver. The
///   inner sink keeps one row per send, so the runtime owns the
///   batch size.
pub(crate) fn build_datalayers_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::datalayers::{
        DatalayersConfig, DatalayersSink, HttpDatalayersTransport,
    };
    let connector = record.connector.trim().to_string();
    let raw_server =
        first_string(connector_doc, &["server", "url", "endpoint"]).map(|value| value.into_owned());
    let raw_server = raw_server.as_deref();
    let database = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["database"],
        "database",
    )?;
    let auth_token = first_string(connector_doc, &["auth_token", "token"])
        .map(|value| value.into_owned())
        .filter(|value| !value.trim().is_empty());
    let username = first_string(connector_doc, &["username", "user"])
        .map(|value| value.into_owned())
        .filter(|value| !value.trim().is_empty());
    let password = first_string(connector_doc, &["password"]).map(|value| value.into_owned());
    let table = merged_field(
        record,
        connector_doc,
        "table",
        &["table", "measurement"],
        "table",
    )?;
    if param_value(record, "write_syntax").is_some() {
        return Err(format!(
            "action {:?} field `parameters.write_syntax` is not supported; remove it",
            record.id
        ));
    }
    let precision = datalayers_precision(
        param_string(record, "precision").or_else(|| {
            first_string(connector_doc, &["precision"]).map(|value| value.into_owned())
        }),
        &format!("action {:?} field `parameters.precision`", record.id),
    )?;
    let (timestamp_field, tag_columns, field_columns) =
        datalayers_column_settings(connector_doc, &connector)?;
    let config = DatalayersConfig {
        endpoint: datalayers_endpoint(raw_server),
        database,
        table,
        auth_token,
        username,
        password,
        timestamp_field,
        tag_columns,
        field_columns,
        precision,
        batch_size: Some(1),
        buffer_capacity: None,
        timeout_ms: connector_timeout_ms(connector_doc),
    };
    let transport = Arc::new(HttpDatalayersTransport::new(&config));
    let sink = DatalayersSink::new(config, transport).map_err(|error| {
        format!(
            "action {:?} cannot build its datalayers sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

/// Build the action's `timestream` sink: the database, region and
/// credentials come from the connector; the action may override the
/// table and measures.
///
/// Values and defaults:
/// - `database`: the connector's `database` (alias `database_name`),
///   required.
/// - `region`: the connector's `region`, required.
/// - `access_key_id`/`secret_access_key`: the connector's pair, both
///   required (every write signs with them). A session token travels
///   in the connector's `session_token` when present.
/// - `table`: the connector's table, required; the action parameter
///   `table` overrides it per target.
/// - `measures`: the payload-field to measure type map (`DOUBLE`,
///   `BIGINT`, `BOOLEAN`, `VARCHAR`, `TIMESTAMP`), required in the
///   merged view.
/// - `measure`: this action's measure name template; default
///   `metrics`, the sink default. The action parameter wins; else
///   the connector value applies.
/// - `precision`: one of `ms`, `us`, `ns`; default `ms`, the
///   reference action default. Seconds have no sink resolution and
///   are refused. The action parameter wins; else the connector
///   value applies.
/// - `parameters.dimensions`: this action's extra dimension templates;
///   default empty, and every record still carries the `topic`
///   dimension.
/// - Batching belongs to the action runtime, not this driver. The
///   inner sink keeps one row per send with a short linger, so the
///   runtime owns the batch size and the batch timer.
pub(crate) fn build_timestream_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::timestream::{
        HttpTimestreamTransport, TimestreamSink, TimestreamSinkConfig, TimestreamTimeUnit,
    };
    let connector = record.connector.trim().to_string();
    let database = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["database", "database_name", "db"],
        "database",
    )?;
    let region = connector_field(connector_doc, &connector, &record.id, &["region"], "region")?;
    let access_key_id = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["access_key_id", "access_key"],
        "access_key_id",
    )?;
    let secret_access_key = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["secret_access_key", "secret_key"],
        "secret_access_key",
    )?;
    let session_token = first_string(connector_doc, &["session_token", "token"])
        .map(|value| value.into_owned())
        .filter(|value| !value.is_empty());
    let endpoint = first_string(connector_doc, &["endpoint", "url"])
        .map(|value| value.into_owned())
        .filter(|value| !value.trim().is_empty())
        .map(|value| http_endpoint(&value));
    let table = merged_field(
        record,
        connector_doc,
        "table",
        &["table", "table_name"],
        "table",
    )?;
    let measures = merged_map(
        record,
        connector_doc,
        "measures",
        &["measures", "multi_measure_mappings"],
    );
    if measures.is_empty() {
        return Err(format!(
            "action {:?} is missing required field: measures (fields `parameters.measures`, connector `measures`)",
            record.id
        ));
    }
    let measure = param_string(record, "measure").or_else(|| {
        first_string(connector_doc, &["measure", "measure_name_template"])
            .map(|value| value.into_owned())
    });
    let time_unit = match param_string(record, "precision")
        .or_else(|| first_string(connector_doc, &["precision"]).map(|value| value.into_owned()))
        .unwrap_or_else(|| "ms".to_string())
        .as_str()
    {
        "ms" => TimestreamTimeUnit::Milliseconds,
        "us" => TimestreamTimeUnit::Microseconds,
        "ns" => TimestreamTimeUnit::Nanoseconds,
        other => {
            return Err(format!(
                "action {:?} field `parameters.precision` must be one of ms, us, ns: the driver has no second resolution ({other:?})",
                record.id
            ));
        }
    };
    let dimensions = merged_map(record, connector_doc, "dimensions", &["dimensions"]);
    let config = TimestreamSinkConfig {
        database_name: database,
        table_name: table,
        region,
        endpoint,
        access_key_id,
        secret_access_key,
        session_token,
        dimensions,
        time_unit,
        measure_name_template: measure,
        multi_measure_mappings: measures,
        batch_size: Some(1),
        batch_bytes: Some(1_048_576),
        linger_ms: Some(10),
        max_retries: Some(4),
        initial_backoff_ms: Some(100),
        max_backoff_ms: Some(2_500),
        timeout_ms: connector_timeout_ms(connector_doc),
    };
    // The connector `timeout_ms` bounds every request of the client.
    let client = reqwest::Client::builder()
        .timeout(config.timeout())
        .build()
        .unwrap_or_default();
    let transport = HttpTimestreamTransport::new(&config, client).map_err(|error| {
        format!(
            "action {:?} cannot build its timestream transport: {error}",
            record.id
        )
    })?;
    let sink = TimestreamSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its timestream sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

/// Build the action's `cassandra` sink: the seeds, keyspace and
/// table come from the connector; the action may override the table
/// and partition key.
///
/// Values and defaults:
/// - `servers`: the connector's `servers` (aliases `server`,
///   `contact_points`), required; entries are `host` or
///   `host:port` (default port 9042).
/// - `keyspace`: the connector's `keyspace`, required.
/// - `username`/`password`: the connector's pair; both or neither (a
///   half pair is refused fail closed, it cannot authenticate).
/// - `table`: the connector's table, required; the action parameter
///   `table` overrides it per target. The value must match
///   `[A-Za-z0-9_]+`: the driver binds one fixed CQL shape per action.
/// - `partition_key`: the token routing template; default
///   `${client_id}`, the client identity routes each row to its token
///   owner. The action parameter `partition_key` overrides it.
/// - Each row binds `(device id, bucket hour, event time, payload)`
///   with `LOCAL_QUORUM`, the sink default consistency.
/// - Batching belongs to the action runtime, not this driver. The
///   inner sink keeps one row per send with a short linger, so the
///   runtime owns the batch size and the batch timer.
#[cfg(feature = "cassandra")]
pub(crate) fn build_cassandra_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::cassandra::{
        CassandraAuth, CassandraSink, CassandraSinkConfig, CqlConsistency, ScyllaCassandraTransport,
    };
    let connector = record.connector.trim().to_string();
    let raw_servers = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["servers", "server", "contact_points"],
        "servers",
    )?;
    let contact_points: Vec<String> = raw_servers
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(str::to_string)
        .collect();
    if contact_points.is_empty() {
        return Err(format!(
            "connector {connector:?} is missing required field: servers (action {:?})",
            record.id
        ));
    }
    let keyspace = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["keyspace"],
        "keyspace",
    )?;
    let username =
        first_string(connector_doc, &["username", "user"]).map(|value| value.into_owned());
    let password = first_string(connector_doc, &["password"]).map(|value| value.into_owned());
    let auth = match (username, password) {
        (Some(username), Some(password)) => CassandraAuth::Password { username, password },
        (None, None) => CassandraAuth::None,
        _ => {
            return Err(format!(
                "connector {connector:?} fields `username` and `password` must be set together (action {:?})",
                record.id
            ));
        }
    };
    let table = merged_field(
        record,
        connector_doc,
        "table",
        &["table", "table_template"],
        "table",
    )?;
    if !is_cassandra_table(&table) {
        return Err(format!(
            "action {:?} field `parameters.table` must match [A-Za-z0-9_]+: {table:?}",
            record.id
        ));
    }
    let partition_key = param_string(record, "partition_key")
        .or_else(|| {
            first_string(connector_doc, &["partition_key", "partition_key_template"])
                .map(|value| value.into_owned())
        })
        .unwrap_or_else(|| "${client_id}".to_string());
    // Default `${client_id}`: the client identity routes each row to
    // its token owner.
    // The CQL text carries `keyspace.table`: the Scylla
    // transport prepares the statement text as given, so a bare
    // table never resolves. Both parts are validated identifiers.
    let cql = format!(
        "INSERT INTO {keyspace}.{table} (device_id, bucket_hour, event_time, payload) VALUES (?, ?, ?, ?)"
    );
    let config = CassandraSinkConfig {
        contact_points,
        keyspace,
        table_template: table,
        auth,
        consistency: CqlConsistency::LocalQuorum,
        partition_key_template: partition_key,
        cql_statement_template: cql,
        ttl_secs: None,
        batch_size: Some(1),
        batch_bytes: Some(1_048_576),
        linger_ms: Some(10),
        max_retries: Some(4),
        initial_backoff_ms: Some(100),
        max_backoff_ms: Some(2_000),
        timeout_ms: connector_timeout_ms(connector_doc),
    };
    let transport = ScyllaCassandraTransport::new(&config).map_err(|error| {
        format!(
            "action {:?} cannot build its cassandra transport: {error}",
            record.id
        )
    })?;
    let sink = CassandraSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its cassandra sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

/// True when the table value fits the fixed CQL shape (the driver
/// binds one statement text per action).
#[cfg(feature = "cassandra")]
fn is_cassandra_table(table: &str) -> bool {
    !table.is_empty()
        && table
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// The default build compiles the Cassandra driver out: the kind is
/// known but the runtime is absent, so the build fails closed naming
/// the missing feature.
#[cfg(not(feature = "cassandra"))]
pub(crate) fn build_cassandra_sink(
    record: &ActionRecord,
    _connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    Err(format!(
        "action {:?} needs cargo feature `cassandra` (disabled in this build)",
        record.id
    ))
}

/// Build the action's `mongodb` sink: the connection and database come
/// from the connector; the action owns the collection and optional
/// document template.
///
/// Values and defaults:
/// - `server` (alias `servers` for a replica set or sharded cluster)
///   and `database`: connector fields, required.
/// - `username`/`password`: connector fields; a half pair refuses.
/// - `collection`: the action parameter, else the connector field;
///   default `mqtt`, the reference action default.
/// - `payload_template`: renders one JSON document. A result that is
///   not a JSON object is a failed forward. Without it the sink writes
///   the event envelope.
/// - Batching belongs to the action runtime; the inner sink keeps one
///   row per send with a short linger.
#[cfg(feature = "mongodb")]
pub(crate) fn mongodb_sink_config(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<broker_connectors_enterprise::MongoDbSinkConfig, String> {
    use broker_connectors_enterprise::{MongoDbSinkConfig, MongoOperation};
    let connector = record.connector.trim().to_string();
    validate_connector_io_bounds(
        connector_doc,
        &connector,
        &format!("action {:?}", record.id),
    )?;
    let mongo_type = first_string_nested(connector_doc, &["mongo_type"])
        .map(|value| value.into_owned())
        .unwrap_or_else(|| "single".to_string());
    let raw_server = if matches!(mongo_type.as_str(), "rs" | "sharded") {
        document_connector_field(
            connector_doc,
            &connector,
            &record.id,
            &["servers", "server", "connection_string"],
            "servers",
        )?
    } else {
        document_connector_field(
            connector_doc,
            &connector,
            &record.id,
            &["server", "servers", "connection_string"],
            "server",
        )?
    };
    let database = document_connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["database"],
        "database",
    )?;
    let username =
        first_string(connector_doc, &["username", "user"]).map(|value| value.into_owned());
    let password = first_string(connector_doc, &["password"]).map(|value| value.into_owned());
    let credentials = match (username, password) {
        (Some(username), Some(password)) => Some((username, password)),
        (None, None) => None,
        _ => {
            return Err(format!(
                "connector {connector:?} fields `username` and `password` must be set together (action {:?})",
                record.id
            ));
        }
    };
    let auth_source =
        first_string_nested(connector_doc, &["auth_source"]).map(|value| value.into_owned());
    let mut connection_string =
        if raw_server.starts_with("mongodb://") || raw_server.starts_with("mongodb+srv://") {
            raw_server
        } else {
            format!("mongodb://{raw_server}")
        };
    if let Some((username, password)) = &credentials {
        let scheme = if connection_string.starts_with("mongodb+srv://") {
            "mongodb+srv://"
        } else {
            "mongodb://"
        };
        if !connection_string[scheme.len()..].contains('@') {
            connection_string = format!(
                "{scheme}{username}:{password}@{}",
                &connection_string[scheme.len()..]
            );
        }
    }
    if let Some(auth_source) = &auth_source {
        let separator = if connection_string.contains('?') {
            '&'
        } else {
            '?'
        };
        connection_string.push(separator);
        connection_string.push_str("authSource=");
        connection_string.push_str(auth_source);
    }
    let collection = param_string(record, "collection")
        .or_else(|| {
            first_string_nested(connector_doc, &["collection", "collection_template"])
                .map(|value| value.into_owned())
        })
        .unwrap_or_else(|| "mqtt".to_string());
    let payload_template = param_string(record, "payload_template").or_else(|| {
        first_string_nested(connector_doc, &["payload_template"]).map(|v| v.into_owned())
    });
    let config = MongoDbSinkConfig {
        connection_string,
        database,
        collection_template: collection,
        payload_template,
        operation: MongoOperation::InsertOne,
        // One row per send: the action runtime owns batching (RO-01).
        // Source: MV-02c action runtime contract, single-row flush.
        batch_size: Some(1),
        // 4 MiB batch cap: the driver default batch byte limit.
        // Source: `MongoDbSinkConfig::default_batch_bytes`.
        batch_bytes: Some(4_194_304),
        // Short linger: the runtime groups rows before send.
        // Source: MV-02c action runtime contract, 10 ms flush window.
        linger_ms: Some(10),
        // 4 retries: the driver default retry count.
        // Source: `MongoDbSinkConfig::default_max_retries`.
        max_retries: Some(4),
        // 100 ms first retry delay: the driver default backoff.
        // Source: `MongoDbSinkConfig::default_initial_backoff_ms`.
        initial_backoff_ms: Some(100),
        // 3 s retry ceiling: the driver default backoff cap.
        // Source: `MongoDbSinkConfig::default_max_backoff_ms`.
        max_backoff_ms: Some(3_000),
        timeout_ms: connector_timeout_ms(connector_doc),
    };
    Ok(config)
}

/// Build the action's `mongodb` sink from its config. The config
/// carries the applied defaults; the transport and sink add no new
/// values.
#[cfg(feature = "mongodb")]
pub(crate) fn build_mongodb_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::{DriverMongoDbTransport, MongoDbSink};
    let config = mongodb_sink_config(record, connector_doc)?;
    let transport = DriverMongoDbTransport::new(&config).map_err(|error| {
        format!(
            "action {:?} cannot build its mongodb transport: {error}",
            record.id
        )
    })?;
    let sink = MongoDbSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its mongodb sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}
/// Build the action's `s3` object sink. The endpoint, region and
/// credentials come from the connector; the bucket, key and content
/// come from the action. A key is required: the driver never invents a
/// bucket or object name.
///
/// Values and defaults:
/// - `endpoint`: connector `endpoint`/`url`/`server`, or `https://host:port`.
/// - `region`: connector `region`; default `us-east-1`, the AWS default.
/// - `bucket`, `key`: action `parameters.bucket` and `parameters.key`,
///   both required. The key is the documented template.
/// - `content`: only the documented default `${.}` is supported. The
///   driver writes the projected event body (see `refuse_object_upload_overrides`).
/// - Batching belongs to the action runtime (RO-01). The driver keeps
///   one row per send with no linger.
#[cfg(feature = "s3")]
pub(crate) fn build_s3_object_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
    _opts: &ActionResourceOpts,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors::s3::{S3Compression, S3Sink, S3SinkConfig, SdkS3Transport};
    let connector = record.connector.trim().to_string();
    let endpoint = object_endpoint(
        connector_doc,
        &connector,
        &record.id,
        &["endpoint", "url", "server", "host"],
    )?;
    let bucket = param_string(record, "bucket")
        .ok_or_else(|| format!("action {:?} is missing required field: bucket", record.id))?;
    let key = param_string(record, "key")
        .ok_or_else(|| format!("action {:?} is missing required field: key", record.id))?;
    let compression = match first_string(connector_doc, &["compression"]).as_deref() {
        None | Some("none") => S3Compression::None,
        Some("gzip") => S3Compression::Gzip,
        Some(other) => {
            return Err(format!(
                "connector {connector:?} field `compression` must be none or gzip: {other:?} (action {:?})",
                record.id
            ));
        }
    };
    // The bucket and key belong to the action. The driver sends no session
    // token. These connector fields would be ignored, so they are refused.
    refuse_connector_fields(
        connector_doc,
        &connector,
        &record.id,
        &["bucket", "key_template", "session_token"],
    )?;
    let config = S3SinkConfig {
        endpoint,
        bucket,
        region: first_string(connector_doc, &["region"])
            .map(|value| value.into_owned())
            .unwrap_or_else(|| "us-east-1".to_string()),
        access_key_id: first_string(connector_doc, &["access_key_id", "access_key"])
            .map(|value| value.into_owned())
            .unwrap_or_default(),
        secret_access_key: first_string(connector_doc, &["secret_access_key", "secret_key"])
            .map(|value| value.into_owned())
            .unwrap_or_default(),
        key_template: key,
        compression,
        // One row per send with no linger: the action runtime owns
        // batching (RO-01). Cap the buffer at 5 MiB (under the 5 GiB
        // single-PUT limit).
        batch_size: 1,
        batch_bytes: 5 * 1024 * 1024,
        batch_timeout_ms: 0,
        timeout_ms: connector_timeout_ms(connector_doc),
    };
    let transport = SdkS3Transport::new(&config).map_err(|error| {
        format!(
            "action {:?} cannot build its s3 transport: {error}",
            record.id
        )
    })?;
    let sink = S3Sink::new(config, Arc::new(transport))
        .map_err(|error| format!("action {:?} cannot build its s3 sink: {error}", record.id))?;
    Ok(Arc::new(sink))
}

#[cfg(not(feature = "s3"))]
pub(crate) fn build_s3_object_sink(
    record: &ActionRecord,
    _connector_doc: &serde_json::Value,
    _opts: &ActionResourceOpts,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    Err(format!(
        "action {:?} needs cargo feature `s3` (disabled in this build)",
        record.id
    ))
}

/// Refuse connector fields that an action sink does not apply. The error
/// names the field, so no setting is accepted and then ignored.
#[cfg(any(
    feature = "azure_blob",
    feature = "s3",
    feature = "dynamodb",
    feature = "kinesis",
    feature = "bigquery"
))]
fn refuse_connector_fields(
    connector_doc: &serde_json::Value,
    connector: &str,
    action_id: &str,
    fields: &[&str],
) -> Result<(), String> {
    for field in fields {
        let present = connector_doc
            .get(*field)
            .or_else(|| connector_doc.get("parameters").and_then(|p| p.get(*field)));
        if present.is_some_and(|value| !value.is_null()) {
            return Err(format!(
                "connector {connector:?} field `{field}` is not supported by its actions; remove it (action {action_id:?})"
            ));
        }
    }
    Ok(())
}

/// The azure blob credential of a connector: the account key, a SAS token
/// or a bearer token, at the top level or inside the `auth` object.
#[cfg(feature = "azure_blob")]
fn azure_blob_auth(
    connector_doc: &serde_json::Value,
    connector: &str,
    action_id: &str,
) -> Result<broker_connectors_enterprise::azure_blob::AzureBlobAuth, String> {
    use broker_connectors_enterprise::azure_blob::AzureBlobAuth;
    let nested = connector_doc
        .get("auth")
        .and_then(|value| value.as_object());
    let pick = |keys: &[&str]| -> Option<String> {
        keys.iter().find_map(|key| {
            connector_doc
                .get(*key)
                .or_else(|| nested.and_then(|object| object.get(*key)))
                .and_then(|value| value.as_str())
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .map(str::to_string)
        })
    };
    if let Some(account_key) = pick(&["account_key", "accountKey"]) {
        return Ok(AzureBlobAuth::SharedKey { account_key });
    }
    if let Some(sas_token) = pick(&["sas_token", "sas"]) {
        return Ok(AzureBlobAuth::SasToken { sas_token });
    }
    if let Some(token) = pick(&["token", "bearer_token"]) {
        return Ok(AzureBlobAuth::BearerToken { token });
    }
    Err(format!(
        "connector {connector:?} is missing required field: account_key (action {action_id:?})"
    ))
}

/// Build the action's `azure_blob` sink. The account, key and optional
/// endpoint come from the connector; the container and blob come from
/// the action. The driver never invents a container or blob name.
///
/// Batching belongs to the action runtime (RO-01). The driver keeps
/// one blob per send.
#[cfg(feature = "azure_blob")]
pub(crate) fn build_azure_blob_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
    _opts: &ActionResourceOpts,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::azure_blob::{
        AzureBlobCompression, AzureBlobSink, AzureBlobSinkConfig, HttpAzureBlobTransport,
    };
    let connector = record.connector.trim().to_string();
    let account_name = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["account_name", "account"],
        "account_name",
    )?;
    let auth = azure_blob_auth(connector_doc, &connector, &record.id)?;
    let compression = match first_string(connector_doc, &["compression"]).as_deref() {
        None | Some("none") => AzureBlobCompression::None,
        Some("gzip") => AzureBlobCompression::Gzip,
        Some(other) => {
            return Err(format!(
                "connector {connector:?} field `compression` must be none or gzip: {other:?} (action {:?})",
                record.id
            ));
        }
    };
    // The action runtime owns batching (`resource_opts`). These connector
    // settings have no effect on an action, so they are refused.
    refuse_connector_fields(
        connector_doc,
        &connector,
        &record.id,
        &[
            "max_records_per_blob",
            "max_bytes_per_blob",
            "flush_interval_secs",
            "buffer_capacity",
        ],
    )?;
    let container = param_string(record, "container").ok_or_else(|| {
        format!(
            "action {:?} is missing required field: container",
            record.id
        )
    })?;
    let blob = param_string(record, "blob")
        .ok_or_else(|| format!("action {:?} is missing required field: blob", record.id))?;
    let endpoint = first_string(connector_doc, &["endpoint", "url"]).map(|value| {
        let raw = value.into_owned();
        if raw.starts_with("http://") || raw.starts_with("https://") {
            raw
        } else {
            format!("http://{raw}")
        }
    });
    let config = AzureBlobSinkConfig {
        account_name,
        container_name: container,
        endpoint,
        auth,
        blob_path_template: blob,
        compression,
        // One blob per send: the action runtime owns batching (RO-01).
        // Cap the blob at 10 MiB (under the block-blob staging limit
        // for small rows). Keep one batch buffered to bound memory.
        max_records_per_blob: Some(1),
        max_bytes_per_blob: Some(10_485_760),
        flush_interval_secs: 0,
        buffer_capacity: Some(1),
        timeout_ms: connector_timeout_ms(connector_doc),
    };
    let client = reqwest::Client::builder()
        .timeout(config.timeout())
        .build()
        .unwrap_or_default();
    let transport = HttpAzureBlobTransport::new(&config, client).map_err(|error| {
        format!(
            "action {:?} cannot build its azure_blob transport: {error}",
            record.id
        )
    })?;
    let sink = AzureBlobSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its azure_blob sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

#[cfg(not(feature = "mongodb"))]
pub(crate) fn build_mongodb_sink(
    record: &ActionRecord,
    _connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    Err(format!(
        "action {:?} needs cargo feature `mongodb` (disabled in this build)",
        record.id
    ))
}

#[cfg(not(feature = "azure_blob"))]
pub(crate) fn build_azure_blob_sink(
    record: &ActionRecord,
    _connector_doc: &serde_json::Value,
    _opts: &ActionResourceOpts,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    Err(format!(
        "action {:?} needs cargo feature `azure_blob` (disabled in this build)",
        record.id
    ))
}

/// Build the action's `couchbase` sink: the connector supplies the
/// server and credentials; the action supplies the SQL++ statement.
///
/// Values and defaults:
/// - `server`: the connector's `server`, required.
/// - `username`: the connector's `username`, required.
/// - `password`: the connector's `password`; default empty.
/// - `sql`: the action's SQL++ template, required. The statement names
///   the keyspace, key and value, so no fixed bucket or collection is
///   invented.
/// - `max_retries`: the action parameter; default `3`, the reference
///   action default.
/// - Batching belongs to the action runtime; the query driver keeps one
///   statement per send.
#[cfg(feature = "couchbase")]
pub(crate) fn couchbase_sink_config(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<broker_connectors_enterprise::CouchbaseSinkConfig, String> {
    use broker_connectors_enterprise::{CouchbaseAuth, CouchbaseOperation, CouchbaseSinkConfig};
    let connector = record.connector.trim().to_string();
    validate_connector_io_bounds(
        connector_doc,
        &connector,
        &format!("action {:?}", record.id),
    )?;
    let raw_server = document_connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["server", "servers", "connection_string"],
        "server",
    )?;
    let connection_string =
        if raw_server.starts_with("couchbase://") || raw_server.starts_with("couchbases://") {
            raw_server
        } else {
            format!("couchbase://{raw_server}")
        };
    let nested_auth = connector_doc.get("auth");
    let username = first_string_nested(connector_doc, &["username", "user"])
        .map(|value| value.into_owned())
        .or_else(|| {
            nested_auth
                .and_then(|auth| auth.get("username"))
                .and_then(|value| value.as_str())
                .map(str::to_string)
        })
        .ok_or_else(|| {
            format!(
                "connector {connector:?} is missing required field: username (action {:?})",
                record.id
            )
        })?;
    let password = first_string_nested(connector_doc, &["password"])
        .map(|value| value.into_owned())
        .or_else(|| {
            nested_auth
                .and_then(|auth| auth.get("password"))
                .and_then(|value| value.as_str())
                .map(str::to_string)
        })
        .unwrap_or_default();
    let sql = merged_field(
        record,
        connector_doc,
        "sql",
        &["sql", "sql_template"],
        "sql",
    )?;
    let max_retries = param_value(record, "max_retries")
        .and_then(|value| value.as_u64())
        .unwrap_or(3);
    let config = CouchbaseSinkConfig {
        connection_string,
        bucket: String::new(),
        scope: None,
        collection: None,
        auth: CouchbaseAuth { username, password },
        doc_id_template: String::new(),
        sql_template: Some(sql),
        operation: CouchbaseOperation::Upsert,
        expiry_secs: None,
        // One row per send: the action runtime owns batching (RO-01).
        // Source: MV-02c action runtime contract, single-row flush.
        batch_size: Some(1),
        // 2 MiB batch cap: the driver default batch byte limit.
        // Source: `CouchbaseSinkConfig::default_batch_bytes`.
        batch_bytes: Some(2_097_152),
        // Short linger: the runtime groups rows before send.
        // Source: MV-02c action runtime contract, 10 ms flush window.
        linger_ms: Some(10),
        // Reference `max_retries` default is 3.
        // Source: reference `action_couchbase.parameters`.
        max_retries: Some(max_retries as usize),
        // 100 ms first retry delay: the driver default backoff.
        // Source: `CouchbaseSinkConfig::default_initial_backoff_ms`.
        initial_backoff_ms: Some(100),
        // 2 s retry ceiling: the driver default backoff cap.
        // Source: `CouchbaseSinkConfig::default_max_backoff_ms`.
        max_backoff_ms: Some(2_000),
        timeout_ms: connector_timeout_ms(connector_doc),
    };
    Ok(config)
}

/// Build the action's `couchbase` sink from its config. The config
/// carries the applied defaults; the transport and sink add no new
/// values.
#[cfg(feature = "couchbase")]
pub(crate) fn build_couchbase_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::{CouchbaseSink, DriverCouchbaseTransport};
    let config = couchbase_sink_config(record, connector_doc)?;
    let transport = DriverCouchbaseTransport::new(&config).map_err(|error| {
        format!(
            "action {:?} cannot build its couchbase transport: {error}",
            record.id
        )
    })?;
    let sink = CouchbaseSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its couchbase sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}
/// Build the action's `dynamodb` sink. The endpoint, region and
/// credentials come from the connector; the table and keys come from
/// the action. The `template` object becomes the attribute mapping.
///
/// Batching belongs to the action runtime (RO-01). The driver keeps
/// one item per send with no linger.
#[cfg(feature = "dynamodb")]
pub(crate) fn build_dynamodb_action_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
    _opts: &ActionResourceOpts,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::dynamodb::{
        DynamoDbSink, DynamoDbSinkConfig, DynamoKeyConfig, SdkDynamoDbTransport,
    };
    let connector = record.connector.trim().to_string();
    let endpoint = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["url", "endpoint", "server"],
        "url",
    )?;
    let region = connector_field(connector_doc, &connector, &record.id, &["region"], "region")?;
    // The table, the keys and the attribute mapping belong to the action.
    refuse_connector_fields(
        connector_doc,
        &connector,
        &record.id,
        &[
            "table_name",
            "partition_key",
            "sort_key",
            "ttl_attribute",
            "ttl_secs",
            "attributes_mapping",
        ],
    )?;
    let table = param_string(record, "table")
        .ok_or_else(|| format!("action {:?} is missing required field: table", record.id))?;
    let hash_key = param_string(record, "hash_key")
        .ok_or_else(|| format!("action {:?} is missing required field: hash_key", record.id))?;
    let range_key = param_string(record, "range_key");
    let mut attributes = std::collections::HashMap::new();
    let mut hash_template = format!("${{payload.{hash_key}}}");
    let mut range_template = None;
    if let Some(template) = param_string(record, "template") {
        if !template.trim().is_empty() {
            let value: serde_json::Value = serde_json::from_str(&template).map_err(|error| {
                format!(
                    "action {:?} field `parameters.template` must be valid JSON: {error}",
                    record.id
                )
            })?;
            let object = value.as_object().ok_or_else(|| {
                format!(
                    "action {:?} field `parameters.template` must be a JSON object",
                    record.id
                )
            })?;
            for (name, entry) in object {
                let text = entry
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| entry.to_string());
                if name == &hash_key {
                    hash_template = text.clone();
                }
                if range_key.as_deref() == Some(name.as_str()) {
                    range_template = Some(text.clone());
                }
                attributes.insert(name.clone(), text);
            }
        }
    }
    // The documented `undefined_vars_as_null` flag is applied: an
    // empty render becomes a null attribute when true.
    let undefined_vars_as_null = match param_value(record, "undefined_vars_as_null") {
        None => false,
        Some(serde_json::Value::Bool(flag)) => *flag,
        Some(serde_json::Value::String(text)) => match text.trim().to_ascii_lowercase().as_str() {
            "true" => true,
            "false" => false,
            _ => {
                return Err(format!(
                "action {:?} field `parameters.undefined_vars_as_null` must be a boolean: {text:?}",
                record.id
            ))
            }
        },
        Some(other) => {
            return Err(format!(
                "action {:?} field `parameters.undefined_vars_as_null` must be a boolean: {other}",
                record.id
            ))
        }
    };
    let config = DynamoDbSinkConfig {
        table_name: table,
        region,
        endpoint: Some(http_endpoint(&endpoint)),
        access_key_id: connector_field(
            connector_doc,
            &connector,
            &record.id,
            &["aws_access_key_id", "access_key_id", "access_key"],
            "aws_access_key_id",
        )?,
        secret_access_key: connector_field(
            connector_doc,
            &connector,
            &record.id,
            &["aws_secret_access_key", "secret_access_key", "secret_key"],
            "aws_secret_access_key",
        )?,
        session_token: first_string(connector_doc, &["session_token"])
            .map(|value| value.into_owned()),
        partition_key: DynamoKeyConfig {
            name: hash_key,
            template: hash_template,
            key_type: "S".to_string(),
        },
        sort_key: range_key.map(|name| DynamoKeyConfig {
            name,
            template: range_template.unwrap_or_else(|| "${timestamp}".to_string()),
            key_type: "S".to_string(),
        }),
        ttl_attribute: None,
        ttl_secs: None,
        attributes_mapping: attributes,
        undefined_vars_as_null,
        // One item per send with no linger: the action runtime owns
        // batching (RO-01). Cap the batch at 1 MiB (under the 16 MiB
        // API limit). Retry 3 times with backoff.
        batch_size: Some(1),
        batch_bytes: Some(1_048_576),
        linger_ms: Some(0),
        max_retries: Some(3),
        initial_backoff_ms: Some(100),
        max_backoff_ms: Some(2_000),
        timeout_ms: connector_timeout_ms(connector_doc),
    };
    let transport = SdkDynamoDbTransport::new(&config).map_err(|error| {
        format!(
            "action {:?} cannot build its dynamodb transport: {error}",
            record.id
        )
    })?;
    let sink = DynamoDbSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its dynamodb sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

#[cfg(not(feature = "couchbase"))]
pub(crate) fn build_couchbase_sink(
    record: &ActionRecord,
    _connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    Err(format!(
        "action {:?} needs cargo feature `couchbase` (disabled in this build)",
        record.id
    ))
}

#[cfg(not(feature = "dynamodb"))]
pub(crate) fn build_dynamodb_action_sink(
    record: &ActionRecord,
    _connector_doc: &serde_json::Value,
    _opts: &ActionResourceOpts,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    Err(format!(
        "action {:?} needs cargo feature `dynamodb` (disabled in this build)",
        record.id
    ))
}

/// Default delivery mode. Reason: the reference action default is non-persistent.
#[cfg(feature = "rabbitmq")]
const RABBITMQ_NON_PERSISTENT: u8 = 1;
/// Durable delivery mode. Reason: the reference action names persistent as durable.
#[cfg(feature = "rabbitmq")]
const RABBITMQ_PERSISTENT: u8 = 2;

/// Build the action's `elasticsearch` sink: the server and authentication
/// come from the connector; the action owns the operation, index, id and
/// document.
///
/// Values and defaults:
/// - `server`: the connector's `server` (aliases `endpoint`, `servers`,
///   `url`); required, no invented default.
/// - `username`/`password` or `authentication`: the connector credential;
///   absent means no authorization header.
/// - `action`: `create`, `update` or `delete`, required.
/// - `index`: the action parameter, else the connector `index`; required.
/// - `id`: the action parameter, else the connector `doc_id_template`;
///   optional for create, required for update and delete.
/// - `doc`: renders one JSON document; a result that is not a JSON object
///   is a failed forward. Without it the sink writes the event envelope.
/// - `overwrite` (default true), `doc_as_upsert` (default false),
///   `require_alias` (default false) and `routing` from the action.
/// - Batching belongs to the action runtime; the inner sink keeps one
///   row per send with a short linger.
#[cfg(feature = "elasticsearch")]
pub(crate) fn elasticsearch_sink_config(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<broker_connectors::elasticsearch::ElasticsearchSinkConfig, String> {
    use broker_connectors::elasticsearch::{
        ElasticsearchAuth, ElasticsearchOperation, ElasticsearchSinkConfig,
    };
    let connector = record.connector.trim().to_string();
    validate_connector_io_bounds(
        connector_doc,
        &connector,
        &format!("action {:?}", record.id),
    )?;
    let raw_server = document_connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["server", "endpoint", "servers", "url"],
        "server",
    )?;
    let endpoint = http_endpoint(&raw_server);
    let index = merged_field(
        record,
        connector_doc,
        "index",
        &["index", "index_template"],
        "index",
    )?;
    let doc_id_template = param_string(record, "id").or_else(|| {
        // The connector `id` is the connector name, never a document id.
        first_string_nested(connector_doc, &["doc_id_template"]).map(|v| v.into_owned())
    });
    let action = param_string(record, "action").ok_or_else(|| {
        format!(
            "action {:?} is missing required field: action (field `parameters.action`)",
            record.id
        )
    })?;
    let bool_param = |key: &str, default: bool| {
        param_value(record, key)
            .and_then(|value| value.as_bool())
            .unwrap_or(default)
    };
    // Reference `overwrite` default is true: true writes `index`
    // (overwrite), false writes `create` (fail on conflict).
    let overwrite = bool_param("overwrite", true);
    let operation = match action.as_str() {
        "create" if overwrite => ElasticsearchOperation::Index,
        "create" => ElasticsearchOperation::Create,
        "update" => ElasticsearchOperation::Update,
        "delete" => ElasticsearchOperation::Delete,
        other => {
            return Err(format!(
                "action {:?} field `parameters.action` must be one of create, update, delete: {other:?}",
                record.id
            ));
        }
    };
    if matches!(
        operation,
        ElasticsearchOperation::Update | ElasticsearchOperation::Delete
    ) && doc_id_template.is_none()
    {
        return Err(format!(
            "action {:?} is missing required field: id (field `parameters.id`)",
            record.id
        ));
    }
    let username = first_string(connector_doc, &["username", "user"])
        .map(|value| value.into_owned())
        .or_else(|| {
            connector_doc
                .get("authentication")
                .and_then(|auth| auth.get("username"))
                .and_then(|value| value.as_str())
                .map(str::to_string)
        });
    let password = first_string_nested(connector_doc, &["password"])
        .map(|value| value.into_owned())
        .or_else(|| {
            connector_doc
                .get("authentication")
                .and_then(|auth| auth.get("password"))
                .and_then(|value| value.as_str())
                .map(str::to_string)
        });
    // The reference `auth` object may carry an API key. It is applied when
    // no user and password are set; both together are refused.
    let api_key = connector_doc
        .get("auth")
        .filter(|auth| auth.get("type").and_then(|t| t.as_str()) == Some("apikey"))
        .and_then(|auth| auth.get("key"))
        .and_then(|key| key.as_str())
        .map(str::to_string);
    if api_key.is_some() && (username.is_some() || password.is_some()) {
        return Err(format!(
            "connector {connector:?} field `auth` cannot be set together with `username` and `password` (action {:?})",
            record.id
        ));
    }
    let auth = match (username, password) {
        _ if api_key.is_some() => ElasticsearchAuth::ApiKey {
            key: api_key.unwrap_or_default(),
        },
        (Some(username), Some(password)) => ElasticsearchAuth::Basic { username, password },
        (None, None) => ElasticsearchAuth::None,
        _ => {
            return Err(format!(
                "connector {connector:?} fields `username` and `password` must be set together (action {:?})",
                record.id
            ));
        }
    };
    let max_retries = param_value(record, "max_retries")
        .and_then(|value| value.as_u64())
        .unwrap_or(2);
    let config = ElasticsearchSinkConfig {
        endpoint,
        index_template: index,
        doc_id_template,
        operation,
        doc_template: param_string(record, "doc"),
        overwrite,
        doc_as_upsert: bool_param("doc_as_upsert", false),
        require_alias: bool_param("require_alias", false),
        routing: param_string(record, "routing")
            .or_else(|| first_string_nested(connector_doc, &["routing"]).map(|v| v.into_owned())),
        auth,
        // One row per send: the action runtime owns batching (RO-01).
        // Source: MV-02c action runtime contract, single-row flush.
        batch_size: 1,
        // Short linger: the runtime groups rows before send.
        // Source: MV-02c action runtime contract, 10 ms flush window.
        batch_timeout_ms: 10,
        // Reference `max_retries` default is 2.
        // Source: reference `bridge_elasticsearch.action_create`.
        max_retries: max_retries as usize,
        request_timeout_ms: connector_timeout_ms(connector_doc),
    };
    Ok(config)
}

/// Build the action's `elasticsearch` sink from its config. The config
/// carries the applied defaults; the transport and sink add no new
/// values.
#[cfg(feature = "elasticsearch")]
pub(crate) fn build_elasticsearch_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors::elasticsearch::{DriverElasticsearchTransport, ElasticsearchSink};
    let config = elasticsearch_sink_config(record, connector_doc)?;
    let transport = DriverElasticsearchTransport::new(&config).map_err(|error| {
        format!(
            "action {:?} cannot build its elasticsearch transport: {error}",
            record.id
        )
    })?;
    let sink = ElasticsearchSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its elasticsearch sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}
/// Build the action's `bigquery` sink. Authentication comes from the
/// connector; the dataset, table and optional project override come
/// from the action.
///
/// Batching belongs to the action runtime (RO-01). The driver keeps
/// one row per send with no linger.
#[cfg(feature = "bigquery")]
pub(crate) fn build_bigquery_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
    _opts: &ActionResourceOpts,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::bigquery::{
        BigQuerySink, BigQuerySinkConfig, SdkBigQueryTransport,
    };
    let connector = record.connector.trim().to_string();
    let dataset = param_string(record, "dataset")
        .ok_or_else(|| format!("action {:?} is missing required field: dataset", record.id))?;
    let table = param_string(record, "table")
        .ok_or_else(|| format!("action {:?} is missing required field: table", record.id))?;
    let (project_id, auth) = bigquery_auth(connector_doc, &connector, &record.id)?;
    let project_id = param_string(record, "project_id").unwrap_or(project_id);
    // The target and the insert flags belong to the action.
    refuse_connector_fields(
        connector_doc,
        &connector,
        &record.id,
        &[
            "dataset_id",
            "table",
            "template_suffix",
            "ignore_unknown_values",
            "skip_invalid_rows",
        ],
    )?;
    let flag = |key: &str, default: bool| -> Result<bool, String> {
        match param_value(record, key) {
            None => Ok(default),
            Some(value) => value.as_bool().ok_or_else(|| {
                format!(
                    "action {:?} field `parameters.{key}` must be a boolean",
                    record.id
                )
            }),
        }
    };
    let config = BigQuerySinkConfig {
        project_id,
        dataset_id: dataset,
        table_template: table,
        endpoint: first_string(connector_doc, &["endpoint", "url"])
            .map(|value| value.into_owned())
            .map(|value| http_endpoint(&value)),
        auth,
        ignore_unknown_values: flag("ignore_unknown_values", true)?,
        skip_invalid_rows: flag("skip_invalid_rows", false)?,
        template_suffix: param_string(record, "template_suffix"),
        // One row per send with no linger: the action runtime owns
        // batching (RO-01). Cap the batch at 1 MiB (under the 10 MiB
        // API limit). Retry 3 times with backoff.
        batch_size: Some(1),
        batch_bytes: Some(1_048_576),
        linger_ms: Some(0),
        max_retries: Some(3),
        initial_backoff_ms: Some(100),
        max_backoff_ms: Some(2_000),
        timeout_ms: connector_timeout_ms(connector_doc),
    };
    let client = reqwest::Client::builder()
        .timeout(config.timeout())
        .build()
        .unwrap_or_default();
    let transport = SdkBigQueryTransport::new(&config, client).map_err(|error| {
        format!(
            "action {:?} cannot build its bigquery transport: {error}",
            record.id
        )
    })?;
    let sink = BigQuerySink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its bigquery sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

#[cfg(feature = "bigquery")]
fn parse_service_account_key(
    connector_doc: &serde_json::Value,
    connector: &str,
    action_id: &str,
    raw: &str,
    authentication: Option<&serde_json::Value>,
) -> Result<(String, broker_connectors_enterprise::gcp_pubsub::GcpAuth), String> {
    use broker_connectors_enterprise::gcp_pubsub::GcpAuth;
    let key: serde_json::Value = serde_json::from_str(raw).map_err(|error| {
        format!(
            "connector {connector:?} field `authentication.service_account_json` is not JSON: {error}"
        )
    })?;
    let client_email = key
        .get("client_email")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            format!(
                "connector {connector:?} field `authentication.service_account_json` is missing client_email"
            )
        })?
        .to_string();
    let private_key_pem = key
        .get("private_key")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            format!(
                "connector {connector:?} field `authentication.service_account_json` is missing private_key"
            )
        })?
        .to_string();
    let project_id = key
        .get("project_id")
        .and_then(|value| value.as_str())
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            authentication
                .and_then(|auth| auth.get("gcp_project_id"))
                .and_then(|value| value.as_str())
                .filter(|value| !value.trim().is_empty())
        })
        .or_else(|| {
            connector_doc
                .get("gcp_project_id")
                .and_then(|value| value.as_str())
                .filter(|value| !value.trim().is_empty())
        })
        .ok_or_else(|| {
            format!(
                "connector {connector:?} field `authentication` is missing project_id (action {action_id:?})"
            )
        })?
        .to_string();
    Ok((
        project_id,
        GcpAuth::ServiceAccountKey {
            client_email,
            private_key_pem,
        },
    ))
}

#[cfg(feature = "bigquery")]
pub(crate) fn bigquery_auth(
    connector_doc: &serde_json::Value,
    connector: &str,
    action_id: &str,
) -> Result<(String, broker_connectors_enterprise::gcp_pubsub::GcpAuth), String> {
    use broker_connectors_enterprise::gcp_pubsub::GcpAuth;
    // The native form names `authentication`; the reference form names
    // the same object `auth`. Both carry the operator credential.
    // A bare top-level `service_account_json` also carries it.
    if connector_doc
        .get("authentication")
        .or_else(|| connector_doc.get("auth"))
        .is_none()
    {
        if let Some(raw) = connector_doc
            .get("service_account_json")
            .and_then(|value| value.as_str())
        {
            return parse_service_account_key(connector_doc, connector, action_id, raw, None);
        }
        return Err(format!(
            "connector {connector:?} is missing required field: authentication (action {action_id:?})"
        ));
    }
    let authentication = connector_doc
        .get("authentication")
        .or_else(|| connector_doc.get("auth"))
        .expect("authentication present");
    let kind = authentication
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    match kind {
        "service_account_json" => {
            let raw = authentication
                .get("service_account_json")
                .or_else(|| connector_doc.get("service_account_json"))
                .and_then(|value| value.as_str())
                .ok_or_else(|| {
                    format!(
                        "connector {connector:?} is missing required field: authentication.service_account_json (action {action_id:?})"
                    )
                })?;
            parse_service_account_key(connector_doc, connector, action_id, raw, Some(authentication))
        }
        "attached_service_account" => {
            // The reference needs only `type` for this form.
            // Use the project when the operator gives it.
            let project_id = authentication
                .get("gcp_project_id")
                .or_else(|| connector_doc.get("gcp_project_id"))
                .and_then(|value| value.as_str())
                .filter(|value| !value.trim().is_empty())
                .map(|value| value.to_string())
                .unwrap_or_default();
            // The attached identity uses the runtime environment.
            // Use the token when the operator gives it.
            // Use no static header when the token is absent.
            let auth = authentication
                .get("access_token")
                .or_else(|| connector_doc.get("access_token"))
                .and_then(|value| value.as_str())
                .filter(|value| !value.trim().is_empty())
                .map(|value| value.to_string())
                .map(|token| GcpAuth::AccessToken { token })
                .unwrap_or(GcpAuth::None);
            Ok((project_id, auth))
        }
        "access_token" => {
            // The reference token form names the bearer value under
            // `token`; the top-level `access_token` is its alias.
            let token = authentication
                .get("token")
                .or_else(|| authentication.get("access_token"))
                .or_else(|| connector_doc.get("access_token"))
                .and_then(|value| value.as_str())
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| {
                    format!(
                        "connector {connector:?} field `authentication.token` is missing (action {action_id:?})"
                    )
                })?
                .to_string();
            let project_id = authentication
                .get("gcp_project_id")
                .or_else(|| connector_doc.get("gcp_project_id"))
                .or_else(|| connector_doc.get("project_id"))
                .and_then(|value| value.as_str())
                .filter(|value| !value.trim().is_empty())
                .map(|value| value.to_string())
                .unwrap_or_default();
            Ok((project_id, GcpAuth::AccessToken { token }))
        }
        other => Err(format!(
            "connector {connector:?} field `authentication.type` value {other:?} is not supported: use a service account key, an access token or the attached account"
        )),
    }
}

#[cfg(not(feature = "bigquery"))]
pub(crate) fn build_bigquery_sink(
    record: &ActionRecord,
    _connector_doc: &serde_json::Value,
    _opts: &ActionResourceOpts,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    Err(format!(
        "action {:?} needs cargo feature `bigquery` (disabled in this build)",
        record.id
    ))
}

/// Build the action's `disk_log` sink. The connector owns the file
/// path, size and count limits, and rotation; the action owns the
/// record template and write mode.
///
/// The path is relative to the kernel data directory. An absolute path
/// or a `..` component is refused, so a connector cannot write outside
/// the data directory.
#[cfg(test)]
pub(crate) fn build_disk_log_action_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
    opts: &ActionResourceOpts,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    build_disk_log_action_sink_with_dir(record, connector_doc, std::path::Path::new("./data"), opts)
        .map(|sink| sink as Arc<dyn broker_connectors::Sink>)
}

/// Build the `disk_log` sink inside `data_dir`. The file stays in the
/// data directory. The textual checks still run first.
///
/// Batching belongs to the action runtime (RO-01). The file writer
/// appends one record per send.
///
/// It returns the concrete sink. Tests read its config and drive
/// exact timestamps through it.
pub(crate) fn build_disk_log_action_sink_with_dir(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
    data_dir: &std::path::Path,
    _opts: &ActionResourceOpts,
) -> Result<Arc<broker_connectors::disk_log::DiskLogSink>, String> {
    use broker_connectors::disk_log::{
        DiskLogCompression, DiskLogFormat, DiskLogSink, DiskLogSinkConfig, DiskSyncMode,
    };
    let connector = record.connector.trim().to_string();
    let filepath = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["filepath", "file", "path"],
        "filepath",
    )?;
    let (directory, filename_prefix, filename_extension) =
        disk_log_parts_in_dir(&filepath, &record.id, data_dir)?;
    let max_file_size = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["max_file_size"],
        "max_file_size",
    )?;
    let max_file_size_bytes = parse_size(&max_file_size).ok_or_else(|| {
        format!(
            "connector {connector:?} field `max_file_size` value {max_file_size:?} is not a byte size (action {:?})",
            record.id
        )
    })?;
    let max_file_number = connector_doc
        .get("max_file_number")
        .and_then(|value| value.as_u64())
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            format!(
                "connector {connector:?} is missing required field: max_file_number (action {:?})",
                record.id
            )
        })? as usize;
    let rotation = connector_doc.get("rotation");
    let max_file_age_secs = rotation
        .and_then(|value| value.get("period"))
        .and_then(|value| value.as_str())
        .and_then(|period| match period {
            "hour" => Some(3_600),
            "day" => Some(86_400),
            _ => None,
        });
    let max_retention_days = rotation
        .and_then(|value| value.get("retention_period"))
        .and_then(|value| match value {
            serde_json::Value::Number(number) => {
                number.as_u64().and_then(|days| u32::try_from(days).ok())
            }
            serde_json::Value::String(text) if text != "infinity" => text.parse().ok(),
            _ => None,
        });
    let sync_mode = match param_string(record, "write_mode").as_deref() {
        Some("async") => DiskSyncMode::OsDefault,
        _ => DiskSyncMode::EveryBatch,
    };
    let config = DiskLogSinkConfig {
        directory,
        filename_prefix,
        filename_extension,
        format: DiskLogFormat::Raw,
        max_file_size_bytes: Some(max_file_size_bytes),
        max_file_age_secs,
        compression: DiskLogCompression::None,
        max_backup_files: Some(max_file_number),
        max_retention_days,
        sync_mode,
        timeout_ms: connector_timeout_ms(connector_doc),
    };
    let writer = open_disk_log_writer(&config).map_err(|error| {
        format!(
            "action {:?} cannot open its disk_log writer: {error}",
            record.id
        )
    })?;
    let sink = DiskLogSink::new(config, Arc::new(writer)).map_err(|error| {
        format!(
            "action {:?} cannot open its disk_log sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

#[cfg(not(feature = "elasticsearch"))]
pub(crate) fn build_elasticsearch_sink(
    record: &ActionRecord,
    _connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    Err(format!(
        "action {:?} needs cargo feature `elasticsearch` (disabled in this build)",
        record.id
    ))
}

/// Open the file writer on a short current-thread runtime. The action
/// builder runs on both async request paths and the synchronous boot
/// replay, so it cannot await the async writer constructor.
fn open_disk_log_writer(
    config: &broker_connectors::disk_log::DiskLogSinkConfig,
) -> Result<broker_connectors::disk_log::FileDiskLogWriter, String> {
    let config = config.clone();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("disk_log runtime: {error}"))?;
        runtime
            .block_on(broker_connectors::disk_log::FileDiskLogWriter::open(
                &config,
            ))
            .map_err(|error| error.to_string())
    })
    .join()
    .map_err(|_| "disk_log writer thread panicked".to_string())?
}

/// Split the connector `filepath` into data-directory-relative parts.
/// An absolute path or a `..` component is refused: the file must stay
/// inside the kernel data directory. A symlink is refused: it can
/// point outside the data directory.
fn disk_log_parts(filepath: &str, action_id: &str) -> Result<(String, String, String), String> {
    use std::path::{Component, Path};
    let path = Path::new(filepath);
    // Check the symlink first. An absolute symlink must report the symlink.
    if disk_log_has_symlink(path) {
        return Err(format!(
            "action {action_id:?} field `connector.filepath` must not be a symlink that leaves the data directory: {filepath:?}"
        ));
    }
    if path.is_absolute() {
        return Err(format!(
            "action {action_id:?} field `connector.filepath` must be relative to the data directory: {filepath:?}"
        ));
    }
    if path.components().any(|part| {
        matches!(
            part,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err(format!(
            "action {action_id:?} field `connector.filepath` must not leave the data directory: {filepath:?}"
        ));
    }
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            format!(
                "action {action_id:?} field `connector.filepath` must name one file: {filepath:?}"
            )
        })?;
    let (prefix, extension) = match file_name.rsplit_once('.') {
        Some((prefix, extension)) if !prefix.is_empty() && !extension.is_empty() => {
            (prefix.to_string(), extension.to_string())
        }
        _ => (file_name.to_string(), "log".to_string()),
    };
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(|parent| parent.to_string_lossy().into_owned())
        .unwrap_or_else(|| ".".to_string());
    Ok((directory, prefix, extension))
}

/// True when the path or any existing parent is a symlink.
fn disk_log_has_symlink(path: &std::path::Path) -> bool {
    let mut current = path;
    loop {
        if std::fs::symlink_metadata(current).is_ok_and(|meta| meta.file_type().is_symlink()) {
            return true;
        }
        match current.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => current = parent,
            _ => break,
        }
        if current.as_os_str().is_empty() {
            break;
        }
    }
    false
}

/// Split `filepath` and join the directory with `data_dir`.
/// The file stays inside the data directory.
/// The textual checks run on the relative path first.
fn disk_log_parts_in_dir(
    filepath: &str,
    action_id: &str,
    data_dir: &std::path::Path,
) -> Result<(String, String, String), String> {
    let (relative_dir, prefix, extension) = disk_log_parts(filepath, action_id)?;
    let joined_file = data_dir.join(filepath);
    if disk_log_has_symlink(&joined_file) {
        return Err(format!(
            "action {action_id:?} field `connector.filepath` must not be a symlink that leaves the data directory: {filepath:?}"
        ));
    }
    let joined_dir = if relative_dir == "." {
        data_dir.to_string_lossy().into_owned()
    } else {
        data_dir.join(&relative_dir).to_string_lossy().into_owned()
    };
    Ok((joined_dir, prefix, extension))
}

/// Parse `5mb`, `1gb`, `1024` and `64MiB` style sizes into bytes.
fn parse_size(text: &str) -> Option<u64> {
    let trimmed = text.trim();
    let split = trimmed
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(trimmed.len());
    let (number, unit) = trimmed.split_at(split);
    let value: f64 = number.parse().ok()?;
    let multiplier = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1.0,
        "k" | "kb" | "kib" => 1024.0,
        "m" | "mb" | "mib" => 1024.0 * 1024.0,
        "g" | "gb" | "gib" => 1024.0 * 1024.0 * 1024.0,
        "t" | "tb" | "tib" => 1024.0 * 1024.0 * 1024.0 * 1024.0,
        _ => return None,
    };
    let bytes = value * multiplier;
    if bytes <= 0.0 || bytes > u64::MAX as f64 {
        return None;
    }
    Some(bytes as u64)
}

/// Build the action's `snowflake` sink. The account, user, server and
/// private key come from the connector; the database, schema and pipe
/// come from the action. The reference `pipe` names the streaming
/// pipe: the driver writes the streaming rows API with the pipe as
/// the table template and the streaming channel.
///
/// Batching belongs to the action runtime (RO-01). The driver keeps
/// one row per send with no linger.
pub(crate) fn build_snowflake_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
    _opts: &ActionResourceOpts,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::snowflake::{
        HttpSnowflakeTransport, SnowflakeSink, SnowflakeSinkConfig,
    };
    let connector = record.connector.trim().to_string();
    let account = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["account"],
        "account",
    )?;
    let user = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["pipe_user", "user", "username"],
        "pipe_user",
    )?;
    let private_key_pem = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["private_key", "private_key_pem"],
        "private_key",
    )?;
    let database = param_string(record, "database")
        .ok_or_else(|| format!("action {:?} is missing required field: database", record.id))?;
    let schema = param_string(record, "schema")
        .ok_or_else(|| format!("action {:?} is missing required field: schema", record.id))?;
    let pipe = param_string(record, "pipe")
        .ok_or_else(|| format!("action {:?} is missing required field: pipe", record.id))?;
    let endpoint = first_string(connector_doc, &["server", "endpoint", "url"])
        .map(|value| http_endpoint(&value));
    let config = SnowflakeSinkConfig {
        account,
        user,
        database,
        schema,
        table_template: pipe.clone(),
        private_key_pem,
        endpoint,
        role: None,
        channel: pipe,
        column_mappings: std::collections::HashMap::new(),
        // One row per send with no linger: the action runtime owns
        // batching (RO-01). Cap the batch at 4 MiB (under the 16 MiB
        // streaming limit). Retry 3 times.
        batch_size: Some(1),
        batch_bytes: Some(4_194_304),
        linger_ms: Some(0),
        max_retries: Some(3),
        initial_backoff_ms: Some(100),
        max_backoff_ms: Some(2_000),
        timeout_ms: connector_timeout_ms(connector_doc),
    };
    let client = reqwest::Client::builder()
        .timeout(config.timeout())
        .build()
        .unwrap_or_default();
    let transport = HttpSnowflakeTransport::new(&config, client).map_err(|error| {
        format!(
            "action {:?} cannot build its snowflake transport: {error}",
            record.id
        )
    })?;
    let sink = SnowflakeSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its snowflake sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

/// Build the action's `kafka` sink: the connection comes from the
/// connector; the topic, key, headers and strategy come from the
/// action.
///
/// Values and defaults:
/// - `bootstrap_hosts`: the connector's `bootstrap_hosts` (aliases
///   `bootstrap_servers`, `servers`); required, no invented default.
/// - `authentication`: absent or `none` only. SASL is refused with the
///   field named: the driver dials plaintext with no SASL wiring yet.
/// - `ssl`: refused when present: the driver dials plaintext with no
///   TLS knob.
/// - `topic`: the action parameter, required. `${topic}` renders the
///   ingress topic.
/// - `message.key`: the record key template; absent means a null key.
/// - `partition_strategy`: `random` (default, spreads rows at random)
///   or `key_dispatch` (murmur2 hash of the key, sticky).
/// - `required_acks`: `all_isr` (default), `leader_only` or `none`.
/// - `kafka_headers`, `kafka_ext_headers`,
///   `kafka_header_value_encode_mode`: the reference header fields.
/// - Batching belongs to the action runtime, not this driver. The
///   inner sink keeps one row per send with no linger, so the runtime
///   owns the batch size and the batch timer.
#[cfg(feature = "kafka")]
pub(crate) fn kafka_sink_config(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<broker_connectors::kafka::KafkaSinkConfig, String> {
    use broker_connectors::kafka::{KafkaHeaderEncodeMode, KafkaSinkConfig};
    let connector = record.connector.trim().to_string();
    validate_connector_io_bounds(
        connector_doc,
        &connector,
        &format!("action {:?}", record.id),
    )?;
    let bootstrap = required_nested_field(
        connector_doc,
        &connector,
        &record.id,
        &["bootstrap_hosts", "bootstrap_servers", "servers"],
        "bootstrap_hosts",
    )?;
    let auth = connector_doc.get("authentication").or_else(|| {
        connector_doc
            .get("parameters")
            .and_then(|params| params.get("authentication"))
    });
    let auth_none = match auth {
        None => true,
        Some(value) if value.is_null() => true,
        Some(serde_json::Value::String(text)) => text.trim().is_empty() || text.trim() == "none",
        Some(serde_json::Value::Object(map)) => map.is_empty(),
        Some(_) => false,
    };
    if !auth_none {
        return Err(format!(
            "connector {connector:?} field `authentication` only supports `none` for kind `kafka`: the driver dials plaintext with no SASL wiring (action {:?})",
            record.id
        ));
    }
    let topic = param_string(record, "topic").ok_or_else(|| {
        format!(
            "action {:?} is missing required field: topic (field `parameters.topic`)",
            record.id
        )
    })?;
    let strategy =
        param_string(record, "partition_strategy").unwrap_or_else(|| "random".to_string());
    let random_partition = match strategy.as_str() {
        "random" => true,
        "key_dispatch" => false,
        other => {
            return Err(format!(
                "action {:?} field `parameters.partition_strategy` must be one of random, key_dispatch: {other:?}",
                record.id
            ));
        }
    };
    let acks = match param_string(record, "required_acks")
        .unwrap_or_else(|| "all_isr".to_string())
        .as_str()
    {
        "all_isr" => "all",
        "leader_only" => "1",
        "none" => "0",
        other => {
            return Err(format!(
                "action {:?} field `parameters.required_acks` must be one of all_isr, leader_only, none: {other:?}",
                record.id
            ));
        }
    };
    let encode_mode = match param_string(record, "kafka_header_value_encode_mode")
        .unwrap_or_else(|| "none".to_string())
        .as_str()
    {
        "none" => KafkaHeaderEncodeMode::None,
        "json" => KafkaHeaderEncodeMode::Json,
        other => {
            return Err(format!(
                "action {:?} field `parameters.kafka_header_value_encode_mode` must be one of none, json: {other:?}",
                record.id
            ));
        }
    };
    let mut ext_headers = Vec::new();
    if let Some(items) = param_value(record, "kafka_ext_headers") {
        let items = items.as_array().ok_or_else(|| {
            format!(
                "action {:?} field `parameters.kafka_ext_headers` must be an array",
                record.id
            )
        })?;
        for item in items {
            let key = item
                .get("kafka_ext_header_key")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    format!(
                        "action {:?} field `parameters.kafka_ext_headers` needs a non-empty `kafka_ext_header_key`",
                        record.id
                    )
                })?;
            let value = item
                .get("kafka_ext_header_value")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    format!(
                        "action {:?} field `parameters.kafka_ext_headers` needs a string `kafka_ext_header_value`",
                        record.id
                    )
                })?;
            ext_headers.push((key.to_string(), value.to_string()));
        }
    }
    let config = KafkaSinkConfig {
        bootstrap_servers: bootstrap,
        topic_template: topic,
        partition_key_field: None,
        // Reference kafka_message.key defaults to `${.clientid}`.
        key_template: param_message_field(record, "key")
            .or_else(|| Some("${.clientid}".to_string())),
        // Twelve partitions match the loopback fake Metadata. The
        // driver hashes with this count. Real topics report their own
        // count through broker Metadata.
        partitions: MV03A_DEFAULT_PARTITIONS,
        client_id: format!("indramqtt-{}", record.id),
        acks: acks.to_string(),
        batch_max_records: MV03A_BATCH_SIZE_ONE,
        batch_max_bytes: MV03A_BATCH_MAX_BYTES,
        random_partition,
        headers_template: param_string(record, "kafka_headers"),
        extra_headers: ext_headers,
        header_encode_mode: encode_mode,
    };
    Ok(config)
}

/// Build the action's `kafka` sink from its config. The config
/// carries the applied defaults; the transport and sink add no new
/// values.
#[cfg(feature = "kafka")]
pub(crate) fn build_kafka_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors::kafka::{KafkaSink, RdkafkaKafkaTransport};
    let config = kafka_sink_config(record, connector_doc)?;
    // Production write path on the maintained `rdkafka` driver; the
    // hand-written TCP framing stays for offline unit tests only.
    let transport = RdkafkaKafkaTransport::new(&config).map_err(|error| {
        format!(
            "action {:?} cannot build its kafka transport: {error}",
            record.id
        )
    })?;
    let sink = KafkaSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its kafka sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

#[cfg(not(feature = "kafka"))]
pub(crate) fn build_kafka_sink(
    record: &ActionRecord,
    _connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    Err(format!(
        "action {:?} needs cargo feature `kafka` (disabled in this build)",
        record.id
    ))
}

/// Build the action's `confluent` sink: the Cloud endpoints and API
/// credentials come from the connector; the topic, key, headers and
/// strategy come from the action.
///
/// Values and defaults:
/// - `bootstrap_hosts`: the connector's hosts (aliases
///   `bootstrap_servers`, `servers`); required, no invented default.
/// - `api_key`/`api_secret` (aliases `username`/`password`): the
///   connector pair; required, both parts.
/// - `ssl.enable`: the connector flag, default true. It maps to the
///   driver `SASL_SSL`/`SASL_PLAINTEXT` protocol. Reason: the hosted
///   endpoints need TLS; local qualification has no certificate.
/// - `authentication`: the reference object form is stored opaquely;
///   the driver always uses SASL with the API pair above.
/// - `topic`: the action parameter, required.
/// - `message.key`: the partition key template; absent means a null
///   key. The reference default `${.clientid}` reads the payload
///   `client_id` field.
/// - `partition_strategy`: `random` (default) or `key_dispatch`.
/// - `required_acks`: `all_isr` (default), `leader_only` or `none`.
/// - `kafka_headers`, `kafka_ext_headers`,
///   `kafka_header_value_encode_mode`: the reference header fields.
/// - Batching belongs to the action runtime, not this driver. The
///   inner sink keeps one row per send in a bounded buffer, so the
///   runtime owns the batch size and the batch timer.
#[cfg(feature = "confluent")]
pub(crate) fn confluent_sink_config(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<broker_connectors_enterprise::confluent::ConfluentKafkaConfig, String> {
    use broker_connectors_enterprise::confluent::{
        ConfluentHeaderEncodeMode, ConfluentKafkaConfig, ConfluentSecurityProtocol,
    };
    let connector = record.connector.trim().to_string();
    for key in [
        "pool_size",
        "pipelining",
        "enable_pipelining",
        "max_inactive",
    ] {
        let present = connector_doc.get(key).or_else(|| {
            connector_doc
                .get("parameters")
                .and_then(|params| params.get(key))
        });
        if present.is_some_and(|v| !v.is_null()) {
            return Err(format!(
                "connector {connector:?} field `{key}` is not supported; remove it (action {:?})",
                record.id
            ));
        }
    }
    let bootstrap = required_nested_field(
        connector_doc,
        &connector,
        &record.id,
        &["bootstrap_hosts", "bootstrap_servers", "servers"],
        "bootstrap_hosts",
    )?;
    // Credentials: top-level or `parameters` first (the console
    // shape), then the reference `authentication` object.
    let nested_auth = connector_doc
        .get("authentication")
        .or_else(|| {
            connector_doc
                .get("parameters")
                .and_then(|params| params.get("authentication"))
        })
        .and_then(|auth| auth.as_object());
    let api_key = nested_field(connector_doc, &["api_key", "username"])
        .map(|value| value.into_owned())
        .or_else(|| {
            nested_auth
                .and_then(|auth| auth.get("username"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
        .ok_or_else(|| {
            format!(
                "connector {connector:?} is missing required field: api_key (action {:?})",
                record.id
            )
        })?;
    let api_secret = nested_field(connector_doc, &["api_secret", "password"])
        .map(|value| value.into_owned())
        .or_else(|| {
            nested_auth
                .and_then(|auth| auth.get("password"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        })
        .ok_or_else(|| {
            format!(
                "connector {connector:?} is missing required field: api_secret (action {:?})",
                record.id
            )
        })?;
    let tls_on = connector_doc
        .get("ssl")
        .or_else(|| {
            connector_doc
                .get("parameters")
                .and_then(|params| params.get("ssl"))
        })
        .and_then(|ssl| ssl.get("enable"))
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let topic = param_string(record, "topic").ok_or_else(|| {
        format!(
            "action {:?} is missing required field: topic (field `parameters.topic`)",
            record.id
        )
    })?;
    let strategy =
        param_string(record, "partition_strategy").unwrap_or_else(|| "random".to_string());
    let random_partition = match strategy.as_str() {
        "random" => true,
        "key_dispatch" => false,
        other => {
            return Err(format!(
                "action {:?} field `parameters.partition_strategy` must be one of random, key_dispatch: {other:?}",
                record.id
            ));
        }
    };
    let acks = match param_string(record, "required_acks")
        .unwrap_or_else(|| "all_isr".to_string())
        .as_str()
    {
        "all_isr" => "all",
        "leader_only" => "1",
        "none" => "0",
        other => {
            return Err(format!(
                "action {:?} field `parameters.required_acks` must be one of all_isr, leader_only, none: {other:?}",
                record.id
            ));
        }
    };
    let encode_mode = match param_string(record, "kafka_header_value_encode_mode")
        .unwrap_or_else(|| "none".to_string())
        .as_str()
    {
        "none" => ConfluentHeaderEncodeMode::None,
        "json" => ConfluentHeaderEncodeMode::Json,
        other => {
            return Err(format!(
                "action {:?} field `parameters.kafka_header_value_encode_mode` must be one of none, json: {other:?}",
                record.id
            ));
        }
    };
    let mut ext_headers = Vec::new();
    if let Some(items) = param_value(record, "kafka_ext_headers") {
        let items = items.as_array().ok_or_else(|| {
            format!(
                "action {:?} field `parameters.kafka_ext_headers` must be an array",
                record.id
            )
        })?;
        for item in items {
            let key = item
                .get("kafka_ext_header_key")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    format!(
                        "action {:?} field `parameters.kafka_ext_headers` needs a non-empty `kafka_ext_header_key`",
                        record.id
                    )
                })?;
            let value = item
                .get("kafka_ext_header_value")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    format!(
                        "action {:?} field `parameters.kafka_ext_headers` needs a string `kafka_ext_header_value`",
                        record.id
                    )
                })?;
            ext_headers.push((key.to_string(), value.to_string()));
        }
    }
    Ok(ConfluentKafkaConfig {
        bootstrap_servers: bootstrap
            .split(',')
            .map(|part| part.trim().to_string())
            .filter(|part| !part.is_empty())
            .collect(),
        api_key,
        api_secret,
        auth_mechanism: broker_connectors_enterprise::confluent::SaslMechanism::Plain,
        security_protocol: if tls_on {
            ConfluentSecurityProtocol::SaslSsl
        } else {
            ConfluentSecurityProtocol::SaslPlaintext
        },
        topic_template: topic,
        // Reference kafka_message.key defaults to `${.clientid}`.
        partition_key_template: param_message_field(record, "key")
            .or_else(|| Some("${.clientid}".to_string())),
        schema_registry: None,
        // Twelve partitions match the loopback fake Metadata. The
        // driver hashes with this count. Real topics report their own
        // count through broker Metadata.
        partitions: MV03A_DEFAULT_PARTITIONS,
        acks: acks.to_string(),
        random_partition,
        headers_template: param_string(record, "kafka_headers"),
        extra_headers: ext_headers,
        header_encode_mode: encode_mode,
        batch_size: Some(MV03A_BATCH_SIZE_ONE),
        // Bounded inner buffer: 1024 rows cap the memory the driver
        // holds while the runtime owns batching (RO-01).
        buffer_capacity: Some(MV03A_PROXY_BUFFER_ROWS),
        timeout_ms: connector_timeout_ms(connector_doc),
    })
}

/// Build the action's `confluent` sink from its config. The config
/// carries the applied defaults; the transport and sink add no new
/// values.
#[cfg(feature = "confluent")]
pub(crate) fn build_confluent_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::confluent::{ConfluentKafkaSink, RdkafkaConfluentTransport};
    let config = confluent_sink_config(record, connector_doc)?;
    if config.bootstrap_servers.is_empty() {
        return Err(format!(
            "action {:?} is missing required field: bootstrap_hosts (connector {:?})",
            record.id, record.connector
        ));
    }
    let transport = RdkafkaConfluentTransport::new(&config).map_err(|error| {
        format!(
            "action {:?} cannot build its confluent transport: {error}",
            record.id
        )
    })?;
    let sink = ConfluentKafkaSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its confluent sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

#[cfg(not(feature = "confluent"))]
pub(crate) fn build_confluent_sink(
    record: &ActionRecord,
    _connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    Err(format!(
        "action {:?} needs cargo feature `confluent` (disabled in this build)",
        record.id
    ))
}

/// Build the action's `rabbitmq` sink: the connection comes from the
/// connector; the exchange, routing key, headers and body come from
/// the action.
///
/// Values and defaults:
/// - `servers` (alias `server`): the connector target; required,
///   no invented default. A missing target fails closed.
/// - `username`/`password`: the connector pair; required, no
///   invented default.
/// - `virtual_host`: the connector value; default `/`, the reference
///   default.
/// - `exchange`: the action value; default `""` (the default
///   exchange), the reference action default. An empty exchange with
///   a queue in `routing_key` publishes direct.
/// - `routing_key`: the action template, required. MQTT slashes
///   become AMQP dots.
/// - `delivery_mode`: `non_persistent` (default) or `persistent`.
/// - `headers_template`: array of `{key, value}` templates; the
///   values render `${topic}` and `${payload}`.
/// - `payload_template`: the body template; absent forwards the raw
///   event bytes.
/// - Batching belongs to the action runtime: each send is one
///   broker-acknowledged publish.
#[cfg(feature = "rabbitmq")]
pub(crate) fn rabbitmq_sink_config(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<broker_connectors::rabbitmq::RabbitMqSinkConfig, String> {
    use broker_connectors::rabbitmq::RabbitMqSinkConfig;
    let connector = record.connector.trim().to_string();
    validate_connector_io_bounds(
        connector_doc,
        &connector,
        &format!("action {:?}", record.id),
    )?;
    // The driver derives the port from `servers` and has no heartbeat
    // or timeout knob. A present value is refused instead of stored
    // as an ignored setting.
    for field in ["port", "heartbeat", "timeout"] {
        let present = connector_doc.get(field).or_else(|| {
            connector_doc
                .get("parameters")
                .and_then(|params| params.get(field))
        });
        if present.is_some_and(|v| !v.is_null()) {
            return Err(format!(
                "connector {connector:?} field `{field}` is not supported for this kind: remove it (action {:?})",
                record.id
            ));
        }
    }
    let servers = required_nested_field(
        connector_doc,
        &connector,
        &record.id,
        &["servers", "server"],
        "servers",
    )?;
    let username = required_nested_field(
        connector_doc,
        &connector,
        &record.id,
        &["username", "user"],
        "username",
    )?;
    let password = required_nested_field(
        connector_doc,
        &connector,
        &record.id,
        &["password", "pass"],
        "password",
    )?;
    let virtual_host = nested_field(connector_doc, &["virtual_host", "vhost"])
        .map(|value| value.into_owned())
        .unwrap_or_else(|| "/".to_string());
    let host = servers.trim().trim_start_matches("amqp://");
    let endpoint = format!(
        "amqp://{username}:{password}@{host}{vhost}",
        vhost = virtual_host_for_url(&virtual_host)
    );
    let routing_key = param_string(record, "routing_key").ok_or_else(|| {
        format!(
            "action {:?} is missing required field: routing_key (field `parameters.routing_key`)",
            record.id
        )
    })?;
    let delivery_mode = match param_string(record, "delivery_mode")
        .unwrap_or_else(|| "non_persistent".to_string())
        .as_str()
    {
        "non_persistent" => RABBITMQ_NON_PERSISTENT,
        "persistent" => RABBITMQ_PERSISTENT,
        other => {
            return Err(format!(
                "action {:?} field `parameters.delivery_mode` must be one of non_persistent, persistent: {other:?}",
                record.id
            ));
        }
    };
    let mut extra_headers = Vec::new();
    if let Some(items) = param_value(record, "headers_template") {
        let items = items.as_array().ok_or_else(|| {
            format!(
                "action {:?} field `parameters.headers_template` must be an array",
                record.id
            )
        })?;
        for item in items {
            let key = item
                .get("key")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    format!(
                        "action {:?} field `parameters.headers_template` needs a non-empty `key`",
                        record.id
                    )
                })?;
            let value = item.get("value").and_then(|v| v.as_str()).ok_or_else(|| {
                format!(
                    "action {:?} field `parameters.headers_template` needs a string `value`",
                    record.id
                )
            })?;
            extra_headers.push((key.to_string(), value.to_string()));
        }
    }
    Ok(RabbitMqSinkConfig {
        endpoint,
        exchange: param_string(record, "exchange").unwrap_or_default(),
        routing_key_template: routing_key,
        delivery_mode,
        extra_headers,
        payload_template: param_string(record, "payload_template"),
    })
}

/// Render one virtual host for an AMQP URL: `/` becomes the default
/// `%2f` segment, any other value is used as the path.
#[cfg(feature = "rabbitmq")]
fn virtual_host_for_url(virtual_host: &str) -> String {
    let trimmed = virtual_host.trim();
    if trimmed.is_empty() || trimmed == "/" {
        "/%2f".to_string()
    } else if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    }
}

/// Build the action's `rabbitmq` sink from its config. The config
/// carries the applied defaults; the transport and sink add no new
/// values.
#[cfg(feature = "rabbitmq")]
pub(crate) fn build_rabbitmq_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors::rabbitmq::{LapinRabbitTransport, RabbitMqSink};
    let config = rabbitmq_sink_config(record, connector_doc)?;
    // Production write path on the maintained `lapin` driver; the
    // hand-written TCP framing stays for offline unit tests only.
    let transport = LapinRabbitTransport::new(&config.endpoint).map_err(|error| {
        format!(
            "action {:?} cannot build its rabbitmq transport: {error}",
            record.id
        )
    })?;
    let sink = RabbitMqSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its rabbitmq sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

#[cfg(not(feature = "rabbitmq"))]
pub(crate) fn build_rabbitmq_sink(
    record: &ActionRecord,
    _connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    Err(format!(
        "action {:?} needs cargo feature `rabbitmq` (disabled in this build)",
        record.id
    ))
}

/// Build the action's `redis` sink: the endpoint comes from the
/// connector; the command comes from the action.
///
/// Values and defaults:
/// - `servers` (alias `server`): the connector host and port;
///   required, no invented default. The port defaults to 6379, the
///   reference default.
/// - `password`: the connector secret; default empty.
/// - `username`: the connector ACL name; default empty.
/// - `database`: the connector index; default 0, the reference
///   default.
/// - `command_template`: the action array, required. Each entry is one
///   RESP argument rendered per event (`${topic}`, `${payload}`); a
///   value with spaces stays one argument and never becomes a second
///   command.
pub(crate) fn redis_sink_config(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<broker_connectors::redis::RedisSinkConfig, String> {
    use broker_connectors::redis::{RedisCommandKind, RedisSinkConfig};
    let connector = record.connector.trim().to_string();
    validate_connector_io_bounds(
        connector_doc,
        &connector,
        &format!("action {:?}", record.id),
    )?;
    let servers = required_nested_field(
        connector_doc,
        &connector,
        &record.id,
        &["servers", "server", "host"],
        "servers",
    )?;
    let password = nested_field(connector_doc, &["password", "pass"])
        .map(|value| value.into_owned())
        .unwrap_or_default();
    let username = nested_field(connector_doc, &["username", "user"])
        .map(|value| value.into_owned())
        .unwrap_or_default();
    let database = connector_doc
        .get("database")
        .or_else(|| {
            connector_doc
                .get("parameters")
                .and_then(|params| params.get("database"))
        })
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let mut authority = String::new();
    if !username.is_empty() {
        authority.push_str(&username);
        authority.push(':');
    } else if !password.is_empty() {
        authority.push(':');
    }
    if !password.is_empty() {
        authority.push_str(&password);
        authority.push('@');
    } else if !username.is_empty() {
        authority.push('@');
    }
    let (host, port) = match servers.trim().rsplit_once(':') {
        Some((host, port)) => (host.trim(), port.trim()),
        None => (servers.trim(), "6379"),
    };
    if host.is_empty() {
        return Err(format!(
            "connector {connector:?} is missing required field: servers (action {:?})",
            record.id
        ));
    }
    let port: u16 = port.parse().map_err(|_| {
        format!(
            "connector {connector:?} field `servers` holds a bad port (action {:?})",
            record.id
        )
    })?;
    let endpoint = format!("redis://{authority}{host}:{port}/{database}");
    let argv = param_value(record, "command_template")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            format!(
                "action {:?} is missing required field: command_template (field `parameters.command_template`)",
                record.id
            )
        })?
        .iter()
        .map(|entry| {
            entry.as_str().map(str::trim).filter(|s| !s.is_empty()).map(str::to_string).ok_or_else(|| {
                format!(
                    "action {:?} field `parameters.command_template` must be a non-empty array of non-blank strings",
                    record.id
                )
            })
        })
        .collect::<Result<Vec<String>, String>>()?;
    if argv.is_empty() {
        return Err(format!(
            "action {:?} is missing required field: command_template (field `parameters.command_template`)",
            record.id
        ));
    }
    Ok(RedisSinkConfig {
        endpoint,
        command: RedisCommandKind::Raw {
            argv_templates: argv,
        },
    })
}

/// Build the action's `redis` sink from its config. The config
/// carries the applied defaults; the transport and sink add no new
/// values.
pub(crate) fn build_redis_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors::redis::{DriverRedisTransport, RedisSink};
    let config = redis_sink_config(record, connector_doc)?;
    // Production write path on the maintained `redis` driver; the
    // hand-written TCP framing stays for offline unit tests only.
    let transport = DriverRedisTransport::new(&config.endpoint).map_err(|error| {
        format!(
            "action {:?} cannot build its redis transport: {error}",
            record.id
        )
    })?;
    let sink = RedisSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its redis sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

/// Build the action's `mqtt_bridge` sink: the remote broker comes
/// from the connector; the topic, QoS, retain flag and body come from
/// the action.
///
/// Values and defaults:
/// - `server`: the connector's `server` (alias `broker_address`);
///   required, no invented default.
/// - `clientid_prefix`, `username`, `password`: the connector egress
///   identity. The client id defaults to the connector id.
///   `client_id` is not a reference field and is refused.
/// - `keepalive` (duration, default 160 s, the reference default) and
///   `clean_start` (default true) apply to the remote session.
/// - `proto_ver`: the connector version, default `v4`; only `v3`,
///   `v4` and `v5` are accepted. `v3` and `v4` use 3.1.1 framing.
/// - `topic`: the action remote topic, required. Templates are
///   rendered per event; the remapped topic never matches the local
///   forwarding rule again, so a bridged message is not bridged back.
/// - `payload`: the action body template; absent forwards the raw
///   event bytes.
/// - `qos`: 0, 1 (default) or 2. `retain`: default false.
/// - The action runtime owns batching: one row per send with a short
///   linger and a bounded inflight window.
pub(crate) fn mqtt_bridge_sink_config(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<broker_connectors::MqttBridgeSinkConfig, String> {
    use broker_connectors::{MqttBridgeProtocol, MqttBridgeSinkConfig};
    let connector = record.connector.trim().to_string();
    validate_connector_io_bounds(
        connector_doc,
        &connector,
        &format!("action {:?}", record.id),
    )?;
    let server = required_nested_field(
        connector_doc,
        &connector,
        &record.id,
        &["server", "broker_address", "broker", "host"],
        "server",
    )?;
    if let Some(version) = nested_field(connector_doc, &["proto_ver", "protocol_version"]) {
        let version = version.into_owned();
        if !["v3", "v4", "v5"].contains(&version.as_str()) {
            return Err(format!(
                "connector {connector:?} field `proto_ver` must be one of v3, v4, v5: {version:?} (action {:?})",
                record.id
            ));
        }
    }
    let qos = match param_value(record, "qos").filter(|v| !v.is_null()) {
        None => 1,
        Some(value) => {
            if let Some(n) = value.as_u64() {
                if n <= 2 {
                    n as u8
                } else {
                    return Err(format!(
                        "action {:?} field `parameters.qos` must be one of 0, 1, 2",
                        record.id
                    ));
                }
            } else if let Some(text) = value.as_str() {
                match text.trim() {
                    "0" => 0,
                    "1" => 1,
                    "2" => 2,
                    _ => {
                        return Err(format!(
                            "action {:?} field `parameters.qos` must be one of 0, 1, 2",
                            record.id
                        ));
                    }
                }
            } else {
                return Err(format!(
                    "action {:?} field `parameters.qos` must be one of 0, 1, 2",
                    record.id
                ));
            }
        }
    };
    let retain = param_value(record, "retain")
        .filter(|v| !v.is_null())
        .map(|v| v.as_bool())
        .unwrap_or(Some(false))
        .ok_or_else(|| {
            format!(
                "action {:?} field `parameters.retain` must be a boolean",
                record.id
            )
        })?;
    let topic = param_string(record, "topic").ok_or_else(|| {
        format!(
            "action {:?} is missing required field: topic (field `parameters.topic`)",
            record.id
        )
    })?;
    let protocol = match nested_field(connector_doc, &["proto_ver", "protocol_version"])
        .map(|v| v.into_owned())
        .as_deref()
    {
        Some("v5") => MqttBridgeProtocol::V50,
        // The driver speaks 3.1.1 framing for both 3.1 (`v3`) and 3.1.1
        // (`v4`). Both map to V311.
        Some("v3") | Some("v4") => MqttBridgeProtocol::V311,
        // Validated above to be one of v3, v4, v5. Absent defaults to
        // v4 (the reference default), which is 3.1.1 framing.
        None => MqttBridgeProtocol::V311,
        Some(other) => {
            return Err(format!(
                "connector {connector:?} field `proto_ver` must be one of v3, v4, v5: {other:?} (action {:?})",
                record.id
            ));
        }
    };
    // The connector `keepalive` and `clean_start` are top-level or inside
    // the reference `parameters` object.
    let nested_value = |key: &str| {
        connector_doc
            .get(key)
            .or_else(|| connector_doc.get("parameters").and_then(|p| p.get(key)))
            .filter(|value| !value.is_null())
    };
    let keep_alive_secs = match nested_value("keepalive") {
        None => MV03A_MQTT_KEEPALIVE_SECS,
        Some(value) => crate::v5::rules::parse_bridge_keepalive(&connector, value)?,
    };
    let clean_start = match nested_value("clean_start") {
        None => true,
        Some(value) => value.as_bool().ok_or_else(|| {
            format!("connector {connector:?} field `clean_start` must be a boolean")
        })?,
    };
    if nested_value("client_id").is_some() {
        return Err(format!(
            "connector {connector:?} field `client_id` is not supported by the MQTT bridge; use `clientid_prefix` (action {:?})",
            record.id
        ));
    }
    Ok(MqttBridgeSinkConfig {
        broker_address: server,
        client_id: nested_field(connector_doc, &["clientid_prefix"])
            .map(|value| value.into_owned())
            .unwrap_or_else(|| connector.clone()),
        clean_start,
        username: nested_field(connector_doc, &["username", "user"]).map(|v| v.into_owned()),
        password: nested_field(connector_doc, &["password", "pass"]).map(|v| v.into_owned()),
        // Reference connector_mqtt defaults keepalive to 160 s.
        keep_alive_secs,
        topic_prefix: None,
        topic_template: Some(topic),
        payload_template: param_string(record, "payload"),
        qos_override: Some(qos),
        retain_override: Some(retain),
        // One row per send: the action runtime owns batching (RO-01).
        max_inflight: Some(MV03A_MAX_INFLIGHT),
        max_batch_size: Some(MV03A_BATCH_SIZE_ONE),
        linger_ms: Some(MV03A_LINGER_MS),
        protocol,
    })
}

/// Build the action's `mqtt_bridge` sink from its config. The config
/// carries the applied defaults; the transport and sink add no new
/// values.
pub(crate) fn build_mqtt_bridge_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors::{MqttBridgeSink, RumqttcMqttBridgeTransport};
    let config = mqtt_bridge_sink_config(record, connector_doc)?;
    // Production write path on the maintained `rumqttc` driver; the
    // hand-written TCP framing stays for offline unit tests only.
    let transport = RumqttcMqttBridgeTransport::new(&config).map_err(|error| {
        format!(
            "action {:?} cannot build its mqtt_bridge transport: {error}",
            record.id
        )
    })?;
    let sink = MqttBridgeSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its mqtt_bridge sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

/// Build the action's `pulsar` sink: the service URL and tenant come
/// from the connector; the topic, key and strategy come from the
/// action.
///
/// Values and defaults:
/// - `servers` (alias `service_url`): the connector's
///   `pulsar://host:6650` URL; required, no invented default. An
///   `http(s)://` address is refused: it names the admin entry, not
///   the binary entry the driver dials.
/// - `tenant`/`namespace`: the connector values; defaults `public`
///   and `default`, the reference-less driver defaults.
/// - `authentication`: absent or `none` only; token and basic forms
///   are refused with the field named.
/// - `pulsar_topic`: the action topic template, required.
/// - `message.key`: the partition key template; absent means no key.
/// - `strategy`: `random` (default), `roundrobin` or `key_dispatch`.
///   The key selects the routing: a set key is hashed, an absent key
///   spreads across partitions.
/// - Batching belongs to the action runtime, not this driver. The
///   inner sink keeps one row per send with a short linger, so the
///   runtime owns the batch size and the batch timer.
pub(crate) fn pulsar_sink_config(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<broker_connectors_enterprise::pulsar::PulsarSinkConfig, String> {
    use broker_connectors_enterprise::pulsar::{PulsarAuth, PulsarSinkConfig};
    let connector = record.connector.trim().to_string();
    validate_connector_io_bounds(
        connector_doc,
        &connector,
        &format!("action {:?}", record.id),
    )?;
    let service_url = required_nested_field(
        connector_doc,
        &connector,
        &record.id,
        &["servers", "service_url", "server", "endpoint"],
        "servers",
    )?;
    if !service_url.starts_with("pulsar://") && !service_url.starts_with("pulsar+ssl://") {
        return Err(format!(
            "connector {connector:?} field `servers` must be a pulsar:// service URL (action {:?})",
            record.id
        ));
    }
    let auth_value = connector_doc.get("authentication").or_else(|| {
        connector_doc
            .get("parameters")
            .and_then(|params| params.get("authentication"))
    });
    let auth_none = match auth_value {
        None => true,
        Some(value) if value.is_null() => true,
        Some(serde_json::Value::String(text)) => text.trim().is_empty() || text.trim() == "none",
        Some(serde_json::Value::Object(map)) => map.is_empty(),
        Some(_) => false,
    };
    if !auth_none {
        return Err(format!(
            "connector {connector:?} field `authentication` only supports `none` for kind `pulsar`: token and basic forms are not qualified (action {:?})",
            record.id
        ));
    }
    let topic = param_string(record, "pulsar_topic").ok_or_else(|| {
        format!(
            "action {:?} is missing required field: pulsar_topic (field `parameters.pulsar_topic`)",
            record.id
        )
    })?;
    let strategy = param_string(record, "strategy").unwrap_or_else(|| "random".to_string());
    // `key_dispatch` hashes the key. `random` and `roundrobin` send
    // with no key so the broker spreads rows.
    let partition_key_template = if strategy == "key_dispatch" {
        param_message_field(record, "key")
    } else {
        None
    };
    Ok(PulsarSinkConfig {
        service_url,
        tenant: nested_field(connector_doc, &["tenant"])
            .map(|value| value.into_owned())
            .unwrap_or_else(|| "public".to_string()),
        namespace: nested_field(connector_doc, &["namespace"])
            .map(|value| value.into_owned())
            .unwrap_or_else(|| "default".to_string()),
        topic,
        auth: PulsarAuth::None,
        partition_key_template,
        strategy,
        properties: std::collections::HashMap::new(),
        batch_size: Some(MV03A_BATCH_SIZE_ONE),
        batch_bytes: Some(MV03A_PULSAR_BATCH_BYTES),
        linger_ms: Some(MV03A_LINGER_MS),
        max_retries: Some(MV03A_MAX_RETRIES),
        initial_backoff_ms: Some(MV03A_BACKOFF_FIRST_MS),
        max_backoff_ms: Some(MV03A_BACKOFF_MAX_MS),
        timeout_ms: connector_timeout_ms(connector_doc),
    })
}

/// Build the action's `pulsar` sink from its config. The config
/// carries the applied defaults; the transport and sink add no new
/// values.
pub(crate) fn build_pulsar_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::pulsar::{DriverPulsarTransport, PulsarSink};
    let config = pulsar_sink_config(record, connector_doc)?;
    // Production write path on the maintained `pulsar` driver; the
    // hand-written TCP framing stays for offline unit tests only.
    let transport = DriverPulsarTransport::new(&config).map_err(|error| {
        format!(
            "action {:?} cannot build its pulsar transport: {error}",
            record.id
        )
    })?;
    let sink = PulsarSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its pulsar sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

/// Build the action's `tablestore` sink. The endpoint, instance and
/// access keys come from the connector; the table, fields, tags,
/// measurement and timestamp come from the action.
///
/// The action `tags` object becomes the primary-key columns, and
/// `fields` become the attribute columns. When `tags` is empty, the
/// `timestamp` parameter is the primary key.
pub(crate) fn build_tablestore_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
    _opts: &ActionResourceOpts,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::tablestore::{
        AttributeColumnMapping, AttributeColumnType, HttpTablestoreTransport, PrimaryKeyMapping,
        PrimaryKeyType, TablestoreSink, TablestoreSinkConfig,
    };
    let connector = record.connector.trim().to_string();
    let endpoint = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["endpoint", "url"],
        "endpoint",
    )?;
    let instance_name = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["instance_name", "instance"],
        "instance_name",
    )?;
    let access_key_id = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["access_key_id", "ak"],
        "access_key_id",
    )?;
    let access_key_secret = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["access_key_secret", "sk"],
        "access_key_secret",
    )?;
    let table_name = param_string(record, "table_name").ok_or_else(|| {
        format!(
            "action {:?} is missing required field: table_name",
            record.id
        )
    })?;
    // The accepted `measurement` value travels into the sink config
    // and is stored on every row, so it is applied, not ignored.
    let measurement = param_string(record, "measurement").ok_or_else(|| {
        format!(
            "action {:?} is missing required field: measurement",
            record.id
        )
    })?;
    let tags = param_string_map(record, "tags");
    let mut primary_keys = Vec::new();
    for (name, source) in &tags {
        primary_keys.push(PrimaryKeyMapping {
            name: name.clone(),
            source: source.clone(),
            data_type: PrimaryKeyType::String,
        });
    }
    if primary_keys.is_empty() {
        if let Some(timestamp) = param_string(record, "timestamp") {
            if timestamp.starts_with("${") {
                // The `${timestamp}` source resolves to millis as a number.
                // Type it as integer so the strict string check still holds.
                primary_keys.push(PrimaryKeyMapping {
                    name: "timestamp".to_string(),
                    source: timestamp,
                    data_type: PrimaryKeyType::Integer,
                });
            }
        }
    }
    if primary_keys.is_empty() {
        return Err(format!(
            "action {:?} needs a primary key: set `parameters.tags` or a `${{...}}` `parameters.timestamp`",
            record.id
        ));
    }
    let mut attribute_columns = Vec::new();
    let fields = param_value(record, "fields")
        .and_then(|value| value.as_array())
        .ok_or_else(|| format!("action {:?} is missing required field: fields", record.id))?;
    for field in fields {
        let column = field
            .get("column")
            .and_then(|value| value.as_str())
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                format!(
                    "action {:?} field `parameters.fields` entry is missing column",
                    record.id
                )
            })?;
        let source = field
            .get("value")
            .and_then(|value| value.as_str())
            .ok_or_else(|| {
                format!(
                    "action {:?} field `parameters.fields` entry {column:?} is missing a string value template",
                    record.id
                )
            })?;
        let is_binary = field
            .get("isbinary")
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        let is_int = field
            .get("isint")
            .and_then(|value| value.as_bool())
            .unwrap_or(false);
        let data_type = if is_binary {
            AttributeColumnType::Binary
        } else if is_int {
            AttributeColumnType::Integer
        } else {
            AttributeColumnType::String
        };
        attribute_columns.push(AttributeColumnMapping {
            name: column.to_string(),
            source: source.to_string(),
            data_type,
        });
    }
    let config = TablestoreSinkConfig {
        endpoint,
        instance_name,
        table_name,
        access_key_id,
        access_key_secret,
        primary_keys,
        attribute_columns,
        measurement,
        // One row per send with no linger: the action runtime owns
        // batching (RO-01). Keep 10_000 rows buffered (fifty 200-row
        // flushes) to bound memory.
        batch_size: Some(1),
        buffer_capacity: Some(10_000),
        linger_ms: Some(0),
        timeout_ms: connector_timeout_ms(connector_doc),
    };
    let client = reqwest::Client::builder()
        .timeout(config.timeout())
        .build()
        .unwrap_or_default();
    let transport = HttpTablestoreTransport::new(&config, client).map_err(|error| {
        format!(
            "action {:?} cannot build its tablestore transport: {error}",
            record.id
        )
    })?;
    let sink = TablestoreSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its tablestore sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

/// Build the action's `kinesis` sink. Connection (endpoint, region,
/// keys) comes from the connector; the stream and key template come
/// from the action.
///
/// Values and defaults:
/// - `endpoint`: the connector endpoint (reference `endpoint`);
///   a loopback `http://127.0.0.1:port` endpoint selects the HTTP
///   transport used by offline tests.
/// - `region`: the connector region, default `us-east-1`.
/// - `stream_name`: the action target, required, no invented default.
/// - `partition_key`: the action key template; absent means the
///   driver hashes the topic.
/// - Batching belongs to the action runtime, not this driver. The
///   inner sink keeps one row per send with no linger (RO-01).
#[cfg(feature = "kinesis")]
pub(crate) fn build_kinesis_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::kinesis::{
        KinesisSink, KinesisSinkConfig, SdkKinesisTransport,
    };
    let connector = record.connector.trim().to_string();
    let endpoint =
        first_string(connector_doc, &["endpoint", "url", "server"]).map(|value| value.into_owned());
    let region = first_string(connector_doc, &["region"])
        .map(|value| value.into_owned())
        .unwrap_or_else(|| "us-east-1".to_string());
    let access_key_id = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["aws_access_key_id", "access_key_id", "ak"],
        "aws_access_key_id",
    )?;
    let secret_access_key = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["aws_secret_access_key", "secret_access_key", "sk"],
        "aws_secret_access_key",
    )?;
    // The stream and the partition key belong to the action.
    refuse_connector_fields(
        connector_doc,
        &connector,
        &record.id,
        &["stream_name", "partition_key_template", "explicit_hash_key"],
    )?;
    let stream_name = param_string(record, "stream_name").ok_or_else(|| {
        format!(
            "action {:?} is missing required field: stream_name",
            record.id
        )
    })?;
    let partition_key_template = param_string(record, "partition_key").ok_or_else(|| {
        format!(
            "action {:?} is missing required field: partition_key",
            record.id
        )
    })?;
    let config = KinesisSinkConfig {
        stream_name,
        region,
        endpoint,
        access_key_id,
        secret_access_key,
        session_token: first_string(connector_doc, &["session_token", "token"])
            .map(|value| value.into_owned()),
        partition_key_template: Some(partition_key_template),
        explicit_hash_key: None,
        // One row per send with no linger: the action runtime owns
        // batching (RO-01). Retry 3 times with a 100 ms start.
        batch_size: Some(MV03B_KINESIS_BATCH_SIZE),
        batch_bytes: Some(MV03B_KINESIS_BATCH_BYTES),
        linger_ms: Some(MV03B_KINESIS_LINGER_MS),
        max_retries: Some(MV03B_KINESIS_MAX_RETRIES),
        initial_backoff_ms: Some(MV03B_KINESIS_BACKOFF_FIRST_MS),
        max_backoff_ms: Some(MV03B_KINESIS_BACKOFF_MAX_MS),
        timeout_ms: connector_timeout_ms(connector_doc),
    };
    let transport = SdkKinesisTransport::new(&config).map_err(|error| {
        format!(
            "action {:?} cannot build its kinesis transport: {error}",
            record.id
        )
    })?;
    let sink = KinesisSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its kinesis sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

/// Build the action's `gcp_pubsub` sink. The project and credentials
/// come from the connector; the topic and templates come from the
/// action.
///
/// Values and defaults:
/// - `project_id`: the connector project (aliases `gcp_project_id`,
///   `project`); required.
/// - `endpoint`: the connector override for the emulator; absent
///   means the public Pub/Sub endpoint.
/// - `access_token`: the connector bearer token; absent means no
///   `Authorization` header (emulator path).
/// - `pubsub_topic`: the action topic (aliases `topic`, `topic_id`);
///   required.
/// - Batching belongs to the action runtime (RO-01): one row per
///   send with no linger.
#[cfg(feature = "gcp_pubsub")]
pub(crate) fn build_gcp_pubsub_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::gcp_pubsub::{
        GcpPubSubSink, GcpPubSubSinkConfig, SdkGcpPubSubTransport,
    };
    let connector = record.connector.trim().to_string();
    // Use the top-level project when the operator gives it.
    // Else use the project inside the service account key.
    let top_project = first_string(connector_doc, &["project_id", "gcp_project_id", "project"])
        .map(|value| value.into_owned())
        .filter(|value| !value.trim().is_empty());
    let endpoint =
        first_string(connector_doc, &["endpoint", "url", "server"]).map(|value| value.into_owned());
    // A present but bad key is refused. An absent key keeps the
    // anonymous identity; only a present key must prove itself.
    let has_key = connector_doc
        .get("authentication")
        .or_else(|| connector_doc.get("auth"))
        .is_some()
        || connector_doc
            .get("service_account_json")
            .and_then(|value| value.as_str())
            .is_some_and(|value| !value.trim().is_empty());
    let (auth_project_id, auth) = if has_key {
        bigquery_auth(connector_doc, &connector, &record.id)?
    } else {
        (
            String::new(),
            broker_connectors_enterprise::gcp_pubsub::GcpAuth::None,
        )
    };
    let project_id = top_project.unwrap_or(auth_project_id);
    if project_id.trim().is_empty() {
        return Err(format!(
            "connector {connector:?} is missing required field: project_id (action {:?})",
            record.id
        ));
    }
    let topic_id = param_string(record, "pubsub_topic")
        .or_else(|| param_string(record, "topic"))
        .or_else(|| param_string(record, "topic_id"))
        .ok_or_else(|| {
            format!(
                "action {:?} is missing required field: pubsub_topic",
                record.id
            )
        })?;
    let config = GcpPubSubSinkConfig {
        project_id,
        topic_id,
        endpoint,
        auth,
        ordering_key_template: param_string(record, "ordering_key_template")
            .or_else(|| param_string(record, "ordering_key")),
        attributes: param_string_map(record, "attributes"),
        // One row per send with no linger: the action runtime owns
        // batching (RO-01).
        batch_size: Some(MV03B_GCP_BATCH_SIZE),
        batch_bytes: Some(MV03B_GCP_BATCH_BYTES),
        linger_ms: Some(MV03B_GCP_LINGER_MS),
        max_retries: Some(MV03B_GCP_MAX_RETRIES),
        initial_backoff_ms: Some(MV03B_GCP_BACKOFF_FIRST_MS),
        max_backoff_ms: Some(MV03B_GCP_BACKOFF_MAX_MS),
        timeout_ms: connector_timeout_ms(connector_doc),
    };
    let transport = SdkGcpPubSubTransport::new(&config).map_err(|error| {
        format!(
            "action {:?} cannot build its gcp_pubsub transport: {error}",
            record.id
        )
    })?;
    let sink = GcpPubSubSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its gcp_pubsub sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

/// Build the action's `azure_eventhubs` sink. The namespace and SAS
/// pair come from the connector; the hub and key template come from
/// the action.
///
/// Values and defaults:
/// - `namespace`: the connector namespace; required.
/// - `endpoint`: the connector override; absent means
///   `{namespace}.servicebus.windows.net`.
/// - `shared_access_key_name` / `shared_access_key`: the connector
///   SAS pair; both required.
/// - `event_hub`: the action hub (aliases `hub`, `topic`); required.
/// - Batching belongs to the action runtime (RO-01): one row per
///   send with no linger.
#[cfg(feature = "azure_eventhubs")]
pub(crate) fn build_azure_eventhubs_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::azure_eventhubs::{
        AzureEventHubsSink, AzureEventHubsSinkConfig, HttpAzureEventHubsTransport,
    };
    let connector = record.connector.trim().to_string();
    let namespace = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["namespace"],
        "namespace",
    )?;
    let shared_access_key_name = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["shared_access_key_name", "key_name", "policy"],
        "shared_access_key_name",
    )?;
    let shared_access_key = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["shared_access_key", "key"],
        "shared_access_key",
    )?;
    let event_hub = param_string(record, "event_hub")
        .or_else(|| param_string(record, "hub"))
        .or_else(|| param_string(record, "topic"))
        .ok_or_else(|| {
            format!(
                "action {:?} is missing required field: event_hub",
                record.id
            )
        })?;
    let config = AzureEventHubsSinkConfig {
        namespace,
        event_hub,
        endpoint: first_string(connector_doc, &["endpoint", "url", "server"])
            .map(|value| value.into_owned()),
        shared_access_key_name,
        shared_access_key,
        partition_key_template: param_string(record, "partition_key")
            .or_else(|| param_string(record, "partition_key_template")),
        user_properties: param_string_map(record, "user_properties"),
        token_ttl_secs: MV03B_EVENTHUBS_TOKEN_TTL_SECS,
        // One row per send with no linger: the action runtime owns
        // batching (RO-01).
        batch_size: Some(MV03B_EVENTHUBS_BATCH_SIZE),
        batch_bytes: Some(MV03B_EVENTHUBS_BATCH_BYTES),
        linger_ms: Some(MV03B_EVENTHUBS_LINGER_MS),
        max_retries: Some(MV03B_EVENTHUBS_MAX_RETRIES),
        initial_backoff_ms: Some(MV03B_EVENTHUBS_BACKOFF_FIRST_MS),
        max_backoff_ms: Some(MV03B_EVENTHUBS_BACKOFF_MAX_MS),
        timeout_ms: connector_timeout_ms(connector_doc),
    };
    let transport =
        HttpAzureEventHubsTransport::new(&config, reqwest::Client::new()).map_err(|error| {
            format!(
                "action {:?} cannot build its azure_eventhubs transport: {error}",
                record.id
            )
        })?;
    let sink = AzureEventHubsSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its azure_eventhubs sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

/// Build the action's `aws_iot` sink. The endpoint and mTLS material
/// come from the connector; the remote topic comes from the action.
///
/// Values and defaults:
/// - `endpoint`: the connector host (`host:port`); required.
/// - `client_cert_pem` / `client_key_pem`: the connector mTLS pair;
///   both required. SigV4 has no transport and is refused.
/// - `remote_topic`: the action topic (alias `topic`); required.
/// - The bridge maps every local topic (`#`) to the remote topic.
/// - Batching belongs to the action runtime (RO-01): one row per
///   send with a short linger and a finite buffer.
#[cfg(feature = "aws_iot")]
pub(crate) fn build_aws_iot_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::aws_iot::{
        AwsIotAuth, AwsIotConfig, AwsIotSink, BridgeDirection, BridgeTopicMapping,
        TlsAwsIotTransport,
    };
    let connector = record.connector.trim().to_string();
    let endpoint = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["endpoint", "url", "server"],
        "endpoint",
    )?;
    let region = first_string(connector_doc, &["region"])
        .map(|value| value.into_owned())
        .unwrap_or_else(|| "us-east-1".to_string());
    let client_id = first_string(connector_doc, &["client_id", "client"])
        .map(|value| value.into_owned())
        .unwrap_or_else(|| "indra-bridge".to_string());
    // Flat fields first, then the nested `auth` object used by v1.
    let nested = connector_doc
        .get("auth")
        .and_then(|value| value.as_object());
    let field = |keys: &[&str]| -> Option<String> {
        if let Some(value) = first_string(connector_doc, keys) {
            return Some(value.into_owned());
        }
        nested.and_then(|auth| {
            keys.iter().find_map(|key| {
                auth.get(*key)
                    .and_then(|value| value.as_str().map(str::trim))
                    .filter(|text| !text.is_empty())
                    .map(str::to_string)
            })
        })
    };
    let client_cert_pem = field(&["client_cert_pem", "client_cert", "certificate", "cert_pem"])
        .ok_or_else(|| {
            format!(
                "connector {connector:?} is missing required field: client_cert_pem (action {:?})",
                record.id
            )
        })?;
    let client_key_pem = field(&["client_key_pem", "client_key", "private_key", "key_pem"])
        .ok_or_else(|| {
            format!(
                "connector {connector:?} is missing required field: client_key_pem (action {:?})",
                record.id
            )
        })?;
    let remote_topic = param_string(record, "remote_topic")
        .or_else(|| param_string(record, "topic"))
        .ok_or_else(|| {
            format!(
                "action {:?} is missing required field: remote_topic",
                record.id
            )
        })?;
    // Read the CA from the connector. The bundle is a valid source.
    let ca_cert_pem =
        field(&["ca_cert_pem", "ca_cert", "ca", "ca_bundle_pem", "ca_bundle"]).unwrap_or_default();
    let config = AwsIotConfig {
        endpoint,
        region,
        client_id,
        auth: AwsIotAuth::Mtls {
            ca_cert_pem,
            client_cert_pem,
            client_key_pem,
        },
        topic_mappings: vec![BridgeTopicMapping {
            local_topic: "#".to_string(),
            remote_topic,
            direction: BridgeDirection::LocalToRemote,
        }],
        shadow_sync: None,
        batch_size: Some(MV03B_AWS_IOT_BATCH_SIZE),
        buffer_capacity: Some(MV03B_AWS_IOT_BUFFER_CAPACITY),
        linger_ms: Some(MV03B_AWS_IOT_LINGER_MS),
        max_retries: Some(MV03B_AWS_IOT_MAX_RETRIES),
        timeout_ms: connector_timeout_ms(connector_doc),
        connect_timeout_ms: None,
        handshake_timeout_ms: None,
        ca_bundle_pem: first_string(connector_doc, &["ca_bundle_pem", "ca_bundle"])
            .map(|value| value.into_owned()),
        alpn_protocols: None,
    };
    let transport = TlsAwsIotTransport::new(&config).map_err(|error| {
        format!(
            "action {:?} cannot build its aws_iot transport: {error}",
            record.id
        )
    })?;
    let sink = AwsIotSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its aws_iot sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

/// Build the action's `azure_iot` sink. The hub, device and SAS key
/// come from the connector; the action carries no target (the hub
/// topic is fixed by the device).
///
/// Values and defaults:
/// - `iot_hub_name`: the connector hub (`host[:port]`); required.
/// - `device_id`: the connector device; required.
/// - `shared_access_key`: the connector SAS key; required.
/// - Batching belongs to the action runtime (RO-01): one row per
///   send with no linger.
#[cfg(feature = "azure_iot")]
pub(crate) fn build_azure_iot_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::azure_iot::{
        AzureIotAuth, AzureIotConfig, AzureIotSink, TlsAzureIotTransport,
    };
    let connector = record.connector.trim().to_string();
    let iot_hub_name = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["iot_hub_name", "hub_name", "server", "endpoint"],
        "iot_hub_name",
    )?;
    let device_id = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["device_id", "device"],
        "device_id",
    )?;
    let shared_access_key = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["shared_access_key", "key"],
        "shared_access_key",
    )?;
    let config = AzureIotConfig {
        iot_hub_name,
        device_id,
        module_id: first_string(connector_doc, &["module_id", "module"])
            .map(|value| value.into_owned()),
        auth: AzureIotAuth::SharedAccessKey {
            key: shared_access_key,
            key_name: first_string(connector_doc, &["key_name", "shared_access_key_name"])
                .map(|value| value.into_owned()),
        },
        api_version: first_string(connector_doc, &["api_version"])
            .map(|value| value.into_owned())
            .unwrap_or_else(|| "2021-04-12".to_string()),
        // The bridge sends telemetry only. It subscribes to nothing.
        direct_methods_enabled: false,
        twin_sync_enabled: false,
        batch_size: Some(MV03B_AZURE_IOT_BATCH_SIZE),
        buffer_capacity: Some(MV03B_AZURE_IOT_BUFFER_CAPACITY),
        linger_ms: Some(MV03B_AZURE_IOT_LINGER_MS),
        max_retries: Some(MV03B_AZURE_IOT_MAX_RETRIES),
        timeout_ms: connector_timeout_ms(connector_doc),
        connect_timeout_ms: None,
        handshake_timeout_ms: None,
        ca_bundle_pem: first_string(connector_doc, &["ca_bundle_pem", "ca_bundle"])
            .map(|value| value.into_owned()),
        sas_ttl_secs: Some(MV03B_AZURE_IOT_SAS_TTL_SECS),
    };
    // The action suffix is validated in `required_action_parameters`.
    // It rides in the driver config, so each update rebuilds the
    // sink with the new suffix.
    let transport = TlsAzureIotTransport::new(&config).map_err(|error| {
        format!(
            "action {:?} cannot build its azure_iot transport: {error}",
            record.id
        )
    })?;
    let sink = AzureIotSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its azure_iot sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

/// Build the action's `oci_streaming` sink. The endpoint and OCI
/// identity come from the connector; the stream comes from the
/// action.
///
/// Values and defaults:
/// - `endpoint`, `stream_pool_id`, `stream_id`, `tenancy_ocid`,
///   `user_ocid`, `fingerprint`, `private_key_pem`: the connector
///   identity; all required.
/// - `stream_id`: the action may override the connector stream;
///   the connector value is the default.
/// - `partition_key_template`: the action key template; default
///   `${client_id}`.
/// - Batching belongs to the action runtime (RO-01): one row per
///   send with no linger and a finite buffer.
pub(crate) fn build_oci_streaming_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::oci_streaming::{
        HttpOciStreamingTransport, OciStreamingSink, OciStreamingSinkConfig,
    };
    let connector = record.connector.trim().to_string();
    let endpoint = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["endpoint", "url", "server"],
        "endpoint",
    )?;
    let stream_pool_id = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["stream_pool_id", "pool_id", "pool"],
        "stream_pool_id",
    )?;
    let stream_id = param_string(record, "stream_id").unwrap_or_default();
    if stream_id.trim().is_empty() {
        return Err(format!(
            "action {:?} is missing required field: stream_id",
            record.id
        ));
    }
    let tenancy_ocid = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["tenancy_ocid", "tenancy"],
        "tenancy_ocid",
    )?;
    let user_ocid = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["user_ocid", "user"],
        "user_ocid",
    )?;
    let fingerprint = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["fingerprint"],
        "fingerprint",
    )?;
    let private_key_pem = connector_field(
        connector_doc,
        &connector,
        &record.id,
        &["private_key_pem", "private_key"],
        "private_key_pem",
    )?;
    let config = OciStreamingSinkConfig {
        endpoint,
        stream_pool_id,
        stream_id,
        tenancy_ocid,
        user_ocid,
        fingerprint,
        private_key_pem,
        partition_key_template: param_string(record, "partition_key")
            .or_else(|| param_string(record, "partition_key_template"))
            .unwrap_or_else(|| "${client_id}".to_string()),
        // One row per send with no linger: the action runtime owns
        // batching (RO-01). The buffer stays finite (default 10_000
        // rows in the driver).
        batch_size: Some(MV03B_OCI_BATCH_SIZE),
        buffer_capacity: Some(MV03B_OCI_BUFFER_CAPACITY),
        batch_bytes: Some(MV03B_OCI_BATCH_BYTES),
        linger_ms: Some(MV03B_OCI_LINGER_MS),
        max_retries: Some(MV03B_OCI_MAX_RETRIES),
        initial_backoff_ms: Some(MV03B_OCI_BACKOFF_FIRST_MS),
        max_backoff_ms: Some(MV03B_OCI_BACKOFF_MAX_MS),
        timeout_ms: connector_timeout_ms(connector_doc),
    };
    let client = reqwest::Client::builder()
        .timeout(config.timeout())
        .build()
        .unwrap_or_default();
    let transport = HttpOciStreamingTransport::new(&config, client).map_err(|error| {
        format!(
            "action {:?} cannot build its oci_streaming transport: {error}",
            record.id
        )
    })?;
    let sink = OciStreamingSink::new(config, Arc::new(transport)).map_err(|error| {
        format!(
            "action {:?} cannot build its oci_streaming sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

/// Build the action's `rocketmq` sink: the proxy endpoints and ACL
/// keys come from the connector; the topic, tag and keys come from
/// the action.
///
/// Values and defaults:
/// - `servers` (alias `endpoints`): the connector's proxy endpoints;
///   required, no invented default.
/// - `access_key`/`secret_key`: the connector ACL pair; both or
///   neither (a half pair is refused, it cannot sign).
/// - `ssl`: refused when present: the proxy dial is plaintext.
/// - `topic`: the action topic; default `TopicTest`, the reference
///   action default.
/// - `tag`, `keys`: the action templates (`${topic}`,
///   `${client_id}`, `${message_id}`); absent means no tag or keys.
/// - `strategy`: `roundrobin` (default, the reference default) or
///   `key_dispatch`.
/// - `template`: only the empty template is supported; it stores the
///   whole message.
/// - The action runtime owns batching: one row per send with a short
///   linger and a bounded buffer.
pub(crate) fn rocketmq_sink_config(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<broker_connectors_enterprise::rocketmq::RocketMqSinkConfig, String> {
    use broker_connectors_enterprise::rocketmq::RocketMqSinkConfig;
    let connector = record.connector.trim().to_string();
    validate_connector_io_bounds(
        connector_doc,
        &connector,
        &format!("action {:?}", record.id),
    )?;
    let servers = required_nested_field(
        connector_doc,
        &connector,
        &record.id,
        &["servers", "endpoints", "endpoint", "server"],
        "servers",
    )?;
    let endpoints: Vec<String> = servers
        .split(',')
        .map(|part| {
            part.trim()
                .trim_start_matches("http://")
                .trim_start_matches("tcp://")
                .to_string()
        })
        .filter(|part| !part.is_empty())
        .collect();
    if endpoints.is_empty() {
        return Err(format!(
            "connector {connector:?} is missing required field: servers (action {:?})",
            record.id
        ));
    }
    let access_key = nested_field(connector_doc, &["access_key"]).map(|v| v.into_owned());
    let secret_key = nested_field(connector_doc, &["secret_key"]).map(|v| v.into_owned());
    match (&access_key, &secret_key) {
        (Some(_), Some(_)) | (None, None) => {}
        _ => {
            return Err(format!(
                "connector {connector:?} fields `access_key` and `secret_key` must be set together (action {:?})",
                record.id
            ));
        }
    }
    let access_key = access_key.filter(|key| !key.trim().is_empty());
    let secret_key = secret_key.filter(|key| !key.trim().is_empty());
    if access_key.is_some() != secret_key.is_some() {
        return Err(format!(
            "connector {connector:?} fields `access_key` and `secret_key` must be set together (action {:?})",
            record.id
        ));
    }
    let security_token = nested_field(connector_doc, &["security_token", "securityToken"])
        .map(|value| value.into_owned());
    let strategy = param_string(record, "strategy").unwrap_or_else(|| "roundrobin".to_string());
    // `key_dispatch` hashes the keys. `roundrobin` sends with no keys
    // so the proxy spreads rows.
    let keys_template = if strategy == "key_dispatch" {
        param_string(record, "key")
    } else {
        None
    };
    Ok(RocketMqSinkConfig {
        endpoints,
        topic: param_string(record, "topic").unwrap_or_else(|| "TopicTest".to_string()),
        tag_template: param_string(record, "tag"),
        keys_template,
        strategy,
        message_group_template: None,
        access_key,
        secret_key,
        security_token,
        batch_size: Some(MV03A_BATCH_SIZE_ONE),
        // Bounded inner buffer: 1024 rows cap the memory the proxy
        // client holds while the runtime owns batching (RO-01).
        buffer_capacity: Some(MV03A_PROXY_BUFFER_ROWS),
        linger_ms: Some(MV03A_LINGER_MS),
        timeout_ms: connector_timeout_ms(connector_doc),
    })
}

/// Build the action's `rocketmq` sink from its config. The config
/// carries the applied defaults; the transport and sink add no new
/// values.
pub(crate) fn build_rocketmq_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    use broker_connectors_enterprise::rocketmq::{
        RemotingRocketMqTransport, RocketMqSink, RocketMqTransport, TcpRocketMqTransport,
    };
    let config = rocketmq_sink_config(record, connector_doc)?;
    // The proxy ignores the session token. Use the vendor transport
    // when a token is set so the broker applies the setting.
    let transport: Arc<dyn RocketMqTransport> = if config.security_token.is_some() {
        Arc::new(RemotingRocketMqTransport::new(&config).map_err(|error| {
            format!(
                "action {:?} cannot build its rocketmq transport: {error}",
                record.id
            )
        })?)
    } else {
        Arc::new(TcpRocketMqTransport::new(&config).map_err(|error| {
            format!(
                "action {:?} cannot build its rocketmq transport: {error}",
                record.id
            )
        })?)
    };
    let sink = RocketMqSink::new(config, transport).map_err(|error| {
        format!(
            "action {:?} cannot build its rocketmq sink: {error}",
            record.id
        )
    })?;
    Ok(Arc::new(sink))
}

#[cfg(not(feature = "kinesis"))]
pub(crate) fn build_kinesis_sink(
    record: &ActionRecord,
    _connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    Err(format!(
        "action {:?} needs cargo feature `kinesis` (disabled in this build)",
        record.id
    ))
}

#[cfg(not(feature = "gcp_pubsub"))]
pub(crate) fn build_gcp_pubsub_sink(
    record: &ActionRecord,
    _connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    Err(format!(
        "action {:?} needs cargo feature `gcp_pubsub` (disabled in this build)",
        record.id
    ))
}

#[cfg(not(feature = "azure_eventhubs"))]
pub(crate) fn build_azure_eventhubs_sink(
    record: &ActionRecord,
    _connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    Err(format!(
        "action {:?} needs cargo feature `azure_eventhubs` (disabled in this build)",
        record.id
    ))
}

#[cfg(not(feature = "aws_iot"))]
pub(crate) fn build_aws_iot_sink(
    record: &ActionRecord,
    _connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    Err(format!(
        "action {:?} needs cargo feature `aws_iot` (disabled in this build)",
        record.id
    ))
}

#[cfg(not(feature = "azure_iot"))]
pub(crate) fn build_azure_iot_sink(
    record: &ActionRecord,
    _connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    Err(format!(
        "action {:?} needs cargo feature `azure_iot` (disabled in this build)",
        record.id
    ))
}

/// Endpoint for one object-store connector: an explicit
/// `endpoint`/`url`/`server`, else `https://host[:port]`.
#[cfg(feature = "s3")]
fn object_endpoint(
    connector_doc: &serde_json::Value,
    connector: &str,
    action_id: &str,
    keys: &[&str],
) -> Result<String, String> {
    if let Some(raw) = first_string(connector_doc, keys) {
        let raw = raw.into_owned();
        if raw.starts_with("http://") || raw.starts_with("https://") {
            return Ok(raw.trim_end_matches('/').to_string());
        }
        if raw.contains(':') {
            return Ok(format!("http://{}", raw.trim_end_matches('/')));
        }
        return Ok(format!("https://{}", raw.trim_end_matches('/')));
    }
    let host = connector_field(connector_doc, connector, action_id, &["host"], "host")?;
    let port = connector_doc
        .get("port")
        .and_then(|value| value.as_u64())
        .unwrap_or(443);
    Ok(format!("https://{host}:{port}"))
}

/// Bring every stored action's owned runtime sink in step with the
/// current stores: register the freshly built sink under the action
/// dispatch key when the build succeeds, raise the build-failure
/// alarm and report `disconnected` with the reason when it does not,
/// and drop the key when the target connector is gone or the kind
/// owns no runtime. Boot replay, config reload and restore all use
/// this same replay; every rebuild starts from the stored inputs, so
/// nothing keeps an in-memory copy between writes.
///
/// A rebuild failure keeps any previously registered sink delivering
/// (the old generation; the same fail-open-then-report contract the
/// connector replay follows) and reports through the wiring state
/// plus one alarm — never an invented status value.
pub fn rebuild_all_action_sinks(
    engine: &broker_rules::RuleEngine,
    connectors: &ConnectorStore,
    actions: &ActionStore,
    alarms: &crate::v5::alarms::AlarmStore,
) -> Vec<Arc<dyn broker_connectors::Sink>> {
    rebuild_action_sinks(engine, connectors, actions, alarms, |_| true)
}

/// Rebuild only the actions that target one connector. Connector create
/// uses this instead of the full replay, so an unrelated action update
/// can never be overwritten by a stale generation.
pub(crate) fn rebuild_connector_action_sinks(
    engine: &broker_rules::RuleEngine,
    connectors: &ConnectorStore,
    actions: &ActionStore,
    alarms: &crate::v5::alarms::AlarmStore,
    bare: &str,
) -> Vec<Arc<dyn broker_connectors::Sink>> {
    rebuild_action_sinks(engine, connectors, actions, alarms, |record| {
        action_targets_connector(connectors, record, bare)
    })
}

/// Probe the registered runtime sinks of the selected actions. Boot,
/// reload, restore and write paths await this before they answer.
/// A down target then reads `disconnected` from a real attempt.
/// No action without a registered sink is dialled.
pub async fn probe_action_sinks(
    engine: &broker_rules::RuleEngine,
    actions: &ActionStore,
    include: impl Fn(&ActionRecord) -> bool,
) {
    for record in actions.snapshot() {
        if !include(&record) {
            continue;
        }
        let key = broker_config::v5_action_sink_key(&record.id);
        if let Some(sink) = engine.connectors().get(&key) {
            let _ = sink.probe().await;
        }
    }
}

fn rebuild_action_sinks(
    engine: &broker_rules::RuleEngine,
    connectors: &ConnectorStore,
    actions: &ActionStore,
    alarms: &crate::v5::alarms::AlarmStore,
    include: impl Fn(&ActionRecord) -> bool,
) -> Vec<Arc<dyn broker_connectors::Sink>> {
    let mut replaced = Vec::new();
    for record in actions.snapshot() {
        if !include(&record) {
            continue;
        }
        let key = broker_config::v5_action_sink_key(&record.id);
        let previous = engine.connectors().get(&key);
        // MV-01/R2: the dispatch flag is the AND of the action's own
        // enable and its connector's (a connector disable stops every
        // dependent action; a connector enable restores them). Mirrored
        // on every replay so a disable survives restart and reload.
        engine.set_action_enabled(
            &key,
            record.enable && action_connector_enable(connectors, &record),
        );
        if !action_owned_kind(&record.action_type) {
            // No runtime for this kind yet (or the kind changed): drop
            // a stale registration so the key stops claiming delivery.
            // The replaced generation is handed to the caller so it
            // drains the buffered rows to the old target.
            if previous.is_some() {
                engine.connectors().unregister(&key);
                replaced.extend(previous);
            }
            actions.set_wiring(&record.id, ActionWiring::None);
            alarms.deactivate(&format!("action_build_failed:{}", record.id));
            continue;
        }
        let stage = match stage_action_sink(connectors, &record) {
            Ok(stage) => stage,
            Err(message) => {
                // Keep the previous sink: a reload or restore that cannot
                // rebuild one action must not stop its delivery.
                set_failed_wiring(actions, alarms, &record, message);
                continue;
            }
        };
        match stage.wiring {
            ActionWiring::Live => {
                // Live wiring always carries its sink by construction.
                if let Some(sink) = stage.sink {
                    let sink = wrap_action_runtime(
                        engine,
                        &key,
                        sink,
                        stage.resource_opts,
                        stage.health_target,
                    );
                    // A console action shares its captured output with
                    // the connector read path. Register the same runtime
                    // under the bare connector name, as create does.
                    if canonical_action_kind(&record.action_type) == "console" {
                        let bare = record.connector.trim();
                        if !bare.is_empty() {
                            engine
                                .connectors()
                                .register(bare.to_string(), Arc::clone(&sink));
                        }
                    }
                    // Boot finds nothing here. A reload or restore swap
                    // hands the old generation to the caller for drain.
                    engine.connectors().register(key, sink);
                    replaced.extend(previous);
                }
                actions.set_wiring(&record.id, ActionWiring::Live);
                alarms.deactivate(&format!("action_build_failed:{}", record.id));
            }
            ActionWiring::Failed(message) => {
                // Keep the old generation delivering. The wiring and the
                // alarm report the failure.
                set_failed_wiring(actions, alarms, &record, message);
            }
            ActionWiring::None => {
                // No runtime for this kind (or the reference dangles
                // with no resolution): drop a stale registration so the
                // key stops claiming delivery. The replaced generation
                // is handed to the caller for drain.
                if previous.is_some() {
                    engine.connectors().unregister(&key);
                    replaced.extend(previous);
                }
                actions.set_wiring(&record.id, ActionWiring::None);
                alarms.deactivate(&format!("action_build_failed:{}", record.id));
            }
        }
    }
    replaced
}

/// Fail one action's wiring into the stored, alarm-raising, honest
/// `disconnected` state. A previously registered sink stays live (the
/// old generation), so a reload or restore never stops delivery because
/// one rebuild failed.
fn set_failed_wiring(
    actions: &ActionStore,
    alarms: &crate::v5::alarms::AlarmStore,
    record: &ActionRecord,
    message: String,
) {
    actions.set_wiring(&record.id, ActionWiring::Failed(message.clone()));
    alarms.activate(
        &format!("action_build_failed:{}", record.id),
        &message,
        serde_json::json!({
            "kind": "action",
            "id": record.id,
            "type": record.action_type,
        }),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: &str, action_type: &str, connector: &str) -> ActionRecord {
        ActionRecord {
            id: id.to_string(),
            name: id.to_string(),
            action_type: action_type.to_string(),
            enable: true,
            connector: connector.to_string(),
            parameters: serde_json::json!({}),
            resource_opts: serde_json::json!({}),
            params: serde_json::json!({}),
        }
    }

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

    fn mv02b_record(
        id: &str,
        action_type: &str,
        connector: &str,
        parameters: serde_json::Value,
    ) -> ActionRecord {
        ActionRecord {
            parameters,
            ..record(id, action_type, connector)
        }
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

    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

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
        assert!(
            connector_pool_size("act", "conn", &serde_json::json!({"pool_size": 1025})).is_err()
        );
        assert!(
            connector_pool_size("act", "conn", &serde_json::json!({"pool_size": "many"})).is_err()
        );
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
        let pg_doc =
            serde_json::json!({"server": "127.0.0.1:5432", "database": "db", "username": "u"});
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

    #[derive(Default)]
    struct BatchSink {
        batches: Mutex<Vec<usize>>,
        delay: Duration,
        active: AtomicUsize,
        max_active: AtomicUsize,
        fail: bool,
    }

    #[async_trait::async_trait]
    impl broker_connectors::Sink for BatchSink {
        async fn send(
            &self,
            topic: &Topic,
            payload: &Bytes,
            qos: QoS,
        ) -> broker_connectors::Result<()> {
            self.send_batch(&[SinkBatchItem {
                topic: topic.clone(),
                payload: payload.clone(),
                qos,
            }])
            .await
        }

        async fn send_batch(&self, items: &[SinkBatchItem]) -> broker_connectors::Result<()> {
            let active = self.active.fetch_add(1, AtomicOrdering::SeqCst) + 1;
            self.max_active.fetch_max(active, AtomicOrdering::SeqCst);
            self.batches
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(items.len());
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            self.active.fetch_sub(1, AtomicOrdering::SeqCst);
            if self.fail {
                return Err(broker_connectors::ConnectorError::Dispatch(
                    "planned failure".to_string(),
                ));
            }
            Ok(())
        }

        fn kind(&self) -> &'static str {
            "test-batch"
        }
    }

    fn runtime(
        inner: Arc<BatchSink>,
        opts: ActionResourceOpts,
    ) -> (ActionRuntimeSink, Arc<ActionMetrics>) {
        let metrics = Arc::new(ActionMetrics::new());
        (
            ActionRuntimeSink::new(inner, opts, Arc::clone(&metrics), None),
            metrics,
        )
    }

    #[test]
    fn resource_opts_accept_every_documented_default() {
        let raw = serde_json::json!({
            "batch_size": 1,
            "batch_time": "0ms",
            "dispatch_strategy": "per_clientid",
            "health_check_interval": "15s",
            "health_check_interval_jitter": "0ms",
            "health_check_timeout": "60s",
            "inflight_window": 100,
            "max_buffer_bytes": "256MB",
            "query_mode": "async",
            "request_ttl": "45s",
            "worker_pool_size": 16,
        });
        let opts = ActionResourceOpts::parse("act", &raw).expect("defaults parse");
        assert_eq!(opts, ActionResourceOpts::default());
    }

    #[test]
    fn resource_opts_refuse_a_non_default_value_this_runtime_cannot_apply() {
        let above_semaphore_max = (tokio::sync::Semaphore::MAX_PERMITS as u64) + 1;
        for (raw, field) in [
            (
                serde_json::json!({"dispatch_strategy": "random"}),
                "dispatch_strategy",
            ),
            (
                serde_json::json!({"health_check_interval_jitter": "1s"}),
                "health_check_interval_jitter",
            ),
            (serde_json::json!({"no_such_option": 1}), "no_such_option"),
            (serde_json::json!({"batch_time": "infinity"}), "batch_time"),
            (
                serde_json::json!({"max_buffer_bytes": "0"}),
                "max_buffer_bytes",
            ),
            (
                serde_json::json!({"inflight_window": above_semaphore_max}),
                "inflight_window",
            ),
        ] {
            let error = ActionResourceOpts::parse("act", &raw).expect_err("the value is refused");
            assert!(error.contains(field), "{error}");
            assert!(error.contains("act"), "{error}");
        }
    }

    #[test]
    fn resource_opts_apply_the_health_interval_and_the_legacy_batch_timeout() {
        let opts = ActionResourceOpts::parse(
            "act",
            &serde_json::json!({
                "health_check_interval": "30s",
                "batch_size": 10,
                "batch_timeout_ms": 25,
            }),
        )
        .expect("a non-default health interval and the legacy field parse");
        assert_eq!(opts.health_check_interval, Duration::from_secs(30));
        assert_eq!(opts.batch_time, Duration::from_millis(25));
        assert_eq!(opts.batch_size, 10);
    }

    #[test]
    fn resource_opts_accept_a_non_default_health_check_timeout() {
        let opts =
            ActionResourceOpts::parse("act", &serde_json::json!({"health_check_timeout": "30s"}))
                .expect("a non-default health check timeout is now accepted");
        assert_eq!(
            opts.health_check_timeout,
            Some(std::time::Duration::from_secs(30))
        );
    }

    #[tokio::test]
    async fn runtime_health_probe_marks_a_closed_target_unhealthy() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind target");
        let address = listener.local_addr().expect("target address");
        drop(listener);
        let inner = Arc::new(BatchSink::default());
        let opts = ActionResourceOpts {
            health_check_interval: Duration::from_millis(20),
            ..ActionResourceOpts::default()
        };
        let metrics = Arc::new(ActionMetrics::new());
        let runtime =
            ActionRuntimeSink::new(inner, opts, Arc::clone(&metrics), Some(address.to_string()));
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(health) = runtime.health() {
                if !health.healthy {
                    assert!(health.reason.is_some(), "the probe names the failure");
                    break;
                }
            }
            assert!(Instant::now() < deadline, "the probe marked the target");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn runtime_batches_by_size_and_gives_the_driver_one_batch() {
        let inner = Arc::new(BatchSink::default());
        let opts = ActionResourceOpts {
            batch_size: 3,
            batch_time: Duration::from_secs(1),
            ..ActionResourceOpts::default()
        };
        let (runtime, metrics) = runtime(Arc::clone(&inner), opts);
        let topic = Topic::new("t/act").expect("topic");
        let payload = Bytes::from_static(b"{}");
        for _ in 0..3 {
            runtime
                .send(&topic, &payload, QoS::AtMostOnce)
                .await
                .expect("send joins the buffer");
        }
        assert_eq!(
            inner
                .batches
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone(),
            vec![3]
        );
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.matched, 3);
        assert_eq!(snapshot.success, 3);
        assert_eq!(snapshot.failed, 0);
        assert_eq!(snapshot.queuing, 0);
    }

    #[tokio::test]
    async fn runtime_flushes_a_partial_batch_when_the_batch_time_elapses() {
        let inner = Arc::new(BatchSink::default());
        let opts = ActionResourceOpts {
            batch_size: 100,
            batch_time: Duration::from_millis(20),
            ..ActionResourceOpts::default()
        };
        let (runtime, metrics) = runtime(Arc::clone(&inner), opts);
        let topic = Topic::new("t/time").expect("topic");
        let payload = Bytes::from_static(b"{}");
        for _ in 0..2 {
            runtime
                .send(&topic, &payload, QoS::AtMostOnce)
                .await
                .expect("send joins the buffer");
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while inner
            .batches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty()
            && Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            inner
                .batches
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone(),
            vec![2]
        );
        assert_eq!(metrics.snapshot().success, 2);
    }

    #[tokio::test]
    async fn runtime_bounds_the_buffer_and_counts_queue_full_drops() {
        // One row holds 1 topic byte plus 10 payload bytes plus the
        // 64-byte per-row overhead, 75 bytes. A 150-byte bound holds two.
        let inner = Arc::new(BatchSink::default());
        let opts = ActionResourceOpts {
            batch_size: 100,
            batch_time: Duration::from_secs(10),
            max_buffer_bytes: 150,
            ..ActionResourceOpts::default()
        };
        let (runtime, metrics) = runtime(Arc::clone(&inner), opts);
        let topic = Topic::new("t").expect("topic");
        let payload = Bytes::from_static(b"0123456789");
        for _ in 0..3 {
            runtime
                .send(&topic, &payload, QoS::AtMostOnce)
                .await
                .expect("an overflow drop is not an error");
        }
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.matched, 3);
        assert_eq!(snapshot.dropped, 1);
        assert_eq!(snapshot.dropped_queue_full, 1);
        assert_eq!(snapshot.queuing, 2);
        assert!(inner
            .batches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty());
        // A row larger than the bound alone is refused. The buffer keeps
        // its two rows.
        let big = Bytes::from(vec![b'x'; 200]);
        runtime
            .send(&topic, &big, QoS::AtMostOnce)
            .await
            .expect("an oversized drop is not an error");
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.matched, 4);
        assert_eq!(snapshot.dropped, 2);
        assert_eq!(snapshot.dropped_queue_full, 2);
        assert_eq!(snapshot.queuing, 2);
    }

    #[tokio::test]
    async fn runtime_batch_size_applies_only_when_batch_time_is_positive() {
        // A zero batch time sends each row at once: two rows give two
        // driver calls even with `batch_size` 2.
        let inner = Arc::new(BatchSink::default());
        let opts = ActionResourceOpts {
            batch_size: 2,
            batch_time: Duration::ZERO,
            ..ActionResourceOpts::default()
        };
        let (idle_runtime, _) = runtime(Arc::clone(&inner), opts);
        let topic = Topic::new("t/size").expect("topic");
        let payload = Bytes::from_static(b"{}");
        for _ in 0..2 {
            idle_runtime
                .send(&topic, &payload, QoS::AtMostOnce)
                .await
                .expect("send joins the buffer");
        }
        assert_eq!(
            inner
                .batches
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone(),
            vec![1, 1]
        );
        // A positive batch time groups the rows: two rows give one call.
        let inner = Arc::new(BatchSink::default());
        let opts = ActionResourceOpts {
            batch_size: 2,
            batch_time: Duration::from_millis(200),
            ..ActionResourceOpts::default()
        };
        let (runtime, metrics) = runtime(Arc::clone(&inner), opts);
        for _ in 0..2 {
            runtime
                .send(&topic, &payload, QoS::AtMostOnce)
                .await
                .expect("send joins the buffer");
        }
        assert_eq!(
            inner
                .batches
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone(),
            vec![2]
        );
        assert_eq!(metrics.snapshot().success, 2);
    }

    #[tokio::test]
    async fn runtime_drops_a_row_older_than_request_ttl() {
        // The TTL is smaller than the batch time. One row waits past its
        // TTL, so the send drops it and counts the drop.
        let inner = Arc::new(BatchSink::default());
        let opts = ActionResourceOpts {
            batch_size: 2,
            batch_time: Duration::from_millis(200),
            request_ttl: Some(Duration::from_millis(50)),
            ..ActionResourceOpts::default()
        };
        let (runtime, metrics) = runtime(Arc::clone(&inner), opts);
        let topic = Topic::new("t/ttl").expect("topic");
        let payload = Bytes::from_static(b"{}");
        runtime
            .send(&topic, &payload, QoS::AtMostOnce)
            .await
            .expect("send joins the buffer");
        let deadline = Instant::now() + Duration::from_secs(3);
        while metrics.snapshot().dropped == 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.dropped, 1);
        assert_eq!(snapshot.dropped_other, 1);
        assert_eq!(snapshot.success, 0);
        assert!(inner
            .batches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty());
    }

    #[tokio::test]
    async fn runtime_runs_inflight_driver_calls_together() {
        // Ten rows with a 200 ms driver each finish in less than 1 s only
        // when the driver calls run together.
        let inner = Arc::new(BatchSink {
            delay: Duration::from_millis(200),
            ..BatchSink::default()
        });
        let opts = ActionResourceOpts {
            batch_size: 1,
            batch_time: Duration::ZERO,
            inflight_window: 10,
            worker_pool_size: 1,
            ..ActionResourceOpts::default()
        };
        let (runtime, metrics) = runtime(Arc::clone(&inner), opts);
        let runtime = Arc::new(runtime);
        let topic = Topic::new("t/join").expect("topic");
        let payload = Bytes::from_static(b"{}");
        let start = Instant::now();
        let mut tasks = Vec::new();
        for _ in 0..10 {
            let runtime = Arc::clone(&runtime);
            let topic = topic.clone();
            let payload = payload.clone();
            tasks.push(tokio::spawn(async move {
                runtime.send(&topic, &payload, QoS::AtMostOnce).await
            }));
        }
        for task in tasks {
            task.await
                .expect("send task runs")
                .expect("send joins the buffer");
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(1),
            "ten slow calls ran together: {elapsed:?}"
        );
        assert!(
            inner.max_active.load(AtomicOrdering::SeqCst) > 1,
            "more than one driver call ran at once"
        );
        assert_eq!(metrics.snapshot().success, 10);
    }

    #[tokio::test]
    async fn runtime_drop_counts_waiting_rows_as_dropped() {
        // Two rows wait in the buffer. Dropping the runtime counts both
        // rows as dropped instead of losing them silently.
        let inner = Arc::new(BatchSink::default());
        let opts = ActionResourceOpts {
            batch_size: 100,
            batch_time: Duration::from_secs(10),
            ..ActionResourceOpts::default()
        };
        let (runtime, metrics) = runtime(Arc::clone(&inner), opts);
        let topic = Topic::new("t/gone").expect("topic");
        let payload = Bytes::from_static(b"{}");
        for _ in 0..2 {
            runtime
                .send(&topic, &payload, QoS::AtMostOnce)
                .await
                .expect("send joins the buffer");
        }
        assert_eq!(metrics.snapshot().queuing, 2);
        drop(runtime);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.queuing, 0);
        assert_eq!(snapshot.dropped, 2);
        assert_eq!(snapshot.dropped_other, 2);
    }

    #[test]
    fn resource_opts_refuse_batch_time_larger_than_request_ttl() {
        let error = ActionResourceOpts::parse(
            "act",
            &serde_json::json!({"batch_time": "10s", "request_ttl": "5s"}),
        )
        .expect_err("a batch time larger than the TTL is refused");
        assert!(error.contains("batch_time"), "{error}");
        ActionResourceOpts::parse(
            "act",
            &serde_json::json!({"batch_time": "5s", "request_ttl": "5s"}),
        )
        .expect("an equal batch time and TTL parse");
    }

    #[tokio::test]
    async fn runtime_health_reason_hides_the_target() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind target");
        let address = listener.local_addr().expect("target address");
        drop(listener);
        let reason = probe_target(&address.to_string(), Some(Duration::from_secs(2)))
            .await
            .expect_err("a closed target fails");
        assert!(
            !reason.contains(&address.to_string()),
            "the reason hides the target: {reason}"
        );
    }

    #[test]
    fn probe_dial_target_uses_the_scheme_default_port() {
        assert_eq!(probe_dial_target("https://example.com"), "example.com:443");
        assert_eq!(
            probe_dial_target("https://example.com:8443/path"),
            "example.com:8443"
        );
        assert_eq!(probe_dial_target("http://example.com"), "example.com:80");
        assert_eq!(probe_dial_target("example.com:1234"), "example.com:1234");
        assert_eq!(probe_dial_target("example.com"), "example.com:80");
    }

    #[tokio::test]
    async fn runtime_holds_the_inflight_window_and_counts_expired_requests() {
        let inner = Arc::new(BatchSink {
            delay: Duration::from_millis(60),
            ..BatchSink::default()
        });
        let opts = ActionResourceOpts {
            batch_size: 10,
            batch_time: Duration::from_millis(1),
            inflight_window: 1,
            worker_pool_size: 1,
            request_ttl: Some(Duration::from_millis(20)),
            ..ActionResourceOpts::default()
        };
        let (runtime, metrics) = runtime(Arc::clone(&inner), opts);
        let topic = Topic::new("t/ttl").expect("topic");
        let payload = Bytes::from_static(b"{}");
        for _ in 0..2 {
            runtime
                .send(&topic, &payload, QoS::AtMostOnce)
                .await
                .expect("send joins the buffer");
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while (metrics.snapshot().dropped == 0
            || metrics.snapshot().success == 0
            || metrics.snapshot().inflight > 0)
            && Instant::now() < deadline
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.success, 1);
        assert_eq!(snapshot.dropped, 1);
        assert_eq!(snapshot.dropped_other, 1);
        assert_eq!(snapshot.failed, 0);
        assert_eq!(snapshot.inflight, 0);
        assert!(inner.max_active.load(AtomicOrdering::SeqCst) <= 1);
    }

    #[tokio::test]
    async fn runtime_sync_mode_flushes_a_partial_batch_before_it_returns() {
        let inner = Arc::new(BatchSink::default());
        let opts = ActionResourceOpts {
            batch_size: 100,
            batch_time: Duration::from_secs(10),
            query_mode: ActionQueryMode::Sync,
            ..ActionResourceOpts::default()
        };
        let (runtime, metrics) = runtime(Arc::clone(&inner), opts);
        let topic = Topic::new("t/sync").expect("topic");
        let payload = Bytes::from_static(b"{}");
        runtime
            .send(&topic, &payload, QoS::AtMostOnce)
            .await
            .expect("sync send flushes");
        assert_eq!(
            inner
                .batches
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone(),
            vec![1]
        );
        assert_eq!(metrics.snapshot().success, 1);
    }

    #[tokio::test]
    async fn runtime_worker_pool_size_caps_one_driver_batch_chunk() {
        let inner = Arc::new(BatchSink::default());
        let opts = ActionResourceOpts {
            batch_size: 10,
            batch_time: Duration::from_millis(1),
            inflight_window: 10,
            worker_pool_size: 2,
            ..ActionResourceOpts::default()
        };
        let (runtime, metrics) = runtime(Arc::clone(&inner), opts);
        let topic = Topic::new("t/workers").expect("topic");
        let payload = Bytes::from_static(b"{}");
        for _ in 0..5 {
            runtime
                .send(&topic, &payload, QoS::AtMostOnce)
                .await
                .expect("send joins the buffer");
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while metrics.snapshot().success < 5 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let batches = inner
            .batches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(batches.iter().sum::<usize>(), 5);
        assert!(batches.iter().all(|size| *size <= 2), "{batches:?}");
    }

    #[tokio::test]
    async fn runtime_holds_the_inflight_window_across_worker_chunks() {
        let inner = Arc::new(BatchSink {
            delay: Duration::from_millis(60),
            ..BatchSink::default()
        });
        let opts = ActionResourceOpts {
            batch_size: 10,
            batch_time: Duration::from_millis(1),
            inflight_window: 1,
            worker_pool_size: 2,
            ..ActionResourceOpts::default()
        };
        let (runtime, metrics) = runtime(Arc::clone(&inner), opts);
        let topic = Topic::new("t/inflight").expect("topic");
        let payload = Bytes::from_static(b"{}");
        for _ in 0..4 {
            runtime
                .send(&topic, &payload, QoS::AtMostOnce)
                .await
                .expect("send joins the buffer");
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while metrics.snapshot().success < 4 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.success, 4);
        assert_eq!(snapshot.inflight, 0);
        assert!(
            inner.max_active.load(AtomicOrdering::SeqCst) <= 1,
            "the inflight window holds one driver call, worker chunks may be larger"
        );
        let batches = inner
            .batches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(batches.iter().sum::<usize>(), 4);
        assert!(batches.iter().all(|size| *size <= 2), "{batches:?}");
    }

    #[tokio::test]
    async fn runtime_counts_matched_success_and_failed_for_one_action() {
        // Direct unit test for the counter math. The broker-path proof is
        // `ro01_action_runtime_applies_options_through_publish_and_restart`
        // in `tests/e2e_integration.rs`, which drives the same counters
        // through a real publish.
        let inner = Arc::new(BatchSink {
            fail: true,
            ..BatchSink::default()
        });
        let (runtime, metrics) = runtime(inner, ActionResourceOpts::default());
        let topic = Topic::new("t/fail").expect("topic");
        let payload = Bytes::from_static(b"{}");
        for _ in 0..3 {
            let _ = runtime.send(&topic, &payload, QoS::AtMostOnce).await;
        }
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.matched, 3);
        assert_eq!(snapshot.total, 3);
        assert_eq!(snapshot.success, 0);
        assert_eq!(snapshot.failed, 3);
        assert_eq!(snapshot.dropped, 0);
    }

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
        let error =
            build_couchbase_sink(&rec, &serde_json::json!({"username": "u", "password": "p"}))
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
        let error =
            build_elasticsearch_sink(&rec, &serde_json::json!({"server": "127.0.0.1:9200"}))
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
        let error =
            build_elasticsearch_sink(&no_id, &serde_json::json!({"server": "127.0.0.1:9200"}))
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
        let error =
            build_elasticsearch_sink(&bad, &serde_json::json!({"server": "127.0.0.1:9200"}))
                .err()
                .expect("a non-JSON document template refuses");
        assert!(error.contains("doc_template"), "{error}");
    }

    #[test]
    fn canonical_action_kind_maps_every_mv02d_alias() {
        assert_eq!(canonical_action_kind("minio"), "s3");
        assert_eq!(canonical_action_kind("azure_blob_storage"), "azure_blob");
        assert_eq!(canonical_action_kind("dynamo"), "dynamodb");
        assert_eq!(canonical_action_kind("ddb"), "dynamodb");
        assert_eq!(canonical_action_kind("bq"), "bigquery");
        assert_eq!(canonical_action_kind("gbq"), "bigquery");
        assert_eq!(canonical_action_kind("snowflake_streaming"), "snowflake");
        assert_eq!(canonical_action_kind("snowflake_aggregated"), "snowflake");
        assert_eq!(canonical_action_kind("ots"), "tablestore");
        assert_eq!(canonical_action_kind("delta_lake"), "databricks");
    }

    #[test]
    fn action_owned_kind_covers_the_mv02d_kinds() {
        for kind in [
            "s3",
            "minio",
            "azure_blob",
            "azure_blob_storage",
            "dynamodb",
            "dynamo",
            "ddb",
            "bigquery",
            "bq",
            "gbq",
            "disk_log",
            "snowflake",
            "snowflake_streaming",
            "snowflake_aggregated",
            "tablestore",
            "ots",
        ] {
            assert!(action_owned_kind(kind), "{kind} owns a runtime");
        }
        assert!(!action_owned_kind("databricks"));
        assert!(!action_owned_kind("delta_lake"));
    }

    fn mv02d_record(
        id: &str,
        action_type: &str,
        connector: &str,
        parameters: serde_json::Value,
    ) -> ActionRecord {
        ActionRecord {
            parameters,
            ..record(id, action_type, connector)
        }
    }

    #[cfg(feature = "s3")]
    #[tokio::test]
    async fn s3_action_builder_requires_the_bucket_and_key() {
        let rec = mv02d_record("act", "s3", "s3:up", serde_json::json!({}));
        let doc = serde_json::json!({"endpoint": "http://127.0.0.1:9000"});
        let error = build_s3_object_sink(&rec, &doc, &ActionResourceOpts::default())
            .err()
            .expect("a missing bucket refuses");
        assert!(error.contains("bucket"), "{error}");
        let rec = mv02d_record(
            "act",
            "minio",
            "s3:up",
            serde_json::json!({"bucket": "test-bucket", "key": "k/${topic}.ndjson"}),
        );
        let sink = build_s3_object_sink(&rec, &doc, &ActionResourceOpts::default())
            .expect("a full s3 pair builds");
        assert_eq!(sink.kind(), "s3");
    }

    #[cfg(feature = "azure_blob")]
    #[tokio::test]
    async fn azure_blob_action_builder_requires_the_container_and_blob() {
        let rec = mv02d_record("act", "azure_blob", "azure:up", serde_json::json!({}));
        let doc = serde_json::json!({"account_name": "acct", "account_key": "a2V5"});
        let error = build_azure_blob_sink(&rec, &doc, &ActionResourceOpts::default())
            .err()
            .expect("a missing container refuses");
        assert!(error.contains("container"), "{error}");
        let rec = mv02d_record(
            "act",
            "azure_blob_storage",
            "azure:up",
            serde_json::json!({"container": "test-container", "blob": "b/${batch_id}.json"}),
        );
        let sink = build_azure_blob_sink(&rec, &doc, &ActionResourceOpts::default())
            .expect("a full azure pair builds");
        assert_eq!(sink.kind(), "azure_blob");
    }

    #[cfg(feature = "dynamodb")]
    #[tokio::test]
    async fn dynamodb_action_builder_requires_the_table_and_hash_key() {
        let rec = mv02d_record("act", "dynamo", "ddb:up", serde_json::json!({}));
        let doc = serde_json::json!({
            "url": "http://127.0.0.1:8000",
            "region": "us-east-1",
            "aws_access_key_id": "AKID",
            "aws_secret_access_key": "secret",
        });
        let error = build_dynamodb_action_sink(&rec, &doc, &ActionResourceOpts::default())
            .err()
            .expect("a missing table refuses");
        assert!(error.contains("table"), "{error}");
        let rec = mv02d_record(
            "act",
            "ddb",
            "ddb:up",
            serde_json::json!({
                "table": "events",
                "hash_key": "id",
                "template": "{\"id\":\"${payload.id}\",\"temp\":\"${payload.temp}\"}",
            }),
        );
        let sink = build_dynamodb_action_sink(&rec, &doc, &ActionResourceOpts::default())
            .expect("a full dynamo pair builds");
        assert_eq!(sink.kind(), "dynamodb");
    }

    #[cfg(feature = "bigquery")]
    #[tokio::test]
    async fn bigquery_action_builder_requires_the_dataset_and_table() {
        let rec = mv02d_record("act", "bigquery", "bq:up", serde_json::json!({}));
        let doc = serde_json::json!({
            "authentication": {"type": "attached_service_account", "gcp_project_id": "p",
                               "access_token": "test-attached-token"},
        });
        let error = build_bigquery_sink(&rec, &doc, &ActionResourceOpts::default())
            .err()
            .expect("a missing dataset refuses");
        assert!(error.contains("dataset"), "{error}");
        let rec = mv02d_record(
            "act",
            "bq",
            "bq:up",
            serde_json::json!({"dataset": "d", "table": "events"}),
        );
        let sink = build_bigquery_sink(&rec, &doc, &ActionResourceOpts::default())
            .expect("a full bigquery pair builds");
        assert_eq!(sink.kind(), "bigquery");
        // The attached form uses the runtime environment.
        // It builds without a configured token.
        let bare = serde_json::json!({
            "authentication": {"type": "attached_service_account", "gcp_project_id": "p"},
        });
        let sink = build_bigquery_sink(&rec, &bare, &ActionResourceOpts::default())
            .expect("attached without token builds");
        assert_eq!(sink.kind(), "bigquery");
    }

    #[tokio::test]
    async fn disk_log_action_builder_refuses_a_path_outside_the_data_directory() {
        let doc = serde_json::json!({
            "filepath": "../escape.log",
            "max_file_size": "1MB",
            "max_file_number": 3,
        });
        let rec = mv02d_record("act", "disk_log", "disk:up", serde_json::json!({}));
        let error = build_disk_log_action_sink(&rec, &doc, &ActionResourceOpts::default())
            .err()
            .expect("a parent path refuses");
        assert!(error.contains("leave the data directory"), "{error}");
        let doc = serde_json::json!({
            "filepath": "/tmp/escape.log",
            "max_file_size": "1MB",
            "max_file_number": 3,
        });
        let error = build_disk_log_action_sink(&rec, &doc, &ActionResourceOpts::default())
            .err()
            .expect("an absolute path refuses");
        assert!(error.contains("relative to the data directory"), "{error}");
        // A symlink that points out is refused, even with a clean name.
        let dir = std::env::temp_dir().join(format!(
            "mv02d-disklink-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        let outside = dir.join("outside.log");
        std::fs::write(&outside, b"x").expect("outside file");
        let link = dir.join("link.log");
        #[cfg(unix)]
        std::os::unix::fs::symlink(&outside, &link).expect("symlink");
        #[cfg(unix)]
        {
            let error =
                disk_log_parts(link.to_str().expect("path"), "act").expect_err("a symlink refuses");
            assert!(error.contains("symlink"), "{error}");
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    async fn disk_log_write_modes_check(scratch: &std::path::Path) {
        use broker_connectors::disk_log::{DiskLogSink, MemoryDiskLogWriter};
        // Each write mode delivers through the runtime. The flush count
        // changes with the mode: `sync` flushes each batch, `async`
        // leaves sync to the OS. The memory sink clones the entry
        // config, so the test sets no field by hand.
        for (mode, flushes) in [("sync", 1_u64), ("async", 0_u64)] {
            let rec = mv02d_record(
                "act",
                "disk_log",
                "disk:up",
                serde_json::json!({"write_mode": mode}),
            );
            let doc = serde_json::json!({
                "filepath": format!("logs/mv02d-{mode}.log"),
                "max_file_size": "1MB",
                "max_file_number": 3,
            });
            let file_sink = build_disk_log_action_sink_with_dir(
                &rec,
                &doc,
                scratch,
                &ActionResourceOpts::default(),
            )
            .expect("both write modes build");
            assert_eq!(file_sink.kind(), "disk_log");
            let config = file_sink.config().clone();
            let metrics = std::sync::Arc::new(ActionMetrics::new());
            let runtime = ActionRuntimeSink::new(
                file_sink as std::sync::Arc<dyn broker_connectors::Sink>,
                ActionResourceOpts::default(),
                std::sync::Arc::clone(&metrics),
                None,
            );
            let topic = Topic::new("t/mode").unwrap();
            runtime
                .send(&topic, &Bytes::from("{}"), QoS::AtMostOnce)
                .await
                .expect("runtime sends the mode");
            assert_eq!(
                metrics.snapshot().success,
                1,
                "mode {mode} delivers through the runtime"
            );
            let writer =
                std::sync::Arc::new(MemoryDiskLogWriter::new(&config).expect("entry config opens"));
            let mem_sink = std::sync::Arc::new(
                DiskLogSink::new(config, writer.clone()).expect("memory sink builds"),
            );
            let mem_metrics = std::sync::Arc::new(ActionMetrics::new());
            let mem_runtime = ActionRuntimeSink::new(
                std::sync::Arc::clone(&mem_sink) as std::sync::Arc<dyn broker_connectors::Sink>,
                ActionResourceOpts::default(),
                std::sync::Arc::clone(&mem_metrics),
                None,
            );
            mem_runtime
                .send(&topic, &Bytes::from("{}"), QoS::AtMostOnce)
                .await
                .expect("memory runtime sends the mode");
            assert_eq!(
                mem_metrics.snapshot().success,
                1,
                "mode {mode} delivers in memory"
            );
            assert_eq!(
                writer.flush_count(),
                flushes,
                "write mode {mode} flushes {flushes} time(s)"
            );
        }
    }

    async fn disk_log_retention_check(scratch: &std::path::Path) {
        use broker_connectors::disk_log::{DiskLogSink, MemoryDiskLogWriter};
        // `rotation.retention_period` builds through the broker entry.
        // Delivery through the runtime is proven in the mode check
        // and the period check. This check keeps the entry builds
        // and the pruning difference.
        let mut retention_configs = Vec::new();
        for (tag, retention) in [
            ("one", serde_json::json!(1)),
            ("unlimited", serde_json::json!("infinity")),
        ] {
            let doc = serde_json::json!({
                "filepath": format!("logs/mv02d-ret-{tag}.log"),
                "max_file_size": "1MB",
                "max_file_number": 10,
                "rotation": {"period": "hour", "retention_period": retention.clone()},
            });
            let rec = mv02d_record("act", "disk_log", "disk:up", serde_json::json!({}));
            let file_sink = build_disk_log_action_sink_with_dir(
                &rec,
                &doc,
                scratch,
                &ActionResourceOpts::default(),
            )
            .expect("both retention values build");
            assert_eq!(file_sink.kind(), "disk_log");
            retention_configs.push((tag, file_sink.config().clone()));
        }
        // `rotation.retention_period` changes pruning at the driver.
        // One day keeps one fresh backup, no limit keeps three.
        // Explicit timestamps keep the test exact. The configs come
        // from the entry builds above, so no field is set by hand.
        // Four writes land one hour apart, then one lands after one day.
        let mut kept = Vec::new();
        let topic = Topic::new("t").unwrap();
        for (tag, config) in retention_configs {
            let writer =
                std::sync::Arc::new(MemoryDiskLogWriter::new(&config).expect("entry config opens"));
            let sink = DiskLogSink::new(config, writer.clone()).expect("memory sink builds");
            for millis in [0_i64, 3_700_000, 7_400_000, 92_000_000] {
                sink.append_at(&topic, &Bytes::from("{}"), QoS::AtMostOnce, millis)
                    .await
                    .unwrap();
            }
            kept.push((tag, writer.backup_blobs().len(), sink.backup_names().len()));
        }
        assert_eq!(kept[0].1, 1, "one day keeps one backup");
        assert_eq!(kept[0].2, 1, "one day lists one backup");
        assert_eq!(kept[1].1, 3, "no limit keeps three backups");
        assert_eq!(kept[1].2, 3, "no limit lists three backups");
    }

    async fn disk_log_period_check(scratch: &std::path::Path) {
        use broker_connectors::disk_log::{DiskLogSink, MemoryDiskLogWriter};
        // `rotation.period` changes age rotation at the driver.
        // `hour` rotates a segment older than 3_600 s, `none` never
        // rotates by age. Explicit timestamps keep the test exact.
        for (period, rotates) in [("hour", 1_u64), ("none", 0_u64)] {
            let doc = serde_json::json!({
                "filepath": format!("logs/mv02d-age-{period}.log"),
                "max_file_size": "1MB",
                "max_file_number": 3,
                "rotation": {"period": period},
            });
            let rec = mv02d_record("act", "disk_log", "disk:up", serde_json::json!({}));
            let file_sink = build_disk_log_action_sink_with_dir(
                &rec,
                &doc,
                scratch,
                &ActionResourceOpts::default(),
            )
            .expect("both periods build");
            assert_eq!(file_sink.kind(), "disk_log");
            let config = file_sink.config().clone();
            let metrics = std::sync::Arc::new(ActionMetrics::new());
            let runtime = ActionRuntimeSink::new(
                file_sink as std::sync::Arc<dyn broker_connectors::Sink>,
                ActionResourceOpts::default(),
                std::sync::Arc::clone(&metrics),
                None,
            );
            let topic = Topic::new("t/age").unwrap();
            runtime
                .send(&topic, &Bytes::from("{}"), QoS::AtMostOnce)
                .await
                .expect("runtime sends with period");
            assert_eq!(
                metrics.snapshot().success,
                1,
                "period {period} delivers through the runtime"
            );
            let writer =
                std::sync::Arc::new(MemoryDiskLogWriter::new(&config).expect("entry config opens"));
            let sink = DiskLogSink::new(config, writer).expect("memory sink builds");
            sink.append_at(&topic, &Bytes::from("{}"), QoS::AtMostOnce, 0)
                .await
                .unwrap();
            sink.append_at(&topic, &Bytes::from("{}"), QoS::AtMostOnce, 3_601_000)
                .await
                .unwrap();
            assert_eq!(
                sink.rotation_count(),
                rotates,
                "period {period} rotates by age: {rotates}"
            );
        }
    }

    #[tokio::test]
    async fn disk_log_rotation_and_write_mode_change_the_runtime_result() {
        // Both documented write modes build through the broker entry.
        // One scratch directory serves all three legs. Distinct file
        // names keep isolation. The final call cleans the directory.
        let scratch = std::env::temp_dir().join(format!(
            "mv02d-diskmodes-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        disk_log_write_modes_check(&scratch).await;
        disk_log_retention_check(&scratch).await;
        disk_log_period_check(&scratch).await;
        std::fs::remove_dir_all(&scratch).ok();
    }

    #[tokio::test]
    async fn snowflake_and_tablestore_builders_refuse_the_missing_action_fields() {
        let doc = serde_json::json!({
            "account": "acct",
            "pipe_user": "user",
            "private_key": "not-a-key",
        });
        let rec = mv02d_record("act", "snowflake", "sf:up", serde_json::json!({}));
        let error = build_snowflake_sink(&rec, &doc, &ActionResourceOpts::default())
            .err()
            .expect("a missing database refuses");
        assert!(error.contains("database"), "{error}");

        let doc = serde_json::json!({
            "endpoint": "http://127.0.0.1:9000",
            "instance_name": "i",
            "access_key_id": "ak",
            "access_key_secret": "sk",
        });
        let rec = mv02d_record("act", "ots", "ots:up", serde_json::json!({}));
        let error = build_tablestore_sink(&rec, &doc, &ActionResourceOpts::default())
            .err()
            .expect("a missing table refuses");
        assert!(error.contains("table_name"), "{error}");
        let rec = mv02d_record(
            "act",
            "tablestore",
            "ots:up",
            serde_json::json!({
                "table_name": "t",
                "measurement": "m",
                "tags": {"device": "${client_id}"},
                "fields": [{"column": "temp", "value": "${payload.temp}"}],
            }),
        );
        let sink = build_tablestore_sink(&rec, &doc, &ActionResourceOpts::default())
            .expect("a full tablestore pair builds");
        assert_eq!(sink.kind(), "tablestore");
    }

    #[test]
    fn canonical_action_kind_maps_every_mv03b_alias() {
        assert_eq!(canonical_action_kind("aws_kinesis"), "kinesis");
        assert_eq!(canonical_action_kind("gcp"), "gcp_pubsub");
        assert_eq!(canonical_action_kind("pubsub"), "gcp_pubsub");
        assert_eq!(canonical_action_kind("gcp_pubsub_producer"), "gcp_pubsub");
        assert_eq!(canonical_action_kind("gcp_pubsub_consumer"), "gcp_pubsub");
        assert_eq!(canonical_action_kind("azure_event_hubs"), "azure_eventhubs");
        assert_eq!(canonical_action_kind("azure"), "azure_eventhubs");
        assert_eq!(canonical_action_kind("eventhubs"), "azure_eventhubs");
        assert_eq!(
            canonical_action_kind("azure_event_hub_producer"),
            "azure_eventhubs"
        );
        assert_eq!(canonical_action_kind("oci"), "oci_streaming");
        assert_eq!(canonical_action_kind("aws_iot_core"), "aws_iot");
        assert_eq!(canonical_action_kind("azure_iot_hub"), "azure_iot");
        assert_eq!(canonical_action_kind("azure_iothub"), "azure_iot");
        assert_eq!(canonical_action_kind("iothub"), "azure_iot");
        assert_eq!(canonical_action_kind("gcp_iot_core"), "gcp_iot");
        assert_eq!(canonical_action_kind("cloud_iot"), "gcp_iot");
    }

    #[test]
    fn action_owned_kind_covers_the_mv03b_kinds() {
        for kind in [
            "kinesis",
            "aws_kinesis",
            "gcp_pubsub",
            "gcp",
            "pubsub",
            "gcp_pubsub_producer",
            "gcp_pubsub_consumer",
            "azure_eventhubs",
            "azure_event_hubs",
            "azure",
            "eventhubs",
            "azure_event_hub_producer",
            "aws_iot",
            "aws_iot_core",
            "azure_iot",
            "azure_iot_hub",
            "azure_iothub",
            "iothub",
            "oci_streaming",
            "oci",
        ] {
            assert!(action_owned_kind(kind), "{kind} owns a runtime");
        }
        assert!(!action_owned_kind("gcp_iot"));
        assert!(!action_owned_kind("gcp_iot_core"));
    }

    #[cfg(feature = "kinesis")]
    #[tokio::test]
    async fn kinesis_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
        let doc = serde_json::json!({
            "endpoint": "http://127.0.0.1:9",
            "region": "us-east-1",
            "aws_access_key_id": "AKID",
            "aws_secret_access_key": "secret",
        });
        let rec = mv02d_record("act", "aws_kinesis", "k:up", serde_json::json!({}));
        let error = build_kinesis_sink(&rec, &doc)
            .err()
            .expect("a missing stream refuses");
        assert!(error.contains("stream_name"), "{error}");
        let rec = mv02d_record(
            "act",
            "kinesis",
            "k:up",
            serde_json::json!({"stream_name": "telemetry", "partition_key": "${client_id}"}),
        );
        let sink = build_kinesis_sink(&rec, &doc).expect("a full kinesis pair builds");
        assert_eq!(sink.kind(), "kinesis");
    }

    #[cfg(feature = "gcp_pubsub")]
    #[tokio::test]
    async fn gcp_pubsub_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
        let doc = serde_json::json!({"project_id": "demo-proj"});
        let rec = mv02d_record("act", "gcp", "g:up", serde_json::json!({}));
        let error = build_gcp_pubsub_sink(&rec, &doc)
            .err()
            .expect("a missing topic refuses");
        assert!(error.contains("pubsub_topic"), "{error}");
        let rec = mv02d_record(
            "act",
            "gcp_pubsub_producer",
            "g:up",
            serde_json::json!({"pubsub_topic": "events"}),
        );
        let sink = build_gcp_pubsub_sink(&rec, &doc).expect("a full pubsub pair builds");
        assert_eq!(sink.kind(), "gcp_pubsub");
    }

    #[cfg(feature = "azure_eventhubs")]
    #[tokio::test]
    async fn azure_eventhubs_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
        let doc = serde_json::json!({
            "namespace": "ns-1",
            "shared_access_key_name": "send",
            "shared_access_key": "secret",
        });
        let rec = mv02d_record("act", "azure", "e:up", serde_json::json!({}));
        let error = build_azure_eventhubs_sink(&rec, &doc)
            .err()
            .expect("a missing hub refuses");
        assert!(error.contains("event_hub"), "{error}");
        let rec = mv02d_record(
            "act",
            "azure_event_hub_producer",
            "e:up",
            serde_json::json!({"event_hub": "hub-a"}),
        );
        let sink = build_azure_eventhubs_sink(&rec, &doc).expect("a full hub pair builds");
        assert_eq!(sink.kind(), "azure_eventhubs");
    }

    #[cfg(feature = "aws_iot")]
    #[tokio::test]
    async fn aws_iot_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
        let doc = serde_json::json!({"endpoint": "127.0.0.1:8883"});
        let rec = mv02d_record(
            "act",
            "aws_iot",
            "a:up",
            serde_json::json!({"remote_topic": "t/${client_id}"}),
        );
        let error = build_aws_iot_sink(&rec, &doc)
            .err()
            .expect("a missing cert refuses");
        assert!(error.contains("client_cert_pem"), "{error}");
    }

    #[cfg(feature = "azure_iot")]
    #[tokio::test]
    async fn azure_iot_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
        let doc = serde_json::json!({"device_id": "edge-1", "shared_access_key": "s"});
        let rec = mv02d_record("act", "azure_iot", "z:up", serde_json::json!({}));
        let error = build_azure_iot_sink(&rec, &doc)
            .err()
            .expect("a missing hub refuses");
        assert!(error.contains("iot_hub_name"), "{error}");
    }

    #[tokio::test]
    async fn oci_streaming_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
        let doc = serde_json::json!({
            "endpoint": "https://cell-1.streaming.us-east-1.oraclecloud.com",
            "stream_pool_id": "ocid1.streampool.oc1.phx.aaa",
            "tenancy_ocid": "ocid1.tenancy.oc1..aaa",
            "user_ocid": "ocid1.user.oc1..aaa",
            "fingerprint": "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99",
            "private_key_pem": "k",
        });
        let rec = mv02d_record("act", "oci", "o:up", serde_json::json!({}));
        let error = build_oci_streaming_sink(&rec, &doc)
            .err()
            .expect("a missing stream refuses");
        assert!(error.contains("stream_id"), "{error}");
    }

    #[cfg(feature = "rabbitmq")]
    #[tokio::test]
    async fn rabbitmq_sink_builds_with_empty_exchange_for_default_exchange() {
        // Empty exchange selects the default exchange.
        // See #components/schemas/bridge_rabbitmq.action_parameters.
        let rec = mv02b_record(
            "act",
            "rabbitmq",
            "rabbitmq:up",
            serde_json::json!({"routing_key": "queue.${topic}"}),
        );
        let doc = serde_json::json!({
            "servers": "127.0.0.1:5672",
            "username": "u",
            "password": "p",
        });
        let config = rabbitmq_sink_config(&rec, &doc).expect("empty exchange builds a config");
        assert_eq!(config.exchange, "");
        assert_eq!(config.routing_key_template, "queue.${topic}");
        let sink = build_rabbitmq_sink(&rec, &doc).expect("empty exchange builds a sink");
        assert_eq!(sink.kind(), "rabbitmq");
        let bare = mv02b_record("act", "rabbitmq", "rabbitmq:up", serde_json::json!({}));
        let error = build_rabbitmq_sink(&bare, &doc)
            .err()
            .expect("a missing routing key refuses");
        assert!(error.contains("routing_key"), "{error}");
    }

    #[test]
    fn canonical_action_kind_maps_every_mv03a_alias() {
        assert_eq!(canonical_action_kind("mqtt"), "mqtt_bridge");
        assert_eq!(canonical_action_kind("kafka_producer"), "kafka");
        assert_eq!(canonical_action_kind("kafka_consumer"), "kafka");
        assert_eq!(canonical_action_kind("confluent_producer"), "confluent");
        assert_eq!(canonical_action_kind("confluent_kafka"), "confluent");
        assert_eq!(canonical_action_kind("confluent_cloud"), "confluent");
        assert_eq!(canonical_action_kind("rabbit"), "rabbitmq");
        assert_eq!(canonical_action_kind("amqp"), "rabbitmq");
        for kind in [
            "kafka",
            "confluent",
            "rabbitmq",
            "pulsar",
            "rocketmq",
            "mqtt_bridge",
            "redis",
        ] {
            assert_eq!(canonical_action_kind(kind), kind);
        }
    }

    #[test]
    fn canonical_action_kind_maps_every_remaining_alias() {
        assert_eq!(canonical_action_kind("sqlserver"), "mssql");
        assert_eq!(canonical_action_kind("mariadb"), "mysql");
        assert_eq!(canonical_action_kind("cockroach"), "cockroachdb");
        assert_eq!(canonical_action_kind("postgres"), "pgsql");
        assert_eq!(canonical_action_kind("postgresql"), "pgsql");
        assert_eq!(canonical_action_kind("google_alloydb"), "alloydb");
        assert_eq!(canonical_action_kind("alloy_db"), "alloydb");
        assert_eq!(canonical_action_kind("opcua"), "opc_ua");
        assert_eq!(canonical_action_kind("sparkplug"), "sparkplug_b");
        assert_eq!(canonical_action_kind("spb"), "sparkplug_b");
        assert_eq!(canonical_action_kind("rest"), "http");
        assert_eq!(canonical_action_kind("logger"), "console");
    }

    #[test]
    fn action_owned_kind_covers_the_mv03a_kinds() {
        for kind in [
            "kafka",
            "kafka_producer",
            "kafka_consumer",
            "confluent",
            "confluent_producer",
            "confluent_kafka",
            "confluent_cloud",
            "rabbitmq",
            "rabbit",
            "amqp",
            "pulsar",
            "rocketmq",
            "mqtt_bridge",
            "mqtt",
            "redis",
        ] {
            assert!(action_owned_kind(kind), "{kind} owns a runtime");
        }
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn kafka_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
        let rec = mv02b_record(
            "act",
            "kafka",
            "kafka:up",
            serde_json::json!({"topic": "events"}),
        );
        let error = build_kafka_sink(&rec, &serde_json::json!({}))
            .err()
            .expect("a missing bootstrap host refuses");
        assert!(error.contains("bootstrap_hosts"), "{error}");
        let sasl = mv02b_record(
            "act",
            "kafka",
            "kafka:up",
            serde_json::json!({"topic": "events"}),
        );
        let error = build_kafka_sink(
            &sasl,
            &serde_json::json!({
                "bootstrap_hosts": "h:9092",
                "authentication": {"mechanism": "plain", "username": "u", "password": "p"},
            }),
        )
        .err()
        .expect("a SASL connector refuses");
        assert!(error.contains("authentication"), "{error}");
        let cfg = kafka_sink_config(
            &mv02b_record(
                "act",
                "kafka",
                "kafka:up",
                serde_json::json!({
                    "topic": "events",
                    "message": {"key": "${payload.k}"},
                    "partition_strategy": "key_dispatch",
                    "required_acks": "leader_only",
                }),
            ),
            &serde_json::json!({"bootstrap_hosts": "h:9092"}),
        )
        .expect("a full kafka config builds");
        assert_eq!(cfg.bootstrap_servers, "h:9092");
        assert_eq!(cfg.topic_template, "events");
        assert_eq!(cfg.key_template.as_deref(), Some("${payload.k}"));
        assert!(!cfg.random_partition);
        assert_eq!(cfg.acks, "1");
        let sink = build_kafka_sink(
            &mv02b_record(
                "act",
                "kafka",
                "kafka:up",
                serde_json::json!({"topic": "events"}),
            ),
            &serde_json::json!({"bootstrap_hosts": "h:9092"}),
        )
        .expect("a full kafka pair builds");
        assert_eq!(sink.kind(), "kafka");
        // Defaults: random strategy spreads, acks wait for all replicas.
        let cfg = kafka_sink_config(
            &mv02b_record(
                "act",
                "kafka",
                "kafka:up",
                serde_json::json!({"topic": "events"}),
            ),
            &serde_json::json!({"bootstrap_hosts": "h:9092"}),
        )
        .expect("defaults build");
        assert!(cfg.random_partition);
        assert_eq!(cfg.acks, "all");
        assert_eq!(cfg.key_template.as_deref(), Some("${.clientid}"));
    }

    #[cfg(feature = "confluent")]
    #[test]
    fn confluent_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
        let rec = mv02b_record(
            "act",
            "confluent",
            "confluent:up",
            serde_json::json!({"topic": "events"}),
        );
        let error = build_confluent_sink(&rec, &serde_json::json!({"bootstrap_hosts": "h:9092"}))
            .err()
            .expect("a missing API key refuses");
        assert!(error.contains("api_key"), "{error}");
        let error = build_confluent_sink(
            &rec,
            &serde_json::json!({"bootstrap_hosts": "h:9092", "api_key": "k"}),
        )
        .err()
        .expect("a missing API secret refuses");
        assert!(error.contains("api_secret"), "{error}");
        // The reference `authentication` object supplies the same pair.
        let cfg = confluent_sink_config(
            &rec,
            &serde_json::json!({
                "bootstrap_hosts": "h:9092",
                "authentication": {"username": "k", "password": "s"},
                "ssl": {"enable": false},
            }),
        )
        .expect("a nested credentials pair builds");
        assert_eq!(cfg.api_key, "k");
        assert_eq!(cfg.api_secret, "s");
        assert_eq!(
            cfg.security_protocol,
            broker_connectors_enterprise::confluent::ConfluentSecurityProtocol::SaslPlaintext
        );
        assert_eq!(cfg.bootstrap_servers, vec!["h:9092".to_string()]);
        assert_eq!(
            cfg.partition_key_template.as_deref(),
            Some("${.clientid}"),
            "absent key defaults to the reference client id"
        );
        let sink = build_confluent_sink(
            &rec,
            &serde_json::json!({"bootstrap_hosts": "h:9092", "api_key": "k", "api_secret": "s"}),
        )
        .expect("a full confluent pair builds");
        assert_eq!(sink.kind(), "confluent");
    }

    #[cfg(feature = "rabbitmq")]
    #[test]
    fn rabbitmq_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
        let rec = mv02b_record(
            "act",
            "rabbitmq",
            "rabbitmq:up",
            serde_json::json!({"routing_key": "sensor.${topic}"}),
        );
        let error = build_rabbitmq_sink(&rec, &serde_json::json!({"servers": "h:5672"}))
            .err()
            .expect("a missing username refuses");
        assert!(error.contains("username"), "{error}");
        let error =
            build_rabbitmq_sink(&rec, &serde_json::json!({"username": "u", "password": "p"}))
                .err()
                .expect("a missing target refuses");
        assert!(error.contains("servers"), "{error}");
        let cfg = rabbitmq_sink_config(
            &rec,
            &serde_json::json!({"servers": "h:5672", "username": "u", "password": "p"}),
        )
        .expect("a full rabbitmq config builds");
        assert_eq!(cfg.endpoint, "amqp://u:p@h:5672/%2f");
        assert_eq!(cfg.exchange, "");
        assert_eq!(cfg.routing_key_template, "sensor.${topic}");
        assert_eq!(cfg.delivery_mode, 1);
        let sink = build_rabbitmq_sink(
            &mv02b_record(
                "act",
                "rabbitmq",
                "rabbitmq:up",
                serde_json::json!({"routing_key": "k", "exchange": "e", "delivery_mode": "persistent"}),
            ),
            &serde_json::json!({"servers": "h:5672", "username": "u", "password": "p"}),
        )
        .expect("a full rabbitmq pair builds");
        assert_eq!(sink.kind(), "rabbitmq");
    }

    #[test]
    fn redis_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
        let rec = mv02b_record(
            "act",
            "redis",
            "redis:up",
            serde_json::json!({"command_template": ["HSET", "s:${topic}", "f", "${payload}"]}),
        );
        let error = build_redis_sink(&rec, &serde_json::json!({}))
            .err()
            .expect("a missing server refuses");
        assert!(error.contains("servers"), "{error}");
        let cfg = redis_sink_config(
            &rec,
            &serde_json::json!({"servers": "h:6380", "password": "pw", "database": 2}),
        )
        .expect("a full redis config builds");
        assert_eq!(cfg.endpoint, "redis://:pw@h:6380/2");
        match &cfg.command {
            broker_connectors::redis::RedisCommandKind::Raw { argv_templates } => {
                assert_eq!(argv_templates.len(), 4);
                assert_eq!(argv_templates[0], "HSET");
            }
            other => panic!("the action builds a raw command, got {other:?}"),
        }
        let sink = build_redis_sink(&rec, &serde_json::json!({"servers": "h:6379"}))
            .expect("a full redis pair builds");
        assert_eq!(sink.kind(), "redis");
    }

    #[test]
    fn mqtt_bridge_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
        let rec = mv02b_record(
            "act",
            "mqtt_bridge",
            "mqtt_bridge:up",
            serde_json::json!({"topic": "remote/${topic}", "qos": 1, "retain": true}),
        );
        let error = build_mqtt_bridge_sink(&rec, &serde_json::json!({}))
            .err()
            .expect("a missing server refuses");
        assert!(error.contains("server"), "{error}");
        let cfg = mqtt_bridge_sink_config(
            &rec,
            &serde_json::json!({"server": "h:1883", "proto_ver": "v5"}),
        )
        .expect("a full bridge config builds");
        assert_eq!(cfg.broker_address, "h:1883");
        assert_eq!(cfg.topic_template.as_deref(), Some("remote/${topic}"));
        assert_eq!(cfg.qos_override, Some(1));
        assert_eq!(cfg.retain_override, Some(true));
        assert_eq!(cfg.protocol, broker_connectors::MqttBridgeProtocol::V50);
        assert_eq!(cfg.max_batch_size, Some(1));
        let sink = build_mqtt_bridge_sink(&rec, &serde_json::json!({"server": "h:1883"}))
            .expect("a full bridge pair builds");
        assert_eq!(sink.kind(), "mqtt_bridge");
    }

    #[test]
    fn pulsar_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
        let rec = mv02b_record(
            "act",
            "pulsar",
            "pulsar:up",
            serde_json::json!({"pulsar_topic": "events", "message": {"key": "${client_id}"}}),
        );
        let error = build_pulsar_sink(&rec, &serde_json::json!({}))
            .err()
            .expect("a missing service URL refuses");
        assert!(error.contains("servers"), "{error}");
        let error = build_pulsar_sink(&rec, &serde_json::json!({"servers": "http://h:8080"}))
            .err()
            .expect("an admin URL refuses");
        assert!(error.contains("pulsar://"), "{error}");
        let error = build_pulsar_sink(
            &rec,
            &serde_json::json!({"servers": "pulsar://h:6650", "authentication": "token"}),
        )
        .err()
        .expect("a token auth refuses");
        assert!(error.contains("authentication"), "{error}");
        let cfg = pulsar_sink_config(
            &rec,
            &serde_json::json!({"servers": "pulsar://h:6650", "tenant": "t", "namespace": "n"}),
        )
        .expect("a full pulsar config builds");
        assert_eq!(cfg.service_url, "pulsar://h:6650");
        assert_eq!(cfg.tenant, "t");
        assert_eq!(cfg.topic, "events");
        assert_eq!(cfg.batch_size, Some(1));
        assert_eq!(cfg.max_retries, Some(3));
        let sink = build_pulsar_sink(&rec, &serde_json::json!({"servers": "pulsar://h:6650"}))
            .expect("a full pulsar pair builds");
        assert_eq!(sink.kind(), "pulsar");
    }

    #[test]
    fn rocketmq_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
        let rec = mv02b_record(
            "act",
            "rocketmq",
            "rocketmq:up",
            serde_json::json!({"tag": "t", "key": "${client_id}"}),
        );
        let error = build_rocketmq_sink(&rec, &serde_json::json!({}))
            .err()
            .expect("a missing server refuses");
        assert!(error.contains("servers"), "{error}");
        let error = build_rocketmq_sink(
            &rec,
            &serde_json::json!({"servers": "h:9876", "access_key": "k"}),
        )
        .err()
        .expect("a half ACL pair refuses");
        assert!(error.contains("secret_key"), "{error}");
        let cfg = rocketmq_sink_config(
            &rec,
            &serde_json::json!({"servers": "h:9876", "access_key": "k", "secret_key": "s"}),
        )
        .expect("a full rocketmq config builds");
        assert_eq!(cfg.endpoints, vec!["h:9876".to_string()]);
        assert_eq!(cfg.topic, "TopicTest");
        assert_eq!(cfg.tag_template.as_deref(), Some("t"));
        let sink = build_rocketmq_sink(&rec, &serde_json::json!({"servers": "h:9876"}))
            .expect("a full rocketmq pair builds");
        assert_eq!(sink.kind(), "rocketmq");
    }

    #[tokio::test]
    async fn two_actions_keep_separate_counters() {
        // Direct unit test for per-action separation. The broker-path proof
        // is `mv01_pgsql_action_delivers_engine_rows_to_connector_settings`
        // in `tests/e2e_integration.rs`, which separates two actions through
        // a real publish.
        let good = Arc::new(BatchSink::default());
        let bad = Arc::new(BatchSink {
            fail: true,
            ..BatchSink::default()
        });
        let (good_runtime, good_metrics) = runtime(good, ActionResourceOpts::default());
        let (bad_runtime, bad_metrics) = runtime(bad, ActionResourceOpts::default());
        let topic = Topic::new("t/two").expect("topic");
        let payload = Bytes::from_static(b"{}");
        good_runtime
            .send(&topic, &payload, QoS::AtMostOnce)
            .await
            .expect("good action sends");
        let _ = bad_runtime.send(&topic, &payload, QoS::AtMostOnce).await;
        assert_eq!(good_metrics.snapshot().success, 1);
        assert_eq!(good_metrics.snapshot().failed, 0);
        assert_eq!(bad_metrics.snapshot().success, 0);
        assert_eq!(bad_metrics.snapshot().failed, 1);
    }

    #[tokio::test]
    async fn mv03c_canonical_maps_every_alias() {
        assert_eq!(canonical_action_kind("opcua"), "opc_ua");
        assert_eq!(canonical_action_kind("sparkplug"), "sparkplug_b");
        assert_eq!(canonical_action_kind("spb"), "sparkplug_b");
        assert_eq!(canonical_action_kind("rest"), "http");
        assert_eq!(canonical_action_kind("http"), "http");
    }

    #[tokio::test]
    async fn mv03c_owned_kind_covers_device_and_http() {
        assert!(action_owned_kind("http"));
        assert!(action_owned_kind("rest"));
        assert!(action_owned_kind("opc_ua"));
        assert!(action_owned_kind("opcua"));
        assert!(action_owned_kind("sparkplug_b"));
        assert!(action_owned_kind("sparkplug"));
        assert!(action_owned_kind("spb"));
    }

    #[tokio::test]
    async fn mv03c_opc_ua_sink_uses_connector_and_action_fields() {
        let doc = serde_json::json!({"endpoint_url": "opc.tcp://127.0.0.1:4840"});
        let rec = record("act", "opc_ua", "opc_ua:up");
        let error = build_opc_ua_sink(&rec, &doc)
            .err()
            .expect("a missing node id refuses");
        assert!(error.contains("node_id"), "{error}");
        let mut full = record("act", "opc_ua", "opc_ua:up");
        full.parameters = serde_json::json!({"node_id": "ns=1;i=1001"});
        let sink = build_opc_ua_sink(&full, &doc).expect("a full pair builds");
        assert_eq!(sink.kind(), "opc_ua");
        let bad_doc = serde_json::json!({"server": "127.0.0.1:4840"});
        let error = build_opc_ua_sink(&full, &bad_doc)
            .err()
            .expect("a missing endpoint refuses");
        assert!(error.contains("endpoint_url"), "{error}");
    }

    #[tokio::test]
    async fn mv03c_sparkplug_b_sink_uses_connector_and_action_fields() {
        let doc = serde_json::json!({"mqtt_url": "mqtt://127.0.0.1:1883"});
        let mut rec = record("act", "sparkplug_b", "sparkplug_b:up");
        rec.parameters = serde_json::json!({"topic_prefix": "spBv1.0/plant1"});
        let sink = build_sparkplug_b_sink(&rec, &doc).expect("a full pair builds");
        assert_eq!(sink.kind(), "sparkplug_b");
        let bare = record("act", "sparkplug_b", "sparkplug_b:up");
        let error = build_sparkplug_b_sink(&bare, &serde_json::json!({}))
            .err()
            .expect("a missing mqtt url refuses");
        assert!(error.contains("mqtt_url"), "{error}");
        let mut bad_prefix = record("act", "sparkplug_b", "sparkplug_b:up");
        bad_prefix.parameters = serde_json::json!({"topic_prefix": "spBv1.0/a/#/x"});
        let error = required_action_parameters(&bad_prefix).expect_err("a wildcard prefix refuses");
        assert!(error.contains("topic_prefix"), "{error}");
    }

    #[cfg(feature = "bigquery")]
    #[test]
    fn bigquery_auth_refuses_bad_service_account_json() {
        let rec = record("act", "bigquery", "bq:up");
        let doc = serde_json::json!({
            "authentication": {"type": "service_account_json", "service_account_json": "{not json"}
        });
        let error = bigquery_auth(&doc, &rec.connector, &rec.id).expect_err("bad JSON refuses");
        assert!(
            error.contains("authentication.service_account_json"),
            "names the field: {error}"
        );
    }

    #[cfg(feature = "bigquery")]
    #[test]
    fn bigquery_auth_refuses_missing_client_email() {
        let rec = record("act", "bigquery", "bq:up");
        let doc = serde_json::json!({
            "authentication": {"type": "service_account_json", "service_account_json": "{\"private_key\":\"pk\"}"}
        });
        let error =
            bigquery_auth(&doc, &rec.connector, &rec.id).expect_err("missing client_email refuses");
        assert!(
            error.contains("authentication.service_account_json"),
            "names the field: {error}"
        );
    }

    #[cfg(feature = "bigquery")]
    #[test]
    fn bigquery_auth_refuses_missing_private_key() {
        let rec = record("act", "bigquery", "bq:up");
        let doc = serde_json::json!({
            "authentication": {"type": "service_account_json", "service_account_json": "{\"client_email\":\"a@b\"}"}
        });
        let error =
            bigquery_auth(&doc, &rec.connector, &rec.id).expect_err("missing private_key refuses");
        assert!(
            error.contains("authentication.service_account_json"),
            "names the field: {error}"
        );
    }

    #[cfg(feature = "bigquery")]
    #[test]
    fn bigquery_auth_accepts_valid_service_account() {
        let rec = record("act", "bigquery", "bq:up");
        let doc = serde_json::json!({
            "authentication": {"type": "service_account_json", "service_account_json": "{\"client_email\":\"a@b\",\"private_key\":\"pk\",\"project_id\":\"proj\"}"}
        });
        let (project_id, _auth) =
            bigquery_auth(&doc, &rec.connector, &rec.id).expect("valid account builds");
        assert_eq!(project_id, "proj");
    }
}
