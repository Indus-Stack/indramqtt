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
//! `webhook`/`http` pair and the ten MV-02b time-series kinds build
//! their sinks here. The ten kinds are `clickhouse`, `tdengine`,
//! `greptimedb`, `iotdb`, `opentsdb`, `doris`, `datalayers`,
//! `influxdb`, `timestream` and `cassandra`. Every other action kind
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
        if opts.health_check_interval_jitter != Duration::ZERO
            || opts.health_check_timeout != Some(DEFAULT_HEALTH_CHECK_TIMEOUT)
        {
            // TODO(parity): the probe uses the documented timeout and no
            // jitter. The rulebook does not decide a per-connector jitter
            // or a timeout outside the documented default. Refuse those
            // non-default values instead of ignoring them.
            return Err(format!(
                "action {id:?} field `resource_opts.health_check_interval_jitter` / `health_check_timeout` is not supported: this action runtime uses the documented default for those fields"
            ));
        }
        if let Some(ttl) = opts.request_ttl {
            // Reason: a batch wait longer than half the request TTL would
            // let a low-rate batch expire before its first send. The
            // reference caps the wait at half the TTL.
            let cap = ttl / 2;
            if opts.batch_time > cap {
                opts.batch_time = cap;
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
        } else if self.health_check_timeout != Some(DEFAULT_HEALTH_CHECK_TIMEOUT) {
            Some("health_check_timeout")
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
        _ => kind,
    }
}

/// True when the action kind builds an owned sink here. The
/// documented `http` action kind and the historic `webhook` base kind
/// share one builder; `pgsql` covers `postgres`-filed rows; each
/// MV-02b alias (for example `aws_timestream`) runs on the builder of
/// its canonical kind.
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
            | "mongodb"
            | "couchbase"
            | "elasticsearch"
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

impl RuntimeBuffer {
    fn take(&mut self, count: usize) -> Vec<QueuedMessage> {
        let count = count.min(self.rows.len());
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(row) = self.rows.pop_front() {
                self.bytes = self
                    .bytes
                    .saturating_sub(row_size(&row.topic, &row.payload));
                out.push(row);
            }
        }
        out
    }
}

fn row_size(topic: &Topic, payload: &Bytes) -> usize {
    topic.as_str().len().saturating_add(payload.len())
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
        Self {
            healthy: AtomicBool::new(true),
            reason: Mutex::new(None),
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
    flush_gate: tokio::sync::Mutex<()>,
    inflight: Arc<tokio::sync::Semaphore>,
    health: ActionHealth,
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
            flush_gate: tokio::sync::Mutex::new(()),
            health: ActionHealth::new(),
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

async fn probe_target(target: &str, timeout: Option<Duration>) -> Result<(), String> {
    let dial = if target.contains(':') {
        target.to_string()
    } else {
        format!("{target}:80")
    };
    match timeout {
        Some(limit) => {
            match tokio::time::timeout(limit, tokio::net::TcpStream::connect(&dial)).await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(e)) => Err(format!("Connection refused to {dial}: {e}")),
                Err(_) => Err(format!("health check timed out for {dial}")),
            }
        }
        None => match tokio::net::TcpStream::connect(&dial).await {
            Ok(_) => Ok(()),
            Err(e) => Err(format!("Connection refused to {dial}: {e}")),
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
                let take = if stale || full {
                    buffer.rows.len().min(self.opts.batch_size)
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

    async fn dispatch_batch(
        self: &Arc<Self>,
        mut batch: Vec<QueuedMessage>,
    ) -> broker_connectors::Result<()> {
        let _gate = self.flush_gate.lock().await;
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

#[async_trait::async_trait]
impl Sink for ActionRuntimeSink {
    async fn send(
        &self,
        topic: &Topic,
        payload: &Bytes,
        qos: QoS,
    ) -> broker_connectors::Result<()> {
        let now = Instant::now();
        let size = row_size(topic, payload);
        let mut overflowed = false;
        {
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
                    .saturating_sub(row_size(&row.topic, &row.payload));
                sub_saturating(&self.state.metrics.queuing, 1);
                self.state.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                self.state
                    .metrics
                    .dropped_queue_full
                    .fetch_add(1, Ordering::Relaxed);
            }
            if buffer.bytes.saturating_add(size) > self.state.opts.max_buffer_bytes {
                overflowed = true;
            } else {
                buffer.bytes = buffer.bytes.saturating_add(size);
                buffer.rows.push_back(QueuedMessage {
                    topic: topic.clone(),
                    payload: payload.clone(),
                    qos,
                    enqueued: now,
                });
                self.state.metrics.queuing.fetch_add(1, Ordering::Relaxed);
            }
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

    fn health(&self) -> Option<SinkHealth> {
        Some(self.state.health.snapshot())
    }

    fn kind(&self) -> &'static str {
        self.state.inner.kind()
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
/// - Everything else (missing action field, missing connector field,
///   invalid parameter) is a build failure: the caller REFUSES with
///   the returned message, which names the action and the field.
pub(crate) fn stage_action_sink(
    connectors: &ConnectorStore,
    record: &ActionRecord,
) -> Result<ActionSinkStage, String> {
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
        });
    }
    required_action_parameters(record)?;
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
        return Ok(ActionSinkStage {
            sink: None,
            wiring: ActionWiring::Failed(format!(
                "connector {reference:?} of the action not found (field `connector`)"
            )),
            resource_opts,
            health_target: None,
        });
    };
    let sink = build_action_sink(record, &doc, &resource_opts)?;
    let health_target = if sink.is_some() {
        super::rules::extract_target_host_port(&doc)
    } else {
        None
    };
    Ok(ActionSinkStage {
        wiring: if sink.is_some() {
            ActionWiring::Live
        } else {
            ActionWiring::None
        },
        sink,
        resource_opts,
        health_target,
    })
}

/// Build (never register) one action's owned sink from the referenced
/// connector document plus this action's own parameters. `Ok(None)`
/// marks a kind with no action-owned runtime. Every refusal names the
/// action and the offending field, so a refused create or update can
/// carry the reason straight to the operator.
pub(crate) fn build_action_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
    opts: &ActionResourceOpts,
) -> Result<Option<Arc<dyn broker_connectors::Sink>>, String> {
    match canonical_action_kind(record.action_type.as_str()) {
        "pgsql" => build_pgsql_sink(record, connector_doc, opts).map(Some),
        "webhook" | "http" => build_webhook_sink(record, connector_doc).map(Some),
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

/// Check the action-owned value shapes before the connector is
/// resolved. Presence is checked later against the merged view (the
/// action parameter, else the connector document), so a dangling
/// connector still reports the dangling reference first. Thus a
/// dangling connector does not accept an action that is missing its
/// own required field.
fn required_action_parameters(record: &ActionRecord) -> Result<(), String> {
    if record.action_type == "pgsql" && param_string(record, "sql").is_none() {
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
        _ => {}
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
/// - `connection_url`: `postgres://{user}:{password}@{server}/{db}`
///   from the connector document. No invented fallback: a missing
///   `server`, `database` or `username` fails the build (fail closed,
///   S1-01) and the write that introduced it is refused with the
///   field named.
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
    let password =
        first_string(connector_doc, &["password"]).unwrap_or(std::borrow::Cow::Borrowed(""));
    let server = clean_server(&raw_server);
    if server.is_empty() {
        return Err(named("server"));
    }
    let conn_url = format!(
        "postgres://{}:{}@{}/{}",
        username, password, server, database
    );
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
    let transport = broker_connectors::postgres::DriverPgTransport::new(&conn_url, pool_size)
        .map_err(|e| {
            format!(
                "action {:?} cannot build its pgsql transport: {e}",
                record.id
            )
        })?;
    let config = broker_connectors::postgres::PostgreSqlSinkConfig {
        connection_url: conn_url,
        sql_template: sql,
        pool_size,
        batch_size,
        batch_timeout_ms,
    };
    let sink = broker_connectors::postgres::PostgreSqlSink::new(config, Arc::new(transport))
        .map_err(|e| format!("action {:?} cannot build its pgsql sink: {e}", record.id))?;
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
            let value = reqwest::header::HeaderValue::from_str(text)
                .map_err(|_| format!("action {:?} header value {text:?} is invalid", record.id))?;
            headers.insert(name, value);
        }
    }
    Ok(Arc::new(broker_connectors::HttpWebhookSink::new(
        target,
        headers,
        reqwest::Client::new(),
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
    let password = first_string(connector_doc, &["password"])
        .map(|value| value.into_owned())
        .unwrap_or_default();
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
    let bucket = connector_field(connector_doc, &connector, &record.id, &["bucket"], "bucket")?;
    let org = connector_field(connector_doc, &connector, &record.id, &["org"], "org")?;
    let token = connector_field(connector_doc, &connector, &record.id, &["token"], "token")?;
    let measurement = merged_field(
        record,
        connector_doc,
        "measurement",
        &["measurement", "measurement_template"],
        "measurement",
    )?;
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
        .map(|value| value.into_owned())
        .unwrap_or_else(|| "127.0.0.1:4001".to_string());
    let endpoint = if raw_server.contains("://") {
        raw_server.clone()
    } else if raw_server.contains(':') {
        format!("http://{raw_server}")
    } else {
        format!("http://{raw_server}:8086")
    };
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
/// - Rows are never aligned (`is_aligned` false, the reference
///   default).
/// - Batching belongs to the action runtime, not this driver. The
///   inner sink keeps one row per send with a short linger, so the
///   runtime owns the batch size and the batch timer.
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
    let config = IotDbSinkConfig {
        endpoint: iotdb_endpoint(&raw_endpoint),
        device_path_template: device,
        auth: IotDbAuth { username, password },
        is_aligned: false,
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
    let password = first_string(connector_doc, &["password"])
        .map(|value| value.into_owned())
        .unwrap_or_default();
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
// TODO(parity): should datalayers honor a precision parameter, or is
// the microsecond wire precision fixed? The current choice always
// stamps microseconds until the checker pins it.
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
    let raw_server = first_string(connector_doc, &["server", "url", "endpoint"])
        .map(|value| value.into_owned())
        .unwrap_or_else(|| "127.0.0.1:8361".to_string());
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
    let config = DatalayersConfig {
        endpoint: http_endpoint(&raw_server),
        database,
        table,
        auth_token,
        username,
        password,
        timestamp_field: None,
        tag_columns: Vec::new(),
        field_columns: Vec::new(),
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
    let transport =
        HttpTimestreamTransport::new(&config, reqwest::Client::new()).map_err(|error| {
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
        first_string_nested(connector_doc, &["id", "doc_id_template"]).map(|v| v.into_owned())
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
    let auth = match (username, password) {
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
) {
    for record in actions.snapshot() {
        let key = broker_config::v5_action_sink_key(&record.id);
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
            if engine.connectors().get(&key).is_some() {
                engine.connectors().unregister(&key);
            }
            actions.set_wiring(&record.id, ActionWiring::None);
            alarms.deactivate(&format!("action_build_failed:{}", record.id));
            continue;
        }
        let stage = match stage_action_sink(connectors, &record) {
            Ok(stage) => stage,
            Err(message) => {
                set_failed_wiring(engine, actions, alarms, &record, message);
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
                    engine.connectors().register(key, sink);
                }
                actions.set_wiring(&record.id, ActionWiring::Live);
                alarms.deactivate(&format!("action_build_failed:{}", record.id));
            }
            ActionWiring::Failed(message) => {
                set_failed_wiring(engine, actions, alarms, &record, message);
            }
            ActionWiring::None => {
                // No runtime for this kind (or the reference dangles
                // with no resolution): drop a stale registration so
                // the key stops claiming delivery.
                if engine.connectors().get(&key).is_some() {
                    engine.connectors().unregister(&key);
                }
                actions.set_wiring(&record.id, ActionWiring::None);
                alarms.deactivate(&format!("action_build_failed:{}", record.id));
            }
        }
    }
}

/// Fail one action's wiring into the stored, alarm-raising, honest
/// `disconnected` state: drop the key, keep the record readable with
/// the reason and raise `action_build_failed:{id}`.
fn set_failed_wiring(
    engine: &broker_rules::RuleEngine,
    actions: &ActionStore,
    alarms: &crate::v5::alarms::AlarmStore,
    record: &ActionRecord,
    message: String,
) {
    let key = broker_config::v5_action_sink_key(&record.id);
    if engine.connectors().get(&key).is_some() {
        engine.connectors().unregister(&key);
    }
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
            ..record(id, "http", connector)
        }
    }

    #[tokio::test]
    async fn stage_non_owned_kind_wires_nothing() {
        let connectors = ConnectorStore::new();
        let stage = stage_action_sink(&connectors, &record("act", "mqtt_bridge", "mqtt:up"))
            .expect("staging a kind with no runtime stages nothing");
        assert!(stage.sink.is_none());
        assert_eq!(stage.wiring, ActionWiring::None);
    }

    #[tokio::test]
    async fn stage_missing_connector_field_refuses_the_write() {
        let connectors = ConnectorStore::new();
        let error = stage_action_sink(&connectors, &record("act", "http", ""))
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
        let stage = stage_action_sink(&connectors, &record("act", "http", "webhook:gone-away"))
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
        assert!(!action_owned_kind("mqtt_bridge"));
        assert!(!action_owned_kind("kafka"));
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
            (
                serde_json::json!({"health_check_timeout": "30s"}),
                "health_check_timeout",
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
        let inner = Arc::new(BatchSink::default());
        let opts = ActionResourceOpts {
            batch_size: 100,
            batch_time: Duration::from_secs(10),
            max_buffer_bytes: 11,
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
        assert_eq!(snapshot.dropped, 2);
        assert_eq!(snapshot.dropped_queue_full, 2);
        assert_eq!(snapshot.queuing, 1);
        assert!(inner
            .batches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty());
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
        while metrics.snapshot().dropped == 0 && Instant::now() < deadline {
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
}
