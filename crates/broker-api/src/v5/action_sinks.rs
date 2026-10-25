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
//! Bound (stated here and by construction): `pgsql` and the
//! `webhook`/`http` pair build their sinks here (the two kinds the
//! family ticket fully corrects). Every other action kind owns no
//! runtime yet: it stores opaquely, registers nothing, reads
//! `disconnected` with a reason, and a fire counts one `failed`
//! delivery against its per-action metrics. The kind families accept
//! the same surface once they plot their data paths (KC-00).
//!
//! A build never opens a connection. The `pgsql` transport is lazy (the
//! first send dials), the `webhook` client is constructed per rule body.
//! The health worker dials the target on its configured interval.
//! Persistence failures keep the old generation on every write path that
//! uses this module.

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

/// True when the action kind builds an owned sink here. The
/// documented `http` action kind and the historic `webhook` base kind
/// share one builder; `pgsql` covers `postgres`-filed rows.
pub(crate) fn action_owned_kind(kind: &str) -> bool {
    matches!(kind, "pgsql" | "webhook" | "http")
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
    match record.action_type.as_str() {
        "pgsql" => build_pgsql_sink(record, connector_doc, opts).map(Some),
        "webhook" | "http" => build_webhook_sink(record, connector_doc).map(Some),
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

/// Check the action-owned required parameters before the connector is
/// resolved. Thus a dangling connector does not accept an action that
/// is missing its own required field.
fn required_action_parameters(record: &ActionRecord) -> Result<(), String> {
    if record.action_type == "pgsql" && param_string(record, "sql").is_none() {
        return Err(format!(
            "action {:?} is missing required field: sql (field `parameters.sql`)",
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
