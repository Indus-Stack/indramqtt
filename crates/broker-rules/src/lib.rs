#![allow(future_incompatible)]

use async_trait::async_trait;
use broker_connectors::ConnectorManager;
use broker_protocol::{QoS, Topic, TopicFilter};
use bytes::Bytes;
use parking_lot::RwLock;
use rekuiper_core::{RuleCounters, StreamBus, StreamReceiver, StreamSender};
use rekuiper_sql::{Evaluator, Expr, SelectStmt, TimeUnit, WindowDef};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::{mpsc, Mutex};

/// Rule-engine settings store: validated structs, registry persistence and
/// the subscriber callback feeding the fire-path snapshot (W3-10).
pub mod engine_config;
/// Rule-event producer registry behind `GET /rule_events` (W3-11, C06-16):
/// the single catalogue of event types the kernel actually produces.
pub mod events;
/// Built-in rule functions of the reference API (`republish`, `console`):
/// parser, validator, read-route projection and the template renderer.
pub mod functions;
/// Bounded on-disk spill buffer behind [`BackpressurePolicy::SpillToDisk`].
pub mod spill;
/// Standard rule SQL dialect front end: parser plus lowering onto the
/// streaming engine's AST (one engine, one fast path).
pub mod std_sql;
/// Standard rule-dialect built-in functions (RQ-03): inventory in
/// `FUNCTIONS.md`, implementations registered as user functions on the
/// streaming engine's public registry. No engine source is changed.
pub mod stdlib;

/// RX-01 embeddable rule-task runtime: broker rules on rekuiper's runtime
/// (bounded [`rekuiper_core::StreamBus`] feed, per-rule tasks, sink bridge).
/// Bounds, the ingress [`rekuiper_core::StreamRecord`] conversion and the
/// [`runtime::RuleOutputSink`] bridge trait live here.
pub mod runtime;

/// The streaming-SQL function catalog (name, category, aggregate flag,
/// arity, example) served to the dashboard SQL studio.
pub use rekuiper_sql::FunctionMeta;
// TODO(parity): Do sequence_match and sequence_count work through the broker?
// The engine 1.0.0-rc1 adds them. No broker test drives them yet.
// Hide them until a broker test proves them. This keeps the catalog at 219.
pub fn builtin_function_metadata() -> &'static [FunctionMeta] {
    static FILTERED: std::sync::LazyLock<Vec<FunctionMeta>> = std::sync::LazyLock::new(|| {
        rekuiper_sql::builtin_function_metadata()
            .iter()
            .filter(|meta| meta.name != "sequence_match" && meta.name != "sequence_count")
            .cloned()
            .collect()
    });
    FILTERED.as_slice()
}

pub use functions::{
    action_to_api_json, console_text, parse_function_action, render_republish,
    republish_args_from_json, validate_republish_args, FunctionActionError, InputMessage,
    RepublishMessage, RepublishProperties, CONSOLE_LINE_LIMIT, MAX_REPUBLISH_PROPERTY_LEN,
    MAX_REPUBLISH_USER_PROPERTIES,
};

pub use engine_config::{EngineConfigStore, EngineConfigSubscriber, EngineConfigUpdateError};
use spill::SpillLog;
pub use spill::{
    SpillConfig, SpillOutcome, SpillStats, DEFAULT_SPILL_MAX_BYTES, DEFAULT_SPILL_SEGMENT_MAX_BYTES,
};
pub use std_sql::{ForeachPlan, StdParseError, StdRule, StdSource};

/// Event source identifiers served by the rule engine (RQ-04).
/// The kernel fires each source at the point the event happens.
/// A rule names the source in its FROM clause
/// (for example "$events/client_connected").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventSource {
    ClientConnected,
    ClientDisconnected,
    ClientConnack,
    ClientCheckAuthzComplete,
    SessionSubscribed,
    SessionUnsubscribed,
    MessageDelivered,
    MessageAcked,
    MessageDropped,
    DeliveryDropped,
}

impl EventSource {
    /// The canonical source name used in FROM clauses.
    pub const fn as_str(&self) -> &'static str {
        match self {
            EventSource::ClientConnected => "$events/client_connected",
            EventSource::ClientDisconnected => "$events/client_disconnected",
            EventSource::ClientConnack => "$events/client_connack",
            EventSource::ClientCheckAuthzComplete => "$events/client_check_authz_complete",
            EventSource::SessionSubscribed => "$events/session_subscribed",
            EventSource::SessionUnsubscribed => "$events/session_unsubscribed",
            EventSource::MessageDelivered => "$events/message_delivered",
            EventSource::MessageAcked => "$events/message_acked",
            EventSource::MessageDropped => "$events/message_dropped",
            EventSource::DeliveryDropped => "$events/delivery_dropped",
        }
    }

    /// The canonical list of all event sources served by this broker. This
    /// array is the single source of truth for the variant count: [`COUNT`]
    /// and every caller of [`all`] derive from it, so adding a source grows
    /// the per-source flags array in one place instead of a second magic
    /// number.
    const ALL: &'static [EventSource] = &[
        EventSource::ClientConnected,
        EventSource::ClientDisconnected,
        EventSource::ClientConnack,
        EventSource::ClientCheckAuthzComplete,
        EventSource::SessionSubscribed,
        EventSource::SessionUnsubscribed,
        EventSource::MessageDelivered,
        EventSource::MessageAcked,
        EventSource::MessageDropped,
        EventSource::DeliveryDropped,
    ];

    /// Number of event sources served by this broker (the length of
    /// [`ALL`]). Used to size the per-source `event_flags` array so its
    /// length is derived from the source inventory rather than a hardcoded
    /// count.
    const COUNT: usize = Self::ALL.len();

    /// All event sources served by this broker.
    pub fn all() -> &'static [EventSource] {
        Self::ALL
    }

    /// Parse a source name from a rule's FROM clause.
    pub fn parse(name: &str) -> Option<Self> {
        EventSource::all()
            .iter()
            .find(|s| s.as_str() == name)
            .copied()
    }

    /// Field names this event source provides in the rule record.
    /// Used by the rule-events endpoint to document available fields.
    pub fn fields(&self) -> &'static [&'static str] {
        match self {
            EventSource::ClientConnected => &[
                "clientid",
                "username",
                "peerhost",
                "keepalive",
                "proto_ver",
                "clean_start",
                "conn_ack_code",
            ],
            EventSource::ClientDisconnected => &[
                "clientid",
                "username",
                "peerhost",
                "reason",
                "disconnect_code",
            ],
            EventSource::ClientConnack => &[
                "clientid",
                "username",
                "peerhost",
                "return_code",
                "session_present",
            ],
            EventSource::ClientCheckAuthzComplete => &[
                "clientid", "username", "peerhost", "action", "topic", "qos", "retain", "allow",
                "source",
            ],
            EventSource::SessionSubscribed => &[
                "clientid", "username", "peerhost", "topic", "qos", "nl", "rap", "rh",
            ],
            EventSource::SessionUnsubscribed => &["clientid", "username", "peerhost", "topic"],
            EventSource::MessageDelivered => &[
                "clientid", "username", "peerhost", "topic", "qos", "retain", "payload", "msg_id",
            ],
            EventSource::MessageAcked => {
                &["clientid", "username", "peerhost", "topic", "qos", "msg_id"]
            }
            EventSource::MessageDropped => {
                &["clientid", "username", "peerhost", "topic", "qos", "reason"]
            }
            EventSource::DeliveryDropped => {
                &["clientid", "username", "peerhost", "topic", "qos", "reason"]
            }
        }
    }
}

/// Information about an event source for the rule-events endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventSourceInfo {
    /// The canonical source name (e.g., "$events/client_connected").
    pub name: String,
    /// Field names available in the event record.
    pub fields: Vec<String>,
}

#[derive(Error, Debug)]
pub enum RuleEngineError {
    #[error("Event input buffer overflow: {0:?}")]
    Overflow(OverflowReason),

    #[error("Rule execution failed: {0}")]
    Execution(String),

    #[error("Invalid rule: {0}")]
    InvalidRule(String),

    /// The in-memory mutation applied but the registry commit or atomic
    /// save failed. Callers map this to 500; it is never silent.
    #[error("cannot persist rules: {0}")]
    Persist(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OverflowReason {
    DroppedNewest,
    DroppedOldest,
    BufferFull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackpressurePolicy {
    Block,
    DropNewest,
    DropOldest,
    SpillToDisk,
    RejectPublisher,
}

// Map a policy to a small number for lock-free storage.
fn backpressure_policy_to_u8(policy: BackpressurePolicy) -> u8 {
    match policy {
        BackpressurePolicy::Block => 0,
        BackpressurePolicy::DropNewest => 1,
        BackpressurePolicy::DropOldest => 2,
        BackpressurePolicy::SpillToDisk => 3,
        BackpressurePolicy::RejectPublisher => 4,
    }
}

// Map a stored number back to a policy. Unknown values read as DropOldest.
fn backpressure_policy_from_u8(value: u8) -> BackpressurePolicy {
    match value {
        0 => BackpressurePolicy::Block,
        1 => BackpressurePolicy::DropNewest,
        3 => BackpressurePolicy::SpillToDisk,
        4 => BackpressurePolicy::RejectPublisher,
        _ => BackpressurePolicy::DropOldest,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushOutcome {
    Enqueued,
    Dropped(OverflowReason),
    SpilledToDisk(u64),
}

#[derive(Debug, Clone)]
pub struct StreamEvent {
    pub topic: Topic,
    pub payload: Bytes,
    pub timestamp_millis: i64,
    pub client_id: Option<String>,
}

impl StreamEvent {
    pub fn new(topic: Topic, payload: Bytes) -> Self {
        let timestamp_millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        Self {
            topic,
            payload,
            timestamp_millis,
            client_id: None,
        }
    }
}

#[async_trait]
pub trait EventInput: Send + Sync {
    async fn push(&self, event: StreamEvent) -> Result<PushOutcome, RuleEngineError>;
}

/// One ordered message-transformation step (W3-32): the live form of one
/// chain entry. `topics` holds the parsed filters the step applies to;
/// `operations` holds the ordered field sets; `failure_action` selects
/// the behaviour when a `topic` set produces an invalid topic (`drop`
/// and `disconnect` both shed the message, `ignore` keeps the previous
/// topic). Disabled steps are skipped in order.
#[derive(Debug, Clone)]
pub struct TransformStep {
    /// Chain key; unique within the chain.
    pub name: String,
    /// Filters the step applies to, in stored order.
    pub topics: Vec<TopicFilter>,
    /// Failure behaviour (`drop`, `disconnect` or `ignore`).
    pub failure_action: String,
    /// Whether the step participates.
    pub enable: bool,
    /// Ordered field sets applied in order on matching messages.
    pub operations: Vec<TransformOperation>,
}

/// One field set inside a step: set `key` (`payload` or `topic`) to the
/// template `value` (`${payload}` and `${topic}` expand to the current
/// values).
#[derive(Debug, Clone)]
pub struct TransformOperation {
    /// Message field to set.
    pub key: String,
    /// Template for the new value.
    pub value: String,
}

/// Render one operation template: `${topic}` expands to the current
/// topic name and `${payload}` expands to the current payload text.
/// Pure string work; no lock, no I/O.
fn render_transform_template(value: &str, topic: &str, payload: &str) -> String {
    // `${topic}` first so a payload carrying the topic marker still
    // expands once per marker kind.
    value
        .replace("${topic}", topic)
        .replace("${payload}", payload)
}

/// Run one ingress through the ordered chain snapshot (W3-32).
/// Returns the (possibly rewritten) topic and payload plus whether the
/// message is shed (`true` means drop it: no retain, no rules, no
/// fan-out). Steps apply in chain order; each matching enabled step
/// applies its operations in order. Pure function over the snapshot:
/// no lock. The hot path skips this call when the snapshot is empty,
/// so the clone below never runs per ingress; non-empty ingress
/// clones once then allocates only for rewritten values.
pub fn apply_transform_steps(
    steps: &[TransformStep],
    topic: &Topic,
    payload: &Bytes,
) -> (Topic, Bytes, bool) {
    if steps.is_empty() {
        return (topic.clone(), payload.clone(), false);
    }
    let mut cur_topic = topic.clone();
    let mut cur_payload = payload.clone();
    for step in steps {
        if !step.enable {
            continue;
        }
        if !step.topics.iter().any(|filter| filter.matches(&cur_topic)) {
            continue;
        }
        for op in &step.operations {
            if op.key == "payload" {
                let payload_text = String::from_utf8_lossy(&cur_payload).into_owned();
                let rendered =
                    render_transform_template(&op.value, cur_topic.as_str(), &payload_text);
                cur_payload = Bytes::from(rendered.into_bytes());
            } else if op.key == "topic" {
                let payload_text = String::from_utf8_lossy(&cur_payload).into_owned();
                let rendered =
                    render_transform_template(&op.value, cur_topic.as_str(), &payload_text);
                match Topic::new(rendered) {
                    Ok(next) => cur_topic = next,
                    Err(_) => {
                        // Fail closed: an invalid rewritten topic never
                        // fans out. `ignore` keeps the previous topic and
                        // continues; `drop` and `disconnect` shed the
                        // message. A transform step sheds the message; it
                        // cannot close the connection itself - the session
                        // closes a connection on its own bind path, never
                        // from a transform step.
                        if step.failure_action == "ignore" {
                            continue;
                        }
                        return (cur_topic, cur_payload, true);
                    }
                }
            }
        }
    }
    (cur_topic, cur_payload, false)
}

/// Run one ingress through the chain and attribute per-name counters.
///
/// Same rewrite as [`apply_transform_steps`], plus lock-free bumps
/// through `metrics`. A step bumps `matched` when it runs (enabled
/// plus filter hit), then bumps `succeeded` or `failed`. Skipped steps
/// (disabled or no filter hit) bump nothing. A shed stops the chain.
/// Steps without a record (standalone engine, boot race) run without
/// bumps. The probe path uses the pure form and never calls this.
/// No lock, no queue, no per-fire allocation beyond lookups and bumps.
pub fn apply_transform_steps_with_metrics(
    steps: &[TransformStep],
    metrics: &HashMap<String, Arc<TransformMetrics>>,
    topic: &Topic,
    payload: &Bytes,
) -> (Topic, Bytes, bool) {
    if steps.is_empty() {
        return (topic.clone(), payload.clone(), false);
    }
    let mut cur_topic = topic.clone();
    let mut cur_payload = payload.clone();
    for step in steps {
        if !step.enable {
            continue;
        }
        if !step.topics.iter().any(|filter| filter.matches(&cur_topic)) {
            continue;
        }
        let record = metrics.get(&step.name);
        // Count the run before the operations apply.
        if let Some(record) = record {
            record.matched.fetch_add(1, Ordering::Relaxed);
        }
        let mut shed_here = false;
        for op in &step.operations {
            if op.key == "payload" {
                let payload_text = String::from_utf8_lossy(&cur_payload).into_owned();
                let rendered =
                    render_transform_template(&op.value, cur_topic.as_str(), &payload_text);
                cur_payload = Bytes::from(rendered.into_bytes());
            } else if op.key == "topic" {
                let payload_text = String::from_utf8_lossy(&cur_payload).into_owned();
                let rendered =
                    render_transform_template(&op.value, cur_topic.as_str(), &payload_text);
                match Topic::new(rendered) {
                    Ok(next) => cur_topic = next,
                    Err(_) => {
                        // Fail closed: an invalid rewritten topic never
                        // fans out. `ignore` keeps the previous topic and
                        // continues; `drop` and `disconnect` shed the
                        // message. A transform step sheds the message; it
                        // cannot close the connection itself - the session
                        // closes a connection on its own bind path, never
                        // from a transform step.
                        if step.failure_action == "ignore" {
                            continue;
                        }
                        shed_here = true;
                        break;
                    }
                }
            }
        }
        if shed_here {
            if let Some(record) = record {
                record.failed.fetch_add(1, Ordering::Relaxed);
            }
            return (cur_topic, cur_payload, true);
        }
        if let Some(record) = record {
            record.succeeded.fetch_add(1, Ordering::Relaxed);
        }
    }
    (cur_topic, cur_payload, false)
}

/// Direct in-memory sink for rules to publish messages back into the broker
/// without paying the cost of establishing an external loopback network connection.
#[async_trait]
pub trait BrokerSink: Send + Sync {
    async fn publish(
        &self,
        topic: Topic,
        payload: Bytes,
        qos: QoS,
        retain: bool,
    ) -> Result<(), RuleEngineError>;
    /// Publish one derived message inside one tenant. The default
    /// impl keeps the old default-tenant path so test sinks stay
    /// valid. Production sinks fan out in the named tenant.
    async fn publish_in_tenant(
        &self,
        tenant: &str,
        topic: Topic,
        payload: Bytes,
        qos: QoS,
        retain: bool,
    ) -> Result<(), RuleEngineError> {
        let _ = tenant;
        self.publish(topic, payload, qos, retain).await
    }
    /// Publish one message that a `republish` function rendered, inside
    /// one tenant. The message may carry MQTT v5 properties. The default
    /// impl drops the properties and calls
    /// [`BrokerSink::publish_in_tenant`]; a sink that can carry them
    /// overrides this.
    async fn publish_message_in_tenant(
        &self,
        tenant: &str,
        message: RepublishMessage,
    ) -> Result<(), RuleEngineError> {
        self.publish_in_tenant(
            tenant,
            message.topic,
            message.payload,
            message.qos,
            message.retain,
        )
        .await
    }
}

/// Bounded in-memory event queue implementing [`EventInput`].
///
/// `Block` waits for capacity inside `push`; `DropNewest` discards the
/// incoming event when full; `DropOldest` evicts the oldest queued event
/// to make room. `RejectPublisher` reports overflow as an error instead
/// of queueing. `SpillToDisk` appends overflow events to the bounded
/// on-disk spill buffer ([`spill::SpillLog`]) instead of erroring, and
/// the consumer replays them in order once the pressure eases; without
/// a spill directory configured (plain [`BoundedEventInput::new`]) it
/// refuses loudly like `RejectPublisher` and counts the drop, failing
/// closed rather than pretending to be durable.
pub struct BoundedEventInput {
    tx: mpsc::Sender<StreamEvent>,
    rx: Mutex<mpsc::Receiver<StreamEvent>>,
    // Live backpressure policy. Reason: boot applies the file value
    // without a restart. One relaxed load per push, no lock.
    policy: AtomicU8,
    /// Disk behind `SpillToDisk`, if one was configured. `None` for the
    /// other four policies and for directory-less `SpillToDisk` inputs.
    /// Held across the check-send-spill sequence so concurrent pushes
    /// keep one global order (memory first, then disk, sticky while a
    /// backlog exists). Steady-state `SpillToDisk` pushes pay one
    /// uncontended mutex; the other policies never touch it.
    spill: parking_lot::Mutex<Option<SpillLog>>,
    /// Lock-free spill-enabled flag for the live publish path: set once
    /// when a spill directory opens, never flipped back. `dispatch_ingress`
    /// checks this single relaxed load (no mutex) so the default
    /// memory-only engine pays no new lock on publish; only spill-backed
    /// engines take the push path below.
    spill_enabled: AtomicU64,
    /// Lock-free mirror of the on-disk backlog for tests and for the
    /// sticky-spill fast check. Maintained under the spill mutex
    /// wherever the log mutates (mirrors `inflight_spill.len()` the way
    /// the session spill count does).
    spill_backlog: AtomicU64,
    /// Outcome counters for the spill backend. Bumped on every spill,
    /// replay, refusal and recovery outcome so the behaviour is visible
    /// and never silent; mirrored into the node metrics when attached.
    spilled: AtomicU64,
    replayed: AtomicU64,
    spill_dropped: AtomicU64,
    torn_discarded: AtomicU64,
    spill_recovered: AtomicU64,
    /// Node metrics mirror for the counters above (`None` in unit
    /// tests and standalone state). Attached once via
    /// [`BoundedEventInput::set_metrics`]; the first attach also
    /// carries the counts accumulated so far.
    metrics: OnceLock<Arc<broker_observability::Metrics>>,
}

impl BoundedEventInput {
    pub fn new(capacity: usize, policy: BackpressurePolicy) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(capacity.max(1));
        Arc::new(Self {
            tx,
            rx: Mutex::new(rx),
            policy: AtomicU8::new(backpressure_policy_to_u8(policy)),
            spill: parking_lot::Mutex::new(None),
            spill_enabled: AtomicU64::new(0),
            spill_backlog: AtomicU64::new(0),
            spilled: AtomicU64::new(0),
            replayed: AtomicU64::new(0),
            spill_dropped: AtomicU64::new(0),
            torn_discarded: AtomicU64::new(0),
            spill_recovered: AtomicU64::new(0),
            metrics: OnceLock::new(),
        })
    }

    /// Spill-backed input: `SpillToDisk` policy with a bounded on-disk
    /// buffer behind the push path. Opening recovers the directory
    /// (replaying whatever a previous engine left), so recreating the
    /// input on the same directory survives a restart. A bad directory
    /// or config fails here, loudly, before anything can pretend to be
    /// durable.
    pub fn with_spill(capacity: usize, config: SpillConfig) -> io::Result<Arc<Self>> {
        let input = Self::new(capacity, BackpressurePolicy::SpillToDisk);
        let (log, outcome) = SpillLog::open(&config)?;
        input
            .spill_recovered
            .fetch_add(outcome.recovered, Ordering::Relaxed);
        input
            .torn_discarded
            .fetch_add(outcome.torn, Ordering::Relaxed);
        input.spill_backlog.store(log.backlog(), Ordering::Relaxed);
        input.spill_enabled.store(1, Ordering::Relaxed);
        *input.spill.lock() = Some(log);
        Ok(input)
    }

    /// Live backpressure policy. One relaxed load, no lock.
    pub fn policy(&self) -> BackpressurePolicy {
        backpressure_policy_from_u8(self.policy.load(Ordering::Relaxed))
    }

    /// Apply the file backpressure policy. Boot and management only.
    /// One relaxed store, no lock on the publish path.
    pub fn set_policy(&self, policy: BackpressurePolicy) {
        self.policy
            .store(backpressure_policy_to_u8(policy), Ordering::Relaxed);
    }

    /// Attach the node metrics mirror for the spill counters. The first
    /// attach wins and also carries the counts accumulated so far, so
    /// recovery outcomes (counted at open, before any attach) still
    /// reach the existing observability path exactly once.
    pub fn set_metrics(&self, metrics: &Arc<broker_observability::Metrics>) {
        if self.metrics.set(Arc::clone(metrics)).is_ok() {
            metrics.inc_rule_spill_spilled_by(self.spilled.load(Ordering::Relaxed));
            metrics.inc_rule_spill_replayed_by(self.replayed.load(Ordering::Relaxed));
            metrics.inc_rule_spill_dropped_by(self.spill_dropped.load(Ordering::Relaxed));
            metrics.inc_rule_spill_torn_by(self.torn_discarded.load(Ordering::Relaxed));
            metrics.inc_rule_spill_recovered_by(self.spill_recovered.load(Ordering::Relaxed));
        }
    }

    /// Current spill outcome counts (zeros when no spill backend is
    /// configured, apart from refusals, which are always counted).
    pub fn spill_stats(&self) -> SpillStats {
        SpillStats {
            spilled: self.spilled.load(Ordering::Relaxed),
            replayed: self.replayed.load(Ordering::Relaxed),
            dropped: self.spill_dropped.load(Ordering::Relaxed),
            torn_discarded: self.torn_discarded.load(Ordering::Relaxed),
            recovered: self.spill_recovered.load(Ordering::Relaxed),
        }
    }

    /// True while a spill directory is configured behind this input.
    pub fn has_spill(&self) -> bool {
        self.spill.lock().is_some()
    }

    /// Lock-free spill check for the live publish path: one relaxed
    /// atomic load, no mutex. The publish path (`dispatch_ingress`)
    /// uses this, never [`BoundedEventInput::has_spill`].
    pub fn spill_enabled(&self) -> bool {
        self.spill_enabled.load(Ordering::Relaxed) != 0
    }

    /// Events currently awaiting replay on disk.
    pub fn spill_backlog_len(&self) -> u64 {
        self.spill_backlog.load(Ordering::Relaxed)
    }

    /// Force the active spill segment to stable storage. The spill
    /// path itself never syncs (see [`spill`]); call this before an
    /// orderly shutdown or a restart handoff. No backend is a no-op.
    pub fn sync_spill(&self) -> io::Result<()> {
        let guard = self.spill.lock();
        if let Some(log) = guard.as_ref() {
            log.sync()?;
        }
        Ok(())
    }

    fn count_spilled(&self) {
        self.spilled.fetch_add(1, Ordering::Relaxed);
        if let Some(metrics) = self.metrics.get() {
            metrics.inc_rule_spill_spilled();
        }
    }

    fn count_replayed(&self) {
        self.replayed.fetch_add(1, Ordering::Relaxed);
        if let Some(metrics) = self.metrics.get() {
            metrics.inc_rule_spill_replayed();
        }
    }

    fn count_dropped(&self) {
        self.spill_dropped.fetch_add(1, Ordering::Relaxed);
        if let Some(metrics) = self.metrics.get() {
            metrics.inc_rule_spill_dropped();
        }
    }

    fn count_torn_by(&self, n: u64) {
        self.torn_discarded.fetch_add(n, Ordering::Relaxed);
        if let Some(metrics) = self.metrics.get() {
            metrics.inc_rule_spill_torn_by(n);
        }
    }

    /// Non-blocking enqueue attempt. `Block` never blocks here: a full
    /// buffer reports `WouldBlock`-style success value... see below.
    ///
    /// Publish-path cost (B4-07, measured by
    /// `test_spill_workload_timings`, which prints both rates into the
    /// gate log): steady-state (no pressure, no disk backlog) pays two
    /// relaxed atomic loads plus one `try_send` — no spill mutex, no
    /// allocation, no file I/O, identical to the other four policies.
    /// Only overflow (full memory, or a disk backlog while sticky) takes
    /// the spill mutex plus one page-cache `write + flush` (encode `Vec`
    /// allocs bounded by the event size and capped by
    /// `MAX_FRAME_BODY_BYTES`, never an `fsync`); refusals (cap, oversize,
    /// I/O error) fail closed and counted.
    pub fn try_push(&self, event: StreamEvent) -> Result<PushOutcome, RuleEngineError> {
        // Sticky-spill order: while a disk backlog exists every arrival
        // joins the disk (memory always predates it), so replay stays
        // memory-then-disk, oldest first. The backlog mirror is lock-free;
        // steady-state (`0`) skips the mutex entirely.
        if self.policy() == BackpressurePolicy::SpillToDisk
            && self.spill_backlog.load(Ordering::Relaxed) != 0
        {
            return self.spill_or_refuse(event);
        }
        match self.tx.try_send(event) {
            Ok(()) => Ok(PushOutcome::Enqueued),
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(RuleEngineError::Overflow(OverflowReason::BufferFull))
            }
            Err(mpsc::error::TrySendError::Full(event)) => self.apply_full_policy(event),
        }
    }

    fn apply_full_policy(&self, event: StreamEvent) -> Result<PushOutcome, RuleEngineError> {
        match self.policy() {
            // Synchronous callers cannot block: use `push().await` for
            // true blocking behaviour under the Block policy.
            BackpressurePolicy::Block => Ok(PushOutcome::Dropped(OverflowReason::BufferFull)),
            BackpressurePolicy::DropNewest => {
                Ok(PushOutcome::Dropped(OverflowReason::DroppedNewest))
            }
            BackpressurePolicy::DropOldest => {
                // Best-effort eviction without blocking: only a stale
                // receiver can defeat this, in which case the event drops.
                if let Ok(mut rx) = self.rx.try_lock() {
                    let _ = rx.try_recv();
                    match self.tx.try_send(event) {
                        Ok(()) => Ok(PushOutcome::Dropped(OverflowReason::DroppedOldest)),
                        Err(_) => Ok(PushOutcome::Dropped(OverflowReason::DroppedNewest)),
                    }
                } else {
                    Ok(PushOutcome::Dropped(OverflowReason::DroppedNewest))
                }
            }
            BackpressurePolicy::SpillToDisk => self.spill_or_refuse(event),
            BackpressurePolicy::RejectPublisher => {
                self.count_dropped();
                Err(RuleEngineError::Overflow(OverflowReason::BufferFull))
            }
        }
    }

    /// Overflow path for `SpillToDisk`: memory first, disk behind it,
    /// refusal only when there is nowhere to put the event. The spill
    /// mutex is held across the check-send-spill sequence so concurrent
    /// pushes keep one global order: while a disk backlog exists every
    /// arrival joins it (sticky spill), so replay order is always
    /// memory-then-disk, oldest first. The disk write is `write +
    /// flush` to the page cache, never an `fsync`: the live event path
    /// never blocks on stable storage.
    fn spill_or_refuse(&self, event: StreamEvent) -> Result<PushOutcome, RuleEngineError> {
        let mut spill = self.spill.lock();
        let Some(log) = spill.as_mut() else {
            // No disk behind the variant: refuse loudly and count the
            // drop. Fail closed rather than pretending to be durable.
            self.count_dropped();
            tracing::warn!(
                "rule input SpillToDisk overflow refused: no spill directory configured"
            );
            return Err(RuleEngineError::Overflow(OverflowReason::BufferFull));
        };
        let event = if log.backlog() == 0 {
            match self.tx.try_send(event) {
                Ok(()) => return Ok(PushOutcome::Enqueued),
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.count_dropped();
                    return Err(RuleEngineError::Overflow(OverflowReason::BufferFull));
                }
                Err(mpsc::error::TrySendError::Full(event)) => event,
            }
        } else {
            event
        };
        match log.spill(&event) {
            Ok(seq) => {
                self.spill_backlog.store(log.backlog(), Ordering::Relaxed);
                self.count_spilled();
                Ok(PushOutcome::SpilledToDisk(seq))
            }
            Err(e) => {
                self.spill_backlog.store(log.backlog(), Ordering::Relaxed);
                self.count_dropped();
                tracing::warn!(error = %e, "rule input spill refused: failing closed");
                Err(RuleEngineError::Overflow(OverflowReason::BufferFull))
            }
        }
    }

    /// Replay one spilled event under the spill mutex. Counters stay in
    /// sync with the log: the backlog mirror follows every mutation,
    /// replays and torn repairs are counted and mirrored.
    fn replay_one_locked(&self) -> Option<StreamEvent> {
        let mut spill = self.spill.lock();
        let log = spill.as_mut()?;
        let (event, torn) = log.replay_one();
        self.spill_backlog.store(log.backlog(), Ordering::Relaxed);
        if torn > 0 {
            self.count_torn_by(torn);
        }
        if event.is_some() {
            self.count_replayed();
        }
        event
    }

    /// Non-blocking dequeue: returns one event if immediately available
    /// in memory or on disk, otherwise `None`. Unlike [`next_event`],
    /// this never pends — used by `dispatch_ingress` to drain
    /// event-source events pushed by `fire_event` after every ingress.
    pub async fn try_dequeue(&self) -> Option<StreamEvent> {
        // Fast path: one short channel lock, released before any disk.
        {
            let mut rx = self.rx.lock().await;
            match rx.try_recv() {
                Ok(event) => return Some(event),
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    drop(rx);
                    return self.replay_one_locked();
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
            }
        }
        // Memory empty: one disk replay with no channel lock held.
        if let Some(event) = self.replay_one_locked() {
            return Some(event);
        }
        None
    }

    /// Take the next queued event (consumer side): memory first (it
    /// always predates the disk backlog under sticky spill), then one
    /// disk replay, then pend for the next memory arrival. A pending
    /// consumer cannot miss spilled events: pushes join the disk while
    /// a backlog exists, and every wake re-checks memory before disk.
    ///
    /// Locking: the async channel mutex is held only for the
    /// non-blocking `try_recv` and for the final pending `recv`; it is
    /// never held across disk I/O (`replay_one_locked` takes only the
    /// short sync spill mutex for a page-cache read plus at most one
    /// repair truncation). Steady-state consumers pay no disk work at
    /// all.
    pub async fn next_event(&self) -> Option<StreamEvent> {
        // Fast path: one short channel lock, released before any disk.
        {
            let mut rx = self.rx.lock().await;
            match rx.try_recv() {
                Ok(event) => return Some(event),
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    drop(rx);
                    return self.replay_one_locked();
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
            }
        }
        // Memory empty: one disk replay with no channel lock held.
        if let Some(event) = self.replay_one_locked() {
            return Some(event);
        }
        // Nothing anywhere: pend for the next memory arrival (channel
        // lock only across the sleep). A spill that lands while pending
        // wakes via the channel only when it also enqueued to memory;
        // sticky disk-only arrivals are picked up on the next wake by
        // re-checking memory before disk.
        let mut rx = self.rx.lock().await;
        // Re-check memory first: an arrival between the replay above and
        // this lock must not be overtaken by disk.
        match rx.try_recv() {
            Ok(event) => Some(event),
            Err(mpsc::error::TryRecvError::Disconnected) => {
                drop(rx);
                self.replay_one_locked()
            }
            Err(mpsc::error::TryRecvError::Empty) => match rx.recv().await {
                Some(event) => Some(event),
                None => {
                    drop(rx);
                    self.replay_one_locked()
                }
            },
        }
    }
}

#[async_trait]
impl EventInput for BoundedEventInput {
    async fn push(&self, event: StreamEvent) -> Result<PushOutcome, RuleEngineError> {
        match self.policy() {
            BackpressurePolicy::Block => self
                .tx
                .send(event)
                .await
                .map(|()| PushOutcome::Enqueued)
                .map_err(|_| RuleEngineError::Overflow(OverflowReason::BufferFull)),
            _ => self.try_push(event),
        }
    }
}

/// One native stream rule: match ingress by topic filter, run actions.
///
/// Open-core licensing tier of a rule, derived from its SQL at creation:
/// Any window clause makes a rule Enterprise. Plain `GROUP BY` with
/// no window also runs as a session window, so it is Enterprise.
/// Everything else is Community. Tier labels stay for display but never
/// select the execution path: every window rule runs on rekuiper's
/// runtime (see [`runtime`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RuleTier {
    Community,
    Enterprise,
}

/// `sql_query` is an optional streaming-SQL program parsed once at
/// creation into `parsed_query`. Community rules (no window clause) run
/// statelessly at ingress: matching JSON payloads are filtered (WHERE)
/// and projected (SELECT). Rules with a window clause run on rekuiper's
/// runtime: matching ingress is converted once to a
/// [`rekuiper_core::StreamRecord`], handed to the rule's task over the
/// bounded [`rekuiper_core::StreamBus`], and evaluated per window close by
/// that task. `parsed_query` is skipped
/// in JSON output: the wire form carries the source SQL string, which
/// re-parses on creation.
#[derive(Debug, Clone, Serialize)]
pub struct Rule {
    pub id: String,
    pub name: String,
    /// Free-form rule description from the management API. Empty when
    /// the request does not carry one.
    pub description: String,
    /// Rule metadata object from the management API. An empty object
    /// when the request does not carry one.
    pub metadata: serde_json::Value,
    /// Tenant that owns the rule. One rule serves one tenant. Rules
    /// stored before tenancy carry the default tenant.
    pub tenant: String,
    pub topic_filter: TopicFilter,
    pub sql_query: Option<String>,
    #[serde(skip_serializing)]
    pub parsed_query: Option<SelectStmt>,
    /// Standard-dialect expansion plan (`FOREACH ...`), if the rule was
    /// parsed by the standard front end. Consulted on the deliver path to
    /// expand one message into one row per array element.
    #[serde(skip_serializing)]
    pub std_foreach: Option<ForeachPlan>,
    /// Standard-dialect `FROM` sources as written. Consulted on the deliver
    /// path: `Topic` filters must match the ingress topic, `Event` sources
    /// match their own `$events/...` topic on the drain path (and match
    /// nothing on MQTT ingress, keeping event-only rules fail-closed),
    /// `Stream` sources constrain nothing. Empty for pre-front-end rules.
    #[serde(skip_serializing)]
    pub std_sources: Vec<StdSource>,
    /// Topic filters compiled from the `Topic` entries of `std_sources`.
    /// One entry per source, in the same order; non-topic sources hold
    /// `None`. Compiling here keeps the ingress path free of a per-publish
    /// `TopicFilter` parse.
    #[serde(skip_serializing)]
    pub std_topic_filters: Vec<Option<TopicFilter>>,
    /// True when the standard select list holds a top-level `*`. Consulted
    /// on the deliver path to strip injected message keys from the
    /// expansion so `SELECT *` keeps projecting exactly the payload fields.
    #[serde(skip_serializing)]
    pub std_has_wildcard: bool,
    /// Output keys named explicitly by the standard select list; these
    /// survive the wildcard strip.
    #[serde(skip_serializing)]
    pub std_explicit_keys: Vec<String>,
    pub tier: RuleTier,
    pub enabled: bool,
    pub actions: Vec<RuleAction>,
    #[serde(skip_serializing)]
    pub matched_cnt: Arc<AtomicU64>,
    #[serde(skip_serializing)]
    pub passed_cnt: Arc<AtomicU64>,
    #[serde(skip_serializing)]
    pub failed_cnt: Arc<AtomicU64>,
    /// SQL rows that produced no result. The reference API reports this
    /// as `failed.no_result`. A rule without a result increments both
    /// this counter and `failed_cnt`.
    #[serde(skip_serializing)]
    pub no_result_cnt: Arc<AtomicU64>,
    #[serde(skip_serializing)]
    pub actions_total_cnt: Arc<AtomicU64>,
    #[serde(skip_serializing)]
    pub actions_success_cnt: Arc<AtomicU64>,
    #[serde(skip_serializing)]
    pub actions_failed_cnt: Arc<AtomicU64>,
}

/// Per-action outcome counters shared with every `run_actions` call site.
#[derive(Debug, Clone)]
struct ActionCounters {
    total: Arc<AtomicU64>,
    success: Arc<AtomicU64>,
    failed: Arc<AtomicU64>,
}

impl ActionCounters {
    fn from_rule(rule: &Rule) -> Self {
        Self {
            total: rule.actions_total_cnt.clone(),
            success: rule.actions_success_cnt.clone(),
            failed: rule.actions_failed_cnt.clone(),
        }
    }
}

/// Fixed per-action metric counters for one action id (W3-05, C06-08/09,
/// RO-01).
///
/// One record per action id, created by the management plane when the
/// action is created and dropped when the action is deleted. The fields
/// are lock-free atomics. The rule path and the action runtime bump them;
/// the publish path pays one `HashMap` lookup plus relaxed `fetch_add`s,
/// no lock and no per-fire allocation. Text rendering stays in the
/// management handler. Memory-only by design: a restart rebuilds the map at
/// zero. The set is fixed, so no unbounded history is kept.
#[derive(Debug, Default)]
pub struct ActionMetrics {
    /// Fires attributed to this action id. Kept for older readers; it
    /// equals `matched` for an action-owned runtime.
    pub total: AtomicU64,
    /// Messages matched to this action.
    pub matched: AtomicU64,
    /// Messages that the connector sent without error.
    pub success: AtomicU64,
    /// Messages that the connector refused or failed to send.
    pub failed: AtomicU64,
    /// Messages dropped for every reason.
    pub dropped: AtomicU64,
    /// Messages dropped because the bounded buffer was full.
    pub dropped_queue_full: AtomicU64,
    /// Messages dropped because the action target was missing.
    pub dropped_resource_not_found: AtomicU64,
    /// Messages dropped because the action or its connector was stopped.
    pub dropped_resource_stopped: AtomicU64,
    /// Messages dropped for other reasons, including request expiry.
    pub dropped_other: AtomicU64,
    /// Messages queued for delivery.
    pub queuing: AtomicU64,
    /// Messages in flight at the connector.
    pub inflight: AtomicU64,
}

/// Lock-free snapshot of one action's metric counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ActionMetricsSnapshot {
    pub total: u64,
    pub matched: u64,
    pub success: u64,
    pub failed: u64,
    pub dropped: u64,
    pub dropped_queue_full: u64,
    pub dropped_resource_not_found: u64,
    pub dropped_resource_stopped: u64,
    pub dropped_other: u64,
    pub queuing: u64,
    pub inflight: u64,
}

impl ActionMetrics {
    /// Zeroed counters for a freshly created action.
    pub fn new() -> Self {
        Self::default()
    }

    /// Lock-free snapshot of every counter for management reads.
    pub fn snapshot(&self) -> ActionMetricsSnapshot {
        ActionMetricsSnapshot {
            total: self.total.load(Ordering::Relaxed),
            matched: self.matched.load(Ordering::Relaxed),
            success: self.success.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            dropped_queue_full: self.dropped_queue_full.load(Ordering::Relaxed),
            dropped_resource_not_found: self.dropped_resource_not_found.load(Ordering::Relaxed),
            dropped_resource_stopped: self.dropped_resource_stopped.load(Ordering::Relaxed),
            dropped_other: self.dropped_other.load(Ordering::Relaxed),
            queuing: self.queuing.load(Ordering::Relaxed),
            inflight: self.inflight.load(Ordering::Relaxed),
        }
    }

    /// Zero every counter in place. The record stays registered, so the
    /// next fire resumes from zero.
    pub fn reset(&self) {
        self.total.store(0, Ordering::Relaxed);
        self.matched.store(0, Ordering::Relaxed);
        self.success.store(0, Ordering::Relaxed);
        self.failed.store(0, Ordering::Relaxed);
        self.dropped.store(0, Ordering::Relaxed);
        self.dropped_queue_full.store(0, Ordering::Relaxed);
        self.dropped_resource_not_found.store(0, Ordering::Relaxed);
        self.dropped_resource_stopped.store(0, Ordering::Relaxed);
        self.dropped_other.store(0, Ordering::Relaxed);
        self.queuing.store(0, Ordering::Relaxed);
        self.inflight.store(0, Ordering::Relaxed);
    }
}

/// Fixed per-source metric counters for one source id (W3-29, C07-18/19).
///
/// One record per source id, created by the management plane when the
/// source is created and dropped when the source is deleted. Both fields
/// are lock-free atomics bumped on the ingress path
/// ([`RuleEngine::dispatch_ingress`], driven by every broker publish);
/// the publish path pays one `HashMap` lookup plus one relaxed
/// `fetch_add` per registered source, no lock, no per-fire allocation.
/// Text rendering stays in the management handler. Memory-only by design:
/// a restart rebuilds the map at zero (source config itself persists
/// elsewhere). The set is fixed (`received`/`dropped`, default zero); no
/// unbounded history is kept.
///
/// `received` counts ingress observed while the source is enabled;
/// `dropped` counts ingress observed while the source is stopped (the
/// enable flag gates ingress, so stopped sources shed instead of
/// absorbing). Gauges (`inflight`, `queuing`), rates (`rate`, `rate_max`,
/// `rate_last5m`), `retried` and the `dropped.*` breakdown are deliberately
/// omitted: the broker does not track per-source queue depth, retry state
/// or drop causes, and a constant standing in for a measurement would be
/// worse than a missing field.
// TODO(parity): Which source counters does the specification require beyond received and dropped?
#[derive(Debug, Default)]
pub struct SourceMetrics {
    /// Ingress messages attributed to this source while enabled.
    pub received: AtomicU64,
    /// Ingress messages shed because the source was stopped.
    pub dropped: AtomicU64,
}

impl SourceMetrics {
    /// Zeroed counters for a freshly created source.
    pub fn new() -> Self {
        Self::default()
    }

    /// Lock-free snapshot of the two counters for management reads.
    pub fn snapshot(&self) -> (u64, u64) {
        (
            self.received.load(Ordering::Relaxed),
            self.dropped.load(Ordering::Relaxed),
        )
    }

    /// Zero both counters in place; the record stays registered so the
    /// next ingress resumes from zero.
    pub fn reset(&self) {
        self.received.store(0, Ordering::Relaxed);
        self.dropped.store(0, Ordering::Relaxed);
    }
}

/// Fixed per-transformation metric counters for one name (W3-36, C08-09/10).
///
/// One record per transformation name, created by the management plane
/// when the entry is created and dropped when the entry is deleted.
/// All three fields are lock-free atomics bumped on the ingress path;
/// the publish path pays one `HashMap` lookup plus relaxed `fetch_add`
/// calls per matching step, no lock, no per-fire allocation. Text
/// rendering stays in the management handler. Memory-only by design:
/// a restart rebuilds the map at zero (chain config persists elsewhere).
/// The set is fixed (`matched`/`succeeded`/`failed`, default zero);
/// no unbounded history is kept.
///
/// `matched` counts ingress where the step runs (enabled plus filter
/// hit). `succeeded` counts runs that apply without shedding.
/// `failed` counts runs that shed the message. A shed stops the chain,
/// so later steps observe nothing for that ingress.
// TODO(parity): Which transform counters does the specification require beyond matched, succeeded, failed?
#[derive(Debug, Default)]
pub struct TransformMetrics {
    /// Ingress runs attributed to this name (enabled plus filter hit).
    pub matched: AtomicU64,
    /// Runs that apply without shedding.
    pub succeeded: AtomicU64,
    /// Runs that shed the message.
    pub failed: AtomicU64,
}

impl TransformMetrics {
    /// Zeroed counters for a freshly created transformation.
    pub fn new() -> Self {
        Self::default()
    }

    /// Lock-free snapshot of the three counters for management reads.
    pub fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.matched.load(Ordering::Relaxed),
            self.succeeded.load(Ordering::Relaxed),
            self.failed.load(Ordering::Relaxed),
        )
    }

    /// Zero all three counters in place; the record stays registered so
    /// the next ingress resumes from zero.
    pub fn reset(&self) {
        self.matched.store(0, Ordering::Relaxed);
        self.succeeded.store(0, Ordering::Relaxed);
        self.failed.store(0, Ordering::Relaxed);
    }
}

/// Throttle state for one (rule id, connector id) pair: when the last
/// forward-failure line was logged and how many failures were suppressed
/// since then.
#[derive(Debug, Clone, Copy)]
struct ThrottleEntry {
    last_logged_ms: u64,
    suppressed: u64,
}

/// Maximum one forward-failure `warn!` per (rule id, connector id) per
/// window; later failures inside the window are only counted.
const FORWARD_FAILURE_LOG_WINDOW_MS: u64 = 10_000;

/// Wall-clock milliseconds.
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Returns `Some(suppressed)` when the caller must log (first failure, or
/// the window elapsed since the previous line), `None` when the failure
/// must only be counted.
fn forward_failure_should_log(
    throttle: &parking_lot::Mutex<HashMap<(String, String), ThrottleEntry>>,
    rule_id: &str,
    connector_id: &str,
    now_ms: u64,
) -> Option<u64> {
    let mut guard = throttle.lock();
    let key = (rule_id.to_string(), connector_id.to_string());
    match guard.get_mut(&key) {
        None => {
            guard.insert(
                key,
                ThrottleEntry {
                    last_logged_ms: now_ms,
                    suppressed: 0,
                },
            );
            Some(0)
        }
        Some(entry) => {
            if now_ms.saturating_sub(entry.last_logged_ms) >= FORWARD_FAILURE_LOG_WINDOW_MS {
                let suppressed = entry.suppressed;
                entry.last_logged_ms = now_ms;
                entry.suppressed = 0;
                Some(suppressed)
            } else {
                entry.suppressed += 1;
                None
            }
        }
    }
}

/// Actions a rule may execute per matched ingress event.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum RuleAction {
    /// Republish the (possibly transformed) payload in Rust memory via
    /// [`BrokerSink`]. Never opens an MQTT loopback connection.
    Republish {
        topic: Topic,
        #[serde(with = "qos_serde")]
        qos: QoS,
    },
    /// Emit a structured log line for the matched event.
    Log,
    /// Forward the (possibly transformed) payload to a registered
    /// external connector (e.g. an HTTP webhook) by id.
    ForwardConnector { connector_id: String },
    /// The built-in `republish` function of the reference API. Topic,
    /// payload, QoS and retain flag are fixed values or templates that
    /// the engine renders for each result row, see [`functions`].
    RepublishFn { args: broker_config::RepublishArgs },
    /// The built-in `console` function of the reference API: write the
    /// result row to the log at info level, on one line, with a size
    /// limit.
    Console,
}

/// Serde helper: [`QoS`] as its MQTT wire number (0/1/2).
mod qos_serde {
    use super::QoS;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(qos: &QoS, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u8((*qos).into())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<QoS, D::Error> {
        let raw = u8::deserialize(deserializer)?;
        QoS::try_from(raw).map_err(serde::de::Error::custom)
    }
}

/// Ingress-only rule engine shared by every connection task on a node.
///
/// Rules are matched with [`TopicFilter::matches`] at message ingress on
/// the receiving node. Republished messages flow straight into the sink
/// and are never re-dispatched, so rules cannot recurse through their
/// own output.
///
/// Enterprise (windowed) rules do not execute inline: matching ingress is
/// converted once to a [`rekuiper_core::StreamRecord`] and handed to the
/// rule's task ([`RuleTask`]) over the bounded [`rekuiper_core::StreamBus`]);
/// the task aggregates per window close and dispatches through the
/// [`runtime::RuleOutputSink`] bridge.
pub struct RuleEngine {
    rules: RwLock<HashMap<String, Arc<Rule>>>,
    next_id: AtomicU64,
    input: Arc<BoundedEventInput>,
    connectors: Arc<ConnectorManager>,
    flush_ctx: Arc<FlushContext>,
    /// The window executor of the enterprise crate, if one is installed.
    /// The kernel installs it at boot.
    window_executor: RwLock<Option<Arc<dyn WindowExecutor>>>,
    /// Lock-free snapshot of disabled action ids (W3-04, C06-07): the
    /// management plane swaps the whole set on every action
    /// create/update/delete/enable, and the publish path loads one `Arc`
    /// per ingress with no store lock. A single `bool` per action lives
    /// here as set membership (absent means enabled); no list or cache
    /// grows on the message path. Reason for `ArcSwap`: the publish
    /// path must never take the management store lock, and the engine's
    /// own rule-map lock is already held for the rule snapshot, so the
    /// enable read stays a lock-free `Arc` clone plus a `HashSet`
    /// lookup per forward action.
    action_disabled: Arc<arc_swap::ArcSwap<HashSet<String>>>,
    /// Lock-free per-action metric records keyed by action id (W3-05,
    /// C06-08/09): the management plane swaps the whole map on action
    /// create/delete, and the publish path loads one `Arc` per ingress
    /// with no store lock, then bumps lock-free atomics. One fixed-size
    /// record per action (at most the action-store cap of entries), so no
    /// list or history grows on the message path. Reason for `ArcSwap`:
    /// the same as the disabled snapshot above — the publish path must
    /// never take a management lock.
    action_metrics: Arc<arc_swap::ArcSwap<HashMap<String, Arc<ActionMetrics>>>>,
    /// Lock-free snapshot of stopped source ids (W3-29, C07-18/19): the
    /// management plane swaps the whole set on every source
    /// create/update/delete/enable/operation, and the publish path loads
    /// one `Arc` per ingress with no store lock. A single `bool` per source
    /// lives here as set membership (absent means enabled); no list or
    /// cache grows on the message path. Reason for `ArcSwap`: the same as
    /// the action snapshot above — the publish path must never take the
    /// management store lock.
    source_disabled: Arc<arc_swap::ArcSwap<HashSet<String>>>,
    /// Lock-free per-source metric records keyed by source id (W3-29,
    /// C07-18/19): the management plane swaps the whole map on source
    /// create/delete, and the publish path loads one `Arc` per ingress
    /// with no store lock, then bumps lock-free atomics. One fixed-size
    /// record per source (at most the source-store cap of entries), so no
    /// list or history grows on the message path. Reason for `ArcSwap`:
    /// the same as the disabled snapshot above — the publish path must
    /// never take a management lock.
    source_metrics: Arc<arc_swap::ArcSwap<HashMap<String, Arc<SourceMetrics>>>>,
    /// True when at least one source metric record exists (W3-29).
    /// Lets `dispatch_ingress` skip the source snapshots with one
    /// relaxed load when no source exists.
    source_any: AtomicBool,
    /// Embedded rule runtime transport: one [`rekuiper_core::StreamBus`]
    /// per engine plus its send handle for [`runtime::RULE_INGRESS_STREAM`].
    /// Every windowed rule's task subscribes; the publish path hands each
    /// windowed ingress over with [`StreamSender::send`] (async admission
    /// with backpressure).
    stream_bus: StreamBus,
    ingress_sender: StreamSender,
    /// One live rule task per windowed rule (see [`RuleTask`]).
    rule_tasks: RwLock<HashMap<String, RuleTask>>,
    /// Windowed ingress records dropped because runtime admission failed
    /// (the rule task closed mid-admission). Counted with a warning; bus
    /// backpressure awaits capacity instead of dropping on full.
    runtime_feed_dropped: AtomicU64,
    // Live window task feed depth. Reason: boot applies the file value
    // without a restart. One relaxed load at task spawn, never on publish.
    window_channel_depth: AtomicUsize,
    forward_throttle: Arc<parking_lot::Mutex<HashMap<(String, String), ThrottleEntry>>>,
    /// Kernel config registry receiving every rule mutation (`None` in
    /// unit tests and standalone state, which stay memory-only).
    registry: RwLock<Option<Arc<broker_config::ConfigRegistry>>>,
    /// Serialises export-commit-save so concurrent mutations cannot
    /// interleave into a lost update on disk.
    save_lock: parking_lot::Mutex<()>,
    /// Lock-free rule-engine settings for the fire path (W3-10): the
    /// management plane swaps the whole struct on every validated engine
    /// write (via [`RuleEngine::apply_engine_config`], fed by the
    /// [`EngineConfigStore`] subscriber), and the publish path loads one
    /// `Arc` only for system topics with no registry or store lock.
    /// Fixed struct, so no list or cache grows on the message path.
    /// Reason for `ArcSwap`: the publish path must never take a
    /// management lock, and plain traffic pays no settings load.
    engine_config: Arc<arc_swap::ArcSwap<broker_config::RuleEngineConf>>,
    /// Lock-free message-transformation chain for the ingress path
    /// (W3-32): the management plane swaps the whole ordered chain on
    /// every transform create/replace, and the publish path loads one
    /// `Arc` per ingress with no store lock. At most the chain cap of
    /// small steps, so no list grows on the message path. Reason for
    /// `ArcSwap`: the publish path must never take the management
    /// store lock; matching stays filter tests plus template renders.
    transforms: Arc<arc_swap::ArcSwap<Vec<TransformStep>>>,
    /// Lock-free per-transformation metric records keyed by name (W3-36,
    /// C08-09/10): the management plane swaps the whole map on
    /// transform create/delete/replace, and the publish path loads one
    /// `Arc` per ingress with no store lock, then bumps lock-free
    /// atomics. One fixed-size record per name (at most the chain cap
    /// of entries), so no list or history grows on the message path.
    /// Reason for `ArcSwap`: the same as the chain above — the publish
    /// path must never take a management lock.
    transform_metrics: Arc<arc_swap::ArcSwap<HashMap<String, Arc<TransformMetrics>>>>,
    /// Per-event-source selection flags (RQ-04). One AtomicBool per
    /// EventSource variant; set when any enabled rule selects that source.
    /// Checked before building an event record so firing costs nothing when
    /// no rule selects it. Updated in `create_rule`, `remove_rule`,
    /// `set_rule_enabled` and `seed_from_snapshot`. Its length is
    /// [`EventSource`]'s variant count, not a second hardcoded number.
    event_flags: [AtomicBool; EventSource::COUNT],
    /// Aggregate drain gate (RQ-04): true when at least one event source is
    /// selected by an enabled rule. Lets `dispatch_ingress` skip the
    /// event-queue drain on the publish hot path with one relaxed load and
    /// zero locks when no rule selects any event source. Kept in sync with
    /// `event_flags` on every rule mutation and cleared in
    /// `seed_from_snapshot`.
    event_any_selected: AtomicBool,
}

/// Shared flush context: what a rule-task sink worker needs at emit time.
/// The broker sink is installed post-construction via
/// [`RuleEngine::set_broker_sink`] (the engine is built before the node
/// sink exists); connectors are shared with the engine itself.
struct FlushContext {
    connectors: Arc<ConnectorManager>,
    broker_sink: RwLock<Option<Arc<dyn BrokerSink>>>,
    forward_throttle: Arc<parking_lot::Mutex<HashMap<(String, String), ThrottleEntry>>>,
    /// Shared disabled-action snapshot with the engine (W3-04, C06-07):
    /// the same `ArcSwap` instance, so window-flush emits observe the
    /// same management-plane swaps as the inline path without a lock.
    action_disabled: Arc<arc_swap::ArcSwap<HashSet<String>>>,
    /// Shared per-action metric map with the engine (W3-05, C06-08/09):
    /// the same `ArcSwap` instance, so window-flush emits attribute to
    /// the same lock-free records as the inline path without a lock.
    action_metrics: Arc<arc_swap::ArcSwap<HashMap<String, Arc<ActionMetrics>>>>,
}

/// One timestamped record waiting inside a window buffer. The ingress
/// topic travels along so flush dispatches carry real routing context.
/// `tenant` names the owning tenant, so flush output fans out in the
/// same tenant. `qos` carries the ingress QoS so flush delivery stays
/// `min(ingress QoS, action QoS)` and a rule can never upgrade delivery
/// guarantees. `injected` plus `std_has_wildcard`/`std_explicit_keys` carry the
/// standard front-end projection state so window flushes strip injected
/// message keys from `SELECT *` exactly like the inline path.
#[derive(Debug, Clone)]
struct TimedRecord {
    arrived_ms: u64,
    tenant: String,
    topic: Topic,
    qos: QoS,
    record: serde_json::Map<String, serde_json::Value>,
    injected: Vec<String>,
    std_has_wildcard: bool,
    std_explicit_keys: Vec<String>,
}

/// One live rule task on rekuiper's runtime for a windowed rule: a bus
/// feeder (bus subscription into the bounded rule-task feed), an eval loop
/// (window aggregation per close) and a sink worker (bounded sink queue
/// into the [`runtime::RuleOutputSink`] bridge). Created by
/// `ensure_rule_task`, aborted on remove/seed (start/stop/delete with abort
/// semantics). The per-rule [`RuleCounters`] travel with the task;
/// `status` is `running` until the task is stopped.
struct RuleTask {
    eval: tokio::task::JoinHandle<()>,
    feeder: tokio::task::JoinHandle<()>,
    sink_worker: tokio::task::JoinHandle<()>,
    /// Bounded rule-task feed (replacement for the retired worker channel):
    /// depth is the live window depth. A full feed drops with a
    /// warning and a counter, never blocks publish.
    feed_tx: mpsc::Sender<TimedRecord>,
    counters: Arc<RuleCounters>,
    status: Arc<RwLock<String>>,
}

/// One evaluated output row waiting in a rule-task sink queue. It
/// carries the partition's first record topic (real routing context),
/// the owning tenant (so republish fans out in the same tenant), the
/// ingress QoS floor (`min` over the flushed batch, so republish stays
/// `min(ingress QoS, action QoS)`), plus the encoded row bytes.
struct SinkRow {
    tenant: String,
    topic: Topic,
    qos: QoS,
    payload: Bytes,
}

/// The parsed result of a standard-dialect rule SQL statement.
type ParsedStdRule = (
    SelectStmt,
    Option<ForeachPlan>,
    Vec<StdSource>,
    bool,
    Vec<String>,
);

/// A window record enriched with injected timestamps and projection state.
type EnrichedWindowRecord = (
    Topic,
    serde_json::Map<String, serde_json::Value>,
    Vec<String>,
    bool,
    Vec<String>,
);

/// Configuration for a rule-task eval loop, grouping parameters to avoid
/// clippy::too_many_arguments.
struct RuleTaskConfig {
    rule_id: String,
    stmt: SelectStmt,
    feed_rx: mpsc::Receiver<TimedRecord>,
    sink_tx: mpsc::Sender<SinkRow>,
    window: WindowDef,
    std_has_wildcard: bool,
    std_explicit_keys: Vec<String>,
    /// Failed counter of the owning rule. The eval loop bumps it once
    /// per non-empty batch that yields no row.
    failed: Arc<AtomicU64>,
    /// No-result counter of the owning rule. The eval loop bumps it for
    /// the same no-row batches, which are SQL no-result outcomes.
    no_result: Arc<AtomicU64>,
}

/// Configuration for a rule-task bus feeder: what the feeder needs to turn
/// bus [`rekuiper_core::StreamRecord`]s into feed records. Grouped to avoid
/// clippy::too_many_arguments.
struct RuleFeederConfig {
    rule_id: String,
    tenant: String,
    topic_filter: TopicFilter,
    std_sources: Vec<StdSource>,
    std_topic_filters: Vec<Option<TopicFilter>>,
    has_foreach: bool,
    parsed_query: Option<SelectStmt>,
    std_has_wildcard: bool,
    std_explicit_keys: Vec<String>,
    feed_tx: mpsc::Sender<TimedRecord>,
    bus_rx: StreamReceiver,
    counters: Arc<RuleCounters>,
    /// Failed counter of the owning rule. The feeder bumps it once per
    /// ingress that cannot enter the window buffer.
    failed: Arc<AtomicU64>,
    /// No-result counter of the owning rule. The feeder bumps it for a
    /// row that the WHERE clause rejects.
    no_result: Arc<AtomicU64>,
}

/// The runtime sink bridge: [`runtime::RuleOutputSink`] over the broker's
/// [`ConnectorManager::send`] and [`BrokerSink::publish`]. Built once per
/// rule task (cold path only, so the publish path pays no new allocation):
/// the inline stateless path keeps calling [`run_actions`] directly with
/// the same guarantee. Delivery QoS is `min(action QoS, cap)`; the sink
/// worker passes the flushed batch's ingress-QoS floor as the cap so a
/// windowed rule can never upgrade delivery guarantees, exactly like the
/// inline path.
struct BrokerRuleSink {
    rule_id: String,
    tenant: String,
    actions: Vec<RuleAction>,
    flush_ctx: Arc<FlushContext>,
    counters: ActionCounters,
    task_counters: Arc<RuleCounters>,
    /// Passed counter of the owning rule. Each emitted row bumps it once,
    /// so window outputs count exactly like inline outputs.
    passed: Arc<AtomicU64>,
}

#[async_trait]
impl runtime::RuleOutputSink for BrokerRuleSink {
    async fn emit(&self, topic: Topic, payload: Bytes, qos_cap: QoS) {
        // Count the row first, as the inline path counts a projected
        // row before it runs actions. Action outcomes count separately.
        self.passed.fetch_add(1, Ordering::Relaxed);
        let sink = self.flush_ctx.broker_sink.read().clone();
        // W3-04: window-flush emits honour the same lock-free snapshot as
        // the inline path (one `Arc` clone per flush, not per message).
        let disabled = self.flush_ctx.action_disabled.load_full();
        // W3-05: same for the per-action metric map (one `Arc` clone per
        // flush, lock-free atomic bumps per forward, no per-fire
        // allocation).
        let action_metrics = self.flush_ctx.action_metrics.load_full();
        run_actions_in_tenant(
            &self.rule_id,
            &self.tenant,
            &self.actions,
            &topic,
            payload,
            qos_cap,
            &self.flush_ctx.connectors,
            sink.as_ref(),
            &self.counters,
            &self.flush_ctx.forward_throttle,
            &disabled,
            &action_metrics,
            false,
        )
        .await;
        self.task_counters.inc_sink_out(1);
    }
}

/// Default per-rule task feed ingress depth (INDRA-215): large
/// enough for high-scale bursts without drops, still bounded so a
/// stalled task cannot grow memory without limit. Backpressure stays
/// at the edge input queue; a full task feed drops with a warning,
/// never blocks. Override per engine via
/// [`RuleEngine::new_with_window_depth`] or
/// [`RuleEngine::with_window_channel_depth`]. Same value as
/// [`runtime::RULE_TASK_FEED_DEPTH`]: the rule-task feed bound.
pub const DEFAULT_WINDOW_CHANNEL_DEPTH: usize = 65_536;

/// Idle gap that closes an implicit plain-`GROUP BY` session (500ms).
/// Reason: burst publishes arrive microseconds apart, so 500ms groups
/// each burst into one session while keeping added latency well under
/// the 10s management-test bound.
pub const IMPLICIT_GROUP_SESSION_TIMEOUT_MS: u64 = 500;

/// Longest one implicit plain-`GROUP BY` session runs (30s).
/// Reason: caps the session buffer so a continuous stream flushes
/// at least every 30s; the bounded feed drops surplus with a counter.
pub const IMPLICIT_GROUP_SESSION_MAX_MS: u64 = 30_000;

/// True for system-message topics (`$SYS` and `$SYS/...`), which the rule
/// engine skips while `ignore_sys_message` holds (W3-10). Pure prefix test
/// on the topic string: no lock, no allocation.
fn is_system_topic(topic: &Topic) -> bool {
    let name = topic.as_str();
    name == "$SYS" || name.starts_with("$SYS/")
}

/// Window length in milliseconds (saturating; clamped to at least 1ms so
/// degenerate `TUMBLINGWINDOW(ss, 0)` style definitions tick instead of
/// panicking the ticker).
fn window_length_ms(unit: &TimeUnit, length: u64) -> u64 {
    let per_unit: u64 = match unit {
        TimeUnit::Ms => 1,
        TimeUnit::Ss => 1_000,
        TimeUnit::Mi => 60_000,
        TimeUnit::Hh => 3_600_000,
        TimeUnit::Dd => 86_400_000,
    };
    length.saturating_mul(per_unit).max(1)
}

/// Seam for enterprise windowed execution, implemented outside this
/// crate so the community rule path carries no dependency edge to the
/// window workers. The kernel installs the enterprise implementation at
/// boot; every method is a no-op-safe delegation point owned by the
/// engine's rule lifecycle (create, remove, snapshot seed, ingress).
pub trait WindowExecutor: Send + Sync {
    /// Route one matching record into this rule's window worker.
    fn route_record(&self, rule: &Rule, topic: &Topic, payload: &Bytes);
    /// Ensure a background worker exists for an Enterprise window rule.
    fn ensure_worker(&self, rule: &Rule);
    /// Abort the worker for one rule id, if any.
    fn remove_worker(&self, rule_id: &str);
    /// Abort every worker (snapshot replace path).
    fn abort_all(&self);
    /// Live window worker count (observability/testing hook).
    fn worker_count(&self) -> usize;
}

/// Event firing argument structs (RQ-04).
/// Group arguments to avoid clippy::too_many_arguments.
/// Arguments for $events/client_connected
pub struct ClientConnectedEvent {
    pub client_id: String,
    pub username: Option<String>,
    pub peerhost: Option<String>,
    pub keepalive: u16,
    pub proto_ver: u8,
    pub clean_start: bool,
    pub conn_ack_code: u8,
}

/// Arguments for $events/client_disconnected
/// The edge forwards no reason code, so the code stays absent.
/// The record omits it when unknown.
pub struct ClientDisconnectedEvent {
    pub client_id: String,
    pub username: Option<String>,
    pub peerhost: Option<String>,
    pub reason: String,
    pub disconnect_code: Option<u8>,
}

/// Arguments for $events/client_connack
pub struct ClientConnackEvent {
    pub client_id: String,
    pub username: Option<String>,
    pub peerhost: Option<String>,
    pub return_code: u8,
    pub session_present: bool,
}

/// Arguments for $events/client_check_authz_complete
/// Subscribe carries no retain flag, so retain stays absent there.
/// The record omits it when unknown.
pub struct ClientCheckAuthzCompleteEvent {
    pub client_id: String,
    pub username: Option<String>,
    pub peerhost: Option<String>,
    pub action: String,
    pub topic: String,
    pub qos: u8,
    pub retain: Option<bool>,
    pub allow: bool,
    pub source: String,
}

/// Arguments for $events/session_subscribed
pub struct SessionSubscribedEvent {
    pub client_id: String,
    pub username: Option<String>,
    pub peerhost: Option<String>,
    pub topic: String,
    pub qos: u8,
    pub nl: u8,
    pub rap: u8,
    pub rh: u8,
}

/// Arguments for $events/session_unsubscribed
pub struct SessionUnsubscribedEvent {
    pub client_id: String,
    pub username: Option<String>,
    pub peerhost: Option<String>,
    pub topic: String,
}

/// Arguments for $events/message_delivered
pub struct MessageDeliveredEvent {
    pub client_id: String,
    pub username: Option<String>,
    pub peerhost: Option<String>,
    pub topic: String,
    pub qos: u8,
    pub retain: bool,
    pub payload: Bytes,
    pub msg_id: Option<u16>,
}

/// Arguments for $events/message_acked
pub struct MessageAckedEvent {
    pub client_id: String,
    pub username: Option<String>,
    pub peerhost: Option<String>,
    pub topic: String,
    pub qos: u8,
    pub msg_id: u16,
}

/// Arguments for $events/message_dropped
pub struct MessageDroppedEvent {
    pub client_id: String,
    pub username: Option<String>,
    pub peerhost: Option<String>,
    pub topic: String,
    pub qos: u8,
    pub reason: String,
}

/// Arguments for $events/delivery_dropped
pub struct DeliveryDroppedEvent {
    pub client_id: String,
    pub username: Option<String>,
    pub peerhost: Option<String>,
    pub topic: String,
    pub qos: u8,
    pub reason: String,
}

impl RuleEngine {
    /// Create an engine with a bounded ingress queue (`capacity` floors at
    /// 1). The queue backs future async ingestion; the hot path calls
    /// [`RuleEngine::dispatch_ingress`] synchronously for determinism.
    /// Rule-task feed channels use [`DEFAULT_WINDOW_CHANNEL_DEPTH`].
    pub fn new(queue_capacity: usize, policy: BackpressurePolicy) -> Self {
        Self::with_input(
            BoundedEventInput::new(queue_capacity, policy),
            DEFAULT_WINDOW_CHANNEL_DEPTH,
        )
    }

    /// Create an engine whose ingress queue spills to disk under
    /// pressure (`SpillToDisk` policy with a bounded buffer in `dir`).
    /// Opening recovers the directory, so a previous engine's spilled
    /// events survive the restart. Rule-task feed channels use
    /// [`DEFAULT_WINDOW_CHANNEL_DEPTH`]. A bad directory fails loudly.
    pub fn new_with_spill(
        queue_capacity: usize,
        spill_dir: impl Into<std::path::PathBuf>,
    ) -> io::Result<Self> {
        Self::new_with_spill_config(queue_capacity, SpillConfig::new(spill_dir))
    }

    /// Create a spill-backed engine with an explicit disk policy
    /// (segment and total caps, for tests and tuned deployments).
    pub fn new_with_spill_config(
        queue_capacity: usize,
        spill_config: SpillConfig,
    ) -> io::Result<Self> {
        let input = BoundedEventInput::with_spill(queue_capacity, spill_config)?;
        Ok(Self::with_input(input, DEFAULT_WINDOW_CHANNEL_DEPTH))
    }

    /// Shared construction from a ready ingress queue.
    fn with_input(input: Arc<BoundedEventInput>, window_channel_depth: usize) -> Self {
        // RQ-03: standard-dialect functions are registered once process-wide;
        // after the first call this is one atomic load (no mutex), so the
        // publish path pays no new lock.
        stdlib::ensure_stdlib_registered();
        let connectors = Arc::new(ConnectorManager::new());
        let forward_throttle = Arc::new(parking_lot::Mutex::new(HashMap::new()));
        let action_disabled = Arc::new(arc_swap::ArcSwap::from(Arc::new(HashSet::new())));
        let action_metrics = Arc::new(arc_swap::ArcSwap::from(Arc::new(HashMap::new())));
        let source_disabled = Arc::new(arc_swap::ArcSwap::from(Arc::new(HashSet::new())));
        let source_metrics = Arc::new(arc_swap::ArcSwap::from(Arc::new(HashMap::new())));
        let transform_metrics = Arc::new(arc_swap::ArcSwap::from(Arc::new(HashMap::new())));
        let stream_bus = StreamBus::new();
        let ingress_sender = stream_bus.get_or_create(runtime::RULE_INGRESS_STREAM);
        Self {
            rules: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            input,
            connectors: connectors.clone(),
            flush_ctx: Arc::new(FlushContext {
                connectors,
                broker_sink: RwLock::new(None),
                forward_throttle: forward_throttle.clone(),
                action_disabled: action_disabled.clone(),
                action_metrics: action_metrics.clone(),
            }),
            action_disabled,
            action_metrics,
            source_disabled,
            source_metrics,
            source_any: AtomicBool::new(false),
            stream_bus,
            ingress_sender,
            rule_tasks: RwLock::new(HashMap::new()),
            window_executor: RwLock::new(None),
            runtime_feed_dropped: AtomicU64::new(0),
            window_channel_depth: AtomicUsize::new(window_channel_depth.max(1)),
            forward_throttle,
            registry: RwLock::new(None),
            save_lock: parking_lot::Mutex::new(()),
            engine_config: Arc::new(arc_swap::ArcSwap::from(Arc::new(
                broker_config::RuleEngineConf::default(),
            ))),
            transforms: Arc::new(arc_swap::ArcSwap::from(Arc::new(Vec::new()))),
            transform_metrics,
            event_flags: std::array::from_fn(|_| AtomicBool::new(false)),
            event_any_selected: AtomicBool::new(false),
        }
    }

    /// Create an engine with an explicit per-rule task feed channel
    /// depth (`depth` floors at 1, no ceiling). Use 65,536+ for
    /// high-scale bursts.
    pub fn new_with_window_depth(
        queue_capacity: usize,
        policy: BackpressurePolicy,
        window_channel_depth: usize,
    ) -> Self {
        // RQ-03: see `with_input` (one atomic load after the first call).
        stdlib::ensure_stdlib_registered();
        let connectors = Arc::new(ConnectorManager::new());
        let forward_throttle = Arc::new(parking_lot::Mutex::new(HashMap::new()));
        let action_disabled = Arc::new(arc_swap::ArcSwap::from(Arc::new(HashSet::new())));
        let action_metrics = Arc::new(arc_swap::ArcSwap::from(Arc::new(HashMap::new())));
        let source_disabled = Arc::new(arc_swap::ArcSwap::from(Arc::new(HashSet::new())));
        let source_metrics = Arc::new(arc_swap::ArcSwap::from(Arc::new(HashMap::new())));
        let transform_metrics = Arc::new(arc_swap::ArcSwap::from(Arc::new(HashMap::new())));
        let stream_bus = StreamBus::new();
        let ingress_sender = stream_bus.get_or_create(runtime::RULE_INGRESS_STREAM);
        Self {
            rules: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            input: BoundedEventInput::new(queue_capacity, policy),
            connectors: connectors.clone(),
            flush_ctx: Arc::new(FlushContext {
                connectors,
                broker_sink: RwLock::new(None),
                forward_throttle: forward_throttle.clone(),
                action_disabled: action_disabled.clone(),
                action_metrics: action_metrics.clone(),
            }),
            action_disabled,
            action_metrics,
            source_disabled,
            source_metrics,
            source_any: AtomicBool::new(false),
            stream_bus,
            ingress_sender,
            rule_tasks: RwLock::new(HashMap::new()),
            window_executor: RwLock::new(None),
            runtime_feed_dropped: AtomicU64::new(0),
            window_channel_depth: AtomicUsize::new(window_channel_depth.max(1)),
            forward_throttle,
            registry: RwLock::new(None),
            save_lock: parking_lot::Mutex::new(()),
            engine_config: Arc::new(arc_swap::ArcSwap::from(Arc::new(
                broker_config::RuleEngineConf::default(),
            ))),
            transforms: Arc::new(arc_swap::ArcSwap::from(Arc::new(Vec::new()))),
            transform_metrics,
            event_flags: std::array::from_fn(|_| AtomicBool::new(false)),
            event_any_selected: AtomicBool::new(false),
        }
    }

    /// Override the rule-task feed channel depth after construction
    /// (floors at 1, no ceiling). Applies to tasks spawned from here on.
    pub fn with_window_channel_depth(self, depth: usize) -> Self {
        self.window_channel_depth
            .store(depth.max(1), Ordering::Relaxed);
        self
    }

    /// Configured per-rule task feed channel depth.
    pub fn window_channel_depth(&self) -> usize {
        self.window_channel_depth.load(Ordering::Relaxed)
    }

    /// Apply the file window depth. Boot and management only.
    /// One relaxed store, no lock. Tasks spawned later use it.
    pub fn set_window_channel_depth(&self, depth: usize) {
        self.window_channel_depth
            .store(depth.max(1), Ordering::Relaxed);
    }

    /// Live backpressure policy of the ingress queue.
    /// One relaxed load, no lock.
    pub fn backpressure_policy(&self) -> BackpressurePolicy {
        self.input.policy()
    }

    /// Apply the file backpressure policy. Boot and management only.
    /// One relaxed store, no lock on the publish path.
    pub fn set_backpressure_policy(&self, policy: BackpressurePolicy) {
        self.input.set_policy(policy);
    }

    /// Bounded ingress queue for flood protection at outer boundaries.
    pub fn input(&self) -> &Arc<BoundedEventInput> {
        &self.input
    }

    /// Spill outcome counts for the ingress queue (zeros when no spill
    /// directory is configured, apart from refusals, which count).
    pub fn spill_stats(&self) -> SpillStats {
        self.input.spill_stats()
    }

    /// Attach the node metrics mirror for the ingress spill counters.
    /// Called once by the node after the shared metrics exist; the
    /// first attach wins and carries the counts so far (including
    /// recovery outcomes from open).
    pub fn set_metrics(&self, metrics: &Arc<broker_observability::Metrics>) {
        self.input.set_metrics(metrics);
    }

    /// Live outbound connectors addressable by `ForwardConnector` actions.
    pub fn connectors(&self) -> &Arc<ConnectorManager> {
        &self.connectors
    }

    /// Install (or replace) the broker sink used by rule-task sink
    /// workers for republish actions. Called once by the node after the
    /// shared sink exists; the inline stateless path keeps taking its sink
    /// per call.
    pub fn set_broker_sink(&self, sink: Arc<dyn BrokerSink>) {
        *self.flush_ctx.broker_sink.write() = Some(sink);
    }

    /// The currently installed task sink, if any.
    pub fn broker_sink(&self) -> Option<Arc<dyn BrokerSink>> {
        self.flush_ctx.broker_sink.read().clone()
    }

    /// Live rule-task count on the embedded runtime
    /// (observability/testing hook).
    pub fn rule_task_count(&self) -> usize {
        self.rule_tasks.read().len()
    }

    /// Send handle for the runtime ingress stream
    /// ([`runtime::RULE_INGRESS_STREAM`]). The publish path uses it with
    /// async admission and backpressure ([`StreamSender::send`]); sync
    /// non-blocking [`StreamSender::try_send`] stays reserved for cyclic
    /// feedback only. Exposed so qualification tests can
    /// drive the exact transport the broker uses.
    pub fn ingress_sender(&self) -> &StreamSender {
        &self.ingress_sender
    }

    /// Windowed ingress records dropped because runtime admission failed
    /// (the rule task closed mid-admission; fail closed and counted).
    /// Bus backpressure awaits subscriber capacity, so a full bus waits
    /// instead of dropping.
    pub fn runtime_feed_dropped(&self) -> u64 {
        self.runtime_feed_dropped.load(Ordering::Relaxed)
    }

    /// Lifecycle status of one rule task (`running`/`stopped`), if present.
    pub fn rule_task_status(&self, id: &str) -> Option<String> {
        self.rule_tasks
            .read()
            .get(id)
            .map(|task| task.status.read().clone())
    }

    /// Per-rule runtime counters of one rule task, if present. The hot path
    /// never takes the task map lock: tasks hold the counters' `Arc` and
    /// bump them lock-free.
    pub fn rule_task_counters(&self, id: &str) -> Option<Arc<RuleCounters>> {
        self.rule_tasks
            .read()
            .get(id)
            .map(|task| task.counters.clone())
    }

    /// Bounded rule-task feed capacity for one live task, if present
    /// (observability/testing hook). Reads the task's feed sender so the
    /// configured [`DEFAULT_WINDOW_CHANNEL_DEPTH`] is observable per task;
    /// the publish path never takes the task map lock.
    pub fn rule_task_feed_capacity(&self, id: &str) -> Option<usize> {
        self.rule_tasks
            .read()
            .get(id)
            .map(|task| task.feed_tx.max_capacity())
    }

    /// Install the enterprise window executor (the kernel does this at
    /// boot before any rule is created). Replaces any previous one.
    pub fn set_window_executor(&self, executor: Arc<dyn WindowExecutor>) {
        *self.window_executor.write() = Some(executor);
    }

    /// Live window worker count (observability/testing hook). Zero when
    /// no executor is installed.
    pub fn window_worker_count(&self) -> usize {
        self.window_executor
            .read()
            .as_ref()
            .map(|executor| executor.worker_count())
            .unwrap_or(0)
    }

    /// Dispatch one flushed window aggregate row through this rule's
    /// actions. Called by the window executor per result row: the rule is
    /// resolved here (a rule removed mid-flush drops its late rows with a
    /// warning), delivery QoS is capped at [`QoS::ExactlyOnce`] so the
    /// action QoS applies verbatim, and counters, connectors, throttle
    /// and sink are the engine's own, exactly as on the inline path.
    pub async fn dispatch_window_row(&self, rule_id: &str, topic: &Topic, payload: Bytes) {
        let Some(rule) = self.get_rule(rule_id) else {
            tracing::warn!(
                rule_id = %rule_id,
                "window row dropped: rule no longer exists"
            );
            return;
        };
        // Count the row as passed, as the inline path counts a projected
        // row. Action outcomes count inside the shared runner.
        rule.passed_cnt.fetch_add(1, Ordering::Relaxed);
        let sink = self.broker_sink();
        let disabled = self.flush_ctx.action_disabled.load_full();
        let action_metrics = self.flush_ctx.action_metrics.load_full();
        run_actions_in_tenant(
            rule_id,
            &rule.tenant,
            &rule.actions,
            topic,
            payload,
            QoS::ExactlyOnce,
            &self.connectors,
            sink.as_ref(),
            &ActionCounters::from_rule(&rule),
            &self.forward_throttle,
            &disabled,
            &action_metrics,
            false,
        )
        .await;
    }

    /// Build and store a rule in the default tenant, assigning its id
    /// (`rule-<n>`). Pre-tenancy callers use this; it keeps their
    /// behaviour. Tenant callers use `create_rule_in_tenant`.
    /// Callers pass already-validated types; fallible parsing
    /// ([`TopicFilter::new`], [`Topic::new`]) happens at the API boundary.
    /// A present `sql_query` is parsed immediately: invalid SQL fails
    /// creation with [`RuleEngineError::InvalidRule`] so a broken rule
    /// can never go live. The mutation commits the exported
    /// [`broker_config::RulesConf`] root and atomically saves it when a
    /// registry is attached; a save failure is returned and the in-memory
    /// rule stays (commit precedes the atomic save).
    pub fn create_rule(
        &self,
        name: String,
        topic_filter: TopicFilter,
        sql_query: Option<String>,
        enabled: bool,
        actions: Vec<RuleAction>,
    ) -> Result<Rule, RuleEngineError> {
        self.create_rule_in_tenant(
            "default".to_string(),
            name,
            topic_filter,
            sql_query,
            enabled,
            actions,
        )
    }

    /// Build and store a rule in one tenant, assigning its id
    /// (`rule-<n>`). The tenant must pass `is_valid_rule_tenant`;
    /// blank maps to the default tenant. Shares the same SQL
    /// compilation, duplicate rejection and rule-cap bound as
    /// [`RuleEngine::create_rule`].
    pub fn create_rule_in_tenant(
        &self,
        tenant: String,
        name: String,
        topic_filter: TopicFilter,
        sql_query: Option<String>,
        enabled: bool,
        actions: Vec<RuleAction>,
    ) -> Result<Rule, RuleEngineError> {
        let tenant = broker_config::canonical_rule_tenant(&tenant);
        if !broker_config::is_valid_rule_tenant(&tenant) {
            return Err(RuleEngineError::InvalidRule(format!(
                "tenant {tenant:?} is not valid (field `tenant`)"
            )));
        }
        let id = format!("rule-{}", self.next_id.fetch_add(1, Ordering::SeqCst));
        self.create_rule_with_id_in_tenant(
            id,
            tenant,
            name,
            String::new(),
            serde_json::json!({}),
            topic_filter,
            sql_query,
            enabled,
            actions,
        )
    }

    /// Build and store a rule under a caller-chosen id (management-plane
    /// `POST /rules` with an explicit `id`). Shares the same SQL
    /// compilation, duplicate rejection and rule-cap bound as
    /// [`RuleEngine::create_rule`]: invalid SQL fails creation with
    /// [`RuleEngineError::InvalidRule`] so a broken rule can never go
    /// live, a duplicate id fails loudly instead of overwriting, and a
    /// full store (at most [`broker_config::MAX_RULES`] entries) fails
    /// loudly instead of growing without limit. Persists through the
    /// registry like [`RuleEngine::create_rule`]. Pre-tenancy callers
    /// use this; it stores in the default tenant.
    pub fn create_rule_with_id(
        &self,
        id: String,
        name: String,
        topic_filter: TopicFilter,
        sql_query: Option<String>,
        enabled: bool,
        actions: Vec<RuleAction>,
    ) -> Result<Rule, RuleEngineError> {
        self.create_rule_with_id_in_tenant(
            id,
            "default".to_string(),
            name,
            String::new(),
            serde_json::json!({}),
            topic_filter,
            sql_query,
            enabled,
            actions,
        )
    }

    /// Build and store a rule under a caller-chosen id in one tenant.
    /// The tenant must pass `is_valid_rule_tenant`; blank maps to the
    /// default tenant. Shares the same bounds as `create_rule_with_id`.
    /// Stores the `description` and `metadata` of the management request.
    #[allow(clippy::too_many_arguments)]
    pub fn create_rule_with_id_in_tenant(
        &self,
        id: String,
        tenant: String,
        name: String,
        description: String,
        metadata: serde_json::Value,
        topic_filter: TopicFilter,
        sql_query: Option<String>,
        enabled: bool,
        actions: Vec<RuleAction>,
    ) -> Result<Rule, RuleEngineError> {
        let tenant = broker_config::canonical_rule_tenant(&tenant);
        if !broker_config::is_valid_rule_tenant(&tenant) {
            return Err(RuleEngineError::InvalidRule(format!(
                "tenant {tenant:?} is not valid (field `tenant`)"
            )));
        }
        let rule = self.insert_rule_with_id_in_tenant(
            id,
            tenant,
            name,
            description,
            metadata,
            topic_filter,
            sql_query,
            enabled,
            actions,
        )?;
        self.persist()?;
        Ok(rule)
    }

    /// Live rule count for the management plane (rule-cap reporting).
    /// One short read lock; the publish path never calls this (it holds
    /// a rule snapshot per ingress instead).
    pub fn rule_count(&self) -> usize {
        self.rules.read().len()
    }

    /// Insert a rule under a preserved id in one tenant (boot replay
    /// path). Shares the same SQL validation as
    /// [`RuleEngine::create_rule`]: invalid SQL fails loudly instead of
    /// being skipped. A duplicate id fails loudly. Advances `next_id`
    /// past any numeric `rule-<n>` suffix so later creates never
    /// collide. Does not persist; callers persist once after bulk
    /// loads. Files stored before tenancy carry no tenant and load as
    /// the default tenant.
    #[allow(clippy::too_many_arguments)]
    fn insert_rule_with_id_in_tenant(
        &self,
        id: String,
        tenant: String,
        name: String,
        description: String,
        metadata: serde_json::Value,
        topic_filter: TopicFilter,
        sql_query: Option<String>,
        enabled: bool,
        mut actions: Vec<RuleAction>,
    ) -> Result<Rule, RuleEngineError> {
        // The upstream parser only accepts bare-word stream names after
        // FROM, while MQTT rules naturally reference quoted topic filters
        // (`FROM "sensors/+"`). Per-record evaluation keys off the rule's
        // `topic_filter` and never reads `stmt.from`, so the target is
        // normalized to a dummy identifier before parsing. The original
        // SQL string is preserved verbatim on the rule.
        //
        // A trailing `INTO connector("<id>")` clause desugars to an
        // equivalent `ForwardConnector` action (deduped): the streaming
        // bridge to external sinks without a separate code path. The
        // stored `sql_query` keeps the original text for display.
        let stripped_sql = match &sql_query {
            Some(sql) => {
                let (stripped, into_connector) = split_into_connector(sql)?;
                if let Some(connector_id) = into_connector {
                    let already = actions.iter().any(|action| {
                        matches!(action, RuleAction::ForwardConnector { connector_id: id } if id == &connector_id)
                    });
                    if !already {
                        actions.push(RuleAction::ForwardConnector { connector_id });
                    }
                }
                Some(stripped)
            }
            None => None,
        };
        // R-TENANT: a connector forward carries no tenant label. Refuse
        // one outside the default tenant after the SQL `INTO connector`
        // desugar, so every create, replace, boot and reload path is
        // gated at the engine.
        if tenant != "default"
            && actions
                .iter()
                .any(|action| matches!(action, RuleAction::ForwardConnector { .. }))
        {
            return Err(RuleEngineError::InvalidRule(format!(
                "rule tenant {tenant:?} cannot forward to connector actions"
            )));
        }
        let (parsed_query, std_foreach, std_sources, std_has_wildcard, std_explicit_keys) =
            match &stripped_sql {
                Some(sql) => {
                    let (stmt, foreach, sources, has_wildcard, explicit_keys) =
                        parse_rule_sql(sql)?;
                    (Some(stmt), foreach, sources, has_wildcard, explicit_keys)
                }
                None => (None, None, Vec::new(), false, Vec::new()),
            };
        if let Some(stmt) = parsed_query.as_ref() {
            if matches!(stmt.window, Some(rekuiper_sql::WindowDef::State { .. })) {
                return Err(RuleEngineError::InvalidRule(
                    "state window cannot emit in this build (field `sql_query`)".to_string(),
                ));
            }
        }
        if self.rules.read().contains_key(&id) {
            return Err(RuleEngineError::InvalidRule(format!(
                "duplicate rule id: {id}"
            )));
        }
        // Rule cap (management-plane bound): a full store fails loudly
        // instead of growing without limit. Reason: the list handler
        // clones the whole snapshot once per request, so the catalogue
        // must stay small and bounded. Checked after SQL compilation
        // (a broken rule is rejected as invalid either way) and after
        // the duplicate check (mirroring the action directory: duplicate
        // before full), so a duplicate id on a full store still reports
        // the duplicate.
        {
            let live = self.rules.read().len();
            if live >= broker_config::MAX_RULES {
                return Err(RuleEngineError::InvalidRule(format!(
                    "rule store holds {live} entries, at most {}",
                    broker_config::MAX_RULES
                )));
            }
        }
        if let Some(suffix) = id.strip_prefix("rule-") {
            if let Ok(n) = suffix.parse::<u64>() {
                self.next_id.fetch_max(n + 1, Ordering::SeqCst);
            }
        }
        let tier = match &parsed_query {
            Some(stmt) if stmt.window.is_some() => RuleTier::Enterprise,
            _ => RuleTier::Community,
        };
        let tenant = broker_config::canonical_rule_tenant(&tenant);
        if !broker_config::is_valid_rule_tenant(&tenant) {
            return Err(RuleEngineError::InvalidRule(format!(
                "tenant {tenant:?} is not valid (field `tenant`)"
            )));
        }
        let std_topic_filters = compile_std_topic_filters(&std_sources);
        let rule = Rule {
            id: id.clone(),
            name,
            description,
            metadata,
            tenant,
            topic_filter,
            sql_query,
            parsed_query,
            std_foreach,
            std_sources,
            std_topic_filters,
            std_has_wildcard,
            std_explicit_keys,
            tier,
            enabled,
            actions,
            matched_cnt: Arc::new(AtomicU64::new(0)),
            passed_cnt: Arc::new(AtomicU64::new(0)),
            failed_cnt: Arc::new(AtomicU64::new(0)),
            no_result_cnt: Arc::new(AtomicU64::new(0)),
            actions_total_cnt: Arc::new(AtomicU64::new(0)),
            actions_success_cnt: Arc::new(AtomicU64::new(0)),
            actions_failed_cnt: Arc::new(AtomicU64::new(0)),
        };
        let rule_arc = Arc::new(rule);
        self.rules.write().insert(id.clone(), rule_arc.clone());
        // Keep event flags in sync (RQ-04): recompute from all rules.
        self.refresh_event_flags();
        // Eager task start when a runtime is available and the rule is
        // enabled; otherwise the first matching ingress spawns it lazily
        // (see dispatch_ingress). A disabled windowed rule owns no task
        // so bus fan-out can never deliver through it.
        // Tier labels stay for display: the window clause, not the tier,
        // selects the runtime path.
        if rule_arc.enabled
            && rule_arc
                .parsed_query
                .as_ref()
                .is_some_and(|stmt| stmt.window.is_some())
        {
            self.ensure_rule_task(&rule_arc);
        }
        Ok(rule_arc.as_ref().clone())
    }

    /// Seed from a validated snapshot root. An empty snapshot yields
    /// today's empty behaviour (no rules). Memory-only: mutations are
    /// not persisted.
    ///
    /// Boot replays the snapshot through the same validated create path
    /// as [`RuleEngine::create_rule`]: an invalid topic filter, action,
    /// or SQL fails loudly instead of being skipped silently.
    ///
    /// Rule metric counters (`matched_cnt` etc.) are deliberately NOT
    /// persisted: they are monotonic runtime counters that reset to zero
    /// on every rebuild.
    pub fn from_snapshot(conf: &broker_config::RulesConf) -> Result<Self, RuleEngineError> {
        let engine = Self::new(1024, BackpressurePolicy::DropOldest);
        engine.seed_from_snapshot(conf)?;
        Ok(engine)
    }

    /// Seed from the registry's current snapshot and persist every later
    /// rule mutation back through it. This is the kernel boot path: an
    /// empty snapshot yields today's empty behaviour, and an invalid
    /// stored rule fails boot loudly.
    pub fn from_registry(
        registry: &Arc<broker_config::ConfigRegistry>,
    ) -> Result<Self, RuleEngineError> {
        let engine = Self::from_snapshot(&registry.snapshot().rules)?;
        *engine.registry.write() = Some(Arc::clone(registry));
        Ok(engine)
    }

    /// Attach the registry and replace the current contents with its
    /// snapshot, in place on the same instance. Invalid stored rules
    /// fail loudly; metric counters reset to zero (see
    /// [`RuleEngine::from_snapshot`]).
    pub fn seed_from_registry(
        &self,
        registry: &Arc<broker_config::ConfigRegistry>,
    ) -> Result<(), RuleEngineError> {
        self.seed_from_snapshot(&registry.snapshot().rules)?;
        *self.registry.write() = Some(Arc::clone(registry));
        Ok(())
    }

    /// Replace all rules with one validated snapshot root (M1-05 runtime
    /// apply path for the `rules` root).
    ///
    /// Runs the same validated create path as boot; driven afterwards by
    /// publish ingress (each ingress evaluates this store). Window workers
    /// of replaced rules are aborted so no orphan task survives.
    pub fn apply_snapshot_conf(
        &self,
        conf: &broker_config::RulesConf,
    ) -> Result<(), RuleEngineError> {
        self.seed_from_snapshot(conf)
    }

    /// Replace all rules with the snapshot contents through the validated
    /// create path. Existing rule tasks are stopped and aborted first so no
    /// orphan task survives its rule; `next_id` restarts at 1 and
    /// advances past every restored numeric suffix.
    fn seed_from_snapshot(&self, conf: &broker_config::RulesConf) -> Result<(), RuleEngineError> {
        conf.validate()
            .map_err(|e| RuleEngineError::InvalidRule(format!("invalid rules snapshot: {e}")))?;
        // Refuse a non-default forward before the clear, so a rejected
        // reload or restore keeps the running rules. The per-entry gate
        // below still covers a direct create or replace.
        for entry in &conf.rules {
            let tenant = broker_config::canonical_rule_tenant(&entry.tenant);
            let explicit = entry.actions.iter().any(|action| {
                matches!(
                    action,
                    broker_config::RuleActionEntry::ForwardConnector { .. }
                )
            });
            let from_sql = match &entry.sql_query {
                Some(sql) => split_into_connector(sql)?.1.is_some(),
                None => false,
            };
            if tenant != "default" && (explicit || from_sql) {
                return Err(RuleEngineError::InvalidRule(format!(
                    "rule tenant {tenant:?} cannot forward to connector actions"
                )));
            }
        }
        for (_, task) in self.rule_tasks.write().drain() {
            *task.status.write() = "stopped".to_string();
            task.eval.abort();
            task.feeder.abort();
            task.sink_worker.abort();
        }
        self.rules.write().clear();
        self.next_id.store(1, Ordering::SeqCst);
        // Re-seeding replaces every rule, so the RQ-04 event flags and the
        // aggregate drain gate must clear too: stale flags from a prior seed
        // would otherwise keep enqueueing events and draining the publish
        // path even with no event rules selected.
        for flag in &self.event_flags {
            flag.store(false, Ordering::Relaxed);
        }
        self.event_any_selected.store(false, Ordering::Relaxed);
        for entry in &conf.rules {
            let topic_filter = TopicFilter::new(&entry.topic_filter).map_err(|e| {
                RuleEngineError::InvalidRule(format!("invalid topic_filter for {}: {e}", entry.id))
            })?;
            let mut actions = Vec::with_capacity(entry.actions.len());
            for action in &entry.actions {
                actions.push(Self::action_from_entry(&entry.id, action)?);
            }
            // Files stored before tenancy carry no tenant. The serde
            // default maps them to the default tenant here.
            let tenant = broker_config::canonical_rule_tenant(&entry.tenant);
            self.insert_rule_with_id_in_tenant(
                entry.id.clone(),
                tenant,
                entry.name.clone(),
                entry.description.clone(),
                entry.metadata.clone(),
                topic_filter,
                entry.sql_query.clone(),
                entry.enabled,
                actions,
            )?;
        }
        Ok(())
    }

    /// Export the current contents as a validated config root: rules
    /// sorted by id so the persisted file is deterministic, with the
    /// full action list (every variant field, no display coercion).
    /// Metric counters are runtime-only and are never exported.
    fn export_conf(&self) -> broker_config::RulesConf {
        let mut rules: Vec<Rule> = self
            .rules
            .read()
            .values()
            .map(|arc| arc.as_ref().clone())
            .collect();
        rules.sort_by(|a, b| a.id.cmp(&b.id));
        let entries = rules
            .iter()
            .map(|rule| broker_config::RuleEntry {
                id: rule.id.clone(),
                name: rule.name.clone(),
                description: rule.description.clone(),
                metadata: rule.metadata.clone(),
                tenant: rule.tenant.clone(),
                topic_filter: rule.topic_filter.as_str().to_string(),
                sql_query: rule.sql_query.clone(),
                enabled: rule.enabled,
                actions: rule.actions.iter().map(Self::action_to_entry).collect(),
            })
            .collect();
        broker_config::RulesConf { rules: entries }
    }

    /// Commit the exported root and atomically save it. A no-op without
    /// a registry; any commit or save failure surfaces as
    /// [`RuleEngineError::Persist`] so callers answer 500 and the loss
    /// is never silent.
    fn persist(&self) -> Result<(), RuleEngineError> {
        let Some(registry) = self.registry.read().clone() else {
            return Ok(());
        };
        let _guard = self.save_lock.lock();
        let conf = self.export_conf();
        registry
            .commit_rules(conf)
            .map_err(|e| RuleEngineError::Persist(e.to_string()))?;
        registry
            .save()
            .map_err(|e| RuleEngineError::Persist(e.to_string()))?;
        Ok(())
    }

    /// Map one live action to its lossless persisted form.
    fn action_to_entry(action: &RuleAction) -> broker_config::RuleActionEntry {
        match action {
            RuleAction::Republish { topic, qos } => broker_config::RuleActionEntry::Republish {
                topic: topic.as_str().to_string(),
                qos: (*qos).into(),
            },
            RuleAction::Log => broker_config::RuleActionEntry::Log,
            RuleAction::ForwardConnector { connector_id } => {
                broker_config::RuleActionEntry::ForwardConnector {
                    connector_id: connector_id.clone(),
                }
            }
            RuleAction::RepublishFn { args } => {
                broker_config::RuleActionEntry::RepublishFunction { args: args.clone() }
            }
            RuleAction::Console => broker_config::RuleActionEntry::Console,
        }
    }

    /// Map one persisted action back to its live form through the same
    /// validation the API applies: invalid topics or QoS fail loudly.
    fn action_from_entry(
        rule_id: &str,
        entry: &broker_config::RuleActionEntry,
    ) -> Result<RuleAction, RuleEngineError> {
        match entry {
            broker_config::RuleActionEntry::Republish { topic, qos } => {
                let topic = Topic::new(topic).map_err(|e| {
                    RuleEngineError::InvalidRule(format!(
                        "invalid republish topic for {rule_id}: {e}"
                    ))
                })?;
                let qos = QoS::try_from(*qos).map_err(|e| {
                    RuleEngineError::InvalidRule(format!(
                        "invalid republish qos for {rule_id}: {e}"
                    ))
                })?;
                Ok(RuleAction::Republish { topic, qos })
            }
            broker_config::RuleActionEntry::Log => Ok(RuleAction::Log),
            broker_config::RuleActionEntry::Console => Ok(RuleAction::Console),
            broker_config::RuleActionEntry::RepublishFunction { args } => {
                validate_republish_args(args).map_err(|e| {
                    RuleEngineError::InvalidRule(format!(
                        "invalid republish function for {rule_id}: {e}"
                    ))
                })?;
                Ok(RuleAction::RepublishFn { args: args.clone() })
            }
            broker_config::RuleActionEntry::ForwardConnector { connector_id } => {
                if connector_id.trim().is_empty() {
                    return Err(RuleEngineError::InvalidRule(format!(
                        "invalid connector_id for {rule_id}: must not be empty (field `connector_id`)"
                    )));
                }
                Ok(RuleAction::ForwardConnector {
                    connector_id: connector_id.clone(),
                })
            }
        }
    }

    pub fn get_rule(&self, id: &str) -> Option<Rule> {
        self.rules.read().get(id).map(|arc| arc.as_ref().clone())
    }

    pub fn list_rules(&self) -> Vec<Rule> {
        let mut rules: Vec<Rule> = self
            .rules
            .read()
            .values()
            .map(|arc| arc.as_ref().clone())
            .collect();
        rules.sort_by(|a, b| a.id.cmp(&b.id));
        rules
    }

    /// Remove a rule (false when unknown; unknown ids persist nothing).
    /// Persists through the registry when one is attached; a save
    /// failure is returned and the in-memory removal stays (commit
    /// precedes the atomic save). Stops and deletes the rule task first
    /// (status flips to `stopped`, all three handles abort) so no orphan
    /// task survives its rule and the aborted task delivers nothing more.
    pub fn remove_rule(&self, id: &str) -> Result<bool, RuleEngineError> {
        if let Some(task) = self.rule_tasks.write().remove(id) {
            *task.status.write() = "stopped".to_string();
            task.eval.abort();
            task.feeder.abort();
            task.sink_worker.abort();
        }
        let removed = self.rules.write().remove(id).is_some();
        if removed {
            self.refresh_event_flags();
            self.persist()?;
        }
        Ok(removed)
    }

    /// Replace a stored rule in full (W3-14, C06-22): recompile the new
    /// SQL before touching the map (malformed bodies are rejected and the
    /// old rule is kept), then swap one compiled snapshot in place and
    /// persist. Returns `None` when the id is unknown (caller maps to 404
    /// and creates nothing). A replace never grows the map toward
    /// [`broker_config::MAX_RULES`], so an update can never trip the store
    /// cap. Counters are preserved from the old rule (the same `Arc`s are
    /// carried over, mirroring the per-action metric continuity across
    /// config replaces); only delete or an explicit metrics reset zeroes
    /// them.
    /// Management-plane only: one short write lock swapping a single
    /// validated entry (plus the registry save lock on persist); the
    /// publish path keeps reading its per-ingress snapshot and never takes
    /// this lock beyond the snapshot clone in `dispatch_ingress`.
    // TODO(parity): Do per-rule counters reset on replace or persist across updates?
    #[allow(clippy::too_many_arguments)]
    pub fn replace_rule(
        &self,
        id: &str,
        name: String,
        description: String,
        metadata: serde_json::Value,
        topic_filter: TopicFilter,
        sql_query: Option<String>,
        enabled: bool,
        actions: Vec<RuleAction>,
    ) -> Result<Option<Rule>, RuleEngineError> {
        let tenant = self
            .rules
            .read()
            .get(id)
            .map(|arc| arc.tenant.clone())
            .unwrap_or_else(|| "default".to_string());
        self.replace_rule_in_tenant(
            id,
            tenant,
            name,
            description,
            metadata,
            topic_filter,
            sql_query,
            enabled,
            actions,
        )
    }

    /// Replace a stored rule in full in one tenant. Same bounds as
    /// `replace_rule`. The tenant must pass `is_valid_rule_tenant`.
    /// Counters carry over from the old rule. Stores the `description`
    /// and `metadata` of the management request.
    #[allow(clippy::too_many_arguments)]
    pub fn replace_rule_in_tenant(
        &self,
        id: &str,
        tenant: String,
        name: String,
        description: String,
        metadata: serde_json::Value,
        topic_filter: TopicFilter,
        sql_query: Option<String>,
        enabled: bool,
        mut actions: Vec<RuleAction>,
    ) -> Result<Option<Rule>, RuleEngineError> {
        let old = self.rules.read().get(id).map(|arc| arc.as_ref().clone());
        let Some(old) = old else {
            return Ok(None);
        };
        let stripped_sql = match &sql_query {
            Some(sql) => {
                let (stripped, into_connector) = split_into_connector(sql)?;
                if let Some(connector_id) = into_connector {
                    let already = actions.iter().any(|action| {
                        matches!(action, RuleAction::ForwardConnector { connector_id: cid } if cid == &connector_id)
                    });
                    if !already {
                        actions.push(RuleAction::ForwardConnector { connector_id });
                    }
                }
                Some(stripped)
            }
            None => None,
        };
        // R-TENANT: a connector forward carries no tenant label. Refuse
        // one outside the default tenant after the SQL `INTO connector`
        // desugar, so every create, replace, boot and reload path is
        // gated at the engine.
        if tenant != "default"
            && actions
                .iter()
                .any(|action| matches!(action, RuleAction::ForwardConnector { .. }))
        {
            return Err(RuleEngineError::InvalidRule(format!(
                "rule tenant {tenant:?} cannot forward to connector actions"
            )));
        }
        let (parsed_query, std_foreach, std_sources, std_has_wildcard, std_explicit_keys) =
            match &stripped_sql {
                Some(sql) => {
                    let (stmt, foreach, sources, has_wildcard, explicit_keys) =
                        parse_rule_sql(sql)?;
                    (Some(stmt), foreach, sources, has_wildcard, explicit_keys)
                }
                None => (None, None, Vec::new(), false, Vec::new()),
            };
        if let Some(stmt) = parsed_query.as_ref() {
            if matches!(stmt.window, Some(rekuiper_sql::WindowDef::State { .. })) {
                return Err(RuleEngineError::InvalidRule(
                    "state window cannot emit in this build (field `sql_query`)".to_string(),
                ));
            }
        }
        let tier = match &parsed_query {
            Some(stmt) if stmt.window.is_some() => RuleTier::Enterprise,
            _ => RuleTier::Community,
        };
        let tenant = broker_config::canonical_rule_tenant(&tenant);
        if !broker_config::is_valid_rule_tenant(&tenant) {
            return Err(RuleEngineError::InvalidRule(format!(
                "tenant {tenant:?} is not valid (field `tenant`)"
            )));
        }
        let std_topic_filters = compile_std_topic_filters(&std_sources);
        let rule = Rule {
            id: id.to_string(),
            name,
            description,
            metadata,
            tenant,
            topic_filter,
            sql_query,
            parsed_query,
            std_foreach,
            std_sources,
            std_topic_filters,
            std_has_wildcard,
            std_explicit_keys,
            tier,
            enabled,
            actions,
            matched_cnt: old.matched_cnt.clone(),
            passed_cnt: old.passed_cnt.clone(),
            failed_cnt: old.failed_cnt.clone(),
            no_result_cnt: old.no_result_cnt.clone(),
            actions_total_cnt: old.actions_total_cnt.clone(),
            actions_success_cnt: old.actions_success_cnt.clone(),
            actions_failed_cnt: old.actions_failed_cnt.clone(),
        };
        let rule_arc = Arc::new(rule);
        if let Some(task) = self.rule_tasks.write().remove(id) {
            *task.status.write() = "stopped".to_string();
            task.eval.abort();
            task.feeder.abort();
            task.sink_worker.abort();
        }
        self.rules.write().insert(id.to_string(), rule_arc.clone());
        if rule_arc.enabled
            && rule_arc
                .parsed_query
                .as_ref()
                .is_some_and(|stmt| stmt.window.is_some())
        {
            self.ensure_rule_task(&rule_arc);
        }
        self.refresh_event_flags();
        self.persist()?;
        Ok(Some(rule_arc.as_ref().clone()))
    }

    /// Enable or disable a rule (false when unknown; unknown ids persist
    /// nothing). Persists through the registry when one is attached.
    /// Disabling stops and deletes the rule task first (status flips to
    /// `stopped`, all three handles abort) so a disabled windowed rule
    /// delivers nothing more, including via another rule's bus fan-out;
    /// re-enabling defers the task to the next matching ingress.
    pub fn set_rule_enabled(&self, id: &str, enabled: bool) -> Result<bool, RuleEngineError> {
        let changed = {
            let mut rules = self.rules.write();
            if let Some(arc) = rules.get(id) {
                let mut rule = arc.as_ref().clone();
                let was_enabled = rule.enabled;
                rule.enabled = enabled;
                rules.insert(id.to_string(), Arc::new(rule));
                was_enabled != enabled
            } else {
                return Ok(false);
            }
        };
        if changed {
            if !enabled {
                if let Some(task) = self.rule_tasks.write().remove(id) {
                    *task.status.write() = "stopped".to_string();
                    task.eval.abort();
                    task.feeder.abort();
                    task.sink_worker.abort();
                }
            }
            self.refresh_event_flags();
            self.persist()?;
        }
        Ok(changed)
    }

    /// Recompute event flags from all enabled rules.
    /// Runs on rule mutations only, never on the publish path.
    fn refresh_event_flags(&self) {
        let mut selected = [false; EventSource::COUNT];
        {
            let rules = self.rules.read();
            for rule in rules.values() {
                if !rule.enabled {
                    continue;
                }
                for source in &rule.std_sources {
                    if let StdSource::Event(name) = source {
                        if let Some(es) = EventSource::parse(name) {
                            selected[es as usize] = true;
                        }
                    }
                }
            }
        }
        for (flag, on) in self.event_flags.iter().zip(selected) {
            flag.store(on, Ordering::Relaxed);
        }
        self.event_any_selected
            .store(selected.iter().any(|b| *b), Ordering::Relaxed);
    }

    /// Check if any enabled rule selects the given event source.
    /// Public so the kernel checks before it takes a lock or clones.
    #[inline]
    pub fn is_event_source_selected(&self, source: EventSource) -> bool {
        self.event_flags[source as usize].load(Ordering::Relaxed)
    }

    /// True when at least one enabled rule selects any event source.
    /// Public so the kernel skips all event work on the publish path
    /// with one relaxed load when no event rule exists.
    #[inline]
    pub fn has_any_event_selected(&self) -> bool {
        self.event_any_selected.load(Ordering::Relaxed)
    }

    /// Build and fire an event record for the given source.
    /// Returns early if no rule selects this source (zero cost).
    /// The `build_record` closure is only called when the flag is set.
    fn fire_event<F>(&self, source: EventSource, build_record: F)
    where
        F: FnOnce() -> serde_json::Map<String, serde_json::Value>,
    {
        if !self.is_event_source_selected(source) {
            return;
        }
        let record = build_record();
        // INVARIANT: event source string is broker-generated and conforms to valid topic syntax.
        let topic = Topic::new(source.as_str().to_string()).expect("valid event source topic");
        // INVARIANT: event record is a broker-constructed map containing only JSON-serializable values.
        let payload = Bytes::from(serde_json::to_vec(&record).expect("event record serializes"));
        // Enqueue for deferred action execution only: matching, matchedCnt
        // accounting and rule actions all happen once, in the drain loop of
        // dispatch_ingress (the broker's publish path). fire_event itself
        // stays synchronous and never snapshots the rule table.
        let event = StreamEvent::new(topic, payload);
        // Push to the bounded ingress queue for durability and deferred
        // action execution; try_push never blocks.
        let _ = self.input.try_push(event);
    }

    /// List all event sources served by this broker (RQ-04).
    /// Returns the source names and their field schemas.
    pub fn list_event_sources(&self) -> Vec<EventSourceInfo> {
        EventSource::all()
            .iter()
            .map(|es| EventSourceInfo {
                name: es.as_str().to_string(),
                fields: es.fields().iter().map(|s| s.to_string()).collect(),
            })
            .collect()
    }

    // --- Event firing methods (RQ-04) ---
    // Each method builds the event record with the documented fields
    // and fires it if any rule selects the source.

    /// Fire $events/client_connected
    pub fn fire_client_connected(&self, event: ClientConnectedEvent) {
        self.fire_event(EventSource::ClientConnected, || {
            let mut m = serde_json::Map::new();
            m.insert(
                "clientid".to_string(),
                serde_json::Value::String(event.client_id),
            );
            if let Some(u) = event.username {
                m.insert("username".to_string(), serde_json::Value::String(u));
            }
            if let Some(p) = event.peerhost {
                m.insert("peerhost".to_string(), serde_json::Value::String(p));
            }
            m.insert(
                "keepalive".to_string(),
                serde_json::Value::Number(event.keepalive.into()),
            );
            m.insert(
                "proto_ver".to_string(),
                serde_json::Value::Number(event.proto_ver.into()),
            );
            m.insert(
                "clean_start".to_string(),
                serde_json::Value::Bool(event.clean_start),
            );
            m.insert(
                "conn_ack_code".to_string(),
                serde_json::Value::Number(event.conn_ack_code.into()),
            );
            m
        });
    }

    /// Fire $events/client_disconnected
    pub fn fire_client_disconnected(&self, event: ClientDisconnectedEvent) {
        self.fire_event(EventSource::ClientDisconnected, || {
            let mut m = serde_json::Map::new();
            m.insert(
                "clientid".to_string(),
                serde_json::Value::String(event.client_id),
            );
            if let Some(u) = event.username {
                m.insert("username".to_string(), serde_json::Value::String(u));
            }
            if let Some(p) = event.peerhost {
                m.insert("peerhost".to_string(), serde_json::Value::String(p));
            }
            m.insert(
                "reason".to_string(),
                serde_json::Value::String(event.reason),
            );
            if let Some(code) = event.disconnect_code {
                m.insert(
                    "disconnect_code".to_string(),
                    serde_json::Value::Number(code.into()),
                );
            }
            m
        });
    }

    /// Fire $events/client_connack
    pub fn fire_client_connack(&self, event: ClientConnackEvent) {
        self.fire_event(EventSource::ClientConnack, || {
            let mut m = serde_json::Map::new();
            m.insert(
                "clientid".to_string(),
                serde_json::Value::String(event.client_id),
            );
            if let Some(u) = event.username {
                m.insert("username".to_string(), serde_json::Value::String(u));
            }
            if let Some(p) = event.peerhost {
                m.insert("peerhost".to_string(), serde_json::Value::String(p));
            }
            m.insert(
                "return_code".to_string(),
                serde_json::Value::Number(event.return_code.into()),
            );
            m.insert(
                "session_present".to_string(),
                serde_json::Value::Bool(event.session_present),
            );
            m
        });
    }

    /// Fire $events/client_check_authz_complete
    pub fn fire_client_check_authz_complete(&self, event: ClientCheckAuthzCompleteEvent) {
        self.fire_event(EventSource::ClientCheckAuthzComplete, || {
            let mut m = serde_json::Map::new();
            m.insert(
                "clientid".to_string(),
                serde_json::Value::String(event.client_id),
            );
            if let Some(u) = event.username {
                m.insert("username".to_string(), serde_json::Value::String(u));
            }
            if let Some(p) = event.peerhost {
                m.insert("peerhost".to_string(), serde_json::Value::String(p));
            }
            m.insert(
                "action".to_string(),
                serde_json::Value::String(event.action),
            );
            m.insert("topic".to_string(), serde_json::Value::String(event.topic));
            m.insert(
                "qos".to_string(),
                serde_json::Value::Number(event.qos.into()),
            );
            if let Some(retain) = event.retain {
                m.insert("retain".to_string(), serde_json::Value::Bool(retain));
            }
            m.insert("allow".to_string(), serde_json::Value::Bool(event.allow));
            m.insert(
                "source".to_string(),
                serde_json::Value::String(event.source),
            );
            m
        });
    }

    /// Fire $events/session_subscribed
    pub fn fire_session_subscribed(&self, event: SessionSubscribedEvent) {
        self.fire_event(EventSource::SessionSubscribed, || {
            let mut m = serde_json::Map::new();
            m.insert(
                "clientid".to_string(),
                serde_json::Value::String(event.client_id),
            );
            if let Some(u) = event.username {
                m.insert("username".to_string(), serde_json::Value::String(u));
            }
            if let Some(p) = event.peerhost {
                m.insert("peerhost".to_string(), serde_json::Value::String(p));
            }
            m.insert("topic".to_string(), serde_json::Value::String(event.topic));
            m.insert(
                "qos".to_string(),
                serde_json::Value::Number(event.qos.into()),
            );
            m.insert("nl".to_string(), serde_json::Value::Number(event.nl.into()));
            m.insert(
                "rap".to_string(),
                serde_json::Value::Number(event.rap.into()),
            );
            m.insert("rh".to_string(), serde_json::Value::Number(event.rh.into()));
            m
        });
    }

    /// Fire $events/session_unsubscribed
    pub fn fire_session_unsubscribed(&self, event: SessionUnsubscribedEvent) {
        self.fire_event(EventSource::SessionUnsubscribed, || {
            let mut m = serde_json::Map::new();
            m.insert(
                "clientid".to_string(),
                serde_json::Value::String(event.client_id),
            );
            if let Some(u) = event.username {
                m.insert("username".to_string(), serde_json::Value::String(u));
            }
            if let Some(p) = event.peerhost {
                m.insert("peerhost".to_string(), serde_json::Value::String(p));
            }
            m.insert("topic".to_string(), serde_json::Value::String(event.topic));
            m
        });
    }

    /// Fire $events/message_delivered
    pub fn fire_message_delivered(&self, event: MessageDeliveredEvent) {
        self.fire_event(EventSource::MessageDelivered, || {
            let mut m = serde_json::Map::new();
            m.insert(
                "clientid".to_string(),
                serde_json::Value::String(event.client_id),
            );
            if let Some(u) = event.username {
                m.insert("username".to_string(), serde_json::Value::String(u));
            }
            if let Some(p) = event.peerhost {
                m.insert("peerhost".to_string(), serde_json::Value::String(p));
            }
            m.insert("topic".to_string(), serde_json::Value::String(event.topic));
            m.insert(
                "qos".to_string(),
                serde_json::Value::Number(event.qos.into()),
            );
            m.insert("retain".to_string(), serde_json::Value::Bool(event.retain));
            m.insert(
                "payload".to_string(),
                serde_json::Value::String(String::from_utf8_lossy(&event.payload).to_string()),
            );
            if let Some(id) = event.msg_id {
                m.insert("msg_id".to_string(), serde_json::Value::Number(id.into()));
            }
            m
        });
    }

    /// Fire $events/message_acked
    pub fn fire_message_acked(&self, event: MessageAckedEvent) {
        self.fire_event(EventSource::MessageAcked, || {
            let mut m = serde_json::Map::new();
            m.insert(
                "clientid".to_string(),
                serde_json::Value::String(event.client_id),
            );
            if let Some(u) = event.username {
                m.insert("username".to_string(), serde_json::Value::String(u));
            }
            if let Some(p) = event.peerhost {
                m.insert("peerhost".to_string(), serde_json::Value::String(p));
            }
            m.insert("topic".to_string(), serde_json::Value::String(event.topic));
            m.insert(
                "qos".to_string(),
                serde_json::Value::Number(event.qos.into()),
            );
            m.insert(
                "msg_id".to_string(),
                serde_json::Value::Number(event.msg_id.into()),
            );
            m
        });
    }

    /// Fire $events/message_dropped
    pub fn fire_message_dropped(&self, event: MessageDroppedEvent) {
        self.fire_event(EventSource::MessageDropped, || {
            let mut m = serde_json::Map::new();
            m.insert(
                "clientid".to_string(),
                serde_json::Value::String(event.client_id),
            );
            if let Some(u) = event.username {
                m.insert("username".to_string(), serde_json::Value::String(u));
            }
            if let Some(p) = event.peerhost {
                m.insert("peerhost".to_string(), serde_json::Value::String(p));
            }
            m.insert("topic".to_string(), serde_json::Value::String(event.topic));
            m.insert(
                "qos".to_string(),
                serde_json::Value::Number(event.qos.into()),
            );
            m.insert(
                "reason".to_string(),
                serde_json::Value::String(event.reason),
            );
            m
        });
    }

    /// Fire $events/delivery_dropped
    pub fn fire_delivery_dropped(&self, event: DeliveryDroppedEvent) {
        self.fire_event(EventSource::DeliveryDropped, || {
            let mut m = serde_json::Map::new();
            m.insert(
                "clientid".to_string(),
                serde_json::Value::String(event.client_id),
            );
            if let Some(u) = event.username {
                m.insert("username".to_string(), serde_json::Value::String(u));
            }
            if let Some(p) = event.peerhost {
                m.insert("peerhost".to_string(), serde_json::Value::String(p));
            }
            m.insert("topic".to_string(), serde_json::Value::String(event.topic));
            m.insert(
                "qos".to_string(),
                serde_json::Value::Number(event.qos.into()),
            );
            m.insert(
                "reason".to_string(),
                serde_json::Value::String(event.reason),
            );
            m
        });
    }

    /// Record one action's enable flag in the lock-free dispatch snapshot
    /// (W3-04, C06-07). Management-plane only: the action store owns the
    /// flag and persistence, this snapshot only mirrors it so the publish
    /// path never takes the store lock. Enabling removes the id, disabling
    /// inserts it; unknown ids can be recorded (harmless: no rule forwards
    /// to them until one does).
    pub fn set_action_enabled(&self, id: &str, enabled: bool) {
        if self.is_action_enabled(id) == enabled {
            return;
        }
        let current = self.action_disabled_snapshot();
        let mut next = current.as_ref().clone();
        if enabled {
            next.remove(id);
        } else {
            next.insert(id.to_string());
        }
        self.action_disabled.store(Arc::new(next));
    }

    /// Drop one action id from the dispatch snapshot (W3-04, C06-07).
    /// Management-plane only: called after the action store deletes the
    /// entry so a later re-create with the same id starts enabled.
    pub fn remove_action(&self, id: &str) {
        let current = self.action_disabled.load_full();
        if !current.contains(id) {
            return;
        }
        let mut next = current.as_ref().clone();
        next.remove(id);
        self.action_disabled.store(Arc::new(next));
    }

    /// Replace the whole dispatch snapshot from `(id, enable)` pairs
    /// (W3-04, C06-07). Boot path: rebuilds the snapshot from the action
    /// store after a restart so the persisted flags take effect from the
    /// first publish. Management-plane/boot only; the publish path only
    /// loads.
    pub fn sync_action_enables(&self, enables: &[(String, bool)]) {
        let mut disabled = HashSet::with_capacity(enables.len());
        for (id, enabled) in enables {
            if !enabled {
                disabled.insert(id.clone());
            }
        }
        self.action_disabled.store(Arc::new(disabled));
    }

    /// Lock-free snapshot of disabled action ids for the publish path
    /// (W3-04, C06-07): one `Arc` clone, no lock, no allocation beyond
    /// the refcount bump. Disabled forwards are skipped at fire time.
    pub fn action_disabled_snapshot(&self) -> Arc<HashSet<String>> {
        self.action_disabled.load_full()
    }

    /// Whether one action currently participates (W3-04, C06-07).
    /// Management-plane/tests helper; the publish path uses
    /// [`RuleEngine::action_disabled_snapshot`] once per ingress instead.
    pub fn is_action_enabled(&self, id: &str) -> bool {
        !self.action_disabled_snapshot().contains(id)
    }

    /// Ensure a zeroed per-action metric record exists for `id` (W3-05,
    /// C06-08/09). Management-plane only: called when an action is
    /// created (or on boot for every stored action). Swaps the whole map;
    /// the publish path only loads. Existing counters are kept.
    pub fn ensure_action_metrics(&self, id: &str) {
        let current = self.action_metrics.load_full();
        if current.contains_key(id) {
            return;
        }
        let mut next = current.as_ref().clone();
        next.entry(id.to_string())
            .or_insert_with(|| Arc::new(ActionMetrics::new()));
        self.action_metrics.store(Arc::new(next));
    }

    /// Drop the per-action metric record for `id` (W3-05, C06-08/09).
    /// Management-plane only: called after the action store deletes the
    /// entry, so a later re-create starts at zero. Unknown ids are a
    /// no-op.
    pub fn remove_action_metrics(&self, id: &str) {
        let current = self.action_metrics.load_full();
        if !current.contains_key(id) {
            return;
        }
        let mut next = current.as_ref().clone();
        next.remove(id);
        self.action_metrics.store(Arc::new(next));
    }

    /// Rebuild the whole metric map from the live action ids (W3-05,
    /// C06-08/09). Boot path: every stored action gets a zeroed record so
    /// the first publish after a restart attributes from zero. Metrics
    /// never persist: the map is always rebuilt at zero, never loaded from
    /// disk. Management-plane/boot only; the publish path only loads.
    pub fn sync_action_metrics(&self, ids: &[String]) {
        let mut next = HashMap::with_capacity(ids.len());
        for id in ids {
            next.insert(id.clone(), Arc::new(ActionMetrics::new()));
        }
        self.action_metrics.store(Arc::new(next));
    }

    /// Lock-free snapshot of the per-action metric map for the publish
    /// path (W3-05, C06-08/09): one `Arc` clone, no lock, no allocation
    /// beyond the refcount bump. Callers hold the snapshot across one
    /// ingress (or one window flush) and bump atomics through it.
    pub fn action_metrics_snapshot(&self) -> Arc<HashMap<String, Arc<ActionMetrics>>> {
        self.action_metrics.load_full()
    }

    /// Lock-free read of one action's counters for management handlers.
    /// Returns `None` when the id has no record (caller maps to 404).
    /// One `Arc` clone plus one `HashMap` lookup plus relaxed loads; no
    /// lock, no allocation beyond the refcount bump.
    pub fn action_metrics_snapshot_for(&self, id: &str) -> Option<ActionMetricsSnapshot> {
        let snapshot = self.action_metrics.load_full();
        snapshot.get(id).map(|record| record.snapshot())
    }

    /// Lock-free handle to one action's metric record. The action runtime
    /// holds this handle and bumps the counters on its worker path.
    pub fn action_metrics_record(&self, id: &str) -> Option<Arc<ActionMetrics>> {
        self.action_metrics.load_full().get(id).cloned()
    }

    /// Zero one action's counters in place (W3-05, C06-09). Returns false
    /// when the id has no record (caller maps to 404 and creates
    /// nothing). Lock-free atomic stores; the record stays registered.
    pub fn reset_action_metrics(&self, id: &str) -> bool {
        let snapshot = self.action_metrics.load_full();
        match snapshot.get(id) {
            Some(record) => {
                record.reset();
                true
            }
            None => false,
        }
    }

    /// Record one source's enable flag in the lock-free ingress snapshot
    /// (W3-29, C07-18/19). Management-plane only: the source store owns
    /// the flag and persistence, this snapshot only mirrors it so the
    /// publish path never takes the store lock. Enabling removes the id,
    /// disabling inserts it; unknown ids can be recorded (harmless: no
    /// record exists to attribute to until one does).
    pub fn set_source_enabled(&self, id: &str, enabled: bool) {
        if self.is_source_enabled(id) == enabled {
            return;
        }
        let current = self.source_disabled_snapshot();
        let mut next = current.as_ref().clone();
        if enabled {
            next.remove(id);
        } else {
            next.insert(id.to_string());
        }
        self.source_disabled.store(Arc::new(next));
    }

    /// Drop one source id from the ingress snapshot (W3-29, C07-18/19).
    /// Management-plane only: called after the source store deletes the
    /// entry so a later re-create with the same id starts enabled.
    pub fn remove_source(&self, id: &str) {
        let current = self.source_disabled.load_full();
        if !current.contains(id) {
            return;
        }
        let mut next = current.as_ref().clone();
        next.remove(id);
        self.source_disabled.store(Arc::new(next));
    }

    /// Replace the whole ingress snapshot from `(id, enable)` pairs
    /// (W3-29, C07-18/19). Boot path: rebuilds the snapshot from the
    /// source store after a restart so the persisted flags take effect
    /// from the first publish. Management-plane/boot only; the publish
    /// path only loads.
    pub fn sync_source_enables(&self, enables: &[(String, bool)]) {
        let mut disabled = HashSet::with_capacity(enables.len());
        for (id, enabled) in enables {
            if !enabled {
                disabled.insert(id.clone());
            }
        }
        self.source_disabled.store(Arc::new(disabled));
    }

    /// Lock-free snapshot of stopped source ids for the publish path
    /// (W3-29, C07-18/19): one `Arc` clone, no lock, no allocation beyond
    /// the refcount bump. Stopped sources shed ingress into `dropped`.
    pub fn source_disabled_snapshot(&self) -> Arc<HashSet<String>> {
        self.source_disabled.load_full()
    }

    /// Whether one source currently absorbs ingress (W3-29, C07-18/19).
    /// Management-plane/tests helper; the publish path uses
    /// [`RuleEngine::source_disabled_snapshot`] once per ingress instead.
    pub fn is_source_enabled(&self, id: &str) -> bool {
        !self.source_disabled_snapshot().contains(id)
    }

    /// Ensure a zeroed per-source metric record exists for `id` (W3-29,
    /// C07-18/19). Management-plane only: called when a source is
    /// created (or on boot for every stored source). Swaps the whole map;
    /// the publish path only loads. Existing counters are kept.
    pub fn ensure_source_metrics(&self, id: &str) {
        let current = self.source_metrics.load_full();
        if current.contains_key(id) {
            return;
        }
        let mut next = current.as_ref().clone();
        next.entry(id.to_string())
            .or_insert_with(|| Arc::new(SourceMetrics::new()));
        self.source_metrics.store(Arc::new(next));
        self.source_any.store(true, Ordering::Relaxed);
    }

    /// Drop the per-source metric record for `id` (W3-29, C07-18/19).
    /// Management-plane only: called after the source store deletes the
    /// entry, so a later re-create starts at zero. Unknown ids are a
    /// no-op.
    pub fn remove_source_metrics(&self, id: &str) {
        let current = self.source_metrics.load_full();
        if !current.contains_key(id) {
            return;
        }
        let mut next = current.as_ref().clone();
        next.remove(id);
        let empty = next.is_empty();
        self.source_metrics.store(Arc::new(next));
        self.source_any.store(!empty, Ordering::Relaxed);
    }

    /// Rebuild the whole metric map from the live source ids (W3-29,
    /// C07-18/19). Boot path: every stored source gets a zeroed record so
    /// the first publish after a restart attributes from zero. Metrics
    /// never persist: the map is always rebuilt at zero, never loaded from
    /// disk. Management-plane/boot only; the publish path only loads.
    pub fn sync_source_metrics(&self, ids: &[String]) {
        let mut next = HashMap::with_capacity(ids.len());
        for id in ids {
            next.insert(id.clone(), Arc::new(SourceMetrics::new()));
        }
        let empty = next.is_empty();
        self.source_metrics.store(Arc::new(next));
        self.source_any.store(!empty, Ordering::Relaxed);
    }

    /// Lock-free snapshot of the per-source metric map for the publish
    /// path (W3-29, C07-18/19): one `Arc` clone, no lock, no allocation
    /// beyond the refcount bump. Callers hold the snapshot across one
    /// ingress and bump atomics through it.
    pub fn source_metrics_snapshot(&self) -> Arc<HashMap<String, Arc<SourceMetrics>>> {
        self.source_metrics.load_full()
    }

    /// Lock-free read of one source's counters for management handlers.
    /// Returns `None` when the id has no record (caller maps to 404).
    /// One `Arc` clone plus one `HashMap` lookup plus two relaxed
    /// loads; no lock, no allocation beyond the refcount bump.
    pub fn source_metrics_snapshot_for(&self, id: &str) -> Option<(u64, u64)> {
        let snapshot = self.source_metrics.load_full();
        snapshot.get(id).map(|record| record.snapshot())
    }

    /// Zero one source's counters in place (W3-29, C07-19). Returns false
    /// when the id has no record (caller maps to 404 and creates
    /// nothing). Lock-free atomic stores; the record stays registered.
    pub fn reset_source_metrics(&self, id: &str) -> bool {
        let snapshot = self.source_metrics.load_full();
        match snapshot.get(id) {
            Some(record) => {
                record.reset();
                true
            }
            None => false,
        }
    }

    /// Lock-free snapshot of the rule-engine settings for the publish path
    /// (W3-10): one `Arc` clone, no lock, no allocation beyond the
    /// refcount bump. The engine reads this snapshot per ingress, never
    /// the registry or the management store.
    pub fn engine_config_snapshot(&self) -> Arc<broker_config::RuleEngineConf> {
        self.engine_config.load_full()
    }

    /// Swap the fire-path engine settings (W3-10). Hot-apply path: the
    /// [`EngineConfigStore`] subscriber calls this after every validated
    /// management write, and boot calls it once from the registry
    /// snapshot, so persisted settings take effect from the first publish.
    /// Management-plane/boot only; the publish path only loads. Callers
    /// pass validated settings (see
    /// [`broker_config::RuleEngineConf::validate`]); the swap itself is
    /// one atomic store.
    pub fn apply_engine_config(&self, conf: broker_config::RuleEngineConf) {
        self.engine_config.store(Arc::new(conf));
    }

    /// Swap the ingress transformation chain (W3-32). Management-plane
    /// and boot only: every validated transform create/replace swaps the
    /// whole ordered chain, and boot rebuilds it once from the registry
    /// so persisted entries apply from the first publish. The publish
    /// path only loads. One atomic store per swap.
    pub fn sync_transforms(&self, steps: Vec<TransformStep>) {
        self.transforms.store(Arc::new(steps));
    }

    /// Lock-free snapshot of the transformation chain for the publish
    /// path (W3-32): one `Arc` clone, no lock, no allocation beyond the
    /// refcount bump. Callers hold the snapshot across one ingress and
    /// run [`apply_transform_steps`] over it.
    pub fn transform_snapshot(&self) -> Arc<Vec<TransformStep>> {
        self.transforms.load_full()
    }

    /// True when at least one message transformation exists. The publish
    /// path calls this for each message. It does not change the
    /// reference count of the shared list, thus publishers on different
    /// cores do not write to the same cache line when the list is empty.
    pub fn has_transforms(&self) -> bool {
        !self.transforms.load().is_empty()
    }

    /// Ensure a zeroed per-transformation metric record exists for `name`
    /// (W3-36, C08-09/10). Management-plane only: called when a
    /// transformation is created (or on boot for every stored name).
    /// Swaps the whole map; the publish path only loads. Keeps counters.
    pub fn ensure_transform_metrics(&self, name: &str) {
        let current = self.transform_metrics.load_full();
        if current.contains_key(name) {
            return;
        }
        let mut next = current.as_ref().clone();
        next.entry(name.to_string())
            .or_insert_with(|| Arc::new(TransformMetrics::new()));
        self.transform_metrics.store(Arc::new(next));
    }

    /// Drop the per-transformation metric record for `name` (W3-36,
    /// C08-09/10). Management-plane only: called after the chain store
    /// deletes the entry, so a later re-create starts at zero. Unknown
    /// names are a no-op.
    pub fn remove_transform_metrics(&self, name: &str) {
        let current = self.transform_metrics.load_full();
        if !current.contains_key(name) {
            return;
        }
        let mut next = current.as_ref().clone();
        next.remove(name);
        self.transform_metrics.store(Arc::new(next));
    }

    /// Rebuild the whole metric map from the live names (W3-36,
    /// C08-09/10). Boot path: every stored name gets a zeroed record so
    /// the first publish after a restart attributes from zero. Metrics
    /// never persist: the map rebuilds at zero, never loads from disk.
    /// Management-plane/boot only; the publish path only loads.
    pub fn sync_transform_metrics(&self, names: &[String]) {
        let mut next = HashMap::with_capacity(names.len());
        for name in names {
            next.insert(name.clone(), Arc::new(TransformMetrics::new()));
        }
        self.transform_metrics.store(Arc::new(next));
    }

    /// Reconcile the metric map with the live names (W3-36, C08-09/10).
    /// Replace path: keeps counters for surviving names, creates zeroed
    /// records for new names, drops removed names. Management-plane only.
    pub fn reconcile_transform_metrics(&self, names: &[String]) {
        let current = self.transform_metrics.load_full();
        let mut next = HashMap::with_capacity(names.len());
        for name in names {
            if let Some(record) = current.get(name) {
                next.insert(name.clone(), Arc::clone(record));
            } else {
                next.insert(name.clone(), Arc::new(TransformMetrics::new()));
            }
        }
        self.transform_metrics.store(Arc::new(next));
    }

    /// Lock-free snapshot of the per-transformation metric map for the
    /// publish path (W3-36, C08-09/10): one `Arc` clone, no lock, no
    /// allocation beyond the refcount bump. Callers hold the snapshot
    /// across one ingress and bump atomics through it.
    pub fn transform_metrics_snapshot(&self) -> Arc<HashMap<String, Arc<TransformMetrics>>> {
        self.transform_metrics.load_full()
    }

    /// Lock-free read of one name counters for management handlers.
    /// Returns `None` when the name has no record (caller maps to 404).
    /// One `Arc` clone plus one `HashMap` lookup plus three relaxed
    /// loads; no lock, no allocation beyond the refcount bump.
    pub fn transform_metrics_snapshot_for(&self, name: &str) -> Option<(u64, u64, u64)> {
        let snapshot = self.transform_metrics.load_full();
        snapshot.get(name).map(|record| record.snapshot())
    }

    /// Zero one name counters in place (W3-36, C08-10). Returns false
    /// when the name has no record (caller maps to 404 and creates
    /// nothing). Lock-free atomic stores; the record stays registered.
    pub fn reset_transform_metrics(&self, name: &str) -> bool {
        let snapshot = self.transform_metrics.load_full();
        match snapshot.get(name) {
            Some(record) => {
                record.reset();
                true
            }
            None => false,
        }
    }

    /// Feed matching windowed ingress into the embedded rule runtime:
    /// convert the ingress `(topic, payload, qos)` once to a
    /// [`rekuiper_core::StreamRecord`] and hand it to the runtime over the
    /// in-process bounded [`StreamBus`] with async admission and
    /// backpressure ([`StreamSender::send`]: awaits subscriber capacity, so
    /// a burst waits for the feeder to drain instead of dropping; sync
    /// non-blocking [`StreamSender::try_send`] stays reserved for cyclic
    /// feedback only). Every rule task subscribes to the single shared
    /// [`runtime::RULE_INGRESS_STREAM`] (fan-out: each subscriber receives
    /// the one record), rebuilds the exact evaluation record, applies its
    /// own topic-filter/WHERE gate and pushes passing records into its
    /// bounded task feed (a full task feed drops with a warning and a
    /// counter, never blocks publish). Lazily spawns a missing task per
    /// matched rule (creation outside a runtime defers it to this point).
    /// No TCP or loopback connection between the publish path and the
    /// runtime, ever.
    async fn feed_rule_runtime(
        &self,
        tenant: &str,
        windowed: &[Arc<Rule>],
        topic: &Topic,
        payload: &Bytes,
        qos: QoS,
    ) {
        for rule in windowed {
            self.ensure_rule_task(rule);
        }
        let record = runtime::ingress_to_stream_record(tenant, topic, payload, qos);
        if let Err(e) = self.ingress_sender.send(record).await {
            self.runtime_feed_dropped.fetch_add(1, Ordering::Relaxed);
            let ids = windowed
                .iter()
                .map(|rule| rule.id.as_str())
                .collect::<Vec<_>>()
                .join(",");
            tracing::warn!(
                rule_ids = %ids,
                error = %e,
                "runtime feed admission failed; ingress record dropped"
            );
        }
    }

    /// Ensure a rule task exists for a windowed rule on the embedded
    /// runtime. Idempotent: a second call for the same id is a no-op.
    /// Requires a Tokio runtime (returns silently without one; the feed
    /// path retries on the next ingress). Spawns three handles per task
    /// with abort semantics: the bus feeder (bus subscription into the
    /// bounded task feed), the eval loop (window aggregation per close)
    /// and the sink worker (bounded sink queue into the output bridge).
    fn ensure_rule_task(&self, rule: &Rule) {
        // A disabled rule owns no task: never (re)spawn while disabled so
        // shared bus fan-out cannot deliver through it.
        if !rule.enabled {
            return;
        }
        let window = match rule
            .parsed_query
            .as_ref()
            .and_then(|stmt| stmt.window.clone())
        {
            Some(window) => window,
            None => return,
        };
        if self.rule_tasks.read().contains_key(&rule.id) {
            return;
        }
        let handle = match tokio::runtime::Handle::try_current() {
            Ok(handle) => handle,
            Err(_) => {
                tracing::warn!(
                    rule_id = %rule.id,
                    "no async runtime: rule task deferred until first ingress"
                );
                return;
            }
        };
        let stmt = match rule.parsed_query.clone() {
            Some(stmt) => stmt,
            None => return,
        };
        let bus_rx = self.stream_bus.subscribe(runtime::RULE_INGRESS_STREAM);
        let (feed_tx, feed_rx) = mpsc::channel(self.window_channel_depth.load(Ordering::Relaxed));
        let (sink_tx, sink_rx) = mpsc::channel(runtime::PER_RULE_SINK_QUEUE_DEPTH);
        let counters = Arc::new(RuleCounters::default());
        let status = Arc::new(RwLock::new("running".to_string()));
        let feeder = RuleFeederConfig {
            rule_id: rule.id.clone(),
            tenant: rule.tenant.clone(),
            topic_filter: rule.topic_filter.clone(),
            std_sources: rule.std_sources.clone(),
            std_topic_filters: rule.std_topic_filters.clone(),
            has_foreach: rule.std_foreach.is_some(),
            parsed_query: rule.parsed_query.clone(),
            std_has_wildcard: rule.std_has_wildcard,
            std_explicit_keys: rule.std_explicit_keys.clone(),
            feed_tx: feed_tx.clone(),
            bus_rx,
            counters: counters.clone(),
            failed: rule.failed_cnt.clone(),
            no_result: rule.no_result_cnt.clone(),
        };
        let eval_config = RuleTaskConfig {
            rule_id: rule.id.clone(),
            stmt,
            feed_rx,
            sink_tx,
            window,
            std_has_wildcard: rule.std_has_wildcard,
            std_explicit_keys: rule.std_explicit_keys.clone(),
            failed: rule.failed_cnt.clone(),
            no_result: rule.no_result_cnt.clone(),
        };
        let bridge = BrokerRuleSink {
            rule_id: rule.id.clone(),
            tenant: rule.tenant.clone(),
            actions: rule.actions.clone(),
            flush_ctx: self.flush_ctx.clone(),
            counters: ActionCounters::from_rule(rule),
            task_counters: counters.clone(),
            passed: rule.passed_cnt.clone(),
        };
        let task = RuleTask {
            eval: handle.spawn(rule_task_loop(eval_config)),
            feeder: handle.spawn(rule_feeder_loop(feeder)),
            sink_worker: handle.spawn(rule_sink_loop(sink_rx, bridge)),
            feed_tx,
            counters,
            status,
        };
        self.rule_tasks.write().insert(rule.id.clone(), task);
    }
}

/// One rule-task bus feeder: drain this task's [`StreamBus`] subscription,
/// rebuild the exact evaluation record from each [`StreamRecord`], apply
/// the standard-dialect source gate and the WHERE gate, and push passing
/// records into the bounded task feed. A full feed drops with a warning and
/// a counter, never blocks publish (and never blocks the bus: the feeder
/// is the only reader of its subscription).
async fn rule_feeder_loop(config: RuleFeederConfig) {
    let RuleFeederConfig {
        rule_id,
        tenant,
        topic_filter,
        std_sources,
        std_topic_filters,
        has_foreach,
        parsed_query,
        std_has_wildcard,
        std_explicit_keys,
        feed_tx,
        mut bus_rx,
        counters,
        failed,
        no_result,
    } = config;
    let stmt = match parsed_query.as_ref() {
        Some(stmt) => stmt,
        None => return,
    };
    while let Some(stream_record) = bus_rx.recv().await {
        counters.inc_source(1);
        let Some(feeder) = runtime::split_stream_record(stream_record.data) else {
            continue;
        };
        // One task serves one tenant. Records of other tenants
        // never enter the feed.
        if feeder.tenant != tenant {
            continue;
        }
        if !topic_filter.matches(&feeder.topic) {
            continue;
        }
        if !std_sources_match(&std_sources, &std_topic_filters, &feeder.topic) {
            continue;
        }
        // Rebuild the exact evaluation record for either dialect mode:
        // pre-front-end rules see the raw object keys only, standard rules
        // see the enriched message record so window aggregates see
        // `payload`/`topic`/`qos`. Standard-dialect `FOREACH` expansion
        // never fires on MQTT ingress.
        let (record, injected) = if std_sources.is_empty() && !has_foreach {
            // A non-object payload carries no top-level object to
            // aggregate, so skip (fail closed), as the inline path
            // skips it. An empty object still proceeds.
            if !feeder.has_object {
                failed.fetch_add(1, Ordering::Relaxed);
                no_result.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let mut record = feeder.record;
            for key in &feeder.injected {
                record.shift_remove(key);
            }
            (record, Vec::new())
        } else {
            if has_foreach {
                failed.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            // Standard rules accept any payload, as the inline path
            // does. The enriched record holds `payload`/`topic`/`qos`,
            // so `SELECT *` yields the payload fields when present
            // and an empty row otherwise. Never drop here for shape.
            // TODO(parity): What does the reference return for `SELECT *`
            // with a plain payload in a window: empty row or no row?
            (feeder.record, feeder.injected)
        };
        stdlib::ensure_stdlib_registered();
        // Context accessors resolve against the partition's real routing
        // context. `payload_raw` stays empty on this path: `payload()`
        // resolves against the record's `payload` key exactly like the
        // inline standard path, and no constant stands in for the raw
        // bytes. Dry-run probes are unaffected (see `try_evaluate`).
        let ctx = stdlib::RuleContext {
            topic: Some(feeder.topic.as_str().to_string()),
            qos: Some(feeder.qos),
            record: record.clone(),
            payload_raw: None,
            rule_id: rule_id.clone(),
            ..Default::default()
        };
        let _guard = stdlib::RuleContextGuard::enter(ctx);
        let passes = match stmt.where_clause.as_ref() {
            Some(condition) => Evaluator::eval_bool(condition, &record),
            None => true,
        };
        if !passes {
            failed.fetch_add(1, Ordering::Relaxed);
            no_result.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        // Carry the ingress QoS into the window buffer so flush delivery
        // stays `min(ingress QoS, action QoS)`. A malformed transport QoS
        // is unreachable (the conversion writes 0/1/2); fail closed to the
        // lowest QoS so a rule can never upgrade delivery guarantees.
        let ingress_qos = QoS::try_from(feeder.qos).unwrap_or(QoS::AtMostOnce);
        if feed_tx
            .try_send(TimedRecord {
                arrived_ms: now_ms(),
                tenant: tenant.clone(),
                topic: feeder.topic,
                qos: ingress_qos,
                record,
                injected,
                std_has_wildcard,
                std_explicit_keys: std_explicit_keys.clone(),
            })
            .is_err()
        {
            counters.inc_dropped(1);
            failed.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                rule_id = %rule_id,
                "rule-task feed full; ingress record dropped"
            );
        }
    }
}

/// One rule-task sink worker: drain the bounded per-rule sink queue into
/// the [`runtime::RuleOutputSink`] bridge, in order. Runs behind the eval
/// loop, so connector sends stay behind the rule engine's bounded queue.
async fn rule_sink_loop(mut sink_rx: mpsc::Receiver<SinkRow>, bridge: BrokerRuleSink) {
    use runtime::RuleOutputSink;
    while let Some(row) = sink_rx.recv().await {
        // One task serves one tenant. A row of another tenant never
        // leaves; it drops here, fail closed.
        if row.tenant != bridge.tenant {
            continue;
        }
        bridge.emit(row.topic, row.payload, row.qos).await;
    }
}

/// Max event-source events drained per publish ingress on the
/// memory-only path. Bounds the publish tail so a burst of queued kernel
/// events cannot stall one delivery; surplus events wait for the next
/// ingress.
const EVENT_DRAIN_LIMIT: u32 = 64;

impl RuleEngine {
    /// Execute every enabled rule whose filter matches `topic` in the
    /// default tenant. Pre-tenancy callers use this; it keeps their
    /// behaviour. Tenant callers use `dispatch_ingress_in_tenant`.
    pub async fn dispatch_ingress(
        &self,
        topic: &Topic,
        payload: &Bytes,
        qos: QoS,
        broker_sink: &Arc<dyn BrokerSink>,
    ) -> usize {
        self.dispatch_ingress_in_tenant("default", topic, payload, qos, broker_sink)
            .await
    }

    /// Execute every enabled rule of one tenant whose filter matches
    /// `topic`. Only rules of the ingress tenant run. Each republish
    /// fans out in the same tenant. A blank tenant maps to the default
    /// tenant. The match reads the per-ingress rule snapshot the route
    /// already holds. It pays one string compare per rule, no new
    /// lock, no new allocation, no new queue, no unbounded work.
    pub async fn dispatch_ingress_in_tenant(
        &self,
        tenant: &str,
        topic: &Topic,
        payload: &Bytes,
        qos: QoS,
        broker_sink: &Arc<dyn BrokerSink>,
    ) -> usize {
        self.dispatch_ingress_message(tenant, topic, payload, qos, false, broker_sink)
            .await
    }

    /// [`RuleEngine::dispatch_ingress_in_tenant`] for a caller that
    /// knows the retain flag of the input message. The flag is the
    /// value of `${retain}` in a `republish` function.
    pub async fn dispatch_ingress_message(
        &self,
        tenant: &str,
        topic: &Topic,
        payload: &Bytes,
        qos: QoS,
        retain: bool,
        broker_sink: &Arc<dyn BrokerSink>,
    ) -> usize {
        // W3-10: skip system topics while the flag holds. Test the
        // topic first so plain traffic pays no settings load.
        if is_system_topic(topic) && self.engine_config_snapshot().ignore_sys_message {
            return 0;
        }
        // Tenant scope: only rules of the ingress tenant run. Blank
        // maps to the default tenant, so old callers keep behaviour.
        // Borrowed compare only: no allocation on this path.
        let tenant = {
            let trimmed = tenant.trim();
            if trimmed.is_empty() {
                "default"
            } else {
                trimmed
            }
        };
        // Snapshot matching rules so the sink (which may touch the router,
        // never this map) runs without holding the lock.
        // B4-07 live wiring (durability push after match, see below).
        // The rule path ignores the `$` guard. The flag above gates `$SYS`.
        let matched: Vec<Arc<Rule>> = {
            let rules = self.rules.read();
            rules
                .values()
                .filter(|rule| {
                    rule.enabled && rule.tenant == tenant && rule_matches_ingress(rule, topic)
                })
                .cloned()
                .collect()
        };
        // RQ-04 gates: two relaxed loads. They guard the drain below.
        let events_selected = self.event_any_selected.load(Ordering::Relaxed);
        let spill = self.input.spill_enabled();
        // W3-29: attribute ingress only when a source exists. The flag
        // keeps the empty case to relaxed loads with no map clone.
        // Every registered source observes every ingress: enabled
        // sources count `received`, stopped sources shed into `dropped`.
        // Sources observe raw ingress, so this runs whether or not any
        // rule matches.
        if self.source_any.load(Ordering::Relaxed) {
            let source_metrics = self.source_metrics_snapshot();
            if !source_metrics.is_empty() {
                let source_disabled = self.source_disabled_snapshot();
                attribute_source_ingress(&source_metrics, &source_disabled);
            }
        }
        // Fast path: no rule matched and no drain runs. Skip the
        // action snapshots below. The window feed stays empty here.
        if matched.is_empty() && (!events_selected || spill) {
            if spill {
                let durability = StreamEvent::new(topic.clone(), payload.clone());
                // Counted (spilled/dropped) inside `try_push`; overflow
                // refuses fail-closed without blocking publish.
                let _ = self.input.try_push(durability);
            }
            return 0;
        }
        // W3-04: one lock-free `Arc` clone of the disabled-action set per
        // ingress (no store lock, no allocation beyond the refcount bump).
        // Disabled forwards are skipped at fire time below and in
        // `BrokerRuleSink::emit` for window flushes.
        let disabled = self.action_disabled_snapshot();
        // W3-05: one lock-free `Arc` clone of the per-action metric map
        // per ingress (no store lock, no allocation beyond the refcount
        // bump). Forward outcomes attribute through it below and in
        // `BrokerRuleSink::emit` for window flushes.
        let action_metrics = self.action_metrics_snapshot();
        for rule in &matched {
            rule.matched_cnt.fetch_add(1, Ordering::Relaxed);
        }
        // Tier labels stay for display: the window clause, not the
        // tier, selects the runtime path, so every window rule runs on
        // rekuiper's runtime. Windowed ingress is converted once and
        // sent once over the shared fan-out bus; each task self-filters.
        let mut windowed: Vec<Arc<Rule>> = Vec::new();
        for rule in &matched {
            // Tier labels stay for display: the window clause, not the
            // tier, selects the runtime path, so every window rule runs on
            // rekuiper's runtime.
            if rule
                .parsed_query
                .as_ref()
                .is_some_and(|stmt| stmt.window.is_some())
            {
                windowed.push(rule.clone());
                continue;
            }
            let outputs = apply_rule(rule, topic, payload, qos);
            if outputs.is_empty() {
                rule.failed_cnt.fetch_add(1, Ordering::Relaxed);
                rule.no_result_cnt.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            rule.passed_cnt.fetch_add(1, Ordering::Relaxed);
            for data in outputs {
                run_actions_in_tenant(
                    &rule.id,
                    &rule.tenant,
                    &rule.actions,
                    topic,
                    data,
                    qos,
                    &self.connectors,
                    Some(broker_sink),
                    &ActionCounters::from_rule(rule),
                    &self.forward_throttle,
                    &disabled,
                    &action_metrics,
                    retain,
                )
                .await;
            }
        }
        // Drain pending event-source events so their rule actions run
        // on the publish path. The gate uses the loads above. The
        // drain body lives in a cold helper so the hot path keeps
        // no event code inline. Drain at most EVENT_DRAIN_LIMIT
        // events per call. Surplus events wait for the next ingress.
        // Memory-only engines only; spill-backed engines drain
        // via the spill driver.
        if events_selected && !spill {
            self.drain_event_queue(broker_sink, &disabled, &action_metrics)
                .await;
        }
        if !windowed.is_empty() {
            self.feed_rule_runtime(tenant, &windowed, topic, payload, qos)
                .await;
        }
        // B4-07 live wiring: every broker ingress (see
        // `crates/broker-node/src/main.rs:ingress_pipeline_with_publisher`,
        // the single prod caller of `push`/`try_push` via this method)
        // mirrors a durability copy through the bounded input queue after
        // inline matching. Memory-only engines skip this with one
        // lock-free load, so the other four policies are unchanged.
        // Spill-backed engines append overflow to disk instead of erroring
        // (`SpillLog::spill` at `spill.rs:580`, page-cache `write + flush`,
        // never `fsync` on this path); the copy carries topic/payload for
        // order verification and crash durability, while live matching
        // above still uses the original `qos` inline, so delivery
        // guarantees never change. Pushed after the match so a crash
        // between match and push leaves an already-executed event without
        // a copy (safe to discard after restart); a crash before the match
        // leaves nothing (QoS 1 retries via MQTT, QoS 0 loss is
        // best-effort). The broker spill drain task (`broker-node`)
        // replays via `next_event` after the pressure eases; restart
        // recovery reopens the same directory (`SpillLog::open` at
        // `spill.rs:491`). Driven by the rule-ingress event; disk writes
        // consult `crates/broker-rules/src/spill.rs:580`
        // (`SpillLog::spill`) and replays consult
        // `crates/broker-rules/src/spill.rs:637` (`SpillLog::replay_one`).
        if spill {
            let durability = StreamEvent::new(topic.clone(), payload.clone());
            // Counted (spilled/dropped) inside `try_push`; overflow
            // refuses fail-closed without blocking publish.
            let _ = self.input.try_push(durability);
        }
        matched.len()
    }

    /// Drain queued event-source events and run their rule actions.
    /// `dispatch_ingress` calls this only when the gate is set.
    /// It stays out of line so the hot path stays small.
    /// Drain at most EVENT_DRAIN_LIMIT events per call.
    /// Surplus events wait for the next ingress.
    #[inline(never)]
    async fn drain_event_queue(
        &self,
        broker_sink: &Arc<dyn BrokerSink>,
        disabled: &HashSet<String>,
        action_metrics: &HashMap<String, Arc<ActionMetrics>>,
    ) {
        // Kernel events carry no tenant. Only rules of the default
        // tenant react to them, so a tenant rule never sees them.
        let tenant = "default";
        let mut drained = 0u32;
        while drained < EVENT_DRAIN_LIMIT {
            if let Some(event) = self.input.try_dequeue().await {
                let matched_evt: Vec<Arc<Rule>> = {
                    let rules = self.rules.read();
                    rules
                        .values()
                        .filter(|r| {
                            r.enabled && r.tenant == tenant && rule_matches_ingress(r, &event.topic)
                        })
                        .cloned()
                        .collect()
                };
                // Windowed event rules join the runtime bus per event.
                let mut windowed_for_event: Vec<Arc<Rule>> = Vec::new();
                for rule in &matched_evt {
                    rule.matched_cnt.fetch_add(1, Ordering::Relaxed);
                    if rule
                        .parsed_query
                        .as_ref()
                        .is_some_and(|stmt| stmt.window.is_some())
                    {
                        windowed_for_event.push(rule.clone());
                        continue;
                    }
                    let outputs = apply_rule(rule, &event.topic, &event.payload, QoS::AtMostOnce);
                    if outputs.is_empty() {
                        rule.failed_cnt.fetch_add(1, Ordering::Relaxed);
                        rule.no_result_cnt.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    rule.passed_cnt.fetch_add(1, Ordering::Relaxed);
                    for data in outputs {
                        run_actions_in_tenant(
                            &rule.id,
                            tenant,
                            &rule.actions,
                            &event.topic,
                            data,
                            QoS::AtMostOnce,
                            &self.connectors,
                            Some(broker_sink),
                            &ActionCounters::from_rule(rule),
                            &self.forward_throttle,
                            disabled,
                            action_metrics,
                            false,
                        )
                        .await;
                    }
                }
                if !windowed_for_event.is_empty() {
                    self.feed_rule_runtime(
                        tenant,
                        &windowed_for_event,
                        &event.topic,
                        &event.payload,
                        QoS::AtMostOnce,
                    )
                    .await;
                }
                drained += 1;
            } else {
                break;
            }
        }
    }
}

/// Attribute one broker ingress to every registered source (W3-29,
/// C07-18/19): enabled sources count `received`, stopped sources shed into
/// `dropped`. Runs on the publish path inside
/// [`RuleEngine::dispatch_ingress`], so it pays no lock and no per-fire
/// allocation: one `HashSet` lookup plus one relaxed `fetch_add` per
/// registered source (at most the source-store cap of records). The caller
/// holds the lock-free snapshots across the call.
fn attribute_source_ingress(
    source_metrics: &HashMap<String, Arc<SourceMetrics>>,
    source_disabled: &HashSet<String>,
) {
    for (id, record) in source_metrics.iter() {
        if source_disabled.contains(id) {
            record.dropped.fetch_add(1, Ordering::Relaxed);
        } else {
            record.received.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Execute one rule's actions for a single output row in one tenant.
/// Shared by the inline stateless path and rule-task sink workers.
/// `qos_cap` floors delivery: the inline path passes the ingress QoS
/// (`min(action, ingress)`), the flush path passes the flushed batch's
/// ingress-QoS floor (`min(action, ingress floor)`). A missing broker
/// sink drops republish actions with a warning instead of panicking.
/// Republish fans out in the rule tenant only, so output never leaves
/// it. Forward actions send the owning rule's messages only. Dispatch
/// already limits each rule to its own tenant, so a forward never
/// carries another tenant's message. The connector target itself
/// carries no tenant label.
///
/// `disabled` is the lock-free disabled-action snapshot loaded once per
/// ingress (W3-04, C06-07): a `ForwardConnector` naming a disabled action
/// id is skipped at fire time with no counters bumped and no send
/// attempted, so a disabled action costs one `HashSet` lookup and never
/// touches the connector.
///
/// `action_metrics` is the lock-free per-action metric map loaded once per
/// ingress (W3-05, C06-08/09): a `ForwardConnector` whose id names a known
/// action attributes one outcome through it (one `HashMap` lookup plus
/// relaxed `fetch_add`s, no lock, no allocation). Unknown ids
/// (plain connectors with no action record) move only the per-rule
/// counters, never the per-action map.
/// Each forward takes one registry read lock at most. The single
/// `forward_lookup` call fetches the sink, the v1 flag and the
/// action-sink check together.
// TODO(parity): Do skipped disabled forwards count in rule action totals?
// TODO(parity): Do skipped disabled forwards count in per-action totals?
#[allow(clippy::too_many_arguments)]
async fn run_actions_in_tenant(
    rule_id: &str,
    tenant: &str,
    actions: &[RuleAction],
    topic: &Topic,
    payload: Bytes,
    qos_cap: QoS,
    connectors: &Arc<ConnectorManager>,
    broker_sink: Option<&Arc<dyn BrokerSink>>,
    counters: &ActionCounters,
    throttle: &parking_lot::Mutex<HashMap<(String, String), ThrottleEntry>>,
    disabled: &HashSet<String>,
    action_metrics: &HashMap<String, Arc<ActionMetrics>>,
    input_retain: bool,
) {
    for action in actions {
        match action {
            RuleAction::RepublishFn { args } => {
                counters.total.fetch_add(1, Ordering::Relaxed);
                // The input message names `${topic}`, `${qos}` and
                // `${retain}`. A republish never starts a rule again:
                // the sink fans out to subscribers and does not call
                // `dispatch_ingress`.
                let input = functions::InputMessage {
                    topic,
                    qos: qos_cap,
                    retain: input_retain,
                };
                let outcome = match functions::render_republish(args, &payload, input) {
                    Ok(message) => match broker_sink {
                        Some(sink) => sink
                            .publish_message_in_tenant(tenant, message)
                            .await
                            .map_err(|e| e.to_string()),
                        None => Err("no broker sink configured".to_string()),
                    },
                    Err(e) => Err(e),
                };
                match outcome {
                    Ok(()) => {
                        counters.success.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        counters.failed.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            rule_id = %rule_id,
                            error = %e,
                            "Rule republish function failed; continuing with remaining actions"
                        );
                    }
                }
            }
            RuleAction::Console => {
                counters.total.fetch_add(1, Ordering::Relaxed);
                counters.success.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    rule_id = %rule_id,
                    topic = %topic,
                    result = %functions::console_text(&payload),
                    "rule console"
                );
            }
            RuleAction::Republish {
                topic: dst,
                qos: action_qos,
            } => {
                let effective = std::cmp::min(*action_qos, qos_cap);
                match broker_sink {
                    Some(sink) => {
                        if let Err(e) = sink
                            .publish_in_tenant(
                                tenant,
                                dst.clone(),
                                payload.clone(),
                                effective,
                                false,
                            )
                            .await
                        {
                            counters.total.fetch_add(1, Ordering::Relaxed);
                            counters.failed.fetch_add(1, Ordering::Relaxed);
                            tracing::warn!(
                                rule_id = %rule_id,
                                error = %e,
                                "Rule republish failed; continuing with remaining actions"
                            );
                        } else {
                            counters.total.fetch_add(1, Ordering::Relaxed);
                            counters.success.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    None => {
                        counters.total.fetch_add(1, Ordering::Relaxed);
                        counters.failed.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            rule_id = %rule_id,
                            "no broker sink configured; republish dropped"
                        );
                    }
                }
            }
            RuleAction::Log => {
                counters.total.fetch_add(1, Ordering::Relaxed);
                counters.success.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    rule_id = %rule_id,
                    topic = %topic,
                    "Rule matched ingress event"
                );
            }
            RuleAction::ForwardConnector { connector_id } => {
                // One dispatch key owns one metric record. A v5 rule
                // fires under `action:{id}` and counts there. A bare id
                // that names a v1 connector must never credit a v5
                // action. A bare id for a kind with no owned runtime
                // (legacy state, pending kinds, console) still falls
                // back to its `action:{id}` record so its failure
                // counts. A bare id whose v5 action owns a runtime
                // never falls back: the runtime counts itself, and a
                // v1 forward with the same name stays out, including
                // after a restart when the v1 id is gone but the v5
                // runtime was rebuilt.
                // One lock read serves this forward. `forward_lookup`
                // reads the registry once for the sink, the v1 flag
                // and the action-sink check together.
                let lookup = connectors.forward_lookup(connector_id);
                let per_action = action_metrics.get(connector_id).or_else(|| {
                    if broker_config::is_v5_action_sink_key(connector_id) {
                        return None;
                    }
                    if lookup.is_v1 {
                        return None;
                    }
                    if lookup.has_action_sink {
                        return None;
                    }
                    let action_key = broker_config::v5_action_sink_key(connector_id);
                    action_metrics.get(&action_key)
                });
                // W3-04: a disabled action is skipped at fire time. RO-01
                // records the shed message against the action's dropped
                // counters, so an operator sees why nothing was sent.
                if disabled.contains(connector_id) {
                    if let Some(record) = per_action {
                        record.matched.fetch_add(1, Ordering::Relaxed);
                        record.dropped.fetch_add(1, Ordering::Relaxed);
                        record
                            .dropped_resource_stopped
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    continue;
                }
                // RO-01: an action-owned runtime counts its own matched,
                // success, failed and drop outcomes. The rule counters stay
                // here so rule metrics keep their meaning. A plain
                // connector forward is counted here as before.
                // The sink comes from the single lookup above, so
                // delivery takes no second lock.
                let (outcome, owns_metrics) = match lookup.sink {
                    Some(ref sink) => {
                        let owns = sink.owns_action_metrics();
                        (sink.send(topic, &payload, qos_cap).await, owns)
                    }
                    None => (
                        Err(broker_connectors::ConnectorError::UnknownConnector(
                            connector_id.to_string(),
                        )),
                        false,
                    ),
                };
                if let Err(e) = outcome {
                    counters.total.fetch_add(1, Ordering::Relaxed);
                    counters.failed.fetch_add(1, Ordering::Relaxed);
                    if let Some(record) = per_action {
                        if !owns_metrics {
                            record.total.fetch_add(1, Ordering::Relaxed);
                            record.matched.fetch_add(1, Ordering::Relaxed);
                            record.failed.fetch_add(1, Ordering::Relaxed);
                        }
                        if matches!(&e, broker_connectors::ConnectorError::UnknownConnector(_)) {
                            record.dropped.fetch_add(1, Ordering::Relaxed);
                            record
                                .dropped_resource_not_found
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    if let Some(suppressed) =
                        forward_failure_should_log(throttle, rule_id, connector_id, now_ms())
                    {
                        tracing::warn!(
                            rule_id = %rule_id,
                            connector = %connector_id,
                            error = %e,
                            suppressed = suppressed,
                            "Rule connector forward failed; continuing with remaining actions"
                        );
                    }
                } else {
                    counters.total.fetch_add(1, Ordering::Relaxed);
                    counters.success.fetch_add(1, Ordering::Relaxed);
                    if let Some(record) = per_action {
                        if !owns_metrics {
                            record.total.fetch_add(1, Ordering::Relaxed);
                            record.matched.fetch_add(1, Ordering::Relaxed);
                            record.success.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        }
    }
}

/// Rule-task eval loop for one windowed rule on the embedded runtime.
/// Time windows tick; count windows flush on arrivals; sliding windows
/// aggregate on every arrival. An empty window never dispatches. Closed
/// windows go to the bounded per-rule sink queue (`sink_tx`); a full sink
/// queue applies backpressure here (await), which fills the task feed,
/// which drops with a counter — the publish path itself never blocks.
async fn rule_task_loop(config: RuleTaskConfig) {
    let RuleTaskConfig {
        rule_id,
        stmt,
        mut feed_rx,
        sink_tx,
        window,
        std_has_wildcard,
        std_explicit_keys,
        failed,
        no_result,
    } = config;
    match window {
        WindowDef::TumblingTime { unit, length } => {
            let period = std::time::Duration::from_millis(window_length_ms(&unit, length));
            let mut ticker = tokio::time::interval(period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut window_start = now_ms();
            let mut buffer: Vec<TimedRecord> = Vec::new();
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let end = now_ms();
                        let batch = std::mem::take(&mut buffer);
                        flush_rule_window(&rule_id, &stmt, batch, window_start, end, &sink_tx, std_has_wildcard, &std_explicit_keys, &failed, &no_result).await;
                        window_start = end;
                    }
                    rec = feed_rx.recv() => {
                        match rec {
                            Some(record) => buffer.push(record),
                            None => break,
                        }
                    }
                }
            }
        }
        WindowDef::Count { size, interval } => {
            let size = size.max(1);
            let mut buffer: Vec<TimedRecord> = Vec::new();
            let mut since_flush = 0usize;
            while let Some(record) = feed_rx.recv().await {
                buffer.push(record);
                since_flush += 1;
                let step = interval.unwrap_or(size).max(1);
                if buffer.len() >= size && since_flush >= step {
                    let end = now_ms();
                    let batch = if interval.is_some() {
                        // Sliding count: aggregate the trailing window,
                        // retain it for the next step.
                        let start = buffer.len().saturating_sub(size);
                        buffer[start..].to_vec()
                    } else {
                        std::mem::take(&mut buffer)
                    };
                    let start = batch.first().map(|r| r.arrived_ms).unwrap_or(end);
                    flush_rule_window(
                        &rule_id,
                        &stmt,
                        batch,
                        start,
                        end,
                        &sink_tx,
                        std_has_wildcard,
                        &std_explicit_keys,
                        &failed,
                        &no_result,
                    )
                    .await;
                    since_flush = 0;
                }
                // Sliding retention cap: never hold more than one window.
                if interval.is_some() {
                    let excess = buffer.len().saturating_sub(size);
                    buffer.drain(..excess);
                }
            }
        }
        WindowDef::HoppingTime {
            unit,
            length,
            interval,
        } => {
            let period = std::time::Duration::from_millis(window_length_ms(&unit, interval));
            let span = window_length_ms(&unit, length);
            let mut ticker = tokio::time::interval(period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut buffer: VecDeque<TimedRecord> = VecDeque::new();
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let end = now_ms();
                        let cutoff = end.saturating_sub(span);
                        while buffer.front().map(|r| r.arrived_ms < cutoff).unwrap_or(false) {
                            buffer.pop_front();
                        }
                        let batch: Vec<TimedRecord> = buffer.iter().cloned().collect();
                        flush_rule_window(
                            &rule_id,
                            &stmt,
                            batch,
                            cutoff,
                            end,
                            &sink_tx,
                            std_has_wildcard,
                            &std_explicit_keys,
                            &failed,
                            &no_result,
                        )
                        .await;
                    }
                    rec = feed_rx.recv() => {
                        match rec {
                            Some(record) => buffer.push_back(record),
                            None => break,
                        }
                    }
                }
            }
        }
        WindowDef::SlidingTime { unit, length, .. } => {
            let span = window_length_ms(&unit, length);
            let mut buffer: VecDeque<TimedRecord> = VecDeque::new();
            while let Some(record) = feed_rx.recv().await {
                let end = now_ms();
                let cutoff = end.saturating_sub(span);
                buffer.push_back(record);
                while buffer
                    .front()
                    .map(|r| r.arrived_ms < cutoff)
                    .unwrap_or(false)
                {
                    buffer.pop_front();
                }
                // Aggregate the trailing window on every arrival.
                let batch: Vec<TimedRecord> = buffer.iter().cloned().collect();
                flush_rule_window(
                    &rule_id,
                    &stmt,
                    batch,
                    cutoff,
                    end,
                    &sink_tx,
                    std_has_wildcard,
                    &std_explicit_keys,
                    &failed,
                    &no_result,
                )
                .await;
            }
        }
        WindowDef::Session {
            unit,
            max_duration,
            timeout,
        } => {
            let timeout_ms = window_length_ms(&unit, timeout);
            let max_duration_ms = window_length_ms(&unit, max_duration);
            let mut buffer: Vec<TimedRecord> = Vec::new();
            let mut session_start: Option<u64> = None;
            let mut last_arrival: Option<u64> = None;
            loop {
                let sleep_duration = match last_arrival {
                    Some(last) => {
                        let elapsed = now_ms().saturating_sub(last);
                        std::time::Duration::from_millis(timeout_ms.saturating_sub(elapsed).max(1))
                    }
                    None => std::time::Duration::from_secs(3600),
                };
                tokio::select! {
                    _ = tokio::time::sleep(sleep_duration), if last_arrival.is_some() => {
                        let end = now_ms();
                        let start = session_start.unwrap_or(end);
                        let batch = std::mem::take(&mut buffer);
                        flush_rule_window(&rule_id, &stmt, batch, start, end, &sink_tx, std_has_wildcard, &std_explicit_keys, &failed, &no_result).await;
                        session_start = None;
                        last_arrival = None;
                    }
                    rec = feed_rx.recv() => {
                        match rec {
                            Some(record) => {
                                let now = record.arrived_ms;
                                let start = match session_start {
                                    Some(s) => s,
                                    None => {
                                        session_start = Some(now);
                                        now
                                    }
                                };
                                buffer.push(record);
                                last_arrival = Some(now);
                                if now.saturating_sub(start) >= max_duration_ms {
                                    let batch = std::mem::take(&mut buffer);
                                    flush_rule_window(&rule_id, &stmt, batch, start, now, &sink_tx, std_has_wildcard, &std_explicit_keys, &failed, &no_result).await;
                                    session_start = None;
                                    last_arrival = None;
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        }
        WindowDef::State { .. } => {}
    }
}

/// Flush one closed window: inject `__window_start__`,
/// `__window_end__`, `window_start`, `window_end` epoch millis into every
/// record (so `window_start()`/`window_end()` resolve), partition by
/// `GROUP BY` when present, evaluate aggregates per partition, and enqueue
/// each resulting row into the bounded per-rule sink queue (`sink_tx`) for
/// the sink worker, which delivers through the [`runtime::RuleOutputSink`]
/// bridge. Delivery QoS per row is `min(ingress QoS, action QoS)`: the
/// batch's ingress-QoS floor travels in the sink row so a rule can never
/// upgrade delivery guarantees. A full sink queue applies backpressure
/// here (await); a closed queue means the rule is gone, so the flush
/// returns. Empty windows and fully-having-filtered windows enqueue
/// nothing. Each row routes with its partition's first record topic.
#[allow(clippy::too_many_arguments)]
async fn flush_rule_window(
    rule_id: &str,
    stmt: &SelectStmt,
    batch: Vec<TimedRecord>,
    window_start_ms: u64,
    window_end_ms: u64,
    sink_tx: &mpsc::Sender<SinkRow>,
    std_has_wildcard: bool,
    std_explicit_keys: &[String],
    failed: &Arc<AtomicU64>,
    no_result: &Arc<AtomicU64>,
) -> usize {
    if batch.is_empty() {
        return 0;
    }
    // One task serves one tenant: the feeder drops other tenants, so
    // the batch carries one tenant. Flush output fans out in it.
    let batch_tenant = batch
        .first()
        .map(|timed| timed.tenant.clone())
        .unwrap_or_else(|| "default".to_string());
    // Ingress-QoS floor over the flushed batch: the most conservative
    // (lowest) ingress QoS, so `min(ingress, action)` never upgrades.
    let qos_floor = batch
        .iter()
        .map(|timed| timed.qos)
        .min()
        .unwrap_or(QoS::AtMostOnce);
    let records: Vec<EnrichedWindowRecord> = batch
        .into_iter()
        .map(|timed| {
            let mut map = timed.record;
            let mut injected = timed.injected;
            for key in ["window_start", "window_end"] {
                if !map.contains_key(key) {
                    injected.push(key.to_string());
                }
            }
            map.insert(
                "__window_start__".to_string(),
                serde_json::Value::from(window_start_ms),
            );
            map.insert(
                "__window_end__".to_string(),
                serde_json::Value::from(window_end_ms),
            );
            map.insert(
                "window_start".to_string(),
                serde_json::Value::from(window_start_ms),
            );
            map.insert(
                "window_end".to_string(),
                serde_json::Value::from(window_end_ms),
            );
            (
                timed.topic,
                map,
                injected,
                timed.std_has_wildcard,
                timed.std_explicit_keys,
            )
        })
        .collect();
    // Window aggregates run over the enriched records; the per-record
    // wildcard state travels along so `SELECT *` strips exactly the injected
    // message keys afterwards (standard front-end projection state).
    let agg_input = records
        .into_iter()
        .map(|(topic, map, injected, _, _)| (topic, map, injected))
        .collect();
    let mut rows_out = 0usize;
    for (topic, mut row, injected) in
        aggregate_partitioned_with_metadata(stmt, agg_input, rule_id, false)
    {
        strip_injected_keys(&mut row, std_has_wildcard, std_explicit_keys, &injected);
        let object = row;
        let bytes = match serde_json::to_vec(&serde_json::Value::Object(object)) {
            Ok(bytes) => Bytes::from(bytes),
            Err(e) => {
                tracing::warn!(
                    rule_id = %rule_id,
                    error = %e,
                    "window row not serializable; dropped"
                );
                continue;
            }
        };
        if sink_tx
            .send(SinkRow {
                tenant: batch_tenant.clone(),
                topic,
                qos: qos_floor,
                payload: bytes,
            })
            .await
            .is_err()
        {
            return rows_out;
        }
        rows_out += 1;
    }
    // A non-empty batch that yields no row means the window filtered
    // everything out. Count it as failed, as the inline path counts
    // an ingress with no output.
    if rows_out == 0 {
        failed.fetch_add(1, Ordering::Relaxed);
        no_result.fetch_add(1, Ordering::Relaxed);
    }
    rows_out
}

/// Partition records by `GROUP BY` expressions and evaluate one
/// aggregate row per partition (first-seen group order). Without
/// `GROUP BY`, the whole batch is a single partition. Used by window
/// flushes and the batch dry-run tester alike so both agree.
///
/// Each partition's aggregate evaluation runs with that partition's
/// context installed (`RuleContextGuard`): `topic()`, `payload()`,
/// `qos()` and the other zero-argument accessors read the partition's
/// first record instead of stale/empty state from another thread. Without
/// the guard the window path would evaluate with whatever context the
/// flushing thread last saw.
pub fn aggregate_partitioned(
    stmt: &SelectStmt,
    records: Vec<(Topic, serde_json::Map<String, serde_json::Value>)>,
    rule_id: &str,
    dry_run: bool,
) -> Vec<(Topic, serde_json::Map<String, serde_json::Value>)> {
    aggregate_partitioned_with_metadata(
        stmt,
        records
            .into_iter()
            .map(|(topic, row)| (topic, row, ()))
            .collect(),
        rule_id,
        dry_run,
    )
    .into_iter()
    .map(|(topic, row, ())| (topic, row))
    .collect()
}

type AggregateRecord<M> = (Topic, serde_json::Map<String, serde_json::Value>, M);
type AggregateGroup<M> = (Topic, Vec<serde_json::Map<String, serde_json::Value>>, M);

// Keep each partition's first-record metadata with its result. Wildcard
// cleanup must retain payload keys that shadow metadata in that partition.
fn aggregate_partitioned_with_metadata<M: Clone>(
    stmt: &SelectStmt,
    records: Vec<AggregateRecord<M>>,
    rule_id: &str,
    dry_run: bool,
) -> Vec<AggregateRecord<M>> {
    if records.is_empty() {
        return Vec::new();
    }
    stdlib::ensure_stdlib_registered();
    // Build the evaluation context for one partition: the partition's
    // topic plus its first record, so context accessors resolve. Dry-run
    // partitions inherit the probe flag so state-mutating UDFs fail closed
    // instead of touching production state.
    let partition_ctx =
        |topic: &Topic, first: &serde_json::Map<String, serde_json::Value>| stdlib::RuleContext {
            topic: Some(topic.as_str().to_string()),
            record: first.clone(),
            payload_raw: serde_json::to_string(first).ok(),
            rule_id: rule_id.to_string(),
            dry_run,
            ..Default::default()
        };
    if stmt.group_by.is_empty() {
        let topic = records[0].0.clone();
        let metadata = records[0].2.clone();
        let maps: Vec<serde_json::Map<String, serde_json::Value>> =
            records.into_iter().map(|(_, map, _)| map).collect();
        let ctx = partition_ctx(&topic, &maps[0]);
        let _guard = stdlib::RuleContextGuard::enter(ctx);
        return Evaluator::eval_aggregate(stmt, &maps)
            .map(|row| vec![(topic, row.into_iter().collect(), metadata)])
            .unwrap_or_default();
    }
    let mut order: Vec<String> = Vec::new();
    let mut groups: HashMap<String, AggregateGroup<M>> = HashMap::new();
    for (topic, map, metadata) in records {
        let key = group_key(&stmt.group_by, &map);
        let entry = groups.entry(key.clone()).or_insert_with(|| {
            order.push(key);
            (topic, Vec::new(), metadata)
        });
        entry.1.push(map);
    }
    let mut out = Vec::new();
    for key in order {
        // INVARIANT: key in order was inserted into groups map during partitioning immediately before removal.
        let (topic, maps, metadata) = groups.remove(&key).expect("group just inserted");
        let ctx = partition_ctx(&topic, &maps[0]);
        let _guard = stdlib::RuleContextGuard::enter(ctx);
        if let Some(row) = Evaluator::eval_aggregate(stmt, &maps) {
            out.push((topic, row.into_iter().collect(), metadata));
        }
    }
    out
}

/// Canonical group key: type-tagged so `1`, `"1"` and `true` never
/// collide across JSON types.
fn group_key(
    exprs: &[rekuiper_sql::Expr],
    record: &serde_json::Map<String, serde_json::Value>,
) -> String {
    exprs
        .iter()
        .map(|expr| match Evaluator::eval_val(expr, record) {
            serde_json::Value::Null => "null".to_string(),
            serde_json::Value::Bool(value) => format!("b:{value}"),
            serde_json::Value::Number(value) => format!("n:{value}"),
            serde_json::Value::String(value) => format!("s:{value}"),
            other => format!("j:{other}"),
        })
        .collect::<Vec<_>>()
        .join("\x1f")
}

/// Give plain `GROUP BY` (no window clause) a session window so it
/// aggregates per group instead of projecting each message alone.
///
/// A plain `GROUP BY k` with no window arrives here with `window: None`
/// and runs per message, so `sum(v)` returns the single row value. The
/// session groups each burst (gap `IMPLICIT_GROUP_SESSION_TIMEOUT_MS`,
/// cap `IMPLICIT_GROUP_SESSION_MAX_MS`) into one batch; the flush
/// partitions by key and evaluates aggregates per group. Skips `FOREACH`
/// rules: their expansion path never enters the window feed.
fn apply_implicit_group_window(stmt: &mut SelectStmt) {
    if stmt.group_by.is_empty() || stmt.window.is_some() {
        return;
    }
    stmt.window = Some(WindowDef::Session {
        unit: TimeUnit::Ms,
        max_duration: IMPLICIT_GROUP_SESSION_MAX_MS,
        timeout: IMPLICIT_GROUP_SESSION_TIMEOUT_MS,
    });
}

/// Function names the broker never serves (the deferred schema-registry
/// family, out of the MVP). A static call fails rule creation with
/// `InvalidRule` naming the function; the API answers 400.
const REFUSED_SQL_FUNCTIONS: &[&str] = &["schema_encode", "schema_decode"];

/// True when `expr` is a string literal; returns its text.
fn literal_text(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(serde_json::Value::String(text)) => Some(text.as_str()),
        _ => None,
    }
}

/// Reject one call that the broker statically knows it cannot serve:
/// a deferred function name, a D4 hash algorithm literal or a D4 named-zone
/// literal. Dynamic (non-literal) arguments skip this check and fail closed
/// at execution instead, where the run counts as failed.
fn check_one_call(name: &str, args: &[Expr]) -> Result<(), String> {
    let lower = name.to_ascii_lowercase();
    if REFUSED_SQL_FUNCTIONS.contains(&lower.as_str()) {
        return Err(format!("unsupported function {name:?} (field `sql_query`)"));
    }
    if lower == "hash" {
        if let Some(algo) = args.first().and_then(literal_text) {
            if stdlib::refused_hash_algorithm(algo) {
                return Err(format!(
                    "unsupported hash algorithm {algo:?} (field `sql_query`)"
                ));
            }
        }
    }
    if lower == "timezone_to_second" || lower == "timezone_to_offset_seconds" {
        if let Some(zone) = args.first().and_then(literal_text) {
            if stdlib::is_named_zone(zone) {
                return Err(format!(
                    "unsupported time zone {zone:?} (field `sql_query`)"
                ));
            }
        }
    }
    if lower == "date_to_unix_ts" && args.len() == 4 {
        if let Some(zone) = args.get(1).and_then(literal_text) {
            if stdlib::is_named_zone(zone) {
                return Err(format!(
                    "unsupported time zone {zone:?} (field `sql_query`)"
                ));
            }
        }
    }
    Ok(())
}

/// Walk one expression and refuse every statically unsupported call inside
/// it. Window definitions carry no calls and need no walk.
fn check_expr_calls(expr: &Expr) -> Result<(), String> {
    match expr {
        Expr::Call { name, args } => {
            check_one_call(name, args)?;
            for arg in args {
                check_expr_calls(arg)?;
            }
            Ok(())
        }
        Expr::BinaryOp { left, right, .. } => {
            check_expr_calls(left)?;
            check_expr_calls(right)
        }
        Expr::UnaryOp { expr, .. } => check_expr_calls(expr),
        Expr::Between {
            expr, low, high, ..
        } => {
            check_expr_calls(expr)?;
            check_expr_calls(low)?;
            check_expr_calls(high)
        }
        Expr::InList { expr, list, .. } => {
            check_expr_calls(expr)?;
            for item in list {
                check_expr_calls(item)?;
            }
            Ok(())
        }
        Expr::IsNull { expr, .. } => check_expr_calls(expr),
        Expr::FieldAccess { parent, .. } => check_expr_calls(parent),
        Expr::Index { base, index } => {
            check_expr_calls(base)?;
            check_expr_calls(index)
        }
        Expr::Slice { base, lo, hi } => {
            check_expr_calls(base)?;
            if let Some(lo) = lo {
                check_expr_calls(lo)?;
            }
            if let Some(hi) = hi {
                check_expr_calls(hi)?;
            }
            Ok(())
        }
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(operand) = operand {
                check_expr_calls(operand)?;
            }
            for (cond, result) in when_clauses {
                check_expr_calls(cond)?;
                check_expr_calls(result)?;
            }
            if let Some(other) = else_clause {
                check_expr_calls(other)?;
            }
            Ok(())
        }
        Expr::Over {
            call, partition_by, ..
        } => {
            check_expr_calls(call)?;
            if let Some(partition_by) = partition_by {
                check_expr_calls(partition_by)?;
            }
            Ok(())
        }
        Expr::Wildcard | Expr::WildcardModified { .. } | Expr::Identifier(_) | Expr::Literal(_) => {
            Ok(())
        }
    }
}

/// Refuse every statically unsupported call in one parsed rule. The walk
/// covers the SELECT list, WHERE, GROUP BY, HAVING, ORDER BY, JOIN ON,
/// window filter, trigger, partition, UNION branches and FOREACH plan. A
/// refusal names the function, algorithm or zone so the API can answer 400
/// with it. Dynamic values are not literals and stay for execution, where
/// they fail closed and count as a failed run.
fn refuse_unsupported_sql_calls(
    stmt: &SelectStmt,
    foreach: Option<&ForeachPlan>,
) -> Result<(), String> {
    for field in &stmt.fields {
        check_expr_calls(field)?;
    }
    if let Some(where_clause) = stmt.where_clause.as_ref() {
        check_expr_calls(where_clause)?;
    }
    for item in &stmt.group_by {
        check_expr_calls(item)?;
    }
    if let Some(having) = stmt.having.as_ref() {
        check_expr_calls(having)?;
    }
    for item in &stmt.order_by {
        check_expr_calls(&item.expr)?;
    }
    for join in &stmt.joins {
        if let Some(on) = join.on.as_ref() {
            check_expr_calls(on)?;
        }
    }
    if let Some(filter) = stmt.window_filter.as_ref() {
        check_expr_calls(filter)?;
    }
    if let Some(trigger) = stmt.window_trigger_condition.as_ref() {
        check_expr_calls(trigger)?;
    }
    if let Some(partition) = stmt.window_partition_by.as_ref() {
        check_expr_calls(partition)?;
    }
    if let Some((_, next)) = stmt.set_op.as_ref() {
        refuse_unsupported_sql_calls(next, None)?;
    }
    if let Some(plan) = foreach {
        check_expr_calls(&plan.array)?;
        if let Some(fields) = plan.fields.as_ref() {
            for (expr, _) in fields {
                check_expr_calls(expr)?;
            }
        }
        if let Some(incase) = plan.incase.as_ref() {
            check_expr_calls(incase)?;
        }
    }
    Ok(())
}

/// Parse one rule SQL string through the standard front end first, with a
/// fallback to the streaming parser for rekuiper-only syntax beyond the
/// standard window extension.
///
/// The standard dialect (`std_sql::parse_std_rule`) is the documented rule
/// dialect: quoted MQTT topic filters in `FROM`, `payload.*` field access,
/// `=~`, `FOREACH`, plus window clauses as the streaming extension. Older
/// streaming rules (bare `a/#` targets, `SELECT` without `FROM`,
/// `UNION`/`JOIN`/`ORDER BY`/`LIMIT`) predate the front end and keep working
/// via the fallback, which normalizes the `FROM` target to the inert
/// `stream` placeholder. A rule is stored with the SQL the user sent; it is
/// never rewritten in the API output.
///
/// On failure the standard error is returned (it carries line and column
/// for the offending token, per the spec); the fallback is only tried to
/// preserve existing rules, never to mask a standard-dialect typo.
fn parse_rule_sql(sql: &str) -> Result<ParsedStdRule, RuleEngineError> {
    let rewritten = rewrite_colliding_functions(sql);
    match std_sql::parse_std_rule(&rewritten) {
        Ok(std_rule) => {
            let mut stmt = std_rule.stmt.clone();
            if std_rule.foreach.is_none() {
                apply_implicit_group_window(&mut stmt);
            }
            refuse_unsupported_sql_calls(&stmt, std_rule.foreach.as_ref())
                .map_err(RuleEngineError::InvalidRule)?;
            Ok((
                stmt,
                std_rule.foreach.clone(),
                std_rule.sources.clone(),
                std_rule.has_wildcard,
                std_rule.explicit_keys.clone(),
            ))
        }
        Err(std_err) => {
            let normalized = normalize_from_target(&rewritten);
            match rekuiper_sql::Parser::new(&normalized).parse_select() {
                Ok(mut stmt) => {
                    apply_implicit_group_window(&mut stmt);
                    refuse_unsupported_sql_calls(&stmt, None)
                        .map_err(RuleEngineError::InvalidRule)?;
                    let (has_wildcard, explicit_keys) = projection_metadata(&stmt);
                    Ok((stmt, None, Vec::new(), has_wildcard, explicit_keys))
                }
                Err(_) => Err(RuleEngineError::InvalidRule(format!(
                    "invalid sql_query: {std_err}"
                ))),
            }
        }
    }
}

/// Parse the `FROM` sources of a rule SQL string for the management API.
///
/// The standard rule dialect is the first parser. Legacy statements that
/// the streaming parser accepts keep their existing FROM target. A
/// statement neither parser accepts, or one with no FROM target, returns
/// an error that names the problem. The management handler turns this
/// error into the documented 400 answer; it never invents a topic.
pub fn rule_sql_sources(sql: &str) -> Result<Vec<StdSource>, String> {
    let (stripped, _) = split_into_connector(sql).map_err(|e| e.to_string())?;
    let rewritten = rewrite_colliding_functions(&stripped);
    match std_sql::parse_std_rule(&rewritten) {
        Ok(rule) => Ok(rule.sources),
        Err(std_err) => {
            let normalized = normalize_from_target(&rewritten);
            match rekuiper_sql::Parser::new(&normalized).parse_select() {
                Ok(_) => match from_target_text(&stripped) {
                    Some(target) if !target.trim().is_empty() => {
                        if target.starts_with("$events/") {
                            Ok(vec![StdSource::Event(target)])
                        } else {
                            Ok(vec![StdSource::Topic(target)])
                        }
                    }
                    _ => Err(format!("invalid sql_query: {std_err}")),
                },
                Err(_) => Err(format!("invalid sql_query: {std_err}")),
            }
        }
    }
}

// Cache output names only for metadata cleanup. The evaluator owns field
// order and values, including expressions and repeated names.
fn projection_metadata(stmt: &SelectStmt) -> (bool, Vec<String>) {
    let mut has_wildcard = false;
    let mut explicit_keys = Vec::new();
    let mut branch = Some(stmt);
    while let Some(stmt) = branch {
        for (index, field) in stmt.fields.iter().enumerate() {
            if matches!(
                field,
                rekuiper_sql::Expr::Wildcard | rekuiper_sql::Expr::WildcardModified { .. }
            ) {
                has_wildcard = true;
            } else {
                explicit_keys.push(Evaluator::select_field_name_at(stmt, index));
            }
        }
        branch = stmt.set_op.as_ref().map(|(_, next)| next.as_ref());
    }
    (has_wildcard, explicit_keys)
}

/// Compile the `Topic` entries of `std_sources` once, at rule creation.
///
/// The result has one entry per source in source order. A non-topic
/// source, or a filter the parser rejects, holds `None`. The ingress path
/// then matches these filters without a per-publish parse.
fn compile_std_topic_filters(sources: &[StdSource]) -> Vec<Option<TopicFilter>> {
    sources
        .iter()
        .map(|source| match source {
            StdSource::Topic(filter) => TopicFilter::new(filter).ok(),
            StdSource::Event(_) | StdSource::Stream(_) => None,
        })
        .collect()
}

/// True when an ingress topic may execute this rule.
///
/// The rule must match both its stored `topic_filter` and its sources.
/// A rule with several sources stores `#` as the filter and matches each
/// source through [`std_sources_match`], so every source routes and the
/// counters see only real matches. A single event source matches only its
/// own event topic, so MQTT ingress does not run it. The source check
/// uses the filters compiled at creation, so it adds no parse.
fn rule_matches_ingress(rule: &Rule, topic: &Topic) -> bool {
    rule.topic_filter.matches_for_rule_engine(topic)
        && std_sources_match(&rule.std_sources, &rule.std_topic_filters, topic)
}

/// True when the ingress topic may execute this standard rule's statement.
///
/// `Topic` sources must match their precompiled filter; `Event` sources
/// (`"$events/..."`, built in RQ-04) match their own event topic so an
/// event rule runs its actions once on the drain path. Only the kernel
/// `fire_*` entry points enqueue them, so a rule over only events never
/// fires on MQTT ingress (fail closed). `Stream` sources (bare names, the
/// streaming extension) constrain nothing. An empty source list is a
/// pre-front-end rule and constrains nothing.
fn std_sources_match(
    sources: &[StdSource],
    filters: &[Option<TopicFilter>],
    topic: &Topic,
) -> bool {
    if sources.is_empty() {
        return true;
    }
    let mut has_topic = false;
    let mut has_stream = false;
    for (source, filter) in sources.iter().zip(filters) {
        match source {
            StdSource::Topic(_) => {
                has_topic = true;
                if filter.as_ref().is_some_and(|parsed| parsed.matches(topic)) {
                    return true;
                }
            }
            StdSource::Stream(_) => {
                has_stream = true;
            }
            StdSource::Event(name) => {
                // An event source names a `$events/...` topic that only the
                // kernel enqueues via fire_*; matching it here lets the
                // dispatch_ingress drain loop run an event rule's actions
                // exactly once on the publish path. Real MQTT ingress topics
                // never equal an event source name, so this stays
                // fail-closed there.
                if name.as_str() == topic.as_str() {
                    return true;
                }
            }
        }
    }
    if has_topic {
        return false;
    }
    if has_stream {
        return true;
    }
    false
}

/// Build the evaluation record for one standard-dialect ingress.
///
/// The raw JSON object keys stay at top level (so pre-front-end field
/// references keep working), and message fields are injected only when
/// absent: `payload` is the JSON-decoded payload (treated as a string when
/// it is not JSON), `topic` is the ingress topic, `qos` is the ingress QoS
/// number (0/1/2). Fields the broker cannot supply (`clientid`, `username`,
/// `flags`, `retain`, `id`, `timestamp`, `node`, `peerhost`,
/// `publish_received_at`, `pub_props`, `headers`) are omitted, never a
/// constant standing in for a measurement. Returns the record, the keys
/// this call injected (for the wildcard strip), and whether the payload
/// parsed as a JSON object.
fn build_std_record(
    topic: &Topic,
    payload: &Bytes,
    qos: QoS,
) -> (
    serde_json::Map<String, serde_json::Value>,
    Vec<String>,
    bool,
) {
    let (mut record, payload_is_object): (serde_json::Map<String, serde_json::Value>, bool) =
        match serde_json::from_slice(payload) {
            Ok(serde_json::Value::Object(map)) => (map, true),
            _ => (serde_json::Map::new(), false),
        };
    let mut injected = Vec::new();
    // `payload` on first use: decoded JSON, else the raw bytes as a string.
    // Bound: the decoded value borrows nothing; no accumulation.
    if !record.contains_key("payload") {
        let value: serde_json::Value = serde_json::from_slice(payload).unwrap_or_else(|_| {
            serde_json::Value::String(String::from_utf8_lossy(payload).into_owned())
        });
        record.insert("payload".to_string(), value);
        injected.push("payload".to_string());
    }
    if !record.contains_key("topic") {
        record.insert(
            "topic".to_string(),
            serde_json::Value::String(topic.as_str().to_string()),
        );
        injected.push("topic".to_string());
    }
    if !record.contains_key("qos") {
        record.insert("qos".to_string(), serde_json::Value::from(u8::from(qos)));
        injected.push("qos".to_string());
    }
    (record, injected, payload_is_object)
}

/// Strip injected message keys from a `SELECT *` expansion so it keeps
/// projecting exactly the payload fields. Keys named explicitly by the
/// select list (`explicit_keys`: aliases, bare identifiers, dotted leaves)
/// always survive.
fn strip_injected_keys(
    row: &mut serde_json::Map<String, serde_json::Value>,
    has_wildcard: bool,
    explicit_keys: &[String],
    injected: &[String],
) {
    if !has_wildcard {
        return;
    }
    row.retain(|key, _| !injected.contains(key) || explicit_keys.contains(key));
}

/// Output key for one `FOREACH ... DO` field without an `AS` alias: a bare
/// identifier keeps its name, a dotted path keeps its leaf (mirroring the
/// engine's flattened projection), otherwise a positional `col_<idx>`.
fn foreach_output_key(expr: &rekuiper_sql::Expr, idx: usize) -> String {
    match expr {
        rekuiper_sql::Expr::Identifier(name) => name.clone(),
        rekuiper_sql::Expr::FieldAccess { field: leaf, .. } => leaf.clone(),
        rekuiper_sql::Expr::Index { base, .. } | rekuiper_sql::Expr::Slice { base, .. } => {
            foreach_output_key(base, idx)
        }
        _ => format!("col_{idx}"),
    }
}

/// Expand one `FOREACH` ingress into one projected row per array element.
///
/// Each element binds as `item` in an extended record (base message fields
/// plus `item`); the rule `WHERE` and the per-element `INCASE` guard are
/// both evaluated per element (fail closed on non-boolean/Null), and `DO`
/// fields project per element (no `DO` emits `{"item": element}`).
/// A non-array array value yields no rows. Window clauses never reach here
/// (the front end rejects `FOREACH` with a window).
fn expand_foreach_rows(
    stmt: &SelectStmt,
    plan: &ForeachPlan,
    base: &serde_json::Map<String, serde_json::Value>,
) -> Vec<serde_json::Map<String, serde_json::Value>> {
    let array_value = Evaluator::eval_val(&plan.array, base);
    let elements = match array_value {
        serde_json::Value::Array(items) => items,
        _ => return Vec::new(),
    };
    // Bound: one output row per input element; no accumulation beyond the
    // input array itself. Reason: `FOREACH` fans out over the array the
    // message already carries.
    let mut out = Vec::with_capacity(elements.len());
    for element in elements {
        let mut extended = base.clone();
        extended.insert("item".to_string(), element);
        let passes_where = match stmt.where_clause.as_ref() {
            Some(condition) => Evaluator::eval_bool(condition, &extended),
            None => true,
        };
        if !passes_where {
            continue;
        }
        let passes_incase = match plan.incase.as_ref() {
            Some(condition) => Evaluator::eval_bool(condition, &extended),
            None => true,
        };
        if !passes_incase {
            continue;
        }
        match plan.fields.as_ref() {
            None => {
                let mut row = serde_json::Map::new();
                if let Some(item) = extended.get("item") {
                    row.insert("item".to_string(), item.clone());
                }
                out.push(row);
            }
            Some(fields) => {
                let mut row = serde_json::Map::new();
                for (idx, (expr, alias)) in fields.iter().enumerate() {
                    let value = Evaluator::eval_val(expr, &extended);
                    let key = alias
                        .clone()
                        .unwrap_or_else(|| foreach_output_key(expr, idx));
                    row.insert(key, value);
                }
                out.push(row);
            }
        }
    }
    out
}

/// Replace the stream reference after the first top-level FROM with a
/// dummy identifier.
///
/// `rekuiper-sql` tokenizes the FROM target as a bare word, but MQTT
/// rules name quoted topic filters (`FROM "sensors/+"`, `FROM a/#`).
/// Rule dispatch matches on `topic_filter` and `eval_select` never reads
/// `stmt.from`, so the target is semantically inert here. A field
/// literally named `from` is not supported (creation fails loudly rather
/// than misparsing silently only if the rewrite breaks the statement).
fn from_target_bounds(sql: &str) -> Option<(usize, usize)> {
    let chars: Vec<(usize, char)> = sql.char_indices().collect();
    let n = chars.len();
    let is_word_char = |c: char| c.is_alphanumeric() || c == '_';

    let mut ci = 0;
    while ci < n {
        let (b, c) = chars[ci];
        let is_from = (c == 'f' || c == 'F')
            && sql.len() - b >= 4
            && sql[b..b + 4].eq_ignore_ascii_case("from")
            && (b == 0 || !is_word_char(sql[..b].chars().next_back().unwrap_or(' ')))
            && (b + 4 >= sql.len() || !is_word_char(sql[b + 4..].chars().next().unwrap_or(' ')));
        if !is_from {
            ci += 1;
            continue;
        }
        // Skip whitespace after FROM.
        let mut cj = ci + 4;
        while cj < n && chars[cj].1.is_whitespace() {
            cj += 1;
        }
        if cj >= n {
            return None;
        }
        let jb = chars[cj].0;
        let kb = {
            let q = chars[cj].1;
            if q == '"' || q == '\'' || q == '`' {
                let mut ck = cj + 1;
                while ck < n && chars[ck].1 != q {
                    ck += 1;
                }
                if ck >= n {
                    return None;
                }
                chars[ck].0 + q.len_utf8()
            } else {
                let mut ck = cj;
                while ck < n {
                    let ch = chars[ck].1;
                    if ch.is_whitespace() || matches!(ch, ';' | ',' | '(' | ')') {
                        break;
                    }
                    ck += 1;
                }
                if ck < n {
                    chars[ck].0
                } else {
                    sql.len()
                }
            }
        };
        return Some((jb, kb));
    }
    None
}

fn normalize_from_target(sql: &str) -> String {
    match from_target_bounds(sql) {
        Some((jb, kb)) => format!("{}stream{}", &sql[..jb], &sql[kb..]),
        None => sql.to_string(),
    }
}

/// The FROM target text as written, without its quote characters.
fn from_target_text(sql: &str) -> Option<String> {
    let (jb, kb) = from_target_bounds(sql)?;
    let raw = sql[jb..kb].trim();
    let text = if raw.len() >= 2 {
        let first = raw.chars().next();
        let last = raw.chars().next_back();
        match (first, last) {
            (Some(a), Some(b))
                if (a == '"' && b == '"') || (a == '\'' && b == '\'') || (a == '`' && b == '`') =>
            {
                &raw[1..raw.len() - 1]
            }
            _ => raw,
        }
    } else {
        raw
    };
    Some(text.to_string())
}

/// Rewrite colliding standard-dialect function names to internal `std_*`
/// aliases before parsing, so SQL evaluation uses the standard semantics
/// without changing the engine crate.
///
/// The engine resolves `substr`, `split`, `concat`, `length`,
/// `base64_encode`, `base64_decode` and `rtrim` against its own built-ins
/// before consulting the registered-function table, so a same-name
/// wrapper registered from outside can never take effect. Rewriting here
/// (outside the engine) is the from-outside-crate implementation: user
/// SQL keeps the documented names, the parser sees only the aliases,
/// and `stdlib::ensure_stdlib_registered` provides both spellings.
/// String literals (`'...'`, `"..."`, `` `...` ``) and comments are left
/// untouched; matching is case-insensitive and requires `(` after the
/// name so columns named e.g. `substr` are unaffected.
pub fn rewrite_colliding_functions(sql: &str) -> String {
    const MAPPINGS: &[(&str, &str)] = &[
        ("base64_encode", "std_base64_encode"),
        ("base64_decode", "std_base64_decode"),
        ("substr", "std_substr"),
        ("split", "std_split"),
        ("concat", "std_concat"),
        ("length", "std_length"),
        ("rtrim", "std_rtrim"),
    ];
    let bytes = sql.as_bytes();
    let n = bytes.len();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    let mut in_string: Option<u8> = None;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    let is_word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    while i < n {
        let c = bytes[i];
        // Preserve multi-byte UTF-8 in literals/comments: pushing a raw
        // byte via `as char` would corrupt it (e.g. 0xC3 becomes U+00C3
        // instead of the original scalar). All SQL syntax below is
        // ASCII-only, so a non-ASCII byte always belongs to a UTF-8 scalar
        // that is copied verbatim in any lexer state.
        if c >= 0x80 {
            if let Some(ch) = sql[i..].chars().next() {
                out.push(ch);
                i += ch.len_utf8();
            } else {
                i += 1;
            }
            continue;
        }
        if in_line_comment {
            out.push(c as char);
            if c == b'\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }
        if in_block_comment {
            out.push(c as char);
            if c == b'*' && i + 1 < n && bytes[i + 1] == b'/' {
                out.push('/');
                i += 2;
                in_block_comment = false;
            } else {
                i += 1;
            }
            continue;
        }
        if let Some(q) = in_string {
            out.push(c as char);
            if c == q {
                in_string = None;
            }
            i += 1;
            continue;
        }
        if c == b'\'' || c == b'"' || c == b'`' {
            in_string = Some(c);
            out.push(c as char);
            i += 1;
            continue;
        }
        if c == b'-' && i + 1 < n && bytes[i + 1] == b'-' {
            in_line_comment = true;
            out.push('-');
            out.push('-');
            i += 2;
            continue;
        }
        if c == b'/' && i + 1 < n && bytes[i + 1] == b'*' {
            in_block_comment = true;
            out.push('/');
            out.push('*');
            i += 2;
            continue;
        }
        if c.is_ascii_alphabetic() || c == b'_' {
            let start = i;
            while i < n && is_word(bytes[i]) {
                i += 1;
            }
            let word = &sql[start..i];
            let mut j = i;
            while j < n && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            let is_call = j < n && bytes[j] == b'(';
            if is_call {
                let lower = word.to_ascii_lowercase();
                let mut replaced = false;
                for (from, to) in MAPPINGS {
                    if lower == *from {
                        // Keep the call intact: alias plus original
                        // whitespace/paren from j onward.
                        out.push_str(to);
                        replaced = true;
                        break;
                    }
                }
                if !replaced {
                    out.push_str(word);
                }
            } else {
                out.push_str(word);
            }
            continue;
        }
        out.push(c as char);
        i += 1;
    }
    out
}

/// Dry-run SQL evaluation for the management console's payload tester.
///
/// Parses `sql` (if any) through the standard front end first (rule create,
/// update and rule-test all accept this dialect), checks `topic_filter`
/// coverage when given, and evaluates WHERE + SELECT over a JSON `payload`.
/// Returns `(matched, projected)`: `projected` is `Some` with the (possibly
/// projected) value on match and `None` when the predicate is false or the
/// payload cannot be evaluated. A `FOREACH` rule returns an array with one
/// row per element. Invalid SQL, topics, or filters fail with a message
/// carrying line and column for the offending token.
///
/// Side-effect free: the probe runs with `dry_run` set, so state-mutating
/// UDFs (`proc_dict_put/del`, `kv_store_put/del`) fail closed with `Null`
/// instead of touching the node-global maps. A dry-run never mutates
/// production rule state.
pub fn try_evaluate(
    sql: Option<&str>,
    topic_filter: Option<&str>,
    topic: &str,
    payload: &serde_json::Value,
) -> Result<(bool, Option<serde_json::Value>), String> {
    let probe = Topic::new(topic).map_err(|e| e.to_string())?;
    if let Some(filter) = topic_filter {
        if !filter.trim().is_empty() {
            let parsed =
                TopicFilter::new(filter).map_err(|e| format!("invalid topic_filter: {e}"))?;
            if !parsed.matches(&probe) {
                return Ok((false, None));
            }
        }
    }
    let (stmt, foreach, sources, has_wildcard, explicit_keys) = match sql {
        Some(sql) if !sql.trim().is_empty() => {
            let (stripped, _) = split_into_connector(sql).map_err(|e| e.to_string())?;
            let (stmt, foreach, sources, has_wildcard, explicit_keys) =
                parse_rule_sql_for_tester(&stripped)?;
            (Some(stmt), foreach, sources, has_wildcard, explicit_keys)
        }
        _ => (None, None, Vec::new(), false, Vec::new()),
    };
    stdlib::ensure_stdlib_registered();
    let source_filters = compile_std_topic_filters(&sources);
    if !std_sources_match(&sources, &source_filters, &probe) {
        return Ok((false, None));
    }
    let is_std = !sources.is_empty() || foreach.is_some();
    if let Some(plan) = foreach.as_ref() {
        // INVARIANT: FOREACH execution plan requires a valid compiled rule statement.
        let stmt_ref = stmt.as_ref().expect("foreach always carries a statement");
        match payload {
            serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
                let bases: Vec<serde_json::Map<String, serde_json::Value>> = match payload {
                    serde_json::Value::Object(map) => {
                        vec![build_std_value_record(&probe, map.clone(), payload).0]
                    }
                    serde_json::Value::Array(elements) => elements
                        .iter()
                        .filter_map(|element| match element {
                            serde_json::Value::Object(map) => {
                                Some(build_std_value_record(&probe, map.clone(), element).0)
                            }
                            _ => None,
                        })
                        .collect(),
                    _ => Vec::new(),
                };
                let mut rows: Vec<serde_json::Value> = Vec::new();
                for base in &bases {
                    for row in expand_foreach_rows(stmt_ref, plan, base) {
                        rows.push(serde_json::Value::Object(row));
                    }
                }
                if rows.is_empty() {
                    return Ok((false, None));
                }
                if matches!(payload, serde_json::Value::Object(_)) {
                    return Ok((true, Some(serde_json::Value::Array(rows))));
                }
                return Ok((true, Some(serde_json::Value::Array(rows))));
            }
            _ => {
                let (base, _) = build_std_value_record(&probe, serde_json::Map::new(), payload);
                let rows: Vec<serde_json::Value> = expand_foreach_rows(stmt_ref, plan, &base)
                    .into_iter()
                    .map(serde_json::Value::Object)
                    .collect();
                if rows.is_empty() {
                    return Ok((false, None));
                }
                return Ok((true, Some(serde_json::Value::Array(rows))));
            }
        }
    }
    match payload {
        serde_json::Value::Array(elements) => {
            // Batch dry-run: multi-event aggregation over the object
            // elements (non-objects skipped), one row per group.
            let records: Vec<AggregateRecord<Vec<String>>> = elements
                .iter()
                .filter_map(|element| match element {
                    serde_json::Value::Object(map) => {
                        let (record, injected) = if is_std {
                            build_std_value_record(&probe, map.clone(), element)
                        } else {
                            (map.clone(), Vec::new())
                        };
                        Some((probe.clone(), record, injected))
                    }
                    _ => None,
                })
                .collect();
            if records.is_empty() {
                return Ok((false, None));
            }
            match stmt {
                None => {
                    // No SQL: the batch passes through as given.
                    Ok((true, Some(payload.clone())))
                }
                Some(stmt) => {
                    let rows: Vec<serde_json::Value> =
                        aggregate_partitioned_with_metadata(&stmt, records, "dry-run", true)
                            .into_iter()
                            .map(|(_, mut row, injected)| {
                                if is_std {
                                    strip_injected_keys(
                                        &mut row,
                                        has_wildcard,
                                        &explicit_keys,
                                        &injected,
                                    );
                                }
                                serde_json::Value::Object(row)
                            })
                            .collect();
                    if rows.is_empty() {
                        Ok((false, None))
                    } else {
                        Ok((true, Some(serde_json::Value::Array(rows))))
                    }
                }
            }
        }
        serde_json::Value::Object(map) => {
            let (record, injected) = if is_std {
                let base: serde_json::Map<String, serde_json::Value> = map.clone();
                build_std_value_record(&probe, base, payload)
            } else {
                (map.clone(), Vec::new())
            };
            match stmt {
                None => Ok((true, Some(payload.clone()))),
                Some(stmt) => {
                    stdlib::ensure_stdlib_registered();
                    let ctx = stdlib::RuleContext {
                        topic: Some(probe.as_str().to_string()),
                        record: record.clone(),
                        payload_raw: serde_json::to_string(&payload).ok(),
                        rule_id: "dry-run".to_string(),
                        dry_run: true,
                        ..Default::default()
                    };
                    let _guard = stdlib::RuleContextGuard::enter(ctx);
                    match Evaluator::eval_select(&stmt, &record) {
                        Some(projected) => {
                            let mut projected: serde_json::Map<String, serde_json::Value> =
                                projected.into_iter().collect();
                            if is_std {
                                strip_injected_keys(
                                    &mut projected,
                                    has_wildcard,
                                    &explicit_keys,
                                    &injected,
                                );
                            }
                            let object: serde_json::Map<String, serde_json::Value> = projected;
                            Ok((true, Some(serde_json::Value::Object(object))))
                        }
                        None => Ok((false, None)),
                    }
                }
            }
        }
        _ => {
            if !is_std {
                return Ok((false, None));
            }
            let (record, injected) =
                build_std_value_record(&probe, serde_json::Map::new(), payload);
            match stmt {
                None => Ok((true, Some(payload.clone()))),
                Some(stmt) => {
                    stdlib::ensure_stdlib_registered();
                    let ctx = stdlib::RuleContext {
                        topic: Some(probe.as_str().to_string()),
                        record: record.clone(),
                        payload_raw: serde_json::to_string(&payload).ok(),
                        rule_id: "dry-run".to_string(),
                        dry_run: true,
                        ..Default::default()
                    };
                    let _guard = stdlib::RuleContextGuard::enter(ctx);
                    match Evaluator::eval_select(&stmt, &record) {
                        Some(projected) => {
                            let mut projected: serde_json::Map<String, serde_json::Value> =
                                projected.into_iter().collect();
                            strip_injected_keys(
                                &mut projected,
                                has_wildcard,
                                &explicit_keys,
                                &injected,
                            );
                            let object: serde_json::Map<String, serde_json::Value> = projected;
                            Ok((true, Some(serde_json::Value::Object(object))))
                        }
                        None => Ok((false, None)),
                    }
                }
            }
        }
    }
}

/// Parse one tester SQL string like [`parse_rule_sql`] but with a `String`
/// error for the management API boundary.
fn parse_rule_sql_for_tester(sql: &str) -> Result<ParsedStdRule, String> {
    let rewritten = rewrite_colliding_functions(sql);
    match std_sql::parse_std_rule(&rewritten) {
        Ok(std_rule) => {
            let mut stmt = std_rule.stmt.clone();
            if std_rule.foreach.is_none() {
                apply_implicit_group_window(&mut stmt);
            }
            refuse_unsupported_sql_calls(&stmt, std_rule.foreach.as_ref())?;
            Ok((
                stmt,
                std_rule.foreach.clone(),
                std_rule.sources.clone(),
                std_rule.has_wildcard,
                std_rule.explicit_keys.clone(),
            ))
        }
        Err(std_err) => {
            let normalized = normalize_from_target(&rewritten);
            match rekuiper_sql::Parser::new(&normalized).parse_select() {
                Ok(mut stmt) => {
                    apply_implicit_group_window(&mut stmt);
                    refuse_unsupported_sql_calls(&stmt, None)?;
                    let (has_wildcard, explicit_keys) = projection_metadata(&stmt);
                    Ok((stmt, None, Vec::new(), has_wildcard, explicit_keys))
                }
                Err(_) => Err(format!("invalid sql_query: {std_err}")),
            }
        }
    }
}

/// Build the dry-run evaluation record from an object map plus the raw
/// payload value: the object keys stay at top level, and `payload`/`topic`
/// are injected only when absent (`qos` is omitted here: the tester carries
/// no QoS, never a constant standing in for it). Returns the record plus
/// the keys this call injected (for the wildcard strip).
fn build_std_value_record(
    topic: &Topic,
    mut base: serde_json::Map<String, serde_json::Value>,
    payload: &serde_json::Value,
) -> (serde_json::Map<String, serde_json::Value>, Vec<String>) {
    let mut injected = Vec::new();
    if !base.contains_key("payload") {
        base.insert("payload".to_string(), payload.clone());
        injected.push("payload".to_string());
    }
    if !base.contains_key("topic") {
        base.insert(
            "topic".to_string(),
            serde_json::Value::String(topic.as_str().to_string()),
        );
        injected.push("topic".to_string());
    }
    (base, injected)
}

/// Split a trailing `INTO connector("<id>")` clause off a rule SQL
/// statement, returning `(statement_without_into, connector_id)`.
///
/// The scan is top-level only (parentheses and quoted strings are
/// skipped), case-insensitive, and strict: a malformed INTO fails rule
/// creation instead of silently changing routing. A field literally
/// named `into` is unsupported (creation fails loudly in that case).
fn split_into_connector(sql: &str) -> Result<(String, Option<String>), RuleEngineError> {
    fn invalid(reason: impl Into<String>) -> RuleEngineError {
        RuleEngineError::InvalidRule(format!("invalid INTO clause: {}", reason.into()))
    }
    let chars: Vec<(usize, char)> = sql.char_indices().collect();
    let n = chars.len();
    let is_word = |c: char| c.is_alphanumeric() || c == '_';

    let mut i = 0;
    let mut depth = 0u32;
    let mut in_string: Option<char> = None;
    while i < n {
        let (b, c) = chars[i];
        if let Some(quote) = in_string {
            if c == quote {
                in_string = None;
            }
            i += 1;
            continue;
        }
        match c {
            '"' | '\'' | '`' => {
                in_string = Some(c);
                i += 1;
            }
            '(' => {
                depth += 1;
                i += 1;
            }
            ')' => {
                depth = depth.saturating_sub(1);
                i += 1;
            }
            _ => {
                let is_into = depth == 0
                    && (c == 'i' || c == 'I')
                    && sql.len() - b >= 4
                    && sql[b..b + 4].eq_ignore_ascii_case("into")
                    && (b == 0 || !is_word(sql[..b].chars().next_back().unwrap_or(' ')))
                    && (b + 4 >= sql.len() || !is_word(sql[b + 4..].chars().next().unwrap_or(' ')));
                if !is_into {
                    i += 1;
                    continue;
                }
                let mut j = i + 4;
                while j < n && chars[j].1.is_whitespace() {
                    j += 1;
                }
                let word_start = j;
                while j < n && (chars[j].1.is_alphanumeric() || chars[j].1 == '_') {
                    j += 1;
                }
                let word: String = chars[word_start..j].iter().map(|(_, c)| *c).collect();
                if !word.eq_ignore_ascii_case("connector") {
                    return Err(invalid("INTO must target connector(\"<id>\")"));
                }
                while j < n && chars[j].1.is_whitespace() {
                    j += 1;
                }
                if j >= n || chars[j].1 != '(' {
                    return Err(invalid("expected '(' after connector"));
                }
                j += 1;
                while j < n && chars[j].1.is_whitespace() {
                    j += 1;
                }
                if j >= n || !matches!(chars[j].1, '"' | '\'') {
                    return Err(invalid("connector id must be quoted"));
                }
                let quote = chars[j].1;
                j += 1;
                let id_start = j;
                while j < n && chars[j].1 != quote {
                    j += 1;
                }
                if j >= n {
                    return Err(invalid("unterminated connector id"));
                }
                let id: String = chars[id_start..j].iter().map(|(_, c)| *c).collect();
                j += 1;
                while j < n && chars[j].1.is_whitespace() {
                    j += 1;
                }
                if j >= n || chars[j].1 != ')' {
                    return Err(invalid("expected ')' after connector id"));
                }
                j += 1;
                if id.is_empty() {
                    return Err(invalid("connector id must not be empty"));
                }
                let tail: String = chars[j..].iter().map(|(_, c)| *c).collect();
                let tail_trimmed = tail.trim().trim_end_matches(';').trim_end();
                if !tail_trimmed.is_empty() {
                    return Err(invalid("unexpected trailing input after ')'"));
                }
                let stripped = sql[..b].trim_end().to_string();
                return Ok((stripped, Some(id)));
            }
        }
    }
    Ok((sql.to_string(), None))
}

/// Run one ingress payload through one rule's parsed statement, consulting
/// the standard front-end projection state (`foreach`, `sources`,
/// `has_wildcard`, `explicit_keys`).
///
/// Returns every output payload to forward (empty means skip the rule):
/// a `FOREACH` rule expands to one payload per array element, a plain rule
/// yields at most one. Pre-front-end rules (empty sources, no `FOREACH`,
/// wildcard or explicit) run the historical raw-record path so existing
/// rekuiper-dialect rules keep their behaviour and hot-path numbers.
///
/// The caller already matched the rule against the topic through
/// [`rule_matches_ingress`], so this does not repeat the source check.
fn apply_rule(rule: &Rule, topic: &Topic, payload: &Bytes, qos: QoS) -> Vec<Bytes> {
    let stmt = match rule.parsed_query.as_ref() {
        Some(stmt) => stmt,
        None => return vec![payload.clone()],
    };
    if let Some(plan) = rule.std_foreach.as_ref() {
        // FOREACH WHERE evaluates per element with `item` bound
        // (expand_foreach_rows applies `stmt.where_clause` to the
        // extended record carrying `item`).
        let (base, _, _) = build_std_record(topic, payload, qos);
        let rows = expand_foreach_rows(stmt, plan, &base);
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let object = row;
            if let Ok(bytes) = serde_json::to_vec(&serde_json::Value::Object(object)) {
                out.push(Bytes::from(bytes));
            } else {
                tracing::warn!(
                    rule_id = %rule.id,
                    "FOREACH row not serializable; dropped"
                );
            }
        }
        return out;
    }
    let is_std = !rule.std_sources.is_empty();
    if !is_std {
        return apply_sql(
            &rule.parsed_query,
            payload,
            &rule.id,
            Some(topic),
            Some(qos),
        )
        .into_iter()
        .collect();
    }
    let (record, injected, payload_is_object) = build_std_record(topic, payload, qos);
    stdlib::ensure_stdlib_registered();
    let ctx = stdlib::RuleContext {
        topic: Some(topic.as_str().to_string()),
        qos: Some(qos as u8),
        record: record.clone(),
        payload_raw: std::str::from_utf8(payload).ok().map(str::to_string),
        rule_id: rule.id.clone(),
        ..Default::default()
    };
    let _guard = stdlib::RuleContextGuard::enter(ctx);
    match Evaluator::eval_select(stmt, &record) {
        Some(projected) => {
            let mut projected: serde_json::Map<String, serde_json::Value> =
                projected.into_iter().collect();
            strip_injected_keys(
                &mut projected,
                rule.std_has_wildcard,
                &rule.std_explicit_keys,
                &injected,
            );
            // SELECT * keeps the payload fields only. A plain
            // payload yields no field after the strip, so the
            // row is empty. The window path yields the same.
            // When the projection equals the input object, forward
            // the raw bytes so input spacing stays on the wire.
            // Reuse the single parse from `build_std_record`.
            // No second parse runs on this path.
            let object: serde_json::Map<String, serde_json::Value> = projected;
            let mut same =
                payload_is_object && object.len() == record.len().saturating_sub(injected.len());
            if same {
                for (key, value) in object.iter() {
                    if injected.iter().any(|name| name == key) {
                        same = false;
                        break;
                    }
                    if record.get(key) != Some(value) {
                        same = false;
                        break;
                    }
                }
            }
            if same {
                for (key, value) in record.iter() {
                    if injected.iter().any(|name| name == key) {
                        continue;
                    }
                    if object.get(key) != Some(value) {
                        same = false;
                        break;
                    }
                }
            }
            // Order decides: a reordered projection serializes in
            // select order. Raw bytes stay only for the same order.
            if same {
                let record_keys = record
                    .keys()
                    .filter(|key| !injected.iter().any(|name| name == *key));
                if !record_keys.eq(object.keys()) {
                    same = false;
                }
            }
            if same {
                return vec![payload.clone()];
            }
            match serde_json::to_vec(&serde_json::Value::Object(object)) {
                Ok(bytes) => vec![Bytes::from(bytes)],
                Err(e) => {
                    tracing::warn!(
                        rule_id = %rule.id,
                        error = %e,
                        "SQL rule skipped: projected payload not serializable"
                    );
                    Vec::new()
                }
            }
        }
        None => Vec::new(),
    }
}

/// Run one ingress payload through an optional parsed query.
///
/// Returns the bytes to forward: the raw payload when no SQL is present,
/// the projected JSON when evaluation succeeds, or `None` to skip the
/// rule (WHERE false, non-JSON/non-object payload, serialization
/// failure). Never panics on attacker-controlled bytes.
///
/// `topic`/`qos` feed the RQ-03 evaluation context so zero-argument
/// accessors (`topic()`, `payload()`, ...) resolve; client identity fields
/// stay `Null` here because ingress dispatch does not carry them yet
/// (RQ-04 supplies event context).
fn apply_sql(
    parsed: &Option<SelectStmt>,
    payload: &Bytes,
    rule_id: &str,
    topic: Option<&Topic>,
    qos: Option<QoS>,
) -> Option<Bytes> {
    let stmt = match parsed {
        None => return Some(payload.clone()),
        Some(stmt) => stmt,
    };
    let record: serde_json::Map<String, serde_json::Value> = match serde_json::from_slice(payload) {
        Ok(serde_json::Value::Object(map)) => map,
        _ => {
            tracing::debug!(
                rule_id = %rule_id,
                "SQL rule skipped: ingress payload is not a JSON object"
            );
            return None;
        }
    };
    stdlib::ensure_stdlib_registered();
    let ctx = stdlib::RuleContext {
        topic: topic.map(|t| t.as_str().to_string()),
        qos: qos.map(|q| q as u8),
        record: record.clone(),
        payload_raw: std::str::from_utf8(payload).ok().map(str::to_string),
        rule_id: rule_id.to_string(),
        ..Default::default()
    };
    let _guard = stdlib::RuleContextGuard::enter(ctx);
    match Evaluator::eval_select(stmt, &record) {
        Some(projected) => {
            // Keep the evaluator's projection order in the JSON object.
            // The projection is serialized. Input spacing is not
            // preserved on this path.
            let object: serde_json::Map<String, serde_json::Value> =
                projected.into_iter().collect();
            match serde_json::to_vec(&serde_json::Value::Object(object)) {
                Ok(bytes) => Some(Bytes::from(bytes)),
                Err(e) => {
                    tracing::warn!(
                        rule_id = %rule_id,
                        error = %e,
                        "SQL rule skipped: projected payload not serializable"
                    );
                    None
                }
            }
        }
        // WHERE predicate false: rule does not fire.
        None => None,
    }
}

#[cfg(test)]
mod tests;
