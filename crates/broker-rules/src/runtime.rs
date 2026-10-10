//! RX-01: broker rules run on rekuiper's runtime.
//!
//! One embedded rule runtime per broker node, fed in-process from the
//! publish path ([`crate::RuleEngine::dispatch_ingress`], called per publish
//! by `ingress_pipeline_with_publisher` in `crates/broker-node/src/main.rs`
//! and by the management publish path in
//! `crates/broker-api/src/v5/clients.rs`) with no MQTT hop and no TCP or
//! loopback connection between the publish path and the runtime, ever.
//!
//! Feed: the ingress `(topic, payload, qos)` is converted once per windowed
//! ingress to a [`rekuiper_core::StreamRecord`] by
//! [`ingress_to_stream_record`] and handed to the runtime over the
//! in-process bounded [`rekuiper_core::StreamBus`] via
//! [`rekuiper_core::StreamSender::send`] (async admission with
//! backpressure: a burst awaits subscriber capacity while the feeder
//! drains; an admission failure drops with a warning and a counter).
//! [`rekuiper_core::StreamSender::try_send`] (sync, non-blocking) stays
//! reserved for cyclic feedback only. Plain (stateless) rules keep evaluating
//! inline in `dispatch_ingress` through the RQ-02 front end onto
//! `rekuiper-sql`'s evaluator, so the publish hot path pays no new lock and
//! no new allocation when no window task exists.
//!
//! Output: [`RuleOutputSink`] is the caller-provided sink trait the runtime
//! delivers through. The broker's implementation bridges to
//! [`broker_connectors::ConnectorManager::send`] and
//! [`crate::BrokerSink::publish`]. Republish keeps the current guarantee:
//! delivery QoS is `min(ingress QoS, action QoS)` so a rule can never
//! upgrade delivery guarantees, and republished messages are never
//! re-dispatched into rules (no recursion), as today.
//!
//! What is still missing (CTO scope, not this task): `rekuiper-core` at the
//! pinned RQ-01 revision exposes the transport (`StreamBus`, `StreamRecord`,
//! per-rule [`rekuiper_core::RuleCounters`]) but no embeddable rule-task
//! API. Its `RuleManager` stores rule definitions only; rule execution
//! (subscribe, eval loops, sink worker, prepared actions) lives in private
//! functions of rekuiper's server layer, which the broker cannot call. The
//! broker therefore builds its rule tasks here on the public transport and
//! evaluator APIs: create a task per windowed rule from its parsed statement
//! plus actions, feed `StreamRecord`s over the bounded bus, deliver through
//! the caller-provided sink trait, start/stop/delete with abort semantics,
//! and keep per-rule counters and status. This module patches no rekuiper
//! source and copies no private server code.
//!
//! The RQ-02 front end stays the only parser. A rule is stored with the SQL
//! the user sent; the lowered AST executes on rekuiper's runtime, so there
//! is one engine and one fast path. Tier labels (`Community`/`Enterprise`)
//! stay for display but never select the execution path: every window rule
//! runs on this runtime. Window state is in memory and is not persisted
//! across restarts (decision, do not reopen).
//!
//! Rule create, update (enable flag), delete and the rule-test endpoint keep
//! working through `crates/broker-api` (v1 routes and v5 rules call
//! `RuleEngine::create_rule`, `remove_rule`, `set_rule_enabled` and
//! `try_evaluate`, whose signatures are unchanged).

use broker_protocol::{QoS, Topic};
use bytes::Bytes;
use rekuiper_core::StreamRecord;
use std::collections::HashMap;

/// Rule ingress queue capacity default: 1024 events. Reason: today's
/// memory-only default, so the move changes no memory profile.
/// See [`crate::BoundedEventInput`].
pub const RULE_INGRESS_QUEUE_CAPACITY: usize = 1024;

/// Stream bus bound, per subscriber: 4096 records. Reason: rekuiper's
/// `STREAM_QUEUE_CAPACITY`, the bound the runtime was benchmarked with.
/// Re-exported so the bound is named once.
pub use rekuiper_core::STREAM_QUEUE_CAPACITY;

/// Per-rule sink queue (`bufferLength`) default: 10,000 records. Reason:
/// rekuiper's default, the bound the runtime was benchmarked with. A full
/// sink queue applies backpressure to the rule's eval loop (which awaits
/// capacity); the publish path itself never blocks because the rule-task
/// feed in front of it drops with a warning and a counter first.
pub const PER_RULE_SINK_QUEUE_DEPTH: usize = 10_000;

/// Rule-task feed default: 65,536 records. Reason: today's
/// [`crate::DEFAULT_WINDOW_CHANNEL_DEPTH`] (burst headroom); a full feed
/// drops with a warning and a counter, never blocks publish. Alias kept
/// next to the bound it names so the two can never drift apart.
pub use crate::DEFAULT_WINDOW_CHANNEL_DEPTH as RULE_TASK_FEED_DEPTH;

// Connector send path: every `ForwardConnector` action executes from a
// rule-task sink worker behind that rule's bounded sink queue
// (`PER_RULE_SINK_QUEUE_DEPTH`). The send path runs behind the rule
// engine's bounded queue: it needs a bound, not numbers.
// See `broker_connectors::ConnectorManager::send`.

// ---------------------------------------------------------------------------
// Ingress stream.
// ---------------------------------------------------------------------------

/// The single logical ingress stream every rule task subscribes to. Reason:
/// one fan-out point keeps the publish path to a single conversion plus a
/// single non-blocking send no matter how many window rules exist; tasks
/// self-filter by their own topic filter, so routing needs no shared lock
/// on publish.
pub const RULE_INGRESS_STREAM: &str = "broker/ingress";

/// Reserved transport keys inside a [`StreamRecord`]'s data map. A payload
/// that literally carries these top-level keys collides with the transport
/// (the same class of reservation as the `__window_start__` keys the flush
/// path already injects): the transport value wins. Window tests and the
/// RQ-02 corpus carry no such keys.
pub const RUNTIME_TOPIC_KEY: &str = "__runtime_topic__";
pub const RUNTIME_QOS_KEY: &str = "__runtime_qos__";
pub const RUNTIME_ADDED_KEY: &str = "__runtime_added__";
/// Tenant of the ingress that produced the record. One rule task
/// serves one tenant, so the feeder drops records of other tenants.
pub const RUNTIME_TENANT_KEY: &str = "__runtime_tenant__";

/// Convert one ingress `(tenant, topic, payload, qos)` to a [`StreamRecord`].
/// Called exactly once per windowed ingress in `dispatch_ingress`.
///
/// The data map mirrors [`crate::build_std_record`]: the JSON object keys
/// stay at top level, and `payload`/`topic`/`qos` are injected only when
/// absent (the returned record carries the injected names in
/// [`RUNTIME_ADDED_KEY`] so the feeder can rebuild the exact evaluation
/// record for either dialect mode). The real routing tenant, topic and
/// QoS always travel under [`RUNTIME_TENANT_KEY`]/[`RUNTIME_TOPIC_KEY`]/
/// [`RUNTIME_QOS_KEY`]. Fields the broker
/// cannot supply are omitted, never a constant standing in for a
/// measurement. The timestamp is wall-clock millis at conversion.
pub fn ingress_to_stream_record(
    tenant: &str,
    topic: &Topic,
    payload: &Bytes,
    qos: QoS,
) -> StreamRecord {
    let parsed: Option<serde_json::Value> = serde_json::from_slice(payload).ok();
    let is_object = matches!(parsed, Some(serde_json::Value::Object(_)));
    let mut data: HashMap<String, serde_json::Value> = match parsed.clone() {
        Some(serde_json::Value::Object(map)) => map.into_iter().collect(),
        _ => HashMap::new(),
    };
    let mut added: Vec<String> = Vec::new();
    if !data.contains_key("payload") {
        let decoded: serde_json::Value = parsed.unwrap_or_else(|| {
            serde_json::Value::String(String::from_utf8_lossy(payload).into_owned())
        });
        data.insert("payload".to_string(), decoded);
        added.push("payload".to_string());
    }
    if !data.contains_key("topic") {
        data.insert(
            "topic".to_string(),
            serde_json::Value::String(topic.as_str().to_string()),
        );
        added.push("topic".to_string());
    }
    if !data.contains_key("qos") {
        data.insert("qos".to_string(), serde_json::Value::from(qos as u8));
        added.push("qos".to_string());
    }
    data.insert(
        RUNTIME_TOPIC_KEY.to_string(),
        serde_json::Value::String(topic.as_str().to_string()),
    );
    data.insert(
        RUNTIME_QOS_KEY.to_string(),
        serde_json::Value::from(qos as u8),
    );
    data.insert(
        RUNTIME_TENANT_KEY.to_string(),
        serde_json::Value::String(tenant.to_string()),
    );
    data.insert(
        RUNTIME_ADDED_KEY.to_string(),
        serde_json::json!({
            "added": added,
            "object": is_object,
        }),
    );
    StreamRecord {
        timestamp: crate::now_ms() as i64,
        data,
    }
}

/// Rebuilt feeder input: the real ingress tenant and topic, its QoS
/// number, the evaluation record and the injected-key list, plus
/// whether the payload carried a JSON object at top level.
pub struct FeederRecord {
    /// Tenant of the ingress (routing context; rules serve one tenant).
    pub tenant: String,
    /// Real ingress topic (routing context; never the payload's `topic`).
    pub topic: Topic,
    /// Real ingress QoS number (0/1/2).
    pub qos: u8,
    /// Evaluation record with transport keys removed.
    pub record: HashMap<String, serde_json::Value>,
    /// Keys the conversion injected (for the `SELECT *` wildcard strip).
    pub injected: Vec<String>,
    /// False when the payload was not a JSON object (nothing to evaluate).
    pub has_object: bool,
}

/// Split a [`StreamRecord`] back into feeder input. Returns `None` when the
/// transport keys are missing or malformed (fail closed: the record is
/// counted and skipped, never evaluated against a guessed topic).
pub fn split_stream_record(mut data: HashMap<String, serde_json::Value>) -> Option<FeederRecord> {
    let topic_value = data.remove(RUNTIME_TOPIC_KEY)?;
    let topic_str = topic_value.as_str()?.to_string();
    let tenant = data.remove(RUNTIME_TENANT_KEY)?.as_str()?.to_string();
    let qos = data
        .remove(RUNTIME_QOS_KEY)?
        .as_u64()
        .and_then(|q| u8::try_from(q).ok())?;
    let (added, has_object) = match data.remove(RUNTIME_ADDED_KEY)? {
        serde_json::Value::Object(mut annotation) => {
            let added = match annotation.remove("added") {
                Some(serde_json::Value::Array(items)) => {
                    let mut added = Vec::with_capacity(items.len());
                    for item in items {
                        added.push(item.as_str()?.to_string());
                    }
                    added
                }
                _ => return None,
            };
            let has_object = match annotation.remove("object") {
                Some(serde_json::Value::Bool(is_object)) => is_object,
                _ => return None,
            };
            (added, has_object)
        }
        _ => return None,
    };
    let topic = Topic::new(&topic_str).ok()?;
    Some(FeederRecord {
        tenant,
        topic,
        qos,
        record: data,
        injected: added,
        has_object,
    })
}

// ---------------------------------------------------------------------------
// Output bridge.
// ---------------------------------------------------------------------------

/// Caller-provided sink trait the runtime delivers through. The broker's
/// implementation bridges to [`broker_connectors::ConnectorManager::send`]
/// and [`crate::BrokerSink::publish`]. Failures are counted and logged
/// inside the implementation; remaining actions still run.
#[async_trait::async_trait]
pub trait RuleOutputSink: Send + Sync {
    /// Deliver one evaluated output row. `topic` is the partition's first
    /// record topic (real routing context, as with window flushes).
    /// `qos_cap` is the flushed batch's ingress-QoS floor so delivery
    /// stays `min(ingress QoS, action QoS)`.
    async fn emit(&self, topic: Topic, payload: Bytes, qos_cap: QoS);
}
