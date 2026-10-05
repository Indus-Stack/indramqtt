//! Live observability, rate metrics, and alarm polling for the v5 REST API.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

use crate::node_scope::resolve_node;
use crate::ApiState;

use crate::node_scope::local_node_name;

/// Live gauges for the current snapshot, read from the session
/// directory and lock-free metrics. Management-plane only.
fn live_counters(state: &ApiState) -> crate::v5::monitor::LiveCounters {
    let conns = state.metrics.connections_active().max(0) as u64;
    let active_ids = state.sessions.active_client_ids();
    let mut subs: u64 = 0;
    let mut topic_set = std::collections::HashSet::new();
    for cid in &active_ids {
        if let Some(s) = state.sessions.get(cid) {
            let map = s.subscriptions.read();
            subs = subs.saturating_add(map.len() as u64);
            for f in map.keys() {
                topic_set.insert(f.as_str().to_string());
            }
        }
    }
    crate::v5::monitor::LiveCounters {
        connections: conns,
        live_connections: conns,
        topics: topic_set.len() as u64,
        subscriptions: subs,
        received: state.metrics.messages_received(),
        sent: state.metrics.messages_forwarded(),
        dropped: state.metrics.messages_dropped(),
        rules_matched: state.metrics.rules_executed(),
    }
}

/// Documented `GET /monitor_current` snapshot: one object with the
/// current gauges and the message rates of the last sampling window.
///
/// Gauges (`connections`, `live_connections`, `topics`, `subscriptions`,
/// `cluster_sessions`, `retained_msg_count`) are read live from the
/// session directory, the lock-free metrics and the statistics store. The
/// durable gauges have no kernel tracking and carry over from the newest
/// history point, else zero.
///
/// Each `*_rate` field is the change of one cumulative counter between the
/// two newest points of the monitor history, divided by the time between
/// the points. The sampler in the kernel writes a point on a timer, so the
/// rate covers one sampling window. Before two points exist, and after
/// `DELETE /monitor`, every rate reads zero because no window has passed.
/// Counters that no subsystem records yet (persistence, validation,
/// transformation and action counters) keep their rate at zero. The fields
/// `license_quota`, `shared_subscriptions` and `sessions_hist_hwmark` are
/// left out because the broker keeps no such value.
///
/// Never paged; always succeeds when the node is up. The query string is
/// not read, so unknown query parameters are ignored instead of rejected.
/// Management-plane only: one metrics snapshot, one bounded session-directory
/// scan and one short history lock per request; nothing here runs on the
/// per-message path and no new buffering is added to fan-out or fan-in.
pub async fn monitor_current(State(state): State<ApiState>) -> Response {
    (StatusCode::OK, Json(current_snapshot_value(&state))).into_response()
}

/// Shared snapshot read for `GET /monitor_current` and its node-scoped
/// twin below. Combines live gauges with the rates of the newest history
/// window in constant time; no sampling work is added to the message path.
fn current_snapshot_value(state: &ApiState) -> serde_json::Value {
    use crate::v5::monitor::rate_per_sec;
    let live = live_counters(state);
    let (newest, previous) = state.monitor.newest_two();
    let mut body = serde_json::Map::with_capacity(24);
    let mut put = |key: &str, value: u64| {
        body.insert(key.to_string(), serde_json::Value::from(value));
    };
    put("connections", live.connections);
    put("live_connections", live.live_connections);
    put("topics", live.topics);
    put("subscriptions", live.subscriptions);
    put("cluster_sessions", state.sessions.session_count() as u64);
    put("retained_msg_count", state.stats.snapshot().retained);
    put(
        "subscriptions_durable",
        newest.as_ref().map_or(0, |s| s.subscriptions_durable),
    );
    put(
        "disconnected_durable_sessions",
        newest
            .as_ref()
            .map_or(0, |s| s.disconnected_durable_sessions),
    );
    // Rates need two points. Without them no window has passed.
    let rate = |counter: fn(&crate::v5::monitor::MonitorSample) -> u64| -> u64 {
        match (&newest, &previous) {
            (Some(newer), Some(older)) => rate_per_sec(
                counter(newer),
                counter(older),
                newer.time_stamp.saturating_sub(older.time_stamp),
            ),
            _ => 0,
        }
    };
    put("received_msg_rate", rate(|s| s.received));
    put("sent_msg_rate", rate(|s| s.sent));
    put("dropped_msg_rate", rate(|s| s.dropped));
    put("rules_matched_rate", rate(|s| s.rules_matched));
    put("persisted_rate", rate(|s| s.persisted));
    put("actions_executed_rate", rate(|s| s.actions_executed));
    put("actions_messages_rate", rate(|s| s.actions_messages));
    put(
        "validation_succeeded_rate",
        rate(|s| s.validation_succeeded),
    );
    put("validation_failed_rate", rate(|s| s.validation_failed));
    put(
        "transformation_succeeded_rate",
        rate(|s| s.transformation_succeeded),
    );
    put(
        "transformation_failed_rate",
        rate(|s| s.transformation_failed),
    );
    serde_json::Value::Object(body)
}

/// `GET /monitor_current/nodes/{node}`: node-scoped read of the same
/// snapshot as `GET /monitor_current`.
///
/// Single node, management-plane only: the named node is checked with the
/// shared [`resolve_node`] helper against the configured [`ApiState::node_id`]
/// (falling back to the [`local_node_name`] name the v5 rows render
/// today); an unknown name returns the helper's 404 `NOT_FOUND` shape
/// naming the node. A known name returns the same single-object shape as
/// [`monitor_current`] (same numeric fields, same history carry-over),
/// with one metrics snapshot, one bounded session-directory scan and one
/// short history lock per request and no work on the per-message path.
pub async fn monitor_current_node(
    State(state): State<ApiState>,
    Path(node): Path<String>,
) -> Response {
    if resolve_node(&state.node_id, &node).is_err()
        && resolve_node(local_node_name(), &node).is_err()
    {
        // Reuse the helper's error so the unknown-node shape stays exactly
        // the documented `{code: NOT_FOUND, message}` body.
        return resolve_node(&state.node_id, &node)
            .expect_err("node already checked as unknown")
            .into_response();
    }
    let mut body = current_snapshot_value(&state);
    if let Some(map) = body.as_object_mut() {
        map.insert(
            "node_uptime".to_string(),
            serde_json::Value::from(crate::v5::nodes::uptime_ms() / 1000),
        );
    }
    (StatusCode::OK, Json(body)).into_response()
}

/// Documented `GET /stats` snapshot: aggregated resource and session statistics.
///
/// Single node, management-plane only: copies the same gauge-plus-high-water-mark
/// store as `GET /nodes/{node}/stats` ([`stats_snapshot_value`], eight atomic loads)
/// and renders the documented stat names from that copy, so one read costs a bounded
/// copy of the gauge block and adds no work to the per-message path. All `set_*`
/// updates already happen at kernel lifecycle points; this read only copies.
/// Returned as a one-element array naming the local node, so the aggregated shape
/// carries the same documented names as the per-node read. Every stat value is
/// numeric and reflects live state.
/// Management-plane only; never touched on fan-out or fan-in.
pub async fn get_stats(State(state): State<ApiState>) -> Response {
    let mut snapshot = stats_snapshot_value(&state);
    if let Some(map) = snapshot.as_object_mut() {
        map.insert(
            "node".to_string(),
            serde_json::Value::from(local_node_name()),
        );
    }
    (StatusCode::OK, Json(vec![snapshot])).into_response()
}

/// Shared snapshot read for `GET /nodes/{node}/stats`.
///
/// Copies the gauge-plus-high-water-mark store once per request
/// ([`broker_observability::StatsStore::snapshot`], eight atomic loads)
/// and renders the documented per-node stat names from that copy, so one
/// read costs a bounded copy of the gauge block and adds no work to the
/// per-message path. All `set_*` updates already happen at kernel
/// lifecycle points; this read only copies.
///
/// Mapping follows the documented names: `connections`, `live_connections`,
/// `channels`, `sessions` and `cluster_sessions` all read the connection
/// gauge (single node: one channel and one session per bound connection);
/// `suboptions` reads the subscription gauge (one options record per
/// subscription); `subscribers` is bounded by both gauges (distinct
/// subscriber clients can be neither more than bound connections nor more
/// than subscription entries under active-only counting); `delayed` and
/// `subscriptions.shared` read zero (no kernel tracking yet, shape stays
/// stable and they rise once their subsystem records them).
/// Management-plane only; never touched on fan-out or fan-in.
fn stats_snapshot_value(state: &ApiState) -> serde_json::Value {
    let snap = state.stats.snapshot();
    let conns = snap.connections;
    let conns_max = snap.connections_max;
    let subs = snap.subscriptions;
    let subs_max = snap.subscriptions_max;
    let topics = snap.topics;
    let topics_max = snap.topics_max;
    let retained = snap.retained;
    let retained_max = snap.retained_max;
    let subscribers = conns.min(subs);
    let subscribers_max = conns_max.min(subs_max);
    // Built as an explicit map instead of one large `json!` literal: the
    // macro recurses once per key and trips the compiler recursion limit
    // at this size, while a map insert per gauge stays flat.
    let mut body = serde_json::Map::with_capacity(24);
    body.insert("channels.count".to_string(), serde_json::Value::from(conns));
    body.insert(
        "channels.max".to_string(),
        serde_json::Value::from(conns_max),
    );
    body.insert(
        "connections.count".to_string(),
        serde_json::Value::from(conns),
    );
    body.insert(
        "connections.max".to_string(),
        serde_json::Value::from(conns_max),
    );
    body.insert("delayed.count".to_string(), serde_json::Value::from(0u64));
    body.insert("delayed.max".to_string(), serde_json::Value::from(0u64));
    body.insert(
        "live_connections.count".to_string(),
        serde_json::Value::from(conns),
    );
    body.insert(
        "live_connections.max".to_string(),
        serde_json::Value::from(conns_max),
    );
    body.insert(
        "cluster_sessions.count".to_string(),
        serde_json::Value::from(conns),
    );
    body.insert(
        "cluster_sessions.max".to_string(),
        serde_json::Value::from(conns_max),
    );
    body.insert(
        "retained.count".to_string(),
        serde_json::Value::from(retained),
    );
    body.insert(
        "retained.max".to_string(),
        serde_json::Value::from(retained_max),
    );
    body.insert("sessions.count".to_string(), serde_json::Value::from(conns));
    body.insert(
        "sessions.max".to_string(),
        serde_json::Value::from(conns_max),
    );
    body.insert(
        "suboptions.count".to_string(),
        serde_json::Value::from(subs),
    );
    body.insert(
        "suboptions.max".to_string(),
        serde_json::Value::from(subs_max),
    );
    body.insert(
        "subscribers.count".to_string(),
        serde_json::Value::from(subscribers),
    );
    body.insert(
        "subscribers.max".to_string(),
        serde_json::Value::from(subscribers_max),
    );
    body.insert(
        "subscriptions.count".to_string(),
        serde_json::Value::from(subs),
    );
    body.insert(
        "subscriptions.max".to_string(),
        serde_json::Value::from(subs_max),
    );
    body.insert(
        "subscriptions.shared.count".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "subscriptions.shared.max".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert("topics.count".to_string(), serde_json::Value::from(topics));
    body.insert(
        "topics.max".to_string(),
        serde_json::Value::from(topics_max),
    );
    serde_json::Value::Object(body)
}

/// `GET /nodes/{node}/stats`: node-scoped read of the documented
/// resource and session statistics.
///
/// Single node, management-plane only: the named node is checked with the
/// shared [`resolve_node`] helper against the configured [`ApiState::node_id`]
/// (falling back to the [`local_node_name`] name the v5 rows render
/// today); an unknown name returns the helper's 404 `NOT_FOUND` shape
/// naming the node. A known name returns the single-object snapshot from
/// [`stats_snapshot_value`] over the shared gauge-plus-high-water-mark
/// store (one constant-time copy per request, kept exact by the kernel
/// lifecycle points), with every value numeric and no work on the
/// per-message path.
pub async fn get_stats_node(State(state): State<ApiState>, Path(node): Path<String>) -> Response {
    if resolve_node(&state.node_id, &node).is_err()
        && resolve_node(local_node_name(), &node).is_err()
    {
        // Reuse the helper's error so the unknown-node shape stays exactly
        // the documented `{code: NOT_FOUND, message}` body.
        return resolve_node(&state.node_id, &node)
            .expect_err("node already checked as unknown")
            .into_response();
    }
    (StatusCode::OK, Json(stats_snapshot_value(&state))).into_response()
}

/// Shared snapshot read for `GET /metrics` and its node-scoped twin below.
///
/// Builds the documented flat object of named counters: every value is
/// numeric and counters only move forward within a run. Each field reads
/// a lock-free atomic (or a saturating sum of atomics) copied once per
/// request through the shared snapshot, so one scrape costs a bounded
/// copy of the counter block and adds no work to the per-message path.
/// All increments already happen on the message path; this read only copies.
///
/// Grouping follows the documented names: `bytes`, `packets`, `messages`,
/// `delivery`, `client` and `session`. Fields with no kernel tracking yet
/// (QoS 2 acknowledgement flows, unsubscribe/disconnect packet splits,
/// delayed/retained message stores, session lifecycle) read zero so the
/// shape stays stable; they rise once their subsystem records them.
/// Management-plane only; never touched on fan-out or fan-in.
fn metrics_snapshot_value(state: &ApiState) -> serde_json::Value {
    let snap = state.metrics.snapshot();
    let packets_received = snap
        .connect_received
        .saturating_add(snap.publish_received)
        .saturating_add(snap.subscribe_received)
        .saturating_add(snap.pingreq_received);
    let packets_sent = snap
        .connack_sent
        .saturating_add(snap.publish_sent)
        .saturating_add(snap.suback_sent)
        .saturating_add(snap.pingresp_sent);
    let delivery_dropped = snap
        .overload_dropped
        .saturating_add(snap.unknown_conn_dropped)
        .saturating_add(snap.dead_mailbox_dropped)
        .saturating_add(snap.detached_clean_dropped)
        .saturating_add(snap.offline_queue_evicted)
        .saturating_add(snap.inflight_dropped)
        .saturating_add(snap.egress_qos0_shed);
    let delivery_queue_full = snap
        .overload_dropped
        .saturating_add(snap.offline_queue_evicted)
        .saturating_add(snap.inflight_dropped);
    // Built as an explicit map instead of one large `json!` literal: the
    // macro recurses once per key and trips the compiler recursion limit
    // at this size, while a map insert per counter stays flat.
    let mut body = serde_json::Map::with_capacity(57);
    body.insert(
        "bytes.received".to_string(),
        serde_json::Value::from(snap.bytes_received),
    );
    body.insert(
        "bytes.sent".to_string(),
        serde_json::Value::from(snap.bytes_sent),
    );
    body.insert(
        "packets.received".to_string(),
        serde_json::Value::from(packets_received),
    );
    body.insert(
        "packets.sent".to_string(),
        serde_json::Value::from(packets_sent),
    );
    body.insert(
        "packets.connect.received".to_string(),
        serde_json::Value::from(snap.connect_received),
    );
    body.insert(
        "packets.connack.sent".to_string(),
        serde_json::Value::from(snap.connack_sent),
    );
    body.insert(
        "packets.connack.error".to_string(),
        serde_json::Value::from(snap.auth_failures),
    );
    body.insert(
        "packets.connack.auth_error".to_string(),
        serde_json::Value::from(snap.auth_failures),
    );
    body.insert(
        "packets.publish.received".to_string(),
        serde_json::Value::from(snap.publish_received),
    );
    body.insert(
        "packets.publish.sent".to_string(),
        serde_json::Value::from(snap.publish_sent),
    );
    body.insert(
        "packets.publish.error".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "packets.publish.auth_error".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "packets.puback.received".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "packets.puback.sent".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "packets.pubrec.received".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "packets.pubrec.sent".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "packets.pubrel.received".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "packets.pubrel.sent".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "packets.pubcomp.received".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "packets.pubcomp.sent".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "packets.subscribe.received".to_string(),
        serde_json::Value::from(snap.subscribe_received),
    );
    body.insert(
        "packets.suback.sent".to_string(),
        serde_json::Value::from(snap.suback_sent),
    );
    body.insert(
        "packets.unsubscribe.received".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "packets.unsuback.sent".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "packets.pingreq.received".to_string(),
        serde_json::Value::from(snap.pingreq_received),
    );
    body.insert(
        "packets.pingresp.sent".to_string(),
        serde_json::Value::from(snap.pingresp_sent),
    );
    body.insert(
        "packets.disconnect.received".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "packets.disconnect.sent".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "messages.received".to_string(),
        serde_json::Value::from(snap.messages_received),
    );
    body.insert(
        "messages.sent".to_string(),
        serde_json::Value::from(snap.delivered),
    );
    body.insert(
        "messages.dropped".to_string(),
        serde_json::Value::from(snap.messages_dropped),
    );
    body.insert(
        "messages.delivered".to_string(),
        serde_json::Value::from(snap.delivered),
    );
    body.insert("messages.acked".to_string(), serde_json::Value::from(0u64));
    body.insert(
        "messages.retained".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "messages.delayed".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "messages.qos0.received".to_string(),
        serde_json::Value::from(snap.qos0_received),
    );
    body.insert(
        "messages.qos1.received".to_string(),
        serde_json::Value::from(snap.qos1_received),
    );
    body.insert(
        "messages.qos2.received".to_string(),
        serde_json::Value::from(snap.qos2_received),
    );
    body.insert(
        "messages.qos0.sent".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "messages.qos1.sent".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "messages.qos2.sent".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "delivery.dropped".to_string(),
        serde_json::Value::from(delivery_dropped),
    );
    body.insert(
        "delivery.dropped.no_subscribers".to_string(),
        serde_json::Value::from(snap.detached_clean_dropped),
    );
    body.insert(
        "delivery.dropped.qos0_msg".to_string(),
        serde_json::Value::from(snap.egress_qos0_shed),
    );
    body.insert(
        "delivery.dropped.queue_full".to_string(),
        serde_json::Value::from(delivery_queue_full),
    );
    body.insert(
        "delivery.dropped.expired".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "client.connect".to_string(),
        serde_json::Value::from(snap.connect_received),
    );
    body.insert(
        "client.connack".to_string(),
        serde_json::Value::from(snap.connack_sent),
    );
    body.insert(
        "client.connected".to_string(),
        serde_json::Value::from(snap.connect_received),
    );
    body.insert(
        "client.disconnected".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "client.subscribe".to_string(),
        serde_json::Value::from(snap.subscribe_received),
    );
    body.insert(
        "client.unsubscribe".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert("session.created".to_string(), serde_json::Value::from(0u64));
    body.insert(
        "session.discarded".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert("session.resumed".to_string(), serde_json::Value::from(0u64));
    body.insert(
        "session.takenover".to_string(),
        serde_json::Value::from(0u64),
    );
    body.insert(
        "session.terminated".to_string(),
        serde_json::Value::from(0u64),
    );
    serde_json::Value::Object(body)
}

/// Documented `GET /metrics` snapshot: a flat object of named counters.
///
/// Every value is numeric and counters only move forward within a run:
/// one scrape costs a bounded copy of the counter block through the
/// shared snapshot and adds no work to the per-message path.
/// Management-plane only; never touched on fan-out or fan-in.
pub async fn get_metrics(State(state): State<ApiState>) -> Response {
    (StatusCode::OK, Json(metrics_snapshot_value(&state))).into_response()
}

/// `GET /nodes/{node}/metrics`: node-scoped read of the same snapshot
/// as `GET /metrics`.
///
/// Single node, management-plane only: the named node is checked with the
/// shared [`resolve_node`] helper against the configured [`ApiState::node_id`]
/// (falling back to the [`local_node_name`] name the v5 rows render
/// today); an unknown name returns the helper's 404 `NOT_FOUND` shape
/// naming the node. A known name returns the same flat object of named
/// numeric counters as [`get_metrics`] (same documented names, same
/// monotonic counters), with one bounded counter-block copy per request
/// and no work on the per-message path.
pub async fn get_metrics_node(State(state): State<ApiState>, Path(node): Path<String>) -> Response {
    if resolve_node(&state.node_id, &node).is_err()
        && resolve_node(local_node_name(), &node).is_err()
    {
        // Reuse the helper's error so the unknown-node shape stays exactly
        // the documented `{code: NOT_FOUND, message}` body.
        return resolve_node(&state.node_id, &node)
            .expect_err("node already checked as unknown")
            .into_response();
    }
    (StatusCode::OK, Json(metrics_snapshot_value(&state))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ApiState;

    fn standalone_state() -> ApiState {
        let engine = std::sync::Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        ApiState::standalone(engine)
    }

    async fn snapshot_body(response: Response) -> (StatusCode, serde_json::Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 256 * 1024)
            .await
            .expect("current snapshot body is small and readable");
        let body: serde_json::Value =
            serde_json::from_slice(&bytes).expect("current snapshot body is JSON");
        (status, body)
    }

    fn documented_fields() -> [&'static str; 19] {
        [
            "connections",
            "live_connections",
            "topics",
            "subscriptions",
            "cluster_sessions",
            "retained_msg_count",
            "subscriptions_durable",
            "disconnected_durable_sessions",
            "received_msg_rate",
            "sent_msg_rate",
            "dropped_msg_rate",
            "rules_matched_rate",
            "persisted_rate",
            "actions_executed_rate",
            "actions_messages_rate",
            "validation_succeeded_rate",
            "validation_failed_rate",
            "transformation_succeeded_rate",
            "transformation_failed_rate",
        ]
    }

    fn point(time_stamp: u64) -> crate::v5::monitor::MonitorSample {
        crate::v5::monitor::MonitorSample {
            time_stamp,
            connections: 0,
            live_connections: 0,
            topics: 0,
            subscriptions: 0,
            subscriptions_durable: 4,
            disconnected_durable_sessions: 5,
            received: 0,
            sent: 0,
            dropped: 0,
            persisted: 0,
            validation_succeeded: 0,
            validation_failed: 0,
            transformation_succeeded: 0,
            transformation_failed: 0,
            rules_matched: 0,
            actions_executed: 0,
            actions_messages: 0,
        }
    }

    #[tokio::test]
    async fn current_snapshot_has_documented_numeric_fields() {
        let state = standalone_state();
        state.metrics.set_active_connections(1);

        let (status, body) = snapshot_body(monitor_current(State(state)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            body.is_object(),
            "snapshot must be a single object, got: {body}"
        );
        for field in documented_fields() {
            assert!(body.get(field).is_some(), "missing field {field}: {body}");
            assert!(
                body[field].is_u64(),
                "field {field} must be numeric, got: {body}"
            );
        }
        assert_eq!(body["connections"], serde_json::json!(1));
        // The cumulative counters are not part of the documented shape.
        for field in ["received", "sent", "dropped", "time_stamp"] {
            assert!(body.get(field).is_none(), "unexpected field {field}");
        }
    }

    #[tokio::test]
    async fn rates_are_zero_before_two_points_exist() {
        let state = standalone_state();
        state.metrics.inc_messages_received();
        let (_, empty) = snapshot_body(monitor_current(State(state.clone())).await).await;
        assert_eq!(empty["received_msg_rate"], serde_json::json!(0));
        state.monitor.record(point(1_700_000_100));
        let (_, one) = snapshot_body(monitor_current(State(state)).await).await;
        assert_eq!(one["received_msg_rate"], serde_json::json!(0));
    }

    #[tokio::test]
    async fn rates_follow_the_newest_sampling_window() {
        let state = standalone_state();
        let mut older = point(1_700_000_100);
        older.received = 100;
        older.sent = 200;
        older.dropped = 5;
        older.rules_matched = 10;
        let mut newer = point(1_700_000_110);
        newer.received = 600;
        newer.sent = 1200;
        newer.dropped = 5;
        newer.rules_matched = 40;
        newer.persisted = 20;
        state.monitor.record(older);
        state.monitor.record(newer);

        let (status, body) = snapshot_body(monitor_current(State(state)).await).await;
        assert_eq!(status, StatusCode::OK);
        // 500 messages in 10 seconds.
        assert_eq!(body["received_msg_rate"], serde_json::json!(50));
        assert_eq!(body["sent_msg_rate"], serde_json::json!(100));
        assert_eq!(body["dropped_msg_rate"], serde_json::json!(0));
        assert_eq!(body["rules_matched_rate"], serde_json::json!(3));
        assert_eq!(body["persisted_rate"], serde_json::json!(2));
        // Gauges with no kernel tracking carry over from the newest point.
        assert_eq!(body["subscriptions_durable"], serde_json::json!(4));
        assert_eq!(body["disconnected_durable_sessions"], serde_json::json!(5));
    }

    #[tokio::test]
    async fn node_snapshot_returns_same_shape_as_global() {
        let state = standalone_state();
        state.metrics.set_active_connections(1);
        let mut older = point(1_700_000_100);
        older.received = 10;
        let mut newer = point(1_700_000_102);
        newer.received = 30;
        state.monitor.record(older);
        state.monitor.record(newer);
        let (_, global) = snapshot_body(monitor_current(State(state.clone())).await).await;
        for known in [local_node_name(), "indra-node-1"] {
            let response =
                monitor_current_node(State(state.clone()), Path(known.to_string())).await;
            let (status, body) = snapshot_body(response).await;
            assert_eq!(status, StatusCode::OK, "known node {known}");
            assert!(
                body.is_object(),
                "node snapshot must be a single object, got: {body}"
            );
            for field in documented_fields() {
                assert!(body.get(field).is_some(), "missing field {field}: {body}");
                assert!(
                    body[field].is_u64(),
                    "field {field} must be numeric, got: {body}"
                );
            }
            assert!(body["node_uptime"].is_u64(), "node route adds node_uptime");
            assert_eq!(body["received_msg_rate"], serde_json::json!(10));
            for field in documented_fields() {
                assert_eq!(body[field], global[field], "field {field}");
            }
        }
    }

    #[tokio::test]
    async fn node_snapshot_unknown_node_is_not_found() {
        let state = standalone_state();
        state.metrics.inc_messages_received();
        let response = monitor_current_node(State(state), Path("no-such-node".to_string())).await;
        let (status, body) = snapshot_body(response).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
        let message = body["message"].as_str().expect("message is a string");
        assert!(
            message.contains("no-such-node"),
            "message names the unknown node: {body}"
        );
    }

    fn documented_metric_names() -> [&'static str; 28] {
        [
            "bytes.received",
            "bytes.sent",
            "packets.received",
            "packets.sent",
            "packets.connect.received",
            "packets.connack.sent",
            "packets.connack.error",
            "packets.connack.auth_error",
            "packets.publish.received",
            "packets.publish.sent",
            "packets.subscribe.received",
            "packets.suback.sent",
            "packets.pingreq.received",
            "packets.pingresp.sent",
            "messages.received",
            "messages.sent",
            "messages.dropped",
            "messages.delivered",
            "messages.qos0.received",
            "messages.qos1.received",
            "messages.qos2.received",
            "delivery.dropped",
            "delivery.dropped.no_subscribers",
            "delivery.dropped.queue_full",
            "client.connect",
            "client.connack",
            "client.connected",
            "client.subscribe",
        ]
    }

    #[tokio::test]
    async fn node_metrics_match_global_names_and_stay_numeric() {
        let state = standalone_state();
        state.metrics.inc_messages_received();
        state.metrics.inc_messages_forwarded();
        state.metrics.inc_messages_dropped();
        let (_, global) = snapshot_body(get_metrics(State(state.clone())).await).await;
        let global_map = global.as_object().expect("global metrics is a flat object");
        for known in [local_node_name(), "indra-node-1"] {
            let response = get_metrics_node(State(state.clone()), Path(known.to_string())).await;
            let (status, body) = snapshot_body(response).await;
            assert_eq!(status, StatusCode::OK, "known node {known}");
            let node_map = body.as_object().expect("node metrics is a flat object");
            assert!(
                body.get("data").is_none(),
                "node metrics is not paged: {body}"
            );
            for name in documented_metric_names() {
                let value = node_map
                    .get(name)
                    .unwrap_or_else(|| panic!("missing {name} for node {known}"));
                assert!(value.is_number(), "{name} is numeric: {value:?}");
                assert_eq!(
                    value, &global_map[name],
                    "{name} matches the global read for node {known}"
                );
            }
        }
    }

    #[tokio::test]
    async fn node_metrics_unknown_node_is_not_found() {
        let state = standalone_state();
        state.metrics.inc_messages_received();
        let response = get_metrics_node(State(state), Path("no-such-node".to_string())).await;
        let (status, body) = snapshot_body(response).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
        let message = body["message"].as_str().expect("message is a string");
        assert!(
            message.contains("no-such-node"),
            "message names the unknown node: {body}"
        );
    }

    fn documented_stat_names() -> [&'static str; 24] {
        [
            "channels.count",
            "channels.max",
            "connections.count",
            "connections.max",
            "delayed.count",
            "delayed.max",
            "live_connections.count",
            "live_connections.max",
            "cluster_sessions.count",
            "cluster_sessions.max",
            "retained.count",
            "retained.max",
            "sessions.count",
            "sessions.max",
            "suboptions.count",
            "suboptions.max",
            "subscribers.count",
            "subscribers.max",
            "subscriptions.count",
            "subscriptions.max",
            "subscriptions.shared.count",
            "subscriptions.shared.max",
            "topics.count",
            "topics.max",
        ]
    }

    #[tokio::test]
    async fn node_stats_returns_documented_numeric_fields_from_store() {
        let state = standalone_state();
        state.stats.set_connections(3);
        state.stats.set_subscriptions(7);
        state.stats.set_topics(2);
        state.stats.set_retained(1);
        for known in [local_node_name(), "indra-node-1"] {
            let response = get_stats_node(State(state.clone()), Path(known.to_string())).await;
            let (status, body) = snapshot_body(response).await;
            assert_eq!(status, StatusCode::OK, "known node {known}");
            assert!(
                body.is_object(),
                "node stats must be a single object, got: {body}"
            );
            assert!(
                body.get("data").is_none(),
                "node stats is not paged: {body}"
            );
            for name in documented_stat_names() {
                let value = body
                    .get(name)
                    .unwrap_or_else(|| panic!("missing {name} for node {known}"));
                assert!(value.is_number(), "{name} is numeric: {value:?}");
            }
            // Tracked gauges read the store snapshot exactly.
            assert_eq!(body["connections.count"], serde_json::json!(3));
            assert_eq!(body["live_connections.count"], serde_json::json!(3));
            assert_eq!(body["channels.count"], serde_json::json!(3));
            assert_eq!(body["sessions.count"], serde_json::json!(3));
            assert_eq!(body["cluster_sessions.count"], serde_json::json!(3));
            assert_eq!(body["subscriptions.count"], serde_json::json!(7));
            assert_eq!(body["suboptions.count"], serde_json::json!(7));
            assert_eq!(body["topics.count"], serde_json::json!(2));
            assert_eq!(body["retained.count"], serde_json::json!(1));
            assert_eq!(body["subscriptions.shared.count"], serde_json::json!(0));
            assert_eq!(body["delayed.count"], serde_json::json!(0));
            // High-water marks come from the store and never sit below
            // their gauge.
            for (count, max) in [
                ("connections.count", "connections.max"),
                ("live_connections.count", "live_connections.max"),
                ("subscriptions.count", "subscriptions.max"),
                ("topics.count", "topics.max"),
                ("retained.count", "retained.max"),
            ] {
                let current = body[count].as_u64().expect("count numeric");
                let peak = body[max].as_u64().expect("max numeric");
                assert!(
                    peak >= current,
                    "{max} ({peak}) must cover {count} ({current}): {body}"
                );
            }
        }
    }

    #[tokio::test]
    async fn node_stats_maxima_come_from_store_and_never_fall() {
        let state = standalone_state();
        state.stats.set_connections(5);
        state.stats.set_subscriptions(7);
        state.stats.set_topics(3);
        state.stats.set_retained(2);
        let response = get_stats_node(State(state.clone()), Path("indra-node-1".to_string())).await;
        let (_, peak) = snapshot_body(response).await;
        assert_eq!(peak["connections.max"], serde_json::json!(5));
        assert_eq!(peak["subscriptions.max"], serde_json::json!(7));
        // Gauges fall; maxima stay at the true peak from the store.
        state.stats.set_connections(2);
        state.stats.set_subscriptions(0);
        state.stats.set_topics(1);
        state.stats.set_retained(0);
        let response = get_stats_node(State(state), Path("indra-node-1".to_string())).await;
        let (status, body) = snapshot_body(response).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["connections.count"], serde_json::json!(2));
        assert_eq!(body["connections.max"], serde_json::json!(5));
        assert_eq!(body["subscriptions.count"], serde_json::json!(0));
        assert_eq!(body["subscriptions.max"], serde_json::json!(7));
        assert_eq!(body["topics.count"], serde_json::json!(1));
        assert_eq!(body["topics.max"], serde_json::json!(3));
        assert_eq!(body["retained.count"], serde_json::json!(0));
        assert_eq!(body["retained.max"], serde_json::json!(2));
    }

    #[tokio::test]
    async fn node_stats_unknown_node_is_not_found() {
        let state = standalone_state();
        state.stats.set_connections(1);
        let response = get_stats_node(State(state), Path("no-such-node".to_string())).await;
        let (status, body) = snapshot_body(response).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
        let message = body["message"].as_str().expect("message is a string");
        assert!(
            message.contains("no-such-node"),
            "message names the unknown node: {body}"
        );
    }

    #[tokio::test]
    async fn global_stats_returns_aggregated_numeric_fields_from_store() {
        let state = standalone_state();
        state.stats.set_connections(2);
        state.stats.set_subscriptions(4);
        state.stats.set_topics(3);
        state.stats.set_retained(1);
        let (status, body) = snapshot_body(get_stats(State(state)).await).await;
        assert_eq!(status, StatusCode::OK);
        let rows = body.as_array().expect("global stats is an array");
        assert_eq!(rows.len(), 1, "aggregated snapshot has one row: {body}");
        let row = &rows[0];
        assert_eq!(row["node"], serde_json::json!(local_node_name()));
        for name in documented_stat_names() {
            let value = row
                .get(name)
                .unwrap_or_else(|| panic!("missing {name}: {body}"));
            assert!(value.is_number(), "{name} is numeric: {value:?}");
        }
        // Session-related fields reflect the live store, not a fresh scan.
        assert_eq!(row["connections.count"], serde_json::json!(2));
        assert_eq!(row["live_connections.count"], serde_json::json!(2));
        assert_eq!(row["channels.count"], serde_json::json!(2));
        assert_eq!(row["sessions.count"], serde_json::json!(2));
        assert_eq!(row["cluster_sessions.count"], serde_json::json!(2));
        assert!(row["sessions.count"].as_u64().expect("sessions") >= 1);
        assert_eq!(row["subscriptions.count"], serde_json::json!(4));
        assert_eq!(row["suboptions.count"], serde_json::json!(4));
        assert_eq!(row["topics.count"], serde_json::json!(3));
        assert_eq!(row["retained.count"], serde_json::json!(1));
        assert_eq!(row["subscriptions.shared.count"], serde_json::json!(0));
        assert_eq!(row["delayed.count"], serde_json::json!(0));
        for (count, max) in [
            ("connections.count", "connections.max"),
            ("subscriptions.count", "subscriptions.max"),
            ("topics.count", "topics.max"),
            ("retained.count", "retained.max"),
        ] {
            let current = row[count].as_u64().expect("count numeric");
            let peak = row[max].as_u64().expect("max numeric");
            assert!(
                peak >= current,
                "{max} ({peak}) must cover {count} ({current}): {body}"
            );
        }
    }

    #[tokio::test]
    async fn global_stats_matches_node_stats_snapshot() {
        let state = standalone_state();
        state.stats.set_connections(3);
        state.stats.set_subscriptions(7);
        state.stats.set_topics(2);
        state.stats.set_retained(1);
        let (_, global) = snapshot_body(get_stats(State(state.clone())).await).await;
        let row = &global.as_array().expect("global stats is an array")[0];
        let (_, node) =
            snapshot_body(get_stats_node(State(state), Path(local_node_name().to_string())).await)
                .await;
        for name in documented_stat_names() {
            assert_eq!(&row[name], &node[name], "{name} matches the node read");
        }
    }

    #[tokio::test]
    async fn global_stats_maxima_come_from_store_and_never_fall() {
        let state = standalone_state();
        state.stats.set_connections(5);
        state.stats.set_subscriptions(7);
        state.stats.set_topics(3);
        state.stats.set_retained(2);
        let (_, first) = snapshot_body(get_stats(State(state.clone())).await).await;
        let peak = &first.as_array().expect("global stats is an array")[0];
        assert_eq!(peak["connections.max"], serde_json::json!(5));
        assert_eq!(peak["subscriptions.max"], serde_json::json!(7));
        // Gauges fall; maxima stay at the true peak from the store.
        state.stats.set_connections(2);
        state.stats.set_subscriptions(0);
        state.stats.set_topics(1);
        state.stats.set_retained(0);
        let (status, body) = snapshot_body(get_stats(State(state)).await).await;
        assert_eq!(status, StatusCode::OK);
        let row = &body.as_array().expect("global stats is an array")[0];
        assert_eq!(row["connections.count"], serde_json::json!(2));
        assert_eq!(row["connections.max"], serde_json::json!(5));
        assert_eq!(row["subscriptions.count"], serde_json::json!(0));
        assert_eq!(row["subscriptions.max"], serde_json::json!(7));
        assert_eq!(row["topics.count"], serde_json::json!(1));
        assert_eq!(row["topics.max"], serde_json::json!(3));
        assert_eq!(row["retained.count"], serde_json::json!(0));
        assert_eq!(row["retained.max"], serde_json::json!(2));
    }
}
