//! Sampled monitor history for the v5 management API.
//!
//! Covers `GET /monitor` (ordered history list), `DELETE /monitor`
//! (clear the history) and `GET /monitor/nodes/{node}` (same history
//! scoped to the one local node via the shared node helper).
//! Single-node, management-plane only: nothing here
//! runs on the per-message path, so reads and clears never take a
//! delivery lock and no new buffering is added to fan-out or fan-in.
//!
//! Store bounds (both stated here and enforced below):
//! - at most [`MAX_MONITOR_SAMPLES`] points are kept in a fixed
//!   ring (`VecDeque`); recording past the cap evicts the oldest point
//!   instead of growing without limit;
//! - every point is a fixed set of integer counters plus `time_stamp`
//!   (Unix seconds), so per-point memory is constant;
//! - recorder writes are constant-time (one push plus at most one pop);
//!   listing copies at most the capped buffer once per request.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use std::collections::VecDeque;
use std::sync::RwLock;

use crate::node_scope::resolve_node;
use crate::ApiState;

use crate::node_scope::local_node_name;

/// Upper bound for stored history points. Recording past this size evicts
/// the oldest point first instead of growing the buffer without limit.
/// 1440 points cover four hours at one sample per ten seconds; one point
/// holds nine integers, so the buffer stays well under one megabyte.
pub const MAX_MONITOR_SAMPLES: usize = 1440;

/// One sampled history point: owning tenant (`tenant`), the sampling
/// instant (`time_stamp`, Unix seconds) plus gauge and cumulative
/// counters as plain integers.
///
/// Gauges (`connections`, `live_connections`, `topics`, `subscriptions`)
/// describe the tenant at sampling time. Cumulative counters
/// (`received`, `sent`, `dropped`, `rules_matched`) only move forward
/// within a run. The struct holds only tracked counters; names without
/// kernel tracking have no field and stay out of the shape, never zero.
/// Omitted: `subscriptions_durable`, `disconnected_durable_sessions`,
/// `persisted`, `validation_*`, `transformation_*`, `actions_*`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MonitorSample {
    pub time_stamp: u64,
    pub connections: u64,
    pub live_connections: u64,
    pub topics: u64,
    pub subscriptions: u64,
    pub received: u64,
    pub sent: u64,
    pub dropped: u64,
    pub rules_matched: u64,
    pub tenant: String,
}

/// Live counter values captured into one history point. Gauges come from
/// the session directory; cumulative counters come from the lock-free
/// metrics snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LiveCounters {
    pub connections: u64,
    pub live_connections: u64,
    pub topics: u64,
    pub subscriptions: u64,
    pub received: u64,
    pub sent: u64,
    pub dropped: u64,
    pub rules_matched: u64,
}

impl MonitorSample {
    /// Build a point from live counters at `time_stamp`. Copy each
    /// tracked counter. The point belongs to the default tenant; the
    /// sampler records the default-tenant view only.
    pub fn from_live(time_stamp: u64, counters: LiveCounters) -> Self {
        Self::from_live_in_tenant(
            time_stamp,
            counters,
            broker_session::tenant::DEFAULT_TENANT_ID,
        )
    }

    /// Build a point from live counters at `time_stamp` in `tenant`.
    /// Copy each tracked counter. Names without kernel tracking have
    /// no field and stay out of the shape.
    pub fn from_live_in_tenant(time_stamp: u64, counters: LiveCounters, tenant: &str) -> Self {
        Self {
            time_stamp,
            connections: counters.connections,
            live_connections: counters.live_connections,
            topics: counters.topics,
            subscriptions: counters.subscriptions,
            received: counters.received,
            sent: counters.sent,
            dropped: counters.dropped,
            rules_matched: counters.rules_matched,
            tenant: tenant.to_string(),
        }
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "time_stamp": self.time_stamp,
            "connections": self.connections,
            "live_connections": self.live_connections,
            "topics": self.topics,
            "subscriptions": self.subscriptions,
            "received": self.received,
            "sent": self.sent,
            "dropped": self.dropped,
            "rules_matched": self.rules_matched,
            "tenant": self.tenant,
        })
    }
}

/// Bounded history buffer behind one short lock.
///
/// Every method finishes quickly and no delivery path touches it, so
/// management reads never block messaging. Recording is constant-time;
/// listing copies at most [`MAX_MONITOR_SAMPLES`] points.
pub struct MonitorHistory {
    inner: RwLock<VecDeque<MonitorSample>>,
}

impl MonitorHistory {
    /// Empty history.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(VecDeque::new()),
        }
    }

    /// Record one point. Constant-time: a single push plus at most one
    /// pop of the oldest point when the buffer is full.
    pub fn record(&self, sample: MonitorSample) {
        let mut buffer = self.inner.write().expect("monitor store lock");
        if buffer.len() >= MAX_MONITOR_SAMPLES {
            buffer.pop_front();
        }
        buffer.push_back(sample);
    }

    /// Capture live counters into one point and record it. Constant-time,
    /// like [`Self::record`]. Returns the stored point. Records the
    /// default-tenant view; other tenants keep their own points once
    /// per-tenant sampling lands.
    pub fn capture_live(&self, time_stamp: u64, counters: LiveCounters) -> MonitorSample {
        let sample = MonitorSample::from_live(time_stamp, counters);
        self.record(sample.clone());
        sample
    }

    /// Capture live counters for `tenant` and record the point.
    pub fn capture_live_in_tenant(
        &self,
        time_stamp: u64,
        counters: LiveCounters,
        tenant: &str,
    ) -> MonitorSample {
        let sample = MonitorSample::from_live_in_tenant(time_stamp, counters, tenant);
        self.record(sample.clone());
        sample
    }

    /// Snapshot of stored points in ascending `time_stamp` order
    /// (oldest first). An empty store reads as an empty list. When
    /// `latest_secs` is set, only points sampled within the last
    /// `latest_secs` seconds (`time_stamp >= now - latest_secs`) are
    /// returned; otherwise every stored point is returned.
    pub fn list(&self, latest_secs: Option<u64>, now_secs: u64) -> Vec<MonitorSample> {
        let buffer = self.inner.read().expect("monitor store lock");
        let mut out: Vec<MonitorSample> = match latest_secs {
            Some(window) => {
                let cutoff = now_secs.saturating_sub(window);
                buffer
                    .iter()
                    .filter(|s| s.time_stamp >= cutoff)
                    .cloned()
                    .collect()
            }
            None => buffer.iter().cloned().collect(),
        };
        out.sort_by_key(|s| s.time_stamp);
        out
    }

    /// Snapshot of stored points of one tenant in ascending
    /// `time_stamp` order (oldest first). Same window rule as
    /// [`Self::list`].
    pub fn list_in_tenant(
        &self,
        tenant: &str,
        latest_secs: Option<u64>,
        now_secs: u64,
    ) -> Vec<MonitorSample> {
        self.list(latest_secs, now_secs)
            .into_iter()
            .filter(|s| s.tenant == tenant)
            .collect()
    }

    /// Drop every stored point. Always succeeds, even when nothing is
    /// stored.
    pub fn clear(&self) {
        self.inner.write().expect("monitor store lock").clear();
    }

    /// Newest stored point, if any. Constant-time: one short lock and
    /// one clone of the back of the ring. Points are recorded in time
    /// order, so the back is the newest; an empty store returns None.
    /// Management-plane only; never touched on the per-message path.
    pub fn latest(&self) -> Option<MonitorSample> {
        self.inner
            .read()
            .expect("monitor store lock")
            .back()
            .cloned()
    }

    /// The two newest stored points, newest first. A missing point reads as
    /// `None`. Constant-time: one short read lock and two clones.
    /// Management-plane only; never touched on the per-message path.
    pub fn newest_two(&self) -> (Option<MonitorSample>, Option<MonitorSample>) {
        let buffer = self.inner.read().unwrap_or_else(|p| p.into_inner());
        let mut newest_first = buffer.iter().rev();
        (newest_first.next().cloned(), newest_first.next().cloned())
    }

    /// Number of stored points (never above [`MAX_MONITOR_SAMPLES`]).
    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.read().expect("monitor store lock").len()
    }
}

/// Events per second between two readings of one cumulative counter.
///
/// `secs` is the time between the readings. A zero `secs` counts as one
/// second, because the points carry stamps of one second. A counter that
/// went backwards (a restart of its owner) reads as no events. The result
/// is rounded to the nearest whole event per second.
pub fn rate_per_sec(newer: u64, older: u64, secs: u64) -> u64 {
    let secs = secs.max(1);
    let delta = newer.saturating_sub(older);
    delta.saturating_add(secs / 2) / secs
}

impl Default for MonitorHistory {
    fn default() -> Self {
        Self::new()
    }
}

/// Documented list scope. Only `latest` narrows the read (last N seconds);
/// every other query key is ignored by the extractor so new parameters
/// degrade to the full history instead of a 400. A malformed `latest`
/// also falls back to the full history.
#[derive(Deserialize, Default)]
pub struct MonitorListQuery {
    #[serde(default)]
    pub latest: Option<String>,
}

fn parse_latest(query: &MonitorListQuery) -> Option<u64> {
    query
        .latest
        .as_deref()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
}

/// `GET /monitor`: ordered history array (oldest first). An empty store
/// reads as an empty array, never an error. `?latest=N` keeps only points
/// sampled within the last N seconds (`N` is at least 1; zero fails with
/// 400 `BAD_RPC`); unknown query keys are ignored and a malformed
/// `latest` falls back to the full history. Management-plane only: one
/// short store lock, no work on the per-message path.
pub async fn list_monitor(
    State(state): State<ApiState>,
    Query(query): Query<MonitorListQuery>,
) -> Response {
    if let Some(raw) = query.latest.as_deref() {
        if let Ok(value) = raw.trim().parse::<u64>() {
            if value == 0 {
                return crate::errors::ApiError::BadRpc(
                    "query parameter `latest` must be at least 1".to_string(),
                )
                .into_response();
            }
        }
    }
    let latest = parse_latest(&query);
    let now = now_epoch_secs();
    let rows = state.monitor.list(latest, now);
    let data: Vec<serde_json::Value> = rows.iter().map(MonitorSample::to_json).collect();
    (StatusCode::OK, Json(data)).into_response()
}

/// `DELETE /monitor`: drop every history point and report success with
/// 204. Always succeeds, even when nothing is stored. Query strings
/// (including unknown keys) are ignored by design.
pub async fn clear_monitor(State(state): State<ApiState>) -> Response {
    state.monitor.clear();
    StatusCode::NO_CONTENT.into_response()
}

/// `GET /monitor/nodes/{node}`: node-scoped read over the same bounded
/// history as `GET /monitor`.
///
/// Single node, management-plane only: the named node is checked with the
/// shared [`resolve_node`] helper against the configured [`ApiState::node_id`]
/// (falling back to the [`local_node_name`] name the v5 rows render
/// today); an unknown name returns the helper's 404 `NOT_FOUND` shape
/// naming the node. A known name returns the same ordered array shape as
/// [`list_monitor`] (same `?latest=N` window, same unknown-query-keys
/// ignored, same empty-store empty array), with one short store lock and
/// no work on the per-message path.
pub async fn get_monitor_node(
    State(state): State<ApiState>,
    Path(node): Path<String>,
    Query(query): Query<MonitorListQuery>,
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
    if let Some(raw) = query.latest.as_deref() {
        if let Ok(value) = raw.trim().parse::<u64>() {
            if value == 0 {
                return crate::errors::ApiError::BadRpc(
                    "query parameter `latest` must be at least 1".to_string(),
                )
                .into_response();
            }
        }
    }
    let latest = parse_latest(&query);
    let now = now_epoch_secs();
    let rows = state.monitor.list(latest, now);
    let data: Vec<serde_json::Value> = rows.iter().map(MonitorSample::to_json).collect();
    (StatusCode::OK, Json(data)).into_response()
}

fn now_epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_per_sec_divides_by_the_window_and_rounds() {
        assert_eq!(rate_per_sec(600, 100, 10), 50);
        // 14 events in 10 s is 1.4 per second and rounds down.
        assert_eq!(rate_per_sec(14, 0, 10), 1);
        // 15 events in 10 s is 1.5 per second and rounds up.
        assert_eq!(rate_per_sec(15, 0, 10), 2);
        // A zero window counts as one second.
        assert_eq!(rate_per_sec(7, 2, 0), 5);
        // A counter that went backwards reads as no events.
        assert_eq!(rate_per_sec(3, 90, 10), 0);
    }

    #[test]
    fn newest_two_returns_the_last_points_newest_first() {
        let history = MonitorHistory::new();
        assert_eq!(history.newest_two(), (None, None));
        for stamp in [10u64, 20, 30] {
            history.capture_live(stamp, LiveCounters::default());
        }
        let (newest, previous) = history.newest_two();
        assert_eq!(newest.map(|s| s.time_stamp), Some(30));
        assert_eq!(previous.map(|s| s.time_stamp), Some(20));
    }

    fn sample_at(time_stamp: u64, connections: u64) -> MonitorSample {
        MonitorSample::from_live(
            time_stamp,
            LiveCounters {
                connections,
                live_connections: connections,
                topics: 1,
                subscriptions: 2,
                received: time_stamp,
                sent: time_stamp,
                dropped: 0,
                rules_matched: 0,
            },
        )
    }

    #[test]
    fn latest_returns_newest_or_none() {
        let store = MonitorHistory::new();
        assert!(store.latest().is_none());
        store.record(sample_at(1_700_000_100, 3));
        store.record(sample_at(1_700_000_200, 5));
        let newest = store.latest().expect("newest exists");
        assert_eq!(newest.time_stamp, 1_700_000_200);
        assert_eq!(newest.connections, 5);
    }

    #[test]
    fn empty_store_lists_nothing() {
        let store = MonitorHistory::new();
        assert!(store.list(None, 1_700_000_000).is_empty());
        assert!(store.list(Some(300), 1_700_000_000).is_empty());
    }

    #[test]
    fn record_two_samples_list_in_order_then_clear_empties() {
        let store = MonitorHistory::new();
        store.record(sample_at(1_700_000_100, 3));
        store.record(sample_at(1_700_000_200, 5));
        let rows = store.list(None, 1_700_000_300);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].time_stamp, 1_700_000_100);
        assert_eq!(rows[1].time_stamp, 1_700_000_200);
        assert_eq!(rows[0].connections, 3);
        assert_eq!(rows[1].connections, 5);
        store.clear();
        assert!(store.list(None, 1_700_000_300).is_empty());
        // Clearing an empty store still succeeds.
        store.clear();
        assert!(store.list(None, 1_700_000_300).is_empty());
    }

    #[test]
    fn cap_evicts_oldest_point() {
        let store = MonitorHistory::new();
        for i in 0..(MAX_MONITOR_SAMPLES + 5) {
            store.record(sample_at(1_700_000_000 + i as u64, i as u64));
        }
        assert_eq!(store.len(), MAX_MONITOR_SAMPLES);
        let rows = store.list(None, u64::MAX);
        assert_eq!(rows.len(), MAX_MONITOR_SAMPLES);
        // The five oldest points were evicted.
        assert_eq!(rows[0].time_stamp, 1_700_000_005);
    }

    #[test]
    fn latest_window_keeps_recent_points() {
        let store = MonitorHistory::new();
        store.record(sample_at(1_000, 1));
        store.record(sample_at(1_200, 2));
        store.record(sample_at(1_400, 3));
        let rows = store.list(Some(300), 1_500);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].time_stamp, 1_200);
        assert_eq!(rows[1].time_stamp, 1_400);
        let all = store.list(None, 1_500);
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn sample_json_has_documented_shape() {
        let sample = sample_at(1_700_000_100, 7);
        let rendered = sample.to_json();
        assert_eq!(rendered["time_stamp"], serde_json::json!(1_700_000_100));
        assert_eq!(rendered["connections"], serde_json::json!(7));
        assert_eq!(rendered["live_connections"], serde_json::json!(7));
        for field in [
            "topics",
            "subscriptions",
            "received",
            "sent",
            "dropped",
            "rules_matched",
        ] {
            assert!(rendered.get(field).is_some(), "missing field {field}");
            assert!(rendered[field].is_u64(), "field {field} must be an integer");
        }
        // Fields without kernel tracking stay out, never zero.
        for field in [
            "subscriptions_durable",
            "disconnected_durable_sessions",
            "persisted",
            "validation_succeeded",
            "validation_failed",
            "transformation_succeeded",
            "transformation_failed",
            "actions_executed",
            "actions_messages",
        ] {
            assert!(rendered.get(field).is_none(), "untracked field {field}");
        }
    }

    #[test]
    fn capture_live_is_constant_time_and_bounded() {
        let store = MonitorHistory::new();
        let counters = LiveCounters {
            connections: 2,
            live_connections: 2,
            topics: 1,
            subscriptions: 1,
            received: 10,
            sent: 9,
            dropped: 1,
            rules_matched: 4,
        };
        let stored = store.capture_live(1_700_000_100, counters);
        assert_eq!(stored.time_stamp, 1_700_000_100);
        assert_eq!(stored.connections, 2);
        assert_eq!(stored.received, 10);
        assert_eq!(stored.rules_matched, 4);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn samples_stay_in_their_own_tenant() {
        let store = MonitorHistory::new();
        let counters = LiveCounters {
            connections: 1,
            live_connections: 1,
            topics: 1,
            subscriptions: 1,
            received: 2,
            sent: 2,
            dropped: 0,
            rules_matched: 0,
        };
        store.capture_live(1_700_000_100, counters);
        store.capture_live_in_tenant(1_700_000_200, counters, "tenant-b");
        assert_eq!(store.list(None, 1_700_000_300).len(), 2);
        let scoped = store.list_in_tenant("tenant-b", None, 1_700_000_300);
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].tenant, "tenant-b");
    }

    fn standalone_state() -> ApiState {
        let engine = std::sync::Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        ApiState::standalone(engine)
    }

    async fn node_response_body(response: Response) -> (StatusCode, serde_json::Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 256 * 1024)
            .await
            .expect("monitor body is small and readable");
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("monitor body is JSON");
        (status, body)
    }

    #[tokio::test]
    async fn node_read_returns_same_shape_as_global() {
        let state = standalone_state();
        state.monitor.record(sample_at(1_700_000_100, 3));
        state.monitor.record(sample_at(1_700_000_200, 5));
        for known in [local_node_name(), "indra-node-1"] {
            let response = get_monitor_node(
                State(state.clone()),
                Path(known.to_string()),
                Query(MonitorListQuery { latest: None }),
            )
            .await;
            let (status, body) = node_response_body(response).await;
            assert_eq!(status, StatusCode::OK, "known node {known}");
            let rows = body.as_array().expect("node history is an array");
            assert_eq!(rows.len(), 2, "known node {known}");
            assert_eq!(rows[0]["time_stamp"], serde_json::json!(1_700_000_100));
            assert_eq!(rows[1]["time_stamp"], serde_json::json!(1_700_000_200));
            assert_eq!(rows[0]["connections"], serde_json::json!(3));
            assert_eq!(rows[1]["connections"], serde_json::json!(5));
            for row in rows {
                for field in [
                    "time_stamp",
                    "connections",
                    "live_connections",
                    "topics",
                    "subscriptions",
                    "received",
                    "sent",
                    "dropped",
                    "rules_matched",
                ] {
                    assert!(row.get(field).is_some(), "missing field {field}");
                    assert!(row[field].is_u64(), "field {field} must be an integer");
                }
                for field in [
                    "subscriptions_durable",
                    "disconnected_durable_sessions",
                    "persisted",
                    "validation_succeeded",
                    "validation_failed",
                    "transformation_succeeded",
                    "transformation_failed",
                    "actions_executed",
                    "actions_messages",
                ] {
                    assert!(row.get(field).is_none(), "untracked field {field}");
                }
            }
        }
    }

    #[tokio::test]
    async fn unknown_node_is_not_found() {
        let state = standalone_state();
        state.monitor.record(sample_at(1_700_000_100, 3));
        let response = get_monitor_node(
            State(state),
            Path("no-such-node".to_string()),
            Query(MonitorListQuery { latest: None }),
        )
        .await;
        let (status, body) = node_response_body(response).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
        let message = body["message"].as_str().expect("message is a string");
        assert!(
            message.contains("no-such-node"),
            "message names the unknown node: {body}"
        );
    }
}
