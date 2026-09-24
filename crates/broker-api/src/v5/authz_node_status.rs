//! Node-level authorization cache status for the v5 API.
//!
//! Covers `GET /authorization/node_cache/status` (C04-05) over real node
//! state: the enabled flag and entry cap from the live
//! [`broker_auth::AuthzNodeCache`] policy (W2-14 config), plus the live
//! entry count and cumulative hit/miss/insert counters rolled up across
//! every session's decision cache
//! (`crates/broker-session/src/lib.rs`,
//! `SessionManager::authz_node_stats`, whose entries the publish and
//! subscribe authorization events recorded). Single-node,
//! management-plane only: the handler clones one `Arc` per live session
//! under a single short map read, then reads each per-session length and
//! lock-free atomic counters; publish and deliver never touch this read.
//!
//! The route is not paged; values are computed live and do not persist
//! beyond the counters (memory-only, restart at zero). Unknown query keys
//! are ignored so new spec filters degrade to the same read instead of a
//! 400. Error answers use the documented `{code, message}` shape.
//!
//! Shape honesty: `metrics` carries the counters the broker tracks
//! (`hits`/`misses`/`inserts` as `{"value": n}`, live `count`, the
//! configured `max_count` bound). Rate and memory fields the broker does
//! not track are omitted rather than answered with constants standing in
//! for measurements.
// TODO(parity): what is the exact documented status shape (nested
// `metrics` versus flat fields, per-counter rate objects, memory
// accounting, and where the enabled flag sits)? The rulebook does not
// decide the exact shape and forbids constants standing in for
// measurements; the current choice reports the honest live subset
// (`enabled`, counter-only `metrics` plus single-node `node_metrics`)
// and omits rate and memory fields the broker does not track, until the
// checker pins it.
// TODO(parity): should a global or per-client clear zero the cumulative
// counters, and should a disabled node policy stop the increments? The
// rulebook does not decide the reset or disabled semantics; the current
// choice keeps counters cumulative and always counting so the status
// observes every decision, until the checker pins it.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

use crate::ApiState;

/// `GET /authorization/node_cache/status`: live node-cache health.
///
/// Returns 200 with the live `enabled` flag, the counter-only `metrics`
/// (`hits`/`misses`/`inserts` values, live `count`, configured
/// `max_count` bound) and the single-node `node_metrics` row carrying the
/// same metrics under this node's id. An empty cache reads as zero
/// entries and zero counters, never an error. Unknown query keys are
/// ignored: the route is not paged, so extra keys never become a 400.
/// Management-plane read only: one `Arc` clone per live session plus
/// lock-free atomic loads; publish and deliver never touch this read.
pub async fn get_authz_node_cache_status(
    State(state): State<ApiState>,
    Query(_ignored): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let stats = state.sessions.authz_node_stats();
    let enabled = state.authz_node_cache.is_enabled();
    let max_count = state.authz_node_cache.max_count();
    let metrics = serde_json::json!({
        "hits": {"value": stats.hits},
        "misses": {"value": stats.misses},
        "inserts": {"value": stats.inserts},
        "count": stats.entries,
        "max_count": max_count,
    });
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "enabled": enabled,
            "metrics": metrics,
            "node_metrics": [{"node": state.node_id, "metrics": metrics}],
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn standalone_state() -> ApiState {
        let engine = std::sync::Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        ApiState::standalone(engine)
    }

    async fn response_parts(response: Response) -> (StatusCode, serde_json::Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("status body is small and readable");
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("status body is JSON");
        (status, body)
    }

    #[tokio::test]
    async fn empty_cache_reads_as_zero_with_live_bounds() {
        let state = standalone_state();
        let (status, body) = response_parts(
            get_authz_node_cache_status(State(state), Query(Default::default())).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["enabled"], serde_json::json!(true));
        assert_eq!(body["metrics"]["hits"]["value"], serde_json::json!(0));
        assert_eq!(body["metrics"]["misses"]["value"], serde_json::json!(0));
        assert_eq!(body["metrics"]["inserts"]["value"], serde_json::json!(0));
        assert_eq!(body["metrics"]["count"], serde_json::json!(0));
        assert_eq!(
            body["metrics"]["max_count"],
            serde_json::json!(broker_config::DEFAULT_AUTHZ_NODE_CACHE_MAX)
        );
        // Single-node row mirrors the aggregate under this node's id.
        assert_eq!(body["node_metrics"].as_array().expect("rows").len(), 1);
        assert!(body["node_metrics"][0]["node"].is_string());
        assert_eq!(
            body["node_metrics"][0]["metrics"], body["metrics"],
            "node row mirrors the aggregate"
        );
    }

    #[tokio::test]
    async fn decisions_move_counters_and_entries() {
        // Before this task the route itself was missing (bare 404); the
        // counters below had nowhere to surface.
        let state = standalone_state();
        // Exercise decisions through real session state (the same entries
        // the broker's publish/subscribe authorization events record).
        let first = state.sessions.get_or_create("authz-status-a", true).0;
        let second = state.sessions.get_or_create("authz-status-b", true).0;
        // Two new decisions: two misses plus two inserts, two entries.
        first.record_authz_decision("publish", "t/a", 0, false, true);
        second.record_authz_decision("subscribe", "t/b", 0, false, true);
        // A repeat of the same key refreshes in place: one hit, entries
        // unchanged.
        first.record_authz_decision("publish", "t/a", 0, false, true);

        let (status, body) = response_parts(
            get_authz_node_cache_status(State(state.clone()), Query(Default::default())).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["metrics"]["misses"]["value"], serde_json::json!(2));
        assert_eq!(body["metrics"]["inserts"]["value"], serde_json::json!(2));
        assert_eq!(body["metrics"]["hits"]["value"], serde_json::json!(1));
        assert_eq!(body["metrics"]["count"], serde_json::json!(2));
        for key in ["hits", "misses", "inserts"] {
            assert!(
                body["metrics"][key]["value"].is_number(),
                "counter {key} is numeric"
            );
        }
        assert!(body["metrics"]["count"].is_number());

        // Evicting every entry drops the count while the cumulative
        // counters survive the clear.
        state.sessions.clear_all_authz_caches();
        let (status, body) = response_parts(
            get_authz_node_cache_status(State(state), Query(Default::default())).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["metrics"]["count"], serde_json::json!(0));
        assert_eq!(body["metrics"]["hits"]["value"], serde_json::json!(1));
        assert_eq!(body["metrics"]["misses"]["value"], serde_json::json!(2));
    }

    #[tokio::test]
    async fn unknown_query_keys_are_ignored() {
        let state = standalone_state();
        let mut query = std::collections::HashMap::new();
        query.insert("unknown_param".to_string(), "1".to_string());
        let (status, body) =
            response_parts(get_authz_node_cache_status(State(state), Query(query)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["metrics"]["count"].is_number());
    }
}
