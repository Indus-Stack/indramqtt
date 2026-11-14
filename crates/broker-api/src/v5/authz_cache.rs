//! Global authorization decision-cache clear for the v5 API.
//!
//! Covers `DELETE /authorization/cache` (C04-01) over the real per-session
//! decision caches in [`broker_session::SessionManager`]: every publish and
//! subscribe authorization event records one entry on its session
//! (`crates/broker-node/src/main.rs` CONNECT/publish/subscribe handling,
//! plus the console path in `crates/broker-api/src/ws.rs`), and this route
//! evicts all of them cluster-wide on this node so subsequent decisions
//! are recomputed. Single-node, management-plane only: the handler clones
//! one `Arc` per live session under a single short map read and clears
//! each per-session cache under its own short lock; the publish and
//! deliver paths never take these locks beyond the record-time
//! per-session write, and eviction never blocks the message path.
//!
//! Bound (stated with its reason, enforced in `broker-session`): at most
//! [`broker_session::MAX_AUTHZ_DECISIONS_PER_CLIENT`] entries per client
//! (1024 small entries: topic string plus a few bytes, so one chatty
//! client cannot balloon the node while a working set of recent topics
//! stays visible); past the cap the oldest entry is evicted first. The
//! global clear itself clones at most one `Arc` per live session and
//! holds no lock across sessions, so one management request cannot grow
//! unbounded work on the hot path.
//!
//! Default: the decision cache is enabled by default as that bounded
//! per-session map (no opt-in flag; recording starts with the first
//! authorized decision). Contents are memory-only and do not survive a
//! kernel restart (sessions are rebuilt empty; no file backs these
//! entries).
//!
//! The route is not paged and takes no body; unknown query keys are
//! ignored so new spec filters degrade to the same clear instead of a
//! 400. Error answers use the documented `{code, message}` shape.
// TODO(parity): does the documented clear answer 204 with no body or 200
// with a success envelope, and does the documented cache expire entries
// by TTL in addition to the per-client cap and explicit clear? The
// rulebook does not decide the exact success shape or TTL policy; the
// current choice answers 204 (matching the neighbouring cache clears)
// and keeps entries until an explicit per-client or global clear, bounded
// by the per-client cap, with no background TTL sweep on the hot path.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};

use crate::ApiState;

/// `DELETE /authorization/cache`: evict every cached authorization
/// decision on every session on this node.
///
/// Clearing an already-empty cache still succeeds (204 with no body).
/// Unknown query keys are ignored. A background clear only: clones one
/// `Arc` per session under a single short map read, then clears each
/// per-session cache under its own short lock; no work on the publish or
/// deliver path.
pub async fn clear_authz_cache(
    State(state): State<ApiState>,
    Query(_ignored): Query<std::collections::HashMap<String, String>>,
) -> Response {
    state.sessions.clear_all_authz_caches();
    StatusCode::NO_CONTENT.into_response()
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

    async fn response_parts(response: axum::response::Response) -> (StatusCode, bytes::Bytes) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .expect("clear body is small and readable");
        (status, bytes)
    }

    #[tokio::test]
    async fn global_clear_succeeds_empty_and_ignores_unknown_query() {
        let state = standalone_state();
        let (status, body) = response_parts(
            clear_authz_cache(State(state.clone()), Query(Default::default())).await,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(body.is_empty());

        let mut query = std::collections::HashMap::new();
        query.insert("unknown_param".to_string(), "1".to_string());
        let (status, body) =
            response_parts(clear_authz_cache(State(state.clone()), Query(query)).await).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(body.is_empty());
    }

    #[tokio::test]
    async fn global_clear_evicts_entries_on_every_session() {
        let state = standalone_state();
        let first = state.sessions.get_or_create("authz-global-a", true).0;
        let second = state.sessions.get_or_create("authz-global-b", true).0;
        first.record_authz_decision("publish", "t/a", 0, false, true);
        second.record_authz_decision("subscribe", "t/b", 0, false, true);
        assert_eq!(first.authz_cache_snapshot().len(), 1);
        assert_eq!(second.authz_cache_snapshot().len(), 1);

        let (status, _) = response_parts(
            clear_authz_cache(State(state.clone()), Query(Default::default())).await,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(first.authz_cache_snapshot().is_empty());
        assert!(second.authz_cache_snapshot().is_empty());

        // Clearing again still succeeds.
        let (status, _) = response_parts(
            clear_authz_cache(State(state.clone()), Query(Default::default())).await,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }
}
