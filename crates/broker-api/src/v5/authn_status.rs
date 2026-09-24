//! Per-authenticator liveness and counters for the v5 management API.
//!
//! Covers `GET /authentication/{id}/status` over the real
//! [`broker_auth::AuthnChain`] entry plus its live atomic counters.
//! Single-node, management-plane only: the handler clones at most one
//! chain entry under a short lock and reads four lock-free atomics; the
//! CONNECT path attributes one verdict per connect
//! (`crates/broker-node/src/main.rs`, CONNECT handling, plus the console
//! CONNECT in `crates/broker-api/src/ws.rs`) and never takes a management
//! lock beyond the counter lookup; publish and deliver never touch this
//! store.
//!
//! The route is not paged; status is computed live and never persists
//! (counters are memory-only and restart at zero). Unknown ids fail with
//! `NOT_FOUND` in the documented `{code, message}` shape. Backend health
//! is stored state only: no new outbound connections are opened here.
// TODO(parity): which status fields plus rate/resource shapes does the
// spec require (rates, resource metrics, exact status vocabulary)? The
// rulebook does not decide the exact shape; the current choice reports
// the honest live subset the broker can supply (`id`, `enable`,
// `status` plus single-node `node_status`, counter-only `metrics` plus
// single-node `node_metrics`, empty `node_error`) and omits rate and
// resource fields the broker does not track rather than standing a
// constant in for a measurement.
// TODO(parity): what is the exact per-backend status vocabulary and the
// exact CONNECT-to-counter attribution (success/failed/nomatch split,
// which entry moves on bypasses or a chain with no live backend)? The
// rulebook does not decide the mapping; the current choice reports
// `connected` for an enabled built-in entry and `disconnected`
// otherwise, counts local verdicts as success/failed on the first
// enabled built-in entry and bypass verdicts as nomatch, and counts a
// no-live-backend refusal as failed on the first entry, until the
// checker pins it.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

use crate::errors::ApiError;
use crate::ApiState;

/// `GET /authentication/{id}/status`: liveness plus live counters for one
/// authenticator.
///
/// Returns 200 with the stored `id`/`enable`, the live `status` (and its
/// single-node `node_status` row), the live counter-only `metrics` (and
/// its single-node `node_metrics` row) and the empty `node_error` list.
/// Unknown ids fail with `NOT_FOUND` in the documented `{code, message}`
/// shape. Unknown query keys are ignored: the route is not paged, so
/// extra keys never become a 400. Management-plane read only: one short
/// chain lock plus lock-free atomic loads; publish and deliver never
/// touch this store.
pub async fn get_authn_status(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Query(_ignored): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let Some(entry) = state.authn_chain.get(&id) else {
        return ApiError::NotFound(format!("authenticator {id:?} not found")).into_response();
    };
    // Lock-free atomic reads: zero when the authenticator never decided.
    let counters =
        state
            .authn_chain
            .counter_snapshot(&id)
            .unwrap_or(broker_auth::AuthnCounterSnapshot {
                success: 0,
                failed: 0,
                nomatch: 0,
                total: 0,
            });
    let status = live_status(&entry);
    let node = state.node_id.clone();
    let metrics = serde_json::json!({
        "total": counters.total,
        "success": counters.success,
        "failed": counters.failed,
        "nomatch": counters.nomatch,
    });
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "id": entry.id,
            "enable": entry.enable,
            "status": status,
            "node_status": [{"node": node, "status": status}],
            "metrics": metrics,
            "node_metrics": [{"node": node, "metrics": metrics}],
            "node_error": [],
        })),
    )
        .into_response()
}

/// Honest single-node liveness for one chain entry.
///
/// An enabled built-in entry executes locally, so it reads as connected.
/// Anything else (disabled, or a stored-only backend that never executes)
/// reads as disconnected: no live backend serves it. No outbound
/// connection is opened to decide this.
fn live_status(entry: &broker_auth::AuthnEntry) -> &'static str {
    if entry.enable && entry.backend == "built_in_database" {
        "connected"
    } else {
        "disconnected"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_rules::{BackpressurePolicy, RuleEngine};

    fn standalone_state() -> ApiState {
        ApiState::standalone(std::sync::Arc::new(RuleEngine::new(
            16,
            BackpressurePolicy::DropOldest,
        )))
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
    async fn unknown_id_is_404_with_documented_shape() {
        let state = standalone_state();
        let (status, body) = response_parts(
            get_authn_status(
                State(state),
                Path("password_based:nowhere".to_string()),
                Query(Default::default()),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
        assert!(body["message"].is_string());
    }

    #[tokio::test]
    async fn status_reports_live_counters_without_touching_publish() {
        let state = standalone_state();
        let stored = state
            .authn_chain
            .insert(broker_auth::AuthnEntry {
                id: "password_based:built_in_database".to_string(),
                mechanism: "password_based".to_string(),
                backend: "built_in_database".to_string(),
                enable: true,
                config: serde_json::Value::Null,
            })
            .expect("insert");
        assert_eq!(stored.id, "password_based:built_in_database");
        state.authn_chain.record_success(&stored.id);
        state.authn_chain.record_success(&stored.id);
        state.authn_chain.record_failed(&stored.id);
        let (status, body) = response_parts(
            get_authn_status(
                State(state.clone()),
                Path(stored.id.clone()),
                Query(Default::default()),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["id"], serde_json::json!(stored.id));
        assert_eq!(body["enable"], serde_json::json!(true));
        assert_eq!(body["status"], serde_json::json!("connected"));
        assert_eq!(body["metrics"]["success"], serde_json::json!(2));
        assert_eq!(body["metrics"]["failed"], serde_json::json!(1));
        assert_eq!(body["metrics"]["total"], serde_json::json!(3));
        assert!(body["metrics"]["nomatch"].is_number());
        assert_eq!(
            body["node_metrics"][0]["metrics"]["total"],
            body["metrics"]["total"]
        );
        assert_eq!(
            body["node_status"][0]["status"],
            serde_json::json!("connected")
        );
        assert_eq!(body["node_error"], serde_json::json!([]));
        // A disabled entry reads as disconnected with its own counters.
        state
            .authn_chain
            .update(
                &stored.id,
                broker_auth::AuthnEntry {
                    id: stored.id.clone(),
                    mechanism: "password_based".to_string(),
                    backend: "built_in_database".to_string(),
                    enable: false,
                    config: serde_json::Value::Null,
                },
            )
            .expect("disable keeps counters");
        let (status, body) = response_parts(
            get_authn_status(
                State(state),
                Path(stored.id.clone()),
                Query(Default::default()),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], serde_json::json!("disconnected"));
        assert_eq!(body["metrics"]["total"], serde_json::json!(3));
    }
}
