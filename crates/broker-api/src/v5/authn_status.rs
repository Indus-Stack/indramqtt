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
//!
//! Shape note: each `node_*` field is an array with one object per node
//! (today one node). The management console reads the same arrays. Rate
//! fields are absent: the broker tracks counters only, never rates.
//! Health fields the broker does not measure are absent as well: no
//! `status` or `node_status` is reported because the broker performs no
//! backend health probe (an enabled built-in entry executes locally, a
//! stored-only backend never executes, and neither state is a measured
//! connection).

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

use crate::errors::ApiError;
use crate::ApiState;

/// `GET /authentication/{id}/status`: live counters for one
/// authenticator.
///
/// Returns 200 with the stored `id`/`enable`, the live counter-only
/// `metrics` (and its single-node `node_metrics` row), the live
/// counter-only `resource_metrics` (and its single-node
/// `node_resource_metrics` row) and the empty `node_error` list.
/// Health fields the broker does not measure (`status`,
/// `node_status`, rates) are omitted, never invented: no backend
/// health probe runs here. Unknown ids fail with `NOT_FOUND` in
/// the documented `{code, message}` shape. Unknown query keys are
/// ignored: the route is not paged, so extra keys never become a 400.
/// Management-plane read only: one short chain lock plus lock-free atomic
/// loads; publish and deliver never touch this store.
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
    let node = state.node_id.clone();
    let metrics = serde_json::json!({
        "total": counters.total,
        "success": counters.success,
        "failed": counters.failed,
        "nomatch": counters.nomatch,
    });
    let resource_metrics = serde_json::json!({
        "matched": counters.total,
        "success": counters.success,
        "failed": counters.failed,
    });
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "id": entry.id,
            "enable": entry.enable,
            "metrics": metrics,
            "node_metrics": [{"node": node, "metrics": metrics}],
            "resource_metrics": resource_metrics,
            "node_resource_metrics": [{"node": node, "metrics": resource_metrics}],
            "node_error": [],
        })),
    )
        .into_response()
}

/// `POST /authentication/{id}/metrics/reset`: zero the live counters for
/// one authenticator.
///
/// Resets the per-authenticator counters to zero and reports success
/// (204, no body). Resetting an unknown id fails with `NOT_FOUND` in the
/// documented `{code, message}` shape, not an empty body. Counters resume
/// from zero on the next authentication; the reset itself does not
/// persist beyond the counter values. Management-plane only: one short
/// chain read plus lock-free atomic stores; increments stay lock-free
/// atomics and publish and deliver never touch these counters. The
/// counter set is fixed (success/failed/nomatch/total per chain id, at
/// most the chain cap of records), so a reset never grows anything.
// TODO(parity): does the spec report success as an empty 204 or as a 200
// with a body? The rulebook does not decide the exact success shape; the
// current choice answers 204 with no body, mirroring the sibling rule
// and node-cache resets, until the checker pins it.
pub async fn reset_authn_metrics(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Response {
    if !state.authn_chain.reset_counters(&id) {
        return ApiError::NotFound(format!("authenticator {id:?} not found")).into_response();
    }
    StatusCode::NO_CONTENT.into_response()
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
    async fn reset_zeroes_live_counters_and_unknown_is_404() {
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
        state.authn_chain.record_success(&stored.id);
        state.authn_chain.record_failed(&stored.id);
        state.authn_chain.record_nomatch(&stored.id);

        // Unknown ids miss with the documented shape, not an empty body.
        // Before this task the route itself was missing (bare 404).
        let (status, body) = response_parts(
            reset_authn_metrics(
                State(state.clone()),
                Path("password_based:nowhere".to_string()),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
        assert!(body["message"].is_string());

        // Reset reports success with no body; the next status read is zero.
        let response = reset_authn_metrics(State(state.clone()), Path(stored.id.clone())).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
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
        assert_eq!(body["metrics"]["success"], serde_json::json!(0));
        assert_eq!(body["metrics"]["failed"], serde_json::json!(0));
        assert_eq!(body["metrics"]["nomatch"], serde_json::json!(0));
        assert_eq!(body["metrics"]["total"], serde_json::json!(0));

        // Counters resume from zero on the next verdict.
        state.authn_chain.record_failed(&stored.id);
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
        assert_eq!(body["metrics"]["failed"], serde_json::json!(1));
        assert_eq!(body["metrics"]["total"], serde_json::json!(1));
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
        // Health the broker does not measure is omitted, never invented.
        assert!(body.get("status").is_none());
        assert!(body.get("node_status").is_none());
        assert_eq!(body["metrics"]["success"], serde_json::json!(2));
        assert_eq!(body["metrics"]["failed"], serde_json::json!(1));
        assert_eq!(body["metrics"]["total"], serde_json::json!(3));
        assert!(body["metrics"]["nomatch"].is_number());
        assert_eq!(
            body["node_metrics"][0]["metrics"]["total"],
            body["metrics"]["total"]
        );
        assert_eq!(body["node_error"], serde_json::json!([]));
        // Each remaining `node_*` field is an array with one row per node.
        assert!(body["node_metrics"].is_array());
        assert!(body["node_resource_metrics"].is_array());
        assert_eq!(
            body["node_resource_metrics"][0]["metrics"]["matched"],
            body["metrics"]["total"]
        );
        assert_eq!(
            body["resource_metrics"]["success"],
            body["metrics"]["success"]
        );
        // A disabled entry keeps its own counters with no invented status.
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
        assert_eq!(body["enable"], serde_json::json!(false));
        assert!(body.get("status").is_none());
        assert!(body.get("node_status").is_none());
        assert_eq!(body["metrics"]["total"], serde_json::json!(3));
    }
}
