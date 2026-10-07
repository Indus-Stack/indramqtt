//! Topic-metric collections for the v5 management API.
//!
//! Covers `GET /mqtt/topic_metrics2` (list), `POST /mqtt/topic_metrics2`
//! (create), `GET /mqtt/topic_metrics2/{name}` (read),
//! `DELETE /mqtt/topic_metrics2/{name}` (remove),
//! `PUT /mqtt/topic_metrics2/{name}/reset` (reset) and
//! `DELETE /mqtt/topic_metrics2` (remove all). The broker keeps no
//! per-filter counters, so this module applies the honest-empty rules:
//! reads answer the documented shape with an empty state; creates fail
//! with `BAD_REQUEST`; named reads, removals and resets miss with the
//! documented `NAME_NOT_FOUND`; the bulk delete reports success.
//! Nothing here stores state and nothing runs on the publish path.
//!
//! The legacy `/mqtt/topic_metrics` paths carry no operation in the
//! specification, so they have no route and answer 404.
// TODO(parity): should the broker count per-filter traffic? The console
// topic-metrics page lists collections and reads counters. No kernel
// counter exists today, so the list stays empty. A counter build must
// stay off the publish path when no collection exists.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use std::collections::HashMap;

use crate::errors::ApiError;
use crate::ApiState;

/// `GET /mqtt/topic_metrics2`: no collections. The broker tracks no
/// per-filter counters, so the list is truly empty.
pub async fn list_collections() -> Response {
    (StatusCode::OK, Json(serde_json::json!([]))).into_response()
}

/// `POST /mqtt/topic_metrics2`: refused. The broker has no
/// per-filter counters to collect into.
pub async fn create_collection(State(_state): State<ApiState>) -> Response {
    ApiError::BadRequest("topic metrics are not available in this broker".to_string())
        .into_response()
}

/// Miss with the documented `NAME_NOT_FOUND` code. The specification
/// names this endpoint its own code instead of the generic `NOT_FOUND`.
fn named_miss(name: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({
            "code": "NAME_NOT_FOUND",
            "message": format!("topic metric collection {name:?} not found"),
        })),
    )
        .into_response()
}

/// `GET /mqtt/topic_metrics2/{name}`: miss. Nothing is stored, so each
/// name is unknown. The `ns` query key is accepted and ignored: every
/// caller is a global administrator while the broker has no namespaces.
pub async fn get_collection(
    Path(name): Path<String>,
    Query(_query): Query<HashMap<String, String>>,
) -> Response {
    named_miss(&name)
}

/// `DELETE /mqtt/topic_metrics2/{name}`: miss. Nothing is stored, so
/// each name is unknown. The `ns` query key is accepted and ignored,
/// as in [`get_collection`].
pub async fn delete_collection(
    Path(name): Path<String>,
    Query(_query): Query<HashMap<String, String>>,
) -> Response {
    named_miss(&name)
}

/// `PUT /mqtt/topic_metrics2/{name}/reset`: miss. Nothing is stored,
/// so no counter resets; the named collection does not exist, which is
/// the documented `NAME_NOT_FOUND` case. The `ns` query key is accepted
/// and ignored, as in [`get_collection`].
pub async fn reset_collection(
    Path(name): Path<String>,
    Query(_query): Query<HashMap<String, String>>,
) -> Response {
    named_miss(&name)
}

/// `DELETE /mqtt/topic_metrics2`: no collections to remove. Always
/// reports success, even when nothing is stored.
pub async fn delete_all_collections() -> Response {
    StatusCode::NO_CONTENT.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn read(response: Response) -> (StatusCode, serde_json::Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("topic-metrics body is small and readable");
        if bytes.is_empty() {
            return (status, serde_json::Value::Null);
        }
        let body: serde_json::Value =
            serde_json::from_slice(&bytes).expect("topic-metrics body is JSON");
        (status, body)
    }

    fn query() -> Query<HashMap<String, String>> {
        Query(HashMap::new())
    }

    #[tokio::test]
    async fn list_reads_empty() {
        let (status, body) = read(list_collections().await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!([]));
    }

    #[tokio::test]
    async fn single_reads_miss_with_documented_code() {
        for response in [
            get_collection(Path("alpha".to_string()), query()).await,
            delete_collection(Path("alpha".to_string()), query()).await,
        ] {
            let (status, body) = read(response).await;
            assert_eq!(status, StatusCode::NOT_FOUND);
            assert_eq!(body["code"], serde_json::json!("NAME_NOT_FOUND"));
        }
    }

    #[tokio::test]
    async fn writes_refuse_with_bad_request() {
        let state = standalone_state();
        let (status, body) = read(create_collection(State(state)).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
    }

    #[tokio::test]
    async fn reset_of_unknown_name_misses() {
        let (status, body) = read(reset_collection(Path("alpha".to_string()), query()).await).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], serde_json::json!("NAME_NOT_FOUND"));
    }

    #[tokio::test]
    async fn delete_all_reports_success() {
        let (status, _) = read(delete_all_collections().await).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }

    fn standalone_state() -> ApiState {
        let engine = std::sync::Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        ApiState::standalone(engine)
    }
}
