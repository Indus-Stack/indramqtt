//! Honest empty states for functions the broker does not have (AP-01).
//!
//! Covers the reads and writes of six groups: AI providers and completion
//! profiles, message queues, message streams, telemetry, load rebalance and
//! node eviction. The management console loads these reads on main pages.
//! A 404 breaks the page, so each read answers the documented shape with a
//! true empty or disabled state. Each write answers 400 `BAD_REQUEST` with a
//! message that the function is not available in this broker.
//!
//! Nothing here stores state. Handlers render fixed answers and take no
//! lock. Publish and deliver never touch this module. The routes are
//! management-plane only.

use axum::{
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

use crate::errors::ApiError;
use crate::node_scope::{local_node_name, resolve_node};
use crate::pagination::{meta_page, PageParams};

/// Fixed disabled queue config. Each value is the documented default. The
/// broker has no queues, so `enable` is false and auto creation can never
/// trigger. The console reads `enable` from this route.
fn disabled_queue_config() -> serde_json::Value {
    serde_json::json!({
        "auto_create": {
            "lastvalue": {
                "key_expression": "message.from",
            },
            "regular": {},
        },
        "enable": false,
        "find_queue_retry_interval": "10s",
        "gc_interval": "1h",
        "max_queue_count": 100,
        "regular_queue_retention_period": "7d",
    })
}

/// Fixed disabled stream config. Each value is the documented default. The
/// broker has no streams, so `enable` is false and auto creation can never
/// trigger. The console reads `enable` from this route.
fn disabled_stream_config() -> serde_json::Value {
    serde_json::json!({
        "auto_create": {
            "lastvalue": {
                "key_expression": "message.from",
            },
            "regular": {
                "key_expression": "message.from",
            },
        },
        "enable": false,
        "gc_interval": "1h",
        "max_stream_count": 1000,
        "regular_stream_retention_period": "7d",
    })
}

/// `GET /ai/providers`: empty list. The broker has no AI providers. The
/// Flow Designer reads this list together with the completion profiles.
pub async fn list_ai_providers() -> Response {
    (StatusCode::OK, Json(serde_json::json!({ "items": [] }))).into_response()
}

/// `POST /ai/providers`: refused. The broker has no AI providers.
pub async fn create_ai_provider() -> Response {
    ApiError::BadRequest("ai providers are not available in this broker".to_string())
        .into_response()
}

/// `GET /ai/providers/{name}`: miss. Nothing is stored, so each name is
/// unknown.
pub async fn get_ai_provider(Path(name): Path<String>) -> Response {
    ApiError::NotFound(format!("ai provider {name:?} not found")).into_response()
}

/// `PUT /ai/providers/{name}`: refused. The broker has no AI providers.
pub async fn update_ai_provider(Path(_name): Path<String>) -> Response {
    ApiError::BadRequest("ai providers are not available in this broker".to_string())
        .into_response()
}

/// `DELETE /ai/providers/{name}`: refused. The broker has no AI providers.
pub async fn delete_ai_provider(Path(_name): Path<String>) -> Response {
    ApiError::BadRequest("ai providers are not available in this broker".to_string())
        .into_response()
}

/// `GET /ai/providers/{name}/models`: empty list. The broker has no AI
/// providers, so no provider has models.
pub async fn list_ai_provider_models(Path(_name): Path<String>) -> Response {
    (StatusCode::OK, Json(serde_json::json!({ "items": [] }))).into_response()
}

/// `POST /ai/models`: refused. The broker has no AI providers.
pub async fn list_ai_models() -> Response {
    ApiError::BadRequest("ai providers are not available in this broker".to_string())
        .into_response()
}

/// `GET /ai/completion_profiles`: empty list. The broker has no completion
/// profiles. The Flow Designer reads this list together with the providers.
pub async fn list_ai_completion_profiles() -> Response {
    (StatusCode::OK, Json(serde_json::json!({ "items": [] }))).into_response()
}

/// `POST /ai/completion_profiles`: refused. The broker has no completion
/// profiles.
pub async fn create_ai_completion_profile() -> Response {
    ApiError::BadRequest("ai completion profiles are not available in this broker".to_string())
        .into_response()
}

/// `GET /ai/completion_profiles/{name}`: miss. Nothing is stored, so each
/// name is unknown.
pub async fn get_ai_completion_profile(Path(name): Path<String>) -> Response {
    ApiError::NotFound(format!("ai completion profile {name:?} not found")).into_response()
}

/// `PUT /ai/completion_profiles/{name}`: refused. The broker has no
/// completion profiles.
pub async fn update_ai_completion_profile(Path(_name): Path<String>) -> Response {
    ApiError::BadRequest("ai completion profiles are not available in this broker".to_string())
        .into_response()
}

/// `DELETE /ai/completion_profiles/{name}`: refused. The broker has no
/// completion profiles.
pub async fn delete_ai_completion_profile(Path(_name): Path<String>) -> Response {
    ApiError::BadRequest("ai completion profiles are not available in this broker".to_string())
        .into_response()
}

/// `GET /queues`: empty page. The broker has no queues. The console reads
/// the `data` list from the envelope, so the answer uses the envelope even
/// though the specification schema says array. See BASELINE.md.
pub async fn list_queues(params: PageParams) -> Response {
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "data": [],
            "meta": meta_page(params.page, params.limit, 0),
        })),
    )
        .into_response()
}

/// `POST /queues`: refused. The broker has no queues.
pub async fn create_queue() -> Response {
    ApiError::BadRequest("message queues are not available in this broker".to_string())
        .into_response()
}

/// `GET /queues/config`: disabled config. The console reads `enable` from
/// this route. A failed call reads as enabled, so this route must answer.
pub async fn get_queue_config() -> Response {
    (StatusCode::OK, Json(disabled_queue_config())).into_response()
}

/// `PUT /queues/config`: refused. The broker has no queues.
pub async fn put_queue_config() -> Response {
    ApiError::BadRequest("message queues are not available in this broker".to_string())
        .into_response()
}

/// `GET /queue/{name}`: miss. Nothing is stored, so each name is unknown.
pub async fn get_queue(Path(name): Path<String>) -> Response {
    ApiError::NotFound(format!("message queue {name:?} not found")).into_response()
}

/// `PUT /queue/{name}`: refused. The broker has no queues.
pub async fn update_queue(Path(_name): Path<String>) -> Response {
    ApiError::BadRequest("message queues are not available in this broker".to_string())
        .into_response()
}

/// `DELETE /queue/{name}`: refused. The broker has no queues.
pub async fn delete_queue(Path(_name): Path<String>) -> Response {
    ApiError::BadRequest("message queues are not available in this broker".to_string())
        .into_response()
}

/// `GET /streams`: empty page. The broker has no streams. The console reads
/// the `data` list from the envelope, so the answer uses the envelope even
/// though the specification schema says array. See BASELINE.md.
pub async fn list_streams(params: PageParams) -> Response {
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "data": [],
            "meta": meta_page(params.page, params.limit, 0),
        })),
    )
        .into_response()
}

/// `POST /streams`: refused. The broker has no streams.
pub async fn create_stream() -> Response {
    ApiError::BadRequest("message streams are not available in this broker".to_string())
        .into_response()
}

/// `GET /streams/config`: disabled config. The console reads `enable` from
/// this route. A failed call reads as enabled, so this route must answer.
pub async fn get_stream_config() -> Response {
    (StatusCode::OK, Json(disabled_stream_config())).into_response()
}

/// `PUT /streams/config`: refused. The broker has no streams.
pub async fn put_stream_config() -> Response {
    ApiError::BadRequest("message streams are not available in this broker".to_string())
        .into_response()
}

/// `GET /stream/{name}`: miss. Nothing is stored, so each name is unknown.
pub async fn get_stream(Path(name): Path<String>) -> Response {
    ApiError::NotFound(format!("message stream {name:?} not found")).into_response()
}

/// `PUT /stream/{name}`: refused. The broker has no streams.
pub async fn update_stream(Path(_name): Path<String>) -> Response {
    ApiError::BadRequest("message streams are not available in this broker".to_string())
        .into_response()
}

/// `DELETE /stream/{name}`: refused. The broker has no streams.
pub async fn delete_stream(Path(_name): Path<String>) -> Response {
    ApiError::BadRequest("message streams are not available in this broker".to_string())
        .into_response()
}

/// `GET /telemetry/status`: disabled. The broker collects no telemetry.
pub async fn get_telemetry_status() -> Response {
    (StatusCode::OK, Json(serde_json::json!({ "enable": false }))).into_response()
}

/// `PUT /telemetry/status`: refused. The broker collects no telemetry.
pub async fn put_telemetry_status() -> Response {
    ApiError::BadRequest("telemetry is not available in this broker".to_string()).into_response()
}

/// `GET /telemetry/data`: miss. Telemetry is disabled, and the documented
/// disabled answer is 404.
pub async fn get_telemetry_data() -> Response {
    ApiError::NotFound("telemetry is currently not enabled".to_string()).into_response()
}

/// `GET /load_rebalance/status`: disabled. The broker moves no clients
/// between nodes.
pub async fn get_rebalance_status() -> Response {
    (
        StatusCode::OK,
        Json(serde_json::json!({ "status": "disabled" })),
    )
        .into_response()
}

/// `GET /load_rebalance/global_status`: no processes. The broker runs no
/// rebalance, evacuation or purge on any node.
pub async fn get_rebalance_global_status() -> Response {
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "evacuations": [],
            "purges": [],
            "rebalances": [],
        })),
    )
        .into_response()
}

/// `GET /load_rebalance/availability_check`: ready. The node evacuates
/// nothing, so the answer is the documented empty object. Public route: the
/// specification requires no credentials here.
pub async fn get_rebalance_availability() -> Response {
    (StatusCode::OK, Json(serde_json::json!({}))).into_response()
}

/// Shared refused answer of the four node rebalance writes. Unknown nodes
/// miss with 404. The local node refuses with 400: single node has no peer
/// to move clients to and runs no process to stop.
fn refused_rebalance_write(node: &str) -> Response {
    if let Err(miss) = resolve_node(local_node_name(), node) {
        return miss.into_response();
    }
    ApiError::BadRequest("load rebalance is not available in this broker".to_string())
        .into_response()
}

/// `POST /load_rebalance/{node}/start`: refused (see
/// [`refused_rebalance_write`]).
pub async fn start_rebalance(Path(node): Path<String>) -> Response {
    refused_rebalance_write(&node)
}

/// `POST /load_rebalance/{node}/stop`: refused (see
/// [`refused_rebalance_write`]).
pub async fn stop_rebalance(Path(node): Path<String>) -> Response {
    refused_rebalance_write(&node)
}

/// `POST /load_rebalance/{node}/evacuation/start`: refused (see
/// [`refused_rebalance_write`]).
pub async fn start_evacuation(Path(node): Path<String>) -> Response {
    refused_rebalance_write(&node)
}

/// `POST /load_rebalance/{node}/evacuation/stop`: refused (see
/// [`refused_rebalance_write`]).
pub async fn stop_evacuation(Path(node): Path<String>) -> Response {
    refused_rebalance_write(&node)
}

/// `GET /node_eviction/status`: disabled. The broker evicts no nodes.
pub async fn get_node_eviction_status() -> Response {
    (
        StatusCode::OK,
        Json(serde_json::json!({ "status": "disabled" })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v5::api_keys::access_for_path;

    /// Read a handler response into status plus JSON body.
    async fn read(response: Response) -> (StatusCode, serde_json::Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("empty-state body is small and readable");
        let body: serde_json::Value =
            serde_json::from_slice(&bytes).expect("empty-state body is JSON");
        (status, body)
    }

    #[tokio::test]
    async fn ai_lists_read_empty() {
        let (status, body) = read(list_ai_providers().await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({ "items": [] }));
        let (status, body) = read(list_ai_completion_profiles().await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({ "items": [] }));
    }

    #[tokio::test]
    async fn ai_detail_reads_miss() {
        let (status, body) = read(get_ai_provider(Path("x".to_string())).await).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
        let (status, body) = read(get_ai_completion_profile(Path("x".to_string())).await).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
    }

    #[tokio::test]
    async fn ai_writes_refuse() {
        for response in [
            create_ai_provider().await,
            update_ai_provider(Path("x".to_string())).await,
            delete_ai_provider(Path("x".to_string())).await,
            create_ai_completion_profile().await,
            update_ai_completion_profile(Path("x".to_string())).await,
            delete_ai_completion_profile(Path("x".to_string())).await,
            list_ai_models().await,
        ] {
            let (status, body) = read(response).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
        }
    }

    #[tokio::test]
    async fn ai_provider_models_read_empty() {
        let (status, body) = read(list_ai_provider_models(Path("x".to_string())).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({ "items": [] }));
    }

    #[tokio::test]
    async fn queue_list_reads_empty_page() {
        let (status, body) = read(list_queues(PageParams::default()).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"], serde_json::json!([]));
        assert_eq!(body["meta"]["count"], serde_json::json!(0));
        assert_eq!(body["meta"]["hasnext"], serde_json::json!(false));
    }

    #[tokio::test]
    async fn queue_config_reads_disabled() {
        let (status, body) = read(get_queue_config().await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["enable"], serde_json::json!(false));
        assert!(body.get("auto_create").is_some());
        assert!(body.get("find_queue_retry_interval").is_some());
        assert!(body.get("gc_interval").is_some());
        assert!(body.get("max_queue_count").is_some());
        assert!(body.get("regular_queue_retention_period").is_some());
        assert!(body["auto_create"]["lastvalue"].get("limits").is_none());
        assert!(body["auto_create"]["regular"].get("limits").is_none());
    }

    #[tokio::test]
    async fn queue_writes_refuse() {
        for response in [
            create_queue().await,
            put_queue_config().await,
            update_queue(Path("q1".to_string())).await,
            delete_queue(Path("q1".to_string())).await,
        ] {
            let (status, body) = read(response).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
        }
        let (status, body) = read(get_queue(Path("q1".to_string())).await).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
    }

    #[tokio::test]
    async fn stream_list_reads_empty_page() {
        let (status, body) = read(list_streams(PageParams::default()).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"], serde_json::json!([]));
        assert_eq!(body["meta"]["count"], serde_json::json!(0));
        assert_eq!(body["meta"]["hasnext"], serde_json::json!(false));
    }

    #[tokio::test]
    async fn stream_config_reads_disabled() {
        let (status, body) = read(get_stream_config().await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["enable"], serde_json::json!(false));
        assert!(body.get("auto_create").is_some());
        assert!(body.get("gc_interval").is_some());
        assert!(body.get("max_stream_count").is_some());
        assert!(body.get("regular_stream_retention_period").is_some());
        assert!(body["auto_create"]["lastvalue"].get("limits").is_none());
        assert!(body["auto_create"]["regular"].get("limits").is_none());
    }

    #[tokio::test]
    async fn stream_writes_refuse() {
        for response in [
            create_stream().await,
            put_stream_config().await,
            update_stream(Path("s1".to_string())).await,
            delete_stream(Path("s1".to_string())).await,
        ] {
            let (status, body) = read(response).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
        }
        let (status, body) = read(get_stream(Path("s1".to_string())).await).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
    }

    #[tokio::test]
    async fn telemetry_status_reads_disabled() {
        let (status, body) = read(get_telemetry_status().await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({ "enable": false }));
    }

    #[tokio::test]
    async fn telemetry_write_refuses_and_data_misses() {
        let (status, body) = read(put_telemetry_status().await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
        let (status, body) = read(get_telemetry_data().await).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
    }

    #[tokio::test]
    async fn rebalance_reads_show_no_process() {
        let (status, body) = read(get_rebalance_status().await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({ "status": "disabled" }));
        let (status, body) = read(get_rebalance_global_status().await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["evacuations"], serde_json::json!([]));
        assert_eq!(body["purges"], serde_json::json!([]));
        assert_eq!(body["rebalances"], serde_json::json!([]));
        let (status, body) = read(get_rebalance_availability().await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({}));
    }

    #[tokio::test]
    async fn rebalance_writes_refuse_on_local_node() {
        let local = local_node_name().to_string();
        for response in [
            start_rebalance(Path(local.clone())).await,
            stop_rebalance(Path(local.clone())).await,
            start_evacuation(Path(local.clone())).await,
            stop_evacuation(Path(local.clone())).await,
        ] {
            let (status, body) = read(response).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
        }
    }

    #[tokio::test]
    async fn rebalance_writes_miss_on_unknown_node() {
        for response in [
            start_rebalance(Path("no-such-node".to_string())).await,
            stop_rebalance(Path("no-such-node".to_string())).await,
            start_evacuation(Path("no-such-node".to_string())).await,
            stop_evacuation(Path("no-such-node".to_string())).await,
        ] {
            let (status, body) = read(response).await;
            assert_eq!(status, StatusCode::NOT_FOUND);
            assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
        }
    }

    #[tokio::test]
    async fn node_eviction_reads_disabled() {
        let (status, body) = read(get_node_eviction_status().await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({ "status": "disabled" }));
    }

    #[test]
    fn new_segments_have_scopes() {
        use crate::v5::api_keys::PathAccess;
        assert_eq!(
            access_for_path("/api/v5/ai/providers"),
            PathAccess::Scope("data_integration")
        );
        assert_eq!(
            access_for_path("/api/v5/queues"),
            PathAccess::Scope("connections")
        );
        assert_eq!(
            access_for_path("/api/v5/queue/q1"),
            PathAccess::Scope("connections")
        );
        assert_eq!(
            access_for_path("/api/v5/streams"),
            PathAccess::Scope("connections")
        );
        assert_eq!(
            access_for_path("/api/v5/stream/s1"),
            PathAccess::Scope("connections")
        );
        assert_eq!(
            access_for_path("/api/v5/telemetry/status"),
            PathAccess::Scope("monitoring")
        );
        assert_eq!(
            access_for_path("/api/v5/load_rebalance/status"),
            PathAccess::Scope("cluster_operations")
        );
        assert_eq!(
            access_for_path("/api/v5/node_eviction/status"),
            PathAccess::Scope("cluster_operations")
        );
    }
}
