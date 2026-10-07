//! Cluster view for the v5 management API.
//!
//! Covers `GET /cluster`, `PUT /cluster` and `GET /cluster/topology`.
//! Reads clone small strings under short locks. The kernel boot path
//! logs the stored description (`crates/broker-node/src/main.rs`, boot
//! seeding). Publish and deliver never touch this root.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;

use crate::node_scope::local_node_name;
use crate::ApiState;
use broker_config::{ClusterMetaConf, ConfigRoot, MAX_CLUSTER_DESCRIPTION_LEN};

/// Largest accepted `PUT /cluster` body (16 KiB). Reason: the body holds
/// one short text field; larger bodies are rejected before parsing so one
/// request cannot balloon management memory.
const MAX_BODY_LEN: usize = 16 * 1024;

/// Render the cluster view from the stored description.
fn cluster_view(description: &str) -> serde_json::Value {
    let local = local_node_name().to_string();
    serde_json::json!({
        "name": local,
        "nodes": [local.clone()],
        "self": local,
        "description": description,
    })
}

/// `GET /cluster`: single-node view with the stored description.
///
/// One lock-free registry read per request; no delivery locks.
pub async fn get_cluster(State(state): State<ApiState>) -> Response {
    let description = state.config.snapshot().cluster_meta.description.clone();
    (StatusCode::OK, Json(cluster_view(&description))).into_response()
}

/// `PUT /cluster`: validated description replace.
///
/// The body must hold only `description` (the documented request field);
/// unknown fields fail closed with the documented `BAD_REQUEST` shape and
/// leave the stored text untouched. On success the text persists across a
/// restart through the config registry. Management-plane only.
pub async fn put_cluster(State(state): State<ApiState>, body: Bytes) -> Response {
    if body.len() > MAX_BODY_LEN {
        return bad_request("cluster body is too large".to_string());
    }
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(item) => item,
        Err(error) => return bad_request(format!("invalid cluster body: {error}")),
    };
    let obj = match value.as_object() {
        Some(map) => map,
        None => return bad_request("cluster body must be a JSON object".to_string()),
    };
    if obj.len() != 1 {
        return bad_request("cluster body must hold only `description`".to_string());
    }
    let raw = match obj.get("description") {
        Some(item) => item,
        None => return bad_request("cluster body must hold only `description`".to_string()),
    };
    let text = match raw.as_str() {
        Some(item) => item,
        None => return bad_request("field `description` must be a string".to_string()),
    };
    if text.len() > MAX_CLUSTER_DESCRIPTION_LEN {
        return bad_request(format!(
            "field `description` holds {} bytes, at most {}",
            text.len(),
            MAX_CLUSTER_DESCRIPTION_LEN
        ));
    }
    let next = ClusterMetaConf {
        description: text.to_string(),
    };
    if let Err(error) = state.config.commit(ConfigRoot::ClusterMeta(next)) {
        return bad_request(format!("invalid cluster body: {error}"));
    }
    if let Err(error) = state.config.save() {
        return internal(format!("cannot persist cluster: {error}"));
    }
    (StatusCode::OK, Json(cluster_view(text))).into_response()
}

/// `GET /cluster/topology`: the local node is the only core node.
///
/// Replicant links do not exist on a single node, so the list is empty
/// (the truth, not a placeholder). One static read per request.
pub async fn get_topology() -> Response {
    let local = local_node_name().to_string();
    (
        StatusCode::OK,
        Json(serde_json::json!([{
            "core_node": local,
            "replicant_nodes": [],
        }])),
    )
        .into_response()
}

fn bad_request(reason: String) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({
            "code": "BAD_REQUEST",
            "message": reason,
        })),
    )
        .into_response()
}

fn internal(reason: String) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({
            "code": "INTERNAL_ERROR",
            "message": reason,
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

    async fn parts(response: Response) -> (StatusCode, serde_json::Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("cluster body is small and readable");
        if bytes.is_empty() {
            return (status, serde_json::Value::Null);
        }
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("cluster body is JSON");
        (status, body)
    }

    #[tokio::test]
    async fn get_cluster_reports_self_and_empty_description() {
        let state = standalone_state();
        let (status, body) = parts(get_cluster(State(state)).await).await;
        assert_eq!(status, StatusCode::OK);
        let local = local_node_name().to_string();
        assert_eq!(body["name"], serde_json::json!(local));
        assert_eq!(body["self"], serde_json::json!(local));
        assert_eq!(body["nodes"], serde_json::json!([local]));
        assert_eq!(body["description"], serde_json::json!(""));
    }

    #[tokio::test]
    async fn put_cluster_round_trip_stores_description() {
        let state = standalone_state();
        let body = Bytes::from_static(br#"{"description":"one node"}"#);
        let (status, updated) = parts(put_cluster(State(state.clone()), body).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(updated["description"], serde_json::json!("one node"));
        let (status, reread) = parts(get_cluster(State(state.clone())).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reread["description"], serde_json::json!("one node"));
    }

    #[tokio::test]
    async fn put_cluster_rejects_unknown_fields() {
        let state = standalone_state();
        for raw in [
            r#"{"description":"x","nodes":[]}"#,
            r#"{"name":"other"}"#,
            r#"{}"#,
            r#"{"description":7}"#,
            r#"[]"#,
        ] {
            let (status, body) =
                parts(put_cluster(State(state.clone()), Bytes::from(raw.to_string())).await).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "body {raw}");
            assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
        }
        // A rejected write leaves the stored text untouched.
        let (_, reread) = parts(get_cluster(State(state)).await).await;
        assert_eq!(reread["description"], serde_json::json!(""));
    }

    #[tokio::test]
    async fn cluster_description_survives_registry_reload() {
        let dir = std::env::temp_dir().join(format!(
            "indramqtt-cluster-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|left| left.as_nanos())
                .unwrap_or(0)
        ));
        let registry = std::sync::Arc::new(
            broker_config::ConfigRegistry::load(&dir).expect("fresh registry loads"),
        );
        registry
            .commit(ConfigRoot::ClusterMeta(ClusterMetaConf {
                description: "kept".to_string(),
            }))
            .expect("valid description commits");
        registry.save().expect("registry saves");
        drop(registry);
        let again = broker_config::ConfigRegistry::load(&dir).expect("stored registry reloads");
        assert_eq!(again.snapshot().cluster_meta.description, "kept");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn topology_lists_local_core_without_replicants() {
        let (status, body) = parts(get_topology().await).await;
        assert_eq!(status, StatusCode::OK);
        let rows = body.as_array().expect("topology is an array");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]["core_node"],
            serde_json::json!(local_node_name().to_string())
        );
        assert_eq!(rows[0]["replicant_nodes"], serde_json::json!([]));
    }
}
