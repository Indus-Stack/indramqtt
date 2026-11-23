//! Node status endpoint for the v5 REST API.
//!
//! Covers `GET /api/v5/status` and its public alias `GET /status`.
//! Single node, management-plane only: one relaxed atomic load serves
//! each request, no delivery locks, no new buffering.
//!
//! Store bounds (both stated here and enforced by construction):
//! - readiness is exactly one atomic bool shared with the kernel;
//!   listing copies one tiny JSON object per request and never grows with
//!   connections, sessions or subscriptions;
//! - per-request work is one relaxed atomic load, so handler cost stays
//!   constant no matter how many clients are connected.

use axum::{
    extract::{Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;

use crate::ApiState;

/// Product name in the plain text form of the answer.
const PRODUCT: &str = "indramqtt";

/// `app_status` while the node serves requests.
const APP_RUNNING: &str = "running";
/// `app_status` while the node does not serve requests.
const APP_NOT_RUNNING: &str = "not_running";
/// `broker_status` while the node serves requests.
const BROKER_STARTED: &str = "started";
/// `broker_status` while the node does not serve requests.
const BROKER_STOPPED: &str = "stopped";

/// Query of `GET /status`. Only `format` is read. The extractor ignores
/// every other key, so unknown parameters never cause a 400.
#[derive(Deserialize, Default)]
pub struct StatusQuery {
    #[serde(default)]
    format: Option<String>,
}

/// Node status read.
///
/// Single node, management-plane only: one relaxed atomic load serves
/// request, no delivery locks, no new buffering.
///
/// While the node is ready the answer is 200. While it is not ready the
/// answer is 503, so load balancers stop sending traffic.
///
/// With `format=json` the body is an object with `rel_vsn` (the version of
/// this build, with the `v` prefix of the documented example), `node_name`
/// (the name of the local node, as in `GET /nodes`), `broker_status` and
/// `app_status`. With any other `format`, or without it, the body is two
/// lines of plain text:
///
/// ```text
/// Node indra-node-1@host.example is started
/// indramqtt is running
/// ```
///
/// While the node is not ready the first line ends with `stopped` and the
/// second line reads `indramqtt is not_running`.
pub async fn get_status(
    State(state): State<ApiState>,
    Query(query): Query<StatusQuery>,
) -> Response {
    let (code, broker, app) = if state.readiness.is_ready() {
        (StatusCode::OK, BROKER_STARTED, APP_RUNNING)
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            BROKER_STOPPED,
            APP_NOT_RUNNING,
        )
    };
    let node = crate::node_scope::local_node_name();
    if query.format.as_deref() == Some("json") {
        let body = serde_json::json!({
            "rel_vsn": format!("v{}", env!("CARGO_PKG_VERSION")),
            "node_name": node,
            "broker_status": broker,
            "app_status": app,
        });
        return (code, Json(body)).into_response();
    }
    let text = format!("Node {node} is {broker}\n{PRODUCT} is {app}\n");
    (
        code,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        text,
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

    async fn call(state: ApiState, format: Option<&str>) -> (StatusCode, String, String) {
        let query = StatusQuery {
            format: format.map(str::to_string),
        };
        let response = get_status(State(state), Query(query)).await;
        let status = response.status();
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let bytes = axum::body::to_bytes(response.into_body(), 256 * 1024)
            .await
            .expect("status body is small and readable");
        let text = String::from_utf8(bytes.to_vec()).expect("status body is UTF-8");
        (status, content_type, text)
    }

    #[tokio::test]
    async fn ready_node_answers_two_lines_of_text() {
        let state = standalone_state();
        state.readiness.mark_ready();
        let (status, content_type, text) = call(state, None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(content_type.starts_with("text/plain"), "{content_type}");
        let node = crate::node_scope::local_node_name();
        assert!(node.contains('@'), "node name has the name@host form");
        assert_eq!(
            text,
            format!("Node {node} is started\nindramqtt is running\n")
        );
    }

    #[tokio::test]
    async fn text_form_has_two_lines_and_names_the_product() {
        let (_, _, text) = call(standalone_state(), Some("text")).await;
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2, "two lines of text: {text:?}");
        assert!(lines[0].starts_with("Node "));
        assert_eq!(lines[1], "indramqtt is running");
    }

    #[tokio::test]
    async fn json_form_has_the_documented_fields() {
        let (status, content_type, text) = call(standalone_state(), Some("json")).await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            content_type.starts_with("application/json"),
            "{content_type}"
        );
        let body: serde_json::Value = serde_json::from_str(&text).expect("body is JSON");
        let object = body.as_object().expect("body is an object");
        assert_eq!(object.len(), 4, "only the documented fields: {body}");
        assert_eq!(body["broker_status"], serde_json::json!("started"));
        assert_eq!(body["app_status"], serde_json::json!("running"));
        assert_eq!(
            body["node_name"],
            serde_json::json!(crate::node_scope::local_node_name())
        );
        // The version is the build version. The example value of the
        // specification must never return.
        assert_eq!(
            body["rel_vsn"],
            serde_json::json!(format!("v{}", env!("CARGO_PKG_VERSION")))
        );
        assert_ne!(body["rel_vsn"], serde_json::json!("v5.0.23"));
    }

    #[tokio::test]
    async fn standalone_state_is_ready_by_default() {
        let state = standalone_state();
        assert!(state.readiness.is_ready());
        let (status, _, _) = call(state, None).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn draining_node_answers_503_in_both_forms() {
        let state = standalone_state();
        state.readiness.mark_not_ready();
        let (status, _, text) = call(state.clone(), None).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        let node = crate::node_scope::local_node_name();
        assert_eq!(
            text,
            format!("Node {node} is stopped\nindramqtt is not_running\n")
        );
        let (status, _, text) = call(state, Some("json")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        let body: serde_json::Value = serde_json::from_str(&text).expect("body is JSON");
        assert_eq!(body["app_status"], serde_json::json!("not_running"));
        assert_eq!(body["broker_status"], serde_json::json!("stopped"));
    }
}
