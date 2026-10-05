//! Configured network listeners for the v5 REST API (M1-06).
//!
//! Covers `GET /listeners` (the configured MQTT listeners).
//! Management-plane only: nothing here runs on the per-message path, so
//! reads never take a delivery lock and no new buffering is added to
//! fan-out or fan-in.
//!
//! Honesty rule: this endpoint reports the kernel's configured listeners
//! (the same `listeners.*` section the kernel binds from, captured at
//! boot into [`ApiState::listeners`]), not live edge socket state, and it
//! says so. Fields the broker does not track (per-listener connection
//! counts, acceptors) are omitted, never synthesised. Per-listener
//! detail and stop routes stay unimplemented (404):
// TODO(parity): should `GET /listeners/:id` and stop/resume exist, and
// should the rows report live edge socket state rather than configured
// binds? Neither the rulebook nor the task spec decides the shape; today
// only the configured list is served, fail-closed on unknown paths.
//!
//! Store bounds (both stated here and enforced below):
//! - exactly four rows (tcp, ssl, ws, wss), one small JSON object
//!   each, regardless of connection, session or subscription scale;
//! - per-request work is one `Arc` clone plus rendering four rows, so
//!   handler cost stays constant no matter how many clients are connected.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};

use crate::ApiState;

/// One listener row in the shape that the reference API documents.
///
/// `id` has the form `{type}:{name}`. The broker has one listener for each
/// type, and its name is `default`. The type `ssl` is the TLS listener.
fn listener_row(kind: &str, bind: &str, enable: bool) -> serde_json::Value {
    serde_json::json!({
        "id": format!("{kind}:default"),
        "name": "default",
        "type": kind,
        "bind": bind,
        "enable": enable,
    })
}

/// `GET /listeners`: the configured MQTT listeners as a bare array.
///
/// One row per MQTT listener section (`tcp`, `ssl`, `ws`, `wss`), each
/// with the fields `id`, `name`, `type`, `bind` and `enable`. Disabled rows
/// are still listed so an operator can see what would start when enabled.
/// The HTTP listener of the management API is not an MQTT listener. The
/// reference API lists only the types `tcp`, `ssl`, `ws`, `wss` and `quic`,
/// so that listener has no row.
///
/// The broker does not track the number of connections of one listener or
/// the state of its socket, so the optional fields `status`,
/// `node_status`, `number` and `acceptors` are left out and no value is
/// invented. The route takes no `type` filter value other than the
/// documented ones: a `type` query narrows the list to that type.
pub async fn list_listeners(
    State(state): State<ApiState>,
    Query(query): Query<ListenersQuery>,
) -> impl IntoResponse {
    let listeners = &state.listeners;
    let rows: Vec<serde_json::Value> = vec![
        listener_row("tcp", &listeners.tcp.bind, listeners.tcp.enabled),
        listener_row("ssl", &listeners.tls.bind, listeners.tls.enabled),
        listener_row("ws", &listeners.ws.bind, listeners.ws.enabled),
        listener_row("wss", &listeners.wss.bind, listeners.wss.enabled),
    ]
    .into_iter()
    .filter(|row| match query.kind.as_deref() {
        Some(kind) if !kind.is_empty() => row["type"] == kind,
        _ => true,
    })
    .collect();
    (StatusCode::OK, Json(rows))
}

/// Query of `GET /listeners`. The reference API documents `type`.
#[derive(serde::Deserialize, Default)]
pub struct ListenersQuery {
    #[serde(default, rename = "type")]
    kind: Option<String>,
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

    async fn response_body(response: impl IntoResponse) -> (StatusCode, serde_json::Value) {
        let response = response.into_response();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 256 * 1024)
            .await
            .expect("listeners body is small and readable");
        let body: serde_json::Value =
            serde_json::from_slice(&bytes).expect("listeners body is JSON");
        (status, body)
    }

    async fn list_all(state: ApiState) -> (StatusCode, serde_json::Value) {
        response_body(list_listeners(State(state), Query(ListenersQuery::default())).await).await
    }

    #[tokio::test]
    async fn list_reports_four_configured_rows_with_binds() {
        let state = standalone_state();
        let (status, body) = list_all(state).await;
        assert_eq!(status, StatusCode::OK);
        let rows = body.as_array().expect("listeners list is an array");
        assert_eq!(rows.len(), 4, "one row per MQTT listener section");
        let ids: Vec<&str> = rows
            .iter()
            .map(|r| r["id"].as_str().expect("row carries id"))
            .collect();
        assert_eq!(
            ids,
            vec!["tcp:default", "ssl:default", "ws:default", "wss:default"]
        );
        for row in rows {
            assert!(row["bind"].is_string(), "row carries bind: {row}");
            assert!(row["enable"].is_boolean(), "row carries enable: {row}");
            assert_eq!(row["name"], serde_json::json!("default"));
            let kind = row["type"].as_str().expect("row carries type");
            assert!(["tcp", "ssl", "ws", "wss"].contains(&kind), "type: {kind}");
            assert_eq!(row["id"], serde_json::json!(format!("{kind}:default")));
        }
        // Default configuration: plaintext and WS on, TLS and WSS off.
        let by_id = |id: &str| rows.iter().find(|r| r["id"] == id).expect(id);
        assert_eq!(by_id("tcp:default")["enable"], serde_json::json!(true));
        assert_eq!(by_id("ssl:default")["enable"], serde_json::json!(false));
        assert_eq!(by_id("ws:default")["enable"], serde_json::json!(true));
        assert_eq!(by_id("wss:default")["enable"], serde_json::json!(false));
        // No synthesised live state: rows carry configuration only.
        for row in rows {
            assert!(row.get("connections").is_none(), "no invented count: {row}");
            assert!(row.get("acceptors").is_none(), "no invented count: {row}");
        }
    }

    #[tokio::test]
    async fn list_reflects_custom_binds() {
        let engine = std::sync::Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        let mut state = ApiState::standalone(engine);
        let mut listeners = broker_config::schema::ListenersConf::default();
        listeners.tcp.bind = "127.0.0.1:11883".to_string();
        listeners.api.bind = "127.0.0.1:28083".to_string();
        listeners.ws.enabled = false;
        state.listeners = std::sync::Arc::new(listeners);
        let (status, body) = list_all(state).await;
        assert_eq!(status, StatusCode::OK);
        let rows = body.as_array().expect("listeners list is an array");
        let by_id = |id: &str| rows.iter().find(|r| r["id"] == id).expect(id);
        assert_eq!(
            by_id("tcp:default")["bind"],
            serde_json::json!("127.0.0.1:11883")
        );
        assert_eq!(by_id("ws:default")["enable"], serde_json::json!(false));
    }

    #[tokio::test]
    async fn type_query_narrows_the_list() {
        let state = standalone_state();
        let query = Query(ListenersQuery {
            kind: Some("ws".to_string()),
        });
        let (status, body) = response_body(list_listeners(State(state), query).await).await;
        assert_eq!(status, StatusCode::OK);
        let rows = body.as_array().expect("listeners list is an array");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], serde_json::json!("ws:default"));
    }
}
