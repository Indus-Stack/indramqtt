//! Delayed messages for the v5 management API.
//!
//! Covers `GET /mqtt/delayed` (switch plus pending cap),
//! `PUT /mqtt/delayed` (validated switch and cap update),
//! `GET /mqtt/delayed/messages` (paged list),
//! `GET /mqtt/delayed/messages/{node}/{msgid}` (one waiting entry),
//! `DELETE /mqtt/delayed/messages/{node}/{msgid}` (drop one) and
//! `DELETE /mqtt/delayed/messages/{topic}` (drop every entry for one
//! inner topic). All reads and writes run on the shared scheduler the
//! kernel fires from. The list shows what a client published, and a
//! removal stops the delivery.
//!
//! Store bounds (stated here and enforced by the scheduler):
//! - the list clones the waiting entries once per request under one
//!   short wheel lock and renders only the requested page;
//! - one entry holds at most 1 MiB of payload, and the list renders
//!   payloads past 2048 bytes as the documented marker instead of the
//!   bytes;
//! - the pending cap bounds wheel plus file together; management reads
//!   never block the publish or deliver path.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;

use crate::node_scope::local_node_name;
use crate::pagination::{meta_page, paginate, PageParams};
use crate::v5::retainer::format_rfc3339_ms;
use crate::ApiState;

/// Payloads past this size render as the documented marker, not as
/// base64. The value comes from the specification.
const PAYLOAD_RENDER_LIMIT: usize = 2048;

/// Marker rendered for payloads past [`PAYLOAD_RENDER_LIMIT`]. The
/// spelling (single `O`) comes from the specification.
const PAYLOAD_TOO_LARGE: &str = "PAYLOAD_TO_LARGE";

/// Largest accepted `max_delayed_messages` (one million).
///
/// Reason: bounds an explicit operator choice within a reviewable range;
/// zero selects no cap explicitly. Past one million the value is almost
/// certainly a typo, and the write fails closed.
const MAX_DELAYED_MESSAGES_UPPER: u64 = 1_000_000;

/// `GET /mqtt/delayed`: the switch plus the pending cap.
///
/// One atomic load each per request; the publish path never waits on
/// this read.
pub async fn get_delayed_config(State(state): State<ApiState>) -> Response {
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "enable": state.delayed.is_enabled(),
            "max_delayed_messages": state.delayed.max_messages() as u64,
        })),
    )
        .into_response()
}

/// `PUT /mqtt/delayed`: validated switch and cap update.
///
/// The body is a JSON object. Each present field is validated before
/// anything applies, so one bad field rejects the whole write with the
/// documented `BAD_REQUEST` shape and leaves the stored values
/// untouched. Missing fields keep their values. Unknown fields are
/// ignored. An empty object reads back the current values.
pub async fn put_delayed_config(State(state): State<ApiState>, body: Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => return bad_request(format!("invalid delayed body: {error}")),
    };
    let obj = match value.as_object() {
        Some(obj) => obj,
        None => return bad_request("delayed body must be a JSON object".to_string()),
    };
    let mut enable = state.delayed.is_enabled();
    let mut max = state.delayed.max_messages() as u64;
    if let Some(raw) = obj.get("enable") {
        match raw.as_bool() {
            Some(flag) => enable = flag,
            None => return bad_request("field `enable` must be a boolean".to_string()),
        }
    }
    if let Some(raw) = obj.get("max_delayed_messages") {
        match raw.as_u64() {
            Some(limit) if limit <= MAX_DELAYED_MESSAGES_UPPER => max = limit,
            _ => {
                return bad_request(format!(
                    "field `max_delayed_messages` must be an integer 0..={MAX_DELAYED_MESSAGES_UPPER}"
                ));
            }
        }
    }
    state.delayed.set_enable(enable);
    state.delayed.set_max_messages(max as usize);
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "enable": enable,
            "max_delayed_messages": max,
        })),
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

/// Render one waiting entry in the documented list shape.
///
/// `with_payload` selects the list shape (payload included) or the
/// detail shape (payload omitted). Unknown client names read as empty
/// strings. The interval reads at least one second.
fn message_json(
    entry: &broker_delayed::DelayedEntry,
    node: &str,
    now_ms: u64,
    with_payload: bool,
) -> serde_json::Value {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    let interval_secs = entry
        .deliver_at_ms
        .saturating_sub(entry.publish_at_ms)
        .div_ceil(1000)
        .max(1);
    let remaining_secs = entry.deliver_at_ms.saturating_sub(now_ms) / 1000;
    let mut map = serde_json::Map::with_capacity(12);
    map.insert(
        "delayed_interval".to_string(),
        serde_json::Value::from(interval_secs),
    );
    map.insert(
        "delayed_remaining".to_string(),
        serde_json::Value::from(remaining_secs),
    );
    map.insert(
        "expected_at".to_string(),
        serde_json::Value::from(format_rfc3339_ms(entry.deliver_at_ms)),
    );
    map.insert(
        "from_clientid".to_string(),
        serde_json::Value::from(entry.publisher.clone().unwrap_or_default()),
    );
    map.insert(
        "from_username".to_string(),
        serde_json::Value::from(entry.username.clone().unwrap_or_default()),
    );
    map.insert("msgid".to_string(), serde_json::Value::from(entry.id));
    map.insert(
        "node".to_string(),
        serde_json::Value::from(node.to_string()),
    );
    if with_payload {
        let payload = if entry.payload.len() > PAYLOAD_RENDER_LIMIT {
            PAYLOAD_TOO_LARGE.to_string()
        } else {
            STANDARD.encode(&entry.payload)
        };
        map.insert("payload".to_string(), serde_json::Value::from(payload));
    }
    map.insert(
        "publish_at".to_string(),
        serde_json::Value::from(format_rfc3339_ms(entry.publish_at_ms)),
    );
    map.insert("qos".to_string(), serde_json::Value::from(entry.qos));
    map.insert(
        "topic".to_string(),
        serde_json::Value::from(entry.topic.clone()),
    );
    serde_json::Value::Object(map)
}

/// `GET /mqtt/delayed/messages`: paged list of waiting entries.
///
/// Snapshots the wheel once per request under one short lock. Renders
/// only the requested page through the shared paging helper (`page` and
/// `limit` with `meta { page, limit, count, hasnext }`). Unknown query
/// keys fall back to defaults via [`PageParams`]; an empty wheel reads
/// as an empty page, never an error.
pub async fn list_delayed_messages(State(state): State<ApiState>, params: PageParams) -> Response {
    let node = local_node_name().to_string();
    let now = crate::v5::retainer::now_ms();
    let entries = state.delayed.snapshot();
    let total = entries.len();
    let page_items = paginate(&entries, params.page, params.limit);
    let data: Vec<serde_json::Value> = page_items
        .iter()
        .map(|entry| message_json(entry, &node, now, true))
        .collect();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "data": data,
            "meta": meta_page(params.page, params.limit, total),
        })),
    )
        .into_response()
}

/// True when `requested` names this node: the full `name@host` form or
/// the bare node id before the `@`.
fn is_local_node(requested: &str) -> bool {
    let local = local_node_name();
    requested == local || Some(requested) == local.split('@').next()
}

fn invalid_node(requested: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({
            "code": "INVALID_NODE",
            "message": format!("unknown node {requested:?}"),
        })),
    )
        .into_response()
}

fn message_id_error() -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({
            "code": "MESSAGE_ID_SCHEMA_ERROR",
            "message": "delayed message id must be an integer",
        })),
    )
        .into_response()
}

fn message_id_miss(id: u64) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({
            "code": "MESSAGE_ID_NOT_FOUND",
            "message": format!("delayed message {id} not found"),
        })),
    )
        .into_response()
}

/// `GET /mqtt/delayed/messages/{node}/{msgid}`: one waiting entry.
///
/// The node must name this node (`INVALID_NODE` otherwise); the id must
/// parse as an integer (`MESSAGE_ID_SCHEMA_ERROR` otherwise). A fired
/// or removed id reads as `MESSAGE_ID_NOT_FOUND`. The detail shape
/// carries no payload, per the specification.
pub async fn get_delayed_message(
    State(state): State<ApiState>,
    Path((node_raw, msgid_raw)): Path<(String, String)>,
) -> Response {
    if !is_local_node(&node_raw) {
        return invalid_node(&node_raw);
    }
    let id: u64 = match msgid_raw.parse() {
        Ok(id) => id,
        Err(_) => return message_id_error(),
    };
    match state.delayed.get(id) {
        Some(entry) => {
            let node = local_node_name().to_string();
            let now = crate::v5::retainer::now_ms();
            (
                StatusCode::OK,
                Json(message_json(&entry, &node, now, false)),
            )
                .into_response()
        }
        None => message_id_miss(id),
    }
}

/// `DELETE /mqtt/delayed/messages/{node}/{msgid}`: drop one entry.
///
/// Same node and id checks as the detail read. A removed entry never
/// fires: the wheel and the file both forget it. Reports 204 on
/// removal, `MESSAGE_ID_NOT_FOUND` when nothing waited.
pub async fn delete_delayed_message(
    State(state): State<ApiState>,
    Path((node_raw, msgid_raw)): Path<(String, String)>,
) -> Response {
    if !is_local_node(&node_raw) {
        return invalid_node(&node_raw);
    }
    let id: u64 = match msgid_raw.parse() {
        Ok(id) => id,
        Err(_) => return message_id_error(),
    };
    if state.delayed.remove_one(id) {
        StatusCode::NO_CONTENT.into_response()
    } else {
        message_id_miss(id)
    }
}

/// `DELETE /mqtt/delayed/messages/{topic}`: drop every entry for one
/// inner topic.
///
/// The topic must be a valid concrete topic name
/// (`INVALID_TOPIC_NAME` otherwise, including wildcard or empty names
/// that can never be scheduled). No waiting entry for a valid name
/// reads as the documented `MESSAGE_NOT_FOUND`.
pub async fn delete_delayed_by_topic(
    State(state): State<ApiState>,
    Path(topic_raw): Path<String>,
) -> Response {
    if broker_protocol::Topic::new(topic_raw.clone()).is_err() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "INVALID_TOPIC_NAME",
                "message": format!("invalid topic {topic_raw:?}"),
            })),
        )
            .into_response();
    }
    if state.delayed.remove_by_topic(&topic_raw) > 0 {
        StatusCode::NO_CONTENT.into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "code": "MESSAGE_NOT_FOUND",
                "message": format!("no delayed message for topic {topic_raw:?}"),
            })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::Response;

    fn standalone_state() -> ApiState {
        let engine = std::sync::Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        ApiState::standalone(engine)
    }

    async fn read(response: Response) -> (StatusCode, serde_json::Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("delayed body is small and readable");
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("delayed body is JSON");
        (status, body)
    }

    fn schedule(state: &ApiState, topic: &str, delay_secs: u64) -> broker_delayed::DelayedEntry {
        let scheduler = std::sync::Arc::clone(&state.delayed);
        let now = crate::v5::retainer::now_ms();
        let entry = broker_delayed::DelayedEntry {
            id: scheduler.next_id(),
            deliver_at_ms: now + delay_secs * 1000,
            publish_at_ms: now,
            topic: topic.to_string(),
            qos: 1,
            retain: false,
            payload: b"hello-delayed".to_vec(),
            publisher: Some("conf-delayed-c1".to_string()),
            username: Some("alice".to_string()),
            publisher_tenant: broker_session::tenant::DEFAULT_TENANT_ID.to_string(),
        };
        scheduler
            .persist_and_schedule(entry.clone(), delay_secs)
            .expect("fits the test scheduler");
        entry
    }

    #[tokio::test]
    async fn config_reads_defaults_and_round_trips() {
        let state = standalone_state();
        let (status, before) = read(get_delayed_config(State(state.clone())).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(before["enable"], serde_json::json!(true));
        assert_eq!(
            before["max_delayed_messages"],
            serde_json::json!(broker_delayed::MAX_DELAYED_PENDING as u64)
        );

        let body = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "enable": false,
                "max_delayed_messages": 7,
            }))
            .expect("update is JSON"),
        );
        let (status, after) = read(put_delayed_config(State(state.clone()), body).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(after["enable"], serde_json::json!(false));
        assert_eq!(after["max_delayed_messages"], serde_json::json!(7));
        assert!(!state.delayed.is_enabled());
        assert_eq!(state.delayed.max_messages(), 7);

        let (status, reread) = read(get_delayed_config(State(state.clone())).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reread, after);
    }

    #[tokio::test]
    async fn config_rejects_bad_fields_without_applying() {
        let state = standalone_state();
        for bad in [
            serde_json::json!({"enable": "yes"}),
            serde_json::json!({"max_delayed_messages": -1}),
            serde_json::json!({"max_delayed_messages": "many"}),
            serde_json::json!({"max_delayed_messages": 1_000_001}),
            serde_json::json!([]),
            serde_json::json!("enable"),
        ] {
            let body = Bytes::from(serde_json::to_vec(&bad).expect("bad body is JSON"));
            let (status, err) = read(put_delayed_config(State(state.clone()), body).await).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "body {bad} must fail");
            assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
        }
        assert!(state.delayed.is_enabled());
        assert_eq!(
            state.delayed.max_messages(),
            broker_delayed::MAX_DELAYED_PENDING
        );
    }

    #[tokio::test]
    async fn list_detail_and_removal_flow_through_scheduler() {
        let state = standalone_state();
        let node = local_node_name().to_string();

        let (status, empty) =
            read(list_delayed_messages(State(state.clone()), PageParams::default()).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(empty["data"], serde_json::json!([]));
        assert_eq!(empty["meta"]["count"], serde_json::json!(0));

        let entry = schedule(&state, "conf/delayed/w1-a", 600);
        let (status, listed) =
            read(list_delayed_messages(State(state.clone()), PageParams::default()).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(listed["meta"]["count"], serde_json::json!(1));
        let row = &listed["data"][0];
        assert_eq!(row["topic"], serde_json::json!("conf/delayed/w1-a"));
        assert_eq!(row["qos"], serde_json::json!(1));
        assert_eq!(row["node"], serde_json::json!(node));
        assert_eq!(row["msgid"], serde_json::json!(entry.id));
        assert_eq!(row["from_clientid"], serde_json::json!("conf-delayed-c1"));
        assert_eq!(row["from_username"], serde_json::json!("alice"));
        assert!(row["delayed_interval"].as_u64().expect("interval") >= 1);
        assert!(row["delayed_remaining"].as_u64().expect("remaining") >= 1);
        assert!(row["expected_at"].as_str().is_some());
        assert!(row["publish_at"].as_str().is_some());
        assert!(row["payload"].as_str().is_some());
        assert!(row.get("node").and_then(|v| v.as_str()).is_some());

        // Detail carries the same row without the payload.
        let (status, detail) = read(
            get_delayed_message(
                State(state.clone()),
                Path((node.clone(), entry.id.to_string())),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(detail["msgid"], serde_json::json!(entry.id));
        assert!(detail.get("payload").is_none());
        assert_eq!(detail["topic"], row["topic"]);

        // Unknown node, bad id and missing id fail with the codes.
        let (status, err) = read(
            get_delayed_message(
                State(state.clone()),
                Path(("no-such-node".to_string(), entry.id.to_string())),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("INVALID_NODE"));
        let (status, err) = read(
            get_delayed_message(
                State(state.clone()),
                Path((node.clone(), "abc".to_string())),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("MESSAGE_ID_SCHEMA_ERROR"));
        let (status, err) = read(
            get_delayed_message(
                State(state.clone()),
                Path((node.clone(), "424242".to_string())),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(err["code"], serde_json::json!("MESSAGE_ID_NOT_FOUND"));

        // Removal stops the delivery: the wheel forgets the id.
        let response = delete_delayed_message(
            State(state.clone()),
            Path((node.clone(), entry.id.to_string())),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(state.delayed.get(entry.id).is_none());
        let (status, err) = read(
            get_delayed_message(
                State(state.clone()),
                Path((node.clone(), entry.id.to_string())),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(err["code"], serde_json::json!("MESSAGE_ID_NOT_FOUND"));
        let response =
            delete_delayed_message(State(state.clone()), Path((node, entry.id.to_string()))).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn topic_removal_drops_only_that_topic() {
        let state = standalone_state();
        schedule(&state, "conf/delayed/w2-a", 600);
        schedule(&state, "conf/delayed/w2-b", 600);

        let response =
            delete_delayed_by_topic(State(state.clone()), Path("conf/delayed/w2-a".to_string()))
                .await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let (status, listed) =
            read(list_delayed_messages(State(state.clone()), PageParams::default()).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(listed["meta"]["count"], serde_json::json!(1));
        assert_eq!(
            listed["data"][0]["topic"],
            serde_json::json!("conf/delayed/w2-b")
        );

        let (status, err) = read(
            delete_delayed_by_topic(State(state.clone()), Path("conf/delayed/w2-a".to_string()))
                .await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(err["code"], serde_json::json!("MESSAGE_NOT_FOUND"));
        let (status, err) =
            read(delete_delayed_by_topic(State(state.clone()), Path("#".to_string())).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("INVALID_TOPIC_NAME"));
    }

    #[tokio::test]
    async fn large_payload_renders_marker_not_bytes() {
        let state = standalone_state();
        let scheduler = std::sync::Arc::clone(&state.delayed);
        let now = crate::v5::retainer::now_ms();
        let entry = broker_delayed::DelayedEntry {
            id: scheduler.next_id(),
            deliver_at_ms: now + 600_000,
            publish_at_ms: now,
            topic: "conf/delayed/w-big".to_string(),
            qos: 0,
            retain: false,
            payload: vec![b'x'; 3000],
            publisher: None,
            username: None,
            publisher_tenant: broker_session::tenant::DEFAULT_TENANT_ID.to_string(),
        };
        scheduler
            .persist_and_schedule(entry, 600)
            .expect("fits the test scheduler");
        let (status, listed) =
            read(list_delayed_messages(State(state.clone()), PageParams::default()).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            listed["data"][0]["payload"],
            serde_json::json!("PAYLOAD_TO_LARGE")
        );
    }
}
