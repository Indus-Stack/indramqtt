//! Configuration groups for the v5 management API.
//!
//! Covers `GET /configs`, `PUT /configs`, the documented
//! `GET`/`PUT /configs/*` roots and `POST /configs_reset/{rootname}`.
//! Reads clone small snapshots under short locks. Writes validate
//! fully before anything applies, then persist through the registry.
//! Publish and deliver never touch this module.
//!
//! Field map. Each line names the broker setting behind one documented
//! field. Reads omit other fields. Writes refuse other fields.
//! - `mqtt.max_inflight`: session QoS 1 window
//!   (`crates/broker-session/src/lib.rs`, `SessionManager`). New
//!   sessions inherit it.
//! - `mqtt.max_mqueue_len`: session offline cap (`None` is the
//!   documented `infinity`). New sessions inherit it.
//! - `mqtt.max_topic_alias`: session inbound alias cap, advertised in
//!   CONNACK. New sessions inherit it.
//! - `mqtt.keepalive_multiplier`: fixed at 1.5 by the native listener
//!   (`crates/broker-node/src/native.rs`, keepalive deadline).
//! - `force_gc`, `force_shutdown`, `durable_sessions`: `enable: false`.
//!   The broker has no such function.
//! - `alarm.size_limit`: true alarm cap (`v5/alarms.rs`, `MAX_ALARMS`).
//!   All other static roots report `{}`.
// TODO(parity): does the edge keepalive path use the same 1.5
// multiplier as the native listener? The rulebook does not decide it;
// reads report the native value until the checker pins the edge one.

use std::collections::HashMap;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;

use crate::node_scope::local_node_name;
use crate::ApiState;
use broker_config::{ConfigRoot, GlobalZoneConf, MAX_GLOBAL_ZONE_MQUEUE_LEN};

use super::alarms::MAX_ALARMS;

/// Largest accepted configs body (64 KiB). Reason: zone bodies hold a
/// few small fields; larger bodies are rejected before parsing so one
/// request cannot balloon management memory.
const MAX_BODY_LEN: usize = 64 * 1024;

/// Keepalive multiplier enforced by the native listener.
const KEEPALIVE_MULTIPLIER: f64 = 1.5;

/// Routed configs roots served by `GET /configs` without a key.
const ROUTED_ROOTS: [&str; 7] = [
    "broker",
    "log",
    "sysmon",
    "sys_topics",
    "alarm",
    "dashboard",
    "global_zone",
];

/// Resettable root names: the routed roots. Dropped groups (`a2a_registry`,
/// `file_transfer`) and the zone (not listed) answer `NOT_FOUND`.
const RESET_ROOTS: [&str; 6] = [
    "broker",
    "log",
    "sysmon",
    "sys_topics",
    "alarm",
    "dashboard",
];

/// Render the live default-zone view: enforced session knobs plus the
/// fixed disabled blocks. Reads the session manager, not the registry,
/// so the answer is the enforced value.
fn global_zone_view(state: &ApiState) -> serde_json::Value {
    let max_inflight = state.sessions.max_qos1_inflight();
    let max_mqueue = match state.sessions.max_offline_queue() {
        Some(limit) => serde_json::json!(limit),
        None => serde_json::json!("infinity"),
    };
    serde_json::json!({
        "mqtt": {
            "max_inflight": max_inflight,
            "max_mqueue_len": max_mqueue,
            "max_topic_alias": state.sessions.max_topic_alias(),
            "keepalive_multiplier": KEEPALIVE_MULTIPLIER,
        },
        "force_gc": {"enable": false},
        "force_shutdown": {"enable": false},
        "durable_sessions": {"enable": false},
    })
}

/// Render one static root. `global_zone` reads live state; `alarm`
/// reports its true cap; `dashboard` reports the true initial admin
/// password (`public`, the value that seeds the admin record); every
/// other routed root has no broker setting and reads as an empty object.
fn root_body(state: &ApiState, root: &str) -> Option<serde_json::Value> {
    match root {
        "global_zone" => Some(global_zone_view(state)),
        "alarm" => Some(serde_json::json!({"size_limit": MAX_ALARMS})),
        "dashboard" => Some(serde_json::json!({"default_password": "public"})),
        "broker" | "log" | "sysmon" | "sys_topics" => Some(serde_json::json!({})),
        _ => None,
    }
}

/// Check the optional `node` query: absent or local passes, anything
/// else is the documented `BAD_NODE` failure.
fn check_node(state: &ApiState, query: &HashMap<String, String>) -> Result<(), Box<Response>> {
    let Some(wanted) = query.get("node") else {
        return Ok(());
    };
    if wanted == local_node_name() || wanted == &state.node_id {
        return Ok(());
    }
    Err(Box::new(
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "BAD_NODE",
                "message": format!("node not found: {wanted}"),
            })),
        )
            .into_response(),
    ))
}

/// `GET /configs`: one root by `key`, or every routed root as a map.
///
/// Unknown keys fail closed with the documented `NOT_FOUND` shape.
pub async fn get_configs(
    State(state): State<ApiState>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if let Err(error) = check_node(&state, &query) {
        return *error;
    }
    match query.get("key") {
        Some(key) => match root_body(&state, key) {
            Some(body) => (StatusCode::OK, Json(body)).into_response(),
            None => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "code": "NOT_FOUND",
                    "message": format!("config not found: {key}"),
                })),
            )
                .into_response(),
        },
        None => {
            let mut map = serde_json::Map::new();
            for root in ROUTED_ROOTS {
                if let Some(body) = root_body(&state, root) {
                    map.insert(root.to_string(), body);
                }
            }
            (StatusCode::OK, Json(serde_json::Value::Object(map))).into_response()
        }
    }
}

/// `GET /configs/global_zone`: the live default-zone view.
pub async fn get_global_zone(State(state): State<ApiState>) -> Response {
    (StatusCode::OK, Json(global_zone_view(&state))).into_response()
}

/// Parse one `mqtt.max_mqueue_len` value: integer or `infinity`.
fn parse_mqueue_len(raw: &serde_json::Value) -> Result<Option<u32>, String> {
    if let Some(text) = raw.as_str() {
        if text == "infinity" {
            return Ok(None);
        }
        return Err("field `mqtt.max_mqueue_len` must be an integer or `infinity`".to_string());
    }
    match raw.as_u64() {
        Some(value) if value >= 1 && value <= u64::from(MAX_GLOBAL_ZONE_MQUEUE_LEN) => {
            Ok(Some(value as u32))
        }
        _ => Err(format!(
            "field `mqtt.max_mqueue_len` holds {raw}, must be 1..={} or `infinity`",
            MAX_GLOBAL_ZONE_MQUEUE_LEN
        )),
    }
}

/// Apply one validated `global_zone` object onto `current`.
///
/// Mapped fields validate and stage; fixed fields must match the served
/// value; omitted-optional fields and unknown fields fail closed with
/// the documented `UPDATE_FAILED` shape.
fn apply_global_zone(
    mut current: GlobalZoneConf,
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<GlobalZoneConf, String> {
    if obj.is_empty() {
        return Err("global zone body must not be empty".to_string());
    }
    for key in obj.keys() {
        match key.as_str() {
            "mqtt" | "force_gc" | "force_shutdown" | "durable_sessions" | "flapping_detect" => {}
            _ => {
                return Err(format!("field `{key}` is not available in this broker"));
            }
        }
    }
    if let Some(raw) = obj.get("mqtt") {
        let map = raw
            .as_object()
            .ok_or_else(|| "field `mqtt` must be an object".to_string())?;
        for key in map.keys() {
            match key.as_str() {
                "max_inflight" | "max_mqueue_len" | "max_topic_alias" | "keepalive_multiplier" => {}
                _ => {
                    return Err(format!(
                        "field `mqtt.{key}` is not available in this broker"
                    ));
                }
            }
        }
        if let Some(raw) = map.get("max_inflight") {
            match raw.as_u64() {
                Some(value) if (1..=65_535).contains(&value) => {
                    current.max_inflight = value as u32;
                }
                _ => {
                    return Err(format!(
                        "field `mqtt.max_inflight` holds {raw}, must be 1..=65535"
                    ));
                }
            }
        }
        if let Some(raw) = map.get("max_mqueue_len") {
            current.max_mqueue_len = parse_mqueue_len(raw)?;
        }
        if let Some(raw) = map.get("max_topic_alias") {
            match raw.as_u64() {
                Some(value) if value <= 65_535 => {
                    current.max_topic_alias = value as u16;
                }
                _ => {
                    return Err(format!(
                        "field `mqtt.max_topic_alias` holds {raw}, must be 0..=65535"
                    ));
                }
            }
        }
        if let Some(raw) = map.get("keepalive_multiplier") {
            let matches = raw.as_f64().map(|item| item == KEEPALIVE_MULTIPLIER);
            if matches != Some(true) {
                return Err(format!(
                    "field `mqtt.keepalive_multiplier` holds {raw}, must be {KEEPALIVE_MULTIPLIER}"
                ));
            }
        }
    }
    for block in ["force_gc", "force_shutdown", "durable_sessions"] {
        if let Some(raw) = obj.get(block) {
            let map = raw
                .as_object()
                .ok_or_else(|| format!("field `{block}` must be an object"))?;
            if map.get("enable") == Some(&serde_json::Value::Bool(true)) {
                return Err(format!(
                    "field `{block}.enable` is not available in this broker"
                ));
            }
        }
    }
    if let Some(raw) = obj.get("flapping_detect") {
        let map = raw
            .as_object()
            .ok_or_else(|| "field `flapping_detect` must be an object".to_string())?;
        for key in ["by_clientid", "by_peerhost", "by_username"] {
            match map.get(key) {
                None => {}
                Some(value) => {
                    let text = value.as_str().ok_or_else(|| {
                        format!("field `flapping_detect.{key}` is not available in this broker")
                    })?;
                    if text != "none" {
                        return Err(format!(
                            "field `flapping_detect.{key}` is not available in this broker"
                        ));
                    }
                }
            }
        }
        for key in map.keys() {
            if !["by_clientid", "by_peerhost", "by_username"].contains(&key.as_str()) {
                return Err(format!(
                    "field `flapping_detect.{key}` is not available in this broker"
                ));
            }
        }
    }
    Ok(current)
}

/// Commit one validated zone: apply to the live session manager first
/// (new sessions inherit it), then persist through the registry. A save
/// failure answers 500 with sessions and registry already agreeing, so
/// the loss is the disk write only, never silent.
fn commit_zone(state: &ApiState, next: GlobalZoneConf) -> Result<(), Box<Response>> {
    if let Err(error) = next.validate() {
        return Err(Box::new(update_failed(format!(
            "invalid global zone: {error}"
        ))));
    }
    state
        .sessions
        .set_max_qos1_inflight(next.max_inflight as usize);
    state
        .sessions
        .set_max_offline_queue(next.max_mqueue_len.map(|len| len as usize));
    state.sessions.set_max_topic_alias(next.max_topic_alias);
    if let Err(error) = state.config.commit(ConfigRoot::GlobalZone(next)) {
        return Err(Box::new(update_failed(format!(
            "invalid global zone: {error}"
        ))));
    }
    if let Err(error) = state.config.save() {
        return Err(Box::new(
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "code": "INTERNAL_ERROR",
                    "message": format!("cannot persist global zone: {error}"),
                })),
            )
                .into_response(),
        ));
    }
    Ok(())
}

/// `PUT /configs/global_zone`: validated merge over the mapped knobs.
///
/// Answers 200 with the live view. Anything the broker cannot honour
/// fails closed with the documented `UPDATE_FAILED` shape and leaves
/// the stored knobs untouched.
pub async fn put_global_zone(State(state): State<ApiState>, body: Bytes) -> Response {
    if body.len() > MAX_BODY_LEN {
        return update_failed("global zone body is too large".to_string());
    }
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(item) => item,
        Err(error) => return update_failed(format!("invalid global zone body: {error}")),
    };
    let obj = match value.as_object() {
        Some(map) => map,
        None => return update_failed("global zone body must be a JSON object".to_string()),
    };
    let current = state.config.snapshot().global_zone.clone();
    let next = match apply_global_zone(current, obj) {
        Ok(item) => item,
        Err(reason) => return update_failed(reason),
    };
    if let Err(error) = commit_zone(&state, next) {
        return *error;
    }
    (StatusCode::OK, Json(global_zone_view(&state))).into_response()
}

/// Answer one static root read.
macro_rules! static_get {
    ($name:ident, $root:expr) => {
        /// Serve the documented shape for a root without broker
        /// settings: the true empty or capped state, never invented.
        pub async fn $name(State(state): State<ApiState>) -> Response {
            match root_body(&state, $root) {
                Some(body) => (StatusCode::OK, Json(body)).into_response(),
                None => (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({
                        "code": "NOT_FOUND",
                        "message": format!("config not found: {}", $root),
                    })),
                )
                    .into_response(),
            }
        }
    };
}

static_get!(get_broker, "broker");
static_get!(get_log, "log");
static_get!(get_sysmon, "sysmon");
static_get!(get_sys_topics, "sys_topics");
static_get!(get_alarm, "alarm");
static_get!(get_dashboard, "dashboard");

/// Refuse a write to a root without broker settings with the documented
/// `UPDATE_FAILED` shape. The function is not available in this broker.
macro_rules! static_put {
    ($name:ident, $root:expr) => {
        /// Refuse a write to a root the broker does not serve.
        pub async fn $name(State(_state): State<ApiState>, body: Bytes) -> Response {
            if body.len() > MAX_BODY_LEN {
                return update_failed(format!("{} body is too large", $root));
            }
            update_failed(format!("field `{}` is not available in this broker", $root))
        }
    };
}

static_put!(put_broker, "broker");
static_put!(put_log, "log");
static_put!(put_sysmon, "sysmon");
static_put!(put_sys_topics, "sys_topics");
static_put!(put_alarm, "alarm");
static_put!(put_dashboard, "dashboard");

/// `PUT /configs`: merge (default) or replace over the owned roots.
///
/// The body is a JSON object keyed by root, as the console sends it.
/// Roots without broker settings fail closed with `UPDATE_FAILED` and
/// nothing applies. Answers 200 with no body on success.
pub async fn put_configs(
    State(state): State<ApiState>,
    Query(query): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    if let Err(error) = check_node(&state, &query) {
        return *error;
    }
    let replace = match query.get("mode").map(String::as_str) {
        None | Some("merge") => false,
        Some("replace") => true,
        Some(other) => {
            return update_failed(format!(
                "mode holds {other:?}, must be `merge` or `replace`"
            ));
        }
    };
    if body.len() > MAX_BODY_LEN {
        return update_failed("configs body is too large".to_string());
    }
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(item) => item,
        Err(error) => return update_failed(format!("invalid configs body: {error}")),
    };
    let obj = match value.as_object() {
        Some(map) => map.clone(),
        None => return update_failed("configs body must be a JSON object".to_string()),
    };
    if obj.is_empty() {
        return update_failed("configs body must not be empty".to_string());
    }
    // Validate everything before anything applies.
    let mut zone_next: Option<GlobalZoneConf> = None;
    for key in obj.keys() {
        match key.as_str() {
            "global_zone" => {
                let raw = &obj[key];
                let map = match raw.as_object() {
                    Some(item) => item,
                    None => {
                        return update_failed("field `global_zone` must be an object".to_string());
                    }
                };
                let base = if replace {
                    GlobalZoneConf::default()
                } else {
                    state.config.snapshot().global_zone.clone()
                };
                match apply_global_zone(base, map) {
                    Ok(item) => zone_next = Some(item),
                    Err(reason) => return update_failed(reason),
                }
            }
            _ => {
                return update_failed(format!("field `{key}` is not available in this broker"));
            }
        }
    }
    if let Some(next) = zone_next {
        if let Err(error) = commit_zone(&state, next) {
            return *error;
        }
    }
    StatusCode::OK.into_response()
}

/// `POST /configs_reset/{rootname}`: reset one root to its defaults.
///
/// Roots without stored entries reset as a no-op success. A `conf_path`
/// naming an entry with no default fails with the documented
/// `NO_DEFAULT_VALUE` shape; an unknown root fails with `NOT_FOUND`.
pub async fn post_reset(
    State(_state): State<ApiState>,
    Path(rootname): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if !RESET_ROOTS.contains(&rootname.as_str()) {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "code": "NOT_FOUND",
                "message": format!("config not found: {rootname}"),
            })),
        )
            .into_response();
    }
    match query.get("conf_path") {
        None => StatusCode::OK.into_response(),
        Some(path) => {
            let known = rootname == "alarm" && path == "size_limit";
            if known {
                StatusCode::OK.into_response()
            } else {
                (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "code": "NO_DEFAULT_VALUE",
                        "message": format!("no default value for `{rootname}.{path}`"),
                    })),
                )
                    .into_response()
            }
        }
    }
}

fn update_failed(reason: String) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({
            "code": "UPDATE_FAILED",
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
            .expect("configs body is small and readable");
        if bytes.is_empty() {
            return (status, serde_json::Value::Null);
        }
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("configs body is JSON");
        (status, body)
    }

    fn empty_query() -> Query<HashMap<String, String>> {
        Query(HashMap::new())
    }

    #[tokio::test]
    async fn global_zone_reports_live_session_knobs() {
        let state = standalone_state();
        let (status, body) = parts(get_global_zone(State(state)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["mqtt"]["max_inflight"], serde_json::json!(100));
        assert_eq!(body["mqtt"]["max_mqueue_len"], serde_json::json!(10_000));
        assert_eq!(body["mqtt"]["max_topic_alias"], serde_json::json!(10));
        assert_eq!(body["mqtt"]["keepalive_multiplier"], serde_json::json!(1.5));
        assert_eq!(body["force_gc"]["enable"], serde_json::json!(false));
        assert_eq!(body["force_shutdown"]["enable"], serde_json::json!(false));
        assert_eq!(body["durable_sessions"]["enable"], serde_json::json!(false));
    }

    #[tokio::test]
    async fn put_global_zone_applies_to_live_sessions() {
        let state = standalone_state();
        let raw = r#"{"mqtt":{"max_inflight":64,"max_mqueue_len":"infinity","max_topic_alias":5}}"#;
        let (status, body) =
            parts(put_global_zone(State(state.clone()), Bytes::from_static(raw.as_bytes())).await)
                .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["mqtt"]["max_inflight"], serde_json::json!(64));
        assert_eq!(
            body["mqtt"]["max_mqueue_len"],
            serde_json::json!("infinity")
        );
        assert_eq!(body["mqtt"]["max_topic_alias"], serde_json::json!(5));
        // The broker session directory enforces the new knobs.
        assert_eq!(state.sessions.max_qos1_inflight(), 64);
        assert_eq!(state.sessions.max_offline_queue(), None);
        assert_eq!(state.sessions.max_topic_alias(), 5);
        // The registry snapshot carries the same knobs.
        let stored = state.config.snapshot().global_zone.clone();
        assert_eq!(stored.max_inflight, 64);
        assert_eq!(stored.max_mqueue_len, None);
        assert_eq!(stored.max_topic_alias, 5);
    }

    #[tokio::test]
    async fn put_global_zone_refuses_unmapped_fields() {
        let state = standalone_state();
        for raw in [
            r#"{"mqtt":{"max_packet_size":"1MB"}}"#,
            r#"{"mqtt":{"max_inflight":0}}"#,
            r#"{"mqtt":{"max_inflight":70000}}"#,
            r#"{"mqtt":{"max_topic_alias":70000}}"#,
            r#"{"mqtt":{"max_mqueue_len":0}}"#,
            r#"{"mqtt":{"max_mqueue_len":"huge"}}"#,
            r#"{"mqtt":{"keepalive_multiplier":2.0}}"#,
            r#"{"force_gc":{"enable":true}}"#,
            r#"{"durable_sessions":{"enable":true}}"#,
            r#"{"flapping_detect":{"by_clientid":{"threshold":5}}}"#,
            r#"{"stats":{"enable":true}}"#,
            r#"{}"#,
        ] {
            let (status, body) =
                parts(put_global_zone(State(state.clone()), Bytes::from(raw.to_string())).await)
                    .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "body {raw}");
            assert_eq!(body["code"], serde_json::json!("UPDATE_FAILED"));
        }
        // A refused write leaves the live knobs untouched.
        assert_eq!(state.sessions.max_qos1_inflight(), 100);
        assert_eq!(state.sessions.max_topic_alias(), 10);
    }

    #[tokio::test]
    async fn zone_knobs_survive_registry_reload() {
        let dir = std::env::temp_dir().join(format!(
            "indramqtt-zone-{}-{}",
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
            .commit(ConfigRoot::GlobalZone(GlobalZoneConf {
                max_inflight: 48,
                max_mqueue_len: None,
                max_topic_alias: 7,
            }))
            .expect("valid zone commits");
        registry.save().expect("registry saves");
        drop(registry);
        let again = broker_config::ConfigRegistry::load(&dir).expect("stored registry reloads");
        let zone = again.snapshot().global_zone.clone();
        assert_eq!(zone.max_inflight, 48);
        assert_eq!(zone.max_mqueue_len, None);
        assert_eq!(zone.max_topic_alias, 7);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn static_roots_read_empty_and_refuse_writes() {
        let state = standalone_state();
        let (status, body) = parts(get_sysmon(State(state.clone())).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({}));
        let (status, body) = parts(get_alarm(State(state.clone())).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({"size_limit": 1000}));
        let (status, body) =
            parts(put_sysmon(State(state.clone()), Bytes::from_static(b"{}")).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], serde_json::json!("UPDATE_FAILED"));
    }

    #[tokio::test]
    async fn generic_configs_selects_roots_and_rejects_unknown() {
        let state = standalone_state();
        let mut keyed = HashMap::new();
        keyed.insert("key".to_string(), "sysmon".to_string());
        let (status, body) = parts(get_configs(State(state.clone()), Query(keyed)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, serde_json::json!({}));
        let (status, body) = parts(get_configs(State(state.clone()), empty_query()).await).await;
        assert_eq!(status, StatusCode::OK);
        for root in ROUTED_ROOTS {
            assert!(body.get(root).is_some(), "root {root} is present");
        }
        let mut bad = HashMap::new();
        bad.insert("key".to_string(), "bogus".to_string());
        let (status, body) = parts(get_configs(State(state.clone()), Query(bad)).await).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
        let mut node = HashMap::new();
        node.insert("node".to_string(), "no-such-node".to_string());
        let (status, body) = parts(get_configs(State(state.clone()), Query(node)).await).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body["code"], serde_json::json!("BAD_NODE"));
    }

    #[tokio::test]
    async fn generic_put_applies_zone_and_refuses_other_roots() {
        let state = standalone_state();
        let raw = r#"{"global_zone":{"mqtt":{"max_topic_alias":9}}}"#;
        let (status, _) = parts(
            put_configs(
                State(state.clone()),
                empty_query(),
                Bytes::from_static(raw.as_bytes()),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(state.sessions.max_topic_alias(), 9);
        let raw = r#"{"log":{}}"#;
        let (status, body) = parts(
            put_configs(
                State(state.clone()),
                empty_query(),
                Bytes::from_static(raw.as_bytes()),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], serde_json::json!("UPDATE_FAILED"));
        // A refused generic write leaves the live knobs untouched.
        assert_eq!(state.sessions.max_topic_alias(), 9);
    }

    #[tokio::test]
    async fn reset_answers_documented_shapes() {
        let state = standalone_state();
        let (status, _) = parts(
            post_reset(
                State(state.clone()),
                Path("dashboard".to_string()),
                empty_query(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) = parts(
            post_reset(
                State(state.clone()),
                Path("alarm".to_string()),
                Query(HashMap::from([(
                    "conf_path".to_string(),
                    "size_limit".to_string(),
                )])),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, body) = parts(
            post_reset(
                State(state.clone()),
                Path("sysmon".to_string()),
                Query(HashMap::from([(
                    "conf_path".to_string(),
                    "vm.large_heap".to_string(),
                )])),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], serde_json::json!("NO_DEFAULT_VALUE"));
        let (status, body) = parts(
            post_reset(
                State(state.clone()),
                Path("global_zone".to_string()),
                empty_query(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
    }
}
