//! Exporter settings and text exposition for the v5 management API.
//!
//! Covers `GET /prometheus` (stored settings read), `PUT /prometheus`
//! (validated settings replace) and `GET /prometheus/*` (text
//! exposition of the same true counters as `GET /metrics` and
//! `GET /stats`). Reads cost one snapshot per request. Writes validate
//! fully before anything applies, then persist through the config
//! registry. Publish and deliver never touch this module.
//!
//! Store bounds (stated here and enforced by the types): one settings
//! struct (a few dozen small strings, each capped by
//! [`broker_config::MAX_PROMETHEUS_TEXT_LEN`]) plus at most
//! [`broker_config::MAX_PROMETHEUS_HEADERS`] header entries. The
//! exposition renders one line per documented key from a single
//! snapshot, so one scrape cannot balloon management memory.
//!
//! The broker stores the settings and serves the exposition from live
//! counters. It runs no push client: `push_gateway.enable` only stores
//! the operator intent. Query keys on the exposition (`mode`, `ns`,
//! `only_global`) are accepted and ignored: one node holds every
//! counter, so each mode reads the same values.

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use serde::Deserialize;

use broker_config::{
    ConfigRegistry, PrometheusCollector, PrometheusConf, PrometheusPushGatewayConf,
};

use crate::ApiState;

/// Stored exporter settings behind one short lock plus the registry.
///
/// The kernel shares the same instance through `ApiState`, so reads
/// observe the persisted settings from boot. Management-plane only.
pub struct PrometheusStore {
    inner: std::sync::RwLock<PrometheusConf>,
    registry: std::sync::RwLock<Option<Arc<ConfigRegistry>>>,
    save_lock: std::sync::Mutex<()>,
}

impl PrometheusStore {
    /// Default settings with no registry (memory-only until seeded).
    pub fn new() -> Self {
        Self {
            inner: std::sync::RwLock::new(PrometheusConf::default()),
            registry: std::sync::RwLock::new(None),
            save_lock: std::sync::Mutex::new(()),
        }
    }

    /// Seed from the registry snapshot and persist later mutations.
    pub fn from_registry(registry: &Arc<ConfigRegistry>) -> Self {
        let store = Self::new();
        *store
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            registry.snapshot().prometheus.clone();
        *store
            .registry
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::clone(registry));
        store
    }

    /// Snapshot the current settings.
    pub fn get(&self) -> PrometheusConf {
        self.inner
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Replace the settings after full validation, then persist. The
    /// caller validates first; this only swaps and runs the commit.
    fn replace(&self, next: PrometheusConf) -> Result<PrometheusConf, broker_config::ConfigError> {
        *self
            .inner
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = next.clone();
        self.persist()?;
        Ok(next)
    }

    /// Commit the stored root and atomically save it. Any commit or
    /// save failure surfaces so callers answer 500 and the loss is
    /// never silent. Stores without a registry stay memory-only.
    fn persist(&self) -> Result<(), broker_config::ConfigError> {
        let registry = self
            .registry
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let Some(registry) = registry else {
            return Ok(());
        };
        let _guard = self
            .save_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let conf = self.get();
        registry.commit_prometheus(conf)?;
        registry.save()?;
        Ok(())
    }
}

impl Default for PrometheusStore {
    fn default() -> Self {
        Self::new()
    }
}

fn collector_name(value: &PrometheusCollector) -> &'static str {
    match value {
        PrometheusCollector::Enabled => "enabled",
        PrometheusCollector::Disabled => "disabled",
    }
}

/// Render the stored settings in the documented recommend shape. The
/// limiter object appears only while a limit is stored; every other
/// documented field is always present.
fn settings_json(conf: &PrometheusConf) -> serde_json::Value {
    let collectors = &conf.collectors;
    let gateway = &conf.push_gateway;
    let mut body = serde_json::json!({
        "collectors": {
            "mnesia": collector_name(&collectors.mnesia),
            "vm_dist": collector_name(&collectors.vm_dist),
            "vm_memory": collector_name(&collectors.vm_memory),
            "vm_msacc": collector_name(&collectors.vm_msacc),
            "vm_statistics": collector_name(&collectors.vm_statistics),
            "vm_system_info": collector_name(&collectors.vm_system_info),
        },
        "enable_basic_auth": conf.enable_basic_auth,
        "latency_buckets": conf.latency_buckets,
        "push_gateway": {
            "enable": gateway.enable,
            "headers": gateway.headers,
            "interval": gateway.interval,
            "job_name": gateway.job_name,
            "method": gateway.method,
            "url": gateway.url,
        },
    });
    if let Some(limiter) = &conf.namespaced_metrics_limiter {
        let entry = match &limiter.rate {
            Some(rate) => serde_json::json!({"rate": rate}),
            None => serde_json::json!({}),
        };
        if let Some(object) = body.as_object_mut() {
            object.insert("namespaced_metrics_limiter".to_string(), entry);
        }
    }
    body
}

/// `GET /prometheus`: the stored exporter settings in the documented
/// recommend shape. Always 200. Management-plane only: one small
/// snapshot clone per request.
pub async fn get_prometheus(State(state): State<ApiState>) -> Response {
    (StatusCode::OK, Json(settings_json(&state.prometheus.get()))).into_response()
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

/// Map a store persistence failure onto a 500: the in-memory mutation
/// applied but the commit or atomic save failed, so the loss must never
/// be silent.
fn persist_error(error: broker_config::ConfigError) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({
            "code": "INTERNAL_ERROR",
            "message": format!("cannot persist exporter settings: {error}"),
        })),
    )
        .into_response()
}

/// Legacy collector keys with their target field, in file order.
const LEGACY_COLLECTORS: [(&str, &str); 6] = [
    ("mnesia_collector", "mnesia"),
    ("vm_dist_collector", "vm_dist"),
    ("vm_memory_collector", "vm_memory"),
    ("vm_msacc_collector", "vm_msacc"),
    ("vm_statistics_collector", "vm_statistics"),
    ("vm_system_info_collector", "vm_system_info"),
];

/// True while the body carries at least one legacy-only key. The
/// top-level `headers`, `interval` and `job_name` keys exist only in
/// the legacy shape; the recommend shape nests them in `push_gateway`.
fn has_legacy_keys(obj: &serde_json::Map<String, serde_json::Value>) -> bool {
    [
        "enable",
        "push_gateway_server",
        "headers",
        "interval",
        "job_name",
    ]
    .iter()
    .any(|key| obj.contains_key(*key))
        || LEGACY_COLLECTORS
            .iter()
            .any(|(key, _)| obj.contains_key(*key))
}

/// True while the body carries at least one recommend key.
fn has_recommend_keys(obj: &serde_json::Map<String, serde_json::Value>) -> bool {
    [
        "collectors",
        "enable_basic_auth",
        "push_gateway",
        "latency_buckets",
        "namespaced_metrics_limiter",
    ]
    .iter()
    .any(|key| obj.contains_key(*key))
}

fn parse_collector(field: &str, raw: &serde_json::Value) -> Result<PrometheusCollector, String> {
    match raw.as_str() {
        Some("enabled") => Ok(PrometheusCollector::Enabled),
        Some("disabled") => Ok(PrometheusCollector::Disabled),
        _ => Err(format!(
            "field `collectors.{field}` must be `enabled` or `disabled`"
        )),
    }
}

fn parse_text(field: &str, raw: &serde_json::Value) -> Result<String, String> {
    raw.as_str()
        .map(str::to_string)
        .ok_or_else(|| format!("field `{field}` must be a string"))
}

/// Apply the recommend fields present in `obj` onto `next`. Missing
/// fields keep their values. Unknown fields are ignored.
fn apply_recommend(
    obj: &serde_json::Map<String, serde_json::Value>,
    next: &mut PrometheusConf,
) -> Result<(), String> {
    if let Some(raw) = obj.get("collectors") {
        let map = raw
            .as_object()
            .ok_or_else(|| "field `collectors` must be an object".to_string())?;
        for (field, slot) in [
            ("mnesia", &mut next.collectors.mnesia),
            ("vm_dist", &mut next.collectors.vm_dist),
            ("vm_memory", &mut next.collectors.vm_memory),
            ("vm_msacc", &mut next.collectors.vm_msacc),
            ("vm_statistics", &mut next.collectors.vm_statistics),
            ("vm_system_info", &mut next.collectors.vm_system_info),
        ] {
            if let Some(raw) = map.get(field) {
                *slot = parse_collector(field, raw)?;
            }
        }
    }
    if let Some(raw) = obj.get("enable_basic_auth") {
        next.enable_basic_auth = raw
            .as_bool()
            .ok_or_else(|| "field `enable_basic_auth` must be a boolean".to_string())?;
    }
    if let Some(raw) = obj.get("latency_buckets") {
        next.latency_buckets = parse_text("latency_buckets", raw)?;
    }
    if let Some(raw) = obj.get("push_gateway") {
        let map = raw
            .as_object()
            .ok_or_else(|| "field `push_gateway` must be an object".to_string())?;
        apply_push_gateway(map, &mut next.push_gateway)?;
    }
    if let Some(raw) = obj.get("namespaced_metrics_limiter") {
        if raw.is_null() {
            next.namespaced_metrics_limiter = None;
        } else {
            let map = raw.as_object().ok_or_else(|| {
                "field `namespaced_metrics_limiter` must be an object or null".to_string()
            })?;
            let rate = match map.get("rate") {
                None => None,
                Some(raw) => Some(parse_text("namespaced_metrics_limiter.rate", raw)?),
            };
            next.namespaced_metrics_limiter =
                Some(broker_config::PrometheusRateLimiterConf { rate });
        }
    }
    Ok(())
}

/// Apply the push target fields present in `map`. Missing fields keep
/// their values. Unknown fields are ignored.
fn apply_push_gateway(
    map: &serde_json::Map<String, serde_json::Value>,
    next: &mut PrometheusPushGatewayConf,
) -> Result<(), String> {
    if let Some(raw) = map.get("enable") {
        next.enable = raw
            .as_bool()
            .ok_or_else(|| "field `push_gateway.enable` must be a boolean".to_string())?;
    }
    if let Some(raw) = map.get("headers") {
        let headers = raw
            .as_object()
            .ok_or_else(|| "field `push_gateway.headers` must be an object".to_string())?;
        let mut parsed = std::collections::BTreeMap::new();
        for (key, value) in headers {
            let text = value
                .as_str()
                .ok_or_else(|| format!("field `push_gateway.headers.{key}` must be a string"))?;
            parsed.insert(key.clone(), text.to_string());
        }
        next.headers = parsed;
    }
    for (field, slot) in [
        ("interval", &mut next.interval),
        ("job_name", &mut next.job_name),
        ("method", &mut next.method),
        ("url", &mut next.url),
    ] {
        if let Some(raw) = map.get(field) {
            *slot = parse_text(&format!("push_gateway.{field}"), raw)?;
        }
    }
    Ok(())
}

/// Apply the legacy fields present in `obj` onto `next`, mapped onto
/// the recommend shape. Missing fields keep their values.
fn apply_legacy(
    obj: &serde_json::Map<String, serde_json::Value>,
    next: &mut PrometheusConf,
) -> Result<(), String> {
    if let Some(raw) = obj.get("enable") {
        next.push_gateway.enable = raw
            .as_bool()
            .ok_or_else(|| "field `enable` must be a boolean".to_string())?;
    }
    if let Some(raw) = obj.get("headers") {
        let headers = raw
            .as_object()
            .ok_or_else(|| "field `headers` must be an object".to_string())?;
        let mut parsed = std::collections::BTreeMap::new();
        for (key, value) in headers {
            let text = value
                .as_str()
                .ok_or_else(|| format!("field `headers.{key}` must be a string"))?;
            parsed.insert(key.clone(), text.to_string());
        }
        next.push_gateway.headers = parsed;
    }
    if let Some(raw) = obj.get("interval") {
        next.push_gateway.interval = parse_text("interval", raw)?;
    }
    if let Some(raw) = obj.get("job_name") {
        next.push_gateway.job_name = parse_text("job_name", raw)?;
    }
    if let Some(raw) = obj.get("push_gateway_server") {
        next.push_gateway.url = parse_text("push_gateway_server", raw)?;
    }
    // Legacy collector switches map one-to-one onto the recommend
    // switches. Fields absent from the body keep their stored values.
    for (key, field) in LEGACY_COLLECTORS {
        let Some(raw) = obj.get(key) else {
            continue;
        };
        let value = parse_collector(key, raw)?;
        match field {
            "mnesia" => next.collectors.mnesia = value,
            "vm_dist" => next.collectors.vm_dist = value,
            "vm_memory" => next.collectors.vm_memory = value,
            "vm_msacc" => next.collectors.vm_msacc = value,
            "vm_statistics" => next.collectors.vm_statistics = value,
            _ => next.collectors.vm_system_info = value,
        }
    }
    Ok(())
}

/// `PUT /prometheus`: validated settings replace, answered in the
/// documented recommend shape.
///
/// The body is one JSON object in either documented shape (recommend or
/// legacy); a mix of both fails closed. Each present field is validated
/// before anything applies, so one bad field rejects the whole write
/// and leaves the stored values untouched. Missing fields keep their
/// values; unknown fields are ignored. An empty object reads back the
/// current values. Management-plane only.
pub async fn put_prometheus(State(state): State<ApiState>, body: Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => return bad_request(format!("invalid exporter body: {error}")),
    };
    let obj = match value.as_object() {
        Some(obj) => obj,
        None => return bad_request("exporter body must be a JSON object".to_string()),
    };
    let legacy = has_legacy_keys(obj);
    let recommend = has_recommend_keys(obj);
    if legacy && recommend {
        return bad_request(
            "exporter body mixes the recommend and legacy shapes; send one".to_string(),
        );
    }
    if !legacy && !recommend && !obj.is_empty() {
        return bad_request("exporter body matches neither documented shape".to_string());
    }
    let mut next = state.prometheus.get();
    let parsed = if legacy {
        apply_legacy(obj, &mut next)
    } else {
        apply_recommend(obj, &mut next)
    };
    if let Err(reason) = parsed {
        return bad_request(reason);
    }
    if let Err(error) = next.validate() {
        return bad_request(error.to_string());
    }
    match state.prometheus.replace(next.clone()) {
        Ok(_) => (StatusCode::OK, Json(settings_json(&next))).into_response(),
        Err(error) => persist_error(error),
    }
}

/// Accepted exposition query: every key is optional and ignored. One
/// node holds every counter, so each mode reads the same values.
#[derive(Deserialize, Default)]
pub struct PrometheusQuery {
    /// Exposition mode (`node`, `all_nodes_aggregated` or
    /// `all_nodes_unaggregated`). Accepted and ignored.
    #[serde(default)]
    pub mode: Option<String>,
    /// Namespace filter for the data-integration exposition. Accepted
    /// and ignored.
    #[serde(default)]
    pub ns: Option<String>,
    /// Global-only switch for the data-integration exposition.
    /// Accepted and ignored. Text on purpose: a malformed value must
    /// never fail the scrape; unknown query keys are ignored.
    #[serde(default)]
    pub only_global: Option<String>,
}

/// One exposition line: the documented JSON key, its Prometheus name
/// and its value. Counter keys (monotonic `GET /metrics` rows) render
/// with a `_total` suffix; gauge keys (`GET /stats` rows) render bare.
fn exposition_lines(
    rows: &serde_json::Map<String, serde_json::Value>,
    counter: bool,
) -> Vec<String> {
    let mut names: Vec<&String> = rows.keys().collect();
    names.sort();
    let mut out = Vec::with_capacity(rows.len() * 2);
    for key in names {
        let value = rows[key].as_u64().unwrap_or(0);
        let stem = key.replace('.', "_");
        let name = if counter {
            format!("indramqtt_{stem}_total")
        } else {
            format!("indramqtt_{stem}")
        };
        let kind = if counter { "counter" } else { "gauge" };
        out.push(format!("# TYPE {name} {kind}"));
        out.push(format!("{name} {value}"));
    }
    out
}

/// Shared text exposition for every `GET /prometheus/*` stats route:
/// the same true counters as `GET /metrics` (counters) and
/// `GET /stats` (gauges), one line per key. Management-plane only: two
/// bounded snapshot copies per request; nothing here runs on the
/// per-message path.
fn exposition_body(state: &ApiState) -> String {
    let metrics = super::monitoring::metrics_snapshot_for_exposition(state);
    let stats = super::monitoring::stats_snapshot_for_exposition(state);
    let mut text =
        String::from("# Same true counters as GET /metrics and GET /stats, one line per key.\n");
    for line in exposition_lines(&metrics, true) {
        text.push_str(&line);
        text.push('\n');
    }
    for line in exposition_lines(&stats, false) {
        text.push_str(&line);
        text.push('\n');
    }
    text
}

/// Answer one exposition route with the shared body.
fn exposition_response(state: &ApiState) -> Response {
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        exposition_body(state),
    )
        .into_response()
}

/// `GET /prometheus/stats`: text exposition of the node counters and
/// gauges. Always 200 with `text/plain`. Management-plane only.
pub async fn get_prometheus_stats(
    State(state): State<ApiState>,
    Query(_query): Query<PrometheusQuery>,
) -> Response {
    exposition_response(&state)
}

/// `GET /prometheus/auth`: same exposition as `/prometheus/stats`. One
/// node holds every counter, so the area split carries no separate
/// values; the route exists so the documented scrape target answers.
pub async fn get_prometheus_auth(
    State(state): State<ApiState>,
    Query(_query): Query<PrometheusQuery>,
) -> Response {
    exposition_response(&state)
}

/// `GET /prometheus/data_integration`: same exposition as
/// `/prometheus/stats`, for the reason above.
pub async fn get_prometheus_data_integration(
    State(state): State<ApiState>,
    Query(_query): Query<PrometheusQuery>,
) -> Response {
    exposition_response(&state)
}

/// `GET /prometheus/message_transformation`: same exposition as
/// `/prometheus/stats`, for the reason above.
pub async fn get_prometheus_message_transformation(
    State(state): State<ApiState>,
    Query(_query): Query<PrometheusQuery>,
) -> Response {
    exposition_response(&state)
}

/// `GET /prometheus/namespaced_stats`: same exposition as
/// `/prometheus/stats`, for the reason above.
pub async fn get_prometheus_namespaced_stats(
    State(state): State<ApiState>,
    Query(_query): Query<PrometheusQuery>,
) -> Response {
    exposition_response(&state)
}

/// `GET /prometheus/schema_validation`: same exposition as
/// `/prometheus/stats`, for the reason above.
pub async fn get_prometheus_schema_validation(
    State(state): State<ApiState>,
    Query(_query): Query<PrometheusQuery>,
) -> Response {
    exposition_response(&state)
}

/// `GET /prometheus/topic_metrics`: same exposition as
/// `/prometheus/stats`, for the reason above.
pub async fn get_prometheus_topic_metrics(
    State(state): State<ApiState>,
    Query(_query): Query<PrometheusQuery>,
) -> Response {
    exposition_response(&state)
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

    async fn read_json(response: Response) -> (StatusCode, serde_json::Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 256 * 1024)
            .await
            .expect("exporter body is small and readable");
        let body: serde_json::Value =
            serde_json::from_slice(&bytes).expect("exporter body is JSON");
        (status, body)
    }

    async fn read_text(response: Response) -> (StatusCode, String, String) {
        let status = response.status();
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_string();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("exposition body is small and readable");
        (
            status,
            content_type,
            String::from_utf8(bytes.to_vec()).expect("exposition is UTF-8"),
        )
    }

    fn query() -> Query<PrometheusQuery> {
        Query(PrometheusQuery::default())
    }

    #[tokio::test]
    async fn settings_read_defaults_in_documented_shape() {
        let state = standalone_state();
        let (status, body) = read_json(get_prometheus(State(state)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["enable_basic_auth"], serde_json::json!(true));
        assert_eq!(
            body["latency_buckets"],
            serde_json::json!("10ms, 100ms, 1s, 5s, 30s")
        );
        for collector in [
            "mnesia",
            "vm_dist",
            "vm_memory",
            "vm_msacc",
            "vm_statistics",
            "vm_system_info",
        ] {
            assert_eq!(
                body["collectors"][collector],
                serde_json::json!("disabled"),
                "collector {collector}"
            );
        }
        assert_eq!(body["push_gateway"]["enable"], serde_json::json!(false));
        assert_eq!(body["push_gateway"]["method"], serde_json::json!("put"));
        assert_eq!(
            body["push_gateway"]["url"],
            serde_json::json!("http://127.0.0.1:9091")
        );
        assert!(body.get("namespaced_metrics_limiter").is_none());
    }

    #[tokio::test]
    async fn settings_replace_round_trips_and_persists() {
        let state = standalone_state();
        let body = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "enable_basic_auth": false,
                "latency_buckets": "5ms, 50ms",
                "collectors": {"vm_memory": "enabled"},
                "push_gateway": {
                    "enable": true,
                    "url": "http://push:9091",
                    "method": "post",
                    "interval": "30s",
                    "job_name": "broker",
                    "headers": {"Authorization": "Basic eA=="},
                },
                "namespaced_metrics_limiter": {"rate": "100/s"},
            }))
            .expect("update is JSON"),
        );
        let (status, after) = read_json(put_prometheus(State(state.clone()), body).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(after["enable_basic_auth"], serde_json::json!(false));
        assert_eq!(
            after["collectors"]["vm_memory"],
            serde_json::json!("enabled")
        );
        assert_eq!(after["collectors"]["mnesia"], serde_json::json!("disabled"));
        assert_eq!(after["push_gateway"]["method"], serde_json::json!("post"));
        assert_eq!(
            after["namespaced_metrics_limiter"]["rate"],
            serde_json::json!("100/s")
        );
        // The stored snapshot and the registry agree with the answer.
        assert!(!state.prometheus.get().enable_basic_auth);
        assert_eq!(
            state.config.snapshot().prometheus.push_gateway.method,
            "post".to_string()
        );
        let (status, reread) = read_json(get_prometheus(State(state)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reread, after);
    }

    #[tokio::test]
    async fn settings_accept_legacy_shape_mapped_onto_recommend() {
        let state = standalone_state();
        let body = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "enable": true,
                "interval": "32s",
                "job_name": "legacy-job",
                "push_gateway_server": "http://legacy:9091",
                "mnesia_collector": "enabled",
                "vm_dist_collector": "disabled",
                "vm_memory_collector": "disabled",
                "vm_msacc_collector": "disabled",
                "vm_statistics_collector": "disabled",
                "vm_system_info_collector": "disabled",
                "headers": {"Authorization": "Basic bGVnYWN5"},
            }))
            .expect("legacy body is JSON"),
        );
        let (status, after) = read_json(put_prometheus(State(state.clone()), body).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(after["push_gateway"]["enable"], serde_json::json!(true));
        assert_eq!(after["push_gateway"]["interval"], serde_json::json!("32s"));
        assert_eq!(
            after["push_gateway"]["url"],
            serde_json::json!("http://legacy:9091")
        );
        assert_eq!(after["collectors"]["mnesia"], serde_json::json!("enabled"));
        assert_eq!(
            after["collectors"]["vm_dist"],
            serde_json::json!("disabled")
        );
        assert_eq!(after["enable_basic_auth"], serde_json::json!(true));
    }

    #[tokio::test]
    async fn settings_reject_bad_and_mixed_bodies_without_applying() {
        let state = standalone_state();
        for bad in [
            serde_json::json!({"enable_basic_auth": "yes"}),
            serde_json::json!({"collectors": {"mnesia": "sometimes"}}),
            serde_json::json!({"push_gateway": {"method": "delete"}}),
            serde_json::json!({"push_gateway": {"headers": {"Authorization": 7}}}),
            serde_json::json!({"enable_basic_auth": true, "enable": true}),
            serde_json::json!({"mnesia_collector": "enabled", "collectors": {}}),
            serde_json::json!({"wrong_shape": 1}),
            serde_json::json!([]),
        ] {
            let body = Bytes::from(serde_json::to_vec(&bad).expect("bad body is JSON"));
            let (status, err) = read_json(put_prometheus(State(state.clone()), body).await).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "body {bad} must fail");
            assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
        }
        assert!(state.prometheus.get().enable_basic_auth);
        assert!(!state.prometheus.get().push_gateway.enable);
    }

    #[tokio::test]
    async fn exposition_reflects_management_publish_as_text() {
        // Same true counters as the JSON rows, through the broker: one
        // management publish (POST /publish entry) moves the ingress
        // counters, and the exposition renders the true text. No direct
        // counter mutation here. No subscriber matches, so the entry
        // answers 202. The full MQTT proof, with delivery and
        // delayed scheduling, lives in broker-node
        // `ap09_stats_and_metrics_read_true_broker_values`.
        let state = standalone_state();
        let body = Bytes::from(
            serde_json::to_vec(&serde_json::json!({
                "topic": "ap09/prom/one",
                "payload": "hello",
                "qos": 0,
            }))
            .expect("publish body is JSON"),
        );
        let response =
            crate::v5::clients::publish_message(axum::extract::State(state.clone()), body).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let (status, content_type, text) =
            read_text(get_prometheus_stats(State(state.clone()), query()).await).await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            content_type.starts_with("text/plain"),
            "scrape answers text, got: {content_type}"
        );
        assert!(text.contains("indramqtt_messages_received_total 1\n"));
        assert!(text.contains("indramqtt_packets_publish_received_total 1\n"));
    }

    #[tokio::test]
    async fn exposition_serves_true_counters_as_text() {
        let state = standalone_state();
        state.metrics.inc_messages_received_by(5);
        state.metrics.inc_bytes_received_by(100);
        state.stats.set_connections(2);
        state.stats.set_retained(1);
        let (status, content_type, text) =
            read_text(get_prometheus_stats(State(state.clone()), query()).await).await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            content_type.starts_with("text/plain"),
            "scrape answers text, got: {content_type}"
        );
        assert!(text.contains("indramqtt_messages_received_total 5\n"));
        assert!(text.contains("indramqtt_bytes_received_total 100\n"));
        assert!(text.contains("indramqtt_connections_count 2\n"));
        assert!(text.contains("indramqtt_retained_count 1\n"));
        // Every area route serves the same true counters.
        for route in [
            get_prometheus_auth(State(state.clone()), query()).await,
            get_prometheus_data_integration(State(state.clone()), query()).await,
            get_prometheus_message_transformation(State(state.clone()), query()).await,
            get_prometheus_namespaced_stats(State(state.clone()), query()).await,
            get_prometheus_schema_validation(State(state.clone()), query()).await,
            get_prometheus_topic_metrics(State(state.clone()), query()).await,
        ] {
            let (status, _, area) = read_text(route).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(area, text);
        }
    }

    #[test]
    fn serves_after_lock_poison() {
        // A poisoned lock must not stop the store. Reads recover the lock.
        let store = PrometheusStore::new();
        assert!(store.get().enable_basic_auth);
        std::thread::scope(|s| {
            let h = s.spawn(|| {
                let _guard = store.inner.write().unwrap();
                panic!("poison the prometheus lock");
            });
            let _ = h.join();
        });
        assert!(store.get().enable_basic_auth);
    }
}
