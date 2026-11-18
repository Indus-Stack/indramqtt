//! Source ingress runtime.
//!
//! One task reconciles the stored source directory with the running
//! consumers. It reads a bounded snapshot every
//! [`SOURCE_RECONCILE_INTERVAL`], starts the consumer for a new or changed
//! source, and stops the old consumer before the new one starts. A stop
//! and a delete therefore leave no duplicate consumer.
//!
//! Only the MQTT subscriber kind has a runtime here. The source store
//! refuses every other kind at create and update. A stored record of an
//! older version stays readable and reports `disconnected` with the
//! reason.

use crate::{forward_cluster, ingress_pipeline_with_publisher, Shared};
use broker_api::v5::sources::{
    is_supported_source_type, SourceEntry, SourceRuntimeStatus, SourceStore,
};
use broker_connectors::mqtt_source::{
    run_mqtt_source, MqttSourceConfig, MqttSourceEvent, MqttSourceHandler, MqttSourceMessage,
    MQTT_SOURCE_RETRY,
};
use broker_connectors::parse_bridge_address;
use broker_protocol::{QoS, Topic};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::warn;

/// How often the runtime compares the stored source directory with the
/// running consumers. Reason: a prompt update and delete without a
/// second wake channel. One bounded snapshot read runs off the publish
/// path, and the interval is short beside the source health interval.
const SOURCE_RECONCILE_INTERVAL: Duration = Duration::from_millis(200);

/// Longest wait for one consumer to drain after a stop request. Reason:
/// the consumer drain is bounded at 250 ms; two seconds covers a
/// reconnect and keeps one stuck consumer from stopping the reconciler.
const SOURCE_STOP_TIMEOUT: Duration = Duration::from_secs(2);

/// Default MQTT keepalive. Reason: the reference MQTT connector default.
const DEFAULT_KEEPALIVE_SECS: u16 = 160;

/// Default interval between source health checks. Reason: one second is
/// the delay the runtime used before `resource_opts` existed, and it
/// keeps a downed remote broker from spinning the task.
const DEFAULT_HEALTH_CHECK_INTERVAL: Duration = MQTT_SOURCE_RETRY;

/// Health policy from a source `resource_opts` object. The store keeps
/// the raw object; the runtime reads the fields it applies here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SourceHealthOpts {
    interval: Duration,
    jitter: Duration,
    timeout: Option<Duration>,
}

impl SourceHealthOpts {
    /// Parse one source's `resource_opts`. An absent key takes the
    /// runtime default. An unknown key is refused. Reason: this runtime
    /// cannot apply it, and a stored value that does nothing is a defect.
    fn parse(id: &str, raw: &serde_json::Value) -> Result<Self, String> {
        let Some(obj) = raw.as_object() else {
            return Err(format!(
                "source {id:?} field `resource_opts` must be an object"
            ));
        };
        let mut opts = Self {
            interval: DEFAULT_HEALTH_CHECK_INTERVAL,
            jitter: Duration::ZERO,
            timeout: None,
        };
        for (key, value) in obj {
            match key.as_str() {
                "health_check_interval" => {
                    let interval = source_duration(id, key, value)?;
                    if interval.is_zero() {
                        return Err(format!(
                            "source {id:?} field `resource_opts.health_check_interval` must be at least 1ns"
                        ));
                    }
                    opts.interval = interval;
                }
                "health_check_interval_jitter" => {
                    opts.jitter = source_duration(id, key, value)?;
                }
                "health_check_timeout" => {
                    opts.timeout = Some(source_duration(id, key, value)?);
                }
                _ => {
                    return Err(format!(
                        "source {id:?} field `resource_opts.{key}` is not supported"
                    ));
                }
            }
        }
        Ok(opts)
    }

    /// Delay before a failed connection retries. The configured jitter
    /// adds a stable per-source offset to the base interval.
    fn retry_delay(&self, id: &str) -> Duration {
        self.interval.saturating_add(source_jitter(id, self.jitter))
    }
}

/// Parse a duration string such as `250ms`, `15s` or `1h`.
fn source_duration(id: &str, key: &str, value: &serde_json::Value) -> Result<Duration, String> {
    let Some(text) = value.as_str() else {
        return Err(format!(
            "source {id:?} field `resource_opts.{key}` must be a duration string"
        ));
    };
    let text = text.trim().to_ascii_lowercase();
    let split = text
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .ok_or_else(|| source_duration_error(id, key, value))?;
    let (number, unit) = text.split_at(split);
    let number: f64 = number
        .parse()
        .map_err(|_| source_duration_error(id, key, value))?;
    if !number.is_finite() || number < 0.0 {
        return Err(source_duration_error(id, key, value));
    }
    let nanos = match unit.trim() {
        "ns" => number,
        "us" | "µs" => number * 1_000.0,
        "ms" => number * 1_000_000.0,
        "s" => number * 1_000_000_000.0,
        "m" => number * 60.0 * 1_000_000_000.0,
        "h" => number * 3600.0 * 1_000_000_000.0,
        _ => return Err(source_duration_error(id, key, value)),
    };
    if nanos > u64::MAX as f64 {
        return Err(source_duration_error(id, key, value));
    }
    Ok(Duration::from_nanos(nanos as u64))
}

fn source_duration_error(id: &str, key: &str, value: &serde_json::Value) -> String {
    format!("source {id:?} field `resource_opts.{key}` value {value} is not a valid duration")
}

/// Add a stable offset inside the configured jitter. Reason: the
/// reference spreads the health checks of related resources, and a
/// stable offset keeps one source from checking at the same instant as
/// its siblings.
fn source_jitter(id: &str, jitter: Duration) -> Duration {
    if jitter.is_zero() {
        return Duration::ZERO;
    }
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    id.hash(&mut hasher);
    let span = jitter.as_nanos().min(u128::from(u64::MAX));
    let offset = u128::from(hasher.finish()) % (span + 1);
    Duration::from_nanos(offset as u64)
}

struct RunningSource {
    config: MqttSourceConfig,
    handle: JoinHandle<()>,
    stop: watch::Sender<bool>,
}

/// Start the source runtime on the shared kernel state.
pub fn spawn(shared: Shared) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    handle.spawn(async move {
        let mut running: HashMap<String, RunningSource> = HashMap::new();
        let mut last_topics: HashMap<String, String> = HashMap::new();
        loop {
            reconcile(&shared, &mut running, &mut last_topics).await;
            tokio::time::sleep(SOURCE_RECONCILE_INTERVAL).await;
        }
    });
}

async fn reconcile(
    shared: &Shared,
    running: &mut HashMap<String, RunningSource>,
    last_topics: &mut HashMap<String, String>,
) {
    let entries = shared.sources.snapshot();
    let mut seen = HashSet::with_capacity(entries.len());
    for entry in entries {
        seen.insert(entry.id.clone());
        if !entry.enable {
            stop(
                shared,
                running,
                &entry.id,
                Some(SourceRuntimeStatus::Disconnected(
                    "source is disabled".to_string(),
                )),
            )
            .await;
            continue;
        }
        match build_config(&entry, shared) {
            Ok(mut config) => {
                let unchanged = running
                    .get(&entry.id)
                    .is_some_and(|current| current.config.same_config(&config));
                if unchanged {
                    continue;
                }
                if !config.clean_session {
                    if let Some(prev) = last_topics.get(&entry.id) {
                        if prev != &config.topic {
                            config.previous_topic = Some(prev.clone());
                        }
                    }
                }
                last_topics.insert(entry.id.clone(), config.topic.clone());
                stop(
                    shared,
                    running,
                    &entry.id,
                    Some(SourceRuntimeStatus::Disconnected(
                        "source is starting".to_string(),
                    )),
                )
                .await;
                let handler: Arc<dyn MqttSourceHandler> = Arc::new(DeliveryHandler {
                    shared: shared.clone(),
                    source_id: entry.id.clone(),
                    sources: Arc::clone(&shared.sources),
                });
                let source_config = config.clone();
                let (stop, stop_rx) = watch::channel(false);
                let handle = tokio::spawn(async move {
                    run_mqtt_source(source_config, handler, stop_rx).await;
                });
                running.insert(
                    entry.id.clone(),
                    RunningSource {
                        config,
                        handle,
                        stop,
                    },
                );
            }
            Err(reason) => {
                stop(
                    shared,
                    running,
                    &entry.id,
                    Some(SourceRuntimeStatus::Disconnected(reason)),
                )
                .await;
            }
        }
    }
    let missing: Vec<String> = running
        .keys()
        .filter(|id| !seen.contains(*id))
        .cloned()
        .collect();
    for id in missing {
        last_topics.remove(&id);
        stop(shared, running, &id, None).await;
    }
}

async fn stop(
    shared: &Shared,
    running: &mut HashMap<String, RunningSource>,
    id: &str,
    status: Option<SourceRuntimeStatus>,
) {
    if let Some(current) = running.remove(id) {
        let _ = current.stop.send(true);
        let mut handle = current.handle;
        if tokio::time::timeout(SOURCE_STOP_TIMEOUT, &mut handle)
            .await
            .is_err()
        {
            handle.abort();
            let _ = handle.await;
        }
    }
    match status {
        Some(status) => shared.sources.set_runtime_status(id, status),
        None => shared.sources.clear_runtime_status(id),
    }
}

struct DeliveryHandler {
    shared: Shared,
    source_id: String,
    sources: Arc<SourceStore>,
}

#[async_trait::async_trait]
impl MqttSourceHandler for DeliveryHandler {
    async fn on_event(&self, event: MqttSourceEvent) {
        match event {
            MqttSourceEvent::Connected => self
                .sources
                .set_runtime_status(&self.source_id, SourceRuntimeStatus::Connected),
            MqttSourceEvent::Disconnected(reason) => self
                .sources
                .set_runtime_status(&self.source_id, SourceRuntimeStatus::Disconnected(reason)),
            MqttSourceEvent::Message(message) => {
                self.deliver(message).await;
            }
        }
    }
}

impl DeliveryHandler {
    async fn deliver(&self, message: MqttSourceMessage) {
        let Ok(topic) = Topic::new(message.topic.as_str()) else {
            warn!(
                source = self.source_id.as_str(),
                topic = message.topic.as_str(),
                "source ingress dropped a message with an invalid topic"
            );
            return;
        };
        let qos = QoS::try_from(message.qos).unwrap_or(QoS::AtMostOnce);
        // A source record has no tenant field. The reference API scopes a
        // source with the `ns` query, and this build has no source
        // namespace. The path therefore delivers into the default tenant
        // only. The two-tenant test proves that no message reaches
        // another tenant.
        let deliveries = ingress_pipeline_with_publisher(
            &self.shared,
            &topic,
            qos,
            message.retain,
            &message.payload,
            None,
            "default",
        )
        .await;
        for (conn_id, routed) in deliveries {
            let _ = self.shared.conns.route(conn_id, routed);
        }
        forward_cluster(&self.shared, &topic, qos, &message.payload, "default").await;
    }
}

fn build_config(entry: &SourceEntry, shared: &Shared) -> Result<MqttSourceConfig, String> {
    let connectors = &shared.connectors;
    if !is_supported_source_type(&entry.source_type) {
        return Err(format!(
            "source type {} has no ingress runtime in this broker",
            entry.source_type
        ));
    }
    let health = SourceHealthOpts::parse(&entry.id, &entry.resource_opts)?;
    let topic = entry
        .parameters
        .get("topic")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|topic| !topic.is_empty())
        .ok_or_else(|| "source parameters need a topic (field `parameters.topic`)".to_string())?
        .to_string();
    let qos = match entry.parameters.get("qos") {
        None => 1,
        Some(value) => {
            let qos = value
                .as_u64()
                .ok_or_else(|| "field `parameters.qos` must be an integer".to_string())?;
            if qos > 2 {
                return Err("field `parameters.qos` must be 0, 1 or 2".to_string());
            }
            qos as u8
        }
    };
    if entry.connector.trim().is_empty() {
        return Err("source needs a connector (field `connector`)".to_string());
    }
    let connector = connectors
        .get(&entry.connector)
        .ok_or_else(|| format!("connector {:?} is not stored", entry.connector))?;
    if connector
        .get("enable")
        .and_then(|value| value.as_bool())
        .is_some_and(|enable| !enable)
    {
        return Err(format!("connector {:?} is disabled", entry.connector));
    }
    let kind = connector
        .get("type")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    if kind != "mqtt" && kind != "mqtt_bridge" {
        return Err(format!(
            "connector {:?} is not an MQTT connector",
            entry.connector
        ));
    }
    let server = connector
        .get("server")
        .and_then(|value| value.as_str())
        .or_else(|| connector.get("url").and_then(|value| value.as_str()))
        .map(str::trim)
        .filter(|server| !server.is_empty())
        .ok_or_else(|| {
            format!(
                "connector {:?} needs a server (field `server`)",
                entry.connector
            )
        })?;
    let endpoint = parse_bridge_address(server).map_err(|error| error.to_string())?;
    if endpoint.tls {
        return Err("MQTT source TLS is not enabled in this build".to_string());
    }
    if is_self_listener_target(&endpoint.host, endpoint.port, shared) {
        return Err(format!(
            "MQTT source connector {:?} targets this broker's own listener ({}:{}) and would create a publish loop",
            entry.connector, endpoint.host, endpoint.port
        ));
    }
    let proto = connector
        .get("proto_ver")
        .and_then(|value| value.as_str())
        .unwrap_or("v4");
    if proto == "v5" {
        return Err("MQTT v5 source is not supported in this build".to_string());
    }
    let username = connector
        .get("username")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let password = connector
        .get("password")
        .and_then(|value| value.as_str())
        .map(str::to_string);
    let keep_alive_secs = match connector.get("keepalive") {
        Some(value) => {
            let text = value
                .as_str()
                .ok_or_else(|| "field `keepalive` must be a string".to_string())?;
            parse_seconds(text)?
        }
        None => DEFAULT_KEEPALIVE_SECS,
    };
    let clean_session = connector
        .get("clean_start")
        .and_then(|value| value.as_bool())
        .unwrap_or(true);
    Ok(MqttSourceConfig {
        host: endpoint.host,
        port: endpoint.port,
        client_id: format!("indramqtt-source-{}", entry.id),
        username,
        password,
        keep_alive_secs,
        clean_session,
        topic,
        previous_topic: None,
        qos,
        retry_delay: health.retry_delay(&entry.id),
        connection_timeout: health.timeout,
    })
}

/// Parse a duration with an `s`, `m`, `h` or `ms` suffix into whole
/// seconds. Reason: the connector stores durations as text; the driver
/// takes seconds.
fn parse_seconds(text: &str) -> Result<u16, String> {
    let text = text.trim();
    if text.is_empty() {
        return Err("field `keepalive` must not be empty".to_string());
    }
    let (number, multiplier) = if let Some(value) = text.strip_suffix("ms") {
        let millis: u64 = value
            .trim()
            .parse()
            .map_err(|_| format!("field `keepalive` is not a duration: {text:?}"))?;
        (millis.div_ceil(1000).max(1), 1)
    } else if let Some(value) = text.strip_suffix('s') {
        (
            value
                .trim()
                .parse()
                .map_err(|_| format!("field `keepalive` is not a duration: {text:?}"))?,
            1,
        )
    } else if let Some(value) = text.strip_suffix('m') {
        (
            value
                .trim()
                .parse()
                .map_err(|_| format!("field `keepalive` is not a duration: {text:?}"))?,
            60,
        )
    } else if let Some(value) = text.strip_suffix('h') {
        (
            value
                .trim()
                .parse()
                .map_err(|_| format!("field `keepalive` is not a duration: {text:?}"))?,
            3600,
        )
    } else {
        (
            text.parse()
                .map_err(|_| format!("field `keepalive` is not a duration: {text:?}"))?,
            1,
        )
    };
    let seconds = number
        .checked_mul(multiplier)
        .filter(|seconds| *seconds <= u64::from(u16::MAX))
        .ok_or_else(|| format!("field `keepalive` is too large: {text:?}"))?;
    Ok(seconds as u16)
}

fn is_self_listener_target(host: &str, port: u16, shared: &Shared) -> bool {
    let Ok(guard) = shared.api_listeners.read() else {
        return false;
    };
    let Some(listeners) = guard.as_ref() else {
        return false;
    };
    let binds = [
        (listeners.tcp.enabled, listeners.tcp.bind.as_str()),
        (listeners.tls.enabled, listeners.tls.bind.as_str()),
    ];
    for (enabled, bind) in binds {
        if !enabled {
            continue;
        }
        let Ok(addr) = bind.parse::<std::net::SocketAddr>() else {
            continue;
        };
        if addr.port() != port {
            continue;
        }
        if host_targets_listener(host, addr.ip()) {
            return true;
        }
    }
    false
}

fn host_targets_listener(host: &str, bind_ip: std::net::IpAddr) -> bool {
    let trimmed = host.trim().trim_start_matches('[').trim_end_matches(']');
    if trimmed.eq_ignore_ascii_case("localhost") {
        return bind_ip.is_loopback() || bind_ip.is_unspecified();
    }
    if let Ok(ip) = trimmed.parse::<std::net::IpAddr>() {
        if ip == bind_ip {
            return true;
        }
        if (ip.is_loopback() || ip.is_unspecified())
            && (bind_ip.is_loopback() || bind_ip.is_unspecified())
        {
            return true;
        }
    }
    false
}
