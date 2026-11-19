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
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
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
    broker_config::parse_duration_str(text).map_err(|_| source_duration_error(id, key, value))
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

#[derive(Debug, Default, Clone)]
struct PersistedSourceTopics {
    last_topics: HashMap<String, String>,
    pending_unsubscribes: HashMap<String, Vec<String>>,
}

impl PersistedSourceTopics {
    fn load(path: &Path) -> Self {
        let Ok(bytes) = std::fs::read(path) else {
            return Self::default();
        };
        let Ok(val) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
            return Self::default();
        };
        let mut out = Self::default();
        if let Some(obj) = val.get("last_topics").and_then(|v| v.as_object()) {
            for (k, v) in obj {
                if let Some(s) = v.as_str() {
                    out.last_topics.insert(k.clone(), s.to_string());
                }
            }
        }
        if let Some(obj) = val.get("pending_unsubscribes").and_then(|v| v.as_object()) {
            for (k, v) in obj {
                if let Some(arr) = v.as_array() {
                    let list: Vec<String> = arr
                        .iter()
                        .filter_map(|item| item.as_str().map(str::to_string))
                        .collect();
                    if !list.is_empty() {
                        out.pending_unsubscribes.insert(k.clone(), list);
                    }
                }
            }
        }
        out
    }

    fn save(&self, path: &Path) {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let val = serde_json::json!({
            "last_topics": self.last_topics,
            "pending_unsubscribes": self.pending_unsubscribes,
        });
        if let Ok(bytes) = serde_json::to_vec(&val) {
            let tmp = path.with_extension("tmp");
            if std::fs::write(&tmp, &bytes).is_ok() {
                let _ = std::fs::rename(&tmp, path);
            }
        }
    }

    fn record_topic(&mut self, id: &str, topic: &str, clean_session: bool) -> bool {
        let mut changed = false;
        if clean_session {
            if self.last_topics.remove(id).is_some() {
                changed = true;
            }
            if self.pending_unsubscribes.remove(id).is_some() {
                changed = true;
            }
            return changed;
        }
        if let Some(prev) = self.last_topics.get(id) {
            if prev != topic {
                let pending = self.pending_unsubscribes.entry(id.to_string()).or_default();
                if !pending.contains(prev) {
                    pending.push(prev.clone());
                }
                self.last_topics.insert(id.to_string(), topic.to_string());
                changed = true;
            }
        } else {
            self.last_topics.insert(id.to_string(), topic.to_string());
            changed = true;
        }
        let mut remove_empty = false;
        if let Some(pending) = self.pending_unsubscribes.get_mut(id) {
            let before = pending.len();
            pending.retain(|t| t != topic);
            let after = pending.len();
            remove_empty = after == 0;
            if after != before {
                changed = true;
            }
        }
        if remove_empty {
            self.pending_unsubscribes.remove(id);
        }
        changed
    }

    fn clear_unsubscribed(&mut self, id: &str, completed: &[String]) -> bool {
        let Some(pending) = self.pending_unsubscribes.get_mut(id) else {
            return false;
        };
        let before = pending.len();
        pending.retain(|t| !completed.contains(t));
        if pending.is_empty() {
            self.pending_unsubscribes.remove(id);
        }
        before != self.pending_unsubscribes.get(id).map_or(0, Vec::len)
    }
}

/// Start the source runtime on the shared kernel state.
pub fn spawn(shared: Shared) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let state_path = shared.connectors.data_dir().join("source_topic_state.json");
    let topic_state = Arc::new(Mutex::new(PersistedSourceTopics::load(&state_path)));
    handle.spawn(async move {
        let mut running: HashMap<String, RunningSource> = HashMap::new();
        let mut last_inputs: HashMap<String, (SourceEntry, Option<serde_json::Value>)> =
            HashMap::new();
        loop {
            reconcile(
                &shared,
                &mut running,
                &mut last_inputs,
                &topic_state,
                &state_path,
            )
            .await;
            tokio::time::sleep(SOURCE_RECONCILE_INTERVAL).await;
        }
    });
}

async fn reconcile(
    shared: &Shared,
    running: &mut HashMap<String, RunningSource>,
    last_inputs: &mut HashMap<String, (SourceEntry, Option<serde_json::Value>)>,
    topic_state: &Arc<Mutex<PersistedSourceTopics>>,
    state_path: &Path,
) {
    let entries = shared.sources.snapshot();
    let mut seen = HashSet::with_capacity(entries.len());
    for entry in entries {
        seen.insert(entry.id.clone());
        if !entry.enable {
            last_inputs.remove(&entry.id);
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
        let connector_doc = if entry.connector.trim().is_empty() {
            None
        } else {
            shared.connectors.get(&entry.connector)
        };
        if last_inputs
            .get(&entry.id)
            .is_some_and(|(prev_entry, prev_conn)| {
                prev_entry == &entry && prev_conn == &connector_doc
            })
        {
            continue;
        }
        last_inputs.insert(entry.id.clone(), (entry.clone(), connector_doc));
        match build_config(&entry, shared).await {
            Ok(mut config) => {
                let unchanged = running
                    .get(&entry.id)
                    .is_some_and(|current| current.config.same_config(&config));
                if unchanged {
                    continue;
                }
                {
                    let mut state = topic_state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if state.record_topic(&entry.id, &config.topic, config.clean_session) {
                        state.save(state_path);
                    }
                    if !config.clean_session {
                        config.previous_topics = state
                            .pending_unsubscribes
                            .get(&entry.id)
                            .cloned()
                            .unwrap_or_default();
                    }
                }
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
                    topic_state: Arc::clone(topic_state),
                    state_path: state_path.to_path_buf(),
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
    if !missing.is_empty() {
        let mut state = topic_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut dirty = false;
        for id in &missing {
            if state.last_topics.remove(id).is_some() {
                dirty = true;
            }
            if state.pending_unsubscribes.remove(id).is_some() {
                dirty = true;
            }
        }
        if dirty {
            state.save(state_path);
        }
    }
    for id in missing {
        last_inputs.remove(&id);
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
    topic_state: Arc<Mutex<PersistedSourceTopics>>,
    state_path: PathBuf,
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
            MqttSourceEvent::Unsubscribed(completed) => {
                let mut state = self
                    .topic_state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if state.clear_unsubscribed(&self.source_id, &completed) {
                    state.save(&self.state_path);
                }
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

async fn build_config(entry: &SourceEntry, shared: &Shared) -> Result<MqttSourceConfig, String> {
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
    if is_self_listener_target(&endpoint.host, endpoint.port, shared).await {
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
        previous_topics: Vec::new(),
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

async fn is_self_listener_target(host: &str, port: u16, shared: &Shared) -> bool {
    let bind_ips: Vec<std::net::IpAddr> = {
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
        binds
            .into_iter()
            .filter_map(|(enabled, bind)| {
                if !enabled {
                    return None;
                }
                let addr = bind.parse::<std::net::SocketAddr>().ok()?;
                (addr.port() == port).then_some(addr.ip())
            })
            .collect()
    };
    for bind_ip in bind_ips {
        if host_targets_listener(host, bind_ip).await {
            return true;
        }
    }
    false
}

fn normalize_ip(ip: std::net::IpAddr) -> std::net::IpAddr {
    match ip {
        std::net::IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map(std::net::IpAddr::V4)
            .unwrap_or(std::net::IpAddr::V6(v6)),
        v4 => v4,
    }
}

async fn is_local_interface_ip(ip: std::net::IpAddr) -> bool {
    if ip.is_loopback() || ip.is_unspecified() {
        return true;
    }
    tokio::task::spawn_blocking(move || std::net::TcpListener::bind((ip, 0)).is_ok())
        .await
        .unwrap_or(false)
}

async fn ip_targets_listener(ip: std::net::IpAddr, bind_ip: std::net::IpAddr) -> bool {
    let ip = normalize_ip(ip);
    let bind_ip = normalize_ip(bind_ip);
    if ip == bind_ip {
        return true;
    }
    if (ip.is_loopback() || ip.is_unspecified())
        && (bind_ip.is_loopback() || bind_ip.is_unspecified())
    {
        return true;
    }
    bind_ip.is_unspecified() && is_local_interface_ip(ip).await
}

async fn host_targets_listener(host: &str, bind_ip: std::net::IpAddr) -> bool {
    let trimmed = host.trim().trim_start_matches('[').trim_end_matches(']');
    if trimmed.eq_ignore_ascii_case("localhost") {
        return bind_ip.is_loopback() || bind_ip.is_unspecified();
    }
    if let Ok(ip) = trimmed.parse::<std::net::IpAddr>() {
        return ip_targets_listener(ip, bind_ip).await;
    }
    if let Ok(addrs) = tokio::net::lookup_host((trimmed, 0u16)).await {
        for addr in addrs {
            if ip_targets_listener(addr.ip(), bind_ip).await {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn wildcard_listener_catches_local_lan_ip_and_persists_topic_migrations() {
        let bind_any: std::net::IpAddr = "0.0.0.0".parse().unwrap();
        assert!(host_targets_listener("127.0.0.1", bind_any).await);
        // Probe a real local outbound interface IP if available.
        if let Ok(sock) = std::net::UdpSocket::bind("0.0.0.0:0") {
            if sock.connect("8.8.8.8:80").is_ok() {
                if let Ok(local) = sock.local_addr() {
                    assert!(
                        host_targets_listener(&local.ip().to_string(), bind_any).await,
                        "expected local interface IP {} to match 0.0.0.0 listener",
                        local.ip()
                    );
                }
            }
        }

        let dir = std::env::temp_dir().join(format!("indra-src-topic-{}", std::process::id()));
        let path = dir.join("source_topic_state.json");
        let mut state = PersistedSourceTopics::default();
        assert!(state.record_topic("s1", "a/#", false));
        assert!(state.record_topic("s1", "b/#", false));
        state.save(&path);

        let reloaded = PersistedSourceTopics::load(&path);
        assert_eq!(
            reloaded.pending_unsubscribes.get("s1").cloned(),
            Some(vec!["a/#".to_string()])
        );
        let mut reloaded = reloaded;
        assert!(reloaded.clear_unsubscribed("s1", &["a/#".to_string()]));
        assert!(reloaded.pending_unsubscribes.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
