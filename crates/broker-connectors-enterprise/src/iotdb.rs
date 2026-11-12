//! Apache IoTDB producer sink (INDRA-174).
//!
//! Buffers MQTT events as tablet rows and writes them with `POST
//! {endpoint}/rest/v2/insertTablet`: hierarchical device paths
//! (`root.group.device...`), aligned or non-aligned series, typed
//! measurement vectors, and HTTP Basic authentication. Rows group by
//! device per flush so one tablet carries many timestamps.
//!
//! Device path segments sanitize to valid IoTDB node identifiers;
//! measurement values coerce from JSON by the configured data types
//! (missing fields fail loudly). HTTP 305 (cluster routing) and 5xx
//! retry with backoff; other non-2xx statuses are terminal. Errors
//! carry the server answer text; a 200 answer with a failing code
//! is terminal too.

use async_trait::async_trait;
use base64::Engine;
use broker_protocol::{QoS, Topic};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use broker_connectors::{
    now_millis, render_template, BackoffState, BatchQueue, ConnectorError, Result, Sink,
};

/// IoTDB authentication (HTTP Basic).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IotDbAuth {
    pub username: String,
    pub password: String,
}

impl IotDbAuth {
    pub fn header_value(&self) -> Result<String> {
        if self.username.is_empty() {
            return Err(ConnectorError::Dispatch(
                "iotdb username must not be empty".to_string(),
            ));
        }
        let credentials = base64::engine::general_purpose::STANDARD
            .encode(format!("{}:{}", self.username, self.password));
        Ok(format!("Basic {credentials}"))
    }
}

/// IoTDB data types (tablet column types).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum IotDbDataType {
    Boolean,
    Int32,
    Int64,
    Float,
    Double,
    Text,
}

impl IotDbDataType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Boolean => "BOOLEAN",
            Self::Int32 => "INT32",
            Self::Int64 => "INT64",
            Self::Float => "FLOAT",
            Self::Double => "DOUBLE",
            Self::Text => "TEXT",
        }
    }

    /// Coerce a JSON value into the column type for the tablet vector.
    fn coerce(self, value: &serde_json::Value) -> Result<serde_json::Value> {
        match (self, value) {
            (Self::Boolean, serde_json::Value::Bool(v)) => Ok(serde_json::json!(*v)),
            (Self::Int32, serde_json::Value::Number(n)) => n
                .as_i64()
                .and_then(|v| i32::try_from(v).ok())
                .map(|v| serde_json::json!(v))
                .ok_or_else(|| {
                    ConnectorError::Dispatch("iotdb INT32 value out of range".to_string())
                }),
            (Self::Int64, serde_json::Value::Number(n)) => {
                n.as_i64().map(|v| serde_json::json!(v)).ok_or_else(|| {
                    ConnectorError::Dispatch("iotdb INT64 needs an integer".to_string())
                })
            }
            (Self::Float, serde_json::Value::Number(n)) => n
                .as_f64()
                .map(|v| serde_json::json!(v as f32 as f64))
                .ok_or_else(|| ConnectorError::Dispatch("iotdb FLOAT needs a number".to_string())),
            (Self::Double, serde_json::Value::Number(n)) => n
                .as_f64()
                .map(|v| serde_json::json!(v))
                .ok_or_else(|| ConnectorError::Dispatch("iotdb DOUBLE needs a number".to_string())),
            (Self::Text, serde_json::Value::String(text)) => Ok(serde_json::json!(text)),
            (Self::Text, other) if other.is_number() || other.is_boolean() => {
                Ok(serde_json::json!(other.to_string()))
            }
            _ => Err(ConnectorError::Dispatch(format!(
                "iotdb value {value} does not fit {:?}",
                self.as_str()
            ))),
        }
    }
}

fn default_batch_size() -> Option<usize> {
    Some(200)
}

fn default_batch_bytes() -> Option<usize> {
    Some(2_097_152)
}

fn default_linger_ms() -> Option<u64> {
    Some(10)
}

fn default_max_retries() -> Option<usize> {
    Some(3)
}

fn default_initial_backoff_ms() -> Option<u64> {
    Some(100)
}

fn default_max_backoff_ms() -> Option<u64> {
    Some(2_000)
}

/// IoTDB sink configuration. All depths are optional (`None` =
/// unbounded) with zero clamped ceilings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IotDbSinkConfig {
    /// REST endpoint, e.g. `http://localhost:18080/rest/v2`.
    pub endpoint: String,
    /// Device path template, rooted at `root.` (e.g.
    /// `root.factory.${payload.plant_id}.${client_id}`).
    pub device_path_template: String,
    pub auth: IotDbAuth,
    /// Aligned time-series layout (default false).
    #[serde(default)]
    pub is_aligned: bool,
    /// Measurement columns (non-empty).
    pub measurements: Vec<String>,
    /// Column types, one per measurement.
    pub data_types: Vec<IotDbDataType>,
    /// Records per tablet (default 200).
    #[serde(default = "default_batch_size")]
    pub batch_size: Option<usize>,
    /// Batch byte limit (default 2 MiB).
    #[serde(default = "default_batch_bytes")]
    pub batch_bytes: Option<usize>,
    /// Linger flush window in ms (default 10).
    #[serde(default = "default_linger_ms")]
    pub linger_ms: Option<u64>,
    /// Retries on 305/5xx (default 3, `None` unbounded, 0 none).
    #[serde(default = "default_max_retries")]
    pub max_retries: Option<usize>,
    /// First retry delay in ms (default 100).
    #[serde(default = "default_initial_backoff_ms")]
    pub initial_backoff_ms: Option<u64>,
    /// Retry delay ceiling in ms (default 2000).
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: Option<u64>,
    /// Request / network timeout in ms (default 5000).
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

impl IotDbSinkConfig {
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms.unwrap_or(5000).max(1))
    }

    pub fn validate(&self) -> Result<()> {
        if !self.endpoint.starts_with("http://") && !self.endpoint.starts_with("https://") {
            return Err(ConnectorError::Dispatch(format!(
                "iotdb endpoint must be http(s): {:?}",
                self.endpoint
            )));
        }
        if self.device_path_template.trim().is_empty() {
            return Err(ConnectorError::Dispatch(
                "iotdb device_path_template must not be empty".to_string(),
            ));
        }
        // Literal root guard plus strict variable syntax (unknown
        // `${...}` fail here; empty values fail per event at render).
        if !self.device_path_template.starts_with("root.") {
            return Err(ConnectorError::Dispatch(format!(
                "iotdb device path must start with root.: {:?}",
                self.device_path_template
            )));
        }
        check_template_vars(&self.device_path_template)?;
        self.auth.header_value().map(|_| ())?;
        if self.measurements.is_empty() {
            return Err(ConnectorError::Dispatch(
                "iotdb measurements must not be empty".to_string(),
            ));
        }
        for measurement in &self.measurements {
            if !is_node_identifier(measurement) {
                return Err(ConnectorError::Dispatch(format!(
                    "iotdb measurement must match [A-Za-z0-9_]+: {measurement:?}"
                )));
            }
        }
        if self.measurements.len() != self.data_types.len() {
            return Err(ConnectorError::Dispatch(format!(
                "iotdb measurements ({}) and data_types ({}) must align",
                self.measurements.len(),
                self.data_types.len()
            )));
        }
        if self.batch_size == Some(0) {
            return Err(ConnectorError::Dispatch(
                "iotdb batch_size must be >= 1".to_string(),
            ));
        }
        if self.batch_bytes == Some(0) {
            return Err(ConnectorError::Dispatch(
                "iotdb batch_bytes must be >= 1".to_string(),
            ));
        }
        Ok(())
    }

    /// `POST {endpoint}/insertTablet` (trailing slashes trimmed).
    pub fn tablet_url(&self) -> String {
        format!("{}/insertTablet", self.endpoint.trim_end_matches('/'))
    }

    pub fn effective_batch_size(&self) -> usize {
        self.batch_size.unwrap_or(usize::MAX).max(1)
    }

    pub fn effective_batch_bytes(&self) -> usize {
        self.batch_bytes.unwrap_or(usize::MAX).max(1)
    }

    pub fn effective_linger(&self) -> Duration {
        self.linger_ms
            .map(Duration::from_millis)
            .unwrap_or(Duration::MAX)
    }

    /// Template variables for one event.
    fn template_vars(topic: &str, payload: &[u8], qos: QoS, millis: i64) -> Vec<(String, String)> {
        let doc: serde_json::Value = serde_json::from_slice(payload).unwrap_or_default();
        let client_id_val = doc
            .get("client_id")
            .or_else(|| doc.get("clientid"))
            .or_else(|| doc.get("device_id"))
            .or_else(|| {
                doc.get("payload").and_then(|p| {
                    p.get("client_id")
                        .or_else(|| p.get("clientid"))
                        .or_else(|| p.get("device_id"))
                })
            });
        let client_id = match client_id_val {
            Some(serde_json::Value::String(text)) => text.clone(),
            Some(scalar) if scalar.is_number() || scalar.is_boolean() => scalar.to_string(),
            _ => String::new(),
        };
        vec![
            ("topic".to_string(), topic.to_string()),
            ("client_id".to_string(), client_id),
            ("qos".to_string(), u8::from(qos).to_string()),
            ("timestamp".to_string(), millis.to_string()),
        ]
    }

    fn event_vars(
        &self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
        millis: i64,
        template: &str,
    ) -> Result<String> {
        let mut vars = Self::template_vars(topic, payload, qos, millis);
        let doc: serde_json::Value = serde_json::from_slice(payload).unwrap_or_default();
        let mut rest = template;
        while let Some(start) = rest.find("${payload.") {
            let after = &rest[start + "${payload.".len()..];
            if let Some(close) = after.find('}') {
                let name = &after[..close];
                let val = doc
                    .get(name)
                    .or_else(|| doc.get("payload").and_then(|p| p.get(name)));
                let value = match val {
                    Some(serde_json::Value::String(text)) => text.clone(),
                    Some(scalar) if scalar.is_number() || scalar.is_boolean() => scalar.to_string(),
                    _ => String::new(),
                };
                vars.push((format!("payload.{name}"), value));
                rest = &after[close + 1..];
            } else {
                break;
            }
        }
        let borrowed: Vec<(&str, String)> =
            vars.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
        render_template(template, &borrowed)
    }

    /// Resolve + sanitize the device path: every dot-separated
    /// segment becomes a valid node identifier (illegal characters
    /// turn into `_`, leading digits gain a `_` prefix).
    pub fn resolve_device_path(
        &self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
        millis: i64,
    ) -> Result<String> {
        let rendered = self.event_vars(topic, payload, qos, millis, &self.device_path_template)?;
        let segments: Vec<String> = rendered.split('.').map(sanitize_node).collect();
        if segments.iter().any(|segment| segment.is_empty()) {
            return Err(ConnectorError::Dispatch(format!(
                "iotdb device path has empty segments: {rendered:?}"
            )));
        }
        Ok(segments.join("."))
    }
}

fn is_node_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// Check `${...}` syntax without rendering: every variable closes,
/// is non-empty, and names a known variable (`topic`, `client_id`,
/// `qos`, `timestamp`) or a `payload.<field>` extraction.
fn check_template_vars(template: &str) -> Result<()> {
    let mut rest = template;
    while let Some(start) = rest.find("${") {
        let after = &rest[start + 2..];
        let close = after.find('}').ok_or_else(|| {
            ConnectorError::Dispatch(format!("unclosed template variable in {template:?}"))
        })?;
        let name = &after[..close];
        if name.is_empty() {
            return Err(ConnectorError::Dispatch(format!(
                "empty template variable in {template:?}"
            )));
        }
        let known = matches!(name, "topic" | "client_id" | "qos" | "timestamp")
            || name.starts_with("payload.");
        if !known {
            return Err(ConnectorError::Dispatch(format!(
                "unknown template variable {name:?} in {template:?}"
            )));
        }
        rest = &after[close + 1..];
    }
    Ok(())
}

/// Sanitize one path segment into a valid node identifier.
fn sanitize_node(segment: &str) -> String {
    let mut cleaned: String = segment
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if cleaned.starts_with(|c: char| c.is_ascii_digit()) {
        cleaned.insert(0, '_');
    }
    cleaned
}

// ---------------------------------------------------------------------------
// Wire framing.
// ---------------------------------------------------------------------------

/// One tablet write: device, rows, shared schema.
#[derive(Debug, Clone, PartialEq)]
pub struct IotDbTabletRequest {
    pub device: String,
    pub is_aligned: bool,
    pub timestamps: Vec<i64>,
    pub measurements: Vec<String>,
    pub data_types: Vec<IotDbDataType>,
    /// Row-major values, one vector per row.
    pub values: Vec<Vec<serde_json::Value>>,
}

/// Render the `insertTablet` JSON body. Field names match the
/// REST API (`is_aligned`, `data_types`). Values use column-major
/// order (one array per measurement).
pub fn render_tablet_body(tablet: &IotDbTabletRequest) -> Vec<u8> {
    let mut body = String::from("{\"device\":");
    body.push_str(&serde_json::to_string(&tablet.device).unwrap_or_default());
    body.push_str(",\"is_aligned\":");
    body.push_str(if tablet.is_aligned { "true" } else { "false" });
    body.push_str(",\"timestamps\":");
    body.push_str(&serde_json::to_string(&tablet.timestamps).unwrap_or_default());
    body.push_str(",\"measurements\":");
    body.push_str(&serde_json::to_string(&tablet.measurements).unwrap_or_default());
    body.push_str(",\"data_types\":[");
    for (index, data_type) in tablet.data_types.iter().enumerate() {
        if index > 0 {
            body.push(',');
        }
        body.push_str(&serde_json::to_string(data_type.as_str()).unwrap_or_default());
    }
    body.push_str("],\"values\":");
    // Transpose row-major rows to column-major columns.
    let mut columns: Vec<Vec<serde_json::Value>> =
        vec![Vec::with_capacity(tablet.values.len()); tablet.measurements.len()];
    for row in &tablet.values {
        for (index, value) in row.iter().enumerate() {
            if let Some(column) = columns.get_mut(index) {
                column.push(value.clone());
            }
        }
    }
    body.push_str(&serde_json::to_string(&columns).unwrap_or_default());
    body.push('}');
    body.into_bytes()
}

// ---------------------------------------------------------------------------
// Transport.
// ---------------------------------------------------------------------------

/// Scripted outcome for the mock transport.
#[derive(Debug, Clone)]
pub enum MockIotDbOutcome {
    Ok,
    /// Transport failure (retries in-loop).
    ConnectionError(String),
    /// HTTP failure (305/500..=504 retry the batch).
    HttpStatus(u16),
}

/// One captured tablet write.
#[derive(Debug, Clone)]
pub struct CapturedIotDbTablet {
    pub tablet: IotDbTabletRequest,
    pub auth: String,
}

#[async_trait]
pub trait IotDbTransport: Send + Sync {
    async fn insert_tablet(&self, req: IotDbTabletRequest, auth: &IotDbAuth) -> Result<()>;
}

/// In-memory transport with scripted outcomes (tests, dry runs).
#[derive(Debug, Default)]
pub struct MockIotDbTransport {
    scripted: parking_lot::Mutex<std::collections::VecDeque<MockIotDbOutcome>>,
    captured: parking_lot::Mutex<Vec<CapturedIotDbTablet>>,
    calls: AtomicU64,
}

impl MockIotDbTransport {
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue outcomes consumed in order (default: success).
    pub fn script_outcomes(&self, outcomes: Vec<MockIotDbOutcome>) {
        *self.scripted.lock() = outcomes.into_iter().collect();
    }

    pub fn captured(&self) -> Vec<CapturedIotDbTablet> {
        self.captured.lock().clone()
    }

    pub fn calls(&self) -> u64 {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl IotDbTransport for MockIotDbTransport {
    async fn insert_tablet(&self, req: IotDbTabletRequest, auth: &IotDbAuth) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.captured.lock().push(CapturedIotDbTablet {
            tablet: req,
            auth: auth.header_value().unwrap_or_default(),
        });
        match self.scripted.lock().pop_front() {
            None | Some(MockIotDbOutcome::Ok) => Ok(()),
            Some(MockIotDbOutcome::ConnectionError(message)) => {
                Err(ConnectorError::Connection(message))
            }
            Some(MockIotDbOutcome::HttpStatus(status)) => Err(match status {
                305 | 500..=504 => {
                    ConnectorError::Connection(format!("mock iotdb throttled with {status}"))
                }
                _ => ConnectorError::Dispatch(format!("mock iotdb failed with {status}")),
            }),
        }
    }
}

/// Production transport: `POST {tablet-url}` with the tablet JSON.
pub struct HttpIotDbTransport {
    url: String,
    client: reqwest::Client,
}

impl HttpIotDbTransport {
    pub fn new(config: &IotDbSinkConfig, client: reqwest::Client) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            url: config.tablet_url(),
            client,
        })
    }
}

#[async_trait]
impl IotDbTransport for HttpIotDbTransport {
    async fn insert_tablet(&self, req: IotDbTabletRequest, auth: &IotDbAuth) -> Result<()> {
        let response = self
            .client
            .post(&self.url)
            .header(reqwest::header::AUTHORIZATION, auth.header_value()?)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(render_tablet_body(&req))
            .send()
            .await
            .map_err(|e| ConnectorError::Connection(format!("iotdb insert failed: {e}")))?;
        let status = response.status().as_u16();
        // Read the body once: each error below carries the server text.
        let raw = response
            .bytes()
            .await
            .map_err(|e| ConnectorError::Connection(format!("iotdb read failed: {e}")))?;
        // Reason: 800 chars keep the server text in the error without
        // storing a full error page per failed flush.
        let text: String = String::from_utf8_lossy(&raw).chars().take(800).collect();
        if status == 305 || (500..=504).contains(&status) {
            return Err(ConnectorError::Connection(format!(
                "iotdb throttled with {status}: {text}"
            )));
        }
        if !(200..=299).contains(&status) {
            return Err(ConnectorError::Dispatch(format!(
                "iotdb insert failed with {status}: {text}"
            )));
        }
        // The server answers 200 with a JSON code: a failing code is a
        // terminal failure, never a counted success.
        if !raw.is_empty() {
            if let Ok(doc) = serde_json::from_slice::<serde_json::Value>(&raw) {
                if let Some(code) = doc.get("code").and_then(|v| v.as_i64()) {
                    if code != 200 {
                        let message = doc.get("message").and_then(|v| v.as_str()).unwrap_or("");
                        return Err(ConnectorError::Dispatch(format!(
                            "iotdb insert failed with code {code} {message}"
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Sink.
// ---------------------------------------------------------------------------

/// One buffered row: device, timestamp, coerced values.
#[derive(Debug, Clone)]
struct IotDbRow {
    device: String,
    timestamp_ms: i64,
    values: Vec<serde_json::Value>,
}

struct IotDbBuffer {
    queue: BatchQueue<IotDbRow>,
    bytes: usize,
}

/// IoTDB sink: buffers rows, writes tablets grouped by device.
pub struct IotDbSink {
    config: IotDbSinkConfig,
    transport: Arc<dyn IotDbTransport>,
    buffer: parking_lot::Mutex<IotDbBuffer>,
    backoff: parking_lot::Mutex<BackoffState>,
    sent_batches: AtomicU64,
    sent_records: AtomicU64,
}

impl IotDbSink {
    pub fn new(config: IotDbSinkConfig, transport: Arc<dyn IotDbTransport>) -> Result<Self> {
        config.validate()?;
        let linger = config.effective_linger();
        Ok(Self {
            buffer: parking_lot::Mutex::new(IotDbBuffer {
                queue: BatchQueue::new(config.effective_batch_size(), linger),
                bytes: 0,
            }),
            config,
            transport,
            backoff: parking_lot::Mutex::new(BackoffState::default()),
            sent_batches: AtomicU64::new(0),
            sent_records: AtomicU64::new(0),
        })
    }

    pub fn config(&self) -> &IotDbSinkConfig {
        &self.config
    }

    pub fn sent_batches(&self) -> u64 {
        self.sent_batches.load(Ordering::Relaxed)
    }

    pub fn sent_records(&self) -> u64 {
        self.sent_records.load(Ordering::Relaxed)
    }

    pub fn buffered_rows(&self) -> usize {
        self.buffer.lock().queue.len()
    }

    fn backoff_delay(&self, attempt: usize) -> Duration {
        let initial = self.config.initial_backoff_ms.unwrap_or(100).max(1);
        let max = self.config.max_backoff_ms.unwrap_or(2_000).max(1);
        let grown = initial
            .saturating_mul(2u64.saturating_pow(attempt.min(10) as u32))
            .min(max);
        let jitter = (now_millis().max(0) as u64) % (grown / 2 + 1);
        Duration::from_millis(grown.saturating_add(jitter).min(max.saturating_mul(2)))
    }

    /// Build one row: device path plus one coerced value per
    /// measurement (missing fields fail loudly).
    fn build_row(
        &self,
        topic: &Topic,
        payload: &Bytes,
        qos: QoS,
        millis: i64,
    ) -> Result<(IotDbRow, usize)> {
        let text = std::str::from_utf8(payload)
            .map_err(|_| ConnectorError::Dispatch("iotdb payload must be UTF-8".to_string()))?;
        let value: serde_json::Value = serde_json::from_str(text)
            .map_err(|_| ConnectorError::Dispatch("iotdb payload must be JSON".to_string()))?;
        let device = self
            .config
            .resolve_device_path(topic.as_str(), payload, qos, millis)?;
        let mut values = Vec::with_capacity(self.config.measurements.len());
        for (measurement, data_type) in self
            .config
            .measurements
            .iter()
            .zip(self.config.data_types.iter())
        {
            let field = value
                .get(measurement)
                .or_else(|| value.get("payload").and_then(|p| p.get(measurement)))
                .ok_or_else(|| {
                    ConnectorError::Dispatch(format!(
                        "iotdb payload lacks measurement {measurement:?}"
                    ))
                })?;
            values.push(data_type.coerce(field)?);
        }
        let bytes = device.len() + text.len();
        Ok((
            IotDbRow {
                device,
                timestamp_ms: millis,
                values,
            },
            bytes,
        ))
    }

    /// Flush buffered rows grouped by device (no-op when empty).
    /// Transient failures retry in place; terminal failures and
    /// exhaustion restore the buffer, engage backoff, and propagate.
    pub async fn flush(&self) -> Result<()> {
        self.backoff.lock().check()?;
        let (rows, oldest, taken_bytes) = {
            let mut buffer = self.buffer.lock();
            let (rows, oldest) = buffer.queue.take_batch();
            let taken = std::mem::replace(&mut buffer.bytes, 0);
            (rows, oldest, taken)
        };
        if rows.is_empty() {
            return Ok(());
        }
        let mut groups: Vec<(String, Vec<IotDbRow>)> = Vec::new();
        for row in &rows {
            match groups.iter_mut().find(|(device, _)| device == &row.device) {
                Some((_, grouped)) => grouped.push(row.clone()),
                None => groups.push((row.device.clone(), vec![row.clone()])),
            }
        }
        let record_count = rows.len() as u64;
        let max_retries = self.config.max_retries.unwrap_or(usize::MAX);
        let mut attempt = 0usize;
        loop {
            let mut outcome: Result<()> = Ok(());
            for (device, grouped) in &groups {
                let tablet = IotDbTabletRequest {
                    device: device.clone(),
                    is_aligned: self.config.is_aligned,
                    timestamps: grouped.iter().map(|row| row.timestamp_ms).collect(),
                    measurements: self.config.measurements.clone(),
                    data_types: self.config.data_types.clone(),
                    values: grouped.iter().map(|row| row.values.clone()).collect(),
                };
                if let Err(e) = self
                    .transport
                    .insert_tablet(tablet, &self.config.auth)
                    .await
                {
                    outcome = Err(e);
                    break;
                }
            }
            match outcome {
                Ok(()) => {
                    self.backoff.lock().success();
                    self.sent_batches
                        .fetch_add(groups.len() as u64, Ordering::Relaxed);
                    self.sent_records.fetch_add(record_count, Ordering::Relaxed);
                    return Ok(());
                }
                Err(ConnectorError::Connection(message)) => {
                    if attempt >= max_retries {
                        return self.restore_err(
                            rows,
                            oldest,
                            taken_bytes,
                            ConnectorError::Connection(message),
                        );
                    }
                    attempt += 1;
                    tokio::time::sleep(self.backoff_delay(attempt)).await;
                }
                Err(e) => {
                    return self.restore_err(rows, oldest, taken_bytes, e);
                }
            }
        }
    }

    fn restore_err(
        &self,
        rows: Vec<IotDbRow>,
        oldest: Option<std::time::Instant>,
        bytes: usize,
        error: ConnectorError,
    ) -> Result<()> {
        let mut buffer = self.buffer.lock();
        buffer.queue.restore(rows, oldest);
        buffer.bytes = buffer.bytes.saturating_add(bytes);
        self.backoff.lock().failure();
        Err(error)
    }

    /// Validate + buffer one event. Returns true when the batch is
    /// full, stale, or over the byte limit (caller flushes).
    fn buffer_row(&self, topic: &Topic, payload: &Bytes, qos: QoS) -> Result<bool> {
        if topic.as_str().is_empty() {
            return Err(ConnectorError::Dispatch(
                "iotdb row requires a non-empty topic".to_string(),
            ));
        }
        let millis = now_millis();
        let (row, bytes) = self.build_row(topic, payload, qos, millis)?;
        let mut buffer = self.buffer.lock();
        let full = buffer.queue.push(row);
        buffer.bytes = buffer.bytes.saturating_add(bytes);
        Ok(full || buffer.bytes >= self.config.effective_batch_bytes())
    }
}

#[async_trait]
impl Sink for IotDbSink {
    async fn send(&self, topic: &Topic, payload: &Bytes, qos: QoS) -> Result<()> {
        if self.buffer_row(topic, payload, qos)? {
            self.flush().await?;
        }
        Ok(())
    }

    fn kind(&self) -> &'static str {
        "iotdb"
    }
}

/// Management connector handle pairing an id with an IoTDB sink.
pub struct IotDbConnector {
    id: String,
    sink: Arc<IotDbSink>,
}

impl IotDbConnector {
    pub fn new(id: impl Into<String>, sink: Arc<IotDbSink>) -> Self {
        Self {
            id: id.into(),
            sink,
        }
    }
}

impl broker_connectors::Connector for IotDbConnector {
    fn connector_id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> &'static str {
        self.sink.kind()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_connectors::Sink;

    fn test_config() -> IotDbSinkConfig {
        IotDbSinkConfig {
            endpoint: "http://127.0.0.1:18080/rest/v2".to_string(),
            device_path_template: "root.factory.${payload.plant_id}.${client_id}".to_string(),
            auth: IotDbAuth {
                username: "root".to_string(),
                password: "root".to_string(),
            },
            is_aligned: false,
            measurements: vec!["temperature".to_string(), "humidity".to_string()],
            data_types: vec![IotDbDataType::Float, IotDbDataType::Double],
            batch_size: Some(200),
            batch_bytes: Some(2_097_152),
            linger_ms: Some(10),
            max_retries: Some(3),
            initial_backoff_ms: Some(100),
            max_backoff_ms: Some(2_000),
            timeout_ms: None,
        }
    }

    fn test_sink(config: IotDbSinkConfig) -> (Arc<IotDbSink>, Arc<MockIotDbTransport>) {
        let transport = Arc::new(MockIotDbTransport::new());
        let sink = Arc::new(IotDbSink::new(config, transport.clone()).unwrap());
        (sink, transport)
    }

    #[test]
    fn test_config_validation() {
        let mut config = test_config();
        assert!(config.validate().is_ok());
        assert_eq!(
            config.tablet_url(),
            "http://127.0.0.1:18080/rest/v2/insertTablet"
        );

        config.endpoint = "127.0.0.1:18080".to_string();
        assert!(config.validate().is_err());
        config.endpoint = test_config().endpoint;

        config.device_path_template = "factory.plant1".to_string();
        assert!(config.validate().is_err(), "must stay rooted at root.");
        config.device_path_template = test_config().device_path_template;

        config.auth.username.clear();
        assert!(config.validate().is_err());
        config.auth.username = "root".to_string();

        config.measurements.clear();
        assert!(config.validate().is_err());
        config.measurements = vec!["temperature".to_string()];
        assert!(config.validate().is_err(), "types must align");
        config.measurements = test_config().measurements;

        config.data_types = vec![IotDbDataType::Float];
        assert!(config.validate().is_err(), "types must align");
        config.data_types = test_config().data_types;

        config.batch_size = Some(0);
        assert!(config.validate().is_err());
        // Unbounded + huge depths accepted: zero clamped ceilings.
        config.batch_size = None;
        config.batch_bytes = Some(10_000_000);
        config.max_retries = None;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_tablet_url_and_auth_header() {
        let config = test_config();
        assert_eq!(
            config.tablet_url(),
            "http://127.0.0.1:18080/rest/v2/insertTablet"
        );
        assert_eq!(config.auth.header_value().unwrap(), "Basic cm9vdDpyb290");
        let mut slashed = test_config();
        slashed.endpoint = "http://127.0.0.1:18080/rest/v2/".to_string();
        assert_eq!(
            slashed.tablet_url(),
            "http://127.0.0.1:18080/rest/v2/insertTablet"
        );
    }

    #[test]
    fn test_device_path_sanitization() {
        // Dots split levels; illegal characters become `_`.
        let mut templated = test_config();
        templated.device_path_template = "root.${topic}".to_string();
        assert_eq!(
            templated
                .resolve_device_path("a/b c", b"{}", QoS::AtMostOnce, 0)
                .unwrap(),
            "root.a_b_c"
        );
        // Leading digits gain a `_` prefix; MQTT slashes flatten
        // (dots in the template itself create levels).
        assert_eq!(
            templated
                .resolve_device_path("9lives/x", b"{}", QoS::AtMostOnce, 0)
                .unwrap(),
            "root._9lives_x"
        );
        assert_eq!(sanitize_node("ok_1"), "ok_1");
        assert_eq!(sanitize_node("9"), "_9");
    }

    #[test]
    fn test_type_coercion_rejects_mismatches() {
        // Strings never coerce into numeric columns.
        assert!(IotDbDataType::Int32
            .coerce(&serde_json::json!("abc"))
            .is_err());
        assert!(IotDbDataType::Double
            .coerce(&serde_json::json!(true))
            .is_err());
        // Numerics cross-coerce; bools stay strict.
        assert_eq!(
            IotDbDataType::Int64.coerce(&serde_json::json!(7)).unwrap(),
            serde_json::json!(7)
        );
        assert_eq!(
            IotDbDataType::Text.coerce(&serde_json::json!(7)).unwrap(),
            serde_json::json!("7")
        );
    }

    #[tokio::test]
    async fn test_tablet_grouping_and_types() {
        let (sink, transport) = test_sink(test_config());
        sink.send(
            &Topic::new("sensors/a").unwrap(),
            &Bytes::from_static(br#"{"client_id":"sensor42","plant_id":"plant1","temperature":24.5,"humidity":61.2}"#),
            QoS::AtMostOnce,
        )
        .await
        .unwrap();
        sink.send(
            &Topic::new("sensors/b").unwrap(),
            &Bytes::from_static(br#"{"client_id":"sensor42","plant_id":"plant1","temperature":25.5,"humidity":60.0}"#),
            QoS::AtMostOnce,
        )
        .await
        .unwrap();
        sink.flush().await.unwrap();

        // Same device: one tablet with two timestamped rows.
        let captured = transport.captured();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].tablet.device, "root.factory.plant1.sensor42");
        assert_eq!(captured[0].auth, "Basic cm9vdDpyb290");
        assert!(!captured[0].tablet.is_aligned);
        assert_eq!(captured[0].tablet.timestamps.len(), 2);
        assert_eq!(
            captured[0].tablet.measurements,
            vec!["temperature", "humidity"]
        );
        assert_eq!(
            captured[0].tablet.data_types,
            vec![IotDbDataType::Float, IotDbDataType::Double]
        );
        assert_eq!(captured[0].tablet.values.len(), 2);
        assert_eq!(
            captured[0].tablet.values[0][0],
            serde_json::json!(24.5f32 as f64)
        );
        assert_eq!(captured[0].tablet.values[1][1], serde_json::json!(60.0));

        // Rendered body matches the tablet contract.
        let body = String::from_utf8(render_tablet_body(&captured[0].tablet)).unwrap();
        let doc: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(doc["device"], "root.factory.plant1.sensor42");
        assert_eq!(doc["data_types"], serde_json::json!(["FLOAT", "DOUBLE"]));
        assert_eq!(doc["values"].as_array().expect("rows").len(), 2);
        assert_eq!(sink.sent_records(), 2);
    }

    #[tokio::test]
    async fn test_aligned_flag_and_missing_field() {
        let mut config = test_config();
        config.is_aligned = true;
        let (sink, transport) = test_sink(config);
        sink.send(
            &Topic::new("t").unwrap(),
            &Bytes::from_static(br#"{"temperature":1.0}"#),
            QoS::AtMostOnce,
        )
        .await
        .expect_err("humidity is missing");
        assert_eq!(sink.buffered_rows(), 0);
        assert_eq!(transport.calls(), 0);
    }

    #[tokio::test]
    async fn test_retry_on_305_then_success() {
        let mut config = test_config();
        config.batch_size = Some(10);
        config.initial_backoff_ms = Some(1);
        config.max_backoff_ms = Some(2);
        let (sink, transport) = test_sink(config);
        transport.script_outcomes(vec![
            MockIotDbOutcome::HttpStatus(305),
            MockIotDbOutcome::Ok,
        ]);

        sink.send(
            &Topic::new("t").unwrap(),
            &Bytes::from_static(
                br#"{"plant_id":"plant1","client_id":"sensor42","temperature":1.0,"humidity":2.0}"#,
            ),
            QoS::AtMostOnce,
        )
        .await
        .unwrap();
        sink.flush().await.unwrap();
        assert_eq!(transport.calls(), 2);
        assert_eq!(sink.sent_batches(), 1);
        assert_eq!(sink.buffered_rows(), 0);
    }

    #[tokio::test]
    async fn test_terminal_status_aborts() {
        let mut config = test_config();
        config.batch_size = Some(10);
        config.max_retries = Some(5);
        let (sink, transport) = test_sink(config);
        transport.script_outcomes(vec![MockIotDbOutcome::HttpStatus(400)]);

        sink.send(
            &Topic::new("t").unwrap(),
            &Bytes::from_static(
                br#"{"plant_id":"plant1","client_id":"sensor42","temperature":1.0,"humidity":2.0}"#,
            ),
            QoS::AtMostOnce,
        )
        .await
        .unwrap();
        let err = sink.flush().await.expect_err("400 must fail");
        assert!(matches!(err, ConnectorError::Dispatch(_)));
        assert_eq!(transport.calls(), 1);
        assert_eq!(sink.buffered_rows(), 1);
    }

    /// It fails closed when the environment lacks the named address.
    fn qual_required(name: &str) -> String {
        std::env::var(name)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| panic!("{name} must name a real server for qualification"))
    }

    /// Append `/rest/v2` unless the endpoint already carries it.
    fn qual_rest_base(endpoint: &str) -> String {
        let trimmed = endpoint.trim_end_matches('/');
        if trimmed.ends_with("/rest/v2") {
            trimmed.to_string()
        } else {
            format!("{trimmed}/rest/v2")
        }
    }

    /// Strip `/rest/v2` for the `/ping` probe (it lives on the root).
    fn qual_server_root(endpoint: &str) -> String {
        let trimmed = endpoint.trim_end_matches('/');
        match trimmed.strip_suffix("/rest/v2") {
            Some(root) => root.to_string(),
            None => trimmed.to_string(),
        }
    }

    /// Run one `nonQuery` statement and answer with the raw body text.
    async fn qual_non_query(
        client: &reqwest::Client,
        rest: &str,
        auth: &str,
        sql: &str,
    ) -> std::result::Result<String, String> {
        let response = client
            .post(format!("{rest}/nonQuery"))
            .header(reqwest::header::AUTHORIZATION, auth)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(serde_json::json!({"sql": sql}).to_string())
            .send()
            .await
            .map_err(|e| format!("qual nonQuery send failed: {e}"))?;
        let status = response.status().as_u16();
        let body = response
            .bytes()
            .await
            .map_err(|e| format!("qual nonQuery read failed: {e}"))?;
        let text = String::from_utf8_lossy(&body).to_string();
        if !(200..=299).contains(&status) {
            return Err(format!("qual nonQuery failed with {status}: {text}"));
        }
        if let Ok(doc) = serde_json::from_str::<serde_json::Value>(&text) {
            if let Some(code) = doc.get("code").and_then(|v| v.as_i64()) {
                if code != 200 {
                    let message = doc.get("message").and_then(|v| v.as_str()).unwrap_or("");
                    return Err(format!("qual nonQuery failed with code {code} {message}"));
                }
            }
        }
        Ok(text)
    }

    /// Run one `query` statement and answer with the parsed body.
    async fn qual_query(
        client: &reqwest::Client,
        rest: &str,
        auth: &str,
        sql: &str,
        row_limit: u64,
    ) -> serde_json::Value {
        let response = client
            .post(format!("{rest}/query"))
            .header(reqwest::header::AUTHORIZATION, auth)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(serde_json::json!({"sql": sql, "row_limit": row_limit}).to_string())
            .send()
            .await
            .expect("qual query sends");
        let status = response.status().as_u16();
        let body = response.bytes().await.expect("qual query reads");
        let text = String::from_utf8_lossy(&body).to_string();
        assert!(
            (200..=299).contains(&status),
            "qual query failed with {status}: {text}"
        );
        serde_json::from_str(&text).expect("qual query answers JSON")
    }

    /// This test qualifies the sink against a real server over the production
    /// tablet transport (DDL and read-back use the REST query entry).
    ///
    /// Run with e.g.:
    /// `IOTDB_ENDPOINT=http://127.0.0.1:18080 IOTDB_USER=root \
    ///  IOTDB_PASSWORD=root IOTDB_DEVICE=root.qual_b323 \
    ///  cargo +1.96.0 test -p broker-connectors-enterprise --all-features --lib iotdb::tests::test_qualify_write_path -- --ignored --nocapture --test-threads=1`
    ///
    /// - Write 500 rows through the broker rule path
    ///   ([`broker_connectors::ConnectorManager`] ->
    ///   [`IotDbSink`], Basic auth).
    /// - Assert the exact count and the first/last content through
    ///   `/rest/v2/query`.
    /// - Prove bad credentials fail as a dispatch error with the server text.
    /// - Delete the database.
    #[tokio::test]
    #[ignore = "needs a real server (see IOTDB_* env)"]
    async fn test_qualify_write_path() {
        let endpoint = qual_required("IOTDB_ENDPOINT");
        let username = qual_required("IOTDB_USER");
        let password = qual_required("IOTDB_PASSWORD");
        let device = qual_required("IOTDB_DEVICE");
        assert!(
            device.starts_with("root."),
            "qual device must start with root., got {device:?}"
        );
        let rest = qual_rest_base(&endpoint);
        let root = qual_server_root(&endpoint);
        // Reason: 30 s per request fails closed on a stalled server
        // instead of blocking the 2400 s qualification budget.
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("qual client builds");
        let auth = IotDbAuth {
            username: username.clone(),
            password: password.clone(),
        };
        let auth_header = auth.header_value().expect("qual auth builds");

        // Liveness: the `/ping` entry answers without credentials.
        let ping = client
            .get(format!("{root}/ping"))
            .send()
            .await
            .expect("qual ping sends");
        let ping_status = ping.status().as_u16();
        let ping_body = ping.bytes().await.expect("qual ping reads");
        let ping_text = String::from_utf8_lossy(&ping_body).to_string();
        assert_eq!(ping_status, 200, "qual ping: {ping_text}");
        assert!(
            ping_text.contains("SUCCESS_STATUS"),
            "qual ping must report success, got {ping_text:?}"
        );
        eprintln!("qual ping: {ping_text}");

        // Identity: only the real product answers `SHOW VERSION`.
        let version_doc = qual_query(&client, &rest, &auth_header, "SHOW VERSION", 10).await;
        let version_text = version_doc.to_string();
        assert!(
            version_text.to_lowercase().contains("version"),
            "qual version must name a version, got {version_text:?}"
        );
        let mut version_cells = Vec::new();
        if let Some(columns) = version_doc.get("values").and_then(|v| v.as_array()) {
            for column in columns {
                if let Some(cells) = column.as_array() {
                    for cell in cells {
                        if let Some(text) = cell.as_str() {
                            version_cells.push(text.to_string());
                        } else if cell.is_number() {
                            version_cells.push(cell.to_string());
                        }
                    }
                }
            }
        }
        assert!(
            !version_cells.is_empty(),
            "qual version must list rows, got {version_text:?}"
        );
        let version_line = version_cells.join(" ");
        assert!(
            version_line.contains('.'),
            "qual version must read a dotted version, got {version_line:?}"
        );
        eprintln!("qual server: version={version_line}");

        // Stale state from a prior run goes first (tolerated: the
        // database may not exist yet).
        match qual_non_query(
            &client,
            &rest,
            &auth_header,
            &format!("DELETE DATABASE {device}"),
        )
        .await
        {
            Ok(body) => eprintln!("qual cleanup stale: {body}"),
            Err(e) => eprintln!("qual cleanup stale (tolerated): {e}"),
        }
        // The database for this run exists before the first flush.
        match qual_non_query(
            &client,
            &rest,
            &auth_header,
            &format!("CREATE DATABASE {device}"),
        )
        .await
        {
            Ok(body) => eprintln!("qual create database: {body}"),
            Err(e)
                if e.to_lowercase().contains("exist") || e.to_lowercase().contains("already") =>
            {
                eprintln!("qual create database (tolerated): {e}")
            }
            Err(e) => panic!("qual create database: {e}"),
        }

        // Reason: 200 rows keep one tablet a single small request while
        // spreading per-request overhead across the batch.
        let config = IotDbSinkConfig {
            endpoint: rest.clone(),
            device_path_template: device.clone(),
            auth: auth.clone(),
            is_aligned: false,
            measurements: vec!["seq".to_string(), "reading".to_string()],
            data_types: vec![IotDbDataType::Int64, IotDbDataType::Double],
            batch_size: Some(200),
            batch_bytes: Some(2_097_152),
            linger_ms: Some(10),
            max_retries: Some(3),
            initial_backoff_ms: Some(100),
            max_backoff_ms: Some(2_000),
            timeout_ms: Some(30_000),
        };
        config.validate().expect("qual config validates");
        // The production transport below posts tablet JSON itself.
        let transport = Arc::new(
            HttpIotDbTransport::new(&config, client.clone()).expect("qual transport builds"),
        );
        let sink = Arc::new(IotDbSink::new(config.clone(), transport).expect("qual sink builds"));
        assert_eq!(sink.kind(), "iotdb");
        // Broker path: rule actions send through the shared manager.
        // The qual sends through it, never `sink.send` direct.
        let manager = Arc::new(broker_connectors::ConnectorManager::new());
        manager.register("qual-iotdb", sink.clone());

        const ROWS: usize = 500;
        let topic = Topic::new("qual/iotdb/b323").unwrap();
        for seq in 0..ROWS {
            // Reason: 20.0 + seq * 0.5 stays exact in binary floating
            // point, so the read-back compares without rounding doubt.
            let reading = 20.0 + seq as f64 * 0.5;
            let payload = Bytes::from(format!(r#"{{"seq":{seq},"reading":{reading:.1}}}"#));
            manager
                .send("qual-iotdb", &topic, &payload, QoS::AtLeastOnce)
                .await
                .expect("qual send");
            // Reason: one row per 2 ms keeps timestamps unique (the
            // server keys rows by time) for 500 rows in about one second.
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        sink.flush().await.expect("qual flush");
        assert_eq!(sink.sent_records(), ROWS as u64, "qual row count");
        eprintln!("qual rows sent: records={ROWS} device={device}");

        // The read-back uses the server query entry (column-major
        // `values`: one array per selected measurement).
        let select = format!("SELECT seq, reading FROM {device}");
        let mut doc = serde_json::Value::Null;
        for _ in 0..30 {
            let current = qual_query(&client, &rest, &auth_header, &select, 1000).await;
            let got = current
                .get("timestamps")
                .and_then(|v| v.as_array())
                .map(Vec::len)
                .unwrap_or(0);
            doc = current;
            if got == ROWS {
                break;
            }
            // Reason: 1 s between polls lets a synchronous write become
            // visible without busy polling the server.
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let timestamps = doc
            .get("timestamps")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(timestamps.len(), ROWS, "qual count: {doc}");
        let columns = doc
            .get("values")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert_eq!(columns.len(), 2, "qual columns: {doc}");
        let seq_column = columns[0].as_array().cloned().unwrap_or_default();
        let reading_column = columns[1].as_array().cloned().unwrap_or_default();
        assert_eq!(seq_column.len(), ROWS, "qual seq column: {doc}");
        assert_eq!(reading_column.len(), ROWS, "qual reading column: {doc}");
        let mut pairs: Vec<(i64, f64)> = seq_column
            .iter()
            .zip(reading_column.iter())
            .map(|(seq_value, reading_value)| {
                (
                    seq_value.as_i64().expect("qual seq parses"),
                    reading_value.as_f64().expect("qual reading parses"),
                )
            })
            .collect();
        pairs.sort_by_key(|(seq, _)| *seq);
        assert_eq!(
            pairs.first().map(|(seq, _)| *seq),
            Some(0),
            "qual first seq"
        );
        assert_eq!(
            pairs.last().map(|(seq, _)| *seq),
            Some((ROWS - 1) as i64),
            "qual last seq"
        );
        // Reason: 1e-6 absorbs JSON round-trip noise while exact keys
        // still compare (at-least-once permits duplicates, never loss).
        assert!(
            (pairs[0].1 - 20.0).abs() < 1e-6,
            "qual first reading: {:?}",
            pairs[0]
        );
        let last_reading = 20.0 + (ROWS - 1) as f64 * 0.5;
        assert!(
            (pairs[ROWS - 1].1 - last_reading).abs() < 1e-6,
            "qual last reading: {:?}",
            pairs[ROWS - 1]
        );
        eprintln!(
            "qual rows asserted: count={ROWS} first=(0,20.0) last=({},{last_reading})",
            ROWS - 1
        );

        // Fault: bad credentials fail closed with the server text.
        let bad_auth = IotDbAuth {
            username: username.clone(),
            password: "qual-wrong-password".to_string(),
        };
        let bad_header = bad_auth.header_value().expect("qual bad auth builds");
        let probe = client
            .post(format!("{rest}/query"))
            .header(reqwest::header::AUTHORIZATION, &bad_header)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(serde_json::json!({"sql": "SHOW VERSION", "row_limit": 1}).to_string())
            .send()
            .await
            .expect("qual bad probe sends");
        let probe_status = probe.status().as_u16();
        let probe_body = probe.bytes().await.expect("qual bad probe reads");
        let probe_text = String::from_utf8_lossy(&probe_body).to_string();
        assert_eq!(probe_status, 401, "qual bad probe: {probe_text}");
        let server_token = serde_json::from_str::<serde_json::Value>(&probe_text)
            .ok()
            .and_then(|parsed| {
                parsed
                    .get("message")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
            })
            .filter(|message| !message.trim().is_empty())
            .unwrap_or_else(|| "401".to_string());
        let bad_config = IotDbSinkConfig {
            auth: bad_auth,
            ..config
        };
        let bad_transport = Arc::new(
            HttpIotDbTransport::new(&bad_config, client.clone())
                .expect("qual bad transport builds"),
        );
        let bad_sink =
            Arc::new(IotDbSink::new(bad_config, bad_transport).expect("qual bad sink builds"));
        let bad_manager = Arc::new(broker_connectors::ConnectorManager::new());
        bad_manager.register("qual-iotdb-bad", bad_sink.clone());
        bad_manager
            .send(
                "qual-iotdb-bad",
                &topic,
                &Bytes::from_static(br#"{"seq":1,"reading":20.5}"#),
                QoS::AtLeastOnce,
            )
            .await
            .expect("qual bad buffer");
        let err = bad_sink.flush().await.expect_err("bad password must fail");
        match &err {
            ConnectorError::Dispatch(message) => assert!(
                message.contains(&server_token),
                "qual fault must carry the server text, got {message:?} (server said {server_token:?})"
            ),
            other => panic!("bad password must be terminal, got {other:?}"),
        }
        eprintln!("qual fault asserted: bad password is a dispatch error");

        // Cleanup: delete the database for this run (tolerated: the
        // next run deletes stale state first).
        match qual_non_query(
            &client,
            &rest,
            &auth_header,
            &format!("DELETE DATABASE {device}"),
        )
        .await
        {
            Ok(body) => eprintln!("qual cleanup: deleted {device}: {body}"),
            Err(e) => eprintln!("qual cleanup FAILED for {device} (tolerated): {e}"),
        }
        eprintln!("qual done: rows={ROWS} device={device} cleaned database");
    }
}
