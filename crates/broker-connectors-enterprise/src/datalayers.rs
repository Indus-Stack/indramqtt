//! Datalayers industrial time-series sink (INDRA-177).
//!
//! Industrial edge-to-cloud time-series database sink with batched records,
//! Bearer token or Basic authentication, tag/field segregation,
//! microsecond timestamps, and HTTP status code classification.
//!
//! The write path runs on the maintained `reqwest` driver (the vendor
//! publishes no maintained Rust driver): `POST {endpoint}/write?db={database}&precision=us`
//! with `Content-Type: text/plain`, an optional
//! `Authorization: Bearer <token>` or `Authorization: Basic <base64>`
//! header, and an Influx line protocol body
//! `{table},<tags> <fields> <time>` where `time` is
//! microseconds since the Unix epoch. Database management and
//! query-back for qualification run on the same driver against
//! `POST {endpoint}/api/v1/sql[?db={database}]` with the same header
//! (see the open-dialect notes below where the documented dialect is open).

use async_trait::async_trait;
use base64::Engine as _;
use broker_protocol::{QoS, Topic};
use bytes::Bytes;
use parking_lot::Mutex;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use broker_connectors::{
    now_millis, BackoffState, BatchQueue, Connector, ConnectorError, Result, Sink,
};

fn default_batch_size_500() -> Option<usize> {
    Some(500)
}

/// Default outer backlog ceiling: 10_000 rows (about twenty 500-row
/// flushes) so a burst or a stalled server cannot grow the queue
/// without bound, while steady throughput still fits in memory
/// (10_000 small JSON records stay well under tens of MiB).
fn default_buffer_capacity_10k() -> Option<usize> {
    Some(10_000)
}

/// Configuration for the Datalayers industrial time-series sink.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatalayersConfig {
    /// Datalayers REST endpoint (e.g. `http://datalayers-node:8360`).
    pub endpoint: String,
    /// Target database name.
    pub database: String,
    /// Target measurement/table name.
    pub table: String,
    /// Optional Bearer token for authorization.
    #[serde(default)]
    pub auth_token: Option<String>,
    /// Optional username for Basic authentication.
    /// Use the exact field name of the reference specification.
    #[serde(default)]
    pub username: Option<String>,
    /// Optional password for Basic authentication.
    /// Use the exact field name of the reference specification.
    /// Never log this value. Never include it in an error.
    #[serde(default)]
    pub password: Option<String>,
    /// Optional field source for timestamp (defaults to current wall-clock).
    #[serde(default)]
    pub timestamp_field: Option<String>,
    /// Vector of column names to treat as indexed tags.
    #[serde(default)]
    pub tag_columns: Vec<String>,
    /// Vector of column names to treat as measurement fields.
    #[serde(default)]
    pub field_columns: Vec<String>,
    /// Write precision: `ns`, `us`, `ms` or `s`. `None` writes
    /// microseconds, the wire default of this sink.
    #[serde(default)]
    pub precision: Option<String>,
    /// Batch flush size (default 500: one HTTP POST carries at most
    /// this many records, so a single flush cannot grow without bound;
    /// 500 keeps the JSON body well under typical proxy limits while
    /// amortising one POST over hundreds of rows).
    #[serde(default = "default_batch_size_500")]
    pub batch_size: Option<usize>,
    /// In-memory queue backlog ceiling (`None` = default 10_000 rows:
    /// about twenty 500-row flushes, bounding worst-case backlog memory
    /// while absorbing bursts; set it to tune the backlog — when full
    /// the sink fails closed with a connection error instead of
    /// shedding rows silently).
    #[serde(default = "default_buffer_capacity_10k")]
    pub buffer_capacity: Option<usize>,
    /// Network request timeout in ms (default 5000).
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

impl std::fmt::Debug for DatalayersConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Show names, not secrets. Redact the token and the password.
        f.debug_struct("DatalayersConfig")
            .field("endpoint", &self.endpoint)
            .field("database", &self.database)
            .field("table", &self.table)
            .field(
                "auth_token",
                &self.auth_token.as_ref().map(|_| "[redacted]"),
            )
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "[redacted]"))
            .field("timestamp_field", &self.timestamp_field)
            .field("tag_columns", &self.tag_columns)
            .field("field_columns", &self.field_columns)
            .field("batch_size", &self.batch_size)
            .field("buffer_capacity", &self.buffer_capacity)
            .field("timeout_ms", &self.timeout_ms)
            .finish()
    }
}

impl DatalayersConfig {
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms.unwrap_or(5000).max(1))
    }

    pub fn validate(&self) -> Result<()> {
        if self.endpoint.trim().is_empty() {
            return Err(ConnectorError::Dispatch(
                "datalayers endpoint cannot be empty".into(),
            ));
        }
        if self.database.trim().is_empty() {
            return Err(ConnectorError::Dispatch(
                "datalayers database cannot be empty".into(),
            ));
        }
        if self.table.trim().is_empty() {
            return Err(ConnectorError::Dispatch(
                "datalayers table cannot be empty".into(),
            ));
        }
        // TODO(parity): the reference specification does not state
        // whether username without password (or the reverse) is valid;
        // this code refuses a half-set Basic pair and keeps moving.
        let has_bearer = self
            .auth_token
            .as_ref()
            .is_some_and(|t| !t.trim().is_empty());
        let has_user = self.username.as_ref().is_some_and(|u| !u.trim().is_empty());
        let has_pass = self.password.as_ref().is_some_and(|p| !p.is_empty());
        if has_bearer && (has_user || has_pass) {
            return Err(ConnectorError::Dispatch(
                "datalayers auth modes conflict: set either auth_token or username/password, not both"
                    .into(),
            ));
        }
        if has_user != has_pass {
            return Err(ConnectorError::Dispatch(
                "datalayers basic auth needs both username and password".into(),
            ));
        }
        Ok(())
    }

    pub fn auth_header_value(&self) -> Option<String> {
        let bearer = self
            .auth_token
            .as_ref()
            .map(|t| format!("Bearer {}", t.trim()))
            .filter(|h| h.len() > "Bearer ".len());
        if bearer.is_some() {
            return bearer;
        }
        match (&self.username, &self.password) {
            (Some(user), Some(pass)) if !user.trim().is_empty() && !pass.is_empty() => {
                let raw = format!("{user}:{pass}");
                let encoded = base64::engine::general_purpose::STANDARD.encode(raw);
                Some(format!("Basic {encoded}"))
            }
            _ => None,
        }
    }

    pub fn effective_batch_size(&self) -> usize {
        self.batch_size.unwrap_or(500).max(1)
    }

    /// Outer backlog ceiling (default 10_000 rows: about twenty
    /// 500-row flushes, bounding worst-case backlog memory while
    /// absorbing bursts; configure a finite value to tune it).
    pub fn effective_buffer_capacity(&self) -> usize {
        self.buffer_capacity.unwrap_or(10_000).max(1)
    }

    /// Write precision label: `us` unless the config names another.
    pub fn effective_precision(&self) -> &str {
        self.precision.as_deref().unwrap_or("us")
    }

    /// Write URL: `{endpoint}/write?db={database}&precision={precision}`.
    /// The default precision `us` matches the microsecond timestamps below.
    pub fn write_url(&self) -> String {
        format!(
            "{}/write?db={}&precision={}",
            self.endpoint.trim_end_matches('/'),
            self.database,
            self.effective_precision()
        )
    }

    /// SQL URL for management/query-back: `{endpoint}/api/v1/sql`
    /// (`?db={database}` when a database is selected).
    pub fn sql_url(&self, with_db: bool) -> String {
        let base = format!("{}/api/v1/sql", self.endpoint.trim_end_matches('/'));
        if with_db {
            format!("{base}?db={}", self.database)
        } else {
            base
        }
    }
}

/// A single time-series record in Datalayers payload format.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatalayersRecord {
    /// Timestamp in microseconds since Unix epoch.
    pub time: i64,
    /// Indexed string metadata tags.
    pub tags: HashMap<String, String>,
    /// Measurement numeric/string/bool metric fields.
    pub fields: HashMap<String, serde_json::Value>,
}

/// Request body sent to Datalayers `POST /write?db={database}&precision=us`.
/// The struct carries the batch. The transport renders it as line protocol.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatalayersWriteRequest {
    pub table: String,
    pub records: Vec<DatalayersRecord>,
}

/// Escape commas, spaces, newlines and carriage returns in names.
/// Reason: a raw newline splits one line into two records.
fn escape_measurement(name: &str) -> String {
    name.replace(',', "\\,")
        .replace(' ', "\\ ")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
}

/// Escape a tag key, tag value, or field key. Escapes commas, equals, spaces.
fn escape_tag_part(part: &str) -> String {
    part.replace(',', "\\,")
        .replace('=', "\\=")
        .replace(' ', "\\ ")
}

/// Format one field value. Returns `None` for null or empty.
fn format_field_value(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::Bool(b) => Some(if *b {
            "true".to_string()
        } else {
            "false".to_string()
        }),
        serde_json::Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                Some(format!("{n}i"))
            } else {
                Some(n.to_string())
            }
        }
        serde_json::Value::String(s) => {
            let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
            Some(format!("\"{escaped}\""))
        }
        other => {
            let raw = serde_json::to_string(other).unwrap_or_default();
            let escaped = raw.replace('\\', "\\\\").replace('"', "\\\"");
            Some(format!("\"{escaped}\""))
        }
    }
}

/// Render one record as one line protocol line. Returns `None` without fields.
fn format_record_line(table: &str, record: &DatalayersRecord) -> Option<String> {
    // Sort keys. The output stays stable across runs.
    let mut tags: Vec<(&String, &String)> = record.tags.iter().collect();
    tags.sort_by(|a, b| a.0.cmp(b.0));
    let mut fields: Vec<(&String, &serde_json::Value)> = record.fields.iter().collect();
    fields.sort_by(|a, b| a.0.cmp(b.0));
    let mut parts: Vec<String> = Vec::new();
    for (k, v) in &fields {
        if let Some(formatted) = format_field_value(v) {
            parts.push(format!("{}={}", escape_tag_part(k), formatted));
        }
    }
    if parts.is_empty() {
        return None;
    }
    let mut line = escape_measurement(table);
    for (k, v) in tags {
        line.push(',');
        line.push_str(&escape_tag_part(k));
        line.push('=');
        line.push_str(&escape_tag_part(v));
    }
    line.push(' ');
    line.push_str(&parts.join(","));
    line.push(' ');
    line.push_str(&record.time.to_string());
    Some(line)
}

/// Render a batch as line protocol. Skips records without fields.
pub fn write_request_to_lines(request: &DatalayersWriteRequest) -> String {
    request
        .records
        .iter()
        .filter_map(|r| format_record_line(&request.table, r))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Extract microseconds timestamp from payload or fallback to wall-clock.
///
/// Integer magnitudes decide the precision: 10 digits are seconds, 13
/// are milliseconds, 16 are microseconds (larger values pass through
/// as microseconds). Floats carry a fractional part: values below
/// 1e11 are seconds with micros in the fraction, below 1e14
/// milliseconds with micros in the fraction. Anything missing,
/// non-numeric, zero or negative falls back to wall-clock (fail
/// closed on time: never emit a non-positive timestamp).
/// TODO(parity): the documented write API does not state whether the
/// server accepts nanoseconds or string timestamps; both are mapped
/// to microseconds here and the question is open.
pub fn extract_microsecond_timestamp(json_val: &serde_json::Value, field_opt: Option<&str>) -> i64 {
    if let Some(field) = field_opt {
        if let Some(ts_val) = json_val.get(field) {
            if let Some(i) = ts_val.as_i64() {
                // If seconds, scale to micros; if millis, scale to micros; if micros, keep
                if i <= 0 {
                    // Fall through to wall-clock below.
                } else if i < 10_000_000_000 {
                    return i.saturating_mul(1_000_000);
                } else if i < 10_000_000_000_000 {
                    return i.saturating_mul(1_000);
                } else {
                    return i;
                }
            } else if let Some(f) = ts_val.as_f64() {
                if f > 0.0 && f.is_finite() {
                    if f < 10_000_000_000.0 {
                        return (f * 1_000_000.0) as i64;
                    } else if f < 10_000_000_000_000.0 {
                        return (f * 1_000.0) as i64;
                    } else {
                        return f as i64;
                    }
                }
            }
        }
    }
    now_millis().saturating_mul(1000).max(1)
}

/// Extract single Datalayers record from MQTT event.
pub fn extract_datalayers_record(
    payload: &[u8],
    topic: &str,
    config: &DatalayersConfig,
) -> Result<DatalayersRecord> {
    let json_val: serde_json::Value = serde_json::from_slice(payload)
        .map_err(|e| ConnectorError::Dispatch(format!("invalid JSON payload: {e}")))?;

    let time = extract_microsecond_timestamp(&json_val, config.timestamp_field.as_deref());

    let mut tags = HashMap::new();
    let mut fields = HashMap::new();

    if let serde_json::Value::Object(map) = &json_val {
        let empty_map = serde_json::Map::new();
        let payload_map = map
            .get("payload")
            .and_then(|p| p.as_object())
            .unwrap_or(&empty_map);

        for (k, v) in map {
            if k == "payload" {
                continue;
            }
            if config.tag_columns.contains(k) {
                let tag_str = match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                tags.insert(k.clone(), tag_str);
            } else if config.field_columns.is_empty() || config.field_columns.contains(k) {
                fields.insert(k.clone(), v.clone());
            }
        }
        for (k, v) in payload_map {
            if config.tag_columns.contains(k) {
                let tag_str = match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                tags.insert(k.clone(), tag_str);
            } else if config.field_columns.is_empty() || config.field_columns.contains(k) {
                fields.insert(k.clone(), v.clone());
            }
        }
    } else {
        fields.insert("val".to_string(), json_val);
    }

    if tags.is_empty() {
        tags.insert("topic".to_string(), topic.to_string());
    }

    Ok(DatalayersRecord { time, tags, fields })
}

/// Transport abstraction for Datalayers.
#[async_trait]
pub trait DatalayersTransport: Send + Sync {
    async fn write_batch(&self, request: &DatalayersWriteRequest) -> Result<()>;
}

/// Production HTTP transport for Datalayers (maintained `reqwest`
/// driver: connection pooling and timeouts owned by the client).
pub struct HttpDatalayersTransport {
    client: reqwest::Client,
    write_url: String,
    sql_url: String,
    sql_url_no_db: String,
    auth_header: Option<String>,
    precision: String,
}

impl HttpDatalayersTransport {
    pub fn new(config: &DatalayersConfig) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(config.timeout())
                .build()
                .unwrap_or_default(),
            write_url: config.write_url(),
            sql_url: config.sql_url(true),
            sql_url_no_db: config.sql_url(false),
            auth_header: config.auth_header_value(),
            precision: config.effective_precision().to_string(),
        }
    }

    pub fn with_client(config: &DatalayersConfig, client: reqwest::Client) -> Self {
        Self {
            client,
            write_url: config.write_url(),
            sql_url: config.sql_url(true),
            sql_url_no_db: config.sql_url(false),
            auth_header: config.auth_header_value(),
            precision: config.effective_precision().to_string(),
        }
    }

    pub fn write_url_str(&self) -> &str {
        &self.write_url
    }

    /// Terminal-vs-retryable mapping. 429 and 5xx retry in-loop;
    /// 401/403 are terminal (fail closed: a bad credential is never
    /// retried with the same value); every other non-2xx is terminal
    /// (schema/table mismatches must not loop).
    fn classify(status: reqwest::StatusCode, body_text: &str) -> Result<()> {
        if status.is_success() {
            Ok(())
        } else if status.as_u16() == 429 || status.as_u16() >= 500 {
            Err(ConnectorError::Connection(format!(
                "datalayers transient error {status}: {body_text}"
            )))
        } else if status.as_u16() == 401 || status.as_u16() == 403 {
            Err(ConnectorError::Dispatch(format!(
                "datalayers unauthorized {status}: failing closed, rotate the credential: {body_text}"
            )))
        } else {
            Err(ConnectorError::Dispatch(format!(
                "datalayers terminal error {status}: {body_text}"
            )))
        }
    }

    fn authed(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(ref auth) = self.auth_header {
            builder.header(AUTHORIZATION, auth)
        } else {
            builder
        }
    }

    async fn post_write(&self, request: &DatalayersWriteRequest) -> Result<()> {
        // Render the batch as line protocol. An empty batch fails
        // closed without a request.
        let body = if self.precision == "us" {
            write_request_to_lines(request)
        } else {
            // Record times are microseconds; convert to the precision
            // named in the write URL.
            request
                .records
                .iter()
                .filter_map(|record| {
                    let mut record = record.clone();
                    record.time = match self.precision.as_str() {
                        "ns" => record.time.saturating_mul(1_000),
                        "ms" => record.time / 1_000,
                        "s" => record.time / 1_000_000,
                        _ => record.time,
                    };
                    format_record_line(&request.table, &record)
                })
                .collect::<Vec<_>>()
                .join("\n")
        };
        if body.is_empty() {
            return Err(ConnectorError::Dispatch(
                "datalayers write batch has no fields".into(),
            ));
        }
        let resp = self
            .authed(
                self.client
                    .post(&self.write_url)
                    .header(CONTENT_TYPE, "text/plain")
                    .body(body),
            )
            .send()
            .await
            .map_err(|e| {
                ConnectorError::Connection(format!("datalayers http request failed: {e}"))
            })?;
        let status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();
        Self::classify(status, &body_text)
    }

    /// Execute one SQL statement (DDL, INSERT, SELECT) and return the
    /// raw response body. Used for `CREATE DATABASE`, table setup and
    /// query-back in qualification.
    /// TODO(parity): the documented management dialect for
    /// `CREATE DATABASE` / `SELECT COUNT(*)` over this endpoint is
    /// open; the statements here follow the server's SQL reference
    /// and the response parser below accepts every shape observed.
    pub async fn execute_sql(&self, sql: &str) -> Result<Vec<u8>> {
        let resp = self
            .authed(
                self.client
                    .post(&self.sql_url)
                    .header(CONTENT_TYPE, "application/binary")
                    .body(sql.to_string()),
            )
            .send()
            .await
            .map_err(|e| {
                ConnectorError::Connection(format!("datalayers sql request failed: {e}"))
            })?;
        let status = resp.status();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| ConnectorError::Connection(format!("datalayers sql read failed: {e}")))?;
        let body_text = String::from_utf8_lossy(&bytes);
        Self::classify(status, &body_text)?;
        Ok(bytes.to_vec())
    }

    /// Execute one SQL statement without a database selected (for
    /// `CREATE DATABASE` before the database exists).
    pub async fn execute_sql_no_db(&self, sql: &str) -> Result<Vec<u8>> {
        let resp = self
            .authed(
                self.client
                    .post(&self.sql_url_no_db)
                    .header(CONTENT_TYPE, "application/binary")
                    .body(sql.to_string()),
            )
            .send()
            .await
            .map_err(|e| {
                ConnectorError::Connection(format!("datalayers sql request failed: {e}"))
            })?;
        let status = resp.status();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| ConnectorError::Connection(format!("datalayers sql read failed: {e}")))?;
        let body_text = String::from_utf8_lossy(&bytes);
        Self::classify(status, &body_text)?;
        Ok(bytes.to_vec())
    }
}

/// Parse a `SELECT COUNT(*)` result into its row count. Accepts the
/// shapes observed on the wire (`{"result":{"values":[[n]]}}`,
/// `{"result":{"data_array":[[n]]}}`, `{"count":n}`, `{"n":n}`,
/// bare `n`); anything else is a connection-grade parse failure so
/// qualification fails closed instead of asserting a wrong count.
/// TODO(parity): the exact COUNT(*) JSON shape is open; this parser
/// accepts every documented variant and rejects the rest loudly.
pub fn parse_count_result(body: &[u8]) -> Result<u64> {
    let doc: serde_json::Value = serde_json::from_slice(body)
        .map_err(|e| ConnectorError::Connection(format!("datalayers bad count JSON: {e}")))?;
    if let Some(n) = doc
        .get("count")
        .or_else(|| doc.get("n"))
        .and_then(|v| v.as_u64())
    {
        return Ok(n);
    }
    if let Some(n) = doc.as_u64() {
        return Ok(n);
    }
    let nested = doc
        .get("result")
        .and_then(|r| r.get("values").or_else(|| r.get("data_array")))
        .and_then(|v| v.as_array())
        .and_then(|rows| rows.first())
        .and_then(|row| row.as_array())
        .and_then(|cells| cells.first());
    if let Some(cell) = nested {
        if let Some(n) = cell.as_u64() {
            return Ok(n);
        }
        if let Some(s) = cell.as_str() {
            if let Ok(n) = s.trim().parse::<u64>() {
                return Ok(n);
            }
        }
        if let Some(n) = cell.as_i64().and_then(|v| u64::try_from(v).ok()) {
            return Ok(n);
        }
    }
    Err(ConnectorError::Connection(format!(
        "datalayers count result has no countable cell: {}",
        String::from_utf8_lossy(body)
    )))
}

#[async_trait]
impl DatalayersTransport for HttpDatalayersTransport {
    async fn write_batch(&self, request: &DatalayersWriteRequest) -> Result<()> {
        self.post_write(request).await
    }
}

/// Mock transport for testing Datalayers sink.
pub struct MockDatalayersTransport {
    pub captured_requests: Mutex<Vec<DatalayersWriteRequest>>,
    pub fail_count: Mutex<usize>,
    pub is_terminal: Mutex<bool>,
}

impl MockDatalayersTransport {
    pub fn new() -> Self {
        Self {
            captured_requests: Mutex::new(Vec::new()),
            fail_count: Mutex::new(0),
            is_terminal: Mutex::new(false),
        }
    }

    pub fn with_transient_failures(failures: usize) -> Self {
        Self {
            captured_requests: Mutex::new(Vec::new()),
            fail_count: Mutex::new(failures),
            is_terminal: Mutex::new(false),
        }
    }

    pub fn with_terminal_failure() -> Self {
        Self {
            captured_requests: Mutex::new(Vec::new()),
            fail_count: Mutex::new(1),
            is_terminal: Mutex::new(true),
        }
    }
}

impl Default for MockDatalayersTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DatalayersTransport for MockDatalayersTransport {
    async fn write_batch(&self, request: &DatalayersWriteRequest) -> Result<()> {
        self.captured_requests.lock().push(request.clone());

        let mut fails = self.fail_count.lock();
        if *fails > 0 {
            *fails -= 1;
            if *self.is_terminal.lock() {
                return Err(ConnectorError::Dispatch(
                    "mock datalayers 400 bad request".into(),
                ));
            } else {
                return Err(ConnectorError::Connection(
                    "mock datalayers 503 service unavailable".into(),
                ));
            }
        }

        Ok(())
    }
}

/// Datalayers industrial time-series sink.
pub struct DatalayersSink {
    config: DatalayersConfig,
    transport: Arc<dyn DatalayersTransport>,
    queue: Mutex<BatchQueue<DatalayersRecord>>,
    backoff: Mutex<BackoffState>,
    sent: AtomicU64,
}

impl DatalayersSink {
    pub fn new(config: DatalayersConfig, transport: Arc<dyn DatalayersTransport>) -> Result<Self> {
        config.validate()?;
        if config.batch_size == Some(0) {
            return Err(ConnectorError::Dispatch(
                "datalayers batch_size must be >= 1".into(),
            ));
        }
        if config.buffer_capacity == Some(0) {
            return Err(ConnectorError::Dispatch(
                "datalayers buffer_capacity must be >= 1".into(),
            ));
        }
        let batch_size = config.effective_batch_size();
        Ok(Self {
            config,
            transport,
            queue: Mutex::new(BatchQueue::new(batch_size, Duration::from_millis(50))),
            backoff: Mutex::new(BackoffState::default()),
            sent: AtomicU64::new(0),
        })
    }

    pub fn config(&self) -> &DatalayersConfig {
        &self.config
    }

    pub fn sent_count(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }

    pub fn buffered_rows(&self) -> usize {
        self.queue.lock().len()
    }

    pub async fn flush(&self) -> Result<()> {
        self.backoff.lock().check()?;

        let (records, oldest) = {
            let mut q = self.queue.lock();
            if q.is_empty() {
                return Ok(());
            }
            q.take_batch()
        };

        if records.is_empty() {
            return Ok(());
        }

        let request = DatalayersWriteRequest {
            table: self.config.table.clone(),
            records,
        };

        match self.transport.write_batch(&request).await {
            Ok(_) => {
                self.backoff.lock().success();
                self.sent
                    .fetch_add(request.records.len() as u64, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.backoff.lock().failure();
                self.queue.lock().restore(request.records, oldest);
                Err(e)
            }
        }
    }
}

#[async_trait]
impl Sink for DatalayersSink {
    async fn send(&self, topic: &Topic, payload: &Bytes, _qos: QoS) -> Result<()> {
        // Outer backlog bound (publish path): fail closed instead of
        // growing without bound. Single lock acquisition per message:
        // bound check and push share one queue lock.
        let rec = extract_datalayers_record(payload, topic.as_str(), &self.config)?;
        let should_flush = {
            let mut q = self.queue.lock();
            if q.len() >= self.config.effective_buffer_capacity() {
                return Err(ConnectorError::Connection(
                    "datalayers buffer limit reached".into(),
                ));
            }
            q.push(rec)
        };

        if should_flush {
            self.flush().await?;
        }
        Ok(())
    }

    fn kind(&self) -> &'static str {
        "datalayers"
    }
}

/// Addressable registered connector for Datalayers.
pub struct DatalayersConnector {
    id: String,
    sink: Arc<DatalayersSink>,
}

impl DatalayersConnector {
    pub fn new(id: impl Into<String>, sink: Arc<DatalayersSink>) -> Self {
        Self {
            id: id.into(),
            sink,
        }
    }

    pub fn sink(&self) -> Arc<DatalayersSink> {
        self.sink.clone()
    }
}

impl Connector for DatalayersConnector {
    fn connector_id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> &'static str {
        "datalayers"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config() -> DatalayersConfig {
        DatalayersConfig {
            endpoint: "http://datalayers-node:8360".to_string(),
            database: "factory_db".to_string(),
            table: "machinery".to_string(),
            auth_token: Some("dl-secret-token-12345".to_string()),
            username: None,
            password: None,
            timestamp_field: Some("custom_ts".to_string()),
            tag_columns: vec!["line".to_string(), "machine".to_string()],
            field_columns: vec!["pressure".to_string(), "temperature".to_string()],
            precision: None,
            batch_size: Some(1),
            buffer_capacity: None,
            timeout_ms: None,
        }
    }

    fn basic_config() -> DatalayersConfig {
        DatalayersConfig {
            endpoint: "http://datalayers-node:8360".to_string(),
            database: "factory_db".to_string(),
            table: "machinery".to_string(),
            auth_token: None,
            username: Some("admin".to_string()),
            password: Some("public".to_string()),
            timestamp_field: Some("custom_ts".to_string()),
            tag_columns: vec!["line".to_string(), "machine".to_string()],
            field_columns: vec!["pressure".to_string(), "temperature".to_string()],
            precision: None,
            batch_size: Some(1),
            buffer_capacity: None,
            timeout_ms: None,
        }
    }

    #[test]
    fn sb27_measurement_escapes_newline_and_carriage_return() {
        assert_eq!(escape_measurement("a\nb\rc,d e"), "a\\nb\\rc\\,d\\ e");
    }

    #[test]
    fn test_config_validation_and_auth_header() {
        let cfg = sample_config();
        assert!(cfg.validate().is_ok());
        assert_eq!(
            cfg.auth_header_value(),
            Some("Bearer dl-secret-token-12345".to_string())
        );
    }

    #[test]
    fn test_basic_auth_header() {
        // Basic mode sends `Basic base64(user:pass)`.
        let cfg = basic_config();
        assert!(cfg.validate().is_ok());
        assert_eq!(
            cfg.auth_header_value(),
            Some("Basic YWRtaW46cHVibGlj".to_string())
        );
    }

    #[test]
    fn test_both_auth_modes_refused() {
        // Bearer plus Basic at once is a config error, not a silent pick.
        let mut cfg = sample_config();
        cfg.username = Some("admin".to_string());
        cfg.password = Some("public".to_string());
        let err = cfg.validate().expect_err("both modes must fail");
        assert!(err.to_string().contains("not both"));
        assert!(DatalayersSink::new(cfg, Arc::new(MockDatalayersTransport::new())).is_err());
    }

    #[test]
    fn test_half_basic_pair_refused() {
        // A username without a password (or the reverse) fails closed.
        let mut cfg = basic_config();
        cfg.password = None;
        assert!(cfg.validate().is_err());
        cfg = basic_config();
        cfg.username = None;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_password_absent_from_debug_and_errors() {
        // The secret never shows in Debug output or error text.
        let cfg = basic_config();
        let shown = format!("{:?}", cfg);
        assert!(shown.contains("admin"), "username stays visible: {shown}");
        assert!(
            !shown.contains("public"),
            "password leaks in Debug: {shown}"
        );
        assert!(shown.contains("[redacted]"));
        let both = DatalayersConfig {
            auth_token: Some("tok-9z9z-secret".to_string()),
            ..basic_config()
        };
        let err = both.validate().expect_err("both modes fail");
        let text = err.to_string();
        assert!(!text.contains("public"), "password leaks in error: {text}");
        assert!(
            !text.contains("tok-9z9z-secret"),
            "token leaks in error: {text}"
        );
        let header = cfg.auth_header_value().expect("basic header");
        let bad = DatalayersConfig {
            password: Some("wrong-pass-xyz".to_string()),
            ..basic_config()
        };
        let bad_shown = format!("{:?}", bad);
        assert!(!bad_shown.contains("wrong-pass-xyz"));
        let _ = header;
    }

    #[test]
    fn test_timestamp_microsecond_scaling() {
        // Seconds
        let sec_val = serde_json::json!({"ts": 1726000000});
        assert_eq!(
            extract_microsecond_timestamp(&sec_val, Some("ts")),
            1726000000000000
        );

        // Millis
        let ms_val = serde_json::json!({"ts": 1726000000123i64});
        assert_eq!(
            extract_microsecond_timestamp(&ms_val, Some("ts")),
            1726000000123000
        );

        // Micros
        let us_val = serde_json::json!({"ts": 1726000000123456i64});
        assert_eq!(
            extract_microsecond_timestamp(&us_val, Some("ts")),
            1726000000123456
        );

        // Float seconds with fractional micros.
        let float_sec = serde_json::json!({"ts": 1726000000.123456});
        assert_eq!(
            extract_microsecond_timestamp(&float_sec, Some("ts")),
            1726000000123456
        );

        // Missing, zero and negative timestamps fall back to wall-clock.
        let before = now_millis() * 1000;
        for val in [
            serde_json::json!({}),
            serde_json::json!({"ts": 0}),
            serde_json::json!({"ts": -5}),
            serde_json::json!({"ts": "not-a-number"}),
        ] {
            let ts = extract_microsecond_timestamp(&val, Some("ts"));
            assert!(ts >= before, "fallback must be wall-clock, got {ts}");
        }
    }

    #[test]
    fn test_config_bounds_and_urls() {
        let mut cfg = sample_config();
        assert_eq!(cfg.effective_batch_size(), 1);
        assert_eq!(
            cfg.write_url(),
            "http://datalayers-node:8360/write?db=factory_db&precision=us"
        );
        assert_eq!(
            cfg.sql_url(true),
            "http://datalayers-node:8360/api/v1/sql?db=factory_db"
        );
        assert_eq!(cfg.sql_url(false), "http://datalayers-node:8360/api/v1/sql");

        // Empty Bearer degrades to unauthenticated instead of sending
        // `Bearer ` with an empty token.
        cfg.auth_token = Some("   ".to_string());
        assert_eq!(cfg.auth_header_value(), None);

        // Zero depths are dispatch errors, not silent clamps.
        cfg.batch_size = Some(0);
        assert!(
            DatalayersSink::new(cfg.clone(), Arc::new(MockDatalayersTransport::new())).is_err()
        );
        cfg.batch_size = Some(1);
        cfg.buffer_capacity = Some(0);
        assert!(
            DatalayersSink::new(cfg.clone(), Arc::new(MockDatalayersTransport::new())).is_err()
        );
    }

    #[test]
    fn test_parse_count_result_shapes() {
        assert_eq!(parse_count_result(br#"{"count":1000}"#).unwrap(), 1000);
        assert_eq!(
            parse_count_result(br#"{"result":{"values":[[1000]]}}"#).unwrap(),
            1000
        );
        assert_eq!(
            parse_count_result(br#"{"result":{"data_array":[["1000"]]}}"#).unwrap(),
            1000
        );
        assert!(parse_count_result(br#"{"result":{}}"#).is_err());
        assert!(parse_count_result(b"nope").is_err());
    }

    #[test]
    fn test_record_extraction_with_tag_field_segregation() {
        let cfg = sample_config();
        let payload = br#"{
            "custom_ts": 1726000000,
            "line": "A1",
            "machine": "press_03",
            "pressure": 150.2,
            "temperature": 75.8,
            "unmapped_col": "ignore_me"
        }"#;

        let rec = extract_datalayers_record(payload, "factory/p1", &cfg).expect("valid extraction");
        assert_eq!(rec.time, 1726000000000000);
        assert_eq!(rec.tags.get("line").unwrap(), "A1");
        assert_eq!(rec.tags.get("machine").unwrap(), "press_03");
        assert_eq!(
            rec.fields.get("pressure").unwrap(),
            &serde_json::json!(150.2)
        );
        assert_eq!(
            rec.fields.get("temperature").unwrap(),
            &serde_json::json!(75.8)
        );
        assert!(!rec.fields.contains_key("unmapped_col"));
    }

    #[test]
    fn test_write_request_serialization() {
        let mut tags = HashMap::new();
        tags.insert("station".to_string(), "st-4".to_string());
        let mut fields = HashMap::new();
        fields.insert("rpm".to_string(), serde_json::json!(3400));

        let req = DatalayersWriteRequest {
            table: "motors".to_string(),
            records: vec![DatalayersRecord {
                time: 1726000000000000,
                tags,
                fields,
            }],
        };

        let json_str = serde_json::to_string(&req).expect("valid json");
        assert!(json_str.contains("\"table\":\"motors\""));
        assert!(json_str.contains("\"station\":\"st-4\""));
        assert!(json_str.contains("\"rpm\":3400"));
    }

    #[tokio::test]
    async fn test_datalayers_sink_loopback_success() {
        let cfg = sample_config();
        let transport = Arc::new(MockDatalayersTransport::new());
        let sink = DatalayersSink::new(cfg, transport.clone()).expect("valid sink");

        let topic = Topic::new("machinery/lineA").unwrap();
        let payload = Bytes::from_static(
            br#"{
            "line": "L2",
            "machine": "cnc_01",
            "pressure": 82.5,
            "temperature": 60.1
        }"#,
        );

        sink.send(&topic, &payload, QoS::AtLeastOnce)
            .await
            .expect("send succeeds");

        let reqs = transport.captured_requests.lock();
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0].table, "machinery");
        assert_eq!(reqs[0].records.len(), 1);
        assert_eq!(sink.sent_count(), 1);
    }

    #[tokio::test]
    async fn test_datalayers_sink_transient_retry_and_terminal_error() {
        let cfg = sample_config();
        // 1 transient failure then success
        let transport = Arc::new(MockDatalayersTransport::with_transient_failures(1));
        let sink = DatalayersSink::new(cfg, transport.clone()).expect("valid sink");

        let topic = Topic::new("machinery/lineA").unwrap();
        let payload = Bytes::from_static(br#"{"pressure": 10.0}"#);

        let res = sink.send(&topic, &payload, QoS::AtLeastOnce).await;
        assert!(res.is_err());

        // Reset backoff and retry flush
        *sink.backoff.lock() = BackoffState::default();
        sink.flush().await.expect("retry flush succeeds");
        assert_eq!(sink.sent_count(), 1);

        // Terminal error check
        let term_transport = Arc::new(MockDatalayersTransport::with_terminal_failure());
        let term_sink = DatalayersSink::new(sample_config(), term_transport).expect("valid sink");
        let term_res = term_sink.send(&topic, &payload, QoS::AtLeastOnce).await;
        assert!(term_res.is_err());
        assert!(matches!(
            term_res.err().unwrap(),
            ConnectorError::Dispatch(_)
        ));
    }

    #[tokio::test]
    async fn test_backoff_keeps_buffered_rows() {
        let mut cfg = sample_config();
        cfg.batch_size = Some(10);
        let transport = Arc::new(MockDatalayersTransport::with_transient_failures(1));
        let sink = DatalayersSink::new(cfg, transport.clone()).expect("valid sink");

        let topic = Topic::new("machinery/lineA").unwrap();
        let payload1 = Bytes::from_static(br#"{"pressure": 10.0}"#);
        let payload2 = Bytes::from_static(br#"{"pressure": 11.0}"#);

        sink.send(&topic, &payload1, QoS::AtLeastOnce)
            .await
            .expect("buffer first row");
        assert_eq!(sink.buffered_rows(), 1);

        // First flush fails transiently, restores the batch and enters backoff.
        let first = sink.flush().await;
        assert!(first.is_err());
        assert_eq!(sink.buffered_rows(), 1);

        // Still inside the backoff window: flush must fail without dropping rows.
        sink.send(&topic, &payload2, QoS::AtLeastOnce)
            .await
            .expect("buffer second row");
        let second = sink.flush().await;
        assert!(second.is_err());
        assert_eq!(sink.buffered_rows(), 2);

        // After the backoff resets, one flush delivers both rows exactly once.
        *sink.backoff.lock() = BackoffState::default();
        sink.flush().await.expect("retry flush succeeds");
        assert_eq!(sink.buffered_rows(), 0);
        assert_eq!(sink.sent_count(), 2);
        let reqs = transport.captured_requests.lock();
        assert_eq!(reqs.len(), 2);
        assert_eq!(reqs[1].records.len(), 2);
    }

    // ------------------------------------------------------------------
    // Loopback HTTP tests: the production `reqwest` transport against
    // an ephemeral local server (offline; no vendor server needed).
    // ------------------------------------------------------------------

    use axum::{extract::State, http::StatusCode, routing::post, Router};
    use std::sync::Mutex as StdMutex;
    use tokio::net::TcpListener;

    #[derive(Debug, Default)]
    struct FakeDatalayers {
        good_bearer: StdMutex<String>,
        good_basic: StdMutex<String>,
        auth_seen: StdMutex<Vec<String>>,
        query_seen: StdMutex<String>,
        bodies: StdMutex<Vec<String>>,
        status: StdMutex<u16>,
    }

    async fn serve_fake(state: Arc<FakeDatalayers>) -> (String, tokio::task::JoinHandle<()>) {
        async fn write_handler(
            State(state): State<Arc<FakeDatalayers>>,
            uri: axum::http::Uri,
            headers: axum::http::HeaderMap,
            body: String,
        ) -> (StatusCode, String) {
            state.auth_seen.lock().unwrap().push(
                headers
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_string(),
            );
            *state.query_seen.lock().unwrap() = uri.query().unwrap_or_default().to_string();
            state.bodies.lock().unwrap().push(body);
            let good_bearer = state.good_bearer.lock().unwrap().clone();
            let good_basic = state.good_basic.lock().unwrap().clone();
            // The fake verifies the credential it receives.
            // It accepts only the exact expected header value.
            let expected = if !good_bearer.is_empty() {
                good_bearer
            } else {
                good_basic
            };
            let got = state
                .auth_seen
                .lock()
                .unwrap()
                .last()
                .cloned()
                .unwrap_or_default();
            if !expected.is_empty() && got != expected {
                return (
                    StatusCode::UNAUTHORIZED,
                    r#"{"error":"unauthorized"}"#.to_string(),
                );
            }
            let status = *state.status.lock().unwrap();
            if status == 0 || status == 200 {
                (StatusCode::OK, r#"{"ok":true}"#.to_string())
            } else {
                (
                    StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                    format!(r#"{{"error":"fake-{status}"}}"#),
                )
            }
        }
        let app = Router::new()
            .route("/write", post(write_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        (format!("http://127.0.0.1:{port}"), server)
    }

    fn fake_config(endpoint: &str, token: Option<&str>) -> DatalayersConfig {
        DatalayersConfig {
            endpoint: endpoint.to_string(),
            database: "qual_db".to_string(),
            table: "machinery".to_string(),
            auth_token: token.map(str::to_string),
            username: None,
            password: None,
            timestamp_field: Some("custom_ts".to_string()),
            tag_columns: vec!["line".to_string()],
            field_columns: vec!["pressure".to_string()],
            precision: None,
            batch_size: Some(50),
            buffer_capacity: None,
            timeout_ms: Some(5_000),
        }
    }

    fn fake_basic_config(endpoint: &str, user: &str, pass: &str) -> DatalayersConfig {
        DatalayersConfig {
            endpoint: endpoint.to_string(),
            database: "qual_db".to_string(),
            table: "machinery".to_string(),
            auth_token: None,
            username: Some(user.to_string()),
            password: Some(pass.to_string()),
            timestamp_field: Some("custom_ts".to_string()),
            tag_columns: vec!["line".to_string()],
            field_columns: vec!["pressure".to_string()],
            precision: None,
            batch_size: Some(50),
            buffer_capacity: None,
            timeout_ms: Some(5_000),
        }
    }

    #[tokio::test]
    async fn sb27_table_escape_goes_through_manager() {
        // Broker path (publish, deliver): ConnectorManager::send ->
        // Sink::send -> HttpDatalayersTransport POST against the
        // loopback server. A table with newline and carriage return
        // arrives escaped on the wire.
        use broker_connectors::ConnectorManager;
        let fake = Arc::new(FakeDatalayers {
            status: StdMutex::new(200),
            ..Default::default()
        });
        let (endpoint, server) = serve_fake(fake.clone()).await;
        let mut config = fake_config(&endpoint, None);
        config.table = "a\nb\rc,d e".to_string();
        let transport = Arc::new(HttpDatalayersTransport::with_client(
            &config,
            reqwest::Client::new(),
        ));
        let sink = Arc::new(DatalayersSink::new(config, transport).expect("fake sink"));
        let manager = ConnectorManager::new();
        manager.register("dl-sb27", sink.clone());

        let topic = Topic::new("machinery/lineA").unwrap();
        manager
            .send(
                "dl-sb27",
                &topic,
                &Bytes::from_static(br#"{"custom_ts": 1726000000, "line": "A1", "pressure": 9.5}"#),
                QoS::AtLeastOnce,
            )
            .await
            .expect("broker send buffers");
        sink.flush().await.expect("loopback flush");

        assert_eq!(sink.sent_count(), 1);
        let bodies = fake.bodies.lock().unwrap();
        assert_eq!(bodies.len(), 1);
        let line = bodies[0].trim();
        assert!(
            line.starts_with("a\\nb\\rc\\,d\\ e,"),
            "escaped table on the wire: {line}"
        );
        server.abort();
    }

    #[tokio::test]
    async fn test_http_write_posts_bearer_and_json_through_manager() {
        // Broker path (publish, deliver): ConnectorManager::send ->
        // Sink::send -> HttpDatalayersTransport POST with Bearer auth
        // and microsecond timestamps against the loopback server.
        use broker_connectors::ConnectorManager;
        let fake = Arc::new(FakeDatalayers {
            good_bearer: StdMutex::new("Bearer tok-1".to_string()),
            status: StdMutex::new(200),
            ..Default::default()
        });
        let (endpoint, server) = serve_fake(fake.clone()).await;
        let config = fake_config(&endpoint, Some("tok-1"));
        let transport = Arc::new(HttpDatalayersTransport::with_client(
            &config,
            reqwest::Client::new(),
        ));
        assert!(transport.write_url_str().contains("db=qual_db"));
        assert!(transport.write_url_str().contains("precision=us"));
        let sink = Arc::new(DatalayersSink::new(config, transport).expect("fake sink"));
        assert_eq!(sink.kind(), "datalayers");
        let manager = ConnectorManager::new();
        manager.register("dl-qual", sink.clone());

        let topic = Topic::new("machinery/lineA").unwrap();
        manager
            .send(
                "dl-qual",
                &topic,
                &Bytes::from_static(br#"{"custom_ts": 1726000000, "line": "A1", "pressure": 9.5}"#),
                QoS::AtLeastOnce,
            )
            .await
            .expect("broker send buffers");
        sink.flush().await.expect("loopback flush");

        assert_eq!(sink.sent_count(), 1);
        assert_eq!(sink.buffered_rows(), 0);
        assert_eq!(
            fake.auth_seen.lock().unwrap().as_slice(),
            &["Bearer tok-1".to_string()]
        );
        assert!(
            fake.query_seen.lock().unwrap().contains("db=qual_db"),
            "write must select the database: {}",
            fake.query_seen.lock().unwrap()
        );
        assert!(
            fake.query_seen.lock().unwrap().contains("precision=us"),
            "write must use microsecond precision: {}",
            fake.query_seen.lock().unwrap()
        );
        let bodies = fake.bodies.lock().unwrap();
        assert_eq!(bodies.len(), 1);
        let line = bodies[0].trim();
        assert!(
            line.starts_with("machinery,"),
            "line must carry the table: {line}"
        );
        assert!(line.contains("line=A1"), "line must carry the tag: {line}");
        assert!(
            line.contains("pressure=9.5"),
            "line must carry the field: {line}"
        );
        assert!(
            line.ends_with(" 1726000000000000"),
            "line must carry microsecond time: {line}"
        );
        server.abort();
    }

    #[tokio::test]
    async fn test_http_write_posts_basic_and_json_through_manager() {
        // Broker path (publish, deliver): ConnectorManager::send ->
        // Sink::send -> HttpDatalayersTransport POST with Basic auth
        // and microsecond timestamps against the loopback server.
        use broker_connectors::ConnectorManager;
        let probe = fake_basic_config("http://127.0.0.1:9", "qa-user", "s3cret-basic");
        let expected = probe.auth_header_value().expect("basic header");
        assert!(expected.starts_with("Basic "));
        let fake = Arc::new(FakeDatalayers {
            good_basic: StdMutex::new(expected.clone()),
            status: StdMutex::new(200),
            ..Default::default()
        });
        let (endpoint, server) = serve_fake(fake.clone()).await;
        let config = fake_basic_config(&endpoint, "qa-user", "s3cret-basic");
        assert_eq!(config.auth_header_value(), Some(expected.clone()));
        let transport = Arc::new(HttpDatalayersTransport::with_client(
            &config,
            reqwest::Client::new(),
        ));
        assert!(transport.write_url_str().contains("precision=us"));
        let sink = Arc::new(DatalayersSink::new(config, transport).expect("fake sink"));
        assert_eq!(sink.kind(), "datalayers");
        let manager = ConnectorManager::new();
        manager.register("dl-basic", sink.clone());

        let topic = Topic::new("machinery/lineA").unwrap();
        manager
            .send(
                "dl-basic",
                &topic,
                &Bytes::from_static(br#"{"custom_ts": 1726000000, "line": "A1", "pressure": 9.5}"#),
                QoS::AtLeastOnce,
            )
            .await
            .expect("broker send buffers");
        sink.flush().await.expect("loopback flush");

        assert_eq!(sink.sent_count(), 1);
        assert_eq!(sink.buffered_rows(), 0);
        assert_eq!(fake.auth_seen.lock().unwrap().as_slice(), &[expected]);
        let bodies = fake.bodies.lock().unwrap();
        assert_eq!(bodies.len(), 1);
        let line = bodies[0].trim();
        assert!(
            line.starts_with("machinery,"),
            "line must carry the table: {line}"
        );
        assert!(line.contains("line=A1"), "line must carry the tag: {line}");
        assert!(
            line.contains("pressure=9.5"),
            "line must carry the field: {line}"
        );
        assert!(
            line.ends_with(" 1726000000000000"),
            "line must carry microsecond time: {line}"
        );
        server.abort();
    }

    #[tokio::test]
    async fn test_http_401_surfaces_dispatch_and_keeps_rows() {
        // Wrong Bearer fails closed and terminal: Dispatch (never
        // retried with the same token), rows kept in the buffer.
        let fake = Arc::new(FakeDatalayers {
            good_bearer: StdMutex::new("Bearer right".to_string()),
            status: StdMutex::new(200),
            ..Default::default()
        });
        let (endpoint, server) = serve_fake(fake.clone()).await;
        let mut config = fake_config(&endpoint, Some("wrong"));
        config.batch_size = Some(10);
        let transport = Arc::new(HttpDatalayersTransport::with_client(
            &config,
            reqwest::Client::new(),
        ));
        let sink = Arc::new(DatalayersSink::new(config, transport).expect("fake sink"));
        let topic = Topic::new("machinery/lineA").unwrap();
        sink.send(
            &topic,
            &Bytes::from_static(br#"{"pressure": 1.0}"#),
            QoS::AtLeastOnce,
        )
        .await
        .expect("buffer");
        let err = sink.flush().await.expect_err("401 must fail");
        assert!(
            matches!(err, ConnectorError::Dispatch(_)),
            "401 must be terminal, got {err:?}"
        );
        assert!(
            err.to_string().contains("unauthorized"),
            "auth failure must surface, got: {err}"
        );
        assert_eq!(sink.buffered_rows(), 1);
        assert_eq!(sink.sent_count(), 0);
        server.abort();
    }

    #[tokio::test]
    async fn test_http_401_basic_fails_closed_without_password_leak() {
        // Wrong Basic password fails closed as Dispatch.
        // The error text never carries the password.
        let fake = Arc::new(FakeDatalayers {
            good_basic: StdMutex::new("Basic YWRtaW46cmlnaHQ=".to_string()),
            status: StdMutex::new(200),
            ..Default::default()
        });
        let (endpoint, server) = serve_fake(fake.clone()).await;
        let mut config = fake_basic_config(&endpoint, "admin", "wrong-secret-xyz");
        config.batch_size = Some(10);
        let transport = Arc::new(HttpDatalayersTransport::with_client(
            &config,
            reqwest::Client::new(),
        ));
        let sink = Arc::new(DatalayersSink::new(config, transport).expect("fake sink"));
        let topic = Topic::new("machinery/lineA").unwrap();
        sink.send(
            &topic,
            &Bytes::from_static(br#"{"pressure": 1.0}"#),
            QoS::AtLeastOnce,
        )
        .await
        .expect("buffer");
        let err = sink.flush().await.expect_err("401 must fail");
        assert!(
            matches!(err, ConnectorError::Dispatch(_)),
            "401 must be terminal, got {err:?}"
        );
        let text = err.to_string();
        assert!(!text.contains("wrong-secret-xyz"), "password leaks: {text}");
        assert_eq!(sink.buffered_rows(), 1);
        assert_eq!(sink.sent_count(), 0);
        server.abort();
    }

    #[tokio::test]
    async fn test_http_500_is_connection_and_backs_off() {
        // 500 is transient: Connection, rows restored, backoff engaged
        // so the next flush fails fast without touching the server.
        let fake = Arc::new(FakeDatalayers {
            status: StdMutex::new(500),
            ..Default::default()
        });
        let (endpoint, server) = serve_fake(fake.clone()).await;
        let mut config = fake_config(&endpoint, None);
        config.batch_size = Some(10);
        let transport = Arc::new(HttpDatalayersTransport::with_client(
            &config,
            reqwest::Client::new(),
        ));
        let sink = Arc::new(DatalayersSink::new(config, transport).expect("fake sink"));
        let topic = Topic::new("machinery/lineA").unwrap();
        sink.send(
            &topic,
            &Bytes::from_static(br#"{"pressure": 1.0}"#),
            QoS::AtLeastOnce,
        )
        .await
        .expect("buffer");
        let err = sink.flush().await.expect_err("500 must fail");
        assert!(
            matches!(err, ConnectorError::Connection(_)),
            "500 must be retryable, got {err:?}"
        );
        assert_eq!(sink.buffered_rows(), 1);
        let calls = fake.bodies.lock().unwrap().len();
        assert!(sink.flush().await.is_err(), "backoff must fail fast");
        assert_eq!(
            fake.bodies.lock().unwrap().len(),
            calls,
            "backoff must not touch the server"
        );
        server.abort();
    }

    #[tokio::test]
    async fn test_http_unreachable_fails_closed_through_manager() {
        // Unreachable server through the broker path: connection
        // failure, never access granted.
        use broker_connectors::ConnectorManager;
        let mut config = fake_config("http://127.0.0.1:1", None);
        config.batch_size = Some(1);
        config.timeout_ms = Some(500);
        let transport = Arc::new(HttpDatalayersTransport::new(&config));
        let sink = Arc::new(DatalayersSink::new(config, transport).expect("sink"));
        let manager = ConnectorManager::new();
        manager.register("dl-dead", sink);
        let err = manager
            .send(
                "dl-dead",
                &Topic::new("machinery/lineA").unwrap(),
                &Bytes::from_static(br#"{"pressure": 1.0}"#),
                QoS::AtLeastOnce,
            )
            .await
            .expect_err("unreachable must fail");
        assert!(
            matches!(err, ConnectorError::Connection(_)),
            "unreachable must be a connection failure, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_buffer_bound_fails_closed() {
        let mut config = sample_config();
        config.batch_size = Some(10);
        config.buffer_capacity = Some(1);
        let transport = Arc::new(MockDatalayersTransport::new());
        let sink = DatalayersSink::new(config, transport).expect("valid sink");
        let topic = Topic::new("machinery/lineA").unwrap();
        sink.send(
            &topic,
            &Bytes::from_static(br#"{"pressure": 1.0}"#),
            QoS::AtLeastOnce,
        )
        .await
        .expect("first row fits");
        let err = sink
            .send(
                &topic,
                &Bytes::from_static(br#"{"pressure": 2.0}"#),
                QoS::AtLeastOnce,
            )
            .await
            .expect_err("full buffer must fail");
        assert!(matches!(err, ConnectorError::Connection(_)));
        assert_eq!(sink.buffered_rows(), 1);
    }

    fn qual_env(name: &str) -> Option<String> {
        std::env::var(name).ok().filter(|v| !v.trim().is_empty())
    }

    /// Qualification against a real server via the maintained
    /// `reqwest` write transport.
    ///
    /// Run with e.g.:
    /// `DATALAYERS_ENDPOINT=http://127.0.0.1:8361 DATALAYERS_DATABASE=qual_b313 \
    ///  DATALAYERS_TABLE=qual_points DATALAYERS_USERNAME=admin DATALAYERS_PASSWORD=public \
    ///  cargo test -p broker-connectors-enterprise --lib datalayers::tests::test_qualify_write_path -- --ignored --nocapture`
    ///
    /// Creates the database, streams 1000 points through the broker
    /// ([`broker_connectors::ConnectorManager`] -> [`DatalayersSink`] on
    /// [`HttpDatalayersTransport`], Basic auth, microsecond
    /// timestamps), asserts `SELECT COUNT(*)` query-back equality,
    /// proves a bad password fails closed as a dispatch error, then
    /// drops the table. Bearer mode stays covered by the loopback unit
    /// test `test_http_write_posts_bearer_and_json_through_manager`.
    #[tokio::test]
    #[ignore = "needs a real server (see DATALAYERS_* env)"]
    async fn test_qualify_write_path() {
        use broker_connectors::ConnectorManager;
        let endpoint = qual_env("DATALAYERS_ENDPOINT").unwrap_or_else(|| {
            panic!(
                "DATALAYERS_ENDPOINT must point at a real server for qualification; failing closed"
            )
        });
        let database = qual_env("DATALAYERS_DATABASE")
            .unwrap_or_else(|| format!("qual_b313_{}", now_millis()));
        let table = qual_env("DATALAYERS_TABLE").unwrap_or_else(|| "qual_points".to_string());
        let username = qual_env("DATALAYERS_USERNAME").unwrap_or_else(|| {
            panic!("DATALAYERS_USERNAME must be set for qualification; failing closed")
        });
        let password = qual_env("DATALAYERS_PASSWORD").unwrap_or_else(|| {
            panic!("DATALAYERS_PASSWORD must be set for qualification; failing closed")
        });

        let config = DatalayersConfig {
            endpoint: endpoint.clone(),
            database: database.clone(),
            table: table.clone(),
            auth_token: None,
            username: Some(username.clone()),
            password: Some(password.clone()),
            timestamp_field: Some("ts".to_string()),
            tag_columns: vec!["line".to_string()],
            field_columns: vec!["pressure".to_string(), "seq".to_string()],
            precision: None,
            batch_size: Some(100),
            buffer_capacity: None,
            timeout_ms: Some(30_000),
        };
        config.validate().expect("qual config validates");
        let transport = Arc::new(HttpDatalayersTransport::with_client(
            &config,
            reqwest::Client::new(),
        ));

        // Server identity for the report (best effort; the write API
        // itself reports no version, so query it and log whatever
        // answers).
        match transport.execute_sql_no_db("SHOW DATABASES").await {
            Ok(body) => eprintln!(
                "qual server: endpoint={endpoint} databases={}",
                String::from_utf8_lossy(&body)
                    .chars()
                    .take(200)
                    .collect::<String>()
            ),
            Err(e) => eprintln!("qual server info unreachable (tolerated): {e}"),
        }

        transport
            .execute_sql_no_db(&format!("CREATE DATABASE IF NOT EXISTS {database}"))
            .await
            .expect("qual create database");
        transport
            .execute_sql(&format!(
                "CREATE TABLE IF NOT EXISTS {table} (time TIMESTAMP NOT NULL, line STRING, pressure DOUBLE, seq BIGINT, timestamp key(time)) PARTITION BY HASH(line) PARTITIONS 2 ENGINE=TimeSeries"
            ))
            .await
            .expect("qual create table");

        let sink =
            Arc::new(DatalayersSink::new(config.clone(), transport.clone()).expect("qual sink"));
        assert_eq!(sink.kind(), "datalayers");
        // The broker's path: rule actions deliver through the shared
        // connector manager, so the qualification sends through it.
        let manager = Arc::new(ConnectorManager::new());
        manager.register("qual-dl", sink.clone());

        let topic = Topic::new("machinery/qual").unwrap();
        for seq in 0..1000u64 {
            let payload = Bytes::from(format!(
                r#"{{"ts": {}, "line": "L{:02}", "pressure": {:.2}, "seq": {seq}}}"#,
                1726000000000000i64 + seq as i64,
                seq % 8,
                80.0 + (seq as f64) * 0.01
            ));
            manager
                .send("qual-dl", &topic, &payload, QoS::AtLeastOnce)
                .await
                .expect("qual send");
        }
        sink.flush().await.expect("qual flush");
        assert_eq!(sink.sent_count(), 1000, "qual row count");
        eprintln!("qual rows sent: records=1000 table={table}");

        // Row count asserted back from the server, not the counters.
        let body = transport
            .execute_sql(&format!("SELECT COUNT(*) AS n FROM {table}"))
            .await
            .expect("qual count");
        let count = parse_count_result(&body).expect("qual count parses");
        assert_eq!(
            count,
            1000,
            "qual count: {}",
            String::from_utf8_lossy(&body)
        );
        eprintln!("qual rows asserted: count=1000 table={table}");

        // Bad password fails closed and terminal (rows kept).
        let mut bad_config = config.clone();
        bad_config.password = Some("qual-bad-password".to_string());
        let bad_transport = Arc::new(HttpDatalayersTransport::with_client(
            &bad_config,
            reqwest::Client::new(),
        ));
        let bad_sink =
            Arc::new(DatalayersSink::new(bad_config, bad_transport).expect("qual bad sink"));
        bad_sink
            .send(
                &topic,
                &Bytes::from_static(br#"{"line":"L0","pressure":1.0}"#),
                QoS::AtLeastOnce,
            )
            .await
            .expect("qual bad buffer");
        let err = bad_sink.flush().await.expect_err("bad password must fail");
        assert!(
            matches!(err, ConnectorError::Dispatch(_)),
            "bad password must be terminal, got {err:?}"
        );
        assert_eq!(bad_sink.buffered_rows(), 1);
        eprintln!("qual auth failure asserted: bad password is a dispatch error");

        // Cleanup: drop the table created for this run (best effort;
        // a failure is logged, not hidden).
        match transport
            .execute_sql(&format!("DROP TABLE IF EXISTS {table}"))
            .await
        {
            Ok(_) => eprintln!("qual cleanup: dropped table {table}"),
            Err(e) => eprintln!("qual cleanup FAILED to drop {table} (tolerated): {e}"),
        }
        eprintln!("qual done: rows=1000 table={table} cleaned table");
    }
}
