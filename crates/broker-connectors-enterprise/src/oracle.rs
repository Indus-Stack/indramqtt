//! Oracle Database SQL sink (INDRA-166).
//!
//! High-throughput enterprise relational sink for Oracle Database with
//! positional parameter binding (`:1`, `:2`, ...), atomic upserts using
//! `MERGE INTO ... USING DUAL`, and ORA deadlock/transient retry classification.

use async_trait::async_trait;
use base64::Engine;
use broker_protocol::{QoS, Topic};
use bytes::Bytes;
use parking_lot::Mutex;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use broker_connectors::{BackoffState, BatchQueue, Connector, ConnectorError, Result, Sink};

fn default_batch_size() -> Option<usize> {
    Some(500)
}

/// Configuration for the Oracle Database SQL sink.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OracleSinkConfig {
    /// Oracle REST Data Services (ORDS) endpoint or SQL connection string.
    pub url: String,
    /// Oracle schema name.
    pub schema: String,
    /// Target database table name.
    pub table: String,
    /// Oracle database username.
    pub username: String,
    /// Oracle database password.
    pub password: String,
    /// Optional custom `MERGE INTO` statement override.
    #[serde(default)]
    pub custom_upsert: Option<String>,
    /// Column names serving as primary/match keys.
    #[serde(default)]
    pub key_columns: Vec<String>,
    /// Batch flush size (unbounded scale, default 500).
    #[serde(default = "default_batch_size")]
    pub batch_size: Option<usize>,
    /// In-memory queue buffer capacity (`None` = unbounded).
    #[serde(default)]
    pub buffer_capacity: Option<usize>,
    /// HTTP request / connect timeout in ms (default 5000).
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

impl OracleSinkConfig {
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms.unwrap_or(5000).max(1))
    }

    pub fn validate(&self) -> Result<()> {
        if self.url.trim().is_empty() {
            return Err(ConnectorError::Dispatch(
                "oracle url cannot be empty".into(),
            ));
        }
        if self.schema.trim().is_empty() {
            return Err(ConnectorError::Dispatch(
                "oracle schema cannot be empty".into(),
            ));
        }
        if self.table.trim().is_empty() {
            return Err(ConnectorError::Dispatch(
                "oracle table cannot be empty".into(),
            ));
        }
        if self.username.trim().is_empty() {
            return Err(ConnectorError::Dispatch(
                "oracle username cannot be empty".into(),
            ));
        }
        if self.custom_upsert.is_none() && self.key_columns.is_empty() {
            return Err(ConnectorError::Dispatch(
                "oracle sink requires key_columns when custom_upsert is not provided".into(),
            ));
        }
        Ok(())
    }

    pub fn basic_auth_header(&self) -> String {
        let creds = format!("{}:{}", self.username, self.password);
        let encoded = base64::engine::general_purpose::STANDARD.encode(creds.as_bytes());
        format!("Basic {encoded}")
    }
}

/// Typed Oracle bind parameter values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OracleValue {
    Null,
    String(String),
    Number(f64),
    Integer(i64),
    Boolean(bool),
}

impl OracleValue {
    pub fn data_type_hint(&self) -> &'static str {
        match self {
            Self::Null => "VARCHAR2",
            Self::String(_) => "VARCHAR2",
            Self::Number(_) | Self::Integer(_) => "NUMBER",
            Self::Boolean(_) => "NUMBER",
        }
    }
}

/// A positional Oracle bind parameter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OracleBindParam {
    pub name: String,
    pub value: OracleValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_type: Option<String>,
}

/// Result returned from Oracle SQL execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OracleResponse {
    pub rows_affected: usize,
    pub status: String,
    #[serde(default)]
    pub error_code: Option<String>,
    #[serde(default)]
    pub error_message: Option<String>,
}

/// ORA error classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OraErrorClassification {
    /// Retryable error (e.g. deadlock, EOF, shutdown in progress).
    Retryable,
    /// Terminal error (e.g. missing table, invalid login credentials, syntax error).
    Terminal,
    /// Unknown or generic error.
    Unknown,
}

/// Typed Oracle error kind for the MV-13b contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OraErrorKind {
    /// Lost session or listener fault. Retry with backoff.
    Connection,
    /// Bad user, password, lock or expiry. Never retry.
    Authentication,
    /// Row fails a rule (unique, null, check, key). Never retry.
    Constraint,
    /// Wrong name or text (identifier, table, command). Never retry.
    Syntax,
    /// Work timed out (connect, cancel, inbound). Retry with backoff.
    Timeout,
    /// Driver fault with no ORA code (I/O, socket). Retry with backoff.
    Transport,
    /// Code is absent or not in the table. Never retry.
    Unknown,
}

/// True when the kind retries (connection, timeout, transport).
/// Others fail closed as terminal errors.
pub fn ora_error_is_retryable(kind: OraErrorKind) -> bool {
    matches!(
        kind,
        OraErrorKind::Connection | OraErrorKind::Timeout | OraErrorKind::Transport
    )
}

/// Map one numeric ORA code to its kind.
/// Codes come from the database message, not from text search.
pub fn classify_ora_code(code: u32) -> OraErrorKind {
    match code {
        // ORA-00000 carries no detail and closes the session.
        // Retry it; the probe reports the real cause.
        0 => OraErrorKind::Connection,
        // Deadlock: the batch conflicts and a retry wins.
        60 => OraErrorKind::Connection,
        // Unique constraint fails the row itself.
        1 => OraErrorKind::Constraint,
        // Null or precision fails the row itself.
        1400 | 1407 | 1438 | 12899 => OraErrorKind::Constraint,
        // Check or key fails the row itself.
        2290..=2292 => OraErrorKind::Constraint,
        // Bad login or locked account fails the credential.
        1017 | 1005 | 28000 | 28001 | 28009 => OraErrorKind::Authentication,
        // Startup or shutdown is transient and a retry wins.
        1033 | 1089 => OraErrorKind::Connection,
        // Cancel or limit hit the time budget, so retry.
        1013 | 2049 | 3136 | 12170 => OraErrorKind::Timeout,
        // Bad name or bad text fails the statement.
        904 | 911 | 933 | 923 | 942 => OraErrorKind::Syntax,
        // Lost session or listener fault; redial wins.
        3113 | 3114 | 3135 | 12154 | 12505 | 12514 | 12541 | 12545 => OraErrorKind::Connection,
        _ => OraErrorKind::Unknown,
    }
}

/// Read the first `ORA-<digits>` code from text.
/// Return `None` when no digits follow the tag.
pub fn parse_ora_code(text: &str) -> Option<u32> {
    let upper = text.to_ascii_uppercase();
    let mut rest = upper.as_str();
    while let Some(pos) = rest.find("ORA-") {
        let after = &rest[pos + 4..];
        let digits: String = after.chars().take_while(|c| c.is_ascii_digit()).collect();
        if !digits.is_empty() {
            // Codes fit in five digits; longer text keeps its prefix.
            let trimmed = if digits.len() > 5 {
                &digits[..5]
            } else {
                &digits
            };
            if let Ok(code) = trimmed.parse::<u32>() {
                return Some(code);
            }
        }
        rest = after;
    }
    None
}

/// Classify a driver error by its typed variant and code.
/// This path never searches message text.
pub fn classify_oracle_driver_error(error: &oracle_rs::Error) -> OraErrorKind {
    match error {
        oracle_rs::Error::AuthenticationFailed(_)
        | oracle_rs::Error::InvalidCredentials
        | oracle_rs::Error::UnsupportedVerifierType(_) => OraErrorKind::Authentication,
        oracle_rs::Error::ConnectionTimeout(_) => OraErrorKind::Timeout,
        oracle_rs::Error::ConnectionRefused { .. }
        | oracle_rs::Error::ConnectionRedirected { .. }
        | oracle_rs::Error::ConnectionRedirect(_)
        | oracle_rs::Error::ConnectionClosed
        | oracle_rs::Error::ConnectionClosedByServer(_)
        | oracle_rs::Error::ConnectionNotReady
        | oracle_rs::Error::CursorClosed
        | oracle_rs::Error::InvalidCursor(_)
        | oracle_rs::Error::Io(_) => OraErrorKind::Transport,
        oracle_rs::Error::InvalidServiceName { .. } | oracle_rs::Error::InvalidSid { .. } => {
            OraErrorKind::Connection
        }
        oracle_rs::Error::OracleError { code, .. } | oracle_rs::Error::ServerError { code, .. } => {
            classify_ora_code(*code)
        }
        _ => OraErrorKind::Unknown,
    }
}

/// Classify an Oracle error code (e.g. "ORA-00060").
pub fn classify_ora_error(error_code_or_msg: &str) -> OraErrorClassification {
    match parse_ora_code(error_code_or_msg).map(classify_ora_code) {
        Some(kind) if ora_error_is_retryable(kind) => OraErrorClassification::Retryable,
        Some(_) => OraErrorClassification::Terminal,
        None => OraErrorClassification::Unknown,
    }
}

/// Build an atomic Oracle `MERGE INTO` SQL statement.
pub fn build_merge_sql(
    table: &str,
    key_columns: &[String],
    all_columns: &[String],
) -> Result<String> {
    if key_columns.is_empty() {
        return Err(ConnectorError::Dispatch(
            "cannot build MERGE INTO without key_columns".into(),
        ));
    }
    if all_columns.is_empty() {
        return Err(ConnectorError::Dispatch(
            "cannot build MERGE INTO without all_columns".into(),
        ));
    }

    // USING (SELECT :1 AS c1, :2 AS c2 FROM DUAL) s
    let dual_selects: Vec<String> = all_columns
        .iter()
        .enumerate()
        .map(|(idx, col)| format!(":{} AS {}", idx + 1, col))
        .collect();

    // ON (t.k1 = s.k1 AND t.k2 = s.k2)
    let on_clauses: Vec<String> = key_columns
        .iter()
        .map(|k| format!("t.{} = s.{}", k, k))
        .collect();

    // Non-key columns for UPDATE SET
    let update_sets: Vec<String> = all_columns
        .iter()
        .filter(|c| !key_columns.contains(c))
        .map(|c| format!("t.{} = s.{}", c, c))
        .collect();

    // INSERT (col1, col2) VALUES (s.col1, s.col2)
    let insert_cols: Vec<String> = all_columns.to_vec();
    let insert_vals: Vec<String> = all_columns.iter().map(|c| format!("s.{}", c)).collect();

    let mut sql = format!(
        "MERGE INTO {} t\nUSING (SELECT {} FROM DUAL) s\nON ({})\n",
        table,
        dual_selects.join(", "),
        on_clauses.join(" AND ")
    );

    if !update_sets.is_empty() {
        sql.push_str(&format!(
            "WHEN MATCHED THEN UPDATE SET {}\n",
            update_sets.join(", ")
        ));
    }
    sql.push_str(&format!(
        "WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
        insert_cols.join(", "),
        insert_vals.join(", ")
    ));

    Ok(sql)
}

/// Extracted row for Oracle sink ingestion.
#[derive(Debug, Clone)]
pub struct OracleRow {
    pub topic: String,
    pub columns: Vec<(String, OracleValue)>,
}

/// Convert arbitrary JSON value to `OracleValue`.
pub fn json_to_oracle_value(val: &serde_json::Value) -> OracleValue {
    match val {
        serde_json::Value::Null => OracleValue::Null,
        serde_json::Value::Bool(b) => OracleValue::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                OracleValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                OracleValue::Number(f)
            } else {
                OracleValue::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => OracleValue::String(s.clone()),
        other => OracleValue::String(other.to_string()),
    }
}

/// Extract columns and values from JSON payload for Oracle insertion.
pub fn extract_oracle_row(payload: &[u8], topic: &str) -> Result<OracleRow> {
    let json_val: serde_json::Value = serde_json::from_slice(payload)
        .map_err(|e| ConnectorError::Dispatch(format!("invalid JSON payload: {e}")))?;

    let mut columns = Vec::new();
    if let serde_json::Value::Object(map) = json_val {
        for (k, v) in map {
            columns.push((k, json_to_oracle_value(&v)));
        }
    } else {
        columns.push(("payload".to_string(), json_to_oracle_value(&json_val)));
    }

    Ok(OracleRow {
        topic: topic.to_string(),
        columns,
    })
}

/// Transport abstraction for executing Oracle SQL.
#[async_trait]
pub trait OracleTransport: Send + Sync {
    async fn execute(&self, statement: &str, binds: &[OracleBindParam]) -> Result<OracleResponse>;
    /// Run one statement for many rows.
    /// Send one parse for the batch.
    /// The default loops over rows.
    async fn execute_many(
        &self,
        statement: &str,
        all_binds: &[Vec<OracleBindParam>],
    ) -> Result<OracleResponse> {
        let mut affected = 0usize;
        for binds in all_binds {
            let out = self.execute(statement, binds).await?;
            affected += out.rows_affected;
        }
        Ok(OracleResponse {
            rows_affected: affected,
            status: "SUCCESS".into(),
            error_code: None,
            error_message: None,
        })
    }
}

/// Production HTTP transport connecting to Oracle REST Data Services (ORDS).
pub struct HttpOracleTransport {
    client: reqwest::Client,
    url: String,
    auth_header: String,
}

impl HttpOracleTransport {
    pub fn new(config: &OracleSinkConfig) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(config.timeout())
                .build()
                .unwrap_or_default(),
            url: config.url.clone(),
            auth_header: config.basic_auth_header(),
        }
    }
}

#[async_trait]
impl OracleTransport for HttpOracleTransport {
    async fn execute(&self, statement: &str, binds: &[OracleBindParam]) -> Result<OracleResponse> {
        let body = serde_json::json!({
            "statementText": statement,
            "binds": binds
        });

        let resp = self
            .client
            .post(&self.url)
            .header(AUTHORIZATION, &self.auth_header)
            .header(CONTENT_TYPE, "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| ConnectorError::Connection(format!("oracle http request failed: {e}")))?;

        let status = resp.status();
        let body_text = resp
            .text()
            .await
            .unwrap_or_else(|_| "no response body".into());

        if status.is_success() {
            Ok(OracleResponse {
                rows_affected: 1,
                status: "SUCCESS".into(),
                error_code: None,
                error_message: None,
            })
        } else {
            let classification = classify_ora_error(&body_text);
            match classification {
                OraErrorClassification::Retryable => Err(ConnectorError::Connection(format!(
                    "transient oracle error: {body_text}"
                ))),
                _ => Err(ConnectorError::Dispatch(format!(
                    "terminal oracle error: {body_text}"
                ))),
            }
        }
    }
}

/// Map a sink bind value onto the driver value.
/// Booleans become 1/0 numbers: the server type is NUMBER.
fn oracle_value_to_driver(value: &OracleValue) -> oracle_rs::Value {
    match value {
        OracleValue::Null => oracle_rs::Value::Null,
        OracleValue::String(text) => oracle_rs::Value::String(text.clone()),
        OracleValue::Integer(num) => oracle_rs::Value::Integer(*num),
        OracleValue::Number(num) => {
            oracle_rs::Value::Number(oracle_rs::types::OracleNumber::new(num.to_string()))
        }
        OracleValue::Boolean(flag) => oracle_rs::Value::Integer(i64::from(*flag)),
    }
}

/// Map a driver error onto the retry contract.
/// Retry kinds become `Connection`; the sink retries them.
/// Other kinds become `Dispatch`; the sink stops.
/// The match uses the typed variant and code only.
fn map_oracle_driver_error(error: oracle_rs::Error) -> ConnectorError {
    let kind = classify_oracle_driver_error(&error);
    if ora_error_is_retryable(kind) {
        // Keep the server text in the message.
        if matches!(kind, OraErrorKind::Transport) {
            return ConnectorError::Connection(format!("oracle driver: {error}"));
        }
        return ConnectorError::Connection(format!("oracle transient: {error}"));
    }
    ConnectorError::Dispatch(format!("oracle refused: {error}"))
}

/// Production transport on the maintained driver: real TNS with
/// native auth and positional binds, negotiated by the driver.
///
/// Bound: one cached connection plus the sink buffer in front of
/// it (batch rows default 500); no pool, no background queue.
/// The default is finite because an unbounded pool under fan-in
/// would exhaust file descriptors. The sink send path runs
/// behind the rule engine bounded queue, so no numbers are needed.
///
/// TODO(parity): which entry selects this transport versus ORDS —
/// should the management API pick by URL scheme?
pub struct DriverOracleTransport {
    host: String,
    port: u16,
    service: String,
    username: String,
    password: String,
    timeout: Duration,
    connection: tokio::sync::Mutex<Option<oracle_rs::Connection>>,
}

impl DriverOracleTransport {
    /// Create a direct database transport.
    /// Fails closed on empty host, service, username or zero port.
    pub fn new(
        host: &str,
        port: u16,
        service: &str,
        username: &str,
        password: &str,
        timeout: Duration,
    ) -> Result<Self> {
        if host.trim().is_empty() {
            return Err(ConnectorError::Dispatch(
                "oracle host cannot be empty".into(),
            ));
        }
        if port == 0 {
            return Err(ConnectorError::Dispatch(
                "oracle port must be 1..=65535".into(),
            ));
        }
        if service.trim().is_empty() {
            return Err(ConnectorError::Dispatch(
                "oracle service cannot be empty".into(),
            ));
        }
        if username.trim().is_empty() {
            return Err(ConnectorError::Dispatch(
                "oracle username cannot be empty".into(),
            ));
        }
        Ok(Self {
            host: host.to_string(),
            port,
            service: service.to_string(),
            username: username.to_string(),
            password: password.to_string(),
            timeout,
            connection: tokio::sync::Mutex::new(None),
        })
    }

    /// Open one TNS session for the stored credentials.
    async fn connect_new(&self) -> Result<oracle_rs::Connection> {
        let config = oracle_rs::Config::new(
            self.host.clone(),
            self.port,
            self.service.clone(),
            self.username.clone(),
            self.password.clone(),
        );
        tokio::time::timeout(
            self.timeout,
            oracle_rs::Connection::connect_with_config(config),
        )
        .await
        .map_err(|_| {
            ConnectorError::Connection(format!(
                "oracle connect timeout: {}:{}",
                self.host, self.port
            ))
        })?
        .map_err(map_oracle_driver_error)
    }

    /// Probe a failed batch for a terminal table error.
    /// Run at most two statements on the failure path only.
    /// Return `Dispatch` with the server text when the table is missing.
    /// Return `None` when the failure stays retryable.
    async fn probe_batch_failure(
        &self,
        statement: &str,
        _first_binds: Option<&Vec<OracleBindParam>>,
    ) -> Option<ConnectorError> {
        // Extract the table name. Return None when unsafe.
        let table = merge_table_name(statement)?;
        // Open a fresh session. Return None when unreachable.
        let fresh = self.connect_new().await.ok()?;
        // Check the catalog. This query never touches the missing table.
        let upper = table.to_ascii_uppercase();
        let params = [oracle_rs::Value::String(upper)];
        let catalog = tokio::time::timeout(
            self.timeout,
            fresh.query(
                "SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = :1",
                &params,
            ),
        )
        .await
        .ok()?
        .ok()?;
        let count = catalog.rows.first()?.get(0).and_then(|value| {
            if let Some(num) = value.as_i64() {
                return Some(num);
            }
            if let Some(num) = value.as_f64() {
                return Some(num as i64);
            }
            value.as_str()?.trim().parse::<i64>().ok()
        })?;
        // Return None when the table exists. The failure stays retryable.
        if count != 0 {
            return None;
        }
        // The table is missing. Read it to obtain the server text.
        // The read fails because the table is missing. It changes nothing.
        // Free closes the connection without details on some paths.
        // The catalog count above proves the cause.
        let select_sql = format!("SELECT 1 FROM {table} WHERE 1 = 0");
        let select_error = tokio::time::timeout(self.timeout, fresh.query(&select_sql, &[]))
            .await
            .ok()?
            .err()?;
        let kind = classify_oracle_driver_error(&select_error);
        let select_text = select_error.to_string();
        let mapped = map_oracle_driver_error(select_error);
        if let ConnectorError::Dispatch(message) = &mapped {
            if matches!(kind, OraErrorKind::Syntax) && parse_ora_code(message) == Some(942) {
                return Some(mapped);
            }
        }
        // Report the standard code with the server evidence.
        // The catalog proves the miss. The text keeps the server reply.
        Some(ConnectorError::Dispatch(format!(
            "oracle refused: ORA-00942: table or view does not exist ({table}); probe: USER_TABLES count=0; server said: {select_text}"
        )))
    }
}

/// Extract the table from a `MERGE INTO` statement.
/// Return `None` for other statements or unsafe names.
/// Accept only `[A-Za-z0-9_]+`.
/// The probe interpolates the name, so it must stay safe.
fn merge_table_name(statement: &str) -> Option<String> {
    let rest = statement.strip_prefix("MERGE INTO ")?;
    let end = rest.find([' ', '\n', '\t'])?;
    let table = &rest[..end];
    if table.is_empty() {
        return None;
    }
    if !table.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return None;
    }
    Some(table.to_string())
}

#[async_trait]
impl OracleTransport for DriverOracleTransport {
    /// Run one statement for many rows in one batch.
    /// Send one parse for the batch.
    /// Commit once after the batch.
    async fn execute_many(
        &self,
        statement: &str,
        all_binds: &[Vec<OracleBindParam>],
    ) -> Result<OracleResponse> {
        if all_binds.is_empty() {
            return Ok(OracleResponse {
                rows_affected: 0,
                status: "SUCCESS".into(),
                error_code: None,
                error_message: None,
            });
        }
        let mut guard = self.connection.lock().await;
        if guard.is_none() {
            match self.connect_new().await {
                Ok(connection) => *guard = Some(connection),
                Err(error) => return Err(error),
            }
        }
        let connection = guard
            .as_ref()
            .ok_or_else(|| ConnectorError::Connection("oracle not connected".to_string()))?;
        let mut builder = oracle_rs::BatchBuilder::new(statement);
        for binds in all_binds {
            let params: Vec<oracle_rs::Value> = binds
                .iter()
                .map(|bind| oracle_value_to_driver(&bind.value))
                .collect();
            builder = builder.add_row(params);
        }
        let batch = builder.build();
        let outcome = tokio::time::timeout(self.timeout, connection.execute_batch(&batch))
            .await
            .map_err(|_| ConnectorError::Connection("oracle batch timeout".to_string()));
        let result = match outcome {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => {
                let mapped = map_oracle_driver_error(error);
                if matches!(mapped, ConnectorError::Connection(_)) {
                    *guard = None;
                    let statement_owned = statement.to_string();
                    let first = all_binds.first().cloned();
                    drop(guard);
                    if let Some(probed) = self
                        .probe_batch_failure(&statement_owned, first.as_ref())
                        .await
                    {
                        return Err(probed);
                    }
                    return Err(mapped);
                }
                return Err(mapped);
            }
            Err(error) => {
                *guard = None;
                let statement_owned = statement.to_string();
                let first = all_binds.first().cloned();
                drop(guard);
                if let Some(probed) = self
                    .probe_batch_failure(&statement_owned, first.as_ref())
                    .await
                {
                    return Err(probed);
                }
                return Err(error);
            }
        };
        if let Err(error) = tokio::time::timeout(self.timeout, connection.commit())
            .await
            .map_err(|_| ConnectorError::Connection("oracle commit timeout".to_string()))?
            .map_err(map_oracle_driver_error)
        {
            if matches!(error, ConnectorError::Connection(_)) {
                *guard = None;
            }
            return Err(error);
        }
        Ok(OracleResponse {
            rows_affected: result.total_rows_affected as usize,
            status: "SUCCESS".into(),
            error_code: None,
            error_message: None,
        })
    }

    async fn execute(&self, statement: &str, binds: &[OracleBindParam]) -> Result<OracleResponse> {
        let mut guard = self.connection.lock().await;
        if guard.is_none() {
            match self.connect_new().await {
                Ok(connection) => *guard = Some(connection),
                Err(error) => return Err(error),
            }
        }
        let connection = guard
            .as_ref()
            .ok_or_else(|| ConnectorError::Connection("oracle not connected".to_string()))?;
        let params: Vec<oracle_rs::Value> = binds
            .iter()
            .map(|bind| oracle_value_to_driver(&bind.value))
            .collect();
        let outcome = tokio::time::timeout(self.timeout, connection.execute(statement, &params))
            .await
            .map_err(|_| ConnectorError::Connection("oracle execute timeout".to_string()));
        let result = match outcome {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => {
                let mapped = map_oracle_driver_error(error);
                if matches!(mapped, ConnectorError::Connection(_)) {
                    *guard = None;
                }
                return Err(mapped);
            }
            Err(error) => {
                *guard = None;
                return Err(error);
            }
        };
        if let Err(error) = tokio::time::timeout(self.timeout, connection.commit())
            .await
            .map_err(|_| ConnectorError::Connection("oracle commit timeout".to_string()))?
            .map_err(map_oracle_driver_error)
        {
            if matches!(error, ConnectorError::Connection(_)) {
                *guard = None;
            }
            return Err(error);
        }
        Ok(OracleResponse {
            rows_affected: result.rows_affected as usize,
            status: "SUCCESS".into(),
            error_code: None,
            error_message: None,
        })
    }
}

/// Captured execution for testing and mock transports.
#[derive(Debug, Clone)]
pub struct CapturedOracleExecution {
    pub statement: String,
    pub binds: Vec<OracleBindParam>,
}

/// Mock transport for unit testing and offline verification.
pub struct MockOracleTransport {
    pub executions: Mutex<Vec<CapturedOracleExecution>>,
    pub fail_count: Mutex<usize>,
    pub outcome_code: Mutex<Option<String>>,
}

impl MockOracleTransport {
    pub fn new() -> Self {
        Self {
            executions: Mutex::new(Vec::new()),
            fail_count: Mutex::new(0),
            outcome_code: Mutex::new(None),
        }
    }

    pub fn with_transient_failures(failures: usize, ora_code: &str) -> Self {
        Self {
            executions: Mutex::new(Vec::new()),
            fail_count: Mutex::new(failures),
            outcome_code: Mutex::new(Some(ora_code.to_string())),
        }
    }
}

impl Default for MockOracleTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl OracleTransport for MockOracleTransport {
    async fn execute(&self, statement: &str, binds: &[OracleBindParam]) -> Result<OracleResponse> {
        self.executions.lock().push(CapturedOracleExecution {
            statement: statement.to_string(),
            binds: binds.to_vec(),
        });

        let mut fails = self.fail_count.lock();
        if *fails > 0 {
            *fails -= 1;
            let code = self
                .outcome_code
                .lock()
                .clone()
                .unwrap_or_else(|| "ORA-00060".into());
            let class = classify_ora_error(&code);
            if class == OraErrorClassification::Retryable {
                return Err(ConnectorError::Connection(format!(
                    "mock transient error: {code}"
                )));
            } else {
                return Err(ConnectorError::Dispatch(format!(
                    "mock terminal error: {code}"
                )));
            }
        }

        Ok(OracleResponse {
            rows_affected: 1,
            status: "SUCCESS".into(),
            error_code: None,
            error_message: None,
        })
    }
}

/// Oracle Database SQL Sink.
pub struct OracleSink {
    config: OracleSinkConfig,
    transport: Arc<dyn OracleTransport>,
    queue: Mutex<BatchQueue<OracleRow>>,
    backoff: Mutex<BackoffState>,
    sent: AtomicU64,
}

impl OracleSink {
    pub fn new(config: OracleSinkConfig, transport: Arc<dyn OracleTransport>) -> Result<Self> {
        config.validate()?;
        let batch_size = config.batch_size.unwrap_or(500).max(1);
        Ok(Self {
            config,
            transport,
            queue: Mutex::new(BatchQueue::new(batch_size, Duration::from_millis(50))),
            backoff: Mutex::new(BackoffState::default()),
            sent: AtomicU64::new(0),
        })
    }

    pub fn config(&self) -> &OracleSinkConfig {
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

        let (rows, oldest) = {
            let mut q = self.queue.lock();
            if q.is_empty() {
                return Ok(());
            }
            q.take_batch()
        };

        if rows.is_empty() {
            return Ok(());
        }

        // Build one statement per row first.
        // Rows with one shape share one parse.
        let mut planned: Vec<(String, Vec<OracleBindParam>)> = Vec::with_capacity(rows.len());
        for row in &rows {
            let (statement, binds) = if let Some(ref custom) = self.config.custom_upsert {
                let binds: Vec<OracleBindParam> = row
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(idx, (_, val))| OracleBindParam {
                        name: (idx + 1).to_string(),
                        value: val.clone(),
                        data_type: Some(val.data_type_hint().to_string()),
                    })
                    .collect();
                (custom.clone(), binds)
            } else {
                let all_cols: Vec<String> = row.columns.iter().map(|(c, _)| c.clone()).collect();
                let stmt =
                    build_merge_sql(&self.config.table, &self.config.key_columns, &all_cols)?;
                let binds: Vec<OracleBindParam> = row
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(idx, (_, val))| OracleBindParam {
                        name: (idx + 1).to_string(),
                        value: val.clone(),
                        data_type: Some(val.data_type_hint().to_string()),
                    })
                    .collect();
                (stmt, binds)
            };
            planned.push((statement, binds));
        }
        // Send rows with one shape in one batch.
        // One parse serves the batch.
        // Mixed shapes fall back to one call per row.
        let same_shape = planned
            .iter()
            .all(|(statement, _)| statement == &planned[0].0);
        if same_shape {
            let statement = planned[0].0.clone();
            let all_binds: Vec<Vec<OracleBindParam>> =
                planned.into_iter().map(|(_, binds)| binds).collect();
            let count = all_binds.len() as u64;
            match self.transport.execute_many(&statement, &all_binds).await {
                Ok(_) => {
                    self.backoff.lock().success();
                    self.sent.fetch_add(count, Ordering::Relaxed);
                }
                Err(e) => {
                    self.backoff.lock().failure();
                    self.queue.lock().restore(rows, oldest);
                    return Err(e);
                }
            }
            return Ok(());
        }
        for (statement, binds) in &planned {
            match self.transport.execute(statement, binds).await {
                Ok(_) => {
                    self.backoff.lock().success();
                    self.sent.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    self.backoff.lock().failure();
                    self.queue.lock().restore(rows, oldest);
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Sink for OracleSink {
    async fn send(&self, topic: &Topic, payload: &Bytes, _qos: QoS) -> Result<()> {
        let row = extract_oracle_row(payload, topic.as_str())?;
        let should_flush = {
            let mut q = self.queue.lock();
            q.push(row)
        };

        if should_flush {
            // While backing off, keep rows buffered instead of failing the
            // send: an explicit flush reports the backoff without dropping.
            if self.backoff.lock().check().is_err() {
                return Ok(());
            }
            if let Err(e) = self.flush().await {
                if let ConnectorError::Connection(msg) = &e {
                    if msg == "sink backing off after errors" {
                        return Ok(());
                    }
                }
                return Err(e);
            }
        }
        Ok(())
    }

    fn kind(&self) -> &'static str {
        "oracle"
    }
}

/// Addressable registered connector for Oracle Database.
pub struct OracleConnector {
    id: String,
    sink: Arc<OracleSink>,
}

impl OracleConnector {
    pub fn new(id: impl Into<String>, sink: Arc<OracleSink>) -> Self {
        Self {
            id: id.into(),
            sink,
        }
    }

    pub fn sink(&self) -> Arc<OracleSink> {
        self.sink.clone()
    }
}

impl Connector for OracleConnector {
    fn connector_id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> &'static str {
        "oracle"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config() -> OracleSinkConfig {
        OracleSinkConfig {
            url: "https://oracle-host:8080/ords/hr/_/sql".to_string(),
            schema: "HR".to_string(),
            table: "TELEMETRY".to_string(),
            username: "c##appuser".to_string(),
            password: "SecretPassword123!".to_string(),
            custom_upsert: None,
            key_columns: vec!["device_id".to_string()],
            batch_size: Some(1),
            buffer_capacity: None,
            timeout_ms: None,
        }
    }

    #[test]
    fn test_merge_sql_builder() {
        let keys = vec!["device_id".to_string()];
        let cols = vec![
            "device_id".to_string(),
            "temp".to_string(),
            "ts".to_string(),
        ];
        let sql = build_merge_sql("TELEMETRY", &keys, &cols).expect("valid merge sql");
        assert!(sql.starts_with("MERGE INTO TELEMETRY t"));
        assert!(sql.contains("USING (SELECT :1 AS device_id, :2 AS temp, :3 AS ts FROM DUAL) s"));
        assert!(sql.contains("ON (t.device_id = s.device_id)"));
        assert!(sql.contains("WHEN MATCHED THEN UPDATE SET t.temp = s.temp, t.ts = s.ts"));
        assert!(sql.contains(
            "WHEN NOT MATCHED THEN INSERT (device_id, temp, ts) VALUES (s.device_id, s.temp, s.ts)"
        ));
    }

    #[test]
    fn test_custom_upsert_override() {
        let mut cfg = sample_config();
        cfg.custom_upsert = Some("MERGE INTO CUSTOM_TABLE ...".to_string());
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_basic_auth_header() {
        let cfg = sample_config();
        let header = cfg.basic_auth_header();
        assert!(header.starts_with("Basic "));
        let b64 = &header["Basic ".len()..];
        let decoded = String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(b64)
                .expect("valid base64"),
        )
        .expect("utf8 string");
        assert_eq!(decoded, "c##appuser:SecretPassword123!");
    }

    #[test]
    fn test_classify_ora_errors() {
        // Retryable
        assert_eq!(
            classify_ora_error("ORA-00060: deadlock detected"),
            OraErrorClassification::Retryable
        );
        assert_eq!(
            classify_ora_error("ORA-01033: ORACLE initialization in progress"),
            OraErrorClassification::Retryable
        );
        assert_eq!(
            classify_ora_error("ORA-03113: end-of-file on communication channel"),
            OraErrorClassification::Retryable
        );

        // Terminal
        assert_eq!(
            classify_ora_error("ORA-00942: table or view does not exist"),
            OraErrorClassification::Terminal
        );
        assert_eq!(
            classify_ora_error("ORA-01017: invalid username/password"),
            OraErrorClassification::Terminal
        );
        assert_eq!(
            classify_ora_error("ORA-00001: unique constraint violated"),
            OraErrorClassification::Terminal
        );

        // Unknown
        assert_eq!(
            classify_ora_error("Some random error"),
            OraErrorClassification::Unknown
        );
    }

    #[test]
    fn test_typed_connection_errors_retry() {
        // Each case uses the driver error type, not text.
        let cases = [
            oracle_rs::Error::oracle(3113, "end-of-file"),
            oracle_rs::Error::oracle(3114, "not connected"),
            oracle_rs::Error::ServerError {
                code: 3135,
                message: "lost contact".into(),
            },
            oracle_rs::Error::InvalidServiceName {
                service_name: Some("FREEPDB1".into()),
                message: Some("listener fault".into()),
            },
        ];
        for error in cases {
            assert_eq!(
                classify_oracle_driver_error(&error),
                OraErrorKind::Connection,
                "must be connection: {error:?}"
            );
            assert!(
                ora_error_is_retryable(classify_oracle_driver_error(&error)),
                "connection must retry: {error:?}"
            );
            assert!(
                matches!(
                    map_oracle_driver_error(error),
                    ConnectorError::Connection(_)
                ),
                "connection maps to retry"
            );
        }
    }

    #[test]
    fn test_typed_authentication_errors_terminal() {
        let cases = [
            oracle_rs::Error::oracle(1017, "bad password"),
            oracle_rs::Error::oracle(28000, "locked"),
            oracle_rs::Error::AuthenticationFailed("denied".into()),
            oracle_rs::Error::InvalidCredentials,
        ];
        for error in cases {
            assert_eq!(
                classify_oracle_driver_error(&error),
                OraErrorKind::Authentication,
                "must be auth: {error:?}"
            );
            assert!(
                !ora_error_is_retryable(classify_oracle_driver_error(&error)),
                "auth must not retry: {error:?}"
            );
            assert!(
                matches!(map_oracle_driver_error(error), ConnectorError::Dispatch(_)),
                "auth maps to terminal"
            );
        }
    }

    #[test]
    fn test_typed_constraint_errors_terminal() {
        // ServerError without ORA prefix still maps by code.
        // The old text path retried it; the typed path stops.
        let cases = [
            oracle_rs::Error::oracle(1, "unique"),
            oracle_rs::Error::oracle(1400, "null"),
            oracle_rs::Error::oracle(2291, "parent"),
            oracle_rs::Error::ServerError {
                code: 1,
                message: "unique".into(),
            },
        ];
        for error in cases {
            assert_eq!(
                classify_oracle_driver_error(&error),
                OraErrorKind::Constraint,
                "must be constraint: {error:?}"
            );
            assert!(
                matches!(map_oracle_driver_error(error), ConnectorError::Dispatch(_)),
                "constraint maps to terminal"
            );
        }
    }

    #[test]
    fn test_typed_syntax_errors_terminal() {
        let cases = [
            oracle_rs::Error::oracle(904, "identifier"),
            oracle_rs::Error::oracle(942, "table"),
            oracle_rs::Error::ServerError {
                code: 942,
                message: "missing".into(),
            },
        ];
        for error in cases {
            assert_eq!(
                classify_oracle_driver_error(&error),
                OraErrorKind::Syntax,
                "must be syntax: {error:?}"
            );
            assert!(
                matches!(map_oracle_driver_error(error), ConnectorError::Dispatch(_)),
                "syntax maps to terminal"
            );
        }
    }

    #[test]
    fn test_typed_timeout_errors_retry() {
        let cases = [
            oracle_rs::Error::oracle(12170, "connect timeout"),
            oracle_rs::Error::oracle(1013, "cancel"),
            oracle_rs::Error::ConnectionTimeout(Duration::from_secs(1)),
        ];
        for error in cases {
            assert_eq!(
                classify_oracle_driver_error(&error),
                OraErrorKind::Timeout,
                "must be timeout: {error:?}"
            );
            assert!(
                matches!(
                    map_oracle_driver_error(error),
                    ConnectorError::Connection(_)
                ),
                "timeout maps to retry"
            );
        }
    }

    #[test]
    fn test_typed_transport_errors_retry() {
        // Non-Oracle faults carry no ORA code and retry.
        let cases = [
            oracle_rs::Error::ConnectionClosed,
            oracle_rs::Error::ConnectionRefused {
                error_code: None,
                message: None,
            },
            oracle_rs::Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "reset",
            )),
        ];
        for error in cases {
            assert_eq!(
                classify_oracle_driver_error(&error),
                OraErrorKind::Transport,
                "must be transport: {error:?}"
            );
            assert!(
                matches!(
                    map_oracle_driver_error(error),
                    ConnectorError::Connection(_)
                ),
                "transport maps to retry"
            );
        }
    }

    #[test]
    fn test_ora_code_unparsable_and_unknown_terminal() {
        // No digits after the tag means no code.
        assert_eq!(parse_ora_code("ORA-ABCDE"), None);
        assert_eq!(parse_ora_code("no code here"), None);
        assert_eq!(
            classify_ora_error("ORA-ABCDE: bad tag"),
            OraErrorClassification::Unknown
        );
        // Unknown numeric codes fail closed without retry.
        assert_eq!(classify_ora_code(99999), OraErrorKind::Unknown);
        assert!(!ora_error_is_retryable(OraErrorKind::Unknown));
        let error = oracle_rs::Error::oracle(99999, "future code");
        assert!(
            matches!(map_oracle_driver_error(error), ConnectorError::Dispatch(_)),
            "unknown code maps to terminal"
        );
    }

    #[test]
    fn test_typed_ora_00000_retries() {
        // Zero carries no detail and closes the session.
        let error = oracle_rs::Error::oracle(0, "normal");
        assert_eq!(
            classify_oracle_driver_error(&error),
            OraErrorKind::Connection
        );
        assert!(matches!(
            map_oracle_driver_error(error),
            ConnectorError::Connection(_)
        ));
    }

    #[tokio::test]
    async fn test_terminal_state_after_retries() {
        // Two transient faults, then success: the end state commits.
        let transport = Arc::new(MockOracleTransport::with_transient_failures(2, "ORA-00060"));
        let mut cfg = sample_config();
        cfg.batch_size = Some(10);
        let sink = OracleSink::new(cfg, transport.clone()).expect("valid sink");
        let topic = Topic::new("sensors/temp").unwrap();
        let payload = Bytes::from_static(br#"{"device_id": "dev-90", "temp": 70.0}"#);
        sink.send(&topic, &payload, QoS::AtLeastOnce)
            .await
            .expect("buffer row");
        let first = sink.flush().await;
        assert!(matches!(first, Err(ConnectorError::Connection(_))));
        *sink.backoff.lock() = BackoffState::default();
        let second = sink.flush().await;
        assert!(matches!(second, Err(ConnectorError::Connection(_))));
        *sink.backoff.lock() = BackoffState::default();
        sink.flush().await.expect("retry commits");
        assert_eq!(sink.sent_count(), 1);
        assert_eq!(sink.buffered_rows(), 0);
        // Terminal fault stops at once with one execution.
        let bad = Arc::new(MockOracleTransport::with_transient_failures(1, "ORA-00001"));
        let mut bad_cfg = sample_config();
        bad_cfg.batch_size = Some(10);
        let bad_sink = OracleSink::new(bad_cfg, bad.clone()).expect("valid sink");
        bad_sink
            .send(&topic, &payload, QoS::AtLeastOnce)
            .await
            .expect("buffer row");
        let refused = bad_sink.flush().await;
        assert!(matches!(refused, Err(ConnectorError::Dispatch(_))));
        assert_eq!(bad.executions.lock().len(), 1);
    }

    #[test]
    fn test_json_to_oracle_primitives() {
        let v_null = json_to_oracle_value(&serde_json::Value::Null);
        assert_eq!(v_null, OracleValue::Null);

        let v_int = json_to_oracle_value(&serde_json::json!(42));
        assert_eq!(v_int, OracleValue::Integer(42));
        assert_eq!(v_int.data_type_hint(), "NUMBER");

        let v_float = json_to_oracle_value(&serde_json::json!(23.75));
        assert_eq!(v_float, OracleValue::Number(23.75));

        let v_str = json_to_oracle_value(&serde_json::json!("sensor-alpha"));
        assert_eq!(v_str, OracleValue::String("sensor-alpha".into()));
        assert_eq!(v_str.data_type_hint(), "VARCHAR2");

        let v_bool = json_to_oracle_value(&serde_json::json!(true));
        assert_eq!(v_bool, OracleValue::Boolean(true));
    }

    #[test]
    fn test_row_extraction_nested_or_object() {
        let payload = br#"{"device_id": "d-101", "temp": 72.4, "active": true}"#;
        let row = extract_oracle_row(payload, "factory/sensor1").expect("valid row");
        assert_eq!(row.columns.len(), 3);
        let id_val = row.columns.iter().find(|(k, _)| k == "device_id").unwrap();
        assert_eq!(id_val.1, OracleValue::String("d-101".into()));
    }

    #[tokio::test]
    async fn test_oracle_sink_loopback_success() {
        let cfg = sample_config();
        let transport = Arc::new(MockOracleTransport::new());
        let sink = OracleSink::new(cfg, transport.clone()).expect("valid sink");

        let topic = Topic::new("sensors/temp").unwrap();
        let payload = Bytes::from_static(br#"{"device_id": "dev-01", "temp": 85.5}"#);

        sink.send(&topic, &payload, QoS::AtLeastOnce)
            .await
            .expect("send succeeds");

        let execs = transport.executions.lock();
        assert_eq!(execs.len(), 1);
        assert!(execs[0].statement.contains("MERGE INTO TELEMETRY t"));
        assert_eq!(execs[0].binds.len(), 2);
        assert_eq!(sink.sent_count(), 1);
    }

    #[tokio::test]
    async fn test_oracle_sink_retryable_deadlock_handling() {
        let cfg = sample_config();
        // 1 transient failure (ORA-00060) then success
        let transport = Arc::new(MockOracleTransport::with_transient_failures(1, "ORA-00060"));
        let sink = OracleSink::new(cfg, transport.clone()).expect("valid sink");

        let topic = Topic::new("sensors/temp").unwrap();
        let payload = Bytes::from_static(br#"{"device_id": "dev-02", "temp": 91.0}"#);

        // First attempt fails with Connection error (transient)
        let res = sink.send(&topic, &payload, QoS::AtLeastOnce).await;
        assert!(res.is_err());
        assert!(matches!(res.err().unwrap(), ConnectorError::Connection(_)));

        // Reset backoff to simulate time passage
        *sink.backoff.lock() = BackoffState::default();

        // Flush restores rows and succeeds on next attempt
        sink.flush().await.expect("retry succeeds");
        assert_eq!(sink.sent_count(), 1);
    }

    #[tokio::test]
    async fn test_backoff_keeps_buffered_rows() {
        let mut cfg = sample_config();
        cfg.batch_size = Some(10);
        let transport = Arc::new(MockOracleTransport::with_transient_failures(1, "ORA-00060"));
        let sink = OracleSink::new(cfg, transport.clone()).expect("valid sink");

        let topic = Topic::new("sensors/temp").unwrap();
        let payload1 = Bytes::from_static(br#"{"device_id": "dev-10", "temp": 70.0}"#);
        let payload2 = Bytes::from_static(br#"{"device_id": "dev-11", "temp": 71.0}"#);

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
        let execs = transport.executions.lock();
        assert_eq!(execs.len(), 3);
        assert_eq!(execs[1].binds.len(), 2);
        assert_eq!(execs[2].binds.len(), 2);
    }

    /// Deny an empty endpoint at build time.
    /// Show each denial fails closed with a dispatch error.
    #[test]
    fn test_driver_transport_rejects_empty_endpoint() {
        let timeout = Duration::from_secs(1);
        let denied = DriverOracleTransport::new("", 1521, "FREEPDB1", "system", "pw", timeout);
        assert!(matches!(denied, Err(ConnectorError::Dispatch(_))));
        let denied = DriverOracleTransport::new("db", 0, "FREEPDB1", "system", "pw", timeout);
        assert!(matches!(denied, Err(ConnectorError::Dispatch(_))));
        let denied = DriverOracleTransport::new("db", 1521, "", "system", "pw", timeout);
        assert!(matches!(denied, Err(ConnectorError::Dispatch(_))));
        let denied = DriverOracleTransport::new("db", 1521, "FREEPDB1", "", "pw", timeout);
        assert!(matches!(denied, Err(ConnectorError::Dispatch(_))));
        let ok = DriverOracleTransport::new("db", 1521, "FREEPDB1", "system", "pw", timeout);
        assert!(ok.is_ok());
    }

    /// Fail closed when the environment lacks the named address.
    fn qual_require(name: &str) -> String {
        std::env::var(name)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| {
                panic!("{name} must name a real Oracle Database server for qualification")
            })
    }

    /// Fail closed on table names outside the safe identifier set.
    fn qual_identifier(name: &str, value: &str) -> String {
        assert!(
            !value.is_empty() && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "qual {name} must match [A-Za-z0-9_]+, got {value:?} (failing closed)"
        );
        value.to_string()
    }

    /// Open a bare driver session for setup and assertions.
    /// Panics with context when unreachable: qualification fails,
    /// never skips.
    async fn qual_connect(
        host: &str,
        port: u16,
        service: &str,
        user: &str,
        password: &str,
    ) -> oracle_rs::Connection {
        let config = oracle_rs::Config::new(host, port, service, user, password);
        tokio::time::timeout(
            Duration::from_secs(60),
            oracle_rs::Connection::connect_with_config(config),
        )
        .await
        .expect("qual connect timed out")
        .unwrap_or_else(|e| panic!("qual connect failed: {e:?}"))
    }

    /// Read an integer result. Parse text numbers. Fail the test on other types.
    fn qual_as_i64(value: &oracle_rs::Value) -> i64 {
        if let Some(num) = value.as_i64() {
            return num;
        }
        if let Some(num) = value.as_f64() {
            return num as i64;
        }
        if let Some(text) = value.as_str() {
            if let Ok(num) = text.trim().parse::<i64>() {
                return num;
            }
            if let Ok(num) = text.trim().parse::<f64>() {
                return num as i64;
            }
        }
        panic!("qual expected integer, got {value:?}")
    }

    /// Read a float result. Parse text numbers. Fail the test on other types.
    fn qual_as_f64(value: &oracle_rs::Value) -> f64 {
        if let Some(num) = value.as_f64() {
            return num;
        }
        if let Some(num) = value.as_i64() {
            return num as f64;
        }
        if let Some(text) = value.as_str() {
            if let Ok(num) = text.trim().parse::<f64>() {
                return num;
            }
        }
        panic!("qual expected number, got {value:?}")
    }

    /// Read a text bind result. Fails the test on other types.
    fn qual_as_str(value: &oracle_rs::Value) -> String {
        value
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| panic!("qual expected text, got {value:?}"))
    }

    /// Qualification against a real Oracle Database server over
    /// the maintained driver ([`DriverOracleTransport`]).
    ///
    /// Run with e.g.:
    /// `ORACLE_HOST=127.0.0.1 ORACLE_PORT=1521 ORACLE_SERVICE=FREEPDB1 \
    ///  ORACLE_USER=system ORACLE_PASSWORD='Oraclepass1' \
    ///  ORACLE_TABLE=qual_b333 \
    ///  cargo test -p broker-connectors-enterprise --lib oracle::tests::test_qualify_driver_write_path -- --ignored --nocapture --test-threads=1`
    ///
    /// Creates the table.
    /// Streams 500 rows through [`OracleSink`] on
    /// [`DriverOracleTransport`] with MERGE binds.
    /// Asserts the exact count and the first/last content
    /// through an independent session.
    /// Proves a wrong table fails as a dispatch error
    /// with the server text.
    /// Drops the table.
    /// Panics when its environment is missing (fail closed,
    /// never skips).
    #[tokio::test]
    #[ignore = "needs a real Oracle Database server (see ORACLE_* env)"]
    async fn test_qualify_driver_write_path() {
        const ROWS: usize = 500;

        let host = qual_require("ORACLE_HOST");
        let port: u16 = qual_require("ORACLE_PORT")
            .parse()
            .expect("qual ORACLE_PORT must be a port number");
        let service = qual_require("ORACLE_SERVICE");
        let user = qual_require("ORACLE_USER");
        let password = qual_require("ORACLE_PASSWORD");
        let table = qual_identifier("table", &qual_require("ORACLE_TABLE"));

        let admin = qual_connect(&host, port, &service, &user, &password).await;

        // Identity: only the real product answers this view.
        let version_result = admin
            .query("SELECT BANNER FROM v$version", &[])
            .await
            .expect("qual version query");
        let banners: Vec<String> = version_result
            .rows
            .iter()
            .map(|row| {
                row.get(0)
                    .map(qual_as_str)
                    .expect("qual version row has banner")
            })
            .collect();
        assert!(
            !banners.is_empty(),
            "qual version must list rows (failing closed)"
        );
        let version_line = banners.join(" | ");
        assert!(
            version_line.to_ascii_lowercase().contains("oracle"),
            "qual version must name the product, got {version_line:?}"
        );
        eprintln!("qual server: version={version_line}");

        // Remove stale state from a prior run first.
        // Use IF EXISTS: the server returns success when the table
        // is absent. A plain DROP reports ORA-00942 on that path,
        // and the driver closes the connection for it.
        match admin
            .execute(&format!("DROP TABLE IF EXISTS {table}"), &[])
            .await
        {
            Ok(_) => {
                admin.commit().await.expect("qual drop commits");
                eprintln!("qual cleanup stale: dropped {table}");
            }
            Err(error) => {
                let text = error.to_string();
                assert!(
                    text.contains("942") || text.to_ascii_lowercase().contains("does not exist"),
                    "qual drop stale failed: {text}"
                );
                let _ = admin.rollback().await;
                eprintln!("qual cleanup stale (tolerated): {text}");
            }
        }

        // The table for this run exists before the first flush.
        admin
            .execute(
                &format!(
                    "CREATE TABLE {table} (\
                     device_id VARCHAR2(64) PRIMARY KEY, \
                     seq NUMBER NOT NULL, \
                     reading NUMBER NOT NULL)"
                ),
                &[],
            )
            .await
            .expect("qual create table");
        admin.commit().await.expect("qual create commits");
        eprintln!("qual table created: {table} (VARCHAR2/NUMBER)");

        // The production transport below speaks TNS itself.
        let timeout = Duration::from_secs(30);
        let transport = Arc::new(
            DriverOracleTransport::new(&host, port, &service, &user, &password, timeout)
                .expect("qual transport builds"),
        );
        let config = OracleSinkConfig {
            url: format!("oracle:thin:@{host}:{port}/{service}"),
            schema: user.to_ascii_uppercase(),
            table: table.clone(),
            username: user.clone(),
            password: password.clone(),
            custom_upsert: None,
            key_columns: vec!["device_id".to_string()],
            batch_size: Some(100),
            buffer_capacity: None,
            timeout_ms: Some(30_000),
        };
        config.validate().expect("qual config validates");
        let sink = Arc::new(OracleSink::new(config, transport).expect("qual sink builds"));
        assert_eq!(sink.kind(), "oracle");

        let topic = Topic::new("sensors/qual").unwrap();
        for seq in 0..ROWS {
            let device = format!("qual-{seq:06}");
            let reading = 20.0 + seq as f64 * 0.5;
            let payload = Bytes::from(format!(
                r#"{{"device_id":"{device}","seq":{seq},"reading":{reading}}}"#
            ));
            sink.send(&topic, &payload, QoS::AtLeastOnce)
                .await
                .unwrap_or_else(|e| panic!("qual send seq={seq} failed: {e:?}"));
        }
        sink.flush().await.expect("qual flush");
        assert_eq!(sink.sent_count(), ROWS as u64, "qual sent records");
        eprintln!("qual rows sent: records={ROWS} table={table}");

        // The count comes back from the server, not the counters.
        let count_result = admin
            .query(&format!("SELECT COUNT(*) FROM {table}"), &[])
            .await
            .expect("qual count");
        let count = count_result
            .rows
            .first()
            .and_then(|row| row.get(0))
            .map(qual_as_i64)
            .expect("qual count row");
        assert_eq!(count, ROWS as i64, "qual count mismatch");
        eprintln!("qual rows asserted: count={ROWS} table={table}");

        // The first and the last row come back with exact content.
        for (device, seq) in [("qual-000000", 0), ("qual-000499", 499)] {
            let sample = admin
                .query(
                    &format!("SELECT device_id, seq, reading FROM {table} WHERE device_id = :1"),
                    &[oracle_rs::Value::String(device.to_string())],
                )
                .await
                .expect("qual sample");
            let row = sample.rows.first().expect("qual sample row");
            let got_device = row.get(0).map(qual_as_str).expect("qual device");
            let got_seq = row.get(1).map(qual_as_i64).expect("qual seq");
            let got_reading = row.get(2).map(qual_as_f64).expect("qual reading");
            assert_eq!(got_device, device, "qual device mismatch");
            assert_eq!(got_seq, seq, "qual seq mismatch");
            let want_reading = 20.0 + seq as f64 * 0.5;
            assert!(
                (got_reading - want_reading).abs() < 1e-6,
                "qual reading mismatch for {device}: got {got_reading}, want {want_reading}"
            );
        }
        eprintln!("qual values asserted: first=(qual-000000,0,20.0) last=(qual-000499,499,269.5)");

        // Fault: a wrong table fails closed with the server text.
        let bad_table = qual_identifier("missing table", &format!("{table}_missing"));
        let bad_config = OracleSinkConfig {
            url: format!("oracle:thin:@{host}:{port}/{service}"),
            schema: user.to_ascii_uppercase(),
            table: bad_table.clone(),
            username: user.clone(),
            password: password.clone(),
            custom_upsert: None,
            key_columns: vec!["device_id".to_string()],
            batch_size: Some(100),
            buffer_capacity: None,
            timeout_ms: Some(30_000),
        };
        let bad_transport = Arc::new(
            DriverOracleTransport::new(&host, port, &service, &user, &password, timeout)
                .expect("qual bad transport builds"),
        );
        let bad_sink =
            Arc::new(OracleSink::new(bad_config, bad_transport).expect("qual bad sink builds"));
        bad_sink
            .send(
                &topic,
                &Bytes::from_static(br#"{"device_id":"qual-bad","seq":-1,"reading":0.0}"#),
                QoS::AtLeastOnce,
            )
            .await
            .expect("qual bad buffer");
        let error = bad_sink.flush().await.expect_err("wrong table must fail");
        match &error {
            ConnectorError::Dispatch(message) => assert!(
                message.contains("ORA-00942")
                    || message.to_ascii_lowercase().contains("does not exist"),
                "qual fault must carry the server text, got {message:?}"
            ),
            other => panic!("wrong table must be terminal, got {other:?}"),
        }
        eprintln!("qual fault asserted: wrong table is a dispatch error ({error:?})");

        // Cleanup: drop the table for this run.
        admin
            .execute(&format!("DROP TABLE {table}"), &[])
            .await
            .expect("qual drop table");
        admin.commit().await.expect("qual drop commits");
        eprintln!("qual done: rows={ROWS} table={table} cleaned");
    }
}
