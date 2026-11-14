//! MySQL / MariaDB sink over the native client protocol.
//!
//! MQTT events become parameterized rows: `?` markers bind `(topic,
//! QoS, payload)` positionally, so values travel out-of-band and SQL
//! injection is structurally impossible. Production execution prepares
//! once per batch and runs one `COM_STMT_EXECUTE` per buffered row
//! through the maintained `mysql_async` driver
//! ([`DriverMySqlTransport`]). The [`MySqlTransport`] boundary keeps
//! unit tests broker-free ([`MemoryMySqlTransport`]).

use super::{ConnectorError, Result, Sink, SinkBatchItem};
use async_trait::async_trait;
use broker_protocol::{QoS, Topic};
use bytes::Bytes;
use mysql_async::prelude::Queryable;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

fn default_pool_size() -> usize {
    10
}

fn default_batch_size() -> usize {
    100
}

fn default_batch_timeout_ms() -> u64 {
    50
}

// MySQL documented default for max packet (4 MiB).
// The driver asks the server on connect. Setting it saves one roundtrip.
const DRIVER_MAX_ALLOWED_PACKET: usize = 4_194_304;

// MySQL documented default for wait timeout (8 hours in seconds).
// The driver asks the server on connect. Setting it saves one roundtrip.
const DRIVER_WAIT_TIMEOUT: usize = 28_800;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MySqlSinkConfig {
    pub connection_url: String,
    pub sql_template: String,
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_batch_timeout_ms")]
    pub batch_timeout_ms: u64,
}

impl MySqlSinkConfig {
    pub fn validate(&self) -> Result<()> {
        if self.connection_url.trim().is_empty() {
            return Err(ConnectorError::Dispatch(
                "mysql connection_url must not be empty".to_string(),
            ));
        }
        if self.sql_template.trim().is_empty() {
            return Err(ConnectorError::Dispatch(
                "mysql sql_template must not be empty".to_string(),
            ));
        }
        if count_placeholders(&self.sql_template) == 0 {
            return Err(ConnectorError::Dispatch(
                "mysql sql_template needs at least one ? marker".to_string(),
            ));
        }
        if self.pool_size == 0 {
            return Err(ConnectorError::Dispatch(
                "mysql pool_size must be >= 1".to_string(),
            ));
        }
        if self.batch_size == 0 {
            return Err(ConnectorError::Dispatch(
                "mysql batch_size must be >= 1".to_string(),
            ));
        }
        // The sink always binds exactly three values (topic, QoS,
        // payload), so the template must carry exactly three markers.
        if count_placeholders(&self.sql_template) != 3 {
            return Err(ConnectorError::Dispatch(
                "mysql sql_template must carry exactly 3 ? markers (topic, qos, payload)"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

/// Count `?` markers outside strings, quoted identifiers, and comments.
/// The sink always binds exactly three values (topic, QoS, payload), so
/// templates must carry exactly three markers.
pub fn count_placeholders(template: &str) -> usize {
    let chars: Vec<char> = template.chars().collect();
    let n = chars.len();
    let mut count = 0;
    let mut i = 0;
    let mut in_string: Option<char> = None;
    let mut in_line_comment = false;
    let mut in_block_comment = false;
    while i < n {
        let c = chars[i];
        if in_line_comment {
            if c == '\n' {
                in_line_comment = false;
            }
            i += 1;
            continue;
        }
        if in_block_comment {
            if c == '*' && i + 1 < n && chars[i + 1] == '/' {
                in_block_comment = false;
                i += 2;
            } else {
                i += 1;
            }
            continue;
        }
        if let Some(quote) = in_string {
            if c == quote {
                if (quote == '\'' || quote == '"') && i + 1 < n && chars[i + 1] == quote {
                    i += 2;
                } else {
                    in_string = None;
                    i += 1;
                }
            } else if quote == '`' && c == '\\' {
                i += 2;
            } else {
                i += 1;
            }
            continue;
        }
        match c {
            '\'' | '"' | '`' => {
                in_string = Some(c);
                i += 1;
            }
            '-' if i + 1 < n && chars[i + 1] == '-' => {
                in_line_comment = true;
                i += 2;
            }
            '#' => {
                in_line_comment = true;
                i += 1;
            }
            '/' if i + 1 < n && chars[i + 1] == '*' => {
                in_block_comment = true;
                i += 2;
            }
            '?' => {
                count += 1;
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }
    count
}

/// One flushed batch: the statement plus one 3-column row per event.
#[derive(Debug, Clone, Default)]
pub struct MySqlBatch {
    pub sql: String,
    pub rows: Vec<Vec<Vec<u8>>>,
}

/// Outcome of one batch execution: `processed` counts the prefix of
/// rows that reached a final state (inserted or rejected — those rows
/// are never sent again), `rejected` carries the batch index and server
/// message of each row the server refused, and `error` reports the
/// connection- or statement-level failure that stopped the batch early,
/// if any.
#[derive(Debug, Default)]
pub struct MySqlBatchOutcome {
    pub processed: usize,
    pub rejected: Vec<(usize, String)>,
    pub error: Option<ConnectorError>,
}

#[async_trait]
pub trait MySqlTransport: Send + Sync {
    async fn execute_batch(&self, batch: &MySqlBatch) -> MySqlBatchOutcome;
}

/// In-memory transport recording every flushed batch (tests, dry runs).
#[derive(Debug, Default)]
pub struct MemoryMySqlTransport {
    batches: parking_lot::Mutex<Vec<MySqlBatch>>,
    failures_left: parking_lot::Mutex<usize>,
    calls: AtomicU64,
}

impl MemoryMySqlTransport {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fail the next `n` executions with a transport error (backoff tests).
    pub fn fail_next(&self, n: usize) {
        *self.failures_left.lock() = n;
    }

    pub fn batches(&self) -> Vec<MySqlBatch> {
        self.batches.lock().clone()
    }

    pub fn calls(&self) -> u64 {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl MySqlTransport for MemoryMySqlTransport {
    async fn execute_batch(&self, batch: &MySqlBatch) -> MySqlBatchOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut failures = self.failures_left.lock();
        if *failures > 0 {
            *failures -= 1;
            return MySqlBatchOutcome {
                processed: 0,
                rejected: Vec::new(),
                error: Some(ConnectorError::Connection(
                    "mock transport down".to_string(),
                )),
            };
        }
        self.batches.lock().push(batch.clone());
        MySqlBatchOutcome {
            processed: batch.rows.len(),
            rejected: Vec::new(),
            error: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Production transport on the maintained `mysql_async` driver.
// ---------------------------------------------------------------------------

/// Checkout timeout for one driver-pool connection: 5 s bounds the
/// handshake (the protocol requires a bound, not a specific value).
const DRIVER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Per-statement timeout for the batch prepare and each row execute:
/// 10 s bounds each exchange.
const DRIVER_STATEMENT_TIMEOUT: Duration = Duration::from_secs(10);

/// Production MySQL / MariaDB transport on the maintained `mysql_async`
/// driver (MIT/Apache-2.0): native client protocol with driver-owned
/// pooling, both password plugins (`mysql_native_password` and
/// `caching_sha2_password`), an explicit `COM_STMT_PREPARE` per batch
/// and one `COM_STMT_EXECUTE` per buffered row. Parameters travel bound
/// (topic, QoS, payload as binary blobs), so values stay out-of-band.
/// Production wiring uses this transport.
///
/// Bound: the driver pool holds at most `pool_size` connections plus
/// the sink buffer in front of it; no background queue. `pool_size`
/// carries the configured default 10 (`default_pool_size`): the default
/// is finite because an unbounded pool under fan-in would repeat the
/// multi-GB RSS collapse the v4 benchmark measured on this path.
pub struct DriverMySqlTransport {
    pool: mysql_async::Pool,
}

impl DriverMySqlTransport {
    pub fn new(url: &str, pool_size: usize) -> Result<Self> {
        if pool_size == 0 {
            return Err(ConnectorError::Dispatch(
                "mysql pool_size must be >= 1".to_string(),
            ));
        }
        let opts = mysql_async::Opts::from_url(url)
            .map_err(|e| ConnectorError::Dispatch(format!("mysql invalid connection url: {e}")))?;
        let constraints = mysql_async::PoolConstraints::new(0, pool_size).ok_or_else(|| {
            ConnectorError::Dispatch(format!("mysql invalid pool size {pool_size}"))
        })?;
        let pool_opts = mysql_async::PoolOpts::default().with_constraints(constraints);
        let builder = mysql_async::OptsBuilder::from_opts(opts).pool_opts(pool_opts);
        Ok(Self {
            pool: mysql_async::Pool::new(mysql_async::Opts::from(builder)),
        })
    }

    /// Build from discrete connection fields instead of a URL. User,
    /// password and database travel as driver values (never formatted
    /// into a URL string), so a value with URL delimiters (`@`, `:`,
    /// `/`, `?`, `#`) or a quote reaches the server verbatim as a
    /// bound credential, not as URL syntax.
    pub fn from_parts(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        database: &str,
        pool_size: usize,
    ) -> Result<Self> {
        if host.trim().is_empty() {
            return Err(ConnectorError::Dispatch(
                "mysql host must not be empty".to_string(),
            ));
        }
        if port == 0 {
            return Err(ConnectorError::Dispatch(
                "mysql port must be 1..=65535".to_string(),
            ));
        }
        if pool_size == 0 {
            return Err(ConnectorError::Dispatch(
                "mysql pool_size must be >= 1".to_string(),
            ));
        }
        let user = if user.is_empty() { "root" } else { user };
        let constraints = mysql_async::PoolConstraints::new(0, pool_size).ok_or_else(|| {
            ConnectorError::Dispatch(format!("mysql invalid pool size {pool_size}"))
        })?;
        let pool_opts = mysql_async::PoolOpts::default().with_constraints(constraints);
        let builder = mysql_async::OptsBuilder::default()
            .ip_or_hostname(host)
            .tcp_port(port)
            .user(Some(user.to_string()))
            .pass(Some(password.to_string()))
            .db_name(Some(database.to_string()))
            .max_allowed_packet(Some(DRIVER_MAX_ALLOWED_PACKET))
            .wait_timeout(Some(DRIVER_WAIT_TIMEOUT))
            .pool_opts(pool_opts);
        Ok(Self {
            pool: mysql_async::Pool::new(mysql_async::Opts::from(builder)),
        })
    }
}

#[async_trait]
impl MySqlTransport for DriverMySqlTransport {
    async fn execute_batch(&self, batch: &MySqlBatch) -> MySqlBatchOutcome {
        let mut outcome = MySqlBatchOutcome::default();
        if batch.rows.is_empty() {
            return outcome;
        }
        let mut conn =
            match tokio::time::timeout(DRIVER_CONNECT_TIMEOUT, self.pool.get_conn()).await {
                Ok(Ok(conn)) => conn,
                Ok(Err(e)) => {
                    outcome.error = Some(ConnectorError::Connection(format!(
                        "mysql driver connect failed: {e}"
                    )));
                    return outcome;
                }
                Err(_) => {
                    outcome.error = Some(ConnectorError::Connection(
                        "mysql driver connect timeout".to_string(),
                    ));
                    return outcome;
                }
            };
        // Explicit prepare mirrors the legacy COM_STMT_PREPARE: a
        // statement-level failure (unknown table, syntax) stops the
        // batch here instead of being miscounted as row rejections
        // below.
        let stmt =
            match tokio::time::timeout(DRIVER_STATEMENT_TIMEOUT, conn.prep(batch.sql.as_str()))
                .await
            {
                Ok(Ok(stmt)) => stmt,
                Ok(Err(mysql_async::Error::Server(server))) => {
                    outcome.error = Some(ConnectorError::Dispatch(format!(
                        "mysql prepare failed: mysql errno {}: {}",
                        server.code, server.message
                    )));
                    return outcome;
                }
                Ok(Err(e)) => {
                    outcome.error = Some(ConnectorError::Connection(format!(
                        "mysql prepare failed: {e}"
                    )));
                    return outcome;
                }
                Err(_) => {
                    outcome.error = Some(ConnectorError::Connection(
                        "mysql prepare timeout".to_string(),
                    ));
                    return outcome;
                }
            };
        for (index, row) in batch.rows.iter().enumerate() {
            // The sink always buffers exactly three values (topic, QoS,
            // payload) and `validate` enforces exactly three `?` markers,
            // so a short row here is unreachable through the sink; the
            // empty default keeps a hand-built batch total instead of
            // panicking.
            let params = (
                row.first().cloned().unwrap_or_default(),
                row.get(1).cloned().unwrap_or_default(),
                row.get(2).cloned().unwrap_or_default(),
            );
            match tokio::time::timeout(DRIVER_STATEMENT_TIMEOUT, conn.exec_drop(&stmt, params))
                .await
            {
                Ok(Ok(())) => {
                    outcome.processed += 1;
                }
                Ok(Err(mysql_async::Error::Server(server))) => {
                    // Row-level rejection (e.g. a CHECK constraint:
                    // errno 3819 on MySQL, 4025 on MariaDB): log and
                    // count the row, then continue with the next row on
                    // the same connection, exactly as the legacy path
                    // treats an ERR reply to COM_STMT_EXECUTE.
                    let message = format!("mysql errno {}: {}", server.code, server.message);
                    tracing::warn!("mysql row {index} rejected by server: {message}");
                    outcome.rejected.push((index, message));
                    outcome.processed += 1;
                }
                Ok(Err(e)) => {
                    outcome.error = Some(ConnectorError::Connection(format!(
                        "mysql execute failed: {e}"
                    )));
                    return outcome;
                }
                Err(_) => {
                    outcome.error = Some(ConnectorError::Connection(
                        "mysql execute timeout".to_string(),
                    ));
                    return outcome;
                }
            }
        }
        outcome
    }
}

// ---------------------------------------------------------------------------
// Sink.
// ---------------------------------------------------------------------------

/// MySQL sink: buffers MQTT events as 3-column rows
/// (`?` topic, QoS, payload positional) and flushes full or stale
/// batches through the transport, with the same backoff contract as the
/// PostgreSQL sink. Rows the server rejects are dropped (counted via
/// [`MySqlSink::rejected_rows`]); after a failure only rows that never
/// reached a final state are restored to the buffer, in order.
pub struct MySqlSink {
    core: Arc<MySqlSinkCore>,
}

/// Shared sink state; the linger-flush task holds only a weak reference
/// to it, so dropping the sink stops the task.
struct MySqlSinkCore {
    config: MySqlSinkConfig,
    transport: Arc<dyn MySqlTransport>,
    buffer: parking_lot::Mutex<super::BatchQueue<Vec<Vec<u8>>>>,
    backoff: parking_lot::Mutex<super::BackoffState>,
    sent_batches: AtomicU64,
    inserted_rows: AtomicU64,
    rejected_rows: AtomicU64,
    /// Notifies the linger task that new rows are available.
    linger_notify: Arc<Notify>,
    /// Tracks the last logged error text for rate-limiting.
    last_linger_error: parking_lot::Mutex<Option<String>>,
    /// Tracks when the last linger error was logged.
    last_linger_error_time: parking_lot::Mutex<Option<Instant>>,
}

impl MySqlSinkCore {
    async fn flush(&self) -> Result<()> {
        self.backoff.lock().check()?;
        let (rows, oldest) = self.buffer.lock().take_batch();
        if rows.is_empty() {
            return Ok(());
        }
        let batch = MySqlBatch {
            sql: self.config.sql_template.clone(),
            rows,
        };
        let outcome = self.transport.execute_batch(&batch).await;
        let inserted = outcome.processed.saturating_sub(outcome.rejected.len()) as u64;
        self.inserted_rows.fetch_add(inserted, Ordering::Relaxed);
        self.rejected_rows
            .fetch_add(outcome.rejected.len() as u64, Ordering::Relaxed);
        match outcome.error {
            None => {
                self.backoff.lock().success();
                self.sent_batches.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Some(e) => {
                // Only rows that never reached a final state go back
                // into the buffer, in their original order; inserted or
                // rejected rows are never sent again.
                let remaining: Vec<_> = batch.rows.into_iter().skip(outcome.processed).collect();
                self.buffer.lock().restore(remaining, oldest);
                // Notify the linger task that rows are available again
                self.linger_notify.notify_one();
                self.backoff.lock().failure();
                Err(e)
            }
        }
    }
}

impl MySqlSink {
    pub fn new(config: MySqlSinkConfig, transport: Arc<dyn MySqlTransport>) -> Result<Self> {
        config.validate()?;
        let linger = Duration::from_millis(config.batch_timeout_ms);
        let core = Arc::new(MySqlSinkCore {
            buffer: parking_lot::Mutex::new(super::BatchQueue::new(config.batch_size, linger)),
            config,
            transport,
            backoff: parking_lot::Mutex::new(super::BackoffState::default()),
            sent_batches: AtomicU64::new(0),
            inserted_rows: AtomicU64::new(0),
            rejected_rows: AtomicU64::new(0),
            linger_notify: Arc::new(Notify::new()),
            last_linger_error: parking_lot::Mutex::new(None),
            last_linger_error_time: parking_lot::Mutex::new(None),
        });
        spawn_linger_flush(&core, linger);
        Ok(Self { core })
    }

    pub fn config(&self) -> &MySqlSinkConfig {
        &self.core.config
    }

    pub fn sent_batches(&self) -> u64 {
        self.core.sent_batches.load(Ordering::Relaxed)
    }

    /// Rows the server acknowledged (OK reply to COM_STMT_EXECUTE).
    pub fn inserted_rows(&self) -> u64 {
        self.core.inserted_rows.load(Ordering::Relaxed)
    }

    /// Rows the server rejected (ERR reply) and the sink dropped.
    pub fn rejected_rows(&self) -> u64 {
        self.core.rejected_rows.load(Ordering::Relaxed)
    }

    pub fn buffered_rows(&self) -> usize {
        self.core.buffer.lock().len()
    }

    /// Flush buffered rows as one batch (no-op when empty). While
    /// backing off, fails fast without touching the transport.
    pub async fn flush(&self) -> Result<()> {
        self.core.flush().await
    }

    fn buffer_row(&self, topic: &Topic, payload: &Bytes, qos: QoS) -> bool {
        let was_empty = self.core.buffer.lock().is_empty();
        let should_flush = self.core.buffer.lock().push(vec![
            topic.as_str().as_bytes().to_vec(),
            u8::from(qos).to_string().into_bytes(),
            payload.to_vec(),
        ]);
        if was_empty {
            self.core.linger_notify.notify_one();
        }
        should_flush
    }
}

/// Flush stale rows without waiting for new events: the timer runs only
/// while rows are buffered. It arms when a row lands in an empty buffer
/// or rows are restored after a failure; it ends when the buffer is
/// empty. While backing off, it waits for the backoff window instead of
/// spinning. The task holds only a weak reference to the sink state, so
/// dropping the sink stops it; with no tokio runtime (sink built outside
/// one) no task is spawned and flushing stays event-driven.
fn spawn_linger_flush(core: &Arc<MySqlSinkCore>, linger: Duration) {
    if linger.is_zero() {
        // Zero linger flushes on every push, and a zero period would
        // panic tokio's interval.
        return;
    }
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return;
    };
    let weak = Arc::downgrade(core);
    let notify = core.linger_notify.clone();

    handle.spawn(async move {
        const BACKOFF_POLL_INTERVAL: Duration = Duration::from_secs(1);
        const ERROR_LOG_THROTTLE: Duration = Duration::from_secs(30);

        loop {
            // Snapshot buffer/backoff state, then drop the strong
            // reference before any wait so a dropped sink is freed
            // promptly instead of being pinned for up to 60 s.
            let (has_rows, is_stale, in_backoff) = match weak.upgrade() {
                None => break,
                Some(core) => {
                    let (has_rows, is_stale) = {
                        let buffer = core.buffer.lock();
                        (!buffer.is_empty(), buffer.is_stale())
                    };
                    let in_backoff = core.backoff.lock().check().is_err();
                    drop(core);
                    (has_rows, is_stale, in_backoff)
                }
            };

            if !has_rows {
                // Buffer empty: wait for notification of new rows.
                // Use a long periodic sleep to re-check sink existence.
                tokio::select! {
                    _ = notify.notified() => continue,
                    _ = tokio::time::sleep(Duration::from_secs(60)) => continue,
                }
            }

            // Buffer has rows. Check backoff state.
            if in_backoff {
                // In backoff: wait for backoff window or new rows.
                tokio::select! {
                    _ = notify.notified() => continue,
                    _ = tokio::time::sleep(BACKOFF_POLL_INTERVAL) => continue,
                }
            }

            // Not in backoff. If stale, flush immediately; else wait for linger or new rows.
            if !is_stale {
                let timed_out = tokio::select! {
                    _ = notify.notified() => false,
                    _ = tokio::time::sleep(linger) => true,
                };
                if !timed_out {
                    continue;
                }
                // Linger timeout: re-check staleness (a new row may have arrived).
                let is_stale = match weak.upgrade() {
                    None => break,
                    Some(core) => {
                        let stale = core.buffer.lock().is_stale();
                        drop(core);
                        stale
                    }
                };
                if !is_stale {
                    continue;
                }
                // Fall through to flush.
            }

            // Re-upgrade for the flush; holding the strong reference
            // during `flush().await` is fine.
            let Some(core) = weak.upgrade() else {
                break;
            };

            // Flush stale batch.
            let result = core.flush().await;

            // Logging with rate limiting. The backoff fast-fail is not a
            // new failure (its cause was already logged), so it is never
            // logged here.
            match result {
                Ok(()) => {
                    let mut last_error = core.last_linger_error.lock();
                    let mut last_error_time = core.last_linger_error_time.lock();
                    // Successful flush after previous failure: log recovery once.
                    if last_error.is_some() {
                        tracing::info!("mysql linger flush recovered");
                        *last_error = None;
                        *last_error_time = None;
                    }
                }
                Err(e) => {
                    let error_text = e.to_string();
                    if error_text.contains("sink backing off after errors") {
                        continue;
                    }
                    let mut last_error = core.last_linger_error.lock();
                    let mut last_error_time = core.last_linger_error_time.lock();
                    let now = Instant::now();
                    let should_log = last_error
                        .as_ref()
                        .map(|last| last != &error_text)
                        .unwrap_or(true)
                        || last_error_time
                            .map(|t| now.saturating_duration_since(t) > ERROR_LOG_THROTTLE)
                            .unwrap_or(true);

                    if should_log {
                        tracing::warn!("mysql linger flush failed: {error_text}");
                        *last_error = Some(error_text);
                        *last_error_time = Some(now);
                    }
                    // Backoff is already recorded by flush(); the loop will
                    // pick it up on the next iteration and wait.
                }
            }
        }
    });
}

#[async_trait]
impl Sink for MySqlSink {
    async fn send(&self, topic: &Topic, payload: &Bytes, qos: QoS) -> super::Result<()> {
        if self.buffer_row(topic, payload, qos) {
            self.flush().await?;
        }
        Ok(())
    }

    /// Buffer every row, then flush once: one batch means one driver
    /// call, so `batch_size: 2` sends two rows together.
    async fn send_batch(&self, items: &[SinkBatchItem]) -> super::Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        for item in items {
            self.buffer_row(&item.topic, &item.payload, item.qos);
        }
        self.flush().await
    }

    fn kind(&self) -> &'static str {
        "mysql"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> MySqlSinkConfig {
        MySqlSinkConfig {
            connection_url: "mysql://user:pass@127.0.0.1:3306/db".to_string(),
            sql_template:
                "INSERT INTO sensor_data (topic, qos, payload, recorded_at) VALUES (?, ?, ?, NOW())"
                    .to_string(),
            pool_size: 2,
            batch_size: 100,
            batch_timeout_ms: 50,
        }
    }

    #[test]
    fn test_config_validation() {
        let mut config = test_config();
        assert!(config.validate().is_ok());

        config.connection_url.clear();
        assert!(config.validate().is_err());
        config.connection_url = "mysql://h/db".to_string();

        config.sql_template = "INSERT INTO t VALUES (?, ?)".to_string();
        assert!(config.validate().is_err());

        config.pool_size = 0;
        assert!(config.validate().is_err());
        config.pool_size = 2;

        config.batch_size = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_placeholder_counting() {
        assert_eq!(count_placeholders("VALUES (?, ?, ?)"), 3);
        assert_eq!(count_placeholders("no markers"), 0);
        // Markers inside strings and comments do not count.
        assert_eq!(count_placeholders("VALUES (?) -- what (?)"), 1);
        assert_eq!(count_placeholders("VALUES ('a?b', ?)"), 1);
        assert_eq!(count_placeholders("VALUES (/* ? */ ?)"), 1);
        assert_eq!(count_placeholders("VALUES (`weird?col`, ?)"), 1);
        assert_eq!(count_placeholders("VALUES (# ?)"), 0);
    }

    #[tokio::test]
    async fn test_send_batch_delivers_two_rows_in_one_driver_call() {
        // `batch_size: 2` sends two rows in one driver call: the batch
        // buffers both rows, then flushes once.
        let transport = Arc::new(MemoryMySqlTransport::new());
        let mut config = test_config();
        config.batch_size = 1;
        config.batch_timeout_ms = 0;
        let sink = MySqlSink::new(config, transport.clone()).expect("valid sink");
        let first = Topic::new("sensors/temp").unwrap();
        let second = Topic::new("sensors/temp").unwrap();
        sink.send_batch(&[
            SinkBatchItem {
                topic: first,
                payload: Bytes::from_static(br#"{ "v": 1 }"#),
                qos: QoS::AtLeastOnce,
            },
            SinkBatchItem {
                topic: second,
                payload: Bytes::from_static(br#"{ "v": 2 }"#),
                qos: QoS::AtLeastOnce,
            },
        ])
        .await
        .expect("batch sends");
        assert_eq!(transport.calls(), 1);
        assert_eq!(sink.sent_batches(), 1);
        let batches = transport.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].rows.len(), 2);
    }

    #[tokio::test]
    async fn test_stale_rows_flush_without_new_events() {
        // One buffered row flushes on its own once it outlives the
        // linger window; no further event is needed.
        let transport = Arc::new(MemoryMySqlTransport::new());
        let mut config = test_config();
        config.batch_size = 100;
        config.batch_timeout_ms = 20;
        let sink = MySqlSink::new(config, transport.clone()).expect("valid sink");
        let topic = Topic::new("sensors/temp").unwrap();
        sink.send(
            &topic,
            &Bytes::from_static(br#"{ "v": 1 }"#),
            QoS::AtMostOnce,
        )
        .await
        .expect("row buffers");
        assert_eq!(sink.buffered_rows(), 1);
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(sink.sent_batches(), 1);
        assert_eq!(sink.buffered_rows(), 0);
        let batches = transport.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].rows.len(), 1);
    }

    /// Test transport: the first call processes `first_processed` rows
    /// then fails with a connection error; later calls succeed fully.
    struct PartialMySqlTransport {
        first_processed: usize,
        batches: parking_lot::Mutex<Vec<MySqlBatch>>,
    }

    impl PartialMySqlTransport {
        fn new(first_processed: usize) -> Self {
            Self {
                first_processed,
                batches: parking_lot::Mutex::new(Vec::new()),
            }
        }

        fn batches(&self) -> Vec<MySqlBatch> {
            self.batches.lock().clone()
        }
    }

    #[async_trait]
    impl MySqlTransport for PartialMySqlTransport {
        async fn execute_batch(&self, batch: &MySqlBatch) -> MySqlBatchOutcome {
            let first = {
                let mut batches = self.batches.lock();
                let first = batches.is_empty();
                batches.push(batch.clone());
                first
            };
            if first {
                return MySqlBatchOutcome {
                    processed: self.first_processed,
                    rejected: Vec::new(),
                    error: Some(ConnectorError::Connection(
                        "mock partial failure".to_string(),
                    )),
                };
            }
            MySqlBatchOutcome {
                processed: batch.rows.len(),
                rejected: Vec::new(),
                error: None,
            }
        }
    }

    #[tokio::test]
    async fn test_transport_failure_retries_only_unprocessed_rows() {
        let transport = Arc::new(PartialMySqlTransport::new(2));
        let mut config = test_config();
        config.batch_size = 4;
        config.batch_timeout_ms = 60_000; // keep the linger task out of the way
        let sink = MySqlSink::new(config, transport.clone()).expect("valid sink");
        let topic = Topic::new("sensors/temp").unwrap();
        for i in 0..3 {
            sink.send(&topic, &Bytes::from(format!("row-{i}")), QoS::AtLeastOnce)
                .await
                .expect("rows buffer");
        }
        // The fourth row fills the batch; the transport fails after
        // processing two of the four rows.
        let err = sink
            .send(&topic, &Bytes::from_static(b"row-3"), QoS::AtLeastOnce)
            .await
            .expect_err("partial failure surfaces");
        assert!(matches!(err, ConnectorError::Connection(_)));
        assert_eq!(sink.inserted_rows(), 2);
        assert_eq!(sink.buffered_rows(), 2);
        // During backoff, flush fails fast without touching the transport.
        assert!(sink.flush().await.is_err());
        assert_eq!(transport.batches().len(), 1);
        // After the backoff window, the retry carries exactly rows 3 and
        // 4, in order; the first two rows are never re-sent.
        tokio::time::sleep(Duration::from_millis(2_100)).await;
        sink.flush().await.expect("retry succeeds");
        assert_eq!(sink.inserted_rows(), 4);
        assert_eq!(sink.buffered_rows(), 0);
        let batches = transport.batches();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[1].rows.len(), 2);
        assert_eq!(batches[1].rows[0][2], b"row-2");
        assert_eq!(batches[1].rows[1][2], b"row-3");
    }

    /// A test transport that always fails with the given message.
    struct AlwaysFailTransport {
        message: String,
        calls: AtomicU64,
    }

    impl AlwaysFailTransport {
        fn new(message: String) -> Self {
            Self {
                message,
                calls: AtomicU64::new(0),
            }
        }

        fn calls(&self) -> u64 {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl MySqlTransport for AlwaysFailTransport {
        async fn execute_batch(&self, _batch: &MySqlBatch) -> MySqlBatchOutcome {
            self.calls.fetch_add(1, Ordering::SeqCst);
            MySqlBatchOutcome {
                processed: 0,
                rejected: Vec::new(),
                error: Some(ConnectorError::Connection(self.message.clone())),
            }
        }
    }

    /// A test transport that succeeds on the first call, then fails once,
    /// then succeeds again (for recovery test).
    struct FailOnceTransport {
        fail_on_call: u64,
        calls: AtomicU64,
    }

    impl FailOnceTransport {
        fn new(fail_on_call: u64) -> Self {
            Self {
                fail_on_call,
                calls: AtomicU64::new(0),
            }
        }

        fn calls(&self) -> u64 {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl MySqlTransport for FailOnceTransport {
        async fn execute_batch(&self, batch: &MySqlBatch) -> MySqlBatchOutcome {
            let call_num = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if call_num == self.fail_on_call {
                MySqlBatchOutcome {
                    processed: 0,
                    rejected: Vec::new(),
                    error: Some(ConnectorError::Connection(
                        "mock transient failure".to_string(),
                    )),
                }
            } else {
                MySqlBatchOutcome {
                    processed: batch.rows.len(),
                    rejected: Vec::new(),
                    error: None,
                }
            }
        }
    }

    /// A test transport that records every flushed batch.
    struct RecordingTransport {
        batches: parking_lot::Mutex<Vec<MySqlBatch>>,
    }

    impl RecordingTransport {
        fn new() -> Self {
            Self {
                batches: parking_lot::Mutex::new(Vec::new()),
            }
        }

        fn batches(&self) -> Vec<MySqlBatch> {
            self.batches.lock().clone()
        }

        fn calls(&self) -> u64 {
            self.batches.lock().len() as u64
        }
    }

    #[async_trait]
    impl MySqlTransport for RecordingTransport {
        async fn execute_batch(&self, batch: &MySqlBatch) -> MySqlBatchOutcome {
            self.batches.lock().push(batch.clone());
            MySqlBatchOutcome {
                processed: batch.rows.len(),
                rejected: Vec::new(),
                error: None,
            }
        }
    }

    /// Simple test subscriber that captures log lines.
    // `set_default` installs a thread-local subscriber, but tracing caches
    // each callsite's interest globally: a parallel test reaching the same
    // `warn!`/`info!` callsite first with no interested subscriber can cache
    // it as disabled. `register_callsite` returning `sometimes` forces a
    // per-event `enabled()` check, and each test calls
    // `rebuild_interest_cache()` after `set_default` so an already-cached
    // "never" is recomputed with the capture subscriber present.
    struct TestSubscriber {
        logs: parking_lot::Mutex<Vec<String>>,
    }

    impl TestSubscriber {
        fn new() -> Self {
            Self {
                logs: parking_lot::Mutex::new(Vec::new()),
            }
        }

        fn count_contains(&self, needle: &str) -> usize {
            self.logs
                .lock()
                .iter()
                .filter(|l| l.contains(needle))
                .count()
        }
    }

    impl tracing::Subscriber for TestSubscriber {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn register_callsite(
            &self,
            _meta: &'static tracing::Metadata<'static>,
        ) -> tracing::subscriber::Interest {
            tracing::subscriber::Interest::sometimes()
        }

        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            let mut message = String::new();
            let mut visitor = TestVisitor(&mut message);
            event.record(&mut visitor);
            let metadata = event.metadata();
            let level = *metadata.level();
            let target = metadata.target();
            let log_line = format!("{} {}: {}", level, target, message);
            self.logs.lock().push(log_line);
        }

        fn enter(&self, _id: &tracing::span::Id) {}

        fn exit(&self, _id: &tracing::span::Id) {}
    }

    struct TestVisitor<'a>(&'a mut String);

    impl tracing::field::Visit for TestVisitor<'_> {
        fn record_debug(&mut self, _field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            // Formatted event messages (e.g. `tracing::warn!("... {x}")`)
            // arrive here as `fmt::Arguments`.
            self.0.push_str(&format!("{value:?}"));
        }

        fn record_str(&mut self, _field: &tracing::field::Field, value: &str) {
            self.0.push_str(value);
        }
    }

    #[tokio::test]
    async fn test_linger_flush_failure_is_logged_once_per_backoff() {
        let subscriber = Arc::new(TestSubscriber::new());
        let _guard = tracing::subscriber::set_default(subscriber.clone());
        tracing::callsite::rebuild_interest_cache();

        let transport = Arc::new(AlwaysFailTransport::new(
            "mock connection error".to_string(),
        ));
        let mut config = test_config();
        config.batch_size = 100;
        config.batch_timeout_ms = 50;
        let sink = MySqlSink::new(config, transport.clone()).expect("valid sink");
        let topic = Topic::new("sensors/temp").unwrap();

        // Send one row (fewer than batch_size, so only linger flushes it)
        sink.send(&topic, &Bytes::from_static(b"row-1"), QoS::AtMostOnce)
            .await
            .expect("row buffers");
        assert_eq!(sink.buffered_rows(), 1);

        // Wait through the first 2s backoff window: the linger task keeps
        // polling during backoff but must not reflood the log.
        tokio::time::sleep(Duration::from_secs(3)).await;

        assert!(
            transport.calls() >= 1,
            "the linger task attempted at least one flush"
        );
        assert_eq!(
            subscriber.count_contains("mysql linger flush failed"),
            1,
            "the failure is logged exactly once, not on every tick"
        );

        // The backoff fast-fail should NOT be logged on every tick
        let backoff_logs = subscriber.count_contains("sink backing off after errors");
        assert_eq!(
            backoff_logs, 0,
            "backoff fast-fail should not be logged by linger task"
        );

        // Clean up
        drop(sink);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_linger_flush_logs_recovery_on_success() {
        let subscriber = Arc::new(TestSubscriber::new());
        let _guard = tracing::subscriber::set_default(subscriber.clone());
        tracing::callsite::rebuild_interest_cache();

        // The first linger flush fails; the retry after the 2s backoff
        // window succeeds.
        let transport = Arc::new(FailOnceTransport::new(1));
        let mut config = test_config();
        config.batch_size = 100;
        config.batch_timeout_ms = 50;
        let sink = MySqlSink::new(config, transport.clone()).expect("valid sink");
        let topic = Topic::new("sensors/temp").unwrap();

        sink.send(&topic, &Bytes::from_static(b"row-1"), QoS::AtMostOnce)
            .await
            .expect("row buffers");

        tokio::time::sleep(Duration::from_secs(3)).await;

        assert_eq!(transport.calls(), 2, "one failed flush plus one retry");
        assert_eq!(sink.buffered_rows(), 0);
        assert_eq!(sink.sent_batches(), 1);
        assert_eq!(
            subscriber.count_contains("mysql linger flush failed"),
            1,
            "the failure is logged once"
        );
        assert_eq!(
            subscriber.count_contains("mysql linger flush recovered"),
            1,
            "recovery is logged once at info"
        );

        drop(sink);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    #[tokio::test]
    async fn test_linger_timer_idle_when_buffer_empty() {
        let transport = Arc::new(RecordingTransport::new());
        let mut config = test_config();
        config.batch_size = 100;
        config.batch_timeout_ms = 50;
        let sink = MySqlSink::new(config, transport.clone()).expect("valid sink");
        let topic = Topic::new("sensors/temp").unwrap();

        // Send one row - it will be flushed by linger timer
        sink.send(&topic, &Bytes::from_static(b"row-1"), QoS::AtMostOnce)
            .await
            .expect("row buffers");
        assert_eq!(sink.buffered_rows(), 1);

        // Wait for linger flush
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Row should be flushed
        assert_eq!(sink.buffered_rows(), 0);
        assert_eq!(sink.sent_batches(), 1);
        assert_eq!(transport.calls(), 1);

        // Now wait longer - the timer should be idle, no more flushes
        let calls_after_idle = transport.calls();
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            transport.calls(),
            calls_after_idle,
            "timer should be idle when buffer empty"
        );

        // Send another row - timer should re-arm and flush
        sink.send(&topic, &Bytes::from_static(b"row-2"), QoS::AtMostOnce)
            .await
            .expect("row buffers");
        assert_eq!(sink.buffered_rows(), 1);

        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(sink.buffered_rows(), 0);
        assert_eq!(sink.sent_batches(), 2);
        assert_eq!(transport.calls(), 2);

        drop(sink);
    }

    #[tokio::test]
    async fn test_row_arriving_while_timer_stops_is_not_stranded() {
        // Many short send/flush cycles with rows arriving around the
        // linger deadline; every row is eventually flushed.
        let transport = Arc::new(RecordingTransport::new());
        let mut config = test_config();
        config.batch_size = 100;
        config.batch_timeout_ms = 20;
        let sink = MySqlSink::new(config, transport.clone()).expect("valid sink");
        let topic = Topic::new("sensors/temp").unwrap();

        let mut expected_rows = 0;

        for i in 0..50 {
            sink.send(&topic, &Bytes::from(format!("row-{i}")), QoS::AtMostOnce)
                .await
                .expect("send");
            expected_rows += 1;

            // Random small delay around the linger timeout to create races
            if i % 7 == 0 {
                tokio::time::sleep(Duration::from_millis(15)).await; // less than linger
            } else if i % 11 == 0 {
                tokio::time::sleep(Duration::from_millis(25)).await; // more than linger
            }
        }

        // Final wait to ensure all lingering rows flush
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(sink.buffered_rows(), 0, "no rows should be stranded");
        assert_eq!(
            sink.sent_batches(),
            transport.calls(),
            "sent_batches should match transport calls"
        );

        // Total rows sent should equal total rows in all batches
        let total_rows: usize = transport.batches().iter().map(|b| b.rows.len()).sum();
        assert_eq!(
            total_rows, expected_rows,
            "all rows should be flushed exactly once"
        );

        drop(sink);
    }

    #[tokio::test]
    async fn test_dropped_sink_stops_linger_task_promptly() {
        // With an empty buffer the linger task waits on a timer or
        // notification; it must not pin the sink core while waiting,
        // so dropping the sink frees the core within 200 ms.
        // The probe clones the inner `linger_notify` Arc: while the
        // core is alive the count is 3 (core + task + probe), and once
        // the core is freed it drops to 2 (task + probe).
        let transport = Arc::new(MemoryMySqlTransport::new());
        let mut config = test_config();
        config.batch_size = 100;
        config.batch_timeout_ms = 50;
        let sink = MySqlSink::new(config, transport).expect("valid sink");
        let probe = sink.core.linger_notify.clone();
        assert_eq!(Arc::strong_count(&probe), 3);
        drop(sink);
        let mut freed = false;
        for _ in 0..20 {
            if Arc::strong_count(&probe) == 2 {
                freed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(freed, "dropped sink core was not freed within 200 ms");
    }

    #[test]
    fn test_driver_transport_rejects_bad_config() {
        assert!(DriverMySqlTransport::new("mysql://127.0.0.1:1/db", 0).is_err());
        assert!(DriverMySqlTransport::new("postgres://127.0.0.1:3306/db", 1).is_err());
        assert!(DriverMySqlTransport::new("not a url", 1).is_err());
        assert!(DriverMySqlTransport::new("mysql://u:p@127.0.0.1:1/db", 1).is_ok());
    }

    #[tokio::test]
    async fn test_driver_write_path_through_connector_manager() {
        // Broker path (connect, publish, deliver): ConnectorManager::send ->
        // Sink::send -> flush -> DriverMySqlTransport::execute_batch.
        // Points at an unroutable port so the offline gate stays green
        // while still driving the production driver transport; the
        // connection must fail closed, never grant access.
        use super::super::ConnectorManager;
        let url = "mysql://u:p@127.0.0.1:1/db";
        let transport = Arc::new(DriverMySqlTransport::new(url, 1).expect("driver transport"));
        let config = MySqlSinkConfig {
            connection_url: url.to_string(),
            sql_template: "INSERT INTO t (topic, qos, payload) VALUES (?, ?, ?)".to_string(),
            pool_size: 1,
            batch_size: 1,
            batch_timeout_ms: 60_000, // keep the linger task out of the way
        };
        let sink = Arc::new(MySqlSink::new(config, transport).expect("valid sink"));
        assert_eq!(sink.kind(), "mysql");
        let manager = ConnectorManager::new();
        manager.register("mysql-driver", sink);
        let res = manager
            .send(
                "mysql-driver",
                &Topic::new("sensors/broker").unwrap(),
                &Bytes::from_static(b"{}"),
                QoS::AtLeastOnce,
            )
            .await;
        let err = res.expect_err("unreachable driver must fail closed");
        assert!(
            matches!(err, ConnectorError::Connection(_)),
            "driver error must be a connection failure, got: {err}"
        );
        assert!(
            err.to_string().contains("mysql"),
            "driver error must be observable, got: {err}"
        );
    }

    fn qual_env(name: &str) -> Option<String> {
        std::env::var(name).ok().filter(|v| !v.trim().is_empty())
    }

    fn qual_require(name: &str) -> String {
        qual_env(name).unwrap_or_else(|| {
            panic!(
                "{name} must point at a real MySQL/MariaDB server for qualification; \
                 failing closed instead of passing vacuously"
            )
        })
    }

    fn qual_identifier(name: &str, value: &str) -> String {
        assert!(
            !value.is_empty() && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "qual {name} must match [A-Za-z0-9_]+, got {value:?} (failing closed)"
        );
        value.to_string()
    }

    /// Qualification against a real MySQL / MariaDB server over the
    /// maintained `mysql_async` driver ([`DriverMySqlTransport`]).
    ///
    /// Run with e.g.:
    /// `MYSQL_HOST=127.0.0.1 MYSQL_PORT=3306 MYSQL_DATABASE=qual \
    ///  MYSQL_USER=qual MYSQL_PASSWORD=qualpass1 \
    ///  MYSQL_NATIVE_USER=qual_native MYSQL_TABLE=mysql_qual_b329 \
    ///  cargo test -p broker-connectors --lib mysql::tests::test_qualify_driver_write_path -- --ignored --nocapture --test-threads=1`
    ///
    /// Creates a table with a CHECK constraint, proves a violating row
    /// surfaces errno 3819 (MySQL) / 4025 (MariaDB) on both password
    /// plugins, streams 2000 rows (1000 per plugin) through the broker
    /// ([`crate::ConnectorManager`] -> [`MySqlSink`] on
    /// [`DriverMySqlTransport`]), asserts `SELECT COUNT(*)` returns
    /// 2000, proves a wrong password fails closed, then drops the table
    /// it created.
    #[tokio::test]
    #[ignore = "needs a real MySQL/MariaDB server (see MYSQL_* env)"]
    async fn test_qualify_driver_write_path() {
        use crate::ConnectorManager;
        use mysql_async::prelude::Queryable;

        let host = qual_require("MYSQL_HOST");
        let port: u16 = qual_require("MYSQL_PORT")
            .parse()
            .expect("qual MYSQL_PORT must be a port number");
        let database = qual_identifier("database", &qual_require("MYSQL_DATABASE"));
        let user = qual_require("MYSQL_USER");
        let password = qual_require("MYSQL_PASSWORD");
        let native_user = qual_require("MYSQL_NATIVE_USER");
        let table = qual_identifier("table", &qual_require("MYSQL_TABLE"));
        const ROWS_PER_PLUGIN: u64 = 1000;

        let native_url = format!("mysql://{native_user}:{password}@{host}:{port}/{database}");
        let sha2_url = format!("mysql://{user}:{password}@{host}:{port}/{database}");

        // Direct driver pool for DDL and assertions.
        let ddl_pool = mysql_async::Pool::from_url(native_url.as_str()).expect("qual pool");
        let mut ddl = ddl_pool
            .get_conn()
            .await
            .expect("qual mysql connect (native user)");

        // Server version for the report (connectivity is already proved
        // by the connection above; the version string is context).
        let version: Option<String> = ddl
            .query_first("SELECT VERSION()")
            .await
            .expect("qual server version");
        eprintln!("qual server: version={version:?} host={host}:{port} database={database}");

        ddl.query_drop(format!("DROP TABLE IF EXISTS `{table}`"))
            .await
            .expect("qual drop stale table");
        ddl.query_drop(format!(
            "CREATE TABLE `{table}` (\
             `topic` VARCHAR(256) NOT NULL, \
             `qos` TINYINT NOT NULL, \
             `payload` VARCHAR(256) NOT NULL, \
             CONSTRAINT `{table}_chk` CHECK (CHAR_LENGTH(`payload`) <= 64))"
        ))
        .await
        .expect("qual create table with CHECK");

        let template =
            format!("INSERT INTO `{table}` (`topic`, `qos`, `payload`) VALUES (?, ?, ?)");
        let native_transport =
            Arc::new(DriverMySqlTransport::new(&native_url, 2).expect("qual native transport"));
        let sha2_transport =
            Arc::new(DriverMySqlTransport::new(&sha2_url, 2).expect("qual sha2 transport"));

        // CHECK proof on both password plugins: one violating row per
        // transport must surface errno 3819 (MySQL) or 4025 (MariaDB).
        for (label, transport) in [
            ("native", native_transport.clone()),
            ("sha2", sha2_transport.clone()),
        ] {
            let batch = MySqlBatch {
                sql: template.clone(),
                rows: vec![vec![
                    b"sensors/qual".to_vec(),
                    b"1".to_vec(),
                    vec![b'x'; 200],
                ]],
            };
            let outcome: MySqlBatchOutcome = transport.execute_batch(&batch).await;
            assert!(
                outcome.error.is_none(),
                "qual {label} CHECK probe must not stop the batch: {outcome:?}"
            );
            assert_eq!(outcome.rejected.len(), 1, "qual {label} CHECK probe");
            let (_, message) = &outcome.rejected[0];
            assert!(
                message.contains("3819") || message.contains("4025"),
                "qual {label} CHECK errno 3819/4025 must surface, got {message:?}"
            );
            eprintln!("qual CHECK asserted: plugin={label} message={message:?}");
        }

        // The broker's path: rule actions deliver through the shared
        // connector manager, so the qualification sends through it and
        // never `sink.send` directly.
        let mk_config = |url: &str| MySqlSinkConfig {
            connection_url: url.to_string(),
            sql_template: template.clone(),
            pool_size: 2,
            batch_size: 200,
            batch_timeout_ms: 60_000, // explicit flushes only; keeps the linger task out
        };
        let native_sink = Arc::new(
            MySqlSink::new(mk_config(&native_url), native_transport.clone())
                .expect("qual native sink"),
        );
        let sha2_sink = Arc::new(
            MySqlSink::new(mk_config(&sha2_url), sha2_transport.clone()).expect("qual sha2 sink"),
        );
        assert_eq!(native_sink.kind(), "mysql");
        assert_eq!(sha2_sink.kind(), "mysql");
        let manager = ConnectorManager::new();
        manager.register("qual-mysql-native", native_sink.clone());
        manager.register("qual-mysql-sha2", sha2_sink.clone());

        let topic = Topic::new("sensors/qual").unwrap();
        for seq in 0..ROWS_PER_PLUGIN {
            let payload = Bytes::from(format!(r#"{{"seq":{seq}}}"#));
            manager
                .send("qual-mysql-native", &topic, &payload, QoS::AtLeastOnce)
                .await
                .expect("qual native send");
            manager
                .send("qual-mysql-sha2", &topic, &payload, QoS::AtLeastOnce)
                .await
                .expect("qual sha2 send");
        }
        native_sink.flush().await.expect("qual native flush");
        sha2_sink.flush().await.expect("qual sha2 flush");
        assert_eq!(
            native_sink.inserted_rows(),
            ROWS_PER_PLUGIN,
            "qual native row count"
        );
        assert_eq!(
            sha2_sink.inserted_rows(),
            ROWS_PER_PLUGIN,
            "qual sha2 row count"
        );
        assert_eq!(native_sink.sent_batches(), 5, "qual native batches");
        assert_eq!(sha2_sink.sent_batches(), 5, "qual sha2 batches");
        eprintln!("qual rows sent: native={ROWS_PER_PLUGIN} sha2={ROWS_PER_PLUGIN}");

        // Row count asserted back from the server, not the counters.
        let count_sql = format!("SELECT COUNT(*) FROM `{table}`");
        let count: Option<u64> = ddl
            .query_first(count_sql.as_str())
            .await
            .expect("qual count");
        assert_eq!(
            count.unwrap_or(0),
            2 * ROWS_PER_PLUGIN,
            "qual count mismatch"
        );
        eprintln!(
            "qual rows asserted: count={} table={table}",
            2 * ROWS_PER_PLUGIN
        );

        // Wrong password fails closed: no rows processed, connection error.
        let bad_url = format!("mysql://{native_user}:wrongpass@{host}:{port}/{database}");
        let bad_transport = DriverMySqlTransport::new(&bad_url, 1).expect("qual bad transport");
        let bad_batch = MySqlBatch {
            sql: template.clone(),
            rows: vec![vec![
                b"sensors/qual".to_vec(),
                b"1".to_vec(),
                br#"{"seq":0}"#.to_vec(),
            ]],
        };
        let bad_outcome = bad_transport.execute_batch(&bad_batch).await;
        assert_eq!(
            bad_outcome.processed, 0,
            "qual bad password processes nothing"
        );
        assert!(
            matches!(bad_outcome.error, Some(ConnectorError::Connection(_))),
            "qual bad password must fail closed, got {bad_outcome:?}"
        );
        eprintln!("qual auth asserted: wrong password fails closed");

        let count: Option<u64> = ddl
            .query_first(count_sql.as_str())
            .await
            .expect("qual recount");
        assert_eq!(
            count.unwrap_or(0),
            2 * ROWS_PER_PLUGIN,
            "qual count unchanged"
        );

        // Cleanup: drop the table created for this run (best effort).
        match ddl
            .query_drop(format!("DROP TABLE IF EXISTS `{table}`"))
            .await
        {
            Ok(()) => eprintln!("qual cleanup: dropped table {table}"),
            Err(e) => {
                eprintln!("qual cleanup FAILED to drop {table} (tolerated): {e}");
            }
        }
        drop(ddl);
        ddl_pool.disconnect().await.expect("qual disconnect");
        eprintln!(
            "qual done: rows={} table={table} cleaned table",
            2 * ROWS_PER_PLUGIN
        );
    }
}
