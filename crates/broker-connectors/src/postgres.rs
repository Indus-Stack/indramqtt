//! PostgreSQL sink over the native wire protocol.
//!
//! MQTT events become parameterized `INSERT` rows: `$1` is the topic,
//! `$2` the QoS level, `$3` the raw payload (cast server-side, e.g.
//! `$3::jsonb`); any other `$N` marker is rejected so statements can
//! never address unbound parameters. The production write path runs on
//! the maintained `tokio-postgres` driver with a `rustls` TLS connector
//! ([`DriverPgTransport`]): values travel bound out-of-band through the
//! driver's extended-protocol path, so SQL injection is structurally
//! impossible. The [`PgTransport`] boundary keeps unit tests
//! broker-free ([`MemoryPgTransport`]).

use super::{ConnectorError, Result, Sink};
use async_trait::async_trait;
use broker_protocol::{QoS, Topic};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;

fn default_pool_size() -> usize {
    10
}

fn default_batch_size() -> usize {
    100
}

fn default_batch_timeout_ms() -> u64 {
    50
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgreSqlSinkConfig {
    pub connection_url: String,
    pub sql_template: String,
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_batch_timeout_ms")]
    pub batch_timeout_ms: u64,
}

impl PostgreSqlSinkConfig {
    pub fn validate(&self) -> Result<()> {
        if self.connection_url.trim().is_empty() {
            return Err(ConnectorError::Dispatch(
                "postgres connection_url must not be empty".to_string(),
            ));
        }
        if self.sql_template.trim().is_empty() {
            return Err(ConnectorError::Dispatch(
                "postgres sql_template must not be empty".to_string(),
            ));
        }
        validate_placeholders(&self.sql_template)?;
        if self.pool_size == 0 {
            return Err(ConnectorError::Dispatch(
                "postgres pool_size must be >= 1".to_string(),
            ));
        }
        if self.batch_size == 0 {
            return Err(ConnectorError::Dispatch(
                "postgres batch_size must be >= 1".to_string(),
            ));
        }
        Ok(())
    }
}

/// Scan a template for `$N` markers outside strings, quoted identifiers,
/// and comments. Only `$1..=$3` exist (`$1` topic, `$2` QoS, `$3`
/// payload); anything else is rejected so unbound parameters can never
/// reach the server.
/// All `$n` markers referenced outside strings/comments, in order.
/// Shared with the TimescaleDB sink (same wire protocol, wider shape).
pub fn referenced_params(template: &str) -> Result<Vec<u32>> {
    let chars: Vec<char> = template.chars().collect();
    let n = chars.len();
    let mut found = Vec::new();
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
                if quote == '\'' && i + 1 < n && chars[i + 1] == '\'' {
                    i += 2; // escaped quote
                } else {
                    in_string = None;
                    i += 1;
                }
            } else {
                i += 1;
            }
            continue;
        }
        match c {
            '\'' | '"' => {
                in_string = Some(c);
                i += 1;
            }
            '-' if i + 1 < n && chars[i + 1] == '-' => {
                in_line_comment = true;
                i += 2;
            }
            '/' if i + 1 < n && chars[i + 1] == '*' => {
                in_block_comment = true;
                i += 2;
            }
            '$' => {
                let mut j = i + 1;
                while j < n && chars[j].is_ascii_digit() {
                    j += 1;
                }
                if j == i + 1 {
                    i += 1; // lone `$`: not a marker (e.g. dollar quoting edge)
                    continue;
                }
                let number: u32 =
                    chars[i + 1..j]
                        .iter()
                        .collect::<String>()
                        .parse()
                        .map_err(|_| {
                            ConnectorError::Dispatch(
                                "postgres parameter number overflow".to_string(),
                            )
                        })?;
                found.push(number);
                i = j;
            }
            _ => {
                i += 1;
            }
        }
    }
    Ok(found)
}

fn validate_placeholders(template: &str) -> Result<()> {
    for number in referenced_params(template)? {
        if number == 0 || number > 3 {
            return Err(ConnectorError::Dispatch(format!(
                "postgres only $1 (topic), $2 (qos) and $3 (payload) exist, got ${number}"
            )));
        }
    }
    Ok(())
}

/// One flushed batch: the statement plus one 3-column row per event.
#[derive(Debug, Clone, Default)]
pub struct PgBatch {
    pub sql: String,
    pub rows: Vec<Vec<Vec<u8>>>,
}

#[async_trait]
pub trait PgTransport: Send + Sync {
    async fn execute_batch(&self, batch: &PgBatch) -> Result<()>;
}

/// In-memory transport recording every flushed batch (tests, dry runs).
#[derive(Debug, Default)]
pub struct MemoryPgTransport {
    batches: parking_lot::Mutex<Vec<PgBatch>>,
    failures_left: parking_lot::Mutex<usize>,
    calls: AtomicU64,
    fail_dispatch: parking_lot::Mutex<Vec<(Vec<u8>, String)>>,
    fail_connection: parking_lot::Mutex<Vec<(Vec<u8>, String)>>,
}

impl MemoryPgTransport {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fail the next `n` executions with a transport error (backoff tests).
    pub fn fail_next(&self, n: usize) {
        *self.failures_left.lock() = n;
    }

    /// Fail any batch holding a row column that contains `needle` with a
    /// server (`Dispatch`) error carrying `message` (bad-row tests).
    pub fn fail_batches_containing(&self, needle: &[u8], message: impl Into<String>) {
        self.fail_dispatch
            .lock()
            .push((needle.to_vec(), message.into()));
    }

    /// Fail any batch holding a row column that contains `needle` with an
    /// I/O (`Connection`) error carrying `message` (retry-restore tests).
    pub fn fail_connection_batches_containing(&self, needle: &[u8], message: impl Into<String>) {
        self.fail_connection
            .lock()
            .push((needle.to_vec(), message.into()));
    }

    pub fn batches(&self) -> Vec<PgBatch> {
        self.batches.lock().clone()
    }

    pub fn calls(&self) -> u64 {
        self.calls.load(Ordering::SeqCst)
    }
}

fn batch_contains(batch: &PgBatch, needle: &[u8]) -> bool {
    if needle.is_empty() {
        return false;
    }
    batch.rows.iter().any(|row| {
        row.iter().any(|column| {
            column.len() >= needle.len()
                && column.windows(needle.len()).any(|window| window == needle)
        })
    })
}

#[async_trait]
impl PgTransport for MemoryPgTransport {
    async fn execute_batch(&self, batch: &PgBatch) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        for (needle, message) in self.fail_dispatch.lock().iter() {
            if batch_contains(batch, needle) {
                return Err(ConnectorError::Dispatch(message.clone()));
            }
        }
        for (needle, message) in self.fail_connection.lock().iter() {
            if batch_contains(batch, needle) {
                return Err(ConnectorError::Connection(message.clone()));
            }
        }
        let mut failures = self.failures_left.lock();
        if *failures > 0 {
            *failures -= 1;
            return Err(ConnectorError::Connection(
                "mock transport down".to_string(),
            ));
        }
        self.batches.lock().push(batch.clone());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL wire protocol (startup, auth, extended query).
// ---------------------------------------------------------------------------

/// True when `message` carries a data-rejection SQLSTATE: class `22` or
/// `23`, or `42703` (undefined column) / `42804` (datatype mismatch),
/// matched as a whole 5-character token (bounded by a non-alphanumeric
/// character or the string edge).
fn is_pg_data_error(message: &str) -> bool {
    let bytes = message.as_bytes();
    if bytes.len() < 5 {
        return false;
    }
    for start in 0..=(bytes.len() - 5) {
        let window = &bytes[start..start + 5];
        if !window.iter().all(|b| b.is_ascii_alphanumeric()) {
            continue;
        }
        let before_ok = start == 0 || !bytes[start - 1].is_ascii_alphanumeric();
        let after_ok = start + 5 == bytes.len() || !bytes[start + 5].is_ascii_alphanumeric();
        if !(before_ok && after_ok) {
            continue;
        }
        if window.starts_with(b"22")
            || window.starts_with(b"23")
            || window == b"42703"
            || window == b"42804"
        {
            return true;
        }
    }
    false
}

/// Split `postgresql://[user[:password]@]host[:port][/dbname][?params]`
/// into (host, port, user, password, database, use_tls). Both the
/// `postgresql://` and the `postgres://` spellings are accepted so
/// stored configuration written with either spelling keeps working.
fn parse_driver_endpoint(url: &str) -> Result<(String, u16, String, String, String, bool)> {
    let rest = url
        .strip_prefix("postgresql://")
        .or_else(|| url.strip_prefix("postgres://"))
        .ok_or_else(|| {
            ConnectorError::Dispatch(format!(
                "postgres url must start with postgresql://: {url:?}"
            ))
        })?;
    let (authority_path, query) = match rest.split_once('?') {
        Some((left, query)) => (left, query),
        None => (rest, ""),
    };
    // Plaintext unless the stored URL explicitly asks for TLS. The
    // legacy transport is cleartext-only, so stored configuration
    // carries no TLS expectation; an explicit `sslmode=require` (or
    // `verify-ca` / `verify-full`) opts into `rustls` instead of
    // failing against a TLS-demanding server.
    // TODO(parity): should a missing `sslmode` fail closed to TLS
    // instead of plaintext? Plaintext preserves stored-config
    // compatibility; a server that demands TLS still refuses the
    // handshake, so nothing is silently downgraded.
    let mut use_tls = false;
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if key.eq_ignore_ascii_case("sslmode")
            && (value.eq_ignore_ascii_case("require")
                || value.eq_ignore_ascii_case("verify-ca")
                || value.eq_ignore_ascii_case("verify-full"))
        {
            use_tls = true;
        }
    }
    let (authority, dbname) = match authority_path.split_once('/') {
        Some((authority, dbname)) => (authority, dbname),
        None => (authority_path, ""),
    };
    let (credentials, hostport) = match authority.rsplit_once('@') {
        Some((credentials, hostport)) => (credentials, hostport),
        None => ("", authority),
    };
    let (user, password) = match credentials.split_once(':') {
        Some((user, pass)) => (user.to_string(), pass.to_string()),
        None => (credentials.to_string(), String::new()),
    };
    let user = if user.is_empty() {
        "postgres".to_string()
    } else {
        user
    };
    let (host, port) = match hostport.rsplit_once(':') {
        Some((host, port)) => (
            host.to_string(),
            port.parse::<u16>()
                .map_err(|_| ConnectorError::Dispatch(format!("postgres bad port in {url:?}")))?,
        ),
        None => (hostport.to_string(), 5432),
    };
    if host.is_empty() {
        return Err(ConnectorError::Dispatch(format!(
            "postgres url needs a host: {url:?}"
        )));
    }
    let database = if dbname.is_empty() {
        user.clone()
    } else {
        dbname.to_string()
    };
    Ok((host, port, user, password, database, use_tls))
}

/// Checkout timeout for one driver connection: 5 s bounds the handshake.
const PG_DRIVER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Per-statement timeout: 10 s bounds one execute call so a stalled
/// server cannot hold a pooled connection indefinitely.
const PG_DRIVER_STATEMENT_TIMEOUT: Duration = Duration::from_secs(10);

/// Build a `tokio-postgres` connection config from parsed endpoint
/// fields. Field-by-field construction (never the driver's own URL
/// parser) so the default port stays 5432 and `sslmode` handling stays
/// in [`parse_driver_endpoint`].
fn pg_connect_config(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    database: &str,
) -> tokio_postgres::Config {
    // TODO(parity): the password travels percent-encoded in a URL but
    // is used verbatim here; does any stored configuration rely on
    // encoded characters that must be decoded first?
    let mut cfg = tokio_postgres::Config::new();
    cfg.host(host);
    cfg.port(port);
    cfg.dbname(database);
    cfg.user(user);
    if !password.is_empty() {
        cfg.password(password);
    }
    cfg.connect_timeout(PG_DRIVER_CONNECT_TIMEOUT);
    cfg
}

/// True for SQLSTATEs that mean the session is gone or must retry, not
/// that a row was bad: class `08` (connection exception), `57P01`
/// (admin shutdown) / `57P02` (crash shutdown) / `57P03` (cannot
/// connect now), `40001` (serialization failure) and `40P01`
/// (deadlock). The reason is written here because the sink treats the
/// two classes oppositely: `Connection` restores the batch and backs
/// off, `Dispatch` drops the row and counts it rejected.
fn is_pg_retryable_sqlstate(code: &str) -> bool {
    code.starts_with("08") || matches!(code, "57P01" | "57P02" | "57P03" | "40001" | "40P01")
}

/// Map a `tokio-postgres` error onto [`ConnectorError`], preserving the
/// SQLSTATE text so [`is_pg_data_error`] keeps classifying rejected
/// rows (class `22`/`23`, `42703`, `42804`) as `Dispatch` for the
/// sink's per-row isolation.
pub fn map_pg_driver_error(err: &tokio_postgres::Error) -> ConnectorError {
    let mut detail = err.to_string();
    let mut code_text: Option<String> = None;
    if let Some(db) = err.as_db_error() {
        let code_str = db.code().code().to_string();
        code_text = Some(code_str.clone());
        let message = db.message();
        detail = format!("{code_str}: {message}");
    }
    if let Some(code) = code_text {
        if is_pg_retryable_sqlstate(&code) {
            return ConnectorError::Connection(format!("postgres driver error: {detail}"));
        }
        return ConnectorError::Dispatch(format!("postgres driver error: {detail}"));
    }
    ConnectorError::Connection(format!("postgres driver error: {detail}"))
}

fn install_pg_tls_provider() {
    // The TLS connector below needs a process-default crypto provider.
    // The workspace enables exactly one rustls provider (`aws-lc-rs` via
    // the default features), so installing it explicitly is a no-op when
    // already installed and keeps the call safe under feature
    // unification.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

/// Root store for driver TLS: the OS system trust store. Fail closed
/// when nothing trusted anything: no bundled fallback, so an
/// unreachable system store denies the connection instead of silently
/// trusting a stale list.
fn pg_root_store() -> Result<rustls::RootCertStore> {
    let mut store = rustls::RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    for cert in native.certs {
        let _ = store.add(cert);
    }
    if !native.errors.is_empty() {
        tracing::warn!(
            errors = native.errors.len(),
            "postgres system trust store reported load errors"
        );
    }
    if store.is_empty() {
        return Err(ConnectorError::Connection(
            "postgres TLS trust store is empty: system store unreadable".into(),
        ));
    }
    Ok(store)
}

/// TLS connector for the `tokio-postgres` driver built from the system
/// trust store.
fn pg_tls_connector() -> Result<tokio_postgres_rustls::MakeRustlsConnect> {
    install_pg_tls_provider();
    let roots = pg_root_store()?;
    let tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(tokio_postgres_rustls::MakeRustlsConnect::new(tls))
}

/// Decode one buffered row column as TEXT for the driver. The payload
/// travels as TEXT on the legacy path, so the driver binds `String`
/// (TEXT) to match; a column that is not valid UTF-8 surfaces SQLSTATE
/// `22021` (character not in repertoire, class `22`) so the sink
/// counts the row rejected instead of restoring it forever.
// TODO(parity): binary (non-UTF-8) payloads cannot travel as TEXT
// parameters; is BYTEA binding (with a server-side cast) the correct
// mapping, or must such rows stay rejected as here?
fn pg_text_column(row: &[Vec<u8>], index: usize, name: &str) -> Result<String> {
    let bytes = row.get(index).cloned().unwrap_or_default();
    String::from_utf8(bytes).map_err(|_| {
        ConnectorError::Dispatch(format!(
            "postgres driver error: 22021: {name} is not valid UTF-8"
        ))
    })
}

/// QoS bound parameter: the sink buffers QoS as its decimal text
/// (`"0"`/`"1"`/`"2"`), but the server may expect an integer (`$2::int`,
/// integer column) or text. The driver's `String` binding only accepts
/// TEXT-like server types, so binding it directly fails client-side with
/// `error serializing parameter 1` before the statement ever reaches the
/// server (no SQLSTATE, mapped to `Connection`). This wrapper accepts
/// both families and encodes per the server-inferred type: big-endian
/// binary for INT2/INT4/INT8/OID, raw text otherwise.
#[derive(Debug)]
struct PgQosParam {
    text: String,
    value: i32,
}

impl tokio_postgres::types::ToSql for PgQosParam {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    {
        use tokio_postgres::types::{IsNull, Type};
        match *ty {
            Type::INT2 => {
                out.extend_from_slice(&(self.value as i16).to_be_bytes());
                Ok(IsNull::No)
            }
            Type::INT4 => {
                out.extend_from_slice(&self.value.to_be_bytes());
                Ok(IsNull::No)
            }
            Type::INT8 => {
                out.extend_from_slice(&(self.value as i64).to_be_bytes());
                Ok(IsNull::No)
            }
            Type::OID => {
                out.extend_from_slice(&(self.value as u32).to_be_bytes());
                Ok(IsNull::No)
            }
            _ => {
                out.extend_from_slice(self.text.as_bytes());
                Ok(IsNull::No)
            }
        }
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        use tokio_postgres::types::Type;
        matches!(
            *ty,
            Type::INT2
                | Type::INT4
                | Type::INT8
                | Type::OID
                | Type::VARCHAR
                | Type::TEXT
                | Type::BPCHAR
                | Type::NAME
                | Type::UNKNOWN
        ) || matches!(ty.name(), "citext")
    }

    fn to_sql_checked(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    {
        if !<Self as tokio_postgres::types::ToSql>::accepts(ty) {
            return Err(format!("postgres qos parameter has no binding for {ty}").into());
        }
        <Self as tokio_postgres::types::ToSql>::to_sql(self, ty, out)
    }
}

/// Payload bound parameter: the sink buffers the raw payload bytes, but
/// the server may expect JSON/JSONB (`$3::jsonb`) or text. The driver's
/// `String` binding only accepts TEXT-like server types, so a JSONB
/// target fails client-side with `error serializing parameter 2` before
/// the statement reaches the server. This wrapper accepts both families:
/// JSONB targets get the binary encoding (version byte `1` plus the JSON
/// document), every other target gets the raw text.
#[derive(Debug)]
struct PgPayloadParam(String);

impl tokio_postgres::types::ToSql for PgPayloadParam {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    {
        use tokio_postgres::types::{IsNull, Type};
        match *ty {
            Type::JSONB => {
                out.extend_from_slice(&[1u8]);
                out.extend_from_slice(self.0.as_bytes());
                Ok(IsNull::No)
            }
            _ => {
                out.extend_from_slice(self.0.as_bytes());
                Ok(IsNull::No)
            }
        }
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        use tokio_postgres::types::Type;
        matches!(
            *ty,
            Type::JSON
                | Type::JSONB
                | Type::VARCHAR
                | Type::TEXT
                | Type::BPCHAR
                | Type::NAME
                | Type::UNKNOWN
                | Type::BYTEA
        ) || matches!(ty.name(), "citext")
    }

    fn to_sql_checked(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    {
        if !<Self as tokio_postgres::types::ToSql>::accepts(ty) {
            return Err(format!("postgres payload parameter has no binding for {ty}").into());
        }
        <Self as tokio_postgres::types::ToSql>::to_sql(self, ty, out)
    }
}

/// Production PostgreSQL transport on the maintained `tokio-postgres`
/// driver (MIT) with a `rustls` TLS connector (system trust store) when
/// the URL opts in via `sslmode=require`, plaintext otherwise. Startup,
/// MD5 and SCRAM-SHA-256 authentication and the extended query protocol
/// (Parse/Bind/Describe/Execute/Sync) all run inside the driver;
/// parameters travel bound (`$1` topic, `$2` QoS, `$3` payload), so
/// values stay out-of-band. Production wiring uses this transport.
///
/// Bound: the pool holds at most `pool_size` driver clients plus the
/// sink buffer in front of it; no background queue. `pool_size`
/// carries the configured default 10 (`default_pool_size`): the
/// default is finite because an unbounded pool under fan-in would
/// repeat the multi-GB RSS collapse the v4 benchmark measured on this
/// path.
pub struct DriverPgTransport {
    pg_config: tokio_postgres::Config,
    use_tls: bool,
    pool: Vec<AsyncMutex<Option<tokio_postgres::Client>>>,
    cursor: AtomicU64,
}

/// Connection fields for one PostgreSQL target. The action-owned sink
/// builds these from the connector document, so a password or database
/// with a URL delimiter (`@`, `/`, `?`, `#`) reaches the driver as a
/// value instead of a hand-joined URL.
#[derive(Debug, Clone)]
pub struct PgConnectionSettings {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub use_tls: bool,
}

impl DriverPgTransport {
    pub fn new(url: &str, pool_size: usize) -> Result<Self> {
        if pool_size == 0 {
            return Err(ConnectorError::Dispatch(
                "postgres pool_size must be >= 1".to_string(),
            ));
        }
        let (host, port, user, password, database, use_tls) = parse_driver_endpoint(url)?;
        Self::from_settings(
            PgConnectionSettings {
                host,
                port,
                user,
                password,
                database,
                use_tls,
            },
            pool_size,
        )
    }

    /// Build the pool from explicit connection fields. The driver's
    /// config struct receives each value verbatim, so no value needs URL
    /// escaping and no delimiter in a password or database name can
    /// change the target.
    pub fn from_settings(settings: PgConnectionSettings, pool_size: usize) -> Result<Self> {
        if pool_size == 0 {
            return Err(ConnectorError::Dispatch(
                "postgres pool_size must be >= 1".to_string(),
            ));
        }
        Ok(Self {
            pg_config: pg_connect_config(
                &settings.host,
                settings.port,
                &settings.user,
                &settings.password,
                &settings.database,
            ),
            use_tls: settings.use_tls,
            pool: (0..pool_size).map(|_| AsyncMutex::new(None)).collect(),
            cursor: AtomicU64::new(0),
        })
    }

    /// Build from discrete connection fields instead of a URL. Password
    /// and database travel as values (never formatted into a URL), so a
    /// value with URL delimiters (`@`, `:`, `/`, `?`, `#`) or a quote
    /// reaches the driver verbatim. `use_tls` selects the maintained
    /// rustls connector with the system trust store.
    pub fn from_parts(
        host: &str,
        port: u16,
        user: &str,
        password: &str,
        database: &str,
        pool_size: usize,
        use_tls: bool,
    ) -> Result<Self> {
        if host.trim().is_empty() {
            return Err(ConnectorError::Dispatch(
                "postgres host must not be empty".to_string(),
            ));
        }
        if port == 0 {
            return Err(ConnectorError::Dispatch(
                "postgres port must be 1..=65535".to_string(),
            ));
        }
        if pool_size == 0 {
            return Err(ConnectorError::Dispatch(
                "postgres pool_size must be >= 1".to_string(),
            ));
        }
        let user = if user.is_empty() {
            "postgres".to_string()
        } else {
            user.to_string()
        };
        Ok(Self {
            pg_config: pg_connect_config(host, port, &user, password, database),
            use_tls,
            pool: (0..pool_size).map(|_| AsyncMutex::new(None)).collect(),
            cursor: AtomicU64::new(0),
        })
    }

    async fn dial(&self) -> Result<tokio_postgres::Client> {
        if self.use_tls {
            let tls = pg_tls_connector()?;
            let connect = self.pg_config.connect(tls);
            let (client, connection) = tokio::time::timeout(PG_DRIVER_CONNECT_TIMEOUT, connect)
                .await
                .map_err(|_| ConnectorError::Connection("postgres connect timeout".to_string()))?
                .map_err(|e| ConnectorError::Connection(format!("postgres connect failed: {e}")))?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::warn!(error = %e, "postgres driver connection closed");
                }
            });
            Ok(client)
        } else {
            let connect = self.pg_config.connect(tokio_postgres::NoTls);
            let (client, connection) = tokio::time::timeout(PG_DRIVER_CONNECT_TIMEOUT, connect)
                .await
                .map_err(|_| ConnectorError::Connection("postgres connect timeout".to_string()))?
                .map_err(|e| ConnectorError::Connection(format!("postgres connect failed: {e}")))?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::warn!(error = %e, "postgres driver connection closed");
                }
            });
            Ok(client)
        }
    }

    /// Execute one row's bound parameters through the driver's
    /// extended-protocol path.
    async fn execute_row(
        client: &tokio_postgres::Client,
        sql: &str,
        row: &[Vec<u8>],
    ) -> Result<()> {
        // The sink always buffers exactly three values (topic, QoS,
        // payload); the empty default keeps a hand-built batch total
        // instead of panicking.
        let topic = pg_text_column(row, 0, "topic")?;
        let qos_text = pg_text_column(row, 1, "qos")?;
        // QoS travels as decimal text in the buffer but the server may
        // expect an integer (`$2::int`): parsing here keeps a corrupt
        // value a `Dispatch` data error (SQLSTATE 22P02, class 22) instead
        // of a client-side serialization failure with no SQLSTATE.
        let qos_value: i32 = qos_text.trim().parse().map_err(|_| {
            ConnectorError::Dispatch(format!(
                "postgres driver error: 22P02: qos is not an integer: {qos_text:?}"
            ))
        })?;
        let qos = PgQosParam {
            text: qos_text,
            value: qos_value,
        };
        let payload = PgPayloadParam(pg_text_column(row, 2, "payload")?);
        // PERF(parity): one extended-protocol round trip per row; the
        // fast version would pack the batch into a single multi-row
        // VALUES list. Kept per-row so a bad row fails alone and the
        // sink's row-by-row isolation keeps working.
        let refs: &[&(dyn tokio_postgres::types::ToSql + Sync)] = &[&topic, &qos, &payload];
        tokio::time::timeout(PG_DRIVER_STATEMENT_TIMEOUT, client.execute(sql, refs))
            .await
            .map_err(|_| ConnectorError::Connection("postgres query timeout".to_string()))?
            .map(|_| ())
            .map_err(|e| map_pg_driver_error(&e))
    }

    async fn execute_rows(
        client: &tokio_postgres::Client,
        sql: &str,
        rows: &[Vec<Vec<u8>>],
    ) -> Result<()> {
        for row in rows {
            Self::execute_row(client, sql, row).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl PgTransport for DriverPgTransport {
    async fn execute_batch(&self, batch: &PgBatch) -> Result<()> {
        if batch.rows.is_empty() {
            return Ok(());
        }
        // Round-robin checkout with reconnect-once for I/O failures. A
        // server data error (Dispatch) leaves the connection healthy,
        // so it is kept and nothing is replayed; a connection failure
        // drops the slot and the batch is retried once on a fresh
        // client, which is also what recovers a killed backend.
        let slot = (self.cursor.fetch_add(1, Ordering::SeqCst) as usize) % self.pool.len();
        let mut guard = self.pool[slot].lock().await;
        let mut last_error: Option<ConnectorError> = None;
        for _ in 0..2 {
            if guard.is_none() {
                *guard = Some(self.dial().await?);
            }
            let client = guard.as_ref().expect("connected");
            match Self::execute_rows(client, &batch.sql, &batch.rows).await {
                Ok(()) => return Ok(()),
                Err(ConnectorError::Connection(message)) => {
                    *guard = None;
                    last_error = Some(ConnectorError::Connection(message));
                }
                Err(other) => return Err(other),
            }
        }
        Err(match last_error {
            Some(ConnectorError::Connection(message)) => ConnectorError::Connection(format!(
                "postgres batch failed after reconnect: {message}"
            )),
            Some(other) => other,
            None => ConnectorError::Connection("postgres batch failed after reconnect".to_string()),
        })
    }
}

/// PostgreSQL sink: buffers MQTT events as 3-column rows
/// (`$1` topic, `$2` QoS, `$3` payload) and flushes full or stale
/// batches through the transport. Transport failures back off
/// exponentially (capped) without losing buffered rows.
///
/// The kind label stays `postgres` unless an action builder sets its
/// own protocol kind (AlloyDB, CockroachDB and Redshift speak the
/// same wire protocol): the label only names the sink for display,
/// delivery never branches on it.
pub struct PostgreSqlSink {
    config: PostgreSqlSinkConfig,
    transport: Arc<dyn PgTransport>,
    buffer: parking_lot::Mutex<super::BatchQueue<Vec<Vec<u8>>>>,
    backoff: parking_lot::Mutex<super::BackoffState>,
    sent_batches: AtomicU64,
    rejected_rows: AtomicU64,
    kind: &'static str,
}

impl PostgreSqlSink {
    pub fn new(config: PostgreSqlSinkConfig, transport: Arc<dyn PgTransport>) -> Result<Self> {
        config.validate()?;
        let linger = Duration::from_millis(config.batch_timeout_ms);
        Ok(Self {
            buffer: parking_lot::Mutex::new(super::BatchQueue::new(config.batch_size, linger)),
            config,
            transport,
            backoff: parking_lot::Mutex::new(super::BackoffState::default()),
            sent_batches: AtomicU64::new(0),
            rejected_rows: AtomicU64::new(0),
            kind: "postgres",
        })
    }

    /// Name this sink for display (a PostgreSQL-protocol family kind
    /// sharing the wire format). Delivery never branches on the label.
    pub fn with_kind(mut self, kind: &'static str) -> Self {
        self.kind = kind;
        self
    }

    pub fn config(&self) -> &PostgreSqlSinkConfig {
        &self.config
    }

    pub fn sent_batches(&self) -> u64 {
        self.sent_batches.load(Ordering::Relaxed)
    }

    /// Rows the server refused with a data error and the sink dropped.
    pub fn rejected_rows(&self) -> u64 {
        self.rejected_rows.load(Ordering::Relaxed)
    }

    pub fn buffered_rows(&self) -> usize {
        self.buffer.lock().len()
    }

    /// Flush buffered rows as one batch (no-op when empty). While
    /// backing off, fails fast without touching the transport. A batch
    /// the server rejects with a data error (bad row) is retried row by
    /// row so one bad row cannot block the rows behind it; rejected rows
    /// are counted and dropped, never restored.
    pub async fn flush(&self) -> Result<()> {
        self.backoff.lock().check()?;
        let (rows, oldest) = self.buffer.lock().take_batch();
        if rows.is_empty() {
            return Ok(());
        }
        let sql = self.config.sql_template.clone();
        let batch = PgBatch {
            sql: sql.clone(),
            rows,
        };
        match self.transport.execute_batch(&batch).await {
            Ok(()) => {
                self.backoff.lock().success();
                self.sent_batches.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                let data_error = match &e {
                    ConnectorError::Dispatch(message) => is_pg_data_error(message),
                    _ => false,
                };
                if !data_error {
                    self.buffer.lock().restore(batch.rows, oldest);
                    self.backoff.lock().failure();
                    return Err(e);
                }
                if batch.rows.len() == 1 {
                    self.rejected_rows.fetch_add(1, Ordering::Relaxed);
                    self.backoff.lock().success();
                    self.sent_batches.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(rejected = 1, error = %e, "postgres rejected bad row");
                    return Ok(());
                }
                let first_error = e.to_string();
                let mut rejected = 0u64;
                let mut index = 0;
                while index < batch.rows.len() {
                    let single = PgBatch {
                        sql: sql.clone(),
                        rows: vec![batch.rows[index].clone()],
                    };
                    match self.transport.execute_batch(&single).await {
                        Ok(()) => index += 1,
                        Err(row_error) => {
                            let row_data_error = match &row_error {
                                ConnectorError::Dispatch(message) => is_pg_data_error(message),
                                _ => false,
                            };
                            if row_data_error {
                                rejected += 1;
                                index += 1;
                            } else {
                                let rest: Vec<Vec<Vec<u8>>> =
                                    batch.rows.into_iter().skip(index).collect();
                                self.buffer.lock().restore(rest, oldest);
                                self.backoff.lock().failure();
                                return Err(row_error);
                            }
                        }
                    }
                }
                self.rejected_rows.fetch_add(rejected, Ordering::Relaxed);
                self.backoff.lock().success();
                self.sent_batches.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(rejected = rejected, error = %first_error, "postgres rejected bad rows");
                Ok(())
            }
        }
    }

    fn buffer_row(&self, topic: &Topic, payload: &Bytes, qos: QoS) -> bool {
        self.buffer.lock().push(vec![
            topic.as_str().as_bytes().to_vec(),
            u8::from(qos).to_string().into_bytes(),
            payload.to_vec(),
        ])
    }
}

#[async_trait]
impl Sink for PostgreSqlSink {
    async fn send(&self, topic: &Topic, payload: &Bytes, qos: QoS) -> super::Result<()> {
        if self.buffer_row(topic, payload, qos) {
            self.flush().await?;
        }
        Ok(())
    }

    async fn send_batch(&self, items: &[super::SinkBatchItem]) -> super::Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        for item in items {
            self.buffer_row(&item.topic, &item.payload, item.qos);
        }
        self.flush().await
    }

    fn kind(&self) -> &'static str {
        self.kind
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> PostgreSqlSinkConfig {
        PostgreSqlSinkConfig {
            connection_url: "postgresql://user:pass@127.0.0.1:5432/db".to_string(),
            sql_template: "INSERT INTO sensor_data (topic, qos, payload, recorded_at) VALUES ($1, $2, $3::jsonb, NOW())".to_string(),
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
        config.connection_url = "postgresql://h/db".to_string();

        config.sql_template.clear();
        assert!(config.validate().is_err());

        // Unknown / zero markers rejected (only $1..=$3 exist).
        for bad in [
            "INSERT INTO t VALUES ($1, $2, $4)",
            "INSERT INTO t VALUES ($0)",
            "INSERT INTO t VALUES ($1, $2, $3, $9)",
        ] {
            config.sql_template = bad.to_string();
            assert!(config.validate().is_err(), "must reject {bad:?}");
        }
        // Markers inside strings and comments are not parameters.
        config.sql_template =
            "INSERT INTO t (topic, note) VALUES ($1, 'cost $5 -- $9 /* $7 */')".to_string();
        assert!(config.validate().is_ok());
        config.sql_template =
            "INSERT INTO t (topic) VALUES ($1) -- trailing $8 comment".to_string();
        assert!(config.validate().is_ok());
    }

    fn test_sink(config: PostgreSqlSinkConfig) -> (PostgreSqlSink, Arc<MemoryPgTransport>) {
        let transport = Arc::new(MemoryPgTransport::new());
        let sink = PostgreSqlSink::new(config, transport.clone()).expect("valid sink");
        (sink, transport)
    }

    #[tokio::test]
    async fn test_batch_aggregation_and_parameter_binding() {
        let (sink, transport) = test_sink(test_config());
        let topic = Topic::new("sensors/temp").unwrap();

        // Three sends stay buffered (batch_size 100)...
        for _ in 0..3 {
            sink.send(
                &topic,
                &Bytes::from_static(br#"{ "v": 1 }"#),
                QoS::AtLeastOnce,
            )
            .await
            .unwrap();
        }
        assert_eq!(sink.buffered_rows(), 3);
        assert!(transport.batches().is_empty());

        // ...until an explicit flush ships one 3-row batch.
        sink.flush().await.unwrap();
        assert_eq!(sink.buffered_rows(), 0);
        assert_eq!(sink.sent_batches(), 1);
        let batches = transport.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].rows.len(), 3);
        assert!(batches[0].sql.contains("INSERT INTO sensor_data"));
        // $1 topic, $2 qos, $3 payload — no string interpolation.
        assert_eq!(batches[0].rows[0][0], b"sensors/temp");
        assert_eq!(batches[0].rows[0][1], b"1");
        assert_eq!(batches[0].rows[0][2], br#"{ "v": 1 }"#);
        assert!(
            !batches[0].sql.contains("sensors/temp"),
            "values stay bound"
        );
    }

    #[tokio::test]
    async fn test_batch_size_triggers_flush() {
        let mut config = test_config();
        config.batch_size = 2;
        let (sink, transport) = test_sink(config);
        let topic = Topic::new("t").unwrap();
        sink.send(&topic, &Bytes::from_static(b"a"), QoS::AtMostOnce)
            .await
            .unwrap();
        assert!(transport.batches().is_empty());
        sink.send(&topic, &Bytes::from_static(b"b"), QoS::AtMostOnce)
            .await
            .unwrap();
        assert_eq!(transport.batches().len(), 1);
        assert_eq!(transport.batches()[0].rows.len(), 2);
    }

    #[tokio::test]
    async fn test_error_backoff_skips_transport() {
        // Batch of one: every send attempts a flush.
        let mut config = test_config();
        config.batch_size = 1;
        let (sink, transport) = test_sink(config);
        let topic = Topic::new("t").unwrap();
        transport.fail_next(10);

        sink.send(&topic, &Bytes::from_static(b"a"), QoS::AtMostOnce)
            .await
            .expect_err("first flush fails");
        assert_eq!(transport.calls(), 1);
        // Still backing off: the transport is not even consulted, and the
        // failed row is restored, not lost.
        sink.send(&topic, &Bytes::from_static(b"b"), QoS::AtMostOnce)
            .await
            .expect_err("backoff fails fast");
        assert_eq!(transport.calls(), 1, "backoff must skip the transport");
        assert_eq!(sink.buffered_rows(), 2);
    }

    #[tokio::test]
    async fn test_bad_row_rejected_rest_written() {
        let transport = Arc::new(MemoryPgTransport::new());
        transport.fail_batches_containing(
            b"__BAD_ROW_MARKER__",
            "postgres: SQLSTATE 23514: new row violates check constraint",
        );
        let sink = PostgreSqlSink::new(test_config(), transport.clone()).expect("valid sink");
        let topic = Topic::new("sensors/temp").unwrap();
        let payloads = ["good-0", "good-1", "__BAD_ROW_MARKER__", "good-3", "good-4"];
        for payload in payloads {
            sink.send(&topic, &Bytes::from(payload.to_string()), QoS::AtLeastOnce)
                .await
                .expect("buffered");
        }
        assert_eq!(sink.buffered_rows(), 5);
        sink.flush().await.expect("bad row must not fail flush");
        assert_eq!(sink.rejected_rows(), 1);
        assert_eq!(sink.buffered_rows(), 0);
        let batches = transport.batches();
        assert_eq!(batches.len(), 4, "four retried single-row batches");
        let recorded: Vec<Vec<u8>> = batches
            .iter()
            .flat_map(|batch| {
                assert_eq!(batch.rows.len(), 1, "retries are single-row");
                batch
                    .rows
                    .iter()
                    .map(|row| row[2].clone())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(recorded, vec![b"good-0", b"good-1", b"good-3", b"good-4"]);
    }

    #[tokio::test]
    async fn test_connection_error_during_row_retry_restores_rest() {
        let transport = Arc::new(MemoryPgTransport::new());
        transport.fail_batches_containing(
            b"T3-DATA-BAD",
            "postgres: SQLSTATE 23514: new row violates check constraint",
        );
        transport.fail_connection_batches_containing(b"T3-CONN-BAD", "mock connection reset");
        let sink = PostgreSqlSink::new(test_config(), transport.clone()).expect("valid sink");
        let topic = Topic::new("t").unwrap();
        for payload in [
            "t3-r0-good",
            "t3-r1-T3-CONN-BAD",
            "t3-r2-T3-DATA-BAD",
            "t3-r3-good",
            "t3-r4-good",
        ] {
            sink.send(&topic, &Bytes::from(payload.to_string()), QoS::AtMostOnce)
                .await
                .expect("buffered");
        }
        let err = sink.flush().await.expect_err("connection error must fail");
        assert!(
            matches!(err, ConnectorError::Connection(_)),
            "must surface the connection error, got: {err}"
        );
        assert_eq!(sink.rejected_rows(), 0);
        assert_eq!(sink.buffered_rows(), 4);
        let batches = transport.batches();
        assert_eq!(batches.len(), 1, "only the first retried row was written");
        assert_eq!(batches[0].rows.len(), 1);
        assert_eq!(batches[0].rows[0][2], b"t3-r0-good");
    }

    #[test]
    fn test_driver_endpoint_parsing() {
        let (host, port, user, password, database, use_tls) =
            parse_driver_endpoint("postgresql://user:pass@db.internal:5433/telemetry")
                .expect("parses");
        assert_eq!(host, "db.internal");
        assert_eq!(port, 5433);
        assert_eq!(user, "user");
        assert_eq!(password, "pass");
        assert_eq!(database, "telemetry");
        assert!(!use_tls);

        // The single-`postgres://` spelling keeps working.
        let (host, port, user, _, database, use_tls) =
            parse_driver_endpoint("postgres://u@dbhost/mydb").expect("alt scheme");
        assert_eq!(host, "dbhost");
        assert_eq!(port, 5432);
        assert_eq!(user, "u");
        assert_eq!(database, "mydb");
        assert!(!use_tls);

        // Explicit TLS opt-in.
        let (_, _, _, _, _, use_tls) =
            parse_driver_endpoint("postgresql://u:p@h/db?sslmode=require").expect("tls");
        assert!(use_tls);
        let (_, _, _, _, _, use_tls) =
            parse_driver_endpoint("postgresql://u:p@h/db?sslmode=verify-full").expect("tls");
        assert!(use_tls);
        let (_, _, _, _, _, use_tls) =
            parse_driver_endpoint("postgresql://u:p@h/db?sslmode=disable").expect("plain");
        assert!(!use_tls);

        assert!(DriverPgTransport::new("postgresql://127.0.0.1:1/db", 0).is_err());
        assert!(DriverPgTransport::new("mysql://127.0.0.1:5432/db", 1).is_err());
        assert!(DriverPgTransport::new("not a url", 1).is_err());
        assert!(DriverPgTransport::new("postgresql://h:notaport/db", 1).is_err());
        assert!(DriverPgTransport::new("postgresql://u:p@127.0.0.1:1/db", 1).is_ok());
        assert!(DriverPgTransport::new("postgres://u:p@127.0.0.1:1/db", 1).is_ok());
    }

    #[test]
    fn test_driver_error_mapping_preserves_sqlstate() {
        // `is_pg_data_error` drives the sink's rejected-row path, so the
        // driver mapping must keep the SQLSTATE text observable.
        assert!(is_pg_data_error(
            "postgres driver error: 23514: new row violates check constraint"
        ));
        assert!(is_pg_data_error(
            "postgres driver error: 22021: payload is not valid UTF-8"
        ));
        assert!(is_pg_retryable_sqlstate("08000"));
        assert!(is_pg_retryable_sqlstate("57P01"));
        assert!(is_pg_retryable_sqlstate("40001"));
        assert!(!is_pg_retryable_sqlstate("23514"));
        assert!(!is_pg_retryable_sqlstate("42703"));
    }

    #[tokio::test]
    async fn test_driver_write_path_through_connector_manager() {
        // Broker path (connect, publish, deliver):
        // ConnectorManager::send -> Sink::send -> flush ->
        // DriverPgTransport::execute_batch. Points at an unroutable port
        // so the offline gate stays green while still driving the
        // production driver transport; the connection must fail closed,
        // never grant access.
        use super::super::ConnectorManager;
        let url = "postgresql://u:p@127.0.0.1:1/db";
        let transport = Arc::new(DriverPgTransport::new(url, 1).expect("driver transport"));
        let config = PostgreSqlSinkConfig {
            connection_url: url.to_string(),
            sql_template: "INSERT INTO t (topic, qos, payload) VALUES ($1, $2, $3)".to_string(),
            pool_size: 1,
            batch_size: 1,
            batch_timeout_ms: 60_000, // explicit flushes only; keeps staleness out
        };
        let sink = Arc::new(PostgreSqlSink::new(config, transport).expect("valid sink"));
        assert_eq!(sink.kind(), "postgres");
        let manager = ConnectorManager::new();
        manager.register("postgres-driver", sink);
        let res = manager
            .send(
                "postgres-driver",
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
            err.to_string().contains("postgres"),
            "driver error must be observable, got: {err}"
        );
    }

    fn qual_env(name: &str) -> Option<String> {
        std::env::var(name).ok().filter(|v| !v.trim().is_empty())
    }

    fn qual_require(name: &str) -> String {
        qual_env(name).unwrap_or_else(|| {
            panic!(
                "{name} must point at a real PostgreSQL server for qualification; \
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

    /// Direct driver client for DDL and row-count assertions. Panics on
    /// failure: the qualification gate always provides the server, so a
    /// failed connect is a defect, never a skip.
    async fn qual_client(url: &str) -> tokio_postgres::Client {
        let (host, port, user, password, database, use_tls) =
            parse_driver_endpoint(url).expect("qual url parses");
        let cfg = pg_connect_config(&host, port, &user, &password, &database);
        let connect = async {
            if use_tls {
                let tls = pg_tls_connector().expect("qual tls connector");
                let (client, connection) = cfg.connect(tls).await.expect("qual tls connect");
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        eprintln!("qual connection closed: {e}");
                    }
                });
                client
            } else {
                let (client, connection) = cfg
                    .connect(tokio_postgres::NoTls)
                    .await
                    .expect("qual connect");
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        eprintln!("qual connection closed: {e}");
                    }
                });
                client
            }
        };
        tokio::time::timeout(PG_DRIVER_CONNECT_TIMEOUT, connect)
            .await
            .expect("qual connect timeout")
    }

    /// Qualification against a real PostgreSQL server over the
    /// maintained `tokio-postgres` driver ([`DriverPgTransport`]).
    ///
    /// Run with e.g.:
    /// `POSTGRES_HOST=127.0.0.1 POSTGRES_PORT=5432 POSTGRES_DATABASE=qual \
    ///  POSTGRES_USER=qual POSTGRES_PASSWORD=qualpass1 \
    ///  POSTGRES_MD5_USER=qual_md5 POSTGRES_MD5_PASSWORD=md5pass1 \
    ///  POSTGRES_TABLE=pg_qual_b334 \
    ///  cargo test -p broker-connectors --lib postgres::tests::test_qualify_driver_write_path -- --ignored --nocapture --test-threads=1`
    ///
    /// Creates a table with a CHECK constraint, proves a violating row
    /// surfaces SQLSTATE 23514 on both the SCRAM and the MD5 login,
    /// streams 5000 rows (2500 per login) through the broker
    /// ([`crate::ConnectorManager`] -> [`PostgreSqlSink`] on
    /// [`DriverPgTransport`]), asserts `SELECT COUNT(*)` returns 5000,
    /// kills the pooled backends and proves the next flush recovers,
    /// proves a wrong password fails closed, then drops the table it
    /// created. Panics when its environment is missing; never skips.
    #[tokio::test]
    #[ignore = "needs a real PostgreSQL server (see POSTGRES_* env)"]
    async fn test_qualify_driver_write_path() {
        use crate::ConnectorManager;

        let host = qual_require("POSTGRES_HOST");
        let port: u16 = qual_require("POSTGRES_PORT")
            .parse()
            .expect("qual POSTGRES_PORT must be a port number");
        let database = qual_identifier("database", &qual_require("POSTGRES_DATABASE"));
        let user = qual_require("POSTGRES_USER");
        let password = qual_require("POSTGRES_PASSWORD");
        let md5_user = qual_require("POSTGRES_MD5_USER");
        let md5_password = qual_require("POSTGRES_MD5_PASSWORD");
        let table = qual_identifier("table", &qual_require("POSTGRES_TABLE"));
        const ROWS_PER_LOGIN: u64 = 2500;
        const RECOVERY_ROWS: u64 = 10;

        let scram_url = format!("postgresql://{user}:{password}@{host}:{port}/{database}");
        let md5_url = format!("postgresql://{md5_user}:{md5_password}@{host}:{port}/{database}");

        // Direct driver client for DDL and assertions; the SCRAM login
        // connecting at all is the first SCRAM proof.
        let ddl = qual_client(&scram_url).await;
        let version: String = ddl
            .query_one("SELECT version()", &[])
            .await
            .expect("qual server version")
            .get(0);
        eprintln!("qual server: version={version} host={host}:{port} database={database}");
        assert!(
            version.contains("PostgreSQL"),
            "qualification must run against PostgreSQL, got: {version}"
        );

        ddl.batch_execute(format!("DROP TABLE IF EXISTS {table}").as_str())
            .await
            .expect("qual drop stale table");
        let create = format!(
            "CREATE TABLE {table} (topic TEXT NOT NULL, qos INTEGER NOT NULL, \
             payload JSONB NOT NULL, CONSTRAINT {table}_chk CHECK (char_length(topic) <= 128))"
        );
        ddl.batch_execute(create.as_str())
            .await
            .expect("qual create table with CHECK");
        // The table is owned by the SCRAM login; the MD5 login only has
        // schema usage from QUAL-SETUP, so grant table access or every
        // MD5 write fails with 42501 instead of reaching the CHECK.
        let md5_ident = qual_identifier("md5_user", &md5_user);
        ddl.batch_execute(format!("GRANT ALL ON TABLE {table} TO {md5_ident}").as_str())
            .await
            .expect("qual grant md5 table access");

        let template =
            format!("INSERT INTO {table} (topic, qos, payload) VALUES ($1, $2::int, $3::jsonb)");
        let scram_transport =
            Arc::new(DriverPgTransport::new(&scram_url, 2).expect("qual scram transport"));
        let md5_transport =
            Arc::new(DriverPgTransport::new(&md5_url, 2).expect("qual md5 transport"));

        // CHECK proof on both logins (SCRAM and MD5): one violating row
        // per transport must surface SQLSTATE 23514 as a data error.
        for (label, transport) in [
            ("scram", scram_transport.clone()),
            ("md5", md5_transport.clone()),
        ] {
            let batch = PgBatch {
                sql: template.clone(),
                rows: vec![vec![
                    vec![b'x'; 200],
                    b"1".to_vec(),
                    br#"{"seq":0}"#.to_vec(),
                ]],
            };
            let err = transport
                .execute_batch(&batch)
                .await
                .expect_err("qual CHECK probe must fail");
            let text = format!("{err}");
            assert!(
                matches!(err, ConnectorError::Dispatch(_)),
                "qual {label} CHECK must be a data error, got: {text}"
            );
            assert!(
                text.contains("23514"),
                "qual {label} CHECK SQLSTATE 23514 must surface, got: {text:?}"
            );
            eprintln!("qual CHECK asserted: login={label} message={text:?}");
        }

        // The broker's path: rule actions deliver through the shared
        // connector manager, so the qualification sends through it and
        // never `sink.send` directly.
        let mk_config = |url: &str| PostgreSqlSinkConfig {
            connection_url: url.to_string(),
            sql_template: template.clone(),
            pool_size: 2,
            batch_size: 500,
            batch_timeout_ms: 60_000, // explicit flushes only; keeps staleness out
        };
        let scram_sink = Arc::new(
            PostgreSqlSink::new(mk_config(&scram_url), scram_transport.clone())
                .expect("qual scram sink"),
        );
        let md5_sink = Arc::new(
            PostgreSqlSink::new(mk_config(&md5_url), md5_transport.clone()).expect("qual md5 sink"),
        );
        assert_eq!(scram_sink.kind(), "postgres");
        assert_eq!(md5_sink.kind(), "postgres");
        let manager = ConnectorManager::new();
        manager.register("qual-postgres-scram", scram_sink.clone());
        manager.register("qual-postgres-md5", md5_sink.clone());

        let topic = Topic::new("sensors/qual").unwrap();
        for seq in 0..ROWS_PER_LOGIN {
            let scram_payload = Bytes::from(format!(r#"{{"src":"scram","seq":{seq}}}"#));
            manager
                .send(
                    "qual-postgres-scram",
                    &topic,
                    &scram_payload,
                    QoS::AtLeastOnce,
                )
                .await
                .expect("qual scram send");
            let md5_payload = Bytes::from(format!(r#"{{"src":"md5","seq":{seq}}}"#));
            manager
                .send("qual-postgres-md5", &topic, &md5_payload, QoS::AtLeastOnce)
                .await
                .expect("qual md5 send");
        }
        scram_sink.flush().await.expect("qual scram flush");
        md5_sink.flush().await.expect("qual md5 flush");
        assert_eq!(scram_sink.sent_batches(), 5, "qual scram batches");
        assert_eq!(md5_sink.sent_batches(), 5, "qual md5 batches");
        eprintln!("qual rows sent: scram={ROWS_PER_LOGIN} md5={ROWS_PER_LOGIN}");

        // Row count asserted back from the server, not the counters.
        let count_sql = format!("SELECT COUNT(*) FROM {table}");
        let count: i64 = ddl
            .query_one(count_sql.as_str(), &[])
            .await
            .expect("qual count")
            .get(0);
        assert_eq!(count, 2 * ROWS_PER_LOGIN as i64, "qual count mismatch");
        eprintln!(
            "qual rows asserted: count={} table={table}",
            2 * ROWS_PER_LOGIN
        );

        // Pool recovery: kill every pooled backend (all but this DDL
        // session), then prove the next flush reconnects instead of
        // failing. The sleep lets the SIGTERMs land so the reconnect
        // path is actually exercised.
        let killed: Vec<bool> = ddl
            .query(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                 WHERE datname = $1 AND pid <> pg_backend_pid()",
                &[&database],
            )
            .await
            .expect("qual kill backends")
            .iter()
            .map(|row| row.get(0))
            .collect();
        let killed_n = {
            let mut killed_n = 0usize;
            for granted in &killed {
                if *granted {
                    killed_n += 1;
                }
            }
            killed_n
        };
        eprintln!("qual killed {killed_n} backends");
        assert!(killed_n >= 1, "qual expected pooled backends to kill");
        tokio::time::sleep(Duration::from_millis(500)).await;
        for seq in 0..RECOVERY_ROWS {
            let payload = Bytes::from(format!(r#"{{"src":"recovery","seq":{seq}}}"#));
            manager
                .send("qual-postgres-scram", &topic, &payload, QoS::AtLeastOnce)
                .await
                .expect("qual recovery send");
        }
        scram_sink
            .flush()
            .await
            .expect("qual pool must recover after backend kill");
        eprintln!("qual recovery asserted: flush after kill succeeded");
        let count: i64 = ddl
            .query_one(count_sql.as_str(), &[])
            .await
            .expect("qual recount")
            .get(0);
        assert_eq!(
            count,
            2 * ROWS_PER_LOGIN as i64 + RECOVERY_ROWS as i64,
            "qual count after recovery"
        );

        // Wrong password fails closed: no rows written, connection
        // error, and the count is unchanged.
        let bad_url = format!("postgresql://{user}:wrongpass@{host}:{port}/{database}");
        let bad_transport = DriverPgTransport::new(&bad_url, 1).expect("qual bad transport");
        let bad_batch = PgBatch {
            sql: template.clone(),
            rows: vec![vec![
                b"sensors/qual".to_vec(),
                b"1".to_vec(),
                br#"{"seq":0}"#.to_vec(),
            ]],
        };
        let err = bad_transport
            .execute_batch(&bad_batch)
            .await
            .expect_err("qual bad password must fail");
        assert!(
            matches!(err, ConnectorError::Connection(_)),
            "qual bad password must fail closed, got: {err}"
        );
        eprintln!("qual auth asserted: wrong password fails closed");
        let count: i64 = ddl
            .query_one(count_sql.as_str(), &[])
            .await
            .expect("qual final count")
            .get(0);
        assert_eq!(
            count,
            2 * ROWS_PER_LOGIN as i64 + RECOVERY_ROWS as i64,
            "qual count unchanged"
        );

        // Cleanup: drop the table created for this run (best effort).
        match ddl
            .batch_execute(format!("DROP TABLE IF EXISTS {table}").as_str())
            .await
        {
            Ok(()) => eprintln!("qual cleanup: dropped table {table}"),
            Err(e) => {
                eprintln!("qual cleanup FAILED to drop {table} (tolerated): {e}");
            }
        }
        eprintln!(
            "qual done: rows={} table={table} cleaned table",
            2 * ROWS_PER_LOGIN + RECOVERY_ROWS
        );
    }
}
