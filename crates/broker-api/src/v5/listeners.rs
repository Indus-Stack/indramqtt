//! Listener inventory for the v5 REST API (AP-02).
//!
//! Covers `GET /listeners`, `POST /listeners`, `GET`/`PUT`/`POST`/`DELETE
//! `/listeners/{id}`, `POST /listeners/{id}/start|stop|restart` and `GET
//! `/listeners_status`. Management-plane only: reads take one short lock,
//! writes take one short lock plus at most one socket open or close, and
//! the publish path never touches this store.
//!
//! Rows report the stored configuration plus live state. `running` is
//! always present: true only while the kernel holds the socket open.
//! `current_connections` is the live per-listener count on kernel rows
//! and zero on edge rows. The kernel holds no socket for an edge row,
//! so it reports `running: false` and a zero count for it. Unsupported
//! write fields answer 400 `BAD_REQUEST`: the broker never stores a
//! value it does not apply or echo.
//!
//! Store bounds (stated here and enforced below):
//! - at most 32 listener records; creates past the bound fail closed.
//! - one `Arc` clone per request plus one small map scan; handler cost
//!   stays constant while connections scale.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

use crate::errors::ApiError;
use crate::node_scope::local_node_name;
use crate::ApiState;

// Reason: each row can hold one open socket and one accept task. The
// list handler clones every row once per request, so 32 caps open
// sockets far below the Linux default file-descriptor limit of 1024
// while leaving room past the four built-in listeners.
const MAX_LISTENERS: usize = 32;
// Reason: ids are map keys cloned into every list snapshot and carried
// in URL paths. 64 covers multi-word operator names with headroom while
// keeping per-request clones small.
const MAX_LISTENER_NAME_LEN: usize = 64;
// Reason: RFC 1035 caps a host name at 253 octets. With the colon and
// up to five port digits a `host:port` bind is at most 259 bytes.
const MAX_BIND_LEN: usize = 259;
// Reason: option objects ride inside the row cloned on every list read.
// 64 keys cover the documented option sets with headroom while keeping
// per-request clones small.
const MAX_OPTION_KEYS: usize = 64;

/// Holds the result of an SSL option read.
type SslOptionsRead =
    Result<(Option<serde_json::Value>, Option<String>, Option<String>), Box<Response>>;

/// Connection limit of one listener: a count or no limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListenerMaxConnections {
    /// At most this many concurrent connections.
    Finite(u32),
    /// No connection limit.
    Infinite,
}

/// One listener row: stored configuration plus live state.
///
/// `kernel_owned` is true only for the listeners the kernel binds
/// itself (`listeners.tcp.native`, `listeners.tls.native`,
/// `listeners.ws.native`, `listeners.wss.native`).
/// Every other row belongs to the edge, and lifecycle calls on it fail
/// closed with 400. `running` is true only while the kernel holds the
/// socket open.
#[derive(Debug, Clone)]
pub struct ListenerRecord {
    /// Row id in the `{type}:{name}` form.
    pub id: String,
    /// Short name after the colon.
    pub name: String,
    /// Listener kind: `tcp`, `ssl`, `ws`, `wss` or `quic`.
    pub kind: String,
    /// Configured bind (`host:port` or a bare port).
    pub bind: String,
    /// Whether new connections are accepted when the socket is open.
    pub enable: bool,
    /// Whether the kernel holds the socket open now.
    pub running: bool,
    /// Whether the kernel (not the edge) owns the socket.
    pub kernel_owned: bool,
    /// Configured connection limit, when the configuration sets one.
    pub max_connections: Option<ListenerMaxConnections>,
    /// Stored acceptor hint from the console form, when one was sent.
    pub acceptors: Option<u32>,
    /// Accept backlog used when the kernel binds the socket.
    pub backlog: u32,
    /// PEM certificate path of the TLS listener, empty when unused.
    pub cert_file: String,
    /// PEM key path of the TLS listener, empty when unused.
    pub key_file: String,
    /// Stored routing zone. The broker has no zones: only the
    /// documented default `default` is accepted, and reads echo it.
    pub zone: String,
    /// Stored mountpoint prefix. The broker has no mountpoints: only
    /// the empty value is accepted, and reads echo it.
    pub mountpoint: String,
    /// Stored TCP options. Only the reference defaults are accepted;
    /// `backlog` applies at the next bind, the rest only round-trip.
    pub tcp_options: Option<serde_json::Value>,
    /// Stored TLS options. `certfile` and `keyfile` apply at the next
    /// start of a kernel TLS row; the rest only round-trip.
    pub ssl_options: Option<serde_json::Value>,
    /// Stored WebSocket options. The kernel applies none of these keys,
    /// and only defaults round-trip. A kernel WebSocket row takes its
    /// path from the boot configuration.
    pub websocket: Option<serde_json::Value>,
}

/// Opens and closes the sockets of kernel-owned listeners.
///
/// The kernel installs its control at boot; without it, lifecycle calls
/// on kernel-owned rows fail closed. Tests install a loopback control.
pub trait ListenerController: Send + Sync {
    /// Bind the socket of `record` and accept on it. Success is
    /// idempotent: an already-open listener answers `Ok`.
    fn start_listener(&self, record: &ListenerRecord) -> Result<(), String>;
    /// Close the socket of `id`. An already-closed listener is a no-op.
    fn stop_listener(&self, id: &str);
    /// Live connection count of one open listener. Rows the control
    /// does not serve report zero.
    fn connection_count(&self, id: &str) -> u64;
}

/// Live listener directory shared by the kernel and the API handlers.
///
/// The kernel seeds it from the boot configuration and installs its
/// socket control; handlers read one snapshot per request. One short
/// lock per call; publish and deliver never touch this store.
pub struct ListenerStore {
    /// Records by `{type}:{name}` id.
    inner: RwLock<HashMap<String, ListenerRecord>>,
    /// Socket control of kernel-owned rows, empty without the kernel.
    controller: RwLock<Option<Arc<dyn ListenerController>>>,
}

/// Rank of a listener kind for list order. Follows the boot order.
fn listener_kind_rank(kind: &str) -> u8 {
    match kind {
        "tcp" => 0,
        "ssl" => 1,
        "ws" => 2,
        "wss" => 3,
        "quic" => 4,
        _ => 5,
    }
}

impl ListenerStore {
    /// Empty directory. The kernel fills it from the boot configuration.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            controller: RwLock::new(None),
        }
    }

    /// Directory with the four built-in listeners of `conf`. Every row
    /// starts stopped; the kernel marks its own rows running as it binds.
    pub fn from_config(conf: &broker_config::schema::ListenersConf) -> Self {
        let store = Self::new();
        store.reset_from_config(conf);
        store
    }

    /// Replace every row with the four built-in listeners of `conf`.
    /// Drops created rows and any installed control stays installed.
    pub fn reset_from_config(&self, conf: &broker_config::schema::ListenersConf) {
        let mut rows = HashMap::new();
        rows.insert(
            "tcp:default".to_string(),
            ListenerRecord {
                id: "tcp:default".to_string(),
                name: "default".to_string(),
                kind: "tcp".to_string(),
                bind: conf.tcp.bind.clone(),
                enable: conf.tcp.enabled,
                running: false,
                kernel_owned: conf.tcp.native,
                max_connections: Some(ListenerMaxConnections::Finite(conf.tcp.max_connections)),
                acceptors: None,
                backlog: conf.tcp.backlog,
                cert_file: String::new(),
                key_file: String::new(),
                zone: "default".to_string(),
                mountpoint: String::new(),
                tcp_options: None,
                ssl_options: None,
                websocket: None,
            },
        );
        rows.insert(
            "ssl:default".to_string(),
            ListenerRecord {
                id: "ssl:default".to_string(),
                name: "default".to_string(),
                kind: "ssl".to_string(),
                bind: conf.tls.bind.clone(),
                enable: conf.tls.enabled,
                running: false,
                kernel_owned: conf.tls.native,
                max_connections: Some(ListenerMaxConnections::Finite(conf.tcp.max_connections)),
                acceptors: None,
                backlog: conf.tcp.backlog,
                cert_file: conf.tls.cert_file.clone(),
                key_file: conf.tls.key_file.clone(),
                zone: "default".to_string(),
                mountpoint: String::new(),
                tcp_options: None,
                ssl_options: None,
                websocket: None,
            },
        );
        rows.insert(
            "ws:default".to_string(),
            ListenerRecord {
                id: "ws:default".to_string(),
                name: "default".to_string(),
                kind: "ws".to_string(),
                bind: conf.ws.bind.clone(),
                enable: conf.ws.enabled,
                running: false,
                kernel_owned: conf.ws.native,
                // A kernel row uses the limit of the plaintext listener.
                max_connections: conf
                    .ws
                    .native
                    .then_some(ListenerMaxConnections::Finite(conf.tcp.max_connections)),
                acceptors: None,
                backlog: 0,
                cert_file: String::new(),
                key_file: String::new(),
                zone: "default".to_string(),
                mountpoint: String::new(),
                tcp_options: None,
                ssl_options: None,
                websocket: None,
            },
        );
        rows.insert(
            "wss:default".to_string(),
            ListenerRecord {
                id: "wss:default".to_string(),
                name: "default".to_string(),
                kind: "wss".to_string(),
                bind: conf.wss.bind.clone(),
                enable: conf.wss.enabled,
                running: false,
                kernel_owned: conf.wss.native,
                // A kernel row uses the limit of the plaintext listener.
                max_connections: conf
                    .wss
                    .native
                    .then_some(ListenerMaxConnections::Finite(conf.tcp.max_connections)),
                acceptors: None,
                backlog: 0,
                cert_file: String::new(),
                key_file: String::new(),
                zone: "default".to_string(),
                mountpoint: String::new(),
                tcp_options: None,
                ssl_options: None,
                websocket: None,
            },
        );
        *self.inner.write().unwrap_or_else(|p| p.into_inner()) = rows;
    }

    /// Install the socket control. The kernel calls this once at boot.
    pub fn set_controller(&self, controller: Arc<dyn ListenerController>) {
        *self.controller.write().unwrap_or_else(|p| p.into_inner()) = Some(controller);
    }

    /// Fetch the installed control, if any.
    fn controller(&self) -> Option<Arc<dyn ListenerController>> {
        self.controller
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Read one record by id.
    pub fn get(&self, id: &str) -> Option<ListenerRecord> {
        self.inner
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(id)
            .cloned()
    }

    /// Read every record in boot order, then by id.
    pub fn list(&self) -> Vec<ListenerRecord> {
        let mut rows: Vec<ListenerRecord> = self
            .inner
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .cloned()
            .collect();
        rows.sort_by(|a, b| {
            listener_kind_rank(&a.kind)
                .cmp(&listener_kind_rank(&b.kind))
                .then_with(|| a.id.cmp(&b.id))
        });
        rows
    }

    /// Store a new record. Fails when the id exists or the bound is hit.
    pub fn insert(&self, record: ListenerRecord) -> Result<(), InsertError> {
        let mut rows = self.inner.write().unwrap_or_else(|p| p.into_inner());
        if rows.contains_key(&record.id) {
            return Err(InsertError::Duplicate);
        }
        if rows.len() >= MAX_LISTENERS {
            return Err(InsertError::Full);
        }
        rows.insert(record.id.clone(), record);
        Ok(())
    }

    /// Replace the record with the same id.
    pub fn update(&self, record: ListenerRecord) {
        self.inner
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(record.id.clone(), record);
    }

    /// Remove one record and return it, if it exists.
    pub fn remove(&self, id: &str) -> Option<ListenerRecord> {
        self.inner
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .remove(id)
    }
}

impl Default for ListenerStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Why a listener create failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertError {
    /// A record with this id already exists.
    Duplicate,
    /// The directory already holds [`MAX_LISTENERS`] rows.
    Full,
}

/// True when `kind` names a listener type the API serves.
fn is_listener_kind(kind: &str) -> bool {
    matches!(kind, "tcp" | "ssl" | "ws" | "wss" | "quic")
}

/// Split a `{type}:{name}` id into its parts. Rejects unknown types,
///
/// empty names and names that cannot round-trip through a URL path.
fn split_listener_id(id: &str) -> Option<(String, String)> {
    let (kind, name) = id.split_once(':')?;
    if !is_listener_kind(kind) {
        return None;
    }
    if name.is_empty() || name.len() > MAX_LISTENER_NAME_LEN {
        return None;
    }
    if name.contains(':') || name.contains('/') {
        return None;
    }
    Some((kind.to_string(), name.to_string()))
}

/// True when `bind` is `host:port` with a valid port or a bare port.
/// Mirrors the boot bind check: the host part stays unchecked.
fn valid_bind(bind: &str) -> bool {
    if bind.is_empty() || bind.len() > MAX_BIND_LEN {
        return false;
    }
    if let Ok(port) = bind.parse::<u16>() {
        return port >= 1;
    }
    let Some((host, port)) = bind.rsplit_once(':') else {
        return false;
    };
    if host.is_empty() {
        return false;
    }
    matches!(port.parse::<u16>(), Ok(port) if port >= 1)
}

/// Read an optional `max_connections` value: a count of at least one
/// or the string `infinity`.
fn parse_max_connections(
    value: Option<&serde_json::Value>,
) -> Result<Option<ListenerMaxConnections>, ()> {
    let Some(value) = value else {
        return Ok(None);
    };
    if let Some(text) = value.as_str() {
        if text == "infinity" {
            return Ok(Some(ListenerMaxConnections::Infinite));
        }
        return Err(());
    }
    match value.as_u64() {
        Some(count) if count >= 1 && count <= u64::from(u32::MAX) => {
            Ok(Some(ListenerMaxConnections::Finite(count as u32)))
        }
        _ => Err(()),
    }
}

/// Read an optional `acceptors` value: a count of at least one.
fn parse_acceptors(value: Option<&serde_json::Value>) -> Result<Option<u32>, ()> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value.as_u64() {
        Some(count) if count >= 1 && count <= u64::from(u32::MAX) => Ok(Some(count as u32)),
        _ => Err(()),
    }
}

/// Read the `bind` of a create body. The console sends a bare port as
/// a number, so both a string and an integer are accepted.
fn read_bind(body: &serde_json::Value) -> Result<String, Box<Response>> {
    let Some(raw) = body.get("bind") else {
        return Err(Box::new(
            ApiError::BadRequest(
                "missing bind: set `bind` to `host:port` (field `bind`)".to_string(),
            )
            .into_response(),
        ));
    };
    let text = match raw {
        value if value.is_string() => value.as_str().unwrap_or_default().to_string(),
        value if value.is_u64() => value.as_u64().unwrap_or(0).to_string(),
        value if value.is_i64() => value.as_i64().unwrap_or(0).to_string(),
        value => {
            return Err(Box::new(
                ApiError::BadRequest(format!(
                    "bind {} is invalid: set `host:port` with port 1..=65535 (field `bind`)",
                    value_to_short(value)
                ))
                .into_response(),
            ));
        }
    };
    if !valid_bind(&text) {
        return Err(Box::new(
            ApiError::BadRequest(format!(
                "bind {text:?} is invalid: set `host:port` with port 1..=65535 (field `bind`)"
            ))
            .into_response(),
        ));
    }
    Ok(text)
}

/// Read the `zone` of a create or update body. The broker has no zones:
/// only the documented default is accepted.
fn read_zone(body: &serde_json::Value) -> Result<String, Box<Response>> {
    match body.get("zone") {
        None => Ok("default".to_string()),
        Some(value) if value.as_str() == Some("default") => Ok("default".to_string()),
        Some(value) => Err(Box::new(
            ApiError::BadRequest(format!(
                "zone {} is not available in this broker: only `default` applies (field `zone`)",
                value_to_short(value)
            ))
            .into_response(),
        )),
    }
}

/// Read the `mountpoint` of a create or update body. The broker has no
/// mountpoints: only the empty value is accepted.
fn read_mountpoint(body: &serde_json::Value) -> Result<String, Box<Response>> {
    match body.get("mountpoint") {
        None => Ok(String::new()),
        Some(value) if value.as_str() == Some("") => Ok(String::new()),
        Some(value) => Err(Box::new(
            ApiError::BadRequest(format!(
                "mountpoint {} is not available in this broker (field `mountpoint`)",
                value_to_short(value)
            ))
            .into_response(),
        )),
    }
}

/// Short display of a rejected JSON value for error messages.
fn value_to_short(value: &serde_json::Value) -> String {
    let text = serde_json::to_string(value).unwrap_or_else(|_| "?".to_string());
    let short: String = text.chars().take(64).collect();
    if short.len() < text.len() {
        format!("{short}...")
    } else {
        short
    }
}

/// Limiter fields the broker does not apply. Each stays absent, null
/// or empty. Any other value fails closed.
const RATE_LIMIT_FIELDS: [&str; 12] = [
    "bytes_rate",
    "bytes_burst",
    "messages_rate",
    "messages_burst",
    "max_conn_rate",
    "max_conn_burst",
    "delivery_bytes_rate",
    "delivery_bytes_burst",
    "delivery_messages_rate",
    "delivery_messages_burst",
    "subscribes_rate",
    "subscribes_burst",
];

/// Reject any limiter field that asks for behaviour the broker lacks.
fn check_rate_limits(body: &serde_json::Value) -> Result<(), Box<Response>> {
    for field in RATE_LIMIT_FIELDS {
        match body.get(field) {
            None => {}
            Some(value) if value.is_null() => {}
            Some(value) if value.as_str() == Some("") => {}
            Some(value) => {
                return Err(Box::new(
                    ApiError::BadRequest(format!(
                        "{field} {} is not available in this broker (field `{field}`)",
                        value_to_short(value)
                    ))
                    .into_response(),
                ));
            }
        }
    }
    Ok(())
}

/// Read the top-level `ciphers` of a QUIC row. The broker configures no
/// cipher suites per listener: only an empty list is accepted.
fn check_ciphers(body: &serde_json::Value) -> Result<(), Box<Response>> {
    match body.get("ciphers") {
        None => Ok(()),
        Some(value) if value.as_array().is_some_and(|items| items.is_empty()) => Ok(()),
        Some(value) => Err(Box::new(
            ApiError::BadRequest(format!(
                "ciphers {} is not available in this broker (field `ciphers`)",
                value_to_short(value)
            ))
            .into_response(),
        )),
    }
}

/// True when `value` equals the documented default `want`.
fn is_default_int(value: &serde_json::Value, want: u64) -> bool {
    value.as_u64() == Some(want)
}

/// True when `value` equals the documented default `want`.
fn is_default_bool(value: &serde_json::Value, want: bool) -> bool {
    value.as_bool() == Some(want)
}

/// True when `value` equals the documented default `want`.
fn is_default_str(value: &serde_json::Value, want: &str) -> bool {
    value.as_str() == Some(want)
}

/// Read the `tcp_options` of a create or update body. `backlog` applies
/// at the next bind of the row. Every other key must carry its
/// documented default; any other value fails closed.
fn read_tcp_options(
    body: &serde_json::Value,
) -> Result<(Option<serde_json::Value>, Option<u32>), Box<Response>> {
    let Some(options) = body.get("tcp_options") else {
        return Ok((None, None));
    };
    let Some(map) = options.as_object() else {
        return Err(Box::new(
            ApiError::BadRequest(
                "tcp_options is invalid: set an object (field `tcp_options`)".to_string(),
            )
            .into_response(),
        ));
    };
    if map.len() > MAX_OPTION_KEYS {
        return Err(Box::new(
            ApiError::BadRequest(format!(
                "tcp_options holds {} keys, at most {MAX_OPTION_KEYS} (field `tcp_options`)",
                map.len()
            ))
            .into_response(),
        ));
    }
    let mut backlog: Option<u32> = None;
    for (key, value) in map {
        let accepted = match key.as_str() {
            "backlog" => match value.as_u64() {
                Some(count) if (1..=65_535).contains(&count) => {
                    backlog = Some(count as u32);
                    true
                }
                _ => false,
            },
            "active_n" => is_default_int(value, 10),
            "buffer" => is_default_str(value, "4KB"),
            "high_watermark" => is_default_str(value, "1MB"),
            "keepalive" => is_default_str(value, "none"),
            "nodelay" => is_default_bool(value, true),
            "nolinger" => is_default_bool(value, false),
            "reuseaddr" => is_default_bool(value, true),
            "send_timeout" => is_default_str(value, "15s"),
            "send_timeout_close" => is_default_bool(value, true),
            _ => false,
        };
        if !accepted {
            return Err(Box::new(
                ApiError::BadRequest(format!(
                    "tcp_options.{key} {} is not available in this broker (field `tcp_options`)",
                    value_to_short(value)
                ))
                .into_response(),
            ));
        }
    }
    Ok((Some(options.clone()), backlog))
}

/// Read the `ssl_options` of a create or update body. `certfile` and
/// `keyfile` apply at the next start of a kernel TLS row. Every other
/// key must carry its documented default; any other value fails closed.
fn read_ssl_options(body: &serde_json::Value) -> SslOptionsRead {
    let Some(options) = body.get("ssl_options") else {
        return Ok((None, None, None));
    };
    let Some(map) = options.as_object() else {
        return Err(Box::new(
            ApiError::BadRequest(
                "ssl_options is invalid: set an object (field `ssl_options`)".to_string(),
            )
            .into_response(),
        ));
    };
    if map.len() > MAX_OPTION_KEYS {
        return Err(Box::new(
            ApiError::BadRequest(format!(
                "ssl_options holds {} keys, at most {MAX_OPTION_KEYS} (field `ssl_options`)",
                map.len()
            ))
            .into_response(),
        ));
    }
    let mut cert: Option<String> = None;
    let mut key_file: Option<String> = None;
    for (key, value) in map {
        let accepted = match key.as_str() {
            "certfile" => match value.as_str() {
                Some("") => true,
                Some(path) => {
                    cert = Some(path.to_string());
                    true
                }
                None => false,
            },
            "keyfile" => match value.as_str() {
                Some("") => true,
                Some(path) => {
                    key_file = Some(path.to_string());
                    true
                }
                None => false,
            },
            "cacertfile" => value.as_str() == Some(""),
            "ciphers" => value.as_array().is_some_and(|items| items.is_empty()),
            "versions" => value.as_array().is_some_and(|items| {
                items.len() == 2
                    && items[0] == serde_json::json!("tlsv1.3")
                    && items[1] == serde_json::json!("tlsv1.2")
            }),
            "verify" => is_default_str(value, "verify_none"),
            "depth" => is_default_int(value, 10),
            "reuse_sessions" => is_default_bool(value, false),
            "secure_renegotiate" => is_default_bool(value, true),
            "honor_cipher_order" => is_default_bool(value, true),
            "client_renegotiation" => is_default_bool(value, true),
            "enable_crl_check" => is_default_bool(value, false),
            "fail_if_no_peer_cert" => is_default_bool(value, false),
            "gc_after_handshake" => is_default_bool(value, false),
            "handshake_timeout" => is_default_str(value, "15s"),
            "hibernate_after" => is_default_str(value, "5s"),
            "log_level" => is_default_str(value, "notice"),
            "session_tickets" => is_default_str(value, "disabled"),
            "cacerts" => is_default_bool(value, false),
            _ => false,
        };
        if !accepted {
            return Err(Box::new(
                ApiError::BadRequest(format!(
                    "ssl_options.{key} {} is not available in this broker (field `ssl_options`)",
                    value_to_short(value)
                ))
                .into_response(),
            ));
        }
    }
    Ok((Some(options.clone()), cert, key_file))
}

/// Read the `websocket` options of a create or update body. The edge
/// serves WebSocket rows and the kernel applies none of these keys:
/// every key must carry its documented default.
fn read_websocket(body: &serde_json::Value) -> Result<Option<serde_json::Value>, Box<Response>> {
    let Some(options) = body.get("websocket") else {
        return Ok(None);
    };
    let Some(map) = options.as_object() else {
        return Err(Box::new(
            ApiError::BadRequest(
                "websocket is invalid: set an object (field `websocket`)".to_string(),
            )
            .into_response(),
        ));
    };
    if map.len() > MAX_OPTION_KEYS {
        return Err(Box::new(
            ApiError::BadRequest(format!(
                "websocket holds {} keys, at most {MAX_OPTION_KEYS} (field `websocket`)",
                map.len()
            ))
            .into_response(),
        ));
    }
    for (key, value) in map {
        let accepted = match key.as_str() {
            "allow_origin_absence" => is_default_bool(value, true),
            "check_origin_enable" => is_default_bool(value, false),
            "check_origins" => {
                is_default_str(value, "http://localhost:18083, http://127.0.0.1:18083")
            }
            "compress" => is_default_bool(value, false),
            "fail_if_no_subprotocol" => is_default_bool(value, true),
            "idle_timeout" => is_default_str(value, "7200s"),
            "max_frame_size" => is_default_str(value, "infinity"),
            "mqtt_path" => is_default_str(value, "/mqtt"),
            "mqtt_piggyback" => is_default_str(value, "multiple"),
            "proxy_address_header" => is_default_str(value, ""),
            "proxy_port_header" => is_default_str(value, ""),
            "supported_subprotocols" => {
                is_default_str(value, "mqtt, mqtt-v3, mqtt-v3.1.1, mqtt-v5")
            }
            "validate_utf8" => is_default_bool(value, true),
            _ => false,
        };
        if !accepted {
            return Err(Box::new(
                ApiError::BadRequest(format!(
                    "websocket.{key} {} is not available in this broker (field `websocket`)",
                    value_to_short(value)
                ))
                .into_response(),
            ));
        }
    }
    Ok(Some(options.clone()))
}

/// Split `bind` into the resolved address and its source. A bare port
/// binds every interface, so the address is empty and the source is
/// `0.0.0.0`. An explicit host reports that host and the source `bind`.
/// A wildcard host reports an empty address: the socket binds everywhere.
fn resolved_bind(bind: &str) -> (String, &'static str) {
    if bind.parse::<u16>().is_ok() {
        return (String::new(), "0.0.0.0");
    }
    let Some((host, _)) = bind.rsplit_once(':') else {
        return (String::new(), "bind");
    };
    let bare = host
        .strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(']'))
        .unwrap_or(host);
    if bare.is_empty() || bare == "0.0.0.0" || bare == "::" {
        (String::new(), "bind")
    } else {
        (bare.to_string(), "bind")
    }
}

/// Live connection count of `record`. Kernel rows ask the installed
/// control; edge rows report zero: the kernel holds no socket for them.
fn current_count(store: &ListenerStore, record: &ListenerRecord) -> u64 {
    if !record.kernel_owned {
        return 0;
    }
    store
        .controller()
        .map_or(0, |control| control.connection_count(&record.id))
}

/// Render one record in the documented detail shape. `running` and
/// `current_connections` are always present; `zone` and `mountpoint`
/// echo the stored values; option objects round-trip when stored.
fn detail_json(record: &ListenerRecord, current: u64) -> serde_json::Value {
    let mut row = serde_json::Map::new();
    row.insert("id".to_string(), serde_json::json!(record.id));
    row.insert("name".to_string(), serde_json::json!(record.name));
    row.insert("type".to_string(), serde_json::json!(record.kind));
    row.insert("bind".to_string(), serde_json::json!(record.bind));
    row.insert("enable".to_string(), serde_json::json!(record.enable));
    row.insert("running".to_string(), serde_json::json!(record.running));
    row.insert(
        "current_connections".to_string(),
        serde_json::json!(current),
    );
    match record.max_connections {
        Some(ListenerMaxConnections::Finite(count)) => {
            row.insert("max_connections".to_string(), serde_json::json!(count));
        }
        Some(ListenerMaxConnections::Infinite) => {
            row.insert("max_connections".to_string(), serde_json::json!("infinity"));
        }
        None => {}
    }
    if let Some(acceptors) = record.acceptors {
        row.insert("acceptors".to_string(), serde_json::json!(acceptors));
    }
    row.insert("zone".to_string(), serde_json::json!(record.zone));
    row.insert(
        "mountpoint".to_string(),
        serde_json::json!(record.mountpoint),
    );
    if let Some(options) = record.tcp_options.as_ref() {
        row.insert("tcp_options".to_string(), options.clone());
    }
    if let Some(options) = record.ssl_options.as_ref() {
        row.insert("ssl_options".to_string(), options.clone());
    }
    if let Some(options) = record.websocket.as_ref() {
        row.insert("websocket".to_string(), options.clone());
    }
    serde_json::Value::Object(row)
}

/// Render the live status of one row. Holds the running flag, the live
/// count, the configured limit and the resolved bind address.
fn status_json(record: &ListenerRecord, current: u64) -> serde_json::Value {
    let (address, from) = resolved_bind(&record.bind);
    let mut status = serde_json::Map::new();
    status.insert("running".to_string(), serde_json::json!(record.running));
    status.insert(
        "current_connections".to_string(),
        serde_json::json!(current),
    );
    if let Some(ListenerMaxConnections::Finite(count)) = record.max_connections {
        status.insert("max_connections".to_string(), serde_json::json!(count));
    }
    if record.max_connections == Some(ListenerMaxConnections::Infinite) {
        status.insert("max_connections".to_string(), serde_json::json!("infinity"));
    }
    status.insert("resolved_address".to_string(), serde_json::json!(address));
    status.insert("resolved_address_from".to_string(), serde_json::json!(from));
    serde_json::Value::Object(status)
}

/// Render one row of `GET /listeners`: the detail fields plus the live
/// status and node status. The top-level `current_connections` serves
/// the console table, which reads it before the status object. `number`
/// counts the nodes that serve the row: one when running, omitted when
/// stopped.
fn row_json(record: &ListenerRecord, current: u64) -> serde_json::Value {
    let mut row = detail_json(record, current);
    if record.running {
        row["number"] = serde_json::json!(1);
    }
    let status = status_json(record, current);
    let node = local_node_name().to_string();
    row["status"] = status.clone();
    row["node_status"] = serde_json::json!([{ "node": node, "status": status }]);
    row
}

/// 400 with the `BAD_LISTENER_ID` code the lifecycle routes document
/// for unknown ids.
fn bad_listener_id_400(message: String) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({ "code": "BAD_LISTENER_ID", "message": message })),
    )
        .into_response()
}

/// `GET /listeners`: the listener rows as a bare array.
///
/// One row per stored listener with the fields `id`, `name`, `type`,
/// `bind`, `enable`, `running`, `current_connections`, `zone` and
/// `mountpoint`, plus `status` and `node_status`. A `type` query narrows
/// the list to that type.
pub async fn list_listeners(
    State(state): State<ApiState>,
    Query(query): Query<ListenersQuery>,
) -> impl IntoResponse {
    let rows: Vec<serde_json::Value> = state
        .listener_store
        .list()
        .iter()
        .map(|record| {
            let current = current_count(&state.listener_store, record);
            row_json(record, current)
        })
        .filter(|row| match query.kind.as_deref() {
            Some(kind) if !kind.is_empty() => row["type"] == kind,
            _ => true,
        })
        .collect();
    (StatusCode::OK, Json(rows))
}

/// `GET /listeners/{id}`: the detail of one listener.
pub async fn get_listener(State(state): State<ApiState>, Path(id): Path<String>) -> Response {
    let Some(record) = state.listener_store.get(&id) else {
        return ApiError::BadListenerId(format!("listener {id:?} not found")).into_response();
    };
    let current = current_count(&state.listener_store, &record);
    (StatusCode::OK, Json(detail_json(&record, current))).into_response()
}

/// Body fields shared by the create routes. `name`, `type` and `bind`
/// are required; `enable` defaults to true.
struct CreateFields {
    name: String,
    kind: String,
    bind: String,
    enable: bool,
    max_connections: Option<ListenerMaxConnections>,
    acceptors: Option<u32>,
    zone: String,
    mountpoint: String,
    backlog: Option<u32>,
    tcp_options: Option<serde_json::Value>,
    ssl_options: Option<serde_json::Value>,
    websocket: Option<serde_json::Value>,
    cert_file: String,
    key_file: String,
}

/// Read and validate the create fields of `body` for the listener `id`.
/// `None` keeps a field the body does not set.
fn create_fields(
    id: &str,
    kind: &str,
    name: &str,
    body: &serde_json::Value,
) -> Result<CreateFields, Box<Response>> {
    if let Some(body_id) = body.get("id").and_then(|v| v.as_str()) {
        if body_id != id {
            return Err(Box::new(
                ApiError::BadRequest(format!(
                    "body id {body_id:?} differs from path id {id:?} (field `id`)"
                ))
                .into_response(),
            ));
        }
    }
    if let Some(body_kind) = body.get("type").and_then(|v| v.as_str()) {
        if body_kind != kind {
            return Err(Box::new(
                ApiError::BadRequest(format!(
                    "body type {body_kind:?} differs from id type {kind:?} (field `type`)"
                ))
                .into_response(),
            ));
        }
    }
    if let Some(body_name) = body.get("name").and_then(|v| v.as_str()) {
        if body_name != name {
            return Err(Box::new(
                ApiError::BadRequest(format!(
                    "body name {body_name:?} differs from id name {name:?} (field `name`)"
                ))
                .into_response(),
            ));
        }
    }
    let bind = read_bind(body)?;
    let enable = body.get("enable").and_then(|v| v.as_bool()).unwrap_or(true);
    let max_connections =
        parse_max_connections(body.get("max_connections")).map_err(|()| {
            Box::new(
                ApiError::BadRequest(
                    "max_connections is invalid: set a count of at least 1 or `infinity` (field `max_connections`)".to_string(),
                )
                .into_response(),
            )
        })?;
    let acceptors = parse_acceptors(body.get("acceptors")).map_err(|()| {
        Box::new(
            ApiError::BadRequest(
                "acceptors is invalid: set a count of at least 1 (field `acceptors`)".to_string(),
            )
            .into_response(),
        )
    })?;
    let zone = read_zone(body)?;
    let mountpoint = read_mountpoint(body)?;
    check_rate_limits(body)?;
    check_ciphers(body)?;
    let (tcp_options, backlog) = read_tcp_options(body)?;
    let (ssl_options, cert_file, key_file) = read_ssl_options(body)?;
    let websocket = read_websocket(body)?;
    Ok(CreateFields {
        name: name.to_string(),
        kind: kind.to_string(),
        bind,
        enable,
        max_connections,
        acceptors,
        zone,
        mountpoint,
        backlog,
        tcp_options,
        ssl_options,
        websocket,
        cert_file: cert_file.unwrap_or_default(),
        key_file: key_file.unwrap_or_default(),
    })
}

/// Store one validated create and answer its detail. Created rows
/// belong to the edge: the kernel cannot bind them now.
fn store_created(store: &ListenerStore, id: &str, fields: CreateFields) -> Response {
    let record = ListenerRecord {
        id: id.to_string(),
        name: fields.name,
        kind: fields.kind,
        bind: fields.bind,
        enable: fields.enable,
        running: false,
        kernel_owned: false,
        max_connections: fields.max_connections,
        acceptors: fields.acceptors,
        backlog: fields.backlog.unwrap_or(0),
        cert_file: fields.cert_file,
        key_file: fields.key_file,
        zone: fields.zone,
        mountpoint: fields.mountpoint,
        tcp_options: fields.tcp_options,
        ssl_options: fields.ssl_options,
        websocket: fields.websocket,
    };
    match store.insert(record.clone()) {
        Ok(()) => (StatusCode::OK, Json(detail_json(&record, 0))).into_response(),
        Err(InsertError::Duplicate) => {
            ApiError::BadRequest(format!("listener {id:?} already exists (field `id`)"))
                .into_response()
        }
        Err(InsertError::Full) => ApiError::BadRequest(format!(
            "listener limit of {MAX_LISTENERS} reached: delete one first"
        ))
        .into_response(),
    }
}

/// `POST /listeners`: create one listener from a body with `name`,
/// `type` and `bind`.
pub async fn create_listener(
    State(state): State<ApiState>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(name) = body.get("name").and_then(|v| v.as_str()) else {
        return ApiError::BadRequest(
            "missing name: set the listener `name` (field `name`)".to_string(),
        )
        .into_response();
    };
    let Some(kind) = body.get("type").and_then(|v| v.as_str()) else {
        return ApiError::BadRequest(
            "missing type: set one of `tcp`, `ssl`, `ws`, `wss`, `quic` (field `type`)".to_string(),
        )
        .into_response();
    };
    if !is_listener_kind(kind) {
        return bad_listener_id_400(format!(
            "type {kind:?} is invalid: set one of `tcp`, `ssl`, `ws`, `wss`, `quic` (field `type`)"
        ));
    }
    if name.is_empty()
        || name.len() > MAX_LISTENER_NAME_LEN
        || name.contains(':')
        || name.contains('/')
    {
        return bad_listener_id_400(format!(
            "name {name:?} is invalid: set a non-empty name without `:` or `/` (field `name`)"
        ));
    }
    let id = format!("{kind}:{name}");
    let fields = match create_fields(&id, kind, name, &body) {
        Ok(fields) => fields,
        Err(response) => return *response,
    };
    store_created(&state.listener_store, &id, fields)
}

/// `POST /listeners/{id}`: the create call the console sends. The body
/// id, name and type must match the path id when present.
pub async fn create_listener_with_id(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some((kind, name)) = split_listener_id(&id) else {
        return bad_listener_id_400(format!(
            "id {id:?} is invalid: set `{{type}}:{{name}}` with a known type (field `id`)"
        ));
    };
    let fields = match create_fields(&id, &kind, &name, &body) {
        Ok(fields) => fields,
        Err(response) => return *response,
    };
    store_created(&state.listener_store, &id, fields)
}

/// `PUT /listeners/{id}`: replace the mutable fields of one listener.
/// The id, name and type never change through this route.
pub async fn update_listener(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(mut record) = state.listener_store.get(&id) else {
        return ApiError::BadListenerId(format!("listener {id:?} not found")).into_response();
    };
    if let Some(body_id) = body.get("id").and_then(|v| v.as_str()) {
        if body_id != record.id {
            return ApiError::BadRequest(format!(
                "body id {body_id:?} differs from path id {:?} (field `id`)",
                record.id
            ))
            .into_response();
        }
    }
    if let Some(body_kind) = body.get("type").and_then(|v| v.as_str()) {
        if body_kind != record.kind {
            return ApiError::BadRequest(format!(
                "body type {body_kind:?} differs from listener type {:?} (field `type`)",
                record.kind
            ))
            .into_response();
        }
    }
    if let Some(body_name) = body.get("name").and_then(|v| v.as_str()) {
        if body_name != record.name {
            return ApiError::BadRequest(format!(
                "body name {body_name:?} differs from listener name {:?} (field `name`)",
                record.name
            ))
            .into_response();
        }
    }
    if body.get("bind").is_some() {
        let bind = match read_bind(&body) {
            Ok(bind) => bind,
            Err(response) => return *response,
        };
        // A new bind on an open kernel socket rebinds at once. A bind
        // that fails to open keeps the old bind and stops the row.
        if record.kernel_owned && record.running && bind != record.bind {
            match state.listener_store.controller() {
                Some(control) => {
                    control.stop_listener(&record.id);
                    let mut probe = record.clone();
                    probe.bind = bind.clone();
                    match control.start_listener(&probe) {
                        Ok(()) => record.bind = bind.clone(),
                        Err(detail) => {
                            record.running = false;
                            state.listener_store.update(record.clone());
                            return ApiError::BadRequest(format!(
                                "bind {bind:?} does not open: {detail}"
                            ))
                            .into_response();
                        }
                    }
                }
                // Without a control there is no socket to rebind, so
                // the write only changes the stored bind.
                None => record.bind = bind.clone(),
            }
        } else {
            record.bind = bind;
        }
    }
    if let Some(enable) = body.get("enable").and_then(|v| v.as_bool()) {
        record.enable = enable;
        // Disabling an open kernel socket closes it. Enabling a closed
        // one opens it; a bind that fails to open fails the write.
        if record.kernel_owned {
            if !enable && record.running {
                if let Some(control) = state.listener_store.controller() {
                    control.stop_listener(&record.id);
                }
                record.running = false;
            }
            if enable && !record.running {
                let Some(control) = state.listener_store.controller() else {
                    return ApiError::BadRequest(format!(
                        "listener {:?} control is not ready: the kernel is not serving it",
                        record.id
                    ))
                    .into_response();
                };
                if let Err(detail) = control.start_listener(&record) {
                    return ApiError::BadRequest(format!(
                        "listener {:?} does not open: {detail}",
                        record.id
                    ))
                    .into_response();
                }
                record.running = true;
            }
        }
    }
    if body.get("max_connections").is_some() {
        match parse_max_connections(body.get("max_connections")) {
            Ok(value) => record.max_connections = value,
            Err(()) => {
                return ApiError::BadRequest(
                    "max_connections is invalid: set a count of at least 1 or `infinity` (field `max_connections`)".to_string(),
                )
                    .into_response();
            }
        }
    }
    if body.get("acceptors").is_some() {
        match parse_acceptors(body.get("acceptors")) {
            Ok(value) => record.acceptors = value,
            Err(()) => {
                return ApiError::BadRequest(
                    "acceptors is invalid: set a count of at least 1 (field `acceptors`)"
                        .to_string(),
                )
                .into_response();
            }
        }
    }
    if body.get("zone").is_some() {
        record.zone = match read_zone(&body) {
            Ok(zone) => zone,
            Err(response) => return *response,
        };
    }
    if body.get("mountpoint").is_some() {
        record.mountpoint = match read_mountpoint(&body) {
            Ok(mountpoint) => mountpoint,
            Err(response) => return *response,
        };
    }
    if body.get("tcp_options").is_some() {
        match read_tcp_options(&body) {
            Ok((options, backlog)) => {
                record.tcp_options = options;
                if let Some(backlog) = backlog {
                    // Stored now, bound at the next start.
                    record.backlog = backlog;
                }
            }
            Err(response) => return *response,
        }
    }
    if body.get("ssl_options").is_some() {
        match read_ssl_options(&body) {
            Ok((options, cert, key)) => {
                record.ssl_options = options;
                if let Some(path) = cert {
                    record.cert_file = path;
                }
                if let Some(path) = key {
                    record.key_file = path;
                }
            }
            Err(response) => return *response,
        }
    }
    if body.get("websocket").is_some() {
        record.websocket = match read_websocket(&body) {
            Ok(options) => options,
            Err(response) => return *response,
        };
    }
    if body.get("ciphers").is_some() {
        if let Err(response) = check_ciphers(&body) {
            return *response;
        }
    }
    if RATE_LIMIT_FIELDS
        .iter()
        .any(|field| body.get(*field).is_some())
    {
        if let Err(response) = check_rate_limits(&body) {
            return *response;
        }
    }
    state.listener_store.update(record.clone());
    let current = current_count(&state.listener_store, &record);
    (StatusCode::OK, Json(detail_json(&record, current))).into_response()
}

/// `DELETE /listeners/{id}`: remove one listener. A running kernel
/// socket closes first, so its port frees for a later create.
pub async fn delete_listener(State(state): State<ApiState>, Path(id): Path<String>) -> Response {
    let Some(record) = state.listener_store.get(&id) else {
        return ApiError::BadListenerId(format!("listener {id:?} not found")).into_response();
    };
    if record.kernel_owned && record.running {
        if let Some(control) = state.listener_store.controller() {
            control.stop_listener(&record.id);
        }
    }
    state.listener_store.remove(&id);
    StatusCode::NO_CONTENT.into_response()
}

/// Fetch the kernel-owned record for a lifecycle call. Unknown ids
/// answer 400 with `BAD_LISTENER_ID`; edge-owned rows answer 400 with
/// `BAD_REQUEST` and name the edge as the owner.
fn lifecycle_record(store: &ListenerStore, id: &str) -> Result<ListenerRecord, Box<Response>> {
    let Some(record) = store.get(id) else {
        return Err(Box::new(bad_listener_id_400(format!(
            "listener {id:?} not found"
        ))));
    };
    if !record.kernel_owned {
        return Err(Box::new(
            ApiError::BadRequest(format!(
                "listener {id:?} is owned by the edge: this broker cannot change it"
            ))
            .into_response(),
        ));
    }
    Ok(record)
}

/// `POST /listeners/{id}/start`: open the socket of one listener.
/// An open listener answers 200 without new work.
pub async fn start_listener(State(state): State<ApiState>, Path(id): Path<String>) -> Response {
    let mut record = match lifecycle_record(&state.listener_store, &id) {
        Ok(record) => record,
        Err(response) => return *response,
    };
    if record.running {
        return StatusCode::OK.into_response();
    }
    let Some(control) = state.listener_store.controller() else {
        return ApiError::BadRequest(format!(
            "listener {id:?} control is not ready: the kernel is not serving it"
        ))
        .into_response();
    };
    if let Err(detail) = control.start_listener(&record) {
        return ApiError::BadRequest(format!("listener {id:?} does not open: {detail}"))
            .into_response();
    }
    record.running = true;
    state.listener_store.update(record);
    StatusCode::OK.into_response()
}

/// `POST /listeners/{id}/stop`: close the socket of one listener.
/// A closed listener answers 200 without new work.
pub async fn stop_listener(State(state): State<ApiState>, Path(id): Path<String>) -> Response {
    let mut record = match lifecycle_record(&state.listener_store, &id) {
        Ok(record) => record,
        Err(response) => return *response,
    };
    if !record.running {
        return StatusCode::OK.into_response();
    }
    if let Some(control) = state.listener_store.controller() {
        control.stop_listener(&record.id);
    }
    record.running = false;
    state.listener_store.update(record);
    StatusCode::OK.into_response()
}

/// `POST /listeners/{id}/restart`: close and open the socket again.
/// A stop that no start follows answers 400 and leaves the row stopped.
pub async fn restart_listener(State(state): State<ApiState>, Path(id): Path<String>) -> Response {
    let mut record = match lifecycle_record(&state.listener_store, &id) {
        Ok(record) => record,
        Err(response) => return *response,
    };
    let Some(control) = state.listener_store.controller() else {
        return ApiError::BadRequest(format!(
            "listener {id:?} control is not ready: the kernel is not serving it"
        ))
        .into_response();
    };
    control.stop_listener(&record.id);
    if let Err(detail) = control.start_listener(&record) {
        record.running = false;
        state.listener_store.update(record);
        return ApiError::BadRequest(format!("listener {id:?} does not open: {detail}"))
            .into_response();
    }
    record.running = true;
    state.listener_store.update(record);
    StatusCode::OK.into_response()
}

/// `GET /listeners_status`: live status grouped by listener type.
///
/// One entry per type with the sorted ids, the joint enable flag and
/// the joint running flag. Joint rule: enabled only when every row is
/// enabled, running when any row runs. Counts sum over the rows; one
/// `infinity` limit makes the joint limit `infinity`.
pub async fn listeners_status(State(state): State<ApiState>) -> impl IntoResponse {
    let mut by_kind: HashMap<String, Vec<ListenerRecord>> = HashMap::new();
    for record in state.listener_store.list() {
        by_kind.entry(record.kind.clone()).or_default().push(record);
    }
    let mut groups: Vec<serde_json::Value> = Vec::new();
    for kind in ["tcp", "ssl", "ws", "wss", "quic"] {
        let Some(rows) = by_kind.remove(kind) else {
            continue;
        };
        let mut ids: Vec<String> = rows.iter().map(|r| r.id.clone()).collect();
        ids.sort();
        let enable = rows.iter().all(|r| r.enable);
        let running = rows.iter().any(|r| r.running);
        let mut current: u64 = 0;
        let mut max_sum: u64 = 0;
        let mut infinite = false;
        let mut has_max = false;
        for record in &rows {
            current += current_count(&state.listener_store, record);
            match record.max_connections {
                Some(ListenerMaxConnections::Finite(count)) => {
                    has_max = true;
                    max_sum += u64::from(count);
                }
                Some(ListenerMaxConnections::Infinite) => {
                    infinite = true;
                }
                None => {}
            }
        }
        let mut status = serde_json::Map::new();
        status.insert("running".to_string(), serde_json::json!(running));
        status.insert(
            "current_connections".to_string(),
            serde_json::json!(current),
        );
        if infinite {
            status.insert("max_connections".to_string(), serde_json::json!("infinity"));
        } else if has_max {
            status.insert("max_connections".to_string(), serde_json::json!(max_sum));
        }
        let mut group = serde_json::Map::new();
        group.insert("type".to_string(), serde_json::json!(kind));
        group.insert("ids".to_string(), serde_json::json!(ids));
        group.insert("enable".to_string(), serde_json::json!(enable));
        let status = serde_json::Value::Object(status);
        group.insert("status".to_string(), status.clone());
        group.insert(
            "node_status".to_string(),
            serde_json::json!([{ "node": local_node_name(), "status": status }]),
        );
        groups.push(serde_json::Value::Object(group));
    }
    (StatusCode::OK, Json(groups))
}

/// Query of `GET /listeners`. The reference API documents `type`.
#[derive(serde::Deserialize, Default)]
pub struct ListenersQuery {
    #[serde(default, rename = "type")]
    kind: Option<String>,
}

#[cfg(test)]
pub(crate) mod test_support {
    //! Loopback socket control for listener lifecycle tests.
    //!
    //! Binds the record bind for real and accepts connections. Closing
    //! drops the socket, so a connect proves the state through the API.

    use super::{ListenerController, ListenerRecord};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    /// Loopback control: one accept task per open listener id.
    pub(crate) struct LoopbackController {
        handles: std::sync::Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
        counts: std::sync::Mutex<HashMap<String, Arc<AtomicU64>>>,
    }

    impl LoopbackController {
        pub(crate) fn new() -> Self {
            Self {
                handles: std::sync::Mutex::new(HashMap::new()),
                counts: std::sync::Mutex::new(HashMap::new()),
            }
        }
    }

    impl ListenerController for LoopbackController {
        fn start_listener(&self, record: &ListenerRecord) -> Result<(), String> {
            let mut handles = self.handles.lock().unwrap_or_else(|p| p.into_inner());
            if handles.contains_key(&record.id) {
                return Ok(());
            }
            let count = Arc::new(AtomicU64::new(0));
            self.counts
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(record.id.clone(), count.clone());
            let std_listener =
                std::net::TcpListener::bind(&record.bind).map_err(|e| e.to_string())?;
            std_listener
                .set_nonblocking(true)
                .map_err(|e| e.to_string())?;
            let listener =
                tokio::net::TcpListener::from_std(std_listener).map_err(|e| e.to_string())?;
            handles.insert(
                record.id.clone(),
                tokio::spawn(async move {
                    loop {
                        let Ok((stream, _)) = listener.accept().await else {
                            continue;
                        };
                        count.fetch_add(1, Ordering::Relaxed);
                        let count = count.clone();
                        tokio::spawn(async move {
                            let mut buf = [0u8; 64];
                            let mut stream = stream;
                            use tokio::io::AsyncReadExt as _;
                            while stream.read(&mut buf).await.map(|n| n > 0).unwrap_or(false) {}
                            count.fetch_sub(1, Ordering::Relaxed);
                        });
                    }
                }),
            );
            Ok(())
        }

        fn stop_listener(&self, id: &str) {
            if let Some(handle) = self
                .handles
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(id)
            {
                handle.abort();
            }
            self.counts
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(id);
        }

        fn connection_count(&self, id: &str) -> u64 {
            self.counts
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(id)
                .map(|count| count.load(Ordering::Relaxed))
                .unwrap_or(0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::LoopbackController;
    use super::*;
    use axum::routing::{get, post};
    use axum::Router;

    fn standalone_state() -> ApiState {
        let engine = std::sync::Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        ApiState::standalone(engine)
    }

    /// Serve the listener routes over HTTP on loopback. No auth layer:
    /// the test serves the routes directly, like the protected router.
    fn test_router(state: ApiState) -> Router {
        Router::new()
            .route("/listeners", get(list_listeners).post(create_listener))
            .route("/listeners_status", get(listeners_status))
            .route(
                "/listeners/:id",
                get(get_listener)
                    .put(update_listener)
                    .post(create_listener_with_id)
                    .delete(delete_listener),
            )
            .route("/listeners/:id/start", post(start_listener))
            .route("/listeners/:id/stop", post(stop_listener))
            .route("/listeners/:id/restart", post(restart_listener))
            .with_state(state)
    }

    /// One ephemeral loopback server for the listener routes.
    struct HttpServer {
        port: u16,
        task: tokio::task::JoinHandle<()>,
    }

    impl HttpServer {
        async fn start(state: ApiState) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind loopback server");
            let port = listener.local_addr().expect("loopback addr").port();
            let task = tokio::spawn(async move {
                axum::serve(listener, test_router(state))
                    .await
                    .expect("serve listener routes");
            });
            Self { port, task }
        }

        async fn call(
            &self,
            method: &str,
            path: &str,
            body: Option<serde_json::Value>,
        ) -> (u16, serde_json::Value) {
            let mut head = format!("{method} {path} HTTP/1.0\r\nHost: test\r\n");
            if let Some(body) = body {
                let raw = serde_json::to_string(&body).expect("encode body");
                head.push_str(&format!(
                    "Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{raw}",
                    raw.len()
                ));
            } else {
                head.push_str("Connection: close\r\n\r\n");
            }
            let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", self.port))
                .await
                .expect("connect loopback server");
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            stream.write_all(head.as_bytes()).await.expect("write");
            let mut buf = Vec::new();
            stream.read_to_end(&mut buf).await.expect("read");
            let text = String::from_utf8(buf).expect("UTF-8");
            let (head, body) = text.split_once("\r\n\r\n").expect("split");
            let status: u16 = head.lines().next().expect("status")[9..12]
                .parse()
                .expect("code");
            let body: serde_json::Value = if body.trim().is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::from_str(body).expect("JSON")
            };
            (status, body)
        }
    }

    impl Drop for HttpServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn response_body(response: impl IntoResponse) -> (StatusCode, serde_json::Value) {
        let response = response.into_response();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 256 * 1024)
            .await
            .expect("listeners body is small and readable");
        let body: serde_json::Value =
            serde_json::from_slice(&bytes).expect("listeners body is JSON");
        (status, body)
    }

    async fn list_all(state: ApiState) -> (StatusCode, serde_json::Value) {
        response_body(list_listeners(State(state), Query(ListenersQuery::default())).await).await
    }

    /// True when one TCP connect to `addr` succeeds within the budget.
    async fn can_connect(addr: &str) -> bool {
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            tokio::net::TcpStream::connect(addr),
        )
        .await
        .map(|result| result.is_ok())
        .unwrap_or(false)
    }

    /// One free loopback port for a test listener bind.
    async fn free_port() -> u16 {
        tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind probe")
            .local_addr()
            .expect("probe addr")
            .port()
    }

    #[tokio::test]
    async fn list_reports_four_configured_rows_with_binds() {
        let state = standalone_state();
        let (status, body) = list_all(state).await;
        assert_eq!(status, StatusCode::OK);
        let rows = body.as_array().expect("listeners list is an array");
        assert_eq!(rows.len(), 4, "one row per MQTT listener section");
        let ids: Vec<&str> = rows
            .iter()
            .map(|r| r["id"].as_str().expect("row carries id"))
            .collect();
        assert_eq!(
            ids,
            vec!["tcp:default", "ssl:default", "ws:default", "wss:default"]
        );
        for row in rows {
            assert!(row["bind"].is_string(), "row carries bind: {row}");
            assert!(row["enable"].is_boolean(), "row carries enable: {row}");
            assert_eq!(row["name"], serde_json::json!("default"));
            let kind = row["type"].as_str().expect("row carries type");
            assert!(["tcp", "ssl", "ws", "wss"].contains(&kind), "type: {kind}");
            assert_eq!(row["id"], serde_json::json!(format!("{kind}:default")));
        }
        // Default configuration: plaintext and WS on, TLS and WSS off.
        let by_id = |id: &str| rows.iter().find(|r| r["id"] == id).expect(id);
        assert_eq!(by_id("tcp:default")["enable"], serde_json::json!(true));
        assert_eq!(by_id("ssl:default")["enable"], serde_json::json!(false));
        assert_eq!(by_id("ws:default")["enable"], serde_json::json!(true));
        assert_eq!(by_id("wss:default")["enable"], serde_json::json!(false));
        // Documented live state: `running` is always present, edge rows
        // report a zero count, and each row carries status and one node
        // entry. Nothing runs without the socket control, so no row
        // claims a number.
        for row in rows {
            assert_eq!(row["running"], serde_json::json!(false));
            assert_eq!(row["current_connections"], serde_json::json!(0));
            assert_eq!(row["status"]["running"], serde_json::json!(false));
            assert_eq!(row["status"]["current_connections"], serde_json::json!(0));
            assert!(
                row["status"]["resolved_address"].is_string(),
                "status carries bind address: {row}"
            );
            assert!(
                row["status"]["resolved_address_from"].is_string(),
                "status carries address source: {row}"
            );
            let nodes = row["node_status"].as_array().expect("one node entry");
            assert_eq!(nodes.len(), 1);
            assert!(nodes[0]["node"].is_string(), "node names itself: {row}");
            assert_eq!(nodes[0]["status"], row["status"]);
            assert!(
                row.get("number").is_none(),
                "stopped row has no number: {row}"
            );
            assert_eq!(row["zone"], serde_json::json!("default"));
            assert_eq!(row["mountpoint"], serde_json::json!(""));
        }
    }

    #[tokio::test]
    async fn list_reflects_custom_binds() {
        let engine = std::sync::Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        let state = ApiState::standalone(engine);
        let mut listeners = broker_config::schema::ListenersConf::default();
        listeners.tcp.bind = "127.0.0.1:11883".to_string();
        listeners.api.bind = "127.0.0.1:28083".to_string();
        listeners.ws.enabled = false;
        state.listener_store.reset_from_config(&listeners);
        let (status, body) = list_all(state).await;
        assert_eq!(status, StatusCode::OK);
        let rows = body.as_array().expect("listeners list is an array");
        let by_id = |id: &str| rows.iter().find(|r| r["id"] == id).expect(id);
        assert_eq!(
            by_id("tcp:default")["bind"],
            serde_json::json!("127.0.0.1:11883")
        );
        assert_eq!(by_id("ws:default")["enable"], serde_json::json!(false));
    }

    #[tokio::test]
    async fn type_query_narrows_the_list() {
        let state = standalone_state();
        let query = Query(ListenersQuery {
            kind: Some("ws".to_string()),
        });
        let (status, body) = response_body(list_listeners(State(state), query).await).await;
        assert_eq!(status, StatusCode::OK);
        let rows = body.as_array().expect("listeners list is an array");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], serde_json::json!("ws:default"));
    }

    #[tokio::test]
    async fn detail_returns_documented_fields_for_tcp_default() {
        let state = standalone_state();
        let server = HttpServer::start(state).await;
        let (status, body) = server.call("GET", "/listeners/tcp:default", None).await;
        assert_eq!(status, 200);
        assert_eq!(body["id"], serde_json::json!("tcp:default"));
        assert_eq!(body["name"], serde_json::json!("default"));
        assert_eq!(body["type"], serde_json::json!("tcp"));
        assert!(body["bind"].is_string(), "detail carries bind: {body}");
        assert!(body["enable"].is_boolean(), "detail carries enable: {body}");
        assert_eq!(body["running"], serde_json::json!(false));
        assert_eq!(body["current_connections"], serde_json::json!(0));
        assert_eq!(body["zone"], serde_json::json!("default"));
        assert_eq!(body["mountpoint"], serde_json::json!(""));
    }

    #[tokio::test]
    async fn detail_unknown_id_is_404_bad_listener_id() {
        let state = standalone_state();
        let server = HttpServer::start(state).await;
        let (status, body) = server.call("GET", "/listeners/tcp:nope", None).await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], serde_json::json!("BAD_LISTENER_ID"));
        let (status, _) = server.call("GET", "/listeners/bogus", None).await;
        assert_eq!(status, 404);
    }

    #[tokio::test]
    async fn create_update_delete_round_trip_over_http() {
        let state = standalone_state();
        let server = HttpServer::start(state).await;
        let body = serde_json::json!({
            "name": "extra",
            "type": "tcp",
            "bind": "127.0.0.1:11884",
            "max_connections": 100,
            "acceptors": 8,
        });
        let (status, created) = server.call("POST", "/listeners", Some(body)).await;
        assert_eq!(status, 200, "create answers 200: {created}");
        assert_eq!(created["id"], serde_json::json!("tcp:extra"));
        assert_eq!(created["bind"], serde_json::json!("127.0.0.1:11884"));
        assert_eq!(created["max_connections"], serde_json::json!(100));
        // Duplicate create fails closed.
        let (status, dup) = server
            .call(
                "POST",
                "/listeners",
                Some(serde_json::json!({
                    "name": "extra", "type": "tcp", "bind": "127.0.0.1:11885",
                })),
            )
            .await;
        assert_eq!(status, 400, "duplicate create fails: {dup}");
        // Update changes the stored bind.
        let (status, updated) = server
            .call(
                "PUT",
                "/listeners/tcp:extra",
                Some(serde_json::json!({ "bind": "127.0.0.1:11886" })),
            )
            .await;
        assert_eq!(status, 200, "update answers 200: {updated}");
        assert_eq!(updated["bind"], serde_json::json!("127.0.0.1:11886"));
        // Bad bind fails closed and keeps the old value.
        let (status, _) = server
            .call(
                "PUT",
                "/listeners/tcp:extra",
                Some(serde_json::json!({ "bind": "not-a-bind" })),
            )
            .await;
        assert_eq!(status, 400);
        let (status, kept) = server.call("GET", "/listeners/tcp:extra", None).await;
        assert_eq!(status, 200);
        assert_eq!(kept["bind"], serde_json::json!("127.0.0.1:11886"));
        // Delete removes the row.
        let (status, _) = server.call("DELETE", "/listeners/tcp:extra", None).await;
        assert_eq!(status, 204);
        let (status, gone) = server.call("GET", "/listeners/tcp:extra", None).await;
        assert_eq!(status, 404, "deleted row is gone: {gone}");
        // Delete of a missing row is 404.
        let (status, _) = server.call("DELETE", "/listeners/tcp:extra", None).await;
        assert_eq!(status, 404);
    }

    #[tokio::test]
    async fn console_create_with_id_matches_path_and_body() {
        let state = standalone_state();
        let server = HttpServer::start(state).await;
        // The console posts the full form to `/listeners/{id}`.
        let (status, created) = server
            .call(
                "POST",
                "/listeners/tcp:console",
                Some(serde_json::json!({
                    "id": "tcp:console",
                    "name": "console",
                    "type": "tcp",
                    "bind": "127.0.0.1:11887",
                    "acceptors": 16,
                    "enable": true,
                    "max_connections": 102400,
                })),
            )
            .await;
        assert_eq!(status, 200, "console create answers 200: {created}");
        assert_eq!(created["id"], serde_json::json!("tcp:console"));
        // Body id that differs from the path id fails closed.
        let (status, _) = server
            .call(
                "POST",
                "/listeners/tcp:other",
                Some(serde_json::json!({
                    "id": "tcp:console",
                    "name": "other",
                    "type": "tcp",
                    "bind": "127.0.0.1:11888",
                })),
            )
            .await;
        assert_eq!(status, 400);
        let (status, _) = server.call("DELETE", "/listeners/tcp:console", None).await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn lifecycle_on_edge_listener_answers_400() {
        let state = standalone_state();
        let server = HttpServer::start(state).await;
        // The default rows belong to the edge: lifecycle calls fail.
        for action in ["start", "stop", "restart"] {
            let path = format!("/listeners/tcp:default/{action}");
            let (status, body) = server.call("POST", &path, None).await;
            assert_eq!(status, 400, "{action} on edge row fails: {body}");
            assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
        }
        // Unknown ids fail with the documented listener code.
        for action in ["start", "stop", "restart"] {
            let path = format!("/listeners/tcp:missing/{action}");
            let (status, body) = server.call("POST", &path, None).await;
            assert_eq!(status, 400, "{action} on missing row fails: {body}");
            assert_eq!(body["code"], serde_json::json!("BAD_LISTENER_ID"));
        }
    }

    #[tokio::test]
    async fn kernel_listener_stop_closes_socket_and_start_opens_it() {
        let state = standalone_state();
        state
            .listener_store
            .set_controller(Arc::new(LoopbackController::new()));
        // One kernel-owned row on a free loopback port.
        let port = free_port().await;
        let bind = format!("127.0.0.1:{port}");
        state
            .listener_store
            .insert(ListenerRecord {
                id: "tcp:kernel".to_string(),
                name: "kernel".to_string(),
                kind: "tcp".to_string(),
                bind: bind.clone(),
                enable: true,
                running: false,
                kernel_owned: true,
                max_connections: Some(ListenerMaxConnections::Finite(1000)),
                acceptors: None,
                backlog: 128,
                cert_file: String::new(),
                key_file: String::new(),
                zone: "default".to_string(),
                mountpoint: String::new(),
                tcp_options: None,
                ssl_options: None,
                websocket: None,
            })
            .expect("insert kernel row");
        let server = HttpServer::start(state).await;
        let addr = format!("127.0.0.1:{port}");
        // Closed first: no socket answers.
        assert!(
            !can_connect(&addr).await,
            "stopped listener refuses connections"
        );
        // Start through the API opens the socket.
        let (status, _) = server
            .call("POST", "/listeners/tcp:kernel/start", None)
            .await;
        assert_eq!(status, 200);
        assert!(can_connect(&addr).await, "connect after start works");
        // The detail now reports the row as running.
        let (status, detail) = server.call("GET", "/listeners/tcp:kernel", None).await;
        assert_eq!(status, 200);
        assert_eq!(detail["running"], serde_json::json!(true));
        // Stop through the API closes the socket.
        let (status, _) = server
            .call("POST", "/listeners/tcp:kernel/stop", None)
            .await;
        assert_eq!(status, 200);
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(
            !can_connect(&addr).await,
            "stopped listener refuses connections again"
        );
        // Restart opens it again.
        let (status, _) = server
            .call("POST", "/listeners/tcp:kernel/restart", None)
            .await;
        assert_eq!(status, 200);
        assert!(can_connect(&addr).await, "connect after restart works");
        // The status group reports the running row.
        let (status, groups) = server.call("GET", "/listeners_status", None).await;
        assert_eq!(status, 200);
        let tcp = groups
            .as_array()
            .expect("status is an array")
            .iter()
            .find(|g| g.get("type").and_then(|v| v.as_str()) == Some("tcp"))
            .expect("tcp group");
        assert!(
            tcp["ids"]
                .as_array()
                .expect("ids")
                .iter()
                .any(|id| id.as_str() == Some("tcp:kernel")),
            "group lists the kernel row: {tcp}"
        );
    }

    /// Read the detail `current_connections` of one row over HTTP.
    async fn detail_count(server: &HttpServer, id: &str) -> u64 {
        let (status, detail) = server.call("GET", &format!("/listeners/{id}"), None).await;
        assert_eq!(status, 200, "detail of {id} answers 200");
        detail["current_connections"]
            .as_u64()
            .expect("a live count")
    }

    /// Wait until the detail count of `id` reaches `want` over HTTP.
    async fn wait_for_count(server: &HttpServer, id: &str, want: u64) {
        for _ in 0..100 {
            if detail_count(server, id).await == want {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_eq!(
            detail_count(server, id).await,
            want,
            "live count of {id} reaches {want}"
        );
    }

    #[tokio::test]
    async fn kernel_row_reports_live_connection_count() {
        let state = standalone_state();
        state
            .listener_store
            .set_controller(Arc::new(LoopbackController::new()));
        let port = free_port().await;
        let bind = format!("127.0.0.1:{port}");
        state
            .listener_store
            .insert(ListenerRecord {
                id: "tcp:counted".to_string(),
                name: "counted".to_string(),
                kind: "tcp".to_string(),
                bind: bind.clone(),
                enable: true,
                running: false,
                kernel_owned: true,
                max_connections: Some(ListenerMaxConnections::Finite(100)),
                acceptors: None,
                backlog: 128,
                cert_file: String::new(),
                key_file: String::new(),
                zone: "default".to_string(),
                mountpoint: String::new(),
                tcp_options: None,
                ssl_options: None,
                websocket: None,
            })
            .expect("insert kernel row");
        let server = HttpServer::start(state).await;
        let (status, _) = server
            .call("POST", "/listeners/tcp:counted/start", None)
            .await;
        assert_eq!(status, 200);
        assert_eq!(detail_count(&server, "tcp:counted").await, 0);
        let first = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .expect("first client connects");
        wait_for_count(&server, "tcp:counted", 1).await;
        let second = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .expect("second client connects");
        wait_for_count(&server, "tcp:counted", 2).await;
        // The list row and the status group agree with the detail.
        let (status, rows) = server.call("GET", "/listeners", None).await;
        assert_eq!(status, 200);
        let row = rows
            .as_array()
            .expect("list is an array")
            .iter()
            .find(|r| r["id"] == serde_json::json!("tcp:counted"))
            .expect("counted row");
        assert_eq!(row["current_connections"], serde_json::json!(2));
        assert_eq!(row["status"]["current_connections"], serde_json::json!(2));
        assert_eq!(row["number"], serde_json::json!(1));
        drop(first);
        wait_for_count(&server, "tcp:counted", 1).await;
        drop(second);
        wait_for_count(&server, "tcp:counted", 0).await;
    }

    #[tokio::test]
    async fn unsupported_write_fields_fail_closed() {
        let state = standalone_state();
        let store = state.listener_store.clone();
        let server = HttpServer::start(state).await;
        let base = |name: &str| {
            serde_json::json!({
                "name": name,
                "type": "tcp",
                "bind": "127.0.0.1:11884",
            })
        };
        let with = |name: &str, field: &str, value: serde_json::Value| {
            let mut body = base(name);
            body[field] = value;
            body
        };
        // Each field the broker does not apply fails the write.
        for (name, field, value) in [
            ("zone-bad", "zone", serde_json::json!("east")),
            ("mount-bad", "mountpoint", serde_json::json!("/tenant")),
            ("rate-bad", "bytes_rate", serde_json::json!("100KB/s")),
            ("burst-bad", "max_conn_burst", serde_json::json!("10/s")),
            (
                "tcp-bad",
                "tcp_options",
                serde_json::json!({ "nodelay": false }),
            ),
            (
                "tcp-backlog-bad",
                "tcp_options",
                serde_json::json!({ "backlog": 0 }),
            ),
            (
                "ssl-bad",
                "ssl_options",
                serde_json::json!({ "verify": "verify_peer" }),
            ),
            (
                "ws-bad",
                "websocket",
                serde_json::json!({ "mqtt_path": "/other" }),
            ),
            (
                "cipher-bad",
                "ciphers",
                serde_json::json!(["TLS_AES_256_GCM_SHA384"]),
            ),
        ] {
            let (status, body) = server
                .call("POST", "/listeners", Some(with(name, field, value)))
                .await;
            assert_eq!(status, 400, "{field} fails closed: {body}");
            assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
        }
        // Documented defaults and empty states succeed and round-trip.
        let (status, created) = server
            .call(
                "POST",
                "/listeners",
                Some(serde_json::json!({
                    "name": "opts-ok",
                    "type": "tcp",
                    "bind": "127.0.0.1:11894",
                    "zone": "default",
                    "mountpoint": "",
                    "tcp_options": { "backlog": 2048, "nodelay": true },
                    "ssl_options": { "verify": "verify_none" },
                    "websocket": {},
                    "ciphers": [],
                })),
            )
            .await;
        assert_eq!(status, 200, "defaults succeed: {created}");
        assert_eq!(created["zone"], serde_json::json!("default"));
        assert_eq!(created["mountpoint"], serde_json::json!(""));
        assert_eq!(created["tcp_options"]["backlog"], serde_json::json!(2048));
        // The backlog applies to the stored row for the next bind.
        assert_eq!(store.get("tcp:opts-ok").expect("stored row").backlog, 2048);
        let (status, _) = server.call("DELETE", "/listeners/tcp:opts-ok", None).await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn create_stores_ssl_paths_and_numeric_bind() {
        let state = standalone_state();
        let server = HttpServer::start(state).await;
        // The console sends a bare port as a number.
        let port = free_port().await;
        let (status, created) = server
            .call(
                "POST",
                "/listeners",
                Some(serde_json::json!({
                    "name": "num-bind",
                    "type": "tcp",
                    "bind": port,
                })),
            )
            .await;
        assert_eq!(status, 200, "numeric bind succeeds: {created}");
        assert_eq!(created["bind"], serde_json::json!(port.to_string()));
        // Certificate paths round-trip into the stored row.
        let (status, created) = server
            .call(
                "POST",
                "/listeners",
                Some(serde_json::json!({
                    "name": "cert-ok",
                    "type": "ssl",
                    "bind": "127.0.0.1:11895",
                    "ssl_options": {
                        "certfile": "/tmp/listener-cert.pem",
                        "keyfile": "/tmp/listener-key.pem",
                    },
                })),
            )
            .await;
        assert_eq!(status, 200, "certificate paths succeed: {created}");
        let (status, detail) = server.call("GET", "/listeners/ssl:cert-ok", None).await;
        assert_eq!(status, 200);
        assert_eq!(
            detail["ssl_options"]["certfile"],
            serde_json::json!("/tmp/listener-cert.pem")
        );
    }

    #[tokio::test]
    async fn update_validates_new_fields_and_keeps_old_on_error() {
        let state = standalone_state();
        let server = HttpServer::start(state).await;
        let (status, _) = server
            .call(
                "POST",
                "/listeners",
                Some(serde_json::json!({
                    "name": "put-opts",
                    "type": "tcp",
                    "bind": "127.0.0.1:11896",
                })),
            )
            .await;
        assert_eq!(status, 200);
        // Supported values apply and round-trip.
        let (status, updated) = server
            .call(
                "PUT",
                "/listeners/tcp:put-opts",
                Some(serde_json::json!({
                    "zone": "default",
                    "mountpoint": "",
                    "tcp_options": { "backlog": 4096 },
                })),
            )
            .await;
        assert_eq!(status, 200, "supported update succeeds: {updated}");
        assert_eq!(updated["tcp_options"]["backlog"], serde_json::json!(4096));
        // Unsupported values fail and keep the stored row.
        let (status, denied) = server
            .call(
                "PUT",
                "/listeners/tcp:put-opts",
                Some(serde_json::json!({ "zone": "east" })),
            )
            .await;
        assert_eq!(status, 400, "foreign zone fails: {denied}");
        let (status, denied) = server
            .call(
                "PUT",
                "/listeners/tcp:put-opts",
                Some(serde_json::json!({ "bytes_rate": "1KB/s" })),
            )
            .await;
        assert_eq!(status, 400, "limiter fails: {denied}");
        let (status, kept) = server.call("GET", "/listeners/tcp:put-opts", None).await;
        assert_eq!(status, 200);
        assert_eq!(kept["zone"], serde_json::json!("default"));
        assert_eq!(kept["tcp_options"]["backlog"], serde_json::json!(4096));
    }

    #[tokio::test]
    async fn status_groups_carry_joint_counts() {
        let state = standalone_state();
        let server = HttpServer::start(state).await;
        let (status, groups) = server.call("GET", "/listeners_status", None).await;
        assert_eq!(status, 200);
        // Every group carries status and one node entry, even for rows
        // the edge owns.
        for group in groups.as_array().expect("groups is an array") {
            assert!(
                group["enable"].is_boolean(),
                "group carries enable: {group}"
            );
            assert!(
                group["status"]["running"].is_boolean(),
                "group carries running: {group}"
            );
            assert!(
                group["status"]["current_connections"].is_u64(),
                "group carries a count: {group}"
            );
            let nodes = group["node_status"].as_array().expect("one node entry");
            assert_eq!(nodes.len(), 1);
            assert_eq!(nodes[0]["status"], group["status"]);
        }
    }
}
