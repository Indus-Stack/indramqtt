//! RocketMQ sink.
//!
//! Rule results become vendor messages.
//! All production uses the vendor remoting protocol.
//! Keyed production signs each request with the vendor ACL.
//!
//! Production path:
//! rule engine -> sink -> vendor transport -> nameserver -> broker.
//! The transport fetches the route, then sends each row.
//! The offline path keeps the envelope frame below for unit tests.
//!
//! Response handling classifies remoting codes: 0 commits.
//! All errors restore rows and engage backoff for retry.
//! Batching, restore-on-failure and backoff reuse the shared
//! [`broker_connectors::BatchQueue`] / [`broker_connectors::BackoffState`] helpers.

use async_trait::async_trait;
use broker_protocol::{QoS, Topic};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex as AsyncMutex;

use broker_connectors::{
    hmac_sha1, now_millis, rfc3339_millis, BackoffState, BatchQueue, ConnectorError, Result, Sink,
};

/// Envelope wire version.
pub const ENVELOPE_VERSION: u8 = 0x01;

/// One short TCP handshake per send bounds the down check to 1 s.
// A closed listener fails closed even when an old connection still lives.
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);

// Default batch holds 128 rows.
// Reason: the old default stays, one flush stays small.
const DEFAULT_BATCH_SIZE: usize = 128;
// Default buffer holds 10000 rows.
// Reason: the bound caps memory, 10000 small rows use ~10 MB.
pub const DEFAULT_BUFFER_CAPACITY: usize = 10_000;
// Default linger waits 100 ms.
// Reason: the old default stays, idle batches flush fast.
const DEFAULT_LINGER_MS: u64 = 100;
// Default timeout waits 5000 ms.
// Reason: the vendor send timeout is 3000 ms, route lookup needs margin.
const DEFAULT_TIMEOUT_MS: u64 = 5_000;
// Largest ack or header frame the sink reads.
// Reason: vendor acks are small JSON, a larger frame means a fault.
const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
// Vendor client version.
// Reason: the vendor 5.3.0 line reports version 500.
const REMOTING_VERSION: i32 = 500;
// JSON serialization type.
// Reason: the vendor protocol uses 0 for JSON.
const SERIALIZE_JSON: u32 = 0;
// Vendor request code for route lookup.
// Reason: the vendor nameserver uses 105 for this lookup.
const REQUEST_GET_ROUTEINFO: i32 = 105;
// Vendor request code for send.
// Reason: the vendor broker uses 10 for send.
const REQUEST_SEND_MESSAGE: i32 = 10;
// Vendor success code.
// Reason: the vendor uses 0 for success.
const RESPONSE_SUCCESS: i32 = 0;
// Vendor bad message code.
// Reason: the vendor uses 13 for an illegal message.
const RESPONSE_MESSAGE_ILLEGAL: i32 = 13;
// Vendor denied code.
// Reason: the vendor uses 16 for no permission.
const RESPONSE_NO_PERMISSION: i32 = 16;
// Vendor missing topic code.
// Reason: the vendor uses 17 for a missing topic.
const RESPONSE_TOPIC_NOT_EXIST: i32 = 17;
// Fixed producer group.
// Reason: the vendor needs a group, one fixed group avoids a setting.
const PRODUCER_GROUP: &str = "indra_producer";
// Vendor auto-create key.
// Reason: the vendor uses TBW102 for auto-create.
const DEFAULT_TOPIC_KEY: &str = "TBW102";
// Vendor default queue count.
// Reason: the vendor creates 4 queues for a new topic.
const DEFAULT_QUEUE_NUMS: u32 = 4;
// Redial count per exchange.
// Reason: one retry survives a dropped socket.
const EXCHANGE_ATTEMPTS: usize = 2;
// Broker pushes to skip per exchange.
// Reason: one heartbeat wakes one push, 8 covers bursts.
const MAX_PUSH_SKIPS: usize = 8;

/// Decoded header: code, fields, remark, flag, opaque.
type DecodedRemoting = (i32, HashMap<String, String>, String, i32, i32);

/// Exchange result: code, fields, body, remark.
type RemotingResult = (i32, HashMap<String, String>, Vec<u8>, String);

/// RocketMQ sink configuration.
/// Each depth has a finite default.
/// `None` opts into no bound explicitly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RocketMqSinkConfig {
    /// Nameserver endpoints, e.g. `["127.0.0.1:9876"]`.
    pub endpoints: Vec<String>,
    /// Target RocketMQ topic string.
    pub topic: String,
    /// Message tag template, e.g. `${topic_segment_2}`.
    #[serde(default)]
    pub tag_template: Option<String>,
    /// Business message keys template, e.g.
    /// `${client_id}-${message_id}` (`${message_id}` = per-event seq).
    /// Absent for `roundrobin` dispatch.
    #[serde(default)]
    pub keys_template: Option<String>,
    /// Dispatch strategy: `roundrobin` (default, the reference
    /// default) or `key_dispatch`. `key_dispatch` hashes the keys.
    /// The other sends with no keys so the proxy spreads rows.
    #[serde(default = "default_strategy")]
    pub strategy: String,
    /// FIFO message group template for ordered delivery.
    #[serde(default)]
    pub message_group_template: Option<String>,
    /// ACL access key (both keys required for signing).
    #[serde(default)]
    pub access_key: Option<String>,
    /// ACL secret key for HMAC-SHA1 request signing.
    #[serde(default)]
    pub secret_key: Option<String>,
    /// ACL session token; sent only when set.
    #[serde(default)]
    pub security_token: Option<String>,
    /// Flush trigger record count (default 128).
    #[serde(default = "default_batch_size")]
    pub batch_size: Option<usize>,
    /// Buffer capacity (default 10000).
    #[serde(default = "default_buffer_capacity")]
    pub buffer_capacity: Option<usize>,
    /// Linger flush window in ms (default 100).
    #[serde(default = "default_linger_ms")]
    pub linger_ms: Option<u64>,
    /// Network request and connect timeout in ms (default 5000).
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

fn default_batch_size() -> Option<usize> {
    Some(DEFAULT_BATCH_SIZE)
}

fn default_buffer_capacity() -> Option<usize> {
    Some(DEFAULT_BUFFER_CAPACITY)
}

fn default_linger_ms() -> Option<u64> {
    Some(DEFAULT_LINGER_MS)
}

fn default_strategy() -> String {
    "roundrobin".to_string()
}

impl RocketMqSinkConfig {
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS).max(1))
    }

    pub fn validate(&self) -> Result<()> {
        if self.endpoints.is_empty() || self.endpoints.iter().any(|e| e.trim().is_empty()) {
            return Err(ConnectorError::Dispatch(
                "rocketmq endpoints must not be empty".to_string(),
            ));
        }
        if self.topic.trim().is_empty() {
            return Err(ConnectorError::Dispatch(
                "rocketmq topic must not be empty".to_string(),
            ));
        }
        if self.topic.contains([' ', '\0']) {
            return Err(ConnectorError::Dispatch(format!(
                "rocketmq topic must not contain whitespace: {:?}",
                self.topic
            )));
        }
        if !["roundrobin", "key_dispatch"].contains(&self.strategy.as_str()) {
            return Err(ConnectorError::Dispatch(format!(
                "rocketmq strategy must be one of roundrobin, key_dispatch: {:?}",
                self.strategy
            )));
        }
        match (&self.access_key, &self.secret_key) {
            (Some(ak), Some(sk)) => {
                if ak.trim().is_empty() || sk.is_empty() {
                    return Err(ConnectorError::Dispatch(
                        "rocketmq access_key/secret_key must not be empty".to_string(),
                    ));
                }
            }
            (None, None) => {}
            _ => {
                return Err(ConnectorError::Dispatch(
                    "rocketmq access_key and secret_key must be set together".to_string(),
                ))
            }
        }
        if let Some(token) = &self.security_token {
            if self.access_key.is_none() || self.secret_key.is_none() {
                return Err(ConnectorError::Dispatch(
                    "rocketmq security_token needs access_key and secret_key".to_string(),
                ));
            }
            if token.is_empty() {
                return Err(ConnectorError::Dispatch(
                    "rocketmq security_token must not be empty".to_string(),
                ));
            }
        }
        // Strict template checks with dummy values.
        if let Some(template) = &self.tag_template {
            resolve_system_field(template, "dummy/t", "dummy", 0)?;
        }
        if let Some(template) = &self.keys_template {
            resolve_system_field(template, "dummy/t", "dummy", 0)?;
        }
        if let Some(template) = &self.message_group_template {
            resolve_system_field(template, "dummy/t", "dummy", 0)?;
        }
        if self.batch_size == Some(0) {
            return Err(ConnectorError::Dispatch(
                "rocketmq batch_size must be >= 1".to_string(),
            ));
        }
        Ok(())
    }

    pub fn signing_enabled(&self) -> bool {
        self.access_key.is_some() && self.secret_key.is_some()
    }

    pub(crate) fn effective_batch_size(&self) -> usize {
        self.batch_size.unwrap_or(usize::MAX)
    }

    pub(crate) fn effective_buffer(&self) -> usize {
        self.buffer_capacity.unwrap_or(usize::MAX)
    }

    pub(crate) fn linger(&self) -> Duration {
        Duration::from_millis(self.linger_ms.unwrap_or(DEFAULT_LINGER_MS).max(1))
    }
}

// ---------------------------------------------------------------------------
// SystemProperties templates.
// ---------------------------------------------------------------------------

/// Resolve a system field template: `${topic}`, `${topic_segment_N}`
/// (1-based), `${client_id}`, `${message_id}` (per-event sequence).
/// Anything else is a dispatch error so typos fail loudly.
pub fn resolve_system_field(
    template: &str,
    topic: &str,
    client_id: &str,
    message_id: u64,
) -> Result<String> {
    let segments: Vec<&str> = topic.split('/').collect();
    let mut vars = vec![
        ("topic".to_string(), topic.to_string()),
        ("client_id".to_string(), client_id.to_string()),
        ("message_id".to_string(), message_id.to_string()),
    ];
    for (index, segment) in segments.iter().enumerate() {
        vars.push((format!("topic_segment_{}", index + 1), segment.to_string()));
    }
    let mut out = String::with_capacity(template.len() + 16);
    let bytes = template.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            let start = i + 2;
            let mut end = start;
            while end < bytes.len() && bytes[end] != b'}' {
                end += 1;
            }
            if end >= bytes.len() {
                return Err(ConnectorError::Dispatch(format!(
                    "unclosed template variable in {template:?}"
                )));
            }
            let name = &template[start..end];
            if name.is_empty() {
                return Err(ConnectorError::Dispatch(format!(
                    "empty template variable in {template:?}"
                )));
            }
            match vars.iter().find(|(key, _)| key == name) {
                Some((_, value)) => out.push_str(value),
                None => {
                    return Err(ConnectorError::Dispatch(format!(
                        "unknown template variable {name:?} in {template:?}"
                    )))
                }
            }
            i = end + 1;
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    Ok(out)
}

/// Per-message system properties for the SendMessage envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RocketMqSystemProperties {
    pub tag: Option<String>,
    pub keys: Vec<String>,
    pub message_group: Option<String>,
    pub born_timestamp_ms: i64,
}

/// Build [`RocketMqSystemProperties`] for one event: empty renders
/// become absent (no empty tags/keys/groups on the wire).
pub fn build_system_properties(
    config: &RocketMqSinkConfig,
    topic: &str,
    client_id: &str,
    message_id: u64,
    millis: i64,
) -> Result<RocketMqSystemProperties> {
    let tag = match &config.tag_template {
        Some(template) => {
            let rendered = resolve_system_field(template, topic, client_id, message_id)?;
            if rendered.is_empty() {
                None
            } else {
                Some(rendered)
            }
        }
        None => None,
    };
    let keys = match &config.keys_template {
        Some(template) => {
            let rendered = resolve_system_field(template, topic, client_id, message_id)?;
            if rendered.is_empty() {
                Vec::new()
            } else {
                rendered.split_whitespace().map(str::to_string).collect()
            }
        }
        None => Vec::new(),
    };
    let message_group = match &config.message_group_template {
        Some(template) => {
            let rendered = resolve_system_field(template, topic, client_id, message_id)?;
            if rendered.is_empty() {
                None
            } else {
                Some(rendered)
            }
        }
        None => None,
    };
    Ok(RocketMqSystemProperties {
        tag,
        keys,
        message_group,
        born_timestamp_ms: millis,
    })
}

// ---------------------------------------------------------------------------
// HMAC-SHA1 request signing (clean-room).
// ---------------------------------------------------------------------------

/// Build the signing string: client id, RFC 3339 datetime, topic and
/// the hex MD5 of the envelope body, LF-joined.
pub fn signing_string(client_id: &str, datetime: &str, topic: &str, body_md5_hex: &str) -> String {
    format!("{client_id}\n{datetime}\n{topic}\n{body_md5_hex}")
}

/// Lowercase hex MD5 of `data`.
pub fn md5_hex(data: &[u8]) -> String {
    use md5::Digest;
    let mut digest = md5::Md5::new();
    digest.update(data);
    format!("{:x}", digest.finalize())
}

/// `x-mq-authorization: MQ {access_key}:{Base64(HMAC_SHA1(secret, sts))}`.
pub fn authorization_header(access_key: &str, secret_key: &str, signing_string: &str) -> String {
    use base64::Engine;
    let signature = base64::engine::general_purpose::STANDARD
        .encode(hmac_sha1(secret_key.as_bytes(), signing_string.as_bytes()));
    format!("MQ {access_key}:{signature}")
}

// ---------------------------------------------------------------------------
// Vendor ACL request signing.
// ---------------------------------------------------------------------------

/// Vendor ACL signature for one remoting request.
/// Sort the extension fields by name, join the values in order,
/// append the body bytes, then Base64(HMAC-SHA1(secret, data)).
/// Mirrors the vendor ACL client hook. A stale `Signature` entry
/// never signs itself, so it is skipped.
pub fn acl_signature(secret_key: &str, ext: &HashMap<String, String>, body: &[u8]) -> String {
    use base64::Engine;
    let mut keys: Vec<&String> = ext.keys().collect();
    keys.sort();
    let mut data = Vec::new();
    for key in keys {
        if key == "Signature" {
            continue;
        }
        data.extend_from_slice(ext[key].as_bytes());
    }
    data.extend_from_slice(body);
    base64::engine::general_purpose::STANDARD.encode(hmac_sha1(secret_key.as_bytes(), &data))
}

/// Add vendor ACL fields to one request map.
/// Insert `AccessKey`, insert `SecurityToken` only when set,
/// then compute `Signature` over the rest plus the body.
/// Keyless requests stay unsigned.
fn apply_acl_auth(
    ext: &mut HashMap<String, String>,
    body: &[u8],
    access_key: Option<&str>,
    secret_key: Option<&str>,
    security_token: Option<&str>,
) {
    let (Some(access_key), Some(secret_key)) = (access_key, secret_key) else {
        return;
    };
    ext.insert("AccessKey".to_string(), access_key.to_string());
    if let Some(token) = security_token {
        if !token.is_empty() {
            ext.insert("SecurityToken".to_string(), token.to_string());
        }
    }
    let signature = acl_signature(secret_key, ext, body);
    ext.insert("Signature".to_string(), signature);
}

// ---------------------------------------------------------------------------
// Envelope framing.
// ---------------------------------------------------------------------------

/// One message inside a SendMessage envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocketMqEnvelopeMessage {
    pub system_properties: RocketMqSystemProperties,
    /// Raw payload bytes (base64).
    pub body_b64: String,
}

/// Full SendMessage envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocketMqEnvelope {
    pub topic: String,
    pub client_id: String,
    pub datetime: String,
    pub authorization: Option<String>,
    pub messages: Vec<RocketMqEnvelopeMessage>,
}

/// Encode the envelope frame: version byte + BE length + JSON body.
pub fn encode_envelope(envelope: &RocketMqEnvelope) -> Result<Vec<u8>> {
    let body = serde_json::to_vec(envelope).map_err(|e| {
        ConnectorError::Dispatch(format!("rocketmq envelope is not JSON-encodable: {e}"))
    })?;
    let mut frame = Vec::with_capacity(5 + body.len());
    frame.push(ENVELOPE_VERSION);
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    Ok(frame)
}

/// Decode one envelope frame back (length-checked).
pub fn decode_envelope(frame: &[u8]) -> Result<RocketMqEnvelope> {
    if frame.len() < 5 {
        return Err(ConnectorError::Dispatch(
            "rocketmq frame shorter than the 5-byte prefix".to_string(),
        ));
    }
    if frame[0] != ENVELOPE_VERSION {
        return Err(ConnectorError::Dispatch(format!(
            "rocketmq frame has bad version byte {:#04x}",
            frame[0]
        )));
    }
    let len = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
    if frame.len() - 5 != len {
        return Err(ConnectorError::Dispatch(format!(
            "rocketmq frame length mismatch: header says {len}, got {}",
            frame.len() - 5
        )));
    }
    serde_json::from_slice(&frame[5..])
        .map_err(|e| ConnectorError::Dispatch(format!("rocketmq envelope body is not JSON: {e}")))
}

// ---------------------------------------------------------------------------
// Status classification + FIFO hashing.
// ---------------------------------------------------------------------------

/// RocketMQ 5.0 `Status.Code` relevant to producers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RocketMqStatus {
    Ok,
    TooManyRequests,
    MasterNotAvailable,
    IllegalTopic,
    AccessDenied,
    MessageBodyTooLarge,
    Unknown(i32),
}

impl RocketMqStatus {
    /// Toy-protocol status codes for the JSON acknowledgement frame
    /// (`{"code":N}`): 0 OK, 1 TOO_MANY_REQUESTS, 2
    /// MASTER_NOT_AVAILABLE, 3 ILLEGAL_TOPIC, 4 ACCESS_DENIED, 5
    /// MESSAGE_BODY_TOO_LARGE.
    pub fn from_code(code: i32) -> Self {
        match code {
            0 => RocketMqStatus::Ok,
            1 => RocketMqStatus::TooManyRequests,
            2 => RocketMqStatus::MasterNotAvailable,
            3 => RocketMqStatus::IllegalTopic,
            4 => RocketMqStatus::AccessDenied,
            5 => RocketMqStatus::MessageBodyTooLarge,
            other => RocketMqStatus::Unknown(other),
        }
    }
}

/// Classified send outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RocketMqOutcome {
    Success,
    Retryable,
    Terminal,
}

/// `OK` commits; `TOO_MANY_REQUESTS` / `MASTER_NOT_AVAILABLE` retry
/// with backoff; `ILLEGAL_TOPIC` / `ACCESS_DENIED` (and oversized
/// bodies) abort terminally; unknown codes retry conservatively.
pub fn classify_status(status: RocketMqStatus) -> RocketMqOutcome {
    match status {
        RocketMqStatus::Ok => RocketMqOutcome::Success,
        RocketMqStatus::TooManyRequests | RocketMqStatus::MasterNotAvailable => {
            RocketMqOutcome::Retryable
        }
        RocketMqStatus::IllegalTopic
        | RocketMqStatus::AccessDenied
        | RocketMqStatus::MessageBodyTooLarge => RocketMqOutcome::Terminal,
        RocketMqStatus::Unknown(_) => RocketMqOutcome::Retryable,
    }
}

/// FNV-1a 32-bit hash for FIFO message-group queue selection.
pub fn fnv1a_32(data: &[u8]) -> u32 {
    let mut hash = 0x811c9dc5u32;
    for &byte in data {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

/// Queue index for a FIFO group (`None` group = queue 0, unordered).
pub fn fifo_partition(message_group: Option<&str>, queue_count: u32) -> u32 {
    let queues = queue_count.max(1);
    match message_group {
        Some(group) => fnv1a_32(group.as_bytes()) % queues,
        None => 0,
    }
}

// ---------------------------------------------------------------------------
// Transport.
// ---------------------------------------------------------------------------

/// One queued message: envelope properties + raw body.
#[derive(Debug, Clone)]
pub struct RocketMqMessage {
    pub properties: RocketMqSystemProperties,
    pub body: Vec<u8>,
}

#[async_trait]
pub trait RocketMqTransport: Send + Sync {
    async fn send_messages(
        &self,
        topic: &str,
        client_id: &str,
        datetime: &str,
        authorization: Option<&str>,
        messages: Vec<RocketMqMessage>,
    ) -> Result<()>;
}

/// In-memory transport recording every envelope (tests, dry runs).
#[derive(Debug, Default)]
pub struct MockRocketMqTransport {
    envelopes: parking_lot::Mutex<Vec<RocketMqEnvelope>>,
    failures_left: parking_lot::Mutex<usize>,
    calls: AtomicU64,
}

impl MockRocketMqTransport {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fail the next `n` sends with a transport error (backoff tests).
    pub fn fail_next(&self, n: usize) {
        *self.failures_left.lock() = n;
    }

    pub fn envelopes(&self) -> Vec<RocketMqEnvelope> {
        self.envelopes.lock().clone()
    }

    pub fn calls(&self) -> u64 {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl RocketMqTransport for MockRocketMqTransport {
    async fn send_messages(
        &self,
        topic: &str,
        client_id: &str,
        datetime: &str,
        authorization: Option<&str>,
        messages: Vec<RocketMqMessage>,
    ) -> Result<()> {
        use base64::Engine;
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut failures = self.failures_left.lock();
        if *failures > 0 {
            *failures -= 1;
            return Err(ConnectorError::Connection("mock rocketmq down".to_string()));
        }
        self.envelopes.lock().push(RocketMqEnvelope {
            topic: topic.to_string(),
            client_id: client_id.to_string(),
            datetime: datetime.to_string(),
            authorization: authorization.map(str::to_string),
            messages: messages
                .into_iter()
                .map(|message| RocketMqEnvelopeMessage {
                    system_properties: message.properties,
                    body_b64: base64::engine::general_purpose::STANDARD.encode(&message.body),
                })
                .collect(),
        });
        Ok(())
    }
}

/// TCP transport: one envelope frame per flush, one JSON
/// acknowledgement frame (`{"code":N}`) per reply. One in-flight
/// exchange at a time; dropped connections redial once.
pub struct TcpRocketMqTransport {
    endpoint: String,
    timeout: Duration,
    conn: AsyncMutex<Option<TcpStream>>,
}

impl TcpRocketMqTransport {
    pub fn new(config: &RocketMqSinkConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            endpoint: config.endpoints[0].clone(),
            timeout: config.timeout(),
            conn: AsyncMutex::new(None),
        })
    }

    async fn roundtrip(&self, frame: Vec<u8>) -> Result<Vec<u8>> {
        let mut guard = self.conn.lock().await;
        // Probe the listener before reusing the pooled connection.
        // A refused port clears the cache and fails closed, so the
        // action counts one failed forward.
        if guard.is_some() {
            let reachable = tokio::time::timeout(
                PROBE_TIMEOUT,
                tokio::net::TcpStream::connect(&self.endpoint),
            )
            .await
            .ok()
            .and_then(|result| result.ok())
            .is_some();
            if !reachable {
                *guard = None;
                return Err(ConnectorError::Connection(format!(
                    "rocketmq proxy unreachable: {}",
                    self.endpoint
                )));
            }
        }
        for attempt in 0..EXCHANGE_ATTEMPTS {
            if guard.is_none() {
                let stream = tokio::time::timeout(self.timeout, TcpStream::connect(&self.endpoint))
                    .await
                    .map_err(|_| {
                        ConnectorError::Connection(format!(
                            "rocketmq connect timeout: {}",
                            self.endpoint
                        ))
                    })?
                    .map_err(|e| {
                        ConnectorError::Connection(format!(
                            "rocketmq connect to {} failed: {e}",
                            self.endpoint
                        ))
                    })?;
                *guard = Some(stream);
            }
            // INVARIANT: stream is initialized to Some immediately above when None.
            let stream = guard.as_mut().expect("connected");
            let exchange = async {
                stream.write_all(&frame).await?;
                let mut len_buf = [0u8; 4];
                stream.read_exact(&mut len_buf).await?;
                let len = u32::from_be_bytes(len_buf) as usize;
                if len > MAX_FRAME_BYTES {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "rocketmq ack frame too large",
                    ));
                }
                let mut body = vec![0u8; len];
                stream.read_exact(&mut body).await?;
                Ok::<Vec<u8>, std::io::Error>(body)
            };
            match tokio::time::timeout(self.timeout, exchange).await {
                Ok(Ok(body)) => return Ok(body),
                Ok(Err(_)) | Err(_) if attempt == 0 => {
                    *guard = None;
                    continue;
                }
                Ok(Err(e)) => {
                    *guard = None;
                    return Err(ConnectorError::Connection(format!(
                        "rocketmq exchange failed: {e}"
                    )));
                }
                Err(_) => {
                    *guard = None;
                    return Err(ConnectorError::Connection(
                        "rocketmq exchange timed out".to_string(),
                    ));
                }
            }
        }
        Err(ConnectorError::Connection(
            "rocketmq exchange failed".to_string(),
        ))
    }
}

#[async_trait]
impl RocketMqTransport for TcpRocketMqTransport {
    async fn send_messages(
        &self,
        topic: &str,
        client_id: &str,
        datetime: &str,
        authorization: Option<&str>,
        messages: Vec<RocketMqMessage>,
    ) -> Result<()> {
        use base64::Engine;
        let frame = encode_envelope(&RocketMqEnvelope {
            topic: topic.to_string(),
            client_id: client_id.to_string(),
            datetime: datetime.to_string(),
            authorization: authorization.map(str::to_string),
            messages: messages
                .into_iter()
                .map(|message| RocketMqEnvelopeMessage {
                    system_properties: message.properties,
                    body_b64: base64::engine::general_purpose::STANDARD.encode(&message.body),
                })
                .collect(),
        })?;
        // Envelope frames carry their own 5-byte prefix; the ack
        // framing is a bare BE length + JSON `{"code":N}` body.
        let reply = self.roundtrip(frame).await?;
        let ack: serde_json::Value = serde_json::from_slice(&reply)
            .map_err(|e| ConnectorError::Connection(format!("rocketmq ack is not JSON: {e}")))?;
        let code = ack.get("code").and_then(|code| code.as_i64()).unwrap_or(-1) as i32;
        match classify_status(RocketMqStatus::from_code(code)) {
            RocketMqOutcome::Success => Ok(()),
            RocketMqOutcome::Retryable => Err(ConnectorError::Connection(format!(
                "rocketmq {topic} answered retryable code {code}"
            ))),
            RocketMqOutcome::Terminal => Err(ConnectorError::Dispatch(format!(
                "rocketmq {topic} answered terminal code {code}"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Vendor remoting protocol.
// ---------------------------------------------------------------------------

/// Strip URL schemes from an endpoint.
/// Keep host and port only.
fn strip_scheme(endpoint: &str) -> String {
    endpoint
        .trim()
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_start_matches("tcp://")
        .to_string()
}

/// Host part of an address.
/// Return the full address when it has no port.
fn host_of(addr: &str) -> String {
    match addr.rsplit_once(':') {
        Some((host, _)) => host.to_string(),
        None => addr.to_string(),
    }
}

/// Port part of an address.
/// Return empty when it has no port.
fn port_of(addr: &str) -> String {
    match addr.rsplit_once(':') {
        Some((_, port)) => port.to_string(),
        None => String::new(),
    }
}

/// Encode one remoting frame.
/// Layout is total + combined + header + body.
fn encode_remoting(
    code: i32,
    ext: &HashMap<String, String>,
    body: &[u8],
    opaque: i32,
) -> Result<Vec<u8>> {
    let header = serde_json::json!({
        "code": code,
        "language": "JAVA",
        "version": REMOTING_VERSION,
        "opaque": opaque,
        "flag": 0,
        "remark": null,
        "extFields": ext,
    });
    let header_bytes = serde_json::to_vec(&header).map_err(|e| {
        ConnectorError::Dispatch(format!("rocketmq header is not JSON-encodable: {e}"))
    })?;
    let header_len = header_bytes.len();
    if header_len > MAX_FRAME_BYTES {
        return Err(ConnectorError::Dispatch(
            "rocketmq header exceeds the frame bound".to_string(),
        ));
    }
    if body.len() > MAX_FRAME_BYTES {
        return Err(ConnectorError::Dispatch(
            "rocketmq body exceeds the frame bound".to_string(),
        ));
    }
    let combined = (SERIALIZE_JSON << 24) | (header_len as u32);
    let total = (4 + header_len + body.len()) as u32;
    let mut frame = Vec::with_capacity(8 + header_len + body.len());
    frame.extend_from_slice(&total.to_be_bytes());
    frame.extend_from_slice(&combined.to_be_bytes());
    frame.extend_from_slice(&header_bytes);
    frame.extend_from_slice(body);
    Ok(frame)
}

/// Decode one remoting reply.
/// Return code, fields, remark, flag and opaque.
fn decode_remoting(combined: u32, header_bytes: &[u8]) -> Result<DecodedRemoting> {
    let header_len = (combined & 0x00FF_FFFF) as usize;
    if header_len != header_bytes.len() {
        return Err(ConnectorError::Connection(
            "rocketmq header length mismatch".to_string(),
        ));
    }
    let header: serde_json::Value = serde_json::from_slice(header_bytes)
        .map_err(|e| ConnectorError::Connection(format!("rocketmq header is not JSON: {e}")))?;
    let code = header.get("code").and_then(|v| v.as_i64()).unwrap_or(-1) as i32;
    let flag = header.get("flag").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
    let opaque = header.get("opaque").and_then(|v| v.as_i64()).unwrap_or(-1) as i32;
    let remark = header
        .get("remark")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let mut fields = HashMap::new();
    if let Some(map) = header.get("extFields").and_then(|v| v.as_object()) {
        for (key, value) in map {
            if let Some(text) = value.as_str() {
                fields.insert(key.clone(), text.to_string());
            } else {
                fields.insert(key.clone(), value.to_string());
            }
        }
    }
    Ok((code, fields, remark, flag, opaque))
}

/// One remoting exchange.
/// Open a socket, send one frame, read one reply.
/// Skip broker pushes on the same socket.
async fn remoting_exchange(
    addr: &str,
    frame: Vec<u8>,
    expect_opaque: i32,
    timeout: Duration,
) -> Result<RemotingResult> {
    let mut stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| ConnectorError::Connection(format!("rocketmq connect timeout: {addr}")))?
        .map_err(|e| {
            ConnectorError::Connection(format!("rocketmq connect to {addr} failed: {e}"))
        })?;
    let reply = async {
        stream.write_all(&frame).await?;
        for _ in 0..=MAX_PUSH_SKIPS {
            let mut total_buf = [0u8; 4];
            stream.read_exact(&mut total_buf).await?;
            let total = u32::from_be_bytes(total_buf) as usize;
            if !(4..=MAX_FRAME_BYTES + 1024).contains(&total) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "rocketmq frame total out of bound",
                ));
            }
            let mut combined_buf = [0u8; 4];
            stream.read_exact(&mut combined_buf).await?;
            let combined = u32::from_be_bytes(combined_buf);
            let header_len = (combined & 0x00FF_FFFF) as usize;
            if header_len > MAX_FRAME_BYTES || header_len + 4 > total {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "rocketmq header out of bound",
                ));
            }
            let body_len = total - 4 - header_len;
            if body_len > MAX_FRAME_BYTES {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "rocketmq body out of bound",
                ));
            }
            let mut header_bytes = vec![0u8; header_len];
            stream.read_exact(&mut header_bytes).await?;
            let mut body = vec![0u8; body_len];
            if body_len > 0 {
                stream.read_exact(&mut body).await?;
            }
            // Broker push is a request on the same socket.
            // Reason: heartbeat register wakes a notify.
            let header: serde_json::Value =
                serde_json::from_slice(&header_bytes).map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "rocketmq header is not JSON",
                    )
                })?;
            let flag = header.get("flag").and_then(|v| v.as_i64()).unwrap_or(0);
            let opaque = header.get("opaque").and_then(|v| v.as_i64()).unwrap_or(-1);
            if (flag & 1) == 0 {
                continue;
            }
            if opaque != expect_opaque as i64 {
                continue;
            }
            return Ok::<(u32, u32, Vec<u8>, Vec<u8>), std::io::Error>((
                total as u32,
                combined,
                header_bytes,
                body,
            ));
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "rocketmq exchange got only pushes",
        ))
    };
    let (_, combined, header_bytes, body) = tokio::time::timeout(timeout, reply)
        .await
        .map_err(|_| ConnectorError::Connection("rocketmq exchange timed out".to_string()))?
        .map_err(|e| ConnectorError::Connection(format!("rocketmq exchange failed: {e}")))?;
    let (code, fields, remark, _, _) = decode_remoting(combined, &header_bytes)?;
    Ok((code, fields, body, remark))
}

/// One exchange on an open socket.
/// Send one frame, read one reply on the same stream.
/// Skip broker pushes on the same socket.
/// Keep the heartbeat and the pulls on one stream.
/// Reason: the broker ties the group to the channel.
#[cfg(test)]
async fn remoting_exchange_on_stream(
    stream: &mut TcpStream,
    frame: Vec<u8>,
    expect_opaque: i32,
    timeout: Duration,
) -> Result<RemotingResult> {
    let reply = async {
        stream.write_all(&frame).await?;
        for _ in 0..=MAX_PUSH_SKIPS {
            let mut total_buf = [0u8; 4];
            stream.read_exact(&mut total_buf).await?;
            let total = u32::from_be_bytes(total_buf) as usize;
            if !(4..=MAX_FRAME_BYTES + 1024).contains(&total) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "rocketmq frame total out of bound",
                ));
            }
            let mut combined_buf = [0u8; 4];
            stream.read_exact(&mut combined_buf).await?;
            let combined = u32::from_be_bytes(combined_buf);
            let header_len = (combined & 0x00FF_FFFF) as usize;
            if header_len > MAX_FRAME_BYTES || header_len + 4 > total {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "rocketmq header out of bound",
                ));
            }
            let body_len = total - 4 - header_len;
            if body_len > MAX_FRAME_BYTES {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "rocketmq body out of bound",
                ));
            }
            let mut header_bytes = vec![0u8; header_len];
            stream.read_exact(&mut header_bytes).await?;
            let mut body = vec![0u8; body_len];
            if body_len > 0 {
                stream.read_exact(&mut body).await?;
            }
            // Broker push is a request on the same socket.
            // Reason: heartbeat register wakes a notify.
            let header: serde_json::Value =
                serde_json::from_slice(&header_bytes).map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "rocketmq header is not JSON",
                    )
                })?;
            let flag = header.get("flag").and_then(|v| v.as_i64()).unwrap_or(0);
            let opaque = header.get("opaque").and_then(|v| v.as_i64()).unwrap_or(-1);
            if (flag & 1) == 0 {
                continue;
            }
            if opaque != expect_opaque as i64 {
                continue;
            }
            return Ok::<(u32, u32, Vec<u8>, Vec<u8>), std::io::Error>((
                total as u32,
                combined,
                header_bytes,
                body,
            ));
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "rocketmq exchange got only pushes",
        ))
    };
    let (_, combined, header_bytes, body) = tokio::time::timeout(timeout, reply)
        .await
        .map_err(|_| ConnectorError::Connection("rocketmq exchange timed out".to_string()))?
        .map_err(|e| ConnectorError::Connection(format!("rocketmq exchange failed: {e}")))?;
    let (code, fields, remark, _, _) = decode_remoting(combined, &header_bytes)?;
    Ok((code, fields, body, remark))
}

/// Route result: broker address and queue count.
#[derive(Debug)]
struct BrokerRoute {
    addr: String,
    queues: u32,
}

/// Fetch the broker route from the nameserver.
/// Replace a loopback broker host with the nameserver host.
/// Signs the lookup when keys are set.
async fn fetch_broker_route(
    namesrv: &str,
    topic: &str,
    timeout: Duration,
    opaque: &AtomicI32,
    access_key: Option<&str>,
    secret_key: Option<&str>,
    security_token: Option<&str>,
) -> Result<BrokerRoute> {
    let mut lookup = topic;
    // Missing topics use the default route.
    // Reason: the vendor auto-creates on send.
    for attempt in 0..2 {
        let mut ext = HashMap::new();
        ext.insert("topic".to_string(), lookup.to_string());
        apply_acl_auth(&mut ext, &[], access_key, secret_key, security_token);
        let id = opaque.fetch_add(1, Ordering::SeqCst);
        let frame = encode_remoting(REQUEST_GET_ROUTEINFO, &ext, &[], id)?;
        let (code, _fields, body, remark) = remoting_exchange(namesrv, frame, id, timeout).await?;
        if code == RESPONSE_TOPIC_NOT_EXIST && attempt == 0 && lookup != DEFAULT_TOPIC_KEY {
            lookup = DEFAULT_TOPIC_KEY;
            continue;
        }
        if code != RESPONSE_SUCCESS {
            return Err(ConnectorError::Connection(format!(
                "rocketmq route for {topic} failed code {code}: {remark}"
            )));
        }
        let route: serde_json::Value = serde_json::from_slice(&body)
            .map_err(|e| ConnectorError::Connection(format!("rocketmq route is not JSON: {e}")))?;
        let mut broker_name = String::new();
        let mut broker_addr = String::new();
        if let Some(list) = route.get("brokerDatas").and_then(|v| v.as_array()) {
            for entry in list {
                let name = entry
                    .get("brokerName")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                if let Some(addrs) = entry.get("brokerAddrs").and_then(|v| v.as_object()) {
                    if let Some(addr) = addrs.get("0").and_then(|v| v.as_str()) {
                        broker_name = name;
                        broker_addr = addr.to_string();
                        break;
                    }
                }
            }
        }
        if broker_addr.is_empty() {
            return Err(ConnectorError::Connection(format!(
                "rocketmq route for {topic} has no master"
            )));
        }
        let mut queues = DEFAULT_QUEUE_NUMS;
        if let Some(list) = route.get("queueDatas").and_then(|v| v.as_array()) {
            for entry in list {
                let name = entry
                    .get("brokerName")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if name == broker_name {
                    if let Some(count) = entry.get("writeQueueNums").and_then(|v| v.as_u64()) {
                        if count > 0 {
                            queues = count as u32;
                        }
                    }
                    break;
                }
            }
        }
        // New topics use the vendor queue count.
        // Reason: auto-create uses the send queue count.
        if lookup == DEFAULT_TOPIC_KEY && topic != DEFAULT_TOPIC_KEY {
            queues = DEFAULT_QUEUE_NUMS;
        }
        let host = host_of(&broker_addr);
        let port = port_of(&broker_addr);
        let fixed = if host == "127.0.0.1" || host == "localhost" {
            format!("{}:{port}", host_of(namesrv))
        } else {
            broker_addr
        };
        return Ok(BrokerRoute {
            addr: fixed,
            queues: queues.max(1),
        });
    }
    Err(ConnectorError::Connection(format!(
        "rocketmq route for {topic} failed code {RESPONSE_TOPIC_NOT_EXIST}"
    )))
}

/// Encode vendor message properties.
/// Use NAME_VALUE_SEPARATOR (0x01) and PROPERTY_SEPARATOR (0x02).
fn encode_vendor_properties(tag: Option<&str>, keys: &[String]) -> String {
    let mut out = String::new();
    if let Some(tag) = tag {
        if !tag.is_empty() {
            out.push_str("TAGS");
            out.push('\u{1}');
            out.push_str(tag);
            out.push('\u{2}');
        }
    }
    if !keys.is_empty() {
        let joined = keys.join(" ");
        if !joined.trim().is_empty() {
            out.push_str("KEYS");
            out.push('\u{1}');
            out.push_str(&joined);
            out.push('\u{2}');
        }
    }
    out
}

/// Vendor transport on the remoting protocol.
/// It dials the nameserver, then the broker.
/// It signs each request when keys are set.
pub struct RemotingRocketMqTransport {
    namesrv: String,
    timeout: Duration,
    opaque: AtomicI32,
    access_key: Option<String>,
    secret_key: Option<String>,
    security_token: Option<String>,
}

impl RemotingRocketMqTransport {
    pub fn new(config: &RocketMqSinkConfig) -> Result<Self> {
        config.validate()?;
        let raw = config.endpoints[0].clone();
        let namesrv = strip_scheme(&raw);
        if namesrv.trim().is_empty() {
            return Err(ConnectorError::Dispatch(
                "rocketmq endpoints must not be empty".to_string(),
            ));
        }
        Ok(Self {
            namesrv,
            timeout: config.timeout(),
            opaque: AtomicI32::new(1),
            access_key: config.access_key.clone(),
            secret_key: config.secret_key.clone(),
            security_token: config.security_token.clone(),
        })
    }
}

#[async_trait]
impl RocketMqTransport for RemotingRocketMqTransport {
    async fn send_messages(
        &self,
        topic: &str,
        _client_id: &str,
        _datetime: &str,
        _authorization: Option<&str>,
        messages: Vec<RocketMqMessage>,
    ) -> Result<()> {
        let route = fetch_broker_route(
            &self.namesrv,
            topic,
            self.timeout,
            &self.opaque,
            self.access_key.as_deref(),
            self.secret_key.as_deref(),
            self.security_token.as_deref(),
        )
        .await?;
        for message in &messages {
            let queue = match &message.properties.message_group {
                Some(group) => fifo_partition(Some(group), route.queues),
                None => 0,
            };
            let props = encode_vendor_properties(
                message.properties.tag.as_deref(),
                &message.properties.keys,
            );
            let mut ext = HashMap::new();
            ext.insert(PRODUCER_GROUP.to_string(), PRODUCER_GROUP.to_string());
            ext.insert("topic".to_string(), topic.to_string());
            ext.insert("defaultTopic".to_string(), DEFAULT_TOPIC_KEY.to_string());
            ext.insert(
                "defaultTopicQueueNums".to_string(),
                DEFAULT_QUEUE_NUMS.to_string(),
            );
            ext.insert("queueId".to_string(), queue.to_string());
            ext.insert("sysFlag".to_string(), "0".to_string());
            ext.insert("bornTimestamp".to_string(), now_millis().to_string());
            ext.insert("flag".to_string(), "0".to_string());
            ext.insert("properties".to_string(), props);
            ext.insert("reconsumeTimes".to_string(), "0".to_string());
            ext.insert("unitMode".to_string(), "false".to_string());
            // Keyed sends sign here, never on the envelope path.
            // Reason: production only speaks remoting.
            apply_acl_auth(
                &mut ext,
                &message.body,
                self.access_key.as_deref(),
                self.secret_key.as_deref(),
                self.security_token.as_deref(),
            );
            let id = self.opaque.fetch_add(1, Ordering::SeqCst);
            let frame = encode_remoting(REQUEST_SEND_MESSAGE, &ext, &message.body, id)?;
            let (code, _fields, _body, remark) =
                remoting_exchange(&route.addr, frame, id, self.timeout).await?;
            if code == RESPONSE_SUCCESS {
                continue;
            }
            if code == RESPONSE_MESSAGE_ILLEGAL
                || code == RESPONSE_NO_PERMISSION
                || code == RESPONSE_TOPIC_NOT_EXIST
            {
                return Err(ConnectorError::Dispatch(format!(
                    "rocketmq {topic} refused code {code}: {remark}"
                )));
            }
            return Err(ConnectorError::Connection(format!(
                "rocketmq {topic} to {} failed code {code}: {remark}",
                route.addr
            )));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Sink.
// ---------------------------------------------------------------------------

/// One buffered event: raw topic/payload plus per-event sequence.
#[derive(Debug, Clone)]
struct RocketMqRow {
    topic: String,
    payload: Bytes,
    message_id: u64,
}

/// RocketMQ sink: buffers events, sends one vendor message per row.
/// Each attempt uses fresh timestamps and signatures.
pub struct RocketMqSink {
    config: RocketMqSinkConfig,
    transport: Arc<dyn RocketMqTransport>,
    buffer: parking_lot::Mutex<BatchQueue<RocketMqRow>>,
    backoff: parking_lot::Mutex<BackoffState>,
    client_id: String,
    message_seq: AtomicU64,
    sent_batches: AtomicU64,
    sent_records: AtomicU64,
}

impl RocketMqSink {
    pub fn new(config: RocketMqSinkConfig, transport: Arc<dyn RocketMqTransport>) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            client_id: format!("indra-rmq-{}", uuid::Uuid::new_v4()),
            buffer: parking_lot::Mutex::new(BatchQueue::new(
                config.effective_batch_size(),
                config.linger(),
            )),
            config,
            transport,
            backoff: parking_lot::Mutex::new(BackoffState::default()),
            message_seq: AtomicU64::new(0),
            sent_batches: AtomicU64::new(0),
            sent_records: AtomicU64::new(0),
        })
    }

    pub fn config(&self) -> &RocketMqSinkConfig {
        &self.config
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    pub fn sent_batches(&self) -> u64 {
        self.sent_batches.load(Ordering::Relaxed)
    }

    pub fn sent_records(&self) -> u64 {
        self.sent_records.load(Ordering::Relaxed)
    }

    pub fn buffered_rows(&self) -> usize {
        self.buffer.lock().len()
    }

    /// Build transport messages for the pending rows (fresh per attempt
    /// so replays never reuse stale born timestamps... note the born
    /// timestamp rides the flush clock, shared across attempts of one
    /// flush like a broker-side born time).
    fn build_messages(&self, rows: &[RocketMqRow], millis: i64) -> Result<Vec<RocketMqMessage>> {
        let mut messages = Vec::with_capacity(rows.len());
        for row in rows {
            let text = std::str::from_utf8(&row.payload).map_err(|_| {
                ConnectorError::Dispatch("rocketmq payload must be UTF-8".to_string())
            })?;
            let parsed: serde_json::Value =
                serde_json::from_str(text).unwrap_or(serde_json::Value::Null);
            let client_id = parsed
                .get("client_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            messages.push(RocketMqMessage {
                properties: build_system_properties(
                    &self.config,
                    &row.topic,
                    client_id,
                    row.message_id,
                    millis,
                )?,
                body: row.payload.to_vec(),
            });
        }
        Ok(messages)
    }

    /// Flush buffered rows as remoting frames (no-op when empty).
    /// Each flush uses fresh timestamps and signatures.
    /// Any failure restores rows, engages backoff and propagates.
    pub async fn flush(&self) -> Result<()> {
        self.backoff.lock().check()?;
        let (rows, oldest) = {
            let mut buffer = self.buffer.lock();
            buffer.take_batch()
        };
        if rows.is_empty() {
            return Ok(());
        }
        let total = rows.len() as u64;
        let pending: Vec<RocketMqRow> = rows;
        // Single attempt per flush: the datetime + signature are fresh,
        // and any failure restores the rows, engages backoff and
        // propagates (the next flush retries after the backoff window).
        let millis = now_millis();
        let datetime = rfc3339_millis(millis);
        let messages = self.build_messages(&pending, millis)?;
        let body_md5 =
            md5_hex(&serde_json::to_vec(&messages_md5_input(&messages)).unwrap_or_default());
        let authorization = match (&self.config.access_key, &self.config.secret_key) {
            (Some(ak), Some(sk)) => Some(authorization_header(
                ak,
                sk,
                &signing_string(&self.client_id, &datetime, &self.config.topic, &body_md5),
            )),
            _ => None,
        };
        match self
            .transport
            .send_messages(
                &self.config.topic,
                &self.client_id,
                &datetime,
                authorization.as_deref(),
                messages,
            )
            .await
        {
            Ok(()) => {
                self.backoff.lock().success();
                self.sent_batches.fetch_add(1, Ordering::Relaxed);
                self.sent_records.fetch_add(total, Ordering::Relaxed);
                Ok(())
            }
            Err(ConnectorError::Connection(message)) => {
                let mut buffer = self.buffer.lock();
                buffer.restore(pending, oldest);
                self.backoff.lock().failure();
                Err(ConnectorError::Connection(message))
            }
            Err(e) => {
                let mut buffer = self.buffer.lock();
                buffer.restore(pending, oldest);
                self.backoff.lock().failure();
                Err(e)
            }
        }
    }

    /// Validate + buffer one event. Returns true when the batch is
    /// full or stale (caller flushes).
    fn buffer_row(&self, topic: &Topic, payload: &Bytes, _qos: QoS) -> Result<bool> {
        if topic.as_str().is_empty() {
            return Err(ConnectorError::Dispatch(
                "rocketmq row requires a non-empty topic".to_string(),
            ));
        }
        if self.buffer.lock().len() >= self.config.effective_buffer() {
            return Err(ConnectorError::Connection(
                "rocketmq buffer limit reached".to_string(),
            ));
        }
        let message_id = self.message_seq.fetch_add(1, Ordering::SeqCst);
        let mut buffer = self.buffer.lock();
        Ok(buffer.push(RocketMqRow {
            topic: topic.as_str().to_string(),
            payload: payload.clone(),
            message_id,
        }))
    }
}

/// Deterministic signing input for a batch of messages (JSON array of
/// base64 bodies).
fn messages_md5_input(messages: &[RocketMqMessage]) -> Vec<String> {
    use base64::Engine;
    messages
        .iter()
        .map(|m| base64::engine::general_purpose::STANDARD.encode(&m.body))
        .collect()
}

#[async_trait]
impl Sink for RocketMqSink {
    async fn send(&self, topic: &Topic, payload: &Bytes, qos: QoS) -> Result<()> {
        if self.buffer_row(topic, payload, qos)? {
            self.flush().await?;
        }
        Ok(())
    }

    fn kind(&self) -> &'static str {
        "rocketmq"
    }
}

/// Management connector handle pairing an id with a RocketMQ sink.
pub struct RocketMqConnector {
    id: String,
    sink: Arc<RocketMqSink>,
}

impl RocketMqConnector {
    pub fn new(id: impl Into<String>, sink: Arc<RocketMqSink>) -> Self {
        Self {
            id: id.into(),
            sink,
        }
    }
}

impl broker_connectors::Connector for RocketMqConnector {
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

    // Vendor request code for pull.
    // Reason: the vendor broker uses 11 for pull.
    const REQUEST_PULL_MESSAGE: i32 = 11;
    // Vendor pull miss code.
    // Reason: the vendor uses 19 when no message exists.
    const RESPONSE_PULL_NOT_FOUND: i32 = 19;
    // Pull batch reads 32 rows.
    // Reason: 32 rows keep one pull small.
    const PULL_BATCH_SIZE: usize = 32;

    fn test_config() -> RocketMqSinkConfig {
        RocketMqSinkConfig {
            endpoints: vec!["127.0.0.1:8081".to_string()],
            topic: "rocket-telemetry".to_string(),
            tag_template: Some("${topic_segment_2}".to_string()),
            keys_template: Some("${client_id}-${message_id}".to_string()),
            strategy: "key_dispatch".to_string(),
            message_group_template: Some("group-${client_id}".to_string()),
            access_key: Some("rocket-key".to_string()),
            secret_key: Some("rocket-secret".to_string()),
            security_token: None,
            batch_size: Some(128),
            buffer_capacity: None,
            linger_ms: Some(100),
            timeout_ms: None,
        }
    }

    #[test]
    fn test_config_validation() {
        let mut config = test_config();
        assert!(config.validate().is_ok());

        config.endpoints.clear();
        assert!(config.validate().is_err());
        config.endpoints = vec!["  ".to_string()];
        assert!(config.validate().is_err());
        config.endpoints = test_config().endpoints;

        config.topic = "has space".to_string();
        assert!(config.validate().is_err());
        config.topic = String::new();
        assert!(config.validate().is_err());
        config.topic = test_config().topic;

        config.secret_key = None;
        assert!(config.validate().is_err(), "half credentials must fail");
        config.access_key = None;
        assert!(
            config.validate().is_ok(),
            "anonymous proxy testing is allowed"
        );
        config.access_key = Some("rocket-key".to_string());
        config.secret_key = Some(String::new());
        assert!(config.validate().is_err());
        config.secret_key = test_config().secret_key;

        // A token needs both keys and must not be empty.
        config.security_token = Some(String::new());
        assert!(config.validate().is_err(), "empty token must fail");
        config.security_token = Some("token-1".to_string());
        assert!(config.validate().is_ok());
        config.access_key = None;
        config.secret_key = None;
        assert!(config.validate().is_err(), "lone token must fail");
        config.access_key = test_config().access_key;
        config.secret_key = test_config().secret_key;
        config.security_token = None;

        config.tag_template = Some("${bogus}".to_string());
        assert!(config.validate().is_err());
        config.tag_template = test_config().tag_template;

        config.batch_size = Some(0);
        assert!(config.validate().is_err());
        config.batch_size = Some(10_000_000);

        // Zero clamped ceilings: huge depths are accepted.
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_authorization_known_answer() {
        // Independent Python (hmac/hashlib) vector.
        let sts = signing_string(
            "test-client-id",
            "2026-09-12T11:18:09.123Z",
            "rocket-telemetry",
            "5d41402abc4b2a76b9719d911017c592",
        );
        assert_eq!(
            sts,
            "test-client-id\n2026-09-12T11:18:09.123Z\nrocket-telemetry\n5d41402abc4b2a76b9719d911017c592"
        );
        assert_eq!(
            authorization_header("rocket-key", "rocket-secret", &sts),
            "MQ rocket-key:QQN6h/kBpUCyFPOPZ60kUDksydc="
        );
    }

    #[test]
    fn test_acl_signature_known_answer() {
        // Independent vector from Python, never from the code under test.
        // Made with: python3 -c "import hmac,hashlib,base64;
        // ext={'AccessKey':'AK-1','queueId':'0','topic':'T9'};
        // data=''.join(ext[k] for k in sorted(ext)).encode()+b'body-12';
        // print(base64.b64encode(hmac.new(b'test-secret-7',data,hashlib.sha1).digest()).decode())"
        // which prints G+gWda8KrZ5ka1k/BGN8OCufQX0=.
        let mut ext = HashMap::new();
        ext.insert("topic".to_string(), "T9".to_string());
        ext.insert("queueId".to_string(), "0".to_string());
        ext.insert("AccessKey".to_string(), "AK-1".to_string());
        assert_eq!(
            acl_signature("test-secret-7", &ext, b"body-12"),
            "G+gWda8KrZ5ka1k/BGN8OCufQX0="
        );
        // A stale Signature entry never signs itself.
        ext.insert("Signature".to_string(), "stale".to_string());
        assert_eq!(
            acl_signature("test-secret-7", &ext, b"body-12"),
            "G+gWda8KrZ5ka1k/BGN8OCufQX0="
        );
        // Keyed requests carry AccessKey plus Signature.
        // A set token rides along, an absent token stays out.
        let mut signed = HashMap::new();
        signed.insert("topic".to_string(), "T9".to_string());
        apply_acl_auth(
            &mut signed,
            b"body-12",
            Some("AK-1"),
            Some("test-secret-7"),
            None,
        );
        assert_eq!(signed.get("AccessKey").map(String::as_str), Some("AK-1"));
        assert!(!signed.contains_key("SecurityToken"));
        let first = signed.get("Signature").cloned().expect("signed");
        assert!(!first.is_empty());
        apply_acl_auth(
            &mut signed,
            b"body-12",
            Some("AK-1"),
            Some("test-secret-7"),
            Some("TOK-2"),
        );
        assert_eq!(
            signed.get("SecurityToken").map(String::as_str),
            Some("TOK-2")
        );
        // Keyless requests stay unsigned.
        let mut bare = HashMap::new();
        bare.insert("topic".to_string(), "T9".to_string());
        apply_acl_auth(&mut bare, b"body-12", None, None, None);
        assert!(!bare.contains_key("AccessKey"));
        assert!(!bare.contains_key("Signature"));
    }

    #[test]
    fn test_system_properties_builder() {
        let config = test_config();
        let props =
            build_system_properties(&config, "sensors/kitchen", "d7", 41, 1_789_211_889_123)
                .unwrap();
        assert_eq!(
            props,
            RocketMqSystemProperties {
                tag: Some("kitchen".to_string()),
                keys: vec!["d7-41".to_string()],
                message_group: Some("group-d7".to_string()),
                born_timestamp_ms: 1_789_211_889_123,
            }
        );
        // Empty renders vanish instead of riding the wire.
        let mut blank = test_config();
        blank.tag_template = Some("static".to_string());
        blank.keys_template = None;
        blank.message_group_template = None;
        let props = build_system_properties(&blank, "t", "d", 0, 0).unwrap();
        assert_eq!(props.tag.as_deref(), Some("static"));
        assert!(props.keys.is_empty());
        assert!(props.message_group.is_none());

        assert!(resolve_system_field("${bogus}", "a", "b", 0).is_err());
        assert!(resolve_system_field("${topic", "a", "b", 0).is_err());
        assert!(resolve_system_field("${}", "a", "b", 0).is_err());
    }

    #[test]
    fn test_envelope_framing_round_trip() {
        let envelope = RocketMqEnvelope {
            topic: "rocket-telemetry".to_string(),
            client_id: "test-client".to_string(),
            datetime: "2026-09-12T11:18:09.123Z".to_string(),
            authorization: Some("MQ k:s".to_string()),
            messages: vec![RocketMqEnvelopeMessage {
                system_properties: RocketMqSystemProperties {
                    tag: Some("kitchen".to_string()),
                    keys: vec!["d7-41".to_string()],
                    message_group: Some("group-d7".to_string()),
                    born_timestamp_ms: 1_789_211_889_123,
                },
                body_b64: "aGk=".to_string(),
            }],
        };
        let frame = encode_envelope(&envelope).unwrap();
        assert_eq!(frame[0], 0x01);
        let len = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
        assert_eq!(5 + len, frame.len());
        let back = decode_envelope(&frame).unwrap();
        assert_eq!(back.topic, "rocket-telemetry");
        assert_eq!(back.messages.len(), 1);
        assert_eq!(back.messages[0].body_b64, "aGk=");

        assert!(decode_envelope(&[0x01, 0x00]).is_err());
        assert!(decode_envelope(&[0x02, 0x00, 0x00, 0x00, 0x00]).is_err());
        let mut bad_len = frame.clone();
        bad_len[4] ^= 0xFF;
        assert!(decode_envelope(&bad_len).is_err());
    }

    #[test]
    fn test_status_classification_and_fifo_hashing() {
        assert_eq!(
            classify_status(RocketMqStatus::from_code(0)),
            RocketMqOutcome::Success
        );
        assert_eq!(
            classify_status(RocketMqStatus::from_code(1)),
            RocketMqOutcome::Retryable
        );
        assert_eq!(
            classify_status(RocketMqStatus::from_code(2)),
            RocketMqOutcome::Retryable
        );
        assert_eq!(
            classify_status(RocketMqStatus::from_code(3)),
            RocketMqOutcome::Terminal
        );
        assert_eq!(
            classify_status(RocketMqStatus::from_code(4)),
            RocketMqOutcome::Terminal
        );
        assert_eq!(
            classify_status(RocketMqStatus::from_code(5)),
            RocketMqOutcome::Terminal
        );
        assert_eq!(
            classify_status(RocketMqStatus::from_code(99)),
            RocketMqOutcome::Retryable
        );

        // FNV-1a 32 reference values (independent Python check).
        assert_eq!(fnv1a_32(b""), 0x811c9dc5);
        assert_eq!(fnv1a_32(b"a"), 0xe40c292c);
        assert_eq!(fnv1a_32(b"foobar"), 0xbf9cf968);
        assert_eq!(fnv1a_32(b"fifo-group-7"), 0xa7bf4ac9);
        // FIFO groups pin to one queue; unordered lands on 0.
        assert_eq!(fifo_partition(Some("fifo-group-7"), 8), 0xa7bf4ac9 % 8);
        assert_eq!(fifo_partition(None, 8), 0);
        assert_eq!(fifo_partition(Some("g"), 4), fnv1a_32(b"g") % 4);
    }

    #[tokio::test]
    async fn test_send_flow_and_backoff() {
        let transport = Arc::new(MockRocketMqTransport::new());
        let mut config = test_config();
        config.batch_size = Some(2);
        let sink = RocketMqSink::new(config, transport.clone()).unwrap();

        let topic = Topic::new("sensors/kitchen").unwrap();
        sink.send(
            &topic,
            &Bytes::from(r#"{"client_id":"d7"}"#),
            QoS::AtMostOnce,
        )
        .await
        .unwrap();
        sink.send(
            &topic,
            &Bytes::from(r#"{"client_id":"d8"}"#),
            QoS::AtMostOnce,
        )
        .await
        .unwrap();
        assert_eq!(sink.sent_batches(), 1);
        assert_eq!(sink.sent_records(), 2);

        let envelopes = transport.envelopes();
        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0].topic, "rocket-telemetry");
        assert_eq!(envelopes[0].messages.len(), 2);
        assert_eq!(
            envelopes[0].messages[0].system_properties.tag.as_deref(),
            Some("kitchen")
        );
        assert_eq!(
            envelopes[0].messages[0].system_properties.keys,
            vec!["d7-0".to_string()]
        );
        assert_eq!(
            envelopes[0].messages[0]
                .system_properties
                .message_group
                .as_deref(),
            Some("group-d7")
        );
        use base64::Engine;
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(&envelopes[0].messages[0].body_b64)
                .unwrap(),
            br#"{"client_id":"d7"}"#.to_vec()
        );
        assert!(envelopes[0]
            .authorization
            .as_deref()
            .unwrap()
            .starts_with("MQ rocket-key:"));

        // Transport failures restore the buffer and engage backoff.
        transport.fail_next(10);
        sink.send(
            &topic,
            &Bytes::from(r#"{"client_id":"dx"}"#),
            QoS::AtMostOnce,
        )
        .await
        .unwrap();
        let err = sink.flush().await.expect_err("mock down must fail");
        assert!(matches!(err, ConnectorError::Connection(_)));
        assert_eq!(sink.buffered_rows(), 1);
        assert_eq!(transport.calls(), 2);
    }

    /// Fake RocketMQ proxy: reads one envelope frame, checks the auth
    /// header + topics, and acks `{"code":0}`.
    #[tokio::test]
    async fn test_tcp_loopback_envelope() {
        use std::sync::Mutex as StdMutex;

        let seen_auth = Arc::new(StdMutex::new(Vec::<String>::new()));
        let seen_auth_rx = seen_auth.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            // Envelope frame: version + BE length + JSON.
            let mut prefix = [0u8; 5];
            stream.read_exact(&mut prefix).await.expect("prefix");
            assert_eq!(prefix[0], 0x01);
            let len = u32::from_be_bytes([prefix[1], prefix[2], prefix[3], prefix[4]]) as usize;
            let mut body = vec![0u8; len];
            stream.read_exact(&mut body).await.expect("body");
            let envelope: RocketMqEnvelope = serde_json::from_slice(&body).expect("envelope");
            assert_eq!(envelope.topic, "rocket-telemetry");
            assert_eq!(envelope.messages.len(), 1);
            assert_eq!(
                envelope.messages[0].system_properties.tag.as_deref(),
                Some("kitchen")
            );
            seen_auth_rx
                .lock()
                .unwrap()
                .push(envelope.authorization.clone().expect("signed when keyed"));
            // Ack frame: BE length + {"code":0}.
            let ack = br#"{"code":0}"#;
            let mut framed = (ack.len() as u32).to_be_bytes().to_vec();
            framed.extend_from_slice(ack);
            stream.write_all(&framed).await.expect("ack");
        });

        let mut config = test_config();
        config.endpoints = vec![format!("127.0.0.1:{port}")];
        config.batch_size = Some(1);
        let sink = RocketMqSink::new(config, Arc::new(MockRocketMqTransport::new())).unwrap();
        // Drive the TCP transport directly (unit scope: framing on wire).
        let transport = TcpRocketMqTransport::new(&test_config_with_port(port)).unwrap();
        let messages = sink
            .build_messages(
                &[RocketMqRow {
                    topic: "sensors/kitchen".to_string(),
                    payload: Bytes::from_static(br#"{"client_id":"d7"}"#),
                    message_id: 0,
                }],
                1_789_211_889_123,
            )
            .unwrap();
        transport
            .send_messages(
                "rocket-telemetry",
                sink.client_id(),
                "2026-09-12T11:18:09.123Z",
                Some("MQ rocket-key:test"),
                messages,
            )
            .await
            .expect("loopback send succeeds");

        let mut finished = false;
        for _ in 0..500 {
            if server.is_finished() {
                finished = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(finished, "fake proxy never consumed the envelope");
        server.await.expect("fake proxy task");
        assert_eq!(
            *seen_auth.lock().unwrap(),
            vec!["MQ rocket-key:test".to_string()]
        );
    }

    fn test_config_with_port(port: u16) -> RocketMqSinkConfig {
        let mut config = test_config();
        config.endpoints = vec![format!("127.0.0.1:{port}")];
        config
    }

    /// Fail closed when the named env is missing.
    fn qual_required(name: &str) -> String {
        std::env::var(name)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| panic!("{name} must name a real server for qualification"))
    }

    /// Drop unmatched egress for the rule path.
    struct NullBrokerSink;

    #[async_trait::async_trait]
    impl broker_rules::BrokerSink for NullBrokerSink {
        async fn publish(
            &self,
            _topic: Topic,
            _payload: Bytes,
            _qos: QoS,
            _retain: bool,
        ) -> std::result::Result<(), broker_rules::RuleEngineError> {
            Ok(())
        }
    }

    /// Vendor request code for heartbeat.
    /// Reason: the vendor broker uses 34 for the client heartbeat.
    /// Code 32 is GET_EARLIEST_MSG_STORETIME, not a heartbeat.
    const REQUEST_HEART_BEAT: i32 = 34;

    /// Register the consumer group with the broker before pulling.
    /// The vendor rejects pulls from a group it has never seen (code 24).
    /// The heartbeat body carries the consumer group and its subscription,
    /// as a real consumer client sends. Without it the broker never
    /// registers the group and every pull returns code 24.
    /// Run on the pull stream and keep it open.
    /// Reason: the broker ties the group to the channel.
    async fn qual_register_group(
        stream: &mut TcpStream,
        broker_label: &str,
        group: &str,
        topic: &str,
        timeout: Duration,
        opaque: &AtomicI32,
        sub_version: i64,
    ) -> Result<()> {
        let ext = HashMap::new();
        let body = serde_json::to_vec(&serde_json::json!({
            "clientID": format!("indra-qual-{group}"),
            "producerDataSet": [],
            "consumerDataSet": [{
                "groupName": group,
                "consumeType": "CONSUME_PASSIVELY",
                "messageModel": "CLUSTERING",
                "consumeFromWhere": "CONSUME_FROM_LAST_OFFSET",
                "subscriptionDataSet": [{
                    "classFilterMode": false,
                    "topic": topic,
                    "subString": "*",
                    "tagsSet": ["*"],
                    "codeSet": [],
                    "subVersion": sub_version,
                    "expressionType": "TAG"
                }],
                "unitMode": false
            }],
        }))
        .map_err(|e| {
            ConnectorError::Connection(format!("rocketmq heartbeat body is not JSON: {e}"))
        })?;
        let id = opaque.fetch_add(1, Ordering::SeqCst);
        let frame = encode_remoting(REQUEST_HEART_BEAT, &ext, &body, id)?;
        let (code, _fields, _body, remark) =
            remoting_exchange_on_stream(stream, frame, id, timeout).await?;
        eprintln!("qual register: broker={broker_label} code={code} remark={remark}");
        if code != RESPONSE_SUCCESS {
            return Err(ConnectorError::Connection(format!(
                "rocketmq register group {group} at {broker_label} failed code {code}: {remark}"
            )));
        }
        Ok(())
    }

    /// Arguments for one remoting pull.
    /// Grouped so `qual_pull_once` stays under the seven-argument
    /// clippy limit without an `allow` attribute.
    struct QualPullOnce<'a> {
        topic: &'a str,
        group: &'a str,
        timeout: Duration,
        opaque: &'a AtomicI32,
        sub_version: i64,
    }

    /// Pull one queue at one offset through remoting.
    /// Run on the registered stream.
    /// Return code, fields and body.
    async fn qual_pull_once(
        stream: &mut TcpStream,
        args: &QualPullOnce<'_>,
        queue: u32,
        offset: i64,
    ) -> Result<(i32, HashMap<String, String>, Vec<u8>, String)> {
        let mut ext = HashMap::new();
        ext.insert("consumerGroup".to_string(), args.group.to_string());
        ext.insert("topic".to_string(), args.topic.to_string());
        ext.insert("queueId".to_string(), queue.to_string());
        ext.insert("queueOffset".to_string(), offset.to_string());
        ext.insert("maxMsgNums".to_string(), PULL_BATCH_SIZE.to_string());
        ext.insert("sysFlag".to_string(), "0".to_string());
        ext.insert("commitOffset".to_string(), "0".to_string());
        ext.insert("suspendTimeoutMillis".to_string(), "0".to_string());
        ext.insert("subscription".to_string(), "*".to_string());
        ext.insert("expressionType".to_string(), "TAG".to_string());
        ext.insert("subVersion".to_string(), args.sub_version.to_string());
        let id = args.opaque.fetch_add(1, Ordering::SeqCst);
        let frame = encode_remoting(REQUEST_PULL_MESSAGE, &ext, &[], id)?;
        remoting_exchange_on_stream(stream, frame, id, args.timeout).await
    }

    /// Qualification against a real vendor server.
    ///
    /// Run with e.g.:
    /// `ROCKETMQ_NAMESRV=127.0.0.1:9876 ROCKETMQ_TOPIC=qual-b339
    ///  ROCKETMQ_GROUP=qual-group
    ///  cargo test -p broker-connectors-enterprise --lib
    ///  rocketmq::tests::test_qualify_driver_write_path --
    ///  --ignored --nocapture --test-threads=1`
    ///
    /// It streams rows through the broker rule path on the vendor
    /// transport. It drives both production entries. It reads rows
    /// back through an independent pull. It asserts the exact count
    /// plus the first and the last payload. It proves a bad node
    /// fails closed. It panics when env is missing.
    ///
    /// TODO(parity): token auth against a secured server stays
    /// unqualified, which error codes are terminal is open.
    #[tokio::test]
    #[ignore = "needs a real vendor server (see ROCKETMQ_* env)"]
    async fn test_qualify_driver_write_path() {
        use broker_protocol::TopicFilter;
        use broker_rules::{BackpressurePolicy, RuleEngine};

        // Qual writes 100 rows, 50 per entry.
        // Reason: two entries need equal halves.
        const ROWS: usize = 100;

        let namesrv = qual_required("ROCKETMQ_NAMESRV");
        let topic = qual_required("ROCKETMQ_TOPIC");
        let group = qual_required("ROCKETMQ_GROUP");
        assert!(
            namesrv.contains(':'),
            "qual namesrv must carry a port, got {namesrv:?}"
        );
        assert!(!topic.trim().is_empty(), "qual topic must not be empty");
        assert!(!group.trim().is_empty(), "qual group must not be empty");

        // Unique run tag keeps old rows out of the count.
        let run_id = format!("{}-{}", now_millis(), std::process::id());
        // One subscription version for the whole session: the heartbeat
        // registers it and every pull repeats it, as a real consumer does.
        // A fresh subVersion per pull makes the broker reject the pull with
        // code 25 (the consumer's subscription not latest).
        let sub_version = now_millis();
        eprintln!("qual server: namesrv={namesrv} topic={topic} group={group}");

        // Good path rides both production entries.
        // Reason: the qual must fail when either entry breaks.
        // Rule-task channels hold 65536 events.
        // Reason: the default window depth fits the qual burst.
        let engine = RuleEngine::new(65_536, BackpressurePolicy::DropOldest);

        // Entry one runs through `build_connector`.
        // Batch one flushes each send, so no explicit flush runs.
        // Buffer 10000 caps memory per rule sink.
        // Linger 10 flushes idle batches fast.
        // Timeout 30000 covers vendor send plus route lookup.
        let api_config = serde_json::json!({
            "endpoints": [namesrv.clone()],
            "topic": topic.clone(),
            "batch_size": 1,
            "buffer_capacity": 10000,
            "linger_ms": 10,
            "timeout_ms": 30_000
        });
        let api_req = broker_api::CreateConnectorRequest {
            id: "qual-rocketmq-api".to_string(),
            kind: "rocketmq".to_string(),
            config: api_config,
        };
        let (api_kind, api_sink) = broker_api::build_connector(&api_req)
            .await
            .expect("qual build_connector builds");
        assert_eq!(api_kind, "rocketmq");
        assert_eq!(api_sink.kind(), "rocketmq");
        engine.connectors().register("qual-rocketmq-api", api_sink);

        // Entry two runs through `register_live_sink`.
        // Timeout 30000 covers vendor send plus route lookup.
        let v5_body = serde_json::json!({
            "endpoint": namesrv.clone(),
            "topic": topic.clone(),
            "timeout_ms": 30_000
        });
        broker_api::v5::rules::register_live_sink(
            &engine,
            "rocketmq",
            "qual-rocketmq-v5",
            &v5_body,
            std::path::Path::new("./data"),
        )
        .await
        .expect("qual register_live_sink builds");
        let v5_sink = engine
            .connectors()
            .get("qual-rocketmq-v5")
            .expect("qual v5 sink registers");
        assert_eq!(v5_sink.kind(), "rocketmq");

        // Broker path: rule actions send through the shared manager.
        // The qual sends through it, never `sink.send` direct.
        engine
            .create_rule(
                "qual-rocketmq-rule-api".to_string(),
                TopicFilter::new("qual/rocketmq/a/+").unwrap(),
                Some(
                    r#"SELECT seq, run_id, client_id FROM "qual/rocketmq/a/+" INTO connector("qual-rocketmq-api")"#
                        .to_string(),
                ),
                true,
                vec![],
            )
            .expect("rule creates");
        engine
            .create_rule(
                "qual-rocketmq-rule-v5".to_string(),
                TopicFilter::new("qual/rocketmq/b/+").unwrap(),
                Some(
                    r#"SELECT seq, run_id, client_id FROM "qual/rocketmq/b/+" INTO connector("qual-rocketmq-v5")"#
                        .to_string(),
                ),
                true,
                vec![],
            )
            .expect("rule creates");
        let egress: Arc<dyn broker_rules::BrokerSink> = Arc::new(NullBrokerSink);

        for seq in 0..50 {
            let mqtt_topic =
                Topic::new(format!("qual/rocketmq/a/dev-{seq:04}")).expect("qual topic builds");
            let payload = Bytes::from(format!(
                r#"{{"seq":{seq},"run_id":"{run_id}","client_id":"dev-{seq:04}"}}"#
            ));
            engine
                .dispatch_ingress(&mqtt_topic, &payload, QoS::AtLeastOnce, &egress)
                .await;
        }
        for seq in 50..ROWS {
            let mqtt_topic =
                Topic::new(format!("qual/rocketmq/b/dev-{seq:04}")).expect("qual topic builds");
            let payload = Bytes::from(format!(
                r#"{{"seq":{seq},"run_id":"{run_id}","client_id":"dev-{seq:04}"}}"#
            ));
            engine
                .dispatch_ingress(&mqtt_topic, &payload, QoS::AtLeastOnce, &egress)
                .await;
        }

        // Independent pull reads the rows back.
        let timeout = Duration::from_secs(30);
        let opaque = AtomicI32::new(1);
        let route = fetch_broker_route(&namesrv, &topic, timeout, &opaque, None, None, None)
            .await
            .expect("qual route fetches");
        eprintln!("qual route: broker={} queues={}", route.addr, route.queues);
        // The vendor creates four queues for a new topic.
        // Reason: auto-create uses the send queue count.
        assert_eq!(route.queues, 4, "qual route must carry four queues");
        // Open one broker session for register plus pulls.
        // Reason: the broker ties the group to the channel.
        let mut stream = tokio::time::timeout(timeout, TcpStream::connect(&route.addr))
            .await
            .expect("qual broker connects")
            .expect("qual broker connects");
        // The vendor rejects pulls from an unregistered group (code 24).
        // Register the group with a heartbeat first, as a real consumer client does.
        // Pass the topic and the group, then wait for code 0.
        qual_register_group(
            &mut stream,
            &route.addr,
            &group,
            &topic,
            timeout,
            &opaque,
            sub_version,
        )
        .await
        .expect("qual register group");
        let pull = QualPullOnce {
            topic: &topic,
            group: &group,
            timeout,
            opaque: &opaque,
            sub_version,
        };
        let mut collected = Vec::new();
        // Drain each queue on the registered stream.
        // Reason: one pull reads at most 32 rows.
        for queue in 0..route.queues {
            let mut offset: i64 = 0;
            // Pull up to 200 times to drain one queue.
            // Reason: one pull reads at most 32 rows.
            for _ in 0..200 {
                let (code, fields, body, remark) =
                    qual_pull_once(&mut stream, &pull, queue, offset)
                        .await
                        .expect("qual pull sends");
                if code == RESPONSE_SUCCESS {
                    collected.extend_from_slice(&body);
                    let next = fields
                        .get("nextBeginOffset")
                        .and_then(|v| v.parse::<i64>().ok())
                        .unwrap_or(offset + 1);
                    if next <= offset {
                        break;
                    }
                    offset = next;
                    // Cap collected bytes at four max frames.
                    // Reason: a larger reply means a fault.
                    if collected.len() > MAX_FRAME_BYTES * 4 {
                        break;
                    }
                } else if code == RESPONSE_PULL_NOT_FOUND {
                    break;
                } else {
                    panic!("qual pull failed code {code}: {remark}");
                }
            }
        }
        let text = String::from_utf8_lossy(&collected).to_string();
        for seq in 0..ROWS {
            let needle = format!(r#""seq":{seq},"run_id":"{run_id}""#);
            assert!(text.contains(&needle), "qual missing seq {seq}");
        }
        let first = format!(r#""seq":0,"run_id":"{run_id}""#);
        let last = format!(r#""seq":{},"run_id":"{run_id}""#, ROWS - 1);
        assert!(text.contains(&first), "qual first row");
        assert!(text.contains(&last), "qual last row");
        eprintln!("qual rows asserted: count={ROWS} first={first} last={last}");

        // Fault: a bad node fails closed with the node text.
        let probe = tokio::net::TcpStream::connect("127.0.0.1:1")
            .await
            .expect_err("qual probe: port 1 must refuse");
        let probe_text = probe.to_string().to_lowercase();
        assert!(
            probe_text.contains("refus"),
            "qual probe text: {probe_text}"
        );
        // Fault rides the production entry with a bad node.
        // Fault goes through the rule path, never direct send.
        // Reason: the rule path is the production path.
        // Batch one flushes on send, so dispatch must fail.
        // Buffer 10000 caps memory. Linger 10 keeps the fault fast.
        // Timeout 5000 keeps the fault fast.
        let bad_config = serde_json::json!({
            "endpoints": ["127.0.0.1:1"],
            "topic": topic.clone(),
            "batch_size": 1,
            "buffer_capacity": 10000,
            "linger_ms": 10,
            "timeout_ms": 5_000
        });
        let bad_req = broker_api::CreateConnectorRequest {
            id: "qual-rocketmq-bad".to_string(),
            kind: "rocketmq".to_string(),
            config: bad_config,
        };
        let (_, bad_sink) = broker_api::build_connector(&bad_req)
            .await
            .expect("qual bad build succeeds");
        // Fault engine holds 1024 events.
        // Reason: one bad row needs a small queue.
        let bad_engine = RuleEngine::new(1024, BackpressurePolicy::DropOldest);
        bad_engine
            .connectors()
            .register("qual-rocketmq-bad", bad_sink);
        // Use the returned id.
        // Reason: create_rule assigns id rule-N.
        let bad_rule_id = bad_engine
            .create_rule(
                "qual-rocketmq-rule-bad".to_string(),
                TopicFilter::new("qual/rocketmq/bad/+").unwrap(),
                Some(
                    r#"SELECT seq FROM "qual/rocketmq/bad/+" INTO connector("qual-rocketmq-bad")"#
                        .to_string(),
                ),
                true,
                vec![],
            )
            .expect("bad rule creates")
            .id
            .clone();
        let bad_egress: Arc<dyn broker_rules::BrokerSink> = Arc::new(NullBrokerSink);
        bad_engine
            .dispatch_ingress(
                &Topic::new("qual/rocketmq/bad/0").expect("qual bad topic builds"),
                &Bytes::from_static(br#"{"seq":-1}"#),
                QoS::AtLeastOnce,
                &bad_egress,
            )
            .await;
        let bad_rule = bad_engine.get_rule(&bad_rule_id).expect("bad rule stays");
        assert!(
            bad_rule.actions_failed_cnt.load(Ordering::Relaxed) == 1,
            "wrong node must fail closed"
        );
        let route_err = fetch_broker_route(
            "127.0.0.1:1",
            &topic,
            Duration::from_secs(5),
            &opaque,
            None,
            None,
            None,
        )
        .await
        .expect_err("wrong node must fail");
        match &route_err {
            ConnectorError::Connection(message) => assert!(
                message.to_lowercase().contains("refus") || message.contains("127.0.0.1:1"),
                "qual fault must carry the node text, got {message:?}"
            ),
            other => panic!("wrong node must be a connection error, got {other:?}"),
        }
        eprintln!("qual fault asserted: wrong node fails closed");
    }
}
