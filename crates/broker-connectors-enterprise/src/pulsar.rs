//! Apache Pulsar producer sink (INDRA-153).
//!
//! Buffers MQTT events and produces them to multi-tenant Pulsar
//! topics (`persistent://{tenant}/{namespace}/{topic}`) with keyed
//! partition routing, monotonic sequence ids, event-time stamps and
//! custom properties (`mqtt_topic` / `mqtt_qos` always injected).
//!
//! The production write path runs on the maintained `pulsar` driver
//! ([`DriverPulsarTransport`] below): binary protocol through the
//! driver client (no auth against standalone; token auth stays
//! unqualified, see below).
//!
//! The codec is a clean-room subset of the Pulsar binary protocol:
//! `[totalSize u32][commandSize u32][Command][0x0E01][CRC32C][metaSize
//! u32][MessageMetadata][payload]`, where Command and MessageMetadata
//! use Protobuf-style varint/LEN fields. The codec encodes the
//! Connected(3) and Send(19) commands. Metadata subset: producer_name = 1,
//! sequence_id = 2, publish_time = 3, properties = 4
//! (`KeyValue{key = 1, value = 2}`), partition_key = 5,
//! event_time = 6. The memory transport captures everything in-process
//! for tests.

use async_trait::async_trait;
use broker_protocol::{QoS, Topic};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use broker_connectors::{
    now_millis, render_template, BackoffState, BatchQueue, ConnectorError, Result, Sink,
};

/// Pulsar command types (subset).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
enum PulsarCommand {
    Connected = 3,
    Send = 19,
}

/// Pulsar authentication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase", tag = "type")]
pub enum PulsarAuth {
    /// No auth fields on Connect.
    #[default]
    None,
    /// JWT in `auth_method_name = "token"` + `auth_data`.
    Token { token: String },
}

fn default_tenant() -> String {
    "public".to_string()
}

fn default_namespace() -> String {
    "default".to_string()
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

fn is_tenant_segment(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

/// Parse `pulsar://host:6650`, `http://host:8080` or `host:port`
/// (default port 6650, 8080 for `http(s)://`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PulsarEndpoint {
    pub host: String,
    pub port: u16,
}

pub fn parse_service_url(url: &str) -> Result<PulsarEndpoint> {
    let url = url.trim();
    if url.is_empty() {
        return Err(ConnectorError::Dispatch(
            "pulsar service_url must not be empty".to_string(),
        ));
    }
    let (default_port, rest) = match url.split_once("://") {
        Some(("pulsar", rest)) => (6650u16, rest),
        Some(("http" | "https", rest)) => (8080u16, rest),
        Some((scheme, _)) => {
            return Err(ConnectorError::Dispatch(format!(
                "pulsar service_url scheme must be pulsar:// or http(s)://, got {scheme:?}"
            )));
        }
        None => (6650u16, url),
    };
    if rest.is_empty() || rest.contains('/') {
        return Err(ConnectorError::Dispatch(format!(
            "pulsar service_url must be host[:port], got {url:?}"
        )));
    }
    let (host, port) = match rest.rsplit_once(':') {
        Some((host, port)) => {
            let port: u16 = port
                .parse()
                .map_err(|_| ConnectorError::Dispatch(format!("pulsar bad port in {url:?}")))?;
            if port == 0 {
                return Err(ConnectorError::Dispatch(format!(
                    "pulsar port must be 1..=65535 in {url:?}"
                )));
            }
            (host, port)
        }
        None => (rest, default_port),
    };
    if host.is_empty() {
        return Err(ConnectorError::Dispatch(format!(
            "pulsar host must not be empty in {url:?}"
        )));
    }
    Ok(PulsarEndpoint {
        host: host.to_string(),
        port,
    })
}

/// Pulsar sink configuration. All depths are optional (`None` =
/// unbounded) with zero clamped ceilings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PulsarSinkConfig {
    /// Service URL (`pulsar://host:6650`, `http://host:8080`, ...).
    pub service_url: String,
    /// Tenant (default `public`).
    #[serde(default = "default_tenant")]
    pub tenant: String,
    /// Namespace (default `default`).
    #[serde(default = "default_namespace")]
    pub namespace: String,
    /// Topic name or template (`${topic}` supported; MQTT `/`
    /// becomes `.`).
    pub topic: String,
    /// Authentication (default none).
    #[serde(default)]
    pub auth: PulsarAuth,
    /// Partition key template (`${client_id}`, `${topic}`, ...).
    #[serde(default)]
    pub partition_key_template: Option<String>,
    /// Custom properties with template substitution.
    #[serde(default)]
    pub properties: HashMap<String, String>,
    /// Messages per produce call (default 200).
    #[serde(default = "default_batch_size")]
    pub batch_size: Option<usize>,
    /// Batch byte limit (default 2 MiB).
    #[serde(default = "default_batch_bytes")]
    pub batch_bytes: Option<usize>,
    /// Linger flush window in ms (default 10).
    #[serde(default = "default_linger_ms")]
    pub linger_ms: Option<u64>,
    /// Retries on transport failures (default 3, `None` unbounded).
    #[serde(default = "default_max_retries")]
    pub max_retries: Option<usize>,
    /// First retry delay in ms (default 100).
    #[serde(default = "default_initial_backoff_ms")]
    pub initial_backoff_ms: Option<u64>,
    /// Retry delay ceiling in ms (default 2000).
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: Option<u64>,
    /// Network request / handshake timeout in ms (default 5000).
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

impl PulsarSinkConfig {
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms.unwrap_or(5000).max(1))
    }

    pub fn validate(&self) -> Result<()> {
        parse_service_url(&self.service_url)?;
        if !is_tenant_segment(&self.tenant) {
            return Err(ConnectorError::Dispatch(format!(
                "pulsar tenant must match [A-Za-z0-9._-]+: {:?}",
                self.tenant
            )));
        }
        if !is_tenant_segment(&self.namespace) {
            return Err(ConnectorError::Dispatch(format!(
                "pulsar namespace must match [A-Za-z0-9._-]+: {:?}",
                self.namespace
            )));
        }
        if self.topic.trim().is_empty() {
            return Err(ConnectorError::Dispatch(
                "pulsar topic must not be empty".to_string(),
            ));
        }
        // Strict template checks with dummy values.
        self.resolve_topic("dummy/topic", QoS::AtMostOnce, 0)?;
        if let PulsarAuth::Token { token } = &self.auth {
            if token.trim().is_empty() {
                return Err(ConnectorError::Dispatch(
                    "pulsar token must not be empty".to_string(),
                ));
            }
        }
        if let Some(template) = &self.partition_key_template {
            self.event_vars("dummy", b"{}", QoS::AtMostOnce, 0, template)?;
        }
        for (name, template) in &self.properties {
            if name.trim().is_empty() {
                return Err(ConnectorError::Dispatch(
                    "pulsar property names must not be empty".to_string(),
                ));
            }
            self.event_vars("dummy", b"{}", QoS::AtMostOnce, 0, template)?;
        }
        if self.batch_size == Some(0) {
            return Err(ConnectorError::Dispatch(
                "pulsar batch_size must be >= 1".to_string(),
            ));
        }
        if self.batch_bytes == Some(0) {
            return Err(ConnectorError::Dispatch(
                "pulsar batch_bytes must be >= 1".to_string(),
            ));
        }
        Ok(())
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

    /// MQTT levels become Pulsar dots; anything outside
    /// `[A-Za-z0-9._-]` (plus `+` kept verbatim) becomes `-`.
    fn sanitize_topic_name(topic: &str) -> String {
        topic
            .chars()
            .map(|c| {
                if c == '/' {
                    '.'
                } else if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '+') {
                    c
                } else {
                    '-'
                }
            })
            .collect()
    }

    /// Canonical destination
    /// `persistent://{tenant}/{namespace}/{topic}`.
    pub fn resolve_topic(&self, topic: &str, qos: QoS, millis: i64) -> Result<String> {
        if topic.is_empty() {
            return Err(ConnectorError::Dispatch(
                "pulsar topic needs a non-empty MQTT topic".to_string(),
            ));
        }
        let vars = [
            ("topic".to_string(), Self::sanitize_topic_name(topic)),
            ("qos".to_string(), u8::from(qos).to_string()),
            ("timestamp".to_string(), millis.to_string()),
        ];
        let borrowed: Vec<(&str, String)> =
            vars.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
        let name = render_template(&self.topic, &borrowed)?;
        if name.trim().is_empty() || name.contains('/') || name.contains(' ') {
            return Err(ConnectorError::Dispatch(format!(
                "pulsar topic resolved to an invalid name: {name:?}"
            )));
        }
        Ok(format!(
            "persistent://{}/{}/{name}",
            self.tenant, self.namespace
        ))
    }

    fn template_vars(topic: &str, payload: &[u8], qos: QoS, millis: i64) -> Vec<(String, String)> {
        let doc: serde_json::Value = serde_json::from_slice(payload).unwrap_or_default();
        let client_id = match doc.get("client_id") {
            Some(serde_json::Value::String(text)) => text.clone(),
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
        let vars = Self::template_vars(topic, payload, qos, millis);
        let borrowed: Vec<(&str, String)> =
            vars.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
        render_template(template, &borrowed)
    }
}

// ---------------------------------------------------------------------------
// Wire codec: varint/LEN protobuf subset, CRC32C, frame envelope.
// ---------------------------------------------------------------------------

fn encode_varint(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn decode_varint(buf: &[u8]) -> Result<(u64, usize)> {
    let mut value = 0u64;
    for (index, byte) in buf.iter().take(10).enumerate() {
        value |= ((byte & 0x7F) as u64) << (7 * index);
        if byte & 0x80 == 0 {
            return Ok((value, index + 1));
        }
    }
    Err(ConnectorError::Dispatch(
        "pulsar truncated varint".to_string(),
    ))
}

fn encode_tag(field: u32, wire: u32, out: &mut Vec<u8>) {
    encode_varint((u64::from(field) << 3) | u64::from(wire), out);
}

fn encode_string_field(field: u32, value: &str, out: &mut Vec<u8>) {
    encode_tag(field, 2, out);
    encode_varint(value.len() as u64, out);
    out.extend_from_slice(value.as_bytes());
}

fn encode_varint_field(field: u32, value: u64, out: &mut Vec<u8>) {
    encode_tag(field, 0, out);
    encode_varint(value, out);
}

/// CRC32C (Castagnoli, polynomial 0x1EDC6F41) over metadata+payload.
pub fn crc32c(data: &[u8]) -> u32 {
    const TABLE: [u32; 256] = crc32c_table();
    let mut crc = 0xFFFF_FFFFu32;
    for byte in data {
        crc = TABLE[((crc ^ u32::from(*byte)) & 0xFF) as usize] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

const fn crc32c_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            crc = if crc & 1 == 1 {
                0x82F6_3B78 ^ (crc >> 1)
            } else {
                crc >> 1
            };
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

/// Command envelope field numbers used by the subset.
mod fields {
    pub const TYPE: u32 = 1;
    pub const PRODUCER_ID: u32 = 2;
    pub const SEQUENCE_ID: u32 = 2;
    pub const PRODUCER_NAME_META: u32 = 1;
    pub const SEQUENCE_ID_META: u32 = 2;
    pub const PUBLISH_TIME: u32 = 3;
    pub const PROPERTIES: u32 = 4;
    pub const PARTITION_KEY: u32 = 5;
    pub const EVENT_TIME: u32 = 6;
    pub const KV_KEY: u32 = 1;
    pub const KV_VALUE: u32 = 2;
}

/// Encode a `Connected` command body (fake brokers + tests).
pub fn encode_connected() -> Vec<u8> {
    let mut out = Vec::new();
    encode_varint_field(fields::TYPE, PulsarCommand::Connected as u64, &mut out);
    out
}

/// Encode a `Send` command body.
pub fn encode_send(producer_id: u64, sequence_id: u64) -> Vec<u8> {
    let mut out = Vec::new();
    encode_varint_field(fields::TYPE, PulsarCommand::Send as u64, &mut out);
    encode_varint_field(fields::PRODUCER_ID, producer_id, &mut out);
    encode_varint_field(fields::SEQUENCE_ID, sequence_id, &mut out);
    out
}

/// Command-only frame: `[totalSize][commandSize][command]`.
pub fn encode_command_frame(command: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(8 + command.len());
    frame.extend_from_slice(&((command.len() + 4) as u32).to_be_bytes());
    frame.extend_from_slice(&(command.len() as u32).to_be_bytes());
    frame.extend_from_slice(command);
    frame
}

/// Full message frame with metadata, payload and CRC32C.
pub fn encode_message_frame(command: &[u8], metadata: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut checksumed = Vec::with_capacity(4 + metadata.len() + payload.len());
    checksumed.extend_from_slice(&(metadata.len() as u32).to_be_bytes());
    checksumed.extend_from_slice(metadata);
    checksumed.extend_from_slice(payload);
    let checksum = crc32c(&checksumed);
    let mut frame = Vec::new();
    let total = 4 + command.len() + 2 + 4 + checksumed.len();
    frame.extend_from_slice(&(total as u32).to_be_bytes());
    frame.extend_from_slice(&(command.len() as u32).to_be_bytes());
    frame.extend_from_slice(command);
    frame.extend_from_slice(&[0x0E, 0x01]); // payload magic
    frame.extend_from_slice(&checksum.to_be_bytes());
    frame.extend_from_slice(&checksumed);
    frame
}

/// Split a received frame into (command, metadata, payload). Metadata
/// and payload are empty for command-only frames.
pub fn decode_frame(frame: &[u8]) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    if frame.len() < 8 {
        return Err(ConnectorError::Connection(
            "pulsar truncated frame header".to_string(),
        ));
    }
    let total = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
    if frame.len() < 4 + total {
        return Err(ConnectorError::Connection(
            "pulsar truncated frame".to_string(),
        ));
    }
    let frame = &frame[4..4 + total];
    let command_size = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
    if frame.len() < 4 + command_size {
        return Err(ConnectorError::Connection(
            "pulsar truncated command".to_string(),
        ));
    }
    let command = frame[4..4 + command_size].to_vec();
    let mut rest = &frame[4 + command_size..];
    if rest.is_empty() {
        return Ok((command, Vec::new(), Vec::new()));
    }
    if rest.len() < 2 + 4 + 4 || rest[0] != 0x0E || rest[1] != 0x01 {
        return Err(ConnectorError::Connection(
            "pulsar bad payload magic".to_string(),
        ));
    }
    let checksum = u32::from_be_bytes([rest[2], rest[3], rest[4], rest[5]]);
    rest = &rest[6..];
    let meta_size = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]) as usize;
    rest = &rest[4..];
    if rest.len() < meta_size {
        return Err(ConnectorError::Connection(
            "pulsar truncated metadata".to_string(),
        ));
    }
    let (metadata, payload) = rest.split_at(meta_size);
    if crc32c(&frame[4 + command_size + 2 + 4..]) != checksum {
        return Err(ConnectorError::Connection(
            "pulsar checksum mismatch".to_string(),
        ));
    }
    Ok((command, metadata.to_vec(), payload.to_vec()))
}

/// Read the command `type` varint (field 1) from a command body.
pub fn decode_command_type(command: &[u8]) -> Result<u32> {
    let mut cursor = command;
    while !cursor.is_empty() {
        let (tag, used) = decode_varint(cursor)
            .map_err(|_| ConnectorError::Connection("pulsar truncated command tag".to_string()))?;
        cursor = &cursor[used..];
        let (field, wire) = ((tag >> 3) as u32, (tag & 0x07) as u32);
        if field == fields::TYPE && wire == 0 {
            let (value, _) = decode_varint(cursor).map_err(|_| {
                ConnectorError::Connection("pulsar truncated command type".to_string())
            })?;
            return Ok(value as u32);
        }
        let skip = match wire {
            0 => decode_varint(cursor)
                .map(|(_, used)| used)
                .map_err(|_| ConnectorError::Connection("pulsar truncated varint".to_string()))?,
            1 => 8,
            2 => {
                let (len, used) = decode_varint(cursor).map_err(|_| {
                    ConnectorError::Connection("pulsar truncated length".to_string())
                })?;
                used + len as usize
            }
            5 => 4,
            _ => {
                return Err(ConnectorError::Connection(
                    "pulsar bad command wire type".to_string(),
                ))
            }
        };
        if cursor.len() < skip {
            return Err(ConnectorError::Connection(
                "pulsar truncated command field".to_string(),
            ));
        }
        cursor = &cursor[skip..];
    }
    Err(ConnectorError::Connection(
        "pulsar command lacks a type".to_string(),
    ))
}

/// Decoded message metadata.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DecodedMetadata {
    pub producer_name: String,
    pub sequence_id: u64,
    pub publish_time: u64,
    pub properties: Vec<(String, String)>,
    pub partition_key: Option<String>,
    pub event_time: Option<u64>,
}

/// Decode `MessageMetadata` bytes.
pub fn decode_metadata(buf: &[u8]) -> Result<DecodedMetadata> {
    fn read_string(cursor: &[u8]) -> Result<(String, usize)> {
        let (len, used) = decode_varint(cursor)
            .map_err(|_| ConnectorError::Connection("pulsar truncated string".to_string()))?;
        let len = len as usize;
        if cursor.len() < used + len {
            return Err(ConnectorError::Connection(
                "pulsar truncated string body".to_string(),
            ));
        }
        let text = std::str::from_utf8(&cursor[used..used + len])
            .map_err(|_| ConnectorError::Connection("pulsar string not UTF-8".to_string()))?
            .to_string();
        Ok((text, used + len))
    }
    fn skip(cursor: &[u8], wire: u32) -> Result<usize> {
        match wire {
            0 => decode_varint(cursor)
                .map(|(_, used)| used)
                .map_err(|_| ConnectorError::Connection("pulsar truncated varint".to_string())),
            1 => Ok(8),
            2 => {
                let (len, used) = decode_varint(cursor).map_err(|_| {
                    ConnectorError::Connection("pulsar truncated length".to_string())
                })?;
                Ok(used + len as usize)
            }
            5 => Ok(4),
            _ => Err(ConnectorError::Connection(
                "pulsar bad metadata wire type".to_string(),
            )),
        }
    }
    let mut metadata = DecodedMetadata::default();
    let mut cursor = buf;
    while !cursor.is_empty() {
        let (tag, used) = decode_varint(cursor)
            .map_err(|_| ConnectorError::Connection("pulsar truncated metadata tag".to_string()))?;
        cursor = &cursor[used..];
        let (field, wire) = ((tag >> 3) as u32, (tag & 0x07) as u32);
        match (field, wire) {
            (fields::PRODUCER_NAME_META, 2) => {
                let (value, used) = read_string(cursor)?;
                metadata.producer_name = value;
                cursor = &cursor[used..];
            }
            (fields::SEQUENCE_ID_META, 0) => {
                let (value, used) = decode_varint(cursor).map_err(|_| {
                    ConnectorError::Connection("pulsar truncated sequence".to_string())
                })?;
                metadata.sequence_id = value;
                cursor = &cursor[used..];
            }
            (fields::PUBLISH_TIME, 0) => {
                let (value, used) = decode_varint(cursor).map_err(|_| {
                    ConnectorError::Connection("pulsar truncated publish time".to_string())
                })?;
                metadata.publish_time = value;
                cursor = &cursor[used..];
            }
            (fields::PROPERTIES, 2) => {
                let (len, used) = decode_varint(cursor).map_err(|_| {
                    ConnectorError::Connection("pulsar truncated property".to_string())
                })?;
                let end = used + len as usize;
                if cursor.len() < end {
                    return Err(ConnectorError::Connection(
                        "pulsar truncated property body".to_string(),
                    ));
                }
                let mut entry = &cursor[used..end];
                let (mut key, mut value) = (String::new(), String::new());
                while !entry.is_empty() {
                    let (tag, used) = decode_varint(entry).map_err(|_| {
                        ConnectorError::Connection("pulsar truncated kv tag".to_string())
                    })?;
                    entry = &entry[used..];
                    match ((tag >> 3) as u32, (tag & 0x07) as u32) {
                        (fields::KV_KEY, 2) => {
                            let (text, used) = read_string(entry)?;
                            key = text;
                            entry = &entry[used..];
                        }
                        (fields::KV_VALUE, 2) => {
                            let (text, used) = read_string(entry)?;
                            value = text;
                            entry = &entry[used..];
                        }
                        (_, wire) => {
                            let used = skip(entry, wire)?;
                            entry = &entry[used..];
                        }
                    }
                }
                metadata.properties.push((key, value));
                cursor = &cursor[end..];
            }
            (fields::PARTITION_KEY, 2) => {
                let (value, used) = read_string(cursor)?;
                metadata.partition_key = Some(value);
                cursor = &cursor[used..];
            }
            (fields::EVENT_TIME, 0) => {
                let (value, used) = decode_varint(cursor).map_err(|_| {
                    ConnectorError::Connection("pulsar truncated event time".to_string())
                })?;
                metadata.event_time = Some(value);
                cursor = &cursor[used..];
            }
            (_, wire) => {
                let used = skip(cursor, wire)?;
                cursor = &cursor[used..];
            }
        }
    }
    Ok(metadata)
}

/// One produced message with routing metadata.
#[derive(Debug, Clone)]
pub struct PulsarMessage {
    pub sequence_id: u64,
    pub event_time_ms: u64,
    pub partition_key: Option<String>,
    pub properties: Vec<(String, String)>,
    pub payload: Vec<u8>,
}

/// Encode message metadata for one message.
pub fn encode_metadata(
    producer_name: &str,
    message: &PulsarMessage,
    publish_time_ms: u64,
) -> Vec<u8> {
    let mut out = Vec::new();
    encode_string_field(fields::PRODUCER_NAME_META, producer_name, &mut out);
    encode_varint_field(fields::SEQUENCE_ID_META, message.sequence_id, &mut out);
    encode_varint_field(fields::PUBLISH_TIME, publish_time_ms, &mut out);
    for (key, value) in &message.properties {
        let mut entry = Vec::new();
        encode_string_field(fields::KV_KEY, key, &mut entry);
        encode_string_field(fields::KV_VALUE, value, &mut entry);
        encode_tag(fields::PROPERTIES, 2, &mut out);
        encode_varint(entry.len() as u64, &mut out);
        out.extend_from_slice(&entry);
    }
    if let Some(partition_key) = &message.partition_key {
        encode_string_field(fields::PARTITION_KEY, partition_key, &mut out);
    }
    encode_varint_field(fields::EVENT_TIME, message.event_time_ms, &mut out);
    out
}

// ---------------------------------------------------------------------------
// Transport.
// ---------------------------------------------------------------------------

#[async_trait]
pub trait PulsarTransport: Send + Sync {
    async fn produce(&self, topic_path: &str, batch: Vec<PulsarMessage>) -> Result<()>;
}

/// One captured produce call.
#[derive(Debug, Clone)]
pub struct CapturedProduce {
    pub topic_path: String,
    pub messages: Vec<PulsarMessage>,
}

/// In-memory transport capturing every batch (tests, dry runs).
#[derive(Debug, Default)]
pub struct MemoryPulsarTransport {
    captured: parking_lot::Mutex<Vec<CapturedProduce>>,
    failures_left: parking_lot::Mutex<usize>,
    calls: AtomicU64,
}

impl MemoryPulsarTransport {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fail the next `n` calls with a transport error (backoff tests).
    pub fn fail_next(&self, n: usize) {
        *self.failures_left.lock() = n;
    }

    pub fn captured(&self) -> Vec<CapturedProduce> {
        self.captured.lock().clone()
    }

    pub fn calls(&self) -> u64 {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl PulsarTransport for MemoryPulsarTransport {
    async fn produce(&self, topic_path: &str, batch: Vec<PulsarMessage>) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut failures = self.failures_left.lock();
        if *failures > 0 {
            *failures -= 1;
            return Err(ConnectorError::Connection("mock pulsar down".to_string()));
        }
        self.captured.lock().push(CapturedProduce {
            topic_path: topic_path.to_string(),
            messages: batch,
        });
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Maintained-driver transport (production write path).
// ---------------------------------------------------------------------------

/// One driver message: payload plus routing metadata.
struct QualSend {
    payload: Vec<u8>,
    properties: HashMap<String, String>,
    partition_key: Option<String>,
    event_time: Option<u64>,
}

impl pulsar::SerializeMessage for QualSend {
    fn serialize_message(
        input: Self,
    ) -> std::result::Result<pulsar::producer::Message, pulsar::Error> {
        Ok(pulsar::producer::Message {
            payload: input.payload,
            properties: input.properties,
            partition_key: input.partition_key,
            event_time: input.event_time,
            ..Default::default()
        })
    }
}

/// Map a driver failure into the sink contract. Every driver error
/// becomes `Connection` (restore the batch, back off, retry):
/// the standalone qualification uses no auth, so no driver error
/// is known-terminal here.
///
/// TODO(parity): which driver errors must be terminal versus
/// retryable once authenticated tenants are qualified?
fn map_driver_error(context: &str, error: pulsar::Error) -> ConnectorError {
    ConnectorError::Connection(format!("pulsar driver {context} failed: {error:?}"))
}

/// Production transport on the maintained `pulsar` driver.
///
/// The client is created lazily on first publish and cached; the
/// driver dials on first use. Creation validates our config only,
/// so management-plane validation stays offline-safe: creating the
/// sink never touches the network. Each `produce` call builds one
/// producer for its topic and drops it after the batch, so the
/// transport caches one client and no per-topic state: nothing
/// accumulates on the publish path.
pub struct DriverPulsarTransport {
    config: PulsarSinkConfig,
    client: tokio::sync::Mutex<Option<pulsar::Pulsar<pulsar::TokioExecutor>>>,
}

impl DriverPulsarTransport {
    pub fn new(config: &PulsarSinkConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config: config.clone(),
            client: tokio::sync::Mutex::new(None),
        })
    }

    pub fn config(&self) -> &PulsarSinkConfig {
        &self.config
    }

    async fn client(&self) -> Result<pulsar::Pulsar<pulsar::TokioExecutor>> {
        if let Some(client) = self.client.lock().await.clone() {
            return Ok(client);
        }
        let mut guard = self.client.lock().await;
        if let Some(client) = guard.clone() {
            return Ok(client);
        }
        // Reason: the driver speaks `pulsar://` only. An `http(s)://`
        // address names the admin entry, not the binary entry, so it
        // fails closed here instead of dialing the wrong port.
        let url = self.config.service_url.clone();
        if !url.starts_with("pulsar://") && !url.starts_with("pulsar+ssl://") {
            return Err(ConnectorError::Dispatch(format!(
                "pulsar driver needs a pulsar:// service_url, got {url:?}"
            )));
        }
        let builder = pulsar::Pulsar::builder(url.as_str(), pulsar::TokioExecutor);
        // Reason: token auth against a secured server stays
        // unqualified (see the test docs), so the driver refuses a
        // token here instead of sending a token the server may
        // ignore. TODO(parity): wire the driver token
        // authenticator once a secured server is available.
        if let PulsarAuth::Token { .. } = &self.config.auth {
            return Err(ConnectorError::Dispatch(
                "pulsar driver token auth is not qualified yet; use no auth".to_string(),
            ));
        }
        let client = builder
            .build()
            .await
            .map_err(|e| map_driver_error("connect", e))?;
        *guard = Some(client.clone());
        Ok(client)
    }

    /// Drop the cached driver client so the next publish redials
    /// with the current configuration.
    pub async fn reconnect(&self) -> Result<()> {
        *self.client.lock().await = None;
        self.client().await.map(|_| ())
    }
}

#[async_trait]
impl PulsarTransport for DriverPulsarTransport {
    async fn produce(&self, topic_path: &str, batch: Vec<PulsarMessage>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let client = self.client().await?;
        let timeout = self.config.timeout();
        let mut producer = client
            .producer()
            .with_topic(topic_path)
            .build()
            .await
            .map_err(|e| map_driver_error("producer", e))?;
        for message in &batch {
            let send = async {
                producer
                    .send_non_blocking(QualSend {
                        payload: message.payload.clone(),
                        properties: message.properties.iter().cloned().collect(),
                        partition_key: message.partition_key.clone(),
                        event_time: Some(message.event_time_ms),
                    })
                    .await
                    .map_err(|e| map_driver_error("send", e))?
                    .await
                    .map_err(|e| map_driver_error("send", e))
            };
            tokio::time::timeout(timeout, send).await.map_err(|_| {
                ConnectorError::Connection("pulsar driver send timed out".to_string())
            })??;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Sink.
// ---------------------------------------------------------------------------

/// One buffered event with routing metadata.
#[derive(Debug, Clone)]
struct PulsarRow {
    topic: String,
    payload: Vec<u8>,
    qos: u8,
    millis: i64,
    sequence_id: u64,
}

struct PulsarBuffer {
    queue: BatchQueue<PulsarRow>,
    bytes: usize,
}

/// Pulsar producer sink: buffers events, produces framed batches.
pub struct PulsarSink {
    config: PulsarSinkConfig,
    transport: Arc<dyn PulsarTransport>,
    buffer: parking_lot::Mutex<PulsarBuffer>,
    backoff: parking_lot::Mutex<BackoffState>,
    sequence: AtomicU64,
    sent_batches: AtomicU64,
    sent_records: AtomicU64,
}

impl PulsarSink {
    pub fn new(config: PulsarSinkConfig, transport: Arc<dyn PulsarTransport>) -> Result<Self> {
        config.validate()?;
        let linger = config.effective_linger();
        Ok(Self {
            buffer: parking_lot::Mutex::new(PulsarBuffer {
                queue: BatchQueue::new(config.effective_batch_size(), linger),
                bytes: 0,
            }),
            config,
            transport,
            backoff: parking_lot::Mutex::new(BackoffState::default()),
            sequence: AtomicU64::new(0),
            sent_batches: AtomicU64::new(0),
            sent_records: AtomicU64::new(0),
        })
    }

    pub fn config(&self) -> &PulsarSinkConfig {
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

    /// Build wire messages grouped by destination path (first-seen
    /// order), so mixed-topic batches route each message correctly.
    /// Rows share one flush timestamp for event-time coherence.
    fn grouped_messages(&self, rows: &[PulsarRow]) -> Result<Vec<(String, Vec<PulsarMessage>)>> {
        let mut groups: Vec<(String, Vec<PulsarMessage>)> = Vec::new();
        for row in rows {
            let topic_path =
                self.config
                    .resolve_topic(&row.topic, qos_from(row.qos), row.millis)?;
            let partition_key = match &self.config.partition_key_template {
                Some(template) => Some(self.config.event_vars(
                    &row.topic,
                    &row.payload,
                    qos_from(row.qos),
                    row.millis,
                    template,
                )?),
                None => None,
            };
            let mut properties = vec![
                ("mqtt_topic".to_string(), row.topic.clone()),
                ("mqtt_qos".to_string(), row.qos.to_string()),
            ];
            for (name, template) in &self.config.properties {
                properties.push((
                    name.clone(),
                    self.config.event_vars(
                        &row.topic,
                        &row.payload,
                        qos_from(row.qos),
                        row.millis,
                        template,
                    )?,
                ));
            }
            let message = PulsarMessage {
                sequence_id: row.sequence_id,
                event_time_ms: row.millis.max(0) as u64,
                partition_key,
                properties,
                payload: row.payload.clone(),
            };
            match groups.iter_mut().find(|(path, _)| path == &topic_path) {
                Some((_, messages)) => messages.push(message),
                None => groups.push((topic_path, vec![message])),
            }
        }
        Ok(groups)
    }

    /// Flush buffered rows (no-op when empty). Transport failures
    /// retry in place up to `max_retries`; exhaustion restores the
    /// buffer, engages backoff, and propagates.
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
        let groups = self.grouped_messages(&rows)?;
        let record_count = rows.len() as u64;
        let max_retries = self.config.max_retries.unwrap_or(usize::MAX);
        let mut attempt = 0usize;
        loop {
            let mut outcome: Result<()> = Ok(());
            for (topic_path, messages) in &groups {
                if let Err(e) = self.transport.produce(topic_path, messages.clone()).await {
                    outcome = Err(e);
                    break;
                }
            }
            match outcome {
                Ok(()) => {
                    self.backoff.lock().success();
                    self.sent_batches.fetch_add(1, Ordering::Relaxed);
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
        rows: Vec<PulsarRow>,
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

    /// Validate + buffer one event with a fresh sequence id. Returns
    /// true when the batch is full, stale, or over the byte limit.
    fn buffer_row(&self, topic: &Topic, payload: &Bytes, qos: QoS) -> Result<bool> {
        if topic.as_str().is_empty() {
            return Err(ConnectorError::Dispatch(
                "pulsar row requires a non-empty topic".to_string(),
            ));
        }
        // Destination must resolve at buffer time: bad templates fail
        // loudly instead of poisoning the batch at flush.
        let millis = now_millis();
        self.config.resolve_topic(topic.as_str(), qos, millis)?;
        let row = PulsarRow {
            topic: topic.as_str().to_string(),
            payload: payload.to_vec(),
            qos: u8::from(qos),
            millis,
            sequence_id: self.sequence.fetch_add(1, Ordering::SeqCst),
        };
        let added = row.payload.len() + 128;
        let mut buffer = self.buffer.lock();
        let full = buffer.queue.push(row);
        buffer.bytes = buffer.bytes.saturating_add(added);
        Ok(full || buffer.bytes >= self.config.effective_batch_bytes())
    }
}

fn qos_from(value: u8) -> QoS {
    QoS::try_from(value).unwrap_or(QoS::AtMostOnce)
}

#[async_trait]
impl Sink for PulsarSink {
    async fn send(&self, topic: &Topic, payload: &Bytes, qos: QoS) -> Result<()> {
        if self.buffer_row(topic, payload, qos)? {
            self.flush().await?;
        }
        Ok(())
    }

    fn kind(&self) -> &'static str {
        "pulsar"
    }
}

/// Management connector handle pairing an id with a Pulsar sink.
pub struct PulsarConnector {
    id: String,
    sink: Arc<PulsarSink>,
}

impl PulsarConnector {
    pub fn new(id: impl Into<String>, sink: Arc<PulsarSink>) -> Self {
        Self {
            id: id.into(),
            sink,
        }
    }
}

impl broker_connectors::Connector for PulsarConnector {
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

    fn test_config(url: &str) -> PulsarSinkConfig {
        PulsarSinkConfig {
            service_url: url.to_string(),
            tenant: "public".to_string(),
            namespace: "default".to_string(),
            topic: "${topic}".to_string(),
            auth: PulsarAuth::None,
            partition_key_template: Some("${client_id}".to_string()),
            properties: HashMap::from([("source".to_string(), "indramqtt".to_string())]),
            batch_size: Some(200),
            batch_bytes: Some(2_097_152),
            linger_ms: Some(10),
            max_retries: Some(3),
            initial_backoff_ms: Some(100),
            max_backoff_ms: Some(2_000),
            timeout_ms: None,
        }
    }

    fn test_sink(config: PulsarSinkConfig) -> (Arc<PulsarSink>, Arc<MemoryPulsarTransport>) {
        let transport = Arc::new(MemoryPulsarTransport::new());
        let sink = Arc::new(PulsarSink::new(config, transport.clone()).unwrap());
        (sink, transport)
    }

    #[test]
    fn test_config_validation() {
        assert!(test_config("pulsar://127.0.0.1:6650").validate().is_ok());
        assert!(test_config("http://127.0.0.1:8080").validate().is_ok());
        assert!(test_config("127.0.0.1:6650").validate().is_ok());
        assert!(test_config("broker").validate().is_ok());

        assert!(test_config("").validate().is_err());
        assert!(test_config("kafka://127.0.0.1:9092").validate().is_err());
        assert!(test_config("pulsar://:6650").validate().is_err());

        let mut config = test_config("pulsar://127.0.0.1:6650");
        config.tenant = "UPPER SPACE".to_string();
        assert!(config.validate().is_err());
        config.tenant = "public".to_string();

        config.topic.clear();
        assert!(config.validate().is_err());
        config.topic = "${topic}".to_string();

        config.auth = PulsarAuth::Token {
            token: "  ".to_string(),
        };
        assert!(config.validate().is_err());
        config.auth = PulsarAuth::Token {
            token: "jwt".to_string(),
        };
        assert!(config.validate().is_ok());

        config.batch_size = Some(0);
        assert!(config.validate().is_err());
        // Unbounded + huge depths accepted: zero clamped ceilings.
        config.batch_size = None;
        config.batch_bytes = Some(10_000_000);
        config.max_retries = None;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_topic_path_resolution() {
        let config = test_config("pulsar://h:6650");
        assert_eq!(
            config
                .resolve_topic("sensors/t1", QoS::AtMostOnce, 0)
                .unwrap(),
            "persistent://public/default/sensors.t1"
        );
        assert_eq!(
            config.resolve_topic("a b?c", QoS::AtMostOnce, 0).unwrap(),
            "persistent://public/default/a-b-c"
        );

        let mut fixed = test_config("pulsar://h:6650");
        fixed.topic = "iot-telemetry".to_string();
        assert_eq!(
            fixed.resolve_topic("anything", QoS::AtMostOnce, 0).unwrap(),
            "persistent://public/default/iot-telemetry"
        );

        assert!(config.resolve_topic("", QoS::AtMostOnce, 0).is_err());
    }

    #[test]
    fn test_crc32c_check_value() {
        // Standard CRC32C check value for "123456789".
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
        assert_eq!(crc32c(b""), 0x0000_0000);
    }

    #[test]
    fn test_frame_envelope_roundtrip() {
        let metadata = encode_metadata(
            "producer-1",
            &PulsarMessage {
                sequence_id: 41,
                event_time_ms: 1_789_211_889_123,
                partition_key: Some("device-001".to_string()),
                properties: vec![("mqtt_topic".to_string(), "sensors/t1".to_string())],
                payload: b"hi".to_vec(),
            },
            1_789_211_889_124,
        );
        let frame = encode_message_frame(&encode_send(7, 41), &metadata, b"hi");
        let (command, decoded_meta, payload) = decode_frame(&frame).unwrap();
        assert_eq!(
            decode_command_type(&command).unwrap(),
            PulsarCommand::Send as u32
        );
        let meta = decode_metadata(&decoded_meta).unwrap();
        assert_eq!(meta.producer_name, "producer-1");
        assert_eq!(meta.sequence_id, 41);
        assert_eq!(meta.publish_time, 1_789_211_889_124);
        assert_eq!(meta.partition_key.as_deref(), Some("device-001"));
        assert_eq!(meta.event_time, Some(1_789_211_889_123));
        assert_eq!(
            meta.properties,
            vec![("mqtt_topic".to_string(), "sensors/t1".to_string())]
        );
        assert_eq!(payload, b"hi");

        // Corrupted bytes fail the checksum.
        let mut bad = frame.clone();
        let last = bad.len() - 1;
        bad[last] ^= 0xFF;
        assert!(decode_frame(&bad).is_err());

        // Command-only frames round-trip without payload sections.
        let connected = encode_command_frame(&encode_connected());
        let (command, meta, payload) = decode_frame(&connected).unwrap();
        assert_eq!(
            decode_command_type(&command).unwrap(),
            PulsarCommand::Connected as u32
        );
        assert!(meta.is_empty() && payload.is_empty());
    }

    #[tokio::test]
    async fn test_batch_packing_and_sequences() {
        let mut config = test_config("pulsar://h:6650");
        config.batch_size = Some(2);
        let (sink, transport) = test_sink(config);
        let topic = Topic::new("sensors/t1").unwrap();
        for v in [1, 2, 3] {
            sink.send(
                &topic,
                &Bytes::from(format!("{{\"v\":{v}}}")),
                QoS::AtMostOnce,
            )
            .await
            .unwrap();
        }
        // Two rows flushed on count with gapless sequences; one row held.
        assert_eq!(sink.sent_batches(), 1);
        assert_eq!(sink.buffered_rows(), 1);
        sink.flush().await.unwrap();
        assert_eq!(sink.sent_batches(), 2);
        assert_eq!(sink.sent_records(), 3);

        let captured = transport.captured();
        assert_eq!(captured.len(), 2);
        assert_eq!(
            captured[0].topic_path,
            "persistent://public/default/sensors.t1"
        );
        assert_eq!(captured[0].messages.len(), 2);
        assert_eq!(captured[1].messages.len(), 1);
        let seqs: Vec<u64> = captured
            .iter()
            .flat_map(|batch| batch.messages.iter().map(|message| message.sequence_id))
            .collect();
        assert_eq!(seqs, vec![0, 1, 2]);
        // Keyed routing + injected properties on every message.
        for message in captured.iter().flat_map(|batch| &batch.messages) {
            assert_eq!(message.partition_key.as_deref(), Some(""));
            assert!(message
                .properties
                .iter()
                .any(|(k, v)| k == "mqtt_topic" && v == "sensors/t1"));
            assert!(message
                .properties
                .iter()
                .any(|(k, v)| k == "source" && v == "indramqtt"));
        }
    }

    #[tokio::test]
    async fn test_mixed_topics_group_by_destination() {
        let (sink, transport) = test_sink(test_config("pulsar://h:6650"));
        sink.send(
            &Topic::new("a/1").unwrap(),
            &Bytes::from("x"),
            QoS::AtMostOnce,
        )
        .await
        .unwrap();
        sink.send(
            &Topic::new("b/2").unwrap(),
            &Bytes::from("y"),
            QoS::AtMostOnce,
        )
        .await
        .unwrap();
        sink.flush().await.unwrap();
        // One produce call per destination path, payloads routed intact.
        let captured = transport.captured();
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].topic_path, "persistent://public/default/a.1");
        assert_eq!(captured[0].messages[0].payload, b"x");
        assert_eq!(captured[1].topic_path, "persistent://public/default/b.2");
        assert_eq!(captured[1].messages[0].payload, b"y");
        assert_eq!(sink.sent_records(), 2);
    }

    #[tokio::test]
    async fn test_retry_then_fail_fast() {
        let mut config = test_config("pulsar://h:6650");
        config.batch_size = Some(10);
        config.initial_backoff_ms = Some(1);
        config.max_backoff_ms = Some(2);
        let (sink, transport) = test_sink(config);
        transport.fail_next(100);

        sink.send(
            &Topic::new("t").unwrap(),
            &Bytes::from("{}"),
            QoS::AtMostOnce,
        )
        .await
        .unwrap();
        let err = sink.flush().await.expect_err("mock down must fail");
        assert!(matches!(err, ConnectorError::Connection(_)));
        assert_eq!(sink.buffered_rows(), 1);
        let calls = transport.calls();
        assert!(sink.flush().await.is_err());
        assert_eq!(transport.calls(), calls);
        assert_eq!(sink.sent_batches(), 0);
    }

    /// It fails closed when the environment lacks the named address.
    fn qual_required(name: &str) -> String {
        std::env::var(name)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| panic!("{name} must name a real server for qualification"))
    }

    /// Raw consumer payload: the driver delivers bytes unchanged.
    #[derive(Debug, Clone)]
    struct QualBytes(Vec<u8>);

    impl pulsar::DeserializeMessage for QualBytes {
        type Output = Self;

        fn deserialize_message(payload: &pulsar::message::Payload) -> Self::Output {
            QualBytes(payload.data.clone())
        }
    }

    /// Read the server version from Prometheus text. Comment lines
    /// (starting with `#`) never carry a version, so skip them.
    /// Return the `version="..."` label of the first version sample.
    fn qual_pulsar_version(text: &str) -> Option<String> {
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if !(line.contains("pulsar_version") || line.contains("broker_version")) {
                continue;
            }
            if let Some(start) = line.find("version=\"") {
                let rest = &line[start + "version=\"".len()..];
                if let Some(end) = rest.find('"') {
                    let version = rest[..end].trim();
                    if !version.is_empty() {
                        return Some(version.to_string());
                    }
                }
            }
            return Some(line.to_string());
        }
        None
    }

    /// Remove one topic through the admin entry. A missing topic is
    /// fine: the next produce recreates it.
    async fn qual_delete_topic(client: &reqwest::Client, http_url: &str, short: &str) {
        let url = format!("{http_url}/admin/v2/persistent/public/default/{short}?force=true");
        match client
            .delete(url)
            .timeout(Duration::from_secs(30))
            .send()
            .await
        {
            Ok(response) => eprintln!("qual delete {short}: status={}", response.status()),
            Err(error) => eprintln!("qual delete {short} note: {error}"),
        }
    }

    /// Flush with retry on transport failures. Terminal failures
    /// panic at once (fail closed).
    async fn qual_flush(sink: &PulsarSink, what: &str) {
        for attempt in 1..=10 {
            match sink.flush().await {
                Ok(()) => return,
                Err(ConnectorError::Connection(message)) if attempt < 10 => {
                    eprintln!("qual flush {what} retryable (attempt {attempt}): {message}");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                Err(error) => panic!("qual flush {what} terminal: {error:?}"),
            }
        }
        panic!("qual flush {what} never recovered");
    }

    /// Qualification against a real server through the maintained
    /// driver ([`DriverPulsarTransport`]).
    ///
    /// Run with e.g.:
    /// `PULSAR_URL=pulsar://127.0.0.1:6650 \
    ///  PULSAR_HTTP_URL=http://127.0.0.1:8080 \
    ///  PULSAR_TOPIC=qual-b335 PULSAR_SUBSCRIPTION=qual-sub \
    ///  cargo test -p broker-connectors-enterprise --all-features --lib pulsar::tests::test_qualify_driver_write_path -- --ignored --nocapture --test-threads=1`
    ///
    /// It proves the health entry answers. It streams 500 rows
    /// through the sink on the driver transport. It reads all 500
    /// back through an independent driver consumer. It asserts the
    /// exact count plus the first and the last payload. It proves a
    /// wrong node fails closed with the node text. It removes the
    /// topic. Panics when its environment is missing (fail closed,
    /// never skips).
    ///
    /// TODO(parity): the standalone server uses no auth, so token
    /// auth against a secured server stays unqualified. Which
    /// driver errors must be terminal for billed tenants is open
    /// (see [`map_driver_error`]).
    #[tokio::test]
    #[ignore = "needs a real Pulsar server (see PULSAR_* env)"]
    async fn test_qualify_driver_write_path() {
        use futures::TryStreamExt as _;
        use pulsar::message::proto::command_subscribe::SubType;

        const ROWS: usize = 500;

        let url = qual_required("PULSAR_URL");
        let http_url = qual_required("PULSAR_HTTP_URL");
        let short = qual_required("PULSAR_TOPIC");
        let sub_base = qual_required("PULSAR_SUBSCRIPTION");
        assert!(
            url.starts_with("pulsar://") || url.starts_with("pulsar+ssl://"),
            "qual url must use pulsar://, got {url:?}"
        );

        let http = reqwest::Client::new();
        // The health entry proves the real product answers.
        let health = http
            .get(format!("{http_url}/admin/v2/brokers/health"))
            .timeout(Duration::from_secs(30))
            .send()
            .await
            .unwrap_or_else(|e| panic!("qual health probe failed: {e}"));
        assert_eq!(health.status().as_u16(), 200, "qual health status");
        let body = health.text().await.unwrap_or_default();
        assert!(
            body.to_lowercase().contains("ok"),
            "qual health body: {body:?}"
        );
        eprintln!("qual server: health={body:?} url={url}");

        // Best-effort version line for the report. The run omits it
        // when the server reports none (never a constant instead).
        match http
            .get(format!("{http_url}/metrics"))
            .timeout(Duration::from_secs(30))
            .send()
            .await
        {
            Ok(response) => {
                let text = response.text().await.unwrap_or_default();
                if let Some(version) = qual_pulsar_version(&text) {
                    eprintln!("qual version: {version}");
                }
            }
            Err(error) => eprintln!("qual version probe note: {error}"),
        }

        // Fresh topic for the exact-count assertion.
        qual_delete_topic(&http, &http_url, &short).await;
        let full_topic = format!("persistent://public/default/{short}");

        // The consumer subscribes before the first produce, so no
        // row passes it by. The subscription is unique per run, so
        // an older run never leaks rows into this count.
        let subscription = format!("{sub_base}-{}-{}", now_millis(), std::process::id());
        let driver: pulsar::Pulsar<_> =
            pulsar::Pulsar::builder(url.as_str(), pulsar::TokioExecutor)
                .build()
                .await
                .unwrap_or_else(|e| panic!("qual driver connects: {e:?}"));
        let mut consumer: pulsar::Consumer<QualBytes, _> = driver
            .consumer()
            .with_topic(full_topic.as_str())
            .with_subscription(subscription.as_str())
            .with_subscription_type(SubType::Exclusive)
            .build()
            .await
            .unwrap_or_else(|e| panic!("qual consumer subscribes: {e:?}"));

        // The sink under test rides the production transport.
        let config = PulsarSinkConfig {
            service_url: url.clone(),
            tenant: "public".to_string(),
            namespace: "default".to_string(),
            topic: short.clone(),
            auth: PulsarAuth::None,
            partition_key_template: None,
            properties: HashMap::new(),
            batch_size: Some(1000),
            batch_bytes: Some(2_097_152),
            linger_ms: Some(10),
            max_retries: Some(3),
            initial_backoff_ms: Some(100),
            max_backoff_ms: Some(2_000),
            timeout_ms: Some(30_000),
        };
        config.validate().expect("qual config validates");
        let transport =
            Arc::new(DriverPulsarTransport::new(&config).expect("qual transport builds"));
        // Exercise the production accessors. The config stays attached
        // to the transport. Reconnect redials with current config.
        assert_eq!(transport.config().topic, short);
        transport.reconnect().await.expect("qual reconnect redials");
        let sink = Arc::new(PulsarSink::new(config, transport).expect("qual sink builds"));
        assert_eq!(sink.kind(), "pulsar");

        // One batch holds all rows: a single flush sends them.
        // Reason: 1000 exceeds 500, so no send flushes early.
        for seq in 0..ROWS {
            let payload = Bytes::from(format!(r#"{{"seq":{seq},"client_id":"dev-{seq:04}"}}"#));
            sink.send(
                &Topic::new(format!("qual/pulsar/dev-{seq:04}")).expect("qual topic builds"),
                &payload,
                QoS::AtLeastOnce,
            )
            .await
            .expect("qual send buffers");
        }
        qual_flush(&sink, "rows").await;
        assert_eq!(sink.sent_records(), ROWS as u64, "qual row count");
        assert_eq!(sink.buffered_rows(), 0, "qual buffer drains");

        // Read back through the independent consumer. At-least-once
        // permits duplicates, never loss: every key must appear.
        let mut seen: HashMap<i64, String> = HashMap::new();
        let mut total = 0usize;
        let mut duplicates = 0usize;
        while seen.len() < ROWS {
            let next = tokio::time::timeout(Duration::from_secs(180), consumer.try_next())
                .await
                .expect("qual recv timeout")
                .unwrap_or_else(|e| panic!("qual recv: {e:?}"));
            let message = next.expect("qual message");
            consumer
                .ack(&message)
                .await
                .unwrap_or_else(|e| panic!("qual ack: {e:?}"));
            let QualBytes(raw) = message.deserialize();
            total += 1;
            let text = String::from_utf8(raw).expect("qual payload is UTF-8");
            let parsed: serde_json::Value =
                serde_json::from_str(&text).expect("qual payload is JSON");
            let seq = parsed
                .get("seq")
                .and_then(|v| v.as_i64())
                .expect("qual payload carries seq");
            if seen.insert(seq, text).is_some() {
                duplicates += 1;
            } else if seen.len().is_multiple_of(100) || seen.len() == ROWS {
                eprintln!(
                    "qual progress: distinct={} total={total} duplicates={duplicates}",
                    seen.len()
                );
            }
        }
        for seq in 0..ROWS as i64 {
            assert!(seen.contains_key(&seq), "qual missing seq {seq}");
        }
        let first = seen.get(&0).expect("qual first row");
        let last = seen.get(&(ROWS as i64 - 1)).expect("qual last row");
        assert!(first.contains("\"seq\":0"), "qual first row: {first}");
        assert!(
            last.contains(&format!("\"seq\":{}", ROWS - 1)),
            "qual last row: {last}"
        );
        eprintln!("qual delivery: distinct={ROWS} total={total} duplicates={duplicates}");
        eprintln!("qual rows asserted: count={ROWS} first={first} last={last}");

        // Fault: a wrong node fails closed with the node text. The
        // probe proves the port refuses; the sink must report it.
        let probe = tokio::net::TcpStream::connect("127.0.0.1:1")
            .await
            .expect_err("qual probe: port 1 must refuse");
        let probe_text = probe.to_string().to_lowercase();
        assert!(
            probe_text.contains("refus"),
            "qual probe text: {probe_text}"
        );
        let bad_config = PulsarSinkConfig {
            service_url: "pulsar://127.0.0.1:1".to_string(),
            tenant: "public".to_string(),
            namespace: "default".to_string(),
            topic: short.clone(),
            auth: PulsarAuth::None,
            partition_key_template: None,
            properties: HashMap::new(),
            batch_size: Some(1000),
            batch_bytes: Some(2_097_152),
            linger_ms: Some(10),
            max_retries: Some(0),
            initial_backoff_ms: Some(1),
            max_backoff_ms: Some(2),
            timeout_ms: Some(10_000),
        };
        let bad_transport =
            Arc::new(DriverPulsarTransport::new(&bad_config).expect("qual bad transport builds"));
        let bad_sink =
            Arc::new(PulsarSink::new(bad_config, bad_transport).expect("qual bad sink builds"));
        bad_sink
            .send(
                &Topic::new("qual/pulsar/bad").expect("qual bad topic builds"),
                &Bytes::from_static(b"{\"seq\":-1}"),
                QoS::AtLeastOnce,
            )
            .await
            .expect("qual bad send buffers");
        let error = bad_sink.flush().await.expect_err("wrong node must fail");
        match &error {
            ConnectorError::Connection(message)
            | ConnectorError::Dispatch(message)
            | ConnectorError::UnknownConnector(message) => assert!(
                message.to_lowercase().contains("refus"),
                "qual fault must carry the node text, got {message:?}"
            ),
        }
        eprintln!("qual fault asserted: wrong node fails closed");

        // Cleanup: remove the topic. A failure is tolerated and
        // reported: the next run deletes stale state first.
        qual_delete_topic(&http, &http_url, &short).await;
        eprintln!("qual done: rows={ROWS} topic={short} cleaned");
    }
}
