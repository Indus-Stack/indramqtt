//! Test directory server for LDAP tests.
//!
//! Serves directory data from a YAML file over the LDAP wire protocol.
//! Uses `serde_norway` for YAML and `tokio` for sockets.
//! Verifies each bind password. Never grants on error.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// One directory entry.
#[derive(Debug, Clone)]
struct Entry {
    dn: String,
    attrs: HashMap<String, Vec<String>>,
}

/// Server config. Holds the directory file and the bind address.
#[derive(Debug, Clone)]
pub struct Config {
    file: PathBuf,
    bind_addr: SocketAddr,
}

impl Config {
    /// Build a config for the directory file.
    pub fn new(path: &Path) -> Self {
        Self {
            file: path.to_path_buf(),
            bind_addr: "127.0.0.1:0".parse().expect("loopback"),
        }
    }

    /// Set the bind address. Returns the updated config.
    pub fn with_bind_address(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = addr;
        self
    }
}

/// Test LDAP server. Loads entries once, then serves them.
pub struct Server {
    config: Config,
    entries: Arc<Vec<Entry>>,
}

/// Handle to a running server. Keeps the task alive.
pub struct ServerHandle {
    local_addr: SocketAddr,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl ServerHandle {
    /// Return the bound address.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Default, serde::Deserialize)]
struct DirFile {
    #[serde(default)]
    entries: Vec<HashMap<String, serde_norway::Value>>,
}

fn value_to_strings(value: &serde_norway::Value) -> Vec<String> {
    if let Some(text) = value.as_str() {
        return vec![text.to_string()];
    }
    if let Some(items) = value.as_sequence() {
        let mut out = Vec::new();
        for item in items {
            if let Some(text) = item.as_str() {
                out.push(text.to_string());
            }
        }
        return out;
    }
    Vec::new()
}

fn load_entries(path: &Path) -> Result<Vec<Entry>, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("cannot read directory file: {e}"))?;
    let file: DirFile =
        serde_norway::from_str(&text).map_err(|e| format!("cannot parse directory file: {e}"))?;
    let mut entries = Vec::new();
    for mut map in file.entries {
        let Some(dn_value) = map.remove("dn") else {
            continue;
        };
        let Some(dn) = dn_value.as_str() else {
            continue;
        };
        let mut attrs: HashMap<String, Vec<String>> = HashMap::new();
        for (key, value) in map {
            let values = value_to_strings(&value);
            if values.is_empty() {
                continue;
            }
            attrs.insert(key.to_ascii_lowercase(), values);
        }
        entries.push(Entry {
            dn: dn.to_string(),
            attrs,
        });
    }
    Ok(entries)
}

impl Server {
    /// Load the directory file. Returns the ready server.
    pub async fn new(config: Config) -> Result<Self, String> {
        let entries = load_entries(&config.file)?;
        Ok(Self {
            config,
            entries: Arc::new(entries),
        })
    }

    /// Bind loopback and serve. Returns the handle.
    pub async fn start(self) -> Result<ServerHandle, String> {
        let listener = tokio::net::TcpListener::bind(self.config.bind_addr)
            .await
            .map_err(|e| format!("directory bind failed: {e}"))?;
        let local_addr = listener
            .local_addr()
            .map_err(|e| format!("directory addr failed: {e}"))?;
        let entries = self.entries;
        let task = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_) => continue,
                };
                let owned = entries.clone();
                tokio::spawn(async move {
                    handle_conn(stream, owned).await;
                });
            }
        });
        Ok(ServerHandle {
            local_addr,
            task: Some(task),
        })
    }
}

fn encode_length(len: usize, out: &mut Vec<u8>) {
    if len < 128 {
        out.push(len as u8);
    } else {
        let mut parts: Vec<u8> = Vec::new();
        let mut rest = len;
        while rest > 0 {
            parts.push((rest & 0xFF) as u8);
            rest >>= 8;
        }
        parts.reverse();
        out.push(0x80 | (parts.len() as u8));
        out.extend_from_slice(&parts);
    }
}

fn encode_tlv(tag: u8, value: &[u8], out: &mut Vec<u8>) {
    out.push(tag);
    encode_length(value.len(), out);
    out.extend_from_slice(value);
}

fn encode_octets(tag: u8, data: &[u8], out: &mut Vec<u8>) {
    encode_tlv(tag, data, out);
}

fn encode_uint(value: u64, tag: u8, out: &mut Vec<u8>) {
    let mut raw: Vec<u8> = Vec::new();
    if value == 0 {
        raw.push(0);
    } else {
        let mut rest = value;
        let mut rev: Vec<u8> = Vec::new();
        while rest > 0 {
            rev.push((rest & 0xFF) as u8);
            rest >>= 8;
        }
        rev.reverse();
        if rev[0] & 0x80 != 0 {
            raw.push(0);
        }
        raw.extend_from_slice(&rev);
    }
    encode_tlv(tag, &raw, out);
}

fn encode_message(id: u64, op_tag: u8, op_body: &[u8], out: &mut Vec<u8>) {
    let mut inner: Vec<u8> = Vec::new();
    encode_uint(id, 0x02, &mut inner);
    encode_tlv(op_tag, op_body, &mut inner);
    encode_tlv(0x30, &inner, out);
}

fn encode_result(id: u64, op_tag: u8, code: u64, out: &mut Vec<u8>) {
    let mut body: Vec<u8> = Vec::new();
    encode_uint(code, 0x0A, &mut body);
    encode_octets(0x04, &[], &mut body);
    encode_octets(0x04, &[], &mut body);
    encode_message(id, op_tag, &body, out);
}

fn encode_entry(id: u64, dn: &str, attrs: &[(String, Vec<String>)], out: &mut Vec<u8>) {
    let mut body: Vec<u8> = Vec::new();
    encode_octets(0x04, dn.as_bytes(), &mut body);
    let mut list: Vec<u8> = Vec::new();
    for (name, values) in attrs {
        let mut attr: Vec<u8> = Vec::new();
        encode_octets(0x04, name.as_bytes(), &mut attr);
        let mut set_body: Vec<u8> = Vec::new();
        for value in values {
            encode_octets(0x04, value.as_bytes(), &mut set_body);
        }
        encode_tlv(0x31, &set_body, &mut attr);
        encode_tlv(0x30, &attr, &mut list);
    }
    encode_tlv(0x30, &list, &mut body);
    encode_message(id, 0x64, &body, out);
}

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn left(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    fn read_byte(&mut self) -> Result<u8, String> {
        if self.pos >= self.buf.len() {
            return Err("short tag".to_string());
        }
        let byte = self.buf[self.pos];
        self.pos += 1;
        Ok(byte)
    }

    fn read_len(&mut self) -> Result<usize, String> {
        let first = self.read_byte()?;
        if first & 0x80 == 0 {
            return Ok((first & 0x7F) as usize);
        }
        let count = (first & 0x7F) as usize;
        if count == 0 || count > 4 {
            return Err("bad length".to_string());
        }
        if self.left() < count {
            return Err("short length".to_string());
        }
        let mut len: usize = 0;
        for _ in 0..count {
            len = (len << 8) | (self.buf[self.pos] as usize);
            self.pos += 1;
        }
        if len > 16_777_216 {
            return Err("too large".to_string());
        }
        Ok(len)
    }

    fn read_tlv(&mut self) -> Result<(u8, Vec<u8>), String> {
        let tag = self.read_byte()?;
        let len = self.read_len()?;
        if self.left() < len {
            return Err("short value".to_string());
        }
        let value = self.buf[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok((tag, value))
    }
}

fn decode_uint(raw: &[u8]) -> u64 {
    let mut out: u64 = 0;
    for byte in raw {
        out = (out << 8) | (*byte as u64);
    }
    out
}

#[derive(Debug, Clone)]
enum Filter {
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
    Equal(String, String),
    Present(String),
    None,
}

fn parse_filter(tag: u8, value: &[u8]) -> Filter {
    match tag {
        0xA0 | 0xA1 => {
            let mut inner = Cursor::new(value);
            let mut parts = Vec::new();
            while inner.left() > 0 {
                let (sub_tag, sub_value) = match inner.read_tlv() {
                    Ok(pair) => pair,
                    Err(_) => return Filter::None,
                };
                parts.push(parse_filter(sub_tag, &sub_value));
            }
            if tag == 0xA0 {
                Filter::And(parts)
            } else {
                Filter::Or(parts)
            }
        }
        0xA2 => {
            let mut inner = Cursor::new(value);
            let (sub_tag, sub_value) = match inner.read_tlv() {
                Ok(pair) => pair,
                Err(_) => return Filter::None,
            };
            Filter::Not(Box::new(parse_filter(sub_tag, &sub_value)))
        }
        0xA3 => {
            let mut inner = Cursor::new(value);
            let (_, attr_raw) = match inner.read_tlv() {
                Ok(pair) => pair,
                Err(_) => return Filter::None,
            };
            let (_, value_raw) = match inner.read_tlv() {
                Ok(pair) => pair,
                Err(_) => return Filter::None,
            };
            let attr = String::from_utf8_lossy(&attr_raw).to_string();
            let val = String::from_utf8_lossy(&value_raw).to_string();
            Filter::Equal(attr, unescape_filter(&val))
        }
        0x87 => {
            let attr = String::from_utf8_lossy(value).to_string();
            Filter::Present(attr)
        }
        _ => Filter::None,
    }
}

fn unescape_filter(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let bytes = value.as_bytes();
    let mut pos = 0;
    while pos < bytes.len() {
        if bytes[pos] == b'\\' && pos + 2 < bytes.len() {
            let hex = &value[pos + 1..pos + 3];
            if let Ok(byte) = u8::from_str_radix(hex, 16) {
                out.push(byte as char);
                pos += 3;
                continue;
            }
        }
        out.push(bytes[pos] as char);
        pos += 1;
    }
    out
}

fn dn_under(dn: &str, base: &str) -> bool {
    let dn = dn.trim().to_ascii_lowercase();
    let base = base.trim().to_ascii_lowercase();
    if base.is_empty() {
        return true;
    }
    if dn == base {
        return true;
    }
    dn.ends_with(&format!(",{base}"))
}

fn filter_hit(filter: &Filter, entry: &Entry) -> bool {
    match filter {
        Filter::And(parts) => parts.iter().all(|item| filter_hit(item, entry)),
        Filter::Or(parts) => parts.iter().any(|item| filter_hit(item, entry)),
        Filter::Not(inner) => !filter_hit(inner, entry),
        Filter::Equal(attr, want) => {
            if attr.eq_ignore_ascii_case("dn") {
                return entry.dn.eq_ignore_ascii_case(want);
            }
            if let Some(values) = entry.attrs.get(&attr.to_ascii_lowercase()) {
                return values.iter().any(|item| item.eq_ignore_ascii_case(want));
            }
            false
        }
        Filter::Present(attr) => {
            if attr.eq_ignore_ascii_case("dn") {
                return true;
            }
            entry.attrs.contains_key(&attr.to_ascii_lowercase())
        }
        Filter::None => false,
    }
}

fn find_entry(entries: &[Entry], dn: &str) -> Option<Entry> {
    entries
        .iter()
        .find(|item| item.dn.eq_ignore_ascii_case(dn))
        .cloned()
}

fn check_password(entry: &Entry, password: &[u8]) -> bool {
    let text = match std::str::from_utf8(password) {
        Ok(text) => text,
        Err(_) => return false,
    };
    if text.is_empty() {
        return false;
    }
    if let Some(values) = entry.attrs.get("userpassword") {
        if let Some(first) = values.first() {
            return first == text;
        }
    }
    false
}

async fn read_packet(stream: &mut tokio::net::TcpStream) -> Result<Option<Vec<u8>>, String> {
    use tokio::io::AsyncReadExt as _;
    let mut head = [0u8; 1];
    match stream.read_exact(&mut head).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(_) => return Ok(None),
    }
    if head[0] != 0x30 {
        return Err("bad packet".to_string());
    }
    let mut first = [0u8; 1];
    if stream.read_exact(&mut first).await.is_err() {
        return Ok(None);
    }
    let len: usize = if first[0] & 0x80 == 0 {
        (first[0] & 0x7F) as usize
    } else {
        let count = (first[0] & 0x7F) as usize;
        if count == 0 || count > 4 {
            return Err("bad length".to_string());
        }
        let mut raw = vec![0u8; count];
        if stream.read_exact(&mut raw).await.is_err() {
            return Ok(None);
        }
        let mut out: usize = 0;
        for byte in raw {
            out = (out << 8) | (byte as usize);
        }
        out
    };
    if len > 16_777_216 {
        return Err("too large".to_string());
    }
    let mut body = vec![0u8; len];
    if stream.read_exact(&mut body).await.is_err() {
        return Ok(None);
    }
    let mut full: Vec<u8> = Vec::with_capacity(2 + len);
    full.push(head[0]);
    if len < 128 {
        full.push(len as u8);
    } else {
        let mut parts: Vec<u8> = Vec::new();
        let mut rest = len;
        while rest > 0 {
            parts.push((rest & 0xFF) as u8);
            rest >>= 8;
        }
        parts.reverse();
        full.push(0x80 | (parts.len() as u8));
        full.extend_from_slice(&parts);
    }
    full.extend_from_slice(&body);
    Ok(Some(full))
}

async fn handle_conn(mut stream: tokio::net::TcpStream, entries: Arc<Vec<Entry>>) {
    use tokio::io::AsyncWriteExt as _;
    loop {
        let packet = match read_packet(&mut stream).await {
            Ok(Some(packet)) => packet,
            Ok(None) => break,
            Err(_) => break,
        };
        let mut outer = Cursor::new(&packet);
        let (outer_tag, outer_value) = match outer.read_tlv() {
            Ok(pair) => pair,
            Err(_) => break,
        };
        if outer_tag != 0x30 {
            break;
        }
        let mut msg = Cursor::new(&outer_value);
        let (_, id_raw) = match msg.read_tlv() {
            Ok(pair) => pair,
            Err(_) => break,
        };
        let msg_id = decode_uint(&id_raw);
        let (op_tag, op_value) = match msg.read_tlv() {
            Ok(pair) => pair,
            Err(_) => break,
        };
        match op_tag {
            0x60 => {
                let mut body = Cursor::new(&op_value);
                let (_, _) = match body.read_tlv() {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let (_, dn_raw) = match body.read_tlv() {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let (auth_tag, auth_raw) = match body.read_tlv() {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                // Simple bind uses tag 0x80. Other choices fail closed.
                let password: Vec<u8> = if auth_tag == 0x80 {
                    auth_raw
                } else {
                    Vec::new()
                };
                let simple = auth_tag == 0x80;
                let dn = String::from_utf8_lossy(&dn_raw).to_string();
                let code = if dn.trim().is_empty() {
                    0
                } else if !simple {
                    49
                } else if let Some(entry) = find_entry(&entries, &dn) {
                    if check_password(&entry, &password) {
                        0
                    } else {
                        49
                    }
                } else {
                    49
                };
                let mut out: Vec<u8> = Vec::new();
                encode_result(msg_id, 0x61, code, &mut out);
                if stream.write_all(&out).await.is_err() {
                    break;
                }
            }
            0x63 => {
                let mut body = Cursor::new(&op_value);
                let (_, base_raw) = match body.read_tlv() {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let (_, scope_raw) = match body.read_tlv() {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let (_, _) = match body.read_tlv() {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let (_, _) = match body.read_tlv() {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let (_, _) = match body.read_tlv() {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let (_, _) = match body.read_tlv() {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let (filter_tag, filter_value) = match body.read_tlv() {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let (_, attrs_raw) = match body.read_tlv() {
                    Ok(pair) => pair,
                    Err(_) => break,
                };
                let base = String::from_utf8_lossy(&base_raw).to_string();
                let scope = decode_uint(&scope_raw);
                let filter = parse_filter(filter_tag, &filter_value);
                let mut wanted: Vec<String> = Vec::new();
                let mut attr_cursor = Cursor::new(&attrs_raw);
                while attr_cursor.left() > 0 {
                    let (_, name_raw) = match attr_cursor.read_tlv() {
                        Ok(pair) => pair,
                        Err(_) => break,
                    };
                    wanted.push(String::from_utf8_lossy(&name_raw).to_string());
                }
                let mut out: Vec<u8> = Vec::new();
                for entry in entries.iter() {
                    let in_scope = if scope == 0 {
                        entry.dn.eq_ignore_ascii_case(&base)
                    } else if scope == 1 {
                        is_direct_child(&entry.dn, &base)
                    } else {
                        dn_under(&entry.dn, &base)
                    };
                    if !in_scope {
                        continue;
                    }
                    if !filter_hit(&filter, entry) {
                        continue;
                    }
                    let mut picked: Vec<(String, Vec<String>)> = Vec::new();
                    if wanted.is_empty() {
                        // No attribute names: send no values.
                    } else {
                        for name in &wanted {
                            if name == "*" {
                                for (key, values) in &entry.attrs {
                                    if key == "userpassword" {
                                        continue;
                                    }
                                    picked.push((key.clone(), values.clone()));
                                }
                                continue;
                            }
                            if let Some(values) = entry.attrs.get(&name.to_ascii_lowercase()) {
                                picked.push((name.clone(), values.clone()));
                            }
                        }
                    }
                    encode_entry(msg_id, &entry.dn, &picked, &mut out);
                }
                encode_result(msg_id, 0x65, 0, &mut out);
                if stream.write_all(&out).await.is_err() {
                    break;
                }
            }
            0x42 => break,
            0x50 => continue,
            _ => break,
        }
    }
}

fn is_direct_child(dn: &str, base: &str) -> bool {
    let dn = dn.trim().to_ascii_lowercase();
    let base = base.trim().to_ascii_lowercase();
    if base.is_empty() {
        return !dn.contains(',');
    }
    let suffix = format!(",{base}");
    if !dn.ends_with(&suffix) {
        return false;
    }
    let head = &dn[..dn.len() - suffix.len()];
    !head.contains(',')
}
