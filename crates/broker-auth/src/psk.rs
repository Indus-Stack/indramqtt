//! Pre-shared key store for TLS PSK handshakes.
//!
//! The store maps one identity string to its key bytes. The TLS
//! listener consults it during the handshake. It holds no other state.

use parking_lot::RwLock;
use std::collections::HashMap;

/// Most identities in the store.
/// Reason: one entry is small, but an unbounded map lets one
/// management batch exhaust the kernel. 1024 covers constrained
/// fleets. Larger fleets belong in a directory.
pub const MAX_PSK_ENTRIES: usize = 1024;
/// Shortest accepted key in bytes.
/// Reason: 128 bits of entropy is the lowest safe key size.
pub const MIN_PSK_KEY_BYTES: usize = 16;
/// Longest accepted key in bytes.
/// Reason: 512 bits bound memory while covering safe key sizes.
pub const MAX_PSK_KEY_BYTES: usize = 64;
/// Longest accepted identity in bytes.
/// Reason: identities ride the handshake lookup, so one entry
/// cannot balloon the table. 128 bytes cover device names.
pub const MAX_PSK_IDENTITY_BYTES: usize = 128;

/// Why [`PskStore::insert`] refused an entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PskInsertError {
    /// The identity is empty or too long.
    BadIdentity,
    /// The key is too short or too long.
    BadKey,
    /// The table holds [`MAX_PSK_ENTRIES`] entries.
    Full,
}

/// In-memory identity to key table.
/// Cloned keys never leave the handshake path except as copies.
#[derive(Debug, Default)]
pub struct PskStore {
    /// Identity string to key bytes.
    inner: RwLock<HashMap<String, Vec<u8>>>,
}

impl PskStore {
    /// Makes an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Stores one identity with its key. An existing identity
    /// gets the new key. Fails closed on bad input.
    pub fn insert(&self, identity: &str, key: &[u8]) -> Result<(), PskInsertError> {
        if identity.is_empty() || identity.len() > MAX_PSK_IDENTITY_BYTES {
            return Err(PskInsertError::BadIdentity);
        }
        if key.len() < MIN_PSK_KEY_BYTES || key.len() > MAX_PSK_KEY_BYTES {
            return Err(PskInsertError::BadKey);
        }
        // Control characters in the identity would confuse logs.
        if identity.chars().any(|c| c.is_control()) {
            return Err(PskInsertError::BadIdentity);
        }
        let mut table = self.inner.write();
        if !table.contains_key(identity) && table.len() >= MAX_PSK_ENTRIES {
            return Err(PskInsertError::Full);
        }
        table.insert(identity.to_string(), key.to_vec());
        Ok(())
    }

    /// Returns a copy of the key for `identity`.
    /// Returns `None` for an unknown identity.
    /// The caller must not log whether the identity exists.
    pub fn lookup(&self, identity: &[u8]) -> Option<Vec<u8>> {
        let text = std::str::from_utf8(identity).ok()?;
        self.inner.read().get(text).cloned()
    }

    /// Loads identities from a file into the store.
    /// The file holds one `identity:hexkey` entry per line.
    /// Blank lines and `#` comments are ignored. Hex decodes
    /// to the key bytes. Entries load in file order. A bad
    /// line stops the load with the file and the line number.
    /// A duplicate identity stops the load. An empty file
    /// stops the load. Returns the loaded entry count.
    pub fn load_file(&self, path: &str) -> Result<usize, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read PSK file {path}: {e}"))?;
        let mut count = 0;
        for (index, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (id, hex) = line
                .split_once(':')
                .ok_or_else(|| format!("bad PSK entry at {}:{}: missing ':'", path, index + 1))?;
            let identity = id.trim();
            let key = decode_hex(hex.trim())
                .ok_or_else(|| format!("bad PSK key at {}:{}: not hex", path, index + 1))?;
            if self.lookup(identity.as_bytes()).is_some() {
                return Err(format!("duplicate PSK identity at {}:{}", path, index + 1));
            }
            self.insert(identity, &key)
                .map_err(|e| format!("bad PSK entry at {}:{}: {e:?}", path, index + 1))?;
            count += 1;
        }
        if count == 0 {
            return Err(format!("no PSK entry in {path}"));
        }
        Ok(count)
    }

    /// Counts stored identities. Tests only.
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// True when the store holds no identity. Tests only.
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }
}

/// Decodes hex into bytes. Accepts upper and lower case.
/// Returns `None` for odd length or other characters.
fn decode_hex(raw: &str) -> Option<Vec<u8>> {
    if !raw.len().is_multiple_of(2) {
        return None;
    }
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_val(bytes[i])?;
        let lo = hex_val(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Some(out)
}

/// Value of one hex digit. Returns `None` for other bytes.
fn hex_val(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_lookup_round_trip() {
        let store = PskStore::new();
        let key = vec![0xA5u8; 16];
        store.insert("device-1", &key).expect("insert holds");
        assert_eq!(store.lookup(b"device-1"), Some(key));
        assert_eq!(store.lookup(b"unknown"), None);
    }

    #[test]
    fn bad_input_fails_closed() {
        let store = PskStore::new();
        assert_eq!(
            store.insert("", &[0xA5u8; 16]),
            Err(PskInsertError::BadIdentity)
        );
        assert_eq!(
            store.insert("id", &[0x01u8; 8]),
            Err(PskInsertError::BadKey)
        );
        assert_eq!(
            store.insert("id", &[0x01u8; 65]),
            Err(PskInsertError::BadKey)
        );
        assert_eq!(store.lookup(b"no-such-id"), None);
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
    }

    /// Test file path. Unique per test name.
    fn temp_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("indra-psk-{}-{name}.txt", std::process::id()))
    }

    #[test]
    fn load_file_round_trip() {
        let path = temp_path("round-trip");
        std::fs::write(&path, "# comment\n\ndevice-a:2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a\ndevice-b:33333333333333333333333333333333\n")
            .expect("file writes");
        let store = PskStore::new();
        let count = store
            .load_file(path.to_str().expect("path shows"))
            .expect("file loads");
        assert_eq!(count, 2);
        assert_eq!(store.lookup(b"device-a"), Some(vec![0x2Au8; 16]));
        assert_eq!(store.lookup(b"device-b"), Some(vec![0x33u8; 16]));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn load_file_fails_closed() {
        let store = PskStore::new();
        let missing = temp_path("missing");
        let _ = std::fs::remove_file(&missing);
        assert!(store
            .load_file(missing.to_str().expect("path shows"))
            .is_err());
        let bad = temp_path("bad");
        std::fs::write(&bad, "no-separator-here\n").expect("file writes");
        assert!(store.load_file(bad.to_str().expect("path shows")).is_err());
        let dup = temp_path("dup");
        std::fs::write(
            &dup,
            "id:2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a2a\nid:33333333333333333333333333333333\n",
        )
        .expect("file writes");
        assert!(store.load_file(dup.to_str().expect("path shows")).is_err());
        let empty = temp_path("empty");
        std::fs::write(&empty, "# only a comment\n").expect("file writes");
        assert!(store
            .load_file(empty.to_str().expect("path shows"))
            .is_err());
        for path in [&bad, &dup, &empty] {
            let _ = std::fs::remove_file(path);
        }
    }
}
