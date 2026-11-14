//! Secret references for configuration values (M1-05).
//!
//! A secret is never stored inline: configuration holds a reference of the
//! form `file:/path` or `env:NAME`, resolved to bytes at the point of use
//! (authentication, connector construction). Every observable rendering
//! (logs, exports, `explain` output, API responses) shows a redacted marker
//! naming the reference, never the value. A missing or unreadable secret
//! fails closed with an error naming the reference, never the value.
//!
//! The `file:` form reads the file's bytes, stripping a single trailing
//! CR/LF pair so editor-terminated secret files compare equal without
//! changing interior bytes. The `env:` form reads the process environment.
//! TODO(parity): whether `file:` values should be re-read on every use or
//! cached per connection is undecided; resolving at each use is the
//! conservative choice (rotation takes effect immediately) until decided.

use crate::ConfigError;

/// Prefix for file-backed secret references.
pub const FILE_PREFIX: &str = "file:";
/// Prefix for environment-backed secret references.
pub const ENV_PREFIX: &str = "env:";

/// Returns true when `value` is a secret reference (`file:` or `env:`).
#[must_use]
pub fn is_secret_ref(value: &str) -> bool {
    value.starts_with(FILE_PREFIX) || value.starts_with(ENV_PREFIX)
}

/// Parsed form of a secret reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecretRef {
    /// Read the secret bytes from this filesystem path.
    File(String),
    /// Read the secret bytes from this environment variable.
    Env(String),
}

impl SecretRef {
    /// Parse a reference string. Returns `None` when it is not a reference.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        if let Some(path) = value.strip_prefix(FILE_PREFIX) {
            if path.is_empty() {
                return None;
            }
            Some(Self::File(path.to_string()))
        } else if let Some(name) = value.strip_prefix(ENV_PREFIX) {
            if name.is_empty() {
                return None;
            }
            Some(Self::Env(name.to_string()))
        } else {
            None
        }
    }

    /// The original reference string (`file:...` / `env:...`).
    #[must_use]
    pub fn reference(&self) -> String {
        match self {
            Self::File(path) => format!("{FILE_PREFIX}{path}"),
            Self::Env(name) => format!("{ENV_PREFIX}{name}"),
        }
    }

    /// Observable rendering: a marker naming the reference, never the value.
    #[must_use]
    pub fn redacted(&self) -> String {
        format!("<redacted:{}>", self.reference())
    }
}

/// Redact one configured value for any observable output.
///
/// References render as `<redacted:file:...>` / `<redacted:env:...>`; plain
/// values pass through unchanged (they are not secrets).
#[must_use]
pub fn redact_value(value: &str) -> String {
    match SecretRef::parse(value) {
        Some(reference) => reference.redacted(),
        None => value.to_string(),
    }
}

/// Resolve a reference to its secret bytes, failing closed.
///
/// Errors name the reference, never the value: a missing file, an
/// unreadable file or a missing variable denies the operation that needed
/// the secret. Non-reference input is a caller error naming the problem,
/// never the value.
pub fn resolve_secret(reference: &str) -> Result<Vec<u8>, ConfigError> {
    let parsed = SecretRef::parse(reference).ok_or_else(|| {
        ConfigError::Invalid(format!(
            "secret reference {reference:?} is invalid: must start with `file:` or `env:` (field `secret`)"
        ))
    })?;
    match parsed {
        SecretRef::File(path) => {
            let bytes = std::fs::read(&path).map_err(|err| {
                ConfigError::Invalid(format!(
                    "secret reference {reference:?} cannot be read: {err} (field `secret`)"
                ))
            })?;
            Ok(strip_single_trailing_newline(bytes))
        }
        SecretRef::Env(name) => {
            let value = std::env::var(&name).map_err(|_| {
                ConfigError::Invalid(format!(
                    "secret reference {reference:?} is not set (field `secret`)"
                ))
            })?;
            Ok(value.into_bytes())
        }
    }
}

/// Resolve when the value may be a reference, else return the raw bytes.
///
/// Plain values are returned as-is (backwards compatible with configs that
/// predate references); references go through [`resolve_secret`].
pub fn resolve_maybe_secret(value: &str) -> Result<Vec<u8>, ConfigError> {
    if is_secret_ref(value) {
        resolve_secret(value)
    } else {
        Ok(value.as_bytes().to_vec())
    }
}

/// Strip one trailing `\n` (plus a preceding `\r` when present) so files
/// written with a terminating newline still authenticate.
fn strip_single_trailing_newline(mut bytes: Vec<u8>) -> Vec<u8> {
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    bytes
}

/// Marker shown in place of a stored secret on every read. The console
/// and the reference API use the same six asterisks.
pub const SECRET_MASK: &str = "******";

/// Field names whose value is a secret. This is the one list for every
/// connector, action, authenticator, source and snapshot read.
///
/// Source: every property with `"format": "password"` in the reference
/// OpenAPI (`openapi-6.3.0.json`), except the two ids listed in
/// [`REFERENCE_PASSWORD_IDS`]. It also holds the clear secret names that
/// kinds outside the reference use. A match is exact, so a name such as
/// `shared_access_key_name` or an id such as `api_key` is not a secret.
pub const SECRET_FIELD_NAMES: &[&str] = &[
    // Reference `format: password` properties.
    "access_key_secret",
    "account_key",
    "aws_secret_access_key",
    "client_secret",
    "jwt",
    "password",
    "private_key",
    "private_key_password",
    "secret",
    "secret_access_key",
    "secret_key",
    "security_token",
    "sentinel_password",
    "service_account_json",
    "sp_private_key",
    "token",
    // Secret names of kinds outside the reference.
    "access_token",
    "api_secret",
    "auth_token",
    "bearer_token",
    "credentials_json",
    "mfa_secret",
    "passphrase",
    "passwd",
    "private_key_pem",
    "sas_token",
    "session_token",
    "shared_access_key",
];

/// Reference `format: password` properties that are ids, not secrets:
/// the key id of an access key and the key id of an API key. Their secret
/// sits in a separate field (`secret_access_key`, `api_secret`).
pub const REFERENCE_PASSWORD_IDS: &[&str] = &["access_key_id", "api_key"];

/// True when a config field name carries secret material. The match is
/// exact and ignores case. See [`SECRET_FIELD_NAMES`].
#[must_use]
pub fn is_secret_key(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    SECRET_FIELD_NAMES.contains(&lower.as_str())
}

/// Mask every secret in one document, in place. A field with a secret
/// name shows `******` when its value is a plain string. An object or
/// array under such a name is walked, not replaced. A `file:` or `env:` reference shows its `<redacted:...>`
/// marker. Other values are not secrets and stay. The walk covers
/// objects and arrays at any depth.
pub fn mask_secrets(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, member) in map.iter_mut() {
                if is_secret_key(key) {
                    match member {
                        serde_json::Value::String(text) if text.is_empty() => {}
                        serde_json::Value::String(text) if is_secret_ref(text.trim()) => {
                            *text = redact_value(text.trim());
                        }
                        serde_json::Value::String(_) => {
                            *member = serde_json::Value::String(SECRET_MASK.to_string());
                        }
                        // A container under a secret name is not a secret
                        // value. Walk into it and mask its own secrets.
                        serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
                            mask_secrets(member);
                        }
                        _ => {}
                    }
                } else {
                    mask_secrets(member);
                }
            }
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(mask_secrets),
        serde_json::Value::String(text) if is_secret_ref(text.trim()) => {
            *text = redact_value(text.trim());
        }
        // The registry keeps some documents as JSON text. Mask inside it.
        serde_json::Value::String(text)
            if matches!(text.trim_start().as_bytes().first(), Some(b'{' | b'[')) =>
        {
            if let Ok(mut inner) = serde_json::from_str::<serde_json::Value>(text) {
                mask_secrets(&mut inner);
                *text = inner.to_string();
            }
        }
        _ => {}
    }
}

/// Copy of `value` with every secret masked.
#[must_use]
pub fn masked_secrets(value: &serde_json::Value) -> serde_json::Value {
    let mut out = value.clone();
    mask_secrets(&mut out);
    out
}

/// An update that sends the mask back keeps the stored secret. Every
/// field of `body` that holds exactly `******` takes the value stored at
/// the same path. A path with no stored value keeps the mask text.
pub fn restore_masked_secrets(body: &mut serde_json::Value, stored: &serde_json::Value) {
    match (body, stored) {
        (serde_json::Value::Object(new), serde_json::Value::Object(old)) => {
            for (key, member) in new.iter_mut() {
                let Some(previous) = old.get(key) else {
                    continue;
                };
                let echoed_ref = matches!(
                    (member.as_str(), previous.as_str()),
                    (Some(sent), Some(old)) if is_secret_ref(old.trim()) && sent == redact_value(old.trim())
                );
                if member.as_str() == Some(SECRET_MASK) || echoed_ref {
                    *member = previous.clone();
                } else {
                    restore_masked_secrets(member, previous);
                }
            }
        }
        (serde_json::Value::Array(new), serde_json::Value::Array(old)) => {
            for (member, previous) in new.iter_mut().zip(old) {
                restore_masked_secrets(member, previous);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every property with `"format": "password"` in the reference
    /// OpenAPI (`/work/team/plan/reference/openapi-6.3.0.json`, 6.3.0).
    /// The file is outside the repository, so the names are data here.
    const REFERENCE_PASSWORD_PROPERTIES: &[&str] = &[
        "access_key_id",
        "access_key_secret",
        "account_key",
        "api_key",
        "aws_secret_access_key",
        "client_secret",
        "jwt",
        "password",
        "private_key",
        "private_key_password",
        "secret",
        "secret_access_key",
        "secret_key",
        "security_token",
        "sentinel_password",
        "service_account_json",
        "sp_private_key",
        "token",
    ];

    #[test]
    fn every_reference_password_property_is_a_secret_or_a_listed_id() {
        for name in REFERENCE_PASSWORD_PROPERTIES {
            assert!(
                is_secret_key(name) || REFERENCE_PASSWORD_IDS.contains(name),
                "reference password property {name} is not in the list"
            );
        }
        for id in REFERENCE_PASSWORD_IDS {
            assert!(!is_secret_key(id), "{id} is an id, not a secret");
        }
    }

    #[test]
    fn names_and_ids_are_not_secrets() {
        for name in [
            "shared_access_key_name",
            "api_key",
            "access_key_id",
            "username",
            "token_endpoint",
            "password_hash_algorithm",
        ] {
            assert!(!is_secret_key(name), "{name}");
        }
        assert!(is_secret_key("Password"));
        assert!(is_secret_key("api_secret"));
        assert!(is_secret_key("shared_access_key"));
    }

    #[test]
    fn secret_named_container_is_walked_not_replaced() {
        let mut doc = serde_json::json!({
            "api_keys": {"keys": [{"name": "k", "secret": "s3", "enable": true}]},
            "service_account_json": {"client_email": "a@b", "private_key": "pk"},
            "password": "pw",
        });
        mask_secrets(&mut doc);
        assert_eq!(doc["api_keys"]["keys"][0]["name"], "k");
        assert_eq!(doc["api_keys"]["keys"][0]["secret"], SECRET_MASK);
        assert_eq!(doc["api_keys"]["keys"][0]["enable"], true);
        assert_eq!(doc["service_account_json"]["client_email"], "a@b");
        assert_eq!(doc["service_account_json"]["private_key"], SECRET_MASK);
        assert_eq!(doc["password"], SECRET_MASK);
    }

    #[test]
    fn json_text_documents_are_masked_inside() {
        let mut doc = serde_json::json!({
            "config": "{\"name\":\"c\",\"password\":\"pw\"}",
            "note": "{not json",
        });
        mask_secrets(&mut doc);
        let inner: serde_json::Value =
            serde_json::from_str(doc["config"].as_str().unwrap()).unwrap();
        assert_eq!(inner["name"], "c");
        assert_eq!(inner["password"], SECRET_MASK);
        assert_eq!(doc["note"], "{not json");
    }

    #[test]
    fn parse_accepts_both_forms_and_rejects_plain() {
        assert_eq!(
            SecretRef::parse("file:/run/secrets/pw"),
            Some(SecretRef::File("/run/secrets/pw".to_string()))
        );
        assert_eq!(
            SecretRef::parse("env:BROKER_PW"),
            Some(SecretRef::Env("BROKER_PW".to_string()))
        );
        assert_eq!(SecretRef::parse("plaintext"), None);
        assert_eq!(SecretRef::parse("file:"), None);
        assert_eq!(SecretRef::parse("env:"), None);
    }

    #[test]
    fn redacted_names_reference_never_value() {
        let marker = redact_value("env:SOME_VAR");
        assert!(
            marker.contains("env:SOME_VAR"),
            "marker names the reference"
        );
        assert!(!marker.contains("s3cret"), "marker never holds the value");
        assert_eq!(redact_value("plain"), "plain");
    }

    #[test]
    fn missing_secret_fails_closed_naming_reference() {
        let missing = "env:BROKER_CONFIG_TEST_DEFINITELY_UNSET_9F3A";
        let err = resolve_secret(missing).expect_err("unset variable must fail");
        assert!(err.to_string().contains(missing));
        let err = resolve_secret("file:/definitely/not/here-9f3a").expect_err("missing file fails");
        assert!(err.to_string().contains("file:/definitely/not/here-9f3a"));
    }

    #[test]
    fn env_secret_resolves_and_file_strips_newline() {
        std::env::set_var("BROKER_CONFIG_TEST_SECRET_RT", "s3cret-value");
        let bytes = resolve_secret("env:BROKER_CONFIG_TEST_SECRET_RT").expect("env resolves");
        assert_eq!(bytes, b"s3cret-value");
        std::env::remove_var("BROKER_CONFIG_TEST_SECRET_RT");

        let dir = std::env::temp_dir().join(format!("broker-secret-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create dir");
        let path = dir.join("pw");
        std::fs::write(&path, b"file-secret\n").expect("write secret");
        let reference = format!("file:{}", path.display());
        let bytes = resolve_secret(&reference).expect("file resolves");
        assert_eq!(bytes, b"file-secret");
        std::fs::remove_dir_all(&dir).ok();
    }
}
