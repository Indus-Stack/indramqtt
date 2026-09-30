//! Time-based one-time password helpers for dashboard MFA enrolment.
//!
//! Pure functions only: no locks, no I/O, no allocation beyond the small
//! bounded buffers below. The management handlers in `v5/mfa.rs` and the
//! admin-user store in `admin_users.rs` share these so enrolment, setup
//! rendering and verification agree on one implementation. Publish and
//! deliver never call this module.
//!
//! Algorithm (RFC 4226 / RFC 6238, SHA-1, 30 s steps, 6 digits, window
//! of one step either side): HMAC-SHA-1 over the big-endian 8-byte
//! counter, dynamic truncation, `mod 1_000_000`, zero-padded to six
//! ASCII digits. HMAC-SHA-1 comes from the maintained `hmac` and `sha1`
//! crates; nothing here is hand-rolled cryptography.
// TODO(parity): does the documented code factor use these exact TOTP
// parameters (hash, step, digits, window)? The rulebook does not decide
// the factor parameters; the current choice is the authenticator-app
// default (SHA-1, 30 s, 6 digits, +-1 step) until the checker pins it.

use hmac::{Hmac, Mac};
use sha1::Sha1;

/// Seconds per TOTP counter step: 30 s. Reason: the authenticator-app
/// default; short enough that a stolen code expires quickly, long enough
/// that clock skew of a few seconds does not fail closed on every login.
pub const TOTP_STEP_SECS: u64 = 30;
/// Digits per one-time code: 6. Reason: the authenticator-app default;
/// six digits fit the documented code factor's text field.
pub const TOTP_DIGITS: u32 = 6;
/// Verification window: the current step plus/minus one step. Reason: a
/// code generated at the edge of a step must still verify after the
/// management round-trip; wider windows accept stale codes for longer.
pub const TOTP_WINDOW_STEPS: u64 = 1;
/// Raw enrolment secret length: 20 bytes (160 bits). Reason: the RFC 4226
/// recommendation for SHA-1 TOTP; 20 random bytes carry 160 bits of
/// entropy while staying short in base32 provisioning fields.
pub const TOTP_SECRET_LEN: usize = 20;

/// Modulus for six-digit codes (`10^6`).
const TOTP_MODULUS: u32 = 1_000_000;

/// Counter for `unix_secs`: seconds since the epoch divided into steps.
pub fn totp_counter(unix_secs: u64) -> u64 {
    unix_secs / TOTP_STEP_SECS
}

/// Current Unix time in whole seconds (management-plane only).
pub fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

/// One six-digit code for `secret` at `counter`, zero-padded.
pub fn totp_code(secret: &[u8], counter: u64) -> String {
    let value = totp_value(secret, counter);
    format!("{value:06}")
}

/// Raw numeric code for `secret` at `counter`.
fn totp_value(secret: &[u8], counter: u64) -> u32 {
    let mut mac = Hmac::<Sha1>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(&counter.to_be_bytes());
    let digest = mac.finalize().into_bytes();
    let offset = (digest[digest.len() - 1] & 0x0f) as usize;
    let code = ((digest[offset] as u32 & 0x7f) << 24)
        | ((digest[offset + 1] as u32) << 16)
        | ((digest[offset + 2] as u32) << 8)
        | (digest[offset + 3] as u32);
    code % TOTP_MODULUS
}

/// Verify `code` (six ASCII digits) against `secret` at `unix_secs`
/// within the +-1-step window. Returns the matching counter on success,
/// so callers can reject a counter at or below the last verified one
/// (single-use / replay rejection).
pub fn totp_verify(secret: &[u8], code: &str, unix_secs: u64) -> Option<u64> {
    if code.len() != TOTP_DIGITS as usize || !code.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let current = totp_counter(unix_secs);
    let start = current.saturating_sub(TOTP_WINDOW_STEPS);
    (start..=current.saturating_add(TOTP_WINDOW_STEPS))
        .find(|counter| totp_code(secret, *counter) == code)
}

/// Random enrolment secret: [`TOTP_SECRET_LEN`] bytes from the process RNG.
pub fn generate_secret() -> Vec<u8> {
    use rand::Rng;
    let mut secret = vec![0u8; TOTP_SECRET_LEN];
    rand::rng().fill_bytes(&mut secret[..]);
    secret
}

/// RFC 4648 base32 encode without padding (authenticator-app alphabet).
pub fn base32_encode(raw: &[u8]) -> String {
    const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut out = String::with_capacity((raw.len() * 8).div_ceil(5));
    let mut buffer: u32 = 0;
    let mut bits: u32 = 0;
    for byte in raw {
        buffer = (buffer << 8) | u32::from(*byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(ALPHABET[((buffer >> bits) & 0x1f) as usize] as char);
        }
    }
    if bits > 0 {
        out.push(ALPHABET[((buffer << (5 - bits)) & 0x1f) as usize] as char);
    }
    out
}

/// RFC 4648 base32 decode (upper or lower case, padding optional).
/// Returns `None` on any invalid character or bad length.
pub fn base32_decode(text: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(text.len() * 5 / 8);
    let mut buffer: u32 = 0;
    let mut bits: u32 = 0;
    for ch in text.chars() {
        if ch == '=' {
            break;
        }
        let value = match ch {
            'A'..='Z' => u32::from(ch as u8 - b'A'),
            'a'..='z' => u32::from(ch as u8 - b'a'),
            '2'..='7' => u32::from(ch as u8 - b'2' + 26),
            _ => return None,
        };
        buffer = (buffer << 5) | value;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits) as u8);
            buffer &= (1u32 << bits).saturating_sub(1);
        }
    }
    // A valid base32 length leaves 0, 1, 2, 3 or 4 leftover bits (the
    // 2/4/5/7-char trailing quanta); anything else is a bad length.
    if bits >= 5 {
        return None;
    }
    Some(out)
}

/// Provisioning URI for authenticator apps (`otpauth://totp/...`).
/// `issuer` names this broker in the app; the label carries the username.
// TODO(parity): what is the documented provisioning shape (URI scheme,
// issuer string, query fields)? The rulebook does not decide the exact
// fields; the current choice is the widely supported `otpauth://totp`
// form with issuer, SHA-1, 6 digits and a 30 s period until the checker
// pins it.
pub fn provisioning_uri(issuer: &str, username: &str, secret_b32: &str) -> String {
    format!(
        "otpauth://totp/{issuer}:{username}?secret={secret_b32}&issuer={issuer}&algorithm=SHA1&digits=6&period={}",
        TOTP_STEP_SECS
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc6238_sha1_vector_at_59_seconds() {
        // RFC 6238 test vector: secret "12345678901234567890" (ASCII),
        // time 59 s, SHA-1, 8-digit truncation 94287082, hence the
        // six-digit suffix 287082.
        let secret = b"12345678901234567890";
        assert_eq!(totp_code(secret, totp_counter(59)), "287082");
    }

    #[test]
    fn verify_accepts_window_and_rejects_bad_shapes() {
        let secret = b"12345678901234567890";
        let now = 59;
        let code = totp_code(secret, totp_counter(now));
        assert_eq!(totp_verify(secret, &code, now), Some(totp_counter(now)));
        // Adjacent steps verify inside the window.
        let prev = totp_code(secret, totp_counter(now).saturating_sub(1));
        assert!(totp_verify(secret, &prev, now).is_some());
        // Malformed codes fail closed without verification.
        for bad in ["", "12345", "1234567", "abcdef", "12 456", "99999999"] {
            assert_eq!(totp_verify(secret, bad, now), None, "bad code {bad:?}");
        }
        assert_eq!(
            totp_verify(secret, "000000", now + 10 * TOTP_STEP_SECS),
            None
        );
    }

    #[test]
    fn base32_round_trip() {
        let raw: Vec<u8> = (0u8..20).collect();
        let encoded = base32_encode(&raw);
        assert_eq!(base32_decode(&encoded), Some(raw.clone()));
        assert_eq!(base32_decode(&encoded.to_lowercase()), Some(raw));
        assert!(base32_decode("!!!").is_none());
        assert!(base32_decode("A").is_none());
    }

    #[test]
    fn provisioning_uri_carries_secret_and_issuer() {
        let uri = provisioning_uri("IndraMQTT", "alice", "JBSWY3DP");
        assert!(uri.starts_with("otpauth://totp/"));
        assert!(uri.contains("JBSWY3DP"));
        assert!(uri.contains("alice"));
    }
}
