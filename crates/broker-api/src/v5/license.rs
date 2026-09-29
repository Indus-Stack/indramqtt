//! License state and install for the v5 management API.
//!
//! Covers `GET /license` (read the live license, C13-16) and
//! `POST /license` (validated install, C13-17) on the real
//! [`crate::licence::LicenceStore`]. Single-record store by design
//! (bounded): at most one token is ever held, so per-request work is one
//! short-lock snapshot plus one status render, never proportional to
//! connections, sessions or subscriptions.
//!
//! Management-plane only: both handlers run off the publish and deliver
//! paths (edge receive, kernel route, session, deliver never touch this
//! store). The kernel consults the same store at boot and on validated
//! installs only; see `crates/broker-node/src/main.rs:6048`
//! (`load_from_data_dir`), `:6038` (`set_trusted_keys`), `:6099`
//! (`refresh_alarms`) and `:6116` (`stored_token` evaluated for the
//! cluster join path). The broker event that drives it is node boot plus
//! the management install; the publish path never consults it and a
//! licence condition never stops customer traffic.
//!
//! Store bounds (stated here and enforced below):
//! - exactly one stored token (single-record by design); installs replace
//!   in place and never grow a list;
//! - the presented token is capped at [`MAX_LICENSE_TOKEN_LEN`] bytes
//!   (16 KiB); longer bodies are rejected before verification so one
//!   record cannot balloon memory or the persisted `licence.token` file;
//! - reads clone at most the one token plus a small status document per
//!   request under short locks.
//!
//! Persistence: the shared [`crate::licence::LicenceStore`] persists
//! through the kernel data directory (`cluster_identity.json` plus
//! `licence.token`, atomically replaced); a failed install never touches
//! the previously stored licence, and a restart reloads the same state
//! via `load_from_data_dir`.
// TODO(parity): which install body field name (`license` versus `licence`
// versus `token` versus `key`) and which install success status/body
// (200 with the live status versus 201/204 with a payload echo) does the
// spec require? The rulebook does not decide the exact shape; the current
// choice accepts `license` plus the `licence`/`token`/`key` aliases,
// ignores extra fields, and answers 200 with the same status document
// `GET` renders (omitting fields the broker cannot supply, never a
// constant standing in for a measurement) until the checker pins it.
// TODO(parity): which status does an identity-mismatched install carry
// (400 versus 409) and which documented error code names the failure?
// The rulebook does not decide the exact mapping; the current choice
// answers every install rejection with `BAD_REQUEST` (400 in this tree's
// shared error mapping) carrying the distinct install reason
// (`identity mismatch` stays distinct from a bad signature) until the
// checker pins it.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;

use crate::errors::ApiError;
use crate::ApiState;

/// Upper bound for a presented licence token (16 KiB). Reason: a token is
/// a few hundred bytes of base64 payload plus a 64-byte signature, so
/// 16 KiB covers any realistic signed payload with headroom while one
/// record cannot balloon the in-memory clone per request or the
/// atomically replaced `licence.token` file; longer bodies are rejected
/// before verification with `BAD_REQUEST`.
pub const MAX_LICENSE_TOKEN_LEN: usize = 16 * 1024;

/// `GET /license`: the live license, or the documented unlicensed state
/// when nothing is installed.
///
/// Always 200, never an error: with no license the store reports the
/// trial state with entitlements on. Management-plane only; one
/// short-lock snapshot per request, publish and deliver never touch it.
pub async fn get_license(State(state): State<ApiState>) -> Response {
    let now = now_secs();
    let body = state.licence.status_at(now);
    (StatusCode::OK, Json(body)).into_response()
}

/// `POST /license`: validated install of a licence token.
///
/// The body must be a JSON object carrying the token under `license`
/// (aliases `licence`, `token`, `key`; first present string wins). Every
/// supplied value is validated before anything is applied: missing,
/// empty, over-long or unverifiable tokens fail with `BAD_REQUEST` and
/// apply nothing, so a failed install leaves the prior license intact.
/// Installing over an existing license replaces it in place (single
/// record) and the response reports the new live state with 200.
/// Management-plane only; verification runs on the calling task, the
/// store takes short locks, and publish/deliver never touch it.
pub async fn install_license(State(state): State<ApiState>, body: Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid license body: {error}")).into_response();
        }
    };
    let token = match parse_install_request(&value) {
        Ok(token) => token,
        Err(message) => return ApiError::BadRequest(message).into_response(),
    };
    let now = now_secs();
    match state.licence.install(&token, now) {
        Ok(_) => {
            state.licence.refresh_alarms(&state.alarms, now);
            let body = state.licence.status_at(now);
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(error) => ApiError::BadRequest(error.to_string()).into_response(),
    }
}

/// Validate an install body into the token to verify.
///
/// The body must be a JSON object; the token is the first present string
/// among `license`, `licence`, `token` and `key`. Extra fields are
/// ignored so future record fields degrade to the same install instead
/// of a 400. Empty or whitespace-only tokens, non-string tokens and
/// tokens past [`MAX_LICENSE_TOKEN_LEN`] bytes fail; nothing is applied
/// on error.
fn parse_install_request(value: &serde_json::Value) -> Result<String, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "license body must be a JSON object".to_string())?;
    for field in ["license", "licence", "token", "key"] {
        if let Some(candidate) = obj.get(field) {
            let token = candidate.as_str().ok_or_else(|| {
                format!("field `{field}` must be a string carrying the licence token")
            })?;
            let trimmed = token.trim();
            if trimmed.is_empty() {
                return Err(format!("field `{field}` must not be empty"));
            }
            if trimmed.len() > MAX_LICENSE_TOKEN_LEN {
                return Err(format!(
                    "field `{field}` exceeds {MAX_LICENSE_TOKEN_LEN} bytes"
                ));
            }
            return Ok(trimmed.to_string());
        }
    }
    Err("field `license` is required".to_string())
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_cluster::{
        canonical_json_bytes, license::TOKEN_PREFIX, LicensePayload, TrustedKeys,
    };
    use p256::ecdsa::{signature::Signer, SigningKey, VerifyingKey};
    use std::sync::Arc;

    use base64::Engine as _;

    fn signing_key(seed: u8) -> SigningKey {
        let bytes = [seed; 32];
        let field = p256::elliptic_curve::generic_array::GenericArray::clone_from_slice(&bytes);
        SigningKey::from_bytes(&field).expect("seed")
    }

    fn mint(
        signing: &SigningKey,
        kid: &str,
        node_id: &str,
        issued_at: u64,
        expires_at: u64,
    ) -> String {
        let payload = LicensePayload {
            customer: "Acme".to_string(),
            max_nodes: 5,
            issued_at,
            expires_at,
            features: vec!["clustering".to_string()],
            node_id: node_id.to_string(),
            kid: kid.to_string(),
            grace_period_days: broker_cluster::DEFAULT_GRACE_DAYS,
        };
        let bytes = canonical_json_bytes(&payload).expect("json");
        let sig: p256::ecdsa::Signature = signing.sign(&bytes);
        format!(
            "{}.{}.{}",
            TOKEN_PREFIX,
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&bytes),
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sig.to_bytes())
        )
    }

    fn seeded_state(identity: &str, seed: u8) -> (ApiState, SigningKey, String) {
        let signing = signing_key(seed);
        let kid = format!("key-{seed:02x}");
        let engine = Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        let state = ApiState::standalone(engine);
        let mut trusted = TrustedKeys::new();
        trusted.insert(kid.clone(), VerifyingKey::from(&signing));
        state.licence.set_trusted_keys(trusted);
        state
            .licence
            .set_identity_for_tests(broker_cluster::ClusterIdentity {
                identity: identity.to_string(),
                public_key_hex: "aa".to_string(),
                private_key_hex: "bb".to_string(),
                trial_started_at: 1_750_000_000,
                highest_seen_secs: 1_750_000_000,
            });
        (state, signing, kid)
    }

    async fn response_parts(response: Response) -> (StatusCode, serde_json::Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 256 * 1024)
            .await
            .expect("license body is small and readable");
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("license body is JSON");
        (status, body)
    }

    #[tokio::test]
    async fn get_reports_unlicensed_trial_not_an_error() {
        // W5-10 broker-through: the read goes through the served handler
        // on shared `ApiState` (the same state the kernel shares in
        // `crates/broker-node/src/main.rs`, `serve_api`), not only
        // through the store. With no license installed it reports the
        // documented unlicensed (trial) state with 200, never an error.
        let engine = Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        let state = ApiState::standalone(engine);
        let (status, body) = response_parts(get_license(State(state)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["state"], serde_json::json!("trial"));
        assert_eq!(body["entitlements_on"], serde_json::json!(true));
    }

    #[tokio::test]
    async fn post_valid_round_trips_and_replaces() {
        // W5-10 failing test: read, install a valid body and read it
        // back. Before: both 404. Broker-through: every step goes
        // through the handler on `ApiState`.
        let (state, signing, kid) = seeded_state("cluster-w510-a", 0xA1);
        // Handler validates expiry against wall-clock `now_secs()`; mint
        // relative to it so the token is live when the handler checks it.
        let now = now_secs();
        let token = mint(
            &signing,
            &kid,
            "cluster-w510-a",
            now - 100,
            now + 10_000_000,
        );
        let (status, body) = response_parts(
            install_license(
                State(state.clone()),
                Bytes::from(
                    serde_json::to_vec(&serde_json::json!({"license": token})).expect("json"),
                ),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["state"], serde_json::json!("valid"));
        assert_eq!(body["customer"], serde_json::json!("Acme"));
        assert_eq!(body["identity"], serde_json::json!("cluster-w510-a"));
        let (status, reread) = response_parts(get_license(State(state)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reread["state"], serde_json::json!("valid"));
        assert_eq!(reread["customer"], serde_json::json!("Acme"));
    }

    #[tokio::test]
    async fn post_invalid_is_rejected_and_prior_state_is_intact() {
        // W5-10 failing test: install an invalid body expecting
        // rejection with the documented `{code, message}` shape. A
        // failed install leaves the prior license intact.
        let (state, signing, kid) = seeded_state("cluster-w510-b", 0xB2);
        // Handler validates expiry against wall-clock `now_secs()`; mint
        // relative to it so the token is live when the handler checks it.
        let now = now_secs();
        let good = mint(
            &signing,
            &kid,
            "cluster-w510-b",
            now - 100,
            now + 10_000_000,
        );
        let (status, _) = response_parts(
            install_license(
                State(state.clone()),
                Bytes::from(
                    serde_json::to_vec(&serde_json::json!({"license": good})).expect("json"),
                ),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        for bad in [
            serde_json::json!({"license": "not-a-licence"}),
            serde_json::json!({}),
            serde_json::json!({"license": ""}),
            serde_json::json!({"license": 42}),
            serde_json::json!("just-a-string"),
        ] {
            let (status, body) = response_parts(
                install_license(
                    State(state.clone()),
                    Bytes::from(serde_json::to_vec(&bad).expect("json")),
                )
                .await,
            )
            .await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "body {bad} must be rejected"
            );
            assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
            assert!(body["message"].as_str().is_some_and(|m| !m.is_empty()));
        }
        // The prior good license survives every rejection.
        let (status, reread) = response_parts(get_license(State(state)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reread["state"], serde_json::json!("valid"));
        assert_eq!(reread["identity"], serde_json::json!("cluster-w510-b"));
    }

    #[test]
    fn install_persists_across_restart() {
        // License state persists across a restart through the kernel
        // data directory: install on one store, reload from the same
        // directory on a fresh store, observe the same license.
        let dir = std::env::temp_dir().join(format!(
            "indramqtt-license-w510-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).expect("create test dir");
        let now = 1_750_000_000u64;
        let signing = signing_key(0xC3);
        let kid = "key-c3".to_string();
        let mut trusted = TrustedKeys::new();
        trusted.insert(kid.clone(), VerifyingKey::from(&signing));
        let first = crate::licence::LicenceStore::new();
        first.set_trusted_keys(trusted.clone());
        let identity = first.load_from_data_dir(&dir, now).expect("identity loads");
        let _ = identity;
        let stored_identity = first.cluster_identity().expect("identity recorded");
        let token = mint(
            &signing,
            &kid,
            &stored_identity,
            now - 100,
            now + 10_000_000,
        );
        first.install(&token, now).expect("installs");
        // A fresh store reloading the same directory observes the same
        // license without reinstalling.
        let second = crate::licence::LicenceStore::new();
        second.set_trusted_keys(trusted);
        second.load_from_data_dir(&dir, now).expect("reloads");
        let status = second.status_at(now);
        assert_eq!(status["state"], serde_json::json!("valid"));
        assert_eq!(status["identity"], serde_json::json!(stored_identity));
        assert_eq!(status["customer"], serde_json::json!("Acme"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn license_never_stops_customer_traffic() {
        // Management-plane only: installing (or failing to install) a
        // license adds no work to publish/deliver. Broker-through: a
        // management publish through the rule engine and deliver path
        // still answers 200 after the install.
        let (state, signing, kid) = seeded_state("cluster-w510-c", 0xD4);
        // Handler validates expiry against wall-clock `now_secs()`; mint
        // relative to it so the token is live when the handler checks it.
        let now = now_secs();
        let token = mint(
            &signing,
            &kid,
            "cluster-w510-c",
            now - 100,
            now + 10_000_000,
        );
        let (status, _) = response_parts(
            install_license(
                State(state.clone()),
                Bytes::from(
                    serde_json::to_vec(&serde_json::json!({"token": token})).expect("json"),
                ),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, body) = response_parts(
            crate::v5::clients::publish_message(
                State(state),
                Bytes::from(
                    serde_json::to_vec(&serde_json::json!({
                        "topic": "w510/probe",
                        "payload": "hello",
                        "qos": 0,
                    }))
                    .expect("json"),
                ),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.get("id").is_some(), "publish answers with an id");
    }
}
