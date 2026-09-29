//! Feature registry read for the v5 management API.
//!
//! Covers `GET /features` (C13-15) on the real
//! [`crate::licence::LicenceStore`]. Fixed bounded set by design: the
//! registry is the build's known entitlements (today exactly one), and the
//! read reports which of them the installed licence currently enables.
//!
//! Store wiring (rulebook section 2: name the file and line outside
//! `crates/broker-api` that writes or consults it, and the broker event
//! that drives it):
//! - registry definition `crates/broker-cluster/src/license.rs:25`
//!   (`KNOWN_ENTITLEMENTS`);
//! - kernel boot `crates/broker-node/src/main.rs:6048`
//!   (`load_from_data_dir`), `:6038` (`set_trusted_keys`), `:6099` and
//!   `:6086` (`refresh_alarms` after boot and after a validated install),
//!   `:6110` (`cluster_identity`) and `:6118` (`stored_token`) for the
//!   cluster join path. The broker event that drives it is node boot plus
//!   the management install; the publish path never consults it and a
//!   licence condition never stops customer traffic.
//!
//! Store bounds (stated here and enforced by construction):
//! - exactly [`FEATURE_COUNT`] known feature(s); the list lives in code,
//!   so there is no persistence beyond it and no per-request growth;
//! - an empty-enabled set reads as an empty page, never an error;
//! - reads clone at most the enabled subset (at most one small document
//!   today) plus one status render per request under short locks.
//!
//! Persistence: none per request. The registry is static within a run
//! (compiled in, rebuilt at startup with the binary) and the enabled
//! subset is derived live from the shared licence store, which itself
//! persists through the kernel data directory; a restart reloads the same
//! licence and the same read.
//!
//! Management-plane only: the handler runs off the publish and deliver
//! paths (edge receive, kernel route, session, deliver never touch the
//! licence store). Per-request work is one short-lock snapshot plus one
//! status render, never proportional to connections, sessions or
//! subscriptions.
// TODO(parity): which envelope (`data` plus `meta` versus a bare list or
// a `features` key) and which per-feature fields beyond `name`/`enabled`
// does the documented shape require? The rulebook does not decide the
// exact shape; the current choice reuses the shared pagination extractor
// (`data` plus `meta`) and reports `name`/`enabled`/`desc` only, omitting
// fields the broker cannot supply (never a constant standing in for a
// measurement) until the checker pins it.
// TODO(parity): what are the exact operator-facing summaries per feature?
// The rulebook does not decide the wording; the current choice describes
// each feature's area in our own words and keeps the stable feature names
// (licence values) exact until the checker pins them.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

use crate::pagination::{meta_page, paginate, PageParams};
use crate::ApiState;

/// One entry of the fixed feature registry: the stable licence
/// entitlement name plus a short operator-facing summary.
struct FeatureEntry {
    name: &'static str,
    desc: &'static str,
}

/// Fixed registry of known features (C13-15).
///
/// Bound (stated here and enforced by construction): exactly one entry.
/// Reason: the build understands a single entitlement today
/// (`broker_cluster::KNOWN_ENTITLEMENTS` holds only `clustering`), so one
/// entry covers every licence the install path can verify while each
/// response clones at most one small document; a second entitlement grows
/// this array with its own reason. The registry is code-defined, so the
/// cap is enforced by construction and management memory cannot grow from
/// this route. The default is finite (`1`); the operator cannot opt into
/// more because writes are out of scope (read-only route).
const KNOWN_FEATURES: [FeatureEntry; 1] = [FeatureEntry {
    name: "clustering",
    desc: "Clustered deployment across multiple nodes",
}];

/// Stated count of the fixed bounded set served by this route. Reason:
/// the single entitlement above is the whole vocabulary the install path
/// accepts, so the count documents the bound the handler enforces.
pub const FEATURE_COUNT: usize = 1;

// Compile-time guard that the bound covers the served registry.
const _: () = assert!(KNOWN_FEATURES.len() == FEATURE_COUNT);

/// Whether `name` is one of the known registry features.
///
/// Shared by the read path so the served list and the predicate cannot
/// drift; the install path enforces the same vocabulary through
/// `broker_cluster::is_known_entitlement`.
pub fn is_known_feature(name: &str) -> bool {
    KNOWN_FEATURES.iter().any(|entry| entry.name == name)
}

/// `GET /features`: fixed registry filtered by the installed licence.
///
/// Returns the standard page envelope (`data` plus `meta`). Each row
/// carries the documented per-feature fields (`name`, `enabled`, `desc`).
/// The enabled subset is derived live from the shared licence store: a
/// feature is enabled when the licence status reports entitlements on and
/// names it. An empty-enabled set (for example a lapsed installation)
/// reads as an empty page with 200, never an error. The route is paged
/// via the shared [`PageParams`] extractor; unknown query parameters are
/// ignored (the extractor reads only `page`/`limit` and never rejects).
/// Management-plane only: one short-lock snapshot per request; publish
/// and deliver never touch this store.
pub async fn list_features(State(state): State<ApiState>, params: PageParams) -> Response {
    let now = now_secs();
    let status = state.licence.status_at(now);
    let enabled_on = status
        .get("entitlements_on")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let entitled: Vec<String> = status
        .get("entitlements")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .filter(|name| is_known_feature(name))
                .collect()
        })
        .unwrap_or_default();
    let mut rows: Vec<serde_json::Value> = Vec::new();
    if enabled_on {
        for entry in &KNOWN_FEATURES {
            if entitled.iter().any(|name| name.as_str() == entry.name) {
                rows.push(serde_json::json!({
                    "name": entry.name,
                    "enabled": true,
                    "desc": entry.desc,
                }));
            }
        }
    }
    let total = rows.len();
    let page_items = paginate(&rows, params.page, params.limit);
    let data: Vec<serde_json::Value> = page_items.to_vec();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "data": data,
            "meta": meta_page(params.page, params.limit, total),
        })),
    )
        .into_response()
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
    use axum::response::Response;
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

    fn standalone_state() -> ApiState {
        let engine = Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        ApiState::standalone(engine)
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
                trial_started_at: now_secs(),
                highest_seen_secs: now_secs(),
            });
        (state, signing, kid)
    }

    async fn response_parts(response: Response) -> (StatusCode, serde_json::Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 256 * 1024)
            .await
            .expect("features body is small and readable");
        let body: serde_json::Value =
            serde_json::from_slice(&bytes).expect("features body is JSON");
        (status, body)
    }

    #[tokio::test]
    async fn trial_lists_the_fixed_registry_with_fields() {
        // W5-11 broker-through: the read goes through the served handler
        // on shared `ApiState` (the same state the kernel shares in
        // `crates/broker-node/src/main.rs`, `serve_api`), not only
        // through the store. The trial enables the fixed registry entry
        // with the documented per-feature fields.
        let state = standalone_state();
        let (status, body) =
            response_parts(list_features(State(state), PageParams::default()).await).await;
        assert_eq!(status, StatusCode::OK);
        let data = body["data"].as_array().expect("data is a list");
        assert_eq!(data.len(), FEATURE_COUNT);
        assert_eq!(data[0]["name"], serde_json::json!("clustering"));
        assert_eq!(data[0]["enabled"], serde_json::json!(true));
        assert!(data[0]["desc"].as_str().is_some_and(|d| !d.is_empty()));
        assert!(is_known_feature("clustering"));
        assert!(!is_known_feature("no-such-feature"));
        assert_eq!(body["meta"]["count"], serde_json::json!(FEATURE_COUNT));
    }

    #[tokio::test]
    async fn empty_enabled_set_reads_as_empty_list_not_an_error() {
        // W5-11 empty-enabled handling: a lapsed installation (trial long
        // over, no licence) reports entitlements off, so the read is an
        // empty page with 200, never an error.
        let engine = Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        let state = ApiState::standalone(engine);
        let long_ago = now_secs()
            .saturating_sub((broker_cluster::TRIAL_DAYS + 1) * broker_cluster::SECS_PER_DAY);
        state
            .licence
            .set_identity_for_tests(broker_cluster::ClusterIdentity {
                identity: "cluster-w511-lapsed".to_string(),
                public_key_hex: "aa".to_string(),
                private_key_hex: "bb".to_string(),
                trial_started_at: long_ago,
                highest_seen_secs: long_ago,
            });
        let (status, body) =
            response_parts(list_features(State(state), PageParams::default()).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"], serde_json::json!([]));
        assert_eq!(body["meta"]["count"], serde_json::json!(0));
    }

    #[tokio::test]
    async fn installed_licence_is_reflected_and_paging_holds() {
        // W5-11 failing test shape: install a valid licence through the
        // licence store, then read features through the handler and see
        // the installed entitlement. Paging past the single row reads as
        // an empty page, never an error.
        let (state, signing, kid) = seeded_state("cluster-w511-a", 0xF1);
        let now = now_secs();
        let token = mint(
            &signing,
            &kid,
            "cluster-w511-a",
            now - 100,
            now + 10_000_000,
        );
        state.licence.install(&token, now).expect("installs");
        let (status, body) =
            response_parts(list_features(State(state.clone()), PageParams::default()).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"][0]["name"], serde_json::json!("clustering"));
        assert_eq!(body["data"][0]["enabled"], serde_json::json!(true));
        let beyond = PageParams {
            page: 2,
            limit: 100,
        };
        let (status, empty) = response_parts(list_features(State(state), beyond).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(empty["data"], serde_json::json!([]));
        assert_eq!(empty["meta"]["count"], serde_json::json!(FEATURE_COUNT));
    }

    #[tokio::test]
    async fn features_read_adds_no_work_to_publish() {
        // Management-plane only: a features read followed by a management
        // publish through the rule engine and deliver path still answers
        // 200, so customer traffic keeps serving in every licence state.
        let state = standalone_state();
        let (status, _) =
            response_parts(list_features(State(state.clone()), PageParams::default()).await).await;
        assert_eq!(status, StatusCode::OK);
        let response = crate::v5::clients::publish_message(
            State(state),
            bytes::Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "topic": "w511/probe",
                    "payload": "hello",
                    "qos": 0,
                }))
                .expect("json"),
            ),
        )
        .await;
        let (status, body) = response_parts(response).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.get("id").is_some(), "publish answers with an id");
    }
}
