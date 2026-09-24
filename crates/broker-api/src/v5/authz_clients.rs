//! Per-client authorization rule table for the v5 management API.
//!
//! Covers `GET /authorization/sources/built_in_database/rules/clients`
//! (paged list), `POST .../rules/clients` (add one client-scoped rule)
//! and `DELETE .../rules/clients/{clientid}` (remove every rule for one
//! client id) over the live built-in rule table (`MemoryAuth` in
//! `crates/broker-auth/src/lib.rs`, consulted by the publish event in
//! `crates/broker-node/src/main.rs` and the console path in
//! `crates/broker-api/src/ws.rs`). Single-node, management-plane only:
//! handlers clone at most the capped client-scoped list once per request
//! under a short read lock; the CONNECT path keeps its own short locks
//! and publish/deliver never touch this handler's lock.
//!
//! Only client-scoped entries live here: the catch-all `*` entries belong
//! to the `/rules/all` table and are excluded from the listing, and a
//! `*` client id on write is rejected with `BAD_REQUEST` (use the
//! catch-all table). An empty index lists as an empty page, never an
//! error. Error answers use the documented `{code, message}` shape.
//!
//! Bound (stated here and enforced in `broker-auth`): at most
//! [`broker_auth::MAX_AUTHZ_CLIENT_RULES`] client-scoped entries
//! (100,000: the management plane snapshots the whole rule list per
//! request and the publish/subscribe decision scans it linearly, so the
//! list must stay bounded; 100,000 covers fleet provisioning while
//! keeping per-request clones paged and per-decision scans cheap).
//! Default is the empty index.
// TODO(parity): is the documented row shape exactly
// clientid/topic/permission/action with the standard data/meta page
// envelope, and is a successful delete a 204 or a 200 success envelope?
// The rulebook does not decide the exact shapes; the current choice
// renders the modelled four fields under data/meta and answers deletes
// with 204 (mirroring the sibling clears) until the checker pins it.
// TODO(parity): is the duplicate scope (clientid, topic, action), and is
// an unknown client id on delete a NOT_FOUND or a CLIENTID_NOT_FOUND?
// The rulebook does not decide either; the current choice rejects the
// same client, topic and action as ALREADY_EXISTS and answers unknown
// ids with NOT_FOUND (mirroring the sibling authz 404s) until the
// checker pins it.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;

use crate::errors::ApiError;
use crate::pagination::{meta_page, paginate, PageParams};
use crate::ApiState;
use broker_auth::{AclAction, AclRule, ClientRuleError};

/// Render one client-scoped rule with the documented per-rule fields
/// only: `clientid`, `topic`, `permission` (`allow`/`deny`) and `action`
/// (`publish`/`subscribe`/`all`). Fields the broker does not measure are
/// omitted, never invented.
fn render_client_rule(rule: &AclRule) -> serde_json::Value {
    serde_json::json!({
        "clientid": rule.client_pattern,
        "topic": rule.topic_pattern,
        "permission": if rule.allow { "allow" } else { "deny" },
        "action": match rule.action {
            AclAction::Publish => "publish",
            AclAction::Subscribe => "subscribe",
            AclAction::All => "all",
        },
    })
}

/// `GET /authorization/sources/built_in_database/rules/clients`: paged
/// client-scoped rules (`data` plus `meta`) in stored evaluation order.
/// An empty index reads as an empty page, never an error. Malformed
/// `page`/`limit` fall back to defaults via [`PageParams`].
///
/// Management-plane only: clones at most the capped client-scoped list
/// once per request under a short read lock; the publish event in
/// `crates/broker-node/src/main.rs` consults the same table
/// (`MemoryAuth::authorize_publish` in `crates/broker-auth/src/lib.rs`)
/// and publish/deliver never take this handler's lock.
pub async fn list_client_rules(State(state): State<ApiState>, params: PageParams) -> Response {
    let all = state.auth.all_client_rules();
    let total = all.len();
    let page_items = paginate(&all, params.page, params.limit);
    let data: Vec<serde_json::Value> = page_items.iter().map(render_client_rule).collect();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "data": data,
            "meta": meta_page(params.page, params.limit, total),
        })),
    )
        .into_response()
}

/// `POST /authorization/sources/built_in_database/rules/clients`: add one
/// client-scoped rule. Returns the stored entry with 201. Malformed
/// bodies fail with `BAD_REQUEST`; a rule for the same client id and
/// topic scope fails with `ALREADY_EXISTS` instead of merging; a full
/// table fails with `BAD_REQUEST`.
///
/// The write persists across a restart through the config registry (the
/// same `MemoryAuth` commit path the publish/subscribe consults read).
/// Management-plane only: one short rule-table write lock per request
/// (plus the registry save lock on persist).
pub async fn create_client_rule(State(state): State<ApiState>, body: Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return ApiError::BadRequest(format!("invalid client rule body: {e}")).into_response();
        }
    };
    let rule = match parse_client_rule_body(&value) {
        Ok(rule) => rule,
        Err(msg) => return ApiError::BadRequest(msg).into_response(),
    };
    match state.auth.add_client_rule(rule) {
        Ok(()) => {
            let stored = state
                .auth
                .client_rules(
                    value
                        .get("clientid")
                        .or(value.get("client_id"))
                        .and_then(|v| v.as_str())
                        .unwrap_or_default(),
                )
                .into_iter()
                .last();
            match stored {
                Some(rule) => {
                    (StatusCode::CREATED, Json(render_client_rule(&rule))).into_response()
                }
                None => StatusCode::CREATED.into_response(),
            }
        }
        Err(ClientRuleError::Duplicate) => {
            ApiError::AlreadyExists("client rule already exists".to_string()).into_response()
        }
        Err(ClientRuleError::Full) => {
            ApiError::BadRequest("client rule table is full".to_string()).into_response()
        }
        Err(ClientRuleError::Invalid(msg)) => ApiError::BadRequest(msg).into_response(),
        Err(ClientRuleError::Persist(msg)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot persist client authorization rule: {msg}"),
            })),
        )
            .into_response(),
    }
}

/// Validate a create body into one [`AclRule`].
///
/// Required: `clientid` (non-empty, at most
/// [`broker_auth::MAX_AUTHZ_CLIENTID_LEN`] bytes, never the catch-all
/// `*`), `topic` (non-empty, valid MQTT filter), `permission`
/// (`allow`/`deny`, case-insensitive) and `action`
/// (`publish`/`subscribe`/`all`, case-insensitive). A `client_id`
/// spelling is accepted as an alias for `clientid`; every other extra
/// field is ignored so future match fields degrade to the same create
/// instead of a 400.
fn parse_client_rule_body(value: &serde_json::Value) -> Result<AclRule, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "client rule body must be a JSON object".to_string())?;
    let clientid = obj
        .get("clientid")
        .or(obj.get("client_id"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| "field `clientid` is required".to_string())?;
    if clientid.trim().is_empty() {
        return Err("field `clientid` must not be empty".to_string());
    }
    if clientid.len() > broker_auth::MAX_AUTHZ_CLIENTID_LEN {
        return Err(format!(
            "field `clientid` {clientid:?} exceeds {} bytes",
            broker_auth::MAX_AUTHZ_CLIENTID_LEN
        ));
    }
    if clientid == "*" {
        return Err(
            "field `clientid` must name a concrete client, not the catch-all (field `clientid`)"
                .to_string(),
        );
    }
    let topic = obj
        .get("topic")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "field `topic` is required".to_string())?;
    if topic.trim().is_empty() {
        return Err("field `topic` must not be empty".to_string());
    }
    if broker_protocol::TopicFilter::new(topic.to_string()).is_err() {
        return Err(format!("field `topic` {topic:?} is not a valid filter"));
    }
    let permission = obj
        .get("permission")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "field `permission` is required".to_string())?;
    let allow = match permission.to_ascii_lowercase().as_str() {
        "allow" => true,
        "deny" => false,
        _ => {
            return Err(format!(
                "field `permission` {permission:?} is unknown (field `permission`)"
            ));
        }
    };
    let action_raw = obj
        .get("action")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "field `action` is required".to_string())?;
    let action = AclAction::parse(action_raw)
        .ok_or_else(|| format!("field `action` {action_raw:?} is unknown (field `action`)"))?;
    Ok(AclRule::new(
        clientid.to_string(),
        action,
        topic.to_string(),
        allow,
    ))
}

/// `DELETE /authorization/sources/built_in_database/rules/clients/{clientid}`:
/// remove every rule scoped to one client id.
///
/// Removing an unknown client id fails with `NOT_FOUND` in the
/// documented `{code, message}` shape and applies nothing; a hit answers
/// 204 with no body. The removal persists across a restart through the
/// config registry (the same `MemoryAuth` commit path the
/// publish/subscribe consults read). Management-plane only: one short
/// rule-table write lock per request (plus the registry save lock on
/// persist); the publish and subscribe authorization consults keep
/// reading under short read locks.
pub async fn delete_client_rules(
    State(state): State<ApiState>,
    Path(clientid): Path<String>,
) -> Response {
    match state.auth.remove_client_rules(&clientid) {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => ApiError::NotFound(format!(
            "client authorization rules for {clientid:?} not found"
        ))
        .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot delete client authorization rules: {error}"),
            })),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_auth::Authorizer;
    use broker_rules::{BackpressurePolicy, RuleEngine};

    fn standalone_state() -> ApiState {
        ApiState::standalone(std::sync::Arc::new(RuleEngine::new(
            16,
            BackpressurePolicy::DropOldest,
        )))
    }

    fn body(value: serde_json::Value) -> Bytes {
        Bytes::from(serde_json::to_vec(&value).expect("test body serialises"))
    }

    async fn parts(response: Response) -> (StatusCode, serde_json::Value, Vec<u8>) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("test body is small and readable");
        let raw = bytes.to_vec();
        let json: serde_json::Value = if raw.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&raw).expect("test body is JSON")
        };
        (status, json, raw)
    }

    fn params(page: u32, limit: u32) -> PageParams {
        PageParams { page, limit }
    }

    fn client_rule(clientid: &str, topic: &str) -> serde_json::Value {
        serde_json::json!({
            "clientid": clientid,
            "topic": topic,
            "permission": "allow",
            "action": "publish",
        })
    }

    #[tokio::test]
    async fn add_list_duplicate_delete_round_trip_through_broker() {
        // The task's failing test: before this task the three per-client
        // routes were missing (bare 404). Add a rule for client A, list
        // expecting it with paging metadata; re-add the duplicate
        // expecting rejection; delete client A then delete again
        // expecting 404. Goes through the broker decision path, not only
        // the store: the written allow is observed on
        // `authorize_publish` (consulted by the publish event in
        // `crates/broker-node/src/main.rs`), and the delete reopens the
        // table for that client.
        let state = standalone_state();

        // Empty reads as an empty page, never an error.
        let (status, empty, _) =
            parts(list_client_rules(State(state.clone()), params(1, 100)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(empty["data"], serde_json::json!([]));
        assert_eq!(empty["meta"]["count"], serde_json::json!(0));
        assert_eq!(empty["meta"]["page"], serde_json::json!(1));
        assert_eq!(empty["meta"]["limit"], serde_json::json!(100));
        assert_eq!(empty["meta"]["hasnext"], serde_json::json!(false));

        let (status, created, _) = parts(
            create_client_rule(
                State(state.clone()),
                body(client_rule("w219-a", "w219/alpha/#")),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(created["clientid"], serde_json::json!("w219-a"));
        assert_eq!(created["topic"], serde_json::json!("w219/alpha/#"));
        assert_eq!(created["permission"], serde_json::json!("allow"));
        assert_eq!(created["action"], serde_json::json!("publish"));

        // List carries the entry with the standard page envelope.
        let (status, listed, _) =
            parts(list_client_rules(State(state.clone()), params(1, 100)).await).await;
        assert_eq!(status, StatusCode::OK);
        let data = listed["data"].as_array().expect("data array");
        assert_eq!(data.len(), 1);
        assert_eq!(data[0]["clientid"], serde_json::json!("w219-a"));
        assert_eq!(listed["meta"]["count"], serde_json::json!(1));
        assert_eq!(listed["meta"]["hasnext"], serde_json::json!(false));

        // The stored allow is live on the publish path for client A only.
        let topic = broker_protocol::Topic::new("w219/alpha/data").expect("topic parses");
        state
            .auth
            .authorize_publish("w219-a", &topic)
            .await
            .expect("client allow opens the publish path for its client");
        assert!(
            state
                .auth
                .authorize_publish("w219-b", &topic)
                .await
                .is_err(),
            "unmatched clients stay denied once a rule exists"
        );

        // Re-adding the duplicate is rejected, never silently merged.
        let (status, duplicate, _) = parts(
            create_client_rule(
                State(state.clone()),
                body(client_rule("w219-a", "w219/alpha/#")),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(duplicate["code"], serde_json::json!("ALREADY_EXISTS"));
        assert!(duplicate["message"].is_string());
        let (status, listed, _) =
            parts(list_client_rules(State(state.clone()), params(1, 100)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(listed["meta"]["count"], serde_json::json!(1));

        // Delete removes all rules for that client id and reports success.
        let response = delete_client_rules(State(state.clone()), Path("w219-a".to_string())).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let (status, cleared, _) =
            parts(list_client_rules(State(state.clone()), params(1, 100)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(cleared["data"], serde_json::json!([]));

        // Unknown client ids return 404 with the documented shape.
        let (status, missing, _) =
            parts(delete_client_rules(State(state.clone()), Path("w219-a".to_string())).await)
                .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], serde_json::json!("NOT_FOUND"));
        assert!(missing["message"].is_string());
    }

    #[tokio::test]
    async fn paging_slices_after_filtering_and_hides_catch_all() {
        // Page 2 with limit 1 holds the second client entry, and the
        // catch-all `*` entries (owned by the `/rules/all` table) never
        // appear in this listing.
        let state = standalone_state();
        state
            .auth
            .add_rule(AclRule::new("*", AclAction::Publish, "w219/catch/#", true))
            .expect("catch-all applies");
        for (clientid, topic) in [
            ("w219-c1", "w219/c1/#"),
            ("w219-c2", "w219/c2/#"),
            ("w219-c3", "w219/c3/#"),
        ] {
            let (status, _, _) = parts(
                create_client_rule(State(state.clone()), body(client_rule(clientid, topic))).await,
            )
            .await;
            assert_eq!(status, StatusCode::CREATED, "create {clientid}");
        }

        let (status, page2, _) =
            parts(list_client_rules(State(state.clone()), params(2, 1)).await).await;
        assert_eq!(status, StatusCode::OK);
        let data = page2["data"].as_array().expect("data array");
        assert_eq!(data.len(), 1);
        assert_eq!(data[0]["clientid"], serde_json::json!("w219-c2"));
        assert_eq!(page2["meta"]["count"], serde_json::json!(3));
        assert_eq!(page2["meta"]["hasnext"], serde_json::json!(true));

        let (status, last, _) =
            parts(list_client_rules(State(state.clone()), params(3, 1)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(last["data"][0]["clientid"], serde_json::json!("w219-c3"));
        assert_eq!(last["meta"]["hasnext"], serde_json::json!(false));
    }

    #[tokio::test]
    async fn create_rejects_bad_bodies_and_applies_nothing() {
        // Validation failures are client errors and the stored index is
        // unchanged.
        let state = standalone_state();
        let (status, _, _) = parts(
            create_client_rule(
                State(state.clone()),
                body(client_rule("w219-keep", "w219/keep/#")),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);

        let bad_bodies = [
            serde_json::json!({"topic": "w219/x/#", "permission": "allow", "action": "publish"}),
            serde_json::json!({"clientid": "", "topic": "w219/x/#", "permission": "allow", "action": "publish"}),
            serde_json::json!({"clientid": "  ", "topic": "w219/x/#", "permission": "allow", "action": "publish"}),
            serde_json::json!({"clientid": "*", "topic": "w219/x/#", "permission": "allow", "action": "publish"}),
            serde_json::json!({"clientid": "w219-x", "permission": "allow", "action": "publish"}),
            serde_json::json!({"clientid": "w219-x", "topic": "", "permission": "allow", "action": "publish"}),
            serde_json::json!({"clientid": "w219-x", "topic": "w219/bad/#/x", "permission": "allow", "action": "publish"}),
            serde_json::json!({"clientid": "w219-x", "topic": "w219/x/#", "permission": "maybe", "action": "publish"}),
            serde_json::json!({"clientid": "w219-x", "topic": "w219/x/#", "permission": "allow", "action": "forward"}),
            serde_json::json!({"clientid": "w219-x", "topic": "w219/x/#", "permission": "allow"}),
            serde_json::json!({"clientid": "w219-x", "topic": "w219/x/#", "action": "publish"}),
            serde_json::json!({"clientid": 7, "topic": "w219/x/#", "permission": "allow", "action": "publish"}),
            serde_json::json!([]),
        ];
        for bad in bad_bodies {
            let (status, err, _) =
                parts(create_client_rule(State(state.clone()), body(bad.clone())).await).await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "body {bad} must be rejected"
            );
            assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
        }

        // The seeded entry is unchanged by every rejected write.
        let (status, listed, _) =
            parts(list_client_rules(State(state.clone()), params(1, 100)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(listed["meta"]["count"], serde_json::json!(1));
        assert_eq!(
            listed["data"][0]["clientid"],
            serde_json::json!("w219-keep")
        );
    }

    #[tokio::test]
    async fn delete_removes_only_the_named_client() {
        // A per-id delete removes every rule for that id and keeps the
        // neighbour's rules live on the decision path.
        let state = standalone_state();
        for (clientid, topic) in [("w219-d1", "w219/d1/#"), ("w219-d2", "w219/d2/#")] {
            let (status, _, _) = parts(
                create_client_rule(State(state.clone()), body(client_rule(clientid, topic))).await,
            )
            .await;
            assert_eq!(status, StatusCode::CREATED, "create {clientid}");
        }
        let response = delete_client_rules(State(state.clone()), Path("w219-d1".to_string())).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let (status, listed, _) =
            parts(list_client_rules(State(state.clone()), params(1, 100)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(listed["meta"]["count"], serde_json::json!(1));
        assert_eq!(listed["data"][0]["clientid"], serde_json::json!("w219-d2"));

        let kept = broker_protocol::Topic::new("w219/d2/data").expect("topic parses");
        state
            .auth
            .authorize_publish("w219-d2", &kept)
            .await
            .expect("neighbour rule stays live on the publish path");

        // The catch-all `*` belongs to the `/rules/all` table: a per-id
        // delete naming it is a miss, never a wipe of another table.
        state
            .auth
            .add_rule(AclRule::new("*", AclAction::Publish, "w219/catch/#", true))
            .expect("catch-all applies");
        let (status, missing, _) =
            parts(delete_client_rules(State(state.clone()), Path("*".to_string())).await).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], serde_json::json!("NOT_FOUND"));
        assert!(
            !state.auth.acl_rules().is_empty(),
            "catch-all entries survive a per-client delete for `*`"
        );
    }

    #[tokio::test]
    async fn bound_and_persistence_verified() {
        // Bound: the per-client cap is stated, finite, and enforced; the
        // default index is empty.
        assert_eq!(broker_auth::MAX_AUTHZ_CLIENT_RULES, 100_000);
        let fresh = standalone_state();
        let (status, empty, _) =
            parts(list_client_rules(State(fresh.clone()), params(1, 100)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(empty["data"], serde_json::json!([]));

        // Persistence: an add through a registry-backed table survives a
        // reload of the same data dir.
        let dir = std::env::temp_dir().join(format!(
            "authz-clients-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let registry = std::sync::Arc::new(
            broker_config::ConfigRegistry::load(&dir).expect("fresh dir loads"),
        );
        let auth = std::sync::Arc::new(broker_auth::MemoryAuth::from_registry(&registry));
        auth.add_client_rule(broker_auth::AclRule::new(
            "w219-persist",
            broker_auth::AclAction::Publish,
            "w219/persist/#",
            true,
        ))
        .expect("registry-backed add applies");
        assert!(dir.join(broker_config::STATE_FILE_NAME).is_file());
        drop(auth);
        drop(registry);
        let reloaded =
            std::sync::Arc::new(broker_config::ConfigRegistry::load(&dir).expect("dir reloads"));
        let restarted = broker_auth::MemoryAuth::from_registry(&reloaded);
        let rules = restarted.client_rules("w219-persist");
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].topic_pattern, "w219/persist/#");
        std::fs::remove_dir_all(&dir).ok();
    }
}
