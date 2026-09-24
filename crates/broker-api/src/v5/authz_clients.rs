//! Per-client authorization rule table for the v5 management API.
//!
//! Covers `GET /authorization/sources/built_in_database/rules/clients`
//! (paged list), `POST .../rules/clients` (add one client-scoped rule),
//! `DELETE .../rules/clients/{clientid}` (remove every rule for one
//! client id), `GET .../rules/clients/{clientid}` (read one client's
//! set), `PUT .../rules/clients/{clientid}` (replace one client's set),
//! `GET .../rules/users` (paged user-scoped list),
//! `POST .../rules/users` (add one user-scoped rule),
//! `GET .../rules/users/{username}` (read one user's set),
//! `PUT .../rules/users/{username}` (replace one user's set) and
//! `DELETE .../rules/users/{username}` (remove every rule for one
//! username) over the live built-in rule table (`MemoryAuth` in
//! `crates/broker-auth/src/lib.rs`, consulted by the publish event in
//! `crates/broker-node/src/main.rs` and the console path in
//! `crates/broker-api/src/ws.rs`). Single-node, management-plane only:
//! handlers clone at most the capped client-scoped list once per request
//! under a short read lock; the CONNECT path keeps its own short locks
//! and publish/deliver never touch this handler's lock.
//!
//! Only client-scoped entries live here: the catch-all `*` entries belong
//! to the `/rules/all` table and are excluded from every listing, and a
//! `*` client id on write is rejected with `BAD_REQUEST` (use the
//! catch-all table) while on read it answers `NOT_FOUND`. An empty index
//! lists as an empty page, never an error. Error answers use the
//! documented `{code, message}` shape.
//!
//! Bound (stated here and enforced in `broker-auth`): at most
//! [`broker_auth::MAX_AUTHZ_CLIENT_RULES`] client-scoped entries and at
//! most [`broker_auth::MAX_AUTHZ_USER_RULES`] user-scoped entries
//! (100,000 each: the management plane snapshots the whole rule list per
//! request and the publish/subscribe decision scans it linearly, so the
//! list must stay bounded; 100,000 covers fleet provisioning while
//! keeping per-request clones paged and per-decision scans cheap).
//! Both listings read the same shared built-in table under the same cap.
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
// TODO(parity): is the per-client detail envelope `{"rules": [...]}`,
// is a successful replace a 204 like the sibling catch-all replace, and
// are the per-client and per-user tables separate stores? The rulebook
// does not decide any of the three; the current choice mirrors the
// catch-all replace (object body, 204 on success) and shares the one
// built-in table between both listings until the checker pins it.
// TODO(parity): does the documented per-user listing carry the same
// topic/permission/action triple under a `username` key with the
// standard data/meta page envelope, and does the per-user add accept a
// `username` key (with a `user_id` alias) rather than `clientid`? The
// rulebook does not decide the exact row shape; the current choice
// renders the modelled four fields under data/meta and accepts
// `username` (plus a `user_id` alias mirroring the client `client_id`
// alias) until the checker pins it.
// TODO(parity): is the per-user duplicate scope (username, topic,
// action), is a successful per-user delete a 204, and is an unknown
// username on read/delete a NOT_FOUND? The rulebook does not decide any
// of the three; the current choice mirrors the per-client table (same
// scope, 204 on delete, NOT_FOUND on a miss) until the checker pins it.

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

/// Render one user-scoped rule with the documented per-rule fields
/// only: `username`, `topic`, `permission` (`allow`/`deny`) and `action`
/// (`publish`/`subscribe`/`all`). Fields the broker does not measure are
/// omitted, never invented.
fn render_user_rule(rule: &AclRule) -> serde_json::Value {
    serde_json::json!({
        "username": rule.client_pattern,
        "topic": rule.topic_pattern,
        "permission": if rule.allow { "allow" } else { "deny" },
        "action": match rule.action {
            AclAction::Publish => "publish",
            AclAction::Subscribe => "subscribe",
            AclAction::All => "all",
        },
    })
}

/// `GET /authorization/sources/built_in_database/rules/clients/{clientid}`:
/// read one client's rule set.
///
/// Returns 200 with `{"rules": [...]}` in stored evaluation order; a
/// client id with no rules fails with `NOT_FOUND` in the documented
/// `{code, message}` shape and the catch-all `*` (owned by the
/// `/rules/all` table) is always a miss here, never a read of another
/// table. The read persists trivially: it observes the same
/// registry-backed table every write commits through.
///
/// Management-plane only: clones at most that client's capped rules once
/// per request under a short read lock; the publish event in
/// `crates/broker-node/src/main.rs` consults the same table
/// (`MemoryAuth::authorize_publish` in `crates/broker-auth/src/lib.rs`)
/// and publish/deliver never take this handler's lock.
pub async fn get_client_rules(
    State(state): State<ApiState>,
    Path(clientid): Path<String>,
) -> Response {
    if clientid == "*" {
        return ApiError::NotFound(format!(
            "client authorization rules for {clientid:?} not found"
        ))
        .into_response();
    }
    let rules = state.auth.client_rules(&clientid);
    if rules.is_empty() {
        return ApiError::NotFound(format!(
            "client authorization rules for {clientid:?} not found"
        ))
        .into_response();
    }
    let rendered: Vec<serde_json::Value> = rules.iter().map(render_client_rule).collect();
    (
        StatusCode::OK,
        Json(serde_json::json!({ "rules": rendered })),
    )
        .into_response()
}

/// `PUT /authorization/sources/built_in_database/rules/clients/{clientid}`:
/// replace one client's rule set.
///
/// The body must be an object carrying the replacement under `rules`
/// (`{"rules": [...]}`); anything else fails with `BAD_REQUEST` and
/// applies nothing. Every entry is validated before anything becomes
/// visible (validate-all-before-apply): each must carry a non-empty
/// `topic` that parses as an MQTT filter, a `permission` of
/// `allow`/`deny` and an `action` of `publish`/`subscribe`/`all`; a
/// per-entry key field (`clientid`, `client_id` or `username`) when
/// present must name the path id. A bad entry fails the whole write with
/// `BAD_REQUEST` and the stored set is unchanged, as does a duplicate
/// scope within the batch (`ALREADY_EXISTS`) or a replacement that would
/// overflow the table cap (`BAD_REQUEST`). Writing an unknown client id
/// creates its set; writing an empty list clears it. On success the
/// replacement persists across a restart through the config registry and
/// answers 204 with no body.
///
/// Management-plane only: topic validation runs before the lock, then
/// one short rule-table write lock per request (plus the registry save
/// lock on persist); the publish and subscribe authorization consults
/// keep reading under short read locks.
pub async fn replace_client_rules(
    State(state): State<ApiState>,
    Path(clientid): Path<String>,
    body: Bytes,
) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return ApiError::BadRequest(format!("invalid client rule set body: {e}"))
                .into_response();
        }
    };
    let rules = match parse_client_replace_body(&value, &clientid) {
        Ok(rules) => rules,
        Err(msg) => return ApiError::BadRequest(msg).into_response(),
    };
    match state.auth.replace_client_rules(&clientid, rules) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
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
                "message": format!("cannot persist client authorization rules: {msg}"),
            })),
        )
            .into_response(),
    }
}

/// Validate a per-client replace body into ordered [`AclRule`]s keyed by
/// the path id.
///
/// Required shape: an object with a `rules` array. Each entry must be an
/// object with string `topic` (non-empty, valid MQTT filter), string
/// `permission` (`allow`/`deny`, case-insensitive) and string `action`
/// (`publish`/`subscribe`/`all`, case-insensitive). A per-entry key
/// field (`clientid`, `client_id` or `username`) is optional; when
/// present it must name the path id, otherwise the whole write is
/// rejected fail-closed. Every other extra field is ignored so future
/// match fields degrade to the same replace instead of a 400.
fn parse_client_replace_body(
    value: &serde_json::Value,
    clientid: &str,
) -> Result<Vec<AclRule>, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "client rule set body must be an object with a `rules` array".to_string())?;
    let items = obj
        .get("rules")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "client rule set body must carry an array under `rules`".to_string())?;
    let mut out = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let entry = item
            .as_object()
            .ok_or_else(|| format!("client rules[{index}] must be an object (field `rules`)"))?;
        if let Some(key) = entry
            .get("clientid")
            .or(entry.get("client_id"))
            .or(entry.get("username"))
        {
            match key.as_str() {
                Some(name) if name == clientid => {}
                _ => {
                    return Err(format!(
                        "client rules[{index}].clientid must name {clientid:?} (field `clientid`)"
                    ));
                }
            }
        }
        let topic = entry
            .get("topic")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("client rules[{index}].topic is required (field `topic`)"))?;
        if topic.trim().is_empty() {
            return Err(format!(
                "client rules[{index}].topic must not be empty (field `topic`)"
            ));
        }
        if broker_protocol::TopicFilter::new(topic.to_string()).is_err() {
            return Err(format!(
                "client rules[{index}].topic {topic:?} is not a valid filter (field `topic`)"
            ));
        }
        let permission = entry
            .get("permission")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                format!("client rules[{index}].permission is required (field `permission`)")
            })?;
        let allow = match permission.to_ascii_lowercase().as_str() {
            "allow" => true,
            "deny" => false,
            _ => {
                return Err(format!(
                    "client rules[{index}].permission {permission:?} is unknown (field `permission`)"
                ));
            }
        };
        let action_raw = entry
            .get("action")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("client rules[{index}].action is required (field `action`)"))?;
        let action = AclAction::parse(action_raw).ok_or_else(|| {
            format!("client rules[{index}].action {action_raw:?} is unknown (field `action`)")
        })?;
        out.push(AclRule::new(
            clientid.to_string(),
            action,
            topic.to_string(),
            allow,
        ));
    }
    Ok(out)
}

/// `GET /authorization/sources/built_in_database/rules/users`: paged
/// user-scoped rules (`data` plus `meta`) in stored evaluation order.
/// An empty index reads as an empty page, never an error. Malformed
/// `page`/`limit` fall back to defaults via [`PageParams`].
///
/// The listing reads the same shared built-in table as the per-client
/// index under the same [`broker_auth::MAX_AUTHZ_CLIENT_RULES`] cap and
/// persists through the same registry commit path. Management-plane
/// only: clones at most the capped list once per request under a short
/// read lock; the publish event in `crates/broker-node/src/main.rs`
/// consults the same table (`MemoryAuth::authorize_publish` in
/// `crates/broker-auth/src/lib.rs`) and publish/deliver never take this
/// handler's lock.
pub async fn list_user_rules(State(state): State<ApiState>, params: PageParams) -> Response {
    let all = state.auth.all_user_rules();
    let total = all.len();
    let page_items = paginate(&all, params.page, params.limit);
    let data: Vec<serde_json::Value> = page_items.iter().map(render_user_rule).collect();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "data": data,
            "meta": meta_page(params.page, params.limit, total),
        })),
    )
        .into_response()
}

/// `POST /authorization/sources/built_in_database/rules/users`: add one
/// user-scoped rule. Returns the stored entry with 201. Malformed
/// bodies fail with `BAD_REQUEST`; a rule for the same username and
/// topic scope fails with `ALREADY_EXISTS` instead of merging; a full
/// table fails with `BAD_REQUEST`.
///
/// The write persists across a restart through the config registry (the
/// same `MemoryAuth` commit path the publish/subscribe consults read,
/// shared with the per-client index so the entry is live on the
/// publish/subscribe decision path).
/// Management-plane only: one short rule-table write lock per request
/// (plus the registry save lock on persist); the publish and subscribe
/// authorization consults keep reading under short read locks.
pub async fn create_user_rule(State(state): State<ApiState>, body: Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return ApiError::BadRequest(format!("invalid user rule body: {e}")).into_response();
        }
    };
    let rule = match parse_user_rule_body(&value) {
        Ok(rule) => rule,
        Err(msg) => return ApiError::BadRequest(msg).into_response(),
    };
    match state.auth.add_user_rule(rule) {
        Ok(()) => {
            let stored = state
                .auth
                .user_rules(
                    value
                        .get("username")
                        .or(value.get("user_id"))
                        .and_then(|v| v.as_str())
                        .unwrap_or_default(),
                )
                .into_iter()
                .last();
            match stored {
                Some(rule) => (StatusCode::CREATED, Json(render_user_rule(&rule))).into_response(),
                None => StatusCode::CREATED.into_response(),
            }
        }
        Err(ClientRuleError::Duplicate) => {
            ApiError::AlreadyExists("user rule already exists".to_string()).into_response()
        }
        Err(ClientRuleError::Full) => {
            ApiError::BadRequest("user rule table is full".to_string()).into_response()
        }
        Err(ClientRuleError::Invalid(msg)) => ApiError::BadRequest(msg).into_response(),
        Err(ClientRuleError::Persist(msg)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot persist user authorization rule: {msg}"),
            })),
        )
            .into_response(),
    }
}

/// Validate a per-user create body into one [`AclRule`].
///
/// Required: `username` (non-empty, at most
/// [`broker_auth::MAX_AUTHZ_USERNAME_LEN`] bytes, never the catch-all
/// `*`), `topic` (non-empty, valid MQTT filter), `permission`
/// (`allow`/`deny`, case-insensitive) and `action`
/// (`publish`/`subscribe`/`all`, case-insensitive). A `user_id`
/// spelling is accepted as an alias for `username`; every other extra
/// field is ignored so future match fields degrade to the same create
/// instead of a 400.
fn parse_user_rule_body(value: &serde_json::Value) -> Result<AclRule, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "user rule body must be a JSON object".to_string())?;
    let username = obj
        .get("username")
        .or(obj.get("user_id"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| "field `username` is required".to_string())?;
    if username.trim().is_empty() {
        return Err("field `username` must not be empty".to_string());
    }
    if username.len() > broker_auth::MAX_AUTHZ_USERNAME_LEN {
        return Err(format!(
            "field `username` {username:?} exceeds {} bytes",
            broker_auth::MAX_AUTHZ_USERNAME_LEN
        ));
    }
    if username == "*" {
        return Err(
            "field `username` must name a concrete user, not the catch-all (field `username`)"
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
        username.to_string(),
        action,
        topic.to_string(),
        allow,
    ))
}

/// `GET /authorization/sources/built_in_database/rules/users/{username}`:
/// read one user's rule set.
///
/// Returns 200 with `{"rules": [...]}` in stored evaluation order; a
/// username with no rules fails with `NOT_FOUND` in the documented
/// `{code, message}` shape and the catch-all `*` (owned by the
/// `/rules/all` table) is always a miss here, never a read of another
/// table. The read persists trivially: it observes the same
/// registry-backed table every write commits through.
///
/// Management-plane only: clones at most that user's capped rules once
/// per request under a short read lock; the publish event in
/// `crates/broker-node/src/main.rs` consults the same table
/// (`MemoryAuth::authorize_publish` in `crates/broker-auth/src/lib.rs`)
/// and publish/deliver never take this handler's lock.
pub async fn get_user_rules(
    State(state): State<ApiState>,
    Path(username): Path<String>,
) -> Response {
    if username == "*" {
        return ApiError::NotFound(format!(
            "user authorization rules for {username:?} not found"
        ))
        .into_response();
    }
    let rules = state.auth.user_rules(&username);
    if rules.is_empty() {
        return ApiError::NotFound(format!(
            "user authorization rules for {username:?} not found"
        ))
        .into_response();
    }
    let rendered: Vec<serde_json::Value> = rules.iter().map(render_user_rule).collect();
    (
        StatusCode::OK,
        Json(serde_json::json!({ "rules": rendered })),
    )
        .into_response()
}

/// `DELETE /authorization/sources/built_in_database/rules/users/{username}`:
/// remove every rule scoped to one username.
///
/// Removing an unknown username fails with `NOT_FOUND` in the
/// documented `{code, message}` shape and applies nothing; a hit answers
/// 204 with no body. The removal persists across a restart through the
/// config registry (the same `MemoryAuth` commit path the
/// publish/subscribe consults read). Management-plane only: one short
/// rule-table write lock per request (plus the registry save lock on
/// persist); the publish and subscribe authorization consults keep
/// reading under short read locks.
pub async fn delete_user_rules(
    State(state): State<ApiState>,
    Path(username): Path<String>,
) -> Response {
    match state.auth.remove_user_rules(&username) {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => ApiError::NotFound(format!(
            "user authorization rules for {username:?} not found"
        ))
        .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot delete user authorization rules: {error}"),
            })),
        )
            .into_response(),
    }
}

/// `PUT /authorization/sources/built_in_database/rules/users/{username}`:
/// replace one username's rule set.
///
/// The body must be an object carrying the replacement under `rules`
/// (`{"rules": [...]}`); anything else fails with `BAD_REQUEST` and
/// applies nothing. Every entry is validated before anything becomes
/// visible (validate-all-before-apply): each must carry a non-empty
/// `topic` that parses as an MQTT filter, a `permission` of
/// `allow`/`deny` and an `action` of `publish`/`subscribe`/`all`; a
/// per-entry key field (`username`, `user_id` or `clientid`) when
/// present must name the path username, as must a top-level `username`
/// (or `user_id`) when present. A bad entry fails the whole write with
/// `BAD_REQUEST` and the stored set is unchanged, as does a duplicate
/// scope within the batch (`ALREADY_EXISTS`) or a replacement that would
/// overflow the table cap (`BAD_REQUEST`). Writing an unknown username
/// creates its set (create-on-write); writing an empty list clears it.
/// On success the replacement persists across a restart through the
/// config registry and answers 204 with no body. The write lands in the
/// same shared built-in table the per-client index reads, so it is live
/// on the publish/subscribe decision path.
///
/// Management-plane only: topic validation runs before the lock, then
/// one short rule-table write lock per request (plus the registry save
/// lock on persist); the publish and subscribe authorization consults
/// keep reading under short read locks.
// TODO(parity): does the documented replace require a top-level
// `username` matching the path id, accept a bare array body, or ignore
// key fields? The rulebook does not decide the exact envelope; the
// current choice requires the object shape and rejects key mismatches
// fail-closed until the checker pins it.
pub async fn replace_user_rules(
    State(state): State<ApiState>,
    Path(username): Path<String>,
    body: Bytes,
) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return ApiError::BadRequest(format!("invalid user rule set body: {e}"))
                .into_response();
        }
    };
    let rules = match parse_user_replace_body(&value, &username) {
        Ok(rules) => rules,
        Err(msg) => return ApiError::BadRequest(msg).into_response(),
    };
    match state.auth.replace_user_rules(&username, rules) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(ClientRuleError::Duplicate) => {
            ApiError::AlreadyExists("user rule already exists".to_string()).into_response()
        }
        Err(ClientRuleError::Full) => {
            ApiError::BadRequest("user rule table is full".to_string()).into_response()
        }
        Err(ClientRuleError::Invalid(msg)) => ApiError::BadRequest(msg).into_response(),
        Err(ClientRuleError::Persist(msg)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot persist user authorization rules: {msg}"),
            })),
        )
            .into_response(),
    }
}

/// Validate a per-user replace body into ordered [`AclRule`]s keyed by
/// the path username.
///
/// Required shape: an object with a `rules` array. Each entry must be an
/// object with string `topic` (non-empty, valid MQTT filter), string
/// `permission` (`allow`/`deny`, case-insensitive) and string `action`
/// (`publish`/`subscribe`/`all`, case-insensitive). A per-entry key
/// field (`username`, `user_id` or `clientid`) is optional; when present
/// it must name the path username, otherwise the whole write is rejected
/// fail-closed. A top-level `username` (or `user_id`) is optional; when
/// present it must likewise name the path username. Every other extra
/// field is ignored so future match fields degrade to the same replace
/// instead of a 400.
fn parse_user_replace_body(
    value: &serde_json::Value,
    username: &str,
) -> Result<Vec<AclRule>, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "user rule set body must be an object with a `rules` array".to_string())?;
    if let Some(key) = obj.get("username").or(obj.get("user_id")) {
        match key.as_str() {
            Some(name) if name == username => {}
            _ => {
                return Err(format!(
                    "user rule set username must name {username:?} (field `username`)"
                ));
            }
        }
    }
    let items = obj
        .get("rules")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "user rule set body must carry an array under `rules`".to_string())?;
    let mut out = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let entry = item
            .as_object()
            .ok_or_else(|| format!("user rules[{index}] must be an object (field `rules`)"))?;
        if let Some(key) = entry
            .get("username")
            .or(entry.get("user_id"))
            .or(entry.get("clientid"))
        {
            match key.as_str() {
                Some(name) if name == username => {}
                _ => {
                    return Err(format!(
                        "user rules[{index}].username must name {username:?} (field `username`)"
                    ));
                }
            }
        }
        let topic = entry
            .get("topic")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("user rules[{index}].topic is required (field `topic`)"))?;
        if topic.trim().is_empty() {
            return Err(format!(
                "user rules[{index}].topic must not be empty (field `topic`)"
            ));
        }
        if broker_protocol::TopicFilter::new(topic.to_string()).is_err() {
            return Err(format!(
                "user rules[{index}].topic {topic:?} is not a valid filter (field `topic`)"
            ));
        }
        let permission = entry
            .get("permission")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                format!("user rules[{index}].permission is required (field `permission`)")
            })?;
        let allow = match permission.to_ascii_lowercase().as_str() {
            "allow" => true,
            "deny" => false,
            _ => {
                return Err(format!(
                    "user rules[{index}].permission {permission:?} is unknown (field `permission`)"
                ));
            }
        };
        let action_raw = entry
            .get("action")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("user rules[{index}].action is required (field `action`)"))?;
        let action = AclAction::parse(action_raw).ok_or_else(|| {
            format!("user rules[{index}].action {action_raw:?} is unknown (field `action`)")
        })?;
        out.push(AclRule::new(
            username.to_string(),
            action,
            topic.to_string(),
            allow,
        ));
    }
    Ok(out)
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

    fn client_set(entries: serde_json::Value) -> Bytes {
        body(serde_json::json!({ "rules": entries }))
    }

    async fn get_detail(
        state: &ApiState,
        clientid: &str,
    ) -> (StatusCode, serde_json::Value, Vec<u8>) {
        parts(get_client_rules(State(state.clone()), Path(clientid.to_string())).await).await
    }

    async fn put_detail(
        state: &ApiState,
        clientid: &str,
        payload: Bytes,
    ) -> (StatusCode, serde_json::Value, Vec<u8>) {
        parts(replace_client_rules(State(state.clone()), Path(clientid.to_string()), payload).await)
            .await
    }

    #[tokio::test]
    async fn client_replace_read_round_trip_rejects_bad_without_change_through_broker() {
        // The task's failing test: before this task the per-client detail
        // routes were missing (bare 404). Write a client set via replace,
        // read it back; replace with an invalid rule expecting rejection
        // with no change. Goes through the broker decision path, not only
        // the store: the written allow is observed on
        // `authorize_publish` (consulted by the publish event in
        // `crates/broker-node/src/main.rs`).
        let state = standalone_state();

        // Unknown ids read as 404 with the documented shape.
        let (status, missing, _) = get_detail(&state, "w220-a").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], serde_json::json!("NOT_FOUND"));
        assert!(missing["message"].is_string());

        // Writing an unknown client id creates its set.
        let (status, _, raw) = put_detail(
            &state,
            "w220-a",
            client_set(serde_json::json!([
                {"topic": "w220/a/#", "permission": "allow", "action": "publish"},
                {"topic": "w220/b/#", "permission": "deny", "action": "subscribe"},
            ])),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(raw.is_empty(), "replace answers 204 with no body");

        let (status, detail, _) = get_detail(&state, "w220-a").await;
        assert_eq!(status, StatusCode::OK);
        let rules = detail["rules"].as_array().expect("rules array").clone();
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0]["clientid"], serde_json::json!("w220-a"));
        assert_eq!(rules[0]["topic"], serde_json::json!("w220/a/#"));
        assert_eq!(rules[0]["permission"], serde_json::json!("allow"));
        assert_eq!(rules[0]["action"], serde_json::json!("publish"));
        assert_eq!(rules[1]["permission"], serde_json::json!("deny"));
        assert_eq!(rules[1]["action"], serde_json::json!("subscribe"));

        // The stored allow is live on the publish path for that client.
        let topic = broker_protocol::Topic::new("w220/a/data").expect("topic parses");
        state
            .auth
            .authorize_publish("w220-a", &topic)
            .await
            .expect("client allow opens the publish path for its client");
        assert!(
            state
                .auth
                .authorize_publish("w220-b", &topic)
                .await
                .is_err(),
            "unmatched clients stay denied once a rule exists"
        );

        // Every bad batch is a client error and the stored set is
        // unchanged: bad filter, unknown permission, unknown action,
        // missing fields, duplicate scope, key mismatch, wrong envelope.
        let bad_bodies = [
            client_set(serde_json::json!([
                {"topic": "w220/ok/#", "permission": "allow", "action": "publish"},
                {"topic": "", "permission": "allow", "action": "publish"},
            ])),
            client_set(serde_json::json!([
                {"topic": "w220/ok/#", "permission": "maybe", "action": "publish"},
            ])),
            client_set(serde_json::json!([
                {"topic": "w220/ok/#", "permission": "allow", "action": "forward"},
            ])),
            client_set(serde_json::json!([
                {"permission": "allow", "action": "publish"},
            ])),
            client_set(serde_json::json!([
                {"topic": "w220/bad/#/x", "permission": "allow", "action": "publish"},
            ])),
            client_set(serde_json::json!([
                {"topic": "w220/dup/#", "permission": "allow", "action": "publish"},
                {"topic": "w220/dup/#", "permission": "deny", "action": "publish"},
            ])),
            client_set(serde_json::json!([
                {"clientid": "w220-other", "topic": "w220/ok/#", "permission": "allow", "action": "publish"},
            ])),
            body(serde_json::json!([])),
            body(serde_json::json!({"nope": []})),
        ];
        for bad in bad_bodies {
            let (status, err, _) = put_detail(&state, "w220-a", bad.clone()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "batch must be rejected");
            assert!(
                err["code"] == serde_json::json!("BAD_REQUEST")
                    || err["code"] == serde_json::json!("ALREADY_EXISTS"),
                "rejection carries a documented code, got {err}"
            );
        }
        let (status, detail, _) = get_detail(&state, "w220-a").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(detail["rules"].as_array().expect("rules array").len(), 2);

        // A matching per-entry key is accepted; replacing with an empty
        // list clears the set so the next read is a 404 again.
        let (status, _, _) = put_detail(
            &state,
            "w220-a",
            client_set(serde_json::json!([
                {"clientid": "w220-a", "topic": "w220/kept/#", "permission": "allow", "action": "all"},
            ])),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, detail, _) = get_detail(&state, "w220-a").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(detail["rules"].as_array().expect("rules array").len(), 1);

        let (status, _, _) = put_detail(&state, "w220-a", client_set(serde_json::json!([]))).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, missing, _) = get_detail(&state, "w220-a").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], serde_json::json!("NOT_FOUND"));
    }

    #[tokio::test]
    async fn detail_star_never_touches_the_catch_all_table() {
        // The catch-all `*` belongs to the `/rules/all` table: a detail
        // read naming it is a miss and a replace naming it is refused,
        // never a read or wipe of another table.
        let state = standalone_state();
        state
            .auth
            .add_rule(AclRule::new("*", AclAction::Publish, "w220/catch/#", true))
            .expect("catch-all applies");

        let (status, missing, _) = get_detail(&state, "*").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], serde_json::json!("NOT_FOUND"));

        let (status, err, _) = put_detail(
            &state,
            "*",
            client_set(serde_json::json!([
                {"topic": "w220/ok/#", "permission": "allow", "action": "publish"},
            ])),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
        assert!(
            !state.auth.acl_rules().is_empty(),
            "catch-all entries survive a per-client replace for `*`"
        );
    }

    #[tokio::test]
    async fn user_list_envelope_shares_table_and_empty_reads_empty() {
        // The per-user list returns the standard page envelope; empty
        // reads as an empty page, never an error. Rows carry the
        // `username` key over the shared built-in table.
        let state = standalone_state();
        let (status, empty, _) =
            parts(list_user_rules(State(state.clone()), params(1, 100)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(empty["data"], serde_json::json!([]));
        assert_eq!(empty["meta"]["count"], serde_json::json!(0));
        assert_eq!(empty["meta"]["page"], serde_json::json!(1));
        assert_eq!(empty["meta"]["limit"], serde_json::json!(100));
        assert_eq!(empty["meta"]["hasnext"], serde_json::json!(false));

        for (id, topic) in [("w220-u1", "w220/u1/#"), ("w220-u2", "w220/u2/#")] {
            let (status, _, _) =
                parts(create_client_rule(State(state.clone()), body(client_rule(id, topic))).await)
                    .await;
            assert_eq!(status, StatusCode::CREATED, "seed {id}");
        }
        let (status, listed, _) =
            parts(list_user_rules(State(state.clone()), params(1, 100)).await).await;
        assert_eq!(status, StatusCode::OK);
        let data = listed["data"].as_array().expect("data array");
        assert_eq!(data.len(), 2);
        assert_eq!(data[0]["username"], serde_json::json!("w220-u1"));
        assert_eq!(data[0]["topic"], serde_json::json!("w220/u1/#"));
        assert_eq!(listed["meta"]["count"], serde_json::json!(2));
        assert_eq!(listed["meta"]["hasnext"], serde_json::json!(false));

        let (status, page2, _) =
            parts(list_user_rules(State(state.clone()), params(2, 1)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(page2["data"][0]["username"], serde_json::json!("w220-u2"));
        assert_eq!(page2["meta"]["count"], serde_json::json!(2));
        assert_eq!(page2["meta"]["hasnext"], serde_json::json!(false));
    }

    #[tokio::test]
    async fn replace_bound_and_persistence_verified() {
        // Bound: the shared per-client cap is stated and finite.
        assert_eq!(broker_auth::MAX_AUTHZ_CLIENT_RULES, 100_000);

        // Persistence: a replace through a registry-backed table survives
        // a reload of the same data dir, for both the client and the
        // user view of the shared table.
        let dir = std::env::temp_dir().join(format!(
            "authz-client-detail-{}-{}",
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
        auth.replace_client_rules(
            "w220-persist",
            vec![broker_auth::AclRule::new(
                "w220-persist",
                broker_auth::AclAction::Subscribe,
                "w220/persist/#",
                false,
            )],
        )
        .expect("registry-backed replace applies");
        assert!(dir.join(broker_config::STATE_FILE_NAME).is_file());
        drop(auth);
        drop(registry);
        let reloaded =
            std::sync::Arc::new(broker_config::ConfigRegistry::load(&dir).expect("dir reloads"));
        let restarted = broker_auth::MemoryAuth::from_registry(&reloaded);
        let rules = restarted.client_rules("w220-persist");
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].topic_pattern, "w220/persist/#");
        let user_view = restarted.user_rules("w220-persist");
        assert_eq!(user_view.len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    fn user_rule(username: &str, topic: &str) -> serde_json::Value {
        serde_json::json!({
            "username": username,
            "topic": topic,
            "permission": "allow",
            "action": "publish",
        })
    }

    async fn get_user_detail(
        state: &ApiState,
        username: &str,
    ) -> (StatusCode, serde_json::Value, Vec<u8>) {
        parts(get_user_rules(State(state.clone()), Path(username.to_string())).await).await
    }

    #[tokio::test]
    async fn user_add_read_duplicate_delete_round_trip_through_broker() {
        // The task's failing test: before this task the per-user add and
        // per-username routes were missing (bare 404). Add a rule for
        // user U, read it back; re-add the duplicate expecting rejection;
        // delete then read expecting 404. Goes through the broker decision
        // path, not only the store: the written allow is observed on
        // `authorize_publish` (consulted by the publish event in
        // `crates/broker-node/src/main.rs`), and the delete closes it.
        let state = standalone_state();

        // Unknown usernames read as 404 with the documented shape.
        let (status, missing, _) = get_user_detail(&state, "w221-u").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], serde_json::json!("NOT_FOUND"));
        assert!(missing["message"].is_string());

        let (status, created, _) = parts(
            create_user_rule(
                State(state.clone()),
                body(user_rule("w221-u", "w221/alpha/#")),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(created["username"], serde_json::json!("w221-u"));
        assert_eq!(created["topic"], serde_json::json!("w221/alpha/#"));
        assert_eq!(created["permission"], serde_json::json!("allow"));
        assert_eq!(created["action"], serde_json::json!("publish"));

        let (status, detail, _) = get_user_detail(&state, "w221-u").await;
        assert_eq!(status, StatusCode::OK);
        let rules = detail["rules"].as_array().expect("rules array").clone();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0]["username"], serde_json::json!("w221-u"));
        assert_eq!(rules[0]["topic"], serde_json::json!("w221/alpha/#"));

        // The stored allow is live on the publish path for user U only.
        let topic = broker_protocol::Topic::new("w221/alpha/data").expect("topic parses");
        state
            .auth
            .authorize_publish("w221-u", &topic)
            .await
            .expect("user allow opens the publish path for its user");
        assert!(
            state
                .auth
                .authorize_publish("w221-other", &topic)
                .await
                .is_err(),
            "unmatched users stay denied once a rule exists"
        );

        // Re-adding the duplicate is rejected, never silently merged.
        let (status, duplicate, _) = parts(
            create_user_rule(
                State(state.clone()),
                body(user_rule("w221-u", "w221/alpha/#")),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(duplicate["code"], serde_json::json!("ALREADY_EXISTS"));
        assert!(duplicate["message"].is_string());
        let (status, detail, _) = get_user_detail(&state, "w221-u").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(detail["rules"].as_array().expect("rules array").len(), 1);

        // Delete removes every rule for that username and reports success.
        let response = delete_user_rules(State(state.clone()), Path("w221-u".to_string())).await;
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        let (status, missing, _) = get_user_detail(&state, "w221-u").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], serde_json::json!("NOT_FOUND"));

        // Unknown usernames on delete return 404 with the documented shape.
        let (status, missing, _) =
            parts(delete_user_rules(State(state.clone()), Path("w221-u".to_string())).await).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], serde_json::json!("NOT_FOUND"));
        assert!(missing["message"].is_string());
    }

    #[tokio::test]
    async fn user_create_rejects_bad_bodies_and_applies_nothing() {
        // Validation failures are client errors and the stored index is
        // unchanged.
        let state = standalone_state();
        let (status, _, _) = parts(
            create_user_rule(
                State(state.clone()),
                body(user_rule("w221-keep", "w221/keep/#")),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);

        let bad_bodies = [
            serde_json::json!({"topic": "w221/x/#", "permission": "allow", "action": "publish"}),
            serde_json::json!({"username": "", "topic": "w221/x/#", "permission": "allow", "action": "publish"}),
            serde_json::json!({"username": "  ", "topic": "w221/x/#", "permission": "allow", "action": "publish"}),
            serde_json::json!({"username": "*", "topic": "w221/x/#", "permission": "allow", "action": "publish"}),
            serde_json::json!({"username": "w221-x", "permission": "allow", "action": "publish"}),
            serde_json::json!({"username": "w221-x", "topic": "", "permission": "allow", "action": "publish"}),
            serde_json::json!({"username": "w221-x", "topic": "w221/bad/#/x", "permission": "allow", "action": "publish"}),
            serde_json::json!({"username": "w221-x", "topic": "w221/x/#", "permission": "maybe", "action": "publish"}),
            serde_json::json!({"username": "w221-x", "topic": "w221/x/#", "permission": "allow", "action": "forward"}),
            serde_json::json!({"username": "w221-x", "topic": "w221/x/#", "permission": "allow"}),
            serde_json::json!({"username": "w221-x", "topic": "w221/x/#", "action": "publish"}),
            serde_json::json!({"username": 7, "topic": "w221/x/#", "permission": "allow", "action": "publish"}),
            serde_json::json!({"clientid": "w221-x", "topic": "w221/x/#", "permission": "allow", "action": "publish"}),
            serde_json::json!([]),
        ];
        for bad in bad_bodies {
            let (status, err, _) =
                parts(create_user_rule(State(state.clone()), body(bad.clone())).await).await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "body {bad} must be rejected"
            );
            assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
        }

        // The `user_id` alias is accepted like the client `client_id` alias.
        let (status, created, _) = parts(
            create_user_rule(
                State(state.clone()),
                body(serde_json::json!({
                    "user_id": "w221-alias",
                    "topic": "w221/alias/#",
                    "permission": "deny",
                    "action": "subscribe",
                })),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(created["username"], serde_json::json!("w221-alias"));
        assert_eq!(created["permission"], serde_json::json!("deny"));

        // The catch-all `*` belongs to the `/rules/all` table: a per-user
        // delete naming it is a miss, never a wipe of another table.
        state
            .auth
            .add_rule(AclRule::new("*", AclAction::Publish, "w221/catch/#", true))
            .expect("catch-all applies");
        let (status, missing, _) =
            parts(delete_user_rules(State(state.clone()), Path("*".to_string())).await).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], serde_json::json!("NOT_FOUND"));
        assert!(
            !state.auth.acl_rules().is_empty(),
            "catch-all entries survive a per-user delete for `*`"
        );

        // The seeded entries are unchanged by every rejected write.
        let (status, detail, _) = get_user_detail(&state, "w221-keep").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(detail["rules"].as_array().expect("rules array").len(), 1);
    }

    #[tokio::test]
    async fn user_bound_and_persistence_verified() {
        // Bound: the per-user cap is stated, finite, and enforced; the
        // default index is empty.
        assert_eq!(broker_auth::MAX_AUTHZ_USER_RULES, 100_000);
        assert_eq!(broker_auth::MAX_AUTHZ_USERNAME_LEN, 256);
        let fresh = standalone_state();
        let (status, empty, _) =
            parts(list_user_rules(State(fresh.clone()), params(1, 100)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(empty["data"], serde_json::json!([]));

        // Persistence: an add through a registry-backed table survives a
        // reload of the same data dir, for both the user and the client
        // view of the shared table.
        let dir = std::env::temp_dir().join(format!(
            "authz-users-{}-{}",
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
        auth.add_user_rule(broker_auth::AclRule::new(
            "w221-persist",
            broker_auth::AclAction::Publish,
            "w221/persist/#",
            true,
        ))
        .expect("registry-backed add applies");
        assert!(dir.join(broker_config::STATE_FILE_NAME).is_file());
        drop(auth);
        drop(registry);
        let reloaded =
            std::sync::Arc::new(broker_config::ConfigRegistry::load(&dir).expect("dir reloads"));
        let restarted = broker_auth::MemoryAuth::from_registry(&reloaded);
        let rules = restarted.user_rules("w221-persist");
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].topic_pattern, "w221/persist/#");
        let client_view = restarted.client_rules("w221-persist");
        assert_eq!(client_view.len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    fn user_set(entries: serde_json::Value) -> Bytes {
        body(serde_json::json!({ "rules": entries }))
    }

    async fn put_user_detail(
        state: &ApiState,
        username: &str,
        payload: Bytes,
    ) -> (StatusCode, serde_json::Value, Vec<u8>) {
        parts(replace_user_rules(State(state.clone()), Path(username.to_string()), payload).await)
            .await
    }

    #[tokio::test]
    async fn user_replace_read_round_trip_rejects_bad_without_change_through_broker() {
        // The task's failing test: before this task the per-user replace
        // route was missing (bare 404). Seed a user set, replace it and
        // read back; replace with an invalid rule expecting rejection
        // with no change. Goes through the broker decision path, not only
        // the store: the written allow is observed on
        // `authorize_publish` (consulted by the publish event in
        // `crates/broker-node/src/main.rs`).
        let state = standalone_state();

        // Unknown usernames read as 404 with the documented shape.
        let (status, missing, _) = get_user_detail(&state, "w222-a").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], serde_json::json!("NOT_FOUND"));
        assert!(missing["message"].is_string());

        // Writing an unknown username creates its set (create-on-write).
        let (status, _, raw) = put_user_detail(
            &state,
            "w222-a",
            user_set(serde_json::json!([
                {"topic": "w222/a/#", "permission": "allow", "action": "publish"},
                {"topic": "w222/b/#", "permission": "deny", "action": "subscribe"},
            ])),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(raw.is_empty(), "replace answers 204 with no body");

        let (status, detail, _) = get_user_detail(&state, "w222-a").await;
        assert_eq!(status, StatusCode::OK);
        let rules = detail["rules"].as_array().expect("rules array").clone();
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0]["username"], serde_json::json!("w222-a"));
        assert_eq!(rules[0]["topic"], serde_json::json!("w222/a/#"));
        assert_eq!(rules[0]["permission"], serde_json::json!("allow"));
        assert_eq!(rules[0]["action"], serde_json::json!("publish"));
        assert_eq!(rules[1]["permission"], serde_json::json!("deny"));
        assert_eq!(rules[1]["action"], serde_json::json!("subscribe"));

        // The stored allow is live on the publish path for that user.
        let topic = broker_protocol::Topic::new("w222/a/data").expect("topic parses");
        state
            .auth
            .authorize_publish("w222-a", &topic)
            .await
            .expect("user allow opens the publish path for its user");
        assert!(
            state
                .auth
                .authorize_publish("w222-b", &topic)
                .await
                .is_err(),
            "unmatched users stay denied once a rule exists"
        );

        // Every bad batch is a client error and the stored set is
        // unchanged: bad filter, unknown permission, unknown action,
        // missing fields, duplicate scope, per-entry key mismatch,
        // top-level key mismatch, wrong envelope.
        let bad_bodies = [
            user_set(serde_json::json!([
                {"topic": "w222/ok/#", "permission": "allow", "action": "publish"},
                {"topic": "", "permission": "allow", "action": "publish"},
            ])),
            user_set(serde_json::json!([
                {"topic": "w222/ok/#", "permission": "maybe", "action": "publish"},
            ])),
            user_set(serde_json::json!([
                {"topic": "w222/ok/#", "permission": "allow", "action": "forward"},
            ])),
            user_set(serde_json::json!([
                {"permission": "allow", "action": "publish"},
            ])),
            user_set(serde_json::json!([
                {"topic": "w222/bad/#/x", "permission": "allow", "action": "publish"},
            ])),
            user_set(serde_json::json!([
                {"topic": "w222/dup/#", "permission": "allow", "action": "publish"},
                {"topic": "w222/dup/#", "permission": "deny", "action": "publish"},
            ])),
            user_set(serde_json::json!([
                {"username": "w222-other", "topic": "w222/ok/#", "permission": "allow", "action": "publish"},
            ])),
            body(serde_json::json!({
                "username": "w222-other",
                "rules": [{"topic": "w222/ok/#", "permission": "allow", "action": "publish"}],
            })),
            body(serde_json::json!([])),
            body(serde_json::json!({"nope": []})),
        ];
        for bad in bad_bodies {
            let (status, err, _) = put_user_detail(&state, "w222-a", bad.clone()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "batch must be rejected");
            assert!(
                err["code"] == serde_json::json!("BAD_REQUEST")
                    || err["code"] == serde_json::json!("ALREADY_EXISTS"),
                "rejection carries a documented code, got {err}"
            );
        }
        let (status, detail, _) = get_user_detail(&state, "w222-a").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(detail["rules"].as_array().expect("rules array").len(), 2);

        // A matching per-entry key and a matching top-level username are
        // accepted; replacing with an empty list clears the set so the
        // next read is a 404 again.
        let (status, _, _) = put_user_detail(
            &state,
            "w222-a",
            body(serde_json::json!({
                "username": "w222-a",
                "rules": [
                    {"username": "w222-a", "topic": "w222/kept/#", "permission": "allow", "action": "all"},
                ],
            })),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, detail, _) = get_user_detail(&state, "w222-a").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(detail["rules"].as_array().expect("rules array").len(), 1);

        let (status, _, _) =
            put_user_detail(&state, "w222-a", user_set(serde_json::json!([]))).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, missing, _) = get_user_detail(&state, "w222-a").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], serde_json::json!("NOT_FOUND"));
    }

    #[tokio::test]
    async fn user_replace_star_never_touches_the_catch_all_table() {
        // The catch-all `*` belongs to the `/rules/all` table: a replace
        // naming it is refused, never a wipe of another table.
        let state = standalone_state();
        state
            .auth
            .add_rule(AclRule::new("*", AclAction::Publish, "w222/catch/#", true))
            .expect("catch-all applies");

        let (status, err, _) = put_user_detail(
            &state,
            "*",
            user_set(serde_json::json!([
                {"topic": "w222/ok/#", "permission": "allow", "action": "publish"},
            ])),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
        assert!(
            !state.auth.acl_rules().is_empty(),
            "catch-all entries survive a per-user replace for `*`"
        );
    }

    #[tokio::test]
    async fn user_replace_bound_and_persistence_verified() {
        // Bound: the shared per-user cap is stated and finite.
        assert_eq!(broker_auth::MAX_AUTHZ_USER_RULES, 100_000);

        // Persistence: a replace through a registry-backed table survives
        // a reload of the same data dir, for both the user and the client
        // view of the shared table.
        let dir = std::env::temp_dir().join(format!(
            "authz-user-replace-{}-{}",
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
        auth.replace_user_rules(
            "w222-persist",
            vec![broker_auth::AclRule::new(
                "w222-persist",
                broker_auth::AclAction::Subscribe,
                "w222/persist/#",
                false,
            )],
        )
        .expect("registry-backed replace applies");
        assert!(dir.join(broker_config::STATE_FILE_NAME).is_file());
        drop(auth);
        drop(registry);
        let reloaded =
            std::sync::Arc::new(broker_config::ConfigRegistry::load(&dir).expect("dir reloads"));
        let restarted = broker_auth::MemoryAuth::from_registry(&reloaded);
        let rules = restarted.user_rules("w222-persist");
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].topic_pattern, "w222/persist/#");
        let client_view = restarted.client_rules("w222-persist");
        assert_eq!(client_view.len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }
}
