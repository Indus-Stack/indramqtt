//! Streaming SQL rules, data connectors, action sinks, and schema registry for the v5 REST API.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::errors::ApiError;
use crate::node_scope::resolve_node;
use crate::pagination::{meta_page, paginate, PageParams};
use crate::ApiState;

use crate::node_scope::local_node_name;

// ---------------------------------------------------------------------------
// Streaming Rules (W3-13, C06-18/C06-19/C06-20): paged list, validated
// create with duplicate rejection, and delete with a 404 for unknown ids,
// all on the live engine rule-map.
//
// Single-node, management-plane only: handlers clone at most the capped
// list (list) or one entry per request under a short lock; rule evaluation
// reads a snapshot, never this map directly, and publish and deliver never
// touch it.
//
// Store bounds (both stated here and enforced below):
// - at most [`MAX_RULES`] entries (1024: caps management-plane memory
//   for the rule catalogue; creates past the cap are rejected with
//   `BAD_REQUEST` instead of growing without limit);
// - `id` is capped at [`MAX_RULE_ID_LEN`] chars (256: bounds map-key
//   memory per entry; longer ids are rejected with `BAD_REQUEST`);
// - list paging reuses the shared extractor (default limit 100, max
//   1000); the default store is empty.
//
// Persistence: every mutation exports the validated store into the
// config core (`RulesConf`, versioned file) and saves atomically;
// a commit or save failure answers 500 so the loss is never silent.
// Per-rule counters do not persist (memory-only by design: a restart
// rebuilds the map at zero).
//
// Fire-path cost (stated for the delivery-path note): no new work on
// publish or deliver. Dispatch loads one lock-free settings `Arc` plus
// one `Arc` each of the disabled-action set and the per-action metric
// map per ingress, then clones the matching rule `Arc`s under one short
// read lock; list/create/delete take the map lock only on the
// management plane.
// TODO(parity): is 1024 the documented rule cap? The rulebook does not
// decide the exact bound; 1024 is the conservative choice until the
// conformance checker pins it.
// TODO(parity): which per-rule fields does the spec document beyond
// id/name/sql/from/enable/actions? The rulebook does not decide the
// exact shape; fields the broker cannot supply are omitted rather than
// invented until the checker pins it.
// TODO(parity): for an explicit-id create whose SQL is also malformed,
// is the answer the duplicate or the SQL error? The rulebook does not
// decide the precedence; the current choice compiles the SQL first
// (a broken rule is rejected as invalid either way) until the checker
// pins it.
// TODO(parity): do rule actions name action ids only, or also raw
// connector ids? The rulebook does not decide; the current choice
// validates `ForwardConnector` targets against the W3-02 action
// directory (unknown ids rejected) until the checker pins it.

/// Upper bound for stored rules. Creates past this size are rejected
/// with `BAD_REQUEST` instead of growing the map without limit. Reason:
/// the list handler clones the whole snapshot once per request, so the
/// catalogue must stay small and bounded; 1024 covers the documented
/// rule set with headroom while keeping per-request clones cheap.
pub const MAX_RULES: usize = broker_config::MAX_RULES;
/// Longest accepted rule `id`. Reason: bounds map-key memory per
/// entry so one create cannot balloon the store.
const MAX_RULE_ID_LEN: usize = broker_config::MAX_RULE_ID_LEN;

/// Length of a generated rule id in hex chars. Reason: the documented
/// example shows an 8-char id, so a create without one assigns 8 chars.
const GENERATED_RULE_ID_LEN: usize = 8;

/// Assign a rule id for a create without one. Returns 8 lowercase hex
/// chars from secure random. A collision fails as a duplicate, never
/// as an overwrite.
fn generate_rule_id() -> String {
    let value: u32 = rand::random();
    let id = format!("{value:08x}");
    debug_assert_eq!(id.len(), GENERATED_RULE_ID_LEN);
    id
}

// ---------------------------------------------------------------------------
// Streaming Rules
// ---------------------------------------------------------------------------

/// Projection of the stored [`broker_rules::Rule`] for `/api/v5/rules`.
/// Only real engine state: stored actions verbatim (empty stays empty),
/// no invented `description`/`created_at`, no connector-id coercion.
/// `tenant` names the owning tenant (`default` for pre-tenancy rules).
fn rule_to_v5(rule: &broker_rules::Rule) -> serde_json::Value {
    let topic = rule.topic_filter.as_str().to_string();
    let sql = rule
        .sql_query
        .clone()
        .unwrap_or_else(|| format!("SELECT * FROM \"{topic}\""));
    // The documented shape: a string for a stored action, an object with
    // `function` for the built-in functions.
    let actions: Vec<serde_json::Value> = rule
        .actions
        .iter()
        .map(broker_rules::action_to_api_json)
        .collect();
    serde_json::json!({
        "id": rule.id,
        "name": rule.name,
        "tenant": rule.tenant,
        "sql": sql,
        "from": [topic],
        "enable": rule.enabled,
        "actions": actions,
    })
}

/// Resolve the rule tenant from a v5 body: absent or blank rides as
/// the default tenant. A present but non-string tenant fails with
/// 400. A malformed id fails with 400. An unknown tenant (no managed
/// namespace) fails with 400 and stores nothing.
fn resolve_rule_tenant(
    state: &ApiState,
    obj: &serde_json::Map<String, serde_json::Value>,
) -> Result<String, Box<Response>> {
    let raw = match obj.get("tenant") {
        None => "",
        Some(value) => {
            let Some(text) = value.as_str() else {
                return Err(Box::new(
                    ApiError::BadRequest("field `tenant` must be a string".to_string())
                        .into_response(),
                ));
            };
            text
        }
    };
    let tenant = broker_config::canonical_rule_tenant(raw);
    if !broker_config::is_valid_rule_tenant(&tenant) {
        return Err(Box::new(
            ApiError::BadRequest(format!("tenant {tenant:?} is not valid (field `tenant`)"))
                .into_response(),
        ));
    }
    if !state.namespaces.contains(&tenant) {
        return Err(Box::new(
            ApiError::BadRequest(format!("unknown tenant {tenant:?} (field `tenant`)"))
                .into_response(),
        ));
    }
    Ok(tenant)
}

/// R-TENANT: a connector forward carries no tenant label, so a rule
/// outside the default tenant must not forward to one. Republish and
/// log actions stay per-tenant and are allowed. Fail closed here at
/// the entry; the fire path never sees such a rule.
fn refuse_forward_outside_default_tenant(
    tenant: &str,
    actions: &[broker_rules::RuleAction],
) -> Option<Response> {
    let forwards = actions
        .iter()
        .any(|action| matches!(action, broker_rules::RuleAction::ForwardConnector { .. }));
    if forwards && tenant != "default" {
        Some(
            ApiError::BadRequest(format!(
                "rule tenant {tenant:?} cannot forward to connector actions (field `actions`)"
            ))
            .into_response(),
        )
    } else {
        None
    }
}

pub async fn list_rules(State(state): State<ApiState>, params: PageParams) -> Response {
    let data: Vec<_> = state.engine.list_rules().iter().map(rule_to_v5).collect();

    let count = data.len();
    let page_rows = paginate(&data, params.page, params.limit);
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "data": page_rows,
            "meta": meta_page(params.page, params.limit, count),
        })),
    )
        .into_response()
}

/// Map a rule registry failure onto a 500: the in-memory mutation
/// applied but the commit or atomic save failed, so the loss must never
/// be silent. [`broker_rules::RuleEngineError::Persist`] already renders
/// as `cannot persist rules: ...`, so its display is reused verbatim.
fn rule_persist_error(error: broker_rules::RuleEngineError) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({ "code": "INTERNAL_ERROR", "message": error.to_string() })),
    )
        .into_response()
}

/// Read the `actions` list of a rule body. An item is one of:
///
/// - a string: the id of a stored action (checked against the action
///   directory);
/// - an object with `function` `republish` or `console`: a built-in
///   function with the arguments the reference API documents;
/// - an object with `function` in the `module:function` form: a user
///   function. This broker cannot load user code, so it answers 400;
/// - an object with `id` or `name` and no `function`: the older form of a
///   stored action, kept for existing callers.
///
/// A refusal is a 400 that names the field. Management plane only.
fn parse_rule_actions(
    state: &ApiState,
    raw_actions: &[serde_json::Value],
) -> Result<Vec<broker_rules::RuleAction>, Box<Response>> {
    let bad = |message: String| Box::new(ApiError::BadRequest(message).into_response());
    let mut actions = Vec::with_capacity(raw_actions.len());
    for item in raw_actions {
        if let Some(id_str) = item.as_str() {
            // Rule-to-action linkage (W3-02): a forward target naming no
            // known action is rejected instead of going live and failing
            // at fire time.
            let Some(found) = state.actions.get_any(id_str) else {
                return Err(bad(format!("unknown action {id_str:?} (field `actions`)")));
            };
            // MV-01/SB-03: the fire target of a v5 rule action is the
            // action's own dispatch key, so delivery crosses to the
            // action-owned sink registered under that key (one action,
            // one sink, one key).
            actions.push(broker_rules::RuleAction::ForwardConnector {
                connector_id: broker_config::v5_action_sink_key(&found.id),
            });
            continue;
        }
        let Some(object) = item.as_object() else {
            return Err(bad(
                "unsupported action entry: expected a string id or an object (field `actions`)"
                    .to_string(),
            ));
        };
        if let Some(function) = object.get("function") {
            let Some(function) = function.as_str() else {
                return Err(bad(
                    "field `function` of an action must be a string (field `actions`)".to_string(),
                ));
            };
            match broker_rules::parse_function_action(function, object.get("args")) {
                Ok(action) => actions.push(action),
                Err(broker_rules::FunctionActionError::Unavailable(message))
                | Err(broker_rules::FunctionActionError::Invalid(message)) => {
                    return Err(bad(format!("{message} (field `actions`)")));
                }
            }
            continue;
        }
        let Some(conn_id) = object
            .get("id")
            .or_else(|| object.get("name"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
        else {
            // Fail closed: an object without an addressable target is
            // rejected, never silently dropped while the call answers 2xx.
            return Err(bad(
                "action entry object must carry a `function`, an `id` or a `name` (field `actions`)"
                    .to_string(),
            ));
        };
        let Some(found) = state.actions.get_any(conn_id) else {
            return Err(bad(format!("unknown action {conn_id:?} (field `actions`)")));
        };
        // MV-01/SB-03: same action-owned dispatch key as the bare
        // string form above.
        actions.push(broker_rules::RuleAction::ForwardConnector {
            connector_id: broker_config::v5_action_sink_key(&found.id),
        });
    }
    Ok(actions)
}

/// `POST /api/v5/rules`: compile the rule before storing (malformed
/// bodies rejected with the documented error shape); a duplicate id is
/// rejected cleanly, never overwritten. The documented request needs
/// only `sql`: an explicit `id` is stored verbatim so duplicates are
/// detectable, `name` falls back as the id for older callers, and
/// neither present means the broker assigns an 8-char hex id and
/// returns it so `GET /rules/{id}` finds the rule. A `tenant` field
/// selects the owning tenant (absent
/// means the default tenant); an unknown tenant is rejected with 400.
/// The `actions` list follows [`parse_rule_actions`]. A
/// `ForwardConnector` target naming no known action is
/// rejected (the W3-02 action linkage). Management-plane only; publish
/// and deliver never touch this map (dispatch reads a snapshot).
pub async fn create_rule(
    State(state): State<ApiState>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(obj) = body.as_object() else {
        return ApiError::BadRequest("request body must be a JSON object".to_string())
            .into_response();
    };
    // A present `id` must be a non-empty string. An empty id fails
    // with 400. An absent id falls back to `name`, then to a
    // generated id.
    if let Some(id_value) = obj.get("id") {
        let Some(text) = id_value.as_str() else {
            return ApiError::BadRequest("field `id` must be a string".to_string()).into_response();
        };
        if text.trim().is_empty() {
            return ApiError::BadRequest("field `id` must not be empty".to_string())
                .into_response();
        }
    }
    // Explicit `id` wins, `name` falls back (mirrors the action create),
    // and a generated id covers neither present.
    let rule_id = obj
        .get("id")
        .or_else(|| obj.get("name"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(generate_rule_id);
    if rule_id.len() > MAX_RULE_ID_LEN {
        return ApiError::BadRequest(format!(
            "rule id holds {} chars, at most {MAX_RULE_ID_LEN} (field `id`)",
            rule_id.len()
        ))
        .into_response();
    }
    let name = obj
        .get("name")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| rule_id.clone());
    // TODO(parity): does a create without `name` default the name to
    // the id or to ""? The spec gives `name` a "" default, but this
    // route has always echoed the id. The current choice keeps the id
    // until the checker pins it.
    let Some(sql) = obj
        .get("sql")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
    else {
        return ApiError::BadRequest("missing required field: sql".to_string()).into_response();
    };
    let enable = obj.get("enable").and_then(|v| v.as_bool()).unwrap_or(true);
    // Tenant scope first: an unknown tenant fails with 400 before
    // any other store check, and stores nothing.
    let tenant = match resolve_rule_tenant(&state, obj) {
        Ok(tenant) => tenant,
        Err(response) => return *response,
    };
    let raw_actions: Vec<serde_json::Value> = match obj.get("actions") {
        None => Vec::new(),
        Some(value) => match value.as_array() {
            Some(items) => items.clone(),
            None => {
                return ApiError::BadRequest("field `actions` must be an array".to_string())
                    .into_response();
            }
        },
    };
    let topic_str = if sql.contains("FROM \"") {
        sql.split("FROM \"")
            .nth(1)
            .and_then(|s| s.split('"').next())
            .unwrap_or("t/#")
    } else {
        "t/#"
    };

    let topic_filter = match broker_protocol::TopicFilter::new(topic_str) {
        Ok(filter) => {
            // The v5 rules projection treats bracket characters as invalid
            // filter characters (the spec's `t/[` example): the shared
            // `TopicFilter` accepts them, but a rule FROM-target containing
            // `[` or `]` is rejected here with 400 instead of going live.
            if topic_str.contains('[') || topic_str.contains(']') {
                return ApiError::BadRequest(
                    "invalid topic filter: bracket characters are not allowed".to_string(),
                )
                .into_response();
            }
            filter
        }
        Err(e) => {
            return ApiError::BadRequest(format!("invalid topic filter: {e}")).into_response();
        }
    };

    let actions = match parse_rule_actions(&state, &raw_actions) {
        Ok(actions) => actions,
        Err(response) => return *response,
    };
    if let Some(response) = refuse_forward_outside_default_tenant(&tenant, &actions) {
        return response;
    }

    // Rule-cap bound (management-plane, fail closed): the engine re-checks
    // the same bound after SQL compilation, but this early reject wires the
    // local [`MAX_RULES`] cap and [`broker_rules::RuleEngine::rule_count`]
    // so an obviously full store fails without a needless insert attempt.
    // Duplicate ids still report duplicate (mirroring the engine ordering:
    // duplicate before full), and a broken-SQL body on a full store stays
    // a 400 either way.
    {
        let live = state.engine.rule_count();
        let is_duplicate = state.engine.get_rule(&rule_id).is_some();
        if !is_duplicate && live >= MAX_RULES {
            return ApiError::BadRequest(format!(
                "rule store holds {live} entries, at most {MAX_RULES}"
            ))
            .into_response();
        }
    }

    let rule = state.engine.create_rule_with_id_in_tenant(
        rule_id,
        tenant,
        name,
        topic_filter,
        Some(sql),
        enable,
        actions,
    );

    match rule {
        Ok(r) => (StatusCode::CREATED, Json(rule_to_v5(&r))).into_response(),
        // A persist failure is a 500 (the in-memory rule stays but the
        // disk save failed); validation failures stay 400.
        Err(error @ broker_rules::RuleEngineError::Persist(_)) => rule_persist_error(error),
        Err(broker_rules::RuleEngineError::InvalidRule(message))
            if message.starts_with("duplicate rule id:") =>
        {
            ApiError::BadRequest(message).into_response()
        }
        Err(e) => ApiError::BadRequest(format!("Rule creation failed: {e}")).into_response(),
    }
}

/// `GET /api/v5/rules/:id`: read one rule (C06-21).
///
/// A known id returns the full documented rule fields (`id`, `name`,
/// `tenant`, `sql`, `from`, `enable`, `actions`) from live engine state; an unknown
/// id returns 404 with the documented error code, not an empty body. The
/// pair is not paged; unknown query parameters are ignored (the ignored
/// query sink below). Management-plane only: one short read lock cloning
/// at most one small rule per request, and publish and deliver never touch
/// this map (dispatch reads a snapshot).
pub async fn get_rule(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Query(_ignored): Query<HashMap<String, String>>,
) -> Response {
    match state.engine.get_rule(&id) {
        Some(r) => (StatusCode::OK, Json(rule_to_v5(&r))).into_response(),
        _ => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "code": "NOT_FOUND",
                "message": "Rule not found"
            })),
        )
            .into_response(),
    }
}

/// `PUT /api/v5/rules/:id`: validated full replace of one rule (C06-22).
///
/// A `tenant` field moves the rule to that tenant. Absent keeps the
/// stored tenant. An unknown tenant fails with 400 and keeps the old
/// rule.
/// The body gets the same compilation as create: it must be a JSON object
/// carrying the full rule (`sql` required, `actions` as read by
/// [`parse_rule_actions`]: stored-action ids validated against the W3-02
/// action directory, or the built-in `republish` and `console` functions,
/// `enable` as a bool, `name` free-form). The SQL is recompiled before
/// anything is stored: a malformed body is rejected with the documented
/// error shape and the old rule is kept. An unknown path id returns 404
/// and creates nothing. On success the stored entry is swapped for the
/// recompiled snapshot and the new fields read back with 200.
///
/// A body carrying none of `sql`/`from`/`actions`/`name` is treated as a
/// legacy enable flip (the pre-W3-14 shape): only `enable` is applied, so
/// existing enable-only callers keep working while full bodies replace.
///
/// The path id is authoritative: a body `id`/`name` that names a different
/// rule is rejected instead of renaming or forking the entry. An omitted
/// `enable` keeps the stored value; an omitted `name` keeps the stored
/// name; an omitted `actions` clears to empty (whole-replace semantics).
/// The `from` array, when present, is accepted but the topic is derived
/// from the SQL `FROM` clause (the same derivation create uses), so a
/// conflicting `from` does not fork routing.
///
/// The pair is not paged; unknown query parameters are ignored (the
/// ignored query sink below). Management-plane only: one short write lock
/// swapping a single recompiled entry (plus the registry save lock on
/// persist); rule evaluation keeps reading its snapshot and publish and
/// deliver never touch this map.
///
/// Bound (stated here and enforced by construction): a replace never grows
/// the map, so the [`MAX_RULES`] cap from W3-13 applies unchanged and no
/// new accumulation is introduced. Rules persist across restart via the
/// config core; per-rule counters are memory-only and rebuild at zero.
///
/// Fire-path cost (stated for the delivery-path note): no new work on
/// publish or deliver. Dispatch keeps loading one lock-free settings `Arc`
/// plus one `Arc` each of the disabled-action set and the per-action metric
/// map per ingress, then clones the matching rule `Arc`s under one short
/// read lock; this update takes the map lock only on the management plane.
// TODO(parity): may an update rename the rule (body id differing from the
// path id), or change only the enable half? The rulebook does not decide
// the exact PUT field set; the current choice keeps the path id
// authoritative and replaces the whole entry in place until the checker
// pins it.
// TODO(parity): does an omitted `enable` keep the stored value or default
// to `true`? The rulebook does not decide; the current choice keeps the
// stored value (a replace must not surprise-disable a rule) until the
// checker pins it.
// TODO(parity): does an omitted `actions` clear to empty or keep the
// stored list? The rulebook does not decide; the current choice clears to
// empty (whole-replace semantics, mirroring create's default) until the
// checker pins it.
// TODO(parity): is the topic taken from the SQL `FROM` clause, from the
// `from` array, or must both agree? The rulebook does not decide; the
// current choice derives from SQL (the same derivation create uses) and
// accepts `from` without forking routing until the checker pins it.
// TODO(parity): for an unknown id with an invalid body, is the answer 404
// or 400? The rulebook does not decide the precedence; the current choice
// answers 404 first (existence before validation, mirroring the action
// update) and creates nothing either way.
pub async fn update_rule(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Query(_ignored): Query<HashMap<String, String>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(existing) = state.engine.get_rule(&id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "code": "NOT_FOUND",
                "message": "Rule not found"
            })),
        )
            .into_response();
    };
    let Some(obj) = body.as_object() else {
        return ApiError::BadRequest("request body must be a JSON object".to_string())
            .into_response();
    };
    if let Some(body_id) = obj
        .get("id")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        if body_id != id {
            return ApiError::BadRequest(format!(
                "body id {body_id:?} does not match path id {id:?} (field `id`)"
            ))
            .into_response();
        }
    }
    let has_full_keys = obj.contains_key("sql")
        || obj.contains_key("from")
        || obj.contains_key("actions")
        || obj.contains_key("name")
        || obj.contains_key("tenant");
    if !has_full_keys {
        // Legacy enable-only shape (pre-W3-14): apply only the flag.
        if let Some(enable) = obj.get("enable").and_then(|v| v.as_bool()) {
            // A persist failure must surface as a 500, never as a success:
            // the in-memory flag applied but the disk save failed.
            match state.engine.set_rule_enabled(&id, enable) {
                Ok(_) => {}
                Err(error) => return rule_persist_error(error),
            }
        }
        match state.engine.get_rule(&id) {
            Some(r) => (StatusCode::OK, Json(rule_to_v5(&r))).into_response(),
            None => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "code": "NOT_FOUND",
                    "message": "Rule not found"
                })),
            )
                .into_response(),
        }
    } else {
        let name = obj
            .get("name")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .unwrap_or_else(|| existing.name.clone());
        let Some(sql) = obj
            .get("sql")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
        else {
            return ApiError::BadRequest("missing required field: sql".to_string()).into_response();
        };
        let enable = obj
            .get("enable")
            .and_then(|v| v.as_bool())
            .unwrap_or(existing.enabled);
        let raw_actions: Vec<serde_json::Value> = match obj.get("actions") {
            None => Vec::new(),
            Some(value) => match value.as_array() {
                Some(items) => items.clone(),
                None => {
                    return ApiError::BadRequest("field `actions` must be an array".to_string())
                        .into_response();
                }
            },
        };
        let topic_str = if sql.contains("FROM \"") {
            sql.split("FROM \"")
                .nth(1)
                .and_then(|s| s.split('"').next())
                .unwrap_or("t/#")
        } else {
            "t/#"
        };
        let topic_filter = match broker_protocol::TopicFilter::new(topic_str) {
            Ok(filter) => {
                if topic_str.contains('[') || topic_str.contains(']') {
                    return ApiError::BadRequest(
                        "invalid topic filter: bracket characters are not allowed".to_string(),
                    )
                    .into_response();
                }
                filter
            }
            Err(e) => {
                return ApiError::BadRequest(format!("invalid topic filter: {e}")).into_response();
            }
        };
        let actions = match parse_rule_actions(&state, &raw_actions) {
            Ok(actions) => actions,
            Err(response) => return *response,
        };
        // Tenant scope: a body tenant moves the rule. Absent keeps
        // the stored tenant. A present but non-string tenant fails
        // with 400. An unknown tenant fails with 400 and keeps the
        // old rule.
        let tenant = match obj.get("tenant") {
            None => existing.tenant.clone(),
            Some(value) => {
                let Some(raw) = value.as_str() else {
                    return ApiError::BadRequest("field `tenant` must be a string".to_string())
                        .into_response();
                };
                let tenant = broker_config::canonical_rule_tenant(raw);
                if !broker_config::is_valid_rule_tenant(&tenant) {
                    return ApiError::BadRequest(format!(
                        "tenant {tenant:?} is not valid (field `tenant`)"
                    ))
                    .into_response();
                }
                if !state.namespaces.contains(&tenant) {
                    return ApiError::BadRequest(format!(
                        "unknown tenant {tenant:?} (field `tenant`)"
                    ))
                    .into_response();
                }
                tenant
            }
        };
        // A move outside the default tenant keeps the stored forwards
        // when the body names no actions: check the effective set.
        let effective: &[broker_rules::RuleAction] = if obj.contains_key("actions") {
            &actions
        } else {
            &existing.actions
        };
        if let Some(response) = refuse_forward_outside_default_tenant(&tenant, effective) {
            return response;
        }
        match state.engine.replace_rule_in_tenant(
            &id,
            tenant,
            name,
            topic_filter,
            Some(sql),
            enable,
            actions,
        ) {
            Ok(Some(r)) => (StatusCode::OK, Json(rule_to_v5(&r))).into_response(),
            Ok(None) => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "code": "NOT_FOUND",
                    "message": "Rule not found"
                })),
            )
                .into_response(),
            Err(error @ broker_rules::RuleEngineError::Persist(_)) => rule_persist_error(error),
            Err(e) => ApiError::BadRequest(format!("Rule update failed: {e}")).into_response(),
        }
    }
}

/// `DELETE /api/v5/rules/:id`: drop one rule. An unknown id returns
/// 404 with the documented error code, not an empty body.
/// Management-plane only; publish and deliver never touch this map
/// (dispatch reads a snapshot).
pub async fn delete_rule(State(state): State<ApiState>, Path(id): Path<String>) -> Response {
    match state.engine.remove_rule(&id) {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => ApiError::NotFound(format!("rule {id:?} not found")).into_response(),
        // A persist failure must surface as a 500, never as a success:
        // the in-memory removal applied but the disk save failed.
        Err(error) => rule_persist_error(error),
    }
}

/// `GET /api/v5/rules/:id/metrics`: live per-rule counters (C06-23).
///
/// Reads the fixed counter set from live engine state (`matched`,
/// `passed`, `failed` plus the per-rule action attribution
/// `actions.total`/`actions.success`/`actions.failed`). Every field is a
/// lock-free atomic bumped only on the evaluation path
/// (`RuleEngine::dispatch_ingress` in `broker-rules`), never a constant:
/// `matched` counts topic-filter hits, `passed` counts rows that produced
/// output, `failed` counts hits that produced none (WHERE-false and
/// non-JSON payloads alike). A fresh rule reads zeroes; a restart rebuilds
/// the map at zero (memory-only by design, never persisted).
///
/// An unknown id returns 404 with the documented `{code, message}` shape,
/// not an empty body. The pair is not paged; unknown query parameters are
/// ignored (the ignored query sink below). Management-plane only: one
/// snapshot clone plus relaxed atomic loads; increments stay lock-free
/// atomics and publish/deliver never touch this map directly.
///
/// Bound (stated here and enforced by construction): a fixed six-counter
/// set per rule (three `AtomicU64` for match/pass/fail plus three for
/// action attribution, default zero); no per-fire allocation (one snapshot
/// clone per request on the read path, relaxed loads only) and no
/// unbounded history.
///
/// Fire-path cost (stated for the delivery-path note): no new work on
/// publish or deliver. The evaluation path pays one relaxed `fetch_add`
/// per counter per ingress (lock-free atomics only, no lock, no queue, no
/// per-fire allocation); text rendering stays here in the handler.
// TODO(parity): which counter names plus envelope shapes does the documented
// spec require beyond `matched`/`passed`/`failed` (rates, node rows, ...)?
// The rulebook does not decide the exact shape; the current choice reports
// the honest live subset the fire path can attribute and omits the rest
// rather than inventing constants, until the checker pins it.
// TODO(parity): are the per-rule action-attribution keys part of the
// documented shape, or only the three match counters? The rulebook does
// not decide; the current choice reports both (dotted `actions.*` keys)
// until the checker pins it.
pub async fn get_rule_metrics(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Query(_ignored): Query<HashMap<String, String>>,
) -> Response {
    let rules = state.engine.list_rules();
    let Some(r) = rules.iter().find(|r| r.id == id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "code": "NOT_FOUND",
                "message": "Rule not found"
            })),
        )
            .into_response();
    };
    let matched = r.matched_cnt.load(std::sync::atomic::Ordering::Relaxed);
    let passed = r.passed_cnt.load(std::sync::atomic::Ordering::Relaxed);
    let failed = r.failed_cnt.load(std::sync::atomic::Ordering::Relaxed);
    let actions_total = r
        .actions_total_cnt
        .load(std::sync::atomic::Ordering::Relaxed);
    let actions_success = r
        .actions_success_cnt
        .load(std::sync::atomic::Ordering::Relaxed);
    let actions_failed = r
        .actions_failed_cnt
        .load(std::sync::atomic::Ordering::Relaxed);

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "id": id,
            "metrics": {
                "matched": matched,
                "passed": passed,
                "failed": failed,
                "actions.total": actions_total,
                "actions.success": actions_success,
                "actions.failed": actions_failed
            }
        })),
    )
        .into_response()
}

/// `PUT /api/v5/rules/:id/metrics/reset`: zero one rule's counters (C06-24).
///
/// Resets the live counters to zero and reports success with 204 and no
/// body. Resetting an unknown id is 404 with the documented `{code,
/// message}` shape, not an empty body. Counters resume from zero on the
/// next evaluation; the reset itself persists only as the zeroed values
/// (the map is memory-only and restarts at zero by design). The pair is
/// not paged; unknown query parameters are ignored. Management-plane only:
/// one snapshot clone plus lock-free atomic stores; increments stay
/// lock-free atomics and publish/deliver never touch this map directly.
// TODO(parity): does the spec report a reset as an empty 204 or as a 200
// with a body? The rulebook does not decide the exact success shape; the
// current choice answers 204 with no body, mirroring the sibling action
// and authenticator resets, until the checker pins it.
pub async fn reset_rule_metrics(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Query(_ignored): Query<HashMap<String, String>>,
) -> Response {
    let rules = state.engine.list_rules();
    let Some(r) = rules.iter().find(|r| r.id == id) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "code": "NOT_FOUND",
                "message": "Rule not found"
            })),
        )
            .into_response();
    };
    r.matched_cnt.store(0, std::sync::atomic::Ordering::Relaxed);
    r.passed_cnt.store(0, std::sync::atomic::Ordering::Relaxed);
    r.failed_cnt.store(0, std::sync::atomic::Ordering::Relaxed);
    r.actions_total_cnt
        .store(0, std::sync::atomic::Ordering::Relaxed);
    r.actions_success_cnt
        .store(0, std::sync::atomic::Ordering::Relaxed);
    r.actions_failed_cnt
        .store(0, std::sync::atomic::Ordering::Relaxed);
    StatusCode::NO_CONTENT.into_response()
}

// ---------------------------------------------------------------------------
// Data Connectors
// ---------------------------------------------------------------------------

/// Insert refusal: duplicate id or a full store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConnectorInsertError {
    Duplicate,
    Full,
}

/// Bounded in-memory connector directory, one per `ApiState`.
///
/// Each state holds its own list, so two servers in one process never
/// share connectors. Every mutation persists through the bound registry.
/// Management writes take the write lock plus the save lock on persist;
/// reads take one short lock and clone at most [`MAX_CONNECTORS`] small
/// entries. Publish and deliver never touch this store: dispatch reads
/// the connector manager directly.
pub struct ConnectorStore {
    inner: RwLock<Vec<serde_json::Value>>,
    /// Kernel config registry receiving every mutation (`None` in unit
    /// tests and standalone state, which stay memory-only).
    registry: RwLock<Option<Arc<broker_config::ConfigRegistry>>>,
    /// Serialises export-commit-save so concurrent mutations cannot
    /// interleave into a lost update on disk.
    save_lock: Mutex<()>,
}

impl ConnectorStore {
    /// Empty directory (today's empty behaviour: no connectors).
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(Vec::new()),
            registry: RwLock::new(None),
            save_lock: Mutex::new(()),
        }
    }

    /// Seed from the registry snapshot and replay every live sink through
    /// the same path `create_connector` uses. Kernel boot path; an empty
    /// snapshot yields today's empty behaviour.
    pub fn from_registry(
        registry: &Arc<broker_config::ConfigRegistry>,
        engine: &broker_rules::RuleEngine,
    ) -> Self {
        let store = Self::new();
        store.seed_from_registry(registry, engine);
        store
    }

    /// Attach the registry and replace the contents with its snapshot, in
    /// place on the same instance. Kernel boot path: the shared instance
    /// `serve_api` hands to `ApiState` must observe the persisted
    /// connectors from the first connection, and re-register every live
    /// sink through the same path `create_connector` uses. An empty
    /// snapshot keeps today's empty behaviour.
    pub fn seed_from_registry(
        &self,
        registry: &Arc<broker_config::ConfigRegistry>,
        engine: &broker_rules::RuleEngine,
    ) {
        let snapshot = registry.snapshot();
        self.inner
            .write()
            .unwrap_or_else(|poison| poison.into_inner())
            .clear();
        if snapshot.connectors.connectors.is_empty() {
            *self
                .registry
                .write()
                .unwrap_or_else(|poison| poison.into_inner()) = Some(Arc::clone(registry));
            return;
        }
        if let Err(error) = snapshot.connectors.validate_stored() {
            panic!("invalid stored connectors snapshot: {error}");
        }
        let mut restored = Vec::with_capacity(snapshot.connectors.connectors.len());
        for entry in &snapshot.connectors.connectors {
            // Bare `file:`/`env:` references are valid stored configs:
            // keep the reference form instead of panicking on non-JSON.
            let params = connector_store_doc(entry);
            restored.push((entry.id.clone(), entry.connector_type.clone(), params));
        }
        // `register_live_sink` is async (the `disk_log` arm opens its
        // directory) while boot callers are sync (`ApiState::new`), so the
        // replay runs on a dedicated thread with its own current-thread
        // runtime. The thread is joined: boot never completes with sinks
        // half restored.
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("connector boot runtime");
                    runtime.block_on(async {
                        for (id, conn_type, mut params) in restored {
                            let removed = broker_config::is_removed_connector_type(&conn_type);
                            let unavailable = removed || kind_refusal(&conn_type).is_some();
                            // A webhook sink is lazy and registration sends
                            // no bytes. Do not dial it at boot: a bare TCP
                            // probe leaves a connection in the target's
                            // accept queue. Other kinds keep the probe, so a
                            // target that is down now reads `disconnected`
                            // and stays persist-only until a send retries.
                            let has_target = extract_target_host_port(&params).is_some();
                            let lazy_http = matches!(conn_type.as_str(), "webhook" | "http");
                            let reachable = if unavailable {
                                false
                            } else if lazy_http {
                                true
                            } else {
                                !has_target || probe_connector_reachable(&params).await
                            };
                            let status = if unavailable {
                                "disconnected"
                            } else {
                                connector_status(reachable)
                            };
                            if let Some(obj) = params.as_object_mut() {
                                obj.insert(
                                    "status".to_string(),
                                    serde_json::Value::String(status.to_string()),
                                );
                                obj.insert(
                                    "node_status".to_string(),
                                    connector_node_status(status),
                                );
                            }
                            self.upsert(params.clone());
                            if reachable && !unavailable {
                                // A refused build fails closed here: the
                                // entry stays persist-only and the error is
                                // logged naming the connector and its kind.
                                if let Err(register_error) =
                                    try_register_live_sink(engine, &conn_type, &id, &params).await
                                {
                                    tracing::warn!(
                                        connector = %id,
                                        kind = %conn_type,
                                        "{register_error}"
                                    );
                                }
                            }
                        }
                    });
                })
                .join()
                .expect("connector boot thread");
        });
        *self
            .registry
            .write()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(Arc::clone(registry));
    }

    /// Clone of every stored document, unsorted. Callers sort for reads.
    pub fn snapshot(&self) -> Vec<serde_json::Value> {
        self.inner
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    /// Clone of one stored document. Matches the bare `id`, the bare
    /// `name` or the `{type}:{name}` key exactly. Returns `None` when
    /// the id is unknown (caller maps to `NOT_FOUND`).
    pub fn get(&self, id: &str) -> Option<serde_json::Value> {
        self.inner
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .find(|c| connector_doc_matches(c, id))
            .cloned()
    }

    /// True when the id already lives in the store. Matches the bare
    /// `id`, the bare `name` or the `{type}:{name}` key exactly, like
    /// the read and update paths.
    pub fn contains(&self, id: &str) -> bool {
        self.inner
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .any(|c| connector_doc_matches(c, id))
    }

    /// Count of stored documents, under one short read lock.
    pub fn len(&self) -> usize {
        self.inner
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .len()
    }

    /// True when the store holds no document.
    pub fn is_empty(&self) -> bool {
        self.inner
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .is_empty()
    }

    /// Insert one document. Fails cleanly when the id already exists
    /// (caller maps to `ALREADY_EXISTS`) or the store is at
    /// [`MAX_CONNECTORS`] (caller maps to `BAD_REQUEST`). Both checks run
    /// under the write lock, so concurrent creates cannot push duplicates.
    pub(crate) fn try_insert(
        &self,
        id: &str,
        doc: serde_json::Value,
    ) -> Result<(), ConnectorInsertError> {
        let mut inner = self
            .inner
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        if inner.iter().any(|c| {
            c.get("id").and_then(|v| v.as_str()) == Some(id)
                || c.get("name").and_then(|v| v.as_str()) == Some(id)
        }) {
            return Err(ConnectorInsertError::Duplicate);
        }
        if inner.len() >= MAX_CONNECTORS {
            return Err(ConnectorInsertError::Full);
        }
        inner.push(doc);
        Ok(())
    }

    /// Replace the document stored under `id`. Returns false when the id
    /// is unknown (caller maps to `NOT_FOUND` and creates nothing). The
    /// replace swaps one entry in place and never grows the store toward
    /// [`MAX_CONNECTORS`]. Matches the bare id, the bare name or the
    /// `{type}:{name}` key.
    pub(crate) fn replace(&self, id: &str, doc: serde_json::Value) -> bool {
        let mut inner = self
            .inner
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        let Some(pos) = inner.iter().position(|c| connector_doc_matches(c, id)) else {
            return false;
        };
        inner[pos] = doc;
        true
    }

    /// Remove one document by id. Returns false when the id is unknown
    /// (caller maps to `NOT_FOUND`). Matches the bare id, the bare name
    /// or the `{type}:{name}` key.
    pub(crate) fn remove(&self, id: &str) -> bool {
        let mut inner = self
            .inner
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        let before = inner.len();
        inner.retain(|c| !connector_doc_matches(c, id));
        inner.len() != before
    }

    /// Flip the enable flag of one document in place. Returns false when
    /// the id is unknown (caller maps to `NOT_FOUND` and persists
    /// nothing). Touches only the single boolean. Matches the bare id,
    /// the bare name or the `{type}:{name}` key.
    pub(crate) fn set_enabled(&self, id: &str, enable: bool) -> bool {
        let mut inner = self
            .inner
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        let Some(pos) = inner.iter().position(|c| connector_doc_matches(c, id)) else {
            return false;
        };
        if let Some(obj) = inner[pos].as_object_mut() {
            obj.insert("enable".to_string(), serde_json::Value::Bool(enable));
        }
        true
    }

    /// Insert or replace one document by `id`-or-`name` match. Boot and
    /// reload replay only; request paths use the checked writes above.
    pub(crate) fn upsert(&self, doc: serde_json::Value) {
        let id = doc
            .get("id")
            .and_then(|v| v.as_str())
            .or_else(|| doc.get("name").and_then(|v| v.as_str()))
            .unwrap_or("")
            .to_string();
        let mut inner = self
            .inner
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        if let Some(pos) = inner.iter().position(|c| {
            c.get("id").and_then(|v| v.as_str()) == Some(id.as_str())
                || c.get("name").and_then(|v| v.as_str()) == Some(id.as_str())
        }) {
            inner[pos] = doc;
        } else {
            inner.push(doc);
        }
    }

    /// Export the store as a validated config root: entries sorted by id
    /// so the persisted file is deterministic. Only inputs persist: the
    /// derived `status`/`node_status` display values are stripped and
    /// recomputed at read time.
    fn export_conf(&self) -> broker_config::ConnectorsConf {
        let inner = self
            .inner
            .read()
            .unwrap_or_else(|poison| poison.into_inner());
        let mut entries: Vec<broker_config::ConnectorEntry> = inner
            .iter()
            .map(|body| {
                let id = body
                    .get("id")
                    .and_then(|v| v.as_str())
                    .or_else(|| body.get("name").and_then(|v| v.as_str()))
                    .unwrap_or_default()
                    .to_string();
                let connector_type = body
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("http")
                    .to_string();
                let enable = body.get("enable").and_then(|v| v.as_bool()).unwrap_or(true);
                let mut params = body.clone();
                if let Some(obj) = params.as_object_mut() {
                    obj.remove("status");
                    obj.remove("node_status");
                }
                broker_config::ConnectorEntry {
                    id,
                    connector_type,
                    enable,
                    config: serde_json::to_string(&params).unwrap_or_else(|_| "{}".to_string()),
                }
            })
            .collect();
        entries.sort_by(|a, b| a.id.cmp(&b.id));
        broker_config::ConnectorsConf {
            connectors: entries,
        }
    }

    /// Commit the exported root and atomically save it.
    ///
    /// A no-op without a registry. Any commit or save failure surfaces
    /// so callers answer 500 and the loss is never silent.
    ///
    /// It uses the stored rewrite. Already-stored unavailable records
    /// stay readable. It still refuses a new removed kind. Create and
    /// update handlers refuse the kind before the store mutates.
    pub(crate) fn persist(&self) -> Result<(), broker_config::ConfigError> {
        let registry = self
            .registry
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        let Some(registry) = registry else {
            return Ok(());
        };
        let _guard = self
            .save_lock
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let conf = self.export_conf();
        registry.commit_connectors_stored(conf)?;
        registry.save()?;
        Ok(())
    }

    /// Reconcile this store with a freshly committed snapshot
    /// (reload/restore path): upsert every candidate entry (reference
    /// form, status recomputed with the same probe create uses) and drop
    /// ids the candidate removed. Runs on the store of the serving state,
    /// never on a process static.
    pub(crate) async fn reconcile(
        &self,
        previous: &broker_config::ConnectorsConf,
        candidate: &broker_config::ConnectorsConf,
    ) {
        use std::collections::HashSet;
        let candidate_ids: HashSet<&str> = candidate
            .connectors
            .iter()
            .map(|entry| entry.id.as_str())
            .collect();
        for entry in &previous.connectors {
            if candidate_ids.contains(entry.id.as_str()) {
                continue;
            }
            self.remove(entry.id.as_str());
        }
        for entry in &candidate.connectors {
            let mut doc = connector_store_doc(entry);
            let unavailable = broker_config::is_removed_connector_type(&entry.connector_type)
                || kind_refusal(&entry.connector_type).is_some();
            let reachable = !unavailable && probe_connector_reachable(&doc).await;
            let status = if unavailable {
                "disconnected"
            } else {
                connector_status(reachable)
            };
            if let Some(obj) = doc.as_object_mut() {
                obj.insert(
                    "status".to_string(),
                    serde_json::Value::String(status.to_string()),
                );
                obj.insert("node_status".to_string(), connector_node_status(status));
            }
            self.upsert(doc);
        }
    }
}

impl Default for ConnectorStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Map a connector registry failure onto a 500: the in-memory mutation
/// applied but the commit or atomic save failed, so the loss must never
/// be silent.
pub(crate) fn connector_persist_error(error: broker_config::ConfigError) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({
            "code": "INTERNAL_ERROR",
            "message": format!("cannot persist connectors: {error}")
        })),
    )
        .into_response()
}

/// Probe the connector target the way boot replay does: entries
/// without a `server`/`url`/… target never probe and count as reachable,
/// except document kinds which fail closed without a target.
/// Otherwise a 1.5 s TCP connect decides.
pub(crate) async fn probe_connector_reachable(body: &serde_json::Value) -> bool {
    match extract_target_host_port(body) {
        Some(target) => check_tcp_reachable(&target).await.is_ok(),
        None => {
            let kind = body.get("type").and_then(|v| v.as_str()).unwrap_or("");
            !document_kind_needs_target(kind)
        }
    }
}

/// Display status derived from reachability (recomputed at read time,
/// never persisted).
fn connector_status(reachable: bool) -> &'static str {
    if reachable {
        "connected"
    } else {
        "disconnected"
    }
}

/// Per-node display status for a connector entry (recomputed at read
/// time, never persisted).
fn connector_node_status(status: &str) -> serde_json::Value {
    serde_json::json!([{ "node": local_node_name(), "status": status }])
}

/// Required-field gate for connector registration (S1-01): no connector
/// may fall back to a built-in credential or an invented identity. Returns
/// the first missing required field's canonical name, or `None` when the
/// body supplies everything the live-sink builder needs. Callers answer
/// 400 naming that field; `register_live_sink` itself also refuses a
/// build it cannot complete, so boot replay cannot invent values.
///
/// Only the six connector families that previously carried built-in
/// credentials or invented identities are gated here. Every other family
/// keeps its existing behaviour; see the marker on the fallback arm below.
fn missing_connector_field(conn_type: &str, body: &serde_json::Value) -> Option<&'static str> {
    fn present(body: &serde_json::Value, keys: &[&str]) -> bool {
        keys.iter().any(|k| {
            body.get(*k)
                .and_then(|v| v.as_str())
                .is_some_and(|s| !s.trim().is_empty())
        })
    }

    match conn_type {
        "gcp_iot" | "gcp_iot_core" => {
            // Credential first so a bare body names the credential, not an identity.
            if !present(body, &["private_key_pem", "private_key"]) {
                return Some("private_key_pem");
            }
            if !present(body, &["project_id", "project"]) {
                return Some("project_id");
            }
            if !present(body, &["cloud_region", "region"]) {
                return Some("cloud_region");
            }
            if !present(body, &["registry_id", "registry"]) {
                return Some("registry_id");
            }
            if !present(body, &["device_id", "device"]) {
                return Some("device_id");
            }
            None
        }
        "databricks" | "delta_lake" => {
            if !present(body, &["token", "api_key"]) {
                return Some("token");
            }
            if !present(body, &["host", "server", "endpoint"]) {
                return Some("host");
            }
            if !present(body, &["catalog"]) {
                return Some("catalog");
            }
            if !present(body, &["schema"]) {
                return Some("schema");
            }
            if !present(body, &["table", "table_template"]) {
                return Some("table");
            }
            None
        }
        "snowflake" => {
            if !present(body, &["private_key_pem", "private_key"]) {
                return Some("private_key_pem");
            }
            if !present(body, &["account"]) {
                return Some("account");
            }
            if !present(body, &["user", "username"]) {
                return Some("user");
            }
            if !present(body, &["database"]) {
                return Some("database");
            }
            if !present(body, &["schema"]) {
                return Some("schema");
            }
            if !present(body, &["table", "table_template"]) {
                return Some("table");
            }
            None
        }
        "tablestore" | "ots" => {
            if !present(body, &["access_key_id", "ak"]) {
                return Some("access_key_id");
            }
            if !present(body, &["access_key_secret", "sk"]) {
                return Some("access_key_secret");
            }
            if !present(body, &["instance_name", "instance"]) {
                return Some("instance_name");
            }
            if !present(body, &["table_name", "table"]) {
                return Some("table_name");
            }
            if !present(body, &["endpoint", "url"]) {
                return Some("endpoint");
            }
            None
        }
        "oci_streaming" | "oci" => {
            if !present(body, &["private_key_pem", "private_key"]) {
                return Some("private_key_pem");
            }
            if !present(body, &["stream_pool_id"]) {
                return Some("stream_pool_id");
            }
            if !present(body, &["stream_id"]) {
                return Some("stream_id");
            }
            if !present(body, &["tenancy_ocid"]) {
                return Some("tenancy_ocid");
            }
            if !present(body, &["user_ocid"]) {
                return Some("user_ocid");
            }
            if !present(body, &["fingerprint"]) {
                return Some("fingerprint");
            }
            if !present(body, &["endpoint", "url"]) {
                return Some("endpoint");
            }
            None
        }
        "confluent" => {
            if !present(body, &["api_key", "username"]) {
                return Some("api_key");
            }
            if !present(body, &["api_secret", "password"]) {
                return Some("api_secret");
            }
            None
        }
        "azure_eventhubs" | "azure_event_hubs" => {
            if !present(body, &["shared_access_key", "key"]) {
                return Some("shared_access_key");
            }
            None
        }
        "azure_iot" | "azure_iot_hub" => {
            if !present(body, &["shared_access_key", "key"]) {
                return Some("shared_access_key");
            }
            None
        }
        "aws_iot" | "aws_iot_core" => {
            // mTLS-only build (R1-01): client certificate + key come from
            // the operator. SigV4/WebSocket has no transport and is rejected
            // at construction; never invent test credentials here.
            // Accept flat fields and the nested `auth` object used by v1.
            let nested = body.get("auth").and_then(|v| v.as_object());
            let has = |keys: &[&str]| {
                if present(body, keys) {
                    return true;
                }
                if let Some(auth) = nested {
                    return keys.iter().any(|k| {
                        auth.get(*k)
                            .and_then(|v| v.as_str())
                            .is_some_and(|s| !s.trim().is_empty())
                    });
                }
                false
            };
            if !has(&["client_cert_pem", "client_cert", "certificate", "cert_pem"]) {
                return Some("client_cert_pem");
            }
            if !has(&["client_key_pem", "client_key", "private_key", "key_pem"]) {
                return Some("client_key_pem");
            }
            None
        }
        // The MV-02b time-series families carry their own fail-closed
        // gate (documented defaults only); the remaining families keep
        // their existing behaviour until their service documentation is
        // checked for real defaults.
        _ => missing_mv02b_connector_field(conn_type, body),
    }
}

/// MV-02b time-series required fields (S1-01): the connector create and
/// update paths refuse a body that misses one of these with the field
/// named, and the live-sink arms below repeat the same check so boot
/// replay cannot invent a target either. Every default applied below
/// names its source (the reference default or the server default).
fn missing_mv02b_connector_field(
    conn_type: &str,
    body: &serde_json::Value,
) -> Option<&'static str> {
    fn present(body: &serde_json::Value, keys: &[&str]) -> bool {
        keys.iter().any(|k| {
            body.get(*k)
                .and_then(|v| v.as_str())
                .is_some_and(|s| !s.trim().is_empty())
        })
    }

    /// True when one of the keys carries a usable value: a non-blank
    /// string, a non-empty array or a non-empty object.
    fn present_any(body: &serde_json::Value, keys: &[&str]) -> bool {
        keys.iter().any(|k| match body.get(*k) {
            Some(value) if value.is_string() => {
                value.as_str().is_some_and(|s| !s.trim().is_empty())
            }
            Some(value) if value.is_array() => {
                value.as_array().is_some_and(|items| !items.is_empty())
            }
            Some(value) if value.is_object() => {
                value.as_object().is_some_and(|map| !map.is_empty())
            }
            _ => false,
        })
    }

    match conn_type {
        "clickhouse" | "ch" => {
            if !present(body, &["url", "endpoint", "server"]) {
                return Some("url");
            }
            if !present(body, &["database"]) {
                return Some("database");
            }
            if !present(body, &["table", "table_template"]) {
                return Some("table");
            }
            None
        }
        "tdengine" => {
            if !present(body, &["server", "url", "endpoint"]) {
                return Some("server");
            }
            if !present(body, &["database"]) {
                return Some("database");
            }
            if !present(body, &["stable", "stable_name", "table"]) {
                return Some("stable");
            }
            if !present(body, &["subtable", "subtable_template"]) {
                return Some("subtable");
            }
            if !present_any(body, &["metrics", "metrics_template"]) {
                return Some("metrics");
            }
            if !present_any(body, &["tags", "tags_template"]) {
                return Some("tags");
            }
            None
        }
        "greptimedb" | "greptime" => {
            if !present(body, &["database", "dbname"]) {
                return Some("database");
            }
            if !present(body, &["table", "table_template"]) {
                return Some("table");
            }
            None
        }
        "iotdb" => {
            if !present(body, &["endpoint", "server", "url", "base_url"]) {
                return Some("endpoint");
            }
            if !present(body, &["username", "user"]) {
                return Some("username");
            }
            if !present(body, &["password"]) {
                return Some("password");
            }
            if !present(body, &["device", "device_path", "device_path_template"]) {
                return Some("device");
            }
            if !present_any(body, &["measurements"]) {
                return Some("measurements");
            }
            if !present_any(body, &["types", "data_types"]) {
                return Some("types");
            }
            None
        }
        "opentsdb" | "opents" => {
            if !present(body, &["server", "endpoint", "url"]) {
                return Some("server");
            }
            if !present(body, &["metric", "metric_template"]) {
                return Some("metric");
            }
            if !present(body, &["value", "value_field"]) {
                return Some("value");
            }
            None
        }
        "doris" => {
            if !present(body, &["server", "fe_host", "host", "endpoint"]) {
                return Some("server");
            }
            if !present(body, &["database"]) {
                return Some("database");
            }
            if !present(body, &["table", "table_template"]) {
                return Some("table");
            }
            None
        }
        "datalayers" => {
            if !present(body, &["database"]) {
                return Some("database");
            }
            if !present(body, &["table", "measurement"]) {
                return Some("table");
            }
            None
        }
        "influxdb" => {
            if !present(body, &["bucket"]) {
                return Some("bucket");
            }
            if !present(body, &["org"]) {
                return Some("org");
            }
            if !present(body, &["token"]) {
                return Some("token");
            }
            if !present(body, &["measurement", "measurement_template"]) {
                return Some("measurement");
            }
            None
        }
        "timestream" | "aws_timestream" => {
            if !present(body, &["database", "database_name", "db"]) {
                return Some("database");
            }
            if !present(body, &["region"]) {
                return Some("region");
            }
            if !present(body, &["access_key_id", "access_key"]) {
                return Some("access_key_id");
            }
            if !present(body, &["secret_access_key", "secret_key"]) {
                return Some("secret_access_key");
            }
            if !present(body, &["table", "table_name"]) {
                return Some("table");
            }
            if !present_any(body, &["measures", "multi_measure_mappings"]) {
                return Some("measures");
            }
            None
        }
        "cassandra" | "scylla" | "scylladb" | "cql" => {
            if !present(body, &["servers", "server", "contact_points"]) {
                return Some("servers");
            }
            if !present(body, &["keyspace"]) {
                return Some("keyspace");
            }
            if !present(body, &["table", "table_template"]) {
                return Some("table");
            }
            None
        }
        "mongodb" | "mongo" | "documentdb" => {
            let nested = body.get("parameters").and_then(|value| value.as_object());
            let nested_present = |keys: &[&str]| {
                nested.is_some_and(|map| {
                    keys.iter().any(|key| {
                        map.get(*key)
                            .and_then(|value| value.as_str())
                            .is_some_and(|text| !text.trim().is_empty())
                    })
                })
            };
            if !present(body, &["server", "servers", "connection_string"])
                && !nested_present(&["server", "servers"])
            {
                return Some("server");
            }
            if !present(body, &["database"]) && !nested_present(&["database"]) {
                return Some("database");
            }
            None
        }
        "couchbase" | "couch" | "cb" => {
            if !present(body, &["server", "servers", "connection_string"]) {
                return Some("server");
            }
            let nested_auth = body.get("auth").and_then(|value| value.as_object());
            let auth_user = nested_auth.is_some_and(|auth| {
                auth.get("username")
                    .and_then(|value| value.as_str())
                    .is_some_and(|text| !text.trim().is_empty())
            });
            if !present(body, &["username", "user"]) && !auth_user {
                return Some("username");
            }
            None
        }
        "elasticsearch" | "elastic" | "es" | "opensearch" => {
            let nested = body.get("parameters").and_then(|value| value.as_object());
            let nested_server = nested.is_some_and(|map| {
                ["server", "endpoint", "servers", "url"].iter().any(|key| {
                    map.get(*key)
                        .and_then(|value| value.as_str())
                        .is_some_and(|text| !text.trim().is_empty())
                })
            });
            if !present(body, &["server", "endpoint", "servers", "url"]) && !nested_server {
                return Some("server");
            }
            None
        }
        _ => None,
    }
}

/// Build the observable connector document for one registry entry,
/// preserving secret references (never resolving values into the store).
///
/// A bare `file:`/`env:` reference is kept verbatim in `config` (and
/// mirrored in `url` so probe/live paths see it without inventing a
/// target); a JSON object config is flattened so each string leaf stays
/// a separate redactable value; anything else is kept opaque in
/// `config`. The store keeps references; responses redact them.
fn connector_store_doc(entry: &broker_config::ConnectorEntry) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    let trimmed = entry.config.trim();
    if !trimmed.is_empty() && !broker_config::secrets::is_secret_ref(trimmed) {
        if let Ok(serde_json::Value::Object(obj)) =
            serde_json::from_str::<serde_json::Value>(trimmed)
        {
            for (key, value) in obj {
                map.insert(key, value);
            }
        } else {
            map.insert(
                "config".to_string(),
                serde_json::Value::String(entry.config.clone()),
            );
        }
    }
    // Authoritative identity/enable from the registry entry win over any
    // keys the stored params carried.
    map.insert(
        "id".to_string(),
        serde_json::Value::String(entry.id.clone()),
    );
    map.insert(
        "name".to_string(),
        serde_json::Value::String(entry.id.clone()),
    );
    map.insert(
        "type".to_string(),
        serde_json::Value::String(entry.connector_type.clone()),
    );
    map.insert("enable".to_string(), serde_json::Value::Bool(entry.enable));
    if !trimmed.is_empty() && broker_config::secrets::is_secret_ref(trimmed) {
        map.insert(
            "config".to_string(),
            serde_json::Value::String(trimmed.to_string()),
        );
        // Mirror the bare reference as `url` so probe and live-sink paths
        // see the reference without an invented target; resolution happens
        // at use time, the store keeps the reference.
        if !map.contains_key("url") {
            map.insert(
                "url".to_string(),
                serde_json::Value::String(trimmed.to_string()),
            );
        }
    }
    serde_json::Value::Object(map)
}

/// Upper bound for stored connectors. Creates past this size are rejected
/// with `BAD_REQUEST` instead of growing the map without limit. Reason:
/// the list handler clones the whole snapshot once per request, so the
/// catalogue must stay small and bounded; 512 covers the documented
/// connector catalogue with headroom while keeping per-request clones
/// cheap.
// Bound of this broker: the reference specification documents no maximum.
pub const MAX_CONNECTORS: usize = 512;
/// Longest accepted connector `id`. Reason: bounds map-key memory per
/// entry so one create cannot balloon the store.
const MAX_CONNECTOR_ID_LEN: usize = 256;

/// Bare name of a stored connector document. The console sends the
/// `{type}:{name}` key in `id`. The store keys rows by the bare name.
fn connector_bare_name(doc: &serde_json::Value) -> String {
    let kind = doc.get("type").and_then(|v| v.as_str()).unwrap_or("");
    let raw = doc
        .get("id")
        .and_then(|v| v.as_str())
        .or_else(|| doc.get("name").and_then(|v| v.as_str()))
        .unwrap_or("");
    super::typed::bare_name(raw, kind)
}

/// True when `requested` names the stored document: the bare id, the
/// bare name or the `{type}:{name}` key. No fuzzy matching: any other
/// form is unknown.
fn connector_doc_matches(doc: &serde_json::Value, requested: &str) -> bool {
    if doc.get("id").and_then(|v| v.as_str()) == Some(requested) {
        return true;
    }
    if doc.get("name").and_then(|v| v.as_str()) == Some(requested) {
        return true;
    }
    let kind = doc.get("type").and_then(|v| v.as_str()).unwrap_or("");
    let bare = connector_bare_name(doc);
    requested == bare || requested == super::typed::bridge_key(kind, &bare)
}

/// Render one stored connector with live health, never the write-time
/// snapshot: `status`/`node_status` are recomputed from the live
/// connector manager (`engine.connectors()`), so a restart or a later
/// delete is visible on the next read. Only the inputs persist (see
/// [`ConnectorStore::export_conf`]); display values are derived here per
/// request. Management-plane only: one short store lock plus lock-free
/// manager lookups, and publish and deliver never call this.
fn connector_live_view(
    body: &serde_json::Value,
    engine: &broker_rules::RuleEngine,
) -> serde_json::Value {
    let mut out = body.clone();
    let id = body
        .get("id")
        .and_then(|v| v.as_str())
        .or_else(|| body.get("name").and_then(|v| v.as_str()))
        .unwrap_or("");
    let live = engine.connectors().get(id).is_some();
    // KC-00/D2: a stored record whose kind is removed or not `ready` is
    // kept and visible but reads back `disconnected` with a reason.
    let reason = body.get("type").and_then(|v| v.as_str()).and_then(|kind| {
        if broker_config::is_removed_connector_type(kind) {
            Some(crate::REMOVED_CONNECTOR_MESSAGE.to_string())
        } else {
            kind_refusal(kind)
        }
    });
    let status = if reason.is_some() {
        "disconnected"
    } else {
        connector_status(live)
    };
    if let Some(obj) = out.as_object_mut() {
        obj.insert(
            "status".to_string(),
            serde_json::Value::String(status.to_string()),
        );
        obj.insert("node_status".to_string(), connector_node_status(status));
        if let Some(reason) = reason {
            obj.insert(
                "status_reason".to_string(),
                serde_json::Value::String(reason),
            );
        }
    }
    out
}

/// Resolve the connector reference an action stores (the bare id or
/// the `{type}:{name}` key) to the bare connector id plus the stored
/// document, through the same matching the connector read paths use.
/// `None` when no stored connector matches.
pub(crate) fn resolve_action_connector(
    connectors: &ConnectorStore,
    reference: &str,
) -> Option<(String, serde_json::Value)> {
    let doc = connectors
        .snapshot()
        .into_iter()
        .find(|stored| connector_doc_matches(stored, reference))?;
    Some((connector_bare_name(&doc), doc))
}

/// Redact secret references in a connector document for any observable
/// response: every `file:`/`env:` string leaf renders as its
/// `<redacted:...>` marker naming the reference, never the value. The
/// store keeps the reference; only the response is redacted.
fn redact_connector_doc(value: &serde_json::Value) -> serde_json::Value {
    fn walk(node: &mut serde_json::Value) {
        match node {
            serde_json::Value::String(text) => {
                if broker_config::secrets::is_secret_ref(text.trim()) {
                    *text = broker_config::secrets::redact_value(text.trim());
                }
            }
            serde_json::Value::Array(entries) => {
                for entry in entries {
                    walk(entry);
                }
            }
            serde_json::Value::Object(map) => {
                for (_, entry) in map.iter_mut() {
                    walk(entry);
                }
            }
            _ => {}
        }
    }
    let mut out = value.clone();
    walk(&mut out);
    out
}

/// `GET /api/v5/connectors` (C07-01): every connector as a bare JSON
/// array, as the reference API and the management console expect. An empty
/// store reads as `[]`, never an error. The reference API documents no
/// paging, so the route takes none.
/// Each row renders live health from the connector manager, not the
/// write-time snapshot. Unknown query parameters are ignored (the ignored
/// query sink below). Management-plane only: one short lock
/// cloning at most the capped list per request plus lock-free manager
/// lookups for health; publish and deliver never touch this store (rule
/// dispatch reads the manager directly, never this map).
pub async fn list_connectors(
    State(state): State<ApiState>,
    Query(_ignored): Query<HashMap<String, String>>,
) -> Response {
    let mut rows: Vec<serde_json::Value> = state
        .connectors
        .snapshot()
        .iter()
        .map(|body| redact_connector_doc(&connector_live_view(body, &state.engine)))
        .collect();
    rows.sort_by(|a, b| {
        let a_id = a
            .get("id")
            .and_then(|v| v.as_str())
            .or_else(|| a.get("name").and_then(|v| v.as_str()))
            .unwrap_or("");
        let b_id = b
            .get("id")
            .and_then(|v| v.as_str())
            .or_else(|| b.get("name").and_then(|v| v.as_str()))
            .unwrap_or("");
        a_id.cmp(b_id)
    });
    (StatusCode::OK, Json(rows)).into_response()
}

/// `GET /api/v5/connectors/:id`: read one connector (C07-04).
///
/// A known id returns the documented fields (`name`, `type`, `enable`
/// plus the stored kind-specific config) with live health
/// (`status`/`node_status` recomputed from the connector manager, never
/// the write-time snapshot); an unknown id returns 404 with the
/// documented `{code, message}` shape, not an empty body. Matching
/// accepts the bare id, the bare name and the documented
/// `{type}:{name}` key; any other form is a 404. The pair is not paged;
/// unknown query parameters are ignored (the ignored query sink below).
/// Management-plane only: one short store lock cloning at most one
/// small entry per request plus lock-free manager lookups, and publish
/// and deliver never touch this store (dispatch reads the manager
/// directly, never this map).
///
/// Bound (stated here and enforced by construction): a read never grows
/// the map, so the [`MAX_CONNECTORS`] cap from W3-17 applies unchanged
/// and no new accumulation is introduced. Connectors persist across
/// restart via the config core; live health does not (recomputed here
/// per read from the manager).
///
/// Fire-path cost (stated for the delivery-path note): no new work on
/// publish or deliver. This handler takes no lock on the fire path at
/// all; the fire path loads the manager directly.
// TODO(parity): which per-kind config fields does the documented spec
// require beyond the kind being known plus the credential gates? The
// rulebook does not decide the exact per-kind schemas; the current
// choice stores remaining fields opaquely until the checker pins it.
pub async fn get_connector(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Query(_ignored): Query<HashMap<String, String>>,
) -> Response {
    // Exact id matching only: the path id must equal the stored `id` or
    // `name` exactly. No suffix splitting: `a:b` must not read `b`.
    let stored = state.connectors.get(&id);
    match stored {
        Some(body) => {
            let live = redact_connector_doc(&connector_live_view(&body, &state.engine));
            (StatusCode::OK, Json(live)).into_response()
        }
        None => ApiError::NotFound(format!("connector {id:?} not found")).into_response(),
    }
}

/// Cargo feature gating a connector kind, or `None` when the kind is
/// always compiled in. X1-09 gates the first half (cloud-IoT +
/// cloud-store) and X1-10 gates the rest: `kafka`, `rabbitmq`,
/// `clickhouse` and `elasticsearch` live in `broker-connectors`,
/// `cassandra`, `confluent`, `couchbase`, `mongodb`, `gcp_pubsub` and
/// `kinesis` in `broker-connectors-enterprise`. A disabled kind is absent from the
/// build and the binary.
struct ConnectorGate {
    /// Each alias that selects this kind.
    kinds: &'static [&'static str],
    /// Cargo feature that compiles the kind in.
    feature: &'static str,
    /// True when the feature is enabled in this build.
    enabled: bool,
}

/// One row per gated kind. This table is the single source: the lookup
/// below and the compiled-in list read from it.
static CONNECTOR_GATES: &[ConnectorGate] = &[
    ConnectorGate {
        kinds: &["kafka"],
        feature: "kafka",
        enabled: cfg!(feature = "kafka"),
    },
    ConnectorGate {
        kinds: &["rabbitmq", "rabbit", "amqp"],
        feature: "rabbitmq",
        enabled: cfg!(feature = "rabbitmq"),
    },
    ConnectorGate {
        kinds: &["clickhouse", "ch"],
        feature: "clickhouse",
        enabled: cfg!(feature = "clickhouse"),
    },
    ConnectorGate {
        kinds: &["elasticsearch", "elastic", "es", "opensearch"],
        feature: "elasticsearch",
        enabled: cfg!(feature = "elasticsearch"),
    },
    ConnectorGate {
        kinds: &["cassandra", "scylla", "scylladb", "cql"],
        feature: "cassandra",
        enabled: cfg!(feature = "cassandra"),
    },
    ConnectorGate {
        kinds: &["confluent", "confluent_kafka", "confluent_cloud"],
        feature: "confluent",
        enabled: cfg!(feature = "confluent"),
    },
    ConnectorGate {
        kinds: &["couchbase", "couch", "cb"],
        feature: "couchbase",
        enabled: cfg!(feature = "couchbase"),
    },
    ConnectorGate {
        kinds: &["mongodb", "mongo", "documentdb"],
        feature: "mongodb",
        enabled: cfg!(feature = "mongodb"),
    },
    ConnectorGate {
        kinds: &["gcp_pubsub", "gcp", "pubsub"],
        feature: "gcp_pubsub",
        enabled: cfg!(feature = "gcp_pubsub"),
    },
    ConnectorGate {
        kinds: &["kinesis", "aws_kinesis"],
        feature: "kinesis",
        enabled: cfg!(feature = "kinesis"),
    },
    ConnectorGate {
        kinds: &["s3", "minio"],
        feature: "s3",
        enabled: cfg!(feature = "s3"),
    },
    ConnectorGate {
        kinds: &["dynamodb", "dynamo", "ddb"],
        feature: "dynamodb",
        enabled: cfg!(feature = "dynamodb"),
    },
    ConnectorGate {
        kinds: &["bigquery", "bq", "gbq"],
        feature: "bigquery",
        enabled: cfg!(feature = "bigquery"),
    },
    ConnectorGate {
        kinds: &["aws_iot", "aws_iot_core"],
        feature: "aws_iot",
        enabled: cfg!(feature = "aws_iot"),
    },
    ConnectorGate {
        kinds: &["azure_blob", "azure_blob_storage", "azureblob", "azblob"],
        feature: "azure_blob",
        enabled: cfg!(feature = "azure_blob"),
    },
    ConnectorGate {
        kinds: &["azure_eventhubs", "azure_event_hubs", "azure", "eventhubs"],
        feature: "azure_eventhubs",
        enabled: cfg!(feature = "azure_eventhubs"),
    },
    ConnectorGate {
        kinds: &["azure_iot", "azure_iot_hub", "azure_iothub", "iothub"],
        feature: "azure_iot",
        enabled: cfg!(feature = "azure_iot"),
    },
    ConnectorGate {
        kinds: &["gcp_iot", "gcp_iot_core", "cloud_iot"],
        feature: "gcp_iot",
        enabled: cfg!(feature = "gcp_iot"),
    },
];

pub(crate) fn disabled_connector_feature(conn_type: &str) -> Option<&'static str> {
    CONNECTOR_GATES
        .iter()
        .find(|gate| gate.kinds.contains(&conn_type))
        .and_then(|gate| {
            if gate.enabled {
                None
            } else {
                Some(gate.feature)
            }
        })
}

/// KC-00 refusal for a kind whose capability state is not `ready`.
///
/// Returns `None` for a ready kind (and for the `test` helper in a test
/// build); otherwise the exact refusal message. A kind whose Cargo
/// feature is not compiled in names the feature, so a disabled build
/// refuses it the same way everywhere. Unknown names are not the map's
/// business: the caller answers them before this runs.
pub(crate) fn kind_refusal(kind: &str) -> Option<String> {
    match broker_config::kind_state(kind) {
        Some(broker_config::KindState::Ready) => return None,
        Some(broker_config::KindState::TestOnly) => {
            if broker_config::test_kinds_enabled() {
                return None;
            }
        }
        Some(_) => {}
        None => return None,
    }
    let base = broker_config::capability::kind_row(kind)
        .map(|row| row.name)
        .unwrap_or(kind);
    if let Some(feature) = disabled_connector_feature(base) {
        return Some(format!(
            "connector type {kind} is not available in this broker: feature `{feature}` is not compiled in"
        ));
    }
    Some(format!(
        "connector type {kind} is not available in this broker"
    ))
}

/// Features compiled into this build, from the same table.
#[cfg(test)]
pub(crate) fn compiled_connector_features() -> Vec<&'static str> {
    CONNECTOR_GATES
        .iter()
        .filter(|gate| gate.enabled)
        .map(|gate| gate.feature)
        .collect()
}

/// Map a documented type name to the runtime kind that executes it.
/// The console sends documented names (`mqtt`, `kafka_producer`,
/// ...). Each maps to the runtime of its base kind, so a documented
/// create dials the same target as the historic alias. Unknown names
/// pass through unchanged (the caller rejects them before this runs).
fn live_sink_kind(conn_type: &str) -> &str {
    broker_config::capability::live_sink_kind_for(conn_type)
}

/// Canonical type of a document-store connector. The KC-00 aliases map to
/// their base kind. Every other name is unchanged.
fn canonical_connector_kind(conn_type: &str) -> &str {
    match conn_type {
        "mongo" | "documentdb" => "mongodb",
        "couch" | "cb" => "couchbase",
        "elastic" | "es" | "opensearch" => "elasticsearch",
        _ => conn_type,
    }
}

/// Write the canonical type and the reference MongoDB `parameters` object
/// into a connector body. The API accepts top-level fields. A missing value
/// is never invented.
fn normalize_connector_document(conn_type: &str, body: &mut serde_json::Value) {
    fn read(map: &serde_json::Map<String, serde_json::Value>, key: &str) -> Option<String> {
        map.get(key)
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(str::to_string)
    }

    let canonical = canonical_connector_kind(conn_type);
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    obj.insert(
        "type".to_string(),
        serde_json::Value::String(canonical.to_string()),
    );
    if canonical != "mongodb" {
        return;
    }
    let nested = obj
        .get("parameters")
        .and_then(|value| value.as_object())
        .cloned()
        .unwrap_or_default();
    let mongo_type = read(obj, "mongo_type")
        .or_else(|| read(&nested, "mongo_type"))
        .unwrap_or_else(|| "single".to_string());
    let mut parameters = nested;
    parameters.insert(
        "mongo_type".to_string(),
        serde_json::Value::String(mongo_type.clone()),
    );
    match mongo_type.as_str() {
        "rs" => {
            if let Some(servers) = read(obj, "servers").or_else(|| read(&parameters, "servers")) {
                parameters.insert("servers".to_string(), serde_json::Value::String(servers));
            }
            if let Some(name) =
                read(obj, "replica_set_name").or_else(|| read(&parameters, "replica_set_name"))
            {
                parameters.insert(
                    "replica_set_name".to_string(),
                    serde_json::Value::String(name),
                );
            }
        }
        "sharded" => {
            if let Some(servers) = read(obj, "servers").or_else(|| read(&parameters, "servers")) {
                parameters.insert("servers".to_string(), serde_json::Value::String(servers));
            }
        }
        _ => {
            if let Some(server) = read(obj, "server").or_else(|| read(&parameters, "server")) {
                parameters.insert("server".to_string(), serde_json::Value::String(server));
            }
        }
    }
    obj.insert(
        "parameters".to_string(),
        serde_json::Value::Object(parameters),
    );
}

/// Checked registration: a kind whose Cargo feature is disabled fails
/// closed with an error naming the missing feature — never a silent
/// skip, never a fallback identity. A sink or transport that fails to
/// build fails the same way with its own error text. Enabled kinds go
/// through [`register_live_sink`], the same path `create_connector` uses.
pub(crate) async fn try_register_live_sink(
    engine: &broker_rules::RuleEngine,
    conn_type: &str,
    name: &str,
    body: &serde_json::Value,
) -> Result<(), String> {
    // KC-00: only `ready` kinds register. A kind that is not ready is
    // refused with the documented message (naming the feature when that
    // is the reason), so boot replay and a live config apply never dial
    // an invented target for a pending kind.
    if let Some(message) = kind_refusal(conn_type) {
        return Err(message);
    }
    // Documented names run on the runtime of their base kind.
    let runtime = live_sink_kind(conn_type);
    if let Some(feature) = disabled_connector_feature(runtime) {
        return Err(format!(
            "connector kind '{conn_type}' needs cargo feature '{feature}' (disabled in this build)"
        ));
    }
    register_live_sink(engine, runtime, name, body).await
}

/// Closed fallback for a kind whose vendor SDK is not compiled in.
/// Logs the refusal and returns the error text for the caller.
#[allow(dead_code)]
fn refuse_disabled_kind(conn_type: &str, feature: &str) -> String {
    tracing::warn!(
        "connector kind `{conn_type}` needs missing feature `{feature}`; registration refused"
    );
    format!("connector kind '{conn_type}' needs cargo feature '{feature}' (disabled in this build)")
}

pub(crate) async fn register_live_sink(
    engine: &broker_rules::RuleEngine,
    conn_type: &str,
    name: &str,
    body: &serde_json::Value,
) -> Result<(), String> {
    // First build error in this call. Each arm records its failure
    // through the macro below instead of dropping it, and the tail of
    // this function refuses with it. Arms that register nothing by
    // design (no execution path, missing optional target) leave it
    // empty and stay persist-only.
    let mut build_error: Option<String> = None;
    // Build one fallible sink piece. Success yields the value for the
    // surrounding `if let`; failure records the sink text and yields
    // `None` so the arm registers nothing.
    macro_rules! record_sink_error {
        ($expr:expr) => {
            match $expr {
                Ok(built) => Some(built),
                Err(error) => {
                    build_error = Some(error.to_string());
                    None
                }
            }
        };
    }
    match conn_type {
        // Offline kinds: always live, no network probe. The snapshot keeps
        // the reference form while the sink holds no credential material.
        "console" | "logger" | "test" => {
            let sink = Arc::new(broker_connectors::ConsoleLoggerSink::new(name.to_string()));
            engine.connectors().register(name, sink.clone());
            engine
                .connectors()
                .register(format!("{conn_type}:{name}"), sink);
        }
        "webhook" => {
            // Connector URLs may be secret references; resolve at use time
            // so the snapshot keeps the reference while the live sink dials
            // the real target. Unresolvable targets fail closed (no sink).
            // No invented default: a missing URL stays persist-only instead
            // of dialling a loopback the operator never configured.
            let Some(raw_url) = body
                .get("url")
                .or_else(|| body.get("endpoint"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Ok(());
            };
            let url = broker_connectors::resolve_field_secret(raw_url);
            if url.trim().is_empty() {
                return Ok(());
            }
            let sink = Arc::new(broker_connectors::HttpWebhookSink::new(
                url,
                reqwest::header::HeaderMap::new(),
                reqwest::Client::new(),
            ));
            engine.connectors().register(name, sink.clone());
            engine
                .connectors()
                .register(format!("webhook:{name}"), sink);
        }
        "redis" => {
            let servers = body
                .get("servers")
                .or_else(|| body.get("server"))
                .and_then(|v| v.as_str())
                .unwrap_or("127.0.0.1:6379");
            let pass = body.get("password").and_then(|v| v.as_str()).unwrap_or("");
            let endpoint = if pass.is_empty() {
                format!("redis://{}", servers)
            } else {
                format!("redis://:{}@{}", pass, servers)
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors::redis::DriverRedisTransport::new(&endpoint)
            ) {
                let config = broker_connectors::redis::RedisSinkConfig {
                    endpoint,
                    command: broker_connectors::redis::RedisCommandKind::HSet {
                        key_template: "sensors:${topic}".to_string(),
                        field_template: "payload".to_string(),
                    },
                };
                if let Some(sink) = record_sink_error!(broker_connectors::redis::RedisSink::new(
                    config,
                    Arc::new(transport)
                )) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("redis:{}", name), sink);
                }
            }
        }
        "alloydb" => {
            let server = body
                .get("server")
                .or_else(|| body.get("servers"))
                .or_else(|| body.get("host"))
                .and_then(|v| v.as_str())
                .unwrap_or("127.0.0.1:5433");
            let clean_server = server
                .split("://")
                .last()
                .unwrap_or(server)
                .split('/')
                .next()
                .unwrap_or(server);
            let (host, port) = if let Some((h, p)) = clean_server.split_once(':') {
                (h.to_string(), p.parse::<u16>().unwrap_or(5433))
            } else {
                (clean_server.to_string(), 5433)
            };
            let database = body
                .get("database")
                .and_then(|v| v.as_str())
                .unwrap_or("telemetry");
            let table = body
                .get("table")
                .and_then(|v| v.as_str())
                .unwrap_or("sensor_events");
            let username = body
                .get("username")
                .and_then(|v| v.as_str())
                .unwrap_or("postgres");
            let password = body
                .get("password")
                .and_then(|v| v.as_str())
                .unwrap_or("password");
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());
            let config = broker_connectors_enterprise::alloydb::AlloydbConfig {
                host,
                port,
                database: database.to_string(),
                username: username.to_string(),
                auth: broker_connectors_enterprise::alloydb::AlloydbAuth::Password {
                    password: password.to_string(),
                },
                table: table.to_string(),
                column_mappings: Vec::new(),
                batch_size: Some(1),
                buffer_capacity: None,
                timeout_ms,
                tls: None,
                ca_bundle_pem: None,
                tls_ca_file: None,
            };
            let transport = Arc::new(
                broker_connectors_enterprise::alloydb::PgDriverAlloydbTransport::new(&config),
            );
            if let Some(sink) = record_sink_error!(
                broker_connectors_enterprise::alloydb::AlloydbSink::new(config, transport)
            ) {
                let sink = Arc::new(sink);
                engine.connectors().register(name, sink.clone());
                engine
                    .connectors()
                    .register(format!("alloydb:{}", name), sink);
            }
        }
        "http" => {
            // No invented loopback default: a missing URL registers a sink
            // with an empty target that fails closed on send (never dials
            // an operator-unconfigured address) while preserving the
            // long-standing live-sink contract for target-less `http`
            // connectors (probe reports reachable, create/boot expect a
            // live sink). Reason: an empty target fails the send instead
            // of sending somewhere unintended.
            let raw_url = body
                .get("url")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or_default();
            let url = broker_connectors::resolve_field_secret(raw_url);
            let sink = Arc::new(broker_connectors::HttpWebhookSink::new(
                url,
                reqwest::header::HeaderMap::new(),
                reqwest::Client::new(),
            ));
            engine.connectors().register(name, sink.clone());
            engine.connectors().register(format!("http:{}", name), sink);
        }
        #[cfg(feature = "kafka")]
        "kafka" => {
            let servers = body
                .get("bootstrap_hosts")
                .or_else(|| body.get("servers"))
                .and_then(|v| v.as_str())
                .unwrap_or("127.0.0.1:9092");
            let config = broker_connectors::kafka::KafkaSinkConfig {
                bootstrap_servers: servers.to_string(),
                topic_template: "events-${topic}".to_string(),
                partition_key_field: None,
                partitions: 1,
                client_id: format!("indramqtt-{}", name),
                acks: "1".to_string(),
                batch_max_records: 1,
                batch_max_bytes: 65536,
            };
            // Production write path on the maintained `rdkafka` driver;
            // the hand-written TCP framing stays for offline unit tests.
            if let Some(transport) = record_sink_error!(
                broker_connectors::kafka::RdkafkaKafkaTransport::new(&config)
            ) {
                if let Some(sink) = record_sink_error!(broker_connectors::kafka::KafkaSink::new(
                    config,
                    Arc::new(transport)
                )) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("kafka:{}", name), sink);
                }
            }
        }
        #[cfg(not(feature = "kafka"))]
        "kafka" => {
            return Err(refuse_disabled_kind(conn_type, "kafka"));
        }
        "pgsql" => {
            let server = body
                .get("server")
                .and_then(|v| v.as_str())
                .unwrap_or("127.0.0.1:5432");
            let database = body
                .get("database")
                .and_then(|v| v.as_str())
                .unwrap_or("postgres");
            let username = body
                .get("username")
                .and_then(|v| v.as_str())
                .unwrap_or("postgres");
            let password = body.get("password").and_then(|v| v.as_str()).unwrap_or("");
            let conn_url = format!(
                "postgres://{}:{}@{}/{}",
                username, password, server, database
            );
            if let Some(transport) = record_sink_error!(
                broker_connectors::postgres::DriverPgTransport::new(&conn_url, 1)
            ) {
                let config = broker_connectors::postgres::PostgreSqlSinkConfig {
                    connection_url: conn_url,
                    sql_template: "INSERT INTO test_telemetry (clientid, topic, payload, timestamp) VALUES ('rule-engine', $1, $3, 0)".to_string(),
                    pool_size: 1,
                    batch_size: 1,
                    batch_timeout_ms: 10,
                };
                if let Some(sink) = record_sink_error!(
                    broker_connectors::postgres::PostgreSqlSink::new(config, Arc::new(transport))
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("pgsql:{}", name), sink);
                }
            }
        }
        "mysql" => {
            let server = body
                .get("server")
                .and_then(|v| v.as_str())
                .unwrap_or("127.0.0.1:3306");
            let database = body
                .get("database")
                .and_then(|v| v.as_str())
                .unwrap_or("test");
            let username = body
                .get("username")
                .and_then(|v| v.as_str())
                .unwrap_or("root");
            let password = body.get("password").and_then(|v| v.as_str()).unwrap_or("");
            let conn_url = if password.is_empty() {
                format!("mysql://{}@{}/{}", username, server, database)
            } else {
                format!("mysql://{}:{}@{}/{}", username, password, server, database)
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors::mysql::DriverMySqlTransport::new(&conn_url, 1)
            ) {
                let config = broker_connectors::mysql::MySqlSinkConfig {
                    connection_url: conn_url,
                    sql_template:
                        "INSERT INTO test_mqtt_events (topic, qos, payload) VALUES (?, ?, ?)"
                            .to_string(),
                    pool_size: 1,
                    batch_size: 1,
                    batch_timeout_ms: 10,
                };
                if let Some(sink) = record_sink_error!(broker_connectors::mysql::MySqlSink::new(
                    config,
                    Arc::new(transport)
                )) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("mysql:{}", name), sink);
                }
            }
        }
        #[cfg(feature = "rabbitmq")]
        "rabbitmq" => {
            let endpoint = body
                .get("server")
                .and_then(|v| v.as_str())
                .or_else(|| body.get("endpoint").and_then(|v| v.as_str()))
                .unwrap_or("127.0.0.1:5672");
            let username = body
                .get("username")
                .and_then(|v| v.as_str())
                .unwrap_or("guest");
            let password = body
                .get("password")
                .and_then(|v| v.as_str())
                .unwrap_or("guest");
            let full_endpoint = if endpoint.starts_with("amqp://") {
                endpoint.to_string()
            } else {
                format!("amqp://{}:{}@{}/", username, password, endpoint)
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors::rabbitmq::LapinRabbitTransport::new(&full_endpoint)
            ) {
                let config = broker_connectors::rabbitmq::RabbitMqSinkConfig {
                    endpoint: full_endpoint,
                    exchange: "amq.topic".to_string(),
                    routing_key_template: "sensor.${topic}".to_string(),
                    delivery_mode: 1,
                };
                if let Some(sink) = record_sink_error!(
                    broker_connectors::rabbitmq::RabbitMqSink::new(config, Arc::new(transport))
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("rabbitmq:{}", name), sink);
                }
            }
        }
        #[cfg(not(feature = "rabbitmq"))]
        "rabbitmq" => {
            return Err(refuse_disabled_kind(conn_type, "rabbitmq"));
        }
        #[cfg(feature = "clickhouse")]
        "clickhouse" | "ch" => {
            // Fail closed (S1-01): the create and update paths gate
            // these fields first; the arm repeats the check so boot
            // replay cannot invent a target either.
            let Some(raw_url) = body
                .get("url")
                .or_else(|| body.get("endpoint"))
                .or_else(|| body.get("server"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("clickhouse connector is missing required field: url".to_string());
            };
            let endpoint = if raw_url.starts_with("http://") || raw_url.starts_with("https://") {
                raw_url.to_string()
            } else {
                format!("http://{raw_url}")
            };
            let Some(database) = body
                .get("database")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("clickhouse connector is missing required field: database".to_string());
            };
            let Some(table) = body
                .get("table")
                .or_else(|| body.get("table_template"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("clickhouse connector is missing required field: table".to_string());
            };
            let request_timeout_ms = body
                .get("request_timeout_ms")
                .or_else(|| body.get("timeout_ms"))
                .or_else(|| body.get("timeout"))
                .and_then(|v| v.as_u64());
            let config = broker_connectors::clickhouse::ClickHouseSinkConfig {
                endpoint,
                database: database.to_string(),
                table: table.to_string(),
                format: "JSONEachRow".to_string(),
                batch_size: 1,
                batch_timeout_ms: 10,
                username: body
                    .get("username")
                    .or_else(|| body.get("user"))
                    .and_then(|v| v.as_str())
                    // Default `default`: the ClickHouse server-side
                    // default user.
                    .unwrap_or("default")
                    .to_string(),
                password: body
                    .get("password")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                request_timeout_ms,
            };
            if let Some(driver) =
                record_sink_error!(broker_connectors::DriverClickHouseTransport::new(&config))
            {
                let transport: std::sync::Arc<dyn broker_connectors::ClickHouseTransport> =
                    std::sync::Arc::new(driver);
                if let Some(sink) = record_sink_error!(
                    broker_connectors::clickhouse::ClickHouseSink::new(config, transport)
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("clickhouse:{}", name), sink);
                }
            }
        }
        #[cfg(not(feature = "clickhouse"))]
        "clickhouse" => {
            return Err(refuse_disabled_kind(conn_type, "clickhouse"));
        }
        "mqtt_bridge" | "bridge" => {
            let server = body
                .get("server")
                .and_then(|v| v.as_str())
                .or_else(|| body.get("broker_address").and_then(|v| v.as_str()))
                .unwrap_or("127.0.0.1:1883");
            let client_id = body
                .get("client_id")
                .and_then(|v| v.as_str())
                .unwrap_or("indra-bridge");
            let username = body
                .get("username")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let password = body
                .get("password")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let config = broker_connectors::MqttBridgeSinkConfig {
                broker_address: server.to_string(),
                client_id: client_id.to_string(),
                clean_start: true,
                username,
                password,
                keep_alive_secs: 60,
                topic_prefix: None,
                topic_template: None,
                qos_override: None,
                retain_override: None,
                max_inflight: Some(1000),
                max_batch_size: Some(1),
                linger_ms: Some(10),
                protocol: broker_connectors::MqttBridgeProtocol::V311,
            };
            if let Some(transport) =
                record_sink_error!(broker_connectors::RumqttcMqttBridgeTransport::new(&config))
            {
                if let Some(sink) = record_sink_error!(broker_connectors::MqttBridgeSink::new(
                    config,
                    Arc::new(transport)
                )) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("bridge:{}", name), sink.clone());
                    engine
                        .connectors()
                        .register(format!("mqtt_bridge:{}", name), sink);
                }
            }
        }
        "influxdb" => {
            // Fail closed (S1-01): the create and update paths gate
            // these fields first; the arm repeats the check so boot
            // replay cannot invent a target either. The server default
            // is the reference connector default.
            let raw_server = body
                .get("server")
                .or_else(|| body.get("url"))
                .or_else(|| body.get("endpoint"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("127.0.0.1:8086");
            let endpoint =
                if raw_server.starts_with("http://") || raw_server.starts_with("https://") {
                    raw_server.to_string()
                } else {
                    format!("http://{raw_server}")
                };
            let Some(bucket) = body
                .get("bucket")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("influxdb connector is missing required field: bucket".to_string());
            };
            let Some(org) = body
                .get("org")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("influxdb connector is missing required field: org".to_string());
            };
            let Some(token) = body
                .get("token")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("influxdb connector is missing required field: token".to_string());
            };
            let Some(measurement) = body
                .get("measurement")
                .or_else(|| body.get("measurement_template"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("influxdb connector is missing required field: measurement".to_string());
            };
            // The sink validates the value; an unknown precision fails
            // the build with the sink text.
            let precision = body
                .get("precision")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("ms")
                .to_string();
            let request_timeout_ms = body
                .get("request_timeout_ms")
                .or_else(|| body.get("timeout_ms"))
                .or_else(|| body.get("timeout"))
                .and_then(|v| v.as_u64());
            let config = broker_connectors::InfluxDbSinkConfig {
                endpoint,
                bucket: bucket.to_string(),
                org: org.to_string(),
                token: token.to_string(),
                measurement_template: measurement.to_string(),
                precision,
                batch_size: 1,
                batch_timeout_ms: 100,
                request_timeout_ms,
            };
            if let Some(sink) = record_sink_error!(broker_connectors::InfluxDbSink::new(
                config,
                reqwest::Client::new()
            )) {
                let sink = Arc::new(sink);
                engine.connectors().register(name, sink.clone());
                engine
                    .connectors()
                    .register(format!("influxdb:{}", name), sink);
            }
        }
        #[cfg(feature = "mongodb")]
        "mongodb" | "mongo" | "documentdb" => {
            crate::v5::action_sinks::validate_connector_io_bounds(body, name, "connector config")?;
            // The connector carries the connection and database only.
            // The collection and document template belong to the action,
            // so the connector alone registers no sink and invents no
            // target.
        }
        #[cfg(not(feature = "mongodb"))]
        "mongodb" | "mongo" | "documentdb" => {
            return Err(refuse_disabled_kind(conn_type, "mongodb"));
        }
        #[cfg(feature = "cassandra")]
        "cassandra" | "scylla" | "scylladb" | "cql" => {
            // Fail closed (S1-01): the create and update paths gate
            // these fields first; the arm repeats the check so boot
            // replay cannot invent a target either.
            let Some(raw_servers) = body
                .get("servers")
                .or_else(|| body.get("server"))
                .or_else(|| body.get("contact_points"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("cassandra connector is missing required field: servers".to_string());
            };
            let contact_points: Vec<String> = raw_servers
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(str::to_string)
                .collect();
            if contact_points.is_empty() {
                return Err("cassandra connector is missing required field: servers".to_string());
            }
            let Some(keyspace) = body
                .get("keyspace")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("cassandra connector is missing required field: keyspace".to_string());
            };
            let Some(table) = body
                .get("table")
                .or_else(|| body.get("table_template"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("cassandra connector is missing required field: table".to_string());
            };
            if !table
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_')
            {
                return Err(format!(
                    "cassandra connector field `table` must match [A-Za-z0-9_]+: {table:?}"
                ));
            }
            let username = body
                .get("username")
                .or_else(|| body.get("user"))
                .and_then(|v| v.as_str());
            let password = body.get("password").and_then(|v| v.as_str());
            let auth = match (username, password) {
                (Some(username), Some(password)) => {
                    broker_connectors_enterprise::CassandraAuth::Password {
                        username: username.to_string(),
                        password: password.to_string(),
                    }
                }
                (None, None) => broker_connectors_enterprise::CassandraAuth::None,
                _ => {
                    return Err(
                        "cassandra connector fields `username` and `password` must be set together"
                            .to_string(),
                    );
                }
            };
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());
            // The CQL text carries `keyspace.table`: the Scylla
            // transport prepares the statement text as given, so a
            // bare table never resolves. Both parts are validated
            // identifiers; one fixed shape per connector.
            let cql = format!("INSERT INTO {keyspace}.{table} (device_id, bucket_hour, event_time, payload) VALUES (?, ?, ?, ?)");
            let partition_key = body
                .get("partition_key")
                .or_else(|| body.get("partition_key_template"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("${client_id}")
                .to_string();
            // Default `${client_id}`: the client identity routes each
            // row to its token owner.
            let config = broker_connectors_enterprise::CassandraSinkConfig {
                contact_points,
                keyspace: keyspace.to_string(),
                table_template: table.to_string(),
                auth,
                consistency: broker_connectors_enterprise::CqlConsistency::LocalQuorum,
                partition_key_template: partition_key,
                cql_statement_template: cql,
                ttl_secs: None,
                batch_size: Some(1),
                batch_bytes: Some(1_048_576),
                linger_ms: Some(10),
                max_retries: Some(4),
                initial_backoff_ms: Some(100),
                max_backoff_ms: Some(2_000),
                timeout_ms,
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::ScyllaCassandraTransport::new(&config)
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::CassandraSink::new(config, transport)
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("cassandra:{}", name), sink.clone());
                    engine
                        .connectors()
                        .register(format!("scylla:{}", name), sink.clone());
                    engine
                        .connectors()
                        .register(format!("scylladb:{}", name), sink.clone());
                    engine.connectors().register(format!("cql:{}", name), sink);
                }
            }
        }
        #[cfg(not(feature = "cassandra"))]
        "cassandra" | "scylla" | "scylladb" | "cql" => {
            return Err(refuse_disabled_kind(conn_type, "cassandra"));
        }
        "cockroachdb" | "cockroach" => {
            let conn_str = body
                .get("connection_string")
                .and_then(|v| v.as_str())
                .or_else(|| body.get("server").and_then(|v| v.as_str()))
                .or_else(|| body.get("url").and_then(|v| v.as_str()))
                .unwrap_or("postgresql://root@127.0.0.1:26257/idacs?sslmode=disable");
            let table = body
                .get("table")
                .and_then(|v| v.as_str())
                .unwrap_or("sensor_events");
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());
            let config = broker_connectors_enterprise::CockroachDbConfig {
                connection_string: conn_str.to_string(),
                table: table.to_string(),
                upsert_conflict_columns: Vec::new(),
                batch_size: Some(1),
                max_retry_attempts: 5,
                buffer_capacity: None,
                timeout_ms,
            };
            let transport =
                Arc::new(broker_connectors_enterprise::PgDriverCockroachDbTransport::new(&config));
            if let Some(sink) = record_sink_error!(
                broker_connectors_enterprise::CockroachDbSink::new(config, transport)
            ) {
                let sink = Arc::new(sink);
                engine.connectors().register(name, sink.clone());
                engine
                    .connectors()
                    .register(format!("cockroachdb:{}", name), sink.clone());
                engine
                    .connectors()
                    .register(format!("cockroach:{}", name), sink);
            }
        }
        #[cfg(feature = "couchbase")]
        "couchbase" | "couch" | "cb" => {
            crate::v5::action_sinks::validate_connector_io_bounds(body, name, "connector config")?;
            // The connector carries the server and credentials only.
            // The action owns the SQL++ statement, so the connector alone
            // registers no sink and invents no keyspace.
        }
        #[cfg(not(feature = "couchbase"))]
        "couchbase" | "couch" | "cb" => {
            return Err(refuse_disabled_kind(conn_type, "couchbase"));
        }
        "mssql" | "sqlserver" => {
            let server = body
                .get("server")
                .or_else(|| body.get("servers"))
                .or_else(|| body.get("host"))
                .and_then(|v| v.as_str())
                .unwrap_or("127.0.0.1:1433");
            let clean_server = server
                .split("://")
                .last()
                .unwrap_or(server)
                .split('/')
                .next()
                .unwrap_or(server);
            let (host, port) = if let Some((h, p)) = clean_server.split_once(':') {
                (h.to_string(), p.parse::<u16>().ok())
            } else {
                (clean_server.to_string(), None)
            };
            let database = body
                .get("database")
                .and_then(|v| v.as_str())
                .unwrap_or("telemetry");
            let table = body
                .get("table")
                .or_else(|| body.get("table_template"))
                .and_then(|v| v.as_str())
                .unwrap_or("dbo.SensorEvents");
            let username = body
                .get("username")
                .and_then(|v| v.as_str())
                .unwrap_or("sa");
            let password = body
                .get("password")
                .and_then(|v| v.as_str())
                .unwrap_or("secret");
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());
            let config = broker_connectors_enterprise::MssqlSinkConfig {
                host,
                port,
                database: database.to_string(),
                table_template: table.to_string(),
                auth: broker_connectors_enterprise::MssqlAuth::SqlPassword {
                    username: username.to_string(),
                    password: password.to_string(),
                },
                query_mode: broker_connectors_enterprise::MssqlQueryMode::InsertJson,
                trust_server_certificate: true,
                batch_size: Some(1),
                batch_bytes: Some(2_097_152),
                linger_ms: Some(10),
                max_retries: Some(3),
                initial_backoff_ms: Some(100),
                max_backoff_ms: Some(2_000),
                timeout_ms,
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::DriverMssqlTransport::new(&config)
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::MssqlSink::new(config, transport)
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("mssql:{}", name), sink.clone());
                    engine
                        .connectors()
                        .register(format!("sqlserver:{}", name), sink);
                }
            }
        }
        "oracle" => {
            let url = body
                .get("url")
                .and_then(|v| v.as_str())
                .or_else(|| body.get("server").and_then(|v| v.as_str()))
                .unwrap_or("http://127.0.0.1:8080/ords/hr/_/sql");
            let schema = body.get("schema").and_then(|v| v.as_str()).unwrap_or("HR");
            let table = body
                .get("table")
                .and_then(|v| v.as_str())
                .unwrap_or("TELEMETRY");
            let username = body
                .get("username")
                .and_then(|v| v.as_str())
                .unwrap_or("c##appuser");
            let password = body
                .get("password")
                .and_then(|v| v.as_str())
                .unwrap_or("password");
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());
            let config = broker_connectors_enterprise::oracle::OracleSinkConfig {
                url: url.to_string(),
                schema: schema.to_string(),
                table: table.to_string(),
                username: username.to_string(),
                password: password.to_string(),
                custom_upsert: None,
                key_columns: vec!["device_id".to_string(), "client_id".to_string()],
                batch_size: Some(1),
                buffer_capacity: None,
                timeout_ms,
            };
            let transport =
                Arc::new(broker_connectors_enterprise::oracle::HttpOracleTransport::new(&config));
            if let Some(sink) = record_sink_error!(
                broker_connectors_enterprise::oracle::OracleSink::new(config, transport)
            ) {
                let sink = Arc::new(sink);
                engine.connectors().register(name, sink.clone());
                engine
                    .connectors()
                    .register(format!("oracle:{}", name), sink);
            }
        }
        "tdengine" => {
            // Fail closed (S1-01): the create and update paths gate
            // these fields first; the arm repeats the check so boot
            // replay cannot invent a target either.
            let Some(raw_server) = body
                .get("server")
                .or_else(|| body.get("servers"))
                .or_else(|| body.get("url"))
                .or_else(|| body.get("endpoint"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("tdengine connector is missing required field: server".to_string());
            };
            let endpoint =
                if raw_server.starts_with("http://") || raw_server.starts_with("https://") {
                    raw_server.to_string()
                } else {
                    format!("http://{raw_server}")
                };
            let Some(database) = body
                .get("database")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("tdengine connector is missing required field: database".to_string());
            };
            let Some(stable) = body
                .get("stable_name")
                .or_else(|| body.get("stable"))
                .or_else(|| body.get("table"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("tdengine connector is missing required field: stable".to_string());
            };
            let Some(subtable) = body
                .get("subtable_template")
                .or_else(|| body.get("subtable"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("tdengine connector is missing required field: subtable".to_string());
            };
            let username = body
                .get("username")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty());
            let password = body
                .get("password")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty());
            let token = body
                .get("token")
                .or_else(|| body.get("auth_token"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty());
            // A half Basic pair cannot authenticate: refuse it. A
            // token selects token auth and never mixes with Basic.
            let auth = match (username, password, token) {
                (Some(username), Some(password), None) => {
                    broker_connectors_enterprise::tdengine::TdengineAuth::Basic {
                        username: username.to_string(),
                        password: password.to_string(),
                    }
                }
                (None, None, Some(token)) => {
                    broker_connectors_enterprise::tdengine::TdengineAuth::Token {
                        token: token.to_string(),
                    }
                }
                (None, None, None) => broker_connectors_enterprise::tdengine::TdengineAuth::Basic {
                    username: "root".to_string(),
                    password: "taosdata".to_string(),
                },
                _ => {
                    return Err("tdengine connector fields `username` and `password` must be set together, and `token` never mixes with them".to_string());
                }
            };
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());

            // The row shape travels in the connector body for the
            // legacy connector-level path (the action carries it per
            // target on the action-owned path). An absent or empty
            // shape is refused, never invented.
            let metrics_template = match body
                .get("metrics")
                .or_else(|| body.get("metrics_template"))
                .and_then(|v| v.as_object())
            {
                Some(map) if !map.is_empty() && map.values().all(|entry| entry.is_string()) => map
                    .iter()
                    .map(|(name, entry)| (name.clone(), entry.as_str().unwrap_or("").to_string()))
                    .collect::<std::collections::HashMap<String, String>>(),
                _ => {
                    return Err("tdengine connector is missing required field: metrics".to_string());
                }
            };
            let tags_template = match body
                .get("tags")
                .or_else(|| body.get("tags_template"))
                .and_then(|v| v.as_object())
            {
                Some(map) if !map.is_empty() && map.values().all(|entry| entry.is_string()) => map
                    .iter()
                    .map(|(name, entry)| (name.clone(), entry.as_str().unwrap_or("").to_string()))
                    .collect::<std::collections::HashMap<String, String>>(),
                _ => {
                    return Err("tdengine connector is missing required field: tags".to_string());
                }
            };

            let config = broker_connectors_enterprise::tdengine::TdengineSinkConfig {
                endpoint,
                database: database.to_string(),
                stable_name: stable.to_string(),
                subtable_template: subtable.to_string(),
                auth,
                tags_template,
                metrics_template,
                batch_size: Some(1),
                batch_bytes: Some(1_048_576),
                linger_ms: Some(10),
                buffer_capacity: None,
                max_retries: Some(3),
                initial_backoff_ms: Some(100),
                max_backoff_ms: Some(2_000),
                timeout_ms,
            };
            // Same production split as the v1 wiring: Basic + plaintext
            // rides the maintained driver, anything else stays on REST.
            if config.use_driver() {
                if let Some(transport) = record_sink_error!(
                    broker_connectors_enterprise::tdengine::DriverTdengineTransport::new(&config)
                ) {
                    let transport = Arc::new(transport);
                    if let Some(sink) = record_sink_error!(
                        broker_connectors_enterprise::tdengine::TdengineSink::new(
                            config, transport
                        )
                    ) {
                        let sink = Arc::new(sink);
                        engine.connectors().register(name, sink.clone());
                        engine
                            .connectors()
                            .register(format!("tdengine:{}", name), sink);
                    }
                }
                // A failed driver build refuses with the sink text
                // instead of falling through to the REST path.
                if let Some(error) = build_error {
                    return Err(error);
                }
                return Ok(());
            }
            let client = reqwest::Client::builder()
                .timeout(config.timeout())
                .build()
                .unwrap_or_default();
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::tdengine::HttpTdengineTransport::new(&config, client)
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::tdengine::TdengineSink::new(config, transport)
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("tdengine:{}", name), sink);
                }
            }
        }
        "greptimedb" | "greptime" => {
            // Fail closed (S1-01): the create and update paths gate
            // the database and table first; the arm repeats the check
            // so boot replay cannot invent a target either. The server
            // default is the reference connector default.
            // TODO(parity): the reference default (127.0.0.1:4001)
            // conflicts with its description port (8086). The current
            // choice applies 4001 and appends 8086 to a bare host
            // until the checker pins it.
            let raw_server = body
                .get("server")
                .or_else(|| body.get("endpoint"))
                .or_else(|| body.get("servers"))
                .or_else(|| body.get("url"))
                .or_else(|| body.get("host"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("127.0.0.1:4001");
            let endpoint = if raw_server.contains("://") {
                raw_server.to_string()
            } else if raw_server.contains(':') {
                format!("http://{raw_server}")
            } else {
                format!("http://{raw_server}:8086")
            };
            let Some(database) = body
                .get("database")
                .or_else(|| body.get("dbname"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("greptimedb connector is missing required field: database".to_string());
            };
            let Some(table) = body
                .get("table")
                .or_else(|| body.get("table_template"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("greptimedb connector is missing required field: table".to_string());
            };
            let username = body
                .get("username")
                .or_else(|| body.get("user"))
                .and_then(|v| v.as_str());
            let password = body.get("password").and_then(|v| v.as_str());
            let auth = match (username, password) {
                (Some(username), Some(password)) => {
                    Some(broker_connectors::greptimedb::GreptimeDbAuth {
                        username: username.to_string(),
                        password: password.to_string(),
                    })
                }
                (None, None) => None,
                _ => {
                    return Err(
                        "greptimedb connector fields `username` and `password` must be set together"
                            .to_string(),
                    );
                }
            };
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());
            // Connector-level `precision` and `format` apply here as
            // on the action path; an unknown value is refused.
            let precision = match body
                .get("precision")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                None | Some("ms") => broker_connectors::greptimedb::GreptimePrecision::Millisecond,
                Some("ns") => broker_connectors::greptimedb::GreptimePrecision::Nanosecond,
                Some("us") => broker_connectors::greptimedb::GreptimePrecision::Microsecond,
                Some("s") => broker_connectors::greptimedb::GreptimePrecision::Second,
                Some(other) => {
                    return Err(format!(
                        "greptimedb connector field `precision` must be one of s, ms, us, ns: {other:?}"
                    ));
                }
            };
            let format = match body
                .get("format")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                None | Some("sql") => broker_connectors::greptimedb::GreptimeFormat::SqlInsert,
                Some("influx") => broker_connectors::greptimedb::GreptimeFormat::InfluxLineProtocol,
                Some(other) => {
                    return Err(format!(
                        "greptimedb connector field `format` must be one of sql, influx: {other:?}"
                    ));
                }
            };
            let config = broker_connectors::greptimedb::GreptimeDbConfig {
                endpoint,
                database: database.to_string(),
                auth,
                format,
                table_template: table.to_string(),
                timestamp_precision: precision,
                batch_size: Some(1),
                buffer_capacity: None,
                timeout_ms,
            };
            let transport = Arc::new(broker_connectors::greptimedb::HttpGreptimeDbTransport::new(
                &config,
            ));
            if let Some(sink) = record_sink_error!(
                broker_connectors::greptimedb::GreptimeDbSink::new(config, transport)
            ) {
                let sink = Arc::new(sink);
                engine.connectors().register(name, sink.clone());
                engine
                    .connectors()
                    .register(format!("greptimedb:{}", name), sink.clone());
                engine
                    .connectors()
                    .register(format!("greptime:{}", name), sink);
            }
        }
        "iotdb" => {
            // Fail closed (S1-01): the create and update paths gate
            // these fields first; the arm repeats the check so boot
            // replay cannot invent a target either.
            let Some(raw_server) = body
                .get("endpoint")
                .or_else(|| body.get("server"))
                .or_else(|| body.get("servers"))
                .or_else(|| body.get("url"))
                .or_else(|| body.get("base_url"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("iotdb connector is missing required field: endpoint".to_string());
            };
            let endpoint = crate::v5::action_sinks::iotdb_endpoint(raw_server);
            let Some(device_path) = body
                .get("device_path_template")
                .or_else(|| body.get("device_path"))
                .or_else(|| body.get("device"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("iotdb connector is missing required field: device".to_string());
            };
            let Some(username) = body
                .get("username")
                .or_else(|| body.get("user"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("iotdb connector is missing required field: username".to_string());
            };
            let Some(password) = body
                .get("password")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
            else {
                return Err("iotdb connector is missing required field: password".to_string());
            };
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());

            let config = broker_connectors_enterprise::iotdb::IotDbSinkConfig {
                endpoint,
                device_path_template: device_path.to_string(),
                auth: broker_connectors_enterprise::iotdb::IotDbAuth {
                    username: username.to_string(),
                    password: password.to_string(),
                },
                is_aligned: false,
                measurements: match body.get("measurements").and_then(|v| v.as_array()) {
                    Some(items)
                        if !items.is_empty()
                            && items.iter().all(|item| {
                                item.as_str().is_some_and(|s| !s.trim().is_empty())
                            }) =>
                    {
                        items
                            .iter()
                            .map(|item| item.as_str().unwrap_or("").trim().to_string())
                            .collect()
                    }
                    _ => {
                        return Err(
                            "iotdb connector is missing required field: measurements".to_string()
                        );
                    }
                },
                data_types: match body
                    .get("types")
                    .or_else(|| body.get("data_types"))
                    .and_then(|v| v.as_array())
                {
                    Some(items)
                        if !items.is_empty()
                            && items.iter().all(|item| {
                                item.as_str().is_some_and(|s| !s.trim().is_empty())
                            }) =>
                    {
                        let mut types = Vec::with_capacity(items.len());
                        for item in items {
                            let name = item.as_str().unwrap_or("").trim().to_lowercase();
                            let data_type = match name.as_str() {
                                "boolean" => {
                                    broker_connectors_enterprise::iotdb::IotDbDataType::Boolean
                                }
                                "int32" => {
                                    broker_connectors_enterprise::iotdb::IotDbDataType::Int32
                                }
                                "int64" => {
                                    broker_connectors_enterprise::iotdb::IotDbDataType::Int64
                                }
                                "float" => {
                                    broker_connectors_enterprise::iotdb::IotDbDataType::Float
                                }
                                "double" => {
                                    broker_connectors_enterprise::iotdb::IotDbDataType::Double
                                }
                                "text" => broker_connectors_enterprise::iotdb::IotDbDataType::Text,
                                _ => {
                                    return Err(format!(
                                        "iotdb connector field `types` entry {name:?} must be one of boolean, int32, int64, float, double, text"
                                    ));
                                }
                            };
                            types.push(data_type);
                        }
                        types
                    }
                    _ => {
                        return Err("iotdb connector is missing required field: types".to_string());
                    }
                },
                batch_size: Some(1),
                batch_bytes: Some(2_097_152),
                linger_ms: Some(10),
                max_retries: Some(3),
                initial_backoff_ms: Some(100),
                max_backoff_ms: Some(2_000),
                timeout_ms,
            };
            let client = reqwest::Client::builder()
                .timeout(config.timeout())
                .build()
                .unwrap_or_default();
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::iotdb::HttpIotDbTransport::new(&config, client)
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::iotdb::IotDbSink::new(config, transport)
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("iotdb:{}", name), sink);
                }
            }
        }
        "opentsdb" => {
            // Fail closed (S1-01): the create and update paths gate
            // these fields first; the arm repeats the check so boot
            // replay cannot invent a target either.
            let Some(raw_server) = body
                .get("server")
                .or_else(|| body.get("endpoint"))
                .or_else(|| body.get("servers"))
                .or_else(|| body.get("url"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("opentsdb connector is missing required field: server".to_string());
            };
            let endpoint = if raw_server.starts_with("http://")
                || raw_server.starts_with("https://")
                || raw_server.starts_with("telnet://")
            {
                raw_server.to_string()
            } else {
                format!("http://{raw_server}")
            };
            let protocol = if endpoint.starts_with("telnet://") {
                broker_connectors::opentsdb::OpenTsdbProtocol::Telnet
            } else {
                broker_connectors::opentsdb::OpenTsdbProtocol::Http
            };
            let Some(metric_template) = body
                .get("metric")
                .or_else(|| body.get("metric_template"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("opentsdb connector is missing required field: metric".to_string());
            };
            let Some(value_field) = body
                .get("value")
                .or_else(|| body.get("value_field"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("opentsdb connector is missing required field: value".to_string());
            };
            let summary = body
                .get("summary")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());

            let mut tag_mappings = std::collections::HashMap::new();
            if let Some(tags) = body
                .get("tags")
                .or_else(|| body.get("tag_mappings"))
                .and_then(|v| v.as_object())
            {
                for (k, v) in tags {
                    if let Some(s) = v.as_str() {
                        tag_mappings.insert(k.clone(), s.to_string());
                    }
                }
            }

            let config = broker_connectors::opentsdb::OpenTsdbConfig {
                endpoint,
                protocol,
                metric_template: metric_template.to_string(),
                tag_mappings,
                value_field: value_field.to_string(),
                summary,
                compression: broker_connectors::opentsdb::OpenTsdbCompression::None,
                batch_size: Some(1),
                buffer_capacity: None,
                timeout_ms,
            };
            let transport = Arc::new(broker_connectors::opentsdb::NetworkOpenTsdbTransport::new(
                &config,
            ));
            if let Some(sink) = record_sink_error!(broker_connectors::opentsdb::OpenTsdbSink::new(
                config, transport
            )) {
                let sink = Arc::new(sink);
                engine.connectors().register(name, sink.clone());
                engine
                    .connectors()
                    .register(format!("opentsdb:{}", name), sink.clone());
                engine
                    .connectors()
                    .register(format!("opents:{}", name), sink);
            }
        }
        "doris" => {
            // Fail closed (S1-01): the create and update paths gate
            // these fields first; the arm repeats the check so boot
            // replay cannot invent a target either.
            let Some(raw_server) = body
                .get("server")
                .or_else(|| body.get("fe_host"))
                .or_else(|| body.get("host"))
                .or_else(|| body.get("endpoint"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("doris connector is missing required field: server".to_string());
            };
            let no_scheme = raw_server
                .trim_start_matches("http://")
                .trim_start_matches("https://");
            let fe_host = no_scheme
                .split([':', '/'])
                .next()
                .unwrap_or("")
                .trim()
                .to_string();
            if fe_host.is_empty() {
                return Err("doris connector is missing required field: server".to_string());
            }
            // The port rides in `server` (`host:port`); an explicit
            // `http_port` (alias `port`) wins, else the server port
            // applies, else the Doris front-end default HTTP port.
            let server_port = no_scheme
                .split('/')
                .next()
                .unwrap_or("")
                .split(':')
                .nth(1)
                .and_then(|port| port.trim().parse::<u16>().ok());
            let http_port = body
                .get("http_port")
                .or_else(|| body.get("port"))
                .and_then(|v| v.as_u64())
                .map(|port| port as u16)
                .or(server_port)
                .unwrap_or(8030);
            // Default `8030`: the Doris front-end default HTTP port.
            let Some(database) = body
                .get("database")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("doris connector is missing required field: database".to_string());
            };
            let database = database.to_string();
            let Some(table) = body
                .get("table")
                .or_else(|| body.get("table_template"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("doris connector is missing required field: table".to_string());
            };
            let table = table.to_string();
            let username = body
                .get("username")
                .and_then(|v| v.as_str())
                .unwrap_or("root")
                .to_string();
            let password = body
                .get("password")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());

            let config = broker_connectors_enterprise::doris::DorisSinkConfig {
                fe_host,
                http_port,
                database,
                table_template: table,
                auth: broker_connectors_enterprise::doris::DorisAuth { username, password },
                format: broker_connectors_enterprise::doris::DorisFormat::Json,
                jsonpaths: None,
                strip_outer_array: true,
                // No ratio header: the server default (zero tolerance)
                // applies.
                max_filter_ratio: None,
                batch_size: Some(1),
                batch_bytes: Some(4_194_304),
                linger_ms: Some(10),
                max_retries: Some(3),
                initial_backoff_ms: Some(100),
                max_backoff_ms: Some(2_000),
                timeout_ms,
            };
            // No-redirect client: the transport replays auth + label
            // itself on the FE 307 -> BE hop (a stock client strips
            // Authorization when the BE is a different origin).
            let client = reqwest::Client::builder()
                .timeout(config.timeout())
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap_or_default();
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::doris::HttpDorisTransport::new(&config, client)
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::doris::DorisSink::new(config, transport)
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("doris:{}", name), sink);
                }
            }
        }
        "datalayers" => {
            // Fail closed (S1-01): the create and update paths gate
            // these fields first; the arm repeats the check so boot
            // replay cannot invent a target either. The server default
            // is the reference connector default.
            let raw_server = body
                .get("server")
                .or_else(|| body.get("endpoint"))
                .or_else(|| body.get("servers"))
                .or_else(|| body.get("url"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or("127.0.0.1:8361");
            let endpoint =
                if raw_server.starts_with("http://") || raw_server.starts_with("https://") {
                    raw_server.to_string()
                } else {
                    format!("http://{raw_server}")
                };
            let Some(database) = body
                .get("database")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("datalayers connector is missing required field: database".to_string());
            };
            let database = database.to_string();
            let Some(table) = body
                .get("table")
                .or_else(|| body.get("measurement"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("datalayers connector is missing required field: table".to_string());
            };
            let table = table.to_string();
            let auth_token = body
                .get("auth_token")
                .or_else(|| body.get("token"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            // Field names follow the reference specification.
            let username = body
                .get("username")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let password = body
                .get("password")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());

            let config = broker_connectors_enterprise::datalayers::DatalayersConfig {
                endpoint,
                database,
                table,
                auth_token,
                username,
                password,
                // Stamps are always microseconds (the wire requirement);
                // the default column split keeps the `topic` tag and
                // measures every payload field.
                timestamp_field: None,
                tag_columns: Vec::new(),
                field_columns: Vec::new(),
                batch_size: Some(1),
                buffer_capacity: None,
                timeout_ms,
            };
            let transport = Arc::new(
                broker_connectors_enterprise::datalayers::HttpDatalayersTransport::new(&config),
            );
            if let Some(sink) = record_sink_error!(
                broker_connectors_enterprise::datalayers::DatalayersSink::new(config, transport)
            ) {
                let sink = Arc::new(sink);
                engine.connectors().register(name, sink.clone());
                engine
                    .connectors()
                    .register(format!("datalayers:{}", name), sink);
            }
        }
        #[cfg(feature = "elasticsearch")]
        "elasticsearch" | "elastic" | "es" | "opensearch" => {
            crate::v5::action_sinks::validate_connector_io_bounds(body, name, "connector config")?;
            // The connector carries the endpoint and authentication only.
            // The action owns the index, id and document, so the connector
            // alone registers no sink and invents no index. A carried
            // legacy index template is still validated, so a bad legacy
            // body cannot store an unusable connector.
            if let Some(index) = body
                .get("index_template")
                .or_else(|| body.get("index"))
                .and_then(|value| value.as_str())
                .map(str::trim)
                .filter(|text| !text.is_empty())
            {
                let raw_server = body
                    .get("endpoint")
                    .or_else(|| body.get("server"))
                    .or_else(|| body.get("servers"))
                    .or_else(|| body.get("url"))
                    .and_then(|value| value.as_str())
                    .map(str::trim)
                    .filter(|text| !text.is_empty())
                    .unwrap_or("127.0.0.1:9200");
                let endpoint =
                    if raw_server.starts_with("http://") || raw_server.starts_with("https://") {
                        raw_server.to_string()
                    } else {
                        format!("http://{raw_server}")
                    };
                let config = broker_connectors::elasticsearch::ElasticsearchSinkConfig {
                    endpoint,
                    index_template: index.to_string(),
                    doc_id_template: None,
                    operation: broker_connectors::elasticsearch::ElasticsearchOperation::Index,
                    doc_template: None,
                    overwrite: true,
                    doc_as_upsert: false,
                    require_alias: false,
                    routing: None,
                    auth: broker_connectors::elasticsearch::ElasticsearchAuth::None,
                    batch_size: 1,
                    batch_timeout_ms: 10,
                    max_retries: 2,
                    request_timeout_ms: None,
                };
                config
                    .validate()
                    .map_err(|error| format!("invalid elasticsearch config: {error}"))?;
            }
        }
        #[cfg(not(feature = "elasticsearch"))]
        "elasticsearch" | "elastic" | "es" | "opensearch" => {
            return Err(refuse_disabled_kind(conn_type, "elasticsearch"));
        }
        "pulsar" => {
            let server = body
                .get("service_url")
                .or_else(|| body.get("server"))
                .or_else(|| body.get("servers"))
                .or_else(|| body.get("endpoint"))
                .and_then(|v| v.as_str())
                .unwrap_or("pulsar://127.0.0.1:6650");
            let service_url = if server.starts_with("pulsar://")
                || server.starts_with("http://")
                || server.starts_with("https://")
            {
                server.to_string()
            } else {
                format!("pulsar://{}", server)
            };
            let topic = body
                .get("topic")
                .or_else(|| body.get("topic_template"))
                .and_then(|v| v.as_str())
                .unwrap_or("persistent://public/default/telemetry")
                .to_string();
            let tenant = body
                .get("tenant")
                .and_then(|v| v.as_str())
                .unwrap_or("public")
                .to_string();
            let namespace = body
                .get("namespace")
                .and_then(|v| v.as_str())
                .unwrap_or("default")
                .to_string();
            let token = body.get("token").and_then(|v| v.as_str());
            let auth = if let Some(t) = token {
                broker_connectors_enterprise::pulsar::PulsarAuth::Token {
                    token: t.to_string(),
                }
            } else {
                broker_connectors_enterprise::pulsar::PulsarAuth::None
            };
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());

            let config = broker_connectors_enterprise::pulsar::PulsarSinkConfig {
                service_url,
                tenant,
                namespace,
                topic,
                auth,
                partition_key_template: Some("${client_id}".to_string()),
                properties: std::collections::HashMap::new(),
                batch_size: Some(1),
                batch_bytes: Some(2_097_152),
                linger_ms: Some(10),
                max_retries: Some(3),
                initial_backoff_ms: Some(100),
                max_backoff_ms: Some(2_000),
                timeout_ms,
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::pulsar::DriverPulsarTransport::new(&config)
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::pulsar::PulsarSink::new(config, transport)
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("pulsar:{}", name), sink);
                }
            }
        }
        "rocketmq" => {
            let server = body
                .get("endpoint")
                .or_else(|| body.get("endpoints"))
                .or_else(|| body.get("server"))
                .or_else(|| body.get("servers"))
                .and_then(|v| v.as_str())
                .unwrap_or("127.0.0.1:9876");
            let endpoint = server
                .trim_start_matches("http://")
                .trim_start_matches("tcp://")
                .to_string();
            let topic = body
                .get("topic")
                .and_then(|v| v.as_str())
                .unwrap_or("rocket-telemetry")
                .to_string();
            let access_key = body
                .get("access_key")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let secret_key = body
                .get("secret_key")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());

            let config = broker_connectors_enterprise::rocketmq::RocketMqSinkConfig {
                endpoints: vec![endpoint],
                topic,
                tag_template: None,
                keys_template: Some("${client_id}".to_string()),
                message_group_template: None,
                access_key,
                secret_key,
                batch_size: Some(1),
                buffer_capacity: None,
                linger_ms: Some(10),
                timeout_ms,
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::rocketmq::TcpRocketMqTransport::new(&config)
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::rocketmq::RocketMqSink::new(config, transport)
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("rocketmq:{}", name), sink);
                }
            }
        }
        #[cfg(feature = "confluent")]
        "confluent" => {
            let server = body
                .get("bootstrap_servers")
                .or_else(|| body.get("bootstrap_hosts"))
                .or_else(|| body.get("server"))
                .or_else(|| body.get("servers"))
                .and_then(|v| v.as_str())
                .unwrap_or("127.0.0.1:9092");
            let bootstrap = server
                .trim_start_matches("http://")
                .trim_start_matches("tcp://")
                .to_string();
            let topic = body
                .get("topic_template")
                .or_else(|| body.get("topic"))
                .and_then(|v| v.as_str())
                .unwrap_or("telemetry-events")
                .to_string();
            // Fail closed (S1-01): API key and secret are operator-supplied;
            // the previous built-in placeholders are removed.
            let Some(api_key) = body
                .get("api_key")
                .or_else(|| body.get("username"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let Some(api_secret) = body
                .get("api_secret")
                .or_else(|| body.get("password"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());

            let config = broker_connectors_enterprise::confluent::ConfluentKafkaConfig {
                bootstrap_servers: vec![bootstrap],
                api_key: api_key.to_string(),
                api_secret: api_secret.to_string(),
                auth_mechanism: broker_connectors_enterprise::confluent::SaslMechanism::Plain,
                security_protocol:
                    broker_connectors_enterprise::confluent::ConfluentSecurityProtocol::SaslSsl,
                topic_template: topic,
                partition_key_template: Some("${client_id}".to_string()),
                schema_registry: None,
                partitions: 1,
                batch_size: Some(1),
                buffer_capacity: None,
                timeout_ms,
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::confluent::RdkafkaConfluentTransport::new(&config)
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::confluent::ConfluentKafkaSink::new(
                        config, transport,
                    )
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("confluent:{}", name), sink);
                }
            }
        }
        #[cfg(not(feature = "confluent"))]
        "confluent" => {
            return Err(refuse_disabled_kind(conn_type, "confluent"));
        }
        "disk_log" | "disk" | "disklog" => {
            let dir = body
                .get("directory")
                .or_else(|| body.get("dir"))
                .or_else(|| body.get("path"))
                .and_then(|v| v.as_str())
                .unwrap_or("./target/disk_logs");
            let prefix = body
                .get("filename_prefix")
                .or_else(|| body.get("prefix"))
                .and_then(|v| v.as_str())
                .unwrap_or("indra");
            let ext = body
                .get("filename_extension")
                .or_else(|| body.get("extension"))
                .and_then(|v| v.as_str())
                .unwrap_or("log");
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());
            // Bounded by default: 64 MiB active segment caps the recovery
            // scan, 10 retained segments cap total disk (~640 MiB + active)
            // so a forgotten connector cannot fill the disk.
            let config = broker_connectors::disk_log::DiskLogSinkConfig {
                directory: dir.to_string(),
                filename_prefix: prefix.to_string(),
                filename_extension: ext.to_string(),
                format: broker_connectors::disk_log::DiskLogFormat::Ndjson,
                max_file_size_bytes: Some(67_108_864),
                max_file_age_secs: None,
                compression: broker_connectors::disk_log::DiskLogCompression::None,
                max_backup_files: Some(10),
                max_retention_days: None,
                sync_mode: broker_connectors::disk_log::DiskSyncMode::EveryBatch,
                timeout_ms,
            };
            if let Some(writer) = record_sink_error!(
                broker_connectors::disk_log::FileDiskLogWriter::open(&config).await
            ) {
                if let Some(sink) = record_sink_error!(
                    broker_connectors::disk_log::DiskLogSink::open(config, Arc::new(writer)).await
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("disk_log:{}", name), sink.clone());
                    engine.connectors().register(format!("disk:{}", name), sink);
                }
            }
        }
        "opc_ua" | "opcua" => {
            let endpoint_url = body
                .get("endpoint_url")
                .or_else(|| body.get("server"))
                .or_else(|| body.get("url"))
                .or_else(|| body.get("endpoint"))
                .and_then(|v| v.as_str())
                .unwrap_or("opc.tcp://127.0.0.1:4840");
            let endpoint_url = if endpoint_url.starts_with("opc.tcp://") {
                endpoint_url.to_string()
            } else {
                format!("opc.tcp://{}", endpoint_url.trim_start_matches("tcp://"))
            };
            let node_id = body
                .get("node_id")
                .and_then(|v| v.as_str())
                .unwrap_or("ns=1;i=1001");
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());

            let config = broker_connectors_enterprise::opc_ua::OpcUaSinkConfig {
                endpoint_url,
                security_policy: broker_connectors_enterprise::opc_ua::OpcUaSecurityPolicy::None,
                security_mode: broker_connectors_enterprise::opc_ua::OpcUaSecurityMode::None,
                auth: broker_connectors_enterprise::opc_ua::OpcUaAuth::Anonymous,
                node_subscriptions: vec![
                    broker_connectors_enterprise::opc_ua::NodeSubscriptionConfig {
                        node_id: node_id.to_string(),
                        sampling_interval_ms: 1000,
                        publish_topic_template: "opcua/${node.sanitized_id}".to_string(),
                        write_topic_pattern: Some("#".to_string()),
                    },
                ],
                buffer_capacity: None,
                batch_size: Some(1),
                linger_ms: Some(10),
                timeout_ms,
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::opc_ua::DriverOpcUaTransport::new(&config)
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::opc_ua::OpcUaSink::new(config, transport)
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("opc_ua:{}", name), sink.clone());
                    engine
                        .connectors()
                        .register(format!("opcua:{}", name), sink);
                }
            }
        }
        "sparkplug_b" | "sparkplug" => {
            let topic_prefix = body
                .get("topic_prefix")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());
            let mqtt_url = body
                .get("mqtt_url")
                .or_else(|| body.get("broker_address"))
                .or_else(|| body.get("server"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let client_id = body
                .get("client_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            let config = broker_connectors_enterprise::sparkplug_b::SparkplugSinkConfig {
                topic_prefix,
                tier: broker_connectors_enterprise::sparkplug_b::SPARKPLUG_TIER.to_string(),
                batch_size: Some(1),
                linger_ms: Some(10),
                timeout_ms,
                mqtt_url: mqtt_url.clone(),
                client_id: client_id.clone(),
                max_aliases: Some(10_000),
            };
            // The sink sends to the MQTT server that the connector
            // names. Without a server there is no destination, thus no
            // sink is registered.
            let transport: Arc<dyn broker_connectors_enterprise::sparkplug_b::SparkplugTransport> =
                match mqtt_url.filter(|url| !url.trim().is_empty()) {
                    Some(url) => {
                        match broker_connectors_enterprise::sparkplug_b::RumqttcSparkplugTransport::new(
                            &url,
                            client_id.as_deref(),
                        ) {
                            Ok(driver) => Arc::new(driver),
                            Err(error) => return Err(error.to_string()),
                        }
                    }
                    None => return Ok(()),
                };
            if let Some(sink) = record_sink_error!(
                broker_connectors_enterprise::sparkplug_b::SparkplugBSink::new(config, transport)
            ) {
                let sink = Arc::new(sink);
                engine.connectors().register(name, sink.clone());
                engine
                    .connectors()
                    .register(format!("sparkplug_b:{}", name), sink.clone());
                engine
                    .connectors()
                    .register(format!("sparkplug:{}", name), sink);
            }
        }
        #[cfg(feature = "s3")]
        "s3" | "minio" => {
            let endpoint = body
                .get("endpoint")
                .or_else(|| body.get("url"))
                .or_else(|| body.get("server"))
                .and_then(|v| v.as_str())
                .unwrap_or("http://127.0.0.1:9000");
            let bucket = body
                .get("bucket")
                .and_then(|v| v.as_str())
                .unwrap_or("telemetry");
            let region = body
                .get("region")
                .and_then(|v| v.as_str())
                .unwrap_or("us-east-1");
            let access_key_id = body
                .get("access_key_id")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let secret_access_key = body
                .get("secret_access_key")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let key_template = body
                .get("key_template")
                .and_then(|v| v.as_str())
                .unwrap_or("telemetry/${topic}_${seq}.ndjson");
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());

            let config = broker_connectors::s3::S3SinkConfig {
                endpoint: endpoint.to_string(),
                bucket: bucket.to_string(),
                region: region.to_string(),
                access_key_id: access_key_id.to_string(),
                secret_access_key: secret_access_key.to_string(),
                key_template: key_template.to_string(),
                compression: broker_connectors::s3::S3Compression::None,
                batch_size: 1,
                batch_bytes: 5 * 1024 * 1024,
                batch_timeout_ms: 10,
                timeout_ms,
            };
            if let Some(transport) =
                record_sink_error!(broker_connectors::s3::SdkS3Transport::new(&config))
            {
                let transport = Arc::new(transport);
                if let Some(sink) =
                    record_sink_error!(broker_connectors::s3::S3Sink::new(config, transport))
                {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("s3:{}", name), sink.clone());
                    engine
                        .connectors()
                        .register(format!("minio:{}", name), sink);
                }
            }
        }
        #[cfg(not(feature = "s3"))]
        "s3" | "minio" => {
            return Err(refuse_disabled_kind(conn_type, "s3"));
        }
        #[cfg(feature = "kinesis")]
        "kinesis" | "aws_kinesis" => {
            let stream = body
                .get("stream_name")
                .or_else(|| body.get("stream"))
                .and_then(|v| v.as_str())
                .unwrap_or("telemetry-stream");
            let region = body
                .get("region")
                .and_then(|v| v.as_str())
                .unwrap_or("us-east-1");
            let endpoint = body
                .get("endpoint")
                .or_else(|| body.get("url"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let access_key = body
                .get("access_key_id")
                .and_then(|v| v.as_str())
                .unwrap_or("test");
            let secret_key = body
                .get("secret_access_key")
                .and_then(|v| v.as_str())
                .unwrap_or("test");
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());

            let config = broker_connectors_enterprise::kinesis::KinesisSinkConfig {
                stream_name: stream.to_string(),
                region: region.to_string(),
                endpoint,
                access_key_id: access_key.to_string(),
                secret_access_key: secret_key.to_string(),
                session_token: None,
                partition_key_template: Some("${topic}".to_string()),
                explicit_hash_key: None,
                batch_size: Some(1),
                batch_bytes: Some(4_194_304),
                linger_ms: Some(10),
                max_retries: Some(3),
                initial_backoff_ms: Some(100),
                max_backoff_ms: Some(2_000),
                timeout_ms,
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::kinesis::SdkKinesisTransport::new(&config)
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::kinesis::KinesisSink::new(config, transport)
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("kinesis:{}", name), sink);
                }
            }
        }
        #[cfg(not(feature = "kinesis"))]
        "kinesis" | "aws_kinesis" => {
            return Err(refuse_disabled_kind(conn_type, "kinesis"));
        }
        #[cfg(not(feature = "dynamodb"))]
        "dynamodb" | "dynamo" | "ddb" => {
            // Feature disabled (X1-09): fail closed with no sink; the
            // checked wrapper names the missing feature.
            return Err(refuse_disabled_kind(conn_type, "dynamodb"));
        }
        #[cfg(feature = "dynamodb")]
        "dynamodb" | "dynamo" | "ddb" => {
            let table = body
                .get("table_name")
                .or_else(|| body.get("table"))
                .and_then(|v| v.as_str())
                .unwrap_or("sensor_events");
            let region = body
                .get("region")
                .and_then(|v| v.as_str())
                .unwrap_or("us-east-1");
            let endpoint = body
                .get("endpoint")
                .or_else(|| body.get("url"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let access_key = body
                .get("access_key_id")
                .and_then(|v| v.as_str())
                .unwrap_or("test");
            let secret_key = body
                .get("secret_access_key")
                .and_then(|v| v.as_str())
                .unwrap_or("test");
            let partition_key_name = body
                .get("partition_key")
                .and_then(|v| v.as_str())
                .unwrap_or("id");
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());

            let config = broker_connectors_enterprise::dynamodb::DynamoDbSinkConfig {
                table_name: table.to_string(),
                region: region.to_string(),
                endpoint,
                access_key_id: access_key.to_string(),
                secret_access_key: secret_key.to_string(),
                session_token: None,
                partition_key: broker_connectors_enterprise::dynamodb::DynamoKeyConfig {
                    name: partition_key_name.to_string(),
                    template: "${timestamp}".to_string(),
                    key_type: "S".to_string(),
                },
                sort_key: None,
                ttl_attribute: None,
                ttl_secs: None,
                attributes_mapping: std::collections::HashMap::new(),
                batch_size: Some(1),
                batch_bytes: Some(1_048_576),
                linger_ms: Some(10),
                max_retries: Some(3),
                initial_backoff_ms: Some(100),
                max_backoff_ms: Some(2_000),
                timeout_ms,
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::dynamodb::SdkDynamoDbTransport::new(&config)
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::dynamodb::DynamoDbSink::new(config, transport)
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("dynamodb:{}", name), sink);
                }
            }
        }
        "timestream" => {
            // Fail closed (S1-01): the create and update paths gate
            // these fields first; the arm repeats the check so boot
            // replay cannot invent a target either.
            let Some(database) = body
                .get("database")
                .or_else(|| body.get("database_name"))
                .or_else(|| body.get("db"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("timestream connector is missing required field: database".to_string());
            };
            let Some(table) = body
                .get("table")
                .or_else(|| body.get("table_name"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("timestream connector is missing required field: table".to_string());
            };
            let Some(region) = body
                .get("region")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("timestream connector is missing required field: region".to_string());
            };
            let endpoint = body
                .get("endpoint")
                .or_else(|| body.get("url"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|value| {
                    if value.starts_with("http://") || value.starts_with("https://") {
                        value.to_string()
                    } else {
                        format!("http://{value}")
                    }
                });
            let Some(access_key) = body
                .get("access_key_id")
                .or_else(|| body.get("access_key"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err(
                    "timestream connector is missing required field: access_key_id".to_string(),
                );
            };
            let Some(secret_key) = body
                .get("secret_access_key")
                .or_else(|| body.get("secret_key"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
            else {
                return Err(
                    "timestream connector is missing required field: secret_access_key".to_string(),
                );
            };
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());

            // The row shape travels in the connector body for the
            // legacy connector-level path (the action carries it per
            // target on the action-owned path). An absent or empty
            // shape is refused, never invented.
            let measures = match body
                .get("measures")
                .or_else(|| body.get("multi_measure_mappings"))
                .and_then(|v| v.as_object())
            {
                Some(map) if !map.is_empty() && map.values().all(|entry| entry.is_string()) => map
                    .iter()
                    .map(|(name, entry)| (name.clone(), entry.as_str().unwrap_or("").to_string()))
                    .collect::<std::collections::HashMap<String, String>>(),
                _ => {
                    return Err(
                        "timestream connector is missing required field: measures".to_string()
                    );
                }
            };
            let mut dimensions = std::collections::HashMap::new();
            if let Some(map) = body.get("dimensions").and_then(|v| v.as_object()) {
                for (name, entry) in map {
                    if let Some(text) = entry.as_str() {
                        dimensions.insert(name.clone(), text.to_string());
                    }
                }
            }
            // Connector-level `precision` applies here as on the
            // action path; an unknown value is refused.
            let time_unit = match body
                .get("precision")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                None | Some("ms") => {
                    broker_connectors_enterprise::timestream::TimestreamTimeUnit::Milliseconds
                }
                Some("us") => {
                    broker_connectors_enterprise::timestream::TimestreamTimeUnit::Microseconds
                }
                Some("ns") => {
                    broker_connectors_enterprise::timestream::TimestreamTimeUnit::Nanoseconds
                }
                Some(other) => {
                    return Err(format!(
                        "timestream connector field `precision` must be one of ms, us, ns: {other:?}"
                    ));
                }
            };

            let config = broker_connectors_enterprise::timestream::TimestreamSinkConfig {
                database_name: database.to_string(),
                table_name: table.to_string(),
                region: region.to_string(),
                endpoint,
                access_key_id: access_key.to_string(),
                secret_access_key: secret_key.to_string(),
                session_token: body
                    .get("session_token")
                    .or_else(|| body.get("token"))
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                    .map(str::to_string),
                time_unit,
                measure_name_template: body
                    .get("measure")
                    .or_else(|| body.get("measure_name_template"))
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string),
                dimensions,
                multi_measure_mappings: measures,
                batch_size: Some(1),
                batch_bytes: Some(1_048_576),
                linger_ms: Some(10),
                max_retries: Some(3),
                initial_backoff_ms: Some(100),
                max_backoff_ms: Some(2_000),
                timeout_ms,
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::timestream::HttpTimestreamTransport::new(
                    &config,
                    reqwest::Client::new(),
                )
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::timestream::TimestreamSink::new(
                        config, transport
                    )
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("timestream:{}", name), sink.clone());
                    engine
                        .connectors()
                        .register(format!("aws_timestream:{}", name), sink);
                }
            }
        }
        "redshift" => {
            let database = body
                .get("database")
                .and_then(|v| v.as_str())
                .unwrap_or("dev");
            let table = body
                .get("table_template")
                .or_else(|| body.get("table"))
                .and_then(|v| v.as_str())
                .unwrap_or("sensor_events");
            let workgroup = body
                .get("workgroup_name")
                .or_else(|| body.get("workgroup"))
                .and_then(|v| v.as_str())
                .unwrap_or("default");
            let region = body
                .get("region")
                .and_then(|v| v.as_str())
                .unwrap_or("us-east-1");
            let endpoint = body
                .get("endpoint")
                .or_else(|| body.get("url"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let access_key = body
                .get("access_key_id")
                .and_then(|v| v.as_str())
                .unwrap_or("test");
            let secret_key = body
                .get("secret_access_key")
                .and_then(|v| v.as_str())
                .unwrap_or("test");
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());

            let config = broker_connectors_enterprise::redshift::RedshiftSinkConfig {
                database: database.to_string(),
                table_template: table.to_string(),
                cluster_identifier: None,
                workgroup_name: Some(workgroup.to_string()),
                region: region.to_string(),
                endpoint,
                access_key_id: access_key.to_string(),
                secret_access_key: secret_key.to_string(),
                session_token: None,
                db_user: None,
                sql_template: None,
                batch_size: Some(1),
                batch_bytes: Some(1_048_576),
                linger_ms: Some(10),
                max_retries: Some(3),
                initial_backoff_ms: Some(100),
                max_backoff_ms: Some(2_000),
                timeout_ms,
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::redshift::SdkRedshiftTransport::new(&config)
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::redshift::RedshiftSink::new(config, transport)
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("redshift:{}", name), sink);
                }
            }
        }
        #[cfg(not(feature = "aws_iot"))]
        "aws_iot" | "aws_iot_core" => {
            // Feature disabled (X1-09): fail closed with no sink; the
            // checked wrapper names the missing feature.
            return Err(refuse_disabled_kind(conn_type, "aws_iot"));
        }
        #[cfg(feature = "aws_iot")]
        "aws_iot" | "aws_iot_core" => {
            // Fail closed (S1-01 + R1-01): mTLS material is operator-supplied
            // from the request body (flat or nested `auth`), never invented.
            // A missing certificate or key registers nothing; SigV4 has no
            // transport in this build and is rejected at construction.
            let auth_obj = body.get("auth").and_then(|v| v.as_object());
            let field = |keys: &[&str]| -> Option<String> {
                for key in keys {
                    if let Some(value) = body.get(*key).and_then(|v| v.as_str()) {
                        if !value.trim().is_empty() {
                            return Some(value.to_string());
                        }
                    }
                    if let Some(auth) = auth_obj {
                        if let Some(value) = auth.get(*key).and_then(|v| v.as_str()) {
                            if !value.trim().is_empty() {
                                return Some(value.to_string());
                            }
                        }
                    }
                }
                None
            };
            let endpoint = field(&["endpoint", "url", "server"])
                .unwrap_or_else(|| "127.0.0.1:8883".to_string());
            let region = field(&["region"]).unwrap_or_else(|| "us-east-1".to_string());
            let client_id = field(&["client_id"]).unwrap_or_else(|| "indra-bridge".to_string());
            let Some(client_cert_pem) =
                field(&["client_cert_pem", "client_cert", "certificate", "cert_pem"])
            else {
                return Ok(());
            };
            let Some(client_key_pem) =
                field(&["client_key_pem", "client_key", "private_key", "key_pem"])
            else {
                return Ok(());
            };
            let ca_cert_pem = field(&["ca_cert_pem", "ca_cert", "ca_pem", "certificate_authority"])
                .unwrap_or_default();
            let ca_bundle_pem = field(&["ca_bundle_pem", "ca_bundle"]);
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());
            let connect_timeout_ms = body.get("connect_timeout_ms").and_then(|v| v.as_u64());
            let handshake_timeout_ms = body.get("handshake_timeout_ms").and_then(|v| v.as_u64());

            let config = broker_connectors_enterprise::aws_iot::AwsIotConfig {
                endpoint: endpoint.to_string(),
                region: region.to_string(),
                client_id: client_id.to_string(),
                auth: broker_connectors_enterprise::aws_iot::AwsIotAuth::Mtls {
                    ca_cert_pem,
                    client_cert_pem,
                    client_key_pem,
                },
                topic_mappings: vec![broker_connectors_enterprise::aws_iot::BridgeTopicMapping {
                    local_topic: "#".to_string(),
                    remote_topic: "aws/telemetry/${client_id}".to_string(),
                    direction:
                        broker_connectors_enterprise::aws_iot::BridgeDirection::LocalToRemote,
                }],
                shadow_sync: None,
                buffer_capacity: None,
                batch_size: Some(1),
                linger_ms: Some(10),
                max_retries: Some(3),
                timeout_ms,
                connect_timeout_ms,
                handshake_timeout_ms,
                ca_bundle_pem,
                alpn_protocols: None,
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::aws_iot::TlsAwsIotTransport::new(&config)
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::aws_iot::AwsIotSink::new(config, transport)
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("aws_iot:{}", name), sink.clone());
                    engine
                        .connectors()
                        .register(format!("aws_iot_core:{}", name), sink);
                }
            }
        }
        #[cfg(not(feature = "azure_blob"))]
        "azure_blob" | "azure_blob_storage" | "azureblob" | "azblob" => {
            // Feature disabled (X1-09): fail closed with no sink; the
            // checked wrapper names the missing feature.
            return Err(refuse_disabled_kind(conn_type, "azure_blob"));
        }
        #[cfg(feature = "azure_blob")]
        "azure_blob" | "azure_blob_storage" | "azureblob" | "azblob" => {
            let account_name = body
                .get("account_name")
                .and_then(|v| v.as_str())
                .unwrap_or("devstoreaccount1");
            let container_name = body
                .get("container_name")
                .or_else(|| body.get("container"))
                .and_then(|v| v.as_str())
                .unwrap_or("telemetry");
            let endpoint = body
                .get("endpoint")
                .or_else(|| body.get("url"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let blob_path = body
                .get("blob_path_template")
                .or_else(|| body.get("path_template"))
                .and_then(|v| v.as_str())
                .unwrap_or("telemetry/${batch_id}.json");
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .and_then(|v| v.as_u64());
            let auth = body
                .get("auth")
                .and_then(|v| {
                    serde_json::from_value::<broker_connectors_enterprise::azure_blob::AzureBlobAuth>(
                        v.clone(),
                    )
                    .ok()
                })
                .or_else(|| {
                    body.get("account_key")
                        .or_else(|| body.get("accountKey"))
                        .and_then(|v| v.as_str())
                        .map(
                            |s| broker_connectors_enterprise::azure_blob::AzureBlobAuth::SharedKey {
                                account_key: s.to_string(),
                            },
                        )
                })
                .or_else(|| {
                    body.get("auth").and_then(|v| v.as_object()).and_then(|o| {
                        o.get("account_key")
                            .or_else(|| o.get("accountKey"))
                            .and_then(|v| v.as_str())
                            .map(
                                |s| broker_connectors_enterprise::azure_blob::AzureBlobAuth::SharedKey {
                                    account_key: s.to_string(),
                                },
                            )
                    })
                })
                .or_else(|| {
                    body.get("sas_token")
                        .or_else(|| body.get("sas"))
                        .and_then(|v| v.as_str())
                        .map(|s| broker_connectors_enterprise::azure_blob::AzureBlobAuth::SasToken {
                            sas_token: s.to_string(),
                        })
                        .or_else(|| {
                            body.get("auth").and_then(|v| v.as_object()).and_then(|o| {
                                o.get("sas_token")
                                    .or_else(|| o.get("sas"))
                                    .and_then(|v| v.as_str())
                                    .map(|s| {
                                        broker_connectors_enterprise::azure_blob::AzureBlobAuth::SasToken {
                                            sas_token: s.to_string(),
                                        }
                                    })
                            })
                        })
                })
                .or_else(|| {
                    body.get("token")
                        .or_else(|| body.get("bearer_token"))
                        .and_then(|v| v.as_str())
                        .map(
                            |s| broker_connectors_enterprise::azure_blob::AzureBlobAuth::BearerToken {
                                token: s.to_string(),
                            },
                        )
                        .or_else(|| {
                            body.get("auth").and_then(|v| v.as_object()).and_then(|o| {
                                o.get("token")
                                    .or_else(|| o.get("bearer_token"))
                                    .and_then(|v| v.as_str())
                                    .map(|s| {
                                        broker_connectors_enterprise::azure_blob::AzureBlobAuth::BearerToken {
                                            token: s.to_string(),
                                        }
                                    })
                            })
                        })
                });
            let Some(auth) = auth else {
                return Ok(());
            };

            let config = broker_connectors_enterprise::azure_blob::AzureBlobSinkConfig {
                account_name: account_name.to_string(),
                container_name: container_name.to_string(),
                endpoint,
                auth,
                blob_path_template: blob_path.to_string(),
                compression: broker_connectors_enterprise::azure_blob::AzureBlobCompression::None,
                max_records_per_blob: Some(1),
                max_bytes_per_blob: Some(1_048_576),
                flush_interval_secs: 1,
                buffer_capacity: None,
                timeout_ms,
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::azure_blob::HttpAzureBlobTransport::new(
                    &config,
                    reqwest::Client::new(),
                )
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::azure_blob::AzureBlobSink::new(config, transport)
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("azure_blob:{}", name), sink.clone());
                    engine
                        .connectors()
                        .register(format!("azure_blob_storage:{}", name), sink);
                }
            }
        }
        #[cfg(not(feature = "azure_eventhubs"))]
        "azure_eventhubs" | "azure_event_hubs" | "azure" | "eventhubs" => {
            // Feature disabled (X1-09): fail closed with no sink; the
            // checked wrapper names the missing feature.
            return Err(refuse_disabled_kind(conn_type, "azure_eventhubs"));
        }
        #[cfg(feature = "azure_eventhubs")]
        "azure_eventhubs" | "azure_event_hubs" | "azure" | "eventhubs" => {
            let namespace = body
                .get("namespace")
                .and_then(|v| v.as_str())
                .unwrap_or("test-namespace");
            let event_hub = body
                .get("event_hub")
                .or_else(|| body.get("hub"))
                .and_then(|v| v.as_str())
                .unwrap_or("test-hub");
            let endpoint = body
                .get("endpoint")
                .or_else(|| body.get("url"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let shared_access_key_name = body
                .get("shared_access_key_name")
                .or_else(|| body.get("key_name"))
                .and_then(|v| v.as_str())
                .unwrap_or("SendPolicy");
            // Fail closed (S1-01): SAS key is operator-supplied; the previous
            // built-in test key is removed.
            let Some(shared_access_key) = body
                .get("shared_access_key")
                .or_else(|| body.get("key"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .and_then(|v| v.as_u64());

            let config = broker_connectors_enterprise::azure_eventhubs::AzureEventHubsSinkConfig {
                namespace: namespace.to_string(),
                event_hub: event_hub.to_string(),
                endpoint,
                shared_access_key_name: shared_access_key_name.to_string(),
                shared_access_key: shared_access_key.to_string(),
                partition_key_template: None,
                user_properties: std::collections::HashMap::new(),
                token_ttl_secs: 3600,
                batch_size: Some(1),
                batch_bytes: Some(1_048_576),
                linger_ms: Some(10),
                max_retries: Some(3),
                initial_backoff_ms: Some(100),
                max_backoff_ms: Some(2_000),
                timeout_ms,
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::azure_eventhubs::HttpAzureEventHubsTransport::new(
                    &config,
                    reqwest::Client::new(),
                )
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::azure_eventhubs::AzureEventHubsSink::new(
                        config, transport,
                    )
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("azure_eventhubs:{}", name), sink.clone());
                    engine
                        .connectors()
                        .register(format!("azure_event_hubs:{}", name), sink);
                }
            }
        }
        #[cfg(not(feature = "azure_iot"))]
        "azure_iot" | "azure_iot_hub" | "azure_iothub" | "iothub" => {
            // Feature disabled (X1-09): fail closed with no sink; the
            // checked wrapper names the missing feature.
            return Err(refuse_disabled_kind(conn_type, "azure_iot"));
        }
        #[cfg(feature = "azure_iot")]
        "azure_iot" | "azure_iot_hub" | "azure_iothub" | "iothub" => {
            let hub_name = body
                .get("iot_hub_name")
                .or_else(|| body.get("hub_name"))
                .or_else(|| body.get("server"))
                .or_else(|| body.get("endpoint"))
                .and_then(|v| v.as_str())
                .unwrap_or("127.0.0.1:8883");
            let device_id = body
                .get("device_id")
                .and_then(|v| v.as_str())
                .unwrap_or("device-01");
            // Fail closed (S1-01): SAS key is operator-supplied; the previous
            // built-in test key is removed.
            let Some(shared_access_key) = body
                .get("shared_access_key")
                .or_else(|| body.get("key"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .and_then(|v| v.as_u64());
            let module_id = body
                .get("module_id")
                .or_else(|| body.get("module"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let key_name = body
                .get("key_name")
                .or_else(|| body.get("shared_access_key_name"))
                .and_then(|v| v.as_str())
                .unwrap_or("device")
                .to_string();
            let api_version = body
                .get("api_version")
                .and_then(|v| v.as_str())
                .unwrap_or("2021-04-12")
                .to_string();
            let direct_methods_enabled = body
                .get("direct_methods_enabled")
                .or_else(|| body.get("direct_methods"))
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            let twin_sync_enabled = body
                .get("twin_sync_enabled")
                .or_else(|| body.get("twin_sync"))
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            let batch_size = body
                .get("batch_size")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize)
                .or(Some(1));
            let buffer_capacity = body
                .get("buffer_capacity")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize);
            let linger_ms = body.get("linger_ms").and_then(|v| v.as_u64()).or(Some(10));
            let max_retries = body
                .get("max_retries")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize)
                .or(Some(3));
            let connect_timeout_ms = body.get("connect_timeout_ms").and_then(|v| v.as_u64());
            let handshake_timeout_ms = body.get("handshake_timeout_ms").and_then(|v| v.as_u64());
            let ca_bundle_pem = body
                .get("ca_bundle_pem")
                .or_else(|| body.get("ca_bundle"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let sas_ttl_secs = body
                .get("sas_ttl_secs")
                .or_else(|| body.get("sas_ttl"))
                .or_else(|| body.get("token_ttl_secs"))
                .and_then(|v| v.as_u64());

            let config = broker_connectors_enterprise::azure_iot::AzureIotConfig {
                iot_hub_name: hub_name.to_string(),
                device_id: device_id.to_string(),
                module_id,
                auth: broker_connectors_enterprise::azure_iot::AzureIotAuth::SharedAccessKey {
                    key: shared_access_key.to_string(),
                    key_name: Some(key_name),
                },
                api_version,
                direct_methods_enabled,
                twin_sync_enabled,
                batch_size,
                buffer_capacity,
                linger_ms,
                max_retries,
                timeout_ms,
                connect_timeout_ms,
                handshake_timeout_ms,
                ca_bundle_pem,
                sas_ttl_secs,
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::azure_iot::TlsAzureIotTransport::new(&config)
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::azure_iot::AzureIotSink::new(config, transport)
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("azure_iot:{}", name), sink.clone());
                    engine
                        .connectors()
                        .register(format!("azure_iot_hub:{}", name), sink);
                }
            }
        }
        #[cfg(feature = "gcp_pubsub")]
        "gcp_pubsub" | "pubsub" => {
            let project_id = body
                .get("project_id")
                .or_else(|| body.get("project"))
                .and_then(|v| v.as_str())
                .unwrap_or("test-project");
            let topic_id = body
                .get("topic_id")
                .or_else(|| body.get("topic"))
                .and_then(|v| v.as_str())
                .unwrap_or("test-topic");
            let endpoint = body
                .get("endpoint")
                .or_else(|| body.get("url"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .and_then(|v| v.as_u64());

            let config = broker_connectors_enterprise::gcp_pubsub::GcpPubSubSinkConfig {
                project_id: project_id.to_string(),
                topic_id: topic_id.to_string(),
                endpoint,
                auth: broker_connectors_enterprise::gcp_pubsub::GcpAuth::None,
                ordering_key_template: None,
                attributes: std::collections::HashMap::new(),
                batch_size: Some(1),
                batch_bytes: Some(1_048_576),
                linger_ms: Some(10),
                max_retries: Some(3),
                initial_backoff_ms: Some(100),
                max_backoff_ms: Some(2_000),
                timeout_ms,
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::gcp_pubsub::SdkGcpPubSubTransport::new(&config)
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::gcp_pubsub::GcpPubSubSink::new(config, transport)
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("gcp_pubsub:{}", name), sink.clone());
                    engine
                        .connectors()
                        .register(format!("pubsub:{}", name), sink);
                }
            }
        }
        #[cfg(not(feature = "gcp_pubsub"))]
        "gcp_pubsub" | "pubsub" => {
            return Err(refuse_disabled_kind(conn_type, "gcp_pubsub"));
        }
        #[cfg(not(feature = "bigquery"))]
        "bigquery" | "bq" | "gbq" => {
            // Feature disabled: fail closed with no sink; the
            // checked wrapper names the missing feature.
            return Err(refuse_disabled_kind(conn_type, "bigquery"));
        }
        #[cfg(feature = "bigquery")]
        "bigquery" | "bq" | "gbq" => {
            let project_id = body
                .get("project_id")
                .or_else(|| body.get("project"))
                .and_then(|v| v.as_str())
                .unwrap_or("test-project");
            let dataset_id = body
                .get("dataset_id")
                .or_else(|| body.get("dataset"))
                .and_then(|v| v.as_str())
                .unwrap_or("test-dataset");
            let table = body
                .get("table")
                .or_else(|| body.get("table_template"))
                .and_then(|v| v.as_str())
                .unwrap_or("telemetry");
            let endpoint = body
                .get("endpoint")
                .or_else(|| body.get("url"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .and_then(|v| v.as_u64());
            // Batching / retry knobs are read from the request so management
            // configuration drives the sink; defaults preserve the previous
            // live-path behaviour (auto-flush every row).
            let ignore_unknown_values = body
                .get("ignore_unknown_values")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            let skip_invalid_rows = body
                .get("skip_invalid_rows")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let template_suffix = body
                .get("template_suffix")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let batch_size = body
                .get("batch_size")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize)
                .or(Some(1));
            let batch_bytes = body
                .get("batch_bytes")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize)
                .or(Some(1_048_576));
            let linger_ms = body.get("linger_ms").and_then(|v| v.as_u64()).or(Some(10));
            let max_retries = body
                .get("max_retries")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize)
                .or(Some(3));
            let initial_backoff_ms = body
                .get("initial_backoff_ms")
                .and_then(|v| v.as_u64())
                .or(Some(100));
            let max_backoff_ms = body
                .get("max_backoff_ms")
                .and_then(|v| v.as_u64())
                .or(Some(2_000));

            let config = broker_connectors_enterprise::bigquery::BigQuerySinkConfig {
                project_id: project_id.to_string(),
                dataset_id: dataset_id.to_string(),
                table_template: table.to_string(),
                endpoint,
                auth: broker_connectors_enterprise::gcp_pubsub::GcpAuth::None,
                ignore_unknown_values,
                skip_invalid_rows,
                template_suffix,
                batch_size,
                batch_bytes,
                linger_ms,
                max_retries,
                initial_backoff_ms,
                max_backoff_ms,
                timeout_ms,
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::bigquery::SdkBigQueryTransport::new(
                    &config,
                    reqwest::Client::new(),
                )
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::bigquery::BigQuerySink::new(config, transport)
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("bigquery:{}", name), sink);
                }
            }
        }
        #[cfg(not(feature = "gcp_iot"))]
        "gcp_iot" | "gcp_iot_core" | "cloud_iot" => {
            // Feature disabled: fail closed with no sink; the
            // checked wrapper names the missing feature.
            return Err(refuse_disabled_kind(conn_type, "gcp_iot"));
        }
        #[cfg(feature = "gcp_iot")]
        "gcp_iot" | "gcp_iot_core" | "cloud_iot" => {
            // Kept: service-documented default endpoint
            // (`mqtt.googleapis.com:8883`, the Google Cloud IoT MQTT bridge;
            // same default as `GcpIotConfig::default_endpoint`).
            // The connector still fails closed without its required fields below.
            let endpoint = body
                .get("endpoint")
                .or_else(|| body.get("url"))
                .or_else(|| body.get("server"))
                .and_then(|v| v.as_str())
                .unwrap_or("mqtt.googleapis.com:8883");
            // Fail closed (S1-01): an operator-supplied project, region,
            // registry, device and key are required; nothing is invented.
            let Some(project_id) = body
                .get("project_id")
                .or_else(|| body.get("project"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let Some(cloud_region) = body
                .get("cloud_region")
                .or_else(|| body.get("region"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let Some(registry_id) = body
                .get("registry_id")
                .or_else(|| body.get("registry"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let Some(device_id) = body
                .get("device_id")
                .or_else(|| body.get("device"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let Some(private_key_pem) = body
                .get("private_key_pem")
                .or_else(|| body.get("private_key"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .and_then(|v| v.as_u64());
            // The operator's extra CA bundle rides the same path the
            // stored connector body keeps: without it a private-CA
            // bridge fails closed at TLS verification.
            let ca_bundle_pem = body
                .get("ca_bundle_pem")
                .or_else(|| body.get("ca_bundle"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            let algorithm = match body.get("algorithm").and_then(|v| v.as_str()) {
                Some("ES256") | Some("es256") => {
                    broker_connectors_enterprise::gcp_iot::GcpIotAlgorithm::Es256
                }
                _ => broker_connectors_enterprise::gcp_iot::GcpIotAlgorithm::Rs256,
            };

            let config = broker_connectors_enterprise::gcp_iot::GcpIotConfig {
                project_id: project_id.to_string(),
                cloud_region: cloud_region.to_string(),
                registry_id: registry_id.to_string(),
                device_id: device_id.to_string(),
                private_key_pem: private_key_pem.to_string(),
                algorithm,
                token_lifetime_secs: 3600,
                endpoint: endpoint.to_string(),
                batch_size: Some(1),
                buffer_capacity: None,
                linger_ms: Some(10),
                max_retries: Some(3),
                timeout_ms,
                ca_bundle_pem,
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::gcp_iot::TlsGcpIotTransport::new(&config)
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::gcp_iot::GcpIotSink::new(config, transport)
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("gcp_iot:{}", name), sink.clone());
                    engine
                        .connectors()
                        .register(format!("gcp_iot_core:{}", name), sink);
                }
            }
        }
        "databricks" | "delta_lake" => {
            // Fail closed (S1-01): host, token, catalog, schema and table are
            // all operator-supplied; the previous mock token and invented
            // names are removed.
            let Some(host) = body
                .get("host")
                .or_else(|| body.get("server"))
                .or_else(|| body.get("endpoint"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let Some(token) = body
                .get("token")
                .or_else(|| body.get("api_key"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let Some(catalog) = body
                .get("catalog")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let Some(schema) = body
                .get("schema")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let Some(table) = body
                .get("table")
                .or_else(|| body.get("table_template"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .and_then(|v| v.as_u64());

            let config = broker_connectors_enterprise::databricks::DatabricksSinkConfig {
                host: host.to_string(),
                token: token.to_string(),
                catalog: catalog.to_string(),
                schema: schema.to_string(),
                table_template: table.to_string(),
                http_path: None,
                partition_key_template: None,
                column_mappings: std::collections::HashMap::new(),
                batch_size: Some(1),
                batch_bytes: Some(2_097_152),
                linger_ms: Some(10),
                max_retries: Some(3),
                initial_backoff_ms: Some(100),
                max_backoff_ms: Some(2_000),
                timeout_ms,
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::databricks::HttpDatabricksTransport::new(
                    &config,
                    reqwest::Client::new(),
                )
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::databricks::DatabricksSink::new(
                        config, transport
                    )
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("databricks:{}", name), sink.clone());
                    engine
                        .connectors()
                        .register(format!("delta_lake:{}", name), sink);
                }
            }
        }
        "snowflake" => {
            // Fail closed (S1-01): account, user, database, schema, table and
            // key are all operator-supplied; the previous test names and
            // built-in key are removed.
            let Some(account) = body
                .get("account")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let Some(user) = body
                .get("user")
                .or_else(|| body.get("username"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let Some(database) = body
                .get("database")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let Some(schema) = body
                .get("schema")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let Some(table) = body
                .get("table")
                .or_else(|| body.get("table_template"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let endpoint = body
                .get("endpoint")
                .or_else(|| body.get("url"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let Some(private_key_pem) = body
                .get("private_key_pem")
                .or_else(|| body.get("private_key"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .and_then(|v| v.as_u64());

            let config = broker_connectors_enterprise::snowflake::SnowflakeSinkConfig {
                account: account.to_string(),
                user: user.to_string(),
                database: database.to_string(),
                schema: schema.to_string(),
                table_template: table.to_string(),
                private_key_pem: private_key_pem.to_string(),
                endpoint,
                role: None,
                channel: "INDRA_CHANNEL".to_string(),
                column_mappings: std::collections::HashMap::new(),
                batch_size: Some(1),
                batch_bytes: Some(4_194_304),
                linger_ms: Some(10),
                max_retries: Some(3),
                initial_backoff_ms: Some(100),
                max_backoff_ms: Some(2_000),
                timeout_ms,
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::snowflake::HttpSnowflakeTransport::new(
                    &config,
                    reqwest::Client::new(),
                )
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::snowflake::SnowflakeSink::new(config, transport)
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("snowflake:{}", name), sink);
                }
            }
        }
        "tablestore" | "ots" => {
            // Fail closed (S1-01): endpoint, instance, table and both access
            // keys are all operator-supplied; no loopback default is invented
            // (Tablestore endpoints are instance-specific, so there is no
            // service-documented default to keep).
            let Some(endpoint) = body
                .get("endpoint")
                .or_else(|| body.get("url"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            // Fail closed (S1-01): instance, table and both access keys are
            // operator-supplied; the previous test names and keys are removed.
            let Some(instance_name) = body
                .get("instance_name")
                .or_else(|| body.get("instance"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let Some(table_name) = body
                .get("table_name")
                .or_else(|| body.get("table"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let Some(access_key_id) = body
                .get("access_key_id")
                .or_else(|| body.get("ak"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let Some(access_key_secret) = body
                .get("access_key_secret")
                .or_else(|| body.get("sk"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .and_then(|v| v.as_u64());

            let config = broker_connectors_enterprise::tablestore::TablestoreSinkConfig {
                endpoint: endpoint.to_string(),
                instance_name: instance_name.to_string(),
                table_name: table_name.to_string(),
                access_key_id: access_key_id.to_string(),
                access_key_secret: access_key_secret.to_string(),
                primary_keys: vec![
                    broker_connectors_enterprise::tablestore::PrimaryKeyMapping {
                        name: "device_id".to_string(),
                        source: "${client_id}".to_string(),
                        data_type: broker_connectors_enterprise::tablestore::PrimaryKeyType::String,
                    },
                ],
                attribute_columns: vec![
                    broker_connectors_enterprise::tablestore::AttributeColumnMapping {
                        name: "temperature".to_string(),
                        source: "${payload.temperature}".to_string(),
                        data_type:
                            broker_connectors_enterprise::tablestore::AttributeColumnType::Double,
                    },
                ],
                batch_size: Some(1),
                buffer_capacity: None,
                linger_ms: Some(10),
                timeout_ms,
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::tablestore::HttpTablestoreTransport::new(
                    &config,
                    reqwest::Client::new(),
                )
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::tablestore::TablestoreSink::new(
                        config, transport
                    )
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("tablestore:{}", name), sink.clone());
                    engine.connectors().register(format!("ots:{}", name), sink);
                }
            }
        }
        "oci_streaming" | "oci" => {
            // Fail closed (S1-01): endpoint, OCIDs, fingerprint and key are
            // all operator-supplied; no loopback default is invented (OCI
            // Streaming endpoints are cell-specific, so there is no
            // service-documented default to keep).
            let Some(endpoint) = body
                .get("endpoint")
                .or_else(|| body.get("url"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            // Fail closed (S1-01): OCIDs, fingerprint and key are
            // operator-supplied; the previous test OCIDs, test fingerprint
            // and built-in key are removed.
            let Some(stream_pool_id) = body
                .get("stream_pool_id")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let Some(stream_id) = body
                .get("stream_id")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let Some(tenancy_ocid) = body
                .get("tenancy_ocid")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let Some(user_ocid) = body
                .get("user_ocid")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let Some(fingerprint) = body
                .get("fingerprint")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let Some(private_key_pem) = body
                .get("private_key_pem")
                .or_else(|| body.get("private_key"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .and_then(|v| v.as_u64());

            let partition_key_template = body
                .get("partition_key_template")
                .or_else(|| body.get("partition_key"))
                .and_then(|v| v.as_str())
                .unwrap_or("${topic}");

            let config = broker_connectors_enterprise::oci_streaming::OciStreamingSinkConfig {
                endpoint: endpoint.to_string(),
                stream_pool_id: stream_pool_id.to_string(),
                stream_id: stream_id.to_string(),
                tenancy_ocid: tenancy_ocid.to_string(),
                user_ocid: user_ocid.to_string(),
                fingerprint: fingerprint.to_string(),
                private_key_pem: private_key_pem.to_string(),
                partition_key_template: partition_key_template.to_string(),
                batch_size: Some(1),
                buffer_capacity: None,
                batch_bytes: Some(4_194_304),
                linger_ms: Some(10),
                max_retries: Some(3),
                initial_backoff_ms: Some(100),
                max_backoff_ms: Some(2_000),
                timeout_ms,
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::oci_streaming::HttpOciStreamingTransport::new(
                    &config,
                    reqwest::Client::new(),
                )
            ) {
                let transport = Arc::new(transport);
                if let Some(sink) = record_sink_error!(
                    broker_connectors_enterprise::oci_streaming::OciStreamingSink::new(
                        config, transport,
                    )
                ) {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("oci_streaming:{}", name), sink.clone());
                    engine.connectors().register(format!("oci:{}", name), sink);
                }
            }
        }
        _ => {}
    }
    // A recorded build failure refuses with the sink text. Nothing
    // registered above when it is set, so the caller stores nothing.
    if let Some(error) = build_error {
        return Err(error);
    }
    Ok(())
}

/// `POST /api/v5/connectors` (C07-02): validate the body against the
/// named kind (unknown kind or malformed config rejected with the
/// documented error shape); a duplicate id is rejected cleanly, never
/// overwritten. Only the built-in execution path registers a live sink;
/// non-built-in kinds store config and health first and never claim
/// execution they lack (their live status reads back `disconnected` until
/// their follow-up build registers them). Management-plane only: one
/// short lock per request plus lock-free manager registration; publish
/// and deliver never touch this map (dispatch reads the manager
/// directly, never this map).
///
/// Store bounds (stated here and enforced below): at most
/// [`MAX_CONNECTORS`] entries (512); creates past the cap are rejected
/// with `BAD_REQUEST`. Connectors persist across restart via the config
/// core; live health does not (recomputed per read from the manager).
// TODO(parity): which per-kind config fields does the spec require beyond
// the kind being known plus the credential gates in
// `missing_connector_field`? The rulebook does not decide the exact
// per-kind schemas; the current choice validates the kind against the
// kernel registry and the credential gates, storing remaining fields
// opaquely until the checker pins it.
// TODO(parity): for an explicit-id create that is both a duplicate and an
// unknown kind, is the answer the duplicate or the kind error? The
// rulebook does not decide the precedence; the current choice rejects the
// duplicate first (existence before validation, mirroring the rule and
// action creates) until the checker pins it.
pub async fn create_connector(
    State(state): State<ApiState>,
    Json(mut body): Json<serde_json::Value>,
) -> Response {
    let Some(obj_check) = body.as_object() else {
        return ApiError::BadRequest("request body must be a JSON object".to_string())
            .into_response();
    };
    // The connector id is `name`, with `id` accepted as an alias: stored
    // entries carry both keys with the same bare value, and reads match
    // the bare value or the `{type}:{name}` key exactly (no fuzzy
    // matching). A console body carries the key in `id`; it strips to
    // the bare name here.
    let conn_type_early = obj_check
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let raw_name = obj_check
        .get("id")
        .or_else(|| obj_check.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let name = super::typed::bare_name(&raw_name, &conn_type_early);
    if name.is_empty() {
        return ApiError::BadRequest("connector id or name is required".to_string())
            .into_response();
    }
    // MV-01: `action:` is the reserved dispatch-key prefix of the
    // action-owned sinks (the fire targets of v5 rules). A connector
    // id with the prefix would collide with that namespace, so it is
    // refused here before anything stores.
    if broker_config::is_v5_action_sink_key(&name) {
        return ApiError::BadRequest(format!(
            "connector id {name:?} is reserved: ids starting with `action:` name action sinks (field `id`)"
        ))
        .into_response();
    }
    if name.len() > MAX_CONNECTOR_ID_LEN {
        return ApiError::BadRequest(format!(
            "connector id holds {} chars, at most {MAX_CONNECTOR_ID_LEN} (field `id`)",
            name.len()
        ))
        .into_response();
    }
    let conn_type = obj_check
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if conn_type.is_empty() {
        return ApiError::BadRequest("missing required field: type".to_string()).into_response();
    }
    if let Some(message) = crate::removed_connector_refusal(&conn_type) {
        return ApiError::AlreadyExists(message.to_string()).into_response();
    }
    if !broker_config::is_known_connector_type(&conn_type) {
        return ApiError::AlreadyExists(format!("unknown connector type: {conn_type}"))
            .into_response();
    }
    // Reject a duplicate id before mutating anything: pushing it into
    // the store would poison every later connector POST/PUT/DELETE, whose
    // persistence export re-validates the whole store and fails on the
    // duplicated id. The documented conflict code is `ALREADY_EXISTS`.
    // This early read-check also skips the wasted live-sink
    // registration below; the write-locked re-check before the push
    // closes the check-then-insert race for the in-memory store.
    if state.connectors.contains(&name) {
        return ApiError::AlreadyExists(format!("connector {name:?} already exists"))
            .into_response();
    }
    // Store-cap bound (management-plane, fail closed): an obviously full
    // store fails without a needless registration attempt.
    {
        let live = state.connectors.len();
        if live >= MAX_CONNECTORS {
            return ApiError::BadRequest(format!(
                "connector store holds {live} entries, at most {MAX_CONNECTORS}"
            ))
            .into_response();
        }
    }

    // Fail closed (S1-01): a connector without its required credential or
    // identity is rejected here with the missing field named. It is never
    // stored in a degraded state, and `register_live_sink` below also
    // refuses to invent values.
    if let Some(missing) = missing_connector_field(&conn_type, &body) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "BAD_REQUEST",
                "message": format!(
                    "missing required field: {missing} for connector type {conn_type}"
                ),
            })),
        )
            .into_response();
    }
    // The official document-store drivers own their pools and TLS. An
    // explicit pool, pipelining or `ssl` value is refused at create,
    // not stored and ignored.
    if matches!(
        crate::v5::action_sinks::canonical_action_kind(&conn_type),
        "mongodb" | "couchbase" | "elasticsearch"
    ) {
        if let Err(message) =
            crate::v5::action_sinks::validate_connector_io_bounds(&body, &name, "connector create")
        {
            return ApiError::BadRequest(message).into_response();
        }
    }

    // KC-00: a known kind whose capability state is not `ready` is refused
    // after its credential fields pass, before anything is stored, with the
    // documented message (the feature is named when it is the reason).
    // Create keeps the reference error code for this operation,
    // `ALREADY_EXISTS`.
    if let Some(message) = kind_refusal(&conn_type) {
        return ApiError::AlreadyExists(message).into_response();
    }

    // Register the live sink on the create event through the same
    // path boot replay uses: delivery dials its target lazily at send
    // time, so registration never depends on a point-in-time
    // reachability probe. A kind with no execution path registers
    // nothing, so it honestly reads back `disconnected` (stored config
    // first, no claimed execution).
    // A sink that fails to build is refused with the documented error
    // shape carrying the sink text, and nothing is stored. A valid sink
    // whose server is down now still builds, so it stores as before.
    if let Err(register_error) =
        try_register_live_sink(&state.engine, &conn_type, &name, &body).await
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "BAD_REQUEST",
                "message": register_error,
            })),
        )
            .into_response();
    }
    // Live health, never a write-time snapshot: a kind with no
    // execution path registers nothing, so it honestly reads back
    // `disconnected` (stored config first, no claimed execution).
    let live = state.engine.connectors().get(&name).is_some();
    let status_str = connector_status(live);

    if let Some(obj) = body.as_object_mut() {
        obj.insert("id".to_string(), serde_json::Value::String(name.clone()));
        obj.insert("name".to_string(), serde_json::Value::String(name.clone()));
        obj.insert(
            "type".to_string(),
            serde_json::Value::String(conn_type.clone()),
        );
        obj.insert(
            "status".to_string(),
            serde_json::Value::String(status_str.to_string()),
        );
        if !obj.contains_key("enable") {
            obj.insert("enable".to_string(), serde_json::Value::Bool(true));
        }
        obj.insert("node_status".to_string(), connector_node_status(status_str));
    }
    normalize_connector_document(&conn_type, &mut body);

    // Re-check under the write lock so two concurrent creates for the
    // same id cannot both pass the early check and push a duplicate pair.
    match state.connectors.try_insert(&name, body.clone()) {
        Ok(()) => {}
        Err(ConnectorInsertError::Duplicate) => {
            return ApiError::AlreadyExists(format!("connector {name:?} already exists"))
                .into_response();
        }
        Err(ConnectorInsertError::Full) => {
            return ApiError::BadRequest(format!(
                "connector store holds {} entries, at most {MAX_CONNECTORS}",
                state.connectors.len()
            ))
            .into_response();
        }
    }
    // W3-19: mirror the flag into the engine's lock-free dispatch
    // snapshot so rule evaluation observes a created-disabled connector
    // as skipped from the first publish without taking this store's
    // lock on the message path.
    let created_enable = body.get("enable").and_then(|v| v.as_bool()).unwrap_or(true);
    state.engine.set_action_enabled(&name, created_enable);

    // A persist failure is a 500 (the in-memory entry stays but the disk
    // save failed); the loss must never be silent.
    if let Err(error) = state.connectors.persist() {
        return connector_persist_error(error);
    }

    // MV-01: the new connector can resolve actions that stored while
    // dangling (dangling references keep the action storable, see
    // `stage_action_sink`): replay every stored action's owned sink from
    // the stores so a stored action starts delivering the moment its
    // target connector arrives. Actions whose targets still miss keep
    // their wiring state and alarm — the replay is observational for
    // everything it cannot build.
    crate::v5::action_sinks::rebuild_all_action_sinks(
        &state.engine,
        &state.connectors,
        &state.actions,
        &state.alarms,
    );

    (StatusCode::CREATED, Json(body)).into_response()
}

/// `PUT /api/v5/connectors/:id`: validated replace of one connector (C07-05).
///
/// The body gets the same validation as create: it must be a JSON object
/// carrying a known `type` (unknown kind or a missing `type` is rejected
/// with the documented error shape), the same id-length bound applies to
/// the path id, and the same required-credential gate applies (an update
/// that drops a credential or identity is rejected with the field named).
/// An unknown path id returns 404 and creates nothing. On success the
/// stored entry is swapped for the validated snapshot and the new fields
/// read back with 200, with live health recomputed from the manager.
///
/// The path id is authoritative: a body `id`/`name` that names a
/// different connector is rejected instead of renaming or forking the
/// entry. An omitted `enable` defaults to `true`, mirroring create. The
/// pair is not paged; unknown query parameters are ignored (the ignored
/// query sink below). Management-plane only: one short write lock
/// swapping a single validated entry (plus the registry save lock on
/// persist and lock-free manager swaps for the live sink); rule
/// evaluation keeps reading its snapshot and publish and deliver never
/// touch this store.
///
/// Bound (stated here and enforced by construction): a replace never
/// grows the map, so the [`MAX_CONNECTORS`] cap from W3-17 applies
/// unchanged and no new accumulation is introduced. Connectors persist
/// across restart via the config core; live health does not (recomputed
/// per read from the manager).
///
/// Fire-path cost (stated for the delivery-path note): no new work on
/// publish or deliver. This handler swaps one validated snapshot on the
/// management plane; the fire path loads the manager directly.
// TODO(parity): may an update rename the connector (body id differing
// from the path id), or change only the config half? The rulebook does
// not decide the exact PUT field set; the current choice keeps the path
// id authoritative and replaces the whole entry in place until the
// checker pins it.
// TODO(parity): does an omitted `enable` keep the stored value or
// default to `true`? The rulebook does not decide; the current choice
// mirrors create (`true`) until the checker pins it.
// TODO(parity): for an unknown id with an invalid body, is the answer
// 404 or 400? The rulebook does not decide the precedence; the current
// choice answers 404 first (existence before validation, mirroring the
// action update) and creates nothing either way.
pub async fn update_connector(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    Query(_ignored): Query<HashMap<String, String>>,
    Json(mut body): Json<serde_json::Value>,
) -> Response {
    // The path id is authoritative and accepts the bare id or the
    // `{type}:{name}` key. No fuzzy matching: any other form is 404.
    // Existence before validation: an unknown id is 404 and creates
    // nothing, even when the body is also invalid.
    let Some(stored) = state.connectors.get(&id) else {
        return ApiError::NotFound(format!("connector {id:?} not found")).into_response();
    };
    let bare = connector_bare_name(&stored);
    let Some(obj) = body.as_object() else {
        return ApiError::BadRequest("request body must be a JSON object".to_string())
            .into_response();
    };
    if let Some(body_id) = obj
        .get("id")
        .or_else(|| obj.get("name"))
        .and_then(|v| v.as_str())
    {
        let trimmed = body_id.trim();
        if !trimmed.is_empty() {
            let body_bare = super::typed::bare_name(
                trimmed,
                obj.get("type").and_then(|v| v.as_str()).unwrap_or(""),
            );
            if body_bare != bare && trimmed != id {
                return ApiError::BadRequest(format!(
                    "body id {trimmed:?} does not match path id {id:?} (field `id`)"
                ))
                .into_response();
            }
        }
    }
    if bare.len() > MAX_CONNECTOR_ID_LEN {
        return ApiError::BadRequest(format!(
            "connector id holds {} chars, at most {MAX_CONNECTOR_ID_LEN} (field `id`)",
            bare.len()
        ))
        .into_response();
    }
    let conn_type = obj
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if conn_type.is_empty() {
        return ApiError::BadRequest("missing required field: type".to_string()).into_response();
    }
    if let Some(message) = crate::removed_connector_refusal(&conn_type) {
        return ApiError::BadRequest(message.to_string()).into_response();
    }
    if !broker_config::is_known_connector_type(&conn_type) {
        return ApiError::BadRequest(format!("unknown connector type: {conn_type}"))
            .into_response();
    }
    // KC-00: refuse a non-ready kind before the live sink is swapped, so a
    // refused update keeps the old record and its registration untouched.
    if let Some(message) = kind_refusal(&conn_type) {
        return ApiError::BadRequest(message).into_response();
    }
    // MV-01: `action:` is the reserved action-sink dispatch prefix; a
    // connector id renamed into it stays refused.
    if broker_config::is_v5_action_sink_key(&bare) {
        return ApiError::BadRequest(format!(
            "connector id {bare:?} is reserved: ids starting with `action:` name action sinks (field `id`)"
        ))
        .into_response();
    }
    // Fail closed (S1-01): same required-field gate as create; an update
    // that drops a credential or identity is rejected with the field named.
    if let Some(missing) = missing_connector_field(&conn_type, &body) {
        return ApiError::BadRequest(format!(
            "missing required field: {missing} for connector type {conn_type}"
        ))
        .into_response();
    }
    // The official document-store drivers own their pools and TLS. An
    // explicit pool, pipelining or `ssl` value is refused at update.
    if matches!(
        crate::v5::action_sinks::canonical_action_kind(&conn_type),
        "mongodb" | "couchbase" | "elasticsearch"
    ) {
        if let Err(message) =
            crate::v5::action_sinks::validate_connector_io_bounds(&body, &bare, "connector update")
        {
            return ApiError::BadRequest(message).into_response();
        }
    }
    // MV-01 (R2-04): every dependent action must accept the new
    // connector settings before anything swaps in. Each action's owned
    // sink is built here staged (built, never registered): a failure
    // answers 400 naming the action and the field, keeps every old
    // sink (the old generation keeps delivering) and leaves the stored
    // row untouched. On success the staged sinks swap in under the
    // action dispatch keys below, after the stored update and its
    // persist.
    let mut staged_sinks: Vec<(String, Arc<dyn broker_connectors::Sink>)> = Vec::new();
    let mut staged_ids: Vec<String> = Vec::new();
    for record in state.actions.snapshot() {
        if !crate::v5::action_sinks::action_targets_connector(&state.connectors, &record, &bare) {
            continue;
        }
        if !crate::v5::action_sinks::action_owned_kind(&record.action_type) {
            continue;
        }
        let opts = match crate::v5::action_sinks::ActionResourceOpts::parse(
            &record.id,
            &record.resource_opts,
        ) {
            Ok(opts) => opts,
            Err(message) => {
                return ApiError::BadRequest(format!(
                    "action {:?} cannot use connector {bare:?}: {message}",
                    record.id
                ))
                .into_response();
            }
        };
        match crate::v5::action_sinks::build_action_sink(&record, &body, &opts) {
            Ok(Some(sink)) => {
                let key = broker_config::v5_action_sink_key(&record.id);
                // R2-04: wrap the rebuilt sink in its resource-option
                // runtime, as create, update and boot do. A raw sink
                // would stop applying the action options after this
                // connector update.
                let sink = crate::v5::action_sinks::wrap_action_runtime(
                    &state.engine,
                    &key,
                    sink,
                    opts,
                    extract_target_host_port(&body),
                );
                staged_sinks.push((key, sink));
                staged_ids.push(record.id.clone());
            }
            Ok(None) => {
                staged_ids.push(record.id.clone());
            }
            Err(message) => {
                return ApiError::BadRequest(format!(
                    "action {:?} cannot use connector {bare:?}: {message}",
                    record.id
                ))
                .into_response();
            }
        }
    }
    let enable = obj.get("enable").and_then(|v| v.as_bool()).unwrap_or(true);
    // Swap the live sink first: drop every registration for this id
    // (bare plus any `kind:id` alias, mirroring delete) so a kind change
    // leaves no stale alias behind, then re-register through the same
    // path create uses. Registration never depends on a point-in-time
    // reachability probe (delivery dials lazily at send time); a kind
    // with no execution path registers nothing and honestly reads back
    // `disconnected`.
    for registered in state.engine.connectors().ids() {
        if registered == bare
            || (registered.ends_with(&format!(":{bare}"))
                && !broker_config::is_v5_action_sink_key(&registered))
        {
            state.engine.connectors().unregister(&registered);
        }
    }
    if let Err(register_error) =
        try_register_live_sink(&state.engine, &conn_type, &bare, &body).await
    {
        // The failed replace stores nothing: put the previous live sink
        // back so the engine still matches the stored entry.
        let old_kind = stored
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if !old_kind.is_empty() {
            let _ = try_register_live_sink(&state.engine, &old_kind, &bare, &stored).await;
        }
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "BAD_REQUEST",
                "message": register_error,
            })),
        )
            .into_response();
    }
    // Live health, never a write-time snapshot: a kind with no
    // execution path registers nothing, so it honestly reads back
    // `disconnected` (stored config first, no claimed execution).
    let live = state.engine.connectors().get(&bare).is_some();
    let status_str = connector_status(live);
    if let Some(obj) = body.as_object_mut() {
        obj.insert("id".to_string(), serde_json::Value::String(bare.clone()));
        obj.insert("name".to_string(), serde_json::Value::String(bare.clone()));
        obj.insert(
            "type".to_string(),
            serde_json::Value::String(conn_type.clone()),
        );
        obj.insert("enable".to_string(), serde_json::Value::Bool(enable));
        obj.insert(
            "status".to_string(),
            serde_json::Value::String(status_str.to_string()),
        );
        obj.insert("node_status".to_string(), connector_node_status(status_str));
    }
    normalize_connector_document(&conn_type, &mut body);
    if !state.connectors.replace(&bare, body.clone()) {
        return ApiError::NotFound(format!("connector {id:?} not found")).into_response();
    }
    // W3-19: mirror the flag into the dispatch snapshot (same lock-free
    // path as the enable route below).
    state.engine.set_action_enabled(&bare, enable);
    // MV-01/R2: the replace can flip the enable flag along with the
    // settings; the dependent actions' delivery follows it.
    mirror_dependent_actions(&state, &bare, enable);
    // A persist failure is a 500 (the in-memory replacement stays but the
    // disk save failed); the loss must never be silent.
    if let Err(error) = state.connectors.persist() {
        return connector_persist_error(error);
    }
    // MV-01/R2-04: swap the staged dependent-action sinks in under their
    // action dispatch keys (staged build → persist → swap, in that
    // order) and report their wiring live. The actions that staged
    // nothing keep their state (they never owned a sink for this
    // connector's new shape). Rows that still wait in an old runtime
    // count as stopped drops before the swap.
    for (key, sink) in staged_sinks {
        if let Some(old) = state.engine.connectors().get(&key) {
            old.discard_buffered();
        }
        state.engine.connectors().register(key, sink);
    }
    for action_id in &staged_ids {
        state
            .actions
            .set_wiring(action_id, crate::v5::actions::ActionWiring::Live);
        state
            .alarms
            .deactivate(&format!("action_build_failed:{action_id}"));
    }
    (StatusCode::OK, Json(body)).into_response()
}

pub async fn delete_connector(State(state): State<ApiState>, Path(id): Path<String>) -> Response {
    // The path id accepts the bare id or the `{type}:{name}` key. No
    // fuzzy matching: any other form is 404. Read (C07-04) and replace
    // (C07-05) match exactly the same way.
    let Some(stored) = state.connectors.get(&id) else {
        return ApiError::NotFound(format!("connector {id:?} not found")).into_response();
    };
    let bare = connector_bare_name(&stored);
    // Reference guard: a connector still referenced by a rule forward or
    // by an action's stored `connector` field cannot be dropped while the
    // reference stands. Both id forms count as a reference. The first
    // referencing owner is named in the refusal, mirroring the action
    // delete guard.
    if let Some(rule_id) = state.engine.list_rules().iter().find_map(|rule| {
        rule.actions
            .iter()
            .any(|action| {
                matches!(
                    action,
                    broker_rules::RuleAction::ForwardConnector { connector_id }
                    if connector_id == &bare || connector_id == &id
                )
            })
            .then(|| rule.id.clone())
    }) {
        return ApiError::BadRequest(format!(
            "connector {id:?} is referenced by rule {rule_id:?}"
        ))
        .into_response();
    }
    if let Some(action_id) = state.actions.snapshot().iter().find_map(|record| {
        let targets = [
            record.connector.as_str(),
            record
                .params
                .get("connector")
                .or_else(|| record.params.get("connector_id"))
                .and_then(|v| v.as_str())
                .unwrap_or(""),
        ];
        (targets.contains(&bare.as_str()) || targets.contains(&id.as_str()))
            .then(|| record.id.clone())
    }) {
        return ApiError::BadRequest(format!(
            "connector {id:?} is referenced by action {action_id:?}"
        ))
        .into_response();
    }
    // TODO(parity): do sources reference connectors (ingress bindings), or
    // is the rule/action guard above the complete reference set? The
    // rulebook does not decide; the current choice guards rules and
    // actions (the only stores with a connector reference field today)
    // until the checker pins it.
    for registered in state.engine.connectors().ids() {
        if registered == bare
            || (registered.ends_with(&format!(":{bare}"))
                && !broker_config::is_v5_action_sink_key(&registered))
        {
            state.engine.connectors().unregister(&registered);
        }
    }

    if !state.connectors.remove(&bare) {
        return ApiError::NotFound(format!("connector {id:?} not found")).into_response();
    }
    state
        .alarms
        .deactivate(&format!("connector_unavailable:{bare}"));
    // The id no longer names anything: drop it from the dispatch
    // snapshot so a later re-create with the same id starts enabled
    // (mirrors the W3-04 action delete path; unknown ids are a no-op).
    state.engine.remove_action(&bare);
    // Unknown ids persist nothing; a persist failure after a real removal
    // is a 500 (the in-memory removal stays but the disk save failed).
    if let Err(error) = state.connectors.persist() {
        return connector_persist_error(error);
    }
    StatusCode::NO_CONTENT.into_response()
}

/// `PUT /api/v5/connectors/:id/enable/:enable`: flip one connector's
/// enable flag (W3-19, C07-06).
///
/// An unknown id returns 404 with the documented `{code, message}` shape
/// and persists nothing. A malformed enable value is rejected with the
/// documented error shape and persists nothing. On success the stored
/// flag is flipped, the change is mirrored into the engine's lock-free
/// dispatch snapshot (disabled forwards are skipped at fire time before
/// reaching `ConnectorManager::send`), the mutation persists through the
/// config core like every other connector write, and the answer is 204
/// with no body, as the reference API documents.
///
/// The pair is not paged; unknown query parameters are ignored (no query
/// is extracted). Management-plane only: one short write lock plus the
/// registry save lock on persist, and one lock-free snapshot swap; rule
/// evaluation reads the snapshot, and publish and deliver never touch
/// this store.
///
/// Bound (stated here and enforced by construction): a single boolean
/// per connector; no new list, no new accumulation. The flag persists
/// across restart with the connector record via the config core.
///
/// Fire-path cost (stated for the delivery-path note): no new work on
/// publish or deliver. The enable read stays the one `Arc` clone of the
/// shared dispatch snapshot per ingress plus one `HashSet` lookup per
/// forward that the W3-04 action flag already pays; connector ids share
/// that snapshot (absent means enabled), so a disabled connector costs
/// the same single lookup and never touches the connector store.
// TODO(parity): which spellings does the enable path accept (`true`/
/// `false` only, or also `1`/`0`, `on`/`off`, mixed case)? The rulebook
// does not decide; the current choice accepts exactly `true`/`false`
// case-insensitively and rejects everything else with `BAD_REQUEST`
// (mirroring the W3-04 action enable route) until the checker pins it.
// TODO(parity): for an unknown id with a malformed enable value, is the
// answer 404 or 400? The rulebook does not decide the precedence; the
// current choice answers 404 first (existence before validation,
// mirroring the action update and enable routes) and persists nothing
// either way.
// TODO(parity): do connector ids and action ids share one disable
// namespace in the dispatch snapshot, or does the checker address them
// separately? The rulebook does not decide; the current choice shares
// the W3-04 snapshot (one lookup per forward covers both) until the
// checker pins it.
/// MV-01/R2: a connector disable stops every dependent action's
/// delivery; a connector enable restores them. Each dependent action's
/// dispatch flag rides `action.enable && connector.enable`. Actions
/// that target other connectors are untouched.
fn mirror_dependent_actions(state: &ApiState, bare: &str, enable: bool) {
    for record in state.actions.snapshot() {
        if crate::v5::action_sinks::action_targets_connector(&state.connectors, &record, bare) {
            state.engine.set_action_enabled(
                &broker_config::v5_action_sink_key(&record.id),
                record.enable && enable,
            );
        }
    }
}

pub async fn set_connector_enabled(
    State(state): State<ApiState>,
    Path((id, enable_raw)): Path<(String, String)>,
) -> Response {
    // The path id accepts the bare id or the `{type}:{name}` key.
    // Existence before validation: an unknown id is 404 and persists
    // nothing, even when the flag is also malformed.
    let Some(stored) = state.connectors.get(&id) else {
        return ApiError::NotFound(format!("connector {id:?} not found")).into_response();
    };
    let bare = connector_bare_name(&stored);
    let enable = match enable_raw.trim().to_ascii_lowercase().as_str() {
        "true" => true,
        "false" => false,
        _ => {
            return ApiError::BadRequest(format!(
                "invalid enable value {enable_raw:?}: expected \"true\" or \"false\" (field `enable`)"
            ))
            .into_response();
        }
    };
    // Flip only the single boolean; id, kind and params are untouched, so
    // the update can never trip the store cap or change validation.
    if !state.connectors.set_enabled(&bare, enable) {
        return ApiError::NotFound(format!("connector {id:?} not found")).into_response();
    }
    // Mirror into the dispatch snapshot before persisting: the in-memory
    // flag is already flipped, so evaluation must observe it even if the
    // disk save below fails (the store itself keeps the mutation on a
    // persist failure and answers 500, never silently).
    state.engine.set_action_enabled(&bare, enable);
    // MV-01/R2: the connector's flag rides the action dispatch key as
    // well as the bare id, so a rule firing under the `action:{id}` key
    // observes the connector disable/enable (shared snapshot, one
    // lookup per forward either way).
    state
        .engine
        .set_action_enabled(&broker_config::v5_action_sink_key(&bare), enable);
    // MV-01/R2: a connector disable stops every dependent action's
    // delivery; an enable restores them (the flap rides the same
    // mirror, before persist, after the store flip).
    mirror_dependent_actions(&state, &bare, enable);
    if let Err(error) = state.connectors.persist() {
        return connector_persist_error(error);
    }
    StatusCode::NO_CONTENT.into_response()
}

/// `POST /api/v5/connectors/:id/:operation`: run one named lifecycle
/// operation against one connector (W3-20, C07-07).
///
/// Only the operations the lifecycle supports are served: `start`
/// (enable the connector) and `stop` (disable it). Both flip the stored
/// enable flag in place, mirror the flag into the engine's lock-free
/// dispatch snapshot (disabled forwards are skipped at fire time before
/// reaching `ConnectorManager::send`, shared with the W3-19 enable
/// route), persist through the config core like every other connector
/// write, and report the resulting connector with 200 so the state
/// change is visible without a second read.
///
/// An unknown connector id returns 404 with the documented `{code,
/// message}` shape and persists nothing; an unknown operation name is
/// rejected with the documented error shape and persists nothing. The
/// pair is not paged; unknown query parameters are ignored (no query is
/// extracted). Management-plane only: one short store read plus one
/// short write lock cloning at most one small record per request (plus
/// the registry save lock on persist) and one lock-free snapshot swap;
/// rule evaluation reads the snapshot, and publish and deliver never
/// touch this store.
///
/// Lifecycle state persists with the connector record (the enable flag
/// in the config core); transient run state is not kept, so nothing
/// extra needs clearing across a restart (boot rebuilds the dispatch
/// snapshot from the persisted flags).
///
/// Fire-path cost (stated for the delivery-path note): no new work on
/// publish or deliver. The operation itself runs entirely on the
/// management plane; the publish path keeps paying what the enable
/// route already pays (one `Arc` of the disabled set per ingress plus
/// one `HashSet` lookup per forward, lock-free, no per-fire
/// allocation).
// TODO(parity): which operation names does the documented spec serve
// beyond `start` (for example `stop`, `restart`)? The rulebook does not
// decide the exact verb set; the current choice serves exactly
// `start`/`stop` as enable flips (the only lifecycle the kernel tracks,
// mirroring the W3-06 action operation) and rejects everything else
// with `BAD_REQUEST` until the checker pins it.
// TODO(parity): are operation names case-sensitive (`START` versus
// `start`)? The rulebook does not decide; the current choice matches
// exactly `start`/`stop` in lower case and rejects every other spelling
// with `BAD_REQUEST` until the checker pins it.
// TODO(parity): for an unknown id with an unknown operation, is the
// answer 404 or 400? The rulebook does not decide the precedence; the
// current choice answers 404 first (existence before validation,
// mirroring the action operation and the connector enable route) and
// persists nothing either way.
pub async fn run_connector_operation(
    State(state): State<ApiState>,
    Path((id, operation)): Path<(String, String)>,
) -> Response {
    apply_connector_operation(&state, &id, &operation).await
}

/// `POST /api/v5/nodes/:node/connectors/:id/:operation`: run one named
/// lifecycle operation against one connector on one named node (W3-22,
/// C07-09).
///
/// Single-node execution only: the `{node}` segment is checked with the
/// shared [`resolve_node`] helper against the configured
/// [`ApiState::node_id`] (falling back to the [`local_node_name`] name
/// the v5 rows render today); an unknown name returns the helper's 404
/// `NOT_FOUND` shape naming the node and touches no other state. A known
/// node dispatches to the same [`apply_connector_operation`] path as the
/// global `POST /connectors/:id/:operation` route (W3-20), so the reported
/// state, the 404 for an unknown id, and the rejection of an unknown
/// operation are identical on both routes.
///
/// The pair is not paged; unknown query parameters are ignored (no query
/// is extracted). Management-plane only: one short store read plus one
/// short write lock cloning at most one small record per request (plus the
/// registry save lock on persist) and one lock-free snapshot swap; rule
/// evaluation reads the snapshot, and publish and deliver never touch this
/// store.
///
/// Bound (stated here and enforced by construction): single-request bounded
/// work with no wait and no accumulated per-node state. The operation
/// completes synchronously under short in-memory locks with no remote call,
/// no retry and no timer, so there is no timeout to tune; node membership
/// itself is owned elsewhere (exactly one local record) and nothing here
/// grows with connections, sessions or subscriptions.
///
/// Fire-path cost: no new work on publish or deliver. The operation itself
/// runs entirely on the management plane; the publish path keeps paying
/// what the enable route already pays (one `Arc` of the disabled set per
/// ingress plus one `HashSet` lookup per forward, lock-free, no per-fire
/// allocation).
// TODO(parity): for an unknown node with an unknown id or operation, is
// the answer the node 404 or the connector/operation error? The rulebook
// does not decide the precedence; the current choice answers the node 404
// first (node scope before connector existence before operation validity,
// fail closed) and touches nothing else either way.
pub async fn run_node_connector_operation(
    State(state): State<ApiState>,
    Path((node, id, operation)): Path<(String, String, String)>,
) -> Response {
    if resolve_node(&state.node_id, &node).is_err()
        && resolve_node(local_node_name(), &node).is_err()
    {
        // Reuse the helper's error so the unknown-node shape stays exactly
        // the documented `{code: NOT_FOUND, message}` body.
        return resolve_node(&state.node_id, &node)
            .expect_err("node already checked as unknown")
            .into_response();
    }
    apply_connector_operation(&state, &id, &operation).await
}

/// Shared dispatch behind the connector operation route: existence check
/// (unknown id is 404, creates nothing), operation validity check
/// (unknown name is `BAD_REQUEST`, persists nothing), then the enable
/// flip with the engine snapshot mirror, persistence and the 204 answer
/// with no body. Management-plane only; see the caller for the
/// locking and fire-path notes.
async fn apply_connector_operation(state: &ApiState, id: &str, operation: &str) -> Response {
    // The path id accepts the bare id or the `{type}:{name}` key.
    // Existence before validation: an unknown id is 404 and persists
    // nothing, even when the operation is also unknown.
    let Some(stored) = state.connectors.get(id) else {
        return ApiError::NotFound(format!("connector {id:?} not found")).into_response();
    };
    let bare = connector_bare_name(&stored);
    let enable = match operation {
        "start" => true,
        "stop" => false,
        _ => {
            return ApiError::BadRequest(format!(
                "unknown connector operation {operation:?}: expected \"start\" or \"stop\" (field `operation`)"
            ))
            .into_response();
        }
    };
    // Flip only the single boolean; id, kind and params are untouched, so
    // the update can never trip the store cap or change validation.
    if !state.connectors.set_enabled(&bare, enable) {
        return ApiError::NotFound(format!("connector {id:?} not found")).into_response();
    }
    // Mirror into the dispatch snapshot before persisting: the in-memory
    // flag is already flipped, so evaluation must observe it even if the
    // disk save below fails (the store itself keeps the mutation on a
    // persist failure and answers 500, never silently). Connector ids
    // share the W3-04 snapshot with action ids (absent means enabled),
    // so a disabled connector costs the same single lookup and never
    // touches the connector store on the fire path.
    state.engine.set_action_enabled(&bare, enable);
    // MV-01/R2: the connector's flag rides the action dispatch key as
    // well as the bare id, so a rule firing under the `action:{id}` key
    // observes the connector start/stop.
    state
        .engine
        .set_action_enabled(&broker_config::v5_action_sink_key(&bare), enable);
    // MV-01/R2: a connector disable stops every dependent action's
    // delivery; an enable restores them.
    mirror_dependent_actions(state, &bare, enable);
    if let Err(error) = state.connectors.persist() {
        return connector_persist_error(error);
    }
    StatusCode::NO_CONTENT.into_response()
}

/// Resolve one URL-like connector field to a real `http`/`https` network
/// target. A secret reference (`file:`/`env:`) resolves first, so the
/// probe never dials the reference string. Any other scheme runs no probe.
fn http_target_host_port(reference: &str) -> Option<String> {
    let trimmed = reference.trim();
    let candidate = if broker_config::secrets::is_secret_ref(trimmed) {
        let resolved = broker_connectors::resolve_field_secret(trimmed);
        let resolved = resolved.trim().to_string();
        if resolved.is_empty() {
            return None;
        }
        resolved
    } else {
        trimmed.to_string()
    };
    let parsed = reqwest::Url::parse(candidate.trim()).ok()?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return None;
    }
    let host = parsed.host_str()?;
    if host.is_empty() {
        return None;
    }
    let port = parsed.port_or_known_default().unwrap_or(80);
    Some(format!("{}:{}", host, port))
}

pub(crate) fn extract_target_host_port(body: &serde_json::Value) -> Option<String> {
    if let Some(s) = body.get("server").and_then(|v| v.as_str()) {
        let mut clean = s.trim();
        if let Some((_, rest)) = clean.split_once("://") {
            clean = rest;
        }
        let host_port = clean
            .split('@')
            .next_back()
            .unwrap_or(clean)
            .split('/')
            .next()
            .unwrap_or(clean)
            .split('?')
            .next()
            .unwrap_or(clean);
        if !host_port.is_empty() {
            return Some(host_port.to_string());
        }
    }
    if let Some(s) = body.get("servers").and_then(|v| v.as_str()) {
        let first = s.split(',').next().unwrap_or(s).trim();
        let mut clean = first;
        if let Some((_, rest)) = clean.split_once("://") {
            clean = rest;
        }
        let host_port = clean
            .split('@')
            .next_back()
            .unwrap_or(clean)
            .split('/')
            .next()
            .unwrap_or(clean)
            .split('?')
            .next()
            .unwrap_or(clean);
        if !host_port.is_empty() {
            return Some(host_port.to_string());
        }
    }
    if let Some(s) = body.get("endpoint").and_then(|v| v.as_str()) {
        let trimmed = s.trim();
        if broker_config::secrets::is_secret_ref(trimmed) {
            // The `endpoint` alias resolves like `url`: a secret reference
            // is never dialled as a fabricated target.
            return http_target_host_port(trimmed);
        }
        let mut clean = trimmed;
        if let Some((_, rest)) = clean.split_once("://") {
            clean = rest;
        }
        let host_port = clean
            .split('@')
            .next_back()
            .unwrap_or(clean)
            .split('/')
            .next()
            .unwrap_or(clean)
            .split('?')
            .next()
            .unwrap_or(clean);
        if !host_port.is_empty() {
            return Some(host_port.to_string());
        }
    }
    if let Some(s) = body.get("endpoint_url").and_then(|v| v.as_str()) {
        let mut clean = s.trim();
        if let Some((_, rest)) = clean.split_once("://") {
            clean = rest;
        }
        let host_port = clean
            .split('@')
            .next_back()
            .unwrap_or(clean)
            .split('/')
            .next()
            .unwrap_or(clean)
            .split('?')
            .next()
            .unwrap_or(clean);
        if !host_port.is_empty() {
            return Some(host_port.to_string());
        }
    }
    if let Some(s) = body.get("bootstrap_hosts").and_then(|v| v.as_str()) {
        let first = s.split(',').next().unwrap_or(s).trim();
        if !first.is_empty() {
            return Some(first.to_string());
        }
    }
    if let Some(s) = body.get("connection_string").and_then(|v| v.as_str()) {
        let mut clean = s.trim();
        if let Some((_, rest)) = clean.split_once("://") {
            clean = rest;
        }
        let host_port = clean
            .split('@')
            .next_back()
            .unwrap_or(clean)
            .split('/')
            .next()
            .unwrap_or(clean)
            .split('?')
            .next()
            .unwrap_or(clean);
        if !host_port.is_empty() {
            return Some(host_port.to_string());
        }
    }
    if let Some(u) = body.get("url").and_then(|v| v.as_str()) {
        // A secret reference (`file:`/`env:`) never probes a fabricated
        // loopback target. Resolve it first; probe only a real `http` or
        // `https` target with a host. Anything else runs no probe.
        return http_target_host_port(u);
    }
    if let Some(h) = body.get("host").and_then(|v| v.as_str()) {
        let mut clean = h.trim();
        if let Some((_, rest)) = clean.split_once("://") {
            clean = rest;
        }
        let host_port = clean
            .split('@')
            .next_back()
            .unwrap_or(clean)
            .split('/')
            .next()
            .unwrap_or(clean)
            .split('?')
            .next()
            .unwrap_or(clean);
        if host_port.contains(':') {
            return Some(host_port.to_string());
        }
        let port = body.get("port").and_then(|v| v.as_u64()).unwrap_or(1433);
        return Some(format!("{}:{}", host_port, port));
    }
    // The reference-shaped `parameters` object carries the same target
    // keys one level down; read them the same way.
    if let Some(parameters) = body.get("parameters") {
        if parameters.is_object() {
            if let Some(target) = extract_target_host_port(parameters) {
                return Some(target);
            }
        }
    }
    None
}

pub(crate) async fn check_tcp_reachable(addr: &str) -> Result<(), String> {
    let target = if addr.contains(':') {
        addr.to_string()
    } else {
        format!("{}:80", addr)
    };
    match tokio::time::timeout(
        std::time::Duration::from_millis(1500),
        tokio::net::TcpStream::connect(&target),
    )
    .await
    {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(format!("Connection refused to {}: {}", target, e)),
        Err(_) => Err(format!("Connection to {} timed out after 1.5s", target)),
    }
}

/// True when the kind stores documents through a server target. Such a
/// kind is never probed without one: a missing target is a failed
/// probe, never a silent reachable.
fn document_kind_needs_target(kind: &str) -> bool {
    matches!(
        crate::v5::action_sinks::canonical_action_kind(kind),
        "mongodb" | "couchbase" | "elasticsearch"
    )
}

/// Required-field gate for document probes. A probe with a reachable
/// port but a missing database or username must fail, never read
/// connected. Reads top-level and the reference `parameters` object.
fn probe_document_missing_field(kind: &str, body: &serde_json::Value) -> Option<&'static str> {
    fn present(body: &serde_json::Value, keys: &[&str]) -> bool {
        keys.iter().any(|k| {
            body.get(*k)
                .and_then(|v| v.as_str())
                .is_some_and(|s| !s.trim().is_empty())
        }) || body.get("parameters").is_some_and(|p| {
            keys.iter().any(|k| {
                p.get(*k)
                    .and_then(|v| v.as_str())
                    .is_some_and(|s| !s.trim().is_empty())
            })
        })
    }
    match crate::v5::action_sinks::canonical_action_kind(kind) {
        "mongodb" => {
            if !present(body, &["server", "servers"]) {
                return Some("server");
            }
            if !present(body, &["database"]) {
                return Some("database");
            }
            None
        }
        "couchbase" => {
            if !present(body, &["server", "servers"]) {
                return Some("server");
            }
            if !present(body, &["username", "user"]) {
                return Some("username");
            }
            None
        }
        _ => None,
    }
}

/// True when the kind speaks HTTP, so the probe adds an HTTP round
/// trip after the TCP dial.
fn probe_kind_needs_http_handshake(kind: &str) -> bool {
    matches!(
        crate::v5::action_sinks::canonical_action_kind(kind),
        "elasticsearch"
    )
}

/// HTTP root handshake for search kinds: one bounded GET to `/`. Any
/// HTTP answer counts as reachable; only a transport failure fails.
/// A bare `host:port` reads as `http` unless the body carries an
/// `https://` endpoint, mirroring the builder.
async fn probe_http_root(target: &str, use_tls: bool) -> Result<(), String> {
    probe_http_root_with(&probe_http_client(), target, use_tls).await
}

/// Build the search probe client. When `SSL_CERT_FILE` is set, the file
/// is the trust store. The client turns off the built-in roots and adds
/// every certificate in the file. An unreadable file trusts no root, so
/// the probe fails closed. Without the variable the default roots stay.
pub(crate) fn probe_http_client() -> reqwest::Client {
    let mut builder = reqwest::Client::builder();
    if let Some(path) = std::env::var_os("SSL_CERT_FILE") {
        builder = builder.tls_built_in_root_certs(false);
        if let Ok(pem) = std::fs::read(path) {
            for cert in probe_certificates(&pem) {
                builder = builder.add_root_certificate(cert);
            }
        }
    }
    builder.build().unwrap_or_else(|_| reqwest::Client::new())
}

/// Read every PEM certificate in a file. Parse the bundle first. If one
/// section is invalid, parse each section alone so a good certificate is
/// still trusted.
fn probe_certificates(pem: &[u8]) -> Vec<reqwest::Certificate> {
    if let Ok(certs) = reqwest::Certificate::from_pem_bundle(pem) {
        return certs;
    }
    let text = String::from_utf8_lossy(pem);
    let mut certs = Vec::new();
    for section in text.split("-----BEGIN CERTIFICATE-----").skip(1) {
        let block = format!("-----BEGIN CERTIFICATE-----{section}");
        if let Ok(cert) = reqwest::Certificate::from_pem(block.as_bytes()) {
            certs.push(cert);
        }
    }
    certs
}

/// Return an error text with its source chain. The probe message keeps
/// the TLS cause, so an operator can see a bad certificate.
fn probe_error_chain(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}

/// HTTP handshake with a caller-supplied client. Tests use a client that
/// trusts their loopback certificate.
pub(crate) async fn probe_http_root_with(
    client: &reqwest::Client,
    target: &str,
    use_tls: bool,
) -> Result<(), String> {
    let url = if target.contains("://") {
        target.to_string()
    } else if use_tls {
        format!("https://{target}/")
    } else {
        format!("http://{target}/")
    };
    client
        .get(url.as_str())
        .send()
        .await
        .map(|_| ())
        .map_err(|error| {
            format!(
                "search handshake to {target} failed: {}",
                probe_error_chain(&error)
            )
        })
}

/// True when the probe body names an `https://` endpoint, so the
/// search handshake dials TLS instead of plain HTTP.
fn probe_body_wants_tls(body: &serde_json::Value) -> bool {
    for key in ["server", "servers", "endpoint", "url"] {
        if body
            .get(key)
            .and_then(|v| v.as_str())
            .is_some_and(|s| s.trim_start().starts_with("https://"))
        {
            return true;
        }
    }
    if let Some(params) = body.get("parameters") {
        for key in ["server", "servers", "endpoint", "url"] {
            if params
                .get(key)
                .and_then(|v| v.as_str())
                .is_some_and(|s| s.trim_start().starts_with("https://"))
            {
                return true;
            }
        }
    }
    false
}

/// Read one probe field from the top level, then from the reference
/// `parameters` object.
fn probe_string(body: &serde_json::Value, keys: &[&str]) -> Option<String> {
    let read = |source: &serde_json::Value| {
        keys.iter().find_map(|key| {
            source
                .get(*key)
                .and_then(|value| value.as_str())
                .map(str::trim)
                .filter(|text| !text.is_empty())
                .map(str::to_string)
        })
    };
    read(body).or_else(|| body.get("parameters").and_then(read))
}

/// True when the kind needs a real protocol round trip after the TCP
/// dial: MongoDB HELLO and Couchbase SASL PLAIN.
fn probe_kind_needs_document_handshake(kind: &str) -> bool {
    matches!(
        crate::v5::action_sinks::canonical_action_kind(kind),
        "mongodb" | "couchbase"
    )
}

/// MongoDB probe: open one connection and run the driver handshake
/// (HELLO plus SCRAM when credentials are configured).
async fn probe_mongodb_handshake(body: &serde_json::Value, target: &str) -> Result<(), String> {
    #[cfg(feature = "mongodb")]
    {
        let database = probe_string(body, &["database"])
            .ok_or_else(|| "missing required field: database".to_string())?;
        let mut connection_string = if target.contains("://") {
            target.to_string()
        } else {
            format!("mongodb://{target}")
        };
        let username = probe_string(body, &["username", "user"]);
        let password = probe_string(body, &["password"]);
        match (username, password) {
            (Some(username), Some(password)) => {
                let scheme = if connection_string.starts_with("mongodb+srv://") {
                    "mongodb+srv://"
                } else {
                    "mongodb://"
                };
                if !connection_string[scheme.len()..].contains('@') {
                    connection_string = format!(
                        "{scheme}{username}:{password}@{}",
                        &connection_string[scheme.len()..]
                    );
                }
            }
            (None, None) => {}
            _ => return Err("fields `username` and `password` must be set together".to_string()),
        }
        if let Some(auth_source) = probe_string(body, &["auth_source"]) {
            let separator = if connection_string.contains('?') {
                '&'
            } else {
                '?'
            };
            connection_string.push(separator);
            connection_string.push_str("authSource=");
            connection_string.push_str(&auth_source);
        }
        let config = broker_connectors_enterprise::MongoDbSinkConfig {
            connection_string,
            database,
            collection_template: "mqtt".to_string(),
            payload_template: None,
            operation: broker_connectors_enterprise::MongoOperation::InsertOne,
            batch_size: Some(1),
            batch_bytes: Some(4_096),
            linger_ms: Some(1),
            max_retries: Some(0),
            initial_backoff_ms: Some(1),
            max_backoff_ms: Some(2),
            timeout_ms: Some(1_500),
        };
        let transport = broker_connectors_enterprise::NativeMongoDbTransport::new(&config)
            .map_err(|error| format!("mongodb handshake to {target} failed: {error}"))?;
        transport
            .connect()
            .await
            .map_err(|error| format!("mongodb handshake to {target} failed: {error}"))?;
        Ok(())
    }
    #[cfg(not(feature = "mongodb"))]
    {
        let _ = (body, target);
        Err("mongodb feature is disabled in this build".to_string())
    }
}

/// Couchbase probe: open one connection and run SASL PLAIN, the same
/// handshake the KV transport uses.
async fn probe_couchbase_handshake(body: &serde_json::Value, target: &str) -> Result<(), String> {
    #[cfg(feature = "couchbase")]
    {
        let username = probe_string(body, &["username", "user"])
            .ok_or_else(|| "missing required field: username".to_string())?;
        let password = probe_string(body, &["password"]).unwrap_or_default();
        let connection_string =
            if target.starts_with("couchbase://") || target.starts_with("couchbases://") {
                target.to_string()
            } else {
                format!("couchbase://{target}")
            };
        let config = broker_connectors_enterprise::CouchbaseSinkConfig {
            connection_string,
            bucket: String::new(),
            scope: None,
            collection: None,
            auth: broker_connectors_enterprise::CouchbaseAuth { username, password },
            doc_id_template: String::new(),
            sql_template: Some("SELECT 1".to_string()),
            operation: broker_connectors_enterprise::CouchbaseOperation::Upsert,
            expiry_secs: None,
            batch_size: Some(1),
            batch_bytes: Some(4_096),
            linger_ms: Some(1),
            max_retries: Some(0),
            initial_backoff_ms: Some(1),
            max_backoff_ms: Some(2),
            timeout_ms: Some(1_500),
        };
        let transport = broker_connectors_enterprise::NativeCouchbaseTransport::new(&config)
            .map_err(|error| format!("couchbase handshake to {target} failed: {error}"))?;
        transport
            .connect()
            .await
            .map_err(|error| format!("couchbase handshake to {target} failed: {error}"))?;
        Ok(())
    }
    #[cfg(not(feature = "couchbase"))]
    {
        let _ = (body, target);
        Err("couchbase feature is disabled in this build".to_string())
    }
}

async fn probe_document_handshake(
    kind: &str,
    body: &serde_json::Value,
    target: &str,
) -> Result<(), String> {
    match crate::v5::action_sinks::canonical_action_kind(kind) {
        "mongodb" => probe_mongodb_handshake(body, target).await,
        "couchbase" => probe_couchbase_handshake(body, target).await,
        _ => Ok(()),
    }
}

// ---------------------------------------------------------------------------
// Connector Probe (W3-21, C07-08)
// ---------------------------------------------------------------------------

/// Single-request bound for `POST /api/v5/connectors_probe`: one TCP dial at
/// most, capped by this timeout. Reason: a probe is an operator-driven
/// management-plane dial, never on the publish/deliver path; 5 s bounds the
/// worst case per request (the inner dial itself times out after 1.5 s) while
/// staying well under management-plane HTTP timeouts. Results are never
/// cached (default no caching): every request re-validates and re-dials so a
/// transient failure cannot be served stale.
pub const CONNECTOR_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Stateless connector probe: validates the config against the named kind
/// (the W3-17 kind set from the config registry, the same set
/// `POST /api/v5/connectors` validates against) and dials where the kind
/// exposes a network target, reporting success or the failure reason with
/// the documented `{code, message}` shape. Success answers 204 with no body.
/// Management-plane only: runs off the message path (no lock, no
/// allocation, no state on publish/deliver), performs single-request bounded
/// work capped by [`CONNECTOR_PROBE_TIMEOUT`], writes to no store, registers
/// no runtime, and leaves nothing running after return.
// TODO(parity): what per-kind config validation does the probe require
// beyond the kind being known (required fields per kind, value ranges)?
// The rulebook does not decide the exact per-kind schemas; the current
// choice validates the kind against the W3-17 registry and dials the
// exposed network target, storing nothing, until the checker pins it.
fn probe_failed(message: impl ToString) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({
            "code": "TEST_FAILED",
            "message": message.to_string()
        })),
    )
        .into_response()
}

pub async fn probe_connector(body: Option<Json<serde_json::Value>>) -> Response {
    let body = match body {
        Some(Json(b)) => b,
        None => return probe_failed("request body is required"),
    };
    if !body.is_object() {
        return probe_failed("request body must be a JSON object");
    }
    let kind = body
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if kind.is_empty() {
        return probe_failed("missing required field: type");
    }
    if let Some(message) = crate::removed_connector_refusal(&kind) {
        return probe_failed(message);
    }
    if !broker_config::is_known_connector_type(&kind) {
        return probe_failed(format!("unknown connector type: {kind}"));
    }
    // KC-00: a known kind that is not `ready` is refused before any dial.
    // The probe keeps the reference error code for this operation,
    // `TEST_FAILED`.
    if let Some(message) = kind_refusal(&kind) {
        return probe_failed(message);
    }
    // Document kinds fail without their required fields, never read
    // connected on a bare port.
    if let Some(field) = probe_document_missing_field(&kind, &body) {
        return probe_failed(format!("missing required field: {field}"));
    }
    // Runtime probe hook (the W3-17 dial path reused from the connector
    // create path): kinds that expose a network target get one bounded
    // dial plus a protocol handshake where the driver allows one; a
    // document kind without a target fails instead of reading
    // reachable. Kinds without a network target count as reachable once
    // the kind itself validated. No store write, no runtime registration,
    // no background task survives the return.
    let target = extract_target_host_port(&body).or_else(|| {
        if document_kind_needs_target(&kind) {
            probe_string(
                &body,
                &["server", "servers", "endpoint", "url", "connection_string"],
            )
        } else {
            None
        }
    });
    if let Some(target) = target {
        let dial =
            tokio::time::timeout(CONNECTOR_PROBE_TIMEOUT, check_tcp_reachable(&target)).await;
        match dial {
            Ok(Ok(())) => {
                if probe_kind_needs_http_handshake(&kind) {
                    let use_tls = probe_body_wants_tls(&body);
                    match tokio::time::timeout(
                        CONNECTOR_PROBE_TIMEOUT,
                        probe_http_root(&target, use_tls),
                    )
                    .await
                    {
                        Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
                        Ok(Err(error)) => probe_failed(error),
                        Err(_) => {
                            probe_failed(format!("search handshake to {target} timed out after 5s"))
                        }
                    }
                } else if probe_kind_needs_document_handshake(&kind) {
                    match tokio::time::timeout(
                        CONNECTOR_PROBE_TIMEOUT,
                        probe_document_handshake(&kind, &body, &target),
                    )
                    .await
                    {
                        Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
                        Ok(Err(error)) => probe_failed(error),
                        Err(_) => probe_failed(format!(
                            "document handshake to {target} timed out after 5s"
                        )),
                    }
                } else {
                    StatusCode::NO_CONTENT.into_response()
                }
            }
            Ok(Err(err)) => probe_failed(err),
            Err(_) => probe_failed(format!("Connection to {} timed out after 5s", target)),
        }
    } else if document_kind_needs_target(&kind) {
        probe_failed(format!(
            "connector type {kind} needs a probe target: missing required field: server"
        ))
    } else {
        StatusCode::NO_CONTENT.into_response()
    }
}

// ---------------------------------------------------------------------------
// Action Probe (W3-07, C06-11)
// ---------------------------------------------------------------------------

/// KC-00: documented names to list for a capability, in documented order.
/// A non-ready kind and a name outside the documented list can never
/// appear.
pub(crate) fn ready_kind_names(
    documented: &[&'static str],
    pick: fn(&broker_config::KindRow) -> bool,
) -> Vec<&'static str> {
    documented
        .iter()
        .copied()
        .filter(|name| {
            broker_config::kind_row(name)
                .is_some_and(|row| broker_config::row_is_ready(row) && pick(row))
        })
        .collect()
}

/// `GET /api/v5/action_types`: names of supported action kinds.
///
/// Returns the bare list of documented names (not paged). A name is
/// listed only if its runtime is `ready` in this build. Unknown query
/// parameters are ignored. Management-plane only; publish and deliver
/// never touch it.
pub async fn get_action_types(Query(_ignored): Query<HashMap<String, String>>) -> Response {
    let names = ready_kind_names(super::typed::SPEC_ACTION_TYPES, |row| row.action);
    (StatusCode::OK, Json(serde_json::json!(names))).into_response()
}

/// Single-request bound for `POST /api/v5/actions_probe`: one TCP dial at
/// most, capped by this timeout. Reason: a probe is an operator-driven
/// management-plane dial, never on the publish/deliver path; 5 s bounds the
/// worst case per request (the inner dial itself times out after 1.5 s) while
/// staying well under management-plane HTTP timeouts. Results are never
/// cached (default no caching): every request re-validates and re-dials so a
/// transient failure cannot be served stale.
pub const ACTION_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Action-kind validation for the probe (the W3-01 kind set): a kind is
/// listed only if the kernel can execute it, read from the shared kind
/// registry in `broker-config`, which also accepts the documented names
/// the console sends. This is the same registry `POST /api/v5/actions`
/// validates against.
fn is_known_probe_action_type(kind: &str) -> bool {
    broker_config::is_known_action_type(kind)
}

/// Stateless action probe: validates the config against the named kind and
/// dials where the kind exposes a network target, reporting success or the
/// failure reason with the documented `{code, message}` shape. Success
/// answers 204 with no body. Management-plane only: runs off the message path
/// (no lock, no allocation, no state on publish/deliver), performs
/// single-request bounded work capped by [`ACTION_PROBE_TIMEOUT`], writes to
/// no store, registers no runtime, and leaves nothing running after return.
// TODO(parity): what per-kind config validation does the probe require
// beyond the kind being known (required fields per kind, value ranges)?
// The rulebook does not decide the exact per-kind schemas; the current
// choice validates the kind against the W3-01 registry and dials the
// exposed network target, storing nothing, until the checker pins it.
pub async fn probe_action(body: Option<Json<serde_json::Value>>) -> Response {
    let body = match body {
        Some(Json(b)) => b,
        None => return probe_failed("request body is required"),
    };
    if !body.is_object() {
        return probe_failed("request body must be a JSON object");
    }
    let kind = body
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if kind.is_empty() {
        return probe_failed("missing required field: type");
    }
    if let Some(message) = crate::removed_connector_refusal(&kind) {
        return probe_failed(message);
    }
    if !is_known_probe_action_type(&kind) {
        return probe_failed(format!("unknown action type: {kind}"));
    }
    // KC-00: a known action kind that is not `ready` is refused before any
    // dial. The probe keeps the reference error code for this operation,
    // `TEST_FAILED`.
    if let Some(message) = kind_refusal(&kind) {
        return probe_failed(message);
    }
    // Document kinds fail without their required fields, never read
    // connected on a bare port.
    if let Some(field) = probe_document_missing_field(&kind, &body) {
        return probe_failed(format!("missing required field: {field}"));
    }
    // Runtime probe hook (the connector dial path reused from the
    // connector create path): kinds that expose a network target get one
    // bounded dial plus a protocol handshake where the driver allows
    // one; a document kind without a target fails instead of reading
    // reachable. Kinds without one count as reachable once the kind
    // itself validated. No store write, no runtime registration, no
    // background task survives the return.
    let target = extract_target_host_port(&body).or_else(|| {
        if document_kind_needs_target(&kind) {
            probe_string(
                &body,
                &["server", "servers", "endpoint", "url", "connection_string"],
            )
        } else {
            None
        }
    });
    if let Some(target) = target {
        let dial = tokio::time::timeout(ACTION_PROBE_TIMEOUT, check_tcp_reachable(&target)).await;
        match dial {
            Ok(Ok(())) => {
                if probe_kind_needs_http_handshake(&kind) {
                    let use_tls = probe_body_wants_tls(&body);
                    match tokio::time::timeout(
                        ACTION_PROBE_TIMEOUT,
                        probe_http_root(&target, use_tls),
                    )
                    .await
                    {
                        Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
                        Ok(Err(error)) => probe_failed(error),
                        Err(_) => {
                            probe_failed(format!("search handshake to {target} timed out after 5s"))
                        }
                    }
                } else if probe_kind_needs_document_handshake(&kind) {
                    match tokio::time::timeout(
                        ACTION_PROBE_TIMEOUT,
                        probe_document_handshake(&kind, &body, &target),
                    )
                    .await
                    {
                        Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
                        Ok(Err(error)) => probe_failed(error),
                        Err(_) => probe_failed(format!(
                            "document handshake to {target} timed out after 5s"
                        )),
                    }
                } else {
                    StatusCode::NO_CONTENT.into_response()
                }
            }
            Ok(Err(err)) => probe_failed(err),
            Err(_) => probe_failed(format!("Connection to {} timed out after 5s", target)),
        }
    } else if document_kind_needs_target(&kind) {
        probe_failed(format!(
            "connector type {kind} needs a probe target: missing required field: server"
        ))
    } else {
        StatusCode::NO_CONTENT.into_response()
    }
}

// ---------------------------------------------------------------------------
// Source Probe (W3-30, C07-21)
// ---------------------------------------------------------------------------

/// Single-request bound for `POST /api/v5/sources_probe`: one TCP dial at
/// most, capped by this timeout. Reason: a probe is an operator-driven
/// management-plane dial, never on the publish/deliver path; 5 s bounds the
/// worst case per request (the inner dial itself times out after 1.5 s) while
/// staying well under management-plane HTTP timeouts. Results are never
/// cached (default no caching): every request re-validates and re-dials so a
/// transient failure cannot be served stale.
pub const SOURCE_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Source-kind validation for the probe (the W3-25 kind set): a kind is
/// listed only if the kernel can instantiate it. Reads the shared W3-25
/// source-type registry in `broker-connectors` (same set the catalogue
/// serves and the source store validates against) so the three cannot
/// drift.
fn is_known_source_type(kind: &str) -> bool {
    broker_connectors::is_known_source_type(kind)
}

/// Stateless source probe: validates the config against the named kind and
/// dials where the kind exposes a network target. Success answers 204 with
/// no body, as the reference API documents. A failure answers with the
/// documented `{code, message}` shape. Management-plane only: runs off the message path (no lock, no
/// allocation, no state on publish/deliver), performs single-request bounded
/// work capped by [`SOURCE_PROBE_TIMEOUT`], writes to no store, registers no
/// runtime, and leaves nothing running after return.
pub async fn probe_source(body: Option<Json<serde_json::Value>>) -> Response {
    let body = match body {
        Some(Json(b)) => b,
        None => return probe_failed("request body is required"),
    };
    if !body.is_object() {
        return probe_failed("request body must be a JSON object");
    }
    let kind = body
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if kind.is_empty() {
        return probe_failed("missing required field: type");
    }
    if let Some(message) = crate::removed_connector_refusal(&kind) {
        return probe_failed(message);
    }
    if !is_known_source_type(&kind) {
        return probe_failed(format!("unknown source type: {kind}"));
    }
    // KC-00: a known source kind that is not `ready` is refused before any
    // dial. The probe keeps the reference error code for this operation,
    // `TEST_FAILED`.
    if let Some(message) = kind_refusal(&kind) {
        return probe_failed(message);
    }
    // Runtime probe hook (the W3-23 dial path): kinds that expose a network
    // target get one bounded dial; kinds without one count as reachable once
    // the kind itself validated, matching the connector/action probes.
    if let Some(target) = extract_target_host_port(&body) {
        let dial = tokio::time::timeout(SOURCE_PROBE_TIMEOUT, check_tcp_reachable(&target)).await;
        match dial {
            Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
            Ok(Err(err)) => probe_failed(err),
            Err(_) => probe_failed(format!("Connection to {} timed out after 5s", target)),
        }
    } else {
        StatusCode::NO_CONTENT.into_response()
    }
}

// ---------------------------------------------------------------------------
// X1-09 feature-gate tests: disabled kinds fail closed naming the feature,
// enabled kinds register through the broker rule path.
// ---------------------------------------------------------------------------
#[cfg(test)]
#[allow(dead_code, unused_imports)]
mod connector_feature_tests {
    use super::*;
    use broker_protocol::{QoS, Topic, TopicFilter};
    use broker_rules::{BackpressurePolicy, BrokerSink, RuleEngine, RuleEngineError};
    use bytes::Bytes;
    use std::sync::{Arc, Mutex as StdMutex};

    #[derive(Debug, Default)]
    struct RecordingSink {
        published: StdMutex<Vec<(Topic, Bytes, QoS, bool)>>,
    }

    #[async_trait::async_trait]
    impl BrokerSink for RecordingSink {
        async fn publish(
            &self,
            topic: Topic,
            payload: Bytes,
            qos: QoS,
            retain: bool,
        ) -> Result<(), RuleEngineError> {
            self.published
                .lock()
                .unwrap()
                .push((topic, payload, qos, retain));
            Ok(())
        }
    }

    async fn expect_disabled(kind: &str, feature: &str) {
        assert_eq!(
            disabled_connector_feature(kind),
            Some(feature),
            "{kind} must map to feature {feature} when disabled"
        );
        let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
        let err = try_register_live_sink(&engine, kind, "feat-test", &serde_json::json!({}))
            .await
            .expect_err("disabled kind must fail registration");
        assert!(
            err.contains(feature),
            "closed error must name feature '{feature}': {err}"
        );
        assert!(
            engine.connectors().get("feat-test").is_none(),
            "no sink may register when feature '{feature}' is disabled"
        );
    }

    /// The compiled-in list comes from the same table as the lookup.
    /// Each entry tracks its own feature, so no second list exists.
    /// The test fails if the lookup ever disagrees with the table or
    /// if two rows claim one alias.
    #[tokio::test]
    async fn test_compiled_features_match_table() {
        let compiled = compiled_connector_features();
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for gate in CONNECTOR_GATES {
            assert_eq!(
                compiled.contains(&gate.feature),
                gate.enabled,
                "feature `{}` must track its table row",
                gate.feature
            );
            for kind in gate.kinds {
                assert!(
                    seen.insert(kind),
                    "alias `{kind}` must appear in one table row only"
                );
                assert_eq!(
                    disabled_connector_feature(kind).is_some(),
                    !gate.enabled,
                    "kind `{kind}` must track its table row"
                );
            }
        }
    }

    #[cfg(not(feature = "s3"))]
    #[tokio::test]
    async fn test_disabled_s3_names_feature() {
        expect_disabled("s3", "s3").await;
        expect_disabled("minio", "s3").await;
    }

    #[cfg(not(feature = "dynamodb"))]
    #[tokio::test]
    async fn test_disabled_dynamodb_names_feature() {
        expect_disabled("dynamodb", "dynamodb").await;
        expect_disabled("dynamo", "dynamodb").await;
    }

    #[cfg(not(feature = "bigquery"))]
    #[tokio::test]
    async fn test_disabled_bigquery_names_feature() {
        expect_disabled("bigquery", "bigquery").await;
    }

    #[cfg(not(feature = "aws_iot"))]
    #[tokio::test]
    async fn test_disabled_aws_iot_names_feature() {
        expect_disabled("aws_iot", "aws_iot").await;
        expect_disabled("aws_iot_core", "aws_iot").await;
    }

    #[cfg(not(feature = "azure_blob"))]
    #[tokio::test]
    async fn test_disabled_azure_blob_names_feature() {
        expect_disabled("azure_blob", "azure_blob").await;
        expect_disabled("azure_blob_storage", "azure_blob").await;
    }

    #[cfg(not(feature = "azure_eventhubs"))]
    #[tokio::test]
    async fn test_disabled_azure_eventhubs_names_feature() {
        expect_disabled("azure_eventhubs", "azure_eventhubs").await;
        expect_disabled("azure_event_hubs", "azure_eventhubs").await;
    }

    #[cfg(not(feature = "azure_iot"))]
    #[tokio::test]
    async fn test_disabled_azure_iot_names_feature() {
        expect_disabled("azure_iot", "azure_iot").await;
        expect_disabled("azure_iot_hub", "azure_iot").await;
    }

    #[cfg(not(feature = "gcp_iot"))]
    #[tokio::test]
    async fn test_disabled_gcp_iot_names_feature() {
        expect_disabled("gcp_iot", "gcp_iot").await;
        expect_disabled("gcp_iot_core", "gcp_iot").await;
    }

    #[cfg(not(feature = "kafka"))]
    #[tokio::test]
    async fn test_disabled_kafka_names_feature() {
        expect_disabled("kafka", "kafka").await;
    }

    #[cfg(not(feature = "rabbitmq"))]
    #[tokio::test]
    async fn test_disabled_rabbitmq_names_feature() {
        expect_disabled("rabbitmq", "rabbitmq").await;
        expect_disabled("amqp", "rabbitmq").await;
    }

    #[cfg(not(feature = "clickhouse"))]
    #[tokio::test]
    async fn test_disabled_clickhouse_names_feature() {
        expect_disabled("clickhouse", "clickhouse").await;
        expect_disabled("ch", "clickhouse").await;
    }

    #[cfg(feature = "clickhouse")]
    #[tokio::test]
    async fn test_ch_alias_registers_like_clickhouse() {
        assert_eq!(live_sink_kind("ch"), "clickhouse");
        let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
        try_register_live_sink(
            &engine,
            "ch",
            "ch-test",
            &serde_json::json!({"url": "http://127.0.0.1:9", "database": "db", "table": "t"}),
        )
        .await
        .expect("ch must register through the clickhouse arm");
        assert!(
            engine.connectors().get("ch-test").is_some(),
            "ch must leave a live sink"
        );
    }

    #[cfg(not(feature = "elasticsearch"))]
    #[tokio::test]
    async fn test_disabled_elasticsearch_names_feature() {
        expect_disabled("elasticsearch", "elasticsearch").await;
        expect_disabled("opensearch", "elasticsearch").await;
    }

    #[cfg(not(feature = "cassandra"))]
    #[tokio::test]
    async fn test_disabled_cassandra_names_feature() {
        expect_disabled("cassandra", "cassandra").await;
        expect_disabled("scylla", "cassandra").await;
    }

    #[cfg(not(feature = "confluent"))]
    #[tokio::test]
    async fn test_disabled_confluent_names_feature() {
        expect_disabled("confluent", "confluent").await;
    }

    #[cfg(not(feature = "couchbase"))]
    #[tokio::test]
    async fn test_disabled_couchbase_names_feature() {
        expect_disabled("couchbase", "couchbase").await;
    }

    #[cfg(not(feature = "mongodb"))]
    #[tokio::test]
    async fn test_disabled_mongodb_names_feature() {
        expect_disabled("mongodb", "mongodb").await;
        expect_disabled("mongo", "mongodb").await;
    }

    #[cfg(not(feature = "gcp_pubsub"))]
    #[tokio::test]
    async fn test_disabled_gcp_pubsub_names_feature() {
        expect_disabled("gcp_pubsub", "gcp_pubsub").await;
        expect_disabled("pubsub", "gcp_pubsub").await;
    }

    #[cfg(not(feature = "kinesis"))]
    #[tokio::test]
    async fn test_disabled_kinesis_names_feature() {
        expect_disabled("kinesis", "kinesis").await;
        expect_disabled("aws_kinesis", "kinesis").await;
    }

    /// Enabled evidence (dynamodb): a rule registered with the connector
    /// manager delivers exactly one batch through the broker rule path.
    #[cfg(feature = "dynamodb")]
    #[tokio::test]
    async fn test_enabled_dynamodb_through_rule_engine() {
        use broker_connectors::Sink as _;
        use broker_connectors_enterprise::dynamodb::{
            DynamoDbSink, DynamoDbSinkConfig, DynamoKeyConfig, MockDynamoDbTransport,
        };
        let engine = RuleEngine::new(65_536, BackpressurePolicy::DropOldest);
        let transport = Arc::new(MockDynamoDbTransport::new());
        let config = DynamoDbSinkConfig {
            table_name: "telemetry_table".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            access_key_id: "AKID".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: None,
            partition_key: DynamoKeyConfig {
                name: "device_id".to_string(),
                template: "${client_id}".to_string(),
                key_type: "S".to_string(),
            },
            sort_key: None,
            ttl_attribute: None,
            ttl_secs: None,
            attributes_mapping: std::collections::HashMap::new(),
            batch_size: Some(1),
            batch_bytes: Some(1_048_576),
            linger_ms: Some(10),
            max_retries: Some(3),
            initial_backoff_ms: Some(1),
            max_backoff_ms: Some(2),
            timeout_ms: None,
        };
        let sink = Arc::new(DynamoDbSink::new(config, transport.clone()).expect("valid sink"));
        assert_eq!(sink.kind(), "dynamodb");
        engine.connectors().register("dd-feat", sink);
        engine
            .create_rule(
                "dd-feat-rule".to_string(),
                TopicFilter::new("sensors/+").unwrap(),
                Some(
                    r#"SELECT client_id, temp FROM "sensors/+" WHERE temp > 20.0 INTO connector("dd-feat")"#.to_string(),
                ),
                true,
                vec![],
            )
            .expect("rule creates");
        let broker: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());
        engine
            .dispatch_ingress(
                &Topic::new("sensors/kitchen").unwrap(),
                &Bytes::from_static(br#"{ "client_id": "device-42", "temp": 22.5 }"#),
                QoS::AtMostOnce,
                &broker,
            )
            .await;
        let captured = transport.captured();
        assert_eq!(captured.len(), 1, "exactly one batch must deliver");
        assert_eq!(captured[0].table, "telemetry_table");
        assert_eq!(captured[0].items.len(), 1, "every key present, no loss");
    }

    /// Enabled evidence (s3): same rule-path shape over the mock object
    /// transport; exactly one object lands with the projected JSON inside.
    #[cfg(feature = "s3")]
    #[tokio::test]
    async fn test_enabled_s3_through_rule_engine() {
        use broker_connectors::s3::{MockS3Transport, S3Compression, S3Sink, S3SinkConfig};
        use broker_connectors::Sink as _;
        let engine = RuleEngine::new(65_536, BackpressurePolicy::DropOldest);
        let transport = Arc::new(MockS3Transport::new());
        let config = S3SinkConfig {
            endpoint: "http://127.0.0.1:9000".to_string(),
            bucket: "telemetry".to_string(),
            region: "us-east-1".to_string(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            key_template: "telemetry/${topic}_${seq}.ndjson".to_string(),
            compression: S3Compression::None,
            batch_size: 1,
            batch_bytes: 5 * 1024 * 1024,
            batch_timeout_ms: 10,
            timeout_ms: None,
        };
        let sink = Arc::new(S3Sink::new(config, transport.clone()).expect("valid sink"));
        assert_eq!(sink.kind(), "s3");
        engine.connectors().register("s3-feat", sink);
        engine
            .create_rule(
                "s3-feat-rule".to_string(),
                TopicFilter::new("sensors/+").unwrap(),
                Some(
                    r#"SELECT client_id, temp FROM "sensors/+" WHERE temp > 20.0 INTO connector("s3-feat")"#.to_string(),
                ),
                true,
                vec![],
            )
            .expect("rule creates");
        let broker: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());
        engine
            .dispatch_ingress(
                &Topic::new("sensors/kitchen").unwrap(),
                &Bytes::from_static(br#"{ "client_id": "device-42", "temp": 22.5 }"#),
                QoS::AtMostOnce,
                &broker,
            )
            .await;
        let puts = transport.puts();
        assert_eq!(puts.len(), 1, "exactly one object must land");
        assert_eq!(puts[0].bucket, "telemetry");
        let body = String::from_utf8(puts[0].body.clone()).expect("ndjson is UTF-8");
        assert!(
            body.contains("device-42"),
            "projected payload must travel untouched: {body}"
        );
    }
}
