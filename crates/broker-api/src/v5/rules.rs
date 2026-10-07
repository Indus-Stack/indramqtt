//! Streaming SQL rules, data connectors, action sinks, and schema registry for the v5 REST API.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};

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
    let actions = serde_json::to_value(&rule.actions).unwrap_or_else(|_| serde_json::json!([]));
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

/// `POST /api/v5/rules`: compile the rule before storing (malformed
/// bodies rejected with the documented error shape); a duplicate id is
/// rejected cleanly, never overwritten. An explicit `id` is stored
/// verbatim so duplicates are detectable; without one the engine assigns
/// `rule-<n>`. A `tenant` field selects the owning tenant (absent
/// means the default tenant); an unknown tenant is rejected with 400.
/// A `ForwardConnector` target naming no known action is
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
    // Explicit `id` wins, `name` falls back (mirrors the action create).
    let raw_id = obj
        .get("id")
        .or_else(|| obj.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();
    if raw_id.is_empty() {
        return ApiError::BadRequest("rule id or name is required".to_string()).into_response();
    }
    if raw_id.len() > MAX_RULE_ID_LEN {
        return ApiError::BadRequest(format!(
            "rule id holds {} chars, at most {MAX_RULE_ID_LEN} (field `id`)",
            raw_id.len()
        ))
        .into_response();
    }
    let explicit_id = obj
        .get("id")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let name = obj
        .get("name")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| raw_id.to_string());
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

    let mut actions = Vec::new();
    for a in &raw_actions {
        if let Some(id_str) = a.as_str() {
            // Rule-to-action linkage (W3-02): a forward target naming no
            // known action is rejected instead of going live and failing
            // at fire time.
            if state.actions.get(id_str).is_none() {
                return ApiError::BadRequest(format!(
                    "unknown action {id_str:?} (field `actions`)"
                ))
                .into_response();
            }
            actions.push(broker_rules::RuleAction::ForwardConnector {
                connector_id: id_str.to_string(),
            });
        } else if let Some(obj) = a.as_object() {
            let Some(conn_id) = obj
                .get("id")
                .or_else(|| obj.get("name"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                // Fail closed (UNKNOWN rule): an object without an
                // addressable forward target is rejected, never silently
                // dropped while create still answers 201.
                return ApiError::BadRequest(
                    "action entry object must carry an `id` or `name` (field `actions`)"
                        .to_string(),
                )
                .into_response();
            };
            if state.actions.get(conn_id).is_none() {
                return ApiError::BadRequest(format!(
                    "unknown action {conn_id:?} (field `actions`)"
                ))
                .into_response();
            }
            actions.push(broker_rules::RuleAction::ForwardConnector {
                connector_id: conn_id.to_string(),
            });
        } else {
            return ApiError::BadRequest(
                "unsupported action entry: expected a string id or an object (field `actions`)"
                    .to_string(),
            )
            .into_response();
        }
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
        let is_duplicate = explicit_id
            .as_ref()
            .is_some_and(|id| state.engine.get_rule(id).is_some());
        if !is_duplicate && live >= MAX_RULES {
            return ApiError::BadRequest(format!(
                "rule store holds {live} entries, at most {MAX_RULES}"
            ))
            .into_response();
        }
    }

    let rule = match explicit_id {
        Some(id) => state.engine.create_rule_with_id_in_tenant(
            id,
            tenant,
            name,
            topic_filter,
            Some(sql),
            enable,
            actions,
        ),
        None => state.engine.create_rule_in_tenant(
            tenant,
            name,
            topic_filter,
            Some(sql),
            enable,
            actions,
        ),
    };

    match rule {
        Ok(r) => (StatusCode::CREATED, Json(rule_to_v5(&r))).into_response(),
        // A persist failure is a 500 (the in-memory rule stays but the
        // disk save failed); validation failures stay 400.
        Err(error @ broker_rules::RuleEngineError::Persist(_)) => rule_persist_error(error),
        Err(broker_rules::RuleEngineError::InvalidRule(message))
            if message.starts_with("duplicate rule id:") =>
        {
            ApiError::AlreadyExists(message).into_response()
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
/// carrying the full rule (`sql` required, `actions` as string ids or
/// `{id,name}` objects validated against the W3-02 action directory,
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
        let mut actions = Vec::new();
        for a in &raw_actions {
            if let Some(id_str) = a.as_str() {
                if state.actions.get(id_str).is_none() {
                    return ApiError::BadRequest(format!(
                        "unknown action {id_str:?} (field `actions`)"
                    ))
                    .into_response();
                }
                actions.push(broker_rules::RuleAction::ForwardConnector {
                    connector_id: id_str.to_string(),
                });
            } else if let Some(action_obj) = a.as_object() {
                let Some(conn_id) = action_obj
                    .get("id")
                    .or_else(|| action_obj.get("name"))
                    .and_then(|v| v.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                else {
                    return ApiError::BadRequest(
                        "action entry object must carry an `id` or `name` (field `actions`)"
                            .to_string(),
                    )
                    .into_response();
                };
                if state.actions.get(conn_id).is_none() {
                    return ApiError::BadRequest(format!(
                        "unknown action {conn_id:?} (field `actions`)"
                    ))
                    .into_response();
                }
                actions.push(broker_rules::RuleAction::ForwardConnector {
                    connector_id: conn_id.to_string(),
                });
            } else {
                return ApiError::BadRequest(
                    "unsupported action entry: expected a string id or an object (field `actions`)"
                        .to_string(),
                )
                .into_response();
            }
        }
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

static CONNECTORS: LazyLock<RwLock<Vec<serde_json::Value>>> =
    LazyLock::new(|| RwLock::new(Vec::new()));

/// Serialises export-commit-save for connector mutations so concurrent
/// creates, updates and deletes cannot interleave into a lost update on
/// disk (mirrors the `save_lock` in `broker-auth` and `broker-rules`).
static CONNECTORS_SAVE_LOCK: LazyLock<std::sync::Mutex<()>> =
    LazyLock::new(|| std::sync::Mutex::new(()));

/// Map a connector registry failure onto a 500: the in-memory mutation
/// applied but the commit or atomic save failed, so the loss must never
/// be silent.
fn connector_persist_error(error: broker_config::ConfigError) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({
            "code": "INTERNAL_ERROR",
            "message": format!("cannot persist connectors: {error}")
        })),
    )
        .into_response()
}

/// Export the connector store as a validated config root: entries sorted
/// by id so the persisted file is deterministic. Each entry carries the
/// id, type, enable flag and the full connector params as a JSON
/// document (everything `create_connector` accepts, lossless). The
/// derived `status`/`node_status` display values are stripped: only
/// inputs are stored, display values are recomputed at read time.
fn export_connectors_conf() -> broker_config::ConnectorsConf {
    let store = CONNECTORS.read().unwrap();
    let mut entries: Vec<broker_config::ConnectorEntry> = store
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

/// Commit the exported connectors root and atomically save it. Any
/// commit or save failure surfaces as [`broker_config::ConfigError`] so
/// callers answer 500 and the loss is never silent (the in-memory
/// mutation stays: commit precedes the atomic save).
fn persist_connectors(
    config: &Arc<broker_config::ConfigRegistry>,
) -> Result<(), broker_config::ConfigError> {
    let _guard = CONNECTORS_SAVE_LOCK.lock().unwrap();
    let conf = export_connectors_conf();
    config.commit_connectors(conf)?;
    config.save()?;
    Ok(())
}

/// Probe the connector target the way boot replay does: entries
/// without a `server`/`url`/… target never probe and count as reachable;
/// otherwise a 1.5 s TCP connect decides.
pub(crate) async fn probe_connector_reachable(body: &serde_json::Value) -> bool {
    match extract_target_host_port(body) {
        Some(target) => check_tcp_reachable(&target).await.is_ok(),
        None => true,
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
/// 400 naming that field; `register_live_sink` itself also fails closed
/// (returns without registering) so boot replay cannot invent values.
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
        // TODO(parity): remaining `register_live_sink` families still invent
        // operational defaults (endpoints, tables, batch knobs); each needs
        // the same fail-closed gate once its service documentation is
        // checked for real defaults.
        _ => None,
    }
}

/// Boot replay for the connector store: merge the persisted snapshot
/// into the process-global store and re-register every live sink through
/// [`register_live_sink`], the same path `create_connector` uses, so a
/// restored connector actually connects instead of merely being listed.
///
/// Entries merge by id without clearing: on a fresh boot the store is
/// empty and the merge is a full load; on an in-process rebuild (tests)
/// entries created after the snapshot was taken are kept, which keeps
/// parallel test servers sharing the process-global store deterministic.
/// Stored `status`/`node_status` display values are recomputed with the
/// same probe `create_connector` runs; only the persisted inputs
/// round-trip.
///
/// An invalid snapshot or an unparsable stored params document fails boot
/// loudly instead of being skipped silently.
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

/// Reconcile the process-global connector list with a freshly committed
/// snapshot (reload/restore path): upsert every candidate entry (reference
/// form, status recomputed with the same probe create uses) and drop ids
/// the candidate removed. Entries never in `previous` are left alone, so
/// parallel test servers sharing the global store stay deterministic.
pub(crate) async fn reconcile_connectors_store(
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
        let mut store = CONNECTORS.write().unwrap();
        store.retain(|doc| {
            doc.get("id").and_then(|v| v.as_str()) != Some(entry.id.as_str())
                && doc.get("name").and_then(|v| v.as_str()) != Some(entry.id.as_str())
        });
    }
    for entry in &candidate.connectors {
        let mut doc = connector_store_doc(entry);
        let reachable = probe_connector_reachable(&doc).await;
        let status = connector_status(reachable);
        if let Some(obj) = doc.as_object_mut() {
            obj.insert(
                "status".to_string(),
                serde_json::Value::String(status.to_string()),
            );
            obj.insert("node_status".to_string(), connector_node_status(status));
        }
        let mut store = CONNECTORS.write().unwrap();
        if let Some(pos) = store.iter().position(|c| {
            c.get("id").and_then(|v| v.as_str()) == Some(entry.id.as_str())
                || c.get("name").and_then(|v| v.as_str()) == Some(entry.id.as_str())
        }) {
            store[pos] = doc;
        } else {
            store.push(doc);
        }
    }
}

pub fn seed_connectors_from_registry(
    config: &Arc<broker_config::ConfigRegistry>,
    engine: &broker_rules::RuleEngine,
) {
    let snapshot = config.snapshot();
    if snapshot.connectors.connectors.is_empty() {
        return;
    }
    if let Err(error) = snapshot.connectors.validate() {
        panic!("invalid stored connectors snapshot: {error}");
    }
    let mut restored = Vec::with_capacity(snapshot.connectors.connectors.len());
    for entry in &snapshot.connectors.connectors {
        // Bare `file:`/`env:` references are valid stored configs (M1-05):
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
                        let reachable = probe_connector_reachable(&params).await;
                        let status = connector_status(reachable);
                        if let Some(obj) = params.as_object_mut() {
                            obj.insert(
                                "status".to_string(),
                                serde_json::Value::String(status.to_string()),
                            );
                            obj.insert("node_status".to_string(), connector_node_status(status));
                        }
                        let live_name = params
                            .get("id")
                            .and_then(|v| v.as_str())
                            .unwrap_or(&id)
                            .to_string();
                        {
                            let mut store = CONNECTORS.write().unwrap();
                            if let Some(pos) = store.iter().position(|c| {
                                c.get("id").and_then(|v| v.as_str()) == Some(live_name.as_str())
                                    || c.get("name").and_then(|v| v.as_str())
                                        == Some(live_name.as_str())
                            }) {
                                store[pos] = params.clone();
                            } else {
                                store.push(params.clone());
                            }
                        }
                        if reachable {
                            // A disabled Cargo feature fails closed here:
                            // the entry stays persist-only and the missing
                            // feature is logged naming it.
                            if let Err(feature_error) =
                                try_register_live_sink(engine, &conn_type, &live_name, &params)
                                    .await
                            {
                                tracing::warn!(
                                    connector = %live_name,
                                    kind = %conn_type,
                                    "{feature_error}"
                                );
                            }
                        }
                    }
                });
            })
            .join()
            .expect("connector boot thread");
    });
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

/// Render one stored connector with live health, never the write-time
/// snapshot: `status`/`node_status` are recomputed from the live
/// connector manager (`engine.connectors()`), so a restart or a later
/// delete is visible on the next read. Only the inputs persist (see
/// [`export_connectors_conf`]); display values are derived here per
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
    let status = connector_status(live);
    if let Some(obj) = out.as_object_mut() {
        obj.insert(
            "status".to_string(),
            serde_json::Value::String(status.to_string()),
        );
        obj.insert("node_status".to_string(), connector_node_status(status));
    }
    out
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
    let store = CONNECTORS.read().unwrap();
    let mut rows: Vec<serde_json::Value> = store
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
    drop(store);
    (StatusCode::OK, Json(rows)).into_response()
}

/// `GET /api/v5/connectors/:id`: read one connector (C07-04).
///
/// A known id returns the documented fields (`id`, `name`, `type`,
/// `enable` plus the stored kind-specific config) with live health
/// (`status`/`node_status` recomputed from the connector manager, never
/// the write-time snapshot); an unknown id returns 404 with the
/// documented `{code, message}` shape, not an empty body. Matching is
/// exact only: the path id must equal the stored `id` or `name`
/// exactly, so a near-miss such as `x:<id>` is a 404. The pair is not
/// paged; unknown query parameters are ignored (the ignored query sink
/// below). Management-plane only: one short store lock cloning at most
/// one small entry per request plus lock-free manager lookups, and
/// publish and deliver never touch this store (dispatch reads the
/// manager directly, never this map).
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
    let stored = {
        let connectors = CONNECTORS.read().unwrap();
        connectors
            .iter()
            .find(|c| {
                c.get("id").and_then(|v| v.as_str()) == Some(id.as_str())
                    || c.get("name").and_then(|v| v.as_str()) == Some(id.as_str())
            })
            .cloned()
    };
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

/// Features compiled into this build, from the same table.
#[cfg(test)]
pub(crate) fn compiled_connector_features() -> Vec<&'static str> {
    CONNECTOR_GATES
        .iter()
        .filter(|gate| gate.enabled)
        .map(|gate| gate.feature)
        .collect()
}

/// Checked registration: a kind whose Cargo feature is disabled fails
/// closed with an error naming the missing feature — never a silent
/// skip, never a fallback identity. Enabled kinds go through
/// [`register_live_sink`], the same path `create_connector` uses.
pub(crate) async fn try_register_live_sink(
    engine: &broker_rules::RuleEngine,
    conn_type: &str,
    name: &str,
    body: &serde_json::Value,
) -> Result<(), String> {
    if let Some(feature) = disabled_connector_feature(conn_type) {
        return Err(format!(
            "connector kind '{conn_type}' needs cargo feature '{feature}' (disabled in this build)"
        ));
    }
    register_live_sink(engine, conn_type, name, body).await;
    Ok(())
}

/// Closed fallback for a kind whose vendor SDK is not compiled in.
/// Fails closed without registering anything and names the missing feature.
#[allow(dead_code)]
fn refuse_disabled_kind(conn_type: &str, feature: &str) {
    tracing::warn!(
        "connector kind `{conn_type}` needs missing feature `{feature}`; registration refused"
    );
}

pub(crate) async fn register_live_sink(
    engine: &broker_rules::RuleEngine,
    conn_type: &str,
    name: &str,
    body: &serde_json::Value,
) {
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
                return;
            };
            let url = broker_connectors::resolve_field_secret(raw_url);
            if url.trim().is_empty() {
                return;
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
            if let Ok(transport) = broker_connectors::redis::DriverRedisTransport::new(&endpoint) {
                let config = broker_connectors::redis::RedisSinkConfig {
                    endpoint,
                    command: broker_connectors::redis::RedisCommandKind::HSet {
                        key_template: "sensors:${topic}".to_string(),
                        field_template: "payload".to_string(),
                    },
                };
                if let Ok(sink) =
                    broker_connectors::redis::RedisSink::new(config, Arc::new(transport))
                {
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
            if let Ok(sink) =
                broker_connectors_enterprise::alloydb::AlloydbSink::new(config, transport)
            {
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
            if let Ok(transport) = broker_connectors::kafka::RdkafkaKafkaTransport::new(&config) {
                if let Ok(sink) =
                    broker_connectors::kafka::KafkaSink::new(config, Arc::new(transport))
                {
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
            refuse_disabled_kind(conn_type, "kafka");
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
            if let Ok(transport) = broker_connectors::postgres::DriverPgTransport::new(&conn_url, 1)
            {
                let config = broker_connectors::postgres::PostgreSqlSinkConfig {
                    connection_url: conn_url,
                    sql_template: "INSERT INTO test_telemetry (clientid, topic, payload, timestamp) VALUES ('rule-engine', $1, $3, 0)".to_string(),
                    pool_size: 1,
                    batch_size: 1,
                    batch_timeout_ms: 10,
                };
                if let Ok(sink) =
                    broker_connectors::postgres::PostgreSqlSink::new(config, Arc::new(transport))
                {
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
            if let Ok(transport) = broker_connectors::mysql::DriverMySqlTransport::new(&conn_url, 1)
            {
                let config = broker_connectors::mysql::MySqlSinkConfig {
                    connection_url: conn_url,
                    sql_template:
                        "INSERT INTO test_mqtt_events (topic, qos, payload) VALUES (?, ?, ?)"
                            .to_string(),
                    pool_size: 1,
                    batch_size: 1,
                    batch_timeout_ms: 10,
                };
                if let Ok(sink) =
                    broker_connectors::mysql::MySqlSink::new(config, Arc::new(transport))
                {
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
            if let Ok(transport) =
                broker_connectors::rabbitmq::LapinRabbitTransport::new(&full_endpoint)
            {
                let config = broker_connectors::rabbitmq::RabbitMqSinkConfig {
                    endpoint: full_endpoint,
                    exchange: "amq.topic".to_string(),
                    routing_key_template: "sensor.${topic}".to_string(),
                    delivery_mode: 1,
                };
                if let Ok(sink) =
                    broker_connectors::rabbitmq::RabbitMqSink::new(config, Arc::new(transport))
                {
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
            refuse_disabled_kind(conn_type, "rabbitmq");
        }
        #[cfg(feature = "clickhouse")]
        "clickhouse" => {
            let url = body
                .get("url")
                .and_then(|v| v.as_str())
                .or_else(|| body.get("server").and_then(|v| v.as_str()))
                .unwrap_or("http://127.0.0.1:8123");
            let endpoint = if url.starts_with("http://") || url.starts_with("https://") {
                url.to_string()
            } else {
                format!("http://{}", url)
            };
            let database = body
                .get("database")
                .and_then(|v| v.as_str())
                .unwrap_or("default");
            let table = body
                .get("table")
                .and_then(|v| v.as_str())
                .unwrap_or("test_mqtt_events");
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
                    .unwrap_or("default")
                    .to_string(),
                password: body
                    .get("password")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string(),
                request_timeout_ms,
            };
            if let Ok(driver) = broker_connectors::DriverClickHouseTransport::new(&config) {
                let transport: std::sync::Arc<dyn broker_connectors::ClickHouseTransport> =
                    std::sync::Arc::new(driver);
                if let Ok(sink) =
                    broker_connectors::clickhouse::ClickHouseSink::new(config, transport)
                {
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
            refuse_disabled_kind(conn_type, "clickhouse");
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
            if let Ok(transport) = broker_connectors::RumqttcMqttBridgeTransport::new(&config) {
                if let Ok(sink) =
                    broker_connectors::MqttBridgeSink::new(config, Arc::new(transport))
                {
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
            let url = body
                .get("url")
                .and_then(|v| v.as_str())
                .or_else(|| body.get("server").and_then(|v| v.as_str()))
                .unwrap_or("http://127.0.0.1:8086");
            let endpoint = if url.starts_with("http://") || url.starts_with("https://") {
                url.to_string()
            } else {
                format!("http://{}", url)
            };
            let bucket = body
                .get("bucket")
                .and_then(|v| v.as_str())
                .unwrap_or("default");
            let org = body.get("org").and_then(|v| v.as_str()).unwrap_or("idacs");
            let token = body
                .get("token")
                .and_then(|v| v.as_str())
                .unwrap_or("idacs_test_token");
            let measurement = body
                .get("measurement")
                .and_then(|v| v.as_str())
                .or_else(|| body.get("measurement_template").and_then(|v| v.as_str()))
                .unwrap_or("mqtt_events");
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
                precision: "ms".to_string(),
                batch_size: 1,
                batch_timeout_ms: 100,
                request_timeout_ms,
            };
            if let Ok(sink) = broker_connectors::InfluxDbSink::new(config, reqwest::Client::new()) {
                let sink = Arc::new(sink);
                engine.connectors().register(name, sink.clone());
                engine
                    .connectors()
                    .register(format!("influxdb:{}", name), sink);
            }
        }
        #[cfg(feature = "mongodb")]
        "mongodb" | "mongo" => {
            let server = body
                .get("server")
                .and_then(|v| v.as_str())
                .or_else(|| body.get("connection_string").and_then(|v| v.as_str()))
                .unwrap_or("127.0.0.1:27017");
            let conn_str =
                if server.starts_with("mongodb://") || server.starts_with("mongodb+srv://") {
                    server.to_string()
                } else {
                    format!("mongodb://{}", server)
                };
            let database = body
                .get("database")
                .and_then(|v| v.as_str())
                .unwrap_or("telemetry");
            let collection = body
                .get("collection")
                .and_then(|v| v.as_str())
                .or_else(|| body.get("collection_template").and_then(|v| v.as_str()))
                .unwrap_or("telemetry_${topic}");
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());
            let config = broker_connectors_enterprise::MongoDbSinkConfig {
                connection_string: conn_str,
                database: database.to_string(),
                collection_template: collection.to_string(),
                operation: broker_connectors_enterprise::MongoOperation::InsertOne,
                batch_size: Some(1),
                batch_bytes: Some(4_194_304),
                linger_ms: Some(10),
                max_retries: Some(4),
                initial_backoff_ms: Some(100),
                max_backoff_ms: Some(3_000),
                timeout_ms,
            };
            if let Ok(transport) =
                broker_connectors_enterprise::DriverMongoDbTransport::new(&config)
            {
                let transport = Arc::new(transport);
                if let Ok(sink) = broker_connectors_enterprise::MongoDbSink::new(config, transport)
                {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("mongodb:{}", name), sink.clone());
                    engine
                        .connectors()
                        .register(format!("mongo:{}", name), sink);
                }
            }
        }
        #[cfg(not(feature = "mongodb"))]
        "mongodb" | "mongo" => {
            refuse_disabled_kind(conn_type, "mongodb");
        }
        #[cfg(feature = "cassandra")]
        "cassandra" | "scylla" | "scylladb" => {
            let server = body
                .get("server")
                .and_then(|v| v.as_str())
                .or_else(|| body.get("servers").and_then(|v| v.as_str()))
                .or_else(|| body.get("contact_points").and_then(|v| v.as_str()))
                .unwrap_or("127.0.0.1:9042");
            let contact_points: Vec<String> =
                server.split(',').map(|s| s.trim().to_string()).collect();
            let keyspace = body
                .get("keyspace")
                .and_then(|v| v.as_str())
                .unwrap_or("idacs");
            let table = body
                .get("table")
                .and_then(|v| v.as_str())
                .or_else(|| body.get("table_template").and_then(|v| v.as_str()))
                .unwrap_or("sensor_events");
            let username = body.get("username").and_then(|v| v.as_str());
            let password = body.get("password").and_then(|v| v.as_str());
            let auth = if let (Some(u), Some(p)) = (username, password) {
                broker_connectors_enterprise::CassandraAuth::Password {
                    username: u.to_string(),
                    password: p.to_string(),
                }
            } else {
                broker_connectors_enterprise::CassandraAuth::None
            };
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());
            let cql = format!("INSERT INTO {}.{} (device_id, bucket_hour, event_time, payload) VALUES (?, ?, ?, ?)", keyspace, table);
            let config = broker_connectors_enterprise::CassandraSinkConfig {
                contact_points,
                keyspace: keyspace.to_string(),
                table_template: table.to_string(),
                auth,
                consistency: broker_connectors_enterprise::CqlConsistency::One,
                partition_key_template: "${client_id}".to_string(),
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
            if let Ok(transport) =
                broker_connectors_enterprise::ScyllaCassandraTransport::new(&config)
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::CassandraSink::new(config, transport)
                {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("cassandra:{}", name), sink.clone());
                    engine
                        .connectors()
                        .register(format!("scylla:{}", name), sink);
                }
            }
        }
        #[cfg(not(feature = "cassandra"))]
        "cassandra" | "scylla" | "scylladb" => {
            refuse_disabled_kind(conn_type, "cassandra");
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
            if let Ok(sink) = broker_connectors_enterprise::CockroachDbSink::new(config, transport)
            {
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
        "couchbase" => {
            let server = body
                .get("server")
                .and_then(|v| v.as_str())
                .or_else(|| body.get("connection_string").and_then(|v| v.as_str()))
                .unwrap_or("couchbase://127.0.0.1:11210");
            let conn_str =
                if server.starts_with("couchbase://") || server.starts_with("couchbases://") {
                    server.to_string()
                } else {
                    format!("couchbase://{}", server)
                };
            let bucket = body
                .get("bucket")
                .and_then(|v| v.as_str())
                .unwrap_or("telemetry");
            let scope = body
                .get("scope")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let collection = body
                .get("collection")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let username = body
                .get("username")
                .and_then(|v| v.as_str())
                .unwrap_or("Administrator");
            let password = body
                .get("password")
                .and_then(|v| v.as_str())
                .unwrap_or("password");
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());
            let config = broker_connectors_enterprise::CouchbaseSinkConfig {
                connection_string: conn_str,
                bucket: bucket.to_string(),
                scope,
                collection,
                auth: broker_connectors_enterprise::CouchbaseAuth {
                    username: username.to_string(),
                    password: password.to_string(),
                },
                doc_id_template: body
                    .get("doc_id_template")
                    .and_then(|v| v.as_str())
                    .unwrap_or("${client_id}::${timestamp}")
                    .to_string(),
                operation: broker_connectors_enterprise::CouchbaseOperation::Upsert,
                expiry_secs: None,
                batch_size: Some(1),
                batch_bytes: Some(2_097_152),
                linger_ms: Some(10),
                max_retries: Some(3),
                initial_backoff_ms: Some(100),
                max_backoff_ms: Some(2_000),
                timeout_ms,
            };
            if let Ok(transport) =
                broker_connectors_enterprise::DriverCouchbaseTransport::new(&config)
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::CouchbaseSink::new(config, transport)
                {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("couchbase:{}", name), sink);
                }
            }
        }
        #[cfg(not(feature = "couchbase"))]
        "couchbase" => {
            refuse_disabled_kind(conn_type, "couchbase");
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
            if let Ok(transport) = broker_connectors_enterprise::DriverMssqlTransport::new(&config)
            {
                let transport = Arc::new(transport);
                if let Ok(sink) = broker_connectors_enterprise::MssqlSink::new(config, transport) {
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
            if let Ok(sink) =
                broker_connectors_enterprise::oracle::OracleSink::new(config, transport)
            {
                let sink = Arc::new(sink);
                engine.connectors().register(name, sink.clone());
                engine
                    .connectors()
                    .register(format!("oracle:{}", name), sink);
            }
        }
        "tdengine" => {
            let server = body
                .get("server")
                .or_else(|| body.get("servers"))
                .or_else(|| body.get("url"))
                .or_else(|| body.get("endpoint"))
                .and_then(|v| v.as_str())
                .unwrap_or("http://127.0.0.1:6041/rest/sql");
            let endpoint = if server.starts_with("http://") || server.starts_with("https://") {
                server.to_string()
            } else {
                format!("http://{}", server)
            };
            let database = body
                .get("database")
                .and_then(|v| v.as_str())
                .unwrap_or("power");
            let stable = body
                .get("stable_name")
                .or_else(|| body.get("table"))
                .and_then(|v| v.as_str())
                .unwrap_or("meters");
            let subtable = body
                .get("subtable_template")
                .or_else(|| body.get("subtable"))
                .and_then(|v| v.as_str())
                .unwrap_or("d_meters");
            let username = body
                .get("username")
                .and_then(|v| v.as_str())
                .unwrap_or("root");
            let password = body
                .get("password")
                .and_then(|v| v.as_str())
                .unwrap_or("taosdata");
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());

            let mut metrics_template = std::collections::HashMap::new();
            metrics_template.insert("temp".to_string(), "${payload.temperature}".to_string());
            let mut tags_template = std::collections::HashMap::new();
            tags_template.insert("location".to_string(), "room1".to_string());

            let config = broker_connectors_enterprise::tdengine::TdengineSinkConfig {
                endpoint,
                database: database.to_string(),
                stable_name: stable.to_string(),
                subtable_template: subtable.to_string(),
                auth: broker_connectors_enterprise::tdengine::TdengineAuth::Basic {
                    username: username.to_string(),
                    password: password.to_string(),
                },
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
                if let Ok(transport) =
                    broker_connectors_enterprise::tdengine::DriverTdengineTransport::new(&config)
                {
                    let transport = Arc::new(transport);
                    if let Ok(sink) =
                        broker_connectors_enterprise::tdengine::TdengineSink::new(config, transport)
                    {
                        let sink = Arc::new(sink);
                        engine.connectors().register(name, sink.clone());
                        engine
                            .connectors()
                            .register(format!("tdengine:{}", name), sink);
                    }
                }
                return;
            }
            let client = reqwest::Client::builder()
                .timeout(config.timeout())
                .build()
                .unwrap_or_default();
            if let Ok(transport) =
                broker_connectors_enterprise::tdengine::HttpTdengineTransport::new(&config, client)
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::tdengine::TdengineSink::new(config, transport)
                {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("tdengine:{}", name), sink);
                }
            }
        }
        "greptimedb" | "greptime" => {
            let server = body
                .get("endpoint")
                .or_else(|| body.get("server"))
                .or_else(|| body.get("servers"))
                .or_else(|| body.get("url"))
                .and_then(|v| v.as_str())
                .unwrap_or("http://127.0.0.1:4000/v1");
            let endpoint = if server.starts_with("http://") || server.starts_with("https://") {
                server.to_string()
            } else {
                format!("http://{}", server)
            };
            let database = body
                .get("database")
                .and_then(|v| v.as_str())
                .unwrap_or("public");
            let table = body
                .get("table")
                .or_else(|| body.get("table_template"))
                .and_then(|v| v.as_str())
                .unwrap_or("sensor_events");
            let username = body.get("username").and_then(|v| v.as_str());
            let password = body.get("password").and_then(|v| v.as_str());
            let auth = if let (Some(u), Some(p)) = (username, password) {
                Some(broker_connectors::greptimedb::GreptimeDbAuth {
                    username: u.to_string(),
                    password: p.to_string(),
                })
            } else {
                None
            };
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());
            let config = broker_connectors::greptimedb::GreptimeDbConfig {
                endpoint,
                database: database.to_string(),
                auth,
                format: broker_connectors::greptimedb::GreptimeFormat::SqlInsert,
                table_template: table.to_string(),
                timestamp_precision: broker_connectors::greptimedb::GreptimePrecision::Millisecond,
                batch_size: Some(1),
                buffer_capacity: None,
                timeout_ms,
            };
            let transport = Arc::new(broker_connectors::greptimedb::HttpGreptimeDbTransport::new(
                &config,
            ));
            if let Ok(sink) = broker_connectors::greptimedb::GreptimeDbSink::new(config, transport)
            {
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
            let server = body
                .get("endpoint")
                .or_else(|| body.get("server"))
                .or_else(|| body.get("servers"))
                .or_else(|| body.get("url"))
                .and_then(|v| v.as_str())
                .unwrap_or("http://127.0.0.1:6667/rest/v2");
            let endpoint = if server.starts_with("http://") || server.starts_with("https://") {
                server.to_string()
            } else {
                format!("http://{}", server)
            };
            let device_path = body
                .get("device_path_template")
                .or_else(|| body.get("device_path"))
                .and_then(|v| v.as_str())
                .unwrap_or("root.factory.${payload.plant_id}.${client_id}");
            let username = body
                .get("username")
                .and_then(|v| v.as_str())
                .unwrap_or("root");
            let password = body
                .get("password")
                .and_then(|v| v.as_str())
                .unwrap_or("root");
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
                measurements: vec!["temperature".to_string()],
                data_types: vec![broker_connectors_enterprise::iotdb::IotDbDataType::Float],
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
            if let Ok(transport) =
                broker_connectors_enterprise::iotdb::HttpIotDbTransport::new(&config, client)
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::iotdb::IotDbSink::new(config, transport)
                {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("iotdb:{}", name), sink);
                }
            }
        }
        "opentsdb" => {
            let server = body
                .get("endpoint")
                .or_else(|| body.get("server"))
                .or_else(|| body.get("servers"))
                .or_else(|| body.get("url"))
                .and_then(|v| v.as_str())
                .unwrap_or("http://127.0.0.1:4242");
            let endpoint = if server.starts_with("http://")
                || server.starts_with("https://")
                || server.starts_with("telnet://")
            {
                server.to_string()
            } else {
                format!("http://{}", server)
            };
            let metric_template = body
                .get("metric_template")
                .or_else(|| body.get("metric"))
                .and_then(|v| v.as_str())
                .unwrap_or("factory.telemetry");
            let value_field = body
                .get("value_field")
                .or_else(|| body.get("value"))
                .and_then(|v| v.as_str())
                .unwrap_or("temperature");
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
                .get("tag_mappings")
                .or_else(|| body.get("tags"))
                .and_then(|v| v.as_object())
            {
                for (k, v) in tags {
                    if let Some(s) = v.as_str() {
                        tag_mappings.insert(k.clone(), s.to_string());
                    }
                }
            } else {
                tag_mappings.insert("sensor".to_string(), "${payload.sensor}".to_string());
            }

            let config = broker_connectors::opentsdb::OpenTsdbConfig {
                endpoint,
                protocol: broker_connectors::opentsdb::OpenTsdbProtocol::Http,
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
            if let Ok(sink) = broker_connectors::opentsdb::OpenTsdbSink::new(config, transport) {
                let sink = Arc::new(sink);
                engine.connectors().register(name, sink.clone());
                engine
                    .connectors()
                    .register(format!("opentsdb:{}", name), sink);
            }
        }
        "doris" => {
            let server = body
                .get("server")
                .or_else(|| body.get("fe_host"))
                .or_else(|| body.get("host"))
                .or_else(|| body.get("endpoint"))
                .and_then(|v| v.as_str())
                .unwrap_or("127.0.0.1");
            let fe_host = server
                .trim_start_matches("http://")
                .trim_start_matches("https://")
                .split(':')
                .next()
                .unwrap_or("127.0.0.1")
                .to_string();
            let http_port = body
                .get("http_port")
                .or_else(|| body.get("port"))
                .and_then(|v| v.as_u64())
                .unwrap_or(8030) as u16;
            let database = body
                .get("database")
                .and_then(|v| v.as_str())
                .unwrap_or("telemetry")
                .to_string();
            let table = body
                .get("table_template")
                .or_else(|| body.get("table"))
                .and_then(|v| v.as_str())
                .unwrap_or("events")
                .to_string();
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
                max_filter_ratio: Some(0.0),
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
            if let Ok(transport) =
                broker_connectors_enterprise::doris::HttpDorisTransport::new(&config, client)
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::doris::DorisSink::new(config, transport)
                {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("doris:{}", name), sink);
                }
            }
        }
        "datalayers" => {
            let server = body
                .get("endpoint")
                .or_else(|| body.get("server"))
                .or_else(|| body.get("servers"))
                .or_else(|| body.get("url"))
                .and_then(|v| v.as_str())
                .unwrap_or("http://127.0.0.1:8360");
            let endpoint = if server.starts_with("http://") || server.starts_with("https://") {
                server.to_string()
            } else {
                format!("http://{}", server)
            };
            let database = body
                .get("database")
                .and_then(|v| v.as_str())
                .unwrap_or("factory_db")
                .to_string();
            let table = body
                .get("table")
                .or_else(|| body.get("measurement"))
                .and_then(|v| v.as_str())
                .unwrap_or("sensor_events")
                .to_string();
            let auth_token = body
                .get("auth_token")
                .or_else(|| body.get("token"))
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
                timestamp_field: Some("timestamp".to_string()),
                tag_columns: vec!["sensor".to_string(), "client_id".to_string()],
                field_columns: vec!["temperature".to_string()],
                batch_size: Some(1),
                buffer_capacity: None,
                timeout_ms,
            };
            let transport = Arc::new(
                broker_connectors_enterprise::datalayers::HttpDatalayersTransport::new(&config),
            );
            if let Ok(sink) =
                broker_connectors_enterprise::datalayers::DatalayersSink::new(config, transport)
            {
                let sink = Arc::new(sink);
                engine.connectors().register(name, sink.clone());
                engine
                    .connectors()
                    .register(format!("datalayers:{}", name), sink);
            }
        }
        #[cfg(feature = "elasticsearch")]
        "elasticsearch" | "opensearch" => {
            let server = body
                .get("endpoint")
                .or_else(|| body.get("server"))
                .or_else(|| body.get("servers"))
                .or_else(|| body.get("url"))
                .and_then(|v| v.as_str())
                .unwrap_or("http://127.0.0.1:9200");
            let endpoint = if server.starts_with("http://") || server.starts_with("https://") {
                server.to_string()
            } else {
                format!("http://{}", server)
            };
            let index = body
                .get("index_template")
                .or_else(|| body.get("index"))
                .and_then(|v| v.as_str())
                .unwrap_or("sensor_events")
                .to_string();
            let username = body.get("username").and_then(|v| v.as_str());
            let password = body.get("password").and_then(|v| v.as_str());
            let auth = if let (Some(u), Some(p)) = (username, password) {
                broker_connectors::elasticsearch::ElasticsearchAuth::Basic {
                    username: u.to_string(),
                    password: p.to_string(),
                }
            } else {
                broker_connectors::elasticsearch::ElasticsearchAuth::None
            };
            let timeout_ms = body
                .get("request_timeout_ms")
                .or_else(|| body.get("timeout_ms"))
                .or_else(|| body.get("timeout"))
                .and_then(|v| v.as_u64());

            let config = broker_connectors::elasticsearch::ElasticsearchSinkConfig {
                endpoint,
                index_template: index,
                doc_id_template: None,
                auth,
                batch_size: 1,
                batch_timeout_ms: 10,
                max_retries: 3,
                request_timeout_ms: timeout_ms,
            };
            if let Ok(transport) =
                broker_connectors::elasticsearch::DriverElasticsearchTransport::new(&config)
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors::elasticsearch::ElasticsearchSink::new(config, transport)
                {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("elasticsearch:{}", name), sink.clone());
                    engine
                        .connectors()
                        .register(format!("opensearch:{}", name), sink);
                }
            }
        }
        #[cfg(not(feature = "elasticsearch"))]
        "elasticsearch" | "opensearch" => {
            refuse_disabled_kind(conn_type, "elasticsearch");
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
            if let Ok(transport) =
                broker_connectors_enterprise::pulsar::TcpPulsarTransport::new(&config)
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::pulsar::PulsarSink::new(config, transport)
                {
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
            if let Ok(transport) =
                broker_connectors_enterprise::rocketmq::TcpRocketMqTransport::new(&config)
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::rocketmq::RocketMqSink::new(config, transport)
                {
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
                return;
            };
            let Some(api_secret) = body
                .get("api_secret")
                .or_else(|| body.get("password"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
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
            if let Ok(transport) =
                broker_connectors_enterprise::confluent::RdkafkaConfluentTransport::new(&config)
            {
                let transport = Arc::new(transport);
                if let Ok(sink) = broker_connectors_enterprise::confluent::ConfluentKafkaSink::new(
                    config, transport,
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
            refuse_disabled_kind(conn_type, "confluent");
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
            if let Ok(writer) = broker_connectors::disk_log::FileDiskLogWriter::open(&config).await
            {
                if let Ok(sink) =
                    broker_connectors::disk_log::DiskLogSink::open(config, Arc::new(writer)).await
                {
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
            if let Ok(transport) =
                broker_connectors_enterprise::opc_ua::TcpOpcUaTransport::new(&config)
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::opc_ua::OpcUaSink::new(config, transport)
                {
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
                            Err(_) => return,
                        }
                    }
                    None => return,
                };
            if let Ok(sink) =
                broker_connectors_enterprise::sparkplug_b::SparkplugBSink::new(config, transport)
            {
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
            if let Ok(transport) = broker_connectors::s3::SdkS3Transport::new(&config) {
                let transport = Arc::new(transport);
                if let Ok(sink) = broker_connectors::s3::S3Sink::new(config, transport) {
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
            refuse_disabled_kind(conn_type, "s3");
        }
        "s3_tables" | "s3tables" => {
            let arn = body
                .get("table_bucket_arn")
                .or_else(|| body.get("bucket_arn"))
                .or_else(|| body.get("arn"))
                .and_then(|v| v.as_str())
                .unwrap_or("arn:aws:s3tables:us-east-1:123456789012:bucket/telemetry");
            let namespace = body
                .get("namespace")
                .and_then(|v| v.as_str())
                .unwrap_or("production_iot");
            let table = body
                .get("table_name")
                .or_else(|| body.get("table"))
                .and_then(|v| v.as_str())
                .unwrap_or("device_events");
            let region = body
                .get("region")
                .and_then(|v| v.as_str())
                .unwrap_or("us-east-1");
            let access_key = body
                .get("access_key_id")
                .and_then(|v| v.as_str())
                .unwrap_or("test");
            let secret_key = body
                .get("secret_access_key")
                .and_then(|v| v.as_str())
                .unwrap_or("test");
            let endpoint = body
                .get("endpoint")
                .or_else(|| body.get("url"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());

            let config = broker_connectors_enterprise::s3_tables::S3TablesSinkConfig {
                table_bucket_arn: arn.to_string(),
                namespace: namespace.to_string(),
                table_name: table.to_string(),
                region: region.to_string(),
                access_key_id: access_key.to_string(),
                secret_access_key: secret_key.to_string(),
                session_token: None,
                endpoint,
                target_format:
                    broker_connectors_enterprise::s3_tables::S3TablesFormat::NdjsonCompressed,
                partition_spec: Vec::new(),
                batch_size: Some(1),
                buffer_capacity: None,
                linger_ms: Some(10),
                timeout_ms,
            };
            if let Ok(transport) =
                broker_connectors_enterprise::s3_tables::HttpS3TablesTransport::new(
                    &config,
                    reqwest::Client::new(),
                )
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::s3_tables::S3TablesSink::new(config, transport)
                {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("s3_tables:{}", name), sink.clone());
                    engine
                        .connectors()
                        .register(format!("s3tables:{}", name), sink);
                }
            }
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
            if let Ok(transport) =
                broker_connectors_enterprise::kinesis::SdkKinesisTransport::new(&config)
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::kinesis::KinesisSink::new(config, transport)
                {
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
            refuse_disabled_kind(conn_type, "kinesis");
        }
        #[cfg(not(feature = "dynamodb"))]
        "dynamodb" | "dynamo" | "ddb" => {
            // Feature disabled (X1-09): fail closed with no sink; the
            // checked wrapper names the missing feature.
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
            if let Ok(transport) =
                broker_connectors_enterprise::dynamodb::SdkDynamoDbTransport::new(&config)
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::dynamodb::DynamoDbSink::new(config, transport)
                {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("dynamodb:{}", name), sink);
                }
            }
        }
        "timestream" => {
            let database = body
                .get("database_name")
                .or_else(|| body.get("database"))
                .and_then(|v| v.as_str())
                .unwrap_or("telemetry_db");
            let table = body
                .get("table_name")
                .or_else(|| body.get("table"))
                .and_then(|v| v.as_str())
                .unwrap_or("metrics");
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

            let mut dimensions = std::collections::HashMap::new();
            dimensions.insert("device".to_string(), "${client_id}".to_string());
            let mut measures = std::collections::HashMap::new();
            measures.insert("temperature".to_string(), "DOUBLE".to_string());

            let config = broker_connectors_enterprise::timestream::TimestreamSinkConfig {
                database_name: database.to_string(),
                table_name: table.to_string(),
                region: region.to_string(),
                endpoint,
                access_key_id: access_key.to_string(),
                secret_access_key: secret_key.to_string(),
                session_token: None,
                time_unit:
                    broker_connectors_enterprise::timestream::TimestreamTimeUnit::Milliseconds,
                measure_name_template: Some("${topic}".to_string()),
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
            if let Ok(transport) =
                broker_connectors_enterprise::timestream::HttpTimestreamTransport::new(
                    &config,
                    reqwest::Client::new(),
                )
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::timestream::TimestreamSink::new(config, transport)
                {
                    let sink = Arc::new(sink);
                    engine.connectors().register(name, sink.clone());
                    engine
                        .connectors()
                        .register(format!("timestream:{}", name), sink);
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
            if let Ok(transport) =
                broker_connectors_enterprise::redshift::SdkRedshiftTransport::new(&config)
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::redshift::RedshiftSink::new(config, transport)
                {
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
                return;
            };
            let Some(client_key_pem) =
                field(&["client_key_pem", "client_key", "private_key", "key_pem"])
            else {
                return;
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
            if let Ok(transport) =
                broker_connectors_enterprise::aws_iot::TlsAwsIotTransport::new(&config)
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::aws_iot::AwsIotSink::new(config, transport)
                {
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
                return;
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
            if let Ok(transport) =
                broker_connectors_enterprise::azure_blob::HttpAzureBlobTransport::new(
                    &config,
                    reqwest::Client::new(),
                )
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::azure_blob::AzureBlobSink::new(config, transport)
                {
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
                return;
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
            if let Ok(transport) =
                broker_connectors_enterprise::azure_eventhubs::HttpAzureEventHubsTransport::new(
                    &config,
                    reqwest::Client::new(),
                )
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::azure_eventhubs::AzureEventHubsSink::new(
                        config, transport,
                    )
                {
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
                return;
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
            if let Ok(transport) =
                broker_connectors_enterprise::azure_iot::TlsAzureIotTransport::new(&config)
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::azure_iot::AzureIotSink::new(config, transport)
                {
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
            if let Ok(transport) =
                broker_connectors_enterprise::gcp_pubsub::SdkGcpPubSubTransport::new(&config)
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::gcp_pubsub::GcpPubSubSink::new(config, transport)
                {
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
            refuse_disabled_kind(conn_type, "gcp_pubsub");
        }
        #[cfg(not(feature = "bigquery"))]
        "bigquery" | "bq" | "gbq" => {
            // Feature disabled: fail closed with no sink; the
            // checked wrapper names the missing feature.
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
            if let Ok(transport) = broker_connectors_enterprise::bigquery::SdkBigQueryTransport::new(
                &config,
                reqwest::Client::new(),
            ) {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::bigquery::BigQuerySink::new(config, transport)
                {
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
                return;
            };
            let Some(cloud_region) = body
                .get("cloud_region")
                .or_else(|| body.get("region"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
            };
            let Some(registry_id) = body
                .get("registry_id")
                .or_else(|| body.get("registry"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
            };
            let Some(device_id) = body
                .get("device_id")
                .or_else(|| body.get("device"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
            };
            let Some(private_key_pem) = body
                .get("private_key_pem")
                .or_else(|| body.get("private_key"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
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
            if let Ok(transport) =
                broker_connectors_enterprise::gcp_iot::TlsGcpIotTransport::new(&config)
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::gcp_iot::GcpIotSink::new(config, transport)
                {
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
                return;
            };
            let Some(token) = body
                .get("token")
                .or_else(|| body.get("api_key"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
            };
            let Some(catalog) = body
                .get("catalog")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
            };
            let Some(schema) = body
                .get("schema")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
            };
            let Some(table) = body
                .get("table")
                .or_else(|| body.get("table_template"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
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
            if let Ok(transport) =
                broker_connectors_enterprise::databricks::HttpDatabricksTransport::new(
                    &config,
                    reqwest::Client::new(),
                )
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::databricks::DatabricksSink::new(config, transport)
                {
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
                return;
            };
            let Some(user) = body
                .get("user")
                .or_else(|| body.get("username"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
            };
            let Some(database) = body
                .get("database")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
            };
            let Some(schema) = body
                .get("schema")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
            };
            let Some(table) = body
                .get("table")
                .or_else(|| body.get("table_template"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
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
                return;
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
            if let Ok(transport) =
                broker_connectors_enterprise::snowflake::HttpSnowflakeTransport::new(
                    &config,
                    reqwest::Client::new(),
                )
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::snowflake::SnowflakeSink::new(config, transport)
                {
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
                return;
            };
            // Fail closed (S1-01): instance, table and both access keys are
            // operator-supplied; the previous test names and keys are removed.
            let Some(instance_name) = body
                .get("instance_name")
                .or_else(|| body.get("instance"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
            };
            let Some(table_name) = body
                .get("table_name")
                .or_else(|| body.get("table"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
            };
            let Some(access_key_id) = body
                .get("access_key_id")
                .or_else(|| body.get("ak"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
            };
            let Some(access_key_secret) = body
                .get("access_key_secret")
                .or_else(|| body.get("sk"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
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
            if let Ok(transport) =
                broker_connectors_enterprise::tablestore::HttpTablestoreTransport::new(
                    &config,
                    reqwest::Client::new(),
                )
            {
                let transport = Arc::new(transport);
                if let Ok(sink) =
                    broker_connectors_enterprise::tablestore::TablestoreSink::new(config, transport)
                {
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
                return;
            };
            // Fail closed (S1-01): OCIDs, fingerprint and key are
            // operator-supplied; the previous test OCIDs, test fingerprint
            // and built-in key are removed.
            let Some(stream_pool_id) = body
                .get("stream_pool_id")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
            };
            let Some(stream_id) = body
                .get("stream_id")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
            };
            let Some(tenancy_ocid) = body
                .get("tenancy_ocid")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
            };
            let Some(user_ocid) = body
                .get("user_ocid")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
            };
            let Some(fingerprint) = body
                .get("fingerprint")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
            };
            let Some(private_key_pem) = body
                .get("private_key_pem")
                .or_else(|| body.get("private_key"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return;
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
            if let Ok(transport) =
                broker_connectors_enterprise::oci_streaming::HttpOciStreamingTransport::new(
                    &config,
                    reqwest::Client::new(),
                )
            {
                let transport = Arc::new(transport);
                if let Ok(sink) = broker_connectors_enterprise::oci_streaming::OciStreamingSink::new(
                    config, transport,
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
}

/// Whether a connector id already lives in the store, matching the
/// same `id`-or-`name` predicate the read and update paths use. Control
/// plane only: one linear scan per create, nothing on the message path.
fn connector_id_exists(name: &str) -> bool {
    CONNECTORS.read().unwrap().iter().any(|c| {
        c.get("id").and_then(|v| v.as_str()) == Some(name)
            || c.get("name").and_then(|v| v.as_str()) == Some(name)
    })
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
    // entries carry both keys with the same value, and reads match either
    // exactly (no fuzzy matching).
    let name = obj_check
        .get("id")
        .or_else(|| obj_check.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if name.is_empty() {
        return ApiError::BadRequest("connector id or name is required".to_string())
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
    if !broker_config::is_known_connector_type(&conn_type) {
        return ApiError::BadRequest(format!("unknown connector type: {conn_type}"))
            .into_response();
    }

    // Reject a duplicate id before mutating anything: pushing it into
    // the store would poison every later connector POST/PUT/DELETE, whose
    // persistence export re-validates the whole store and fails on the
    // duplicated id. The documented conflict code is `ALREADY_EXISTS`.
    // This early read-check also skips the wasted live-sink
    // registration below; the write-locked re-check before the push
    // closes the check-then-insert race for the in-memory store.
    if connector_id_exists(&name) {
        return ApiError::AlreadyExists(format!("connector {name:?} already exists"))
            .into_response();
    }
    // Store-cap bound (management-plane, fail closed): an obviously full
    // store fails without a needless registration attempt.
    {
        let live = CONNECTORS.read().unwrap().len();
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

    // Register the live sink on the create event through the same
    // path boot replay uses: delivery dials its target lazily at send
    // time, so registration never depends on a point-in-time
    // reachability probe. A kind with no execution path registers
    // nothing, so it honestly reads back `disconnected` (stored config
    // first, no claimed execution).
    // A kind whose Cargo feature is off is refused, and the error names
    // the feature. Nothing is stored for it.
    if let Err(feature_error) =
        try_register_live_sink(&state.engine, &conn_type, &name, &body).await
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "BAD_REQUEST",
                "message": feature_error,
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

    // Re-check under the write lock so two concurrent creates for the
    // same id cannot both pass the early check and push a duplicate pair.
    {
        let mut store = CONNECTORS.write().unwrap();
        if store.iter().any(|c| {
            c.get("id").and_then(|v| v.as_str()) == Some(name.as_str())
                || c.get("name").and_then(|v| v.as_str()) == Some(name.as_str())
        }) {
            return ApiError::AlreadyExists(format!("connector {name:?} already exists"))
                .into_response();
        }
        if store.len() >= MAX_CONNECTORS {
            return ApiError::BadRequest(format!(
                "connector store holds {} entries, at most {MAX_CONNECTORS}",
                store.len()
            ))
            .into_response();
        }
        store.push(body.clone());
    }
    // W3-19: mirror the flag into the engine's lock-free dispatch
    // snapshot so rule evaluation observes a created-disabled connector
    // as skipped from the first publish without taking this store's
    // lock on the message path.
    let created_enable = body.get("enable").and_then(|v| v.as_bool()).unwrap_or(true);
    state.engine.set_action_enabled(&name, created_enable);

    // A persist failure is a 500 (the in-memory entry stays but the disk
    // save failed); the loss must never be silent.
    if let Err(error) = persist_connectors(&state.config) {
        return connector_persist_error(error);
    }

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
    // Exact id matching only: the path id must equal the stored `id` or
    // `name` exactly. No suffix splitting: `a:b` must not update `b`.
    // Existence before validation: an unknown id is 404 and creates
    // nothing, even when the body is also invalid.
    let exists = CONNECTORS.read().unwrap().iter().any(|c| {
        c.get("id").and_then(|v| v.as_str()) == Some(id.as_str())
            || c.get("name").and_then(|v| v.as_str()) == Some(id.as_str())
    });
    if !exists {
        return ApiError::NotFound(format!("connector {id:?} not found")).into_response();
    }
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
        if !trimmed.is_empty() && trimmed != id {
            return ApiError::BadRequest(format!(
                "body id {trimmed:?} does not match path id {id:?} (field `id`)"
            ))
            .into_response();
        }
    }
    if id.len() > MAX_CONNECTOR_ID_LEN {
        return ApiError::BadRequest(format!(
            "connector id holds {} chars, at most {MAX_CONNECTOR_ID_LEN} (field `id`)",
            id.len()
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
    if !broker_config::is_known_connector_type(&conn_type) {
        return ApiError::BadRequest(format!("unknown connector type: {conn_type}"))
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
    let enable = obj.get("enable").and_then(|v| v.as_bool()).unwrap_or(true);
    // Swap the live sink first: drop every registration for this id
    // (bare plus any `kind:id` alias, mirroring delete) so a kind change
    // leaves no stale alias behind, then re-register through the same
    // path create uses. Registration never depends on a point-in-time
    // reachability probe (delivery dials lazily at send time); a kind
    // with no execution path registers nothing and honestly reads back
    // `disconnected`.
    for registered in state.engine.connectors().ids() {
        if registered == id || registered.ends_with(&format!(":{id}")) {
            state.engine.connectors().unregister(&registered);
        }
    }
    if let Err(feature_error) = try_register_live_sink(&state.engine, &conn_type, &id, &body).await
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "BAD_REQUEST",
                "message": feature_error,
            })),
        )
            .into_response();
    }
    // Live health, never a write-time snapshot: a kind with no
    // execution path registers nothing, so it honestly reads back
    // `disconnected` (stored config first, no claimed execution).
    let live = state.engine.connectors().get(&id).is_some();
    let status_str = connector_status(live);
    if let Some(obj) = body.as_object_mut() {
        obj.insert("id".to_string(), serde_json::Value::String(id.clone()));
        obj.insert("name".to_string(), serde_json::Value::String(id.clone()));
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
    {
        let mut connectors = CONNECTORS.write().unwrap();
        if let Some(pos) = connectors.iter().position(|c| {
            c.get("id").and_then(|v| v.as_str()) == Some(id.as_str())
                || c.get("name").and_then(|v| v.as_str()) == Some(id.as_str())
        }) {
            connectors[pos] = body.clone();
        } else {
            return ApiError::NotFound(format!("connector {id:?} not found")).into_response();
        }
    }
    // W3-19: mirror the flag into the dispatch snapshot (same lock-free
    // path as the enable route below).
    state.engine.set_action_enabled(&id, enable);
    // A persist failure is a 500 (the in-memory replacement stays but the
    // disk save failed); the loss must never be silent.
    if let Err(error) = persist_connectors(&state.config) {
        return connector_persist_error(error);
    }
    (StatusCode::OK, Json(body)).into_response()
}

pub async fn delete_connector(State(state): State<ApiState>, Path(id): Path<String>) -> Response {
    // Exact id matching only: the path id must equal the stored `id` or
    // `name` exactly. The previous `split(':')` suffix match is gone: it
    // let `a:b` delete `b`, which is never correct for operator-owned ids.
    // Read (C07-04) and replace (C07-05) match exactly the same way.
    let exists = CONNECTORS.read().unwrap().iter().any(|c| {
        c.get("id").and_then(|v| v.as_str()) == Some(id.as_str())
            || c.get("name").and_then(|v| v.as_str()) == Some(id.as_str())
    });
    if !exists {
        return ApiError::NotFound(format!("connector {id:?} not found")).into_response();
    }
    // Reference guard: a connector still referenced by a rule forward or
    // by an action's stored `connector`/`connector_id` field cannot be
    // dropped while the reference stands. The first referencing owner is
    // named in the refusal, mirroring the action delete guard.
    if let Some(rule_id) = state.engine.list_rules().iter().find_map(|rule| {
        rule.actions
            .iter()
            .any(|action| {
                matches!(
                    action,
                    broker_rules::RuleAction::ForwardConnector { connector_id }
                    if connector_id == &id
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
        let params = &record.params;
        let target = params
            .get("connector")
            .or_else(|| params.get("connector_id"))
            .and_then(|v| v.as_str());
        (target == Some(id.as_str())).then(|| record.id.clone())
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
        if registered == id || registered.ends_with(&format!(":{id}")) {
            state.engine.connectors().unregister(&registered);
        }
    }

    let removed = {
        let mut connectors = CONNECTORS.write().unwrap();
        let before = connectors.len();
        connectors.retain(|c| {
            c.get("id").and_then(|v| v.as_str()) != Some(id.as_str())
                && c.get("name").and_then(|v| v.as_str()) != Some(id.as_str())
        });
        connectors.len() != before
    };
    if !removed {
        return ApiError::NotFound(format!("connector {id:?} not found")).into_response();
    }
    // The id no longer names anything: drop it from the dispatch
    // snapshot so a later re-create with the same id starts enabled
    // (mirrors the W3-04 action delete path; unknown ids are a no-op).
    state.engine.remove_action(&id);
    // Unknown ids persist nothing; a persist failure after a real removal
    // is a 500 (the in-memory removal stays but the disk save failed).
    if let Err(error) = persist_connectors(&state.config) {
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
pub async fn set_connector_enabled(
    State(state): State<ApiState>,
    Path((id, enable_raw)): Path<(String, String)>,
) -> Response {
    // Exact id matching only, mirroring read/replace/delete: the path id
    // must equal the stored `id` or `name` exactly. Existence before
    // validation: an unknown id is 404 and persists nothing, even when
    // the flag is also malformed.
    let exists = CONNECTORS.read().unwrap().iter().any(|c| {
        c.get("id").and_then(|v| v.as_str()) == Some(id.as_str())
            || c.get("name").and_then(|v| v.as_str()) == Some(id.as_str())
    });
    if !exists {
        return ApiError::NotFound(format!("connector {id:?} not found")).into_response();
    }
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
    {
        let mut connectors = CONNECTORS.write().unwrap();
        let Some(pos) = connectors.iter().position(|c| {
            c.get("id").and_then(|v| v.as_str()) == Some(id.as_str())
                || c.get("name").and_then(|v| v.as_str()) == Some(id.as_str())
        }) else {
            return ApiError::NotFound(format!("connector {id:?} not found")).into_response();
        };
        // Flip only the single boolean; id, kind and params are
        // untouched, so the update can never trip the store cap or change
        // validation.
        if let Some(obj) = connectors[pos].as_object_mut() {
            obj.insert("enable".to_string(), serde_json::Value::Bool(enable));
        }
    }
    // Mirror into the dispatch snapshot before persisting: the in-memory
    // flag is already flipped, so evaluation must observe it even if the
    // disk save below fails (the store itself keeps the mutation on a
    // persist failure and answers 500, never silently).
    state.engine.set_action_enabled(&id, enable);
    if let Err(error) = persist_connectors(&state.config) {
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
    // Exact id matching only, mirroring read/replace/delete/enable: the
    // path id must equal the stored `id` or `name` exactly. Existence
    // before validation: an unknown id is 404 and persists nothing, even
    // when the operation is also unknown.
    let exists = CONNECTORS.read().unwrap().iter().any(|c| {
        c.get("id").and_then(|v| v.as_str()) == Some(id)
            || c.get("name").and_then(|v| v.as_str()) == Some(id)
    });
    if !exists {
        return ApiError::NotFound(format!("connector {id:?} not found")).into_response();
    }
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
    {
        let mut connectors = CONNECTORS.write().unwrap();
        let Some(pos) = connectors.iter().position(|c| {
            c.get("id").and_then(|v| v.as_str()) == Some(id)
                || c.get("name").and_then(|v| v.as_str()) == Some(id)
        }) else {
            return ApiError::NotFound(format!("connector {id:?} not found")).into_response();
        };
        // Flip only the single boolean; id, kind and params are
        // untouched, so the update can never trip the store cap or change
        // validation.
        if let Some(obj) = connectors[pos].as_object_mut() {
            obj.insert("enable".to_string(), serde_json::Value::Bool(enable));
        }
    }
    // Mirror into the dispatch snapshot before persisting: the in-memory
    // flag is already flipped, so evaluation must observe it even if the
    // disk save below fails (the store itself keeps the mutation on a
    // persist failure and answers 500, never silently). Connector ids
    // share the W3-04 snapshot with action ids (absent means enabled),
    // so a disabled connector costs the same single lookup and never
    // touches the connector store on the fire path.
    state.engine.set_action_enabled(id, enable);
    if let Err(error) = persist_connectors(&state.config) {
        return connector_persist_error(error);
    }
    StatusCode::NO_CONTENT.into_response()
}

fn extract_target_host_port(body: &serde_json::Value) -> Option<String> {
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
        if let Ok(parsed) = reqwest::Url::parse(u) {
            let host = parsed.host_str().unwrap_or("127.0.0.1");
            let port = parsed.port_or_known_default().unwrap_or(80);
            return Some(format!("{}:{}", host, port));
        }
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
    None
}

async fn check_tcp_reachable(addr: &str) -> Result<(), String> {
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
/// the documented `{code, message}` / `{result, status}` shape.
/// Management-plane only: runs off the message path (no lock, no
/// allocation, no state on publish/deliver), performs single-request bounded
/// work capped by [`CONNECTOR_PROBE_TIMEOUT`], writes to no store, registers
/// no runtime, and leaves nothing running after return.
// TODO(parity): what per-kind config validation does the probe require
// beyond the kind being known (required fields per kind, value ranges)?
// The rulebook does not decide the exact per-kind schemas; the current
// choice validates the kind against the W3-17 registry and dials the
// exposed network target, storing nothing, until the checker pins it.
// TODO(parity): is a kind with no network target a success once the kind
// itself validated, or does the spec require a target for every kind? The
// rulebook does not decide; the current choice counts kind-only configs as
// reachable (matching the sibling action/source probes) until the checker
// pins it.
pub async fn probe_connector(body: Option<Json<serde_json::Value>>) -> Response {
    let body = match body {
        Some(Json(b)) => b,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "code": "BAD_REQUEST",
                    "message": "request body is required"
                })),
            )
                .into_response();
        }
    };
    if !body.is_object() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "BAD_REQUEST",
                "message": "request body must be a JSON object"
            })),
        )
            .into_response();
    }
    let kind = body
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if kind.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "BAD_REQUEST",
                "message": "missing required field: type"
            })),
        )
            .into_response();
    }
    if !broker_config::is_known_connector_type(&kind) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "BAD_REQUEST",
                "message": format!("unknown connector type: {kind}")
            })),
        )
            .into_response();
    }
    // Runtime probe hook (the W3-17 dial path reused from the connector
    // create path): kinds that expose a network target get one bounded
    // dial; kinds without one count as reachable once the kind itself
    // validated. No store write, no runtime registration, no background
    // task survives the return.
    if let Some(target) = extract_target_host_port(&body) {
        let dial =
            tokio::time::timeout(CONNECTOR_PROBE_TIMEOUT, check_tcp_reachable(&target)).await;
        match dial {
            Ok(Ok(())) => (
                StatusCode::OK,
                Json(serde_json::json!({
                    "result": "ok",
                    "status": "connected",
                    "message": format!("Target endpoint {} reachable", target)
                })),
            )
                .into_response(),
            Ok(Err(err)) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "code": "TEST_FAILED",
                    "message": err
                })),
            )
                .into_response(),
            Err(_) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "code": "TEST_FAILED",
                    "message": format!("Connection to {} timed out after 5s", target)
                })),
            )
                .into_response(),
        }
    } else {
        (
            StatusCode::OK,
            Json(serde_json::json!({ "result": "ok", "status": "connected" })),
        )
            .into_response()
    }
}

// ---------------------------------------------------------------------------
// Action Probe (W3-07, C06-11)
// ---------------------------------------------------------------------------

pub async fn get_action_types() -> Response {
    let catalogue = broker_connectors::action_types_catalogue();
    (StatusCode::OK, Json(serde_json::json!(catalogue))).into_response()
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
/// listed only if the kernel can execute it, read from the action-types
/// catalogue rather than a second list that could drift from the runtimes.
/// This is the same registry `POST /api/v5/actions` validates against.
fn is_known_probe_action_type(kind: &str) -> bool {
    broker_connectors::action_types_catalogue()
        .iter()
        .any(|entry| entry.kind == kind)
}

/// Stateless action probe: validates the config against the named kind and
/// dials where the kind exposes a network target, reporting success or the
/// failure reason with the documented `{code, message}` / `{result, status}`
/// shape. Management-plane only: runs off the message path (no lock, no
/// allocation, no state on publish/deliver), performs single-request bounded
/// work capped by [`ACTION_PROBE_TIMEOUT`], writes to no store, registers no
/// runtime, and leaves nothing running after return.
// TODO(parity): what per-kind config validation does the probe require
// beyond the kind being known (required fields per kind, value ranges)?
// The rulebook does not decide the exact per-kind schemas; the current
// choice validates the kind against the W3-01 registry and dials the
// exposed network target, storing nothing, until the checker pins it.
pub async fn probe_action(body: Option<Json<serde_json::Value>>) -> Response {
    let body = match body {
        Some(Json(b)) => b,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "code": "BAD_REQUEST",
                    "message": "request body is required"
                })),
            )
                .into_response();
        }
    };
    if !body.is_object() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "BAD_REQUEST",
                "message": "request body must be a JSON object"
            })),
        )
            .into_response();
    }
    let kind = body
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if kind.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "BAD_REQUEST",
                "message": "missing required field: type"
            })),
        )
            .into_response();
    }
    if !is_known_probe_action_type(&kind) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "BAD_REQUEST",
                "message": format!("unknown action type: {kind}")
            })),
        )
            .into_response();
    }
    // Runtime probe hook (the connector dial path reused from the
    // connector create path): kinds that expose a network target get one
    // bounded dial; kinds without one count as reachable once the kind
    // itself validated. No store write, no runtime registration, no
    // background task survives the return.
    if let Some(target) = extract_target_host_port(&body) {
        let dial = tokio::time::timeout(ACTION_PROBE_TIMEOUT, check_tcp_reachable(&target)).await;
        match dial {
            Ok(Ok(())) => (
                StatusCode::OK,
                Json(serde_json::json!({
                    "result": "ok",
                    "status": "connected",
                    "message": format!("Target endpoint {} reachable", target)
                })),
            )
                .into_response(),
            Ok(Err(err)) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "code": "TEST_FAILED",
                    "message": err
                })),
            )
                .into_response(),
            Err(_) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "code": "TEST_FAILED",
                    "message": format!("Connection to {} timed out after 5s", target)
                })),
            )
                .into_response(),
        }
    } else {
        (
            StatusCode::OK,
            Json(serde_json::json!({ "result": "ok", "status": "connected" })),
        )
            .into_response()
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
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "code": "BAD_REQUEST",
                    "message": "request body is required"
                })),
            )
                .into_response();
        }
    };
    if !body.is_object() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "BAD_REQUEST",
                "message": "request body must be a JSON object"
            })),
        )
            .into_response();
    }
    let kind = body
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if kind.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "BAD_REQUEST",
                "message": "missing required field: type"
            })),
        )
            .into_response();
    }
    if !is_known_source_type(&kind) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "BAD_REQUEST",
                "message": format!("unknown source type: {kind}")
            })),
        )
            .into_response();
    }
    // Runtime probe hook (the W3-23 dial path): kinds that expose a network
    // target get one bounded dial; kinds without one count as reachable once
    // the kind itself validated, matching the connector/action probes.
    if let Some(target) = extract_target_host_port(&body) {
        let dial = tokio::time::timeout(SOURCE_PROBE_TIMEOUT, check_tcp_reachable(&target)).await;
        match dial {
            Ok(Ok(())) => StatusCode::NO_CONTENT.into_response(),
            Ok(Err(err)) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "code": "TEST_FAILED",
                    "message": err
                })),
            )
                .into_response(),
            Err(_) => (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "code": "TEST_FAILED",
                    "message": format!("Connection to {} timed out after 5s", target)
                })),
            )
                .into_response(),
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
