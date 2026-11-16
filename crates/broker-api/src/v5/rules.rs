//! Streaming SQL rules, data connectors, action sinks, and schema registry for the v5 REST API.

use axum::{
    extract::{Path, Query, RawQuery, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::errors::ApiError;
use crate::node_scope::resolve_node;
use crate::pagination::{meta_page, paginate};
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
//   10_000); the default store is empty.
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

/// First accepted list page. Reason: the reference pages count from 1,
/// so 0 and a negative value are refused.
const MIN_RULE_PAGE: u32 = 1;
/// Default list page size. Reason: the reference default page holds 100
/// rows; the rule catalogue is capped well below this.
const DEFAULT_RULE_LIMIT: u32 = 100;
/// Largest accepted list page size. Reason: the reference maximum
/// prevents one request from cloning an unbounded slice.
const MAX_RULE_LIMIT: u32 = 10_000;

/// Length of a generated rule id in hex chars. Reason: the documented
/// example shows an 8-char id, so a create without one assigns 8 chars.
const GENERATED_RULE_ID_LEN: usize = 8;

/// Event Hubs Kafka port from the vendor docs. Reason: the derived
/// `bootstrap_hosts` must name the Kafka endpoint port.
const AZURE_EVENTHUBS_KAFKA_PORT: u16 = 9093;

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

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn decode_query_component(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                out.push(b' ');
                index += 1;
            }
            b'%' if index + 2 < bytes.len() => {
                match (hex_value(bytes[index + 1]), hex_value(bytes[index + 2])) {
                    (Some(high), Some(low)) => {
                        out.push(high * 16 + low);
                        index += 3;
                    }
                    _ => {
                        out.push(bytes[index]);
                        index += 1;
                    }
                }
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[derive(Default)]
struct RuleListQuery {
    ns: Option<String>,
    only_global: Option<String>,
    enable: Option<String>,
    from: Option<String>,
    like_id: Option<String>,
    like_from: Option<String>,
    like_description: Option<String>,
    match_from: Option<String>,
    action: Vec<String>,
    source: Vec<String>,
    page: Option<String>,
    limit: Option<String>,
}

fn parse_rule_list_query(raw: Option<String>) -> RuleListQuery {
    let mut query = RuleListQuery::default();
    for pair in raw.unwrap_or_default().split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_query_component(key);
        let value = decode_query_component(value);
        match key.as_str() {
            "ns" => query.ns = Some(value),
            "only_global" => query.only_global = Some(value),
            "enable" => query.enable = Some(value),
            "from" => query.from = Some(value),
            "like_id" => query.like_id = Some(value),
            "like_from" => query.like_from = Some(value),
            "like_description" => query.like_description = Some(value),
            "match_from" => query.match_from = Some(value),
            "action" => query.action.push(value),
            "source" => query.source.push(value),
            "page" => query.page = Some(value),
            "limit" => query.limit = Some(value),
            _ => {}
        }
    }
    query
}

fn source_text(source: &broker_rules::StdSource) -> String {
    match source {
        broker_rules::StdSource::Topic(value)
        | broker_rules::StdSource::Event(value)
        | broker_rules::StdSource::Stream(value) => value.clone(),
    }
}

/// The SQL `FROM` sources of a stored rule, as the reference `from` array.
/// A pre-front-end rule keeps the stored topic filter.
fn rule_from(rule: &broker_rules::Rule) -> Vec<String> {
    if rule.std_sources.is_empty() {
        vec![rule.topic_filter.as_str().to_string()]
    } else {
        rule.std_sources.iter().map(source_text).collect()
    }
}

/// Derive the stored topic filter from the SQL `FROM` clause through the
/// rule engine parser. A single source uses its filter. Several sources
/// use `#` and the engine matches each source, so every source routes.
/// An invalid or absent FROM is a 400 that names the problem.
fn topic_filter_from_sql(sql: &str) -> Result<broker_protocol::TopicFilter, Box<Response>> {
    let bad = |message: String| Box::new(ApiError::BadRequest(message).into_response());
    let sources = broker_rules::rule_sql_sources(sql).map_err(bad)?;
    if sources.is_empty() {
        return Err(bad(
            "rule SQL FROM must name a quoted topic filter or an event source (field `sql`)"
                .to_string(),
        ));
    }
    if sources
        .iter()
        .any(|source| matches!(source, broker_rules::StdSource::Stream(_)))
    {
        return Err(bad(
            "rule SQL FROM must name a quoted topic filter or an event source (field `sql`)"
                .to_string(),
        ));
    }
    let texts: Vec<String> = sources.iter().map(source_text).collect();
    if texts
        .iter()
        .any(|text| text.contains('[') || text.contains(']'))
    {
        return Err(bad(
            "invalid topic filter: bracket characters are not allowed".to_string(),
        ));
    }
    let topic = if texts.len() == 1 {
        texts[0].clone()
    } else {
        "#".to_string()
    };
    broker_protocol::TopicFilter::new(&topic)
        .map_err(|error| bad(format!("invalid topic filter: {error}")))
}

/// Projection of the stored [`broker_rules::Rule`] for `/api/v5/rules`.
/// Only real engine state: stored actions verbatim (empty stays empty),
/// stored description and metadata, no invented `created_at`, no
/// connector-id coercion. `tenant` names the owning tenant (`default`
/// for pre-tenancy rules).
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
        "description": rule.description,
        "metadata": rule.metadata,
        "tenant": rule.tenant,
        "sql": sql,
        "from": rule_from(rule),
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

fn query_bool(name: &str, value: Option<&String>) -> Result<Option<bool>, Box<Response>> {
    match value {
        None => Ok(None),
        Some(raw) if raw.eq_ignore_ascii_case("true") => Ok(Some(true)),
        Some(raw) if raw.eq_ignore_ascii_case("false") => Ok(Some(false)),
        Some(_) => Err(Box::new(
            ApiError::BadRequest(format!("query parameter `{name}` must be a boolean"))
                .into_response(),
        )),
    }
}

fn rule_has_action(rule: &broker_rules::Rule, wanted: &str) -> bool {
    rule.actions.iter().any(|action| match action {
        broker_rules::RuleAction::ForwardConnector { connector_id } => {
            connector_id == wanted || broker_config::v5_action_sink_id(connector_id) == Some(wanted)
        }
        _ => false,
    })
}

fn rule_matches_topic_query(
    sources: &[String],
    query: &str,
    query_topic: &Result<broker_protocol::Topic, broker_protocol::ProtocolError>,
) -> bool {
    sources.iter().any(
        |source| match (query_topic, broker_protocol::TopicFilter::new(source)) {
            (Ok(topic), Ok(filter)) => filter.matches(topic),
            _ => source == query,
        },
    )
}

pub async fn list_rules(State(state): State<ApiState>, RawQuery(raw): RawQuery) -> Response {
    let params = parse_rule_list_query(raw);
    let page = match params.page.as_deref() {
        None => MIN_RULE_PAGE,
        Some(raw) => match raw.parse::<u32>() {
            Ok(value) if value >= MIN_RULE_PAGE => value,
            _ => {
                return ApiError::BadRequest("page must be a positive integer".to_string())
                    .into_response();
            }
        },
    };
    let limit = match params.limit.as_deref() {
        None => DEFAULT_RULE_LIMIT,
        Some(raw) => match raw.parse::<u32>() {
            Ok(value) if (MIN_RULE_PAGE..=MAX_RULE_LIMIT).contains(&value) => value,
            _ => {
                return ApiError::BadRequest(format!(
                    "limit must be an integer from {MIN_RULE_PAGE} to {MAX_RULE_LIMIT}"
                ))
                .into_response();
            }
        },
    };
    let only_global = match query_bool("only_global", params.only_global.as_ref()) {
        Ok(value) => value.unwrap_or(false),
        Err(response) => return *response,
    };
    let enable = match query_bool("enable", params.enable.as_ref()) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let match_from_topic = params
        .match_from
        .as_deref()
        .map(broker_protocol::Topic::new);
    let rules: Vec<_> = state
        .engine
        .list_rules()
        .into_iter()
        .filter(|rule| {
            if let Some(namespace) = params.ns.as_deref() {
                if rule.tenant != namespace {
                    return false;
                }
            }
            if only_global && rule.tenant != "default" {
                return false;
            }
            if let Some(wanted) = enable {
                if rule.enabled != wanted {
                    return false;
                }
            }
            let sources = rule_from(rule);
            if let Some(wanted) = params.from.as_deref() {
                if !sources.iter().any(|source| source == wanted) {
                    return false;
                }
            }
            if let Some(wanted) = params.like_id.as_deref() {
                if !rule.id.contains(wanted) {
                    return false;
                }
            }
            if let Some(wanted) = params.like_from.as_deref() {
                if !sources.iter().any(|source| source.contains(wanted)) {
                    return false;
                }
            }
            if let Some(wanted) = params.like_description.as_deref() {
                if !rule.description.contains(wanted) {
                    return false;
                }
            }
            if let Some(wanted) = params.match_from.as_deref() {
                // Use the precomputed topic when present. An invalid topic
                // falls back to string equality in the matcher.
                if let Some(query_topic) = match_from_topic.as_ref() {
                    if !rule_matches_topic_query(&sources, wanted, query_topic) {
                        return false;
                    }
                }
            }
            if !params.action.is_empty()
                && !params
                    .action
                    .iter()
                    .any(|wanted| rule_has_action(rule, wanted))
            {
                return false;
            }
            if !params.source.is_empty()
                && !params
                    .source
                    .iter()
                    .any(|wanted| sources.iter().any(|source| source == wanted))
            {
                return false;
            }
            true
        })
        .collect();

    let count = rules.len();
    let page_rows: Vec<_> = paginate(&rules, page, limit)
        .iter()
        .map(rule_to_v5)
        .collect();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "data": page_rows,
            "meta": meta_page(page, limit, count),
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
            // one sink, one key). A connector-only row owns no such
            // sink; its own id names the connector sink.
            let connector_id = rule_action_target(state, &found.id);
            actions.push(broker_rules::RuleAction::ForwardConnector { connector_id });
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
        // string form above. A connector-only row has no action sink,
        // so its own id reaches the connector sink.
        let connector_id = rule_action_target(state, &found.id);
        actions.push(broker_rules::RuleAction::ForwardConnector { connector_id });
    }
    Ok(actions)
}

/// Dispatch target of a rule action: the action's own key, also when
/// the action has no sink yet, because its sink registers under that key.
fn rule_action_target(_state: &ApiState, action_id: &str) -> String {
    broker_config::v5_action_sink_key(action_id)
}

/// `POST /api/v5/rules`: compile the rule before storing. A malformed
/// body is rejected with the documented error shape. A duplicate id is
/// rejected cleanly and never overwritten.
///
/// The documented request needs only `sql`. An explicit `id` is stored
/// verbatim so duplicates are detectable. An absent `id` falls back to
/// `name`, then to an 8-char hex id that the response carries so
/// `GET /rules/{id}` finds the rule. An absent `name` is the documented
/// empty string.
///
/// The `FROM` clause is read with the rule SQL parser. An invalid or
/// absent `FROM` is a 400 that names the problem. A `tenant` field
/// selects the owning tenant; absent means the default tenant. An
/// unknown tenant is rejected with 400.
///
/// The `actions` list follows [`parse_rule_actions`]. A
/// `ForwardConnector` target naming no known action is rejected (the
/// W3-02 action linkage). The handler stores `description` and
/// `metadata` and returns them on every read.
///
/// Management-plane only; publish and deliver never touch this map
/// (dispatch reads a snapshot).
pub async fn create_rule(
    State(state): State<ApiState>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(obj) = body.as_object() else {
        return ApiError::BadRequest("request body must be a JSON object".to_string())
            .into_response();
    };
    // A present `id` must be a non-empty string. An empty id fails
    // with 400. An absent id falls back to `name`, then to a generated
    // id.
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
    // The reference schema gives `name` an empty default.
    let name = match obj.get("name") {
        None => String::new(),
        Some(value) => match value.as_str() {
            Some(name) => name.trim().to_string(),
            None => {
                return ApiError::BadRequest("field `name` must be a string".to_string())
                    .into_response();
            }
        },
    };
    let description = match obj.get("description") {
        None => String::new(),
        Some(value) => match value.as_str() {
            Some(text) => text.to_string(),
            None => {
                return ApiError::BadRequest("field `description` must be a string".to_string())
                    .into_response();
            }
        },
    };
    let metadata = match obj.get("metadata") {
        None => serde_json::json!({}),
        Some(value) if value.is_object() => value.clone(),
        Some(_) => {
            return ApiError::BadRequest("field `metadata` must be an object".to_string())
                .into_response();
        }
    };
    let Some(sql) = obj
        .get("sql")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
    else {
        return ApiError::BadRequest("missing required field: sql".to_string()).into_response();
    };
    let enable = match obj.get("enable") {
        None => true,
        Some(value) => match value.as_bool() {
            Some(enable) => enable,
            None => {
                return ApiError::BadRequest("field `enable` must be a boolean".to_string())
                    .into_response();
            }
        },
    };
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
    let topic_filter = match topic_filter_from_sql(&sql) {
        Ok(filter) => filter,
        Err(response) => return *response,
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
        description,
        metadata,
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
/// The reference PUT merges the body into the stored rule. Only the fields
/// that the body carries change; an omitted `sql`, `name`, `enable`,
/// `actions` or `tenant` keeps the stored value. The `FROM` clause of a
/// supplied `sql` is read with the rule SQL parser. The `from` array is
/// not a create field; routing comes from `sql` only.
///
/// The path id is authoritative: a body `id` that names a different rule
/// is rejected instead of renaming or forking the entry.
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
// TODO(parity): on an unknown id with an invalid body, may the answer be
// 400 instead of 404? The reference reads the stored rule first; this
// handler answers 404 before body validation and creates nothing.
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
    // The reference PUT merges the body into the stored rule. Only the
    // fields present change; absent fields keep the stored value.
    let name = match obj.get("name") {
        None => existing.name.clone(),
        Some(value) => match value.as_str() {
            Some(name) => name.trim().to_string(),
            None => {
                return ApiError::BadRequest("field `name` must be a string".to_string())
                    .into_response();
            }
        },
    };
    let description = match obj.get("description") {
        None => existing.description.clone(),
        Some(value) => match value.as_str() {
            Some(text) => text.to_string(),
            None => {
                return ApiError::BadRequest("field `description` must be a string".to_string())
                    .into_response();
            }
        },
    };
    let metadata = match obj.get("metadata") {
        None => existing.metadata.clone(),
        Some(value) if value.is_object() => value.clone(),
        Some(_) => {
            return ApiError::BadRequest("field `metadata` must be an object".to_string())
                .into_response();
        }
    };
    let sql = match obj.get("sql") {
        None => existing.sql_query.clone(),
        Some(value) => match value
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            Some(sql) => Some(sql.to_string()),
            None => {
                return ApiError::BadRequest("field `sql` must be a non-empty string".to_string())
                    .into_response();
            }
        },
    };
    let topic_filter = match sql.as_deref() {
        Some(sql) => match topic_filter_from_sql(sql) {
            Ok(filter) => filter,
            Err(response) => return *response,
        },
        None => existing.topic_filter.clone(),
    };
    let enable = match obj.get("enable") {
        None => existing.enabled,
        Some(value) => match value.as_bool() {
            Some(enable) => enable,
            None => {
                return ApiError::BadRequest("field `enable` must be a boolean".to_string())
                    .into_response();
            }
        },
    };
    let actions = match obj.get("actions") {
        None => existing.actions.clone(),
        Some(value) => match value.as_array() {
            Some(items) => match parse_rule_actions(&state, items) {
                Ok(actions) => actions,
                Err(response) => return *response,
            },
            None => {
                return ApiError::BadRequest("field `actions` must be an array".to_string())
                    .into_response();
            }
        },
    };
    // Tenant scope: a body tenant moves the rule. Absent keeps the
    // stored tenant. A present but non-string tenant fails with 400.
    // An unknown tenant fails with 400 and keeps the old rule.
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
                return ApiError::BadRequest(format!("unknown tenant {tenant:?} (field `tenant`)"))
                    .into_response();
            }
            tenant
        }
    };
    if let Some(response) = refuse_forward_outside_default_tenant(&tenant, &actions) {
        return response;
    }
    match state.engine.replace_rule_in_tenant(
        &id,
        tenant,
        name,
        description,
        metadata,
        topic_filter,
        sql,
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
/// `passed`, `failed`, `failed.no_result` plus the per-rule action
/// attribution `actions.total`/`actions.success`/`actions.failed`). Every
/// field is a lock-free atomic bumped only on the evaluation path
/// (`RuleEngine::dispatch_ingress` in `broker-rules`), never a constant:
/// `matched` counts topic-filter hits, `passed` counts rows that produced
/// output, `failed` counts hits that produced none, and
/// `failed.no_result` counts the SQL no-result subset. A fresh rule reads
/// zeroes; a restart rebuilds the map at zero (memory-only by design,
/// never persisted). The response carries the same metrics for the local
/// node in `node_metrics`, as the reference does.
///
/// An unknown id returns 404 with the documented `{code, message}` shape,
/// not an empty body. The pair is not paged; unknown query parameters are
/// ignored (the ignored query sink below). Management-plane only: one
/// snapshot clone plus relaxed atomic loads; increments stay lock-free
/// atomics and publish/deliver never touch this map directly.
///
/// Bound (stated here and enforced by construction): a fixed seven-counter
/// set per rule (four `AtomicU64` for match/pass/fail/no-result plus three
/// for action attribution, default zero); no per-fire allocation (one
/// snapshot clone per request on the read path, relaxed loads only) and no
/// unbounded history.
///
/// Fire-path cost (stated for the delivery-path note): no new work on
/// publish or deliver. The evaluation path pays one relaxed `fetch_add`
/// per counter per ingress (lock-free atomics only, no lock, no queue, no
/// per-fire allocation); text rendering stays here in the handler.
// TODO(parity): the reference body also lists `failed.exception`,
// `failed.unknown`, three `matched.rate` fields and three action failure
// counters that this engine does not classify or measure. They stay out
// rather than appear as invented zeroes.
fn rule_metrics_value(r: &broker_rules::Rule) -> serde_json::Value {
    let load =
        |counter: &std::sync::atomic::AtomicU64| counter.load(std::sync::atomic::Ordering::Relaxed);
    serde_json::json!({
        "matched": load(&r.matched_cnt),
        "passed": load(&r.passed_cnt),
        "failed": load(&r.failed_cnt),
        "failed.no_result": load(&r.no_result_cnt),
        "actions.total": load(&r.actions_total_cnt),
        "actions.success": load(&r.actions_success_cnt),
        "actions.failed": load(&r.actions_failed_cnt),
    })
}

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
    let metrics = rule_metrics_value(r);
    let node_metrics = serde_json::json!([{
        "node": local_node_name(),
        "metrics": metrics.clone(),
    }]);
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "id": id,
            "metrics": metrics,
            "node_metrics": node_metrics,
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
    r.no_result_cnt
        .store(0, std::sync::atomic::Ordering::Relaxed);
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
                            let lazy_http =
                                matches!(canonical_connector_kind(conn_type.as_str()), "webhook");
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
                                if let Err(register_error) = try_register_live_sink(
                                    engine,
                                    &conn_type,
                                    &id,
                                    &params,
                                    registry.dir(),
                                )
                                .await
                                {
                                    tracing::warn!(
                                        connector = %id,
                                        kind = %conn_type,
                                        "{register_error}"
                                    );
                                } else if lazy_http {
                                    // The registration stayed lazy, but the
                                    // health probe now runs before service.
                                    if let Some(sink) = engine.connectors().get(&id) {
                                        let _ = sink.probe().await;
                                    }
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

    /// Directory holding kernel state. Disk sinks join it.
    /// Defaults to `./data` in unit tests with no registry.
    pub(crate) fn data_dir(&self) -> std::path::PathBuf {
        self.registry
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref()
            .map(|registry| registry.dir().to_path_buf())
            .unwrap_or_else(|| std::path::PathBuf::from("./data"))
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
/// except kinds that need a target, which fail closed without one.
/// Otherwise a 1.5 s TCP connect decides.
pub(crate) async fn probe_connector_reachable(body: &serde_json::Value) -> bool {
    match extract_target_host_port(body) {
        Some(target) => check_tcp_reachable(&target).await.is_ok(),
        None => {
            let kind = body.get("type").and_then(|v| v.as_str()).unwrap_or("");
            !probe_kind_needs_target(kind)
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

/// Fill the driver fields of an Azure Event Hubs connector from the
/// reference fields. The reference connector carries `authentication`,
/// `bootstrap_hosts` and `ssl`; the driver reads `namespace` and the
/// SAS pair. Each value comes from an operator input, never a constant.
fn apply_cloud_reference_fields(conn_type: &str, body: &mut serde_json::Value) {
    if canonical_connector_kind(conn_type) != "azure_eventhubs" {
        return;
    }
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    fn text(map: &serde_json::Map<String, serde_json::Value>, key: &str) -> Option<String> {
        map.get(key)
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    }
    let password = obj
        .get("authentication")
        .and_then(|value| value.as_object())
        .and_then(|auth| text(auth, "password"));
    let mut endpoint_namespace = None;
    let mut key_name = None;
    let mut secret = None;
    if let Some(password) = &password {
        for part in password.split(';') {
            let Some((name, value)) = part.split_once('=') else {
                continue;
            };
            let value = value.trim();
            match name.trim() {
                "Endpoint" => {
                    let host = value.trim_start_matches("sb://").trim_end_matches('/');
                    if let Some(label) = host.split('.').next().filter(|label| !label.is_empty()) {
                        endpoint_namespace = Some(label.to_string());
                    }
                }
                "SharedAccessKeyName" => key_name = Some(value.to_string()),
                "SharedAccessKey" => secret = Some(value.to_string()),
                _ => {}
            }
        }
    }
    if text(obj, "namespace").is_none() {
        let namespace = endpoint_namespace.or_else(|| {
            text(obj, "bootstrap_hosts").and_then(|hosts| {
                let host = hosts
                    .split(',')
                    .map(str::trim)
                    .find(|host| !host.is_empty())?;
                let host = host.rsplit_once(':').map_or(host, |(host, _)| host);
                let label = host.split('.').next()?;
                if label.is_empty() {
                    None
                } else {
                    Some(label.to_string())
                }
            })
        });
        if let Some(namespace) = namespace {
            obj.insert(
                "namespace".to_string(),
                serde_json::Value::String(namespace),
            );
        }
    }
    if text(obj, "shared_access_key_name").is_none() {
        if let Some(key_name) = key_name {
            obj.insert(
                "shared_access_key_name".to_string(),
                serde_json::Value::String(key_name),
            );
        }
    }
    if text(obj, "shared_access_key").is_none() {
        if let Some(secret) = secret {
            obj.insert(
                "shared_access_key".to_string(),
                serde_json::Value::String(secret),
            );
        }
    }
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
fn missing_connector_field(
    conn_type: &str,
    body: &serde_json::Value,
    data_dir: &std::path::Path,
) -> Option<&'static str> {
    fn present(body: &serde_json::Value, keys: &[&str]) -> bool {
        keys.iter().any(|k| {
            body.get(*k)
                .and_then(|v| v.as_str())
                .is_some_and(|s| !s.trim().is_empty())
        })
    }

    /// Top-level or reference `parameters` object, for kinds whose
    /// connectors accept both shapes.
    fn present_nested(body: &serde_json::Value, keys: &[&str]) -> bool {
        if present(body, keys) {
            return true;
        }
        body.get("parameters").is_some_and(|params| {
            keys.iter().any(|k| {
                params
                    .get(*k)
                    .and_then(|v| v.as_str())
                    .is_some_and(|s| !s.trim().is_empty())
            })
        })
    }

    /// Reference `authentication` object (top-level or nested in
    /// `parameters`), for kinds whose credentials may arrive there.
    fn nested_auth_field(body: &serde_json::Value, key: &str) -> bool {
        for parent in [
            body.get("authentication"),
            body.get("parameters")
                .and_then(|params| params.get("authentication")),
        ] {
            if parent.is_some_and(|auth| {
                auth.get(key)
                    .and_then(|v| v.as_str())
                    .is_some_and(|s| !s.trim().is_empty())
            }) {
                return true;
            }
        }
        false
    }

    // A present target or identity field must carry a value. A blank
    // string is the same as a missing setting and never selects a default.
    for key in [
        "database",
        "database_name",
        "table",
        "table_name",
        "server",
        "endpoint",
        "url",
        "host",
        "catalog",
        "schema",
        "topic",
        "bucket",
        "org",
        "token",
        "instance_name",
        "table_bucket_arn",
        "stable_name",
        "filename_prefix",
        "private_key_pem",
        "device_path",
        "device_path_template",
        "workgroup_name",
        "connection_string",
    ] {
        if body
            .get(key)
            .and_then(|value| value.as_str())
            .is_some_and(|value| value.trim().is_empty())
        {
            return Some(key);
        }
    }
    // An identifier is one bare name. A space would become a different
    // target or an invalid statement, so refuse it here.
    for key in [
        "database",
        "database_name",
        "table",
        "table_name",
        "catalog",
        "schema",
        "stable_name",
        "instance_name",
        "device_path",
        "device_path_template",
        "filename_prefix",
        "workgroup_name",
        "topic",
    ] {
        if body
            .get(key)
            .and_then(|value| value.as_str())
            .is_some_and(|value| value.chars().any(char::is_whitespace))
        {
            return Some(key);
        }
    }
    match conn_type {
        "gcp_iot" | "gcp_iot_core" | "cloud_iot" => {
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
        "confluent" | "confluent_producer" => {
            if !present(body, &["api_key", "username"]) && !nested_auth_field(body, "username") {
                return Some("api_key");
            }
            if !present(body, &["api_secret", "password"]) && !nested_auth_field(body, "password") {
                return Some("api_secret");
            }
            if !present_nested(body, &["bootstrap_hosts", "bootstrap_servers", "servers"]) {
                return Some("bootstrap_hosts");
            }
            None
        }
        "kinesis" | "aws_kinesis" => {
            if !present(body, &["endpoint", "url", "server"]) {
                return Some("endpoint");
            }
            if !present(body, &["aws_access_key_id", "access_key_id", "ak"]) {
                return Some("aws_access_key_id");
            }
            if !present(body, &["aws_secret_access_key", "secret_access_key", "sk"]) {
                return Some("aws_secret_access_key");
            }
            None
        }
        "gcp_pubsub" | "gcp" | "pubsub" | "gcp_pubsub_producer" | "gcp_pubsub_consumer" => {
            // The driver reads `project_id`. The sink ignores
            // `authentication`. A body with only `authentication`
            // has no usable identity and is refused.
            if !present(body, &["project_id", "gcp_project_id", "project"]) {
                return Some("project_id");
            }
            None
        }
        "azure_eventhubs"
        | "azure_event_hubs"
        | "azure"
        | "eventhubs"
        | "azure_event_hub_producer" => {
            // The driver reads `namespace` and the SAS pair.
            // `apply_cloud_reference_fields` fills both from the
            // reference `authentication` and `bootstrap_hosts` input
            // before this check runs.
            if !present(body, &["namespace"]) {
                return Some("namespace");
            }
            if !present(body, &["shared_access_key_name", "key_name", "policy"]) {
                return Some("shared_access_key_name");
            }
            if !present(body, &["shared_access_key", "key"]) {
                return Some("shared_access_key");
            }
            None
        }
        "azure_iot" | "azure_iot_hub" => {
            if !present(body, &["iot_hub_name", "hub_name", "server", "endpoint"]) {
                return Some("iot_hub_name");
            }
            if !present(body, &["device_id", "device"]) {
                return Some("device_id");
            }
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
        // MV-02d object, file and cloud kinds: every target value is
        // operator-supplied. A missing field answers 400 with its name.
        "webhook" | "http" | "rest" => {
            if !present(body, &["url", "endpoint"]) {
                return Some("url");
            }
            None
        }
        "opc_ua" | "opcua" => {
            if !present(body, &["endpoint_url", "server", "url", "endpoint"]) {
                return Some("endpoint_url");
            }
            None
        }
        "sparkplug_b" | "sparkplug" | "spb" => {
            if !present(
                body,
                &["mqtt_url", "broker_address", "server", "endpoint", "url"],
            ) {
                return Some("mqtt_url");
            }
            None
        }
        "s3" | "minio" => {
            if !present(body, &["endpoint", "host", "url", "server"]) {
                return Some("host");
            }
            None
        }
        "azure_blob" | "azure_blob_storage" | "azureblob" | "azblob" => {
            if !present(body, &["account_name", "account"]) {
                return Some("account_name");
            }
            // The reference form carries the credential in `auth` or
            // `auth`-style variants. Accept every documented form; the
            // sink builder resolves the same names.
            let credential_keys = [
                "account_key",
                "accountKey",
                "sas_token",
                "sas",
                "token",
                "bearer_token",
            ];
            let nested_credential = ["auth", "authentication"].iter().any(|parent| {
                body.get(*parent)
                    .and_then(|value| value.as_object())
                    .is_some_and(|auth| {
                        credential_keys.iter().any(|key| {
                            auth.get(*key)
                                .and_then(|value| value.as_str())
                                .is_some_and(|text| !text.trim().is_empty())
                        })
                    })
            });
            if !present(body, &credential_keys) && !nested_credential {
                return Some("account_key");
            }
            None
        }
        "dynamodb" | "dynamo" | "ddb" => {
            if !present(body, &["url", "endpoint", "server"]) {
                return Some("url");
            }
            if !present(body, &["region"]) {
                return Some("region");
            }
            None
        }
        "bigquery" | "bq" | "gbq" => {
            // The native form names `authentication`; the reference form
            // names the same object `auth`. Both carry an operator value.
            if !body
                .get("authentication")
                .or_else(|| body.get("auth"))
                .is_some_and(|value| value.is_object())
            {
                return Some("authentication");
            }
            None
        }
        "disk_log" | "disk" | "disklog" => {
            // Lane A wrote the file under `directory` with a separate
            // prefix and extension. That form supplies its own bounds in
            // the sink builder; only the native `filepath` form needs the
            // path and rotating-file fields here.
            let legacy_directory = body
                .get("directory")
                .or_else(|| body.get("dir"))
                .and_then(|value| value.as_str())
                .is_some_and(|value| !value.trim().is_empty());
            if legacy_directory && !present(body, &["filepath", "file"]) {
                return None;
            }
            if !present(body, &["filepath", "file", "path"]) {
                return Some("filepath");
            }
            if !present(body, &["max_file_size"]) {
                return Some("max_file_size");
            }
            if !body
                .get("max_file_number")
                .and_then(|value| value.as_u64())
                .is_some_and(|value| value > 0)
            {
                return Some("max_file_number");
            }
            let filepath = body
                .get("filepath")
                .or_else(|| body.get("file"))
                .or_else(|| body.get("path"))
                .and_then(|value| value.as_str())
                .unwrap_or_default();
            if !disk_log_path_contained(filepath, data_dir) {
                return Some("filepath");
            }
            None
        }
        "snowflake" | "snowflake_streaming" | "snowflake_aggregated" => {
            if !present(body, &["account"]) {
                return Some("account");
            }
            if !present(body, &["pipe_user", "user", "username"]) {
                return Some("pipe_user");
            }
            if !present(body, &["private_key_pem", "private_key"]) {
                return Some("private_key_pem");
            }
            if !present(body, &["server", "endpoint", "url"]) {
                return Some("server");
            }
            None
        }
        "tablestore" | "ots" => {
            if !present(body, &["access_key_secret", "sk"]) {
                return Some("access_key_secret");
            }
            if !present(body, &["access_key_id", "ak"]) {
                return Some("access_key_id");
            }
            if !present(body, &["instance_name", "instance"]) {
                return Some("instance_name");
            }
            if !present(body, &["endpoint", "url"]) {
                return Some("endpoint");
            }
            None
        }
        // The MV-02b time-series families carry their own fail-closed
        // gate (documented defaults only); the remaining families keep
        // their existing behaviour until their service documentation is
        // checked for real defaults.
        "kafka" | "kafka_producer" | "kafka_consumer" => {
            if !present_nested(body, &["bootstrap_hosts", "bootstrap_servers", "servers"]) {
                return Some("bootstrap_hosts");
            }
            // Only `none` is wired to the plaintext driver. Any other
            // authentication value is refused here, naming the field,
            // instead of storing credentials the broker never uses.
            let auth = body.get("authentication").or_else(|| {
                body.get("parameters")
                    .and_then(|params| params.get("authentication"))
            });
            let auth_none = match auth {
                None => true,
                Some(value) if value.is_null() => true,
                Some(serde_json::Value::String(text)) => {
                    text.trim().is_empty() || text.trim() == "none"
                }
                Some(serde_json::Value::Object(map)) => map.is_empty(),
                Some(_) => false,
            };
            if !auth_none {
                return Some("authentication");
            }
            None
        }
        "rabbitmq" => {
            if !present_nested(body, &["servers", "server"]) {
                return Some("servers");
            }
            // Check password before username. Source: reference lists
            // password before username in required fields.
            if !present_nested(body, &["password", "pass"]) {
                return Some("password");
            }
            if !present_nested(body, &["username", "user"]) {
                return Some("username");
            }
            None
        }
        "redis" => {
            if !present_nested(body, &["servers", "server", "host"]) {
                return Some("servers");
            }
            None
        }
        "mqtt_bridge" | "mqtt" => {
            if !present_nested(body, &["server", "broker_address", "broker", "host"]) {
                return Some("server");
            }
            if body
                .get("client_id")
                .and_then(|value| value.as_str())
                .is_some_and(|value| value.trim().is_empty())
            {
                return Some("client_id");
            }
            None
        }
        "pulsar" => {
            if !present_nested(body, &["servers", "service_url", "server", "endpoint"]) {
                return Some("servers");
            }
            // Only `none` is wired to the driver. Token and basic
            // forms are refused here, naming the field, instead of
            // storing credentials the broker never uses.
            let auth = body.get("authentication").or_else(|| {
                body.get("parameters")
                    .and_then(|params| params.get("authentication"))
            });
            let auth_none = match auth {
                None => true,
                Some(value) if value.is_null() => true,
                Some(serde_json::Value::String(text)) => {
                    text.trim().is_empty() || text.trim() == "none"
                }
                Some(serde_json::Value::Object(map)) => map.is_empty(),
                Some(_) => false,
            };
            if !auth_none {
                return Some("authentication");
            }
            None
        }
        "rocketmq" => {
            if !present_nested(body, &["servers", "endpoints", "endpoint", "server"]) {
                return Some("servers");
            }
            // The pair sets together or not at all. A half pair
            // fails closed here, so create names the missing half
            // instead of storing a half credential.
            let has_access = present_nested(body, &["access_key"]);
            let has_secret = present_nested(body, &["secret_key"]);
            if has_access != has_secret {
                return Some(if has_access {
                    "secret_key"
                } else {
                    "access_key"
                });
            }
            None
        }
        _ => missing_mv02b_connector_field(conn_type, body),
    }
}

/// True when a `disk_log` file path stays inside the data directory:
/// relative, with no `..`, root or Windows prefix component, and with
/// no symlink that leaves the directory.
fn disk_log_path_contained(filepath: &str, data_dir: &std::path::Path) -> bool {
    use std::path::{Component, Path};
    let path = Path::new(filepath);
    if filepath.trim().is_empty()
        || path.is_absolute()
        || path.components().any(|part| {
            matches!(
                part,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return false;
    }
    // Fail closed on symlinks: any symlink on the path or its parents
    // can point outside the data directory, so refuse it. Check the
    // path as given and joined to the data directory, so a link
    // inside the data directory is refused even when the process
    // working directory differs from the data directory.
    if disk_log_has_symlink(path) {
        return false;
    }
    if disk_log_has_symlink(&data_dir.join(filepath)) {
        return false;
    }
    true
}

/// True when the path or any existing parent is a symlink.
fn disk_log_has_symlink(path: &std::path::Path) -> bool {
    let mut current = path;
    loop {
        if std::fs::symlink_metadata(current).is_ok_and(|meta| meta.file_type().is_symlink()) {
            return true;
        }
        match current.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => current = parent,
            _ => break,
        }
        if current.as_os_str().is_empty() {
            break;
        }
    }
    false
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
            // A present top-level field must still carry a value; the
            // reference form does not turn an empty top-level override
            // into a silently ignored setting.
            for key in ["bucket", "org", "token", "measurement"] {
                if body
                    .get(key)
                    .and_then(|value| value.as_str())
                    .is_some_and(|value| value.trim().is_empty())
                {
                    return Some(key);
                }
            }
            // The reference form puts the connection fields in the
            // connector `parameters` object and lets the sink pick the
            // measurement. The native form keeps them at the top level.
            let nested_reference = body
                .get("parameters")
                .and_then(|value| value.as_object())
                .is_some_and(|params| {
                    ["bucket", "database", "org", "token", "username"]
                        .iter()
                        .any(|key| params.get(*key).is_some_and(|value| !value.is_null()))
                });
            if nested_reference {
                return None;
            }
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
            if let Some(endpoint) = body
                .get("endpoint")
                .or_else(|| body.get("url"))
                .and_then(|value| value.as_str())
                .map(str::trim)
                .filter(|value| value.contains("://"))
            {
                if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
                    return Some("endpoint");
                }
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
    let registered = engine.connectors().get(id);
    let health = registered.as_ref().and_then(|sink| sink.health());
    // A sink that reports its own probe result decides the state. A sink
    // without a probe falls back to registration presence. Never assume
    // a webhook target is reachable because it is registered.
    let live = match &health {
        Some(result) => result.healthy,
        None => registered.is_some(),
    };
    // KC-00/D2: a stored record whose kind is removed or not `ready` is
    // kept and visible but reads back `disconnected` with a reason.
    let mut reason = body.get("type").and_then(|v| v.as_str()).and_then(|kind| {
        if broker_config::is_removed_connector_type(kind) {
            Some(crate::REMOVED_CONNECTOR_MESSAGE.to_string())
        } else {
            kind_refusal(kind)
        }
    });
    if reason.is_none() {
        if let Some(result) = health {
            if !result.healthy {
                reason = Some(
                    result
                        .reason
                        .unwrap_or_else(|| "health check failed".to_string()),
                );
            }
        }
    }
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

/// Mask every secret in a connector document for any observable
/// response. The one shared rule is `broker_config::secrets::mask_secrets`:
/// a secret field shows `******` and a `file:`/`env:` reference shows its
/// `<redacted:...>` marker, never the value. The store keeps the value.
fn redact_connector_doc(value: &serde_json::Value) -> serde_json::Value {
    broker_config::secrets::masked_secrets(value)
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

/// True for the three IoT kinds that the CEO kept out of the MVP on
/// 2026-11-08. Create and update refuse them with `BAD_REQUEST`.
pub(crate) fn is_mvp_pending_iot_kind(kind: &str) -> bool {
    matches!(
        broker_config::kind_row(kind).map(|row| row.name),
        Some("aws_iot" | "azure_iot" | "oci_streaming")
    )
}

/// KC-00 refusal for a kind whose capability state is not `ready`.
///
/// Returns `None` for a ready kind (and for the `test` helper in a test
/// build); otherwise the exact refusal message. Unknown names are not the map's
/// business: the caller answers them before this runs.
pub(crate) fn kind_refusal(kind: &str) -> Option<String> {
    let row = broker_config::kind_row(kind);
    match row.map(|r| r.state) {
        Some(broker_config::KindState::Ready) => return None,
        Some(broker_config::KindState::TestOnly) => {
            if broker_config::test_kinds_enabled() {
                return None;
            }
        }
        Some(_) => {}
        None => return None,
    }
    let canonical = row.map(|r| r.name).unwrap_or(kind);
    Some(format!(
        "connector type {canonical} is not available in this broker"
    ))
}

/// Make a refused kind visible in the alarm list. The alarm name carries
/// the kind so an operator can find the refusal that created it.
pub(crate) fn raise_unavailable_kind_alarm(state: &ApiState, kind: &str) {
    if kind_refusal(kind).is_none() {
        return;
    }
    let canonical = broker_config::kind_row(kind)
        .map(|row| row.name)
        .unwrap_or(kind);
    state.alarms.activate(
        &format!("kind_unavailable:{canonical}"),
        &format!("kind {canonical:?} is not available; the request is refused"),
        serde_json::json!({"kind": canonical}),
    );
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

/// Canonical type of a connector. Documented aliases map to the
/// runtime kind that executes them. The reference names its HTTP
/// connector `http`; the tree runs it in the `webhook` arm.
/// The stored type keeps the reference name when the reference
/// defines one. `gcp_pubsub_producer` stays `gcp_pubsub_producer`.
/// All other aliases map to the base kind.
fn canonical_connector_kind(conn_type: &str) -> &str {
    match conn_type {
        "http" | "rest" => "webhook",
        "logger" => "console",
        "mongo" | "documentdb" => "mongodb",
        "couch" | "cb" => "couchbase",
        "elastic" | "es" | "opensearch" => "elasticsearch",
        "opcua" => "opc_ua",
        "sparkplug" | "spb" => "sparkplug_b",
        "aws_kinesis" => "kinesis",
        "kafka_producer" | "kafka_consumer" => "kafka",
        "confluent_producer" | "confluent_kafka" | "confluent_cloud" => "confluent",
        "gcp_pubsub_producer" => "gcp_pubsub_producer",
        "gcp_pubsub_consumer" => "gcp_pubsub_consumer",
        "gcp" | "pubsub" => "gcp_pubsub",
        "azure_event_hubs" | "azure" | "eventhubs" | "azure_event_hub_producer" => {
            "azure_eventhubs"
        }
        "oci" | "oracle_streaming" => "oci_streaming",
        "aws_iot_core" => "aws_iot",
        "azure_iot_hub" | "azure_iothub" | "iothub" => "azure_iot",
        "gcp_iot_core" | "cloud_iot" => "gcp_iot",
        _ => conn_type,
    }
}

/// Type stored and returned in a connector document.
/// The reference names its HTTP connector `http`.
/// `rest` is the legacy alias of the same kind.
/// The runtime executes both in the `webhook` arm.
/// The document keeps `kafka` and `confluent`.
/// Their aliases map to the same stored type.
fn stored_connector_kind(conn_type: &str) -> &str {
    match conn_type {
        "http" | "rest" => "http",
        "kafka_consumer" => "kafka_consumer",
        "kafka" | "kafka_producer" => "kafka",
        "confluent" | "confluent_producer" | "confluent_kafka" | "confluent_cloud" => "confluent",
        other => canonical_connector_kind(other),
    }
}

/// Split an S3-style endpoint into the reference `host` and `port`.
/// The API accepts `endpoint`, `url`, `server` or `host`. The reference
/// connector schema requires `host` and `port`, so the stored document
/// carries the derived pair next to the operator value. A port the
/// endpoint does not name defaults to the URL scheme (`http` reads 80,
/// `https` reads 443). An unparseable value changes nothing.
fn normalize_s3_host_port(obj: &mut serde_json::Map<String, serde_json::Value>) {
    fn text(obj: &serde_json::Map<String, serde_json::Value>, key: &str) -> Option<String> {
        obj.get(key)
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(str::to_string)
    }
    if text(obj, "host").is_some() && obj.get("port").and_then(|v| v.as_u64()).is_some() {
        return;
    }
    let Some(raw) = text(obj, "endpoint")
        .or_else(|| text(obj, "url"))
        .or_else(|| text(obj, "server"))
        .or_else(|| text(obj, "host"))
    else {
        return;
    };
    let (scheme, rest) = match raw.split_once("://") {
        Some((scheme, rest)) => (scheme.to_lowercase(), rest),
        None => (String::new(), raw.as_str()),
    };
    let authority = rest
        .split('/')
        .next()
        .unwrap_or("")
        .split('?')
        .next()
        .unwrap_or("");
    let authority = authority
        .split('@')
        .next_back()
        .unwrap_or("")
        .trim()
        .trim_end_matches(':');
    if authority.is_empty() {
        return;
    }
    let (host, port) = if let Some(stripped) = authority.strip_prefix('[') {
        // Bracketed IPv6 (`[::1]:9000`).
        match stripped.split_once("]:") {
            Some((host, port)) => (format!("[{host}]"), port.trim().to_string()),
            None => (authority.to_string(), String::new()),
        }
    } else if authority.matches(':').count() == 1 {
        match authority.split_once(':') {
            Some((host, port)) => (host.trim().to_string(), port.trim().to_string()),
            None => (authority.to_string(), String::new()),
        }
    } else {
        (authority.to_string(), String::new())
    };
    if host.is_empty() {
        return;
    }
    let port = port
        .parse::<u64>()
        .ok()
        .filter(|port| *port > 0)
        .or_else(|| {
            if scheme == "http" {
                Some(80)
            } else if scheme == "https" {
                Some(443)
            } else {
                None
            }
        });
    if text(obj, "host").is_none() {
        obj.insert("host".to_string(), serde_json::Value::String(host));
    }
    if obj.get("port").and_then(|v| v.as_u64()).is_none() {
        if let Some(port) = port {
            obj.insert("port".to_string(), serde_json::Value::Number(port.into()));
        }
    }
}

/// Store a numeric `disk_log` retention count in its string form.
/// The reference rotation schema reads a duration string (`"12m"`) or
/// `"infinity"`. The driver also accepts a day count, so a numeric
/// count is stored as its string form. The driver parses the same
/// days back. A string value stays unchanged.
fn normalize_disk_log_retention(obj: &mut serde_json::Map<String, serde_json::Value>) {
    let Some(rotation) = obj.get_mut("rotation").and_then(|v| v.as_object_mut()) else {
        return;
    };
    if let Some(days) = rotation.get("retention_period").and_then(|v| v.as_u64()) {
        rotation.insert(
            "retention_period".to_string(),
            serde_json::Value::String(days.to_string()),
        );
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
        serde_json::Value::String(stored_connector_kind(conn_type).to_string()),
    );
    match crate::v5::action_sinks::canonical_action_kind(conn_type) {
        "s3" => normalize_s3_host_port(obj),
        "disk_log" => normalize_disk_log_retention(obj),
        _ => {}
    }
    // Live API compatibility: the read back document carries the
    // required reference fields. The driver uses the original input
    // before this step; nothing here changes delivery. The code
    // invents no value: each derived field uses an operator input or
    // the reference default named below.
    if canonical == "azure_eventhubs" {
        // The reference requires `bootstrap_hosts`, `authentication`
        // and `ssl`. The driver needs `namespace` plus the SAS pair.
        // Derive `bootstrap_hosts` from `namespace` plus the Kafka port.
        // The Kafka port is the Event Hubs port.
        //
        // Derive `authentication` password from `shared_access_key`.
        // Use empty `ssl` when the operator gives none.
        // Empty `ssl` selects the reference default.
        if obj.get("bootstrap_hosts").is_none() {
            if let Some(namespace) = read(obj, "namespace") {
                obj.insert(
                    "bootstrap_hosts".to_string(),
                    serde_json::Value::String(format!(
                        "{namespace}.servicebus.windows.net:{AZURE_EVENTHUBS_KAFKA_PORT}"
                    )),
                );
            }
        }
        if obj.get("authentication").is_none() {
            if let Some(secret) = read(obj, "shared_access_key").or_else(|| read(obj, "key")) {
                obj.insert(
                    "authentication".to_string(),
                    serde_json::json!({"password": secret}),
                );
            }
        }
        if obj.get("ssl").is_none() {
            obj.insert("ssl".to_string(), serde_json::json!({}));
        }
        // The reference names this kind `azure_event_hub_producer`.
        // Store the reference name so the response validates.
        obj.insert(
            "type".to_string(),
            serde_json::Value::String("azure_event_hub_producer".to_string()),
        );
    }
    if canonical == "kafka" {
        // The reference names this kind `kafka_producer`. Store the
        // reference name so the response validates. A `kafka_consumer`
        // keeps the type that was sent.
        let stored = if conn_type == "kafka_consumer" {
            "kafka_consumer"
        } else {
            "kafka_producer"
        };
        obj.insert(
            "type".to_string(),
            serde_json::Value::String(stored.to_string()),
        );
    }
    if canonical == "confluent" {
        // The reference requires `authentication` and `ssl`.
        // Derive `authentication` from the operator pair.
        // Use empty `ssl` when the operator gives none.
        // Empty `ssl` selects the reference default.
        if obj.get("authentication").is_none() {
            let username = read(obj, "api_key").or_else(|| read(obj, "username"));
            let password = read(obj, "api_secret").or_else(|| read(obj, "password"));
            if let (Some(username), Some(password)) = (username, password) {
                obj.insert(
                    "authentication".to_string(),
                    serde_json::json!({"username": username, "password": password}),
                );
            }
        }
        if obj.get("ssl").is_none() {
            obj.insert("ssl".to_string(), serde_json::json!({}));
        }
        // The reference names this kind `confluent_producer`.
        // Store the reference name so the response validates.
        obj.insert(
            "type".to_string(),
            serde_json::Value::String("confluent_producer".to_string()),
        );
    }
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

/// Client for a registered webhook connector. The request timeout bounds
/// a delivery the same way the action runtime bounds one; the action path
/// passes its own client built from `resource_opts.request_ttl`.
fn webhook_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(broker_connectors::DEFAULT_WEBHOOK_TIMEOUT)
        .build()
        .unwrap_or_default()
}

/// Checked registration: a kind whose Cargo feature is disabled fails
/// closed with an error naming the missing feature — never a silent
/// skip, never a fallback identity. A sink or transport that fails to
/// build fails the same way with its own error text. Enabled kinds go
/// through [`register_live_sink`], the same path `create_connector` uses.
/// Qualification calls this entry directly.
pub async fn try_register_live_sink(
    engine: &broker_rules::RuleEngine,
    conn_type: &str,
    name: &str,
    body: &serde_json::Value,
    data_dir: &std::path::Path,
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
    register_live_sink(engine, runtime, name, body, data_dir).await
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

/// MQTT bridge keep-alive default. Reason: the connector schema default.
const MQTT_BRIDGE_KEEPALIVE_SECS: u16 = 60;
/// MQTT bridge inflight bound. Reason: 1000 rows cap the sink queue
/// while the runtime drains it.
const MQTT_BRIDGE_MAX_INFLIGHT: usize = 1_000;
/// MQTT bridge rows per send. Reason: one row per send keeps the
/// runtime in charge of batching.
const MQTT_BRIDGE_BATCH_ROWS: usize = 1;
/// MQTT bridge linger. Reason: 10 ms lets one row wait for the runtime
/// batch timer.
const MQTT_BRIDGE_LINGER_MS: u64 = 10;

/// Read one positive bridge bound. A value below 1 is refused so the
/// sink never stores an unbounded or zero limit.
fn parse_bridge_bound(
    name: &str,
    value: Option<&serde_json::Value>,
    default: usize,
    field: &str,
) -> Result<usize, String> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(default),
        Some(value) => value
            .as_u64()
            .filter(|bound| *bound >= 1)
            .and_then(|bound| usize::try_from(bound).ok())
            .ok_or_else(|| {
                format!("connector {name:?} field `{field}` must be a positive integer")
            }),
    }
}

/// Read the bridge linger window. The reference form accepts a whole
/// number of milliseconds; an absent value uses the sink default.
fn parse_bridge_linger(
    name: &str,
    value: Option<&serde_json::Value>,
    default: u64,
) -> Result<u64, String> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(default),
        Some(value) => value.as_u64().ok_or_else(|| {
            format!("connector {name:?} field `linger_ms` must be a whole number of milliseconds")
        }),
    }
}

/// Parse the connector `keepalive` value. Accept whole seconds or a
/// duration string such as `60s`, `1m` or `1500ms`. Reason: the
/// reference connector accepts both shapes.
pub(crate) fn parse_bridge_keepalive(name: &str, value: &serde_json::Value) -> Result<u16, String> {
    let bad = || {
        format!(
            "connector {name:?} field `keepalive` must be whole seconds or a duration string such as `60s`"
        )
    };
    let seconds = match value {
        serde_json::Value::Number(number) => number.as_f64().ok_or_else(bad)?,
        serde_json::Value::String(text) => {
            let text = text.trim().to_ascii_lowercase();
            let split = text
                .find(|c: char| !c.is_ascii_digit() && c != '.')
                .ok_or_else(bad)?;
            let (number, unit) = text.split_at(split);
            let number: f64 = number.parse().map_err(|_| bad())?;
            match unit.trim() {
                "ms" => number / 1_000.0,
                "s" => number,
                "m" => number * 60.0,
                "h" => number * 3_600.0,
                _ => return Err(bad()),
            }
        }
        _ => return Err(bad()),
    };
    if !seconds.is_finite() || seconds < 0.0 || seconds > f64::from(u16::MAX) {
        return Err(bad());
    }
    Ok(seconds.round() as u16)
}

/// Register one live sink on the engine. Qualification calls this entry.
/// Qualification covers both this entry and `build_connector`.
pub async fn register_live_sink(
    engine: &broker_rules::RuleEngine,
    conn_type: &str,
    name: &str,
    body: &serde_json::Value,
    data_dir: &std::path::Path,
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
                webhook_http_client(),
            ));
            engine.connectors().register(name, sink.clone());
            engine
                .connectors()
                .register(format!("webhook:{name}"), sink);
        }
        "redis" => {
            // The connector carries the connection only. The command
            // belongs to the action, so a connector alone registers no
            // sink and invents no target.
            crate::v5::action_sinks::validate_connector_io_bounds(body, name, "connector config")?;
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
                webhook_http_client(),
            ));
            engine.connectors().register(name, sink.clone());
            engine.connectors().register(format!("http:{}", name), sink);
        }
        #[cfg(feature = "kafka")]
        "kafka" => {
            // The connector carries the connection only. The topic,
            // key, headers and strategy belong to the action, so a
            // connector alone registers no sink and invents no target.
            crate::v5::action_sinks::validate_connector_io_bounds(body, name, "connector config")?;
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
            let raw_password = body.get("password").and_then(|v| v.as_str()).unwrap_or("");
            let password = broker_connectors::resolve_field_secret(raw_password);
            if broker_config::secrets::is_secret_ref(raw_password) && password.is_empty() {
                return Err(format!(
                    "connector {name:?} password reference is not resolvable (field `password`)"
                ));
            }
            let (host, port) = match server.rsplit_once(':') {
                Some((host, port)) => match port.parse::<u16>() {
                    Ok(port) => (host.trim().trim_matches(&['[', ']'][..]).to_string(), port),
                    Err(_) => {
                        return Err(format!("postgres bad port in {server:?}"));
                    }
                },
                None => (server.to_string(), 5432),
            };
            if host.is_empty() {
                return Err(format!(
                    "connector {name:?} is missing required field: server (field `server`)"
                ));
            }
            let use_tls = body
                .get("ssl")
                .and_then(|ssl| ssl.get("enable"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let settings = broker_connectors::postgres::PgConnectionSettings {
                host,
                port,
                user: username.to_string(),
                password,
                database: database.to_string(),
                use_tls,
            };
            if let Some(transport) = record_sink_error!(
                broker_connectors::postgres::DriverPgTransport::from_settings(settings, 1)
            ) {
                let display_url =
                    format!("postgres://{}@{}:{}/{}", username, server, port, database);
                let config = broker_connectors::postgres::PostgreSqlSinkConfig {
                    connection_url: display_url,
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
            // Fail closed when the target is missing: no invented server
            // or database. An empty config stays persist-only and reports
            // pending, never live. The username keeps the reference
            // default `root`, like the action builder.
            let has_server = body
                .get("server")
                .and_then(|v| v.as_str())
                .is_some_and(|s| !s.trim().is_empty());
            let has_database = body
                .get("database")
                .and_then(|v| v.as_str())
                .is_some_and(|s| !s.trim().is_empty());
            if !has_server || !has_database {
                return Ok(());
            }
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
            // The connector carries the connection only. The exchange,
            // routing key, headers and body belong to the action, so a
            // connector alone registers no sink and invents no target.
            crate::v5::action_sinks::validate_connector_io_bounds(body, name, "connector config")?;
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
            // Lane B keeps the target on the action. The reference form
            // carries bridge controls on the connector and keeps its
            // connector sink. Detect that form by its bridge fields.
            // The Rust bridge has no reader for the reference transport,
            // identity pool and TLS blocks. Refuse them before storage so
            // the operator never gets a setting that does nothing.
            for key in [
                "tcp_opts",
                "static_clientids",
                "pool_size",
                "bridge_mode",
                "mode",
                "connect_timeout",
                "retry_interval",
                "reconnect_interval",
                "ssl",
                // The reference names the identity `clientid_prefix`.
                "client_id",
            ] {
                if body.get(key).is_some_and(|value| !value.is_null()) {
                    return Err(format!(
                        "connector {name:?} field `{key}` is not supported by the MQTT bridge; remove it"
                    ));
                }
            }
            let reference_form = [
                "proto_ver",
                "keepalive",
                "clean_start",
                "clientid_prefix",
                "max_inflight",
                "batch_size",
                "max_batch_size",
                "linger_ms",
            ]
            .iter()
            .any(|key| body.get(*key).is_some());
            if !reference_form {
                crate::v5::action_sinks::validate_connector_io_bounds(
                    body,
                    name,
                    "connector config",
                )?;
                return Ok(());
            }
            // Fail closed when the operator omits the target. No
            // invented address.
            let Some(server) = body
                .get("server")
                .or_else(|| body.get("broker_address"))
                .or_else(|| body.get("broker"))
                .or_else(|| body.get("host"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err(format!(
                    "connector {name:?} is missing required field: server"
                ));
            };
            let client_id = body
                .get("clientid_prefix")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(name)
                .to_string();
            let username = body
                .get("username")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let password = body
                .get("password")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let keep_alive_secs = match body.get("keepalive") {
                None | Some(serde_json::Value::Null) => MQTT_BRIDGE_KEEPALIVE_SECS,
                Some(value) => parse_bridge_keepalive(name, value)?,
            };
            let clean_start = match body.get("clean_start") {
                None | Some(serde_json::Value::Null) => true,
                Some(value) => value.as_bool().ok_or_else(|| {
                    format!("connector {name:?} field `clean_start` must be a boolean")
                })?,
            };
            let protocol = match body
                .get("proto_ver")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            {
                None | Some("v3") | Some("v4") => broker_connectors::MqttBridgeProtocol::V311,
                Some("v5") => broker_connectors::MqttBridgeProtocol::V50,
                Some(other) => {
                    return Err(format!(
                        "connector {name:?} field `proto_ver` must be one of v3, v4, v5: {other:?}"
                    ));
                }
            };
            let max_inflight = parse_bridge_bound(
                name,
                body.get("max_inflight"),
                MQTT_BRIDGE_MAX_INFLIGHT,
                "max_inflight",
            )?;
            let max_batch_size = parse_bridge_bound(
                name,
                body.get("batch_size")
                    .or_else(|| body.get("max_batch_size")),
                MQTT_BRIDGE_BATCH_ROWS,
                "batch_size",
            )?;
            let linger_ms =
                parse_bridge_linger(name, body.get("linger_ms"), MQTT_BRIDGE_LINGER_MS)?;
            let config = broker_connectors::MqttBridgeSinkConfig {
                broker_address: server.to_string(),
                client_id,
                clean_start,
                username,
                password,
                keep_alive_secs,
                topic_prefix: None,
                topic_template: None,
                payload_template: None,
                qos_override: None,
                retain_override: None,
                max_inflight: Some(max_inflight),
                max_batch_size: Some(max_batch_size),
                linger_ms: Some(linger_ms),
                protocol,
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
        "influxdb" | "influx" => {
            // Documented `endpoint` first (DX2-023): it must already name
            // `http(s)` or the sink refuses it. Legacy `url` and `server`
            // carry `host:port` and keep the historic prefix for the
            // stored canonical parameters.
            let endpoint = if let Some(explicit) = body.get("endpoint").and_then(|v| v.as_str()) {
                explicit.to_string()
            } else {
                let url = body
                    .get("url")
                    .and_then(|v| v.as_str())
                    .or_else(|| body.get("server").and_then(|v| v.as_str()))
                    .unwrap_or("http://127.0.0.1:8086");
                if url.starts_with("http://") || url.starts_with("https://") {
                    url.to_string()
                } else {
                    format!("http://{url}")
                }
            };
            // The reference form keeps the connection fields in the
            // connector `parameters` object; the native form names them
            // at the top level. Read both and fail closed when a value
            // is absent. Never invent a bucket, org, token or
            // measurement.
            let nested = body.get("parameters").and_then(|value| value.as_object());
            let field = |keys: &[&str]| -> Option<String> {
                keys.iter().find_map(|key| {
                    body.get(*key)
                        .or_else(|| nested.and_then(|params| params.get(*key)))
                        .and_then(|value| value.as_str())
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(str::to_string)
                })
            };
            let Some(bucket) = field(&["bucket"]) else {
                return Err("influxdb connector is missing required field: bucket".to_string());
            };
            let Some(org) = field(&["org"]) else {
                return Err("influxdb connector is missing required field: org".to_string());
            };
            let Some(token) = field(&["token"]) else {
                return Err("influxdb connector is missing required field: token".to_string());
            };
            let Some(measurement) = field(&["measurement", "measurement_template"]) else {
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
            // Documented settings (DX2-023): the sink applies each one.
            // Missing batch size and linger keep the historic defaults.
            let batch_size = body.get("batch_size").and_then(|v| v.as_u64()).unwrap_or(1) as usize;
            let batch_timeout_ms = body
                .get("batch_timeout_ms")
                .or_else(|| body.get("linger_ms"))
                .and_then(|v| v.as_u64())
                .unwrap_or(100);
            let config = broker_connectors::InfluxDbSinkConfig {
                endpoint,
                bucket,
                org,
                token,
                measurement_template: measurement,
                precision,
                batch_size,
                batch_timeout_ms,
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
                    .register(format!("influxdb:{}", name), sink.clone());
                engine
                    .connectors()
                    .register(format!("influx:{}", name), sink);
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
                Some("influx") | Some("influx_line_protocol") => {
                    broker_connectors::greptimedb::GreptimeFormat::InfluxLineProtocol
                }
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
        "iotdb" | "iot_db" => {
            // Fail closed (S1-01): the create and update paths gate these
            // fields first; the arm repeats the check so boot replay cannot
            // invent a target either.
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
            // Documented auth object first (DX2-024). Flat fields stay for
            // the old entry shape. The value must be non-empty.
            let nested_auth = body.get("auth");
            let Some(username) = nested_auth
                .and_then(|auth| auth.get("username"))
                .and_then(|v| v.as_str())
                .or_else(|| body.get("username").and_then(|v| v.as_str()))
                .or_else(|| body.get("user").and_then(|v| v.as_str()))
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Err("iotdb connector is missing required field: username".to_string());
            };
            let Some(password) = nested_auth
                .and_then(|auth| auth.get("password"))
                .and_then(|v| v.as_str())
                .or_else(|| body.get("password").and_then(|v| v.as_str()))
                .filter(|s| !s.is_empty())
            else {
                return Err("iotdb connector is missing required field: password".to_string());
            };
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());

            // One path for each shape (DX2-024). Flat fields and the
            // documented object share defaults and validation.
            // Reason: docs default 200 rows per flush.
            let batch_size = match body.get("batch_size") {
                None => Some(200),
                Some(value) if value.is_null() => None,
                Some(value) => match value.as_u64() {
                    Some(v) => Some(v as usize),
                    None => {
                        return Err(
                            "invalid iotdb config: batch_size must be an integer".to_string()
                        );
                    }
                },
            };
            // Reason: docs default 2 MiB per batch.
            let batch_bytes = match body.get("batch_bytes") {
                None => Some(2_097_152),
                Some(value) if value.is_null() => None,
                Some(value) => match value.as_u64() {
                    Some(v) => Some(v as usize),
                    None => {
                        return Err(
                            "invalid iotdb config: batch_bytes must be an integer".to_string()
                        );
                    }
                },
            };
            // Reason: docs default 10 ms linger.
            let linger_ms = match body.get("linger_ms") {
                None => Some(10),
                Some(value) if value.is_null() => None,
                Some(value) => match value.as_u64() {
                    Some(v) => Some(v),
                    None => {
                        return Err(
                            "invalid iotdb config: linger_ms must be an integer".to_string()
                        );
                    }
                },
            };
            // Reason: docs default 3 retries on 305 and 5xx.
            let max_retries = match body.get("max_retries") {
                None => Some(3),
                Some(value) if value.is_null() => None,
                Some(value) => match value.as_u64() {
                    Some(v) => Some(v as usize),
                    None => {
                        return Err(
                            "invalid iotdb config: max_retries must be an integer".to_string()
                        );
                    }
                },
            };
            // Reason: docs default 100 ms first retry delay.
            let initial_backoff_ms = match body.get("initial_backoff_ms") {
                None => Some(100),
                Some(value) if value.is_null() => None,
                Some(value) => match value.as_u64() {
                    Some(v) => Some(v),
                    None => {
                        return Err(
                            "invalid iotdb config: initial_backoff_ms must be an integer"
                                .to_string(),
                        );
                    }
                },
            };
            // Reason: docs default 2000 ms retry ceiling.
            let max_backoff_ms = match body.get("max_backoff_ms") {
                None => Some(2_000),
                Some(value) if value.is_null() => None,
                Some(value) => match value.as_u64() {
                    Some(v) => Some(v),
                    None => {
                        return Err(
                            "invalid iotdb config: max_backoff_ms must be an integer".to_string()
                        );
                    }
                },
            };
            let is_aligned = body
                .get("is_aligned")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let config = broker_connectors_enterprise::iotdb::IotDbSinkConfig {
                endpoint,
                device_path_template: device_path.to_string(),
                auth: broker_connectors_enterprise::iotdb::IotDbAuth {
                    username: username.to_string(),
                    password: password.to_string(),
                },
                is_aligned,
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
                batch_size,
                batch_bytes,
                linger_ms,
                max_retries,
                initial_backoff_ms,
                max_backoff_ms,
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
                        .register(format!("iotdb:{}", name), sink.clone());
                    engine
                        .connectors()
                        .register(format!("iot_db:{}", name), sink);
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

            let (timestamp_field, tag_columns, field_columns) =
                crate::v5::action_sinks::datalayers_column_settings(body, name)?;
            let precision = crate::v5::action_sinks::datalayers_precision(
                body.get("precision")
                    .and_then(|v| v.as_str())
                    .map(str::to_string),
                &format!("connector {name:?} field `precision`"),
            )?;
            let config = broker_connectors_enterprise::datalayers::DatalayersConfig {
                endpoint,
                database,
                table,
                auth_token,
                username,
                password,
                timestamp_field,
                tag_columns,
                field_columns,
                precision,
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
            // The connector carries the service URL only. The topic,
            // key and strategy belong to the action, so a connector
            // alone registers no sink and invents no target.
            crate::v5::action_sinks::validate_connector_io_bounds(body, name, "connector config")?;
        }
        "rocketmq" => {
            // Lane A keeps the connector's own sink. Build it from the
            // operator's endpoint and topic. Fail closed when either is
            // absent: never invent a target.
            let Some(raw_endpoint) = body
                .get("endpoint")
                .or_else(|| body.get("endpoints"))
                .or_else(|| body.get("server"))
                .or_else(|| body.get("servers"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Ok(());
            };
            let endpoints: Vec<String> = raw_endpoint
                .split(',')
                .map(|part| {
                    part.trim()
                        .trim_start_matches("http://")
                        .trim_start_matches("tcp://")
                        .to_string()
                })
                .filter(|part| !part.is_empty())
                .collect();
            if endpoints.is_empty() {
                return Ok(());
            }
            // The topic is the connector-only target. Without it the
            // connector stays persist-only; never invent a topic.
            let Some(topic) = body
                .get("topic")
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Ok(());
            };
            let access_key = body
                .get("access_key")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let secret_key = body
                .get("secret_key")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let security_token = body
                .get("security_token")
                .or_else(|| body.get("securityToken"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());
            let config = broker_connectors_enterprise::rocketmq::RocketMqSinkConfig {
                endpoints,
                topic: topic.to_string(),
                tag_template: None,
                // The lane A connector form keys each row by its MQTT
                // client id, so the proxy keeps per-client ordering.
                keys_template: Some("${client_id}".to_string()),
                // Round-robin is the reference default dispatch.
                strategy: "roundrobin".to_string(),
                message_group_template: None,
                access_key,
                secret_key,
                security_token,
                // One row per send: the runtime owns batching (RO-01).
                batch_size: Some(1),
                buffer_capacity: Some(
                    broker_connectors_enterprise::rocketmq::DEFAULT_BUFFER_CAPACITY,
                ),
                // 10 ms lets one row wait for the runtime batch timer.
                linger_ms: Some(10),
                timeout_ms,
            };
            // The vendor server speaks the remoting protocol only.
            let transport: Option<
                Arc<dyn broker_connectors_enterprise::rocketmq::RocketMqTransport>,
            > = record_sink_error!(
                broker_connectors_enterprise::rocketmq::RemotingRocketMqTransport::new(&config)
            )
            .map(|built| Arc::new(built) as _);
            if let Some(transport) = transport {
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
            // The connector carries the endpoints and credentials
            // only. The topic, key, headers and strategy belong to the
            // action, so a connector alone registers no sink and
            // invents no target.
            for key in [
                "pool_size",
                "pipelining",
                "enable_pipelining",
                "max_inactive",
            ] {
                let present = body
                    .get(key)
                    .or_else(|| body.get("parameters").and_then(|params| params.get(key)));
                if present.is_some_and(|v| !v.is_null()) {
                    return Err(format!(
                        "connector {name:?} field `{key}` is not supported; remove it"
                    ));
                }
            }
        }
        #[cfg(not(feature = "confluent"))]
        "confluent" => {
            return Err(refuse_disabled_kind(conn_type, "confluent"));
        }
        "disk_log" | "disk" | "disklog" => {
            // A given directory is relative to the data directory and
            // must stay inside it, like the `filepath` form.
            let dir = match ["directory", "dir", "path"]
                .into_iter()
                .find_map(|key| body.get(key).and_then(|v| v.as_str()).map(|v| (key, v)))
            {
                Some((key, given)) => {
                    if !disk_log_path_contained(given, data_dir) {
                        return Err(format!(
                            "connector {name:?} field `{key}` must be a relative path inside the data directory"
                        ));
                    }
                    data_dir.join(given).to_string_lossy().into_owned()
                }
                None => data_dir.join("disk_logs").to_string_lossy().into_owned(),
            };
            let disk_number = |key: &str| -> Result<Option<u64>, String> {
                match body.get(key) {
                    None | Some(serde_json::Value::Null) => Ok(None),
                    Some(value) => value.as_u64().map(Some).ok_or_else(|| {
                        format!("connector {name:?} field `{key}` must be a non-negative integer")
                    }),
                }
            };
            let disk_text = |key: &str| -> Result<Option<&str>, String> {
                match body.get(key) {
                    None | Some(serde_json::Value::Null) => Ok(None),
                    Some(value) => value.as_str().map(Some).ok_or_else(|| {
                        format!("connector {name:?} field `{key}` must be a string")
                    }),
                }
            };
            let format = match disk_text("format")? {
                None | Some("ndjson") => broker_connectors::disk_log::DiskLogFormat::Ndjson,
                Some("raw") => broker_connectors::disk_log::DiskLogFormat::Raw,
                Some("csv") => broker_connectors::disk_log::DiskLogFormat::Csv,
                Some(other) => {
                    let wanted = "ndjson, raw or csv";
                    return Err(format!(
                        "connector {name:?} field `format` must be {wanted}: {other:?}"
                    ));
                }
            };
            let compression = match disk_text("compression")? {
                None | Some("none") => broker_connectors::disk_log::DiskLogCompression::None,
                Some("gzip") => broker_connectors::disk_log::DiskLogCompression::Gzip,
                Some(other) => {
                    return Err(format!(
                        "connector {name:?} field `compression` value {other:?} is not none or gzip"
                    ))
                }
            };
            let sync_mode = match disk_text("sync_mode")? {
                None | Some("every_batch") => broker_connectors::disk_log::DiskSyncMode::EveryBatch,
                Some("os_default") => broker_connectors::disk_log::DiskSyncMode::OsDefault,
                Some(other) => {
                    return Err(format!(
                        "connector {name:?} field `sync_mode` value {other:?} is not every_batch or os_default"
                    ))
                }
            };
            let max_file_size_bytes = disk_number("max_file_size_bytes")?.or(Some(67_108_864));
            let max_file_age_secs = disk_number("max_file_age_secs")?;
            let max_backup_files = match disk_number("max_backup_files")? {
                Some(count) => Some(usize::try_from(count).map_err(|_| {
                    format!("connector {name:?} field `max_backup_files` is too large")
                })?),
                None => Some(10),
            };
            let max_retention_days = match disk_number("max_retention_days")? {
                Some(days) => Some(u32::try_from(days).map_err(|_| {
                    format!("connector {name:?} field `max_retention_days` is too large")
                })?),
                None => None,
            };
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
                directory: dir,
                filename_prefix: prefix.to_string(),
                filename_extension: ext.to_string(),
                format,
                max_file_size_bytes,
                max_file_age_secs,
                compression,
                max_backup_files,
                max_retention_days,
                sync_mode,
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
            // No invented endpoint or node: a missing target stays
            // persist-only with no live sink. The v5 action owns the
            // node id; the connector only holds the server.
            let Some(raw_endpoint) = body
                .get("endpoint_url")
                .or_else(|| body.get("server"))
                .or_else(|| body.get("url"))
                .or_else(|| body.get("endpoint"))
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
            else {
                return Ok(());
            };
            let endpoint_url = if raw_endpoint.starts_with("opc.tcp://") {
                raw_endpoint.to_string()
            } else {
                format!("opc.tcp://{}", raw_endpoint.trim_start_matches("tcp://"))
            };
            let timeout_ms = body
                .get("timeout_ms")
                .or_else(|| body.get("timeout"))
                .or_else(|| body.get("request_timeout_ms"))
                .and_then(|v| v.as_u64());

            // TODO(parity): the connector alone carries no node id. Which
            // subscription should a connector-only row register? The
            // current choice registers nothing and leaves the row
            // persist-only; the v5 action owns the node id.
            let _ = (endpoint_url, timeout_ms);
            return Ok(());
        }
        "sparkplug_b" | "sparkplug" | "spb" => {
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
            let Some(endpoint) = body
                .get("endpoint")
                .or_else(|| body.get("url"))
                .or_else(|| body.get("server"))
                .or_else(|| body.get("host"))
                .and_then(|v| v.as_str())
                .map(|value| {
                    if value.starts_with("http://") || value.starts_with("https://") {
                        value.to_string()
                    } else if value.contains(':') {
                        format!("http://{value}")
                    } else {
                        let port = body
                            .get("port")
                            .and_then(|port| port.as_u64())
                            .unwrap_or(443);
                        format!("https://{value}:{port}")
                    }
                })
            else {
                return Ok(());
            };
            // The bucket and key are action targets. Without them this
            // connector has no legacy target, so it registers no sink.
            let Some(bucket) = body.get("bucket").and_then(|v| v.as_str()) else {
                return Ok(());
            };
            let Some(key_template) = body.get("key_template").and_then(|v| v.as_str()) else {
                return Ok(());
            };
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
            // The table and keys are action targets. Without them this
            // connector has no legacy target, so it registers no sink.
            let Some(table) = body
                .get("table_name")
                .or_else(|| body.get("table"))
                .and_then(|v| v.as_str())
            else {
                return Ok(());
            };
            let region = body
                .get("region")
                .and_then(|v| v.as_str())
                .unwrap_or("us-east-1");
            let endpoint = body
                .get("endpoint")
                .or_else(|| body.get("url"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let Some(access_key) = body
                .get("access_key_id")
                .and_then(|v| v.as_str())
                .filter(|value| !value.trim().is_empty())
            else {
                return Ok(());
            };
            let Some(secret_key) = body
                .get("secret_access_key")
                .and_then(|v| v.as_str())
                .filter(|value| !value.trim().is_empty())
            else {
                return Ok(());
            };
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
                undefined_vars_as_null: false,
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
            // The connector `timeout_ms` bounds every request of the client.
            let client = reqwest::Client::builder()
                .timeout(config.timeout())
                .build()
                .unwrap_or_default();
            if let Some(transport) = record_sink_error!(
                broker_connectors_enterprise::timestream::HttpTimestreamTransport::new(
                    &config, client,
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
            // The reference connector is the Postgres wire form. These
            // Data API fields are not reference fields and no sink
            // applies them, so they are refused.
            for key in [
                "cluster_identifier",
                "session_token",
                "db_user",
                "sql_template",
            ] {
                let present = body
                    .get(key)
                    .or_else(|| body.get("parameters").and_then(|params| params.get(key)));
                if present.is_some_and(|v| !v.is_null()) {
                    return Err(format!(
                        "connector {name:?} field `{key}` is not supported; remove it"
                    ));
                }
            }
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
            let ca_cert_pem = field(&[
                "ca_cert_pem",
                "ca_cert",
                "ca_pem",
                "certificate_authority",
                "ca",
                "ca_bundle_pem",
                "ca_bundle",
            ])
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
            let Some(account_name) = body.get("account_name").and_then(|v| v.as_str()) else {
                return Ok(());
            };
            // The container and blob are action targets. Without them
            // this connector has no legacy target, so it registers no sink.
            let Some(container_name) = body
                .get("container_name")
                .or_else(|| body.get("container"))
                .and_then(|v| v.as_str())
            else {
                return Ok(());
            };
            let endpoint = body
                .get("endpoint")
                .or_else(|| body.get("url"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let Some(blob_path) = body
                .get("blob_path_template")
                .or_else(|| body.get("path_template"))
                .and_then(|v| v.as_str())
            else {
                return Ok(());
            };
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
        "azure_eventhubs"
        | "azure_event_hubs"
        | "azure"
        | "eventhubs"
        | "azure_event_hub_producer" => {
            // Feature disabled (X1-09): fail closed with no sink; the
            // checked wrapper names the missing feature.
            return Err(refuse_disabled_kind(conn_type, "azure_eventhubs"));
        }
        #[cfg(feature = "azure_eventhubs")]
        "azure_eventhubs"
        | "azure_event_hubs"
        | "azure"
        | "eventhubs"
        | "azure_event_hub_producer" => {
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
        "gcp_pubsub" | "gcp" | "pubsub" | "gcp_pubsub_producer" | "gcp_pubsub_consumer" => {
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
        "gcp_pubsub" | "gcp" | "pubsub" | "gcp_pubsub_producer" | "gcp_pubsub_consumer" => {
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
            let Some(project_id) = body
                .get("project_id")
                .or_else(|| body.get("project"))
                .or_else(|| {
                    body.get("authentication")
                        .or_else(|| body.get("auth"))
                        .and_then(|auth| auth.get("gcp_project_id"))
                })
                .and_then(|v| v.as_str())
            else {
                return Ok(());
            };
            // Dataset and table are action targets. Without them this
            // connector has no legacy target, so it registers no sink.
            let Some(dataset_id) = body
                .get("dataset_id")
                .or_else(|| body.get("dataset"))
                .and_then(|v| v.as_str())
            else {
                return Ok(());
            };
            let Some(table) = body
                .get("table")
                .or_else(|| body.get("table_template"))
                .and_then(|v| v.as_str())
            else {
                return Ok(());
            };
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

            // Read the credential from `authentication` or `auth` and
            // apply it. Never fall back to `GcpAuth::None` when a
            // credential is present.
            let (_, auth) = crate::v5::action_sinks::bigquery_auth(body, name, name)?;
            let config = broker_connectors_enterprise::bigquery::BigQuerySinkConfig {
                project_id: project_id.to_string(),
                dataset_id: dataset_id.to_string(),
                table_template: table.to_string(),
                endpoint,
                auth,
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
            // The action owns the measurement. The legacy connector
            // registers no live sink without one. Fail closed.
            let Some(measurement) = body
                .get("measurement")
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            else {
                return Ok(());
            };

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
                measurement: measurement.to_string(),
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

/// Render a duplicate connector create: HTTP 400 with the documented
/// `ALREADY_EXISTS` code.
fn connector_already_exists(message: String) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({
            "code": "ALREADY_EXISTS",
            "message": message,
        })),
    )
        .into_response()
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
    let _sink_guard = state.action_sink_lock.lock().await;
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
    // Fail closed (S1-01): a connector without its required credential
    // or identity names that field first, even when the kind is not
    // ready yet. A credential-complete body whose kind is not ready is
    // refused with ALREADY_EXISTS after alias mapping, below.
    apply_cloud_reference_fields(conn_type.as_str(), &mut body);
    let missing = missing_connector_field(conn_type.as_str(), &body, &state.connectors.data_dir());
    if missing.is_none() {
        // Refuse a pending kind before alias mapping: an alias of a
        // ready kind must not hide a pending one.
        if let Some(message) = kind_refusal(conn_type.as_str()) {
            raise_unavailable_kind_alarm(&state, conn_type.as_str());
            if is_mvp_pending_iot_kind(conn_type.as_str()) {
                return ApiError::BadRequest(message).into_response();
            }
            return ApiError::AlreadyExists(message).into_response();
        }
    }
    // The reference names its HTTP connector `http`. The tree runs
    // that connector in the `webhook` arm. Canonicalize for the runtime
    // and the readiness gate. The stored document keeps the documented
    // type through `normalize_connector_document`.
    let conn_type = canonical_connector_kind(conn_type.as_str()).to_string();
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
    // duplicated id. The documented code is `ALREADY_EXISTS` and the
    // status is `BAD_REQUEST`, as the error catalogue documents.
    // This early read-check also skips the wasted live-sink
    // registration below; the write-locked re-check before the push
    // closes the check-then-insert race for the in-memory store.
    if state.connectors.contains(&name) {
        return connector_already_exists(format!("connector {name:?} already exists"));
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
    if let Some(missing) = missing {
        let message = format!("missing required field: {missing} for connector type {conn_type}");
        // The three MVP-pending IoT kinds answer the documented
        // `BAD_REQUEST` code for every create, before anything stores.
        if is_mvp_pending_iot_kind(&conn_type_early) {
            raise_unavailable_kind_alarm(&state, &conn_type_early);
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "code": "BAD_REQUEST",
                    "message": message,
                })),
            )
                .into_response();
        }
        // `POST /connectors` documents one 400 code, `ALREADY_EXISTS`.
        // The http, device, object, file and cloud kinds answer with that
        // code so the refusal body validates. Other kinds keep
        // `BAD_REQUEST`.
        if matches!(
            crate::v5::action_sinks::canonical_action_kind(&conn_type_early),
            "s3" | "azure_blob"
                | "dynamodb"
                | "bigquery"
                | "disk_log"
                | "snowflake"
                | "tablestore"
                | "databricks"
                | "http"
                | "opc_ua"
                | "sparkplug_b"
                | "kinesis"
                | "gcp_pubsub"
                | "azure_eventhubs"
                | "gcp_iot"
                | "kafka"
                | "confluent"
                | "rabbitmq"
                | "redis"
                | "mqtt_bridge"
                | "mqtt"
                | "pulsar"
                | "rocketmq"
        ) {
            return ApiError::AlreadyExists(message).into_response();
        }
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "BAD_REQUEST",
                "message": message,
            })),
        )
            .into_response();
    }
    // A RocketMQ topic is a bare name. The connector-only form keeps no
    // sink under lane B, so validate the stored target here instead.
    if crate::v5::action_sinks::canonical_action_kind(&conn_type) == "rocketmq" {
        if let Some(topic) = body.get("topic").and_then(|value| value.as_str()) {
            if topic.trim().is_empty() || topic.chars().any(char::is_whitespace) {
                return ApiError::BadRequest(format!(
                    "connector {name:?} field `topic` must be a bare topic name (connector create)"
                ))
                .into_response();
            }
        }
    }
    // The official document-store drivers own their pools and TLS. An
    // explicit pool, pipelining or `ssl` value is refused at create,
    // not stored and ignored.
    if matches!(
        crate::v5::action_sinks::canonical_action_kind(&conn_type),
        "mongodb"
            | "couchbase"
            | "elasticsearch"
            | "kafka"
            | "rabbitmq"
            | "redis"
            | "pulsar"
            | "rocketmq"
    ) {
        if let Err(message) =
            crate::v5::action_sinks::validate_connector_io_bounds(&body, &name, "connector create")
        {
            return ApiError::BadRequest(message).into_response();
        }
    }
    // Confluent applies `ssl.enable` to the driver protocol, so the
    // shared refusal above does not fit; only the pool knobs are
    // refused here.
    if matches!(
        crate::v5::action_sinks::canonical_action_kind(&conn_type),
        "confluent" | "confluent_producer"
    ) {
        for key in [
            "pool_size",
            "pipelining",
            "enable_pipelining",
            "max_inactive",
        ] {
            let present = body
                .get(key)
                .or_else(|| body.get("parameters").and_then(|params| params.get(key)));
            if present.is_some_and(|v| !v.is_null()) {
                return ApiError::BadRequest(format!(
                    "connector {name:?} field `{key}` is not supported; remove it (connector create)"
                ))
                .into_response();
            }
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

    // Build the connector's live sink into a staged generation. The
    // staged keys are invisible to delivery until the store commits, so
    // a persist failure leaves the live map as it was.
    state.engine.connectors().begin_stage();
    // Delivery dials its target lazily at send time, so registration
    // never depends on a point-in-time reachability probe. A kind with
    // no execution path registers nothing, so it honestly reads back
    // `disconnected` (stored config first, no claimed execution).
    if let Err(register_error) = try_register_live_sink(
        &state.engine,
        &conn_type,
        &name,
        &body,
        &state.connectors.data_dir(),
    )
    .await
    {
        state.engine.connectors().abort_stage();
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "BAD_REQUEST",
                "message": register_error,
            })),
        )
            .into_response();
    }
    if let Some(obj) = body.as_object_mut() {
        obj.insert("id".to_string(), serde_json::Value::String(name.clone()));
        obj.insert("name".to_string(), serde_json::Value::String(name.clone()));
        obj.insert(
            "type".to_string(),
            serde_json::Value::String(conn_type.clone()),
        );
        if !obj.contains_key("enable") {
            obj.insert("enable".to_string(), serde_json::Value::Bool(true));
        }
    }
    normalize_connector_document(&conn_type_early, &mut body);

    // Re-check under the write lock so two concurrent creates for the
    // same id cannot both pass the early check and push a duplicate pair.
    match state.connectors.try_insert(&name, body.clone()) {
        Ok(()) => {}
        Err(ConnectorInsertError::Duplicate) => {
            state.engine.connectors().abort_stage();
            return connector_already_exists(format!("connector {name:?} already exists"));
        }
        Err(ConnectorInsertError::Full) => {
            state.engine.connectors().abort_stage();
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

    if let Err(error) = state.connectors.persist() {
        // The store and the live map stay as they were: drop the staged
        // sinks and remove the uncommitted row.
        state.engine.connectors().abort_stage();
        state.connectors.remove(&name);
        state.engine.remove_action(&name);
        return connector_persist_error(error);
    }
    // Publish the generation, then drain any sink it replaced.
    for old in state.engine.connectors().commit_stage() {
        let _ = old.flush().await;
    }
    // The new connector can resolve actions that stored while dangling
    // (dangling references keep the action storable): rebuild only the
    // actions of this connector so a stored action starts delivering
    // the moment its target arrives. An unrelated action update is never
    // overwritten by a stale full replay.
    let replaced = crate::v5::action_sinks::rebuild_connector_action_sinks(
        &state.engine,
        &state.connectors,
        &state.actions,
        &state.alarms,
        &name,
    );
    for old in replaced {
        let _ = old.flush().await;
    }
    // Probe the new connector and every action it wired before the write
    // answers, so a down target reads `disconnected` from a real attempt.
    if let Some(sink) = state.engine.connectors().get(&name) {
        let _ = sink.probe().await;
    }
    crate::v5::action_sinks::probe_action_sinks(&state.engine, &state.actions, |record| {
        crate::v5::action_sinks::action_targets_connector(&state.connectors, record, &name)
    })
    .await;
    let body = redact_connector_doc(&connector_live_view(&body, &state.engine));

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
    let _sink_guard = state.action_sink_lock.lock().await;
    // A refused kind answers 400 before the existence check.
    // Reason: fail closed for refused kinds.
    let raw = body
        .as_object()
        .and_then(|obj| obj.get("type").and_then(|v| v.as_str()))
        .unwrap_or("")
        .trim()
        .to_string();
    if raw.is_empty() {
        return ApiError::BadRequest("missing required field: type".to_string()).into_response();
    }
    if let Some(message) = kind_refusal(raw.as_str()) {
        raise_unavailable_kind_alarm(&state, raw.as_str());
        return ApiError::BadRequest(message).into_response();
    }
    // Existence before validation: an unknown id is 404 and creates
    // nothing, even when the body is also invalid.
    let Some(stored) = state.connectors.get(&id) else {
        return ApiError::NotFound(format!("connector {id:?} not found")).into_response();
    };
    // Body validation after existence check.
    let canonical = canonical_connector_kind(raw.as_str()).to_string();
    if let Some(message) = crate::removed_connector_refusal(&canonical) {
        return ApiError::BadRequest(message.to_string()).into_response();
    }
    if !broker_config::is_known_connector_type(&canonical) {
        return ApiError::BadRequest(format!("unknown connector type: {canonical}"))
            .into_response();
    }
    if let Some(message) = kind_refusal(&canonical) {
        return ApiError::BadRequest(message).into_response();
    }
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
    // Refuse a pending kind before alias mapping.
    if let Some(message) = kind_refusal(conn_type.as_str()) {
        return ApiError::BadRequest(message).into_response();
    }
    let stored_type = conn_type.clone();
    // The reference names its HTTP connector `http`. The tree runs
    // that connector in the `webhook` arm. Canonicalize for the runtime
    // and the readiness gate. The stored document keeps the documented
    // type through `normalize_connector_document`.
    let conn_type = canonical_connector_kind(conn_type.as_str()).to_string();
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
    apply_cloud_reference_fields(&conn_type, &mut body);
    broker_config::secrets::restore_masked_secrets(&mut body, &stored);
    if let Some(missing) = missing_connector_field(&conn_type, &body, &state.connectors.data_dir())
    {
        return ApiError::BadRequest(format!(
            "missing required field: {missing} for connector type {conn_type}"
        ))
        .into_response();
    }
    // A RocketMQ topic is a bare name. The connector-only form keeps no
    // sink under lane B, so validate the stored target here instead.
    if crate::v5::action_sinks::canonical_action_kind(&conn_type) == "rocketmq" {
        if let Some(topic) = body.get("topic").and_then(|value| value.as_str()) {
            if topic.trim().is_empty() || topic.chars().any(char::is_whitespace) {
                return ApiError::BadRequest(format!(
                    "connector {bare:?} field `topic` must be a bare topic name (connector update)"
                ))
                .into_response();
            }
        }
    }
    // The reference password form for PostgreSQL is resolved at create;
    // an update keeps the old URL form and refuses the reference. Name
    // the password field; do not invent a port error.
    if crate::v5::action_sinks::canonical_action_kind(&conn_type) == "pgsql" {
        let raw_password = body
            .get("password")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        if broker_config::secrets::is_secret_ref(raw_password) {
            return ApiError::BadRequest(format!(
                "connector {bare:?} field `password` cannot be a secret reference on update"
            ))
            .into_response();
        }
    }
    // The official document-store drivers own their pools and TLS. An
    // explicit pool, pipelining or `ssl` value is refused at update.
    if matches!(
        crate::v5::action_sinks::canonical_action_kind(&conn_type),
        "mongodb"
            | "couchbase"
            | "elasticsearch"
            | "kafka"
            | "rabbitmq"
            | "redis"
            | "pulsar"
            | "rocketmq"
    ) {
        if let Err(message) =
            crate::v5::action_sinks::validate_connector_io_bounds(&body, &bare, "connector update")
        {
            return ApiError::BadRequest(message).into_response();
        }
    }
    if matches!(
        crate::v5::action_sinks::canonical_action_kind(&conn_type),
        "confluent" | "confluent_producer"
    ) {
        for key in [
            "pool_size",
            "pipelining",
            "enable_pipelining",
            "max_inactive",
        ] {
            let present = body
                .get(key)
                .or_else(|| body.get("parameters").and_then(|params| params.get(key)));
            if present.is_some_and(|v| !v.is_null()) {
                return ApiError::BadRequest(format!(
                    "connector {bare:?} field `{key}` is not supported; remove it (connector update)"
                ))
                .into_response();
            }
        }
    }
    // MV-01 (R2-04): build the whole new generation before anything
    // swaps in. The connector's own sinks and every dependent action's
    // owned sink go into one staged map. A failure answers 400 naming
    // the action and the field, keeps every old sink (the old
    // generation keeps delivering) and leaves the stored row untouched.
    // Every dependent action must accept the new connector settings
    // before anything swaps in; each action's owned sink is built here
    // staged (built, never published): on success the staged sinks swap
    // in under the action dispatch keys below, after the stored update
    // and its persist.
    state.engine.connectors().begin_stage();
    if let Err(register_error) = try_register_live_sink(
        &state.engine,
        &conn_type,
        &bare,
        &body,
        &state.connectors.data_dir(),
    )
    .await
    {
        state.engine.connectors().abort_stage();
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "BAD_REQUEST",
                "message": register_error,
            })),
        )
            .into_response();
    }
    // MV-01 (R2-04): every dependent action must accept the new
    // connector settings before anything swaps in. Each action's owned
    // sink is built here staged (built, never registered): a failure
    // answers 400 naming the action and the field, keeps every old
    // sink (the old generation keeps delivering) and leaves the stored
    // row untouched. On success the staged sinks swap in under the
    // action dispatch keys below, after the stored update and its
    // persist.
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
                state.engine.connectors().abort_stage();
                return ApiError::BadRequest(format!(
                    "action {:?} cannot use connector {bare:?}: {message}",
                    record.id
                ))
                .into_response();
            }
        };
        match crate::v5::action_sinks::build_action_sink_with_dir(
            &record,
            &body,
            &opts,
            state.config.dir(),
        ) {
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
                state.engine.connectors().register(key, sink);
                staged_ids.push(record.id.clone());
            }
            Ok(None) => {
                staged_ids.push(record.id.clone());
            }
            Err(message) => {
                state.engine.connectors().abort_stage();
                return ApiError::BadRequest(format!(
                    "action {:?} cannot use connector {bare:?}: {message}",
                    record.id
                ))
                .into_response();
            }
        }
    }
    let enable = body.get("enable").and_then(|v| v.as_bool()).unwrap_or(true);
    let old_enable = stored
        .get("enable")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let old_connector_sinks: Vec<(String, Arc<dyn broker_connectors::Sink>)> = state
        .engine
        .connectors()
        .ids()
        .into_iter()
        .filter(|registered| {
            registered == &bare
                || (registered.ends_with(&format!(":{bare}"))
                    && !broker_config::is_v5_action_sink_key(registered))
        })
        .filter_map(|registered| {
            state
                .engine
                .connectors()
                .get(&registered)
                .map(|sink| (registered, sink))
        })
        .collect();
    if let Some(obj) = body.as_object_mut() {
        obj.insert("id".to_string(), serde_json::Value::String(bare.clone()));
        obj.insert("name".to_string(), serde_json::Value::String(bare.clone()));
        obj.insert(
            "type".to_string(),
            serde_json::Value::String(conn_type.clone()),
        );
        obj.insert("enable".to_string(), serde_json::Value::Bool(enable));
    }
    normalize_connector_document(&stored_type, &mut body);
    if !state.connectors.replace(&bare, body.clone()) {
        state.engine.connectors().abort_stage();
        return ApiError::NotFound(format!("connector {id:?} not found")).into_response();
    }
    // W3-19: mirror the flag into the dispatch snapshot (same lock-free
    // path as the enable route below).
    state.engine.set_action_enabled(&bare, enable);
    // MV-01/R2: the replace can flip the enable flag along with the
    // settings; the dependent actions' delivery follows it.
    mirror_dependent_actions(&state, &bare);
    if let Err(error) = state.connectors.persist() {
        // Nothing swapped: drop the staged generation, restore the
        // stored row and the flags, and keep every old sink delivering.
        state.engine.connectors().abort_stage();
        state.connectors.replace(&bare, stored.clone());
        state.engine.set_action_enabled(&bare, old_enable);
        mirror_dependent_actions(&state, &bare);
        return connector_persist_error(error);
    }
    // Publish the new generation in one step (staged build, persist,
    // swap, in that order). Drain the waiting rows of each replaced
    // sink with no loss. A
    // registration that the new
    // kind no longer claims is removed too, so a kind change leaves
    // no stale alias.
    // The actions that staged nothing keep their state (they never
    // owned a sink for this connector's new shape).
    let mut replaced = state.engine.connectors().commit_stage();
    for (registered, old) in old_connector_sinks {
        if state
            .engine
            .connectors()
            .get(&registered)
            .is_some_and(|current| Arc::ptr_eq(&current, &old))
        {
            state.engine.connectors().unregister(&registered);
            replaced.push(old);
        }
    }
    for old in replaced {
        let _ = old.flush().await;
    }
    for action_id in &staged_ids {
        state
            .actions
            .set_wiring(action_id, crate::v5::actions::ActionWiring::Live);
        state
            .alarms
            .deactivate(&format!("action_build_failed:{action_id}"));
    }
    // Probe the updated connector and every dependent action before the
    // write answers, so a down target reads `disconnected` from a real
    // attempt.
    if let Some(sink) = state.engine.connectors().get(&bare) {
        let _ = sink.probe().await;
    }
    crate::v5::action_sinks::probe_action_sinks(&state.engine, &state.actions, |record| {
        crate::v5::action_sinks::action_targets_connector(&state.connectors, record, &bare)
    })
    .await;
    let body = redact_connector_doc(&connector_live_view(&body, &state.engine));
    (StatusCode::OK, Json(body)).into_response()
}

pub async fn delete_connector(State(state): State<ApiState>, Path(id): Path<String>) -> Response {
    let _sink_guard = state.action_sink_lock.lock().await;
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
    let mut removed_sinks = Vec::new();
    for registered in state.engine.connectors().ids() {
        if registered == bare
            || (registered.ends_with(&format!(":{bare}"))
                && !broker_config::is_v5_action_sink_key(&registered))
        {
            if let Some(sink) = state.engine.connectors().get(&registered) {
                removed_sinks.push(sink);
            }
            state.engine.connectors().unregister(&registered);
        }
    }
    for sink in removed_sinks {
        let _ = sink.flush().await;
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
/// dispatch flag rides `action.enable && every referenced connector
/// enable`. Actions that target other connectors are untouched. The
/// store already holds the new flag at each call.
fn mirror_dependent_actions(state: &ApiState, bare: &str) {
    for record in state.actions.snapshot() {
        if crate::v5::action_sinks::action_targets_connector(&state.connectors, &record, bare) {
            state.engine.set_action_enabled(
                &broker_config::v5_action_sink_key(&record.id),
                record.enable
                    && crate::v5::action_sinks::action_connector_enable(&state.connectors, &record),
            );
        }
    }
}

pub async fn set_connector_enabled(
    State(state): State<ApiState>,
    Path((id, enable_raw)): Path<(String, String)>,
) -> Response {
    let _sink_guard = state.action_sink_lock.lock().await;
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
    mirror_dependent_actions(&state, &bare);
    if !enable {
        flush_dependent_actions(&state, &bare).await;
    }
    if let Err(error) = state.connectors.persist() {
        return connector_persist_error(error);
    }
    StatusCode::NO_CONTENT.into_response()
}

/// `POST /api/v5/connectors/:id/:operation`: run one named lifecycle
/// operation against one connector (W3-20, C07-07, MV-06b).
///
/// Only the operations the lifecycle supports are served: `start`
/// (enable the connector), `stop` (disable it) and `restart` (run a
/// fresh connection test, then enable it). All three keep the stored
/// settings: they flip only the enable flag in place, mirror the flag
/// into the engine's lock-free dispatch snapshot (disabled forwards are
/// skipped at fire time before reaching `ConnectorManager::send`,
/// shared with the W3-19 enable route) and persist through the config
/// core like every other connector write. `restart` then probes the
/// connector sink, so the next read reports the fresh connection test.
/// Every operation answers 204 with no body, as the reference API
/// documents for `start`.
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
// TODO(parity): the documented spec names only `start` in the operation
// enum and answers an unknown operation with 404, while this route also
// serves `stop`/`restart` and answers an unknown operation with
// `BAD_REQUEST`. The rulebook does not decide the verb set; the MV-06b
// binding requires `start`, `stop` and `restart`, so the current choice
// keeps all three and the `BAD_REQUEST` refusal until the checker pins
// the shape.
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
/// with no body. `restart` enables like `start`, then probes the
/// connector sink so the next read reports the fresh connection test.
/// Management-plane only; see the caller for the
/// locking and fire-path notes.
async fn apply_connector_operation(state: &ApiState, id: &str, operation: &str) -> Response {
    let _sink_guard = state.action_sink_lock.lock().await;
    // The path id accepts the bare id or the `{type}:{name}` key.
    // Existence before validation: an unknown id is 404 and persists
    // nothing, even when the operation is also unknown.
    let Some(stored) = state.connectors.get(id) else {
        return ApiError::NotFound(format!("connector {id:?} not found")).into_response();
    };
    let bare = connector_bare_name(&stored);
    let enable = match operation {
        "start" | "restart" => true,
        "stop" => false,
        _ => {
            return ApiError::BadRequest(format!(
                "unknown connector operation {operation:?}: expected \"start\", \"stop\" or \"restart\" (field `operation`)"
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
    mirror_dependent_actions(state, &bare);
    if !enable {
        flush_dependent_actions(state, &bare).await;
    }
    if let Err(error) = state.connectors.persist() {
        return connector_persist_error(error);
    }
    // MV-06b: a restart runs a fresh connection test after the enable,
    // so the next read reports the new probe result from the sink state.
    if operation == "restart" {
        if let Some(sink) = state.engine.connectors().get(&bare) {
            let _ = sink.probe().await;
        }
    }
    StatusCode::NO_CONTENT.into_response()
}

/// Drain every dependent action runtime after its connector stops. The
/// flags already stop new fires; the buffered rows still reach the old
/// target.
async fn flush_dependent_actions(state: &ApiState, bare: &str) {
    for record in state.actions.snapshot() {
        if crate::v5::action_sinks::action_targets_connector(&state.connectors, &record, bare) {
            let key = broker_config::v5_action_sink_key(&record.id);
            if let Some(sink) = state.engine.connectors().get(&key) {
                let _ = sink.flush().await;
            }
        }
    }
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
    // Read the MQTT target first. Sparkplug uses `mqtt_url` or
    // `broker_address`. Parse the host and port from the value.
    if let Some(s) = body
        .get("mqtt_url")
        .or_else(|| body.get("broker_address"))
        .and_then(|v| v.as_str())
    {
        let trimmed = s.trim();
        if !trimmed.is_empty() && !broker_config::secrets::is_secret_ref(trimmed) {
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
                if host_port.contains(':') {
                    return Some(host_port.to_string());
                }
                // Default MQTT port. Reason: IANA assigns 1883 to MQTT.
                return Some(format!("{host_port}:1883"));
            }
        }
    }
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
    // IoT and messaging hubs name the target outside `host`. The probe
    // dials the same closed-port value that delivery uses.
    for key in ["iot_hub_name", "hub_name", "hub", "namespace"] {
        if let Some(s) = body.get(key).and_then(|v| v.as_str()) {
            let mut clean = s.trim();
            if clean.is_empty() {
                continue;
            }
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
/// True when a probe without a network target must fail instead of
/// answering success. Every kind whose delivery dials a configured
/// target is listed: a probe with no target names the missing field
/// instead of reporting a success it never measured.
fn probe_kind_needs_target(kind: &str) -> bool {
    matches!(
        crate::v5::action_sinks::canonical_action_kind(kind),
        "mongodb"
            | "couchbase"
            | "elasticsearch"
            | "opc_ua"
            | "sparkplug_b"
            | "kinesis"
            | "gcp_pubsub"
            | "azure_eventhubs"
            | "aws_iot"
            | "azure_iot"
            | "oci_streaming"
            | "kafka"
            | "confluent"
            | "rabbitmq"
            | "pulsar"
            | "rocketmq"
            | "mqtt_bridge"
            | "redis"
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
        "opc_ua" => {
            if !present(body, &["endpoint_url", "server", "url", "endpoint"]) {
                return Some("endpoint_url");
            }
            None
        }
        "sparkplug_b" => {
            if !present(
                body,
                &["mqtt_url", "broker_address", "server", "endpoint", "url"],
            ) {
                return Some("mqtt_url");
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
        if probe_kind_needs_target(&kind) {
            probe_string(
                &body,
                &[
                    "mqtt_url",
                    "broker_address",
                    "server",
                    "servers",
                    "endpoint",
                    "endpoint_url",
                    "url",
                    "connection_string",
                    "iot_hub_name",
                    "hub_name",
                    "hub",
                    "namespace",
                ],
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
    } else if probe_kind_needs_target(&kind) {
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
pub async fn probe_action(
    State(state): State<ApiState>,
    body: Option<Json<serde_json::Value>>,
) -> Response {
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
    // Fail closed: the probe tests the action's delivery path. A named
    // connector that resolves to nothing has no path to test.
    if let Some(reference) = body
        .get("connector")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|reference| !reference.is_empty())
    {
        if resolve_action_connector(&state.connectors, reference).is_none() {
            return probe_failed(format!(
                "connector {reference:?} of the action not found (field `connector`)"
            ));
        }
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
        if probe_kind_needs_target(&kind) {
            probe_string(
                &body,
                &[
                    "mqtt_url",
                    "broker_address",
                    "server",
                    "servers",
                    "endpoint",
                    "endpoint_url",
                    "url",
                    "connection_string",
                    "iot_hub_name",
                    "hub_name",
                    "hub",
                    "namespace",
                ],
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
    } else if probe_kind_needs_target(&kind) {
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
        let err = try_register_live_sink(
            &engine,
            kind,
            "feat-test",
            &serde_json::json!({}),
            std::path::Path::new("./data"),
        )
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
            std::path::Path::new("./data"),
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
            undefined_vars_as_null: false,
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

    #[tokio::test]
    async fn disk_log_connector_applies_format_rotation_and_contains_paths() {
        use broker_connectors::Sink as _;
        let data = std::env::temp_dir().join(format!(
            "disk-log-connector-{}-{}",
            std::process::id(),
            now_nanos_for_test()
        ));
        let outside = data
            .join("..")
            .join(format!("outside-{}", now_nanos_for_test()));
        std::fs::create_dir_all(&data).expect("data dir");
        std::fs::create_dir_all(&outside).expect("outside dir");
        std::os::unix::fs::symlink(&outside, data.join("link")).expect("symlink");
        let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
        let body = |directory: &str| {
            serde_json::json!({
                "directory": directory, "filename_prefix": "sel", "filename_extension": "ent",
                "format": "raw", "max_file_size_bytes": 16, "compression": "gzip",
                "max_backup_files": 4, "sync_mode": "every_batch"
            })
        };
        // A directory outside the data directory is refused with the field named.
        for bad in [
            outside.to_string_lossy().into_owned(),
            "../x".to_string(),
            "link".to_string(),
        ] {
            let error = register_live_sink(&engine, "disk_log", "d", &body(&bad), &data)
                .await
                .expect_err("outside directory is refused");
            assert!(error.contains("directory"), "{error}");
        }
        assert_eq!(std::fs::read_dir(&outside).expect("read").count(), 0);
        // A bad setting is refused with the field named.
        let mut bad_format = body("inside");
        bad_format["format"] = serde_json::json!("xml");
        let error = register_live_sink(&engine, "disk_log", "d", &bad_format, &data)
            .await
            .expect_err("unknown format is refused");
        assert!(error.contains("format"), "{error}");
        // A directory inside the data directory takes the format and the size limit.
        register_live_sink(&engine, "disk_log", "d", &body("inside"), &data)
            .await
            .expect("inside directory registers");
        let sink = engine.connectors().get("d").expect("sink registered");
        for value in 0..3 {
            let payload = Bytes::from(format!("{{\"v\":{value}}}"));
            sink.send(&Topic::new("t/x").unwrap(), &payload, QoS::AtMostOnce)
                .await
                .expect("send");
        }
        let active = std::fs::read(data.join("inside").join("sel.ent")).expect("active file");
        assert_eq!(
            active, b"{\"v\":2}\n",
            "raw format, one record after rotation"
        );
        let rotated = std::fs::read_dir(data.join("inside"))
            .expect("read dir")
            .filter_map(|entry| entry.ok())
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".gz"))
            .count();
        assert_eq!(rotated, 1, "the size limit rotated one gzip segment");
        let _ = std::fs::remove_dir_all(&data);
        let _ = std::fs::remove_dir_all(&outside);
    }

    fn now_nanos_for_test() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or(0)
    }
}
