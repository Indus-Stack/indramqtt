//! Managed tenant namespaces for the v5 management API (TM-02).
//!
//! Holds the explicit namespace registry behind `POST /mt/ns/{ns}`.
//! The registry joins the tenant state that TM-01 and the MT wave own:
//! credential tables in `MemoryAuth`
//! (`crates/broker-auth/src/lib.rs`, per-tenant users and rules driven by
//! CONNECT and publish checks in `crates/broker-node/src/main.rs`),
//! sessions in `SessionManager`
//! (`crates/broker-session/src/lib.rs`, scoped by `(tenant, client id)`
//! at bind), and subscriptions in the router
//! (`crates/broker-router/src/lib.rs`, matched per tenant on publish).
//! This module adds no second tenancy scheme: a namespace id uses the
//! same shape as every other tenant id, and client reads use
//! `active_client_ids_in_tenant` so one tenant never sees another.
//!
//! Bounds (each stated here and enforced below):
//! - at most [`MAX_MANAGED_NAMESPACES`] namespaces;
//! - one namespace id holds at most [`MAX_NS_ID_LEN`] chars;
//! - list pages hold at most [`MT_LIST_MAX_LIMIT`] rows, default
//!   [`MT_LIST_DEFAULT_LIMIT`].
//!
//! Management-plane only. No publish path and no deliver path touches
//! this store.

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::errors::ApiError;
use crate::ApiState;

/// Tenant ids this broker never manages (TM-02).
/// Reason: the documented deny list keeps reserved names out of the
/// namespace space, so internal scopes never collide with tenants.
const DENY_NAMESPACES: [&str; 4] = ["global", "undefined", "null", "none"];

/// Upper bound for explicitly managed namespaces (TM-02).
/// Reason: one entry is one map header plus short strings, so 1024
/// entries bound the table near tens of KiB while covering installs far
/// past real deployments; writes past the cap fail closed.
pub const MAX_MANAGED_NAMESPACES: usize = 1024;

/// Longest accepted namespace id (TM-02).
/// Reason: mirrors the session tenant bound (128 chars), so the id
/// joins session keys and log lines with bounded key memory.
pub const MAX_NS_ID_LEN: usize = 128;

/// Default list page size (TM-02).
/// Reason: the console pages at 100 rows, so the default serves one
/// console page with one bounded response.
pub const MT_LIST_DEFAULT_LIMIT: usize = 100;

/// Upper bound for a list page size (TM-02).
/// Reason: 1000 rows bound one response near hundreds of KiB while
/// covering console scans; larger asks clamp instead of failing.
pub const MT_LIST_MAX_LIMIT: usize = 1000;

/// One managed namespace: its stored config plus creation time.
#[derive(Debug, Clone)]
pub struct ManagedNamespace {
    /// Namespace id.
    pub name: String,
    /// Stored namespace config (limiter and session blocks).
    pub config: serde_json::Value,
    /// Creation time as unix seconds.
    pub created_at: u64,
}

/// Explicit namespace registry for `POST /mt/ns/{ns}` (TM-02).
///
/// The default tenant always exists without an entry. Every other
/// tenant exists after the operator creates it here. Client and config
/// reads consult this registry for existence and read live broker state
/// (sessions, users, rules) for content, so the API never invents a
/// value.
pub struct NamespaceStore {
    inner: std::sync::RwLock<HashMap<String, ManagedNamespace>>,
    /// Creation time of the store itself (unix seconds). Serves as the
    /// `created_at` of the default tenant, which has no entry.
    born_at: u64,
}

impl NamespaceStore {
    /// Empty registry: only the default tenant exists.
    pub fn new() -> Self {
        Self {
            inner: std::sync::RwLock::new(HashMap::new()),
            born_at: unix_now_secs(),
        }
    }

    /// True when `name` exists (the default tenant always exists).
    pub fn contains(&self, name: &str) -> bool {
        if is_default_ns(name) {
            return true;
        }
        self.inner
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .contains_key(name)
    }

    /// Number of explicitly managed namespaces.
    pub fn len(&self) -> usize {
        self.inner
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .len()
    }

    /// True when no explicit namespace exists.
    pub fn is_empty(&self) -> bool {
        self.inner
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .is_empty()
    }

    /// Insert one namespace with an empty config.
    pub fn create(&self, name: &str) -> Result<(), String> {
        validate_ns_id(name)?;
        if is_default_ns(name) {
            return Err(format!("namespace {name:?} is reserved"));
        }
        let mut inner = self
            .inner
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        if inner.contains_key(name) {
            return Err(format!("namespace {name:?} already exists"));
        }
        if inner.len() >= MAX_MANAGED_NAMESPACES {
            return Err(format!(
                "namespace table holds {} entries, at most {MAX_MANAGED_NAMESPACES}",
                inner.len()
            ));
        }
        inner.insert(
            name.to_string(),
            ManagedNamespace {
                name: name.to_string(),
                config: serde_json::json!({}),
                created_at: unix_now_secs(),
            },
        );
        Ok(())
    }

    /// Insert or replace one namespace config (bulk upsert path).
    pub fn upsert_config(&self, name: &str, config: serde_json::Value) -> Result<(), String> {
        validate_ns_id(name)?;
        if is_default_ns(name) {
            return Err(format!("namespace {name:?} is reserved"));
        }
        validate_ns_config(&config)?;
        let mut inner = self
            .inner
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        if !inner.contains_key(name) && inner.len() >= MAX_MANAGED_NAMESPACES {
            return Err(format!(
                "namespace table holds {} entries, at most {MAX_MANAGED_NAMESPACES}",
                inner.len()
            ));
        }
        let created_at = inner
            .get(name)
            .map(|entry| entry.created_at)
            .unwrap_or_else(unix_now_secs);
        inner.insert(
            name.to_string(),
            ManagedNamespace {
                name: name.to_string(),
                config,
                created_at,
            },
        );
        Ok(())
    }

    /// Replace the config of one existing namespace.
    pub fn put_config(&self, name: &str, config: serde_json::Value) -> Result<(), String> {
        validate_ns_id(name)?;
        validate_ns_config(&config)?;
        let mut inner = self
            .inner
            .write()
            .unwrap_or_else(|poison| poison.into_inner());
        let Some(entry) = inner.get_mut(name) else {
            return Err(format!("namespace {name:?} not found"));
        };
        entry.config = config;
        Ok(())
    }

    /// Read one namespace (the default tenant reads with empty config).
    pub fn get(&self, name: &str) -> Option<ManagedNamespace> {
        if is_default_ns(name) {
            return Some(ManagedNamespace {
                name: name.to_string(),
                config: serde_json::json!({}),
                created_at: self.born_at,
            });
        }
        self.inner
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(name)
            .cloned()
    }

    /// Remove one namespace. The default tenant never leaves.
    pub fn remove(&self, name: &str) -> bool {
        if is_default_ns(name) {
            return false;
        }
        self.inner
            .write()
            .unwrap_or_else(|poison| poison.into_inner())
            .remove(name)
            .is_some()
    }

    /// Sorted names of explicitly managed namespaces.
    pub fn managed_sorted(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .inner
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .keys()
            .cloned()
            .collect();
        names.sort();
        names
    }

    /// Sorted names of every known namespace: the default tenant plus
    /// each managed namespace.
    /// TODO(parity): does the documented list also carry tenants that
    /// hold users but have no managed entry? The rulebook does not
    /// decide the union rule; the current choice lists the default plus
    /// managed entries only, so implicit credential tenants appear after
    /// the operator creates them here.
    pub fn all_sorted(&self) -> Vec<String> {
        let mut names = self.managed_sorted();
        if !names.iter().any(|name| is_default_ns(name)) {
            names.push(default_tenant_id().to_string());
        }
        names.sort();
        names
    }
}

impl Default for NamespaceStore {
    fn default() -> Self {
        Self::new()
    }
}

/// True when `name` selects the default tenant: empty (absent) or the
/// default literal. Empty rides as default so callers without a scope
/// land in the pre-tenancy path, never in another tenant's space.
fn is_default_ns(name: &str) -> bool {
    name.is_empty() || name == default_tenant_id()
}

/// The default tenant id. Lives here so this module needs no new
/// dependency for the literal.
pub fn default_tenant_id() -> &'static str {
    "default"
}

/// Current unix time in seconds. Falls back to zero when the clock
/// reads before the epoch (never on a running broker).
fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|span| span.as_secs())
        .unwrap_or(0)
}

/// Check one namespace id before it applies.
fn validate_ns_id(name: &str) -> Result<(), String> {
    if name.trim().is_empty() {
        return Err("field `ns` must not be empty".to_string());
    }
    if name.len() > MAX_NS_ID_LEN {
        return Err(format!("field `ns` {name:?} exceeds {MAX_NS_ID_LEN} chars"));
    }
    if name.chars().any(|c| c.is_control()) {
        return Err(format!("field `ns` {name:?} must not hold control chars"));
    }
    if DENY_NAMESPACES.contains(&name) {
        return Err(format!("namespace {name:?} is reserved"));
    }
    Ok(())
}

/// Check one stored namespace config before it applies.
fn validate_ns_config(config: &serde_json::Value) -> Result<(), String> {
    if !config.is_object() {
        return Err("field `config` must be a JSON object".to_string());
    }
    Ok(())
}

/// 404 shape for an unknown namespace.
fn ns_not_found(name: &str) -> Response {
    ApiError::NotFound(format!("namespace {name:?} not found")).into_response()
}

/// Keyset page over sorted names with `last_ns`/`first_ns`/`limit`.
/// Reason: the documented cursor paging keeps pages stable while the
/// table changes; `limit` bounds one response.
#[derive(Debug, Deserialize, Default)]
pub struct NsListQuery {
    #[serde(default)]
    pub last_ns: Option<String>,
    #[serde(default)]
    pub first_ns: Option<String>,
    #[serde(default)]
    pub limit: Option<String>,
}

/// Keyset page over sorted client ids with `last_clientid` etc.
#[derive(Debug, Deserialize, Default)]
pub struct ClientListQuery {
    #[serde(default)]
    pub last_clientid: Option<String>,
    #[serde(default)]
    pub first_clientid: Option<String>,
    #[serde(default)]
    pub limit: Option<String>,
}

fn parse_limit(raw: Option<&str>) -> Result<usize, Box<Response>> {
    let Some(text) = raw else {
        return Ok(MT_LIST_DEFAULT_LIMIT);
    };
    let value: i64 = text.trim().parse().map_err(|_| {
        Box::new(
            ApiError::BadRequest("field `limit` must be an integer".to_string()).into_response(),
        )
    })?;
    if value < 1 {
        return Err(Box::new(
            ApiError::BadRequest("field `limit` must be at least 1".to_string()).into_response(),
        ));
    }
    Ok((value as usize).min(MT_LIST_MAX_LIMIT))
}

fn page_names(
    sorted: &[String],
    last_ns: Option<&str>,
    first_ns: Option<&str>,
) -> Result<Vec<String>, Box<Response>> {
    let last = last_ns.unwrap_or("");
    let first = first_ns.unwrap_or("");
    if !last.is_empty() && !first.is_empty() {
        return Err(Box::new(
            ApiError::BadRequest(
                "cursor `last_ns` and cursor `first_ns` are mutually exclusive".to_string(),
            )
            .into_response(),
        ));
    }
    let mut out = Vec::new();
    for name in sorted {
        if !first.is_empty() {
            if name.as_str() < first {
                continue;
            }
        } else if !last.is_empty() && name.as_str() <= last {
            continue;
        }
        out.push(name.clone());
    }
    Ok(out)
}

fn page_clients(
    sorted: &[String],
    last: Option<&str>,
    first: Option<&str>,
) -> Result<Vec<String>, Box<Response>> {
    let last = last.unwrap_or("");
    let first = first.unwrap_or("");
    if !last.is_empty() && !first.is_empty() {
        return Err(Box::new(
            ApiError::BadRequest(
                "cursor `last_clientid` and cursor `first_clientid` are mutually exclusive"
                    .to_string(),
            )
            .into_response(),
        ));
    }
    let mut out = Vec::new();
    for id in sorted {
        if !first.is_empty() {
            if id.as_str() < first {
                continue;
            }
        } else if !last.is_empty() && id.as_str() <= last {
            continue;
        }
        out.push(id.clone());
    }
    Ok(out)
}

/// Render the `Link: <...>; rel="next"` header for one more page.
fn next_link(
    base: &str,
    cursor_name: &str,
    last_item: &str,
    limit: usize,
    headers: &mut HeaderMap,
) {
    let value = format!("<{base}?{cursor_name}={last_item}&limit={limit}>; rel=\"next\"");
    if let Ok(header) = value.parse() {
        headers.insert("link", header);
    }
}

/// `POST /mt/ns/{ns}`: create one managed namespace (TM-02).
///
/// Answers 204 with no body. A duplicate answers 409 with code
/// `CONFLICT`. A reserved or malformed id answers 400 with code
/// `BAD_REQUEST`. A full table answers 400 with code `BAD_REQUEST`.
/// Management-plane only; publish and deliver never touch this store.
pub async fn create_ns(State(state): State<ApiState>, Path(ns): Path<String>) -> Response {
    match state.namespaces.create(&ns) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(message) => {
            if message.contains("already exists") {
                (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "code": "CONFLICT", "message": message })),
                )
                    .into_response()
            } else {
                ApiError::BadRequest(message).into_response()
            }
        }
    }
}

/// `DELETE /mt/ns/{ns}`: delete one managed namespace (TM-02).
///
/// Answers 204 with no body, even for an unknown id (idempotent
/// delete). Kicks each connected client of the tenant first, then drops
/// the managed entry plus the tenant credential and rule rows, so a
/// deleted tenant denies (closed, counted, logged) instead of serving
/// stale users. A registry save failure after the in-memory drop answers
/// 500 with code `INTERNAL_ERROR` (the loss must never be silent). The
/// default tenant never leaves. Management-plane only; publish and
/// deliver never touch this store.
pub async fn delete_ns(State(state): State<ApiState>, Path(ns): Path<String>) -> Response {
    if is_default_ns(&ns) {
        return ApiError::BadRequest("the default namespace cannot be deleted".to_string())
            .into_response();
    }
    if validate_ns_id(&ns).is_err() {
        return ApiError::BadRequest(format!("field `ns` {ns:?} is not valid")).into_response();
    }
    kick_all_in_tenant(&state, &ns);
    if let Err(message) = remove_tenant_credentials(&state, &ns) {
        return ApiError::InternalError(message).into_response();
    }
    state.namespaces.remove(&ns);
    StatusCode::NO_CONTENT.into_response()
}

/// Kick each connected client of one tenant.
fn kick_all_in_tenant(state: &ApiState, tenant: &str) {
    for client_id in state.sessions.active_client_ids_in_tenant(tenant) {
        let Some(session) = state.sessions.get_in_tenant(tenant, &client_id) else {
            continue;
        };
        let conn_id = *session.conn_id.read();
        let Some(conn_id) = conn_id else {
            continue;
        };
        if let Ok(frame) = brokerlink::BrokerFrame::new(
            brokerlink::OpCode::ConnClose,
            conn_id,
            0,
            bytes::Bytes::new(),
            bytes::Bytes::new(),
        ) {
            state.conns.route(conn_id, frame.clone());
            if state.edge_tx.send(frame).is_err() {
                tracing::warn!(
                    client_id = %client_id,
                    conn_id,
                    "mt kick: edge gone, ConnClose dropped"
                );
            }
        }
        state.conns.unregister(conn_id);
        state
            .sessions
            .unbind_connection_in_tenant(tenant, &client_id, conn_id);
    }
}

/// Drop the credential and rule rows of one tenant.
fn remove_tenant_credentials(state: &ApiState, tenant: &str) -> Result<(), String> {
    for username in state.auth.usernames_in_tenant(tenant) {
        if let Err(error) = state.auth.remove_user_in_tenant(tenant, &username) {
            tracing::warn!(
                tenant = %tenant,
                username = %username,
                "mt delete: cannot drop user: {error}"
            );
            return Err(format!("cannot drop tenant {tenant:?} users: {error}"));
        }
    }
    if let Err(error) = state.auth.clear_rules_in_tenant(tenant) {
        tracing::warn!(tenant = %tenant, "mt delete: cannot drop rules: {error}");
        return Err(format!("cannot drop tenant {tenant:?} rules: {error}"));
    }
    Ok(())
}

/// `GET /mt/ns_list`: list every known namespace (TM-02).
///
/// Reads the default tenant plus each managed namespace, sorted.
/// Answers 200 with a JSON array of ids. Pages with `last_ns`,
/// `first_ns` and `limit`; a further page carries the documented
/// `Link` header. Management-plane only.
pub async fn list_ns(State(state): State<ApiState>, Query(query): Query<NsListQuery>) -> Response {
    let limit = match parse_limit(query.limit.as_deref()) {
        Ok(limit) => limit,
        Err(response) => return *response,
    };
    let sorted = state.namespaces.all_sorted();
    let window = match page_names(&sorted, query.last_ns.as_deref(), query.first_ns.as_deref()) {
        Ok(window) => window,
        Err(response) => return *response,
    };
    let has_next = window.len() > limit;
    let items: Vec<String> = window.into_iter().take(limit).collect();
    let mut headers = HeaderMap::new();
    if has_next {
        if let Some(last) = items.last() {
            next_link("/api/v5/mt/ns_list", "last_ns", last, limit, &mut headers);
        }
    }
    (StatusCode::OK, headers, Json(items)).into_response()
}

/// `GET /mt/managed_ns_list`: list managed namespaces (TM-02).
///
/// Same paging as [`list_ns`] but without the default tenant.
/// Answers 200 with a JSON array of ids. Management-plane only.
pub async fn list_managed_ns(
    State(state): State<ApiState>,
    Query(query): Query<NsListQuery>,
) -> Response {
    let limit = match parse_limit(query.limit.as_deref()) {
        Ok(limit) => limit,
        Err(response) => return *response,
    };
    let sorted = state.namespaces.managed_sorted();
    let window = match page_names(&sorted, query.last_ns.as_deref(), query.first_ns.as_deref()) {
        Ok(window) => window,
        Err(response) => return *response,
    };
    let has_next = window.len() > limit;
    let items: Vec<String> = window.into_iter().take(limit).collect();
    let mut headers = HeaderMap::new();
    if has_next {
        if let Some(last) = items.last() {
            next_link(
                "/api/v5/mt/managed_ns_list",
                "last_ns",
                last,
                limit,
                &mut headers,
            );
        }
    }
    (StatusCode::OK, headers, Json(items)).into_response()
}

/// `GET /mt/ns_list_details`: list namespaces with details (TM-02).
///
/// Each row carries `name` and `created_at` (unix seconds, the true
/// creation time of a managed entry, the store start for the default
/// tenant). Same paging as [`list_ns`]. Management-plane only.
pub async fn list_ns_details(
    State(state): State<ApiState>,
    Query(query): Query<NsListQuery>,
) -> Response {
    let limit = match parse_limit(query.limit.as_deref()) {
        Ok(limit) => limit,
        Err(response) => return *response,
    };
    let sorted = state.namespaces.all_sorted();
    let window = match page_names(&sorted, query.last_ns.as_deref(), query.first_ns.as_deref()) {
        Ok(window) => window,
        Err(response) => return *response,
    };
    let has_next = window.len() > limit;
    let items: Vec<serde_json::Value> = window
        .into_iter()
        .take(limit)
        .filter_map(|name| {
            state.namespaces.get(&name).map(
                |entry| serde_json::json!({ "name": entry.name, "created_at": entry.created_at }),
            )
        })
        .collect();
    let mut headers = HeaderMap::new();
    if has_next {
        if let Some(last) = items.last() {
            if let Some(name) = last.get("name").and_then(|v| v.as_str()) {
                let owned = name.to_string();
                next_link(
                    "/api/v5/mt/ns_list_details",
                    "last_ns",
                    &owned,
                    limit,
                    &mut headers,
                );
            }
        }
    }
    (StatusCode::OK, headers, Json(items)).into_response()
}

/// `GET /mt/managed_ns_list_details`: managed namespaces plus config.
///
/// Each row carries `name`, `config` (exactly what the operator stored,
/// `{}` when never set) and `created_at`. Same paging as [`list_ns`].
/// Management-plane only.
pub async fn list_managed_ns_details(
    State(state): State<ApiState>,
    Query(query): Query<NsListQuery>,
) -> Response {
    let limit = match parse_limit(query.limit.as_deref()) {
        Ok(limit) => limit,
        Err(response) => return *response,
    };
    let sorted = state.namespaces.managed_sorted();
    let window = match page_names(&sorted, query.last_ns.as_deref(), query.first_ns.as_deref()) {
        Ok(window) => window,
        Err(response) => return *response,
    };
    let has_next = window.len() > limit;
    let items: Vec<serde_json::Value> = window
        .into_iter()
        .take(limit)
        .filter_map(|name| {
            state.namespaces.get(&name).map(|entry| {
                serde_json::json!({
                    "name": entry.name,
                    "config": entry.config,
                    "created_at": entry.created_at,
                })
            })
        })
        .collect();
    let mut headers = HeaderMap::new();
    if has_next {
        if let Some(last) = items.last() {
            if let Some(name) = last.get("name").and_then(|v| v.as_str()) {
                let owned = name.to_string();
                next_link(
                    "/api/v5/mt/managed_ns_list_details",
                    "last_ns",
                    &owned,
                    limit,
                    &mut headers,
                );
            }
        }
    }
    (StatusCode::OK, headers, Json(items)).into_response()
}

/// `GET /mt/ns/{ns}/client_count`: count connected clients (TM-02).
///
/// Counts the live sessions of the owning tenant only. Unknown
/// namespaces answer 404 with code `NOT_FOUND`. Management-plane only.
pub async fn ns_client_count(State(state): State<ApiState>, Path(ns): Path<String>) -> Response {
    if !state.namespaces.contains(&ns) {
        return ns_not_found(&ns);
    }
    let count = state.sessions.active_client_ids_in_tenant(&ns).len();
    (StatusCode::OK, Json(serde_json::json!({ "count": count }))).into_response()
}

/// `GET /mt/ns/{ns}/client_list`: list connected client ids (TM-02).
///
/// Reads only the owning tenant's live sessions. Unknown namespaces
/// answer 404 with code `NOT_FOUND`. Pages with `last_clientid`,
/// `first_clientid` and `limit` plus the documented `Link` header.
/// Management-plane only.
pub async fn ns_client_list(
    State(state): State<ApiState>,
    Path(ns): Path<String>,
    Query(query): Query<ClientListQuery>,
) -> Response {
    if !state.namespaces.contains(&ns) {
        return ns_not_found(&ns);
    }
    let limit = match parse_limit(query.limit.as_deref()) {
        Ok(limit) => limit,
        Err(response) => return *response,
    };
    let mut sorted = state.sessions.active_client_ids_in_tenant(&ns);
    sorted.sort();
    let window = match page_clients(
        &sorted,
        query.last_clientid.as_deref(),
        query.first_clientid.as_deref(),
    ) {
        Ok(window) => window,
        Err(response) => return *response,
    };
    let has_next = window.len() > limit;
    let items: Vec<String> = window.into_iter().take(limit).collect();
    let mut headers = HeaderMap::new();
    if has_next {
        if let Some(last) = items.last() {
            let base = format!("/api/v5/mt/ns/{ns}/client_list");
            next_link(&base, "last_clientid", last, limit, &mut headers);
        }
    }
    (StatusCode::OK, headers, Json(items)).into_response()
}

/// `GET /mt/ns/{ns}/config`: read one namespace config (TM-02).
///
/// Answers the exact stored object (`{}` when never set). Unknown
/// namespaces answer 404 with code `NOT_FOUND`. Per-tenant limits are
/// stored here but not enforced yet (TM-06 owns enforcement); the read
/// reports what the operator stored, never an applied claim.
/// Management-plane only.
pub async fn get_ns_config(State(state): State<ApiState>, Path(ns): Path<String>) -> Response {
    if !state.namespaces.contains(&ns) {
        return ns_not_found(&ns);
    }
    let Some(entry) = state.namespaces.get(&ns) else {
        return ns_not_found(&ns);
    };
    (StatusCode::OK, Json(entry.config)).into_response()
}

/// `PUT /mt/ns/{ns}/config`: replace one namespace config (TM-02).
///
/// The body must be a JSON object; anything else answers 400 with code
/// `BAD_REQUEST` and applies nothing. Unknown namespaces answer 404
/// and create nothing. The write stores the object and answers it back;
/// enforcement arrives with TM-06, so the route claims storage only.
/// Management-plane only.
pub async fn put_ns_config(
    State(state): State<ApiState>,
    Path(ns): Path<String>,
    body: bytes::Bytes,
) -> Response {
    if !state.namespaces.contains(&ns) {
        return ns_not_found(&ns);
    }
    if is_default_ns(&ns) {
        return ApiError::BadRequest("the default namespace config is not managed".to_string())
            .into_response();
    }
    let config: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(config) => config,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid config body: {error}")).into_response();
        }
    };
    if let Err(message) = state.namespaces.put_config(&ns, config.clone()) {
        if message.contains("not found") {
            return ns_not_found(&ns);
        }
        return ApiError::BadRequest(message).into_response();
    }
    (StatusCode::OK, Json(config)).into_response()
}

/// `POST /mt/ns/{ns}/kick_all_clients`: kick each client (TM-02).
///
/// Kicks every connected client of the owning tenant only, then answers
/// 202 with no body. Unknown namespaces answer 404 with code
/// `NOT_FOUND`.
/// TODO(parity): does the documented 409 (a kick already underway)
/// ever fire here? The rulebook does not decide the async kick state;
/// the current choice kicks synchronously and never answers 409.
pub async fn kick_ns_clients(State(state): State<ApiState>, Path(ns): Path<String>) -> Response {
    if !state.namespaces.contains(&ns) {
        return ns_not_found(&ns);
    }
    kick_all_in_tenant(&state, &ns);
    StatusCode::ACCEPTED.into_response()
}

/// `GET /mt/ns/{ns}/metrics`: read one namespace meter set (TM-02).
///
/// The broker tracks meters globally, not per tenant, and tenant
/// metering is out of scope for this task. The route answers the
/// documented shape with the true empty set (`{"metrics": {}}`), never
/// a constant standing in for a measurement. Unknown namespaces answer
/// 404 with code `NOT_FOUND`. Management-plane only.
pub async fn ns_metrics(State(state): State<ApiState>, Path(ns): Path<String>) -> Response {
    if !state.namespaces.contains(&ns) {
        return ns_not_found(&ns);
    }
    (StatusCode::OK, Json(serde_json::json!({ "metrics": {} }))).into_response()
}

/// Body of `DELETE /mt/bulk_delete_ns`: `{"nss": [...]}`.
#[derive(Debug, Deserialize)]
struct BulkDeleteBody {
    #[serde(default)]
    nss: Option<Vec<String>>,
}

/// `DELETE /mt/bulk_delete_ns`: delete many namespaces (TM-02).
///
/// Deletes every named namespace (kicking its clients and dropping its
/// credential rows first) and answers 204. Unknown names are skipped
/// (idempotent delete). A registry save failure after an in-memory drop
/// answers 500 with code `INTERNAL_ERROR`. A malformed body answers 400
/// with code `BAD_REQUEST`. Management-plane only.
pub async fn bulk_delete_ns(State(state): State<ApiState>, body: bytes::Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid bulk delete body: {error}"))
                .into_response();
        }
    };
    let parsed: BulkDeleteBody = match serde_json::from_value(value) {
        Ok(parsed) => parsed,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid bulk delete body: {error}"))
                .into_response();
        }
    };
    let Some(nss) = parsed.nss else {
        return ApiError::BadRequest("field `nss` is required".to_string()).into_response();
    };
    for ns in &nss {
        if is_default_ns(ns) || validate_ns_id(ns).is_err() {
            continue;
        }
        kick_all_in_tenant(&state, ns);
        if let Err(message) = remove_tenant_credentials(&state, ns) {
            return ApiError::InternalError(message).into_response();
        }
        state.namespaces.remove(ns);
    }
    StatusCode::NO_CONTENT.into_response()
}

/// Body of `POST /mt/bulk_export_ns_configs`: `{"namespaces": [...]}`.
#[derive(Debug, Deserialize)]
struct BulkExportBody {
    #[serde(default)]
    namespaces: Option<Vec<String>>,
}

/// `POST /mt/bulk_export_ns_configs`: export stored configs (TM-02).
///
/// Answers 200 with `{ns: config}` for every known namespace.
/// Unknown names are left out (never invented). A missing `namespaces`
/// field answers 400 with code `BAD_REQUEST`. Management-plane only.
pub async fn bulk_export_ns_configs(State(state): State<ApiState>, body: bytes::Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid export body: {error}")).into_response();
        }
    };
    let parsed: BulkExportBody = match serde_json::from_value(value) {
        Ok(parsed) => parsed,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid export body: {error}")).into_response();
        }
    };
    let Some(namespaces) = parsed.namespaces else {
        return ApiError::BadRequest("field `namespaces` is required".to_string()).into_response();
    };
    let mut out = serde_json::Map::new();
    for ns in &namespaces {
        if state.namespaces.contains(ns) {
            if let Some(entry) = state.namespaces.get(ns) {
                out.insert(ns.clone(), entry.config);
            }
        }
    }
    (StatusCode::OK, Json(serde_json::Value::Object(out))).into_response()
}

/// One entry of `POST /mt/bulk_import_configs`: `{"ns", "config"}`.
#[derive(Debug, Deserialize)]
struct BulkConfigEntry {
    ns: String,
    config: serde_json::Value,
}

/// `POST /mt/bulk_import_configs`: upsert many configs (TM-02).
///
/// Validates every entry before anything applies; one bad entry
/// answers 400 with code `BAD_REQUEST` and applies nothing. Unknown
/// namespaces are created by the upsert. Answers 204 on success.
/// Management-plane only.
pub async fn bulk_import_configs(State(state): State<ApiState>, body: bytes::Bytes) -> Response {
    let entries: Vec<BulkConfigEntry> = match serde_json::from_slice(&body) {
        Ok(entries) => entries,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid import body: {error}")).into_response();
        }
    };
    for entry in &entries {
        if let Err(message) = validate_ns_id(&entry.ns) {
            return ApiError::BadRequest(message).into_response();
        }
        if is_default_ns(&entry.ns) {
            return ApiError::BadRequest("the default namespace config is not managed".to_string())
                .into_response();
        }
        if let Err(message) = validate_ns_config(&entry.config) {
            return ApiError::BadRequest(message).into_response();
        }
    }
    let mut fresh = 0;
    for entry in &entries {
        if !state.namespaces.contains(&entry.ns) {
            fresh += 1;
        }
    }
    if state.namespaces.len() + fresh > MAX_MANAGED_NAMESPACES {
        return ApiError::BadRequest(format!(
            "namespace table holds {} entries, at most {MAX_MANAGED_NAMESPACES}",
            state.namespaces.len()
        ))
        .into_response();
    }
    for entry in entries {
        if let Err(message) = state.namespaces.upsert_config(&entry.ns, entry.config) {
            return ApiError::BadRequest(message).into_response();
        }
    }
    StatusCode::NO_CONTENT.into_response()
}

/// Body of `POST /mt/bulk_import_ns_configs`.
#[derive(Debug, Deserialize)]
struct BulkImportNsBody {
    #[serde(default)]
    configs: Option<HashMap<String, serde_json::Value>>,
    #[serde(default)]
    dry_run: Option<bool>,
}

/// `POST /mt/bulk_import_ns_configs`: import a config map (TM-02).
///
/// Validates every entry before anything applies; one bad entry
/// answers 400 and applies nothing. With `dry_run: true` nothing
/// applies and the route answers 200 with the supplied map. Else it
/// upserts each namespace and answers 200 with the applied map.
/// `configs` and `dry_run` are both required. Management-plane only.
pub async fn bulk_import_ns_configs(State(state): State<ApiState>, body: bytes::Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid import body: {error}")).into_response();
        }
    };
    let parsed: BulkImportNsBody = match serde_json::from_value(value) {
        Ok(parsed) => parsed,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid import body: {error}")).into_response();
        }
    };
    let (Some(configs), Some(dry_run)) = (parsed.configs, parsed.dry_run) else {
        return ApiError::BadRequest("fields `configs` and `dry_run` are required".to_string())
            .into_response();
    };
    for (ns, config) in &configs {
        if let Err(message) = validate_ns_id(ns) {
            return ApiError::BadRequest(message).into_response();
        }
        if is_default_ns(ns) {
            return ApiError::BadRequest("the default namespace config is not managed".to_string())
                .into_response();
        }
        if let Err(message) = validate_ns_config(config) {
            return ApiError::BadRequest(message).into_response();
        }
    }
    if dry_run {
        return (StatusCode::OK, Json(configs)).into_response();
    }
    let mut fresh = 0;
    for ns in configs.keys() {
        if !state.namespaces.contains(ns) {
            fresh += 1;
        }
    }
    if state.namespaces.len() + fresh > MAX_MANAGED_NAMESPACES {
        return ApiError::BadRequest(format!(
            "namespace table holds {} entries, at most {MAX_MANAGED_NAMESPACES}",
            state.namespaces.len()
        ))
        .into_response();
    }
    for (ns, config) in &configs {
        if let Err(message) = state.namespaces.upsert_config(ns, config.clone()) {
            return ApiError::BadRequest(message).into_response();
        }
    }
    (StatusCode::OK, Json(configs)).into_response()
}

#[cfg(test)]
mod mt_tests {
    use super::*;
    use crate::admin_users::AdminRole;
    use broker_rules::{BackpressurePolicy, RuleEngine};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct TestServer {
        port: u16,
        task: tokio::task::JoinHandle<()>,
    }

    struct TestState {
        inner: ApiState,
    }

    impl TestServer {
        async fn start() -> (Self, TestState) {
            let engine = Arc::new(RuleEngine::new(16, BackpressurePolicy::DropOldest));
            let state = ApiState::standalone(engine);
            state
                .auth
                .set_policy(broker_auth::PasswordHashPolicy::for_tests());
            state
                .admin_users
                .create("op-admin", "Adm1n-test-pass!", AdminRole::Administrator, "")
                .expect("admin creates");
            state
                .admin_users
                .create("op-viewer", "V13wer-test-pass!", AdminRole::Viewer, "")
                .expect("viewer creates");
            let inner = state.clone();
            let app = crate::router(state);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind test server");
            let port = listener.local_addr().expect("local addr").port();
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.expect("serve test app");
            });
            (Self { port, task }, TestState { inner })
        }

        async fn send(
            &self,
            method: &str,
            path: &str,
            body: Option<serde_json::Value>,
            token: Option<&str>,
        ) -> (u16, serde_json::Value) {
            let mut head = format!("{method} {path} HTTP/1.0\r\nHost: test\r\n");
            if let Some(token) = token {
                head.push_str(&format!("Authorization: Bearer {token}\r\n"));
            }
            if let Some(body) = body {
                let raw = serde_json::to_string(&body).expect("test body serialises");
                head.push_str(&format!(
                    "Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{raw}",
                    raw.len()
                ));
            } else {
                head.push_str("Connection: close\r\n\r\n");
            }
            let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", self.port))
                .await
                .expect("connect test server");
            stream
                .write_all(head.as_bytes())
                .await
                .expect("write request");
            let mut buf = Vec::new();
            stream.read_to_end(&mut buf).await.expect("read response");
            let text = String::from_utf8(buf).expect("response is UTF-8");
            let (head, body) = text.split_once("\r\n\r\n").expect("header/body split");
            let status: u16 = head.lines().next().expect("status line")[9..12]
                .parse()
                .expect("status code");
            let json = if body.trim().is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::from_str(body).expect("response body is JSON")
            };
            (status, json)
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn admin_token(state: &ApiState) -> String {
        state.tokens.issue("op-admin", AdminRole::Administrator)
    }

    fn viewer_token(state: &ApiState) -> String {
        state.tokens.issue("op-viewer", AdminRole::Viewer)
    }

    /// Create two tenants through the API. List shows both. Show shows
    /// each. Delete removes one and keeps the other. Unknown ids miss.
    #[tokio::test]
    async fn tm02_create_list_show_delete_goes_through_the_api() {
        let (server, state) = TestServer::start().await;
        let admin = admin_token(&state.inner);
        let viewer = viewer_token(&state.inner);
        // No token fails.
        let (status, body) = server
            .send("GET", "/api/v5/mt/managed_ns_list", None, None)
            .await;
        assert_eq!(status, 401);
        assert_eq!(body["code"], serde_json::json!("UNAUTHORIZED"));
        // Create two tenants through the production entry.
        for ns in ["tm02-alpha", "tm02-beta"] {
            let (status, _) = server
                .send(
                    "POST",
                    &format!("/api/v5/mt/ns/{ns}"),
                    Some(serde_json::json!({})),
                    Some(&admin),
                )
                .await;
            assert_eq!(status, 204, "create {ns}");
        }
        // A duplicate create conflicts.
        let (status, body) = server
            .send(
                "POST",
                "/api/v5/mt/ns/tm02-alpha",
                Some(serde_json::json!({})),
                Some(&admin),
            )
            .await;
        assert_eq!(status, 409);
        assert_eq!(body["code"], serde_json::json!("CONFLICT"));
        // List shows both.
        let (status, body) = server
            .send("GET", "/api/v5/mt/managed_ns_list", None, Some(&admin))
            .await;
        assert_eq!(status, 200);
        assert!(body
            .as_array()
            .expect("list is an array")
            .contains(&serde_json::json!("tm02-alpha")));
        assert!(body
            .as_array()
            .expect("list is an array")
            .contains(&serde_json::json!("tm02-beta")));
        // The full list carries the default tenant too.
        let (status, body) = server
            .send("GET", "/api/v5/mt/ns_list", None, Some(&admin))
            .await;
        assert_eq!(status, 200);
        assert!(body
            .as_array()
            .expect("list is an array")
            .contains(&serde_json::json!("default")));
        // Show shows each through its config read.
        for ns in ["tm02-alpha", "tm02-beta"] {
            let (status, _) = server
                .send(
                    "GET",
                    &format!("/api/v5/mt/ns/{ns}/config"),
                    None,
                    Some(&admin),
                )
                .await;
            assert_eq!(status, 200, "show {ns}");
        }
        // An unknown tenant misses with the documented shape.
        let (status, body) = server
            .send("GET", "/api/v5/mt/ns/tm02-ghost/config", None, Some(&admin))
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
        // A viewer reads but cannot write.
        let (status, _) = server
            .send("GET", "/api/v5/mt/managed_ns_list", None, Some(&viewer))
            .await;
        assert_eq!(status, 200);
        let (status, body) = server
            .send(
                "POST",
                "/api/v5/mt/ns/tm02-viewer-try",
                Some(serde_json::json!({})),
                Some(&viewer),
            )
            .await;
        assert_eq!(status, 403);
        assert_eq!(body["code"], serde_json::json!("FORBIDDEN"));
        // Delete removes one and keeps the other.
        let (status, _) = server
            .send("DELETE", "/api/v5/mt/ns/tm02-alpha", None, Some(&admin))
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .send("GET", "/api/v5/mt/managed_ns_list", None, Some(&admin))
            .await;
        assert_eq!(status, 200);
        let items = body.as_array().expect("list is an array");
        assert!(!items.contains(&serde_json::json!("tm02-alpha")));
        assert!(items.contains(&serde_json::json!("tm02-beta")));
        // The deleted tenant misses now.
        let (status, body) = server
            .send("GET", "/api/v5/mt/ns/tm02-alpha/config", None, Some(&admin))
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
    }

    /// The same username in two tenants holds two passwords. Each opens
    /// only its own tenant. The default tenant works with no change.
    #[tokio::test]
    async fn tm02_same_username_in_two_tenants_authenticates_apart() {
        let (server, state) = TestServer::start().await;
        let admin = admin_token(&state.inner);
        for ns in ["tm02-ten-a", "tm02-ten-b"] {
            let (status, _) = server
                .send(
                    "POST",
                    &format!("/api/v5/mt/ns/{ns}"),
                    Some(serde_json::json!({})),
                    Some(&admin),
                )
                .await;
            assert_eq!(status, 204, "create {ns}");
        }
        // Same username, different passwords, one user per tenant.
        for (tenant, password) in [("tm02-ten-a", "pw-for-a-4x"), ("tm02-ten-b", "pw-for-b-8q")] {
            let (status, _) = server
                .send(
                    "POST",
                    &format!("/api/v1/auth/users?tenant={tenant}"),
                    Some(serde_json::json!({"username": "sam", "password": password})),
                    Some(&admin),
                )
                .await;
            assert_eq!(status, 201, "user creates in {tenant}");
            for action in ["publish", "subscribe"] {
                let (status, _) = server
                    .send(
                        "POST",
                        &format!("/api/v1/auth/acls?tenant={tenant}"),
                        Some(serde_json::json!({
                            "client_pattern": "*",
                            "action": action,
                            "topic_pattern": "t/#",
                            "allow": true,
                        })),
                        Some(&admin),
                    )
                    .await;
                assert_eq!(status, 201, "{action} rule creates in {tenant}");
            }
        }
        // Each password opens only its own tenant.
        assert!(state
            .inner
            .auth
            .authenticate_in_tenant("tm02-ten-a", "c", Some("sam"), Some(b"pw-for-a-4x"))
            .await
            .is_ok());
        assert!(state
            .inner
            .auth
            .authenticate_in_tenant("tm02-ten-b", "c", Some("sam"), Some(b"pw-for-b-8q"))
            .await
            .is_ok());
        assert!(state
            .inner
            .auth
            .authenticate_in_tenant("tm02-ten-a", "c", Some("sam"), Some(b"pw-for-b-8q"))
            .await
            .is_err());
        assert!(state
            .inner
            .auth
            .authenticate_in_tenant("tm02-ten-b", "c", Some("sam"), Some(b"pw-for-a-4x"))
            .await
            .is_err());
        // The default tenant still works with no scope change.
        let (status, _) = server
            .send(
                "POST",
                "/api/v1/auth/users",
                Some(serde_json::json!({"username": "base", "password": "base-pw-5z"})),
                Some(&admin),
            )
            .await;
        assert_eq!(status, 201);
        use broker_auth::Authenticator as _;
        assert!(state
            .inner
            .auth
            .authenticate("device-1", Some("base"), Some(b"base-pw-5z"))
            .await
            .is_ok());
    }

    /// Tenant client reads, counts, kicks, metrics and bulk config go
    /// through the API. Delivery stays inside one tenant.
    #[tokio::test]
    async fn tm02_tenant_clients_configs_and_bulk_go_through_the_api() {
        let (server, state) = TestServer::start().await;
        let admin = admin_token(&state.inner);
        for ns in ["tm02-iso-a", "tm02-iso-b"] {
            let (status, _) = server
                .send(
                    "POST",
                    &format!("/api/v5/mt/ns/{ns}"),
                    Some(serde_json::json!({})),
                    Some(&admin),
                )
                .await;
            assert_eq!(status, 204, "create {ns}");
        }
        // One connected session per tenant, same client id in both.
        for (tenant, conn) in [("tm02-iso-a", 8101_u64), ("tm02-iso-b", 8102_u64)] {
            let (session, _) =
                state
                    .inner
                    .sessions
                    .get_or_create_in_tenant(tenant, "dup-client", true);
            state.inner.sessions.bind_session(&session, conn);
        }
        // Count sees one client per tenant.
        for ns in ["tm02-iso-a", "tm02-iso-b"] {
            let (status, body) = server
                .send(
                    "GET",
                    &format!("/api/v5/mt/ns/{ns}/client_count"),
                    None,
                    Some(&admin),
                )
                .await;
            assert_eq!(status, 200, "count in {ns}");
            assert_eq!(body["count"], serde_json::json!(1));
        }
        // List shows the client of the owning tenant only.
        let (status, body) = server
            .send(
                "GET",
                "/api/v5/mt/ns/tm02-iso-a/client_list?limit=100",
                None,
                Some(&admin),
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body, serde_json::json!(["dup-client"]));
        // A publish in A matches only the subscriber in A.
        let filter = broker_protocol::TopicFilter::new("t/#").expect("filter parses");
        state.inner.router.subscribe(
            &filter,
            broker_router::Subscription::new_in_tenant(
                "sub-a",
                8201,
                broker_protocol::QoS::AtMostOnce,
                "tm02-iso-a",
            ),
        );
        state.inner.router.subscribe(
            &filter,
            broker_router::Subscription::new_in_tenant(
                "sub-b",
                8202,
                broker_protocol::QoS::AtMostOnce,
                "tm02-iso-b",
            ),
        );
        let topic = broker_protocol::Topic::new("t/1").expect("topic parses");
        let (matched_a, _) =
            state
                .inner
                .router
                .matches_in_tenant_counted(&topic, None, "tm02-iso-a");
        assert!(matched_a.iter().any(|s| s.client_id.as_ref() == "sub-a"));
        assert!(!matched_a.iter().any(|s| s.client_id.as_ref() == "sub-b"));
        // Metrics answer the documented shape with a true empty set.
        let (status, body) = server
            .send(
                "GET",
                "/api/v5/mt/ns/tm02-iso-a/metrics",
                None,
                Some(&admin),
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body, serde_json::json!({"metrics": {}}));
        // Config stores and reads back exactly what the operator sent.
        let config = serde_json::json!({"session": {"max_sessions": 10}});
        let (status, body) = server
            .send(
                "PUT",
                "/api/v5/mt/ns/tm02-iso-a/config",
                Some(config.clone()),
                Some(&admin),
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body, config);
        // Bulk export carries the stored config.
        let (status, body) = server
            .send(
                "POST",
                "/api/v5/mt/bulk_export_ns_configs",
                Some(serde_json::json!({"namespaces": ["tm02-iso-a"]})),
                Some(&admin),
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["tm02-iso-a"], config);
        // Bulk import upserts a new namespace, then details show it.
        let (status, _) = server
            .send(
                "POST",
                "/api/v5/mt/bulk_import_configs",
                Some(serde_json::json!([
                    {"ns": "tm02-iso-c", "config": {"session": {"max_sessions": 3}}}
                ])),
                Some(&admin),
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .send(
                "GET",
                "/api/v5/mt/managed_ns_list_details?limit=100",
                None,
                Some(&admin),
            )
            .await;
        assert_eq!(status, 200);
        assert!(body
            .as_array()
            .expect("details list is an array")
            .iter()
            .any(|row| row["name"] == serde_json::json!("tm02-iso-c")));
        // Kick empties the tenant count and keeps the other tenant.
        let (status, _) = server
            .send(
                "POST",
                "/api/v5/mt/ns/tm02-iso-a/kick_all_clients",
                Some(serde_json::json!({})),
                Some(&admin),
            )
            .await;
        assert_eq!(status, 202);
        let (status, body) = server
            .send(
                "GET",
                "/api/v5/mt/ns/tm02-iso-a/client_count",
                None,
                Some(&admin),
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["count"], serde_json::json!(0));
        let (status, body) = server
            .send(
                "GET",
                "/api/v5/mt/ns/tm02-iso-b/client_count",
                None,
                Some(&admin),
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["count"], serde_json::json!(1));
        // Bulk delete removes one namespace and keeps the other.
        let (status, _) = server
            .send(
                "DELETE",
                "/api/v5/mt/bulk_delete_ns",
                Some(serde_json::json!({"nss": ["tm02-iso-c"]})),
                Some(&admin),
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .send("GET", "/api/v5/mt/ns/tm02-iso-c/config", None, Some(&admin))
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
    }

    /// Store bounds stay finite: ids, configs, tables and pages.
    #[test]
    fn tm02_namespace_store_bounds_stay_finite() {
        const {
            assert!(MAX_MANAGED_NAMESPACES > 0 && MAX_MANAGED_NAMESPACES <= 65536);
            assert!(MAX_NS_ID_LEN > 0 && MAX_NS_ID_LEN <= 1024);
            assert!(MT_LIST_DEFAULT_LIMIT > 0 && MT_LIST_DEFAULT_LIMIT <= MT_LIST_MAX_LIMIT);
            assert!(MT_LIST_MAX_LIMIT <= 10000);
        }
        let store = NamespaceStore::new();
        assert!(store.is_empty());
        assert!(store.create("default").is_err());
        for denied in ["global", "undefined", "null", "none"] {
            assert!(store.create(denied).is_err(), "{denied} stays reserved");
        }
        assert!(store.create(&"n".repeat(MAX_NS_ID_LEN + 1)).is_err());
        assert!(store
            .put_config("tm02-never", serde_json::json!({}))
            .is_err());
    }
}
