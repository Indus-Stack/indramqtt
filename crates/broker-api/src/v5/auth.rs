//! SCRAM-SHA-256 and fallback authentication for the v5 REST API.

use axum::{
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use broker_auth::{
    AclAction, AclRule, ImportUsersError, MAX_AUTHN_USERNAME_LEN, MAX_AUTHN_USERS,
    MAX_PASSWORD_BYTES,
};
use bytes::Bytes;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::admin_users::{AdminRole, AdminUserError};
use crate::errors::ApiError;
use crate::pagination::{meta_page, paginate, PageParams};
use crate::ApiState;

/// Token lifetime: 3600 s (60 minutes), the documented dashboard TTL.
/// Reason: the documented expiry for dashboard sessions; every lookup
/// re-checks `expires_at`, and logout plus password change revoke eagerly.
const TOKEN_TTL: Duration = Duration::from_secs(60 * 60);
/// Upper bound on live dashboard tokens: 1024 entries, expiry-swept on
/// every issue. Failed logins never insert.
/// Reason: each entry holds a short token string plus identity (hundreds
/// of bytes), so 1024 covers concurrent dashboard sessions while keeping
/// management memory bounded regardless of login rate. The default is
/// finite: no opt-in to unlimited exists anywhere on this path.
/// Management-plane only: publish and deliver never touch this map.
pub(crate) const MAX_LIVE_TOKENS: usize = 1024;
/// SCRAM challenge lifetime: 60 seconds.
const CHALLENGE_TTL: Duration = Duration::from_secs(60);
/// Upper bound on pending SCRAM challenges (flood cap).
pub(crate) const MAX_PENDING_CHALLENGES: usize = 10_000;

/// In-memory challenge state for pending SCRAM-SHA-256 handshakes.
#[derive(Clone)]
pub(crate) struct ScramChallengeState {
    username: String,
    client_nonce: String,
    _server_nonce: String,
    salt: Vec<u8>,
    iterations: u32,
    created_at: Instant,
}

#[cfg(test)]
impl ScramChallengeState {
    pub(crate) fn for_test(username: &str) -> Self {
        Self {
            username: username.to_string(),
            client_nonce: "test-client-nonce".to_string(),
            _server_nonce: "test-server-nonce".to_string(),
            salt: vec![0u8; 16],
            iterations: 4096,
            created_at: Instant::now(),
        }
    }
}

/// Authenticated identity resolved from a bearer token.
#[derive(Debug, Clone)]
pub struct TokenInfo {
    pub username: String,
    pub role: AdminRole,
    pub expires_at: Instant,
    pub must_change_password: bool,
}

/// Per-`ApiState` token and SCRAM-challenge store. Lives in `ApiState`
/// (not in process globals) so tests are isolated.
pub struct ApiTokens {
    tokens: Mutex<HashMap<String, TokenInfo>>,
    challenges: Mutex<HashMap<String, ScramChallengeState>>,
}

impl ApiTokens {
    pub fn new() -> Self {
        Self {
            tokens: Mutex::new(HashMap::new()),
            challenges: Mutex::new(HashMap::new()),
        }
    }

    /// Issue a random 32-byte URL-safe token bound to `username`/`role`.
    /// Management-plane only: one short lock plus an expiry sweep per
    /// login; publish and deliver never touch this map.
    pub fn issue(&self, username: &str, role: AdminRole) -> String {
        let mut token_bytes = [0u8; 32];
        rand::rng().fill_bytes(&mut token_bytes);
        let token = format!("indra_{}", URL_SAFE_NO_PAD.encode(token_bytes));
        let info = TokenInfo {
            username: username.to_string(),
            role,
            expires_at: Instant::now() + TOKEN_TTL,
            must_change_password: false,
        };
        let mut tokens = self.tokens.lock().unwrap();
        let now = Instant::now();
        tokens.retain(|_, v| v.expires_at > now);
        // TODO(parity): when the map is still full after the expiry sweep,
        // does the documented behaviour evict the oldest entry or refuse
        // the login? The rulebook does not decide the full-map policy; the
        // current choice evicts the oldest-expiring entry so a live
        // administrator can always log in, until the checker pins it.
        while tokens.len() >= MAX_LIVE_TOKENS {
            let oldest = tokens
                .iter()
                .min_by_key(|(_, info)| info.expires_at)
                .map(|(key, _)| key.clone());
            match oldest {
                Some(key) => {
                    tokens.remove(&key);
                }
                None => break,
            }
        }
        tokens.insert(token.clone(), info);
        token
    }

    /// Number of entries currently held (tests only).
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.tokens.lock().unwrap().len()
    }

    /// Look up a token; unknown or expired tokens return `None`.
    /// Expired entries are removed.
    pub fn lookup(&self, token: &str) -> Option<TokenInfo> {
        let mut tokens = self.tokens.lock().unwrap();
        let info = tokens.get(token).cloned()?;
        if info.expires_at > Instant::now() {
            Some(info)
        } else {
            tokens.remove(token);
            None
        }
    }

    /// Revoke one token (logout).
    pub fn revoke(&self, token: &str) {
        self.tokens.lock().unwrap().remove(token);
    }

    /// Issue an already-expired token (tests only).
    #[cfg(test)]
    pub(crate) fn issue_expired(&self, username: &str, role: AdminRole) -> String {
        let mut token_bytes = [0u8; 32];
        rand::rng().fill_bytes(&mut token_bytes);
        let token = format!("indra_{}", URL_SAFE_NO_PAD.encode(token_bytes));
        let info = TokenInfo {
            username: username.to_string(),
            role,
            expires_at: Instant::now() - Duration::from_secs(1),
            must_change_password: false,
        };
        self.tokens.lock().unwrap().insert(token.clone(), info);
        token
    }

    /// Revoke every token of `username` except `keep` (password change).
    pub fn revoke_user_except(&self, username: &str, keep: Option<&str>) {
        self.tokens
            .lock()
            .unwrap()
            .retain(|token, info| info.username != username || Some(token.as_str()) == keep);
    }

    pub(crate) fn insert_challenge(&self, id: String, state: ScramChallengeState) -> bool {
        let mut challenges = self.challenges.lock().unwrap();
        challenges.retain(|_, v| v.created_at.elapsed() < CHALLENGE_TTL);
        if challenges.len() >= MAX_PENDING_CHALLENGES {
            return false;
        }
        challenges.insert(id, state);
        true
    }

    pub(crate) fn take_challenge(&self, id: &str) -> Option<ScramChallengeState> {
        let mut challenges = self.challenges.lock().unwrap();
        let state = challenges.remove(id)?;
        if state.created_at.elapsed() < CHALLENGE_TTL {
            Some(state)
        } else {
            None
        }
    }
}

impl Default for ApiTokens {
    fn default() -> Self {
        Self::new()
    }
}

fn role_str(role: &AdminRole) -> &'static str {
    match role {
        AdminRole::Administrator => "administrator",
        AdminRole::Viewer => "viewer",
    }
}

fn parse_role(raw: &str) -> Option<AdminRole> {
    match raw.to_ascii_lowercase().as_str() {
        "administrator" | "admin" => Some(AdminRole::Administrator),
        "viewer" => Some(AdminRole::Viewer),
        _ => None,
    }
}

fn forbidden() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(serde_json::json!({ "code": "FORBIDDEN" })),
    )
        .into_response()
}

fn name_pwd_error() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({
            "code": "NAME_PWD_ERROR",
            "message": "Bad username or password"
        })),
    )
        .into_response()
}

/// Map an `AdminUsers` error onto its HTTP status.
fn admin_error(error: AdminUserError) -> Response {
    let (status, code) = match error {
        AdminUserError::NotFound => (StatusCode::NOT_FOUND, "NOT_FOUND"),
        AdminUserError::AlreadyExists => (StatusCode::CONFLICT, "ALREADY_EXISTS"),
        AdminUserError::Persist(_) => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR"),
        AdminUserError::InvalidUsername
        | AdminUserError::WeakPassword
        | AdminUserError::SameAsOld
        | AdminUserError::LastAdministrator => (StatusCode::BAD_REQUEST, "BAD_REQUEST"),
    };
    (
        status,
        Json(serde_json::json!({ "code": code, "message": error.to_string() })),
    )
        .into_response()
}

/// Map an MQTT users/ACL registry failure onto a 500: the in-memory
/// mutation applied but the commit or atomic save failed, so the loss
/// must never be silent.
fn mqtt_persist_error(error: broker_config::ConfigError) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({ "code": "INTERNAL_ERROR", "message": error.to_string() })),
    )
        .into_response()
}

/// Resolve a bearer token through the token store: token lookup, then a
/// fresh load of the user from the store. Tokens of deleted users are
/// revoked and rejected. The returned [`TokenInfo`] carries the current
/// role and `must_change_password` so the middleware needs no second
/// `admin_users` lookup.
pub(crate) fn resolve_bearer(state: &ApiState, token: &str) -> Option<TokenInfo> {
    let info = state.tokens.lookup(token)?;
    let view = match state.admin_users.get(&info.username) {
        Some(view) => view,
        None => {
            state.tokens.revoke(token);
            return None;
        }
    };
    Some(TokenInfo {
        username: info.username,
        role: view.role,
        expires_at: info.expires_at,
        must_change_password: view.must_change_password,
    })
}

/// Resolve `Authorization: Bearer <token>` through the token store,
/// for reuse by SEC-C middleware. Roles are always read fresh from the
/// admin-user store; tokens of deleted users are revoked and rejected.
pub(crate) fn authenticated_user(state: &ApiState, headers: &HeaderMap) -> Option<TokenInfo> {
    match crate::api_auth::parse_credentials(headers)? {
        crate::api_auth::Credentials::Bearer(token) => resolve_bearer(state, &token),
        crate::api_auth::Credentials::Basic { .. } => None,
    }
}

/// Extract the bearer token from already-parsed credentials, if any.
pub(crate) fn bearer_token_from_credentials(headers: &HeaderMap) -> Option<String> {
    match crate::api_auth::parse_credentials(headers)? {
        crate::api_auth::Credentials::Bearer(token) => Some(token),
        crate::api_auth::Credentials::Basic { .. } => None,
    }
}

#[derive(Deserialize)]
pub struct ScramChallengeRequest {
    pub username: String,
    pub client_nonce: String,
}

#[derive(Serialize)]
pub struct ScramChallengeResponse {
    pub mechanism: &'static str,
    pub challenge_id: String,
    pub server_nonce: String,
    pub salt: String,
    pub iterations: u32,
}

pub async fn scram_challenge(
    State(state): State<ApiState>,
    Json(req): Json<ScramChallengeRequest>,
) -> Response {
    let mut rng = rand::rng();
    let mut server_nonce_bytes = [0u8; 24];
    rng.fill_bytes(&mut server_nonce_bytes);
    let server_nonce = BASE64
        .encode(server_nonce_bytes)
        .replace('+', "-")
        .replace('/', "_")
        .replace('=', "");

    // The stored salt (or a deterministic fake salt for unknown users so
    // names cannot be enumerated). Challenge id and server nonce stay
    // random per handshake.
    let (salt, iterations) = state.admin_users.scram_params(&req.username);
    let salt_b64 = BASE64.encode(&salt);

    let mut challenge_id_bytes = [0u8; 24];
    rng.fill_bytes(&mut challenge_id_bytes);
    let challenge_id = BASE64
        .encode(challenge_id_bytes)
        .replace('+', "-")
        .replace('/', "_")
        .replace('=', "");

    let inserted = state.tokens.insert_challenge(
        challenge_id.clone(),
        ScramChallengeState {
            username: req.username.clone(),
            client_nonce: req.client_nonce,
            _server_nonce: server_nonce.clone(),
            salt,
            iterations,
            created_at: Instant::now(),
        },
    );
    if !inserted {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(serde_json::json!({
                "code": "TOO_MANY_REQUESTS",
                "message": "too many pending login challenges"
            })),
        )
            .into_response();
    }

    (
        StatusCode::OK,
        Json(ScramChallengeResponse {
            mechanism: "SCRAM-SHA-256",
            challenge_id,
            server_nonce,
            salt: salt_b64,
            iterations,
        }),
    )
        .into_response()
}

#[derive(Deserialize)]
pub struct ScramVerifyRequest {
    pub challenge_id: String,
    pub combined_nonce: String,
    pub client_proof: String,
    #[serde(default)]
    pub mfa_token: Option<String>,
}

#[derive(Serialize)]
pub struct LoginResponse {
    pub token: String,
    pub server_signature: String,
    pub role: String,
    pub must_change_password: bool,
}

pub async fn scram_verify(
    State(state): State<ApiState>,
    Json(req): Json<ScramVerifyRequest>,
) -> Response {
    let ch = {
        let challenge = state.tokens.take_challenge(&req.challenge_id);
        let Some(challenge) = challenge else {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "code": "SCRAM_CHALLENGE_INVALID",
                    "message": "Challenge expired or invalid"
                })),
            )
                .into_response();
        };
        challenge
    };

    let client_proof_bytes = match BASE64.decode(&req.client_proof) {
        Ok(bytes) => bytes,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "code": "BAD_REQUEST",
                    "message": "Invalid client_proof encoding"
                })),
            )
                .into_response();
        }
    };

    let escaped_user = ch.username.replace('=', "=3D").replace(',', "=2C");
    let salt_b64 = BASE64.encode(&ch.salt);
    let auth_message = format!(
        "n={},r={},r={},s={},i={},c=biws,r={}",
        escaped_user,
        ch.client_nonce,
        req.combined_nonce,
        salt_b64,
        ch.iterations,
        req.combined_nonce
    );

    // Verify against the stored SCRAM credentials. Unknown users fail here:
    // `scram_verify` returns `None` when the user does not exist. Login
    // never creates or modifies users.
    let server_sig =
        match state
            .admin_users
            .scram_verify(&ch.username, &auth_message, &client_proof_bytes)
        {
            Some(sig) => sig,
            None => {
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(serde_json::json!({
                        "code": "NAME_PWD_ERROR",
                        "message": "Invalid credentials"
                    })),
                )
                    .into_response();
            }
        };
    let server_sig_b64 = BASE64.encode(server_sig);

    let Some(view) = state.admin_users.get(&ch.username) else {
        return name_pwd_error();
    };
    let role = view.role;
    let must_change_password = view.must_change_password;
    let token = state.tokens.issue(&ch.username, role);

    (
        StatusCode::OK,
        Json(LoginResponse {
            token,
            server_signature: server_sig_b64,
            role: role_str(&role).to_string(),
            must_change_password,
        }),
    )
        .into_response()
}

#[derive(Deserialize)]
pub struct DirectLoginRequest {
    pub username: String,
    pub password: String,
}

/// `POST /login`: verify dashboard credentials against the real
/// admin-user store and, on success, issue a live bearer token.
///
/// Only the admin-user store decides (constant-time password comparison
/// in `AdminUsers::verify_password`, including a dummy derivation for
/// unknown users so names cannot be enumerated). The default
/// `admin`/`public` already lives there; there is no shortcut and no
/// MQTT-store fallback. A successful login mints one entry in the
/// bounded live-token map (`MAX_LIVE_TOKENS`, `TOKEN_TTL`); a failed
/// login creates nothing. Tokens are revoked by logout and by password
/// change, and expire on lookup. The route is not paged and sets no
/// cookies: the token travels in the JSON body only.
/// Management-plane only: one short store read plus one token-map lock
/// per call; publish and deliver never touch either store.
// TODO(parity): is `NAME_PWD_ERROR` the documented failure code here or
// is it `BAD_USERNAME_OR_PWD` (the shared error module names the latter
// as the dashboard login code)? The rulebook does not decide the exact
// code name; the current choice keeps `NAME_PWD_ERROR` (401, documented
// `{code, message}` shape) until the checker pins it.
pub async fn direct_login(State(state): State<ApiState>, body: Bytes) -> Response {
    let req: DirectLoginRequest = match serde_json::from_slice(&body) {
        Ok(req) => req,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid login body: {error}")).into_response();
        }
    };
    // TODO(parity): are empty credentials malformed (400) or wrong (401)?
    // The rulebook does not decide the empty boundary; the current choice
    // rejects empty the same way as a missing field (400, fail closed,
    // creates nothing) until the checker pins it.
    if req.username.trim().is_empty() || req.password.is_empty() {
        return ApiError::BadRequest("username and password must not be empty".to_string())
            .into_response();
    }
    if !state
        .admin_users
        .verify_password(&req.username, &req.password)
    {
        return name_pwd_error();
    }

    let Some(view) = state.admin_users.get(&req.username) else {
        return name_pwd_error();
    };
    let role = view.role;
    let must_change_password = view.must_change_password;
    let token = state.tokens.issue(&req.username, role);

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "token": token,
            "role": role_str(&role),
            "must_change_password": must_change_password
        })),
    )
        .into_response()
}

/// `POST /logout`: revoke the caller's live bearer token.
///
/// The bearer token from `Authorization: Bearer <token>` is looked up in
/// the bounded live-token map (`MAX_LIVE_TOKENS`, `TOKEN_TTL` from the
/// login task); a live entry is removed and the call answers 204 with no
/// body. A missing header, an unknown token, an expired token, or a token
/// whose user was deleted is a client error: 401 with the documented
/// `{code, message}` shape, never success. Revoked entries are removed,
/// not retained, so logout never grows the map toward its cap. The route
/// is not paged and takes no parameters; any body or query is ignored.
/// Management-plane only: one short token-map lock per call (plus the
/// admin-user read inside the liveness check); publish and deliver never
/// touch either store.
// TODO(parity): is the documented failure code for a missing or
// already-revoked logout token `UNAUTHORIZED` or a dedicated code? The
// rulebook does not decide the exact code name; the current choice keeps
// `UNAUTHORIZED` (401, documented `{code, message}` shape, matching the
// auth middleware) until the checker pins it.
// TODO(parity): does the documented logout require a username body that
// must match the token owner, or is the bearer token alone sufficient?
// The rulebook does not decide the body contract; the current choice
// revokes by bearer alone and ignores any body until the checker pins it.
pub async fn logout(State(state): State<ApiState>, headers: HeaderMap) -> Response {
    let Some(token) = bearer_token_from_credentials(&headers) else {
        return ApiError::Unauthorized("missing or invalid credentials".to_string())
            .into_response();
    };
    // Liveness check through the same resolver the middleware uses:
    // unknown, expired, or deleted-user tokens are already gone (expired
    // entries removed, deleted-user tokens revoked) and must not report
    // success.
    if resolve_bearer(&state, &token).is_none() {
        return ApiError::Unauthorized("missing or invalid credentials".to_string())
            .into_response();
    }
    state.tokens.revoke(&token);
    StatusCode::NO_CONTENT.into_response()
}

pub async fn current_user(
    State(state): State<ApiState>,
    Extension(info): Extension<TokenInfo>,
) -> Response {
    // The middleware guarantees a valid token; `info` already carries the
    // fresh role. Re-read the remaining profile fields.
    let Some(view) = state.admin_users.get(&info.username) else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "code": "UNAUTHORIZED" })),
        )
            .into_response();
    };
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "username": view.username,
            "role": role_str(&view.role).to_string(),
            "must_change_password": view.must_change_password,
        })),
    )
        .into_response()
}

/// One entry of the fixed dashboard-user scope catalogue: the stable
/// scope name, a short operator-facing summary, and whether only the
/// administrator role may hold it.
struct UserScopeEntry {
    name: &'static str,
    desc: &'static str,
    admin_only: bool,
}

/// Fixed catalogue of scopes a dashboard login user may hold (C05-24).
///
/// Bound (stated here and enforced by construction): exactly fourteen
/// entries, no growth; the list lives in code, so there is no
/// persistence beyond it and no per-request growth. Each summary is a
/// short static string, so serving the catalogue allocates only the
/// small bounded response body once per request.
///
/// The first ten are the generic scopes shared with API keys (held by
/// either role); the last four are login-only scopes restricted to the
/// administrator role. This is the single registry the dashboard-user
/// scope validation shares with the catalogue route:
/// [`is_known_user_scope`] and [`role_may_hold_scope`] consult this
/// same list. Vocabulary enforcement on the user write paths (rejecting
/// unknown names, refusing non-administrators the login-only scopes)
/// is owned by the admin-users lane; the catalogue route serves the
/// list verbatim regardless of which scopes any stored user holds.
// TODO(parity): what are the exact operator-facing summaries per
// scope? The rulebook does not decide the wording; the current choice
// describes each scope's area in our own words and keeps the stable
// scope names (protocol values) exact until the checker pins them.
// TODO(parity): does the documented user create/update reject scope
// lists that mix administrator-equivalent scopes with restricted ones,
// or only enforce the per-scope administrator-only flag? The rulebook
// does not decide the combination rule; the current choice records the
// per-scope flag in the catalogue and checks only per-scope role
// compatibility (`role_may_hold_scope`), leaving combination refusal
// to the write-path lane until the checker pins it.
const USER_SCOPES: [UserScopeEntry; 14] = [
    UserScopeEntry {
        name: "connections",
        desc: "View and manage client connections, subscriptions and topics",
        admin_only: false,
    },
    UserScopeEntry {
        name: "publish",
        desc: "Publish messages through the management API",
        admin_only: false,
    },
    UserScopeEntry {
        name: "data_integration",
        desc: "Manage rules, connectors and data integrations",
        admin_only: false,
    },
    UserScopeEntry {
        name: "access_control",
        desc: "Manage client authentication and authorization settings",
        admin_only: false,
    },
    UserScopeEntry {
        name: "gateways",
        desc: "Manage protocol gateways and their clients",
        admin_only: false,
    },
    UserScopeEntry {
        name: "monitoring",
        desc: "Read metrics, alarms, traces and monitoring data",
        admin_only: false,
    },
    UserScopeEntry {
        name: "cluster_operations",
        desc: "Manage cluster membership and node operations",
        admin_only: false,
    },
    UserScopeEntry {
        name: "system",
        desc: "Manage core configuration, listeners and system settings",
        admin_only: false,
    },
    UserScopeEntry {
        name: "audit",
        desc: "Query the audit log",
        admin_only: false,
    },
    UserScopeEntry {
        name: "license",
        desc: "Manage licence installation and status",
        admin_only: false,
    },
    UserScopeEntry {
        name: "user_management",
        desc: "Manage dashboard users and their scopes",
        admin_only: true,
    },
    UserScopeEntry {
        name: "mfa_management",
        desc: "Manage other users' multi-factor authentication",
        admin_only: true,
    },
    UserScopeEntry {
        name: "sso_management",
        desc: "Manage single sign-on backends and settings",
        admin_only: true,
    },
    UserScopeEntry {
        name: "api_key_management",
        desc: "Manage API keys and their scopes",
        admin_only: true,
    },
];

/// Whether `name` is one of the catalogue scopes a dashboard login
/// user may hold.
///
/// Shared by the dashboard-user create/update validation path and the
/// catalogue route so both read the same registry. Advisory for now:
/// enforcement stays with the admin-users write-path lane (see the
/// module TODO); the catalogue route serves this list regardless.
pub fn is_known_user_scope(name: &str) -> bool {
    USER_SCOPES.iter().any(|entry| entry.name == name)
}

/// Whether `role` may hold `scope` under the catalogue's
/// administrator-only flags: viewers hold only the generic scopes,
/// administrators hold any catalogue scope. Unknown names fail closed
/// (false) so validation never grants what the catalogue does not
/// list. Advisory for now, shared with the catalogue route through
/// [`USER_SCOPES`]; enforcement on the write paths stays with the
/// admin-users lane until the checker pins it.
pub fn role_may_hold_scope(role: &AdminRole, scope: &str) -> bool {
    match USER_SCOPES.iter().find(|entry| entry.name == scope) {
        Some(entry) => !entry.admin_only || *role == AdminRole::Administrator,
        None => false,
    }
}

/// `GET /user_scopes`: fixed catalogue of scopes a dashboard login
/// user may hold (C05-24).
///
/// Returns `{"scopes": [{"name", "desc", "admin_only"}]}` in catalogue
/// order, served from the same [`USER_SCOPES`] registry that the
/// dashboard-user validation predicates consult. The route is not
/// paged and takes no parameters; unknown query parameters are ignored
/// (the handler takes no extractor, so Axum never parses them).
/// Management-plane only: reads a code-defined list, takes no lock,
/// and publish/deliver never touch it.
pub async fn user_scopes() -> Response {
    let scopes: Vec<serde_json::Value> = USER_SCOPES
        .iter()
        .map(|entry| {
            serde_json::json!({
                "name": entry.name,
                "desc": entry.desc,
                "admin_only": entry.admin_only,
            })
        })
        .collect();
    (
        StatusCode::OK,
        Json(serde_json::json!({ "scopes": scopes })),
    )
        .into_response()
}

/// Dashboard/API users live in the admin-user store only. MQTT client
/// credentials stay in `MemoryAuth` behind the `/authentication/.../users`
/// and v1 `/auth/users` endpoints.
pub async fn list_users(State(state): State<ApiState>) -> Response {
    let users: Vec<_> = state
        .admin_users
        .list()
        .into_iter()
        .map(|u| {
            serde_json::json!({
                "username": u.username,
                "role": role_str(&u.role),
                "description": u.description,
            })
        })
        .collect();
    (StatusCode::OK, Json(users)).into_response()
}

#[derive(Deserialize)]
pub struct CreateUserReq {
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

pub async fn create_user(
    State(state): State<ApiState>,
    Json(req): Json<CreateUserReq>,
) -> Response {
    // Authentication and the administrator role are enforced by the
    // middleware; the handler only validates the payload.
    let role = match req.role.as_deref() {
        None => AdminRole::Administrator,
        Some(raw) => match parse_role(raw) {
            Some(role) => role,
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "code": "BAD_REQUEST",
                        "message": "role must be administrator or viewer"
                    })),
                )
                    .into_response();
            }
        },
    };
    let description = req.description.unwrap_or_default();
    if let Err(error) = state
        .admin_users
        .create(&req.username, &req.password, role, &description)
    {
        return admin_error(error);
    }
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "username": req.username,
            "role": role_str(&role),
            "description": description
        })),
    )
        .into_response()
}

#[derive(Deserialize)]
pub struct UpdateUserReq {
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

pub async fn update_user(
    State(state): State<ApiState>,
    Path(username): Path<String>,
    Json(req): Json<UpdateUserReq>,
) -> Response {
    if let Some(raw) = req.role.as_deref() {
        let role = match parse_role(raw) {
            Some(role) => role,
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "code": "BAD_REQUEST",
                        "message": "role must be administrator or viewer"
                    })),
                )
                    .into_response();
            }
        };
        if let Err(error) = state.admin_users.set_role(&username, role) {
            return admin_error(error);
        }
    }
    if let Some(description) = req.description.as_deref() {
        if let Err(error) = state.admin_users.set_description(&username, description) {
            return admin_error(error);
        }
    }
    match state.admin_users.get(&username) {
        Some(view) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "username": view.username,
                "role": role_str(&view.role),
                "description": view.description
            })),
        )
            .into_response(),
        None => admin_error(AdminUserError::NotFound),
    }
}

pub async fn delete_user(State(state): State<ApiState>, Path(username): Path<String>) -> Response {
    match state.admin_users.delete(&username) {
        Ok(()) => {
            state.tokens.revoke_user_except(&username, None);
            StatusCode::NO_CONTENT.into_response()
        }
        Err(error) => admin_error(error),
    }
}

#[derive(Deserialize)]
pub struct ChangePasswordReq {
    #[serde(default)]
    pub old_pwd: Option<String>,
    #[serde(default)]
    pub new_pwd: Option<String>,
}

/// `PUT /users/:username/change_pwd {old_pwd, new_pwd}` (compatible with the documented API).
/// Administrators may change any user's password; viewers only their own.
/// `old_pwd` is required when changing your own password. The middleware
/// guarantees a valid token; the self-or-administrator rule stays here.
pub async fn change_user_password(
    State(state): State<ApiState>,
    Extension(caller): Extension<TokenInfo>,
    headers: HeaderMap,
    Path(username): Path<String>,
    Json(req): Json<ChangePasswordReq>,
) -> Response {
    let is_self = caller.username == username;
    if !is_self && caller.role != AdminRole::Administrator {
        return forbidden();
    }
    if state.admin_users.get(&username).is_none() {
        return admin_error(AdminUserError::NotFound);
    }
    if is_self {
        let Some(old_pwd) = req.old_pwd.as_deref() else {
            return name_pwd_error();
        };
        if !state.admin_users.verify_password(&username, old_pwd) {
            return name_pwd_error();
        }
    }
    let Some(new_pwd) = req.new_pwd.as_deref() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "BAD_REQUEST",
                "message": "new_pwd must not be empty"
            })),
        )
            .into_response();
    };
    if let Err(error) = state.admin_users.change_password(&username, new_pwd) {
        return admin_error(error);
    }
    // Revoke that user's other tokens; the caller's token keeps working.
    state.tokens.revoke_user_except(
        &username,
        bearer_token_from_credentials(&headers).as_deref(),
    );
    StatusCode::NO_CONTENT.into_response()
}

// ---------------------------------------------------------------------------
// Authentication (AuthN) built-in-database users (backed by `MemoryAuth`).
// ---------------------------------------------------------------------------

/// `GET /authentication/{id}/users`: paged users of one authenticator.
///
/// Reads the live built-in user map (`MemoryAuth::usernames`, sorted) and
/// slices the requested page with the shared [`PageParams`] helper
/// (defaults `page=1`, `limit=100`, clamped to `1..=1000`; malformed
/// values fall back to defaults, never 400). An empty map reads as an
/// empty page, never an error. Unknown authenticator ids fail with
/// `NOT_FOUND` in the documented `{code, message}` shape; a known id
/// whose backend is not the built-in database fails with `BAD_REQUEST`
/// (stored-only backends never execute). Passwords are write-only and
/// never rendered: each row carries only `user_id`.
/// Management-plane only: clones at most the sorted username list once
/// per request under short read locks; the CONNECT path keeps reading
/// under its own short locks (`MemoryAuth::authenticate` in
/// `crates/broker-auth/src/lib.rs`, driven by CONNECT in
/// `crates/broker-node/src/main.rs`) and publish/deliver never touch
/// this store. The map is bounded by [`MAX_AUTHN_USERS`] (100,000
/// entries: each entry holds a verifier plus map overhead, so an
/// unbounded map lets one management batch exhaust the kernel; 100,000
/// covers fleet provisioning while keeping list reads paged and
/// per-CONNECT lookups cheap).
// TODO(parity): which per-user fields beyond `user_id` does the listing
// require (superuser flag, creation time)? The rulebook does not decide
// the exact row shape; fields the broker does not track are omitted
// rather than invented.
pub async fn list_authn_users(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    params: PageParams,
) -> Response {
    let Some(entry) = state.authn_chain.get(&id) else {
        return ApiError::NotFound(format!("authenticator {id:?} not found")).into_response();
    };
    if entry.backend != "built_in_database" {
        return ApiError::BadRequest(format!(
            "authenticator {id:?} uses backend {:?}, which cannot serve the built-in user map",
            entry.backend
        ))
        .into_response();
    }
    let names = state.auth.usernames();
    let total = names.len();
    let page_items = paginate(&names, params.page, params.limit);
    let data: Vec<_> = page_items
        .iter()
        .map(|name| {
            serde_json::json!({
                "user_id": name
            })
        })
        .collect();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "data": data,
            "meta": meta_page(params.page, params.limit, total),
        })),
    )
        .into_response()
}

/// `POST /authentication/{id}/users`: add one user to the live map.
///
/// The body must be a JSON object with non-empty string `user_id` (at
/// most [`MAX_AUTHN_USERNAME_LEN`] bytes) and `password` (non-empty, at
/// most [`MAX_PASSWORD_BYTES`] bytes); anything else fails with
/// `BAD_REQUEST` in the documented `{code, message}` shape. A `user_id`
/// that already exists fails with `ALREADY_EXISTS` instead of updating
/// in place; a map that already holds [`MAX_AUTHN_USERS`] entries fails
/// with `BAD_REQUEST`. Unknown authenticator ids fail with `NOT_FOUND`;
/// a known non-built-in backend fails with `BAD_REQUEST`. On success
/// returns 201 with the stored `user_id` (never the password). The
/// password is hashed under the configured policy before storing;
/// plaintext never persists. The write persists across a restart through
/// the config registry.
/// Management-plane only: one short existence check plus the user-map
/// write lock (plus the registry save lock on persist); the CONNECT path
/// keeps reading under short read locks and publish/deliver never touch
/// this store.
// TODO(parity): is a duplicate `user_id` rejected or an overwrite, and
// which status (400 versus 409) does the documented `ALREADY_EXISTS`
// code carry? The rulebook does not decide the exact duplicate
// semantics; the current choice rejects with `ALREADY_EXISTS` (400 in
// this tree's shared error mapping) until the checker pins it.
pub async fn create_authn_user(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    body: Bytes,
) -> Response {
    let Some(entry) = state.authn_chain.get(&id) else {
        return ApiError::NotFound(format!("authenticator {id:?} not found")).into_response();
    };
    if entry.backend != "built_in_database" {
        return ApiError::BadRequest(format!(
            "authenticator {id:?} uses backend {:?}, which cannot serve the built-in user map",
            entry.backend
        ))
        .into_response();
    }
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return ApiError::BadRequest(format!("invalid user body: {e}")).into_response();
        }
    };
    let (user_id, password) = match parse_authn_user_body(&value) {
        Ok(pair) => pair,
        Err(msg) => return ApiError::BadRequest(msg).into_response(),
    };
    if state.auth.has_user(&user_id) {
        return ApiError::AlreadyExists(format!("user {user_id:?} already exists")).into_response();
    }
    if state.auth.user_count() >= MAX_AUTHN_USERS {
        return ApiError::BadRequest(format!(
            "user limit of {MAX_AUTHN_USERS} reached for authenticator {id:?}"
        ))
        .into_response();
    }
    if let Err(error) = state.auth.add_user(user_id.clone(), password.as_bytes()) {
        // Pre-validation above already rejected every client-side shape,
        // so only the hashing/oversize failures stay client errors here;
        // anything else is a persist failure and must never be silent.
        let message = error.to_string();
        if message.contains("exceeds") || message.contains("cannot be hashed") {
            return ApiError::BadRequest(message).into_response();
        }
        return mqtt_persist_error(error);
    }
    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "user_id": user_id
        })),
    )
        .into_response()
}

/// Validate a create-user body into `(user_id, password)`.
///
/// The body must be a JSON object with non-empty string `user_id` and
/// `password` fields within their bounds. Extra fields are ignored so
/// future record fields degrade to the same create instead of a 400.
fn parse_authn_user_body(value: &serde_json::Value) -> Result<(String, String), String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "user body must be a JSON object".to_string())?;
    let user_id = obj
        .get("user_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "field `user_id` is required".to_string())?;
    if user_id.trim().is_empty() {
        return Err("field `user_id` must not be empty".to_string());
    }
    if user_id.len() > MAX_AUTHN_USERNAME_LEN {
        return Err(format!(
            "field `user_id` {user_id:?} exceeds {MAX_AUTHN_USERNAME_LEN} bytes"
        ));
    }
    let password = obj
        .get("password")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "field `password` is required".to_string())?;
    if password.is_empty() {
        return Err("field `password` must not be empty".to_string());
    }
    if password.len() > MAX_PASSWORD_BYTES {
        return Err(format!(
            "field `password` exceeds {MAX_PASSWORD_BYTES} bytes"
        ));
    }
    Ok((user_id.to_string(), password.to_string()))
}

/// `GET /authentication/{id}/users/{user_id}`: read one user record.
///
/// Returns the stored `user_id` without credential secrets, or 404 with
/// the documented `{code, message}` shape when either the authenticator
/// id or the user id is unknown; a known non-built-in backend fails with
/// `BAD_REQUEST` (stored-only backends never execute). The route is not
/// paged. Passwords are write-only and never rendered.
/// Management-plane only: at most two short read locks per request (chain
/// entry plus user-map presence); the CONNECT path keeps reading under
/// its own short locks (`MemoryAuth::authenticate` in
/// `crates/broker-auth/src/lib.rs`, driven by CONNECT in
/// `crates/broker-node/src/main.rs`) and publish/deliver never touch
/// this store.
// TODO(parity): which per-user fields beyond `user_id` does the detail
// require (superuser flag, creation time)? The rulebook does not decide
// the exact record shape; fields the broker does not track are omitted
// rather than invented.
pub async fn get_authn_user(
    State(state): State<ApiState>,
    Path((id, user_id)): Path<(String, String)>,
) -> Response {
    let Some(entry) = state.authn_chain.get(&id) else {
        return ApiError::NotFound(format!("authenticator {id:?} not found")).into_response();
    };
    if entry.backend != "built_in_database" {
        return ApiError::BadRequest(format!(
            "authenticator {id:?} uses backend {:?}, which cannot serve the built-in user map",
            entry.backend
        ))
        .into_response();
    }
    if !state.auth.has_user(&user_id) {
        return ApiError::NotFound(format!("user {user_id:?} not found")).into_response();
    }
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "user_id": user_id
        })),
    )
        .into_response()
}

/// `PUT /authentication/{id}/users/{user_id}`: replace one user's credential.
///
/// The body must be a JSON object with a non-empty string `password`
/// (at most [`MAX_PASSWORD_BYTES`] bytes); anything else fails with
/// `BAD_REQUEST` in the documented `{code, message}` shape and applies
/// nothing. Unknown authenticator ids and unknown user ids both fail
/// with `NOT_FOUND` and create nothing; a known non-built-in backend
/// fails with `BAD_REQUEST`. On success returns 200 with the stored
/// `user_id` (never the password). The password is hashed under the
/// configured policy before storing; plaintext never persists. The write
/// persists across a restart through the config registry. The route is
/// not paged.
/// Management-plane only: one short existence check plus the user-map
/// write lock (plus the registry save lock on persist); the CONNECT path
/// keeps reading under short read locks and publish/deliver never touch
/// this store. The map is bounded by [`MAX_AUTHN_USERS`] (100,000
/// entries: each entry holds a verifier plus map overhead, so an
/// unbounded map lets one management batch exhaust the kernel); an
/// update replaces in place and never grows the map toward the cap.
pub async fn update_authn_user(
    State(state): State<ApiState>,
    Path((id, user_id)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let Some(entry) = state.authn_chain.get(&id) else {
        return ApiError::NotFound(format!("authenticator {id:?} not found")).into_response();
    };
    if entry.backend != "built_in_database" {
        return ApiError::BadRequest(format!(
            "authenticator {id:?} uses backend {:?}, which cannot serve the built-in user map",
            entry.backend
        ))
        .into_response();
    }
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return ApiError::BadRequest(format!("invalid user body: {e}")).into_response();
        }
    };
    let password = match parse_authn_password_body(&value) {
        Ok(password) => password,
        Err(msg) => return ApiError::BadRequest(msg).into_response(),
    };
    if !state.auth.has_user(&user_id) {
        return ApiError::NotFound(format!("user {user_id:?} not found")).into_response();
    }
    if let Err(error) = state.auth.add_user(user_id.clone(), password.as_bytes()) {
        // Pre-validation above already rejected every client-side shape,
        // so only the hashing/oversize failures stay client errors here;
        // anything else is a persist failure and must never be silent.
        let message = error.to_string();
        if message.contains("exceeds") || message.contains("cannot be hashed") {
            return ApiError::BadRequest(message).into_response();
        }
        return mqtt_persist_error(error);
    }
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "user_id": user_id
        })),
    )
        .into_response()
}

/// Validate an update-user body into its new password.
///
/// The body must be a JSON object with a non-empty string `password`
/// field within bounds. Extra fields are ignored so future record
/// fields degrade to the same update instead of a 400.
fn parse_authn_password_body(value: &serde_json::Value) -> Result<String, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "user body must be a JSON object".to_string())?;
    let password = obj
        .get("password")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "field `password` is required".to_string())?;
    if password.is_empty() {
        return Err("field `password` must not be empty".to_string());
    }
    if password.len() > MAX_PASSWORD_BYTES {
        return Err(format!(
            "field `password` exceeds {MAX_PASSWORD_BYTES} bytes"
        ));
    }
    Ok(password.to_string())
}

/// `DELETE /authentication/{id}/users/{user_id}`: remove one user.
///
/// Unknown authenticator ids and unknown user ids both fail with
/// `NOT_FOUND` in the documented `{code, message}` shape and apply
/// nothing; a known non-built-in backend fails with `BAD_REQUEST`. On
/// success returns 204 with no body. The removal persists across a
/// restart through the config registry.
/// Management-plane only: one short user-map write lock per request
/// (plus the registry save lock on persist); the CONNECT path keeps
/// reading under short read locks and publish/deliver never touch this
/// store.
pub async fn delete_authn_user(
    State(state): State<ApiState>,
    Path((id, user_id)): Path<(String, String)>,
) -> Response {
    let Some(entry) = state.authn_chain.get(&id) else {
        return ApiError::NotFound(format!("authenticator {id:?} not found")).into_response();
    };
    if entry.backend != "built_in_database" {
        return ApiError::BadRequest(format!(
            "authenticator {id:?} uses backend {:?}, which cannot serve the built-in user map",
            entry.backend
        ))
        .into_response();
    }
    match state.auth.remove_user(&user_id) {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => ApiError::NotFound(format!("user {user_id:?} not found")).into_response(),
        Err(error) => mqtt_persist_error(error),
    }
}

/// `POST /authentication/:id/import_users`: bulk import into the built-in
/// user map with validate-all-before-apply.
///
/// The body is a JSON array of `{user_id, password}` records (an object
/// carrying the array under `users` is accepted as well); every entry is
/// validated before anything is applied, so a malformed entry, a
/// duplicate `user_id` within the batch or against the existing map, or
/// a batch that would exceed [`broker_auth::MAX_AUTHN_USERS`] rejects
/// the whole batch and applies nothing. Unknown authenticator ids fail
/// with `NOT_FOUND` in the documented `{code, message}` shape.
/// Non-built-in backends are rejected with `BAD_REQUEST`: import targets
/// the built-in user map only, and no endpoint may claim a backend it
/// cannot execute.
// TODO(parity): which import body shapes (bare array versus `users`
// envelope versus file upload), which `type=plain|hash` query handling,
// and which duplicate semantics (reject versus overwrite/merge with
// per-entry counts) does the spec require? The rulebook does not decide
// the exact set; the current choice accepts both JSON shapes as plain
// passwords, rejects duplicates for the whole batch, and reports atomic
// counts until the checker pins it. Extra record fields (for example a
// superuser flag our store does not model) are ignored.
// TODO(parity): is the documented success shape the five-count result or
// a per-entry list? The rulebook does not decide the exact envelope; the
// current choice reports the atomic counts until the checker pins it.
///
/// Management-plane only: locks only the user map (plus the registry
/// save lock on persist), never the message path; the CONNECT path keeps
/// reading under short read locks and publish/deliver never touch this
/// store. The import persists across a restart through the config
/// registry. The route is not paged.
pub async fn import_authn_users(
    State(state): State<ApiState>,
    Path(id): Path<String>,
    body: Bytes,
) -> Response {
    let Some(entry) = state.authn_chain.get(&id) else {
        return ApiError::NotFound(format!("authenticator {id:?} not found")).into_response();
    };
    if entry.backend != "built_in_database" {
        return ApiError::BadRequest(format!(
            "authenticator {id:?} uses backend {:?}, which cannot be imported into the built-in user map",
            entry.backend
        ))
        .into_response();
    }
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return ApiError::BadRequest(format!("invalid user batch: {e}")).into_response();
        }
    };
    let items = match parse_import_batch(&value) {
        Ok(items) => items,
        Err(msg) => return ApiError::BadRequest(msg).into_response(),
    };
    let batch: Vec<(String, Vec<u8>)> = items
        .into_iter()
        .map(|(user_id, password)| (user_id, password.into_bytes()))
        .collect();
    match state.auth.import_users(batch) {
        Ok(summary) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "total": summary.total,
                "success": summary.success,
                "override": 0,
                "skipped": 0,
                "failed": 0,
            })),
        )
            .into_response(),
        Err(ImportUsersError::Malformed(msg) | ImportUsersError::TooMany(msg)) => {
            ApiError::BadRequest(msg).into_response()
        }
        Err(ImportUsersError::Duplicate(msg)) => ApiError::AlreadyExists(msg).into_response(),
        Err(ImportUsersError::Persist(msg)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot persist user batch: {msg}"),
            })),
        )
            .into_response(),
    }
}

/// Validate an import body into `(user_id, password)` pairs.
///
/// Accepts a bare JSON array or an object carrying the array under
/// `users`. Every element must be an object with non-empty string
/// `user_id` and `password` fields; anything else is malformed. Extra
/// fields are ignored so future record fields degrade to the same
/// import instead of a 400.
fn parse_import_batch(value: &serde_json::Value) -> Result<Vec<(String, String)>, String> {
    let items = if let Some(array) = value.as_array() {
        array
    } else if let Some(obj) = value.as_object() {
        match obj.get("users") {
            Some(array) if array.is_array() => array.as_array().expect("checked is array"),
            _ => {
                return Err(
                    "user batch must be a JSON array or an object with a `users` array".to_string(),
                );
            }
        }
    } else {
        return Err(
            "user batch must be a JSON array or an object with a `users` array".to_string(),
        );
    };
    let mut out = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let obj = item
            .as_object()
            .ok_or_else(|| format!("user batch[{index}] must be an object"))?;
        let user_id = obj
            .get("user_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("user batch[{index}].user_id is required"))?;
        if user_id.trim().is_empty() {
            return Err(format!("user batch[{index}].user_id must not be empty"));
        }
        let password = obj
            .get("password")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("user batch[{index}].password is required"))?;
        if password.is_empty() {
            return Err(format!("user batch[{index}].password must not be empty"));
        }
        out.push((user_id.to_string(), password.to_string()));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Authorization (AuthZ / ACL) rules (backed by `MemoryAuth`).
// ---------------------------------------------------------------------------

pub async fn list_authz_rules(
    State(state): State<ApiState>,
    Path(source_type): Path<String>,
) -> Response {
    // Unknown source types are 404 with the documented shape; they never
    // serve the built-in table (W2-17, C04-10: unknown types referenced
    // elsewhere return 404). Known types keep today's behaviour.
    if !broker_config::is_known_authz_source_type(&source_type) {
        return ApiError::NotFound(format!("authorization source {source_type:?} not found"))
            .into_response();
    }
    let rules = state.auth.acl_rules();
    let data: Vec<_> = rules
        .into_iter()
        .enumerate()
        .map(|(idx, r)| {
            serde_json::json!({
                "id": idx,
                "clientid": r.client_pattern,
                "action": match r.action {
                    AclAction::Publish => "publish",
                    AclAction::Subscribe => "subscribe",
                    AclAction::All => "all",
                },
                "topic": r.topic_pattern,
                "permission": if r.allow { "allow" } else { "deny" }
            })
        })
        .collect();

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "data": data,
            "meta": {
                "page": 1,
                "limit": 100,
                "count": data.len(),
                "hasnext": false
            }
        })),
    )
        .into_response()
}

#[derive(Deserialize)]
pub struct CreateAuthzRuleReq {
    pub clientid: Option<String>,
    pub action: String,
    pub topic: String,
    pub permission: String,
}

pub async fn create_authz_rule(
    State(state): State<ApiState>,
    Path(source_type): Path<String>,
    Json(req): Json<CreateAuthzRuleReq>,
) -> Response {
    // Unknown source types are 404 with the documented shape; they never
    // write the built-in table. Known types keep today's behaviour.
    if !broker_config::is_known_authz_source_type(&source_type) {
        return ApiError::NotFound(format!("authorization source {source_type:?} not found"))
            .into_response();
    }
    let client_pattern = req.clientid.unwrap_or_else(|| "*".to_string());
    let action = match AclAction::parse(&req.action) {
        Some(a) => a,
        None => AclAction::All,
    };
    let allow = req.permission.to_lowercase() == "allow";

    if let Err(error) = state
        .auth
        .add_rule(AclRule::new(client_pattern, action, req.topic, allow))
    {
        return mqtt_persist_error(error);
    }
    StatusCode::CREATED.into_response()
}

pub async fn delete_authz_rule(
    State(state): State<ApiState>,
    Path((source_type, index)): Path<(String, usize)>,
) -> Response {
    // Unknown source types are 404 with the documented shape; they never
    // touch the built-in table. Known types keep today's behaviour.
    if !broker_config::is_known_authz_source_type(&source_type) {
        return ApiError::NotFound(format!("authorization source {source_type:?} not found"))
            .into_response();
    }
    match state.auth.remove_rule(index) {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => mqtt_persist_error(error),
    }
}

#[cfg(test)]
mod authn_users_tests {
    use super::*;
    use broker_auth::{Authenticator, PasswordHashPolicy};
    use broker_rules::{BackpressurePolicy, RuleEngine};

    const CHAIN_ID: &str = "password_based:built_in_database";

    fn standalone_state() -> ApiState {
        let state = ApiState::standalone(std::sync::Arc::new(RuleEngine::new(
            16,
            BackpressurePolicy::DropOldest,
        )));
        // Fast hashing for tests only; production keeps the default policy.
        state.auth.set_policy(PasswordHashPolicy::for_tests());
        state
            .authn_chain
            .insert(broker_auth::AuthnEntry {
                id: CHAIN_ID.to_string(),
                mechanism: "password_based".to_string(),
                backend: "built_in_database".to_string(),
                enable: true,
                config: serde_json::Value::Null,
            })
            .expect("chain entry inserts");
        state
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

    #[tokio::test]
    async fn crud_lists_with_paging_rejects_duplicates_and_misses_404() {
        // Before this task the list ignored paging (fixed page 1/limit 50),
        // create updated in place instead of rejecting duplicates, and
        // delete answered 204 even for unknown users.
        let state = standalone_state();

        for (user_id, password) in [("w211-alice", "AliceSecret1!"), ("w211-bob", "BobSecret1!")] {
            let (status, created, _) = parts(
                create_authn_user(
                    State(state.clone()),
                    Path(CHAIN_ID.to_string()),
                    body(serde_json::json!({"user_id": user_id, "password": password})),
                )
                .await,
            )
            .await;
            assert_eq!(status, StatusCode::CREATED, "create {user_id}");
            assert_eq!(created["user_id"], serde_json::json!(user_id));
            assert!(created.get("password").is_none(), "password never rendered");
        }

        // List carries both users with the standard page envelope.
        let (status, listed, _) = parts(
            list_authn_users(
                State(state.clone()),
                Path(CHAIN_ID.to_string()),
                params(1, 100),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(listed["data"].as_array().expect("data array").len(), 2);
        assert_eq!(listed["meta"]["page"], serde_json::json!(1));
        assert_eq!(listed["meta"]["limit"], serde_json::json!(100));
        assert_eq!(listed["meta"]["count"], serde_json::json!(2));
        assert_eq!(listed["meta"]["hasnext"], serde_json::json!(false));
        let ids: Vec<&str> = listed["data"]
            .as_array()
            .expect("data array")
            .iter()
            .map(|row| row["user_id"].as_str().expect("row has user_id"))
            .collect();
        assert_eq!(ids, vec!["w211-alice", "w211-bob"]);

        // Paging slices after sorting: page 2 with limit 1 holds the second user.
        let (status, page2, _) = parts(
            list_authn_users(
                State(state.clone()),
                Path(CHAIN_ID.to_string()),
                params(2, 1),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(page2["data"].as_array().expect("data").len(), 1);
        assert_eq!(page2["data"][0]["user_id"], serde_json::json!("w211-bob"));
        assert_eq!(page2["meta"]["count"], serde_json::json!(2));
        assert_eq!(page2["meta"]["hasnext"], serde_json::json!(false));

        // Re-creating an existing user is rejected, never an in-place update.
        let (status, duplicate, _) = parts(
            create_authn_user(
                State(state.clone()),
                Path(CHAIN_ID.to_string()),
                body(serde_json::json!({"user_id": "w211-alice", "password": "Other1!"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(duplicate["code"], serde_json::json!("ALREADY_EXISTS"));
        assert!(duplicate["message"].is_string());

        // Malformed bodies are client errors.
        let (status, malformed, _) = parts(
            create_authn_user(
                State(state.clone()),
                Path(CHAIN_ID.to_string()),
                body(serde_json::json!({"user_id": "w211-nopw"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(malformed["code"], serde_json::json!("BAD_REQUEST"));

        // Delete one user; one is left.
        let gone = delete_authn_user(
            State(state.clone()),
            Path((CHAIN_ID.to_string(), "w211-bob".to_string())),
        )
        .await;
        assert_eq!(gone.status(), StatusCode::NO_CONTENT);
        let (status, listed, _) = parts(
            list_authn_users(
                State(state.clone()),
                Path(CHAIN_ID.to_string()),
                params(1, 100),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(listed["data"].as_array().expect("data").len(), 1);
        assert_eq!(
            listed["data"][0]["user_id"],
            serde_json::json!("w211-alice")
        );

        // Deleting an unknown user id is a 404 with the documented shape.
        let (status, missing, _) = parts(
            delete_authn_user(
                State(state.clone()),
                Path((CHAIN_ID.to_string(), "w211-ghost".to_string())),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], serde_json::json!("NOT_FOUND"));

        // Unknown authenticator ids are 404s on every route.
        let (status, unknown, _) = parts(
            list_authn_users(
                State(state.clone()),
                Path("password_based:nowhere".to_string()),
                params(1, 100),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(unknown["code"], serde_json::json!("NOT_FOUND"));
        let (status, unknown, _) = parts(
            create_authn_user(
                State(state.clone()),
                Path("password_based:nowhere".to_string()),
                body(serde_json::json!({"user_id": "w211-x", "password": "X1!"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(unknown["code"], serde_json::json!("NOT_FOUND"));
        let (status, unknown, _) = parts(
            delete_authn_user(
                State(state.clone()),
                Path((
                    "password_based:nowhere".to_string(),
                    "w211-alice".to_string(),
                )),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(unknown["code"], serde_json::json!("NOT_FOUND"));
    }

    #[tokio::test]
    async fn live_user_map_drives_the_connect_path() {
        // Goes through the broker, not only the store: the created
        // credential must authenticate on the CONNECT path
        // (`MemoryAuth::authenticate`, driven by CONNECT in
        // `crates/broker-node/src/main.rs`), and must stop working after
        // the delete.
        let state = standalone_state();
        for (user_id, password) in [
            ("w211-conn", "ConnSecret1!"),
            // Keeper: the store is open while empty
            // (`MemoryAuth::authenticate` allows everything with no users,
            // and the broker only closes anonymous once users exist), so
            // one user must remain for the deleted credential to fail
            // closed below.
            ("w211-keeper", "KeeperSecret1!"),
        ] {
            let (status, _, _) = parts(
                create_authn_user(
                    State(state.clone()),
                    Path(CHAIN_ID.to_string()),
                    body(serde_json::json!({"user_id": user_id, "password": password})),
                )
                .await,
            )
            .await;
            assert_eq!(status, StatusCode::CREATED, "create {user_id}");
        }

        state
            .auth
            .authenticate("w211-client", Some("w211-conn"), Some(b"ConnSecret1!"))
            .await
            .expect("correct password authenticates on the CONNECT path");
        assert!(
            state
                .auth
                .authenticate("w211-client", Some("w211-conn"), Some(b"wrong"))
                .await
                .is_err(),
            "wrong password fails closed"
        );

        let gone = delete_authn_user(
            State(state.clone()),
            Path((CHAIN_ID.to_string(), "w211-conn".to_string())),
        )
        .await;
        assert_eq!(gone.status(), StatusCode::NO_CONTENT);
        assert!(
            state
                .auth
                .authenticate("w211-client", Some("w211-conn"), Some(b"ConnSecret1!"))
                .await
                .is_err(),
            "deleted user fails closed on the CONNECT path"
        );
        state
            .auth
            .authenticate("w211-client", Some("w211-keeper"), Some(b"KeeperSecret1!"))
            .await
            .expect("remaining user still authenticates on the CONNECT path");
    }

    #[tokio::test]
    async fn passwords_are_hashed_never_rendered_and_survive_a_restart() {
        let state = standalone_state();
        let (status, _, _) = parts(
            create_authn_user(
                State(state.clone()),
                Path(CHAIN_ID.to_string()),
                body(serde_json::json!({"user_id": "w211-hash", "password": "HashSecret1!"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);

        // The stored verifier is a hash, never the plaintext.
        let verifier = state
            .auth
            .verifier_string("w211-hash")
            .expect("created user has a verifier");
        assert!(
            !verifier.contains("HashSecret1!"),
            "no plaintext in the store"
        );

        // No list rendering carries password material.
        let (_, listed, raw) = parts(
            list_authn_users(
                State(state.clone()),
                Path(CHAIN_ID.to_string()),
                params(1, 100),
            )
            .await,
        )
        .await;
        assert!(listed.get("password").is_none());
        for row in listed["data"].as_array().expect("data array") {
            assert!(row.get("password").is_none(), "row renders no password");
            assert!(row.get("password_hash").is_none(), "row renders no hash");
        }
        assert!(
            !String::from_utf8_lossy(&raw).contains("HashSecret1!"),
            "list body carries no plaintext"
        );

        // Persistence rides the config registry: a fresh registry load
        // from the same directory still holds the user, so the credential
        // survives a kernel restart.
        let dir = std::env::temp_dir().join(format!(
            "indramqtt-w211-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let registry = std::sync::Arc::new(
            broker_config::ConfigRegistry::load(&dir).expect("scratch registry loads"),
        );
        let durable = broker_auth::MemoryAuth::from_registry(&registry);
        durable.set_policy(PasswordHashPolicy::for_tests());
        durable
            .add_user("w211-kept", b"KeptSecret1!")
            .expect("registry-backed add persists");
        drop(durable);
        let reloaded =
            broker_config::ConfigRegistry::load(&dir).expect("registry reloads after a restart");
        let found = reloaded
            .snapshot()
            .mqtt_users
            .users
            .iter()
            .any(|u| u.username == "w211-kept");
        assert!(found, "user persists across a restart");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn non_built_in_backend_is_rejected_not_executed() {
        let state = standalone_state();
        state
            .authn_chain
            .insert(broker_auth::AuthnEntry {
                id: "password_based:mysql".to_string(),
                mechanism: "password_based".to_string(),
                backend: "mysql".to_string(),
                enable: true,
                config: serde_json::Value::Null,
            })
            .expect("non-built-in entry inserts");
        let (status, rejected, _) = parts(
            list_authn_users(
                State(state.clone()),
                Path("password_based:mysql".to_string()),
                params(1, 100),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(rejected["code"], serde_json::json!("BAD_REQUEST"));
        // TODO(parity): should non-built-in user routes be 400 or 404?
        // The rulebook does not decide the exact code; the current choice
        // is 400 (stored-only backends never execute) until the checker
        // pins it.
    }

    #[tokio::test]
    async fn detail_read_update_round_trip_through_connect_path() {
        // Fails before W2-12: the detail read had no route (404 on every
        // hit) and the update ignored the authenticator id, created
        // unknown users and accepted bodies without validation.
        let state = standalone_state();

        let (status, _, _) = parts(
            create_authn_user(
                State(state.clone()),
                Path(CHAIN_ID.to_string()),
                body(serde_json::json!({"user_id": "w212-detail", "password": "DetailSecret1!"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);

        // Read hits with the documented fields and no secrets.
        let (status, fetched, raw) = parts(
            get_authn_user(
                State(state.clone()),
                Path((CHAIN_ID.to_string(), "w212-detail".to_string())),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(fetched["user_id"], serde_json::json!("w212-detail"));
        assert!(
            fetched.get("password").is_none(),
            "detail renders no password"
        );
        assert!(
            fetched.get("password_hash").is_none(),
            "detail renders no hash"
        );
        assert!(
            !String::from_utf8_lossy(&raw).contains("DetailSecret1!"),
            "detail body carries no plaintext"
        );

        // Read misses are 404s with the documented shape, at both levels.
        let (status, missing, _) = parts(
            get_authn_user(
                State(state.clone()),
                Path((CHAIN_ID.to_string(), "w212-ghost".to_string())),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], serde_json::json!("NOT_FOUND"));
        let (status, missing, _) = parts(
            get_authn_user(
                State(state.clone()),
                Path((
                    "password_based:nowhere".to_string(),
                    "w212-detail".to_string(),
                )),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], serde_json::json!("NOT_FOUND"));

        // Update replaces the credential: the old secret stops working on
        // the CONNECT path and the new one authenticates.
        let (status, updated, _) = parts(
            update_authn_user(
                State(state.clone()),
                Path((CHAIN_ID.to_string(), "w212-detail".to_string())),
                body(serde_json::json!({"password": "DetailSecret2!"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(updated["user_id"], serde_json::json!("w212-detail"));
        assert!(
            updated.get("password").is_none(),
            "update renders no password"
        );
        state
            .auth
            .authenticate("w212-client", Some("w212-detail"), Some(b"DetailSecret2!"))
            .await
            .expect("new credential authenticates on the CONNECT path");
        assert!(
            state
                .auth
                .authenticate("w212-client", Some("w212-detail"), Some(b"DetailSecret1!"))
                .await
                .is_err(),
            "old credential fails closed after the update"
        );

        // Invalid updates are client errors and apply nothing: the current
        // credential keeps working.
        let (status, rejected, _) = parts(
            update_authn_user(
                State(state.clone()),
                Path((CHAIN_ID.to_string(), "w212-detail".to_string())),
                body(serde_json::json!({"user_id": "w212-detail"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(rejected["code"], serde_json::json!("BAD_REQUEST"));
        state
            .auth
            .authenticate("w212-client", Some("w212-detail"), Some(b"DetailSecret2!"))
            .await
            .expect("rejected update applies nothing");

        // Updating an unknown user creates nothing and returns 404.
        let (status, missing, _) = parts(
            update_authn_user(
                State(state.clone()),
                Path((CHAIN_ID.to_string(), "w212-ghost".to_string())),
                body(serde_json::json!({"password": "GhostSecret1!"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], serde_json::json!("NOT_FOUND"));
        assert!(
            !state.auth.has_user("w212-ghost"),
            "unknown update creates nothing"
        );

        // Unknown authenticator ids are 404s on both detail routes.
        let (status, missing, _) = parts(
            update_authn_user(
                State(state.clone()),
                Path((
                    "password_based:nowhere".to_string(),
                    "w212-detail".to_string(),
                )),
                body(serde_json::json!({"password": "DetailSecret3!"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(missing["code"], serde_json::json!("NOT_FOUND"));
    }
}

#[cfg(test)]
mod login_token_tests {
    use super::*;

    #[test]
    fn live_token_map_is_bounded() {
        // Before the bound the map grew without limit; now it holds at
        // most MAX_LIVE_TOKENS live entries no matter the login rate.
        let store = ApiTokens::new();
        for i in 0..(MAX_LIVE_TOKENS + 64) {
            store.issue(&format!("w229-user-{i}"), AdminRole::Viewer);
        }
        assert!(
            store.len() <= MAX_LIVE_TOKENS,
            "live-token map must stay bounded at {MAX_LIVE_TOKENS}, got {}",
            store.len()
        );
        assert_eq!(
            store.len(),
            MAX_LIVE_TOKENS,
            "a full map of live tokens keeps exactly the bound"
        );
    }

    #[test]
    fn issue_sweeps_expired_entries() {
        let store = ApiTokens::new();
        for i in 0..8 {
            store.issue_expired(&format!("w229-stale-{i}"), AdminRole::Viewer);
        }
        assert_eq!(store.len(), 8, "expired entries linger until swept");
        store.issue("w229-fresh", AdminRole::Administrator);
        assert_eq!(store.len(), 1, "the next issue sweeps every expired entry");
    }

    #[test]
    fn issued_tokens_carry_the_documented_ttl() {
        let store = ApiTokens::new();
        let token = store.issue("w229-ttl", AdminRole::Administrator);
        let info = store.lookup(&token).expect("fresh token looks up");
        let remaining = info.expires_at.saturating_duration_since(Instant::now());
        assert!(
            remaining <= TOKEN_TTL,
            "remaining TTL {remaining:?} must not exceed {TOKEN_TTL:?}"
        );
        assert!(
            remaining > TOKEN_TTL - Duration::from_secs(100),
            "remaining TTL {remaining:?} must sit at the documented {TOKEN_TTL:?}"
        );
    }
}

#[cfg(test)]
mod logout_tests {
    use super::*;
    use broker_rules::{BackpressurePolicy, RuleEngine};

    fn standalone_state() -> ApiState {
        ApiState::standalone(std::sync::Arc::new(RuleEngine::new(
            16,
            BackpressurePolicy::DropOldest,
        )))
    }

    fn bearer_headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Bearer {token}").parse().expect("header value"),
        );
        headers
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

    #[tokio::test]
    async fn logout_revokes_live_token_and_rejects_reuse_and_missing() {
        // Before the fix the handler revoked when a bearer token was
        // present but still answered 204 when it was missing or unknown:
        // a second logout with the same token reported success.
        let state = standalone_state();
        let token = state.tokens.issue("admin", AdminRole::Administrator);

        // Live token: 204 with an empty body, entry removed not retained.
        let (status, body, raw) =
            parts(logout(State(state.clone()), bearer_headers(&token)).await).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(raw.is_empty(), "success is 204 with no body");
        assert_eq!(body, serde_json::Value::Null);
        assert!(
            state.tokens.lookup(&token).is_none(),
            "logout must remove the entry, not retain it"
        );

        // Already-revoked token: client error, never success.
        let (status, body, _) =
            parts(logout(State(state.clone()), bearer_headers(&token)).await).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["code"], serde_json::json!("UNAUTHORIZED"));
        assert!(body.get("message").is_some());
        assert_eq!(body.as_object().expect("error is an object").len(), 2);

        // Unknown token: same client error.
        let (status, body, _) = parts(
            logout(
                State(state.clone()),
                bearer_headers("indra_garbage-token-that-was-never-issued"),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["code"], serde_json::json!("UNAUTHORIZED"));
        assert_eq!(body.as_object().expect("error is an object").len(), 2);

        // Missing header: same client error, never success.
        let (status, body, _) = parts(logout(State(state.clone()), HeaderMap::new()).await).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["code"], serde_json::json!("UNAUTHORIZED"));
        assert_eq!(body.as_object().expect("error is an object").len(), 2);
    }
}

#[cfg(test)]
mod user_scope_tests {
    use super::*;

    #[test]
    fn scope_catalogue_lists_the_documented_names_with_fields() {
        // W2-40 failing test: the catalogue must carry the documented
        // scope names with per-scope fields, and the validation
        // predicates must agree with the served list. Before: a
        // constant string list detached from validation (no fields, no
        // shared registry, wrong vocabulary).
        assert_eq!(USER_SCOPES.len(), 14);
        let mut names: Vec<&str> = USER_SCOPES.iter().map(|entry| entry.name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), 14);
        for entry in &USER_SCOPES {
            assert!(!entry.name.is_empty());
            assert!(!entry.desc.is_empty());
            assert!(is_known_user_scope(entry.name));
        }
        for documented in [
            "connections",
            "publish",
            "data_integration",
            "access_control",
            "gateways",
            "monitoring",
            "cluster_operations",
            "system",
            "audit",
            "license",
            "user_management",
            "mfa_management",
            "sso_management",
            "api_key_management",
        ] {
            assert!(
                is_known_user_scope(documented),
                "catalogue must contain {documented}"
            );
        }
        assert!(!is_known_user_scope("no-such-scope"));
        assert!(!is_known_user_scope("all"));
        // The generic scopes are shared with the API-key catalogue, so
        // both registries agree on them.
        for shared in [
            "connections",
            "publish",
            "data_integration",
            "access_control",
            "gateways",
            "monitoring",
            "cluster_operations",
            "system",
            "audit",
            "license",
        ] {
            assert!(
                super::super::api_keys::is_known_api_key_scope(shared),
                "generic scope {shared} must stay shared with the key catalogue"
            );
        }
    }

    #[test]
    fn role_compatibility_follows_the_admin_only_flags() {
        // Viewers hold only the generic scopes; administrators hold any
        // catalogue scope; unknown names fail closed for both roles.
        for entry in &USER_SCOPES {
            assert!(
                role_may_hold_scope(&AdminRole::Administrator, entry.name),
                "administrator holds {}",
                entry.name
            );
            assert_eq!(
                role_may_hold_scope(&AdminRole::Viewer, entry.name),
                !entry.admin_only,
                "viewer holds {} exactly when it is not admin-only",
                entry.name
            );
        }
        assert!(!role_may_hold_scope(
            &AdminRole::Administrator,
            "no-such-scope"
        ));
        assert!(!role_may_hold_scope(&AdminRole::Viewer, "no-such-scope"));
    }
}
