//! Management API authentication middleware (SEC-C1).
//!
//! Every route except the public ones (login endpoints, health checks,
//! static dashboard assets, MQTT-over-WebSocket) requires credentials.
//! The middleware resolves them, enforces the must-change-password gate
//! and the viewer read-only rule, and publishes the resolved
//! [`TokenInfo`] into request extensions for handlers.

use axum::{
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};

use crate::admin_users::AdminRole;
use crate::v5::api_keys::{KeyDecision, KeyGrant, KeyRole, ResolvedApiKey};
use crate::v5::auth::{authenticated_user, TokenInfo};
use crate::ApiState;

/// Credentials presented on a management API request.
///
/// The management API accepts these credentials:
/// - the token of a user who logged in (`Authorization: Bearer`);
/// - an API key that `POST /api_key` made
///   ([`crate::v5::api_keys::ApiKeyStore`]): `Authorization: Basic
///   base64(api_key:api_secret)` as in the reference API, or the secret
///   as `Authorization: Bearer <api_secret>` as an extension;
/// - an operator key from the environment of the process
///   ([`crate::api_keys::ApiKeyStore`]), as a bearer credential.
///
/// [`resolve_credentials`] is the one place that gives an identity.
#[derive(Debug)]
pub enum Credentials {
    Bearer(String),
    Basic { username: String, secret: String },
}

/// Parse the `Authorization` header into [`Credentials`].
/// Only the `Bearer` and `Basic` schemes (case-insensitive) are accepted;
/// anything else, including a bare token with no scheme, returns `None`.
pub fn parse_credentials(headers: &HeaderMap) -> Option<Credentials> {
    let value = headers.get("authorization")?.to_str().ok()?;
    if let Some(token) = strip_scheme(value, "Bearer") {
        if token.is_empty() {
            return None;
        }
        return Some(Credentials::Bearer(token.to_string()));
    }
    if let Some(decoded) = strip_scheme(value, "Basic") {
        let bytes = base64_decode(decoded)?;
        let text = String::from_utf8(bytes).ok()?;
        let (username, secret) = text.split_once(':')?;
        if username.is_empty() {
            return None;
        }
        return Some(Credentials::Basic {
            username: username.to_string(),
            secret: secret.to_string(),
        });
    }
    None
}

/// The identity of a request, and the grant when the credential is an
/// API key of the key store.
#[derive(Debug, Clone)]
pub struct ResolvedIdentity {
    pub info: TokenInfo,
    /// `Some` for an API key that `POST /api_key` made. The middleware
    /// applies the role and the scopes to the request path. `None` for a
    /// user token and for an operator key from the environment.
    pub key_grant: Option<KeyGrant>,
}

/// The lifetime of the identity of one API-key request. The key does
/// not expire. Each request finds the key again, thus this time must
/// only be longer than one request.
const API_KEY_IDENTITY_TTL: std::time::Duration = std::time::Duration::from_secs(60 * 60);

/// Resolve credentials to an authenticated identity.
///
/// The order for a Bearer credential is: the token of a user (the role is
/// read again from the user store, and the token of a removed user does
/// not work), then an API key of the key store, then an operator key from
/// the environment. A Basic credential is an API key of the key store:
/// the user name is the `api_key` identifier and the password is the
/// `api_secret`. The key store gives no identity to a disabled key or to
/// an expired key.
///
/// An API key of the key store has the name `api-key:<name>` in the audit
/// log. A `viewer` key has the viewer role of a user who logged in, and
/// any other key has the administrator role. The role and the scopes
/// limit the paths (see [`crate::v5::api_keys::key_decision`]). An
/// operator key from the environment has no scopes and no path limit: the
/// person who sets the environment of the process controls the broker
/// already.
pub fn resolve_credentials(state: &ApiState, headers: &HeaderMap) -> Option<ResolvedIdentity> {
    match parse_credentials(headers)? {
        Credentials::Bearer(token) => {
            if let Some(info) = authenticated_user(state, headers) {
                return Some(ResolvedIdentity {
                    info,
                    key_grant: None,
                });
            }
            if let Some(key) = state.api_keys.resolve(&token) {
                return Some(key_identity(key));
            }
            state
                .operator_api_keys
                .resolve(&token)
                .map(|info| ResolvedIdentity {
                    info,
                    key_grant: None,
                })
        }
        Credentials::Basic { username, secret } => state
            .api_keys
            .resolve_basic(&username, &secret)
            .map(key_identity),
    }
}

/// The identity of a request that an API key of the key store signed.
fn key_identity(key: ResolvedApiKey) -> ResolvedIdentity {
    let role = match key.grant.role {
        KeyRole::Viewer => AdminRole::Viewer,
        KeyRole::Administrator | KeyRole::Publisher => AdminRole::Administrator,
    };
    ResolvedIdentity {
        info: TokenInfo {
            username: format!("api-key:{}", key.name),
            role,
            expires_at: std::time::Instant::now() + API_KEY_IDENTITY_TTL,
            must_change_password: false,
        },
        key_grant: Some(key.grant),
    }
}

fn strip_scheme<'a>(value: &'a str, scheme: &str) -> Option<&'a str> {
    if value.len() > scheme.len()
        && value[..scheme.len()].eq_ignore_ascii_case(scheme)
        && value[scheme.len()..].starts_with(' ')
    {
        Some(value[scheme.len() + 1..].trim())
    } else {
        None
    }
}

fn base64_decode(input: &str) -> Option<Vec<u8>> {
    use base64::engine::general_purpose::STANDARD as BASE64;
    use base64::Engine;
    BASE64.decode(input.trim()).ok()
}

fn unauthorized(message: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({ "code": "UNAUTHORIZED", "message": message })),
    )
        .into_response()
}

fn password_change_required() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(serde_json::json!({
            "code": "PASSWORD_CHANGE_REQUIRED",
            "message": "password change required before using the API"
        })),
    )
        .into_response()
}

fn scope_forbidden() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(serde_json::json!({
            "code": "FORBIDDEN",
            "message": "this API key does not have the right to call this path"
        })),
    )
        .into_response()
}

/// The answer to a key that calls an operation which the reference lists
/// for a bearer token only. `POST /logout` documents 401 and the other
/// operations document 403 or nothing, thus they get 403.
fn login_only(path: &str) -> Response {
    if path.trim_end_matches('/') == "/api/v5/logout" {
        return unauthorized("this operation needs the token of a user who logged in");
    }
    (
        StatusCode::FORBIDDEN,
        Json(serde_json::json!({
            "code": "FORBIDDEN",
            "message": "this operation needs the token of a user who logged in"
        })),
    )
        .into_response()
}

fn forbidden() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(serde_json::json!({
            "code": "FORBIDDEN",
            "message": "insufficient permissions"
        })),
    )
        .into_response()
}

/// Path of `PUT`/`POST /api/v5/users/<own username>/change_pwd`, the one
/// mutating route a must-change-password user may still call. The username
/// segment is percent-decoded before comparing; malformed encodings never
/// match.
pub(crate) fn is_own_change_pwd(method: &axum::http::Method, path: &str, username: &str) -> bool {
    if *method != axum::http::Method::PUT && *method != axum::http::Method::POST {
        return false;
    }
    let Some(rest) = path.strip_prefix("/api/v5/users/") else {
        return false;
    };
    let Some((name, tail)) = rest.split_once('/') else {
        return false;
    };
    if tail != "change_pwd" {
        return false;
    }
    let Some(decoded) = percent_decode(name) else {
        return false;
    };
    decoded == username
}

/// Tiny `%XX` percent-decoder for a single path segment. Returns `None`
/// on malformed encodings (`%` not followed by two hex digits, including
/// a truncated tail).
fn percent_decode(segment: &str) -> Option<String> {
    let bytes = segment.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            if i + 2 >= bytes.len() {
                return None;
            }
            let hi = hex_val(bytes[i + 1])?;
            let lo = hex_val(bytes[i + 2])?;
            out.push((hi << 4) | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn hex_val(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// True for the three routes with which a user enrols MFA. A viewer can
/// call them, and the route functions let a viewer name only that viewer
/// (see `v5::mfa::caller_may_target`).
fn is_mfa_self_service(method: &axum::http::Method, path: &str) -> bool {
    *method == axum::http::Method::POST
        && matches!(
            path,
            "/api/v5/sso/mfa/setup" | "/api/v5/sso/mfa/setup_info" | "/api/v5/sso/mfa/verify"
        )
}

/// Require authentication on every route behind this layer.
///
/// Exemptions are structural: public routes (login, health, dashboard
/// assets, `/ws/mqtt`) never pass through here. Inside, the only
/// exemptions are logout and current_user plus a user's own password
/// change while `must_change_password` is set, and GET-only access for
/// viewers. A viewer can also call the three MFA enrolment routes for
/// that viewer.
pub async fn require_api_auth(State(state): State<ApiState>, req: Request, next: Next) -> Response {
    let Some(identity) = resolve_credentials(&state, req.headers()) else {
        return unauthorized("missing or invalid credentials");
    };
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    if let Some(grant) = &identity.key_grant {
        match crate::v5::api_keys::key_decision(grant, &path) {
            KeyDecision::Allowed => {}
            KeyDecision::LoginOnly => return login_only(&path),
            KeyDecision::Forbidden => return scope_forbidden(),
        }
    }
    let info = identity.info;
    // `resolve_credentials` already loaded the user fresh from the store
    // (revoking tokens of deleted users), so `info` carries the current
    // role and `must_change_password` with no second `admin_users.get`.
    let must_change = info.must_change_password;

    let is_logout = method == axum::http::Method::POST && path == "/api/v5/logout";
    let is_current_user = method == axum::http::Method::GET && path == "/api/v5/current_user";
    let own_change = is_own_change_pwd(&method, &path, &info.username);

    if must_change && !(own_change || is_logout || is_current_user) {
        return password_change_required();
    }
    if info.role == AdminRole::Viewer
        && !(method == axum::http::Method::GET
            || is_logout
            || own_change
            || is_mfa_self_service(&method, &path))
    {
        return forbidden();
    }

    let audited = matches!(method.as_str(), "POST" | "PUT" | "DELETE");
    let mut req = req;
    req.extensions_mut().insert(info.clone());
    let started = std::time::Instant::now();
    let response = next.run(req).await;
    // Management audit (W2-28): one fire-and-forget entry per
    // authenticated management write, recorded after the response so
    // the audited request never waits on the ring. Reads make no
    // record: the documented audit methods are post, put and delete.
    // Management-plane only; publish and deliver never pass through
    // here. The peer address is not available in this layer without
    // connection info, so the record carries no address and the render
    // omits `source_ip`.
    if !audited {
        return response;
    }
    let status = response.status().as_u16();
    state.audit.record(crate::v5::audit::AuditRecordInput {
        created_at_us: crate::v5::audit::now_micros(),
        node: state.node_id.clone(),
        from: crate::v5::audit::AUDIT_FROM.to_string(),
        source: info.username.clone(),
        source_ip: String::new(),
        operation_id: path.clone(),
        operation_type: crate::v5::audit::operation_type_for_path(&path),
        operation_result: crate::v5::audit::outcome_for_status(status).to_string(),
        http_method: method.as_str().to_lowercase(),
        http_status_code: status,
        duration_ms: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        tenant: broker_session::tenant::DEFAULT_TENANT_ID.to_string(),
    });
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers_with(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", value.parse().expect("header value"));
        headers
    }

    #[test]
    fn parses_bearer_case_insensitively() {
        let creds = parse_credentials(&headers_with("Bearer abc123")).expect("bearer");
        assert!(matches!(creds, Credentials::Bearer(ref t) if t == "abc123"));
        let creds = parse_credentials(&headers_with("bearer abc123")).expect("lowercase");
        assert!(matches!(creds, Credentials::Bearer(ref t) if t == "abc123"));
    }

    #[test]
    fn parses_basic_credentials() {
        // "admin:secret" in base64.
        let creds = parse_credentials(&headers_with("Basic YWRtaW46c2VjcmV0")).expect("basic");
        match creds {
            Credentials::Basic { username, secret } => {
                assert_eq!(username, "admin");
                assert_eq!(secret, "secret");
            }
            other => panic!("expected Basic, got {other:?}"),
        }
    }

    #[test]
    fn rejects_missing_and_empty_credentials() {
        assert!(parse_credentials(&HeaderMap::new()).is_none());
        assert!(parse_credentials(&headers_with("Bearer ")).is_none());
        assert!(parse_credentials(&headers_with("Basic !!!not-base64!!!")).is_none());
    }

    #[test]
    fn a_basic_credential_with_an_empty_user_name_is_refused() {
        // ":secret" in base64.
        assert!(parse_credentials(&headers_with("Basic OnNlY3JldA==")).is_none());
    }

    #[test]
    fn matches_own_change_pwd_path() {
        let put = axum::http::Method::PUT;
        assert!(is_own_change_pwd(
            &put,
            "/api/v5/users/admin/change_pwd",
            "admin"
        ));
        assert!(!is_own_change_pwd(
            &put,
            "/api/v5/users/other/change_pwd",
            "admin"
        ));
        assert!(!is_own_change_pwd(
            &axum::http::Method::GET,
            "/api/v5/users/admin/change_pwd",
            "admin"
        ));
        assert!(!is_own_change_pwd(&put, "/api/v5/users/admin", "admin"));
    }
}
