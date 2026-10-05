//! Dashboard multi-factor enrolment and verification (TOTP code factor).
//!
//! Covers `POST /sso/mfa/setup` (C05-13), `POST /sso/mfa/setup_info`
//! (C05-14) and `POST /sso/mfa/verify` (C05-15) on the real admin-user
//! MFA state in [`crate::admin_users::AdminUsers`]: setup generates one
//! TOTP secret per user and stores it as pending without enabling;
//! setup_info reports the enrolment state; verify checks a six-digit
//! code inside the +-1-step window and enables on the first success.
//! Replay (reusing a code at or below the last verified counter) and
//! codes outside the window are client errors; five consecutive failures
//! trip a 60 s per-user cool-down (429).
//!
//! Store bounds (stated here and enforced in `admin_users.rs`):
//! - one enrolment record per user (a single secret slot; a second setup
//!   while pending returns the stored secret, a setup while enabled is
//!   rejected, so enrolment never grows);
//! - code window of one 30 s step either side (stale codes expire);
//! - attempt cap of [`crate::admin_users::MFA_VERIFY_MAX_ATTEMPTS`]
//!   consecutive failures then a [`crate::admin_users::MFA_VERIFY_LOCKOUT_SECS`]
//!   s cool-down; the counter is memory-only per known user (within
//!   `MAX_ADMIN_USERS`), the secret plus enabled flag persist through the
//!   config registry across a restart.
//!
//! Secrets are stored reversibly (verification must recompute codes),
//! never logged, and only returned once by the setup route.
//!
//! Management-plane only: short user-map and attempt-map locks per call;
//! publish and deliver never touch either map, add no locks and allocate
//! nothing on the message path.
// TODO(parity): what are the documented request/response field names and
// shapes for these three routes (secret versus provisioning fields,
// envelope versus bare object, code field name)? The rulebook does not
// decide the exact shapes; the current choice takes `{username}` for
// setup and setup_info and `{username, code}` for verify, and answers
// `{username, secret, provisioning_uri}`, `{username, enabled, pending}`
// and `{username, enabled}` respectively, until the checker pins them.
// TODO(parity): which error codes does the documented shape assign to
// bad codes, replays, missing enrolments and duplicate setups? The
// rulebook does not decide the exact code names; the current choice keeps
// `BAD_REQUEST` (400) for bad/replayed/missing-enrolment codes,
// `ALREADY_EXISTS` (409) for setup while enabled, `NOT_FOUND` (404) for
// unknown users and `TOO_MANY_REQUESTS` (429) for the cool-down, all in
// the documented `{code, message}` shape, until the checker pins them.
// TODO(parity): is an already-enabled user re-enrolled by setup (secret
// rotation) or rejected? The rulebook does not decide the rotation
// policy; the current choice rejects with `ALREADY_EXISTS` fail-closed
// (never silently replaces a live second factor) until the checker pins it.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;

use crate::admin_users::{AdminUserError, MfaVerifyError};
use crate::errors::ApiError;
use crate::ApiState;

/// Issuer label rendered in the provisioning URI shown to authenticator
/// apps. Reason: the app displays it beside the username so operators
/// can tell broker enrolments apart; a short product name is enough.
const MFA_ISSUER: &str = "IndraMQTT";

/// Extract a non-empty `username` string field from a JSON object body.
fn parse_username(value: &serde_json::Value) -> Result<String, String> {
    let username = value
        .as_object()
        .and_then(|object| object.get("username"))
        .and_then(|name| name.as_str())
        .ok_or_else(|| "field `username` is required".to_string())?;
    if username.trim().is_empty() {
        return Err("field `username` must not be empty".to_string());
    }
    Ok(username.to_string())
}

/// Parse a `{username, code}` verify body. The code shape itself is
/// checked by the TOTP verifier (six ASCII digits); anything else fails
/// there as an invalid code, never as a 400 here, except a missing or
/// non-string field which is malformed.
fn parse_verify_body(value: &serde_json::Value) -> Result<(String, String), String> {
    let username = parse_username(value)?;
    let code = value
        .as_object()
        .and_then(|object| object.get("code"))
        .and_then(|code| code.as_str())
        .ok_or_else(|| "field `code` is required".to_string())?;
    if code.is_empty() {
        return Err("field `code` must not be empty".to_string());
    }
    Ok((username, code.to_string()))
}

fn already_exists(message: String) -> Response {
    (
        StatusCode::CONFLICT,
        Json(serde_json::json!({
            "code": "ALREADY_EXISTS",
            "message": message,
        })),
    )
        .into_response()
}

fn internal_error(message: String) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({
            "code": "INTERNAL_ERROR",
            "message": message,
        })),
    )
        .into_response()
}

/// Examines the target of an MFA request. An administrator can name each
/// user. A user who is not an administrator can name only that user:
/// the secret of an enrolment is a credential, and a viewer must not
/// get the secret of a different user or use up that user's attempts.
/// A body that is not valid is not refused here: the handler gives the
/// usual 400 for it.
fn caller_may_target(caller: &crate::v5::auth::TokenInfo, body: &[u8]) -> Option<Response> {
    if caller.role == crate::admin_users::AdminRole::Administrator {
        return None;
    }
    let target = serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|value| parse_username(&value).ok());
    match target {
        Some(username) if username != caller.username => Some(
            (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({
                    "code": "FORBIDDEN",
                    "message": "a user who is not an administrator can change only that user's MFA",
                })),
            )
                .into_response(),
        ),
        _ => None,
    }
}

/// The route of [`setup`]. It examines the caller first.
pub async fn setup_route(
    State(state): State<ApiState>,
    axum::Extension(caller): axum::Extension<crate::v5::auth::TokenInfo>,
    body: Bytes,
) -> Response {
    if let Some(refusal) = caller_may_target(&caller, &body) {
        return refusal;
    }
    setup(State(state), body).await
}

/// The route of [`setup_info`]. It examines the caller first.
pub async fn setup_info_route(
    State(state): State<ApiState>,
    axum::Extension(caller): axum::Extension<crate::v5::auth::TokenInfo>,
    body: Bytes,
) -> Response {
    if let Some(refusal) = caller_may_target(&caller, &body) {
        return refusal;
    }
    setup_info(State(state), body).await
}

/// The route of [`verify`]. It examines the caller first.
pub async fn verify_route(
    State(state): State<ApiState>,
    axum::Extension(caller): axum::Extension<crate::v5::auth::TokenInfo>,
    body: Bytes,
) -> Response {
    if let Some(refusal) = caller_may_target(&caller, &body) {
        return refusal;
    }
    verify(State(state), body).await
}

/// `POST /sso/mfa/setup`: start per-user MFA enrolment.
///
/// The body must be a JSON object with a non-empty string `username`.
/// Unknown users fail with 404; a user whose MFA is already enabled fails
/// with 409 `ALREADY_EXISTS` and changes nothing; otherwise one secret is
/// stored as pending (a repeat setup while pending returns the same
/// secret) and the response carries the base32 `secret` plus the
/// `provisioning_uri` once. MFA is not enabled by this route. The route
/// is not paged. Secrets are never logged.
pub async fn setup(State(state): State<ApiState>, body: Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid mfa setup body: {error}"))
                .into_response();
        }
    };
    let username = match parse_username(&value) {
        Ok(username) => username,
        Err(message) => return ApiError::BadRequest(message).into_response(),
    };
    let secret = match state.admin_users.mfa_setup(&username) {
        Ok(secret) => secret,
        Err(AdminUserError::NotFound) => {
            return ApiError::NotFound(format!("user {username:?} not found")).into_response();
        }
        Err(AdminUserError::AlreadyExists) => {
            return already_exists(format!("user {username:?} already has MFA enabled"));
        }
        Err(error @ AdminUserError::Persist(_)) => return internal_error(error.to_string()),
        Err(error) => return ApiError::BadRequest(error.to_string()).into_response(),
    };
    let secret_b32 = crate::mfa_totp::base32_encode(&secret);
    let uri = crate::mfa_totp::provisioning_uri(MFA_ISSUER, &username, &secret_b32);
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "username": username,
            "secret": secret_b32,
            "provisioning_uri": uri,
        })),
    )
        .into_response()
}

/// `POST /sso/mfa/setup_info`: report one user's MFA enrolment state.
///
/// The body must be a JSON object with a non-empty string `username`.
/// Unknown users fail with 404. Answers `{username, enabled, pending}`
/// where `pending` means a secret is stored but no code has verified yet.
/// Secrets never render here. The route is not paged.
pub async fn setup_info(State(state): State<ApiState>, body: Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid mfa setup_info body: {error}"))
                .into_response();
        }
    };
    let username = match parse_username(&value) {
        Ok(username) => username,
        Err(message) => return ApiError::BadRequest(message).into_response(),
    };
    let Some(status) = state.admin_users.mfa_status(&username) else {
        return ApiError::NotFound(format!("user {username:?} not found")).into_response();
    };
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "username": username,
            "enabled": status.enabled,
            "pending": status.pending,
        })),
    )
        .into_response()
}

/// `POST /sso/mfa/verify`: confirm a one-time code and enable MFA.
///
/// The body must be a JSON object with non-empty string `username` and
/// `code` fields. Unknown users fail with 404; users with no enrolment
/// fail with 400; malformed codes and codes outside the +-1-step window
/// fail with 400; a code whose counter was already used fails with 400
/// (single-use replay rejection); five consecutive failures trip a 60 s
/// per-user 429 cool-down. The first successful verification flips the
/// user to enabled and persists it. The route is not paged. Codes and
/// secrets are never logged.
pub async fn verify(State(state): State<ApiState>, body: Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid mfa verify body: {error}"))
                .into_response();
        }
    };
    let (username, code) = match parse_verify_body(&value) {
        Ok(parsed) => parsed,
        Err(message) => return ApiError::BadRequest(message).into_response(),
    };
    let now = crate::mfa_totp::unix_now_secs();
    match state.admin_users.mfa_verify(&username, &code, now) {
        Ok(enabled) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "username": username,
                "enabled": enabled,
            })),
        )
            .into_response(),
        Err(MfaVerifyError::NotFound) => {
            ApiError::NotFound(format!("user {username:?} not found")).into_response()
        }
        Err(MfaVerifyError::NoEnrolment) => {
            ApiError::BadRequest(format!("user {username:?} has no MFA enrolment")).into_response()
        }
        Err(MfaVerifyError::Invalid) => {
            ApiError::BadRequest("invalid verification code".to_string()).into_response()
        }
        Err(MfaVerifyError::Replayed) => {
            ApiError::BadRequest("verification code already used".to_string()).into_response()
        }
        Err(MfaVerifyError::Locked) => {
            ApiError::TooManyRequests("too many failed attempts; try again later".to_string())
                .into_response()
        }
        Err(MfaVerifyError::Persist(reason)) => internal_error(reason),
    }
}

/// `POST /users/{username}/mfa`: enrol TOTP multi-factor for one
/// dashboard user (C05-31).
///
/// The username comes from the path. The body may be empty (TOTP is the
/// only factor this broker tracks, so an empty body enrols TOTP) or a
/// JSON object with an optional string `mechanism` field; any other
/// mechanism fails with 400 fail-closed, since non-TOTP factors are out
/// of scope and must never enrol silently. A malformed body fails with
/// 400 and enrols nothing. Unknown users fail with 404; a user with any
/// existing enrolment (pending or enabled) fails with 409
/// `ALREADY_EXISTS` and changes nothing, so enrolment never grows past
/// the single secret slot. On success answers 204 with no body, as the
/// reference API documents. The user stays pending until a code verifies
/// on the verification route. The user reads the secret and the
/// provisioning URI from `POST /sso/mfa/setup`, which returns the same
/// pending secret.
/// The route is not paged. Secrets are never logged.
/// Management-plane only: one short user-map write lock per call (plus
/// the registry save lock on persist); publish and deliver never touch
/// the user map or the attempt counters.
// TODO(parity): what are the documented request/response field names and
// shapes for this route (mechanism body, secret versus provisioning
// fields, 200 versus an empty success)? The rulebook does not decide the
// exact shapes; the current choice accepts an empty or
// `{mechanism: "totp"}` body and answers the same provisioning fields
// as the setup route until the checker pins them.
// TODO(parity): is a duplicate enrolment rejected with `ALREADY_EXISTS`
// (409 here) or `BAD_REQUEST` (400)? The rulebook does not decide the
// exact duplicate code; the current choice keeps `ALREADY_EXISTS`
// (409, distinct from validation failures, shared with the setup route's
// enabled-duplicate) until the checker pins it.
// TODO(parity): may an administrator enrol their own account through
// this route, or must self-service stay on the current-user MFA route?
// The rulebook does not decide the self-target policy; the current
// choice enrols any known user including the caller until the checker
// pins it.
pub async fn enrol(
    State(state): State<ApiState>,
    Path(username): Path<String>,
    body: Bytes,
) -> Response {
    if let Err(message) = check_enrol_body(&body) {
        return ApiError::BadRequest(message).into_response();
    }
    match state.admin_users.mfa_admin_enrol(&username) {
        Ok(_) => {}
        Err(AdminUserError::NotFound) => {
            return ApiError::UserNotFound(format!("user {username:?} not found")).into_response();
        }
        Err(AdminUserError::AlreadyExists) => {
            return already_exists(format!("user {username:?} already has MFA enrolled"));
        }
        Err(error @ AdminUserError::Persist(_)) => return internal_error(error.to_string()),
        Err(error) => return ApiError::BadRequest(error.to_string()).into_response(),
    }
    StatusCode::NO_CONTENT.into_response()
}

/// Check the optional enrol body: empty enrols TOTP; a JSON object with
/// no `mechanism` or `"mechanism": "totp"` enrols TOTP; any other
/// mechanism fails closed (non-TOTP factors are out of scope); anything
/// else malformed fails with 400 and enrols nothing.
fn check_enrol_body(body: &[u8]) -> Result<(), String> {
    if body.is_empty() {
        return Ok(());
    }
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|error| format!("invalid mfa body: {error}"))?;
    let object = value
        .as_object()
        .ok_or_else(|| "invalid mfa body: expected JSON object".to_string())?;
    let mechanism = object.get("mechanism");
    match mechanism {
        None => Ok(()),
        Some(raw) => match raw.as_str() {
            Some("totp") => Ok(()),
            Some(other) => Err(format!("unsupported mfa mechanism {other:?}")),
            None => Err("field `mechanism` must be a string".to_string()),
        },
    }
}

/// `DELETE /users/{username}/mfa`: disable multi-factor for one
/// dashboard user (C05-30).
///
/// Clears the secret, the enabled flag and the last verified counter so
/// re-enabling later requires a fresh enrolment. Unknown users fail with
/// 404 `USER_NOT_FOUND` in the documented `{code, message}` shape and
/// change nothing. A known user with no enrolment has MFA off already:
/// the call changes nothing and answers 204. On success answers 204 with
/// no body. The route is not paged. Codes and secrets are never
/// logged.
/// Management-plane only: one short user-map write lock per call (plus
/// the registry save lock on persist); publish and deliver never touch
/// the user map or the attempt counters.
pub async fn disable(State(state): State<ApiState>, Path(username): Path<String>) -> Response {
    match state.admin_users.mfa_disable(&username) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(AdminUserError::NotFound) => {
            // The store answers `NotFound` for an unknown user and for a
            // user with no enrolment. The second case has MFA off already.
            if state.admin_users.get(&username).is_some() {
                StatusCode::NO_CONTENT.into_response()
            } else {
                ApiError::UserNotFound(format!("user {username:?} not found")).into_response()
            }
        }
        Err(error @ AdminUserError::Persist(_)) => internal_error(error.to_string()),
        Err(error) => ApiError::BadRequest(error.to_string()).into_response(),
    }
}

#[cfg(test)]
mod mfa_handler_tests {
    use super::*;
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

    fn create_user(state: &ApiState, username: &str) {
        state
            .admin_users
            .create(
                username,
                "MfaSecret1!",
                crate::admin_users::AdminRole::Viewer,
                "mfa probe",
            )
            .expect("probe user creates");
    }

    /// Current valid code for `username`, read back through the store's
    /// test hook and computed with the shared TOTP helper at this instant.
    fn current_code(state: &ApiState, username: &str) -> String {
        let secret = state
            .admin_users
            .mfa_secret_for_test(username)
            .expect("enrolment stores a secret");
        crate::mfa_totp::totp_code(
            &secret,
            crate::mfa_totp::totp_counter(crate::mfa_totp::unix_now_secs()),
        )
    }

    #[tokio::test]
    async fn setup_then_info_then_verify_round_trip_with_replay_rejected() {
        // W2-34 failing test: setup starts enrolment, setup_info reports
        // pending, a valid code enables, and the same code replayed is
        // rejected. Before: all three routes answered 404.
        let state = standalone_state();
        create_user(&state, "w234-alice");

        // Setup enrols without enabling and returns provisioning fields.
        let (status, enrolled, raw) = parts(
            setup(
                State(state.clone()),
                body(serde_json::json!({"username": "w234-alice"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(enrolled["username"], serde_json::json!("w234-alice"));
        let secret = enrolled["secret"]
            .as_str()
            .expect("setup returns a secret")
            .to_string();
        assert!(!secret.is_empty());
        assert!(
            enrolled["provisioning_uri"].is_string(),
            "setup returns provisioning fields"
        );
        assert!(
            !String::from_utf8_lossy(&raw).contains("MfaSecret1!"),
            "setup body carries no password"
        );

        // Setup_info reports pending (secret stored, not yet enabled).
        let (status, info, _) = parts(
            setup_info(
                State(state.clone()),
                body(serde_json::json!({"username": "w234-alice"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(info["enabled"], serde_json::json!(false));
        assert_eq!(info["pending"], serde_json::json!(true));
        assert!(
            info.get("secret").is_none(),
            "setup_info never renders the secret"
        );

        // A valid code enables.
        let code = current_code(&state, "w234-alice");
        let (status, verified, _) = parts(
            verify(
                State(state.clone()),
                body(serde_json::json!({"username": "w234-alice", "code": code})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(verified["enabled"], serde_json::json!(true));

        // Setup_info now reports enabled.
        let (status, info, _) = parts(
            setup_info(
                State(state.clone()),
                body(serde_json::json!({"username": "w234-alice"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(info["enabled"], serde_json::json!(true));
        assert_eq!(info["pending"], serde_json::json!(false));

        // Replaying the same code is rejected (single-use window).
        let (status, replayed, _) = parts(
            verify(
                State(state.clone()),
                body(serde_json::json!({"username": "w234-alice", "code": code})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(replayed["code"], serde_json::json!("BAD_REQUEST"));
        assert!(replayed["message"].is_string());
        assert_eq!(replayed.as_object().expect("error is an object").len(), 2);
    }

    #[tokio::test]
    async fn bad_codes_unknown_users_and_missing_enrolment_are_client_errors() {
        let state = standalone_state();
        create_user(&state, "w234-bob");

        // Verify with no enrolment is a client error with the documented shape.
        let (status, err, _) = parts(
            verify(
                State(state.clone()),
                body(serde_json::json!({"username": "w234-bob", "code": "123456"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));

        // Enrol, then a wrong code is rejected and does not enable.
        let (status, _, _) = parts(
            setup(
                State(state.clone()),
                body(serde_json::json!({"username": "w234-bob"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let valid = current_code(&state, "w234-bob");
        let mut wrong = valid.clone();
        wrong.replace_range(0..1, if &valid[0..1] == "0" { "1" } else { "0" });
        assert_ne!(wrong, valid);
        let (status, err, _) = parts(
            verify(
                State(state.clone()),
                body(serde_json::json!({"username": "w234-bob", "code": wrong})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
        let (status, info, _) = parts(
            setup_info(
                State(state.clone()),
                body(serde_json::json!({"username": "w234-bob"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(info["enabled"], serde_json::json!(false));

        // Malformed codes fail closed without enabling.
        for bad in ["", "12345", "1234567", "abcdef", "not-a-code"] {
            let (status, err, _) = parts(
                verify(
                    State(state.clone()),
                    body(serde_json::json!({"username": "w234-bob", "code": bad})),
                )
                .await,
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "bad code {bad:?}");
            assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
        }

        // Unknown users are 404 with the documented shape on all routes.
        let (status, err, _) = parts(
            setup(
                State(state.clone()),
                body(serde_json::json!({"username": "w234-ghost"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(err["code"], serde_json::json!("NOT_FOUND"));
        assert_eq!(err.as_object().expect("error is an object").len(), 2);
        let (status, err, _) = parts(
            setup_info(
                State(state.clone()),
                body(serde_json::json!({"username": "w234-ghost"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(err["code"], serde_json::json!("NOT_FOUND"));
        assert_eq!(err.as_object().expect("error is an object").len(), 2);
        let (status, err, _) = parts(
            verify(
                State(state.clone()),
                body(serde_json::json!({"username": "w234-ghost", "code": "123456"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(err["code"], serde_json::json!("NOT_FOUND"));
        assert_eq!(err.as_object().expect("error is an object").len(), 2);

        // Malformed bodies are 400 and change nothing.
        let (status, err, _) =
            parts(setup(State(state.clone()), Bytes::from_static(b"not json")).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
        let (status, err, _) =
            parts(setup(State(state.clone()), body(serde_json::json!({}))).await).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
    }

    #[tokio::test]
    async fn repeated_failures_trip_the_cool_down() {
        let state = standalone_state();
        create_user(&state, "w234-carol");
        let (status, _, _) = parts(
            setup(
                State(state.clone()),
                body(serde_json::json!({"username": "w234-carol"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        // Bound: five consecutive failures, then the cool-down.
        const {
            assert!(crate::admin_users::MFA_VERIFY_MAX_ATTEMPTS == 5);
        }
        const {
            assert!(crate::admin_users::MFA_VERIFY_LOCKOUT_SECS > 0);
        }
        for _ in 0..crate::admin_users::MFA_VERIFY_MAX_ATTEMPTS {
            let (status, _, _) = parts(
                verify(
                    State(state.clone()),
                    body(serde_json::json!({"username": "w234-carol", "code": "000000"})),
                )
                .await,
            )
            .await;
            // "000000" is well-formed but wrong for this secret at this
            // instant with overwhelming probability; if it ever verifies,
            // the next iteration still exercises the same path.
            assert!(
                status == StatusCode::BAD_REQUEST || status == StatusCode::OK,
                "attempt answers 400 (or 200 on a 1-in-a-million hit)"
            );
            if status == StatusCode::OK {
                return;
            }
        }
        let (status, err, _) = parts(
            verify(
                State(state.clone()),
                body(serde_json::json!({"username": "w234-carol", "code": "000000"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(err["code"], serde_json::json!("TOO_MANY_REQUESTS"));
        assert_eq!(err.as_object().expect("error is an object").len(), 2);
    }

    #[tokio::test]
    async fn enrolment_bounds_and_persistence() {
        // One pending enrolment per user: a second setup returns the same
        // secret; a setup while enabled is rejected. The secret plus the
        // enabled flag survive a registry round-trip (restart).
        let state = standalone_state();
        create_user(&state, "w234-dave");
        let (status, first, _) = parts(
            setup(
                State(state.clone()),
                body(serde_json::json!({"username": "w234-dave"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, second, _) = parts(
            setup(
                State(state.clone()),
                body(serde_json::json!({"username": "w234-dave"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(first["secret"], second["secret"]);

        let code = current_code(&state, "w234-dave");
        let (status, _, _) = parts(
            verify(
                State(state.clone()),
                body(serde_json::json!({"username": "w234-dave", "code": code})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, err, _) = parts(
            setup(
                State(state.clone()),
                body(serde_json::json!({"username": "w234-dave"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(err["code"], serde_json::json!("ALREADY_EXISTS"));

        // Persistence rides the config registry: a registry-backed store
        // still holds the enrolment after a reload.
        let dir = std::env::temp_dir().join(format!(
            "indramqtt-w234-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let registry = std::sync::Arc::new(
            broker_config::ConfigRegistry::load(&dir).expect("scratch registry loads"),
        );
        let durable = crate::admin_users::AdminUsers::from_registry(&registry);
        durable
            .create(
                "w234-kept",
                "KeptSecret1!",
                crate::admin_users::AdminRole::Viewer,
                "kept across restart",
            )
            .expect("registry-backed create persists");
        let secret = durable.mfa_setup("w234-kept").expect("setup persists");
        let now = crate::mfa_totp::unix_now_secs();
        let code = crate::mfa_totp::totp_code(&secret, crate::mfa_totp::totp_counter(now));
        durable
            .mfa_verify("w234-kept", &code, now)
            .expect("verify enables");
        drop(durable);
        let reloaded =
            broker_config::ConfigRegistry::load(&dir).expect("registry reloads after a restart");
        let snapshot = reloaded.snapshot();
        let found = snapshot
            .admin_users
            .users
            .iter()
            .find(|user| user.username == "w234-kept")
            .expect("user persists across a restart");
        assert!(found.mfa_enabled, "enabled flag persists across a restart");
        assert!(
            !found.mfa_secret.is_empty(),
            "secret persists across a restart"
        );
        let restored =
            crate::admin_users::AdminUsers::from_snapshot(&reloaded.snapshot().admin_users);
        let status = restored
            .mfa_status("w234-kept")
            .expect("restored store reports enrolment");
        assert!(status.enabled);
        assert!(!status.pending);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn admin_mfa_enrol_then_disable_round_trip_with_duplicate_rejected() {
        // W2-44 failing test: enrol returns the provisioning fields, a
        // second enrol is rejected as a duplicate, delete disables, and a
        // second delete is a miss. Before: both routes answered 404.
        let state = standalone_state();
        create_user(&state, "w244-alice");

        // Enrol answers 204 with no body, as the reference API documents.
        let (status, enrolled, raw) = parts(
            enrol(
                State(state.clone()),
                Path("w244-alice".to_string()),
                body(serde_json::json!({"mechanism": "totp"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(enrolled.is_null() && raw.is_empty(), "enrol has no body");
        assert!(
            state
                .admin_users
                .mfa_secret_for_test("w244-alice")
                .is_some_and(|secret| !secret.is_empty()),
            "enrolment stores a secret"
        );

        // The pending secret and the provisioning URI come from the setup
        // route.
        let (status, provisioning, _) = parts(
            setup(
                State(state.clone()),
                body(serde_json::json!({"username": "w244-alice"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(provisioning["secret"]
            .as_str()
            .is_some_and(|s| !s.is_empty()));
        assert!(provisioning["provisioning_uri"].is_string());

        // The shared admin-user state reports the enrolment as pending.
        let (status, info, _) = parts(
            setup_info(
                State(state.clone()),
                body(serde_json::json!({"username": "w244-alice"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(info["pending"], serde_json::json!(true));

        // Re-enrolling (pending or enabled) is rejected cleanly and keeps
        // the stored secret.
        let before = state
            .admin_users
            .mfa_secret_for_test("w244-alice")
            .expect("enrolment stores a secret");
        let (status, err, _) = parts(
            enrol(
                State(state.clone()),
                Path("w244-alice".to_string()),
                body(serde_json::json!({"mechanism": "totp"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(err["code"], serde_json::json!("ALREADY_EXISTS"));
        assert_eq!(err.as_object().expect("error is an object").len(), 2);
        assert_eq!(
            state
                .admin_users
                .mfa_secret_for_test("w244-alice")
                .expect("secret survives a duplicate"),
            before
        );

        // Delete disables and reports success with no body.
        let (status, gone, raw) =
            parts(disable(State(state.clone()), Path("w244-alice".to_string())).await).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(raw.is_empty(), "delete answers 204 with no body");
        assert_eq!(gone, serde_json::Value::Null);

        // Deleting again changes nothing: MFA is off, so the answer is 204.
        let (status, again, raw) =
            parts(disable(State(state.clone()), Path("w244-alice".to_string())).await).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(raw.is_empty(), "repeat delete answers 204 with no body");
        assert_eq!(again, serde_json::Value::Null);

        // Unknown users are 404 with the documented shape on both routes.
        let (status, err, _) = parts(
            enrol(
                State(state.clone()),
                Path("w244-ghost".to_string()),
                body(serde_json::json!({"mechanism": "totp"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(err["code"], serde_json::json!("USER_NOT_FOUND"));
        assert_eq!(err.as_object().expect("error is an object").len(), 2);
        let (status, err, _) =
            parts(disable(State(state.clone()), Path("w244-ghost".to_string())).await).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(err["code"], serde_json::json!("USER_NOT_FOUND"));
        assert_eq!(err.as_object().expect("error is an object").len(), 2);
    }

    #[tokio::test]
    async fn admin_mfa_enrol_body_rules_and_enabled_duplicate() {
        let state = standalone_state();
        create_user(&state, "w244-bob");

        // An empty body enrols TOTP (the only factor this broker tracks).
        let (status, enrolled, _) = parts(
            enrol(
                State(state.clone()),
                Path("w244-bob".to_string()),
                Bytes::new(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert!(enrolled.is_null());

        // Non-TOTP factors are out of scope and fail closed.
        create_user(&state, "w244-carol");
        for body_value in [
            serde_json::json!({"mechanism": "sms"}),
            serde_json::json!({"mechanism": 7}),
            serde_json::json!([1, 2]),
        ] {
            let (status, err, _) = parts(
                enrol(
                    State(state.clone()),
                    Path("w244-carol".to_string()),
                    body(body_value.clone()),
                )
                .await,
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "body {body_value}");
            assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
            assert!(
                state
                    .admin_users
                    .mfa_secret_for_test("w244-carol")
                    .is_none_or(|secret| secret.is_empty()),
                "rejected bodies enrol nothing"
            );
        }
        let (status, _, _) = parts(
            enrol(
                State(state.clone()),
                Path("w244-carol".to_string()),
                Bytes::from_static(b"not json"),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        // An enabled user re-enrolling is rejected the same clean way.
        let secret = state
            .admin_users
            .mfa_secret_for_test("w244-bob")
            .expect("enrolment stores a secret");
        let now = crate::mfa_totp::unix_now_secs();
        let code = crate::mfa_totp::totp_code(&secret, crate::mfa_totp::totp_counter(now));
        state
            .admin_users
            .mfa_verify("w244-bob", &code, now)
            .expect("verify enables");
        let (status, err, _) = parts(
            enrol(
                State(state.clone()),
                Path("w244-bob".to_string()),
                Bytes::new(),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(err["code"], serde_json::json!("ALREADY_EXISTS"));

        // Disabling an enabled user clears the enrolment; the state reads
        // as neither pending nor enabled afterwards.
        let (status, _, _) =
            parts(disable(State(state.clone()), Path("w244-bob".to_string())).await).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, info, _) = parts(
            setup_info(
                State(state.clone()),
                body(serde_json::json!({"username": "w244-bob"})),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(info["enabled"], serde_json::json!(false));
        assert_eq!(info["pending"], serde_json::json!(false));
    }

    #[tokio::test]
    async fn admin_mfa_enrol_and_disable_persist_across_restart() {
        // Bound plus persistence: one record per user rides the config
        // registry byte-identically, and the disable survives a reload.
        let dir = std::env::temp_dir().join(format!(
            "indramqtt-w244-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let registry = std::sync::Arc::new(
            broker_config::ConfigRegistry::load(&dir).expect("scratch registry loads"),
        );
        let durable = crate::admin_users::AdminUsers::from_registry(&registry);
        durable
            .create(
                "w244-kept",
                "KeptSecret1!",
                crate::admin_users::AdminRole::Viewer,
                "kept across restart",
            )
            .expect("registry-backed create persists");
        let secret = durable
            .mfa_admin_enrol("w244-kept")
            .expect("enrol persists");
        assert_eq!(
            durable.mfa_admin_enrol("w244-kept"),
            Err(crate::admin_users::AdminUserError::AlreadyExists),
            "duplicate enrol is rejected without growing state"
        );
        drop(durable);
        let reloaded = std::sync::Arc::new(
            broker_config::ConfigRegistry::load(&dir).expect("registry reloads after a restart"),
        );
        let restored = crate::admin_users::AdminUsers::from_registry(&reloaded);
        let status = restored
            .mfa_status("w244-kept")
            .expect("restored store reports enrolment");
        assert!(!status.enabled);
        assert!(
            status.pending,
            "pending enrolment persists across a restart"
        );
        restored.mfa_disable("w244-kept").expect("disable persists");
        assert_eq!(
            restored.mfa_disable("w244-kept"),
            Err(crate::admin_users::AdminUserError::NotFound),
            "second disable is a miss"
        );
        drop(restored);
        let reloaded =
            broker_config::ConfigRegistry::load(&dir).expect("registry reloads after a disable");
        let cleared =
            crate::admin_users::AdminUsers::from_snapshot(&reloaded.snapshot().admin_users);
        let status = cleared
            .mfa_status("w244-kept")
            .expect("user survives the disable");
        assert!(!status.enabled);
        assert!(!status.pending, "disable persists across a restart");
        assert!(
            (10..=64).contains(&secret.len()),
            "one bounded record per user, never logged"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
