use super::*;

/// A viewer can enrol MFA for that viewer. A viewer cannot start or
/// read the enrolment of a different user.
#[tokio::test]
async fn a_viewer_enrols_mfa_only_for_that_viewer() {
    let (server, state) = TestServer::start().await;
    state
        .admin_users
        .create(
            "watcher",
            "Watch3r-test-pass!",
            crate::admin_users::AdminRole::Viewer,
            "",
        )
        .expect("viewer");
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "watcher", "password": "Watch3r-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200, "viewer login: {body}");
    let viewer = body["token"].as_str().expect("token").to_string();

    for path in ["/api/v5/sso/mfa/setup", "/api/v5/sso/mfa/setup_info"] {
        let (status, body) = server
            .post_auth(path, json!({"username": "admin"}), &viewer)
            .await;
        assert_eq!(status, 403, "{path} for a different user: {body}");
        assert!(body.get("secret").is_none());
    }
    let (status, _) = server
        .post_auth(
            "/api/v5/sso/mfa/verify",
            json!({"username": "admin", "code": "000000"}),
            &viewer,
        )
        .await;
    assert_eq!(status, 403);
    let admin_status = state.admin_users.mfa_status("admin").expect("admin");
    assert!(!admin_status.pending && !admin_status.enabled);

    let (status, setup) = server
        .post_auth(
            "/api/v5/sso/mfa/setup",
            json!({"username": "watcher"}),
            &viewer,
        )
        .await;
    assert_eq!(status, 200, "own setup: {setup}");
    let secret = crate::mfa_totp::base32_decode(setup["secret"].as_str().expect("secret"))
        .expect("base32 secret");
    let step = crate::mfa_totp::totp_counter(crate::mfa_totp::unix_now_secs());
    let (status, body) = server
        .post_auth(
            "/api/v5/sso/mfa/verify",
            json!({"username": "watcher", "code": crate::mfa_totp::totp_code(&secret, step)}),
            &viewer,
        )
        .await;
    assert_eq!(status, 200, "own verify: {body}");
    assert!(
        state
            .admin_users
            .mfa_status("watcher")
            .expect("viewer")
            .enabled
    );

    // The other write routes stay closed for a viewer.
    let (status, _) = server
        .post_auth("/api/v5/api_key", json!({"name": "k"}), &viewer)
        .await;
    assert_eq!(status, 403);
}

/// A user with MFA cannot log in with the password only. The login
/// needs a correct code, and one code works one time.
#[tokio::test]
async fn login_needs_the_mfa_code_when_the_user_has_mfa() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Enrol: get the secret, then confirm it with a code.
    let (status, setup) = server
        .post_auth(
            "/api/v5/sso/mfa/setup",
            json!({"username": "admin"}),
            &token,
        )
        .await;
    assert_eq!(status, 200, "setup: {setup}");
    let secret = crate::mfa_totp::base32_decode(setup["secret"].as_str().expect("secret"))
        .expect("base32 secret");
    let now = crate::mfa_totp::unix_now_secs();
    let step = crate::mfa_totp::totp_counter(now);
    // The code of the step before this one is in the accepted window,
    // and it leaves the code of this step for the login below.
    let enrol_code = crate::mfa_totp::totp_code(&secret, step - 1);
    let (status, body) = server
        .post_auth(
            "/api/v5/sso/mfa/verify",
            json!({"username": "admin", "code": enrol_code}),
            &token,
        )
        .await;
    assert_eq!(status, 200, "verify: {body}");

    let password = json!({"username": "admin", "password": "Adm1n-test-pass!"});
    // The password alone is not sufficient.
    let (status, body) = server.post("/api/v5/login", password.clone()).await;
    assert_eq!(status, 401);
    assert_eq!(body["code"], json!("BAD_MFA_TOKEN"));
    assert!(body.get("token").is_none(), "no token without the code");

    // An incorrect code is refused.
    let mut wrong = password.clone();
    wrong["mfa_token"] = json!("000000");
    let login_code = crate::mfa_totp::totp_code(&secret, step);
    if login_code != "000000" {
        let (status, body) = server.post("/api/v5/login", wrong).await;
        assert_eq!(status, 401);
        assert_eq!(body["code"], json!("BAD_MFA_TOKEN"));
    }

    // The correct code gives a token.
    let mut good = password.clone();
    good["mfa_token"] = json!(login_code);
    let (status, body) = server.post("/api/v5/login", good.clone()).await;
    assert_eq!(status, 200, "login with the code: {body}");
    assert!(body["token"].as_str().is_some_and(|t| !t.is_empty()));

    // The same code does not work a second time.
    let (status, body) = server.post("/api/v5/login", good).await;
    assert_eq!(status, 401);
    assert_eq!(body["code"], json!("BAD_MFA_TOKEN"));
}

/// After five incorrect passwords the user is locked for a time, and
/// the correct password does not work during that time. A name that
/// is not a user gets the usual refusal and is not locked.
#[tokio::test]
async fn repeated_failed_logins_lock_the_user() {
    let (server, _state) = TestServer::start().await;
    for _ in 0..crate::admin_users::MFA_VERIFY_MAX_ATTEMPTS {
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "not-the-password"}),
            )
            .await;
        assert_eq!(status, 401);
        assert_eq!(body["code"], json!("BAD_USERNAME_OR_PWD"));
    }
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "public"}),
        )
        .await;
    assert_eq!(status, 401, "locked: {body}");
    assert_eq!(body["code"], json!("LOGIN_LOCKED"));
    assert!(body.get("token").is_none());

    for _ in 0..(crate::admin_users::MFA_VERIFY_MAX_ATTEMPTS + 2) {
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "no-such-user", "password": "x"}),
            )
            .await;
        assert_eq!(status, 401, "an unknown name is not locked: {body}");
    }
}

/// Minimal SCRAM-SHA-256 client for the login tests (independent of the
/// server's credential store).
mod scram_client {
    use base64::engine::general_purpose::STANDARD as BASE64;
    use base64::Engine;
    use sha2::{Digest, Sha256};

    #[cfg(test)]
    fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
        let mut padded = [0u8; 64];
        if key.len() > 64 {
            let hash = Sha256::digest(key);
            padded[..32].copy_from_slice(&hash);
        } else {
            padded[..key.len()].copy_from_slice(key);
        }
        let mut ipad = [0x36u8; 64];
        let mut opad = [0x5cu8; 64];
        for i in 0..64 {
            ipad[i] ^= padded[i];
            opad[i] ^= padded[i];
        }
        let mut inner = Sha256::new();
        inner.update(ipad);
        inner.update(data);
        let inner_hash = inner.finalize();
        let mut outer = Sha256::new();
        outer.update(opad);
        outer.update(inner_hash);
        outer.finalize().into()
    }

    #[cfg(test)]
    fn salted_password(password: &str, salt: &[u8], iterations: u32) -> [u8; 32] {
        let mut block = Vec::with_capacity(salt.len() + 4);
        block.extend_from_slice(salt);
        block.extend_from_slice(&1u32.to_be_bytes());
        let mut u = hmac(password.as_bytes(), &block);
        let mut result = u;
        for _ in 1..iterations {
            u = hmac(password.as_bytes(), &u);
            for (r, v) in result.iter_mut().zip(u.iter()) {
                *r ^= *v;
            }
        }
        result
    }

    /// Returns `(client_proof_b64, server_signature_b64)`.
    #[cfg(test)]
    pub(super) fn proof(
        password: &str,
        salt_b64: &str,
        iterations: u32,
        auth_message: &str,
    ) -> (String, String) {
        let salt = BASE64.decode(salt_b64).expect("challenge salt");
        let salted = salted_password(password, &salt, iterations);
        let client_key = hmac(&salted, b"Client Key".as_slice());
        let stored_key: [u8; 32] = Sha256::digest(client_key).into();
        let client_sig = hmac(&stored_key, auth_message.as_bytes());
        let mut proof = [0u8; 32];
        for i in 0..32 {
            proof[i] = client_key[i] ^ client_sig[i];
        }
        let server_key = hmac(&salted, b"Server Key");
        let server_sig = hmac(&server_key, auth_message.as_bytes());
        (BASE64.encode(proof), BASE64.encode(server_sig))
    }
}

#[cfg(test)]
fn scram_auth_message(
    username: &str,
    client_nonce: &str,
    combined_nonce: &str,
    salt_b64: &str,
    iterations: u32,
) -> String {
    let escaped = username.replace('=', "=3D").replace(',', "=2C");
    format!(
            "n={escaped},r={client_nonce},r={combined_nonce},s={salt_b64},i={iterations},c=biws,r={combined_nonce}"
        )
}

#[tokio::test]
async fn login_default_admin_requires_password_change() {
    let (server, _state) = TestServer::start().await;
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "public"}),
        )
        .await;
    assert_eq!(status, 200);
    assert!(body["token"].as_str().is_some_and(|t| !t.is_empty()));
    assert_eq!(body["role"], json!("administrator"));
    assert_eq!(body["must_change_password"], json!(true));
}

#[tokio::test]
async fn login_wrong_password_is_rejected_and_creates_nothing() {
    let (server, state) = TestServer::start().await;

    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "wrong-password"}),
        )
        .await;
    assert_eq!(status, 401);
    assert_eq!(body["code"], json!("BAD_USERNAME_OR_PWD"));

    // An unknown user is rejected and appears in neither store.
    let (status, _) = server
        .post(
            "/api/v5/login",
            json!({"username": "ghost", "password": "whatever-123"}),
        )
        .await;
    assert_eq!(status, 401);
    assert!(state.admin_users.get("ghost").is_none());
    assert_eq!(state.auth.user_count(), 0);
}

#[tokio::test]
async fn login_issues_usable_token_and_rejects_bad_and_malformed() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (server, state) = TestServer::start().await;
    let admin = server.login_as_admin().await;

    // Create a fresh administrator through the broker, then log in as
    // it: the issued token must open a follow-up management call.
    let (status, _) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "w229-op", "password": "W229-op-pass!", "role": "administrator"}),
            &admin,
        )
        .await;
    assert_eq!(status, 200);
    let before = state.tokens.len();
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "w229-op", "password": "W229-op-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    let token = body["token"]
        .as_str()
        .expect("login issues a token")
        .to_string();
    assert!(!token.is_empty(), "issued token must not be empty");
    assert_eq!(body["role"], json!("administrator"));
    assert_eq!(
        state.tokens.len(),
        before + 1,
        "one successful login mints exactly one entry"
    );

    // The token carries the documented 3600 s TTL ...
    let info = state.tokens.lookup(&token).expect("fresh token looks up");
    let remaining = info
        .expires_at
        .saturating_duration_since(std::time::Instant::now());
    assert!(
        remaining <= std::time::Duration::from_secs(3600),
        "remaining TTL {remaining:?} must not exceed the documented 3600 s"
    );
    assert!(
        remaining > std::time::Duration::from_secs(3500),
        "remaining TTL {remaining:?} must sit at the documented 3600 s"
    );

    // ... authorises a follow-up call ...
    let (status, me) = server.get_auth("/api/v5/current_user", &token).await;
    assert_eq!(status, 200);
    assert_eq!(me["username"], json!("w229-op"));

    // ... and the success response sets no cookies: the token travels
    // in the JSON body only.
    let raw_login =
        serde_json::to_string(&json!({"username": "w229-op", "password": "W229-op-pass!"}))
            .expect("encode login body");
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", server.port))
        .await
        .expect("connect test server");
    stream
            .write_all(
                format!(
                    "POST /api/v5/login HTTP/1.0\r\nHost: test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{raw_login}",
                    raw_login.len()
                )
                .as_bytes(),
            )
            .await
            .expect("write login request");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read response");
    let text = String::from_utf8(buf).expect("response is UTF-8");
    assert!(
        !text.to_ascii_lowercase().contains("set-cookie"),
        "login must not set cookies, got: {text}"
    );
    assert_eq!(
        state.tokens.len(),
        before + 2,
        "the raw-socket success above mints one more entry"
    );

    // Wrong password: the documented error, no token, no new entry.
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "w229-op", "password": "Wr0ng-pass!"}),
        )
        .await;
    assert_eq!(status, 401);
    assert_eq!(body["code"], json!("BAD_USERNAME_OR_PWD"));
    assert_eq!(body.as_object().expect("error is an object").len(), 2);
    assert!(
        body.get("token").is_none(),
        "a failed login issues no token"
    );
    assert_eq!(
        state.tokens.len(),
        before + 2,
        "failed logins create no entries"
    );

    // Unknown user: same refusal, nothing created anywhere.
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "w229-ghost", "password": "whatever-123"}),
        )
        .await;
    assert_eq!(status, 401);
    assert_eq!(body["code"], json!("BAD_USERNAME_OR_PWD"));
    assert!(body.get("token").is_none());
    assert!(state.admin_users.get("w229-ghost").is_none());
    assert_eq!(state.tokens.len(), before + 2);

    // Malformed bodies are client errors in the documented shape and
    // create nothing: a missing field ...
    let (status, body) = server
        .post("/api/v5/login", json!({"username": "w229-op"}))
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    assert!(body.get("message").is_some());
    assert_eq!(body.as_object().expect("error is an object").len(), 2);
    assert!(body.get("token").is_none());
    assert_eq!(state.tokens.len(), before + 2);

    // ... empty credentials ...
    let (status, body) = server
        .post("/api/v5/login", json!({"username": "", "password": ""}))
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    assert!(body.get("token").is_none());
    assert_eq!(state.tokens.len(), before + 2);

    // ... and a body that is not JSON at all.
    let (status, raw) = server
            .request_raw(
                "POST /api/v5/login HTTP/1.0\r\nHost: test\r\nContent-Type: application/json\r\nContent-Length: 10\r\nConnection: close\r\n\r\n{not json!",
            )
            .await;
    assert_eq!(status, 400);
    let malformed: serde_json::Value =
        serde_json::from_str(&raw).expect("malformed login answers JSON");
    assert_eq!(malformed["code"], json!("BAD_REQUEST"));
    assert!(malformed.get("token").is_none());
    assert_eq!(state.tokens.len(), before + 2);

    // Logout revokes the issued token: the follow-up call fails after.
    let (status, _) = server
        .send("POST", "/api/v5/logout", None, Some(&token))
        .await;
    assert_eq!(status, 204);
    let (status, body) = server.get_auth("/api/v5/current_user", &token).await;
    assert_eq!(status, 401);
    assert_eq!(body["code"], json!("UNAUTHORIZED"));
}

#[tokio::test]
async fn logout_revokes_and_rejects_reuse_and_missing() {
    // Broker-through logout (C05-09): log in, call an authorized
    // route, log out, then retry the same route with the old token.
    // Before the fix the second logout still reported success; now a
    // missing or already-revoked token is a client error, never 204.
    // Writes to the bounded live-token map in `v5/auth.rs`
    // (`ApiTokens::issue` on login, `ApiTokens::revoke` on logout);
    // revoked entries are removed, not retained. Driven by the login
    // POST, the authorized GET, and the logout POST management calls.
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let live_before = state.tokens.len();

    // The fresh token authorizes a management call.
    let (status, _) = server.get_auth("/api/v5/current_user", &token).await;
    assert_eq!(status, 200);

    // Logout revokes it: 204 with an empty body.
    let (status, body) = server
        .send("POST", "/api/v5/logout", None, Some(&token))
        .await;
    assert_eq!(status, 204);
    assert_eq!(body, serde_json::Value::Null);
    assert!(
        state.tokens.lookup(&token).is_none(),
        "logout must remove the entry, not retain it"
    );
    assert_eq!(
        state.tokens.len(),
        live_before - 1,
        "logout must shrink the live-token map"
    );

    // Reuse of the old token is rejected with the documented shape.
    let (status, body) = server.get_auth("/api/v5/current_user", &token).await;
    assert_eq!(status, 401);
    assert_eq!(body["code"], json!("UNAUTHORIZED"));
    assert!(body.get("message").is_some());
    assert_eq!(body.as_object().expect("error is an object").len(), 2);

    // A second logout with the already-revoked token is a client
    // error, not success.
    let (status, body) = server
        .send("POST", "/api/v5/logout", None, Some(&token))
        .await;
    assert_eq!(status, 401);
    assert_eq!(body["code"], json!("UNAUTHORIZED"));
    assert_eq!(body.as_object().expect("error is an object").len(), 2);

    // A logout with no token at all is a client error, not success.
    let (status, body) = server.send("POST", "/api/v5/logout", None, None).await;
    assert_eq!(status, 401);
    assert_eq!(body["code"], json!("UNAUTHORIZED"));
    assert_eq!(body.as_object().expect("error is an object").len(), 2);
}

#[tokio::test]
async fn scram_login_end_to_end() {
    let (server, _state) = TestServer::start().await;
    let client_nonce = "testclientnonce123";

    // Correct password: challenge, proof, verify.
    let (status, challenge) = server
        .post(
            "/api/v5/login/challenge",
            json!({"username": "admin", "client_nonce": client_nonce}),
        )
        .await;
    assert_eq!(status, 200);
    let challenge_id = challenge["challenge_id"].as_str().expect("challenge id");
    let server_nonce = challenge["server_nonce"].as_str().expect("server nonce");
    let salt_b64 = challenge["salt"].as_str().expect("salt");
    let iterations = challenge["iterations"].as_u64().expect("iterations") as u32;
    let combined = format!("{client_nonce}{server_nonce}");
    let auth_message = scram_auth_message("admin", client_nonce, &combined, salt_b64, iterations);
    let (proof_b64, server_sig_b64) =
        scram_client::proof("public", salt_b64, iterations, &auth_message);

    let (status, body) = server
        .post(
            "/api/v5/login/verify",
            json!({
                "challenge_id": challenge_id,
                "combined_nonce": combined,
                "client_proof": proof_b64,
            }),
        )
        .await;
    assert_eq!(status, 200);
    assert!(body["token"].as_str().is_some_and(|t| !t.is_empty()));
    assert_eq!(body["server_signature"], json!(server_sig_b64));

    // Wrong password: a fresh challenge, then the proof is rejected.
    let (status, challenge) = server
        .post(
            "/api/v5/login/challenge",
            json!({"username": "admin", "client_nonce": client_nonce}),
        )
        .await;
    assert_eq!(status, 200);
    let challenge_id = challenge["challenge_id"].as_str().expect("challenge id");
    let salt_b64 = challenge["salt"].as_str().expect("salt");
    let iterations = challenge["iterations"].as_u64().expect("iterations") as u32;
    let auth_message = scram_auth_message("admin", client_nonce, &combined, salt_b64, iterations);
    let (bad_proof, _) = scram_client::proof("wrong-password", salt_b64, iterations, &auth_message);
    let (status, body) = server
        .post(
            "/api/v5/login/verify",
            json!({
                "challenge_id": challenge_id,
                "combined_nonce": combined,
                "client_proof": bad_proof,
            }),
        )
        .await;
    assert_eq!(status, 401);
    assert_eq!(body["code"], json!("BAD_USERNAME_OR_PWD"));
}

#[tokio::test]
async fn current_user_reflects_token() {
    let (server, _state) = TestServer::start().await;
    let admin_token = server.login_as_admin().await;

    // Create a viewer through the admin API.
    let (status, _) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "watcher", "password": "W4tcher-pass", "role": "viewer"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200);

    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "watcher", "password": "W4tcher-pass"}),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["role"], json!("viewer"));
    let viewer_token = body["token"].as_str().expect("viewer token").to_string();

    let (status, body) = server.get_auth("/api/v5/current_user", &viewer_token).await;
    assert_eq!(status, 200);
    assert_eq!(body["username"], json!("watcher"));
    assert_eq!(body["role"], json!("viewer"));

    // No token is a 401.
    let (status, body) = server.get("/api/v5/current_user").await;
    assert_eq!(status, 401);
    assert_eq!(body["code"], json!("UNAUTHORIZED"));
}

#[tokio::test]
async fn change_pwd_flow() {
    let (server, _state) = TestServer::start().await;
    let admin_token = server.admin_token().await;
    // A second admin token that must die on password change.
    let (status, other) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "public"}),
        )
        .await;
    assert_eq!(status, 200);
    let other_token = other["token"].as_str().expect("second token").to_string();
    assert_ne!(admin_token, other_token);

    // Admin changes their own password from the default.
    let (status, _) = server
        .put_auth(
            "/api/v5/users/admin/change_pwd",
            json!({"old_pwd": "public", "new_pwd": "N3w-passw0rd!"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 204);

    // Flag cleared on the caller's surviving token.
    let (status, body) = server.get_auth("/api/v5/current_user", &admin_token).await;
    assert_eq!(status, 200);
    assert_eq!(body["must_change_password"], json!(false));

    // Old password no longer logs in; the other token is revoked.
    let (status, _) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "public"}),
        )
        .await;
    assert_eq!(status, 401);
    let (status, _) = server.get_auth("/api/v5/current_user", &other_token).await;
    assert_eq!(status, 401);

    // A viewer cannot change another user's password.
    let (status, _) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "watcher", "password": "W4tcher-pass", "role": "viewer"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "watcher", "password": "W4tcher-pass"}),
        )
        .await;
    assert_eq!(status, 200);
    let viewer_token = body["token"].as_str().expect("viewer token").to_string();
    let (status, body) = server
        .put_auth(
            "/api/v5/users/admin/change_pwd",
            json!({"old_pwd": "N3w-passw0rd!", "new_pwd": "An0ther-pass!"}),
            &viewer_token,
        )
        .await;
    assert_eq!(status, 403);
    assert_eq!(body["code"], json!("FORBIDDEN"));
}

#[tokio::test]
async fn change_pwd_accepts_post() {
    let (server, _state) = TestServer::start().await;
    let fresh = server.admin_token().await;
    let (status, _) = server
        .post_auth(
            "/api/v5/users/admin/change_pwd",
            json!({"old_pwd": "public", "new_pwd": "N3w-post-passw0rd!"}),
            &fresh,
        )
        .await;
    assert_eq!(status, 204);
    let (status, _) = server.get_auth("/api/v1/clients", &fresh).await;
    assert_eq!(status, 200);
}

/// An administrator resets the password of a different user without
/// the old password. The same user must prove the old password. A
/// refused update keeps the old password. Old sessions end.
#[tokio::test]
async fn mv09c_admin_reset_needs_no_old_password_own_change_does() {
    // MV-09c broker-through test: every step uses the served
    // management API on `ApiState` (the same state the kernel shares
    // in `crates/broker-node/src/main.rs`, `serve_api`), not the
    // store. Management-plane only; publish and deliver never touch
    // these stores.
    let (server, _state) = TestServer::start().await;
    let admin = server.login_as_admin().await;

    // Create one viewer through the router.
    let (status, _) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "mv09c-reset", "password": "OldSecret1!", "role": "viewer"}),
            &admin,
        )
        .await;
    assert_eq!(status, 200);

    // Two live viewer sessions on the login path.
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "mv09c-reset", "password": "OldSecret1!"}),
        )
        .await;
    assert_eq!(status, 200);
    let viewer_old = body["token"].as_str().expect("viewer token").to_string();
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "mv09c-reset", "password": "OldSecret1!"}),
        )
        .await;
    assert_eq!(status, 200);
    let viewer_other = body["token"].as_str().expect("second token").to_string();
    assert_ne!(viewer_old, viewer_other);

    // An administrator reset needs no old password.
    let (status, body) = server
        .put_auth(
            "/api/v5/users/mv09c-reset/change_pwd",
            json!({"new_pwd": "NewSecret2!"}),
            &admin,
        )
        .await;
    assert_eq!(status, 204, "{body}");
    assert!(body.is_null());

    // The new password logs in; the old one does not.
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "mv09c-reset", "password": "NewSecret2!"}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    let viewer = body["token"].as_str().expect("new token").to_string();
    let (status, _) = server
        .post(
            "/api/v5/login",
            json!({"username": "mv09c-reset", "password": "OldSecret1!"}),
        )
        .await;
    assert_eq!(status, 401);

    // Both old sessions end: the reset revokes every live token of
    // the user.
    let (status, _) = server.get_auth("/api/v5/current_user", &viewer_old).await;
    assert_eq!(status, 401);
    let (status, _) = server.get_auth("/api/v5/current_user", &viewer_other).await;
    assert_eq!(status, 401);

    // An own change without the old password is refused.
    let (status, body) = server
        .put_auth(
            "/api/v5/users/mv09c-reset/change_pwd",
            json!({"new_pwd": "ThirdSecret3!"}),
            &viewer,
        )
        .await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["code"], json!("ERROR_PWD_NOT_MATCH"));

    // An own change with a wrong old password is refused.
    let (status, body) = server
        .put_auth(
            "/api/v5/users/mv09c-reset/change_pwd",
            json!({"old_pwd": "Wrong-old-1!", "new_pwd": "ThirdSecret3!"}),
            &viewer,
        )
        .await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["code"], json!("ERROR_PWD_NOT_MATCH"));

    // A weak new password is refused and keeps the old one.
    let (status, body) = server
        .put_auth(
            "/api/v5/users/mv09c-reset/change_pwd",
            json!({"old_pwd": "NewSecret2!", "new_pwd": "short"}),
            &viewer,
        )
        .await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    let (status, _) = server
        .post(
            "/api/v5/login",
            json!({"username": "mv09c-reset", "password": "NewSecret2!"}),
        )
        .await;
    assert_eq!(status, 200);

    // A second session of the same user, then an own change with the
    // old password. The other session ends; the caller keeps working.
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "mv09c-reset", "password": "NewSecret2!"}),
        )
        .await;
    assert_eq!(status, 200);
    let viewer_second = body["token"].as_str().expect("second token").to_string();
    let (status, body) = server
        .put_auth(
            "/api/v5/users/mv09c-reset/change_pwd",
            json!({"old_pwd": "NewSecret2!", "new_pwd": "ThirdSecret3!"}),
            &viewer,
        )
        .await;
    assert_eq!(status, 204, "{body}");
    let (status, _) = server
        .get_auth("/api/v5/current_user", &viewer_second)
        .await;
    assert_eq!(status, 401);
    let (status, _) = server.get_auth("/api/v5/current_user", &viewer).await;
    assert_eq!(status, 200);
    let (status, _) = server
        .post(
            "/api/v5/login",
            json!({"username": "mv09c-reset", "password": "ThirdSecret3!"}),
        )
        .await;
    assert_eq!(status, 200);
}

/// The router refuses a wrong MFA code.
/// The router refuses a used code.
#[tokio::test]
async fn mv09c_mfa_verify_refuses_wrong_and_replayed_codes_via_http() {
    // MV-09c broker-through test: setup runs through the served
    // management API on `ApiState`. Verify runs through the same API.
    // The kernel shares this state in `serve_api`. The test avoids
    // the store. Management-plane only; publish and deliver
    // never touch these maps. Codes and secrets never reach logs.
    let (server, _state) = TestServer::start().await;
    let admin = server.login_as_admin().await;
    let (status, _) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "mv09c-mfa", "password": "MfaSecret1!", "role": "viewer"}),
            &admin,
        )
        .await;
    assert_eq!(status, 200);

    // Setup enrols and returns the secret one time.
    let (status, setup) = server
        .post_auth(
            "/api/v5/sso/mfa/setup",
            json!({"username": "mv09c-mfa"}),
            &admin,
        )
        .await;
    assert_eq!(status, 200, "{setup}");
    let secret = crate::mfa_totp::base32_decode(setup["secret"].as_str().expect("secret"))
        .expect("setup returns base32");

    // A wrong code is refused and does not enable.
    let step = crate::mfa_totp::totp_counter(crate::mfa_totp::unix_now_secs());
    let good = crate::mfa_totp::totp_code(&secret, step);
    let mut wrong = good.clone();
    wrong.replace_range(0..1, if &good[0..1] == "0" { "1" } else { "0" });
    assert_ne!(wrong, good);
    let (status, body) = server
        .post_auth(
            "/api/v5/sso/mfa/verify",
            json!({"username": "mv09c-mfa", "code": wrong}),
            &admin,
        )
        .await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["code"], json!("BAD_REQUEST"));

    // A correct code enables.
    let (status, body) = server
        .post_auth(
            "/api/v5/sso/mfa/verify",
            json!({"username": "mv09c-mfa", "code": good.clone()}),
            &admin,
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["enabled"], json!(true));

    // The same code cannot be used again.
    let (status, body) = server
        .post_auth(
            "/api/v5/sso/mfa/verify",
            json!({"username": "mv09c-mfa", "code": good}),
            &admin,
        )
        .await;
    assert_eq!(status, 400, "{body}");
    assert_eq!(body["code"], json!("BAD_REQUEST"));
}

#[tokio::test]
async fn lowercase_bearer_scheme_is_accepted() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, body) = server
            .request(&format!(
                "GET /api/v5/current_user HTTP/1.0\r\nHost: test\r\nAuthorization: bearer {token}\r\nConnection: close\r\n\r\n"
            ))
            .await;
    assert_eq!(status, 200);
    assert!(body["username"].is_string());
}

#[tokio::test]
async fn bare_token_without_scheme_is_rejected() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, body) = server
            .request(&format!(
                "GET /api/v5/current_user HTTP/1.0\r\nHost: test\r\nAuthorization: {token}\r\nConnection: close\r\n\r\n"
            ))
            .await;
    assert_eq!(status, 401);
    assert_eq!(body["code"], json!("UNAUTHORIZED"));
}

#[test]
fn is_own_change_pwd_percent_decodes_username() {
    use crate::api_auth::is_own_change_pwd;
    let post = axum::http::Method::POST;
    assert!(is_own_change_pwd(
        &post,
        "/api/v5/users/ops.team-1%40site/change_pwd",
        "ops.team-1@site"
    ));
    assert!(!is_own_change_pwd(
        &post,
        "/api/v5/users/ops.team-1%4/change_pwd",
        "ops.team-1@site"
    ));
}

#[cfg(test)]
fn fill_pending_challenges(tokens: &crate::v5::auth::ApiTokens, count: usize) {
    for i in 0..count {
        let inserted = tokens.insert_challenge(
            format!("fill-{i}"),
            crate::v5::auth::ScramChallengeState::for_test("admin"),
        );
        assert!(inserted, "insert {i} should succeed");
    }
}

#[tokio::test]
async fn challenge_cap_returns_429() {
    use crate::v5::auth::{ScramChallengeState, MAX_PENDING_CHALLENGES};
    let (server, state) = TestServer::start().await;
    fill_pending_challenges(&state.tokens, MAX_PENDING_CHALLENGES);
    assert!(
        !state.tokens.insert_challenge(
            "one-more".to_string(),
            ScramChallengeState::for_test("admin")
        ),
        "insert past the cap should fail"
    );
    let (status, body) = server
        .post(
            "/api/v5/login/challenge",
            json!({"username": "admin", "client_nonce": "nonce-123"}),
        )
        .await;
    assert_eq!(status, 429);
    assert_eq!(body["code"], json!("TOO_MANY_REQUESTS"));
}

#[tokio::test]
async fn api_rejects_requests_without_token() {
    let (server, _state) = TestServer::start().await;

    // Authenticated routes refuse anonymous callers.
    let (status, body) = server.get("/api/v1/clients").await;
    assert_eq!(status, 401);
    assert_eq!(body["code"], json!("UNAUTHORIZED"));
    let (status, body) = server.get("/api/v5/rules").await;
    assert_eq!(status, 401);
    assert_eq!(body["code"], json!("UNAUTHORIZED"));

    // Public routes stay open.
    let (status, _) = server
        .request_raw("GET /healthz HTTP/1.0\r\nHost: test\r\nConnection: close\r\n\r\n")
        .await;
    assert_eq!(status, 200);
    let (status, _) = server
        .request_raw("GET /dashboard HTTP/1.0\r\nHost: test\r\nConnection: close\r\n\r\n")
        .await;
    assert_eq!(status, 200);
}

#[tokio::test]
async fn default_admin_must_change_password_first() {
    let (server, _state) = TestServer::start().await;
    let fresh = server.admin_token().await;

    // The default-password token is gated everywhere but the
    // password change, logout and current_user.
    let (status, body) = server.get_auth("/api/v1/clients", &fresh).await;
    assert_eq!(status, 403);
    assert_eq!(body["code"], json!("PASSWORD_CHANGE_REQUIRED"));
    let (status, _) = server.get_auth("/api/v5/current_user", &fresh).await;
    assert_eq!(status, 200);

    let (status, _) = server
        .put_auth(
            "/api/v5/users/admin/change_pwd",
            json!({"old_pwd": "public", "new_pwd": "Adm1n-test-pass!"}),
            &fresh,
        )
        .await;
    assert_eq!(status, 204);

    // After the change the same token opens the API.
    let (status, _) = server.get_auth("/api/v1/clients", &fresh).await;
    assert_eq!(status, 200);
}

#[tokio::test]
async fn viewer_is_read_only() {
    let (server, _state) = TestServer::start().await;
    let admin = server.login_as_admin().await;

    let (status, _) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "ro-view", "password": "R0-viewer-pass", "role": "viewer"}),
            &admin,
        )
        .await;
    assert_eq!(status, 200);
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "ro-view", "password": "R0-viewer-pass"}),
        )
        .await;
    assert_eq!(status, 200);
    let viewer = body["token"].as_str().expect("viewer token").to_string();

    // Reads work; writes are forbidden.
    let (status, _) = server.get_auth("/api/v5/rules", &viewer).await;
    assert_eq!(status, 200);
    let (status, body) = server
        .post_auth(
            "/api/v1/rules",
            json!({"name": "x", "topic_filter": "a/#", "actions": []}),
            &viewer,
        )
        .await;
    assert_eq!(status, 403);
    assert_eq!(body["code"], json!("FORBIDDEN"));
}

#[tokio::test]
async fn expired_token_is_rejected() {
    let (server, state) = TestServer::start().await;
    let stale = state
        .tokens
        .issue_expired("admin", crate::admin_users::AdminRole::Administrator);
    let (status, body) = server.get_auth("/api/v1/clients", &stale).await;
    assert_eq!(status, 401);
    assert_eq!(body["code"], json!("UNAUTHORIZED"));
}

#[tokio::test]
async fn login_response_has_no_invented_fields() {
    let (server, _state) = TestServer::start().await;
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "public"}),
        )
        .await;
    assert_eq!(status, 200);
    assert!(body["token"].as_str().is_some_and(|t| !t.is_empty()));
    assert_eq!(body["role"], json!("administrator"));
    assert_eq!(body["must_change_password"], json!(true));
    assert!(
        body.get("version").is_none(),
        "login must not invent version"
    );
    assert!(
        body.get("license").is_none(),
        "login must not invent license"
    );
}

#[tokio::test]
async fn change_pwd_returns_documented_status() {
    let (server, _state) = TestServer::start().await;
    let fresh = server.admin_token().await;
    let (status, body) = server
        .put_auth(
            "/api/v5/users/admin/change_pwd",
            json!({"old_pwd": "public", "new_pwd": "Tk02-new-pass!"}),
            &fresh,
        )
        .await;
    assert_eq!(status, 204);
    assert_eq!(body, json!(null));
}
