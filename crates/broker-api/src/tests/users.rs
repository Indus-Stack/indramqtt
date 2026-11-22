use super::*;

#[tokio::test]
async fn test_auth_users_and_acls() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Create a user.
    let (status, created) = server
        .post_auth(
            "/api/v1/auth/users",
            json!({"username": "alice", "password": "s3cret"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["username"], json!("alice"));
    // Passwords are write-only: never echoed.
    assert!(created.get("password").is_none());
    assert_eq!(state.auth.user_count(), 1);

    // Empty username or password is rejected.
    let (status, _) = server
        .post_auth(
            "/api/v1/auth/users",
            json!({"username": "", "password": "x"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    let (status, _) = server
        .post_auth(
            "/api/v1/auth/users",
            json!({"username": "bob", "password": ""}),
            &token,
        )
        .await;
    assert_eq!(status, 400);

    // Create an ACL rule.
    let (status, created) = server
        .post_auth(
            "/api/v1/auth/acls",
            json!({"client_pattern": "alice",
                       "action": "publish",
                       "topic_pattern": "sensors/#",
                       "allow": true}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["allow"], json!(true));

    // Invalid action, topic pattern, and client pattern rejected.
    for body in [
        json!({"client_pattern": "*", "action": "delete", "topic_pattern": "#", "allow": true}),
        json!({"client_pattern": "*", "action": "publish", "topic_pattern": "a/#/b", "allow": true}),
        json!({"client_pattern": "", "action": "all", "topic_pattern": "#", "allow": false}),
    ] {
        let (status, _) = server.post_auth("/api/v1/auth/acls", body, &token).await;
        assert_eq!(status, 400);
    }
}

#[tokio::test]
async fn test_auth_lists_users_and_acls() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, body) = server.get_auth("/api/v1/auth/users", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));
    let (status, body) = server.get_auth("/api/v1/auth/acls", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));

    server
        .post_auth(
            "/api/v1/auth/users",
            json!({"username": "alice", "password": "s3cret"}),
            &token,
        )
        .await;
    server
        .post_auth(
            "/api/v1/auth/acls",
            json!({"client_pattern": "alice",
                       "action": "subscribe",
                       "topic_pattern": "sensors/#",
                       "allow": true}),
            &token,
        )
        .await;

    let (status, body) = server.get_auth("/api/v1/auth/users", &token).await;
    assert_eq!(status, 200);
    assert_eq!(
        body,
        json!([{"username": "alice", "quotas": {"max_connections": null, "max_publish_rate": null, "max_publish_burst": null}}])
    );
    let (status, body) = server.get_auth("/api/v1/auth/acls", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().expect("acls").len(), 1);
    assert_eq!(body[0]["client_pattern"], json!("alice"));
    assert_eq!(body[0]["action"], json!("subscribe"));
    assert_eq!(body[0]["allow"], json!(true));
}

#[tokio::test]
async fn test_auth_users_quotas_roundtrip() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Create with quotas: every bound echoes back.
    let (status, created) = server
        .post_auth(
            "/api/v1/auth/users",
            json!({"username": "capped",
                       "password": "pw",
                       "quotas": {"max_connections": 100,
                                  "max_publish_rate": 50,
                                  "max_publish_burst": 10}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(created["username"], json!("capped"));
    assert_eq!(created["quotas"]["max_connections"], json!(100));
    assert_eq!(created["quotas"]["max_publish_rate"], json!(50));
    assert_eq!(created["quotas"]["max_publish_burst"], json!(10));

    // Without quotas: all-null bounds object.
    let (status, created) = server
        .post_auth(
            "/api/v1/auth/users",
            json!({"username": "plain", "password": "pw"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    assert_eq!(
        created["quotas"]["max_connections"],
        serde_json::Value::Null
    );

    // Listing shows both shapes side by side.
    let (status, body) = server.get_auth("/api/v1/auth/users", &token).await;
    assert_eq!(status, 200);
    let users = body.as_array().expect("users list");
    assert_eq!(users.len(), 2);
    assert_eq!(users[0]["username"], json!("capped"));
    assert_eq!(users[0]["quotas"]["max_connections"], json!(100));
    assert_eq!(users[1]["username"], json!("plain"));
    assert_eq!(
        users[1]["quotas"]["max_publish_rate"],
        serde_json::Value::Null
    );
}

#[tokio::test]
async fn admin_users_api_does_not_touch_mqtt_store() {
    let (server, state) = TestServer::start().await;
    let admin_token = server.login_as_admin().await;

    let (status, created) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "ops", "password": "Ops-passw0rd", "role": "viewer",
                       "description": "read-only"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(created["username"], json!("ops"));
    assert_eq!(created["role"], json!("viewer"));
    assert_eq!(created["description"], json!("read-only"));
    assert_eq!(state.auth.user_count(), 0);

    // Listing serves the admin store with roles, sorted by username,
    // as a bare array, as the reference API documents (C05-25).
    let (status, body) = server.get_auth("/api/v5/users", &admin_token).await;
    assert_eq!(status, 200);
    assert_eq!(body.as_array().map(Vec::len), Some(2));
    assert_eq!(body[0]["backend"], json!("local"));
    assert_eq!(body[0]["mfa"], json!("none"));
    let names: Vec<&str> = body
        .as_array()
        .expect("users data array")
        .iter()
        .map(|u| u["username"].as_str().expect("username"))
        .collect();
    assert_eq!(names, vec!["admin", "ops"]);

    // Role/description update and delete stay out of the MQTT store.
    let (status, updated) = server
        .put_auth(
            "/api/v5/users/ops",
            json!({"role": "administrator", "description": "on-call"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(updated["role"], json!("administrator"));
    assert_eq!(updated["description"], json!("on-call"));
    let (status, _) = server.delete_auth("/api/v5/users/ops", &admin_token).await;
    assert_eq!(status, 204);
    assert_eq!(state.auth.user_count(), 0);
}

#[tokio::test]
async fn poisoned_admin_user_store_lock_keeps_http_api_answering() {
    let (server, state) = TestServer::start().await;
    let admin_token = server.login_as_admin().await;

    // Poison the user-map lock: the thread panics while it holds the
    // write guard, so every later acquisition sees a poisoned lock.
    let store = Arc::clone(&state.admin_users);
    let poisoned = tokio::task::spawn_blocking(move || {
        store.poison_user_store_for_test();
    })
    .await;
    assert!(poisoned.is_err(), "the poison thread must panic");

    // A read through the poisoned lock still answers over HTTP.
    let (status, body) = server.get_auth("/api/v5/users", &admin_token).await;
    assert_eq!(status, 200);
    let names: Vec<&str> = body
        .as_array()
        .expect("users data array")
        .iter()
        .map(|user| user["username"].as_str().expect("username"))
        .collect();
    assert!(names.contains(&"admin"));

    // A write through the poisoned lock also recovers.
    let (status, created) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "after-poison", "password": "P0ison-pass", "role": "viewer"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(created["username"], json!("after-poison"));

    // The broker stays up: the next read sees the new user.
    let (status, body) = server.get_auth("/api/v5/users", &admin_token).await;
    assert_eq!(status, 200);
    let names: Vec<&str> = body
        .as_array()
        .expect("users data array")
        .iter()
        .map(|user| user["username"].as_str().expect("username"))
        .collect();
    assert!(names.contains(&"after-poison"));
}

#[tokio::test]
async fn demoted_admin_token_loses_admin_rights() {
    let (server, _state) = TestServer::start().await;
    let admin_token = server.login_as_admin().await;

    let (status, _) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "b-admin", "password": "B-passw0rd!", "role": "administrator"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200);

    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "b-admin", "password": "B-passw0rd!"}),
        )
        .await;
    assert_eq!(status, 200);
    let b_token = body["token"].as_str().expect("b token").to_string();

    let (status, _) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "probe", "password": "Probe-pass1", "role": "viewer"}),
            &b_token,
        )
        .await;
    assert_eq!(status, 200);

    let (status, _) = server
        .put_auth(
            "/api/v5/users/b-admin",
            json!({"role": "viewer", "scopes": "unset"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200);

    let (status, body) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "probe2", "password": "Probe-pass1", "role": "viewer"}),
            &b_token,
        )
        .await;
    assert_eq!(status, 403);
    assert_eq!(body["code"], json!("FORBIDDEN"));
}

#[tokio::test]
async fn deleted_user_token_is_rejected() {
    let (server, _state) = TestServer::start().await;
    let admin_token = server.login_as_admin().await;

    let (status, _) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "viewer-v", "password": "V-passw0rd!", "role": "viewer"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200);

    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "viewer-v", "password": "V-passw0rd!"}),
        )
        .await;
    assert_eq!(status, 200);
    let v_token = body["token"].as_str().expect("viewer token").to_string();

    let (status, _) = server.get_auth("/api/v5/current_user", &v_token).await;
    assert_eq!(status, 200);

    let (status, _) = server
        .delete_auth("/api/v5/users/viewer-v", &admin_token)
        .await;
    assert_eq!(status, 204);

    let (status, body) = server.get_auth("/api/v5/current_user", &v_token).await;
    assert_eq!(status, 401);
    assert_eq!(body["code"], json!("UNAUTHORIZED"));
}

/// MV-05: the create, update and read paths carry the dashboard user
/// scopes of the reference contract.
#[tokio::test]
async fn dashboard_user_scope_contract_round_trip() {
    let (server, _state) = TestServer::start().await;
    let admin_token = server.login_as_admin().await;
    let mut admin_scopes: Vec<&str> = broker_config::GENERIC_USER_SCOPES.to_vec();
    admin_scopes.extend_from_slice(&broker_config::LOGIN_ONLY_SCOPES);

    // An omitted scope list on create stores the role default.
    let (status, created) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "scope-admin-default", "password": "Scope-pass-1a",
                       "role": "administrator"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200, "create admin default: {created}");
    assert_eq!(created["scopes"], json!(admin_scopes));

    let (status, created) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "scope-viewer-default", "password": "Scope-pass-1a",
                       "role": "viewer"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200, "create viewer default: {created}");
    assert_eq!(created["scopes"], json!(broker_config::GENERIC_USER_SCOPES));

    // An empty list denies each management scope.
    let (status, created) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "scope-deny", "password": "Scope-pass-1a",
                       "role": "viewer", "scopes": []}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200, "create deny-all: {created}");
    assert_eq!(created["scopes"], json!([]));

    // `unset` and a list equal to the role default store no explicit
    // list, and the read renders the sentinel.
    let (status, created) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "scope-unset", "password": "Scope-pass-1a",
                       "role": "administrator", "scopes": "unset"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200, "create unset: {created}");
    assert_eq!(created["scopes"], json!("unset"));

    let (status, created) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "scope-default-list", "password": "Scope-pass-1a",
                       "role": "viewer", "scopes": broker_config::GENERIC_USER_SCOPES}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200, "create default-equivalent list: {created}");
    assert_eq!(created["scopes"], json!("unset"));

    // A restricted list stores and renders as written.
    let (status, created) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "scope-monitor", "password": "Scope-pass-1a",
                       "role": "viewer", "scopes": ["monitoring"]}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200, "create restricted list: {created}");
    assert_eq!(created["scopes"], json!(["monitoring"]));

    // A duplicate and one missing default scope do not normalise to
    // the full role default.
    let mut ambiguous: Vec<String> = broker_config::GENERIC_USER_SCOPES
        .iter()
        .map(|scope| (*scope).to_string())
        .collect();
    ambiguous.pop();
    ambiguous.push("connections".to_string());
    let (status, body) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "scope-duplicate", "password": "Scope-pass-1a",
                       "role": "viewer", "scopes": ambiguous}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 400, "duplicate scope: {body}");

    // Unknown and role-forbidden scopes fail closed.
    let (status, body) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "scope-unknown", "password": "Scope-pass-1a",
                       "role": "viewer", "scopes": ["not_a_scope"]}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 400, "unknown scope: {body}");
    let (status, body) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "scope-forbidden", "password": "Scope-pass-1a",
                       "role": "viewer", "scopes": ["user_management"]}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 400, "viewer user_management: {body}");

    // mfa_management is valid for a viewer.
    let (status, created) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "scope-mfa", "password": "Scope-pass-1a",
                       "role": "viewer", "scopes": ["mfa_management"]}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200, "viewer mfa_management: {created}");
    assert_eq!(created["scopes"], json!(["mfa_management"]));

    // Own MFA stays public. Another user's MFA needs mfa_management.
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "scope-mfa", "password": "Scope-pass-1a"}),
        )
        .await;
    assert_eq!(status, 200, "mfa user login: {body}");
    let mfa_token = body["token"].as_str().expect("mfa token").to_string();
    let (status, body) = server
        .post_auth(
            "/api/v5/sso/mfa/setup",
            json!({"username": "scope-mfa"}),
            &mfa_token,
        )
        .await;
    assert_eq!(status, 200, "own MFA: {body}");
    let (status, body) = server
        .post_auth(
            "/api/v5/sso/mfa/setup",
            json!({"username": "admin"}),
            &mfa_token,
        )
        .await;
    assert_eq!(status, 403, "viewer targets another user: {body}");

    for (name, scopes) in [
        ("scope-limited-admin", json!(["monitoring"])),
        ("scope-mfa-admin", json!(["mfa_management"])),
    ] {
        let (status, created) = server
            .post_auth(
                "/api/v5/users",
                json!({"username": name, "password": "Scope-pass-1a",
                           "role": "administrator", "scopes": scopes}),
                &admin_token,
            )
            .await;
        assert_eq!(status, 200, "create {name}: {created}");
    }
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "scope-limited-admin", "password": "Scope-pass-1a"}),
        )
        .await;
    assert_eq!(status, 200);
    let limited_token = body["token"].as_str().expect("limited token").to_string();
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "scope-mfa-admin", "password": "Scope-pass-1a"}),
        )
        .await;
    assert_eq!(status, 200);
    let mfa_admin_token = body["token"].as_str().expect("mfa admin token").to_string();

    // A limited administrator cannot target another user.
    let (status, body) = server
        .put_auth(
            "/api/v5/users/admin/change_pwd",
            json!({"old_pwd": "unused", "new_pwd": "Unused-pass-1a"}),
            &limited_token,
        )
        .await;
    assert_eq!(
        status, 403,
        "limited admin targets another password: {body}"
    );
    let (status, body) = server
        .post_auth(
            "/api/v5/sso/mfa/setup",
            json!({"username": "admin"}),
            &limited_token,
        )
        .await;
    assert_eq!(status, 403, "limited admin targets another MFA: {body}");

    // mfa_management allows another user's MFA, not another password.
    let (status, body) = server
        .post_auth(
            "/api/v5/sso/mfa/setup",
            json!({"username": "admin"}),
            &mfa_admin_token,
        )
        .await;
    assert_eq!(status, 200, "mfa_management targets another MFA: {body}");
    let (status, body) = server
        .post_auth("/api/v5/users/admin/mfa", json!({}), &mfa_admin_token)
        .await;
    assert_eq!(status, 204, "mfa_management enrols another user: {body}");
    let (status, body) = server
        .post_auth("/api/v5/users/admin/mfa", json!({}), &limited_token)
        .await;
    assert_eq!(status, 403, "limited admin enrols another user: {body}");

    // GET /users shows the stored value, and a read-modify-write
    // succeeds with the body that the read returned.
    let (status, users) = server.get_auth("/api/v5/users", &admin_token).await;
    assert_eq!(status, 200);
    let users = users.as_array().expect("users is an array");
    let monitor = users
        .iter()
        .find(|row| row["username"] == json!("scope-monitor"))
        .expect("monitor row");
    assert_eq!(monitor["scopes"], json!(["monitoring"]));
    let unset = users
        .iter()
        .find(|row| row["username"] == json!("scope-unset"))
        .expect("unset row");
    assert_eq!(unset["scopes"], json!("unset"));
    let (status, updated) = server
        .put_auth(
            "/api/v5/users/scope-monitor",
            json!({
                "role": monitor["role"],
                "description": monitor["description"],
                "scopes": monitor["scopes"],
            }),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200, "round-trip PUT: {updated}");
    assert_eq!(updated["scopes"], json!(["monitoring"]));
}

/// MV-05: a deny-all user keeps only the public management routes.
#[tokio::test]
async fn dashboard_user_deny_all_keeps_public_paths() {
    let (server, _state) = TestServer::start().await;
    let admin_token = server.login_as_admin().await;
    let (status, _) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "scope-deny-all", "password": "Deny-pass-1a",
                       "role": "viewer", "scopes": []}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "scope-deny-all", "password": "Deny-pass-1a"}),
        )
        .await;
    assert_eq!(status, 200, "login: {body}");
    let token = body["token"].as_str().expect("token").to_string();

    for path in [
        "/api/v5/metrics",
        "/api/v5/clients",
        "/api/v5/users",
        "/api/v5/api_key",
    ] {
        let (status, body) = server.get_auth(path, &token).await;
        assert_eq!(status, 403, "deny-all GET {path}: {body}");
    }
    let (status, _) = server.post_auth("/api/v5/users", json!({}), &token).await;
    assert_eq!(status, 403, "deny-all write");

    for path in [
        "/api/v5/current_user",
        "/api/v5/user_scopes",
        "/api/v5/api_key_scopes",
        "/api/v5/status?format=json",
        "/schemas/hotconf",
    ] {
        let (status, body) = server.get_auth(path, &token).await;
        assert_eq!(status, 200, "public GET {path}: {body}");
    }

    // The own password change and MFA self-service stay open.
    let (status, body) = server
        .put_auth(
            "/api/v5/users/scope-deny-all/change_pwd",
            json!({"old_pwd": "Deny-pass-1a", "new_pwd": "Deny-pass-2b"}),
            &token,
        )
        .await;
    assert_eq!(status, 204, "own password change: {body}");
    let (status, body) = server
        .post_auth(
            "/api/v5/sso/mfa/setup",
            json!({"username": "scope-deny-all"}),
            &token,
        )
        .await;
    assert_eq!(status, 200, "own MFA setup: {body}");
    assert!(body["secret"].is_string());

    let (status, _) = server.post_auth("/api/v5/logout", json!({}), &token).await;
    assert_eq!(status, 204, "logout");
}

/// MV-05: a scope change or a role change applies to a token that
/// was issued before the change.
#[tokio::test]
async fn dashboard_user_scope_and_role_changes_reach_existing_tokens() {
    let (server, _state) = TestServer::start().await;
    let admin_token = server.login_as_admin().await;
    let (status, _) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "scope-live", "password": "Live-pass-1a",
                       "role": "viewer", "scopes": ["monitoring"]}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "scope-live", "password": "Live-pass-1a"}),
        )
        .await;
    assert_eq!(status, 200);
    let token = body["token"].as_str().expect("token").to_string();
    let (status, _) = server.get_auth("/api/v5/metrics", &token).await;
    assert_eq!(status, 200);
    let (status, _) = server.get_auth("/api/v5/listeners", &token).await;
    assert_eq!(status, 403);

    // The same token sees the new scope on its next request.
    let (status, body) = server
        .put_auth(
            "/api/v5/users/scope-live",
            json!({"scopes": ["system"]}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200, "scope change: {body}");
    let (status, _) = server.get_auth("/api/v5/metrics", &token).await;
    assert_eq!(status, 403);
    let (status, body) = server.get_auth("/api/v5/listeners", &token).await;
    assert_eq!(status, 200, "new scope on old token: {body}");

    // Role demotion also applies to the existing token.
    let (status, _) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "scope-role", "password": "Role-pass-1a",
                       "role": "administrator", "scopes": "unset"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "scope-role", "password": "Role-pass-1a"}),
        )
        .await;
    assert_eq!(status, 200);
    let role_token = body["token"].as_str().expect("role token").to_string();
    let (status, _) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "scope-role-probe", "password": "Probe-pass-1a"}),
            &role_token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, body) = server
        .put_auth(
            "/api/v5/users/scope-role",
            json!({"role": "viewer", "scopes": "unset"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200, "demotion: {body}");
    let (status, body) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "scope-role-probe2", "password": "Probe-pass-1a"}),
            &role_token,
        )
        .await;
    assert_eq!(status, 403, "old admin token after demotion: {body}");
}

/// MV-05: scopes persist across a restart and still limit the token.
#[tokio::test]
async fn dashboard_user_scopes_survive_restart() {
    let dir = unique_data_dir("user-scopes");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let admin_token = server.login_as_admin().await;
    let (status, _) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "scope-restart", "password": "Restart-pass-1a",
                       "role": "viewer", "scopes": ["monitoring"]}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200);
    drop(server);
    drop(registry);

    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "scope-restart", "password": "Restart-pass-1a"}),
        )
        .await;
    assert_eq!(status, 200, "login after restart: {body}");
    let token = body["token"].as_str().expect("token").to_string();
    let (status, _) = server.get_auth("/api/v5/metrics", &token).await;
    assert_eq!(status, 200);
    let (status, _) = server.get_auth("/api/v5/clients", &token).await;
    assert_eq!(status, 403);
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    let admin_token = body["token"].as_str().expect("admin token").to_string();
    let (status, users) = server.get_auth("/api/v5/users", &admin_token).await;
    assert_eq!(status, 200);
    let row = users
        .as_array()
        .expect("users is an array")
        .iter()
        .find(|row| row["username"] == json!("scope-restart"))
        .expect("stored user");
    assert_eq!(row["scopes"], json!(["monitoring"]));
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

/// MV-05: the scope check uses a normalized path and does not open the
/// MCP door to a dashboard login token.
#[tokio::test]
async fn dashboard_user_scope_paths_are_normalized() {
    let (server, _state) = TestServer::start().await;
    let admin_token = server.login_as_admin().await;
    let (status, _) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "scope-url", "password": "Url-pass-1a",
                       "role": "viewer", "scopes": ["monitoring"]}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "scope-url", "password": "Url-pass-1a"}),
        )
        .await;
    assert_eq!(status, 200);
    let token = body["token"].as_str().expect("token").to_string();

    // An encoded path that reaches the user route must not pass the
    // monitoring scope. A route miss is also a denial.
    for path in [
        "/api/v5/%75sers",
        "/api/v5/metrics/../users",
        "/api/v5/users/",
        "/api/v5/%2e%2e/users",
    ] {
        let (status, body) = server.get_auth(path, &token).await;
        assert!(
            status == 403 || status == 404,
            "{path} must not pass the monitoring scope: {status} {body}"
        );
    }
    // The monitoring scope still opens its own encoded path when the
    // router resolves it.
    let (status, _) = server.get_auth("/api/v5/stat%73", &token).await;
    assert!(status == 200 || status == 404, "encoded stats: {status}");

    // MCP accepts API keys only, so a login token cannot bypass scopes
    // there.
    let (status, body) = server
        .post_auth(
            "/mcp",
            json!({"jsonrpc": "2.0", "id": 1, "method": "ping"}),
            &token,
        )
        .await;
    assert_eq!(status, 401, "MCP with a login token: {body}");

    // A limited key reaches /mcp with monitoring. A management route
    // outside that scope is refused through the tool call.
    let (_key_id, secret) = make_key(
        &server,
        &admin_token,
        json!({"name": "scope-mcp-key", "scopes": ["monitoring"]}),
    )
    .await;
    let bearer = format!("Bearer {secret}");
    let (status, body) = mcp_rpc(
        &server,
        &bearer,
        json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "get_api_v5_users", "arguments": {}}, "id": 2}),
    )
    .await;
    assert_eq!(status, 200, "MCP users call answers: {body}");
    assert_eq!(
        body["result"]["isError"],
        json!(true),
        "limited key cannot call users: {body}"
    );
    let text = body["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default();
    assert!(
        text.contains("FORBIDDEN") || text.contains("403"),
        "users refusal names the code: {text}"
    );
    let (status, body) = mcp_rpc(
        &server,
        &bearer,
        json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "get_api_v5_metrics", "arguments": {}}, "id": 3}),
    )
    .await;
    assert_eq!(status, 200, "MCP metrics call answers: {body}");
    assert!(
        body["result"].get("isError").is_none(),
        "monitoring key calls metrics: {body}"
    );
}

/// MV-05: a default administrator keeps the bearer management routes.
#[tokio::test]
async fn default_administrator_keeps_bearer_management_routes() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    for path in ["/api/v5/users", "/api/v5/user_scopes"] {
        let (status, body) = server.get_auth(path, &token).await;
        assert_eq!(status, 200, "default admin GET {path}: {body}");
    }
}

/// MV-05: a limited administrator keeps tenant checks and can write a
/// tenant limit. The MQTT refusal is proved by
/// `mv05_tenant_session_limit_refuses_second_mqtt_session` in
/// `tests/e2e_integration.rs`.
#[tokio::test]
async fn dashboard_user_scopes_keep_tenant_checks() {
    let (server, _state) = TestServer::start().await;
    let admin_token = server.login_as_admin().await;
    let (status, _) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "scope-tenant", "password": "Tenant-pass-1a",
                       "role": "administrator", "scopes": ["system"]}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "scope-tenant", "password": "Tenant-pass-1a"}),
        )
        .await;
    assert_eq!(status, 200);
    let token = body["token"].as_str().expect("token").to_string();

    // The limited administrator cannot manage users.
    let (status, _) = server.get_auth("/api/v5/users", &token).await;
    assert_eq!(status, 403);

    let (status, body) = server
        .post_auth("/api/v5/mt/ns/scope-tenant", json!({}), &token)
        .await;
    assert_eq!(status, 204, "create tenant: {body}");
    let (status, body) = server
        .put_auth(
            "/api/v5/mt/ns/scope-tenant/config",
            json!({"limits": {"max_sessions": 1}}),
            &token,
        )
        .await;
    assert_eq!(status, 200, "set tenant limit: {body}");
    assert_eq!(body["limits"]["max_sessions"], json!(1));
    let (status, body) = server
        .get_auth("/api/v5/mt/ns/scope-tenant/config", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["limits"]["max_sessions"], json!(1));
    // A missing tenant still fails, whatever the scope.
    let (status, body) = server
        .get_auth("/api/v5/mt/ns/scope-tenant-missing/config", &token)
        .await;
    assert_eq!(status, 404, "missing tenant: {body}");
}

#[tokio::test]
async fn admin_users_survive_restart() {
    let dir = unique_data_dir("survive");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let admin_token = server.login_as_admin().await;

    // Create `w0keep` through the API, then change its password.
    let (status, _) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "w0keep", "password": "W0keep-pass1", "role": "viewer"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = server
        .put_auth(
            "/api/v5/users/w0keep/change_pwd",
            json!({"new_pwd": "W0keep-new-pass2"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 204);
    drop(server);
    drop(registry);

    // Simulate a kernel restart: rebuild state from the same dir.
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;

    // The new password logs in; the old one is rejected.
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "w0keep", "password": "W0keep-new-pass2"}),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["role"], json!("viewer"));
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "w0keep", "password": "W0keep-pass1"}),
        )
        .await;
    assert_eq!(status, 401);
    assert_eq!(body["code"], json!("BAD_USERNAME_OR_PWD"));
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn admin_users_delete_survives_restart() {
    let dir = unique_data_dir("delete");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(registry).await;
    let admin_token = server.login_as_admin().await;

    let (status, _) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "w0keep", "password": "W0keep-pass1", "role": "viewer"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200);
    // Sanity: the user logs in before the delete.
    let (status, _) = server
        .post(
            "/api/v5/login",
            json!({"username": "w0keep", "password": "W0keep-pass1"}),
        )
        .await;
    assert_eq!(status, 200);
    let (status, _) = server
        .delete_auth("/api/v5/users/w0keep", &admin_token)
        .await;
    assert_eq!(status, 204);
    drop(server);

    // Simulate a kernel restart: rebuild state from the same dir.
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;

    // The deleted user can no longer log in ...
    let (status, _) = server
        .post(
            "/api/v5/login",
            json!({"username": "w0keep", "password": "W0keep-pass1"}),
        )
        .await;
    assert_eq!(status, 401);
    // ... and the reloaded list has no `w0keep`. The admin password
    // persisted too, so the rebooted node logs in with it.
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    let admin_token = body["token"].as_str().expect("admin token").to_string();
    let (status, body) = server.get_auth("/api/v5/users", &admin_token).await;
    assert_eq!(status, 200);
    let names: Vec<&str> = body
        .as_array()
        .expect("users data array")
        .iter()
        .map(|u| u["username"].as_str().expect("username"))
        .collect();
    assert!(
        !names.contains(&"w0keep"),
        "deleted user must stay deleted, got: {names:?}"
    );
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn dashboard_user_mfa_enrol_disable_round_trip() {
    // W2-44 through-the-router test: enrol MFA for a dashboard user
    // over HTTP, reject the duplicate, delete, then miss the second
    // delete. The enrolment lands on the same admin-user MFA state the
    // setup/verify routes consult, and the MQTT credential store stays
    // untouched throughout (management-plane only).
    let (server, state) = TestServer::start().await;
    let admin_token = server.login_as_admin().await;

    let (status, _) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "w244-http", "password": "W244-http-pass1", "role": "viewer"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200);

    // Enrol answers 204 with no body, as the reference API documents.
    let (status, enrolled) = server
        .post_auth(
            "/api/v5/users/w244-http/mfa",
            json!({"mechanism": "totp"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 204);
    assert!(enrolled.is_null(), "enrol returns no body, got: {enrolled}");
    assert_eq!(state.auth.user_count(), 0);

    // The secret and the provisioning URI come from the setup route.
    let (status, provisioning) = server
        .post_auth(
            "/api/v5/sso/mfa/setup",
            json!({"username": "w244-http"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(provisioning["username"], json!("w244-http"));
    assert!(
        provisioning["secret"]
            .as_str()
            .is_some_and(|s| !s.is_empty()),
        "setup returns the pending secret, got: {provisioning}"
    );
    assert!(provisioning["provisioning_uri"].is_string());
    let first_secret = provisioning["secret"]
        .as_str()
        .expect("setup returns a secret")
        .to_string();

    // Setup_info follows the single-sign-on shape. A local body
    // fails with 400. A full body fails with 401.
    let (status, err) = server
        .post_auth(
            "/api/v5/sso/mfa/setup_info",
            json!({"username": "w244-http"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(err["code"], json!("BAD_REQUEST"));
    let (status, err) = server
        .post_auth(
            "/api/v5/sso/mfa/setup_info",
            json!({
                "backend": "oidc",
                "setup_token": "w244-token",
                "username": "w244-http",
            }),
            &admin_token,
        )
        .await;
    assert_eq!(status, 401);
    assert_eq!(err["code"], json!("UNAUTHORIZED"));

    // A second enrol resets with a fresh secret.
    let (status, _) = server
        .post_auth(
            "/api/v5/users/w244-http/mfa",
            json!({"mechanism": "totp"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 204);
    let (status, second) = server
        .post_auth(
            "/api/v5/sso/mfa/setup",
            json!({"username": "w244-http"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 200);
    let rotated = second["secret"]
        .as_str()
        .expect("reset returns a secret")
        .to_string();
    assert_ne!(first_secret, rotated);

    // Delete disables; a second delete changes nothing and answers 204.
    let (status, _) = server
        .delete_auth("/api/v5/users/w244-http/mfa", &admin_token)
        .await;
    assert_eq!(status, 204);
    let (status, _) = server
        .delete_auth("/api/v5/users/w244-http/mfa", &admin_token)
        .await;
    assert_eq!(status, 204);

    // Unknown users are 404 on both routes.
    let (status, err) = server
        .post_auth(
            "/api/v5/users/w244-ghost/mfa",
            json!({"mechanism": "totp"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(err["code"], json!("USER_NOT_FOUND"));
    let (status, err) = server
        .delete_auth("/api/v5/users/w244-ghost/mfa", &admin_token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(err["code"], json!("USER_NOT_FOUND"));
    assert_eq!(state.auth.user_count(), 0);

    let (status, _) = server
        .delete_auth("/api/v5/users/w244-http", &admin_token)
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn mqtt_users_and_quotas_survive_restart() {
    let dir = unique_data_dir("mqtt-survive");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(registry).await;
    let admin_token = server.login_as_admin().await;

    // Create MQTT users through the API, one with quotas.
    let (status, _) = server
        .post_auth(
            "/api/v1/auth/users",
            json!({"username": "capped", "password": "pw-capped",
                       "quotas": {"max_connections": 2,
                                  "max_publish_rate": 50,
                                  "max_publish_burst": 10}}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v1/auth/users",
            json!({"username": "plain", "password": "pw-plain"}),
            &admin_token,
        )
        .await;
    assert_eq!(status, 201);
    drop(server);

    // Simulate a kernel restart: rebuild state from the same dir.
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;

    // The admin password persisted too, so the rebooted node logs in
    // with it; both MQTT users and the configured quotas are intact.
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    let admin_token = body["token"].as_str().expect("admin token").to_string();
    let (status, body) = server.get_auth("/api/v1/auth/users", &admin_token).await;
    assert_eq!(status, 200);
    let users = body.as_array().expect("users list");
    assert_eq!(users.len(), 2);
    assert_eq!(users[0]["username"], json!("capped"));
    assert_eq!(users[0]["quotas"]["max_connections"], json!(2));
    assert_eq!(users[0]["quotas"]["max_publish_rate"], json!(50));
    assert_eq!(users[0]["quotas"]["max_publish_burst"], json!(10));
    assert_eq!(users[1]["username"], json!("plain"));
    assert_eq!(
        users[1]["quotas"]["max_publish_rate"],
        serde_json::Value::Null
    );
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn user_scopes_catalogue_is_stable_and_unpaged() {
    // W2-40 failing test: fetch the dashboard-user scope catalogue
    // expecting the documented names with per-scope fields, matching
    // what the dashboard-user validation accepts, stable across two
    // reads. Before: a constant string list detached from validation
    // (wrong vocabulary, no fields, no shared registry).
    // Broker-through: both reads go through the served management
    // API on `ApiState` (the same state the kernel shares in
    // `crates/broker-node/src/main.rs`, `serve_api`), not only
    // through the store; the catalogue itself is code-defined
    // (`crates/broker-api/src/v5/auth.rs`, `USER_SCOPES`), shared
    // with the dashboard-user validation predicates
    // (`is_known_user_scope`, `role_may_hold_scope`), with no broker
    // event driving it beyond serving the management read.
    // Management-plane only; publish and deliver never touch it.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, first) = server.get_auth("/api/v5/user_scopes", &token).await;
    assert_eq!(status, 200);
    let scopes = first["scopes"].as_array().expect("scopes is a list");
    assert_eq!(scopes.len(), 14);
    let names: Vec<&str> = scopes
        .iter()
        .map(|entry| entry["name"].as_str().expect("entry has a name"))
        .collect();
    for entry in scopes {
        assert!(entry["name"].is_string(), "entry carries a name");
        assert!(entry["desc"].is_string(), "entry carries a summary");
        assert!(
            entry["admin_only"].is_boolean(),
            "entry carries its administrator-only flag"
        );
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
            names.contains(&documented),
            "catalogue must contain {documented}"
        );
    }
    // Three login-only scopes are administrator-only. mfa_management
    // is not: a non-administrator may hold it for self-service.
    for entry in scopes {
        let name = entry["name"].as_str().expect("entry has a name");
        let admin_only = entry["admin_only"].as_bool().expect("flag is a bool");
        let expect_admin_only = matches!(
            name,
            "user_management" | "sso_management" | "api_key_management"
        );
        assert_eq!(
            admin_only, expect_admin_only,
            "scope {name} carries the documented administrator-only flag"
        );
    }

    // Stable across two reads.
    let (status, second) = server.get_auth("/api/v5/user_scopes", &token).await;
    assert_eq!(status, 200);
    assert_eq!(first, second);

    // Not paged and takes no parameters: unknown query parameters
    // are ignored, not errors.
    let (status, tolerated) = server
        .send(
            "GET",
            "/api/v5/user_scopes?unexpected_param=1&page=2&limit=1",
            None,
            Some(&token),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(tolerated, first);
}

#[tokio::test]
async fn dashboard_user_create_returns_documented_status() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, created) = server
        .post_auth(
            "/api/v5/users",
            json!({"username": "tk02-user", "password": "Tk02-passw0rd!"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(created["username"], json!("tk02-user"));
}
