use crate::jwks_test_support::{TestJwksServer, TestSigningKey};
use crate::reauth_harness::*;

#[tokio::test]
async fn jwt_reauth_preserves_connection_and_clears_cache() {
    let signing = TestSigningKey::generate("pv14-key");
    let jwks = TestJwksServer::start(vec![signing.jwk_json()]).await;
    let cert_path = std::env::temp_dir().join(format!(
        "pv14-jwks-{}-{}.pem",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock reads")
            .as_nanos()
    ));
    std::fs::write(&cert_path, crate::jwks_test_support::TEST_CERT_PEM).expect("cert writes");
    let extra = format!(
        "\n[jwks]\n\
         url = \"{}\"\n\
         issuer = \"https://issuer.example\"\n\
         audience = \"indra-mqtt\"\n\
         ca_cert = \"{}\"\n",
        jwks.url(),
        cert_path.display()
    );
    let kernel = boot_kernel("jwt", &extra).await;
    let token = signing.mint("https://issuer.example", "indra-mqtt", 3600, false);
    let mut sock = connect_jwt_ok(kernel.mqtt_port, "client-reauth-jwt", &token, None).await;

    send(&mut sock, &subscribe_v5(1, "pv14/cache/#")).await;
    let (head, _body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x90, "subscription is accepted");

    let base = format!("http://127.0.0.1:{}", kernel.api_port);
    let http = reqwest::Client::new();
    let before = http
        .get(format!(
            "{base}/api/v5/clients/client-reauth-jwt/authorization/cache"
        ))
        .bearer_auth(API_KEY)
        .send()
        .await
        .expect("cache read sends");
    assert_eq!(before.status(), 200, "cache route answers before re-auth");
    let before_text = before.text().await.expect("cache body reads");
    assert!(
        before_text.contains("pv14/cache/#"),
        "an authorization decision is cached before re-authentication"
    );

    let renewed = signing.mint("https://issuer.example", "indra-mqtt", 7200, false);
    send(
        &mut sock,
        &auth_packet(0x19, Some("JWT"), Some(renewed.as_bytes())),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0xF0, "kernel answers with AUTH");
    assert_eq!(parse_auth(&body).0, 0x00, "re-authentication succeeds");

    send(&mut sock, &[0xC0, 0x00]).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(
        (head & 0xF0, body.len()),
        (0xD0, 0),
        "connection stays active"
    );

    let after = http
        .get(format!(
            "{base}/api/v5/clients/client-reauth-jwt/authorization/cache"
        ))
        .bearer_auth(API_KEY)
        .send()
        .await
        .expect("cache read sends");
    assert_eq!(after.status(), 200, "cache route answers after re-auth");
    let text = after.text().await.expect("cache body reads");
    assert_eq!(
        text.trim(),
        "[]",
        "successful re-authentication clears the cache"
    );

    let _ = std::fs::remove_file(&cert_path);
    kernel.finish().await;
}

#[tokio::test]
async fn jwt_reauth_with_expired_token_disconnects() {
    let signing = TestSigningKey::generate("pv14-expired-key");
    let jwks = TestJwksServer::start(vec![signing.jwk_json()]).await;
    let cert_path = std::env::temp_dir().join(format!(
        "pv14-jwks-expired-{}-{}.pem",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock reads")
            .as_nanos()
    ));
    std::fs::write(&cert_path, crate::jwks_test_support::TEST_CERT_PEM).expect("cert writes");
    let extra = format!(
        "\n[jwks]\n\
         url = \"{}\"\n\
         issuer = \"https://issuer.example\"\n\
         audience = \"indra-mqtt\"\n\
         ca_cert = \"{}\"\n",
        jwks.url(),
        cert_path.display()
    );
    let kernel = boot_kernel("expired", &extra).await;
    let token = signing.mint("https://issuer.example", "indra-mqtt", 3600, false);
    let mut sock = connect_jwt_ok(kernel.mqtt_port, "client-reauth-expired", &token, None).await;
    let expired = signing.mint("https://issuer.example", "indra-mqtt", -60, false);
    send(
        &mut sock,
        &auth_packet(0x19, Some("JWT"), Some(expired.as_bytes())),
    )
    .await;
    expect_disconnect(&mut sock, 0x87).await;

    let _ = std::fs::remove_file(&cert_path);
    kernel.finish().await;
}

#[tokio::test]
async fn jwt_reauth_replaces_credential_acl_rules() {
    let signing = TestSigningKey::generate("pv14-acl-key");
    let jwks = TestJwksServer::start(vec![signing.jwk_json()]).await;
    let cert_path = std::env::temp_dir().join(format!(
        "pv14-jwks-acl-{}-{}.pem",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock reads")
            .as_nanos()
    ));
    std::fs::write(&cert_path, crate::jwks_test_support::TEST_CERT_PEM).expect("cert writes");
    let extra = format!(
        "\n[jwks]\n\
         url = \"{}\"\n\
         issuer = \"https://issuer.example\"\n\
         audience = \"indra-mqtt\"\n\
         ca_cert = \"{}\"\n",
        jwks.url(),
        cert_path.display()
    );
    let kernel = boot_kernel("jwt-acl", &extra).await;

    let initial_claims = serde_json::json!({
        "client_id": "client-acl-test",
        "acl": [
            {
                "action": "subscribe",
                "topic_pattern": "pv14/initial/#",
                "allow": true
            }
        ]
    });
    let initial_token = mint_custom_jwt(
        &signing,
        "https://issuer.example",
        "indra-mqtt",
        3600,
        initial_claims,
    );
    let mut sock = connect_jwt_ok(kernel.mqtt_port, "client-acl-test", &initial_token, None).await;

    send(&mut sock, &subscribe_v5(1, "pv14/initial/sensor")).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x90, "initial subscription answered");
    assert_eq!(
        body.get(3).copied().unwrap_or(0xFF),
        0x00,
        "pv14/initial/sensor is allowed"
    );

    let renewed_claims = serde_json::json!({
        "client_id": "client-acl-test",
        "acl": [
            {
                "action": "subscribe",
                "topic_pattern": "pv14/renewed/#",
                "allow": true
            }
        ]
    });
    let renewed_token = mint_custom_jwt(
        &signing,
        "https://issuer.example",
        "indra-mqtt",
        7200,
        renewed_claims,
    );
    send(
        &mut sock,
        &auth_packet(0x19, Some("JWT"), Some(renewed_token.as_bytes())),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0xF0, "kernel answers with AUTH");
    assert_eq!(parse_auth(&body).0, 0x00, "re-authentication succeeds");

    send(&mut sock, &subscribe_v5(2, "pv14/renewed/sensor")).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x90, "renewed subscription answered");
    assert_eq!(
        body.get(3).copied().unwrap_or(0xFF),
        0x00,
        "pv14/renewed/sensor is allowed"
    );

    send(&mut sock, &subscribe_v5(3, "pv14/initial/sensor")).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(
        head & 0xF0,
        0x90,
        "initial subscription rejected after re-auth"
    );
    assert_eq!(
        body.get(3).copied().unwrap_or(0xFF),
        0x87,
        "pv14/initial/sensor is rejected after replacement"
    );

    let _ = std::fs::remove_file(&cert_path);
    kernel.finish().await;
}

#[tokio::test]
async fn admin_acls_survive_empty_credential_acl_refused_bind_and_disconnect() {
    let signing = TestSigningKey::generate("pv14-admin-key");
    let jwks = TestJwksServer::start(vec![signing.jwk_json()]).await;
    let cert_path = std::env::temp_dir().join(format!(
        "pv14-jwks-admin-{}-{}.pem",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock reads")
            .as_nanos()
    ));
    std::fs::write(&cert_path, crate::jwks_test_support::TEST_CERT_PEM).expect("cert writes");
    let extra = format!(
        "\n[jwks]\n\
         url = \"{}\"\n\
         issuer = \"https://issuer.example\"\n\
         audience = \"indra-mqtt\"\n\
         ca_cert = \"{}\"\n",
        jwks.url(),
        cert_path.display()
    );
    let kernel = boot_kernel("jwt-admin-survive", &extra).await;
    let base = format!("http://127.0.0.1:{}", kernel.api_port);
    let http = reqwest::Client::new();

    // Seed an administrator deny rule for client-admin-survives.
    let acl_res = http
        .post(format!("{base}/api/v1/auth/acls"))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({
            "client_pattern": "client-admin-survives",
            "action": "subscribe",
            "topic_pattern": "admin/blocked",
            "allow": false
        }))
        .send()
        .await
        .expect("admin acl post");
    assert!(acl_res.status().is_success(), "admin deny rule created");

    // 1. Refused initial JWT bind (mismatched client_id) must not touch admin ACLs.
    let bad_token = mint_custom_jwt(
        &signing,
        "https://issuer.example",
        "indra-mqtt",
        3600,
        serde_json::json!({ "client_id": "other-client", "acl": [] }),
    );
    let mut bad_sock = dial(kernel.mqtt_port).await;
    send(
        &mut bad_sock,
        &connect5_full(
            "client-admin-survives",
            true,
            Some("JWT"),
            Some(bad_token.as_bytes()),
            None,
            None,
        ),
    )
    .await;
    let (c_head, c_body) = recv(&mut bad_sock).await;
    assert_eq!(c_head & 0xF0, 0x20);
    assert_eq!(c_body.get(1).copied().unwrap_or(0), 0x87);

    // 2. Connect with empty credential ACL: admin deny rule must remain enforced.
    let empty_acl_token = mint_custom_jwt(
        &signing,
        "https://issuer.example",
        "indra-mqtt",
        3600,
        serde_json::json!({ "client_id": "client-admin-survives", "acl": [] }),
    );
    let mut sock = connect_jwt_ok(
        kernel.mqtt_port,
        "client-admin-survives",
        &empty_acl_token,
        None,
    )
    .await;
    send(&mut sock, &subscribe_v5(1, "admin/blocked")).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x90);
    assert_eq!(
        body.get(3).copied().unwrap_or(0),
        0x87,
        "admin deny rule survives initial JWT with empty acl"
    );

    // 3. Re-authenticate with empty credential ACL: admin deny rule still enforced.
    send(
        &mut sock,
        &auth_packet(0x19, Some("JWT"), Some(empty_acl_token.as_bytes())),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0xF0);
    assert_eq!(parse_auth(&body).0, 0x00);

    send(&mut sock, &subscribe_v5(2, "admin/blocked")).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x90);
    assert_eq!(
        body.get(3).copied().unwrap_or(0),
        0x87,
        "admin deny rule survives reauth with empty acl"
    );

    // 4. Disconnect and verify admin rule is still present in /api/v5/auth/acls.
    drop(sock);
    let acls_text = http
        .get(format!("{base}/api/v1/auth/acls"))
        .bearer_auth(API_KEY)
        .send()
        .await
        .expect("acls get")
        .text()
        .await
        .expect("acls text");
    assert!(
        acls_text.contains("admin/blocked"),
        "admin deny rule survives disconnect: {acls_text}"
    );

    let _ = std::fs::remove_file(&cert_path);
    kernel.finish().await;
}

#[tokio::test]
async fn jwt_rejects_mismatched_connect_username_and_revokes_subscriptions_on_reauth() {
    let signing = TestSigningKey::generate("pv14-revoke-key");
    let jwks = TestJwksServer::start(vec![signing.jwk_json()]).await;
    let cert_path = std::env::temp_dir().join(format!(
        "pv14-jwks-revoke-{}-{}.pem",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock reads")
            .as_nanos()
    ));
    std::fs::write(&cert_path, crate::jwks_test_support::TEST_CERT_PEM).expect("cert writes");
    let extra = format!(
        "\n[jwks]\n\
         url = \"{}\"\n\
         issuer = \"https://issuer.example\"\n\
         audience = \"indra-mqtt\"\n\
         ca_cert = \"{}\"\n",
        jwks.url(),
        cert_path.display()
    );
    let kernel = boot_kernel("jwt-revoke", &extra).await;

    // 1. Conflicting CONNECT username ("admin") vs JWT sub ("device-1") must fail.
    let token = mint_custom_jwt(
        &signing,
        "https://issuer.example",
        "indra-mqtt",
        3600,
        serde_json::json!({ "client_id": "client-impersonate" }),
    );
    let mut bad = dial(kernel.mqtt_port).await;
    send(
        &mut bad,
        &connect5_auth_with_username("client-impersonate", "JWT", token.as_bytes(), "admin"),
    )
    .await;
    let (c_head, c_body) = recv(&mut bad).await;
    assert_eq!(c_head & 0xF0, 0x20);
    assert_eq!(
        c_body.get(1).copied().unwrap_or(0),
        0x87,
        "conflicting CONNECT username vs JWT subject must be rejected"
    );

    // 2. Reauth revokes subscriptions no longer permitted by the new ACL.
    let old_token = mint_custom_jwt(
        &signing,
        "https://issuer.example",
        "indra-mqtt",
        3600,
        serde_json::json!({
            "client_id": "client-revoke-sub",
            "acl": [
                { "action": "subscribe", "topic_pattern": "review/old/#", "allow": true }
            ]
        }),
    );
    let mut sub_sock =
        connect_jwt_ok(kernel.mqtt_port, "client-revoke-sub", &old_token, None).await;
    send(&mut sub_sock, &subscribe_v5(1, "review/old/topic")).await;
    let (s_head, s_body) = recv(&mut sub_sock).await;
    assert_eq!(s_head & 0xF0, 0x90);
    assert_eq!(s_body.get(3).copied().unwrap_or(0xFF), 0x00);

    let new_token = mint_custom_jwt(
        &signing,
        "https://issuer.example",
        "indra-mqtt",
        7200,
        serde_json::json!({
            "client_id": "client-revoke-sub",
            "acl": [
                { "action": "subscribe", "topic_pattern": "review/new/#", "allow": true }
            ]
        }),
    );
    send(
        &mut sub_sock,
        &auth_packet(0x19, Some("JWT"), Some(new_token.as_bytes())),
    )
    .await;
    let (a_head, a_body) = recv(&mut sub_sock).await;
    assert_eq!(a_head & 0xF0, 0xF0);
    assert_eq!(parse_auth(&a_body).0, 0x00);

    // Publish from a second JWT client to review/old/topic; revoked sub must not receive it.
    let pub_token = mint_custom_jwt(
        &signing,
        "https://issuer.example",
        "indra-mqtt",
        3600,
        serde_json::json!({
            "client_id": "client-revoke-pub",
            "acl": [
                { "action": "publish", "topic_pattern": "review/old/#", "allow": true }
            ]
        }),
    );
    let mut pub_sock =
        connect_jwt_ok(kernel.mqtt_port, "client-revoke-pub", &pub_token, None).await;
    let topic = b"review/old/topic";
    let payload = b"should-not-deliver";
    let mut pub_pkt = Vec::new();
    let rem_len = 2 + topic.len() + 1 + payload.len();
    pub_pkt.push(0x30);
    pub_pkt.push(rem_len as u8);
    pub_pkt.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    pub_pkt.extend_from_slice(topic);
    pub_pkt.push(0x00);
    pub_pkt.extend_from_slice(payload);
    send(&mut pub_sock, &pub_pkt).await;
    send(&mut sub_sock, &[0xC0, 0x00]).await;
    let (p_head, _) = recv(&mut sub_sock).await;
    assert_eq!(
        p_head & 0xF0,
        0xD0,
        "PINGRESP must arrive without any PUBLISH for revoked subscription"
    );

    let _ = std::fs::remove_file(&cert_path);
    kernel.finish().await;
}

#[tokio::test]
async fn jwt_rejects_cross_tenant_subject_boundary() {
    let signing = TestSigningKey::generate("pv14-key");
    let jwks = TestJwksServer::start(vec![signing.jwk_json()]).await;
    let cert_path =
        std::env::temp_dir().join(format!("pv14-jwks-tenant-{}.pem", std::process::id()));
    std::fs::write(&cert_path, crate::jwks_test_support::TEST_CERT_PEM).expect("cert writes");
    let extra = format!(
        "\n[jwks]\n\
         url = \"{}\"\n\
         issuer = \"https://issuer.example\"\n\
         audience = \"indra-mqtt\"\n\
         ca_cert = \"{}\"\n\
         \n[[tenants.rules]]\n\
         listener = \"\"\n\
         attribute = \"tenant\"\n\
         expression = \"${{username}}\"\n",
        jwks.url(),
        cert_path.display()
    );
    let kernel = boot_kernel("jwt-tenant-boundary", &extra).await;

    let token = mint_custom_jwt(
        &signing,
        "https://issuer.example",
        "indra-mqtt",
        3600,
        serde_json::json!({ "sub": "tenant-a" }),
    );
    let mut sock = dial(kernel.mqtt_port).await;
    send(
        &mut sock,
        &connect5_full(
            "default",
            true,
            Some("JWT"),
            Some(token.as_bytes()),
            None,
            None,
        ),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x20, "kernel answers with CONNACK");
    assert_eq!(
        body.get(1).copied().unwrap_or(0),
        0x87,
        "JWT verified in default tenant must not cross into tenant-a"
    );

    let _ = std::fs::remove_file(&cert_path);
    kernel.finish().await;
}

#[tokio::test]
async fn jwt_reconnect_filters_revoked_offline_qos1_replay() {
    let signing = TestSigningKey::generate("pv14-key");
    let jwks = TestJwksServer::start(vec![signing.jwk_json()]).await;
    let cert_path =
        std::env::temp_dir().join(format!("pv14-jwks-offline-{}.pem", std::process::id()));
    std::fs::write(&cert_path, crate::jwks_test_support::TEST_CERT_PEM).expect("cert writes");
    let extra = format!(
        "\n[jwks]\n\
         url = \"{}\"\n\
         issuer = \"https://issuer.example\"\n\
         audience = \"indra-mqtt\"\n\
         ca_cert = \"{}\"\n",
        jwks.url(),
        cert_path.display()
    );
    let kernel = boot_kernel("jwt-offline-replay", &extra).await;

    // 1. Persistent client connects with review/old/# ACL and subscribes QoS 1.
    let old_token = mint_custom_jwt(
        &signing,
        "https://issuer.example",
        "indra-mqtt",
        3600,
        serde_json::json!({
            "client_id": "client-offline-revoke",
            "acl": [
                { "action": "subscribe", "topic_pattern": "review/old/#", "allow": true }
            ]
        }),
    );
    let mut sub_sock = dial(kernel.mqtt_port).await;
    send(
        &mut sub_sock,
        &connect5_full(
            "client-offline-revoke",
            true,
            Some("JWT"),
            Some(old_token.as_bytes()),
            Some(300),
            None,
        ),
    )
    .await;
    let (c_head, c_body) = recv(&mut sub_sock).await;
    assert_eq!(c_head & 0xF0, 0x20);
    assert_eq!(c_body.get(1).copied().unwrap_or(0xFF), 0x00);

    // Subscribe QoS 1 to review/old/#
    let filter = b"review/old/#";
    let mut sub_pkt = Vec::new();
    sub_pkt.push(0x82);
    sub_pkt.push((2 + 1 + 2 + filter.len() + 1) as u8);
    sub_pkt.extend_from_slice(&1u16.to_be_bytes());
    sub_pkt.push(0x00);
    sub_pkt.extend_from_slice(&(filter.len() as u16).to_be_bytes());
    sub_pkt.extend_from_slice(filter);
    sub_pkt.push(0x01); // QoS 1
    send(&mut sub_sock, &sub_pkt).await;
    let (s_head, _) = recv(&mut sub_sock).await;
    assert_eq!(s_head & 0xF0, 0x90, "SUBACK");

    // Cleanly disconnect while keeping session_expiry = 300.
    send(&mut sub_sock, &[0xE0, 0x00]).await;
    drop(sub_sock);

    // 2. Publisher sends a QoS 1 message to review/old/topic while subscriber is offline.
    let pub_token = mint_custom_jwt(
        &signing,
        "https://issuer.example",
        "indra-mqtt",
        3600,
        serde_json::json!({
            "client_id": "client-offline-pub",
            "acl": [
                { "action": "publish", "topic_pattern": "review/old/#", "allow": true }
            ]
        }),
    );
    let mut pub_sock =
        connect_jwt_ok(kernel.mqtt_port, "client-offline-pub", &pub_token, None).await;
    let topic = b"review/old/topic";
    let payload = b"queued-revoked-payload";
    let mut pub_pkt = Vec::new();
    let rem_len = 2 + topic.len() + 2 + 1 + payload.len();
    pub_pkt.push(0x32); // QoS 1
    pub_pkt.push(rem_len as u8);
    pub_pkt.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    pub_pkt.extend_from_slice(topic);
    pub_pkt.extend_from_slice(&7u16.to_be_bytes());
    pub_pkt.push(0x00);
    pub_pkt.extend_from_slice(payload);
    send(&mut pub_sock, &pub_pkt).await;
    let (pa_head, _) = recv(&mut pub_sock).await;
    assert_eq!(pa_head & 0xF0, 0x40, "PUBACK");

    // 3. Persistent client reconnects with a new token allowing only review/new/#.
    let new_token = mint_custom_jwt(
        &signing,
        "https://issuer.example",
        "indra-mqtt",
        3600,
        serde_json::json!({
            "client_id": "client-offline-revoke",
            "acl": [
                { "action": "subscribe", "topic_pattern": "review/new/#", "allow": true }
            ]
        }),
    );
    let mut re_sock = dial(kernel.mqtt_port).await;
    send(
        &mut re_sock,
        &connect5_full(
            "client-offline-revoke",
            false,
            Some("JWT"),
            Some(new_token.as_bytes()),
            Some(300),
            None,
        ),
    )
    .await;
    let (rc_head, rc_body) = recv(&mut re_sock).await;
    assert_eq!(rc_head & 0xF0, 0x20);
    assert_eq!(rc_body.get(1).copied().unwrap_or(0xFF), 0x00);

    // PINGREQ must get PINGRESP with no offline PUBLISH delivered for review/old/topic.
    send(&mut re_sock, &[0xC0, 0x00]).await;
    let (p_head, _) = recv(&mut re_sock).await;
    assert_eq!(
        p_head & 0xF0,
        0xD0,
        "PINGRESP must arrive without replaying revoked offline QoS 1 message"
    );

    let _ = std::fs::remove_file(&cert_path);
    kernel.finish().await;
}
