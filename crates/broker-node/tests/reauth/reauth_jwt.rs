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
