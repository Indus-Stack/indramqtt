use super::*;

/// Send one v5 QoS 1 PUBLISH and return the PUBACK reason (0 when the
/// short two-byte form carries no reason).
async fn sr01_publish_reason(sock: &mut tokio::net::TcpStream, packet_id: u16, topic: &str) -> u8 {
    send_mqtt(
        sock,
        &sy02_publish5(topic, packet_id, 1, false, None, b"sr01"),
    )
    .await;
    let (head, body) = recv_mqtt(sock).await;
    assert_eq!(head & 0xF0, 0x40, "kernel answers with PUBACK");
    assert_eq!(
        &body[..2],
        &packet_id.to_be_bytes(),
        "PUBACK mirrors its packet id"
    );
    body.get(2).copied().unwrap_or(0)
}

/// Install one catch-all deny for every action on `sr01/#` through the
/// HTTP API.
async fn sr01_deny_all(base: &str) {
    let response = reqwest::Client::new()
        .post(format!(
            "{base}/api/v5/authorization/sources/built_in_database/rules/all"
        ))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({"rules": [
            {"topic": "sr01/#", "permission": "deny", "action": "all"}
        ]}))
        .send()
        .await
        .expect("ACL write sends");
    assert_eq!(response.status(), 204, "ACL write applies");
}

/// Create one MQTT user through the HTTP API.
async fn sr01_create_user(base: &str, username: &str, password: &str) {
    let response = reqwest::Client::new()
        .post(format!("{base}/api/v1/auth/users"))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({"username": username, "password": password}))
        .send()
        .await
        .expect("user write sends");
    assert_eq!(response.status(), 201, "user {username} is created");
}

/// SR-01 item 1: `auth.superusers` matches the verified username, not
/// the client id the client chooses.
#[tokio::test]
async fn sr01_superuser_uses_verified_username_not_client_id() {
    let kernel = spawn_kernel("[auth]\nsuperusers = [\"admin\", \"root\", \"super\"]\n").await;
    let base = format!("http://127.0.0.1:{}", kernel.api_port);
    sr01_create_user(&base, "alice", "Alice-pass-1a").await;
    sr01_create_user(&base, "super", "Super-pass-1a").await;
    sr01_deny_all(&base).await;

    // An ordinary user with the superuser client ids admin and root is
    // denied on both publish and subscribe.
    for (index, client_id) in ["admin", "root"].into_iter().enumerate() {
        let mut sock = sr01_connect5(
            kernel.mqtt_port,
            client_id,
            Some(("alice", b"Alice-pass-1a")),
        )
        .await;
        let sub = sr01_subscribe_code(&mut sock, 1, "sr01/sub/#").await;
        assert_eq!(sub, 0x87, "client id {client_id} must not bypass subscribe");
        let reason = sr01_publish_reason(&mut sock, 1 + index as u16, "sr01/pub/data").await;
        assert_eq!(
            reason, 0x87,
            "client id {client_id} must not bypass publish"
        );
    }

    // A configured superuser username bypasses with any client id.
    let mut sock = sr01_connect5(
        kernel.mqtt_port,
        "not-admin-root-or-super",
        Some(("super", b"Super-pass-1a")),
    )
    .await;
    let sub = sr01_subscribe_code(&mut sock, 2, "sr01/sub/#").await;
    assert_eq!(sub, 0, "the superuser username bypasses subscribe");
    let reason = sr01_publish_reason(&mut sock, 3, "sr01/pub/data").await;
    assert_eq!(reason, 0, "the superuser username bypasses publish");

    kernel.finish().await;
}

/// SR-01 item 1: an anonymous session never becomes a superuser because
/// its client id is admin.
#[tokio::test]
async fn sr01_anonymous_client_id_admin_gets_no_superuser_bypass() {
    let kernel = spawn_kernel("").await;
    let base = format!("http://127.0.0.1:{}", kernel.api_port);
    sr01_deny_all(&base).await;

    let mut sock = sr01_connect5(kernel.mqtt_port, "admin", None).await;
    let sub = sr01_subscribe_code(&mut sock, 1, "sr01/sub/#").await;
    assert_eq!(
        sub, 0x87,
        "anonymous client id admin must not bypass subscribe"
    );
    let reason = sr01_publish_reason(&mut sock, 1, "sr01/pub/data").await;
    assert_eq!(
        reason, 0x87,
        "anonymous client id admin must not bypass publish"
    );

    kernel.finish().await;
}

/// SR-01 item 1: open mode verifies no username, so a credentialed
/// client that claims `admin` is not a superuser.
#[tokio::test]
async fn sr01_open_mode_claimed_admin_gets_no_superuser_bypass() {
    let kernel = spawn_kernel("").await;
    let base = format!("http://127.0.0.1:{}", kernel.api_port);
    sr01_deny_all(&base).await;

    let mut sock = sr01_connect5(
        kernel.mqtt_port,
        "sr01-open-claimed-admin",
        Some(("admin", b"")),
    )
    .await;
    let sub = sr01_subscribe_code(&mut sock, 1, "sr01/sub/#").await;
    assert_eq!(
        sub, 0x87,
        "open-mode username admin must not bypass subscribe"
    );
    let reason = sr01_publish_reason(&mut sock, 1, "sr01/pub/data").await;
    assert_eq!(
        reason, 0x87,
        "open-mode username admin must not bypass publish"
    );

    kernel.finish().await;
}

/// SR-01 item 1 (auto-subscribe): the ACL check uses the verified JWT
/// subject, never the claimed MQTT username. A JWT client with subject
/// `device-1` claims username `admin` and must not auto-subscribe
/// through the superuser list.
#[tokio::test]
async fn sr01_auto_subscribe_uses_verified_identity() {
    use jwks_test_support::{TestJwksServer, TestSigningKey};

    let signing = TestSigningKey::generate("sr01-key");
    let jwks = TestJwksServer::start(vec![signing.jwk_json()]).await;
    let cert_path = std::env::temp_dir().join(format!(
        "sr01-jwks-{}-{}.pem",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock reads")
            .as_nanos()
    ));
    std::fs::write(&cert_path, jwks_test_support::TEST_CERT_PEM).expect("cert writes");
    let config = format!(
        "[auth]\n\
         superusers = [\"admin\", \"root\", \"pubsuper\"]\n\
         \n\
         [jwks]\n\
         url = \"{}\"\n\
         issuer = \"https://issuer.example\"\n\
         audience = \"indra-mqtt\"\n\
         ca_cert = \"{}\"\n",
        jwks.url(),
        cert_path.display()
    );
    let kernel = spawn_kernel(&config).await;
    let base = format!("http://127.0.0.1:{}", kernel.api_port);
    let http = reqwest::Client::new();

    sr01_create_user(&base, "pubsuper", "Pub-super-1a").await;
    sr01_deny_all(&base).await;
    let response = http
        .put(format!("{base}/api/v5/mqtt/auto_subscribe"))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!([
            {"topic": "sr01/auto/#", "qos": 0, "rh": 0, "rap": 0, "nl": 0}
        ]))
        .send()
        .await
        .expect("auto subscribe write sends");
    assert_eq!(response.status(), 200, "auto subscribe write applies");

    let token = signing.mint("https://issuer.example", "indra-mqtt", 3600, false);
    let mut victim = sr01_connect5(
        kernel.mqtt_port,
        "sr01-jwt-victim",
        Some(("admin", token.as_bytes())),
    )
    .await;
    let mut publisher = sr01_connect5(
        kernel.mqtt_port,
        "sr01-super-publisher",
        Some(("pubsuper", b"Pub-super-1a")),
    )
    .await;
    let mut subscriber = sr01_connect5(
        kernel.mqtt_port,
        "sr01-super-subscriber",
        Some(("pubsuper", b"Pub-super-1a")),
    )
    .await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let listed = tokio::time::timeout(
            STEP,
            http.get(format!(
                "{base}/api/v5/clients/sr01-super-subscriber/subscriptions"
            ))
            .bearer_auth(API_KEY)
            .send(),
        )
        .await
        .expect("subscription list in time")
        .expect("subscription list sends");
        assert_eq!(listed.status(), 200, "subscription list reads");
        let text = listed.text().await.expect("subscription list reads");
        if text.contains("sr01/auto/#") {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "auto-subscription sr01/auto/# registers in time"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let reason = sr01_publish_reason(&mut publisher, 1, "sr01/auto/data").await;
    assert_eq!(reason, 0, "the verified superuser publishes");
    let (head, body) = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        recv_mqtt(&mut subscriber),
    )
    .await
    .expect("permitted super-subscriber receives in time");
    assert_eq!(head & 0xF0, 0x30, "permitted client gets a PUBLISH");
    let got = sy02_parse_publish_v5(head, &body);
    assert_eq!(
        got.topic, "sr01/auto/data",
        "permitted super-subscriber receives the auto-subscribed publish"
    );
    let delivery = tokio::time::timeout(
        std::time::Duration::from_millis(500),
        recv_mqtt(&mut victim),
    )
    .await;
    assert!(
        delivery.is_err(),
        "the JWT client must not auto-subscribe through the claimed username"
    );

    let _ = std::fs::remove_file(&cert_path);
    kernel.finish().await;
}
