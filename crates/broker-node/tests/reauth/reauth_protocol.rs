use std::time::Duration;

use crate::reauth_harness::*;

#[tokio::test]
async fn reauth_without_enhanced_auth_is_protocol_error() {
    let kernel = boot_kernel("plain", "\n[auth]\nallow_anonymous = true\n").await;
    let mut sock = dial(kernel.mqtt_port).await;
    send(
        &mut sock,
        &connect5_full("client-plain", true, None, None, None, None),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x20, "kernel answers with CONNACK");
    assert_eq!(body.get(1).copied().unwrap_or(0xFF), 0, "CONNECT accepted");
    send(&mut sock, &auth_packet(0x19, Some("JWT"), Some(b"token"))).await;
    expect_disconnect(&mut sock, 0x82).await;
    kernel.finish().await;
}

#[tokio::test]
async fn reauth_rejects_success_reason_and_method_change() {
    let kernel = boot_kernel("method", "").await;
    let mut wrong_reason =
        connect_scram_ok(kernel.mqtt_port, "client-reason", SCRAM_PASSWORD, None).await;
    send(
        &mut wrong_reason,
        &auth_packet(0x00, Some("SCRAM-SHA-256"), None),
    )
    .await;
    expect_disconnect(&mut wrong_reason, 0x82).await;

    let mut wrong_method =
        connect_scram_ok(kernel.mqtt_port, "client-method", SCRAM_PASSWORD, None).await;
    send(
        &mut wrong_method,
        &auth_packet(0x19, Some("JWT"), Some(b"token")),
    )
    .await;
    expect_disconnect(&mut wrong_method, 0x82).await;
    kernel.finish().await;
}

#[tokio::test]
async fn reauth_without_authentication_data_is_not_authorized() {
    let kernel = boot_kernel("missing", "").await;
    let mut sock = connect_scram_ok(kernel.mqtt_port, "client-missing", SCRAM_PASSWORD, None).await;
    send(&mut sock, &auth_packet(0x19, Some("SCRAM-SHA-256"), None)).await;
    expect_disconnect(&mut sock, 0x87).await;
    kernel.finish().await;
}

#[tokio::test]
async fn will_published_on_abort_not_on_success() {
    let kernel = boot_kernel("will-suite", "\n[auth]\nallow_anonymous = true\n").await;
    let base = format!("http://127.0.0.1:{}", kernel.api_port);
    let http = reqwest::Client::new();

    let mut watcher =
        connect_scram_ok(kernel.mqtt_port, "will-watcher", SCRAM_PASSWORD, None).await;
    send(&mut watcher, &subscribe_v5(1, "pv14/will/#")).await;
    let (head, _) = recv(&mut watcher).await;
    assert_eq!(head & 0xF0, 0x90, "watcher subscribed");

    let mut succ_sock = connect_scram_ok(
        kernel.mqtt_port,
        "client-will-succ",
        SCRAM_PASSWORD,
        Some(("pv14/will/succ", b"will-succ-payload", 0)),
    )
    .await;
    let client_nonce = "rOprNGfwEbeRWgbNEkqO";
    let client_first = format!("n={SCRAM_USER},r={client_nonce}");
    send(
        &mut succ_sock,
        &auth_packet(0x19, Some("SCRAM-SHA-256"), Some(client_first.as_bytes())),
    )
    .await;
    let (_head, body) = recv(&mut succ_sock).await;
    let server_first =
        String::from_utf8(parse_auth(&body).2.expect("server-first data")).expect("UTF-8");
    let client_final = scram_client_final(&client_first, &server_first, SCRAM_PASSWORD);
    send(
        &mut succ_sock,
        &auth_packet(0x18, Some("SCRAM-SHA-256"), Some(client_final.as_bytes())),
    )
    .await;
    let (head, body) = recv(&mut succ_sock).await;
    assert_eq!(head & 0xF0, 0xF0, "AUTH success");
    assert_eq!(parse_auth(&body).0, 0x00);
    send(&mut succ_sock, &[0xE0, 0x02, 0x00, 0x00]).await;
    drop(succ_sock);
    // Reason: 500 ms wait gives sufficient window for any unintended will publication to arrive.
    const QUIET_CHECK_TIMEOUT: Duration = Duration::from_millis(500);
    let quiet = tokio::time::timeout(QUIET_CHECK_TIMEOUT, recv(&mut watcher)).await;
    assert!(quiet.is_err(), "no will published on successful re-auth");

    let mut close_sock = connect_scram_ok(
        kernel.mqtt_port,
        "client-will-close",
        SCRAM_PASSWORD,
        Some(("pv14/will/close", b"will-close-payload", 0)),
    )
    .await;
    send(
        &mut close_sock,
        &auth_packet(0x19, Some("SCRAM-SHA-256"), Some(client_first.as_bytes())),
    )
    .await;
    let (head, _) = recv(&mut close_sock).await;
    assert_eq!(head & 0xF0, 0xF0, "server continue");
    drop(close_sock);
    let (w_head, w_body) = recv(&mut watcher).await;
    assert_eq!(
        parse_publish_payload(w_head, &w_body),
        b"will-close-payload"
    );

    let mut kick_sock = connect_scram_ok(
        kernel.mqtt_port,
        "client-will-kick",
        SCRAM_PASSWORD,
        Some(("pv14/will/kick", b"will-kick-payload", 0)),
    )
    .await;
    send(
        &mut kick_sock,
        &auth_packet(0x19, Some("SCRAM-SHA-256"), Some(client_first.as_bytes())),
    )
    .await;
    let (head, _) = recv(&mut kick_sock).await;
    assert_eq!(head & 0xF0, 0xF0, "server continue");
    let kick_res = http
        .delete(format!("{base}/api/v5/clients/client-will-kick"))
        .bearer_auth(API_KEY)
        .send()
        .await
        .expect("kick sends");
    assert!(kick_res.status().is_success(), "client kick succeeds");
    let (w_head, w_body) = recv(&mut watcher).await;
    assert_eq!(parse_publish_payload(w_head, &w_body), b"will-kick-payload");

    let mut takeover_sock = connect_scram_ok(
        kernel.mqtt_port,
        "client-will-takeover",
        SCRAM_PASSWORD,
        Some(("pv14/will/takeover", b"will-takeover-payload", 0)),
    )
    .await;
    send(
        &mut takeover_sock,
        &auth_packet(0x19, Some("SCRAM-SHA-256"), Some(client_first.as_bytes())),
    )
    .await;
    let (head, _) = recv(&mut takeover_sock).await;
    assert_eq!(head & 0xF0, 0xF0, "server continue");
    let _new_sock = connect_scram_ok(
        kernel.mqtt_port,
        "client-will-takeover",
        SCRAM_PASSWORD,
        None,
    )
    .await;
    let (w_head, w_body) = recv(&mut watcher).await;
    assert_eq!(
        parse_publish_payload(w_head, &w_body),
        b"will-takeover-payload"
    );

    let mut plain_takeover_sock = connect_scram_ok(
        kernel.mqtt_port,
        "client-will-plain-takeover",
        SCRAM_PASSWORD,
        Some((
            "pv14/will/plain-takeover",
            b"will-plain-takeover-payload",
            0,
        )),
    )
    .await;
    send(
        &mut plain_takeover_sock,
        &auth_packet(0x19, Some("SCRAM-SHA-256"), Some(client_first.as_bytes())),
    )
    .await;
    let (head, _) = recv(&mut plain_takeover_sock).await;
    assert_eq!(head & 0xF0, 0xF0, "server continue");

    let user_res = http
        .post(format!("{base}/api/v1/auth/users"))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({
            "username": "takeover-user",
            "password": "correct-password"
        }))
        .send()
        .await
        .expect("user post");
    assert!(user_res.status().is_success(), "user created");

    let mut bad_plain_sock = dial(kernel.mqtt_port).await;
    send(
        &mut bad_plain_sock,
        &connect5_with_credentials(
            "client-will-plain-takeover",
            true,
            Some("takeover-user"),
            Some(b"wrong-password"),
        ),
    )
    .await;
    let (b_head, b_body) = recv(&mut bad_plain_sock).await;
    assert_eq!(
        b_head & 0xF0,
        0x20,
        "server sends CONNACK for bad credentials"
    );
    assert_eq!(
        b_body.get(1).copied().unwrap_or(0xFF),
        0x86,
        "bad credentials rejected with 0x86"
    );
    let quiet_bad = tokio::time::timeout(QUIET_CHECK_TIMEOUT, recv(&mut watcher)).await;
    assert!(
        quiet_bad.is_err(),
        "no will published when takeover authentication fails"
    );

    let mut plain_sock = dial(kernel.mqtt_port).await;
    send(
        &mut plain_sock,
        &connect5_with_credentials(
            "client-will-plain-takeover",
            true,
            Some("takeover-user"),
            Some(b"correct-password"),
        ),
    )
    .await;
    let (c_head, c_body) = recv(&mut plain_sock).await;
    assert_eq!(
        c_head & 0xF0,
        0x20,
        "server sends CONNACK for plain takeover"
    );
    assert_eq!(
        c_body.get(1).copied().unwrap_or(0xFF),
        0x00,
        "plain takeover accepted"
    );
    let (w_head, w_body) = recv(&mut watcher).await;
    assert_eq!(
        parse_publish_payload(w_head, &w_body),
        b"will-plain-takeover-payload"
    );
    expect_disconnect(&mut plain_takeover_sock, 0x8E).await;

    kernel.finish().await;
}
