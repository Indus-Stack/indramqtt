use crate::reauth_harness::*;

#[tokio::test]
async fn scram_reauth_round_trip_and_bad_proof() {
    let kernel = boot_kernel("scram", "").await;
    let mut sock = connect_scram_ok(
        kernel.mqtt_port,
        "client-reauth-scram",
        SCRAM_PASSWORD,
        None,
    )
    .await;
    let client_nonce = "rOprNGfwEbeRWgbNEkqO";
    let client_first = format!("n={SCRAM_USER},r={client_nonce}");
    send(
        &mut sock,
        &auth_packet(0x19, Some("SCRAM-SHA-256"), Some(client_first.as_bytes())),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0xF0, "kernel sends AUTH continue");
    let (reason, method, data) = parse_auth(&body);
    assert_eq!(reason, 0x18, "server-first uses continue");
    assert_eq!(method.as_deref(), Some("SCRAM-SHA-256"), "method echoes");
    let server_first = String::from_utf8(data.expect("server-first data")).expect("UTF-8");

    let client_final = scram_client_final(&client_first, &server_first, SCRAM_PASSWORD);
    send(
        &mut sock,
        &auth_packet(0x18, Some("SCRAM-SHA-256"), Some(client_final.as_bytes())),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0xF0, "kernel sends AUTH success");
    let (reason, method, data) = parse_auth(&body);
    assert_eq!(reason, 0x00, "valid proof succeeds");
    assert_eq!(method.as_deref(), Some("SCRAM-SHA-256"), "method echoes");
    let server_final = String::from_utf8(data.expect("server-final data")).expect("UTF-8");
    assert!(server_final.starts_with("v="), "server signature returns");

    let mut bad = connect_scram_ok(
        kernel.mqtt_port,
        "client-reauth-scram-bad",
        SCRAM_PASSWORD,
        None,
    )
    .await;
    send(
        &mut bad,
        &auth_packet(0x19, Some("SCRAM-SHA-256"), Some(client_first.as_bytes())),
    )
    .await;
    let (_head, body) = recv(&mut bad).await;
    let server_first =
        String::from_utf8(parse_auth(&body).2.expect("server-first data")).expect("UTF-8");
    let client_final = scram_client_final(&client_first, &server_first, "wrong-password");
    send(
        &mut bad,
        &auth_packet(0x18, Some("SCRAM-SHA-256"), Some(client_final.as_bytes())),
    )
    .await;
    expect_disconnect(&mut bad, 0x87).await;

    kernel.finish().await;
}

#[tokio::test]
async fn initial_enhanced_auth_rejects_reauth_code_and_bad_proof() {
    let kernel = boot_kernel("initial-enhanced", "").await;
    let client_nonce = "rOprNGfwEbeRWgbNEkqO";
    let client_first = format!("n={SCRAM_USER},r={client_nonce}");

    let mut sock1 = dial(kernel.mqtt_port).await;
    send(
        &mut sock1,
        &connect5_full(
            "client-initial-82",
            true,
            Some("SCRAM-SHA-256"),
            Some(client_first.as_bytes()),
            None,
            None,
        ),
    )
    .await;
    let (head, _body) = recv(&mut sock1).await;
    assert_eq!(head & 0xF0, 0xF0, "initial SCRAM sends AUTH continue");
    send(
        &mut sock1,
        &auth_packet(0x19, Some("SCRAM-SHA-256"), Some(client_first.as_bytes())),
    )
    .await;
    let (c_head, c_body) = recv(&mut sock1).await;
    assert_eq!(c_head & 0xF0, 0x20, "broker answers with CONNACK");
    assert_eq!(
        c_body.get(1).copied().unwrap_or(0),
        0x82,
        "CONNACK Reason Code 0x82"
    );

    let mut sock2 = dial(kernel.mqtt_port).await;
    send(
        &mut sock2,
        &connect5_full(
            "client-initial-87",
            true,
            Some("SCRAM-SHA-256"),
            Some(client_first.as_bytes()),
            None,
            None,
        ),
    )
    .await;
    let (_head, body) = recv(&mut sock2).await;
    let server_first =
        String::from_utf8(parse_auth(&body).2.expect("server-first data")).expect("UTF-8");
    let bad_final = scram_client_final(&client_first, &server_first, "wrong-password");
    send(
        &mut sock2,
        &auth_packet(0x18, Some("SCRAM-SHA-256"), Some(bad_final.as_bytes())),
    )
    .await;
    let (c_head, c_body) = recv(&mut sock2).await;
    assert_eq!(c_head & 0xF0, 0x20, "broker answers with CONNACK");
    assert_eq!(
        c_body.get(1).copied().unwrap_or(0),
        0x87,
        "CONNACK Reason Code 0x87"
    );

    kernel.finish().await;
}

#[tokio::test]
async fn scram_reauth_respects_tenant_isolation() {
    let extra = "\n[[tenants.rules]]\nlistener = \"\"\nattribute = \"tenant\"\nexpression = \"${clientid}\"\n";
    let kernel = boot_kernel("tenant-scram", extra).await;
    let base = format!("http://127.0.0.1:{}", kernel.api_port);
    let http = reqwest::Client::new();

    let def_res = http
        .post(format!("{base}/api/v1/auth/acls"))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({
            "client_pattern": "tenant-alpha",
            "action": "publish",
            "topic_pattern": "default/data",
            "allow": true
        }))
        .send()
        .await
        .expect("default acl post");
    assert!(def_res.status().is_success(), "default acl created");

    let alpha_res = http
        .post(format!("{base}/api/v1/auth/acls?tenant=tenant-alpha"))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({
            "client_pattern": "tenant-alpha",
            "action": "publish",
            "topic_pattern": "alpha/data",
            "allow": true
        }))
        .send()
        .await
        .expect("alpha acl post");
    assert!(alpha_res.status().is_success(), "alpha acl created");

    let mut sock = connect_scram_ok(kernel.mqtt_port, "tenant-alpha", SCRAM_PASSWORD, None).await;

    let pre_acls = http
        .get(format!("{base}/api/v1/auth/acls?tenant=tenant-alpha"))
        .bearer_auth(API_KEY)
        .send()
        .await
        .expect("pre-reauth alpha acl get");
    let pre_text = pre_acls.text().await.expect("pre body");
    assert!(
        pre_text.contains("alpha/data"),
        "tenant-alpha rules preserved after connect"
    );

    let client_nonce = "rOprNGfwEbeRWgbNEkqO";
    let client_first = format!("n={SCRAM_USER},r={client_nonce}");
    send(
        &mut sock,
        &auth_packet(0x19, Some("SCRAM-SHA-256"), Some(client_first.as_bytes())),
    )
    .await;
    let (_head, body) = recv(&mut sock).await;
    let server_first =
        String::from_utf8(parse_auth(&body).2.expect("server-first data")).expect("UTF-8");
    let client_final = scram_client_final(&client_first, &server_first, SCRAM_PASSWORD);
    send(
        &mut sock,
        &auth_packet(0x18, Some("SCRAM-SHA-256"), Some(client_final.as_bytes())),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0xF0, "kernel sends AUTH success");
    assert_eq!(parse_auth(&body).0, 0x00, "re-authentication succeeds");

    let alpha_acls = http
        .get(format!("{base}/api/v1/auth/acls?tenant=tenant-alpha"))
        .bearer_auth(API_KEY)
        .send()
        .await
        .expect("alpha acl get");
    let alpha_text = alpha_acls.text().await.expect("alpha body");
    assert!(
        alpha_text.contains("alpha/data"),
        "tenant-alpha admin rules survive re-auth"
    );

    let def_acls = http
        .get(format!("{base}/api/v1/auth/acls"))
        .bearer_auth(API_KEY)
        .send()
        .await
        .expect("def acl get");
    let def_text = def_acls.text().await.expect("def body");
    assert!(
        def_text.contains("default/data"),
        "default tenant rules preserved"
    );

    kernel.finish().await;
}

#[tokio::test]
async fn scram_initial_and_reauth_reject_mismatched_principals() {
    let kernel = boot_kernel("scram-mismatch", "").await;
    let base = format!("http://127.0.0.1:{}", kernel.api_port);
    let http = reqwest::Client::new();

    // Create a second valid user for reauth principal-switch testing.
    let res = http
        .post(format!("{base}/api/v1/auth/users"))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({
            "username": "other-scram",
            "password": SCRAM_PASSWORD
        }))
        .send()
        .await
        .expect("create other-scram user");
    assert!(res.status().is_success(), "other-scram user created");

    // 1. Mismatched CONNECT username vs SCRAM client-first username must be rejected.
    let mut sock1 = dial(kernel.mqtt_port).await;
    let client_nonce = "rOprNGfwEbeRWgbNEkqO";
    let client_first = format!("n={SCRAM_USER},r={client_nonce}");
    send(
        &mut sock1,
        &connect5_auth_with_username(
            "client-scram-mismatch",
            "SCRAM-SHA-256",
            client_first.as_bytes(),
            "other-scram",
        ),
    )
    .await;
    let (_head, body) = recv(&mut sock1).await;
    let server_first =
        String::from_utf8(parse_auth(&body).2.expect("server-first data")).expect("UTF-8");
    let client_final = scram_client_final(&client_first, &server_first, SCRAM_PASSWORD);
    send(
        &mut sock1,
        &auth_packet(0x18, Some("SCRAM-SHA-256"), Some(client_final.as_bytes())),
    )
    .await;
    let (c_head, c_body) = recv(&mut sock1).await;
    assert_eq!(c_head & 0xF0, 0x20, "broker answers with CONNACK");
    assert_eq!(
        c_body.get(1).copied().unwrap_or(0),
        0x87,
        "mismatched CONNECT vs SCRAM username must be rejected with 0x87"
    );

    // 2. Re-authentication for a different valid principal must be rejected.
    let mut sock2 = connect_scram_ok(
        kernel.mqtt_port,
        "client-scram-reauth-diff",
        SCRAM_PASSWORD,
        None,
    )
    .await;
    let other_first = format!("n=other-scram,r={client_nonce}");
    send(
        &mut sock2,
        &auth_packet(0x19, Some("SCRAM-SHA-256"), Some(other_first.as_bytes())),
    )
    .await;
    let (_head, body) = recv(&mut sock2).await;
    let other_server_first =
        String::from_utf8(parse_auth(&body).2.expect("server-first data")).expect("UTF-8");
    let other_final = scram_client_final(&other_first, &other_server_first, SCRAM_PASSWORD);
    send(
        &mut sock2,
        &auth_packet(0x18, Some("SCRAM-SHA-256"), Some(other_final.as_bytes())),
    )
    .await;
    expect_disconnect(&mut sock2, 0x87).await;

    kernel.finish().await;
}
