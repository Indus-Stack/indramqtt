use super::support::*;

/// Boots a kernel with custom settings without the command-line anonymous override.
async fn boot_kernel_strict_auth(name: &str, extra_toml: &str) -> Kernel {
    for attempt in 1..=3 {
        let run_tag = format!("{}-{attempt}", tag(name));
        let scratch = std::env::temp_dir().join(format!("indramqtt-{run_tag}-data"));
        let config_dir = std::env::temp_dir().join(format!("indramqtt-{run_tag}-cfg"));
        std::fs::create_dir_all(&config_dir).expect("create temp config dir");
        let (bl_port, mqtt_port, ws_port, api_port) = distinct_ports();
        std::fs::write(
            config_dir.join("indra.toml"),
            format!(
                "[node]\n\
                 id = \"lc-{run_tag}\"\n\
                 data_dir = \"{}\"\n\
                 brokerlink_bind = \"127.0.0.1:{bl_port}\"\n\
                 \n\
                 [listeners.tcp]\n\
                 bind = \"127.0.0.1:{mqtt_port}\"\n\
                 native = true\n\
                 \n\
                 [listeners.ws]\n\
                 bind = \"127.0.0.1:{ws_port}\"\n\
                 native = true\n\
                 \n\
                 [listeners.api]\n\
                 bind = \"127.0.0.1:{api_port}\"\n\
                 \n\
                 [logging]\n\
                 level = \"warn\"\n\
                 {extra_toml}",
                scratch.display(),
            ),
        )
        .expect("write indra.toml");
        let mut child = tokio::process::Command::new(kernel_binary())
            .arg("--config-dir")
            .arg(&config_dir)
            .env("INDRA_API_KEYS", API_KEY)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn real kernel");
        let http = reqwest::Client::new();
        let base = format!("http://127.0.0.1:{api_port}");
        let deadline = std::time::Instant::now() + BOOT;
        loop {
            let up = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                http.get(format!("{base}/healthz")).send(),
            )
            .await;
            if let Ok(Ok(res)) = up {
                if let Ok(text) = res.text().await {
                    if text == "OK"
                        && tokio::net::TcpStream::connect(("127.0.0.1", mqtt_port))
                            .await
                            .is_ok()
                        && tokio::net::TcpStream::connect(("127.0.0.1", ws_port))
                            .await
                            .is_ok()
                    {
                        return Kernel {
                            child,
                            mqtt_port,
                            ws_port,
                            api_port,
                            scratch,
                            config_dir,
                        };
                    }
                }
            }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill().await;
                panic!("kernel never opened API {api_port} and MQTT {mqtt_port}");
            }
            if let Ok(Some(_status)) = child.try_wait() {
                let _ = std::fs::remove_dir_all(&scratch);
                let _ = std::fs::remove_dir_all(&config_dir);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }
    panic!("kernel failed to boot after 3 attempts");
}

/// Anonymous connections are rejected when anonymous access is disabled.
/// When the broker permits anonymous connection without credentials,
/// `assert_eq!(code, 5, "anonymous connection is rejected when allow_anonymous is false")` is the assertion that fails.
#[tokio::test]
async fn ta_001_authenticate_rejects_anonymous_when_disabled() {
    let kernel =
        boot_kernel_strict_auth("ta-001-authn", "\n[auth]\nallow_anonymous = false\n").await;
    let mut sock = dial(kernel.mqtt_port).await;
    send(&mut sock, &connect311("ta-001-anon-cli", true, 60)).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x20);
    let (present, code) = parse_connack311(&body);
    assert!(!present);
    assert_eq!(
        code, 5,
        "anonymous connection is rejected when allow_anonymous is false"
    );
    kernel.finish().await;
}

/// A banned client identifier is refused on connection.
/// When the broker admits a banned client identifier,
/// `assert_eq!(reason, 0x8A, "banned client identifier is refused on connection")` is the assertion that fails.
#[tokio::test]
async fn ta_001_banned_client_id_refused() {
    let kernel = spawn_kernel("ta-001-ban-chk", None).await;
    let http = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{}", kernel.api_port);
    let res = tokio::time::timeout(
        STEP,
        http.post(format!("{base}/api/v5/banned"))
            .bearer_auth(API_KEY)
            .json(&serde_json::json!({"as": "clientid", "who": "ta-001-banned-cli"}))
            .send(),
    )
    .await
    .expect("ban in time")
    .expect("ban send");
    assert_eq!(res.status().as_u16(), 200);

    let mut sock = dial(kernel.mqtt_port).await;
    send(&mut sock, &connect5("ta-001-banned-cli", true, 60, 0)).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x20);
    let (present, reason, _) = parse_connack5(&body);
    assert!(!present);
    assert_eq!(
        reason, 0x8A,
        "banned client identifier is refused on connection"
    );
    kernel.finish().await;
}

/// A banned client cannot resume an offline session on reconnect.
/// When the broker admits a banned client reconnecting to an offline session,
/// `assert_eq!(reason, 0x8A, "banned client cannot resume offline session on reconnect")` is the assertion that fails.
#[tokio::test]
async fn ta_001_banned_client_offline_session_refused() {
    let kernel = spawn_kernel("ta-001-ban-off", None).await;
    let client_id = "ta-001-ban-off-cli";
    let topic = "ta01/ban/offline";
    let props = [17, 0, 0, 0, 120];

    let mut sock = dial(kernel.mqtt_port).await;
    send(
        &mut sock,
        &connect5_with_props(client_id, false, 60, &props),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x20);
    let (present, reason, _) = parse_connack5(&body);
    assert!(!present);
    assert_eq!(reason, 0);

    send(&mut sock, &subscribe_v5(1, &[(topic, 1)], 0)).await;
    let (head, _) = recv(&mut sock).await;
    assert_eq!(head, 0x90);
    send(&mut sock, &[0xE0, 0x00]).await;

    let http = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{}", kernel.api_port);
    let res = tokio::time::timeout(
        STEP,
        http.post(format!("{base}/api/v5/banned"))
            .bearer_auth(API_KEY)
            .json(&serde_json::json!({"as": "clientid", "who": client_id}))
            .send(),
    )
    .await
    .expect("ban in time")
    .expect("ban send");
    assert_eq!(res.status().as_u16(), 200);

    let mut reconnect = dial(kernel.mqtt_port).await;
    send(
        &mut reconnect,
        &connect5_with_props(client_id, false, 60, &props),
    )
    .await;
    let (head, body) = recv(&mut reconnect).await;
    assert_eq!(head, 0x20);
    let (present, reason, _) = parse_connack5(&body);
    assert!(!present);
    assert_eq!(
        reason, 0x8A,
        "banned client cannot resume offline session on reconnect"
    );
    kernel.finish().await;
}

/// Authentication failure in MQTT version 5 returns reason code 0x87.
/// When the broker returns success on authentication failure,
/// `assert_eq!(reason, 0x87, "authentication failure returns reason code 0x87")` is the assertion that fails.
#[tokio::test]
async fn ta_001_connack_auth_error_v5() {
    let kernel =
        boot_kernel_strict_auth("ta-001-autherr", "\n[auth]\nallow_anonymous = false\n").await;
    let mut sock = dial(kernel.mqtt_port).await;
    send(&mut sock, &connect5("ta-001-autherr-cli", true, 60, 0)).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x20);
    let (present, reason, _) = parse_connack5(&body);
    assert!(!present);
    assert_eq!(
        reason, 0x87,
        "authentication failure returns reason code 0x87"
    );
    kernel.finish().await;
}

/// An authorized publish is accepted and acknowledged with PUBACK when authorization is enabled.
/// When authorization does not gate access,
/// `assert_eq!(denied_body[2], 0x87, "unauthorized publish receives PUBACK with reason code 0x87 Not authorized")` is the assertion that fails.
#[tokio::test]
async fn ta_001_authorize_publish_accepted() {
    let kernel = spawn_kernel("ta-001-authz", None).await;
    let http = reqwest::Client::new();
    let res = tokio::time::timeout(
        STEP,
        http.put(format!(
            "http://127.0.0.1:{}/api/v5/authorization/settings",
            kernel.api_port
        ))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({
            "no_match": "deny",
            "deny_action": "ignore",
        }))
        .send(),
    )
    .await
    .expect("update authorization settings in time")
    .expect("update authorization settings send");
    assert_eq!(res.status(), 200, "authorization settings updated");
    let res = tokio::time::timeout(
        STEP,
        http.post(format!(
            "http://127.0.0.1:{}/api/v5/authorization/sources/built_in_database/rules/all",
            kernel.api_port
        ))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({
            "rules": [
                {
                    "action": "publish",
                    "permission": "allow",
                    "topic": "ta01/authz/allowed/#"
                }
            ]
        }))
        .send(),
    )
    .await
    .expect("update authorization rules in time")
    .expect("update authorization rules send");
    assert_eq!(res.status(), 204, "authorization rules updated");

    let mut sock = dial(kernel.mqtt_port).await;
    send(&mut sock, &connect5("ta-001-authz-cli", true, 60, 0)).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x20);
    let (_, reason, _) = parse_connack5(&body);
    assert_eq!(reason, 0);

    // An unauthorized publish is rejected with PUBACK reason code 0x87 Not authorized.
    send(
        &mut sock,
        &publish5(
            "ta01/authz/denied/topic",
            10,
            1,
            false,
            None,
            b"unauth-data",
        ),
    )
    .await;
    let (denied_head, denied_body) = recv(&mut sock).await;
    assert_eq!(denied_head, 0x40, "unauthorized publish receives PUBACK");
    assert_eq!(
        &denied_body[..2],
        &[0x00, 10],
        "PUBACK packet identifier matches publish"
    );
    assert_eq!(
        denied_body[2], 0x87,
        "unauthorized publish receives PUBACK with reason code 0x87 Not authorized"
    );

    // An authorized publish is accepted with PUBACK reason code 0x00 Success.
    send(
        &mut sock,
        &publish5(
            "ta01/authz/allowed/topic",
            20,
            1,
            false,
            None,
            b"authz-data",
        ),
    )
    .await;
    let (auth_head, auth_body) = recv(&mut sock).await;
    assert_eq!(auth_head, 0x40, "authorized publish receives PUBACK");
    assert_eq!(
        &auth_body[..2],
        &[0x00, 20],
        "PUBACK packet identifier matches publish"
    );
    assert_eq!(
        auth_body.get(2).copied().unwrap_or(0),
        0x00,
        "authorized publish receives PUBACK with reason code 0x00 Success"
    );
    kernel.finish().await;
}

/// A published message fans out to all active subscribers on the topic.
/// When any subscriber misses the published message,
/// `assert_eq!(got.payload, b"fanout-msg", "every subscriber receives the fanned-out publish")` is the assertion that fails.
#[tokio::test]
async fn ta_001_fanout_delivers_to_all_subscribers() {
    let kernel = spawn_kernel("ta-001-fanout", None).await;
    let topic = "ta01/fanout/topic";
    let mut subs = Vec::new();
    for i in 1..=4 {
        let mut sock = dial(kernel.mqtt_port).await;
        let cid = format!("ta-001-fanout-sub-{i}");
        assert!(!connect_client(&mut sock, &cid, true, 60).await);
        send(&mut sock, &subscribe(1, &[(topic, 0)])).await;
        let (head, _) = recv(&mut sock).await;
        assert_eq!(head, 0x90);
        subs.push(sock);
    }

    let mut publisher = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut publisher, "ta-001-fanout-pub", true, 60).await);
    send(
        &mut publisher,
        &publish311(topic, 0, 0, false, b"fanout-msg"),
    )
    .await;

    for mut sub in subs {
        let (head, body) = recv(&mut sub).await;
        let got = parse_publish(head, &body);
        assert_eq!(
            got.payload, b"fanout-msg",
            "every subscriber receives the fanned-out publish"
        );
    }
    kernel.finish().await;
}

/// A publish to a topic with no subscribers receives PUBACK.
/// When the broker fails to acknowledge a publish without subscribers,
/// `assert_eq!(head, 0x40, "publish with no subscribers receives PUBACK")` is the assertion that fails.
#[tokio::test]
async fn ta_001_nosub_pub_receives_puback() {
    let kernel = spawn_kernel("ta-001-nosub", None).await;
    let mut sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sock, "ta-001-nosub-pub", true, 60).await);

    send(
        &mut sock,
        &publish311("ta01/nosub/topic", 20, 1, false, b"nosub-payload"),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x40, "publish with no subscribers receives PUBACK");
    assert_eq!(
        &body[..2],
        &[0x00, 20],
        "PUBACK packet identifier matches publish"
    );
    kernel.finish().await;
}

/// A publish fans out to multiple distinct shared subscription groups.
/// When the second shared subscription group misses the publish,
/// `assert_eq!(got2.payload, b"shared2-msg", "second shared group receives the publish")` is the assertion that fails.
#[tokio::test]
async fn ta_001_shared_subscribe_multiple_groups() {
    let kernel = spawn_kernel("ta-001-sh2", None).await;
    let topic = "ta01/shared2";

    let mut sub1 = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub1, "ta-001-sh2-sub1", true, 60).await);
    let filter1 = format!("$share/group1/{topic}");
    send(&mut sub1, &subscribe(1, &[(filter1.as_str(), 0)])).await;
    let (head, _) = recv(&mut sub1).await;
    assert_eq!(head, 0x90);

    let mut sub2 = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub2, "ta-001-sh2-sub2", true, 60).await);
    let filter2 = format!("$share/group2/{topic}");
    send(&mut sub2, &subscribe(1, &[(filter2.as_str(), 0)])).await;
    let (head, _) = recv(&mut sub2).await;
    assert_eq!(head, 0x90);

    let mut publisher = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut publisher, "ta-001-sh2-pub", true, 60).await);
    send(
        &mut publisher,
        &publish311(topic, 0, 0, false, b"shared2-msg"),
    )
    .await;

    let (head1, body1) = recv(&mut sub1).await;
    let got1 = parse_publish(head1, &body1);
    assert_eq!(got1.payload, b"shared2-msg");

    let (head2, body2) = recv(&mut sub2).await;
    let got2 = parse_publish(head2, &body2);
    assert_eq!(
        got2.payload, b"shared2-msg",
        "second shared group receives the publish"
    );

    kernel.finish().await;
}
