use super::*;

async fn sb17_source_status(kernel: &Kernel, id: &str) -> serde_json::Value {
    let (status, body) = mv00_api(
        kernel,
        reqwest::Method::GET,
        &format!("/api/v5/sources/{id}"),
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "source read: {body}");
    body
}

async fn sb17_wait_status(kernel: &Kernel, id: &str, want: &str) {
    let deadline = std::time::Instant::now() + STEP;
    loop {
        let body = sb17_source_status(kernel, id).await;
        if body["status"] == serde_json::json!(want) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "source {id} did not reach {want}: {body}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

async fn sb17_wait_reason(kernel: &Kernel, id: &str, needle: &str) -> serde_json::Value {
    let deadline = std::time::Instant::now() + STEP;
    loop {
        let body = sb17_source_status(kernel, id).await;
        if body["status"] == serde_json::json!("disconnected") {
            if let Some(reason) = body["status_reason"].as_str() {
                if reason.contains(needle) {
                    return body;
                }
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "source {id} did not report {needle:?}: {body}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

async fn sb17_wait_fake(fake: &FakeMqttSource, want_active: usize, want_subscriptions: usize) {
    let deadline = std::time::Instant::now() + STEP;
    loop {
        if fake.active() == want_active && fake.subscription_count() >= want_subscriptions {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "fake broker active={} subscriptions={} (want {want_active}/{want_subscriptions})",
            fake.active(),
            fake.subscription_count()
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn sb17_mqtt_source_ingress_lifecycle_through_broker() {
    let fake = FakeMqttSource::start().await;
    let kernel = spawn_kernel("").await;

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "sb17-conn",
            "type": "mqtt",
            "server": format!("127.0.0.1:{}", fake.port),
        }),
    )
    .await;
    assert_eq!(status, 201, "source connector create: {body}");

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/sources",
        serde_json::json!({
            "name": "sb17-src",
            "type": "mqtt",
            "connector": "sb17-conn",
            "parameters": {"topic": "sb17/remote", "qos": 0},
        }),
    )
    .await;
    assert_eq!(status, 201, "source create: {body}");
    sb17_wait_status(&kernel, "sb17-src", "connected").await;
    sb17_wait_fake(&fake, 1, 1).await;

    let mut subscriber = mv00_connect(&kernel, "sb17-sub").await;
    send_mqtt(&mut subscriber, &sy02_subscribe(1, &[("sb17/remote", 0)])).await;
    let (head, body) = recv_mqtt(&mut subscriber).await;
    assert_eq!(head, 0x90);
    assert_eq!(sy02_parse_suback(&body), (1, vec![0]));

    fake.publish("sb17/remote", br#"{"v":1}"#);
    let (head, body) = recv_mqtt(&mut subscriber).await;
    assert_eq!(head & 0xf0, 0x30, "source ingress is a PUBLISH");
    let message = sy02_parse_publish_classic(head, &body);
    assert_eq!(message.topic, "sb17/remote");
    assert_eq!(message.payload, br#"{"v":1}"#.to_vec());

    // Update with the same remote topic. The old consumer must stop
    // before the new one starts, so one remote message yields one local
    // delivery.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/sources/sb17-src",
        serde_json::json!({
            "name": "sb17-src",
            "type": "mqtt",
            "connector": "sb17-conn",
            "parameters": {"topic": "sb17/remote", "qos": 1},
        }),
    )
    .await;
    assert_eq!(status, 200, "source update: {body}");
    sb17_wait_status(&kernel, "sb17-src", "connected").await;
    sb17_wait_fake(&fake, 1, 2).await;

    // An identical update keeps the live connected status.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/sources/sb17-src",
        serde_json::json!({
            "name": "sb17-src",
            "type": "mqtt",
            "connector": "sb17-conn",
            "parameters": {"topic": "sb17/remote", "qos": 1},
        }),
    )
    .await;
    assert_eq!(status, 200, "identical source update: {body}");
    assert_eq!(body["status"], serde_json::json!("connected"));
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    let current = sb17_source_status(&kernel, "sb17-src").await;
    assert_eq!(current["status"], serde_json::json!("connected"));

    // Start on a running source keeps the live connected status.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/sources/sb17-src/start",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "start on running source: {body}");
    let current = sb17_source_status(&kernel, "sb17-src").await;
    assert_eq!(current["status"], serde_json::json!("connected"));

    // Publish a burst, then update the source while the burst is still
    // pending. The stopped consumer must drain every packet that the
    // driver already read, so the whole burst arrives once.
    let burst: Vec<Vec<u8>> = (0..40)
        .map(|index| format!("cutover-{index}").into_bytes())
        .collect();
    let before = fake.written();
    fake.publish_burst("sb17/remote", &burst);
    let deadline = std::time::Instant::now() + STEP;
    while fake.written() < before + burst.len() {
        assert!(
            std::time::Instant::now() < deadline,
            "fake wrote {} of {} burst messages",
            fake.written() - before,
            burst.len()
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/sources/sb17-src",
        serde_json::json!({
            "name": "sb17-src",
            "type": "mqtt",
            "connector": "sb17-conn",
            "parameters": {"topic": "sb17/remote", "qos": 0},
        }),
    )
    .await;
    assert_eq!(status, 200, "cutover source update: {body}");
    sb17_wait_status(&kernel, "sb17-src", "connected").await;
    let mut received = Vec::new();
    for _ in 0..burst.len() {
        let (head, body) = recv_mqtt(&mut subscriber).await;
        assert_eq!(head & 0xf0, 0x30, "cutover burst delivers");
        received.push(sy02_parse_publish_classic(head, &body).payload);
    }
    for expected in &burst {
        assert!(
            received.contains(expected),
            "cutover lost {:?}",
            String::from_utf8_lossy(expected)
        );
    }
    let extra = tokio::time::timeout(
        std::time::Duration::from_millis(400),
        read_mqtt_packet(&mut subscriber),
    )
    .await;
    assert!(extra.is_err(), "the cutover delivered no duplicate");

    fake.publish("sb17/remote", b"one");
    let (head, body) = recv_mqtt(&mut subscriber).await;
    assert_eq!(head & 0xf0, 0x30, "updated source delivers");
    let message = sy02_parse_publish_classic(head, &body);
    assert_eq!(message.payload, b"one".to_vec());
    let duplicate = tokio::time::timeout(
        std::time::Duration::from_millis(400),
        read_mqtt_packet(&mut subscriber),
    )
    .await;
    assert!(
        duplicate.is_err(),
        "the stopped consumer must not deliver a second copy"
    );

    // A reference source kind without an ingress runtime is refused and
    // leaves the working source in place.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/sources/sb17-src",
        serde_json::json!({"name": "sb17-src", "type": "kafka_consumer"}),
    )
    .await;
    assert_eq!(status, 400, "unsupported update: {body}");
    let current = sb17_source_status(&kernel, "sb17-src").await;
    assert_eq!(current["type"], serde_json::json!("mqtt"));
    assert_eq!(current["status"], serde_json::json!("connected"));

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/sources_probe",
        serde_json::json!({
            "type": "mqtt",
            "server": format!("127.0.0.1:{}", fake.port),
            "parameters": {"topic": "sb17/remote"},
        }),
    )
    .await;
    assert_eq!(status, 204, "source probe dials a supplied target: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/sources_probe",
        serde_json::json!({"type": "mqtt", "server": "127.0.0.1:9"}),
    )
    .await;
    assert_eq!(status, 400, "source probe refuses a closed target: {body}");

    // A source with no connector is refused before the store. The read
    // answers 404 because nothing was stored.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/sources",
        serde_json::json!({
            "name": "sb17-bad",
            "type": "mqtt",
            "parameters": {"topic": "sb17/bad"},
        }),
    )
    .await;
    assert_eq!(status, 400, "source without connector: {body}");
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|message| message.contains("connector")),
        "the refusal names the connector: {body}"
    );
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/sources/sb17-bad",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 404, "a refused source is not stored");

    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/sources/sb17-src",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "source delete");
    let deadline = std::time::Instant::now() + STEP;
    while fake.active() != 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "delete stops the consumer"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/sources/sb17-src",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 404, "deleted source is unknown");

    kernel.finish().await;
}

#[tokio::test]
async fn sb17_source_resource_opts_apply_and_refuse_through_the_api() {
    let slow_fake = ClosingMqttSource::start().await;
    let fast_fake = ClosingMqttSource::start().await;
    let kernel = spawn_kernel("").await;

    for (name, fake) in [
        ("sb17-opts-slow", &slow_fake),
        ("sb17-opts-fast", &fast_fake),
    ] {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/connectors",
            serde_json::json!({
                "name": format!("{name}-conn"),
                "type": "mqtt",
                "server": format!("127.0.0.1:{}", fake.port),
            }),
        )
        .await;
        assert_eq!(status, 201, "{name} connector create: {body}");
    }

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/sources",
        serde_json::json!({
            "name": "sb17-opts-slow",
            "type": "mqtt",
            "connector": "sb17-opts-slow-conn",
            "parameters": {"topic": "sb17/opts"},
            "resource_opts": {"health_check_interval": "10s"},
        }),
    )
    .await;
    assert_eq!(status, 201, "slow source create: {body}");

    let fast_body = serde_json::json!({
        "name": "sb17-opts-fast",
        "type": "mqtt",
        "connector": "sb17-opts-fast-conn",
        "parameters": {"topic": "sb17/opts"},
        "resource_opts": {"health_check_interval": "10ms"},
    });
    let (status, body) =
        mv00_api(&kernel, reqwest::Method::POST, "/api/v5/sources", fast_body).await;
    assert_eq!(status, 201, "fast source create: {body}");

    // An unknown key and a malformed duration are refused before the
    // store on create and update.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/sources",
        serde_json::json!({
            "name": "sb17-opts-bad",
            "type": "mqtt",
            "connector": "sb17-opts-fast-conn",
            "parameters": {"topic": "sb17/opts"},
            "resource_opts": {"inventory_unknown": "requested"},
        }),
    )
    .await;
    assert_eq!(status, 400, "unknown resource option create: {body}");
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|message| message.contains("resource_opts.inventory_unknown")),
        "the refusal names the unknown key: {body}"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/sources/sb17-opts-fast",
        serde_json::json!({
            "name": "sb17-opts-fast",
            "type": "mqtt",
            "connector": "sb17-opts-fast-conn",
            "parameters": {"topic": "sb17/opts"},
            "resource_opts": {"health_check_interval": "soon"},
        }),
    )
    .await;
    assert_eq!(status, 400, "malformed resource option update: {body}");
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|message| message.contains("not a valid duration")),
        "the refusal names the malformed value: {body}"
    );
    let current = sb17_source_status(&kernel, "sb17-opts-fast").await;
    assert_eq!(
        current["resource_opts"]["health_check_interval"],
        serde_json::json!("10ms"),
        "a refused update keeps the stored options: {current}"
    );

    // The fast interval retries many times inside one slow interval.
    let deadline = std::time::Instant::now() + STEP;
    while (fast_fake.accepts() < 3 || slow_fake.accepts() < 1)
        && std::time::Instant::now() < deadline
    {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let fast = fast_fake.accepts();
    let slow = slow_fake.accepts();
    assert!(
        fast >= 3,
        "the fast interval reconnects: fast={fast} slow={slow}"
    );
    assert!(
        (1..=2).contains(&slow),
        "the slow interval holds the retry: fast={fast} slow={slow}"
    );

    kernel.finish().await;
}

#[tokio::test]
async fn sb17_mqtt_source_survives_restart_and_keeps_delivering() {
    let fake = FakeMqttSource::start().await;
    let mut kernel = spawn_kernel("").await;

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "sb17-restart-conn",
            "type": "mqtt",
            "server": format!("127.0.0.1:{}", fake.port),
        }),
    )
    .await;
    assert_eq!(status, 201, "restart connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/sources",
        serde_json::json!({
            "name": "sb17-restart-src",
            "type": "mqtt",
            "connector": "sb17-restart-conn",
            "parameters": {"topic": "sb17/restart", "qos": 0},
        }),
    )
    .await;
    assert_eq!(status, 201, "restart source create: {body}");
    sb17_wait_status(&kernel, "sb17-restart-src", "connected").await;

    let mut subscriber = mv00_connect(&kernel, "sb17-restart-sub").await;
    send_mqtt(&mut subscriber, &sy02_subscribe(1, &[("sb17/restart", 0)])).await;
    let (head, body) = recv_mqtt(&mut subscriber).await;
    assert_eq!(head, 0x90);
    assert_eq!(sy02_parse_suback(&body), (1, vec![0]));
    fake.publish("sb17/restart", b"before");
    let (head, body) = recv_mqtt(&mut subscriber).await;
    assert_eq!(head & 0xf0, 0x30);
    assert_eq!(
        sy02_parse_publish_classic(head, &body).payload,
        b"before".to_vec()
    );

    kernel.restart().await;
    sb17_wait_status(&kernel, "sb17-restart-src", "connected").await;

    let mut subscriber = mv00_connect(&kernel, "sb17-restart-sub2").await;
    send_mqtt(&mut subscriber, &sy02_subscribe(1, &[("sb17/restart", 0)])).await;
    let (head, body) = recv_mqtt(&mut subscriber).await;
    assert_eq!(head, 0x90);
    assert_eq!(sy02_parse_suback(&body), (1, vec![0]));
    fake.publish("sb17/restart", b"after");
    let (head, body) = recv_mqtt(&mut subscriber).await;
    assert_eq!(head & 0xf0, 0x30, "restarted source delivers");
    let message = sy02_parse_publish_classic(head, &body);
    assert_eq!(message.topic, "sb17/restart");
    assert_eq!(message.payload, b"after".to_vec());

    kernel.finish().await;
}

#[tokio::test]
async fn sb17_mqtt_source_stays_in_the_default_tenant() {
    let fake = FakeMqttSource::start().await;
    let kernel = spawn_kernel("[[tenants.rules]]\nexpression = \"t-${username}\"\n").await;

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "sb17-tenant-conn",
            "type": "mqtt",
            "server": format!("127.0.0.1:{}", fake.port),
        }),
    )
    .await;
    assert_eq!(status, 201, "tenant connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/sources",
        serde_json::json!({
            "name": "sb17-tenant-src",
            "type": "mqtt",
            "connector": "sb17-tenant-conn",
            "parameters": {"topic": "sb17/tenant", "qos": 0},
        }),
    )
    .await;
    assert_eq!(status, 201, "tenant source create: {body}");
    sb17_wait_status(&kernel, "sb17-tenant-src", "connected").await;

    let mut default_sub = mv00_connect(&kernel, "sb17-tenant-default").await;
    send_mqtt(&mut default_sub, &sy02_subscribe(1, &[("sb17/tenant", 0)])).await;
    let (head, body) = recv_mqtt(&mut default_sub).await;
    assert_eq!(head, 0x90);
    assert_eq!(sy02_parse_suback(&body), (1, vec![0]));

    let mut tenant_sub = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(
        &mut tenant_sub,
        &sy02_connect5("sb17-tenant-a", true, 60, Some(("user-a", b"pw")), None, 0),
    )
    .await;
    let (head, body) = recv_mqtt(&mut tenant_sub).await;
    assert_eq!(head, 0x20, "tenant A CONNECT");
    assert_eq!(sy02_parse_connack5(&body), (false, 0, 10));
    send_mqtt(
        &mut tenant_sub,
        &sy02_subscribe_v5(1, &[("sb17/tenant", 0)], 0),
    )
    .await;
    let (head, body) = recv_mqtt(&mut tenant_sub).await;
    assert_eq!(head & 0xf0, 0x90);
    assert_eq!(sy02_parse_suback5(&body), (1, vec![0]));

    fake.publish("sb17/tenant", b"default-only");
    let (head, body) = recv_mqtt(&mut default_sub).await;
    assert_eq!(
        head & 0xf0,
        0x30,
        "the default tenant receives source ingress"
    );
    assert_eq!(
        sy02_parse_publish_classic(head, &body).payload,
        b"default-only".to_vec()
    );
    let leaked = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_mqtt_packet(&mut tenant_sub),
    )
    .await;
    assert!(leaked.is_err(), "the source output does not reach tenant A");

    kernel.finish().await;
}

#[tokio::test]
async fn sb17_mqtt_source_refuses_tls_and_v5_with_a_reason() {
    let kernel = spawn_kernel("").await;

    for (name, connector_body, needle) in [
        (
            "sb17-tls",
            serde_json::json!({
                "name": "sb17-tls-conn",
                "type": "mqtt",
                "server": "mqtts://127.0.0.1:1",
            }),
            "TLS",
        ),
        (
            "sb17-v5",
            serde_json::json!({
                "name": "sb17-v5-conn",
                "type": "mqtt",
                "server": "127.0.0.1:1",
                "proto_ver": "v5",
            }),
            "v5",
        ),
    ] {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/connectors",
            connector_body,
        )
        .await;
        assert_eq!(status, 201, "{name} connector create: {body}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/sources",
            serde_json::json!({
                "name": format!("{name}-src"),
                "type": "mqtt",
                "connector": format!("{name}-conn"),
                "parameters": {"topic": "sb17/refused"},
            }),
        )
        .await;
        assert_eq!(status, 201, "{name} source create: {body}");
        sb17_wait_reason(&kernel, &format!("{name}-src"), needle).await;
    }

    kernel.finish().await;
}
