use super::support::*;

/// Subscription authorization rejects unauthorized subscriptions and accepts authorized subscriptions.
/// When the broker permits unauthorized subscription without access,
/// `assert_eq!(reason_codes, &[0x87], "unauthorized subscription receives SUBACK with reason code 0x87 Not authorized")` is the assertion that fails.
#[tokio::test]
async fn ta_002_subscribe_authorization_gated() {
    let kernel = spawn_kernel("ta-002-sub-authz", None).await;
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
                    "action": "subscribe",
                    "permission": "allow",
                    "topic": "ta02/authz/allowed/#"
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
    send(&mut sock, &connect5("ta-002-authz-cli", true, 60, 0)).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x20);
    let (_, reason, _) = parse_connack5(&body);
    assert_eq!(reason, 0);

    // An unauthorized subscription is rejected with SUBACK reason code 0x87.
    send(
        &mut sock,
        &subscribe_v5(1, &[("ta02/authz/denied/topic", 0)], 0),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x90, "unauthorized subscribe receives SUBACK");
    assert_eq!(&body[..2], &[0x00, 0x01]);
    let (prop_len, prop_bytes) = decode_varint(&body[2..]).expect("valid suback varint");
    let reason_codes = &body[2 + prop_bytes + prop_len..];
    assert_eq!(
        reason_codes,
        &[0x87],
        "unauthorized subscription receives SUBACK with reason code 0x87 Not authorized"
    );

    // An authorized subscription is accepted with SUBACK reason code 0x00.
    send(
        &mut sock,
        &subscribe_v5(2, &[("ta02/authz/allowed/topic", 0)], 0),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x90, "authorized subscribe receives SUBACK");
    assert_eq!(&body[..2], &[0x00, 0x02]);
    let (prop_len, prop_bytes) = decode_varint(&body[2..]).expect("valid suback varint");
    let reason_codes = &body[2 + prop_bytes + prop_len..];
    assert_eq!(
        reason_codes,
        &[0x00],
        "authorized subscription receives SUBACK with reason code 0x00 Success"
    );

    kernel.finish().await;
}

/// Session takeover terminates the superseded connection and closes its socket.
/// When the broker fails to terminate the superseded connection socket,
/// `assert!(matches!(closed, Ok(Err(_))), "session takeover terminates the superseded connection socket")` is the assertion that fails.
#[tokio::test]
async fn ta_002_session_takeover_terminates_previous_connection() {
    let kernel = spawn_kernel("ta-002-takeover", None).await;
    let client_id = "ta-002-takeover-client";

    let mut sock1 = dial(kernel.mqtt_port).await;
    send(&mut sock1, &connect311(client_id, false, 60)).await;
    let (head, body) = recv(&mut sock1).await;
    assert_eq!(head, 0x20);
    assert_eq!(parse_connack311(&body), (false, 0));

    let mut sock2 = dial(kernel.mqtt_port).await;
    send(&mut sock2, &connect311(client_id, false, 60)).await;
    let (head, body) = recv(&mut sock2).await;
    assert_eq!(head, 0x20);
    assert_eq!(parse_connack311(&body), (true, 0));

    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock1)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "session takeover terminates the superseded connection socket"
    );

    kernel.finish().await;
}

/// A publish to a shared subscription group delivers to exactly one group member.
/// When multiple group members receive the publish or delivery is missed,
/// `assert_eq!(delivery_count, 1, "exactly one member of the shared subscription group receives the publish")` is the assertion that fails.
#[tokio::test]
async fn ta_002_shared_subscribe_single_delivery_across_group() {
    let kernel = spawn_kernel("ta-002-shared", None).await;
    let topic = "ta02/shared/deliver";
    let share_filter = format!("$share/ta02grp/{topic}");

    let mut sub1 = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub1, "ta-002-sh-sub1", true, 60).await);
    send(&mut sub1, &subscribe(1, &[(share_filter.as_str(), 0)])).await;
    let (head, _) = recv(&mut sub1).await;
    assert_eq!(head, 0x90);

    let mut sub2 = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub2, "ta-002-sh-sub2", true, 60).await);
    send(&mut sub2, &subscribe(2, &[(share_filter.as_str(), 0)])).await;
    let (head, _) = recv(&mut sub2).await;
    assert_eq!(head, 0x90);

    let mut publisher = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut publisher, "ta-002-sh-pub", true, 60).await);
    send(
        &mut publisher,
        &publish311(topic, 0, 0, false, b"shared-single-msg"),
    )
    .await;

    let res1 = tokio::time::timeout(STEP, read_mqtt_packet(&mut sub1)).await;
    let res2 = tokio::time::timeout(STEP, read_mqtt_packet(&mut sub2)).await;

    let mut delivery_count = 0;
    if let Ok(Ok((head, body))) = res1 {
        let pub_msg = parse_publish(head, &body);
        if pub_msg.payload == b"shared-single-msg" {
            delivery_count += 1;
        }
    }
    if let Ok(Ok((head, body))) = res2 {
        let pub_msg = parse_publish(head, &body);
        if pub_msg.payload == b"shared-single-msg" {
            delivery_count += 1;
        }
    }
    assert_eq!(
        delivery_count, 1,
        "exactly one member of the shared subscription group receives the publish"
    );

    kernel.finish().await;
}

/// Subscribing to a topic delivers subsequent publishes on that topic to the active subscriber.
/// When the subscriber misses the published message,
/// `assert_eq!(got.payload, b"sub-pub-payload", "subscriber receives the published message")` is the assertion that fails.
#[tokio::test]
async fn ta_002_sub_pub_delivers_to_subscriber() {
    let kernel = spawn_kernel("ta-002-subpub", None).await;
    let topic = "ta02/subpub/direct";

    let mut sub = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub, "ta-002-subpub-sub", true, 60).await);
    send(&mut sub, &subscribe(1, &[(topic, 0)])).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90);

    let mut publisher = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut publisher, "ta-002-subpub-pub", true, 60).await);
    send(
        &mut publisher,
        &publish311(topic, 0, 0, false, b"sub-pub-payload"),
    )
    .await;

    let (head, body) = recv(&mut sub).await;
    let got = parse_publish(head, &body);
    assert_eq!(got.topic, topic);
    assert_eq!(
        got.payload, b"sub-pub-payload",
        "subscriber receives the published message"
    );

    kernel.finish().await;
}

/// Batch delivery of messages to an active subscriber preserves outgoing QoS levels.
/// When delivery fails to preserve the QoS 1 level,
/// `assert_eq!(q1_got.qos, 1, "delivery preserves QoS 1 level")` is the assertion that fails.
#[tokio::test]
async fn ta_002_deliver_preserves_outgoing_qos_levels() {
    let kernel = spawn_kernel("ta-002-qos-del", None).await;
    let topic_q1 = "ta02/qos/deliver/q1";
    let topic_q2 = "ta02/qos/deliver/q2";

    let mut sub = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub, "ta-002-qos-sub", true, 60).await);
    send(&mut sub, &subscribe(1, &[("ta02/qos/deliver/+", 2)])).await;
    let (head, body) = recv(&mut sub).await;
    assert_eq!(head, 0x90);
    assert_eq!(&body[2..], &[0x02]);

    let mut publisher = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut publisher, "ta-002-qos-pub", true, 60).await);

    // Publish QoS 1 message
    send(
        &mut publisher,
        &publish311(topic_q1, 10, 1, false, b"qos1-payload"),
    )
    .await;
    let (head, body) = recv(&mut publisher).await;
    assert_eq!(head, 0x40, "publisher receives PUBACK for QoS 1");
    assert_eq!(&body[..2], &[0x00, 10]);

    let (head, body) = recv(&mut sub).await;
    let q1_got = parse_publish(head, &body);
    assert_eq!(q1_got.topic, topic_q1);
    assert_eq!(q1_got.qos, 1, "delivery preserves QoS 1 level");
    assert_eq!(q1_got.payload, b"qos1-payload");
    send(&mut sub, &ack(0x40, q1_got.packet_id)).await;

    // Publish QoS 2 message
    send(
        &mut publisher,
        &publish311(topic_q2, 20, 2, false, b"qos2-payload"),
    )
    .await;
    let (head, body) = recv(&mut publisher).await;
    assert_eq!(head, 0x50, "publisher receives PUBREC for QoS 2");
    assert_eq!(&body[..2], &[0x00, 20]);
    send(&mut publisher, &ack(0x62, 20)).await;
    let (head, body) = recv(&mut publisher).await;
    assert_eq!(head, 0x70, "publisher receives PUBCOMP for QoS 2");
    assert_eq!(&body[..2], &[0x00, 20]);

    let (head, body) = recv(&mut sub).await;
    let q2_got = parse_publish(head, &body);
    assert_eq!(q2_got.topic, topic_q2);
    assert_eq!(q2_got.qos, 2, "delivery preserves QoS 2 level");
    assert_eq!(q2_got.payload, b"qos2-payload");
    send(&mut sub, &ack(0x50, q2_got.packet_id)).await;
    let (head, body) = recv(&mut sub).await;
    assert_eq!(head, 0x62, "subscriber receives PUBREL");
    assert_eq!(&body[..2], &q2_got.packet_id.to_be_bytes());
    send(&mut sub, &ack(0x70, q2_got.packet_id)).await;

    kernel.finish().await;
}
