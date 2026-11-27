//! Conformance tests for core MQTT packet handling and channel operations.
//!
//! Proves that the kernel listener handles ingress and egress MQTT
//! packets, session operations and topic alias management over real TCP sockets.

use super::support::*;

/// Builds an MQTT 5 CONNECT with username and password.
fn connect5_auth(client_id: &str, username: &str, password: &[u8], keepalive: u16) -> Vec<u8> {
    let mut rest = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0xC2];
    rest.extend_from_slice(&keepalive.to_be_bytes());
    rest.push(0x00);
    rest.extend_from_slice(&mqtt_str(client_id));
    rest.extend_from_slice(&mqtt_str(username));
    rest.extend_from_slice(&(password.len() as u16).to_be_bytes());
    rest.extend_from_slice(password);
    let mut packet = vec![0x10];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// Builds an MQTT 5 CONNECT with session expiry interval zero and a will message.
fn connect5_will_exp0(client_id: &str, will_topic: &str, will_payload: &[u8]) -> Vec<u8> {
    let mut rest = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x06];
    rest.extend_from_slice(&60u16.to_be_bytes());
    let props = [17u8, 0, 0, 0, 0];
    rest.extend_from_slice(&varint(props.len() as u32));
    rest.extend_from_slice(&props);
    rest.extend_from_slice(&mqtt_str(client_id));
    rest.push(0x00);
    rest.extend_from_slice(&mqtt_str(will_topic));
    rest.extend_from_slice(&(will_payload.len() as u16).to_be_bytes());
    rest.extend_from_slice(will_payload);
    let mut packet = vec![0x10];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

/// An authorized publish message is accepted and delivered to subscribers when authorization is enabled.
/// When authorization erroneously drops the authorized publish,
/// `assert_eq!(head & 0xF0, 0x30, "subscriber receives authorized publish")` is the assertion that fails.
#[tokio::test]
async fn ta_006_publish_check_authz() {
    let kernel = spawn_kernel("ta-006-authz", None).await;
    let topic_ok = "ta06/authz/ok/test";
    let http = reqwest::Client::new();
    let res = http
        .put(format!(
            "http://127.0.0.1:{}/api/v5/authorization/settings",
            kernel.api_port
        ))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({
            "no_match": "deny",
            "deny_action": "ignore",
        }))
        .send()
        .await
        .expect("update authorization settings");
    assert_eq!(res.status(), 200, "authorization settings updated");
    let res = http
        .post(format!(
            "http://127.0.0.1:{}/api/v5/authorization/sources/built_in_database/rules/all",
            kernel.api_port
        ))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({
            "rules": [
                {
                    "action": "all",
                    "permission": "allow",
                    "topic": "ta06/authz/ok/#"
                }
            ]
        }))
        .send()
        .await
        .expect("update authorization rules");
    assert_eq!(res.status(), 204, "authorization rules updated");

    let mut sub = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub, "ta-006-authz-sub", true, 60).await);
    send(&mut sub, &subscribe(1, &[(topic_ok, 0)])).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90);

    let mut pub_sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut pub_sock, "ta-006-authz-pub", true, 60).await);
    send(
        &mut pub_sock,
        &publish311(topic_ok, 0, 0, false, b"authz-ok-payload"),
    )
    .await;

    let (head, body) = recv(&mut sub).await;
    assert_eq!(head & 0xF0, 0x30, "subscriber receives authorized publish");
    let got = parse_publish(head, &body);
    assert_eq!(
        got.payload, b"authz-ok-payload",
        "authorized publish delivers matching payload"
    );
    kernel.finish().await;
}

/// A rejected QoS 2 publish releases the awaiting packet identifier immediately in MQTT 5.
/// When the broker does not return the error PUBREC for denied QoS 2 publish,
/// `assert_eq!(body[2], 0x87, "PUBREC carries reason code 0x87 Not authorized")` is the assertion that fails.
#[tokio::test]
async fn ta_006_rejected_qos2_publish() {
    let kernel = spawn_kernel("ta-006-rej-q2", None).await;
    let topic_ok = "ta06/q2/allowed";
    let topic_denied = "ta06/q2/denied";
    let http = reqwest::Client::new();
    let res = http
        .put(format!(
            "http://127.0.0.1:{}/api/v5/authorization/settings",
            kernel.api_port
        ))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({
            "no_match": "deny",
            "deny_action": "ignore",
        }))
        .send()
        .await
        .expect("update authorization settings");
    assert_eq!(res.status(), 200);
    let res = http
        .post(format!(
            "http://127.0.0.1:{}/api/v5/authorization/sources/built_in_database/rules/all",
            kernel.api_port
        ))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({
            "rules": [
                {
                    "action": "all",
                    "permission": "allow",
                    "topic": "ta06/q2/allowed/#"
                }
            ]
        }))
        .send()
        .await
        .expect("update authorization rules");
    assert_eq!(res.status(), 204);

    let mut sub = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub, "ta-006-rej-q2-sub", true, 60).await);
    send(&mut sub, &subscribe(1, &[(topic_ok, 2)])).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90);

    // MQTT 5: rejected QoS 2 publish receives PUBREC with reason code 0x87,
    // and releases packet identifier without requiring PUBREL.
    let mut v5_pub = dial(kernel.mqtt_port).await;
    send(&mut v5_pub, &connect5("ta-006-rej-v5-pub", true, 60, 0)).await;
    let (head, _) = recv(&mut v5_pub).await;
    assert_eq!(head & 0xF0, 0x20);

    send(
        &mut v5_pub,
        &publish5(topic_denied, 10, 2, false, None, b"denied-q2"),
    )
    .await;
    let (head, body) = recv(&mut v5_pub).await;
    assert_eq!(head, 0x50, "QoS 2 publish receives PUBREC");
    assert_eq!(&body[..2], &[0x00, 10], "PUBREC packet id matches");
    assert_eq!(
        body[2], 0x87,
        "PUBREC carries reason code 0x87 Not authorized"
    );

    // Reuse packet identifier 10 immediately for authorized publish without PUBREL.
    send(
        &mut v5_pub,
        &publish5(topic_ok, 10, 2, false, None, b"ok-q2"),
    )
    .await;
    let (head, body) = recv(&mut v5_pub).await;
    assert_eq!(head, 0x50, "authorized QoS 2 receives PUBREC");
    assert_eq!(&body[..2], &[0x00, 10]);
    assert_eq!(
        body.get(2).copied().unwrap_or(0),
        0x00,
        "authorized PUBREC has reason 0x00 Success"
    );
    send(&mut v5_pub, &ack(0x62, 10)).await;
    let (head, _) = recv(&mut v5_pub).await;
    assert_eq!(head, 0x70, "broker sends PUBCOMP");

    // Subscriber receives the authorized message.
    let (head, body) = recv(&mut sub).await;
    assert_eq!(head & 0xF0, 0x30);
    let live = parse_publish(head, &body);
    assert_eq!(live.payload, b"ok-q2");
    send(&mut sub, &ack(0x50, live.packet_id)).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x62);
    send(&mut sub, &ack(0x70, live.packet_id)).await;

    // MQTT 3.1.1: rejected QoS 2 publish receives short PUBREC (no reason code).
    let mut v4_pub = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut v4_pub, "ta-006-rej-v4-pub", true, 60).await);
    send(
        &mut v4_pub,
        &publish311(topic_denied, 20, 2, false, b"denied-v4"),
    )
    .await;
    let (head, body) = recv(&mut v4_pub).await;
    assert_eq!(head, 0x50);
    assert_eq!(&body[..2], &[0x00, 20]);
    assert_eq!(body.len(), 2, "MQTT 3.1.1 PUBREC has no reason code");
    send(&mut v4_pub, &ack(0x62, 20)).await;
    let (head, body) = recv(&mut v4_pub).await;
    assert_eq!(head, 0x70, "broker acknowledges PUBREL with PUBCOMP");
    assert_eq!(&body[..2], &[0x00, 20]);

    kernel.finish().await;
}

/// Kicking a connected client publishes its registered will message to subscribers.
/// When the broker does not publish the will message upon kicking a client,
/// `assert_eq!(head & 0xF0, 0x30, "watcher receives will on client kick")` is the assertion that fails.
#[tokio::test]
async fn ta_006_will_kick_connected() {
    let kernel = spawn_kernel("ta-006-will-kick", None).await;
    let will_topic = "ta06/will/kicked";
    let target_client = "ta-006-kicked-client";

    let mut watcher = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut watcher, "ta-006-kick-watcher", true, 60).await);
    send(&mut watcher, &subscribe(1, &[(will_topic, 0)])).await;
    let (head, _) = recv(&mut watcher).await;
    assert_eq!(head, 0x90);

    let mut target = dial(kernel.mqtt_port).await;
    send(
        &mut target,
        &connect311_will(target_client, 60, will_topic, b"kick-will-payload"),
    )
    .await;
    let (head, body) = recv(&mut target).await;
    assert_eq!(head, 0x20);
    assert_eq!(parse_connack311(&body), (false, 0));

    let http = reqwest::Client::new();
    let res = http
        .delete(format!(
            "http://127.0.0.1:{}/api/v5/clients/{}",
            kernel.api_port, target_client
        ))
        .bearer_auth(API_KEY)
        .send()
        .await
        .expect("kick client via API");
    assert_eq!(res.status(), 204, "client kick returns 204");

    let (head, body) = recv(&mut watcher).await;
    assert_eq!(head & 0xF0, 0x30, "watcher receives will on client kick");
    let got = parse_publish(head, &body);
    assert_eq!(got.topic, will_topic);
    assert_eq!(
        got.payload, b"kick-will-payload",
        "will payload matches registered message"
    );
    kernel.finish().await;
}

/// A session takeover publishes the will message of the replaced connection.
/// When the broker does not publish the will message upon session takeover,
/// `assert_eq!(head & 0xF0, 0x30, "watcher receives will on session takeover")` is the assertion that fails.
#[tokio::test]
async fn ta_006_will_takeover_no_delay_connected() {
    let kernel = spawn_kernel("ta-006-takeover", None).await;
    let will_topic = "ta06/will/takeover";
    let client_id = "ta-006-takeover-client";

    let mut watcher = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut watcher, "ta-006-takeover-watcher", true, 60).await);
    send(&mut watcher, &subscribe(1, &[(will_topic, 0)])).await;
    let (head, _) = recv(&mut watcher).await;
    assert_eq!(head, 0x90);

    let mut first = dial(kernel.mqtt_port).await;
    send(
        &mut first,
        &connect311_will(client_id, 60, will_topic, b"takeover-will-payload"),
    )
    .await;
    let (head, body) = recv(&mut first).await;
    assert_eq!(head, 0x20);
    assert_eq!(parse_connack311(&body), (false, 0));

    // Second connection takes over the session with the same client id.
    let mut second = dial(kernel.mqtt_port).await;
    send(&mut second, &connect311(client_id, false, 60)).await;
    let (head, body) = recv(&mut second).await;
    assert_eq!(head, 0x20);
    assert_eq!(parse_connack311(&body).1, 0, "second connect succeeds");

    let (head, body) = recv(&mut watcher).await;
    assert_eq!(
        head & 0xF0,
        0x30,
        "watcher receives will on session takeover"
    );
    let got = parse_publish(head, &body);
    assert_eq!(got.topic, will_topic);
    assert_eq!(
        got.payload, b"takeover-will-payload",
        "takeover will payload matches registered message"
    );
    kernel.finish().await;
}

/// A session takeover on a connection with session expiry interval zero publishes its will message.
/// When the broker does not publish the will message upon takeover with expiry 0,
/// `assert_eq!(head & 0xF0, 0x30, "watcher receives will on takeover with session expiry 0")` is the assertion that fails.
#[tokio::test]
async fn ta_006_will_takeover_session_expiry0_connected() {
    let kernel = spawn_kernel("ta-006-tk-exp0", None).await;
    let will_topic = "ta06/will/exp0";
    let client_id = "ta-006-exp0-client";

    let mut watcher = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut watcher, "ta-006-exp0-watcher", true, 60).await);
    send(&mut watcher, &subscribe(1, &[(will_topic, 0)])).await;
    let (head, _) = recv(&mut watcher).await;
    assert_eq!(head, 0x90);

    let mut first = dial(kernel.mqtt_port).await;
    send(
        &mut first,
        &connect5_will_exp0(client_id, will_topic, b"exp0-will-payload"),
    )
    .await;
    let (head, body) = recv(&mut first).await;
    assert_eq!(head & 0xF0, 0x20);
    let (_, reason, _) = parse_connack5(&body);
    assert_eq!(reason, 0);

    // Second connection takes over.
    let mut second = dial(kernel.mqtt_port).await;
    send(&mut second, &connect5(client_id, true, 60, 0)).await;
    let (head, body) = recv(&mut second).await;
    assert_eq!(head & 0xF0, 0x20);
    let (_, reason, _) = parse_connack5(&body);
    assert_eq!(reason, 0);

    let (head, body) = recv(&mut watcher).await;
    assert_eq!(
        head & 0xF0,
        0x30,
        "watcher receives will on takeover with session expiry 0"
    );
    let got = parse_publish(head, &body);
    assert_eq!(got.topic, will_topic);
    assert_eq!(
        got.payload, b"exp0-will-payload",
        "takeover expiry 0 will payload matches"
    );
    kernel.finish().await;
}

/// An incoming publish with a topic alias exceeding the negotiated maximum is rejected with Topic Alias Invalid.
/// When the broker does not reject an exceeded topic alias with 0x94,
/// `assert_eq!(body[2], 0x94, "PUBACK reason code is Topic Alias Invalid (0x94)")` is the assertion that fails.
#[tokio::test]
async fn ta_006_publish_check_alias() {
    let kernel = spawn_kernel("ta-006-alias-chk", None).await;
    let mut sock = dial(kernel.mqtt_port).await;
    send(&mut sock, &connect5("ta-006-alias-chk-cli", true, 60, 10)).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x20);
    let (_, reason, server_max) = parse_connack5(&body);
    assert_eq!(reason, 0);
    assert!(server_max > 0, "server advertises topic alias maximum");

    // Publish with alias within range succeeds.
    send(
        &mut sock,
        &publish5("ta06/alias/valid", 1, 1, false, Some(1), b"valid-alias"),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x40, "valid alias receives PUBACK");
    assert_eq!(&body[..2], &[0x00, 1]);
    assert_eq!(
        body.get(2).copied().unwrap_or(0),
        0x00,
        "valid alias PUBACK reason code is 0x00 Success"
    );

    // Publish with alias exceeding the negotiated server maximum is rejected.
    send(
        &mut sock,
        &publish5(
            "ta06/alias/overflow",
            2,
            1,
            false,
            Some(server_max + 1),
            b"over-max",
        ),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x40, "exceeded alias receives PUBACK");
    assert_eq!(&body[..2], &[0x00, 2]);
    assert_eq!(
        body[2], 0x94,
        "PUBACK reason code is Topic Alias Invalid (0x94)"
    );

    // Over-limit alias triggers DISCONNECT 0x94 and closes the socket.
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0xE0, "server sends DISCONNECT on invalid alias");
    assert_eq!(body[0], 0x94, "DISCONNECT reason code is 0x94");
    kernel.finish().await;
}

/// An incoming publish carrying an empty topic and an unmapped topic alias is rejected with Topic Alias Invalid.
/// When the broker does not reject an unmapped alias with 0x94,
/// `assert_eq!(body[2], 0x94, "PUBACK reason code is Topic Alias Invalid (0x94)")` is the assertion that fails.
#[tokio::test]
async fn ta_006_publish_process_alias_inexistent_alias() {
    let kernel = spawn_kernel("ta-006-alias-inex", None).await;
    let mut sock = dial(kernel.mqtt_port).await;
    send(&mut sock, &connect5("ta-006-inex-cli", true, 60, 10)).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x20);
    let (_, reason, _) = parse_connack5(&body);
    assert_eq!(reason, 0);

    // Publishing an empty topic with an unmapped alias returns TOPIC_ALIAS_INVALID.
    send(
        &mut sock,
        &publish5("", 5, 1, false, Some(1), b"unregistered-alias"),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x40, "unmapped alias receives PUBACK");
    assert_eq!(&body[..2], &[0x00, 5]);
    assert_eq!(
        body[2], 0x94,
        "PUBACK reason code is Topic Alias Invalid (0x94)"
    );

    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0xE0, "server sends DISCONNECT on unmapped alias");
    assert_eq!(body[0], 0x94, "DISCONNECT reason code is 0x94");
    kernel.finish().await;
}

/// An incoming QoS 0 publish that exceeds the publisher rate quota is dropped without routing.
/// When the broker routes a QoS 0 publish that exceeds the configured rate quota,
/// `assert!(quiet.is_err(), "second QoS 0 publish exceeding quota is dropped")` is the assertion that fails.
#[tokio::test]
async fn ta_006_quota_qos0() {
    let kernel = spawn_kernel("ta-006-quota-q0", None).await;
    let user = "quota-q0-user";
    let password = "pw";
    let topic = "ta06/quota/qos0";

    let http = reqwest::Client::new();
    let res = http
        .post(format!(
            "http://127.0.0.1:{}/api/v1/auth/users",
            kernel.api_port
        ))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({
            "username": user,
            "password": password,
            "quotas": {
                "max_publish_rate": 1,
                "max_publish_burst": 1
            }
        }))
        .send()
        .await
        .expect("create user with rate quota");
    assert_eq!(res.status(), 201, "user created with quotas");

    let mut sub = dial(kernel.mqtt_port).await;
    send(
        &mut sub,
        &connect5_auth("ta-006-q0-sub", user, password.as_bytes(), 60),
    )
    .await;
    let (head, body) = recv(&mut sub).await;
    assert_eq!(head & 0xF0, 0x20);
    assert_eq!(parse_connack5(&body).1, 0);
    send(&mut sub, &subscribe_v5(1, &[(topic, 0)], 0)).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90);

    let mut pub_sock = dial(kernel.mqtt_port).await;
    send(
        &mut pub_sock,
        &connect5_auth("ta-006-q0-pub", user, password.as_bytes(), 60),
    )
    .await;
    let (head, body) = recv(&mut pub_sock).await;
    assert_eq!(head & 0xF0, 0x20);
    assert_eq!(parse_connack5(&body).1, 0);

    // First QoS 0 publish routes to subscriber.
    send(
        &mut pub_sock,
        &publish5(topic, 0, 0, false, None, b"q0-msg-first"),
    )
    .await;
    let (head, body) = recv(&mut sub).await;
    assert_eq!(head & 0xF0, 0x30);
    let got = parse_publish5(head, &body);
    assert_eq!(got.payload, b"q0-msg-first");

    // Second immediate QoS 0 publish exceeds rate quota and is dropped.
    send(
        &mut pub_sock,
        &publish5(topic, 0, 0, false, None, b"q0-msg-second"),
    )
    .await;
    let quiet = tokio::time::timeout(QUIET, read_mqtt_packet(&mut sub)).await;
    assert!(
        quiet.is_err(),
        "second QoS 0 publish exceeding quota is dropped"
    );
    kernel.finish().await;
}

/// An incoming QoS 1 publish that exceeds the publisher rate quota receives PUBACK with reason code 0x97.
/// When the broker does not return reason code 0x97 for quota overage,
/// `assert_eq!(body[2], 0x97, "second PUBACK reason code is Quota exceeded (0x97)")` is the assertion that fails.
#[tokio::test]
async fn ta_006_quota_qos1() {
    let kernel = spawn_kernel("ta-006-quota-q1", None).await;
    let user = "quota-q1-user";
    let password = "pw";
    let topic = "ta06/quota/qos1";

    let http = reqwest::Client::new();
    let res = http
        .post(format!(
            "http://127.0.0.1:{}/api/v1/auth/users",
            kernel.api_port
        ))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({
            "username": user,
            "password": password,
            "quotas": {
                "max_publish_rate": 1,
                "max_publish_burst": 1
            }
        }))
        .send()
        .await
        .expect("create user with rate quota");
    assert_eq!(res.status(), 201, "user created with quotas");

    let mut sock = dial(kernel.mqtt_port).await;
    send(
        &mut sock,
        &connect5_auth("ta-006-q1-pub", user, password.as_bytes(), 60),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x20);
    assert_eq!(parse_connack5(&body).1, 0);

    // First QoS 1 publish within quota succeeds.
    send(
        &mut sock,
        &publish5(topic, 1, 1, false, None, b"q1-msg-first"),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x40, "first publish receives PUBACK");
    assert_eq!(&body[..2], &[0x00, 1]);
    assert_eq!(
        body.get(2).copied().unwrap_or(0),
        0x00,
        "first PUBACK reason code is 0x00 Success"
    );

    // Second immediate QoS 1 publish exceeds quota and receives 0x97 Quota Exceeded.
    send(
        &mut sock,
        &publish5(topic, 2, 1, false, None, b"q1-msg-second"),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x40, "second publish receives PUBACK");
    assert_eq!(&body[..2], &[0x00, 2]);
    assert_eq!(
        body[2], 0x97,
        "second PUBACK reason code is Quota exceeded (0x97)"
    );
    kernel.finish().await;
}

/// An incoming QoS 2 publish that exceeds the publisher rate quota receives PUBREC with reason code 0x97.
/// When the broker does not return reason code 0x97 for quota overage,
/// `assert_eq!(body[2], 0x97, "second PUBREC reason code is Quota exceeded (0x97)")` is the assertion that fails.
#[tokio::test]
async fn ta_006_quota_qos2() {
    let kernel = spawn_kernel("ta-006-quota-q2", None).await;
    let user = "quota-q2-user";
    let password = "pw";
    let topic = "ta06/quota/qos2";

    let http = reqwest::Client::new();
    let res = http
        .post(format!(
            "http://127.0.0.1:{}/api/v1/auth/users",
            kernel.api_port
        ))
        .bearer_auth(API_KEY)
        .json(&serde_json::json!({
            "username": user,
            "password": password,
            "quotas": {
                "max_publish_rate": 1,
                "max_publish_burst": 1
            }
        }))
        .send()
        .await
        .expect("create user with rate quota");
    assert_eq!(res.status(), 201, "user created with quotas");

    let mut sock = dial(kernel.mqtt_port).await;
    send(
        &mut sock,
        &connect5_auth("ta-006-q2-pub", user, password.as_bytes(), 60),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x20);
    assert_eq!(parse_connack5(&body).1, 0);

    // First QoS 2 publish within quota receives PUBREC 0x00 Success.
    send(
        &mut sock,
        &publish5(topic, 1, 2, false, None, b"q2-msg-first"),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x50, "first publish receives PUBREC");
    assert_eq!(&body[..2], &[0x00, 1]);
    assert_eq!(
        body.get(2).copied().unwrap_or(0),
        0x00,
        "first PUBREC reason code is 0x00 Success"
    );
    send(&mut sock, &ack(0x62, 1)).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x70);
    assert_eq!(&body[..2], &[0x00, 1]);

    // Second immediate QoS 2 publish exceeds quota and receives PUBREC 0x97 Quota Exceeded.
    send(
        &mut sock,
        &publish5(topic, 2, 2, false, None, b"q2-msg-second"),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x50, "second publish receives PUBREC");
    assert_eq!(&body[..2], &[0x00, 2]);
    assert_eq!(
        body[2], 0x97,
        "second PUBREC reason code is Quota exceeded (0x97)"
    );
    kernel.finish().await;
}

/// A broker booted with topic alias maximum zero advertises zero in CONNACK and rejects topic aliases.
/// When the broker advertises non-zero topic alias maximum after being configured with zero,
/// `assert_eq!(server_max, 0, "server advertises topic alias maximum 0")` is the assertion that fails.
#[tokio::test]
async fn ta_006_topic_aliases_zero_limits() {
    let kernel = boot_kernel(
        "ta-006-alias-0",
        None,
        "[session]\ntopic_alias_maximum = 0\n",
    )
    .await;
    let mut sock = dial(kernel.mqtt_port).await;
    send(&mut sock, &connect5("ta-006-zero-lim-cli", true, 60, 0)).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x20);
    let (_, reason, server_max) = parse_connack5(&body);
    assert_eq!(reason, 0);
    assert_eq!(server_max, 0, "server advertises topic alias maximum 0");

    // Publishing with any alias is rejected with 0x94 Topic Alias Invalid.
    send(
        &mut sock,
        &publish5("ta06/alias/zero", 1, 1, false, Some(1), b"payload"),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x40, "publish receives PUBACK");
    assert_eq!(&body[..2], &[0x00, 1]);
    assert_eq!(
        body[2], 0x94,
        "PUBACK reason code is Topic Alias Invalid (0x94)"
    );
    kernel.finish().await;
}

/// A client subscribes at QoS 1 and QoS 2, publishes three QoS 2 messages, and receives all three.
/// When the client does not receive all published QoS 2 deliveries,
/// `assert_eq!(received_count, 3, "client receives all 3 published QoS 2 messages")` is the assertion that fails.
#[tokio::test]
async fn ta_006_basic_subscribe_and_publish_qos2() {
    let kernel = spawn_kernel("ta-006-basic", None).await;
    let topic = "ta06/basic/topic_a";
    let mut sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sock, "ta-006-basic-cli", true, 60).await);

    // Subscribe QoS 1 then QoS 2 to the same topic.
    send(&mut sock, &subscribe(1, &[(topic, 1)])).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x90, "SUBSCRIBE receives SUBACK");
    assert_eq!(&body[..2], &[0x00, 1]);

    send(&mut sock, &subscribe(2, &[(topic, 2)])).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x90, "SUBSCRIBE QoS 2 receives SUBACK");
    assert_eq!(&body[..2], &[0x00, 2]);

    let mut received_count = 0;
    for i in 1..=3u16 {
        send(
            &mut sock,
            &publish311(topic, i, 2, false, format!("qos 2 #{i}").as_bytes()),
        )
        .await;
        let (head, body) = recv(&mut sock).await;
        assert_eq!(head, 0x50, "publisher receives PUBREC");
        assert_eq!(&body[..2], &i.to_be_bytes());

        send(&mut sock, &ack(0x62, i)).await;
        let mut got_comp = false;
        let mut got_pub = false;
        while !got_comp || !got_pub {
            let (head, body) = recv(&mut sock).await;
            if head == 0x70 {
                assert_eq!(&body[..2], &i.to_be_bytes());
                got_comp = true;
            } else if head & 0xF0 == 0x30 {
                let live = parse_publish(head, &body);
                assert_eq!(live.topic, topic);
                assert_eq!(live.payload, format!("qos 2 #{i}").as_bytes());
                received_count += 1;
                got_pub = true;
                send(&mut sock, &ack(0x50, live.packet_id)).await;
                let (rel_head, rel_body) = recv(&mut sock).await;
                assert_eq!(rel_head, 0x62, "broker sends PUBREL for delivery");
                assert_eq!(&rel_body[..2], &live.packet_id.to_be_bytes());
                send(&mut sock, &ack(0x70, live.packet_id)).await;
            }
        }
    }
    assert_eq!(
        received_count, 3,
        "client receives all 3 published QoS 2 messages"
    );
    send(&mut sock, &[0xE0, 0x00]).await;
    kernel.finish().await;
}

/// An MQTT 5 client connecting with Receive-Maximum publishes and receives three QoS 2 messages.
/// When the client does not receive all published QoS 2 deliveries under MQTT 5,
/// `assert_eq!(received_count, 3, "MQTT 5 client receives all 3 published QoS 2 messages")` is the assertion that fails.
#[tokio::test]
async fn ta_006_basic_with_props_v5() {
    let kernel = spawn_kernel("ta-006-basic-v5", None).await;
    let topic = "ta06/basic/topic_v5";
    let mut sock = dial(kernel.mqtt_port).await;
    // Connect with Receive-Maximum 4 (Property 33: [33, 0, 4]).
    let props = [33u8, 0, 4];
    send(
        &mut sock,
        &connect5_with_props("ta-006-basic-v5-cli", true, 60, &props),
    )
    .await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head & 0xF0, 0x20);
    let (_, reason, _) = parse_connack5(&body);
    assert_eq!(reason, 0);

    send(&mut sock, &subscribe_v5(1, &[(topic, 1)], 0)).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x90, "SUBSCRIBE receives SUBACK");
    assert_eq!(&body[..2], &[0x00, 1]);

    send(&mut sock, &subscribe_v5(2, &[(topic, 2)], 0)).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x90, "SUBSCRIBE QoS 2 receives SUBACK");
    assert_eq!(&body[..2], &[0x00, 2]);

    let mut received_count = 0;
    for i in 1..=3u16 {
        send(
            &mut sock,
            &publish5(
                topic,
                i,
                2,
                false,
                None,
                format!("qos 2 v5 #{i}").as_bytes(),
            ),
        )
        .await;
        let (head, body) = recv(&mut sock).await;
        assert_eq!(head, 0x50, "publisher receives PUBREC");
        assert_eq!(&body[..2], &i.to_be_bytes());

        send(&mut sock, &ack(0x62, i)).await;
        let mut got_comp = false;
        let mut got_pub = false;
        while !got_comp || !got_pub {
            let (head, body) = recv(&mut sock).await;
            if head == 0x70 {
                assert_eq!(&body[..2], &i.to_be_bytes());
                got_comp = true;
            } else if head & 0xF0 == 0x30 {
                let live = parse_publish5(head, &body);
                assert_eq!(live.topic, topic);
                assert_eq!(live.payload, format!("qos 2 v5 #{i}").as_bytes());
                received_count += 1;
                got_pub = true;
                send(&mut sock, &ack(0x50, live.packet_id)).await;
                let (rel_head, rel_body) = recv(&mut sock).await;
                assert_eq!(rel_head, 0x62, "broker sends PUBREL for delivery");
                assert_eq!(&rel_body[..2], &live.packet_id.to_be_bytes());
                send(&mut sock, &ack(0x70, live.packet_id)).await;
            }
        }
    }
    assert_eq!(
        received_count, 3,
        "MQTT 5 client receives all 3 published QoS 2 messages"
    );
    send(&mut sock, &[0xE0, 0x00]).await;
    kernel.finish().await;
}
