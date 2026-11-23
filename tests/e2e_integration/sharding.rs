use super::*;

/// Topic aliases cross the broker on real MQTT 5 sockets.
///
/// A 3.1.1 subscriber and a version-5 publisher use the kernel listener.
/// The publisher registers alias 3 with a full topic, then publishes by
/// reference with an empty topic. Both downlinks carry the full topic.
/// A version-5 subscriber then proves the server-to-client direction:
/// the kernel assigns an outbound alias to a plain publish.
#[tokio::test]
async fn sharded_topic_alias_negotiation_and_delivery() {
    let kernel = spawn_kernel("").await;

    let mut sub = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(&mut sub, &sy02_connect311("alias-e2e-2", true, 60, None)).await;
    let (head, body) = recv_mqtt(&mut sub).await;
    assert_eq!(head, 0x20, "kernel answers with CONNACK");
    assert_eq!(sy02_parse_connack311(&body), (false, 0));
    send_mqtt(&mut sub, &sy02_subscribe(1, &[("alias/e2e", 0)])).await;
    let (head, body) = recv_mqtt(&mut sub).await;
    assert_eq!(head & 0xF0, 0x90, "kernel answers with SUBACK");
    assert_eq!(sy02_parse_suback(&body), (1, vec![0u8]));

    let mut publisher = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(
        &mut publisher,
        &sy02_connect5("alias-e2e-1", true, 60, None, None, 10),
    )
    .await;
    let (head, body) = recv_mqtt(&mut publisher).await;
    assert_eq!(head & 0xF0, 0x20, "publisher gets CONNACK");
    let (_, reason, alias_max) = sy02_parse_connack5(&body);
    assert_eq!(reason, 0, "alias-carrying CONNECT is accepted");
    assert_eq!(alias_max, 10, "CONNACK carries the alias maximum");

    // First publish carries topic plus alias 3. It registers the mapping
    // and delivers the full topic.
    send_mqtt(
        &mut publisher,
        &sy02_publish5("alias/e2e", 0, 0, false, Some(3), b"21.5"),
    )
    .await;
    let (head, body) = recv_mqtt(&mut sub).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let first = sy02_parse_publish_classic(head, &body);
    assert_eq!(first.topic, "alias/e2e");
    assert_eq!(first.payload, b"21.5");

    // Second publish carries the empty topic plus alias 3. The broker
    // resolves the alias and the subscriber still sees the topic.
    send_mqtt(
        &mut publisher,
        &sy02_publish5("", 0, 0, false, Some(3), b"22.5"),
    )
    .await;
    let (head, body) = recv_mqtt(&mut sub).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let second = sy02_parse_publish_classic(head, &body);
    assert_eq!(second.topic, "alias/e2e");
    assert_eq!(second.payload, b"22.5");

    // Server-to-client direction: a version-5 subscriber gets an
    // outbound alias on a plain publish.
    let mut v5sub = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(
        &mut v5sub,
        &sy02_connect5("alias-e2e-3", true, 60, None, None, 10),
    )
    .await;
    let (head, body) = recv_mqtt(&mut v5sub).await;
    assert_eq!(head & 0xF0, 0x20, "version-5 subscriber gets CONNACK");
    let (_, reason, alias_max) = sy02_parse_connack5(&body);
    assert_eq!(reason, 0);
    assert_eq!(alias_max, 10, "CONNACK carries the alias maximum");
    send_mqtt(&mut v5sub, &sy02_subscribe_v5(1, &[("alias/e2e", 0)], 0)).await;
    let (head, _) = recv_mqtt(&mut v5sub).await;
    assert_eq!(head & 0xF0, 0x90, "kernel answers with SUBACK");
    send_mqtt(
        &mut publisher,
        &sy02_publish5("alias/e2e", 0, 0, false, None, b"23.5"),
    )
    .await;
    let (head, body) = recv_mqtt(&mut v5sub).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let third = sy02_parse_publish_alias(head, &body);
    assert_eq!(third.topic, "alias/e2e");
    assert_eq!(third.payload, b"23.5");
    assert_ne!(third.alias, 0, "server assigns an outbound alias");

    kernel.finish().await;
}

/// Concurrent publishers share one kernel without loss or duplication.
///
/// 200 publishers publish one QoS 1 message each through the kernel
/// listener. A durable subscriber receives every payload exactly once.
/// The subscriber then drops, one message publishes while it is gone,
/// and the reconnect replays exactly that message.
#[tokio::test]
async fn sharded_transports_do_not_serialize() {
    use std::collections::HashSet;

    const PUBLISHERS: u32 = 200;

    let kernel = spawn_kernel("").await;

    let mut sub = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(&mut sub, &sy02_connect311("shard-sub", false, 60, None)).await;
    let (head, body) = recv_mqtt(&mut sub).await;
    assert_eq!(head, 0x20, "kernel answers with CONNACK");
    assert_eq!(
        sy02_parse_connack311(&body),
        (false, 0),
        "first durable CONNECT reports session_present=false"
    );
    send_mqtt(&mut sub, &sy02_subscribe(1, &[("shard/topic", 1)])).await;
    let (head, body) = recv_mqtt(&mut sub).await;
    assert_eq!(head, 0x90, "kernel answers with SUBACK");
    assert_eq!(sy02_parse_suback(&body), (1, vec![1]));

    // 200 publishers publish concurrently, one QoS 1 message each.
    let mut tasks = Vec::new();
    for i in 0..PUBLISHERS {
        let port = kernel.mqtt_port;
        tasks.push(tokio::spawn(async move {
            let mut sock = dial_mqtt(port).await;
            let id = format!("shard-pub-{i}");
            send_mqtt(&mut sock, &sy02_connect311(&id, true, 60, None)).await;
            let (head, _) = recv_mqtt(&mut sock).await;
            assert_eq!(head, 0x20, "publisher gets CONNACK");
            let payload = format!("burst-{i}");
            send_mqtt(
                &mut sock,
                &sy02_publish311("shard/topic", 1, 1, false, payload.as_bytes()),
            )
            .await;
            let (head, body) = recv_mqtt(&mut sock).await;
            assert_eq!(head, 0x40, "publisher sees PUBACK");
            assert_eq!(body, vec![0x00, 0x01], "PUBACK mirrors its packet id");
        }));
    }
    for task in tasks {
        tokio::time::timeout(BOOT, task)
            .await
            .expect("publisher joins in time")
            .expect("publisher passes");
    }

    // The subscriber receives every payload exactly once and acks each
    // delivery, so nothing stays inflight at the drop below.
    let mut seen = HashSet::new();
    for _ in 0..PUBLISHERS {
        let (head, body) = recv_mqtt(&mut sub).await;
        assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
        let downlink = sy02_parse_publish_classic(head, &body);
        assert_eq!(downlink.qos, 1, "downlink keeps QoS 1");
        assert!(seen.insert(downlink.payload.clone()), "no duped delivery");
        send_mqtt(&mut sub, &sy02_puback(downlink.packet_id)).await;
    }
    assert_eq!(seen.len(), PUBLISHERS as usize, "every publish arrives");
    for i in 0..PUBLISHERS {
        assert!(
            seen.contains(&format!("burst-{i}").into_bytes()),
            "burst {i} arrives"
        );
    }

    // Ungraceful drop of the subscriber socket. One publish while
    // detached queues instead of routing to the dead socket.
    drop(sub);
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let mut detached = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(
        &mut detached,
        &sy02_connect311("shard-detached-pub", true, 60, None),
    )
    .await;
    let (head, _) = recv_mqtt(&mut detached).await;
    assert_eq!(head, 0x20, "detached publisher gets CONNACK");
    send_mqtt(
        &mut detached,
        &sy02_publish311("shard/topic", 9, 1, false, b"replay-me"),
    )
    .await;
    let (head, body) = recv_mqtt(&mut detached).await;
    assert_eq!(head, 0x40, "detached publisher sees PUBACK");
    assert_eq!(body, vec![0x00, 0x09], "PUBACK mirrors its packet id");

    // Reconnect resumes the durable session and replays the queued
    // message. A dropped first bind that never detached would route the
    // publish into the dead mailbox instead of the queue, and this read
    // would time out.
    let mut re = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(&mut re, &sy02_connect311("shard-sub", false, 60, None)).await;
    let (head, body) = recv_mqtt(&mut re).await;
    assert_eq!(head, 0x20, "kernel answers the reconnect with CONNACK");
    assert_eq!(
        sy02_parse_connack311(&body),
        (true, 0),
        "resumed session reports session_present=true"
    );
    let (head, body) = recv_mqtt(&mut re).await;
    assert_eq!(head & 0xF0, 0x30, "replay is PUBLISH");
    let replay = sy02_parse_publish_classic(head, &body);
    assert_eq!(replay.topic, "shard/topic");
    assert_eq!(replay.payload, b"replay-me");
    let quiet =
        tokio::time::timeout(std::time::Duration::from_secs(2), read_mqtt_packet(&mut re)).await;
    assert!(quiet.is_err(), "exactly one message replays");

    kernel.finish().await;
}
