//! Conformance tests for core MQTT packet handling and channel operations (part 8).
//!
//! Proves that the kernel listener handles large publishes, offline message queueing,
//! raised packet limits, pipelined packet batches, incomplete QoS 2 frame recovery,
//! and overlapping subscription routing over real TCP sockets.

use super::support::*;
use std::time::Duration;

/// Standard test keepalive in seconds.
/// Sockets stay open during test execution.
const KEEP_ALIVE_SECS: u16 = 60;

/// Payload size for large publish after connect in bytes.
/// Matches the large publish payload size in test scenarios.
const LARGE_PAYLOAD_BYTES: usize = 256 * 1024;

/// Payload size exceeding connect packet limit in bytes.
/// Exceeds the 64 KiB connect packet bound.
const OVER_CONNECT_LIMIT_BYTES: usize = 128 * 1024;

/// Payload size for pipelined large publish in bytes.
/// Sits within the 64 KiB connect packet bound.
const PIPELINED_PAYLOAD_BYTES: usize = 32 * 1024;

/// Packet identifier for QoS 1 test operations.
const PACKET_ID_QOS1: u16 = 1;

/// Second packet identifier for test operations.
const PACKET_ID_SECOND: u16 = 2;

/// Pause duration before durable reconnect in milliseconds.
/// Allows the kernel to register the disconnect event.
const DETACH_PAUSE: Duration = Duration::from_millis(200);

/// A connected client publishes a 256 KiB QoS 1 message and receives PUBACK and echo delivery.
/// When the kernel fails to accept or deliver a large publish after connect,
/// `assert!(got_puback, "large PUBLISH after connect receives PUBACK")` is the assertion that fails.
#[tokio::test]
async fn ta_008_large_publish_after_connect() {
    let kernel = spawn_kernel("ta-008-large-pub", None).await;
    let topic = "ta008/large/pub";
    let mut sock = dial(kernel.mqtt_port).await;

    assert!(!connect_client(&mut sock, "ta008-large-pub-client", true, KEEP_ALIVE_SECS).await);
    send(&mut sock, &subscribe(PACKET_ID_QOS1, &[(topic, 1)])).await;
    let (head, _) = recv(&mut sock).await;
    assert_eq!(head, 0x90);

    let payload = vec![b'p'; LARGE_PAYLOAD_BYTES];
    send(
        &mut sock,
        &publish311(topic, PACKET_ID_SECOND, 1, false, &payload),
    )
    .await;

    let mut got_puback = false;
    let mut got_publish = false;
    for _ in 0..2 {
        let (head, body) = recv(&mut sock).await;
        if head == 0x40 {
            assert_eq!(body[..2], PACKET_ID_SECOND.to_be_bytes());
            got_puback = true;
        } else if head & 0xF0 == 0x30 {
            let pubmsg = parse_publish(head, &body);
            assert_eq!(pubmsg.topic, topic);
            assert_eq!(pubmsg.payload, payload);
            send(&mut sock, &ack(0x40, pubmsg.packet_id)).await;
            got_publish = true;
        }
    }
    assert!(got_puback, "large PUBLISH after connect receives PUBACK");
    assert!(got_publish, "large PUBLISH after connect receives delivery");

    kernel.finish().await;
}

/// A disconnected durable session queues published messages and replays them on reconnect.
/// When the broker fails to queue or replay offline messages on durable reconnect,
/// `assert_eq!(replayed.len(), 3, "durable subscriber receives all offline messages on reconnect")` is the assertion that fails.
#[tokio::test]
async fn ta_008_offline_message_queueing() {
    let kernel = spawn_kernel("ta-008-offline-queue", None).await;
    let client_id = "ta008-offline-sub";

    let mut sub = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub, client_id, false, KEEP_ALIVE_SECS).await);
    send(
        &mut sub,
        &subscribe(PACKET_ID_QOS1, &[("ta008/offline/+", 2)]),
    )
    .await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90);

    send(&mut sub, &[0xE0, 0x00]).await;
    let closed = tokio::time::timeout(STEP, read_mqtt_packet(&mut sub)).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "socket closes after DISCONNECT"
    );
    drop(sub);

    tokio::time::sleep(DETACH_PAUSE).await;

    let mut pub_sock = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut pub_sock, "ta008-offline-pub", true, KEEP_ALIVE_SECS).await);

    send(
        &mut pub_sock,
        &publish311("ta008/offline/0", 0, 0, false, b"offline-qos0"),
    )
    .await;

    send(
        &mut pub_sock,
        &publish311("ta008/offline/1", 10, 1, false, b"offline-qos1"),
    )
    .await;
    let (head, body) = recv(&mut pub_sock).await;
    assert_eq!(head, 0x40);
    assert_eq!(body[..2], 10u16.to_be_bytes());

    send(
        &mut pub_sock,
        &publish311("ta008/offline/2", 20, 2, false, b"offline-qos2"),
    )
    .await;
    let (head, body) = recv(&mut pub_sock).await;
    assert_eq!(head, 0x50);
    assert_eq!(body[..2], 20u16.to_be_bytes());
    send(&mut pub_sock, &ack(0x62, 20)).await;
    let (head, body) = recv(&mut pub_sock).await;
    assert_eq!(head, 0x70);
    assert_eq!(body[..2], 20u16.to_be_bytes());

    send(&mut pub_sock, &[0xE0, 0x00]).await;
    drop(pub_sock);

    let mut resume = dial(kernel.mqtt_port).await;
    send(&mut resume, &connect311(client_id, false, KEEP_ALIVE_SECS)).await;
    let (head, body) = recv(&mut resume).await;
    assert_eq!(head, 0x20);
    let (present, code) = parse_connack311(&body);
    assert_eq!(code, 0);
    assert!(present, "resumed session reports session present true");

    let mut replayed = Vec::new();
    for _ in 0..3 {
        let (head, body) = recv(&mut resume).await;
        assert_eq!(head & 0xF0, 0x30);
        let msg = parse_publish(head, &body);
        if msg.qos == 1 {
            send(&mut resume, &ack(0x40, msg.packet_id)).await;
        } else if msg.qos == 2 {
            send(&mut resume, &ack(0x50, msg.packet_id)).await;
            let (h, b) = recv(&mut resume).await;
            assert_eq!(h, 0x62);
            send(&mut resume, &ack(0x70, u16::from_be_bytes([b[0], b[1]]))).await;
        }
        replayed.push(msg.payload);
    }
    assert_eq!(
        replayed.len(),
        3,
        "durable subscriber receives all offline messages on reconnect"
    );
    assert!(replayed.contains(&b"offline-qos0".to_vec()));
    assert!(replayed.contains(&b"offline-qos1".to_vec()));
    assert!(replayed.contains(&b"offline-qos2".to_vec()));

    kernel.finish().await;
}

/// Once connected the packet size limit is raised to the general maximum packet size.
/// When the broker does not raise the packet size limit after connect,
/// `assert!(got_puback, "PUBLISH larger than connect limit receives PUBACK")` is the assertion that fails.
#[tokio::test]
async fn ta_008_packet_size_raised_after_connect() {
    let kernel = spawn_kernel("ta-008-pkt-raised", None).await;
    let topic = "ta008/packet/raised";
    let mut sock = dial(kernel.mqtt_port).await;

    assert!(!connect_client(&mut sock, "ta008-pkt-raised-client", true, KEEP_ALIVE_SECS).await);
    send(&mut sock, &subscribe(PACKET_ID_QOS1, &[(topic, 1)])).await;
    let (head, _) = recv(&mut sock).await;
    assert_eq!(head, 0x90);

    let payload = vec![b'r'; OVER_CONNECT_LIMIT_BYTES];
    send(
        &mut sock,
        &publish311(topic, PACKET_ID_SECOND, 1, false, &payload),
    )
    .await;

    let mut got_puback = false;
    let mut got_publish = false;
    for _ in 0..2 {
        let (head, body) = recv(&mut sock).await;
        if head == 0x40 {
            assert_eq!(body[..2], PACKET_ID_SECOND.to_be_bytes());
            got_puback = true;
        } else if head & 0xF0 == 0x30 {
            let pubmsg = parse_publish(head, &body);
            assert_eq!(pubmsg.topic, topic);
            assert_eq!(pubmsg.payload, payload);
            send(&mut sock, &ack(0x40, pubmsg.packet_id)).await;
            got_publish = true;
        }
    }
    assert!(
        got_puback,
        "PUBLISH larger than connect limit receives PUBACK"
    );
    assert!(
        got_publish,
        "PUBLISH larger than connect limit receives delivery"
    );

    kernel.finish().await;
}

/// A client may pipeline CONNECT, SUBSCRIBE and a 32 KiB PUBLISH in a single TCP write.
/// When the broker fails to process pipelined packets,
/// `assert!(has_connack && has_suback && has_puback && has_publish, "pipelined packets receive all responses")` is the assertion that fails.
#[tokio::test]
async fn ta_008_pipelined_large_publish() {
    let kernel = spawn_kernel("ta-008-pipe-large", None).await;
    let topic = "ta008/pipelined/large";
    let client_id = "ta008-pipe-large-client";
    let mut sock = dial(kernel.mqtt_port).await;

    let payload = vec![b'x'; PIPELINED_PAYLOAD_BYTES];
    let mut batch = connect311(client_id, true, KEEP_ALIVE_SECS);
    batch.extend_from_slice(&subscribe(PACKET_ID_QOS1, &[(topic, 1)]));
    batch.extend_from_slice(&publish311(topic, PACKET_ID_SECOND, 1, false, &payload));
    send(&mut sock, &batch).await;

    let mut has_connack = false;
    let mut has_suback = false;
    let mut has_puback = false;
    let mut has_publish = false;

    for _ in 0..4 {
        let (head, body) = recv(&mut sock).await;
        match head & 0xF0 {
            0x20 => {
                let (_, code) = parse_connack311(&body);
                assert_eq!(code, 0);
                has_connack = true;
            }
            0x90 => {
                assert_eq!(body[..2], PACKET_ID_QOS1.to_be_bytes());
                has_suback = true;
            }
            0x40 => {
                assert_eq!(body[..2], PACKET_ID_SECOND.to_be_bytes());
                has_puback = true;
            }
            0x30 => {
                let msg = parse_publish(head, &body);
                assert_eq!(msg.topic, topic);
                assert_eq!(msg.payload, payload);
                send(&mut sock, &ack(0x40, msg.packet_id)).await;
                has_publish = true;
            }
            _ => panic!("unexpected packet header {head:#x}"),
        }
    }
    assert!(
        has_connack && has_suback && has_puback && has_publish,
        "pipelined packets receive all responses"
    );

    kernel.finish().await;
}

/// A pipelined PUBLISH larger than the connect limit is processed under the raised packet limit.
/// When the broker fails to raise the packet limit for pipelined publishes,
/// `assert!(has_connack && has_suback && has_puback && has_publish, "pipelined oversized publish receives all responses")` is the assertion that fails.
#[tokio::test]
async fn ta_008_pipelined_publish_larger_than_connect_limit() {
    let kernel = spawn_kernel("ta-008-pipe-over", None).await;
    let topic = "ta008/pipelined/over";
    let client_id = "ta008-pipe-over-client";
    let mut sock = dial(kernel.mqtt_port).await;

    let payload = vec![b'y'; OVER_CONNECT_LIMIT_BYTES];
    let mut batch = connect311(client_id, true, KEEP_ALIVE_SECS);
    batch.extend_from_slice(&subscribe(PACKET_ID_QOS1, &[(topic, 1)]));
    batch.extend_from_slice(&publish311(topic, PACKET_ID_SECOND, 1, false, &payload));
    send(&mut sock, &batch).await;

    let mut has_connack = false;
    let mut has_suback = false;
    let mut has_puback = false;
    let mut has_publish = false;

    for _ in 0..4 {
        let (head, body) = recv(&mut sock).await;
        match head & 0xF0 {
            0x20 => {
                let (_, code) = parse_connack311(&body);
                assert_eq!(code, 0);
                has_connack = true;
            }
            0x90 => {
                assert_eq!(body[..2], PACKET_ID_QOS1.to_be_bytes());
                has_suback = true;
            }
            0x40 => {
                assert_eq!(body[..2], PACKET_ID_SECOND.to_be_bytes());
                has_puback = true;
            }
            0x30 => {
                let msg = parse_publish(head, &body);
                assert_eq!(msg.topic, topic);
                assert_eq!(msg.payload, payload);
                send(&mut sock, &ack(0x40, msg.packet_id)).await;
                has_publish = true;
            }
            _ => panic!("unexpected packet header {head:#x}"),
        }
    }
    assert!(
        has_connack && has_suback && has_puback && has_publish,
        "pipelined oversized publish receives all responses"
    );

    kernel.finish().await;
}

/// A PUBLISH larger than the connect packet size limit round-trips for a connected client.
/// When the broker rejects a publish larger than the connect limit from a connected client,
/// `assert!(got_puback, "publish larger than connect limit receives PUBACK")` is the assertion that fails.
#[tokio::test]
async fn ta_008_publish_larger_than_connect_limit() {
    let kernel = spawn_kernel("ta-008-pub-over", None).await;
    let topic = "ta008/pub/over";
    let mut sock = dial(kernel.mqtt_port).await;

    assert!(!connect_client(&mut sock, "ta008-pub-over-client", true, KEEP_ALIVE_SECS).await);
    send(&mut sock, &subscribe(PACKET_ID_QOS1, &[(topic, 1)])).await;
    let (head, _) = recv(&mut sock).await;
    assert_eq!(head, 0x90);

    let payload = vec![b'z'; OVER_CONNECT_LIMIT_BYTES];
    send(
        &mut sock,
        &publish311(topic, PACKET_ID_SECOND, 1, false, &payload),
    )
    .await;

    let mut got_puback = false;
    let mut got_publish = false;
    for _ in 0..2 {
        let (head, body) = recv(&mut sock).await;
        if head == 0x40 {
            assert_eq!(body[..2], PACKET_ID_SECOND.to_be_bytes());
            got_puback = true;
        } else if head & 0xF0 == 0x30 {
            let pubmsg = parse_publish(head, &body);
            assert_eq!(pubmsg.topic, topic);
            assert_eq!(pubmsg.payload, payload);
            send(&mut sock, &ack(0x40, pubmsg.packet_id)).await;
            got_publish = true;
        }
    }
    assert!(
        got_puback,
        "publish larger than connect limit receives PUBACK"
    );
    assert!(
        got_publish,
        "publish larger than connect limit receives delivery"
    );

    kernel.finish().await;
}

/// When a socket closes during incomplete QoS 2 publish transmission, resending with DUP succeeds on reconnect.
/// When the subscriber fails to receive the complete QoS 2 publish after reconnect resend,
/// `assert_eq!(pubmsg.payload, payload, "subscriber receives complete QoS 2 publish after reconnect resend")` is the assertion that fails.
#[tokio::test]
async fn ta_008_sock_closed_incomplete_qos2_transmission() {
    let kernel = spawn_kernel("ta-008-q2-incomp", None).await;
    let topic = "ta008/q2/incomplete";
    let pub_id = "ta008-q2-incomp-pub";
    let sub_id = "ta008-q2-incomp-sub";

    let mut sub = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut sub, sub_id, true, KEEP_ALIVE_SECS).await);
    send(&mut sub, &subscribe(PACKET_ID_QOS1, &[(topic, 2)])).await;
    let (head, _) = recv(&mut sub).await;
    assert_eq!(head, 0x90);

    let mut pub_sock1 = dial(kernel.mqtt_port).await;
    assert!(!connect_client(&mut pub_sock1, pub_id, false, KEEP_ALIVE_SECS).await);

    let payload = b"complete-qos2-data";
    let full_publish = publish311(topic, PACKET_ID_QOS1, 2, false, payload);
    send(&mut pub_sock1, &full_publish[..full_publish.len() - 1]).await;
    drop(pub_sock1);

    tokio::time::sleep(DETACH_PAUSE).await;

    let mut pub_sock2 = dial(kernel.mqtt_port).await;
    send(&mut pub_sock2, &connect311(pub_id, false, KEEP_ALIVE_SECS)).await;
    let (head, body) = recv(&mut pub_sock2).await;
    assert_eq!(head, 0x20);
    let (present, code) = parse_connack311(&body);
    assert_eq!(code, 0);
    assert!(present, "publisher reconnect reports session present true");

    let mut dup_publish = full_publish.clone();
    dup_publish[0] |= 0x08;
    send(&mut pub_sock2, &dup_publish).await;

    let (head, body) = recv(&mut pub_sock2).await;
    assert_eq!(head, 0x50);
    assert_eq!(body[..2], PACKET_ID_QOS1.to_be_bytes());

    send(&mut pub_sock2, &ack(0x62, PACKET_ID_QOS1)).await;
    let (head, body) = recv(&mut pub_sock2).await;
    assert_eq!(head, 0x70);
    assert_eq!(body[..2], PACKET_ID_QOS1.to_be_bytes());

    let (head, body) = recv(&mut sub).await;
    let pubmsg = parse_publish(head, &body);
    assert_eq!(pubmsg.qos, 2);
    assert!(!pubmsg.dup, "subscriber delivery has dup false");
    assert_eq!(
        pubmsg.payload, payload,
        "subscriber receives complete QoS 2 publish after reconnect resend"
    );

    send(&mut sub, &ack(0x50, pubmsg.packet_id)).await;
    let (head, body) = recv(&mut sub).await;
    assert_eq!(head, 0x62);
    send(&mut sub, &ack(0x70, u16::from_be_bytes([body[0], body[1]]))).await;

    kernel.finish().await;
}

/// Overlapping subscriptions receive matching publish delivery respecting granted QoS.
/// When overlapping subscriptions receive fewer than 2 deliveries,
/// `assert_eq!(count, 2, "overlapping subscriptions receive 2 deliveries")` is the assertion that fails.
#[tokio::test]
async fn ta_008_overlapping_subscriptions() {
    let kernel = spawn_kernel("ta-008-overlap", None).await;
    let mut sub_sock = dial(kernel.mqtt_port).await;
    let sub_id = "ta008-overlap-sub";

    assert!(!connect_client(&mut sub_sock, sub_id, true, KEEP_ALIVE_SECS).await);

    let sub_packet = subscribe(
        PACKET_ID_QOS1,
        &[("ta008/overlap/#", 2), ("ta008/overlap/+", 1)],
    );
    send(&mut sub_sock, &sub_packet).await;
    let (head, _) = recv(&mut sub_sock).await;
    assert_eq!(head, 0x90);

    let mut pub_sock = dial(kernel.mqtt_port).await;
    let pub_id = "ta008-overlap-pub";
    assert!(!connect_client(&mut pub_sock, pub_id, true, KEEP_ALIVE_SECS).await);

    let payload = b"overlap-msg";
    send(
        &mut pub_sock,
        &publish311("ta008/overlap/target", PACKET_ID_SECOND, 2, false, payload),
    )
    .await;

    let (head, body) = recv(&mut pub_sock).await;
    assert_eq!(head, 0x50);
    assert_eq!(body[..2], PACKET_ID_SECOND.to_be_bytes());
    send(&mut pub_sock, &ack(0x62, PACKET_ID_SECOND)).await;
    let (head, body) = recv(&mut pub_sock).await;
    assert_eq!(head, 0x70);
    assert_eq!(body[..2], PACKET_ID_SECOND.to_be_bytes());

    let mut count = 0;
    let mut qos2_pending_rel = false;
    let mut qos2_packet_id = 0;

    while count < 2 || qos2_pending_rel {
        let (head, body) = recv(&mut sub_sock).await;
        if head & 0xF0 == 0x30 {
            let msg = parse_publish(head, &body);
            assert_eq!(msg.topic, "ta008/overlap/target");
            assert_eq!(msg.payload, payload);
            count += 1;
            if msg.qos == 1 {
                send(&mut sub_sock, &ack(0x40, msg.packet_id)).await;
            } else if msg.qos == 2 {
                send(&mut sub_sock, &ack(0x50, msg.packet_id)).await;
                qos2_pending_rel = true;
                qos2_packet_id = msg.packet_id;
            }
        } else if head == 0x62 {
            let rel_id = u16::from_be_bytes([body[0], body[1]]);
            assert_eq!(rel_id, qos2_packet_id);
            send(&mut sub_sock, &ack(0x70, rel_id)).await;
            qos2_pending_rel = false;
        } else {
            panic!("unexpected packet header {head:#x}");
        }
    }

    assert_eq!(count, 2, "overlapping subscriptions receive 2 deliveries");

    kernel.finish().await;
}
