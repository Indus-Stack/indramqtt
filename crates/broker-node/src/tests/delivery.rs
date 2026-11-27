use super::*;
use crate::link_harness::{handle_connection, handle_inbound_frame};
use tokio::net::TcpListener;

#[cfg(test)]
fn decode_publish_dup(meta: &[u8]) -> bool {
    let topic_len = u16::from_be_bytes([meta[0], meta[1]]) as usize;
    let base = 2 + topic_len;
    meta[base + 4] != 0
}

/// D1-03: an 8 KB payload fanned out to N subscribers must share one
/// buffer instead of copying N times. Fails if any delivery or queued
/// copy allocates its own 8 KB (e.g. via `to_vec` + `Bytes::from` per
/// subscriber); passes when every handle is a refcount clone of one
/// root (`Bytes::ptr_eq`).
#[tokio::test]
async fn large_payload_shares_single_buffer_across_subscribers() {
    let shared_state = test_shared();
    for (conn, client) in [(61u64, "large-a"), (62u64, "large-b"), (63u64, "large-c")] {
        let frame = subscribe_frame(
            conn,
            1,
            encode_subscribe_meta(1, client, &[("big/topic", 0)]),
        );
        let (reply, _) = apply_subscribe(&frame, &shared_state).await;
        reply.expect("subscribe replies");
        let (session, _) = shared_state.sessions.get_or_create(client, true);
        *session.conn_id.write() = Some(conn);
    }
    let big = Bytes::from(vec![0xABu8; 8192]);
    let topic = Topic::new("big/topic").unwrap();
    let deliveries = build_downlink_frames(
        &shared_state.router,
        &shared_state.sessions,
        &shared_state.metrics,
        &topic,
        QoS::AtMostOnce,
        false,
        &big,
        broker_session::tenant::DEFAULT_TENANT_ID,
        None,
    );
    assert_eq!(deliveries.len(), 3, "one delivery per subscriber");
    for (_, frame) in &deliveries {
        assert_eq!(frame.payload.len(), 8192);
    }
    // Every delivery payload must point at the same 8 KB buffer.
    for (_, frame) in deliveries.iter().skip(1) {
        assert!(
            deliveries[0].1.payload.as_ptr() == frame.payload.as_ptr(),
            "D1-03: payload copied per subscriber instead of shared"
        );
    }
    // The shared root itself must also be the same buffer: cloning
    // the ingress once and refcounting N times costs 8 KB, not N*8 KB.
    for (_, frame) in &deliveries {
        assert!(
            big.as_ptr() == frame.payload.as_ptr(),
            "D1-03: delivery detached from the ingress buffer"
        );
    }
}

/// D1-03 companion: detached durable subscribers queue the same 8 KB
/// buffer instead of copying it per queue. Two offline queues holding
/// the same publish must be `ptr_eq`, proving one allocation.
#[test]
fn large_payload_offline_queues_share_single_buffer() {
    let router = Router::new();
    let sessions = SessionManager::new();
    let metrics = Metrics::new();
    let filter = TopicFilter::new("big/topic").unwrap();
    for (client, conn) in [("off-a", 71u64), ("off-b", 72u64)] {
        router.subscribe(&filter, Subscription::new(client, conn, QoS::AtMostOnce));
        let (session, _) = sessions.get_or_create(client, false);
        *session.connected.write() = false;
        *session.conn_id.write() = None;
    }
    let big = Bytes::from(vec![0xCDu8; 8192]);
    let topic = Topic::new("big/topic").unwrap();
    let deliveries = build_downlink_frames(
        &router,
        &sessions,
        &metrics,
        &topic,
        QoS::AtMostOnce,
        false,
        &big,
        broker_session::tenant::DEFAULT_TENANT_ID,
        None,
    );
    assert!(deliveries.is_empty(), "detached queues buffer, not deliver");
    let a = sessions.get("off-a").expect("session a");
    let b = sessions.get("off-b").expect("session b");
    assert_eq!(a.offline_len(), 1);
    assert_eq!(b.offline_len(), 1);
    let qa = a.drain_offline();
    let qb = b.drain_offline();
    assert!(big.as_ptr() == qa[0].payload.as_ptr());
    assert!(
        qa[0].payload.as_ptr() == qb[0].payload.as_ptr(),
        "D1-03: offline queues copied the 8 KB payload per subscriber"
    );
}

#[tokio::test]
async fn publish_qos1_acks_publisher_and_routes() {
    let shared = test_shared();
    let (session, _) = shared.sessions.get_or_create("q1-sub", true);
    *session.conn_id.write() = Some(51);
    let sub = subscribe_frame(51, 1, encode_subscribe_meta(3, "q1-sub", &[("t", 1)]));
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");
    bind_test_publisher(&shared, 52);

    let (meta, payload) = encode_publish_meta("t", 42, 1, false, b"data");
    let (ack, deliveries) = apply_publish(&publish_frame(52, 8, meta, payload), &shared).await;

    let ack = ack.expect("QoS 1 needs PubAck");
    assert_eq!(ack.header.opcode, OpCode::PubAckOut);
    assert_eq!(ack.header.conn_id, 52, "ack mirrors publisher conn");
    assert_eq!(ack.header.sequence_no, 8, "ack mirrors publisher seq");
    assert_eq!(&ack.metadata[..], &[0x00, 0x2A, 0x00]);

    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].0, 51);
    assert_eq!(deliveries[0].1.payload, Bytes::from_static(b"data"));
}

/// A QoS 1 publish whose downlink mailbox is unknown must not be
/// counted as delivered. The publisher still gets its PubAck
/// (receipt), but the frame never reached any edge mailbox, so
/// `delivered`/`publish_sent`/`messages_forwarded` must stay zero.
/// Fails before the fix by counting a delivery to nobody.
#[tokio::test]
async fn qos1_publish_to_unknown_mailbox_is_not_counted_delivered() {
    let shared = test_shared();
    // Subscriber session is live and subscribed, but its edge
    // mailbox was never registered (dead/pruned connection): the
    // router still matches, so a delivery frame is built.
    let bind = bind_frame(51, 1, encode_bind_meta("q1-ghost", true, 60));
    reply_for_frame(&bind, &shared.sessions).expect("bind replies");
    let sub = subscribe_frame(51, 2, encode_subscribe_meta(3, "q1-ghost", &[("t", 1)]));
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");

    // Drive the real ingress path (ack + outcome counting) over a
    // loopback BrokerLink transport.
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let client = FramedTransport::new(client_io);
    let server = FramedTransport::new(server_io);
    let (tx, _rx) = unbounded_channel::<BrokerFrame>();
    let mut bound = Vec::new();

    let (meta, payload) = encode_publish_meta("t", 42, 1, false, b"data");
    bind_test_publisher(&shared, 52);
    handle_inbound_frame(
        publish_frame(52, 8, meta, payload),
        &shared,
        &tx,
        &server,
        &mut bound,
        &Arc::new(tokio::sync::Notify::new()),
    )
    .await
    .expect("publish handling succeeds");

    // The publisher is acked (receipt), even though nothing was delivered.
    let ack = client.recv().await.expect("publisher ack");
    assert_eq!(ack.header.opcode, OpCode::PubAckOut);

    // Nothing reached a live mailbox: the loss must be visible, not
    // counted as a delivery.
    assert_eq!(
        shared.metrics.delivered(),
        0,
        "dropped downlink counted as delivered"
    );
    assert_eq!(shared.metrics.publish_sent(), 0);
    assert_eq!(shared.metrics.messages_forwarded(), 0);
    assert_eq!(
        shared.metrics.qos1_received(),
        1,
        "ingress itself was processed"
    );
}

/// The same dropped downlink must surface as exactly one drop under
/// exactly one existing counter (no double counting): an unknown
/// destination bumps `unknown_conn_dropped` and nothing else.
#[tokio::test]
async fn qos1_publish_to_unknown_mailbox_counts_single_drop_only() {
    let shared = test_shared();
    let bind = bind_frame(71, 1, encode_bind_meta("q1-ghost-2", true, 60));
    reply_for_frame(&bind, &shared.sessions).expect("bind replies");
    let sub = subscribe_frame(71, 2, encode_subscribe_meta(3, "q1-ghost-2", &[("t", 1)]));
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");

    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let client = FramedTransport::new(client_io);
    let server = FramedTransport::new(server_io);
    let (tx, _rx) = unbounded_channel::<BrokerFrame>();
    let mut bound = Vec::new();

    let (meta, payload) = encode_publish_meta("t", 9, 1, false, b"data");
    bind_test_publisher(&shared, 72);
    handle_inbound_frame(
        publish_frame(72, 8, meta, payload),
        &shared,
        &tx,
        &server,
        &mut bound,
        &Arc::new(tokio::sync::Notify::new()),
    )
    .await
    .expect("publish handling succeeds");
    let ack = client.recv().await.expect("publisher ack");
    assert_eq!(ack.header.opcode, OpCode::PubAckOut);

    assert_eq!(shared.metrics.delivered(), 0);
    assert_eq!(
        shared.metrics.unknown_conn_dropped(),
        1,
        "unknown mailbox must count exactly one unknown-conn drop"
    );
    assert_eq!(shared.metrics.dead_mailbox_dropped(), 0);
    assert_eq!(shared.metrics.detached_clean_dropped(), 0);
    assert_eq!(shared.metrics.offline_queue_evicted(), 0);
}

/// A dropped downlink into a dead (receiver-gone) mailbox must bump
/// exactly `dead_mailbox_dropped` and nothing else.
#[tokio::test]
async fn qos1_publish_to_dead_mailbox_counts_single_drop_only() {
    let shared = test_shared();
    let bind = bind_frame(81, 1, encode_bind_meta("q1-ghost-3", true, 60));
    reply_for_frame(&bind, &shared.sessions).expect("bind replies");
    let sub = subscribe_frame(81, 2, encode_subscribe_meta(3, "q1-ghost-3", &[("t", 1)]));
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");
    // Register then drop the receiver: the mailbox is dead at route
    // time, so `route` prunes it and counts a dead-mailbox drop.
    let (tx_dead, rx_dead) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(81, tx_dead);
    drop(rx_dead);

    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let client = FramedTransport::new(client_io);
    let server = FramedTransport::new(server_io);
    let (tx, _rx) = unbounded_channel::<BrokerFrame>();
    let mut bound = Vec::new();

    let (meta, payload) = encode_publish_meta("t", 11, 1, false, b"data");
    bind_test_publisher(&shared, 82);
    handle_inbound_frame(
        publish_frame(82, 8, meta, payload),
        &shared,
        &tx,
        &server,
        &mut bound,
        &Arc::new(tokio::sync::Notify::new()),
    )
    .await
    .expect("publish handling succeeds");
    let ack = client.recv().await.expect("publisher ack");
    assert_eq!(ack.header.opcode, OpCode::PubAckOut);

    assert_eq!(shared.metrics.delivered(), 0);
    assert_eq!(shared.metrics.dead_mailbox_dropped(), 1);
    assert_eq!(shared.metrics.unknown_conn_dropped(), 0);
    assert_eq!(shared.metrics.detached_clean_dropped(), 0);
    assert_eq!(shared.metrics.offline_queue_evicted(), 0);
}

#[test]
fn egress_stage_counters_start_at_zero() {
    let metrics = Metrics::new();
    assert_eq!(metrics.transport_sent(), 0);
    assert_eq!(metrics.transport_send_failed(), 0);
    assert_eq!(metrics.egress_qos0_shed(), 0);
    assert_eq!(metrics.puback_deferred(), 0);
    assert_eq!(metrics.puback_deferred_released(), 0);
    let snap = metrics.snapshot();
    assert_eq!(snap.transport_sent, 0);
    let text = metrics.render_prometheus_metrics();
    assert!(text.contains("indramqtt_transport_sent_total 0\n"));
}

/// T-31: a durable subscriber that disconnects with a QoS 1
/// downlink inflight (sent, never acked) must see that message
/// redelivered on reconnect with DUP set.
#[tokio::test]
async fn qos1_unacked_downlink_is_redelivered_after_reconnect() {
    let shared = test_shared();
    let bind = bind_frame(61, 1, encode_bind_meta("q1-durable", false, 60));
    reply_for_frame(&bind, &shared.sessions).expect("bind replies");
    let sub = subscribe_frame(61, 2, encode_subscribe_meta(5, "q1-durable", &[("t", 1)]));
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");

    // Live mailbox: the downlink is routed (inflight, unacked).
    let (tx61, mut rx61) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(61, tx61);
    bind_test_publisher(&shared, 62);
    let (meta, payload) = encode_publish_meta("t", 7, 1, false, b"redeliver-me");
    let (_, deliveries) = apply_publish(&publish_frame(62, 3, meta, payload), &shared).await;
    assert_eq!(
        deliveries.len(),
        1,
        "downlink must be built for the live subscriber"
    );
    for (conn_id, frame) in deliveries {
        let _ = shared.conns.route(conn_id, frame);
    }
    let inflight = rx61.try_recv().expect("downlink reached the edge mailbox");
    assert_eq!(inflight.payload, Bytes::from_static(b"redeliver-me"));
    assert!(
        !decode_publish_dup(&inflight.metadata),
        "fresh downlink carries DUP=0"
    );
    let (_, first_pid, _, _) = decode_publish_meta_parts(&inflight.metadata);

    // Subscriber disconnects without acking (durable session keeps
    // subscriptions, offline queue and inflight), then reconnects
    // non-clean with a live mailbox.
    let mut unbind_meta = vec![0x00u8, 0x0Au8];
    unbind_meta.extend_from_slice(b"q1-durable");
    let unbind = BrokerFrame::new(
        OpCode::UnbindConnection,
        61,
        4,
        Bytes::from(unbind_meta),
        Bytes::new(),
    )
    .expect("valid unbind frame");
    apply_unbind(&unbind, &shared);
    let rebind = bind_frame(63, 1, encode_bind_meta("q1-durable", false, 60));
    reply_for_frame(&rebind, &shared.sessions).expect("rebind replies");
    let (tx63, mut rx63) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(63, tx63);

    // The unacked message must come back with DUP set.
    let replayed = replay_offline(&rebind, &shared).await;
    assert!(
        replayed >= 1,
        "T-26: unacked QoS 1 downlink was not redelivered after reconnect"
    );
    assert_eq!(replayed, 1, "exactly the one unacked downlink replays");
    let redelivered = rx63.try_recv().expect("redelivery reached the new mailbox");
    assert_eq!(redelivered.payload, Bytes::from_static(b"redeliver-me"));
    assert!(
        decode_publish_dup(&redelivered.metadata),
        "redelivery must carry DUP=1"
    );
    let (_, replay_pid, _, _) = decode_publish_meta_parts(&redelivered.metadata);
    assert_eq!(
        replay_pid, first_pid,
        "redelivery reuses the downlink packet id"
    );
}

#[tokio::test]
async fn qos1_puback_releases_inflight_nothing_replayed() {
    let shared = test_shared();
    let bind = bind_frame(61, 1, encode_bind_meta("q1-acked", false, 60));
    reply_for_frame(&bind, &shared.sessions).expect("bind replies");
    let sub = subscribe_frame(61, 2, encode_subscribe_meta(5, "q1-acked", &[("t", 1)]));
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");

    let (tx61, mut rx61) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(61, tx61);
    bind_test_publisher(&shared, 62);
    let (meta, payload) = encode_publish_meta("t", 7, 1, false, b"acked-me");
    let (_, deliveries) = apply_publish(&publish_frame(62, 3, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1);
    for (conn_id, frame) in deliveries {
        let _ = shared.conns.route(conn_id, frame);
    }
    let downlink = rx61.try_recv().expect("downlink arrived");
    let (_, pid, _, _) = decode_publish_meta_parts(&downlink.metadata);
    assert_ne!(pid, 0);

    // Subscriber acks before disconnecting: the entry is released.
    apply_puback(&puback_frame(61, 9, pid), &shared);
    let session = shared.sessions.get("q1-acked").expect("session known");
    assert_eq!(session.inflight_len(), 0, "PUBACK must release the entry");

    // Reconnect replays nothing.
    let mut unbind_meta = vec![0x00u8, 0x08u8];
    unbind_meta.extend_from_slice(b"q1-acked");
    let unbind = BrokerFrame::new(
        OpCode::UnbindConnection,
        61,
        4,
        Bytes::from(unbind_meta),
        Bytes::new(),
    )
    .expect("valid unbind frame");
    apply_unbind(&unbind, &shared);
    let rebind = bind_frame(63, 1, encode_bind_meta("q1-acked", false, 60));
    reply_for_frame(&rebind, &shared.sessions).expect("rebind replies");
    let (tx63, mut rx63) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(63, tx63);
    assert_eq!(replay_offline(&rebind, &shared).await, 0);
    assert!(rx63.try_recv().is_err(), "acked message must not replay");
}

#[tokio::test]
async fn qos1_redelivery_preserves_order_with_dup() {
    let shared = test_shared();
    let bind = bind_frame(61, 1, encode_bind_meta("q1-order", false, 60));
    reply_for_frame(&bind, &shared.sessions).expect("bind replies");
    let sub = subscribe_frame(61, 2, encode_subscribe_meta(5, "q1-order", &[("t", 1)]));
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");

    let (tx61, mut rx61) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(61, tx61);
    let mut sent_pids = Vec::new();
    bind_test_publisher(&shared, 62);
    for (i, body) in [
        b"m-one".as_slice(),
        b"m-two".as_slice(),
        b"m-three".as_slice(),
    ]
    .iter()
    .enumerate()
    {
        let (meta, payload) = encode_publish_meta("t", i as u16, 1, false, body);
        let (_, deliveries) =
            apply_publish(&publish_frame(62, 3 + i as u64, meta, payload), &shared).await;
        assert_eq!(deliveries.len(), 1);
        for (conn_id, frame) in deliveries {
            let _ = shared.conns.route(conn_id, frame);
        }
        let got = rx61.try_recv().expect("downlink arrived");
        let (_, pid, _, _) = decode_publish_meta_parts(&got.metadata);
        sent_pids.push((pid, got.payload.clone()));
    }

    let mut unbind_meta = vec![0x00u8, 0x08u8];
    unbind_meta.extend_from_slice(b"q1-order");
    let unbind = BrokerFrame::new(
        OpCode::UnbindConnection,
        61,
        4,
        Bytes::from(unbind_meta),
        Bytes::new(),
    )
    .expect("valid unbind frame");
    apply_unbind(&unbind, &shared);
    let rebind = bind_frame(63, 1, encode_bind_meta("q1-order", false, 60));
    reply_for_frame(&rebind, &shared.sessions).expect("rebind replies");
    let (tx63, mut rx63) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(63, tx63);

    assert_eq!(replay_offline(&rebind, &shared).await, 3);
    for (pid, payload) in sent_pids {
        let got = rx63.try_recv().expect("ordered redelivery");
        assert_eq!(got.payload, payload, "original order preserved");
        assert!(
            decode_publish_dup(&got.metadata),
            "every replay carries DUP=1"
        );
        let (_, replay_pid, _, _) = decode_publish_meta_parts(&got.metadata);
        assert_eq!(replay_pid, pid, "same packet id reused in order");
    }
    assert!(rx63.try_recv().is_err(), "no new traffic interleaved");
}

#[tokio::test]
async fn qos1_clean_session_resumes_with_empty_inflight() {
    let shared = test_shared();
    let bind = bind_frame(61, 1, encode_bind_meta("q1-clean", true, 60));
    reply_for_frame(&bind, &shared.sessions).expect("bind replies");
    let sub = subscribe_frame(61, 2, encode_subscribe_meta(5, "q1-clean", &[("t", 1)]));
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");

    let (tx61, mut rx61) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(61, tx61);
    bind_test_publisher(&shared, 62);
    let (meta, payload) = encode_publish_meta("t", 7, 1, false, b"volatile");
    let (_, deliveries) = apply_publish(&publish_frame(62, 3, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1);
    for (conn_id, frame) in deliveries {
        let _ = shared.conns.route(conn_id, frame);
    }
    rx61.try_recv().expect("downlink arrived");
    assert_eq!(
        shared
            .sessions
            .get("q1-clean")
            .expect("session known")
            .inflight_len(),
        1
    );

    // Clean disconnect drops inflight state.
    let mut unbind_meta = vec![0x00u8, 0x08u8];
    unbind_meta.extend_from_slice(b"q1-clean");
    let unbind = BrokerFrame::new(
        OpCode::UnbindConnection,
        61,
        4,
        Bytes::from(unbind_meta),
        Bytes::new(),
    )
    .expect("valid unbind frame");
    apply_unbind(&unbind, &shared);
    assert_eq!(
        shared
            .sessions
            .get("q1-clean")
            .expect("session survives unbind")
            .inflight_len(),
        0,
        "clean disconnect keeps no inflight state"
    );

    let rebind = bind_frame(63, 1, encode_bind_meta("q1-clean", true, 60));
    reply_for_frame(&rebind, &shared.sessions).expect("rebind replies");
    let (tx63, mut rx63) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(63, tx63);
    assert_eq!(replay_offline(&rebind, &shared).await, 0);
    assert!(rx63.try_recv().is_err(), "clean resume replays nothing");
}

#[tokio::test]
async fn qos1_inflight_bound_spills_past_window_and_counts() {
    use broker_session::DEFAULT_MAX_QOS1_INFLIGHT;
    let shared = test_shared();
    let bind = bind_frame(61, 1, encode_bind_meta("q1-bound", false, 60));
    reply_for_frame(&bind, &shared.sessions).expect("bind replies");
    let sub = subscribe_frame(61, 2, encode_subscribe_meta(5, "q1-bound", &[("t", 1)]));
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");

    let (tx61, mut rx61) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(61, tx61);
    bind_test_publisher(&shared, 62);
    for i in 0..DEFAULT_MAX_QOS1_INFLIGHT {
        let (meta, payload) = encode_publish_meta("t", i as u16, 1, false, b"x");
        let (_, deliveries) =
            apply_publish(&publish_frame(62, 3 + i as u64, meta, payload), &shared).await;
        assert_eq!(deliveries.len(), 1, "live delivery unaffected by fill");
        for (conn_id, frame) in deliveries {
            let _ = shared.conns.route(conn_id, frame);
        }
        rx61.try_recv().expect("live downlink arrived");
    }
    let session = shared.sessions.get("q1-bound").expect("session known");
    assert_eq!(session.inflight_len(), DEFAULT_MAX_QOS1_INFLIGHT);
    assert_eq!(shared.metrics.inflight_dropped(), 0);
    assert_eq!(shared.metrics.inflight_spilled(), 0);

    // One past the window: still delivered live once AND still
    // tracked, now in the spill buffer (B4-01, no silent loss).
    let (meta, payload) = encode_publish_meta("t", 0x0FFF, 1, false, b"over");
    let (_, deliveries) = apply_publish(&publish_frame(62, 999, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1, "bound never blocks live delivery");
    for (conn_id, frame) in deliveries {
        let _ = shared.conns.route(conn_id, frame);
    }
    rx61.try_recv()
        .expect("over-window downlink still delivered live");
    assert_eq!(
        session.inflight_len(),
        DEFAULT_MAX_QOS1_INFLIGHT,
        "window stays bounded at the limit"
    );
    assert_eq!(
        session.inflight_spill_len(),
        1,
        "overflow is tracked in spill"
    );
    assert_eq!(session.inflight_total_len(), DEFAULT_MAX_QOS1_INFLIGHT + 1);
    assert_eq!(shared.metrics.inflight_spilled(), 1, "spill counted");
    assert_eq!(
        shared.metrics.inflight_dropped(),
        0,
        "nothing untracked while spill has room"
    );
}

#[tokio::test]
async fn qos1_inflight_spill_redelivers_everything_with_dup() {
    use broker_session::{DEFAULT_MAX_QOS1_INFLIGHT, DEFAULT_MAX_QOS1_SPILL};
    let shared = test_shared();
    let bind = bind_frame(61, 1, encode_bind_meta("q1-spill-all", false, 60));
    reply_for_frame(&bind, &shared.sessions).expect("bind replies");
    let sub = subscribe_frame(61, 2, encode_subscribe_meta(5, "q1-spill-all", &[("t", 1)]));
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");

    // Stall the acknowledger: publish window + spill + 3 without ever
    // acking, through the broker publish-to-delivery event.
    let (tx61, mut rx61) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(61, tx61);
    let total = DEFAULT_MAX_QOS1_INFLIGHT + 5;
    assert!(
        total < DEFAULT_MAX_QOS1_INFLIGHT + DEFAULT_MAX_QOS1_SPILL,
        "test stays inside the spill bound"
    );
    let mut live_pids = Vec::new();
    let mut live_payloads = Vec::new();
    bind_test_publisher(&shared, 62);
    for i in 0..total {
        let body = format!("spill-{i:04}");
        let (meta, payload) = encode_publish_meta("t", i as u16, 1, false, body.as_bytes());
        let (_, deliveries) =
            apply_publish(&publish_frame(62, 100 + i as u64, meta, payload), &shared).await;
        assert_eq!(deliveries.len(), 1, "live delivery never blocks");
        for (conn_id, frame) in deliveries {
            let _ = shared.conns.route(conn_id, frame);
        }
        let got = rx61.try_recv().expect("live downlink arrived");
        let (_, pid, _, _) = decode_publish_meta_parts(&got.metadata);
        assert_ne!(pid, 0);
        live_pids.push(pid);
        live_payloads.push(got.payload.clone());
    }
    let session = shared.sessions.get("q1-spill-all").expect("session known");
    assert_eq!(session.inflight_len(), DEFAULT_MAX_QOS1_INFLIGHT);
    assert_eq!(session.inflight_spill_len(), 5);
    assert_eq!(shared.metrics.inflight_spilled(), 5);
    assert_eq!(shared.metrics.inflight_dropped(), 0);

    // Disconnect without acking anything, then reconnect: every QoS 1
    // message replays, window oldest-first then spill oldest-first,
    // every replay with DUP set and its original packet id.
    let mut unbind_meta = vec![0x00u8, 0x0Bu8];
    unbind_meta.extend_from_slice(b"q1-spill-all");
    let unbind = BrokerFrame::new(
        OpCode::UnbindConnection,
        61,
        4,
        Bytes::from(unbind_meta),
        Bytes::new(),
    )
    .expect("valid unbind frame");
    apply_unbind(&unbind, &shared);
    let rebind = bind_frame(63, 1, encode_bind_meta("q1-spill-all", false, 60));
    reply_for_frame(&rebind, &shared.sessions).expect("rebind replies");
    let (tx63, mut rx63) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(63, tx63);
    assert_eq!(replay_offline(&rebind, &shared).await, total);
    assert_eq!(shared.metrics.inflight_spill_replayed(), 5);
    for (pid, payload) in live_pids.iter().zip(live_payloads.iter()) {
        let got = rx63.try_recv().expect("ordered redelivery past the window");
        assert_eq!(&got.payload, payload, "original order preserved");
        assert!(
            decode_publish_dup(&got.metadata),
            "every replay past the window carries DUP=1"
        );
        let (_, replay_pid, _, _) = decode_publish_meta_parts(&got.metadata);
        assert_eq!(&replay_pid, pid, "same packet id reused in order");
    }
    assert!(rx63.try_recv().is_err(), "no new traffic interleaved");
}

#[tokio::test]
async fn qos1_inflight_bound_configurable_and_overflow_counted() {
    let shared = test_shared();
    shared.sessions.set_max_qos1_inflight(3);
    shared.sessions.set_max_qos1_spill(2);
    assert_eq!(shared.sessions.max_qos1_inflight(), 3);
    assert_eq!(shared.sessions.max_qos1_spill(), 2);

    let bind = bind_frame(61, 1, encode_bind_meta("q1-tiny", false, 60));
    reply_for_frame(&bind, &shared.sessions).expect("bind replies");
    let session = shared.sessions.get("q1-tiny").expect("session known");
    assert_eq!(
        session.max_inflight(),
        3,
        "manager bound reaches the session"
    );
    assert_eq!(session.max_spill(), 2);
    let sub = subscribe_frame(61, 2, encode_subscribe_meta(5, "q1-tiny", &[("t", 1)]));
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");

    let (tx61, mut rx61) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(61, tx61);
    bind_test_publisher(&shared, 62);
    for i in 0..6u16 {
        let (meta, payload) = encode_publish_meta("t", i, 1, false, b"x");
        let (_, deliveries) = apply_publish(
            &publish_frame(62, 200 + u64::from(i), meta, payload),
            &shared,
        )
        .await;
        assert_eq!(deliveries.len(), 1);
        for (conn_id, frame) in deliveries {
            let _ = shared.conns.route(conn_id, frame);
        }
        rx61.try_recv().expect("live downlink arrived");
    }
    // Window 3 tracked, next 2 spilled, last one past both: live once,
    // untracked, both drop counters bump.
    assert_eq!(session.inflight_len(), 3);
    assert_eq!(session.inflight_spill_len(), 2);
    assert_eq!(session.inflight_total_len(), 5);
    assert_eq!(shared.metrics.inflight_spilled(), 2);
    assert_eq!(shared.metrics.inflight_dropped(), 1);
    assert_eq!(shared.metrics.inflight_spill_evicted(), 1);
}

#[tokio::test]
async fn qos1_inflight_delivery_workload_timings() {
    use broker_session::DEFAULT_MAX_QOS1_INFLIGHT;
    use std::hint::black_box;
    use std::time::Instant;

    // B4-01 delivery-workload before/after numbers (spec Done-when
    // bullet 5, RULEBOOK hot-path row): measured through the broker
    // publish-to-delivery event (`apply_publish`), not just the
    // session store. BEFORE is the steady-state loop with the spill
    // empty: `build_downlink_frames` takes only the window lock here,
    // exactly the pre-B4-01 work plus one relaxed atomic load, so its
    // rate is the pre-change baseline. AFTER is the overflow loop past
    // the window, which additionally takes the spill lock and bumps
    // `inflight_spilled`. Both print msgs/sec and avg ns/msg into the
    // gate output with no threshold assert; counts are CI sample
    // sizes, not SLOs. Per-message bound: window (default 100) + spill
    // (default 1000) entries, each one shared `Bytes` handle plus topic
    // bytes, never N payload copies.
    let shared = test_shared();
    let bind = bind_frame(61, 1, encode_bind_meta("q1-timing", false, 60));
    reply_for_frame(&bind, &shared.sessions).expect("bind replies");
    let sub = subscribe_frame(61, 2, encode_subscribe_meta(5, "q1-timing", &[("t", 1)]));
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");
    let (tx61, mut rx61) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(61, tx61);
    let session = shared.sessions.get("q1-timing").expect("session known");

    // BEFORE: steady-state publish-to-delivery, spill empty. Every
    // publish is acked at once so the window never fills and the spill
    // lock is never taken (guarded by `has_spill`).
    let steady_iters = 200usize;
    let steady_start = Instant::now();
    bind_test_publisher(&shared, 62);
    for i in 0..steady_iters {
        let (meta, payload) = encode_publish_meta("t", i as u16, 1, false, b"x");
        let (_, deliveries) =
            apply_publish(&publish_frame(62, 1000 + i as u64, meta, payload), &shared).await;
        assert_eq!(deliveries.len(), 1, "live delivery never blocks");
        for (conn_id, frame) in deliveries {
            let _ = shared.conns.route(conn_id, frame);
        }
        let got = rx61.try_recv().expect("live downlink arrived");
        let (_, pid, _, _) = decode_publish_meta_parts(&got.metadata);
        black_box(pid);
        assert!(session.ack_inflight(pid), "steady-state ack releases");
    }
    let steady_elapsed = steady_start.elapsed();
    assert!(
        !session.has_spill(),
        "steady-state loop must never touch the spill"
    );
    let steady_rate = steady_iters as f64 / steady_elapsed.as_secs_f64();
    let steady_avg_ns = steady_elapsed.as_nanos() as f64 / steady_iters as f64;
    println!(
        "qos1 broker delivery workload steady-state (spill empty, before): {steady_rate:.0} msgs/sec, avg {steady_avg_ns:.1} ns/msg ({steady_iters} apply_publish+ack rounds in {steady_elapsed:?})"
    );

    // AFTER: fill the window without acking, then time overflow past it.
    for i in 0..DEFAULT_MAX_QOS1_INFLIGHT {
        let (meta, payload) = encode_publish_meta("t", i as u16, 1, false, b"y");
        let (_, deliveries) =
            apply_publish(&publish_frame(62, 2000 + i as u64, meta, payload), &shared).await;
        assert_eq!(deliveries.len(), 1, "live delivery never blocks");
        for (conn_id, frame) in deliveries {
            let _ = shared.conns.route(conn_id, frame);
        }
        rx61.try_recv().expect("live downlink arrived");
    }
    assert_eq!(session.inflight_len(), DEFAULT_MAX_QOS1_INFLIGHT);
    let spill_iters = 20usize;
    let spill_start = Instant::now();
    for i in 0..spill_iters {
        let (meta, payload) = encode_publish_meta("t", i as u16, 1, false, b"z");
        let (_, deliveries) =
            apply_publish(&publish_frame(62, 3000 + i as u64, meta, payload), &shared).await;
        assert_eq!(deliveries.len(), 1, "bound never blocks live delivery");
        for (conn_id, frame) in deliveries {
            let _ = shared.conns.route(conn_id, frame);
        }
        rx61.try_recv()
            .expect("over-window downlink still delivered live");
    }
    let spill_elapsed = spill_start.elapsed();
    assert_eq!(session.inflight_spill_len(), spill_iters);
    assert_eq!(shared.metrics.inflight_spilled(), spill_iters as u64);
    let spill_rate = spill_iters as f64 / spill_elapsed.as_secs_f64();
    let spill_avg_ns = spill_elapsed.as_nanos() as f64 / spill_iters as f64;
    println!(
        "qos1 broker delivery workload spill-overflow (past window, after): {spill_rate:.0} msgs/sec, avg {spill_avg_ns:.1} ns/msg ({spill_iters} apply_publish rounds in {spill_elapsed:?})"
    );

    // Replay workload through the reconnect event, window then spill.
    let mut unbind_meta = vec![0x00u8, 0x09u8];
    unbind_meta.extend_from_slice(b"q1-timing");
    let unbind = BrokerFrame::new(
        OpCode::UnbindConnection,
        61,
        4,
        Bytes::from(unbind_meta),
        Bytes::new(),
    )
    .expect("valid unbind frame");
    apply_unbind(&unbind, &shared);
    let rebind = bind_frame(63, 1, encode_bind_meta("q1-timing", false, 60));
    reply_for_frame(&rebind, &shared.sessions).expect("rebind replies");
    let (tx63, mut rx63) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(63, tx63);
    let replay_start = Instant::now();
    let replayed = replay_offline(&rebind, &shared).await;
    let replay_elapsed = replay_start.elapsed();
    assert_eq!(replayed, DEFAULT_MAX_QOS1_INFLIGHT + spill_iters);
    let replay_rate = replayed as f64 / replay_elapsed.as_secs_f64();
    let replay_avg_ns = replay_elapsed.as_nanos() as f64 / replayed as f64;
    println!(
        "qos1 broker replay workload (window plus spill, after): {replay_rate:.0} msgs/sec, avg {replay_avg_ns:.1} ns/msg ({replayed} replayed in {replay_elapsed:?})"
    );
    for _ in 0..replayed {
        let got = rx63.try_recv().expect("ordered redelivery");
        assert!(decode_publish_dup(&got.metadata), "replay carries DUP=1");
    }
    assert!(rx63.try_recv().is_err(), "no new traffic interleaved");
}

#[tokio::test]
async fn publish_qos0_needs_no_ack() {
    let shared = test_shared();
    bind_test_publisher(&shared, 60);
    let (meta, payload) = encode_publish_meta("t", 0, 0, false, b"x");
    let (ack, deliveries) = apply_publish(&publish_frame(60, 1, meta, payload), &shared).await;
    assert!(ack.is_none(), "QoS 0 needs no PubAck");
    assert!(deliveries.is_empty(), "no subscribers, no deliveries");
}

#[tokio::test]
async fn publish_malformed_meta_yields_nothing() {
    let shared = test_shared();
    let (ack, deliveries) = apply_publish(
        &publish_frame(60, 1, Bytes::from(vec![0xFF]), Bytes::new()),
        &shared,
    )
    .await;
    assert!(ack.is_none());
    assert!(deliveries.is_empty());
}

#[cfg(test)]
fn qos2_frame(opcode: OpCode, conn_id: u64, seq: u64, packet_id: u16) -> BrokerFrame {
    BrokerFrame::new(
        opcode,
        conn_id,
        seq,
        Bytes::from(packet_id.to_be_bytes().to_vec()),
        Bytes::new(),
    )
    .expect("valid QoS 2 ack frame")
}

#[cfg(test)]
fn decode_qos2_id(meta: &[u8]) -> u16 {
    u16::from_be_bytes([meta[0], meta[1]])
}

/// D1-01 normal flow: PUBLISH QoS 2 stores and replies PUBREC with no
/// routing; PUBREL routes once and replies PUBCOMP; the downlink runs
/// PUBLISH/PUBREC/PUBREL/PUBCOMP to completion with the payload
/// delivered exactly once.
#[tokio::test]
async fn qos2_normal_flow_routes_once_on_pubrel() {
    let shared = test_shared();
    let bind_sub = bind_frame(51, 1, encode_bind_meta("q2-sub", false, 60));
    reply_for_frame(&bind_sub, &shared.sessions).expect("bind replies");
    let sub = subscribe_frame(51, 2, encode_subscribe_meta(3, "q2-sub", &[("t", 2)]));
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");
    let (session_sub, _) = shared.sessions.get_or_create("q2-sub", false);
    *session_sub.conn_id.write() = Some(51);
    let bind_pub = bind_frame(52, 1, encode_bind_meta("q2-pub", true, 60));
    reply_for_frame(&bind_pub, &shared.sessions).expect("bind replies");

    // Phase 1: PUBLISH QoS 2 stores, replies PUBREC, routes nothing.
    let (meta, payload) = encode_publish_meta("t", 7, 2, false, b"exactly-once");
    let (rec, deliveries) = apply_publish(&publish_frame(52, 8, meta, payload), &shared).await;
    let rec = rec.expect("QoS 2 PUBLISH needs PUBREC");
    assert_eq!(rec.header.opcode, OpCode::PubRecOut);
    assert_eq!(rec.header.conn_id, 52);
    assert_eq!(decode_qos2_id(&rec.metadata), 7);
    assert!(deliveries.is_empty(), "nothing routes before PUBREL");
    assert_eq!(
        shared
            .sessions
            .get("q2-pub")
            .expect("pub session")
            .qos2_inbound_len(),
        1
    );

    // Phase 2: PUBREL routes once and replies PUBCOMP.
    let (comp, deliveries) =
        apply_qos2_pubrel(&qos2_frame(OpCode::PubRelIn, 52, 9, 7), &shared).await;
    let comp = comp.expect("PUBREL needs PUBCOMP");
    assert_eq!(comp.header.opcode, OpCode::PubCompOut);
    assert_eq!(decode_qos2_id(&comp.metadata), 7);
    assert_eq!(deliveries.len(), 1, "PUBREL routes exactly once");
    assert_eq!(deliveries[0].0, 51);
    assert_eq!(deliveries[0].1.payload, Bytes::from_static(b"exactly-once"));
    let (topic, downlink_pid, qos, _) = decode_publish_meta_parts(&deliveries[0].1.metadata);
    assert_eq!(topic, "t");
    assert_eq!(qos, 2, "effective QoS is min(pub 2, sub 2)");
    assert_ne!(downlink_pid, 0);
    assert_eq!(
        shared
            .sessions
            .get("q2-pub")
            .expect("pub session")
            .qos2_inbound_len(),
        0,
        "packet id released on PUBREL"
    );

    // Outbound phase 1: subscriber PUBREC yields PUBREL.
    for (conn_id, frame) in deliveries {
        let _ = shared.conns.route(conn_id, frame);
    }
    let session = shared.sessions.get("q2-sub").expect("sub session");
    assert_eq!(session.qos2_outbound_len(), 1);
    let rel = apply_qos2_pubrec(&qos2_frame(OpCode::PubRecIn, 51, 10, downlink_pid), &shared)
        .expect("PUBREC needs PUBREL");
    assert_eq!(rel.header.opcode, OpCode::PubRelOut);
    assert_eq!(decode_qos2_id(&rel.metadata), downlink_pid);

    // Outbound phase 2: subscriber PUBCOMP releases the id.
    apply_qos2_pubcomp(
        &qos2_frame(OpCode::PubCompIn, 51, 11, downlink_pid),
        &shared,
    );
    assert_eq!(session.qos2_outbound_len(), 0);
}

/// D1-01 duplicate PUBLISH before PUBREL replies PUBREC without a
/// second route; a second PUBREL after release completes empty.
#[tokio::test]
async fn qos2_duplicate_publish_before_pubrel_no_second_route() {
    let shared = test_shared();
    let bind_sub = bind_frame(51, 1, encode_bind_meta("q2-dup-sub", false, 60));
    reply_for_frame(&bind_sub, &shared.sessions).expect("bind replies");
    let sub = subscribe_frame(51, 2, encode_subscribe_meta(3, "q2-dup-sub", &[("t", 2)]));
    apply_subscribe(&sub, &shared)
        .await
        .0
        .expect("subscribe replies");
    let (session_sub, _) = shared.sessions.get_or_create("q2-dup-sub", false);
    *session_sub.conn_id.write() = Some(51);
    let bind_pub = bind_frame(52, 1, encode_bind_meta("q2-dup-pub", true, 60));
    reply_for_frame(&bind_pub, &shared.sessions).expect("bind replies");

    let (meta, payload) = encode_publish_meta("t", 9, 2, false, b"dup");
    let (first_rec, first_deliveries) =
        apply_publish(&publish_frame(52, 2, meta, payload), &shared).await;
    assert!(first_rec.is_some());
    assert!(first_deliveries.is_empty());
    // Repeat of the same packet id before PUBREL: duplicate.
    let (meta, payload) = encode_publish_meta("t", 9, 2, false, b"dup");
    let (second_rec, second_deliveries) =
        apply_publish(&publish_frame(52, 3, meta, payload), &shared).await;
    let second_rec = second_rec.expect("duplicate needs PUBREC again");
    assert_eq!(second_rec.header.opcode, OpCode::PubRecOut);
    assert!(
        second_deliveries.is_empty(),
        "duplicate must not route again"
    );

    // PUBREL routes exactly once; a retried PUBREL completes empty.
    let (_, deliveries) = apply_qos2_pubrel(&qos2_frame(OpCode::PubRelIn, 52, 4, 9), &shared).await;
    assert_eq!(deliveries.len(), 1);
    let (comp, redeliveries) =
        apply_qos2_pubrel(&qos2_frame(OpCode::PubRelIn, 52, 5, 9), &shared).await;
    assert!(comp.is_some(), "retried PUBREL still gets PUBCOMP");
    assert!(redeliveries.is_empty(), "retried PUBREL routes nothing");
}

/// D1-01 PUBREL for an unknown packet id replies PUBCOMP and routes
/// nothing, so a retried PUBREL never stalls the publisher.
#[tokio::test]
async fn qos2_pubrel_unknown_id_replies_pubcomp_without_route() {
    let shared = test_shared();
    let bind = bind_frame(61, 1, encode_bind_meta("q2-unknown", true, 60));
    reply_for_frame(&bind, &shared.sessions).expect("bind replies");
    let (comp, deliveries) =
        apply_qos2_pubrel(&qos2_frame(OpCode::PubRelIn, 61, 2, 1234), &shared).await;
    let comp = comp.expect("unknown PUBREL needs PUBCOMP");
    assert_eq!(comp.header.opcode, OpCode::PubCompOut);
    assert_eq!(comp.header.conn_id, 61);
    assert_eq!(decode_qos2_id(&comp.metadata), 1234);
    assert!(deliveries.is_empty());
    assert_eq!(decode_qos2_meta(&comp.metadata), Some(1234));
}

/// D1-01 resend after reconnect: uncompleted QoS 2 downlinks replay in
/// queued order (PUBLISH with DUP while waiting PUBREC, PUBREL while
/// waiting PUBCOMP).
#[tokio::test]
async fn qos2_uncompleted_downlink_resends_after_reconnect() {
    let shared = test_shared();
    let bind = bind_frame(61, 1, encode_bind_meta("q2-replay", false, 60));
    reply_for_frame(&bind, &shared.sessions).expect("bind replies");
    let sub = subscribe_frame(61, 2, encode_subscribe_meta(5, "q2-replay", &[("t", 2)]));
    apply_subscribe(&sub, &shared)
        .await
        .0
        .expect("subscribe replies");

    let (tx61, mut rx61) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(61, tx61);
    let bind_pub = bind_frame(62, 1, encode_bind_meta("q2-replay-pub", true, 60));
    reply_for_frame(&bind_pub, &shared.sessions).expect("pub bind replies");
    let (meta, payload) = encode_publish_meta("t", 7, 2, false, b"replay-me");
    let (_, deliveries) = apply_publish(&publish_frame(62, 3, meta, payload), &shared).await;
    // First phase alone routes nothing: complete it via PUBREL.
    assert!(deliveries.is_empty());
    let (_, deliveries) = apply_qos2_pubrel(&qos2_frame(OpCode::PubRelIn, 62, 4, 7), &shared).await;
    assert_eq!(deliveries.len(), 1);
    for (conn_id, frame) in deliveries {
        let _ = shared.conns.route(conn_id, frame);
    }
    let first = rx61.try_recv().expect("downlink arrived");
    let (_, first_pid, qos, _) = decode_publish_meta_parts(&first.metadata);
    assert_eq!(qos, 2);
    assert!(!decode_publish_dup(&first.metadata));

    // Detach without completing, reconnect: PUBLISH replays with DUP.
    let session = shared.sessions.get("q2-replay").expect("session known");
    *session.conn_id.write() = Some(61);
    apply_unbind(&unbind_frame(61, 5, "q2-replay"), &shared);
    let rebind = bind_frame(63, 1, encode_bind_meta("q2-replay", false, 60));
    reply_for_frame(&rebind, &shared.sessions).expect("rebind replies");
    let (tx63, mut rx63) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(63, tx63);
    assert_eq!(replay_offline(&rebind, &shared).await, 1);
    let replayed = rx63.try_recv().expect("PUBLISH replay arrived");
    assert_eq!(replayed.header.opcode, OpCode::PublishOut);
    assert!(decode_publish_dup(&replayed.metadata));
    let (_, replay_pid, _, _) = decode_publish_meta_parts(&replayed.metadata);
    assert_eq!(replay_pid, first_pid);

    // Subscriber answers PUBREC: the entry now waits for PUBCOMP.
    let rel = apply_qos2_pubrec(&qos2_frame(OpCode::PubRecIn, 63, 2, first_pid), &shared)
        .expect("PUBREC needs PUBREL");
    assert_eq!(rel.header.opcode, OpCode::PubRelOut);
    // Detach again before PUBCOMP, reconnect: PUBREL replays.
    apply_unbind(&unbind_frame(63, 3, "q2-replay"), &shared);
    let rebind2 = bind_frame(64, 1, encode_bind_meta("q2-replay", false, 60));
    reply_for_frame(&rebind2, &shared.sessions).expect("rebind replies");
    let (tx64, mut rx64) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(64, tx64);
    assert_eq!(replay_offline(&rebind2, &shared).await, 1);
    let rel_replay = rx64.try_recv().expect("PUBREL replay arrived");
    assert_eq!(rel_replay.header.opcode, OpCode::PubRelOut);
    assert_eq!(decode_qos2_id(&rel_replay.metadata), first_pid);

    // PUBCOMP completes: a further reconnect replays nothing.
    apply_qos2_pubcomp(&qos2_frame(OpCode::PubCompIn, 64, 4, first_pid), &shared);
    assert_eq!(session.qos2_outbound_len(), 0);
    apply_unbind(&unbind_frame(64, 5, "q2-replay"), &shared);
    let rebind3 = bind_frame(65, 1, encode_bind_meta("q2-replay", false, 60));
    reply_for_frame(&rebind3, &shared.sessions).expect("rebind replies");
    let (tx65, mut rx65) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(65, tx65);
    assert_eq!(replay_offline(&rebind3, &shared).await, 0);
    assert!(rx65.try_recv().is_err());
}

/// D1-01 integration: a QoS 2 publish to a QoS 2 subscriber arrives
/// exactly once end to end, even when the publisher repeats PUBLISH
/// before PUBREL.
#[tokio::test]
async fn qos2_end_to_end_exactly_once() {
    let shared = test_shared();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (stream, _) = listener.accept().await.expect("accept");
            let shared = shared.clone();
            tokio::spawn(async move {
                handle_connection(stream, shared).await.expect("handle");
            });
        }
        std::future::pending::<()>().await;
    });

    let sub_io = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect sub");
    let sub = FramedTransport::new(sub_io);
    sub.send(bind_frame(
        101,
        1,
        encode_bind_meta("q2-e2e-sub", false, 60),
    ))
    .await
    .expect("bind");
    let _ = sub.recv().await.expect("recv binding");
    sub.send(subscribe_frame(
        101,
        2,
        encode_subscribe_meta(11, "q2-e2e-sub", &[("q2/e2e", 2)]),
    ))
    .await
    .expect("subscribe");
    let suback = sub.recv().await.expect("recv suback");
    assert_eq!(suback.header.opcode, OpCode::SubAckOut);

    let pub_io = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect pub");
    let publ = FramedTransport::new(pub_io);
    publ.send(bind_frame(102, 1, encode_bind_meta("q2-e2e-pub", true, 60)))
        .await
        .expect("bind");
    let _ = publ.recv().await.expect("recv binding");

    // PUBLISH QoS 2, duplicated before PUBREL: both get PUBREC.
    let (meta, payload) = encode_publish_meta("q2/e2e", 77, 2, false, b"once");
    publ.send(publish_frame(102, 2, meta, payload))
        .await
        .expect("publish");
    let rec1 = publ.recv().await.expect("recv pubrec");
    assert_eq!(rec1.header.opcode, OpCode::PubRecOut);
    assert_eq!(decode_qos2_id(&rec1.metadata), 77);
    let (meta, payload) = encode_publish_meta("q2/e2e", 77, 2, false, b"once");
    publ.send(publish_frame(102, 3, meta, payload))
        .await
        .expect("duplicate");
    let rec2 = publ.recv().await.expect("recv second pubrec");
    assert_eq!(rec2.header.opcode, OpCode::PubRecOut);

    // PUBREL completes the inbound flow.
    publ.send(qos2_frame(OpCode::PubRelIn, 102, 4, 77))
        .await
        .expect("pubrel");
    let comp = publ.recv().await.expect("recv pubcomp");
    assert_eq!(comp.header.opcode, OpCode::PubCompOut);
    assert_eq!(decode_qos2_id(&comp.metadata), 77);

    // Subscriber gets exactly one PUBLISH QoS 2.
    let routed = sub.recv().await.expect("recv publish");
    assert_eq!(routed.header.opcode, OpCode::PublishOut);
    assert_eq!(routed.payload, Bytes::from_static(b"once"));
    let (_, downlink_pid, qos, _) = decode_publish_meta_parts(&routed.metadata);
    assert_eq!(qos, 2);
    assert_ne!(downlink_pid, 0);

    // Outbound flow to completion.
    sub.send(qos2_frame(OpCode::PubRecIn, 101, 3, downlink_pid))
        .await
        .expect("pubrec");
    let rel = sub.recv().await.expect("recv pubrel");
    assert_eq!(rel.header.opcode, OpCode::PubRelOut);
    sub.send(qos2_frame(OpCode::PubCompIn, 101, 4, downlink_pid))
        .await
        .expect("pubcomp");

    // No second delivery: the duplicate PUBLISH routed only once.
    let nothing = tokio::time::timeout(std::time::Duration::from_millis(300), sub.recv()).await;
    assert!(nothing.is_err(), "exactly-once: no second delivery");

    server.abort();
}

/// D1-02: the bounded QoS 0 backlog drops oldest-first, counts every
/// drop labelled by client, and keeps a fast subscriber whole while
/// a stalled one sheds. Drives the real `apply_publish` fan-out plus
/// `ConnTable::route` (the production egress path), not a test-only
/// queue.
#[tokio::test]
async fn qos0_bounded_backlog_sheds_oldest_fast_keeps_receiving() {
    let shared = test_shared();
    shared.conns.set_qos0_bound(3);

    for (conn, client) in [(61u64, "q0-fast"), (63u64, "q0-stalled")] {
        let frame = subscribe_frame(conn, 1, encode_subscribe_meta(1, client, &[("q0/t", 0)]));
        let (reply, _) = apply_subscribe(&frame, &shared).await;
        reply.expect("subscribe replies");
        let (session, _) = shared.sessions.get_or_create(client, true);
        *session.conn_id.write() = Some(conn);
        let (tx, _rx) = unbounded_channel::<BrokerFrame>();
        shared.conns.register(conn, tx);
        shared.conns.set_client_label(conn, client);
    }

    let mut fast_got: Vec<Vec<u8>> = Vec::new();
    bind_test_publisher(&shared, 99);
    for i in 0..6u8 {
        let (meta, payload) = encode_publish_meta("q0/t", 0, 0, false, std::slice::from_ref(&i));
        let (_, deliveries) =
            apply_publish(&publish_frame(99, u64::from(i), meta, payload), &shared).await;
        assert_eq!(deliveries.len(), 2, "both subscribers match every publish");
        for (conn_id, frame) in deliveries {
            assert!(shared.conns.route(conn_id, frame));
        }
        // Fast drains immediately; stalled never drains until the end.
        for frame in shared.conns.drain_qos0(61, 8) {
            fast_got.push(frame.payload.to_vec());
        }
    }
    for frame in shared.conns.drain_qos0(61, 8) {
        fast_got.push(frame.payload.to_vec());
    }
    assert_eq!(
        fast_got,
        vec![vec![0], vec![1], vec![2], vec![3], vec![4], vec![5]],
        "fast subscriber sees every QoS 0 publish in order"
    );
    assert_eq!(shared.conns.qos0_len(61), 0);
    assert_eq!(shared.conns.qos0_len(63), 3);
    let stalled: Vec<Vec<u8>> = shared
        .conns
        .drain_qos0(63, 8)
        .iter()
        .map(|f| f.payload.to_vec())
        .collect();
    assert_eq!(
        stalled,
        vec![vec![3], vec![4], vec![5]],
        "stalled keeps newest, oldest shed"
    );
    assert_eq!(shared.metrics.egress_qos0_shed(), 3);
    assert_eq!(shared.metrics.egress_qos0_shed_for("q0-stalled"), 3);
    assert_eq!(shared.metrics.egress_qos0_shed_for("q0-fast"), 0);
}

/// D1-02: publishers never block on a stalled subscriber. Ten
/// thousand synchronous QoS 0 publishes to a never-drained backlog
/// complete (each `route` returns true immediately) and the backlog
/// stays at its bound; the shed counter accounts for the rest.
#[tokio::test]
async fn qos0_stalled_subscriber_never_blocks_publisher() {
    let shared = test_shared();
    shared.conns.set_qos0_bound(8);
    let frame = subscribe_frame(61, 1, encode_subscribe_meta(1, "q0-block", &[("q0/b", 0)]));
    let (reply, _) = apply_subscribe(&frame, &shared).await;
    reply.expect("subscribe replies");
    let (session, _) = shared.sessions.get_or_create("q0-block", true);
    *session.conn_id.write() = Some(61);
    let (tx, _rx) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(61, tx);
    shared.conns.set_client_label(61, "q0-block");

    bind_test_publisher(&shared, 99);
    for i in 0..10_000u32 {
        let (meta, payload) = encode_publish_meta("q0/b", 0, 0, false, &[(i % 251) as u8]);
        let (_, deliveries) = apply_publish(
            &publish_frame(99, u64::from(i % 10_000), meta, payload),
            &shared,
        )
        .await;
        assert_eq!(deliveries.len(), 1);
        for (conn_id, frame) in deliveries {
            assert!(shared.conns.route(conn_id, frame));
        }
    }
    assert_eq!(shared.conns.qos0_len(61), 8);
    assert_eq!(shared.metrics.egress_qos0_shed(), 10_000 - 8);
    assert_eq!(shared.metrics.egress_qos0_shed_for("q0-block"), 10_000 - 8);
}

/// D1-02: QoS 1 keeps its delivery guarantee under QoS 0 pressure.
/// Filling a subscriber's QoS 0 backlog must not drop, delay, or
/// recount its QoS 1 downlink (shared egress path, separate queues).
#[tokio::test]
async fn qos1_keeps_guarantee_under_qos0_pressure() {
    let shared = test_shared();
    shared.conns.set_qos0_bound(2);
    let sub0 = subscribe_frame(
        61,
        1,
        encode_subscribe_meta(1, "q01-mixed", &[("q01/t", 0)]),
    );
    let (reply, _) = apply_subscribe(&sub0, &shared).await;
    reply.expect("subscribe replies");
    let sub1 = subscribe_frame(
        61,
        2,
        encode_subscribe_meta(2, "q01-mixed", &[("q01/q1", 1)]),
    );
    let (reply, _) = apply_subscribe(&sub1, &shared).await;
    reply.expect("subscribe replies");
    let (session, _) = shared.sessions.get_or_create("q01-mixed", true);
    *session.conn_id.write() = Some(61);
    let (tx, mut rx) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(61, tx);
    shared.conns.set_client_label(61, "q01-mixed");

    bind_test_publisher(&shared, 99);
    for i in 0..4u8 {
        let (meta, payload) = encode_publish_meta("q01/t", 0, 0, false, std::slice::from_ref(&i));
        let (_, deliveries) =
            apply_publish(&publish_frame(99, u64::from(i), meta, payload), &shared).await;
        for (conn_id, frame) in deliveries {
            assert!(shared.conns.route(conn_id, frame));
        }
    }
    let shed_before = shared.metrics.egress_qos0_shed();
    assert_eq!(shed_before, 2);
    assert_eq!(shared.conns.qos0_len(61), 2);

    let (meta, payload) = encode_publish_meta("q01/q1", 41, 1, false, b"keep");
    let (ack, deliveries) = apply_publish(&publish_frame(99, 50, meta, payload), &shared).await;
    assert!(ack.is_some(), "QoS 1 publisher is acked");
    assert_eq!(deliveries.len(), 1);
    for (conn_id, frame) in deliveries {
        assert!(shared.conns.route(conn_id, frame));
    }
    assert_eq!(
        shared.conns.qos0_len(61),
        2,
        "QoS 1 must not disturb the QoS 0 backlog"
    );
    assert_eq!(
        shared.metrics.egress_qos0_shed(),
        shed_before,
        "QoS 1 must not shed"
    );
    let qos1 = rx.try_recv().expect("QoS 1 via guaranteed mailbox");
    assert_eq!(qos1.payload, Bytes::from_static(b"keep"));
    assert_eq!(session.inflight_len(), 1, "QoS 1 tracked for redelivery");
}
