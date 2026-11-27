use super::*;

/// Decode the B4-05 Topic Alias Maximum out of a `SessionBinding`
/// reply (0 for pre-alias 10-byte encodings; the head value for
/// 12-byte and X1-02 extended encodings).
#[cfg(test)]
fn decode_session_binding_alias_max(meta: &[u8]) -> u16 {
    let (_, _, _, alias_max, _) =
        crate::decode_session_binding_full(meta).expect("SessionBinding meta must decode");
    alias_max
}

/// Encode `BindConnection` metadata with a B4-05 client alias maximum
/// (test mirror of the extended BEAM `encode_bind_meta/6` contract).
#[cfg(test)]
fn encode_bind_meta_with_alias(
    client_id: &str,
    clean_start: bool,
    keepalive: u16,
    client_alias_max: u16,
) -> Bytes {
    let mut meta = encode_bind_meta(client_id, clean_start, keepalive).to_vec();
    meta.extend_from_slice(&protocol_v5::encode_alias_maximum(client_alias_max));
    Bytes::from(meta)
}

/// Encode `PublishMeta` with a B4-05 topic alias (test mirror of the
/// extended BEAM `encode_publish_meta/6` contract). An empty topic
/// with a nonzero alias encodes alias-by-reference.
#[cfg(test)]
fn encode_publish_meta_with_alias(
    topic: &str,
    packet_id: u16,
    qos: u8,
    retain: bool,
    alias: u16,
    payload: &[u8],
) -> (Bytes, Bytes) {
    let mut meta = Vec::new();
    meta.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    meta.extend_from_slice(topic.as_bytes());
    meta.extend_from_slice(&packet_id.to_be_bytes());
    meta.push(qos);
    meta.push(u8::from(retain));
    meta.push(0u8);
    meta.extend_from_slice(&protocol_v5::encode_topic_alias(alias));
    (Bytes::from(meta), Bytes::from(payload.to_vec()))
}

/// Decode the B4-05 alias out of a `PublishOut` downlink frame (0 for
/// pre-alias encodings).
#[cfg(test)]
fn decode_publish_out_alias(meta: &[u8]) -> u16 {
    crate::decode_publish_out_alias(meta).expect("downlink meta decodes")
}

#[tokio::test]
async fn topic_alias_conack_carries_configured_maximum() {
    // Done-when 1: negotiate a maximum through the broker (CONNECT ->
    // bind, CONNACK <- SessionBinding) and assert CONNACK carries it.
    let shared = test_shared();
    shared.sessions.set_max_topic_alias(7);
    let bind = bind_frame(81, 1, encode_bind_meta("alias-neg-1", true, 60));
    let reply = reply_for_frame(&bind, &shared.sessions).expect("bind replies");
    assert_eq!(reply.header.opcode, OpCode::SessionBinding);
    let (_, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
    assert!(!present);
    assert_eq!(
        decode_session_binding_alias_max(&reply.metadata),
        7,
        "CONNACK carries the configured maximum"
    );
    let session = shared.sessions.get("alias-neg-1").expect("session known");
    assert_eq!(session.inbound_alias_max(), 7);
    // The client's own maximum rides the bind: the kernel outbound
    // bound follows it.
    let bind2 = bind_frame(
        82,
        1,
        encode_bind_meta_with_alias("alias-neg-2", true, 60, 5),
    );
    reply_for_frame(&bind2, &shared.sessions).expect("bind replies");
    let session2 = shared.sessions.get("alias-neg-2").expect("session known");
    assert_eq!(session2.outbound_alias_max(), 5);
    assert_eq!(session2.inbound_alias_max(), 7);
}

#[tokio::test]
async fn topic_alias_inbound_alias_delivers_correct_topic() {
    // Done-when 2 (inbound): publish through the broker using an alias
    // and assert the subscriber receives the correct topic.
    let shared = test_shared();
    let sub_bind = bind_frame(91, 1, encode_bind_meta("alias-sub-1", false, 60));
    reply_for_frame(&sub_bind, &shared.sessions).expect("sub binds");
    let sub = subscribe_frame(
        91,
        2,
        encode_subscribe_meta(5, "alias-sub-1", &[("sensors/#", 0)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");
    let (tx91, _rx91) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(91, tx91);

    let pub_bind = bind_frame(92, 1, encode_bind_meta("alias-pub-1", false, 60));
    reply_for_frame(&pub_bind, &shared.sessions).expect("pub binds");
    // First publish carries topic + alias 3: registers the mapping.
    let (meta, payload) = encode_publish_meta_with_alias("sensors/temp", 0, 0, false, 3, b"21.5");
    let (_, deliveries) = apply_publish(&publish_frame(92, 10, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1);
    for (conn_id, frame) in deliveries {
        let _ = shared.conns.route(conn_id, frame);
    }
    // QoS 0 downlinks ride the bounded per-subscriber backlog
    // (`ConnTable::route` enqueues), never the guaranteed mailbox.
    let got = shared.conns.pop_qos0(91).expect("downlink arrived");
    let (topic, _, _, _) = decode_publish_meta_parts(&got.metadata);
    assert_eq!(topic, "sensors/temp");
    assert_eq!(got.payload, Bytes::from_static(b"21.5"));
    // Second publish carries the empty topic + alias 3: resolves it.
    let (meta2, payload2) = encode_publish_meta_with_alias("", 0, 0, false, 3, b"22.5");
    let (_, deliveries2) = apply_publish(&publish_frame(92, 11, meta2, payload2), &shared).await;
    assert_eq!(deliveries2.len(), 1);
    for (conn_id, frame) in deliveries2 {
        let _ = shared.conns.route(conn_id, frame);
    }
    let got2 = shared.conns.pop_qos0(91).expect("aliased downlink arrived");
    let (topic2, _, _, _) = decode_publish_meta_parts(&got2.metadata);
    assert_eq!(
        topic2, "sensors/temp",
        "alias resolves to the correct topic"
    );
    assert_eq!(got2.payload, Bytes::from_static(b"22.5"));
}

#[tokio::test]
async fn topic_alias_outbound_assignment_reused_and_bounded() {
    // Done-when 2 (outbound) + 4: deliveries on a connection that
    // negotiated a maximum carry aliases; the table is bounded by the
    // maximum and reused rather than grown per message.
    let shared = test_shared();
    let sub_bind = bind_frame(
        101,
        1,
        encode_bind_meta_with_alias("alias-out-sub", false, 60, 2),
    );
    reply_for_frame(&sub_bind, &shared.sessions).expect("sub binds");
    let sub = subscribe_frame(
        101,
        2,
        encode_subscribe_meta(5, "alias-out-sub", &[("sensors/#", 0)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");
    let (tx101, _rx101) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(101, tx101);
    let pub_bind = bind_frame(102, 1, encode_bind_meta("alias-out-pub", false, 60));
    reply_for_frame(&pub_bind, &shared.sessions).expect("pub binds");

    // Publish three distinct topics past the maximum of 2.
    let mut aliases = Vec::new();
    for (i, topic) in ["sensors/a", "sensors/b", "sensors/c"]
        .into_iter()
        .enumerate()
    {
        let (meta, payload) = encode_publish_meta(topic, 0, 0, false, b"x");
        let (_, deliveries) =
            apply_publish(&publish_frame(102, 100 + i as u64, meta, payload), &shared).await;
        assert_eq!(deliveries.len(), 1);
        for (conn_id, frame) in deliveries {
            let _ = shared.conns.route(conn_id, frame);
        }
        // QoS 0 downlinks ride the bounded backlog, not the mailbox.
        let got = shared.conns.pop_qos0(101).expect("downlink arrived");
        let (got_topic, _, _, _) = decode_publish_meta_parts(&got.metadata);
        assert_eq!(got_topic, topic, "full topic still delivered");
        aliases.push(decode_publish_out_alias(&got.metadata));
    }
    assert_eq!(aliases[0], 1, "first topic claims alias 1");
    assert_eq!(aliases[1], 2, "second topic claims alias 2");
    assert_eq!(
        aliases[2], 0,
        "past the bound the full topic is sent with alias 0"
    );
    let session = shared.sessions.get("alias-out-sub").expect("session known");
    assert_eq!(
        session.outbound_alias_len(),
        2,
        "table bounded by the maximum"
    );
    // Repeat: the first topic reuses alias 1 instead of growing.
    let (meta, payload) = encode_publish_meta("sensors/a", 0, 0, false, b"y");
    let (_, deliveries) = apply_publish(&publish_frame(102, 200, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1);
    for (conn_id, frame) in deliveries {
        let _ = shared.conns.route(conn_id, frame);
    }
    let got = shared.conns.pop_qos0(101).expect("downlink arrived");
    assert_eq!(decode_publish_out_alias(&got.metadata), 1, "alias reused");
    assert_eq!(session.outbound_alias_len(), 2, "no growth per message");
}

#[tokio::test]
async fn topic_alias_above_maximum_rejected_with_correct_code() {
    // Done-when 3: an alias above the negotiated maximum is rejected
    // with the correct reason code (0x94).
    let shared = test_shared();
    let pub_bind = bind_frame(111, 1, encode_bind_meta("alias-rej-pub", false, 60));
    reply_for_frame(&pub_bind, &shared.sessions).expect("pub binds");
    let sub_bind = bind_frame(112, 1, encode_bind_meta("alias-rej-sub", false, 60));
    reply_for_frame(&sub_bind, &shared.sessions).expect("sub binds");
    let sub = subscribe_frame(
        112,
        2,
        encode_subscribe_meta(5, "alias-rej-sub", &[("sensors/#", 1)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");
    let (tx112, mut rx112) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(112, tx112);

    let max = shared.sessions.max_topic_alias();
    assert!(max > 0, "test needs aliases enabled");
    // QoS 1 publish with alias above the maximum: PUBACK 0x94, no
    // delivery.
    let (meta, payload) =
        encode_publish_meta_with_alias("sensors/temp", 7, 1, false, max + 1, b"x");
    let (ack, deliveries) = apply_publish(&publish_frame(111, 50, meta, payload), &shared).await;
    let ack = ack.expect("QoS 1 answers");
    assert_eq!(ack.header.opcode, OpCode::PubAckOut);
    assert_eq!(
        ack.metadata[2],
        protocol_v5::REASON_TOPIC_ALIAS_INVALID,
        "rejection carries the correct reason code"
    );
    assert!(deliveries.is_empty(), "rejected publish routes nowhere");
    assert!(rx112.try_recv().is_err(), "subscriber sees nothing");
    // Alias within range but never registered: still 0x94.
    let (meta2, payload2) = encode_publish_meta_with_alias("", 8, 1, false, max.max(1), b"y");
    let (ack2, deliveries2) =
        apply_publish(&publish_frame(111, 51, meta2, payload2), &shared).await;
    let ack2 = ack2.expect("QoS 1 answers");
    assert_eq!(ack2.metadata[2], protocol_v5::REASON_TOPIC_ALIAS_INVALID);
    assert!(deliveries2.is_empty());
}

#[tokio::test]
async fn topic_alias_over_maximum_orders_disconnect_with_0x94() {
    // CTO ruling B4-05.1: an inbound alias above the CONNACK-negotiated
    // maximum is refused with DISCONNECT 0x94. Drives bind, subscribe,
    // publish and delivery through the broker, never the store alone.
    let shared = test_shared();
    let pub_bind = bind_frame(141, 1, encode_bind_meta("alias-disc-pub", false, 60));
    reply_for_frame(&pub_bind, &shared.sessions).expect("pub binds");
    let (tx141, mut rx141) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(141, tx141);
    let sub_bind = bind_frame(142, 1, encode_bind_meta("alias-disc-sub", false, 60));
    reply_for_frame(&sub_bind, &shared.sessions).expect("sub binds");
    let sub = subscribe_frame(
        142,
        2,
        encode_subscribe_meta(5, "alias-disc-sub", &[("sensors/#", 1)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");
    let (tx142, mut rx142) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(142, tx142);

    let max = shared.sessions.max_topic_alias();
    assert!(max > 0, "test needs aliases enabled");
    let (meta, payload) =
        encode_publish_meta_with_alias("sensors/temp", 7, 1, false, max + 1, b"x");
    let (ack, deliveries) = apply_publish(&publish_frame(141, 50, meta, payload), &shared).await;
    let ack = ack.expect("QoS 1 answers");
    assert_eq!(ack.header.opcode, OpCode::PubAckOut);
    assert_eq!(
        ack.metadata[2],
        protocol_v5::REASON_TOPIC_ALIAS_INVALID,
        "rejection carries 0x94"
    );
    assert!(deliveries.is_empty(), "rejected publish routes nowhere");
    assert!(rx142.try_recv().is_err(), "subscriber sees nothing");
    // The DISCONNECT rides the kernel->edge close channel for the
    // offending connection.
    let close = rx141.try_recv().expect("publisher is disconnected");
    assert_eq!(
        close.header.opcode,
        OpCode::ConnClose,
        "over-maximum alias orders DISCONNECT"
    );
    assert_eq!(close.header.conn_id, 141);
    // The faulty connection holds no session state afterwards.
    assert!(
        shared.sessions.client_id_for_conn(141).is_none(),
        "disconnected connection is detached"
    );
}

#[tokio::test]
async fn topic_alias_explicit_zero_orders_disconnect_with_0x94() {
    // CTO ruling B4-05.1: explicit alias 0 is never valid on the wire
    // (MQTT 5.0 §3.3.2.3.4) and is refused with DISCONNECT 0x94. An
    // absent alias section (pre-alias encoding) still routes normally.
    let shared = test_shared();
    let pub_bind = bind_frame(151, 1, encode_bind_meta("alias-zero-pub", false, 60));
    reply_for_frame(&pub_bind, &shared.sessions).expect("pub binds");
    let (tx151, mut rx151) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(151, tx151);

    // Absent alias section: plain delivery, no disconnect.
    let (meta, payload) = encode_publish_meta("sensors/ok", 0, 0, false, b"x");
    let (_, deliveries) = apply_publish(&publish_frame(151, 60, meta, payload), &shared).await;
    assert!(deliveries.is_empty(), "no subscribers yet");
    assert!(rx151.try_recv().is_err(), "no disconnect for absent alias");

    // Explicit alias 0 section with a concrete topic: QoS 1 gets
    // PUBACK 0x94 plus DISCONNECT.
    let (meta0, payload0) = encode_publish_meta_with_alias("sensors/zero", 7, 1, false, 0, b"x");
    let (ack, deliveries0) = apply_publish(&publish_frame(151, 61, meta0, payload0), &shared).await;
    let ack = ack.expect("QoS 1 answers");
    assert_eq!(ack.header.opcode, OpCode::PubAckOut);
    assert_eq!(
        ack.metadata[2],
        protocol_v5::REASON_TOPIC_ALIAS_INVALID,
        "explicit alias 0 carries 0x94"
    );
    assert!(deliveries0.is_empty());
    let close = rx151.try_recv().expect("explicit alias 0 disconnects");
    assert_eq!(close.header.opcode, OpCode::ConnClose);
    assert_eq!(close.header.conn_id, 151);
}

#[tokio::test]
async fn topic_alias_reconnect_clears_state() {
    // CTO ruling B4-05.1: mappings are per connection and never survive
    // a reconnect. Drives bind, publish, rebind and resolve through
    // the broker.
    let shared = test_shared();
    let bind = bind_frame(161, 1, encode_bind_meta("alias-reconn", false, 60));
    reply_for_frame(&bind, &shared.sessions).expect("bind replies");
    let (meta, payload) = encode_publish_meta_with_alias("re/one", 0, 0, false, 1, b"x");
    let (_, deliveries) = apply_publish(&publish_frame(161, 10, meta, payload), &shared).await;
    assert!(deliveries.is_empty(), "no subscribers yet");
    let session = shared.sessions.get("alias-reconn").expect("session known");
    assert_eq!(session.resolve_inbound_alias(1).unwrap().as_str(), "re/one");
    // Reconnect on a new edge connection renegotiates: tables start
    // empty with the current maxima.
    let rebind = bind_frame(162, 1, encode_bind_meta("alias-reconn", false, 60));
    reply_for_frame(&rebind, &shared.sessions).expect("rebind replies");
    assert!(
        session.resolve_inbound_alias(1).is_none(),
        "alias state dies with the connection"
    );
    assert_eq!(session.inbound_alias_len(), 0);
    assert_eq!(session.outbound_alias_len(), 0);
    assert_eq!(
        session.inbound_alias_max(),
        shared.sessions.max_topic_alias()
    );
}

#[tokio::test]
async fn topic_alias_state_per_connection_and_bounded() {
    // Done-when 4: alias state is per-connection and bounded.
    let shared = test_shared();
    shared.sessions.set_max_topic_alias(4);
    for (conn, client) in [(121u64, "alias-iso-1"), (122u64, "alias-iso-2")] {
        let bind = bind_frame(conn, 1, encode_bind_meta(client, true, 60));
        reply_for_frame(&bind, &shared.sessions).expect("bind replies");
    }
    // Same alias number, different topics, different connections.
    for (conn, seq, topic) in [(121u64, 10u64, "iso/one"), (122u64, 11u64, "iso/two")] {
        let (meta, payload) = encode_publish_meta_with_alias(topic, 0, 0, false, 1, b"x");
        let (_, deliveries) =
            apply_publish(&publish_frame(conn, seq, meta, payload), &shared).await;
        assert!(deliveries.is_empty(), "no subscribers yet");
    }
    let first = shared.sessions.get("alias-iso-1").expect("session known");
    let second = shared.sessions.get("alias-iso-2").expect("session known");
    assert_eq!(first.resolve_inbound_alias(1).unwrap().as_str(), "iso/one");
    assert_eq!(second.resolve_inbound_alias(1).unwrap().as_str(), "iso/two");
    assert!(first.inbound_alias_len() <= 4);
    assert!(second.inbound_alias_len() <= 4);
    // Fill to the bound: further aliases are rejected, never stored.
    for alias in 2..=4u16 {
        let (meta, payload) = encode_publish_meta_with_alias("iso/fill", 0, 0, false, alias, b"x");
        let (_, deliveries) = apply_publish(
            &publish_frame(121, 20 + u64::from(alias), meta, payload),
            &shared,
        )
        .await;
        assert!(deliveries.is_empty());
    }
    assert_eq!(
        first.inbound_alias_len(),
        4,
        "table holds at most the maximum"
    );
    let (meta, payload) = encode_publish_meta_with_alias("iso/over", 0, 0, false, 5, b"x");
    // CTO ruling B4-05.1: an alias past the bound is a protocol error,
    // so it is refused with DISCONNECT 0x94 (the faulty connection is
    // detached and its per-connection state dies with it) rather than
    // kept alive with its table intact.
    let (tx121, mut rx121) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(121, tx121);
    let (_, deliveries) = apply_publish(&publish_frame(121, 99, meta, payload), &shared).await;
    assert!(deliveries.is_empty(), "alias past the bound is refused");
    let close = rx121.try_recv().expect("over-bound alias disconnects");
    assert_eq!(close.header.opcode, OpCode::ConnClose);
    assert!(
        shared.sessions.client_id_for_conn(121).is_none(),
        "disconnected connection is detached"
    );
    assert_eq!(
        first.inbound_alias_len(),
        0,
        "disconnect drops the per-connection table"
    );
}

#[tokio::test]
async fn topic_alias_delivery_workload_timings() {
    use std::hint::black_box;
    use std::time::Instant;
    // Done-when 6: before/after publish-to-deliver numbers with
    // aliases on and off, measured through the broker, not invented.
    // BEFORE is plain delivery (no alias negotiated anywhere, so both
    // the inbound fast path and the outbound skip cost one branch
    // each). AFTER enables the maximum on both directions and routes
    // every publish through alias registration plus aliased delivery.
    // Both print msgs/sec and avg ns/msg into the gate output with no
    // threshold assert; counts are CI sample sizes, not SLOs.
    // Per-message bound: at most `max` alias entries per direction
    // (default 10 topics), each one shared handle plus topic bytes.
    let shared = test_shared();
    let sub_bind = bind_frame(131, 1, encode_bind_meta("alias-time-sub", false, 60));
    reply_for_frame(&sub_bind, &shared.sessions).expect("sub binds");
    let sub = subscribe_frame(
        131,
        2,
        encode_subscribe_meta(5, "alias-time-sub", &[("time/#", 0)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");
    let (tx131, _rx131) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(131, tx131);
    let pub_bind = bind_frame(132, 1, encode_bind_meta("alias-time-pub", false, 60));
    reply_for_frame(&pub_bind, &shared.sessions).expect("pub binds");

    let iters = 300usize;
    let off_start = Instant::now();
    for i in 0..iters {
        let (meta, payload) = encode_publish_meta("time/off", 0, 0, false, b"x");
        let (_, deliveries) =
            apply_publish(&publish_frame(132, 1000 + i as u64, meta, payload), &shared).await;
        assert_eq!(deliveries.len(), 1);
        for (conn_id, frame) in deliveries {
            let _ = shared.conns.route(conn_id, frame);
        }
        // QoS 0 downlinks ride the bounded backlog, not the mailbox.
        let got = shared.conns.pop_qos0(131).expect("downlink arrived");
        black_box(got);
    }
    let off_elapsed = off_start.elapsed();
    let off_rate = iters as f64 / off_elapsed.as_secs_f64();
    let off_avg_ns = off_elapsed.as_nanos() as f64 / iters as f64;
    println!(
        "alias broker delivery workload aliases-off (before): {off_rate:.0} msgs/sec, avg {off_avg_ns:.1} ns/msg ({iters} apply_publish rounds in {off_elapsed:?})"
    );
    println!("BENCH alias_publish_deliver_off {off_avg_ns:.1} ns_per_msg");
    println!("BENCH alias_publish_deliver_off_rate {off_rate:.0} msgs_per_sec");

    // Renegotiate with aliases: publisher registers alias 1 once,
    // then every publish resolves it; the subscriber negotiated a
    // maximum so deliveries carry assigned aliases.
    let sub_session = shared.sessions.get("alias-time-sub").expect("sub known");
    sub_session.set_outbound_alias_max(10);
    let (meta, payload) = encode_publish_meta_with_alias("time/on", 0, 0, false, 1, b"x");
    let (_, deliveries) = apply_publish(&publish_frame(132, 2000, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1);
    for (conn_id, frame) in deliveries {
        let _ = shared.conns.route(conn_id, frame);
    }
    shared
        .conns
        .pop_qos0(131)
        .expect("registration downlink arrived");
    let on_start = Instant::now();
    for i in 0..iters {
        let (meta, payload) = encode_publish_meta_with_alias("", 0, 0, false, 1, b"x");
        let (_, deliveries) =
            apply_publish(&publish_frame(132, 3000 + i as u64, meta, payload), &shared).await;
        assert_eq!(deliveries.len(), 1);
        for (conn_id, frame) in deliveries {
            let _ = shared.conns.route(conn_id, frame);
        }
        let got = shared
            .conns
            .pop_qos0(131)
            .expect("aliased downlink arrived");
        black_box(got);
    }
    let on_elapsed = on_start.elapsed();
    let on_rate = iters as f64 / on_elapsed.as_secs_f64();
    let on_avg_ns = on_elapsed.as_nanos() as f64 / iters as f64;
    println!(
        "alias broker delivery workload aliases-on (after): {on_rate:.0} msgs/sec, avg {on_avg_ns:.1} ns/msg ({iters} aliased apply_publish rounds in {on_elapsed:?})"
    );
    println!("BENCH alias_publish_deliver_on {on_avg_ns:.1} ns_per_msg");
    println!("BENCH alias_publish_deliver_on_rate {on_rate:.0} msgs_per_sec");
}

/// Pipeline benchmark hook (B4-05 HOT-PATH): plain publish-to-deliver
/// rounds through the broker, printing `BENCH` lines for the gates.
/// `#[ignore]` so the pipeline's bench runner picks it up (three runs
/// on the base commit, three on the tree); it uses only broker APIs
/// that exist on the base commit (bind, subscribe, plain publish with
/// no alias section), so it compiles and runs on both. The tree's
/// alias fast path (no alias section: one branch, no lock, no table
/// touch) is what this measures; the on/off comparison lives in
/// `topic_alias_delivery_workload_timings` above.
#[tokio::test]
#[ignore]
async fn topic_alias_bench_baseline() {
    use std::hint::black_box;
    use std::time::Instant;
    let shared = test_shared();
    let sub_bind = bind_frame(171, 1, encode_bind_meta("alias-bench-sub", false, 60));
    reply_for_frame(&sub_bind, &shared.sessions).expect("sub binds");
    let sub = subscribe_frame(
        171,
        2,
        encode_subscribe_meta(5, "alias-bench-sub", &[("bench/#", 0)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");
    let (tx171, _rx171) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(171, tx171);
    let pub_bind = bind_frame(172, 1, encode_bind_meta("alias-bench-pub", false, 60));
    reply_for_frame(&pub_bind, &shared.sessions).expect("pub binds");
    let iters = 200usize;
    let start = Instant::now();
    for i in 0..iters {
        let (meta, payload) = encode_publish_meta("bench/t", 0, 0, false, b"x");
        let (_, deliveries) =
            apply_publish(&publish_frame(172, 1000 + i as u64, meta, payload), &shared).await;
        assert_eq!(deliveries.len(), 1);
        for (conn_id, frame) in deliveries {
            let _ = shared.conns.route(conn_id, frame);
        }
        let got = shared.conns.pop_qos0(171).expect("downlink arrived");
        black_box(got);
    }
    let elapsed = start.elapsed();
    let rate = iters as f64 / elapsed.as_secs_f64();
    let avg_ns = elapsed.as_nanos() as f64 / iters as f64;
    println!(
        "alias broker delivery baseline: {rate:.0} msgs/sec, avg {avg_ns:.1} ns/msg ({iters} apply_publish rounds in {elapsed:?})"
    );
    println!("BENCH alias_publish_deliver_baseline {avg_ns:.1} ns_per_msg");
    println!("BENCH alias_publish_deliver_baseline_rate {rate:.0} msgs_per_sec");
}
