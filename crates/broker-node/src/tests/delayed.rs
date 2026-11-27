use super::*;
use crate::link_harness::handle_connection;
use tokio::net::TcpListener;

/// Drive one delayed publish through `apply_publish` with a live
/// mailbox registered, returning the immediate reply.
#[cfg(test)]
async fn delayed_setup(
    shared: &Shared,
) -> (
    tokio::sync::mpsc::UnboundedReceiver<BrokerFrame>,
    Option<BrokerFrame>,
) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    shared.conns.register(951, tx);
    // D1-02: label for per-client shed accounting; QoS 0 delayed
    // deliveries ride the bounded backlog, not the guaranteed mailbox.
    shared.conns.set_client_label(951, "delayed-sub");
    let (session, _) = shared.sessions.get_or_create("delayed-sub", true);
    *session.conn_id.write() = Some(951);
    let sub = subscribe_frame(
        951,
        1,
        encode_subscribe_meta(1, "delayed-sub", &[("alerts", 0)]),
    );
    let (reply, _) = apply_subscribe(&sub, shared).await;
    reply.expect("subscribe replies");
    // The single shared driver serves the wheel; without it nothing
    // fires (one task per node, never one task per message).
    shared.spawn_delayed_driver();
    bind_test_publisher(shared, 952);
    let (meta, payload) = encode_publish_meta("$delayed/1/alerts", 0, 0, false, b"later");
    let ack = apply_publish(&publish_frame(952, 1, meta, payload), shared).await;
    (rx, ack.0)
}

#[tokio::test]
async fn delayed_publish_defers_delivery_by_one_second() {
    let shared = test_shared();
    let start = std::time::Instant::now();
    let (mut rx, ack) = delayed_setup(&shared).await;
    // QoS 0: no ack, and crucially no immediate delivery on either
    // mailbox (guaranteed `rx` stays empty; bounded QoS 0 backlog
    // stays empty until the timer fires).
    assert!(ack.is_none());
    let early = tokio::time::timeout(std::time::Duration::from_millis(300), rx.recv()).await;
    assert!(early.is_err(), "delayed message must not arrive early");
    assert_eq!(
        shared.conns.qos0_len(951),
        0,
        "bounded backlog must not hold the delayed frame early"
    );

    // ...but it arrives after the delay with the inner topic on the
    // bounded QoS 0 backlog (D1-02: QoS 0 no longer rides `rx`).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let routed = loop {
        if let Some(frame) = shared.conns.pop_qos0(951) {
            break frame;
        }
        if std::time::Instant::now() > deadline {
            panic!("delivery fires");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };
    assert_eq!(routed.header.opcode, OpCode::PublishOut);
    assert!(start.elapsed() >= std::time::Duration::from_millis(900));
    let (topic, _, _, _) = decode_publish_meta_parts(&routed.metadata);
    assert_eq!(topic, "alerts");
    assert_eq!(routed.payload, Bytes::from_static(b"later"));
    // Guaranteed mailbox never saw the QoS 0 frame.
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn delayed_publish_rejects_garbage() {
    let shared = test_shared();
    bind_test_publisher(&shared, 953);
    // Non-numeric delay is not a delayed target at all: it publishes
    // literally (no subscribers exist for it here).
    let (meta, payload) = encode_publish_meta("$delayed/soon/t", 0, 0, false, b"x");
    let (ack, deliveries) = apply_publish(&publish_frame(953, 1, meta, payload), &shared).await;
    assert!(ack.is_none());
    assert!(deliveries.is_empty());

    // Absurd delays are dropped, not scheduled, and counted.
    let over_before = shared.delayed.dropped_over_limit();
    let (meta, payload) = encode_publish_meta("$delayed/99999999/t", 0, 0, false, b"x");
    let (ack, deliveries) = apply_publish(&publish_frame(953, 2, meta, payload), &shared).await;
    assert!(ack.is_none());
    assert!(deliveries.is_empty());
    assert_eq!(shared.delayed.dropped_over_limit(), over_before + 1);
    assert!(shared.delayed.is_empty());

    // An inner topic that is not a servable concrete topic is
    // dropped with a warning and a counter, never queued.
    let malformed_before = shared.delayed.dropped_malformed();
    let (meta, payload) = encode_publish_meta("$delayed/1/#/x", 0, 0, false, b"x");
    let (ack, deliveries) = apply_publish(&publish_frame(953, 3, meta, payload), &shared).await;
    assert!(ack.is_none());
    assert!(deliveries.is_empty());
    assert_eq!(shared.delayed.dropped_malformed(), malformed_before + 1);
    assert!(shared.delayed.is_empty());

    // Delayed markers are not subscribable.
    let sub = subscribe_frame(
        954,
        1,
        encode_subscribe_meta(1, "c", &[("$delayed/1/t", 0)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    let reply = reply.expect("suback replies");
    assert_eq!(&reply.metadata[2..], &[0x80u8]);
}

#[tokio::test]
async fn delayed_message_lists_and_removal_stops_delivery() {
    // AP-04: a client-published delayed message waits on the shared
    // scheduler (the management list reads it there), and removing
    // it stops the delivery while other entries still fire.
    let shared = test_shared();
    let (session, _) = shared.sessions.get_or_create("ap04-pub", true);
    *session.conn_id.write() = Some(941);
    *session.username.write() = Some("ap04-user".to_string());

    let (meta, payload) = encode_publish_meta("$delayed/600/ap04/one", 0, 0, false, b"one");
    let (ack, deliveries) = apply_publish(&publish_frame(941, 1, meta, payload), &shared).await;
    assert!(ack.is_none());
    assert!(deliveries.is_empty());
    let (meta, payload) = encode_publish_meta("$delayed/600/ap04/two", 0, 0, false, b"two");
    let (_, deliveries) = apply_publish(&publish_frame(941, 2, meta, payload), &shared).await;
    assert!(deliveries.is_empty());

    // Both publishes wait on the scheduler with client and user names.
    let waiting = shared.delayed.snapshot();
    assert_eq!(waiting.len(), 2);
    for entry in &waiting {
        assert!(entry.topic.starts_with("ap04/"));
        assert_eq!(entry.publisher.as_deref(), Some("ap04-pub"));
        assert_eq!(entry.username.as_deref(), Some("ap04-user"));
    }

    // A due entry that is removed never fires; a kept one does.
    let now = broker_storage::now_ms();
    let gone = DelayedEntry {
        id: shared.delayed.next_id(),
        deliver_at_ms: now.saturating_sub(1_000),
        publish_at_ms: now.saturating_sub(61_000),
        topic: "ap04/gone".to_string(),
        qos: 0,
        retain: false,
        payload: b"x".to_vec(),
        publisher: None,
        username: None,
        publisher_tenant: broker_session::tenant::DEFAULT_TENANT_ID.to_string(),
    };
    let gone_id = gone.id;
    shared
        .delayed
        .persist_and_schedule(gone, 60)
        .expect("due entry queues");
    let kept = DelayedEntry {
        id: shared.delayed.next_id(),
        deliver_at_ms: now.saturating_sub(1_000),
        publish_at_ms: now.saturating_sub(61_000),
        topic: "ap04/kept".to_string(),
        qos: 0,
        retain: false,
        payload: b"x".to_vec(),
        publisher: None,
        username: None,
        publisher_tenant: broker_session::tenant::DEFAULT_TENANT_ID.to_string(),
    };
    let kept_id = kept.id;
    shared
        .delayed
        .persist_and_schedule(kept, 60)
        .expect("due entry queues");
    assert!(shared.delayed.remove_one(gone_id));
    assert!(!shared.delayed.remove_one(gone_id));
    let due = shared.delayed.tick_due();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].id, kept_id);
    // The two published entries still wait with 600 s deadlines.
    assert_eq!(shared.delayed.snapshot().len(), 2);
}

#[tokio::test]
async fn delayed_publish_survives_restart_from_same_dir() {
    // Persistence lives behind the wheel in `<data-dir>/delayed.jsonl`
    // (written by `DelayedScheduler::persist_and_schedule` in
    // `broker-delayed`, consulted by `DelayedScheduler::load`): the
    // publish event drives the write, the boot path drives the
    // reload, and the driver delivers on schedule afterwards.
    let dir = unique_data_dir();
    std::fs::create_dir_all(&dir).expect("scratch data dir");
    let data_dir = dir.to_str().expect("temp path is UTF-8").to_string();

    // First incarnation: publish through the broker but never start
    // its driver, so the entry stays pending in the file.
    // Delay 6 s keeps the entry pending across reload on a loaded runner.
    let pending = {
        let shared = test_shared();
        shared.delayed.set_persist_dir(&data_dir);
        assert_eq!(shared.delayed.load(), (0, 0, 0));
        let (meta, payload) =
            encode_publish_meta("$delayed/6/restart/alerts", 0, 0, false, b"restart-me");
        bind_test_publisher(&shared, 957);
        let (ack, deliveries) = apply_publish(&publish_frame(957, 1, meta, payload), &shared).await;
        assert!(ack.is_none());
        assert!(deliveries.is_empty());
        assert_eq!(shared.delayed.len(), 1);
        assert!(
            dir.join("delayed.jsonl").is_file(),
            "the publish event must persist before ack"
        );
        shared.delayed.scheduled_count()
    };
    assert_eq!(pending, 1);

    // Second incarnation from the same directory: reload, subscribe,
    // then deliver on schedule through the broker.
    let shared = test_shared();
    shared.delayed.set_persist_dir(&data_dir);
    let (loaded, torn, expired) = shared.delayed.load();
    assert_eq!(loaded, 1);
    assert_eq!((torn, expired), (0, 0));
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    shared.conns.register(958, tx);
    shared.conns.set_client_label(958, "restart-sub");
    let (session, _) = shared.sessions.get_or_create("restart-sub", true);
    *session.conn_id.write() = Some(958);
    let sub = subscribe_frame(
        958,
        1,
        encode_subscribe_meta(1, "restart-sub", &[("restart/alerts", 0)]),
    );
    apply_subscribe(&sub, &shared).await.0.expect("subscribe");
    shared.spawn_delayed_driver();
    // Wait 15 s covers the 6 s delay plus scheduling slack.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let routed = loop {
        if let Some(frame) = shared.conns.pop_qos0(958) {
            break frame;
        }
        if std::time::Instant::now() > deadline {
            panic!("restarted node must still deliver on schedule");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };
    assert_eq!(routed.header.opcode, OpCode::PublishOut);
    let (topic, _, _, _) = decode_publish_meta_parts(&routed.metadata);
    assert_eq!(topic, "restart/alerts");
    assert_eq!(routed.payload, Bytes::from_static(b"restart-me"));
    assert_eq!(shared.delayed.delivered_count(), 1);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn delayed_publish_end_to_end_over_tcp() {
    let mut shared = Shared::new();
    shared.allow_anonymous = true;
    shared.spawn_delayed_driver();
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

    let sub = bind_client(addr, 961, "patient", true).await;
    sub.send(subscribe_frame(
        961,
        2,
        encode_subscribe_meta(4, "patient", &[("alerts", 0)]),
    ))
    .await
    .expect("subscribe");
    assert_eq!(
        sub.recv().await.expect("suback").header.opcode,
        OpCode::SubAckOut
    );

    let publ = bind_client(addr, 962, "procrastinator", true).await;
    let start = std::time::Instant::now();
    let (meta, payload) = encode_publish_meta("$delayed/1/alerts", 0, 0, false, b"finally");
    publ.send(publish_frame(962, 2, meta, payload))
        .await
        .expect("publish delayed");

    let routed = sub.recv().await.expect("recv delayed");
    assert_eq!(routed.header.opcode, OpCode::PublishOut);
    assert_eq!(routed.header.conn_id, 961);
    assert_eq!(routed.payload, Bytes::from_static(b"finally"));
    let (topic, _, _, _) = decode_publish_meta_parts(&routed.metadata);
    assert_eq!(topic, "alerts");
    assert!(
        start.elapsed() >= std::time::Duration::from_millis(900),
        "delivery must honor the 1s delay"
    );

    server.abort();
}

#[tokio::test]
async fn delayed_delivery_workload_timings() {
    use std::hint::black_box;
    use std::time::Instant;

    // B4-03 delayed-delivery workload before/after numbers (spec
    // Done-when bullet 5, RULEBOOK hot-path row): measured through the
    // broker publish event (`apply_publish`), not just the wheel.
    // BEFORE is the steady-state immediate publish-to-delivery loop
    // (no wheel lock, no file append, no fsync): its rate is the
    // pre-change baseline. AFTER is the delayed-schedule loop through
    // `$delayed` (one file_lock hold, one bounded append plus fsync,
    // two short wheel-lock holds, one payload copy). Both print
    // msgs/sec and avg ns/msg into the gate output with no threshold
    // assert; counts are CI sample sizes, not SLOs. On-time behaviour
    // (requested vs actual delay) and pending memory (wheel depth,
    // persisted file bytes) print alongside. Publish-path cost prose
    // lives in `broker-delayed`; this test is the measured numbers behind
    // it.
    let shared = test_shared();
    let (tx_base, _rx_base) = tokio::sync::mpsc::unbounded_channel();
    shared.conns.register(971, tx_base);
    shared.conns.set_client_label(971, "wload-base");
    let (session_base, _) = shared.sessions.get_or_create("wload-base", true);
    *session_base.conn_id.write() = Some(971);
    let sub = subscribe_frame(
        971,
        1,
        encode_subscribe_meta(1, "wload-base", &[("wload/base", 0)]),
    );
    apply_subscribe(&sub, &shared).await.0.expect("subscribe");

    // BEFORE: immediate publish-to-delivery, no delayed work.
    let steady_iters = 200usize;
    let steady_start = Instant::now();
    bind_test_publisher(&shared, 970);
    for i in 0..steady_iters {
        let (meta, payload) = encode_publish_meta("wload/base", 0, 0, false, b"x");
        let (_, deliveries) =
            apply_publish(&publish_frame(970, 1000 + i as u64, meta, payload), &shared).await;
        for (conn_id, frame) in deliveries {
            let _ = shared.conns.route(conn_id, frame);
        }
        let got = shared.conns.pop_qos0(971).expect("live downlink arrived");
        black_box(got);
    }
    let steady_elapsed = steady_start.elapsed();
    let steady_rate = steady_iters as f64 / steady_elapsed.as_secs_f64();
    let steady_avg_ns = steady_elapsed.as_nanos() as f64 / steady_iters as f64;
    println!(
        "delayed broker workload immediate baseline (before): {steady_rate:.0} msgs/sec, avg {steady_avg_ns:.1} ns/msg ({steady_iters} apply_publish rounds in {steady_elapsed:?})"
    );

    // AFTER: delayed schedule through the broker with persistence.
    let dir = unique_data_dir();
    std::fs::create_dir_all(&dir).expect("scratch data dir");
    let data_dir = dir.to_str().expect("temp path is UTF-8").to_string();
    shared.delayed.set_persist_dir(&data_dir);
    assert_eq!(shared.delayed.load(), (0, 0, 0));
    let payload_bytes = b"x".len();
    let delayed_iters = 200usize;
    let delayed_start = Instant::now();
    for i in 0..delayed_iters {
        let (meta, payload) = encode_publish_meta("$delayed/60/wload/delayed", 0, 0, false, b"x");
        let (ack, deliveries) =
            apply_publish(&publish_frame(970, 5000 + i as u64, meta, payload), &shared).await;
        black_box(ack);
        assert!(deliveries.is_empty(), "delayed schedule defers delivery");
    }
    let delayed_elapsed = delayed_start.elapsed();
    assert_eq!(shared.delayed.len(), delayed_iters);
    let delayed_rate = delayed_iters as f64 / delayed_elapsed.as_secs_f64();
    let delayed_avg_ns = delayed_elapsed.as_nanos() as f64 / delayed_iters as f64;
    let file_bytes = std::fs::metadata(dir.join("delayed.jsonl"))
        .map(|m| m.len())
        .unwrap_or(0);
    let estimated_payload_bytes = (delayed_iters * payload_bytes) as u64;
    println!(
        "delayed broker schedule workload (wheel plus file append, after): {delayed_rate:.0} msgs/sec, avg {delayed_avg_ns:.1} ns/msg ({delayed_iters} apply_publish rounds in {delayed_elapsed:?})"
    );
    println!(
        "delayed pending memory: wheel depth {delayed_iters} entries, persisted file {file_bytes} bytes, estimated payload {estimated_payload_bytes} bytes ({delayed_iters} entries)"
    );

    // On-time behaviour: short-delay batch through the broker driver.
    let otime = test_shared();
    let (tx_ot, _rx_ot) = tokio::sync::mpsc::unbounded_channel();
    otime.conns.register(972, tx_ot);
    otime.conns.set_client_label(972, "wload-ontime");
    let (session_ot, _) = otime.sessions.get_or_create("wload-ontime", true);
    *session_ot.conn_id.write() = Some(972);
    let sub = subscribe_frame(
        972,
        1,
        encode_subscribe_meta(1, "wload-ontime", &[("wload/ontime", 0)]),
    );
    apply_subscribe(&sub, &otime).await.0.expect("subscribe");
    otime.spawn_delayed_driver();
    let ontime_count = 5usize;
    let ontime_start = Instant::now();
    bind_test_publisher(&otime, 973);
    for i in 0..ontime_count {
        let (meta, payload) = encode_publish_meta("$delayed/1/wload/ontime", 0, 0, false, b"y");
        let (_, deliveries) =
            apply_publish(&publish_frame(973, 9000 + i as u64, meta, payload), &otime).await;
        assert!(deliveries.is_empty(), "delayed schedule defers delivery");
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(8);
    let mut received = 0usize;
    while received < ontime_count {
        if otime.conns.pop_qos0(972).is_some() {
            received += 1;
            continue;
        }
        if std::time::Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let ontime_elapsed = ontime_start.elapsed();
    let ontime_ms = ontime_elapsed.as_millis() as u64;
    // Requested 1000 ms plus one 100 ms tick of firing slack: late
    // only past that window; early only before the delay.
    let ontime_ok = received == ontime_count && (900..=8_000).contains(&ontime_ms);
    println!(
        "delayed on-time delivery: {received}/{ontime_count} delivered in {ontime_elapsed:?} (requested 1000 ms, on_time={ontime_ok})"
    );
    assert_eq!(received, ontime_count, "short-delay batch must fire");
    assert!(
        ontime_ms >= 900,
        "delivery must honor the 1s delay (got {ontime_elapsed:?})"
    );
    std::fs::remove_dir_all(&dir).ok();
}
