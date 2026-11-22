use super::*;
use crate::link_harness::handle_connection;
use tokio::net::TcpListener;

#[tokio::test]
async fn offline_queue_replays_on_durable_reconnect() {
    let shared = test_shared();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let server = tokio::spawn(async move {
        for _ in 0..3 {
            let (stream, _) = listener.accept().await.expect("accept");
            let shared = shared.clone();
            tokio::spawn(async move {
                handle_connection(stream, shared).await.expect("handle");
            });
        }
        std::future::pending::<()>().await;
    });

    // Client A connects durably and subscribes.
    let sub = bind_client(addr, 601, "worker-a", false).await;
    sub.send(subscribe_frame(
        601,
        2,
        encode_subscribe_meta(3, "worker-a", &[("job/queue", 1)]),
    ))
    .await
    .expect("subscribe");
    let suback = sub.recv().await.expect("recv suback");
    assert_eq!(suback.header.opcode, OpCode::SubAckOut);

    // Unclean disconnect: TCP drops without DISCONNECT. Give the
    // server a beat to detach the session before publishing.
    drop(sub);
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    // Client B publishes QoS 1 while A is detached.
    let publ = bind_client(addr, 602, "producer-b", true).await;
    let (meta, payload) = encode_publish_meta("job/queue", 9, 1, false, b"work-item");
    publ.send(publish_frame(602, 2, meta, payload))
        .await
        .expect("publish");
    let ack = publ.recv().await.expect("recv puback");
    assert_eq!(ack.header.opcode, OpCode::PubAckOut);

    // Client A reconnects durably on a new connection.
    let io = tokio::net::TcpStream::connect(addr)
        .await
        .expect("reconnect");
    let resumed = FramedTransport::new(io);
    resumed
        .send(bind_frame(603, 1, encode_bind_meta("worker-a", false, 60)))
        .await
        .expect("rebind");
    let binding = resumed.recv().await.expect("recv binding");
    assert_eq!(binding.header.opcode, OpCode::SessionBinding);
    let (_, present, rc) = decode_session_binding_meta(&binding.metadata);
    assert!(present, "resumed session must report session_present");
    assert_eq!(rc, 0);

    // The queued message replays onto the new connection.
    let replayed = resumed.recv().await.expect("recv replay");
    assert_eq!(replayed.header.opcode, OpCode::PublishOut);
    assert_eq!(replayed.header.conn_id, 603);
    assert_eq!(replayed.payload, Bytes::from_static(b"work-item"));
    let (topic, packet_id, qos, _) = decode_publish_meta_parts(&replayed.metadata);
    assert_eq!(topic, "job/queue");
    assert_eq!(qos, 1);
    assert_ne!(packet_id, 0, "replayed QoS 1 needs a fresh packet id");

    server.abort();
}

#[tokio::test]
async fn offline_queue_survives_restart_through_broker() {
    // B4-06: buffer through the broker, recreate the node from the
    // same directory, reconnect, and assert every queued message
    // redelivers in order. The buffering write runs inside
    // `push_offline_with_limit` on the disconnect-buffer event
    // (`build_downlink_frames`, detached durable branch); the reload
    // runs at boot (`restore_offline_queues`); the handoff runs on
    // the reconnect-replay event (`replay_offline`).
    let dir = unique_data_dir();
    let offline_dir = dir.join("offline");
    {
        let shared = test_shared();
        shared.sessions.set_offline_store(Arc::new(
            OfflineQueueStore::open(&offline_dir).expect("open offline store"),
        ));
        let bind = bind_frame(61, 1, encode_bind_meta("rest-sub", false, 60));
        reply_for_frame(&bind, &shared.sessions).expect("bind replies");
        let sub = subscribe_frame(
            61,
            2,
            encode_subscribe_meta(5, "rest-sub", &[("job/queue", 1)]),
        );
        let (reply, _) = apply_subscribe(&sub, &shared).await;
        reply.expect("subscribe replies");
        // Detach without a clean disconnect: the session stays durable.
        let mut unbind_meta = vec![0x00u8, 0x08u8];
        unbind_meta.extend_from_slice(b"rest-sub");
        let unbind = BrokerFrame::new(
            OpCode::UnbindConnection,
            61,
            4,
            Bytes::from(unbind_meta),
            Bytes::new(),
        )
        .expect("valid unbind frame");
        apply_unbind(&unbind, &shared);
        // Five QoS 1 publishes buffer while detached. Each `await`
        // returns after the durable append: the publisher's ack is
        // only observed once the entry has reached stable storage.
        bind_test_publisher(&shared, 62);
        for i in 0..5u8 {
            let (meta, payload) = encode_publish_meta("job/queue", 9, 1, false, &[i]);
            let (ack, _) = apply_publish(
                &publish_frame(62, u64::from(i) + 10, meta, payload),
                &shared,
            )
            .await;
            ack.expect("QoS 1 publisher is acked");
        }
        let session = shared.sessions.get("rest-sub").expect("session known");
        assert_eq!(session.offline_len(), 5);
    }
    // Recreate the node from the same directory: the backlog rebuilds
    // detached, oldest first.
    let shared = test_shared();
    shared.sessions.set_offline_store(Arc::new(
        OfflineQueueStore::open(&offline_dir).expect("reopen offline store"),
    ));
    let stats = shared.sessions.restore_offline_queues();
    assert_eq!(stats.messages, 5);
    assert_eq!(stats.clients, 1);
    assert_eq!(stats.torn, 0);
    assert_eq!(stats.capped_dropped, 0);
    let session = shared.sessions.get("rest-sub").expect("restored session");
    assert!(!*session.connected.read());
    assert_eq!(session.offline_len(), 5);
    // Reconnect on a new connection: every queued message replays.
    let rebind = bind_frame(63, 1, encode_bind_meta("rest-sub", false, 60));
    reply_for_frame(&rebind, &shared.sessions).expect("rebind replies");
    let (tx63, mut rx63) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(63, tx63);
    let replayed = replay_offline(&rebind, &shared).await;
    assert_eq!(replayed, 5);
    for i in 0..5u8 {
        let frame = rx63.try_recv().expect("replay reached the new mailbox");
        assert_eq!(frame.payload, Bytes::from(vec![i]));
        let (topic, _, qos, _) = decode_publish_meta_parts(&frame.metadata);
        assert_eq!(topic, "job/queue");
        assert_eq!(qos, 1);
    }
    assert!(rx63.try_recv().is_err());
    // The drain deleted the queue file: a further restart replays nothing.
    assert!(shared
        .sessions
        .offline_store()
        .expect("store installed")
        .client_ids()
        .is_empty());
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn offline_queue_durable_drop_policy_holds_and_counts() {
    // B4-06: the configurable drop policy (oldest dropped past the
    // cap, counted drops) still holds with the durable store
    // installed, and the survivors are what a restart reloads.
    let dir = unique_data_dir();
    let offline_dir = dir.join("offline");
    let sessions = Arc::new(SessionManager::new_with_limits(Some(3)));
    sessions.set_offline_store(Arc::new(
        OfflineQueueStore::open(&offline_dir).expect("open offline store"),
    ));
    let router = Router::new();
    let metrics = Metrics::new();
    let filter = TopicFilter::new("jobs/backlog").unwrap();
    router.subscribe(
        &filter,
        Subscription::new("durable-tiny", 78, QoS::AtMostOnce),
    );
    let (session, _) = sessions.get_or_create("durable-tiny", false);
    *session.connected.write() = false;
    *session.conn_id.write() = None;
    let topic = Topic::new("jobs/backlog").unwrap();
    for i in 0..5u8 {
        let deliveries = build_downlink_frames(
            &router,
            &sessions,
            &metrics,
            &topic,
            QoS::AtMostOnce,
            false,
            &Bytes::from(vec![i]),
            broker_session::tenant::DEFAULT_TENANT_ID,
            None,
        );
        assert!(deliveries.is_empty());
    }
    assert_eq!(session.offline_len(), 3);
    assert_eq!(metrics.offline_queue_evicted(), 2);
    // A fresh node on the same directory reloads the newest three.
    let fresh = SessionManager::new_with_limits(Some(3));
    fresh.set_offline_store(Arc::new(
        OfflineQueueStore::open(&offline_dir).expect("reopen offline store"),
    ));
    let stats = fresh.restore_offline_queues();
    assert_eq!(stats.messages, 3);
    assert_eq!(stats.capped_dropped, 0);
    let restored = fresh.get("durable-tiny").expect("restored session");
    let drained = restored.drain_offline();
    assert_eq!(drained.len(), 3);
    assert_eq!(drained[0].payload, Bytes::from(vec![2u8]));
    assert_eq!(drained[2].payload, Bytes::from(vec![4u8]));
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn offline_queue_torn_tail_discarded_and_counted() {
    // B4-06: a torn tail on recovery is discarded with a counter and
    // never served; everything before it still replays in order.
    let dir = unique_data_dir();
    let offline_dir = dir.join("offline");
    {
        let shared = test_shared();
        shared.sessions.set_offline_store(Arc::new(
            OfflineQueueStore::open(&offline_dir).expect("open offline store"),
        ));
        let bind = bind_frame(61, 1, encode_bind_meta("torn-sub", false, 60));
        reply_for_frame(&bind, &shared.sessions).expect("bind replies");
        let sub = subscribe_frame(
            61,
            2,
            encode_subscribe_meta(5, "torn-sub", &[("torn/topic", 1)]),
        );
        let (reply, _) = apply_subscribe(&sub, &shared).await;
        reply.expect("subscribe replies");
        let mut unbind_meta = vec![0x00u8, 0x08u8];
        unbind_meta.extend_from_slice(b"torn-sub");
        let unbind = BrokerFrame::new(
            OpCode::UnbindConnection,
            61,
            4,
            Bytes::from(unbind_meta),
            Bytes::new(),
        )
        .expect("valid unbind frame");
        apply_unbind(&unbind, &shared);
        bind_test_publisher(&shared, 62);
        for i in 0..5u8 {
            let (meta, payload) = encode_publish_meta("torn/topic", 9, 1, false, &[i]);
            let _ = apply_publish(
                &publish_frame(62, u64::from(i) + 20, meta, payload),
                &shared,
            )
            .await;
        }
        assert_eq!(
            shared
                .sessions
                .get("torn-sub")
                .expect("session")
                .offline_len(),
            5
        );
    }
    // Tear the tail: cut the last bytes of the single queue file so
    // the final frame loses its checksum.
    let mut logs = Vec::new();
    for entry in std::fs::read_dir(&offline_dir).expect("list offline dir") {
        let entry = entry.expect("dir entry");
        if entry.path().extension().and_then(|ext| ext.to_str()) == Some("log") {
            logs.push(entry.path());
        }
    }
    assert_eq!(logs.len(), 1, "one queue file must exist");
    let len = std::fs::metadata(&logs[0]).expect("queue file size").len();
    assert!(len > 16);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&logs[0])
        .expect("open queue file")
        .set_len(len - 6)
        .expect("tear the tail");
    // Recreate the node: the torn frame is gone and counted, the four
    // survivors replay in order.
    let shared = test_shared();
    let store = Arc::new(OfflineQueueStore::open(&offline_dir).expect("reopen store"));
    shared.sessions.set_offline_store(store.clone());
    let stats = shared.sessions.restore_offline_queues();
    assert_eq!(stats.torn, 1);
    assert_eq!(store.recovery_torn(), 1);
    assert_eq!(stats.messages, 4);
    let rebind = bind_frame(63, 1, encode_bind_meta("torn-sub", false, 60));
    reply_for_frame(&rebind, &shared.sessions).expect("rebind replies");
    let (tx63, mut rx63) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(63, tx63);
    assert_eq!(replay_offline(&rebind, &shared).await, 4);
    for i in 0..4u8 {
        let frame = rx63.try_recv().expect("survivor replayed");
        assert_eq!(frame.payload, Bytes::from(vec![i]));
    }
    assert!(rx63.try_recv().is_err());
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn offline_durable_reconnect_workload_timings() {
    // B4-06 timing record: the disconnect-reconnect workload before
    // (memory-only buffering, the pre-change path) and after (durable
    // append plus fsync per buffered message), plus restore and
    // reconnect-replay rates. Live fan-out to connected sessions takes
    // no new work in either case: files are touched only for detached
    // durable matches. Every section prints its measured rate into
    // the gate output with no threshold assert; counts are CI sample
    // sizes, not SLOs.
    use std::hint::black_box;
    use std::time::Instant;

    let topic = Topic::new("wload/q").unwrap();
    let payload = Bytes::from_static(b"x");
    let filter = TopicFilter::new("wload/q").unwrap();

    // BEFORE: memory-only disconnect buffering.
    let router = Router::new();
    router.subscribe(&filter, Subscription::new("wload-mem", 81, QoS::AtMostOnce));
    let sessions = SessionManager::new_with_limits(None);
    let metrics = Metrics::new();
    let (session, _) = sessions.get_or_create("wload-mem", false);
    *session.connected.write() = false;
    *session.conn_id.write() = None;
    let iters = 2_000usize;
    let start = Instant::now();
    for _ in 0..iters {
        let deliveries = build_downlink_frames(
            &router,
            &sessions,
            &metrics,
            &topic,
            QoS::AtMostOnce,
            false,
            black_box(&payload),
            broker_session::tenant::DEFAULT_TENANT_ID,
            None,
        );
        assert!(deliveries.is_empty());
    }
    let elapsed = start.elapsed();
    assert_eq!(session.offline_len(), iters);
    let rate = iters as f64 / elapsed.as_secs_f64();
    let avg_ns = elapsed.as_nanos() as f64 / iters as f64;
    println!(
        "offline buffer memory-only disconnect-buffer (before): {rate:.0} msgs/sec, avg {avg_ns:.1} ns/msg ({iters} buffered in {elapsed:?})"
    );
    let start = Instant::now();
    let drained = session.drain_offline();
    let elapsed = start.elapsed();
    assert_eq!(drained.len(), iters);
    let rate = iters as f64 / elapsed.as_secs_f64();
    let avg_ns = elapsed.as_nanos() as f64 / iters as f64;
    println!(
        "offline drain memory-only (before): {rate:.0} msgs/sec, avg {avg_ns:.1} ns/msg ({iters} drained in {elapsed:?})"
    );

    // AFTER: durable disconnect buffering (fsync per append).
    let dir = unique_data_dir();
    let offline_dir = dir.join("offline");
    let router2 = Router::new();
    router2.subscribe(
        &filter,
        Subscription::new("wload-disk", 82, QoS::AtMostOnce),
    );
    let sessions2 = SessionManager::new_with_limits(None);
    sessions2.set_offline_store(Arc::new(
        OfflineQueueStore::open(&offline_dir).expect("open offline store"),
    ));
    let metrics2 = Metrics::new();
    let (session2, _) = sessions2.get_or_create("wload-disk", false);
    *session2.connected.write() = false;
    *session2.conn_id.write() = None;
    let disk_iters = 200usize;
    let start = Instant::now();
    for _ in 0..disk_iters {
        let deliveries = build_downlink_frames(
            &router2,
            &sessions2,
            &metrics2,
            &topic,
            QoS::AtMostOnce,
            false,
            black_box(&payload),
            broker_session::tenant::DEFAULT_TENANT_ID,
            None,
        );
        assert!(deliveries.is_empty());
    }
    let elapsed = start.elapsed();
    assert_eq!(session2.offline_len(), disk_iters);
    let rate = disk_iters as f64 / elapsed.as_secs_f64();
    let avg_us = elapsed.as_micros() as f64 / disk_iters as f64;
    println!(
        "offline buffer durable fsync-per-append (after): {rate:.0} msgs/sec, avg {avg_us:.1} us/msg ({disk_iters} buffered in {elapsed:?})"
    );

    // Reconnect half on a fresh node from the same directory: restore
    // rate, then rebind plus replay rate onto a live mailbox.
    let shared3 = test_shared();
    shared3.sessions.set_offline_store(Arc::new(
        OfflineQueueStore::open(&offline_dir).expect("reopen offline store"),
    ));
    let start = Instant::now();
    let stats = shared3.sessions.restore_offline_queues();
    let elapsed = start.elapsed();
    assert_eq!(stats.messages, disk_iters);
    let rate = disk_iters as f64 / elapsed.as_secs_f64();
    println!("offline restore from disk: {rate:.0} msgs/sec ({disk_iters} rebuilt in {elapsed:?})");
    let rebind = bind_frame(83, 1, encode_bind_meta("wload-disk", false, 60));
    let rebind_start = Instant::now();
    reply_for_frame(&rebind, &shared3.sessions).expect("rebind replies");
    let (tx83, _rx83) = unbounded_channel::<BrokerFrame>();
    shared3.conns.register(83, tx83);
    let replayed = replay_offline(&rebind, &shared3).await;
    let rebind_elapsed = rebind_start.elapsed();
    assert_eq!(replayed, disk_iters);
    let rate = disk_iters as f64 / rebind_elapsed.as_secs_f64();
    let avg_us = rebind_elapsed.as_micros() as f64 / disk_iters as f64;
    println!(
        "offline reconnect rebind-plus-replay: {rate:.0} msgs/sec, avg {avg_us:.1} us/msg ({disk_iters} replayed in {rebind_elapsed:?})"
    );
    // QoS 0 replays ride the bounded backlog, not the mailbox.
    for _ in 0..disk_iters {
        black_box(shared3.conns.pop_qos0(83).expect("replay arrived"));
    }
    std::fs::remove_dir_all(&dir).ok();
}

/// B1-07: a publish through the real broker ingress lands in the
/// durable stream journal without blocking delivery.
///
/// Attaches a journal backed by a scratch directory, publishes via
/// `ingress_pipeline` (the same path MQTT and CoAP publishes take),
/// and asserts the background writer persisted the record at offset 0
/// with the right payload. Fails when the store is never wired in
/// (nothing to read back); passes once the hook enqueues off the hot
/// path. Also asserts a restart of the store from the same directory
/// still serves the journalled record.
#[tokio::test]
async fn publish_is_journalled_to_durable_stream_off_hot_path() {
    let dir = {
        static SLOT: AtomicU64 = AtomicU64::new(0);
        let slot = SLOT.fetch_add(1, Ordering::SeqCst);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!(
            "indramqtt-stream-journal-{}-{nanos}-{slot}",
            std::process::id()
        ))
    };
    let mut shared = Shared::new();
    let journal = StreamJournalHandle::open(&dir, StreamConfig::default()).expect("open journal");
    let store = journal.store.clone();
    shared.stream_journal = Some(journal.clone());

    // Real broker ingress, not a direct store call.
    let topic = Topic::new("journal/probe").unwrap();
    let payload = Bytes::from_static(b"journalled-bytes");
    let deliveries = ingress_pipeline(
        &shared,
        &topic,
        QoS::AtLeastOnce,
        false,
        &payload,
        broker_session::tenant::DEFAULT_TENANT_ID,
    )
    .await;
    let _ = deliveries;

    // The publish path never blocks: it only enqueued. The background
    // task persists within milliseconds; poll for it.
    let mut seen = None;
    for _ in 0..100 {
        if store.stream_len("journal/probe") >= 1 {
            seen = Some(store.seek_offset("journal/probe", 0, 1).expect("seek"));
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let records = seen.expect("background journal persists within timeout");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].offset, 0);
    assert_eq!(records[0].payload, Bytes::from_static(b"journalled-bytes"));
    assert_eq!(records[0].topic.as_str(), "journal/probe");
    assert_eq!(journal.dropped_count(), 0);

    // Restarting the store from the same directory keeps the record.
    drop(store);
    drop(journal);
    // Give the background task a moment to exit with its sender.
    tokio::task::yield_now().await;
    let reopened =
        DurableStreamStore::open_with_config(&dir, StreamConfig::default()).expect("reopen");
    assert_eq!(reopened.stream_len("journal/probe"), 1);
    assert_eq!(
        reopened
            .get("journal/probe", 0)
            .expect("replayed after restart")
            .payload,
        Bytes::from_static(b"journalled-bytes")
    );
    std::fs::remove_dir_all(&dir).ok();
}
