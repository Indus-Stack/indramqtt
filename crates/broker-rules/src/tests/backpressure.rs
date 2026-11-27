use super::*;

struct MockInput {
    policy: BackpressurePolicy,
}

#[async_trait]
impl EventInput for MockInput {
    async fn push(&self, _event: StreamEvent) -> Result<PushOutcome, RuleEngineError> {
        match self.policy {
            BackpressurePolicy::DropNewest => {
                Ok(PushOutcome::Dropped(OverflowReason::DroppedNewest))
            }
            _ => Ok(PushOutcome::Enqueued),
        }
    }
}

fn test_event(topic: &str, payload: &'static [u8]) -> StreamEvent {
    StreamEvent::new(Topic::new(topic).unwrap(), Bytes::from_static(payload))
}

#[tokio::test]
async fn test_event_input_mock() {
    let input = MockInput {
        policy: BackpressurePolicy::DropNewest,
    };
    let event = StreamEvent {
        topic: Topic::new("sensors/temp").unwrap(),
        payload: Bytes::from_static(b"{\"temp\": 23}"),
        timestamp_millis: 1700000000,
        client_id: Some("c1".to_string()),
    };

    let outcome = input.push(event).await.unwrap();
    assert_eq!(outcome, PushOutcome::Dropped(OverflowReason::DroppedNewest));
}

#[tokio::test]
async fn test_drop_oldest_under_saturation() {
    let engine = RuleEngine::new(2, BackpressurePolicy::DropOldest);
    let input = engine.input();
    assert_eq!(
        input.try_push(test_event("t", b"one")).unwrap(),
        PushOutcome::Enqueued
    );
    assert_eq!(
        input.try_push(test_event("t", b"two")).unwrap(),
        PushOutcome::Enqueued
    );
    assert_eq!(
        input.try_push(test_event("t", b"three")).unwrap(),
        PushOutcome::Dropped(OverflowReason::DroppedOldest)
    );

    assert_eq!(
        input.next_event().await.unwrap().payload,
        Bytes::from_static(b"two")
    );
    assert_eq!(
        input.next_event().await.unwrap().payload,
        Bytes::from_static(b"three")
    );
}

#[tokio::test]
async fn test_drop_newest_under_saturation() {
    let engine = RuleEngine::new(2, BackpressurePolicy::DropNewest);
    let input = engine.input();
    assert_eq!(
        input.try_push(test_event("t", b"one")).unwrap(),
        PushOutcome::Enqueued
    );
    assert_eq!(
        input.try_push(test_event("t", b"two")).unwrap(),
        PushOutcome::Enqueued
    );
    assert_eq!(
        input.try_push(test_event("t", b"three")).unwrap(),
        PushOutcome::Dropped(OverflowReason::DroppedNewest)
    );

    assert_eq!(
        input.next_event().await.unwrap().payload,
        Bytes::from_static(b"one")
    );
    assert_eq!(
        input.next_event().await.unwrap().payload,
        Bytes::from_static(b"two")
    );
}

#[tokio::test]
async fn test_block_waits_for_capacity() {
    let engine = RuleEngine::new(1, BackpressurePolicy::Block);
    let input = engine.input();
    assert_eq!(
        input.try_push(test_event("t", b"one")).unwrap(),
        PushOutcome::Enqueued
    );
    // Synchronous callers cannot block: a full buffer reports overflow.
    assert_eq!(
        input.try_push(test_event("t", b"two")).unwrap(),
        PushOutcome::Dropped(OverflowReason::BufferFull)
    );

    // The async push waits until the consumer drains one slot.
    let slow = input.push(test_event("t", b"three"));
    tokio::pin!(slow);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut slow)
            .await
            .is_err(),
        "blocked push must pend while full"
    );
    assert_eq!(
        input.next_event().await.unwrap().payload,
        Bytes::from_static(b"one")
    );
    tokio::time::timeout(std::time::Duration::from_secs(2), slow)
        .await
        .expect("push completes after drain")
        .unwrap();
    assert_eq!(
        input.next_event().await.unwrap().payload,
        Bytes::from_static(b"three")
    );
}

#[tokio::test]
async fn test_reject_and_spill_report_overflow_when_full() {
    for policy in [
        BackpressurePolicy::RejectPublisher,
        BackpressurePolicy::SpillToDisk,
    ] {
        let engine = RuleEngine::new(1, policy);
        let input = engine.input();
        assert!(input.try_push(test_event("t", b"one")).is_ok());
        assert!(matches!(
            input.try_push(test_event("t", b"two")),
            Err(RuleEngineError::Overflow(OverflowReason::BufferFull))
        ));
    }
}

// ------------------------------------------------------------------
// B4-07: SpillToDisk is a real bounded disk buffer, not a silent
// alias for rejection. The tests below are buffer units through the
// broker's rule engine input (never a bare store); the Done-when
// broker-path coverage (fill via publish/deliver through
// `ingress_pipeline`, spill, replay in order) lives in
// `crates/broker-node/src/main.rs:rule_spill_broker_path_spills_and_replays_in_order`.
// Every test cleans up its scratch directory.
// ------------------------------------------------------------------

static SPILL_TEST_SEQ: AtomicU64 = AtomicU64::new(0);

/// Unique scratch directory for one spill test (no new crates:
/// process id plus a counter). Callers remove it when done.
fn spill_test_dir(tag: &str) -> std::path::PathBuf {
    let seq = SPILL_TEST_SEQ.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "indra-rule-spill-{tag}-{}-{seq}",
        std::process::id()
    ))
}

fn remove_spill_dir(dir: &std::path::Path) {
    let _ = std::fs::remove_dir_all(dir);
}

/// Fill the in-memory queue through the broker input path, push
/// past it, and drain: overflow spills to disk instead of erroring
/// and replays oldest-first after the memory events.
#[tokio::test]
async fn test_spill_to_disk_spills_and_replays_in_order() {
    let dir = spill_test_dir("order");
    remove_spill_dir(&dir);
    let engine = RuleEngine::new_with_spill(2, dir.clone()).expect("spill engine opens");
    let metrics = Arc::new(broker_observability::Metrics::new());
    engine.set_metrics(&metrics);
    let input = engine.input();
    assert!(input.has_spill());

    assert_eq!(
        input.push(test_event("t", b"one")).await.unwrap(),
        PushOutcome::Enqueued
    );
    assert_eq!(
        input.push(test_event("t", b"two")).await.unwrap(),
        PushOutcome::Enqueued
    );
    assert_eq!(
        input.push(test_event("t", b"three")).await.unwrap(),
        PushOutcome::SpilledToDisk(0)
    );
    assert_eq!(
        input.push(test_event("t", b"four")).await.unwrap(),
        PushOutcome::SpilledToDisk(1)
    );

    // Rule matching is unchanged on a spill-backed engine, and live
    // ingress now mirrors a durability copy through the input queue
    // (`dispatch_ingress` pushes when spill-backed): with a disk
    // backlog present the copy joins the disk (sticky spill), so it
    // replays last after the four events below.
    engine
        .create_rule(
            "spill-probe".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            None,
            true,
            vec![RuleAction::Log],
        )
        .expect("rule creates");
    let sink: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());
    assert_eq!(
        engine
            .dispatch_ingress(
                &Topic::new("sensors/kitchen").unwrap(),
                &Bytes::from_static(b"21.5C"),
                QoS::AtMostOnce,
                &sink,
            )
            .await,
        1
    );

    // Drain: memory events first in order, then spilled in order,
    // then the live-ingress durability copy (`21.5C`) last.
    for want in [
        b"one".as_slice(),
        b"two".as_slice(),
        b"three".as_slice(),
        b"four".as_slice(),
        b"21.5C".as_slice(),
    ] {
        let event = input.next_event().await.expect("replay delivers");
        assert_eq!(event.payload, Bytes::from_static(want));
    }
    let stats = engine.spill_stats();
    assert_eq!(stats.spilled, 3);
    assert_eq!(stats.replayed, 3);
    assert_eq!(stats.dropped, 0);
    assert_eq!(stats.recovered, 0);
    assert_eq!(input.spill_backlog_len(), 0);
    // The existing observability path mirrors every outcome.
    assert_eq!(metrics.rule_spill_spilled(), 3);
    assert_eq!(metrics.rule_spill_replayed(), 3);
    assert_eq!(metrics.rule_spill_dropped(), 0);

    input.sync_spill().expect("sync");
    remove_spill_dir(&dir);
}

/// Spilled events survive an engine rebuild from the same spill
/// directory (restart). Memory-queued events do not: only overflow
/// is durable, which the test asserts explicitly.
#[tokio::test]
async fn test_spill_survives_engine_rebuild() {
    let dir = spill_test_dir("restart");
    remove_spill_dir(&dir);
    {
        let engine = RuleEngine::new_with_spill(1, dir.clone()).expect("spill engine opens");
        let input = engine.input();
        assert_eq!(
            input.push(test_event("t", b"memory-only")).await.unwrap(),
            PushOutcome::Enqueued
        );
        assert_eq!(
            input.push(test_event("t", b"spilled-a")).await.unwrap(),
            PushOutcome::SpilledToDisk(0)
        );
        assert_eq!(
            input.push(test_event("t", b"spilled-b")).await.unwrap(),
            PushOutcome::SpilledToDisk(1)
        );
        input.sync_spill().expect("sync before restart");
    }
    // Recreate from the same directory: the two spilled events are
    // recovered, the memory event is gone by design.
    let engine = RuleEngine::new_with_spill(1, dir.clone()).expect("spill engine reopens");
    let metrics = Arc::new(broker_observability::Metrics::new());
    // Attaching after open still mirrors the recovery counts: the
    // first attach carries everything accumulated so far.
    engine.set_metrics(&metrics);
    assert_eq!(engine.spill_stats().recovered, 2);
    assert_eq!(metrics.rule_spill_recovered(), 2);
    let input = engine.input();
    assert_eq!(
        input.next_event().await.expect("first replay").payload,
        Bytes::from_static(b"spilled-a")
    );
    assert_eq!(
        input.next_event().await.expect("second replay").payload,
        Bytes::from_static(b"spilled-b")
    );
    assert_eq!(engine.spill_stats().replayed, 2);
    // Nothing left anywhere: the consumer pends instead of
    // inventing an event.
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), input.next_event())
            .await
            .is_err(),
        "drained spill must pend, not invent"
    );
    remove_spill_dir(&dir);
}

/// A spill file truncated mid-record recovers everything before
/// the tear, discards the torn tail, and counts it.
#[tokio::test]
async fn test_spill_torn_tail_discarded_and_counted() {
    use std::fs::OpenOptions;

    let dir = spill_test_dir("torn");
    remove_spill_dir(&dir);
    {
        let engine = RuleEngine::new_with_spill(1, dir.clone()).expect("spill engine opens");
        let input = engine.input();
        assert_eq!(
            input.push(test_event("t", b"pad")).await.unwrap(),
            PushOutcome::Enqueued
        );
        for (seq, payload) in [b"keep-a", b"keep-b", b"torn-c"].into_iter().enumerate() {
            assert_eq!(
                input.push(test_event("t", payload)).await.unwrap(),
                PushOutcome::SpilledToDisk(seq as u64)
            );
        }
        input.sync_spill().expect("sync");
    }
    // Truncate the single segment three bytes into the second
    // spilled frame: parse the first frame header to land strictly
    // inside a later frame rather than on a boundary.
    let seg = std::fs::read_dir(&dir)
        .expect("spill dir lists")
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .find(|path| {
            path.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("spill-") && n.ends_with(".log"))
        })
        .expect("one segment file");
    let data = std::fs::read(&seg).expect("segment reads");
    assert!(data.len() > 16, "segment must hold frames");
    let first_len = u32::from_be_bytes(data[4..8].try_into().expect("header")) as usize;
    let cut = 8 + first_len + 4 + 3;
    assert!(cut < data.len(), "cut must land inside a later frame");
    drop(data);
    OpenOptions::new()
        .write(true)
        .open(&seg)
        .expect("segment opens")
        .set_len(cut as u64)
        .expect("truncate");

    let engine = RuleEngine::new_with_spill(1, dir.clone()).expect("spill engine reopens");
    let stats = engine.spill_stats();
    assert_eq!(stats.torn_discarded, 1, "one torn tail counted");
    assert_eq!(
        stats.recovered, 1,
        "everything before the tear survives, got {stats:?}"
    );
    // The intact first frame replays; nothing follows it.
    let input = engine.input();
    let first = input.next_event().await.expect("first frame replays");
    assert_eq!(first.payload, Bytes::from_static(b"keep-a"));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), input.next_event())
            .await
            .is_err(),
        "torn tail must not replay"
    );
    remove_spill_dir(&dir);
}

/// SpillToDisk without a directory refuses loudly and counts the
/// drop: fail closed, never silently memory-only.
#[tokio::test]
async fn test_spill_without_directory_refuses_and_counts() {
    let engine = RuleEngine::new(1, BackpressurePolicy::SpillToDisk);
    let input = engine.input();
    assert!(!input.has_spill());
    assert!(input.push(test_event("t", b"one")).await.is_ok());
    assert!(matches!(
        input.push(test_event("t", b"two")).await,
        Err(RuleEngineError::Overflow(OverflowReason::BufferFull))
    ));
    assert_eq!(engine.spill_stats().dropped, 1);
}

/// Publish-path cost, before and after (B4-07): time the
/// steady-state push loop (no pressure, no disk, no spill mutex) and
/// the overflow spill-append loop (page-cache writes, no fsync) plus
/// the replay loop, printing msgs/sec for the gate log.
/// Steady-state pushes (no pressure) never touch disk; only overflow
/// pays the spill mutex plus one page-cache `write + flush`.
#[tokio::test]
async fn test_spill_workload_timings() {
    // BEFORE: steady-state pushes with room — no pressure, no disk.
    // One relaxed load for the sticky check plus one `try_send`; no
    // spill mutex, no `Vec` alloc, no file I/O.
    let dir_base = spill_test_dir("timings-base");
    remove_spill_dir(&dir_base);
    let engine_base =
        RuleEngine::new_with_spill(4096, dir_base.clone()).expect("spill engine opens");
    let input_base = engine_base.input();
    let total = 2_000usize;
    let start = std::time::Instant::now();
    for i in 0..total {
        let payload = format!("event-{i:05}");
        let event = StreamEvent {
            topic: Topic::new("t").unwrap(),
            payload: Bytes::from(payload.into_bytes()),
            timestamp_millis: 1700000000,
            client_id: None,
        };
        assert_eq!(
            input_base.push(event).await.expect("memory admits"),
            PushOutcome::Enqueued,
            "baseline must never touch disk"
        );
    }
    let elapsed = start.elapsed();
    let rate = total as f64 / elapsed.as_secs_f64();
    println!(
        "rule spill steady-state workload: {total} enqueued pushes in {elapsed:?} \
         ({rate:.0} msgs/sec, avg {:.1} ns/msg, no disk, no spill mutex)",
        elapsed.as_nanos() as f64 / total as f64
    );
    assert_eq!(engine_base.spill_stats().spilled, 0);
    // Drain the baseline so the scratch dir check stays clean.
    for _ in 0..total {
        input_base.next_event().await.expect("baseline drains");
    }
    remove_spill_dir(&dir_base);
    // AFTER: overflow pushes spill to disk (one spill mutex plus one
    // page-cache write+flush each, never fsync) and replay reads back.
    let dir = spill_test_dir("timings");
    remove_spill_dir(&dir);
    let engine = RuleEngine::new_with_spill(128, dir.clone()).expect("spill engine opens");
    let input = engine.input();
    let total = 2_000usize;
    let start = std::time::Instant::now();
    let mut spilled = 0usize;
    for i in 0..total {
        let payload = format!("event-{i:05}");
        let event = StreamEvent {
            topic: Topic::new("t").unwrap(),
            payload: Bytes::from(payload.into_bytes()),
            timestamp_millis: 1700000000,
            client_id: None,
        };
        match input.push(event).await.expect("spill admits") {
            PushOutcome::Enqueued => {}
            PushOutcome::SpilledToDisk(_) => spilled += 1,
            PushOutcome::Dropped(reason) => panic!("spill must not drop: {reason:?}"),
        }
    }
    let elapsed = start.elapsed();
    assert!(spilled > 0, "the run must actually exercise the disk path");
    let rate = total as f64 / elapsed.as_secs_f64();
    println!(
        "rule spill workload: {total} pushes ({spilled} spilled) in {elapsed:?} \
         ({rate:.0} msgs/sec, avg {:.1} ns/msg)",
        elapsed.as_nanos() as f64 / total as f64
    );
    let start = std::time::Instant::now();
    for i in 0..total {
        let event = input.next_event().await.expect("replay delivers");
        assert_eq!(
            event.payload,
            Bytes::from(format!("event-{i:05}").into_bytes()),
            "replay keeps global order"
        );
    }
    let elapsed = start.elapsed();
    let rate = total as f64 / elapsed.as_secs_f64();
    println!(
        "rule spill replay workload: {total} replays in {elapsed:?} \
         ({rate:.0} msgs/sec, avg {:.1} ns/msg)",
        elapsed.as_nanos() as f64 / total as f64
    );
    let stats = engine.spill_stats();
    assert_eq!(stats.spilled as usize, spilled);
    assert_eq!(stats.replayed as usize, spilled);
    assert_eq!(stats.dropped, 0);
    remove_spill_dir(&dir);
}
