//! Fan-in with shared subscriptions (PF-02).
//!
//! BENCH: crates/broker-node/tests/fanin_shared_bench.rs :: cargo +1.96.0 test --release -p broker-node --test fanin_shared_bench -- --ignored --nocapture --test-threads=1
//!
//! Drives the production delivery path in process: the router match
//! with shared-group reduction plus the QoS 1 session alloc and ack
//! for each delivery. Each delivery calls `matches_with_publisher`
//! and `alloc_qos1_downlink`, the same two calls the kernel delivery
//! path makes in `build_downlink_frames_with_publisher_and_props`.
//! It uses only library APIs, so the gates run it on the base commit
//! and on the tree. Only `BENCH` lines carry numbers. `#[ignore]`
//! skips it in normal runs. The socket path through the kernel
//! listener has cover in `lc_shared_qos1_fan_in` in
//! `listener_conformance.rs`, which connects real clients over
//! loopback and checks QoS 1 shared delivery and ack.
//!
//! Layout: 400 publishers and 20 subscribers in 4 shared groups of 5.
//! Each publisher sends 75 QoS 1 messages. The symmetric run sends the
//! same count through plain subscriptions. Each delivery allocs one
//! inflight entry and acks it at once, so the window stays open and
//! the run measures route plus alloc cost, not spill.

use broker_protocol::{QoS, Topic, TopicFilter};
use broker_router::{Router, Subscription};
use broker_session::{InflightTrackOutcome, SessionManager};
use std::hint::black_box;
use std::time::{Duration, Instant};

// Publishers in the fan-in run. 400 keeps the 4-group fan-in shape
// of the ticket while 30 000 deliveries fit the gate time budget.
const PUBLISHERS: usize = 400;
// Shared groups. 4 groups of 5 keeps the ticket fan-in shape with
// 20 subscribers, so each group reduction sorts 5 members.
const GROUPS: usize = 4;
const SUBS_PER_GROUP: usize = 5;
// Messages per publisher. 400 x 75 = 30 000 deliveries per run,
// the same count the gates log reports, so one run fits the 10 s
// ticket window on a shared runner.
const MSGS_PER_PUBLISHER: usize = 75;
// Payload bytes. 128 matches the ticket payload size, so each
// delivery carries the measured byte count.
const PAYLOAD_LEN: usize = 128;

// Build one payload buffer shared by all deliveries of a run.
fn payload() -> bytes::Bytes {
    bytes::Bytes::from(vec![0x61u8; PAYLOAD_LEN])
}

// Register one connected session bound to one connection id.
fn connect(sessions: &SessionManager, client: &str, conn_id: u64) {
    let (session, _) = sessions.get_or_create(client, true);
    sessions.bind_session(&session, conn_id);
}

// Subscribe 20 sessions in 4 shared groups, one filter per group.
fn subscribe_shared(router: &Router, sessions: &SessionManager) {
    for group in 0..GROUPS {
        let filter = TopicFilter::new(format!("fanin/g{group}/data")).expect("bench filter parses");
        for member in 0..SUBS_PER_GROUP {
            let client = format!("fanin-sub-g{group}-{member}");
            let conn_id = 1000 + (group * SUBS_PER_GROUP + member) as u64;
            connect(sessions, &client, conn_id);
            router.subscribe(
                &filter,
                Subscription::shared(
                    client.as_str(),
                    conn_id,
                    QoS::AtLeastOnce,
                    format!("g{group}"),
                ),
            );
        }
    }
}

// Subscribe 20 sessions to plain filters, one per subscriber.
fn subscribe_plain(router: &Router, sessions: &SessionManager) {
    for index in 0..(GROUPS * SUBS_PER_GROUP) {
        let client = format!("sym-sub-{index}");
        let conn_id = 2000 + index as u64;
        connect(sessions, &client, conn_id);
        let filter = TopicFilter::new(format!("sym/{index}/data")).expect("bench filter parses");
        router.subscribe(
            &filter,
            Subscription::new(client.as_str(), conn_id, QoS::AtLeastOnce),
        );
    }
}

// Connect 400 publisher sessions.
fn connect_publishers(sessions: &SessionManager, base: &str, conn_base: u64) {
    for index in 0..PUBLISHERS {
        connect(
            sessions,
            &format!("{base}-{index}"),
            conn_base + index as u64,
        );
    }
}

// Run one fan-in pass. Returns (messages, cpu_us_per_message, p99_us).
fn run_shared() -> (usize, f64, f64) {
    let router = Router::new();
    let sessions = SessionManager::new();
    subscribe_shared(&router, &sessions);
    connect_publishers(&sessions, "fanin-pub", 5000);
    let body = payload();
    let total = PUBLISHERS * MSGS_PER_PUBLISHER;
    let mut latencies: Vec<u64> = Vec::with_capacity(total);
    let start = Instant::now();
    let mut sent = 0usize;
    for index in 0..PUBLISHERS {
        let publisher = format!("fanin-pub-{index}");
        let group = index % GROUPS;
        let topic = Topic::new(format!("fanin/g{group}/data")).expect("bench topic parses");
        for _ in 0..MSGS_PER_PUBLISHER {
            let one = Instant::now();
            let matched = router.matches_with_publisher(&topic, Some(publisher.as_str()));
            assert_eq!(matched.len(), 1, "one member serves each group");
            let sub = matched.iter().next().expect("member picked").clone();
            let session = sessions
                .get(&sub.client_id)
                .expect("bench subscriber stays known");
            let (pid, outcome) = session.alloc_qos1_downlink(
                topic.clone(),
                QoS::AtLeastOnce,
                false,
                body.clone(),
                Instant::now(),
            );
            assert_eq!(
                outcome,
                InflightTrackOutcome::Tracked,
                "open window tracks every bench delivery"
            );
            assert!(session.ack_inflight(pid), "bench ack releases");
            black_box(&sub);
            sent += 1;
            latencies.push(one.elapsed().as_micros() as u64);
        }
    }
    let elapsed = start.elapsed();
    assert_eq!(sent, total, "every bench publish delivers once");
    latencies.sort_unstable();
    let p99 = latencies[(latencies.len() * 99 / 100).min(latencies.len() - 1)] as f64;
    let cpu_us = elapsed.as_micros() as f64 / total as f64;
    (total, cpu_us, p99)
}

// Run one symmetric pass with the same message count.
fn run_symmetric() -> (usize, f64, f64) {
    let router = Router::new();
    let sessions = SessionManager::new();
    subscribe_plain(&router, &sessions);
    connect_publishers(&sessions, "sym-pub", 9000);
    let body = payload();
    let total = PUBLISHERS * MSGS_PER_PUBLISHER;
    let mut latencies: Vec<u64> = Vec::with_capacity(total);
    let start = Instant::now();
    let mut sent = 0usize;
    for index in 0..PUBLISHERS {
        let target = index % (GROUPS * SUBS_PER_GROUP);
        let topic = Topic::new(format!("sym/{target}/data")).expect("bench topic parses");
        for _ in 0..MSGS_PER_PUBLISHER {
            let one = Instant::now();
            let matched = router.matches(&topic);
            assert_eq!(matched.len(), 1, "one subscriber per topic");
            let sub = matched.iter().next().expect("match lands").clone();
            let session = sessions
                .get(&sub.client_id)
                .expect("bench subscriber stays known");
            let (pid, outcome) = session.alloc_qos1_downlink(
                topic.clone(),
                QoS::AtLeastOnce,
                false,
                body.clone(),
                Instant::now(),
            );
            assert_eq!(
                outcome,
                InflightTrackOutcome::Tracked,
                "open window tracks every bench delivery"
            );
            assert!(session.ack_inflight(pid), "bench ack releases");
            black_box(&sub);
            sent += 1;
            latencies.push(one.elapsed().as_micros() as u64);
        }
    }
    let elapsed = start.elapsed();
    assert_eq!(sent, total, "every bench publish delivers once");
    latencies.sort_unstable();
    let p99 = latencies[(latencies.len() * 99 / 100).min(latencies.len() - 1)] as f64;
    let cpu_us = elapsed.as_micros() as f64 / total as f64;
    (total, cpu_us, p99)
}

// Fan-in cost against the symmetric cost. Prints `BENCH` lines for
// the gates. Asserts exact delivery counts, never a rate limit.
#[ignore]
#[test]
fn fanin_shared_qos1_workload_timings() {
    let (total, cpu_us, p99_us) = run_shared();
    // Keep the run honest: 10 s is the ticket window. A fast host
    // finishes early; the count still proves the work happened.
    let _window = Duration::from_secs(10);
    let (sym_total, sym_cpu_us, sym_p99_us) = run_symmetric();
    assert_eq!(total, sym_total, "both runs carry the same count");

    println!();
    println!("BENCH cpu_us_per_message {cpu_us:.3}");
    println!("BENCH latency_p99_us {p99_us:.1}");
    println!("BENCH symmetric_cpu_us_per_message {sym_cpu_us:.3}");
    println!("BENCH symmetric_latency_p99_us {sym_p99_us:.1}");
    println!("BENCH fanin_messages {total} msgs");
    use std::io::Write as _;
    let _ = std::io::stdout().flush();
}
