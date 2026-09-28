//! RX-01 rule runtime benchmark (hot path: rule evaluation runs on publish).
//!
//! BENCH: crates/broker-rules/tests/rule_runtime_bench.rs :: cargo +1.96.0 test --release -p broker-rules --test rule_runtime_bench -- --ignored --nocapture --test-threads=1
//!
//! What it measures is the per-publish cost of a plain
//! `SELECT payload.x FROM "t/#" WHERE payload.x > 10` rule: one
//! `dispatch_ingress` round (topic match, JSON parse, WHERE filter,
//! SELECT projection, republish into the sink) per message. Numbers are
//! measured by the pipeline, never by an agent: the gates run this file
//! three times on the base commit and three times on the tree.
//!
//! Base-commit compatibility: this file uses only broker-rules and
//! broker-protocol interfaces that exist on the base commit
//! (`RuleEngine::new`, `create_rule`, `dispatch_ingress`, `RuleAction`,
//! `BackpressurePolicy`, `BrokerSink`, `Topic::new`, `TopicFilter::new`,
//! `QoS`), so the same file compiles and runs on both sides.
//!
//! `dispatch_ingress` is the broker publish path's rule stage: the node
//! calls it per publish from `ingress_pipeline_with_publisher`
//! (`crates/broker-node/src/main.rs`), so this bench measures the live
//! publish path, not the store alone. Windowed ingress additionally
//! traverses the embedded runtime (one `StreamRecord` conversion plus one
//! non-blocking bus feed per ingress); plain rules evaluate inline.
//!
//! Output contract: every result line starts with
//! `BENCH <metric> <value> <unit>` so the gates log can be scraped
//! mechanically. `#[ignore]` so normal `cargo test` runs skip it and the
//! pipeline's bench runner picks it up explicitly. Counts are CI sample
//! sizes, not SLOs; the test asserts exact delivery counts (at-least-once
//! permits duplicates, never loss), never a time threshold.

use async_trait::async_trait;
use broker_protocol::{QoS, Topic, TopicFilter};
use broker_rules::{BackpressurePolicy, BrokerSink, RuleAction, RuleEngine, RuleEngineError};
use bytes::Bytes;
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Warm-up dispatches before timing. Reason: prime the branch predictor,
/// instruction cache and allocator so the timed window measures steady
/// state, not first-touch costs. Fixed count, not an SLO.
const WARMUP_ITERS: usize = 2_000;
/// Timed dispatches. Reason: 10 000 JSON parses plus evaluations give a
/// stable throughput and p99 while keeping the ignored release test to a
/// few seconds on the pipeline runner. Fixed sample size, not an SLO.
const MEASURED_ITERS: usize = 10_000;
/// Ingress queue capacity. Reason: the memory-only engine never queues on
/// this path, so 1024 is ample headroom with negligible setup cost.
/// Fixed size, not an SLO.
const QUEUE_CAPACITY: usize = 1024;

/// Lock-free counting sink: records deliveries without storing payloads,
/// so the bench holds no unbounded list (the per-iter latency sample is
/// the only accumulation, bounded by `MEASURED_ITERS` with capacity
/// reserved up front).
#[derive(Debug, Default)]
struct CountingSink {
    count: AtomicU64,
}

#[async_trait]
impl BrokerSink for CountingSink {
    async fn publish(
        &self,
        _topic: Topic,
        _payload: Bytes,
        _qos: QoS,
        _retain: bool,
    ) -> Result<(), RuleEngineError> {
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// Nearest-rank percentile of a sorted sample (`q` in `[0, 1]`).
fn percentile(sorted: &[f64], q: f64) -> f64 {
    debug_assert!(!sorted.is_empty());
    let rank = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    sorted[rank - 1]
}

/// RX-01 hot-path benchmark: one plain rule, one publish-shaped
/// `dispatch_ingress` per message. Prints `BENCH` lines for the gates
/// log; asserts only exact delivery counts, never timing.
#[tokio::test]
#[ignore]
async fn rule_runtime_bench() {
    let engine = RuleEngine::new(QUEUE_CAPACITY, BackpressurePolicy::DropOldest);
    engine
        .create_rule(
            "bench-plain".to_string(),
            TopicFilter::new("t/#").expect("bench filter parses"),
            Some("SELECT payload.x FROM \"t/#\" WHERE payload.x > 10".to_string()),
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("bench/out").expect("bench out topic parses"),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("bench rule creates");
    let sink = Arc::new(CountingSink::default());
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();
    let topic = Topic::new("t/bench").expect("bench ingress topic parses");
    // Nested shape so the rule fires on the base engine too: the base
    // record is the raw JSON object, and `payload` resolves to its
    // top-level `payload` key (a flat `{"x": 20}` would read Null for
    // `payload.x` and only measure the reject path on both sides).
    let payload = Bytes::from_static(b"{\"payload\": {\"x\": 20}}");

    for _ in 0..WARMUP_ITERS {
        let matched = engine
            .dispatch_ingress(&topic, &payload, QoS::AtMostOnce, &sink_obj)
            .await;
        black_box(matched);
    }
    let warmup_delivered = sink.count.load(Ordering::Relaxed);
    assert_eq!(
        warmup_delivered as usize, WARMUP_ITERS,
        "warm-up: every rule evaluation must reach the sink"
    );

    // Latency sample only; bounded by MEASURED_ITERS with capacity
    // reserved up front so the bench holds no unbounded list.
    let mut lat_ns: Vec<f64> = Vec::with_capacity(MEASURED_ITERS);
    let wall = Instant::now();
    for _ in 0..MEASURED_ITERS {
        let start = Instant::now();
        let matched = engine
            .dispatch_ingress(&topic, &payload, QoS::AtMostOnce, &sink_obj)
            .await;
        black_box(matched);
        lat_ns.push(start.elapsed().as_nanos() as f64);
    }
    let secs = wall.elapsed().as_secs_f64().max(f64::EPSILON);
    let per_sec = MEASURED_ITERS as f64 / secs;
    lat_ns.sort_by(|a, b| a.total_cmp(b));
    let p99_ns = percentile(&lat_ns, 0.99);

    let delivered = sink.count.load(Ordering::Relaxed) - warmup_delivered;
    assert_eq!(
        delivered as usize, MEASURED_ITERS,
        "timed window: every rule evaluation must reach the sink"
    );

    // Leading blank line so the first BENCH line starts at column 0.
    // Reason: the test harness prints `test <name> ... ` with no trailing
    // newline, so without this the first BENCH line shares that line and
    // the pipeline's `grep -E '^BENCH '` misses it.
    println!();
    println!(
        "BENCH rule_throughput_events_per_sec {:.0} events/sec",
        per_sec
    );
    println!("BENCH rule_p99_ns {:.1} ns", p99_ns);
}
