//! RQ-04 hot-path cost probe: publish-to-deliver rule evaluation with no
//! event rules defined, before and after the event-source change.
//!
//! The gates run this file on the base commit and on the tree and compare
//! the numbers, so it uses only APIs that exist on the base commit
//! (`RuleEngine::new`, `dispatch_ingress`, `BrokerSink`; no event APIs).
//! An engine with zero rules has zero event selections, which is exactly
//! the "no event rules defined" case: the firing points must cost nothing.
//!
//! Run with:
//! `cargo +1.96.0 test --release -p broker-rules --test event_cost_bench -- --ignored --nocapture --test-threads=1`

use async_trait::async_trait;
use broker_protocol::{QoS, Topic};
use broker_rules::{BackpressurePolicy, BrokerSink, RuleEngine, RuleEngineError};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Instant;

/// Black-hole sink: rule output goes nowhere, so the loop measures
/// ingress evaluation only.
struct BlackholeSink;

#[async_trait]
impl BrokerSink for BlackholeSink {
    async fn publish(
        &self,
        _topic: Topic,
        _payload: Bytes,
        _qos: QoS,
        _retain: bool,
    ) -> Result<(), RuleEngineError> {
        Ok(())
    }
}

/// Publish-to-deliver evaluation rate with no event rules defined.
/// Prints `BENCH` lines for the gate log; asserts nothing about absolute
/// speed (the gates compare base vs. tree spread).
#[ignore]
#[tokio::test]
async fn event_cost_no_rules() {
    let engine = RuleEngine::new(1024, BackpressurePolicy::DropOldest);
    let sink: Arc<dyn BrokerSink> = Arc::new(BlackholeSink);
    let topic = Topic::new("bench/t").expect("bench topic");
    let payload = Bytes::from_static(b"{\"t\": 21}");

    // Warmup so the timed loop never includes first-call setup.
    for _ in 0..10_000 {
        engine
            .dispatch_ingress(&topic, &payload, QoS::AtMostOnce, &sink)
            .await;
    }

    const ITERS: usize = 100_000;
    let mut nanos: Vec<u64> = Vec::with_capacity(ITERS);
    let start = Instant::now();
    for _ in 0..ITERS {
        let call = Instant::now();
        engine
            .dispatch_ingress(&topic, &payload, QoS::AtMostOnce, &sink)
            .await;
        nanos.push(call.elapsed().as_nanos() as u64);
    }
    let elapsed = start.elapsed();
    nanos.sort_unstable();
    let p99 = nanos[(ITERS * 99 / 100).min(ITERS - 1)];
    let per_sec = ITERS as f64 / elapsed.as_secs_f64();
    println!("BENCH publish_dispatch_per_sec {per_sec:.0} msgs/sec");
    println!("BENCH publish_dispatch_p99_ns {p99} ns");
}
