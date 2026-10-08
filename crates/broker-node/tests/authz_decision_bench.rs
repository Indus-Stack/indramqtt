//! Authorization decision workload for AP-11.
//!
//! BENCH: crates/broker-node/tests/authz_decision_bench.rs :: cargo +1.96.0 test --release -p broker-node --test authz_decision_bench -- --ignored --nocapture --test-threads=1
//!
//! Measures the publish authorization path with one allow rule: run one
//! rule match per probe (`MemoryAuth::authorize_publish`), then report
//! the rate. Uses only APIs that exist on the task's base commit
//! (`MemoryAuth`, `AclRule`, `Authorizer`, `Topic`) so the gates can run
//! it on the base and on the tree; only `BENCH` lines carry numbers,
//! everything else is context. `#[ignore]` so normal `cargo test` skips
//! it. Counts are CI sample sizes, not SLOs.

use broker_auth::{AclAction, AclRule, Authorizer, MemoryAuth};
use broker_protocol::Topic;
use std::hint::black_box;
use std::time::Instant;

/// Probe count (20 000 decisions).
/// Reason: enough decisions to average out timer jitter while the whole
/// run stays under a second on the shared test host.
const BENCH_DECISIONS: usize = 20_000;

/// One allow rule plus steady probes against it. Prints `BENCH` lines
/// for the gates; asserts only the exact allow count, never a rate
/// threshold.
#[ignore]
#[tokio::test]
async fn authz_allow_rule_workload_timings() {
    let auth = MemoryAuth::new();
    auth.add_rule(AclRule::new("*", AclAction::Publish, "bench/#", true))
        .expect("memory-only persist cannot fail");
    let topic = Topic::new("bench/sensor-1").expect("probe topic parses");

    let start = Instant::now();
    let mut allowed = 0usize;
    for _ in 0..BENCH_DECISIONS {
        if auth.authorize_publish("bench-client", &topic).await.is_ok() {
            allowed += 1;
        }
    }
    let elapsed = start.elapsed();
    black_box(allowed);
    assert_eq!(
        allowed, BENCH_DECISIONS,
        "every probe matches the allow rule"
    );

    let rate = BENCH_DECISIONS as f64 / elapsed.as_secs_f64();
    let avg_ns = elapsed.as_nanos() as f64 / BENCH_DECISIONS as f64;

    // Leading blank line so the first BENCH line starts at column 0.
    println!();
    println!("BENCH authz_allow_per_sec {rate:.0} ops/s");
    println!("BENCH authz_allow_ns_per_msg {avg_ns:.1} ns");
    use std::io::Write as _;
    let _ = std::io::stdout().flush();
}
