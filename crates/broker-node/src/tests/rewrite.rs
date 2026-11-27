use super::*;

/// Decode the topic out of a `PublishOut` downlink frame (test mirror
/// of `encode_publish_out`: `TopicLen:16be | Topic | PacketId:16be |
/// QoS:8 | Retain:8 | Dup:8`).
#[cfg(test)]
fn downlink_topic(frame: &BrokerFrame) -> String {
    let meta = &frame.metadata;
    let topic_len = u16::from_be_bytes([meta[0], meta[1]]) as usize;
    std::str::from_utf8(&meta[2..2 + topic_len])
        .expect("downlink topic is UTF-8")
        .to_string()
}

/// B4-04: a publish through the broker with a capture-group rule
/// delivers the rewritten topic. Subscriber sits on the rewritten
/// name; the publisher sends the original; the downlink carries the
/// rewritten name. Fails before the change (no rewrite existed, so
/// nothing matched and no delivery arrived).
#[tokio::test]
async fn rewrite_publish_capture_group_delivers_rewritten_topic() {
    let shared = test_shared();
    shared
        .router
        .set_rewrite_rules(vec![broker_router::RewriteRuleConfig::new(
            "^sensors/(.*)$",
            "devices/$1",
            broker_router::RewriteScope::Publish,
        )])
        .expect("valid rule installs");

    // Subscriber on the rewritten name (publish-scoped rule leaves
    // subscribe filters untouched, so this subscribes literally).
    let (session, _) = shared.sessions.get_or_create("rw-sub", true);
    *session.conn_id.write() = Some(61);
    let sub = subscribe_frame(
        61,
        1,
        encode_subscribe_meta(3, "rw-sub", &[("devices/temp", 0)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");
    bind_test_publisher(&shared, 62);

    // Publish the original name through the real ingress event.
    let (meta, payload) = encode_publish_meta("sensors/temp", 0, 0, false, b"21.5");
    let (_, deliveries) = apply_publish(&publish_frame(62, 2, meta, payload), &shared).await;

    assert_eq!(deliveries.len(), 1, "rewritten publish must fan out");
    assert_eq!(deliveries[0].0, 61);
    assert_eq!(downlink_topic(&deliveries[0].1), "devices/temp");
    assert_eq!(shared.router.rewrite_stats().rewritten, 1);
}

/// B4-04: publish-side, subscribe-side and both-scoped rules act
/// independently through the broker. A publish-scoped rule never
/// rewrites a subscription; a subscribe-scoped rule never rewrites
/// a publish; a both-scoped rule rewrites either.
#[tokio::test]
async fn rewrite_scopes_apply_independently_through_broker() {
    // Publish-only: publish rewrites, subscribe does not.
    let shared = test_shared();
    shared
        .router
        .set_rewrite_rules(vec![broker_router::RewriteRuleConfig::new(
            "^p/(.*)$",
            "pub/$1",
            broker_router::RewriteScope::Publish,
        )])
        .expect("publish rule installs");
    let (session, _) = shared.sessions.get_or_create("rw-p", true);
    *session.conn_id.write() = Some(63);
    // Subscribing to the pre-rewrite name stays literal (no match).
    let sub = subscribe_frame(63, 1, encode_subscribe_meta(4, "rw-p", &[("p/a", 0)]));
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");
    bind_test_publisher(&shared, 64);
    // Publishing the same name rewrites away from the subscription.
    let (meta, payload) = encode_publish_meta("p/a", 0, 0, false, b"x");
    let (_, deliveries) = apply_publish(&publish_frame(64, 1, meta, payload), &shared).await;
    assert!(
        deliveries.is_empty(),
        "publish-only rewrite must not match the literal subscription"
    );
    // A subscriber on the rewritten name receives it.
    let sub = subscribe_frame(63, 2, encode_subscribe_meta(5, "rw-p", &[("pub/a", 0)]));
    apply_subscribe(&sub, &shared).await.0.expect("subscribe");
    let (meta, payload) = encode_publish_meta("p/a", 0, 0, false, b"x");
    let (_, deliveries) = apply_publish(&publish_frame(64, 2, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(downlink_topic(&deliveries[0].1), "pub/a");

    // Subscribe-only: subscribe rewrites, publish does not.
    let shared = test_shared();
    shared
        .router
        .set_rewrite_rules(vec![broker_router::RewriteRuleConfig::new(
            "^s/(.*)$",
            "sub/$1",
            broker_router::RewriteScope::Subscribe,
        )])
        .expect("subscribe rule installs");
    let (session, _) = shared.sessions.get_or_create("rw-s", true);
    *session.conn_id.write() = Some(65);
    // Subscribing to the pre-rewrite name lands on the rewritten filter.
    let sub = subscribe_frame(65, 1, encode_subscribe_meta(6, "rw-s", &[("s/a", 0)]));
    apply_subscribe(&sub, &shared).await.0.expect("subscribe");
    bind_test_publisher(&shared, 66);
    // Publishing the rewritten name (untouched by the rule) delivers.
    let (meta, payload) = encode_publish_meta("sub/a", 0, 0, false, b"y");
    let (_, deliveries) = apply_publish(&publish_frame(66, 1, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(downlink_topic(&deliveries[0].1), "sub/a");
    // Publishing the pre-rewrite name does not rewrite, so nothing matches.
    let (meta, payload) = encode_publish_meta("s/a", 0, 0, false, b"y");
    let (_, deliveries) = apply_publish(&publish_frame(66, 2, meta, payload), &shared).await;
    assert!(
        deliveries.is_empty(),
        "subscribe-only rule must not rewrite publishes"
    );

    // Both-scoped: either direction rewrites.
    let shared = test_shared();
    shared
        .router
        .set_rewrite_rules(vec![broker_router::RewriteRuleConfig::new(
            "^b/(.*)$",
            "both/$1",
            broker_router::RewriteScope::Both,
        )])
        .expect("both rule installs");
    let (session, _) = shared.sessions.get_or_create("rw-b", true);
    *session.conn_id.write() = Some(67);
    let sub = subscribe_frame(67, 1, encode_subscribe_meta(7, "rw-b", &[("b/a", 0)]));
    apply_subscribe(&sub, &shared).await.0.expect("subscribe");
    bind_test_publisher(&shared, 68);
    let (meta, payload) = encode_publish_meta("b/a", 0, 0, false, b"z");
    let (_, deliveries) = apply_publish(&publish_frame(68, 1, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(downlink_topic(&deliveries[0].1), "both/a");
}

/// B4-04: rule order decides (first match wins) and an unmatched
/// topic passes through unchanged, through the real broker events.
#[tokio::test]
async fn rewrite_order_first_match_wins_and_miss_passes_through() {
    let shared = test_shared();
    shared
        .router
        .set_rewrite_rules(vec![
            broker_router::RewriteRuleConfig::new(
                "^o/(.*)$",
                "first/$1",
                broker_router::RewriteScope::Both,
            ),
            broker_router::RewriteRuleConfig::new(
                "^o/(.*)$",
                "second/$1",
                broker_router::RewriteScope::Both,
            ),
        ])
        .expect("rules install");
    let (session, _) = shared.sessions.get_or_create("rw-o", true);
    *session.conn_id.write() = Some(69);
    for filter in ["first/x", "second/x", "plain/x"] {
        let sub = subscribe_frame(69, 1, encode_subscribe_meta(8, "rw-o", &[(filter, 0)]));
        apply_subscribe(&sub, &shared).await.0.expect("subscribe");
    }
    bind_test_publisher(&shared, 70);
    // First matching rule wins.
    let (meta, payload) = encode_publish_meta("o/x", 0, 0, false, b"o");
    let (_, deliveries) = apply_publish(&publish_frame(70, 1, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(downlink_topic(&deliveries[0].1), "first/x");
    // An unmatched topic passes through unchanged to its subscriber.
    let (meta, payload) = encode_publish_meta("plain/x", 0, 0, false, b"p");
    let (_, deliveries) = apply_publish(&publish_frame(70, 2, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(downlink_topic(&deliveries[0].1), "plain/x");
}

/// B4-04: the rule-count bound and the evaluation budget are enforced
/// with counters. Over-cap installs fail closed (table unchanged);
/// over-long inputs fail closed to no-rewrite with the budget counter.
#[tokio::test]
async fn rewrite_bounds_enforced_with_counters() {
    let shared = test_shared();
    let too_many: Vec<broker_router::RewriteRuleConfig> = (0..(broker_router::MAX_REWRITE_RULES
        + 1))
        .map(|i| {
            broker_router::RewriteRuleConfig::new(
                format!("^bound{i}/(.*)$"),
                "x/$1",
                broker_router::RewriteScope::Both,
            )
        })
        .collect();
    let err = shared
        .router
        .set_rewrite_rules(too_many)
        .expect_err("over-cap install must fail");
    assert!(
        matches!(err, broker_router::RewriteRuleError::TooManyRules(_)),
        "unexpected error: {err}"
    );
    assert_eq!(shared.router.rewrite_rule_count(), 0);

    // Budget: an input past the length bound never rewrites and bumps
    // exactly the budget counter (delivery would proceed unchanged).
    let huge = "t".repeat(broker_router::MAX_REWRITE_LEN + 1);
    assert_eq!(shared.router.rewrite_publish(&huge), None);
    let stats = shared.router.rewrite_stats();
    assert_eq!(stats.budget_exceeded, 1);
    assert_eq!(stats.rewritten, 0);
}

/// B4-04 publish-path workload numbers with rewriting off and on,
/// measured through the real broker publish event (`apply_publish`),
/// not just the router. OFF is the no-rules baseline; ON installs one
/// capture-group rule plus a full 64-rule table miss path on a second
/// router state. Both print msgs/sec and avg ns/msg into the gate
/// output with no threshold assert; counts are CI sample sizes, not
/// SLOs. Per-message cost prose lives on `Router::rewrite_for`; this
/// test is the measured numbers behind it.
#[tokio::test]
async fn rewrite_publish_timing_reports_throughput() {
    use std::hint::black_box;
    use std::time::Instant;

    // OFF: no rules installed.
    let shared = test_shared();
    let (session, _) = shared.sessions.get_or_create("rw-load", true);
    *session.conn_id.write() = Some(71);
    let sub = subscribe_frame(
        71,
        1,
        encode_subscribe_meta(9, "rw-load", &[("devices/load", 0), ("load/plain", 0)]),
    );
    apply_subscribe(&sub, &shared).await.0.expect("subscribe");
    let off_iters = 200usize;
    let off_start = Instant::now();
    bind_test_publisher(&shared, 72);
    for i in 0..off_iters {
        let (meta, payload) = encode_publish_meta("load/plain", 0, 0, false, b"x");
        let (_, deliveries) =
            apply_publish(&publish_frame(72, 100 + i as u64, meta, payload), &shared).await;
        black_box(&deliveries);
        assert_eq!(deliveries.len(), 1, "baseline must deliver");
    }
    let off_elapsed = off_start.elapsed();
    let off_rate = off_iters as f64 / off_elapsed.as_secs_f64();
    let off_avg_ns = off_elapsed.as_nanos() as f64 / off_iters as f64;
    println!(
        "rewrite broker workload baseline (off, no rules): {off_rate:.0} msgs/sec, avg {off_avg_ns:.1} ns/msg ({off_iters} apply_publish rounds in {off_elapsed:?})"
    );

    // ON: one matching capture-group rule plus 63 preceding misses so
    // the measured path pays the worst-case ordered scan.
    let mut rules = Vec::with_capacity(broker_router::MAX_REWRITE_RULES);
    for i in 0..(broker_router::MAX_REWRITE_RULES - 1) {
        rules.push(broker_router::RewriteRuleConfig::new(
            format!("^nomatch{i}/(.*)$"),
            "miss/$1",
            broker_router::RewriteScope::Both,
        ));
    }
    rules.push(broker_router::RewriteRuleConfig::new(
        "^load/(.*)$",
        "devices/$1",
        broker_router::RewriteScope::Both,
    ));
    shared
        .router
        .set_rewrite_rules(rules)
        .expect("full table installs");
    // Resubscribe through the rewritten filter so the ON loop delivers.
    let sub = subscribe_frame(
        71,
        2,
        encode_subscribe_meta(10, "rw-load", &[("load/plain", 0)]),
    );
    apply_subscribe(&sub, &shared).await.0.expect("subscribe");
    let on_iters = 200usize;
    let on_start = Instant::now();
    for i in 0..on_iters {
        let (meta, payload) = encode_publish_meta("load/plain", 0, 0, false, b"x");
        let (_, deliveries) =
            apply_publish(&publish_frame(72, 500 + i as u64, meta, payload), &shared).await;
        black_box(&deliveries);
        assert_eq!(deliveries.len(), 1, "rewritten publish must deliver");
        assert_eq!(downlink_topic(&deliveries[0].1), "devices/plain");
    }
    let on_elapsed = on_start.elapsed();
    let on_rate = on_iters as f64 / on_elapsed.as_secs_f64();
    let on_avg_ns = on_elapsed.as_nanos() as f64 / on_iters as f64;
    println!(
        "rewrite broker workload (on, 64-rule table, last-rule hit): {on_rate:.0} msgs/sec, avg {on_avg_ns:.1} ns/msg ({on_iters} apply_publish rounds in {on_elapsed:?})"
    );
    println!(
        "rewrite counters after workload: {:?}",
        shared.router.rewrite_stats()
    );
}
