use super::*;
use crate::link_harness::handle_connection;
use tokio::net::TcpListener;

/// Subscribe one shared-group member through the broker subscribe
/// path (`apply_subscribe` with a `$share/<group>/tasks` filter) and
/// bind its session to `conn`, so later publishes deliver live.
#[cfg(test)]
async fn subscribe_shared_member(shared: &Shared, conn: u64, client: &str, group: &str) {
    let filter = format!("$share/{group}/tasks");
    let frame = subscribe_frame(
        conn,
        1,
        encode_subscribe_meta(1, client, &[(filter.as_str(), 0)]),
    );
    let (reply, _) = apply_subscribe(&frame, shared).await;
    reply.expect("shared subscribe replies");
    let (session, _) = shared.sessions.get_or_create(client, true);
    *session.conn_id.write() = Some(conn);
}

/// Publish one message through the broker ingress pipeline and
/// return the receiving connection ids (one per delivery).
#[cfg(test)]
async fn publish_shared_once(
    shared: &Shared,
    topic: &Topic,
    publisher: Option<&str>,
    payload: &[u8],
) -> Vec<u64> {
    ingress_pipeline_with_publisher(
        shared,
        topic,
        QoS::AtMostOnce,
        false,
        &Bytes::from(payload.to_vec()),
        publisher,
        broker_session::tenant::DEFAULT_TENANT_ID,
    )
    .await
    .into_iter()
    .map(|(conn, _)| conn)
    .collect()
}

/// B4-02: round-robin through the broker rotates across members.
#[tokio::test]
async fn shared_strategy_round_robin_rotates_through_broker() {
    let shared = test_shared();
    for (conn, client) in [(101u64, "rr-a"), (102, "rr-b"), (103, "rr-c")] {
        subscribe_shared_member(&shared, conn, client, "rr").await;
    }
    shared
        .router
        .set_shared_strategy("rr", "round_robin")
        .expect("set");
    let topic = Topic::new("tasks").unwrap();
    let mut owners = Vec::new();
    for i in 0..6u8 {
        let got = publish_shared_once(&shared, &topic, Some("pub"), &[i]).await;
        assert_eq!(got.len(), 1, "exactly one member per publish");
        owners.push(got[0]);
    }
    assert_eq!(owners, vec![101, 102, 103, 101, 102, 103]);
}

/// B4-02: `hash_clientid` through the broker pins one member per
/// publisher id.
#[tokio::test]
async fn shared_strategy_hash_clientid_stable_through_broker() {
    let shared = test_shared();
    for (conn, client) in [(111u64, "hc-a"), (112, "hc-b"), (113, "hc-c")] {
        subscribe_shared_member(&shared, conn, client, "hc").await;
    }
    shared
        .router
        .set_shared_strategy("hc", "hash_clientid")
        .expect("set");
    let topic = Topic::new("tasks").unwrap();
    for publisher in ["pub-1", "pub-2", "pub-3"] {
        let first = publish_shared_once(&shared, &topic, Some(publisher), b"x").await;
        assert_eq!(first.len(), 1);
        for _ in 0..5 {
            let again = publish_shared_once(&shared, &topic, Some(publisher), b"x").await;
            assert_eq!(again, first, "one publisher id pins one member");
        }
    }
}

/// B4-02: `hash_topic` through the broker pins one member per topic.
#[tokio::test]
async fn shared_strategy_hash_topic_stable_through_broker() {
    let shared = test_shared();
    for (conn, client) in [(121u64, "ht-a"), (122, "ht-b"), (123, "ht-c")] {
        subscribe_shared_member(&shared, conn, client, "ht").await;
    }
    shared
        .router
        .set_shared_strategy("ht", "hash_topic")
        .expect("set");
    let topic = Topic::new("tasks").unwrap();
    let first = publish_shared_once(&shared, &topic, Some("pub"), b"x").await;
    assert_eq!(first.len(), 1);
    for _ in 0..5 {
        let again = publish_shared_once(&shared, &topic, Some("other-pub"), b"x").await;
        assert_eq!(
            again, first,
            "one topic pins one member whatever the publisher"
        );
    }
}

/// B4-02: sticky through the broker holds one member while the
/// membership is stable.
#[tokio::test]
async fn shared_strategy_sticky_stable_through_broker() {
    let shared = test_shared();
    for (conn, client) in [(131u64, "st-a"), (132, "st-b"), (133, "st-c")] {
        subscribe_shared_member(&shared, conn, client, "st").await;
    }
    shared
        .router
        .set_shared_strategy("st", "sticky")
        .expect("set");
    let topic = Topic::new("tasks").unwrap();
    let first = publish_shared_once(&shared, &topic, Some("pub"), b"x").await;
    assert_eq!(first.len(), 1);
    for i in 0..10u8 {
        let again = publish_shared_once(&shared, &topic, Some("pub"), &[i]).await;
        assert_eq!(again, first, "sticky holds while members stable");
    }
}

/// B4-02: random through the broker shows liveness across members.
#[tokio::test]
async fn shared_strategy_random_live_through_broker() {
    let shared = test_shared();
    for (conn, client) in [(141u64, "rn-a"), (142, "rn-b"), (143, "rn-c")] {
        subscribe_shared_member(&shared, conn, client, "rn").await;
    }
    shared
        .router
        .set_shared_strategy("rn", "random")
        .expect("set");
    let topic = Topic::new("tasks").unwrap();
    let mut seen = std::collections::HashSet::new();
    for i in 0..60u8 {
        let got = publish_shared_once(&shared, &topic, Some("pub"), &[i]).await;
        assert_eq!(got.len(), 1);
        seen.insert(got[0]);
    }
    assert_eq!(
        seen.len(),
        3,
        "random must reach every member, saw {seen:?}"
    );
}

/// B4-02: an unknown strategy name is rejected, never mapped to
/// round-robin: the set call fails and the group keeps its default.
#[tokio::test]
async fn shared_strategy_unknown_rejected_through_broker() {
    let shared = test_shared();
    subscribe_shared_member(&shared, 151, "ux-a", "ux").await;
    subscribe_shared_member(&shared, 152, "ux-b", "ux").await;
    let err = shared
        .router
        .set_shared_strategy("ux", "least-connections")
        .expect_err("unknown strategy must fail");
    assert!(err.to_string().contains("round_robin"));
    assert_eq!(
        shared.router.shared_strategy("ux"),
        broker_router::SharedStrategy::RoundRobin
    );
    // The group still balances (default), it was not left broken.
    let topic = Topic::new("tasks").unwrap();
    let got = publish_shared_once(&shared, &topic, Some("pub"), b"x").await;
    assert_eq!(got.len(), 1);
}

/// B4-02: plain subscribers alongside shared members each receive a
/// copy through the broker.
#[tokio::test]
async fn shared_and_plain_coexist_through_broker() {
    let shared = test_shared();
    subscribe_shared_member(&shared, 161, "co-a", "co").await;
    subscribe_shared_member(&shared, 162, "co-b", "co").await;
    let frame = subscribe_frame(
        163,
        1,
        encode_subscribe_meta(1, "co-plain", &[("tasks", 0)]),
    );
    let (reply, _) = apply_subscribe(&frame, &shared).await;
    reply.expect("plain subscribe replies");
    let (session, _) = shared.sessions.get_or_create("co-plain", true);
    *session.conn_id.write() = Some(163);
    let topic = Topic::new("tasks").unwrap();
    for i in 0..4u8 {
        let mut got = publish_shared_once(&shared, &topic, Some("pub"), &[i]).await;
        got.sort_unstable();
        assert_eq!(got.len(), 2, "plain plus exactly one member");
        assert!(got.contains(&163), "plain subscriber always present");
        assert!(
            got.contains(&161) || got.contains(&162),
            "exactly one member joins, saw {got:?}"
        );
    }
}

/// B4-08: subscribe storm alongside steady publishes through the
/// broker. A steady subscriber on `b408/steady` must receive every
/// publish while churn tasks subscribe distinct
/// `b408/churn/<t>/<i>` filters through the real broker subscribe
/// ingress (`apply_subscribe` at `crates/broker-node/src/main.rs`,
/// driven by the subscribe event) and clean them up through the
/// kernel unsubscribe path. The steady path goes through the broker
/// ingress pipeline (`ingress_pipeline_with_publisher`, driven by
/// the publish delivery event), asserting 100% steady delivery plus
/// exact route, session-mirror, wildcard and shared-group agreement.
/// Storm coverage for the same workload family also runs at the
/// router level in `crates/broker-router/src/lib.rs`
/// (`subscribe_storm_alongside_matches_keeps_semantics`, baseline vs
/// CoW); this harness exercises the through-broker path, reporting
/// min/median/max across repeats to the gates log. No time
/// threshold is asserted so the test is stable on loaded runners.
#[tokio::test]
async fn router_lock_contention_subscribe_storm_with_steady_publishes() {
    use std::time::Instant;

    const CHURN_TASKS: usize = 4;
    const FILTERS_PER_TASK: usize = 25;
    const STEADY_PUBLISHES: usize = 50;
    const REPEATS: usize = 3;

    async fn churn_subscribe_one(shared: &Shared, task: usize, i: usize) -> f64 {
        let client = format!("b408-c{task}-{i}");
        let conn = 9100 + task as u64 * 100 + i as u64;
        let filter_str = format!("b408/churn/{task}/{i}");
        // Bind before subscribe (production order): the session must
        // exist so `apply_subscribe` can mirror the grant on it. A
        // `get_or_create(true)` after the subscribe would replace the
        // session and wipe the mirror.
        let (session, _) = shared.sessions.get_or_create(&client, true);
        *session.conn_id.write() = Some(conn);
        let frame = subscribe_frame(
            conn,
            1,
            encode_subscribe_meta(1, &client, &[(filter_str.as_str(), 0)]),
        );
        let start = Instant::now();
        let (reply, _) = apply_subscribe(&frame, shared).await;
        let us = start.elapsed().as_secs_f64() * 1e6;
        reply.expect("churn subscribe replies");
        us
    }

    let mut delivery_rates: Vec<f64> = Vec::with_capacity(REPEATS);
    let mut churn_rates: Vec<f64> = Vec::with_capacity(REPEATS);
    let mut all_sub_us: Vec<f64> = Vec::new();
    let mut all_pub_us: Vec<f64> = Vec::new();

    for _ in 0..REPEATS {
        let shared = test_shared();
        // Steady subscriber through the broker subscribe path. Bind
        // first so the subscribe mirror lands on this session (a
        // clean-start create after the subscribe would wipe it).
        let (session, _) = shared.sessions.get_or_create("b408-steady", true);
        *session.conn_id.write() = Some(4081);
        let steady_frame = subscribe_frame(
            4081,
            1,
            encode_subscribe_meta(1, "b408-steady", &[("b408/steady", 0)]),
        );
        let (reply, _) = apply_subscribe(&steady_frame, &shared).await;
        reply.expect("steady subscribe replies");

        let topic = Topic::new("b408/steady").unwrap();
        let start = Instant::now();
        // Churn runs as concurrent broker-ingress tasks while the
        // main task publishes steadily: deliveries (readers) must
        // never wait on subscribes (writers) longer than a snapshot
        // load (`Router::root` via `load_full` in
        // `crates/broker-router/src/lib.rs`).
        let mut churn_handles = Vec::with_capacity(CHURN_TASKS);
        for task in 0..CHURN_TASKS {
            let shared = shared.clone();
            churn_handles.push(tokio::spawn(async move {
                let mut lat = Vec::with_capacity(FILTERS_PER_TASK);
                for i in 0..FILTERS_PER_TASK {
                    lat.push(churn_subscribe_one(&shared, task, i).await);
                }
                lat
            }));
        }
        let mut publish_us = Vec::with_capacity(STEADY_PUBLISHES);
        for i in 0..STEADY_PUBLISHES {
            let one = Instant::now();
            let got = publish_shared_once(&shared, &topic, Some("b408-pub"), &[i as u8]).await;
            publish_us.push(one.elapsed().as_secs_f64() * 1e6);
            assert_eq!(
                got,
                vec![4081],
                "steady publish {i} delivers exactly once under storm"
            );
        }
        let mut sub_us: Vec<f64> = Vec::new();
        for handle in churn_handles {
            sub_us.extend(handle.await.expect("churn task joins"));
        }
        assert_eq!(
            sub_us.len(),
            CHURN_TASKS * FILTERS_PER_TASK,
            "every churn subscribe reports latency"
        );
        let elapsed = start.elapsed();
        let elapsed_secs = elapsed.as_secs_f64().max(f64::EPSILON);
        delivery_rates.push(STEADY_PUBLISHES as f64 / elapsed_secs);
        let churn_ops = (CHURN_TASKS * FILTERS_PER_TASK) as f64;
        churn_rates.push(churn_ops / elapsed_secs);
        all_sub_us.extend_from_slice(&sub_us);
        all_pub_us.extend_from_slice(&publish_us);

        // Storm filters resolve through the broker, then are removed
        // through the kernel unsubscribe path (no MQTT unsubscribe
        // ingress exists; this mirrors `detach`).
        for task in 0..CHURN_TASKS {
            for i in 0..FILTERS_PER_TASK {
                let probe =
                    Topic::new(format!("b408/churn/{task}/{i}")).expect("valid probe topic");
                let matched = shared.router.matches(&probe);
                assert_eq!(matched.len(), 1, "churn filter {task}/{i} resolves");
                assert_eq!(
                    matched.iter().next().unwrap().client_id.as_ref(),
                    format!("b408-c{task}-{i}")
                );
            }
        }
        // Routing correctness under churn, through the broker
        // delivery path: every message published after a churn
        // subscribe completes reaches exactly that subscriber.
        for task in 0..CHURN_TASKS {
            for i in 0..FILTERS_PER_TASK {
                let probe =
                    Topic::new(format!("b408/churn/{task}/{i}")).expect("valid probe topic");
                let got = publish_shared_once(&shared, &probe, Some("b408-pub"), b"x").await;
                assert_eq!(
                    got,
                    vec![9100 + task as u64 * 100 + i as u64],
                    "churn subscriber {task}/{i} receives after its subscribe"
                );
            }
        }
        for task in 0..CHURN_TASKS {
            for i in 0..FILTERS_PER_TASK {
                let filter = broker_protocol::TopicFilter::new(format!("b408/churn/{task}/{i}"))
                    .expect("valid churn filter");
                shared
                    .router
                    .unsubscribe(&filter, &format!("b408-c{task}-{i}"));
                shared
                    .sessions
                    .remove_subscription(&format!("b408-c{task}-{i}"), &filter);
            }
        }
        // Storm filters are gone; the steady route still resolves to
        // exactly the steady client, and the session mirror agrees.
        let steady = shared.router.matches(&topic);
        assert_eq!(steady.len(), 1);
        assert_eq!(
            steady.iter().next().unwrap().client_id.as_ref(),
            "b408-steady"
        );
        let probe = Topic::new("b408/churn/0/0").expect("valid probe topic");
        assert!(
            shared.router.matches(&probe).is_empty(),
            "churn filters fully removed after storm"
        );
        // And nothing reaches a churn subscriber after its
        // unsubscribe completes: every churn topic goes silent
        // through the broker delivery path.
        for task in 0..CHURN_TASKS {
            for i in 0..FILTERS_PER_TASK {
                let silent =
                    Topic::new(format!("b408/churn/{task}/{i}")).expect("valid probe topic");
                let got = publish_shared_once(&shared, &silent, Some("b408-pub"), b"x").await;
                assert!(
                    got.is_empty(),
                    "churn topic {task}/{i} silent after unsubscribe, got {got:?}"
                );
            }
        }
        let steady_session = shared
            .sessions
            .get("b408-steady")
            .expect("steady session present");
        assert!(
            steady_session
                .subscriptions
                .read()
                .keys()
                .any(|f| f.as_str() == "b408/steady"),
            "session mirror keeps the steady subscription"
        );
        // Wildcard levels still agree through the broker ingress.
        // Bind first so the mirror survives (see steady note above).
        let (wild_session, _) = shared.sessions.get_or_create("b408-wild", true);
        *wild_session.conn_id.write() = Some(4082);
        let wild_frame = subscribe_frame(
            4082,
            1,
            encode_subscribe_meta(1, "b408-wild", &[("b408/+", 0)]),
        );
        let (reply, _) = apply_subscribe(&wild_frame, &shared).await;
        reply.expect("wildcard subscribe replies");
        let wild = shared.router.matches(&topic);
        assert_eq!(wild.len(), 2);
        assert!(wild.iter().any(|s| s.client_id.as_ref() == "b408-wild"));
        assert!(wild.iter().any(|s| s.client_id.as_ref() == "b408-steady"));
        let wild_filter = broker_protocol::TopicFilter::new("b408/+").expect("valid wild filter");
        shared.router.unsubscribe(&wild_filter, "b408-wild");
        shared
            .sessions
            .remove_subscription("b408-wild", &wild_filter);
        let after = shared.router.matches(&topic);
        assert_eq!(after.len(), 1);
        assert_eq!(
            after.iter().next().unwrap().client_id.as_ref(),
            "b408-steady"
        );
        // Shared-group reduction still agrees through the broker.
        for (conn, client) in [(4181u64, "b408-sg-a"), (4182, "b408-sg-b")] {
            let (session, _) = shared.sessions.get_or_create(client, true);
            *session.conn_id.write() = Some(conn);
            let frame = subscribe_frame(
                conn,
                1,
                encode_subscribe_meta(1, client, &[("$share/b408sg/b408/sg", 0)]),
            );
            let (reply, _) = apply_subscribe(&frame, &shared).await;
            reply.expect("shared subscribe replies");
        }
        let sg_topic = Topic::new("b408/sg").unwrap();
        let sg_got = publish_shared_once(&shared, &sg_topic, Some("b408-pub"), b"x").await;
        assert_eq!(sg_got.len(), 1, "shared group reduces to one member");
        let sg_inner = broker_protocol::TopicFilter::new("b408/sg").expect("valid sg filter");
        for client in ["b408-sg-a", "b408-sg-b"] {
            shared.router.unsubscribe(&sg_inner, client);
            shared.sessions.remove_subscription(client, &sg_inner);
        }
    }
    delivery_rates.sort_by(|a, b| a.total_cmp(b));
    churn_rates.sort_by(|a, b| a.total_cmp(b));
    all_sub_us.sort_by(|a, b| a.total_cmp(b));
    all_pub_us.sort_by(|a, b| a.total_cmp(b));
    let churn_ops = (CHURN_TASKS * FILTERS_PER_TASK) as f64;
    eprintln!(
        "B4-08 broker storm: {STEADY_PUBLISHES} steady deliveries + {churn_ops:.0} broker-ingress subscribes per repeat; \
        deliveries/s min={:.0} median={:.0} max={:.0}; churn subscribes/s min={:.0} median={:.0} max={:.0} \
        (x {REPEATS} repeats)",
        delivery_rates[0],
        delivery_rates[REPEATS / 2],
        delivery_rates[REPEATS - 1],
        churn_rates[0],
        churn_rates[REPEATS / 2],
        churn_rates[REPEATS - 1],
    );
    let smin = all_sub_us[0];
    let smed = all_sub_us[all_sub_us.len() / 2];
    let smax = all_sub_us[all_sub_us.len() - 1];
    eprintln!(
        "B4-08 broker subscribe latency under storm: min={smin:.1}us median={smed:.1}us max={smax:.1}us \
        ({} subscribes via apply_subscribe)",
        all_sub_us.len(),
    );
    let pmin = all_pub_us[0];
    let pmed = all_pub_us[all_pub_us.len() / 2];
    let pmax = all_pub_us[all_pub_us.len() - 1];
    eprintln!(
        "B4-08 steady publish latency under storm: min={pmin:.1}us median={pmed:.1}us max={pmax:.1}us \
        ({} publishes via ingress_pipeline_with_publisher)",
        all_pub_us.len(),
    );
    assert!(
        smin > 0.0 && smin.is_finite() && smax.is_finite(),
        "broker storm must report positive finite subscribe latency"
    );
    assert!(
        pmin > 0.0 && pmin.is_finite() && pmax.is_finite(),
        "broker storm must report positive finite publish latency"
    );
    assert!(
        delivery_rates[0] > 0.0 && delivery_rates[0].is_finite(),
        "broker storm must report positive delivery throughput"
    );
    assert!(
        churn_rates[0] > 0.0 && churn_rates[0].is_finite(),
        "broker storm must report positive churn throughput"
    );
}

#[tokio::test]
async fn shared_subscriptions_balance_across_workers() {
    let shared = test_shared();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let server = tokio::spawn(async move {
        for _ in 0..4 {
            let (stream, _) = listener.accept().await.expect("accept");
            let shared = shared.clone();
            tokio::spawn(async move {
                handle_connection(stream, shared).await.expect("handle");
            });
        }
        std::future::pending::<()>().await;
    });

    // Two workers share one group; one plain subscriber listens too.
    let worker_a = bind_client(addr, 711, "worker-a", true).await;
    worker_a
        .send(subscribe_frame(
            711,
            2,
            encode_subscribe_meta(1, "worker-a", &[("$share/pool/work/jobs", 0)]),
        ))
        .await
        .expect("subscribe a");
    assert_eq!(
        worker_a.recv().await.expect("suback a").header.opcode,
        OpCode::SubAckOut
    );

    let worker_b = bind_client(addr, 712, "worker-b", true).await;
    worker_b
        .send(subscribe_frame(
            712,
            2,
            encode_subscribe_meta(1, "worker-b", &[("$share/pool/work/jobs", 0)]),
        ))
        .await
        .expect("subscribe b");
    assert_eq!(
        worker_b.recv().await.expect("suback b").header.opcode,
        OpCode::SubAckOut
    );

    let plain = bind_client(addr, 713, "plain-c", true).await;
    plain
        .send(subscribe_frame(
            713,
            2,
            encode_subscribe_meta(1, "plain-c", &[("work/jobs", 0)]),
        ))
        .await
        .expect("subscribe c");
    assert_eq!(
        plain.recv().await.expect("suback c").header.opcode,
        OpCode::SubAckOut
    );

    // Publisher emits two jobs.
    let publ = bind_client(addr, 714, "producer", true).await;
    for (seq, job) in [(3u64, "job-1"), (4u64, "job-2")] {
        let (meta, payload) = encode_publish_meta("work/jobs", 0, 0, false, job.as_bytes());
        publ.send(publish_frame(714, seq, meta, payload))
            .await
            .expect("publish job");
    }

    // Deterministic round-robin (members sort by client id): job-1
    // goes to worker-a, job-2 to worker-b, exactly one each.
    let first_a = worker_a.recv().await.expect("worker-a delivery");
    assert_eq!(first_a.header.conn_id, 711);
    assert_eq!(first_a.payload, Bytes::from_static(b"job-1"));
    let first_b = worker_b.recv().await.expect("worker-b delivery");
    assert_eq!(first_b.header.conn_id, 712);
    assert_eq!(first_b.payload, Bytes::from_static(b"job-2"));
    for (worker, name) in [(&worker_a, "a"), (&worker_b, "b")] {
        let extra =
            tokio::time::timeout(std::time::Duration::from_millis(300), worker.recv()).await;
        assert!(extra.is_err(), "worker {name} must receive exactly one job");
    }

    // The plain subscriber still gets the full fan-out: both jobs.
    for expected in [b"job-1".as_slice(), b"job-2".as_slice()] {
        let got = plain.recv().await.expect("plain delivery");
        assert_eq!(got.header.conn_id, 713);
        assert_eq!(got.payload, Bytes::from(expected));
    }

    server.abort();
}
