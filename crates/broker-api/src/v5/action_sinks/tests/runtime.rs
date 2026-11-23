use super::*;

use std::sync::atomic::Ordering as AtomicOrdering;

#[test]
fn resource_opts_accept_every_documented_default() {
    let raw = serde_json::json!({
        "batch_size": 1,
        "batch_time": "0ms",
        "dispatch_strategy": "per_clientid",
        "health_check_interval": "15s",
        "health_check_interval_jitter": "0ms",
        "health_check_timeout": "60s",
        "inflight_window": 100,
        "max_buffer_bytes": "256MB",
        "query_mode": "async",
        "request_ttl": "45s",
        "worker_pool_size": 16,
    });
    let opts = ActionResourceOpts::parse("act", &raw).expect("defaults parse");
    assert_eq!(opts, ActionResourceOpts::default());
}

#[test]
fn resource_opts_refuse_a_non_default_value_this_runtime_cannot_apply() {
    let above_semaphore_max = (tokio::sync::Semaphore::MAX_PERMITS as u64) + 1;
    for (raw, field) in [
        (
            serde_json::json!({"dispatch_strategy": "random"}),
            "dispatch_strategy",
        ),
        (
            serde_json::json!({"health_check_interval_jitter": "1s"}),
            "health_check_interval_jitter",
        ),
        (serde_json::json!({"no_such_option": 1}), "no_such_option"),
        (serde_json::json!({"batch_time": "infinity"}), "batch_time"),
        (
            serde_json::json!({"max_buffer_bytes": "0"}),
            "max_buffer_bytes",
        ),
        (
            serde_json::json!({"inflight_window": above_semaphore_max}),
            "inflight_window",
        ),
    ] {
        let error = ActionResourceOpts::parse("act", &raw).expect_err("the value is refused");
        assert!(error.contains(field), "{error}");
        assert!(error.contains("act"), "{error}");
    }
}

#[test]
fn resource_opts_apply_the_health_interval_and_the_legacy_batch_timeout() {
    let opts = ActionResourceOpts::parse(
        "act",
        &serde_json::json!({
            "health_check_interval": "30s",
            "batch_size": 10,
            "batch_timeout_ms": 25,
        }),
    )
    .expect("a non-default health interval and the legacy field parse");
    assert_eq!(opts.health_check_interval, Duration::from_secs(30));
    assert_eq!(opts.batch_time, Duration::from_millis(25));
    assert_eq!(opts.batch_size, 10);
}

#[test]
fn resource_opts_accept_a_non_default_health_check_timeout() {
    let opts =
        ActionResourceOpts::parse("act", &serde_json::json!({"health_check_timeout": "30s"}))
            .expect("a non-default health check timeout is now accepted");
    assert_eq!(
        opts.health_check_timeout,
        Some(std::time::Duration::from_secs(30))
    );
}

#[tokio::test]
async fn runtime_health_probe_marks_a_closed_target_unhealthy() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind target");
    let address = listener.local_addr().expect("target address");
    drop(listener);
    let inner = Arc::new(BatchSink::default());
    let opts = ActionResourceOpts {
        health_check_interval: Duration::from_millis(20),
        ..ActionResourceOpts::default()
    };
    let metrics = Arc::new(ActionMetrics::new());
    let runtime =
        ActionRuntimeSink::new(inner, opts, Arc::clone(&metrics), Some(address.to_string()));
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Some(health) = runtime.health() {
            if !health.healthy {
                assert!(health.reason.is_some(), "the probe names the failure");
                break;
            }
        }
        assert!(Instant::now() < deadline, "the probe marked the target");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn runtime_batches_by_size_and_gives_the_driver_one_batch() {
    let inner = Arc::new(BatchSink::default());
    let opts = ActionResourceOpts {
        batch_size: 3,
        batch_time: Duration::from_secs(1),
        ..ActionResourceOpts::default()
    };
    let (runtime, metrics) = runtime(Arc::clone(&inner), opts);
    let topic = Topic::new("t/act").expect("topic");
    let payload = Bytes::from_static(b"{}");
    for _ in 0..3 {
        runtime
            .send(&topic, &payload, QoS::AtMostOnce)
            .await
            .expect("send joins the buffer");
    }
    assert_eq!(
        inner
            .batches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone(),
        vec![3]
    );
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.matched, 3);
    assert_eq!(snapshot.success, 3);
    assert_eq!(snapshot.failed, 0);
    assert_eq!(snapshot.queuing, 0);
}

#[tokio::test]
async fn runtime_flushes_a_partial_batch_when_the_batch_time_elapses() {
    let inner = Arc::new(BatchSink::default());
    let opts = ActionResourceOpts {
        batch_size: 100,
        batch_time: Duration::from_millis(20),
        ..ActionResourceOpts::default()
    };
    let (runtime, metrics) = runtime(Arc::clone(&inner), opts);
    let topic = Topic::new("t/time").expect("topic");
    let payload = Bytes::from_static(b"{}");
    for _ in 0..2 {
        runtime
            .send(&topic, &payload, QoS::AtMostOnce)
            .await
            .expect("send joins the buffer");
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while inner
        .batches
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .is_empty()
        && Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        inner
            .batches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone(),
        vec![2]
    );
    assert_eq!(metrics.snapshot().success, 2);
}

#[tokio::test]
async fn runtime_bounds_the_buffer_and_counts_queue_full_drops() {
    // One row holds 1 topic byte plus 10 payload bytes plus the
    // 64-byte per-row overhead, 75 bytes. A 150-byte bound holds two.
    let inner = Arc::new(BatchSink::default());
    let opts = ActionResourceOpts {
        batch_size: 100,
        batch_time: Duration::from_secs(10),
        max_buffer_bytes: 150,
        ..ActionResourceOpts::default()
    };
    let (runtime, metrics) = runtime(Arc::clone(&inner), opts);
    let topic = Topic::new("t").expect("topic");
    let payload = Bytes::from_static(b"0123456789");
    for _ in 0..3 {
        runtime
            .send(&topic, &payload, QoS::AtMostOnce)
            .await
            .expect("an overflow drop is not an error");
    }
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.matched, 3);
    assert_eq!(snapshot.dropped, 1);
    assert_eq!(snapshot.dropped_queue_full, 1);
    assert_eq!(snapshot.queuing, 2);
    assert!(inner
        .batches
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .is_empty());
    // A row larger than the bound alone is refused. The buffer keeps
    // its two rows.
    let big = Bytes::from(vec![b'x'; 200]);
    runtime
        .send(&topic, &big, QoS::AtMostOnce)
        .await
        .expect("an oversized drop is not an error");
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.matched, 4);
    assert_eq!(snapshot.dropped, 2);
    assert_eq!(snapshot.dropped_queue_full, 2);
    assert_eq!(snapshot.queuing, 2);
}

#[tokio::test]
async fn runtime_batch_size_applies_only_when_batch_time_is_positive() {
    // A zero batch time sends each row at once: two rows give two
    // driver calls even with `batch_size` 2.
    let inner = Arc::new(BatchSink::default());
    let opts = ActionResourceOpts {
        batch_size: 2,
        batch_time: Duration::ZERO,
        ..ActionResourceOpts::default()
    };
    let (idle_runtime, _) = runtime(Arc::clone(&inner), opts);
    let topic = Topic::new("t/size").expect("topic");
    let payload = Bytes::from_static(b"{}");
    for _ in 0..2 {
        idle_runtime
            .send(&topic, &payload, QoS::AtMostOnce)
            .await
            .expect("send joins the buffer");
    }
    assert_eq!(
        inner
            .batches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone(),
        vec![1, 1]
    );
    // A positive batch time groups the rows: two rows give one call.
    let inner = Arc::new(BatchSink::default());
    let opts = ActionResourceOpts {
        batch_size: 2,
        batch_time: Duration::from_millis(200),
        ..ActionResourceOpts::default()
    };
    let (runtime, metrics) = runtime(Arc::clone(&inner), opts);
    for _ in 0..2 {
        runtime
            .send(&topic, &payload, QoS::AtMostOnce)
            .await
            .expect("send joins the buffer");
    }
    assert_eq!(
        inner
            .batches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone(),
        vec![2]
    );
    assert_eq!(metrics.snapshot().success, 2);
}

#[tokio::test]
async fn runtime_drops_a_row_older_than_request_ttl() {
    // The TTL is smaller than the batch time. One row waits past its
    // TTL, so the send drops it and counts the drop.
    let inner = Arc::new(BatchSink::default());
    let opts = ActionResourceOpts {
        batch_size: 2,
        batch_time: Duration::from_millis(200),
        request_ttl: Some(Duration::from_millis(50)),
        ..ActionResourceOpts::default()
    };
    let (runtime, metrics) = runtime(Arc::clone(&inner), opts);
    let topic = Topic::new("t/ttl").expect("topic");
    let payload = Bytes::from_static(b"{}");
    runtime
        .send(&topic, &payload, QoS::AtMostOnce)
        .await
        .expect("send joins the buffer");
    let deadline = Instant::now() + Duration::from_secs(3);
    while metrics.snapshot().dropped == 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.dropped, 1);
    assert_eq!(snapshot.dropped_other, 1);
    assert_eq!(snapshot.success, 0);
    assert!(inner
        .batches
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .is_empty());
}

#[tokio::test]
async fn runtime_runs_inflight_driver_calls_together() {
    // Ten rows with a 200 ms driver each finish in less than 1 s only
    // when the driver calls run together.
    let inner = Arc::new(BatchSink {
        delay: Duration::from_millis(200),
        ..BatchSink::default()
    });
    let opts = ActionResourceOpts {
        batch_size: 1,
        batch_time: Duration::ZERO,
        inflight_window: 10,
        worker_pool_size: 1,
        ..ActionResourceOpts::default()
    };
    let (runtime, metrics) = runtime(Arc::clone(&inner), opts);
    let runtime = Arc::new(runtime);
    let topic = Topic::new("t/join").expect("topic");
    let payload = Bytes::from_static(b"{}");
    let start = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..10 {
        let runtime = Arc::clone(&runtime);
        let topic = topic.clone();
        let payload = payload.clone();
        tasks.push(tokio::spawn(async move {
            runtime.send(&topic, &payload, QoS::AtMostOnce).await
        }));
    }
    for task in tasks {
        task.await
            .expect("send task runs")
            .expect("send joins the buffer");
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(1),
        "ten slow calls ran together: {elapsed:?}"
    );
    assert!(
        inner.max_active.load(AtomicOrdering::SeqCst) > 1,
        "more than one driver call ran at once"
    );
    assert_eq!(metrics.snapshot().success, 10);
}

#[tokio::test]
async fn runtime_drop_counts_waiting_rows_as_dropped() {
    // Two rows wait in the buffer. Dropping the runtime counts both
    // rows as dropped instead of losing them silently.
    let inner = Arc::new(BatchSink::default());
    let opts = ActionResourceOpts {
        batch_size: 100,
        batch_time: Duration::from_secs(10),
        ..ActionResourceOpts::default()
    };
    let (runtime, metrics) = runtime(Arc::clone(&inner), opts);
    let topic = Topic::new("t/gone").expect("topic");
    let payload = Bytes::from_static(b"{}");
    for _ in 0..2 {
        runtime
            .send(&topic, &payload, QoS::AtMostOnce)
            .await
            .expect("send joins the buffer");
    }
    assert_eq!(metrics.snapshot().queuing, 2);
    drop(runtime);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.queuing, 0);
    assert_eq!(snapshot.dropped, 2);
    assert_eq!(snapshot.dropped_other, 2);
}

#[test]
fn resource_opts_refuse_batch_time_larger_than_request_ttl() {
    let error = ActionResourceOpts::parse(
        "act",
        &serde_json::json!({"batch_time": "10s", "request_ttl": "5s"}),
    )
    .expect_err("a batch time larger than the TTL is refused");
    assert!(error.contains("batch_time"), "{error}");
    ActionResourceOpts::parse(
        "act",
        &serde_json::json!({"batch_time": "5s", "request_ttl": "5s"}),
    )
    .expect("an equal batch time and TTL parse");
}

#[tokio::test]
async fn runtime_health_reason_hides_the_target() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind target");
    let address = listener.local_addr().expect("target address");
    drop(listener);
    let reason = probe_target(&address.to_string(), Some(Duration::from_secs(2)))
        .await
        .expect_err("a closed target fails");
    assert!(
        !reason.contains(&address.to_string()),
        "the reason hides the target: {reason}"
    );
}

#[test]
fn probe_dial_target_uses_the_scheme_default_port() {
    assert_eq!(probe_dial_target("https://example.com"), "example.com:443");
    assert_eq!(
        probe_dial_target("https://example.com:8443/path"),
        "example.com:8443"
    );
    assert_eq!(probe_dial_target("http://example.com"), "example.com:80");
    assert_eq!(probe_dial_target("example.com:1234"), "example.com:1234");
    assert_eq!(probe_dial_target("example.com"), "example.com:80");
}

#[tokio::test]
async fn runtime_holds_the_inflight_window_and_counts_expired_requests() {
    let inner = Arc::new(BatchSink {
        delay: Duration::from_millis(60),
        ..BatchSink::default()
    });
    let opts = ActionResourceOpts {
        batch_size: 10,
        batch_time: Duration::from_millis(1),
        inflight_window: 1,
        worker_pool_size: 1,
        request_ttl: Some(Duration::from_millis(20)),
        ..ActionResourceOpts::default()
    };
    let (runtime, metrics) = runtime(Arc::clone(&inner), opts);
    let topic = Topic::new("t/ttl").expect("topic");
    let payload = Bytes::from_static(b"{}");
    for _ in 0..2 {
        runtime
            .send(&topic, &payload, QoS::AtMostOnce)
            .await
            .expect("send joins the buffer");
    }
    let deadline = Instant::now() + Duration::from_secs(3);
    while (metrics.snapshot().dropped == 0
        || metrics.snapshot().success == 0
        || metrics.snapshot().inflight > 0)
        && Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.success, 1);
    assert_eq!(snapshot.dropped, 1);
    assert_eq!(snapshot.dropped_other, 1);
    assert_eq!(snapshot.failed, 0);
    assert_eq!(snapshot.inflight, 0);
    assert!(inner.max_active.load(AtomicOrdering::SeqCst) <= 1);
}

#[tokio::test]
async fn runtime_sync_mode_flushes_a_partial_batch_before_it_returns() {
    let inner = Arc::new(BatchSink::default());
    let opts = ActionResourceOpts {
        batch_size: 100,
        batch_time: Duration::from_secs(10),
        query_mode: ActionQueryMode::Sync,
        ..ActionResourceOpts::default()
    };
    let (runtime, metrics) = runtime(Arc::clone(&inner), opts);
    let topic = Topic::new("t/sync").expect("topic");
    let payload = Bytes::from_static(b"{}");
    runtime
        .send(&topic, &payload, QoS::AtMostOnce)
        .await
        .expect("sync send flushes");
    assert_eq!(
        inner
            .batches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone(),
        vec![1]
    );
    assert_eq!(metrics.snapshot().success, 1);
}

#[tokio::test]
async fn runtime_worker_pool_size_caps_one_driver_batch_chunk() {
    let inner = Arc::new(BatchSink::default());
    let opts = ActionResourceOpts {
        batch_size: 10,
        batch_time: Duration::from_millis(1),
        inflight_window: 10,
        worker_pool_size: 2,
        ..ActionResourceOpts::default()
    };
    let (runtime, metrics) = runtime(Arc::clone(&inner), opts);
    let topic = Topic::new("t/workers").expect("topic");
    let payload = Bytes::from_static(b"{}");
    for _ in 0..5 {
        runtime
            .send(&topic, &payload, QoS::AtMostOnce)
            .await
            .expect("send joins the buffer");
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while metrics.snapshot().success < 5 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let batches = inner
        .batches
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    assert_eq!(batches.iter().sum::<usize>(), 5);
    assert!(batches.iter().all(|size| *size <= 2), "{batches:?}");
}

#[tokio::test]
async fn runtime_holds_the_inflight_window_across_worker_chunks() {
    let inner = Arc::new(BatchSink {
        delay: Duration::from_millis(60),
        ..BatchSink::default()
    });
    let opts = ActionResourceOpts {
        batch_size: 10,
        batch_time: Duration::from_millis(1),
        inflight_window: 1,
        worker_pool_size: 2,
        ..ActionResourceOpts::default()
    };
    let (runtime, metrics) = runtime(Arc::clone(&inner), opts);
    let topic = Topic::new("t/inflight").expect("topic");
    let payload = Bytes::from_static(b"{}");
    for _ in 0..4 {
        runtime
            .send(&topic, &payload, QoS::AtMostOnce)
            .await
            .expect("send joins the buffer");
    }
    let deadline = Instant::now() + Duration::from_secs(3);
    while metrics.snapshot().success < 4 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.success, 4);
    assert_eq!(snapshot.inflight, 0);
    assert!(
        inner.max_active.load(AtomicOrdering::SeqCst) <= 1,
        "the inflight window holds one driver call, worker chunks may be larger"
    );
    let batches = inner
        .batches
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    assert_eq!(batches.iter().sum::<usize>(), 4);
    assert!(batches.iter().all(|size| *size <= 2), "{batches:?}");
}

#[tokio::test]
async fn runtime_counts_matched_success_and_failed_for_one_action() {
    // Direct unit test for the counter math. The broker-path proof is
    // `ro01_action_runtime_applies_options_through_publish_and_restart`
    // in `tests/e2e_integration/`, which drives the same counters
    // through a real publish.
    let inner = Arc::new(BatchSink {
        fail: true,
        ..BatchSink::default()
    });
    let (runtime, metrics) = runtime(inner, ActionResourceOpts::default());
    let topic = Topic::new("t/fail").expect("topic");
    let payload = Bytes::from_static(b"{}");
    for _ in 0..3 {
        let _ = runtime.send(&topic, &payload, QoS::AtMostOnce).await;
    }
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.matched, 3);
    assert_eq!(snapshot.total, 3);
    assert_eq!(snapshot.success, 0);
    assert_eq!(snapshot.failed, 3);
    assert_eq!(snapshot.dropped, 0);
}

#[tokio::test]
async fn two_actions_keep_separate_counters() {
    // Direct unit test for per-action separation. The broker-path proof
    // is `mv01_pgsql_action_delivers_engine_rows_to_connector_settings`
    // in `tests/e2e_integration/`, which separates two actions through
    // a real publish.
    let good = Arc::new(BatchSink::default());
    let bad = Arc::new(BatchSink {
        fail: true,
        ..BatchSink::default()
    });
    let (good_runtime, good_metrics) = runtime(good, ActionResourceOpts::default());
    let (bad_runtime, bad_metrics) = runtime(bad, ActionResourceOpts::default());
    let topic = Topic::new("t/two").expect("topic");
    let payload = Bytes::from_static(b"{}");
    good_runtime
        .send(&topic, &payload, QoS::AtMostOnce)
        .await
        .expect("good action sends");
    let _ = bad_runtime.send(&topic, &payload, QoS::AtMostOnce).await;
    assert_eq!(good_metrics.snapshot().success, 1);
    assert_eq!(good_metrics.snapshot().failed, 0);
    assert_eq!(bad_metrics.snapshot().success, 0);
    assert_eq!(bad_metrics.snapshot().failed, 1);
}
