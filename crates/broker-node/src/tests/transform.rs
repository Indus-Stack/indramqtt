use super::*;
use crate::link_harness::handle_connection;
use tokio::net::TcpListener;

#[tokio::test]
async fn transform_chain_rewrites_payload_in_chain_order() {
    use broker_api::v5::transformations::{TransformOperation, TransformRecord};

    fn record(name: &str, value: &str) -> TransformRecord {
        TransformRecord {
            name: name.to_string(),
            topics: vec!["chain/order".to_string()],
            failure_action: "drop".to_string(),
            enable: true,
            operations: vec![TransformOperation {
                key: "payload".to_string(),
                value: value.to_string(),
            }],
            params: serde_json::json!({}),
        }
    }

    let shared = test_shared();
    // Two stored transforms append in chain order through the
    // production store plus the engine snapshot (the same path the
    // management handlers drive).
    shared
        .transformations
        .insert(record("t-a", "${payload}[t-a]"))
        .expect("first transform inserts");
    shared
        .transformations
        .insert(record("t-b", "${payload}[t-b]"))
        .expect("second transform inserts");
    // A duplicate name is rejected and overwrites nothing.
    assert!(shared.transformations.insert(record("t-a", "x")).is_err());
    shared
        .engine
        .sync_transforms(shared.transformations.steps_snapshot());
    // Keep one handle outside the server task so the test can
    // replace the chain later; the store is shared, so the replace
    // is visible to the serving task without a restart.
    let driver = shared.clone();

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

    let sub = bind_client(addr, 511, "chain-watcher", true).await;
    sub.send(subscribe_frame(
        511,
        2,
        encode_subscribe_meta(7, "chain-watcher", &[("chain/order", 0)]),
    ))
    .await
    .expect("subscribe");
    let suback = sub.recv().await.expect("recv suback");
    assert_eq!(suback.header.opcode, OpCode::SubAckOut);

    let publ = bind_client(addr, 512, "chain-publisher", true).await;
    let (meta, payload) = encode_publish_meta("chain/order", 0, 0, false, b"m");
    publ.send(publish_frame(512, 2, meta, payload))
        .await
        .expect("publish");
    let first = sub.recv().await.expect("recv transformed");
    assert_eq!(first.header.opcode, OpCode::PublishOut);
    assert_eq!(
        first.payload,
        Bytes::from_static(b"m[t-a][t-b]"),
        "chain order defines execution order"
    );

    // Replace with the reordered chain (the same `replace_all` plus
    // `sync_transforms` pair the PUT handler runs): the new order
    // takes effect on the next publish.
    driver.transformations.replace_all(vec![
        record("t-b", "${payload}[t-b]"),
        record("t-a", "${payload}[t-a]"),
    ]);
    driver
        .engine
        .sync_transforms(driver.transformations.steps_snapshot());
    let (meta, payload) = encode_publish_meta("chain/order", 0, 0, false, b"m");
    publ.send(publish_frame(512, 3, meta, payload))
        .await
        .expect("publish after reorder");
    let second = sub.recv().await.expect("recv reordered");
    assert_eq!(second.header.opcode, OpCode::PublishOut);
    assert_eq!(
        second.payload,
        Bytes::from_static(b"m[t-b][t-a]"),
        "reordered chain runs in the new order"
    );

    server.abort();
}

#[tokio::test]
async fn transform_runs_before_retained_and_rules() {
    use broker_api::v5::transformations::{TransformOperation, TransformRecord};
    use broker_rules::RuleAction;

    let shared = test_shared();
    shared
        .transformations
        .insert(TransformRecord {
            name: "t-both".to_string(),
            topics: vec!["chain/order".to_string()],
            failure_action: "drop".to_string(),
            enable: true,
            operations: vec![TransformOperation {
                key: "payload".to_string(),
                value: "${payload}[t]".to_string(),
            }],
            params: serde_json::json!({}),
        })
        .expect("transform inserts");
    shared
        .engine
        .sync_transforms(shared.transformations.steps_snapshot());
    shared
        .engine
        .create_rule(
            "transform-rule".to_string(),
            TopicFilter::new("chain/order").unwrap(),
            None,
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("chain/alerts").unwrap(),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("rule creates");

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

    let sub = bind_client(addr, 521, "chain-watcher", true).await;
    sub.send(subscribe_frame(
        521,
        2,
        encode_subscribe_meta(7, "chain-watcher", &[("chain/order", 0)]),
    ))
    .await
    .expect("subscribe");
    assert_eq!(
        sub.recv().await.expect("suback").header.opcode,
        OpCode::SubAckOut
    );
    let alerts = bind_client(addr, 522, "alert-watcher", true).await;
    alerts
        .send(subscribe_frame(
            522,
            2,
            encode_subscribe_meta(7, "alert-watcher", &[("chain/alerts", 0)]),
        ))
        .await
        .expect("subscribe alerts");
    assert_eq!(
        alerts.recv().await.expect("suback").header.opcode,
        OpCode::SubAckOut
    );

    let publ = bind_client(addr, 523, "chain-publisher", true).await;
    let (meta, payload) = encode_publish_meta("chain/order", 0, 0, false, b"m");
    publ.send(publish_frame(523, 2, meta, payload))
        .await
        .expect("publish");
    let live = sub.recv().await.expect("recv transformed");
    assert_eq!(live.payload, Bytes::from_static(b"m[t]"));
    let fired = alerts.recv().await.expect("recv rule republish");
    assert_eq!(fired.header.opcode, OpCode::PublishOut);
    assert_eq!(
        fired.payload,
        Bytes::from_static(b"m[t]"),
        "rules observe the transformed payload"
    );

    let (meta, payload) = encode_publish_meta("chain/order", 0, 0, true, b"r");
    publ.send(publish_frame(523, 3, meta, payload))
        .await
        .expect("publish retained");
    let retained_live = sub.recv().await.expect("recv retained live");
    assert_eq!(retained_live.payload, Bytes::from_static(b"r[t]"));
    let _ = alerts.recv().await.expect("recv rule republish");
    let late = bind_client(addr, 524, "late-watcher", true).await;
    late.send(subscribe_frame(
        524,
        2,
        encode_subscribe_meta(8, "late-watcher", &[("chain/order", 0)]),
    ))
    .await
    .expect("subscribe late");
    assert_eq!(
        late.recv().await.expect("suback").header.opcode,
        OpCode::SubAckOut
    );
    let held = late.recv().await.expect("recv retained");
    assert_eq!(held.payload, Bytes::from_static(b"r[t]"));
    let (topic, _, _, retain) = decode_publish_meta_parts(&held.metadata);
    assert_eq!(topic, "chain/order");
    assert!(retain, "retained replay carries retain");

    server.abort();
}

#[tokio::test]
async fn transform_metrics_move_on_real_publish_and_reset() {
    use broker_api::v5::transformations::{TransformOperation, TransformRecord};

    let shared = test_shared();
    shared
        .transformations
        .insert(TransformRecord {
            name: "w3-36-live".to_string(),
            topics: vec!["metrics/live/#".to_string()],
            failure_action: "drop".to_string(),
            enable: true,
            operations: vec![TransformOperation {
                key: "payload".to_string(),
                value: "${payload}[m]".to_string(),
            }],
            params: serde_json::json!({}),
        })
        .expect("transform inserts");
    // Same wiring the management create handler drives: metric
    // record plus engine snapshot.
    shared.engine.ensure_transform_metrics("w3-36-live");
    shared
        .engine
        .sync_transforms(shared.transformations.steps_snapshot());

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let server_shared = shared.clone();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (stream, _) = listener.accept().await.expect("accept");
            let shared = server_shared.clone();
            tokio::spawn(async move {
                handle_connection(stream, shared).await.expect("handle");
            });
        }
        std::future::pending::<()>().await;
    });

    let sub = bind_client(addr, 531, "metrics-watcher", true).await;
    sub.send(subscribe_frame(
        531,
        2,
        encode_subscribe_meta(7, "metrics-watcher", &[("metrics/live", 0)]),
    ))
    .await
    .expect("subscribe");
    assert_eq!(
        sub.recv().await.expect("suback").header.opcode,
        OpCode::SubAckOut
    );
    let publ = bind_client(addr, 532, "metrics-publisher", true).await;
    let (meta, payload) = encode_publish_meta("metrics/live", 0, 0, false, b"m");
    publ.send(publish_frame(532, 2, meta, payload))
        .await
        .expect("publish");
    let live = sub.recv().await.expect("recv transformed");
    assert_eq!(live.header.opcode, OpCode::PublishOut);
    assert_eq!(live.payload, Bytes::from_static(b"m[m]"));
    // The broker event (publish through the ingress pipeline)
    // attributes the run on the engine record.
    let (matched, succeeded, failed) = shared
        .engine
        .transform_metrics_snapshot_for("w3-36-live")
        .expect("known name has a record");
    assert_eq!(matched, 1);
    assert_eq!(succeeded, 1);
    assert_eq!(failed, 0);
    // Reset zeroes the counters; unknown names hold no record.
    assert!(shared.engine.reset_transform_metrics("w3-36-live"));
    let (matched, succeeded, failed) = shared
        .engine
        .transform_metrics_snapshot_for("w3-36-live")
        .expect("record stays after reset");
    assert_eq!((matched, succeeded, failed), (0, 0, 0));
    assert!(!shared.engine.reset_transform_metrics("no-such-transform"));
    assert!(shared
        .engine
        .transform_metrics_snapshot_for("no-such-transform")
        .is_none());

    server.abort();
}
