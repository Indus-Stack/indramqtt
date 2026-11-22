use super::*;
use crate::link_harness::handle_connection;
use tokio::net::TcpListener;

#[tokio::test]
async fn retained_message_delivered_to_late_subscriber() {
    let mut shared = Shared::new();
    shared.allow_anonymous = true;
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

    // Client A publishes retained state before anyone subscribes.
    let publ = bind_client(addr, 501, "sensor-a", true).await;
    let (meta, payload) =
        encode_publish_meta("device/state", 0, 0, true, b"{ \"status\": \"online\" }");
    publ.send(publish_frame(501, 2, meta, payload))
        .await
        .expect("publish retained");

    // Client B connects later and subscribes with a wildcard.
    let sub = bind_client(addr, 502, "watcher-b", true).await;
    sub.send(subscribe_frame(
        502,
        2,
        encode_subscribe_meta(7, "watcher-b", &[("device/+", 0)]),
    ))
    .await
    .expect("subscribe");
    let suback = sub.recv().await.expect("recv suback");
    assert_eq!(suback.header.opcode, OpCode::SubAckOut);

    // The retained message follows the SUBACK immediately.
    let held = sub.recv().await.expect("recv retained");
    assert_eq!(held.header.opcode, OpCode::PublishOut);
    assert_eq!(held.header.conn_id, 502);
    assert_eq!(
        held.payload,
        Bytes::from_static(b"{ \"status\": \"online\" }")
    );
    let (topic, _, qos, retain) = decode_publish_meta_parts(&held.metadata);
    assert_eq!(topic, "device/state");
    assert_eq!(qos, 0);
    assert!(retain, "replayed retained message must carry retain = true");

    // Clearing with an empty retained publish removes the state.
    let (meta, payload) = encode_publish_meta("device/state", 0, 0, true, b"");
    publ.send(publish_frame(501, 3, meta, payload))
        .await
        .expect("clear retained");

    // A later subscriber gets its SUBACK but no retained message.
    let late = bind_client(addr, 503, "watcher-c", true).await;
    late.send(subscribe_frame(
        503,
        2,
        encode_subscribe_meta(8, "watcher-c", &[("device/+", 0)]),
    ))
    .await
    .expect("subscribe");
    let suback = late.recv().await.expect("recv suback");
    assert_eq!(suback.header.opcode, OpCode::SubAckOut);
    let nothing = tokio::time::timeout(std::time::Duration::from_millis(400), late.recv()).await;
    assert!(
        nothing.is_err(),
        "cleared retained state must not be delivered"
    );

    server.abort();
}

#[tokio::test]
async fn kernel_ingress_enforces_retainer_limits_like_mgmt() {
    // W1-28 parity: kernel ingress and management publishes share
    // the same retainer_config object, so configurable limits apply
    // on both paths. Oversized payloads deliver but are not stored;
    // new topics past the cap are dropped while replacements land.
    use axum::extract::State;
    let shared = Shared::new();
    let engine = std::sync::Arc::new(broker_rules::RuleEngine::new(
        16,
        broker_rules::BackpressurePolicy::DropOldest,
    ));
    let mut api_state = broker_api::ApiState::standalone(engine);
    api_state.retainer_config = shared.retainer_config.clone();
    api_state.retained = shared.retained.clone();
    api_state.stats = shared.stats.clone();
    let body = bytes::Bytes::from(
        serde_json::to_vec(&serde_json::json!({
            "max_payload_size": "10B",
            "backend": {"max_retained_messages": 1},
        }))
        .expect("config is JSON"),
    );
    let resp = broker_api::v5::retainer::put_retainer_config(State(api_state.clone()), body).await;
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    // Oversized retained publish is dropped from the store.
    let big_topic = broker_protocol::Topic::new("w128/limit/big").expect("topic");
    let big_payload = bytes::Bytes::from(vec![b'x'; 100]);
    ingress_pipeline(
        &shared,
        &big_topic,
        broker_protocol::QoS::AtMostOnce,
        true,
        &big_payload,
        broker_session::tenant::DEFAULT_TENANT_ID,
    )
    .await;
    let stored = shared
        .retained
        .get_retained(&big_topic)
        .await
        .expect("store reads");
    assert!(stored.is_none(), "oversized payload must not be stored");
    // Reset to a generous size, then exercise the topic cap.
    let body = bytes::Bytes::from(
        serde_json::to_vec(&serde_json::json!({"max_payload_size": "1MB"}))
            .expect("config is JSON"),
    );
    let resp = broker_api::v5::retainer::put_retainer_config(State(api_state.clone()), body).await;
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let first = broker_protocol::Topic::new("w128/limit/first").expect("topic");
    ingress_pipeline(
        &shared,
        &first,
        broker_protocol::QoS::AtMostOnce,
        true,
        &bytes::Bytes::from_static(b"one"),
        broker_session::tenant::DEFAULT_TENANT_ID,
    )
    .await;
    assert!(shared
        .retained
        .get_retained(&first)
        .await
        .expect("read")
        .is_some());
    let second = broker_protocol::Topic::new("w128/limit/second").expect("topic");
    ingress_pipeline(
        &shared,
        &second,
        broker_protocol::QoS::AtMostOnce,
        true,
        &bytes::Bytes::from_static(b"two"),
        broker_session::tenant::DEFAULT_TENANT_ID,
    )
    .await;
    assert!(
        shared
            .retained
            .get_retained(&second)
            .await
            .expect("read")
            .is_none(),
        "new topic past the cap must be dropped"
    );
    // Replacements for an existing topic still land.
    ingress_pipeline(
        &shared,
        &first,
        broker_protocol::QoS::AtMostOnce,
        true,
        &bytes::Bytes::from_static(b"one-v2"),
        broker_session::tenant::DEFAULT_TENANT_ID,
    )
    .await;
    let replaced = shared
        .retained
        .get_retained(&first)
        .await
        .expect("read")
        .expect("exists");
    assert_eq!(replaced.payload, bytes::Bytes::from_static(b"one-v2"));
}
