use super::*;
use crate::link_harness::handle_connection;
use tokio::net::TcpListener;

#[tokio::test]
async fn unbind_detaches_connection_but_keeps_subscriptions() {
    let shared = test_shared();
    // Durable session (clean_start=false): the detach keeps subscriptions.
    // (Clean sessions are covered by
    // `clean_session_disconnect_drops_subscriptions_no_ghost`, which
    // asserts the router copy is removed.)
    let bind = bind_frame(71, 1, encode_bind_meta("gone-1", false, 60));
    reply_for_frame(&bind, &shared.sessions).expect("bind replies");
    let sub = subscribe_frame(71, 2, encode_subscribe_meta(1, "gone-1", &[("t", 0)]));
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");

    let mut unbind_meta = vec![0x00u8, 0x06u8];
    unbind_meta.extend_from_slice(b"gone-1");
    let unbind = BrokerFrame::new(
        OpCode::UnbindConnection,
        71,
        3,
        Bytes::from(unbind_meta),
        Bytes::new(),
    )
    .expect("valid unbind frame");
    apply_unbind(&unbind, &shared);

    let session = shared
        .sessions
        .get("gone-1")
        .expect("session survives unbind");
    assert!(!*session.connected.read());
    assert_eq!(*session.conn_id.read(), None);
    // Durable subscriptions survive the detach.
    assert_eq!(shared.router.matches(&Topic::new("t").unwrap()).len(), 1);
}

/// F1-01: the last will fires on an ungraceful close, never after a
/// clean DISCONNECT, exactly once, honouring its QoS and retain bit.
/// Fails before the fix with no delivery at all (the will was never
/// captured and the close never reported). Drives connect, subscribe,
/// disconnect and delivery through the broker, never the store alone.
#[tokio::test]
async fn last_will_fires_on_disconnect_not_on_unbind() {
    let shared = test_shared();
    // Subscriber binds durably and subscribes QoS 1 to the will topic.
    let sub_bind = bind_frame(301, 1, encode_bind_meta("will-sub", false, 60));
    apply_bind(&sub_bind, &shared)
        .await
        .expect("sub bind replies");
    let sub = subscribe_frame(
        301,
        2,
        encode_subscribe_meta(11, "will-sub", &[("will/test", 1)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");

    // Publisher connects with a QoS 1 retained will.
    let pub_bind = bind_frame(
        302,
        1,
        encode_bind_meta_with_will("will-pub", true, 5, "will/test", b"client-gone", 1, true),
    );
    apply_bind(&pub_bind, &shared)
        .await
        .expect("pub bind replies");
    // The CONNECT event captured the will on the session.
    assert!(
        shared
            .sessions
            .get("will-pub")
            .expect("session row")
            .last_will
            .read()
            .is_some(),
        "bind must capture the will"
    );

    // Ungraceful close: the will fires once with topic, payload, QoS
    // and retain bit intact, routed to the live subscriber.
    let deliveries = apply_disconnect(&disconnect_frame(302, 2, "will-pub"), &shared).await;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].0, 301);
    assert_eq!(deliveries[0].1.payload, Bytes::from_static(b"client-gone"));
    let (topic, _, qos, retain) = decode_publish_meta_parts(&deliveries[0].1.metadata);
    assert_eq!(topic, "will/test");
    assert_eq!(qos, 1);
    assert!(retain, "will retain bit must survive");
    assert!(
        !*shared
            .sessions
            .get("will-pub")
            .expect("session row")
            .connected
            .read(),
        "disconnect detaches the session"
    );

    // A second notice for the same dead connection publishes nothing.
    let again = apply_disconnect(&disconnect_frame(302, 3, "will-pub"), &shared).await;
    assert!(again.is_empty(), "will must publish exactly once");

    // Reconnect with a will, then close cleanly: nothing fires, and a
    // later stale notice still fires nothing (suppressed, then gone).
    let rebind = bind_frame(
        303,
        1,
        encode_bind_meta_with_will("will-pub", true, 5, "will/test", b"second", 0, false),
    );
    apply_bind(&rebind, &shared).await.expect("rebind replies");
    apply_unbind(&unbind_frame(303, 2, "will-pub"), &shared);
    let after_clean = apply_disconnect(&disconnect_frame(303, 3, "will-pub"), &shared).await;
    assert!(
        after_clean.is_empty(),
        "clean DISCONNECT must suppress the will"
    );

    // The retained will from the ungraceful fire is stored for later
    // subscribers, like any retained publish.
    assert!(
        shared
            .retained
            .get_retained(&Topic::new("will/test").unwrap())
            .await
            .expect("retained read")
            .is_some(),
        "retained will must be stored"
    );
}

/// TK-04: a clean session leaves no subscriptions behind, so a later
/// reconnect with the same client id receives nothing until it
/// resubscribes. Fails before the fix with one ghost delivery.
#[tokio::test]
async fn clean_session_disconnect_drops_subscriptions_no_ghost() {
    let shared = test_shared();
    let bind = bind_frame(81, 1, encode_bind_meta("tk04-clean", true, 60));
    apply_bind(&bind, &shared).await.expect("bind replies");
    let sub = subscribe_frame(
        81,
        2,
        encode_subscribe_meta(1, "tk04-clean", &[("tk04/ghost", 0)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");
    assert_eq!(
        shared
            .router
            .matches(&Topic::new("tk04/ghost").unwrap())
            .len(),
        1
    );

    // Clean disconnect sweeps both the session mirror and the router.
    apply_unbind(&unbind_frame(81, 3, "tk04-clean"), &shared);
    let session = shared.sessions.get("tk04-clean").expect("session row");
    assert!(!*session.connected.read());
    assert_eq!(*session.conn_id.read(), None);
    assert!(session.subscriptions.read().is_empty());
    assert!(
        shared
            .router
            .matches(&Topic::new("tk04/ghost").unwrap())
            .is_empty(),
        "clean disconnect must remove the router copy"
    );

    // Reconnect clean with the same client id: fresh session, no subs.
    let rebind = bind_frame(82, 1, encode_bind_meta("tk04-clean", true, 60));
    apply_bind(&rebind, &shared).await.expect("rebind replies");
    assert!(shared
        .router
        .matches(&Topic::new("tk04/ghost").unwrap())
        .is_empty());

    // A publish to the old topic reaches nobody.
    let (meta, payload) = encode_publish_meta("tk04/ghost", 0, 0, false, b"stale");
    let (_, deliveries) = apply_publish(&publish_frame(99, 4, meta, payload), &shared).await;
    assert!(
        deliveries.is_empty(),
        "ghost delivery: clean reconnect must receive nothing until resubscribe"
    );
}

/// TK-04 companion: a persistent session keeps its subscription across
/// the same disconnect/reconnect sequence (what durable redelivery
/// builds on).
#[tokio::test]
async fn durable_session_disconnect_keeps_subscriptions() {
    let shared = test_shared();
    let bind = bind_frame(91, 1, encode_bind_meta("tk04-durable", false, 60));
    apply_bind(&bind, &shared).await.expect("bind replies");
    let sub = subscribe_frame(
        91,
        2,
        encode_subscribe_meta(1, "tk04-durable", &[("tk04/kept", 0)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");

    apply_unbind(&unbind_frame(91, 3, "tk04-durable"), &shared);
    // Detached durable session: router copy and session mirror survive.
    assert_eq!(
        shared
            .router
            .matches(&Topic::new("tk04/kept").unwrap())
            .len(),
        1
    );
    assert_eq!(
        shared
            .sessions
            .get("tk04-durable")
            .expect("session row")
            .subscriptions
            .read()
            .len(),
        1
    );

    // Reconnect durably on a new connection: the subscription routes.
    let rebind = bind_frame(92, 1, encode_bind_meta("tk04-durable", false, 60));
    apply_bind(&rebind, &shared).await.expect("rebind replies");
    bind_test_publisher(&shared, 99);
    let (meta, payload) = encode_publish_meta("tk04/kept", 0, 0, false, b"live");
    let (_, deliveries) = apply_publish(&publish_frame(99, 4, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].0, 92);
}

#[tokio::test]
async fn subscribe_then_publish_routes_to_subscriber_transport() {
    let shared = test_shared();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");

    // Accept exactly two edge connections sharing one kernel.
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (stream, _) = listener.accept().await.expect("accept");
            let shared = shared.clone();
            tokio::spawn(async move {
                handle_connection(stream, shared).await.expect("handle");
            });
        }
        // Park until the test aborts us; handlers are independent tasks.
        std::future::pending::<()>().await;
    });

    // Subscriber: bind as conn 101, subscribe with a wildcard.
    let sub_io = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect sub");
    let sub = FramedTransport::new(sub_io);
    sub.send(bind_frame(101, 1, encode_bind_meta("route-me", true, 60)))
        .await
        .expect("bind");
    let binding = sub.recv().await.expect("recv binding");
    assert_eq!(binding.header.opcode, OpCode::SessionBinding);
    sub.send(subscribe_frame(
        101,
        2,
        encode_subscribe_meta(11, "route-me", &[("sport/+", 1)]),
    ))
    .await
    .expect("subscribe");
    let suback = sub.recv().await.expect("recv suback");
    assert_eq!(suback.header.opcode, OpCode::SubAckOut);
    assert_eq!(&suback.metadata[0..2], &11u16.to_be_bytes());
    assert_eq!(&suback.metadata[2..], &[1u8]);

    // Publisher: bind as conn 102, publish QoS 1.
    let pub_io = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect pub");
    let publ = FramedTransport::new(pub_io);
    publ.send(bind_frame(102, 1, encode_bind_meta("writer", true, 60)))
        .await
        .expect("bind");
    let _ = publ.recv().await.expect("recv binding");
    let (meta, payload) = encode_publish_meta("sport/tennis", 77, 1, false, b"match-point");
    publ.send(publish_frame(102, 2, meta, payload))
        .await
        .expect("publish");

    // Publisher gets its PubAck on its own transport.
    let ack = publ.recv().await.expect("recv puback");
    assert_eq!(ack.header.opcode, OpCode::PubAckOut);
    assert_eq!(ack.header.conn_id, 102);
    assert_eq!(&ack.metadata[..], &[0x00, 0x4D, 0x00]);

    // Subscriber gets the routed PublishOut on its own transport.
    let routed = sub.recv().await.expect("recv publish");
    assert_eq!(routed.header.opcode, OpCode::PublishOut);
    assert_eq!(routed.header.conn_id, 101);
    assert_eq!(routed.payload, Bytes::from_static(b"match-point"));
    let (topic, _, qos, _) = decode_publish_meta_parts(&routed.metadata);
    assert_eq!(topic, "sport/tennis");
    assert_eq!(qos, 1);

    server.abort();
}

/// W1-39: a fresh bind receives the configured auto-subscriptions.
/// Fails before the change (empty list, no hook) and passes after:
/// write one entry via the management write, bind a new client, and
/// expect the subscription present in both the session mirror and
/// the router.
#[tokio::test]
async fn auto_subscribe_hook_subscribes_new_connection() {
    use axum::extract::State;
    let shared = test_shared();
    let engine = std::sync::Arc::new(broker_rules::RuleEngine::new(
        16,
        broker_rules::BackpressurePolicy::DropOldest,
    ));
    let mut api_state = broker_api::ApiState::standalone(engine);
    api_state.auto_subscribe = shared.auto_subscribe.clone();
    // Write one entry and read it back.
    let body = bytes::Bytes::from(
        serde_json::to_vec(&serde_json::json!([
            {"topic": "conf/auto/w139-hook", "qos": 1},
        ]))
        .expect("config is JSON"),
    );
    let resp =
        broker_api::v5::auto_subscribe::put_auto_subscribe(State(api_state.clone()), body).await;
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    assert_eq!(shared.auto_subscribe.list().len(), 1);

    // Fresh client binds successfully.
    let bind = bind_frame(501, 1, encode_bind_meta("w139-fresh", true, 60));
    let reply = apply_bind(&bind, &shared).await.expect("bind replies");
    assert!(is_binding_accepted(&reply));
    apply_auto_subscribe(&shared, "w139-fresh", None, 501).await;

    // Session mirror and router both hold the new subscription.
    let session = shared.sessions.get("w139-fresh").expect("session row");
    assert!(
        session
            .subscriptions
            .read()
            .keys()
            .any(|f| f.as_str() == "conf/auto/w139-hook"),
        "fresh client must carry the auto subscription"
    );
    assert_eq!(
        shared
            .router
            .matches(&Topic::new("conf/auto/w139-hook").unwrap())
            .len(),
        1
    );

    // Placeholder entries render per client.
    let body = bytes::Bytes::from(
        serde_json::to_vec(&serde_json::json!([
            {"topic": "cmd/${clientid}/z", "qos": 0},
        ]))
        .expect("config is JSON"),
    );
    let resp =
        broker_api::v5::auto_subscribe::put_auto_subscribe(State(api_state.clone()), body).await;
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let bind = bind_frame(502, 1, encode_bind_meta("w139-ph", true, 60));
    apply_bind(&bind, &shared).await.expect("bind replies");
    apply_auto_subscribe(&shared, "w139-ph", None, 502).await;
    assert!(
        shared
            .sessions
            .get("w139-ph")
            .expect("session row")
            .subscriptions
            .read()
            .keys()
            .any(|f| f.as_str() == "cmd/w139-ph/z"),
        "placeholder must render per client"
    );
}

/// B4-08 bound (ruling B4-08b.3): a full subscription table denies
/// the auto subscription fail closed, exactly as `apply_subscribe`
/// does: no session mirror, nothing counted as applied, router
/// silent. Fills to MAX_SUBSCRIPTIONS with three-level filters so
/// each copy-on-write path copy stays small.
#[tokio::test]
async fn auto_subscribe_quota_denied_changes_no_session_state() {
    use axum::extract::State;
    let shared = test_shared();
    for i in 0..broker_router::MAX_SUBSCRIPTIONS {
        let a = i / 10_000;
        let b = (i / 100) % 100;
        let c = i % 100;
        let filter =
            TopicFilter::new(format!("quota/fill/{a}/{b:02}/{c:02}")).expect("valid fill filter");
        assert!(
            shared.router.subscribe(
                &filter,
                Subscription::new(format!("fill-{i}"), i as u64, QoS::AtMostOnce)
            ),
            "fill subscribe {i} must succeed"
        );
    }
    let engine = std::sync::Arc::new(broker_rules::RuleEngine::new(
        16,
        broker_rules::BackpressurePolicy::DropOldest,
    ));
    let mut api_state = broker_api::ApiState::standalone(engine);
    api_state.auto_subscribe = shared.auto_subscribe.clone();
    let body = bytes::Bytes::from(
        serde_json::to_vec(&serde_json::json!([
            {"topic": "conf/auto/quota-denied", "qos": 1},
        ]))
        .expect("config is JSON"),
    );
    let resp =
        broker_api::v5::auto_subscribe::put_auto_subscribe(State(api_state.clone()), body).await;
    assert_eq!(resp.status(), axum::http::StatusCode::OK);

    let (session, _) = shared.sessions.get_or_create("quota-auto", true);
    *session.conn_id.write() = Some(9091);
    apply_auto_subscribe(&shared, "quota-auto", None, 9091).await;

    assert!(
        session
            .subscriptions
            .read()
            .keys()
            .all(|f| f.as_str() != "conf/auto/quota-denied"),
        "quota-denied auto subscribe must leave the session unchanged"
    );
    assert!(
        shared
            .router
            .matches(&Topic::new("conf/auto/quota-denied").unwrap())
            .is_empty(),
        "quota-denied auto subscribe must leave the router unchanged"
    );
}
