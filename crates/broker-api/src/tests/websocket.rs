use super::*;

#[tokio::test]
async fn test_ws_mqtt_console_flow() {
    let (server, _state) = TestServer::start().await;

    // Subscriber console: CONNECT -> CONNACK, SUBSCRIBE -> SUBACK.
    let mut sub = WsClient::connect(server.port).await;
    sub.send_bin(&mqtt_connect("console-a", None, None)).await;
    let connack = sub.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0, "CONNACK return code must be 0");
    sub.send_bin(&mqtt_subscribe(7, &[("demo/#", 1)])).await;
    let suback = sub.recv_msg().await.expect("suback");
    assert_eq!(mqtt_packet_type(&suback), 9);
    assert_eq!(&suback[4..], &[1], "granted QoS 1");

    // Publisher console: QoS 1 publish lands on the subscriber.
    let mut publ = WsClient::connect(server.port).await;
    publ.send_bin(&mqtt_connect("console-b", None, None)).await;
    let connack = publ.recv_msg().await.expect("connack");
    assert_eq!(connack[3], 0);
    publ.send_bin(&mqtt_publish("demo/1", 42, 1, b"hi-ws"))
        .await;
    // Publisher gets its PUBACK...
    let puback = publ.recv_msg().await.expect("puback");
    assert_eq!(mqtt_packet_type(&puback), 4);
    // ...and the subscriber gets the delivery with flags + payload.
    let delivery = sub.recv_msg().await.expect("delivery");
    assert_eq!(mqtt_packet_type(&delivery), 3);
    assert_eq!(delivery[0] & 0x06, 0x02, "downstream QoS 1");
    assert!(delivery.windows(b"demo/1".len()).any(|w| w == b"demo/1"));
    assert!(delivery.ends_with(b"hi-ws"));

    // PINGREQ -> PINGRESP on the same socket.
    publ.send_bin(&[0xC0, 0x00]).await;
    let pong = publ.recv_msg().await.expect("pingresp");
    assert_eq!(pong, vec![0xD0, 0x00]);
}

#[tokio::test]
async fn test_ws_auth_rejected_with_0x04() {
    let (server, state) = TestServer::start().await;
    state
        .auth
        .add_user("alice", b"s3cret")
        .expect("test user persists");

    let mut client = WsClient::connect(server.port).await;
    client
        .send_bin(&mqtt_connect("console-x", Some("alice"), Some(b"wrong")))
        .await;
    let connack = client.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0x04, "bad password must yield 0x04");
    // Socket closes right after the failed CONNACK.
    assert!(client.recv_msg().await.is_none());
    assert!(state.sessions.get("console-x").is_none());
}

#[tokio::test]
async fn test_ws_anonymous_rejected_when_users_exist() {
    let (server, state) = TestServer::start().await;
    state
        .auth
        .add_user("alice", b"s3cret")
        .expect("test user persists");

    // No username while users exist: rejected with 0x05 even though
    // the console path has no `--allow-anonymous` flag.
    let mut client = WsClient::connect(server.port).await;
    client
        .send_bin(&mqtt_connect("console-anon", None, None))
        .await;
    let connack = client.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0x05, "anonymous must yield 0x05");
    // Socket closes right after the failed CONNACK.
    assert!(client.recv_msg().await.is_none());
    assert!(state.sessions.get("console-anon").is_none());

    // A valid credentialed console CONNECT in the same store gets 0.
    let mut client = WsClient::connect(server.port).await;
    client
        .send_bin(&mqtt_connect("console-ok", Some("alice"), Some(b"s3cret")))
        .await;
    let connack = client.recv_msg().await.expect("connack");
    assert_eq!(connack[3], 0);
}

#[tokio::test]
async fn test_ws_subscribe_denied_with_0x80() {
    let (server, state) = TestServer::start().await;
    state
        .auth
        .add_rule(broker_auth::AclRule::new(
            "console-y",
            broker_auth::AclAction::All,
            "#",
            false,
        ))
        .expect("test rule persists");

    let mut client = WsClient::connect(server.port).await;
    client
        .send_bin(&mqtt_connect("console-y", None, None))
        .await;
    let connack = client.recv_msg().await.expect("connack");
    assert_eq!(connack[3], 0);

    client.send_bin(&mqtt_subscribe(3, &[("t", 0)])).await;
    let suback = client.recv_msg().await.expect("suback");
    assert_eq!(mqtt_packet_type(&suback), 9);
    assert_eq!(&suback[4..], &[0x80], "denied filter must yield 0x80");
}

#[tokio::test]
async fn test_ws_subscribe_quota_denied_with_0x80() {
    // B4-08 bound: a full subscription table denies the console
    // subscribe fail closed with 0x80 Failure and changes no
    // session state. Fills the shared router to MAX_SUBSCRIPTIONS
    // with three-level filters so each path copy stays small.
    let (server, state) = TestServer::start().await;
    for i in 0..broker_router::MAX_SUBSCRIPTIONS {
        let a = i / 10_000;
        let b = (i / 100) % 100;
        let c = i % 100;
        let filter = broker_protocol::TopicFilter::new(format!("quota/fill/{a}/{b:02}/{c:02}"))
            .expect("valid fill filter");
        assert!(
            state.router.subscribe(
                &filter,
                broker_router::Subscription::new(
                    format!("fill-{i}"),
                    i as u64,
                    broker_protocol::QoS::AtMostOnce
                )
            ),
            "fill subscribe {i} must succeed"
        );
    }

    let mut client = WsClient::connect(server.port).await;
    client
        .send_bin(&mqtt_connect("console-q", None, None))
        .await;
    let connack = client.recv_msg().await.expect("connack");
    assert_eq!(connack[3], 0);

    client
        .send_bin(&mqtt_subscribe(9, &[("quota/probe", 0)]))
        .await;
    let suback = client.recv_msg().await.expect("suback");
    assert_eq!(mqtt_packet_type(&suback), 9);
    assert_eq!(
        &suback[4..],
        &[0x80],
        "quota denial must yield 0x80, not success"
    );

    // The failed subscribe registers nothing: no session mirror, no
    // router entry.
    let session = state.sessions.get("console-q").expect("session row");
    assert!(
        session
            .subscriptions
            .read()
            .keys()
            .all(|f| f.as_str() != "quota/probe"),
        "quota-denied subscribe must leave the session unchanged"
    );
    assert!(
        state
            .router
            .matches(&broker_protocol::Topic::new("quota/probe").expect("valid probe topic"))
            .is_empty(),
        "quota-denied subscribe must leave the router unchanged"
    );
}
