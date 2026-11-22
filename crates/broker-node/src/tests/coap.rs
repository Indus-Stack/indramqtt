use super::*;

/// B1-04: a CoAP publish through the gateway reaches an ordinary MQTT
/// subscriber through the same router path an MQTT publish takes.
///
/// Drives the real gateway entry point (`apply_coap_publish`:
/// retained store plus `ingress_pipeline` plus cluster forward) and
/// asserts the router delivers to a session bound via the normal
/// `apply_subscribe` path. Fails before the wiring (no gateway
/// ingress reaches the router); passes after it.
#[tokio::test]
async fn coap_publish_reaches_mqtt_subscriber() {
    let shared = test_shared();
    // Ordinary MQTT subscriber on the gateway topic.
    let sub = subscribe_frame(
        71,
        1,
        encode_subscribe_meta(1, "mqtt-sub-1", &[("coap/sensors/temp", 0)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");
    let (session, _) = shared.sessions.get_or_create("mqtt-sub-1", true);
    *session.conn_id.write() = Some(71);
    *session.connected.write() = true;

    // CoAP gateway ingress: POST /ps/coap/sensors/temp.
    let payload = Bytes::from_static(b"21.5");
    let deliveries = apply_coap_publish(&shared, "coap/sensors/temp", &payload)
        .await
        .expect("valid CoAP topic routes");
    assert_eq!(deliveries.len(), 1, "one MQTT subscriber must match");
    assert_eq!(deliveries[0].0, 71);
    assert_eq!(deliveries[0].1.payload, payload);
    let (topic, _, _, _) = decode_publish_meta_parts(&deliveries[0].1.metadata);
    assert_eq!(topic, "coap/sensors/temp");

    // Retained so a later CoAP GET reads the same value.
    let stored = shared
        .retained
        .get_retained(&Topic::new("coap/sensors/temp").unwrap())
        .await
        .expect("retained read")
        .expect("CoAP publish is retained");
    assert_eq!(stored.payload, payload);

    // Malformed CoAP topics route nothing.
    assert!(apply_coap_publish(&shared, "coap/#/bad", &payload)
        .await
        .is_none());
    assert!(apply_coap_publish(&shared, "", &payload).await.is_none());
}

/// SEC-01: the CoAP gateway checks access rules before it routes.
///
/// Sends POST datagrams through a UDP socket to the gateway task.
/// A denied topic answers 4.01 and reaches no subscriber.
/// An allowed topic answers 2.04 and reaches the subscriber.
#[tokio::test]
async fn coap_publish_denied_topic_is_refused_over_udp() {
    use broker_gateway::coap::{option_number, CoapOption, CoapType};
    let shared = test_shared();
    shared
        .auth
        .add_rule(AclRule::new("*", AclAction::Publish, "secret/#", false))
        .expect("rule adds");
    shared
        .auth
        .add_rule(AclRule::new("*", AclAction::All, "#", true))
        .expect("rule adds");

    // One MQTT subscriber for both probe topics.
    let sub = subscribe_frame(
        71,
        1,
        encode_subscribe_meta(1, "coap-acl-sub", &[("secret/x", 0), ("open/x", 0)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");
    let (session, _) = shared.sessions.get_or_create("coap-acl-sub", true);
    *session.conn_id.write() = Some(71);
    *session.connected.write() = true;
    let (tx, mut rx) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(71, tx);

    // Start the gateway on loopback. The task runs until the test ends.
    let gateway = Arc::new(
        tokio::net::UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("gateway binds"),
    );
    let gateway_addr = gateway.local_addr().expect("gateway addr");
    let gateway_shared = shared.clone();
    let gateway_socket = gateway.clone();
    tokio::spawn(async move {
        run_coap_gateway(gateway_shared, gateway_socket).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let client = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("client binds");

    // Build one POST for `/ps/<topic>`.
    let coap_post = |mid: u16, token: &[u8], topic: &str, payload: &[u8]| {
        let mut options = vec![CoapOption {
            number: option_number::URI_PATH,
            value: Bytes::from_static(b"ps"),
        }];
        for part in topic.split('/') {
            options.push(CoapOption {
                number: option_number::URI_PATH,
                value: Bytes::copy_from_slice(part.as_bytes()),
            });
        }
        broker_gateway::coap::CoapMessage {
            message_type: CoapType::Confirmable,
            code: CoapCode::POST,
            message_id: mid,
            token: Bytes::copy_from_slice(token),
            options,
            payload: Bytes::copy_from_slice(payload),
        }
    };

    // Denied topic: the gateway refuses and routes nothing.
    let denied_msg = coap_post(11, b"d1", "secret/x", b"hidden");
    client
        .send_to(&denied_msg.encode(), gateway_addr)
        .await
        .expect("POST sends");
    let mut buf = [0u8; 2048];
    let (len, _) = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.recv_from(&mut buf),
    )
    .await
    .expect("answer arrives")
    .expect("udp recv");
    let denied = broker_gateway::coap::CoapMessage::decode(&buf[..len]).expect("answer decodes");
    assert_eq!(denied.code, CoapCode::UNAUTHORIZED);
    let leaked = tokio::time::timeout(std::time::Duration::from_millis(800), rx.recv()).await;
    assert!(leaked.is_err(), "denied CoAP publish routes nothing");
    // QoS 0 rides the bounded backlog, not the mailbox.
    // A denied publish queues nothing there either.
    assert!(
        shared.conns.pop_qos0(71).is_none(),
        "denied CoAP publish queues nothing"
    );
    let stored = shared
        .retained
        .get_retained(&Topic::new("secret/x").unwrap())
        .await
        .expect("retained read");
    assert!(stored.is_none(), "denied CoAP publish stores nothing");

    // Allowed topic: the gateway confirms and delivers.
    let allowed_msg = coap_post(12, b"a1", "open/x", b"shown");
    client
        .send_to(&allowed_msg.encode(), gateway_addr)
        .await
        .expect("POST sends");
    let (len, _) = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.recv_from(&mut buf),
    )
    .await
    .expect("answer arrives")
    .expect("udp recv");
    let allowed = broker_gateway::coap::CoapMessage::decode(&buf[..len]).expect("answer decodes");
    assert_eq!(allowed.code, CoapCode::CHANGED);
    // QoS 0 rides the bounded backlog, not the mailbox.
    // Poll the backlog until the frame arrives.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let delivered = loop {
        if let Some(frame) = shared.conns.pop_qos0(71) {
            break frame;
        }
        if std::time::Instant::now() > deadline {
            panic!("allowed CoAP publish arrives");
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };
    assert_eq!(delivered.payload, Bytes::from_static(b"shown"));
    assert!(rx.try_recv().is_err(), "QoS 0 uses the backlog");
    let stored = shared
        .retained
        .get_retained(&Topic::new("open/x").unwrap())
        .await
        .expect("retained read")
        .expect("allowed CoAP publish is retained");
    assert_eq!(stored.payload, Bytes::from_static(b"shown"));
}

/// B1-04: an ordinary MQTT publish notifies a registered CoAP
/// observer over UDP.
///
/// Registers one bounded observer, publishes through the real MQTT
/// ingress (`apply_publish` -> `ingress_pipeline` -> notify hook),
/// and asserts the observer socket receives a CONTENT notification
/// carrying the Observe option and the published payload. Fails
/// before the hook (no UDP leaves the broker); passes after it.
#[tokio::test]
async fn mqtt_publish_notifies_coap_observer_over_udp() {
    let mut shared = test_shared();
    let server = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind gateway socket");
    let server = Arc::new(server);
    shared.coap_socket = Some(server.clone());

    let client = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind observer socket");
    let client_addr = client.local_addr().expect("observer addr");
    let token = Bytes::from_static(b"ntfy");
    assert!(shared
        .coap
        .register_observer("notify/topic", client_addr, token.clone()));
    assert!(shared.coap.has_observers());
    bind_test_publisher(&shared, 90);

    // Ordinary MQTT publish on the observed topic.
    let (meta, payload) = encode_publish_meta("notify/topic", 7, 0, false, b"hello-udp");
    let (_, deliveries) = apply_publish(&publish_frame(90, 1, meta, payload), &shared).await;
    let _ = deliveries;

    // The notify hook spawns the UDP send off the hot path: wait for it.
    let mut buf = [0u8; 2048];
    let (len, _) = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        client.recv_from(&mut buf),
    )
    .await
    .expect("observer notified within timeout")
    .expect("udp recv");
    let notify = broker_gateway::coap::CoapMessage::decode(&buf[..len]).expect("decode notify");
    assert_eq!(notify.code, broker_gateway::coap::CoapCode::CONTENT);
    assert_eq!(notify.token, token);
    assert_eq!(notify.payload, Bytes::from_static(b"hello-udp"));
    assert!(
        notify
            .options
            .iter()
            .any(|o| o.number == broker_gateway::coap::option_number::OBSERVE),
        "notification must carry the Observe option"
    );
}
