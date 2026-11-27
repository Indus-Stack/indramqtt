use super::*;

/// SY-02: two tenants cannot see each other's messages while aliases,
/// retained messages and offline queues work in each, over real MQTT.
///
/// The kernel boots with one tenant rule (`t-${username}`) installed from
/// `indra.toml` via `TenantsConf -> TenantRegistry::replace`, and the
/// kernel listener serves real MQTT 5 clients, so CONNECTs carrying
/// username `user-a` land in one tenant and `user-b` in another.
/// In each tenant: the subscriber connects durably with an alias maximum,
/// a topic+alias publish registers the inbound alias and arrives with the
/// full topic plus an outbound alias, an alias-by-reference publish
/// resolves to the same topic, a retained publish is visible only inside
/// its own tenant, then the subscriber drops and a publish while detached
/// replays on reconnect (with the same username, so the same tenant).
/// Every client receives exactly its own tenant's payloads — the spec's
/// isolation, observed over real connections through the broker, never
/// the store.
#[tokio::test]
async fn sy02_tenants_isolated_with_aliases_and_offline() {
    // The kernel boots with one tenant rule in its config dir. Real
    // MQTT 5 clients use the kernel listener, so CONNECTs that carry
    // username `user-a` land in one tenant and `user-b` in another.
    let kernel = spawn_kernel("[[tenants.rules]]\nexpression = \"t-${username}\"\n").await;

    // One socket per client so every downlink is attributable.
    async fn dial(host: &str, port: u16) -> tokio::net::TcpStream {
        tokio::time::timeout(STEP, tokio::net::TcpStream::connect((host, port)))
            .await
            .expect("dial in time")
            .expect("dial")
    }
    // CONNECT with the tenant username over MQTT 5 (alias maximum 10),
    // returning session-present. The CONNECT is accepted (rc 0) with the
    // kernel alias bound every time.
    async fn connect_tenant(
        sock: &mut tokio::net::TcpStream,
        client_id: &str,
        clean_start: bool,
        username: &str,
    ) -> bool {
        use tokio::io::AsyncWriteExt;
        let connect = sy02_connect5(
            client_id,
            clean_start,
            60,
            Some((username, b"pw")),
            None,
            10,
        );
        tokio::time::timeout(STEP, sock.write_all(&connect))
            .await
            .expect("CONNECT in time")
            .expect("CONNECT send");
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(sock))
            .await
            .expect("CONNACK in time")
            .expect("CONNACK");
        assert_eq!(head, 0x20, "kernel answers {client_id} with CONNACK");
        let (present, reason, alias_max) = sy02_parse_connack5(&body);
        assert_eq!(reason, 0, "CONNECT of {client_id} must be accepted");
        assert_eq!(alias_max, 10, "CONNACK carries the kernel alias maximum");
        present
    }
    async fn subscribe_qos1(sock: &mut tokio::net::TcpStream, client_id: &str, filter: &str) {
        use tokio::io::AsyncWriteExt;
        // Speak MQTT 5 on a v5 socket. Options hold QoS 1
        // with retain-as-published set, so retained replay
        // keeps the retain flag like the old v4 path.
        let subscribe = sy02_subscribe_v5(1, &[(filter, 9)], 0);
        tokio::time::timeout(STEP, sock.write_all(&subscribe))
            .await
            .expect("SUBSCRIBE in time")
            .expect("SUBSCRIBE send");
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(sock))
            .await
            .expect("SUBACK in time")
            .expect("SUBACK");
        assert_eq!(head & 0xF0, 0x90, "kernel answers with SUBACK");
        assert_eq!(
            sy02_parse_suback5(&body),
            (1, vec![1]),
            "QoS 1 subscription of {client_id} to {filter} must be granted"
        );
    }
    async fn recv_packet(sock: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
        tokio::time::timeout(STEP, read_mqtt_packet(sock))
            .await
            .expect("downlink in time")
            .expect("downlink")
    }
    async fn publish_qos1(
        sock: &mut tokio::net::TcpStream,
        topic: &str,
        packet_id: u16,
        retain: bool,
        alias: Option<u16>,
        payload: &[u8],
    ) {
        use tokio::io::AsyncWriteExt;
        let publish = sy02_publish5(topic, packet_id, 1, retain, alias, payload);
        tokio::time::timeout(STEP, sock.write_all(&publish))
            .await
            .expect("publish in time")
            .expect("publish send");
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(sock))
            .await
            .expect("PUBACK in time")
            .expect("PUBACK");
        assert_eq!(head, 0x40, "publisher sees PUBACK");
        assert_eq!(
            body,
            vec![(packet_id >> 8) as u8, (packet_id & 0xff) as u8],
            "PUBACK mirrors its packet id"
        );
    }
    async fn ack(sock: &mut tokio::net::TcpStream, packet_id: u16) {
        use tokio::io::AsyncWriteExt;
        tokio::time::timeout(STEP, sock.write_all(&sy02_puback(packet_id)))
            .await
            .expect("PUBACK in time")
            .expect("PUBACK send");
    }
    async fn expect_silence(sock: &mut tokio::net::TcpStream, what: &str) {
        let extra =
            tokio::time::timeout(std::time::Duration::from_secs(2), read_mqtt_packet(sock)).await;
        assert!(extra.is_err(), "{what}");
    }

    let mut a_sub = dial("127.0.0.1", kernel.mqtt_port).await;
    let mut a_pub = dial("127.0.0.1", kernel.mqtt_port).await;
    let mut b_sub = dial("127.0.0.1", kernel.mqtt_port).await;
    let mut b_pub = dial("127.0.0.1", kernel.mqtt_port).await;

    assert!(
        !connect_tenant(&mut a_sub, "sy02-a-sub", false, "user-a").await,
        "tenant-a first CONNECT reports session_present=false"
    );
    assert!(
        !connect_tenant(&mut b_sub, "sy02-b-sub", false, "user-b").await,
        "tenant-b first CONNECT reports session_present=false"
    );
    subscribe_qos1(&mut a_sub, "sy02-a-sub", "sy02/iso").await;
    subscribe_qos1(&mut b_sub, "sy02-b-sub", "sy02/iso").await;
    connect_tenant(&mut a_pub, "sy02-a-pub", true, "user-a").await;
    connect_tenant(&mut b_pub, "sy02-b-pub", true, "user-b").await;

    // Alias leg per tenant: topic+alias registers, alias-by-reference
    // resolves, downlink carries the full topic plus an outbound alias.
    publish_qos1(&mut a_pub, "sy02/iso", 1, false, Some(3), b"a-1").await;
    publish_qos1(&mut b_pub, "sy02/iso", 1, false, Some(5), b"b-1").await;
    let (head, body) = recv_packet(&mut a_sub).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let a1 = sy02_parse_publish_alias(head, &body);
    assert_eq!(a1.topic, "sy02/iso");
    assert_eq!(a1.payload, b"a-1");
    assert!(!a1.dup, "live delivery carries no DUP");
    assert_ne!(a1.alias, 0, "downlink carries an outbound alias");
    let (head, body) = recv_packet(&mut b_sub).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let b1 = sy02_parse_publish_alias(head, &body);
    assert_eq!(b1.topic, "sy02/iso");
    assert_eq!(b1.payload, b"b-1");
    assert!(!b1.dup, "live delivery carries no DUP");
    assert_ne!(b1.alias, 0, "downlink carries an outbound alias");
    ack(&mut a_sub, a1.packet_id).await;
    ack(&mut b_sub, b1.packet_id).await;

    // Alias-by-reference in each tenant resolves to the same topic.
    publish_qos1(&mut a_pub, "", 2, false, Some(3), b"a-2").await;
    publish_qos1(&mut b_pub, "", 2, false, Some(5), b"b-2").await;
    let (head, body) = recv_packet(&mut a_sub).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let a2 = sy02_parse_publish_alias(head, &body);
    assert_eq!(a2.topic, "sy02/iso", "alias resolves in tenant-a");
    assert_eq!(a2.payload, b"a-2");
    assert_ne!(a2.alias, 0, "downlink carries an outbound alias");
    let (head, body) = recv_packet(&mut b_sub).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let b2 = sy02_parse_publish_alias(head, &body);
    assert_eq!(b2.topic, "sy02/iso", "alias resolves in tenant-b");
    assert_eq!(b2.payload, b"b-2");
    assert_ne!(b2.alias, 0, "downlink carries an outbound alias");
    ack(&mut a_sub, a2.packet_id).await;
    ack(&mut b_sub, b2.packet_id).await;

    // Retained leg: the same topic string holds one retained message
    // per tenant, and each tenant's new subscriber sees only its own.
    publish_qos1(&mut a_pub, "sy02/retained", 3, true, None, b"a-ret").await;
    publish_qos1(&mut b_pub, "sy02/retained", 3, true, None, b"b-ret").await;
    let mut a_ret = dial("127.0.0.1", kernel.mqtt_port).await;
    assert!(
        !connect_tenant(&mut a_ret, "sy02-a-ret", true, "user-a").await,
        "tenant-a retained reader connects clean"
    );
    subscribe_qos1(&mut a_ret, "sy02-a-ret", "sy02/retained").await;
    let (head, body) = recv_packet(&mut a_ret).await;
    assert_eq!(head & 0xF0, 0x30, "retained downlink is PUBLISH");
    let retained_a = sy02_parse_publish_alias(head, &body);
    assert_eq!(retained_a.topic, "sy02/retained");
    assert_eq!(
        retained_a.payload, b"a-ret",
        "tenant-a sees only its own retained message"
    );
    assert!(retained_a.retain, "retained replay carries retain");
    expect_silence(&mut a_ret, "tenant-a holds only its own retained message").await;
    let mut b_ret = dial("127.0.0.1", kernel.mqtt_port).await;
    assert!(
        !connect_tenant(&mut b_ret, "sy02-b-ret", true, "user-b").await,
        "tenant-b retained reader connects clean"
    );
    subscribe_qos1(&mut b_ret, "sy02-b-ret", "sy02/retained").await;
    let (head, body) = recv_packet(&mut b_ret).await;
    assert_eq!(head & 0xF0, 0x30, "retained downlink is PUBLISH");
    let retained_b = sy02_parse_publish_alias(head, &body);
    assert_eq!(retained_b.topic, "sy02/retained");
    assert_eq!(
        retained_b.payload, b"b-ret",
        "tenant-b sees only its own retained message"
    );
    assert!(retained_b.retain, "retained replay carries retain");
    expect_silence(&mut b_ret, "tenant-b holds only its own retained message").await;

    // Offline leg: both subscribers drop; one publish per tenant while
    // detached; each reconnect (same username, so the same tenant)
    // replays exactly its own tenant's message.
    drop(a_sub);
    drop(b_sub);
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    publish_qos1(&mut a_pub, "sy02/iso", 4, false, None, b"a-off").await;
    publish_qos1(&mut b_pub, "sy02/iso", 4, false, None, b"b-off").await;
    let mut a_re = dial("127.0.0.1", kernel.mqtt_port).await;
    let mut b_re = dial("127.0.0.1", kernel.mqtt_port).await;
    assert!(
        connect_tenant(&mut a_re, "sy02-a-sub", false, "user-a").await,
        "tenant-a resume reports session_present=true"
    );
    assert!(
        connect_tenant(&mut b_re, "sy02-b-sub", false, "user-b").await,
        "tenant-b resume reports session_present=true"
    );
    let (head, body) = recv_packet(&mut a_re).await;
    assert_eq!(head & 0xF0, 0x30, "replay is PUBLISH");
    let off_a = sy02_parse_publish_alias(head, &body);
    assert_eq!(off_a.topic, "sy02/iso");
    assert_eq!(off_a.payload, b"a-off");
    let (head, body) = recv_packet(&mut b_re).await;
    assert_eq!(head & 0xF0, 0x30, "replay is PUBLISH");
    let off_b = sy02_parse_publish_alias(head, &body);
    assert_eq!(off_b.topic, "sy02/iso");
    assert_eq!(off_b.payload, b"b-off");
    // Nothing crosses tenants: each reconnect holds exactly one frame.
    expect_silence(&mut a_re, "tenant-a holds only its own replay").await;
    expect_silence(&mut b_re, "tenant-b holds only its own replay").await;

    kernel.finish().await;
}

/// MT-07: the same client id in two tenants holds two connections.
///
/// The kernel boots with one tenant rule from `indra.toml`. Two real
/// MQTT clients connect with the same client id but different
/// usernames. Both CONNACKs succeed and both sockets stay open. A
/// publish in each tenant reaches only its own tenant. The test uses
/// the file entry and client sockets only.
#[tokio::test]
async fn mt07_same_client_id_in_two_tenants_stays_connected() {
    use tokio::io::AsyncWriteExt;
    // The kernel boots with one tenant rule in its config dir. Real
    // MQTT clients use the kernel listener, so each username lands in
    // its own tenant.
    let kernel = spawn_kernel("[[tenants.rules]]\nexpression = \"t-${username}\"\n").await;

    // Two sockets share one client id. Each carries its own username,
    // so each lands in its own tenant.
    let mut a_sock = tokio::time::timeout(
        STEP,
        tokio::net::TcpStream::connect(("127.0.0.1", kernel.mqtt_port)),
    )
    .await
    .expect("dial A in time")
    .expect("dial A");
    let mut b_sock = tokio::time::timeout(
        STEP,
        tokio::net::TcpStream::connect(("127.0.0.1", kernel.mqtt_port)),
    )
    .await
    .expect("dial B in time")
    .expect("dial B");
    for (sock, user) in [(&mut a_sock, "alpha"), (&mut b_sock, "beta")] {
        let connect = sy02_connect311("mt07-dup", true, 60, Some((user, b"pw")));
        tokio::time::timeout(STEP, sock.write_all(&connect))
            .await
            .expect("CONNECT in time")
            .expect("CONNECT send");
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(sock))
            .await
            .expect("CONNACK in time")
            .expect("CONNACK");
        assert_eq!(head, 0x20, "kernel answers {user} with CONNACK");
        assert_eq!(
            sy02_parse_connack311(&body),
            (false, 0),
            "first CONNECT of {user} is accepted"
        );
    }
    // Both tenants subscribe to the same topic string.
    for sock in [&mut a_sock, &mut b_sock] {
        let subscribe = sy02_subscribe(1, &[("mt07/dup", 1)]);
        tokio::time::timeout(STEP, sock.write_all(&subscribe))
            .await
            .expect("SUBSCRIBE in time")
            .expect("SUBSCRIBE send");
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(sock))
            .await
            .expect("SUBACK in time")
            .expect("SUBACK");
        assert_eq!(head, 0x90, "kernel answers with SUBACK");
        assert_eq!(sy02_parse_suback(&body), (1, vec![1]));
    }
    // One publisher per tenant.
    let mut a_pub = tokio::time::timeout(
        STEP,
        tokio::net::TcpStream::connect(("127.0.0.1", kernel.mqtt_port)),
    )
    .await
    .expect("dial A pub in time")
    .expect("dial A pub");
    let mut b_pub = tokio::time::timeout(
        STEP,
        tokio::net::TcpStream::connect(("127.0.0.1", kernel.mqtt_port)),
    )
    .await
    .expect("dial B pub in time")
    .expect("dial B pub");
    for (sock, id, user) in [
        (&mut a_pub, "mt07-a-pub", "alpha"),
        (&mut b_pub, "mt07-b-pub", "beta"),
    ] {
        let connect = sy02_connect311(id, true, 60, Some((user, b"pw")));
        tokio::time::timeout(STEP, sock.write_all(&connect))
            .await
            .expect("pub CONNECT in time")
            .expect("pub CONNECT send");
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(sock))
            .await
            .expect("pub CONNACK in time")
            .expect("pub CONNACK");
        assert_eq!(head, 0x20, "kernel answers the publisher with CONNACK");
        assert_eq!(sy02_parse_connack311(&body), (false, 0));
    }
    // A publish in tenant A reaches A only. B stays silent.
    let publish = sy02_publish311("mt07/dup", 1, 1, false, b"from-a");
    tokio::time::timeout(STEP, a_pub.write_all(&publish))
        .await
        .expect("publish in time")
        .expect("publish send");
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut a_pub))
        .await
        .expect("PUBACK in time")
        .expect("PUBACK");
    assert_eq!(head, 0x40, "publisher sees PUBACK");
    assert_eq!(body, vec![0, 1], "PUBACK mirrors its packet id");
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut a_sock))
        .await
        .expect("A downlink in time")
        .expect("A downlink");
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let downlink = sy02_parse_publish_classic(head, &body);
    assert_eq!(downlink.topic, "mt07/dup");
    assert_eq!(downlink.payload, b"from-a");
    tokio::time::timeout(STEP, a_sock.write_all(&sy02_puback(downlink.packet_id)))
        .await
        .expect("PUBACK in time")
        .expect("PUBACK send");
    let silent = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_mqtt_packet(&mut b_sock),
    )
    .await;
    assert!(silent.is_err(), "B holds none of A's publish");
    // A publish in tenant B reaches B only. A stays silent.
    let publish = sy02_publish311("mt07/dup", 2, 1, false, b"from-b");
    tokio::time::timeout(STEP, b_pub.write_all(&publish))
        .await
        .expect("publish in time")
        .expect("publish send");
    let (head, _) = tokio::time::timeout(STEP, read_mqtt_packet(&mut b_pub))
        .await
        .expect("PUBACK in time")
        .expect("PUBACK");
    assert_eq!(head, 0x40, "publisher sees PUBACK");
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut b_sock))
        .await
        .expect("B downlink in time")
        .expect("B downlink");
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let downlink = sy02_parse_publish_classic(head, &body);
    assert_eq!(downlink.topic, "mt07/dup");
    assert_eq!(downlink.payload, b"from-b");
    tokio::time::timeout(STEP, b_sock.write_all(&sy02_puback(downlink.packet_id)))
        .await
        .expect("PUBACK in time")
        .expect("PUBACK send");
    let silent = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_mqtt_packet(&mut a_sock),
    )
    .await;
    assert!(silent.is_err(), "A holds none of B's publish");

    kernel.finish().await;
}

/// SA-09: shared subscriptions keep the tenant.
///
/// The kernel boots with one tenant rule. Each tenant holds one
/// shared subscription on the same group and topic. A publish in
/// tenant A reaches only the member in tenant A. A publish in
/// tenant B reaches only the member in tenant B. The test uses
/// real sockets through the broker, never the store.
#[tokio::test]
async fn sa09_shared_subscriptions_stay_in_tenant() {
    let kernel = spawn_kernel("[[tenants.rules]]\nexpression = \"t-${username}\"\n").await;
    let mut a_sub = dial_mqtt(kernel.mqtt_port).await;
    let mut b_sub = dial_mqtt(kernel.mqtt_port).await;
    let mut a_pub = dial_mqtt(kernel.mqtt_port).await;
    let mut b_pub = dial_mqtt(kernel.mqtt_port).await;
    for (sock, id, user) in [
        (&mut a_sub, "sa09-a-sub", "user-a"),
        (&mut b_sub, "sa09-b-sub", "user-b"),
        (&mut a_pub, "sa09-a-pub", "user-a"),
        (&mut b_pub, "sa09-b-pub", "user-b"),
    ] {
        send_mqtt(sock, &sy02_connect311(id, true, 60, Some((user, b"pw")))).await;
        let (head, body) = recv_mqtt(sock).await;
        assert_eq!(head, 0x20, "kernel answers with CONNACK");
        assert_eq!(sy02_parse_connack311(&body), (false, 0));
    }
    // Both tenants share one group name and one topic string.
    // Each group balances inside its own tenant only.
    for sock in [&mut a_sub, &mut b_sub] {
        send_mqtt(sock, &sy02_subscribe(1, &[("$share/g/sa09/shared", 1)])).await;
        let (head, body) = recv_mqtt(sock).await;
        assert_eq!(head, 0x90, "kernel answers with SUBACK");
        assert_eq!(sy02_parse_suback(&body), (1, vec![1]));
    }
    // A publish in tenant A reaches A only. B stays silent.
    send_mqtt(
        &mut a_pub,
        &sy02_publish311("sa09/shared", 1, 1, false, b"from-a"),
    )
    .await;
    let (head, _) = recv_mqtt(&mut a_pub).await;
    assert_eq!(head, 0x40, "publisher sees PUBACK");
    let (head, body) = recv_mqtt(&mut a_sub).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let downlink = sy02_parse_publish_classic(head, &body);
    assert_eq!(downlink.topic, "sa09/shared");
    assert_eq!(downlink.payload, b"from-a");
    send_mqtt(&mut a_sub, &sy02_puback(downlink.packet_id)).await;
    let silent = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_mqtt_packet(&mut b_sub),
    )
    .await;
    assert!(silent.is_err(), "B holds none of A shared publish");
    // A publish in tenant B reaches B only. A stays silent.
    send_mqtt(
        &mut b_pub,
        &sy02_publish311("sa09/shared", 2, 1, false, b"from-b"),
    )
    .await;
    let (head, _) = recv_mqtt(&mut b_pub).await;
    assert_eq!(head, 0x40, "publisher sees PUBACK");
    let (head, body) = recv_mqtt(&mut b_sub).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let downlink = sy02_parse_publish_classic(head, &body);
    assert_eq!(downlink.topic, "sa09/shared");
    assert_eq!(downlink.payload, b"from-b");
    send_mqtt(&mut b_sub, &sy02_puback(downlink.packet_id)).await;
    let silent = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_mqtt_packet(&mut a_sub),
    )
    .await;
    assert!(silent.is_err(), "A holds none of B shared publish");

    kernel.finish().await;
}

/// SA-09: the last will keeps the tenant.
///
/// The kernel boots with one tenant rule. Each tenant subscribes
/// to one will topic. A publisher in tenant A drops without
/// DISCONNECT. Only tenant A receives the will. Then a publisher
/// in tenant B drops. Only tenant B receives the will. The test
/// uses real sockets through the broker, never the store.
#[tokio::test]
async fn sa09_will_stays_in_tenant() {
    let kernel = spawn_kernel("[[tenants.rules]]\nexpression = \"t-${username}\"\n").await;
    let mut a_sub = dial_mqtt(kernel.mqtt_port).await;
    let mut b_sub = dial_mqtt(kernel.mqtt_port).await;
    for (sock, id, user) in [
        (&mut a_sub, "sa09-will-a-sub", "user-a"),
        (&mut b_sub, "sa09-will-b-sub", "user-b"),
    ] {
        send_mqtt(sock, &sy02_connect311(id, true, 60, Some((user, b"pw")))).await;
        let (head, body) = recv_mqtt(sock).await;
        assert_eq!(head, 0x20, "kernel answers with CONNACK");
        assert_eq!(sy02_parse_connack311(&body), (false, 0));
        send_mqtt(sock, &sy02_subscribe(1, &[("sa09/will", 0)])).await;
        let (head, body) = recv_mqtt(sock).await;
        assert_eq!(head, 0x90, "kernel answers with SUBACK");
        assert_eq!(sy02_parse_suback(&body), (1, vec![0]));
    }
    // Tenant A publisher drops. Only tenant A receives the will.
    let mut a_pub = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(
        &mut a_pub,
        &sy02_connect5(
            "sa09-will-a-pub",
            true,
            60,
            Some(("user-a", b"pw")),
            Some(("sa09/will", b"will-a")),
            0,
        ),
    )
    .await;
    let (head, body) = recv_mqtt(&mut a_pub).await;
    assert_eq!(head & 0xF0, 0x20, "publisher gets CONNACK");
    assert_eq!(sy02_parse_connack5(&body).1, 0);
    drop(a_pub);
    let (head, body) = recv_mqtt(&mut a_sub).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let first = sy02_parse_publish_classic(head, &body);
    assert_eq!(first.topic, "sa09/will");
    assert_eq!(first.payload, b"will-a");
    let silent = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_mqtt_packet(&mut b_sub),
    )
    .await;
    assert!(silent.is_err(), "B holds none of A will");
    // Tenant B publisher drops. Only tenant B receives the will.
    let mut b_pub = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(
        &mut b_pub,
        &sy02_connect5(
            "sa09-will-b-pub",
            true,
            60,
            Some(("user-b", b"pw")),
            Some(("sa09/will", b"will-b")),
            0,
        ),
    )
    .await;
    let (head, body) = recv_mqtt(&mut b_pub).await;
    assert_eq!(head & 0xF0, 0x20, "publisher gets CONNACK");
    assert_eq!(sy02_parse_connack5(&body).1, 0);
    drop(b_pub);
    let (head, body) = recv_mqtt(&mut b_sub).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let second = sy02_parse_publish_classic(head, &body);
    assert_eq!(second.topic, "sa09/will");
    assert_eq!(second.payload, b"will-b");
    let silent = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_mqtt_packet(&mut a_sub),
    )
    .await;
    assert!(silent.is_err(), "A holds none of B will");

    kernel.finish().await;
}

/// SA-09: a reconnect in another tenant keeps both sessions.
///
/// The kernel boots with one tenant rule. One client id connects
/// in tenant A and again in tenant B. Both connects succeed and
/// both sessions stay live. Each publish reaches only its own
/// tenant. A new connect in tenant A resumes A and leaves B live.
/// The test uses real sockets through the broker, never the store.
#[tokio::test]
async fn sa09_reconnect_in_other_tenant_keeps_both_sessions() {
    let kernel = spawn_kernel("[[tenants.rules]]\nexpression = \"t-${username}\"\n").await;
    let mut a_sock = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(
        &mut a_sock,
        &sy02_connect311("sa09-dup", false, 60, Some(("user-a", b"pw"))),
    )
    .await;
    let (head, body) = recv_mqtt(&mut a_sock).await;
    assert_eq!(head, 0x20, "kernel answers with CONNACK");
    assert_eq!(sy02_parse_connack311(&body), (false, 0));
    let mut b_sock = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(
        &mut b_sock,
        &sy02_connect311("sa09-dup", false, 60, Some(("user-b", b"pw"))),
    )
    .await;
    let (head, body) = recv_mqtt(&mut b_sock).await;
    assert_eq!(head, 0x20, "kernel answers with CONNACK");
    assert_eq!(
        sy02_parse_connack311(&body),
        (false, 0),
        "other tenant starts a fresh session"
    );
    // Both sessions subscribe to one topic string.
    for sock in [&mut a_sock, &mut b_sock] {
        send_mqtt(sock, &sy02_subscribe(1, &[("sa09/dup", 1)])).await;
        let (head, body) = recv_mqtt(sock).await;
        assert_eq!(head, 0x90, "kernel answers with SUBACK");
        assert_eq!(sy02_parse_suback(&body), (1, vec![1]));
    }
    let mut a_pub = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(
        &mut a_pub,
        &sy02_connect311("sa09-dup-a-pub", true, 60, Some(("user-a", b"pw"))),
    )
    .await;
    let (head, _) = recv_mqtt(&mut a_pub).await;
    assert_eq!(head, 0x20, "publisher gets CONNACK");
    let mut b_pub = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(
        &mut b_pub,
        &sy02_connect311("sa09-dup-b-pub", true, 60, Some(("user-b", b"pw"))),
    )
    .await;
    let (head, _) = recv_mqtt(&mut b_pub).await;
    assert_eq!(head, 0x20, "publisher gets CONNACK");
    // A publish in tenant A reaches A only. B stays silent.
    send_mqtt(
        &mut a_pub,
        &sy02_publish311("sa09/dup", 1, 1, false, b"a-1"),
    )
    .await;
    let (head, _) = recv_mqtt(&mut a_pub).await;
    assert_eq!(head, 0x40, "publisher sees PUBACK");
    let (head, body) = recv_mqtt(&mut a_sock).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let downlink = sy02_parse_publish_classic(head, &body);
    assert_eq!(downlink.payload, b"a-1");
    send_mqtt(&mut a_sock, &sy02_puback(downlink.packet_id)).await;
    let silent = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_mqtt_packet(&mut b_sock),
    )
    .await;
    assert!(silent.is_err(), "B holds none of A publish");
    // A publish in tenant B reaches B only. A stays silent.
    send_mqtt(
        &mut b_pub,
        &sy02_publish311("sa09/dup", 2, 1, false, b"b-1"),
    )
    .await;
    let (head, _) = recv_mqtt(&mut b_pub).await;
    assert_eq!(head, 0x40, "publisher sees PUBACK");
    let (head, body) = recv_mqtt(&mut b_sock).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let downlink = sy02_parse_publish_classic(head, &body);
    assert_eq!(downlink.payload, b"b-1");
    send_mqtt(&mut b_sock, &sy02_puback(downlink.packet_id)).await;
    let silent = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_mqtt_packet(&mut a_sock),
    )
    .await;
    assert!(silent.is_err(), "A holds none of B publish");
    // A new connect in tenant A resumes A and leaves B live.
    let mut a_re = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(
        &mut a_re,
        &sy02_connect311("sa09-dup", false, 60, Some(("user-a", b"pw"))),
    )
    .await;
    let (head, body) = recv_mqtt(&mut a_re).await;
    assert_eq!(head, 0x20, "kernel answers with CONNACK");
    assert_eq!(
        sy02_parse_connack311(&body),
        (true, 0),
        "same tenant resumes its session"
    );
    send_mqtt(
        &mut b_pub,
        &sy02_publish311("sa09/dup", 3, 1, false, b"b-2"),
    )
    .await;
    let (head, _) = recv_mqtt(&mut b_pub).await;
    assert_eq!(head, 0x40, "publisher sees PUBACK");
    let (head, body) = recv_mqtt(&mut b_sock).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let downlink = sy02_parse_publish_classic(head, &body);
    assert_eq!(downlink.payload, b"b-2");
    send_mqtt(&mut b_sock, &sy02_puback(downlink.packet_id)).await;
    let silent = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_mqtt_packet(&mut a_re),
    )
    .await;
    assert!(silent.is_err(), "resumed A holds none of B publish");

    kernel.finish().await;
}

/// SA-09: the cluster entry is refused and the node stays single.
///
/// The loader refuses each cluster entry at schema load. The boot
/// with a file entry exits. The single node binds no UDP socket.
/// A default tenant publish reaches its own subscriber only, so
/// no message crosses nodes. The test uses file, flag and socket
/// entries, never the store.
#[tokio::test]
async fn sa09_cluster_entry_stays_single_node() {
    // The schema refuses each entry. Each load must fail.
    // On the base each load succeeds, so each assert fails there.
    {
        use broker_config::layers::{load_layered_with_env, CliOverrides};
        let tag = format!(
            "sa09-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        );
        // The file seed_nodes leg must fail.
        {
            let dir = std::env::temp_dir().join(format!("indramqtt-{tag}-seeds"));
            std::fs::create_dir_all(&dir).expect("create temp config dir");
            std::fs::write(
                dir.join("indra.toml"),
                "[cluster]\nseed_nodes = [\"127.0.0.1:19883\"]\n",
            )
            .expect("write indra.toml");
            let err = load_layered_with_env(&dir, &[], &CliOverrides::default())
                .expect_err("seed_nodes file must fail");
            assert!(
                err.to_string().contains("cluster.seed_nodes"),
                "error names the seed entry, got: {err}"
            );
            let _ = std::fs::remove_dir_all(&dir);
        }
        // The file enabled leg must fail.
        {
            let dir = std::env::temp_dir().join(format!("indramqtt-{tag}-enabled"));
            std::fs::create_dir_all(&dir).expect("create temp config dir");
            std::fs::write(dir.join("indra.toml"), "[cluster]\nenabled = true\n")
                .expect("write indra.toml");
            let err = load_layered_with_env(&dir, &[], &CliOverrides::default())
                .expect_err("enabled file must fail");
            assert!(
                err.to_string().contains("cluster.enabled"),
                "error names the enabled entry, got: {err}"
            );
            let _ = std::fs::remove_dir_all(&dir);
        }
        // The --cluster-seeds flag leg must fail.
        {
            let dir = std::env::temp_dir().join(format!("indramqtt-{tag}-flag"));
            std::fs::create_dir_all(&dir).expect("create temp config dir");
            let cli = CliOverrides {
                cluster_seeds: Some("127.0.0.1:19883".to_string()),
                ..CliOverrides::default()
            };
            let err =
                load_layered_with_env(&dir, &[], &cli).expect_err("--cluster-seeds flag must fail");
            assert!(
                err.to_string().contains("cluster.seed_nodes"),
                "error names the seed entry, got: {err}"
            );
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
    // The boot with a file entry exits. The file is the production entry.
    // On the base the kernel boots, so the exit assert fails there.
    {
        let tag = format!(
            "sa09-boot-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        );
        let scratch = std::env::temp_dir().join(format!("indramqtt-{tag}-data"));
        let config_dir = std::env::temp_dir().join(format!("indramqtt-{tag}-cfg"));
        std::fs::create_dir_all(&config_dir).expect("create temp config dir");
        let bl_port = ephemeral_port();
        let mqtt_port = ephemeral_port();
        let ws_port = ephemeral_port();
        let api_port = ephemeral_port();
        std::fs::write(
            config_dir.join("indra.toml"),
            format!(
                "[node]\nid = \"sa09-refuse\"\ndata_dir = \"{}\"\nbrokerlink_bind = \"127.0.0.1:{bl_port}\"\n[listeners.tcp]\nbind = \"127.0.0.1:{mqtt_port}\"\n[listeners.tls]\nenabled = false\n[listeners.ws]\nbind = \"127.0.0.1:{ws_port}\"\n[listeners.wss]\nenabled = false\n[listeners.api]\nbind = \"127.0.0.1:{api_port}\"\n[logging]\nlevel = \"warn\"\n[cluster]\nseed_nodes = [\"127.0.0.1:19883\"]\n",
                scratch.display(),
            ),
        )
        .expect("write indra.toml");
        let mut child = tokio::process::Command::new(shard_kernel_binary())
            .arg("--config-dir")
            .arg(&config_dir)
            .arg("--allow-anonymous")
            .env("INDRA_API_KEYS", API_KEY)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn real kernel");
        let mut exited = false;
        for _ in 0..50 {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            if let Ok(Some(_)) = child.try_wait() {
                exited = true;
                break;
            }
        }
        assert!(exited, "kernel with cluster entry exits");
        let _ = child.kill().await;
        let _ = std::fs::remove_dir_all(&scratch);
        let _ = std::fs::remove_dir_all(&config_dir);
    }
    // The single node binds no UDP socket and forwards no default message.
    // The default tenant publish goes through forward_cluster with no plane.
    let kernel = spawn_kernel("").await;
    let udp = tokio::net::UdpSocket::bind("127.0.0.1:19883")
        .await
        .expect("cluster UDP stays free");
    drop(udp);
    let mut sub = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(
        &mut sub,
        &sy02_connect311("sa09-cl-def-sub", true, 60, None),
    )
    .await;
    let (head, body) = recv_mqtt(&mut sub).await;
    assert_eq!(head, 0x20, "kernel answers with CONNACK");
    assert_eq!(sy02_parse_connack311(&body), (false, 0));
    send_mqtt(&mut sub, &sy02_subscribe(1, &[("sa09/cluster", 1)])).await;
    let (head, body) = recv_mqtt(&mut sub).await;
    assert_eq!(head, 0x90, "kernel answers with SUBACK");
    assert_eq!(sy02_parse_suback(&body), (1, vec![1]));
    let mut publisher = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(
        &mut publisher,
        &sy02_connect311("sa09-cl-def-pub", true, 60, None),
    )
    .await;
    let (head, _) = recv_mqtt(&mut publisher).await;
    assert_eq!(head, 0x20, "publisher gets CONNACK");
    send_mqtt(
        &mut publisher,
        &sy02_publish311("sa09/cluster", 1, 1, false, b"default-only"),
    )
    .await;
    let (head, _) = recv_mqtt(&mut publisher).await;
    assert_eq!(head, 0x40, "publisher sees PUBACK");
    let (head, body) = recv_mqtt(&mut sub).await;
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    assert_eq!(
        sy02_parse_publish_classic(head, &body).payload,
        b"default-only"
    );
    let silent = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_mqtt_packet(&mut sub),
    )
    .await;
    assert!(silent.is_err(), "default publish forwards nothing extra");

    kernel.finish().await;
}

/// MV-05: the tenant session limit applies at the MQTT entry.
///
/// The kernel boots with one tenant rule. A management write sets
/// `limits.max_sessions` to 1 for one tenant. The first MQTT session
/// connects. A second client in the same tenant gets CONNACK 0x03.
/// Another tenant still connects. The test uses real sockets and the
/// real API, not a store read.
#[tokio::test]
async fn mv05_tenant_session_limit_refuses_second_mqtt_session() {
    let kernel = spawn_kernel("[[tenants.rules]]\nexpression = \"t-${username}\"\n").await;
    let base = format!("http://127.0.0.1:{}", kernel.api_port);
    let http = reqwest::Client::new();

    let created = tokio::time::timeout(
        STEP,
        http.post(format!("{base}/api/v5/mt/ns/t-mv05-lim"))
            .bearer_auth(API_KEY)
            .json(&serde_json::json!({}))
            .send(),
    )
    .await
    .expect("namespace create in time")
    .expect("namespace create sends");
    assert_eq!(created.status(), 204, "managed namespace is created");

    let limit = tokio::time::timeout(
        STEP,
        http.put(format!("{base}/api/v5/mt/ns/t-mv05-lim/config"))
            .bearer_auth(API_KEY)
            .json(&serde_json::json!({"limits": {"max_sessions": 1}}))
            .send(),
    )
    .await
    .expect("limit write in time")
    .expect("limit write sends");
    assert_eq!(limit.status(), 200, "tenant session limit is stored");

    let mut first = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(
        &mut first,
        &sy02_connect311("mv05-limit-a", true, 60, Some(("mv05-lim", b"pw"))),
    )
    .await;
    let (head, body) = recv_mqtt(&mut first).await;
    assert_eq!(head, 0x20, "first session gets CONNACK");
    let (_, reason) = sy02_parse_connack311(&body);
    assert_eq!(reason, 0, "first session in the tenant is accepted");

    let mut second = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(
        &mut second,
        &sy02_connect311("mv05-limit-b", true, 60, Some(("mv05-lim", b"pw"))),
    )
    .await;
    let (head, body) = recv_mqtt(&mut second).await;
    assert_eq!(head, 0x20, "second session gets CONNACK");
    let (_, reason) = sy02_parse_connack311(&body);
    assert_eq!(
        reason, 0x03,
        "the tenant session limit refuses the second session"
    );

    let mut other = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(
        &mut other,
        &sy02_connect311("mv05-limit-c", true, 60, Some(("mv05-other", b"pw"))),
    )
    .await;
    let (head, body) = recv_mqtt(&mut other).await;
    assert_eq!(head, 0x20, "other tenant gets CONNACK");
    let (_, reason) = sy02_parse_connack311(&body);
    assert_eq!(reason, 0, "another tenant still connects");

    kernel.finish().await;
}

/// TM-09: tenant in client and subscription reads, through real
/// sockets and the real API.
///
/// The kernel boots from `indra.toml` with one tenant rule and its own
/// MQTT listener, so no edge is needed. Two tenants connect the same
/// client id. Each tenant publishes a random value the broker cannot
/// know. Delivery stays in-tenant. The API then shows each tenant its
/// own rows only, and the default-tenant reads keep their shapes.
#[tokio::test]
async fn tm09_tenant_client_reads_through_sockets_and_api() {
    use tokio::io::AsyncWriteExt;

    let tag = format!(
        "tm09-reads-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let scratch = std::env::temp_dir().join(format!("indramqtt-{tag}"));
    let config_dir = std::env::temp_dir().join(format!("indramqtt-{tag}-cfg"));
    std::fs::create_dir_all(&config_dir).expect("create temp config dir");

    let bl_port = ephemeral_port();
    let mqtt_port = ephemeral_port();
    let api_port = ephemeral_port();
    std::fs::write(
        config_dir.join("indra.toml"),
        format!(
            "[node]\n\
             id = \"tm09-e2e\"\n\
             data_dir = \"{}\"\n\
             brokerlink_bind = \"127.0.0.1:{bl_port}\"\n\
             \n\
             [listeners.tcp]\n\
              bind = \"127.0.0.1:{mqtt_port}\"\n\
              \n\
              [listeners.tls]\n\
              enabled = false\n\
              \n\
              [listeners.ws]\n\
              enabled = false\n\
              \n\
              [listeners.wss]\n\
              enabled = false\n\
              \n\
              [listeners.api]\n\
              bind = \"127.0.0.1:{api_port}\"\n\
              \n\
              [logging]\n\
              level = \"warn\"\n\
              \n\
              [[tenants.rules]]\n\
              expression = \"t-${{username}}\"\n",
            scratch.display(),
        ),
    )
    .expect("write indra.toml");

    // Operator key: 21 chars, above the 16-char minimum. Bearer auth
    // opens the management API with no user setup.
    let api_key = "tm09-e2e-test-key-0001";
    let mut child = tokio::process::Command::new(shard_kernel_binary())
        .arg("--config-dir")
        .arg(&config_dir)
        .arg("--allow-anonymous")
        .env("INDRA_API_KEYS", api_key)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn real kernel");
    let http = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{api_port}");
    let deadline = std::time::Instant::now() + BOOT;
    loop {
        let up = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            http.get(format!("{base}/healthz")).send(),
        )
        .await;
        if let Ok(Ok(res)) = up {
            if let Ok(text) = res.text().await {
                if text == "OK"
                    && tokio::net::TcpStream::connect(("127.0.0.1", mqtt_port))
                        .await
                        .is_ok()
                {
                    break;
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill().await;
            panic!("tm09 kernel never opened API {api_port} and MQTT {mqtt_port}");
        }
        if let Ok(Some(status)) = child.try_wait() {
            panic!("tm09 kernel exited during boot: {status}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    async fn dial(port: u16) -> tokio::net::TcpStream {
        tokio::time::timeout(STEP, tokio::net::TcpStream::connect(("127.0.0.1", port)))
            .await
            .expect("dial in time")
            .expect("dial")
    }
    async fn connect_user(sock: &mut tokio::net::TcpStream, client_id: &str, username: &str) {
        tokio::time::timeout(
            STEP,
            sock.write_all(&sy02_connect311(
                client_id,
                true,
                60,
                Some((username, b"pw")),
            )),
        )
        .await
        .expect("CONNECT in time")
        .expect("CONNECT send");
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(sock))
            .await
            .expect("CONNACK in time")
            .expect("CONNACK");
        assert_eq!(head, 0x20, "kernel answers {client_id} with CONNACK");
        assert_eq!(body, vec![0x00, 0x00], "CONNECT of {client_id} is accepted");
    }
    async fn subscribe(sock: &mut tokio::net::TcpStream, filter: &str) {
        tokio::time::timeout(STEP, sock.write_all(&sy02_subscribe(1, &[(filter, 0)])))
            .await
            .expect("SUBSCRIBE in time")
            .expect("SUBSCRIBE send");
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(sock))
            .await
            .expect("SUBACK in time")
            .expect("SUBACK");
        assert_eq!(head & 0xF0, 0x90, "kernel answers with SUBACK");
        assert_eq!(body, vec![0x00, 0x01, 0x00], "subscribe is granted");
    }

    // Same client id in two tenants. Both connects succeed.
    let mut a_sub = dial(mqtt_port).await;
    let mut b_sub = dial(mqtt_port).await;
    let mut a_pub = dial(mqtt_port).await;
    let mut b_pub = dial(mqtt_port).await;
    connect_user(&mut a_sub, "tm09-dup", "tm09-a").await;
    connect_user(&mut b_sub, "tm09-dup", "tm09-b").await;
    subscribe(&mut a_sub, "tm09/check").await;
    subscribe(&mut b_sub, "tm09/check").await;
    connect_user(&mut a_pub, "tm09-a-pub", "tm09-a").await;
    connect_user(&mut b_pub, "tm09-b-pub", "tm09-b").await;

    // Random values the broker cannot know. Delivery stays in-tenant.
    let secret_a = format!("tm09-{}-a-{}", std::process::id(), rand::random::<u64>());
    let secret_b = format!("tm09-{}-b-{}", std::process::id(), rand::random::<u64>());
    tokio::time::timeout(
        STEP,
        a_pub.write_all(&sy02_publish311(
            "tm09/check",
            0,
            0,
            false,
            secret_a.as_bytes(),
        )),
    )
    .await
    .expect("publish in time")
    .expect("publish send");
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut a_sub))
        .await
        .expect("downlink in time")
        .expect("downlink");
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let own = sy02_parse_publish_classic(head, &body);
    assert_eq!(own.topic, "tm09/check");
    assert_eq!(own.payload, secret_a.as_bytes());
    let silent = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_mqtt_packet(&mut b_sub),
    )
    .await;
    assert!(silent.is_err(), "tenant B holds none of tenant A publish");

    tokio::time::timeout(
        STEP,
        b_pub.write_all(&sy02_publish311(
            "tm09/check",
            0,
            0,
            false,
            secret_b.as_bytes(),
        )),
    )
    .await
    .expect("publish in time")
    .expect("publish send");
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut b_sub))
        .await
        .expect("downlink in time")
        .expect("downlink");
    assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
    let own = sy02_parse_publish_classic(head, &body);
    assert_eq!(own.payload, secret_b.as_bytes());
    let silent = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        read_mqtt_packet(&mut a_sub),
    )
    .await;
    assert!(silent.is_err(), "tenant A holds none of tenant B publish");

    // Reads through the API. Each tenant filter shows its rows only.
    async fn api_get(
        http: &reqwest::Client,
        base: &str,
        key: &str,
        path: &str,
    ) -> (u16, serde_json::Value) {
        let res = tokio::time::timeout(
            STEP,
            http.get(format!("{base}{path}")).bearer_auth(key).send(),
        )
        .await
        .expect("API in time")
        .expect("API send");
        let status = res.status().as_u16();
        let body: serde_json::Value = tokio::time::timeout(STEP, res.json())
            .await
            .expect("body in time")
            .expect("body is JSON");
        (status, body)
    }
    for tenant in ["t-tm09-a", "t-tm09-b"] {
        let (status, body) = api_get(
            &http,
            &base,
            api_key,
            &format!("/api/v5/clients?tenant={tenant}"),
        )
        .await;
        assert_eq!(status, 200, "client list of {tenant}");
        let data = body["data"].as_array().expect("client list has data");
        assert!(!data.is_empty(), "{tenant} keeps its rows");
        for row in data {
            assert_eq!(row["mountpoint"], serde_json::json!(tenant));
        }
        assert!(
            data.iter().any(|row| row["clientid"] == "tm09-dup"),
            "{tenant} shows its namesake"
        );
    }
    let (status, body) = api_get(&http, &base, api_key, "/api/v5/clients").await;
    assert_eq!(status, 200);
    let data = body["data"].as_array().expect("client list has data");
    let mut mounts: Vec<&str> = data
        .iter()
        .filter(|row| row["clientid"] == "tm09-dup")
        .map(|row| row["mountpoint"].as_str().expect("row carries mountpoint"))
        .collect();
    mounts.sort_unstable();
    assert_eq!(mounts, vec!["t-tm09-a", "t-tm09-b"]);

    // The bare detail read keeps the default tenant, so it misses
    // here instead of leaking another tenant session.
    let (status, _) = api_get(&http, &base, api_key, "/api/v5/clients/tm09-dup").await;
    assert_eq!(status, 404);
    let (status, body) = api_get(
        &http,
        &base,
        api_key,
        "/api/v5/clients/tm09-dup?tenant=t-tm09-a",
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["mountpoint"], serde_json::json!("t-tm09-a"));

    // Subscription reads scope the same way with the current shape.
    let (status, body) = api_get(
        &http,
        &base,
        api_key,
        "/api/v5/subscriptions?tenant=t-tm09-a",
    )
    .await;
    assert_eq!(status, 200);
    let rows = body.as_array().expect("subscription list is an array");
    assert!(!rows.is_empty(), "tenant A keeps its subscriptions");
    for row in rows {
        assert!(row.get("tenant").is_none());
    }
    assert!(
        rows.iter().any(|row| row["topic"] == "tm09/check"),
        "tenant A shows its subscription"
    );
    let (status, _) = api_get(&http, &base, api_key, "/api/v5/topics").await;
    assert_eq!(status, 200);
    let (status, _) = api_get(&http, &base, api_key, "/api/v5/sessions_count").await;
    assert_eq!(status, 200);

    let _ = child.kill().await;
    let _ = std::fs::remove_dir_all(&scratch);
    let _ = std::fs::remove_dir_all(&config_dir);
}
