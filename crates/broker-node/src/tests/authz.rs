use super::*;
use tokio::net::TcpStream;

#[tokio::test]
async fn bind_quota_overage_returns_0x8b() {
    use broker_auth::UserQuotas;

    let shared = test_shared();
    shared
        .auth
        .add_user("capped-user", b"pw")
        .expect("memory-only persist cannot fail");
    assert!(shared
        .auth
        .set_quotas(
            "capped-user",
            UserQuotas {
                max_connections: Some(1),
                max_publish_rate: None,
                max_publish_burst: None,
            }
        )
        .expect("memory-only persist cannot fail"));

    // First connection admitted.
    let frame = bind_frame(
        84,
        1,
        encode_bind_meta_creds("dev-one", true, 60, "capped-user", b"pw"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);

    // Second concurrent connection for the same user: quota exceeded.
    let frame = bind_frame(
        85,
        1,
        encode_bind_meta_creds("dev-two", true, 60, "capped-user", b"pw"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x8B, "overage must yield return code 0x8B");
    assert!(!present);
    assert!(shared.sessions.get("dev-two").is_none());

    // Takeover by the same client reuses its slot instead of denying.
    let frame = bind_frame(
        86,
        1,
        encode_bind_meta_creds("dev-one", true, 60, "capped-user", b"pw"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);

    // The orphaned first connection detaching afterwards releases
    // nothing (ownership check): the retaken slot stays held.
    shared.sessions.unbind_connection("dev-one", 84);
    let frame = bind_frame(
        87,
        1,
        encode_bind_meta_creds("dev-three", true, 60, "capped-user", b"pw"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x8B, "slot still held after orphan detach");
}

#[tokio::test]
async fn publish_rate_overage_returns_0x97_and_drops() {
    use broker_auth::UserQuotas;

    let shared = test_shared();
    shared
        .auth
        .add_user("fast-user", b"pw")
        .expect("memory-only persist cannot fail");
    assert!(shared
        .auth
        .set_quotas(
            "fast-user",
            UserQuotas {
                max_connections: None,
                max_publish_rate: Some(2),
                max_publish_burst: Some(2),
            }
        )
        .expect("memory-only persist cannot fail"));

    // Subscriber soaks up whatever routes (proves the drop below).
    let (listener, _) = shared.sessions.get_or_create("sink", true);
    *listener.conn_id.write() = Some(96);
    let sub = subscribe_frame(96, 1, encode_subscribe_meta(1, "sink", &[("t/rate", 0)]));
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");

    // Publisher session owned by the capped user.
    let (publ, _) = shared.sessions.get_or_create("throttled", true);
    *publ.conn_id.write() = Some(97);
    *publ.username.write() = Some("fast-user".to_string());

    // Burst of 2 routes; the immediate third is over quota.
    for seq in 2..=3u64 {
        let (meta, payload) = encode_publish_meta("t/rate", seq as u16, 1, false, b"x");
        let (ack, deliveries) =
            apply_publish(&publish_frame(97, seq, meta, payload), &shared).await;
        assert!(ack.is_some());
        assert_eq!(deliveries.len(), 1, "burst publishes must route");
    }
    let (meta, payload) = encode_publish_meta("t/rate", 4, 1, false, b"x");
    let (ack, deliveries) = apply_publish(&publish_frame(97, 4, meta, payload), &shared).await;
    let ack = ack.expect("QoS 1 over-rate still gets a PubAck");
    assert_eq!(&ack.metadata[..], &[0x00, 0x04, 0x97u8]);
    assert!(deliveries.is_empty(), "over-rate publish must not fan out");

    // QoS 0 over-rate drops silently but counts.
    let (meta, payload) = encode_publish_meta("t/rate", 0, 0, false, b"x");
    let (ack, deliveries) = apply_publish(&publish_frame(97, 5, meta, payload), &shared).await;
    assert!(ack.is_none());
    assert!(deliveries.is_empty());
    // Both over-rate drops counted (QoS 1 above + this QoS 0).
    assert_eq!(shared.metrics.messages_dropped(), 2);
}

#[tokio::test]
async fn subscribe_denied_v4_yields_0x80_without_routing() {
    let shared = test_shared();
    shared
        .auth
        .add_rule(AclRule::new("boxed", AclAction::All, "#", false))
        .expect("memory-only persist cannot fail");

    let frame = subscribe_frame(91, 1, encode_subscribe_meta(1, "boxed", &[("t", 0)]));
    let (reply, retained) = apply_subscribe(&frame, &shared).await;
    let reply = reply.expect("suback replies");
    // [MQTT-3.9.3-2]: a version-3 SUBACK allows only 0, 1, 2 and 0x80.
    assert_eq!(&reply.metadata[2..], &[0x80u8]);
    assert!(retained.is_empty());
    // Denied subscriptions register nothing.
    assert!(shared.router.matches(&Topic::new("t").unwrap()).is_empty());
}

#[tokio::test]
async fn subscribe_denied_v5_yields_0x87_without_routing() {
    let shared = test_shared();
    shared
        .auth
        .add_rule(AclRule::new("boxed5", AclAction::All, "#", false))
        .expect("memory-only persist cannot fail");

    let opts = broker_router::Subscription::encode_options_byte(QoS::AtMostOnce, false, false, 0);
    let frame = subscribe_frame(
        91,
        1,
        encode_subscribe_meta_v5(1, "boxed5", &[("t", opts, 0)]),
    );
    let (reply, retained) = apply_subscribe(&frame, &shared).await;
    let reply = reply.expect("suback replies");
    // Version 5 carries the precise reason code, Not Authorized.
    assert_eq!(&reply.metadata[2..], &[V5_SUBACK_NOT_AUTHORIZED]);
    assert!(retained.is_empty());
    assert!(shared.router.matches(&Topic::new("t").unwrap()).is_empty());
}

/// A version-4 client that the ACL denies reads SUBACK 0x80 from a
/// real socket. [MQTT-3.9.3-2] allows only 0, 1, 2 and 0x80 in a
/// version-3 SUBACK. The kernel serves its own native listener here.
#[tokio::test]
async fn subscribe_denied_v4_over_socket_answers_0x80() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    fn remaining_len(mut len: usize) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let mut byte = (len % 128) as u8;
            len /= 128;
            if len > 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if len == 0 {
                return out;
            }
        }
    }
    fn connect_v4(client_id: &str) -> Vec<u8> {
        let mut body = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3C];
        body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
        body.extend_from_slice(client_id.as_bytes());
        let mut packet = vec![0x10];
        packet.extend_from_slice(&remaining_len(body.len()));
        packet.extend_from_slice(&body);
        packet
    }
    fn subscribe_once(packet_id: u16, filter: &str) -> Vec<u8> {
        let mut body = packet_id.to_be_bytes().to_vec();
        body.extend_from_slice(&(filter.len() as u16).to_be_bytes());
        body.extend_from_slice(filter.as_bytes());
        body.push(0x00);
        let mut packet = vec![0x82];
        packet.extend_from_slice(&remaining_len(body.len()));
        packet.extend_from_slice(&body);
        packet
    }
    async fn read_packet(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
        let mut first = [0u8; 1];
        stream.read_exact(&mut first).await?;
        let mut packet = vec![first[0]];
        let mut multiplier = 1usize;
        let mut remaining = 0usize;
        loop {
            let mut byte = [0u8; 1];
            stream.read_exact(&mut byte).await?;
            packet.push(byte[0]);
            remaining += ((byte[0] & 0x7F) as usize) * multiplier;
            multiplier *= 128;
            if byte[0] & 0x80 == 0 {
                break;
            }
        }
        if remaining > 0 {
            let mut body = vec![0u8; remaining];
            stream.read_exact(&mut body).await?;
            packet.extend_from_slice(&body);
        }
        Ok(packet)
    }

    let shared = test_shared();
    shared
        .auth
        .add_rule(AclRule::new(
            "denied-over-socket",
            AclAction::All,
            "#",
            false,
        ))
        .expect("memory-only persist cannot fail");
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe binds");
    let port = probe.local_addr().expect("probe addr").port();
    drop(probe);
    let mut listeners = broker_config::schema::ListenersConf::default();
    listeners.tcp.enabled = true;
    listeners.tcp.bind = format!("127.0.0.1:{port}");
    listeners.tls.enabled = false;
    listeners.ws.enabled = false;
    listeners.wss.enabled = false;
    shared.listener_store.reset_from_config(&listeners);
    let control = std::sync::Arc::new(crate::native::NativeListenerController::new(
        shared.clone(),
        listeners,
    ));
    shared.listener_store.set_controller(control.clone());
    let row = shared
        .listener_store
        .get("tcp:default")
        .expect("tcp row exists");
    broker_api::v5::listeners::ListenerController::start_listener(&*control, &row)
        .expect("listener starts");
    let mut running = row;
    running.running = true;
    shared.listener_store.update(running);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("client dials");
    client
        .write_all(&connect_v4("denied-over-socket"))
        .await
        .expect("CONNECT sends");
    let connack = tokio::time::timeout(std::time::Duration::from_secs(5), read_packet(&mut client))
        .await
        .expect("CONNACK in time")
        .expect("CONNACK reads");
    assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);

    client
        .write_all(&subscribe_once(7, "denied/topic"))
        .await
        .expect("SUBSCRIBE sends");
    let suback = tokio::time::timeout(std::time::Duration::from_secs(5), read_packet(&mut client))
        .await
        .expect("SUBACK in time")
        .expect("SUBACK reads");
    assert_eq!(suback[0], 0x90);
    assert_eq!(*suback.last().expect("suback has a code"), 0x80u8);
    assert!(shared
        .router
        .matches(&Topic::new("denied/topic").unwrap())
        .is_empty());
}

/// SA-23 items 1 and 2 over a real socket: a narrow rule never covers
/// a wider request. Rule `a/+` denies `a/#`, rule `+` denies `#`,
/// while `a/b` and `x` still subscribe under the covering rules.
#[tokio::test]
async fn sa23_narrow_rule_denies_wider_request_over_socket() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    fn remaining_len(mut len: usize) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let mut byte = (len % 128) as u8;
            len /= 128;
            if len > 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if len == 0 {
                return out;
            }
        }
    }
    fn connect_v4(client_id: &str) -> Vec<u8> {
        let mut body = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3C];
        body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
        body.extend_from_slice(client_id.as_bytes());
        let mut packet = vec![0x10];
        packet.extend_from_slice(&remaining_len(body.len()));
        packet.extend_from_slice(&body);
        packet
    }
    fn subscribe_once(packet_id: u16, filter: &str) -> Vec<u8> {
        let mut body = packet_id.to_be_bytes().to_vec();
        body.extend_from_slice(&(filter.len() as u16).to_be_bytes());
        body.extend_from_slice(filter.as_bytes());
        body.push(0x00);
        let mut packet = vec![0x82];
        packet.extend_from_slice(&remaining_len(body.len()));
        packet.extend_from_slice(&body);
        packet
    }
    async fn read_packet(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
        let mut first = [0u8; 1];
        stream.read_exact(&mut first).await?;
        let mut packet = vec![first[0]];
        let mut multiplier = 1usize;
        let mut remaining = 0usize;
        loop {
            let mut byte = [0u8; 1];
            stream.read_exact(&mut byte).await?;
            packet.push(byte[0]);
            remaining += ((byte[0] & 0x7F) as usize) * multiplier;
            multiplier *= 128;
            if byte[0] & 0x80 == 0 {
                break;
            }
        }
        if remaining > 0 {
            let mut body = vec![0u8; remaining];
            stream.read_exact(&mut body).await?;
            packet.extend_from_slice(&body);
        }
        Ok(packet)
    }
    async fn suback_code(client: &mut TcpStream, packet_id: u16, filter: &str) -> u8 {
        client
            .write_all(&subscribe_once(packet_id, filter))
            .await
            .expect("SUBSCRIBE sends");
        let suback = tokio::time::timeout(std::time::Duration::from_secs(5), read_packet(client))
            .await
            .expect("SUBACK in time")
            .expect("SUBACK reads");
        assert_eq!(suback[0], 0x90);
        *suback.last().expect("suback has a code")
    }

    let shared = test_shared();
    shared
        .auth
        .add_rule(AclRule::new(
            "sa23-cover",
            AclAction::Subscribe,
            "a/+",
            true,
        ))
        .expect("memory-only persist cannot fail");
    shared
        .auth
        .add_rule(AclRule::new("sa23-cover", AclAction::Subscribe, "+", true))
        .expect("memory-only persist cannot fail");
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe binds");
    let port = probe.local_addr().expect("probe addr").port();
    drop(probe);
    let mut listeners = broker_config::schema::ListenersConf::default();
    listeners.tcp.enabled = true;
    listeners.tcp.bind = format!("127.0.0.1:{port}");
    listeners.tls.enabled = false;
    listeners.ws.enabled = false;
    listeners.wss.enabled = false;
    shared.listener_store.reset_from_config(&listeners);
    let control = std::sync::Arc::new(crate::native::NativeListenerController::new(
        shared.clone(),
        listeners,
    ));
    shared.listener_store.set_controller(control.clone());
    let row = shared
        .listener_store
        .get("tcp:default")
        .expect("tcp row exists");
    broker_api::v5::listeners::ListenerController::start_listener(&*control, &row)
        .expect("listener starts");
    let mut running = row;
    running.running = true;
    shared.listener_store.update(running);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("client dials");
    client
        .write_all(&connect_v4("sa23-cover"))
        .await
        .expect("CONNECT sends");
    let connack = tokio::time::timeout(std::time::Duration::from_secs(5), read_packet(&mut client))
        .await
        .expect("CONNACK in time")
        .expect("CONNACK reads");
    assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);

    // `a/+` does not cover `a/#`; `+` does not cover `#`.
    assert_eq!(suback_code(&mut client, 7, "a/#").await, 0x80u8);
    assert_eq!(suback_code(&mut client, 8, "#").await, 0x80u8);
    // The covered requests still subscribe.
    assert_eq!(suback_code(&mut client, 9, "a/b").await, 0x00u8);
    assert_eq!(suback_code(&mut client, 10, "x").await, 0x00u8);
    assert!(!shared
        .router
        .matches(&Topic::new("a/b").unwrap())
        .is_empty());
    assert!(shared
        .router
        .matches(&Topic::new("secret/x").unwrap())
        .is_empty());
}

/// SA-23 item 2 over a real socket: a deny rule applies on overlap.
/// Deny `secret/#` plus allow `#` denies `#` and `secret/x` but
/// still grants `other/x`.
#[tokio::test]
async fn sa23_deny_overlap_blocks_hash_over_socket() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    fn remaining_len(mut len: usize) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let mut byte = (len % 128) as u8;
            len /= 128;
            if len > 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if len == 0 {
                return out;
            }
        }
    }
    fn connect_v4(client_id: &str) -> Vec<u8> {
        let mut body = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3C];
        body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
        body.extend_from_slice(client_id.as_bytes());
        let mut packet = vec![0x10];
        packet.extend_from_slice(&remaining_len(body.len()));
        packet.extend_from_slice(&body);
        packet
    }
    fn subscribe_once(packet_id: u16, filter: &str) -> Vec<u8> {
        let mut body = packet_id.to_be_bytes().to_vec();
        body.extend_from_slice(&(filter.len() as u16).to_be_bytes());
        body.extend_from_slice(filter.as_bytes());
        body.push(0x00);
        let mut packet = vec![0x82];
        packet.extend_from_slice(&remaining_len(body.len()));
        packet.extend_from_slice(&body);
        packet
    }
    async fn read_packet(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
        let mut first = [0u8; 1];
        stream.read_exact(&mut first).await?;
        let mut packet = vec![first[0]];
        let mut multiplier = 1usize;
        let mut remaining = 0usize;
        loop {
            let mut byte = [0u8; 1];
            stream.read_exact(&mut byte).await?;
            packet.push(byte[0]);
            remaining += ((byte[0] & 0x7F) as usize) * multiplier;
            multiplier *= 128;
            if byte[0] & 0x80 == 0 {
                break;
            }
        }
        if remaining > 0 {
            let mut body = vec![0u8; remaining];
            stream.read_exact(&mut body).await?;
            packet.extend_from_slice(&body);
        }
        Ok(packet)
    }
    async fn suback_code(client: &mut TcpStream, packet_id: u16, filter: &str) -> u8 {
        client
            .write_all(&subscribe_once(packet_id, filter))
            .await
            .expect("SUBSCRIBE sends");
        let suback = tokio::time::timeout(std::time::Duration::from_secs(5), read_packet(client))
            .await
            .expect("SUBACK in time")
            .expect("SUBACK reads");
        assert_eq!(suback[0], 0x90);
        *suback.last().expect("suback has a code")
    }

    let shared = test_shared();
    shared
        .auth
        .add_rule(AclRule::new(
            "sa23-deny",
            AclAction::Subscribe,
            "secret/#",
            false,
        ))
        .expect("memory-only persist cannot fail");
    shared
        .auth
        .add_rule(AclRule::new("sa23-deny", AclAction::Subscribe, "#", true))
        .expect("memory-only persist cannot fail");
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe binds");
    let port = probe.local_addr().expect("probe addr").port();
    drop(probe);
    let mut listeners = broker_config::schema::ListenersConf::default();
    listeners.tcp.enabled = true;
    listeners.tcp.bind = format!("127.0.0.1:{port}");
    listeners.tls.enabled = false;
    listeners.ws.enabled = false;
    listeners.wss.enabled = false;
    shared.listener_store.reset_from_config(&listeners);
    let control = std::sync::Arc::new(crate::native::NativeListenerController::new(
        shared.clone(),
        listeners,
    ));
    shared.listener_store.set_controller(control.clone());
    let row = shared
        .listener_store
        .get("tcp:default")
        .expect("tcp row exists");
    broker_api::v5::listeners::ListenerController::start_listener(&*control, &row)
        .expect("listener starts");
    let mut running = row;
    running.running = true;
    shared.listener_store.update(running);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut client = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("client dials");
    client
        .write_all(&connect_v4("sa23-deny"))
        .await
        .expect("CONNECT sends");
    let connack = tokio::time::timeout(std::time::Duration::from_secs(5), read_packet(&mut client))
        .await
        .expect("CONNACK in time")
        .expect("CONNACK reads");
    assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);

    // `#` reaches `secret/...`, so the deny overlaps and denies.
    assert_eq!(suback_code(&mut client, 7, "#").await, 0x80u8);
    assert_eq!(suback_code(&mut client, 8, "secret/x").await, 0x80u8);
    // Outside the denied branch the later allow still grants.
    assert_eq!(suback_code(&mut client, 9, "other/x").await, 0x00u8);
    assert!(!shared
        .router
        .matches(&Topic::new("other/x").unwrap())
        .is_empty());
    assert!(shared
        .router
        .matches(&Topic::new("secret/x").unwrap())
        .is_empty());
}

#[tokio::test]
async fn publish_denied_yields_puback_0x87_and_drops() {
    let shared = test_shared();
    // A live listener proves the drop: it would receive otherwise.
    let (listener, _) = shared.sessions.get_or_create("listener", true);
    *listener.conn_id.write() = Some(93);
    let sub = subscribe_frame(93, 1, encode_subscribe_meta(1, "listener", &[("t", 0)]));
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");

    let (muted, _) = shared.sessions.get_or_create("muted", true);
    *muted.conn_id.write() = Some(92);
    shared
        .auth
        .add_rule(AclRule::new("muted", AclAction::Publish, "#", false))
        .expect("memory-only persist cannot fail");

    let (meta, payload) = encode_publish_meta("t", 5, 1, false, b"shh");
    let (ack, deliveries) = apply_publish(&publish_frame(92, 1, meta, payload), &shared).await;
    let ack = ack.expect("QoS 1 denied publish still gets a PubAck");
    assert_eq!(&ack.metadata[..], &[0x00, 0x05, 0x87u8]);
    assert!(deliveries.is_empty(), "denied publish must not fan out");
}

/// A denied QoS 2 publish reads the version-correct PUBREC from a real
/// socket. Version 5 carries reason 0x87 (Not authorized). Version
/// 3.1.1 has no PUBREC reason code, so the short form is correct.
#[tokio::test]
async fn denied_qos2_publish_pubrec_reason_matches_version_over_socket() {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    fn remaining_len(mut len: usize) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let mut byte = (len % 128) as u8;
            len /= 128;
            if len > 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if len == 0 {
                return out;
            }
        }
    }
    fn connect(level: u8, client_id: &str) -> Vec<u8> {
        let mut body = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', level, 0x02, 0x00, 0x3C];
        if level == 5 {
            body.push(0x00);
        }
        body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
        body.extend_from_slice(client_id.as_bytes());
        let mut packet = vec![0x10];
        packet.extend_from_slice(&remaining_len(body.len()));
        packet.extend_from_slice(&body);
        packet
    }
    fn publish_qos2(level: u8, topic: &str, packet_id: u16) -> Vec<u8> {
        let mut body = (topic.len() as u16).to_be_bytes().to_vec();
        body.extend_from_slice(topic.as_bytes());
        body.extend_from_slice(&packet_id.to_be_bytes());
        if level == 5 {
            body.push(0x00);
        }
        let mut packet = vec![0x34];
        packet.extend_from_slice(&remaining_len(body.len()));
        packet.extend_from_slice(&body);
        packet
    }
    async fn read_packet(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
        let mut first = [0u8; 1];
        stream.read_exact(&mut first).await?;
        let mut packet = vec![first[0]];
        let mut multiplier = 1usize;
        let mut remaining = 0usize;
        loop {
            let mut byte = [0u8; 1];
            stream.read_exact(&mut byte).await?;
            packet.push(byte[0]);
            remaining += ((byte[0] & 0x7F) as usize) * multiplier;
            multiplier *= 128;
            if byte[0] & 0x80 == 0 {
                break;
            }
        }
        if remaining > 0 {
            let mut body = vec![0u8; remaining];
            stream.read_exact(&mut body).await?;
            packet.extend_from_slice(&body);
        }
        Ok(packet)
    }

    let shared = test_shared();
    shared
        .auth
        .add_rule(AclRule::new("*", AclAction::Publish, "#", false))
        .expect("memory-only persist cannot fail");
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe binds");
    let port = probe.local_addr().expect("probe addr").port();
    drop(probe);
    let mut listeners = broker_config::schema::ListenersConf::default();
    listeners.tcp.enabled = true;
    listeners.tcp.bind = format!("127.0.0.1:{port}");
    listeners.tls.enabled = false;
    listeners.ws.enabled = false;
    listeners.wss.enabled = false;
    shared.listener_store.reset_from_config(&listeners);
    let control = std::sync::Arc::new(crate::native::NativeListenerController::new(
        shared.clone(),
        listeners,
    ));
    shared.listener_store.set_controller(control.clone());
    let row = shared
        .listener_store
        .get("tcp:default")
        .expect("tcp row exists");
    broker_api::v5::listeners::ListenerController::start_listener(&*control, &row)
        .expect("listener starts");
    let mut running = row;
    running.running = true;
    shared.listener_store.update(running);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    for (level, client_id, packet_id, expected) in [
        (
            4u8,
            "denied-qos2-v4",
            0x11u16,
            vec![0x50u8, 0x02, 0x00, 0x11],
        ),
        (
            5u8,
            "denied-qos2-v5",
            0x12u16,
            vec![0x50u8, 0x03, 0x00, 0x12, 0x87],
        ),
    ] {
        let mut client = TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("client dials");
        client
            .write_all(&connect(level, client_id))
            .await
            .expect("CONNECT sends");
        let connack =
            tokio::time::timeout(std::time::Duration::from_secs(5), read_packet(&mut client))
                .await
                .expect("CONNACK in time")
                .expect("CONNACK reads");
        assert_eq!(connack[0], 0x20, "CONNACK for protocol level {level}");
        client
            .write_all(&publish_qos2(level, "denied/topic", packet_id))
            .await
            .expect("PUBLISH sends");
        let pubrec =
            tokio::time::timeout(std::time::Duration::from_secs(5), read_packet(&mut client))
                .await
                .expect("PUBREC in time")
                .expect("PUBREC reads");
        assert_eq!(pubrec, expected, "PUBREC for protocol level {level}");
    }
}

/// Write the authorization settings through the shared store: the
/// same store the management API writes and the authorize path
/// reads, so each test drives the production write entry.
#[cfg(test)]
fn authz_test_settings(shared: &Shared, no_match: &str, deny_action: &str) {
    shared
        .authz_settings
        .replace(broker_config::AuthzSettingsConf {
            no_match: no_match.to_string(),
            deny_action: deny_action.to_string(),
            ..broker_config::AuthzSettingsConf::default()
        })
        .expect("valid settings replace");
}

/// Bind one client id on one connection and register its close
/// channel. Drives the bind through the broker like the edge does.
/// The caller observes `ConnClose` frames on the returned receiver.
#[cfg(test)]
fn authz_test_bind(
    shared: &Shared,
    conn_id: u64,
    client_id: &str,
) -> tokio::sync::mpsc::UnboundedReceiver<BrokerFrame> {
    let bind = bind_frame(conn_id, 1, encode_bind_meta(client_id, true, 60));
    reply_for_frame(&bind, &shared.sessions).expect("bind replies");
    let (tx, rx) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(conn_id, tx);
    rx
}

/// Add one allow rule that never matches the `authz/#` probe tree,
/// so every probe verdict comes from `no_match`.
#[cfg(test)]
fn authz_test_unmatched_rule(shared: &Shared) {
    shared
        .auth
        .add_rule(AclRule::new(
            "authz-pub",
            AclAction::Publish,
            "other/#",
            true,
        ))
        .expect("memory-only persist cannot fail");
}

#[tokio::test]
async fn authz_no_match_allow_ignore_delivers_unmatched() {
    // AP-11 combo 1 (`allow`, `ignore`): one non-matching rule, an
    // unmatched publish is delivered and the connection stays up.
    // Drives bind, subscribe, publish and delivery through the
    // broker, never the store alone.
    let shared = test_shared();
    let mut rx_pub = authz_test_bind(&shared, 201, "authz-pub");
    authz_test_bind(&shared, 202, "authz-sub");
    authz_test_unmatched_rule(&shared);
    authz_test_settings(&shared, "allow", "ignore");

    let sub = subscribe_frame(
        202,
        1,
        encode_subscribe_meta(1, "authz-sub", &[("authz/#", 0)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    let reply = reply.expect("subscribe replies");
    assert_eq!(&reply.metadata[2..], &[0u8]);

    let (meta, payload) = encode_publish_meta("authz/data", 5, 1, false, b"hello");
    let (ack, deliveries) = apply_publish(&publish_frame(201, 1, meta, payload), &shared).await;
    let ack = ack.expect("QoS 1 publish gets a PubAck");
    assert_eq!(&ack.metadata[..], &[0x00, 0x05, 0x00u8]);
    assert_eq!(deliveries.len(), 1, "unmatched publish is delivered");
    assert!(rx_pub.try_recv().is_err(), "an allow never disconnects");
}

#[tokio::test]
async fn authz_no_match_deny_ignore_drops_without_disconnect() {
    // AP-11 combo 2 (`deny`, `ignore`): one non-matching rule, an
    // unmatched publish and subscribe are refused and both
    // connections stay up. Drives bind, subscribe and publish
    // through the broker, never the store alone.
    let shared = test_shared();
    let mut rx_pub = authz_test_bind(&shared, 211, "authz-pub");
    authz_test_bind(&shared, 212, "authz-sub");
    authz_test_unmatched_rule(&shared);
    authz_test_settings(&shared, "deny", "ignore");

    let sub = subscribe_frame(
        212,
        1,
        encode_subscribe_meta(1, "authz-sub", &[("authz/#", 0)]),
    );
    let (reply, retained) = apply_subscribe(&sub, &shared).await;
    let reply = reply.expect("subscribe replies");
    // A version-4 denial answers 0x80; only the wire version 5
    // carries 0x87.
    assert_eq!(&reply.metadata[2..], &[0x80u8]);
    assert!(retained.is_empty());

    let (meta, payload) = encode_publish_meta("authz/data", 5, 1, false, b"shh");
    let (ack, deliveries) = apply_publish(&publish_frame(211, 1, meta, payload), &shared).await;
    let ack = ack.expect("QoS 1 denied publish still gets a PubAck");
    assert_eq!(&ack.metadata[..], &[0x00, 0x05, 0x87u8]);
    assert!(deliveries.is_empty(), "denied publish must not fan out");
    assert!(rx_pub.try_recv().is_err(), "an ignore never disconnects");
}

#[tokio::test]
async fn authz_no_match_allow_disconnect_delivers_without_close() {
    // AP-11 combo 3 (`allow`, `disconnect`): one non-matching rule,
    // an unmatched publish is delivered and the connection stays up:
    // only a denial disconnects. Drives bind, subscribe, publish and
    // delivery through the broker, never the store alone.
    let shared = test_shared();
    let mut rx_pub = authz_test_bind(&shared, 221, "authz-pub");
    authz_test_bind(&shared, 222, "authz-sub");
    authz_test_unmatched_rule(&shared);
    authz_test_settings(&shared, "allow", "disconnect");

    let sub = subscribe_frame(
        222,
        1,
        encode_subscribe_meta(1, "authz-sub", &[("authz/#", 0)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    let reply = reply.expect("subscribe replies");
    assert_eq!(&reply.metadata[2..], &[0u8]);

    let (meta, payload) = encode_publish_meta("authz/data", 5, 1, false, b"hello");
    let (ack, deliveries) = apply_publish(&publish_frame(221, 1, meta, payload), &shared).await;
    let ack = ack.expect("QoS 1 publish gets a PubAck");
    assert_eq!(&ack.metadata[..], &[0x00, 0x05, 0x00u8]);
    assert_eq!(deliveries.len(), 1, "unmatched publish is delivered");
    assert!(rx_pub.try_recv().is_err(), "an allow never disconnects");
}

#[tokio::test]
async fn authz_deny_action_disconnect_closes_denied() {
    // AP-11 combo 4 (`deny`, `disconnect`): one non-matching rule, a
    // denied publish and a denied subscribe each close the connection
    // with 0x87 after their ack. Drives bind, subscribe and publish
    // through the broker, never the store alone.
    let shared = test_shared();
    let mut rx_pub = authz_test_bind(&shared, 231, "authz-pub");
    let mut rx_sub = authz_test_bind(&shared, 232, "authz-sub");
    authz_test_unmatched_rule(&shared);
    authz_test_settings(&shared, "deny", "disconnect");

    let (meta, payload) = encode_publish_meta("authz/data", 5, 1, false, b"shh");
    let (ack, deliveries) = apply_publish(&publish_frame(231, 1, meta, payload), &shared).await;
    let ack = ack.expect("QoS 1 denied publish still gets a PubAck");
    assert_eq!(&ack.metadata[..], &[0x00, 0x05, 0x87u8]);
    assert!(deliveries.is_empty(), "denied publish must not fan out");
    let close = rx_pub.try_recv().expect("denied publish disconnects");
    assert_eq!(close.header.opcode, OpCode::ConnClose);
    assert_eq!(close.header.conn_id, 231);
    assert_eq!(&close.metadata[..], &[0x87u8]);
    assert!(
        shared.sessions.client_id_for_conn(231).is_none(),
        "denied connection is detached"
    );

    let sub = subscribe_frame(
        232,
        1,
        encode_subscribe_meta(1, "authz-sub", &[("authz/#", 0)]),
    );
    let (reply, retained) = apply_subscribe(&sub, &shared).await;
    let reply = reply.expect("subscribe replies");
    // The refused version-4 SubAck reads 0x80. The close that
    // follows still carries 0x87 for the disconnect.
    assert_eq!(&reply.metadata[2..], &[0x80u8]);
    assert!(retained.is_empty());
    let close = rx_sub.try_recv().expect("denied subscribe disconnects");
    assert_eq!(close.header.opcode, OpCode::ConnClose);
    assert_eq!(close.header.conn_id, 232);
    assert_eq!(&close.metadata[..], &[0x87u8]);
    assert!(
        shared.sessions.client_id_for_conn(232).is_none(),
        "denied connection is detached"
    );
}

#[tokio::test]
async fn publish_from_a_connection_with_no_session_is_dropped_and_closed() {
    // A PUBLISH frame from a connection with no bound session routes
    // nothing and orders the close of that connection, with and
    // without rules. Drives bind, subscribe and publish through the
    // broker.
    let shared = test_shared();
    authz_test_bind(&shared, 241, "nb-sub");
    let (tx, mut rx_unbound) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(242, tx);
    let sub = subscribe_frame(241, 1, encode_subscribe_meta(1, "nb-sub", &[("nb/#", 0)]));
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    assert_eq!(&reply.expect("subscribe replies").metadata[2..], &[0u8]);

    let (meta, payload) = encode_publish_meta("nb/data", 5, 1, false, b"x");
    let (ack, deliveries) = apply_publish(&publish_frame(242, 1, meta, payload), &shared).await;
    assert!(ack.is_none(), "no ack for an unbound connection");
    assert!(deliveries.is_empty(), "nothing routes without a session");
    let close = rx_unbound.try_recv().expect("the connection is closed");
    assert_eq!(close.header.opcode, OpCode::ConnClose);
    assert_eq!(close.header.conn_id, 242);
}
