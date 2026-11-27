use super::*;
use crate::link_harness::handle_connection;
use tokio::net::TcpListener;

#[cfg(test)]
fn ban_entry(as_type: &str, who: &str, until: &str) -> broker_api::v5::banned::BanEntry {
    broker_api::v5::banned::BanEntry {
        as_type: as_type.to_string(),
        who: who.to_string(),
        by: "test".to_string(),
        reason: "test".to_string(),
        at: "2026-01-01T00:00:00+00:00".to_string(),
        until: until.to_string(),
        tenant: broker_session::tenant::DEFAULT_TENANT_ID.to_string(),
    }
}

#[tokio::test]
async fn ban_refuses_banned_client_id_on_connect() {
    let shared = test_shared();
    shared
        .bans
        .insert(ban_entry("clientid", "banned-1", "infinity"))
        .expect("ban insert");

    let frame = bind_frame(91, 1, encode_bind_meta("banned-1", true, 60));
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    assert_eq!(reply.header.opcode, OpCode::SessionBinding);
    let (_, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x8A, "banned client id must yield return code 0x8A");
    assert!(!present);
    assert!(shared.sessions.get("banned-1").is_none());

    // An unbanned id still connects against the same store.
    let frame = bind_frame(92, 1, encode_bind_meta("fine-1", true, 60));
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
}

#[tokio::test]
async fn ban_refuses_banned_username_on_connect() {
    let shared = test_shared();
    shared
        .auth
        .add_user("banned-user", b"pw")
        .expect("memory-only persist cannot fail");
    shared
        .auth
        .add_user("fine-user", b"pw2")
        .expect("memory-only persist cannot fail");
    shared
        .bans
        .insert(ban_entry("username", "banned-user", "infinity"))
        .expect("ban insert");

    // Valid credentials do not save a banned username.
    let frame = bind_frame(
        91,
        1,
        encode_bind_meta_creds("dev-u", true, 60, "banned-user", b"pw"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x8A, "banned username must yield return code 0x8A");
    assert!(!present);
    assert!(shared.sessions.get("dev-u").is_none());

    // The same client id under an unbanned username connects.
    let frame = bind_frame(
        92,
        1,
        encode_bind_meta_creds("dev-u", true, 60, "fine-user", b"pw2"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
}

#[tokio::test]
async fn ban_refuses_banned_peerhost_on_connect() {
    let shared = test_shared();
    shared
        .bans
        .insert(ban_entry("peerhost", "192.0.2.44", "infinity"))
        .expect("ban insert");
    shared
        .bans
        .insert(ban_entry("peerhost_net", "203.0.113.0/24", "infinity"))
        .expect("ban insert");

    // Exact address ban refuses.
    let meta = encode_bind_meta_peer(encode_bind_meta("ip-client", true, 60), "192.0.2.44");
    let reply = apply_bind(&bind_frame(91, 1, meta), &shared)
        .await
        .expect("bind replies");
    let (_, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x8A, "banned peer address must yield return code 0x8A");
    assert!(!present);
    assert!(shared.sessions.get("ip-client").is_none());

    // CIDR ban refuses an address inside the network.
    let meta = encode_bind_meta_peer(encode_bind_meta("net-client", true, 60), "203.0.113.9");
    let reply = apply_bind(&bind_frame(92, 1, meta), &shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x8A, "CIDR-banned peer must yield return code 0x8A");

    // The same id from an unbanned address connects.
    let meta = encode_bind_meta_peer(encode_bind_meta("ip-client", true, 60), "192.0.2.45");
    let reply = apply_bind(&bind_frame(93, 1, meta), &shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
}

#[tokio::test]
async fn ban_refuses_banned_peerhost_with_alias_on_connect() {
    // FX-01: the edge always appends the B4-05 alias section (0 for
    // 3.1.1), so every bind below carries peer plus alias exactly as
    // `indra_brokerlink:encode_bind_meta/6` sends it. Drives the
    // CONNECT event through the broker, never the store alone.
    let shared = test_shared();
    shared
        .bans
        .insert(ban_entry("peerhost", "127.0.0.1", "infinity"))
        .expect("ban insert");
    shared
        .bans
        .insert(ban_entry("peerhost_net", "198.51.100.0/24", "infinity"))
        .expect("ban insert");

    // Exact address ban refuses an anonymous client from that address.
    let meta = encode_bind_meta_alias(
        encode_bind_meta_peer(encode_bind_meta("edge-client", true, 60), "127.0.0.1"),
        0,
    );
    let reply = apply_bind(&bind_frame(91, 1, meta), &shared)
        .await
        .expect("bind replies");
    let (_, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x8A, "banned peer address must yield return code 0x8A");
    assert!(!present);
    assert!(shared.sessions.get("edge-client").is_none());

    // CIDR ban refuses an address inside the network.
    let meta = encode_bind_meta_alias(
        encode_bind_meta_peer(encode_bind_meta("net-client", true, 60), "198.51.100.9"),
        0,
    );
    let reply = apply_bind(&bind_frame(92, 1, meta), &shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x8A, "CIDR-banned peer must yield return code 0x8A");

    // A peer-host ban on a different address does not refuse it.
    let meta = encode_bind_meta_alias(
        encode_bind_meta_peer(encode_bind_meta("edge-client", true, 60), "192.0.2.45"),
        0,
    );
    let reply = apply_bind(&bind_frame(93, 1, meta), &shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);

    // Once the ban is removed the same client connects.
    assert!(shared.bans.remove("peerhost", "127.0.0.1"));
    let meta = encode_bind_meta_alias(
        encode_bind_meta_peer(encode_bind_meta("edge-client", true, 60), "127.0.0.1"),
        0,
    );
    let reply = apply_bind(&bind_frame(94, 1, meta), &shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
}

#[tokio::test]
async fn ban_refuses_banned_peerhost_over_tcp() {
    // Same CONNECT event as above but over a real BrokerLink transport,
    // the way the edge reaches the kernel: a banned peer is refused
    // with 0x8A and creates no session.
    let shared = test_shared();
    shared
        .bans
        .insert(ban_entry("peerhost", "127.0.0.1", "infinity"))
        .expect("ban insert");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        handle_connection(stream, shared).await.expect("handle");
    });

    let io = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let client = FramedTransport::new(io);
    let meta = encode_bind_meta_alias(
        encode_bind_meta_peer(encode_bind_meta("tcp-client", true, 60), "127.0.0.1"),
        0,
    );
    client.send(bind_frame(95, 1, meta)).await.expect("bind");
    let reply = client.recv().await.expect("recv binding");
    assert_eq!(reply.header.opcode, OpCode::SessionBinding);
    let (_, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x8A);
    assert!(!present);

    server.abort();
}

#[tokio::test]
async fn ban_mid_session_publish_is_refused() {
    let shared = test_shared();
    // A live listener proves the drop: it would receive otherwise.
    let (listener, _) = shared.sessions.get_or_create("ban-listener", true);
    *listener.conn_id.write() = Some(93);
    let sub = subscribe_frame(
        93,
        1,
        encode_subscribe_meta(1, "ban-listener", &[("t/ban", 0)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");

    // The publisher connects cleanly before any ban exists.
    let frame = bind_frame(92, 1, encode_bind_meta("ban-pub", true, 60));
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);

    // Served before the ban: QoS 1 ack 0 and one delivery.
    let (meta, payload) = encode_publish_meta("t/ban", 5, 1, false, b"before");
    let (ack, deliveries) = apply_publish(&publish_frame(92, 2, meta, payload), &shared).await;
    let ack = ack.expect("QoS 1 publish gets a PubAck");
    assert_eq!(&ack.metadata[..], &[0x00, 0x05, 0x00]);
    assert_eq!(deliveries.len(), 1);

    // The ban lands mid-session: the next publish is refused exactly
    // like an ACL denial (PubAck 0x87, no fan-out).
    shared
        .bans
        .insert(ban_entry("clientid", "ban-pub", "infinity"))
        .expect("ban insert");
    let (meta, payload) = encode_publish_meta("t/ban", 6, 1, false, b"after");
    let (ack, deliveries) = apply_publish(&publish_frame(92, 3, meta, payload), &shared).await;
    let ack = ack.expect("QoS 1 denied publish still gets a PubAck");
    assert_eq!(&ack.metadata[..], &[0x00, 0x06, 0x87]);
    assert!(deliveries.is_empty(), "banned publish must not fan out");
}

#[tokio::test]
async fn expired_ban_refuses_nobody() {
    let shared = test_shared();
    let past = "2000-01-01T00:00:00+00:00";
    for (as_type, who) in [
        ("clientid", "old-id"),
        ("username", "old-user"),
        ("peerhost", "192.0.2.99"),
    ] {
        shared
            .bans
            .insert(ban_entry(as_type, who, past))
            .expect("ban insert");
    }

    // All three identities at once on one connect: still accepted.
    let meta = encode_bind_meta_peer(
        encode_bind_meta_creds("old-id", true, 60, "old-user", b"pw"),
        "192.0.2.99",
    );
    let reply = apply_bind(&bind_frame(91, 1, meta), &shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0, "expired bans must not refuse the connect");

    // And the publish is served, not refused.
    let (meta, payload) = encode_publish_meta("t/old", 7, 1, false, b"x");
    let (ack, _) = apply_publish(&publish_frame(91, 2, meta, payload), &shared).await;
    let ack = ack.expect("QoS 1 publish gets a PubAck");
    assert_eq!(&ack.metadata[..], &[0x00, 0x07, 0x00]);
}
