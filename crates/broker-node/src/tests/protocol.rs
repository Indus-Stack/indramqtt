use super::*;
use crate::link_harness::handle_connection;
use tokio::net::TcpListener;

/// Decode the X1-02 v5 section of a `SessionBinding` reply, if
/// present (`None` for 10/12-byte bindings).
#[cfg(test)]
fn decode_session_binding_v5(meta: &[u8]) -> Option<crate::SessionBindingV5> {
    let (_, _, _, _, v5) =
        crate::decode_session_binding_full(meta).expect("SessionBinding meta must decode");
    v5
}

/// Encode `PublishMeta` with X1-03 v5 properties (test mirror of
/// the extended BEAM `encode_publish_meta_v5/7` contract).
#[allow(clippy::too_many_arguments)]
#[cfg(test)]
fn encode_publish_meta_v5(
    topic: &str,
    packet_id: u16,
    qos: u8,
    retain: bool,
    alias: u16,
    format: u8,
    expiry: u32,
    users: &[(&str, &str)],
    payload: &[u8],
) -> (Bytes, Bytes) {
    let mut meta = Vec::new();
    meta.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    meta.extend_from_slice(topic.as_bytes());
    meta.extend_from_slice(&packet_id.to_be_bytes());
    meta.push(qos);
    meta.push(u8::from(retain));
    meta.push(0u8);
    meta.extend_from_slice(&protocol_v5::encode_topic_alias(alias));
    meta.push(format);
    meta.extend_from_slice(&expiry.to_be_bytes());
    meta.extend_from_slice(&(users.len() as u16).to_be_bytes());
    for (k, v) in users {
        meta.extend_from_slice(&(k.len() as u16).to_be_bytes());
        meta.extend_from_slice(k.as_bytes());
        meta.extend_from_slice(&(v.len() as u16).to_be_bytes());
        meta.extend_from_slice(v.as_bytes());
    }
    (Bytes::from(meta), Bytes::from(payload.to_vec()))
}

/// Encode `UnsubscribeMeta` for tests. Mirrors the native path layout.
#[cfg(test)]
fn encode_unsubscribe_meta(packet_id: u16, client_id: &str, filters: &[&str]) -> Bytes {
    let id = client_id.as_bytes();
    let mut meta = Vec::new();
    meta.extend_from_slice(&packet_id.to_be_bytes());
    meta.extend_from_slice(&(id.len() as u16).to_be_bytes());
    meta.extend_from_slice(id);
    meta.extend_from_slice(&(filters.len() as u16).to_be_bytes());
    for filter in filters {
        meta.extend_from_slice(&(filter.len() as u16).to_be_bytes());
        meta.extend_from_slice(filter.as_bytes());
    }
    Bytes::from(meta)
}

#[cfg(test)]
fn unsubscribe_frame(conn_id: u64, seq: u64, meta: Bytes) -> BrokerFrame {
    BrokerFrame::new(OpCode::UnsubscribeIn, conn_id, seq, meta, Bytes::new())
        .expect("valid unsubscribe frame")
}

#[test]
fn downlink_detached_clean_session_counts_drop_only() {
    // PERF-10: one frame for a detached clean session must bump
    // exactly the detached-clean counter and queue nothing.
    let router = Router::new();
    let sessions = SessionManager::new();
    let metrics = Metrics::new();
    let filter = TopicFilter::new("sensors/temp").unwrap();
    router.subscribe(
        &filter,
        Subscription::new("ghost-clean", 77, QoS::AtMostOnce),
    );
    let (session, _) = sessions.get_or_create("ghost-clean", true);
    *session.connected.write() = false;
    *session.conn_id.write() = None;

    let topic = Topic::new("sensors/temp").unwrap();
    let deliveries = build_downlink_frames(
        &router,
        &sessions,
        &metrics,
        &topic,
        QoS::AtMostOnce,
        false,
        &Bytes::from_static(b"21.5"),
        broker_session::tenant::DEFAULT_TENANT_ID,
        None,
    );

    assert!(deliveries.is_empty());
    assert_eq!(session.offline_len(), 0);
    assert_eq!(metrics.detached_clean_dropped(), 1);
    assert_eq!(metrics.unknown_conn_dropped(), 0);
    assert_eq!(metrics.dead_mailbox_dropped(), 0);
    assert_eq!(metrics.offline_queue_evicted(), 0);
}

#[test]
fn downlink_offline_eviction_counts_drop_only() {
    // PERF-10: pushing into a full offline queue keeps the queueing
    // itself unchanged (oldest evicted, length capped) and bumps
    // exactly the eviction counter. Uses an explicit 1024 cap so the
    // seeding via `push_offline` (capped at `MAX_OFFLINE_QUEUE`) and
    // the delivery path via the manager cap agree.
    let router = Router::new();
    let sessions = SessionManager::new_with_limits(Some(MAX_OFFLINE_QUEUE));
    let metrics = Metrics::new();
    let filter = TopicFilter::new("jobs/backlog").unwrap();
    router.subscribe(&filter, Subscription::new("durable-1", 78, QoS::AtMostOnce));
    let (session, _) = sessions.get_or_create("durable-1", false);
    *session.connected.write() = false;
    *session.conn_id.write() = None;
    for i in 0..MAX_OFFLINE_QUEUE {
        session.push_offline(QueuedMessage {
            topic: Topic::new("jobs/backlog").unwrap(),
            qos: QoS::AtMostOnce,
            retain: false,
            payload: Bytes::from(format!("seed-{i}")),
            publish_at_ms: None,
        });
    }
    assert_eq!(session.offline_len(), MAX_OFFLINE_QUEUE);

    let topic = Topic::new("jobs/backlog").unwrap();
    let deliveries = build_downlink_frames(
        &router,
        &sessions,
        &metrics,
        &topic,
        QoS::AtMostOnce,
        false,
        &Bytes::from_static(b"new"),
        broker_session::tenant::DEFAULT_TENANT_ID,
        None,
    );

    assert!(deliveries.is_empty());
    assert_eq!(session.offline_len(), MAX_OFFLINE_QUEUE);
    assert_eq!(metrics.offline_queue_evicted(), 1);
    assert_eq!(metrics.unknown_conn_dropped(), 0);
    assert_eq!(metrics.dead_mailbox_dropped(), 0);
    assert_eq!(metrics.detached_clean_dropped(), 0);
}

#[test]
fn ping_maps_to_matching_pong() {
    let sessions = SessionManager::new();
    let ping = BrokerFrame::ping(1234, 56);
    let reply = reply_for_frame(&ping, &sessions).expect("Ping must produce a reply");
    assert_eq!(reply.header.opcode, OpCode::Pong);
    assert_eq!(reply.header.conn_id, 1234);
    assert_eq!(reply.header.sequence_no, 56);
    assert!(reply.metadata.is_empty());
    assert!(reply.payload.is_empty());
}

#[test]
fn ping_pong_preserves_max_ids() {
    let sessions = SessionManager::new();
    let ping = BrokerFrame::ping(u64::MAX, u64::MAX);
    let reply = reply_for_frame(&ping, &sessions).expect("Ping must produce a reply");
    assert_eq!(reply.header.conn_id, u64::MAX);
    assert_eq!(reply.header.sequence_no, u64::MAX);
}

#[test]
fn non_handshake_frames_have_no_reply() {
    let sessions = SessionManager::new();
    for opcode in [
        OpCode::Pong,
        OpCode::SessionBinding,
        OpCode::UnbindConnection,
        OpCode::PublishIn,
        OpCode::PublishOut,
        OpCode::SubscribeIn,
        OpCode::DisconnectIn,
    ] {
        let frame = BrokerFrame::new(
            opcode,
            7,
            9,
            Bytes::from_static(b"meta"),
            Bytes::from_static(b"payload"),
        )
        .expect("valid frame");
        assert!(
            reply_for_frame(&frame, &sessions).is_none(),
            "opcode {:?} must not produce a sync reply",
            opcode
        );
    }
}

/// Encode `BindConnection` metadata with an X1-01 v5 section
/// (test mirror of the BEAM `encode_bind_meta/9` contract):
/// `AliasMax:16be | V5Payload | V5Len:16be` with Flags bit 2 set,
/// where `V5Payload` is `Version:8 | Expiry:32be | RecvMax:16be |
/// MaxPkt:32be | Count:16be | pairs`.
/// Parameters ride one struct so the helper stays under the
/// seven-argument clippy bound; one value per bind, never partial.
#[cfg(test)]
struct V5BindParams<'a> {
    client_id: &'a str,
    clean_start: bool,
    keepalive: u16,
    alias_max: u16,
    session_expiry: u32,
    receive_max: u16,
    max_packet_size: u32,
    user_properties: &'a [(&'a str, &'a str)],
}

#[cfg(test)]
fn encode_bind_meta_v5(params: V5BindParams<'_>) -> Bytes {
    let V5BindParams {
        client_id,
        clean_start,
        keepalive,
        alias_max,
        session_expiry,
        receive_max,
        max_packet_size,
        user_properties,
    } = params;
    assert!(user_properties.len() <= MAX_V5_BIND_USERS);
    let id = client_id.as_bytes();
    let mut meta = Vec::new();
    meta.extend_from_slice(&(id.len() as u16).to_be_bytes());
    meta.extend_from_slice(id);
    // clean_start plus Flags bit 2 (v5 section present), no will.
    meta.push(u8::from(clean_start) | 0x04);
    meta.extend_from_slice(&keepalive.to_be_bytes());
    meta.extend_from_slice(&alias_max.to_be_bytes());
    let mut payload = Vec::new();
    payload.push(5u8);
    payload.extend_from_slice(&session_expiry.to_be_bytes());
    payload.extend_from_slice(&receive_max.to_be_bytes());
    payload.extend_from_slice(&max_packet_size.to_be_bytes());
    payload.extend_from_slice(&(user_properties.len() as u16).to_be_bytes());
    for (key, value) in user_properties {
        payload.extend_from_slice(&(key.len() as u16).to_be_bytes());
        payload.extend_from_slice(key.as_bytes());
        payload.extend_from_slice(&(value.len() as u16).to_be_bytes());
        payload.extend_from_slice(value.as_bytes());
    }
    meta.extend_from_slice(&payload);
    meta.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    Bytes::from(meta)
}

#[test]
fn bind_connection_v5_section_decodes_and_replies() {
    // Through the broker: a version-5 bind reaches the session and
    // the transported properties decode; the reply stays accepted.
    let meta = encode_bind_meta_v5(V5BindParams {
        client_id: "device-v5",
        clean_start: true,
        keepalive: 60,
        alias_max: 10,
        session_expiry: 3600,
        receive_max: 100,
        max_packet_size: 65536,
        user_properties: &[("k", "v")],
    });
    let req = decode_bind_meta(&meta).expect("v5 bind decodes");
    assert_eq!(req.protocol_version, 5);
    assert_eq!(req.session_expiry, 3600);
    assert_eq!(req.receive_maximum, 100);
    assert_eq!(req.max_packet_size, 65536);
    assert_eq!(req.client_alias_max, 10);
    assert_eq!(
        req.user_properties
            .as_ref()
            .and_then(|m| m.get("k"))
            .map(String::as_str),
        Some("v")
    );

    let sessions = SessionManager::new();
    let frame = bind_frame(21, 1, meta);
    let reply = reply_for_frame(&frame, &sessions).expect("v5 bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0, "accepted v5 bind must carry return code 0");
    // X1-02: the accepted v5 reply carries the extended section
    // (granted expiry, server limits) with no reason string and no
    // echoed user properties; the session stores the CONNECT caps.
    let v5 = decode_session_binding_v5(&reply.metadata).expect("v5 section present");
    assert_eq!(v5.granted_expiry, 3600);
    assert_eq!(v5.server_recv_max, SERVER_V5_RECEIVE_MAXIMUM);
    assert_eq!(v5.server_max_pkt, sessions.max_packet_size());
    assert!(v5.reason.is_empty(), "success carries no reason string");
    assert!(
        v5.user_properties.is_empty(),
        "success echoes no user properties"
    );
    let session = sessions.get("device-v5").expect("session stored");
    assert_eq!(session.protocol_version(), 5);
    assert_eq!(session.session_expiry(), 3600);
    assert_eq!(session.client_receive_maximum(), 100);
    assert_eq!(session.client_max_packet_size(), 65536);
    assert_eq!(session.outbound_alias_max(), 10);

    // Version-4 binds still decode with defaulted v5 fields.
    let legacy =
        decode_bind_meta(&encode_bind_meta("device-004", true, 60)).expect("legacy bind decodes");
    assert_eq!(legacy.protocol_version, 4);
    assert_eq!(legacy.session_expiry, 0);
    assert_eq!(legacy.receive_maximum, u16::MAX);
    assert_eq!(legacy.max_packet_size, 0);
    assert!(legacy.user_properties.is_none());

    // A truncated v5 section fails closed (never a guessed version).
    let mut truncated = encode_bind_meta_v5(V5BindParams {
        client_id: "d",
        clean_start: true,
        keepalive: 60,
        alias_max: 0,
        session_expiry: 0,
        receive_max: 10,
        max_packet_size: 0,
        user_properties: &[],
    })
    .to_vec();
    truncated.truncate(truncated.len() - 3);
    assert!(decode_bind_meta(&truncated).is_err());
}

#[test]
fn v5_session_expiry_zero_drops_state_at_disconnect() {
    // Through the broker bind path: a v5 session with expiry 0
    // drops its subscriptions at disconnect, and the next
    // reconnect observes session_present=false with nothing left.
    let sessions = SessionManager::new();
    let first = bind_frame(
        41,
        1,
        encode_bind_meta_v5(V5BindParams {
            client_id: "v5-expiry-zero",
            clean_start: false,
            keepalive: 60,
            alias_max: 0,
            session_expiry: 0,
            receive_max: 100,
            max_packet_size: 0,
            user_properties: &[],
        }),
    );
    let first_reply = reply_for_frame(&first, &sessions).expect("first v5 bind replies");
    let (_, first_present, rc) = decode_session_binding_meta(&first_reply.metadata);
    assert!(!first_present);
    assert_eq!(rc, 0);
    let filter = TopicFilter::new("v5/zero/state").unwrap();
    sessions.add_subscription("v5-expiry-zero", filter, QoS::AtLeastOnce);
    assert_eq!(
        sessions.subscription_filters("v5-expiry-zero").len(),
        1,
        "subscription stored before disconnect"
    );
    sessions.unbind_connection("v5-expiry-zero", 41);
    assert!(
        sessions.subscription_filters("v5-expiry-zero").is_empty(),
        "expiry 0 must drop subscriptions at disconnect"
    );
    let second = bind_frame(
        42,
        1,
        encode_bind_meta_v5(V5BindParams {
            client_id: "v5-expiry-zero",
            clean_start: false,
            keepalive: 60,
            alias_max: 0,
            session_expiry: 0,
            receive_max: 100,
            max_packet_size: 0,
            user_properties: &[],
        }),
    );
    let second_reply = reply_for_frame(&second, &sessions).expect("reconnect replies");
    let (_, present, rc) = decode_session_binding_meta(&second_reply.metadata);
    assert!(
        !present,
        "dropped session must reconnect with present=false"
    );
    assert_eq!(rc, 0);
    assert!(
        sessions.subscription_filters("v5-expiry-zero").is_empty(),
        "nothing survives an expiry-0 disconnect"
    );
}

#[test]
fn v5_session_expiry_nonzero_retains_and_expires() {
    // Through the broker bind path: a v5 session with a nonzero
    // expiry retains its subscriptions past a reconnect within the
    // interval (present=true) and drops them after it (present=false).
    let sessions = SessionManager::new();
    let bind_expiry = |conn: u64, seq: u64| {
        bind_frame(
            conn,
            seq,
            encode_bind_meta_v5(V5BindParams {
                client_id: "v5-expiry-kept",
                clean_start: false,
                keepalive: 60,
                alias_max: 0,
                session_expiry: 3600,
                receive_max: 100,
                max_packet_size: 0,
                user_properties: &[],
            }),
        )
    };
    let first_reply = reply_for_frame(&bind_expiry(51, 1), &sessions).expect("bind replies");
    let (_, first_present, _) = decode_session_binding_meta(&first_reply.metadata);
    assert!(!first_present);
    let filter = TopicFilter::new("v5/kept/state").unwrap();
    sessions.add_subscription("v5-expiry-kept", filter, QoS::AtLeastOnce);
    sessions.unbind_connection("v5-expiry-kept", 51);
    assert_eq!(
        sessions.subscription_filters("v5-expiry-kept").len(),
        1,
        "nonzero expiry must retain subscriptions at disconnect"
    );
    let second_reply = reply_for_frame(&bind_expiry(52, 1), &sessions).expect("reconnect");
    let (_, present, rc) = decode_session_binding_meta(&second_reply.metadata);
    assert!(
        present,
        "unexpired session must reconnect with present=true"
    );
    assert_eq!(rc, 0);
    assert_eq!(
        sessions.subscription_filters("v5-expiry-kept").len(),
        1,
        "retained subscription survives reconnect within the interval"
    );
    // Detach again and backdate the end past the interval: the
    // next reconnect must treat the session as expired.
    sessions.unbind_connection("v5-expiry-kept", 52);
    {
        let session = sessions.get("v5-expiry-kept").expect("detached session");
        let past = broker_session::now_ms().saturating_sub((3600 + 10) * 1000);
        *session.ended_at_ms.write() = Some(past);
    }
    let third_reply = reply_for_frame(&bind_expiry(53, 1), &sessions).expect("reconnect");
    let (_, present, rc) = decode_session_binding_meta(&third_reply.metadata);
    assert!(
        !present,
        "expired session must reconnect with present=false"
    );
    assert_eq!(rc, 0);
}

#[tokio::test]
async fn v5_session_expiry_caps_at_configured_maximum() {
    // Use the live edge path.
    // Send each v5 bind over TCP.
    // Read the granted expiry from the socket reply.
    let shared = test_shared();
    assert_eq!(shared.sessions.max_session_expiry_secs(), 86_400);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let server_shared = shared.clone();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (stream, _) = listener.accept().await.expect("accept");
            let owned = server_shared.clone();
            tokio::spawn(async move {
                handle_connection(stream, owned).await.expect("handle");
            });
        }
        std::future::pending::<()>().await;
    });
    let io = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let client = FramedTransport::new(io);
    let meta = encode_bind_meta_v5(V5BindParams {
        client_id: "v5-expiry-capped",
        clean_start: true,
        keepalive: 60,
        alias_max: 0,
        session_expiry: 999_999,
        receive_max: 100,
        max_packet_size: 0,
        user_properties: &[],
    });
    client
        .send(bind_frame(71, 1, meta))
        .await
        .expect("send bind");
    let reply = client.recv().await.expect("recv binding");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
    let v5 = decode_session_binding_v5(&reply.metadata).expect("v5 section present");
    assert_eq!(v5.granted_expiry, 86_400);
    let session = shared
        .sessions
        .get("v5-expiry-capped")
        .expect("session stored");
    assert_eq!(session.session_expiry(), 86_400);
    // Zero cap opts into no cap.
    // Large values store verbatim.
    shared.sessions.set_max_session_expiry_secs(0);
    let io = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let client = FramedTransport::new(io);
    let meta = encode_bind_meta_v5(V5BindParams {
        client_id: "v5-expiry-uncapped",
        clean_start: true,
        keepalive: 60,
        alias_max: 0,
        session_expiry: 999_999,
        receive_max: 100,
        max_packet_size: 0,
        user_properties: &[],
    });
    client
        .send(bind_frame(72, 1, meta))
        .await
        .expect("send bind");
    let reply = client.recv().await.expect("recv binding");
    let v5 = decode_session_binding_v5(&reply.metadata).expect("v5 section present");
    assert_eq!(v5.granted_expiry, 999_999);
    let session = shared
        .sessions
        .get("v5-expiry-uncapped")
        .expect("session stored");
    assert_eq!(session.session_expiry(), 999_999);
    server.abort();
}

#[test]
fn v5_receive_maximum_caps_inflight_qos1() {
    // Through the broker delivery path: a v5 client announcing
    // receive-maximum 1 gets one QoS 1 downlink tracked, and the
    // next one is shed (counted via `inflight_dropped`, never
    // sent above the client's unacked cap).
    let router = Router::new();
    let sessions = SessionManager::new();
    let metrics = Metrics::new();
    let filter = TopicFilter::new("v5/capped/inflight").unwrap();
    let bind = bind_frame(
        61,
        1,
        encode_bind_meta_v5(V5BindParams {
            client_id: "v5-capped-rx",
            clean_start: true,
            keepalive: 60,
            alias_max: 0,
            session_expiry: 0,
            receive_max: 1,
            max_packet_size: 0,
            user_properties: &[],
        }),
    );
    let reply = reply_for_frame(&bind, &sessions).expect("v5 bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
    router.subscribe(
        &filter,
        Subscription::new("v5-capped-rx", 61, QoS::AtLeastOnce),
    );
    let topic = Topic::new("v5/capped/inflight").unwrap();
    let first = build_downlink_frames(
        &router,
        &sessions,
        &metrics,
        &topic,
        QoS::AtLeastOnce,
        false,
        &Bytes::from_static(b"one"),
        broker_session::tenant::DEFAULT_TENANT_ID,
        None,
    );
    assert_eq!(first.len(), 1, "first downlink fits the window");
    let session = sessions.get("v5-capped-rx").expect("session");
    assert_eq!(session.inflight_total_len(), 1);
    let second = build_downlink_frames(
        &router,
        &sessions,
        &metrics,
        &topic,
        QoS::AtLeastOnce,
        false,
        &Bytes::from_static(b"two"),
        broker_session::tenant::DEFAULT_TENANT_ID,
        None,
    );
    assert!(
        second.is_empty(),
        "second downlink must shed above receive-maximum 1"
    );
    assert_eq!(metrics.inflight_dropped(), 1, "a counter must move");
    assert_eq!(
        session.inflight_total_len(),
        1,
        "shed frame stays untracked"
    );
}

#[test]
fn v5_max_packet_size_sheds_oversize_publish() {
    // Through the broker delivery path: an oversize publish toward
    // a capped v5 client is refused with the packet-too-large code
    // and a counter moves (never sent above the maximum, never
    // silent).
    let router = Router::new();
    let sessions = SessionManager::new();
    let metrics = Metrics::new();
    let filter = TopicFilter::new("v5/capped/size").unwrap();
    let bind = bind_frame(
        71,
        1,
        encode_bind_meta_v5(V5BindParams {
            client_id: "v5-capped-size",
            clean_start: true,
            keepalive: 60,
            alias_max: 0,
            session_expiry: 0,
            receive_max: 65535,
            max_packet_size: 30,
            user_properties: &[],
        }),
    );
    let reply = reply_for_frame(&bind, &sessions).expect("v5 bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
    router.subscribe(
        &filter,
        Subscription::new("v5-capped-size", 71, QoS::AtLeastOnce),
    );
    let session = sessions.get("v5-capped-size").expect("session");
    let refused = crate::v5_egress_refused(&session, 1, "v5/capped/size".len(), 100);
    assert_eq!(
        refused,
        Some(crate::V5_RC_PACKET_TOO_LARGE),
        "oversize frame must refuse with packet-too-large"
    );
    let topic = Topic::new("v5/capped/size").unwrap();
    let deliveries = build_downlink_frames(
        &router,
        &sessions,
        &metrics,
        &topic,
        QoS::AtLeastOnce,
        false,
        &Bytes::from(vec![0x41; 100]),
        broker_session::tenant::DEFAULT_TENANT_ID,
        None,
    );
    assert!(
        deliveries.is_empty(),
        "oversize publish must not deliver above the maximum"
    );
    assert_eq!(metrics.messages_dropped(), 1, "a counter must move");
}

#[test]
fn v5_receive_maximum_zero_rejected_with_protocol_error() {
    // Through the broker bind path: a v5 CONNECT announcing
    // receive-maximum 0 fails closed with the protocol-error code,
    // a server-side reason string and the echoed CONNECT user
    // properties — with no session created.
    let sessions = SessionManager::new();
    let frame = bind_frame(
        81,
        1,
        encode_bind_meta_v5(V5BindParams {
            client_id: "v5-bad-rx",
            clean_start: true,
            keepalive: 60,
            alias_max: 0,
            session_expiry: 0,
            receive_max: 0,
            max_packet_size: 0,
            user_properties: &[("trace", "abc")],
        }),
    );
    let reply = reply_for_frame(&frame, &sessions).expect("malformed v5 bind replies");
    let (_, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert!(!present);
    assert_eq!(rc, crate::V5_RC_PROTOCOL_ERROR);
    let v5 = decode_session_binding_v5(&reply.metadata).expect("v5 section present");
    assert!(!v5.reason.is_empty(), "failure must carry a reason string");
    assert_eq!(
        v5.user_properties,
        vec![("trace".to_string(), "abc".to_string())],
        "failure must echo the CONNECT user properties"
    );
    assert!(
        sessions.get("v5-bad-rx").is_none(),
        "rejected bind must create no session"
    );
}

#[test]
fn v5_failure_reason_strings_and_user_echo() {
    // Unit cover for the failure CONNACK properties: every refusal
    // code maps to a server-side reason string, and the CONNECT
    // user properties echo back sorted and bounded.
    assert!(crate::v5_reason_string(0).is_none());
    for rc in [0x86u8, 0x87, 0x8A, 0x82, 0x97, 0x8B, 0x95] {
        let reason = crate::v5_reason_string(rc).expect("reason string");
        assert!(!reason.is_empty(), "rc {rc:#04x} needs a reason string");
    }
    let meta = encode_bind_meta_v5(V5BindParams {
        client_id: "v5-echo",
        clean_start: true,
        keepalive: 60,
        alias_max: 0,
        session_expiry: 0,
        receive_max: 10,
        max_packet_size: 0,
        user_properties: &[("b", "2"), ("a", "1")],
    });
    let req = decode_bind_meta(&meta).expect("v5 bind decodes");
    let v5 = crate::failure_binding_v5(&req, 0x86, broker_session::DEFAULT_MAX_PACKET_SIZE);
    assert!(!v5.reason.is_empty());
    assert_eq!(
        v5.user_properties,
        vec![
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string())
        ],
        "echoed properties must round-trip"
    );
}

#[test]
fn v4_binds_keep_today_layout_and_defaults() {
    // 3.1.1 integration unchanged: a version-4 bind keeps the
    // 12-byte reply with no v5 section, defaulted caps and the
    // clean-session drop behaviour.
    let sessions = SessionManager::new();
    let frame = bind_frame(91, 1, encode_bind_meta("device-v4-unchanged", true, 60));
    let reply = reply_for_frame(&frame, &sessions).expect("v4 bind replies");
    assert_eq!(
        reply.metadata.len(),
        12,
        "v4 replies keep the 12-byte layout"
    );
    assert!(
        decode_session_binding_v5(&reply.metadata).is_none(),
        "v4 replies carry no v5 section"
    );
    let session = sessions.get("device-v4-unchanged").expect("session stored");
    assert_eq!(session.protocol_version(), 4);
    assert_eq!(session.session_expiry(), 0);
    assert_eq!(
        session.client_receive_maximum(),
        broker_session::DEFAULT_CLIENT_RECEIVE_MAXIMUM
    );
    assert_eq!(session.client_max_packet_size(), 0);
}

#[test]
fn bind_connection_creates_session_without_present_flag() {
    let sessions = SessionManager::new();
    let frame = bind_frame(11, 1, encode_bind_meta("device-001", true, 60));

    let reply = reply_for_frame(&frame, &sessions).expect("Bind must produce a reply");
    assert_eq!(reply.header.opcode, OpCode::SessionBinding);
    assert_eq!(reply.header.conn_id, 11);
    assert_eq!(reply.header.sequence_no, 1);

    let (session_id, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert_ne!(session_id, 0, "fresh session must have a nonzero id");
    assert!(
        !present,
        "first clean-start bind must report session_present=false"
    );
    assert_eq!(rc, 0, "accepted bind must carry return code 0");
}

#[test]
fn bind_connection_resumes_session_with_present_flag() {
    let sessions = SessionManager::new();
    let first = bind_frame(11, 1, encode_bind_meta("device-007", true, 60));
    let first_reply = reply_for_frame(&first, &sessions).expect("first bind replies");
    let (first_id, first_present, _) = decode_session_binding_meta(&first_reply.metadata);
    assert!(!first_present);

    let second = bind_frame(12, 1, encode_bind_meta("device-007", false, 60));
    let second_reply = reply_for_frame(&second, &sessions).expect("second bind replies");
    assert_eq!(
        second_reply.header.conn_id, 12,
        "reply mirrors requesting conn"
    );
    let (second_id, second_present, rc) = decode_session_binding_meta(&second_reply.metadata);
    assert!(
        second_present,
        "resumed session must report session_present=true"
    );
    assert_eq!(
        second_id, first_id,
        "resumed bind must reuse the session id"
    );
    assert_eq!(rc, 0);
}

#[test]
fn bind_connection_clean_start_replaces_session() {
    let sessions = SessionManager::new();
    let first = bind_frame(11, 1, encode_bind_meta("device-009", true, 60));
    let (first_id, _, _) =
        decode_session_binding_meta(&reply_for_frame(&first, &sessions).unwrap().metadata);

    let second = bind_frame(11, 2, encode_bind_meta("device-009", true, 60));
    let reply = reply_for_frame(&second, &sessions).expect("rebind replies");
    let (second_id, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert_ne!(
        second_id, first_id,
        "clean start must mint a fresh session id"
    );
    assert!(!present);
    assert_eq!(rc, 0);
}

#[test]
fn bind_connection_rejects_garbage_meta_with_rc2() {
    let sessions = SessionManager::new();
    // Truncated meta: claims 5 id bytes but carries none of the tail.
    let bad = bind_frame(11, 3, Bytes::from(vec![0x00, 0x05, b'a', b'b']));
    let reply = reply_for_frame(&bad, &sessions).expect("malformed bind still replies");
    assert_eq!(reply.header.opcode, OpCode::SessionBinding);
    assert_eq!(reply.header.conn_id, 11);
    assert_eq!(reply.header.sequence_no, 3);
    let (_, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert!(!present);
    assert_eq!(rc, 2, "malformed bind must carry return code 2");
}

#[test]
fn bind_connection_rejects_non_utf8_client_id_with_rc2() {
    let sessions = SessionManager::new();
    // id_len=2, bytes 0xFF 0xFE are not valid UTF-8.
    let bad = bind_frame(
        11,
        4,
        Bytes::from(vec![0x00, 0x02, 0xFF, 0xFE, 0x01, 0x00, 0x3C]),
    );
    let reply = reply_for_frame(&bad, &sessions).expect("non-UTF8 bind still replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 2, "non-UTF8 client id must carry return code 2");
}

#[tokio::test]
async fn ping_pong_round_trip_over_transport() {
    let sessions = SessionManager::new();
    let (client_io, server_io) = tokio::io::duplex(1024);
    let client = FramedTransport::new(client_io);
    let server = FramedTransport::new(server_io);

    let ping = BrokerFrame::ping(4242, 7);
    client.send(ping).await.expect("client send");

    let received = server.recv().await.expect("server recv");
    assert_eq!(received.header.opcode, OpCode::Ping);

    let reply = reply_for_frame(&received, &sessions).expect("server reply");
    server.send(reply).await.expect("server send");

    let pong = client.recv().await.expect("client recv");
    assert_eq!(pong.header.opcode, OpCode::Pong);
    assert_eq!(pong.header.conn_id, 4242);
    assert_eq!(pong.header.sequence_no, 7);
}

#[tokio::test]
async fn server_replies_pong_to_ping_end_to_end() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");

    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        handle_connection(stream, Shared::new())
            .await
            .expect("handle");
    });

    let stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let client = FramedTransport::new(stream);
    client
        .send(BrokerFrame::ping(99, 100))
        .await
        .expect("send ping");
    let pong = client.recv().await.expect("recv pong");
    assert_eq!(pong.header.opcode, OpCode::Pong);
    assert_eq!(pong.header.conn_id, 99);
    assert_eq!(pong.header.sequence_no, 100);

    server_task.abort();
}

#[tokio::test]
async fn server_binds_session_end_to_end() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");

    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        handle_connection(stream, test_shared())
            .await
            .expect("handle");
    });

    let stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let client = FramedTransport::new(stream);
    client
        .send(bind_frame(55, 9, encode_bind_meta("e2e-device", true, 30)))
        .await
        .expect("send bind");
    let reply = client.recv().await.expect("recv binding");
    assert_eq!(reply.header.opcode, OpCode::SessionBinding);
    assert_eq!(reply.header.conn_id, 55);
    assert_eq!(reply.header.sequence_no, 9);
    let (session_id, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert_ne!(session_id, 0);
    assert!(!present);
    assert_eq!(rc, 0);

    server_task.abort();
}

#[tokio::test]
async fn subscribe_registers_and_replies_suback() {
    let shared = test_shared();
    let frame = subscribe_frame(
        21,
        4,
        encode_subscribe_meta(7, "sub-1", &[("sport/tennis", 1), ("news", 0)]),
    );

    let (reply, retained) = apply_subscribe(&frame, &shared).await;
    let reply = reply.expect("subscribe replies");
    assert!(retained.is_empty(), "no retained state stored yet");
    assert_eq!(reply.header.opcode, OpCode::SubAckOut);
    assert_eq!(reply.header.conn_id, 21);
    assert_eq!(reply.header.sequence_no, 4);
    assert_eq!(&reply.metadata[0..2], &7u16.to_be_bytes());
    assert_eq!(&reply.metadata[2..], &[1u8, 0u8]);

    let matches = shared.router.matches(&Topic::new("sport/tennis").unwrap());
    assert_eq!(matches.len(), 1);
    let sub = matches.iter().next().unwrap();
    assert_eq!(sub.client_id.as_ref(), "sub-1");
    assert_eq!(sub.conn_id, 21);
    assert_eq!(sub.qos, QoS::AtLeastOnce);
}

#[tokio::test]
async fn subscribe_invalid_filter_gets_0x80_without_registering() {
    let shared = test_shared();
    let frame = subscribe_frame(
        21,
        4,
        encode_subscribe_meta(9, "sub-bad", &[("sport/#/bogus", 0)]),
    );

    let (reply, retained) = apply_subscribe(&frame, &shared).await;
    let reply = reply.expect("subscribe replies");
    assert!(retained.is_empty());
    assert_eq!(reply.header.opcode, OpCode::SubAckOut);
    assert_eq!(&reply.metadata[0..2], &9u16.to_be_bytes());
    assert_eq!(&reply.metadata[2..], &[0x80u8]);

    let matches = shared.router.matches(&Topic::new("sport/x").unwrap());
    assert!(matches.is_empty());
}

#[tokio::test]
async fn subscribe_malformed_meta_yields_no_reply() {
    let shared = test_shared();
    let frame = subscribe_frame(21, 4, Bytes::from(vec![0x00, 0x07, 0xAA]));
    let (reply, retained) = apply_subscribe(&frame, &shared).await;
    assert!(reply.is_none());
    assert!(retained.is_empty());
}

/// Unsubscribe removes the router copy and the session mirror
/// through the broker. A publish after it delivers nothing.
/// Goes through `apply_subscribe` and `apply_unsubscribe`, not
/// the store alone.
#[tokio::test]
async fn unsubscribe_removes_subscription_and_stops_delivery() {
    let shared = test_shared();
    let bind_meta = encode_bind_meta("unsub-1", true, 60);
    let bind_reply = apply_bind(&bind_frame(21, 1, bind_meta), &shared)
        .await
        .expect("bind replies");
    let (_, _, bind_rc) = decode_session_binding_meta(&bind_reply.metadata);
    assert_eq!(bind_rc, 0);
    let sub_frame = subscribe_frame(
        21,
        2,
        encode_subscribe_meta(1, "unsub-1", &[("k1/data", 0)]),
    );
    apply_subscribe(&sub_frame, &shared)
        .await
        .0
        .expect("subscribe");
    bind_test_publisher(&shared, 40);
    let (meta, payload) = encode_publish_meta("k1/data", 0, 0, false, b"first");
    let (_, before) = apply_publish(&publish_frame(40, 1, meta, payload), &shared).await;
    assert!(!before.is_empty(), "delivery before unsubscribe");
    let unsub = unsubscribe_frame(21, 3, encode_unsubscribe_meta(2, "unsub-1", &["k1/data"]));
    let reply = apply_unsubscribe(&unsub, &shared)
        .await
        .expect("unsuback replies");
    assert_eq!(reply.header.opcode, OpCode::UnsubAckOut);
    assert_eq!(&reply.metadata[0..2], &2u16.to_be_bytes());
    assert_eq!(&reply.metadata[2..], &[0x00u8]);
    assert!(
        shared
            .sessions
            .get("unsub-1")
            .expect("session exists")
            .subscriptions
            .read()
            .is_empty(),
        "session mirror drops the filter"
    );
    let (meta, payload) = encode_publish_meta("k1/data", 0, 0, false, b"second");
    let (_, after) = apply_publish(&publish_frame(40, 2, meta, payload), &shared).await;
    assert!(after.is_empty(), "no delivery after unsubscribe");
}

/// An unknown filter answers 0x11 and an invalid filter answers
/// 0x8F, each failing only its filter. Goes through the broker.
#[tokio::test]
async fn unsubscribe_unknown_and_invalid_filters_get_their_codes() {
    let shared = test_shared();
    let bind_meta = encode_bind_meta("unsub-2", true, 60);
    apply_bind(&bind_frame(22, 1, bind_meta), &shared)
        .await
        .expect("bind");
    let sub_frame = subscribe_frame(
        22,
        2,
        encode_subscribe_meta(1, "unsub-2", &[("k2/kept", 0)]),
    );
    apply_subscribe(&sub_frame, &shared)
        .await
        .0
        .expect("subscribe");
    let unsub = unsubscribe_frame(
        22,
        3,
        encode_unsubscribe_meta(5, "unsub-2", &["k2/missing", "sport/#/bogus", "k2/kept"]),
    );
    let reply = apply_unsubscribe(&unsub, &shared)
        .await
        .expect("unsuback replies");
    assert_eq!(&reply.metadata[0..2], &5u16.to_be_bytes());
    assert_eq!(&reply.metadata[2..], &[0x11u8, 0x8Fu8, 0x00u8]);
    let (meta, payload) = encode_publish_meta("k2/kept", 0, 0, false, b"x");
    let (_, deliveries) = apply_publish(&publish_frame(40, 1, meta, payload), &shared).await;
    assert!(
        deliveries.is_empty(),
        "kept filter is gone, missing never existed"
    );
}

/// A shared subscription leaves through its `$share` form. Goes
/// through the broker subscribe and unsubscribe events.
#[tokio::test]
async fn unsubscribe_shared_group_form_removes_the_member() {
    let shared = test_shared();
    let bind_meta = encode_bind_meta("unsub-share", true, 60);
    apply_bind(&bind_frame(23, 1, bind_meta), &shared)
        .await
        .expect("bind");
    let sub_frame = subscribe_frame(
        23,
        2,
        encode_subscribe_meta(1, "unsub-share", &[("$share/pool/k3/tasks", 0)]),
    );
    apply_subscribe(&sub_frame, &shared)
        .await
        .0
        .expect("subscribe");
    bind_test_publisher(&shared, 40);
    let (meta, payload) = encode_publish_meta("k3/tasks", 0, 0, false, b"job");
    let (_, before) = apply_publish(&publish_frame(40, 1, meta, payload), &shared).await;
    assert_eq!(before.len(), 1, "shared member delivers before unsubscribe");
    let unsub = unsubscribe_frame(
        23,
        3,
        encode_unsubscribe_meta(2, "unsub-share", &["$share/pool/k3/tasks"]),
    );
    let reply = apply_unsubscribe(&unsub, &shared)
        .await
        .expect("unsuback replies");
    assert_eq!(&reply.metadata[2..], &[0x00u8]);
    let (meta, payload) = encode_publish_meta("k3/tasks", 0, 0, false, b"job2");
    let (_, after) = apply_publish(&publish_frame(40, 2, meta, payload), &shared).await;
    assert!(
        after.is_empty(),
        "shared member is silent after unsubscribe"
    );
}

/// Malformed unsubscribe metadata yields no reply and changes
/// nothing. Goes through the broker.
#[tokio::test]
async fn unsubscribe_malformed_meta_yields_no_reply() {
    let shared = test_shared();
    let frame = unsubscribe_frame(21, 4, Bytes::from(vec![0x00, 0x07, 0xAA]));
    assert!(apply_unsubscribe(&frame, &shared).await.is_none());
}

/// One client id in two tenants unsubscribes independently. A
/// removal in one tenant keeps the other tenant delivery.
/// Goes through the broker bind, subscribe, unsubscribe and
/// publish events.
#[tokio::test]
async fn unsubscribe_keeps_the_other_tenant_subscription() {
    use broker_session::tenant::TenantRule;
    let shared = test_shared();
    shared
        .tenant_registry
        .replace(vec![TenantRule::new("", "tenant", "u-${username}")])
        .expect("tenant rules store");
    for (conn, client, user) in [(211u64, "dup-unsub", "alice"), (212u64, "dup-unsub", "bob")] {
        let meta = encode_bind_meta_creds(client, true, 60, user, b"pw");
        let reply = apply_bind(&bind_frame(conn, 1, meta), &shared)
            .await
            .expect("bind replies");
        let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
        assert_eq!(rc, 0);
    }
    for (conn, client) in [(211u64, "dup-unsub"), (212u64, "dup-unsub")] {
        let frame = subscribe_frame(conn, 2, encode_subscribe_meta(1, client, &[("k4/data", 0)]));
        apply_subscribe(&frame, &shared).await.0.expect("subscribe");
    }
    let unsub = unsubscribe_frame(
        211,
        3,
        encode_unsubscribe_meta(2, "dup-unsub", &["k4/data"]),
    );
    let reply = apply_unsubscribe(&unsub, &shared)
        .await
        .expect("unsuback replies");
    assert_eq!(&reply.metadata[2..], &[0x00u8]);
    let (meta, payload) = encode_publish_meta("k4/data", 0, 0, false, b"from-b");
    let (_, deliveries) = apply_publish(&publish_frame(212, 4, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1, "other tenant still delivers");
    assert_eq!(deliveries[0].0, 212);
}

/// X1-03: each subscription option round-trips through the extended
/// subscribe meta (options byte plus identifier) into the router copy
/// and the session mirror. Goes through the broker (`apply_subscribe`),
/// not only through the store.
#[tokio::test]
async fn subscribe_v5_options_round_trip() {
    let shared = test_shared();
    // Bind through the production entry before subscribing.
    let bind_meta = encode_bind_meta("v5-opt", true, 60);
    let bind_reply = apply_bind(&bind_frame(21, 1, bind_meta), &shared)
        .await
        .expect("bind replies");
    let (_, _, bind_rc) = decode_session_binding_meta(&bind_reply.metadata);
    assert_eq!(bind_rc, 0);
    // Opts: QoS 1 | no-local | rap | RH 1 => 1 | 0x04 | 0x08 | 0x10.
    let opts = broker_router::Subscription::encode_options_byte(QoS::AtLeastOnce, true, true, 1);
    let frame = subscribe_frame(
        21,
        4,
        encode_subscribe_meta_v5(7, "v5-opt", &[(("sport/tennis"), opts, 42)]),
    );
    let (reply, _) = apply_subscribe(&frame, &shared).await;
    let reply = reply.expect("subscribe replies");
    assert_eq!(&reply.metadata[0..2], &7u16.to_be_bytes());
    assert_eq!(&reply.metadata[2..], &[1u8]);
    let matches = shared.router.matches(&Topic::new("sport/tennis").unwrap());
    assert_eq!(matches.len(), 1);
    let sub = matches.iter().next().unwrap();
    assert!(sub.no_local);
    assert!(sub.retain_as_published);
    assert_eq!(sub.retain_handling, 1);
    assert_eq!(sub.subscription_id, 42);
    let session = shared.sessions.get("v5-opt").expect("session mirror");
    let stored = session.subscriptions.read();
    let filter = TopicFilter::new("sport/tennis").unwrap();
    let mirror = stored.get(&filter).expect("mirror entry");
    assert_eq!(mirror.nl, 1);
    assert_eq!(mirror.rap, 1);
    assert_eq!(mirror.rh, 1);
}

/// X1-03: no-local suppresses self-delivery but not others'
/// delivery. Two subscribers on one filter (one no-local, one
/// plain); a publish from the no-local client reaches only the
/// plain subscriber. Through the broker publish path.
#[tokio::test]
async fn subscribe_v5_no_local_suppresses_self_only() {
    let shared = test_shared();
    // Bind each client through the production entry first.
    for (conn, client) in [(31u64, "nl-self"), (32u64, "nl-other")] {
        let bind_meta = encode_bind_meta(client, true, 60);
        let bind_reply = apply_bind(&bind_frame(conn, 1, bind_meta), &shared)
            .await
            .expect("bind replies");
        let (_, _, bind_rc) = decode_session_binding_meta(&bind_reply.metadata);
        assert_eq!(bind_rc, 0);
    }
    let opts_nl = broker_router::Subscription::encode_options_byte(QoS::AtMostOnce, true, false, 0);
    let opts_plain =
        broker_router::Subscription::encode_options_byte(QoS::AtMostOnce, false, false, 0);
    for (conn, client, opts) in [(31u64, "nl-self", opts_nl), (32u64, "nl-other", opts_plain)] {
        let frame = subscribe_frame(
            conn,
            1,
            encode_subscribe_meta_v5(1, client, &[("nl/topic", opts, 0)]),
        );
        apply_subscribe(&frame, &shared).await.0.expect("subscribe");
        let (session, _) = shared.sessions.get_or_create(client, false);
        *session.conn_id.write() = Some(conn);
    }
    // Publish from the no-local client through the broker path.
    let (pub_session, _) = shared.sessions.get_or_create("nl-self", false);
    *pub_session.conn_id.write() = Some(31);
    shared.sessions.bind_session(&pub_session, 31);
    let (meta, payload) = encode_publish_meta("nl/topic", 0, 0, false, b"x");
    let (_, deliveries) = apply_publish(&publish_frame(31, 2, meta, payload), &shared).await;
    let targets: Vec<u64> = deliveries.iter().map(|(conn, _)| *conn).collect();
    assert!(
        !targets.contains(&31),
        "no-local suppresses self-delivery, got {targets:?}"
    );
    assert!(
        targets.contains(&32),
        "others' delivery still arrives, got {targets:?}"
    );
}

/// X1-03: retain-handling 0/1/2 against a retained message. A
/// retained publish is stored; RH 0 replays on every subscribe, RH 2
/// never replays, and RH 1 replays only on the first (new)
/// subscribe. Through the broker subscribe path.
#[tokio::test]
async fn subscribe_v5_retain_handling_variants() {
    let shared = test_shared();
    bind_test_publisher(&shared, 90);
    // Store one retained message via the broker publish path.
    let (meta, payload) = encode_publish_meta("rh/topic", 0, 0, true, b"kept");
    apply_publish(&publish_frame(90, 1, meta, payload), &shared).await;
    // Bind each subscriber through the production entry first.
    for (conn, client) in [(91u64, "rh-zero"), (92u64, "rh-two"), (93u64, "rh-one")] {
        let bind_meta = encode_bind_meta(client, true, 60);
        let bind_reply = apply_bind(&bind_frame(conn, 1, bind_meta), &shared)
            .await
            .expect("bind replies");
        let (_, _, bind_rc) = decode_session_binding_meta(&bind_reply.metadata);
        assert_eq!(bind_rc, 0);
    }
    for (conn, client, rh, expect_retained) in
        [(91u64, "rh-zero", 0u8, true), (92u64, "rh-two", 2u8, false)]
    {
        let opts =
            broker_router::Subscription::encode_options_byte(QoS::AtMostOnce, false, false, rh);
        let frame = subscribe_frame(
            conn,
            1,
            encode_subscribe_meta_v5(1, client, &[("rh/topic", opts, 0)]),
        );
        let (_, retained) = apply_subscribe(&frame, &shared).await;
        assert_eq!(
            !retained.is_empty(),
            expect_retained,
            "RH {rh} replay expectation"
        );
    }
    // RH 1: first subscribe replays, second (existing) does not.
    let opts1 = broker_router::Subscription::encode_options_byte(QoS::AtMostOnce, false, false, 1);
    let first = subscribe_frame(
        93,
        1,
        encode_subscribe_meta_v5(1, "rh-one", &[("rh/topic", opts1, 0)]),
    );
    let (_, retained_first) = apply_subscribe(&first, &shared).await;
    assert!(!retained_first.is_empty(), "RH 1 new subscription replays");
    let second = subscribe_frame(
        93,
        2,
        encode_subscribe_meta_v5(2, "rh-one", &[("rh/topic", opts1, 0)]),
    );
    let (_, retained_second) = apply_subscribe(&second, &shared).await;
    assert!(
        retained_second.is_empty(),
        "RH 1 existing subscription suppresses replay"
    );
}

/// X1-03: malformed options fail only the affected filter with 0x8F.
/// One filter carries a reserved-bit options byte, the other is
/// valid; the valid grant registers while the bad one reports 0x8F.
#[tokio::test]
async fn subscribe_v5_malformed_opts_fail_only_filter() {
    let shared = test_shared();
    // Bind through the production entry before subscribing.
    let bind_meta = encode_bind_meta("v5-mixed", true, 60);
    let bind_reply = apply_bind(&bind_frame(21, 1, bind_meta), &shared)
        .await
        .expect("bind replies");
    let (_, _, bind_rc) = decode_session_binding_meta(&bind_reply.metadata);
    assert_eq!(bind_rc, 0);
    let bad_opts = 0xC1u8; // reserved bits set (plus QoS 1).
    let good_opts =
        broker_router::Subscription::encode_options_byte(QoS::AtMostOnce, false, false, 0);
    let frame = subscribe_frame(
        21,
        4,
        encode_subscribe_meta_v5(
            7,
            "v5-mixed",
            &[("good/topic", good_opts, 0), ("bad/topic", bad_opts, 0)],
        ),
    );
    let (reply, _) = apply_subscribe(&frame, &shared).await;
    let reply = reply.expect("subscribe replies");
    assert_eq!(&reply.metadata[0..2], &7u16.to_be_bytes());
    assert_eq!(&reply.metadata[2..], &[0u8, V5_SUBACK_FILTER_INVALID]);
    let matches = shared.router.matches(&Topic::new("good/topic").unwrap());
    assert_eq!(matches.len(), 1);
    let bad = shared.router.matches(&Topic::new("bad/topic").unwrap());
    assert!(bad.is_empty());
}

/// X1-03: the delivery carries the subscription identifier. A v5
/// subscriber with identifier 7 receives a PublishOut whose trailing
/// identifier reads back as 7; a version-4 subscriber reads 0.
/// Through the broker publish-to-delivery path.
#[tokio::test]
async fn publish_v5_delivery_carries_subscription_id() {
    let shared = test_shared();
    let opts = broker_router::Subscription::encode_options_byte(QoS::AtMostOnce, false, false, 0);
    let frame = subscribe_frame(
        61,
        1,
        encode_subscribe_meta_v5(1, "sub-id-1", &[("id/topic", opts, 7)]),
    );
    apply_subscribe(&frame, &shared).await.0.expect("subscribe");
    let (session, _) = shared.sessions.get_or_create("sub-id-1", true);
    *session.conn_id.write() = Some(61);
    bind_test_publisher(&shared, 62);
    let (meta, payload) = encode_publish_meta("id/topic", 0, 0, false, b"v");
    let (_, deliveries) = apply_publish(&publish_frame(62, 1, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1);
    let sub_id =
        decode_publish_out_sub_id(&deliveries[0].1.metadata).expect("downlink meta decodes");
    assert_eq!(sub_id, 7);
}

/// X1-03: inbound v5 properties forward to version-5 deliveries.
/// A publish carrying payload-format, message-expiry and user
/// properties fans out with the same values on the downlink meta;
/// a property-less publish still delivers with defaults. Through
/// the broker publish-to-delivery path.
#[tokio::test]
async fn publish_v5_delivery_forwards_properties() {
    let shared = test_shared();
    let opts = broker_router::Subscription::encode_options_byte(QoS::AtMostOnce, false, false, 0);
    let frame = subscribe_frame(
        61,
        1,
        encode_subscribe_meta_v5(1, "sub-props", &[("props/topic", opts, 9)]),
    );
    apply_subscribe(&frame, &shared).await.0.expect("subscribe");
    let (session, _) = shared.sessions.get_or_create("sub-props", true);
    *session.conn_id.write() = Some(61);
    bind_test_publisher(&shared, 62);
    let (meta, payload) = encode_publish_meta_v5(
        "props/topic",
        0,
        0,
        false,
        0,
        1,
        3600,
        &[("k", "v")],
        b"hello",
    );
    let (_, deliveries) = apply_publish(&publish_frame(62, 1, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1);
    let props = crate::decode_publish_out_props(&deliveries[0].1.metadata).expect("props decode");
    assert_eq!(props.format, 1);
    assert_eq!(props.expiry, 3600);
    assert_eq!(props.users.len(), 1);
    assert_eq!(props.users[0].0, b"k".to_vec());
    assert_eq!(props.users[0].1, b"v".to_vec());
    let sub_id =
        decode_publish_out_sub_id(&deliveries[0].1.metadata).expect("downlink meta decodes");
    assert_eq!(sub_id, 9);
}

/// X1-03: v5 DISCONNECT with reason 0x04 publishes the stored will;
/// a normal reason suppresses it. Drives the unbind/disconnect
/// events through the broker.
#[tokio::test]
async fn disconnect_v5_with_will_reason_publishes_will() {
    let shared = test_shared();
    // Subscriber for the will topic.
    let sub = subscribe_frame(
        71,
        1,
        encode_subscribe_meta(1, "will-watcher", &[("will/v5", 0)]),
    );
    apply_subscribe(&sub, &shared).await.0.expect("subscribe");
    let (watch_session, _) = shared.sessions.get_or_create("will-watcher", true);
    *watch_session.conn_id.write() = Some(71);
    // Publisher session with a stored will, bound to conn 70.
    let (pub_session, _) = shared.sessions.get_or_create("will-pub", true);
    *pub_session.conn_id.write() = Some(70);
    shared.sessions.bind_session(&pub_session, 70);
    pub_session.set_last_will(Some(broker_session::StoredWill {
        tenant: broker_session::tenant::DEFAULT_TENANT_ID.to_string(),
        topic: Topic::new("will/v5").unwrap(),
        qos: QoS::AtMostOnce,
        retain: false,
        payload: Bytes::from_static(b"bye"),
    }));
    // DISCONNECT with 0x04 arrives as an unbind with reason.
    let mut meta = Vec::new();
    meta.extend_from_slice(&("will-pub".len() as u16).to_be_bytes());
    meta.extend_from_slice(b"will-pub");
    meta.push(V5_DISCONNECT_WITH_WILL);
    let frame = BrokerFrame::new(
        OpCode::UnbindConnection,
        70,
        2,
        Bytes::from(meta),
        Bytes::new(),
    )
    .expect("unbind frame");
    // Route through the test-visible helper (same event as the live path).
    let deliveries = apply_disconnect_with_unbind_meta(&frame, &shared).await;
    assert!(!deliveries.is_empty(), "0x04 must publish the stored will");
}

/// X1-03: no path leaves a tenant. Two tenants hold the same filter
/// with v5 options (no-local plus identifiers in A, plain plus an
/// identifier in B). A publish in A reaches only A (and no-local
/// still suppresses the self copy); a publish in B reaches only B
/// with its own identifier. Through the broker bind, subscribe,
/// publish and delivery events.
#[tokio::test]
async fn subscribe_v5_options_isolate_two_tenants_through_broker() {
    use broker_session::tenant::TenantRule;
    let shared = test_shared();
    shared
        .tenant_registry
        .replace(vec![TenantRule::new("", "tenant", "u-${username}")])
        .expect("tenant rules store");
    for (conn, client, user) in [(111u64, "x103-a1", "alice"), (112u64, "x103-b1", "bob")] {
        let meta = encode_bind_meta_creds(client, true, 60, user, b"pw");
        let reply = apply_bind(&bind_frame(conn, 1, meta), &shared)
            .await
            .expect("bind replies");
        let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
        assert_eq!(rc, 0);
    }
    let opts_a = broker_router::Subscription::encode_options_byte(QoS::AtMostOnce, true, false, 0);
    let opts_b = broker_router::Subscription::encode_options_byte(QoS::AtMostOnce, false, false, 0);
    for (conn, client, opts, sub_id) in [
        (111u64, "x103-a1", opts_a, 5u32),
        (112u64, "x103-b1", opts_b, 6u32),
    ] {
        let frame = subscribe_frame(
            conn,
            2,
            encode_subscribe_meta_v5(1, client, &[("x103/two-tenant", opts, sub_id)]),
        );
        apply_subscribe(&frame, &shared).await.0.expect("subscribe");
    }
    // Publish in A from its own subscriber: no-local suppresses the
    // self copy, and B never observes A.
    let before = shared.metrics.tenant_mismatch_dropped();
    let (meta, payload) = encode_publish_meta("x103/two-tenant", 0, 0, false, b"from-a");
    let (_, deliveries) = apply_publish(&publish_frame(111, 3, meta, payload), &shared).await;
    assert!(
        deliveries.is_empty(),
        "A self-publish stays suppressed, B sees nothing"
    );
    assert!(
        shared.metrics.tenant_mismatch_dropped() > before,
        "B's copy is counted, never delivered"
    );
    // Publish in B: only B delivers, carrying its own identifier.
    let (meta, payload) = encode_publish_meta("x103/two-tenant", 0, 0, false, b"from-b");
    let (_, deliveries) = apply_publish(&publish_frame(112, 4, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1, "B delivers once");
    assert_eq!(deliveries[0].0, 112, "B's publish stays in B");
    let sub_id =
        decode_publish_out_sub_id(&deliveries[0].1.metadata).expect("downlink meta decodes");
    assert_eq!(sub_id, 6, "delivery carries B's identifier, never A's");
}

#[tokio::test]
async fn publish_qos1_fans_out_with_qos_downshift() {
    let shared = test_shared();
    // Subscriber A wants QoS 1 on an exact filter; B wants QoS 0 via wildcard.
    for (conn, client, filter, qos) in [
        (31u64, "fan-a", "sport/tennis", 1u8),
        (32u64, "fan-b", "sport/#", 0u8),
    ] {
        let frame = subscribe_frame(conn, 1, encode_subscribe_meta(1, client, &[(filter, qos)]));
        let (reply, _) = apply_subscribe(&frame, &shared).await;
        reply.expect("subscribe replies");
        // Sessions must exist and be bound for live delivery.
        let (session, _) = shared.sessions.get_or_create(client, true);
        *session.conn_id.write() = Some(conn);
    }
    bind_test_publisher(&shared, 40);

    let (meta, payload) = encode_publish_meta("sport/tennis", 5, 1, false, b"hello");
    let (ack, deliveries) = apply_publish(&publish_frame(40, 2, meta, payload), &shared).await;
    assert!(ack.is_some(), "QoS 1 needs a PubAck");

    assert_eq!(deliveries.len(), 2);
    let mut by_conn: std::collections::HashMap<u64, BrokerFrame> = deliveries.into_iter().collect();
    let for_a = by_conn.remove(&31).expect("exact subscriber routed");
    let for_b = by_conn.remove(&32).expect("wildcard subscriber routed");
    for frame in [&for_a, &for_b] {
        assert_eq!(frame.header.opcode, OpCode::PublishOut);
        assert_eq!(frame.payload, Bytes::from_static(b"hello"));
    }
    let (topic_a, pid_a, qos_a, _) = decode_publish_meta_parts(&for_a.metadata);
    assert_eq!(topic_a, "sport/tennis");
    assert_eq!(qos_a, 1, "subscriber QoS preserved under QoS 1 publish");
    assert_ne!(pid_a, 0, "QoS 1 downlink needs a packet id");
    let (_, pid_b, qos_b, _) = decode_publish_meta_parts(&for_b.metadata);
    assert_eq!(qos_b, 0, "delivery downshifted to min(pub, sub)");
    assert_eq!(pid_b, 0, "QoS 0 downlink carries no packet id");
}
