//! Session replay, v5 receive-maximum credit and deferred retained delivery.

use super::*;

#[tokio::test]
async fn unbound_conn_cannot_replay_or_consume_durable_offline_queue() {
    let shared = test_shared();
    let (sess, _, _, _) = shared.sessions.connect_bind_v5_in_tenant(
        broker_session::tenant::DEFAULT_TENANT_ID,
        "durable-replay-guard",
        false,
        None,
        5,
        60,
        16,
        0,
        Some(broker_session::ConnectBindMeta {
            conn_id: 301,
            username: Some("alice".to_string()),
            verified_username: Some("alice".to_string()),
            is_superuser: false,
            peerhost: None,
            keepalive_secs: 60,
            client_alias_max: 0,
            will: None,
        }),
    );
    shared
        .sessions
        .unbind_connection("durable-replay-guard", 301);
    sess.push_offline(broker_session::QueuedMessage {
        topic: Topic::new("t/1").unwrap(),
        qos: QoS::AtLeastOnce,
        retain: false,
        payload: Bytes::from_static(b"secret"),
        publish_at_ms: None,
    });
    assert_eq!(sess.offline_len(), 1);

    let (tx_bad, mut rx_bad) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(999, tx_bad);
    let bad_bind = bind_frame(999, 1, encode_bind_meta("durable-replay-guard", false, 60));
    assert_eq!(replay_offline(&bad_bind, &shared).await, 0);
    assert!(rx_bad.try_recv().is_err());
    assert_eq!(sess.offline_len(), 1);
}

#[tokio::test]
async fn replay_qos1_full_inflight_leaves_overflow_in_offline_queue() {
    let shared = test_shared();
    let (sess, _, _, _) = shared.sessions.connect_bind_v5_in_tenant(
        broker_session::tenant::DEFAULT_TENANT_ID,
        "replay-cap",
        false,
        None,
        5,
        60,
        16,
        0,
        Some(broker_session::ConnectBindMeta {
            conn_id: 101,
            username: None,
            verified_username: None,
            is_superuser: false,
            peerhost: None,
            keepalive_secs: 60,
            client_alias_max: 0,
            will: None,
        }),
    );
    sess.set_max_inflight(1);
    sess.set_max_spill(1);
    for (topic, payload) in [
        ("cap/1", b"one".as_slice()),
        ("cap/2", b"two".as_slice()),
        ("cap/3", b"thr".as_slice()),
    ] {
        sess.push_offline(QueuedMessage {
            topic: Topic::new(topic).unwrap(),
            qos: QoS::AtLeastOnce,
            retain: false,
            payload: Bytes::from_static(payload),
            publish_at_ms: None,
        });
    }
    let (tx, mut rx) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(101, tx);
    let bind = bind_frame(101, 1, encode_bind_meta("replay-cap", false, 60));
    assert_eq!(replay_offline(&bind, &shared).await, 2);
    let f1 = rx.try_recv().expect("first replayed publish");
    let f2 = rx.try_recv().expect("second replayed publish");
    assert!(rx.try_recv().is_err());
    assert_eq!(sess.inflight_snapshot().len(), 1);
    assert_eq!(sess.offline_len(), 1);

    let pid_of = |f: &BrokerFrame| -> u16 {
        let tlen = u16::from_be_bytes([f.metadata[0], f.metadata[1]]) as usize;
        u16::from_be_bytes([f.metadata[2 + tlen], f.metadata[2 + tlen + 1]])
    };
    apply_puback(&puback_frame(101, 2, pid_of(&f1)), &shared);
    apply_puback(&puback_frame(101, 3, pid_of(&f2)), &shared);

    let f3 = rx.try_recv().expect("third message resumes after PUBACK");
    assert_eq!(f3.payload, Bytes::from_static(b"thr"));
    assert!(rx.try_recv().is_err());
    assert_eq!(sess.offline_len(), 0);
}

#[tokio::test]
async fn replay_respects_v5_receive_maximum_and_resumes_on_puback() {
    let shared = test_shared();
    let (sess, _, _, _) = shared.sessions.connect_bind_v5_in_tenant(
        broker_session::tenant::DEFAULT_TENANT_ID,
        "replay-v5-rm",
        false,
        None,
        5,
        60,
        16,
        0,
        Some(broker_session::ConnectBindMeta {
            conn_id: 102,
            username: None,
            verified_username: None,
            is_superuser: false,
            peerhost: None,
            keepalive_secs: 60,
            client_alias_max: 0,
            will: None,
        }),
    );
    sess.set_protocol_version(5);
    sess.set_client_receive_maximum(1);
    for (topic, payload) in [("rm/1", b"one".as_slice()), ("rm/2", b"two".as_slice())] {
        sess.push_offline(QueuedMessage {
            topic: Topic::new(topic).unwrap(),
            qos: QoS::AtLeastOnce,
            retain: false,
            payload: Bytes::from_static(payload),
            publish_at_ms: None,
        });
    }
    let (tx, mut rx) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(102, tx);
    let bind = bind_frame(102, 1, encode_bind_meta("replay-v5-rm", false, 60));
    assert_eq!(replay_offline(&bind, &shared).await, 1);
    let f1 = rx
        .try_recv()
        .expect("first message within Receive Maximum=1");
    assert!(rx.try_recv().is_err());
    assert_eq!(sess.offline_len(), 1);

    let tlen = u16::from_be_bytes([f1.metadata[0], f1.metadata[1]]) as usize;
    let pid1 = u16::from_be_bytes([f1.metadata[2 + tlen], f1.metadata[2 + tlen + 1]]);
    apply_puback(&puback_frame(102, 2, pid1), &shared);

    let f2 = rx.try_recv().expect("second message resumes after PUBACK");
    assert_eq!(f2.payload, Bytes::from_static(b"two"));
    assert!(rx.try_recv().is_err());
    assert_eq!(sess.offline_len(), 0);
}

#[tokio::test]
async fn replay_deferred_retransmissions_resume_before_offline_on_puback() {
    let shared = test_shared();
    let (sess, _, _, _) = shared.sessions.connect_bind_v5_in_tenant(
        broker_session::tenant::DEFAULT_TENANT_ID,
        "replay-retrans-rm",
        false,
        None,
        5,
        60,
        16,
        0,
        Some(broker_session::ConnectBindMeta {
            conn_id: 103,
            username: None,
            verified_username: None,
            is_superuser: false,
            peerhost: None,
            keepalive_secs: 60,
            client_alias_max: 0,
            will: None,
        }),
    );
    sess.set_protocol_version(5);
    sess.set_client_receive_maximum(1);
    assert!(sess.track_inflight(broker_session::InflightMessage {
        packet_id: 11,
        topic: Topic::new("rt/1").unwrap(),
        qos: QoS::AtLeastOnce,
        retain: false,
        payload: Bytes::from_static(b"r1"),
        enqueued_at: std::time::Instant::now(),
        publish_at_ms: None,
    }));
    assert!(sess.track_inflight(broker_session::InflightMessage {
        packet_id: 12,
        topic: Topic::new("rt/2").unwrap(),
        qos: QoS::AtLeastOnce,
        retain: false,
        payload: Bytes::from_static(b"r2"),
        enqueued_at: std::time::Instant::now(),
        publish_at_ms: None,
    }));
    sess.push_offline(QueuedMessage {
        topic: Topic::new("rt/3").unwrap(),
        qos: QoS::AtLeastOnce,
        retain: false,
        payload: Bytes::from_static(b"q3"),
        publish_at_ms: None,
    });
    let (tx, mut rx) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(103, tx);
    let bind = bind_frame(103, 1, encode_bind_meta("replay-retrans-rm", false, 60));
    assert_eq!(replay_offline(&bind, &shared).await, 1);
    let f1 = rx.try_recv().expect("first retransmission");
    assert_eq!(f1.payload, Bytes::from_static(b"r1"));
    assert!(rx.try_recv().is_err());
    assert_eq!(sess.offline_len(), 1);

    apply_puback(&puback_frame(103, 2, 11), &shared);
    let f2 = rx.try_recv().expect("second retransmission after PUBACK");
    assert_eq!(f2.payload, Bytes::from_static(b"r2"));
    assert!(rx.try_recv().is_err());
    assert_eq!(sess.offline_len(), 1);

    apply_puback(&puback_frame(103, 3, 12), &shared);
    let f3 = rx.try_recv().expect("queued message after retransmissions");
    assert_eq!(f3.payload, Bytes::from_static(b"q3"));
    assert!(rx.try_recv().is_err());
    assert_eq!(sess.offline_len(), 0);
}

#[tokio::test]
async fn replay_qos2_respects_v5_receive_maximum_and_resumes_on_pubcomp() {
    let shared = test_shared();
    let (sess, _, _, _) = shared.sessions.connect_bind_v5_in_tenant(
        broker_session::tenant::DEFAULT_TENANT_ID,
        "replay-qos2-rm",
        false,
        None,
        5,
        60,
        16,
        0,
        Some(broker_session::ConnectBindMeta {
            conn_id: 104,
            username: None,
            verified_username: None,
            is_superuser: false,
            peerhost: None,
            keepalive_secs: 60,
            client_alias_max: 0,
            will: None,
        }),
    );
    sess.set_protocol_version(5);
    sess.set_client_receive_maximum(1);
    for (topic, payload) in [("q2/1", b"one".as_slice()), ("q2/2", b"two".as_slice())] {
        sess.push_offline(QueuedMessage {
            topic: Topic::new(topic).unwrap(),
            qos: QoS::ExactlyOnce,
            retain: false,
            payload: Bytes::from_static(payload),
            publish_at_ms: None,
        });
    }
    let (tx, mut rx) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(104, tx);
    let bind = bind_frame(104, 1, encode_bind_meta("replay-qos2-rm", false, 60));
    assert_eq!(replay_offline(&bind, &shared).await, 1);
    let f1 = rx.try_recv().expect("first QoS 2 within Receive Maximum=1");
    assert_eq!(f1.payload, Bytes::from_static(b"one"));
    assert!(rx.try_recv().is_err());
    assert_eq!(sess.offline_len(), 1);
    assert_eq!(sess.qos2_outbound_len(), 1);

    let tlen = u16::from_be_bytes([f1.metadata[0], f1.metadata[1]]) as usize;
    let pid1 = u16::from_be_bytes([f1.metadata[2 + tlen], f1.metadata[2 + tlen + 1]]);
    let comp = BrokerFrame::new(
        OpCode::PubCompIn,
        104,
        2,
        Bytes::from(pid1.to_be_bytes().to_vec()),
        Bytes::new(),
    )
    .unwrap();
    apply_qos2_pubcomp(&comp, &shared);

    let f2 = rx.try_recv().expect("second QoS 2 resumes after PUBCOMP");
    assert_eq!(f2.payload, Bytes::from_static(b"two"));
    assert!(rx.try_recv().is_err());
    assert_eq!(sess.offline_len(), 0);
}

#[tokio::test]
async fn replay_qos2_negative_pubrec_releases_credit_and_advances_replay() {
    let shared = test_shared();
    let (sess, _, _, _) = shared.sessions.connect_bind_v5_in_tenant(
        broker_session::tenant::DEFAULT_TENANT_ID,
        "replay-qos2-neg-pubrec",
        false,
        None,
        5,
        60,
        16,
        0,
        Some(broker_session::ConnectBindMeta {
            conn_id: 105,
            username: None,
            verified_username: None,
            is_superuser: false,
            peerhost: None,
            keepalive_secs: 60,
            client_alias_max: 0,
            will: None,
        }),
    );
    sess.set_protocol_version(5);
    sess.set_client_receive_maximum(1);
    for (topic, payload) in [("q2n/1", b"one".as_slice()), ("q2n/2", b"two".as_slice())] {
        sess.push_offline(QueuedMessage {
            topic: Topic::new(topic).unwrap(),
            qos: QoS::ExactlyOnce,
            retain: false,
            payload: Bytes::from_static(payload),
            publish_at_ms: None,
        });
    }
    let (tx, mut rx) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(105, tx);
    let bind = bind_frame(
        105,
        1,
        encode_bind_meta("replay-qos2-neg-pubrec", false, 60),
    );
    assert_eq!(replay_offline(&bind, &shared).await, 1);
    let f1 = rx.try_recv().expect("first QoS 2 within Receive Maximum=1");
    assert_eq!(f1.payload, Bytes::from_static(b"one"));
    assert!(rx.try_recv().is_err());
    assert_eq!(sess.offline_len(), 1);
    assert_eq!(sess.qos2_outbound_len(), 1);

    let tlen = u16::from_be_bytes([f1.metadata[0], f1.metadata[1]]) as usize;
    let pid1 = u16::from_be_bytes([f1.metadata[2 + tlen], f1.metadata[2 + tlen + 1]]);
    let mut neg_meta = pid1.to_be_bytes().to_vec();
    neg_meta.push(0x80);
    let neg_rec = BrokerFrame::new(
        OpCode::PubRecIn,
        105,
        2,
        Bytes::from(neg_meta),
        Bytes::new(),
    )
    .unwrap();
    assert!(apply_qos2_pubrec(&neg_rec, &shared).is_none());

    let f2 = rx
        .try_recv()
        .expect("second QoS 2 resumes after negative PUBREC");
    assert_eq!(f2.payload, Bytes::from_static(b"two"));
    assert!(rx.try_recv().is_err());
    assert_eq!(sess.offline_len(), 0);
}

#[tokio::test]
async fn retained_qos1_respects_v5_receive_maximum() {
    let shared = test_shared();
    let (sess, _, _, _) = shared.sessions.connect_bind_v5_in_tenant(
        broker_session::tenant::DEFAULT_TENANT_ID,
        "retained-v5-rm",
        false,
        None,
        5,
        60,
        16,
        0,
        Some(broker_session::ConnectBindMeta {
            conn_id: 106,
            username: None,
            verified_username: None,
            is_superuser: false,
            peerhost: None,
            keepalive_secs: 60,
            client_alias_max: 0,
            will: None,
        }),
    );
    sess.set_protocol_version(5);
    sess.set_client_receive_maximum(1);
    shared
        .retained
        .set_retained_in_tenant(
            Topic::new("ret/1").unwrap(),
            QoS::AtLeastOnce,
            Bytes::from_static(b"r1"),
            broker_session::tenant::DEFAULT_TENANT_ID,
        )
        .await
        .unwrap();
    shared
        .retained
        .set_retained_in_tenant(
            Topic::new("ret/2").unwrap(),
            QoS::AtLeastOnce,
            Bytes::from_static(b"r2"),
            broker_session::tenant::DEFAULT_TENANT_ID,
        )
        .await
        .unwrap();

    // Pre-populate offline_queue to verify deferred retained matches do not
    // touch or evict durable offline entries.
    sess.push_offline(QueuedMessage {
        topic: Topic::new("offline/keep").unwrap(),
        qos: QoS::AtLeastOnce,
        retain: false,
        payload: Bytes::from_static(b"keep"),
        publish_at_ms: None,
    });
    let (tx, mut rx) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(106, tx);
    let sub = subscribe_frame(
        106,
        1,
        encode_subscribe_meta_v5(1, "retained-v5-rm", &[("ret/+", 1, 42)]),
    );
    let (reply, retained) = apply_subscribe(&sub, &shared).await;
    assert!(reply.is_some());
    assert_eq!(retained.len(), 1);
    assert_eq!(sess.inflight_len(), 1);
    assert_eq!(sess.pending_retained_len(), 1);
    assert_eq!(sess.offline_len(), 1);

    let f1 = &retained[0];
    let tlen = u16::from_be_bytes([f1.metadata[0], f1.metadata[1]]) as usize;
    let pid1 = u16::from_be_bytes([f1.metadata[2 + tlen], f1.metadata[2 + tlen + 1]]);
    // Verify Subscription Identifier 42 is preserved on the deferred frame.
    apply_puback(&puback_frame(106, 2, pid1), &shared);

    let f2 = rx
        .try_recv()
        .expect("second retained message resumes after PUBACK");
    assert!(rx.try_recv().is_err());
    assert_eq!(sess.pending_retained_len(), 0);
    assert_ne!(f2.payload, f1.payload);
    let tlen2 = u16::from_be_bytes([f2.metadata[0], f2.metadata[1]]) as usize;
    let sub_id_offset = 2 + tlen2 + 2 + 3 + 2;
    assert_eq!(
        u32::from_be_bytes([
            f2.metadata[sub_id_offset],
            f2.metadata[sub_id_offset + 1],
            f2.metadata[sub_id_offset + 2],
            f2.metadata[sub_id_offset + 3],
        ]),
        42
    );

    // Next PUBACK drains the pre-existing offline message untouched.
    let pid2 = u16::from_be_bytes([f2.metadata[2 + tlen2], f2.metadata[2 + tlen2 + 1]]);
    apply_puback(&puback_frame(106, 3, pid2), &shared);
    let f3 = rx
        .try_recv()
        .expect("offline message preserved and delivered");
    assert_eq!(f3.payload, Bytes::from_static(b"keep"));
    assert_eq!(sess.offline_len(), 0);
}

#[tokio::test]
async fn deferred_retained_respects_max_packet_size_on_resume() {
    let shared = test_shared();
    let (sess, _, _, _) = shared.sessions.connect_bind_v5_in_tenant(
        broker_session::tenant::DEFAULT_TENANT_ID,
        "retained-max-pkt",
        false,
        None,
        5,
        60,
        16,
        0,
        Some(broker_session::ConnectBindMeta {
            conn_id: 109,
            username: None,
            verified_username: None,
            is_superuser: false,
            peerhost: None,
            keepalive_secs: 60,
            client_alias_max: 0,
            will: None,
        }),
    );
    sess.set_protocol_version(5);
    sess.set_client_receive_maximum(1);
    sess.set_client_max_packet_size(64);
    assert_eq!(
        sess.defer_retained_for_conn(
            Some(109),
            100,
            vec![
                broker_session::PendingRetained {
                    topic: Topic::new("rp/big").unwrap(),
                    qos: QoS::AtLeastOnce,
                    payload: Bytes::from(vec![b'x'; 128]),
                    sub_id: 7,
                },
                broker_session::PendingRetained {
                    topic: Topic::new("rp/ok").unwrap(),
                    qos: QoS::AtLeastOnce,
                    payload: Bytes::from_static(b"ok"),
                    sub_id: 7,
                },
            ],
        ),
        Some(0)
    );
    let (tx, mut rx) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(109, tx);
    let _guard = sess.lock_replay();
    let (sent, _) = resume_pending_retained_locked(&shared, &sess, 109, Some(109));
    assert_eq!(sent, 1);
    let f = rx.try_recv().expect("small retained delivered");
    assert_eq!(f.payload, Bytes::from_static(b"ok"));
    assert!(rx.try_recv().is_err());
    assert_eq!(sess.pending_retained_len(), 0);
}

#[tokio::test]
async fn superseded_subscribe_cannot_consume_replacement_retained_credit() {
    let shared = test_shared();
    let (sess, _, _, _) = shared.sessions.connect_bind_v5_in_tenant(
        broker_session::tenant::DEFAULT_TENANT_ID,
        "retained-superseded",
        false,
        None,
        5,
        60,
        16,
        0,
        Some(broker_session::ConnectBindMeta {
            conn_id: 107,
            username: None,
            verified_username: None,
            is_superuser: false,
            peerhost: None,
            keepalive_secs: 60,
            client_alias_max: 0,
            will: None,
        }),
    );
    sess.set_protocol_version(5);
    sess.set_client_receive_maximum(1);
    // Reconnect on conn_id 207 (superseding 107) on the same session.
    let _ = shared.sessions.connect_bind_v5_in_tenant(
        broker_session::tenant::DEFAULT_TENANT_ID,
        "retained-superseded",
        false,
        None,
        5,
        60,
        16,
        0,
        Some(broker_session::ConnectBindMeta {
            conn_id: 207,
            username: None,
            verified_username: None,
            is_superuser: false,
            peerhost: None,
            keepalive_secs: 60,
            client_alias_max: 0,
            will: None,
        }),
    );
    assert!(sess
        .try_alloc_qos1_downlink_v5_checked_for_conn(
            Some(107),
            Topic::new("ret/sup").unwrap(),
            QoS::AtLeastOnce,
            true,
            Bytes::from_static(b"stale"),
            std::time::Instant::now(),
        )
        .is_none());
    assert!(sess
        .defer_retained_for_conn(
            Some(107),
            100,
            vec![broker_session::PendingRetained {
                topic: Topic::new("ret/sup").unwrap(),
                qos: QoS::AtLeastOnce,
                payload: Bytes::from_static(b"stale"),
                sub_id: 1,
            }],
        )
        .is_none());

    // Bounded admission: exceeding max_msgs drops excess without
    // unbounded growth.
    assert_eq!(
        sess.defer_retained_for_conn(
            Some(207),
            1,
            vec![
                broker_session::PendingRetained {
                    topic: Topic::new("ret/b1").unwrap(),
                    qos: QoS::AtLeastOnce,
                    payload: Bytes::from_static(b"1"),
                    sub_id: 1,
                },
                broker_session::PendingRetained {
                    topic: Topic::new("ret/b2").unwrap(),
                    qos: QoS::AtLeastOnce,
                    payload: Bytes::from_static(b"2"),
                    sub_id: 1,
                },
            ],
        ),
        Some(1)
    );
    assert_eq!(sess.pending_retained_len(), 1);
    assert_eq!(sess.offline_len(), 0);
    assert_eq!(sess.unacked_on_conn_len(), 0);
}

#[tokio::test]
async fn deferred_retained_survives_same_session_reconnect() {
    let shared = test_shared();
    let (sess, _, _, _) = shared.sessions.connect_bind_v5_in_tenant(
        broker_session::tenant::DEFAULT_TENANT_ID,
        "retained-reconnect",
        false,
        None,
        5,
        60,
        16,
        0,
        Some(broker_session::ConnectBindMeta {
            conn_id: 110,
            username: None,
            verified_username: None,
            is_superuser: false,
            peerhost: None,
            keepalive_secs: 60,
            client_alias_max: 0,
            will: None,
        }),
    );
    sess.set_protocol_version(5);
    sess.set_client_receive_maximum(1);
    shared
        .retained
        .set_retained_in_tenant(
            Topic::new("rr/1").unwrap(),
            QoS::AtLeastOnce,
            Bytes::from_static(b"one"),
            broker_session::tenant::DEFAULT_TENANT_ID,
        )
        .await
        .unwrap();
    shared
        .retained
        .set_retained_in_tenant(
            Topic::new("rr/2").unwrap(),
            QoS::AtLeastOnce,
            Bytes::from_static(b"two"),
            broker_session::tenant::DEFAULT_TENANT_ID,
        )
        .await
        .unwrap();

    let sub = subscribe_frame(
        110,
        1,
        encode_subscribe_meta_v5(1, "retained-reconnect", &[("rr/+", 1, 9)]),
    );
    let (_, retained) = apply_subscribe(&sub, &shared).await;
    assert_eq!(retained.len(), 1);
    assert_eq!(sess.pending_retained_len(), 1);

    // Reconnect the same persistent session on conn_id 210.
    let (resumed, _, _, _) = shared.sessions.connect_bind_v5_in_tenant(
        broker_session::tenant::DEFAULT_TENANT_ID,
        "retained-reconnect",
        false,
        None,
        5,
        60,
        16,
        0,
        Some(broker_session::ConnectBindMeta {
            conn_id: 210,
            username: None,
            verified_username: None,
            is_superuser: false,
            peerhost: None,
            keepalive_secs: 60,
            client_alias_max: 0,
            will: None,
        }),
    );
    assert_eq!(resumed.id, sess.id);
    assert_eq!(resumed.pending_retained_len(), 1);
    resumed.set_protocol_version(5);
    resumed.set_client_receive_maximum(1);

    let (tx, mut rx) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(210, tx);
    let bind = bind_frame(210, 1, encode_bind_meta("retained-reconnect", false, 60));
    assert_eq!(replay_offline(&bind, &shared).await, 1);
    let f1 = rx
        .try_recv()
        .expect("first message retransmitted on new conn");
    assert!(rx.try_recv().is_err());

    let tlen1 = u16::from_be_bytes([f1.metadata[0], f1.metadata[1]]) as usize;
    let pid1 = u16::from_be_bytes([f1.metadata[2 + tlen1], f1.metadata[2 + tlen1 + 1]]);
    apply_puback(&puback_frame(210, 2, pid1), &shared);

    let f2 = rx
        .try_recv()
        .expect("deferred retained message preserved across reconnect and delivered on PUBACK");
    assert_ne!(f2.payload, f1.payload);
    assert_eq!(resumed.pending_retained_len(), 0);
}

#[tokio::test]
async fn deferred_retained_rejects_exact_encoded_packet_size_over_limit() {
    let shared = test_shared();
    let (sess, _, _, _) = shared.sessions.connect_bind_v5_in_tenant(
        broker_session::tenant::DEFAULT_TENANT_ID,
        "retained-exact-size",
        false,
        None,
        5,
        60,
        16,
        0,
        Some(broker_session::ConnectBindMeta {
            conn_id: 111,
            username: None,
            verified_username: None,
            is_superuser: false,
            peerhost: None,
            keepalive_secs: 60,
            client_alias_max: 0,
            will: None,
        }),
    );
    sess.set_protocol_version(5);
    sess.set_client_receive_maximum(2);
    // Topic "a" (1) + payload "bcd" (3) + 10 estimate = 14, whereas encoded
    // MQTT 5 QoS 1 PUBLISH with Subscription Identifier = 16 bytes.
    let sample = encode_retained_publish_out(
        111,
        &Topic::new("a").unwrap(),
        1,
        1,
        200_000,
        Bytes::from_static(b"bcd"),
    )
    .unwrap();
    let encoded_len = native::encoded_publish_out_len(&sample, 5).unwrap();
    assert!(encoded_len > 14);
    sess.set_client_max_packet_size((encoded_len - 1) as u32);

    assert_eq!(
        sess.defer_retained_for_conn(
            Some(111),
            100,
            vec![broker_session::PendingRetained {
                topic: Topic::new("a").unwrap(),
                qos: QoS::AtLeastOnce,
                payload: Bytes::from_static(b"bcd"),
                sub_id: 200_000,
            }],
        ),
        Some(0)
    );
    let (tx, mut rx) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(111, tx);
    let _guard = sess.lock_replay();
    let (sent, _) = resume_pending_retained_locked(&shared, &sess, 111, Some(111));
    assert_eq!(sent, 0);
    assert!(rx.try_recv().is_err());
    assert_eq!(sess.unacked_on_conn_len(), 0);
}

#[tokio::test]
async fn subscribe_returns_0x97_when_deferred_retained_exceeds_limit() {
    let shared = test_shared();
    shared.sessions.set_max_offline_queue(Some(1));
    let (sess, _, _, _) = shared.sessions.connect_bind_v5_in_tenant(
        broker_session::tenant::DEFAULT_TENANT_ID,
        "sub-0x97",
        false,
        None,
        5,
        60,
        16,
        0,
        Some(broker_session::ConnectBindMeta {
            conn_id: 112,
            username: None,
            verified_username: None,
            is_superuser: false,
            peerhost: None,
            keepalive_secs: 60,
            client_alias_max: 0,
            will: None,
        }),
    );
    sess.set_protocol_version(5);
    sess.set_client_receive_maximum(1);
    for (t, p) in [
        ("q97/1", b"1".as_slice()),
        ("q97/2", b"2".as_slice()),
        ("q97/3", b"3".as_slice()),
    ] {
        shared
            .retained
            .set_retained_in_tenant(
                Topic::new(t).unwrap(),
                QoS::AtLeastOnce,
                Bytes::from_static(p),
                broker_session::tenant::DEFAULT_TENANT_ID,
            )
            .await
            .unwrap();
    }
    let sub = subscribe_frame(
        112,
        1,
        encode_subscribe_meta_v5(1, "sub-0x97", &[("q97/+", 1, 0)]),
    );
    let (reply, retained) = apply_subscribe(&sub, &shared).await;
    let reply = reply.expect("suback");
    assert_eq!(
        reply.metadata.last().copied(),
        Some(V5_SUBACK_QUOTA_EXCEEDED)
    );
    assert!(retained.is_empty());
    assert!(!sess
        .subscriptions
        .read()
        .contains_key(&TopicFilter::new("q97/+").unwrap()));
    assert_eq!(sess.pending_retained_len(), 0);
}

#[tokio::test]
async fn replay_retransmission_drops_oversized_and_allows_exact_wire_size() {
    let shared = test_shared();
    let (sess, _, _, _) = shared.sessions.connect_bind_v5_in_tenant(
        broker_session::tenant::DEFAULT_TENANT_ID,
        "retrans-size",
        false,
        None,
        5,
        60,
        16,
        0,
        Some(broker_session::ConnectBindMeta {
            conn_id: 113,
            username: None,
            verified_username: None,
            is_superuser: false,
            peerhost: None,
            keepalive_secs: 60,
            client_alias_max: 0,
            will: None,
        }),
    );
    // Oversized QoS 1 retransmission (payload 32 bytes) + small QoS 1 offline
    // message ("a" + "bcd" -> 11 bytes encoded vs 14 bytes old estimate).
    assert!(sess.track_inflight(broker_session::InflightMessage {
        packet_id: 55,
        topic: Topic::new("big/1").unwrap(),
        qos: QoS::AtLeastOnce,
        retain: false,
        payload: Bytes::from(vec![b'z'; 32]),
        enqueued_at: std::time::Instant::now(),
        publish_at_ms: None,
    }));
    sess.push_offline(QueuedMessage {
        topic: Topic::new("a").unwrap(),
        qos: QoS::AtLeastOnce,
        retain: false,
        payload: Bytes::from_static(b"bcd"),
        publish_at_ms: None,
    });
    let small = encode_retained_publish_out(
        113,
        &Topic::new("a").unwrap(),
        1,
        1,
        0,
        Bytes::from_static(b"bcd"),
    )
    .unwrap();
    let small_wire = native::encoded_publish_out_len(&small, 5).unwrap();
    assert!(small_wire <= 12);

    sess.set_protocol_version(5);
    sess.set_client_receive_maximum(2);
    sess.set_client_max_packet_size(12);

    let (tx, mut rx) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(113, tx);
    let bind = bind_frame(113, 1, encode_bind_meta("retrans-size", false, 60));
    assert_eq!(replay_offline(&bind, &shared).await, 1);
    let f = rx
        .try_recv()
        .expect("11-byte packet allowed under 12-byte limit");
    assert_eq!(f.payload, Bytes::from_static(b"bcd"));
    assert!(rx.try_recv().is_err());
    // Oversized retransmission (pid 55) was released from inflight.
    assert!(!sess.inflight_snapshot().iter().any(|m| m.packet_id == 55));
}

#[tokio::test]
async fn subscribe_multi_filter_reserves_quota_and_excludes_oversized() {
    let shared = test_shared();
    shared.sessions.set_max_offline_queue(Some(1));
    let (sess, _, _, _) = shared.sessions.connect_bind_v5_in_tenant(
        broker_session::tenant::DEFAULT_TENANT_ID,
        "sub-multi-quota",
        false,
        None,
        5,
        60,
        16,
        0,
        Some(broker_session::ConnectBindMeta {
            conn_id: 114,
            username: None,
            verified_username: None,
            is_superuser: false,
            peerhost: None,
            keepalive_secs: 60,
            client_alias_max: 0,
            will: None,
        }),
    );
    sess.set_protocol_version(5);
    sess.set_client_receive_maximum(1);
    sess.set_client_max_packet_size(64);
    // Filter 1 ("m1/+") has 1 oversized (>64B) + 1 valid + 1 valid -> 1 sent, 1 deferred, oversized excluded.
    // Filter 2 ("m2/+") has 2 valid -> exceeds remaining pending_retained (0 slots left) -> 0x97.
    for (t, p) in [
        ("m1/big", vec![b'x'; 128]),
        ("m1/ok1", b"1".to_vec()),
        ("m1/ok2", b"2".to_vec()),
        ("m2/a", b"a".to_vec()),
        ("m2/b", b"b".to_vec()),
    ] {
        shared
            .retained
            .set_retained_in_tenant(
                Topic::new(t).unwrap(),
                QoS::AtLeastOnce,
                Bytes::from(p),
                broker_session::tenant::DEFAULT_TENANT_ID,
            )
            .await
            .unwrap();
    }
    let sub = subscribe_frame(
        114,
        1,
        encode_subscribe_meta_v5(1, "sub-multi-quota", &[("m1/+", 1, 0), ("m2/+", 1, 0)]),
    );
    let (reply, retained) = apply_subscribe(&sub, &shared).await;
    let reply = reply.expect("suback");
    let n = reply.metadata.len();
    assert_eq!(reply.metadata[n - 2], 1); // m1/+ granted QoS 1
    assert_eq!(reply.metadata[n - 1], V5_SUBACK_QUOTA_EXCEEDED); // m2/+ rejected 0x97
    assert_eq!(retained.len(), 1);
    assert_eq!(sess.pending_retained_len(), 1);
    assert!(sess
        .subscriptions
        .read()
        .contains_key(&TopicFilter::new("m1/+").unwrap()));
    assert!(!sess
        .subscriptions
        .read()
        .contains_key(&TopicFilter::new("m2/+").unwrap()));
}

#[tokio::test]
async fn resubscribe_rejected_0x97_preserves_existing_qos_and_options() {
    let shared = test_shared();
    shared.sessions.set_max_offline_queue(Some(1));
    let (sess, _, _, _) = shared.sessions.connect_bind_v5_in_tenant(
        broker_session::tenant::DEFAULT_TENANT_ID,
        "resub-0x97",
        false,
        None,
        5,
        60,
        16,
        0,
        Some(broker_session::ConnectBindMeta {
            conn_id: 115,
            username: None,
            verified_username: None,
            is_superuser: false,
            peerhost: None,
            keepalive_secs: 60,
            client_alias_max: 0,
            will: None,
        }),
    );
    sess.set_protocol_version(5);
    sess.set_client_receive_maximum(1);
    let filter = TopicFilter::new("resub/+").unwrap();

    // Initial SUBSCRIBE at QoS 1 with rh=1 and sub_id=77 succeeds.
    let sub1 = subscribe_frame(
        115,
        1,
        encode_subscribe_meta_v5(1, "resub-0x97", &[("resub/+", 1 | (1 << 4), 77)]),
    );
    let (reply1, _) = apply_subscribe(&sub1, &shared).await;
    assert_eq!(reply1.unwrap().metadata.last().copied(), Some(1));

    for (t, p) in [
        ("resub/1", b"1".as_slice()),
        ("resub/2", b"2".as_slice()),
        ("resub/3", b"3".as_slice()),
    ] {
        shared
            .retained
            .set_retained_in_tenant(
                Topic::new(t).unwrap(),
                QoS::AtLeastOnce,
                Bytes::from_static(p),
                broker_session::tenant::DEFAULT_TENANT_ID,
            )
            .await
            .unwrap();
    }

    // Re-subscribe at QoS 2 with nl=1, rap=1, rh=0 exceeds retained quota -> 0x97.
    let sub2 = subscribe_frame(
        115,
        2,
        encode_subscribe_meta_v5(2, "resub-0x97", &[("resub/+", 2 | (1 << 2) | (1 << 3), 0)]),
    );
    let (reply2, retained2) = apply_subscribe(&sub2, &shared).await;
    assert_eq!(
        reply2.unwrap().metadata.last().copied(),
        Some(V5_SUBACK_QUOTA_EXCEEDED)
    );
    assert!(retained2.is_empty());

    // Existing session options and router QoS 1 must remain intact.
    let opts = sess
        .subscriptions
        .read()
        .get(&filter)
        .cloned()
        .expect("existing subscription preserved");
    assert_eq!(opts.qos, QoS::AtLeastOnce);
    assert_eq!(opts.nl, 0);
    assert_eq!(opts.rap, 0);
    assert_eq!(opts.rh, 1);
    let matches = shared.router.matches(&Topic::new("resub/1").unwrap());
    assert_eq!(matches.len(), 1);
    let kept = matches.iter().next().unwrap();
    assert_eq!(kept.qos, QoS::AtLeastOnce);
    assert_eq!(kept.subscription_id, 77);
}

#[tokio::test]
async fn superseded_conn_cannot_unsubscribe_replacement_routes() {
    let shared = test_shared();
    let _ = shared.sessions.connect_bind_v5_in_tenant(
        broker_session::tenant::DEFAULT_TENANT_ID,
        "unsub-race",
        false,
        Some(Some("alice")),
        5,
        60,
        16,
        0,
        Some(broker_session::ConnectBindMeta {
            conn_id: 100,
            username: Some("alice".to_string()),
            verified_username: Some("alice".to_string()),
            is_superuser: false,
            peerhost: None,
            keepalive_secs: 60,
            client_alias_max: 0,
            will: None,
        }),
    );
    let replacement = shared.sessions.connect_bind_v5_in_tenant(
        broker_session::tenant::DEFAULT_TENANT_ID,
        "unsub-race",
        false,
        Some(Some("bob")),
        5,
        60,
        16,
        0,
        Some(broker_session::ConnectBindMeta {
            conn_id: 200,
            username: Some("bob".to_string()),
            verified_username: Some("bob".to_string()),
            is_superuser: false,
            peerhost: None,
            keepalive_secs: 60,
            client_alias_max: 0,
            will: None,
        }),
    );
    let filter = TopicFilter::new("sensors/race".to_string()).unwrap();
    let sub_frame = subscribe_frame(
        200,
        1,
        encode_subscribe_meta(1, "unsub-race", &[("sensors/race", 1)]),
    );
    let (sub_reply, _) = apply_subscribe(&sub_frame, &shared).await;
    assert!(sub_reply.is_some());

    let mut meta = Vec::new();
    meta.extend_from_slice(&1u16.to_be_bytes());
    meta.extend_from_slice(&(b"unsub-race".len() as u16).to_be_bytes());
    meta.extend_from_slice(b"unsub-race");
    meta.extend_from_slice(&1u16.to_be_bytes());
    meta.extend_from_slice(&(b"sensors/race".len() as u16).to_be_bytes());
    meta.extend_from_slice(b"sensors/race");
    let frame = BrokerFrame::new(
        OpCode::UnsubscribeIn,
        100,
        0,
        Bytes::from(meta),
        Bytes::new(),
    )
    .unwrap();
    let reply = apply_unsubscribe(&frame, &shared).await.unwrap();
    assert_eq!(
        reply.metadata.last().copied(),
        Some(V5_UNSUBACK_NO_SUBSCRIPTION)
    );
    assert!(replacement.0.subscriptions.read().contains_key(&filter));
    assert!(
        !shared
            .router
            .matches(&Topic::new("sensors/race").unwrap())
            .is_empty(),
        "superseded connection must not remove replacement session's route"
    );
}

#[tokio::test]
async fn superseded_conn_cannot_subscribe_as_replacement_owner() {
    let shared = test_shared();
    let _ = shared.sessions.connect_bind_v5_in_tenant(
        broker_session::tenant::DEFAULT_TENANT_ID,
        "sub-race",
        false,
        Some(Some("alice")),
        5,
        60,
        16,
        0,
        Some(broker_session::ConnectBindMeta {
            conn_id: 100,
            username: Some("alice".to_string()),
            verified_username: Some("alice".to_string()),
            is_superuser: false,
            peerhost: None,
            keepalive_secs: 60,
            client_alias_max: 0,
            will: None,
        }),
    );
    let _ = shared.sessions.connect_bind_v5_in_tenant(
        broker_session::tenant::DEFAULT_TENANT_ID,
        "sub-race",
        false,
        Some(Some("bob")),
        5,
        60,
        16,
        0,
        Some(broker_session::ConnectBindMeta {
            conn_id: 200,
            username: Some("bob".to_string()),
            verified_username: Some("bob".to_string()),
            is_superuser: true,
            peerhost: None,
            keepalive_secs: 60,
            client_alias_max: 0,
            will: None,
        }),
    );

    let frame = subscribe_frame(
        100,
        1,
        encode_subscribe_meta(1, "sub-race", &[("secret/admin", 1)]),
    );
    let (reply, retained) = apply_subscribe(&frame, &shared).await;
    assert!(retained.is_empty());
    let reply = reply.expect("suback reply");
    assert_eq!(reply.header.opcode, OpCode::SubAckOut);
    assert_eq!(
        reply.metadata.as_ref().last().copied(),
        Some(V5_SUBACK_UNSPECIFIED)
    );
    assert!(shared
        .router
        .matches(&Topic::new("secret/admin").unwrap())
        .is_empty());
}
