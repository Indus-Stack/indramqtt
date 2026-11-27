use super::*;

#[tokio::test]
async fn test_clients_lists_connected_ids() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, body) = server.get_auth("/api/v1/clients", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!([]));

    state.sessions.get_or_create("client-b", true);
    state.sessions.get_or_create("client-a", true);
    let (status, body) = server.get_auth("/api/v1/clients", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body, json!(["client-a", "client-b"]));
}

#[tokio::test]
async fn kick_sends_connclose_frame() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // A connected fake session with kernel state bound.
    let conn_id = 4242u64;
    let (session, _) = state.sessions.get_or_create("kick-me", true);
    *session.conn_id.write() = Some(conn_id);
    *session.connected.write() = true;

    let (status, _) = server.delete_auth("/api/v5/clients/kick-me", &token).await;
    assert_eq!(status, 204);

    // The kernel→edge close for that connection arrives on the
    // channel receiver.
    let mut edge_rx = state.edge_rx.lock().await;
    let frame = tokio::time::timeout(std::time::Duration::from_secs(5), edge_rx.recv())
        .await
        .expect("ConnClose arrives on the channel receiver")
        .expect("channel stays open");
    assert_eq!(frame.header.opcode, brokerlink::OpCode::ConnClose);
    assert_eq!(frame.header.conn_id, conn_id);
    assert!(frame.metadata.is_empty());
    assert!(frame.payload.is_empty());

    // Kernel state is torn down as before.
    assert_eq!(*session.conn_id.read(), None);
    assert!(!*session.connected.read());
}

#[tokio::test]
async fn clients_list_is_paged_with_envelope() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    state.sessions.get_or_create("w1-03-a", true);
    state.sessions.get_or_create("w1-03-b", true);
    state.sessions.get_or_create("w1-03-c", true);

    // Full list carries both clients plus paging metadata.
    let (status, body) = server.get_auth("/api/v5/clients", &token).await;
    assert_eq!(status, 200);
    let ids: Vec<&str> = body["data"]
        .as_array()
        .expect("data is a list")
        .iter()
        .map(|row| row["clientid"].as_str().expect("clientid"))
        .collect();
    assert_eq!(ids, vec!["w1-03-a", "w1-03-b", "w1-03-c"]);
    assert_eq!(body["meta"]["page"], json!(1));
    assert_eq!(body["meta"]["limit"], json!(100));
    assert_eq!(body["meta"]["count"], json!(3));
    assert_eq!(body["meta"]["hasnext"], json!(false));
    assert_eq!(body["data"][0]["connected"], json!(true));

    // Documented paging params are honoured.
    let (status, body) = server
        .get_auth("/api/v5/clients?page=1&limit=2", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["data"].as_array().expect("data").len(), 2);
    assert_eq!(body["meta"]["count"], json!(3));
    assert_eq!(body["meta"]["hasnext"], json!(true));
    let (status, body) = server
        .get_auth("/api/v5/clients?page=2&limit=2", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["data"].as_array().expect("data").len(), 1);
    assert_eq!(body["data"][0]["clientid"], json!("w1-03-c"));
    assert_eq!(body["meta"]["hasnext"], json!(false));

    // Unknown params are ignored gracefully, not rejected.
    let (status, body) = server
        .get_auth("/api/v5/clients?unknown_param=1", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["meta"]["count"], json!(3));

    // Malformed paging falls back to defaults, never 400.
    let (status, body) = server
        .get_auth("/api/v5/clients?page=abc&limit=xyz", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["meta"]["page"], json!(1));
    assert_eq!(body["meta"]["limit"], json!(100));
    assert_eq!(body["meta"]["count"], json!(3));
}

#[tokio::test]
async fn clients_list_filters_before_paginating() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    state.sessions.get_or_create("w1-03-f1", true);
    state.sessions.get_or_create("w1-03-f2", true);

    // The documented id filter narrows the page and the count.
    let (status, body) = server
        .get_auth("/api/v5/clients?clientid=w1-03-f2", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["data"].as_array().expect("data").len(), 1);
    assert_eq!(body["data"][0]["clientid"], json!("w1-03-f2"));
    assert_eq!(body["meta"]["count"], json!(1));

    // Filtering applies before the page window: the match on the
    // second row is still found on the first page.
    let (status, body) = server
        .get_auth("/api/v5/clients?clientid=w1-03-f2&page=1&limit=1", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["data"].as_array().expect("data").len(), 1);
    assert_eq!(body["meta"]["count"], json!(1));
    assert_eq!(body["meta"]["hasnext"], json!(false));

    // A filter with no match yields an empty page, not an error.
    let (status, body) = server
        .get_auth("/api/v5/clients?clientid=no-such-client", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["data"], json!([]));
    assert_eq!(body["meta"]["count"], json!(0));
}

#[tokio::test]
async fn clients_kick_removes_from_list() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    for (id, conn) in [("w1-03-k1", 9101u64), ("w1-03-k2", 9102u64)] {
        let (session, _) = state.sessions.get_or_create(id, true);
        *session.conn_id.write() = Some(conn);
        *session.connected.write() = true;
    }

    let (status, body) = server.get_auth("/api/v5/clients", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["meta"]["count"], json!(2));

    let (status, _) = server.delete_auth("/api/v5/clients/w1-03-k1", &token).await;
    assert_eq!(status, 204);

    let (status, body) = server.get_auth("/api/v5/clients", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["meta"]["count"], json!(1));
    assert_eq!(body["data"][0]["clientid"], json!("w1-03-k2"));
}

#[tokio::test]
async fn kick_unknown_client_is_not_found() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, body) = server
        .delete_auth("/api/v5/clients/no-such-client", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("CLIENTID_NOT_FOUND"));
    assert!(body["message"].is_string());
}

#[tokio::test]
async fn bulk_kick_mixed_known_unknown_answers_204_and_skips_the_unknown() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    for (id, conn) in [("w1-04-a", 9201u64), ("w1-04-b", 9202u64)] {
        let (session, _) = state.sessions.get_or_create(id, true);
        *session.conn_id.write() = Some(conn);
        *session.connected.write() = true;
    }

    // The reference API documents 204 for every bulk kick. An id with
    // no live connection is skipped and does not change the answer.
    let (status, _) = server
        .post_auth(
            "/api/v5/clients/kickout/bulk",
            json!(["w1-04-a", "w1-04-b", "w1-04-missing"]),
            &token,
        )
        .await;
    assert_eq!(status, 204);

    // Both known clients are disconnected and gone from the list.
    let (status, body) = server.get_auth("/api/v5/clients", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["meta"]["count"], json!(0));
    assert_eq!(body["data"], json!([]));
}

#[tokio::test]
async fn bulk_kick_malformed_body_is_bad_request() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, body) = server
        .post_auth(
            "/api/v5/clients/kickout/bulk",
            json!({"clientids": ["w1-04-a"]}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    assert!(body["message"].is_string());

    let (status, body) = server
        .post_auth("/api/v5/clients/kickout/bulk", json!([123]), &token)
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    assert!(body["message"].is_string());
}

#[tokio::test]
async fn inflight_lists_staged_delivery_empty_and_unknown() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    state.sessions.get_or_create("w1-06-a", true);

    // Empty inflight reads as an empty page, not an error.
    let (status, body) = server
        .get_auth("/api/v5/clients/w1-06-a/inflight_messages", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["data"], json!([]));
    assert_eq!(body["meta"]["count"], json!(0));
    assert_eq!(body["meta"]["hasnext"], json!(false));

    // Stage one unacknowledged QoS 1 downlink over the real tracker.
    let session = state.sessions.get("w1-06-a").expect("known session");
    assert!(session.track_inflight(broker_session::InflightMessage {
        packet_id: 7,
        topic: Topic::new("conf/inflight/1").expect("valid topic"),
        qos: QoS::AtLeastOnce,
        retain: false,
        payload: bytes::Bytes::from_static(b"hello"),
        enqueued_at: std::time::Instant::now(),
        publish_at_ms: None,
    }));

    let (status, body) = server
        .get_auth("/api/v5/clients/w1-06-a/inflight_messages", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["meta"]["count"], json!(1));
    let entry = &body["data"][0];
    assert_eq!(entry["topic"], json!("conf/inflight/1"));
    assert_eq!(entry["qos"], json!(1));
    assert_eq!(
        entry["msgid"],
        json!("7"),
        "inflight entry carries the downlink packet id as msgid: {entry}"
    );
    assert!(entry["payload"].is_string());

    // Unknown clients give not-found with the documented shape.
    let (status, body) = server
        .get_auth("/api/v5/clients/no-such-client/inflight_messages", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("CLIENTID_NOT_FOUND"));
    assert!(body["message"].is_string());
}

#[tokio::test]
async fn inflight_is_paged_with_w0_helper() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    state.sessions.get_or_create("w1-06-p", true);
    let session = state.sessions.get("w1-06-p").expect("known session");
    for pid in [11u16, 12, 13] {
        assert!(session.track_inflight(broker_session::InflightMessage {
            packet_id: pid,
            topic: Topic::new("conf/inflight/p").expect("valid topic"),
            qos: QoS::AtLeastOnce,
            retain: false,
            payload: bytes::Bytes::from_static(b"x"),
            enqueued_at: std::time::Instant::now(),
            publish_at_ms: None,
        }));
    }

    let (status, body) = server
        .get_auth(
            "/api/v5/clients/w1-06-p/inflight_messages?page=1&limit=2",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["data"].as_array().expect("data").len(), 2);
    assert_eq!(body["meta"]["count"], json!(3));
    assert_eq!(body["meta"]["hasnext"], json!(true));

    let (status, body) = server
        .get_auth(
            "/api/v5/clients/w1-06-p/inflight_messages?page=2&limit=2",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["data"].as_array().expect("data").len(), 1);
    assert_eq!(body["meta"]["hasnext"], json!(false));
}

#[tokio::test]
async fn mqueue_lists_staged_offline_empty_and_unknown() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    state.sessions.get_or_create("w1-07-a", true);

    // Empty queue reads as an empty page, not an error.
    let (status, body) = server
        .get_auth("/api/v5/clients/w1-07-a/mqueue_messages", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["data"], json!([]));
    assert_eq!(body["meta"]["count"], json!(0));
    assert_eq!(body["meta"]["hasnext"], json!(false));

    // Hold one message for the offline client over the real queue.
    let session = state.sessions.get("w1-07-a").expect("known session");
    session.push_offline(broker_session::QueuedMessage {
        topic: Topic::new("conf/mqueue/1").expect("valid topic"),
        qos: QoS::AtLeastOnce,
        retain: false,
        payload: bytes::Bytes::from_static(b"hello"),
        publish_at_ms: None,
    });

    let (status, body) = server
        .get_auth("/api/v5/clients/w1-07-a/mqueue_messages", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["meta"]["count"], json!(1));
    let entry = &body["data"][0];
    assert_eq!(entry["topic"], json!("conf/mqueue/1"));
    assert_eq!(entry["qos"], json!(1));
    assert!(entry["payload"].is_string());
    assert!(
        entry.get("msgid").is_none(),
        "queued entries hold no identifier and omit msgid: {entry}"
    );

    // Unknown clients give not-found with the documented shape.
    let (status, body) = server
        .get_auth("/api/v5/clients/no-such-client/mqueue_messages", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("CLIENTID_NOT_FOUND"));
    assert!(body["message"].is_string());
}

#[tokio::test]
async fn mqueue_is_paged_with_w0_helper() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    state.sessions.get_or_create("w1-07-p", true);
    let session = state.sessions.get("w1-07-p").expect("known session");
    for i in 0..3u8 {
        session.push_offline(broker_session::QueuedMessage {
            topic: Topic::new("conf/mqueue/p").expect("valid topic"),
            qos: QoS::AtLeastOnce,
            retain: false,
            payload: bytes::Bytes::from(vec![i]),
            publish_at_ms: None,
        });
    }

    let (status, body) = server
        .get_auth(
            "/api/v5/clients/w1-07-p/mqueue_messages?page=1&limit=2",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["data"].as_array().expect("data").len(), 2);
    assert_eq!(body["meta"]["count"], json!(3));
    assert_eq!(body["meta"]["hasnext"], json!(true));

    let (status, body) = server
        .get_auth(
            "/api/v5/clients/w1-07-p/mqueue_messages?page=2&limit=2",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["data"].as_array().expect("data").len(), 1);
    assert_eq!(body["meta"]["hasnext"], json!(false));
}

#[tokio::test]
async fn test_clients_detail_endpoint() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (session, _) = state.sessions.get_or_create("detail-1", false);
    *session.conn_id.write() = Some(4242);
    *session.keepalive_secs.write() = 60;
    state.sessions.add_subscription(
        "detail-1",
        TopicFilter::new("sensors/+").unwrap(),
        QoS::AtLeastOnce,
    );

    let (status, body) = server.get_auth("/api/v1/clients/detail-1", &token).await;
    assert_eq!(status, 200);
    let info: broker_session::ClientInfo =
        serde_json::from_value(body).expect("detail row decodes");
    assert_eq!(info.client_id, "detail-1");
    assert_eq!(info.conn_id, Some(4242));
    assert_eq!(info.keepalive_secs, 60);
    assert!(!info.clean_start);
    assert!(info.connected);
    assert_eq!(info.subscriptions, vec!["sensors/+".to_string()]);

    let (status, _) = server.get_auth("/api/v1/clients/ghost", &token).await;
    assert_eq!(status, 404);
}

#[tokio::test]
async fn client_detail_reads_live_session_and_unknown_is_not_found() {
    // Detail read through the served handler: known id answers
    // the detail shape, unknown id answers the documented 404.
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    state.sessions.get_or_create("ap07-detail-1", true);

    let (status, body) = server
        .get_auth("/api/v5/clients/ap07-detail-1", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["clientid"], json!("ap07-detail-1"));
    assert!(body["keepalive"].is_number());
    assert_eq!(body["proto_name"], json!("MQTT"));

    let (status, body) = server
        .get_auth("/api/v5/clients/ap07-detail-missing", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("CLIENTID_NOT_FOUND"));
}

#[tokio::test]
async fn client_detail_reads_mqtt_connected_session() {
    // Detail read finds a kernel-bind session and a console
    // session. Unknown ids still answer 404.
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // Kernel-bind session: same calls as the edge bind path
    // (create in tenant, pin the connection, record keepalive).
    let (bind_session, _, _) = state.sessions.get_or_create_v5_in_tenant(
        broker_session::tenant::DEFAULT_TENANT_ID,
        "ap03-bind-1",
        true,
        4,
        0,
        100,
        65536,
    );
    state.sessions.bind_session(&bind_session, 9101);
    *bind_session.keepalive_secs.write() = 60;
    let (status, body) = server.get_auth("/api/v5/clients/ap03-bind-1", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["clientid"], json!("ap03-bind-1"));
    assert_eq!(body["connected"], json!(true));
    // Console session over the live socket.
    let mut client = WsClient::connect(server.port).await;
    client
        .send_bin(&mqtt_connect("ap03-ws-1", None, None))
        .await;
    let connack = client.recv_msg().await.expect("connack");
    assert_eq!(mqtt_packet_type(&connack), 2);
    assert_eq!(connack[3], 0);
    let (status, body) = server.get_auth("/api/v5/clients/ap03-ws-1", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["clientid"], json!("ap03-ws-1"));
    assert_eq!(body["connected"], json!(true));
    let (status, body) = server
        .get_auth("/api/v5/clients/ap03-ws-missing", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("CLIENTID_NOT_FOUND"));
}

#[tokio::test]
async fn client_keepalive_write_returns_detail_shape() {
    // Hidden keepalive route: takes `interval`, answers the full
    // detail shape with the new value applied to live state.
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    state.sessions.get_or_create("ap07-ka-1", true);

    let (status, body) = server
        .put_auth(
            "/api/v5/clients/ap07-ka-1/keepalive",
            json!({"interval": 90}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["clientid"], json!("ap07-ka-1"));
    assert_eq!(body["keepalive"], json!(90));

    let (status, body) = server.get_auth("/api/v5/clients/ap07-ka-1", &token).await;
    assert_eq!(status, 200);
    assert_eq!(body["keepalive"], json!(90));

    // Missing field is 400.
    let (status, body) = server
        .put_auth(
            "/api/v5/clients/ap07-ka-1/keepalive",
            json!({"keepalive": 90}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));

    // Out of range is 400: the wire field holds 0..=65535.
    let (status, body) = server
        .put_auth(
            "/api/v5/clients/ap07-ka-1/keepalive",
            json!({"interval": 70000}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));

    // Unknown id is 404 with the documented code.
    let (status, body) = server
        .put_auth(
            "/api/v5/clients/ap07-ka-missing/keepalive",
            json!({"interval": 60}),
            &token,
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("CLIENTID_NOT_FOUND"));
}
