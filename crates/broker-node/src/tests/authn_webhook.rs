use super::*;

/// Bind one live subscriber through the real router plus session
/// mirror (no management calls, no store-only shortcut).
#[cfg(test)]
fn webhook_test_subscriber(shared: &Shared, client_id: &str, conn_id: u64, filter_str: &str) {
    let (session, _) = shared.sessions.get_or_create(client_id, true);
    *session.connected.write() = true;
    *session.conn_id.write() = Some(conn_id);
    shared.sessions.bind_session(&session, conn_id);
    let filter = TopicFilter::new(filter_str).expect("filter");
    shared.router.subscribe(
        &filter,
        Subscription {
            client_id: client_id.into(),
            conn_id,
            qos: QoS::AtMostOnce,
            group: None,
            tenant: broker_router::DEFAULT_TENANT_ID.into(),
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
            subscription_id: 0,
        },
    );
}

/// B5-04: webhook allow/deny verdicts through the broker CONNECT
/// and publish paths. A real verdict endpoint serves on loopback;
/// CONNECTs go through `apply_bind` and publishes through
/// `apply_publish`, the same entry points the edge uses. Allow
/// connects and fans out; wrong credentials are refused with 0x86
/// (the endpoint checks what it receives); a flipped publish
/// verdict refuses delivery with a 0x87 ack like an ACL denial.
#[tokio::test]
async fn webhook_allow_verdict_connects_and_publishes_through_broker() {
    let server = Arc::new(WebhookTestServer::new("alice", b"alicepw"));
    let (handle, url) = start_webhook_test_server(server.clone()).await;
    let mut shared = test_shared();
    shared.webhook = Some(Arc::new(WebhookAuth::new(webhook_test_config(url))));

    // CONNECT with the credential the endpoint expects: accepted.
    let frame = bind_frame(
        701,
        1,
        encode_bind_meta_creds("webhook-pub-1", true, 60, "alice", b"alicepw"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0, "webhook allow verdict must connect");
    assert!(shared.sessions.get("webhook-pub-1").is_some());

    webhook_test_subscriber(&shared, "webhook-sub-1", 702, "webhook/test");

    // Publish through the real kernel publish path: allowed, fanned out.
    let (meta, payload) = encode_publish_meta("webhook/test", 1, 0, false, b"hello");
    let (ack, deliveries) = apply_publish(&publish_frame(701, 2, meta, payload), &shared).await;
    assert!(ack.is_none(), "QoS 0 carries no ack");
    assert_eq!(
        deliveries.len(),
        1,
        "allowed publish must fan out to the subscriber"
    );

    // Wrong password: refused with 0x86, no session created.
    let frame = bind_frame(
        703,
        1,
        encode_bind_meta_creds("webhook-pub-2", true, 60, "alice", b"wrong"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x86, "webhook deny verdict must refuse CONNECT");
    assert!(shared.sessions.get("webhook-pub-2").is_none());

    // Publish verdict flipped to deny: the next publish on a fresh
    // topic (cache miss) is refused before delivery.
    *server.publish_allow.lock().unwrap() = false;
    let (meta, payload) = encode_publish_meta("webhook/denied", 1, 1, false, b"nope");
    let (ack, deliveries) = apply_publish(&publish_frame(701, 3, meta, payload), &shared).await;
    assert!(deliveries.is_empty(), "denied publish must not fan out");
    let ack = ack.expect("QoS 1 refused publish still acks");
    assert_eq!(
        ack.metadata[2], 0x87,
        "refused publish acks 0x87 like an ACL denial"
    );

    handle.abort();
}

/// B5-04: a dead webhook fails closed on both paths. CONNECT is
/// refused with 0x86; a session bound before the outage (a live
/// connection) can no longer publish: the publish path refuses with
/// 0x87 instead of allowing.
#[tokio::test]
async fn webhook_outage_fails_closed_through_broker() {
    // Closed loopback port: connection refused, no server.
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("ephemeral port");
    let addr = closed.local_addr().expect("local addr");
    drop(closed);
    let mut config = webhook_test_config(format!("http://{addr}"));
    config.request_timeout_ms = 1_000;
    let mut shared = test_shared();
    shared.webhook = Some(Arc::new(WebhookAuth::new(config)));

    let frame = bind_frame(
        711,
        1,
        encode_bind_meta_creds("webhook-down-1", true, 60, "alice", b"alicepw"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x86, "webhook outage must fail closed with 0x86");
    assert!(shared.sessions.get("webhook-down-1").is_none());

    let (session, _) = shared.sessions.get_or_create("webhook-down-live", true);
    *session.connected.write() = true;
    *session.conn_id.write() = Some(712);
    shared.sessions.bind_session(&session, 712);
    webhook_test_subscriber(&shared, "webhook-down-sub", 713, "webhook/down");
    let (meta, payload) = encode_publish_meta("webhook/down", 1, 1, false, b"x");
    let (ack, deliveries) = apply_publish(&publish_frame(712, 1, meta, payload), &shared).await;
    assert!(
        deliveries.is_empty(),
        "publish during an outage must not fan out"
    );
    assert_eq!(
        ack.expect("QoS 1 acks").metadata[2],
        0x87,
        "publish during an outage is refused like an ACL denial"
    );
}

/// B5-04: the breaker trips open under consecutive endpoint faults,
/// fails closed fast (no endpoint contact) while open on both the
/// CONNECT and the publish path, then recovers on the half-open
/// probe once the endpoint heals.
#[tokio::test]
async fn webhook_breaker_trips_and_recovers_through_broker() {
    let server = Arc::new(WebhookTestServer::new("alice", b"alicepw"));
    let (handle, url) = start_webhook_test_server(server.clone()).await;
    let mut config = webhook_test_config(url);
    config.breaker_failure_threshold = 2;
    config.breaker_reset_timeout_ms = 1_000;
    config.request_timeout_ms = 1_000;
    config.cache_ttl_secs = 0;
    let mut shared = test_shared();
    shared.webhook = Some(Arc::new(WebhookAuth::new(config)));

    *server.fail.lock().unwrap() = true;
    // Two consecutive faulting CONNECTs trip the breaker (the cache
    // is disabled so every CONNECT asks the endpoint).
    for (conn, client) in [(721u64, "webhook-flap-1"), (722, "webhook-flap-2")] {
        let frame = bind_frame(
            conn,
            1,
            encode_bind_meta_creds(client, true, 60, "alice", b"alicepw"),
        );
        let reply = apply_bind(&frame, &shared).await.expect("bind replies");
        let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
        assert_eq!(rc, 0x86, "faulting webhook must fail closed");
    }
    let webhook = shared.webhook.clone().expect("webhook configured");
    assert!(webhook.breaker_is_open(), "breaker must trip open");
    let hits = server.hits.load(Ordering::Relaxed);
    assert_eq!(hits, 2);

    // While open the CONNECT fails fast without endpoint contact.
    let frame = bind_frame(
        723,
        1,
        encode_bind_meta_creds("webhook-flap-3", true, 60, "alice", b"alicepw"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x86, "open circuit must refuse CONNECT");
    assert_eq!(
        server.hits.load(Ordering::Relaxed),
        hits,
        "open circuit must not contact the endpoint"
    );

    // A live session cannot publish while the circuit is open either.
    let (session, _) = shared.sessions.get_or_create("webhook-flap-live", true);
    *session.connected.write() = true;
    *session.conn_id.write() = Some(724);
    shared.sessions.bind_session(&session, 724);
    webhook_test_subscriber(&shared, "webhook-flap-sub", 725, "webhook/flap");
    let (meta, payload) = encode_publish_meta("webhook/flap", 1, 0, false, b"x");
    let (_, deliveries) = apply_publish(&publish_frame(724, 1, meta, payload), &shared).await;
    assert!(
        deliveries.is_empty(),
        "publish while the circuit is open must be refused"
    );
    assert_eq!(
        server.hits.load(Ordering::Relaxed),
        hits,
        "open circuit must not contact the endpoint for publishes"
    );

    // After the reset timeout the half-open probe recovers: with the
    // fault cleared the CONNECT is accepted and the breaker closes.
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    *server.fail.lock().unwrap() = false;
    let frame = bind_frame(
        726,
        1,
        encode_bind_meta_creds("webhook-flap-4", true, 60, "alice", b"alicepw"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(
        rc, 0,
        "half-open probe must recover once the endpoint heals"
    );
    assert!(
        !webhook.breaker_is_open(),
        "breaker must close after a good probe"
    );

    handle.abort();
}

/// B5-04: the verdict cache serves publishes without re-asking and
/// re-asks exactly once after the TTL expires, honoring the new
/// verdict. Exact endpoint-contact counts prove hit versus miss.
#[tokio::test]
async fn webhook_cache_ttl_reasks_through_broker() {
    let server = Arc::new(WebhookTestServer::new("alice", b"alicepw"));
    let (handle, url) = start_webhook_test_server(server.clone()).await;
    let mut config = webhook_test_config(url);
    config.cache_ttl_secs = 1;
    let mut shared = test_shared();
    shared.webhook = Some(Arc::new(WebhookAuth::new(config)));

    let frame = bind_frame(
        731,
        1,
        encode_bind_meta_creds("webhook-ttl-pub", true, 60, "alice", b"alicepw"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    assert_eq!(decode_session_binding_meta(&reply.metadata).2, 0);
    webhook_test_subscriber(&shared, "webhook-ttl-sub", 732, "webhook/ttl");
    let hits_after_bind = server.hits.load(Ordering::Relaxed);

    // First publish misses the cache and fans out.
    let (meta, payload) = encode_publish_meta("webhook/ttl", 1, 0, false, b"v1");
    let (_, deliveries) = apply_publish(&publish_frame(731, 2, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(server.hits.load(Ordering::Relaxed), hits_after_bind + 1);

    // Second publish hits the cache: no new endpoint contact.
    let (meta, payload) = encode_publish_meta("webhook/ttl", 1, 0, false, b"v2");
    let (_, deliveries) = apply_publish(&publish_frame(731, 3, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(
        server.hits.load(Ordering::Relaxed),
        hits_after_bind + 1,
        "cache hit must not re-ask the endpoint"
    );

    // Flip the verdict: the stale entry still allows until expiry.
    *server.publish_allow.lock().unwrap() = false;
    let (meta, payload) = encode_publish_meta("webhook/ttl", 1, 0, false, b"v3");
    let (_, deliveries) = apply_publish(&publish_frame(731, 4, meta, payload), &shared).await;
    assert_eq!(
        deliveries.len(),
        1,
        "stale verdict still allows before TTL expiry"
    );
    assert_eq!(server.hits.load(Ordering::Relaxed), hits_after_bind + 1);

    // Past the TTL the publish re-asks and honors the new deny.
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    let (meta, payload) = encode_publish_meta("webhook/ttl", 1, 0, false, b"v4");
    let (_, deliveries) = apply_publish(&publish_frame(731, 5, meta, payload), &shared).await;
    assert!(
        deliveries.is_empty(),
        "expired verdict must re-ask and refuse"
    );
    assert_eq!(
        server.hits.load(Ordering::Relaxed),
        hits_after_bind + 2,
        "TTL expiry must re-ask the endpoint exactly once"
    );

    handle.abort();
}

/// B5-04 qualification (QUAL-NONE: no vendor webhook-auth server
/// product exists, so the test serves its own verdict endpoint on
/// loopback with allow, deny, flap and stop faults plus breaker and
/// TTL assertions, and no container is needed).
///
/// Goes through the broker's CONNECT and publish paths (`apply_bind`
/// at `apply_bind` and `apply_publish` at `apply_publish`, the same
/// entry points the edge uses), never `authenticate` /
/// `authorize_publish` directly. Asserts exact endpoint-contact counts
/// by key and never skips: a missing loopback bind panics via
/// `expect`, and every fault asserts fail-closed. `#[ignore]` so
/// normal `cargo test` runs skip it and the pipeline's qualification
/// runner picks it up explicitly via
/// `QUAL-CMD: cargo +1.96.0 test -p broker-node --bin indramqtt test_qualify_webhook_ -- --ignored --nocapture --test-threads=1`
/// (the B5-03 `QUAL-CMD` precedent, scoped to this binary's
/// `test_qualify_` prefix).
#[tokio::test]
#[ignore]
async fn test_qualify_webhook_auth_and_publish_through_broker() {
    // Allow then deny verdicts through the broker CONNECT path, with
    // exact endpoint-contact counts.
    let server = Arc::new(WebhookTestServer::new("alice", b"alicepw"));
    let (handle, url) = start_webhook_test_server(server.clone()).await;
    let mut shared = test_shared();
    shared.webhook = Some(Arc::new(WebhookAuth::new(webhook_test_config(url))));
    let frame = bind_frame(
        801,
        1,
        encode_bind_meta_creds("qual-conn-1", true, 60, "alice", b"alicepw"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    assert_eq!(
        decode_session_binding_meta(&reply.metadata).2,
        0,
        "seeded webhook credential must connect through the broker"
    );
    assert_eq!(
        server.hits.load(Ordering::Relaxed),
        1,
        "allow CONNECT must contact the endpoint exactly once"
    );
    for (conn, client, pw, why) in [
        (802u64, "qual-conn-2", b"wrong".as_slice(), "wrong password"),
        (803u64, "qual-conn-3", b"alicepw".as_slice(), "unknown user"),
    ] {
        let user = if client == "qual-conn-3" {
            "mallory"
        } else {
            "alice"
        };
        let frame = bind_frame(conn, 1, encode_bind_meta_creds(client, true, 60, user, pw));
        let reply = apply_bind(&frame, &shared).await.expect("bind replies");
        assert_eq!(
            decode_session_binding_meta(&reply.metadata).2,
            0x86,
            "{why} must be refused through the broker"
        );
        assert!(shared.sessions.get(client).is_none());
    }
    assert_eq!(
        server.hits.load(Ordering::Relaxed),
        3,
        "each deny CONNECT must contact the endpoint exactly once"
    );
    webhook_test_subscriber(&shared, "qual-sub-1", 804, "qual/allowed");
    let (meta, payload) = encode_publish_meta("qual/allowed", 1, 0, false, b"v");
    let (_, deliveries) = apply_publish(&publish_frame(801, 2, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1, "allowed topic must fan out");
    assert_eq!(server.hits.load(Ordering::Relaxed), 4);
    *server.publish_allow.lock().unwrap() = false;
    let (meta, payload) = encode_publish_meta("qual/denied", 1, 1, false, b"no");
    let (ack, deliveries) = apply_publish(&publish_frame(801, 3, meta, payload), &shared).await;
    assert!(
        deliveries.is_empty(),
        "denied topic must be refused through the broker"
    );
    assert_eq!(ack.expect("QoS 1 acks").metadata[2], 0x87);
    assert_eq!(server.hits.load(Ordering::Relaxed), 5);
    *server.publish_allow.lock().unwrap() = true;
    handle.abort();

    // Stop fault: a dead endpoint fails closed on both broker paths.
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = closed.local_addr().expect("local addr");
    drop(closed);
    let mut down_config = webhook_test_config(format!("http://{addr}"));
    down_config.request_timeout_ms = 1_000;
    let mut down_shared = test_shared();
    down_shared.webhook = Some(Arc::new(WebhookAuth::new(down_config)));
    let frame = bind_frame(
        811,
        1,
        encode_bind_meta_creds("qual-down", true, 60, "alice", b"alicepw"),
    );
    let reply = apply_bind(&frame, &down_shared)
        .await
        .expect("bind replies");
    assert_eq!(
        decode_session_binding_meta(&reply.metadata).2,
        0x86,
        "stopped webhook must deny CONNECT through the broker"
    );
    let (session, _) = down_shared.sessions.get_or_create("qual-down-live", true);
    *session.connected.write() = true;
    *session.conn_id.write() = Some(812);
    down_shared.sessions.bind_session(&session, 812);
    webhook_test_subscriber(&down_shared, "qual-down-sub", 813, "qual/down");
    let (meta, payload) = encode_publish_meta("qual/down", 1, 1, false, b"x");
    let (ack, deliveries) =
        apply_publish(&publish_frame(812, 1, meta, payload), &down_shared).await;
    assert!(
        deliveries.is_empty(),
        "stopped webhook must refuse publishes through the broker"
    );
    assert_eq!(ack.expect("QoS 1 acks").metadata[2], 0x87);

    // Flap fault: consecutive failures trip the breaker; the open
    // circuit fails fast on both broker paths without endpoint
    // contact, then the half-open probe recovers.
    let flap = Arc::new(WebhookTestServer::new("alice", b"alicepw"));
    let (flap_handle, flap_url) = start_webhook_test_server(flap.clone()).await;
    let mut flap_config = webhook_test_config(flap_url);
    flap_config.breaker_failure_threshold = 2;
    flap_config.breaker_reset_timeout_ms = 1_000;
    flap_config.request_timeout_ms = 1_000;
    flap_config.cache_ttl_secs = 0;
    let mut flap_shared = test_shared();
    flap_shared.webhook = Some(Arc::new(WebhookAuth::new(flap_config)));
    *flap.fail.lock().unwrap() = true;
    for (conn, client) in [(821u64, "qual-flap-1"), (822, "qual-flap-2")] {
        let frame = bind_frame(
            conn,
            1,
            encode_bind_meta_creds(client, true, 60, "alice", b"alicepw"),
        );
        let reply = apply_bind(&frame, &flap_shared)
            .await
            .expect("bind replies");
        assert_eq!(
            decode_session_binding_meta(&reply.metadata).2,
            0x86,
            "faulting webhook must fail closed through the broker"
        );
    }
    let webhook = flap_shared.webhook.clone().expect("webhook configured");
    assert!(webhook.breaker_is_open(), "breaker must trip open");
    assert_eq!(flap.hits.load(Ordering::Relaxed), 2);
    let frame = bind_frame(
        823,
        1,
        encode_bind_meta_creds("qual-flap-fast", true, 60, "alice", b"alicepw"),
    );
    let reply = apply_bind(&frame, &flap_shared)
        .await
        .expect("bind replies");
    assert_eq!(
        decode_session_binding_meta(&reply.metadata).2,
        0x86,
        "open circuit must refuse CONNECT through the broker"
    );
    assert_eq!(
        flap.hits.load(Ordering::Relaxed),
        2,
        "open circuit must not contact the endpoint"
    );
    let (session, _) = flap_shared.sessions.get_or_create("qual-flap-live", true);
    *session.connected.write() = true;
    *session.conn_id.write() = Some(824);
    flap_shared.sessions.bind_session(&session, 824);
    webhook_test_subscriber(&flap_shared, "qual-flap-sub", 825, "qual/flap");
    let (meta, payload) = encode_publish_meta("qual/flap", 1, 0, false, b"x");
    let (_, deliveries) = apply_publish(&publish_frame(824, 1, meta, payload), &flap_shared).await;
    assert!(
        deliveries.is_empty(),
        "publish while the circuit is open must be refused"
    );
    assert_eq!(
        flap.hits.load(Ordering::Relaxed),
        2,
        "open circuit must not contact the endpoint for publishes"
    );
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    *flap.fail.lock().unwrap() = false;
    let frame = bind_frame(
        826,
        1,
        encode_bind_meta_creds("qual-flap-probe", true, 60, "alice", b"alicepw"),
    );
    let reply = apply_bind(&frame, &flap_shared)
        .await
        .expect("bind replies");
    assert_eq!(
        decode_session_binding_meta(&reply.metadata).2,
        0,
        "half-open probe must recover once the endpoint heals"
    );
    assert!(
        !webhook.breaker_is_open(),
        "breaker must close after a good probe"
    );
    flap_handle.abort();

    // TTL fault: expiry re-asks the endpoint exactly once through the
    // broker publish path and honors the new verdict.
    let ttl_server = Arc::new(WebhookTestServer::new("alice", b"alicepw"));
    let (ttl_handle, ttl_url) = start_webhook_test_server(ttl_server.clone()).await;
    let mut ttl_config = webhook_test_config(ttl_url);
    ttl_config.cache_ttl_secs = 1;
    let mut ttl_shared = test_shared();
    ttl_shared.webhook = Some(Arc::new(WebhookAuth::new(ttl_config)));
    let frame = bind_frame(
        831,
        1,
        encode_bind_meta_creds("qual-ttl-pub", true, 60, "alice", b"alicepw"),
    );
    let reply = apply_bind(&frame, &ttl_shared).await.expect("bind replies");
    assert_eq!(decode_session_binding_meta(&reply.metadata).2, 0);
    webhook_test_subscriber(&ttl_shared, "qual-ttl-sub", 832, "qual/ttl");
    let hits_after_bind = ttl_server.hits.load(Ordering::Relaxed);
    let (meta, payload) = encode_publish_meta("qual/ttl", 1, 0, false, b"v1");
    let (_, deliveries) = apply_publish(&publish_frame(831, 2, meta, payload), &ttl_shared).await;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(ttl_server.hits.load(Ordering::Relaxed), hits_after_bind + 1);
    let (meta, payload) = encode_publish_meta("qual/ttl", 1, 0, false, b"v2");
    let (_, deliveries) = apply_publish(&publish_frame(831, 3, meta, payload), &ttl_shared).await;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(
        ttl_server.hits.load(Ordering::Relaxed),
        hits_after_bind + 1,
        "cache hit must not re-ask the endpoint"
    );
    *ttl_server.publish_allow.lock().unwrap() = false;
    tokio::time::sleep(std::time::Duration::from_millis(1_100)).await;
    let (meta, payload) = encode_publish_meta("qual/ttl", 1, 0, false, b"v3");
    let (_, deliveries) = apply_publish(&publish_frame(831, 4, meta, payload), &ttl_shared).await;
    assert!(
        deliveries.is_empty(),
        "expired verdict must re-ask and refuse through the broker"
    );
    assert_eq!(
        ttl_server.hits.load(Ordering::Relaxed),
        hits_after_bind + 2,
        "TTL expiry must re-ask the endpoint exactly once"
    );
    ttl_handle.abort();
}

/// B5-04: the last will consults the webhook verdict through the
/// broker disconnect path. An allowed will topic fires to the
/// subscriber; once the verdict flips to deny, the next will on a
/// fresh topic is refused (no delivery). Drives bind, subscribe and
/// disconnect through the broker, never the store alone.
#[tokio::test]
async fn webhook_will_consults_verdict_through_broker() {
    let server = Arc::new(WebhookTestServer::new("alice", b"alicepw"));
    let (handle, url) = start_webhook_test_server(server.clone()).await;
    let mut shared = test_shared();
    shared.webhook = Some(Arc::new(WebhookAuth::new(webhook_test_config(url))));
    webhook_test_subscriber(&shared, "will-webhook-sub", 902, "webhook/will");

    // Publisher binds with a will plus the credential the endpoint
    // expects: the CONNECT verdict allows.
    let mut meta = encode_bind_meta_with_will(
        "will-webhook-pub",
        true,
        60,
        "webhook/will",
        b"gone",
        0,
        false,
    )
    .to_vec();
    meta.extend_from_slice(&("alice".len() as u16).to_be_bytes());
    meta.extend_from_slice(b"alice");
    meta.extend_from_slice(&(b"alicepw".len() as u16).to_be_bytes());
    meta.extend_from_slice(b"alicepw");
    let reply = apply_bind(&bind_frame(901, 1, Bytes::from(meta)), &shared)
        .await
        .expect("bind replies");
    assert_eq!(
        decode_session_binding_meta(&reply.metadata).2,
        0,
        "webhook allow verdict must connect a will publisher"
    );

    // Ungraceful close: the will consults the webhook and fires.
    let deliveries = apply_disconnect(&disconnect_frame(901, 2, "will-webhook-pub"), &shared).await;
    assert_eq!(deliveries.len(), 1, "allowed will must fire");
    assert_eq!(deliveries[0].1.payload, Bytes::from_static(b"gone"));

    // A second publisher's will on a fresh topic is refused once the
    // verdict flips to deny.
    *server.publish_allow.lock().unwrap() = false;
    let mut meta = encode_bind_meta_with_will(
        "will-webhook-pub2",
        true,
        60,
        "webhook/will-denied",
        b"no",
        0,
        false,
    )
    .to_vec();
    meta.extend_from_slice(&("alice".len() as u16).to_be_bytes());
    meta.extend_from_slice(b"alice");
    meta.extend_from_slice(&(b"alicepw".len() as u16).to_be_bytes());
    meta.extend_from_slice(b"alicepw");
    let reply = apply_bind(&bind_frame(903, 1, Bytes::from(meta)), &shared)
        .await
        .expect("bind replies");
    assert_eq!(
        decode_session_binding_meta(&reply.metadata).2,
        0,
        "CONNECT still allows; the will verdict applies at publish time"
    );
    let denied = apply_disconnect(&disconnect_frame(903, 2, "will-webhook-pub2"), &shared).await;
    assert!(
        denied.is_empty(),
        "denied will must not deliver through the broker"
    );

    handle.abort();
}

/// B5-04: per-packet authorization cost probe for the report. Times
/// cache hits (one lock plus hash lookup) against cache misses (one
/// bounded loopback round-trip) and prints `WEBHOOK_PUBLISH_COST`
/// with mean/max microseconds for the gate log, plus `BENCH`
/// hit/miss lines in the pipeline's `BENCH <metric> <value> <unit>`
/// shape so a bench runner picks them up. Asserts only the
/// verdicts, never the timings, so a loaded runner cannot flake it.
#[tokio::test]
async fn webhook_publish_cost_hit_and_miss() {
    let server = Arc::new(WebhookTestServer::new("alice", b"alicepw"));
    let (handle, url) = start_webhook_test_server(server.clone()).await;
    let auth = WebhookAuth::new(webhook_test_config(url));

    let warm = Topic::new("webhook/cost/0").expect("topic");
    auth.authorize_publish("cost-client", &warm)
        .await
        .expect("warmup allows");
    let mut hit_sum = 0u128;
    let mut hit_max = 0u128;
    for _ in 0..20 {
        let start = std::time::Instant::now();
        auth.authorize_publish("cost-client", &warm)
            .await
            .expect("hit allows");
        let micros = start.elapsed().as_micros();
        hit_sum += micros;
        hit_max = hit_max.max(micros);
    }
    let mut miss_sum = 0u128;
    let mut miss_max = 0u128;
    for n in 1..=20u32 {
        let fresh = Topic::new(format!("webhook/cost/{n}")).expect("topic");
        let start = std::time::Instant::now();
        auth.authorize_publish("cost-client", &fresh)
            .await
            .expect("miss allows");
        let micros = start.elapsed().as_micros();
        miss_sum += micros;
        miss_max = miss_max.max(micros);
    }
    eprintln!(
        "WEBHOOK_PUBLISH_COST hit_mean_us={} hit_max_us={} miss_mean_us={} miss_max_us={}",
        hit_sum / 20,
        hit_max,
        miss_sum / 20,
        miss_max
    );
    println!("BENCH webhook_publish_hit_mean_us {} us", hit_sum / 20);
    println!("BENCH webhook_publish_hit_max_us {hit_max} us");
    println!("BENCH webhook_publish_miss_mean_us {} us", miss_sum / 20);
    println!("BENCH webhook_publish_miss_max_us {miss_max} us");
    handle.abort();
}
