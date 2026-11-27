use super::*;

/// Shared JWKS fixtures (B5-02): see the crate-root `#[path]` include of
/// `crates/broker-auth/src/jwks_test_support.rs` above.
use crate::jwks_test_support::{
    TestJwksServer as JwksTestServer, TestSigningKey as JwksSigningKey,
};

/// B5-02: JWT CONNECT through the broker plus publish and deliver.
///
/// A valid token against the live loopback HTTPS endpoint is accepted
/// (rc 0, real session, username stamped with the verified subject),
/// then a publish through `apply_publish` reaches a subscriber bound
/// on the same broker: connect, publish and deliver with a JWT
/// identity, never through the store alone.
#[tokio::test]
async fn jwks_bind_through_broker_connect() {
    let signing = JwksSigningKey::generate("conn-key-a");
    let server = JwksTestServer::start(vec![signing.jwk_json()]).await;
    let shared = jwks_test_shared(server.url());
    let token = signing.mint("https://issuer.example", "indra-mqtt", 3600, false);

    let frame = bind_frame(
        101,
        1,
        encode_bind_meta_creds("jwt-device-1", true, 60, "jwt-user", token.as_bytes()),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0, "valid JWT CONNECT must be accepted");
    let session = shared
        .sessions
        .get("jwt-device-1")
        .expect("session created");
    assert_eq!(
        session.username.read().as_deref(),
        Some("device-1"),
        "username stamps the verified subject"
    );

    // Publish and deliver with the JWT identity on the same broker.
    let sub = subscribe_frame(
        101,
        2,
        encode_subscribe_meta(7, "jwt-device-1", &[("jwt/topic", 0)]),
    );
    let (sub_reply, _) = apply_subscribe(&sub, &shared).await;
    sub_reply.expect("subscribe replies");
    let (tx101, _rx101) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(101, tx101);
    bind_test_publisher(&shared, 102);
    let (meta, payload) = encode_publish_meta("jwt/topic", 1, 0, false, b"hello-jwt");
    let publish = publish_frame(102, 3, meta, payload);
    let (_, deliveries) = apply_publish(&publish, &shared).await;
    assert_eq!(deliveries.len(), 1, "one live delivery expected");
    route_publish_out_deliveries(&shared, &publish, deliveries);
    // QoS 0 downlinks ride the bounded backlog, not the mailbox.
    let got = shared.conns.pop_qos0(101).expect("downlink arrived");
    assert_eq!(got.payload.as_ref(), b"hello-jwt");
}

/// B5-02: key rotation without a restart, plus unknown-`kid` refusal.
///
/// Rotates the endpoint's keys mid-test: a token signed with the new
/// key verifies with no restart (unknown-`kid` refresh), the old
/// token is refused once its kid leaves the document, and a token
/// naming a never-served kid is refused.
#[tokio::test]
async fn jwks_rotation_without_restart_and_unknown_kid_refused() {
    let first = JwksSigningKey::generate("rot-key-a");
    let server = JwksTestServer::start(vec![first.jwk_json()]).await;
    let shared = jwks_test_shared(server.url());
    let before = first.mint("https://issuer.example", "indra-mqtt", 3600, false);
    let frame = bind_frame(
        111,
        1,
        encode_bind_meta_creds("jwt-rot-1", true, 60, "u", before.as_bytes()),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    assert_eq!(decode_session_binding_meta(&reply.metadata).2, 0);

    let second = JwksSigningKey::generate("rot-key-b");
    server.rotate(vec![second.jwk_json()]);
    let after = second.mint("https://issuer.example", "indra-mqtt", 3600, false);
    let frame = bind_frame(
        112,
        1,
        encode_bind_meta_creds("jwt-rot-2", true, 60, "u", after.as_bytes()),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    assert_eq!(
        decode_session_binding_meta(&reply.metadata).2,
        0,
        "rotated key must verify without a restart"
    );

    let frame = bind_frame(
        113,
        1,
        encode_bind_meta_creds("jwt-rot-3", true, 60, "u", before.as_bytes()),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    assert_eq!(
        decode_session_binding_meta(&reply.metadata).2,
        0x86,
        "retired kid must be refused"
    );

    let stranger = JwksSigningKey::generate("rot-key-zzz");
    let unknown = stranger.mint("https://issuer.example", "indra-mqtt", 3600, false);
    let frame = bind_frame(
        114,
        1,
        encode_bind_meta_creds("jwt-rot-4", true, 60, "u", unknown.as_bytes()),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    assert_eq!(
        decode_session_binding_meta(&reply.metadata).2,
        0x86,
        "unknown kid must be refused"
    );
    assert!(shared.sessions.get("jwt-rot-4").is_none());
}

/// B5-02: claim and signature refusals through the broker CONNECT.
///
/// Expired tokens, wrong audience, wrong issuer and bad signatures
/// are all refused with 0x86 and create no session. A configured JWKS
/// endpoint also closes the open mode: non-JWT passwords and
/// anonymous CONNECTs are refused (0x86 and 0x87).
#[tokio::test]
async fn jwks_claim_and_signature_refusals() {
    let signing = JwksSigningKey::generate("ref-key-a");
    let server = JwksTestServer::start(vec![signing.jwk_json()]).await;
    let shared = jwks_test_shared(server.url());

    for (label, token) in [
        (
            "expired",
            // 300 s in the past: well past the 60 s clock-skew leeway.
            signing.mint("https://issuer.example", "indra-mqtt", -300, false),
        ),
        (
            "wrong-audience",
            signing.mint("https://issuer.example", "other-service", 3600, false),
        ),
        (
            "wrong-issuer",
            signing.mint("https://other.example", "indra-mqtt", 3600, false),
        ),
        (
            "bad-signature",
            signing.mint("https://issuer.example", "indra-mqtt", 3600, true),
        ),
    ] {
        let frame = bind_frame(
            121,
            1,
            encode_bind_meta_creds("jwt-ref", true, 60, "u", token.as_bytes()),
        );
        let reply = apply_bind(&frame, &shared).await.expect("bind replies");
        assert_eq!(
            decode_session_binding_meta(&reply.metadata).2,
            0x86,
            "{label} token must be refused"
        );
    }
    assert!(shared.sessions.get("jwt-ref").is_none());

    // Non-JWT password with JWKS-only configured: fail closed, never
    // open-accepted.
    let frame = bind_frame(
        122,
        1,
        encode_bind_meta_creds("jwt-plain", true, 60, "u", b"password"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    assert_eq!(
        decode_session_binding_meta(&reply.metadata).2,
        0x86,
        "non-JWT password must fail closed while JWKS is configured"
    );

    // Anonymous while JWKS is configured: refused with 0x87.
    let frame = bind_frame(123, 1, encode_bind_meta("jwt-anon", true, 60));
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    assert_eq!(
        decode_session_binding_meta(&reply.metadata).2,
        0x87,
        "anonymous must be refused while JWKS is configured"
    );
}

/// B5-02: unreachable JWKS endpoint fails closed through the broker.
///
/// A verifier pointed at a dead loopback port refuses CONNECT with
/// 0x86 and creates no session: access is denied, never granted.
#[tokio::test]
async fn jwks_unreachable_endpoint_fails_closed() {
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("ephemeral port");
    let closed_addr = closed.local_addr().expect("local addr");
    drop(closed);
    let mut shared = test_shared();
    let config = JwksConfig {
        jwks_url: format!("https://{closed_addr}/jwks.json"),
        issuer: "https://issuer.example".to_string(),
        audience: "indra-mqtt".to_string(),
        refresh_period_secs: 60,
        fetch_timeout_ms: 1_000,
        refresh_timeout_ms: 1_000,
        cache_max_keys: 32,
        cache_ttl_secs: 60,
        max_document_bytes: broker_auth::JWKS_DEFAULT_DOCUMENT_CAP,
        clock_skew_secs: 60,
        tls_verify: false,
        ca_cert_path: None,
    };
    shared.jwks = Some(Arc::new(JwksAuthenticator::new(config)));
    let signing = JwksSigningKey::generate("down-key-a");
    let token = signing.mint("https://issuer.example", "indra-mqtt", 3600, false);
    let frame = bind_frame(
        131,
        1,
        encode_bind_meta_creds("jwt-down", true, 60, "u", token.as_bytes()),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x86, "JWKS outage must fail closed with 0x86");
    assert!(shared.sessions.get("jwt-down").is_none());
}

/// B5-02 qualification probe (QUAL-NONE): the full JWKS matrix through
/// the broker CONNECT path plus publish and deliver.
///
/// Self-contained on loopback (no vendor server product exists): it
/// mints its own key sets, serves them from a test-owned HTTPS
/// endpoint with rotation and outage faults, and drives every leg
/// through `apply_bind` (the kernel CONNECT entry point the edge
/// uses), then publishes through the JWT identity. Exact counts only:
/// 3 valid CONNECTs accepted, exactly 1 live delivery carrying the
/// exact payload, rotation accepted without a restart with the retired
/// and unknown kids refused, 4 claim/signature refusals, outage
/// refused fail-closed, recovery accepted. Setup failures panic
/// (`expect`); the test never skips or returns early. `#[ignore]` so
/// only the pipeline's qualification run picks it up explicitly.
#[tokio::test]
#[ignore]
async fn test_qualify_jwks_connect_publish_deliver() {
    // Valid tokens against the live endpoint: exactly 3 CONNECTs
    // accepted through the broker.
    let signing = JwksSigningKey::generate("qual-key-a");
    let server = JwksTestServer::start(vec![signing.jwk_json()]).await;
    let shared = jwks_test_shared(server.url());
    let mut accepted = 0u32;
    for (conn_id, client_id) in [
        (201u64, "jwt-qual-1"),
        (202, "jwt-qual-2"),
        (203, "jwt-qual-3"),
    ] {
        let token = signing.mint("https://issuer.example", "indra-mqtt", 3600, false);
        let frame = bind_frame(
            conn_id,
            1,
            encode_bind_meta_creds(client_id, true, 60, "qual-user", token.as_bytes()),
        );
        let reply = apply_bind(&frame, &shared).await.expect("bind replies");
        if decode_session_binding_meta(&reply.metadata).2 == 0 {
            accepted += 1;
        }
    }
    assert_eq!(accepted, 3, "exactly 3 valid JWT CONNECTs must be accepted");

    // Publish and deliver with the JWT identity on the same broker:
    // exactly one live delivery carrying the exact payload.
    let sub = subscribe_frame(
        201,
        2,
        encode_subscribe_meta(7, "jwt-qual-1", &[("qual/topic", 0)]),
    );
    let (sub_reply, _) = apply_subscribe(&sub, &shared).await;
    sub_reply.expect("subscribe replies");
    let (tx201, _rx201) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(201, tx201);
    let (meta, payload) = encode_publish_meta("qual/topic", 1, 0, false, b"hello-qual");
    let publish = publish_frame(202, 3, meta, payload);
    let (_, deliveries) = apply_publish(&publish, &shared).await;
    assert_eq!(deliveries.len(), 1, "exactly one live delivery expected");
    route_publish_out_deliveries(&shared, &publish, deliveries);
    // QoS 0 downlinks ride the bounded backlog, not the mailbox.
    let got = shared.conns.pop_qos0(201).expect("downlink arrived");
    assert_eq!(got.payload.as_ref(), b"hello-qual");

    // Rotate the endpoint mid-test: the new kid verifies with no
    // restart (unknown-`kid` refresh), which also evicts the retired
    // kid from the cache, so the old token and a never-served kid are
    // both refused.
    let second = JwksSigningKey::generate("qual-key-b");
    server.rotate(vec![second.jwk_json()]);
    let after = second.mint("https://issuer.example", "indra-mqtt", 3600, false);
    let frame = bind_frame(
        211,
        1,
        encode_bind_meta_creds("jwt-qual-4", true, 60, "u", after.as_bytes()),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    assert_eq!(
        decode_session_binding_meta(&reply.metadata).2,
        0,
        "rotated key must verify without a restart"
    );
    let before = signing.mint("https://issuer.example", "indra-mqtt", 3600, false);
    let frame = bind_frame(
        212,
        1,
        encode_bind_meta_creds("jwt-qual-5", true, 60, "u", before.as_bytes()),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    assert_eq!(
        decode_session_binding_meta(&reply.metadata).2,
        0x86,
        "retired kid must be refused"
    );
    let stranger = JwksSigningKey::generate("qual-key-zzz");
    let unknown = stranger.mint("https://issuer.example", "indra-mqtt", 3600, false);
    let frame = bind_frame(
        213,
        1,
        encode_bind_meta_creds("jwt-qual-6", true, 60, "u", unknown.as_bytes()),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    assert_eq!(
        decode_session_binding_meta(&reply.metadata).2,
        0x86,
        "unknown kid must be refused"
    );

    // Claim and signature refusals through the broker: exactly 4
    // refusals, no session created. The endpoint still serves key-b,
    // so refusals blame the claim, not the setup.
    let mut refused = 0u32;
    for token in [
        // 300 s in the past: well past the 60 s clock-skew leeway.
        second.mint("https://issuer.example", "indra-mqtt", -300, false),
        second.mint("https://issuer.example", "other-service", 3600, false),
        second.mint("https://other.example", "indra-mqtt", 3600, false),
        second.mint("https://issuer.example", "indra-mqtt", 3600, true),
    ] {
        let frame = bind_frame(
            221,
            1,
            encode_bind_meta_creds("jwt-qual-claim", true, 60, "u", token.as_bytes()),
        );
        let reply = apply_bind(&frame, &shared).await.expect("bind replies");
        if decode_session_binding_meta(&reply.metadata).2 == 0x86 {
            refused += 1;
        }
    }
    assert_eq!(refused, 4, "all 4 bad-claim tokens must be refused");
    assert!(shared.sessions.get("jwt-qual-claim").is_none());

    // Outage: a verifier pointed at a dead loopback port refuses
    // CONNECT fail-closed and creates no session.
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("ephemeral port");
    let closed_addr = closed.local_addr().expect("local addr");
    drop(closed);
    let mut down_shared = test_shared();
    let down_config = JwksConfig {
        jwks_url: format!("https://{closed_addr}/jwks.json"),
        issuer: "https://issuer.example".to_string(),
        audience: "indra-mqtt".to_string(),
        refresh_period_secs: 60,
        fetch_timeout_ms: 1_000,
        refresh_timeout_ms: 1_000,
        cache_max_keys: 32,
        cache_ttl_secs: 60,
        max_document_bytes: broker_auth::JWKS_DEFAULT_DOCUMENT_CAP,
        clock_skew_secs: 60,
        tls_verify: false,
        ca_cert_path: None,
    };
    down_shared.jwks = Some(Arc::new(JwksAuthenticator::new(down_config)));
    let down_signing = JwksSigningKey::generate("qual-down-a");
    let down_token = down_signing.mint("https://issuer.example", "indra-mqtt", 3600, false);
    let frame = bind_frame(
        231,
        1,
        encode_bind_meta_creds("jwt-qual-down", true, 60, "u", down_token.as_bytes()),
    );
    let reply = apply_bind(&frame, &down_shared)
        .await
        .expect("bind replies");
    assert_eq!(
        decode_session_binding_meta(&reply.metadata).2,
        0x86,
        "JWKS outage must fail closed with 0x86"
    );
    assert!(down_shared.sessions.get("jwt-qual-down").is_none());

    // Recovery: a verifier against the healed endpoint accepts again
    // with no restart.
    let healed = jwks_test_shared(server.url());
    let token = second.mint("https://issuer.example", "indra-mqtt", 3600, false);
    let frame = bind_frame(
        241,
        1,
        encode_bind_meta_creds("jwt-qual-healed", true, 60, "u", token.as_bytes()),
    );
    let reply = apply_bind(&frame, &healed).await.expect("bind replies");
    assert_eq!(
        decode_session_binding_meta(&reply.metadata).2,
        0,
        "healed endpoint must accept"
    );
}
