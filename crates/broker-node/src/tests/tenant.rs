use super::*;

/// Set section flags on an encoded bind (test mirror of the edge
/// flag patch). Bytes stay the same except the flag bits.
#[cfg(test)]
fn mark_bind_sections_flagged(
    mut meta: Vec<u8>,
    creds: bool,
    peer: bool,
    cert: bool,
    listener: bool,
) -> Bytes {
    let id_len = u16::from_be_bytes([meta[0], meta[1]]) as usize;
    let flag_pos = 2 + id_len;
    if creds {
        meta[flag_pos] |= crate::BIND_FLAG_CREDS;
    }
    if peer {
        meta[flag_pos] |= crate::BIND_FLAG_PEER;
    }
    if cert {
        meta[flag_pos] |= crate::BIND_FLAG_CERT;
    }
    if listener {
        meta[flag_pos] |= crate::BIND_FLAG_LISTENER;
    }
    Bytes::from(meta)
}

/// Mirror of the edge certificate section (`encode_cert_payload`):
/// append `CertLen:16be | CnLen:16be | Cn | SubjectLen:16be |
/// Subject | SanCount:16be | (SanLen:16be | San)*` to an encoded
/// bind, ahead of any trailing alias section. Same bytes the edge
/// sends, so the test drives the production bind entry.
#[cfg(test)]
fn encode_bind_meta_cert(
    base: Bytes,
    cn: Option<&str>,
    subject: Option<&str>,
    sans: &[&str],
) -> Bytes {
    fn push_cert_string(out: &mut Vec<u8>, value: Option<&str>) {
        match value {
            Some(text) => {
                out.extend_from_slice(&(text.len() as u16).to_be_bytes());
                out.extend_from_slice(text.as_bytes());
            }
            None => out.extend_from_slice(&0u16.to_be_bytes()),
        }
    }
    let mut payload = Vec::new();
    push_cert_string(&mut payload, cn);
    push_cert_string(&mut payload, subject);
    payload.extend_from_slice(&(sans.len() as u16).to_be_bytes());
    for san in sans {
        payload.extend_from_slice(&(san.len() as u16).to_be_bytes());
        payload.extend_from_slice(san.as_bytes());
    }
    let mut meta = base.to_vec();
    meta.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    meta.extend_from_slice(&payload);
    Bytes::from(meta)
}

#[test]
fn bind_meta_peer_section_round_trip() {
    // Credentials plus peer address decode together.
    let meta = encode_bind_meta_peer(
        encode_bind_meta_creds("dev-p", true, 60, "alice", b"pw"),
        "192.0.2.10",
    );
    let req = decode_bind_meta(&meta).expect("peer bind decodes");
    assert_eq!(req.client_id, "dev-p");
    assert_eq!(req.username.as_deref(), Some("alice"));
    assert_eq!(req.password, Some(b"pw".to_vec()));
    assert_eq!(req.peerhost.as_deref(), Some("192.0.2.10"));
    // Anonymous plus peer address decodes without credentials.
    let meta = encode_bind_meta_peer(encode_bind_meta("dev-a", false, 30), "2001:db8::1");
    let req = decode_bind_meta(&meta).expect("anonymous peer bind decodes");
    assert_eq!(req.client_id, "dev-a");
    assert!(req.username.is_none());
    assert_eq!(req.peerhost.as_deref(), Some("2001:db8::1"));
    // Legacy binds without a peer section still decode with no peer.
    let req = decode_bind_meta(&encode_bind_meta("dev-l", true, 60)).expect("legacy decodes");
    assert!(req.peerhost.is_none());
    let req = decode_bind_meta(&encode_bind_meta_creds("dev-c", true, 60, "u", b"p"))
        .expect("legacy creds decode");
    assert!(req.peerhost.is_none());
    // Trailing bytes that are neither credentials nor a peer address
    // stay malformed, exactly as before the peer section.
    let mut garbage = encode_bind_meta("dev-g", true, 60).to_vec();
    garbage.extend_from_slice(b"not-a-section");
    assert!(decode_bind_meta(&Bytes::from(garbage)).is_err());
}

#[test]
fn bind_meta_alias_peer_variants_round_trip() {
    // Every bind variant the edge sends (/3-/7) with and without the
    // peer section, including with the B4-05 alias section present.
    // Anonymous /3 plus alias decodes with no peer and the maximum.
    let req = decode_bind_meta(&encode_bind_meta_alias(
        encode_bind_meta("a-anon", true, 60),
        10,
    ))
    .expect("anon alias decodes");
    assert!(req.username.is_none());
    assert!(req.peerhost.is_none());
    assert_eq!(req.client_alias_max, 10);
    // Credentialed /4 plus alias decodes with no peer and the maximum.
    let req = decode_bind_meta(&encode_bind_meta_alias(
        encode_bind_meta_creds("a-creds", true, 60, "u", b"p"),
        7,
    ))
    .expect("creds alias decodes");
    assert_eq!(req.username.as_deref(), Some("u"));
    assert!(req.peerhost.is_none());
    assert_eq!(req.client_alias_max, 7);
    // FX-01 regression: anonymous /5 peer plus the /6 alias of 0 (what
    // a 3.1.1 edge always sends) is byte-identical to credentials with
    // the peer literal as username and an empty password. It must
    // decode as anonymous with the peer set, never as a username, or
    // peerhost bans stop matching.
    let meta = encode_bind_meta_alias(
        encode_bind_meta_peer(encode_bind_meta("a-peer0", true, 60), "127.0.0.1"),
        0,
    );
    let req = decode_bind_meta(&meta).expect("anon peer alias-0 decodes");
    assert!(req.username.is_none());
    assert!(req.password.is_none());
    assert_eq!(req.peerhost.as_deref(), Some("127.0.0.1"));
    assert_eq!(req.client_alias_max, 0);
    // Same with a nonzero alias and an IPv6 literal.
    let meta = encode_bind_meta_alias(
        encode_bind_meta_peer(encode_bind_meta("a-peer6", false, 30), "2001:db8::1"),
        9,
    );
    let req = decode_bind_meta(&meta).expect("anon peer alias decodes");
    assert!(req.username.is_none());
    assert_eq!(req.peerhost.as_deref(), Some("2001:db8::1"));
    assert_eq!(req.client_alias_max, 9);
    // Credentialed peer plus alias decodes with both sections.
    for alias in [0u16, 7u16] {
        let meta = encode_bind_meta_alias(
            encode_bind_meta_peer(
                encode_bind_meta_creds("a-full", true, 60, "alice", b"pw"),
                "192.0.2.10",
            ),
            alias,
        );
        let req = decode_bind_meta(&meta).expect("creds peer alias decodes");
        assert_eq!(req.username.as_deref(), Some("alice"));
        assert_eq!(req.password, Some(b"pw".to_vec()));
        assert_eq!(req.peerhost.as_deref(), Some("192.0.2.10"));
        assert_eq!(req.client_alias_max, alias);
    }
    // Will plus peer plus alias (/7 full form) keeps the peer.
    let mut will_base =
        encode_bind_meta_with_will("a-will", true, 5, "will/test", b"bye", 0, false).to_vec();
    will_base.extend_from_slice(&(5u16).to_be_bytes());
    will_base.extend_from_slice(b"alice");
    will_base.extend_from_slice(&(2u16).to_be_bytes());
    will_base.extend_from_slice(b"pw");
    will_base.extend_from_slice(&(10u16).to_be_bytes());
    will_base.extend_from_slice(b"192.0.2.10");
    will_base.extend_from_slice(&protocol_v5::encode_alias_maximum(3));
    let req = decode_bind_meta(&Bytes::from(will_base)).expect("will peer alias decodes");
    assert_eq!(req.username.as_deref(), Some("alice"));
    assert_eq!(req.peerhost.as_deref(), Some("192.0.2.10"));
    assert_eq!(req.client_alias_max, 3);
    assert!(req.will.is_some());
    // Anonymous will plus peer plus alias-0 keeps the peer.
    let mut anon_will =
        encode_bind_meta_with_will("a-will-a", true, 5, "will/test", b"bye", 0, false).to_vec();
    anon_will.extend_from_slice(&(10u16).to_be_bytes());
    anon_will.extend_from_slice(b"192.0.2.10");
    anon_will.extend_from_slice(&protocol_v5::encode_alias_maximum(0));
    let req = decode_bind_meta(&Bytes::from(anon_will)).expect("anon will peer decodes");
    assert!(req.username.is_none());
    assert_eq!(req.peerhost.as_deref(), Some("192.0.2.10"));
    assert_eq!(req.client_alias_max, 0);
    // An empty password is never valid credentials (mirrors the edge
    // `PLen > 0' guard): the collision above stays malformed as
    // credentials rather than a guessed identity.
    let mut empty_pw = encode_bind_meta("e-empty", true, 60).to_vec();
    empty_pw.extend_from_slice(&(1u16).to_be_bytes());
    empty_pw.extend_from_slice(b"u");
    empty_pw.extend_from_slice(&(0u16).to_be_bytes());
    assert!(decode_bind_meta(&Bytes::from(empty_pw)).is_err());
}

#[test]
fn bind_meta_cert_section_round_trip() {
    // Credentials plus peer plus certificate decode together (the
    // credentialed order the edge will send once it forwards
    // certificates: peer literal, then the certificate section).
    let base = encode_bind_meta_peer(
        encode_bind_meta_creds("dev-cert", true, 60, "alice", b"pw"),
        "192.0.2.10",
    );
    let meta = encode_bind_meta_cert(base, Some("sensor-9"), Some("CN=sensor-9"), &["dns:s9"]);
    let req = decode_bind_meta(&meta).expect("cert bind decodes");
    assert_eq!(req.client_id, "dev-cert");
    assert_eq!(req.peerhost.as_deref(), Some("192.0.2.10"));
    assert_eq!(req.cert_cn.as_deref(), Some("sensor-9"));
    assert_eq!(req.cert_subject.as_deref(), Some("CN=sensor-9"));
    assert_eq!(req.cert_sans, vec!["dns:s9".to_string()]);
    // Anonymous bind with only a certificate section decodes.
    let meta = encode_bind_meta_cert(encode_bind_meta("dev-anon", true, 60), None, None, &[]);
    let req = decode_bind_meta(&meta).expect("anon cert bind decodes");
    assert!(req.cert_cn.is_none());
    assert!(req.cert_sans.is_empty());
    // Legacy binds without a certificate section still decode absent.
    let req = decode_bind_meta(&encode_bind_meta("dev-legacy", true, 60)).expect("legacy decodes");
    assert!(req.cert_cn.is_none());
    assert!(req.cert_subject.is_none());
    assert!(req.cert_sans.is_empty());
    // A truncated certificate section stays malformed (fail closed).
    let mut truncated = encode_bind_meta("dev-bad", true, 60).to_vec();
    truncated.extend_from_slice(&[0, 10, 0, 3, b'a', b'b']);
    assert!(decode_bind_meta(&Bytes::from(truncated)).is_err());
}

#[test]
fn bind_meta_explicit_sections_crosslang() {
    // Same bytes as the edge cross-language test.
    // Flags decide the parse. A short section fails closed.
    let anon: &[u8] = &[0, 6, b't', b'm', b'0', b'7', b'-', b'a', 1, 0, 60];
    let req = decode_bind_meta(anon).expect("anonymous decodes");
    assert_eq!(req.client_id, "tm07-a");
    assert!(req.username.is_none());
    assert!(req.peerhost.is_none());
    assert!(req.cert_cn.is_none());
    // Credentialed bind with flags (clean plus creds = 9).
    let creds: &[u8] = &[
        0, 6, b't', b'm', b'0', b'7', b'-', b'b', 9, 0, 60, 0, 5, b'a', b'l', b'i', b'c', b'e', 0,
        2, b'p', b'w',
    ];
    let req = decode_bind_meta(creds).expect("credentialed decodes");
    assert_eq!(req.client_id, "tm07-b");
    assert_eq!(req.username.as_deref(), Some("alice"));
    assert_eq!(req.password, Some(b"pw".to_vec()));
    assert!(req.peerhost.is_none());
    // Same bytes without flags decode to the same fields.
    let mut legacy = creds.to_vec();
    legacy[8] = 1;
    let old = decode_bind_meta(&legacy).expect("legacy credentialed decodes");
    assert_eq!(old.username, req.username);
    assert_eq!(old.password, req.password);
    // Peer bind with flags (peer only = 16, clean is false).
    let peer: &[u8] = &[
        0, 6, b't', b'm', b'0', b'7', b'-', b'c', 16, 0, 30, 0, 9, b'1', b'2', b'7', b'.', b'0',
        b'.', b'0', b'.', b'1',
    ];
    let req = decode_bind_meta(peer).expect("peer decodes");
    assert_eq!(req.client_id, "tm07-c");
    assert!(req.username.is_none());
    assert_eq!(req.peerhost.as_deref(), Some("127.0.0.1"));
    // Same peer bytes without flags decode to the same peer.
    let mut legacy_peer = peer.to_vec();
    legacy_peer[8] = 0;
    let old = decode_bind_meta(&legacy_peer).expect("legacy peer decodes");
    assert_eq!(old.peerhost, req.peerhost);
    // Certificate bind with flags (clean plus all sections = 57).
    let cert: &[u8] = &[
        0, 6, b't', b'm', b'0', b'7', b'-', b'd', 57, 0, 60, 0, 3, b'b', b'o', b'b', 0, 6, b's',
        b'e', b'c', b'r', b'e', b't', 0, 10, b'1', b'9', b'2', b'.', b'0', b'.', b'2', b'.', b'1',
        b'0', 0, 9, 0, 3, b'c', b'n', b'1', 0, 0, 0, 0,
    ];
    let req = decode_bind_meta(cert).expect("certificate decodes");
    assert_eq!(req.client_id, "tm07-d");
    assert_eq!(req.username.as_deref(), Some("bob"));
    assert_eq!(req.peerhost.as_deref(), Some("192.0.2.10"));
    assert_eq!(req.cert_cn.as_deref(), Some("cn1"));
    // Same certificate bytes without flags decode to the same fields.
    let mut legacy_cert = cert.to_vec();
    legacy_cert[8] = 1;
    let old = decode_bind_meta(&legacy_cert).expect("legacy certificate decodes");
    assert_eq!(old.username, req.username);
    assert_eq!(old.peerhost, req.peerhost);
    assert_eq!(old.cert_cn, req.cert_cn);
    // The flagged encoder mirror sets the same flag bits.
    let mirrored = mark_bind_sections_flagged(
        encode_bind_meta_creds("tm07-b", true, 60, "alice", b"pw").to_vec(),
        true,
        false,
        false,
        false,
    );
    assert_eq!(&mirrored[..], creds);
    // A short section fails with the documented error.
    let mut short = creds.to_vec();
    short.pop();
    assert!(decode_bind_meta(&short).is_err());
    let mut short_cert = cert.to_vec();
    short_cert.pop();
    assert!(decode_bind_meta(&short_cert).is_err());
}

/// Build one flagged bind (test mirror of the edge flag patch).
/// Sets the section bits on the same bytes the edge sends.
#[cfg(test)]
fn encode_bind_meta_flagged(base: Bytes, creds: bool, peer: bool, cert: bool) -> Bytes {
    mark_bind_sections_flagged(base.to_vec(), creds, peer, cert, false)
}

/// Mirror of the edge listener section (`encode_listener_bin`):
/// append `ListenerLen:16be | Listener` to an encoded bind.
/// Same bytes the edge sends, so the test drives the production
/// bind entry.
#[cfg(test)]
fn encode_bind_meta_listener(base: Bytes, listener: &str) -> Bytes {
    let mut meta = base.to_vec();
    meta.extend_from_slice(&(listener.len() as u16).to_be_bytes());
    meta.extend_from_slice(listener.as_bytes());
    Bytes::from(meta)
}

/// Build one flagged bind with a listener section.
#[cfg(test)]
fn encode_bind_meta_flagged_listener(base: Bytes, listener: &str) -> Bytes {
    let with_section = encode_bind_meta_listener(base, listener);
    mark_bind_sections_flagged(with_section.to_vec(), false, false, false, true)
}

#[tokio::test]
async fn tm07_flagged_two_tenants_isolated_through_broker() {
    // Two tenants connect with flagged binds through `apply_bind`
    // (the production entry the edge drives at connect). Each
    // tenant publishes a random value. Delivery stays in-tenant.
    use broker_session::tenant::TenantRule;
    let shared = test_shared();
    shared
        .tenant_registry
        .replace(vec![TenantRule::new("", "tenant", "t-${clientid}")])
        .expect("tenant rules store");
    for (tenant, user, pw) in [
        ("t-tm07-a", "alice", b"pw-a".as_slice()),
        ("t-tm07-b", "bob", b"pw-b".as_slice()),
    ] {
        shared
            .auth
            .add_user_in_tenant(tenant, user, pw)
            .expect("tenant user stores");
        for action in [AclAction::Publish, AclAction::Subscribe] {
            shared
                .auth
                .add_rule_in_tenant(tenant, AclRule::new("*", action, "tm07/#", true))
                .expect("tenant rule stores");
        }
    }
    // Flagged credentialed binds with a peer section (bits set).
    let base_a = encode_bind_meta_peer(
        encode_bind_meta_creds("tm07-a", true, 60, "alice", b"pw-a"),
        "127.0.0.1",
    );
    let meta_a = encode_bind_meta_flagged(base_a, true, true, false);
    let base_b = encode_bind_meta_peer(
        encode_bind_meta_creds("tm07-b", true, 60, "bob", b"pw-b"),
        "127.0.0.1",
    );
    let meta_b = encode_bind_meta_flagged(base_b, true, true, false);
    for (conn, meta) in [(401u64, meta_a), (402u64, meta_b)] {
        let reply = apply_bind(&bind_frame(conn, 1, meta), &shared)
            .await
            .expect("bind replies");
        let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
        assert_eq!(rc, 0);
    }
    for (conn, client) in [(401u64, "tm07-a"), (402u64, "tm07-b")] {
        let sub = subscribe_frame(
            conn,
            1,
            encode_subscribe_meta(1, client, &[("tm07/data", 0)]),
        );
        let (reply, _) = apply_subscribe(&sub, &shared).await;
        reply.expect("subscribe replies");
    }
    // Random value the broker cannot know (nanos plus process id).
    let secret = format!(
        "tm07-{}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
        std::process::id()
    );
    let (meta, payload) = encode_publish_meta("tm07/data", 0, 0, false, secret.as_bytes());
    let (_, deliveries) = apply_publish(&publish_frame(401, 2, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1, "same-tenant delivery works");
    assert_eq!(deliveries[0].0, 401);
    let (meta_b, payload_b) = encode_publish_meta("tm07/data", 0, 0, false, secret.as_bytes());
    let (_, deliveries_b) = apply_publish(&publish_frame(402, 2, meta_b, payload_b), &shared).await;
    assert_eq!(deliveries_b.len(), 1, "other tenant self-delivers");
    assert_eq!(deliveries_b[0].0, 402);
    assert_ne!(
        deliveries[0].0, deliveries_b[0].0,
        "no cross-tenant delivery"
    );
}

#[tokio::test]
async fn tm07_flagged_default_tenant_unchanged() {
    // Default tenant still connects, publishes and delivers.
    // The bind carries the flagged peer section (new bind path).
    use broker_session::tenant::DEFAULT_TENANT_ID;
    let shared = test_shared();
    assert!(shared.tenant_registry.is_empty());
    let base = encode_bind_meta_peer(encode_bind_meta("tm07-def", true, 60), "127.0.0.1");
    let meta = encode_bind_meta_flagged(base, false, true, false);
    let reply = apply_bind(&bind_frame(403, 1, meta), &shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
    let tenant = shared
        .sessions
        .get("tm07-def")
        .expect("session")
        .tenant_id
        .read()
        .clone();
    assert_eq!(tenant, DEFAULT_TENANT_ID);
    let sub = subscribe_frame(
        403,
        1,
        encode_subscribe_meta(1, "tm07-def", &[("tm07/def", 0)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");
    let (meta, payload) = encode_publish_meta("tm07/def", 0, 0, false, b"ping");
    let (_, deliveries) = apply_publish(&publish_frame(403, 2, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].0, 403);
}

#[tokio::test]
async fn tm08_listener_name_drives_tenant_through_bind() {
    // Production entry is bind bytes, as the spec lists. The edge
    // sends the same bytes with the same flag. A bind with a
    // listener name lands in the rule for that listener. A bind
    // without a name keeps the default scope.
    use broker_session::tenant::{TenantRule, DEFAULT_TENANT_ID};
    let shared = test_shared();
    shared
        .tenant_registry
        .replace(vec![
            TenantRule::new("tls", "tenant", "t-tls"),
            TenantRule::new("tcp", "tenant", "t-tcp"),
        ])
        .expect("scoped rules store");
    let tls_meta = encode_bind_meta_flagged_listener(encode_bind_meta("tm08-tls", true, 60), "tls");
    let reply = apply_bind(&bind_frame(501, 1, tls_meta), &shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
    let tenant = shared
        .sessions
        .get_in_tenant("t-tls", "tm08-tls")
        .expect("session")
        .tenant_id
        .read()
        .clone();
    assert_eq!(tenant, "t-tls");
    let plain_meta = encode_bind_meta("tm08-plain", true, 60);
    let reply = apply_bind(&bind_frame(502, 1, plain_meta), &shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
    let tenant = shared
        .sessions
        .get("tm08-plain")
        .expect("session")
        .tenant_id
        .read()
        .clone();
    assert_eq!(tenant, DEFAULT_TENANT_ID);
}

#[tokio::test]
async fn tm08_two_listeners_isolated_through_broker() {
    // Production entry is bind bytes, as the spec lists. Two
    // listeners hold two tenants apart. Each client uses a random
    // value the broker cannot know. Delivery stays in-tenant.
    use broker_session::tenant::TenantRule;
    let shared = test_shared();
    shared
        .tenant_registry
        .replace(vec![
            TenantRule::new("tls", "tenant", "t-lis-a"),
            TenantRule::new("tcp", "tenant", "t-lis-b"),
        ])
        .expect("scoped rules store");
    let meta_a = encode_bind_meta_flagged_listener(encode_bind_meta("tm08-a", true, 60), "tls");
    let meta_b = encode_bind_meta_flagged_listener(encode_bind_meta("tm08-b", true, 60), "tcp");
    for (conn, meta) in [(511u64, meta_a), (512u64, meta_b)] {
        let reply = apply_bind(&bind_frame(conn, 1, meta), &shared)
            .await
            .expect("bind replies");
        let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
        assert_eq!(rc, 0);
    }
    for (conn, client) in [(511u64, "tm08-a"), (512u64, "tm08-b")] {
        let sub = subscribe_frame(
            conn,
            1,
            encode_subscribe_meta(1, client, &[("tm08/data", 0)]),
        );
        let (reply, _) = apply_subscribe(&sub, &shared).await;
        reply.expect("subscribe replies");
    }
    let secret = format!(
        "tm08-{}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
        std::process::id()
    );
    let (meta, payload) = encode_publish_meta("tm08/data", 0, 0, false, secret.as_bytes());
    let (_, deliveries) = apply_publish(&publish_frame(511, 2, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1, "same-tenant delivery works");
    assert_eq!(deliveries[0].0, 511);
    let (meta_b, payload_b) = encode_publish_meta("tm08/data", 0, 0, false, secret.as_bytes());
    let (_, deliveries_b) = apply_publish(&publish_frame(512, 2, meta_b, payload_b), &shared).await;
    assert_eq!(deliveries_b.len(), 1, "other tenant self-delivers");
    assert_ne!(
        deliveries[0].0, deliveries_b[0].0,
        "no cross-tenant delivery"
    );
}

#[tokio::test]
async fn tm08_client_cert_check_through_bind() {
    // Production entry is bind bytes, as the spec lists. The edge
    // sends the same bytes. The TLS handshake supplies the fields.
    // Verification off lets a bind without a certificate through.
    // Verification on refuses it, counts and logs. A bind with a
    // certificate passes. All through `apply_bind` with bind bytes.
    let mut shared = test_shared();
    let base = encode_bind_meta("tm08-nocert", true, 60);
    let flagged = mark_bind_sections_flagged(base.to_vec(), false, false, false, false);
    let reply = apply_bind(&bind_frame(521, 1, flagged), &shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0, "no verification means no certificate is fine");
    shared.tls_verify_client_cert = true;
    let before = shared.metrics.auth_failures();
    let base = encode_bind_meta("tm08-deny", true, 60);
    let with_listener = encode_bind_meta_listener(base, "tls");
    let flagged = mark_bind_sections_flagged(with_listener.to_vec(), false, false, false, true);
    let reply = apply_bind(&bind_frame(522, 1, flagged), &shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x87, "missing certificate is refused");
    assert_eq!(
        shared.metrics.auth_failures(),
        before + 1,
        "denial is counted"
    );
    assert!(
        shared.sessions.get("tm08-deny").is_none(),
        "no session is made"
    );
    let base = encode_bind_meta_cert(
        encode_bind_meta("tm08-cert", true, 60),
        Some("cn-1"),
        None,
        &[],
    );
    let with_listener = encode_bind_meta_listener(base, "tls");
    let flagged = mark_bind_sections_flagged(with_listener.to_vec(), false, false, true, true);
    let reply = apply_bind(&bind_frame(523, 1, flagged), &shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0, "a certificate passes verification");
}

#[tokio::test]
async fn tenant_two_tenants_recorded_through_broker_connect() {
    // Two real clients whose connect contexts render two different
    // tenant ids connect concurrently through `apply_bind` (the same
    // entry point the edge uses); the kernel records two distinct
    // tenant ids and each client's self-delivery works. Full
    // isolation is proved in MT-02; here self-delivery plus distinct
    // records is enough.
    use broker_session::tenant::{TenantRule, DEFAULT_TENANT_ID};
    let shared = test_shared();
    shared
        .tenant_registry
        .replace(vec![TenantRule::new("", "tenant", "t-${clientid}")])
        .expect("tenant rules store");
    let frame_a = bind_frame(91, 1, encode_bind_meta("mt-tenant-a", true, 60));
    let frame_b = bind_frame(92, 1, encode_bind_meta("mt-tenant-b", true, 60));
    let (reply_a, reply_b) =
        tokio::join!(apply_bind(&frame_a, &shared), apply_bind(&frame_b, &shared));
    for reply in [reply_a, reply_b] {
        let reply = reply.expect("bind replies");
        let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
        assert_eq!(rc, 0);
    }
    // MT-03: sessions are keyed by `(tenant, client id)`, so the
    // lookup is qualified with the rendered tenant.
    let tenant_a = shared
        .sessions
        .get_in_tenant("t-mt-tenant-a", "mt-tenant-a")
        .expect("client a session")
        .tenant_id
        .read()
        .clone();
    let tenant_b = shared
        .sessions
        .get_in_tenant("t-mt-tenant-b", "mt-tenant-b")
        .expect("client b session")
        .tenant_id
        .read()
        .clone();
    assert_eq!(tenant_a, "t-mt-tenant-a");
    assert_eq!(tenant_b, "t-mt-tenant-b");
    assert_ne!(tenant_a, tenant_b);
    assert_ne!(tenant_a, DEFAULT_TENANT_ID);
    // Each client's self-delivery works within its own tenant.
    for (client, conn) in [("mt-tenant-a", 91u64), ("mt-tenant-b", 92u64)] {
        let sub = subscribe_frame(
            conn,
            1,
            encode_subscribe_meta(1, client, &[(format!("t/{client}").as_str(), 0)]),
        );
        let (reply, _) = apply_subscribe(&sub, &shared).await;
        reply.expect("subscribe replies");
        let topic = format!("t/{client}");
        let (meta, payload) = encode_publish_meta(&topic, 0, 0, false, b"hello");
        let (_, deliveries) = apply_publish(&publish_frame(conn, 2, meta, payload), &shared).await;
        assert_eq!(deliveries.len(), 1, "self-delivery must work for {client}");
        assert_eq!(deliveries[0].0, conn);
    }
}

#[tokio::test]
async fn tenant_default_behaves_as_before() {
    // A client with no tenant attribute connects with the default
    // (empty) expression and behaves exactly as before the change:
    // subscribe, publish, deliver round trip unchanged, tenant id is
    // the default.
    use broker_session::tenant::DEFAULT_TENANT_ID;
    let shared = test_shared();
    assert!(shared.tenant_registry.is_empty());
    let frame = bind_frame(93, 1, encode_bind_meta("mt-default-1", true, 60));
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
    let tenant = shared
        .sessions
        .get("mt-default-1")
        .expect("session")
        .tenant_id
        .read()
        .clone();
    assert_eq!(tenant, DEFAULT_TENANT_ID);
    let sub = subscribe_frame(
        93,
        1,
        encode_subscribe_meta(1, "mt-default-1", &[("t/default/rt", 0)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");
    let (meta, payload) = encode_publish_meta("t/default/rt", 0, 0, false, b"ping");
    let (_, deliveries) = apply_publish(&publish_frame(93, 2, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].0, 93);
}

#[tokio::test]
async fn tenant_router_isolates_two_tenants_through_broker() {
    // Two tenants through the real broker path (bind, subscribe,
    // publish, deliver): the same filter (and one `#`, and one
    // shared group name) in tenant A and tenant B never cross. A
    // publish in A is received only in A and a publish in B only in
    // B; mismatches deliver nothing and are counted, never an error
    // to the publisher (QoS 1 still gets its clean ack).
    use broker_session::tenant::TenantRule;
    let shared = test_shared();
    shared
        .tenant_registry
        .replace(vec![TenantRule::new("", "tenant", "u-${username}")])
        .expect("tenant rules store");
    // a1 and a2 share tenant u-alice; b1 lives in u-bob.
    for (conn, client, user) in [
        (101u64, "mt2-a1", "alice"),
        (102u64, "mt2-a2", "alice"),
        (103u64, "mt2-b1", "bob"),
    ] {
        let meta = encode_bind_meta_creds(client, true, 60, user, b"pw");
        let reply = apply_bind(&bind_frame(conn, 1, meta), &shared)
            .await
            .expect("bind replies");
        let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
        assert_eq!(rc, 0);
    }
    // MT-03: sessions are keyed by `(tenant, client id)`, so the
    // lookup is qualified with the rendered tenant.
    assert_eq!(
        shared
            .sessions
            .get_in_tenant("u-alice", "mt2-a1")
            .expect("a1")
            .tenant_id
            .read()
            .clone(),
        "u-alice"
    );
    assert_eq!(
        shared
            .sessions
            .get_in_tenant("u-bob", "mt2-b1")
            .expect("b1")
            .tenant_id
            .read()
            .clone(),
        "u-bob"
    );
    // Phase 1: `#` in A never observes B. Only a1 subscribes yet,
    // so every delivery assertion below is exact.
    let sub = subscribe_frame(101, 1, encode_subscribe_meta(1, "mt2-a1", &[("#", 0)]));
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");
    let (meta, payload) = encode_publish_meta("mt2/probe", 0, 0, false, b"probe-a");
    let (_, deliveries) = apply_publish(&publish_frame(101, 2, meta, payload), &shared).await;
    let conns: Vec<u64> = deliveries.iter().map(|(conn, _)| *conn).collect();
    assert_eq!(conns, vec![101], "A's `#` sees A's own traffic");
    let before = shared.metrics.tenant_mismatch_dropped();
    let (meta, payload) = encode_publish_meta("mt2/probe", 0, 0, false, b"probe-b");
    let (_, deliveries) = apply_publish(&publish_frame(103, 3, meta, payload), &shared).await;
    assert!(
        deliveries.is_empty(),
        "nobody in B subscribed; A's `#` must not see B"
    );
    assert!(
        shared.metrics.tenant_mismatch_dropped() > before,
        "A's `#` is counted on B's publish, never delivered"
    );
    // Phase 2..4 run without the `#` (one tenant's teardown removes
    // only its own copy) so group sizes stay exact.
    shared.router.unsubscribe_in_tenant(
        &TopicFilter::new("#").expect("valid filter"),
        "mt2-a1",
        "u-alice",
    );

    // Same exact filter in both tenants, and the same `$share`
    // group name in both tenants.
    for (conn, client, filter) in [
        (101u64, "mt2-a1", "mt2/data"),
        (102u64, "mt2-a2", "mt2/data"),
        (103u64, "mt2-b1", "mt2/data"),
        (101u64, "mt2-a1", "$share/mt2pool/mt2/jobs"),
        (102u64, "mt2-a2", "$share/mt2pool/mt2/jobs"),
        (103u64, "mt2-b1", "$share/mt2pool/mt2/jobs"),
    ] {
        let sub = subscribe_frame(conn, 1, encode_subscribe_meta(1, client, &[(filter, 0)]));
        let (reply, _) = apply_subscribe(&sub, &shared).await;
        reply.expect("subscribe replies");
    }

    // Publish in A: only A's exact subscribers see it.
    let before = shared.metrics.tenant_mismatch_dropped();
    let (meta, payload) = encode_publish_meta("mt2/data", 0, 0, false, b"from-a");
    let (_, deliveries) = apply_publish(&publish_frame(101, 4, meta, payload), &shared).await;
    let mut conns: Vec<u64> = deliveries.iter().map(|(conn, _)| *conn).collect();
    conns.sort_unstable();
    assert_eq!(conns, vec![101, 102], "A's publish stays in A");
    assert!(
        shared.metrics.tenant_mismatch_dropped() > before,
        "B's exact copy is counted, never delivered"
    );

    // Publish in B: only B sees it.
    let before = shared.metrics.tenant_mismatch_dropped();
    let (meta, payload) = encode_publish_meta("mt2/data", 0, 0, false, b"from-b");
    let (_, deliveries) = apply_publish(&publish_frame(103, 5, meta, payload), &shared).await;
    let conns: Vec<u64> = deliveries.iter().map(|(conn, _)| *conn).collect();
    assert_eq!(conns, vec![103], "B's publish stays in B");
    assert!(
        shared.metrics.tenant_mismatch_dropped() > before,
        "A's exact copies are counted, never delivered"
    );

    // Shared groups balance independently per tenant: two publishes
    // in A rotate over a1/a2 only, never touching b1.
    let mut owners = Vec::new();
    for seq in 0..4u64 {
        let (meta, payload) = encode_publish_meta("mt2/jobs", 0, 0, false, b"job");
        let (_, deliveries) =
            apply_publish(&publish_frame(101, 10 + seq, meta, payload), &shared).await;
        assert_eq!(deliveries.len(), 1, "exactly one group member per publish");
        owners.push(deliveries[0].0);
    }
    assert_eq!(owners, vec![101, 102, 101, 102]);
    let (meta, payload) = encode_publish_meta("mt2/jobs", 0, 0, false, b"job-b");
    let (_, deliveries) = apply_publish(&publish_frame(103, 20, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].0, 103, "B's group rotates on its own cursor");

    // QoS 1 in A still acks cleanly: mismatch is silent, not an error.
    let (meta, payload) = encode_publish_meta("mt2/data", 7, 1, false, b"qos1-a");
    let (ack, deliveries) = apply_publish(&publish_frame(101, 30, meta, payload), &shared).await;
    assert!(ack.is_some(), "QoS 1 publisher still gets its ack");
    assert_eq!(deliveries.len(), 2, "both A subscribers delivered");
}

#[tokio::test]
async fn mt06_auth_two_tenants_same_username_isolated_through_broker() {
    // MT-06 through the real broker path (bind, subscribe, publish,
    // deliver — never store-only): the same username `sam` in two
    // tenants holds two independent passwords and rule sets. Tenants
    // render from the client id (`t-<clientid>`) so both tenants can
    // share the username. A publishes under a rule that allows A
    // only: A's subscriber receives it while B's subscriber (and any
    // cross-tenant subscribe attempt) is refused; A's password
    // against B fails to connect, as does an unknown tenant.
    use broker_session::tenant::TenantRule;
    let shared = test_shared();
    shared
        .auth
        .set_policy(broker_auth::PasswordHashPolicy::for_tests());
    shared
        .tenant_registry
        .replace(vec![TenantRule::new("", "tenant", "t-${clientid}")])
        .expect("tenant rules store");
    // Per-tenant credentials: same username, different passwords.
    shared
        .auth
        .add_user_in_tenant("t-mt6-a", "sam", b"pw-for-a")
        .expect("tenant-a user stores");
    shared
        .auth
        .add_user_in_tenant("t-mt6-b", "sam", b"pw-for-b")
        .expect("tenant-b user stores");
    // Rules grant publish and subscribe on mt6/# in A only; B stays
    // closed (no users' rules, deny).
    shared
        .auth
        .add_rule_in_tenant(
            "t-mt6-a",
            broker_auth::AclRule::new("*", broker_auth::AclAction::Publish, "mt6/#", true),
        )
        .expect("tenant-a publish rule stores");
    shared
        .auth
        .add_rule_in_tenant(
            "t-mt6-a",
            broker_auth::AclRule::new("*", broker_auth::AclAction::Subscribe, "mt6/#", true),
        )
        .expect("tenant-a subscribe rule stores");
    // A connects with A's password, B with B's (same username).
    for (conn, client, pw) in [
        (301u64, "mt6-a", b"pw-for-a".as_slice()),
        (302u64, "mt6-b", b"pw-for-b".as_slice()),
    ] {
        let meta = encode_bind_meta_creds(client, true, 60, "sam", pw);
        let reply = apply_bind(&bind_frame(conn, 1, meta), &shared)
            .await
            .expect("bind replies");
        let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
        assert_eq!(rc, 0, "{client} connects with its own password");
    }
    // Sessions are keyed by (tenant, client id).
    assert_eq!(
        shared
            .sessions
            .get_in_tenant("t-mt6-a", "mt6-a")
            .expect("a session")
            .tenant_id
            .read()
            .clone(),
        "t-mt6-a"
    );
    assert_eq!(
        shared
            .sessions
            .get_in_tenant("t-mt6-b", "mt6-b")
            .expect("b session")
            .tenant_id
            .read()
            .clone(),
        "t-mt6-b"
    );
    // A's password against B fails to connect (bad credentials).
    let meta = encode_bind_meta_creds("mt6-b", true, 60, "sam", b"pw-for-a");
    let reply = apply_bind(&bind_frame(303, 1, meta), &shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x86, "A's password against B must fail");
    // An unknown tenant (no users, no rules) fails to connect too.
    let meta = encode_bind_meta_creds("mt6-c", true, 60, "sam", b"pw-for-a");
    let reply = apply_bind(&bind_frame(304, 1, meta), &shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x86, "unknown tenant must fail closed");
    // Subscribe: A granted, B refused (0x87, not authorized).
    let sub = subscribe_frame(
        301,
        1,
        encode_subscribe_meta(1, "mt6-a", &[("mt6/data", 0)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    let reply = reply.expect("subscribe replies");
    assert_eq!(reply.metadata[2], 0, "A's subscribe is granted");
    let sub = subscribe_frame(
        302,
        1,
        encode_subscribe_meta(1, "mt6-b", &[("mt6/data", 0)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    let reply = reply.expect("subscribe replies");
    assert_eq!(
        reply.metadata[2], 0x80,
        "B's cross-tenant subscribe is refused with the version-4 code"
    );
    // Publish in A: A's subscriber receives it, B never does.
    let (meta, payload) = encode_publish_meta("mt6/data", 0, 0, false, b"from-a");
    let (_, deliveries) = apply_publish(&publish_frame(301, 2, meta, payload), &shared).await;
    let conns: Vec<u64> = deliveries.iter().map(|(conn, _)| *conn).collect();
    assert_eq!(conns, vec![301], "A's publish stays in A");
    // Publish in B is refused like an ACL denial (QoS 1 ack 0x87,
    // nothing delivered).
    let (meta, payload) = encode_publish_meta("mt6/data", 9, 1, false, b"from-b");
    let (ack, deliveries) = apply_publish(&publish_frame(302, 3, meta, payload), &shared).await;
    let ack = ack.expect("QoS 1 publisher gets its ack");
    assert_eq!(ack.metadata[2], 0x87, "B's publish is refused");
    assert!(deliveries.is_empty(), "B's publish delivers nothing");
}

#[tokio::test]
async fn tm04_rule_in_tenant_fires_only_for_own_tenant_through_broker() {
    // TM-04 through the real broker path (bind, subscribe,
    // publish, rule dispatch, sink — never store-only). Two
    // tenants share the username `sam` with separate passwords.
    // One rule of tenant A republishes. A publish with a random
    // value in A fires the rule and the output carries tenant A
    // to the subscriber set of A only. A publish in B fires
    // nothing and outputs nothing.
    use broker_session::tenant::TenantRule;
    let mut shared = test_shared();
    shared
        .auth
        .set_policy(broker_auth::PasswordHashPolicy::for_tests());
    shared
        .tenant_registry
        .replace(vec![TenantRule::new("", "tenant", "t-${clientid}")])
        .expect("tenant rules store");
    for (tenant, password) in [
        ("t-tm04-a", b"pw-for-a".as_slice()),
        ("t-tm04-b", b"pw-for-b".as_slice()),
    ] {
        shared
            .auth
            .add_user_in_tenant(tenant, "sam", password)
            .expect("tenant user stores");
        for action in [
            broker_auth::AclAction::Publish,
            broker_auth::AclAction::Subscribe,
        ] {
            shared
                .auth
                .add_rule_in_tenant(
                    tenant,
                    broker_auth::AclRule::new("*", action, "tm04/#", true),
                )
                .expect("tenant rule stores");
        }
    }
    for (conn, client, pw) in [
        (401u64, "tm04-a", b"pw-for-a".as_slice()),
        (402u64, "tm04-b", b"pw-for-b".as_slice()),
    ] {
        let meta = encode_bind_meta_creds(client, true, 60, "sam", pw);
        let reply = apply_bind(&bind_frame(conn, 1, meta), &shared)
            .await
            .expect("bind replies");
        let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
        assert_eq!(rc, 0, "{client} connects with its own password");
    }
    // One rule of tenant A: match `tm04/in`, republish `tm04/out`.
    let rule = shared
        .engine
        .create_rule_in_tenant(
            "t-tm04-a".to_string(),
            "tm04-a-rule".to_string(),
            broker_protocol::TopicFilter::new("tm04/in").expect("filter parses"),
            None,
            true,
            vec![broker_rules::RuleAction::Republish {
                topic: broker_protocol::Topic::new("tm04/out").expect("topic parses"),
                qos: broker_protocol::QoS::AtMostOnce,
            }],
        )
        .expect("rule creates");
    assert_eq!(rule.tenant, "t-tm04-a");
    // Both tenants subscribe to the output topic. Both grants land.
    for (conn, client) in [(401u64, "tm04-a"), (402u64, "tm04-b")] {
        let sub = subscribe_frame(
            conn,
            1,
            encode_subscribe_meta(1, client, &[("tm04/out", 0)]),
        );
        let (reply, _) = apply_subscribe(&sub, &shared).await;
        let reply = reply.expect("subscribe replies");
        assert_eq!(reply.metadata[2], 0, "{client} subscribe is granted");
    }
    // Tee probe: record the output tenant, then run the real
    // fan-out unchanged.
    #[derive(Default)]
    struct TeeSink {
        inner: std::sync::Mutex<Option<std::sync::Arc<dyn BrokerSink>>>,
        recorded: std::sync::Mutex<Vec<(String, String, bytes::Bytes)>>,
    }
    #[async_trait::async_trait]
    impl BrokerSink for TeeSink {
        async fn publish(
            &self,
            topic: Topic,
            payload: Bytes,
            qos: QoS,
            retain: bool,
        ) -> Result<(), broker_rules::RuleEngineError> {
            self.recorded.lock().unwrap().push((
                "default".to_string(),
                topic.as_str().to_string(),
                payload.clone(),
            ));
            let inner = self.inner.lock().unwrap().clone();
            if let Some(inner) = inner {
                inner.publish(topic, payload, qos, retain).await?;
            }
            Ok(())
        }
        async fn publish_in_tenant(
            &self,
            tenant: &str,
            topic: Topic,
            payload: Bytes,
            qos: QoS,
            retain: bool,
        ) -> Result<(), broker_rules::RuleEngineError> {
            self.recorded.lock().unwrap().push((
                tenant.to_string(),
                topic.as_str().to_string(),
                payload.clone(),
            ));
            let inner = self.inner.lock().unwrap().clone();
            if let Some(inner) = inner {
                inner
                    .publish_in_tenant(tenant, topic, payload, qos, retain)
                    .await?;
            }
            Ok(())
        }
    }
    let tee = std::sync::Arc::new(TeeSink::default());
    *tee.inner.lock().unwrap() = Some(shared.sink.clone());
    let tee_obj: std::sync::Arc<dyn BrokerSink> = tee.clone();
    shared.sink = tee_obj;
    // Random values the broker cannot know.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|span| span.as_nanos())
        .unwrap_or(0);
    let secret_a = format!("tm04-secret-a-{nanos}");
    let secret_b = format!("tm04-secret-b-{nanos}");
    // Publish in A: the rule fires once.
    let (meta, payload) = encode_publish_meta("tm04/in", 0, 0, false, secret_a.as_bytes());
    let (_, deliveries) = apply_publish(&publish_frame(401, 2, meta, payload), &shared).await;
    assert!(deliveries.is_empty(), "no direct subscriber on tm04/in");
    let recorded = tee.recorded.lock().unwrap().clone();
    assert_eq!(recorded.len(), 1, "one rule output for the A publish");
    assert_eq!(recorded[0].0, "t-tm04-a", "output stays in tenant A");
    assert_eq!(recorded[0].1, "tm04/out");
    assert_eq!(recorded[0].2, bytes::Bytes::from(secret_a.clone()));
    // The output tenant holds only the A subscriber.
    let out_topic = broker_protocol::Topic::new("tm04/out").expect("topic parses");
    let (matched_a, _) = shared
        .router
        .matches_in_tenant_counted(&out_topic, None, "t-tm04-a");
    assert!(matched_a.iter().any(|s| s.client_id.as_ref() == "tm04-a"));
    assert!(!matched_a.iter().any(|s| s.client_id.as_ref() == "tm04-b"));
    let (matched_b, _) = shared
        .router
        .matches_in_tenant_counted(&out_topic, None, "t-tm04-b");
    assert!(matched_b.iter().any(|s| s.client_id.as_ref() == "tm04-b"));
    assert!(!matched_b.iter().any(|s| s.client_id.as_ref() == "tm04-a"));
    // Publish in B: no rule of B exists, so nothing outputs.
    let (meta, payload) = encode_publish_meta("tm04/in", 0, 0, false, secret_b.as_bytes());
    let (_, deliveries) = apply_publish(&publish_frame(402, 3, meta, payload), &shared).await;
    assert!(deliveries.is_empty(), "no direct subscriber on tm04/in");
    assert_eq!(
        tee.recorded.lock().unwrap().len(),
        1,
        "the B publish outputs nothing"
    );
    // Counters show one match and one pass, both from A.
    let stored = shared.engine.get_rule(&rule.id).expect("rule stays");
    assert_eq!(
        stored
            .matched_cnt
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        stored.passed_cnt.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn mt06_auth_default_tenant_keeps_working_through_broker() {
    // MT-06: the pre-change username/password and ACL rows keep
    // working unchanged through the real broker path while no tenant
    // rule is configured (every client lands in the default tenant
    // seeded from today's tables).
    let shared = test_shared();
    assert!(shared.tenant_registry.is_empty());
    shared
        .auth
        .set_policy(broker_auth::PasswordHashPolicy::for_tests());
    shared
        .auth
        .add_user("base", b"base-pw")
        .expect("user stores");
    shared
        .auth
        .add_rule(broker_auth::AclRule::new(
            "*",
            broker_auth::AclAction::Publish,
            "base/#",
            true,
        ))
        .expect("publish rule stores");
    shared
        .auth
        .add_rule(broker_auth::AclRule::new(
            "*",
            broker_auth::AclAction::Subscribe,
            "base/#",
            true,
        ))
        .expect("subscribe rule stores");
    let meta = encode_bind_meta_creds("mt6-base", true, 60, "base", b"base-pw");
    let reply = apply_bind(&bind_frame(311, 1, meta), &shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0, "default-tenant credentials keep working");
    let sub = subscribe_frame(
        311,
        1,
        encode_subscribe_meta(1, "mt6-base", &[("base/1", 0)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    let reply = reply.expect("subscribe replies");
    assert_eq!(reply.metadata[2], 0, "default-tenant subscribe granted");
    let (meta, payload) = encode_publish_meta("base/1", 0, 0, false, b"ping");
    let (_, deliveries) = apply_publish(&publish_frame(311, 2, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1, "default-tenant delivery works");
    assert_eq!(deliveries[0].0, 311);
    // A wrong password still fails.
    let meta = encode_bind_meta_creds("mt6-base-wrong", true, 60, "base", b"wrong");
    let reply = apply_bind(&bind_frame(312, 1, meta), &shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x86, "wrong password still refused");
}

#[tokio::test]
async fn tenant_retained_isolates_two_tenants_through_broker() {
    // Retained scope through the real broker path (bind, retained
    // publish, subscribe-time fan-out): the same topic retained in two
    // tenants reads back independently; `#` in one tenant excludes the
    // other's messages; clearing in one tenant leaves the other intact.
    use broker_session::tenant::TenantRule;
    let shared = test_shared();
    shared
        .tenant_registry
        .replace(vec![TenantRule::new("", "tenant", "u-${username}")])
        .expect("tenant rules store");
    for (conn, client, user) in [(201u64, "mtR-a1", "alice"), (202u64, "mtR-b1", "bob")] {
        let meta = encode_bind_meta_creds(client, true, 60, user, b"pw");
        let reply = apply_bind(&bind_frame(conn, 1, meta), &shared)
            .await
            .expect("bind replies");
        let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
        assert_eq!(rc, 0);
    }
    // Same topic retained in both tenants with different payloads.
    let (meta, payload) = encode_publish_meta("mt2/retained", 0, 0, true, b"from-a");
    let (_, _) = apply_publish(&publish_frame(201, 2, meta, payload), &shared).await;
    let (meta, payload) = encode_publish_meta("mt2/retained", 0, 0, true, b"from-b");
    let (_, _) = apply_publish(&publish_frame(202, 3, meta, payload), &shared).await;
    // Late subscribers in each tenant receive only their own message.
    for (conn, client, user, expect) in [
        (203u64, "mtR-a2", "alice", b"from-a".as_slice()),
        (204u64, "mtR-b2", "bob", b"from-b".as_slice()),
    ] {
        let meta = encode_bind_meta_creds(client, true, 60, user, b"pw");
        let reply = apply_bind(&bind_frame(conn, 1, meta), &shared)
            .await
            .expect("bind replies");
        let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
        assert_eq!(rc, 0);
        let sub = subscribe_frame(
            conn,
            1,
            encode_subscribe_meta(1, client, &[("mt2/retained", 0)]),
        );
        let (reply, retained) = apply_subscribe(&sub, &shared).await;
        reply.expect("subscribe replies");
        assert_eq!(retained.len(), 1, "one retained message in {client}");
        assert_eq!(retained[0].payload.as_ref(), expect);
    }
    // `#` in A sees only A's retained message, never B's.
    let sub = subscribe_frame(203, 2, encode_subscribe_meta(2, "mtR-a2", &[("#", 0)]));
    let (reply, retained) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");
    assert_eq!(retained.len(), 1, "A's `#` sees only A's retained");
    assert_eq!(retained[0].payload.as_ref(), b"from-a");
    // Clearing in A (empty retained publish) leaves B intact.
    let (meta, payload) = encode_publish_meta("mt2/retained", 0, 0, true, b"");
    let (_, _) = apply_publish(&publish_frame(201, 4, meta, payload), &shared).await;
    for (conn, client, user, expect_len, expect) in [
        (205u64, "mtR-a3", "alice", 0usize, b"".as_slice()),
        (206u64, "mtR-b3", "bob", 1usize, b"from-b".as_slice()),
    ] {
        let meta = encode_bind_meta_creds(client, true, 60, user, b"pw");
        let reply = apply_bind(&bind_frame(conn, 1, meta), &shared)
            .await
            .expect("bind replies");
        let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
        assert_eq!(rc, 0);
        let sub = subscribe_frame(
            conn,
            1,
            encode_subscribe_meta(1, client, &[("mt2/retained", 0)]),
        );
        let (reply, retained) = apply_subscribe(&sub, &shared).await;
        reply.expect("subscribe replies");
        assert_eq!(retained.len(), expect_len, "clear stays in {client}");
        if expect_len == 1 {
            assert_eq!(retained[0].payload.as_ref(), expect);
        }
    }
}

#[tokio::test]
async fn sessions_same_client_id_in_two_tenants_are_independent() {
    // MT-03 through the real broker path (connect, publish,
    // deliver): the same client id in tenant A and tenant B
    // connects concurrently with durable sessions; each side
    // publishes while the other is offline and, on reconnect,
    // receives only its own tenant's offline queue; a takeover in
    // A never disconnects B.
    use broker_session::tenant::TenantRule;
    let shared = test_shared();
    shared
        .tenant_registry
        .replace(vec![TenantRule::new("", "tenant", "u-${username}")])
        .expect("tenant rules store");

    // Same client id, two tenants, durable sessions, concurrently.
    for (conn, user) in [(301u64, "alice"), (302u64, "bob")] {
        let meta = encode_bind_meta_creds("mt3-dup", false, 60, user, b"pw");
        let reply = apply_bind(&bind_frame(conn, 1, meta), &shared)
            .await
            .expect("bind replies");
        let (_, present, rc) = decode_session_binding_meta(&reply.metadata);
        assert_eq!(rc, 0);
        assert!(!present, "first durable bind must be fresh");
    }
    let session_a = shared
        .sessions
        .get_in_tenant("u-alice", "mt3-dup")
        .expect("A session");
    let session_b = shared
        .sessions
        .get_in_tenant("u-bob", "mt3-dup")
        .expect("B session");
    assert_ne!(session_a.id, session_b.id, "two tenants, two sessions");
    assert!(*session_a.connected.read());
    assert!(*session_b.connected.read());
    // The bare default lookup sees neither tenant's session.
    assert!(shared.sessions.get("mt3-dup").is_none());

    // Same filter in both tenants, via each connection's own session.
    // QoS 1 subscriptions so detached QoS 1 publishes queue with
    // effective QoS 1 and replay through the guaranteed mailbox
    // (QoS 0 replays ride the bounded QoS 0 backlog, not `rx`).
    for (conn, client) in [(301u64, "mt3-dup"), (302u64, "mt3-dup")] {
        let sub = subscribe_frame(
            conn,
            1,
            encode_subscribe_meta(1, client, &[("mt3/data", 1)]),
        );
        let (reply, _) = apply_subscribe(&sub, &shared).await;
        reply.expect("subscribe replies");
    }
    // Live publish stays in its own tenant despite the shared id.
    let (meta, payload) = encode_publish_meta("mt3/data", 0, 0, false, b"from-a");
    let (_, deliveries) = apply_publish(&publish_frame(301, 2, meta, payload), &shared).await;
    assert_eq!(
        deliveries.iter().map(|(conn, _)| *conn).collect::<Vec<_>>(),
        vec![301],
        "A's publish reaches only A"
    );
    let (meta, payload) = encode_publish_meta("mt3/data", 0, 0, false, b"from-b");
    let (_, deliveries) = apply_publish(&publish_frame(302, 2, meta, payload), &shared).await;
    assert_eq!(
        deliveries.iter().map(|(conn, _)| *conn).collect::<Vec<_>>(),
        vec![302],
        "B's publish reaches only B"
    );

    // Takeover in A (second connection, same id, same tenant)
    // resumes A's durable session and never disconnects B.
    let meta = encode_bind_meta_creds("mt3-dup", false, 60, "alice", b"pw");
    let reply = apply_bind(&bind_frame(303, 1, meta), &shared)
        .await
        .expect("takeover replies");
    let (_, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
    assert!(present, "durable takeover resumes the same session");
    let resumed_a = shared
        .sessions
        .get_in_tenant("u-alice", "mt3-dup")
        .expect("A session after takeover");
    assert_eq!(resumed_a.id, session_a.id);
    assert_eq!(shared.sessions.key_for_conn(301), None, "old A conn pruned");
    let b_after = shared
        .sessions
        .session_for_conn(302)
        .expect("B still bound after A's takeover");
    assert_eq!(b_after.id, session_b.id, "takeover in A leaves B untouched");
    assert!(*b_after.connected.read());

    // Detach both; each tenant queues exactly one offline message.
    apply_unbind(&unbind_frame(303, 3, "mt3-dup"), &shared);
    apply_unbind(&unbind_frame(302, 3, "mt3-dup"), &shared);
    assert!(!*session_a.connected.read());
    assert!(!*session_b.connected.read());
    for (conn, client, user) in [(304u64, "mt3-pub-a", "alice"), (305u64, "mt3-pub-b", "bob")] {
        let meta = encode_bind_meta_creds(client, true, 60, user, b"pw");
        let reply = apply_bind(&bind_frame(conn, 1, meta), &shared)
            .await
            .expect("publisher binds");
        let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
        assert_eq!(rc, 0);
    }
    let (meta, payload) = encode_publish_meta("mt3/data", 11, 1, false, b"queued-a");
    let (ack, deliveries) = apply_publish(&publish_frame(304, 2, meta, payload), &shared).await;
    assert!(ack.is_some());
    assert!(deliveries.is_empty(), "A detached: queued, not delivered");
    let (meta, payload) = encode_publish_meta("mt3/data", 12, 1, false, b"queued-b");
    let (ack, deliveries) = apply_publish(&publish_frame(305, 2, meta, payload), &shared).await;
    assert!(ack.is_some());
    assert!(deliveries.is_empty(), "B detached: queued, not delivered");
    assert_eq!(session_a.offline_len(), 1);
    assert_eq!(session_b.offline_len(), 1);

    // Reconnect each: every side replays only its own tenant's queue.
    for (conn, user, expect) in [
        (306u64, "alice", b"queued-a".as_slice()),
        (307u64, "bob", b"queued-b".as_slice()),
    ] {
        let meta = encode_bind_meta_creds("mt3-dup", false, 60, user, b"pw");
        let reply = apply_bind(&bind_frame(conn, 1, meta), &shared)
            .await
            .expect("reconnect replies");
        let (_, present, rc) = decode_session_binding_meta(&reply.metadata);
        assert_eq!(rc, 0);
        assert!(present, "durable reconnect resumes within its tenant");
        let (tx, mut rx) = unbounded_channel::<BrokerFrame>();
        shared.conns.register(conn, tx);
        let rebind = bind_frame(
            conn,
            1,
            encode_bind_meta_creds("mt3-dup", false, 60, user, b"pw"),
        );
        assert_eq!(replay_offline(&rebind, &shared).await, 1);
        let got = rx.try_recv().expect("own offline queue replays");
        assert_eq!(
            got.payload.as_ref(),
            expect,
            "{user} must receive only its own tenant's queue"
        );
        assert!(
            rx.try_recv().is_err(),
            "exactly one queued message for {user}"
        );
    }
    assert_eq!(session_a.offline_len(), 0);
    assert_eq!(session_b.offline_len(), 0);
}

#[tokio::test]
async fn tm06_tenant_at_connection_limit_refuses_new_connection() {
    // Bind through the kernel. Tenant A allows one live connection.
    // The second CONNECT in A fails with the documented code.
    // Tenant B still connects. The refusal is counted and logged.
    use broker_session::tenant::TenantRule;
    let shared = test_shared();
    shared
        .tenant_registry
        .replace(vec![TenantRule::new("", "tenant", "t-${username}")])
        .expect("tenant rules store");
    shared.namespaces.create("t-alpha").expect("ns stores");
    shared
        .namespaces
        .put_config(
            "t-alpha",
            serde_json::json!({"limits": {"max_connections": 1}}),
        )
        .expect("limit stores");
    for (conn, client) in [(501u64, "tm06-a1")] {
        let meta = encode_bind_meta_creds(client, true, 60, "alpha", b"pw");
        let reply = apply_bind(&bind_frame(conn, 1, meta), &shared)
            .await
            .expect("bind replies");
        let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
        assert_eq!(rc, 0);
    }
    let meta = encode_bind_meta_creds("tm06-a2", true, 60, "alpha", b"pw");
    let reply = apply_bind(&bind_frame(502, 1, meta), &shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x8B, "full tenant refuses with quota code");
    assert_eq!(shared.metrics.tenant_conn_refused(), 1);
    assert_eq!(shared.tenant_limits.conn_refused(), 1);
    let meta = encode_bind_meta_creds("tm06-b1", true, 60, "beta", b"pw");
    let reply = apply_bind(&bind_frame(503, 1, meta), &shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0, "other tenant still connects");
}

#[tokio::test]
async fn tm06_tenant_at_session_limit_refuses_new_session() {
    // Bind through the kernel. Tenant A allows one session.
    // A second client id in A fails with the documented code.
    // Tenant B still creates sessions.
    use broker_session::tenant::TenantRule;
    let shared = test_shared();
    shared
        .tenant_registry
        .replace(vec![TenantRule::new("", "tenant", "t-${username}")])
        .expect("tenant rules store");
    shared.namespaces.create("t-alpha").expect("ns stores");
    shared
        .namespaces
        .put_config(
            "t-alpha",
            serde_json::json!({"limits": {"max_sessions": 1}}),
        )
        .expect("limit stores");
    let meta = encode_bind_meta_creds("tm06-s1", true, 60, "alpha", b"pw");
    let reply = apply_bind(&bind_frame(511, 1, meta), &shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
    let meta = encode_bind_meta_creds("tm06-s2", true, 60, "alpha", b"pw");
    let reply = apply_bind(&bind_frame(512, 1, meta), &shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x8B, "full tenant refuses with quota code");
    assert_eq!(shared.metrics.tenant_sess_refused(), 1);
    let meta = encode_bind_meta_creds("tm06-sb1", true, 60, "beta", b"pw");
    let reply = apply_bind(&bind_frame(513, 1, meta), &shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0, "other tenant still creates sessions");
}

#[tokio::test]
async fn tm06_tenant_at_retained_limit_drops_new_topic() {
    // Publish through the kernel. Tenant A allows one retained topic.
    // A second retained topic drops but delivery still works.
    // Tenant B still stores retained messages.
    use broker_session::tenant::TenantRule;
    let shared = test_shared();
    shared
        .tenant_registry
        .replace(vec![TenantRule::new("", "tenant", "t-${username}")])
        .expect("tenant rules store");
    shared.namespaces.create("t-alpha").expect("ns stores");
    shared
        .namespaces
        .put_config(
            "t-alpha",
            serde_json::json!({"limits": {"max_retained": 1}}),
        )
        .expect("limit stores");
    shared.retained.set_retained_per_tenant_cap("t-alpha", 1);
    for (conn, client, user) in [(521u64, "tm06-ra", "alpha"), (522u64, "tm06-rb", "beta")] {
        let meta = encode_bind_meta_creds(client, true, 60, user, b"pw");
        let reply = apply_bind(&bind_frame(conn, 1, meta), &shared)
            .await
            .expect("bind replies");
        let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
        assert_eq!(rc, 0);
    }
    let (meta, payload) = encode_publish_meta("tm06/r1", 0, 0, true, b"keep-a");
    let _ = apply_publish(&publish_frame(521, 2, meta, payload), &shared).await;
    let sub = subscribe_frame(
        521,
        1,
        encode_subscribe_meta(1, "tm06-ra", &[("tm06/r2", 0)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");
    let (meta, payload) = encode_publish_meta("tm06/r2", 0, 0, true, b"drop-a");
    let (_, deliveries) = apply_publish(&publish_frame(521, 3, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1, "delivery still works at the limit");
    let stored = shared
        .retained
        .get_retained_in_tenant(&broker_protocol::Topic::new("tm06/r2").unwrap(), "t-alpha")
        .await
        .expect("retained reads");
    assert!(stored.is_none(), "new topic drops at the limit");
    assert_eq!(shared.metrics.tenant_retained_dropped(), 1);
    let (meta, payload) = encode_publish_meta("tm06/r2", 0, 0, true, b"keep-b");
    let _ = apply_publish(&publish_frame(522, 2, meta, payload), &shared).await;
    let stored = shared
        .retained
        .get_retained_in_tenant(&broker_protocol::Topic::new("tm06/r2").unwrap(), "t-beta")
        .await
        .expect("retained reads");
    assert!(stored.is_some(), "other tenant still stores");
}

#[tokio::test]
async fn tm06_limits_apply_from_file_and_env_and_api() {
    // File and env entries drive the defaults. The API entry
    // drives the per-tenant override. Sockets observe the result.
    let dir = std::env::temp_dir().join(format!(
        "tm06-cfg-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&dir).expect("create config dir");
    std::fs::write(
        dir.join("indra.toml"),
        "[quotas]\nmax_connections_per_tenant = 7\nmax_sessions_per_tenant = 8\nmax_retained_per_tenant = 9\n",
    )
    .expect("write indra.toml");
    let env = vec![(
        "INDRA_QUOTAS__MAX_SESSIONS_PER_TENANT".to_string(),
        "8".to_string(),
    )];
    let layered = broker_config::layers::load_layered_with_env(
        &dir,
        &env,
        &broker_config::layers::CliOverrides::default(),
    )
    .expect("layers load");
    assert_eq!(layered.config().quotas.max_connections_per_tenant, 7);
    assert_eq!(layered.config().quotas.max_sessions_per_tenant, 8);
    assert_eq!(layered.config().quotas.max_retained_per_tenant, 9);
    let shared = test_shared();
    apply_tenant_limit_defaults(&shared, layered.config());
    assert_eq!(shared.tenant_limits.defaults().max_connections, 7);
    assert_eq!(shared.tenant_limits.defaults().max_sessions, 8);
    assert_eq!(shared.tenant_limits.defaults().max_retained, 9);
    shared.namespaces.create("t-api").expect("ns stores");
    shared
        .namespaces
        .put_config(
            "t-api",
            serde_json::json!({"limits": {"max_connections": 2}}),
        )
        .expect("limit stores");
    let effective = effective_tenant_limits(&shared, "t-api").expect("limits resolve");
    assert_eq!(effective.max_connections, 2);
    assert_eq!(effective.max_sessions, 8);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn tm06_default_tenant_holds_no_bound_through_broker() {
    // File quotas cap managed tenants only. Two binds in the
    // default tenant both succeed past a file limit of one, while
    // a managed tenant with a limit of one refuses the second
    // bind. Goes through the broker bind path.
    use broker_session::tenant::TenantRule;
    let shared = test_shared();
    shared
        .tenant_limits
        .set_defaults(broker_session::tenant_limits::TenantLimits {
            max_connections: 1,
            max_sessions: 1,
            max_retained: 1,
        });
    for (conn, client) in [(611u64, "tm06-d1"), (612u64, "tm06-d2")] {
        let meta = encode_bind_meta(client, true, 60);
        let reply = apply_bind(&bind_frame(conn, 1, meta), &shared)
            .await
            .expect("bind replies");
        let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
        assert_eq!(rc, 0x00, "default tenant stays unlimited");
    }
    shared
        .tenant_registry
        .replace(vec![TenantRule::new("", "tenant", "q-${username}")])
        .expect("tenant rules store");
    shared.namespaces.create("q-cap").expect("ns stores");
    shared
        .namespaces
        .put_config(
            "q-cap",
            serde_json::json!({"limits": {"max_connections": 1}}),
        )
        .expect("limit stores");
    for (conn, client, want) in [(613u64, "tm06-c1", 0x00), (614u64, "tm06-c2", 0x8B)] {
        let meta = encode_bind_meta_creds(client, true, 60, "cap", b"pw");
        let reply = apply_bind(&bind_frame(conn, 1, meta), &shared)
            .await
            .expect("bind replies");
        let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
        assert_eq!(rc, want, "managed tenant enforces its limit");
    }
}

#[tokio::test]
async fn tm06_unreachable_limit_store_denies_new_use() {
    // Fail closed: an unreachable store denies new connections.
    use broker_session::tenant::TenantRule;
    let shared = test_shared();
    shared
        .tenant_registry
        .replace(vec![TenantRule::new("", "tenant", "t-${username}")])
        .expect("tenant rules store");
    shared.tenant_limits.set_unreachable(true);
    let meta = encode_bind_meta_creds("tm06-u1", true, 60, "alpha", b"pw");
    let reply = apply_bind(&bind_frame(531, 1, meta), &shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x8B, "unreachable store denies");
    shared.tenant_limits.set_unreachable(false);
}

#[tokio::test]
async fn mt07_kernel_records_tenant_on_clients_and_sessions_through_broker() {
    // MT-07 through the real broker path (connect, publish,
    // deliver): the same client id in tenant A and tenant B holds
    // two records with the two tenant ids (session record,
    // connection-table label and mirrored subscription row);
    // publishes in each tenant still deliver only within that
    // tenant (recording did not leak isolation); existing
    // client/subscription reads keep serving both tenants without
    // cross-tenant delivery.
    use broker_session::tenant::TenantRule;
    let shared = test_shared();
    shared
        .tenant_registry
        .replace(vec![TenantRule::new("", "tenant", "u-${username}")])
        .expect("tenant rules store");

    // Same client id, two tenants, through the kernel bind hook.
    for (conn, user) in [(401u64, "alice"), (402u64, "bob")] {
        let meta = encode_bind_meta_creds("mt7-dup", true, 60, user, b"pw");
        let reply = apply_bind(&bind_frame(conn, 1, meta), &shared)
            .await
            .expect("bind replies");
        let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
        assert_eq!(rc, 0);
        // Mirror of the connection bind hook
        // (`handle_inbound_frame_with`): register the mailbox, then stamp the
        // client label and the tenant label from the session record
        // the bind just wrote.
        let (tx, _rx) = unbounded_channel::<BrokerFrame>();
        shared.conns.register(conn, tx);
        shared.conns.set_client_label(conn, "mt7-dup");
        let key = shared
            .sessions
            .key_for_conn(conn)
            .expect("fresh bind is indexed");
        shared.conns.set_tenant_label(conn, &key.tenant);
    }

    // The kernel holds two records with the two tenant ids.
    let tenant_a = shared
        .sessions
        .get_in_tenant("u-alice", "mt7-dup")
        .expect("A session")
        .tenant_id
        .read()
        .clone();
    let tenant_b = shared
        .sessions
        .get_in_tenant("u-bob", "mt7-dup")
        .expect("B session")
        .tenant_id
        .read()
        .clone();
    assert_eq!(tenant_a, "u-alice");
    assert_eq!(tenant_b, "u-bob");

    // Connection-table labels carry the owning tenant.
    assert_eq!(
        shared.conns.tenant_for(401).as_deref(),
        Some("u-alice"),
        "conn label carries the owning tenant"
    );
    assert_eq!(shared.conns.tenant_for(402).as_deref(), Some("u-bob"));
    assert_eq!(shared.conns.conns_in_tenant("u-alice"), vec![401]);
    assert_eq!(shared.conns.conns_in_tenant("u-bob"), vec![402]);

    // Tenant-tagged client records are distinct per tenant.
    let info_a = shared
        .sessions
        .client_info_in_tenant("u-alice", "mt7-dup")
        .expect("A record");
    let info_b = shared
        .sessions
        .client_info_in_tenant("u-bob", "mt7-dup")
        .expect("B record");
    assert_eq!(info_a.tenant_id, "u-alice");
    assert_eq!(info_b.tenant_id, "u-bob");
    assert_ne!(info_a.session_id, info_b.session_id);

    // Same filter in both tenants, via each connection's own
    // session (the subscribe path resolves the tenant from the
    // bound connection, never from the bare client id).
    for (conn, client) in [(401u64, "mt7-dup"), (402u64, "mt7-dup")] {
        let sub = subscribe_frame(
            conn,
            1,
            encode_subscribe_meta(1, client, &[("mt7/shared", 0)]),
        );
        let (reply, _) = apply_subscribe(&sub, &shared).await;
        reply.expect("subscribe replies");
    }
    // The mirrored subscription rows carry the owning tenant each.
    let rows_a = shared.sessions.global_subscriptions_in_tenant("u-alice");
    assert_eq!(rows_a.len(), 1);
    assert_eq!(rows_a[0].tenant, "u-alice");
    let rows_b = shared.sessions.global_subscriptions_in_tenant("u-bob");
    assert_eq!(rows_b.len(), 1);
    assert_eq!(rows_b[0].tenant, "u-bob");

    // A publish in A delivers only in A; a publish in B only in B.
    let (meta, payload) = encode_publish_meta("mt7/shared", 0, 0, false, b"from-a");
    let (_, deliveries) = apply_publish(&publish_frame(401, 2, meta, payload), &shared).await;
    let conns: Vec<u64> = deliveries.iter().map(|(conn, _)| *conn).collect();
    assert_eq!(conns, vec![401], "A's publish stays in A");
    let (meta, payload) = encode_publish_meta("mt7/shared", 0, 0, false, b"from-b");
    let (_, deliveries) = apply_publish(&publish_frame(402, 3, meta, payload), &shared).await;
    let conns: Vec<u64> = deliveries.iter().map(|(conn, _)| *conn).collect();
    assert_eq!(conns, vec![402], "B's publish stays in B");

    // Existing reads keep serving both tenants: the global client
    // list and the global subscription index still cover both
    // records with today's shapes, and the per-tenant queries split
    // them exactly.
    let mut global_ids = shared.sessions.active_client_ids();
    global_ids.sort();
    assert_eq!(global_ids, vec!["mt7-dup", "mt7-dup"]);
    assert_eq!(
        shared.sessions.active_client_ids_in_tenant("u-alice"),
        vec!["mt7-dup"]
    );
    assert_eq!(
        shared.sessions.active_client_ids_in_tenant("u-bob"),
        vec!["mt7-dup"]
    );
    assert_eq!(shared.sessions.global_subscriptions().len(), 2);
    assert_eq!(shared.sessions.connected_count(), 2);
}

#[tokio::test]
async fn mt07_default_tenant_reads_keep_todays_shapes_through_broker() {
    // MT-07 default tenant through the real broker path: the
    // client, subscription, topic and session-count reads return
    // today's shapes unchanged (no tenant field on the wire), while
    // the records underneath already carry the default tenant tag.
    let shared = test_shared();
    assert!(shared.tenant_registry.is_empty());
    let frame = bind_frame(403, 1, encode_bind_meta("mt7-default-1", true, 60));
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
    let (tx, _rx) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(403, tx);
    shared.conns.set_client_label(403, "mt7-default-1");
    let key = shared
        .sessions
        .key_for_conn(403)
        .expect("fresh bind is indexed");
    shared.conns.set_tenant_label(403, &key.tenant);

    let sub = subscribe_frame(
        403,
        1,
        encode_subscribe_meta(1, "mt7-default-1", &[("mt7/plain", 0)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");
    let (meta, payload) = encode_publish_meta("mt7/plain", 0, 0, false, b"ping");
    let (_, deliveries) = apply_publish(&publish_frame(403, 2, meta, payload), &shared).await;
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].0, 403);

    // Records underneath carry the default tenant tag.
    let info = shared
        .sessions
        .client_info("mt7-default-1")
        .expect("default record");
    assert_eq!(info.tenant_id, broker_session::tenant::DEFAULT_TENANT_ID);
    assert_eq!(
        shared.conns.tenant_for(403).as_deref(),
        Some(broker_session::tenant::DEFAULT_TENANT_ID)
    );
    // ... while the management shapes stay exactly as before: no
    // tenant field on the wire, bare client-id list, exact session
    // count.
    let wire = serde_json::to_value(&info).expect("serializes");
    assert!(wire.get("tenant_id").is_none());
    assert!(wire.get("tenant").is_none());
    assert_eq!(
        wire.get("client_id").and_then(|v| v.as_str()),
        Some("mt7-default-1")
    );
    assert_eq!(shared.sessions.active_client_ids(), vec!["mt7-default-1"]);
    assert_eq!(
        shared
            .sessions
            .active_client_ids_in_tenant(broker_session::tenant::DEFAULT_TENANT_ID),
        vec!["mt7-default-1"]
    );
    assert_eq!(shared.sessions.global_subscriptions().len(), 1);
    assert_eq!(shared.sessions.connected_count(), 1);
}

#[tokio::test]
async fn sessions_default_tenant_takeover_resume_unchanged() {
    // MT-03 default tenant through the tenant-aware bind path:
    // clean-start takeover, durable resume and offline-queue
    // redelivery behave exactly as before the change.
    let shared = test_shared();
    assert!(shared.tenant_registry.is_empty());

    // Clean-start takeover replaces the session and prunes the old
    // connection.
    let reply = apply_bind(
        &bind_frame(401, 1, encode_bind_meta("mt3-clean", true, 60)),
        &shared,
    )
    .await
    .expect("bind replies");
    let (first_id, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
    assert!(!present);
    let reply = apply_bind(
        &bind_frame(402, 1, encode_bind_meta("mt3-clean", true, 60)),
        &shared,
    )
    .await
    .expect("takeover replies");
    let (second_id, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
    assert!(!present, "clean start always creates");
    assert_ne!(first_id, second_id);
    assert_eq!(shared.sessions.key_for_conn(401), None);

    // Durable disconnect/reconnect resumes the same session.
    let reply = apply_bind(
        &bind_frame(403, 1, encode_bind_meta("mt3-durable", false, 60)),
        &shared,
    )
    .await
    .expect("bind replies");
    let (durable_id, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
    assert!(!present);
    let sub = subscribe_frame(
        403,
        2,
        encode_subscribe_meta(1, "mt3-durable", &[("mt3/keep", 1)]),
    );
    let (reply, _) = apply_subscribe(&sub, &shared).await;
    reply.expect("subscribe replies");
    apply_unbind(&unbind_frame(403, 3, "mt3-durable"), &shared);

    // While detached, a QoS 1 publish queues for the durable session.
    let reply = apply_bind(
        &bind_frame(405, 1, encode_bind_meta("mt3-sender", true, 60)),
        &shared,
    )
    .await
    .expect("sender binds");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
    let (meta, payload) = encode_publish_meta("mt3/keep", 21, 1, false, b"kept");
    let (ack, deliveries) = apply_publish(&publish_frame(405, 2, meta, payload), &shared).await;
    assert!(ack.is_some());
    assert!(deliveries.is_empty());

    let reply = apply_bind(
        &bind_frame(404, 1, encode_bind_meta("mt3-durable", false, 60)),
        &shared,
    )
    .await
    .expect("reconnect replies");
    let (resumed_id, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
    assert!(present, "durable reconnect resumes");
    assert_eq!(resumed_id, durable_id);
    let (tx, mut rx) = unbounded_channel::<BrokerFrame>();
    shared.conns.register(404, tx);
    let rebind = bind_frame(404, 1, encode_bind_meta("mt3-durable", false, 60));
    assert_eq!(replay_offline(&rebind, &shared).await, 1);
    let got = rx.try_recv().expect("offline queue redelivers");
    assert_eq!(got.payload.as_ref(), b"kept");
}

#[tokio::test]
async fn tenant_default_shared_round_trip_unchanged() {
    // Single-tenant installs behave exactly as before, including a
    // shared subscription: one group member plus the plain
    // subscriber per publish, rotating deterministically, with no
    // tenant-mismatch counts.
    let shared = test_shared();
    assert!(shared.tenant_registry.is_empty());
    for (conn, client) in [(111u64, "mt2-d1"), (112u64, "mt2-d2"), (113u64, "mt2-d3")] {
        let reply = apply_bind(
            &bind_frame(conn, 1, encode_bind_meta(client, true, 60)),
            &shared,
        )
        .await
        .expect("bind replies");
        let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
        assert_eq!(rc, 0);
    }
    for (conn, client, filter) in [
        (111u64, "mt2-d1", "$share/mt2pool/mt2/jobs"),
        (112u64, "mt2-d2", "$share/mt2pool/mt2/jobs"),
        (113u64, "mt2-d3", "mt2/jobs"),
    ] {
        let sub = subscribe_frame(conn, 1, encode_subscribe_meta(1, client, &[(filter, 0)]));
        let (reply, _) = apply_subscribe(&sub, &shared).await;
        reply.expect("subscribe replies");
    }
    let mut owners = Vec::new();
    for seq in 0..4u64 {
        let (meta, payload) = encode_publish_meta("mt2/jobs", 0, 0, false, b"job");
        let (_, deliveries) =
            apply_publish(&publish_frame(111, 10 + seq, meta, payload), &shared).await;
        assert_eq!(
            deliveries.len(),
            2,
            "one group member plus the plain subscriber"
        );
        let mut conns: Vec<u64> = deliveries.iter().map(|(conn, _)| *conn).collect();
        conns.sort_unstable();
        assert!(conns.contains(&113), "plain subscriber always present");
        owners.push(if conns[0] == 113 { conns[1] } else { conns[0] });
    }
    assert_eq!(owners, vec![111, 112, 111, 112]);
    assert_eq!(shared.metrics.tenant_mismatch_dropped(), 0);
}

#[tokio::test]
async fn tenant_cert_cn_drives_assignment_through_bind() {
    // The TLS certificate common name forwarded in the bind drives
    // the tenant expression through the same `apply_bind` path.
    use broker_session::tenant::{TenantRule, DEFAULT_TENANT_ID};
    let shared = test_shared();
    shared
        .tenant_registry
        .replace(vec![TenantRule::new("", "tenant", "cn-${cert_cn}")])
        .expect("tenant rules store");
    let base = encode_bind_meta("mt-cert-1", true, 60);
    let meta = encode_bind_meta_cert(base, Some("sensor-9"), None, &[]);
    let frame = bind_frame(94, 1, meta);
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
    // MT-03: sessions are keyed by `(tenant, client id)`, so the
    // lookup is qualified with the rendered tenant.
    let tenant = shared
        .sessions
        .get_in_tenant("cn-sensor-9", "mt-cert-1")
        .expect("session")
        .tenant_id
        .read()
        .clone();
    assert_eq!(tenant, "cn-sensor-9");
    // No certificate: the same expression falls back to default.
    let frame = bind_frame(95, 1, encode_bind_meta("mt-nocert-1", true, 60));
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
    let tenant = shared
        .sessions
        .get("mt-nocert-1")
        .expect("session")
        .tenant_id
        .read()
        .clone();
    assert_eq!(tenant, DEFAULT_TENANT_ID);
}

#[tokio::test]
async fn tenant_overlong_render_rejected_to_default_through_bind() {
    // An overlong rendered id is rejected to the default tenant
    // through the broker connect path, counted and logged.
    use broker_session::tenant::{TenantRule, DEFAULT_TENANT_ID, MAX_TENANT_ID_LEN};
    let shared = test_shared();
    let long = "t-".to_string() + &"x".repeat(MAX_TENANT_ID_LEN);
    shared
        .tenant_registry
        .replace(vec![TenantRule::new("", "tenant", long)])
        .expect("long rule stores");
    let before = shared.tenant_registry.fallback_count();
    let frame = bind_frame(96, 1, encode_bind_meta("mt-long-1", true, 60));
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0, "overlong tenant must not refuse the connect");
    let tenant = shared
        .sessions
        .get("mt-long-1")
        .expect("session")
        .tenant_id
        .read()
        .clone();
    assert_eq!(tenant, DEFAULT_TENANT_ID);
    assert_eq!(shared.tenant_registry.fallback_count(), before + 1);
}
