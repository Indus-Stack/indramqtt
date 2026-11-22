use super::*;
use crate::link_harness::handle_connection;
use tokio::net::TcpListener;

#[tokio::test]
async fn bind_auth_rejects_bad_password_with_0x86() {
    let shared = test_shared();
    shared
        .auth
        .add_user("alice", b"s3cret")
        .expect("memory-only persist cannot fail");

    let frame = bind_frame(
        81,
        1,
        encode_bind_meta_creds("dev-x", true, 60, "alice", b"wrong"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    assert_eq!(reply.header.opcode, OpCode::SessionBinding);
    let (_, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x86, "bad password must yield return code 0x86");
    assert!(!present);
    // Failed binds create no session.
    assert!(shared.sessions.get("dev-x").is_none());
}

#[tokio::test]
async fn bind_auth_rejects_anonymous_when_users_exist() {
    let shared = test_shared();
    shared
        .auth
        .add_user("alice", b"s3cret")
        .expect("memory-only persist cannot fail");

    // Anonymous bind is rejected with 0x87 and creates no session.
    let frame = bind_frame(83, 1, encode_bind_meta("dev-z", true, 60));
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x87, "anonymous bind must yield return code 0x87");
    assert!(!present);
    assert!(shared.sessions.get("dev-z").is_none());

    // A valid credentialed bind in the same store still gets rc 0.
    let frame = bind_frame(
        82,
        1,
        encode_bind_meta_creds("dev-y", true, 60, "alice", b"s3cret"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
}

#[tokio::test]
async fn bind_auth_accepts_anonymous_when_no_users() {
    let shared = test_shared();

    let frame = bind_frame(83, 1, encode_bind_meta("dev-z", true, 60));
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
}

#[tokio::test]
async fn bind_auth_rejects_anonymous_when_allow_flag_is_false() {
    // Fail closed: the flag refuses an anonymous CONNECT while the
    // user store is empty.
    let mut shared = test_shared();
    shared.allow_anonymous = false;

    let frame = bind_frame(84, 1, encode_bind_meta("dev-f", true, 60));
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x87, "anonymous bind must yield return code 0x87");
    assert!(!present);
    assert!(shared.sessions.get("dev-f").is_none());
}

#[tokio::test]
async fn bind_auth_rejects_anonymous_with_allow_flag_when_users_exist() {
    // `--allow-anonymous` is ignored once users exist: a non-empty
    // user store always rejects unauthenticated clients with 0x87.
    let mut shared = test_shared();
    shared.allow_anonymous = true;
    shared
        .auth
        .add_user("alice", b"s3cret")
        .expect("memory-only persist cannot fail");

    let frame = bind_frame(83, 1, encode_bind_meta("dev-z", true, 60));
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x87, "anonymous bind must yield return code 0x87");
    assert!(!present);
    assert!(shared.sessions.get("dev-z").is_none());
}

#[tokio::test]
async fn bind_auth_accepts_anonymous_with_allow_flag_when_store_empty() {
    // While the user store is empty the flag keeps its meaning.
    let mut shared = test_shared();
    shared.allow_anonymous = true;

    let frame = bind_frame(83, 1, encode_bind_meta("dev-z", true, 60));
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
}

#[tokio::test]
async fn password_hashing_migrated_credential_binds_end_to_end() {
    // B5-01: CONNECT with a valid migrated credential succeeds through
    // the real listener and kernel, and a wrong password is refused
    // with 0x86. No fake auth channel: real TCP plus
    // `handle_connection`, the same entry point the edge uses. The
    // legacy SHA-256 entry migrates to the default (Argon2id) on its
    // first successful login. Hashing is local computation
    // (QUAL-NONE: no external server).
    let shared = test_shared();
    shared.auth.set_policy(PasswordHashPolicy::for_tests());
    shared
        .auth
        .add_user_with_algorithm("migr", b"migr-secret", PasswordAlgorithm::Sha256Legacy)
        .expect("memory-only persist cannot fail");
    shared
        .auth
        .add_user("fast", b"fast-secret")
        .expect("memory-only persist cannot fail");
    assert_eq!(
        shared
            .auth
            .verifier_string("migr")
            .expect("legacy stored")
            .len(),
        64,
        "legacy verifier is 64 hex chars before migration"
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let server_shared = shared.clone();
    let server = tokio::spawn(async move {
        for _ in 0..4 {
            let (stream, _) = listener.accept().await.expect("accept");
            let owned = server_shared.clone();
            tokio::spawn(async move {
                handle_connection(stream, owned).await.expect("handle");
            });
        }
        std::future::pending::<()>().await;
    });

    async fn bind_rc(
        addr: std::net::SocketAddr,
        conn_id: u64,
        client_id: &str,
        username: &str,
        password: &[u8],
    ) -> u8 {
        let io = tokio::net::TcpStream::connect(addr)
            .await
            .expect("connect client");
        let client = FramedTransport::new(io);
        client
            .send(bind_frame(
                conn_id,
                1,
                encode_bind_meta_creds(client_id, true, 60, username, password),
            ))
            .await
            .expect("bind");
        let reply = client.recv().await.expect("recv binding");
        assert_eq!(reply.header.opcode, OpCode::SessionBinding);
        decode_session_binding_meta(&reply.metadata).2
    }

    // Valid legacy credential binds and migrates to the default.
    assert_eq!(
        bind_rc(addr, 911, "migr-1", "migr", b"migr-secret").await,
        0
    );
    let migrated = shared.auth.verifier_string("migr").expect("migrated");
    assert!(
        migrated.starts_with("$argon2id$"),
        "legacy entry must migrate to Argon2id, got {migrated}"
    );
    // Wrong password for the migrated entry is refused.
    assert_eq!(
        bind_rc(addr, 912, "migr-2", "migr", b"wrong-secret").await,
        0x86
    );
    // Valid default-algorithm credential binds.
    assert_eq!(
        bind_rc(addr, 913, "fast-1", "fast", b"fast-secret").await,
        0
    );
    // Wrong password is refused.
    assert_eq!(
        bind_rc(addr, 914, "fast-2", "fast", b"wrong-secret").await,
        0x86
    );

    server.abort();
}

/// B2-01: directory authentication through the broker CONNECT path.
///
/// Starts the in-tree test directory server speaking the LDAP wire
/// protocol and drives CONNECTs through `apply_bind`, the same entry
/// point the BrokerLink edge uses. Valid directory binds are accepted,
/// wrong passwords, unknown users and wrong-group users are refused
/// with 0x86, anonymous is refused with 0x87 while the directory is
/// configured, and an unreachable directory fails closed.
#[tokio::test]
async fn ldap_bind_through_broker_connect() {
    const BASE_DN: &str = "dc=example,dc=com";
    const SERVICE_DN: &str = "cn=reader,dc=example,dc=com";
    const SERVICE_PW: &str = "readerpw";
    const REQUIRED_GROUP: &str = "cn=mqtt-users,ou=groups,dc=example,dc=com";

    let dir_yaml = format!(
        "directory:\n  base_dn: {BASE_DN}\nentries:\n  - dn: {BASE_DN}\n    objectClass: [top, domain]\n    dc: example\n  - dn: ou=users,{BASE_DN}\n    objectClass: [top, organizationalUnit]\n    ou: users\n  - dn: ou=groups,{BASE_DN}\n    objectClass: [top, organizationalUnit]\n    ou: groups\n  - dn: {SERVICE_DN}\n    objectClass: [top, person]\n    cn: reader\n    sn: reader\n    userPassword: {SERVICE_PW}\n  - dn: uid=alice,ou=users,{BASE_DN}\n    objectClass: [top, person, inetOrgPerson]\n    uid: alice\n    cn: Alice\n    sn: Alice\n    userPassword: alicepw\n    memberOf: {REQUIRED_GROUP}\n  - dn: uid=bob,ou=users,{BASE_DN}\n    objectClass: [top, person, inetOrgPerson]\n    uid: bob\n    cn: Bob\n    sn: Bob\n    userPassword: bobpw\n    memberOf: cn=other-group,ou=groups,{BASE_DN}\n  - dn: {REQUIRED_GROUP}\n    objectClass: [top, groupOfNames]\n    cn: mqtt-users\n    member: uid=alice,ou=users,{BASE_DN}\n"
    );
    let dir_file = tempfile::NamedTempFile::new().expect("temp directory YAML");
    std::fs::write(dir_file.path(), dir_yaml).expect("write directory YAML");
    let directory_config = crate::test_directory_server::Config::new(dir_file.path())
        .with_bind_address("127.0.0.1:0".parse().expect("loopback"));
    let directory = crate::test_directory_server::Server::new(directory_config)
        .await
        .expect("directory starts");
    let handle = directory.start().await.expect("directory listens");
    let url = format!("ldap://{}", handle.local_addr());

    let ldap_config = LdapConfig {
        server_url: url,
        base_dn: BASE_DN.to_string(),
        bind_dn: SERVICE_DN.to_string(),
        bind_password: SERVICE_PW.to_string(),
        user_filter: "(uid={username})".to_string(),
        group_attribute: "memberOf".to_string(),
        required_group: REQUIRED_GROUP.to_string(),
        pool_size: 4,
        connect_timeout_ms: 3_000,
        read_timeout_ms: 3_000,
        timeout_ms: 0,
        tls_verify: true,
        ca_cert_path: None,
        bind_dn_template: String::new(),
        filter_template: String::new(),
    };
    let mut shared = test_shared();
    shared.ldap = Some(std::sync::Arc::new(LdapAuthenticator::new(ldap_config)));
    // Local users keep working alongside the directory.
    shared
        .auth
        .add_user("local-user", b"localpw")
        .expect("seed local user");

    // Valid directory bind through the broker: accepted.
    let frame = bind_frame(
        91,
        1,
        encode_bind_meta_creds("ldap-device-1", true, 60, "alice", b"alicepw"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0, "valid directory bind must be accepted");
    assert!(shared.sessions.get("ldap-device-1").is_some());

    // Local user still authenticates with the directory configured.
    let frame = bind_frame(
        92,
        1,
        encode_bind_meta_creds("local-device", true, 60, "local-user", b"localpw"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0, "local users keep working with LDAP enabled");

    // Wrong directory password: refused with 0x86, no session.
    let frame = bind_frame(
        93,
        1,
        encode_bind_meta_creds("ldap-device-2", true, 60, "alice", b"wrong"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x86, "wrong directory password must yield 0x86");
    assert!(!present);
    assert!(shared.sessions.get("ldap-device-2").is_none());

    // Unknown directory user: refused with 0x86.
    let frame = bind_frame(
        94,
        1,
        encode_bind_meta_creds("ldap-device-3", true, 60, "mallory", b"anything"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x86, "unknown directory user must yield 0x86");

    // Correct password but wrong group: authorised as denied.
    let frame = bind_frame(
        95,
        1,
        encode_bind_meta_creds("ldap-device-4", true, 60, "bob", b"bobpw"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x86, "wrong-group user must be denied with 0x86");
    assert!(shared.sessions.get("ldap-device-4").is_none());

    // Anonymous while a directory is configured: refused with 0x87.
    let frame = bind_frame(96, 1, encode_bind_meta("ldap-anon", true, 60));
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(
        rc, 0x87,
        "anonymous must be refused while LDAP is configured"
    );

    // Unreachable directory fails closed through the same path.
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("ephemeral port");
    let closed_addr = closed.local_addr().expect("local addr");
    drop(closed);
    let down_config = LdapConfig {
        server_url: format!("ldap://{closed_addr}"),
        base_dn: BASE_DN.to_string(),
        bind_dn: SERVICE_DN.to_string(),
        bind_password: SERVICE_PW.to_string(),
        user_filter: "(uid={username})".to_string(),
        group_attribute: "memberOf".to_string(),
        required_group: REQUIRED_GROUP.to_string(),
        pool_size: 2,
        connect_timeout_ms: 1_000,
        read_timeout_ms: 1_000,
        timeout_ms: 0,
        tls_verify: true,
        ca_cert_path: None,
        bind_dn_template: String::new(),
        filter_template: String::new(),
    };
    let mut down_shared = test_shared();
    down_shared.ldap = Some(std::sync::Arc::new(LdapAuthenticator::new(down_config)));
    let frame = bind_frame(
        97,
        1,
        encode_bind_meta_creds("ldap-down", true, 60, "alice", b"alicepw"),
    );
    let reply = apply_bind(&frame, &down_shared)
        .await
        .expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x86, "directory outage must fail closed with 0x86");
    assert!(down_shared.sessions.get("ldap-down").is_none());
}

/// B5-03: database authentication through the broker CONNECT and
/// publish paths.
///
/// A database source pointed at a closed loopback port (no server)
/// fails closed through `apply_bind` (0x86, no session) and through
/// the publish authorizer, and anonymous CONNECT is refused while
/// any database source is configured. This proves the broker calls
/// the lookup on both paths; real-server seeding and verdicts live
/// in the qualification tests in `broker-auth`.
#[tokio::test]
async fn db_auth_unreachable_fails_closed_through_broker() {
    let closed = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("ephemeral port");
    let closed_addr = closed.local_addr().expect("local addr");
    drop(closed);
    let db_config = DbAuthSetConfig {
        postgres_url: format!("postgresql://qual:qualpass1@{closed_addr}/qual"),
        mysql_url: String::new(),
        redis_url: String::new(),
        mongodb_url: String::new(),
        pool_size: 2,
        connect_timeout_ms: 500,
        read_timeout_ms: 500,
        cache_max_entries: 16,
        cache_ttl_secs: 60,
    };
    let mut shared = test_shared();
    shared.db_auth = Some(std::sync::Arc::new(DbAuthSet::new(&db_config)));

    // Credentialed CONNECT against the unreachable database: 0x86.
    let frame = bind_frame(
        201,
        1,
        encode_bind_meta_creds("db-device-1", true, 60, "qualuser", b"qual-pass-1"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x86, "database outage must fail closed with 0x86");
    assert!(shared.sessions.get("db-device-1").is_none());

    // Anonymous while a database is configured: 0x87.
    let frame = bind_frame(202, 1, encode_bind_meta("db-anon", true, 60));
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(
        rc, 0x87,
        "anonymous must be refused while a database is configured"
    );

    // Publish through the database authorizer fails closed too.
    let db = shared.db_auth.as_ref().expect("database configured");
    let topic = Topic::new("qual/allowed".to_string()).expect("valid topic");
    assert!(
        db.authorize_publish("qualuser", &topic).await.is_err(),
        "unreachable database must refuse publish"
    );
}

/// B2-02: Kerberos authentication through the broker CONNECT path.
///
/// Starts an in-process test KDC (real DER via manual encoding that the
/// broker decodes with `kerberos-parser`/`der-parser`, real AES-256-GCM
/// via `ring`) holding the broker service key in a MIT keytab v2 file,
/// then drives CONNECTs through `apply_bind`, the same entry point the
/// edge uses. A valid ticket (bare and SPNEGO-wrapped) is accepted,
/// expired, wrong-service, replayed and malformed tokens are refused
/// with 0x86, the old B1-01 plaintext bypasses still fail, anonymous is
/// refused while Kerberos is enabled, and local users keep working.
#[tokio::test]
async fn kerberos_bind_through_broker_connect() {
    use std::collections::HashMap;
    use std::io::Write as _;

    const REALM: &str = "EXAMPLE.COM";
    const SERVICE_FULL: &str = "mqtt/broker.example.com@EXAMPLE.COM";

    fn krb_len(len: usize) -> Vec<u8> {
        if len < 128 {
            vec![len as u8]
        } else {
            let mut bytes = Vec::new();
            let mut n = len;
            while n > 0 {
                bytes.push((n & 0xFF) as u8);
                n >>= 8;
            }
            bytes.reverse();
            let mut out = vec![0x80 | (bytes.len() as u8)];
            out.extend_from_slice(&bytes);
            out
        }
    }

    fn krb_tlv(tag: u8, content: &[u8]) -> Vec<u8> {
        let mut out = vec![tag];
        out.extend_from_slice(&krb_len(content.len()));
        out.extend_from_slice(content);
        out
    }

    fn krb_u32(v: u32) -> Vec<u8> {
        let mut bytes = v.to_be_bytes().to_vec();
        while bytes.len() > 1 && bytes[0] == 0 {
            bytes.remove(0);
        }
        if bytes[0] & 0x80 != 0 {
            let mut prefixed = vec![0x00];
            prefixed.extend_from_slice(&bytes);
            bytes = prefixed;
        }
        krb_tlv(0x02, &bytes)
    }

    fn krb_general_string(s: &str) -> Vec<u8> {
        krb_tlv(0x1B, s.as_bytes())
    }

    fn krb_octets(b: &[u8]) -> Vec<u8> {
        krb_tlv(0x04, b)
    }

    fn krb_seq(content: &[u8]) -> Vec<u8> {
        krb_tlv(0x30, content)
    }

    fn krb_ctx(n: u8, content: &[u8]) -> Vec<u8> {
        krb_tlv(0xA0 | n, content)
    }

    fn krb_app(n: u8, content: &[u8]) -> Vec<u8> {
        krb_tlv(0x60 | n, content)
    }

    fn krb_oid_content(numbers: &[u64]) -> Vec<u8> {
        let mut out = vec![(numbers[0] * 40 + numbers[1]) as u8];
        for n in &numbers[2..] {
            let mut stack = Vec::new();
            let mut v = *n;
            loop {
                stack.push((v & 0x7F) as u8);
                v >>= 7;
                if v == 0 {
                    break;
                }
            }
            stack.reverse();
            for (i, b) in stack.iter().enumerate() {
                if i + 1 < stack.len() {
                    out.push(b | 0x80);
                } else {
                    out.push(*b);
                }
            }
        }
        out
    }

    fn krb_oid(numbers: &[u64]) -> Vec<u8> {
        krb_tlv(0x06, &krb_oid_content(numbers))
    }

    fn krb_cts_encrypt(key: &[u8; 32], key_usage: i32, plain: &[u8]) -> Vec<u8> {
        use picky_krb::crypto::CipherSuite;
        CipherSuite::Aes256CtsHmacSha196
            .cipher()
            .encrypt(key, key_usage, plain)
            .expect("CTS encrypt")
    }

    fn krb_realm(s: &str) -> picky_krb::data_types::Realm {
        use picky_asn1::restricted_string::IA5String;
        picky_krb::data_types::Realm::from(picky_asn1::wrapper::GeneralStringAsn1::from(
            IA5String::from_string(s.to_string()).expect("realm as IA5"),
        ))
    }

    fn krb_principal(name_type: u8, comps: &[String]) -> picky_krb::data_types::PrincipalName {
        use picky_asn1::restricted_string::IA5String;
        use picky_asn1::wrapper::{Asn1SequenceOf, ExplicitContextTag0, ExplicitContextTag1};
        let strings: Vec<picky_krb::data_types::KerberosStringAsn1> = comps
            .iter()
            .map(|c| {
                picky_krb::data_types::KerberosStringAsn1::from(
                    IA5String::from_string(c.clone()).expect("component as IA5"),
                )
            })
            .collect();
        picky_krb::data_types::PrincipalName {
            name_type: ExplicitContextTag0::from(picky_asn1::wrapper::IntegerAsn1::from(vec![
                name_type,
            ])),
            name_string: ExplicitContextTag1::from(Asn1SequenceOf::from(strings)),
        }
    }

    fn krb_time(secs: i64) -> picky_krb::data_types::KerberosTime {
        use picky_asn1::date::GeneralizedTime;
        let dt = time::OffsetDateTime::from_unix_timestamp(secs).expect("valid time");
        picky_krb::data_types::KerberosTime::from(GeneralizedTime::from(dt))
    }

    fn krb_ticket_inner(
        client: &str,
        _service: &str,
        start: i64,
        end: i64,
        session_key: &[u8],
    ) -> Vec<u8> {
        use picky_asn1::wrapper::{
            ExplicitContextTag0, ExplicitContextTag1, ExplicitContextTag2, ExplicitContextTag3,
            ExplicitContextTag4, ExplicitContextTag5, ExplicitContextTag6, ExplicitContextTag7,
            OctetStringAsn1, Optional,
        };
        use picky_krb::data_types::{
            EncTicketPart, EncTicketPartInner, EncryptionKey, TransitedEncoding,
        };
        let (cname_part, crealm_part) = client.split_once('@').unwrap_or((client, "EXAMPLE.COM"));
        let cname = krb_principal(1, &[cname_part.to_string()]);
        let crealm = krb_realm(crealm_part);
        let enc_part = EncTicketPart::from(EncTicketPartInner {
            flags: ExplicitContextTag0::from(picky_asn1::wrapper::BitStringAsn1::default()),
            key: ExplicitContextTag1::from(EncryptionKey {
                key_type: ExplicitContextTag0::from(picky_asn1::wrapper::IntegerAsn1::from(vec![
                    18u8,
                ])),
                key_value: ExplicitContextTag1::from(OctetStringAsn1::from(session_key.to_vec())),
            }),
            crealm: ExplicitContextTag2::from(crealm),
            cname: ExplicitContextTag3::from(cname),
            transited: ExplicitContextTag4::from(TransitedEncoding {
                tr_type: ExplicitContextTag0::from(picky_asn1::wrapper::IntegerAsn1::from(vec![
                    0u8,
                ])),
                contents: ExplicitContextTag1::from(OctetStringAsn1::from(vec![1u8])),
            }),
            auth_time: ExplicitContextTag5::from(krb_time(start)),
            starttime: Optional::from(Some(ExplicitContextTag6::from(krb_time(start)))),
            endtime: ExplicitContextTag7::from(krb_time(end)),
            renew_till: Optional::from(None),
            caddr: Optional::from(None),
            authorization_data: Optional::from(None),
        });
        picky_asn1_der::to_vec(&enc_part).expect("encode EncTicketPart")
    }

    fn krb_auth_inner(client: &str, ts: i64, usec: u32) -> Vec<u8> {
        use picky_asn1::wrapper::{
            ExplicitContextTag0, ExplicitContextTag1, ExplicitContextTag2, ExplicitContextTag4,
            ExplicitContextTag5, IntegerAsn1, Optional,
        };
        use picky_krb::data_types::{Authenticator, AuthenticatorInner};
        let (cname_part, crealm_part) = client.split_once('@').unwrap_or((client, "EXAMPLE.COM"));
        let cname = krb_principal(1, &[cname_part.to_string()]);
        let crealm = krb_realm(crealm_part);
        let mut usec_bytes = usec.to_be_bytes().to_vec();
        while usec_bytes.len() > 1 && usec_bytes[0] == 0 {
            usec_bytes.remove(0);
        }
        if usec_bytes[0] & 0x80 != 0 {
            let mut prefixed = vec![0x00];
            prefixed.extend_from_slice(&usec_bytes);
            usec_bytes = prefixed;
        }
        let auth = Authenticator::from(AuthenticatorInner {
            authenticator_vno: ExplicitContextTag0::from(IntegerAsn1::from(vec![5u8])),
            crealm: ExplicitContextTag1::from(crealm),
            cname: ExplicitContextTag2::from(cname),
            cksum: Optional::from(None),
            cusec: ExplicitContextTag4::from(IntegerAsn1::from(usec_bytes)),
            ctime: ExplicitContextTag5::from(krb_time(ts)),
            subkey: Optional::from(None),
            seq_number: Optional::from(None),
            authorization_data: Optional::from(None),
        });
        picky_asn1_der::to_vec(&auth).expect("encode Authenticator")
    }

    fn krb_principal_name(comps: &[String]) -> Vec<u8> {
        let mut strings = Vec::new();
        for c in comps {
            strings.extend_from_slice(&krb_general_string(c));
        }
        let seq = krb_seq(&strings);
        let mut content = Vec::new();
        content.extend_from_slice(&krb_ctx(0, &krb_u32(1)));
        content.extend_from_slice(&krb_ctx(1, &seq));
        krb_seq(&content)
    }

    fn krb_encrypted(cipher: &[u8]) -> Vec<u8> {
        let mut content = Vec::new();
        content.extend_from_slice(&krb_ctx(0, &krb_tlv(0x02, &[18])));
        content.extend_from_slice(&krb_ctx(2, &krb_octets(cipher)));
        krb_seq(&content)
    }

    fn krb_ticket(realm: &str, comps: &[String], cipher: &[u8]) -> Vec<u8> {
        let mut content = Vec::new();
        content.extend_from_slice(&krb_ctx(0, &krb_u32(5)));
        content.extend_from_slice(&krb_ctx(1, &krb_general_string(realm)));
        content.extend_from_slice(&krb_ctx(2, &krb_principal_name(comps)));
        content.extend_from_slice(&krb_ctx(3, &krb_encrypted(cipher)));
        krb_app(1, &krb_seq(&content))
    }

    fn krb_ap_req(ticket: &[u8], auth_cipher: &[u8]) -> Vec<u8> {
        let mut content = Vec::new();
        content.extend_from_slice(&krb_ctx(0, &krb_u32(5)));
        content.extend_from_slice(&krb_ctx(1, &krb_u32(14)));
        content.extend_from_slice(&krb_ctx(2, &krb_tlv(0x03, &[0x00])));
        let mut ticket_field = vec![0xA3];
        ticket_field.extend_from_slice(&krb_len(ticket.len()));
        ticket_field.extend_from_slice(ticket);
        content.extend_from_slice(&ticket_field);
        content.extend_from_slice(&krb_ctx(4, &krb_encrypted(auth_cipher)));
        krb_app(14, &krb_seq(&content))
    }

    fn krb_spnego(ap_req: &[u8]) -> Vec<u8> {
        let kerberos_oid = krb_oid(&[1, 2, 840, 113554, 1, 2, 2]);
        let mut mech_list = Vec::new();
        mech_list.extend_from_slice(&kerberos_oid);
        let mech_seq = krb_seq(&mech_list);
        let mut neg = Vec::new();
        neg.extend_from_slice(&krb_ctx(0, &mech_seq));
        neg.extend_from_slice(&krb_ctx(2, &krb_octets(ap_req)));
        let neg_seq = krb_seq(&neg);
        let spnego_oid = krb_oid(&[1, 3, 6, 1, 5, 5, 2]);
        let mut outer = Vec::new();
        outer.extend_from_slice(&spnego_oid);
        outer.extend_from_slice(&neg_seq);
        krb_app(0, &outer)
    }

    fn krb_now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }

    fn krb_random_key() -> [u8; 32] {
        use ring::rand::{SecureRandom, SystemRandom};
        let rng = SystemRandom::new();
        let mut key = [0u8; 32];
        rng.fill(&mut key).expect("system randomness");
        key
    }

    // Test KDC state: service key shared with the keytab file.
    let service_key = krb_random_key();
    let service_comps = vec!["mqtt".to_string(), "broker.example.com".to_string()];
    let dir = tempfile::tempdir().expect("temp dir");
    let keytab_path = dir.path().join("broker.keytab");
    {
        let mut file = vec![0x05u8, 0x02u8];
        let mut entry = Vec::new();
        entry.extend_from_slice(&2u16.to_be_bytes());
        entry.extend_from_slice(&(REALM.len() as u16).to_be_bytes());
        entry.extend_from_slice(REALM.as_bytes());
        for comp in &service_comps {
            entry.extend_from_slice(&(comp.len() as u16).to_be_bytes());
            entry.extend_from_slice(comp.as_bytes());
        }
        entry.extend_from_slice(&1u32.to_be_bytes());
        entry.extend_from_slice(&0u32.to_be_bytes());
        entry.push(1u8);
        entry.extend_from_slice(&18u16.to_be_bytes());
        entry.extend_from_slice(&32u16.to_be_bytes());
        entry.extend_from_slice(&service_key);
        entry.extend_from_slice(&1u32.to_be_bytes());
        file.extend_from_slice(&(entry.len() as i32).to_be_bytes());
        file.extend_from_slice(&entry);
        let mut handle = std::fs::File::create(&keytab_path).expect("write keytab");
        handle.write_all(&file).expect("write keytab");
    }
    let mint = |client: &str, start: i64, end: i64, ts: i64, usec: u32| {
        let session_key = krb_random_key();
        let inner = krb_ticket_inner(client, SERVICE_FULL, start, end, &session_key);
        let ticket_cipher = krb_cts_encrypt(&service_key, 2, &inner);
        let ticket = krb_ticket(REALM, &service_comps, &ticket_cipher);
        let auth_inner = krb_auth_inner(client, ts, usec);
        let auth_cipher = krb_cts_encrypt(&session_key, 11, &auth_inner);
        krb_ap_req(&ticket, &auth_cipher)
    };

    let kerberos_config = KerberosConfig {
        service_principal_name: SERVICE_FULL.to_string(),
        realm: REALM.to_string(),
        allowed_realms: vec![REALM.to_string()],
        keytab_path: keytab_path.to_string_lossy().to_string(),
        clock_skew_secs: 300,
        replay_max_entries: broker_auth::REPLAY_MAX_ENTRIES,
        principal_role_map: HashMap::from([(
            "alice@EXAMPLE.COM".to_string(),
            "publisher".to_string(),
        )]),
    };
    let mut shared = test_shared();
    shared.kerberos = Some(std::sync::Arc::new(KerberosAuthenticator::new(
        kerberos_config,
    )));
    assert!(
        shared.kerberos.as_ref().is_some_and(|k| k.is_enabled()),
        "test keytab must enable Kerberos"
    );
    shared
        .auth
        .add_user("local-user", b"localpw")
        .expect("seed local user");

    // Valid bare AP-REQ through the broker: accepted, principal stamped.
    let now = krb_now();
    let token = mint("alice@EXAMPLE.COM", now - 60, now + 3600, now, 21);
    let frame = bind_frame(
        91,
        1,
        encode_bind_meta_creds("krb-device-1", true, 60, "alice", &token),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0, "valid Kerberos ticket must be accepted");
    let session = shared.sessions.get("krb-device-1").expect("session exists");
    assert_eq!(
        session.username.read().clone(),
        Some("alice@EXAMPLE.COM".to_string()),
        "session must carry the verified principal"
    );
    // Role mapping through the broker (live, not store-only): the
    // broker's authenticator maps the verified principal onto its
    // configured role, and its config carries the map.
    let kerberos = shared.kerberos.as_ref().expect("kerberos enabled");
    assert_eq!(
        kerberos.role_for_principal("alice@EXAMPLE.COM"),
        "publisher"
    );
    assert_eq!(kerberos.role_for_principal("mallory@EXAMPLE.COM"), "user");
    assert_eq!(
        kerberos
            .config()
            .principal_role_map
            .get("alice@EXAMPLE.COM")
            .map(String::as_str),
        Some("publisher")
    );

    // Valid SPNEGO-wrapped AP-REQ: accepted.
    let token = krb_spnego(&mint("alice@EXAMPLE.COM", now - 60, now + 3600, now, 22));
    let frame = bind_frame(
        92,
        1,
        encode_bind_meta_creds("krb-device-2", true, 60, "alice", &token),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0, "valid SPNEGO ticket must be accepted");

    // Local users keep working with Kerberos enabled.
    let frame = bind_frame(
        93,
        1,
        encode_bind_meta_creds("local-device", true, 60, "local-user", b"localpw"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0, "local users keep working with Kerberos enabled");

    // Expired ticket: refused with 0x86, no session.
    let token = mint("alice@EXAMPLE.COM", now - 7200, now - 3600, now - 3600, 23);
    let frame = bind_frame(
        94,
        1,
        encode_bind_meta_creds("krb-device-3", true, 60, "alice", &token),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x86, "expired ticket must yield 0x86");
    assert!(shared.sessions.get("krb-device-3").is_none());

    // Ticket for another service: refused with 0x86.
    let other_key = krb_random_key();
    let other_inner = krb_ticket_inner(
        "alice@EXAMPLE.COM",
        "mqtt/other.example.com@EXAMPLE.COM",
        now - 60,
        now + 3600,
        &krb_random_key(),
    );
    let other_cipher = krb_cts_encrypt(&other_key, 2, &other_inner);
    let other_ticket = krb_ticket(
        REALM,
        &["mqtt".to_string(), "other.example.com".to_string()],
        &other_cipher,
    );
    // Authenticator for the other ticket (session key unknown to the
    // broker, but the outer service check already fails first).
    let dummy_session = krb_random_key();
    let dummy_auth = krb_cts_encrypt(
        &dummy_session,
        11,
        &krb_auth_inner("alice@EXAMPLE.COM", now, 24),
    );
    let other_token = krb_ap_req(&other_ticket, &dummy_auth);
    let frame = bind_frame(
        95,
        1,
        encode_bind_meta_creds("krb-device-4", true, 60, "alice", &other_token),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x86, "wrong-service ticket must yield 0x86");

    // Replay: same token twice, second refused.
    let token = mint("alice@EXAMPLE.COM", now - 60, now + 3600, now, 25);
    let frame = bind_frame(
        96,
        1,
        encode_bind_meta_creds("krb-device-5", true, 60, "alice", &token),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0, "first presentation must succeed");
    let frame = bind_frame(
        97,
        1,
        encode_bind_meta_creds("krb-device-6", true, 60, "alice", &token),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x86, "replayed token must yield 0x86");

    // Malformed token: refused with 0x86.
    let frame = bind_frame(
        98,
        1,
        encode_bind_meta_creds("krb-device-7", true, 60, "alice", b"not a token"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x86, "malformed token must yield 0x86");

    // Old B1-01 bypasses still fail through the broker.
    let legacy = b"KRB5:operator@EXAMPLE.COM:mqtt/broker.example.com@EXAMPLE.COM:EXAMPLE.COM";
    let frame = bind_frame(
        99,
        1,
        encode_bind_meta_creds("krb-device-8", true, 60, "operator", legacy),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x86, "legacy plaintext bypass must yield 0x86");
    let mut tagged = vec![0x6E, legacy.len() as u8];
    tagged.extend_from_slice(legacy);
    let frame = bind_frame(
        100,
        1,
        encode_bind_meta_creds("krb-device-9", true, 60, "operator", &tagged),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x86, "legacy 0x6E bypass must yield 0x86");

    // Anonymous while Kerberos is enabled: refused with 0x87.
    let frame = bind_frame(101, 1, encode_bind_meta("krb-anon", true, 60));
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(
        rc, 0x87,
        "anonymous must be refused while Kerberos is enabled"
    );
}
