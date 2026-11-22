use super::*;
use crate::link_harness::handle_connection;
use tokio::net::TcpListener;

/// Boot one kernel-owned TCP listener on `shared` for socket tests.
/// Returns the loopback port. Follows the orderly-stop test: the
/// kernel serves the row by default, with no edge in the path.
#[cfg(test)]
async fn sock_listener(shared: Shared) -> u16 {
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
    port
}

/// Encode one MQTT remaining length as varint bytes.
#[cfg(test)]
fn sock_remaining(mut len: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            break;
        }
    }
    out
}

/// Build one 3.1.1 CONNECT packet with username and password.
#[cfg(test)]
fn sock_connect_v4(client_id: &str, username: &str, password: &[u8]) -> Vec<u8> {
    let mut body = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0xC2, 0x00, 0x3C];
    body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    body.extend_from_slice(client_id.as_bytes());
    body.extend_from_slice(&(username.len() as u16).to_be_bytes());
    body.extend_from_slice(username.as_bytes());
    body.extend_from_slice(&(password.len() as u16).to_be_bytes());
    body.extend_from_slice(password);
    let mut packet = vec![0x10];
    packet.extend_from_slice(&sock_remaining(body.len()));
    packet.extend_from_slice(&body);
    packet
}

/// Build one MQTT 5 CONNECT packet with username and password.
/// Property length is zero. Long passwords use varint length.
#[cfg(test)]
fn sock_connect_v5(client_id: &str, username: &str, password: &[u8]) -> Vec<u8> {
    let mut body = vec![
        0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0xC2, 0x00, 0x3C, 0x00,
    ];
    body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    body.extend_from_slice(client_id.as_bytes());
    body.extend_from_slice(&(username.len() as u16).to_be_bytes());
    body.extend_from_slice(username.as_bytes());
    body.extend_from_slice(&(password.len() as u16).to_be_bytes());
    body.extend_from_slice(password);
    let mut packet = vec![0x10];
    packet.extend_from_slice(&sock_remaining(body.len()));
    packet.extend_from_slice(&body);
    packet
}

/// Read one full MQTT packet from a socket, at most five seconds.
#[cfg(test)]
async fn sock_read(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
    use tokio::io::AsyncReadExt as _;
    let mut first = [0u8; 1];
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        stream.read_exact(&mut first),
    )
    .await
    .expect("packet in time")
    .expect("socket stays open");
    let mut packet = vec![first[0]];
    let mut multiplier = 1usize;
    let mut remaining = 0usize;
    loop {
        let mut byte = [0u8; 1];
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            stream.read_exact(&mut byte),
        )
        .await
        .expect("length in time")
        .expect("socket stays open");
        packet.push(byte[0]);
        remaining += ((byte[0] & 0x7F) as usize) * multiplier;
        multiplier *= 128;
        if byte[0] & 0x80 == 0 {
            break;
        }
    }
    if remaining > 0 {
        let mut body = vec![0u8; remaining];
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            stream.read_exact(&mut body),
        )
        .await
        .expect("body in time")
        .expect("socket stays open");
        packet.extend_from_slice(&body);
    }
    packet
}

/// Send one CONNECT on a new socket. Return the CONNACK packet.
#[cfg(test)]
async fn sock_connack(port: u16, packet: &[u8]) -> Vec<u8> {
    use tokio::io::AsyncWriteExt as _;
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("tcp dials");
    stream.write_all(packet).await.expect("CONNECT sends");
    sock_read(&mut stream).await
}

/// Return code of a 3.1.1 CONNACK packet.
#[cfg(test)]
fn sock_connack_v4(packet: &[u8]) -> u8 {
    assert_eq!(packet[0], 0x20, "a CONNACK is expected");
    assert!(packet.len() >= 4, "v4 CONNACK carries a code");
    packet[3]
}

/// Reason code of a v5 CONNACK packet.
#[cfg(test)]
fn sock_connack_v5(packet: &[u8]) -> u8 {
    assert_eq!(packet[0], 0x20, "a CONNACK is expected");
    let mut at = 1usize;
    while packet[at] & 0x80 != 0 {
        at += 1;
    }
    let body = &packet[at + 1..];
    assert!(body.len() >= 2, "v5 CONNACK carries flags and reason");
    body[1]
}

/// Store one enabled chain entry for the socket tests.
#[cfg(test)]
fn sock_chain_entry(id: &str, mechanism: &str, backend: &str) -> AuthnEntry {
    AuthnEntry {
        id: id.to_string(),
        mechanism: mechanism.to_string(),
        backend: backend.to_string(),
        enable: true,
        config: serde_json::Value::Null,
    }
}

/// MV-09a: directory CONNECTs through real TCP sockets for both
/// protocol versions. The stored chain order drives the decision
/// through the broker; the directory stays the authority. A valid
/// bind connects, a wrong password is refused.
#[tokio::test]
async fn mv09a_ldap_auth_over_tcp_v4_and_v5() {
    const BASE_DN: &str = "dc=example,dc=com";
    const SERVICE_DN: &str = "cn=reader,dc=example,dc=com";
    const SERVICE_PW: &str = "readerpw";
    const REQUIRED_GROUP: &str = "cn=mqtt-users,ou=groups,dc=example,dc=com";

    let dir_yaml = format!(
        "directory:\n  base_dn: {BASE_DN}\nentries:\n  - dn: {BASE_DN}\n    objectClass: [top, domain]\n    dc: example\n  - dn: ou=users,{BASE_DN}\n    objectClass: [top, organizationalUnit]\n    ou: users\n  - dn: ou=groups,{BASE_DN}\n    objectClass: [top, organizationalUnit]\n    ou: groups\n  - dn: {SERVICE_DN}\n    objectClass: [top, person]\n    cn: reader\n    sn: reader\n    userPassword: {SERVICE_PW}\n  - dn: uid=alice,ou=users,{BASE_DN}\n    objectClass: [top, person, inetOrgPerson]\n    uid: alice\n    cn: Alice\n    sn: Alice\n    userPassword: alicepw\n    memberOf: {REQUIRED_GROUP}\n  - dn: {REQUIRED_GROUP}\n    objectClass: [top, groupOfNames]\n    cn: mqtt-users\n    member: uid=alice,ou=users,{BASE_DN}\n"
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
    // Stored chain drives the socket decision: the built-in entry
    // abstains (no local users) and the directory entry decides.
    shared
        .authn_chain
        .insert(sock_chain_entry(
            "password_based:built_in_database",
            "password_based",
            "built_in_database",
        ))
        .expect("chain insert");
    shared
        .authn_chain
        .insert(sock_chain_entry(
            "password_based:ldap",
            "password_based",
            "ldap",
        ))
        .expect("chain insert");
    let port = sock_listener(shared).await;

    // v4 valid directory bind connects.
    let reply = sock_connack(port, &sock_connect_v4("ldap-v4-ok", "alice", b"alicepw")).await;
    assert_eq!(
        sock_connack_v4(&reply),
        0,
        "v4 valid directory bind connects"
    );
    // v4 wrong directory password is refused.
    let reply = sock_connack(port, &sock_connect_v4("ldap-v4-bad", "alice", b"wrong")).await;
    assert_ne!(
        sock_connack_v4(&reply),
        0,
        "v4 wrong directory password is refused"
    );
    // v5 valid directory bind connects.
    let reply = sock_connack(port, &sock_connect_v5("ldap-v5-ok", "alice", b"alicepw")).await;
    assert_eq!(
        sock_connack_v5(&reply),
        0,
        "v5 valid directory bind connects"
    );
    // v5 wrong directory password is refused.
    let reply = sock_connack(port, &sock_connect_v5("ldap-v5-bad", "alice", b"wrong")).await;
    assert_ne!(
        sock_connack_v5(&reply),
        0,
        "v5 wrong directory password is refused"
    );
}

/// MV-09a: JWT CONNECTs through real TCP sockets for both protocol
/// versions. The stored chain order drives the decision through the
/// broker; the JWKS endpoint stays the authority. A valid token
/// connects, a bad token is refused.
#[tokio::test]
async fn mv09a_jwks_auth_over_tcp_v4_and_v5() {
    let signing = JwksSigningKey::generate("sock-key-a");
    let server = JwksTestServer::start(vec![signing.jwk_json()]).await;
    let shared = jwks_test_shared(server.url());
    // Stored chain drives the socket decision: the built-in entry
    // abstains (no local users) and the JWT entry decides.
    shared
        .authn_chain
        .insert(sock_chain_entry(
            "password_based:built_in_database",
            "password_based",
            "built_in_database",
        ))
        .expect("chain insert");
    shared
        .authn_chain
        .insert(sock_chain_entry("jwt:jwt", "jwt", "jwt"))
        .expect("chain insert");
    let port = sock_listener(shared).await;
    let token = signing.mint("https://issuer.example", "indra-mqtt", 3600, false);

    // v4 valid JWT connects.
    let reply = sock_connack(
        port,
        &sock_connect_v4("jwt-v4-ok", "jwt-user", token.as_bytes()),
    )
    .await;
    assert_eq!(sock_connack_v4(&reply), 0, "v4 valid JWT connects");
    // v4 bad token is refused.
    let reply = sock_connack(
        port,
        &sock_connect_v4("jwt-v4-bad", "jwt-user", b"plain-wrong-password"),
    )
    .await;
    assert_ne!(sock_connack_v4(&reply), 0, "v4 bad token is refused");
    // v5 valid JWT connects.
    let reply = sock_connack(
        port,
        &sock_connect_v5("jwt-v5-ok", "jwt-user", token.as_bytes()),
    )
    .await;
    assert_eq!(sock_connack_v5(&reply), 0, "v5 valid JWT connects");
    // v5 expired token is refused.
    let old = signing.mint("https://issuer.example", "indra-mqtt", -300, false);
    let reply = sock_connack(
        port,
        &sock_connect_v5("jwt-v5-old", "jwt-user", old.as_bytes()),
    )
    .await;
    assert_ne!(sock_connack_v5(&reply), 0, "v5 expired token is refused");
}

/// MV-09a: database CONNECTs through real TCP sockets for both
/// protocol versions. The database is unreachable, so every
/// credentialed CONNECT fails closed through the broker with a
/// refusal and no session.
#[tokio::test]
async fn mv09a_db_auth_over_tcp_v4_and_v5() {
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
    // Stored chain drives the socket decision: the built-in entry
    // abstains (no local users) and the database entry denies.
    shared
        .authn_chain
        .insert(sock_chain_entry(
            "password_based:built_in_database",
            "password_based",
            "built_in_database",
        ))
        .expect("chain insert");
    shared
        .authn_chain
        .insert(sock_chain_entry(
            "password_based:postgresql",
            "password_based",
            "postgresql",
        ))
        .expect("chain insert");
    let port = sock_listener(shared).await;

    // v4 CONNECT against the unreachable database is refused.
    let reply = sock_connack(
        port,
        &sock_connect_v4("db-v4-1", "qualuser", b"qual-pass-1"),
    )
    .await;
    assert_ne!(
        sock_connack_v4(&reply),
        0,
        "v4 database outage fails closed"
    );
    // v5 CONNECT against the unreachable database is refused.
    let reply = sock_connack(
        port,
        &sock_connect_v5("db-v5-1", "qualuser", b"qual-pass-1"),
    )
    .await;
    assert_ne!(
        sock_connack_v5(&reply),
        0,
        "v5 database outage fails closed"
    );
}

/// MV-09a: webhook CONNECTs through real TCP sockets for both
/// protocol versions. The stored chain order drives the decision
/// through the broker; the verdict endpoint stays the authority. An
/// allow verdict connects, a deny verdict is refused.
#[tokio::test]
async fn mv09a_webhook_auth_over_tcp_v4_and_v5() {
    let server = Arc::new(WebhookTestServer::new("alice", b"webhook-pass"));
    let (handle, url) = start_webhook_test_server(server.clone()).await;
    let mut shared = test_shared();
    shared.webhook = Some(Arc::new(WebhookAuth::new(webhook_test_config(url))));
    // Stored chain drives the socket decision: the built-in entry
    // abstains (no local users) and the webhook entry decides.
    shared
        .authn_chain
        .insert(sock_chain_entry(
            "password_based:built_in_database",
            "password_based",
            "built_in_database",
        ))
        .expect("chain insert");
    shared
        .authn_chain
        .insert(sock_chain_entry(
            "password_based:http",
            "password_based",
            "http",
        ))
        .expect("chain insert");
    let port = sock_listener(shared).await;

    // v4 allow verdict connects.
    let reply = sock_connack(
        port,
        &sock_connect_v4("hook-v4-ok", "alice", b"webhook-pass"),
    )
    .await;
    assert_eq!(sock_connack_v4(&reply), 0, "v4 webhook allow connects");
    // v4 deny verdict is refused.
    let reply = sock_connack(port, &sock_connect_v4("hook-v4-bad", "alice", b"wrong")).await;
    assert_ne!(sock_connack_v4(&reply), 0, "v4 webhook deny is refused");
    // v5 allow verdict connects.
    let reply = sock_connack(
        port,
        &sock_connect_v5("hook-v5-ok", "alice", b"webhook-pass"),
    )
    .await;
    assert_eq!(sock_connack_v5(&reply), 0, "v5 webhook allow connects");
    // v5 deny verdict is refused.
    let reply = sock_connack(port, &sock_connect_v5("hook-v5-bad", "alice", b"wrong")).await;
    assert_ne!(sock_connack_v5(&reply), 0, "v5 webhook deny is refused");
    // Every CONNECT asked the endpoint: the verdicts are live.
    assert!(
        server.hits.load(Ordering::Relaxed) >= 4,
        "each socket CONNECT consults the endpoint"
    );
    handle.abort();
}

/// MV-09a: Kerberos CONNECTs through real TCP sockets for both
/// protocol versions. The stored chain order drives the decision
/// through the broker; the keytab stays the authority. A valid
/// ticket connects, a bad token is refused.
#[tokio::test]
async fn mv09a_kerberos_auth_over_tcp_v4_and_v5() {
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

    fn krb_random_key() -> [u8; 32] {
        use ring::rand::{SecureRandom, SystemRandom};
        let rng = SystemRandom::new();
        let mut key = [0u8; 32];
        rng.fill(&mut key).expect("system randomness");
        key
    }

    fn krb_now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
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
    // Stored chain drives the socket decision: the built-in entry
    // abstains (no local users) and the Kerberos entry decides.
    shared
        .authn_chain
        .insert(sock_chain_entry(
            "password_based:built_in_database",
            "password_based",
            "built_in_database",
        ))
        .expect("chain insert");
    shared
        .authn_chain
        .insert(sock_chain_entry("gssapi:kerberos", "gssapi", "kerberos"))
        .expect("chain insert");
    let port = sock_listener(shared).await;

    // v4 valid ticket connects. Each leg mints a fresh token: the
    // replay cache refuses a reused token.
    let now = krb_now();
    let token_v4 = mint("alice@EXAMPLE.COM", now - 60, now + 3600, now, 41);
    let reply = sock_connack(port, &sock_connect_v4("krb-v4-ok", "alice", &token_v4)).await;
    assert_eq!(sock_connack_v4(&reply), 0, "v4 valid ticket connects");
    // v5 valid ticket connects.
    let token_v5 = mint("alice@EXAMPLE.COM", now - 60, now + 3600, now, 42);
    let reply = sock_connack(port, &sock_connect_v5("krb-v5-ok", "alice", &token_v5)).await;
    assert_eq!(sock_connack_v5(&reply), 0, "v5 valid ticket connects");
    // v4 bad token is refused.
    let bad_spnego = krb_spnego(&[0xde, 0xad]);
    let reply = sock_connack(port, &sock_connect_v4("krb-v4-bad", "alice", &bad_spnego)).await;
    assert_ne!(sock_connack_v4(&reply), 0, "v4 bad token is refused");
    // v5 bad token is refused.
    let reply = sock_connack(port, &sock_connect_v5("krb-v5-bad", "alice", &bad_spnego)).await;
    assert_ne!(sock_connack_v5(&reply), 0, "v5 bad token is refused");
}

/// MV-09a: two authenticators decide in stored order through real
/// TCP sockets for both protocol versions. A deny stops the chain:
/// with the built-in entry first the webhook password is denied and
/// the endpoint sees no request; with the webhook entry first the
/// same credential connects. Per-entry counters show the decider.
#[tokio::test]
async fn mv09a_chain_order_decides_over_tcp_v4_and_v5() {
    let server = Arc::new(WebhookTestServer::new("alice", b"webhook-pass"));
    let (handle, url) = start_webhook_test_server(server.clone()).await;
    let mut shared = test_shared();
    shared
        .auth
        .add_user("alice", b"local-pass")
        .expect("seed local user");
    // The verdict cache stays off: every CONNECT must ask the
    // endpoint, so the hit count proves the chain position.
    let mut hook_config = webhook_test_config(url);
    hook_config.cache_ttl_secs = 0;
    shared.webhook = Some(Arc::new(WebhookAuth::new(hook_config)));
    shared
        .authn_chain
        .insert(sock_chain_entry(
            "password_based:built_in_database",
            "password_based",
            "built_in_database",
        ))
        .expect("chain insert");
    shared
        .authn_chain
        .insert(sock_chain_entry(
            "password_based:http",
            "password_based",
            "http",
        ))
        .expect("chain insert");
    let port = sock_listener(shared.clone()).await;

    // Built-in first: the known-user wrong password denies at once.
    let reply = sock_connack(
        port,
        &sock_connect_v4("order-v4-deny", "alice", b"webhook-pass"),
    )
    .await;
    assert_ne!(
        sock_connack_v4(&reply),
        0,
        "built-in first denies the webhook password"
    );
    let reply = sock_connack(
        port,
        &sock_connect_v5("order-v5-deny", "alice", b"webhook-pass"),
    )
    .await;
    assert_ne!(
        sock_connack_v5(&reply),
        0,
        "built-in first denies the webhook password on v5"
    );
    assert_eq!(
        server.hits.load(Ordering::Relaxed),
        0,
        "a local deny never asks the next authenticator"
    );
    // The local password still connects through the first backend.
    let reply = sock_connack(
        port,
        &sock_connect_v4("order-v4-local", "alice", b"local-pass"),
    )
    .await;
    assert_eq!(
        sock_connack_v4(&reply),
        0,
        "local password connects with built-in first"
    );
    let built_in = shared
        .authn_chain
        .counter_snapshot("password_based:built_in_database")
        .expect("built-in counters exist");
    assert_eq!(built_in.failed, 2, "two denies hit the first entry");
    assert_eq!(built_in.success, 1, "one allow hits the first entry");
    let http = shared
        .authn_chain
        .counter_snapshot("password_based:http")
        .expect("webhook counters exist");
    assert_eq!((http.success, http.failed), (0, 0));

    // Webhook first: the same credential now connects.
    shared
        .authn_chain
        .reorder(vec![
            "password_based:http".to_string(),
            "password_based:built_in_database".to_string(),
        ])
        .expect("reorder applies");
    let reply = sock_connack(
        port,
        &sock_connect_v4("order-v4-allow", "alice", b"webhook-pass"),
    )
    .await;
    assert_eq!(
        sock_connack_v4(&reply),
        0,
        "webhook first allows the webhook password"
    );
    let reply = sock_connack(
        port,
        &sock_connect_v5("order-v5-allow", "alice", b"webhook-pass"),
    )
    .await;
    assert_eq!(
        sock_connack_v5(&reply),
        0,
        "webhook first allows the webhook password on v5"
    );
    // The local password now meets the webhook first and is denied.
    let reply = sock_connack(
        port,
        &sock_connect_v4("order-v4-stop", "alice", b"local-pass"),
    )
    .await;
    assert_ne!(
        sock_connack_v4(&reply),
        0,
        "webhook first denies the local password"
    );
    assert_eq!(
        server.hits.load(Ordering::Relaxed),
        3,
        "three CONNECTs reached the endpoint after the reorder"
    );
    let http = shared
        .authn_chain
        .counter_snapshot("password_based:http")
        .expect("webhook counters exist");
    assert_eq!(http.success, 2, "two allows hit the webhook entry");
    assert_eq!(http.failed, 1, "one deny hits the webhook entry");
    let built_in = shared
        .authn_chain
        .counter_snapshot("password_based:built_in_database")
        .expect("built-in counters exist");
    assert_eq!((built_in.success, built_in.failed), (1, 2));
    handle.abort();
}

/// MV-09a: the enable flag decides through a real TCP socket.
/// A disabled entry denies on both protocol versions.
#[tokio::test]
async fn mv09a_chain_enable_toggles_socket_decision() {
    let shared = test_shared();
    shared
        .auth
        .add_user("alice", b"local-pass")
        .expect("seed local user");
    shared
        .authn_chain
        .insert(sock_chain_entry(
            "password_based:built_in_database",
            "password_based",
            "built_in_database",
        ))
        .expect("chain insert");
    let port = sock_listener(shared.clone()).await;
    let reply = sock_connack(
        port,
        &sock_connect_v4("enable-v4-allow", "alice", b"local-pass"),
    )
    .await;
    assert_eq!(sock_connack_v4(&reply), 0, "enabled entry allows on v4");
    let reply = sock_connack(
        port,
        &sock_connect_v5("enable-v5-allow", "alice", b"local-pass"),
    )
    .await;
    assert_eq!(sock_connack_v5(&reply), 0, "enabled entry allows on v5");
    let mut disabled = sock_chain_entry(
        "password_based:built_in_database",
        "password_based",
        "built_in_database",
    );
    disabled.enable = false;
    shared
        .authn_chain
        .update("password_based:built_in_database", disabled)
        .expect("disable applies");
    let reply = sock_connack(
        port,
        &sock_connect_v4("enable-v4-deny", "alice", b"local-pass"),
    )
    .await;
    assert_ne!(sock_connack_v4(&reply), 0, "disabled entry denies on v4");
    let reply = sock_connack(
        port,
        &sock_connect_v5("enable-v5-deny", "alice", b"local-pass"),
    )
    .await;
    assert_ne!(sock_connack_v5(&reply), 0, "disabled entry denies on v5");
}

/// MV-09a: the node cache fills through a socket CONNECT.
/// A password change evicts it and the next CONNECT sees it.
#[tokio::test]
async fn mv09a_node_cache_evicts_on_user_change_over_tcp() {
    let shared = test_shared();
    shared
        .auth
        .add_user("alice", b"old-pass")
        .expect("seed local user");
    shared
        .authn_chain
        .insert(sock_chain_entry(
            "password_based:built_in_database",
            "password_based",
            "built_in_database",
        ))
        .expect("chain insert");
    let port = sock_listener(shared.clone()).await;
    assert_eq!(shared.authn_node_cache.len(), 0);
    let reply = sock_connack(port, &sock_connect_v4("cache-v4-one", "alice", b"old-pass")).await;
    assert_eq!(sock_connack_v4(&reply), 0, "old password connects");
    assert_eq!(
        shared.authn_node_cache.len(),
        1,
        "socket CONNECT populates the cache"
    );
    // Change the password through the production HTTP entry.
    // The handler evicts the cached user on the next CONNECT.
    let mut api_state = broker_api::ApiState::standalone(shared.engine.clone());
    api_state.auth = shared.auth.clone();
    api_state.authn_chain = shared.authn_chain.clone();
    api_state.authn_node_cache = shared.authn_node_cache.clone();
    api_state.sessions = shared.sessions.clone();
    api_state.config = shared.config.clone();
    api_state
        .admin_users
        .create(
            "op-admin",
            "Adm1n-test-pass!",
            broker_api::admin_users::AdminRole::Administrator,
            "",
        )
        .expect("admin creates");
    let token = api_state.tokens.issue(
        "op-admin",
        broker_api::admin_users::AdminRole::Administrator,
    );
    let app = broker_api::router(api_state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind management server");
    let http_port = listener.local_addr().expect("local addr").port();
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve management app");
    });
    let body = serde_json::json!({"password": "new-pass"});
    let raw = serde_json::to_string(&body).expect("body serialises");
    let head = format!(
        "PUT /api/v5/authentication/password_based:built_in_database/users/alice HTTP/1.0\r\nHost: test\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{raw}",
        raw.len()
    );
    let status = {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", http_port))
            .await
            .expect("dial management server");
        stream
            .write_all(head.as_bytes())
            .await
            .expect("write request");
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.expect("read response");
        let text = String::from_utf8(buf).expect("response is UTF-8");
        let code: u16 = text.lines().next().expect("status line")[9..12]
            .parse()
            .expect("status code");
        code
    };
    task.abort();
    assert_eq!(status, 200, "password change through HTTP applies");
    assert_eq!(shared.authn_node_cache.len(), 0, "user change evicts cache");
    let reply = sock_connack(port, &sock_connect_v4("cache-v4-old", "alice", b"old-pass")).await;
    assert_ne!(
        sock_connack_v4(&reply),
        0,
        "old password fails after change"
    );
    let reply = sock_connack(port, &sock_connect_v4("cache-v4-new", "alice", b"new-pass")).await;
    assert_eq!(sock_connack_v4(&reply), 0, "new password connects");
}

/// MV-09a: two tenants stay isolated through a real TCP socket.
/// No tenant falls back to another tenant user.
#[tokio::test]
async fn mv09a_two_tenant_chain_isolation_over_tcp() {
    use broker_session::tenant::TenantRule;
    let shared = test_shared();
    shared
        .auth
        .add_user_in_tenant("t-a", "alice", b"pass-a")
        .expect("seed tenant t-a");
    shared
        .auth
        .add_user_in_tenant("t-b", "alice", b"pass-b")
        .expect("seed tenant t-b");
    shared
        .authn_chain
        .insert(sock_chain_entry(
            "password_based:built_in_database",
            "password_based",
            "built_in_database",
        ))
        .expect("chain insert");
    shared
        .tenant_registry
        .replace(vec![TenantRule::new("", "tenant", "t-${client_id}")])
        .expect("tenant rules store");
    let port = sock_listener(shared.clone()).await;
    let reply = sock_connack(port, &sock_connect_v4("a", "alice", b"pass-a")).await;
    assert_eq!(sock_connack_v4(&reply), 0, "tenant t-a allows its password");
    let reply = sock_connack(port, &sock_connect_v4("a", "alice", b"pass-b")).await;
    assert_ne!(
        sock_connack_v4(&reply),
        0,
        "tenant t-a denies the other tenant password"
    );
    let reply = sock_connack(port, &sock_connect_v5("b", "alice", b"pass-b")).await;
    assert_eq!(sock_connack_v5(&reply), 0, "tenant t-b allows its password");
    let reply = sock_connack(port, &sock_connect_v5("b", "alice", b"pass-a")).await;
    assert_ne!(
        sock_connack_v5(&reply),
        0,
        "tenant t-b denies the other tenant password"
    );
    let reply = sock_connack(port, &sock_connect_v4("c", "alice", b"pass-a")).await;
    assert_ne!(sock_connack_v4(&reply), 0, "unknown tenant fails closed");
}

#[tokio::test]
async fn bind_auth_failure_over_tcp_returns_0x86() {
    let shared = Shared::new();
    shared
        .auth
        .add_user("alice", b"s3cret")
        .expect("memory-only persist cannot fail");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept");
        handle_connection(stream, shared).await.expect("handle");
    });

    // Wrong password over a real transport: 0x86, no session.
    let io = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let client = FramedTransport::new(io);
    client
        .send(bind_frame(
            84,
            1,
            encode_bind_meta_creds("dev-bad", true, 60, "alice", b"nope"),
        ))
        .await
        .expect("bind");
    let reply = client.recv().await.expect("recv binding");
    assert_eq!(reply.header.opcode, OpCode::SessionBinding);
    let (_, present, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0x86);
    assert!(!present);

    server.abort();
}
