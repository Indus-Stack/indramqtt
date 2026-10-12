//! MQTT connections that the kernel owns.
//!
//! One task for each client socket. The task decodes MQTT packets, makes
//! the same link frames that the edge makes, and gives them to
//! [`handle_inbound_frame_with`]. Frames that the kernel routes to the
//! connection are encoded as MQTT packets and written to the socket.
//! There is no second process and no link socket between the client and
//! the kernel logic.
//!
//! The connection uses the same session, routing and delivery code as a
//! connection of the edge. Only the transport is different.

use super::{
    decode_session_binding_full, detach, handle_inbound_frame_with, merge_egress_batch,
    BindRequest, BindWill, Shared,
};
use async_trait::async_trait;
use broker_protocol::wire::{self, kind, ConnackProperties, PublishOut, WireError};
use broker_session::SessionKey;
use brokerlink::{BrokerFrame, BrokerLinkError, BrokerLinkTransport, OpCode};
use bytes::{Bytes, BytesMut};
use futures::{Sink, Stream};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpListener;
use tokio::sync::mpsc::unbounded_channel;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tracing::{debug, error, warn};

/// Connection identifiers of kernel-owned connections start here. The
/// edge counts from 1, and the API WebSocket connections start at
/// `1 << 62`, thus the three ranges do not overlap.
const NATIVE_CONN_ID_BASE: u64 = 1 << 61;
static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(NATIVE_CONN_ID_BASE);

/// A client must send CONNECT in this time (the same value as the edge).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Largest number of frames in one write to the socket.
const EGRESS_BATCH_FRAMES: usize = 64;
/// Largest number of frame bytes in one write to the socket.
const EGRESS_BATCH_BYTES: usize = 64 * 1024;
/// Largest CONNECT packet. A client that is not authenticated yet must
/// not make the broker keep more than this.
const MAX_CONNECT_PACKET_BYTES: usize = 64 * 1024;
/// Fallback packet cap when the session manager reports none.
/// Reason: keeps one connection bounded near 1 MiB even if the
/// manager value is missing; the configured `session.max_packet_size`
/// sets the real limit at boot.
const FALLBACK_MAX_PACKET_BYTES: usize = 1024 * 1024;
/// A write to a client that does not read must end in this time. The
/// connection then closes, thus a stalled client cannot keep its
/// queues and its session for an unlimited time.
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
/// Initial size of the read buffer of one connection. An idle
/// connection keeps only this much. The buffer grows for a larger packet.
const READ_BUFFER_BYTES: usize = 256;

/// One accepted connection in a per-listener count. The drop takes it
/// out again, also when the task ends on a panic.
struct PerListenerSlot {
    count: Arc<AtomicU64>,
}

impl PerListenerSlot {
    /// Takes one slot of `count` below `limit`. `None` sets no limit:
    /// per-connection memory stays bounded by the read buffer and the
    /// egress backlog, so no counter bounds the listener.
    fn take(count: &Arc<AtomicU64>, limit: Option<u32>) -> Option<Self> {
        let Some(limit) = limit else {
            count.fetch_add(1, Ordering::Relaxed);
            return Some(Self {
                count: count.clone(),
            });
        };
        let limit = u64::from(limit);
        let mut current = count.load(Ordering::Relaxed);
        loop {
            if current >= limit {
                return None;
            }
            match count.compare_exchange_weak(
                current,
                current + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(Self {
                        count: count.clone(),
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }
}

impl Drop for PerListenerSlot {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::Relaxed);
    }
}

// Reason: the reference API documents 1024 as the default accept
// backlog, and `listeners.tcp.backlog` defaults to the same value.
// Rows without a backlog (edge rows, API creates) bind with it.
const DEFAULT_BIND_BACKLOG: u32 = 1024;

/// Binds a listener socket with the accept backlog of the configuration.
pub(super) fn bind_listener(bind: &str, backlog: u32) -> std::io::Result<TcpListener> {
    let addr: std::net::SocketAddr = bind
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let socket = if addr.is_ipv4() {
        tokio::net::TcpSocket::new_v4()?
    } else {
        tokio::net::TcpSocket::new_v6()?
    };
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    socket.listen(backlog)
}

/// Socket control of the kernel-owned listeners (AP-02).
///
/// The API listener store calls this control when an operator starts,
/// stops or restarts a kernel row. One accept task per open listener;
/// stopping aborts the task and drops the socket. Management-plane
/// only; publish and deliver never touch these handles.
pub(super) struct NativeListenerController {
    shared: Shared,
    /// Boot settings of the built-in listeners. They carry the keys
    /// that a listener row does not hold: the client certificate policy,
    /// the PSK switch and the WebSocket paths.
    listeners: broker_config::schema::ListenersConf,
    /// Accept tasks by `{type}:{name}` id.
    handles: std::sync::Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
    /// Live connection counts by `{type}:{name}` id.
    counts: std::sync::Mutex<HashMap<String, Arc<AtomicU64>>>,
}

impl NativeListenerController {
    /// Control that serves new sockets with a clone of `shared` and the
    /// boot settings `listeners`.
    pub(super) fn new(shared: Shared, listeners: broker_config::schema::ListenersConf) -> Self {
        Self {
            shared,
            listeners,
            handles: std::sync::Mutex::new(HashMap::new()),
            counts: std::sync::Mutex::new(HashMap::new()),
        }
    }
}

impl broker_api::v5::listeners::ListenerController for NativeListenerController {
    fn start_listener(
        &self,
        record: &broker_api::v5::listeners::ListenerRecord,
    ) -> Result<(), String> {
        let mut handles = self
            .handles
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if handles.contains_key(&record.id) {
            return Ok(());
        }
        let backlog = if record.backlog == 0 {
            DEFAULT_BIND_BACKLOG
        } else {
            record.backlog
        };
        let max = match record.max_connections {
            Some(broker_api::v5::listeners::ListenerMaxConnections::Finite(count)) => Some(count),
            Some(broker_api::v5::listeners::ListenerMaxConnections::Infinite) | None => None,
        };
        let conf = &self.listeners;
        // Build the serving future first. A bad certificate fails the
        // start before the socket opens and before a count is stored.
        let kind = record.kind.as_str();
        let tls = match kind {
            "ssl" if !conf.tls.psk_enabled => Some(
                load_tls_config(
                    &record.cert_file,
                    &record.key_file,
                    &TlsClientAuth {
                        verify_peer: conf.tls.verify == "verify_peer",
                        legacy_verify: conf.tls.verify_client_cert,
                        cacertfile: conf.tls.cacertfile.clone(),
                        fail_if_no_peer_cert: conf.tls.fail_if_no_peer_cert,
                    },
                )
                .map_err(|e| e.to_string())?,
            ),
            "wss" => Some(
                load_tls_config(
                    &conf.wss.cert_file,
                    &conf.wss.key_file,
                    &TlsClientAuth {
                        verify_peer: conf.wss.verify == "verify_peer",
                        legacy_verify: conf.wss.verify_client_cert,
                        cacertfile: conf.wss.cacertfile.clone(),
                        fail_if_no_peer_cert: conf.wss.fail_if_no_peer_cert,
                    },
                )
                .map_err(|e| e.to_string())?,
            ),
            "tcp" | "ssl" | "ws" => None,
            other => return Err(format!("the kernel does not serve {other} listeners")),
        };
        let listener =
            bind_listener(&record.bind, backlog).map_err(|e| format!("{}: {e}", record.bind))?;
        let count = Arc::new(AtomicU64::new(0));
        let shared = self.shared.clone();
        let task = match (kind, tls) {
            ("ssl", Some(tls)) => tokio::spawn(serve_native_mqtts(
                listener,
                tls,
                max,
                count.clone(),
                shared,
            )),
            // PSK serves the identity store with PSK suites only. An
            // unknown identity and a wrong key both fail the handshake.
            ("ssl", None) => tokio::spawn(serve_native_mqtt_psk(
                listener,
                shared.psk.clone(),
                max,
                count.clone(),
                shared,
            )),
            ("wss", Some(tls)) => tokio::spawn(serve_native_wss(
                listener,
                tls,
                conf.wss.path.clone(),
                max,
                count.clone(),
                shared,
            )),
            ("ws", _) => tokio::spawn(serve_native_ws(
                listener,
                conf.ws.path.clone(),
                max,
                count.clone(),
                shared,
            )),
            _ => tokio::spawn(serve_native_mqtt(listener, max, count.clone(), shared)),
        };
        self.counts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(record.id.clone(), count);
        handles.insert(record.id.clone(), task);
        Ok(())
    }

    fn stop_listener(&self, id: &str) {
        if let Some(handle) = self
            .handles
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(id)
        {
            handle.abort();
        }
        self.counts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(id);
    }

    fn connection_count(&self, id: &str) -> u64 {
        self.counts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(id)
            .map(|count| count.load(Ordering::Relaxed))
            .unwrap_or(0)
    }
}

/// Accepts MQTT clients and runs one task for each of them. A client
/// above `max_connections` is closed at once. The listener name is
/// `tcp` for tenant rules and certificate checks.
pub(super) async fn serve_native_mqtt(
    listener: TcpListener,
    max_connections: Option<u32>,
    count: Arc<AtomicU64>,
    shared: Shared,
) {
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                warn!("Kernel MQTT accept error: {}", e);
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let Some(slot) = PerListenerSlot::take(&count, max_connections) else {
            debug!("Kernel MQTT listener is at its connection limit");
            continue;
        };
        let _ = stream.set_nodelay(true);
        let shared = shared.clone();
        let conn_id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
        let accepted_at = Instant::now();
        tokio::spawn(async move {
            let _slot = slot;
            if let Err(e) = run_connection(
                stream,
                peer.ip().to_string(),
                conn_id,
                shared,
                "tcp",
                PeerCert::default(),
                accepted_at,
            )
            .await
            {
                debug!("Kernel MQTT connection {} ended: {}", conn_id, e);
            }
        });
    }
}

/// A TLS handshake must end in this time. A client that opens a socket
/// and sends nothing does not keep a task for a longer time.
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Client certificate policy of one secure listener.
/// The fields mirror the listener settings with the same names.
/// `verify_peer` mirrors `verify = "verify_peer"`.
/// `legacy_verify` mirrors `verify_client_cert`.
/// `cacertfile` mirrors `cacertfile`.
/// `fail_if_no_peer_cert` mirrors `fail_if_no_peer_cert`.
#[derive(Clone, Debug, Default)]
pub(super) struct TlsClientAuth {
    /// True when the mode setting requests a chain check.
    pub verify_peer: bool,
    /// True when the legacy switch requests a chain check.
    pub legacy_verify: bool,
    /// CA file for the chain check. Required when a check is on.
    pub cacertfile: String,
    /// True when a missing certificate fails the handshake.
    pub fail_if_no_peer_cert: bool,
}

impl TlsClientAuth {
    /// True when the listener checks any client certificate chain.
    fn verify_on(&self) -> bool {
        self.verify_peer || self.legacy_verify
    }

    /// True when a missing certificate fails the handshake.
    /// The bind check repeats the refusal for defense in depth.
    fn require_cert(&self) -> bool {
        self.fail_if_no_peer_cert && self.verify_on()
    }
}

/// Loads the trust roots for client certificate checks.
/// The path must name a PEM file with at least one certificate.
/// An empty path, a missing file, an unreadable file and an empty
/// file return an error. No default set of public authorities
/// applies. The caller fails the listener start on an error.
fn load_client_roots(
    cacertfile: &str,
) -> Result<rustls::RootCertStore, Box<dyn std::error::Error + Send + Sync>> {
    if cacertfile.trim().is_empty() {
        return Err("client certificate verification needs a CA file (cacertfile)".into());
    }
    let file = std::fs::File::open(cacertfile)?;
    let mut reader = std::io::BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)?;
    if certs.is_empty() {
        return Err(format!("no CA certificate in {cacertfile}").into());
    }
    let mut roots = rustls::RootCertStore::empty();
    for der in certs {
        roots.add(rustls::pki_types::CertificateDer::from(der))?;
    }
    Ok(roots)
}

/// Reads the certificate chain and the private key of the TLS listener
/// and makes the server configuration. The protocol versions are TLS 1.2
/// and TLS 1.3 with the default cipher suites of the library.
/// Without verification the server requests no client certificate.
/// With optional verification the server requests a client certificate
/// and checks a sent chain against the CA file. A missing certificate
/// passes the handshake and fails at the bind when required there.
/// With required verification a missing certificate fails the handshake.
/// A bad chain (wrong CA, expired, not yet valid) fails the handshake.
/// Off by default keeps handshakes unchanged.
pub(super) fn load_tls_config(
    cert_file: &str,
    key_file: &str,
    client_auth: &TlsClientAuth,
) -> Result<Arc<rustls::ServerConfig>, Box<dyn std::error::Error + Send + Sync>> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};

    let mut cert_reader = std::io::BufReader::new(std::fs::File::open(cert_file)?);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)?
        .into_iter()
        .map(CertificateDer::from)
        .collect();
    if certs.is_empty() {
        return Err(format!("no certificate in {cert_file}").into());
    }
    let key_bytes = std::fs::read(key_file)?;
    let mut key: Option<PrivateKeyDer<'static>> = None;
    for item in rustls_pemfile::read_all(&mut key_bytes.as_slice())? {
        key = match item {
            rustls_pemfile::Item::PKCS8Key(der) => Some(PrivateKeyDer::Pkcs8(der.into())),
            rustls_pemfile::Item::RSAKey(der) => Some(PrivateKeyDer::Pkcs1(der.into())),
            rustls_pemfile::Item::ECKey(der) => Some(PrivateKeyDer::Sec1(der.into())),
            _ => continue,
        };
        break;
    }
    let key = key.ok_or_else(|| format!("no private key in {key_file}"))?;
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?;
    if !client_auth.verify_on() {
        let config = builder.with_no_client_auth().with_single_cert(certs, key)?;
        return Ok(Arc::new(config));
    }
    let roots = load_client_roots(&client_auth.cacertfile)?;
    let verifier_builder = rustls::server::WebPkiClientVerifier::builder(roots.into());
    let verifier = if client_auth.require_cert() {
        verifier_builder.build()?
    } else {
        verifier_builder.allow_unauthenticated().build()?
    };
    let config = builder
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)?;
    Ok(Arc::new(config))
}

/// Largest certificate field kept for tenant rules.
/// Reason: matches the bind section bound, so one peer cannot grow memory.
const MAX_NATIVE_CERT_FIELD_LEN: usize = 1024;
/// Most SANs kept for tenant rules.
/// Reason: matches the bind section bound.
const MAX_NATIVE_CERT_SANS: usize = 16;

/// Client certificate fields pulled from the TLS handshake.
/// The kernel native path fills them. Plain TCP keeps them empty.
#[derive(Clone, Debug, Default)]
pub(super) struct PeerCert {
    /// Common name of the leaf, if present.
    pub cn: Option<String>,
    /// Subject name of the leaf, if present.
    pub subject: Option<String>,
    /// DNS and IP subject-alt names of the leaf.
    pub sans: Vec<String>,
    /// Verified PSK identity of the connection, if the handshake
    /// used PSK. The bind carries it as the username.
    pub psk_identity: Option<String>,
}

/// Pulls CN, subject and SANs from the leaf peer certificate.
/// Runs once at handshake, never on publish or deliver.
/// A missing or bad certificate yields empty fields.
/// The bind check then denies when verification is on.
fn peer_cert_fields(certs: Option<&[rustls::pki_types::CertificateDer<'_>]>) -> PeerCert {
    use x509_parser::prelude::*;
    let Some(certs) = certs else {
        return PeerCert::default();
    };
    let Some(leaf) = certs.first() else {
        return PeerCert::default();
    };
    let Ok((_, cert)) = parse_x509_certificate(leaf.as_ref()) else {
        return PeerCert::default();
    };
    let cn = cert
        .subject()
        .iter_common_name()
        .next()
        .and_then(|cn| cn.attr_value().as_str().ok().map(str::to_string))
        .filter(|text| !text.is_empty())
        .map(|mut text| {
            text.truncate(MAX_NATIVE_CERT_FIELD_LEN);
            text
        });
    let mut subject = cert.subject().to_string();
    if subject.is_empty() {
        return PeerCert {
            cn,
            subject: None,
            sans: Vec::new(),
            psk_identity: None,
        };
    }
    subject.truncate(MAX_NATIVE_CERT_FIELD_LEN);
    let mut sans: Vec<String> = Vec::new();
    for ext in cert.extensions().iter() {
        if let ParsedExtension::SubjectAlternativeName(names) = ext.parsed_extension() {
            for name in &names.general_names {
                let text = match name {
                    GeneralName::DNSName(value) => Some(value.to_string()),
                    GeneralName::IPAddress(bytes) => {
                        if bytes.len() == 4 {
                            Some(format!(
                                "{}.{}.{}.{}",
                                bytes[0], bytes[1], bytes[2], bytes[3]
                            ))
                        } else {
                            None
                        }
                    }
                    _ => None,
                };
                if let Some(mut text) = text {
                    if !text.is_empty() && sans.len() < MAX_NATIVE_CERT_SANS {
                        text.truncate(MAX_NATIVE_CERT_FIELD_LEN);
                        sans.push(text);
                    }
                }
            }
        }
    }
    PeerCert {
        cn,
        subject: Some(subject),
        sans,
        psk_identity: None,
    }
}

/// Accepts MQTT clients over TLS. The handshake runs in the task of the
/// connection, thus a slow handshake does not delay the accept of other
/// clients. A client above `max_connections` is closed at once. The
/// listener name is `tls` for tenant rules and certificate checks.
/// The handshake pulls the peer certificate fields for the bind.
/// A bad chain fails the handshake. A missing certificate reaches
/// the bind check, which denies it only when verification is on.
pub(super) async fn serve_native_mqtts(
    listener: TcpListener,
    config: Arc<rustls::ServerConfig>,
    max_connections: Option<u32>,
    count: Arc<AtomicU64>,
    shared: Shared,
) {
    let acceptor = tokio_rustls::TlsAcceptor::from(config);
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                warn!("Kernel MQTT TLS accept error: {}", e);
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let Some(slot) = PerListenerSlot::take(&count, max_connections) else {
            debug!("Kernel MQTT TLS listener is at its connection limit");
            continue;
        };
        let _ = stream.set_nodelay(true);
        let shared = shared.clone();
        let acceptor = acceptor.clone();
        let conn_id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
        let accepted_at = Instant::now();
        tokio::spawn(async move {
            let _slot = slot;
            let tls =
                match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                    Ok(Ok(tls)) => tls,
                    Ok(Err(e)) => {
                        debug!("Kernel MQTT TLS handshake with {} failed: {}", peer, e);
                        return;
                    }
                    Err(_) => {
                        debug!("Kernel MQTT TLS handshake with {} timed out", peer);
                        return;
                    }
                };
            let peer_cert = peer_cert_fields(tls.get_ref().1.peer_certificates());
            if let Err(e) = run_connection(
                tls,
                peer.ip().to_string(),
                conn_id,
                shared,
                "tls",
                peer_cert,
                accepted_at,
            )
            .await
            {
                debug!("Kernel MQTT TLS connection {} ended: {}", conn_id, e);
            }
        });
    }
}

/// PSK cipher suites for TLS 1.2.
/// Reason: AEAD only, so no CBC padding-oracle class.
/// Matches the cipher suites of the edge reference.
const PSK_CIPHER_LIST: &str = "PSK-AES128-GCM-SHA256:PSK-AES256-GCM-SHA384";
/// PSK cipher suites for TLS 1.3.
/// Reason: the two GCM suites cover 128 and 256 bit keys.
const PSK_CIPHERSUITES: &str = "TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384";

/// Length of the PSK server secret in bytes.
/// Reason: 256 bits stop guessing across restarts.
const PSK_SERVER_SECRET_LEN: usize = 32;

/// Makes one random PSK server secret. The stand-in key for an
/// unknown identity derives from it. Fails closed when the platform
/// generator fails: no listener starts without a secret.
fn psk_new_server_secret() -> Result<[u8; PSK_SERVER_SECRET_LEN], openssl::error::ErrorStack> {
    let mut secret = [0u8; PSK_SERVER_SECRET_LEN];
    openssl::rand::rand_bytes(&mut secret)?;
    Ok(secret)
}

/// Derives the stand-in key for an unknown PSK identity. HMAC with
/// the per-boot server secret keeps the value unpredictable. The
/// handshake then fails at the same check as a wrong key. A failed
/// HMAC yields zeros, which still fail the handshake.
fn psk_fake_key(secret: &[u8; PSK_SERVER_SECRET_LEN], identity: &[u8]) -> [u8; 32] {
    use openssl::hash::MessageDigest;
    let mut out = [0u8; 32];
    let Ok(pkey) = openssl::pkey::PKey::hmac(secret) else {
        return out;
    };
    let Ok(mut signer) = openssl::sign::Signer::new(MessageDigest::sha256(), &pkey) else {
        return out;
    };
    if signer.update(identity).is_err() {
        return out;
    }
    let Ok(len) = signer.len() else {
        return out;
    };
    if len != out.len() || signer.sign(&mut out).is_err() {
        out = [0u8; 32];
    }
    out
}

/// Raw OpenSSL handshake hooks for TLS 1.3 external PSK.
/// The safe wrapper exposes only the TLS 1.2 callback, so this
/// module calls `SSL_CTX_set_psk_find_session_callback` directly.
/// Both callbacks read the same store and the same server secret.
mod psk_tls13 {
    use std::collections::HashMap;
    use std::os::raw::{c_char, c_int, c_uchar, c_void};
    use std::sync::{Arc, OnceLock};

    /// TLS 1.3 version number for the session object.
    /// Reason: the protocol version comes from the TLS specification.
    const TLS1_3_VERSION: c_int = 0x0304;

    extern "C" {
        fn SSL_CTX_set_psk_find_session_callback(
            ctx: *mut c_void,
            cb: Option<
                unsafe extern "C" fn(*mut c_void, *const c_uchar, usize, *mut *mut c_void) -> c_int,
            >,
        );
        fn SSL_get_SSL_CTX(ssl: *const c_void) -> *mut c_void;
        fn SSL_SESSION_new() -> *mut c_void;
        fn SSL_SESSION_free(sess: *mut c_void);
        fn SSL_SESSION_set1_master_key(
            sess: *mut c_void,
            key: *const c_uchar,
            keylen: usize,
        ) -> c_int;
        fn SSL_SESSION_set_cipher(sess: *mut c_void, cipher: *const c_void) -> c_int;
        fn SSL_SESSION_set_protocol_version(sess: *mut c_void, version: c_int) -> c_int;
        fn SSL_get_ciphers(ssl: *const c_void) -> *mut c_void;
        fn SSL_CIPHER_get_name(cipher: *const c_void) -> *const c_char;
        fn OPENSSL_sk_num(stack: *const c_void) -> c_int;
        fn OPENSSL_sk_value(stack: *const c_void, index: c_int) -> *mut c_void;
    }

    /// Store plus server secret for one PSK context.
    struct PskContext {
        store: Arc<broker_auth::PskStore>,
        secret: [u8; super::PSK_SERVER_SECRET_LEN],
    }

    /// Context pointer to store map. One entry for each PSK listener.
    /// Holds at most [`MAX_PSK_CONTEXTS`] entries.
    fn ctx_stores() -> &'static parking_lot::Mutex<HashMap<usize, PskContext>> {
        static CELL: OnceLock<parking_lot::Mutex<HashMap<usize, PskContext>>> = OnceLock::new();
        CELL.get_or_init(|| parking_lot::Mutex::new(HashMap::new()))
    }

    /// Most TLS 1.3 PSK contexts remembered at once.
    /// Reason: the kernel runs one PSK listener, so one entry is live.
    /// The rest covers restarts and tests.
    pub(super) const MAX_PSK_CONTEXTS: usize = 16;

    /// Remembers the store and the secret for one context pointer.
    /// Refuses a new context past the bound and reports false.
    /// The caller fails the listener start on false.
    pub(super) fn register(
        ctx: *mut c_void,
        store: Arc<broker_auth::PskStore>,
        secret: [u8; super::PSK_SERVER_SECRET_LEN],
    ) -> bool {
        let mut map = ctx_stores().lock();
        if !map.contains_key(&(ctx as usize)) && map.len() >= MAX_PSK_CONTEXTS {
            return false;
        }
        map.insert(ctx as usize, PskContext { store, secret });
        true
    }

    /// Sets the TLS 1.3 find-session callback on one context.
    pub(super) fn set_callback(ctx: *mut c_void) {
        // Sound: the pointer is a live server context from the
        // builder, and the callback has the required signature.
        unsafe {
            SSL_CTX_set_psk_find_session_callback(ctx, Some(find_session_cb));
        }
    }

    /// Picks a TLS 1.3 cipher for the new session.
    /// Prefers an AES GCM suite, else takes the first suite.
    unsafe fn pick_cipher(ssl: *const c_void) -> *const c_void {
        // Sound: the handshake passes a live handshake object, so
        // the cipher list call reads valid state.
        let stack = SSL_get_ciphers(ssl);
        if stack.is_null() {
            return std::ptr::null();
        }
        // Sound: the stack pointer is valid, and the count call
        // reads only the stack header.
        let count = OPENSSL_sk_num(stack);
        let mut first: *const c_void = std::ptr::null();
        let mut index = 0;
        while index < count {
            // Sound: the index stays below the count, so the
            // element call returns a valid cipher pointer.
            let cipher = OPENSSL_sk_value(stack, index) as *const c_void;
            if !cipher.is_null() {
                if first.is_null() {
                    first = cipher;
                }
                // Sound: the cipher pointer is valid, so the name
                // call returns a valid string pointer.
                let name = SSL_CIPHER_get_name(cipher);
                if !name.is_null()
                    // Sound: the name pointer points to a valid
                    // NUL-ended string owned by the library.
                    && std::ffi::CStr::from_ptr(name)
                        .to_bytes()
                        .starts_with(b"TLS_AES_")
                {
                    return cipher;
                }
            }
            index += 1;
        }
        first
    }

    /// Key for one TLS 1.3 identity. An unknown identity gets the
    /// stand-in key, so the handshake fails at the binder check like
    /// a wrong key does. It never logs the identity.
    fn session_key(context: &PskContext, identity: &[u8]) -> Vec<u8> {
        match context.store.lookup(identity) {
            Some(key) => key,
            None => super::psk_fake_key(&context.secret, identity).to_vec(),
        }
    }

    /// TLS 1.3 session lookup. Runs during the handshake.
    /// A panic never crosses the FFI boundary: any panic becomes
    /// a refused lookup.
    unsafe extern "C" fn find_session_cb(
        ssl: *mut c_void,
        identity: *const c_uchar,
        idlen: usize,
        sess_out: *mut *mut c_void,
    ) -> c_int {
        let step = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            find_session_inner(ssl, identity, idlen, sess_out)
        }));
        step.unwrap_or(0)
    }

    /// Body of the TLS 1.3 session lookup. Runs inside the panic
    /// guard above, so no panic reaches the library.
    unsafe fn find_session_inner(
        ssl: *mut c_void,
        identity: *const c_uchar,
        idlen: usize,
        sess_out: *mut *mut c_void,
    ) -> c_int {
        if identity.is_null() || sess_out.is_null() {
            return 0;
        }
        // Sound: the handshake passes a valid identity buffer of
        // idlen bytes when the pointer is not null.
        let id = std::slice::from_raw_parts(identity, idlen);
        // Sound: the handshake passes a live handshake object, so
        // the context call reads valid state.
        let ctx = SSL_get_SSL_CTX(ssl);
        let key = ctx_stores()
            .lock()
            .get(&(ctx as usize))
            .map(|context| session_key(context, id));
        let Some(key) = key else {
            return 0;
        };
        // Sound: the constructor returns either a valid session or
        // null, and the null case returns at once.
        let sess = SSL_SESSION_new();
        if sess.is_null() {
            return 0;
        }
        // Sound: the session pointer is valid here, and the key
        // slice is live for the call.
        if SSL_SESSION_set1_master_key(sess, key.as_ptr(), key.len()) != 1 {
            // Sound: the session pointer is valid and owned here,
            // so the free call releases it exactly once.
            SSL_SESSION_free(sess);
            return 0;
        }
        let cipher = pick_cipher(ssl);
        // Sound: the session pointer is valid here.
        if cipher.is_null() || SSL_SESSION_set_cipher(sess, cipher) != 1 {
            // Sound: the session pointer is valid and owned here,
            // so the free call releases it exactly once.
            SSL_SESSION_free(sess);
            return 0;
        }
        // Sound: the session pointer is valid here.
        if SSL_SESSION_set_protocol_version(sess, TLS1_3_VERSION) != 1 {
            // Sound: the session pointer is valid and owned here,
            // so the free call releases it exactly once.
            SSL_SESSION_free(sess);
            return 0;
        }
        // Sound: the caller passes a valid output pointer, and the
        // session is fully built, so ownership moves to the library.
        *sess_out = sess;
        1
    }
}

/// Builds the OpenSSL server context for the PSK listener.
/// Uses only PSK suites and no certificate. Wires the TLS 1.2
/// server callback and the TLS 1.3 find-session callback to the
/// same store and the same per-boot secret. An unknown identity
/// gets a stand-in key, so it fails at the same check as a wrong
/// key. Releases idle buffers. Refuses tickets, so every handshake
/// runs the lookup. Fails closed on any OpenSSL error.
pub(super) fn build_psk_ssl_context(
    store: Arc<broker_auth::PskStore>,
) -> Result<openssl::ssl::SslContext, Box<dyn std::error::Error + Send + Sync>> {
    use openssl::ssl::{SslContextBuilder, SslMethod, SslVersion};
    let mut builder = SslContextBuilder::new(SslMethod::tls_server())?;
    builder.set_min_proto_version(Some(SslVersion::TLS1_2))?;
    builder.set_cipher_list(PSK_CIPHER_LIST)?;
    builder.set_ciphersuites(PSK_CIPHERSUITES)?;
    // Reason: every PSK handshake runs the full lookup, so no
    // session resumes and the cache would only spend memory. No
    // tickets either, so a client cannot resume without the lookup.
    builder.set_session_cache_mode(openssl::ssl::SslSessionCacheMode::OFF);
    builder.set_options(openssl::ssl::SslOptions::NO_TICKET);
    builder.set_mode(openssl::ssl::SslMode::RELEASE_BUFFERS);
    // One secret serves both protocol versions, so the stand-in
    // key is the same on TLS 1.2 and TLS 1.3.
    let secret = psk_new_server_secret()?;
    let lookup = store.clone();
    // An unknown identity gets the stand-in key, so the handshake
    // fails at the Finished check like a wrong key does. Both
    // failures carry the same alert. Neither path names the
    // identity. A panic never escapes into the library: any panic
    // becomes a refused handshake.
    builder.set_psk_server_callback(move |_ssl, identity, psk| {
        let step = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let id = identity.unwrap_or_default();
            match lookup.lookup(id) {
                Some(key) => key,
                None => psk_fake_key(&secret, id).to_vec(),
            }
        }));
        let key = step.map_err(|_| openssl::error::ErrorStack::get())?;
        if key.len() > psk.len() {
            return Err(openssl::error::ErrorStack::get());
        }
        psk[..key.len()].copy_from_slice(&key);
        Ok(key.len())
    });
    if !psk_tls13::register(builder.as_ptr() as *mut std::ffi::c_void, store, secret) {
        return Err(format!(
            "PSK context table holds {} entries",
            psk_tls13::MAX_PSK_CONTEXTS
        )
        .into());
    }
    psk_tls13::set_callback(builder.as_ptr() as *mut std::ffi::c_void);
    Ok(builder.build())
}

/// Accepts MQTT clients over TLS PSK. The caller enables PSK by
/// starting this listener. The certificate TLS listener stays
/// unchanged and keeps its rustls path. Each handshake runs in
/// the task of the connection with the TLS time limit. An
/// unknown identity and a wrong key both fail the handshake with
/// the same alert. The failure never names the identity. The
/// verified identity rides to the bind step as the username. The
/// listener name is `tls` for tenant rules. The same packet
/// limits, the connect timeout from accept, and `max_connections`
/// apply as on TCP.
pub(super) async fn serve_native_mqtt_psk(
    listener: TcpListener,
    psk: Arc<broker_auth::PskStore>,
    max_connections: Option<u32>,
    count: Arc<AtomicU64>,
    shared: Shared,
) {
    let context = match build_psk_ssl_context(psk) {
        Ok(context) => Arc::new(context),
        Err(e) => {
            warn!("Kernel MQTT PSK listener failed to start: {}", e);
            return;
        }
    };
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                warn!("Kernel MQTT PSK accept error: {}", e);
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let Some(slot) = PerListenerSlot::take(&count, max_connections) else {
            debug!("Kernel MQTT PSK listener is at its connection limit");
            continue;
        };
        let _ = stream.set_nodelay(true);
        let shared = shared.clone();
        let context = context.clone();
        let conn_id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
        let accepted_at = Instant::now();
        tokio::spawn(async move {
            let _slot = slot;
            let ssl = match openssl::ssl::Ssl::new(&context) {
                Ok(ssl) => ssl,
                Err(e) => {
                    debug!("Kernel MQTT PSK handshake setup failed: {}", e);
                    return;
                }
            };
            let mut tls = match tokio_openssl::SslStream::new(ssl, stream) {
                Ok(tls) => tls,
                Err(e) => {
                    debug!("Kernel MQTT PSK stream setup failed: {}", e);
                    return;
                }
            };
            if let Err(e) =
                tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, std::pin::Pin::new(&mut tls).accept())
                    .await
                    .map_err(|_| "timeout")
                    .and_then(|handshake| handshake.map_err(|_| "refused"))
            {
                debug!("Kernel MQTT PSK handshake with {} failed: {}", peer, e);
                return;
            }
            // The handshake verified the PSK identity. It rides to
            // the bind step as the username, so authentication uses
            // the verified value, never the CONNECT text.
            let peer_cert = psk_peer_cert(&tls);
            if let Err(e) = run_connection(
                tls,
                peer.ip().to_string(),
                conn_id,
                shared,
                "tls",
                peer_cert,
                accepted_at,
            )
            .await
            {
                debug!("Kernel MQTT PSK connection {} ended: {}", conn_id, e);
            }
        });
    }
}

/// Builds the peer fields of one PSK connection. The handshake
/// verified the identity, so the bind trusts it. A missing or
/// non-UTF-8 identity fails closed with empty fields.
fn psk_peer_cert(tls: &tokio_openssl::SslStream<tokio::net::TcpStream>) -> PeerCert {
    let psk_identity = tls
        .ssl()
        .psk_identity()
        .and_then(|id| std::str::from_utf8(id).ok())
        .filter(|text| !text.is_empty())
        .map(str::to_string);
    PeerCert {
        psk_identity,
        ..PeerCert::default()
    }
}

/// Largest HTTP upgrade head the broker reads, in bytes.
/// Reason: the security task sets an 8 KiB upgrade limit.
/// 8 KiB bounds memory per connection. Larger heads are rejected.
const MAX_WS_UPGRADE_BYTES: usize = 8 * 1024;
/// Target size of the WebSocket write buffer in bytes.
/// Reason: the transport batches one MQTT flush into one frame,
/// so the library buffer only needs small frames in one write.
const WS_WRITE_BUFFER_BYTES: usize = 8 * 1024;
/// Most ping frames without an MQTT byte before the close.
/// Reason: MQTT sends a ping only when idle.
/// 32 pings with no MQTT byte show a flood.
/// The broker then closes the connection.
const MAX_WS_PINGS_WITHOUT_BINARY: u32 = 32;

/// Builds the WebSocket bound config for one cap.
/// The frame bound matches the MQTT packet bound, so one peer cannot
/// grow memory past the configured `session.max_packet_size`.
fn ws_bound_config(cap: usize) -> WebSocketConfig {
    WebSocketConfig {
        write_buffer_size: WS_WRITE_BUFFER_BYTES,
        max_write_buffer_size: cap,
        max_message_size: Some(cap),
        max_frame_size: Some(cap),
        accept_unmasked_frames: false,
        ..WebSocketConfig::default()
    }
}

/// Reads the configured packet cap of one shared state.
/// Falls back to the documented default when the manager reports
/// zero, so one connection stays bounded in tests that skip boot.
fn packet_cap(shared: &Shared) -> usize {
    let cap = shared.sessions.max_packet_size();
    if cap == 0 {
        return FALLBACK_MAX_PACKET_BYTES;
    }
    (cap as usize).min(broker_session::MAX_PACKET_SIZE_WIRE as usize)
}

/// Checks the WebSocket upgrade request.
/// The path must match. The client must offer the `mqtt` subprotocol.
/// The answer selects `mqtt`. A wrong path gets 404.
/// A missing subprotocol gets 400.
// The callback returns the HTTP response type of the library.
// That type exceeds the lint limit. The signature is fixed.
#[allow(clippy::result_large_err)]
fn ws_upgrade_answer(
    req: &Request,
    path: &str,
    mut resp: Response,
) -> Result<Response, http::Response<Option<String>>> {
    if req.uri().path() != path {
        let denied = http::Response::builder()
            .status(404)
            .body(Some("Unknown WS path".to_string()))
            .unwrap_or_else(|_| {
                http::Response::builder()
                    .status(404)
                    .body(None)
                    .expect("empty 404 builds")
            });
        return Err(denied);
    }
    let wants_mqtt = req
        .headers()
        .get("Sec-WebSocket-Protocol")
        .and_then(|value| value.to_str().ok())
        .map(|value| {
            value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("mqtt"))
        })
        .unwrap_or(false);
    if !wants_mqtt {
        let denied = http::Response::builder()
            .status(400)
            .body(Some("mqtt subprotocol required".to_string()))
            .unwrap_or_else(|_| {
                http::Response::builder()
                    .status(400)
                    .body(None)
                    .expect("empty 400 builds")
            });
        return Err(denied);
    }
    resp.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        "mqtt".parse().expect("static header parses"),
    );
    Ok(resp)
}

/// Accepts MQTT clients over WebSocket and runs one task for each.
/// The HTTP upgrade requires the configured path and the `mqtt`
/// subprotocol. The upgrade head holds at most 8 KiB, and the
/// upgrade keeps the connect time limit. A wrong path gets 404.
/// A missing subprotocol gets 400. MQTT travels in binary frames.
/// A text frame is a protocol error and closes the connection.
/// This transport answers a ping with one pong. A ping flood closes
/// the connection. The
/// listener name is `ws` for tenant rules. The same packet limits,
/// the connect timeout from accept, and `max_connections` apply
/// as on TCP.
// The handshake callback returns the HTTP response type of the library.
// That type exceeds the lint limit. The signature is fixed.
#[allow(clippy::result_large_err)]
pub(super) async fn serve_native_ws(
    listener: TcpListener,
    path: String,
    max_connections: Option<u32>,
    count: Arc<AtomicU64>,
    shared: Shared,
) {
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                warn!("Kernel MQTT WebSocket accept error: {}", e);
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let Some(slot) = PerListenerSlot::take(&count, max_connections) else {
            debug!("Kernel MQTT WebSocket listener is at its connection limit");
            continue;
        };
        let _ = stream.set_nodelay(true);
        let shared = shared.clone();
        let path = path.clone();
        let cap = packet_cap(&shared);
        let conn_id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
        let accepted_at = Instant::now();
        tokio::spawn(async move {
            let _slot = slot;
            let mut ws = match tokio::time::timeout(
                CONNECT_TIMEOUT,
                tokio_tungstenite::accept_hdr_async_with_config(
                    HandshakeLimit::new(stream),
                    |req: &Request, resp: Response| ws_upgrade_answer(req, &path, resp),
                    Some(ws_bound_config(cap)),
                ),
            )
            .await
            {
                Ok(Ok(ws)) => ws,
                Ok(Err(e)) => {
                    debug!(
                        "Kernel MQTT WebSocket handshake with {} failed: {}",
                        peer, e
                    );
                    return;
                }
                Err(_) => {
                    debug!("Kernel MQTT WebSocket handshake with {} timed out", peer);
                    return;
                }
            };
            // The upgrade passed. The read cap ends here, so MQTT
            // bytes never count against it.
            ws.get_mut().release();
            let io = WsIo::new(ws, cap);
            if let Err(e) = run_connection(
                io,
                peer.ip().to_string(),
                conn_id,
                shared,
                "ws",
                PeerCert::default(),
                accepted_at,
            )
            .await
            {
                debug!("Kernel MQTT WebSocket connection {} ended: {}", conn_id, e);
            }
        });
    }
}

/// Accepts MQTT clients over secure WebSocket and runs one task for each.
/// TLS wraps the socket first. The upgrade then runs on the TLS stream
/// with the same path and subprotocol checks as the plain WebSocket
/// listener. The handshake uses the same certificate settings as the
/// TLS listener with TLS 1.2 and 1.3. A client without TLS fails the
/// TLS handshake and the task ends. The TLS handshake has the TLS time
/// limit. The upgrade has the connect time limit. The listener name is
/// `wss` for tenant rules. The same packet limits, the connect timeout
/// from accept, and `max_connections` apply as on TCP.
// The handshake callback returns the HTTP response type of the library.
// That type exceeds the lint limit. The signature is fixed.
#[allow(clippy::result_large_err)]
pub(super) async fn serve_native_wss(
    listener: TcpListener,
    config: Arc<rustls::ServerConfig>,
    path: String,
    max_connections: Option<u32>,
    count: Arc<AtomicU64>,
    shared: Shared,
) {
    let acceptor = tokio_rustls::TlsAcceptor::from(config);
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                warn!("Kernel MQTT secure WebSocket accept error: {}", e);
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let Some(slot) = PerListenerSlot::take(&count, max_connections) else {
            debug!("Kernel MQTT secure WebSocket listener is at its connection limit");
            continue;
        };
        let _ = stream.set_nodelay(true);
        let shared = shared.clone();
        let acceptor = acceptor.clone();
        let path = path.clone();
        let cap = packet_cap(&shared);
        let conn_id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
        let accepted_at = Instant::now();
        tokio::spawn(async move {
            let _slot = slot;
            let tls =
                match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                    Ok(Ok(tls)) => tls,
                    Ok(Err(e)) => {
                        debug!(
                            "Kernel MQTT secure WebSocket TLS handshake with {} failed: {}",
                            peer, e
                        );
                        return;
                    }
                    Err(_) => {
                        debug!(
                            "Kernel MQTT secure WebSocket TLS handshake with {} timed out",
                            peer
                        );
                        return;
                    }
                };
            let peer_cert = peer_cert_fields(tls.get_ref().1.peer_certificates());
            let mut ws = match tokio::time::timeout(
                CONNECT_TIMEOUT,
                tokio_tungstenite::accept_hdr_async_with_config(
                    HandshakeLimit::new(tls),
                    |req: &Request, resp: Response| ws_upgrade_answer(req, &path, resp),
                    Some(ws_bound_config(cap)),
                ),
            )
            .await
            {
                Ok(Ok(ws)) => ws,
                Ok(Err(e)) => {
                    debug!(
                        "Kernel MQTT secure WebSocket handshake with {} failed: {}",
                        peer, e
                    );
                    return;
                }
                Err(_) => {
                    debug!(
                        "Kernel MQTT secure WebSocket handshake with {} timed out",
                        peer
                    );
                    return;
                }
            };
            // The upgrade passed. The read cap ends here, so MQTT
            // bytes never count against it.
            ws.get_mut().release();
            let io = WsIo::new(ws, cap);
            if let Err(e) = run_connection(
                io,
                peer.ip().to_string(),
                conn_id,
                shared,
                "wss",
                peer_cert,
                accepted_at,
            )
            .await
            {
                debug!(
                    "Kernel MQTT secure WebSocket connection {} ended: {}",
                    conn_id, e
                );
            }
        });
    }
}

/// Read cap for the WebSocket upgrade head. Counts bytes read
/// while armed and fails past [`MAX_WS_UPGRADE_BYTES`]. The
/// handshake disarms it, so MQTT bytes never count. One peer
/// cannot hold more than the cap before the handshake ends.
struct HandshakeLimit<S> {
    /// The socket. Plain TCP or the TLS stream.
    inner: S,
    /// Bytes read while armed.
    read: usize,
    /// True until the handshake passes.
    armed: bool,
}

impl<S> HandshakeLimit<S> {
    /// Wraps one socket with the cap armed.
    fn new(inner: S) -> Self {
        Self {
            inner,
            read: 0,
            armed: true,
        }
    }

    /// Disarms the cap after the handshake passes.
    fn release(&mut self) {
        self.armed = false;
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for HandshakeLimit<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.armed && this.read >= MAX_WS_UPGRADE_BYTES {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                "websocket upgrade head is too large",
            )));
        }
        let before = buf.filled().len();
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                if this.armed {
                    this.read += buf.filled().len().saturating_sub(before);
                    if this.read > MAX_WS_UPGRADE_BYTES {
                        return Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::ConnectionAborted,
                            "websocket upgrade head is too large",
                        )));
                    }
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for HandshakeLimit<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, data)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// One WebSocket connection as a byte transport.
/// Binary payloads are MQTT bytes. This transport answers a ping
/// with one pong. Text is an error.
/// A ping flood closes the connection.
/// Idle memory of one connection: the three empty buffers below
/// (about one hundred bytes) plus the small library state. The
/// library keeps no read buffer in this version, and the write
/// buffer stays empty with nothing queued, so one idle connection
/// holds well under one KiB plus socket state.
struct WsIo<S> {
    /// The WebSocket stream. It yields messages and takes replies.
    inner: S,
    /// Largest accepted binary frame in bytes. It is the packet limit of the listener.
    max_frame_bytes: usize,
    /// Binary bytes not yet read by the MQTT layer.
    pending_in: BytesMut,
    /// MQTT bytes not yet sent as a binary frame.
    pending_out: Vec<u8>,
    /// One pong payload that waits for the next flush. At most
    /// one: a newer ping replaces the older one.
    pending_pong: Option<Vec<u8>>,
    /// Ping frames since the last binary frame. Past
    /// [`MAX_WS_PINGS_WITHOUT_BINARY`] the connection closes.
    pings_since_binary: u32,
    /// A pong is in the sink and the flush did not end yet.
    /// An idle connection has no MQTT write that flushes it.
    pong_unflushed: bool,
}

impl<S> WsIo<S> {
    /// Wraps a WebSocket stream as a byte transport.
    fn new(inner: S, max_frame_bytes: usize) -> Self {
        Self {
            inner,
            max_frame_bytes,
            pending_in: BytesMut::new(),
            pending_out: Vec::new(),
            pending_pong: None,
            pings_since_binary: 0,
            pong_unflushed: false,
        }
    }
}

/// Starts one send when the sink is ready. Queues nothing.
fn ws_poll_send<S>(
    inner: Pin<&mut S>,
    cx: &mut Context<'_>,
    message: Message,
) -> Poll<std::io::Result<()>>
where
    S: Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    let mut inner = inner;
    match inner.as_mut().poll_ready(cx) {
        Poll::Pending => Poll::Pending,
        Poll::Ready(Ok(())) => match inner.as_mut().start_send(message) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(e) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                format!("websocket send failed: {e}"),
            ))),
        },
        Poll::Ready(Err(e)) => Poll::Ready(Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted,
            format!("websocket send failed: {e}"),
        ))),
    }
}

impl<S> AsyncRead for WsIo<S>
where
    S: Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    S: Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = &mut *self;
        // The pong of an idle connection goes out on the read path.
        if let Some(payload) = this.pending_pong.take() {
            match ws_poll_send(
                Pin::new(&mut this.inner),
                cx,
                Message::Pong(payload.clone()),
            ) {
                Poll::Ready(Ok(())) => this.pong_unflushed = true,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => this.pending_pong = Some(payload),
            }
        }
        if this.pong_unflushed {
            match Pin::new(&mut this.inner).poll_flush(cx) {
                Poll::Ready(Ok(())) => this.pong_unflushed = false,
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionAborted,
                        format!("websocket send failed: {e}"),
                    )));
                }
                Poll::Pending => this.pong_unflushed = true,
            }
        }
        if !this.pending_in.is_empty() {
            let take = this.pending_in.len().min(buf.remaining());
            buf.put_slice(&this.pending_in[..take]);
            let _ = this.pending_in.split_to(take);
            return Poll::Ready(Ok(()));
        }
        loop {
            let next = Pin::new(&mut this.inner).poll_next(cx);
            match next {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionAborted,
                        format!("websocket read failed: {e}"),
                    )));
                }
                Poll::Ready(Some(Ok(message))) => match message {
                    Message::Binary(data) => {
                        this.pings_since_binary = 0;
                        if data.is_empty() {
                            continue;
                        }
                        if data.len() > this.max_frame_bytes {
                            return Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::ConnectionAborted,
                                "websocket frame is too large",
                            )));
                        }
                        let take = data.len().min(buf.remaining());
                        buf.put_slice(&data[..take]);
                        if take < data.len() {
                            this.pending_in.extend_from_slice(&data[take..]);
                        }
                        return Poll::Ready(Ok(()));
                    }
                    Message::Ping(payload) => {
                        this.pings_since_binary = this.pings_since_binary.saturating_add(1);
                        if this.pings_since_binary > MAX_WS_PINGS_WITHOUT_BINARY {
                            return Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::ConnectionAborted,
                                "websocket ping flood",
                            )));
                        }
                        match ws_poll_send(
                            Pin::new(&mut this.inner),
                            cx,
                            Message::Pong(payload.clone()),
                        ) {
                            // Flush now. An idle connection must answer the ping.
                            Poll::Ready(Ok(())) => match Pin::new(&mut this.inner).poll_flush(cx) {
                                Poll::Ready(Ok(())) => this.pong_unflushed = false,
                                Poll::Ready(Err(e)) => {
                                    return Poll::Ready(Err(std::io::Error::new(
                                        std::io::ErrorKind::ConnectionAborted,
                                        format!("websocket send failed: {e}"),
                                    )));
                                }
                                Poll::Pending => this.pong_unflushed = true,
                            },
                            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                            Poll::Pending => {
                                this.pending_pong = Some(payload);
                            }
                        }
                        continue;
                    }
                    Message::Pong(_) => continue,
                    Message::Text(_) => {
                        return Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::ConnectionAborted,
                            "text frame on an MQTT transport",
                        )));
                    }
                    Message::Close(_) => return Poll::Ready(Ok(())),
                    _ => {
                        return Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::ConnectionAborted,
                            "unsupported websocket frame",
                        )));
                    }
                },
            }
        }
    }
}

impl<S> AsyncWrite for WsIo<S>
where
    S: Sink<Message> + Unpin,
    S::Error: std::fmt::Display,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        // One flush sends the batch as one binary frame. The batch
        // holds at most EGRESS_BATCH_BYTES, far below the frame bound.
        self.pending_out.extend_from_slice(data);
        Poll::Ready(Ok(data.len()))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = &mut *self;
        // Readiness runs before any take, so no bytes are lost.
        // At most one pong waits here (see `poll_read`).
        if let Some(payload) = this.pending_pong.take() {
            match Pin::new(&mut this.inner).poll_ready(cx) {
                Poll::Pending => {
                    this.pending_pong = Some(payload);
                    return Poll::Pending;
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionAborted,
                        format!("websocket send failed: {e}"),
                    )));
                }
                Poll::Ready(Ok(())) => {}
            }
            if let Err(e) = Pin::new(&mut this.inner).start_send(Message::Pong(payload)) {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    format!("websocket send failed: {e}"),
                )));
            }
        }
        if !this.pending_out.is_empty() {
            match Pin::new(&mut this.inner).poll_ready(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::ConnectionAborted,
                        format!("websocket send failed: {e}"),
                    )));
                }
                Poll::Ready(Ok(())) => {}
            }
            let data = std::mem::take(&mut this.pending_out);
            if let Err(e) = Pin::new(&mut this.inner).start_send(Message::Binary(data)) {
                return Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    format!("websocket send failed: {e}"),
                )));
            }
        }
        match Pin::new(&mut this.inner).poll_flush(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                format!("websocket flush failed: {e}"),
            ))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_close(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                format!("websocket close failed: {e}"),
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Collects the MQTT bytes for frames that the kernel sends to this
/// connection. [`handle_inbound_frame_with`] sends its replies through
/// the transport trait. This type encodes each reply at once and keeps
/// the bytes until the connection task writes them.
struct NativeSink {
    out: parking_lot::Mutex<Vec<u8>>,
    /// Negotiated protocol level (4 until CONNECT is decoded).
    level: AtomicU8,
    /// Set when a frame told the connection to close after the write.
    close: AtomicBool,
    /// Set when the kernel accepted the bind.
    accepted: AtomicBool,
}

impl NativeSink {
    fn new() -> Self {
        Self {
            out: parking_lot::Mutex::new(Vec::new()),
            level: AtomicU8::new(4),
            close: AtomicBool::new(false),
            accepted: AtomicBool::new(false),
        }
    }

    fn level(&self) -> u8 {
        self.level.load(Ordering::Relaxed)
    }

    fn encode(&self, frame: &BrokerFrame) {
        let level = self.level();
        let mut out = self.out.lock();
        match encode_frame(frame, level, &mut out) {
            FrameEffect::None => {}
            FrameEffect::Accepted => self.accepted.store(true, Ordering::Relaxed),
            FrameEffect::Close => self.close.store(true, Ordering::Relaxed),
        }
    }

    fn take(&self) -> Vec<u8> {
        std::mem::take(&mut *self.out.lock())
    }
}

#[async_trait]
impl BrokerLinkTransport for NativeSink {
    async fn send(&self, frame: BrokerFrame) -> brokerlink::Result<()> {
        self.encode(&frame);
        Ok(())
    }

    async fn recv(&self) -> brokerlink::Result<BrokerFrame> {
        // The connection task reads the socket. No caller uses this.
        Err(BrokerLinkError::ConnectionClosed)
    }
}

/// What one encoded frame means for the connection.
enum FrameEffect {
    None,
    /// The kernel accepted the bind.
    Accepted,
    /// Close the socket after the bytes are written.
    Close,
}

/// The return code of a CONNACK for protocol level 3 or 4.
fn connack_return_code(reason: u8) -> u8 {
    match reason {
        0..=5 => reason,
        0x84 => 1,
        0x85 => 2,
        0x86 => 4,
        0x87 | 0x8A => 5,
        _ => 3,
    }
}

/// The parts of a `PublishOut` frame metadata. The topic and the user
/// properties borrow from the frame.
struct PublishOutMeta<'a> {
    topic: &'a str,
    packet_id: u16,
    qos: u8,
    retain: bool,
    dup: bool,
    alias: u16,
    subscription_id: u32,
    payload_format: u8,
    message_expiry: u32,
    users: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Decodes `TopicLen:16 | Topic | PacketId:16 | QoS | Retain | Dup`,
/// then optionally `Alias:16`, then optionally `SubId:32`, then
/// optionally `Format | Expiry:32 | UserCount:16 | pairs`.
fn decode_publish_out(meta: &[u8]) -> Option<PublishOutMeta<'_>> {
    if meta.len() < 7 {
        return None;
    }
    let topic_len = usize::from(u16::from_be_bytes([meta[0], meta[1]]));
    let head = 2 + topic_len + 5;
    if meta.len() < head {
        return None;
    }
    let topic = std::str::from_utf8(&meta[2..2 + topic_len]).ok()?;
    let base = 2 + topic_len;
    let mut parsed = PublishOutMeta {
        topic,
        packet_id: u16::from_be_bytes([meta[base], meta[base + 1]]),
        qos: meta[base + 2],
        retain: meta[base + 3] != 0,
        dup: meta[base + 4] != 0,
        alias: 0,
        subscription_id: 0,
        payload_format: 0,
        message_expiry: 0,
        users: Vec::new(),
    };
    let mut rest = &meta[head..];
    if rest.is_empty() {
        return Some(parsed);
    }
    if rest.len() < 2 {
        return None;
    }
    parsed.alias = u16::from_be_bytes([rest[0], rest[1]]);
    rest = &rest[2..];
    if rest.is_empty() {
        return Some(parsed);
    }
    if rest.len() < 4 {
        return None;
    }
    parsed.subscription_id = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
    rest = &rest[4..];
    if rest.is_empty() {
        return Some(parsed);
    }
    if rest.len() < 7 {
        return None;
    }
    parsed.payload_format = rest[0];
    parsed.message_expiry = u32::from_be_bytes([rest[1], rest[2], rest[3], rest[4]]);
    let count = usize::from(u16::from_be_bytes([rest[5], rest[6]]));
    rest = &rest[7..];
    for _ in 0..count {
        if rest.len() < 2 {
            return None;
        }
        let key_len = usize::from(u16::from_be_bytes([rest[0], rest[1]]));
        if rest.len() < 2 + key_len + 2 {
            return None;
        }
        let key = rest[2..2 + key_len].to_vec();
        rest = &rest[2 + key_len..];
        let value_len = usize::from(u16::from_be_bytes([rest[0], rest[1]]));
        if rest.len() < 2 + value_len {
            return None;
        }
        let value = rest[2..2 + value_len].to_vec();
        rest = &rest[2 + value_len..];
        parsed.users.push((key, value));
    }
    if !rest.is_empty() {
        return None;
    }
    Some(parsed)
}

/// Encodes one kernel frame as MQTT bytes for the client.
fn encode_frame(frame: &BrokerFrame, level: u8, out: &mut Vec<u8>) -> FrameEffect {
    let meta = &frame.metadata[..];
    match frame.header.opcode {
        OpCode::PublishOut => {
            let Some(publish) = decode_publish_out(meta) else {
                return FrameEffect::Close;
            };
            wire::encode_publish(
                &PublishOut {
                    dup: publish.dup,
                    qos: publish.qos,
                    retain: publish.retain,
                    topic: publish.topic,
                    packet_id: publish.packet_id,
                    alias: publish.alias,
                    subscription_id: publish.subscription_id,
                    payload_format: publish.payload_format,
                    message_expiry: publish.message_expiry,
                    user_properties: &publish.users,
                    payload: &frame.payload,
                },
                level,
                out,
            );
            FrameEffect::None
        }
        OpCode::SessionBinding => {
            let Some((_session_id, present, reason, alias_max, v5)) =
                decode_session_binding_full(meta)
            else {
                return FrameEffect::Close;
            };
            if level == 5 {
                let empty: Vec<(String, String)> = Vec::new();
                let mut props = ConnackProperties {
                    topic_alias_maximum: (alias_max >= 1).then_some(alias_max),
                    user_properties: &empty,
                    ..ConnackProperties::default()
                };
                if let Some(v5) = v5.as_ref() {
                    props.session_expiry = (v5.granted_expiry >= 1).then_some(v5.granted_expiry);
                    props.receive_maximum = (v5.server_recv_max >= 1).then_some(v5.server_recv_max);
                    props.maximum_packet_size =
                        (v5.server_max_pkt >= 1).then_some(v5.server_max_pkt);
                    props.reason_string = (!v5.reason.is_empty()).then_some(v5.reason.as_str());
                    props.user_properties = &v5.user_properties;
                }
                wire::encode_connack_v5(present, reason, &props, out);
            } else {
                wire::encode_connack(present, connack_return_code(reason), out);
            }
            if reason == 0 {
                FrameEffect::Accepted
            } else {
                FrameEffect::Close
            }
        }
        OpCode::SubAckOut => {
            if meta.len() < 2 {
                return FrameEffect::Close;
            }
            let packet_id = u16::from_be_bytes([meta[0], meta[1]]);
            wire::encode_suback(packet_id, &meta[2..], level, out);
            FrameEffect::None
        }
        OpCode::UnsubAckOut => {
            if meta.len() < 2 {
                return FrameEffect::Close;
            }
            let packet_id = u16::from_be_bytes([meta[0], meta[1]]);
            wire::encode_unsuback(packet_id, &meta[2..], level, out);
            FrameEffect::None
        }
        OpCode::PubAckOut | OpCode::PubRecOut | OpCode::PubRelOut | OpCode::PubCompOut => {
            if meta.len() < 2 {
                return FrameEffect::Close;
            }
            let packet_id = u16::from_be_bytes([meta[0], meta[1]]);
            let reason = meta.get(2).copied().unwrap_or(0);
            let packet_kind = match frame.header.opcode {
                OpCode::PubAckOut => kind::PUBACK,
                OpCode::PubRecOut => kind::PUBREC,
                OpCode::PubRelOut => kind::PUBREL,
                _ => kind::PUBCOMP,
            };
            wire::encode_ack(packet_kind, packet_id, reason, level, out);
            FrameEffect::None
        }
        OpCode::ConnClose => {
            // An MQTT 5 client gets a DISCONNECT with the reason code
            // before the socket closes. A 3.1.1 client gets no packet.
            if level == 5 {
                if let Some(reason) = meta.first() {
                    wire::encode_disconnect_v5(*reason, out);
                }
            }
            FrameEffect::Close
        }
        _ => FrameEffect::None,
    }
}

/// Why the connection task stops.
enum Stop {
    /// The client sent DISCONNECT, or the kernel closed the connection.
    Clean,
    /// The socket closed, a timer ended or a packet was incorrect.
    Abnormal,
}

/// The state of one connection that is not in the kernel tables.
struct Conn {
    conn_id: u64,
    seq: u64,
    level: u8,
    client_id: String,
    connected: bool,
    keepalive: Option<Duration>,
    peer: String,
    /// Set by the kernel when it orders the close of this connection (a
    /// denial with `deny_action = disconnect`). Packets that are already
    /// in the read buffer are not handled after that.
    closing: Option<Arc<AtomicBool>>,
}

impl Conn {
    fn frame(&mut self, opcode: OpCode, meta: Vec<u8>, payload: Bytes) -> Option<BrokerFrame> {
        self.seq += 1;
        BrokerFrame::new(opcode, self.conn_id, self.seq, Bytes::from(meta), payload).ok()
    }

    fn id_meta(&self) -> Vec<u8> {
        let id = self.client_id.as_bytes();
        let mut meta = Vec::with_capacity(2 + id.len() + 1);
        meta.extend_from_slice(&(id.len() as u16).to_be_bytes());
        meta.extend_from_slice(id);
        meta
    }
}

/// Takes one `peer_cert_as_*` mode and returns the certificate value.
/// `cn` selects the common name. `dn` selects the subject name.
/// `disabled` and unknown modes return `None`.
/// An absent or empty source returns `None`.
/// A bind with mapping on but no certificate keeps the CONNECT values.
fn cert_identity_value(mode: &str, peer_cert: &PeerCert) -> Option<String> {
    let value = match mode {
        "cn" => peer_cert.cn.clone(),
        "dn" => peer_cert.subject.clone(),
        _ => None,
    }?;
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

/// Makes the bind request of one CONNECT on a kernel-owned connection.
/// The verified certificate fills the same bind fields the edge sends:
/// common name, subject and SANs. The listener policy takes the bind
/// username and client identifier from the certificate when set.
/// Connections the edge holds keep their CONNECT values: the edge
/// has no such setting.
/// Only kernel-held connections map `peer_cert_as_*`.
fn bind_request(
    connect: wire::Connect,
    peer: &str,
    listener: &str,
    peer_cert: &PeerCert,
    peer_as_username: &str,
    peer_as_clientid: &str,
) -> BindRequest {
    let props = connect.properties;
    // A verified PSK identity replaces the CONNECT username: the
    // bind carries the value authentication checks.
    // A PSK bind keeps accepted behavior.
    // It never grants a session by username.
    let username = peer_cert
        .psk_identity
        .clone()
        .or_else(|| cert_identity_value(peer_as_username, peer_cert))
        .or(connect.username);
    BindRequest {
        client_id: cert_identity_value(peer_as_clientid, peer_cert).unwrap_or(connect.client_id),
        clean_start: connect.clean_start,
        keepalive_secs: connect.keepalive,
        username,
        password: connect.password,
        peerhost: Some(peer.to_string()),
        client_alias_max: props
            .as_ref()
            .and_then(|p| p.topic_alias_maximum)
            .unwrap_or(0),
        will: connect.will.map(|will| BindWill {
            topic: will.topic,
            payload: Bytes::from(will.payload),
            qos: will.qos,
            retain: will.retain,
        }),
        cert_cn: peer_cert.cn.clone(),
        cert_subject: peer_cert.subject.clone(),
        cert_sans: peer_cert.sans.clone(),
        protocol_version: if connect.level == 5 { 5 } else { 4 },
        session_expiry: props.as_ref().and_then(|p| p.session_expiry).unwrap_or(0),
        receive_maximum: props
            .as_ref()
            .and_then(|p| p.receive_maximum)
            .unwrap_or(u16::MAX),
        max_packet_size: props
            .as_ref()
            .and_then(|p| p.maximum_packet_size)
            .unwrap_or(0),
        user_properties: props.map(|p| p.user_properties.into_iter().collect()),
        listener: listener.to_string(),
    }
}

/// The metadata of a `PublishIn` frame for one decoded PUBLISH.
fn publish_in_meta(publish: &wire::Publish<'_>, level: u8) -> Vec<u8> {
    let topic = publish.topic.as_bytes();
    let has_props = publish.payload_format != 0
        || publish.message_expiry != 0
        || !publish.user_properties.is_empty();
    let mut meta = Vec::with_capacity(2 + topic.len() + 5 + 2 + 7);
    meta.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    meta.extend_from_slice(topic);
    meta.extend_from_slice(&publish.packet_id.to_be_bytes());
    meta.push(publish.qos);
    meta.push(u8::from(publish.retain));
    meta.push(u8::from(publish.dup));
    if level == 5 && has_props {
        meta.extend_from_slice(&publish.alias.unwrap_or(0).to_be_bytes());
        meta.push(publish.payload_format);
        meta.extend_from_slice(&publish.message_expiry.to_be_bytes());
        meta.extend_from_slice(&(publish.user_properties.len() as u16).to_be_bytes());
        for (key, value) in &publish.user_properties {
            meta.extend_from_slice(&(key.len() as u16).to_be_bytes());
            meta.extend_from_slice(key.as_bytes());
            meta.extend_from_slice(&(value.len() as u16).to_be_bytes());
            meta.extend_from_slice(value.as_bytes());
        }
    } else if let Some(alias) = publish.alias {
        // The alias section alone. An alias of 0 stays in the frame,
        // thus the kernel refuses it with reason code 0x94.
        meta.extend_from_slice(&alias.to_be_bytes());
    }
    meta
}

/// The metadata of an `UnsubscribeIn` frame.
fn unsubscribe_in_meta(client_id: &str, unsubscribe: &wire::Unsubscribe<'_>) -> Vec<u8> {
    let id = client_id.as_bytes();
    let mut meta = Vec::with_capacity(6 + id.len() + unsubscribe.filters.len() * 16);
    meta.extend_from_slice(&unsubscribe.packet_id.to_be_bytes());
    meta.extend_from_slice(&(id.len() as u16).to_be_bytes());
    meta.extend_from_slice(id);
    meta.extend_from_slice(&(unsubscribe.filters.len() as u16).to_be_bytes());
    for filter in &unsubscribe.filters {
        meta.extend_from_slice(&(filter.len() as u16).to_be_bytes());
        meta.extend_from_slice(filter.as_bytes());
    }
    meta
}

/// The metadata of a `SubscribeIn` frame.
fn subscribe_in_meta(client_id: &str, subscribe: &wire::Subscribe<'_>, level: u8) -> Vec<u8> {
    let id = client_id.as_bytes();
    let mut meta = Vec::with_capacity(6 + id.len() + subscribe.filters.len() * 16);
    meta.extend_from_slice(&subscribe.packet_id.to_be_bytes());
    meta.extend_from_slice(&(id.len() as u16).to_be_bytes());
    meta.extend_from_slice(id);
    meta.extend_from_slice(&(subscribe.filters.len() as u16).to_be_bytes());
    for (filter, options) in &subscribe.filters {
        meta.extend_from_slice(&(filter.len() as u16).to_be_bytes());
        meta.extend_from_slice(filter.as_bytes());
        meta.push(*options);
        if level == 5 {
            meta.extend_from_slice(&subscribe.subscription_id.to_be_bytes());
        }
    }
    meta
}

/// Removes one connection from the kernel tables. `fire_will` tells the
/// kernel that the connection ended without DISCONNECT.
async fn release_connection(
    shared: Shared,
    tx: tokio::sync::mpsc::UnboundedSender<BrokerFrame>,
    mut bound: Vec<(SessionKey, u64)>,
    conn_id: u64,
    client_id: String,
    fire_will: bool,
) {
    if fire_will {
        let id = client_id.as_bytes();
        let mut meta = Vec::with_capacity(2 + id.len());
        meta.extend_from_slice(&(id.len() as u16).to_be_bytes());
        meta.extend_from_slice(id);
        if let Ok(frame) = BrokerFrame::new(
            OpCode::DisconnectIn,
            conn_id,
            0,
            Bytes::from(meta),
            Bytes::new(),
        ) {
            // The replies of this frame have no socket to go to.
            let sink = NativeSink::new();
            let waker = Arc::new(tokio::sync::Notify::new());
            let _ = handle_inbound_frame_with(frame, &shared, &tx, &sink, &mut bound, &waker, None)
                .await;
        }
    }
    detach(&bound, &shared, &tx);
    shared.conns.unregister(conn_id);
}

/// Owns the kernel state of one connection. When the task ends without
/// the normal release (a panic in the task, or a cancelled task), the
/// drop of this guard starts the release. One faulty connection then
/// leaves no session that looks connected and no entry in the tables.
struct ConnGuard {
    shared: Shared,
    tx: tokio::sync::mpsc::UnboundedSender<BrokerFrame>,
    bound: Vec<(SessionKey, u64)>,
    conn_id: u64,
    client_id: String,
    connected: bool,
    released: bool,
}

impl Drop for ConnGuard {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        if std::thread::panicking() {
            error!(
                "Kernel MQTT connection {} stopped on a panic. The other connections continue.",
                self.conn_id
            );
        }
        let release = release_connection(
            self.shared.clone(),
            self.tx.clone(),
            std::mem::take(&mut self.bound),
            self.conn_id,
            std::mem::take(&mut self.client_id),
            self.connected,
        );
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(release);
        }
    }
}

async fn run_connection<S>(
    mut io: S,
    peer: String,
    conn_id: u64,
    shared: Shared,
    listener: &str,
    peer_cert: PeerCert,
    accepted_at: Instant,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let (tx, mut rx) = unbounded_channel::<BrokerFrame>();
    let waker = Arc::new(tokio::sync::Notify::new());
    let mut guard = ConnGuard {
        shared: shared.clone(),
        tx: tx.clone(),
        bound: Vec::new(),
        conn_id,
        client_id: String::new(),
        connected: false,
        released: false,
    };
    let sink = NativeSink::new();
    let mut inbuf = BytesMut::with_capacity(READ_BUFFER_BYTES);
    let mut conn = Conn {
        conn_id,
        seq: 0,
        level: 4,
        client_id: String::new(),
        connected: false,
        keepalive: None,
        peer,
        closing: None,
    };
    let mut last_packet = Instant::now();

    let stop = loop {
        // One deadline: CONNECT in time, then the keepalive of the client.
        let deadline = if conn.connected {
            conn.keepalive.map(|keepalive| last_packet + keepalive)
        } else {
            Some(accepted_at + CONNECT_TIMEOUT)
        };
        let timer = async {
            match deadline {
                Some(at) => tokio::time::sleep_until(at.into()).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            read = io.read_buf(&mut inbuf) => {
                match read {
                    Ok(0) | Err(_) => break Stop::Abnormal,
                    Ok(_) => {}
                }
                last_packet = Instant::now();
                let drained = drain_inbound(
                    &mut inbuf,
                    &mut conn,
                    &shared,
                    &tx,
                    &sink,
                    &mut guard.bound,
                    &waker,
                    listener,
                    &peer_cert,
                )
                .await;
                // The guard must know the state for a release after a panic.
                guard.connected = conn.connected;
                if guard.client_id.is_empty() && !conn.client_id.is_empty() {
                    guard.client_id.clone_from(&conn.client_id);
                }
                match drained {
                    Ok(None) => {}
                    Ok(Some(stop)) => {
                        let _ = flush(&sink, &mut io).await;
                        break stop;
                    }
                    Err(reason) => {
                        debug!("Kernel MQTT connection {}: {}", conn_id, reason);
                        break Stop::Abnormal;
                    }
                }
                // Frames for this connection that the packets above
                // made (a client that subscribes to its own topic).
                collect_egress(&mut rx, &shared, conn.conn_id, &sink, &waker);
            }
            routed = rx.recv() => {
                let Some(frame) = routed else { break Stop::Abnormal };
                sink.encode(&frame);
                shared.metrics.inc_transport_sent_by(1);
                collect_egress(&mut rx, &shared, conn.conn_id, &sink, &waker);
            }
            () = waker.notified() => {
                collect_egress(&mut rx, &shared, conn.conn_id, &sink, &waker);
            }
            () = timer => break Stop::Abnormal,
        }
        if flush(&sink, &mut io).await.is_err() {
            break Stop::Abnormal;
        }
        if sink.close.load(Ordering::Relaxed) {
            break Stop::Clean;
        }
    };

    // An abnormal end of an accepted connection is a disconnect without
    // DISCONNECT: the kernel publishes the will.
    let fire_will = matches!(stop, Stop::Abnormal) && conn.connected;
    guard.released = true;
    release_connection(
        shared,
        tx,
        std::mem::take(&mut guard.bound),
        conn_id,
        std::mem::take(&mut conn.client_id),
        fire_will,
    )
    .await;
    let _ = io.shutdown().await;
    Ok(())
}

/// Writes the collected bytes to the socket in one write.
async fn flush<W>(sink: &NativeSink, writer: &mut W) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin + Send,
{
    let bytes = sink.take();
    if bytes.is_empty() {
        return Ok(());
    }
    let write = async {
        writer.write_all(&bytes).await?;
        // A TLS stream keeps the record until the flush.
        writer.flush().await
    };
    match tokio::time::timeout(WRITE_TIMEOUT, write).await {
        Ok(result) => result,
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "the client does not read",
        )),
    }
}

/// Encodes the frames that wait for this connection: the guaranteed
/// frames of the mailbox and the QoS 0 frames of the bounded queue, in
/// their order. When one pass uses its full budget, the task wakes
/// itself again, thus the remaining frames do not wait for a new event.
fn collect_egress(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<BrokerFrame>,
    shared: &Shared,
    conn_id: u64,
    sink: &NativeSink,
    waker: &tokio::sync::Notify,
) {
    let mut guaranteed = Vec::new();
    let mut bytes = 0usize;
    while guaranteed.len() < EGRESS_BATCH_FRAMES && bytes < EGRESS_BATCH_BYTES {
        match rx.try_recv() {
            Ok(frame) => {
                bytes += frame.total_frame_len();
                guaranteed.push(frame);
            }
            Err(_) => break,
        }
    }
    let qos0 = shared.conns.drain_qos0_with_budget(
        conn_id,
        EGRESS_BATCH_FRAMES.saturating_sub(guaranteed.len()),
        EGRESS_BATCH_BYTES.saturating_sub(bytes),
    );
    if guaranteed.is_empty() && qos0.is_empty() {
        return;
    }
    let batch = merge_egress_batch(guaranteed, qos0);
    let batch_bytes: usize = batch.iter().map(BrokerFrame::total_frame_len).sum();
    for frame in &batch {
        sink.encode(frame);
    }
    shared.metrics.inc_transport_sent_by(batch.len() as u64);
    if batch.len() >= EGRESS_BATCH_FRAMES || batch_bytes >= EGRESS_BATCH_BYTES {
        waker.notify_one();
    }
}

/// Handles each complete packet in the read buffer.
#[allow(clippy::too_many_arguments)]
async fn drain_inbound(
    inbuf: &mut BytesMut,
    conn: &mut Conn,
    shared: &Shared,
    tx: &tokio::sync::mpsc::UnboundedSender<BrokerFrame>,
    sink: &NativeSink,
    bound: &mut Vec<(SessionKey, u64)>,
    waker: &Arc<tokio::sync::Notify>,
    listener: &str,
    peer_cert: &PeerCert,
) -> Result<Option<Stop>, WireError> {
    loop {
        // One atomic load for each packet, no lock. After a close order the
        // rest of the buffer is not handled: the task writes the close
        // packet and ends.
        if conn
            .closing
            .as_ref()
            .is_some_and(|flag| flag.load(Ordering::Acquire))
        {
            return Ok(None);
        }
        let Some(header) = wire::fixed_header(inbuf)? else {
            return Ok(None);
        };
        let limit = if conn.connected {
            packet_cap(shared)
        } else {
            MAX_CONNECT_PACKET_BYTES
        };
        if header.packet_len() > limit {
            // P2: an oversize packet closes only this connection. A v5
            // client gets DISCONNECT 0x95 first. The close still counts
            // as abnormal, so the will fires.
            // TODO(parity): the rulebook does not decide whether the
            // will fires on a size refusal. Current choice keeps the
            // abnormal close, so the will fires.
            if conn.level == 5 {
                wire::encode_disconnect_v5(0x95, &mut sink.out.lock());
            }
            return Ok(Some(Stop::Abnormal));
        }
        if inbuf.len() < header.packet_len() {
            inbuf.reserve(header.packet_len() - inbuf.len());
            return Ok(None);
        }
        let packet = inbuf.split_to(header.packet_len()).freeze();
        let body = packet.slice(header.header_len..);
        if !conn.connected && header.kind != kind::CONNECT {
            return Err(WireError::Malformed("first packet is not CONNECT"));
        }
        let frame = match header.kind {
            kind::CONNECT => {
                if conn.connected || !conn.client_id.is_empty() {
                    return Err(WireError::Malformed("second CONNECT"));
                }
                let connect = wire::decode_connect(&body)?;
                conn.level = if connect.level == 5 { 5 } else { 4 };
                sink.level.store(conn.level, Ordering::Relaxed);
                conn.keepalive = (connect.keepalive > 0)
                    .then(|| Duration::from_millis(u64::from(connect.keepalive) * 1500));
                // The listener policy selects the certificate identity
                // for kernel-owned connections. Other listeners keep
                // the CONNECT values.
                let (peer_as_username, peer_as_clientid) = match listener {
                    "tls" => (
                        shared.tls_peer_cert_as_username.as_str(),
                        shared.tls_peer_cert_as_clientid.as_str(),
                    ),
                    "wss" => (
                        shared.wss_peer_cert_as_username.as_str(),
                        shared.wss_peer_cert_as_clientid.as_str(),
                    ),
                    _ => ("disabled", "disabled"),
                };
                let request = bind_request(
                    connect,
                    &conn.peer,
                    listener,
                    peer_cert,
                    peer_as_username,
                    peer_as_clientid,
                );
                conn.client_id = request.client_id.clone();
                let Some(frame) = conn.frame(OpCode::BindConnection, Vec::new(), Bytes::new())
                else {
                    return Err(WireError::Malformed("frame"));
                };
                let handled = handle_inbound_frame_with(
                    frame,
                    shared,
                    tx,
                    sink,
                    bound,
                    waker,
                    Some(&request),
                )
                .await;
                if handled.is_err() {
                    return Ok(Some(Stop::Abnormal));
                }
                if sink.accepted.load(Ordering::Relaxed) {
                    conn.connected = true;
                    conn.closing = shared.conns.closing_flag(conn.conn_id);
                } else {
                    // Refused: the CONNACK with the reason is in the sink.
                    return Ok(Some(Stop::Clean));
                }
                continue;
            }
            kind::PUBLISH => {
                let publish = wire::decode_publish(header.flags, &body, conn.level)?;
                let meta = publish_in_meta(&publish, conn.level);
                let payload = body.slice_ref(publish.payload);
                conn.frame(OpCode::PublishIn, meta, payload)
            }
            kind::SUBSCRIBE => {
                let subscribe = wire::decode_subscribe(&body, conn.level)?;
                let meta = subscribe_in_meta(&conn.client_id, &subscribe, conn.level);
                conn.frame(OpCode::SubscribeIn, meta, Bytes::new())
            }
            kind::UNSUBSCRIBE => {
                let unsubscribe = wire::decode_unsubscribe(&body, conn.level)?;
                let meta = unsubscribe_in_meta(&conn.client_id, &unsubscribe);
                conn.frame(OpCode::UnsubscribeIn, meta, Bytes::new())
            }
            kind::PUBACK | kind::PUBREC | kind::PUBREL | kind::PUBCOMP => {
                let (packet_id, reason) = wire::decode_ack(&body, conn.level)?;
                let mut meta = packet_id.to_be_bytes().to_vec();
                if reason != 0 {
                    meta.push(reason);
                }
                let opcode = match header.kind {
                    kind::PUBACK => OpCode::PubAckIn,
                    kind::PUBREC => OpCode::PubRecIn,
                    kind::PUBREL => OpCode::PubRelIn,
                    _ => OpCode::PubCompIn,
                };
                conn.frame(opcode, meta, Bytes::new())
            }
            kind::PINGREQ => {
                shared.metrics.inc_pingreq_received();
                wire::encode_pingresp(&mut sink.out.lock());
                shared.metrics.inc_pingresp_sent();
                continue;
            }
            kind::DISCONNECT => {
                let reason = wire::decode_disconnect(&body, conn.level)?;
                let mut meta = conn.id_meta();
                if conn.level == 5 {
                    meta.push(reason);
                }
                if let Some(frame) = conn.frame(OpCode::UnbindConnection, meta, Bytes::new()) {
                    let _ = handle_inbound_frame_with(frame, shared, tx, sink, bound, waker, None)
                        .await;
                }
                // The kernel handled the disconnect. The task must not
                // send a second one.
                conn.connected = false;
                return Ok(Some(Stop::Clean));
            }
            _ => return Err(WireError::Unsupported("packet type")),
        };
        let Some(frame) = frame else {
            return Err(WireError::Malformed("frame"));
        };
        if handle_inbound_frame_with(frame, shared, tx, sink, bound, waker, None)
            .await
            .is_err()
        {
            return Ok(Some(Stop::Abnormal));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small generator of test bytes (xorshift). The sequence is the
    /// same in each run.
    struct TestBytes(u64);

    impl TestBytes {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn bytes(&mut self, max_len: usize) -> Vec<u8> {
            let len = (self.next() as usize) % (max_len + 1);
            (0..len).map(|_| self.next() as u8).collect()
        }
    }

    #[test]
    fn publish_out_metadata_round_trip() {
        let mut meta = vec![0, 3, b'a', b'/', b'b', 0, 9, 1, 1, 0];
        let plain = decode_publish_out(&meta).expect("plain form");
        assert_eq!((plain.topic, plain.packet_id, plain.qos), ("a/b", 9, 1));
        assert!(plain.retain && !plain.dup);
        meta.extend_from_slice(&[0, 4]);
        assert_eq!(decode_publish_out(&meta).expect("alias form").alias, 4);
        meta.extend_from_slice(&[0, 0, 0, 7]);
        assert_eq!(
            decode_publish_out(&meta)
                .expect("identifier form")
                .subscription_id,
            7
        );
        meta.extend_from_slice(&[1, 0, 0, 0, 60, 0, 1, 0, 1, b'k', 0, 1, b'v']);
        let full = decode_publish_out(&meta).expect("property form");
        assert_eq!((full.payload_format, full.message_expiry), (1, 60));
        assert_eq!(full.users, vec![(b"k".to_vec(), b"v".to_vec())]);
        // One more byte is not a known shape.
        meta.push(0);
        assert!(decode_publish_out(&meta).is_none());
    }

    #[test]
    fn frames_with_incorrect_metadata_do_not_panic() {
        let opcodes = [
            OpCode::PublishOut,
            OpCode::SessionBinding,
            OpCode::SubAckOut,
            OpCode::UnsubAckOut,
            OpCode::PubAckOut,
            OpCode::PubRecOut,
            OpCode::PubRelOut,
            OpCode::PubCompOut,
            OpCode::ConnClose,
            OpCode::Pong,
        ];
        let mut source = TestBytes(0x9E37_79B9_7F4A_7C15);
        for round in 0..200_000u32 {
            let opcode = opcodes[(round as usize) % opcodes.len()];
            let meta = source.bytes(48);
            let payload = source.bytes(16);
            let Ok(frame) = BrokerFrame::new(opcode, 1, 1, Bytes::from(meta), Bytes::from(payload))
            else {
                continue;
            };
            let mut out = Vec::new();
            let level = if round % 2 == 0 { 4 } else { 5 };
            let _ = encode_frame(&frame, level, &mut out);
        }
    }

    /// Binds one client through the production frame handler and gives
    /// back what the connection task keeps.
    async fn bind_client(
        shared: &Shared,
        conn_id: u64,
        client_id: &str,
    ) -> (
        tokio::sync::mpsc::UnboundedSender<BrokerFrame>,
        tokio::sync::mpsc::UnboundedReceiver<BrokerFrame>,
        Vec<(SessionKey, u64)>,
    ) {
        let (tx, rx) = unbounded_channel::<BrokerFrame>();
        let waker = Arc::new(tokio::sync::Notify::new());
        let sink = NativeSink::new();
        let mut bound = Vec::new();
        let request = bind_request(
            wire::Connect {
                level: 4,
                clean_start: true,
                keepalive: 60,
                client_id: client_id.to_string(),
                will: None,
                username: None,
                password: None,
                properties: None,
            },
            "127.0.0.1",
            "tcp",
            &PeerCert::default(),
            "disabled",
            "disabled",
        );
        let frame = BrokerFrame::new(
            OpCode::BindConnection,
            conn_id,
            1,
            Bytes::new(),
            Bytes::new(),
        )
        .expect("frame");
        handle_inbound_frame_with(
            frame,
            shared,
            &tx,
            &sink,
            &mut bound,
            &waker,
            Some(&request),
        )
        .await
        .expect("bind is handled");
        assert!(
            sink.accepted.load(Ordering::Relaxed),
            "the bind is accepted"
        );
        assert_eq!(
            sink.take(),
            [0x20, 0x02, 0x00, 0x00],
            "CONNACK for the client"
        );
        (tx, rx, bound)
    }

    /// Binds one client with credentials through the production frame
    /// handler. Gives back whether the kernel accepted the bind plus the
    /// CONNACK bytes, so tests observe refusals as well as accepts.
    async fn bind_client_with_password(
        shared: &Shared,
        conn_id: u64,
        client_id: &str,
        username: &str,
        password: &[u8],
    ) -> (bool, Vec<u8>) {
        let (tx, _rx) = unbounded_channel::<BrokerFrame>();
        let waker = Arc::new(tokio::sync::Notify::new());
        let sink = NativeSink::new();
        let mut bound = Vec::new();
        let request = bind_request(
            wire::Connect {
                level: 4,
                clean_start: true,
                keepalive: 60,
                client_id: client_id.to_string(),
                will: None,
                username: Some(username.to_string()),
                password: Some(password.to_vec()),
                properties: None,
            },
            "127.0.0.1",
            "tcp",
            &PeerCert::default(),
            "disabled",
            "disabled",
        );
        let frame = BrokerFrame::new(
            OpCode::BindConnection,
            conn_id,
            1,
            Bytes::new(),
            Bytes::new(),
        )
        .expect("frame");
        handle_inbound_frame_with(
            frame,
            shared,
            &tx,
            &sink,
            &mut bound,
            &waker,
            Some(&request),
        )
        .await
        .expect("bind is handled");
        let accepted = sink.accepted.load(Ordering::Relaxed);
        (accepted, sink.take())
    }

    /// TM-01: two tenants hold the same username with different
    /// passwords. Each CONNECT authenticates only in its own tenant,
    /// through the production bind handler (not the store alone). The
    /// tenant rule keys on the client id, so the same username in two
    /// connections lands in two tenants.
    #[tokio::test]
    async fn tm01_connect_authenticates_same_username_in_two_tenants() {
        let shared = Shared::new();
        shared
            .auth
            .set_policy(broker_auth::PasswordHashPolicy::for_tests());
        shared
            .tenant_registry
            .replace(vec![broker_session::tenant::TenantRule::new(
                "",
                "tenant",
                "t-${client_id}",
            )])
            .expect("tenant rule installs");
        shared
            .auth
            .add_user_in_tenant("t-c-a", "sam", b"pw-for-a")
            .expect("first tenant user stores");
        shared
            .auth
            .add_user_in_tenant("t-c-b", "sam", b"pw-for-b")
            .expect("second tenant user stores");
        // Each password opens only its own tenant.
        let (accepted, connack) = bind_client_with_password(
            &shared,
            NATIVE_CONN_ID_BASE + 910_001,
            "c-a",
            "sam",
            b"pw-for-a",
        )
        .await;
        assert!(accepted, "own password binds");
        assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);
        let (accepted, connack) = bind_client_with_password(
            &shared,
            NATIVE_CONN_ID_BASE + 910_002,
            "c-b",
            "sam",
            b"pw-for-b",
        )
        .await;
        assert!(accepted, "own password binds");
        assert_eq!(connack, vec![0x20, 0x02, 0x00, 0x00]);
        // Each password refuses in the other tenant.
        let (accepted, connack) = bind_client_with_password(
            &shared,
            NATIVE_CONN_ID_BASE + 910_003,
            "c-a",
            "sam",
            b"pw-for-b",
        )
        .await;
        assert!(!accepted, "other tenant password refuses");
        assert!(
            connack.len() == 4 && connack[3] != 0,
            "refusal carries a CONNACK code, got: {connack:?}"
        );
        let (accepted, connack) = bind_client_with_password(
            &shared,
            NATIVE_CONN_ID_BASE + 910_004,
            "c-b",
            "sam",
            b"pw-for-a",
        )
        .await;
        assert!(!accepted, "other tenant password refuses");
        assert!(
            connack.len() == 4 && connack[3] != 0,
            "refusal carries a CONNACK code, got: {connack:?}"
        );
    }

    /// Burst binds with one credential share one hash. Each bind goes
    /// through a real TCP socket on the production listener, not the
    /// store alone. The join counter grows, so removing the dedup map
    /// fails this test.
    #[tokio::test]
    async fn burst_same_credential_binds_share_one_verification() {
        use tokio::io::AsyncWriteExt as _;
        let shared = Shared::new();
        shared
            .auth
            .set_policy(broker_auth::PasswordHashPolicy::for_tests());
        shared
            .auth
            .add_user("storm-only", b"storm-secret-value")
            .expect("user stores");
        let port = p2_tcp_listener(shared.clone()).await;
        // 16 binds start together. Reason: 16 waiters prove sharing
        // while staying inside the 32 verify permits.
        let gate = std::sync::Arc::new(tokio::sync::Barrier::new(17));
        let mut tasks = Vec::new();
        for index in 0..16u64 {
            let gate = std::sync::Arc::clone(&gate);
            tasks.push(tokio::spawn(async move {
                gate.wait().await;
                let mut client = tokio::net::TcpStream::connect(("127.0.0.1", port))
                    .await
                    .expect("tcp dials");
                client
                    .write_all(&ws_test_connect_with_auth(
                        &format!("storm-client-{index:02}"),
                        "storm-only",
                        b"storm-secret-value",
                    ))
                    .await
                    .expect("CONNECT sends");
                tcp_test_recv(&mut client).await
            }));
        }
        gate.wait().await;
        for task in tasks {
            assert_eq!(
                task.await.expect("bind task ends"),
                vec![0x20, 0x02, 0x00, 0x00],
                "burst bind is accepted"
            );
        }
        assert!(
            shared.auth.verify_dedup_join_count() > 0,
            "burst shares verifications through the dedup map"
        );
    }

    #[tokio::test]
    async fn a_panic_in_one_connection_releases_only_that_connection() {
        let shared = Shared::new();
        let first = NATIVE_CONN_ID_BASE + 900_001;
        let second = NATIVE_CONN_ID_BASE + 900_002;
        let (tx_a, _rx_a, bound_a) = bind_client(&shared, first, "panic-a").await;
        let (_tx_b, mut rx_b, bound_b) = bind_client(&shared, second, "panic-b").await;
        assert_eq!(bound_a.len(), 1);
        assert_eq!(bound_b.len(), 1);
        let mut connected = shared.sessions.active_client_ids();
        connected.sort();
        assert_eq!(
            connected,
            vec!["panic-a".to_string(), "panic-b".to_string()]
        );

        // The task of the first connection panics while it owns its state.
        let guard_shared = shared.clone();
        let task = tokio::spawn(async move {
            let _guard = ConnGuard {
                shared: guard_shared,
                tx: tx_a,
                bound: bound_a,
                conn_id: first,
                client_id: "panic-a".to_string(),
                connected: true,
                released: false,
            };
            panic!("deliberate panic of one connection task");
        });
        let ended = task.await.expect_err("the task ends with an error");
        assert!(
            ended.is_panic(),
            "the runtime contains the panic in the task"
        );

        // The release runs as a task. Wait until it is complete.
        for _ in 0..100 {
            if shared.sessions.active_client_ids() == vec!["panic-b".to_string()] {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            shared.sessions.active_client_ids(),
            vec!["panic-b".to_string()],
            "only the connection that panicked is released"
        );
        assert!(
            shared.conns.tenant_for(first).is_none(),
            "its table entry is removed"
        );
        assert!(
            shared.conns.tenant_for(second).is_some(),
            "the other entry stays"
        );

        // The second connection still gets frames.
        let frame = BrokerFrame::new(
            OpCode::PubAckOut,
            second,
            0,
            Bytes::from_static(&[0, 7, 0]),
            Bytes::new(),
        )
        .expect("frame");
        assert!(
            shared.conns.route(second, frame),
            "the route to the other connection works"
        );
        let got = tokio::time::timeout(Duration::from_secs(2), rx_b.recv())
            .await
            .expect("a frame in time")
            .expect("a frame");
        assert_eq!(got.header.opcode, OpCode::PubAckOut);
    }

    #[test]
    fn connack_return_codes_for_protocol_level_4() {
        assert_eq!(connack_return_code(0), 0);
        assert_eq!(connack_return_code(0x86), 4);
        assert_eq!(connack_return_code(0x87), 5);
        assert_eq!(connack_return_code(0x8A), 5);
        assert_eq!(connack_return_code(0x97), 3);
    }

    /// No peer certificate yields empty fields.
    /// The bind check then denies only when verification is on.
    #[test]
    fn peer_cert_fields_empty_when_no_cert() {
        let fields = peer_cert_fields(None);
        assert!(fields.cn.is_none());
        assert!(fields.subject.is_none());
        assert!(fields.sans.is_empty());
        let bad: Vec<rustls::pki_types::CertificateDer<'static>> = Vec::new();
        let fields = peer_cert_fields(Some(&bad));
        assert!(fields.cn.is_none());
    }

    /// Real leaf certificate yields its common name.
    /// The fixture is the test client certificate.
    /// The bind check uses this name for tenant rules.
    #[test]
    fn peer_cert_fields_pull_cn_from_real_cert() {
        let cert_pem = include_str!("../../broker-connectors-enterprise/testdata/client-cert.pem");
        let mut reader = std::io::BufReader::new(cert_pem.as_bytes());
        let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
            rustls_pemfile::certs(&mut reader)
                .expect("fixture reads")
                .into_iter()
                .map(rustls::pki_types::CertificateDer::from)
                .collect();
        assert!(!certs.is_empty());
        let fields = peer_cert_fields(Some(&certs));
        assert_eq!(fields.cn.as_deref(), Some("test-device"));
    }

    /// Unsubscribe on the native path stops delivery and keeps the
    /// socket. The wire bytes decode, the shared broker handler
    /// removes the router copy, and the reply encodes as UNSUBACK.
    /// Goes through the broker, not the store alone.
    #[tokio::test]
    async fn native_unsubscribe_stops_delivery_with_unsuback() {
        let shared = Shared::new();
        let conn_id = NATIVE_CONN_ID_BASE + 920_001;
        let (tx, mut rx, mut bound) = bind_client(&shared, conn_id, "native-unsub").await;
        let waker = Arc::new(tokio::sync::Notify::new());
        let sink = NativeSink::new();
        // Subscribe through the production frame handler.
        let sub = wire::Subscribe {
            packet_id: 1,
            subscription_id: 0,
            filters: vec![("nsub/data", 0)],
        };
        let meta = subscribe_in_meta("native-unsub", &sub, 4);
        let frame = BrokerFrame::new(
            OpCode::SubscribeIn,
            conn_id,
            2,
            Bytes::from(meta),
            Bytes::new(),
        )
        .expect("frame");
        handle_inbound_frame_with(frame, &shared, &tx, &sink, &mut bound, &waker, None)
            .await
            .expect("subscribe is handled");
        let suback = sink.take();
        assert_eq!(&suback[0..2], &[0x90, 0x03], "SUBACK for the client");
        // Unsubscribe through the wire decode and the shared handler.
        // Body: packet id 2 plus one filter.
        let mut body = vec![0x00, 0x02];
        body.extend_from_slice(&[0x00, 0x09]);
        body.extend_from_slice(b"nsub/data");
        let unsub = wire::decode_unsubscribe(&body, 4).expect("wire decodes");
        assert_eq!(unsub.packet_id, 2);
        let meta = unsubscribe_in_meta("native-unsub", &unsub);
        let frame = BrokerFrame::new(
            OpCode::UnsubscribeIn,
            conn_id,
            3,
            Bytes::from(meta),
            Bytes::new(),
        )
        .expect("frame");
        handle_inbound_frame_with(frame, &shared, &tx, &sink, &mut bound, &waker, None)
            .await
            .expect("unsubscribe is handled");
        let unsuback = sink.take();
        assert_eq!(
            unsuback,
            vec![0xB0, 0x02, 0x00, 0x02],
            "UNSUBACK for the client"
        );
        // No routed delivery remains for the filter.
        let _ = rx.try_recv();
        let topic = broker_protocol::Topic::new("nsub/data".to_string()).expect("topic");
        let matched = shared.router.matches(&topic);
        assert!(matched.is_empty(), "router drops the filter");
    }

    /// Server config loads with and without client verification.
    /// The fixture is the test server certificate and key.
    /// Verification on requests a client certificate.
    /// A set CA file loads. An empty CA path returns an error.
    /// A missing CA file returns an error.
    #[test]
    fn tls_config_loads_with_and_without_verify() {
        let base = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../broker-connectors-enterprise/testdata"
        );
        let cert = format!("{base}/server-cert.pem");
        let key = format!("{base}/server-key.pem");
        let off = TlsClientAuth::default();
        assert!(super::load_tls_config(&cert, &key, &off).is_ok());
        let on = TlsClientAuth {
            verify_peer: true,
            cacertfile: format!("{base}/ca-cert.pem"),
            ..TlsClientAuth::default()
        };
        assert!(super::load_tls_config(&cert, &key, &on).is_ok());
        let missing_ca = TlsClientAuth {
            verify_peer: true,
            cacertfile: "/nonexistent-ca.pem".to_string(),
            ..TlsClientAuth::default()
        };
        assert!(super::load_tls_config(&cert, &key, &missing_ca).is_err());
    }

    /// Builds a small MQTT 3.1.1 CONNECT packet.
    fn ws_test_connect(client_id: &str) -> Vec<u8> {
        let mut body = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3C];
        body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
        body.extend_from_slice(client_id.as_bytes());
        let mut packet = vec![0x10, body.len() as u8];
        packet.extend_from_slice(&body);
        packet
    }

    /// Builds a CONNECT packet with username and password.
    fn ws_test_connect_with_auth(client_id: &str, username: &str, password: &[u8]) -> Vec<u8> {
        let mut body = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0xC2, 0x00, 0x3C];
        body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
        body.extend_from_slice(client_id.as_bytes());
        body.extend_from_slice(&(username.len() as u16).to_be_bytes());
        body.extend_from_slice(username.as_bytes());
        body.extend_from_slice(&(password.len() as u16).to_be_bytes());
        body.extend_from_slice(password);
        let mut packet = vec![0x10, body.len() as u8];
        packet.extend_from_slice(&body);
        packet
    }

    /// Builds a small SUBSCRIBE packet for one filter.
    fn ws_test_subscribe(packet_id: u16, filter: &str) -> Vec<u8> {
        let mut body = packet_id.to_be_bytes().to_vec();
        body.extend_from_slice(&(filter.len() as u16).to_be_bytes());
        body.extend_from_slice(filter.as_bytes());
        body.push(0x00);
        let mut packet = vec![0x82, body.len() as u8];
        packet.extend_from_slice(&body);
        packet
    }

    /// Builds a small QoS 0 PUBLISH packet.
    fn ws_test_publish(topic: &str, payload: &[u8]) -> Vec<u8> {
        let mut body = (topic.len() as u16).to_be_bytes().to_vec();
        body.extend_from_slice(topic.as_bytes());
        body.extend_from_slice(payload);
        let mut packet = vec![0x30, body.len() as u8];
        packet.extend_from_slice(&body);
        packet
    }

    /// Starts one TCP and one WebSocket kernel listener on one [`Shared`].
    /// Returns the two ports. The tasks run until the test aborts them.
    async fn ws_test_listeners(shared: Shared) -> (u16, u16) {
        use tokio::net::TcpListener as Listener;
        let tcp = Listener::bind("127.0.0.1:0").await.expect("tcp binds");
        let ws = Listener::bind("127.0.0.1:0").await.expect("ws binds");
        let tcp_port = tcp.local_addr().expect("tcp addr").port();
        let ws_port = ws.local_addr().expect("ws addr").port();
        let tcp_shared = shared.clone();
        tokio::spawn(async move {
            serve_native_mqtt(tcp, Some(1000), Arc::new(AtomicU64::new(0)), tcp_shared).await;
        });
        let ws_shared = shared;
        tokio::spawn(async move {
            serve_native_ws(
                ws,
                "/mqtt".to_string(),
                Some(1000),
                Arc::new(AtomicU64::new(0)),
                ws_shared,
            )
            .await;
        });
        // The accept loops need a moment to start.
        tokio::time::sleep(Duration::from_millis(50)).await;
        (tcp_port, ws_port)
    }

    /// Connects a WebSocket client with the `mqtt` subprotocol.
    /// Returns the stream. The server must select `mqtt`.
    async fn ws_test_dial(
        ws_port: u16,
    ) -> tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>
    {
        use tokio_tungstenite::tungstenite::client::ClientRequestBuilder;
        let uri: http::Uri = format!("ws://127.0.0.1:{ws_port}/mqtt")
            .parse()
            .expect("uri parses");
        let builder = ClientRequestBuilder::new(uri).with_sub_protocol("mqtt");
        let (stream, response) = tokio_tungstenite::connect_async(builder)
            .await
            .expect("websocket dials");
        let selected = response
            .headers()
            .get("Sec-WebSocket-Protocol")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        assert_eq!(selected, "mqtt", "server selects mqtt");
        stream
    }

    /// Reads one binary MQTT packet from a WebSocket client.
    async fn ws_test_recv(
        stream: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> Vec<u8> {
        use futures::StreamExt as _;
        let message = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("a message in time")
            .expect("the stream stays open")
            .expect("a correct frame");
        match message {
            tokio_tungstenite::tungstenite::protocol::Message::Binary(data) => data,
            other => panic!("binary expected, got: {other:?}"),
        }
    }

    /// Reads one MQTT packet from a plaintext TCP client.
    async fn tcp_test_recv(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        use tokio::io::AsyncReadExt as _;
        let mut head = [0u8; 2];
        tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut head))
            .await
            .expect("a header in time")
            .expect("the socket stays open");
        let length = head[1] as usize;
        let mut packet = head.to_vec();
        if length > 0 {
            let mut body = vec![0u8; length];
            tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut body))
                .await
                .expect("a body in time")
                .expect("the socket stays open");
            packet.extend_from_slice(&body);
        }
        packet
    }

    /// A WebSocket client and a TCP client exchange publishes both ways.
    /// Goes through the broker, not the store alone.
    #[tokio::test]
    async fn ws_and_tcp_clients_exchange_publishes_both_ways() {
        use futures::SinkExt as _;
        use tokio::io::AsyncWriteExt as _;
        let shared = Shared::new();
        let (tcp_port, ws_port) = ws_test_listeners(shared).await;

        // A TCP subscriber waits on the topic.
        let mut tcp_sub = tokio::net::TcpStream::connect(("127.0.0.1", tcp_port))
            .await
            .expect("tcp dials");
        tcp_sub
            .write_all(&ws_test_connect("ws-tcp-sub"))
            .await
            .expect("tcp CONNECT sends");
        assert_eq!(
            tcp_test_recv(&mut tcp_sub).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "TCP CONNACK accepts"
        );
        tcp_sub
            .write_all(&ws_test_subscribe(1, "ws/share"))
            .await
            .expect("tcp SUBSCRIBE sends");
        let suback = tcp_test_recv(&mut tcp_sub).await;
        assert_eq!(suback[0], 0x90, "TCP SUBACK answers");

        // A WebSocket client publishes. TCP gets it.
        let mut ws_pub = ws_test_dial(ws_port).await;
        ws_pub
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_connect("ws-pub"),
            ))
            .await
            .expect("ws CONNECT sends");
        assert_eq!(
            ws_test_recv(&mut ws_pub).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "WS CONNACK accepts"
        );
        ws_pub
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_publish("ws/share", b"from-ws"),
            ))
            .await
            .expect("ws PUBLISH sends");
        let delivered = tcp_test_recv(&mut tcp_sub).await;
        assert!(delivered.ends_with(b"from-ws"), "TCP gets the WS publish");

        // The reverse: WS subscribes, TCP publishes, WS gets it.
        let mut ws_sub = ws_test_dial(ws_port).await;
        ws_sub
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_connect("ws-sub"),
            ))
            .await
            .expect("ws CONNECT sends");
        assert_eq!(
            ws_test_recv(&mut ws_sub).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "WS CONNACK accepts"
        );
        ws_sub
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_subscribe(1, "ws/back"),
            ))
            .await
            .expect("ws SUBSCRIBE sends");
        let ws_suback = ws_test_recv(&mut ws_sub).await;
        assert_eq!(ws_suback[0], 0x90, "WS SUBACK answers");
        let mut tcp_pub = tokio::net::TcpStream::connect(("127.0.0.1", tcp_port))
            .await
            .expect("tcp dials");
        tcp_pub
            .write_all(&ws_test_connect("ws-tcp-pub"))
            .await
            .expect("tcp CONNECT sends");
        assert_eq!(
            tcp_test_recv(&mut tcp_pub).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "TCP CONNACK accepts"
        );
        tcp_pub
            .write_all(&ws_test_publish("ws/back", b"from-tcp"))
            .await
            .expect("tcp PUBLISH sends");
        let back = ws_test_recv(&mut ws_sub).await;
        assert!(back.ends_with(b"from-tcp"), "WS gets the TCP publish");
        let _ = ws_pub.close(None).await;
        let _ = ws_sub.close(None).await;
    }

    /// One packet split over three frames and two packets in one frame
    /// decode. Goes through the broker.
    #[tokio::test]
    async fn ws_split_and_batched_packets_decode() {
        use futures::SinkExt as _;
        use tokio::io::AsyncWriteExt as _;
        let shared = Shared::new();
        let (tcp_port, ws_port) = ws_test_listeners(shared).await;

        // A TCP subscriber waits on two topics.
        let mut tcp_sub = tokio::net::TcpStream::connect(("127.0.0.1", tcp_port))
            .await
            .expect("tcp dials");
        tcp_sub
            .write_all(&ws_test_connect("ws-split-tcp"))
            .await
            .expect("tcp CONNECT sends");
        assert_eq!(
            tcp_test_recv(&mut tcp_sub).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "TCP CONNACK accepts"
        );
        tcp_sub
            .write_all(&ws_test_subscribe(1, "ws/split/#"))
            .await
            .expect("tcp SUBSCRIBE sends");
        assert_eq!(tcp_test_recv(&mut tcp_sub).await[0], 0x90);

        // A WebSocket client connects, then sends SUBSCRIBE in 3 parts.
        let mut ws = ws_test_dial(ws_port).await;
        ws.send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
            ws_test_connect("ws-split"),
        ))
        .await
        .expect("ws CONNECT sends");
        assert_eq!(
            ws_test_recv(&mut ws).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "WS CONNACK accepts"
        );
        let subscribe = ws_test_subscribe(7, "ws/split/a");
        let first = subscribe.len() / 3;
        let second = 2 * subscribe.len() / 3;
        for part in [
            &subscribe[..first],
            &subscribe[first..second],
            &subscribe[second..],
        ] {
            ws.send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                part.to_vec(),
            ))
            .await
            .expect("ws part sends");
        }
        let suback = ws_test_recv(&mut ws).await;
        assert_eq!(suback[0], 0x90, "split SUBSCRIBE gets SUBACK");

        // Two PUBLISH packets in one binary frame both deliver.
        let mut batched = ws_test_publish("ws/split/a", b"one");
        batched.extend_from_slice(&ws_test_publish("ws/split/a", b"two"));
        ws.send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
            batched,
        ))
        .await
        .expect("ws batch sends");
        let first_got = tcp_test_recv(&mut tcp_sub).await;
        assert!(first_got.ends_with(b"one"), "first publish delivers");
        let second_got = tcp_test_recv(&mut tcp_sub).await;
        assert!(second_got.ends_with(b"two"), "second publish delivers");
        let _ = ws.close(None).await;
    }

    /// Reads the status line of a raw HTTP upgrade answer.
    async fn ws_test_raw_status(port: u16, path: &str, subprotocol: Option<&str>) -> String {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("tcp dials");
        let mut request = format!(
            "GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n"
        );
        if let Some(protocol) = subprotocol {
            request.push_str(&format!("Sec-WebSocket-Protocol: {protocol}\r\n"));
        }
        request.push_str("\r\n");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("request sends");
        let mut answer = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut byte = [0u8; 1];
                stream.read_exact(&mut byte).await.expect("a byte");
                answer.extend_from_slice(&byte);
                if answer.ends_with(b"\r\n\r\n") || answer.len() > 512 {
                    break;
                }
                if answer.ends_with(b"\n") && answer.len() > 12 {
                    break;
                }
            }
        })
        .await;
        String::from_utf8_lossy(&answer)
            .lines()
            .next()
            .unwrap_or("")
            .to_string()
    }

    /// Bad upgrades are refused. Bad frames close only that connection.
    /// Goes through the broker. The broker stays up after each close.
    #[tokio::test]
    async fn ws_refusals_and_frame_errors_close_only_that_connection() {
        use futures::{SinkExt as _, StreamExt as _};
        let shared = Shared::new();
        let (_tcp_port, ws_port) = ws_test_listeners(shared).await;

        let no_protocol = ws_test_raw_status(ws_port, "/mqtt", None).await;
        assert!(
            no_protocol.contains("400"),
            "no subprotocol gets 400: {no_protocol}"
        );
        let wrong_path = ws_test_raw_status(ws_port, "/wrong", Some("mqtt")).await;
        assert!(
            wrong_path.contains("404"),
            "wrong path gets 404: {wrong_path}"
        );

        // A text frame closes the connection.
        let mut text_client = ws_test_dial(ws_port).await;
        text_client
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_connect("ws-text"),
            ))
            .await
            .expect("ws CONNECT sends");
        assert_eq!(
            ws_test_recv(&mut text_client).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "WS CONNACK accepts"
        );
        text_client
            .send(tokio_tungstenite::tungstenite::protocol::Message::Text(
                "hello".into(),
            ))
            .await
            .expect("text sends");
        let text_end = tokio::time::timeout(Duration::from_secs(5), text_client.next()).await;
        let text_closed = match text_end {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::protocol::Message::Close(_)))) => true,
            Ok(Some(Err(_))) => true,
            Ok(None) => true,
            other => panic!("text closes the connection, got: {other:?}"),
        };
        assert!(text_closed, "a text frame closes the connection");

        // A frame over the limit closes the connection.
        let mut big_client = ws_test_dial(ws_port).await;
        big_client
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_connect("ws-big"),
            ))
            .await
            .expect("ws CONNECT sends");
        assert_eq!(
            ws_test_recv(&mut big_client).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "WS CONNACK accepts"
        );
        big_client
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                vec![0u8; FALLBACK_MAX_PACKET_BYTES + 1],
            ))
            .await
            .expect("big frame sends");
        let big_end = tokio::time::timeout(Duration::from_secs(5), big_client.next()).await;
        let big_closed = match big_end {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::protocol::Message::Close(_)))) => true,
            Ok(Some(Err(_))) => true,
            Ok(None) => true,
            other => panic!("an oversize frame closes the connection, got: {other:?}"),
        };
        assert!(big_closed, "an oversize frame closes the connection");

        // The broker still serves a new client after each close.
        let mut fresh = ws_test_dial(ws_port).await;
        fresh
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_connect("ws-fresh"),
            ))
            .await
            .expect("ws CONNECT sends");
        assert_eq!(
            ws_test_recv(&mut fresh).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "the broker stays up"
        );
        let _ = fresh.close(None).await;
    }

    /// An upgrade head past 8 KiB is refused. A normal upgrade
    /// still passes on a fresh socket. Goes through the broker.
    #[tokio::test]
    async fn ws_oversized_upgrade_head_is_refused() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let shared = Shared::new();
        let (_tcp_port, ws_port) = ws_test_listeners(shared).await;
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", ws_port))
            .await
            .expect("tcp dials");
        let mut request = format!(
            "GET /mqtt HTTP/1.1\r\nHost: 127.0.0.1:{ws_port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: mqtt\r\nX-Pad: "
        );
        while request.len() < 9 * 1024 {
            request.push('A');
        }
        request.push_str("\r\n\r\n");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("request sends");
        let mut answer = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut answer)).await;
        let text = String::from_utf8_lossy(&answer);
        assert!(
            !text.contains("101"),
            "an oversized head gets no upgrade: {text}"
        );
        let normal = ws_test_raw_status(ws_port, "/mqtt", Some("mqtt")).await;
        assert!(
            normal.contains("101"),
            "a normal upgrade still passes: {normal}"
        );
    }

    /// One ping keeps the connection open. A ping flood with no
    /// MQTT byte closes only that connection. Goes through the broker.
    #[tokio::test]
    async fn ws_ping_flood_closes_only_that_connection() {
        use futures::{SinkExt as _, StreamExt as _};
        use tokio_tungstenite::tungstenite::protocol::Message;
        let shared = Shared::new();
        let (_tcp_port, ws_port) = ws_test_listeners(shared).await;
        // One ping gets a pong and the connection stays open.
        // The library may answer the ping itself as well, so skip
        // pongs until the CONNACK arrives.
        let mut calm = ws_test_dial(ws_port).await;
        calm.send(Message::Ping(b"one".to_vec()))
            .await
            .expect("ping sends");
        calm.send(Message::Binary(ws_test_connect("ws-calm")))
            .await
            .expect("ws CONNECT sends");
        let mut got_connack = false;
        for _ in 0..4 {
            let message = tokio::time::timeout(Duration::from_secs(5), calm.next())
                .await
                .expect("a reply in time")
                .expect("the stream stays open")
                .expect("a correct frame");
            match message {
                Message::Pong(_) => continue,
                Message::Binary(data) => {
                    assert_eq!(data, vec![0x20, 0x02, 0x00, 0x00], "WS CONNACK accepts");
                    got_connack = true;
                    break;
                }
                other => panic!("CONNACK expected, got: {other:?}"),
            }
        }
        assert!(got_connack, "one ping keeps the connection open");
        // A flood of pings with no MQTT byte closes the connection.
        // The library answers each ping itself too, so drain both
        // pongs per ping until the close arrives. A stall (no more
        // messages with the socket open) is not a close.
        let mut loud = ws_test_dial(ws_port).await;
        for _ in 0..(super::MAX_WS_PINGS_WITHOUT_BINARY + 8) {
            loud.send(Message::Ping(b"flood".to_vec()))
                .await
                .expect("ping sends");
        }
        let mut closed = false;
        for _ in 0..(2 * super::MAX_WS_PINGS_WITHOUT_BINARY + 16) {
            match tokio::time::timeout(Duration::from_secs(5), loud.next()).await {
                Ok(Some(Ok(Message::Pong(_)))) => continue,
                Ok(Some(Ok(Message::Close(_)))) | Ok(Some(Err(_))) | Ok(None) => {
                    closed = true;
                    break;
                }
                Ok(Some(Ok(other))) => panic!("close expected, got: {other:?}"),
                Err(_) => break,
            }
        }
        assert!(closed, "a ping flood closes the connection");
        let _ = calm.close(None).await;
    }

    /// Limits hold while MQTT still delivers through the broker.
    /// An oversized upgrade head gets no upgrade. A ping flood with
    /// no MQTT byte closes only that connection. Fresh clients then
    /// still subscribe and exchange a publish in both directions.
    /// Goes through the broker.
    #[tokio::test]
    async fn ws_limits_hold_while_mqtt_delivers_through_broker() {
        use futures::{SinkExt as _, StreamExt as _};
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        use tokio_tungstenite::tungstenite::protocol::Message;
        let shared = Shared::new();
        let (tcp_port, ws_port) = ws_test_listeners(shared).await;
        // An oversized head gets no upgrade.
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", ws_port))
            .await
            .expect("tcp dials");
        let mut request = format!(
            "GET /mqtt HTTP/1.1\r\nHost: 127.0.0.1:{ws_port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: mqtt\r\nX-Pad: "
        );
        while request.len() < 9 * 1024 {
            request.push('A');
        }
        request.push_str("\r\n\r\n");
        stream
            .write_all(request.as_bytes())
            .await
            .expect("request sends");
        let mut answer = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut answer)).await;
        assert!(
            !String::from_utf8_lossy(&answer).contains("101"),
            "an oversized head gets no upgrade"
        );
        // A ping flood with no MQTT byte closes only that connection.
        let mut loud = ws_test_dial(ws_port).await;
        for _ in 0..(super::MAX_WS_PINGS_WITHOUT_BINARY + 8) {
            loud.send(Message::Ping(b"flood".to_vec()))
                .await
                .expect("ping sends");
        }
        let mut closed = false;
        for _ in 0..(2 * super::MAX_WS_PINGS_WITHOUT_BINARY + 16) {
            match tokio::time::timeout(Duration::from_secs(5), loud.next()).await {
                Ok(Some(Ok(Message::Pong(_)))) => continue,
                Ok(Some(Ok(Message::Close(_)))) | Ok(Some(Err(_))) | Ok(None) => {
                    closed = true;
                    break;
                }
                Ok(Some(Ok(other))) => panic!("close expected, got: {other:?}"),
                Err(_) => break,
            }
        }
        assert!(closed, "a ping flood closes the connection");
        // Fresh clients still exchange publishes both ways.
        let mut tcp_sub = tokio::net::TcpStream::connect(("127.0.0.1", tcp_port))
            .await
            .expect("tcp dials");
        tcp_sub
            .write_all(&ws_test_connect("ws-lim-tcp-sub"))
            .await
            .expect("tcp CONNECT sends");
        assert_eq!(
            tcp_test_recv(&mut tcp_sub).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "TCP CONNACK accepts"
        );
        tcp_sub
            .write_all(&ws_test_subscribe(1, "ws/limits/check"))
            .await
            .expect("tcp SUBSCRIBE sends");
        assert_eq!(
            tcp_test_recv(&mut tcp_sub).await[0],
            0x90,
            "TCP SUBACK answers"
        );
        let mut ws = ws_test_dial(ws_port).await;
        ws.send(Message::Binary(ws_test_connect("ws-lim-sub")))
            .await
            .expect("ws CONNECT sends");
        assert_eq!(
            ws_test_recv(&mut ws).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "WS CONNACK accepts"
        );
        ws.send(Message::Binary(ws_test_subscribe(1, "ws/limits/check")))
            .await
            .expect("ws SUBSCRIBE sends");
        assert_eq!(ws_test_recv(&mut ws).await[0], 0x90, "WS SUBACK answers");
        ws.send(Message::Binary(ws_test_publish(
            "ws/limits/check",
            b"from-ws",
        )))
        .await
        .expect("ws PUBLISH sends");
        assert!(
            tcp_test_recv(&mut tcp_sub).await.ends_with(b"from-ws"),
            "TCP gets the WS publish"
        );
        // The broker also delivers the publish to the subscriber
        // that sent it, so drain the own echo before the next
        // publish. Else the next read returns the echo.
        assert!(
            ws_test_recv(&mut ws).await.ends_with(b"from-ws"),
            "WS gets its own publish"
        );
        let mut tcp_pub = tokio::net::TcpStream::connect(("127.0.0.1", tcp_port))
            .await
            .expect("tcp dials");
        tcp_pub
            .write_all(&ws_test_connect("ws-lim-tcp-pub"))
            .await
            .expect("tcp CONNECT sends");
        assert_eq!(
            tcp_test_recv(&mut tcp_pub).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "TCP CONNACK accepts"
        );
        tcp_pub
            .write_all(&ws_test_publish("ws/limits/check", b"from-tcp"))
            .await
            .expect("tcp PUBLISH sends");
        assert!(
            ws_test_recv(&mut ws).await.ends_with(b"from-tcp"),
            "WS gets the TCP publish"
        );
        let _ = ws.close(None).await;
    }

    /// A second pong never queues behind the first. Two pings while
    /// the transport cannot send leave one pong only. Unit test of
    /// the bound. The next test proves pongs through the broker.
    #[test]
    fn second_pending_pong_replaces_the_first() {
        use std::collections::VecDeque;
        use std::task::{Context, Poll};
        use tokio::io::{AsyncRead as _, ReadBuf};
        use tokio_tungstenite::tungstenite::protocol::Message;

        /// A transport that never sends. Reads stay queued and the
        /// sink never turns ready, so pongs park in the transport.
        struct Parked {
            inbound: VecDeque<Message>,
        }
        impl futures::Stream for Parked {
            type Item = Result<Message, tokio_tungstenite::tungstenite::Error>;
            fn poll_next(
                mut self: std::pin::Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                match self.inbound.pop_front() {
                    Some(message) => Poll::Ready(Some(Ok(message))),
                    None => Poll::Pending,
                }
            }
        }
        impl futures::Sink<Message> for Parked {
            type Error = tokio_tungstenite::tungstenite::Error;
            fn poll_ready(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Result<(), Self::Error>> {
                Poll::Pending
            }
            fn start_send(
                self: std::pin::Pin<&mut Self>,
                _item: Message,
            ) -> Result<(), Self::Error> {
                unreachable!("the sink never turns ready");
            }
            fn poll_flush(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Result<(), Self::Error>> {
                Poll::Pending
            }
            fn poll_close(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Result<(), Self::Error>> {
                Poll::Pending
            }
        }

        let inner = Parked {
            inbound: VecDeque::from([
                Message::Ping(b"one".to_vec()),
                Message::Ping(b"two".to_vec()),
            ]),
        };
        let mut io = super::WsIo::new(inner, super::FALLBACK_MAX_PACKET_BYTES);
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut buf = [0u8; 64];
        let mut read_buf = ReadBuf::new(&mut buf);
        let polled = std::pin::Pin::new(&mut io).poll_read(&mut cx, &mut read_buf);
        assert!(matches!(polled, Poll::Pending), "no MQTT byte is ready");
        assert_eq!(
            io.pending_pong,
            Some(b"two".to_vec()),
            "the second pong replaces the first"
        );
    }

    /// Two rapid pings get pongs through the broker. The connection
    /// stays open and the CONNECT still gets a CONNACK. Goes through
    /// the broker.
    #[tokio::test]
    async fn two_rapid_pings_get_pongs_through_broker() {
        use futures::{SinkExt as _, StreamExt as _};
        use tokio_tungstenite::tungstenite::protocol::Message;
        let shared = Shared::new();
        let (_tcp_port, ws_port) = ws_test_listeners(shared).await;
        let mut ws = ws_test_dial(ws_port).await;
        ws.send(Message::Ping(b"one".to_vec()))
            .await
            .expect("first ping sends");
        ws.send(Message::Ping(b"two".to_vec()))
            .await
            .expect("second ping sends");
        ws.send(Message::Binary(ws_test_connect("ws-two-ping")))
            .await
            .expect("ws CONNECT sends");
        let mut got_connack = false;
        for _ in 0..8 {
            let message = tokio::time::timeout(Duration::from_secs(5), ws.next())
                .await
                .expect("a reply in time")
                .expect("the stream stays open")
                .expect("a correct frame");
            match message {
                Message::Pong(_) => continue,
                Message::Binary(data) => {
                    assert_eq!(data, vec![0x20, 0x02, 0x00, 0x00], "WS CONNACK accepts");
                    got_connack = true;
                    break;
                }
                other => panic!("CONNACK expected, got: {other:?}"),
            }
        }
        assert!(got_connack, "two pings keep the connection open");
        let _ = ws.close(None).await;
    }

    /// Two tenants stay apart on the WebSocket path. Goes through the
    /// broker. Each publish reaches its own tenant only.
    #[tokio::test]
    async fn ws_two_tenants_stay_apart() {
        use futures::SinkExt as _;
        let shared = Shared::new();
        shared
            .auth
            .set_policy(broker_auth::PasswordHashPolicy::for_tests());
        shared
            .tenant_registry
            .replace(vec![broker_session::tenant::TenantRule::new(
                "",
                "tenant",
                "t-${client_id}",
            )])
            .expect("tenant rule installs");
        for (tenant, user, password) in [
            ("t-ws-a", "alice", b"pw-a".as_slice()),
            ("t-ws-b", "bob", b"pw-b".as_slice()),
        ] {
            shared
                .auth
                .add_user_in_tenant(tenant, user, password)
                .expect("tenant user stores");
            for action in [
                broker_auth::AclAction::Publish,
                broker_auth::AclAction::Subscribe,
            ] {
                shared
                    .auth
                    .add_rule_in_tenant(
                        tenant,
                        broker_auth::AclRule::new("*", action, "ws/tenant/#", true),
                    )
                    .expect("tenant rule stores");
            }
        }
        let (_tcp_port, ws_port) = ws_test_listeners(shared).await;

        // Each tenant connects with its own client id and credentials.
        let mut client_a = ws_test_dial(ws_port).await;
        client_a
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_connect_with_auth("ws-a", "alice", b"pw-a"),
            ))
            .await
            .expect("ws CONNECT sends");
        assert_eq!(
            ws_test_recv(&mut client_a).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "first tenant CONNECT accepts"
        );
        client_a
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_subscribe(1, "ws/tenant/x"),
            ))
            .await
            .expect("ws SUBSCRIBE sends");
        assert_eq!(ws_test_recv(&mut client_a).await[0], 0x90);

        let mut client_b = ws_test_dial(ws_port).await;
        client_b
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_connect_with_auth("ws-b", "bob", b"pw-b"),
            ))
            .await
            .expect("ws CONNECT sends");
        assert_eq!(
            ws_test_recv(&mut client_b).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "second tenant CONNECT accepts"
        );
        client_b
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_subscribe(1, "ws/tenant/x"),
            ))
            .await
            .expect("ws SUBSCRIBE sends");
        assert_eq!(ws_test_recv(&mut client_b).await[0], 0x90);

        // A publish in the first tenant reaches its own subscriber only.
        client_a
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_publish("ws/tenant/x", b"secret-a"),
            ))
            .await
            .expect("ws PUBLISH sends");
        let got_a = ws_test_recv(&mut client_a).await;
        assert!(got_a.ends_with(b"secret-a"), "own tenant delivers");
        let other_a =
            tokio::time::timeout(Duration::from_millis(300), ws_test_recv(&mut client_b)).await;
        assert!(other_a.is_err(), "other tenant gets nothing");

        // A publish in the second tenant reaches its own subscriber only.
        client_b
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_publish("ws/tenant/x", b"secret-b"),
            ))
            .await
            .expect("ws PUBLISH sends");
        let got_b = ws_test_recv(&mut client_b).await;
        assert!(got_b.ends_with(b"secret-b"), "own tenant delivers");
        let other_b =
            tokio::time::timeout(Duration::from_millis(300), ws_test_recv(&mut client_a)).await;
        assert!(other_b.is_err(), "other tenant gets nothing");
        let _ = client_a.close(None).await;
        let _ = client_b.close(None).await;
    }

    /// Opens a TLS session to the secure WebSocket port.
    /// Trusts the test CA only. Sends no HTTP yet.
    async fn wss_test_tls(port: u16) -> tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
        let base = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../broker-connectors-enterprise/testdata"
        );
        let ca_pem = std::fs::read(format!("{base}/ca-cert.pem")).expect("test CA reads");
        let mut reader = std::io::BufReader::new(ca_pem.as_slice());
        let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
            rustls_pemfile::certs(&mut reader)
                .expect("test CA parses")
                .into_iter()
                .map(rustls::pki_types::CertificateDer::from)
                .collect();
        let mut roots = rustls::RootCertStore::empty();
        for cert in certs {
            roots.add(cert).expect("test CA trusts");
        }
        let provider = std::sync::Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let client = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("versions hold")
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(client));
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("tcp dials");
        let name =
            rustls::pki_types::ServerName::try_from("127.0.0.1".to_string()).expect("name holds");
        connector.connect(name, tcp).await.expect("TLS opens")
    }

    /// Starts one TCP and one secure WebSocket kernel listener.
    /// Returns the two ports. The tasks run until the test aborts them.
    async fn wss_test_listeners(shared: Shared) -> (u16, u16) {
        use tokio::net::TcpListener as Listener;
        let base = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../broker-connectors-enterprise/testdata"
        );
        let cert = format!("{base}/server-cert.pem");
        let key = format!("{base}/server-key.pem");
        let tls = super::load_tls_config(&cert, &key, &super::TlsClientAuth::default())
            .expect("test TLS loads");
        let tcp = Listener::bind("127.0.0.1:0").await.expect("tcp binds");
        let wss = Listener::bind("127.0.0.1:0").await.expect("wss binds");
        let tcp_port = tcp.local_addr().expect("tcp addr").port();
        let wss_port = wss.local_addr().expect("wss addr").port();
        let tcp_shared = shared.clone();
        tokio::spawn(async move {
            serve_native_mqtt(tcp, Some(1000), Arc::new(AtomicU64::new(0)), tcp_shared).await;
        });
        let wss_shared = shared;
        tokio::spawn(async move {
            serve_native_wss(
                wss,
                tls,
                "/mqtt".to_string(),
                Some(1000),
                Arc::new(AtomicU64::new(0)),
                wss_shared,
            )
            .await;
        });
        // The accept loops need a moment to start.
        tokio::time::sleep(Duration::from_millis(50)).await;
        (tcp_port, wss_port)
    }

    /// Connects a secure WebSocket client with the `mqtt` subprotocol.
    /// Returns the stream. The server must select `mqtt`.
    async fn wss_test_dial(
        wss_port: u16,
    ) -> tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>
    {
        use tokio_tungstenite::tungstenite::client::ClientRequestBuilder;
        let tls = wss_test_tls(wss_port).await;
        let uri: http::Uri = format!("wss://127.0.0.1:{wss_port}/mqtt")
            .parse()
            .expect("uri parses");
        let builder = ClientRequestBuilder::new(uri).with_sub_protocol("mqtt");
        let (stream, response) = tokio_tungstenite::client_async_with_config(builder, tls, None)
            .await
            .expect("websocket dials");
        let selected = response
            .headers()
            .get("Sec-WebSocket-Protocol")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        assert_eq!(selected, "mqtt", "server selects mqtt");
        stream
    }

    /// Reads one binary MQTT packet from a secure WebSocket client.
    async fn wss_test_recv(
        stream: &mut tokio_tungstenite::WebSocketStream<
            tokio_rustls::client::TlsStream<tokio::net::TcpStream>,
        >,
    ) -> Vec<u8> {
        use futures::StreamExt as _;
        let message = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("a message in time")
            .expect("the stream stays open")
            .expect("a correct frame");
        match message {
            tokio_tungstenite::tungstenite::protocol::Message::Binary(data) => data,
            other => panic!("binary expected, got: {other:?}"),
        }
    }

    /// Reads the status line of a raw HTTPS upgrade answer.
    async fn wss_test_raw_status(port: u16, path: &str, subprotocol: Option<&str>) -> String {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let mut tls = wss_test_tls(port).await;
        let mut request = format!(
            "GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n"
        );
        if let Some(protocol) = subprotocol {
            request.push_str(&format!("Sec-WebSocket-Protocol: {protocol}\r\n"));
        }
        request.push_str("\r\n");
        tls.write_all(request.as_bytes())
            .await
            .expect("request sends");
        let mut answer = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut byte = [0u8; 1];
                tls.read_exact(&mut byte).await.expect("a byte");
                answer.extend_from_slice(&byte);
                if answer.ends_with(b"\r\n\r\n") || answer.len() > 512 {
                    break;
                }
                if answer.ends_with(b"\n") && answer.len() > 12 {
                    break;
                }
            }
        })
        .await;
        String::from_utf8_lossy(&answer)
            .lines()
            .next()
            .unwrap_or("")
            .to_string()
    }

    /// A secure WebSocket client and a TCP client exchange publishes.
    /// Goes through the broker, not the store alone.
    #[tokio::test]
    async fn wss_and_tcp_clients_exchange_publishes_both_ways() {
        use futures::SinkExt as _;
        use tokio::io::AsyncWriteExt as _;
        let shared = Shared::new();
        let (tcp_port, wss_port) = wss_test_listeners(shared).await;

        // A TCP subscriber waits on the topic.
        let mut tcp_sub = tokio::net::TcpStream::connect(("127.0.0.1", tcp_port))
            .await
            .expect("tcp dials");
        tcp_sub
            .write_all(&ws_test_connect("wss-tcp-sub"))
            .await
            .expect("tcp CONNECT sends");
        assert_eq!(
            tcp_test_recv(&mut tcp_sub).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "TCP CONNACK accepts"
        );
        tcp_sub
            .write_all(&ws_test_subscribe(1, "wss/share"))
            .await
            .expect("tcp SUBSCRIBE sends");
        let suback = tcp_test_recv(&mut tcp_sub).await;
        assert_eq!(suback[0], 0x90, "TCP SUBACK answers");

        // A secure WebSocket client publishes. TCP gets it.
        let mut wss_pub = wss_test_dial(wss_port).await;
        wss_pub
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_connect("wss-pub"),
            ))
            .await
            .expect("wss CONNECT sends");
        assert_eq!(
            wss_test_recv(&mut wss_pub).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "WSS CONNACK accepts"
        );
        wss_pub
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_publish("wss/share", b"from-wss"),
            ))
            .await
            .expect("wss PUBLISH sends");
        let delivered = tcp_test_recv(&mut tcp_sub).await;
        assert!(delivered.ends_with(b"from-wss"), "TCP gets the WSS publish");

        // The reverse: WSS subscribes, TCP publishes, WSS gets it.
        let mut wss_sub = wss_test_dial(wss_port).await;
        wss_sub
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_connect("wss-sub"),
            ))
            .await
            .expect("wss CONNECT sends");
        assert_eq!(
            wss_test_recv(&mut wss_sub).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "WSS CONNACK accepts"
        );
        wss_sub
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_subscribe(1, "wss/back"),
            ))
            .await
            .expect("wss SUBSCRIBE sends");
        let wss_suback = wss_test_recv(&mut wss_sub).await;
        assert_eq!(wss_suback[0], 0x90, "WSS SUBACK answers");
        let mut tcp_pub = tokio::net::TcpStream::connect(("127.0.0.1", tcp_port))
            .await
            .expect("tcp dials");
        tcp_pub
            .write_all(&ws_test_connect("wss-tcp-pub"))
            .await
            .expect("tcp CONNECT sends");
        assert_eq!(
            tcp_test_recv(&mut tcp_pub).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "TCP CONNACK accepts"
        );
        tcp_pub
            .write_all(&ws_test_publish("wss/back", b"from-tcp"))
            .await
            .expect("tcp PUBLISH sends");
        let back = wss_test_recv(&mut wss_sub).await;
        assert!(back.ends_with(b"from-tcp"), "WSS gets the TCP publish");
        let _ = wss_pub.close(None).await;
        let _ = wss_sub.close(None).await;
    }

    /// Bad upgrades and plain clients are refused on the secure port.
    /// Goes through the broker. The broker stays up after each close.
    #[tokio::test]
    async fn wss_refusals_close_only_that_connection() {
        use futures::SinkExt as _;
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let shared = Shared::new();
        let (_tcp_port, wss_port) = wss_test_listeners(shared).await;

        let no_protocol = wss_test_raw_status(wss_port, "/mqtt", None).await;
        assert!(
            no_protocol.contains("400"),
            "no subprotocol gets 400: {no_protocol}"
        );
        let wrong_path = wss_test_raw_status(wss_port, "/wrong", Some("mqtt")).await;
        assert!(
            wrong_path.contains("404"),
            "wrong path gets 404: {wrong_path}"
        );

        // A client without TLS gets no upgrade answer.
        let mut plain = tokio::net::TcpStream::connect(("127.0.0.1", wss_port))
            .await
            .expect("tcp dials");
        plain
            .write_all(
                b"GET /mqtt HTTP/1.1\r\nHost: smoke\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: mqtt\r\n\r\n",
            )
            .await
            .expect("request sends");
        let mut head = [0u8; 12];
        let answer =
            tokio::time::timeout(Duration::from_secs(5), plain.read_exact(&mut head)).await;
        if let Ok(Ok(_)) = answer {
            assert!(
                !head.starts_with(b"HTTP/1.1 101"),
                "plain client gets no upgrade"
            );
        }

        // The broker still serves a secure client after each refusal.
        let mut fresh = wss_test_dial(wss_port).await;
        fresh
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_connect("wss-fresh"),
            ))
            .await
            .expect("wss CONNECT sends");
        assert_eq!(
            wss_test_recv(&mut fresh).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "the broker stays up"
        );
        let _ = fresh.close(None).await;
    }

    /// A TLS handshake that sends nothing ends in the time limit.
    /// Goes through the broker. The broker stays up after the close.
    #[tokio::test]
    async fn wss_idle_tls_handshake_times_out() {
        use futures::SinkExt as _;
        use tokio::io::AsyncReadExt as _;
        let shared = Shared::new();
        let (_tcp_port, wss_port) = wss_test_listeners(shared).await;

        // A client opens the socket and sends nothing.
        let mut idle = tokio::net::TcpStream::connect(("127.0.0.1", wss_port))
            .await
            .expect("tcp dials");
        let mut byte = [0u8; 1];
        let end = tokio::time::timeout(Duration::from_secs(15), idle.read_exact(&mut byte)).await;
        assert!(end.is_ok(), "an idle handshake ends in the limit");

        // The broker still serves a secure client after the close.
        let mut fresh = wss_test_dial(wss_port).await;
        fresh
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_connect("wss-after-idle"),
            ))
            .await
            .expect("wss CONNECT sends");
        assert_eq!(
            wss_test_recv(&mut fresh).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "the broker stays up"
        );
        let _ = fresh.close(None).await;
    }

    /// Two tenants stay apart on the secure WebSocket path.
    /// Goes through the broker. Each publish reaches its own tenant only.
    #[tokio::test]
    async fn wss_two_tenants_stay_apart() {
        use futures::SinkExt as _;
        let shared = Shared::new();
        shared
            .auth
            .set_policy(broker_auth::PasswordHashPolicy::for_tests());
        shared
            .tenant_registry
            .replace(vec![broker_session::tenant::TenantRule::new(
                "",
                "tenant",
                "t-${client_id}",
            )])
            .expect("tenant rule installs");
        for (tenant, user, password) in [
            ("t-wss-a", "alice", b"pw-a".as_slice()),
            ("t-wss-b", "bob", b"pw-b".as_slice()),
        ] {
            shared
                .auth
                .add_user_in_tenant(tenant, user, password)
                .expect("tenant user stores");
            for action in [
                broker_auth::AclAction::Publish,
                broker_auth::AclAction::Subscribe,
            ] {
                shared
                    .auth
                    .add_rule_in_tenant(
                        tenant,
                        broker_auth::AclRule::new("*", action, "wss/tenant/#", true),
                    )
                    .expect("tenant rule stores");
            }
        }
        let (_tcp_port, wss_port) = wss_test_listeners(shared).await;

        // Each tenant connects with its own client id and credentials.
        let mut client_a = wss_test_dial(wss_port).await;
        client_a
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_connect_with_auth("wss-a", "alice", b"pw-a"),
            ))
            .await
            .expect("wss CONNECT sends");
        assert_eq!(
            wss_test_recv(&mut client_a).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "first tenant CONNECT accepts"
        );
        client_a
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_subscribe(1, "wss/tenant/x"),
            ))
            .await
            .expect("wss SUBSCRIBE sends");
        assert_eq!(wss_test_recv(&mut client_a).await[0], 0x90);

        let mut client_b = wss_test_dial(wss_port).await;
        client_b
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_connect_with_auth("wss-b", "bob", b"pw-b"),
            ))
            .await
            .expect("wss CONNECT sends");
        assert_eq!(
            wss_test_recv(&mut client_b).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "second tenant CONNECT accepts"
        );
        client_b
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_subscribe(1, "wss/tenant/x"),
            ))
            .await
            .expect("wss SUBSCRIBE sends");
        assert_eq!(wss_test_recv(&mut client_b).await[0], 0x90);

        // A publish in the first tenant reaches its own subscriber only.
        client_a
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_publish("wss/tenant/x", b"secret-a"),
            ))
            .await
            .expect("wss PUBLISH sends");
        let got_a = wss_test_recv(&mut client_a).await;
        assert!(got_a.ends_with(b"secret-a"), "own tenant delivers");
        let other_a =
            tokio::time::timeout(Duration::from_millis(300), wss_test_recv(&mut client_b)).await;
        assert!(other_a.is_err(), "other tenant gets nothing");

        // A publish in the second tenant reaches its own subscriber only.
        client_b
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_publish("wss/tenant/x", b"secret-b"),
            ))
            .await
            .expect("wss PUBLISH sends");
        let got_b = wss_test_recv(&mut client_b).await;
        assert!(got_b.ends_with(b"secret-b"), "own tenant delivers");
        let other_b =
            tokio::time::timeout(Duration::from_millis(300), wss_test_recv(&mut client_a)).await;
        assert!(other_b.is_err(), "other tenant gets nothing");
        let _ = client_a.close(None).await;
        let _ = client_b.close(None).await;
    }

    /// Path of one K1-05 test PKI file.
    /// The files live under `crates/broker-node/tests/k1_05`.
    /// OpenSSL commands generate them: one CA, one server certificate
    /// for 127.0.0.1, two client certificates, one other CA with its
    /// own client certificate, one expired client certificate.
    /// Each client certificate is version 3 with clientAuth use.
    /// The sign step uses an extension file with clientAuth use.
    /// No new crate was added for them: `rcgen` is not in the lock file.
    fn k1_05_file(name: &str) -> String {
        format!("{}/tests/k1_05/{name}", env!("CARGO_MANIFEST_DIR"))
    }

    /// Trust roots of the K1-05 test CA.
    fn k1_05_roots() -> rustls::RootCertStore {
        let pem = std::fs::read(k1_05_file("ca-cert.pem")).expect("test CA reads");
        let mut reader = std::io::BufReader::new(pem.as_slice());
        let certs = rustls_pemfile::certs(&mut reader).expect("test CA parses");
        let mut roots = rustls::RootCertStore::empty();
        for der in certs {
            roots
                .add(rustls::pki_types::CertificateDer::from(der))
                .expect("test CA trusts");
        }
        roots
    }

    /// Private key in a PEM file. Accepts PKCS#8, RSA and SEC1 forms.
    fn k1_05_key(path: &str) -> rustls::pki_types::PrivateKeyDer<'static> {
        use rustls::pki_types::PrivateKeyDer;
        let bytes = std::fs::read(path).expect("test key reads");
        for item in rustls_pemfile::read_all(&mut bytes.as_slice()).expect("test key parses") {
            match item {
                rustls_pemfile::Item::PKCS8Key(der) => return PrivateKeyDer::Pkcs8(der.into()),
                rustls_pemfile::Item::RSAKey(der) => return PrivateKeyDer::Pkcs1(der.into()),
                rustls_pemfile::Item::ECKey(der) => return PrivateKeyDer::Sec1(der.into()),
                _ => continue,
            }
        }
        panic!("no private key in test file");
    }

    /// Certificate chain in a PEM file.
    fn k1_05_chain(path: &str) -> Vec<rustls::pki_types::CertificateDer<'static>> {
        let pem = std::fs::read(path).expect("test chain reads");
        let mut reader = std::io::BufReader::new(pem.as_slice());
        rustls_pemfile::certs(&mut reader)
            .expect("test chain parses")
            .into_iter()
            .map(rustls::pki_types::CertificateDer::from)
            .collect()
    }

    /// TLS client config for the K1-05 tests. Trusts the test CA only.
    /// `client` selects the client certificate, if any.
    fn k1_05_client_config(client: Option<&str>) -> std::sync::Arc<rustls::ClientConfig> {
        let provider = std::sync::Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        let base = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("versions hold")
            .with_root_certificates(k1_05_roots());
        let config = match client {
            Some(name) => base
                .with_client_auth_cert(
                    k1_05_chain(&k1_05_file(&format!("{name}-cert.pem"))),
                    k1_05_key(&k1_05_file(&format!("{name}-key.pem"))),
                )
                .expect("client identity loads"),
            None => base.with_no_client_auth(),
        };
        std::sync::Arc::new(config)
    }

    /// Opens a TLS session to the test port. `client` selects the
    /// client certificate, if any. Returns the handshake result.
    async fn k1_05_dial(
        port: u16,
        client: Option<&str>,
    ) -> Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>, std::io::Error> {
        let connector = tokio_rustls::TlsConnector::from(k1_05_client_config(client));
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("tcp dials");
        let name =
            rustls::pki_types::ServerName::try_from("127.0.0.1".to_string()).expect("name holds");
        tokio::time::timeout(Duration::from_secs(10), connector.connect(name, tcp))
            .await
            .expect("handshake ends in time")
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::ConnectionAborted, e))
    }

    /// Starts one kernel TLS listener with the K1-05 test server
    /// certificate and the given client policy. Returns the port.
    /// The task runs until the test aborts it.
    /// The listener copies the policy into the shared bind flags.
    /// The bind then refuses a missing certificate when required.
    async fn k1_05_listener(mut shared: Shared, policy: super::TlsClientAuth) -> u16 {
        shared.tls_verify_peer = policy.verify_peer;
        shared.tls_verify_client_cert = policy.legacy_verify;
        shared.tls_fail_if_no_peer_cert = policy.fail_if_no_peer_cert;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("tls binds");
        let port = listener.local_addr().expect("tls addr").port();
        let config = super::load_tls_config(
            &k1_05_file("server-cert.pem"),
            &k1_05_file("server-key.pem"),
            &policy,
        )
        .expect("test TLS loads");
        tokio::spawn(async move {
            serve_native_mqtts(
                listener,
                config,
                Some(1000),
                Arc::new(AtomicU64::new(0)),
                shared,
            )
            .await;
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        port
    }

    /// Server policy for the K1-05 tests. Trusts the test CA file.
    /// `require` selects the require mode.
    fn k1_05_policy(require: bool) -> super::TlsClientAuth {
        super::TlsClientAuth {
            verify_peer: true,
            legacy_verify: false,
            cacertfile: k1_05_file("ca-cert.pem"),
            fail_if_no_peer_cert: require,
        }
    }

    /// Reads one MQTT packet from a TLS client.
    async fn k1_05_recv<S>(stream: &mut S) -> Vec<u8>
    where
        S: tokio::io::AsyncRead + Unpin,
    {
        use tokio::io::AsyncReadExt as _;
        let mut head = [0u8; 2];
        tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut head))
            .await
            .expect("a header in time")
            .expect("the socket stays open");
        let length = head[1] as usize;
        let mut packet = head.to_vec();
        if length > 0 {
            let mut body = vec![0u8; length];
            tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut body))
                .await
                .expect("a body in time")
                .expect("the socket stays open");
            packet.extend_from_slice(&body);
        }
        packet
    }

    /// In require mode a client without a certificate fails the
    /// handshake. Before the change it connected. Goes through
    /// the broker. The broker stays up after the failed handshake.
    /// TLS 1.3 reports the refusal after the client flight.
    /// The test accepts a dial error or a refused CONNECT.
    #[tokio::test]
    async fn k1_05_require_mode_refuses_client_without_certificate() {
        use tokio::io::AsyncWriteExt as _;
        let shared = Shared::new();
        let port = k1_05_listener(shared, k1_05_policy(true)).await;

        // No certificate: the handshake fails or the bind refuses.
        match k1_05_dial(port, None).await {
            Err(_) => {}
            Ok(mut stream) => {
                let _ = stream.write_all(&ws_test_connect("k1-05-no-cert")).await;
                use tokio::io::AsyncReadExt as _;
                let mut head = [0u8; 4];
                let denied =
                    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut head))
                        .await;
                match denied {
                    Err(_) => {}
                    Ok(Err(_)) => {}
                    Ok(Ok(_)) => {
                        assert_ne!(
                            head,
                            [0x20, 0x02, 0x00, 0x00],
                            "no certificate must not connect"
                        );
                    }
                }
            }
        }

        // A valid certificate still connects. The broker stays up.
        let mut valid = k1_05_dial(port, Some("client-a"))
            .await
            .expect("valid dials");
        valid
            .write_all(&ws_test_connect("k1-05-valid"))
            .await
            .expect("CONNECT sends");
        assert_eq!(
            k1_05_recv(&mut valid).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "valid certificate connects"
        );
    }

    /// A client with a valid certificate connects. With the common
    /// name as username the session username is the common name.
    /// Goes through the broker, not the store alone.
    #[tokio::test]
    async fn k1_05_valid_certificate_maps_common_name_to_username() {
        use tokio::io::AsyncWriteExt as _;
        let mut shared = Shared::new();
        shared.tls_peer_cert_as_username = "cn".to_string();
        let port = k1_05_listener(shared.clone(), k1_05_policy(false)).await;

        let mut client = k1_05_dial(port, Some("client-a"))
            .await
            .expect("valid dials");
        client
            .write_all(&ws_test_connect("k1-05-cn-user"))
            .await
            .expect("CONNECT sends");
        assert_eq!(
            k1_05_recv(&mut client).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "valid certificate connects"
        );
        let session = shared
            .sessions
            .get("k1-05-cn-user")
            .expect("session exists");
        assert_eq!(
            session.username.read().as_deref(),
            Some("k1-05-a"),
            "session username is the common name"
        );
    }

    /// With the common name as client identifier the session uses
    /// the common name. A publish through the broker still delivers.
    /// Goes through the broker, not the store alone.
    #[tokio::test]
    async fn k1_05_certificate_client_id_carries_publish() {
        use tokio::io::AsyncWriteExt as _;
        let mut shared = Shared::new();
        shared.tls_peer_cert_as_clientid = "cn".to_string();
        let port = k1_05_listener(shared.clone(), k1_05_policy(false)).await;

        let mut client = k1_05_dial(port, Some("client-a"))
            .await
            .expect("valid dials");
        client
            .write_all(&ws_test_connect("k1-05-ignored-id"))
            .await
            .expect("CONNECT sends");
        assert_eq!(
            k1_05_recv(&mut client).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "mapped identifier connects"
        );
        assert!(
            shared.sessions.get("k1-05-a").is_some(),
            "session uses the common name"
        );
        client
            .write_all(&ws_test_subscribe(1, "k1-05/cert/#"))
            .await
            .expect("SUBSCRIBE sends");
        assert_eq!(k1_05_recv(&mut client).await[0], 0x90, "SUBACK answers");
        client
            .write_all(&ws_test_publish("k1-05/cert/x", b"secret"))
            .await
            .expect("PUBLISH sends");
        let delivered = k1_05_recv(&mut client).await;
        assert!(delivered.ends_with(b"secret"), "own publish delivers");
    }

    /// A certificate from a different CA fails the handshake.
    /// An expired certificate fails the handshake. Goes through
    /// the broker. The broker stays up after each failed handshake.
    /// TLS 1.3 reports the refusal after the client flight.
    /// The test accepts a dial error or a refused CONNECT.
    #[tokio::test]
    async fn k1_05_wrong_ca_and_expired_certificates_fail() {
        use tokio::io::AsyncWriteExt as _;
        let shared = Shared::new();
        let port = k1_05_listener(shared, k1_05_policy(true)).await;

        for client in ["wrong-client", "expired-client"] {
            match k1_05_dial(port, Some(client)).await {
                Err(_) => {}
                Ok(mut stream) => {
                    let _ = stream.write_all(&ws_test_connect("k1-05-bad-cert")).await;
                    use tokio::io::AsyncReadExt as _;
                    let mut head = [0u8; 4];
                    let denied =
                        tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut head))
                            .await;
                    match denied {
                        Err(_) => {}
                        Ok(Err(_)) => {}
                        Ok(Ok(_)) => {
                            assert_ne!(
                                head,
                                [0x20, 0x02, 0x00, 0x00],
                                "a bad certificate must not connect"
                            );
                        }
                    }
                }
            }
        }

        let mut valid = k1_05_dial(port, Some("client-a"))
            .await
            .expect("valid dials");
        valid
            .write_all(&ws_test_connect("k1-05-after-denials"))
            .await
            .expect("CONNECT sends");
        assert_eq!(
            k1_05_recv(&mut valid).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "the broker stays up"
        );
    }

    /// Without verification the behavior of today does not change.
    /// A client without a certificate connects. Goes through the broker.
    #[tokio::test]
    async fn k1_05_no_verification_keeps_today_behavior() {
        use tokio::io::AsyncWriteExt as _;
        let shared = Shared::new();
        let port = k1_05_listener(shared, super::TlsClientAuth::default()).await;

        let mut plain = k1_05_dial(port, None).await.expect("plain TLS dials");
        plain
            .write_all(&ws_test_connect("k1-05-plain"))
            .await
            .expect("CONNECT sends");
        assert_eq!(
            k1_05_recv(&mut plain).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "no verification connects"
        );
    }

    /// Opens a TLS 1.2 session that offers one legacy cipher list.
    /// Returns `Ok` when the handshake passes. Uses the maintained
    /// openssl crate, never hand-rolled cryptography.
    async fn probe_legacy_dial(port: u16, cipher: &str) -> Result<(), String> {
        use openssl::ssl::{Ssl, SslContextBuilder, SslMethod, SslVerifyMode, SslVersion};
        use std::pin::Pin;
        let mut builder =
            SslContextBuilder::new(SslMethod::tls_client()).map_err(|e| e.to_string())?;
        builder.set_verify(SslVerifyMode::NONE);
        builder
            .set_min_proto_version(Some(SslVersion::TLS1_2))
            .map_err(|e| e.to_string())?;
        builder
            .set_max_proto_version(Some(SslVersion::TLS1_2))
            .map_err(|e| e.to_string())?;
        builder.set_cipher_list(cipher).map_err(|e| e.to_string())?;
        let context = builder.build();
        let ssl = Ssl::new(&context).map_err(|e| e.to_string())?;
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .map_err(|e| e.to_string())?;
        let mut stream = tokio_openssl::SslStream::new(ssl, tcp).map_err(|e| e.to_string())?;
        tokio::time::timeout(Duration::from_secs(10), Pin::new(&mut stream).connect())
            .await
            .map_err(|_| "handshake timeout".to_string())?
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    /// TLS suite probe. Dials the production TLS listener with a
    /// modern client and with legacy CBC-only clients. Prints one
    /// PROBE line per suite. Goes through the broker. The modern
    /// handshake passes and its CONNECT gets CONNACK. Each legacy
    /// probe reports OK or FAIL. The R1 note uses the result.
    #[tokio::test]
    async fn tls_suite_probe_matrix_reports_modern_and_legacy() {
        use tokio::io::AsyncWriteExt as _;
        let shared = Shared::new();
        let port = k1_05_listener(shared, super::TlsClientAuth::default()).await;

        // Modern client with default suites. This probe must pass.
        let mut modern = k1_05_dial(port, None).await.expect("modern TLS dials");
        modern
            .write_all(&ws_test_connect("tls-probe-modern"))
            .await
            .expect("CONNECT sends");
        assert_eq!(
            k1_05_recv(&mut modern).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "modern client connects"
        );
        println!("PROBE kernel modern OK (rustls handshake, CONNACK 0x00)");

        // Legacy CBC suites. Probe each suite and report the result.
        // The R1 note uses it. The probe records, it does not require.
        let mut failures = 0u32;
        for (label, cipher) in [
            ("ecdhe-cbc", "ECDHE-RSA-AES128-SHA"),
            ("rsa-cbc", "AES128-SHA"),
        ] {
            match probe_legacy_dial(port, cipher).await {
                Ok(()) => println!("PROBE kernel {label} OK ({cipher})"),
                Err(_) => {
                    println!("PROBE kernel {label} FAIL (handshake failed)");
                    failures += 1;
                }
            }
        }
        println!("PROBE done failures={failures} backlog=1024");
    }

    /// Two tenants stay apart on the certificate path. Each client
    /// carries its own certificate. A publish reaches its own tenant
    /// only. Goes through the broker.
    #[tokio::test]
    async fn k1_05_two_tenants_stay_apart_on_certificate_path() {
        use tokio::io::AsyncWriteExt as _;
        let mut shared = Shared::new();
        shared.tls_peer_cert_as_username = "cn".to_string();
        shared
            .tenant_registry
            .replace(vec![broker_session::tenant::TenantRule::new(
                "",
                "tenant",
                "t-${username}",
            )])
            .expect("tenant rule installs");
        let port = k1_05_listener(shared.clone(), k1_05_policy(false)).await;

        // Each tenant connects with its own certificate.
        let mut client_a = k1_05_dial(port, Some("client-a"))
            .await
            .expect("first dials");
        client_a
            .write_all(&ws_test_connect("k1-05-tenant-a"))
            .await
            .expect("CONNECT sends");
        assert_eq!(
            k1_05_recv(&mut client_a).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "first tenant connects"
        );
        client_a
            .write_all(&ws_test_subscribe(1, "k1-05/tenant/x"))
            .await
            .expect("SUBSCRIBE sends");
        assert_eq!(k1_05_recv(&mut client_a).await[0], 0x90);

        let mut client_b = k1_05_dial(port, Some("client-b"))
            .await
            .expect("second dials");
        client_b
            .write_all(&ws_test_connect("k1-05-tenant-b"))
            .await
            .expect("CONNECT sends");
        assert_eq!(
            k1_05_recv(&mut client_b).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "second tenant connects"
        );
        client_b
            .write_all(&ws_test_subscribe(1, "k1-05/tenant/x"))
            .await
            .expect("SUBSCRIBE sends");
        assert_eq!(k1_05_recv(&mut client_b).await[0], 0x90);

        // A publish in the first tenant reaches its own subscriber only.
        client_a
            .write_all(&ws_test_publish("k1-05/tenant/x", b"secret-a"))
            .await
            .expect("PUBLISH sends");
        let got_a = k1_05_recv(&mut client_a).await;
        assert!(got_a.ends_with(b"secret-a"), "own tenant delivers");
        let other_a =
            tokio::time::timeout(Duration::from_millis(300), k1_05_recv(&mut client_b)).await;
        assert!(other_a.is_err(), "other tenant gets nothing");

        // A publish in the second tenant reaches its own subscriber only.
        client_b
            .write_all(&ws_test_publish("k1-05/tenant/x", b"secret-b"))
            .await
            .expect("PUBLISH sends");
        let got_b = k1_05_recv(&mut client_b).await;
        assert!(got_b.ends_with(b"secret-b"), "own tenant delivers");
        let other_b =
            tokio::time::timeout(Duration::from_millis(300), k1_05_recv(&mut client_a)).await;
        assert!(other_b.is_err(), "other tenant gets nothing");
    }

    /// Starts one kernel secure WebSocket listener with the K1-05
    /// test server certificate and the given client policy.
    /// Returns the port. The task runs until the test aborts it.
    /// The listener copies the policy into the shared bind flags.
    /// The bind then refuses a missing certificate when required.
    async fn k1_05_wss_listener(mut shared: Shared, policy: super::TlsClientAuth) -> u16 {
        shared.wss_verify_peer = policy.verify_peer;
        shared.wss_verify_client_cert = policy.legacy_verify;
        shared.wss_fail_if_no_peer_cert = policy.fail_if_no_peer_cert;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("wss binds");
        let port = listener.local_addr().expect("wss addr").port();
        let config = super::load_tls_config(
            &k1_05_file("server-cert.pem"),
            &k1_05_file("server-key.pem"),
            &policy,
        )
        .expect("test TLS loads");
        tokio::spawn(async move {
            serve_native_wss(
                listener,
                config,
                "/mqtt".to_string(),
                Some(1000),
                Arc::new(AtomicU64::new(0)),
                shared,
            )
            .await;
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        port
    }

    /// Opens a secure WebSocket session to the test port.
    /// `client` selects the client certificate, if any.
    /// Returns the handshake result. The server selects `mqtt`.
    // The result error type is large. The signature is fixed.
    #[allow(clippy::result_large_err)]
    async fn k1_05_wss_dial(
        port: u16,
        client: Option<&str>,
    ) -> Result<
        tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>,
        tokio_tungstenite::tungstenite::Error,
    > {
        use tokio_tungstenite::tungstenite::client::ClientRequestBuilder;
        let connector = tokio_rustls::TlsConnector::from(k1_05_client_config(client));
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("tcp dials");
        let name =
            rustls::pki_types::ServerName::try_from("127.0.0.1".to_string()).expect("name holds");
        let tls = tokio::time::timeout(Duration::from_secs(10), connector.connect(name, tcp))
            .await
            .expect("handshake ends in time")?;
        let uri: http::Uri = format!("wss://127.0.0.1:{port}/mqtt")
            .parse()
            .expect("uri parses");
        let builder = ClientRequestBuilder::new(uri).with_sub_protocol("mqtt");
        let (stream, response) = tokio::time::timeout(
            Duration::from_secs(10),
            tokio_tungstenite::client_async_with_config(builder, tls, None),
        )
        .await
        .expect("upgrade ends in time")?;
        let selected = response
            .headers()
            .get("Sec-WebSocket-Protocol")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("");
        assert_eq!(selected, "mqtt", "server selects mqtt");
        Ok(stream)
    }

    /// In require mode a secure WebSocket client without a certificate
    /// fails. A valid certificate connects. Goes through the broker.
    /// The broker stays up after the failed handshake.
    #[tokio::test]
    async fn k1_05_wss_require_mode_checks_client_certificate() {
        use futures::SinkExt as _;
        let shared = Shared::new();
        let port = k1_05_wss_listener(shared, k1_05_policy(true)).await;

        let denied = k1_05_wss_dial(port, None).await;
        assert!(denied.is_err(), "no certificate fails");

        let mut valid = k1_05_wss_dial(port, Some("client-a"))
            .await
            .expect("valid dials");
        valid
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_connect("k1-05-wss-valid"),
            ))
            .await
            .expect("CONNECT sends");
        assert_eq!(
            wss_test_recv(&mut valid).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "valid certificate connects"
        );
        let _ = valid.close(None).await;
    }

    /// Test PSK identity and key. The key has 32 bytes.
    const PSK_TEST_IDENTITY: &str = "psk-device";
    /// Makes a store with one test identity. Goes through the store.
    fn psk_test_store() -> std::sync::Arc<broker_auth::PskStore> {
        let store = std::sync::Arc::new(broker_auth::PskStore::new());
        store
            .insert(PSK_TEST_IDENTITY, &[0x2Au8; 32])
            .expect("test identity stores");
        store
    }

    /// Starts one kernel PSK listener. Returns the port.
    /// The task runs until the test aborts it.
    async fn psk_test_listener(
        shared: Shared,
        store: std::sync::Arc<broker_auth::PskStore>,
    ) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("psk binds");
        let port = listener.local_addr().expect("psk addr").port();
        tokio::spawn(async move {
            serve_native_mqtt_psk(
                listener,
                store,
                Some(1000),
                Arc::new(AtomicU64::new(0)),
                shared,
            )
            .await;
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        port
    }

    /// Opens a TLS PSK session to the test port. Forces TLS 1.2,
    /// the version the edge reference offers for PSK. Returns the
    /// handshake result as a string for the leak check.
    async fn psk_test_dial(port: u16, identity: &str, key: Vec<u8>) -> Result<(), String> {
        use openssl::ssl::{Ssl, SslContextBuilder, SslMethod, SslVerifyMode, SslVersion};
        use std::pin::Pin;
        let mut builder =
            SslContextBuilder::new(SslMethod::tls_client()).map_err(|e| e.to_string())?;
        builder.set_verify(SslVerifyMode::NONE);
        builder
            .set_min_proto_version(Some(SslVersion::TLS1_2))
            .map_err(|e| e.to_string())?;
        builder
            .set_max_proto_version(Some(SslVersion::TLS1_2))
            .map_err(|e| e.to_string())?;
        builder
            .set_cipher_list(super::PSK_CIPHER_LIST)
            .map_err(|e| e.to_string())?;
        let id = identity.to_string();
        builder.set_psk_client_callback(move |_ssl, _hint, identity_out, psk_out| {
            if id.len() + 1 > identity_out.len() || key.len() > psk_out.len() {
                return Err(openssl::error::ErrorStack::get());
            }
            identity_out[..id.len()].copy_from_slice(id.as_bytes());
            identity_out[id.len()] = 0;
            psk_out[..key.len()].copy_from_slice(&key);
            Ok(key.len())
        });
        let context = builder.build();
        let ssl = Ssl::new(&context).map_err(|e| e.to_string())?;
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .map_err(|e| e.to_string())?;
        let mut stream = tokio_openssl::SslStream::new(ssl, tcp).map_err(|e| e.to_string())?;
        tokio::time::timeout(Duration::from_secs(10), Pin::new(&mut stream).connect())
            .await
            .map_err(|_| "handshake timeout".to_string())?
            .map_err(|e| e.to_string())?;
        // The handshake holds the stream. Drop it at once.
        Ok(())
    }

    /// Opens a PSK session and keeps it for MQTT bytes.
    /// Forces TLS 1.2. Returns the live stream.
    async fn psk_test_connect(
        port: u16,
        identity: &str,
        key: Vec<u8>,
    ) -> tokio_openssl::SslStream<tokio::net::TcpStream> {
        use openssl::ssl::{Ssl, SslContextBuilder, SslMethod, SslVerifyMode, SslVersion};
        use std::pin::Pin;
        let mut builder = SslContextBuilder::new(SslMethod::tls_client()).expect("client builds");
        builder.set_verify(SslVerifyMode::NONE);
        builder
            .set_min_proto_version(Some(SslVersion::TLS1_2))
            .expect("min holds");
        builder
            .set_max_proto_version(Some(SslVersion::TLS1_2))
            .expect("max holds");
        builder
            .set_cipher_list(super::PSK_CIPHER_LIST)
            .expect("ciphers hold");
        let id = identity.to_string();
        builder.set_psk_client_callback(move |_ssl, _hint, identity_out, psk_out| {
            identity_out[..id.len()].copy_from_slice(id.as_bytes());
            identity_out[id.len()] = 0;
            psk_out[..key.len()].copy_from_slice(&key);
            Ok(key.len())
        });
        let context = builder.build();
        let ssl = Ssl::new(&context).expect("ssl builds");
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("tcp dials");
        let mut stream = tokio_openssl::SslStream::new(ssl, tcp).expect("stream builds");
        tokio::time::timeout(Duration::from_secs(10), Pin::new(&mut stream).connect())
            .await
            .expect("handshake ends in time")
            .expect("valid PSK dials");
        stream
    }

    /// The PSK context builds with a store. Goes through the builder.
    #[test]
    fn psk_ssl_context_builds_with_store() {
        let context = super::build_psk_ssl_context(psk_test_store());
        assert!(context.is_ok(), "PSK context builds");
    }

    /// A client with the correct identity and key connects and
    /// exchanges MQTT bytes. Goes through the broker.
    #[tokio::test]
    async fn psk_valid_identity_connects_and_exchanges_mqtt() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let shared = Shared::new();
        let port = psk_test_listener(shared, psk_test_store()).await;
        let mut client = psk_test_connect(port, PSK_TEST_IDENTITY, vec![0x2Au8; 32]).await;
        client
            .write_all(&ws_test_connect("k1-04-psk-valid"))
            .await
            .expect("CONNECT sends");
        let mut head = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(5), client.read_exact(&mut head))
            .await
            .expect("CONNACK in time")
            .expect("socket stays open");
        assert_eq!(head, [0x20, 0x02, 0x00, 0x00], "valid PSK connects");
        client
            .write_all(&ws_test_subscribe(1, "k1-04/psk/#"))
            .await
            .expect("SUBSCRIBE sends");
        let suback = k1_05_recv(&mut client).await;
        assert_eq!(suback[0], 0x90, "SUBACK answers");
        client
            .write_all(&ws_test_publish("k1-04/psk/x", b"psk-secret"))
            .await
            .expect("PUBLISH sends");
        let delivered = k1_05_recv(&mut client).await;
        assert!(delivered.ends_with(b"psk-secret"), "own publish delivers");
    }

    /// An unknown identity and a wrong key fail with the same alert.
    /// The unknown identity gets a stand-in key, so it fails at the
    /// Finished check like a wrong key does. Neither failure names
    /// the identity. Goes through the broker.
    /// Both failures run in the same handshake budget, as far as
    /// the library permits.
    #[tokio::test]
    async fn psk_unknown_identity_and_wrong_key_fail_without_leak() {
        let shared = Shared::new();
        let port = psk_test_listener(shared, psk_test_store()).await;
        let unknown = psk_test_dial(port, "no-such-device", vec![0x2Au8; 32]).await;
        assert!(unknown.is_err(), "unknown identity fails");
        let unknown_text = unknown.err().unwrap_or_default();
        assert!(
            !unknown_text.contains("no-such-device"),
            "failure names no identity"
        );
        let wrong = psk_test_dial(port, PSK_TEST_IDENTITY, vec![0x33u8; 32]).await;
        assert!(wrong.is_err(), "wrong key fails");
        let wrong_text = wrong.err().unwrap_or_default();
        assert!(
            !wrong_text.contains(PSK_TEST_IDENTITY),
            "failure names no identity"
        );
        assert_eq!(
            unknown_text, wrong_text,
            "both failures carry the same alert"
        );
        // The broker stays up after both failed handshakes.
        let mut client = psk_test_connect(port, PSK_TEST_IDENTITY, vec![0x2Au8; 32]).await;
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        client
            .write_all(&ws_test_connect("k1-04-psk-after-denials"))
            .await
            .expect("CONNECT sends");
        let mut head = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(5), client.read_exact(&mut head))
            .await
            .expect("CONNACK in time")
            .expect("socket stays open");
        assert_eq!(head, [0x20, 0x02, 0x00, 0x00], "broker stays up");
    }

    /// The verified PSK identity reaches the bind as the username.
    /// A CONNECT username of another user does not pass: the
    /// session carries the verified identity. Goes through the broker.
    #[tokio::test]
    async fn psk_verified_identity_reaches_the_bind() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let shared = Shared::new();
        let port = psk_test_listener(shared.clone(), psk_test_store()).await;
        let mut client = psk_test_connect(port, PSK_TEST_IDENTITY, vec![0x2Au8; 32]).await;
        client
            .write_all(&ws_test_connect_with_auth(
                "k1-04-psk-bound",
                "intruder",
                b"wrong",
            ))
            .await
            .expect("CONNECT sends");
        let mut head = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(5), client.read_exact(&mut head))
            .await
            .expect("CONNACK in time")
            .expect("socket stays open");
        assert_eq!(head, [0x20, 0x02, 0x00, 0x00], "PSK CONNECT connects");
        let username = shared
            .sessions
            .get("k1-04-psk-bound")
            .expect("session exists")
            .username
            .read()
            .clone();
        assert_eq!(
            username.as_deref(),
            Some(PSK_TEST_IDENTITY),
            "the bind carries the verified identity"
        );
    }

    /// Opens a TLS 1.3 PSK session to the test port. Forces TLS 1.3,
    /// so only the find-session callback can serve it. Returns the
    /// handshake result as a string for the leak check.
    async fn psk_test_dial_tls13(port: u16, identity: &str, key: Vec<u8>) -> Result<(), String> {
        use openssl::ssl::{Ssl, SslContextBuilder, SslMethod, SslVerifyMode, SslVersion};
        use std::pin::Pin;
        let mut builder =
            SslContextBuilder::new(SslMethod::tls_client()).map_err(|e| e.to_string())?;
        builder.set_verify(SslVerifyMode::NONE);
        builder
            .set_min_proto_version(Some(SslVersion::TLS1_3))
            .map_err(|e| e.to_string())?;
        builder
            .set_max_proto_version(Some(SslVersion::TLS1_3))
            .map_err(|e| e.to_string())?;
        builder
            .set_ciphersuites(super::PSK_CIPHERSUITES)
            .map_err(|e| e.to_string())?;
        let id = identity.to_string();
        builder.set_psk_client_callback(move |_ssl, _hint, identity_out, psk_out| {
            if id.len() + 1 > identity_out.len() || key.len() > psk_out.len() {
                return Err(openssl::error::ErrorStack::get());
            }
            identity_out[..id.len()].copy_from_slice(id.as_bytes());
            identity_out[id.len()] = 0;
            psk_out[..key.len()].copy_from_slice(&key);
            Ok(key.len())
        });
        let context = builder.build();
        let ssl = Ssl::new(&context).map_err(|e| e.to_string())?;
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .map_err(|e| e.to_string())?;
        let mut stream = tokio_openssl::SslStream::new(ssl, tcp).map_err(|e| e.to_string())?;
        tokio::time::timeout(Duration::from_secs(10), Pin::new(&mut stream).connect())
            .await
            .map_err(|_| "handshake timeout".to_string())?
            .map_err(|e| e.to_string())?;
        // The handshake holds the stream. Drop it at once.
        Ok(())
    }

    /// Opens a TLS 1.3 PSK session and keeps it for MQTT bytes.
    /// Forces TLS 1.3. Returns the live stream.
    async fn psk_test_connect_tls13(
        port: u16,
        identity: &str,
        key: Vec<u8>,
    ) -> tokio_openssl::SslStream<tokio::net::TcpStream> {
        use openssl::ssl::{Ssl, SslContextBuilder, SslMethod, SslVerifyMode, SslVersion};
        use std::pin::Pin;
        let mut builder = SslContextBuilder::new(SslMethod::tls_client()).expect("client builds");
        builder.set_verify(SslVerifyMode::NONE);
        builder
            .set_min_proto_version(Some(SslVersion::TLS1_3))
            .expect("min holds");
        builder
            .set_max_proto_version(Some(SslVersion::TLS1_3))
            .expect("max holds");
        builder
            .set_ciphersuites(super::PSK_CIPHERSUITES)
            .expect("ciphers hold");
        let id = identity.to_string();
        builder.set_psk_client_callback(move |_ssl, _hint, identity_out, psk_out| {
            identity_out[..id.len()].copy_from_slice(id.as_bytes());
            identity_out[id.len()] = 0;
            psk_out[..key.len()].copy_from_slice(&key);
            Ok(key.len())
        });
        let context = builder.build();
        let ssl = Ssl::new(&context).expect("ssl builds");
        let tcp = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("tcp dials");
        let mut stream = tokio_openssl::SslStream::new(ssl, tcp).expect("stream builds");
        tokio::time::timeout(Duration::from_secs(10), Pin::new(&mut stream).connect())
            .await
            .expect("handshake ends in time")
            .expect("valid PSK dials");
        stream
    }

    /// A TLS 1.3 client with the correct identity and key connects
    /// and exchanges MQTT bytes. Forces TLS 1.3, so only the
    /// find-session callback serves the handshake. Goes through
    /// the broker.
    #[tokio::test]
    async fn psk_tls13_valid_identity_connects_and_exchanges_mqtt() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let shared = Shared::new();
        let port = psk_test_listener(shared, psk_test_store()).await;
        let mut client = psk_test_connect_tls13(port, PSK_TEST_IDENTITY, vec![0x2Au8; 32]).await;
        client
            .write_all(&ws_test_connect("k1-04-psk-tls13-valid"))
            .await
            .expect("CONNECT sends");
        let mut head = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(5), client.read_exact(&mut head))
            .await
            .expect("CONNACK in time")
            .expect("socket stays open");
        assert_eq!(head, [0x20, 0x02, 0x00, 0x00], "valid PSK connects");
        client
            .write_all(&ws_test_subscribe(1, "k1-04/psk13/#"))
            .await
            .expect("SUBSCRIBE sends");
        let suback = k1_05_recv(&mut client).await;
        assert_eq!(suback[0], 0x90, "SUBACK answers");
        client
            .write_all(&ws_test_publish("k1-04/psk13/x", b"psk13-secret"))
            .await
            .expect("PUBLISH sends");
        let delivered = k1_05_recv(&mut client).await;
        assert!(delivered.ends_with(b"psk13-secret"), "own publish delivers");
        // An unknown identity and a wrong key both fail over TLS 1.3
        // with the same alert. Neither failure names the identity.
        let unknown = psk_test_dial_tls13(port, "no-such-device", vec![0x2Au8; 32]).await;
        assert!(unknown.is_err(), "unknown identity fails");
        let unknown_text = unknown.err().unwrap_or_default();
        assert!(
            !unknown_text.contains("no-such-device"),
            "failure names no identity"
        );
        let wrong = psk_test_dial_tls13(port, PSK_TEST_IDENTITY, vec![0x33u8; 32]).await;
        assert!(wrong.is_err(), "wrong key fails");
        let wrong_text = wrong.err().unwrap_or_default();
        assert!(
            !wrong_text.contains(PSK_TEST_IDENTITY),
            "failure names no identity"
        );
        assert_eq!(
            unknown_text, wrong_text,
            "both TLS 1.3 failures carry the same alert"
        );
    }

    /// An empty store fails every handshake. Goes through the broker.
    /// Proves the fail-closed default: no identity connects.
    #[tokio::test]
    async fn psk_empty_store_fails_closed() {
        let shared = Shared::new();
        let empty = std::sync::Arc::new(broker_auth::PskStore::new());
        let port = psk_test_listener(shared, empty).await;
        let denied = psk_test_dial(port, PSK_TEST_IDENTITY, vec![0x2Au8; 32]).await;
        assert!(denied.is_err(), "empty store denies");
    }

    /// Builds an MQTT 5 CONNECT packet with no properties.
    fn p2_v5_connect(client_id: &str) -> Vec<u8> {
        let mut body = vec![
            0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3C, 0x00,
        ];
        body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
        body.extend_from_slice(client_id.as_bytes());
        let mut packet = vec![0x10];
        broker_protocol::wire::encode_varint(body.len() as u32, &mut packet);
        packet.extend_from_slice(&body);
        packet
    }

    /// Builds an MQTT 5 QoS 0 PUBLISH packet with no properties.
    fn p2_v5_publish(topic: &str, payload: &[u8]) -> Vec<u8> {
        let mut body = (topic.len() as u16).to_be_bytes().to_vec();
        body.extend_from_slice(topic.as_bytes());
        body.push(0x00);
        body.extend_from_slice(payload);
        let mut packet = vec![0x30];
        broker_protocol::wire::encode_varint(body.len() as u32, &mut packet);
        packet.extend_from_slice(&body);
        packet
    }

    /// Builds an MQTT 3.1.1 QoS 0 PUBLISH packet.
    fn p2_v4_publish(topic: &str, payload: &[u8]) -> Vec<u8> {
        let mut body = (topic.len() as u16).to_be_bytes().to_vec();
        body.extend_from_slice(topic.as_bytes());
        body.extend_from_slice(payload);
        let mut packet = vec![0x30];
        broker_protocol::wire::encode_varint(body.len() as u32, &mut packet);
        packet.extend_from_slice(&body);
        packet
    }

    /// Builds a v5 PUBLISH whose full packet is exactly `cap + 1`
    /// bytes. Grows the payload until the packet hits the size.
    fn p2_exact_oversize_v5(topic: &str, cap: usize) -> Vec<u8> {
        let mut len = 0usize;
        loop {
            let payload = vec![0xABu8; len];
            let packet = p2_v5_publish(topic, &payload);
            if packet.len() == cap + 1 {
                return packet;
            }
            assert!(packet.len() < cap + 1, "packet jumps past cap + 1");
            len += 1;
        }
    }

    /// Builds a v4 PUBLISH whose full packet is exactly `cap + 1`
    /// bytes. Grows the payload until the packet hits the size.
    fn p2_exact_oversize_v4(topic: &str, cap: usize) -> Vec<u8> {
        let mut len = 0usize;
        loop {
            let payload = vec![0xABu8; len];
            let packet = p2_v4_publish(topic, &payload);
            if packet.len() == cap + 1 {
                return packet;
            }
            assert!(packet.len() < cap + 1, "packet jumps past cap + 1");
            len += 1;
        }
    }

    /// Reads one full MQTT packet from a TCP stream.
    /// Handles multi-byte remaining lengths for large packets.
    async fn p2_tcp_recv_full(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        use tokio::io::AsyncReadExt as _;
        let mut buf = Vec::new();
        loop {
            let header = broker_protocol::wire::fixed_header(&buf).expect("header parses");
            if let Some(header) = header {
                let need = header.packet_len();
                while buf.len() < need {
                    let mut chunk = vec![0u8; need - buf.len()];
                    tokio::time::timeout(Duration::from_secs(30), stream.read_exact(&mut chunk))
                        .await
                        .expect("body in time")
                        .expect("socket stays open");
                    buf.extend_from_slice(&chunk);
                }
                return buf;
            }
            let mut one = [0u8; 1];
            tokio::time::timeout(Duration::from_secs(30), stream.read_exact(&mut one))
                .await
                .expect("header in time")
                .expect("socket stays open");
            buf.push(one[0]);
        }
    }

    /// Reads the Maximum Packet Size (property 39) of a v5 CONNACK.
    /// Returns `None` when the property is absent.
    fn p2_connack_max_packet(packet: &[u8]) -> Option<u32> {
        let header = broker_protocol::wire::fixed_header(packet).ok()??;
        let body = &packet[header.header_len..];
        if body.len() < 3 {
            return None;
        }
        let mut cursor = &body[2..];
        let (prop_len, used) = broker_protocol::wire::decode_varint(cursor).ok()??;
        cursor = &cursor[used..];
        if cursor.len() < prop_len as usize {
            return None;
        }
        let mut props = &cursor[..prop_len as usize];
        while !props.is_empty() {
            let id = props[0];
            props = &props[1..];
            match id {
                17 => {
                    props = props.get(4..)?;
                }
                33 | 34 => {
                    props = props.get(2..)?;
                }
                39 => {
                    let value = u32::from_be_bytes(props.get(..4)?.try_into().ok()?);
                    return Some(value);
                }
                18 | 31 => {
                    if props.len() < 2 {
                        return None;
                    }
                    let len = u16::from_be_bytes([props[0], props[1]]) as usize;
                    props = props.get(2 + len..)?;
                }
                38 => {
                    for _ in 0..2 {
                        if props.len() < 2 {
                            return None;
                        }
                        let len = u16::from_be_bytes([props[0], props[1]]) as usize;
                        props = props.get(2 + len..)?;
                    }
                }
                _ => return None,
            }
        }
        None
    }

    /// Asserts that a TCP stream closes soon after the read bytes.
    async fn p2_assert_closed(stream: &mut tokio::net::TcpStream) {
        use tokio::io::AsyncReadExt as _;
        let mut one = [0u8; 1];
        let end = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut one)).await;
        match end {
            Ok(Ok(0)) => {}
            Ok(Ok(_)) => panic!("socket must close, got a byte"),
            Ok(Err(_)) => {}
            Err(_) => panic!("socket must close in time"),
        }
    }

    /// Starts one TCP listener on one [`Shared`]. Returns the port.
    /// The task runs until the test aborts it.
    async fn p2_tcp_listener(shared: Shared) -> u16 {
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("tcp binds");
        let port = tcp.local_addr().expect("tcp addr").port();
        tokio::spawn(async move {
            serve_native_mqtt(tcp, Some(1000), Arc::new(AtomicU64::new(0)), shared).await;
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        port
    }

    /// CONNACK advertises the configured cap. Goes through the broker.
    /// The default cap is 1 MiB. A set cap replaces it exactly.
    #[tokio::test]
    async fn max_packet_size_connack_advertises_configured_cap() {
        use tokio::io::AsyncWriteExt as _;
        let shared = Shared::new();
        assert_eq!(shared.sessions.max_packet_size(), 1_048_576);
        let port = p2_tcp_listener(shared).await;
        let mut client = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("tcp dials");
        client
            .write_all(&p2_v5_connect("p2-default-cap"))
            .await
            .expect("CONNECT sends");
        let connack = p2_tcp_recv_full(&mut client).await;
        assert_eq!(
            p2_connack_max_packet(&connack),
            Some(1_048_576),
            "default CONNACK advertises 1 MiB"
        );

        let capped = Shared::new();
        capped.sessions.set_max_packet_size(2048);
        let port = p2_tcp_listener(capped).await;
        let mut client = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("tcp dials");
        client
            .write_all(&p2_v5_connect("p2-custom-cap"))
            .await
            .expect("CONNECT sends");
        let connack = p2_tcp_recv_full(&mut client).await;
        assert_eq!(
            p2_connack_max_packet(&connack),
            Some(2048),
            "CONNACK advertises the set cap"
        );
    }

    /// An oversize PUBLISH closes the connection. Goes through the
    /// broker. A v5 client gets DISCONNECT 0x95 first. A v4 client
    /// gets no packet. The broker serves the next client.
    #[tokio::test]
    async fn max_packet_size_oversize_publish_closes_with_0x95() {
        use tokio::io::AsyncWriteExt as _;
        let shared = Shared::new();
        shared.sessions.set_max_packet_size(2048);
        let port = p2_tcp_listener(shared).await;

        let mut v5 = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("tcp dials");
        v5.write_all(&p2_v5_connect("p2-v5-big"))
            .await
            .expect("CONNECT sends");
        let connack = p2_tcp_recv_full(&mut v5).await;
        assert_eq!(p2_connack_max_packet(&connack), Some(2048));
        v5.write_all(&p2_exact_oversize_v5("p2/big", 2048))
            .await
            .expect("PUBLISH sends");
        let refused = p2_tcp_recv_full(&mut v5).await;
        assert_eq!(refused, vec![0xE0, 0x01, 0x95], "v5 gets DISCONNECT 0x95");
        p2_assert_closed(&mut v5).await;

        let mut v4 = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("tcp dials");
        v4.write_all(&ws_test_connect("p2-v4-big"))
            .await
            .expect("CONNECT sends");
        assert_eq!(
            tcp_test_recv(&mut v4).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "v4 CONNACK accepts"
        );
        v4.write_all(&p2_exact_oversize_v4("p2/big", 2048))
            .await
            .expect("PUBLISH sends");
        p2_assert_closed(&mut v4).await;

        let mut fresh = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("tcp dials");
        fresh
            .write_all(&ws_test_connect("p2-fresh"))
            .await
            .expect("CONNECT sends");
        assert_eq!(
            tcp_test_recv(&mut fresh).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "broker stays up"
        );
    }

    /// The file entry drives the socket behaviour. Goes through the
    /// broker. Loads `session.max_packet_size` from `indra.toml`,
    /// applies it like boot does, then observes it on the socket.
    #[tokio::test]
    async fn max_packet_size_from_file_applies_to_socket() {
        use tokio::io::AsyncWriteExt as _;
        let dir = std::env::temp_dir().join(format!(
            "indra-p2-file-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).expect("config dir forms");
        std::fs::write(
            dir.join("indra.toml"),
            "[session]\nmax_packet_size = 4096\n",
        )
        .expect("config writes");
        let layered = broker_config::load_layered(&dir, &broker_config::CliOverrides::default())
            .expect("config loads");
        assert_eq!(layered.config().session.max_packet_size, 4096);
        let shared = Shared::new();
        shared
            .sessions
            .set_max_packet_size(layered.config().session.max_packet_size);
        let port = p2_tcp_listener(shared).await;
        let mut client = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("tcp dials");
        client
            .write_all(&p2_v5_connect("p2-file-cap"))
            .await
            .expect("CONNECT sends");
        let connack = p2_tcp_recv_full(&mut client).await;
        assert_eq!(
            p2_connack_max_packet(&connack),
            Some(4096),
            "file cap shows in CONNACK"
        );
        client
            .write_all(&p2_exact_oversize_v5("p2/file", 4096))
            .await
            .expect("PUBLISH sends");
        let refused = p2_tcp_recv_full(&mut client).await;
        assert_eq!(refused, vec![0xE0, 0x01, 0x95], "file cap refuses");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A 3 MiB publish flows at a 4 MiB cap. Goes through the broker.
    /// One TCP subscriber and one WS subscriber both get the bytes.
    #[tokio::test]
    async fn max_packet_size_large_publish_delivers_over_tcp_and_ws() {
        use futures::SinkExt as _;
        use tokio::io::AsyncWriteExt as _;
        let shared = Shared::new();
        shared.sessions.set_max_packet_size(4 * 1024 * 1024);
        let (tcp_port, ws_port) = ws_test_listeners(shared).await;

        let mut tcp_sub = tokio::net::TcpStream::connect(("127.0.0.1", tcp_port))
            .await
            .expect("tcp dials");
        tcp_sub
            .write_all(&ws_test_connect("p2-tcp-sub"))
            .await
            .expect("CONNECT sends");
        assert_eq!(
            tcp_test_recv(&mut tcp_sub).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "TCP CONNACK accepts"
        );
        tcp_sub
            .write_all(&ws_test_subscribe(1, "p2/big"))
            .await
            .expect("SUBSCRIBE sends");
        assert_eq!(tcp_test_recv(&mut tcp_sub).await[0], 0x90, "SUBACK answers");

        let mut ws_sub = ws_test_dial(ws_port).await;
        ws_sub
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_connect("p2-ws-sub"),
            ))
            .await
            .expect("ws CONNECT sends");
        assert_eq!(
            ws_test_recv(&mut ws_sub).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "WS CONNACK accepts"
        );
        ws_sub
            .send(tokio_tungstenite::tungstenite::protocol::Message::Binary(
                ws_test_subscribe(1, "p2/big"),
            ))
            .await
            .expect("ws SUBSCRIBE sends");
        assert_eq!(
            ws_test_recv(&mut ws_sub).await[0],
            0x90,
            "ws SUBACK answers"
        );

        let payload = vec![0x5Au8; 3 * 1024 * 1024];
        let mut pub_client = tokio::net::TcpStream::connect(("127.0.0.1", tcp_port))
            .await
            .expect("tcp dials");
        pub_client
            .write_all(&ws_test_connect("p2-pub"))
            .await
            .expect("CONNECT sends");
        assert_eq!(
            tcp_test_recv(&mut pub_client).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "pub CONNACK accepts"
        );
        pub_client
            .write_all(&p2_v4_publish("p2/big", &payload))
            .await
            .expect("PUBLISH sends");
        let tcp_got = p2_tcp_recv_full(&mut tcp_sub).await;
        assert_eq!(
            tcp_got.len(),
            payload.len() + 13,
            "TCP gets the full publish"
        );
        assert!(tcp_got.ends_with(&payload[..]), "TCP payload intact");
        let ws_got = ws_test_recv(&mut ws_sub).await;
        assert_eq!(ws_got.len(), payload.len() + 13, "WS gets the full publish");
        assert!(ws_got.ends_with(&payload[..]), "WS payload intact");
    }

    /// Identities from a keys file connect through the broker.
    /// Writes one `identity:hexkey` line, loads it through the file
    /// entry, then connects and exchanges MQTT bytes.
    #[tokio::test]
    async fn psk_keys_file_loads_and_connects_through_broker() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let path =
            std::env::temp_dir().join(format!("indra-psk-k104-{}-file.txt", std::process::id()));
        std::fs::write(
            &path,
            format!("{}:{}\n", PSK_TEST_IDENTITY, "2a".repeat(32)),
        )
        .expect("psk file writes");
        let store = std::sync::Arc::new(broker_auth::PskStore::new());
        let count = store
            .load_file(path.to_str().expect("path shows"))
            .expect("psk file loads");
        assert_eq!(count, 1, "one identity loads");
        let shared = Shared::new();
        let port = psk_test_listener(shared, store).await;
        let mut client = psk_test_connect(port, PSK_TEST_IDENTITY, vec![0x2Au8; 32]).await;
        client
            .write_all(&ws_test_connect("k1-04-psk-file"))
            .await
            .expect("CONNECT sends");
        let mut head = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(5), client.read_exact(&mut head))
            .await
            .expect("CONNACK in time")
            .expect("socket stays open");
        assert_eq!(head, [0x20, 0x02, 0x00, 0x00], "file identity connects");
        let _ = std::fs::remove_file(&path);
    }

    /// A keys file drives the same-alert check through the broker.
    /// The store loads from the file entry. An unknown identity and
    /// a wrong key then fail with the same alert. A valid identity
    /// still connects, and its publish still delivers. Goes through
    /// the broker.
    #[tokio::test]
    async fn psk_file_entry_same_alert_and_delivers_through_broker() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let path = std::env::temp_dir().join(format!(
            "indra-psk-k104-{}-same-alert.txt",
            std::process::id()
        ));
        std::fs::write(
            &path,
            format!("{}:{}\n", PSK_TEST_IDENTITY, "2a".repeat(32)),
        )
        .expect("psk file writes");
        let store = std::sync::Arc::new(broker_auth::PskStore::new());
        let count = store
            .load_file(path.to_str().expect("path shows"))
            .expect("psk file loads");
        assert_eq!(count, 1, "one identity loads");
        let shared = Shared::new();
        let port = psk_test_listener(shared, store).await;
        let unknown = psk_test_dial(port, "no-such-device", vec![0x2Au8; 32]).await;
        assert!(unknown.is_err(), "unknown identity fails");
        let unknown_text = unknown.err().unwrap_or_default();
        let wrong = psk_test_dial(port, PSK_TEST_IDENTITY, vec![0x33u8; 32]).await;
        assert!(wrong.is_err(), "wrong key fails");
        let wrong_text = wrong.err().unwrap_or_default();
        assert_eq!(
            unknown_text, wrong_text,
            "both failures carry the same alert"
        );
        let mut client = psk_test_connect(port, PSK_TEST_IDENTITY, vec![0x2Au8; 32]).await;
        client
            .write_all(&ws_test_connect("k1-04-psk-file-alert"))
            .await
            .expect("CONNECT sends");
        let mut head = [0u8; 4];
        tokio::time::timeout(Duration::from_secs(5), client.read_exact(&mut head))
            .await
            .expect("CONNACK in time")
            .expect("socket stays open");
        assert_eq!(head, [0x20, 0x02, 0x00, 0x00], "file identity connects");
        client
            .write_all(&ws_test_subscribe(1, "k1-04/pskfile/#"))
            .await
            .expect("SUBSCRIBE sends");
        let suback = k1_05_recv(&mut client).await;
        assert_eq!(suback[0], 0x90, "SUBACK answers");
        client
            .write_all(&ws_test_publish("k1-04/pskfile/x", b"psk-file-secret"))
            .await
            .expect("PUBLISH sends");
        let delivered = k1_05_recv(&mut client).await;
        assert!(
            delivered.ends_with(b"psk-file-secret"),
            "own publish delivers"
        );
        let _ = std::fs::remove_file(&path);
    }

    // Security tests: client certificate roots, publish after a denial
    // with disconnect, and delayed publish authorization.

    /// Client certificate verification needs a CA file. An empty path
    /// does not select any default set of public authorities.
    #[test]
    fn client_verification_without_ca_file_is_refused() {
        let cert = k1_05_file("server-cert.pem");
        let key = k1_05_file("server-key.pem");
        for (verify_peer, legacy_verify) in [(true, false), (false, true)] {
            let policy = super::TlsClientAuth {
                verify_peer,
                legacy_verify,
                cacertfile: String::new(),
                fail_if_no_peer_cert: false,
            };
            let result = super::load_tls_config(&cert, &key, &policy);
            assert!(
                result.is_err(),
                "verification with no CA file must not load"
            );
        }
        let with_ca = k1_05_policy(false);
        assert!(super::load_tls_config(&cert, &key, &with_ca).is_ok());
    }

    /// The listener controller refuses to start a secure listener with
    /// client verification on and no CA file, for TLS and for secure
    /// WebSocket.
    #[tokio::test]
    async fn controller_does_not_start_secure_listener_without_ca_file() {
        use broker_api::v5::listeners::{ListenerController as _, ListenerRecord};
        let record = |kind: &str| ListenerRecord {
            id: format!("{kind}:default"),
            name: "default".to_string(),
            kind: kind.to_string(),
            bind: "127.0.0.1:0".to_string(),
            enable: true,
            running: false,
            kernel_owned: true,
            max_connections: None,
            acceptors: None,
            backlog: 0,
            cert_file: k1_05_file("server-cert.pem"),
            key_file: k1_05_file("server-key.pem"),
            zone: "default".to_string(),
            mountpoint: String::new(),
            tcp_options: None,
            ssl_options: None,
            websocket: None,
        };
        let mut conf = broker_config::schema::ListenersConf::default();
        conf.tls.verify = "verify_peer".to_string();
        conf.wss.cert_file = k1_05_file("server-cert.pem");
        conf.wss.key_file = k1_05_file("server-key.pem");
        conf.wss.verify_client_cert = true;
        let control = NativeListenerController::new(Shared::new(), conf);
        for kind in ["ssl", "wss"] {
            let refused = control.start_listener(&record(kind));
            assert!(refused.is_err(), "{kind} must not start without a CA file");
            assert!(
                refused
                    .expect_err("refused")
                    .to_lowercase()
                    .contains("ca file"),
                "the error names the CA file"
            );
        }
    }

    /// Rules for the authorization tests: publish to `secret/#` is
    /// denied and everything else is allowed.
    fn acl_deny_secret(shared: &Shared, deny_action: &str) {
        shared
            .auth
            .add_rule(broker_auth::AclRule::new(
                "*",
                broker_auth::AclAction::Publish,
                "secret/#",
                false,
            ))
            .expect("rule adds");
        shared
            .auth
            .add_rule(broker_auth::AclRule::new(
                "*",
                broker_auth::AclAction::All,
                "#",
                true,
            ))
            .expect("rule adds");
        shared
            .authz_settings
            .replace(broker_config::AuthzSettingsConf {
                no_match: "deny".to_string(),
                deny_action: deny_action.to_string(),
                ..broker_config::AuthzSettingsConf::default()
            })
            .expect("settings replace");
    }

    /// Opens a TCP client, sends CONNECT and reads the CONNACK.
    async fn tcp_test_client(port: u16, client_id: &str) -> tokio::net::TcpStream {
        use tokio::io::AsyncWriteExt as _;
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("tcp dials");
        stream
            .write_all(&ws_test_connect(client_id))
            .await
            .expect("CONNECT sends");
        assert_eq!(
            tcp_test_recv(&mut stream).await,
            vec![0x20, 0x02, 0x00, 0x00],
            "CONNACK accepts"
        );
        stream
    }

    /// Subscribes a TCP client to the filters and reads the SUBACKs.
    async fn tcp_test_subscribe(stream: &mut tokio::net::TcpStream, filters: &[&str]) {
        use tokio::io::AsyncWriteExt as _;
        for (index, filter) in filters.iter().enumerate() {
            stream
                .write_all(&ws_test_subscribe(index as u16 + 1, filter))
                .await
                .expect("SUBSCRIBE sends");
            assert_eq!(tcp_test_recv(stream).await[0], 0x90, "SUBACK answers");
        }
    }

    /// With `deny_action = disconnect` the first denied PUBLISH ends the
    /// connection. The PUBLISH packets that follow it in the same read
    /// buffer are not routed.
    #[tokio::test]
    async fn denied_publish_with_disconnect_stops_the_rest_of_the_buffer() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let shared = Shared::new();
        acl_deny_secret(&shared, "disconnect");
        let (tcp_port, _ws_port) = ws_test_listeners(shared).await;

        let mut subscriber = tcp_test_client(tcp_port, "sec-h2-sub").await;
        tcp_test_subscribe(&mut subscriber, &["secret/x", "open/x"]).await;
        let mut offender = tcp_test_client(tcp_port, "sec-h2-bad").await;

        // One write: the denied PUBLISH, then more PUBLISH packets.
        let mut burst = ws_test_publish("secret/x", b"first");
        for index in 0..20 {
            burst.extend(ws_test_publish(
                "secret/x",
                format!("later-{index}").as_bytes(),
            ));
        }
        offender.write_all(&burst).await.expect("burst sends");

        let leaked =
            tokio::time::timeout(Duration::from_millis(800), tcp_test_recv(&mut subscriber)).await;
        assert!(
            leaked.is_err(),
            "no packet of the offender reaches a subscriber"
        );

        // The connection of the offender ends.
        let mut scratch = [0u8; 64];
        let closed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match offender.read(&mut scratch).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        })
        .await;
        assert!(closed.is_ok(), "the denied connection is closed");

        // The path still works for a client that is allowed.
        let mut good = tcp_test_client(tcp_port, "sec-h2-good").await;
        good.write_all(&ws_test_publish("open/x", b"fine"))
            .await
            .expect("PUBLISH sends");
        let delivered = tcp_test_recv(&mut subscriber).await;
        assert!(delivered.ends_with(b"fine"), "an allowed publish arrives");
    }

    /// A delayed publish is checked on its inner topic, also with the
    /// retain flag. An inner topic that the rules allow is delivered
    /// after the delay.
    #[tokio::test]
    async fn delayed_publish_is_authorized_on_the_inner_topic() {
        use tokio::io::AsyncWriteExt as _;
        let shared = Shared::new();
        acl_deny_secret(&shared, "ignore");
        shared.spawn_delayed_driver();
        let (tcp_port, _ws_port) = ws_test_listeners(shared).await;

        let mut subscriber = tcp_test_client(tcp_port, "sec-h3-sub").await;
        tcp_test_subscribe(&mut subscriber, &["secret/#", "open/#"]).await;
        let mut publisher = tcp_test_client(tcp_port, "sec-h3-pub").await;

        let mut retained = ws_test_publish("$delayed/1/secret/kept", b"hidden-kept");
        retained[0] = 0x31;
        publisher.write_all(&retained).await.expect("PUBLISH sends");
        publisher
            .write_all(&ws_test_publish("$delayed/1/secret/x", b"hidden"))
            .await
            .expect("PUBLISH sends");
        publisher
            .write_all(&ws_test_publish("$delayed/1/open/x", b"shown"))
            .await
            .expect("PUBLISH sends");

        // The only packet is the allowed one, after the delay.
        let first = tokio::time::timeout(Duration::from_secs(6), tcp_test_recv(&mut subscriber))
            .await
            .expect("the allowed delayed publish arrives");
        assert!(first.ends_with(b"shown"), "first packet is the allowed one");
        let more =
            tokio::time::timeout(Duration::from_millis(1500), tcp_test_recv(&mut subscriber)).await;
        assert!(more.is_err(), "the denied inner topic is not delivered");

        // The denied retained message was not stored.
        let mut late = tcp_test_client(tcp_port, "sec-h3-late").await;
        late.write_all(&ws_test_subscribe(1, "secret/#"))
            .await
            .expect("SUBSCRIBE sends");
        assert_eq!(tcp_test_recv(&mut late).await[0], 0x90);
        let stored =
            tokio::time::timeout(Duration::from_millis(800), tcp_test_recv(&mut late)).await;
        assert!(stored.is_err(), "no retained message under secret/");
    }
}
