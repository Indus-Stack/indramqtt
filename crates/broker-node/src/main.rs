use async_trait::async_trait;
use broker_auth::{
    AuthnChain, AuthnEntry, AuthnSettingsStore, AuthzNodeCache, AuthzSettingsStore,
    AuthzSourceRegistry, DbAuthSet, DbAuthSetConfig, JwksAuthenticator, JwksConfig,
    KerberosAuthenticator, KerberosConfig, LdapAuthenticator, LdapConfig, MemoryAuth,
    NodeAuthCache, WebhookAuth, WebhookConfig,
};
use broker_cluster::{ClusterMessage, InstallationState, RoutingPlane, TrustedKeys};
use broker_config::{CliOverrides, ConfigError, ConfigRegistry, LayeredConfig};
use broker_delayed::{DelayedEntry, DelayedScheduler, DEFAULT_MAX_DELAYED_SECS, DELAYED_TICK_MS};
use broker_gateway::coap::{CoapCode, CoapGatewayHandler, CoapMessage};
use broker_observability::{Metrics, NodeReadiness, StatsStore};
use broker_protocol::{v5 as protocol_v5, QoS, Topic, TopicFilter};
use broker_router::{
    is_qos0_publish_out, split_shared_filter, strip_delayed_prefix, ConnTable, Router, Subscription,
};
use broker_rules::{BackpressurePolicy, BrokerSink, RuleEngine};
use broker_session::{
    InflightTrackOutcome, Qos2InboundEntry, QueuedMessage, SessionKey, SessionManager,
};
use broker_storage::stream::{DurableStreamStore, StreamConfig};
use broker_storage::OfflineQueueStore;
use broker_storage::{MemoryStore, RetainedStore};
#[cfg(test)]
use brokerlink::FramedTransport;
use brokerlink::{BrokerFrame, BrokerLinkTransport, OpCode};
use bytes::Bytes;
use clap::Parser;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Shared JWKS fixtures (B5-02): single-sourced in
/// `crates/broker-auth/src/jwks_test_support.rs` (test-only in both crates),
/// included here with `#[path]` so there is one copy of the
/// RSA/base64/PEM/server logic without shipping test material
/// in the production `broker-auth` library.
#[cfg(test)]
#[path = "../../broker-auth/src/jwks_test_support.rs"]
mod jwks_test_support;
/// Test harness for the removed link server (R3): drives kernel
/// frame logic over loopback sockets in tests only.
#[cfg(test)]
mod link_harness;
mod native;
mod source_runtime;
/// Test directory server (LDAP). Single-sourced in
/// `crates/broker-auth/src/test_directory_server.rs` (test-only in both
/// crates), included here with `#[path]` so there is one copy of the
/// wire server without shipping test material in any library.
#[cfg(test)]
#[path = "../../broker-auth/src/test_directory_server.rs"]
mod test_directory_server;
#[cfg(test)]
use tokio::net::TcpStream;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tracing::{debug, info, warn};

#[derive(Parser, Debug)]
#[command(
    name = "indramqtt",
    version,
    about = "IndraMQTT Distributed Broker Kernel"
)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:1883")]
    bind: String,

    /// Deprecated. Ignored. The kernel serves clients directly.
    #[arg(long, hide = true, default_value = "127.0.0.1:18883")]
    brokerlink_bind: String,

    /// Management REST API listen address. Empty disables the API
    /// (useful for test environments).
    #[arg(long, default_value = "127.0.0.1:18083")]
    api_bind: String,

    /// Directory holding kernel persistent state (`state.toml`).
    /// Created on boot when missing.
    #[arg(long, default_value = "./data")]
    data_dir: String,

    /// Directory holding operator configuration (`indra.toml`).
    /// Created on boot when missing.  A missing directory or a missing
    /// `indra.toml` starts on defaults.
    #[arg(long, default_value = broker_config::DEFAULT_CONFIG_DIR)]
    config_dir: String,

    /// Enterprise commercial license key for multi-node clustering (or via INDRA_LICENSE_KEY env).
    #[arg(long)]
    license_key: Option<String>,

    /// Trusted licence-signing keys (B2-03): a JSON file or a directory of
    /// JSON files of the form
    /// `{"keys": [{"kid": "...", "public_key_hex": "<SEC1 hex>"}]}`.
    /// Empty (the default) trusts nothing: enterprise licences are then
    /// rejected as unknown keys while community mode still boots. A
    /// missing or corrupt path fails boot loudly rather than silently
    /// running unlicensed.
    #[arg(long, default_value = "")]
    license_keys: String,

    /// Write the installation licence request to this path and exit, for
    /// sites with no dashboard. The file carries the cluster identity and
    /// is safe to email to sales.
    #[arg(long, default_value = "")]
    licence_request_out: String,

    /// Install the licence token read from this path and exit. Verified
    /// before any write, so a failed install never touches the stored
    /// licence.
    #[arg(long, default_value = "")]
    licence_install_file: String,

    /// Days before expiry that the approaching-expiry alarm and log
    /// warning start. Applies to the trial and to licensed expiry alike.
    #[arg(long, default_value_t = 30)]
    licence_expiry_warn_days: u64,

    /// Cluster seed nodes (e.g. 127.0.0.1:19883). Specifying enables distributed clustering.
    #[arg(long)]
    cluster_seeds: Option<String>,

    /// Cluster UDP/gossip bind address for distributed clustering (e.g. 127.0.0.1:19883).
    #[arg(long, default_value = "127.0.0.1:19883")]
    cluster_bind: String,

    /// Unique cluster node identity. Defaults to "indra-node-1".
    #[arg(long, default_value = "indra-node-1")]
    node_id: String,

    /// Accept MQTT connections without credentials while no users are
    /// configured. Ignored once users exist: a non-empty user store
    /// always rejects unauthenticated clients.
    #[arg(long)]
    allow_anonymous: bool,

    /// Per-subscriber QoS 0 egress backlog bound (D1-02). Each live
    /// connection queues at most this many QoS 0 frames; past it the
    /// oldest queued QoS 0 frame drops (counted via `egress_qos0_shed`,
    /// labelled by client). QoS 1/2 never shed. Default 1000 matches
    /// the detached offline bound and caps per-subscriber QoS 0 memory
    /// near 1,000 small frames while absorbing a ~1 s burst at 1k
    /// msg/s without drops.
    #[arg(long, default_value_t = broker_router::DEFAULT_QOS0_BACKLOG)]
    qos0_backlog: usize,

    /// Per-session QoS 1 inflight window bound (B4-01). Each live session
    /// tracks at most this many unacknowledged QoS 1 downlinks in the
    /// fast in-memory window; past it overflow spills into the bounded
    /// per-session spill buffer (still tracked for DUP redelivery).
    /// Default 100 caps per-session live unacked state near 100 small
    /// frames while absorbing a short ack stall without spilling.
    /// Schema home: `session.max_qos1_inflight`.
    #[arg(long)]
    qos1_inflight_window: Option<usize>,

    /// Per-session QoS 1 spill bound past the window (B4-01). Overflow
    /// beyond the window is still delivered live once and tracked here
    /// for oldest-first DUP replay; past window + spill the newest live
    /// delivery stays untracked (counted). Default 1000 (10x the window)
    /// absorbs roughly a 1 s burst at 1k msg/s from a stalled
    /// acknowledger while keeping per-session tracked memory bounded
    /// near 1,100 messages total.
    /// Schema home: `session.max_qos1_spill`.
    #[arg(long)]
    qos1_spill: Option<usize>,

    /// CoAP gateway UDP listen address (B1-04). Empty disables the
    /// gateway (the default): nothing listens, nothing is dispatched,
    /// and the crate stays out of the data path. Set explicitly
    /// (e.g. `127.0.0.1:5683`) to run the gateway: CoAP POST/PUT to
    /// `/ps/<topic>` publishes through the kernel router so ordinary
    /// MQTT subscribers receive it, GET reads the retained store, and
    /// GET with Observe registers a bounded observer notified on later
    /// publishes.
    #[arg(long, default_value = "")]
    coap_bind: String,

    /// Durable stream journal directory (B1-07). Empty (the default)
    /// disables journaling: publishes pay one `is_none` check and no disk
    /// I/O. Set to a directory (e.g. `<data-dir>/streams`) to journal
    /// every publish through a bounded channel drained by a background
    /// task that appends to [`DurableStreamStore`] with an fsync per
    /// record. Disabled by default so benchmarks and existing deployments
    /// see no new work on the fan-out path.
    #[arg(long, default_value = "")]
    stream_dir: String,

    /// Rule ingress spill directory (B4-07). Empty (the default) keeps
    /// today's memory-only `DropOldest` rule input: no new work, no new
    /// files. Set to a dedicated directory (e.g. `<data-dir>/rule-spill`)
    /// to build the rule engine with the `SpillToDisk` backpressure
    /// policy instead: overflow rule-ingress events append to a bounded
    /// on-disk buffer (1 MiB segments, 64 MiB total) and replay in order
    /// once the pressure eases, surviving a restart. A directory that
    /// cannot be opened fails boot loudly rather than silently running
    /// memory-only.
    /// Schema home: `rules_engine.spill_dir`.
    #[arg(long)]
    rule_spill_dir: Option<String>,

    /// Directory server URL for LDAP authentication (B2-01). Empty (the
    /// default) disables LDAP: CONNECT falls back to the local user
    /// store only. Set to `ldap://host:port` or `ldaps://host:port` to
    /// enable directory-backed authentication at CONNECT (service bind,
    /// search, then bind as the entry DN). TLS verification is on by
    /// default for `ldaps://`; point `--ldap-ca-cert` at a PEM CA file
    /// for private directories.
    #[arg(long, default_value = "")]
    ldap_url: String,

    /// Base DN under which directory users are searched.
    #[arg(long, default_value = "")]
    ldap_base_dn: String,

    /// Service account DN used for the initial bind plus search.
    #[arg(long, default_value = "")]
    ldap_bind_dn: String,

    /// Service account password for the initial bind.
    #[arg(long, default_value = "")]
    ldap_bind_password: String,

    /// User search filter with a `{username}` placeholder, escaped per
    /// RFC 4515 (default `(uid={username})`).
    #[arg(long, default_value = "(uid={username})")]
    ldap_user_filter: String,

    /// User-entry attribute listing directory groups (`memberOf`).
    #[arg(long, default_value = "memberOf")]
    ldap_group_attribute: String,

    /// Required group DN: empty disables the group check, otherwise the
    /// entry must list this DN to connect.
    #[arg(long, default_value = "")]
    ldap_required_group: String,

    /// Path to a PEM CA certificate for private directories (`ldaps://`).
    #[arg(long, default_value = "")]
    ldap_ca_cert: String,

    /// Keytab file for Kerberos authentication (B2-02). Empty (the
    /// default) disables Kerberos: CONNECT falls back to the local user
    /// store (plus LDAP when configured). Set to a MIT keytab v2 file
    /// holding the broker's service principal to enable GSSAPI-based
    /// authentication at CONNECT (SPNEGO NegTokenInit wrapping AP-REQ,
    /// or bare AP-REQ). A missing or unreadable keytab disables the
    /// mechanism explicitly with a clear log line and never accepts
    /// tokens.
    #[arg(long, default_value = "")]
    kerberos_keytab: String,

    /// Service principal name held in the keytab (for example
    /// `mqtt/broker.example.com@EXAMPLE.COM`). Must match the ticket's
    /// service name or CONNECT is refused.
    #[arg(long, default_value = "")]
    kerberos_service_principal: String,

    /// Expected service realm (for example `EXAMPLE.COM`). Empty disables
    /// the realm check on the service name.
    #[arg(long, default_value = "")]
    kerberos_realm: String,

    /// Comma-separated list of trusted client realms (for example
    /// `EXAMPLE.COM`). Empty allows any realm that verifies.
    #[arg(long, default_value = "")]
    kerberos_allowed_realms: String,

    /// Clock-skew allowance in seconds for ticket validity and
    /// authenticator timestamps (default 300, clamped 1..3600).
    #[arg(long, default_value_t = 300)]
    kerberos_clock_skew_secs: u64,

    /// Comma-separated `principal=role` mappings for verified Kerberos
    /// client principals (for example
    /// `alice@EXAMPLE.COM=publisher,bob@EXAMPLE.COM=subscriber`).
    /// Unmapped principals authenticate with role `user`. Empty (the
    /// default) leaves every principal at `user`.
    #[arg(long, default_value = "")]
    kerberos_role_map: String,

    /// Replay-cache bound for Kerberos authenticators (default 1024,
    /// clamped 16..8192). Bounds CONNECT-only memory: each entry is
    /// under 128 bytes, so the default holds under 128 KiB.
    /// Schema home: `kerberos.replay_max_entries`.
    #[arg(long)]
    kerberos_replay_max: Option<usize>,

    /// JWKS endpoint URL for JWT authentication (B5-02). Empty (the
    /// default) disables JWT: CONNECT falls back to the local user store
    /// (plus LDAP/Kerberos when configured). Set to an `https://` URL
    /// serving a `{"keys": [...]}` document to verify JWT-bearer clients
    /// at CONNECT (signature by `kid`, expiry, issuer, audience). A
    /// configured endpoint disables the anonymous open mode like a
    /// directory does, and any endpoint outage fails closed.
    /// Schema home: `jwks.url`.
    #[arg(long)]
    jwks_url: Option<String>,

    /// Expected JWT issuer (`iss` claim). Empty skips the issuer check
    /// (signature plus expiry only); set it in production so a token
    /// minted for another service is refused.
    /// Schema home: `jwks.issuer`.
    #[arg(long)]
    jwks_issuer: Option<String>,

    /// Expected JWT audience (`aud` claim). Empty skips the audience
    /// check; set it in production for the same reason as the issuer.
    /// Schema home: `jwks.audience`.
    #[arg(long)]
    jwks_audience: Option<String>,

    /// JWKS background refresh period in seconds (default 300: rotation
    /// propagates within five minutes; fast rotation is covered by the
    /// unknown-`kid` trigger, so the period only bounds TTL staleness).
    /// Clamped to 1..86400 at use.
    #[arg(long, default_value_t = 300)]
    jwks_refresh_period_secs: u64,

    /// JWKS fetch timeout in milliseconds (default 5000, clamped
    /// 100..60000): slow-endpoint tolerant while keeping a stalled
    /// accept bounded. Matches the directory 5 s ceiling.
    #[arg(long, default_value_t = 5000)]
    jwks_fetch_timeout_ms: u64,

    /// JWKS on-demand (unknown-`kid`) refresh timeout in milliseconds
    /// (default 5000, clamped 100..60000): covers one fetch plus the
    /// singleflight cache-poll window for a CONNECT with a fresh key.
    #[arg(long, default_value_t = 5000)]
    jwks_refresh_timeout_ms: u64,

    /// JWKS key-cache bound in entries (default 32, clamped 1..256):
    /// JWKS documents carry a handful of rotation keys, so 32 entries
    /// of about 2 KiB cap CONNECT-only key memory near 64 KiB. Past the
    /// bound the key set is sorted by `kid` and truncated to the bound,
    /// so the same document always caches the same keys.
    #[arg(long, default_value_t = broker_auth::JWKS_CACHE_MAX_KEYS)]
    jwks_cache_max_keys: usize,

    /// JWKS cache entry TTL in seconds (default 300, clamped 1..86400):
    /// equals the refresh period so the background task keeps entries
    /// fresh; expired entries force an on-demand refresh (fail closed).
    #[arg(long, default_value_t = 300)]
    jwks_cache_ttl_secs: u64,

    /// JWT clock-skew allowance in seconds for `exp`/`nbf` (default 60,
    /// clamped 0..3600): tolerates a minute of issuer/broker drift
    /// without accepting clearly expired tokens.
    #[arg(long, default_value_t = 60)]
    jwks_clock_skew_secs: u64,

    /// Path to a PEM CA certificate for private JWKS issuers. Empty
    /// uses the platform trust store.
    #[arg(long, default_value = "")]
    jwks_ca_cert: String,

    /// Skip TLS verification for the JWKS endpoint (loopback tests
    /// only, never production: without it any network peer could serve
    /// a forged key set).
    #[arg(long)]
    jwks_insecure_skip_verify: bool,
    /// PostgreSQL URL for database authentication (B5-03). Empty (the
    /// default) disables the PostgreSQL source: CONNECT pays one
    /// `is_some` branch. Set to `postgresql://user:pass@host:port/db`
    /// to enable credential and ACL lookups at CONNECT and on the
    /// publish path. An unreachable database fails closed.
    #[arg(long, default_value = "")]
    dbauth_postgres_url: String,

    /// MySQL URL for database authentication (B5-03). Empty disables
    /// the MySQL source. Set to `mysql://user:pass@host:port/db`.
    #[arg(long, default_value = "")]
    dbauth_mysql_url: String,

    /// Redis URL for database authentication (B5-03). Empty disables
    /// the Redis source. Set to `redis://:pass@host:port/db`.
    #[arg(long, default_value = "")]
    dbauth_redis_url: String,

    /// MongoDB URL for database authentication (B5-03). Empty disables
    /// the MongoDB source. Set to
    /// `mongodb://user:pass@host:port/db?authSource=admin`.
    #[arg(long, default_value = "")]
    dbauth_mongodb_url: String,

    /// Bound on concurrent database operations per source (B5-03,
    /// default 8: enough for CONNECT bursts, small enough to never
    /// overwhelm the database; a slow database exhausts permits and
    /// fails closed instead of stalling accepts).
    #[arg(long, default_value_t = broker_auth::DEFAULT_POOL_SIZE)]
    dbauth_pool_size: usize,

    /// Database connect timeout in milliseconds (B5-03, default 3000:
    /// covers TCP connect plus container-start jitter while failing
    /// closed fast enough to not stall accepts).
    #[arg(long, default_value_t = broker_auth::DEFAULT_CONNECT_TIMEOUT_MS)]
    dbauth_connect_timeout_ms: u64,

    /// Database per-operation timeout in milliseconds (B5-03, default
    /// 3000: covers one lookup round-trip).
    #[arg(long, default_value_t = broker_auth::DEFAULT_READ_TIMEOUT_MS)]
    dbauth_read_timeout_ms: u64,

    /// Database verdict-cache bound in entries per source and kind
    /// (B5-03, default 1024: under one megabyte total; evicts oldest
    /// first).
    #[arg(long, default_value_t = broker_auth::DEFAULT_CACHE_MAX_ENTRIES)]
    dbauth_cache_size: usize,

    /// Database verdict-cache TTL in seconds (B5-03, default 60: bounds
    /// a stale ACL after a change to one minute; lower for faster
    /// revocation).
    #[arg(long, default_value_t = broker_auth::DEFAULT_CACHE_TTL_SECS)]
    dbauth_cache_ttl_secs: u64,

    /// HTTP webhook verdict endpoint (B5-04). Empty (the default)
    /// disables the webhook: CONNECT falls back to the local user store
    /// (plus LDAP/Kerberos when configured) and publishes consult the
    /// local ACL only. Set to the verdict service base URL (for example
    /// `http://127.0.0.1:9090`) to authenticate every credentialed
    /// CONNECT and authorize every publish against it; a missing, slow
    /// or disagreeing endpoint fails closed with a clear log line and
    /// never grants access.
    #[arg(long, default_value = "")]
    webhook_url: String,

    /// Bound on concurrent webhook verdict requests (B5-04). At most
    /// this many CONNECT/publish checks are in flight against the
    /// endpoint at once; past it checks wait up to the request timeout
    /// and then fail closed. Default 8 absorbs a CONNECT burst while a
    /// slow endpoint cannot spawn unbounded work.
    #[arg(long, default_value_t = broker_auth::WEBHOOK_POOL_SIZE)]
    webhook_pool_size: usize,

    /// Per-request webhook timeout in milliseconds (B5-04). Covers the
    /// whole verdict round-trip; past it the check fails closed. Default
    /// 2000 keeps the worst case a CONNECT or publish waits bounded
    /// while tolerating a loaded loopback verdict service.
    #[arg(long, default_value_t = broker_auth::WEBHOOK_REQUEST_TIMEOUT_MS)]
    webhook_timeout_ms: u64,

    /// Consecutive webhook failures before the circuit opens (B5-04).
    /// An explicit deny verdict is not a failure. Default 5 tolerates
    /// one transient while tripping fast under a real outage.
    #[arg(long, default_value_t = broker_auth::WEBHOOK_BREAKER_THRESHOLD)]
    webhook_breaker_threshold: u32,

    /// Open-circuit reset timeout in milliseconds (B5-04). After this
    /// long one half-open probe is admitted; its outcome closes or
    /// re-opens the circuit. Default 30000 gives a dead endpoint time
    /// to recover without hammering it.
    #[arg(long, default_value_t = broker_auth::WEBHOOK_BREAKER_RESET_MS)]
    webhook_breaker_reset_ms: u64,

    /// Bound on cached webhook verdicts (B5-04). Expired entries evict
    /// lazily; when full past the sweep the oldest-inserted entry goes.
    /// Default 1024 holds under 256 KiB of small entries.
    #[arg(long, default_value_t = broker_auth::WEBHOOK_CACHE_MAX_ENTRIES)]
    webhook_cache_size: usize,

    /// Webhook verdict time-to-live in seconds (B5-04). `0` disables the
    /// cache (every check asks the endpoint). Default 60 removes
    /// per-packet HTTP cost for steady publishers while capping the
    /// stale-verdict window at one minute.
    #[arg(long, default_value_t = broker_auth::WEBHOOK_CACHE_TTL_SECS)]
    webhook_cache_ttl_secs: u64,

    /// Upper bound for `$delayed` deferrals in seconds (B4-03). Past it
    /// the request is dropped with a warning instead of pinning a wheel
    /// slot and a file row indefinitely. Default one day: longer
    /// deferrals pin memory for no realistic device schedule, and the
    /// previous per-message sleep used the same one-day ceiling so
    /// existing publishers see the same limit.
    #[arg(long, default_value_t = DEFAULT_MAX_DELAYED_SECS)]
    delayed_max_secs: u64,

    /// Inbound topic-alias maximum advertised in CONNACK (B4-05, T-92):
    /// the largest alias a client may use when publishing. Default 10
    /// covers typical small-device alias use while capping
    /// per-connection alias memory near 10 topic strings; 0 disables
    /// inbound aliases (alias-carrying publishes are rejected).
    /// Schema home: `session.topic_alias_maximum`.
    #[arg(long)]
    topic_alias_maximum: Option<u16>,

    /// Largest MQTT packet in bytes. Sets the read limit and the WS
    /// frame cap. Advertised in CONNACK. (P2) Default 1048576 matches
    /// the documented server limit and bounds one connection near
    /// 1 MiB. Range is 1..=268435455, the wire ceiling.
    /// Schema home: `session.max_packet_size`.
    #[arg(long)]
    max_packet_size: Option<u32>,

    /// Monitor sampling interval in seconds (F1-02, T-72): how often the
    /// background sampler copies live counters into the bounded monitor
    /// ring behind `GET /monitor`. Default 10 keeps four hours of history
    /// in the 1440-point ring (one point holds nine integers, well
    /// under one megabyte) while waking the timer six times a minute;
    /// each tick does a handful of atomic loads plus one short ring lock.
    /// Clamped to 1..3600 at boot; 0 records a single boot sample and
    /// disables the periodic tick (tests and one-shot runs).
    #[arg(long, default_value_t = DEFAULT_MONITOR_SAMPLE_SECS)]
    monitor_sample_secs: u64,

    /// Require a client certificate on the TLS listener (TM-08). Off by
    /// default keeps current handshakes unchanged. On requests a client
    /// certificate and refuses the connect when none is present.
    /// Schema home: `listeners.tls.verify_client_cert`.
    #[arg(long)]
    tls_verify_client_cert: Option<bool>,

    /// Require a client certificate on the secure WebSocket listener
    /// (TM-08). Off by default mirrors the TLS listener. On requests a
    /// client certificate and refuses the connect when none is present.
    /// Schema home: `listeners.wss.verify_client_cert`.
    #[arg(long)]
    wss_verify_client_cert: Option<bool>,

    /// Validate the layered configuration and exit. Print `ok` on
    /// success. Read `indra.toml`, `conf.d` and the `INDRA_` variables.
    #[arg(long)]
    check_config: bool,
}

/// Default monitor sampling interval in seconds (F1-02, T-72). Ten seconds
/// keeps four hours of history in the 1440-point ring while waking the
/// sampler six times a minute; each tick reads atomics only, so the cost
/// stays far below the per-message path.
const DEFAULT_MONITOR_SAMPLE_SECS: u64 = 10;

/// Server Receive Maximum advertised in v5 CONNACK properties (X1-02):
/// 65535, the wire maximum. Reason: the kernel enforces no inbound
/// unacked-delivery cap today, so it advertises no throttling; a client
/// sending within the wire format is never refused for quota. The
/// CONNACK property carries this value explicitly so a v5 client
/// observes the negotiated limit instead of an absent property.
const SERVER_V5_RECEIVE_MAXIMUM: u16 = u16::MAX;

/// Largest SessionBinding v5 section in bytes (X1-02, mirrors
/// [`MAX_V5_SECTION_LEN`] on the bind path): one section must leave
/// room for the 12-byte binding head inside the 65535-byte BrokerLink
/// meta cap; 4 KiB covers a reason string plus realistic echoed user
/// properties. At most [`MAX_V5_BIND_USERS`] user properties per
/// binding, each key/value at most [`MAX_V5_BIND_STRING`] bytes
/// (bounds per-bind memory on the handshake path).
const MAX_BINDING_V5_SECTION_LEN: usize = 4096;

/// v5 egress refusal: quota exceeded (client Receive Maximum reached).
/// The frame is shed (counted via `inflight_dropped`, never silent);
/// never sent above the client's unacked cap.
const V5_RC_QUOTA_EXCEEDED: u8 = 0x97;

/// v5 egress refusal: packet too large (client Maximum Packet Size).
/// The frame is shed (counted via `messages_dropped`, never silent);
/// never sent above the client's size cap.
const V5_RC_PACKET_TOO_LARGE: u8 = 0x95;

/// v5 bind refusal: protocol error (malformed v5 properties, e.g. a
/// Receive Maximum of 0 which the wire forbids). Fail closed with no
/// session created.
const V5_RC_PROTOCOL_ERROR: u8 = 0x82;

/// v5 bind refusal: empty client id without clean start. Reason: the
/// specification forbids that combination (section 3.1.3.1). Fail
/// closed with no session created.
const V5_RC_CLIENT_ID_REJECTED: u8 = 0x85;

/// v5 quota refusal on CONNECT (too many connections for the user).
/// Version-4 binds keep the existing `0x8B` byte unchanged; version-5
/// binds carry the documented quota code below with a reason string.
const V5_RC_CONNECT_QUOTA: u8 = 0x97;

/// DISCONNECT reason for the orderly stop.
/// Reason: MQTT 5 code 0x8B means Server shutting down.
const SHUTDOWN_DISCONNECT_RC: u8 = 0x8B;

/// Longest wait for clients to close during the stop.
/// Reason: the removal plan bounds the orderly stop at ten seconds.
const SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(10);

/// Longest wait to drain connector buffers during the stop.
/// Reason: a down target must not hold the process open.
const SHUTDOWN_CONNECTOR_FLUSH: std::time::Duration = std::time::Duration::from_secs(5);

/// Server-side reason string for one CONNACK return code (X1-02).
/// Our own wording, written for this broker (never copied): success
/// and identifier-rejected omit the property (`None`) to keep the
/// handshake small; every other failure carries a short human-readable
/// cause the edge places in CONNACK property 28.
fn v5_reason_string(rc: u8) -> Option<&'static str> {
    match rc {
        0 => None,
        2 => None,
        0x86 => Some("Bad username or password"),
        0x87 => Some("Not authorized"),
        0x8A => Some("Client banned"),
        V5_RC_PROTOCOL_ERROR => Some("Protocol error: malformed request"),
        0x85 => Some("Client identifier not valid"),
        // X1-02: `V5_RC_CONNECT_QUOTA` and `V5_RC_QUOTA_EXCEEDED` share
        // code 0x97 (CONNECT quota and egress window share the wire
        // code; the reason string covers both), plus the legacy 0x8B byte.
        V5_RC_QUOTA_EXCEEDED | 0x8B => Some("Quota exceeded: too many connections"),
        V5_RC_PACKET_TOO_LARGE => Some("Packet too large for the client maximum"),
        _ => Some("Connection refused"),
    }
}

/// v5 CONNACK properties carried in one `SessionBinding` reply
/// (X1-02): the granted session expiry plus the server limits for the
/// CONNACK, with the failure reason string and the echoed CONNECT
/// user properties. `reason` is empty on success; `user_properties`
/// ride only on failure paths (echoed from CONNECT), never on
/// success. Counts are bounded by [`MAX_V5_BIND_USERS`] /
/// [`MAX_V5_BIND_STRING`] at encode time (fail closed: oversize
/// input refuses the whole bind, never a truncated CONNACK).
#[derive(Debug, Clone, Default)]
struct SessionBindingV5 {
    granted_expiry: u32,
    server_recv_max: u16,
    server_max_pkt: u32,
    reason: String,
    user_properties: Vec<(String, String)>,
}

/// Success v5 properties for one accepted bind (X1-02): the granted
/// session expiry (the session's stored interval, echoed so the
/// client observes what the kernel retained), the server limits, no
/// reason string and no user properties.
fn success_binding_v5(granted_expiry: u32, server_max_pkt: u32) -> SessionBindingV5 {
    SessionBindingV5 {
        granted_expiry,
        server_recv_max: SERVER_V5_RECEIVE_MAXIMUM,
        server_max_pkt,
        reason: String::new(),
        user_properties: Vec::new(),
    }
}

/// Failure v5 properties for one refused bind (X1-02): granted expiry
/// 0 (no session, so the edge omits the property), the server limits,
/// the server-side reason string for `rc`, and the CONNECT user
/// properties echoed back (bounded; over-bound input is dropped to the
/// bound, never a truncated CONNACK — the refusal itself still goes
/// out with the reason string).
fn failure_binding_v5(req: &BindRequest, rc: u8, server_max_pkt: u32) -> SessionBindingV5 {
    let reason = v5_reason_string(rc)
        .unwrap_or("Connection refused")
        .to_string();
    let mut users: Vec<(String, String)> = Vec::new();
    if let Some(map) = req.user_properties.as_ref() {
        let mut pairs: Vec<(&String, &String)> = map.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        for (k, v) in pairs.into_iter().take(MAX_V5_BIND_USERS) {
            if k.len() > MAX_V5_BIND_STRING || v.len() > MAX_V5_BIND_STRING || k.is_empty() {
                continue;
            }
            users.push((k.clone(), v.clone()));
        }
    }
    SessionBindingV5 {
        granted_expiry: 0,
        server_recv_max: SERVER_V5_RECEIVE_MAXIMUM,
        server_max_pkt,
        reason,
        user_properties: users,
    }
}

/// True when the raw `BindConnection` metadata carries Flags bit 2
/// (X1-01 v5 section present), i.e. the peer negotiated version 5 and
/// a malformed bind must answer with the v5 protocol-error code
/// rather than the 3.1.1 identifier-rejected code. Anything too short
/// for a head reads as version 4 (fail closed to the old code, never
/// a guessed version).
/// Test harness only: production binds carry a decoded request.
#[cfg(test)]
fn bind_looks_v5(meta: &[u8]) -> bool {
    if meta.len() < 5 {
        return false;
    }
    let id_len = u16::from_be_bytes([meta[0], meta[1]]) as usize;
    if meta.len() < 2 + id_len + 1 + 2 {
        return false;
    }
    meta[2 + id_len] & 0x04 != 0
}

/// Check one live v5 subscriber against its CONNECT caps before
/// sending (X1-02, delivery event only): `None` means send; `Some(rc)`
/// means shed the frame for this subscriber (counted by the caller,
/// never silent, never sent). Version-4 sessions always send
/// (`None`). Receive Maximum caps QoS 1/2 only (QoS 0 has no
/// unacked state and is never counted against the window); Maximum
/// Packet Size caps every QoS with the
/// [`broker_session::V5_EGRESS_OVERHEAD_BYTES`] estimate. Both reads
/// are relaxed atomic loads; uncapped sessions (defaults) return
/// after two loads with no lock.
fn v5_encoded_packet_too_large(session: &broker_session::Session, frame: &BrokerFrame) -> bool {
    if session.protocol_version() != 5 {
        return false;
    }
    let cap = session.client_max_packet_size();
    if cap == 0 {
        return false;
    }
    native::encoded_publish_out_len(frame, 5).is_some_and(|len| len > cap as usize)
}

fn v5_packet_size_refused(
    session: &broker_session::Session,
    topic_len: usize,
    payload_len: usize,
) -> Option<u8> {
    if session.protocol_version() != 5 {
        return None;
    }
    let cap = session.client_max_packet_size();
    if cap != 0 {
        let estimated = topic_len
            .saturating_add(payload_len)
            .saturating_add(broker_session::V5_EGRESS_OVERHEAD_BYTES);
        if estimated > cap as usize {
            return Some(V5_RC_PACKET_TOO_LARGE);
        }
    }
    None
}

fn v5_egress_refused(
    session: &broker_session::Session,
    effective_qos: u8,
    topic_len: usize,
    payload_len: usize,
) -> Option<u8> {
    if session.protocol_version() != 5 {
        return None;
    }
    if effective_qos == 1 || effective_qos == 2 {
        let cap = usize::from(session.client_receive_maximum().max(1));
        if session.unacked_on_conn_len() >= cap {
            return Some(V5_RC_QUOTA_EXCEEDED);
        }
    }
    v5_packet_size_refused(session, topic_len, payload_len)
}

/// Map an inbound frame to its synchronous reply, if any.
///
/// Contracts:
/// * `Ping` is answered immediately with a `Pong` carrying the identical
///   `conn_id` and `sequence_no`.
/// * `BindConnection` metadata is `ClientIdLen:16be | ClientId (UTF-8)
///   | Flags:8 (bit 0 = clean_start) | Keepalive:16be`, optionally
///   followed by credentials, a peer section and the B4-05 trailing
///   `ClientAliasMax:16be`. The canonical session is resolved via
///   `SessionManager::get_or_create_v5_in_tenant` (X1-02: clean-start
///   plus Session Expiry; version-4 binds carry defaults and behave as
///   before) and answered with `SessionBinding`
///   metadata `SessionId:64be | Present:8 | ReturnCode:8 |
///   TopicAliasMaximum:16be` (RC 0 = accepted, 2 = identifier rejected),
///   plus the X1-02 trailing v5 section on version-5 binds (granted
///   expiry, server limits, reason string, echoed user properties).
///   Replies always mirror the request `conn_id` and `sequence_no`.
/// * All other opcodes have no synchronous reply yet and return `None`.
///
/// Test harness only: production answers through the socket codecs.
#[cfg(test)]
fn reply_for_frame(frame: &BrokerFrame, sessions: &SessionManager) -> Option<BrokerFrame> {
    match frame.header.opcode {
        OpCode::Ping => Some(BrokerFrame::pong(
            frame.header.conn_id,
            frame.header.sequence_no,
        )),
        OpCode::BindConnection => Some(bind_connection_reply(frame, sessions)),
        _ => None,
    }
}

/// Resolve one `BindConnection` frame into its `SessionBinding` reply.
///
/// Every `BindConnection` yields exactly one reply so the BEAM edge never
/// hangs waiting for CONNACK parameters: malformed metadata (or a
/// non-UTF-8 client id) is answered with return code 2, mirroring the
/// MQTT 3.1.1 CONNACK "identifier rejected" code. Version-5 binds with
/// a malformed v5 section answer with the v5 protocol-error code
/// (`0x82`) plus a reason string instead, so a v5 client observes the
/// failure honestly; version-4 bytes are unchanged.
/// Test harness only: production binds carry a decoded request.
#[cfg(test)]
fn bind_connection_reply(frame: &BrokerFrame, sessions: &SessionManager) -> BrokerFrame {
    // P2: the configured server packet limit rides every v5 reply.
    let server_max_pkt = sessions.max_packet_size();
    let (session_id, present, return_code) = match decode_bind_meta(&frame.metadata) {
        Ok(req) => {
            // F1-01: validate the CONNECT last will before touching
            // session state, so an unusable will section rejects the bind
            // with no session created (fail closed, like malformed meta).
            let will = match will_from_bind(&req) {
                Ok(will) => will,
                Err(_) => {
                    if req.protocol_version == 5 {
                        let v5 = failure_binding_v5(&req, V5_RC_PROTOCOL_ERROR, server_max_pkt);
                        return session_binding_reply_with_v5(
                            frame,
                            0,
                            false,
                            V5_RC_PROTOCOL_ERROR,
                            sessions.max_topic_alias(),
                            &v5,
                        );
                    }
                    return refusal_for_req(
                        frame,
                        &req,
                        2u8,
                        sessions.max_topic_alias(),
                        server_max_pkt,
                    );
                }
            };
            // An empty client id without a clean start is refused
            // with no session created. Reason: the specification
            // forbids it ([MQTT-3.1.3-8] for 3.1.1, section 3.1.3.1
            // for v5 with code 0x85). An empty id with a clean start
            // stays accepted, as before.
            if req.client_id.is_empty() && !req.clean_start {
                let rc = if req.protocol_version == 5 {
                    V5_RC_CLIENT_ID_REJECTED
                } else {
                    2u8
                };
                return refusal_for_req(
                    frame,
                    &req,
                    rc,
                    sessions.max_topic_alias(),
                    server_max_pkt,
                );
            }
            // X1-02: a Receive Maximum of 0 is a protocol error on the
            // wire (the client would accept no QoS 1/2 downlink at
            // all). Fail closed with no session created.
            // TODO(parity): the specification's exact CONNACK code for
            // a zero Receive Maximum is open (protocol error vs quota
            // exceeded); the rulebook does not decide it. Current
            // choice is the conservative protocol-error refusal.
            if req.protocol_version == 5 && req.receive_maximum == 0 {
                let v5 = failure_binding_v5(&req, V5_RC_PROTOCOL_ERROR, server_max_pkt);
                return session_binding_reply_with_v5(
                    frame,
                    0,
                    false,
                    V5_RC_PROTOCOL_ERROR,
                    sessions.max_topic_alias(),
                    &v5,
                );
            }
            // X1-02: session resolution honours clean-start plus
            // Session Expiry (expiry 0 drops at disconnect, nonzero
            // retains past reconnect within the interval, absent
            // defaults to 0 with its reason on the session type).
            // Both versions resolve here: version-4 binds carry the
            // documented defaults (4, 0, wire-max, 0), so a pure 3.1.1
            // flow behaves bit for bit as before, while a reconnect
            // in either version drops an elapsed v5 husk instead of
            // resuming it (present=false).
            let (session, present, _) = sessions.get_or_create_v5_in_tenant(
                broker_session::tenant::DEFAULT_TENANT_ID,
                &req.client_id,
                req.clean_start,
                req.protocol_version,
                req.session_expiry,
                req.receive_maximum,
                req.max_packet_size,
            );
            // The synchronous reply path owns no router: an
            // expired-drop replacement starts clean (fresh object,
            // empty mirror), so no router sweep is needed here.
            // The live `apply_bind` path sweeps the owning
            // tenant's router copies from the returned filters.
            // Connection != Session: pin this edge connection to the session
            // so Unbind can later verify ownership before detaching.
            // Indexed (PERF-04): keeps conn_id -> client_id O(1).
            sessions.bind_session(&session, frame.header.conn_id);
            *session.keepalive_secs.write() = req.keepalive_secs;
            // B4-05: a new connection renegotiates aliases. The inbound
            // bound is the manager default (advertised below in CONNACK);
            // the outbound bound is the client's CONNECT maximum (0 when
            // the bind carries none). Tables start empty.
            session.clear_aliases();
            session.set_inbound_alias_max(sessions.max_topic_alias());
            session.set_outbound_alias_max(req.client_alias_max);
            observe_bind_v5(&req);
            // F1-01: the CONNECT last will rides the bind (edge forwards
            // what CONNECT carried). Stored on the session where the
            // disconnect decision lives; `None` clears a previous will.
            session.set_last_will(will);
            if req.protocol_version == 5 {
                let granted = session.session_expiry();
                return session_binding_reply_with_v5(
                    frame,
                    session.id.0,
                    present,
                    0u8,
                    sessions.max_topic_alias(),
                    &success_binding_v5(granted, server_max_pkt),
                );
            }
            (session.id.0, present, 0u8)
        }
        Err(_) => {
            if bind_looks_v5(&frame.metadata) {
                let v5 = SessionBindingV5 {
                    granted_expiry: 0,
                    server_recv_max: SERVER_V5_RECEIVE_MAXIMUM,
                    server_max_pkt,
                    reason: v5_reason_string(V5_RC_PROTOCOL_ERROR)
                        .unwrap_or("Connection refused")
                        .to_string(),
                    user_properties: Vec::new(),
                };
                return session_binding_reply_with_v5(
                    frame,
                    0,
                    false,
                    V5_RC_PROTOCOL_ERROR,
                    sessions.max_topic_alias(),
                    &v5,
                );
            }
            (0u64, false, 2u8)
        }
    };

    session_binding_reply(
        frame,
        session_id,
        present,
        return_code,
        sessions.max_topic_alias(),
    )
}

/// Build one `SessionBinding` reply frame.
///
/// Metadata is `SessionId:64be | Present:8 | ReturnCode:8 |
/// TopicAliasMaximum:16be` (B4-05): the trailing alias maximum is the
/// kernel inbound bound advertised for CONNACK negotiation. Always 12
/// bytes; the edge accepts 10-byte (pre-alias) bindings as maximum 0.
fn session_binding_reply(
    frame: &BrokerFrame,
    session_id: u64,
    present: bool,
    return_code: u8,
    alias_max: u16,
) -> BrokerFrame {
    let mut meta = Vec::with_capacity(12);
    meta.extend_from_slice(&session_id.to_be_bytes());
    meta.push(u8::from(present));
    meta.push(return_code);
    meta.extend_from_slice(&protocol_v5::encode_alias_maximum(alias_max));

    BrokerFrame::new(
        OpCode::SessionBinding,
        frame.header.conn_id,
        frame.header.sequence_no,
        Bytes::from(meta),
        Bytes::new(),
    )
    .expect("SessionBinding reply within size bounds")
}

/// Build one v5 `SessionBinding` reply frame (X1-02).
///
/// Metadata is the 12-byte head (`SessionId:64be | Present:8 |
/// ReturnCode:8 | TopicAliasMaximum:16be`, identical to
/// [`session_binding_reply`]) followed by `V5Payload | V5Len:16be`
/// where `V5Payload` is `ServerRecvMax:16be | ServerMaxPkt:32be |
/// GrantedExpiry:32be | ReasonLen:16be | Reason |
/// UserCount:16be | (KeyLen:16be | Key | ValLen:16be | Val)*`.
/// The trailing length rides last so the edge strips the section
/// deterministically from the end; the head then parses exactly like
/// a 12-byte binding. Version-4 binds never use this form (they stay
/// byte-identical 12-byte replies). Every length is bounded
/// ([`MAX_BINDING_V5_SECTION_LEN`], [`MAX_V5_BIND_USERS`],
/// [`MAX_V5_BIND_STRING`]); over-bound input fails the encode and the
/// caller falls back to the head-only 12-byte reply with the same
/// return code (fail closed: the refusal still goes out, only the
/// properties are dropped, counted in the refusal log).
fn session_binding_reply_with_v5(
    frame: &BrokerFrame,
    session_id: u64,
    present: bool,
    return_code: u8,
    alias_max: u16,
    v5: &SessionBindingV5,
) -> BrokerFrame {
    let mut meta = Vec::with_capacity(12);
    meta.extend_from_slice(&session_id.to_be_bytes());
    meta.push(u8::from(present));
    meta.push(return_code);
    meta.extend_from_slice(&protocol_v5::encode_alias_maximum(alias_max));
    if let Some(payload) = encode_binding_v5_payload(v5) {
        meta.extend_from_slice(&payload);
        meta.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    }
    BrokerFrame::new(
        OpCode::SessionBinding,
        frame.header.conn_id,
        frame.header.sequence_no,
        Bytes::from(meta),
        Bytes::new(),
    )
    .expect("SessionBinding reply within size bounds")
}

/// Encode one binding v5 payload, or `None` when over bound (the
/// caller falls back to the head-only reply with the same return
/// code). Reason and user properties are UTF-8 `String`s already
/// validated at the bind edge; lengths are re-checked here (fail
/// closed, never a truncated section).
fn encode_binding_v5_payload(v5: &SessionBindingV5) -> Option<Vec<u8>> {
    if v5.reason.len() > MAX_V5_BIND_STRING || v5.user_properties.len() > MAX_V5_BIND_USERS {
        return None;
    }
    for (k, v) in &v5.user_properties {
        if k.is_empty() || k.len() > MAX_V5_BIND_STRING || v.len() > MAX_V5_BIND_STRING {
            return None;
        }
    }
    let mut payload = Vec::new();
    payload.extend_from_slice(&v5.server_recv_max.to_be_bytes());
    payload.extend_from_slice(&v5.server_max_pkt.to_be_bytes());
    payload.extend_from_slice(&v5.granted_expiry.to_be_bytes());
    payload.extend_from_slice(&(v5.reason.len() as u16).to_be_bytes());
    payload.extend_from_slice(v5.reason.as_bytes());
    payload.extend_from_slice(&(v5.user_properties.len() as u16).to_be_bytes());
    for (k, v) in &v5.user_properties {
        payload.extend_from_slice(&(k.len() as u16).to_be_bytes());
        payload.extend_from_slice(k.as_bytes());
        payload.extend_from_slice(&(v.len() as u16).to_be_bytes());
        payload.extend_from_slice(v.as_bytes());
    }
    if payload.len() > MAX_BINDING_V5_SECTION_LEN {
        return None;
    }
    Some(payload)
}

/// Decode one binding v5 payload back into its fields (tests and any
/// future kernel read path; the edge decodes the same layout in
/// `indra_brokerlink`). `None` on any truncation or over-bound value.
fn decode_binding_v5_payload(payload: &[u8]) -> Option<SessionBindingV5> {
    if payload.len() < 2 + 4 + 4 + 2 + 2 {
        return None;
    }
    let server_recv_max = u16::from_be_bytes([payload[0], payload[1]]);
    let server_max_pkt = u32::from_be_bytes([payload[2], payload[3], payload[4], payload[5]]);
    let granted_expiry = u32::from_be_bytes([payload[6], payload[7], payload[8], payload[9]]);
    let reason_len = u16::from_be_bytes([payload[10], payload[11]]) as usize;
    if reason_len > MAX_V5_BIND_STRING || payload.len() < 14 + reason_len {
        return None;
    }
    let reason = std::str::from_utf8(&payload[12..12 + reason_len])
        .ok()?
        .to_string();
    let base = 12 + reason_len;
    let user_count = u16::from_be_bytes([payload[base], payload[base + 1]]) as usize;
    if user_count > MAX_V5_BIND_USERS {
        return None;
    }
    let mut cursor = &payload[base + 2..];
    let mut user_properties = Vec::with_capacity(user_count);
    for _ in 0..user_count {
        let (key, tail) = take_binding_v5_string(cursor)?;
        let (value, tail) = take_binding_v5_string(tail)?;
        if key.is_empty() {
            return None;
        }
        user_properties.push((key, value));
        cursor = tail;
    }
    if !cursor.is_empty() {
        return None;
    }
    Some(SessionBindingV5 {
        granted_expiry,
        server_recv_max,
        server_max_pkt,
        reason,
        user_properties,
    })
}

/// Take one length-prefixed string off the front of a binding v5
/// cursor. Overlong, non-UTF-8 or truncated fails the section.
fn take_binding_v5_string(cursor: &[u8]) -> Option<(String, &[u8])> {
    if cursor.len() < 2 {
        return None;
    }
    let len = u16::from_be_bytes([cursor[0], cursor[1]]) as usize;
    if len > MAX_V5_BIND_STRING || cursor.len() < 2 + len {
        return None;
    }
    let text = std::str::from_utf8(&cursor[2..2 + len]).ok()?;
    Some((text.to_string(), &cursor[2 + len..]))
}

/// Strip and decode the trailing v5 section of a `SessionBinding`
/// reply, if present. Returns `(session_id, present, rc, alias_max,
/// v5)` with `v5 == None` for 10/12-byte bindings (pre-v5 or
/// version-4 replies). Used by tests; the edge implements the same
/// layout in `indra_brokerlink:decode_session_binding_meta/1`.
fn decode_session_binding_full(
    meta: &[u8],
) -> Option<(u64, bool, u8, u16, Option<SessionBindingV5>)> {
    if meta.len() == 10 || meta.len() == 12 {
        if meta.len() < 10 {
            return None;
        }
        let session_id = u64::from_be_bytes(meta[0..8].try_into().ok()?);
        let present = match meta[8] {
            0 => false,
            1 => true,
            _ => return None,
        };
        let alias_max = if meta.len() == 12 {
            protocol_v5::decode_alias_maximum(&meta[10..12]).unwrap_or(protocol_v5::NO_TOPIC_ALIAS)
        } else {
            protocol_v5::NO_TOPIC_ALIAS
        };
        return Some((session_id, present, meta[9], alias_max, None));
    }
    if meta.len() < 12 + 2 {
        return None;
    }
    let session_id = u64::from_be_bytes(meta[0..8].try_into().ok()?);
    let present = match meta[8] {
        0 => false,
        1 => true,
        _ => return None,
    };
    let rc = meta[9];
    let alias_max =
        protocol_v5::decode_alias_maximum(&meta[10..12]).unwrap_or(protocol_v5::NO_TOPIC_ALIAS);
    let v5_len = u16::from_be_bytes([meta[meta.len() - 2], meta[meta.len() - 1]]) as usize;
    if v5_len > MAX_BINDING_V5_SECTION_LEN || meta.len() < 12 + v5_len + 2 {
        return None;
    }
    let start = meta.len() - 2 - v5_len;
    if start != 12 {
        return None;
    }
    let v5 = decode_binding_v5_payload(&meta[12..12 + v5_len])?;
    Some((session_id, present, rc, alias_max, Some(v5)))
}

/// Refusal for one decoded bind (X1-02): version-4 binds get the
/// head-only 12-byte reply with `rc` (bytes unchanged); version-5
/// binds get the extended reply with the server-side reason string
/// and the echoed CONNECT user properties on top of the same `rc`.
/// CONNECT-only, never on the delivery path.
fn refusal_for_req(
    frame: &BrokerFrame,
    req: &BindRequest,
    rc: u8,
    alias_max: u16,
    server_max_pkt: u32,
) -> BrokerFrame {
    if req.protocol_version == 5 {
        let v5 = failure_binding_v5(req, rc, server_max_pkt);
        session_binding_reply_with_v5(frame, 0, false, rc, alias_max, &v5)
    } else {
        session_binding_reply(frame, 0, false, rc, alias_max)
    }
}

/// Verify one JWT-shaped password against the live JWKS endpoint (B5-02).
///
/// Shared by both CONNECT arms (populated and empty local store) so the
/// endpoint stays the authority either way: real HTTPS fetch by `kid`,
/// never a bypass. Any endpoint problem fails closed. Returns the
/// verified subject (`None` when the token carries no `sub`) on success;
/// every failure increments the auth-failure metric and returns `Err`,
/// and the caller replies `0x86` with no session. CONNECT-only, never on
/// the delivery path.
async fn verify_jwks_connect(
    shared: &Shared,
    tenant: &str,
    client_id: &str,
    password: Option<&[u8]>,
) -> Result<Option<String>, ()> {
    let Some(jwks) = shared.jwks.as_ref() else {
        shared.metrics.inc_auth_failures();
        return Err(());
    };
    let Some(token_bytes) = password else {
        shared.metrics.inc_auth_failures();
        return Err(());
    };
    let Ok(token) = std::str::from_utf8(token_bytes) else {
        shared.metrics.inc_auth_failures();
        return Err(());
    };
    // Tenant scope (MT-06): the JWT lookup carries the bind's tenant;
    // only the built-in table holds per-tenant state in this wave, so a
    // non-default tenant fails closed inside the verifier (deferred).
    match jwks.verify_token_in_tenant(tenant, client_id, token).await {
        Ok(verified) => {
            // Wire every verified field live: subject, issuer, key id and
            // expiry (the token itself is never logged). Audit-logged for
            // SSO traceability; CONNECT-only.
            tracing::info!(
                subject = %verified.subject,
                issuer = %verified.issuer,
                key_id = %verified.key_id,
                expires_at = verified.expires_at,
                "JWT CONNECT accepted"
            );
            if verified.subject.is_empty() {
                Ok(None)
            } else {
                Ok(Some(verified.subject))
            }
        }
        Err(_) => {
            shared.metrics.inc_auth_failures();
            Err(())
        }
    }
}

/// One successful chain verdict for a credentialed CONNECT.
struct ChainAllow {
    /// Id of the entry that allowed. Drives per-entry success counts.
    id: String,
    /// The built-in local table verified the password. Stamps the
    /// verified identity.
    via_local: bool,
    /// The directory cast the deciding verdict. Selects quota handling.
    via_ldap: bool,
    /// Kerberos cast the deciding verdict. Selects quota handling.
    via_kerberos: bool,
    /// The JWKS endpoint cast the deciding verdict. Selects quota use.
    via_jwks: bool,
    /// A database source cast the deciding verdict. Selects quota use.
    via_db: bool,
    /// The webhook cast the deciding verdict. Selects quota handling.
    via_webhook: bool,
    /// Verified JWT subject on JWKS success. Stamps the session.
    jwks_subject: Option<String>,
    /// Verified Kerberos principal on success. Stamps the session.
    kerberos_principal: Option<String>,
    /// Mapped Kerberos role on success. Feeds the role invariant.
    kerberos_role: Option<String>,
}

impl ChainAllow {
    /// Empty verdict for `id`: no backend decided yet.
    fn none(id: String) -> Self {
        Self {
            id,
            via_local: false,
            via_ldap: false,
            via_kerberos: false,
            via_jwks: false,
            via_db: false,
            via_webhook: false,
            jwks_subject: None,
            kerberos_principal: None,
            kerberos_role: None,
        }
    }
}

/// One refused chain verdict for a credentialed CONNECT.
struct ChainDeny {
    /// Id of the entry that denied. Empty when all entries abstained.
    id: String,
    /// Backend of the denying entry. Empty when all entries abstained.
    backend: String,
}

/// Run one credentialed CONNECT through the stored chain order (MV-09a).
///
/// Iterates `entries` in stored order and skips disabled entries: a
/// disabled entry never executes. Each backend maps to its live
/// authenticator. An entry that does not know the credential abstains
/// and the next enabled entry runs. A deny stops the chain at once and
/// never falls through to a later allow. All abstain denies fail-closed
/// with an empty id. Backends with no live code here (`scram`, `file`,
/// `cinfo`) always abstain. CONNECT-only, never on the delivery path.
async fn decide_chain_auth(
    shared: &Shared,
    entries: &[AuthnEntry],
    tenant: &str,
    client_id: &str,
    username: Option<&str>,
    password: Option<&[u8]>,
) -> Result<ChainAllow, ChainDeny> {
    for entry in entries {
        if !entry.enable {
            continue;
        }
        let deny = || ChainDeny {
            id: entry.id.clone(),
            backend: entry.backend.clone(),
        };
        match entry.backend.as_str() {
            "built_in_database" => {
                // An unknown name abstains, except open mode allows.
                // Open mode needs an empty store and no other live entry.
                // A live entry decides, so the built-in entry abstains.
                if !username.is_some_and(|name| shared.auth.has_user_in_tenant(tenant, name)) {
                    if shared.auth.user_count() != 0 {
                        continue;
                    }
                    let other_live = entries.iter().any(|other| {
                        if !other.enable || other.id == entry.id {
                            return false;
                        }
                        match other.backend.as_str() {
                            "ldap" => shared.ldap.is_some(),
                            "kerberos" => shared.kerberos.as_ref().is_some_and(|k| k.is_enabled()),
                            "jwt" => shared.jwks.as_ref().is_some_and(|j| j.is_enabled()),
                            "mysql" | "postgresql" | "mongodb" | "redis" => {
                                shared.db_auth.as_ref().is_some_and(|d| d.is_configured())
                            }
                            "http" => shared.webhook.is_some(),
                            _ => false,
                        }
                    });
                    if other_live {
                        continue;
                    }
                    return Ok(ChainAllow::none(entry.id.clone()));
                }
                // A known local name with a wrong password denies here.
                let ok = shared
                    .auth
                    .authenticate_in_tenant(tenant, client_id, username, password)
                    .await
                    .is_ok();
                if ok {
                    let mut allow = ChainAllow::none(entry.id.clone());
                    allow.via_local = true;
                    return Ok(allow);
                }
                return Err(deny());
            }
            "jwt" => {
                let Some(jwks) = shared.jwks.as_ref() else {
                    continue;
                };
                if !jwks.is_enabled() {
                    continue;
                }
                if !password.is_some_and(JwksAuthenticator::looks_like_jwt) {
                    continue;
                }
                match verify_jwks_connect(shared, tenant, client_id, password).await {
                    Ok(subject) => {
                        let mut allow = ChainAllow::none(entry.id.clone());
                        allow.via_jwks = true;
                        allow.jwks_subject = subject;
                        return Ok(allow);
                    }
                    Err(()) => {
                        return Err(deny());
                    }
                }
            }
            "kerberos" => {
                let Some(kerberos) = shared.kerberos.as_ref() else {
                    continue;
                };
                if !kerberos.is_enabled() {
                    continue;
                }
                let Some(token) = password else {
                    continue;
                };
                if token.is_empty() || (token[0] != 0x60 && token[0] != 0x6E) {
                    continue;
                }
                match kerberos.verify_token_in_tenant(tenant, client_id, token) {
                    Ok(verified) => {
                        // Wire every verified field live, as the fixed
                        // order path does. Audit-logged for SSO use.
                        let expected_service = kerberos.config().service_principal_name.clone();
                        tracing::info!(
                            client_principal = %verified.client_principal,
                            service_principal = %verified.service_principal,
                            expected_service = %expected_service,
                            role = %verified.role,
                            session_key_len = verified.session_key.len(),
                            "Kerberos CONNECT accepted"
                        );
                        debug_assert_eq!(verified.service_principal, expected_service);
                        debug_assert_eq!(verified.session_key.len(), 32);
                        let mut allow = ChainAllow::none(entry.id.clone());
                        allow.via_kerberos = true;
                        allow.kerberos_principal = Some(verified.client_principal);
                        allow.kerberos_role = Some(verified.role);
                        return Ok(allow);
                    }
                    Err(_) => {
                        return Err(deny());
                    }
                }
            }
            "ldap" => {
                let Some(ldap) = shared.ldap.as_ref() else {
                    continue;
                };
                if ldap
                    .authenticate_in_tenant(tenant, client_id, username, password)
                    .await
                    .is_ok()
                {
                    let mut allow = ChainAllow::none(entry.id.clone());
                    allow.via_ldap = true;
                    return Ok(allow);
                }
                return Err(deny());
            }
            "mysql" | "postgresql" | "mongodb" | "redis" => {
                let Some(db) = shared.db_auth.as_ref().filter(|d| d.is_configured()) else {
                    continue;
                };
                if db
                    .authenticate_in_tenant(tenant, client_id, username, password)
                    .await
                    .is_ok()
                {
                    let mut allow = ChainAllow::none(entry.id.clone());
                    allow.via_db = true;
                    return Ok(allow);
                }
                return Err(deny());
            }
            "http" => {
                let Some(webhook) = shared.webhook.as_ref() else {
                    continue;
                };
                if webhook
                    .authenticate_in_tenant(tenant, client_id, username, password)
                    .await
                    .is_ok()
                {
                    let mut allow = ChainAllow::none(entry.id.clone());
                    allow.via_webhook = true;
                    return Ok(allow);
                }
                return Err(deny());
            }
            _ => {}
        }
    }
    Err(ChainDeny {
        id: String::new(),
        backend: String::new(),
    })
}

/// Authenticated bind for the live path: credentialed requests verify
/// against the auth store first (failure short-circuits to return code
/// `0x86`, bad username/password, with no session created), then the
/// per-user connection quota applies (overage short-circuits to `0x8B`),
/// then the standard session resolution applies.
/// Test harness only: production calls [`apply_bind_request`] directly.
#[cfg(test)]
async fn apply_bind(frame: &BrokerFrame, shared: &Shared) -> Option<BrokerFrame> {
    apply_bind_request(
        frame,
        decode_bind_meta(&frame.metadata),
        bind_looks_v5(&frame.metadata),
        shared,
    )
    .await
}

/// The bind operation on a decoded request.
///
/// A link frame carries the request as metadata, and [`apply_bind`]
/// decodes it. A connection that the kernel owns makes the request from
/// the CONNECT packet and calls this function directly. `frame` gives
/// the connection identifier and the sequence number for the reply.
/// `looks_v5` selects the MQTT 5 shape of the refusal when `parsed` is
/// an error.
///
/// RQ-04: this is the kernel bind point. It fires each connect
/// event once per decoded bind. Rejected CONNACKs fire too. Every
/// broker entry uses this path. The transport wrapper fires nothing.
async fn apply_bind_request(
    frame: &BrokerFrame,
    parsed: Result<BindRequest, &'static str>,
    looks_v5: bool,
    shared: &Shared,
) -> Option<BrokerFrame> {
    let reply = apply_bind_request_inner(frame, parsed.clone(), looks_v5, shared).await;
    if let (Ok(req), Some(reply)) = (parsed, reply.as_ref()) {
        let return_code = if reply.metadata.len() >= 10 {
            reply.metadata[9]
        } else {
            0x80
        };
        let session_present = reply.metadata.get(8).copied().unwrap_or(0) != 0;
        let tenant = shared
            .sessions
            .key_for_conn(frame.header.conn_id)
            .map(|key| key.tenant.clone())
            .unwrap_or_else(|| broker_session::tenant::DEFAULT_TENANT_ID.to_string());
        if is_default_tenant(tenant.as_str()) {
            shared
                .engine
                .fire_client_connected(broker_rules::ClientConnectedEvent {
                    client_id: req.client_id.clone(),
                    username: req.username.clone(),
                    peerhost: req.peerhost.clone(),
                    keepalive: req.keepalive_secs,
                    proto_ver: req.protocol_version,
                    clean_start: req.clean_start,
                    conn_ack_code: return_code,
                });
            shared
                .engine
                .fire_client_connack(broker_rules::ClientConnackEvent {
                    client_id: req.client_id.clone(),
                    username: req.username.clone(),
                    peerhost: req.peerhost.clone(),
                    return_code,
                    session_present,
                });
        }
    }
    reply
}

/// The bind operation on a decoded request.
///
/// A link frame carries the request as metadata, and [`apply_bind`]
/// decodes it. A connection that the kernel owns makes the request from
/// the CONNECT packet and calls this function directly. `frame` gives
/// the connection identifier and the sequence number for the reply.
/// `looks_v5` selects the MQTT 5 shape of the refusal when `parsed` is
/// an error.
async fn apply_bind_request_inner(
    frame: &BrokerFrame,
    parsed: Result<BindRequest, &'static str>,
    looks_v5: bool,
    shared: &Shared,
) -> Option<BrokerFrame> {
    // B2-02: verified Kerberos identity for this CONNECT. Set inside the
    // auth block on Kerberos success and read by the ownership stamp below
    // without re-verifying the same token (the replay cache would refuse a
    // second verify). CONNECT-only, never on the delivery path.
    let mut kerberos_principal: Option<String> = None;
    let mut kerberos_role: Option<String> = None;
    // B5-02: verified JWT subject for this CONNECT. Set inside the auth
    // block on JWKS success and read by the ownership stamp below.
    // CONNECT-only, never on the delivery path.
    let mut jwks_subject: Option<String> = None;
    // Verified identity for the ownership stamp. Set only when a backend
    // verified the credential. Open mode leaves it `None`.
    let mut verified_connect_username: Option<String> = None;
    // B4-05: the CONNACK-negotiated inbound alias bound rides every
    // `SessionBinding` reply, including refusals, so the client always
    // learns it on the CONNECT event.
    let alias_max = shared.sessions.max_topic_alias();
    // P2: the same replies carry the configured server packet limit.
    let server_max_pkt = shared.sessions.max_packet_size();
    // Cache one render per distinct input for this bind.
    // The registry counts one rejection per render.
    // Reuse keeps one bind at one count.
    let mut cached_none_tenant: Option<String> = None;
    let mut cached_verified_tenant: Option<String> = None;
    let mut acquired_slot_user: Option<(String, String)> = None;
    let mut pre_validated_will = None;
    let mut incumbent_slot_owner: Option<String> = None;
    let mut same_user_takeover = false;
    if let Ok(req) = parsed.clone() {
        // X1-02: a Receive Maximum of 0 is a protocol error on the wire
        // (see the synchronous path for the code-choice rationale).
        // Fail closed before bans/credentials/quotas so no session is
        // created and no quota slot is consumed. Every bind is
        // answered (counted via `connack_sent` at the send site), so
        // no extra counter moves here.
        if req.protocol_version == 5 && req.receive_maximum == 0 {
            return Some(refusal_for_req(
                frame,
                &req,
                V5_RC_PROTOCOL_ERROR,
                alias_max,
                server_max_pkt,
            ));
        }
        // B1-03: banned identities never reach the session. The bind's
        // own tenant is resolved first, so a ban in one tenant never
        // blocks another tenant's namesake. Checked before credentials
        // and quotas so a banned client is refused with the banned
        // return code (0x8A, mapped to CONNACK 5 on the edge) with no
        // session created and no quota slot consumed. A connect is not
        // the delivery path, so one read lock here costs the message
        // path nothing.
        let tenant_for_ban = bind_tenant_for_req(shared, &req, None);
        // Store the render for later steps in this bind.
        cached_none_tenant = Some(tenant_for_ban.clone());
        if shared.bans.is_banned_in_tenant(
            &tenant_for_ban,
            &req.client_id,
            req.username.as_deref(),
            req.peerhost.as_deref(),
        ) {
            shared.metrics.inc_auth_failures();
            // CONNECT drives the alarm store.
            // A banned CONNECT raises a bounded alarm in its own tenant.
            // CONNECT is rare, so the write stays off the hot path.
            shared.alarms.activate_in_tenant(
                &tenant_for_ban,
                "banned_connect_rejected",
                &format!("banned CONNECT refused for '{}'", req.client_id),
                serde_json::json!({"clientid": req.client_id}),
            );
            // No audit record here. The log holds only post, put and delete.
            return Some(refusal_for_req(
                frame,
                &req,
                0x8A,
                alias_max,
                server_max_pkt,
            ));
        }
        // TM-08: client-certificate check. Runs on connect only, never
        // on publish or deliver. No lock, no queue, no per-client store.
        // The accept loop bounds the rate (50 ms backoff plus the
        // connection limit). When the listener verifies, the bind must
        // carry a certificate field. Else deny, count and log.
        if client_cert_required(shared, &req.listener) && !has_client_cert(&req) {
            shared.metrics.inc_auth_failures();
            tracing::warn!(
                client_id = %req.client_id,
                listener = %req.listener,
                "client certificate is missing or not valid: refusing CONNECT"
            );
            return Some(refusal_for_req(
                frame,
                &req,
                0x87,
                alias_max,
                server_max_pkt,
            ));
        }
        // W2-02: authenticator-chain consult. Loads one lock-free
        // snapshot per connect and never takes the chain write lock;
        // publish and deliver never touch this store. An empty chain
        // preserves today's behaviour. A non-empty chain with no enabled
        // built-in slot fails closed (no live backend can execute):
        // anonymous and credentialed CONNECTs are refused with no session
        // created. Only the built-in database executes; other backends
        // are stored and reported, never claimed as live.
        // TODO(parity): which CONNACK code does the spec require for a
        // chain with no live backend? The rulebook does not decide the
        // code; 0x86 (bad credentials) is the conservative fail-closed
        // choice until the checker pins it.
        if !shared.authn_chain.built_in_available() {
            tracing::warn!(
                client_id = %req.client_id,
                "authentication chain has no live backend: refusing CONNECT"
            );
            shared.metrics.inc_auth_failures();
            // W2-08: attribute the no-live-backend refusal to the first
            // entry so status shows the outage. CONNECT-only; publish and
            // deliver never touch these counters.
            shared.authn_chain.record_first_failed();
            return Some(refusal_for_req(
                frame,
                &req,
                0x86,
                alias_max,
                server_max_pkt,
            ));
        }
        // W2-05: authentication-settings consult. Loads one lock-free
        // snapshot per connect to observe the backend-failure flag and
        // never takes the settings write lock; publish and deliver never
        // touch this store. Only the built-in database executes: an
        // outage still fails closed (deny access and log) even when the
        // flag is set, until the checker pins the intended behaviour.
        let ignore_backend_failures = shared.authn_settings.ignore_backend_failures();
        if ignore_backend_failures {
            tracing::debug!(
                client_id = %req.client_id,
                "ignore_backend_failures is set: CONNECT still fails closed on auth failure"
            );
        }
        if req.enhanced_auth_verified {
            verified_connect_username = req.username.clone();
        } else if req.username.is_none() {
            // A non-empty user store always wins over `--allow-anonymous`:
            // unauthenticated clients are rejected whenever users exist.
            // A configured directory, database, webhook or JWT endpoint does the same: anonymous
            // never authenticates against LDAP, Kerberos, JWKS, a webhook or a database.
            // The flag only opens the broker while the store is empty and
            // no directory, database, webhook, enabled Kerberos mechanism or JWKS endpoint
            // is configured.
            let kerberos_enabled = shared.kerberos.as_ref().is_some_and(|k| k.is_enabled());
            let jwks_enabled = shared.jwks.as_ref().is_some_and(|j| j.is_enabled());
            let db_configured = shared.db_auth.as_ref().is_some_and(|d| d.is_configured());
            // B5-04: a configured webhook does the same: anonymous never
            // carries a verdict, so it is refused while the webhook is on.
            if shared.auth.user_count() > 0
                || shared.ldap.is_some()
                || kerberos_enabled
                || jwks_enabled
                || db_configured
                || shared.webhook.is_some()
                || !shared.allow_anonymous
            {
                // `auth.allow_anonymous = false` refuses an anonymous CONNECT
                // even while the user store is empty: the flag is applied
                // before the session is created, with the version-correct
                // code (3.1.1: return code 5, 5.0: reason code 0x87).
                shared.metrics.inc_auth_failures();
                // W2-08: anonymous refusal is a failed verdict on the
                // first enabled built-in entry when one exists.
                // CONNECT-only; publish and deliver never touch these
                // counters.
                shared.authn_chain.record_built_in_failed();
                return Some(refusal_for_req(
                    frame,
                    &req,
                    0x87,
                    alias_max,
                    server_max_pkt,
                ));
            }
        } else {
            let password = req.password.as_deref();
            // B2-01: local users first, then the directory. With an empty
            // local store and no directory the broker stays open (existing
            // behaviour); with a directory configured the open mode is
            // disabled and every credentialed CONNECT goes through the
            // directory unless a local user matches. A directory outage
            // fails closed with a clear log line inside the authenticator
            // and never grants access.
            // B2-02: Kerberos tokens (SPNEGO `0x60` or AP-REQ `0x6E`) go
            // through the Kerberos authenticator when enabled (real DER
            // plus keytab verification, never a bypass). Other passwords
            // keep the LDAP path. An enabled Kerberos mechanism disables
            // the open mode like LDAP does: non-Kerberos passwords fail
            // closed with 0x86 instead of falling through.
            let local_has_users = shared.auth.user_count() > 0;
            // Tenant scope (MT-06): the credential lookup runs in the
            // bind's own tenant, rendered here from the claimed username
            // (the verified Kerberos principal/JWT subject is unknown
            // until its own verification below; quota and session
            // resolution re-render from the verified identity as before).
            // Same pure function of the bind the session path renders, no
            // shared state. CONNECT-only; publish/deliver never touch it.
            // Reuse the ban render here.
            // It uses the same input, so reuse avoids a second count.
            let tenant_for_auth = tenant_for_ban.clone();
            let kerberos_enabled = shared.kerberos.as_ref().is_some_and(|k| k.is_enabled());
            let looks_kerberos =
                password.is_some_and(|p| !p.is_empty() && (p[0] == 0x60 || p[0] == 0x6E));
            let jwks_enabled = shared.jwks.as_ref().is_some_and(|j| j.is_enabled());
            let looks_jwt = password.is_some_and(JwksAuthenticator::looks_like_jwt);
            let mut via_local = false;
            let mut via_ldap = false;
            let mut via_kerberos = false;
            let mut via_jwks = false;
            let mut via_db = false;
            // B5-04: set when the webhook casts the deciding credential
            // verdict below. Like directory and Kerberos users, webhook
            // users have no local quotas (unlimited).
            let mut via_webhook = false;
            // MV-09a: chain-driven dispatch. A stored non-empty chain
            // decides in its own order with its own enable flags, never
            // in the fixed order below. An empty chain keeps the fixed
            // order path unchanged. One lock-free snapshot per connect.
            let chain_entries = shared.authn_chain.snapshot();
            if !chain_entries.is_empty() {
                match decide_chain_auth(
                    shared,
                    &chain_entries,
                    &tenant_for_auth,
                    &req.client_id,
                    req.username.as_deref(),
                    password,
                )
                .await
                {
                    Ok(allow) => {
                        via_local = allow.via_local;
                        via_ldap = allow.via_ldap;
                        via_kerberos = allow.via_kerberos;
                        via_jwks = allow.via_jwks;
                        via_db = allow.via_db;
                        via_webhook = allow.via_webhook;
                        jwks_subject = allow.jwks_subject;
                        kerberos_principal = allow.kerberos_principal;
                        kerberos_role = allow.kerberos_role;
                        // Attribute the verdict to the deciding entry so
                        // status shows which backend allowed the CONNECT.
                        // CONNECT-only; publish/deliver never touch this.
                        if via_ldap || via_kerberos || via_jwks || via_db || via_webhook {
                            shared.authn_chain.record_success(&allow.id);
                        }
                    }
                    Err(deny) => {
                        shared.metrics.inc_auth_failures();
                        if deny.id.is_empty() {
                            // No entry decided: fail closed and attribute
                            // the refusal to the first entry so status
                            // shows the outage. CONNECT-only.
                            shared.authn_chain.record_first_failed();
                            tracing::warn!(
                                client_id = %req.client_id,
                                "authentication chain abstained: refusing CONNECT"
                            );
                        } else {
                            // A deny stops the chain: the next backend
                            // never runs. Count, attribute and log it.
                            shared.authn_chain.record_failed(&deny.id);
                            tracing::warn!(
                                client_id = %req.client_id,
                                entry = %deny.id,
                                backend = %deny.backend,
                                "authentication chain denied the credential: refusing CONNECT"
                            );
                        }
                        return Some(refusal_for_req(
                            frame,
                            &req,
                            0x86,
                            alias_max,
                            server_max_pkt,
                        ));
                    }
                }
            } else if local_has_users {
                // Tenant scope (MT-06): the same username in two tenants
                // is two independent identities, so the local lookup runs
                // in the bind's own tenant resolved above. The default
                // tenant keeps today's passwords and open-when-empty
                // behaviour; other tenants start closed (deny, counted,
                // logged inside the store).
                let local_ok = shared
                    .auth
                    .authenticate_in_tenant(
                        &tenant_for_auth,
                        &req.client_id,
                        req.username.as_deref(),
                        password,
                    )
                    .await
                    .is_ok();
                if local_ok {
                    // Local credential accepted; quotas apply below.
                    via_local = true;
                } else if req
                    .username
                    .as_deref()
                    .is_some_and(|u| shared.auth.has_user_in_tenant(&tenant_for_auth, u))
                {
                    // A known local user with a wrong password denies here.
                    // A deny never falls through to a later allow: the
                    // chain stops instead of trying the next backend.
                    // An unknown local name ignores to the next backend.
                    tracing::warn!(
                        client_id = %req.client_id,
                        "known local user gave a wrong password: refusing CONNECT"
                    );
                    shared.metrics.inc_auth_failures();
                    // W2-08: failed credential verdict. CONNECT-only.
                    shared.authn_chain.record_built_in_failed();
                    return Some(refusal_for_req(
                        frame,
                        &req,
                        0x86,
                        alias_max,
                        server_max_pkt,
                    ));
                } else if jwks_enabled && looks_jwt {
                    // B5-02: JWT-shaped passwords verify against the live
                    // JWKS endpoint when configured; success carries the
                    // verified subject for the ownership stamp below.
                    match verify_jwks_connect(shared, &tenant_for_auth, &req.client_id, password)
                        .await
                    {
                        Ok(subject) => {
                            via_jwks = true;
                            jwks_subject = subject;
                        }
                        Err(()) => {
                            // W2-08: failed credential verdict. CONNECT-only.
                            shared.authn_chain.record_built_in_failed();
                            return Some(refusal_for_req(
                                frame,
                                &req,
                                0x86,
                                alias_max,
                                server_max_pkt,
                            ));
                        }
                    }
                } else if kerberos_enabled && looks_kerberos {
                    let Some(kerberos) = shared.kerberos.as_ref() else {
                        shared.metrics.inc_auth_failures();
                        // W2-08: failed credential verdict. CONNECT-only.
                        shared.authn_chain.record_built_in_failed();
                        return Some(refusal_for_req(
                            frame,
                            &req,
                            0x86,
                            alias_max,
                            server_max_pkt,
                        ));
                    };
                    let Some(token) = password else {
                        shared.metrics.inc_auth_failures();
                        // W2-08: failed credential verdict. CONNECT-only.
                        shared.authn_chain.record_built_in_failed();
                        return Some(refusal_for_req(
                            frame,
                            &req,
                            0x86,
                            alias_max,
                            server_max_pkt,
                        ));
                    };
                    match kerberos.verify_token_in_tenant(&tenant_for_auth, &req.client_id, token) {
                        Ok(verified) => {
                            // Wire every verified field live: client and
                            // service principals, session-key length and
                            // mapped role, plus the authenticator config
                            // for the expected service. Audit-logged for
                            // SSO traceability; CONNECT-only.
                            let expected_service = kerberos.config().service_principal_name.clone();
                            tracing::info!(
                                client_principal = %verified.client_principal,
                                service_principal = %verified.service_principal,
                                expected_service = %expected_service,
                                role = %verified.role,
                                session_key_len = verified.session_key.len(),
                                "Kerberos CONNECT accepted"
                            );
                            debug_assert_eq!(verified.service_principal, expected_service);
                            debug_assert_eq!(verified.session_key.len(), 32);
                            via_kerberos = true;
                            kerberos_principal = Some(verified.client_principal);
                            kerberos_role = Some(verified.role);
                        }
                        Err(_) => {
                            shared.metrics.inc_auth_failures();
                            // W2-08: failed credential verdict. CONNECT-only.
                            shared.authn_chain.record_built_in_failed();
                            return Some(refusal_for_req(
                                frame,
                                &req,
                                0x86,
                                alias_max,
                                server_max_pkt,
                            ));
                        }
                    }
                } else if let Some(ldap) = shared.ldap.as_ref() {
                    // Tenant scope (MT-06): the directory lookup carries
                    // the bind's tenant; only the built-in table holds
                    // per-tenant state in this wave (deferred).
                    if ldap
                        .authenticate_in_tenant(
                            &tenant_for_auth,
                            &req.client_id,
                            req.username.as_deref(),
                            password,
                        )
                        .await
                        .is_ok()
                    {
                        via_ldap = true;
                    } else {
                        shared.metrics.inc_auth_failures();
                        // W2-08: failed credential verdict. CONNECT-only.
                        shared.authn_chain.record_built_in_failed();
                        return Some(refusal_for_req(
                            frame,
                            &req,
                            0x86,
                            alias_max,
                            server_max_pkt,
                        ));
                    }
                } else if let Some(db) = shared.db_auth.as_ref().filter(|d| d.is_configured()) {
                    // B5-03: database sources after the directory. A
                    // database outage fails closed with a clear log line
                    // inside the source and never grants access.
                    // Tenant scope (MT-06): the lookup carries the bind's
                    // tenant; only the built-in table holds per-tenant
                    // state in this wave (deferred).
                    if db
                        .authenticate_in_tenant(
                            &tenant_for_auth,
                            &req.client_id,
                            req.username.as_deref(),
                            password,
                        )
                        .await
                        .is_ok()
                    {
                        via_db = true;
                    } else {
                        shared.metrics.inc_auth_failures();
                        // W2-08: failed credential verdict. CONNECT-only.
                        shared.authn_chain.record_built_in_failed();
                        return Some(refusal_for_req(
                            frame,
                            &req,
                            0x86,
                            alias_max,
                            server_max_pkt,
                        ));
                    }
                } else if let Some(webhook) = shared.webhook.as_ref() {
                    // B5-04: the local store (and the directory above)
                    // refused; the webhook casts the final credential
                    // verdict. Allow proceeds to the quota check below;
                    // deny or an unreachable/slow endpoint fails closed
                    // with 0x86 and never grants access.
                    // TODO(parity): when several backends are configured,
                    // should the webhook run before LDAP/Kerberos instead
                    // of last? The rulebook does not decide precedence;
                    // current choice keeps accepted LDAP/Kerberos behavior
                    // untouched and fails closed either way.
                    if webhook
                        .authenticate_in_tenant(
                            &tenant_for_auth,
                            &req.client_id,
                            req.username.as_deref(),
                            password,
                        )
                        .await
                        .is_ok()
                    {
                        via_webhook = true;
                    } else {
                        shared.metrics.inc_auth_failures();
                        // W2-08: failed credential verdict. CONNECT-only.
                        shared.authn_chain.record_built_in_failed();
                        return Some(refusal_for_req(
                            frame,
                            &req,
                            0x86,
                            alias_max,
                            server_max_pkt,
                        ));
                    }
                } else {
                    // No LDAP, no database, no webhook, and (no enabled Kerberos or a
                    // non-Kerberos password): with local users present the
                    // failure is final. When Kerberos is enabled a
                    // non-Kerberos password is not an open-mode accept.
                    if kerberos_enabled {
                        shared.metrics.inc_auth_failures();
                        // W2-08: failed credential verdict. CONNECT-only.
                        shared.authn_chain.record_built_in_failed();
                        return Some(refusal_for_req(
                            frame,
                            &req,
                            0x86,
                            alias_max,
                            server_max_pkt,
                        ));
                    }
                    shared.metrics.inc_auth_failures();
                    // W2-08: failed credential verdict. CONNECT-only.
                    shared.authn_chain.record_built_in_failed();
                    return Some(refusal_for_req(
                        frame,
                        &req,
                        0x86,
                        alias_max,
                        server_max_pkt,
                    ));
                }
            } else if jwks_enabled && looks_jwt {
                // B5-02: same JWKS verification as above for the empty
                // local store: the endpoint is the authority, so a
                // JWT-shaped password never falls through to open mode.
                match verify_jwks_connect(shared, &tenant_for_auth, &req.client_id, password).await
                {
                    Ok(subject) => {
                        via_jwks = true;
                        jwks_subject = subject;
                    }
                    Err(()) => {
                        // W2-08: failed credential verdict. CONNECT-only.
                        shared.authn_chain.record_built_in_failed();
                        return Some(refusal_for_req(
                            frame,
                            &req,
                            0x86,
                            alias_max,
                            server_max_pkt,
                        ));
                    }
                }
            } else if kerberos_enabled && looks_kerberos {
                let Some(kerberos) = shared.kerberos.as_ref() else {
                    shared.metrics.inc_auth_failures();
                    // W2-08: failed credential verdict. CONNECT-only.
                    shared.authn_chain.record_built_in_failed();
                    return Some(refusal_for_req(
                        frame,
                        &req,
                        0x86,
                        alias_max,
                        server_max_pkt,
                    ));
                };
                let Some(token) = password else {
                    shared.metrics.inc_auth_failures();
                    // W2-08: failed credential verdict. CONNECT-only.
                    shared.authn_chain.record_built_in_failed();
                    return Some(refusal_for_req(
                        frame,
                        &req,
                        0x86,
                        alias_max,
                        server_max_pkt,
                    ));
                };
                match kerberos.verify_token_in_tenant(&tenant_for_auth, &req.client_id, token) {
                    Ok(verified) => {
                        // Same live wiring as above: every verified field
                        // plus the authenticator config is read here.
                        let expected_service = kerberos.config().service_principal_name.clone();
                        tracing::info!(
                            client_principal = %verified.client_principal,
                            service_principal = %verified.service_principal,
                            expected_service = %expected_service,
                            role = %verified.role,
                            session_key_len = verified.session_key.len(),
                            "Kerberos CONNECT accepted"
                        );
                        debug_assert_eq!(verified.service_principal, expected_service);
                        debug_assert_eq!(verified.session_key.len(), 32);
                        via_kerberos = true;
                        kerberos_principal = Some(verified.client_principal);
                        kerberos_role = Some(verified.role);
                    }
                    Err(_) => {
                        shared.metrics.inc_auth_failures();
                        // W2-08: failed credential verdict. CONNECT-only.
                        shared.authn_chain.record_built_in_failed();
                        return Some(refusal_for_req(
                            frame,
                            &req,
                            0x86,
                            alias_max,
                            server_max_pkt,
                        ));
                    }
                }
            } else if let Some(ldap) = shared.ldap.as_ref() {
                // Tenant scope (MT-06): the directory lookup carries the
                // bind's tenant (deferred: only the built-in table holds
                // per-tenant state in this wave).
                if ldap
                    .authenticate_in_tenant(
                        &tenant_for_auth,
                        &req.client_id,
                        req.username.as_deref(),
                        password,
                    )
                    .await
                    .is_ok()
                {
                    via_ldap = true;
                } else {
                    shared.metrics.inc_auth_failures();
                    // W2-08: failed credential verdict. CONNECT-only.
                    shared.authn_chain.record_built_in_failed();
                    return Some(refusal_for_req(
                        frame,
                        &req,
                        0x86,
                        alias_max,
                        server_max_pkt,
                    ));
                }
            } else if let Some(db) = shared.db_auth.as_ref().filter(|d| d.is_configured()) {
                // B5-03: a configured database disables the open mode
                // like LDAP does: every credentialed CONNECT goes through
                // the databases unless a local user matches (none exist
                // here). An outage fails closed, never an accept.
                // Tenant scope (MT-06): the lookup carries the bind's
                // tenant (deferred: only the built-in table holds
                // per-tenant state in this wave).
                if db
                    .authenticate_in_tenant(
                        &tenant_for_auth,
                        &req.client_id,
                        req.username.as_deref(),
                        password,
                    )
                    .await
                    .is_ok()
                {
                    via_db = true;
                } else {
                    shared.metrics.inc_auth_failures();
                    // W2-08: failed credential verdict. CONNECT-only.
                    shared.authn_chain.record_built_in_failed();
                    return Some(refusal_for_req(
                        frame,
                        &req,
                        0x86,
                        alias_max,
                        server_max_pkt,
                    ));
                }
            } else if let Some(webhook) = shared.webhook.as_ref() {
                // B5-04: no local user matched and no directory verdict
                // above; the webhook decides instead of the open mode. An
                // unreachable or slow endpoint fails closed with 0x86.
                // Tenant scope (MT-06): the verdict carries the bind's
                // tenant (deferred: only the built-in table holds
                // per-tenant state in this wave).
                if webhook
                    .authenticate_in_tenant(
                        &tenant_for_auth,
                        &req.client_id,
                        req.username.as_deref(),
                        password,
                    )
                    .await
                    .is_ok()
                {
                    via_webhook = true;
                } else {
                    shared.metrics.inc_auth_failures();
                    // W2-08: failed credential verdict. CONNECT-only.
                    shared.authn_chain.record_built_in_failed();
                    return Some(refusal_for_req(
                        frame,
                        &req,
                        0x86,
                        alias_max,
                        server_max_pkt,
                    ));
                }
            } else if kerberos_enabled {
                // Enabled Kerberos disables the open mode: a credentialed
                // CONNECT without a Kerberos token fails closed.
                shared.metrics.inc_auth_failures();
                // W2-08: failed credential verdict. CONNECT-only.
                shared.authn_chain.record_built_in_failed();
                return Some(refusal_for_req(
                    frame,
                    &req,
                    0x86,
                    alias_max,
                    server_max_pkt,
                ));
            } else if jwks_enabled {
                // B5-02: a configured JWKS endpoint disables the open mode
                // like a directory does. A non-JWT password with no other
                // mechanism configured fails closed instead of
                // open-accepting: nothing unverified is ever granted.
                shared.metrics.inc_auth_failures();
                // W2-08: failed credential verdict. CONNECT-only.
                shared.authn_chain.record_built_in_failed();
                return Some(refusal_for_req(
                    frame,
                    &req,
                    0x86,
                    alias_max,
                    server_max_pkt,
                ));
            } else if !shared.allow_anonymous {
                // `auth.allow_anonymous = false` must still fail closed when
                // no authentication backend is configured, even if CONNECT
                // carries a username.
                shared.metrics.inc_auth_failures();
                shared.authn_chain.record_built_in_failed();
                return Some(refusal_for_req(
                    frame,
                    &req,
                    0x87,
                    alias_max,
                    server_max_pkt,
                ));
            } else {
                // Open broker: no users, no directory, no database, no webhook, no enabled Kerberos
                // or JWKS. Fall through to the session resolution below
                // (rc 0).
            }
            // Effective identity for quotas and takeover: the verified
            // Kerberos principal when present, the verified JWT subject
            // for JWT CONNECTs carrying one, else the MQTT username.
            // TODO(parity): whether a scope/role claim should drive ACLs
            // is open; JWT claims drive no authorisation today.
            let effective_username: Option<String> = if via_jwks {
                jwks_subject.clone()
            } else {
                kerberos_principal.clone().or_else(|| req.username.clone())
            };
            // Takeover pre-release: a live session for this client means
            // the previous holder was orphaned by this very bind. Resolved
            // through the bind's own tenant (rendered here from this
            // connection's credentials), so a takeover in another tenant
            // never releases this tenant's slot. Reuse the earlier render
            // when the input matches, so one bind counts one rejection.
            // A verified principal uses a fresh render instead.
            let tenant_for_quota = if kerberos_principal.is_none() {
                tenant_for_ban.clone()
            } else {
                let resolved = bind_tenant_for_req(shared, &req, kerberos_principal.as_deref());
                cached_verified_tenant = Some(resolved.clone());
                resolved
            };
            let will = match will_from_bind(&req) {
                Ok(will) => will,
                Err(_) => {
                    if req.protocol_version == 5 {
                        let v5 = failure_binding_v5(&req, V5_RC_PROTOCOL_ERROR, server_max_pkt);
                        return Some(session_binding_reply_with_v5(
                            frame,
                            0,
                            false,
                            V5_RC_PROTOCOL_ERROR,
                            shared.sessions.max_topic_alias(),
                            &v5,
                        ));
                    }
                    return Some(session_binding_reply(
                        frame,
                        0,
                        false,
                        2u8,
                        shared.sessions.max_topic_alias(),
                    ));
                }
            };
            if req.client_id.is_empty() && !req.clean_start {
                let rc = if req.protocol_version == 5 {
                    V5_RC_CLIENT_ID_REJECTED
                } else {
                    2u8
                };
                return Some(refusal_for_req(frame, &req, rc, alias_max, server_max_pkt));
            }
            pre_validated_will = Some(will);
            let incumbent_session = shared
                .sessions
                .get_in_tenant(&tenant_for_quota, &req.client_id);
            let is_takeover = incumbent_session
                .as_ref()
                .is_some_and(|session| *session.connected.read());
            incumbent_slot_owner = incumbent_session
                .as_ref()
                .filter(|session| *session.connected.read())
                .and_then(|session| session.username.read().clone());
            same_user_takeover = incumbent_session.as_ref().is_some_and(|session| {
                *session.connected.read()
                    && session.username.read().as_deref() == effective_username.as_deref()
            });
            if let Some(username) = &effective_username {
                debug_assert!(via_kerberos == kerberos_role.is_some());
                let max = if via_ldap || via_kerberos || via_jwks || via_db || via_webhook {
                    None
                } else {
                    shared
                        .auth
                        .get_quotas_in_tenant(&tenant_for_quota, username)
                        .and_then(|quotas| quotas.max_connections)
                };
                if !same_user_takeover {
                    if !shared.sessions.acquire_connection_slot_in_tenant(
                        &tenant_for_quota,
                        username,
                        max,
                    ) {
                        let rc = if req.protocol_version == 5 {
                            V5_RC_CONNECT_QUOTA
                        } else {
                            0x8B
                        };
                        return Some(refusal_for_req(frame, &req, rc, alias_max, server_max_pkt));
                    }
                    acquired_slot_user = Some((tenant_for_quota.clone(), username.clone()));
                }
                if !is_takeover && !tenant_connection_fits(shared, &tenant_for_quota) {
                    if let Some((slot_tenant, slot_user)) = acquired_slot_user.take() {
                        shared
                            .sessions
                            .release_connection_slot_in_tenant(&slot_tenant, &slot_user);
                    }
                    shared.metrics.inc_tenant_conn_refused();
                    shared.tenant_limits.count_conn_refused();
                    tracing::warn!(
                        tenant = %tenant_for_quota,
                        client_id = %req.client_id,
                        "tenant connection limit reached: refusing CONNECT"
                    );
                    let rc = if req.protocol_version == 5 {
                        V5_RC_CONNECT_QUOTA
                    } else {
                        0x8B
                    };
                    return Some(refusal_for_req(frame, &req, rc, alias_max, server_max_pkt));
                }
            } else {
                // TM-06: anonymous binds carry no username slot, but the
                // per-tenant connection bound still applies. A full
                // tenant refuses with the documented quota code.
                let tenant_for_anon =
                    bind_tenant_for_req(shared, &req, kerberos_principal.as_deref());
                if !tenant_connection_fits(shared, &tenant_for_anon) {
                    shared.metrics.inc_tenant_conn_refused();
                    shared.tenant_limits.count_conn_refused();
                    tracing::warn!(
                        tenant = %tenant_for_anon,
                        client_id = %req.client_id,
                        "tenant connection limit reached: refusing anonymous CONNECT"
                    );
                    let rc = if req.protocol_version == 5 {
                        V5_RC_CONNECT_QUOTA
                    } else {
                        0x8B
                    };
                    return Some(refusal_for_req(frame, &req, rc, alias_max, server_max_pkt));
                }
            }
            // W2-03: record the successful credentialed CONNECT in the
            // node auth cache (one short lock, CONNECT-only; publish and
            // deliver never touch this store).
            if let Some(username) = &effective_username {
                shared.authn_node_cache.record_success(username);
            }
            // W2-08: attribute one CONNECT verdict to the first enabled
            // built-in entry. A bypass verdict (another mechanism
            // answered) counts as `nomatch` plus `total`; a local verdict
            // counts as `success` plus `total`. Open-mode CONNECTs (no
            // users, no mechanism) record nothing: no verdict exists.
            // CONNECT-only; publish and deliver never touch these
            // counters.
            if via_ldap || via_kerberos || via_jwks || via_db || via_webhook {
                shared.authn_chain.record_built_in_nomatch();
            } else if local_has_users {
                shared.authn_chain.record_built_in_success();
            }
            verified_connect_username = if via_kerberos {
                kerberos_principal.clone()
            } else if via_jwks {
                jwks_subject.clone()
            } else if via_local || via_ldap || via_db || via_webhook {
                req.username.clone()
            } else {
                verified_connect_username
            };
        }
    }
    // Tenant assignment (MT-01..MT-03): resolve the tenant BEFORE session
    // creation so the `(tenant, client id)` key is known up front: the
    // same client id in two tenants must land in two independent
    // sessions, and a takeover must kick only the same-tenant session.
    // Built from this connection's own credentials only (verified
    // Kerberos principal, else the MQTT username): never from stored
    // state of another tenant's namesake session, so a client can never
    // be placed into a tenant it did not authenticate for (fail closed).
    // The bind carries the listener name on flagged binds and the
    // kernel native path. Edge binds without the flag still carry the
    // default scope. Absent means the default tenant, never another
    // tenant's (fail closed).
    // TODO(parity): an anonymous reconnect carries no username, so it
    // renders the default tenant even when a previous credentialed bind
    // for the same client id held a named tenant; the prior tenant's
    // session stays detached in place (no cross-tenant migration, no
    // displacement). The open question is whether a reconnect without
    // credentials should inherit its own prior tenant.
    // TODO(parity): a tenant-rule change between connects lands the
    // client in a fresh session under the new tenant key; the old
    // tenant's session stays detached in place for its own reconnect.
    let tenant_for_bind: String = match parsed.clone() {
        Ok(req) => {
            // Reuse the render from this bind when the input matches.
            // A verified principal reuses its own render instead.
            // Fresh renders run only when no cached render fits.
            if kerberos_principal.is_some() {
                if let Some(cached) = cached_verified_tenant.clone() {
                    cached
                } else {
                    bind_tenant_for_req(shared, &req, kerberos_principal.as_deref())
                }
            } else if let Some(cached) = cached_none_tenant.clone() {
                cached
            } else {
                bind_tenant_for_req(shared, &req, kerberos_principal.as_deref())
            }
        }
        Err(_) => broker_session::tenant::DEFAULT_TENANT_ID.to_string(),
    };
    // Clean-start bind discards any previous state for this
    // `(tenant, client id)`, including router entries left by an earlier
    // session: snapshot them before `get_or_create` drops the old
    // session object. Scoped to the owning tenant: another tenant's
    // namesake keeps its subscriptions.

    // Session resolution in the owning tenant (MT-03): same takeover
    // semantics as before, scoped to the key. The tenant is stamped
    // before binding so the `conn_id -> session key` index carries the
    // owning tenant. Keepalive is framing-validated at decode;
    // supervision lives on the edge. Malformed binds answer return code
    // 2 exactly like the synchronous reply path (0x82 with a reason
    // string when the peer negotiated version 5).
    // TM-06: per-tenant session bound. A full tenant refuses with
    // the documented quota code, counted and logged. Another tenant
    // still creates sessions. Connect path only.
    if let Ok(req) = parsed.clone() {
        let is_new_session = shared
            .sessions
            .get_in_tenant(&tenant_for_bind, &req.client_id)
            .is_none();
        if is_new_session && !tenant_session_fits(shared, &tenant_for_bind) {
            if let Some((slot_tenant, slot_user)) = acquired_slot_user.take() {
                shared
                    .sessions
                    .release_connection_slot_in_tenant(&slot_tenant, &slot_user);
            }
            shared.metrics.inc_tenant_sess_refused();
            shared.tenant_limits.count_sess_refused();
            tracing::warn!(
                tenant = %tenant_for_bind,
                client_id = %req.client_id,
                "tenant session limit reached: refusing CONNECT"
            );
            let rc = if req.protocol_version == 5 {
                V5_RC_CONNECT_QUOTA
            } else {
                0x8B
            };
            return Some(refusal_for_req(frame, &req, rc, alias_max, server_max_pkt));
        }
    }
    let reply = match parsed.clone() {
        Ok(req) => {
            // F1-01: validate the CONNECT last will before touching
            // session state, so an unusable will section rejects the bind
            // with no session created (fail closed, like malformed meta).
            let will = match pre_validated_will.take() {
                Some(will) => will,
                None => match will_from_bind(&req) {
                    Ok(will) => will,
                    Err(_) => {
                        if let Some((slot_tenant, slot_user)) = acquired_slot_user.take() {
                            shared
                                .sessions
                                .release_connection_slot_in_tenant(&slot_tenant, &slot_user);
                        }
                        if req.protocol_version == 5 {
                            let v5 = failure_binding_v5(&req, V5_RC_PROTOCOL_ERROR, server_max_pkt);
                            return Some(session_binding_reply_with_v5(
                                frame,
                                0,
                                false,
                                V5_RC_PROTOCOL_ERROR,
                                shared.sessions.max_topic_alias(),
                                &v5,
                            ));
                        }
                        return Some(session_binding_reply(
                            frame,
                            0,
                            false,
                            2u8,
                            shared.sessions.max_topic_alias(),
                        ));
                    }
                },
            };
            // An empty client id without a clean start is refused
            // with no session created. Reason: the specification
            // forbids it ([MQTT-3.1.3-8] for 3.1.1, section 3.1.3.1
            // for v5 with code 0x85). An empty id with a clean start
            // stays accepted, as before.
            if req.client_id.is_empty() && !req.clean_start {
                let rc = if req.protocol_version == 5 {
                    V5_RC_CLIENT_ID_REJECTED
                } else {
                    2u8
                };
                return Some(refusal_for_req(frame, &req, rc, alias_max, server_max_pkt));
            }
            // X1-02: a Receive Maximum of 0 is a protocol error on the
            // wire. Fail closed with no session created (see the
            // synchronous path for the code-choice rationale).
            if req.protocol_version == 5 && req.receive_maximum == 0 {
                let v5 = failure_binding_v5(&req, V5_RC_PROTOCOL_ERROR, server_max_pkt);
                return Some(session_binding_reply_with_v5(
                    frame,
                    0,
                    false,
                    V5_RC_PROTOCOL_ERROR,
                    shared.sessions.max_topic_alias(),
                    &v5,
                ));
            }
            // X1-02: resolution honours clean-start plus Session
            // Expiry (see `get_or_create_v5_in_tenant`). Both versions
            // resolve here: version-4 binds carry the documented
            // defaults, so pure 3.1.1 flows behave bit for bit as
            // before, while a reconnect in either version drops an
            // elapsed v5 husk instead of resuming it.
            let effective_username = if let Some(principal) = &kerberos_principal {
                Some(principal.clone())
            } else if let Some(subject) = &jwks_subject {
                Some(subject.clone())
            } else {
                req.username.clone()
            };
            let is_superuser = verified_connect_username
                .as_deref()
                .is_some_and(|name| shared.auth.is_superuser(name));
            let incumbent_takeover_will = shared
                .sessions
                .get_in_tenant(&tenant_for_bind, &req.client_id)
                .filter(|s| {
                    *s.connected.read()
                        && s.conn_id
                            .read()
                            .is_some_and(|id| id != frame.header.conn_id)
                })
                .and_then(|s| {
                    s.take_last_will().map(|w| {
                        (
                            s.clone(),
                            s.username.read().clone(),
                            s.verified_username.read().clone(),
                            s.peerhost.read().clone(),
                            w,
                        )
                    })
                });
            let (session, present, expired_swept, stale_conn) =
                shared.sessions.connect_bind_v5_in_tenant_with_acls(
                    &tenant_for_bind,
                    &req.client_id,
                    req.clean_start,
                    Some(verified_connect_username.as_deref()),
                    req.protocol_version,
                    req.session_expiry,
                    req.receive_maximum,
                    req.max_packet_size,
                    Some(broker_session::ConnectBindMeta {
                        conn_id: frame.header.conn_id,
                        username: effective_username,
                        verified_username: verified_connect_username.clone(),
                        is_superuser,
                        peerhost: req.peerhost.clone(),
                        keepalive_secs: req.keepalive_secs,
                        client_alias_max: req.client_alias_max,
                        will,
                    }),
                    req.pending_acls.clone(),
                );
            if present {
                prune_revoked_subscriptions(shared, &session, &tenant_for_bind).await;
            }
            if let Some(old_conn_id) = stale_conn {
                if old_conn_id != frame.header.conn_id {
                    if !same_user_takeover {
                        if let Some(old_user) = incumbent_slot_owner.as_deref() {
                            shared
                                .sessions
                                .release_connection_slot_in_tenant(&tenant_for_bind, old_user);
                        }
                    }
                    if let Ok(close) = BrokerFrame::new(
                        OpCode::ConnClose,
                        old_conn_id,
                        0,
                        Bytes::from(vec![0x8E]),
                        Bytes::new(),
                    ) {
                        let _ = shared.conns.order_close(old_conn_id, close);
                    }
                }
            }
            if let Some((incumbent_sess, inc_user, inc_verified, inc_peer, inc_will)) =
                incumbent_takeover_will
            {
                if stale_conn.is_some_and(|id| id != frame.header.conn_id) {
                    let deliveries = publish_session_last_will(
                        shared,
                        &incumbent_sess,
                        &tenant_for_bind,
                        inc_user.as_deref(),
                        inc_verified.as_deref(),
                        inc_peer.as_deref(),
                        inc_will,
                    )
                    .await;
                    for (conn_id, routed) in deliveries {
                        let _ = shared.conns.route(conn_id, routed);
                    }
                }
            }
            // X1-02: version-5 success carries session-present, the
            // granted expiry and the server limits; version-4 success
            // keeps today's 12-byte layout and bytes.
            let reply = if req.protocol_version == 5 {
                let granted = session.session_expiry();
                session_binding_reply_with_v5(
                    frame,
                    session.id.0,
                    present,
                    0,
                    shared.sessions.max_topic_alias(),
                    &success_binding_v5(granted, server_max_pkt),
                )
            } else {
                session_binding_reply(
                    frame,
                    session.id.0,
                    present,
                    0,
                    shared.sessions.max_topic_alias(),
                )
            };
            // X1-02: sweep the dropped expired session's router copies
            // (owning tenant only) so an elapsed persistent session
            // resubscribes from nothing like a clean start. Empty
            // unless the expired-drop path ran. Off the hot path
            // (bind only).
            shared.sessions.sweep_unsubscribed_filters_in_tenant(
                &tenant_for_bind,
                &req.client_id,
                &expired_swept,
                |filter| {
                    shared
                        .router
                        .unsubscribe_in_tenant(filter, &req.client_id, &tenant_for_bind);
                },
            );
            if !expired_swept.is_empty() {
                refresh_subscription_stats(shared);
            }
            // X1-02: opportunistic bounded expiry sweep (one timer per
            // persistent session, bounded: at most
            // `MAX_EXPIRY_SWEEP` sessions scanned per bind). Reaps
            // other detached expired sessions in the owning tenant
            // whose timer elapsed without a reconnect; their router
            // copies go with them. Detach/bind paths only; the
            // publish and delivery paths never scan sessions.
            for (victim, filters) in shared
                .sessions
                .prune_expired_bounded(&tenant_for_bind, broker_session::MAX_EXPIRY_SWEEP)
            {
                shared.sessions.sweep_unsubscribed_filters_in_tenant(
                    &tenant_for_bind,
                    &victim,
                    &filters,
                    |filter| {
                        shared
                            .router
                            .unsubscribe_in_tenant(filter, &victim, &tenant_for_bind);
                    },
                );
                if !filters.is_empty() {
                    refresh_subscription_stats(shared);
                }
            }
            reply
        }
        Err(_) => {
            if looks_v5 {
                let v5 = SessionBindingV5 {
                    granted_expiry: 0,
                    server_recv_max: SERVER_V5_RECEIVE_MAXIMUM,
                    server_max_pkt,
                    reason: v5_reason_string(V5_RC_PROTOCOL_ERROR)
                        .unwrap_or("Connection refused")
                        .to_string(),
                    user_properties: Vec::new(),
                };
                session_binding_reply_with_v5(
                    frame,
                    0,
                    false,
                    V5_RC_PROTOCOL_ERROR,
                    shared.sessions.max_topic_alias(),
                    &v5,
                )
            } else {
                session_binding_reply(frame, 0, false, 2, shared.sessions.max_topic_alias())
            }
        }
    };
    // Sweep the stale router copies so a clean reconnect resubscribes
    // from nothing. Scoped to the owning tenant (MT-03): one tenant's
    // clean start never removes another tenant's subscription for the
    // same client id. Off the hot path (bind only); fan-out takes no new
    // lock and delivery cost is unchanged.

    Some(reply)
}

/// Whether a `SessionBinding` reply accepted the connection.
///
/// The reply metadata is `SessionId:64be | Present:8 | ReturnCode:8`
/// plus the B4-05 trailing `TopicAliasMaximum:16be` (12 bytes total;
/// 10-byte pre-alias replies still count with maximum 0); only return
/// code 0 proceeds to the auto-subscribe hook. Malformed replies never
/// trigger subscriptions.
fn is_binding_accepted(reply: &BrokerFrame) -> bool {
    // 10-byte (pre-alias) and 12-byte heads accept with code 0; v5
    // extended bindings (12-byte head plus `V5Payload | V5Len:16be`)
    // accept the same way. Only return code 0 proceeds to the
    // auto-subscribe hook. Malformed replies never trigger
    // subscriptions.
    if reply.metadata.len() == 10 || reply.metadata.len() == 12 {
        return reply.metadata[9] == 0;
    }
    decode_session_binding_full(&reply.metadata)
        .map(|(_, _, rc, _, _)| rc == 0)
        .unwrap_or(false)
}

/// Subscribe one freshly bound connection to the configured
/// auto-subscribe list (W1-39).
///
/// Runs once per successful bind, never on the per-message path: it
/// clones the capped list once, renders `${clientid}`/`${username}`/
/// `${host}` per entry, and registers each valid filter in the router
/// plus the session mirror. Invalid rendered filters are skipped so one
/// bad template can never fail a connect; the stored templates themselves
/// are validated at the management write, so skips only cover
/// placeholder edge cases.
async fn apply_auto_subscribe(
    shared: &Shared,
    client_id: &str,
    username: Option<&str>,
    conn_id: u64,
) {
    let entries = shared.auto_subscribe.list();
    if entries.is_empty() {
        return;
    }
    // Tenant scope (MT-02, MT-03): auto-subscriptions live in the
    // freshly bound session's tenant, so a wildcard installed here can
    // never observe another tenant's topics. Resolved from the bound
    // connection's own session. Runs once per bind, never per message.
    let tenant = session_tenant_for_conn(shared, conn_id, client_id);
    // Fast superuser flag first (one atomic, no lock, no clone).
    // Only a superuser bypass needs the verified name for metrics.
    let session_for_auth = session_for_conn_if_client(shared, conn_id, client_id);
    let is_superuser = session_for_auth
        .as_ref()
        .is_some_and(|session| session.verified_is_superuser.load(Ordering::Relaxed));
    let verified_username = if is_superuser {
        session_for_auth
            .as_ref()
            .and_then(|session| session.verified_username.read().clone())
    } else {
        None
    };
    let mut applied = false;
    for entry in &entries {
        let rendered =
            broker_api::v5::auto_subscribe::render_auto_topic(&entry.topic, client_id, username);
        let Ok(filter) = TopicFilter::new(&rendered) else {
            continue;
        };
        let Ok(qos) = QoS::try_from(entry.qos) else {
            continue;
        };
        // AZ-02: an automatic subscription follows the same subscribe
        // ACL as a manual one. A denied entry registers nothing, so a
        // denied client receives nothing through auto-subscribe (D2).
        if shared
            .auth
            .authorize_subscribe_in_tenant_as(
                &tenant,
                client_id,
                verified_username.as_deref(),
                &filter,
            )
            .await
            .is_err()
        {
            continue;
        };
        // B4-08 bound: a full subscription table denies the entry fail
        // closed, exactly as `apply_subscribe` does. A denied auto
        // subscription registers nothing (no session mirror) and counts
        // nothing as applied, so a failed subscribe changes no session
        // state.
        let mut auto_sub = Subscription::with_options(
            client_id,
            conn_id,
            qos,
            entry.nl != 0,
            entry.rap != 0,
            entry.rh,
            0,
        );
        auto_sub.tenant = tenant.clone().into();
        let _route_guard = shared.sessions.lock_route_guard();
        if conn_id != 0 && shared.sessions.session_for_conn(conn_id).is_none() {
            break;
        }
        let stored = shared.router.subscribe(&filter, auto_sub);
        if !stored {
            continue;
        }
        shared.sessions.add_subscription_with_options_in_tenant(
            &tenant, client_id, filter, qos, entry.nl, entry.rap, entry.rh,
        );
        applied = true;
    }
    if applied {
        refresh_subscription_stats(shared);
    }
}

/// Longest accepted TLS certificate field (common name, subject or one
/// subject-alt name). Reason: X.509 DN strings are short; 1 KiB bounds
/// bind metadata while covering long distinguished names.
/// Test harness only: production binds carry a decoded request.
#[cfg(test)]
const MAX_CERT_FIELD_LEN: usize = 1024;

/// Most subject-alt names accepted from one client certificate.
/// Reason: client certificates carry short SAN lists; 16 entries bound
/// bind-time allocation while covering multi-SAN certificates.
/// Test harness only: production binds carry a decoded request.
#[cfg(test)]
const MAX_CERT_SANS: usize = 16;

/// Bind flag for a credentials section in the bind tail.
/// Reason: the bit removes the peer-as-username ambiguity.
/// Test harness only: production binds carry a decoded request.
#[cfg(test)]
const BIND_FLAG_CREDS: u8 = 0x08;

/// Bind flag for a peer-address section in the bind tail.
/// Reason: the bit tells the peer section from credentials bytes.
/// Test harness only: production binds carry a decoded request.
#[cfg(test)]
const BIND_FLAG_PEER: u8 = 0x10;

/// Bind flag for a certificate section in the bind tail.
/// Reason: the bit tells the certificate section from other bytes.
/// Test harness only: production binds carry a decoded request.
#[cfg(test)]
const BIND_FLAG_CERT: u8 = 0x20;

/// Decode `BindConnection` metadata.
///
/// Layout: `ClientIdLen:16be | ClientId | Flags:8 | Keepalive:16be`,
/// optionally followed by an F1-01 will section (present iff Flags bit 1
/// is set) `WillQos:8 | WillRetain:8 | TopicLen:16be | Topic |
/// PayloadLen:32be | Payload`, optionally followed by a credentials
/// section `UserLen:16be | Username | PassLen:16be | Password` (present
/// iff Flags bit 3 is set; absent entirely when the client connects
/// anonymously), followed by a peer-address section `PeerLen:16be |
/// PeerIp` (present iff Flags bit 4 is set) carrying the client IP
/// literal the edge saw on its socket (B1-03: drives `peerhost` /
/// `peerhost_net` bans), followed by a certificate section
/// `CertLen:16be | CnLen:16be | Cn | SubjectLen:16be | Subject |
/// SanCount:16be | (SanLen:16be | San)*` (present iff Flags bit 5 is
/// set) carrying the TLS client-certificate fields the edge extracted
/// where TLS terminates (MT-01: expression inputs for tenant
/// assignment), optionally followed by a B4-05 alias section
/// `ClientAliasMax:16be` carrying the client's receive limit (the
/// kernel outbound bound; 0 or absent means the kernel never assigns
/// an alias toward the client).
/// Parse rule: flags decide. A set flag means the section is present.
/// A clear flag means the section is absent. Binds without the new
/// flags use the old position parse. Both decoders use this rule.
/// A short section fails closed with a truncation error.
/// Binds encoded before the peer/alias/will/cert sections existed carry
/// no trailing bytes and still decode with `peerhost` unset, alias
/// maximum 0, no will and no certificate. The keepalive is
/// framing-validated here; supervision lives on the edge. Flags bit 0 is
/// clean_start; Flags bit 1 is will-present (mirrors
/// `indra_brokerlink:encode_bind_meta/7`); Flags bit 2 marks a trailing
/// X1-01 v5 section `AliasMax:16be | V5Payload | V5Len:16be` carrying the
/// negotiated version plus the decoded CONNECT properties the kernel
/// needs (mirrors `indra_brokerlink:encode_bind_meta/9`). Version-4
/// binds carry no v5 section and decode with defaulted v5 fields.
/// Largest v5 section: [`MAX_V5_SECTION_LEN`] bytes (one section must
/// leave room for the head, credentials, peer, certificate, will and
/// alias sections inside the 65535-byte BrokerLink meta cap; 4 KiB
/// covers realistic CONNECT properties). At most [`MAX_V5_BIND_USERS`]
/// user properties per bind (bounds per-bind memory on the handshake
/// path; mirrors the 16-SAN bound on the certificate section), each
/// key/value at most [`MAX_V5_BIND_STRING`] bytes (bounds per-bind
/// memory; mirrors the 1024-byte bound on certificate fields).
pub(crate) fn session_credential_allows(
    session: &broker_session::Session,
    topic: &str,
    is_publish: bool,
) -> bool {
    let guard = session.credential_acls.read();
    let Some(rules) = guard.as_ref() else {
        return true;
    };
    for rule in rules {
        let action_matches = match rule.action {
            0 => is_publish,
            1 => !is_publish,
            _ => true,
        };
        if !action_matches {
            continue;
        }
        let acl_rule = broker_auth::AclRule {
            client_pattern: "*".to_string(),
            topic_pattern: rule.topic_pattern.clone(),
            action: match rule.action {
                0 => broker_auth::AclAction::Publish,
                1 => broker_auth::AclAction::Subscribe,
                _ => broker_auth::AclAction::All,
            },
            allow: rule.allow,
        };
        let matched = if is_publish {
            Topic::new(topic).is_ok_and(|t| acl_rule.topic_matches(&t))
        } else {
            TopicFilter::new(topic).is_ok_and(|f| acl_rule.matches_subscribe(&f))
        };
        if matched {
            return rule.allow;
        }
    }
    false
}

pub(crate) async fn prune_revoked_subscriptions(
    shared: &Shared,
    session: &Arc<broker_session::Session>,
    tenant: &str,
) {
    let expected_session_id = session.id;
    let expected_conn_id = *session.conn_id.read();
    let subs: Vec<TopicFilter> = session.subscriptions.read().keys().cloned().collect();
    if subs.is_empty() {
        return;
    }
    let is_superuser = session.verified_is_superuser.load(Ordering::Relaxed);
    let verified = session.verified_username.read().clone();
    let mut revoked = Vec::new();
    for filter in subs {
        let auth_ok = if is_superuser {
            shared
                .auth
                .authorize_subscribe_in_tenant_as(
                    tenant,
                    &session.client_id,
                    verified.as_deref(),
                    &filter,
                )
                .await
                .is_ok()
        } else {
            shared
                .auth
                .authorize_subscribe_in_tenant(tenant, &session.client_id, &filter)
                .await
                .is_ok()
        };
        if !auth_ok || !session_credential_allows(session, filter.as_str(), false) {
            revoked.push(filter);
        }
    }
    if revoked.is_empty() {
        return;
    }
    let _route_guard = shared.sessions.lock_route_guard();
    let still_current = shared
        .sessions
        .get_in_tenant(tenant, &session.client_id)
        .is_some_and(|cur| {
            cur.id == expected_session_id && *cur.conn_id.read() == expected_conn_id
        });
    if !still_current {
        return;
    }
    for filter in &revoked {
        shared
            .sessions
            .remove_subscription_in_tenant(tenant, &session.client_id, filter);
        unsubscribe_scoped_in_tenant(shared, tenant, &session.client_id, filter);
    }
    drop(_route_guard);
    refresh_subscription_stats(shared);
}

#[derive(Clone)]
struct BindRequest {
    client_id: String,
    clean_start: bool,
    keepalive_secs: u16,
    username: Option<String>,
    password: Option<Vec<u8>>,
    peerhost: Option<String>,
    /// Client's Topic Alias Maximum from CONNECT (kernel outbound bound).
    client_alias_max: u16,
    /// Last will from CONNECT, if the client registered one.
    will: Option<BindWill>,
    /// TLS client-certificate common name forwarded in the bind, if any.
    cert_cn: Option<String>,
    /// TLS client-certificate subject forwarded in the bind, if any.
    cert_subject: Option<String>,
    /// TLS client-certificate subject-alt names forwarded in the bind.
    cert_sans: Vec<String>,
    /// Negotiated protocol level from the bind v5 section (4 when the
    /// edge sent no v5 section). Drives the v5 session policy (X1-02):
    /// version-4 binds keep 3.1.1 behaviour bit for bit.
    protocol_version: u8,
    /// Session Expiry Interval from CONNECT (seconds). Applied at
    /// session resolution (X1-02): 0 drops detached state at
    /// disconnect, nonzero retains it past reconnect within the
    /// interval, absent defaults to
    /// [`broker_session::DEFAULT_SESSION_EXPIRY_SECS`] with its
    /// reason on the session type.
    session_expiry: u32,
    /// Receive Maximum from CONNECT. Enforced as a cap on what the
    /// kernel sends the connection (X1-02): never more unacknowledged
    /// QoS 1/2 than the client allows (shed with `0x97`, counted).
    /// A wire 0 is rejected at bind (protocol error), never stored.
    receive_maximum: u16,
    /// Maximum Packet Size from CONNECT (0 means the client announced
    /// no limit). Enforced as a cap on what the kernel sends the
    /// connection (X1-02): never a frame above the maximum (shed with
    /// `0x95`, counted).
    max_packet_size: u32,
    /// Client user properties from CONNECT (`Some` when the edge sent
    /// a v5 section, even when empty; `None` on version-4 binds).
    /// Echoed back on v5 failure CONNACKs (X1-02); success CONNACKs
    /// omit them. No session, routing, auth or tenant logic reads
    /// them beyond the echo.
    user_properties: Option<std::collections::HashMap<String, String>>,
    /// Listener name the connection arrived on (`""` means default).
    /// The edge reports it. The kernel native path sets it.
    listener: String,
    /// True only when native enhanced authentication (SCRAM-SHA-256)
    /// verified the client proof for this CONNECT.
    enhanced_auth_verified: bool,
    /// Credential-derived ACL rules verified during enhanced authentication,
    /// installed on the session only after admission succeeds.
    pending_acls: Option<Vec<broker_session::CredentialAclRule>>,
}

/// Longest accepted listener name in a bind.
/// Reason: listener names are short labels, so 64 chars bound bind memory.
/// Test harness only: production binds carry a decoded request.
#[cfg(test)]
const MAX_BIND_LISTENER_LEN: usize = 64;

/// Bind flag for a listener-name section in the bind tail.
/// Reason: the bit tells the listener section from other bytes.
/// Test harness only: production binds carry a decoded request.
#[cfg(test)]
const BIND_FLAG_LISTENER: u8 = 0x40;

/// Largest v5 bind section in bytes (see [`BindRequest`]).
/// Test harness only: production binds carry a decoded request.
#[cfg(test)]
const MAX_V5_SECTION_LEN: usize = 4096;
/// Most client user properties carried per bind (see [`BindRequest`]).
const MAX_V5_BIND_USERS: usize = 16;
/// Largest single user-property key or value in a bind (see [`BindRequest`]).
const MAX_V5_BIND_STRING: usize = 1024;

/// Last will as carried in the bind (validated into a session will by
/// [`will_from_bind`]).
#[derive(Clone)]
struct BindWill {
    topic: String,
    payload: Bytes,
    qos: u8,
    retain: bool,
}

/// Decode `BindConnection` metadata. Test harness only.
#[cfg(test)]
fn decode_bind_meta(meta: &[u8]) -> Result<BindRequest, &'static str> {
    if meta.len() < 5 {
        return Err("bind meta too short");
    }
    let id_len = u16::from_be_bytes([meta[0], meta[1]]) as usize;
    if meta.len() < 2 + id_len + 1 + 2 {
        return Err("bind meta too short");
    }
    let id_bytes = &meta[2..2 + id_len];
    let flags = meta[2 + id_len];
    let keepalive_secs = u16::from_be_bytes([meta[3 + id_len], meta[4 + id_len]]);
    let client_id = std::str::from_utf8(id_bytes).map_err(|_| "client id not UTF-8")?;
    let mut rest = &meta[5 + id_len..];
    // F1-01: the will section rides immediately after the keepalive when
    // Flags bit 1 is set, ahead of credentials/peer/alias, so the
    // remainder below parses exactly like a legacy bind.
    let will = if flags & 0x02 != 0 {
        let (parsed, tail) = decode_bind_will(rest)?;
        rest = tail;
        Some(parsed)
    } else {
        None
    };
    // TM-07: flags decide. A set section flag means the section is
    // present. A clear flag means it is absent. Flagged binds parse
    // by flags, so peer bytes never become a username. Binds without
    // the flags keep the old position parse below.
    if flags & (BIND_FLAG_CREDS | BIND_FLAG_PEER | BIND_FLAG_CERT | BIND_FLAG_LISTENER) != 0 {
        return decode_flagged_bind(client_id, flags, keepalive_secs, will, rest);
    }
    // X1-01: Flags bit 2 marks a trailing v5 section
    // `AliasMax:16be | V5Payload | V5Len:16be'. The section is stripped
    // from the end first; the remainder (ending in the alias maximum)
    // then parses exactly like a legacy alias-carrying bind. Anything
    // short or out of range is malformed (fail closed, never a guessed
    // version or identity). The decoded values are stored on the
    // request and ignored: X1-02 owns the session semantics.
    if flags & 0x04 != 0 {
        let (legacy_rest, v5) = decode_bind_v5_section(rest)?;
        let (identity, client_alias_max) = decode_legacy_tail(legacy_rest)?;
        return Ok(BindRequest {
            client_id: client_id.to_string(),
            clean_start: flags & 0x01 != 0,
            keepalive_secs,
            username: identity.username,
            password: identity.password,
            peerhost: identity.peerhost,
            client_alias_max,
            will,
            cert_cn: identity.cert_cn,
            cert_subject: identity.cert_subject,
            cert_sans: identity.cert_sans,
            protocol_version: v5.version,
            session_expiry: v5.session_expiry,
            receive_maximum: v5.receive_maximum,
            max_packet_size: v5.max_packet_size,
            user_properties: Some(v5.user_properties),
            listener: identity.listener,
            enhanced_auth_verified: false,
            pending_acls: None,
        });
    }
    // Preferred: exact legacy parse (no alias section).
    if let Ok(identity) = decode_bind_identity(rest) {
        return Ok(BindRequest {
            client_id: client_id.to_string(),
            clean_start: flags & 0x01 != 0,
            keepalive_secs,
            username: identity.username,
            password: identity.password,
            peerhost: identity.peerhost,
            client_alias_max: 0,
            will,
            cert_cn: identity.cert_cn,
            cert_subject: identity.cert_subject,
            cert_sans: identity.cert_sans,
            protocol_version: 4,
            session_expiry: 0,
            receive_maximum: u16::MAX,
            max_packet_size: 0,
            user_properties: None,
            listener: identity.listener,
            enhanced_auth_verified: false,
            pending_acls: None,
        });
    }
    // Otherwise the last two bytes may be the alias section: the prefix
    // must parse exactly as a legacy bind or the whole frame is malformed
    // (fail closed, never a guessed identity).
    if rest.len() >= 2 {
        let (prefix, suffix) = rest.split_at(rest.len() - 2);
        if let Ok(identity) = decode_bind_identity(prefix) {
            let client_alias_max =
                protocol_v5::decode_alias_maximum(suffix).unwrap_or(protocol_v5::NO_TOPIC_ALIAS);
            return Ok(BindRequest {
                client_id: client_id.to_string(),
                clean_start: flags & 0x01 != 0,
                keepalive_secs,
                username: identity.username,
                password: identity.password,
                peerhost: identity.peerhost,
                client_alias_max,
                will,
                cert_cn: identity.cert_cn,
                cert_subject: identity.cert_subject,
                cert_sans: identity.cert_sans,
                protocol_version: 4,
                session_expiry: 0,
                receive_maximum: u16::MAX,
                max_packet_size: 0,
                user_properties: None,
                listener: identity.listener,
                enhanced_auth_verified: false,
                pending_acls: None,
            });
        }
    }
    Err("bind meta length mismatch")
}

/// Decode the credentials/peer/certificate tail allowing the trailing
/// alias section: exact legacy parse first (no alias, maximum 0), else
/// the last two bytes as the alias maximum over a legacy prefix.
/// Anything else is malformed (fail closed).
#[cfg(test)]
fn decode_legacy_tail(rest: &[u8]) -> Result<(BindIdentity, u16), &'static str> {
    if let Ok(identity) = decode_bind_identity(rest) {
        return Ok((identity, 0));
    }
    if rest.len() >= 2 {
        let (prefix, suffix) = rest.split_at(rest.len() - 2);
        if let Ok(identity) = decode_bind_identity(prefix) {
            let client_alias_max =
                protocol_v5::decode_alias_maximum(suffix).unwrap_or(protocol_v5::NO_TOPIC_ALIAS);
            return Ok((identity, client_alias_max));
        }
    }
    Err("bind meta length mismatch")
}

/// Decode a flagged bind tail. Flags decide each section.
/// A short section fails closed with a truncation error.
#[cfg(test)]
fn decode_flagged_bind(
    client_id: &str,
    flags: u8,
    keepalive_secs: u16,
    will: Option<BindWill>,
    rest: &[u8],
) -> Result<BindRequest, &'static str> {
    // X1-01: strip the v5 section first when bit 2 is set.
    // The remainder keeps the alias maximum for the tail decoder.
    let (tail, v5) = if flags & 0x04 != 0 {
        let (legacy_rest, parsed) = decode_bind_v5_section(rest)?;
        (legacy_rest, Some(parsed))
    } else {
        (rest, None)
    };
    let (identity, client_alias_max) = decode_flagged_tail(tail, flags)?;
    let (protocol_version, session_expiry, receive_maximum, max_packet_size, user_properties) =
        match v5 {
            Some(parsed) => (
                parsed.version,
                parsed.session_expiry,
                parsed.receive_maximum,
                parsed.max_packet_size,
                Some(parsed.user_properties),
            ),
            None => (4, 0, u16::MAX, 0, None),
        };
    Ok(BindRequest {
        client_id: client_id.to_string(),
        clean_start: flags & 0x01 != 0,
        keepalive_secs,
        username: identity.username,
        password: identity.password,
        peerhost: identity.peerhost,
        client_alias_max,
        will,
        cert_cn: identity.cert_cn,
        cert_subject: identity.cert_subject,
        cert_sans: identity.cert_sans,
        protocol_version,
        session_expiry,
        receive_maximum,
        max_packet_size,
        user_properties,
        listener: identity.listener,
        enhanced_auth_verified: false,
        pending_acls: None,
    })
}

/// Decode the flagged tail allowing the trailing alias section.
/// Exact flagged parse first (no alias), else the last two bytes
/// carry the alias maximum over a flagged prefix. Fail closed.
#[cfg(test)]
fn decode_flagged_tail(rest: &[u8], flags: u8) -> Result<(BindIdentity, u16), &'static str> {
    if let Ok(identity) = decode_flagged_identity(rest, flags) {
        return Ok((identity, 0));
    }
    if rest.len() >= 2 {
        let (prefix, suffix) = rest.split_at(rest.len() - 2);
        if let Ok(identity) = decode_flagged_identity(prefix, flags) {
            let client_alias_max =
                protocol_v5::decode_alias_maximum(suffix).unwrap_or(protocol_v5::NO_TOPIC_ALIAS);
            return Ok((identity, client_alias_max));
        }
    }
    Err("bind meta length mismatch")
}

/// Decode flagged credentials, peer, certificate and listener sections.
/// A set flag needs its section. A clear flag needs no bytes.
/// A short section fails with a truncation error.
#[cfg(test)]
fn decode_flagged_identity(rest: &[u8], flags: u8) -> Result<BindIdentity, &'static str> {
    let mut cursor = rest;
    let (username, password) = if flags & BIND_FLAG_CREDS != 0 {
        let (user, pass, tail) = take_flagged_creds(cursor)?;
        cursor = tail;
        (Some(user), Some(pass))
    } else {
        (None, None)
    };
    let peerhost = if flags & BIND_FLAG_PEER != 0 {
        let (peer, tail) = take_flagged_peer(cursor)?;
        cursor = tail;
        Some(peer)
    } else {
        None
    };
    let (cert_cn, cert_subject, cert_sans) = if flags & BIND_FLAG_CERT != 0 {
        let (cn, subject, sans, tail) = take_flagged_cert(cursor)?;
        cursor = tail;
        (cn, subject, sans)
    } else {
        (None, None, Vec::new())
    };
    let listener = if flags & BIND_FLAG_LISTENER != 0 {
        let (name, tail) = take_flagged_listener(cursor)?;
        cursor = tail;
        name
    } else {
        String::new()
    };
    if !cursor.is_empty() {
        return Err("bind meta length mismatch");
    }
    Ok(BindIdentity {
        username,
        password,
        peerhost,
        cert_cn,
        cert_subject,
        cert_sans,
        listener,
    })
}

/// Take one flagged listener section off the front.
/// The name is UTF-8 and at most 64 chars. Short input fails.
#[cfg(test)]
fn take_flagged_listener(cursor: &[u8]) -> Result<(String, &[u8]), &'static str> {
    if cursor.len() < 2 {
        return Err("bind listener truncated");
    }
    let len = u16::from_be_bytes([cursor[0], cursor[1]]) as usize;
    if len > MAX_BIND_LISTENER_LEN || cursor.len() < 2 + len {
        return Err("bind listener truncated");
    }
    let name = std::str::from_utf8(&cursor[2..2 + len]).map_err(|_| "listener not UTF-8")?;
    Ok((name.to_string(), &cursor[2 + len..]))
}

/// Take one flagged credentials section off the front.
/// Empty password fails. Short input fails as truncated.
#[cfg(test)]
fn take_flagged_creds(cursor: &[u8]) -> Result<(String, Vec<u8>, &[u8]), &'static str> {
    if cursor.len() < 2 {
        return Err("bind credentials truncated");
    }
    let user_len = u16::from_be_bytes([cursor[0], cursor[1]]) as usize;
    if cursor.len() < 2 + user_len + 2 {
        return Err("bind credentials truncated");
    }
    let username =
        std::str::from_utf8(&cursor[2..2 + user_len]).map_err(|_| "username not UTF-8")?;
    let base = 2 + user_len;
    let pass_len = u16::from_be_bytes([cursor[base], cursor[base + 1]]) as usize;
    if pass_len == 0 || cursor.len() < base + 2 + pass_len {
        return Err("bind credentials truncated");
    }
    let password = cursor[base + 2..base + 2 + pass_len].to_vec();
    Ok((
        username.to_string(),
        password,
        &cursor[base + 2 + pass_len..],
    ))
}

/// Take one flagged peer section off the front.
/// Only IP literals pass. Short input fails as truncated.
#[cfg(test)]
fn take_flagged_peer(cursor: &[u8]) -> Result<(String, &[u8]), &'static str> {
    let (peer, tail) = split_peer_prefix(cursor).ok_or("bind peer truncated")?;
    Ok((peer, tail))
}

/// Decoded flagged certificate section plus the rest.
#[cfg(test)]
type FlaggedCert<'a> = (Option<String>, Option<String>, Vec<String>, &'a [u8]);

/// Take one flagged certificate section off the front.
/// Short input fails as truncated.
#[cfg(test)]
fn take_flagged_cert(cursor: &[u8]) -> Result<FlaggedCert<'_>, &'static str> {
    if cursor.len() < 2 {
        return Err("bind certificate truncated");
    }
    let cert_len = u16::from_be_bytes([cursor[0], cursor[1]]) as usize;
    if cursor.len() < 2 + cert_len {
        return Err("bind certificate truncated");
    }
    let section = &cursor[..2 + cert_len];
    let tail = &cursor[2 + cert_len..];
    let (cn, subject, sans) = decode_cert_section(section).ok_or("bind certificate truncated")?;
    Ok((cn, subject, sans, tail))
}

/// Decoded X1-01 v5 section: the negotiated version plus the CONNECT
/// properties the kernel needs.
#[cfg(test)]
struct BindV5 {
    version: u8,
    session_expiry: u32,
    receive_maximum: u16,
    max_packet_size: u32,
    user_properties: std::collections::HashMap<String, String>,
}

/// Strip the trailing v5 section `AliasMax:16be | V5Payload |
/// V5Len:16be' off the bind tail. Returns the remainder (ending in the
/// alias maximum, parsed by [`decode_legacy_tail`]) plus the decoded v5
/// fields. Every length is checked (fail closed, never a guessed
/// version).
#[cfg(test)]
fn decode_bind_v5_section(rest: &[u8]) -> Result<(&[u8], BindV5), &'static str> {
    // Smallest payload: Version:8 | Expiry:32 | RecvMax:16 | MaxPkt:32
    // | Count:16 with zero pairs (13 bytes), plus the alias maximum
    // and the section length around it.
    if rest.len() < 2 + 13 + 2 {
        return Err("bind v5 section truncated");
    }
    let v5_len = u16::from_be_bytes([rest[rest.len() - 2], rest[rest.len() - 1]]) as usize;
    if !(13..=MAX_V5_SECTION_LEN).contains(&v5_len) || rest.len() < 2 + v5_len + 2 {
        return Err("bind v5 section length mismatch");
    }
    let payload_start = rest.len() - 2 - v5_len;
    let payload = &rest[payload_start..payload_start + v5_len];
    let version = payload[0];
    if version != 4 && version != 5 {
        return Err("bind v5 version out of range");
    }
    let session_expiry = u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]);
    let receive_maximum = u16::from_be_bytes([payload[5], payload[6]]);
    let max_packet_size = u32::from_be_bytes([payload[7], payload[8], payload[9], payload[10]]);
    let pair_count = u16::from_be_bytes([payload[11], payload[12]]) as usize;
    if pair_count > MAX_V5_BIND_USERS {
        return Err("bind v5 user properties over bound");
    }
    let mut cursor = &payload[13..];
    let mut user_properties = std::collections::HashMap::with_capacity(pair_count);
    for _ in 0..pair_count {
        let (key, tail) = take_v5_string(cursor)?;
        let (value, tail) = take_v5_string(tail)?;
        user_properties.insert(key, value);
        cursor = tail;
    }
    if !cursor.is_empty() {
        return Err("bind v5 section trailing bytes");
    }
    // The remainder ends in the alias maximum and parses through the
    // legacy tail decoder (exact or alias-stripped).
    Ok((
        &rest[..payload_start],
        BindV5 {
            version,
            session_expiry,
            receive_maximum,
            max_packet_size,
            user_properties,
        },
    ))
}

/// Take one length-prefixed string off the front of the v5 user-property
/// cursor. Overlong (beyond [`MAX_V5_BIND_STRING`]), non-UTF-8 or
/// truncated fails the whole section (fail closed).
#[cfg(test)]
fn take_v5_string(cursor: &[u8]) -> Result<(String, &[u8]), &'static str> {
    if cursor.len() < 2 {
        return Err("bind v5 string truncated");
    }
    let len = u16::from_be_bytes([cursor[0], cursor[1]]) as usize;
    if len > MAX_V5_BIND_STRING || cursor.len() < 2 + len {
        return Err("bind v5 string over bound");
    }
    let text = std::str::from_utf8(&cursor[2..2 + len]).map_err(|_| "bind v5 string not UTF-8")?;
    Ok((text.to_string(), &cursor[2 + len..]))
}

/// Decode the F1-01 will section at the head of the bind tail:
/// `WillQos:8 | WillRetain:8 | TopicLen:16be | Topic | PayloadLen:32be |
/// Payload`. Returns the parsed will plus the unconsumed tail
/// (credentials/peer/alias). Anything short or out of range is malformed
/// (fail closed, never a guessed will).
#[cfg(test)]
fn decode_bind_will(rest: &[u8]) -> Result<(BindWill, &[u8]), &'static str> {
    if rest.len() < 8 {
        return Err("bind will truncated");
    }
    let qos = rest[0];
    let retain = rest[1];
    if qos > 2 || retain > 1 {
        return Err("bind will flags out of range");
    }
    let topic_len = u16::from_be_bytes([rest[2], rest[3]]) as usize;
    if topic_len == 0 || rest.len() < 4 + topic_len + 4 {
        return Err("bind will topic truncated");
    }
    let topic = std::str::from_utf8(&rest[4..4 + topic_len]).map_err(|_| "will topic not UTF-8")?;
    let base = 4 + topic_len;
    let payload_len =
        u32::from_be_bytes([rest[base], rest[base + 1], rest[base + 2], rest[base + 3]]) as usize;
    if rest.len() < base + 4 + payload_len {
        return Err("bind will payload truncated");
    }
    let payload = Bytes::copy_from_slice(&rest[base + 4..base + 4 + payload_len]);
    Ok((
        BindWill {
            topic: topic.to_string(),
            payload,
            qos,
            retain: retain != 0,
        },
        &rest[base + 4 + payload_len..],
    ))
}

/// Validate a bind will into the session will: the topic must be a
/// concrete publish topic (no wildcards) and the QoS a known level.
/// `None` (CONNECT without a will) stays `None` (clears any previous).
/// Fail closed: an unusable will rejects the bind, never a guessed topic.
fn will_from_bind(req: &BindRequest) -> Result<Option<broker_session::StoredWill>, &'static str> {
    let Some(will) = req.will.as_ref() else {
        return Ok(None);
    };
    let topic = Topic::new(will.topic.clone()).map_err(|_| "will topic invalid")?;
    let qos = QoS::try_from(will.qos).map_err(|_| "will qos invalid")?;
    Ok(Some(broker_session::StoredWill {
        tenant: broker_session::tenant::DEFAULT_TENANT_ID.to_string(),
        topic,
        qos,
        retain: will.retain,
        payload: will.payload.clone(),
    }))
}

/// Observe the v5 bind properties on the CONNECT event (X1-02). Reads
/// the negotiated version plus the CONNECT properties the session
/// resolution and the delivery caps just applied, so the bind contract
/// stays live in the kernel logs. CONNECT-only, never on the delivery
/// path.
#[cfg(test)]
fn observe_bind_v5(req: &BindRequest) {
    tracing::debug!(
        client_id = %req.client_id,
        protocol_version = req.protocol_version,
        session_expiry = req.session_expiry,
        receive_maximum = req.receive_maximum,
        max_packet_size = req.max_packet_size,
        user_properties = req
            .user_properties
            .as_ref()
            .map(|m| m.len())
            .unwrap_or(0),
        "v5 bind properties applied (X1-02 session semantics)"
    );
}

/// Credentials plus peer address plus certificate fields decoded from
/// the bind tail. The listener name rides only in flagged binds.
#[cfg(test)]
struct BindIdentity {
    username: Option<String>,
    password: Option<Vec<u8>>,
    peerhost: Option<String>,
    cert_cn: Option<String>,
    cert_subject: Option<String>,
    cert_sans: Vec<String>,
    listener: String,
}

/// Decode the optional trailing credentials, peer-address and
/// certificate sections: empty means anonymous with no peer address or
/// certificate; otherwise a credentials section `UserLen | Username |
/// PassLen | Password` (password non-empty, mirroring the edge
/// `decode_bind_creds' `PLen > 0' guard), optionally followed by a peer
/// section `PeerLen:16be | PeerIp` (an IP literal), optionally followed
/// by a certificate section `CertLen:16be | CnLen:16be | Cn |
/// SubjectLen:16be | Subject | SanCount:16be | (SanLen:16be | San)*`
/// carrying the TLS client-certificate fields. An anonymous bind from a
/// peer-aware edge carries only the peer (and optional certificate)
/// sections. A trailing section that is none of these is malformed,
/// exactly as trailing garbage was before the peer section.
#[cfg(test)]
fn decode_bind_identity(rest: &[u8]) -> Result<BindIdentity, &'static str> {
    if rest.is_empty() {
        return Ok(BindIdentity {
            username: None,
            password: None,
            peerhost: None,
            cert_cn: None,
            cert_subject: None,
            cert_sans: Vec::new(),
            listener: String::new(),
        });
    }
    if rest.len() < 2 {
        return Err("bind credentials truncated");
    }
    let user_len = u16::from_be_bytes([rest[0], rest[1]]) as usize;
    if rest.len() < 2 + user_len + 2 {
        // Too short for credentials: it can only be an anonymous bind
        // carrying just the peer and optional certificate sections.
        return match split_peer_prefix(rest) {
            Some((peerhost, after_peer)) => {
                let (cert_cn, cert_subject, cert_sans) =
                    decode_cert_tail(after_peer).ok_or("bind meta length mismatch")?;
                Ok(BindIdentity {
                    username: None,
                    password: None,
                    peerhost: Some(peerhost),
                    cert_cn,
                    cert_subject,
                    cert_sans,
                    listener: String::new(),
                })
            }
            None => match decode_cert_tail(rest) {
                Some((cert_cn, cert_subject, cert_sans)) => Ok(BindIdentity {
                    username: None,
                    password: None,
                    peerhost: None,
                    cert_cn,
                    cert_subject,
                    cert_sans,
                    listener: String::new(),
                }),
                None => Err("bind credentials truncated"),
            },
        };
    }
    let username = std::str::from_utf8(&rest[2..2 + user_len]).map_err(|_| "username not UTF-8")?;
    let base = 2 + user_len;
    let pass_len = u16::from_be_bytes([rest[base], rest[base + 1]]) as usize;
    if rest.len() < base + 2 + pass_len {
        return Err("bind credentials truncated");
    }
    // Parse rule: flags decide. Flagged binds never reach this
    // legacy path, so peer bytes never become a username there.
    // This legacy path keeps the old position parse for binds
    // without the flags. Credentials win here, exactly as before.
    // An anonymous bind carrying peer plus certificate sections is
    // byte-identical to credentials whose username is the peer
    // literal and whose password is the certificate payload.
    // B4-05 alias collision: an anonymous peer section plus the trailing
    // `ClientAliasMax:16be' of 0 is byte-identical to credentials with the
    // peer literal as username and an empty password (`PeerLen|Peer|0x0000'
    // == `ULen|User|PLen=0'). The edge never encodes an empty password
    // (`decode_bind_creds' requires `PLen > 0', a username without a
    // password is rejected at encode time), so an empty `pass_len' here
    // can only be that collision: fail this parse so the caller strips the
    // alias section and retries as an anonymous peer bind. Fail closed:
    // the peer address is never mistaken for a username, so address bans
    // still match on every alias-carrying bind the edge sends.
    if pass_len == 0 {
        return Err("bind credentials truncated");
    }
    let password = rest[base + 2..base + 2 + pass_len].to_vec();
    let tail = &rest[base + 2 + pass_len..];
    if tail.is_empty() {
        return Ok(BindIdentity {
            username: Some(username.to_string()),
            password: Some(password),
            peerhost: None,
            cert_cn: None,
            cert_subject: None,
            cert_sans: Vec::new(),
            listener: String::new(),
        });
    }
    // After credentials: optional peer prefix, then optional certificate
    // tail. Either section may be absent; anything else is malformed.
    if let Some((peerhost, after_peer)) = split_peer_prefix(tail) {
        let (cert_cn, cert_subject, cert_sans) =
            decode_cert_tail(after_peer).ok_or("bind meta length mismatch")?;
        return Ok(BindIdentity {
            username: Some(username.to_string()),
            password: Some(password),
            peerhost: Some(peerhost),
            cert_cn,
            cert_subject,
            cert_sans,
            listener: String::new(),
        });
    }
    if let Some((cert_cn, cert_subject, cert_sans)) = decode_cert_tail(tail) {
        // A well-formed certificate section without a peer section is
        // accepted (the edge may forward certificate fields while the
        // peer section is absent); anything else is malformed.
        return Ok(BindIdentity {
            username: Some(username.to_string()),
            password: Some(password),
            peerhost: None,
            cert_cn,
            cert_subject,
            cert_sans,
            listener: String::new(),
        });
    }
    Err("bind meta length mismatch")
}

/// Split one peer-address prefix `PeerLen:16be | PeerIp` off the front of
/// `section`, returning the IP literal plus the unconsumed tail (peer,
/// certificate or alias sections). `None` for a missing, empty,
/// non-UTF-8 or non-IP peer: only IP literals travel here, anything else
/// is malformed input, never a peer identity to match bans against.
#[cfg(test)]
fn split_peer_prefix(section: &[u8]) -> Option<(String, &[u8])> {
    if section.len() < 2 {
        return None;
    }
    let peer_len = u16::from_be_bytes([section[0], section[1]]) as usize;
    if peer_len == 0 || section.len() < 2 + peer_len {
        return None;
    }
    let literal = std::str::from_utf8(&section[2..2 + peer_len]).ok()?;
    literal.parse::<std::net::IpAddr>().ok()?;
    Some((literal.to_string(), &section[2 + peer_len..]))
}

/// Decode the optional trailing certificate tail: empty means no
/// certificate; otherwise exactly one `CertLen:16be | CertPayload`
/// section that must consume the whole tail, else `None` (malformed).
#[cfg(test)]
fn decode_cert_tail(tail: &[u8]) -> Option<(Option<String>, Option<String>, Vec<String>)> {
    if tail.is_empty() {
        return Some((None, None, Vec::new()));
    }
    decode_cert_section(tail)
}

/// Decode one certificate section `CertLen:16be | CertPayload` where the
/// payload is `CnLen:16be | Cn | SubjectLen:16be | Subject |
/// SanCount:16be | (SanLen:16be | San)*`. All strings must be UTF-8 and
/// within [`MAX_CERT_FIELD_LEN`], with at most [`MAX_CERT_SANS`] SANs;
/// anything else is not a certificate section (fail closed: the caller
/// treats it as malformed bind metadata).
#[cfg(test)]
fn decode_cert_section(section: &[u8]) -> Option<(Option<String>, Option<String>, Vec<String>)> {
    if section.len() < 2 {
        return None;
    }
    let cert_len = u16::from_be_bytes([section[0], section[1]]) as usize;
    if section.len() != 2 + cert_len {
        return None;
    }
    let mut cursor = &section[2..];
    let cn = take_cert_string(&mut cursor)?;
    let subject = take_cert_string(&mut cursor)?;
    if cursor.len() < 2 {
        return None;
    }
    let san_count = u16::from_be_bytes([cursor[0], cursor[1]]) as usize;
    if san_count > MAX_CERT_SANS {
        return None;
    }
    cursor = &cursor[2..];
    let mut sans = Vec::with_capacity(san_count);
    for _ in 0..san_count {
        let san = take_cert_string(&mut cursor)?;
        if let Some(san) = san {
            sans.push(san);
        }
    }
    if !cursor.is_empty() {
        return None;
    }
    Some((cn, subject, sans))
}

/// Take one length-prefixed certificate string off the front of
/// `cursor`. Empty decodes as absent (`None`); overlong or non-UTF-8
/// fails the whole section (`None` at the section level).
#[cfg(test)]
fn take_cert_string(cursor: &mut &[u8]) -> Option<Option<String>> {
    if cursor.len() < 2 {
        return None;
    }
    let len = u16::from_be_bytes([cursor[0], cursor[1]]) as usize;
    if len > MAX_CERT_FIELD_LEN || cursor.len() < 2 + len {
        return None;
    }
    let bytes = &cursor[2..2 + len];
    *cursor = &cursor[2 + len..];
    if len == 0 {
        return Some(None);
    }
    let text = std::str::from_utf8(bytes).ok()?;
    Some(Some(text.to_string()))
}

/// State shared by every BrokerLink connection task on this node.
/// One publish queued for the background stream writer.
#[derive(Debug)]
struct JournalEntry {
    topic: Topic,
    qos: QoS,
    payload: Bytes,
    timestamp_ms: u64,
}

/// Bounded async journal from the publish path to [`DurableStreamStore`].
///
/// The publish path calls [`try_record`](StreamJournalHandle::try_record),
/// which does one non-blocking `try_send` and never awaits file I/O, so an
/// fsync can never stall delivery. A single background task owns the
/// receiver and calls `append_with_timestamp` (fsync per record under the
/// default policy). Capacity is 1024 entries; when full the newest record
/// is dropped and `dropped` increments. Durability window: channel delay
/// plus one fsync (usually well under 10 ms); dropped records are lost and
/// counted, never retried. Memory is bounded by the channel plus one
/// in-flight append.
#[derive(Debug)]
#[allow(dead_code)]
struct StreamJournalHandle {
    store: Arc<DurableStreamStore>,
    tx: tokio::sync::mpsc::Sender<JournalEntry>,
    dropped: Arc<AtomicU64>,
}

#[allow(dead_code)]
impl StreamJournalHandle {
    fn open(
        dir: impl AsRef<std::path::Path>,
        config: StreamConfig,
    ) -> std::result::Result<Arc<Self>, broker_storage::StorageError> {
        let store = Arc::new(DurableStreamStore::open_with_config(dir, config)?);
        let (tx, mut rx) = tokio::sync::mpsc::channel::<JournalEntry>(1024);
        let writer = store.clone();
        tokio::spawn(async move {
            while let Some(entry) = rx.recv().await {
                if let Err(e) = writer.append_with_timestamp(
                    entry.topic,
                    entry.qos,
                    entry.payload,
                    std::collections::HashMap::new(),
                    entry.timestamp_ms,
                ) {
                    warn!("Stream journal append failed: {e}");
                }
            }
        });
        Ok(Arc::new(Self {
            store,
            tx,
            dropped: Arc::new(AtomicU64::new(0)),
        }))
    }

    /// Enqueue one publish without blocking. Drops (and counts) the record
    /// when the channel is full so delivery never waits for disk.
    fn try_record(&self, topic: &Topic, qos: QoS, payload: &Bytes) {
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let entry = JournalEntry {
            topic: topic.clone(),
            qos,
            payload: payload.clone(),
            timestamp_ms,
        };
        if self.tx.try_send(entry).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn dropped_count(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// Journal one publish when enabled, otherwise one `is_none` check.
///
/// Off the hot path by construction: no await, no file I/O, at most a
/// small clone plus a channel send. Empty journals (the default) cost the
/// branch only.
fn maybe_journal_stream(shared: &Shared, topic: &Topic, qos: QoS, payload: &Bytes) {
    if let Some(journal) = shared.stream_journal.as_ref() {
        journal.try_record(topic, qos, payload);
    }
}

/// Capture one publish into active trace sessions (F1-04, T-74).
///
/// Fast path (no tracing, the default): one relaxed atomic load on the
/// global flag and a return. No lock, no allocation, no clock read.
/// Slow path (sessions exist): resolve the publisher's last-known peer
/// address (one session lookup plus at most one small clone, off the
/// delivery locks) and delegate to `TraceStore::capture_publish`, which
/// takes one short read lock, scans at most `MAX_TRACES` sessions with no
/// allocation on no-match, and formats one small bounded line per matching
/// session (see `MAX_TRACE_PAYLOAD_BYTES`). Appends reuse the store's
/// per-session byte cap. Publishes without a known publisher id (detached
/// timers, gateway ingress without a session) capture nothing.
/// TODO(parity): should gateway and timer publishes without a publisher id
/// capture into topic-filter sessions with an empty client id? Neither this
/// rulebook nor the task spec decides; current choice is the conservative
/// one (no capture) so a trace never invents a client identity.
fn maybe_capture_trace(shared: &Shared, publisher: Option<&str>, topic: &Topic, payload: &Bytes) {
    if !shared.tracing.is_enabled() {
        return;
    }
    let Some(client_id) = publisher else {
        return;
    };
    let peerhost = shared
        .sessions
        .get(client_id)
        .and_then(|session| session.peerhost.read().clone());
    shared
        .traces
        .capture_publish(client_id, peerhost.as_deref(), topic.as_str(), payload);
}

/// Bound for the slow-record handoff channel (F1-03, T-73). At most this
/// many pending slow records wait for the background recorder; past it the
/// newest record drops (counted in `slow_dropped`). Each record carries one
/// topic truncated to the recorder's 512-char cap plus two integers (under
/// one kilobyte per record), so the bound holds under one megabyte.
/// Rationale: 1024 matches the stream journal bound so slow bursts share
/// one memory story with the existing bounded channels, and absorbs a
/// flood burst while the single recorder drains.
const SLOW_RECORD_CHANNEL_BOUND: usize = 1024;

/// Quiet gap that starts a new shedding burst, in milliseconds (F1-03).
/// A shed arriving more than this after the previous shed starts a fresh
/// burst (`burst_start = now`); sheds inside the gap extend the same burst
/// so `timespan = now - burst_start` measures sustained backpressure.
/// Rationale: 10 s is 20x the default 500 ms threshold, so sheds from one
/// sustained episode (which crosses the threshold mid-burst) stay in one
/// burst while episodes separated by much longer than the threshold start
/// fresh; far below the 300 s default expiry so a burst never spans expiry.
const SLOW_BURST_GAP_MS: u64 = 10_000;

/// Recorder topic cap mirrored from `SlowSubsStore` (F1-03): the handoff
/// truncates to this many chars so the bounded channel's memory story
/// holds (see [`SLOW_RECORD_CHANNEL_BOUND`]).
const SLOW_TOPIC_CAP_CHARS: usize = 512;

/// Truncate to at most `max` chars on a char boundary (F1-03). Used on the
/// shed (slow) path only, never on the fast path.
fn truncate_to_chars(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max.min(s.len());
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// One slow delivery handed from the egress hook to the background
/// recorder (F1-03). `conn_id` resolves to the subscriber client id off
/// the hot path; `topic` is the concrete publish topic; `timespan_ms` is
/// the measured shedding-burst duration (wall-clock milliseconds the
/// egress has been shedding for this burst), compared against the
/// configured threshold before sending.
#[derive(Debug)]
struct SlowRecord {
    conn_id: u64,
    topic: String,
    timespan_ms: u64,
}

/// Wall-clock milliseconds for slow-burst tracking (F1-03). Zero on clock
/// failure (fail closed: the burst restarts, recording is delayed rather
/// than invented).
fn slow_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

/// Note slow backpressure from one PublishIn fan-out and hand a slow
/// record off asynchronously when the measured delivery latency exceeds
/// the threshold (F1-03, T-73; FX-03 extends it to QoS 0 backlogs).
///
/// `shed_before`/`shed_after` are the `egress_qos0_shed` counter around
/// the route loop; `backlog_depth` is the kernel QoS 0 queue length for
/// `conn_id` just after this fan-out enqueued (`ConnTable::qos0_len`,
/// one short shard lock plus one short queue lock, no allocation);
/// `topic_str` is the concrete publish topic, `conn_id` the first
/// enqueued delivery's connection (the slow subscriber when a single
/// subscriber floods) and `delivery_ms` the route-loop time just
/// measured around the fan-out (the delivery time already spent routing
/// this publish).
///
/// A shed message is not a slow delivery: it never reached a mailbox
/// and stays counted as dropped inside `ConnTable::route` (via
/// `egress_qos0_shed`); the slow record names the backlogged
/// subscriber-topic whose sustained backlog age (`burst_age_ms`, time
/// the kernel backlog has persisted, which includes time deliveries
/// spent queued) exceeds the threshold. `backlog_depth <= 1` means no
/// queue ahead (just this message, or a race that already drained), so
/// there is no queued time to measure and nothing is recorded.
/// `backlog_depth > 1` without shed still holds queued deliveries whose
/// whole-path time (ingress to delivery complete, approximated by the
/// sustained-backlog age plus this fan-out's route time) is slow.
///
/// Fast path (no backlog: no shed and depth <= 1): two relaxed atomic
/// loads happened in the caller to produce the counter pair, plus one
/// integer comparison and a return here. No lock, no allocation, no
/// clock read.
///
/// Slow path (backlog): one relaxed enable load, two relaxed atomic loads
/// plus at most two relaxed stores for burst tracking, one wall-clock
/// read, one relaxed threshold load plus one relaxed stats-type load,
/// and at most one small topic clone plus a non-blocking `try_send`.
/// The store lock is never taken here; the background recorder takes one
/// short store lock per drained record.
/// TODO(parity): only the PublishIn fan-out calls this yet; the QoS 2
/// second phase, last-will, delayed-driver, CoAP and cluster fan-outs
/// route through their own loops without a slow note. Should every
/// egress loop share one `route_deliveries` helper so all of them record?
/// TODO(parity): the kernel queue is the only backlog observed here;
/// edge-side queueing past its dispatch bound sheds before the kernel
/// can time it. Should an edge backpressure signal feed the same burst
/// so edge-only stalls record with the same whole-path meaning?
#[allow(clippy::too_many_arguments)]
fn maybe_note_slow_delivery(
    shared: &Shared,
    topic_str: &str,
    conn_id: u64,
    shed_before: u64,
    shed_after: u64,
    backlog_depth: usize,
    delivery_ms: u64,
) {
    let shed_happened = shed_after > shed_before;
    if !shed_happened && backlog_depth <= 1 {
        return;
    }
    // Honour `enable=false`: recording is gated on the
    // flag the API exposes, read atomically with no lock.
    if !shared.slow_subs_settings.is_enabled() {
        return;
    }
    let now = slow_now_ms();
    let last = shared.slow_last_shed_ms.load(Ordering::Relaxed);
    let burst_start = if now.saturating_sub(last) > SLOW_BURST_GAP_MS || last == 0 {
        shared.slow_burst_start_ms.store(now, Ordering::Relaxed);
        now
    } else {
        shared.slow_burst_start_ms.load(Ordering::Relaxed)
    };
    shared.slow_last_shed_ms.store(now, Ordering::Relaxed);
    let burst_age_ms = now.saturating_sub(burst_start);
    // Delivery latency (`timespan`) selected by the configured
    // `stats_type`, read atomically: `whole` is ingress-to-complete
    // approximated by the sustained-backpressure age (which includes
    // time deliveries spent queued behind the backlog) plus this
    // fan-out's route time, `internal` is the sustained-backpressure
    // age (time the backlog has persisted), and `response` is this
    // fan-out's route time. A shed burst ceils sub-millisecond ages to
    // 1 ms so the first shed of a burst (which proves the backlog is
    // full) records under the 1 ms judge threshold; a non-full backlog
    // (depth > 1, nothing shed) requires the measured age itself to
    // reach the threshold so a single transient queueing never invents
    // a slow subscriber.
    let stats_code = shared.slow_subs_settings.stats_type_code();
    let timespan_ms = if shed_happened {
        match stats_code {
            broker_api::v5::slow_subscriptions::SLOW_STATS_RESPONSE => delivery_ms.max(1),
            broker_api::v5::slow_subscriptions::SLOW_STATS_INTERNAL => burst_age_ms.max(1),
            _ => burst_age_ms.max(delivery_ms).max(1),
        }
    } else {
        match stats_code {
            broker_api::v5::slow_subscriptions::SLOW_STATS_RESPONSE => delivery_ms,
            broker_api::v5::slow_subscriptions::SLOW_STATS_INTERNAL => burst_age_ms,
            _ => burst_age_ms.max(delivery_ms),
        }
    };
    // Honour the configured threshold (default 500 ms) read atomically:
    // latencies shorter than it are still shedding but not yet slow.
    let threshold_ms = shared.slow_subs_settings.threshold_ms();
    if timespan_ms < threshold_ms {
        return;
    }
    // Truncate to the recorder's topic cap (mirrors `SlowSubsStore`'s 512
    // chars) so a 64KB MQTT topic never rides the bounded handoff: the
    // stored row keeps the same prefix the store would keep.
    let topic = truncate_to_chars(topic_str, SLOW_TOPIC_CAP_CHARS).to_string();
    let record = SlowRecord {
        conn_id,
        topic,
        timespan_ms,
    };
    if shared.slow_tx.try_send(record).is_err() {
        shared.slow_dropped.fetch_add(1, Ordering::Relaxed);
    }
}

/// Decode the concrete publish topic out of a `PublishIn` frame for the
/// slow hook (F1-03). Mirrors [`decode_publish_meta`] without allocating
/// on failure: returns `None` for malformed frames and for empty-topic
/// alias-by-reference uses (the alias table lives behind a session lock;
/// the hook skips those rather than inventing a topic).
/// Called only on the backlog (slow) path, so the returned clone never costs
/// the fast path an allocation.
fn decode_publish_topic_for_slow(frame: &BrokerFrame) -> Option<String> {
    let meta = &frame.metadata;
    if meta.len() < 2 + 2 + 3 {
        return None;
    }
    let topic_len = u16::from_be_bytes([meta[0], meta[1]]) as usize;
    if topic_len == 0 {
        return None;
    }
    if meta.len() < 2 + topic_len + 2 + 3 {
        return None;
    }
    std::str::from_utf8(&meta[2..2 + topic_len])
        .ok()
        .map(str::to_string)
}

/// Fire `$events/delivery_dropped` when a downlink frame misses its
/// mailbox. The connection is dead or unknown.
/// Keep cost at zero when no rule selects the source.
/// Check the flag before any lock.
fn fire_delivery_dropped_on_route_fail(
    engine: &Arc<broker_rules::RuleEngine>,
    sessions: &SessionManager,
    conn_id: u64,
    topic: String,
    qos: u8,
    reason: &str,
) {
    use broker_rules::EventSource;
    if !engine.is_event_source_selected(EventSource::DeliveryDropped) {
        return;
    }
    // Owner session first so a shared client id resolves in its own
    // tenant. Rules belong to the default tenant, so another tenant
    // fires nothing.
    let Some(session) = sessions.session_for_conn(conn_id).or_else(|| {
        sessions
            .client_id_for_conn(conn_id)
            .as_deref()
            .and_then(|id| sessions.get(id))
    }) else {
        return;
    };
    if !session.tenant_is_default.load(Ordering::Relaxed) {
        return;
    }
    let username = session.username.read().clone();
    let peerhost = session.peerhost.read().clone();
    engine.fire_delivery_dropped(broker_rules::DeliveryDroppedEvent {
        client_id: session.client_id.clone(),
        username,
        peerhost,
        topic,
        qos,
        reason: reason.to_string(),
    });
}

/// Route one PublishIn fan-out and note slow backpressure (F1-03, T-73;
/// FX-03 extends the note to QoS 0 backlogs that have not shed yet).
///
/// Shared by the live PublishIn path and the slow test so both exercise
/// the same egress hook. When slow tracking is disabled the fast path
/// pays one relaxed atomic load plus the existing route loop and
/// counters: no shed snapshot, no clock, no lock, no allocation.
/// When enabled, the helper snapshots the QoS 0 shed counter (two relaxed
/// atomic loads, no lock) and times the loop itself (the delivery time
/// already spent routing this publish), then hands at most one slow
/// record off asynchronously when this fan-out shed or left a backlog
/// behind it. QoS 1/2 fan-outs skip the backlog read (their queue is the
/// guaranteed mailbox, never the bounded QoS 0 backlog); QoS 0 reads one
/// bounded backlog length (`qos0_len`: one short shard lock plus one
/// short queue lock, no allocation) so the whole-path time includes time
/// spent queued. Shed messages stay counted as dropped inside
/// `ConnTable::route` and are never recorded as slow themselves.
fn route_publish_out_deliveries(
    shared: &Shared,
    publish_in: &BrokerFrame,
    deliveries: Vec<(u64, BrokerFrame)>,
) {
    // F1-03: disabled fast path routes exactly as before
    // the hook existed, plus one relaxed atomic load for the enable flag.
    if !shared.slow_subs_settings.is_enabled() {
        let mut enqueued = 0u64;
        let mut enqueued_bytes = 0u64;
        // One aggregate event load per fan-out; no clone when no rule.
        let want_drop_event = shared.engine.has_any_event_selected();
        for (conn_id, routed) in deliveries {
            let len = routed.total_frame_len() as u64;
            let meta = if want_drop_event {
                Some(routed.metadata.clone())
            } else {
                None
            };
            if shared.conns.route(conn_id, routed) {
                enqueued += 1;
                enqueued_bytes += len;
            } else if let Some(meta) = meta
                .as_ref()
                .and_then(|m| decode_publish_meta(m).map(|(t, _, q, _, _, _, _)| (t, q)))
            {
                // Fire $events/delivery_dropped for dead/unknown conn.
                fire_delivery_dropped_on_route_fail(
                    &shared.engine,
                    &shared.sessions,
                    conn_id,
                    meta.0,
                    meta.1,
                    "dead_mailbox",
                );
            }
        }
        shared.metrics.inc_messages_forwarded_by(enqueued);
        shared.metrics.inc_publish_sent_by(enqueued);
        shared.metrics.inc_delivered_by(enqueued);
        shared.metrics.inc_bytes_sent_by(enqueued_bytes);
        return;
    }
    // FX-03 enabled path: snapshot the QoS 0 shed counter around the
    // route loop (two relaxed atomic loads, no lock) and time the loop
    // itself (the delivery time already spent) so the hook compares a
    // measured latency. The slow note below runs when this fan-out shed
    // (backlog full, oldest drop counted) or left more than one QoS 0
    // frame queued behind it (backlog ahead, whose age includes queued
    // time); a lone queued frame with no queue ahead records nothing.
    // QoS 1/2 fan-outs pay no backlog read; QoS 0 pays one bounded
    // `qos0_len` read. The slow path decodes the topic (one small clone)
    // and `try_send`s at most one record.
    let shed_before = shared.metrics.egress_qos0_shed();
    let route_start = std::time::Instant::now();
    let mut enqueued = 0u64;
    let mut enqueued_bytes = 0u64;
    let mut first_conn_id = 0u64;
    let mut have_first = false;
    let mut first_is_qos0 = false;
    // One aggregate event load per fan-out; no clone when no rule.
    let want_drop_event = shared.engine.has_any_event_selected();
    for (conn_id, routed) in deliveries {
        if !have_first {
            first_conn_id = conn_id;
            first_is_qos0 = is_qos0_publish_out(&routed);
            have_first = true;
        }
        let len = routed.total_frame_len() as u64;
        let meta = if want_drop_event {
            Some(routed.metadata.clone())
        } else {
            None
        };
        if shared.conns.route(conn_id, routed) {
            enqueued += 1;
            enqueued_bytes += len;
        } else if let Some((decoded_topic, decoded_qos)) = meta
            .as_ref()
            .and_then(|m| decode_publish_meta(m).map(|(t, _, q, _, _, _, _)| (t, q)))
        {
            // Fire $events/delivery_dropped for dead/unknown conn.
            fire_delivery_dropped_on_route_fail(
                &shared.engine,
                &shared.sessions,
                conn_id,
                decoded_topic,
                decoded_qos,
                "dead_mailbox",
            );
        }
    }
    let delivery_ms = route_start.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
    shared.metrics.inc_messages_forwarded_by(enqueued);
    shared.metrics.inc_publish_sent_by(enqueued);
    shared.metrics.inc_delivered_by(enqueued);
    shared.metrics.inc_bytes_sent_by(enqueued_bytes);
    // FX-03: hand a slow record off asynchronously on shed or backlog.
    // Fast path (no shed, depth <= 1) is integer comparisons; the slow
    // path decodes the topic (one small clone) and `try_send`s at most
    // one record.
    if have_first {
        let shed_after = shared.metrics.egress_qos0_shed();
        // QoS 1/2 never ride the bounded QoS 0 backlog: shed alone
        // decides, with no backlog read and no new lock.
        let backlog_depth = if first_is_qos0 {
            shared.conns.qos0_len(first_conn_id)
        } else {
            0
        };
        if shed_after > shed_before || backlog_depth > 1 {
            if let Some(topic_str) = decode_publish_topic_for_slow(publish_in) {
                maybe_note_slow_delivery(
                    shared,
                    &topic_str,
                    first_conn_id,
                    shed_before,
                    shed_after,
                    backlog_depth,
                    delivery_ms,
                );
            }
        }
    }
}

#[derive(Clone)]
struct Shared {
    sessions: Arc<SessionManager>,
    router: Arc<Router>,
    conns: Arc<ConnTable>,
    engine: Arc<RuleEngine>,
    sink: Arc<dyn BrokerSink>,
    retained: Arc<dyn RetainedStore>,
    /// Retained writer shared by the ingress pipeline and the rule sink.
    retain_writer: RetainWriter,
    /// Coordinates `PUBLISH(retain=true)` (`write`) with `SUBSCRIBE` retained
    /// snapshot + atomic route commit (`read`) so no retained update falls
    /// between snapshot and route installation without staging uncommitted routes.
    retained_subscribe_gate: Arc<tokio::sync::RwLock<()>>,
    /// Retained-delivery settings for W1-28: shared with the management
    /// API so kernel ingress and management publishes enforce the same
    /// configurable limits. One Arc clone at boot; reads take a short
    /// lock on retained publishes only, never on the fan-out path.
    retainer_config: Arc<broker_api::v5::retainer::RetainerConfigStore>,
    /// Auto-subscribe list for W1-39: shared with the management API so
    /// validated writes are visible to the connect hook without a
    /// restart. One Arc clone; the hook clones the capped list once per
    /// bind, never per message.
    auto_subscribe: Arc<broker_api::v5::auto_subscribe::AutoSubscribeStore>,
    /// Ban directory for B1-03: shared with the management API so
    /// validated ban writes are visible to the connect and publish
    /// checks without a restart. One Arc clone; connects take one read
    /// lock off the delivery path, publishes take one read lock that
    /// returns after a length check when the store is empty.
    bans: Arc<broker_api::v5::banned::BanStore>,
    /// Licence request/install/status store for B2-04: cluster identity,
    /// stored token and trusted signing set, shared with the management
    /// API so validated installs are visible without a restart. One Arc
    /// clone; the publish path never touches it.
    licence: Arc<broker_api::licence::LicenceStore>,
    /// Alarm directory shared with the management API so CONNECT refusals
    /// (ban path below) are visible via the API.
    /// One Arc clone; CONNECT-only, never on the per-message path.
    alarms: Arc<broker_api::v5::alarms::AlarmStore>,
    /// Monitor history ring shared with the management API (F1-02, T-72)
    /// so the sampler below and the `GET /monitor` reads observe the same
    /// bounded buffer. One Arc clone; the sampler takes one short write
    /// lock per tick, reads take one short read lock, and the publish
    /// path never touches it.
    monitor: Arc<broker_api::v5::monitor::MonitorHistory>,
    /// Cluster routing plane. `None` runs standalone; `Some` announces
    /// local subscriptions and forwards each publish once per matching
    /// remote node (single forward per node; remotes fan out locally).
    cluster: Option<Arc<dyn RoutingPlane>>,
    metrics: Arc<Metrics>,
    /// Gauge-plus-high-water-mark store for the broker `/stats` endpoint (W1 reads it;
    /// lifecycle points below keep it exact).
    stats: Arc<StatsStore>,
    /// Node readiness flag for `GET /api/v5/status` (W1-27): single atomic
    /// bool shared with the management API. Set at boot; the handler does
    /// one constant-time load per request. Management-plane only; never
    /// touched on the per-message path.
    readiness: Arc<NodeReadiness>,
    auth: Arc<MemoryAuth>,
    /// Ordered authenticator chain for W2-02: shared with the management
    /// API so validated chain writes are visible to the CONNECT consult
    /// without a restart. One `Arc` clone at boot; CONNECT loads one
    /// lock-free snapshot per connect and never takes the chain write
    /// lock; publish and deliver never touch it.
    authn_chain: Arc<AuthnChain>,
    /// Node authentication cache for W2-03: bounded per-username record
    /// of successful credentialed CONNECTs, shared with the management
    /// API so status reads and resets observe the same entries without
    /// a restart. One `Arc` clone at boot; CONNECT records one entry
    /// per success under a short lock; publish and deliver never touch
    /// it. Entries are memory-only; the enabled flag and cap persist
    /// via the config registry.
    authn_node_cache: Arc<NodeAuthCache>,
    /// Global authentication settings for W2-05: shared with the
    /// management API so validated settings writes are visible to the
    /// CONNECT consult without a restart. One `Arc` clone at boot;
    /// CONNECT loads one lock-free snapshot per connect and never takes
    /// the settings write lock; publish and deliver never touch it.
    authn_settings: Arc<AuthnSettingsStore>,
    /// Node authorization cache policy for W2-14: shared with the
    /// management API so validated policy writes are visible to reads
    /// without a restart. One `Arc` clone at boot; the kernel boot path
    /// seeds it from the registry and management reads take one
    /// lock-free snapshot per request; publish and deliver never touch
    /// it. Cached verdicts stay per-session and memory-only.
    authz_node_cache: Arc<AuthzNodeCache>,
    /// Global authorization settings for W2-16: shared with the
    /// management API so validated settings writes are visible to reads
    /// without a restart. One `Arc` clone at boot; the kernel boot path
    /// seeds it from the registry and management reads take one bounded
    /// snapshot per request. The publish and subscribe authorization
    /// events read the stored `no_match` and `deny_action` values through
    /// lock-free bool consults with no snapshot clone.
    authz_settings: Arc<AuthzSettingsStore>,
    /// Ordered authorization source registry for W2-17: shared with the
    /// management API so validated registry writes are visible to reads
    /// without a restart. One `Arc` clone at boot; the kernel boot path
    /// seeds it from the registry and management reads take one bounded
    /// snapshot per request; publish and deliver never touch its write
    /// lock. The built-in rule table it fronts (`MemoryAuth`) is
    /// consulted by the publish and subscribe authorization events.
    authz_sources: Arc<AuthzSourceRegistry>,
    /// Management API key directory for W2-25: shared with the
    /// management API so validated mints and revocations are visible to
    /// reads without a restart. One `Arc` clone at boot; the kernel boot
    /// path seeds it from the registry and management reads take one
    /// bounded snapshot per request. Key checks use constant-time
    /// comparison off the hot path; publish and deliver never touch it.
    /// Bearer/Basic enforcement itself is owned elsewhere; this store
    /// only manages the key records.
    api_keys: Arc<broker_api::v5::api_keys::ApiKeyStore>,
    /// The licence settings and the session history of the management API.
    license: Arc<broker_api::v5::license::LicenseState>,
    /// Management audit ring for W2-28: shared with the management API
    /// so middleware records and CONNECT outcomes plus `GET /audit`
    /// reads observe the same bounded buffer. One `Arc` clone at boot;
    /// writes are non-blocking `try_write` from the management middleware
    /// and the CONNECT bind path. Publish and deliver never touch it.
    /// Memory-only; a restart starts from an empty ring (persist-or-trim: trim).
    audit: Arc<broker_api::v5::audit::AuditLog>,
    /// Configured single-sign-on backend registry for W2-32: shared with
    /// the management API so registry writes are visible to `GET /sso`
    /// reads without a restart. One `Arc` clone at boot; the kernel boot
    /// path seeds it from the registry and management reads take one
    /// bounded snapshot per request. Publish and deliver never touch it.
    sso_backends: Arc<broker_api::v5::sso::SsoStore>,
    /// Source instance directory for W3-26 (`GET/POST /sources`,
    /// `DELETE /sources/{id}`) and W3-31 (`GET /sources_summary`):
    /// shared with the management API so validated creates and deletes
    /// are visible to list and summary reads without a restart. One
    /// `Arc` clone at boot; the kernel boot path seeds it from the
    /// registry and management reads take one bounded snapshot per
    /// request. Management-plane only; publish and deliver never touch it.
    sources: Arc<broker_api::v5::sources::SourceStore>,
    /// Action instance directory for W3-02 (`GET/POST /actions`,
    /// `DELETE /actions/:id`): shared with the management API so
    /// validated creates and deletes are visible to list reads without
    /// a restart. One `Arc` clone at boot; the list takes one bounded
    /// snapshot clone per request. Management-plane only; rule
    /// evaluation reads a snapshot, never this store directly, and
    /// publish and deliver never touch it.
    actions: Arc<broker_api::v5::actions::ActionStore>,
    /// Connector instance directory for `GET/POST /connectors` and the
    /// per-connector routes: shared with the management API so validated
    /// creates and deletes are visible to list reads without a restart.
    /// One `Arc` clone at boot; the kernel boot path seeds it from the
    /// registry and management reads take one bounded snapshot per
    /// request. Management-plane only; rule evaluation reads the
    /// connector manager directly, and publish and deliver never touch it.
    /// The create, enable and operation events mirror the flag into the
    /// engine's lock-free dispatch snapshot.
    connectors: Arc<broker_api::v5::rules::ConnectorStore>,
    /// Rule-engine settings for W3-10 (`GET`/`PUT /rule_engine`): shared
    /// with the management API so validated writes are visible to reads
    /// without a restart and hot-apply to the engine's fire-path snapshot
    /// through the subscriber. One `Arc` clone at boot; the fire path
    /// loads the engine snapshot per ingress and never takes a management
    /// lock. Management-plane only; publish and deliver never touch this
    /// store.
    rule_engine_config: Arc<broker_rules::EngineConfigStore>,
    /// Ordered message-transformation chain for W3-32 through W3-34
    /// (`GET`/`POST`/`PUT /message_transformations` plus dry-run and
    /// reorder): shared with the
    /// management API so validated writes are visible to reads without a
    /// restart. One `Arc` clone at boot; the ingress pipeline loads the
    /// engine snapshot per message and never takes this store lock.
    transformations: Arc<broker_api::v5::transformations::TransformChainStore>,
    /// Schema registry for W3-37 (`GET`/`POST /schema_registry`,
    /// `DELETE /schema_registry/:name`): shared with the management API
    /// so checked writes show in reads without a restart. One `Arc` clone
    /// at boot. Reads take one short lock per request. No publish path
    /// reads this store. Publish and deliver take no lock here.
    schema_registry: Arc<broker_api::v5::schema_registry::SchemaRegistryStore>,
    /// Managed tenant namespaces for TM-02 (`GET/POST/PUT/DELETE
    /// /api/v5/mt/...`): shared with the management API so validated
    /// creates and deletes are visible to reads without a restart. One
    /// `Arc` clone at boot. Management-plane only; publish and deliver
    /// never touch it.
    namespaces: Arc<broker_api::v5::mt::NamespaceStore>,
    /// Directory authenticator for B2-01 (`None` disables LDAP). Consulted
    /// at CONNECT when the local store refuses the credentials; a
    /// directory outage fails closed. One `Arc` clone at boot; connects
    /// take a semaphore permit off the delivery path.
    ldap: Option<Arc<LdapAuthenticator>>,
    /// Kerberos authenticator for B2-02 (`None` disables Kerberos).
    /// Consulted at CONNECT for SPNEGO/AP-REQ tokens when the local store
    /// refuses; a missing keytab disables explicitly with a log line and
    /// never accepts tokens. One `Arc` clone at boot; CONNECT-only (one
    /// bounded replay-cache lock, never on the delivery path).
    kerberos: Option<Arc<KerberosAuthenticator>>,
    /// JWKS authenticator for B5-02 (`None` disables JWT). Consulted at
    /// CONNECT for JWT-shaped passwords when the local store refuses; an
    /// unreachable or invalid endpoint fails closed with 0x86. One `Arc`
    /// clone at boot; CONNECT-only (one bounded cache read plus one
    /// signature verification on a hit, one singleflight fetch on a key
    /// miss, never on the delivery path).
    jwks: Option<Arc<JwksAuthenticator>>,
    /// Database authentication set for B5-03 (`None` disables every
    /// database source). Consulted at CONNECT when the local store (plus
    /// LDAP/Kerberos) refuses, and on the publish/subscribe path for
    /// users with no local record. One `Arc` clone at boot; CONNECT and
    /// cache-miss lookups take a semaphore permit, cache hits take one
    /// short cache lock.
    db_auth: Option<Arc<DbAuthSet>>,
    /// Webhook verdict authenticator/authorizer for B5-04 (`None`
    /// disables the webhook). Consulted at CONNECT when the local store
    /// (and LDAP/Kerberos above) refuses the credentials, and on every
    /// publish authorization; an unreachable, slow or disagreeing
    /// endpoint fails closed. One `Arc` clone at boot; CONNECT takes a
    /// bounded pool permit off the delivery path, publishes take one
    /// short cache lock on hit (one bounded round-trip on miss).
    webhook: Option<Arc<WebhookAuth>>,
    allow_anonymous: bool,
    /// Tenant-assignment registry (MT-01): per-listener rules rendering
    /// the tenant id from the connect context. Consulted once per bind
    /// (`apply_bind`); the publish and deliver paths never touch it
    /// (they read the stamped session tenant instead). Ships empty so a
    /// default install lands every client in the default tenant.
    tenant_registry: Arc<broker_session::tenant::TenantRegistry>,
    /// Kernel-owned configuration registry: loaded from `--data-dir` at
    /// boot; validated defaults in tests and before boot wiring runs.
    config: Arc<ConfigRegistry>,
    /// Local node name (`--node-id`), passed into the API layer for the
    /// node-scope helper. Stored only until W1 wires it in.
    node_id: String,
    /// CoAP gateway translator and bounded observer table (B1-04).
    /// Always present so the publish path needs no `Option` branch;
    /// empty when `--coap-bind` is unset, in which case the gateway
    /// task never runs and the publish hook returns after one length
    /// check. One `Arc` clone at boot; registrations take a short
    /// write lock off the delivery path.
    coap: Arc<CoapGatewayHandler>,
    /// UDP socket the gateway listener bound (B1-04). `None` unless
    /// `--coap-bind` names an address: set once at boot before any
    /// task clones `Shared`, read-only afterwards. The publish hook
    /// snapshots observers and spawns the UDP sends off the hot path;
    /// unit tests leave it `None` so the hook is a no-op.
    coap_socket: Option<Arc<tokio::net::UdpSocket>>,
    /// Durable stream journal (B1-07). `None` (the default) disables
    /// journaling with a single branch on the publish path. `Some` journals
    /// every publish through a bounded channel drained off the hot path.
    /// One `Arc` clone at boot; the hook never blocks delivery.
    stream_journal: Option<Arc<StreamJournalHandle>>,
    /// Delayed-publish scheduler (B4-03): one shared hierarchical timer
    /// wheel plus the file-backed pending log behind it. The publish
    /// event records here before the publisher is acknowledged; a single
    /// background driver ticks the wheel and delivers due entries through
    /// the normal ingress pipeline. One `Arc` clone at boot; the publish
    /// path pays one atomic load, one bounded file append and one short
    /// wheel-lock insert, while cascading and file rewrites stay in the
    /// driver task. Measured before/after rates print in
    /// `delayed_delivery_workload_timings`. Bounded by the pending cap
    /// and the per-message
    /// payload cap documented in `broker-delayed`.
    delayed: Arc<DelayedScheduler>,
    /// Slow-subscription recorder for F1-03 (T-73): bounded ranked table
    /// shared with the management API so the egress hook and `GET/DELETE
    /// /slow_subscriptions` observe the same records. One Arc clone; the
    /// publish fast path never touches the store (records hand off through
    /// `slow_tx` below), the background recorder takes one short store
    /// lock per drained record.
    slow_subs: Arc<broker_api::v5::slow_subscriptions::SlowSubsStore>,
    /// Slow-subscription thresholds for F1-03 (T-73): single validated
    /// struct shared with the management API so validated PUTs are visible
    /// to the egress hook without a restart. One Arc clone; the publish
    /// fast path reads only the lock-free atomics (`threshold_ms`,
    /// `enabled`, `top_k_num`, `expire_interval_ms`, `stats_type_code`),
    /// never the lock.
    slow_subs_settings: Arc<broker_api::v5::slow_subscriptions::SlowSubsSettingsStore>,
    /// Trace-session registry for F1-04 (T-74): bounded capture sessions
    /// shared with the management API so validated `POST /trace` writes are
    /// visible to the publish hook without a restart. One Arc clone; the
    /// publish fast path pays one atomic flag load (see `tracing` below)
    /// plus one short read lock with a bounded scan when sessions exist,
    /// and one small bounded line clone per matching session. Appends reuse
    /// the store's per-session byte cap, so capture memory stays bounded.
    traces: Arc<broker_api::v5::trace::TraceStore>,
    /// Packet-tracing flag for F1-04 (T-74): single atomic bool shared with
    /// the management API. The session lifecycle raises it while a session
    /// exists and lowers it when none remains; the publish hook reads it
    /// first (one relaxed atomic load, no lock, no allocation when
    /// disabled). Disabled by default: no sessions means no capture.
    tracing: Arc<broker_api::v5::tracing::TracingFlagStore>,
    /// Running message collects for `POST /mcp` tool
    /// `subscribe_collect` (MC-04): bounded filter-keyed slots shared
    /// with the management API so kernel ingress feeds the same slots
    /// the tool drains. One Arc clone; the publish fast path pays one
    /// relaxed atomic load when no collect runs.
    collectors: Arc<broker_api::mcp::CollectRegistry>,
    /// Async handoff from the egress hook to the slow recorder (F1-03).
    /// Bounded at [`SLOW_RECORD_CHANNEL_BOUND`] pending records (each a
    /// topic truncated to 512 chars plus two integers, well under one
    /// megabyte); the
    /// hook uses non-blocking `try_send` and drops (counted in
    /// `slow_dropped`) when full, so a flood can never stall delivery or
    /// balloon memory. Rationale: 1024 absorbs a burst while the single
    /// recorder drains; past it the oldest pressure is already recorded.
    slow_tx: tokio::sync::mpsc::Sender<SlowRecord>,
    /// Receiving end of the slow handoff, taken once by
    /// [`Shared::spawn_slow_recorder`]. Behind a mutex solely so `Shared`
    /// stays Clone; touched only at boot/test setup, never on the
    /// per-message path.
    slow_rx: Arc<tokio::sync::Mutex<Option<tokio::sync::mpsc::Receiver<SlowRecord>>>>,
    /// Start of the current shedding burst in wall-clock milliseconds
    /// (F1-03). Set when a shed arrives after a quiet gap of at least
    /// [`SLOW_BURST_GAP_MS`]; combined with this fan-out's measured route
    /// time it yields the `timespan` selected by `stats_type` (sustained
    /// backpressure age for `whole`/`internal`, route time for
    /// `response`). One relaxed atomic store on the shed (slow) path
    /// only; the fast path (no shed) never touches it.
    slow_burst_start_ms: Arc<AtomicU64>,
    /// Wall-clock milliseconds of the last observed shed (F1-03). One
    /// relaxed atomic store on the shed path only; used to detect the
    /// quiet gap that starts a new burst.
    slow_last_shed_ms: Arc<AtomicU64>,
    /// Slow records dropped because the handoff channel was full (F1-03).
    /// One relaxed atomic increment on the drop path only; observed via
    /// [`Shared::slow_dropped_count`], never on the per-message path.
    slow_dropped: Arc<AtomicU64>,
    /// Recorder claim bit (F1-03): the first `spawn_slow_recorder` caller
    /// wins so connection tasks can call it freely without spawning a
    /// recorder per connection.
    slow_claimed: Arc<std::sync::atomic::AtomicBool>,
    /// Resolved listeners for `GET /listeners` (SY-02): the same
    /// `listeners.*` section the kernel binds from, captured at boot.
    /// `None` until boot resolves the layered config; `serve_api` shares
    /// it into `ApiState` so the endpoint reports the running
    /// configuration instead of defaults. One short read lock per API
    /// boot; never touched on the per-message path.
    api_listeners: Arc<std::sync::RwLock<Option<Arc<broker_config::schema::ListenersConf>>>>,
    /// Live listener directory for the v5 listener routes (AP-02):
    /// stored configuration plus the running flag and the socket
    /// control of the kernel-owned rows. Seeded from the boot
    /// configuration and shared with the management API (`serve_api`
    /// below), so validated management writes reach the bound sockets
    /// without a restart. One short lock per request; publish and
    /// deliver never touch this store.
    listener_store: Arc<broker_api::v5::listeners::ListenerStore>,
    /// Resolved startup layers for `explain` (SY-02): the winning layer
    /// per setting. `serve_api` shares it into `ApiState` so explain
    /// reports the file, variable or flag that set each value.
    api_startup_config: Arc<std::sync::RwLock<Option<Arc<LayeredConfig>>>>,
    /// Per-tenant limit defaults and overrides (TM-06). Defaults come
    /// from the file configuration. Overrides come from the namespace
    /// config. The connect path reads the effective triple. The publish
    /// path never touches this store directly; retained caps live in
    /// the retained store under its own lock.
    tenant_limits: Arc<broker_session::tenant_limits::TenantLimitStore>,
    /// Whether the TLS listener verifies a client certificate (TM-08).
    /// Set once at boot from the resolved config. Read on connect only.
    /// Off by default keeps handshakes unchanged.
    tls_verify_client_cert: bool,
    /// Whether the secure WebSocket listener verifies a client
    /// certificate (TM-08). Mirrors the TLS listener. Off by default.
    wss_verify_client_cert: bool,
    /// True when `listeners.tls.verify` is `verify_peer` (K1-05).
    /// Set once at boot from the resolved config. Read on connect only.
    tls_verify_peer: bool,
    /// True when the TLS listener refuses a handshake without a
    /// certificate (K1-05). Set once at boot. Read on connect only.
    tls_fail_if_no_peer_cert: bool,
    /// Source for the bind username from the client certificate
    /// on the TLS listener (K1-05). Set once at boot.
    /// Read on connect only.
    tls_peer_cert_as_username: String,
    /// Source for the bind client identifier from the client
    /// certificate on the TLS listener (K1-05). Set once at boot.
    /// Read on connect only.
    tls_peer_cert_as_clientid: String,
    /// True when `listeners.wss.verify` is `verify_peer` (K1-05).
    /// Set once at boot from the resolved config. Read on connect only.
    wss_verify_peer: bool,
    /// True when the secure WebSocket listener refuses a handshake
    /// without a certificate (K1-05). Set once at boot.
    /// Read on connect only.
    wss_fail_if_no_peer_cert: bool,
    /// Source for the bind username from the client certificate
    /// on the secure WebSocket listener (K1-05). Set once at boot.
    /// Read on connect only.
    wss_peer_cert_as_username: String,
    /// Source for the bind client identifier from the client
    /// certificate on the secure WebSocket listener (K1-05). Set once at boot.
    /// Read on connect only.
    wss_peer_cert_as_clientid: String,
    /// Pre-shared-key identity store for the kernel TLS listener (K1-04).
    /// Shared with the PSK handshake callbacks. One `Arc` clone at boot;
    /// handshakes take one short read lock per lookup. The keys file fills
    /// it at boot. Empty by default, so every handshake fails closed.
    psk: Arc<broker_auth::PskStore>,
    /// Built-in MCP endpoint settings (MC-01): shared with the
    /// management API so `POST /mcp` reads the `[mcp]` boot values
    /// without a restart. One `Arc` clone at boot; the handler takes
    /// two atomic loads per request. Management-plane only; publish
    /// and deliver never touch it.
    mcp: Arc<broker_api::mcp::McpStore>,
}

impl Shared {
    fn new() -> Self {
        let sessions = Arc::new(SessionManager::new());
        let router = Arc::new(Router::new());
        let conns = Arc::new(ConnTable::default());
        // Boot default: 1024 is the ingress queue capacity. The window task
        // depth defaults to 65,536 inside the engine. Boot applies the file
        // `rules_engine.window_channel_depth` and `backpressure_policy` next.
        let engine = Arc::new(RuleEngine::new(1024, BackpressurePolicy::DropOldest));
        // Enterprise window split (X1-06): windowed rules execute in the
        // enterprise crate; attach its executor before any rule is
        // created so eager worker spawn covers the preseeds below and
        // the registry seed after boot.
        broker_rules_enterprise::attach(&engine);
        let metrics = Arc::new(Metrics::new());
        // Rule spill accounting (B4-07): mirror the ingress queue's
        // spill, replay, drop and recovery counters into the node
        // metrics so disk-backpressure behaviour is visible on the
        // existing observability path. Memory-only inputs count only
        // refusals, so the attach is a no-op otherwise.
        engine.set_metrics(&metrics);
        // PERF-10: attach drop accounting to the delivery table so
        // `ConnTable::route` counts unknown-conn and dead-mailbox drops
        // exactly where it discards them.
        conns.set_metrics(&metrics);
        let stats = Arc::new(StatsStore::new());
        {
            let counted_sessions = sessions.clone();
            stats.set_subscription_counter(Box::new(move |stats| {
                count_subscription_stats(&counted_sessions, stats);
            }));
        }
        let readiness = Arc::new(NodeReadiness::new());
        // NO MQTT LOOPBACK: rule republishes route straight into the local
        // router/mailboxes through this in-memory sink. The same sink
        // serves window-flush republish actions (INDRA-213).
        let retained: Arc<dyn RetainedStore> = Arc::new(MemoryStore::new());
        let retainer_config = Arc::new(broker_api::v5::retainer::RetainerConfigStore::new());
        let tenant_limits = Arc::new(broker_session::tenant_limits::TenantLimitStore::default());
        let retain_writer = RetainWriter {
            retained: retained.clone(),
            config: retainer_config.clone(),
            stats: stats.clone(),
            metrics: metrics.clone(),
            tenant_limits: tenant_limits.clone(),
        };
        let retained_subscribe_gate = Arc::new(tokio::sync::RwLock::new(()));
        let sink: Arc<dyn BrokerSink> = Arc::new(InMemoryBrokerSink {
            router: router.clone(),
            sessions: sessions.clone(),
            conns: conns.clone(),
            metrics: metrics.clone(),
            engine: engine.clone(),
            retain_writer: retain_writer.clone(),
            retained_subscribe_gate: retained_subscribe_gate.clone(),
        });
        engine.set_broker_sink(sink.clone());

        // Pre-seed end-to-end streaming pipelines connecting Ingress Source -> Rule SQL -> Connector Sink
        let _ = engine.create_rule(
            "factory-telemetry-to-kafka".to_string(),
            TopicFilter::new("sensors/+/telemetry").unwrap(),
            Some("SELECT payload.temperature as temp, payload.pressure as pressure, clientid FROM \"sensors/+/telemetry\" WHERE payload.temperature > 25".to_string()),
            true,
            vec![broker_rules::RuleAction::ForwardConnector {
                connector_id: "kafka:kafka-prod".to_string(),
            }],
        );
        let _ = engine.create_rule(
            "critical-alerts-to-webhook".to_string(),
            TopicFilter::new("sensors/+/alerts").unwrap(),
            Some("SELECT payload.level as alert_level, payload.msg as message, clientid FROM \"sensors/+/alerts\" WHERE payload.level = 'CRITICAL'".to_string()),
            true,
            vec![broker_rules::RuleAction::ForwardConnector {
                connector_id: "http:webhook-alerts".to_string(),
            }],
        );
        let _ = engine.create_rule(
            "telemetry-to-postgres".to_string(),
            TopicFilter::new("sensors/+/telemetry").unwrap(),
            Some(
                "SELECT payload.temperature as temp, clientid FROM \"sensors/+/telemetry\""
                    .to_string(),
            ),
            true,
            vec![broker_rules::RuleAction::ForwardConnector {
                connector_id: "pgsql:postgres-analytics".to_string(),
            }],
        );
        let (slow_tx, slow_rx) =
            tokio::sync::mpsc::channel::<SlowRecord>(SLOW_RECORD_CHANNEL_BOUND);
        // W2-05: settings store shares its subscriber with the very node
        // cache below, so validated settings replaces keep the cache's
        // enabled flag and cap in sync without a restart.
        let authn_node_cache = Arc::new(NodeAuthCache::new());
        let authn_settings = Arc::new(AuthnSettingsStore::new());
        {
            let cache = Arc::clone(&authn_node_cache);
            authn_settings.set_subscriber(Arc::new(
                move |next: &broker_config::AuthnSettingsConf| {
                    cache.apply_settings(next.node_cache.enable, next.node_cache.max_count);
                },
            ));
        }
        // W3-10: engine-config store shares its subscriber with the very
        // engine above, so validated engine writes hot-apply to the
        // fire-path snapshot without a restart.
        let rule_engine_config = Arc::new(broker_rules::EngineConfigStore::new());
        engine.apply_engine_config(rule_engine_config.get());
        {
            let engine_handle = Arc::clone(&engine);
            rule_engine_config.set_subscriber(Arc::new(
                move |next: &broker_config::RuleEngineConf| {
                    engine_handle.apply_engine_config(next.clone());
                },
            ));
        }
        let shared = Self {
            sessions,
            router,
            conns,
            engine,
            sink,
            retained,
            retain_writer,
            retained_subscribe_gate,
            retainer_config,
            auto_subscribe: Arc::new(broker_api::v5::auto_subscribe::AutoSubscribeStore::new()),
            bans: Arc::new(broker_api::v5::banned::BanStore::new()),
            licence: Arc::new(broker_api::licence::LicenceStore::new()),
            alarms: Arc::new(broker_api::v5::alarms::AlarmStore::new()),
            monitor: Arc::new(broker_api::v5::monitor::MonitorHistory::new()),
            cluster: None,
            metrics,
            stats,
            readiness,
            auth: Arc::new(MemoryAuth::new()),
            authn_chain: Arc::new(AuthnChain::new()),
            authn_node_cache,
            authn_settings,
            authz_node_cache: Arc::new(AuthzNodeCache::new()),
            authz_settings: Arc::new(AuthzSettingsStore::new()),
            authz_sources: Arc::new(AuthzSourceRegistry::new()),
            api_keys: Arc::new(broker_api::v5::api_keys::ApiKeyStore::new()),
            license: Arc::new(broker_api::v5::license::LicenseState::new()),
            audit: Arc::new(broker_api::v5::audit::AuditLog::new()),
            sso_backends: Arc::new(broker_api::v5::sso::SsoStore::new()),
            sources: Arc::new(broker_api::v5::sources::SourceStore::new()),
            actions: Arc::new(broker_api::v5::actions::ActionStore::new()),
            connectors: Arc::new(broker_api::v5::rules::ConnectorStore::new()),
            rule_engine_config,
            transformations: Arc::new(broker_api::v5::transformations::TransformChainStore::new()),
            schema_registry: Arc::new(broker_api::v5::schema_registry::SchemaRegistryStore::new()),
            namespaces: Arc::new(broker_api::v5::mt::NamespaceStore::new()),
            ldap: None,
            kerberos: None,
            jwks: None,
            db_auth: None,
            webhook: None,
            allow_anonymous: false,
            tenant_registry: Arc::new(broker_session::tenant::TenantRegistry::new()),
            config: defaults_registry(),
            node_id: "indra-node-1".to_string(),
            coap: Arc::new(CoapGatewayHandler::new()),
            coap_socket: None,
            stream_journal: None,
            delayed: Arc::new(DelayedScheduler::new()),
            slow_subs: Arc::new(broker_api::v5::slow_subscriptions::SlowSubsStore::new()),
            slow_subs_settings: Arc::new(
                broker_api::v5::slow_subscriptions::SlowSubsSettingsStore::new(),
            ),
            traces: Arc::new(broker_api::v5::trace::TraceStore::new()),
            tracing: Arc::new(broker_api::v5::tracing::TracingFlagStore::new()),
            collectors: Arc::new(broker_api::mcp::CollectRegistry::new()),
            slow_tx,
            slow_rx: Arc::new(tokio::sync::Mutex::new(Some(slow_rx))),
            slow_burst_start_ms: Arc::new(AtomicU64::new(0)),
            slow_last_shed_ms: Arc::new(AtomicU64::new(0)),
            slow_dropped: Arc::new(AtomicU64::new(0)),
            slow_claimed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            api_listeners: Arc::new(std::sync::RwLock::new(None)),
            listener_store: Arc::new(broker_api::v5::listeners::ListenerStore::new()),
            api_startup_config: Arc::new(std::sync::RwLock::new(None)),
            tenant_limits,
            tls_verify_client_cert: false,
            wss_verify_client_cert: false,
            tls_verify_peer: false,
            tls_fail_if_no_peer_cert: false,
            tls_peer_cert_as_username: "disabled".to_string(),
            tls_peer_cert_as_clientid: "disabled".to_string(),
            wss_verify_peer: false,
            wss_fail_if_no_peer_cert: false,
            wss_peer_cert_as_username: "disabled".to_string(),
            wss_peer_cert_as_clientid: "disabled".to_string(),
            psk: Arc::new(broker_auth::PskStore::new()),
            mcp: Arc::new(broker_api::mcp::McpStore::new()),
        };
        // Mirror the boot path: the rule table consults the same settings
        // instance the management plane serves, so validated writes are
        // visible to the authorize path without a restart. One `Arc`
        // clone; defaults keep today's behaviour.
        shared
            .auth
            .set_authz_settings(Arc::clone(&shared.authz_settings));
        shared
    }

    /// Start the single delayed-delivery driver task for this node. The
    /// first caller wins (the scheduler's claim bit); later callers are
    /// no-ops so connection tasks can call this freely without spawning
    /// a driver per connection. The driver ticks every 100 ms off the
    /// delivery path and routes due entries through the normal ingress
    /// pipeline.
    fn spawn_delayed_driver(&self) {
        if !self.delayed.claim_driver() {
            return;
        }
        let shared = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(DELAYED_TICK_MS)).await;
                let due = shared.delayed.tick_due();
                if due.is_empty() {
                    continue;
                }
                let mut fired_ids = Vec::with_capacity(due.len());
                for entry in due {
                    fired_ids.push(entry.id);
                    deliver_delayed_entry(&shared, entry).await;
                }
                shared.delayed.remove_ids(&fired_ids);
            }
        });
    }

    /// Start the single slow-delivery recorder task for this node (F1-03,
    /// T-73). The first caller wins; later callers are no-ops so boot and
    /// tests can call it freely without spawning a recorder per
    /// connection. The task owns the handoff receiver and resolves each
    /// `conn_id` to its subscriber client id off the hot path, then takes
    /// one short store lock per record. Unknown connections are skipped
    /// (no invented client id); the publish path never waits on this task.
    /// Each drained record honours the configured `expire_interval` (drops
    /// rows older than the window) and `top_k_num` (keeps only the slowest
    /// rows), both read atomically with no work on the per-message path.
    fn spawn_slow_recorder(&self) {
        if self
            .slow_claimed
            .swap(true, std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }
        let shared = self.clone();
        tokio::spawn(async move {
            let mut rx = shared.slow_rx.lock().await.take();
            let mut rx = match rx.take() {
                Some(rx) => rx,
                None => return,
            };
            while let Some(record) = rx.recv().await {
                let client_id = shared.sessions.client_id_for_conn(record.conn_id);
                let Some(client_id) = client_id else {
                    continue;
                };
                shared.slow_subs.record(
                    &client_id,
                    &record.topic,
                    record.timespan_ms,
                    slow_now_ms(),
                );
                // Honour `expire_interval` and `top_k_num` off the hot path:
                // expiry drops rows older than the window, truncation keeps
                // only the slowest `top_k_num` rows (bounded by the hard cap).
                let now_ms = slow_now_ms();
                let expire_ms = shared.slow_subs_settings.expire_interval_ms();
                shared.slow_subs.remove_expired(now_ms, expire_ms);
                let max_records = shared.slow_subs_settings.max_records();
                shared.slow_subs.truncate_to(max_records);
            }
            // Observe the handoff-drop counter off the hot path so it is
            // never write-only; logs only when bursts overflowed the bound.
            let dropped = shared.slow_dropped_count();
            if dropped > 0 {
                warn!("Slow-record handoff dropped {dropped} records: channel full");
            }
        });
    }

    /// Slow records dropped because the handoff channel was full (F1-03).
    /// One relaxed atomic load; never on the per-message path. Observed by
    /// the recorder shutdown log and the slow test, never write-only.
    pub fn slow_dropped_count(&self) -> u64 {
        self.slow_dropped.load(Ordering::Relaxed)
    }
}

/// Rule spill pressure-ease driver (B4-07): the prod `next_event` caller
/// beside tests. Pends on the spill-backed input queue and acknowledges
/// durability copies in order after bursts ease (each pop counts a replay
/// via the input). Copies were already executed inline by
/// `dispatch_ingress`, so the driver drops them; crash survivors on disk
/// are likewise already-executed (pushed after the match) and safe to
/// ack. Runs off the publish path; memory-only engines never spawn it.
fn spawn_rule_spill_driver(engine: Arc<RuleEngine>) {
    tokio::spawn(async move {
        loop {
            // `next_event` holds the channel lock only across the wait,
            // never across disk I/O, so this task never stalls publish.
            let next = engine.input().next_event().await;
            if next.is_none() {
                // Channel closed: input is gone, driver exits.
                break;
            }
            // Durability ack: drop, replay already counted inside.
        }
    });
}

/// Deliver one due delayed entry through the same ingress pipeline an
/// immediate publish takes (retained store, rules, fan-out, cluster
/// forward), then count it. Runs in the delayed driver task, never on
/// the live publish or deliver path.
/// TODO(parity): the entry was checked at schedule time. Rules can change
/// before it fires. Does the fire need a second check?
async fn deliver_delayed_entry(shared: &Shared, entry: DelayedEntry) {
    let Ok(topic) = Topic::new(entry.topic.clone()) else {
        shared.delayed.note_malformed();
        warn!("Dropping delayed entry with bad inner topic");
        return;
    };
    let Ok(qos) = QoS::try_from(entry.qos) else {
        shared.delayed.note_malformed();
        warn!("Dropping delayed entry with bad QoS");
        return;
    };
    let payload = Bytes::from(entry.payload.clone());
    // The publisher's tenant rides into the deferred pipeline (MT-02):
    // fan-out after the timer stays inside the same tenant it was
    // scheduled in.
    let deliveries = ingress_pipeline_with_publisher(
        shared,
        &topic,
        qos,
        entry.retain,
        &payload,
        entry.publisher.as_deref(),
        &entry.publisher_tenant,
    )
    .await;
    let mut enqueued = 0u64;
    let mut enqueued_bytes = 0u64;
    for (conn_id, routed) in deliveries {
        let len = routed.total_frame_len() as u64;
        if shared.conns.route(conn_id, routed) {
            enqueued += 1;
            enqueued_bytes += len;
        }
    }
    shared.metrics.inc_messages_forwarded_by(enqueued);
    shared.metrics.inc_publish_sent_by(enqueued);
    shared.metrics.inc_delivered_by(enqueued);
    shared.metrics.inc_bytes_sent_by(enqueued_bytes);
    forward_cluster(shared, &topic, qos, &payload, &entry.publisher_tenant).await;
    shared.delayed.note_delivered();
}

/// Defaults-only registry for fresh `Shared` state (tests and pre-boot).
///
/// Loads from a unique scratch path that is never created, so `load`
/// yields validated empty roots with no filesystem writes and never
/// observes the kernel data dir.
fn defaults_registry() -> Arc<ConfigRegistry> {
    static SCRATCH_COUNTER: AtomicU64 = AtomicU64::new(0);
    let slot = SCRATCH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir().join(format!(
        "indramqtt-kernel-{nanos}-{}-{slot}",
        std::process::id()
    ));
    Arc::new(ConfigRegistry::load(&dir).expect("validated defaults always load"))
}

/// Load the kernel config registry from `data_dir`.
///
/// Creates the directory when missing, then loads `<dir>/state.toml`
/// (a missing file yields validated defaults). A corrupt file or an
/// invalid snapshot is a fatal error naming the file and the field;
/// defaults are never silently substituted.
/// Log the licence lifecycle state at boot (B2-04).
///
/// Names the state, the days remaining and what stopped where relevant, so
/// an operator never needs the status route to answer what they are
/// licensed for. MQTT serving never depends on this state: trial, grace
/// and lapsed alike keep connect, publish and subscribe working, and only
/// enterprise entitlements turn off when lapsed.
fn log_licence_lifecycle(state: &InstallationState, warn_days: u64) {
    match state {
        InstallationState::Trial { days_remaining } => {
            info!(
                "Licence state: trial ({} days remaining, every entitlement working)",
                days_remaining
            );
            if *days_remaining <= warn_days {
                warn!(
                    "Trial ends in {} days; generate a licence request via GET /api/v1/licence/request or `indramqtt --licence-request-out <file>`",
                    days_remaining
                );
            }
        }
        InstallationState::Valid {
            customer,
            expires_at,
            max_nodes,
            days_remaining,
            ..
        } => {
            info!(
                "Licence state: valid for '{}' (max {} nodes, expires at {}, {} days remaining)",
                customer, max_nodes, expires_at, days_remaining
            );
            if *days_remaining <= warn_days {
                warn!(
                    "Licence for '{}' expires in {} days (at {}); renew before grace runs out",
                    customer, days_remaining, expires_at
                );
            }
        }
        InstallationState::Grace {
            customer,
            expires_at,
            days_remaining,
            ..
        } => {
            warn!(
                "Licence state: grace for '{}' (expired at {}, {} days remaining); every entitlement still working",
                customer, expires_at, days_remaining
            );
        }
        InstallationState::Lapsed { reason } => {
            warn!(
                "Licence state: lapsed ({reason}); enterprise entitlements off, MQTT still serving"
            );
        }
    }
}

fn load_data_dir_registry(data_dir: &str) -> Result<ConfigRegistry, ConfigError> {
    std::fs::create_dir_all(data_dir).map_err(|err| ConfigError::Load {
        file: data_dir.to_string(),
        reason: format!("cannot create data directory: {err}"),
    })?;
    ConfigRegistry::load_for_boot(std::path::Path::new(data_dir))
}

/// Copy exporter settings set through the startup layers into the
/// config registry (AP-09).
///
/// Only the scalar file homes move (`enable_basic_auth`,
/// `latency_buckets`, the push target scalars and the collector
/// switches). Push headers and the namespaced limiter have no file
/// home and stay managed through `PUT /prometheus`. A key moves only
/// when a startup layer set it, so runtime changes keep every other
/// key; an unchanged registry is left alone with no save. Boot-time
/// only: one snapshot clone plus at most one commit and one save.
fn apply_prometheus_file_settings(
    layered: &LayeredConfig,
    registry: &ConfigRegistry,
) -> Result<(), ConfigError> {
    const PATHS: [&str; 13] = [
        "prometheus.enable_basic_auth",
        "prometheus.latency_buckets",
        "prometheus.push_gateway.enable",
        "prometheus.push_gateway.url",
        "prometheus.push_gateway.interval",
        "prometheus.push_gateway.job_name",
        "prometheus.push_gateway.method",
        "prometheus.collectors.mnesia",
        "prometheus.collectors.vm_dist",
        "prometheus.collectors.vm_memory",
        "prometheus.collectors.vm_msacc",
        "prometheus.collectors.vm_statistics",
        "prometheus.collectors.vm_system_info",
    ];
    let provenance = layered.provenance();
    if !PATHS.iter().any(|path| provenance.contains_key(*path)) {
        return Ok(());
    }
    let file = &layered.config().prometheus;
    let mut next = registry.snapshot().prometheus.clone();
    if provenance.contains_key(PATHS[0]) {
        next.enable_basic_auth = file.enable_basic_auth;
    }
    if provenance.contains_key(PATHS[1]) {
        next.latency_buckets = file.latency_buckets.clone();
    }
    if provenance.contains_key(PATHS[2]) {
        next.push_gateway.enable = file.push_gateway.enable;
    }
    if provenance.contains_key(PATHS[3]) {
        next.push_gateway.url = file.push_gateway.url.clone();
    }
    if provenance.contains_key(PATHS[4]) {
        next.push_gateway.interval = file.push_gateway.interval.clone();
    }
    if provenance.contains_key(PATHS[5]) {
        next.push_gateway.job_name = file.push_gateway.job_name.clone();
    }
    if provenance.contains_key(PATHS[6]) {
        next.push_gateway.method = file.push_gateway.method.clone();
    }
    if provenance.contains_key(PATHS[7]) {
        next.collectors.mnesia = file.collectors.mnesia;
    }
    if provenance.contains_key(PATHS[8]) {
        next.collectors.vm_dist = file.collectors.vm_dist;
    }
    if provenance.contains_key(PATHS[9]) {
        next.collectors.vm_memory = file.collectors.vm_memory;
    }
    if provenance.contains_key(PATHS[10]) {
        next.collectors.vm_msacc = file.collectors.vm_msacc;
    }
    if provenance.contains_key(PATHS[11]) {
        next.collectors.vm_statistics = file.collectors.vm_statistics;
    }
    if provenance.contains_key(PATHS[12]) {
        next.collectors.vm_system_info = file.collectors.vm_system_info;
    }
    if next == registry.snapshot().prometheus {
        return Ok(());
    }
    next.validate()?;
    registry.commit_prometheus(next)?;
    registry.save()?;
    Ok(())
}

/// Copy authorization sources set through the startup layers into the
/// config registry.
///
/// The file home replaces the stored source order only when a startup
/// layer sets `authz_sources.sources`; a later boot without the key keeps
/// the stored order. Boot-time only: one snapshot clone plus at most one
/// commit and one save.
fn apply_authz_sources_file_settings(
    layered: &LayeredConfig,
    registry: &ConfigRegistry,
) -> Result<(), ConfigError> {
    const PATH: &str = "authz_sources.sources";
    if !layered.provenance().contains_key(PATH) {
        return Ok(());
    }
    let next = layered.config().authz_sources.clone();
    if next == registry.snapshot().authz_sources {
        return Ok(());
    }
    next.validate()?;
    registry.commit_authz_sources(next)?;
    registry.save()?;
    Ok(())
}

/// Resolve the startup configuration through the typed-schema layers
/// (SY-02): schema defaults, `indra.toml`, `conf.d/*.toml`,
/// `INDRA_*` environment, then explicitly passed flags. Main's defaults
/// stay unchanged; a flag that is not passed leaves its setting to the
/// lower layers. `--config-dir` is the bootstrap locator with no schema
/// home. A missing directory or missing file starts on defaults; an
/// unreadable, unparsable or invalid layer fails boot loudly.
fn load_startup_config(
    args: &Args,
    matches: &clap::ArgMatches,
) -> Result<LayeredConfig, ConfigError> {
    broker_config::load_layered(
        std::path::Path::new(&args.config_dir),
        &cli_overrides(args, matches),
    )
}

/// Returns the flags that the operator typed. These flags are the flag
/// layer.
///
/// Most flags have a clap default. Thus the parsed value does not show
/// if the operator typed the flag. `matches` shows it. Only a value from
/// the command line becomes an override. If a flag is absent, the file,
/// the fragments and the environment set the value.
fn cli_overrides(args: &Args, matches: &clap::ArgMatches) -> CliOverrides {
    let typed = |id: &str| matches.value_source(id) == Some(clap::parser::ValueSource::CommandLine);
    let text = |id: &str, value: &str| typed(id).then(|| value.to_string());
    CliOverrides {
        node_id: text("node_id", &args.node_id),
        data_dir: text("data_dir", &args.data_dir),
        brokerlink_bind: text("brokerlink_bind", &args.brokerlink_bind),
        bind: text("bind", &args.bind),
        api_bind: text("api_bind", &args.api_bind),
        allow_anonymous: args.allow_anonymous,
        qos0_backlog: typed("qos0_backlog").then_some(args.qos0_backlog),
        license_key: args.license_key.clone(),
        license_keys: text("license_keys", &args.license_keys),
        licence_request_out: text("licence_request_out", &args.licence_request_out),
        licence_install_file: text("licence_install_file", &args.licence_install_file),
        licence_expiry_warn_days: typed("licence_expiry_warn_days")
            .then_some(args.licence_expiry_warn_days),
        cluster_seeds: args.cluster_seeds.clone(),
        cluster_bind: text("cluster_bind", &args.cluster_bind),
        coap_bind: text("coap_bind", &args.coap_bind),
        stream_dir: text("stream_dir", &args.stream_dir),
        ldap_url: text("ldap_url", &args.ldap_url),
        ldap_base_dn: text("ldap_base_dn", &args.ldap_base_dn),
        ldap_bind_dn: text("ldap_bind_dn", &args.ldap_bind_dn),
        ldap_bind_password: text("ldap_bind_password", &args.ldap_bind_password),
        ldap_user_filter: text("ldap_user_filter", &args.ldap_user_filter),
        ldap_group_attribute: text("ldap_group_attribute", &args.ldap_group_attribute),
        ldap_required_group: text("ldap_required_group", &args.ldap_required_group),
        ldap_ca_cert: text("ldap_ca_cert", &args.ldap_ca_cert),
        kerberos_keytab: text("kerberos_keytab", &args.kerberos_keytab),
        kerberos_service_principal: text(
            "kerberos_service_principal",
            &args.kerberos_service_principal,
        ),
        kerberos_realm: text("kerberos_realm", &args.kerberos_realm),
        kerberos_allowed_realms: text("kerberos_allowed_realms", &args.kerberos_allowed_realms),
        kerberos_clock_skew_secs: typed("kerberos_clock_skew_secs")
            .then_some(args.kerberos_clock_skew_secs),
        kerberos_role_map: text("kerberos_role_map", &args.kerberos_role_map),
        kerberos_replay_max: args.kerberos_replay_max,
        qos1_inflight_window: args.qos1_inflight_window,
        qos1_spill: args.qos1_spill,
        rule_spill_dir: args.rule_spill_dir.clone(),
        jwks_url: args.jwks_url.clone(),
        jwks_issuer: args.jwks_issuer.clone(),
        jwks_audience: args.jwks_audience.clone(),
        jwks_refresh_period_secs: typed("jwks_refresh_period_secs")
            .then_some(args.jwks_refresh_period_secs),
        jwks_fetch_timeout_ms: typed("jwks_fetch_timeout_ms").then_some(args.jwks_fetch_timeout_ms),
        jwks_refresh_timeout_ms: typed("jwks_refresh_timeout_ms")
            .then_some(args.jwks_refresh_timeout_ms),
        jwks_cache_max_keys: typed("jwks_cache_max_keys").then_some(args.jwks_cache_max_keys),
        jwks_cache_ttl_secs: typed("jwks_cache_ttl_secs").then_some(args.jwks_cache_ttl_secs),
        jwks_clock_skew_secs: typed("jwks_clock_skew_secs").then_some(args.jwks_clock_skew_secs),
        jwks_ca_cert: text("jwks_ca_cert", &args.jwks_ca_cert),
        dbauth_postgres_url: text("dbauth_postgres_url", &args.dbauth_postgres_url),
        dbauth_mysql_url: text("dbauth_mysql_url", &args.dbauth_mysql_url),
        dbauth_redis_url: text("dbauth_redis_url", &args.dbauth_redis_url),
        dbauth_mongodb_url: text("dbauth_mongodb_url", &args.dbauth_mongodb_url),
        dbauth_pool_size: typed("dbauth_pool_size").then_some(args.dbauth_pool_size),
        dbauth_connect_timeout_ms: typed("dbauth_connect_timeout_ms")
            .then_some(args.dbauth_connect_timeout_ms),
        dbauth_read_timeout_ms: typed("dbauth_read_timeout_ms")
            .then_some(args.dbauth_read_timeout_ms),
        dbauth_cache_size: typed("dbauth_cache_size").then_some(args.dbauth_cache_size),
        dbauth_cache_ttl_secs: typed("dbauth_cache_ttl_secs").then_some(args.dbauth_cache_ttl_secs),
        webhook_url: text("webhook_url", &args.webhook_url),
        webhook_pool_size: typed("webhook_pool_size").then_some(args.webhook_pool_size),
        webhook_timeout_ms: typed("webhook_timeout_ms").then_some(args.webhook_timeout_ms),
        webhook_breaker_threshold: typed("webhook_breaker_threshold")
            .then_some(args.webhook_breaker_threshold),
        webhook_breaker_reset_ms: typed("webhook_breaker_reset_ms")
            .then_some(args.webhook_breaker_reset_ms),
        webhook_cache_size: typed("webhook_cache_size").then_some(args.webhook_cache_size),
        webhook_cache_ttl_secs: typed("webhook_cache_ttl_secs")
            .then_some(args.webhook_cache_ttl_secs),
        delayed_max_secs: typed("delayed_max_secs").then_some(args.delayed_max_secs),
        topic_alias_maximum: args.topic_alias_maximum,
        max_packet_size: args.max_packet_size,
        monitor_sample_secs: typed("monitor_sample_secs").then_some(args.monitor_sample_secs),
        tls_verify_client_cert: args.tls_verify_client_cert,
        wss_verify_client_cert: args.wss_verify_client_cert,
    }
}

/// Copies the resolved configuration to the fields that the startup
/// reads.
///
/// A startup layer is `indra.toml`, a `conf.d` fragment, an `INDRA_*`
/// variable or a typed flag. If a layer set a setting, this function
/// copies the value to its field. Thus the value that `explain` shows is
/// the value in operation. If no layer set a setting, the field keeps
/// the default of the flag.
fn apply_resolved_config(args: &mut Args, layered: &LayeredConfig) -> Result<(), ConfigError> {
    let root = toml::Value::try_from(layered.config()).map_err(|err| {
        ConfigError::Invalid(format!("resolved configuration cannot be read back: {err}"))
    })?;
    let set_by_a_layer = |path: &str| layered.provenance().contains_key(path);
    let lookup = |path: &str| -> Option<&toml::Value> {
        path.split('.')
            .try_fold(&root, |node, segment| node.get(segment))
    };
    let wrong_type = |path: &str, want: &str| {
        ConfigError::Invalid(format!(
            "setting `{path}` is invalid: expected {want} (field `{path}`)"
        ))
    };
    let text = |path: &str, slot: &mut String| -> Result<(), ConfigError> {
        if !set_by_a_layer(path) {
            return Ok(());
        }
        match lookup(path) {
            Some(toml::Value::String(value)) => *slot = value.clone(),
            // The flag uses a list with commas. Convert the list to that form.
            Some(toml::Value::Array(entries)) => {
                *slot = entries
                    .iter()
                    .filter_map(|entry| entry.as_str())
                    .collect::<Vec<_>>()
                    .join(",");
            }
            Some(_) => return Err(wrong_type(path, "a string")),
            None => {}
        }
        Ok(())
    };
    let number = |path: &str| -> Result<Option<u64>, ConfigError> {
        if !set_by_a_layer(path) {
            return Ok(None);
        }
        match lookup(path) {
            Some(toml::Value::Integer(value)) => u64::try_from(*value)
                .map(Some)
                .map_err(|_| wrong_type(path, "a non-negative integer")),
            Some(_) => Err(wrong_type(path, "an integer")),
            None => Ok(None),
        }
    };

    text("node.id", &mut args.node_id)?;
    text("node.data_dir", &mut args.data_dir)?;
    text("node.brokerlink_bind", &mut args.brokerlink_bind)?;
    text("listeners.tcp.bind", &mut args.bind)?;
    text("listeners.api.bind", &mut args.api_bind)?;
    if set_by_a_layer("auth.allow_anonymous") {
        args.allow_anonymous = layered.config().auth.allow_anonymous;
    }
    if let Some(value) = number("session.max_qos0_backlog")? {
        args.qos0_backlog = value as usize;
    }
    let mut optional = String::new();
    text("licence.license_key", &mut optional)?;
    if !optional.is_empty() {
        args.license_key = Some(std::mem::take(&mut optional));
    }
    text("licence.trusted_keys_path", &mut args.license_keys)?;
    text("licence.request_out", &mut args.licence_request_out)?;
    text("licence.install_file", &mut args.licence_install_file)?;
    if let Some(value) = number("licence.expiry_warn_days")? {
        args.licence_expiry_warn_days = value;
    }
    text("cluster.seed_nodes", &mut optional)?;
    if !optional.is_empty() {
        args.cluster_seeds = Some(std::mem::take(&mut optional));
    }
    text("cluster.bind", &mut args.cluster_bind)?;
    text("gateway.coap_bind", &mut args.coap_bind)?;
    text("persistence.stream_dir", &mut args.stream_dir)?;
    text("ldap.server_url", &mut args.ldap_url)?;
    text("ldap.base_dn", &mut args.ldap_base_dn)?;
    text("ldap.bind_dn", &mut args.ldap_bind_dn)?;
    text("ldap.bind_password", &mut args.ldap_bind_password)?;
    text("ldap.user_filter", &mut args.ldap_user_filter)?;
    text("ldap.group_attribute", &mut args.ldap_group_attribute)?;
    text("ldap.required_group", &mut args.ldap_required_group)?;
    text("ldap.ca_cert_path", &mut args.ldap_ca_cert)?;
    text("kerberos.keytab_path", &mut args.kerberos_keytab)?;
    text(
        "kerberos.service_principal",
        &mut args.kerberos_service_principal,
    )?;
    text("kerberos.realm", &mut args.kerberos_realm)?;
    text("kerberos.allowed_realms", &mut args.kerberos_allowed_realms)?;
    if let Some(value) = number("kerberos.clock_skew_secs")? {
        args.kerberos_clock_skew_secs = value;
    }
    text("kerberos.role_map", &mut args.kerberos_role_map)?;
    if let Some(value) = number("jwks.refresh_period_secs")? {
        args.jwks_refresh_period_secs = value;
    }
    if let Some(value) = number("jwks.fetch_timeout_ms")? {
        args.jwks_fetch_timeout_ms = value;
    }
    if let Some(value) = number("jwks.refresh_timeout_ms")? {
        args.jwks_refresh_timeout_ms = value;
    }
    if let Some(value) = number("jwks.cache_max_keys")? {
        args.jwks_cache_max_keys = value as usize;
    }
    if let Some(value) = number("jwks.cache_ttl_secs")? {
        args.jwks_cache_ttl_secs = value;
    }
    if let Some(value) = number("jwks.clock_skew_secs")? {
        args.jwks_clock_skew_secs = value;
    }
    text("jwks.ca_cert", &mut args.jwks_ca_cert)?;
    text("dbauth.postgres_url", &mut args.dbauth_postgres_url)?;
    text("dbauth.mysql_url", &mut args.dbauth_mysql_url)?;
    text("dbauth.redis_url", &mut args.dbauth_redis_url)?;
    text("dbauth.mongodb_url", &mut args.dbauth_mongodb_url)?;
    if let Some(value) = number("dbauth.pool_size")? {
        args.dbauth_pool_size = value as usize;
    }
    if let Some(value) = number("dbauth.connect_timeout_ms")? {
        args.dbauth_connect_timeout_ms = value;
    }
    if let Some(value) = number("dbauth.read_timeout_ms")? {
        args.dbauth_read_timeout_ms = value;
    }
    if let Some(value) = number("dbauth.cache_size")? {
        args.dbauth_cache_size = value as usize;
    }
    if let Some(value) = number("dbauth.cache_ttl_secs")? {
        args.dbauth_cache_ttl_secs = value;
    }
    text("webhook.url", &mut args.webhook_url)?;
    if let Some(value) = number("webhook.pool_size")? {
        args.webhook_pool_size = value as usize;
    }
    if let Some(value) = number("webhook.timeout_ms")? {
        args.webhook_timeout_ms = value;
    }
    if let Some(value) = number("webhook.breaker_threshold")? {
        args.webhook_breaker_threshold = u32::try_from(value)
            .map_err(|_| wrong_type("webhook.breaker_threshold", "a 32-bit integer"))?;
    }
    if let Some(value) = number("webhook.breaker_reset_ms")? {
        args.webhook_breaker_reset_ms = value;
    }
    if let Some(value) = number("webhook.cache_size")? {
        args.webhook_cache_size = value as usize;
    }
    if let Some(value) = number("webhook.cache_ttl_secs")? {
        args.webhook_cache_ttl_secs = value;
    }
    if let Some(value) = number("delayed.max_secs")? {
        args.delayed_max_secs = value;
    }
    if let Some(value) = number("observability.monitor_sample_secs")? {
        args.monitor_sample_secs = value;
    }
    if set_by_a_layer("listeners.tls.verify_client_cert") {
        args.tls_verify_client_cert = Some(layered.config().listeners.tls.verify_client_cert);
    }
    if set_by_a_layer("listeners.wss.verify_client_cert") {
        args.wss_verify_client_cert = Some(layered.config().listeners.wss.verify_client_cert);
    }
    Ok(())
}

/// Validate the layered configuration for `--check-config`.
/// Load the layers and apply them. Return `ok` on success.
/// Return the error text on failure. `main` prints the
/// result and exits with 0 or 2.
fn check_config_outcome(args: &mut Args, matches: &clap::ArgMatches) -> Result<String, String> {
    let layered = load_startup_config(&*args, matches).map_err(|err| err.to_string())?;
    apply_resolved_config(args, &layered).map_err(|err| err.to_string())?;
    Ok("ok".to_string())
}

/// Install the configured tenant assignment rules (SY-02): `TenantsConf`
/// rows become the registry entries the CONNECT hook assigns from. Empty
/// ships default-only; a bad row fails boot loudly, never a silent
/// default-tenant fallback.
fn install_tenant_rules(
    shared: &Shared,
    cfg: &broker_config::schema::BrokerConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let rules: Vec<broker_session::tenant::TenantRule> = cfg
        .tenants
        .rules
        .iter()
        .map(|row| {
            broker_session::tenant::TenantRule::new(
                row.listener.clone(),
                row.attribute.clone(),
                row.expression.clone(),
            )
        })
        .collect();
    shared
        .tenant_registry
        .replace(rules)
        .map_err(|e| format!("invalid tenants.rules: {e}"))?;
    Ok(())
}

/// Apply the file defaults for per-tenant limits (TM-06).
///
/// Copies the three quota bounds into the limit store and the
/// retained store before any edge binds. The file and the
/// environment already resolved into `cfg`, so both entries apply
/// here. Management overrides apply later per namespace.
fn apply_tenant_limit_defaults(shared: &Shared, cfg: &broker_config::schema::BrokerConfig) {
    let defaults = broker_session::tenant_limits::TenantLimits {
        max_connections: cfg.quotas.max_connections_per_tenant,
        max_sessions: cfg.quotas.max_sessions_per_tenant,
        max_retained: cfg.quotas.max_retained_per_tenant,
    };
    shared.tenant_limits.set_defaults(defaults);
    shared
        .retained
        .set_default_retained_per_tenant_cap(defaults.max_retained as usize);
}

/// Writes retained messages: store, clear and the limits that guard them.
/// The ingress pipeline and the rule republish sink share it, so a
/// retained message obeys the same payload size, message count and tenant
/// bounds whichever way it arrives. One `Arc` clone per field; it holds
/// no lock of its own.
#[derive(Clone)]
struct RetainWriter {
    retained: Arc<dyn RetainedStore>,
    config: Arc<broker_api::v5::retainer::RetainerConfigStore>,
    stats: Arc<StatsStore>,
    metrics: Arc<Metrics>,
    tenant_limits: Arc<broker_session::tenant_limits::TenantLimitStore>,
}

impl RetainWriter {
    /// Recount retained topics into the stats store.
    async fn sync_stat(&self) {
        match self.retained.count_retained().await {
            Ok(count) => self.stats.set_retained(count as u64),
            Err(e) => warn!("Retained recount failed: {}", e),
        }
    }

    /// Store the message as retained state of `tenant`, or clear the
    /// topic when the payload is empty. Oversized payloads are delivered
    /// but not stored, and new topics past the configured count are
    /// dropped while replacements still land. The hard cap in
    /// `broker_storage::MAX_RETAINED_MESSAGES` always applies inside the
    /// store. Retained state never crosses tenants.
    async fn apply(&self, topic: &Topic, qos: QoS, payload: &Bytes, tenant: &str) {
        if payload.is_empty() {
            if let Err(e) = self.retained.clear_retained_in_tenant(topic, tenant).await {
                warn!("Retained clear failed for {}: {}", topic.as_str(), e);
            } else {
                self.sync_stat().await;
            }
        } else {
            let cfg = self.config.get();
            let max_bytes =
                broker_api::v5::retainer::parse_bytesize_to_bytes(&cfg.max_payload_size)
                    .unwrap_or(1_048_576);
            let mut drop_new = false;
            let mut tenant_limited = false;
            if (payload.len() as u64) > max_bytes {
                drop_new = true;
            } else {
                let cap = cfg.backend.max_retained_messages;
                if cap > 0 {
                    let already = self
                        .retained
                        .get_retained_in_tenant(topic, tenant)
                        .await
                        .ok()
                        .flatten()
                        .is_some();
                    if !already && self.stats.retained() >= cap {
                        drop_new = true;
                    }
                }
                // TM-06: per-tenant retained bound lives in the store.
                // The store checks the cap inside its write lock.
                // Caps sync off path at boot and on API writes.
                // This path takes no new lock. It reads one atomic
                // for the unreachable flag. It counts and logs each
                // refusal.
                if !drop_new && self.tenant_limits.is_unreachable() {
                    drop_new = true;
                    tenant_limited = true;
                    self.tenant_limits.count_store_denied();
                }
            }
            if tenant_limited {
                self.metrics.inc_tenant_retained_dropped();
                self.tenant_limits.count_retained_dropped();
                tracing::warn!(
                    tenant = %tenant,
                    topic = %topic.as_str(),
                    "tenant retained limit reached: dropping new retained topic"
                );
            }
            if drop_new {
                // Delivered but not stored, matching the management path.
            } else {
                match self
                    .retained
                    .set_retained_in_tenant_checked(topic.clone(), qos, payload.clone(), tenant)
                    .await
                {
                    Ok(broker_storage::RetainedWriteOutcome::Stored) => {
                        self.sync_stat().await;
                    }
                    Ok(broker_storage::RetainedWriteOutcome::DroppedPerTenantLimit) => {
                        self.metrics.inc_tenant_retained_dropped();
                        self.tenant_limits.count_retained_dropped();
                        tracing::warn!(
                            tenant = %tenant,
                            topic = %topic.as_str(),
                            "tenant retained limit reached: dropping new retained topic"
                        );
                    }
                    Ok(broker_storage::RetainedWriteOutcome::DroppedGlobalLimit) => {
                        // Delivered but not stored at the global cap.
                    }
                    Err(e) => {
                        warn!("Retained store failed for {}: {}", topic.as_str(), e);
                    }
                }
            }
        }
    }
}

/// [`BrokerSink`] that forwards rule output directly to local router
/// subscribers. Pure in-memory fan-out: no sockets, no MQTT loopback.
struct InMemoryBrokerSink {
    router: Arc<Router>,
    sessions: Arc<SessionManager>,
    conns: Arc<ConnTable>,
    metrics: Arc<Metrics>,
    engine: Arc<RuleEngine>,
    retain_writer: RetainWriter,
    retained_subscribe_gate: Arc<tokio::sync::RwLock<()>>,
}

#[async_trait]
impl BrokerSink for InMemoryBrokerSink {
    async fn publish(
        &self,
        topic: Topic,
        payload: Bytes,
        qos: QoS,
        retain: bool,
    ) -> Result<(), broker_rules::RuleEngineError> {
        self.publish_in_tenant(
            broker_session::tenant::DEFAULT_TENANT_ID,
            topic,
            payload,
            qos,
            retain,
        )
        .await
    }
    /// Fan out one derived message inside one tenant. The match reads
    /// only that tenant, so output never leaves it.
    async fn publish_in_tenant(
        &self,
        tenant: &str,
        topic: Topic,
        payload: Bytes,
        qos: QoS,
        retain: bool,
    ) -> Result<(), broker_rules::RuleEngineError> {
        self.fan_out(
            tenant,
            topic,
            payload,
            qos,
            retain,
            &PublishV5Props::default(),
        )
        .await;
        Ok(())
    }

    /// Publish one message that a `republish` function rendered. The
    /// MQTT v5 properties ride to version-5 subscribers.
    async fn publish_message_in_tenant(
        &self,
        tenant: &str,
        message: broker_rules::RepublishMessage,
    ) -> Result<(), broker_rules::RuleEngineError> {
        let props = PublishV5Props {
            format: message.properties.payload_format,
            expiry: message.properties.message_expiry,
            users: message
                .properties
                .user_properties
                .into_iter()
                .map(|(key, value)| (key.into_bytes(), value.into_bytes()))
                .collect(),
        };
        self.fan_out(
            tenant,
            message.topic,
            message.payload,
            message.qos,
            message.retain,
            &props,
        )
        .await;
        Ok(())
    }
}

impl InMemoryBrokerSink {
    /// Store the message as retained state when it carries the retain
    /// flag, then fan it out to the subscribers of `tenant`.
    async fn fan_out(
        &self,
        tenant: &str,
        topic: Topic,
        payload: Bytes,
        qos: QoS,
        retain: bool,
        props: &PublishV5Props,
    ) {
        let _retained_sub_gate = if retain {
            Some(self.retained_subscribe_gate.write().await)
        } else {
            None
        };
        if retain {
            self.retain_writer
                .apply(&topic, qos, &payload, tenant)
                .await;
        }
        // Rule output must not fire delivery events. It routes here,
        // not through ingress. A delivery event here requeues and
        // retriggers the same rule. The drain loop then emits 64
        // copies for one delivery.
        let deliveries = build_downlink_frames_with_publisher_and_props(
            &self.router,
            &self.sessions,
            &self.metrics,
            &topic,
            qos,
            retain,
            &payload,
            None,
            tenant,
            props,
            None,
        );
        // Only frames that reached a live mailbox count as
        // forwarded/sent/delivered. Drops are already counted inside
        // `ConnTable::route` (unknown-conn vs. dead-mailbox); counting
        // them here as well would double-count the same loss.
        let mut enqueued = 0u64;
        let mut enqueued_bytes = 0u64;
        for (conn_id, frame) in deliveries {
            let len = frame.total_frame_len() as u64;
            if self.conns.route(conn_id, frame) {
                enqueued += 1;
                enqueued_bytes += len;
            } else {
                // Fire $events/delivery_dropped (RQ-04) for dead/unknown conn.
                fire_delivery_dropped_on_route_fail(
                    &self.engine,
                    &self.sessions,
                    conn_id,
                    topic.as_str().to_string(),
                    u8::from(qos),
                    "dead_mailbox",
                );
            }
        }
        self.metrics.inc_messages_forwarded_by(enqueued);
        self.metrics.inc_publish_sent_by(enqueued);
        self.metrics.inc_delivered_by(enqueued);
        self.metrics.inc_bytes_sent_by(enqueued_bytes);
    }
}

/// Merge guaranteed (`rx`) and QoS 0 batches into one socket write
/// while preserving per-destination order. Frames carry independent
/// per-connection sequence numbers, so grouping by `conn_id` and
/// sorting each group by `sequence_no` restores publish order for a
/// subscriber that receives mixed qualities; cross-connection order is
/// irrelevant (different subscribers). Groups emit in `conn_id` order
/// for determinism.
fn merge_egress_batch(
    rx_batch: Vec<BrokerFrame>,
    qos0_batch: Vec<BrokerFrame>,
) -> Vec<BrokerFrame> {
    if qos0_batch.is_empty() {
        return rx_batch;
    }
    if rx_batch.is_empty() {
        return qos0_batch;
    }
    use std::collections::HashMap;
    let mut by_conn: HashMap<u64, Vec<BrokerFrame>> = HashMap::new();
    for frame in rx_batch.into_iter().chain(qos0_batch) {
        by_conn.entry(frame.header.conn_id).or_default().push(frame);
    }
    let mut conn_ids: Vec<u64> = by_conn.keys().copied().collect();
    conn_ids.sort_unstable();
    let mut out = Vec::new();
    for conn_id in conn_ids {
        let mut frames = by_conn.remove(&conn_id).unwrap_or_default();
        frames.sort_by_key(|frame| frame.header.sequence_no);
        out.extend(frames);
    }
    out
}

/// Detach a dead transport: forget its mailbox and detach every
/// session bound on it. Clean sessions drop their subscriptions (session
/// mirror and router copies, plus their unacked QoS 1 inflight state);
/// durable sessions keep subscriptions + offline queue + inflight for
/// reconnect replay. Per-transport work only: each entry touches its own
/// session plus atomics, so deaths on other transports never block this
/// one. Router sweeps run once per detached clean session, off the hot
/// path; fan-out takes no new lock.
fn detach(bound: &[(SessionKey, u64)], shared: &Shared, tx: &UnboundedSender<BrokerFrame>) {
    shared.conns.prune_sender(tx);
    if bound.is_empty() {
        return;
    }
    for (key, conn_id) in bound {
        let swept =
            shared
                .sessions
                .unbind_connection_in_tenant(&key.tenant, &key.client_id, *conn_id);
        for filter in &swept {
            unsubscribe_scoped_in_tenant(shared, &key.tenant, &key.client_id, filter);
        }
        let active = shared.metrics.dec_connections();
        shared.stats.set_connections(active.max(0) as u64);
    }
    refresh_subscription_stats(shared);
}

/// Push subscription/topic gauges into the stats store using the same
/// counting as the management API `gather_stats` (active sessions only,
/// distinct filters), so `/stats` maxima agree exactly with live reads.
/// Lifecycle points only (bind, detach, unbind, subscribe): the
/// per-message publish path performs no session/router scans.
fn refresh_subscription_stats(shared: &Shared) {
    // The count is a walk of all connected sessions. It runs when a
    // subscription gauge is read, not for each connect and subscribe.
    shared.stats.mark_subscriptions_dirty();
}

/// Counts the subscriptions and the distinct topic filters of the
/// connected sessions and stores them in `stats`.
fn count_subscription_stats(sessions: &SessionManager, stats: &StatsStore) {
    // Snapshot connected sessions directly (MT-03): iterating bare
    // client ids plus a default-tenant `get` would miss (or conflate)
    // non-default tenants sharing a client id.
    let active = sessions.active_sessions();
    let mut subs = 0u64;
    let mut topic_set = std::collections::HashSet::new();
    for s in &active {
        let map = s.subscriptions.read();
        subs += map.len() as u64;
        for f in map.keys() {
            topic_set.insert(f.as_str().to_string());
        }
    }
    stats.set_subscriptions(subs);
    stats.set_topics(topic_set.len() as u64);
}

/// Recount retained topics into the stats store after a successful
/// insert/remove. Counts across all tenants in O(1) (no scan, no
/// allocation), so concurrent retained writes self-heal instead of
/// drifting the way check-then-act deltas would. Single-tenant installs
/// observe the same count as the old `#`-match read.
async fn sync_retained_stat(shared: &Shared) {
    shared.retain_writer.sync_stat().await;
}

/// Current Unix time in seconds for monitor points. Zero on clock failure
/// (fail closed: the point still records, ordered before real samples).
fn monitor_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Sample live counters into the shared monitor ring (F1-02, T-72).
///
/// Off the delivery path by construction: reads only lock-free atomics
/// (`Metrics` loads plus the `StatsStore` snapshot the lifecycle points
/// keep exact), then takes one short ring write lock to push the point.
/// The publish path never touches `Shared.monitor`, so a slow management
/// read can never stall fan-out or fan-in. Constant-time per tick and
/// bounded by `MAX_MONITOR_SAMPLES` in the ring.
// TODO(parity): which untracked counters should the sampler combine
// next? Durable, validation, transformation, persistence and action
// counters have no kernel source yet, so the shape omits them.
fn sample_monitor(shared: &Shared) {
    let conns = shared.metrics.connections_active().max(0) as u64;
    let counters = broker_api::v5::monitor::LiveCounters {
        connections: conns,
        live_connections: conns,
        topics: shared.stats.topics(),
        subscriptions: shared.stats.subscriptions(),
        received: shared.metrics.messages_received(),
        sent: shared.metrics.messages_forwarded(),
        dropped: shared.metrics.messages_dropped(),
        rules_matched: shared.metrics.rules_executed(),
    };
    shared.monitor.capture_live(monitor_now_secs(), counters);
    // The session history of the licence endpoints uses the same timer.
    shared
        .license
        .record_sessions(shared.sessions.connected_count(), monitor_now_secs());
}

/// Spawn the monitor sampler (F1-02, T-72): one immediate boot sample so
/// `GET /monitor` is never a permanent empty list, then one sample per
/// `interval_secs`. The task owns only a `Shared` clone and sleeps between
/// ticks, so it adds no lock, allocation or unbounded work to the publish
/// or deliver path.
fn spawn_monitor_sampler(shared: Shared, interval_secs: u64) {
    sample_monitor(&shared);
    if interval_secs == 0 {
        return;
    }
    // Bound the timer: faster than one second wakes the task for no
    // visible gain (points carry one-second stamps), slower than one hour
    // leaves the history stale past the four-hour ring window.
    let clamped = interval_secs.clamp(1, 3600);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(clamped)).await;
            sample_monitor(&shared);
        }
    });
}

/// Handles one inbound frame for any transport.
///
/// A link transport gives `native_bind = None`, and the bind request is
/// decoded from the frame metadata. A connection that the kernel owns
/// gives the request that it made from the CONNECT packet. Its bind
/// frame has no metadata.
async fn handle_inbound_frame_with<T>(
    frame: BrokerFrame,
    shared: &Shared,
    tx: &UnboundedSender<BrokerFrame>,
    transport: &T,
    bound: &mut Vec<(SessionKey, u64)>,
    waker: &Arc<tokio::sync::Notify>,
    native_bind: Option<&BindRequest>,
) -> Result<(), Box<dyn std::error::Error>>
where
    T: BrokerLinkTransport + ?Sized,
{
    // Measured ingress bytes from the real BrokerLink frame length.
    shared
        .metrics
        .inc_bytes_received_by(frame.total_frame_len() as u64);
    match frame.header.opcode {
        OpCode::BindConnection => {
            shared.metrics.inc_connect_received();
            let bind_reply = if let Some(request) = native_bind {
                // The bind operation has a large state. It runs one time
                // for each connection, thus it is on the heap. The state
                // of each connection task stays small.
                Box::pin(apply_bind_request(
                    &frame,
                    Ok(request.clone()),
                    request.protocol_version == 5,
                    shared,
                ))
                .await
            } else {
                // Production binds carry a request. The link-wire
                // decode lives in the test harness only.
                #[cfg(test)]
                let reply = Box::pin(apply_bind(&frame, shared)).await;
                #[cfg(not(test))]
                let reply: Option<BrokerFrame> = None;
                reply
            };
            if let Some(reply) = bind_reply {
                let accepted = is_binding_accepted(&reply);
                shared.metrics.inc_connack_sent();
                transport.send(reply).await?;
                shared.conns.register(frame.header.conn_id, tx.clone());
                if accepted
                    && shared
                        .sessions
                        .session_for_conn(frame.header.conn_id)
                        .is_none()
                {
                    if let Ok(close) = BrokerFrame::new(
                        OpCode::ConnClose,
                        frame.header.conn_id,
                        0,
                        Bytes::from(vec![0x8E]),
                        Bytes::new(),
                    ) {
                        let _ = transport.send(close).await;
                    }
                    shared.conns.unregister(frame.header.conn_id);
                    return Ok(());
                }
                let active = shared.metrics.inc_connections();
                shared.stats.set_connections(active.max(0) as u64);
                refresh_subscription_stats(shared);
                // Remember who we serve so a dead socket detaches cleanly.
                // One entry per bind: this transport multiplexes its whole
                // shard, and death must detach every client on it.
                let bind_parsed = match native_bind {
                    Some(request) => Ok(request.clone()),
                    #[cfg(test)]
                    None => decode_bind_meta(&frame.metadata),
                    #[cfg(not(test))]
                    None => Err("link-wire bind decode is test-only"),
                };
                if let Ok(req) = bind_parsed {
                    // D1-02: label the mailbox for per-client shed
                    // accounting and point it at this transport's directed
                    // wakeup. Short writes on bind only; the per-message
                    // publish path reads them without new work beyond its
                    // existing shard lock.
                    shared
                        .conns
                        .set_client_label(frame.header.conn_id, &req.client_id);
                    shared.conns.set_waker(frame.header.conn_id, waker);
                    // Death-detach key (MT-03): the index authoritative
                    // key for this fresh bind, so teardown prunes only
                    // the owning tenant's session. Falls back to the
                    // default-tenant key when the bind was refused
                    // before a session existed (nothing to detach).
                    let key = shared
                        .sessions
                        .key_for_conn(frame.header.conn_id)
                        .unwrap_or_else(|| SessionKey::default_tenant(&req.client_id));
                    // Connection-table tenant label (MT-07): the owning
                    // tenant from the session record just stamped above
                    // (single source of truth, never re-rendered here),
                    // so tenant-filtered connection queries observe the
                    // same tenant the session carries. Swept with the
                    // slot by `unregister` on disconnect. One short
                    // shard write on bind only; the publish and deliver
                    // paths never touch it.
                    shared
                        .conns
                        .set_tenant_label(frame.header.conn_id, &key.tenant);
                    bound.push((key.clone(), frame.header.conn_id));
                    // RQ-04: connect events fire in `apply_bind_request`.
                    // Never fire here. One bind fires once.
                    // Auto-subscribe hook (W1-39): only on accepted binds.
                    // Per-connect cost only; fan-out takes no new lock.
                    if accepted {
                        apply_auto_subscribe(
                            shared,
                            &req.client_id,
                            req.username.as_deref(),
                            frame.header.conn_id,
                        )
                        .await;
                    }
                }
                let replayed = if accepted {
                    match native_bind {
                        Some(request) => {
                            Box::pin(replay_offline_client(&frame, &request.client_id, shared))
                                .await
                        }
                        #[cfg(test)]
                        None => Box::pin(replay_offline(&frame, shared)).await,
                        #[cfg(not(test))]
                        None => 0,
                    }
                } else {
                    0
                };
                shared.metrics.inc_messages_forwarded_by(replayed as u64);
            }
        }
        OpCode::Ping => {
            shared.metrics.inc_pingreq_received();
            // The kernel listener answers PINGREQ directly. This arm
            // serves the test harness link frames only.
            #[cfg(test)]
            if let Some(reply) = reply_for_frame(&frame, &shared.sessions) {
                shared.metrics.inc_pingresp_sent();
                transport.send(reply).await?;
            }
        }
        OpCode::SubscribeIn => match apply_subscribe(&frame, shared).await {
            (Some(reply), retained) => {
                shared.metrics.inc_suback_sent();
                transport.send(reply).await?;
                // SUBACK first, then retained state, per MQTT ordering.
                for (idx, held) in retained.iter().enumerate() {
                    if let Err(err) = transport.send(held.clone()).await {
                        rollback_unsent_retained_credits(
                            shared,
                            frame.header.conn_id,
                            &retained[idx..],
                        );
                        return Err(err.into());
                    }
                }
                shared
                    .metrics
                    .inc_messages_forwarded_by(retained.len() as u64);
                shared.metrics.inc_publish_sent_by(retained.len() as u64);
                shared.metrics.inc_delivered_by(retained.len() as u64);
                shared.metrics.inc_bytes_sent_by(
                    retained
                        .iter()
                        .map(|held| held.total_frame_len() as u64)
                        .sum(),
                );
            }
            (None, _) => {
                warn!(
                    "Dropping malformed SubscribeIn from conn {}",
                    frame.header.conn_id
                );
            }
        },
        OpCode::UnsubscribeIn => match apply_unsubscribe(&frame, shared).await {
            Some(reply) => {
                transport.send(reply).await?;
            }
            None => {
                warn!(
                    "Dropping malformed UnsubscribeIn from conn {}",
                    frame.header.conn_id
                );
            }
        },
        OpCode::PublishIn => {
            let (ack, deliveries) = apply_publish(&frame, shared).await;
            if let Some(ack) = ack {
                transport.send(ack).await?;
            }
            // Only frames that reached a live mailbox count as
            // forwarded/sent/delivered (counted inside the helper); drops
            // are already counted inside `ConnTable::route`
            // (unknown-conn vs. dead-mailbox) and counting them here as
            // well would double-count the same loss. The helper also notes
            // slow backpressure (F1-03) without adding a lock or an
            // allocation to the fast path.
            route_publish_out_deliveries(shared, &frame, deliveries);
        }
        OpCode::PubAckIn => {
            // T-31: the edge forwards each subscriber PUBACK; release
            // the matching inflight entry so it is never replayed.
            // Unknown connections, unknown sessions and stale ids are
            // ignored: at-most-once PUBACKs from a previous incarnation
            // must never disturb live state.
            apply_puback(&frame, shared);
        }
        OpCode::PubRelIn => {
            // D1-01 inbound second phase: route once, reply PUBCOMP.
            let (comp, deliveries) = apply_qos2_pubrel(&frame, shared).await;
            if let Some(comp) = comp {
                transport.send(comp).await?;
            }
            let mut enqueued = 0u64;
            let mut enqueued_bytes = 0u64;
            for (conn_id, routed) in deliveries {
                let len = routed.total_frame_len() as u64;
                if shared.conns.route(conn_id, routed) {
                    enqueued += 1;
                    enqueued_bytes += len;
                }
            }
            shared.metrics.inc_messages_forwarded_by(enqueued);
            shared.metrics.inc_publish_sent_by(enqueued);
            shared.metrics.inc_delivered_by(enqueued);
            shared.metrics.inc_bytes_sent_by(enqueued_bytes);
        }
        OpCode::PubRecIn => {
            // D1-01 outbound second phase: the subscriber answered our
            // QoS 2 PUBLISH; send PUBREL. Unknown ids are ignored.
            if let Some(rel) = apply_qos2_pubrec(&frame, shared) {
                transport.send(rel).await?;
            }
        }
        OpCode::PubCompIn => {
            // D1-01 outbound completion: release the packet id.
            apply_qos2_pubcomp(&frame, shared);
        }
        OpCode::DisconnectIn => {
            // F1-01 ungraceful close: detach, then publish the taken will
            // once (no-op when the client registered none, or when a
            // racing unbind already consumed it). Routed exactly like a
            // normal publish fan-out below.
            let deliveries = apply_disconnect(&frame, shared).await;
            let mut enqueued = 0u64;
            let mut enqueued_bytes = 0u64;
            for (conn_id, routed) in deliveries {
                let len = routed.total_frame_len() as u64;
                if shared.conns.route(conn_id, routed) {
                    enqueued += 1;
                    enqueued_bytes += len;
                }
            }
            shared.metrics.inc_messages_forwarded_by(enqueued);
            shared.metrics.inc_publish_sent_by(enqueued);
            shared.metrics.inc_delivered_by(enqueued);
            shared.metrics.inc_bytes_sent_by(enqueued_bytes);
            // Forget this transport's identities for the connection so a
            // long-lived shard never accumulates one entry per bind ever.
            // Each forgotten entry balances its bind's `inc_connections`
            // here, leaving death to settle whatever remains.
            let before = bound.len();
            bound.retain(|(_, conn_id)| *conn_id != frame.header.conn_id);
            for _ in 0..before - bound.len() {
                let active = shared.metrics.dec_connections();
                shared.stats.set_connections(active.max(0) as u64);
            }
            if before != bound.len() {
                refresh_subscription_stats(shared);
            }
        }
        OpCode::UnbindConnection => {
            // X1-03: a version-5 DISCONNECT with reason 0x04 requests
            // will publication (handled like an ungraceful close);
            // every other reason (or no reason/version-4) suppresses
            // the will like a clean DISCONNECT. The reason rides the
            // trailing unbind byte; absent means version-4.
            if decode_unbind_reason(&frame.metadata) == Some(V5_DISCONNECT_WITH_WILL) {
                let deliveries = apply_disconnect_with_unbind_meta(&frame, shared).await;
                let mut enqueued = 0u64;
                let mut enqueued_bytes = 0u64;
                for (conn_id, routed) in deliveries {
                    let len = routed.total_frame_len() as u64;
                    if shared.conns.route(conn_id, routed) {
                        enqueued += 1;
                        enqueued_bytes += len;
                    }
                }
                shared.metrics.inc_messages_forwarded_by(enqueued);
                shared.metrics.inc_publish_sent_by(enqueued);
                shared.metrics.inc_delivered_by(enqueued);
                shared.metrics.inc_bytes_sent_by(enqueued_bytes);
            } else {
                apply_unbind(&frame, shared);
            }
            // Forget this transport's identities for the connection so a
            // long-lived shard never accumulates one entry per bind ever.
            // Each forgotten entry balances its bind's `inc_connections`
            // here, leaving death to settle whatever remains.
            let before = bound.len();
            bound.retain(|(_, conn_id)| *conn_id != frame.header.conn_id);
            for _ in 0..before - bound.len() {
                let active = shared.metrics.dec_connections();
                shared.stats.set_connections(active.max(0) as u64);
            }
            if before != bound.len() {
                refresh_subscription_stats(shared);
            }
        }
        _ => {
            // Other opcodes carry no synchronous reply in production.
            // The test harness answers Ping and Bind here.
            #[cfg(test)]
            if let Some(reply) = reply_for_frame(&frame, &shared.sessions) {
                transport.send(reply).await?;
            }
        }
    }
    Ok(())
}

/// Register the subscriptions of one `SubscribeIn` frame and build its
/// `SubAckOut` reply plus any retained messages for the new subscription.
/// Malformed framing yields `(None, empty)` (the edge treats a missing
/// SubAck as a hung request scoped to that connection).
///
/// Meta layout: `PacketId:16be | IdLen:16be | ClientId | N:16be |
/// (FilterLen:16be | Filter | QoS:8) * N`. Each granted code echoes the
/// requested QoS; a denial registers nothing. A version-4 denial answers
/// `0x80`, the only failure code its wire allows; a version-5 denial
/// answers the precise reason code (`0x87` not authorized, `0x8F` filter
/// invalid).
/// A new entry past the router `MAX_SUBSCRIPTIONS` bound is denied with
/// `0x97` Quota Exceeded and registers nothing.
/// `$share/<group>/` prefixes are split before routing, ACL checks,
/// retained fetch, and cluster announce so every downstream stage sees
/// the plain inner filter. Retained deliveries carry `retain = true` at
/// `min(subscription QoS, stored QoS)`.
fn encode_retained_publish_out(
    conn_id: u64,
    topic: &Topic,
    downlink_id: u16,
    effective_qos: u8,
    sub_id: u32,
    payload: Bytes,
) -> Option<BrokerFrame> {
    let topic_str = topic.as_str();
    let sub_id = if sub_id <= broker_router::MAX_SUBSCRIPTION_ID {
        sub_id
    } else {
        0
    };
    let mut meta = Vec::with_capacity(2 + topic_str.len() + 2 + 3 + 2 + 4);
    meta.extend_from_slice(&(topic_str.len() as u16).to_be_bytes());
    meta.extend_from_slice(topic_str.as_bytes());
    meta.extend_from_slice(&downlink_id.to_be_bytes());
    meta.push(effective_qos);
    meta.push(1u8); // retain: replayed retained state
    meta.push(0u8);
    meta.extend_from_slice(&protocol_v5::encode_topic_alias(0));
    if sub_id != 0 {
        meta.extend_from_slice(&sub_id.to_be_bytes());
    }
    BrokerFrame::new(OpCode::PublishOut, conn_id, 0, Bytes::from(meta), payload).ok()
}

fn make_pending_retained(
    msg: &broker_storage::StoredMessage,
    effective_qos: u8,
    sub_id: u32,
) -> broker_session::PendingRetained {
    let qos = match effective_qos {
        0 => QoS::AtMostOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtLeastOnce,
    };
    broker_session::PendingRetained {
        topic: msg.topic.clone(),
        qos,
        payload: msg.payload.clone(),
        sub_id,
    }
}

fn rollback_session_retained_credits(
    session: Option<&broker_session::Session>,
    unsent: &[BrokerFrame],
) {
    let Some(session) = session else {
        return;
    };
    for frame in unsent {
        if frame.header.opcode != OpCode::PublishOut || frame.metadata.len() < 5 {
            continue;
        }
        let tlen = u16::from_be_bytes([frame.metadata[0], frame.metadata[1]]) as usize;
        if frame.metadata.len() < 2 + tlen + 3 {
            continue;
        }
        let pid = u16::from_be_bytes([frame.metadata[2 + tlen], frame.metadata[2 + tlen + 1]]);
        let qos = frame.metadata[2 + tlen + 2];
        if qos == 1 {
            let _ = session.ack_inflight(pid);
        } else if qos == 2 {
            let _ = session.ack_qos2_pubcomp(pid);
        }
    }
}

fn rollback_unsent_retained_credits(shared: &Shared, conn_id: u64, unsent: &[BrokerFrame]) {
    let session = shared.sessions.session_for_conn(conn_id).or_else(|| {
        shared
            .sessions
            .client_id_for_conn(conn_id)
            .as_deref()
            .and_then(|p| shared.sessions.get(p))
    });
    let Some(session) = session else {
        return;
    };
    for frame in unsent {
        if frame.header.opcode != OpCode::PublishOut || frame.metadata.len() < 5 {
            continue;
        }
        let tlen = u16::from_be_bytes([frame.metadata[0], frame.metadata[1]]) as usize;
        if frame.metadata.len() < 2 + tlen + 3 {
            continue;
        }
        let pid = u16::from_be_bytes([frame.metadata[2 + tlen], frame.metadata[2 + tlen + 1]]);
        let qos = frame.metadata[2 + tlen + 2];
        if qos == 1 {
            let _ = session.ack_inflight(pid);
        } else if qos == 2 {
            let _ = session.ack_qos2_pubcomp(pid);
        }
    }
}

async fn apply_subscribe(
    frame: &BrokerFrame,
    shared: &Shared,
) -> (Option<BrokerFrame>, Vec<BrokerFrame>) {
    shared.metrics.inc_subscribe_received();
    let (packet_id, client_id, subs) = match decode_subscribe_meta(&frame.metadata) {
        Some(parts) => parts,
        None => return (None, Vec::new()),
    };

    let mut codes = Vec::with_capacity(subs.len());
    // Each granted filter, with its retain-handling decision and the
    // subscription identifier that the retained replay carries.
    let mut retained = Vec::new();
    let mut granted_filters: Vec<TopicFilter> = Vec::with_capacity(subs.len());
    // AP-11: a denied subscribe disconnects when the stored `deny_action`
    // is `disconnect`. The flag accumulates across filters; the close is
    // ordered once below after the SubAck is built, so every filter still
    // gets its own code.
    let mut authz_disconnect = false;
    // Tenant scope (MT-02, MT-03): resolved once from the subscribing
    // connection's own session, before any authorization decision is
    // recorded, so the denial/grant below lands on the owning tenant's
    // session and never on a default-tenant namesake. Two tenants sharing
    // a client id subscribe independently; a tenant mismatch at publish
    // time delivers nothing, counted, never an error to the publisher.
    let conn_session = shared.sessions.session_for_conn(frame.header.conn_id);
    let conn_was_bound = conn_session.is_some();
    let tenant = conn_session
        .as_ref()
        .map(|s| s.tenant_id.read().clone())
        .unwrap_or_else(|| session_tenant_for_conn(shared, frame.header.conn_id, &client_id));
    #[cfg(test)]
    let (superseded_conn, owner) = {
        let fallback_session = if conn_session.is_none() {
            shared.sessions.get_in_tenant(&tenant, &client_id)
        } else {
            None
        };
        let superseded = fallback_session.as_ref().is_some_and(|s| {
            let cur_conn = *s.conn_id.read();
            cur_conn != Some(frame.header.conn_id)
                && (cur_conn.is_some() || s.ended_at_ms.read().is_some())
        });
        let owner = conn_session.or_else(|| (!superseded).then_some(fallback_session).flatten());
        (superseded, owner)
    };
    #[cfg(not(test))]
    let (superseded_conn, owner) = (conn_session.is_none(), conn_session);
    let max_deferred_msgs = shared
        .sessions
        .max_offline_queue()
        .unwrap_or(broker_session::DEFAULT_MAX_OFFLINE_QUEUE)
        .max(1);
    let initial_credits = owner
        .as_ref()
        .map(|s| {
            if s.protocol_version() == 5 {
                usize::from(s.client_receive_maximum().max(1))
            } else {
                0
            }
        })
        .unwrap_or(0);
    let mut remaining_lookup_budget = max_deferred_msgs
        .saturating_add(initial_credits)
        .saturating_mul(4)
        .max(1024)
        .min(broker_session::DEFAULT_MAX_OFFLINE_QUEUE.saturating_mul(2));
    let mut remaining_scan_budget = remaining_lookup_budget.saturating_mul(4);
    let expected_conn_id = owner.as_ref().and_then(|bound| {
        if conn_was_bound {
            Some(frame.header.conn_id)
        } else {
            (*bound.conn_id.read()).map(|_| frame.header.conn_id)
        }
    });
    for entry in subs {
        if superseded_conn {
            codes.push(if entry.has_options {
                V5_SUBACK_NOT_AUTHORIZED
            } else {
                V5_SUBACK_UNSPECIFIED
            });
            authz_disconnect = true;
            continue;
        }
        let raw_filter_str = entry.filter;
        // X1-03: decode the per-filter options. Legacy entries carry a
        // plain QoS byte; extended entries carry the options byte plus a
        // subscription identifier. Malformed options fail only this
        // filter with 0x8F (topic filter invalid), never the whole packet.
        let (qos_raw, no_local, rap, rh, sub_id) = if entry.has_options {
            match Subscription::decode_options_byte(entry.opts_raw) {
                Some((qos, nl, rap, rh)) => {
                    if entry.subscription_id > broker_router::MAX_SUBSCRIPTION_ID {
                        codes.push(V5_SUBACK_FILTER_INVALID);
                        continue;
                    }
                    (u8::from(qos), nl, rap, rh, entry.subscription_id)
                }
                None => {
                    codes.push(V5_SUBACK_FILTER_INVALID);
                    continue;
                }
            }
        } else {
            match QoS::try_from(entry.opts_raw) {
                // X1-03: version-4 subscribes carry no options byte. The
                // 3.1.1 wire has no retain-as-published knob and always
                // delivers the retain flag as published, so the legacy
                // mirror preserves it (rap = true); only version-5
                // RAP=0 clears it. Keeps v4 bytes and semantics identical.
                Ok(qos) => (u8::from(qos), false, true, 0u8, 0u32),
                Err(_) => {
                    codes.push(V5_SUBACK_UNSPECIFIED);
                    continue;
                }
            }
        };
        // B4-04 ordered regex filter rewriting (subscribe scope): the
        // first matching subscribe/both rule wins and the router, the
        // session mirror, the cluster announce and retained replay all
        // observe the rewritten filter. `None` subscribes unchanged.
        // System-prefixed filters bypass rewriting inside the router so
        // shared-subscription group splitting keeps its semantics.
        let filter_str = match shared.router.rewrite_subscribe(&raw_filter_str) {
            Some(rewritten) => rewritten,
            None => raw_filter_str,
        };
        // Delayed topics are publish-only markers, never subscriptions.
        // Version-4 denies with 0x80 (bytes unchanged); version-5 with
        // 0x8F (topic filter invalid), failing only this filter.
        if strip_delayed_prefix(&filter_str).is_some() {
            codes.push(if entry.has_options {
                V5_SUBACK_FILTER_INVALID
            } else {
                V5_SUBACK_UNSPECIFIED
            });
            continue;
        }
        let parsed = TopicFilter::new(filter_str).ok().and_then(|filter| {
            QoS::try_from(qos_raw).ok().and_then(|qos| {
                split_shared_filter(&filter).map(|(group, inner)| (inner, qos, group))
            })
        });
        let (filter, qos, group) = match parsed {
            Some(parts) => parts,
            None => {
                codes.push(if entry.has_options {
                    V5_SUBACK_FILTER_INVALID
                } else {
                    V5_SUBACK_UNSPECIFIED
                });
                continue;
            }
        };
        // X1-03: no-local is forbidden on shared subscriptions (protocol
        // error per the specification). Fail only this filter with 0x8F
        // (topic filter invalid): the filter carries an option the
        // specification forbids on shared subscriptions.
        if no_local && group.is_some() {
            codes.push(V5_SUBACK_FILTER_INVALID);
            continue;
        }
        // X1-03: wildcard subscriptions are supported, so the
        // wildcard-not-supported code never applies. No branch returns
        // it while every filter class stays supported.
        // Tenant scope (MT-06): authorisation runs in the subscriber's
        // own tenant resolved above, so a rule in one tenant never
        // grants subscribe in another. Same single-guard discipline as
        // the publish path. The stored cache policy is consulted first.
        let cached_subscribe = owner.as_ref().and_then(|session| {
            cached_authz_lookup(
                shared,
                session,
                "subscribe",
                filter.as_str(),
                qos_raw,
                false,
            )
        });
        let local_denied = match cached_subscribe {
            Some(allow) => !allow,
            None => {
                // Fast superuser flag first (one atomic, no lock).
                // Only the rare superuser bypass clones the name.
                let is_superuser = owner
                    .as_ref()
                    .is_some_and(|session| session.verified_is_superuser.load(Ordering::Relaxed));
                if is_superuser {
                    let verified = owner
                        .as_ref()
                        .and_then(|s| s.verified_username.read().clone());
                    shared
                        .auth
                        .authorize_subscribe_in_tenant_as(
                            &tenant,
                            &client_id,
                            verified.as_deref(),
                            &filter,
                        )
                        .await
                        .is_err()
                } else {
                    shared
                        .auth
                        .authorize_subscribe_in_tenant(&tenant, &client_id, &filter)
                        .await
                        .is_err()
                }
            }
        };
        let local_denied = local_denied
            || owner
                .as_ref()
                .is_some_and(|session| !session_credential_allows(session, filter.as_str(), false));
        if local_denied {
            // Per-client decision cache (W2-01): record the denial on the
            // owning tenant's session so the management read observes it.
            // One bounded per-session insert on the subscribe
            // authorization event only; delivery never touches this lock.
            // A cache hit needs no new insert.
            if cached_subscribe.is_none() {
                if let Some(session) = owner.as_ref() {
                    record_authz_under_policy(
                        shared,
                        session,
                        "subscribe",
                        filter.as_str(),
                        qos_raw,
                        false,
                        false,
                    );
                }
            }
            // Fire $events/client_check_authz_complete.
            // Rules belong to the default tenant, so another tenant fires
            // nothing. Subscribe carries no retain flag, so retain stays
            // absent. Check the flag before any lock.
            if shared
                .engine
                .is_event_source_selected(broker_rules::EventSource::ClientCheckAuthzComplete)
                && is_default_tenant(&tenant)
            {
                if let Some(session) = owner.as_ref() {
                    let username = session.username.read().clone();
                    let peerhost = session.peerhost.read().clone();
                    shared.engine.fire_client_check_authz_complete(
                        broker_rules::ClientCheckAuthzCompleteEvent {
                            client_id: client_id.clone(),
                            username,
                            peerhost,
                            action: "subscribe".to_string(),
                            topic: filter.as_str().to_string(),
                            qos: qos_raw,
                            retain: None,
                            allow: false,
                            source: "local".to_string(),
                        },
                    );
                }
            }
            // Version-4 peers see 0x80 (the only version-3 failure code);
            // version-5 peers see 0x87 Not Authorized. Neither registers
            // anything. With the stored `deny_action: disconnect` the
            // connection also closes below with 0x87 after the SubAck.
            if shared.auth.authz_deny_disconnects() {
                authz_disconnect = true;
            }
            codes.push(if entry.has_options {
                V5_SUBACK_NOT_AUTHORIZED
            } else {
                V5_SUBACK_UNSPECIFIED
            });
            continue;
        }
        // B5-03: database ACLs for users with no local record (same key
        // rule as the publish path: session username, else client id).
        // The username rides the owning tenant's session resolved above,
        // never a default-tenant namesake. A database outage fails closed
        // like a local denial. A cached allow skips this leg.
        if cached_subscribe != Some(true) {
            if let Some(db) = shared.db_auth.as_ref().filter(|d| d.is_configured()) {
                let username = owner.as_ref().and_then(|s| s.username.read().clone());
                // Tenant scope (MT-06): the probe and the lookup run in the
                // subscriber's own tenant (deferred: only the built-in table
                // holds per-tenant state).
                let is_local = username
                    .as_deref()
                    .is_some_and(|u| shared.auth.has_user_in_tenant(&tenant, u));
                if !is_local {
                    let key = username.as_deref().unwrap_or(&client_id);
                    if db
                        .authorize_subscribe_in_tenant(&tenant, key, &filter)
                        .await
                        .is_err()
                    {
                        if shared.engine.is_event_source_selected(
                            broker_rules::EventSource::ClientCheckAuthzComplete,
                        ) && is_default_tenant(&tenant)
                        {
                            if let Some(session) = owner.as_ref() {
                                let username = session.username.read().clone();
                                let peerhost = session.peerhost.read().clone();
                                shared.engine.fire_client_check_authz_complete(
                                    broker_rules::ClientCheckAuthzCompleteEvent {
                                        client_id: client_id.clone(),
                                        username,
                                        peerhost,
                                        action: "subscribe".to_string(),
                                        topic: filter.as_str().to_string(),
                                        qos: qos_raw,
                                        retain: None,
                                        allow: false,
                                        source: "database".to_string(),
                                    },
                                );
                            }
                        }
                        // The database denial follows the same version split
                        // as the local denial above. Record it under the cache
                        // policy so the next operation reuses it until TTL.
                        if cached_subscribe.is_none() {
                            if let Some(session) = owner.as_ref() {
                                record_authz_under_policy(
                                    shared,
                                    session,
                                    "subscribe",
                                    filter.as_str(),
                                    qos_raw,
                                    false,
                                    false,
                                );
                            }
                        }
                        codes.push(if entry.has_options {
                            V5_SUBACK_NOT_AUTHORIZED
                        } else {
                            V5_SUBACK_UNSPECIFIED
                        });
                        continue;
                    }
                }
            }
        }
        // X1-03 retain-handling 1 needs the pre-existing state: a
        // re-subscribe of an already-held filter suppresses retained
        // replay, a new filter does not. One short session read on the
        // subscribe event only; delivery never touches this lock.
        let existed = owner
            .as_ref()
            .is_some_and(|session| session.subscriptions.read().contains_key(&filter));
        // Tenant scope (MT-02, MT-03): the subscription carries the
        // subscriber's tenant resolved above from the subscribing
        // connection's own session, so two tenants sharing a client id
        // subscribe independently.
        // B4-08 bound: a new entry past MAX_SUBSCRIPTIONS is denied fail
        // closed. Existing delivery proceeds. A version-5 client gets 0x97
        // Quota Exceeded; a version-4 client gets 0x80. The denial
        // registers nothing (no session mirror, no grant, no announce,
        // no retained replay).
        let mut sub = Subscription::with_options(
            client_id.clone(),
            frame.header.conn_id,
            qos,
            no_local,
            rap,
            rh,
            sub_id,
        );
        sub.group = group;
        sub.tenant = tenant.clone().into();
        sub.session_id = owner.as_ref().map_or(0, |s| s.id.0);
        {
            let _route_guard = shared.sessions.lock_route_guard();
            if let Some(bound) = owner.as_ref() {
                let still_bound = if conn_was_bound {
                    shared
                        .sessions
                        .session_for_conn(frame.header.conn_id)
                        .is_some_and(|cur| cur.id == bound.id)
                } else {
                    let cur_conn = *bound.conn_id.read();
                    cur_conn == Some(frame.header.conn_id)
                        || (cur_conn.is_none() && bound.ended_at_ms.read().is_none())
                };
                if !still_bound {
                    codes.push(if entry.has_options {
                        V5_SUBACK_NOT_AUTHORIZED
                    } else {
                        V5_SUBACK_UNSPECIFIED
                    });
                    authz_disconnect = true;
                    continue;
                }
            }
        }
        let send_retained = match rh {
            1 => !existed,
            2 => false,
            _ => true,
        };
        let _retained_sub_gate = if send_retained {
            Some(shared.retained_subscribe_gate.read().await)
        } else {
            None
        };
        let mut filter_retained = Vec::new();
        let mut staged_retained: Vec<(broker_storage::StoredMessage, u8)> = Vec::new();
        let mut quota_exceeded = false;
        let mut superseded_during_retained = false;
        if send_retained {
            let subscriber_tenant = owner
                .as_ref()
                .map(|s| s.tenant_id.read().clone())
                .unwrap_or_else(|| tenant.clone());
            let (matched, truncated, scanned) = match shared
                .retained
                .find_matching_in_tenant_bounded(
                    &filter,
                    &subscriber_tenant,
                    remaining_lookup_budget,
                    remaining_scan_budget,
                )
                .await
            {
                Ok(res) => res,
                Err(e) => {
                    warn!("Retained lookup failed for {}: {}", filter.as_str(), e);
                    (Vec::new(), false, 0)
                }
            };
            if filter.as_str().contains('+') || filter.as_str().contains('#') {
                remaining_scan_budget = remaining_scan_budget.saturating_sub(scanned);
            }
            remaining_lookup_budget = remaining_lookup_budget.saturating_sub(matched.len());
            quota_exceeded = truncated;
            for msg in matched {
                if quota_exceeded {
                    break;
                }
                let conn_still_current = owner.as_ref().is_none_or(|bound| {
                    if conn_was_bound {
                        shared
                            .sessions
                            .session_for_conn(frame.header.conn_id)
                            .is_some_and(|cur| {
                                cur.id == bound.id
                                    && expected_conn_id
                                        .is_none_or(|cid| *cur.conn_id.read() == Some(cid))
                            })
                    } else {
                        expected_conn_id.is_none_or(|cid| *bound.conn_id.read() == Some(cid))
                    }
                });
                if !conn_still_current {
                    superseded_during_retained = true;
                    break;
                }
                let effective = std::cmp::min(qos_raw, u8::from(msg.qos));
                let probe_id = if effective == 0 { 0u16 } else { 1u16 };
                let Some(probe_frame) = encode_retained_publish_out(
                    frame.header.conn_id,
                    &msg.topic,
                    probe_id,
                    effective,
                    sub_id,
                    msg.payload.clone(),
                ) else {
                    continue;
                };
                if owner
                    .as_ref()
                    .is_some_and(|s| v5_encoded_packet_too_large(s, &probe_frame))
                {
                    shared.metrics.inc_messages_dropped();
                    continue;
                }
                staged_retained.push((msg, effective));
            }
        }
        if superseded_during_retained || quota_exceeded {
            if superseded_during_retained {
                codes.push(if entry.has_options {
                    V5_SUBACK_NOT_AUTHORIZED
                } else {
                    V5_SUBACK_UNSPECIFIED
                });
                authz_disconnect = true;
            } else {
                shared.metrics.inc_messages_dropped();
                codes.push(if entry.has_options {
                    V5_SUBACK_QUOTA_EXCEEDED
                } else {
                    V5_SUBACK_UNSPECIFIED
                });
            }
            continue;
        }
        let mut commit_failed_quota = false;
        let mut commit_superseded = false;
        {
            let _rg = shared.sessions.lock_route_guard();
            let _replay_guard = owner.as_ref().map(|s| s.lock_replay());
            let still_owner = owner.as_ref().is_none_or(|bound| {
                if conn_was_bound {
                    shared
                        .sessions
                        .session_for_conn(frame.header.conn_id)
                        .is_some_and(|cur| {
                            cur.id == bound.id
                                && expected_conn_id
                                    .is_none_or(|cid| *cur.conn_id.read() == Some(cid))
                        })
                } else {
                    expected_conn_id.is_none_or(|cid| *bound.conn_id.read() == Some(cid))
                }
            });
            if !still_owner {
                commit_superseded = true;
            } else {
                enum DeferCommitErr {
                    QuotaExceeded,
                    Superseded,
                }
                let mut staged_opt = Some(staged_retained);
                match shared.router.try_subscribe_with(&filter, sub, || {
                    let mut deferred_retained = Vec::new();
                    let mut staged_iter = staged_opt.take().unwrap_or_default().into_iter();
                    while let Some((msg, effective)) = staged_iter.next() {
                        let downlink_id = if effective == 0 {
                            0u16
                        } else if effective == 1 {
                            if let Some(session) = owner.as_ref() {
                                let Some((pid, outcome)) = session
                                    .try_alloc_qos1_downlink_v5_checked_for_conn(
                                        expected_conn_id,
                                        msg.topic.clone(),
                                        QoS::AtLeastOnce,
                                        true,
                                        msg.payload.clone(),
                                        std::time::Instant::now(),
                                    )
                                else {
                                    if expected_conn_id
                                        .is_some_and(|cid| *session.conn_id.read() != Some(cid))
                                    {
                                        rollback_session_retained_credits(
                                            owner.as_deref(),
                                            &filter_retained,
                                        );
                                        filter_retained.clear();
                                        return Err(DeferCommitErr::Superseded);
                                    }
                                    deferred_retained
                                        .push(make_pending_retained(&msg, effective, sub_id));
                                    for (rest, rest_eff) in staged_iter {
                                        deferred_retained
                                            .push(make_pending_retained(&rest, rest_eff, sub_id));
                                    }
                                    break;
                                };
                                match outcome {
                                    InflightTrackOutcome::Tracked => {}
                                    InflightTrackOutcome::Spilled => {
                                        shared.metrics.inc_inflight_spilled();
                                    }
                                    InflightTrackOutcome::Dropped => {
                                        shared.metrics.inc_inflight_dropped();
                                        shared.metrics.inc_inflight_spill_evicted();
                                    }
                                }
                                pid
                            } else {
                                1u16
                            }
                        } else if let Some(session) = owner.as_ref() {
                            let Some((pid, tracked)) = session
                                .try_alloc_qos2_outbound_v5_checked_for_conn(
                                    expected_conn_id,
                                    msg.topic.clone(),
                                    true,
                                    msg.payload.clone(),
                                )
                            else {
                                if expected_conn_id
                                    .is_some_and(|cid| *session.conn_id.read() != Some(cid))
                                {
                                    rollback_session_retained_credits(
                                        owner.as_deref(),
                                        &filter_retained,
                                    );
                                    filter_retained.clear();
                                    return Err(DeferCommitErr::Superseded);
                                }
                                deferred_retained
                                    .push(make_pending_retained(&msg, effective, sub_id));
                                for (rest, rest_eff) in staged_iter {
                                    deferred_retained
                                        .push(make_pending_retained(&rest, rest_eff, sub_id));
                                }
                                break;
                            };
                            if !tracked {
                                shared.metrics.inc_inflight_dropped();
                            }
                            pid
                        } else {
                            1u16
                        };
                        if let Some(routed) = encode_retained_publish_out(
                            frame.header.conn_id,
                            &msg.topic,
                            downlink_id,
                            effective,
                            sub_id,
                            msg.payload,
                        ) {
                            filter_retained.push(routed);
                        } else if let Some(session) = owner.as_ref() {
                            if effective == 1 {
                                let _ = session.ack_inflight(downlink_id);
                            } else if effective == 2 {
                                let _ = session.ack_qos2_pubcomp(downlink_id);
                            }
                        }
                    }
                    if !deferred_retained.is_empty() {
                        if let Some(session) = owner.as_ref() {
                            match session.try_defer_retained_all_or_none_for_conn(
                                expected_conn_id,
                                max_deferred_msgs,
                                deferred_retained,
                            ) {
                                broker_session::DeferRetainedOutcome::Admitted => Ok(()),
                                broker_session::DeferRetainedOutcome::QuotaExceeded => {
                                    rollback_session_retained_credits(
                                        owner.as_deref(),
                                        &filter_retained,
                                    );
                                    filter_retained.clear();
                                    Err(DeferCommitErr::QuotaExceeded)
                                }
                                broker_session::DeferRetainedOutcome::Superseded => {
                                    rollback_session_retained_credits(
                                        owner.as_deref(),
                                        &filter_retained,
                                    );
                                    filter_retained.clear();
                                    Err(DeferCommitErr::Superseded)
                                }
                            }
                        } else {
                            Ok(())
                        }
                    } else {
                        Ok(())
                    }
                }) {
                    Ok(true) => {
                        if cached_subscribe.is_none() {
                            if let Some(session) = owner.as_ref() {
                                record_authz_under_policy(
                                    shared,
                                    session,
                                    "subscribe",
                                    filter.as_str(),
                                    qos_raw,
                                    false,
                                    true,
                                );
                            }
                        }
                        shared.sessions.add_subscription_if_session_in_tenant(
                            &tenant,
                            &client_id,
                            owner.as_ref().map(|s| s.id),
                            filter.clone(),
                            qos,
                            u8::from(no_local),
                            u8::from(rap),
                            rh,
                        );
                    }
                    Ok(false) | Err(DeferCommitErr::QuotaExceeded) => {
                        commit_failed_quota = true;
                    }
                    Err(DeferCommitErr::Superseded) => {
                        commit_superseded = true;
                    }
                }
            }
        }
        drop(_retained_sub_gate);
        if commit_superseded || commit_failed_quota {
            rollback_session_retained_credits(owner.as_deref(), &filter_retained);
            if commit_superseded {
                codes.push(if entry.has_options {
                    V5_SUBACK_NOT_AUTHORIZED
                } else {
                    V5_SUBACK_UNSPECIFIED
                });
                authz_disconnect = true;
            } else {
                shared.metrics.inc_messages_dropped();
                codes.push(if entry.has_options {
                    V5_SUBACK_QUOTA_EXCEEDED
                } else {
                    V5_SUBACK_UNSPECIFIED
                });
            }
            continue;
        }
        if shared
            .engine
            .is_event_source_selected(broker_rules::EventSource::ClientCheckAuthzComplete)
            && is_default_tenant(&tenant)
        {
            if let Some(session) = owner.as_ref() {
                let username = session.username.read().clone();
                let peerhost = session.peerhost.read().clone();
                shared.engine.fire_client_check_authz_complete(
                    broker_rules::ClientCheckAuthzCompleteEvent {
                        client_id: client_id.clone(),
                        username,
                        peerhost,
                        action: "subscribe".to_string(),
                        topic: filter.as_str().to_string(),
                        qos: qos_raw,
                        retain: None,
                        allow: true,
                        source: "local".to_string(),
                    },
                );
            }
        }
        if shared
            .engine
            .is_event_source_selected(broker_rules::EventSource::SessionSubscribed)
            && is_default_tenant(&tenant)
        {
            if let Some(session) = owner.as_ref() {
                let username = session.username.read().clone();
                let peerhost = session.peerhost.read().clone();
                shared
                    .engine
                    .fire_session_subscribed(broker_rules::SessionSubscribedEvent {
                        client_id: client_id.clone(),
                        username,
                        peerhost,
                        topic: filter.as_str().to_string(),
                        qos: qos_raw,
                        nl: u8::from(no_local),
                        rap: u8::from(rap),
                        rh,
                    });
            }
        }
        retained.extend(filter_retained);
        granted_filters.push(filter);
        codes.push(qos_raw);
    }
    if !granted_filters.is_empty() {
        refresh_subscription_stats(shared);
    }

    let mut meta = Vec::with_capacity(2 + codes.len());
    meta.extend_from_slice(&packet_id.to_be_bytes());
    meta.extend_from_slice(&codes);
    let reply = BrokerFrame::new(
        OpCode::SubAckOut,
        frame.header.conn_id,
        frame.header.sequence_no,
        Bytes::from(meta),
        Bytes::new(),
    )
    .ok();

    // Clustered mode: publish our route summary (filter-level only, never
    // individual subscriptions) so peers forward matching publishes here.
    if let Some(cluster) = &shared.cluster {
        for filter in &granted_filters {
            if let Err(e) = cluster.announce_filter(filter).await {
                warn!("Cluster announce failed for {}: {}", filter.as_str(), e);
            }
        }
    }

    // AP-11: a denied subscribe with the stored `deny_action:
    // disconnect` closes the connection with 0x87 after the SubAck above
    // is built, so the client still observes its per-filter codes. The
    // close routes immediately; the reply follows the normal path.
    if authz_disconnect {
        order_authz_disconnect(shared, frame.header.conn_id);
    }

    (reply, retained)
}

/// Remove the subscriptions of one `UnsubscribeIn` frame and build its
/// `UnsubAckOut` reply. Both transports share this function: the edge
/// path and the kernel listener path build the same frame.
/// Malformed framing yields `None` (the caller drops the frame).
///
/// Meta layout: `PacketId:16be | IdLen:16be | ClientId | N:16be |
/// (FilterLen:16be | Filter) * N`. Each reply code covers one filter:
/// `0x00` success, `0x11` no subscription existed, `0x8F` topic filter
/// invalid. Version-4 peers observe the packet identifier only; the
/// wider set rides the same bytes for version-5 peers.
/// A `$share/<group>/` prefix removes the inner filter, like subscribe.
/// A rewrite rule applies before removal, like subscribe.
async fn apply_unsubscribe(frame: &BrokerFrame, shared: &Shared) -> Option<BrokerFrame> {
    let (packet_id, client_id, filters) = decode_unsubscribe_meta(&frame.metadata)?;
    let _route_guard = shared.sessions.lock_route_guard();
    // Tenant scope: resolve once from the owning connection, like subscribe.
    // Two tenants with one client id unsubscribe independently, and a
    // superseded connection must never remove its replacement's routes.
    let conn_session = session_for_conn_if_client(shared, frame.header.conn_id, &client_id);
    let tenant = conn_session
        .as_ref()
        .map(|s| s.tenant_id.read().clone())
        .unwrap_or_else(|| session_tenant_for_conn(shared, frame.header.conn_id, &client_id));
    #[cfg(test)]
    let owner = {
        let fallback_session = if conn_session.is_none() {
            shared.sessions.get_in_tenant(&tenant, &client_id)
        } else {
            None
        };
        let superseded = fallback_session.as_ref().is_some_and(|s| {
            let cur_conn = *s.conn_id.read();
            cur_conn != Some(frame.header.conn_id)
                && (cur_conn.is_some() || s.ended_at_ms.read().is_some())
        });
        conn_session.or_else(|| (!superseded).then_some(fallback_session).flatten())
    };
    #[cfg(not(test))]
    let owner = conn_session;
    let mut codes = Vec::with_capacity(filters.len());
    let mut removed_any = false;
    let mut removed = Vec::new();
    for raw in &filters {
        // Subscribe rewrites before it stores. Unsubscribe rewrites
        // before it removes, so it removes the stored form.
        let filter_str = match shared.router.rewrite_subscribe(raw) {
            Some(rewritten) => rewritten,
            None => raw.clone(),
        };
        let parsed = TopicFilter::new(filter_str)
            .ok()
            .and_then(|filter| split_shared_filter(&filter).map(|(_, inner)| inner));
        let Some(inner) = parsed else {
            codes.push(V5_UNSUBACK_FILTER_INVALID);
            continue;
        };
        let Some(session) = owner.as_ref() else {
            codes.push(V5_UNSUBACK_NO_SUBSCRIPTION);
            continue;
        };
        let existed = session.subscriptions.read().contains_key(&inner);
        // Fire the event before removal. Rules belong to the default
        // tenant, so another tenant fires nothing. Check flag first.
        if shared
            .engine
            .is_event_source_selected(broker_rules::EventSource::SessionUnsubscribed)
            && is_default_tenant(&tenant)
        {
            {
                let username = session.username.read().clone();
                let peerhost = session.peerhost.read().clone();
                shared
                    .engine
                    .fire_session_unsubscribed(broker_rules::SessionUnsubscribedEvent {
                        client_id: client_id.clone(),
                        username,
                        peerhost,
                        topic: inner.as_str().to_string(),
                    });
            }
        }
        shared
            .router
            .unsubscribe_in_tenant(&inner, &client_id, &tenant);
        shared
            .sessions
            .remove_subscription_in_tenant(&tenant, &client_id, &inner);
        if existed {
            removed_any = true;
            removed.push(inner.clone());
            codes.push(V5_UNSUBACK_SUCCESS);
        } else {
            // TODO(parity): the reference answer for an unknown filter
            // is open (success vs no-subscription-existed). Current
            // choice answers 0x11 when the session holds no such filter.
            codes.push(V5_UNSUBACK_NO_SUBSCRIPTION);
        }
    }
    drop(_route_guard);
    if removed_any {
        refresh_subscription_stats(shared);
    }
    // Clustered mode: withdraw our route summary.
    if let Some(cluster) = &shared.cluster {
        for filter in &removed {
            if let Err(e) = cluster.revoke_filter(filter).await {
                warn!("Cluster revoke failed for {}: {}", filter.as_str(), e);
            }
        }
    }
    let mut meta = Vec::with_capacity(2 + codes.len());
    meta.extend_from_slice(&packet_id.to_be_bytes());
    meta.extend_from_slice(&codes);
    BrokerFrame::new(
        OpCode::UnsubAckOut,
        frame.header.conn_id,
        frame.header.sequence_no,
        Bytes::from(meta),
        Bytes::new(),
    )
    .ok()
}

/// Replay a resumed durable session's backlog onto its new connection:
/// unacknowledged QoS 1 window downlinks first (T-31, DUP set, original
/// order, same packet ids), then the QoS 1 spill overflow (B4-01, DUP
/// set, arrival order, same packet ids), then uncompleted QoS 2
/// downlinks (D1-01, in queued order: PUBLISH with DUP set while waiting
/// for PUBREC, PUBREL while waiting for PUBCOMP), then the offline queue
/// of never-routed messages. Runs after the `SessionBinding` reply so
/// the edge always observes binding before backlog. Returns the replayed
/// frame count.
/// Test harness only: production replays by client id directly.
#[cfg(test)]
async fn replay_offline(frame: &BrokerFrame, shared: &Shared) -> usize {
    match decode_bind_meta(&frame.metadata) {
        Ok(req) => replay_offline_client(frame, &req.client_id, shared).await,
        Err(_) => 0,
    }
}

/// The offline replay for the client that the bind in `frame` named.
async fn replay_offline_client(frame: &BrokerFrame, client_id: &str, shared: &Shared) -> usize {
    // Replay delivers only the owning tenant's queued state (MT-03):
    // prefer the session bound to this connection (tenant-correct when
    // two tenants share the client id), falling back to the
    // default-tenant record for binds that predate the index entry.
    let (session, expected_conn_id) =
        match session_for_conn_if_client(shared, frame.header.conn_id, client_id) {
            Some(session) => (session, Some(frame.header.conn_id)),
            None => {
                #[cfg(test)]
                {
                    match shared.sessions.get(client_id).filter(|s| {
                        let cur = *s.conn_id.read();
                        cur == Some(frame.header.conn_id)
                            || (cur.is_none() && s.ended_at_ms.read().is_none())
                    }) {
                        Some(s) => {
                            let expected = (*s.conn_id.read()).map(|_| frame.header.conn_id);
                            (s, expected)
                        }
                        None => return 0,
                    }
                }
                #[cfg(not(test))]
                {
                    return 0;
                }
            }
        };
    let _replay_guard = session.lock_replay();
    let mut replayed = 0usize;
    let mut replayed_bytes: u64 = 0;
    // Seed this connection's deferred-retransmission queue from all unacked
    // QoS 1 and QoS 2 PUBLISH state, and send any QoS 2 PUBRELs immediately
    // (PUBREL does not consume Receive Maximum).
    let pubrels = session.seed_pending_replays();
    for held in pubrels {
        if let Some(routed) = encode_pubrel_out(frame.header.conn_id, held.packet_id) {
            let len = routed.total_frame_len() as u64;
            if shared.conns.route(frame.header.conn_id, routed) {
                replayed_bytes += len;
                replayed += 1;
            }
        }
    }
    let (resumed, resumed_bytes) =
        resume_replay_locked(shared, &session, frame.header.conn_id, expected_conn_id);
    replayed += resumed;
    replayed_bytes += resumed_bytes;
    shared.metrics.inc_publish_sent_by(replayed as u64);
    shared.metrics.inc_delivered_by(replayed as u64);
    shared.metrics.inc_bytes_sent_by(replayed_bytes);
    replayed
}

/// Resume deferred retransmissions first, then drain queued offline messages,
/// both paced by the same connection-scoped MQTT 5 Receive Maximum (covering
/// QoS 1 and QoS 2) and session inflight/spill capacity. Caller must hold
/// `session.lock_replay()`.
fn replay_topic_allowed(shared: &Shared, session: &broker_session::Session, topic: &Topic) -> bool {
    if !session_credential_allows(session, topic.as_str(), false) {
        return false;
    }
    let Ok(filter) = TopicFilter::new(topic.as_str()) else {
        return false;
    };
    let tenant = session.tenant_id.read().clone();
    let verified = if session.verified_is_superuser.load(Ordering::Relaxed) {
        session.verified_username.read().clone()
    } else {
        None
    };
    shared
        .auth
        .authorize_subscribe_in_tenant_as_sync(
            &tenant,
            &session.client_id,
            verified.as_deref(),
            &filter,
        )
        .is_ok()
}

fn resume_replay_locked(
    shared: &Shared,
    session: &Arc<broker_session::Session>,
    conn_id: u64,
    expected_conn_id: Option<u64>,
) -> (usize, u64) {
    let mut replayed = 0usize;
    let mut replayed_bytes = 0u64;
    loop {
        if expected_conn_id.is_some_and(|cid| *session.conn_id.read() != Some(cid)) {
            break;
        }
        let Some(item) = session.pop_next_pending_replay_paced() else {
            break;
        };
        let item_topic = match &item {
            broker_session::PendingReplayItem::Qos1 { entry, .. } => &entry.topic,
            broker_session::PendingReplayItem::Qos2(held) => &held.topic,
        };
        if !replay_topic_allowed(shared, session, item_topic) {
            match &item {
                broker_session::PendingReplayItem::Qos1 { entry, .. } => {
                    let _ = session.ack_inflight(entry.packet_id);
                }
                broker_session::PendingReplayItem::Qos2(held) => {
                    let _ = session.ack_qos2_pubcomp(held.packet_id);
                }
            }
            continue;
        }
        let routed = match &item {
            broker_session::PendingReplayItem::Qos1 { entry, .. } => {
                let topic_str = entry.topic.as_str();
                let mut meta = Vec::with_capacity(2 + topic_str.len() + 2 + 3);
                meta.extend_from_slice(&(topic_str.len() as u16).to_be_bytes());
                meta.extend_from_slice(topic_str.as_bytes());
                meta.extend_from_slice(&entry.packet_id.to_be_bytes());
                meta.push(u8::from(entry.qos));
                meta.push(u8::from(entry.retain));
                meta.push(1u8); // dup: redelivery of an unacked downlink
                BrokerFrame::new(
                    OpCode::PublishOut,
                    conn_id,
                    0,
                    Bytes::from(meta),
                    entry.payload.clone(),
                )
                .ok()
            }
            broker_session::PendingReplayItem::Qos2(held) => encode_publish_out_with_dup(
                conn_id,
                held.topic.as_str(),
                held.packet_id,
                2,
                held.retain,
                true,
                0,
                &held.payload,
            ),
        };
        let Some(routed) = routed else {
            continue;
        };
        if v5_encoded_packet_too_large(session, &routed) {
            shared.metrics.inc_messages_dropped();
            match &item {
                broker_session::PendingReplayItem::Qos1 { entry, .. } => {
                    let _ = session.ack_inflight(entry.packet_id);
                }
                broker_session::PendingReplayItem::Qos2(held) => {
                    let _ = session.ack_qos2_pubcomp(held.packet_id);
                }
            }
            continue;
        }
        let len = routed.total_frame_len() as u64;
        if shared.conns.route(conn_id, routed) {
            if matches!(
                item,
                broker_session::PendingReplayItem::Qos1 { is_spill: true, .. }
            ) {
                shared.metrics.inc_inflight_spill_replayed();
            }
            replayed_bytes += len;
            replayed += 1;
        } else {
            session.push_front_pending_replay(&item);
            break;
        }
    }

    let (ret_replayed, ret_bytes) =
        resume_pending_retained_locked(shared, session, conn_id, expected_conn_id);
    replayed += ret_replayed;
    replayed_bytes += ret_bytes;

    let (queued_replayed, queued_bytes) =
        replay_queued_offline_locked(shared, session, conn_id, expected_conn_id);
    replayed += queued_replayed;
    replayed_bytes += queued_bytes;
    (replayed, replayed_bytes)
}

/// Drain deferred retained matches for the current connection while holding
/// `session.lock_replay()`, preserving `sub_id` and enforcing both MQTT 5
/// Maximum Packet Size and Receive Maximum.
fn resume_pending_retained_locked(
    shared: &Shared,
    session: &Arc<broker_session::Session>,
    conn_id: u64,
    expected_conn_id: Option<u64>,
) -> (usize, u64) {
    let mut replayed = 0usize;
    let mut replayed_bytes = 0u64;
    while let Some(item) = session.pop_next_pending_retained() {
        if expected_conn_id.is_some_and(|cid| *session.conn_id.read() != Some(cid)) {
            session.push_front_pending_retained(item);
            break;
        }
        if !replay_topic_allowed(shared, session, &item.topic) {
            continue;
        }

        let effective = u8::from(item.qos);
        let mut tracked_qos1_spilled = false;
        let downlink_id = if effective == 0 {
            0u16
        } else if effective == 1 {
            let Some((pid, outcome)) = session.try_alloc_qos1_downlink_v5_checked_for_conn(
                expected_conn_id,
                item.topic.clone(),
                QoS::AtLeastOnce,
                true,
                item.payload.clone(),
                std::time::Instant::now(),
            ) else {
                session.push_front_pending_retained(item);
                break;
            };
            tracked_qos1_spilled = outcome == InflightTrackOutcome::Spilled;
            pid
        } else {
            let Some((pid, _)) = session.try_alloc_qos2_outbound_v5_checked_for_conn(
                expected_conn_id,
                item.topic.clone(),
                true,
                item.payload.clone(),
            ) else {
                session.push_front_pending_retained(item);
                break;
            };
            pid
        };
        let Some(routed) = encode_retained_publish_out(
            conn_id,
            &item.topic,
            downlink_id,
            effective,
            item.sub_id,
            item.payload.clone(),
        ) else {
            if effective == 1 {
                let _ = session.ack_inflight(downlink_id);
            } else if effective == 2 {
                let _ = session.ack_qos2_pubcomp(downlink_id);
            }
            continue;
        };
        if v5_encoded_packet_too_large(session, &routed) {
            shared.metrics.inc_messages_dropped();
            if effective == 1 {
                let _ = session.ack_inflight(downlink_id);
            } else if effective == 2 {
                let _ = session.ack_qos2_pubcomp(downlink_id);
            }
            continue;
        }
        let len = routed.total_frame_len() as u64;
        if shared.conns.route(conn_id, routed) {
            if tracked_qos1_spilled {
                shared.metrics.inc_inflight_spilled();
            }
            replayed_bytes += len;
            replayed += 1;
        } else {
            if effective == 1 {
                let _ = session.ack_inflight(downlink_id);
            } else if effective == 2 {
                let _ = session.ack_qos2_pubcomp(downlink_id);
            }
            session.push_front_pending_retained(item);
            break;
        }
    }
    (replayed, replayed_bytes)
}

/// Drain and route queued offline messages for `session` while the caller
/// holds `session.lock_replay()`. Paces QoS 1/2 deliveries by both the
/// client's negotiated MQTT 5 Receive Maximum and the session's inflight/spill
/// capacity, leaving any overflow queued until ACKs free space.
fn replay_queued_offline_locked(
    shared: &Shared,
    session: &Arc<broker_session::Session>,
    conn_id: u64,
    expected_conn_id: Option<u64>,
) -> (usize, u64) {
    let mut replayed = 0usize;
    let mut replayed_bytes = 0u64;
    let mut retained = session.take_offline_retained().into_iter();
    let mut unrouted: Vec<QueuedMessage> = Vec::new();
    while let Some(queued) = retained.next() {
        if expected_conn_id.is_some_and(|cid| *session.conn_id.read() != Some(cid)) {
            unrouted.push(queued);
            unrouted.extend(retained);
            break;
        }
        if !replay_topic_allowed(shared, session, &queued.topic) {
            continue;
        }

        // Atomically check MQTT 5 Receive Maximum and reserve QoS 1/2
        // tracking capacity before sending so neither concurrent live
        // delivery nor a full buffer can oversubscribe the send window.
        let mut tracked_qos1_spilled = false;
        let downlink_id = match queued.qos {
            QoS::AtMostOnce => 0u16,
            QoS::AtLeastOnce => {
                let pid = session.next_packet_id();
                match session.try_reserve_replay_qos1(broker_session::InflightMessage {
                    packet_id: pid,
                    topic: queued.topic.clone(),
                    qos: QoS::AtLeastOnce,
                    retain: queued.retain,
                    payload: queued.payload.clone(),
                    enqueued_at: std::time::Instant::now(),
                    publish_at_ms: None,
                }) {
                    Some(InflightTrackOutcome::Tracked) => pid,
                    Some(InflightTrackOutcome::Spilled) => {
                        tracked_qos1_spilled = true;
                        pid
                    }
                    Some(InflightTrackOutcome::Dropped) | None => {
                        unrouted.push(queued);
                        unrouted.extend(retained);
                        break;
                    }
                }
            }
            QoS::ExactlyOnce => {
                match session.try_alloc_qos2_outbound_v5_checked(
                    queued.topic.clone(),
                    queued.retain,
                    queued.payload.clone(),
                ) {
                    Some((pid, true)) => pid,
                    Some((pid, false)) => {
                        let _ = session.ack_qos2_pubcomp(pid);
                        unrouted.push(queued);
                        unrouted.extend(retained);
                        break;
                    }
                    None => {
                        unrouted.push(queued);
                        unrouted.extend(retained);
                        break;
                    }
                }
            }
        };
        let topic_str = queued.topic.as_str();
        let mut meta = Vec::with_capacity(2 + topic_str.len() + 2 + 3);
        meta.extend_from_slice(&(topic_str.len() as u16).to_be_bytes());
        meta.extend_from_slice(topic_str.as_bytes());
        meta.extend_from_slice(&downlink_id.to_be_bytes());
        meta.push(u8::from(queued.qos));
        meta.push(u8::from(queued.retain));
        meta.push(0u8);
        let routed = match BrokerFrame::new(
            OpCode::PublishOut,
            conn_id,
            0, // stamped per-destination by ConnTable::route
            Bytes::from(meta),
            queued.payload.clone(),
        ) {
            Ok(routed) => routed,
            Err(_) => {
                if queued.qos == QoS::AtLeastOnce && downlink_id != 0 {
                    let _ = session.ack_inflight(downlink_id);
                } else if queued.qos == QoS::ExactlyOnce && downlink_id != 0 {
                    let _ = session.ack_qos2_pubcomp(downlink_id);
                }
                unrouted.push(queued);
                continue;
            }
        };
        if v5_encoded_packet_too_large(session, &routed) {
            shared.metrics.inc_messages_dropped();
            if queued.qos == QoS::AtLeastOnce && downlink_id != 0 {
                let _ = session.ack_inflight(downlink_id);
            } else if queued.qos == QoS::ExactlyOnce && downlink_id != 0 {
                let _ = session.ack_qos2_pubcomp(downlink_id);
            }
            continue;
        }
        let len = routed.total_frame_len() as u64;
        if shared.conns.route(conn_id, routed) {
            if tracked_qos1_spilled {
                shared.metrics.inc_inflight_spilled();
            }
            replayed_bytes += len;
            replayed += 1;
        } else {
            if queued.qos == QoS::AtLeastOnce && downlink_id != 0 {
                let _ = session.ack_inflight(downlink_id);
            } else if queued.qos == QoS::ExactlyOnce && downlink_id != 0 {
                let _ = session.ack_qos2_pubcomp(downlink_id);
            }
            unrouted.push(queued);
        }
    }
    shared
        .sessions
        .confirm_offline_consumed_if_current(session, expected_conn_id, unrouted);
    (replayed, replayed_bytes)
}

/// Route one `PublishIn` frame: an optional `PubAckOut` for the publisher
/// (QoS 1) plus one `PublishOut` per matching subscriber.
///
/// Ingress-only rule execution runs first on the receiving node; the
/// standard router fan-out follows. Delivery QoS is `min(publish QoS,
/// subscription QoS)`; QoS 1 deliveries draw their packet id from the
/// subscriber session so each downstream flow keeps an independent id
/// space. The raw payload bytes are forwarded untouched.
///
/// `$delayed/<secs>/<topic>` targets defer everything except the
/// publisher ack: after the delay the message re-enters through
/// [`ingress_pipeline`] with the inner topic (retained store, rules,
/// fan-out, cluster forward), so delayed delivery observes the same
/// semantics as immediate delivery, just later.
async fn apply_publish(
    frame: &BrokerFrame,
    shared: &Shared,
) -> (Option<BrokerFrame>, Vec<(u64, BrokerFrame)>) {
    shared.metrics.inc_publish_received();
    let (raw_topic_str, packet_id, qos_raw, retain, alias, alias_present, v5_props) =
        match decode_publish_meta(&frame.metadata) {
            Some(parts) => parts,
            None => return (None, Vec::new()),
        };
    // B4-05 inbound topic aliases: resolve the effective topic before any
    // other stage observes it. No alias section on the wire is the fast
    // path (no lock, no table touch). Otherwise the kernel inbound table
    // (written here on the PUBLISH event) maps the alias: a publish
    // carrying both topic and alias registers the mapping, an empty-topic
    // publish resolves it. Any unusable alias — explicit alias 0, an alias
    // above the CONNACK-negotiated maximum, or an empty-topic alias with
    // no prior mapping — is a protocol error (MQTT 5.0 §3.3.2.3.4): the
    // publisher gets reason code 0x94 (Topic Alias Invalid) on QoS 1
    // and QoS 2 (QoS 0 has no ack channel) and the kernel orders a
    // DISCONNECT via `ConnClose` and detaches the connection, so the
    // faulty connection never publishes again.
    //
    // QoS 2 carries 0x94 through the 3-byte PubRecOut meta
    // (`PacketId:16be | RC:8`) built by `qos2_reply_with_reason`; the
    // pre-alias 2-byte form still decodes as reason 0.
    let raw_topic_str = if !alias_present {
        raw_topic_str
    } else {
        match resolve_inbound_alias_topic(shared, frame.header.conn_id, &raw_topic_str, alias) {
            InboundAliasOutcome::Topic(topic) => topic,
            InboundAliasOutcome::Reject => {
                let ack = match qos_raw {
                    1 => pub_ack_reply(frame, packet_id, protocol_v5::REASON_TOPIC_ALIAS_INVALID),
                    2 => qos2_reply_with_reason(
                        OpCode::PubRecOut,
                        frame,
                        packet_id,
                        protocol_v5::REASON_TOPIC_ALIAS_INVALID,
                    ),
                    _ => None,
                };
                order_alias_disconnect(shared, frame.header.conn_id);
                return (ack, Vec::new());
            }
        }
    };
    // B4-04 ordered regex topic rewriting (publish scope): the first
    // matching publish/both rule wins and every stage below (auth,
    // quota, retained store, rule engine, fan-out, cluster forward)
    // observes the rewritten name. `None` delivers unchanged. QoS 2
    // stores the rewritten name, so its second phase routes without a
    // second rewrite. System-prefixed names (`$delayed/` markers,
    // shared-subscription requests) bypass rewriting inside the router,
    // so delayed entries schedule the literal inner topic and delayed
    // markers keep their semantics.
    let topic_str = match shared.router.rewrite_publish(&raw_topic_str) {
        Some(rewritten) => rewritten,
        None => raw_topic_str,
    };
    let topic = match Topic::new(topic_str.clone()) {
        Ok(topic) => topic,
        Err(_) => {
            // A `$delayed/<secs>/<inner>` marker whose inner topic carries
            // wildcards fails the outer concrete-topic check before the
            // delayed path is reached. Count it as a malformed delayed
            // drop (fail closed, never queued) so the counter observes it.
            if strip_delayed_prefix(&topic_str).is_some() {
                shared.delayed.note_malformed();
                warn!("Dropping delayed publish with bad inner topic");
            }
            return (None, Vec::new());
        }
    };
    let qos = match QoS::try_from(qos_raw) {
        Ok(qos) => qos,
        Err(_) => return (None, Vec::new()),
    };
    match qos {
        QoS::AtMostOnce => {
            shared.metrics.inc_qos0_received();
        }
        QoS::AtLeastOnce => {
            shared.metrics.inc_qos1_received();
        }
        QoS::ExactlyOnce => {
            shared.metrics.inc_qos2_received();
        }
    }
    shared.metrics.inc_messages_received();

    // ACL: the publisher is whoever owns this edge connection. Denied
    // publishes die here (QoS 1 still gets its PubAck, stamped 0x87;
    // QoS 2 completes its handshake with PUBREC so the publisher never
    // stalls, but nothing is stored and the later PUBREL routes nothing).
    // Tenant scope (MT-02): the publisher's tenant is read from the same
    // session object the ban check already fetches. Single-tenant fast
    // path: one relaxed atomic load (`tenant_is_default`, set at bind via
    // `Session::set_tenant_id`) borrows the default static with no lock
    // and no per-message allocation, so the BENCH single-tenant load pays
    // no new lock/allocation on publish. Only a non-default tenant takes
    // the short field read plus one bounded copy (at most 128 chars per
    // the MT-01 bound), resolved once, with per-delivery filtering on the
    // subscription's own tag.
    // Publisher session (MT-03): resolve through the connection index
    // so a non-default tenant's publish is attributed to its own
    // session, never to the default-tenant namesake. Falls back to the
    // legacy client-id lookup for connections bound outside the hook.
    // Resolved once per publish, never per delivery: fan-out takes no
    // new lock and no per-message allocation on the single-tenant fast
    // path (one relaxed atomic plus the default static borrow).
    let publisher_session = shared
        .sessions
        .session_for_conn(frame.header.conn_id)
        .or_else(|| {
            shared
                .sessions
                .client_id_for_conn(frame.header.conn_id)
                .as_deref()
                .and_then(|p| shared.sessions.get(p))
        });
    let publisher_opt = publisher_session
        .as_ref()
        .map(|session| session.client_id.clone());
    let tenant_buf: Option<String> = publisher_session.as_ref().and_then(|session| {
        if session
            .tenant_is_default
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            None
        } else {
            let stored = session.tenant_id.read();
            if stored.as_str() != broker_session::tenant::DEFAULT_TENANT_ID {
                Some(stored.clone())
            } else {
                session
                    .tenant_is_default
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                None
            }
        }
    });
    let tenant: &str = tenant_buf
        .as_deref()
        .unwrap_or(broker_session::tenant::DEFAULT_TENANT_ID);
    // A `$delayed/<secs>/<topic>` publish is authorised on the inner
    // target topic: the rules, the databases and the webhook decide on the
    // topic that the message reaches, never on the marker name. The
    // check only runs while delayed delivery is on, as the marker routes
    // as a plain topic otherwise. Ordinary topics take one prefix test and
    // no allocation.
    let delayed_target: Option<Topic> = if shared.delayed.is_enabled() {
        match strip_delayed_prefix(&topic_str) {
            Some((_, inner)) => match Topic::new(inner.to_string()) {
                Ok(inner_topic) => Some(inner_topic),
                Err(e) => {
                    shared.delayed.note_malformed();
                    warn!("Dropping delayed publish with bad inner topic: {}", e);
                    return (None, Vec::new());
                }
            },
            None => None,
        }
    } else {
        None
    };
    if let Some(publisher) = publisher_opt {
        // The authorisation steps below use the target topic.
        let topic = delayed_target.as_ref().unwrap_or(&topic);
        // B1-03: a client banned mid-session stops being served. The ban
        // directory is consulted on every publish authorisation: one read
        // lock that returns after a length check when no bans exist, one
        // short scan otherwise (QoS 1 timings before/after are in the
        // B1-03 report). Denied publishes die exactly like ACL denials.
        // The ban identity comes from the owning session resolved above
        // (tenant-correct for shared client ids), not a second lookup.
        let (username, peerhost) = match publisher_session.as_ref() {
            Some(session) => (
                session.username.read().clone(),
                session.peerhost.read().clone(),
            ),
            None => (None, None),
        };
        if shared.bans.is_banned_in_tenant(
            tenant,
            &publisher,
            username.as_deref(),
            peerhost.as_deref(),
        ) {
            let protocol_version = publisher_session
                .as_ref()
                .map_or(4, |session| session.protocol_version());
            let ack = denied_publish_ack(frame, packet_id, qos, protocol_version, 0x87);
            return (ack, Vec::new());
        }
        // Tenant scope (MT-06): authorisation runs in the publisher's
        // own tenant resolved above, so a rule in one tenant never
        // grants publish in another. One short guard with no new lock
        // and no allocation on the single-tenant fast path. The stored
        // cache policy is consulted first.
        let cached_publish = publisher_session.as_ref().and_then(|session| {
            cached_authz_lookup(shared, session, "publish", topic.as_str(), qos_raw, retain)
        });
        let allowed = match cached_publish {
            Some(allow) => allow,
            None => {
                // Fast superuser flag first (one atomic, no lock).
                // Only the rare superuser bypass clones the name.
                let is_superuser = publisher_session
                    .as_ref()
                    .is_some_and(|session| session.verified_is_superuser.load(Ordering::Relaxed));
                if is_superuser {
                    let verified_username = publisher_session
                        .as_ref()
                        .and_then(|session| session.verified_username.read().clone());
                    shared
                        .auth
                        .authorize_publish_in_tenant_as(
                            tenant,
                            &publisher,
                            verified_username.as_deref(),
                            topic,
                        )
                        .await
                        .is_ok()
                } else {
                    shared
                        .auth
                        .authorize_publish_in_tenant(tenant, &publisher, topic)
                        .await
                        .is_ok()
                }
            }
        };
        let allowed = allowed
            && publisher_session
                .as_ref()
                .is_none_or(|session| session_credential_allows(session, topic.as_str(), true));
        // Per-client decision cache (W2-01): record the publish decision
        // so the management read observes edge publishers. One bounded
        // per-session insert on the publish authorization event only;
        // delivery never touches this lock. A cache hit needs no insert.
        if cached_publish.is_none() {
            if let Some(session) = publisher_session.as_ref() {
                record_authz_under_policy(
                    shared,
                    session,
                    "publish",
                    topic.as_str(),
                    qos_raw,
                    retain,
                    allowed,
                );
            }
        }
        if !allowed {
            // Fire the authz event. Rules belong to the default tenant.
            // Check flag first; publish carries a retain flag.
            if shared
                .engine
                .is_event_source_selected(broker_rules::EventSource::ClientCheckAuthzComplete)
                && is_default_tenant(tenant)
            {
                shared.engine.fire_client_check_authz_complete(
                    broker_rules::ClientCheckAuthzCompleteEvent {
                        client_id: publisher.clone(),
                        username: username.clone(),
                        peerhost: peerhost.clone(),
                        action: "publish".to_string(),
                        topic: topic.to_string(),
                        qos: qos_raw,
                        retain: Some(retain),
                        allow: false,
                        source: "local".to_string(),
                    },
                );
            }
            let protocol_version = publisher_session
                .as_ref()
                .map_or(4, |session| session.protocol_version());
            let ack = denied_publish_ack(frame, packet_id, qos, protocol_version, 0x87);
            // AP-11: a denied publish with the stored `deny_action:
            // disconnect` closes the connection with 0x87 after the ack
            // above is built, so the publisher still observes its ack.
            // The close routes immediately; the ack follows the normal
            // path. Allowed publishes take no branch here.
            if shared.auth.authz_deny_disconnects() {
                order_authz_disconnect(shared, frame.header.conn_id);
            }
            return (ack, Vec::new());
        }
        // B5-03: database ACLs for users with no local record. Local
        // users are decided by the check above; everyone else consults
        // the databases when configured (one `is_some` branch when not).
        // The lookup key is the session username (stamped at CONNECT),
        // falling back to the client id when the session carries none.
        // A database outage fails closed like a local denial. The
        // `has_user` probe is one read lock with no allocation, off the
        // fan-out locks. The database leg is bounded: one short cache lock
        // plus one small key allocation on a hit, one semaphore permit
        // (pool_size bound, default 8) plus one DB round-trip consulting at
        // most MAX_ACL_ROWS rows on a miss. A cached allow skips this leg.
        if cached_publish != Some(true) {
            if let Some(db) = shared.db_auth.as_ref().filter(|d| d.is_configured()) {
                // Tenant scope (MT-06): the local-user probe and the database
                // lookup both run in the publisher's own tenant (deferred:
                // only the built-in table holds per-tenant state).
                let is_local = username
                    .as_deref()
                    .is_some_and(|u| shared.auth.has_user_in_tenant(tenant, u));
                if !is_local {
                    let key = username.as_deref().unwrap_or(&publisher);
                    if db
                        .authorize_publish_in_tenant(tenant, key, topic)
                        .await
                        .is_err()
                    {
                        if shared.engine.is_event_source_selected(
                            broker_rules::EventSource::ClientCheckAuthzComplete,
                        ) && is_default_tenant(tenant)
                        {
                            shared.engine.fire_client_check_authz_complete(
                                broker_rules::ClientCheckAuthzCompleteEvent {
                                    client_id: publisher.clone(),
                                    username: username.clone(),
                                    peerhost: peerhost.clone(),
                                    action: "publish".to_string(),
                                    topic: topic.to_string(),
                                    qos: qos_raw,
                                    retain: Some(retain),
                                    allow: false,
                                    source: "database".to_string(),
                                },
                            );
                        }
                        let protocol_version = publisher_session
                            .as_ref()
                            .map_or(4, |session| session.protocol_version());
                        let ack = denied_publish_ack(frame, packet_id, qos, protocol_version, 0x87);
                        if cached_publish.is_none() {
                            if let Some(session) = publisher_session.as_ref() {
                                record_authz_under_policy(
                                    shared,
                                    session,
                                    "publish",
                                    topic.as_str(),
                                    qos_raw,
                                    retain,
                                    false,
                                );
                            }
                        }
                        return (ack, Vec::new());
                    }
                }
            }
        }
        // B5-04: webhook publish authorization. On a cache hit this is
        // one short lock plus a hash lookup; on a miss it is one bounded
        // HTTP round-trip (pool permit plus request timeout) behind the
        // breaker, off the fan-out locks. Deny or an unreachable, slow
        // or tripped endpoint refuses exactly like an ACL denial.
        // Disabled (`None`, the default) costs one branch. Subscribes
        // stay with the local ACL (see the webhook authorizer).
        // Pipeline hit/miss numbers come from the bench hook at
        // `crates/broker-auth/tests/webhook_publish_bench.rs`
        // (`BENCH publish_per_sec` and `BENCH publish_p99_us`, measured
        // with no webhook configured and with a webhook configured
        // against an in-process server); the in-tree cost probe below
        // (`webhook_publish_cost_hit_and_miss`) asserts only verdicts,
        // never timings.
        // TODO(parity): gateway and cluster-forwarded publishes enter the
        // pipeline past this check, so edge PublishIn frames plus the last
        // will (see `apply_disconnect`) consult the webhook. The rulebook
        // does not decide those paths; gateway (CoAP) and cluster forwards
        // carry no MQTT credential context, so they keep the local ACL
        // rather than inventing a publish context for them.
        // A cached allow skips this leg.
        if cached_publish != Some(true) {
            if let Some(webhook) = shared.webhook.as_ref() {
                // Tenant scope (MT-06): the verdict carries the publisher's
                // own tenant (deferred: only the built-in table holds
                // per-tenant state).
                if webhook
                    .authorize_publish_in_tenant(tenant, &publisher, topic)
                    .await
                    .is_err()
                {
                    if shared.engine.is_event_source_selected(
                        broker_rules::EventSource::ClientCheckAuthzComplete,
                    ) && is_default_tenant(tenant)
                    {
                        shared.engine.fire_client_check_authz_complete(
                            broker_rules::ClientCheckAuthzCompleteEvent {
                                client_id: publisher.clone(),
                                username,
                                peerhost,
                                action: "publish".to_string(),
                                topic: topic.to_string(),
                                qos: qos_raw,
                                retain: Some(retain),
                                allow: false,
                                source: "webhook".to_string(),
                            },
                        );
                    }
                    let protocol_version = publisher_session
                        .as_ref()
                        .map_or(4, |session| session.protocol_version());
                    let ack = denied_publish_ack(frame, packet_id, qos, protocol_version, 0x87);
                    if cached_publish.is_none() {
                        if let Some(session) = publisher_session.as_ref() {
                            record_authz_under_policy(
                                shared,
                                session,
                                "publish",
                                topic.as_str(),
                                qos_raw,
                                retain,
                                false,
                            );
                        }
                    }
                    return (ack, Vec::new());
                }
            }
        }
        // Fire the allowed-publish authz event. Rules belong to the
        // default tenant, so another tenant fires nothing. Check flag
        // before any lock. Source names the highest backend that
        // approved: webhook when configured, else database when the
        // publisher has no local record, else local.
        if shared
            .engine
            .is_event_source_selected(broker_rules::EventSource::ClientCheckAuthzComplete)
            && is_default_tenant(tenant)
        {
            let (username, peerhost) = match publisher_session.as_ref() {
                Some(session) => (
                    session.username.read().clone(),
                    session.peerhost.read().clone(),
                ),
                None => (None, None),
            };
            let source = if shared.webhook.is_some() {
                "webhook"
            } else if shared.db_auth.as_ref().is_some_and(|d| d.is_configured())
                && !username
                    .as_deref()
                    .is_some_and(|u| shared.auth.has_user_in_tenant(tenant, u))
            {
                "database"
            } else {
                "local"
            };
            shared.engine.fire_client_check_authz_complete(
                broker_rules::ClientCheckAuthzCompleteEvent {
                    client_id: publisher.clone(),
                    username,
                    peerhost,
                    action: "publish".to_string(),
                    topic: topic.to_string(),
                    qos: qos_raw,
                    retain: Some(retain),
                    allow: true,
                    source: source.to_string(),
                },
            );
        }
    } else {
        // No session is bound to this connection, so no rule, ban, database
        // or webhook can decide about the publish. Fail closed: nothing is
        // routed or stored, and the connection is closed.
        shared.metrics.inc_messages_dropped();
        order_unbound_close(shared, frame.header.conn_id);
        return (None, Vec::new());
    }

    // Rate policing (INDRA-128): token bucket per publishing client,
    // configured per user. Over quota the message dies here — QoS 0 is
    // dropped and counted, QoS 1 gets its PubAck stamped 0x97, QoS 2
    // gets its PubRec stamped 0x97 and stores nothing.
    if !check_publish_quota(frame, shared).await {
        shared.metrics.inc_messages_dropped();
        shared.metrics.inc_overload_dropped();
        // Fire the drop event. Rules belong to the default tenant.
        // Check flag before any lock.
        if shared
            .engine
            .is_event_source_selected(broker_rules::EventSource::MessageDropped)
            && is_default_tenant(tenant)
        {
            let (client_id, username, peerhost) = match publisher_session.as_ref() {
                Some(session) => (
                    session.client_id.clone(),
                    session.username.read().clone(),
                    session.peerhost.read().clone(),
                ),
                None => (String::new(), None, None),
            };
            shared
                .engine
                .fire_message_dropped(broker_rules::MessageDroppedEvent {
                    client_id,
                    username,
                    peerhost,
                    topic: topic.to_string(),
                    qos: qos_raw,
                    reason: "quota_exceeded".to_string(),
                });
        }
        let protocol_version = publisher_session
            .as_ref()
            .map_or(4, |session| session.protocol_version());
        let ack = denied_publish_ack(frame, packet_id, qos, protocol_version, 0x97);
        return (ack, Vec::new());
    }

    // QoS 2 inbound first phase (D1-01): record the packet id against the
    // publisher session and reply PUBREC. A repeat of the same id before
    // PUBREL is a duplicate: reply PUBREC again without storing or
    // routing a second time. Nothing routes until PUBREL arrives.
    if qos == QoS::ExactlyOnce {
        return apply_qos2_publish(frame, shared, &topic_str, topic, packet_id, retain).await;
    }

    // Delayed delivery: ack now (the publisher must not stall), defer
    // everything else past the timer. Gated on the runtime switch: one
    // relaxed load; while off the marker routes as a normal topic and
    // nothing new is queued.
    if shared.delayed.is_enabled() {
        if let Some((delay_secs, inner)) = strip_delayed_prefix(&topic_str) {
            return apply_delayed_publish(
                frame, shared, delay_secs, inner, qos, retain, packet_id, tenant,
            )
            .await;
        }
    }

    // B4-06 fail-closed: snapshot the durable offline persist-failure
    // counter so a detached-queue write that fails inside the pipeline
    // below withholds the QoS 1 ack (publisher retries) instead of acking
    // a non-durable write. The store counts every append/rewrite failure;
    // a rise across the pipeline means this publish lost at least one
    // detached durable copy, which was also dropped from memory (see
    // `push_offline_with_limit`). Over-withholding under concurrency (a
    // racing publish failed instead) only retries, never loses.
    // TODO(parity): should the withheld ack carry a 5.x error code instead
    // of no ack at all? The rulebook does not decide the wire shape; no
    // ack (publisher retries) is the conservative choice.
    let offline_persist_failed_before = shared.sessions.offline_store().map(|s| s.persist_failed());

    // B4-02: resolve the publisher once for the shared `hash_clientid`
    // strategy. `None` (unknown session) hashes the empty string in the
    // router; delivery still proceeds to exactly one member. The fan-out
    // stays inside the publisher's tenant (MT-02), resolved once above.
    // The hashing id comes from the owning session (tenant-correct for
    // shared client ids), not a second connection lookup.
    let publisher = publisher_session
        .as_ref()
        .map(|session| session.client_id.clone());
    let deliveries = ingress_pipeline_with_publisher_and_props(
        shared,
        &topic,
        qos,
        retain,
        &frame.payload,
        publisher.as_deref(),
        tenant,
        &v5_props,
    )
    .await;
    forward_cluster(shared, &topic, qos, &frame.payload, tenant).await;

    let ack = if qos == QoS::AtLeastOnce {
        let persist_failed = offline_persist_failed_before.is_some_and(|before| {
            shared
                .sessions
                .offline_store()
                .map(|s| s.persist_failed() > before)
                .unwrap_or(false)
        });
        if persist_failed {
            // The PUBACK is deferred (withheld) here so the publisher
            // retries: count it where the event happens.
            shared.metrics.inc_puback_deferred();
            warn!("Offline queue persist failed: withholding QoS 1 ack so the publisher retries");
            None
        } else {
            pub_ack_reply(frame, packet_id, 0)
        }
    } else {
        None
    };

    (ack, deliveries)
}

/// Handle a `$delayed/<secs>/<topic>` publish: immediate ack, deferred
/// pipeline through the shared timer wheel. An unparseable inner topic,
/// an over-limit delay, an oversized payload or a full backlog drops the
/// message with a warning and a counter (it could never be delivered
/// correctly). The entry is recorded in the wheel and in
/// `<data-dir>/delayed.jsonl` (see `broker-delayed`) before the ack is
/// returned, so the publisher is never stalled past the timer and a
/// restart keeps the entry on schedule.
#[allow(clippy::too_many_arguments)]
async fn apply_delayed_publish(
    frame: &BrokerFrame,
    shared: &Shared,
    delay_secs: u64,
    inner: &str,
    qos: QoS,
    retain: bool,
    packet_id: u16,
    publisher_tenant: &str,
) -> (Option<BrokerFrame>, Vec<(u64, BrokerFrame)>) {
    let inner_topic = match Topic::new(inner.to_string()) {
        Ok(topic) => topic,
        Err(e) => {
            shared.delayed.note_malformed();
            warn!("Dropping delayed publish with bad inner topic: {}", e);
            return (None, Vec::new());
        }
    };
    if delay_secs == 0 {
        // B4-06 fail-closed: same persist-failed snapshot/withhold as the
        // immediate QoS 1 path below. The deferred pipeline can buffer for
        // detached durable sessions; acking before it would confirm a
        // non-durable write.
        let offline_persist_failed_before =
            shared.sessions.offline_store().map(|s| s.persist_failed());
        // B4-02: delayed delivery keeps the publisher when the session
        // still exists so `hash_clientid` stays stable across the defer.
        // The deferred fan-out stays inside the publisher's tenant (MT-02).
        let publisher = shared.sessions.client_id_for_conn(frame.header.conn_id);
        let deliveries = ingress_pipeline_with_publisher(
            shared,
            &inner_topic,
            qos,
            retain,
            &frame.payload,
            publisher.as_deref(),
            publisher_tenant,
        )
        .await;
        forward_cluster(shared, &inner_topic, qos, &frame.payload, publisher_tenant).await;
        let ack = if qos == QoS::AtLeastOnce {
            let persist_failed = offline_persist_failed_before.is_some_and(|before| {
                shared
                    .sessions
                    .offline_store()
                    .map(|s| s.persist_failed() > before)
                    .unwrap_or(false)
            });
            if persist_failed {
                // Same deferred-PUBACK count as the immediate path: the
                // ack is withheld so the publisher retries.
                shared.metrics.inc_puback_deferred();
                warn!(
                    "Offline queue persist failed: withholding delayed QoS 1 ack so the publisher retries"
                );
                None
            } else {
                pub_ack_reply(frame, packet_id, 0)
            }
        } else {
            None
        };
        return (ack, deliveries);
    }
    // TODO(parity): a backlog-full, oversized or over-limit drop returns
    // no ack, so a QoS 1 publisher retries and may worsen the overload.
    // The alternative (ack a message that was never queued) lies about
    // acceptance. The rulebook does not decide this case; current choice
    // fails closed.
    let max_secs = shared.delayed.max_secs();
    if delay_secs > max_secs {
        warn!(
            "Dropping delayed publish: {}s exceeds the {}s cap",
            delay_secs, max_secs
        );
    }
    let now_ms = broker_storage::now_ms();
    let entry = DelayedEntry {
        id: shared.delayed.next_id(),
        deliver_at_ms: now_ms.saturating_add(delay_secs.saturating_mul(1_000)),
        publish_at_ms: now_ms,
        topic: inner.to_string(),
        qos: qos as u8,
        retain,
        payload: frame.payload.to_vec(),
        publisher: shared.sessions.client_id_for_conn(frame.header.conn_id),
        // The list shows the name the session carried at schedule
        // time; anonymous sessions read as unknown, never as another
        // user. One short lock, delayed path only.
        username: shared
            .sessions
            .session_for_conn(frame.header.conn_id)
            .or_else(|| {
                shared
                    .sessions
                    .client_id_for_conn(frame.header.conn_id)
                    .as_deref()
                    .and_then(|id| shared.sessions.get(id))
            })
            .and_then(|session| session.username.read().clone()),
        // The publisher's tenant rides into the deferred pipeline (MT-02):
        // fan-out after the timer stays inside the same tenant.
        publisher_tenant: publisher_tenant.to_string(),
    };
    match shared.delayed.persist_and_schedule(entry, delay_secs) {
        Ok(()) => {
            let ack = if qos == QoS::AtLeastOnce {
                pub_ack_reply(frame, packet_id, 0)
            } else {
                None
            };
            (ack, Vec::new())
        }
        Err(e) => {
            // The scheduler already bumped the matching drop counter
            // (over-limit, oversized, backlog-full or file error); warn
            // here so the drop is visible in the log like before.
            if delay_secs <= max_secs {
                warn!("Dropping delayed publish: {:?}", e);
            }
            (None, Vec::new())
        }
    }
}

/// QoS 2 inbound first phase (D1-01): store the publish against the
/// publisher session and reply PUBREC. Duplicates (same packet id held)
/// reply PUBREC without storing or routing again. A zero packet id or a
/// missing session yields no reply; a full inbound window yields no
/// reply so the publisher retries. Nothing routes here: routing waits
/// for PUBREL.
async fn apply_qos2_publish(
    frame: &BrokerFrame,
    shared: &Shared,
    topic_str: &str,
    topic: Topic,
    packet_id: u16,
    retain: bool,
) -> (Option<BrokerFrame>, Vec<(u64, BrokerFrame)>) {
    if packet_id == 0 {
        return (None, Vec::new());
    }
    // Owner session first (MT-03), legacy lookup behind, so QoS 2
    // inbound state stays per `(tenant, client id)`.
    let session = match shared
        .sessions
        .session_for_conn(frame.header.conn_id)
        .or_else(|| {
            shared
                .sessions
                .client_id_for_conn(frame.header.conn_id)
                .as_deref()
                .and_then(|p| shared.sessions.get(p))
        }) {
        Some(session) => session,
        None => return (None, Vec::new()),
    };
    if session.has_qos2_inbound(packet_id) {
        return (qos2_reply(OpCode::PubRecOut, frame, packet_id), Vec::new());
    }
    let stored = session.store_qos2_inbound(
        packet_id,
        Qos2InboundEntry {
            topic,
            retain,
            payload: frame.payload.clone(),
        },
    );
    if !stored {
        // Full window (not a duplicate, checked above): no PUBREC so the
        // publisher retries instead of wedging the session.
        return (None, Vec::new());
    }
    let _ = topic_str;
    (qos2_reply(OpCode::PubRecOut, frame, packet_id), Vec::new())
}

/// QoS 2 inbound second phase (D1-01): on PUBREL route the stored message
/// exactly once, reply PUBCOMP and release the packet id. Unknown packet
/// ids reply PUBCOMP with no routing so a retried PUBREL never stalls.
async fn apply_qos2_pubrel(
    frame: &BrokerFrame,
    shared: &Shared,
) -> (Option<BrokerFrame>, Vec<(u64, BrokerFrame)>) {
    let packet_id = match decode_qos2_meta(&frame.metadata) {
        Some(packet_id) => packet_id,
        None => return (None, Vec::new()),
    };
    // Owner session first (MT-03), legacy lookup behind, so the PUBREL
    // routes the owning tenant's stored message exactly once.
    let session = match shared
        .sessions
        .session_for_conn(frame.header.conn_id)
        .or_else(|| {
            shared
                .sessions
                .client_id_for_conn(frame.header.conn_id)
                .as_deref()
                .and_then(|p| shared.sessions.get(p))
        }) {
        Some(session) => session,
        None => return (qos2_reply(OpCode::PubCompOut, frame, packet_id), Vec::new()),
    };
    // Publisher identity for shared-strategy hashing and delayed
    // entries comes from the owning session (tenant-correct for shared
    // client ids), not a second connection lookup.
    let client_id = session.client_id.clone();
    // QoS 2 routes on PUBREL inside the publisher's tenant (MT-02):
    // the tenant rides from the owning session into the pipeline below.
    let publisher_tenant = session.tenant_id.read().clone();
    let stored = match session.take_qos2_inbound(packet_id) {
        Some(stored) => stored,
        None => return (qos2_reply(OpCode::PubCompOut, frame, packet_id), Vec::new()),
    };
    // B4-06 fail-closed: snapshot the durable offline persist-failure
    // counter so a detached-queue write that fails inside the pipeline
    // below withholds the PUBCOMP (the publisher retries PUBREL) instead
    // of confirming a non-durable write. Mirrors the QoS 1 ack withhold;
    // over-withholding under concurrency only retries, never loses.
    let offline_persist_failed_before = shared.sessions.offline_store().map(|s| s.persist_failed());
    // Delayed targets defer everything except the PUBCOMP: the publisher
    // must not stall past the timer. The entry is recorded on the shared
    // wheel and in `<data-dir>/delayed.jsonl` before the PUBCOMP is
    // returned; the driver delivers it through the normal pipeline.
    // Gated on the runtime switch like the QoS 0/1 branch above.
    if shared.delayed.is_enabled() {
        if let Some((delay_secs, inner)) = strip_delayed_prefix(stored.topic.as_str()) {
            let comp = qos2_reply(OpCode::PubCompOut, frame, packet_id);
            let Ok(inner_topic) = Topic::new(inner.to_string()) else {
                shared.delayed.note_malformed();
                warn!("Dropping delayed QoS 2 publish with bad inner topic");
                return (comp, Vec::new());
            };
            if delay_secs == 0 {
                // B4-02: the QoS 2 publisher id is the PUBREL sender, so the
                // deferred fan-out hashes the same key as immediate delivery.
                let deliveries = ingress_pipeline_with_publisher(
                    shared,
                    &inner_topic,
                    QoS::ExactlyOnce,
                    stored.retain,
                    &stored.payload,
                    Some(client_id.as_str()),
                    &publisher_tenant,
                )
                .await;
                forward_cluster(
                    shared,
                    &inner_topic,
                    QoS::ExactlyOnce,
                    &stored.payload,
                    &publisher_tenant,
                )
                .await;
                let persist_failed = offline_persist_failed_before.is_some_and(|before| {
                    shared
                        .sessions
                        .offline_store()
                        .map(|s| s.persist_failed() > before)
                        .unwrap_or(false)
                });
                if persist_failed {
                    warn!(
                        "Offline queue persist failed: withholding QoS 2 PUBCOMP so the publisher retries"
                    );
                    // Restore the taken entry so the retried PUBREL re-routes
                    // instead of hitting the unknown-id fast path. The
                    // original delayed topic is kept so the retry defers again.
                    // Cloned (failure path only): avoids moving `stored` while
                    // the delayed prefix borrow is live.
                    let _ = session.store_qos2_inbound(
                        packet_id,
                        Qos2InboundEntry {
                            topic: stored.topic.clone(),
                            retain: stored.retain,
                            payload: stored.payload.clone(),
                        },
                    );
                    return (None, deliveries);
                }
                return (comp, deliveries);
            }
            let max_secs = shared.delayed.max_secs();
            if delay_secs > max_secs {
                warn!(
                    "Dropping delayed QoS 2 publish: {}s exceeds the {}s cap",
                    delay_secs, max_secs
                );
            }
            let now_ms = broker_storage::now_ms();
            let entry = DelayedEntry {
                id: shared.delayed.next_id(),
                deliver_at_ms: now_ms.saturating_add(delay_secs.saturating_mul(1_000)),
                publish_at_ms: now_ms,
                topic: inner.to_string(),
                qos: QoS::ExactlyOnce as u8,
                retain: stored.retain,
                payload: stored.payload.to_vec(),
                publisher: Some(client_id.clone()),
                // The list shows the name the owning session carried; the
                // QoS 2 phase already holds the session, so no new lookup.
                username: session.username.read().clone(),
                // The publisher's tenant rides into the deferred pipeline:
                // fan-out after the timer stays inside the same tenant.
                publisher_tenant: publisher_tenant.clone(),
            };
            if let Err(e) = shared.delayed.persist_and_schedule(entry, delay_secs) {
                // The scheduler already bumped the matching drop counter;
                // warn here so the drop is visible in the log like before.
                if delay_secs <= max_secs {
                    warn!("Dropping delayed QoS 2 publish: {:?}", e);
                }
            }
            return (comp, Vec::new());
        }
    }
    // B4-02: QoS 2 second phase hashes the original publisher id.
    let deliveries = ingress_pipeline_with_publisher(
        shared,
        &stored.topic,
        QoS::ExactlyOnce,
        stored.retain,
        &stored.payload,
        Some(client_id.as_str()),
        &publisher_tenant,
    )
    .await;
    forward_cluster(
        shared,
        &stored.topic,
        QoS::ExactlyOnce,
        &stored.payload,
        &publisher_tenant,
    )
    .await;
    let persist_failed = offline_persist_failed_before.is_some_and(|before| {
        shared
            .sessions
            .offline_store()
            .map(|s| s.persist_failed() > before)
            .unwrap_or(false)
    });
    if persist_failed {
        warn!("Offline queue persist failed: withholding QoS 2 PUBCOMP so the publisher retries");
        // Restore the taken entry so the retried PUBREL re-routes instead
        // of hitting the unknown-id fast path.
        let _ = session.store_qos2_inbound(packet_id, stored);
        return (None, deliveries);
    }
    (qos2_reply(OpCode::PubCompOut, frame, packet_id), deliveries)
}

/// QoS 2 outbound second phase (D1-01): on the subscriber's PUBREC send
/// PUBREL and retain only the packet id until PUBCOMP. A duplicate
/// PUBREC for an entry already waiting for PUBCOMP resends PUBREL (the
/// PUBREL was lost). Unknown ids are ignored.
fn maybe_resume_replay_on_ack(
    shared: &Shared,
    session: &Arc<broker_session::Session>,
    conn_id: u64,
) {
    if !session.has_pending_replay_or_offline() {
        return;
    }
    let _replay_guard = session.lock_replay();
    let expected_conn_id = (*session.conn_id.read()).map(|_| conn_id);
    let (resumed, resumed_bytes) = resume_replay_locked(shared, session, conn_id, expected_conn_id);
    if resumed > 0 {
        shared.metrics.inc_publish_sent_by(resumed as u64);
        shared.metrics.inc_delivered_by(resumed as u64);
        shared.metrics.inc_bytes_sent_by(resumed_bytes);
    }
}

fn apply_qos2_pubrec(frame: &BrokerFrame, shared: &Shared) -> Option<BrokerFrame> {
    let (packet_id, reason_code) = decode_qos2_meta_with_reason(&frame.metadata)?;
    // Owner session first (MT-03), legacy lookup behind.
    let session = shared
        .sessions
        .session_for_conn(frame.header.conn_id)
        .or_else(|| {
            shared
                .sessions
                .client_id_for_conn(frame.header.conn_id)
                .as_deref()
                .and_then(|p| shared.sessions.get(p))
        })?;
    if reason_code >= 0x80 {
        if session.ack_qos2_pubcomp(packet_id) {
            maybe_resume_replay_on_ack(shared, &session, frame.header.conn_id);
        }
        return None;
    }
    if session.complete_qos2_pubrec(packet_id) {
        return qos2_reply(OpCode::PubRelOut, frame, packet_id);
    }
    if session
        .qos2_outbound_snapshot()
        .iter()
        .any(|m| m.packet_id == packet_id)
    {
        return qos2_reply(OpCode::PubRelOut, frame, packet_id);
    }
    None
}

/// QoS 2 outbound completion (D1-01): on the subscriber's PUBCOMP release
/// the packet id. Unknown ids are ignored: PUBCOMPs from a previous
/// incarnation must never disturb live state.
fn apply_qos2_pubcomp(frame: &BrokerFrame, shared: &Shared) {
    let packet_id = match decode_qos2_meta(&frame.metadata) {
        Some(packet_id) => packet_id,
        None => return,
    };
    // Owner session first (MT-03), legacy lookup behind: a PUBCOMP
    // releases only the owning tenant's downlink id.
    let session = shared
        .sessions
        .session_for_conn(frame.header.conn_id)
        .or_else(|| {
            shared
                .sessions
                .client_id_for_conn(frame.header.conn_id)
                .as_deref()
                .and_then(|p| shared.sessions.get(p))
        });
    if let Some(session) = session {
        if session.ack_qos2_pubcomp(packet_id) {
            maybe_resume_replay_on_ack(shared, &session, frame.header.conn_id);
        }
    }
}

/// Build one QoS 2 acknowledgement (`PubRecOut`, `PubRelOut`,
/// `PubCompOut`) mirroring the request identity. Meta is the 2-byte
/// packet id; a zero id yields no reply.
fn qos2_reply(opcode: OpCode, frame: &BrokerFrame, packet_id: u16) -> Option<BrokerFrame> {
    qos2_reply_with_reason(opcode, frame, packet_id, 0)
}

/// Build one QoS 2 acknowledgement carrying a reason code (B4-05, T-92:
/// a rejected alias answers PUBREC 0x94 Topic Alias Invalid). Layout is
/// `PacketId:16be | RC:8`; `RC 0` is success. The edge accepts both the
/// 2-byte (pre-alias, RC 0) and 3-byte forms. A zero packet id yields no
/// reply.
fn qos2_reply_with_reason(
    opcode: OpCode,
    frame: &BrokerFrame,
    packet_id: u16,
    reason_code: u8,
) -> Option<BrokerFrame> {
    if packet_id == 0 {
        return None;
    }
    let mut meta = Vec::with_capacity(3);
    meta.extend_from_slice(&packet_id.to_be_bytes());
    if opcode == OpCode::PubRecOut || reason_code != 0 {
        meta.push(reason_code);
    }
    BrokerFrame::new(
        opcode,
        frame.header.conn_id,
        frame.header.sequence_no,
        Bytes::from(meta),
        Bytes::new(),
    )
    .ok()
}

/// Decode QoS 2 acknowledgement metadata into the packet id. Layout is
/// `PacketId:16be` optionally followed by `RC:8` (B4-05 PUBREC 0x94;
/// pre-alias 2-byte frames decode as RC 0). The id must be nonzero;
/// further trailing bytes are tolerated.
fn decode_qos2_meta(meta: &[u8]) -> Option<u16> {
    decode_qos2_meta_with_reason(meta).map(|(packet_id, _)| packet_id)
}

/// Decode QoS 2 acknowledgement metadata into `(packet_id, reason_code)`.
/// Accepts the 2-byte pre-alias form (reason 0) and the 3-byte B4-05 form
/// (`PacketId:16be | RC:8`, used by PUBREC 0x94 rejects).
fn decode_qos2_meta_with_reason(meta: &[u8]) -> Option<(u16, u8)> {
    if meta.len() < 2 {
        return None;
    }
    let packet_id = u16::from_be_bytes([meta[0], meta[1]]);
    if packet_id == 0 {
        return None;
    }
    let reason_code = if meta.len() >= 3 { meta[2] } else { 0 };
    Some((packet_id, reason_code))
}

/// The shared ingress pipeline: retained store/clear, rule execution,
/// and local fan-out. Used by immediate delivery and, after its timer,
/// by delayed delivery alike. Default-tenant entry; publish paths that
/// know the publisher call [`ingress_pipeline_with_publisher`] with the
/// publisher's tenant instead.
async fn ingress_pipeline(
    shared: &Shared,
    topic: &Topic,
    qos: QoS,
    retain: bool,
    payload: &Bytes,
    tenant: &str,
) -> Vec<(u64, BrokerFrame)> {
    ingress_pipeline_with_publisher(shared, topic, qos, retain, payload, None, tenant).await
}

/// [`ingress_pipeline`] with publisher context for shared strategies
/// (B4-02) and tenant scope (MT-02). The MQTT ingress path passes the
/// publishing client id plus its tenant; all other ingress paths
/// (delayed timers without a live session, QoS 2 second phase without a
/// stored publisher, CoAP gateway, rule and management publishes) pass
/// `None` plus the tenant they fan out in (default unless the caller
/// knows better). Retained reads/writes and fan-out stay inside
/// `publisher_tenant`: the same topic string in two tenants is two
/// independent spaces, and a mismatch delivers nothing, counted, never
/// an error to the publisher.
async fn ingress_pipeline_with_publisher(
    shared: &Shared,
    topic: &Topic,
    qos: QoS,
    retain: bool,
    payload: &Bytes,
    publisher: Option<&str>,
    publisher_tenant: &str,
) -> Vec<(u64, BrokerFrame)> {
    let empty = PublishV5Props::default();
    ingress_pipeline_with_publisher_and_props(
        shared,
        topic,
        qos,
        retain,
        payload,
        publisher,
        publisher_tenant,
        &empty,
    )
    .await
}

/// [`ingress_pipeline_with_publisher`] with X1-03 v5 properties.
/// The MQTT ingress path passes the parsed inbound properties so
/// version-5 deliveries observe what the publisher sent; all other
/// ingress paths pass the default (no properties).
#[allow(clippy::too_many_arguments)]
async fn ingress_pipeline_with_publisher_and_props(
    shared: &Shared,
    topic: &Topic,
    qos: QoS,
    retain: bool,
    payload: &Bytes,
    publisher: Option<&str>,
    publisher_tenant: &str,
    v5: &PublishV5Props,
) -> Vec<(u64, BrokerFrame)> {
    // Message transformations (W3-32) run first on ingress, before
    // retained writes and rule dispatch, so retained state and rules
    // observe the transformed message. Order assumption: transforms,
    // then retained, then rules, then fan-out. A shed message (`drop`
    // or `disconnect` failure behaviour) stops here: no retain, no
    // rules, no fan-out. Per-message cost: one pointer load plus one
    // length check when the chain is empty, without a reference-count
    // write and without cloning topic or payload; matching steps add one filter test per filter plus one
    // template render per operation. Per-transform metrics (W3-36)
    // add one `Arc` clone of the metric map plus lookups and bumps.
    // TODO(parity): is transforms-before-retained-and-rules the
    // documented order? The rulebook does not decide; the current
    // choice applies transforms first until the checker pins it.
    let transformed: Option<(Topic, Bytes)> = if !shared.engine.has_transforms() {
        None
    } else {
        let snapshot = shared.engine.transform_snapshot();
        let metric_snapshot = shared.engine.transform_metrics_snapshot();
        let (work_topic, work_payload, shed) = broker_rules::apply_transform_steps_with_metrics(
            &snapshot,
            &metric_snapshot,
            topic,
            payload,
        );
        if shed {
            return Vec::new();
        }
        Some((work_topic, work_payload))
    };
    let (topic, payload): (&Topic, &Bytes) = match transformed.as_ref() {
        Some((work_topic, work_payload)) => (work_topic, work_payload),
        None => (topic, payload),
    };
    // Retained state tracks transformed ingress: store (or clear on empty
    // payload) before rules and fan-out observe the message. Enforces
    // the same configurable limits as the management publish path
    // (W1-28): oversized payloads past max_payload_size are delivered
    // but not stored, and new topics past backend.max_retained_messages
    // (when non-zero) are dropped while replacements still land. The
    // hard cap in broker_storage::MAX_RETAINED_MESSAGES always applies
    // inside the store. Retained-only work; non-retained fan-out and
    // fan-in never take the config lock. Scoped to the publisher's
    // tenant: retained state never crosses tenants.
    // The writer holds the retained store and the limits it needs, so a
    // rule republish with the retain flag takes the same path.
    // Keep retained-store mutation and subscriber selection in one short
    // critical section, releasing the gate before invoking rule actions
    // (which may republish with `retain = true` and re-acquire this gate).
    let deliveries = {
        let _retained_pub_gate = if retain {
            let gate = shared.retained_subscribe_gate.write().await;
            shared
                .retain_writer
                .apply(topic, qos, payload, publisher_tenant)
                .await;
            Some(gate)
        } else {
            None
        };
        build_downlink_frames_with_publisher_and_props(
            &shared.router,
            &shared.sessions,
            &shared.metrics,
            topic,
            qos,
            retain,
            payload,
            publisher,
            publisher_tenant,
            v5,
            Some(&shared.engine),
        )
    };

    // Rules execute at ingress on this node, after releasing the retained gate.
    // Republished output re-enters through the sink only, so rules can
    // never recurse through their own output.
    //
    // Each rule serves one tenant. Only rules of the publisher tenant
    // run. Each output fans out in the same tenant. A default-only
    // setup runs as before.
    let rules_fired = shared
        .engine
        .dispatch_ingress_message(publisher_tenant, topic, payload, qos, retain, &shared.sink)
        .await;
    shared.metrics.inc_rules_executed_by(rules_fired as u64);

    // Topic index (W1-15): remember the concrete publish topic so the
    // management list/detail reads observe edge publishes as well as
    // management publishes. Bounded short-lock insert shared with the
    // API layer; delivery proceeds even when the index is full.
    // Management-plane index only, no work on fan-out or fan-in.
    shared.router.record_topic(topic.as_str());

    // Durable stream journal (B1-07): enqueue without blocking so the
    // publish path never waits for disk. Disabled (`None`) costs one
    // branch; enabled costs one bounded `try_send`, with drops counted.
    // The background task fsyncs per record; the window is channel delay
    // plus one fsync.
    maybe_journal_stream(shared, topic, qos, payload);

    // Trace capture (F1-04, T-74): append one bounded line per matching
    // session. Disabled (the default) costs one atomic load; enabled costs
    // one session lookup plus one short store read lock, with allocation
    // only for matching sessions.
    maybe_capture_trace(shared, publisher, topic, payload);

    // Agent collects (MC-04): hand accepted ingress to running collects
    // in the publisher tenant. Idle publishes pay one atomic load
    // inside `feed`; feeds clone only for matching collects.
    shared
        .collectors
        .feed(publisher_tenant, topic, qos, retain, payload);
    // CoAP observe fan-out (B1-04): notify bounded observers off the
    // hot path. Empty when the gateway never ran: one read lock that
    // returns after a length check, no spawn, no socket work.
    // Observers read the default tenant only. A message of another
    // tenant never reaches them (fail closed, TM-05 owns gateways).
    if is_default_tenant(publisher_tenant) {
        maybe_notify_coap_observers(shared, topic.as_str(), payload);
    }
    deliveries
}

/// Returns true for the default tenant. An empty id is also the default
/// tenant.
fn is_default_tenant(tenant: &str) -> bool {
    tenant.is_empty() || tenant == broker_session::tenant::DEFAULT_TENANT_ID
}

/// Clustered mode: one forward per matching remote node; each remote
/// fans out locally.
async fn forward_cluster(
    shared: &Shared,
    topic: &Topic,
    qos: QoS,
    payload: &Bytes,
    publisher_tenant: &str,
) {
    // The cluster protocol does not contain the tenant. A peer delivers
    // a forwarded message in the default tenant. Thus this node forwards
    // only the messages of the default tenant. A message of a different
    // tenant stays on this node.
    if !is_default_tenant(publisher_tenant) {
        return;
    }
    if let Some(cluster) = &shared.cluster {
        match cluster.resolve_route(topic).await {
            Ok(targets) => {
                for target in targets {
                    if let Err(e) = cluster.forward_message(&target, topic, payload, qos).await {
                        warn!("Cluster forward to {} failed: {}", target, e);
                    }
                }
            }
            Err(e) => {
                warn!("Cluster route resolution failed: {}", e);
            }
        }
    }
}

/// Identity the CoAP gateway uses at authorization.
///
/// CoAP carries no client credential. The gateway checks this fixed id.
const COAP_GATEWAY_CLIENT_ID: &str = "coap-gateway";

/// CoAP gateway ingress and observe fan-out (B1-04).
///
/// A CoAP POST/PUT to `/ps/<topic>` enters here after URI validation:
/// the payload is stored retained (CoAP has no retain flag; keeping the
/// last value preserves the old gateway's GET semantics on the real
/// retained store) and fanned out through the same [`ingress_pipeline`]
/// an MQTT publish takes, so ordinary MQTT subscribers receive it.
/// Cluster forwarding matches the MQTT path. QoS is `AtMostOnce`:
/// CoAP confirms at its own layer (CHANGED/CONTENT), so the kernel owes
/// no MQTT packet-id tracking for gateway ingress.
///
/// Publish-path cost mirrors the ban check (B1-03): when no observer is
/// registered the notify hook below takes one read lock and returns
/// after a length check. When observers exist the hook snapshots at
/// most 32 entries for this topic and spawns the UDP sends, so fan-out
/// never blocks on the network.
///
/// Callers without a publisher id:
/// - `apply_publish` with no session denies and closes (client path).
/// - `apply_coap_publish` checks the gateway id here (client path).
/// - `deliver_delayed_entry` replays a checked entry (trusted source).
/// - QoS 2 second phase uses the stored publisher (trusted source).
/// - Last will checks before ingress (trusted source).
/// - Cluster inbox fans out checked ingress (trusted source).
async fn apply_coap_publish(
    shared: &Shared,
    topic_str: &str,
    payload: &Bytes,
) -> Option<Vec<(u64, BrokerFrame)>> {
    let topic = Topic::new(topic_str.to_string()).ok()?;
    // Authorize the gateway publish before it stores or routes.
    // The gateway serves the default tenant only.
    // A denial stores nothing and routes nothing.
    let tenant = broker_session::tenant::DEFAULT_TENANT_ID;
    if shared
        .bans
        .is_banned_in_tenant(tenant, COAP_GATEWAY_CLIENT_ID, None, None)
    {
        warn!("Dropping CoAP publish: gateway identity is banned");
        shared.metrics.inc_messages_dropped();
        return None;
    }
    if !authorize_coap_publish(shared, &topic).await {
        warn!("Dropping CoAP publish: access rules deny the topic");
        shared.metrics.inc_messages_dropped();
        return None;
    }
    shared.metrics.inc_publish_received();
    shared.metrics.inc_qos0_received();
    shared.metrics.inc_messages_received();
    let deliveries = ingress_pipeline(
        shared,
        &topic,
        QoS::AtMostOnce,
        true,
        payload,
        broker_session::tenant::DEFAULT_TENANT_ID,
    )
    .await;
    forward_cluster(
        shared,
        &topic,
        QoS::AtMostOnce,
        payload,
        broker_session::tenant::DEFAULT_TENANT_ID,
    )
    .await;
    Some(deliveries)
}

/// Snapshot this topic's observers and send one NON notification each,
/// off the hot path.
///
/// No-op when the gateway never ran (`coap_socket` is `None`) or when
/// nobody observes anything (`has_observers` false): a single length
/// check, no allocation. Otherwise clones at most
/// `MAX_OBSERVERS_PER_TOPIC` small entries and spawns one task that owns
/// its socket clone, payload and observer list; the publish path never
/// awaits the network.
fn maybe_notify_coap_observers(shared: &Shared, topic_str: &str, payload: &Bytes) {
    if !shared.coap.has_observers() {
        return;
    }
    let observers = shared.coap.observers_for(topic_str);
    if observers.is_empty() {
        return;
    }
    let Some(socket) = shared.coap_socket.clone() else {
        return;
    };
    let handler = shared.coap.clone();
    let payload = payload.clone();
    tokio::spawn(async move {
        let seq = handler.next_observe_seq();
        for observer in &observers {
            let notify = handler.build_notify(observer, payload.clone(), seq);
            let bytes = notify.encode();
            let _ = socket.send_to(&bytes, observer.peer).await;
        }
    });
}

async fn authorize_coap_publish(shared: &Shared, topic: &Topic) -> bool {
    let tenant = broker_session::tenant::DEFAULT_TENANT_ID;
    if shared
        .auth
        .authorize_publish_in_tenant(tenant, COAP_GATEWAY_CLIENT_ID, topic)
        .await
        .is_err()
    {
        return false;
    }
    if let Some(db) = shared.db_auth.as_ref().filter(|d| d.is_configured()) {
        if db
            .authorize_publish_in_tenant(tenant, COAP_GATEWAY_CLIENT_ID, topic)
            .await
            .is_err()
        {
            return false;
        }
    }
    if let Some(webhook) = shared.webhook.as_ref() {
        if webhook
            .authorize_publish_in_tenant(tenant, COAP_GATEWAY_CLIENT_ID, topic)
            .await
            .is_err()
        {
            return false;
        }
    }
    true
}

async fn authorize_coap_subscribe(shared: &Shared, filter: &TopicFilter) -> bool {
    let tenant = broker_session::tenant::DEFAULT_TENANT_ID;
    if shared
        .auth
        .authorize_subscribe_in_tenant(tenant, COAP_GATEWAY_CLIENT_ID, filter)
        .await
        .is_err()
    {
        return false;
    }
    if let Some(db) = shared.db_auth.as_ref().filter(|d| d.is_configured()) {
        if db
            .authorize_subscribe_in_tenant(tenant, COAP_GATEWAY_CLIENT_ID, filter)
            .await
            .is_err()
        {
            return false;
        }
    }
    true
}

/// Serve the CoAP gateway (B1-04) on an already-bound UDP socket.
///
/// Every datagram is validated as `/ps/<topic>` before it touches the
/// router: POST/PUT publishes through [`apply_coap_publish`] (retained
/// store plus router fan-out, then routed into live mailboxes with the
/// same forwarded/sent/delivered accounting as MQTT), GET reads the
/// retained store (CONTENT or NOT FOUND, with an Observe option and
/// registration when the client asked to observe), DELETE clears
/// retained state. Malformed topics answer BAD REQUEST and route
/// nothing, so a listener that accepts bytes can never silently drop
/// them: every accepted publish reaches the router.
async fn run_coap_gateway(shared: Shared, socket: Arc<tokio::net::UdpSocket>) {
    let mut buf = [0u8; 2048];
    loop {
        let (len, peer) = match socket.recv_from(&mut buf).await {
            Ok(next) => next,
            Err(e) => {
                warn!("CoAP gateway recv failed: {e}");
                continue;
            }
        };
        let req = match CoapMessage::decode(&buf[..len]) {
            Ok(req) => req,
            Err(_) => continue,
        };
        match req.code {
            CoapCode::POST | CoapCode::PUT => {
                let Some(topic_str) = CoapGatewayHandler::coap_topic(&req) else {
                    let resp = req.make_ack(
                        CoapCode::BAD_REQUEST,
                        Bytes::from_static(b"Topic cannot be empty"),
                    );
                    let _ = socket.send_to(&resp.encode(), peer).await;
                    continue;
                };
                if Topic::new(topic_str.clone()).is_err() {
                    let resp =
                        req.make_ack(CoapCode::BAD_REQUEST, Bytes::from_static(b"Invalid topic"));
                    let _ = socket.send_to(&resp.encode(), peer).await;
                    continue;
                }
                let payload = req.payload.clone();
                let Some(deliveries) = apply_coap_publish(&shared, &topic_str, &payload).await
                else {
                    let resp = req.make_ack(CoapCode::UNAUTHORIZED, Bytes::new());
                    let _ = socket.send_to(&resp.encode(), peer).await;
                    continue;
                };
                let mut enqueued = 0u64;
                let mut enqueued_bytes = 0u64;
                for (conn_id, routed) in deliveries {
                    let len = routed.total_frame_len() as u64;
                    if shared.conns.route(conn_id, routed) {
                        enqueued += 1;
                        enqueued_bytes += len;
                    }
                }
                shared.metrics.inc_messages_forwarded_by(enqueued);
                shared.metrics.inc_publish_sent_by(enqueued);
                shared.metrics.inc_delivered_by(enqueued);
                shared.metrics.inc_bytes_sent_by(enqueued_bytes);
                let resp = req.make_ack(CoapCode::CHANGED, Bytes::new());
                let _ = socket.send_to(&resp.encode(), peer).await;
            }
            CoapCode::GET => {
                let Some(topic_str) = CoapGatewayHandler::coap_topic(&req) else {
                    let resp = req.make_ack(
                        CoapCode::BAD_REQUEST,
                        Bytes::from_static(b"Topic cannot be empty"),
                    );
                    let _ = socket.send_to(&resp.encode(), peer).await;
                    continue;
                };
                let (Ok(topic), Ok(filter)) =
                    (Topic::new(topic_str.clone()), TopicFilter::new(&topic_str))
                else {
                    let resp =
                        req.make_ack(CoapCode::BAD_REQUEST, Bytes::from_static(b"Invalid topic"));
                    let _ = socket.send_to(&resp.encode(), peer).await;
                    continue;
                };
                if !authorize_coap_subscribe(&shared, &filter).await {
                    let resp = req.make_ack(CoapCode::UNAUTHORIZED, Bytes::new());
                    let _ = socket.send_to(&resp.encode(), peer).await;
                    continue;
                }
                let observing = CoapGatewayHandler::is_observe_register(&req);
                let registered = observing
                    && shared
                        .coap
                        .register_observer(&topic_str, peer, req.token.clone());
                let retained = shared
                    .retained
                    .get_retained_in_tenant(&topic, broker_session::tenant::DEFAULT_TENANT_ID)
                    .await
                    .ok()
                    .flatten();
                match retained {
                    Some(stored) => {
                        let resp = if registered {
                            let seq = shared.coap.next_observe_seq();
                            shared
                                .coap
                                .make_observe_response(&req, stored.payload.clone(), seq)
                        } else {
                            req.make_ack(CoapCode::CONTENT, stored.payload.clone())
                        };
                        let _ = socket.send_to(&resp.encode(), peer).await;
                    }
                    None => {
                        let resp = if registered {
                            let seq = shared.coap.next_observe_seq();
                            shared.coap.make_observe_response(&req, Bytes::new(), seq)
                        } else {
                            req.make_ack(CoapCode::NOT_FOUND, Bytes::from_static(b"Not Found"))
                        };
                        let _ = socket.send_to(&resp.encode(), peer).await;
                    }
                }
            }
            CoapCode::DELETE => {
                let Some(topic_str) = CoapGatewayHandler::coap_topic(&req) else {
                    let resp = req.make_ack(
                        CoapCode::BAD_REQUEST,
                        Bytes::from_static(b"Topic cannot be empty"),
                    );
                    let _ = socket.send_to(&resp.encode(), peer).await;
                    continue;
                };
                let Ok(topic) = Topic::new(topic_str) else {
                    let resp =
                        req.make_ack(CoapCode::BAD_REQUEST, Bytes::from_static(b"Invalid topic"));
                    let _ = socket.send_to(&resp.encode(), peer).await;
                    continue;
                };
                if !authorize_coap_publish(&shared, &topic).await {
                    let resp = req.make_ack(CoapCode::UNAUTHORIZED, Bytes::new());
                    let _ = socket.send_to(&resp.encode(), peer).await;
                    continue;
                }
                if shared
                    .retained
                    .clear_retained_in_tenant(&topic, broker_session::tenant::DEFAULT_TENANT_ID)
                    .await
                    .is_ok()
                {
                    sync_retained_stat(&shared).await;
                }
                let resp = req.make_ack(CoapCode::DELETED, Bytes::new());
                let _ = socket.send_to(&resp.encode(), peer).await;
            }
            _ => {
                let resp = req.make_ack(CoapCode::METHOD_NOT_ALLOWED, Bytes::new());
                let _ = socket.send_to(&resp.encode(), peer).await;
            }
        }
    }
}

/// Tenant for one bind request (MT-01..MT-03): renders the tenant
/// registry against this connection's own credentials (verified Kerberos
/// principal, else the MQTT username; certificate, peer and client-id
/// inputs from the bind itself), never against stored state of another
/// tenant's namesake session. Pure function of the bind: quota admission
/// and session resolution call it independently with the same result.
/// Fail-closed to the default tenant on undecodable binds.
fn bind_tenant_for_req(
    shared: &Shared,
    req: &BindRequest,
    kerberos_principal: Option<&str>,
) -> String {
    let ctx = broker_session::tenant::ConnectContext {
        client_id: req.client_id.clone(),
        username: kerberos_principal
            .map(str::to_string)
            .or_else(|| req.username.clone()),
        peer_host: req.peerhost.clone(),
        listener: req.listener.clone(),
        cert_cn: req.cert_cn.clone(),
        cert_subject: req.cert_subject.clone(),
        cert_sans: req.cert_sans.clone(),
        user_properties: req.user_properties.clone(),
    };
    shared.tenant_registry.assign(&ctx)
}

/// True when the listener of this bind must carry a client certificate.
/// The legacy switches deny at the bind for compatibility.
/// The new settings deny at the bind when `fail_if_no_peer_cert` is set
/// with verification on. The handshake already refuses a missing
/// certificate in that mode. Optional verification never denies
/// a missing certificate here. Off by default keeps handshakes unchanged.
/// Connect only.
fn client_cert_required(shared: &Shared, listener: &str) -> bool {
    match listener {
        "tls" => {
            shared.tls_verify_client_cert
                || (shared.tls_verify_peer && shared.tls_fail_if_no_peer_cert)
        }
        "wss" => {
            shared.wss_verify_client_cert
                || (shared.wss_verify_peer && shared.wss_fail_if_no_peer_cert)
        }
        _ => false,
    }
}

/// True when the bind carries a client certificate field.
/// At least one of common name, subject or SANs is present.
fn has_client_cert(req: &BindRequest) -> bool {
    req.cert_cn.as_ref().is_some_and(|value| !value.is_empty())
        || req
            .cert_subject
            .as_ref()
            .is_some_and(|value| !value.is_empty())
        || !req.cert_sans.is_empty()
}

/// Effective per-tenant bounds for one tenant (TM-06).
///
/// Defaults come from the limit store. A namespace config override
/// wins when present. An invalid override or an unreachable store
/// denies new use (fail closed) and logs the event. Reads take short
/// locks on the connect path only; the deliver path never calls this.
/// TODO(parity): the exact wire shape of limit fields stays open;
/// the current JSON shape applies until the checker pins it.
fn effective_tenant_limits(
    shared: &Shared,
    tenant: &str,
) -> Result<broker_session::tenant_limits::TenantLimits, &'static str> {
    if shared.tenant_limits.is_unreachable() {
        shared.tenant_limits.count_store_denied();
        tracing::warn!(tenant = %tenant, "tenant limit store is unreachable: denying new use");
        return Err("limit store is unreachable");
    }
    // The default tenant holds no bound. Only the listener bound
    // gates it. A tenant with no quota setting reads the file
    // defaults below, which default to unlimited.
    if tenant == broker_session::tenant::DEFAULT_TENANT_ID {
        return Ok(broker_session::tenant_limits::TenantLimits {
            max_connections: 0,
            max_sessions: 0,
            max_retained: 0,
        });
    }
    let mut effective = shared.tenant_limits.effective(tenant);
    let Some(entry) = shared.namespaces.get(tenant) else {
        return Ok(effective);
    };
    let parsed = broker_api::v5::mt::ns_limit_override(&entry.config)
        .map_err(|_| "namespace limit config is not valid")?;
    if let Some(value) = parsed.max_connections {
        effective.max_connections = value;
    }
    if let Some(value) = parsed.max_sessions {
        effective.max_sessions = value;
    }
    if let Some(value) = parsed.max_retained {
        effective.max_retained = value;
    }
    Ok(effective)
}

/// True when one more live connection fits in `tenant`.
///
/// Counts live sessions in the owning tenant only. A full tenant
/// refuses with the documented quota code, counted and logged.
/// Connect path only; the publish and deliver paths never call this.
fn tenant_connection_fits(shared: &Shared, tenant: &str) -> bool {
    let limits = match effective_tenant_limits(shared, tenant) {
        Ok(limits) => limits,
        Err(_) => return false,
    };
    if limits.max_connections == 0 {
        return true;
    }
    let live = shared.sessions.live_connection_count_in_tenant(tenant);
    (live as u64) < u64::from(limits.max_connections)
}

/// True when one more session fits in `tenant`.
///
/// Counts sessions in the owning tenant only. A full tenant refuses
/// with the documented quota code, counted and logged. Connect path
/// only; the publish and deliver paths never call this.
fn tenant_session_fits(shared: &Shared, tenant: &str) -> bool {
    let limits = match effective_tenant_limits(shared, tenant) {
        Ok(limits) => limits,
        Err(_) => return false,
    };
    if limits.max_sessions == 0 {
        return true;
    }
    let total = shared.sessions.session_count_in_tenant(tenant);
    (total as u64) < u64::from(limits.max_sessions)
}

/// Tenant id recorded on one session, or the default tenant when the
/// session is unknown. Unknown fails closed to the pre-tenancy path,
/// never to another tenant's space. One short session read on
/// subscribe/bind/unsubscribe paths only; the per-message publish path
/// resolves the publisher once per publish (below), never per delivery,
/// so fan-out itself takes no new lock.
/// Default-tenant lookup; connection-scoped callers prefer
/// [`session_tenant_for_conn`] below so a non-default tenant's
/// connection resolves to its own tenant.
fn session_tenant(shared: &Shared, client_id: &str) -> String {
    shared
        .sessions
        .get(client_id)
        .map(|session| session.tenant_id.read().clone())
        .unwrap_or_else(|| broker_session::tenant::DEFAULT_TENANT_ID.to_string())
}

/// Tenant of the session owning `conn_id` when it also owns
/// `client_id` (MT-03): a non-default tenant's connection resolves to
/// its own tenant, never to the default-tenant namesake. Falls back to
/// the bare [`session_tenant`] lookup when the connection is unbound or
/// has been rebound to another client (fail closed to the pre-tenancy
/// path). One short index read plus at most one short session read;
/// subscribe/unsubscribe paths only, never per delivery.
fn session_tenant_for_conn(shared: &Shared, conn_id: u64, client_id: &str) -> String {
    if let Some(session) = shared.sessions.session_for_conn(conn_id) {
        if session.client_id == client_id {
            return session.tenant_id.read().clone();
        }
    }
    session_tenant(shared, client_id)
}

/// Session owning `conn_id` when it also owns `client_id` (MT-03),
/// else `None`. Connection ids are unique per edge connection, so the
/// index identifies exactly one `(tenant, client id)` pair; the
/// client-id check guards a conn id rebound to another client.
/// Connection paths only; the per-message publish path resolves the
/// publisher once per publish, never per delivery.
fn session_for_conn_if_client(
    shared: &Shared,
    conn_id: u64,
    client_id: &str,
) -> Option<Arc<broker_session::Session>> {
    let session = shared.sessions.session_for_conn(conn_id)?;
    if session.client_id == client_id {
        Some(session)
    } else {
        None
    }
}

/// Remove one session's router copies in its own tenant only, so one
/// tenant's teardown can never remove another tenant's subscription for
/// the same client id. Falls back to the unscoped sweep when the session
/// (and its tenant) is already gone: removing too much on a dead session
/// is fail-closed, keeping a stray copy would leak across reconnects.
/// Runs off the hot path (unbind/detach only).
fn unsubscribe_scoped(shared: &Shared, client_id: &str, filter: &TopicFilter) {
    if let Some(session) = shared.sessions.get(client_id) {
        let tenant = session.tenant_id.read().clone();
        shared
            .router
            .unsubscribe_in_tenant(filter, client_id, &tenant);
    } else {
        shared.router.unsubscribe(filter, client_id);
    }
}

/// Remove one `(tenant, client id)` session's router copies in its own
/// tenant only (MT-03). Used by death sweeps that already carry the
/// owning key, so one tenant's teardown can never remove another
/// tenant's subscription for the same client id.
fn unsubscribe_scoped_in_tenant(
    shared: &Shared,
    tenant: &str,
    client_id: &str,
    filter: &TopicFilter,
) {
    shared
        .router
        .unsubscribe_in_tenant(filter, client_id, tenant);
}

/// Build one `PublishOut` frame per router match. Shared by the standard
/// fan-out and the rule [`BrokerSink`] so both paths downgrade QoS and
/// allocate downlink packet ids identically.
///
/// Delivery targets the session's live connection: a detached durable
/// session buffers into its offline queue instead, and a detached clean
/// session drops. Sessions unknown to the manager fall back to the
/// router-registered conn_id.
///
/// PERF-10: each silent drop also bumps its kernel counter (detached
/// clean drop, durable-queue eviction) exactly where the frame is
/// discarded. Drops that happened before still happen.
///
/// B4-02: `publisher` carries the publishing client id for the shared
/// `hash_clientid` strategy. The MQTT ingress path passes the real id;
/// rule republishes, retained replays, cluster forwards and management
/// publishes pass `None` (the router hashes the empty string there; see
/// its open-question note on that branch). Default-tenant entry; fan-out
/// paths that know the publisher's tenant call
/// [`build_downlink_frames_with_publisher`] instead.
#[allow(clippy::too_many_arguments)]
fn build_downlink_frames(
    router: &Router,
    sessions: &SessionManager,
    metrics: &Metrics,
    topic: &Topic,
    qos: QoS,
    retain: bool,
    payload: &Bytes,
    tenant: &str,
    engine: Option<&Arc<RuleEngine>>,
) -> Vec<(u64, BrokerFrame)> {
    build_downlink_frames_with_publisher(
        router, sessions, metrics, topic, qos, retain, payload, None, tenant, engine,
    )
}

/// [`build_downlink_frames`] with publisher context for shared
/// strategies plus tenant scope (MT-02). This is the delivery event that
/// drives `balance_shared_groups` (via `matches_in_tenant_counted`):
/// every publish fan-out reduces the matched shared groups through the
/// router call and consumes the reduced set here. Only the publisher's
/// tenant matches: a tenant mismatch delivers nothing and is counted via
/// `tenant_mismatch_dropped`, never an error to the publisher.
#[allow(clippy::too_many_arguments)]
fn build_downlink_frames_with_publisher(
    router: &Router,
    sessions: &SessionManager,
    metrics: &Metrics,
    topic: &Topic,
    qos: QoS,
    retain: bool,
    payload: &Bytes,
    publisher: Option<&str>,
    publisher_tenant: &str,
    engine: Option<&Arc<RuleEngine>>,
) -> Vec<(u64, BrokerFrame)> {
    let empty = PublishV5Props::default();
    build_downlink_frames_with_publisher_and_props(
        router,
        sessions,
        metrics,
        topic,
        qos,
        retain,
        payload,
        publisher,
        publisher_tenant,
        &empty,
        engine,
    )
}

/// [`build_downlink_frames_with_publisher`] with X1-03 v5 properties.
/// The MQTT ingress path passes the parsed inbound properties so each
/// version-5 delivery carries them; every other fan-out passes the
/// default (no properties, bytes unchanged). `engine` carries the rule
/// engine for RQ-04 event fires (checked with one relaxed load when no
/// event rule exists, so the publish fast path pays no lock).
#[allow(clippy::too_many_arguments)]
fn build_downlink_frames_with_publisher_and_props(
    router: &Router,
    sessions: &SessionManager,
    metrics: &Metrics,
    topic: &Topic,
    qos: QoS,
    retain: bool,
    payload: &Bytes,
    publisher: Option<&str>,
    publisher_tenant: &str,
    v5: &PublishV5Props,
    engine: Option<&Arc<RuleEngine>>,
) -> Vec<(u64, BrokerFrame)> {
    let qos_raw = u8::from(qos);
    let topic_str = topic.as_str();
    // D1-03: share one payload buffer across all N subscribers.
    // `Bytes::clone` bumps a refcount without copying the bytes, so an
    // 8 KB publish fanned out to N subscribers costs one 8 KB buffer
    // plus N small handles. Per-subscriber memory stays bounded by the
    // existing queue caps (`MAX_OFFLINE_QUEUE`, the QoS 1 window
    // `DEFAULT_MAX_QOS1_INFLIGHT` plus spill `DEFAULT_MAX_QOS1_SPILL`,
    // `MAX_QOS2_INFLIGHT`, the `ConnTable` QoS 0 bound); it is never
    // N copies of the payload. Clone once here so every per-subscriber
    // `clone` below shares this single root.
    let shared = payload.clone();
    let mut deliveries = Vec::new();
    // One aggregate event load per publish. When no event rule exists
    // all fire blocks below skip with no lock and no clone.
    let want_events = engine.map(|e| e.has_any_event_selected()).unwrap_or(false);
    let (matched, tenant_skipped) =
        router.matches_in_tenant_counted(topic, publisher, publisher_tenant);
    if tenant_skipped > 0 {
        metrics.inc_tenant_mismatch_dropped_by(tenant_skipped);
    }
    for sub in matched {
        let effective = std::cmp::min(qos_raw, u8::from(sub.qos));
        // X1-03 no-local (delivery event only): never deliver a client's
        // own publish to its no-local subscription. Others' publishes
        // still deliver. No new lock (fields ride the already-cloned
        // `Subscription`); suppressed self-delivery is specified
        // behaviour, never counted as loss.
        if sub.no_local {
            if let Some(publisher_id) = publisher {
                if publisher_id == sub.client_id.as_ref() {
                    continue;
                }
            }
        }
        // X1-03 retain-as-published (delivery event only): RAP=1
        // preserves the publish retain bit on the wire; RAP=0 clears it.
        // No new lock; the stored inflight/offline copy keeps the
        // delivered flag so replay preserves what the wire carried.
        // Retained replays honor the same flag via the granting
        // subscription's stored RAP (legacy grants preserve).
        let delivered_retain = if sub.retain_as_published {
            retain
        } else {
            false
        };
        // Session in the subscription's own tenant (MT-03): the same
        // client id in two tenants delivers from independent sessions
        // with independent queues and inflight state. A missing session
        // in this tenant falls back to the legacy conn-id route below.
        match sessions.get_in_tenant(&sub.tenant, &sub.client_id) {
            Some(session) => {
                if sub.session_id != 0 && session.id.0 != sub.session_id {
                    continue;
                }
                let connected = *session.connected.read();
                let live = *session.conn_id.read();
                match (connected, live) {
                    (true, Some(conn_id)) => {
                        // X1-02: honour the v5 CONNECT caps on what the
                        // kernel sends this connection (never more
                        // unacknowledged QoS 1/2 than the client allows;
                        // never a frame above its maximum — shed with
                        // the documented reason code, counted, never
                        // silent). Version-4 sessions always send.
                        // Uncapped sessions return after two atomic
                        // loads; the publish fast path pays no lock.
                        if let Some(rc) =
                            v5_egress_refused(&session, effective, topic_str.len(), shared.len())
                        {
                            if rc == V5_RC_QUOTA_EXCEEDED {
                                metrics.inc_inflight_dropped();
                            } else {
                                metrics.inc_messages_dropped();
                            }
                            tracing::debug!(
                                client_id = %session.client_id,
                                reason_code = rc,
                                effective_qos = effective,
                                "v5 egress cap shed delivery"
                            );
                            continue;
                        }
                        let mut downlink_id = 0u16;
                        // PF-02: one pass assigns the id and holds the
                        // entry, atomically enforcing the v5 Receive Maximum
                        // send window across live delivery and replay.
                        if effective == 1 {
                            let Some((pid, outcome)) = session.try_alloc_qos1_downlink_v5_checked(
                                topic.clone(),
                                QoS::AtLeastOnce,
                                delivered_retain,
                                shared.clone(),
                                std::time::Instant::now(),
                            ) else {
                                metrics.inc_inflight_dropped();
                                continue;
                            };
                            downlink_id = pid;
                            match outcome {
                                InflightTrackOutcome::Tracked => {}
                                InflightTrackOutcome::Spilled => {
                                    metrics.inc_inflight_spilled();
                                }
                                InflightTrackOutcome::Dropped => {
                                    metrics.inc_inflight_dropped();
                                    metrics.inc_inflight_spill_evicted();
                                }
                            }
                        } else if effective == 2 {
                            let Some((pid, tracked)) = session.try_alloc_qos2_outbound_v5_checked(
                                topic.clone(),
                                delivered_retain,
                                shared.clone(),
                            ) else {
                                metrics.inc_inflight_dropped();
                                continue;
                            };
                            downlink_id = pid;
                            if !tracked {
                                metrics.inc_inflight_dropped();
                            }
                        }
                        // B4-05 outbound aliases: assign (or reuse) one
                        // alias for this topic when the subscriber
                        // negotiated a maximum. Read-first (one read lock
                        // plus a bounded scan, no allocation) so repeat
                        // deliveries reuse without a write; only the first
                        // delivery of a new topic takes the write lock.
                        // Max 0 (the client sent no maximum, the common
                        // case) costs one relaxed atomic load and skips
                        // the table with no lock and no allocation.
                        // Driven by the delivery event. The alias rides in
                        // the downlink meta next to the full topic.
                        let alias = if session.outbound_alias_max() == 0 {
                            0
                        } else {
                            session
                                .outbound_alias_for(topic_str)
                                .or_else(|| session.assign_outbound_alias(topic))
                                .unwrap_or(0)
                        };
                        if let Some(frame) = encode_publish_out_full_with_props(
                            conn_id,
                            topic_str,
                            downlink_id,
                            effective,
                            delivered_retain,
                            false,
                            alias,
                            sub.subscription_id,
                            &shared,
                            v5,
                        ) {
                            // Fire $events/message_delivered.
                            // Check flags before any lock: no cost
                            // when no rule selects the source.
                            if want_events {
                                if let Some(engine) = engine {
                                    use broker_rules::EventSource;
                                    if engine
                                        .is_event_source_selected(EventSource::MessageDelivered)
                                        && is_default_tenant(&sub.tenant)
                                        && is_default_tenant(publisher_tenant)
                                    {
                                        let username = session.username.read().clone();
                                        let peerhost = session.peerhost.read().clone();
                                        engine.fire_message_delivered(
                                            broker_rules::MessageDeliveredEvent {
                                                client_id: sub.client_id.to_string(),
                                                username,
                                                peerhost,
                                                topic: topic_str.to_string(),
                                                qos: effective,
                                                retain,
                                                payload: shared.clone(),
                                                msg_id: if effective == 0 {
                                                    None
                                                } else {
                                                    Some(downlink_id)
                                                },
                                            },
                                        );
                                    }
                                }
                            }
                            deliveries.push((conn_id, frame));
                        }
                    }
                    _ => {
                        // Detached (or half-bound) session: durable
                        // sessions buffer for replay, sessions that drop
                        // state at disconnect (X1-02: v5 expiry 0, v4
                        // clean start) drop.
                        if !session.drops_state_at_disconnect() {
                            // Hook only: the eviction itself stays
                            // inside `push_offline_with_limit` under the
                            // manager's configured cap; count what the
                            // push displaced. A `false` return means the
                            // durable write failed and nothing was queued
                            // (fail closed, ack withheld upstream), so no
                            // eviction is counted for that drop.
                            let before = session.offline_len();
                            let queued = session.push_offline_with_limit(
                                QueuedMessage {
                                    topic: topic.clone(),
                                    qos: QoS::try_from(effective).unwrap_or(QoS::AtMostOnce),
                                    retain: delivered_retain,
                                    payload: shared.clone(),
                                    publish_at_ms: None,
                                },
                                sessions.max_offline_queue(),
                            );
                            if queued {
                                let evicted = (before + 1).saturating_sub(session.offline_len());
                                if evicted > 0 {
                                    metrics.inc_offline_queue_evicted_by(evicted as u64);
                                }
                            }
                        } else {
                            // Fire $events/message_dropped for detached clean session.
                            // Check flags before any lock.
                            if want_events {
                                if let Some(engine) = engine {
                                    use broker_rules::EventSource;
                                    if engine.is_event_source_selected(EventSource::MessageDropped)
                                        && is_default_tenant(&sub.tenant)
                                        && is_default_tenant(publisher_tenant)
                                    {
                                        let username = session.username.read().clone();
                                        let peerhost = session.peerhost.read().clone();
                                        engine.fire_message_dropped(
                                            broker_rules::MessageDroppedEvent {
                                                client_id: sub.client_id.to_string(),
                                                username,
                                                peerhost,
                                                topic: topic_str.to_string(),
                                                qos: effective,
                                                reason: "no_session_clean".to_string(),
                                            },
                                        );
                                    }
                                }
                            }
                            metrics.inc_detached_clean_dropped();
                        }
                    }
                }
            }
            None => {
                // No session on record: legacy fallback to the
                // router-registered conn_id (same no-local/RAP/sub-id
                // semantics as the session path above).
                let downlink_id = if effective == 0 { 0u16 } else { 1u16 };
                if let Some(frame) = encode_publish_out_full_with_props(
                    sub.conn_id,
                    topic_str,
                    downlink_id,
                    effective,
                    delivered_retain,
                    false,
                    0,
                    sub.subscription_id,
                    &shared,
                    v5,
                ) {
                    // Fire $events/message_delivered for legacy fallback.
                    // Check flags first: no work when no rule selects it.
                    if want_events {
                        if let Some(engine) = engine {
                            use broker_rules::EventSource;
                            if engine.is_event_source_selected(EventSource::MessageDelivered)
                                && is_default_tenant(&sub.tenant)
                                && is_default_tenant(publisher_tenant)
                            {
                                engine.fire_message_delivered(
                                    broker_rules::MessageDeliveredEvent {
                                        client_id: sub.client_id.to_string(),
                                        username: None,
                                        peerhost: None,
                                        topic: topic_str.to_string(),
                                        qos: effective,
                                        retain,
                                        payload: shared.clone(),
                                        msg_id: if effective == 0 {
                                            None
                                        } else {
                                            Some(downlink_id)
                                        },
                                    },
                                );
                            }
                        }
                    }
                    deliveries.push((sub.conn_id, frame));
                }
            }
        }
    }
    deliveries
}

/// Token-bucket gate for one publish (INDRA-128). Resolves the owning
/// client, looks up its user's rate quota, and consumes one token.
/// True means route; false means drop. Anonymous, unknown, and
/// unconfigured clients are unlimited.
async fn check_publish_quota(frame: &BrokerFrame, shared: &Shared) -> bool {
    // Owner session first (MT-03), legacy lookup behind: one tenant's
    // publishes consume only its own budget, even for identical client
    // ids. Anonymous, unknown, and unconfigured clients are unlimited.
    let session = shared
        .sessions
        .session_for_conn(frame.header.conn_id)
        .or_else(|| {
            shared
                .sessions
                .client_id_for_conn(frame.header.conn_id)
                .as_deref()
                .and_then(|p| shared.sessions.get(p))
        });
    let Some(session) = session else {
        return true;
    };
    let username = session.username.read().clone();
    let Some(username) = username else {
        return true;
    };
    // Tenant scope (MT-06): quotas are per (tenant, username). Fast path
    // borrows the default static with one relaxed atomic load (no lock,
    // no allocation); only a non-default tenant takes the short field
    // read plus one bounded copy.
    let quota_tenant_buf: Option<String> = if session
        .tenant_is_default
        .load(std::sync::atomic::Ordering::Relaxed)
    {
        None
    } else {
        Some(session.tenant_id.read().clone())
    };
    let quota_tenant: &str = quota_tenant_buf
        .as_deref()
        .unwrap_or(broker_session::tenant::DEFAULT_TENANT_ID);
    let Some(quotas) = shared.auth.get_quotas_in_tenant(quota_tenant, &username) else {
        return true;
    };
    let Some(rate) = quotas.max_publish_rate else {
        return true;
    };
    let burst = quotas.max_publish_burst.unwrap_or(rate);
    // The budget key is the session's own (tenant, client id): the
    // connection index already resolved the owning tenant above.
    // Borrowed lookup takes no per-publish allocation on the
    // single-tenant fast path (default static plus the session's own
    // `client_id` borrow). Only a non-default tenant takes one bounded
    // copy (at most 128 chars per the MT-01 `MAX_TENANT_ID_LEN` bound).
    if session
        .tenant_is_default
        .load(std::sync::atomic::Ordering::Relaxed)
    {
        return shared.sessions.check_publish_budget_in_tenant(
            broker_session::tenant::DEFAULT_TENANT_ID,
            session.client_id.as_str(),
            rate,
            burst,
        );
    }
    let tenant = session.tenant_id.read().clone();
    shared.sessions.check_publish_budget_in_tenant(
        tenant.as_str(),
        session.client_id.as_str(),
        rate,
        burst,
    )
}

/// Build the acknowledgement for a denied QoS 1 or QoS 2 publish.
/// Version 5 carries `reason_code` on the ack. Version 3.1.1 has no ack
/// reason code, so the reason stays 0 and the edge sends the short form.
fn denied_publish_ack(
    frame: &BrokerFrame,
    packet_id: u16,
    qos: QoS,
    protocol_version: u8,
    reason_code: u8,
) -> Option<BrokerFrame> {
    match qos {
        QoS::AtLeastOnce => pub_ack_reply(frame, packet_id, reason_code),
        QoS::ExactlyOnce => qos2_reply_with_reason(
            OpCode::PubRecOut,
            frame,
            packet_id,
            if protocol_version == 5 {
                reason_code
            } else {
                0
            },
        ),
        QoS::AtMostOnce => None,
    }
}

/// Build one `PubAckOut` reply mirroring the request identity.
fn pub_ack_reply(frame: &BrokerFrame, packet_id: u16, return_code: u8) -> Option<BrokerFrame> {
    let mut meta = Vec::with_capacity(3);
    meta.extend_from_slice(&packet_id.to_be_bytes());
    meta.push(return_code);
    BrokerFrame::new(
        OpCode::PubAckOut,
        frame.header.conn_id,
        frame.header.sequence_no,
        Bytes::from(meta),
        Bytes::new(),
    )
    .ok()
}

/// Encode one `PublishOut` frame for `conn_id` (sequence stamped later
/// per-destination by `ConnTable::route`). Base alias-only form
/// (`old_len + 2` with alias 0): the canonical encoding for deliveries
/// without a subscription identifier, so version-4 bytes stay identical.
fn encode_publish_out(
    conn_id: u64,
    topic: &str,
    packet_id: u16,
    qos: u8,
    retain: bool,
    payload: &Bytes,
) -> Option<BrokerFrame> {
    let mut meta = Vec::with_capacity(2 + topic.len() + 2 + 3 + 2);
    meta.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    meta.extend_from_slice(topic.as_bytes());
    meta.extend_from_slice(&packet_id.to_be_bytes());
    meta.push(qos);
    meta.push(u8::from(retain));
    meta.push(0u8);
    meta.extend_from_slice(&protocol_v5::encode_topic_alias(0));
    let frame = BrokerFrame::new(
        OpCode::PublishOut,
        conn_id,
        0, // stamped per-destination by ConnTable::route
        Bytes::from(meta),
        payload.clone(),
    )
    .ok()?;
    debug_assert_eq!(decode_publish_out_alias(&frame.metadata), Some(0));
    debug_assert_eq!(decode_publish_out_sub_id(&frame.metadata), Some(0));
    debug_assert_eq!(
        decode_publish_out_props(&frame.metadata),
        Some(PublishV5Props::default())
    );
    Some(frame)
}

/// Encode one `PublishOut` frame with explicit DUP (replays set DUP=1,
/// fresh deliveries DUP=0). Sequence is stamped later per-destination by
/// `ConnTable::route`. `alias` is the B4-05 outbound alias (0 = full
/// topic, no alias); the topic itself always travels in full so a
/// pre-alias edge still delivers correctly while the alias rides along
/// for alias-aware edges.
///
/// TODO(parity): sending the full topic alongside the alias costs the
/// bytes the alias is meant to save on the socket. Once the edge speaks
/// the alias property on the wire, repeat deliveries should carry the
/// empty topic with the alias instead.
#[allow(clippy::too_many_arguments)]
fn encode_publish_out_with_dup(
    conn_id: u64,
    topic: &str,
    packet_id: u16,
    qos: u8,
    retain: bool,
    dup: bool,
    alias: u16,
    payload: &Bytes,
) -> Option<BrokerFrame> {
    if !dup && alias == 0 {
        return encode_publish_out(conn_id, topic, packet_id, qos, retain, payload);
    }
    // X1-03: alias/dup form builds directly (no delegation to
    // `encode_publish_out_full`, which delegates here when the
    // subscription identifier is absent; mutual delegation recursed
    // without bound and aborted the broker-node test binary with
    // SIGABRT on any delivery carrying a nonzero alias).
    let mut meta = Vec::with_capacity(2 + topic.len() + 2 + 3 + 2);
    meta.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    meta.extend_from_slice(topic.as_bytes());
    meta.extend_from_slice(&packet_id.to_be_bytes());
    meta.push(qos);
    meta.push(u8::from(retain));
    meta.push(u8::from(dup));
    meta.extend_from_slice(&protocol_v5::encode_topic_alias(alias));
    let frame = BrokerFrame::new(
        OpCode::PublishOut,
        conn_id,
        0, // stamped per-destination by ConnTable::route
        Bytes::from(meta),
        payload.clone(),
    )
    .ok()?;
    debug_assert_eq!(decode_publish_out_alias(&frame.metadata), Some(alias));
    debug_assert_eq!(decode_publish_out_sub_id(&frame.metadata), Some(0));
    debug_assert_eq!(
        decode_publish_out_props(&frame.metadata),
        Some(PublishV5Props::default())
    );
    Some(frame)
}

/// Encode one `PublishOut` frame with its X1-03 subscription identifier
/// (0 = absent, never encoded as a property on version-4 sockets; the
/// edge omits the property when 0 so 3.1.1 bytes stay identical).
/// `sub_id` is one matching subscription's identifier (bounded by
/// [`broker_router::MAX_SUBSCRIPTION_ID`]); a delivery matching N
/// filters fans out as N frames each carrying its own identifier, so one
/// frame never accumulates an unbounded id list. One frame per match
/// keeps the pre-v5 fan-out, so version-4 duplicates stay unchanged.
#[allow(clippy::too_many_arguments)]
fn encode_publish_out_full(
    conn_id: u64,
    topic: &str,
    packet_id: u16,
    qos: u8,
    retain: bool,
    dup: bool,
    alias: u16,
    sub_id: u32,
    payload: &Bytes,
) -> Option<BrokerFrame> {
    let empty = PublishV5Props::default();
    encode_publish_out_full_with_props(
        conn_id, topic, packet_id, qos, retain, dup, alias, sub_id, payload, &empty,
    )
}

/// Encode one `PublishOut` frame with its X1-03 subscription identifier
/// plus the forwarded v5 properties (payload format, message expiry,
/// user properties). Empty properties encode exactly like
/// [`encode_publish_out_full`] (bytes identical); non-empty properties
/// append `Format:8 | Expiry:32be | UserCount:16be | pairs' after the
/// `SubId:32be` trailer. The edge omits the whole section toward
/// version-4 sockets so 3.1.1 bytes stay identical, and version-5
/// sockets observe what the publisher sent. User pairs are bounded by
/// [`MAX_PUBLISH_V5_USERS`] / [`MAX_PUBLISH_V5_STRING`] (fail closed:
/// over-bound properties fall back to no properties, never half a
/// section).
#[allow(clippy::too_many_arguments)]
fn encode_publish_out_full_with_props(
    conn_id: u64,
    topic: &str,
    packet_id: u16,
    qos: u8,
    retain: bool,
    dup: bool,
    alias: u16,
    sub_id: u32,
    payload: &Bytes,
    v5: &PublishV5Props,
) -> Option<BrokerFrame> {
    if v5.is_empty() {
        if sub_id == 0 || sub_id > broker_router::MAX_SUBSCRIPTION_ID {
            return encode_publish_out_with_dup(
                conn_id, topic, packet_id, qos, retain, dup, alias, payload,
            );
        }
        let mut meta = Vec::with_capacity(2 + topic.len() + 2 + 3 + 2 + 4);
        meta.extend_from_slice(&(topic.len() as u16).to_be_bytes());
        meta.extend_from_slice(topic.as_bytes());
        meta.extend_from_slice(&packet_id.to_be_bytes());
        meta.push(qos);
        meta.push(u8::from(retain));
        meta.push(u8::from(dup));
        meta.extend_from_slice(&protocol_v5::encode_topic_alias(alias));
        meta.extend_from_slice(&sub_id.to_be_bytes());
        let frame = BrokerFrame::new(
            OpCode::PublishOut,
            conn_id,
            0, // stamped per-destination by ConnTable::route
            Bytes::from(meta),
            payload.clone(),
        )
        .ok()?;
        debug_assert_eq!(decode_publish_out_alias(&frame.metadata), Some(alias));
        debug_assert_eq!(decode_publish_out_sub_id(&frame.metadata), Some(sub_id));
        debug_assert_eq!(
            decode_publish_out_props(&frame.metadata),
            Some(PublishV5Props::default())
        );
        return Some(frame);
    }
    if v5.format != 0 && v5.format != 1 {
        return encode_publish_out_full(
            conn_id, topic, packet_id, qos, retain, dup, alias, sub_id, payload,
        );
    }
    if v5.users.len() > MAX_PUBLISH_V5_USERS {
        return encode_publish_out_full(
            conn_id, topic, packet_id, qos, retain, dup, alias, sub_id, payload,
        );
    }
    for (k, val) in &v5.users {
        if k.is_empty() || k.len() > MAX_PUBLISH_V5_STRING || val.len() > MAX_PUBLISH_V5_STRING {
            return encode_publish_out_full(
                conn_id, topic, packet_id, qos, retain, dup, alias, sub_id, payload,
            );
        }
        if std::str::from_utf8(k).is_err() || std::str::from_utf8(val).is_err() {
            return encode_publish_out_full(
                conn_id, topic, packet_id, qos, retain, dup, alias, sub_id, payload,
            );
        }
    }
    let mut meta =
        Vec::with_capacity(2 + topic.len() + 2 + 3 + 2 + 4 + 1 + 4 + 2 + v5.users.len() * 8);
    meta.extend_from_slice(&(topic.len() as u16).to_be_bytes());
    meta.extend_from_slice(topic.as_bytes());
    meta.extend_from_slice(&packet_id.to_be_bytes());
    meta.push(qos);
    meta.push(u8::from(retain));
    meta.push(u8::from(dup));
    meta.extend_from_slice(&protocol_v5::encode_topic_alias(alias));
    let stored_sub = if sub_id == 0 || sub_id > broker_router::MAX_SUBSCRIPTION_ID {
        0u32
    } else {
        sub_id
    };
    meta.extend_from_slice(&stored_sub.to_be_bytes());
    meta.push(v5.format);
    meta.extend_from_slice(&v5.expiry.to_be_bytes());
    meta.extend_from_slice(&(v5.users.len() as u16).to_be_bytes());
    for (k, val) in &v5.users {
        meta.extend_from_slice(&(k.len() as u16).to_be_bytes());
        meta.extend_from_slice(k);
        meta.extend_from_slice(&(val.len() as u16).to_be_bytes());
        meta.extend_from_slice(val);
    }
    let frame = BrokerFrame::new(
        OpCode::PublishOut,
        conn_id,
        0, // stamped per-destination by ConnTable::route
        Bytes::from(meta),
        payload.clone(),
    )
    .ok()?;
    debug_assert_eq!(decode_publish_out_alias(&frame.metadata), Some(alias));
    debug_assert_eq!(decode_publish_out_sub_id(&frame.metadata), Some(stored_sub));
    debug_assert_eq!(decode_publish_out_props(&frame.metadata), Some(v5.clone()));
    Some(frame)
}

/// Encode one `PubRelOut` frame for `conn_id` (sequence stamped later
/// per-destination by `ConnTable::route`). Meta is the 2-byte packet id.
fn encode_pubrel_out(conn_id: u64, packet_id: u16) -> Option<BrokerFrame> {
    BrokerFrame::new(
        OpCode::PubRelOut,
        conn_id,
        0, // stamped per-destination by ConnTable::route
        Bytes::from(packet_id.to_be_bytes().to_vec()),
        Bytes::new(),
    )
    .ok()
}

/// Pump messages forwarded from peer nodes until the plane closes.
/// Inbound cluster traffic fans out locally only: only ingress paths
/// forward, so loops cannot form even though messages carry no history.
async fn run_cluster_inbox(shared: Shared) {
    let cluster = match &shared.cluster {
        Some(cluster) => cluster.clone(),
        None => return,
    };
    while let Some(msg) = cluster.recv_message().await {
        deliver_cluster_message(&shared, msg).await;
    }
    debug!("Cluster inbox closed");
}

/// Deliver one peer-forwarded message to local subscribers: pure fan-out
/// with no retained writes, no rule execution (both already happened at
/// the ingress node), and no re-forward.
async fn deliver_cluster_message(shared: &Shared, msg: ClusterMessage) {
    // TODO(parity): `ClusterMessage` carries no tenant yet, so peer forwards
    // fan out in the default tenant only (fail closed, single-node
    // single-tenant behaviour unchanged). The open question is the tenant
    // attribution for cluster forwards once the cluster protocol carries one.
    for (conn_id, frame) in build_downlink_frames(
        &shared.router,
        &shared.sessions,
        &shared.metrics,
        &msg.topic,
        msg.qos,
        false,
        &msg.payload,
        broker_session::tenant::DEFAULT_TENANT_ID,
        Some(&shared.engine),
    ) {
        // A dead mailbox is already counted inside `ConnTable::route`.
        if !shared.conns.route(conn_id, frame) {
            // Fire $events/delivery_dropped (RQ-04) for dead/unknown conn.
            fire_delivery_dropped_on_route_fail(
                &shared.engine,
                &shared.sessions,
                conn_id,
                msg.topic.as_str().to_string(),
                u8::from(msg.qos),
                "dead_mailbox",
            );
        }
    }
    // CoAP observers see cluster forwards like local ingress (same cheap
    // empty check when the gateway never ran).
    maybe_notify_coap_observers(shared, msg.topic.as_str(), &msg.payload);
}

/// Detach one edge connection: mark its session disconnected (ownership
/// verified against the bound conn_id) and forget its mailbox. A clean
/// session drops its subscriptions (session mirror and router copies, so
/// a later reconnect with the same client id receives nothing until it
/// resubscribes) and its unacked QoS 1 inflight state; durable
/// subscriptions and unacked downlinks survive for reconnect replay. The
/// sweep touches only this session's own filters and runs off the hot path.
///
/// F1-01: a clean `DISCONNECT` (unbind) suppresses the last will. The
/// stored will is taken and dropped, but only for the verified owner, so
/// a racing teardown for a superseded conn_id never clears the fresh
/// connection's will.
fn apply_unbind(frame: &BrokerFrame, shared: &Shared) {
    if let Some(client_id) = decode_unbind_meta(&frame.metadata) {
        // The connection index is authoritative for ownership (MT-03):
        // the key identifies the owning `(tenant, client id)` even when
        // two tenants share the client id, so teardown detaches and
        // sweeps only the owning tenant. A stale decoded id for a
        // rebound connection never detaches the new owner: the key wins.
        // Unindexed connections fall back to the default-tenant detach.
        // F1-01: a clean `DISCONNECT` (unbind) suppresses the last will.
        // The stored will is taken and dropped, but only for the verified
        // owner, so a racing teardown for a superseded conn_id never
        // clears the fresh connection's will.
        // Capture identity for the disconnect event before detach.
        let key = shared.sessions.key_for_conn(frame.header.conn_id);
        let (username, peerhost) = match key.as_ref() {
            Some(k) => shared
                .sessions
                .get_in_tenant(&k.tenant, &k.client_id)
                .map(|s| (s.username.read().clone(), s.peerhost.read().clone()))
                .unwrap_or((None, None)),
            None => shared
                .sessions
                .get(&client_id)
                .map(|s| (s.username.read().clone(), s.peerhost.read().clone()))
                .unwrap_or((None, None)),
        };
        let event_tenant: String = key
            .as_ref()
            .map(|k| k.tenant.to_string())
            .unwrap_or_else(|| broker_session::tenant::DEFAULT_TENANT_ID.to_string());
        match key {
            Some(key) => {
                if let Some(session) = shared.sessions.get_in_tenant(&key.tenant, &key.client_id) {
                    if *session.conn_id.read() == Some(frame.header.conn_id) {
                        session.take_last_will();
                    }
                }
                let swept = shared.sessions.unbind_connection_in_tenant(
                    &key.tenant,
                    &key.client_id,
                    frame.header.conn_id,
                );
                for filter in &swept {
                    unsubscribe_scoped_in_tenant(shared, &key.tenant, &key.client_id, filter);
                }
            }
            None => {
                if let Some(session) = shared.sessions.get(&client_id) {
                    if *session.conn_id.read() == Some(frame.header.conn_id) {
                        session.take_last_will();
                    }
                }
                let swept = shared
                    .sessions
                    .unbind_connection(&client_id, frame.header.conn_id);
                for filter in &swept {
                    unsubscribe_scoped(shared, &client_id, filter);
                }
            }
        }
        refresh_subscription_stats(shared);
        // Fire the disconnect event. Rules belong to the default tenant.
        // The edge forwards no reason code, so the code stays absent.
        // Disconnect path only, never publish or deliver.
        if shared
            .engine
            .is_event_source_selected(broker_rules::EventSource::ClientDisconnected)
            && is_default_tenant(&event_tenant)
        {
            shared
                .engine
                .fire_client_disconnected(broker_rules::ClientDisconnectedEvent {
                    client_id: client_id.clone(),
                    username,
                    peerhost,
                    reason: "normal".to_string(),
                    disconnect_code: None,
                });
        }
    }
    shared.conns.unregister(frame.header.conn_id);
}

/// Decode a `DisconnectIn` (ungraceful close notice) into the client id.
/// Same layout as `UnbindMeta` (`IdLen:16be | ClientId`); the opcode is
/// what distinguishes a will-firing close from a will-suppressing one.
fn decode_disconnect_meta(meta: &[u8]) -> Option<String> {
    decode_unbind_meta(meta)
}

/// Handle one ungraceful close notice from the edge (F1-01): a socket
/// that closed without `DISCONNECT` (TCP close/reset, keepalive expiry,
/// protocol error). The edge detects the close; the session lives here,
/// so the decision lives here too: detach exactly like an unbind, then
/// publish the taken will once through the normal ingress pipeline
/// (retained store, rules, fan-out, cluster forward), honouring its QoS
/// and retain bit. Returns the will deliveries for the caller to route,
/// mirroring [`apply_publish`].
///
/// Exactly-once: the will is atomically taken, so a racing unbind (clean
/// close) or a second notice for the same connection observes `None`.
/// Stale notices for a superseded conn_id are ignored entirely (no
/// detach, no publish). Disconnect events only; never on the publish or
/// delivery hot path.
///
/// TODO(parity): the will publish enforces bans, the publish ACL and the
/// webhook verdict but skips the publish rate quota (a single disconnect
/// signal is not a rate). Whether the reference authorizes the will at
/// CONNECT time, at publish time, or not at all is still open; current
/// choice fails closed on bans/ACL/webhook. Gateway (CoAP) and
/// cluster-forwarded publishes carry no MQTT credential context, so they
/// stay on the local ACL: they were authorized at their ingress node and
/// re-asking the webhook without a client identity would invent a publish
/// context the spec does not define.
/// Handle one version-5 DISCONNECT with reason 0x04 arriving as an
/// unbind frame (X1-03): strip the trailing reason byte and run the
/// ungraceful-close path so the stored will publishes once. Every other
/// unbind reason never reaches here (clean suppress path in the caller).
/// Disconnect event only; never on the publish or delivery hot path.
async fn apply_disconnect_with_unbind_meta(
    frame: &BrokerFrame,
    shared: &Shared,
) -> Vec<(u64, BrokerFrame)> {
    let Some((client_id, _)) = decode_unbind_meta_full(&frame.metadata) else {
        shared.conns.unregister(frame.header.conn_id);
        return Vec::new();
    };
    let mut stripped = Vec::with_capacity(2 + client_id.len());
    stripped.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    stripped.extend_from_slice(client_id.as_bytes());
    let owned = BrokerFrame::new(
        OpCode::DisconnectIn,
        frame.header.conn_id,
        frame.header.sequence_no,
        Bytes::from(stripped),
        Bytes::new(),
    );
    match owned {
        Ok(stripped_frame) => apply_disconnect(&stripped_frame, shared).await,
        Err(_) => {
            shared.conns.unregister(frame.header.conn_id);
            Vec::new()
        }
    }
}

async fn apply_disconnect(frame: &BrokerFrame, shared: &Shared) -> Vec<(u64, BrokerFrame)> {
    let Some(client_id) = decode_disconnect_meta(&frame.metadata) else {
        shared.conns.unregister(frame.header.conn_id);
        return Vec::new();
    };
    // Owner session first (MT-03): the connection index identifies the
    // owning `(tenant, client id)` even when two tenants share the client
    // id, so an ungraceful close detaches and fires the will in the right
    // tenant. Falls back to the default-tenant record for unindexed
    // connections. A decoded id that does not own this connection is
    // stale (rebound conn): ignored entirely, like a superseded conn_id.
    let Some(session) = shared
        .sessions
        .session_for_conn(frame.header.conn_id)
        .filter(|session| session.client_id == client_id)
        .or_else(|| shared.sessions.get(&client_id))
    else {
        shared.conns.unregister(frame.header.conn_id);
        return Vec::new();
    };
    if *session.conn_id.read() != Some(frame.header.conn_id) {
        return Vec::new();
    }
    // Capture the publish-time identity before detaching (the detach
    // clears the binding the checks below read).
    let username = session.username.read().clone();
    let verified_username = session.verified_username.read().clone();
    let peerhost = session.peerhost.read().clone();
    let tenant = session.tenant_id.read().clone();
    let will = session.take_last_will();
    let swept = shared.sessions.unbind_connection_in_tenant(
        &tenant,
        &session.client_id,
        frame.header.conn_id,
    );
    for filter in &swept {
        unsubscribe_scoped_in_tenant(shared, &tenant, &session.client_id, filter);
    }
    refresh_subscription_stats(shared);
    shared.conns.unregister(frame.header.conn_id);
    // Fire the disconnect event. Rules belong to the default tenant.
    // The edge forwards no reason code, so the code stays absent.
    // Disconnect path only, never publish or deliver.
    if shared
        .engine
        .is_event_source_selected(broker_rules::EventSource::ClientDisconnected)
        && is_default_tenant(&tenant)
    {
        shared
            .engine
            .fire_client_disconnected(broker_rules::ClientDisconnectedEvent {
                client_id: client_id.clone(),
                username: username.clone(),
                peerhost: peerhost.clone(),
                reason: "tcp_close".to_string(),
                disconnect_code: None,
            });
    }
    let Some(will) = will else {
        return Vec::new();
    };
    publish_session_last_will(
        shared,
        &session,
        &tenant,
        username.as_deref(),
        verified_username.as_deref(),
        peerhost.as_deref(),
        will,
    )
    .await
}

async fn publish_session_last_will(
    shared: &Shared,
    session: &Arc<broker_session::Session>,
    tenant: &str,
    username: Option<&str>,
    verified_username: Option<&str>,
    peerhost: Option<&str>,
    will: broker_session::StoredWill,
) -> Vec<(u64, BrokerFrame)> {
    let client_id = session.client_id.as_str();
    info!(
        "Publishing last will for {} on {}",
        client_id,
        will.topic.as_str()
    );
    if shared
        .bans
        .is_banned_in_tenant(tenant, client_id, username, peerhost)
    {
        return Vec::new();
    }
    let cached_will = cached_authz_lookup(
        shared,
        session,
        "publish",
        will.topic.as_str(),
        u8::from(will.qos),
        will.retain,
    );
    let will_allowed = match cached_will {
        Some(allow) => allow,
        None => shared
            .auth
            .authorize_publish_in_tenant_as(tenant, client_id, verified_username, &will.topic)
            .await
            .is_ok(),
    };
    if cached_will.is_none() {
        record_authz_under_policy(
            shared,
            session,
            "publish",
            will.topic.as_str(),
            u8::from(will.qos),
            will.retain,
            will_allowed,
        );
    }
    if !will_allowed || !session_credential_allows(session, will.topic.as_str(), true) {
        return Vec::new();
    }
    if let Some(db) = shared.db_auth.as_ref().filter(|d| d.is_configured()) {
        let is_local = username.is_some_and(|u| shared.auth.has_user_in_tenant(tenant, u));
        if !is_local {
            let key = username.unwrap_or(client_id);
            if db
                .authorize_publish_in_tenant(tenant, key, &will.topic)
                .await
                .is_err()
            {
                return Vec::new();
            }
        }
    }
    if let Some(webhook) = shared.webhook.as_ref() {
        if webhook
            .authorize_publish_in_tenant(tenant, client_id, &will.topic)
            .await
            .is_err()
        {
            tracing::warn!(
                "webhook authorization unavailable for last will of {client_id}: failing closed"
            );
            return Vec::new();
        }
    }
    let deliveries = ingress_pipeline_with_publisher(
        shared,
        &will.topic,
        will.qos,
        will.retain,
        &will.payload,
        Some(client_id),
        tenant,
    )
    .await;
    forward_cluster(shared, &will.topic, will.qos, &will.payload, tenant).await;
    deliveries
}

/// Release one unacknowledged QoS 1 downlink on the subscriber's PUBACK
/// (T-31). The edge reports the downlink packet id via `PubAckIn`;
/// ownership resolves through the `conn_id -> client_id` index, so acks
/// from a superseded connection never release a new incarnation's entry.
///
/// FX-02: the PUBACK event also drives slow-subscription recording. The
/// held entry carries its delivery instant, so ack latency
/// (`now - enqueued_at`) is the measured delivery time compared against
/// the configured threshold. Slow acks hand one bounded record off
/// through `slow_tx` (`try_send`, counted drop when full); prompt acks
/// pay two relaxed atomic loads and return. Ack path only: the publish
/// fast path never touches this.
/// TODO(parity): QoS 2 PUBREC/PUBCOMP completions are not timed yet;
/// should the second-phase latency record the same way?
fn apply_puback(frame: &BrokerFrame, shared: &Shared) {
    let packet_id = match decode_puback_meta(&frame.metadata) {
        Some(packet_id) => packet_id,
        None => return,
    };
    // Owner session first (MT-03), legacy lookup behind: an ack from
    // one tenant's connection never releases another tenant's entry.
    let session = shared
        .sessions
        .session_for_conn(frame.header.conn_id)
        .or_else(|| {
            shared
                .sessions
                .client_id_for_conn(frame.header.conn_id)
                .as_deref()
                .and_then(|p| shared.sessions.get(p))
        });
    let Some(session) = session else {
        return;
    };
    let Some(entry) = session.ack_inflight_timed(packet_id) else {
        return;
    };
    maybe_resume_replay_on_ack(shared, &session, frame.header.conn_id);
    // Fire the ack event. Rules belong to the default tenant.
    // Check flag before any lock. Ack path only, never publish.
    if shared
        .engine
        .is_event_source_selected(broker_rules::EventSource::MessageAcked)
        && session.tenant_is_default.load(Ordering::Relaxed)
    {
        let username = session.username.read().clone();
        let peerhost = session.peerhost.read().clone();
        shared
            .engine
            .fire_message_acked(broker_rules::MessageAckedEvent {
                client_id: session.client_id.clone(),
                username,
                peerhost,
                topic: entry.topic.to_string(),
                qos: u8::from(entry.qos),
                msg_id: packet_id,
            });
    }
    // Honour `enable=false`: recording is gated on the flag the API
    // exposes, read atomically with no lock.
    if !shared.slow_subs_settings.is_enabled() {
        return;
    }
    let elapsed_ms = entry
        .enqueued_at
        .elapsed()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64;
    // Timespan is whole milliseconds: ceil sub-millisecond acks to 1 ms
    // so a real ack never invents a zero latency.
    let timespan_ms = elapsed_ms.max(1);
    // Honour the configured threshold read atomically: latencies
    // shorter than it acked promptly and are not slow.
    if timespan_ms < shared.slow_subs_settings.threshold_ms() {
        return;
    }
    let topic = truncate_to_chars(entry.topic.as_str(), SLOW_TOPIC_CAP_CHARS).to_string();
    let record = SlowRecord {
        conn_id: frame.header.conn_id,
        topic,
        timespan_ms,
    };
    if shared.slow_tx.try_send(record).is_err() {
        shared.slow_dropped.fetch_add(1, Ordering::Relaxed);
    }
}

/// Decode `PubAckIn` metadata into the downlink packet id. Layout
/// `PacketId:16be | RC:8` (mirrors `indra_brokerlink:encode_puback_meta`);
/// the return code is ignored and trailing bytes tolerated, but the id
/// must be nonzero.
fn decode_puback_meta(meta: &[u8]) -> Option<u16> {
    if meta.len() < 2 {
        return None;
    }
    let packet_id = u16::from_be_bytes([meta[0], meta[1]]);
    if packet_id == 0 {
        return None;
    }
    Some(packet_id)
}

/// One decoded subscription request (X1-03): the filter plus the raw
/// v5 options byte and subscription identifier. `has_options` is true
/// only for the extended form (options byte plus 4-byte identifier);
/// the legacy form carries a plain QoS byte with identifier 0. An
/// invalid options byte (reserved bits, QoS > 2, retain-handling > 2)
/// or an over-maximum identifier is kept here and failed per-filter by
/// the caller, never as a whole-packet malformed frame.
#[derive(Debug, Clone)]
struct SubscribeEntry {
    filter: String,
    opts_raw: u8,
    subscription_id: u32,
    has_options: bool,
}

/// Decoded `(packet_id, client_id, [entries])` subscription metadata.
type SubscribeMeta = (u16, String, Vec<SubscribeEntry>);

/// v5 SUBACK reason codes carried per filter (X1-03): granted QoS 0-2,
/// unspecified error, not authorized, topic filter invalid and quota
/// exceeded. Version-4 peers only ever observe 0-2 and 0x80; the wider
/// set rides the same byte on version-5 bindings. A malformed filter or
/// option byte answers 0x8F (topic filter invalid), failing only the
/// affected filter.
const V5_SUBACK_UNSPECIFIED: u8 = 0x80;
const V5_SUBACK_NOT_AUTHORIZED: u8 = 0x87;
const V5_SUBACK_FILTER_INVALID: u8 = 0x8F;
const V5_SUBACK_QUOTA_EXCEEDED: u8 = 0x97;

/// `ConnClose` reason for an authorization disconnect (AP-11): 0x87 Not
/// Authorized. Reason: the documented MQTT reason code for a refused
/// publish or subscribe; the edge forwards it as DISCONNECT 0x87 to
/// version-5 clients and closes older sockets with no packet.
const AUTHZ_DISCONNECT_RC: u8 = 0x87;

/// v5 UNSUBACK reason codes carried per filter: success, no
/// subscription existed and topic filter invalid. Version-4 peers
/// observe the packet identifier only. A malformed filter answers
/// 0x8F, failing only the affected filter.
const V5_UNSUBACK_SUCCESS: u8 = 0x00;
const V5_UNSUBACK_NO_SUBSCRIPTION: u8 = 0x11;
const V5_UNSUBACK_FILTER_INVALID: u8 = 0x8F;

/// Decode `SubscribeMeta` into `(packet_id, client_id, [entries])`.
/// Accepts the legacy form (`FilterLen | Filter | QoS`) and the X1-03
/// extended form (`FilterLen | Filter | Opts | SubId:32be`) per filter.
/// The extended form is detected by exact length: `N` extended entries
/// consume 4 more bytes each than the legacy form. Structural truncation
/// or bad UTF-8 fails the whole frame (`None`); per-filter option
/// validity is left to the caller.
fn decode_subscribe_meta(meta: &[u8]) -> Option<SubscribeMeta> {
    if meta.len() < 6 {
        return None;
    }
    let packet_id = u16::from_be_bytes([meta[0], meta[1]]);
    if packet_id == 0 {
        return None;
    }
    let id_len = u16::from_be_bytes([meta[2], meta[3]]) as usize;
    if meta.len() < 4 + id_len + 2 {
        return None;
    }
    let client_id = std::str::from_utf8(&meta[4..4 + id_len]).ok()?.to_string();
    let mut cursor = 4 + id_len;
    let count = u16::from_be_bytes([meta[cursor], meta[cursor + 1]]) as usize;
    cursor += 2;
    // Try the extended form first when the length can hold it; fall back
    // to the legacy form otherwise. Both consume exactly on success.
    if let Some(entries) = decode_subscribe_entries_v5(meta, cursor, count) {
        return Some((packet_id, client_id, entries));
    }
    let mut subs = Vec::with_capacity(count);
    for _ in 0..count {
        if meta.len() < cursor + 3 {
            return None;
        }
        let filter_len = u16::from_be_bytes([meta[cursor], meta[cursor + 1]]) as usize;
        if filter_len == 0 || meta.len() < cursor + 2 + filter_len + 1 {
            return None;
        }
        let filter = std::str::from_utf8(&meta[cursor + 2..cursor + 2 + filter_len])
            .ok()?
            .to_string();
        let qos = meta[cursor + 2 + filter_len];
        subs.push(SubscribeEntry {
            filter,
            opts_raw: qos,
            subscription_id: 0,
            has_options: false,
        });
        cursor += 2 + filter_len + 1;
    }
    if cursor != meta.len() {
        return None;
    }
    Some((packet_id, client_id, subs))
}

/// Decode `count` extended entries (`FilterLen | Filter | Opts |
/// SubId:32be`) from `meta` at `cursor`. `None` when the bytes do not
/// frame exactly as the extended form (caller falls back to legacy).
fn decode_subscribe_entries_v5(
    meta: &[u8],
    mut cursor: usize,
    count: usize,
) -> Option<Vec<SubscribeEntry>> {
    let mut subs = Vec::with_capacity(count);
    for _ in 0..count {
        if meta.len() < cursor + 2 + 1 + 4 {
            return None;
        }
        let filter_len = u16::from_be_bytes([meta[cursor], meta[cursor + 1]]) as usize;
        if filter_len == 0 || meta.len() < cursor + 2 + filter_len + 1 + 4 {
            return None;
        }
        let filter = std::str::from_utf8(&meta[cursor + 2..cursor + 2 + filter_len])
            .ok()?
            .to_string();
        let opts = meta[cursor + 2 + filter_len];
        let base = cursor + 2 + filter_len + 1;
        let sub_id =
            u32::from_be_bytes([meta[base], meta[base + 1], meta[base + 2], meta[base + 3]]);
        subs.push(SubscribeEntry {
            filter,
            opts_raw: opts,
            subscription_id: sub_id,
            has_options: true,
        });
        cursor = base + 4;
    }
    if cursor != meta.len() {
        return None;
    }
    Some(subs)
}

/// Decode `UnsubscribeMeta` into `(packet_id, client_id, [filters])`.
/// Layout: `PacketId:16be | IdLen:16be | ClientId | N:16be |
/// (FilterLen:16be | Filter) * N`. Structural truncation, bad UTF-8,
/// an empty filter or an empty list fails the frame (`None`).
fn decode_unsubscribe_meta(meta: &[u8]) -> Option<(u16, String, Vec<String>)> {
    if meta.len() < 6 {
        return None;
    }
    let packet_id = u16::from_be_bytes([meta[0], meta[1]]);
    if packet_id == 0 {
        return None;
    }
    let id_len = u16::from_be_bytes([meta[2], meta[3]]) as usize;
    if meta.len() < 4 + id_len + 2 {
        return None;
    }
    let client_id = std::str::from_utf8(&meta[4..4 + id_len]).ok()?.to_string();
    let mut cursor = 4 + id_len;
    let count = u16::from_be_bytes([meta[cursor], meta[cursor + 1]]) as usize;
    cursor += 2;
    if count == 0 {
        return None;
    }
    let mut filters = Vec::with_capacity(count);
    for _ in 0..count {
        if meta.len() < cursor + 2 {
            return None;
        }
        let filter_len = u16::from_be_bytes([meta[cursor], meta[cursor + 1]]) as usize;
        if filter_len == 0 || meta.len() < cursor + 2 + filter_len {
            return None;
        }
        let filter = std::str::from_utf8(&meta[cursor + 2..cursor + 2 + filter_len])
            .ok()?
            .to_string();
        filters.push(filter);
        cursor += 2 + filter_len;
    }
    if cursor != meta.len() {
        return None;
    }
    Some((packet_id, client_id, filters))
}

/// Encode one extended subscription entry for tests (mirror of the edge
/// `encode_subscribe_meta` v5 contract).
#[cfg(test)]
fn encode_subscribe_meta_v5(packet_id: u16, client_id: &str, subs: &[(&str, u8, u32)]) -> Bytes {
    let id = client_id.as_bytes();
    let mut meta = Vec::new();
    meta.extend_from_slice(&packet_id.to_be_bytes());
    meta.extend_from_slice(&(id.len() as u16).to_be_bytes());
    meta.extend_from_slice(id);
    meta.extend_from_slice(&(subs.len() as u16).to_be_bytes());
    for (filter, opts, sub_id) in subs {
        meta.extend_from_slice(&(filter.len() as u16).to_be_bytes());
        meta.extend_from_slice(filter.as_bytes());
        meta.push(*opts);
        meta.extend_from_slice(&sub_id.to_be_bytes());
    }
    Bytes::from(meta)
}

/// Decode `PublishMeta` into `(topic, packet_id, qos, retain, alias,
/// alias_present)`.
///
/// Layout: `TopicLen:16be | Topic | PacketId:16be | QoS:8 | Retain:8 |
/// Dup:8`, optionally followed by the B4-05 alias section `Alias:16be`.
/// `alias_present` is true only when the trailing section is on the wire;
/// absent (pre-alias encoding) decodes as `(0, false)` and means "no alias
/// carried". An explicit `Alias:16be = 0` section decodes as `(0, true)`
/// and is never valid on the wire (MQTT 5.0 §3.3.2.3.4: alias 0 is a
/// protocol error, DISCONNECT 0x94). A zero-length topic is accepted only
/// when the alias section is present (alias-by-reference); otherwise the
/// topic must be non-empty as before.
/// Largest v5 publish user-properties section accepted on the
/// BrokerLink publish path (X1-03, bytes). Reason: one publish section
/// must leave room for the topic, ids and alias inside the 65535-byte
/// BrokerLink meta cap; 4 KiB covers realistic inbound properties while
/// failing oversized blocks closed instead of routing half a publish.
/// At most 16 pairs (mirrors the edge `MAX_V5_USER_PROPS` bound).
const MAX_PUBLISH_V5_SECTION_LEN: usize = 4096;
const MAX_PUBLISH_V5_USERS: usize = 16;
const MAX_PUBLISH_V5_STRING: usize = 1024;

/// X1-03 inbound v5 publish properties carried toward delivery.
/// `format` is the payload-format indicator (0|1), `expiry` the
/// message-expiry interval (seconds, carried without enforcement: the
/// task scope is decode-and-forward), and `users` the bounded
/// user-property pairs.
/// Reason: the specification requires decode/forward of these
/// properties; they ride the `PublishOut` meta trailing section so a
/// version-5 subscriber observes what the publisher sent, while
/// version-4 deliveries omit them (bytes identical).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct PublishV5Props {
    format: u8,
    expiry: u32,
    users: Vec<(Vec<u8>, Vec<u8>)>,
}

impl PublishV5Props {
    fn is_empty(&self) -> bool {
        self.format == 0 && self.expiry == 0 && self.users.is_empty()
    }
}

fn decode_publish_meta(meta: &[u8]) -> Option<(String, u16, u8, bool, u16, bool, PublishV5Props)> {
    if meta.len() < 2 + 2 + 3 {
        return None;
    }
    let topic_len = u16::from_be_bytes([meta[0], meta[1]]) as usize;
    let old_len = 2 + topic_len + 2 + 3;
    let new_len = old_len + 2;
    // X1-03: the edge v5 form appends `Format:8 | Expiry:32be |
    // UserCount:16be | pairs' after the alias section. The kernel
    // validates the section (fail closed on truncation or over-bound
    // values) and routes on topic/qos/retain/alias exactly as before;
    // inbound user properties are parsed and forwarded to version-5
    // deliveries (never for routing). Message-expiry is carried
    // without enforcement (no timer drop): the task scope is
    // decode-and-forward.
    let v5 = if meta.len() != old_len && meta.len() != new_len && meta.len() > new_len {
        decode_publish_v5_section(meta, topic_len)?
    } else if meta.len() != old_len && meta.len() != new_len {
        return None;
    } else {
        PublishV5Props::default()
    };
    if meta.len() < 2 + topic_len + 2 + 3 {
        return None;
    }
    let topic = if topic_len == 0 {
        String::new()
    } else {
        std::str::from_utf8(&meta[2..2 + topic_len])
            .ok()?
            .to_string()
    };
    let base = 2 + topic_len;
    let packet_id = u16::from_be_bytes([meta[base], meta[base + 1]]);
    let qos = meta[base + 2];
    let retain = meta[base + 3] != 0;
    let (raw_alias, raw_present) = if meta.len() >= new_len {
        let raw = [meta[base + 5], meta[base + 6]];
        (
            protocol_v5::decode_topic_alias(&raw).unwrap_or(protocol_v5::NO_TOPIC_ALIAS),
            true,
        )
    } else {
        (protocol_v5::NO_TOPIC_ALIAS, false)
    };
    // X1-03 v5 form always carries the alias field.
    // Alias 0 with a topic means absent here.
    // Route it on the fast path with no table.
    // Keep explicit 0 on the /6 form present.
    // The caller then rejects it with 0x94.
    let has_v5_section = meta.len() > new_len;
    let (alias, alias_present) = if raw_present
        && raw_alias == protocol_v5::NO_TOPIC_ALIAS
        && has_v5_section
        && topic_len != 0
    {
        (protocol_v5::NO_TOPIC_ALIAS, false)
    } else {
        (raw_alias, raw_present)
    };
    if topic_len == 0 && !alias_present {
        return None;
    }
    Some((topic, packet_id, qos, retain, alias, alias_present, v5))
}

/// Parse one X1-03 v5 publish trailing section (`Alias:16be |
/// Format:8 | Expiry:32be | UserCount:16be | pairs') after the fixed
/// publish head. `None` on any truncation or over-bound value.
fn decode_publish_v5_section(meta: &[u8], topic_len: usize) -> Option<PublishV5Props> {
    let old_len = 2 + topic_len + 2 + 3;
    if meta.len() < old_len + 2 + 1 + 4 + 2 {
        return None;
    }
    if meta.len() > old_len + 2 + MAX_PUBLISH_V5_SECTION_LEN {
        return None;
    }
    let base = 2 + topic_len;
    let format = meta[base + 7];
    if format != 0 && format != 1 {
        return None;
    }
    let expiry = u32::from_be_bytes([
        meta[base + 8],
        meta[base + 9],
        meta[base + 10],
        meta[base + 11],
    ]);
    let user_count = u16::from_be_bytes([meta[base + 12], meta[base + 13]]) as usize;
    if user_count > MAX_PUBLISH_V5_USERS {
        return None;
    }
    let mut cursor = base + 14;
    let mut users = Vec::with_capacity(user_count);
    for _ in 0..user_count {
        if meta.len() < cursor + 2 {
            return None;
        }
        let k_len = u16::from_be_bytes([meta[cursor], meta[cursor + 1]]) as usize;
        if k_len == 0 || k_len > MAX_PUBLISH_V5_STRING || meta.len() < cursor + 2 + k_len + 2 {
            return None;
        }
        let key = meta[cursor + 2..cursor + 2 + k_len].to_vec();
        if std::str::from_utf8(&key).is_err() {
            return None;
        }
        cursor += 2 + k_len;
        let v_len = u16::from_be_bytes([meta[cursor], meta[cursor + 1]]) as usize;
        if v_len > MAX_PUBLISH_V5_STRING || meta.len() < cursor + 2 + v_len {
            return None;
        }
        let value = meta[cursor + 2..cursor + 2 + v_len].to_vec();
        if std::str::from_utf8(&value).is_err() {
            return None;
        }
        users.push((key, value));
        cursor += 2 + v_len;
    }
    if cursor != meta.len() {
        return None;
    }
    Some(PublishV5Props {
        format,
        expiry,
        users,
    })
}

/// Outcome of one inbound alias resolution (B4-05).
enum InboundAliasOutcome {
    /// Effective topic to route.
    Topic(String),
    /// The alias is unusable: the caller answers 0x94 (where it has a
    /// channel) and orders a DISCONNECT via [`order_alias_disconnect`].
    Reject,
}

/// Order a DISCONNECT for one inbound topic-alias protocol error (B4-05,
/// MQTT 5.0 §3.3.2.3.4: explicit alias 0, alias above the
/// CONNACK-negotiated maximum, or empty-topic alias with no prior
/// mapping; reason code 0x94 Topic Alias Invalid).
///
/// Best-effort and idempotent: routes a `ConnClose` frame to the edge so
/// the socket closes (the 3.1.1 edge closes without a wire reason code;
/// a version-5 edge sends DISCONNECT 0x94 first from the carried reason),
/// then detaches the session exactly like the management kick path
/// (verified unbind plus router sweep) so the faulty connection holds no
/// state. Unknown connections only count a drop inside `ConnTable::route`;
/// PUBLISH-event only, never on the per-message fast path (rejects only).
/// The close reason is honestly 0x94 (never a reason string; the edge
/// omits it when the broker has nothing to say).
fn order_alias_disconnect(shared: &Shared, conn_id: u64) {
    if let Ok(close) = BrokerFrame::new(
        OpCode::ConnClose,
        conn_id,
        0,
        Bytes::from(vec![protocol_v5::REASON_TOPIC_ALIAS_INVALID]),
        Bytes::new(),
    ) {
        let _ = shared.conns.order_close(conn_id, close);
    }
    // Owner key first (MT-03), decoded id behind: a faulty connection in
    // one tenant detaches and sweeps only its own tenant, never another
    // tenant's namesake session.
    match shared.sessions.key_for_conn(conn_id) {
        Some(key) => {
            let swept =
                shared
                    .sessions
                    .unbind_connection_in_tenant(&key.tenant, &key.client_id, conn_id);
            for filter in &swept {
                unsubscribe_scoped_in_tenant(shared, &key.tenant, &key.client_id, filter);
            }
        }
        None => {
            if let Some(client_id) = shared.sessions.client_id_for_conn(conn_id) {
                let swept = shared.sessions.unbind_connection(&client_id, conn_id);
                for filter in &swept {
                    unsubscribe_scoped(shared, &client_id, filter);
                }
            }
        }
    }
}

/// Order a DISCONNECT for one denied publish or subscribe (AP-11, stored
/// `deny_action: disconnect`).
///
/// Look up a cached authorization verdict for one operation.
/// Returns `None` when caching is off, the topic is excluded, the TTL
/// does not parse, or no fresh entry exists. Reads the stored cache
/// policy (`enable`, `max_size`, `ttl`, `excludes`) with one lock-free
/// load. One short per-session lock for the lookup only.
fn cached_authz_lookup(
    shared: &Shared,
    session: &broker_session::Session,
    action: &str,
    topic: &str,
    qos: u8,
    retain: bool,
) -> Option<bool> {
    // The webhook keeps its own TTL. Skip the session cache for
    // publishes while the webhook is on, so the broker asks again.
    if action == "publish" && shared.webhook.is_some() {
        return None;
    }
    let snap = shared.authz_settings.snapshot();
    if !snap.cache.enable {
        return None;
    }
    if broker_session::Session::authz_topic_excluded(topic, &snap.cache.excludes) {
        return None;
    }
    let ttl_ms = broker_config::parse_settings_duration_to_ms(&snap.cache.ttl)?;
    session.cached_authz_verdict(action, topic, qos, retain, ttl_ms)
}

/// Record one authorization verdict under the stored cache policy.
/// Skips excluded topics and a disabled cache. Trims the per-client
/// cache to the stored `max_size`. One bounded per-session insert.
fn record_authz_under_policy(
    shared: &Shared,
    session: &broker_session::Session,
    action: &str,
    topic: &str,
    qos: u8,
    retain: bool,
    allow: bool,
) {
    // Do not store publishes while the webhook is on. The broker
    // asks the webhook again after its TTL ends.
    if action == "publish" && shared.webhook.is_some() {
        return;
    }
    let snap = shared.authz_settings.snapshot();
    if !snap.cache.enable {
        return;
    }
    if broker_session::Session::authz_topic_excluded(topic, &snap.cache.excludes) {
        return;
    }
    session.record_authz_decision(action, topic, qos, retain, allow);
    session.trim_authz_cache_to(snap.cache.max_size);
}

/// Best-effort and idempotent like [`order_alias_disconnect`]: routes a
/// `ConnClose` frame carrying 0x87 so the edge closes the socket (a
/// version-5 edge sends DISCONNECT 0x87 first), then detaches the session
/// exactly like the management kick path so the denied connection holds
/// no state. Unknown connections only count a drop inside
/// `ConnTable::route`. Denial events only, never on the per-message fast
/// path (allowed messages take no branch here).
fn order_authz_disconnect(shared: &Shared, conn_id: u64) {
    if let Ok(close) = BrokerFrame::new(
        OpCode::ConnClose,
        conn_id,
        0,
        Bytes::from(vec![AUTHZ_DISCONNECT_RC]),
        Bytes::new(),
    ) {
        let _ = shared.conns.order_close(conn_id, close);
    }
    // Owner key first (MT-03), decoded id behind: a denied connection in
    // one tenant detaches and sweeps only its own tenant, never another
    // tenant's namesake session.
    match shared.sessions.key_for_conn(conn_id) {
        Some(key) => {
            let swept =
                shared
                    .sessions
                    .unbind_connection_in_tenant(&key.tenant, &key.client_id, conn_id);
            for filter in &swept {
                unsubscribe_scoped_in_tenant(shared, &key.tenant, &key.client_id, filter);
            }
        }
        None => {
            if let Some(client_id) = shared.sessions.client_id_for_conn(conn_id) {
                let swept = shared.sessions.unbind_connection(&client_id, conn_id);
                for filter in &swept {
                    unsubscribe_scoped(shared, &client_id, filter);
                }
            }
        }
    }
}

/// Closes a connection that sends a PUBLISH while no session is bound to
/// it. Nothing is routed. A connection can reach this state when a
/// refused CONNECT or an earlier denial removed its session while more
/// packets were still buffered. Fail closed: the close order carries the
/// not-authorized reason. Rejects only, never on an allowed message.
fn order_unbound_close(shared: &Shared, conn_id: u64) {
    if let Ok(close) = BrokerFrame::new(
        OpCode::ConnClose,
        conn_id,
        0,
        Bytes::from(vec![AUTHZ_DISCONNECT_RC]),
        Bytes::new(),
    ) {
        let _ = shared.conns.order_close(conn_id, close);
    }
}

/// Resolve one inbound alias use to its effective topic.
///
/// * topic present (`raw` non-empty): validate the concrete topic, then
///   register `alias -> topic` on the publisher session (overwrites).
/// * topic absent (`raw` empty): resolve a previously registered alias.
/// * unknown connection, explicit alias 0, alias above the
///   session maximum, or unknown alias: `Reject` (fail closed, caller
///   disconnects with 0x94).
///
/// PUBLISH event only; the session table write is one bounded index
/// store under a single write lock.
fn resolve_inbound_alias_topic(
    shared: &Shared,
    conn_id: u64,
    raw: &str,
    alias: u16,
) -> InboundAliasOutcome {
    // Explicit alias 0 is never valid on the wire.
    // Absence never reaches this function.
    // The caller uses the fast path when alias is absent.
    // Reject so the caller answers 0x94 and disconnects.
    if alias == protocol_v5::NO_TOPIC_ALIAS {
        return InboundAliasOutcome::Reject;
    }
    // Owner session first (MT-03), legacy lookup behind: alias tables are
    // per session, so a non-default tenant's aliases register and resolve
    // on its own session, never on the default-tenant namesake.
    let session = shared.sessions.session_for_conn(conn_id).or_else(|| {
        shared
            .sessions
            .client_id_for_conn(conn_id)
            .as_deref()
            .and_then(|client_id| shared.sessions.get(client_id))
    });
    let session = match session {
        Some(session) => session,
        None => return InboundAliasOutcome::Reject,
    };
    let max = session.inbound_alias_max();
    if protocol_v5::check_inbound_alias(alias, max).is_err()
        || !protocol_v5::alias_in_range(alias, max)
    {
        return InboundAliasOutcome::Reject;
    }
    if !raw.is_empty() {
        // Concrete topics never carry wildcards; validate before storing
        // so the table only ever holds routable names.
        let Ok(topic) = Topic::new(raw.to_string()) else {
            return InboundAliasOutcome::Reject;
        };
        match session.register_inbound_alias(alias, topic.clone()) {
            Ok(()) => InboundAliasOutcome::Topic(topic.as_str().to_string()),
            Err(_) => InboundAliasOutcome::Reject,
        }
    } else {
        match session.resolve_inbound_alias(alias) {
            Some(topic) => InboundAliasOutcome::Topic(topic.as_str().to_string()),
            None => InboundAliasOutcome::Reject,
        }
    }
}

/// Read the outbound alias out of a `PublishOut` downlink frame.
/// Pre-alias frames (old length) read as alias 0. Read by the edge on
/// delivery, verified by the kernel write path (`encode_publish_out_with_dup`
/// round-trips it back here), and by the B4-05 tests. X1-03 frames carry
/// a trailing subscription identifier (`old_len + 2 + 4`) and read the
/// same alias bytes; X1-03 forwarded-property frames carry a further
/// v5 section and read the same alias bytes.
fn decode_publish_out_alias(meta: &[u8]) -> Option<u16> {
    if meta.len() < 7 {
        return None;
    }
    let topic_len = u16::from_be_bytes([meta[0], meta[1]]) as usize;
    let old_len = 2 + topic_len + 2 + 3;
    if meta.len() == old_len {
        return Some(0);
    }
    if meta.len() >= old_len + 2 {
        if meta.len() < old_len + 2 {
            return None;
        }
        let base = 2 + topic_len;
        if meta.len() < base + 7 {
            return None;
        }
        return Some(u16::from_be_bytes([meta[base + 5], meta[base + 6]]));
    }
    None
}

/// Read the X1-03 subscription identifier out of a `PublishOut`
/// downlink frame. Pre-alias and alias-only frames read as 0 (absent,
/// so version-4 sockets observe no property). Over-maximum values read
/// as 0 (fail closed, never a stored invalid identifier). Forwarded
/// v5-property frames read the same trailer bytes.
fn decode_publish_out_sub_id(meta: &[u8]) -> Option<u32> {
    if meta.len() < 7 {
        return None;
    }
    let topic_len = u16::from_be_bytes([meta[0], meta[1]]) as usize;
    let old_len = 2 + topic_len + 2 + 3;
    if meta.len() == old_len || meta.len() == old_len + 2 {
        return Some(0);
    }
    if meta.len() >= old_len + 2 + 4 {
        let base = 2 + topic_len;
        let raw = u32::from_be_bytes([
            meta[base + 7],
            meta[base + 8],
            meta[base + 9],
            meta[base + 10],
        ]);
        if raw > broker_router::MAX_SUBSCRIPTION_ID {
            return Some(0);
        }
        return Some(raw);
    }
    None
}

/// Read the X1-03 forwarded v5 properties out of a `PublishOut`
/// downlink frame. Pre-alias, alias-only and alias-plus-sub-id frames
/// read as the default (no properties). Forwarded frames carry
/// `Format:8 | Expiry:32be | UserCount:16be | pairs' after the
/// `SubId:32be` trailer; truncation or over-bound values fail the
/// frame (`None`).
fn decode_publish_out_props(meta: &[u8]) -> Option<PublishV5Props> {
    if meta.len() < 7 {
        return None;
    }
    let topic_len = u16::from_be_bytes([meta[0], meta[1]]) as usize;
    let old_len = 2 + topic_len + 2 + 3;
    if meta.len() == old_len || meta.len() == old_len + 2 || meta.len() == old_len + 2 + 4 {
        return Some(PublishV5Props::default());
    }
    if meta.len() < old_len + 2 + 4 + 1 + 4 + 2 {
        return None;
    }
    if meta.len() > old_len + 2 + 4 + MAX_PUBLISH_V5_SECTION_LEN {
        return None;
    }
    let base = 2 + topic_len;
    let format = meta[base + 11];
    if format != 0 && format != 1 {
        return None;
    }
    let expiry = u32::from_be_bytes([
        meta[base + 12],
        meta[base + 13],
        meta[base + 14],
        meta[base + 15],
    ]);
    let user_count = u16::from_be_bytes([meta[base + 16], meta[base + 17]]) as usize;
    if user_count > MAX_PUBLISH_V5_USERS {
        return None;
    }
    let mut cursor = base + 18;
    let mut users = Vec::with_capacity(user_count);
    for _ in 0..user_count {
        if meta.len() < cursor + 2 {
            return None;
        }
        let k_len = u16::from_be_bytes([meta[cursor], meta[cursor + 1]]) as usize;
        if k_len == 0 || k_len > MAX_PUBLISH_V5_STRING || meta.len() < cursor + 2 + k_len + 2 {
            return None;
        }
        let key = meta[cursor + 2..cursor + 2 + k_len].to_vec();
        if std::str::from_utf8(&key).is_err() {
            return None;
        }
        cursor += 2 + k_len;
        if meta.len() < cursor + 2 {
            return None;
        }
        let v_len = u16::from_be_bytes([meta[cursor], meta[cursor + 1]]) as usize;
        if v_len > MAX_PUBLISH_V5_STRING || meta.len() < cursor + 2 + v_len {
            return None;
        }
        let value = meta[cursor + 2..cursor + 2 + v_len].to_vec();
        if std::str::from_utf8(&value).is_err() {
            return None;
        }
        users.push((key, value));
        cursor += 2 + v_len;
    }
    if cursor != meta.len() {
        return None;
    }
    Some(PublishV5Props {
        format,
        expiry,
        users,
    })
}

/// Decode `UnbindMeta` into the client id.
fn decode_unbind_meta(meta: &[u8]) -> Option<String> {
    decode_unbind_meta_full(meta).map(|(client_id, _)| client_id)
}

/// Decode `UnbindMeta` into `(client_id, v5_reason)`. Layout
/// `IdLen:16be | ClientId` (version-4, bytes unchanged) optionally
/// followed by one v5 DISCONNECT reason byte (`Reason:8`). The reason
/// is `None` for version-4 unbinds; `Some(code)` for version-5
/// DISCONNECTs (0x00 normal, 0x04 disconnect-with-will, others clean).
/// Structural truncation or bad UTF-8 fails the frame (`None`). Reason
/// strings and user properties stay on the edge: the task scope omits
/// them toward the kernel (never invented), so the kernel stores none.
fn decode_unbind_meta_full(meta: &[u8]) -> Option<(String, Option<u8>)> {
    if meta.len() < 2 {
        return None;
    }
    let id_len = u16::from_be_bytes([meta[0], meta[1]]) as usize;
    if meta.len() != 2 + id_len && meta.len() != 2 + id_len + 1 {
        return None;
    }
    let client_id = std::str::from_utf8(&meta[2..2 + id_len]).ok()?.to_string();
    let reason = if meta.len() == 2 + id_len + 1 {
        Some(meta[2 + id_len])
    } else {
        None
    };
    Some((client_id, reason))
}

/// v5 DISCONNECT reason requesting will publication (X1-03): 0x04
/// "Disconnect with Will Message" publishes the stored will like an
/// ungraceful close; every other code (including absent/version-4)
/// suppresses it like a clean DISCONNECT.
const V5_DISCONNECT_WITH_WILL: u8 = 0x04;

/// Decode the v5 DISCONNECT reason out of an unbind frame, if present.
fn decode_unbind_reason(meta: &[u8]) -> Option<u8> {
    decode_unbind_meta_full(meta).and_then(|(_, reason)| reason)
}

/// Waits for the stop signal. Returns on SIGTERM or SIGINT.
#[cfg(unix)]
async fn wait_for_shutdown() {
    let terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
    let interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt());
    match (terminate, interrupt) {
        (Ok(mut terminate), Ok(mut interrupt)) => {
            tokio::select! {
                _ = terminate.recv() => {},
                _ = interrupt.recv() => {},
            }
        }
        (Ok(mut terminate), Err(_)) => {
            let _ = terminate.recv().await;
        }
        (Err(_), Ok(mut interrupt)) => {
            let _ = interrupt.recv().await;
        }
        (Err(_), Err(_)) => {
            std::future::pending::<()>().await;
        }
    }
}

/// Waits for the stop signal. Returns on Ctrl-C.
#[cfg(not(unix))]
async fn wait_for_shutdown() {
    let _ = tokio::signal::ctrl_c().await;
}

/// Stops the broker in order. Stops new accepts, closes clients, flushes state.
///
/// Sends `ConnClose` with reason 0x8B to every registered connection.
/// Version-5 clients get DISCONNECT 0x8B first. Version-4 clients get
/// a plain close. The close is clean, thus no will fires. Waits up to
/// [`SHUTDOWN_GRACE`] for the session count to reach zero, then syncs
/// the offline store directory (appends already fsync per write).
async fn shutdown(shared: &Shared) {
    // Stop new accepts first. Running tasks keep their sockets.
    shared.listener_store.stop_all();
    // Ask every live connection to close through the delivery table.
    for conn_id in shared.conns.connection_ids() {
        if let Ok(frame) = BrokerFrame::new(
            OpCode::ConnClose,
            conn_id,
            0,
            Bytes::from(vec![SHUTDOWN_DISCONNECT_RC]),
            Bytes::new(),
        ) {
            let _ = shared.conns.route(conn_id, frame);
        }
    }
    // Wait for the clients to leave, at most the grace period.
    let deadline = std::time::Instant::now() + SHUTDOWN_GRACE;
    while shared.sessions.connected_count() != 0 {
        if std::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    // Drain each connector buffer so a partial batch reaches its target.
    let _ = tokio::time::timeout(
        SHUTDOWN_CONNECTOR_FLUSH,
        shared.engine.connectors().flush_all(),
    )
    .await;
    // Flush durable state. Each append already flushes and fsyncs, so
    // only the directory entry still needs a sync for renames.
    if let Some(store) = shared.sessions.offline_store() {
        if let Ok(dir) = std::fs::File::open(store.dir()) {
            let _ = dir.sync_all();
        }
    }
    info!("Kernel stopped orderly");
}

/// Start background tasks for one kernel. Pumps peer messages when clustered.
fn start_background_tasks(shared: &Shared) {
    if shared.cluster.is_some() {
        tokio::spawn(run_cluster_inbox(shared.clone()));
    }
    // Source ingress (MV-08): reconcile the stored sources with the
    // running consumers. The task reads one bounded snapshot off the
    // publish path and stops an old consumer before it starts a new one.
    source_runtime::spawn(shared.clone());
}

/// Serve the management REST API on an already-bound listener.
async fn serve_api(listener: tokio::net::TcpListener, shared: Shared) -> std::io::Result<()> {
    // Kernel→edge close channel (W0-26): the API kick sends `ConnClose`
    // here after teardown; the forwarder below routes each frame through
    // the connection directory to the owning edge task. The send is
    // non-blocking and a missing edge only warns in the kick path.
    let (edge_tx, mut edge_rx) = unbounded_channel::<BrokerFrame>();
    let mut state = broker_api::ApiState::new(
        shared.engine.clone(),
        shared.sessions.clone(),
        shared.router.clone(),
        shared.metrics.clone(),
        shared.auth.clone(),
        shared.conns.clone(),
        shared.config.clone(),
        shared.node_id.clone(),
        edge_tx,
    );
    // Share the kernel's gauge-plus-high-water-mark store (W1-25) so the
    // node/global stats reads observe the lifecycle points above instead
    // of an empty per-request copy. One `Arc` clone, no new buffering.
    state.api_https = std::env::var("INDRA_API_HTTPS")
        .ok()
        .is_some_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"));
    state.trust_proxy_proto = std::env::var("INDRA_API_TRUST_PROXY_PROTO")
        .ok()
        .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "0" | "false" | "no"))
        .unwrap_or(true);
    state.stats = shared.stats.clone();
    // Share the kernel's readiness flag (W1-27) so `GET /status` reports
    // the same atomic the kernel marks ready. One `Arc` clone, one load
    // per request, no work on the per-message path.
    state.readiness = shared.readiness.clone();
    // Share the kernel retained store (W1-28) so the single-message read
    // observes the same MQTT ingress state. One `Arc` clone, one short
    // read lock per request, no work on the per-message path.
    state.retained = shared.retained.clone();
    state.retained_subscribe_gate = shared.retained_subscribe_gate.clone();
    // Share the validated retainer settings (W1-28) so management reads
    // and writes observe the same object the kernel ingress enforces.
    // One Arc clone; the persistence hook stays with the store.
    state.retainer_config = shared.retainer_config.clone();
    // Share the auto-subscribe list (W1-39) so validated management
    // writes are visible to the connect hook without a restart.
    // One Arc clone; reads take a short lock and clone at most 20 rows.
    state.auto_subscribe = shared.auto_subscribe.clone();
    // Share the delayed scheduler (AP-04) so the list, detail and
    // removal routes observe the same wheel the publish path schedules
    // on and the driver fires from. One `Arc` clone; management reads
    // take one short wheel lock per request and the publish path pays
    // one atomic load for the switch.
    state.delayed = shared.delayed.clone();
    // Share the ban directory (B1-03) so validated management writes
    // are enforced by the connect and publish checks without a restart.
    // One Arc clone; checks take a short read lock, never a write.
    state.bans = shared.bans.clone();
    // Share the authenticator chain (W2-02) so validated management
    // writes are visible to the CONNECT consult without a restart. One
    // `Arc` clone; CONNECT loads one lock-free snapshot per connect and
    // never takes the chain write lock; publish and deliver never touch
    // it. Seeding already ran inside `ApiState::new`; sharing replaces
    // that fresh instance with the kernel's so no second copy diverges.
    state.authn_chain = shared.authn_chain.clone();
    // Share the node auth cache (W2-03) so validated CONNECT records and
    // management status/reset observe the same entries without a restart.
    // One `Arc` clone; CONNECT takes one short lock per credentialed
    // success, management takes one short lock per request; publish and
    // deliver never touch it. Seeding already ran inside `ApiState::new`;
    // sharing replaces that fresh instance with the kernel's so no
    // second copy diverges.
    state.authn_node_cache = shared.authn_node_cache.clone();
    // Share the authentication settings (W2-05) so validated management
    // writes are visible to the CONNECT consult without a restart. One
    // `Arc` clone; CONNECT loads one lock-free snapshot per connect and
    // never takes the settings write lock; publish and deliver never
    // touch it. Seeding already ran inside `ApiState::new`; sharing
    // replaces that fresh instance with the kernel's so no second copy
    // diverges.
    state.authn_settings = shared.authn_settings.clone();
    // Share the authorization node-cache policy (W2-14) so validated
    // management writes are visible to reads without a restart. One
    // `Arc` clone; management-plane only, never on the delivery path.
    // Seeding already ran inside `ApiState::new`; sharing replaces that
    // fresh instance with the kernel's so no second copy diverges.
    state.authz_node_cache = shared.authz_node_cache.clone();
    // Share the authorization settings (W2-16) so validated management
    // writes are visible to reads without a restart. One `Arc` clone;
    // management-plane only, never on the delivery path. Seeding already
    // ran inside `ApiState::new`; sharing replaces that fresh instance
    // with the kernel's so no second copy diverges.
    state.authz_settings = shared.authz_settings.clone();
    // Share the authorization source registry (W2-17) so validated
    // management writes are visible to reads without a restart. One
    // `Arc` clone; management-plane only, never on the delivery path.
    // Seeding already ran inside `ApiState::new`; sharing replaces that
    // fresh instance with the kernel's so no second copy diverges.
    state.authz_sources = shared.authz_sources.clone();
    // Link the rule table to the shared registry again. `ApiState::new`
    // linked its own instance; without this link a management delete,
    // reorder or update reaches the API copy only, not the authorize
    // path. One `Arc` clone at boot.
    shared
        .auth
        .set_authz_sources(Arc::clone(&shared.authz_sources));
    // Share the management API key directory (W2-25) so validated
    // management mints and revocations are visible to reads without a
    // restart. One `Arc` clone; management-plane only, never on the
    // delivery path. Seeding already ran inside `ApiState::new`;
    // sharing replaces that fresh instance with the kernel's so no
    // second copy diverges.
    state.api_keys = shared.api_keys.clone();
    state.license = shared.license.clone();
    // Share the management audit ring (W2-28) so middleware records and
    // `GET /audit` reads observe the same bounded buffer instead of an
    // empty per-request copy. One `Arc` clone; management-plane only,
    // the publish path never touches it.
    state.audit = shared.audit.clone();
    // Share the SSO backend registry (W2-32) so registry writes are
    // visible to `GET /sso` reads without a restart. One `Arc` clone;
    // management-plane only, never on the delivery path. Seeding already
    // ran inside `ApiState::new`; sharing replaces that fresh instance
    // with the kernel's so no second copy diverges.
    state.sso_backends = shared.sso_backends.clone();
    // Share the source instance directory (W3-26, W3-31) so validated
    // management creates/deletes are visible to `GET /sources` and
    // `GET /sources_summary` reads without a restart. One `Arc` clone;
    // management-plane only, the publish and deliver paths never touch it.
    state.sources = shared.sources.clone();
    // Share the action instance directory (W3-02) so validated
    // management creates/deletes are visible to `GET /actions` reads
    // without a restart. One `Arc` clone; management-plane only, rule
    // evaluation reads a snapshot and the publish and deliver paths
    // never touch it.
    state.actions = shared.actions.clone();
    // Share the connector instance directory so validated management
    // creates/deletes are visible to `GET /connectors` reads without a
    // restart. One `Arc` clone; management-plane only, rule evaluation
    // reads the connector manager and the publish and deliver paths never
    // touch it. Seeding already ran inside `ApiState::new`; sharing
    // replaces that fresh instance with the kernel's so no second copy
    // diverges.
    state.connectors = shared.connectors.clone();
    // Share the rule-engine settings (W3-10) so validated management
    // writes are visible to `GET /rule_engine` reads without a restart
    // and hot-apply to the engine's fire-path snapshot through the
    // subscriber. One `Arc` clone; the fire path loads the engine
    // snapshot per ingress and never takes a management lock.
    // Seeding already ran inside `ApiState::new`; sharing replaces that
    // fresh instance with the kernel's so no second copy diverges.
    state.rule_engine_config = shared.rule_engine_config.clone();
    // Share the transformation chain (W3-32) so validated management
    // writes are visible to `GET /message_transformations` reads without
    // a restart. One `Arc` clone; the ingress pipeline loads the engine
    // snapshot per message and never takes this store lock. Seeding
    // already ran inside `ApiState::new`; sharing replaces that fresh
    // instance with the kernel's so no second copy diverges.
    state.transformations = shared.transformations.clone();
    // Share the schema registry (W3-37) so checked writes show in
    // `GET /schema_registry` reads without a restart. One `Arc` clone.
    // Reads take one short lock per request. No publish path reads this
    // store. Publish and deliver take no lock here.
    state.schema_registry = shared.schema_registry.clone();
    // Share the managed namespaces (TM-02) so validated creates and
    // deletes are visible to reads without a restart. One `Arc` clone.
    // Management-plane only; publish and deliver never touch it.
    state.namespaces = shared.namespaces.clone();
    // Share the licence store and alarms (B2-04) so validated management
    // installs and lifecycle alarms are visible without a restart. One
    // Arc clone each; management-plane only, never on the delivery path.
    state.licence = shared.licence.clone();
    state.alarms = shared.alarms.clone();
    // KC-00: `ApiState::new` raised the stored-kind alarms into its own
    // store before this swap. Raise them again on the shared kernel store
    // so `GET /alarms` sees them through the production entry.
    state.raise_unavailable_stored_kinds();
    // Boot replay MV-01: rebuild the stored action sinks on the shared
    // stores after the swaps above. The build and the wiring report then
    // live in the same stores that the routes and alarms read.
    let _ = broker_api::v5::action_sinks::rebuild_all_action_sinks(
        &state.engine,
        &state.connectors,
        &state.actions,
        &state.alarms,
    );
    // Probe every rebuilt action sink before the API serves.
    // A down target then reads `disconnected` from a real attempt.
    broker_api::v5::action_sinks::probe_action_sinks(&state.engine, &state.actions, |_| true).await;
    // Share the monitor ring (F1-02, T-72) so the kernel sampler and the
    // `GET /monitor` reads observe the same bounded buffer instead of an
    // empty per-request copy. One `Arc` clone; management-plane only, the
    // publish path never touches it.
    state.monitor = shared.monitor.clone();
    // Share the slow-subscription recorder and thresholds (F1-03, T-73) so
    // the egress hook and the `GET/DELETE /slow_subscriptions` reads plus
    // `GET/PUT /slow_subscriptions/settings` observe the same records and
    // thresholds instead of an empty per-request copy. One `Arc` clone
    // each; the publish fast path reads only the lock-free threshold
    // atomic, records hand off through the bounded channel, and the
    // background recorder takes one short store lock per record.
    state.slow_subs = shared.slow_subs.clone();
    state.slow_subs_settings = shared.slow_subs_settings.clone();
    // Share the trace-session registry and packet-tracing flag (F1-04,
    // T-74) so validated management writes are visible to the publish hook
    // without a restart. One `Arc` clone each; the publish fast path reads
    // only the flag atomic plus one short store read lock, and capture
    // appends reuse the store's per-session byte cap.
    state.traces = shared.traces.clone();
    state.tracing = shared.tracing.clone();
    // Share the collect registry (MC-04) so kernel ingress feeds the
    // same slots the `subscribe_collect` tool drains. One `Arc` clone;
    // the publish fast path pays one atomic load when idle.
    state.collectors = shared.collectors.clone();
    // SY-02: share the boot-resolved listeners and startup provenance so
    // `GET /listeners` and `explain` report the running configuration
    // instead of defaults. `None` in tests keeps the defaults behaviour.
    if let Some(listeners) = shared.api_listeners.read().expect("listeners lock").clone() {
        state.listeners = listeners;
    }
    // AP-02: share the live listener directory so the v5 listener routes
    // read the boot configuration plus the running flags the kernel
    // controls, and management writes reach the bound sockets.
    state.listener_store = shared.listener_store.clone();
    // MC-01: share the MCP settings so `POST /mcp` reads the `[mcp]`
    // boot values. One `Arc` clone; the handler takes two atomic loads
    // per request.
    state.mcp = shared.mcp.clone();
    if let Some(startup) = shared
        .api_startup_config
        .read()
        .expect("startup config lock")
        .clone()
    {
        // MC-02: apply the `mcp` section to the management switch.
        // Disabled answers 404 on `POST /mcp`. Read-only lists and runs
        // read tools only.
        let mcp = startup.config().mcp.clone();
        state.mcp.apply(mcp.enabled, mcp.read_only);
        *state.startup_config.write().expect("startup config lock") = Some(startup);
    }
    let conns = shared.conns.clone();
    tokio::spawn(async move {
        while let Some(frame) = edge_rx.recv().await {
            conns.route(frame.header.conn_id, frame);
        }
    });
    broker_api::serve(listener, state).await
}

/// Collect the deprecation warnings for the removed edge settings.
///
/// Read the provenance of the layered startup configuration. Return one
/// message per deprecated setting that a layer sets. The caller logs
/// each message. A test sets the values through a file, a flag
/// layer and an environment layer and checks the returned messages.
fn deprecated_startup_warnings(layered: &LayeredConfig) -> Vec<String> {
    let cfg = layered.config();
    let mut out = Vec::new();
    for (path, native) in [
        ("listeners.tcp.native", cfg.listeners.tcp.native),
        ("listeners.tls.native", cfg.listeners.tls.native),
        ("listeners.ws.native", cfg.listeners.ws.native),
        ("listeners.wss.native", cfg.listeners.wss.native),
    ] {
        if layered.provenance().contains_key(path) {
            if native {
                out.push(format!(
                    "setting `{path}` is deprecated and ignored: the kernel always serves this listener"
                ));
            } else {
                out.push(format!(
                    "setting `{path}` is deprecated: the edge was removed; this listener is served by the kernel"
                ));
            }
        }
    }
    if let Some(layer) = layered.provenance().get("node.brokerlink_bind") {
        if matches!(layer, broker_config::layers::Layer::Flag(_)) {
            out.push(
                "flag `--brokerlink-bind` is deprecated and ignored: the kernel serves clients directly"
                    .to_string(),
            );
        } else {
            out.push(
                "setting `node.brokerlink_bind` is deprecated: it remains until a later release removes it"
                    .to_string(),
            );
        }
    }
    out
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = <Args as clap::CommandFactory>::command().get_matches();
    let mut args =
        <Args as clap::FromArgMatches>::from_arg_matches(&matches).unwrap_or_else(|err| err.exit());

    // Resolve the startup configuration from the layers: the defaults,
    // `indra.toml`, `conf.d`, the `INDRA_` variables and the typed flags.
    // Then copy the result to the fields that the startup reads. A typed
    // flag overrides the other layers. If no layer set a setting, the
    // field keeps the default of the flag.
    if args.check_config {
        // Validate only and exit. Print `ok`.
        match check_config_outcome(&mut args, &matches) {
            Ok(ok) => {
                println!("{ok}");
                return Ok(());
            }
            Err(err) => {
                // `--check-config` reports the error on stderr and exits 2.
                eprintln!("{err}");
                std::process::exit(2);
            }
        }
    }
    let layered = match load_startup_config(&args, &matches) {
        Ok(layered) => layered,
        Err(err) => {
            // Any other start returns the error and exits 1.
            return Err(err.into());
        }
    };
    if let Err(err) = apply_resolved_config(&mut args, &layered) {
        // Any other start returns the error and exits 1.
        return Err(err.into());
    }
    // Fix the node name before any component reads it. The name has
    // process scope and the first read wins. Late init lets a restart
    // with stored connectors cache the default name instead.
    broker_api::node_scope::init_local_node_name(&args.node_id);
    let cfg = layered.config().clone();
    // Start the log output after the configuration is resolved. Thus
    // `logging.level` applies. If the configuration has an error, `main`
    // returns the error and the runtime prints it on stderr.
    broker_observability::init_tracing_with_level(&cfg.logging.level);
    // One line at debug level records the resolved level and the
    // configuration directory, so an operator can see which level applies.
    debug!(
        level = %cfg.logging.level,
        config_dir = %args.config_dir,
        "log output started"
    );

    // Deprecated settings stay valid for one release. The kernel serves
    // every enabled listener. `native` has no effect. A value of
    // `native = false` requests the removed edge, so it warns more
    // loudly. `node.brokerlink_bind` remains until a later release.
    for warning in deprecated_startup_warnings(&layered) {
        warn!("{warning}");
    }

    info!(
        "Starting IndraMQTT Kernel v{} on {}",
        env!("CARGO_PKG_VERSION"),
        args.bind
    );
    info!("Clean-room architecture ready");

    let mut shared = Shared::new();
    shared.allow_anonymous = args.allow_anonymous;
    shared.node_id = args.node_id.clone();
    // DX2-057: apply the rule engine tuning from the startup layers.
    // Reason: the file value must size live window tasks. Two relaxed
    // stores at boot; task spawn pays one load, publish pays no new lock.
    {
        let depth = cfg.rules_engine.window_channel_depth.max(1) as usize;
        shared.engine.set_window_channel_depth(depth);
        let policy = match cfg.rules_engine.backpressure_policy.as_str() {
            "Block" => BackpressurePolicy::Block,
            "DropNewest" => BackpressurePolicy::DropNewest,
            "DropOldest" => BackpressurePolicy::DropOldest,
            "SpillToDisk" => BackpressurePolicy::SpillToDisk,
            "RejectPublisher" => BackpressurePolicy::RejectPublisher,
            _ => BackpressurePolicy::DropOldest,
        };
        if cfg.rules_engine.spill_dir.is_empty() {
            if policy == BackpressurePolicy::SpillToDisk {
                return Err(
                    "rules_engine.backpressure_policy SpillToDisk needs rules_engine.spill_dir"
                        .into(),
                );
            }
            shared.engine.set_backpressure_policy(policy);
        } else {
            shared
                .engine
                .set_backpressure_policy(BackpressurePolicy::SpillToDisk);
        }
        info!(
            "Rule engine tuning: window_channel_depth={} backpressure_policy={}",
            depth, cfg.rules_engine.backpressure_policy
        );
    }
    // D1-02: apply the configured per-subscriber QoS 0 backlog bound to
    // the shared delivery table before any edge binds. One atomic store
    // at boot; the hot publish path pays a single relaxed load.
    shared.conns.set_qos0_bound(args.qos0_backlog);
    // B4-01: apply the configured QoS 1 window and spill bounds to the
    // session manager before any edge binds. Two atomic stores at boot;
    // future sessions inherit them at creation, and the publish fast
    // path pays one relaxed load for the window check only. SY-02: the
    // window and spill resolve through the typed-schema layers
    // (`session.max_qos1_inflight`, `session.max_qos1_spill`), so
    // `indra.toml` and `INDRA_` variables apply with main's defaults
    // unchanged.
    shared
        .sessions
        .set_max_qos1_inflight(cfg.session.max_qos1_inflight);
    shared
        .sessions
        .set_max_qos1_spill(cfg.session.max_qos1_spill);
    // MV-11c: apply the configured offline queue bound to the session
    // manager before any edge binds. One atomic store at boot; detached
    // durable sessions inherit it for buffering and restore, and the
    // zone view reads the same live value. A kept zone value still wins
    // when set. Resolves through the layers
    // (`session.max_offline_queue`).
    shared
        .sessions
        .set_max_offline_queue(Some(cfg.session.max_offline_queue as usize));
    // B4-05: apply the configured inbound topic-alias bound to the
    // session manager before any edge binds. One atomic store at boot;
    // future sessions inherit it at creation and advertise it in CONNACK.
    // SY-02: resolves through the layers (`session.topic_alias_maximum`).
    shared
        .sessions
        .set_max_topic_alias(cfg.session.topic_alias_maximum);
    // P2: apply the configured packet cap to the session manager
    // before any bind. One atomic store at boot; connections read
    // it per packet and advertise it in CONNACK. Resolves through
    // the layers (`session.max_packet_size`).
    shared
        .sessions
        .set_max_packet_size(cfg.session.max_packet_size);
    // Apply the configured session expiry cap before any bind.
    // One atomic store at boot. CONNECT values above it clamp.
    // Resolves through the layers (`session.max_session_expiry_secs`).
    shared
        .sessions
        .set_max_session_expiry_secs(cfg.session.max_session_expiry_secs);
    // TM-08: apply the client-certificate switches before any bind.
    // Off by default keeps handshakes unchanged. On denies binds
    // without a certificate on that listener. Connect only.
    shared.tls_verify_client_cert = cfg.listeners.tls.verify_client_cert;
    shared.wss_verify_client_cert = cfg.listeners.wss.verify_client_cert;
    // K1-05: apply the client-certificate policy before any bind.
    // The verifier and the bind read these. Connect only.
    shared.tls_verify_peer = cfg.listeners.tls.verify == "verify_peer";
    shared.tls_fail_if_no_peer_cert = cfg.listeners.tls.fail_if_no_peer_cert;
    shared
        .tls_peer_cert_as_username
        .clone_from(&cfg.listeners.tls.peer_cert_as_username);
    shared
        .tls_peer_cert_as_clientid
        .clone_from(&cfg.listeners.tls.peer_cert_as_clientid);
    // MC-01: apply the `[mcp]` boot values before the API serves.
    // The file, the fragments and `INDRA_MCP__ENABLED` /
    // `INDRA_MCP__READ_ONLY` set them through the typed layers.
    shared.mcp = Arc::new(broker_api::mcp::McpStore::from_config(&cfg.mcp));
    shared.wss_verify_peer = cfg.listeners.wss.verify == "verify_peer";
    shared.wss_fail_if_no_peer_cert = cfg.listeners.wss.fail_if_no_peer_cert;
    shared
        .wss_peer_cert_as_username
        .clone_from(&cfg.listeners.wss.peer_cert_as_username);
    shared
        .wss_peer_cert_as_clientid
        .clone_from(&cfg.listeners.wss.peer_cert_as_clientid);
    // K1-04: load the static PSK table before any bind.
    // The file gives the store its identities. An empty path keeps
    // the store empty, so every handshake fails closed. A bad file
    // fails boot loudly, never with an empty store.
    if cfg.listeners.tls.enabled
        && cfg.listeners.tls.psk_enabled
        && !cfg.listeners.tls.psk_keys_file.trim().is_empty()
    {
        let count = shared
            .psk
            .load_file(cfg.listeners.tls.psk_keys_file.trim())
            .map_err(|e| format!("listeners.tls.psk_keys_file: {e}"))?;
        info!("PSK table loaded: {} identities", count);
    }
    // Tenants (MT-01): install the configured assignment rules before
    // any edge binds. Validated whole; a bad row fails boot loudly.
    install_tenant_rules(&shared, &cfg)?;
    // Tenant limits (TM-06): apply the file defaults before any edge
    // binds. One store write at boot; the connect path reads the
    // effective triple and the retained store holds the same default.
    apply_tenant_limit_defaults(&shared, &cfg);
    // Config registry: create the data dir when missing, then load
    // `state.toml` (missing file yields validated defaults). A corrupt
    // file is a fatal boot error naming the file and the field, never a
    // silent boot with defaults.
    shared.config = Arc::new(load_data_dir_registry(&args.data_dir)?);
    // Exporter file settings (AP-09): seed the registry rows the
    // operator set through the startup layers (`indra.toml`,
    // `conf.d`, `INDRA_*`), so the management API and the text
    // exposition observe them from boot. Only keys a layer set move;
    // runtime changes through `PUT /prometheus` keep every other key.
    apply_prometheus_file_settings(&layered, &shared.config)?;
    // Authorization sources (AZ-02): when a startup layer sets the source
    // list, persist it before the boot replay reads the registry. A file
    // or environment list replaces the stored order; an absent key keeps
    // the stored state, so operator recovery is not undone at boot.
    apply_authz_sources_file_settings(&layered, &shared.config)?;
    // Delayed publishes (B4-03): apply the configured delay bound, point
    // the wheel at the kernel data dir, reload pending entries, then
    // start the single driver task. Torn or expired rows are discarded
    // with a counter and never served. One atomic store plus one file
    // read at boot; the publish path pays one atomic load, one bounded
    // file append and one short wheel-lock insert afterwards.
    shared.delayed.set_max_secs(args.delayed_max_secs);
    shared.delayed.set_persist_dir(args.data_dir.clone());
    let (delayed_loaded, delayed_torn, delayed_expired) = shared.delayed.load();
    if delayed_loaded > 0 || delayed_torn > 0 || delayed_expired > 0 {
        info!(
            "Delayed backlog reloaded: {} pending, {} torn discarded, {} expired discarded",
            delayed_loaded, delayed_torn, delayed_expired
        );
    }
    shared.spawn_delayed_driver();
    // Slow-delivery recorder (F1-03, T-73): drain the bounded egress handoff
    // into the shared ranked table so `GET /slow_subscriptions` reflects
    // backpressure instead of a permanent empty list. One task resolving
    // client ids off the delivery path; the publish path never waits on it.
    shared.spawn_slow_recorder();
    // Monitor sampler (F1-02, T-72): record the boot sample and start the
    // periodic tick so `GET /monitor` reflects live counters instead of a
    // permanent empty list. One timer task reading atomics off the delivery
    // path; see `spawn_monitor_sampler` for the interval bounds.
    spawn_monitor_sampler(shared.clone(), args.monitor_sample_secs);
    // Offline queue durability (B4-06): back every detached durable
    // session's queue with `<data-dir>/offline`, then rebuild the
    // in-memory index from it so a restart replays instead of losing the
    // backlog. Torn tails are truncated with a counter and never served.
    // Boot-time only: one directory scan plus one file read per queued
    // client. Afterwards the publish path pays file I/O solely for
    // detached durable matches (see `broker-session`); live fan-out pays
    // nothing. An unreadable directory fails boot loudly rather than
    // silently losing the backlog.
    {
        let offline_dir = std::path::Path::new(&args.data_dir).join("offline");
        match OfflineQueueStore::open(&offline_dir) {
            Ok(store) => {
                let store = Arc::new(store);
                shared.sessions.set_offline_store(store);
                let stats = shared.sessions.restore_offline_queues();
                if stats.messages > 0 || stats.torn > 0 || stats.capped_dropped > 0 {
                    info!(
                        "Offline backlog reloaded: {} messages for {} clients, {} torn discarded, {} capped dropped",
                        stats.messages, stats.clients, stats.torn, stats.capped_dropped
                    );
                }
            }
            Err(e) => {
                return Err(format!(
                    "cannot open offline queue store {}: {e}",
                    offline_dir.display()
                )
                .into());
            }
        }
    }
    // MQTT users/ACLs: seed the shared `MemoryAuth` in place (the same
    // instance `serve_api` hands to `ApiState`), so the BrokerLink plane
    // enforces persisted credentials from the first accepted connection.
    shared
        .auth
        .seed_from_registry(&shared.config)
        .map_err(|e| format!("seeding MQTT users from the config registry failed: {e}"))?;
    // Authentication hashing policy (SA-21): apply the file value to
    // new and changed verifiers. Supported values come from the
    // validated schema (`sha256`, `bcrypt`); any other value fails
    // the start at validation, so the match below stays exhaustive.
    // Existing verifiers keep verifying and migrate lazily on login.
    let hash_algorithm = match cfg.auth.password_hash.as_str() {
        "sha256" => broker_auth::PasswordAlgorithm::Sha256Legacy,
        "bcrypt" => broker_auth::PasswordAlgorithm::Bcrypt,
        other => {
            return Err(format!(
                "auth.password_hash {other:?} is not supported: must be one of sha256, bcrypt (field `auth.password_hash`)"
            )
            .into());
        }
    };
    let hash_policy = broker_auth::PasswordHashPolicy {
        default_algorithm: hash_algorithm,
        ..Default::default()
    };
    shared.auth.set_policy(hash_policy);
    // Superusers (MV-09a): apply the configured list from boot while
    // authentication is enabled. With `auth.enabled = false` the list
    // does not apply, so a matching username gets no bypass. An empty
    // list keeps current behaviour with no bypass.
    let superusers = if cfg.auth.enabled {
        cfg.auth.superusers.clone()
    } else {
        Vec::new()
    };
    shared.auth.set_superusers(superusers);
    // Authenticator chain (W2-02): seed the shared chain in place (the
    // same instance `serve_api` shares with `ApiState`), so the CONNECT
    // consult enforces the persisted chain from the first connection.
    // An empty snapshot is a no-op (today's empty behaviour).
    shared.authn_chain.seed_from_registry(&shared.config);
    // Node auth cache config (W2-03): seed the shared cache in place
    // (the same instance `serve_api` shares with `ApiState`), so
    // management reads observe the persisted enabled flag and cap from
    // the first connection. Entries always start empty (memory-only).
    shared.authn_node_cache.seed_from_registry(&shared.config);
    // Authentication settings (W2-05): seed the shared store in place
    // (the same instance `serve_api` shares with `ApiState`), so the
    // CONNECT consult observes the persisted snapshot from the first
    // connection, then re-apply the cache half so the node cache
    // matches the persisted settings without waiting for a replace.
    shared.authn_settings.seed_from_registry(&shared.config);
    {
        let current = shared.authn_settings.get();
        shared
            .authn_node_cache
            .apply_settings(current.node_cache.enable, current.node_cache.max_count);
    }
    // Authorization node-cache policy (W2-14): seed the shared store in
    // place (the same instance `serve_api` shares with `ApiState`), so
    // management reads observe the persisted policy from the first
    // connection. Cached verdicts always start empty (memory-only,
    // per-session).
    shared.authz_node_cache.seed_from_registry(&shared.config);
    // Authorization settings (W2-16): seed the shared store in place (the
    // same instance `serve_api` shares with `ApiState`), so management
    // reads observe the persisted decisions from the first connection.
    // The link below exposes them to the authorize path.
    shared.authz_settings.seed_from_registry(&shared.config);
    // Authorization source registry (W2-17): seed the shared registry in
    // place (the same instance `serve_api` shares with `ApiState`), so
    // management reads observe the persisted sources from the first
    // connection. Defaults yield the built-in database source.
    shared.authz_sources.seed_from_registry(&shared.config);
    // D2: a stored source this build does not evaluate must never weaken
    // access. Keep the unchanged record. The operator sees and removes it.
    // Deny each decision that reaches it. Raise a named alarm. Log it.
    for source in shared.authz_sources.snapshot().iter() {
        if source.source_type == "built_in_database" {
            continue;
        }
        let source_type = &source.source_type;
        tracing::warn!(source_type, "authorization source {source_type} is not evaluated by this broker version: access it guards is denied; the stored record is unchanged");
        shared.alarms.activate_in_tenant(
            broker_session::tenant::DEFAULT_TENANT_ID,
            "unsupported_authorization_source",
            &format!(
                "authorization source {source_type} is not available in this broker; access it guards is denied"
            ),
            serde_json::json!({"source_type": source.source_type}),
        );
    }
    // Link the shared registry into the shared rule table (AP-06):
    // publish/subscribe verdicts attribute to the built-in counters and
    // honour the stored enable flag and order. One `Arc` clone.
    shared
        .auth
        .set_authz_sources(Arc::clone(&shared.authz_sources));
    // Link the shared authorization settings into the shared rule table
    // (AP-11): publish/subscribe verdicts honour the stored `no_match`
    // and `deny_action` values. One `Arc` clone.
    shared
        .auth
        .set_authz_settings(Arc::clone(&shared.authz_settings));
    // Management API keys (W2-25): seed the shared directory in place
    // (the same instance `serve_api` shares with `ApiState`), so
    // management reads observe the persisted keys from the first
    // connection. An empty snapshot is a no-op (today's empty
    // behaviour); secrets stay hashed at rest and are never re-minted
    // at boot.
    shared.api_keys.seed_from_registry(&shared.config);
    shared.license.load_from_dir(shared.config.dir());
    // SSO backends (W2-32): seed the shared registry in place (the same
    // instance `serve_api` shares with `ApiState`), so management reads
    // observe the persisted backends from the first connection. An empty
    // snapshot is a no-op (today's empty behaviour: `GET /sso` reads
    // `[]`).
    shared.sso_backends.seed_from_registry(&shared.config);
    // Source directory (W3-26): seed the shared directory in place (the
    // same instance `serve_api` shares with `ApiState`), so management
    // reads observe the persisted sources from the first connection and
    // mutations persist through the bound registry. An empty snapshot is
    // a no-op (today's empty behaviour: `GET /sources` reads an empty
    // page).
    shared.sources.seed_from_registry(&shared.config);
    // Source metrics (W3-29, C07-18/19): mirror the persisted enable flags
    // into the engine's lock-free ingress snapshot and register a zeroed
    // per-source metric record for every stored source, so a restart keeps
    // stopped sources shedding and the first publish after a restart
    // attributes from zero. Metrics are memory-only by design; an empty
    // store is a no-op. One snapshot swap each at boot; the publish path
    // only loads afterwards.
    {
        let enables: Vec<(String, bool)> = shared
            .sources
            .snapshot()
            .iter()
            .map(|entry| (entry.id.clone(), entry.enable))
            .collect();
        shared.engine.sync_source_enables(&enables);
        let ids: Vec<String> = enables.iter().map(|(id, _)| id.clone()).collect();
        shared.engine.sync_source_metrics(&ids);
    }
    // Action directory (W3-02): seed the shared directory in place (the
    // same instance `serve_api` shares with `ApiState`), so management
    // reads observe the persisted actions from the first connection and
    // mutations persist through the bound registry. An empty snapshot is
    // a no-op (today's empty behaviour: `GET /actions` reads an empty
    // page).
    shared.actions.seed_from_registry(&shared.config);
    // Managed namespaces (TM-02): seed the shared store in place (the
    // same instance `serve_api` shares with `ApiState`), so management
    // reads observe the persisted namespaces from the first
    // connection. A failed save at runtime rolls the change back.
    // Then apply the stored retained caps, because the retained
    // store keeps caps in memory only. One seed plus one scan at
    // boot; the connect path reads the live table.
    shared.namespaces.seed_from_registry(&shared.config);
    broker_api::v5::mt::apply_stored_retained_caps(&shared.namespaces, &*shared.retained);
    // Connector directory: seed the shared directory in place (the same
    // instance `serve_api` shares with `ApiState`), so management reads
    // observe the persisted connectors from the first connection. An empty
    // snapshot is a no-op (today's empty behaviour: `GET /connectors`
    // reads `[]`).
    shared
        .connectors
        .seed_from_registry(&shared.config, &shared.engine);
    // Connector enable flags: mirror the persisted flags (seeded in place
    // above) into the engine's lock-free dispatch snapshot so a restart
    // keeps disabled connectors skipped from the first publish. An empty
    // store is a no-op (today's empty behaviour: nothing disabled). One
    // snapshot read at boot; the publish path only loads afterwards.
    {
        let enables: Vec<(String, bool)> = shared
            .connectors
            .snapshot()
            .iter()
            .map(|doc| {
                let id = doc
                    .get("id")
                    .and_then(|v| v.as_str())
                    .or_else(|| doc.get("name").and_then(|v| v.as_str()))
                    .unwrap_or("")
                    .to_string();
                let enable = doc.get("enable").and_then(|v| v.as_bool()).unwrap_or(true);
                (id, enable)
            })
            .collect();
        for (id, enable) in &enables {
            if !enable {
                shared.engine.set_action_enabled(id, false);
            }
        }
    }
    // Rule-engine settings (W3-10): seed the shared store in place (the
    // same instance `serve_api` shares with `ApiState`), so management
    // reads observe the persisted knobs from the first connection, then
    // hot-apply the snapshot to the engine so the fire path observes the
    // persisted settings from the first publish. An empty snapshot is a
    // no-op (today's defaults: system messages skipped).
    shared.rule_engine_config.seed_from_registry(&shared.config);
    shared
        .engine
        .apply_engine_config(shared.rule_engine_config.get());
    // Transformation chain (W3-32): seed the shared chain in place (the
    // same instance `serve_api` shares with `ApiState`), so management
    // reads observe the persisted order from the first connection, then
    // mirror it once into the engine's lock-free ingress snapshot so the
    // first publish rewrites without a restart. An empty snapshot is a
    // no-op (today's empty behaviour: no rewrites).
    shared.transformations.seed_from_registry(&shared.config);
    {
        let steps = shared.transformations.steps_snapshot();
        shared.engine.sync_transforms(steps);
        // Transformation metrics (W3-36): register a zeroed record for
        // every stored name so the first publish after a restart
        // attributes from zero. Metrics stay memory-only by design.
        let names: Vec<String> = shared
            .transformations
            .snapshot()
            .iter()
            .map(|record| record.name.clone())
            .collect();
        shared.engine.sync_transform_metrics(&names);
    }
    // Schema registry (W3-37): seed the shared directory in place (the
    // same instance `serve_api` shares with `ApiState`), so reads show
    // kept schemas from the first connection. An empty snapshot is a
    // no-op (empty reads as an empty page).
    shared.schema_registry.seed_from_registry(&shared.config);
    // Default-zone MQTT knobs (AP-03): apply the kept snapshot to
    // the live session manager, so kept values bound new sessions from
    // the first connection. A fresh registry holds defaults, so skip it
    // and keep the layered session config (file and `INDRA_` vars).
    {
        let zone = shared.config.snapshot().global_zone.clone();
        if zone != broker_config::GlobalZoneConf::default() {
            shared
                .sessions
                .set_max_qos1_inflight(zone.max_inflight as usize);
            shared
                .sessions
                .set_max_offline_queue(zone.max_mqueue_len.map(|len| len as usize));
            shared.sessions.set_max_topic_alias(zone.max_topic_alias);
        }
    }
    // Rule spill buffer (B4-07): opt-in via `rules_engine.spill_dir`, before
    // the rules seed so the restored rules land in the engine that will
    // serve them. Empty keeps today's memory-only `DropOldest` input.
    // A directory that cannot be opened fails boot loudly rather than
    // silently running without disk backing.
    if !cfg.rules_engine.spill_dir.is_empty() {
        match RuleEngine::new_with_spill(1024, cfg.rules_engine.spill_dir.as_str()) {
            Ok(engine) => {
                // DX2-057: carry the file window depth onto the spill engine.
                // Reason: the spill path must use the same task depth.
                // One relaxed store at boot; task spawn pays one load.
                engine.set_window_channel_depth(
                    cfg.rules_engine.window_channel_depth.max(1) as usize,
                );
                let engine = Arc::new(engine);
                engine.set_metrics(&shared.metrics);
                // B4-07 boot wiring: carry the live broker sink onto the
                // replacement engine (cf. `Shared::new` installing it at
                // construction) so window-flush republish keeps serving;
                // without this the flush sink stays `None` and republish
                // actions drop. Re-create the three factory preseeds so the
                // spill engine serves exactly what the memory engine would
                // before the registry seed below replaces both.
                engine.set_broker_sink(shared.sink.clone());
                // Enterprise window split (X1-06): the spill replacement
                // engine needs the window executor too, before its
                // preseeds and the registry seed below.
                broker_rules_enterprise::attach(&engine);
                let _ = engine.create_rule(
                    "factory-telemetry-to-kafka".to_string(),
                    TopicFilter::new("sensors/+/telemetry").unwrap(),
                    Some("SELECT payload.temperature as temp, payload.pressure as pressure, clientid FROM \"sensors/+/telemetry\" WHERE payload.temperature > 25".to_string()),
                    true,
                    vec![broker_rules::RuleAction::ForwardConnector {
                        connector_id: "kafka:kafka-prod".to_string(),
                    }],
                );
                let _ = engine.create_rule(
                    "critical-alerts-to-webhook".to_string(),
                    TopicFilter::new("sensors/+/alerts").unwrap(),
                    Some("SELECT payload.level as alert_level, payload.msg as message, clientid FROM \"sensors/+/alerts\" WHERE payload.level = 'CRITICAL'".to_string()),
                    true,
                    vec![broker_rules::RuleAction::ForwardConnector {
                        connector_id: "http:webhook-alerts".to_string(),
                    }],
                );
                let _ = engine.create_rule(
                    "telemetry-to-postgres".to_string(),
                    TopicFilter::new("sensors/+/telemetry").unwrap(),
                    Some(
                        "SELECT payload.temperature as temp, clientid FROM \"sensors/+/telemetry\""
                            .to_string(),
                    ),
                    true,
                    vec![broker_rules::RuleAction::ForwardConnector {
                        connector_id: "pgsql:postgres-analytics".to_string(),
                    }],
                );
                info!(
                    "Rule spill buffer enabled in {}",
                    cfg.rules_engine.spill_dir
                );
                shared.engine = Arc::clone(&engine);
            }
            Err(e) => {
                return Err(format!(
                    "cannot open rule spill dir {}: {e}",
                    cfg.rules_engine.spill_dir
                )
                .into());
            }
        }
    }
    // Rules (W0-22): replay the persisted snapshot through the same
    // validated create path as `RuleEngine::create_rule`. An empty
    // snapshot yields today's empty behaviour (no rules); an invalid
    // stored rule fails boot loudly, never skipped silently.
    shared.engine.seed_from_registry(&shared.config)?;
    // Action enable flags (W3-04): mirror the persisted flags (seeded
    // in place above) into the engine's lock-free dispatch snapshot so a
    // restart keeps disabled actions skipped from the first publish. An
    // empty store is a no-op (today's empty behaviour: nothing disabled).
    // One snapshot swap at boot; the publish path only loads afterwards.
    {
        let enables: Vec<(String, bool)> = shared
            .actions
            .snapshot()
            .iter()
            .map(|record| (record.id.clone(), record.enable))
            .collect();
        shared.engine.sync_action_enables(&enables);
    }
    // Rule spill drain (B4-07): when a spill directory is configured,
    // start the single pressure-ease replay driver. It pends on
    // `input().next_event()` (the prod caller beside tests), draining
    // durability copies in order after bursts ease; each pop counts a
    // replay, keeping spill visible and the disk bounded. Memory-only
    // engines spawn nothing, so steady state pays nothing.
    if shared.engine.input().spill_enabled() {
        spawn_rule_spill_driver(shared.engine.clone());
    }
    // Durable stream journal (B1-07): opt-in via `--stream-dir`. Empty
    // leaves `stream_journal` as `None` so the publish hook is one branch
    // and benchmarks see no new disk work. A corrupt stream directory
    // fails boot loudly rather than silently losing history.
    if !args.stream_dir.is_empty() {
        match StreamJournalHandle::open(&args.stream_dir, StreamConfig::default()) {
            Ok(handle) => {
                info!("Stream journal enabled in {}", args.stream_dir);
                shared.stream_journal = Some(handle);
            }
            Err(e) => {
                return Err(format!("cannot open stream journal {}: {e}", args.stream_dir).into());
            }
        }
    }
    // Directory authentication (B2-01): opt-in via `--ldap-url`. Empty
    // leaves `ldap` as `None` so CONNECT pays one `is_some` branch and
    // benchmarks see no new work. When set, CONNECT consults the local
    // store first, then the directory; a directory outage fails closed.
    if !args.ldap_url.is_empty() {
        let ldap_config = LdapConfig {
            server_url: args.ldap_url.clone(),
            base_dn: args.ldap_base_dn.clone(),
            bind_dn: args.ldap_bind_dn.clone(),
            bind_password: args.ldap_bind_password.clone(),
            user_filter: args.ldap_user_filter.clone(),
            group_attribute: args.ldap_group_attribute.clone(),
            required_group: args.ldap_required_group.clone(),
            // Bound on concurrent directory authentications.
            // Reason: one slot per CONNECT keeps bursts bounded.
            // Exhaustion fails closed and never stalls accepts.
            pool_size: cfg.ldap.pool_size,
            // Bound on TCP connect plus TLS handshake.
            // Reason: a slow directory fails fast and never stalls accepts.
            connect_timeout_ms: cfg.ldap.connect_timeout_ms,
            // Bound on each bind and search round-trip.
            // Reason: a silent directory fails fast and never stalls accepts.
            read_timeout_ms: cfg.ldap.read_timeout_ms,
            ca_cert_path: if args.ldap_ca_cert.is_empty() {
                None
            } else {
                Some(args.ldap_ca_cert.clone())
            },
            ..LdapConfig::default()
        };
        info!(
            "LDAP directory authentication enabled for {}",
            args.ldap_url
        );
        shared.ldap = Some(Arc::new(LdapAuthenticator::new(ldap_config)));
    }
    // Kerberos authentication (B2-02): opt-in via `--kerberos-keytab`.
    // Empty leaves `kerberos` as `None` so CONNECT pays one `is_some`
    // branch and benchmarks see no new work. When set, CONNECT consults
    // the local store first, then Kerberos for SPNEGO/AP-REQ tokens; a
    // missing or unreadable keytab disables explicitly with a clear log
    // line inside the authenticator and never accepts tokens. SY-02: the
    // replay bound resolves through the layers
    // (`kerberos.replay_max_entries`).
    if !args.kerberos_keytab.is_empty() {
        let allowed_realms: Vec<String> = args
            .kerberos_allowed_realms
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        // `principal=role` pairs; malformed entries are ignored so one
        // typo cannot disable the whole map (fail closed per-principal:
        // unmapped stays `user`).
        let mut principal_role_map = std::collections::HashMap::new();
        for pair in args.kerberos_role_map.split(',') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }
            if let Some((principal, role)) = pair.split_once('=') {
                let principal = principal.trim();
                let role = role.trim();
                if !principal.is_empty() && !role.is_empty() {
                    principal_role_map.insert(principal.to_string(), role.to_string());
                }
            }
        }
        let kerberos_config = KerberosConfig {
            service_principal_name: args.kerberos_service_principal.clone(),
            realm: args.kerberos_realm.clone(),
            allowed_realms,
            keytab_path: args.kerberos_keytab.clone(),
            clock_skew_secs: args.kerberos_clock_skew_secs,
            replay_max_entries: cfg.kerberos.replay_max_entries,
            principal_role_map,
        };
        info!(
            "Kerberos authentication enabled for {}",
            args.kerberos_service_principal
        );
        shared.kerberos = Some(Arc::new(KerberosAuthenticator::new(kerberos_config)));
    }
    // JWT authentication (B5-02): opt-in via `--jwks-url`. Empty leaves
    // `jwks` as `None` so CONNECT pays one `is_some` branch and
    // benchmarks see no new work. When set, CONNECT verifies JWT-shaped
    // passwords against the live endpoint (signature by `kid`, expiry,
    // issuer, audience); unknown `kid`s trigger one singleflight refresh
    // and any endpoint outage fails closed. The background task keeps the
    // bounded key cache fresh without a restart. SY-02: the endpoint,
    // issuer and audience resolve through the layers (`jwks.url`,
    // `jwks.issuer`, `jwks.audience`) with main's defaults unchanged.
    if !cfg.jwks.url.is_empty() {
        if !cfg.jwks.url.starts_with("https://") {
            return Err(format!(
                "invalid jwks.url {}: must start with https:// (field `jwks.url`)",
                cfg.jwks.url
            )
            .into());
        }
        let jwks_config = JwksConfig {
            jwks_url: cfg.jwks.url.clone(),
            issuer: cfg.jwks.issuer.clone(),
            audience: cfg.jwks.audience.clone(),
            refresh_period_secs: args.jwks_refresh_period_secs,
            fetch_timeout_ms: args.jwks_fetch_timeout_ms,
            refresh_timeout_ms: args.jwks_refresh_timeout_ms,
            cache_max_keys: args.jwks_cache_max_keys,
            cache_ttl_secs: args.jwks_cache_ttl_secs,
            max_document_bytes: broker_auth::JWKS_DEFAULT_DOCUMENT_CAP,
            clock_skew_secs: args.jwks_clock_skew_secs,
            tls_verify: !args.jwks_insecure_skip_verify,
            ca_cert_path: if args.jwks_ca_cert.is_empty() {
                None
            } else {
                Some(args.jwks_ca_cert.clone())
            },
        };
        info!("JWT authentication enabled for {}", cfg.jwks.url);
        let jwks = Arc::new(JwksAuthenticator::new(jwks_config));
        jwks.spawn_background_refresh();
        shared.jwks = Some(jwks);
    }
    // Database authentication (B5-03): opt-in per `--dbauth-*-url`. All
    // empty leaves `db_auth` as `None` so CONNECT pays one `is_some`
    // branch and benchmarks see no new work. When any URL is set,
    // CONNECT consults the local store (plus LDAP/Kerberos) first, then
    // the databases; a database outage fails closed.
    if !args.dbauth_postgres_url.is_empty()
        || !args.dbauth_mysql_url.is_empty()
        || !args.dbauth_redis_url.is_empty()
        || !args.dbauth_mongodb_url.is_empty()
    {
        let db_config = DbAuthSetConfig {
            postgres_url: args.dbauth_postgres_url.clone(),
            mysql_url: args.dbauth_mysql_url.clone(),
            redis_url: args.dbauth_redis_url.clone(),
            mongodb_url: args.dbauth_mongodb_url.clone(),
            pool_size: args.dbauth_pool_size,
            connect_timeout_ms: args.dbauth_connect_timeout_ms,
            read_timeout_ms: args.dbauth_read_timeout_ms,
            cache_max_entries: args.dbauth_cache_size,
            cache_ttl_secs: args.dbauth_cache_ttl_secs,
        };
        let db_set = DbAuthSet::new(&db_config);
        info!(
            "Database authentication enabled (pool {} per source)",
            db_set.pool_size()
        );
        shared.db_auth = Some(Arc::new(db_set));
    }
    // HTTP webhook authentication and authorization (B5-04): opt-in via
    // `--webhook-url`. Empty leaves `webhook` as `None` so CONNECT pays
    // one `is_some` branch and publishes pay one branch plus the local
    // ACL. When set, credentialed CONNECTs the local store (and any
    // directory) refuses go to the verdict endpoint, and every publish
    // consults it on a cache miss; an unreachable, slow or tripped
    // endpoint fails closed and never grants access.
    if !args.webhook_url.is_empty() {
        let webhook_config = WebhookConfig {
            endpoint_url: args.webhook_url.clone(),
            pool_size: args.webhook_pool_size,
            request_timeout_ms: args.webhook_timeout_ms,
            breaker_failure_threshold: args.webhook_breaker_threshold,
            breaker_reset_timeout_ms: args.webhook_breaker_reset_ms,
            cache_max_entries: args.webhook_cache_size,
            cache_ttl_secs: args.webhook_cache_ttl_secs,
        };
        info!(
            "HTTP webhook authentication enabled for {}",
            args.webhook_url
        );
        shared.webhook = Some(Arc::new(WebhookAuth::new(webhook_config)));
    }

    // Licensing lifecycle (B2-04): trusted keys are configuration loaded at
    // startup whether or not clustering is enabled (a single node is a
    // cluster of one). The cluster identity is created on first start and
    // persisted in the data directory with the trial start, so restarts and
    // added nodes never restart the trial.
    let trusted_keys = if args.license_keys.trim().is_empty() {
        TrustedKeys::new()
    } else {
        match TrustedKeys::load_from_path(std::path::Path::new(args.license_keys.trim())) {
            Ok(keys) => keys,
            Err(e) => {
                return Err(format!(
                    "cannot load trusted licence keys from {}: {e}",
                    args.license_keys.trim()
                )
                .into());
            }
        }
    };
    trusted_keys.log_at_boot();
    shared.licence.set_trusted_keys(trusted_keys.clone());
    shared
        .licence
        .set_expiry_warn_days(args.licence_expiry_warn_days);
    let now_licence = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut licence_state = match shared
        .licence
        .load_from_data_dir(std::path::Path::new(&args.data_dir), now_licence)
    {
        Ok(state) => state,
        Err(e) => {
            return Err(format!("cannot load licence state from {}: {e}", args.data_dir).into());
        }
    };
    // DX2-057: apply `licence.license_key` from the startup layers.
    // Reason: the file value must install the token. A bad token fails
    // boot loudly, never with a silent trial. Empty keeps stored state.
    let licence_key = cfg.licence.license_key.trim().to_string();
    if !licence_key.is_empty() {
        match shared.licence.install(&licence_key, now_licence) {
            Ok(payload) => {
                info!(
                    "Licence key installed for '{}' (max {} nodes)",
                    payload.customer, payload.max_nodes
                );
                licence_state = match shared
                    .licence
                    .load_from_data_dir(std::path::Path::new(&args.data_dir), now_licence)
                {
                    Ok(state) => state,
                    Err(e) => {
                        return Err(format!(
                            "cannot reload licence state from {}: {e}",
                            args.data_dir
                        )
                        .into());
                    }
                };
            }
            Err(e) => {
                return Err(format!("licence.license_key install refused: {e}").into());
            }
        }
    }
    // File commands for sites with no dashboard: generate the request or
    // install a licence, then exit without starting the broker.
    if !args.licence_request_out.trim().is_empty() {
        let request = shared
            .licence
            .generate_request(Some(3), vec!["clustering".to_string()], "")
            .map_err(|e| format!("cannot generate licence request: {e}"))?;
        let text = serde_json::to_string_pretty(&request)
            .map_err(|e| format!("cannot encode licence request: {e}"))?;
        std::fs::write(args.licence_request_out.trim(), text.as_bytes()).map_err(|e| {
            format!(
                "cannot write licence request to {}: {e}",
                args.licence_request_out.trim()
            )
        })?;
        println!(
            "Licence request written to {}\n{}",
            args.licence_request_out.trim(),
            request.summary
        );
        return Ok(());
    }
    if !args.licence_install_file.trim().is_empty() {
        let text = std::fs::read_to_string(args.licence_install_file.trim()).map_err(|e| {
            format!(
                "cannot read licence file {}: {e}",
                args.licence_install_file.trim()
            )
        })?;
        match shared.licence.install(&text, now_licence) {
            Ok(payload) => {
                shared.licence.refresh_alarms(&shared.alarms, now_licence);
                println!(
                    "Licence installed for '{}' (max {} nodes, expires at {})",
                    payload.customer, payload.max_nodes, payload.expires_at
                );
                return Ok(());
            }
            Err(e) => {
                return Err(format!("licence install refused: {e}").into());
            }
        }
    }
    log_licence_lifecycle(&licence_state, args.licence_expiry_warn_days);
    shared.licence.refresh_alarms(&shared.alarms, now_licence);
    // MV-11b: clustering stays out of the MVP. The schema refuses its entry.
    // The broker never binds a cluster socket. This check fails closed.
    // Reason: the cluster path cannot keep tenants apart.
    let cluster_flag = args
        .cluster_seeds
        .as_ref()
        .is_some_and(|seeds| !seeds.trim().is_empty());
    let cluster_file = cfg.cluster.enabled || !cfg.cluster.seed_nodes.is_empty();
    if cluster_flag || cluster_file {
        return Err("clustering is not in the MVP: remove cluster.enabled, cluster.seed_nodes and --cluster-seeds and run single-node"
            .to_string()
            .into());
    }
    // A disabled API listener opens no socket. The bind flag alone
    // never enables it. No flag sets `enabled`, so a file or variable
    // with `enabled = false` wins over a bind value (fail closed).
    if cfg.listeners.api.enabled && !args.api_bind.is_empty() {
        let listener = tokio::net::TcpListener::bind(&args.api_bind).await?;
        info!("Management API listening on {}", args.api_bind);
        // Cluster text (AP-03): name the kept description once at boot,
        // so operators confirm the cluster identity in the log.
        let kept = shared.config.snapshot().cluster_meta.description.clone();
        if !kept.is_empty() {
            info!("Cluster description: {}", kept);
        }
        // SY-02: assign the resolved listeners and startup provenance so
        // `GET /listeners` and `explain` report the running configuration
        // instead of defaults. The api row reports the bound address: the
        // flag is the bind the kernel actually listens on.
        let mut listeners = cfg.listeners.clone();
        listeners.api.bind = args.api_bind.clone();
        *shared.api_listeners.write().expect("listeners lock") = Some(Arc::new(listeners));
        *shared
            .api_startup_config
            .write()
            .expect("startup config lock") = Some(Arc::new(layered.clone()));
        let api_shared = shared.clone();
        tokio::spawn(async move {
            if let Err(e) = serve_api(listener, api_shared).await {
                warn!("Management API exited: {}", e);
            }
        });
    }
    // CoAP gateway (B1-04): off unless `--coap-bind` names an address.
    // Bound once here so `Shared.coap_socket` is read-only afterwards;
    // the listener task owns a clone and publishes through the router.
    if !args.coap_bind.is_empty() {
        match tokio::net::UdpSocket::bind(&args.coap_bind).await {
            Ok(socket) => {
                let socket = Arc::new(socket);
                shared.coap_socket = Some(socket.clone());
                info!("CoAP gateway listening on {}", args.coap_bind);
                let gateway_shared = shared.clone();
                tokio::spawn(async move {
                    run_coap_gateway(gateway_shared, socket).await;
                });
            }
            Err(e) => {
                warn!("Failed to bind CoAP gateway on {}: {}", args.coap_bind, e);
            }
        }
    }

    // The kernel serves every enabled listener. A listener that cannot
    // start stops the kernel: a broker that runs without a configured
    // listener looks healthy and accepts no client. The directory seeds
    // from the boot configuration and carries the socket control, so
    // the v5 start, stop and restart routes drive the same sockets the
    // boot binds here.
    shared.listener_store.reset_from_config(&cfg.listeners);
    let control: Arc<native::NativeListenerController> = Arc::new(
        native::NativeListenerController::new(shared.clone(), cfg.listeners.clone()),
    );
    shared.listener_store.set_controller(control.clone());
    for id in ["tcp:default", "ssl:default", "ws:default", "wss:default"] {
        let Some(mut record) = shared.listener_store.get(id) else {
            continue;
        };
        if !record.enable {
            continue;
        }
        let bind = record.bind.clone();
        broker_api::v5::listeners::ListenerController::start_listener(&*control, &record)
            .map_err(|detail| format!("listeners {id} {detail}"))?;
        info!("Kernel MQTT listener on {bind}");
        record.running = true;
        shared.listener_store.update(record);
    }
    // Start background tasks, then wait for the stop signal.
    // The signal stops the kernel in order.
    start_background_tasks(&shared);
    wait_for_shutdown().await;
    shutdown(&shared).await;
    Ok(())
}

#[cfg(test)]
mod tests;
