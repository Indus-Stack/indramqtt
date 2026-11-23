use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use broker_auth::{AclAction, AclRule, MemoryAuth};
use broker_config::ConfigRegistry;
use broker_observability::{Metrics, NodeReadiness, StatsStore};
use broker_protocol::{QoS, Topic, TopicFilter};
use broker_router::{ConnTable, Router as SubscriptionRouter};
use broker_rules::{Rule, RuleAction, RuleEngine, RuleEngineError};
use broker_session::SessionManager;
use brokerlink::BrokerFrame;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::mpsc::UnboundedSender;

use crate::admin_users::AdminUsers;
use crate::v5::auth::ApiTokens;

/// Exact client error for every spelling of the removed S3 Tables
/// connector. Nothing is stored and an update leaves the old record
/// untouched.
pub(crate) const REMOVED_CONNECTOR_MESSAGE: &str =
    "connector type s3_tables is not available in this broker";

/// Returns the exact refusal when `kind` names the removed backend.
pub(crate) fn removed_connector_refusal(kind: &str) -> Option<&'static str> {
    broker_config::is_removed_connector_type(kind).then_some(REMOVED_CONNECTOR_MESSAGE)
}

pub mod admin_users;
pub mod api_auth;
pub mod api_keys;
pub mod config_runtime;
pub mod dashboard;
pub mod errors;
pub mod licence;
pub mod mcp;
pub mod mfa_totp;
pub mod node_scope;
pub mod pagination;
pub mod swagger;
mod v1_disk_log;
pub mod v5;
pub mod ws;

/// Shared node state injected into every management endpoint: the rule
/// engine (mutable via the rules API), session directory, subscription
/// router, delivery directory (shared with the BrokerLink plane so the
/// dashboard WebSocket console receives the same fan-out), and node
/// metrics.
#[derive(Clone)]
pub struct ApiState {
    pub engine: Arc<RuleEngine>,
    pub sessions: Arc<SessionManager>,
    pub router: Arc<SubscriptionRouter>,
    pub metrics: Arc<Metrics>,
    pub auth: Arc<MemoryAuth>,
    pub conns: Arc<ConnTable>,
    pub admin_users: Arc<AdminUsers>,
    pub tokens: Arc<ApiTokens>,
    /// Kernel-owned configuration registry: lock-free snapshots of admin
    /// users, MQTT users/ACLs, rules and connectors.
    pub config: Arc<ConfigRegistry>,
    /// Local node name (`--node-id`): used by the node-scope helper for
    /// `/nodes/{node}/...` routes. Stored only until W1 wires it in.
    pub node_id: String,
    /// Kernel→edge close sender (W0-26): the kick path hands `ConnClose`
    /// frames here before tearing down kernel state. The sender type is
    /// the same `UnboundedSender<BrokerFrame>` the kernel uses for its
    /// per-connection edge mailboxes; sends are non-blocking and a
    /// failed send only warns (edge may already be gone). Guaranteed
    /// delivery rides the synchronous `ConnTable::route` in the kick
    /// path; the async `serve_api` forwarder routes this copy after the
    /// connection is unregistered and drops it by design.
    pub edge_tx: UnboundedSender<BrokerFrame>,
    /// Ban directory for `/api/v5/banned` (W1-01): bounded matcher map
    /// with lazy expiry. Management-plane only; never touched on the
    /// per-message path.
    pub bans: Arc<crate::v5::banned::BanStore>,
    /// Alarm directory for `/api/v5/alarms` (W1-16): bounded active map
    /// plus deactivated history. The kernel raises entries on CONNECT
    /// (`crates/broker-node/src/main.rs`, ban refusal path) and reads
    /// them here. Never touched on the per-message path.
    pub alarms: Arc<crate::v5::alarms::AlarmStore>,
    /// Monitor history for `/api/v5/monitor` (W1-19): bounded ring of
    /// sampled points. Management-plane only; never touched on the
    /// per-message path.
    pub monitor: Arc<crate::v5::monitor::MonitorHistory>,
    /// Gauge-plus-high-water-mark store for `/api/v5/stats` and
    /// `/api/v5/nodes/{node}/stats` (W1-25): the kernel keeps it exact
    /// from connection/subscription/retained lifecycle points and the
    /// handlers below read one constant-time snapshot per request.
    /// Management-plane only; never touched on the per-message path.
    pub stats: Arc<StatsStore>,
    /// Exporter settings for `GET`/`PUT /api/v5/prometheus` (AP-09):
    /// one validated struct persisted across a restart via the config
    /// core. The routes read one small snapshot per request; the store
    /// seeds from the shared registry at boot (`from_registry` in both
    /// constructors below, fed by the kernel registry), so management
    /// reads observe the persisted settings and no second copy can
    /// diverge. The text exposition routes render the same true
    /// counters as the JSON rows. Management-plane only; publish and
    /// deliver never touch this store.
    pub prometheus: Arc<crate::v5::prometheus::PrometheusStore>,
    /// Node readiness flag for `GET /api/v5/status` (W1-27): single
    /// atomic bool shared with the kernel. The handler does one
    /// constant-time load per request and reports the documented up
    /// value while ready. Management-plane only; never touched on the
    /// per-message path.
    pub readiness: Arc<NodeReadiness>,
    /// Retained-delivery settings for `GET/PUT /api/v5/mqtt/retainer`
    /// (W1-28): single validated struct behind a short lock plus a
    /// persistence hook. Management-plane only; never touched on the
    /// per-message path.
    pub retainer_config: Arc<crate::v5::retainer::RetainerConfigStore>,
    /// Retained store for `GET /api/v5/mqtt/retainer/message/{topic}`
    /// (W1-28): shared with the kernel so management reads observe the
    /// same MQTT ingress state. Reads take only a short read lock and
    /// clone at most one message, so they never block delivery.
    /// Bounded by `broker_storage::MAX_RETAINED_MESSAGES`.
    pub retained: Arc<dyn broker_storage::RetainedStore>,
    /// Coordinates retained writes + fan-out against concurrent `SUBSCRIBE`
    /// retained-snapshot + route installation (shared with the kernel).
    pub retained_subscribe_gate: Arc<tokio::sync::RwLock<()>>,
    /// Slow-subscription recorder for `GET/DELETE /api/v5/slow_subscriptions`
    /// (W1-30): bounded ranked table of slow deliveries. Management-plane
    /// only; never touched on the per-message path.
    pub slow_subs: Arc<crate::v5::slow_subscriptions::SlowSubsStore>,
    /// Slow-subscription thresholds for `GET/PUT
    /// /api/v5/slow_subscriptions/settings` (W1-31): single validated
    /// struct behind a short lock. Management-plane only; readers take a
    /// snapshot clone and never touch the per-message path.
    pub slow_subs_settings: Arc<crate::v5::slow_subscriptions::SlowSubsSettingsStore>,
    /// Packet-tracing flag for `GET/PUT /api/v5/tracing` (W1-32):
    /// single atomic bool. Session lifecycle raises it while a session
    /// exists and lowers it when none remains; the kernel publish event
    /// reads it first as its cheap off-state check (F1-04, T-74).
    pub tracing: Arc<crate::v5::tracing::TracingFlagStore>,
    /// Trace-session registry for `GET/POST/DELETE /api/v5/trace` (W1-33)
    /// plus `DELETE /api/v5/trace/{name}` (W1-34):
    /// bounded map of capture sessions. Reads and writes are
    /// management-plane only; the kernel publish event appends through
    /// `TraceStore::capture_publish` (F1-04, T-74).
    pub traces: Arc<crate::v5::trace::TraceStore>,
    /// Auto-subscribe list for `GET/PUT /api/v5/mqtt/auto_subscribe`
    /// (W1-39): bounded validated list plus the connect-time hook. The
    /// routes read one bounded snapshot per request; the kernel hook
    /// clones the same capped list once per bind, never per message.
    pub auto_subscribe: Arc<crate::v5::auto_subscribe::AutoSubscribeStore>,
    /// Delayed-publish scheduler for the `/api/v5/mqtt/delayed` routes
    /// (AP-04): the same wheel the kernel schedules `$delayed/`
    /// publishes on and fires from. Management reads snapshot the wheel
    /// under one short lock; removals drop the wheel row and the file
    /// row together so a removed entry never fires. Management-plane
    /// only; the publish path pays one atomic load for the switch.
    pub delayed: Arc<broker_delayed::DelayedScheduler>,
    /// Licence request/install/status store for B2-04: cluster identity,
    /// stored token and trusted signing set. Management-plane only; never
    /// touched on the per-message path.
    pub licence: Arc<crate::licence::LicenceStore>,
    /// The licence settings and the session history behind
    /// `/api/v5/license/setting` and `/api/v5/license/session_hwm_history`.
    /// The kernel shares its instance, which it reads from the data
    /// directory and writes the session measurements to.
    pub license: Arc<crate::v5::license::LicenseState>,
    /// Ordered authenticator chain for `GET/POST /api/v5/authentication`
    /// (W2-02): bounded ordered list plus the CONNECT-time consult. The
    /// routes read one bounded snapshot per request; the kernel CONNECT
    /// path loads one lock-free snapshot per connect
    /// (`crates/broker-node/src/main.rs`, CONNECT handling) and never
    /// takes the chain write lock; publish and deliver never touch it.
    pub authn_chain: Arc<broker_auth::AuthnChain>,
    /// Node authentication cache for `GET
    /// /api/v5/authentication/node_cache/status` and `POST
    /// /api/v5/authentication/node_cache/reset` (W2-03): bounded
    /// per-username record of successful credentialed CONNECTs. The
    /// routes read one bounded snapshot per request; the kernel CONNECT
    /// path records one entry per success
    /// (`crates/broker-node/src/main.rs`, CONNECT handling, plus the
    /// console CONNECT in `crates/broker-api/src/ws.rs`); publish and
    /// deliver never touch it.
    pub authn_node_cache: Arc<broker_auth::NodeAuthCache>,
    /// Global authentication settings for `GET/PUT
    /// /api/v5/authentication/settings` (W2-05): single validated struct
    /// with registry persistence plus a lock-free CONNECT snapshot. The
    /// routes read one snapshot per request; the kernel CONNECT path
    /// loads one lock-free snapshot per connect
    /// (`crates/broker-node/src/main.rs`, CONNECT handling, plus the
    /// console CONNECT in `crates/broker-api/src/ws.rs`); publish and
    /// deliver never touch it.
    pub authn_settings: Arc<crate::v5::authn_settings::AuthnSettingsStore>,
    /// Node authorization cache policy for `GET/PUT
    /// /api/v5/authorization/node_cache` and `POST
    /// /api/v5/authorization/node_cache/reset` (W2-14): single validated
    /// struct with registry persistence. The routes read one snapshot
    /// per request; the kernel boot path seeds the same instance from
    /// the registry (`crates/broker-node/src/main.rs`, boot seeding);
    /// the reset evicts every per-session decision entry recorded by
    /// the publish and subscribe authorization events
    /// (`crates/broker-session/src/lib.rs`,
    /// `SessionManager::clear_all_authz_caches`). Publish and deliver
    /// never touch this store.
    pub authz_node_cache: Arc<broker_auth::AuthzNodeCache>,
    /// Global authorization settings for `GET/PUT
    /// /api/v5/authorization/settings` (W2-16): single validated struct
    /// with registry persistence. The routes read one snapshot per
    /// request; the kernel boot path seeds the same instance from the
    /// registry (`crates/broker-node/src/main.rs`, boot seeding); the
    /// stored `no_match`/`deny_action` values are consulted by the
    /// publish and subscribe authorization events
    /// (`crates/broker-node/src/main.rs`, publish and subscribe handling)
    /// through lock-free bool reads with no snapshot clone.
    pub authz_settings: Arc<broker_auth::AuthzSettingsStore>,
    /// Ordered authorization source registry for `GET/POST
    /// /api/v5/authorization/sources` (W2-17): bounded ordered list with
    /// registry persistence plus a lock-free read snapshot. The routes
    /// read one bounded snapshot per request; the kernel boot path seeds
    /// the same instance from the registry
    /// (`crates/broker-node/src/main.rs`, boot seeding); the built-in
    /// catch-all clear empties the live rule table in `MemoryAuth`
    /// (`crates/broker-auth/src/lib.rs`, `MemoryAuth::clear_rules`),
    /// which the publish and subscribe authorization events consult
    /// (`crates/broker-node/src/main.rs`, publish and subscribe
    /// handling). Publish and deliver never touch this registry's write
    /// lock.
    pub authz_sources: Arc<broker_auth::AuthzSourceRegistry>,
    /// Management API key directory for `GET/POST /api/v5/api_key` and
    /// `GET/PUT/DELETE /api/v5/api_key/{name}` (W2-25, W2-26): bounded name-keyed map
    /// with registry persistence. The routes read one bounded snapshot
    /// per request; the kernel boot path seeds the same instance from
    /// the registry (`crates/broker-node/src/main.rs`, boot seeding and
    /// the `serve_api` share below). Key checks use constant-time
    /// comparison off the hot path; publish and deliver never touch
    /// this store. Bearer/Basic enforcement itself is owned elsewhere;
    /// this store only manages the key records.
    pub api_keys: Arc<crate::v5::api_keys::ApiKeyStore>,
    /// Management audit ring for `GET /api/v5/audit` (W2-28): bounded
    /// oldest-evicted log of authenticated management requests plus
    /// CONNECT outcomes. The kernel records CONNECT success and refusal
    /// (`crates/broker-node/src/main.rs`, bind path) and the management
    /// auth middleware records each authenticated request
    /// (`crate::api_auth::require_api_auth`). Publish and deliver never
    /// touch this store.
    pub audit: Arc<crate::v5::audit::AuditLog>,
    /// Configured single-sign-on backend registry for `GET /api/v5/sso`
    /// (W2-32) and the per-backend `GET/PUT/DELETE /api/v5/sso/{backend}`
    /// detail routes (W2-39): bounded name-keyed map with registry
    /// persistence. The routes read one bounded snapshot per request; the
    /// kernel boot path seeds the same instance from the registry
    /// (`crates/broker-node/src/main.rs`, boot seeding and the
    /// `serve_api` share below). Management-plane only; publish and
    /// deliver never touch this store.
    pub sso_backends: Arc<crate::v5::sso::SsoStore>,
    /// Source instance directory for `GET /api/v5/sources` (W3-26,
    /// C07-12), `POST /api/v5/sources` (W3-26, C07-13),
    /// `DELETE /api/v5/sources/{id}` (W3-26, C07-14),
    /// `GET /api/v5/sources/{id}` (W3-27, C07-15),
    /// `PUT /api/v5/sources/{id}` (W3-27, C07-16) and
    /// `GET /api/v5/sources_summary` (W3-31, C07-22): bounded id-keyed
    /// map validated against the source-kind set, persisted across a
    /// restart via the config core. The routes read one bounded snapshot
    /// per request; the kernel boot path shares the same instance
    /// (`crates/broker-node/src/main.rs`, `serve_api` share below) so
    /// management reads observe the kernel's directory. The summary
    /// counts are computed on demand from the live snapshot, never
    /// stored. Management-plane only; publish and deliver never touch
    /// this store.
    pub sources: Arc<crate::v5::sources::SourceStore>,
    /// Action instance directory for `GET /api/v5/actions`,
    /// `POST /api/v5/actions`, `DELETE /api/v5/actions/{id}` (W3-02,
    /// C06-02..C06-04), `GET`/`PUT /api/v5/actions/{id}` (W3-03,
    /// C06-05/C06-06) and `PUT /api/v5/actions/{id}/enable/{enable}`
    /// (W3-04, C06-07): bounded id-keyed map validated against the
    /// action-kind registry from the action-types catalogue, with
    /// persistence across a restart via the config core and delete
    /// refused while a rule references the action. The routes read one
    /// bounded snapshot per request; the kernel boot path shares the
    /// same instance (`crates/broker-node/src/main.rs`, `serve_api`
    /// share below) so management reads observe the kernel's
    /// directory. Management-plane only; rule evaluation reads a
    /// snapshot, never this store directly, and publish and deliver
    /// never touch it.
    pub actions: Arc<crate::v5::actions::ActionStore>,
    /// Serialises the action-sink and connector-sink lifecycle: action
    /// create/update/delete, connector create/update/delete, and the
    /// config reload/restore replay. It makes one staged generation
    /// (build, persist, swap, drain) atomic with respect to the others.
    pub action_sink_lock: Arc<tokio::sync::Mutex<()>>,
    /// Connector instance directory for `GET /api/v5/connectors`,
    /// `POST /api/v5/connectors`, `DELETE /api/v5/connectors/{id}`,
    /// `GET`/`PUT /api/v5/connectors/{id}`, the enable flip and the
    /// start/stop operations: bounded id-keyed list persisted across a
    /// restart via the config core, with delete refused while a rule or
    /// action references the connector. Each state holds its own list, so
    /// two servers in one process never share connectors. The routes read
    /// one bounded snapshot per request; the kernel boot path shares the
    /// same instance (`crates/broker-node/src/main.rs`, `serve_api` share
    /// below) so management reads observe the kernel's directory.
    /// Management-plane only; rule evaluation reads the connector manager
    /// directly, and publish and deliver never touch this store.
    pub connectors: Arc<crate::v5::rules::ConnectorStore>,
    /// Rule-engine settings for `GET`/`PUT /api/v5/rule_engine` (W3-10,
    /// C06-14/C06-15): one validated struct persisted across a restart via
    /// the config core, with a subscriber callback hot-applying every
    /// validated write to the engine's fire-path snapshot. The routes read
    /// one small snapshot per request; the fire path loads the engine
    /// snapshot per ingress and never takes a management lock.
    /// Management-plane only; publish and deliver never touch this store.
    pub rule_engine_config: Arc<broker_rules::EngineConfigStore>,
    /// Operator API-key store for `indra ctl` (M1-06): bounded set of
    /// opaque keys seeded once from `INDRA_API_KEYS`. Consulted by
    /// [`api_auth::resolve_credentials`] when a Bearer token is not a
    /// dashboard token; management-plane only, never on the delivery path.
    pub operator_api_keys: Arc<crate::api_keys::ApiKeyStore>,
    /// Configured network listeners captured at kernel boot (M1-06):
    /// the same `listeners.*` section the kernel binds from, shared here
    /// so `GET /api/v5/listeners` reports the running configuration.
    /// One `Arc` clone at boot; handlers clone at most five small rows per
    /// request. Management-plane only, never on the delivery path.
    /// Superseded by [`listener_store`](Self::listener_store) for the v5
    /// handlers, which need live running state; kept as the boot snapshot.
    pub listeners: Arc<broker_config::schema::ListenersConf>,
    /// Live listener directory for the v5 listener routes (AP-02):
    /// stored configuration plus the running flag and the socket
    /// control the kernel installs at boot. The kernel shares its
    /// instance (`crates/broker-node/src/main.rs`, boot plus the
    /// `serve_api` share) so management writes reach the bound sockets
    /// without a restart. One short lock per request; publish and
    /// deliver never touch this store.
    pub listener_store: Arc<crate::v5::listeners::ListenerStore>,
    /// Startup layer provenance for M1-05 `explain` (defaults, `indra.toml`,
    /// `conf.d` fragment, environment variable, flag). Set by the kernel
    /// boot path from the resolved [`broker_config::layers::LayeredConfig`];
    /// `None` in tests and tools, where every schema key reports built-in
    /// defaults. One short read lock per explain request; never touched on
    /// the per-message path.
    pub startup_config: Arc<std::sync::RwLock<Option<Arc<broker_config::layers::LayeredConfig>>>>,
    /// Ordered message-transformation chain for `GET`/`POST`/`PUT
    /// /api/v5/message_transformations` (W3-32, C08-01..C08-03) plus
    /// the stateless dry-run probe
    /// `POST /api/v5/message_transformations/dryrun` (W3-33, C08-04)
    /// and the order replace
    /// `POST /api/v5/message_transformations/reorder` (W3-34, C08-05):
    /// bounded ordered chain validated per entry, persisted across a
    /// restart via the config core, executed in order on matching
    /// ingress through the engine's lock-free snapshot. The routes read
    /// one bounded snapshot per request; the publish path loads one
    /// `Arc` per ingress and never takes this store's lock. The probe
    /// uses only the supplied chain and writes nothing.
    pub transformations: Arc<crate::v5::transformations::TransformChainStore>,
    /// Schema registry for `GET`/`POST /api/v5/schema_registry` (W3-37,
    /// C08-11/C08-12) and `DELETE /api/v5/schema_registry/:name` (W3-37,
    /// C08-13): bounded map with checked writes, kept across restarts
    /// through the config core. Routes read one bounded snapshot per
    /// request. No publish path reads this store. Publish and deliver take
    /// no lock here.
    pub schema_registry: Arc<crate::v5::schema_registry::SchemaRegistryStore>,
    /// Managed tenant namespaces for the 16 `GET/POST/PUT/DELETE
    /// /api/v5/mt/...` operations (TM-02): bounded name-keyed map with
    /// creation times plus stored configs. Reads join live broker state
    /// (sessions per tenant, credential tables per tenant); writes touch
    /// only this registry plus the tenant rows. The instance is created
    /// once in `ApiState::new`, so the served API and the kernel observe
    /// one registry with no second copy. Management-plane only; publish
    /// and deliver never touch this store.
    pub namespaces: Arc<crate::v5::mt::NamespaceStore>,
    /// MCP endpoint switch (MC-01, MC-02): two atomic flags from
    /// the `[mcp]` boot configuration. The kernel shares its instance
    /// (`crates/broker-node/src/main.rs`, boot plus the `serve_api`
    /// share) so the handler reads the file values, and each test gets
    /// its own instance through `new` so tests stay isolated. No broker
    /// path reads it. Management-plane only; publish and deliver never
    /// touch this store.
    pub mcp: Arc<crate::mcp::McpStore>,
    /// Running message collects for `POST /mcp` tool
    /// `subscribe_collect` (MC-04): bounded filter-keyed slots fed by
    /// the publish paths. The kernel shares its instance (the serve
    /// path hands it over), so MQTT ingress feeds the same slots the
    /// tool drains. Idle publishes pay one atomic load; feeds take one
    /// short lock and clone only for matching slots.
    pub collectors: Arc<crate::mcp::CollectRegistry>,
    ws_conn_counter: Arc<AtomicU64>,
}

impl ApiState {
    // One handle per subsystem plus the config registry, node name and
    // the kernel→edge close sender; bundling them would hide the
    // construction sites without helping.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        engine: Arc<RuleEngine>,
        sessions: Arc<SessionManager>,
        router: Arc<SubscriptionRouter>,
        metrics: Arc<Metrics>,
        auth: Arc<MemoryAuth>,
        conns: Arc<ConnTable>,
        config: Arc<ConfigRegistry>,
        node_id: String,
        edge_tx: UnboundedSender<BrokerFrame>,
    ) -> Self {
        // Boot seeding (W0-21): route the loaded MQTT users/ACLs into the
        // very instance the caller shares (the kernel passes its BrokerLink
        // `MemoryAuth` here), so no second copy can diverge. An empty
        // snapshot is a no-op (today's empty behaviour).
        // Start the uptime clock that `GET /nodes` reads.
        crate::v5::nodes::mark_process_start();
        // Fix the node name that every endpoint renders.
        crate::node_scope::init_local_node_name(&node_id);
        if let Err(error) = auth.seed_from_registry(&config) {
            panic!("invalid stored mqtt_users snapshot: {error}");
        }
        // Boot replay (W0-22): route the loaded rules into the very engine
        // instance the caller shares, through the same validated create
        // path as `RuleEngine::create_rule`. An empty snapshot is a no-op
        // (today's empty behaviour); an invalid stored rule fails boot
        // loudly instead of being skipped silently.
        if let Err(error) = engine.seed_from_registry(&config) {
            panic!("invalid stored rules snapshot: {error}");
        }
        // Rule spill accounting (B4-07): mirror the shared engine's
        // ingress spill counters into these metrics. First attach wins,
        // so a kernel-shared engine keeps the kernel's mirror while
        // standalone engines report here; memory-only inputs only ever
        // count refusals.
        engine.set_metrics(&metrics);
        // Boot replay (W0-23): route the loaded connectors into a shared
        // directory so management reads observe the persisted connectors
        // from the first connection, and re-register every live sink
        // through the same path `create_connector` uses. An empty snapshot
        // is a no-op (today's empty behaviour); an invalid snapshot fails
        // boot loudly instead of being skipped silently.
        let connectors = Arc::new(crate::v5::rules::ConnectorStore::from_registry(
            &config, &engine,
        ));
        // Boot seeding (W2-02): route the loaded authenticator chain into
        // a shared chain instance so management reads and the CONNECT
        // consult observe the same order without a restart. An empty
        // snapshot is a no-op (today's empty behaviour).
        let authn_chain = Arc::new(broker_auth::AuthnChain::from_registry(&config));
        // Boot seeding (W2-03): route the loaded node-cache config into
        // a shared cache instance so management reads and the CONNECT
        // recorder observe the same enabled flag and cap without a
        // restart. Cached entries always start empty (memory-only).
        let authn_node_cache = Arc::new(broker_auth::NodeAuthCache::from_registry(&config));
        // Boot seeding (W2-05): route the loaded authentication
        // settings into a shared store so management reads and the
        // CONNECT consult observe the same snapshot without a restart.
        // The subscriber keeps the node cache's enabled flag and cap in
        // sync with the settings' cache half on every validated replace.
        let authn_settings = Arc::new(broker_auth::AuthnSettingsStore::from_registry(&config));
        // Boot seeding (W2-14): route the loaded authorization
        // node-cache policy into a shared store so management reads
        // observe the persisted policy without a restart. Cached verdicts
        // always start empty (memory-only, per-session).
        let authz_node_cache = Arc::new(broker_auth::AuthzNodeCache::from_registry(&config));
        // Boot seeding (W2-16): route the loaded authorization settings
        // into a shared store so management reads observe the persisted
        // decisions without a restart. Enforcement lands with the
        // rule-table tasks; the store itself is management-plane only.
        let authz_settings = Arc::new(broker_auth::AuthzSettingsStore::from_registry(&config));
        // Boot seeding (W2-17): route the loaded authorization source
        // registry into a shared registry so management reads observe
        // the persisted sources from the first connection. Defaults
        // yield the built-in database source.
        let authz_sources = Arc::new(broker_auth::AuthzSourceRegistry::from_registry(&config));
        // Boot seeding (W2-25): route the loaded API key records into
        // a shared directory so management reads observe the persisted
        // keys from the first connection. An empty snapshot is a no-op
        // (today's empty behaviour); secrets stay hashed at rest.
        let api_keys = Arc::new(crate::v5::api_keys::ApiKeyStore::from_registry(&config));
        // Boot seeding (W2-32): route the loaded SSO backends into a
        // shared registry so management reads observe the persisted
        // backends from the first connection. An empty snapshot is a
        // no-op (today's empty behaviour: an empty set lists as `[]`).
        let sso_backends = Arc::new(crate::v5::sso::SsoStore::from_registry(&config));
        // Boot seeding (W3-02): route the loaded actions into a shared
        // directory so management reads observe the persisted actions
        // from the first connection. An empty snapshot is a no-op
        // (today's empty behaviour: an empty store lists as an empty
        // page).
        let actions = Arc::new(crate::v5::actions::ActionStore::from_registry(&config));
        let action_sink_lock = Arc::new(tokio::sync::Mutex::new(()));
        // The alarm directory hoisted here so the action-sink boot replay
        // can raise build failures (W1-16 owns the endpoint).
        let alarms = Arc::new(crate::v5::alarms::AlarmStore::new());
        // MV-07: a stored list above the cap still boots. The broker
        // keeps all rows, alarms, and refuses new writes. The create
        // path checks the same cap, so no new row can grow the list.
        if actions.snapshot().len() > crate::v5::actions::MAX_ACTIONS {
            alarms.activate(
                "action_limit_exceeded",
                &format!(
                    "action store holds {} entries, at most {} (field `actions`)",
                    actions.snapshot().len(),
                    crate::v5::actions::MAX_ACTIONS
                ),
                serde_json::json!({
                    "kind": "action",
                    "limit": crate::v5::actions::MAX_ACTIONS,
                    "count": actions.snapshot().len(),
                }),
            );
        }
        // Boot seeding (W3-04): mirror the persisted action enable flags
        // into the engine's lock-free dispatch snapshot so a restart keeps
        // disabled actions skipped from the first publish. MV-01: every
        // action rides its own dispatch key (`action:{id}`), the key the
        // fire path uses. An empty store is a no-op (today's empty
        // behaviour: nothing disabled).
        {
            let enables: Vec<(String, bool)> = actions
                .snapshot()
                .iter()
                .map(|record| (broker_config::v5_action_sink_key(&record.id), record.enable))
                .collect();
            engine.sync_action_enables(&enables);
        }
        // Boot seeding (W3-19): mirror the persisted connector enable
        // flags into the same lock-free dispatch snapshot so a restart
        // keeps disabled connectors skipped from the first publish. The
        // connector store owns the flag and persistence; this only
        // mirrors it so the publish path never takes the store lock. An
        // empty store is a no-op (today's empty behaviour).
        for entry in &config.snapshot().connectors.connectors {
            if !entry.enable {
                engine.set_action_enabled(&entry.id, false);
            }
        }
        // Boot seeding (W3-05): register a zeroed per-action metric record
        // for every stored action so the first publish after a restart
        // attributes from zero. Metrics are memory-only by design (action
        // config itself persists via W3-02); an empty store is a no-op.
        // MV-01: records ride the dispatch keys.
        {
            let ids: Vec<String> = actions
                .snapshot()
                .iter()
                .map(|record| broker_config::v5_action_sink_key(&record.id))
                .collect();
            engine.sync_action_metrics(&ids);
        }
        // Boot replay (MV-01/SB-03): rebuild every stored action's owned
        // runtime sink from the stored connector settings plus the
        // action's own parameters and register under the action's
        // dispatch key. A build that cannot complete keeps the action
        // stored, leaves no runtime sink, raises
        // `action_build_failed:{id}` and reports `disconnected` with the
        // reason — never an invented value, nothing dialled.
        let _ = crate::v5::action_sinks::rebuild_all_action_sinks(
            &engine,
            &connectors,
            &actions,
            &alarms,
        );
        {
            let cache = Arc::clone(&authn_node_cache);
            authn_settings.set_subscriber(Arc::new(
                move |next: &broker_config::AuthnSettingsConf| {
                    cache.apply_settings(next.node_cache.enable, next.node_cache.max_count);
                },
            ));
            let current = authn_settings.get();
            authn_node_cache
                .apply_settings(current.node_cache.enable, current.node_cache.max_count);
        }
        // Boot seeding (W3-26): route the loaded sources into a shared
        // directory so management reads observe the persisted sources
        // from the first connection. An empty snapshot is a no-op
        // (today's empty behaviour: an empty store lists as an empty
        // page).
        let sources = Arc::new(crate::v5::sources::SourceStore::from_registry(&config));
        // Boot seeding (W3-29): mirror the persisted source enable flags
        // into the engine's lock-free ingress snapshot so a restart keeps
        // stopped sources shedding from the first publish, and register a
        // zeroed per-source metric record for every stored source so the
        // first publish after a restart attributes from zero. Metrics are
        // memory-only by design (source config itself persists via W3-26);
        // an empty store is a no-op.
        {
            let enables: Vec<(String, bool)> = sources
                .snapshot()
                .iter()
                .map(|entry| (entry.id.clone(), entry.enable))
                .collect();
            engine.sync_source_enables(&enables);
            let ids: Vec<String> = enables.iter().map(|(id, _)| id.clone()).collect();
            engine.sync_source_metrics(&ids);
        }
        // Boot seeding (W3-10): route the loaded rule-engine settings
        // into a shared store so management reads observe the persisted
        // knobs from the first connection, and hot-apply every validated
        // write to the engine's fire-path snapshot through the subscriber
        // (no restart; the fire path never touches the registry or this
        // store). Defaults skip system messages from the first publish.
        let rule_engine_config = Arc::new(broker_rules::EngineConfigStore::from_registry(&config));
        engine.apply_engine_config(rule_engine_config.get());
        {
            let engine_handle = Arc::clone(&engine);
            rule_engine_config.set_subscriber(Arc::new(
                move |next: &broker_config::RuleEngineConf| {
                    engine_handle.apply_engine_config(next.clone());
                },
            ));
        }
        // Boot seeding (W3-32): route the loaded transformation chain
        // into a shared chain so management reads observe the persisted
        // order from the first connection, and mirror it once into the
        // engine's lock-free ingress snapshot so the first publish
        // rewrites without a restart. An empty snapshot is a no-op.
        // Boot seeding (W3-36): register a zeroed per-transformation
        // metric record for every stored name so the first publish
        // after a restart attributes from zero. Metrics stay memory-only
        // by design (chain config persists via W3-32); an empty chain
        // is a no-op.
        let transformations =
            Arc::new(crate::v5::transformations::TransformChainStore::from_registry(&config));
        engine.sync_transforms(transformations.steps_snapshot());
        {
            let names: Vec<String> = transformations
                .snapshot()
                .iter()
                .map(|record| record.name.clone())
                .collect();
            engine.sync_transform_metrics(&names);
        }
        // Link the shared authorization source registry into the
        // caller-shared `MemoryAuth` (AP-06): publish/subscribe verdicts
        // attribute to the built-in counters and honour the stored
        // enable flag and order without a restart. One `Arc` clone.
        auth.set_authz_sources(Arc::clone(&authz_sources));
        let state = Self {
            engine,
            sessions,
            router,
            metrics,
            auth,
            conns,
            admin_users: Arc::new(AdminUsers::from_registry(&config)),
            tokens: Arc::new(ApiTokens::new()),
            config: Arc::clone(&config),
            node_id,
            edge_tx,
            bans: Arc::new(crate::v5::banned::BanStore::new()),
            alarms,
            monitor: Arc::new(crate::v5::monitor::MonitorHistory::new()),
            stats: Arc::new(StatsStore::new()),
            prometheus: Arc::new(crate::v5::prometheus::PrometheusStore::from_registry(
                &config,
            )),
            readiness: Arc::new(NodeReadiness::new()),
            retainer_config: Arc::new(crate::v5::retainer::RetainerConfigStore::new()),
            retained: Arc::new(broker_storage::MemoryStore::new()),
            retained_subscribe_gate: Arc::new(tokio::sync::RwLock::new(())),
            slow_subs: Arc::new(crate::v5::slow_subscriptions::SlowSubsStore::new()),
            slow_subs_settings: Arc::new(
                crate::v5::slow_subscriptions::SlowSubsSettingsStore::new(),
            ),
            tracing: Arc::new(crate::v5::tracing::TracingFlagStore::new()),
            traces: Arc::new(crate::v5::trace::TraceStore::new()),
            auto_subscribe: Arc::new(crate::v5::auto_subscribe::AutoSubscribeStore::new()),
            delayed: Arc::new(broker_delayed::DelayedScheduler::new()),
            licence: Arc::new(crate::licence::LicenceStore::new()),
            authn_chain,
            authn_node_cache,
            authn_settings,
            authz_node_cache,
            authz_settings,
            authz_sources,
            api_keys,
            audit: Arc::new(crate::v5::audit::AuditLog::new()),
            sso_backends,
            sources,
            actions,
            action_sink_lock,
            connectors,
            rule_engine_config,
            operator_api_keys: Arc::new(crate::api_keys::ApiKeyStore::from_env()),
            license: Arc::new(crate::v5::license::LicenseState::new()),
            listeners: Arc::new(broker_config::schema::ListenersConf::default()),
            listener_store: Arc::new(crate::v5::listeners::ListenerStore::from_config(
                &broker_config::schema::ListenersConf::default(),
            )),
            startup_config: Arc::new(std::sync::RwLock::new(None)),
            transformations,
            schema_registry: Arc::new(
                crate::v5::schema_registry::SchemaRegistryStore::from_registry(&config),
            ),
            namespaces: Arc::new(crate::v5::mt::NamespaceStore::new()),
            mcp: Arc::new(crate::mcp::McpStore::new()),
            collectors: Arc::new(crate::mcp::CollectRegistry::new()),
            ws_conn_counter: Arc::new(AtomicU64::new(1 << 62)),
        };
        state.raise_unavailable_stored_kinds();
        state
    }

    /// Standalone state for tests and tools (empty sessions/router).
    /// Uses validated defaults without touching the kernel data dir.
    /// The kernel→edge sender is a throwaway channel whose receiver is
    /// immediately dropped, so kick sends fail silently there and keep
    /// today's unbind-only behaviour in unit tests.
    pub fn standalone(engine: Arc<RuleEngine>) -> Self {
        let (edge_tx, edge_rx) = tokio::sync::mpsc::unbounded_channel::<BrokerFrame>();
        drop(edge_rx);
        crate::v5::nodes::mark_process_start();
        // Standalone settings store shares its subscriber with the very
        // node-cache instance below, so validated replaces keep the
        // cache's enabled flag and cap in sync without a restart.
        let authn_node_cache = Arc::new(broker_auth::NodeAuthCache::new());
        let authn_settings = Arc::new(broker_auth::AuthnSettingsStore::new());
        {
            let cache = Arc::clone(&authn_node_cache);
            authn_settings.set_subscriber(Arc::new(
                move |next: &broker_config::AuthnSettingsConf| {
                    cache.apply_settings(next.node_cache.enable, next.node_cache.max_count);
                },
            ));
        }
        // Standalone engine-config store shares its subscriber with the
        // very engine instance above, so validated replaces hot-apply to
        // the fire-path snapshot without a restart (mirrors the kernel
        // boot wiring).
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
        // Standalone connector directory shares the standalone registry
        // so creates persist to history and backup export carries them.
        // An empty snapshot stays empty.
        let config = defaults_registry();
        let connectors = Arc::new(crate::v5::rules::ConnectorStore::from_registry(
            &config, &engine,
        ));
        let auth = Arc::new(MemoryAuth::new());
        let authz_sources = Arc::new(broker_auth::AuthzSourceRegistry::new());
        // Link the standalone registry into the standalone rule table
        // (AP-06): handler tests that publish through `MemoryAuth`
        // attribute to the built-in counters. One `Arc` clone.
        auth.set_authz_sources(Arc::clone(&authz_sources));
        Self {
            engine,
            sessions: Arc::new(SessionManager::new()),
            router: Arc::new(SubscriptionRouter::new()),
            metrics: Arc::new(Metrics::new()),
            auth,
            conns: Arc::new(ConnTable::default()),
            admin_users: Arc::new(AdminUsers::with_default_admin()),
            tokens: Arc::new(ApiTokens::new()),
            config: Arc::clone(&config),
            node_id: "indra-node-1".to_string(),
            edge_tx,
            bans: Arc::new(crate::v5::banned::BanStore::new()),
            alarms: Arc::new(crate::v5::alarms::AlarmStore::new()),
            monitor: Arc::new(crate::v5::monitor::MonitorHistory::new()),
            stats: Arc::new(StatsStore::new()),
            prometheus: Arc::new(crate::v5::prometheus::PrometheusStore::from_registry(
                &config,
            )),
            readiness: Arc::new(NodeReadiness::new()),
            retainer_config: Arc::new(crate::v5::retainer::RetainerConfigStore::new()),
            retained: Arc::new(broker_storage::MemoryStore::new()),
            retained_subscribe_gate: Arc::new(tokio::sync::RwLock::new(())),
            slow_subs: Arc::new(crate::v5::slow_subscriptions::SlowSubsStore::new()),
            slow_subs_settings: Arc::new(
                crate::v5::slow_subscriptions::SlowSubsSettingsStore::new(),
            ),
            tracing: Arc::new(crate::v5::tracing::TracingFlagStore::new()),
            traces: Arc::new(crate::v5::trace::TraceStore::new()),
            auto_subscribe: Arc::new(crate::v5::auto_subscribe::AutoSubscribeStore::new()),
            delayed: Arc::new(broker_delayed::DelayedScheduler::new()),
            licence: Arc::new(crate::licence::LicenceStore::new()),
            authn_chain: Arc::new(broker_auth::AuthnChain::new()),
            authn_node_cache,
            authn_settings,
            authz_node_cache: Arc::new(broker_auth::AuthzNodeCache::new()),
            authz_settings: Arc::new(broker_auth::AuthzSettingsStore::new()),
            authz_sources,
            api_keys: Arc::new(crate::v5::api_keys::ApiKeyStore::new()),
            audit: Arc::new(crate::v5::audit::AuditLog::new()),
            sso_backends: Arc::new(crate::v5::sso::SsoStore::new()),
            sources: Arc::new(crate::v5::sources::SourceStore::new()),
            actions: Arc::new(crate::v5::actions::ActionStore::new()),
            action_sink_lock: Arc::new(tokio::sync::Mutex::new(())),
            connectors,
            rule_engine_config,
            operator_api_keys: Arc::new(crate::api_keys::ApiKeyStore::from_env()),
            license: Arc::new(crate::v5::license::LicenseState::new()),
            listeners: Arc::new(broker_config::schema::ListenersConf::default()),
            listener_store: Arc::new(crate::v5::listeners::ListenerStore::from_config(
                &broker_config::schema::ListenersConf::default(),
            )),
            startup_config: Arc::new(std::sync::RwLock::new(None)),
            transformations: Arc::new(crate::v5::transformations::TransformChainStore::new()),
            schema_registry: Arc::new(crate::v5::schema_registry::SchemaRegistryStore::new()),
            namespaces: Arc::new(crate::v5::mt::NamespaceStore::new()),
            mcp: Arc::new(crate::mcp::McpStore::new()),
            collectors: Arc::new(crate::mcp::CollectRegistry::new()),
            ws_conn_counter: Arc::new(AtomicU64::new(1 << 62)),
        }
    }

    /// Raise one active alarm per stored connector/action whose kind this
    /// broker removed or has not made `ready` (KC-00/D2). The records stay
    /// untouched and readable; the alarms make the unavailable state
    /// visible without claiming the rule itself is broken. Callers that
    /// share a kernel alarm store repeat this call after that swap.
    pub fn raise_unavailable_stored_kinds(&self) {
        let snapshot = self.config.snapshot();
        for connector in &snapshot.connectors.connectors {
            if broker_config::is_removed_connector_type(&connector.connector_type)
                || crate::v5::rules::kind_refusal(&connector.connector_type).is_some()
            {
                self.alarms.activate(
                    &format!("connector_unavailable:{}", connector.id),
                    &format!(
                        "connector {:?} uses unavailable type {:?}; stored configuration preserved but unavailable",
                        connector.id, connector.connector_type
                    ),
                    serde_json::json!({
                        "kind": "connector",
                        "id": connector.id,
                        "type": connector.connector_type,
                    }),
                );
            }
        }
        for action in &snapshot.actions.actions {
            if broker_config::is_removed_connector_type(&action.action_type)
                || crate::v5::rules::kind_refusal(&action.action_type).is_some()
            {
                self.alarms.activate(
                    &format!("action_unavailable:{}", action.id),
                    &format!(
                        "action {:?} uses unavailable type {:?}; stored configuration preserved but unavailable",
                        action.id, action.action_type
                    ),
                    serde_json::json!({
                        "kind": "action",
                        "id": action.id,
                        "type": action.action_type,
                    }),
                );
            }
        }
    }

    /// Mint a collision-proof virtual connection id for a dashboard
    /// WebSocket client (far above BEAM's monotonic conn ids).
    pub fn next_ws_conn_id(&self) -> u64 {
        self.ws_conn_counter.fetch_add(1, Ordering::SeqCst)
    }
}

/// Defaults-only registry for standalone state (tests and tools).
///
/// Loads from a unique scratch path that is never created, so `load`
/// yields validated empty roots with no filesystem writes and the
/// standalone state can never observe the kernel data dir.
fn defaults_registry() -> Arc<ConfigRegistry> {
    static SCRATCH_COUNTER: AtomicU64 = AtomicU64::new(0);
    let slot = SCRATCH_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir().join(format!(
        "indramqtt-standalone-{nanos}-{}-{slot}",
        std::process::id()
    ));
    Arc::new(ConfigRegistry::load(&dir).expect("validated defaults always load"))
}

/// One-time route table, shared by every caller. Built on first
/// use; every later call clones it. A test-only counter tracks builds.
static SHARED_ROUTES: OnceLock<Router<ApiState>> = OnceLock::new();

/// Count of route table builds. Tests read this; production ignores it.
static ROUTE_BUILDS: AtomicU64 = AtomicU64::new(0);

/// Public routes (dashboard assets, health, login, MQTT-over-WebSocket)
/// stay outside the authentication layer; every other route requires a
/// bearer token enforced by [`api_auth::require_api_auth`].
///
/// The table holds no state. [`router`] sets the live state per call
/// with `with_state`. The MCP endpoint reuses this table through its
/// own one-time cache. Authentication runs per request against the
/// current stores. Concurrent calls share the table. No cycle exists:
/// the table never points back at the outer router or the state.
pub(crate) fn router_routes() -> Router<ApiState> {
    SHARED_ROUTES
        .get_or_init(|| {
            ROUTE_BUILDS.fetch_add(1, Ordering::Relaxed);
            build_router_routes()
        })
        .clone()
}

/// Count of route table builds. Tests read this; production ignores it.
#[cfg(test)]
pub(crate) fn router_route_builds() -> u64 {
    ROUTE_BUILDS.load(Ordering::Relaxed)
}

/// Build the full route table: protected routes behind the auth
/// layer plus the public routes. Holds no state.
fn build_router_routes() -> Router<ApiState> {
    let protected: Router<ApiState> = Router::new()
        .route("/schemas", get(v5::schemas::list_schemas))
        .route("/schemas/:name", get(v5::schemas::get_schema))
        .nest("/api/v5", v5::protected_router())
        .route("/api/v1/nodes", get(get_nodes))
        .route("/api/v1/clients", get(get_clients))
        .route("/api/v1/clients/:id", get(get_client))
        .route("/api/v1/metrics", get(get_metrics))
        .route(
            "/api/v1/connectors",
            get(get_connectors).post(create_connector),
        )
        .route("/api/v1/auth/users", get(list_users).post(create_user))
        .route("/api/v1/auth/users/:username", delete(delete_user))
        .route("/api/v1/auth/verify_stats", get(get_verify_stats))
        .route("/api/v1/auth/acls", get(list_acls).post(create_acl))
        .route("/api/v1/auth/acls/:id", delete(delete_acl))
        .route("/api/v1/rules", get(list_rules).post(create_rule))
        .route("/api/v1/rules/test", post(test_rule))
        .route("/api/v1/rules/functions", get(list_functions))
        .route("/api/v1/rules/sources", get(list_rule_sources))
        .route("/api/v1/rules/:id", get(get_rule).delete(delete_rule))
        .route(
            "/api/v1/config/validate",
            post(config_runtime::validate_config),
        )
        .route("/api/v1/config/diff", post(config_runtime::diff_config))
        .route("/api/v1/config/reload", post(config_runtime::reload_config))
        .route("/api/v1/config/history", get(config_runtime::list_history))
        .route(
            "/api/v1/config/history/:id",
            get(config_runtime::get_history_version),
        )
        .route(
            "/api/v1/config/history/:id/restore",
            post(config_runtime::restore_history_version),
        )
        .route(
            "/api/v1/config/explain",
            get(config_runtime::explain_config),
        )
        .route("/api/v1/licence/request", get(licence::get_request))
        .route("/api/v1/licence/install", post(licence::install_licence))
        .route("/api/v1/licence/status", get(licence::get_status))
        .layer(axum::middleware::from_fn(api_auth::require_api_auth));
    let public: Router<ApiState> = Router::new()
        .route("/", get(redirect_to_dashboard))
        .route("/dashboard", get(get_dashboard))
        .route("/ui", get(get_modern_ui))
        .route("/swagger", get(get_swagger_ui))
        .route("/api-docs/openapi.json", get(get_openapi_spec))
        .route("/assets/*path", get(get_dashboard_asset))
        .route("/static/*path", get(get_dashboard_static_asset))
        .route("/favicon.ico", get(get_dashboard_favicon))
        .route("/version", get(get_dashboard_version))
        .route("/ws/mqtt", get(ws::ws_mqtt_handler))
        .route("/healthz", get(|| async { "OK" }))
        // MCP endpoint (MC-01, MC-02): outside the auth middleware
        // on purpose. The handler runs its own API key check and refuses
        // dashboard tokens, so it never passes the management auth layer.
        // A missing key answers 401 with the management error shape and
        // a disabled endpoint answers 404. Tool calls re-enter the router
        // below, so key scopes and the viewer rule apply through the same
        // middleware. The 8 MiB tool limit applies while the body is read,
        // before the key check: a larger body ends with 413 and is never
        // held in memory. The `subscribe_collect`
        // messaging tool (MC-04) rides the same endpoint and the same
        // key check; its temporary subscription is management-plane only.
        .route(
            "/mcp",
            post(crate::mcp::post_mcp).layer(axum::extract::DefaultBodyLimit::max(
                crate::mcp::MCP_MAX_BODY_BYTES,
            )),
        )
        // Public alias of `GET /api/v5/status` for load balancers. It needs no token.
        .route("/status", get(v5::status::get_status))
        .nest("/api/v5", v5::public_router())
        .merge(protected);
    public
}

/// Serve-ready router: the shared route table with the live state set.
/// Each call clones the table, so no caller rebuilds the routes.
pub fn router(state: ApiState) -> Router {
    router_routes()
        .layer(axum::Extension(state.clone()))
        .with_state(state)
}

/// Serve the management API on an already-bound listener.
pub async fn serve(listener: tokio::net::TcpListener, state: ApiState) -> std::io::Result<()> {
    axum::serve(listener, router(state)).await
}

async fn redirect_to_dashboard() -> Response {
    (
        StatusCode::SEE_OTHER,
        [("location", "/dashboard")],
        "redirecting to /dashboard",
    )
        .into_response()
}

async fn get_dashboard() -> Response {
    for candidate in &["dashboard/dist/index.html", "../dashboard/dist/index.html"] {
        if let Ok(content) = tokio::fs::read_to_string(candidate).await {
            return ([("content-type", "text/html; charset=utf-8")], content).into_response();
        }
    }
    (
        [("content-type", "text/html; charset=utf-8")],
        dashboard::DASHBOARD_HTML,
    )
        .into_response()
}

async fn get_swagger_ui() -> Response {
    (
        [("content-type", "text/html; charset=utf-8")],
        swagger::swagger_ui_html(),
    )
        .into_response()
}

async fn get_openapi_spec() -> Response {
    (
        [("content-type", "application/json; charset=utf-8")],
        swagger::openapi_spec_json(),
    )
        .into_response()
}

async fn get_modern_ui() -> Response {
    for candidate in &["dashboard/dist/index.html", "../dashboard/dist/index.html"] {
        if let Ok(content) = tokio::fs::read_to_string(candidate).await {
            return ([("content-type", "text/html; charset=utf-8")], content).into_response();
        }
    }
    (
        [("content-type", "text/html; charset=utf-8")],
        dashboard::DASHBOARD_HTML,
    )
        .into_response()
}

async fn get_dashboard_asset(axum::extract::Path(path): axum::extract::Path<String>) -> Response {
    let clean = path.trim_start_matches('/').replace("..", "");
    for base in &["dashboard/dist/assets", "../dashboard/dist/assets"] {
        let p = std::path::PathBuf::from(base).join(&clean);
        if let Ok(bytes) = tokio::fs::read(&p).await {
            let mime = if clean.ends_with(".js") {
                "application/javascript"
            } else if clean.ends_with(".css") {
                "text/css"
            } else if clean.ends_with(".svg") {
                "image/svg+xml"
            } else if clean.ends_with(".png") {
                "image/png"
            } else if clean.ends_with(".ico") {
                "image/x-icon"
            } else {
                "application/octet-stream"
            };
            return (
                [
                    ("content-type", mime),
                    ("cache-control", "public, max-age=31536000, immutable"),
                ],
                bytes,
            )
                .into_response();
        }
    }
    (StatusCode::NOT_FOUND, "Asset not found").into_response()
}

async fn get_dashboard_static_asset(
    axum::extract::Path(path): axum::extract::Path<String>,
) -> Response {
    let clean = path.trim_start_matches('/').replace("..", "");
    for base in &["dashboard/dist/static", "../dashboard/dist/static"] {
        let p = std::path::PathBuf::from(base).join(&clean);
        if let Ok(bytes) = tokio::fs::read(&p).await {
            let mime = if clean.ends_with(".js") {
                "application/javascript"
            } else if clean.ends_with(".css") {
                "text/css"
            } else if clean.ends_with(".svg") {
                "image/svg+xml"
            } else if clean.ends_with(".png") {
                "image/png"
            } else if clean.ends_with(".ico") {
                "image/x-icon"
            } else if clean.ends_with(".woff2") {
                "font/woff2"
            } else if clean.ends_with(".woff") {
                "font/woff"
            } else if clean.ends_with(".ttf") {
                "font/ttf"
            } else if clean.ends_with(".json") {
                "application/json"
            } else {
                "application/octet-stream"
            };
            return (
                [
                    ("content-type", mime),
                    ("cache-control", "public, max-age=31536000, immutable"),
                ],
                bytes,
            )
                .into_response();
        }
    }
    (StatusCode::NOT_FOUND, "Static asset not found").into_response()
}

async fn get_dashboard_favicon() -> Response {
    for candidate in &[
        "dashboard/dist/favicon.ico",
        "../dashboard/dist/favicon.ico",
    ] {
        if let Ok(bytes) = tokio::fs::read(candidate).await {
            return ([("content-type", "image/x-icon")], bytes).into_response();
        }
    }
    StatusCode::NOT_FOUND.into_response()
}

async fn get_dashboard_version() -> Response {
    for candidate in &["dashboard/dist/version", "../dashboard/dist/version"] {
        if let Ok(content) = tokio::fs::read_to_string(candidate).await {
            return ([("content-type", "text/plain; charset=utf-8")], content).into_response();
        }
    }
    StatusCode::NOT_FOUND.into_response()
}

/// Creation payload: identifiers are assigned server-side (`rule-<n>`).
/// `tenant` selects the owning tenant; absent or blank selects the
/// default tenant, so calls without it behave as before.
#[derive(Debug, Deserialize)]
struct CreateRuleRequest {
    name: String,
    topic_filter: String,
    #[serde(default)]
    sql_query: Option<String>,
    #[serde(default = "default_enabled")]
    enabled: bool,
    #[serde(default)]
    actions: Vec<ActionDto>,
    #[serde(default)]
    tenant: Option<String>,
}

fn default_enabled() -> bool {
    true
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum ActionDto {
    Republish { topic: String, qos: u8 },
    Log,
    ForwardConnector { connector_id: String },
}

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

fn bad_request(message: impl Into<String>) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorBody {
            error: message.into(),
        }),
    )
        .into_response()
}

fn not_found(id: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorBody {
            error: format!("rule not found: {id}"),
        }),
    )
        .into_response()
}

/// A registry commit or atomic save failed after the in-memory MQTT
/// user/ACL mutation applied: the loss must never be silent, so the
/// caller gets a 500 (mirrors the admin-users `Persist` mapping).
fn persist_error(error: broker_config::ConfigError) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorBody {
            error: format!("cannot persist MQTT users: {error}"),
        }),
    )
        .into_response()
}

/// A registry commit or atomic save failed after the in-memory rule
/// mutation applied: the loss must never be silent, so the caller gets
/// a 500. [`RuleEngineError::Persist`] already renders as
/// `cannot persist rules: ...`, so its display is reused verbatim
/// (never re-prefixed).
fn rule_persist_error(error: RuleEngineError) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorBody {
            error: error.to_string(),
        }),
    )
        .into_response()
}

async fn create_rule(
    State(state): State<ApiState>,
    Json(req): Json<CreateRuleRequest>,
) -> Response {
    if req.name.trim().is_empty() {
        return bad_request("rule name must not be empty");
    }
    // Tenant scope: absent or blank rides as the default tenant.
    // A malformed id fails with 400. An unknown tenant (no managed
    // namespace) fails with 400 and stores nothing.
    let tenant = broker_config::canonical_rule_tenant(req.tenant.as_deref().unwrap_or(""));
    if !broker_config::is_valid_rule_tenant(&tenant) {
        return bad_request(format!("tenant {tenant:?} is not valid (field `tenant`)"));
    }
    if !state.namespaces.contains(&tenant) {
        return bad_request(format!("unknown tenant {tenant:?} (field `tenant`)"));
    }
    let topic_filter = match TopicFilter::new(req.topic_filter) {
        Ok(filter) => filter,
        Err(e) => return bad_request(format!("invalid topic_filter: {e}")),
    };
    let mut actions = Vec::with_capacity(req.actions.len());
    for dto in req.actions {
        match dto {
            ActionDto::Republish { topic, qos } => {
                let topic = match Topic::new(topic) {
                    Ok(topic) => topic,
                    Err(e) => return bad_request(format!("invalid republish topic: {e}")),
                };
                let qos = match QoS::try_from(qos) {
                    Ok(qos) => qos,
                    Err(e) => return bad_request(format!("invalid republish qos: {e}")),
                };
                actions.push(RuleAction::Republish { topic, qos });
            }
            ActionDto::Log => actions.push(RuleAction::Log),
            ActionDto::ForwardConnector { connector_id } => {
                if connector_id.trim().is_empty() {
                    return bad_request("connector_id must not be empty");
                }
                actions.push(RuleAction::ForwardConnector { connector_id });
            }
        }
    }

    let rule: Rule = match state.engine.create_rule_in_tenant(
        tenant,
        req.name,
        topic_filter,
        req.sql_query,
        req.enabled,
        actions,
    ) {
        Ok(rule) => rule,
        // A persist failure is a 500 (the in-memory rule stays but the
        // disk save failed); validation failures stay 400.
        Err(error @ RuleEngineError::Persist(_)) => return rule_persist_error(error),
        Err(e) => return bad_request(format!("invalid sql_query: {e}")),
    };
    (StatusCode::CREATED, Json(rule)).into_response()
}

async fn list_rules(State(state): State<ApiState>) -> Json<Vec<Rule>> {
    Json(state.engine.list_rules())
}

async fn get_rule(State(state): State<ApiState>, Path(id): Path<String>) -> Response {
    match state.engine.get_rule(&id) {
        Some(rule) => Json(rule).into_response(),
        None => not_found(&id),
    }
}

async fn delete_rule(State(state): State<ApiState>, Path(id): Path<String>) -> Response {
    match state.engine.remove_rule(&id) {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => not_found(&id),
        Err(error) => rule_persist_error(error),
    }
}

/// Dry-run payload for the SQL studio tester.
#[derive(Debug, Deserialize)]
struct TestRuleRequest {
    #[serde(default)]
    sql_query: Option<String>,
    #[serde(default)]
    topic_filter: Option<String>,
    topic: String,
    payload: serde_json::Value,
}

/// Evaluate SQL + filter over a sample payload without storing a rule.
async fn test_rule(State(_state): State<ApiState>, Json(req): Json<TestRuleRequest>) -> Response {
    match broker_rules::try_evaluate(
        req.sql_query.as_deref(),
        req.topic_filter.as_deref(),
        &req.topic,
        &req.payload,
    ) {
        Ok((matched, projected)) => Json(serde_json::json!({
            "matched": matched,
            "projected": projected,
        }))
        .into_response(),
        Err(error) => bad_request(error),
    }
}

/// List available rule event sources (RQ-04).
async fn list_rule_sources(
    State(state): State<ApiState>,
) -> Json<Vec<broker_rules::EventSourceInfo>> {
    Json(state.engine.list_event_sources())
}

/// The streaming-SQL function catalog for the SQL studio.
async fn list_functions() -> Json<&'static [broker_rules::FunctionMeta]> {
    Json(broker_rules::builtin_function_metadata())
}

/// Creation payload for a user credential (password is write-only).
/// Quotas are optional per-field; omitted fields stay unlimited.
#[derive(Debug, Deserialize)]
struct CreateUserRequest {
    username: String,
    password: String,
    #[serde(default)]
    quotas: Option<QuotasDto>,
}

#[derive(Debug, Deserialize)]
struct QuotasDto {
    #[serde(default)]
    max_connections: Option<u32>,
    #[serde(default)]
    max_publish_rate: Option<u32>,
    #[serde(default)]
    max_publish_burst: Option<u32>,
}

/// Optional tenant scope for the v1 user and ACL routes (TM-01):
/// `?tenant=<id>` selects one tenant table. Absent or empty selects the
/// default tenant, so calls without the parameter behave exactly as
/// before. Each write validates the tenant id before it applies: a bad
/// id fails with 400 naming the field and leaves state unchanged.
#[derive(Debug, Deserialize, Default)]
struct TenantScope {
    #[serde(default)]
    tenant: Option<String>,
}

/// Resolve the tenant scope to its store key: empty rides as the default
/// tenant; other ids must pass the credential-store tenant rule.
fn resolve_tenant_scope(query: &TenantScope) -> Result<String, Box<Response>> {
    let raw = query.tenant.as_deref().unwrap_or("");
    if raw.is_empty() {
        return Ok(broker_auth::DEFAULT_TENANT_ID.to_string());
    }
    if !broker_auth::is_valid_tenant_id(raw) {
        return Err(Box::new(bad_request(format!(
            "tenant {raw:?} is not a valid tenant id (field `tenant`)"
        ))));
    }
    Ok(raw.to_string())
}

/// Users with their quota bounds (passwords are write-only, never listed).
/// `?tenant=<id>` lists one tenant; absent lists the default tenant.
async fn list_users(
    State(state): State<ApiState>,
    axum::extract::Query(scope): axum::extract::Query<TenantScope>,
) -> Response {
    let tenant = match resolve_tenant_scope(&scope) {
        Ok(tenant) => tenant,
        Err(response) => return *response,
    };
    Json(
        state
            .auth
            .usernames_in_tenant(&tenant)
            .iter()
            .map(|username| {
                serde_json::json!({
                    "username": username,
                    "quotas": state.auth.get_quotas_in_tenant(&tenant, username),
                })
            })
            .collect::<Vec<_>>(),
    )
    .into_response()
}

/// Create one user credential (password is write-only).
/// `?tenant=<id>` stores in one tenant; absent stores in the default
/// tenant. The same username in two tenants is two independent
/// identities. The write persists through the registry, so it survives a
/// restart and a reload and ships in the export and the backup.
async fn create_user(
    State(state): State<ApiState>,
    axum::extract::Query(scope): axum::extract::Query<TenantScope>,
    Json(req): Json<CreateUserRequest>,
) -> Response {
    let tenant = match resolve_tenant_scope(&scope) {
        Ok(tenant) => tenant,
        Err(response) => return *response,
    };
    if req.username.trim().is_empty() {
        return bad_request("username must not be empty");
    }
    if req.password.is_empty() {
        return bad_request("password must not be empty");
    }
    if let Err(error) =
        state
            .auth
            .add_user_in_tenant(&tenant, req.username.clone(), req.password.as_bytes())
    {
        return persist_error(error);
    }
    if let Some(quotas) = req.quotas.map(|dto| broker_auth::UserQuotas {
        max_connections: dto.max_connections,
        max_publish_rate: dto.max_publish_rate,
        max_publish_burst: dto.max_publish_burst,
    }) {
        // The user was just created, so this always finds it; a persist
        // failure is still surfaced instead of silently dropped.
        if let Err(error) = state
            .auth
            .set_quotas_in_tenant(&tenant, &req.username, quotas)
        {
            return persist_error(error);
        }
    }
    // Quotas always render as an object (all-null when unset) so GET
    // and POST share one shape.
    let stored = state.auth.get_quotas_in_tenant(&tenant, &req.username);
    // A changed user invalidates cached authorization decisions so the
    // next operation is re-authorized (AZ-02).
    state.sessions.clear_all_authz_caches();
    (
        StatusCode::CREATED,
        Json(serde_json::json!({ "username": req.username, "quotas": stored })),
    )
        .into_response()
}

/// Live password verification counters for the storm test.
/// `verifications` counts blocking hashes run; `dedup_joins` counts
/// CONNECTs that shared a leader check. Both are memory-only and
/// restart at zero. Management-plane read only; the CONNECT path
/// increments them with lock-free atomics.
async fn get_verify_stats(State(state): State<ApiState>) -> Response {
    Json(serde_json::json!({
        "verifications": state.auth.verify_run_count(),
        "dedup_joins": state.auth.verify_dedup_join_count(),
    }))
    .into_response()
}

/// Creation payload for one ordered ACL entry.
#[derive(Debug, Deserialize)]
struct CreateAclRequest {
    client_pattern: String,
    action: String,
    topic_pattern: String,
    allow: bool,
}

/// Ordered ACL snapshot (first match wins on evaluation).
/// `?tenant=<id>` reads one tenant; absent reads the default tenant.
async fn list_acls(
    State(state): State<ApiState>,
    axum::extract::Query(scope): axum::extract::Query<TenantScope>,
) -> Response {
    let tenant = match resolve_tenant_scope(&scope) {
        Ok(tenant) => tenant,
        Err(response) => return *response,
    };
    Json(state.auth.acl_rules_in_tenant(&tenant)).into_response()
}

/// Append one ordered ACL entry (first match wins within the tenant).
/// `?tenant=<id>` writes one tenant; absent writes the default tenant.
/// The write persists through the registry, so it survives a restart
/// and a reload and ships in the export and the backup.
async fn create_acl(
    State(state): State<ApiState>,
    axum::extract::Query(scope): axum::extract::Query<TenantScope>,
    Json(req): Json<CreateAclRequest>,
) -> Response {
    let tenant = match resolve_tenant_scope(&scope) {
        Ok(tenant) => tenant,
        Err(response) => return *response,
    };
    if req.client_pattern.trim().is_empty() {
        return bad_request("client_pattern must not be empty");
    }
    let action = match AclAction::parse(&req.action) {
        Some(action) => action,
        None => return bad_request("action must be publish, subscribe, or all"),
    };
    if TopicFilter::new(req.topic_pattern.clone()).is_err() {
        return bad_request("topic_pattern must be a valid MQTT filter");
    }
    if let Err(error) = state.auth.add_rule_in_tenant(
        &tenant,
        AclRule::new(
            req.client_pattern.clone(),
            action,
            req.topic_pattern.clone(),
            req.allow,
        ),
    ) {
        return persist_error(error);
    }
    // A changed rule invalidates cached decisions so the next operation
    // is re-authorized (AZ-02).
    state.sessions.clear_all_authz_caches();
    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "client_pattern": req.client_pattern,
            "topic_pattern": req.topic_pattern,
            "allow": req.allow,
        })),
    )
        .into_response()
}

/// Delete one user. `?tenant=<id>` deletes in one tenant; absent deletes
/// in the default tenant.
async fn delete_user(
    State(state): State<ApiState>,
    axum::extract::Path(username): axum::extract::Path<String>,
    axum::extract::Query(scope): axum::extract::Query<TenantScope>,
) -> Response {
    let tenant = match resolve_tenant_scope(&scope) {
        Ok(tenant) => tenant,
        Err(response) => return *response,
    };
    match state.auth.remove_user_in_tenant(&tenant, &username) {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => persist_error(error),
    }
}

/// Delete one ACL entry by index within its tenant list.
/// `?tenant=<id>` deletes in one tenant; absent deletes in the default.
async fn delete_acl(
    State(state): State<ApiState>,
    axum::extract::Path(id): axum::extract::Path<usize>,
    axum::extract::Query(scope): axum::extract::Query<TenantScope>,
) -> Response {
    let tenant = match resolve_tenant_scope(&scope) {
        Ok(tenant) => tenant,
        Err(response) => return *response,
    };
    match state.auth.remove_rule_in_tenant(&tenant, id) {
        Ok(true) => {
            // A changed rule invalidates cached decisions so the next
            // operation is re-authorized (AZ-02).
            state.sessions.clear_all_authz_caches();
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => persist_error(error),
    }
}

fn node_id() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "local".to_string())
}

/// Node status, version, live counts, and live rule engine tuning.
async fn get_nodes(State(state): State<ApiState>) -> Json<serde_json::Value> {
    // Live tuning values. Reason: the docs prove the file setting
    // applies by reading the running engine, not the stored file.
    Json(serde_json::json!({
        "node_id": node_id(),
        "status": "running",
        "version": env!("CARGO_PKG_VERSION"),
        "connections": state.metrics.connections_active(),
        "rules": state.engine.list_rules().len(),
        "window_channel_depth": state.engine.window_channel_depth(),
        "backpressure_policy": format!("{:?}", state.engine.backpressure_policy()),
    }))
}

/// Sorted ids of currently connected clients.
async fn get_clients(State(state): State<ApiState>) -> Json<Vec<String>> {
    Json(state.sessions.active_client_ids())
}

/// Full detail row for one client session.
async fn get_client(State(state): State<ApiState>, Path(id): Path<String>) -> Response {
    match state.sessions.client_info(&id) {
        Some(info) => Json(info).into_response(),
        None => not_found(&id),
    }
}

/// Registered connectors with kinds and tier tags for the dashboard.
async fn get_connectors(State(state): State<ApiState>) -> Json<Vec<serde_json::Value>> {
    Json(
        state
            .engine
            .connectors()
            .infos()
            .iter()
            .map(|info| {
                serde_json::json!({
                    "id": info.id,
                    "kind": info.kind,
                    "tier": broker_connectors::connector_tier(&info.kind),
                })
            })
            .collect(),
    )
}

/// Creation payload for a streaming connector. `kind` selects the sink
/// family; `config` carries that family's fields verbatim. Streaming
/// sinks connect lazily on first delivery, so creation never blocks on
/// external brokers. Qualification builds this payload directly.
#[derive(Debug, Deserialize)]
pub struct CreateConnectorRequest {
    pub id: String,
    pub kind: String,
    #[serde(default)]
    pub config: serde_json::Value,
}

async fn create_connector(
    State(state): State<ApiState>,
    Json(mut req): Json<CreateConnectorRequest>,
) -> Response {
    if req.id.trim().is_empty() {
        return bad_request("connector id must not be empty");
    }
    let kind = req.kind.to_ascii_lowercase();
    if let Some(message) = removed_connector_refusal(&kind) {
        return bad_request(message);
    }
    // MV-01: `action:` names the reserved dispatch-key space of the v5
    // action-owned sinks. A v1 connector with that prefix would collide
    // with it, so it is refused here before the sink is built.
    if broker_config::is_v5_action_sink_key(req.id.trim()) {
        return bad_request(format!(
            "connector id {:?} is reserved: ids starting with `action:` name action sinks (field `id`)",
            req.id
        ));
    }
    if matches!(kind.as_str(), "disk_log" | "disklog" | "disk") {
        if let Err(message) =
            v1_disk_log::prepare_v1_disk_log(&mut req, &state.connectors.data_dir())
        {
            return bad_request(message);
        }
    }
    let built = build_connector(&req).await;
    match built {
        Ok((kind, sink)) => {
            // The v1 path keeps its own name space: its sinks live in the
            // connector manager under the bare id and are not written into
            // the v5 connector store. The persisted v5 connectors and
            // their boot replay therefore never adopt a v1 row. The name
            // space has a finite bound so it cannot grow without limit.
            if !state.engine.connectors().register_v1(req.id.clone(), sink) {
                return bad_request(format!(
                    "v1 connector store holds {} entries, at most {}",
                    broker_connectors::MAX_V1_CONNECTORS,
                    broker_connectors::MAX_V1_CONNECTORS
                ));
            }
            (
                StatusCode::CREATED,
                Json(serde_json::json!({ "id": req.id, "kind": kind })),
            )
                .into_response()
        }
        Err(message) => bad_request(&message),
    }
}

/// Build one streaming connector from a creation request: parse the
/// kind-specific config, validate it, and wire the sink to its
/// transport. Pure construction except `disk_log`, which opens its
/// directory (hence async). Qualification calls this entry directly.
pub async fn build_connector(
    req: &CreateConnectorRequest,
) -> Result<(String, std::sync::Arc<dyn broker_connectors::Sink>), String> {
    let kind = req.kind.to_ascii_lowercase();
    match kind.as_str() {
            #[cfg(feature = "kafka")]
            "kafka" => {
                let config: broker_connectors::KafkaSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid kafka config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid kafka config: {e}"))?;
                // Production write path on the maintained `rdkafka`
                // driver; the hand-written TCP framing stays for offline
                // unit tests only.
                let transport = std::sync::Arc::new(
                    broker_connectors::RdkafkaKafkaTransport::new(&config)
                        .map_err(|e| format!("invalid kafka transport: {e}"))?,
                );
                let sink = broker_connectors::KafkaSink::new(config, transport)
                    .map_err(|e| format!("invalid kafka sink: {e}"))?;
                Ok(("kafka".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(not(feature = "kafka"))]
            "kafka" => Err("connector kind `kafka` is not compiled in: missing feature `kafka`; rebuild with `--features kafka` or `--all-features`"
                .to_string()),
            #[cfg(feature = "rabbitmq")]
            "rabbitmq" | "rabbit" | "amqp" => {
                let config: broker_connectors::RabbitMqSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid rabbitmq config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid rabbitmq config: {e}"))?;
                // Production write path on the maintained `lapin`
                // driver; the hand-written TCP framing stays for offline
                // unit tests only.
                let transport = std::sync::Arc::new(
                    broker_connectors::LapinRabbitTransport::new(&config.endpoint)
                        .map_err(|e| format!("invalid rabbitmq transport: {e}"))?,
                );
                let sink = broker_connectors::RabbitMqSink::new(config, transport)
                    .map_err(|e| format!("invalid rabbitmq sink: {e}"))?;
                Ok(("rabbitmq".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(not(feature = "rabbitmq"))]
            "rabbitmq" | "rabbit" | "amqp" => Err("connector kind `rabbitmq` is not compiled in: missing feature `rabbitmq`; rebuild with `--features rabbitmq` or `--all-features`"
                .to_string()),
            "logger" | "console" => {
                let sink = broker_connectors::ConsoleLoggerSink::new(req.id.clone());
                Ok((
                    "console".to_string(),
                    std::sync::Arc::new(sink)
                        as std::sync::Arc<dyn broker_connectors::Sink>,
                ))
            }
            "postgres" | "postgresql" => {
                let config: broker_connectors::PostgreSqlSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid postgres config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid postgres config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors::DriverPgTransport::new(
                        &config.connection_url,
                        config.pool_size,
                    )
                    .map_err(|e| format!("invalid postgres transport: {e}"))?,
                );
                let sink = broker_connectors::PostgreSqlSink::new(config, transport)
                    .map_err(|e| format!("invalid postgres sink: {e}"))?;
                Ok(("postgres".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            "redis" => {
                let config: broker_connectors::RedisSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid redis config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid redis config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors::DriverRedisTransport::new(&config.endpoint)
                        .map_err(|e| format!("invalid redis transport: {e}"))?,
                );
                let sink = broker_connectors::RedisSink::new(config, transport)
                    .map_err(|e| format!("invalid redis sink: {e}"))?;
                Ok(("redis".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            "mysql" | "mariadb" => {
                let config: broker_connectors::MySqlSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid mysql config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid mysql config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors::DriverMySqlTransport::new(
                        &config.connection_url,
                        config.pool_size,
                    )
                    .map_err(|e| format!("invalid mysql transport: {e}"))?,
                );
                let sink = broker_connectors::MySqlSink::new(config, transport)
                    .map_err(|e| format!("invalid mysql sink: {e}"))?;
                Ok(("mysql".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(feature = "clickhouse")]
            "clickhouse" | "ch" => {
                let config: broker_connectors::ClickHouseSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid clickhouse config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid clickhouse config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors::DriverClickHouseTransport::new(&config)
                        .map_err(|e| format!("invalid clickhouse transport: {e}"))?,
                );
                let sink = broker_connectors::ClickHouseSink::new(config, transport)
                    .map_err(|e| format!("invalid clickhouse sink: {e}"))?;
                Ok(("clickhouse".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(not(feature = "clickhouse"))]
            "clickhouse" | "ch" => Err("connector kind `clickhouse` is not compiled in: missing feature `clickhouse`; rebuild with `--features clickhouse` or `--all-features`"
                .to_string()),
            "influxdb" | "influx" => {
                let config: broker_connectors::InfluxDbSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid influxdb config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid influxdb config: {e}"))?;
                let sink = broker_connectors::InfluxDbSink::new(config, reqwest::Client::new())
                    .map_err(|e| format!("invalid influxdb sink: {e}"))?;
                Ok(("influxdb".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(feature = "s3")]
            "s3" | "minio" => {
                let config: broker_connectors::S3SinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid s3 config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid s3 config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors::SdkS3Transport::new(&config)
                        .map_err(|e| format!("invalid s3 transport: {e}"))?,
                );
                let sink = broker_connectors::S3Sink::new(config, transport)
                    .map_err(|e| format!("invalid s3 sink: {e}"))?;
                Ok(("s3".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(not(feature = "s3"))]
            "s3" | "minio" => Err("connector kind `s3` is not compiled in: missing feature `s3`; rebuild with `--features s3` or `--all-features`"
                .to_string()),
            #[cfg(feature = "elasticsearch")]
            "elasticsearch" | "elastic" | "es" | "opensearch" => {
                let config: broker_connectors::ElasticsearchSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid elasticsearch config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid elasticsearch config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors::DriverElasticsearchTransport::new(&config)
                        .map_err(|e| format!("invalid elasticsearch transport: {e}"))?,
                );
                let sink = broker_connectors::ElasticsearchSink::new(config, transport)
                    .map_err(|e| format!("invalid elasticsearch sink: {e}"))?;
                Ok(("elasticsearch".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(not(feature = "elasticsearch"))]
            "elasticsearch" | "elastic" | "es" | "opensearch" => Err("connector kind `elasticsearch` is not compiled in: missing feature `elasticsearch`; rebuild with `--features elasticsearch` or `--all-features`"
                .to_string()),
            "timescaledb" | "timescale" => {
                let config: broker_connectors::TimescaleDbSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid timescaledb config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid timescaledb config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors::DriverTimescaleDbTransport::new(
                        &config.connection_url,
                        config.pool_size,
                    )
                    .map_err(|e| format!("invalid timescaledb transport: {e}"))?,
                );
                let sink = broker_connectors::TimescaleDbSink::new(config, transport)
                    .map_err(|e| format!("invalid timescaledb sink: {e}"))?;
                Ok(("timescaledb".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            "webhook" | "http" | "rest" => {
                let config: broker_connectors::HttpSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid webhook config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid webhook config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors::ReqwestHttpTransport::new(&config, reqwest::Client::new())
                        .map_err(|e| format!("invalid webhook transport: {e}"))?,
                );
                let sink = broker_connectors::HttpSink::new(config, transport)
                    .map_err(|e| format!("invalid webhook sink: {e}"))?;
                Ok(("webhook".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            "mqtt_bridge" | "mqtt-bridge" | "bridge" => {
                let config: broker_connectors::MqttBridgeSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid mqtt_bridge config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid mqtt_bridge config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors::RumqttcMqttBridgeTransport::new(&config)
                        .map_err(|e| format!("invalid mqtt_bridge transport: {e}"))?,
                );
                let sink = broker_connectors::MqttBridgeSink::new(config, transport)
                    .map_err(|e| format!("invalid mqtt_bridge sink: {e}"))?;
                Ok(("mqtt_bridge".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            "disk_log" | "disklog" | "disk" => {
                let config: broker_connectors::DiskLogSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid disk_log config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid disk_log config: {e}"))?;
                let writer = std::sync::Arc::new(
                    broker_connectors::FileDiskLogWriter::open(&config)
                        .await
                        .map_err(|e| format!("invalid disk_log writer: {e}"))?,
                );
                let sink = broker_connectors::DiskLogSink::open(config, writer)
                    .await
                    .map_err(|e| format!("invalid disk_log sink: {e}"))?;
                Ok(("disk_log".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            "sparkplug_b" | "sparkplug" | "spb" => {
                let config: broker_connectors_enterprise::SparkplugSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid sparkplug_b config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid sparkplug_b config: {e}"))?;
                // The sink sends to the MQTT server that `mqtt_url`
                // names, through the `rumqttc` driver transport.
                let transport: std::sync::Arc<dyn broker_connectors_enterprise::SparkplugTransport> =
                    match config.mqtt_url.clone().filter(|url| !url.trim().is_empty()) {
                        Some(url) => std::sync::Arc::new(
                            broker_connectors_enterprise::RumqttcSparkplugTransport::new(
                                &url,
                                config.client_id.as_deref(),
                            )
                            .map_err(|e| format!("invalid sparkplug_b sink: {e}"))?,
                        ),
                        // Without a server there is no destination. A sink
                        // that keeps the frames in memory would report
                        // success and deliver nothing.
                        None => {
                            return Err("invalid sparkplug_b sink: `mqtt_url` is necessary".to_string())
                        }
                    };
                let sink = broker_connectors_enterprise::SparkplugBSink::new(config, transport)
                    .map_err(|e| format!("invalid sparkplug_b sink: {e}"))?;
                Ok(("sparkplug_b".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(feature = "kinesis")]
            "kinesis" | "aws_kinesis" => {
                let config: broker_connectors_enterprise::KinesisSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid kinesis config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid kinesis config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::SdkKinesisTransport::new(&config)
                        .map_err(|e| format!("invalid kinesis transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::KinesisSink::new(config, transport)
                    .map_err(|e| format!("invalid kinesis sink: {e}"))?;
                Ok(("kinesis".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(not(feature = "kinesis"))]
            "kinesis" | "aws_kinesis" => Err("connector kind `kinesis` is not compiled in: missing feature `kinesis`; rebuild with `--features kinesis` or `--all-features`"
                .to_string()),
            #[cfg(feature = "gcp_pubsub")]
            "gcp_pubsub" | "gcp" | "pubsub" => {
                let config: broker_connectors_enterprise::GcpPubSubSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid gcp_pubsub config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid gcp_pubsub config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::SdkGcpPubSubTransport::new(&config)
                        .map_err(|e| format!("invalid gcp_pubsub transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::GcpPubSubSink::new(config, transport)
                    .map_err(|e| format!("invalid gcp_pubsub sink: {e}"))?;
                Ok(("gcp_pubsub".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(not(feature = "gcp_pubsub"))]
            "gcp_pubsub" | "gcp" | "pubsub" => Err("connector kind `gcp_pubsub` is not compiled in: missing feature `gcp_pubsub`; rebuild with `--features gcp_pubsub` or `--all-features`"
                .to_string()),
            #[cfg(not(feature = "azure_eventhubs"))]
            "azure_eventhubs" | "azure" | "eventhubs" => Err(
                "connector kind 'azure_eventhubs' needs cargo feature 'azure_eventhubs' (disabled in this build)"
                    .to_string(),
            ),
            #[cfg(feature = "azure_eventhubs")]
            "azure_eventhubs" | "azure" | "eventhubs" => {
                let config: broker_connectors_enterprise::AzureEventHubsSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid azure_eventhubs config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid azure_eventhubs config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::HttpAzureEventHubsTransport::new(
                        &config,
                        reqwest::Client::new(),
                    )
                    .map_err(|e| format!("invalid azure_eventhubs transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::AzureEventHubsSink::new(config, transport)
                    .map_err(|e| format!("invalid azure_eventhubs sink: {e}"))?;
                Ok(("azure_eventhubs".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            "pulsar" => {
                let config: broker_connectors_enterprise::PulsarSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid pulsar config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid pulsar config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::DriverPulsarTransport::new(&config)
                        .map_err(|e| format!("invalid pulsar transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::PulsarSink::new(config, transport)
                    .map_err(|e| format!("invalid pulsar sink: {e}"))?;
                Ok(("pulsar".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            "oci_streaming" | "oci" | "oracle_streaming" => {
                let config: broker_connectors_enterprise::OciStreamingSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid oci_streaming config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid oci_streaming config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::HttpOciStreamingTransport::new(
                        &config,
                        reqwest::Client::new(),
                    )
                    .map_err(|e| format!("invalid oci_streaming transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::OciStreamingSink::new(config, transport)
                    .map_err(|e| format!("invalid oci_streaming sink: {e}"))?;
                Ok(("oci_streaming".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(not(feature = "aws_iot"))]
            "aws_iot" | "aws_iot_core" => Err(
                "connector kind 'aws_iot' needs cargo feature 'aws_iot' (disabled in this build)"
                    .to_string(),
            ),
            #[cfg(feature = "aws_iot")]
            "aws_iot" | "aws_iot_core" => {
                let config: broker_connectors_enterprise::AwsIotConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid aws_iot config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid aws_iot config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::TlsAwsIotTransport::new(&config)
                        .map_err(|e| format!("invalid aws_iot transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::AwsIotSink::new(config, transport)
                    .map_err(|e| format!("invalid aws_iot sink: {e}"))?;
                Ok(("aws_iot".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(not(feature = "azure_iot"))]
            "azure_iot" | "azure_iothub" | "iothub" => Err(
                "connector kind 'azure_iot' needs cargo feature 'azure_iot' (disabled in this build)"
                    .to_string(),
            ),
            #[cfg(feature = "azure_iot")]
            "azure_iot" | "azure_iothub" | "iothub" => {
                let config: broker_connectors_enterprise::AzureIotConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid azure_iot config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid azure_iot config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::TlsAzureIotTransport::new(&config)
                        .map_err(|e| format!("invalid azure_iot transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::AzureIotSink::new(config, transport)
                    .map_err(|e| format!("invalid azure_iot sink: {e}"))?;
                Ok(("azure_iot".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(not(feature = "gcp_iot"))]
            "gcp_iot" | "gcp_iot_core" | "cloud_iot" => Err(
                "connector kind 'gcp_iot' needs cargo feature 'gcp_iot' (disabled in this build)"
                    .to_string(),
            ),
            #[cfg(feature = "gcp_iot")]
            "gcp_iot" | "gcp_iot_core" | "cloud_iot" => {
                let config: broker_connectors_enterprise::GcpIotConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid gcp_iot config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid gcp_iot config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::TlsGcpIotTransport::new(&config)
                        .map_err(|e| format!("invalid gcp_iot transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::GcpIotSink::new(config, transport)
                    .map_err(|e| format!("invalid gcp_iot sink: {e}"))?;
                Ok(("gcp_iot".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            "opc_ua" | "opcua" => {
                let config: broker_connectors_enterprise::OpcUaSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid opc_ua config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid opc_ua config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::DriverOpcUaTransport::new(&config)
                        .map_err(|e| format!("invalid opc_ua transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::OpcUaSink::new(config, transport)
                    .map_err(|e| format!("invalid opc_ua sink: {e}"))?;
                Ok(("opc_ua".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(not(feature = "azure_blob"))]
            "azure_blob" | "azureblob" | "azblob" => Err(
                "connector kind 'azure_blob' needs cargo feature 'azure_blob' (disabled in this build)"
                    .to_string(),
            ),
            #[cfg(feature = "azure_blob")]
            "azure_blob" | "azureblob" | "azblob" => {
                let config: broker_connectors_enterprise::AzureBlobSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid azure_blob config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid azure_blob config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::HttpAzureBlobTransport::new(&config, reqwest::Client::new())
                        .map_err(|e| format!("invalid azure_blob transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::AzureBlobSink::new(config, transport)
                    .map_err(|e| format!("invalid azure_blob sink: {e}"))?;
                Ok(("azure_blob".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            "tablestore" | "ots" | "alibaba_tablestore" => {
                let config: broker_connectors_enterprise::TablestoreSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid tablestore config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid tablestore config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::HttpTablestoreTransport::new(
                        &config,
                        reqwest::Client::new(),
                    )
                    .map_err(|e| format!("invalid tablestore transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::TablestoreSink::new(config, transport)
                    .map_err(|e| format!("invalid tablestore sink: {e}"))?;
                Ok(("tablestore".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            "s3_tables" | "s3tables" | "iceberg" => {
                Err(crate::REMOVED_CONNECTOR_MESSAGE.to_string())
            }
            #[cfg(feature = "confluent")]
            "confluent" | "confluent_kafka" | "confluent_cloud" => {
                let config: broker_connectors_enterprise::ConfluentKafkaConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid confluent config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid confluent config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::RdkafkaConfluentTransport::new(&config)
                        .map_err(|e| format!("invalid confluent transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::ConfluentKafkaSink::new(config, transport)
                    .map_err(|e| format!("invalid confluent sink: {e}"))?;
                Ok(("confluent".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(not(feature = "confluent"))]
            "confluent" | "confluent_kafka" | "confluent_cloud" => Err("connector kind `confluent` is not compiled in: missing feature `confluent`; rebuild with `--features confluent` or `--all-features`"
                .to_string()),
            "rocketmq" | "rocket_mq" | "apache_rocketmq" => {
                let config: broker_connectors_enterprise::RocketMqSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid rocketmq config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid rocketmq config: {e}"))?;
                // All configs use the vendor path.
                // Reason: the vendor server only speaks remoting.
                let transport: std::sync::Arc<
                    dyn broker_connectors_enterprise::rocketmq::RocketMqTransport,
                > = std::sync::Arc::new(
                    broker_connectors_enterprise::rocketmq::RemotingRocketMqTransport::new(
                        &config,
                    )
                    .map_err(|e| format!("invalid rocketmq transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::RocketMqSink::new(config, transport)
                    .map_err(|e| format!("invalid rocketmq sink: {e}"))?;
                Ok(("rocketmq".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(feature = "mongodb")]
            "mongodb" | "mongo" | "documentdb" => {
                let config: broker_connectors_enterprise::MongoDbSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid mongodb config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid mongodb config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::DriverMongoDbTransport::new(&config)
                        .map_err(|e| format!("invalid mongodb transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::MongoDbSink::new(config, transport)
                    .map_err(|e| format!("invalid mongodb sink: {e}"))?;
                Ok(("mongodb".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(not(feature = "mongodb"))]
            "mongodb" | "mongo" | "documentdb" => Err("connector kind `mongodb` is not compiled in: missing feature `mongodb`; rebuild with `--features mongodb` or `--all-features`"
                .to_string()),
            "mssql" | "sqlserver" | "azuresql" => {
                let config: broker_connectors_enterprise::MssqlSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid mssql config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid mssql config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::DriverMssqlTransport::new(&config)
                        .map_err(|e| format!("invalid mssql transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::MssqlSink::new(config, transport)
                    .map_err(|e| format!("invalid mssql sink: {e}"))?;
                Ok(("mssql".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(feature = "cassandra")]
            "cassandra" | "scylla" | "cql" => {
                let config: broker_connectors_enterprise::CassandraSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid cassandra config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid cassandra config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::ScyllaCassandraTransport::new(&config)
                        .map_err(|e| format!("invalid cassandra transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::CassandraSink::new(config, transport)
                    .map_err(|e| format!("invalid cassandra sink: {e}"))?;
                Ok(("cassandra".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(not(feature = "cassandra"))]
            "cassandra" | "scylla" | "cql" => Err("connector kind `cassandra` is not compiled in: missing feature `cassandra`; rebuild with `--features cassandra` or `--all-features`"
                .to_string()),
            #[cfg(feature = "couchbase")]
            "couchbase" | "couch" | "cb" => {
                let config: broker_connectors_enterprise::CouchbaseSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid couchbase config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid couchbase config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::DriverCouchbaseTransport::new(&config)
                        .map_err(|e| format!("invalid couchbase transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::CouchbaseSink::new(config, transport)
                    .map_err(|e| format!("invalid couchbase sink: {e}"))?;
                Ok(("couchbase".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(not(feature = "couchbase"))]
            "couchbase" | "couch" | "cb" => Err("connector kind `couchbase` is not compiled in: missing feature `couchbase`; rebuild with `--features couchbase` or `--all-features`"
                .to_string()),
            "tdengine" | "td" | "taos" => {
                let config: broker_connectors_enterprise::TdengineSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid tdengine config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid tdengine config: {e}"))?;
                // Production split: Basic + plaintext rides the maintained
                // driver; token auth and TLS endpoints stay on REST so
                // stored configuration keeps working. Either way the kind
                // stays `tdengine`.
                if config.use_driver() {
                    let transport = std::sync::Arc::new(
                        broker_connectors_enterprise::DriverTdengineTransport::new(&config)
                            .map_err(|e| format!("invalid tdengine transport: {e}"))?,
                    );
                    let sink = broker_connectors_enterprise::TdengineSink::new(config, transport)
                        .map_err(|e| format!("invalid tdengine sink: {e}"))?;
                    Ok(("tdengine".to_string(), std::sync::Arc::new(sink)
                        as std::sync::Arc<dyn broker_connectors::Sink>))
                } else {
                    let transport = std::sync::Arc::new(
                        broker_connectors_enterprise::HttpTdengineTransport::new(
                            &config,
                            reqwest::Client::new(),
                        )
                        .map_err(|e| format!("invalid tdengine transport: {e}"))?,
                    );
                    let sink = broker_connectors_enterprise::TdengineSink::new(config, transport)
                        .map_err(|e| format!("invalid tdengine sink: {e}"))?;
                    Ok(("tdengine".to_string(), std::sync::Arc::new(sink)
                        as std::sync::Arc<dyn broker_connectors::Sink>))
                }
            }
            "iotdb" | "iot_db" => {
                let config: broker_connectors_enterprise::IotDbSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid iotdb config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid iotdb config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::HttpIotDbTransport::new(&config, reqwest::Client::new())
                        .map_err(|e| format!("invalid iotdb transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::IotDbSink::new(config, transport)
                    .map_err(|e| format!("invalid iotdb sink: {e}"))?;
                Ok(("iotdb".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            "timestream" | "ts" | "aws_timestream" => {
                let config: broker_connectors_enterprise::TimestreamSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid timestream config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid timestream config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::HttpTimestreamTransport::new(&config, reqwest::Client::new())
                        .map_err(|e| format!("invalid timestream transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::TimestreamSink::new(config, transport)
                    .map_err(|e| format!("invalid timestream sink: {e}"))?;
                Ok(("timestream".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(not(feature = "dynamodb"))]
            "dynamodb" | "dynamo" | "ddb" => Err(
                "connector kind 'dynamodb' needs cargo feature 'dynamodb' (disabled in this build)"
                    .to_string(),
            ),
            #[cfg(feature = "dynamodb")]
            "dynamodb" | "dynamo" | "ddb" => {
                let config: broker_connectors_enterprise::DynamoDbSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid dynamodb config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid dynamodb config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::SdkDynamoDbTransport::new(&config)
                        .map_err(|e| format!("invalid dynamodb transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::DynamoDbSink::new(config, transport)
                    .map_err(|e| format!("invalid dynamodb sink: {e}"))?;
                Ok(("dynamodb".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            "snowflake" | "snow" | "sf" => {
                let config: broker_connectors_enterprise::SnowflakeSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid snowflake config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid snowflake config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::HttpSnowflakeTransport::new(&config, reqwest::Client::new())
                        .map_err(|e| format!("invalid snowflake transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::SnowflakeSink::new(config, transport)
                    .map_err(|e| format!("invalid snowflake sink: {e}"))?;
                Ok(("snowflake".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            "databricks" | "databricks_sql" | "delta" => {
                let config: broker_connectors_enterprise::DatabricksSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid databricks config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid databricks config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::HttpDatabricksTransport::new(&config, reqwest::Client::new())
                        .map_err(|e| format!("invalid databricks transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::DatabricksSink::new(config, transport)
                    .map_err(|e| format!("invalid databricks sink: {e}"))?;
                Ok(("databricks".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            "doris" => {
                let config: broker_connectors_enterprise::DorisSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid doris config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid doris config: {e}"))?;
                // No-redirect client: the transport replays auth + label
                // itself on the FE 307 -> BE hop (a stock client strips
                // Authorization when the BE is a different origin).
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::HttpDorisTransport::new(
                        &config,
                        broker_connectors_enterprise::doris_http_client(config.timeout()),
                    )
                    .map_err(|e| format!("invalid doris transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::DorisSink::new(config, transport)
                    .map_err(|e| format!("invalid doris sink: {e}"))?;
                Ok(("doris".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(feature = "bigquery")]
            "bigquery" | "bq" | "gbq" => {
                let config: broker_connectors_enterprise::BigQuerySinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid bigquery config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid bigquery config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::SdkBigQueryTransport::new(&config, reqwest::Client::new())
                        .map_err(|e| format!("invalid bigquery transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::BigQuerySink::new(config, transport)
                    .map_err(|e| format!("invalid bigquery sink: {e}"))?;
                Ok(("bigquery".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            #[cfg(not(feature = "bigquery"))]
            "bigquery" | "bq" | "gbq" => Err("connector kind `bigquery` is not compiled in: missing feature `bigquery`; rebuild with `--features bigquery` or `--all-features`"
                .to_string()),
            "redshift" | "rs" | "aws_redshift" => {
                let config: broker_connectors_enterprise::RedshiftSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid redshift config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid redshift config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::SdkRedshiftTransport::new(&config)
                        .map_err(|e| format!("invalid redshift transport: {e}"))?,
                );
                let sink = broker_connectors_enterprise::RedshiftSink::new(config, transport)
                    .map_err(|e| format!("invalid redshift sink: {e}"))?;
                Ok(("redshift".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            "oracle" | "oracle_db" | "ords" => {
                let config: broker_connectors_enterprise::OracleSinkConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid oracle config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid oracle config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::HttpOracleTransport::new(&config),
                );
                let sink = broker_connectors_enterprise::OracleSink::new(config, transport)
                    .map_err(|e| format!("invalid oracle sink: {e}"))?;
                Ok(("oracle".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            "cockroachdb" | "cockroach" | "crdb" => {
                let config: broker_connectors_enterprise::CockroachDbConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid cockroachdb config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid cockroachdb config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::PgDriverCockroachDbTransport::new(&config),
                );
                let sink = broker_connectors_enterprise::CockroachDbSink::new(config, transport)
                    .map_err(|e| format!("invalid cockroachdb sink: {e}"))?;
                Ok(("cockroachdb".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            "alloydb" | "google_alloydb" | "alloy_db" => {
                let config: broker_connectors_enterprise::AlloydbConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid alloydb config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid alloydb config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::PgDriverAlloydbTransport::new(&config),
                );
                let sink = broker_connectors_enterprise::AlloydbSink::new(config, transport)
                    .map_err(|e| format!("invalid alloydb sink: {e}"))?;
                Ok(("alloydb".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            "opentsdb" | "open_tsdb" | "tsdb" => {
                let config: broker_connectors::OpenTsdbConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid opentsdb config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid opentsdb config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors::NetworkOpenTsdbTransport::new(&config),
                );
                let sink = broker_connectors::OpenTsdbSink::new(config, transport)
                    .map_err(|e| format!("invalid opentsdb sink: {e}"))?;
                Ok(("opentsdb".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            "greptimedb" | "greptime" | "greptime_db" => {
                let config: broker_connectors::GreptimeDbConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid greptimedb config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid greptimedb config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors::HttpGreptimeDbTransport::new(&config),
                );
                let sink = std::sync::Arc::new(
                    broker_connectors::GreptimeDbSink::new(config, transport)
                        .map_err(|e| format!("invalid greptimedb sink: {e}"))?,
                );
                sink.start_linger_flush();
                Ok(("greptimedb".to_string(), sink
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            "datalayers" | "data_layers" | "datalayers_db" => {
                let config: broker_connectors_enterprise::DatalayersConfig =
                    serde_json::from_value(req.config.clone())
                        .map_err(|e| format!("invalid datalayers config: {e}"))?;
                config
                    .validate()
                    .map_err(|e| format!("invalid datalayers config: {e}"))?;
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::HttpDatalayersTransport::new(&config),
                );
                let sink = broker_connectors_enterprise::DatalayersSink::new(config, transport)
                    .map_err(|e| format!("invalid datalayers sink: {e}"))?;
                Ok(("datalayers".to_string(), std::sync::Arc::new(sink)
                    as std::sync::Arc<dyn broker_connectors::Sink>))
            }
            other => Err(format!(
                "unknown connector kind {other:?} (expected kafka, rabbitmq, postgres, redis, mysql, clickhouse, influxdb, s3, elasticsearch, timescaledb, webhook, mqtt_bridge, disk_log, sparkplug_b, kinesis, gcp_pubsub, azure_eventhubs, pulsar, mongodb, mssql, cassandra, couchbase, tdengine, iotdb, timestream, dynamodb, snowflake, databricks, doris, bigquery, redshift, oracle, cockroachdb, alloydb, opentsdb, greptimedb, datalayers, or logger)"
            )),
        }
}

/// Prometheus text exposition of node counters and gauges.
async fn get_metrics(State(state): State<ApiState>) -> Response {
    (
        [("content-type", "text/plain; version=0.0.4")],
        state.metrics.render_prometheus_metrics(),
    )
        .into_response()
}

#[cfg(test)]
mod tests;
